//! Sstables stand for the sorted string tables.
//!
//! This modules define how such a file is read and written. The files
//! are created during compaction and are exclusively written to by the
//! compaction thread. But they are read by the reader and compaction threads.
//!
//! The on-disk structure is well define in the @README.md#L205.
//! The file has 4 parts:
//! 1. The data blocks. There are BLOCK_SIZE bytes large. They have the
//!     following format items:
//!       - block-len: 4 bytes :how many bytes are packed in this block.
//!       - block-crc: 4 bytes: The block checksum for integrity checks.
//!       - array of key-value pairs: variable length: keys are serialized Key in
//!         key.rs. Values are serialized Value from value.rs.
//! 2. Index block: variable length : Has two parts:
//!        - sstable level min and max keys.
//!        - min and max keys per data-block with the starting offset of the
//!          block.
//! 3. Bloom filter block: variable length: The bloom filter is used to quickly
//!        determine if a key is present in the sstable. It is used to skip
//!        the entire sstable.
//! 4. Footer: variable length: Contains:
//!        - offset to the start of the index block: 8 bytes
//!        - offset to the start of the bloom filter block: 8 bytes
//!        - checksum: 4 bytes (CRC32 of the offsets)
//!        - magic number: 4 bytes
//!
//! Sstable has two variants. The on-disk variant has the structure described
//! above. The in-memory variant loads the index block and bloom filter block
//! into memory for quick access.
//!
//! writing to sstable:
//! 1. SSTable is created in the readwrite state.
//! 2. The caller (compactor) creates a writer and passes it through.
//! 3. The caller use the writer to push key-value pairs to the sstable.
//!     writer buffers a block worth of key-value pairs for each new key, it
//!     checks that it is greater than the last key written - this ensures the
//!     sorted order of the sstable.
//!     for min and max keys, they are the first and last keys (as sorted).
//!     it uses the block start address and the min-max key for the index block.
//!     writer also updates the bloom filter as we go.
//! 4. The block makes a syscall to write the entire data block to disk
//!    with one system call. This makes it very end.
//! 5. When the caller calls finalize on the writer, it snaps in the index
//!    block, notes its offset, snaps in the bloom filter and notes its offset
//!    and finally writes the footer.
//! 6. Write to sstable ensures that the file is fsynced to disk.
//! 7. The caller then marks the sstable as read-only before returning it to
//!    the caller who persists it to the manifest.
//!
//! reading from sstable:
//! 1. One can create a read iterator. Usually done by the compactor when
//!    when it reads all keys of a sstable to merge them into another sstable.
//! 2. SStable also gets a search key request. They should be very efficient.
//!    Most of the work is done in memory using the in-memory representation of
//!    the sstable.
//!    a. Look for the key in sstable's bloom filter. If it says not-found, skip
//!       the sstable. Move to the next sstable.
//!    b. If the key is found, look for the key in the index block. This is
//!       courtesy bloom filter, which aren't so sure if a key is present but
//!       are definitely sure if it is not.
//!    c. next we look for the key in the index-block. This is to narrow down
//!       the search from having to page it the full 10s of GBs of sstable to
//!       just the right block - a few KBs. we first make sure the key is in the
//!       min-max range of the sstable. If it is, we check the min-max for each
//!       block if that range can contain the search key. If it does, we read
//!       the block and look for the key and return its value.
use crate::data_stores::Loggable;
use crate::data_stores::bloom_filter::BloomFilter;
use crate::data_stores::key::Key;
use crate::data_stores::value::Value;
use crate::err::DbError;
use std::fs::File;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::sync::Arc;

pub const BLOCK_SIZE: usize = 4096;
pub const SSTABLE_EXTENSION: &str = "sst";
const MAGIC_NUMBER_BYTES: [u8; 4] = [0x41, 0x42, 0x43, 0x44]; // Example bytes, need to match spec if any

pub struct Sstable {
    pub id: u32,
    file: File,
    index: Vec<(Key, Key, u64)>,
    #[allow(dead_code)] // Used in search logic
    bloom_filter: BloomFilter,
    min_key: Key,
    max_key: Key,
}

pub struct SstableWriter {
    file: BufWriter<File>,
    current_block: BlockBuilder,
    index_entries: Vec<(Key, Key, u64)>, // (Min, Max, Offset)
    bloom_filter: BloomFilter,
    min_key: Option<Key>,
    max_key: Option<Key>,
}

struct BlockBuilder {
    buffer: Vec<u8>,
    min_key: Option<Key>,
    max_key: Option<Key>,
}

impl BlockBuilder {
    fn new() -> Self {
        Self {
            buffer: Vec::new(),
            min_key: None,
            max_key: None,
        }
    }

    fn add(&mut self, key: &Key, value: &Value) -> Result<(), DbError> {
        // Track min/max keys for the block
        if self.min_key.is_none() {
            self.min_key = Some(key.clone());
        }
        self.max_key = Some(key.clone());

        // Serialize K/V directly into buffer
        key.encode(&mut self.buffer)
            .map_err(|e| DbError::Io(Arc::new(e)))?;
        value
            .encode(&mut self.buffer)
            .map_err(|e| DbError::Io(Arc::new(e)))?;
        Ok(())
    }

    fn is_full(&self) -> bool {
        self.buffer.len() >= BLOCK_SIZE
    }

    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    fn flush(&mut self) -> (Vec<u8>, Option<Key>, Option<Key>) {
        if self.buffer.is_empty() {
            return (Vec::new(), None, None);
        }

        let mut block_data = Vec::with_capacity(self.buffer.len() + 8); // + len(u32) + crc(u32)

        // Reasoning for batch CRC calculation vs streaming:
        // We calculate the CRC on the whole buffer at once here (batch) rather than updating it incrementally
        // as we add keys (streaming).
        // 1. Simplicity: The implementation is simpler and less error-prone.
        // 2. Efficiency: Given the small block size (~4KB), the data remains hot in L1 cache.
        //    Batch calculation can leverage SIMD optimizations (in crc32fast) effectively on the contiguous buffer,
        //    often outperforming the overhead of many small incremental hasher state updates.
        let len = self.buffer.len() as u32;
        block_data.extend_from_slice(&len.to_le_bytes());

        let checksum = crc32fast::hash(&self.buffer);
        block_data.extend_from_slice(&checksum.to_le_bytes());

        block_data.extend_from_slice(&self.buffer);

        let min = self.min_key.take();
        let max = self.max_key.take();

        // Reset buffer
        self.buffer.clear();

        (block_data, min, max)
    }
}

impl SstableWriter {
    pub fn new(path: &std::path::Path) -> Result<Self, DbError> {
        let file = File::create(path).map_err(|e| DbError::Io(Arc::new(e)))?;
        let writer = BufWriter::new(file);

        // Bloom filter parameters: 1000 items, 0.01 fp rate.
        let bloom = BloomFilter::new(1000, 0.01);

        Ok(Self {
            file: writer,
            current_block: BlockBuilder::new(),
            index_entries: Vec::new(),
            bloom_filter: bloom,
            min_key: None,
            max_key: None,
        })
    }

    pub fn write(&mut self, key: &Key, value: &Value) -> Result<(), DbError> {
        self.bloom_filter.set(&key.bytes[..]);

        if self.min_key.is_none() {
            self.min_key = Some(key.clone());
        }
        self.max_key = Some(key.clone());

        self.current_block.add(key, value)?;

        if self.current_block.is_full() {
            self.flush_block()?;
        }
        Ok(())
    }

    fn flush_block(&mut self) -> Result<(), DbError> {
        let (block_data, min, max) = self.current_block.flush();
        if block_data.is_empty() {
            return Ok(());
        }

        let offset = self
            .file
            .stream_position()
            .map_err(|e| DbError::Io(Arc::new(e)))?;
        self.file
            .write_all(&block_data)
            .map_err(|e| DbError::Io(Arc::new(e)))?;

        if let (Some(min), Some(max)) = (min, max) {
            self.index_entries.push((min, max, offset));
        }

        Ok(())
    }

    pub fn current_size(&mut self) -> Result<u64, DbError> {
        self.file
            .stream_position()
            .map_err(|e| DbError::Io(Arc::new(e)))
    }

    pub fn finalize(mut self) -> Result<crate::data_stores::manifest::FileMetadata, DbError> {
        // Flush any remaining data in the current block
        if !self.current_block.is_empty() {
            self.flush_block()?;
        }

        // 1. Write Index Block
        let index_offset = self
            .file
            .stream_position()
            .map_err(|e| DbError::Io(Arc::new(e)))?;
        let mut index_data = Vec::new();

        // Write: min-key, max-key for sstable
        // Note: min_key/max_key must exist if we wrote anything.
        // If empty sstable, they are None.
        let min_key = self.min_key.clone().unwrap_or(Key::new(&[], 0));
        let max_key = self.max_key.clone().unwrap_or(Key::new(&[], 0));

        if let (Some(min), Some(max)) = (&self.min_key, &self.max_key) {
            min.encode(&mut index_data)
                .map_err(|e| DbError::Io(Arc::new(e)))?;
            max.encode(&mut index_data)
                .map_err(|e| DbError::Io(Arc::new(e)))?;
        }

        // Write entries: min-key, max-key, offset
        for (min, max, off) in &self.index_entries {
            min.encode(&mut index_data)
                .map_err(|e| DbError::Io(Arc::new(e)))?;
            max.encode(&mut index_data)
                .map_err(|e| DbError::Io(Arc::new(e)))?;
            index_data
                .write_all(&off.to_le_bytes())
                .map_err(|e| DbError::Io(Arc::new(e)))?;
        }

        // Wrap Index Block with [len][crc][data]
        let index_len = index_data.len() as u32;
        self.file
            .write_all(&index_len.to_le_bytes())
            .map_err(|e| DbError::Io(Arc::new(e)))?;
        let index_crc = crc32fast::hash(&index_data);
        self.file
            .write_all(&index_crc.to_le_bytes())
            .map_err(|e| DbError::Io(Arc::new(e)))?;
        self.file
            .write_all(&index_data)
            .map_err(|e| DbError::Io(Arc::new(e)))?;

        // 2. Write Bloom Filter Block
        let bloom_offset = self
            .file
            .stream_position()
            .map_err(|e| DbError::Io(Arc::new(e)))?;

        // Serialize bloom filter using bincode
        let bloom_data =
            bincode::serialize(&self.bloom_filter).map_err(|_| DbError::ManifestCorrupted)?;

        // Wrap Bloom Block with [len][crc][data]
        let bloom_len = bloom_data.len() as u32;
        self.file
            .write_all(&bloom_len.to_le_bytes())
            .map_err(|e| DbError::Io(Arc::new(e)))?;
        let bloom_crc = crc32fast::hash(&bloom_data);
        self.file
            .write_all(&bloom_crc.to_le_bytes())
            .map_err(|e| DbError::Io(Arc::new(e)))?;
        self.file
            .write_all(&bloom_data)
            .map_err(|e| DbError::Io(Arc::new(e)))?;

        // 3. Write Footer
        let mut footer_buffer = Vec::new();
        footer_buffer
            .write_all(&index_offset.to_le_bytes())
            .unwrap();
        footer_buffer
            .write_all(&bloom_offset.to_le_bytes())
            .unwrap();

        let footer_crc = crc32fast::hash(&footer_buffer);

        self.file
            .write_all(&footer_buffer)
            .map_err(|e| DbError::Io(Arc::new(e)))?;
        self.file
            .write_all(&footer_crc.to_le_bytes())
            .map_err(|e| DbError::Io(Arc::new(e)))?;
        self.file
            .write_all(&MAGIC_NUMBER_BYTES)
            .map_err(|e| DbError::Io(Arc::new(e)))?;

        self.file.flush().map_err(|e| DbError::Io(Arc::new(e)))?;
        self.file
            .get_ref()
            .sync_all()
            .map_err(|e| DbError::Io(Arc::new(e)))?;

        let file_size = self
            .file
            .stream_position()
            .map_err(|e| DbError::Io(Arc::new(e)))? as u32;

        Ok(crate::data_stores::manifest::FileMetadata {
            file_id: 0, // Placeholder, caller must set
            file_size,
            min_key,
            max_key,
        })
    }
}

impl Sstable {
    pub fn new(path: &std::path::Path) -> Result<Self, DbError> {
        let id: u32 = path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.parse().ok())
            .ok_or(DbError::ManifestCorrupted)?; // Should be Sstable ID error

        let mut file = File::open(path).map_err(|e| DbError::Io(Arc::new(e)))?;
        let file_len = file.metadata().map_err(|e| DbError::Io(Arc::new(e)))?.len();

        if file_len < 24 {
            // Footer size
            return Err(DbError::ManifestCorrupted); // Should provide SstableCorrupted
        }

        // Read Footer
        file.seek(SeekFrom::Start(file_len - 24))
            .map_err(|e| DbError::Io(Arc::new(e)))?;
        let mut footer_buf = [0u8; 24];
        file.read_exact(&mut footer_buf)
            .map_err(|e| DbError::Io(Arc::new(e)))?;

        // Parse Footer
        let index_offset = u64::from_le_bytes(footer_buf[0..8].try_into().unwrap());
        let bloom_offset = u64::from_le_bytes(footer_buf[8..16].try_into().unwrap());
        let stored_checksum = u32::from_le_bytes(footer_buf[16..20].try_into().unwrap());
        let magic = &footer_buf[20..24];

        if magic != MAGIC_NUMBER_BYTES {
            return Err(DbError::ManifestCorrupted);
        }

        let computed_checksum = crc32fast::hash(&footer_buf[0..16]);
        if computed_checksum != stored_checksum {
            return Err(DbError::ManifestCorrupted);
        }

        // Read Bloom Filter Block
        // We know offset.
        let bloom = Self::read_block_with_crc_at(&file, bloom_offset)?;
        let bloom_filter: BloomFilter =
            bincode::deserialize(&bloom).map_err(|_| DbError::ManifestCorrupted)?;

        // Read Index Block
        let index_data = Self::read_block_with_crc_at(&file, index_offset)?;
        let mut index_cursor = std::io::Cursor::new(index_data);

        // Parse Index Entry: min-key, max-key
        let sst_min_key = Key::decode(&mut index_cursor)?;
        let sst_max_key = Key::decode(&mut index_cursor)?;

        let mut index = Vec::new();
        // The remaining data is entries.
        while index_cursor.position() < index_cursor.get_ref().len() as u64 {
            let block_min = Key::decode(&mut index_cursor)?;
            let block_max = Key::decode(&mut index_cursor)?;
            let mut off_buf = [0u8; 8];
            index_cursor
                .read_exact(&mut off_buf)
                .map_err(|e| DbError::Io(Arc::new(e)))?;
            let offset = u64::from_le_bytes(off_buf);
            index.push((block_min, block_max, offset));
        }

        Ok(Self {
            id,
            file,
            index,
            bloom_filter,
            min_key: sst_min_key,
            max_key: sst_max_key,
        })
    }

    fn read_block_with_crc_at(file: &File, offset: u64) -> Result<Vec<u8>, DbError> {
        let mut len_buf = [0u8; 4];
        file.read_exact_at(&mut len_buf, offset)
            .map_err(|e| DbError::Io(Arc::new(e)))?;
        let len = u32::from_le_bytes(len_buf) as usize;

        let mut crc_buf = [0u8; 4];
        file.read_exact_at(&mut crc_buf, offset + 4)
            .map_err(|e| DbError::Io(Arc::new(e)))?;
        let stored_crc = u32::from_le_bytes(crc_buf);

        let mut data = vec![0u8; len];
        file.read_exact_at(&mut data, offset + 8)
            .map_err(|e| DbError::Io(Arc::new(e)))?;

        let computed_crc = crc32fast::hash(&data);
        if computed_crc != stored_crc {
            return Err(DbError::ManifestCorrupted);
        }

        Ok(data)
    }

    pub fn search(&self, key_bytes: &[u8]) -> Result<Option<Value>, DbError> {
        // 1. Check Bloom Filter (On User Key Bytes)
        if !self.bloom_filter.check(key_bytes) {
            return Ok(None);
        }

        // 2. Binary Search Index
        // Key is byte-sorted ASC, LSN-sorted DESC.
        // We want the latest version. Latest version is (Key, MAX_LSN) (effectively).
        // It is "smaller" than (Key, 0).
        // Range for KeyA is [(KeyA, MAX) ... (KeyA, 0)].

        let target = Key::new(key_bytes, u64::MAX);

        // We want the first block where max >= target.
        // If max < target, then all keys in block are "smaller" than target.
        // Since target is the "smallest positive KeyA", being smaller means
        // the block contains keys strictly before KeyA (e.g. Key0).
        // So we scan until we find a block that *might* overlap.

        let idx = self.index.partition_point(|(_, max, _)| max < &target);

        if idx < self.index.len() {
            let (min, _, offset) = &self.index[idx];
            // Check if min > target's end range?
            // target end is (KeyA, 0).
            // Basically check if min.bytes > key_bytes.
            // If min.bytes > key_bytes, then the block starts after our key.
            if min.bytes.as_ref() > key_bytes {
                return Ok(None);
            }

            return self.search_block(key_bytes, *offset);
        }

        Ok(None)
    }

    fn search_block(&self, key_bytes: &[u8], offset: u64) -> Result<Option<Value>, DbError> {
        let block_data = Self::read_block_with_crc_at(&self.file, offset)?;

        let mut cursor = std::io::Cursor::new(block_data);

        // Linear scan
        while cursor.position() < cursor.get_ref().len() as u64 {
            let k = Key::decode(&mut cursor)?;
            let v = Value::decode(&mut cursor)?;

            if k.bytes.as_ref() == key_bytes {
                // Found match. Since sorted, first match is latest LSN.
                return Ok(Some(v));
            }

            // Optimization: If k.bytes > key_bytes, we can stop.
            if k.bytes.as_ref() > key_bytes {
                return Ok(None);
            }
        }

        Ok(None)
    }

    pub fn min_key(&self) -> &Key {
        &self.min_key
    }

    pub fn max_key(&self) -> &Key {
        &self.max_key
    }
}

pub(crate) struct BlockIterator {
    cursor: std::io::Cursor<Vec<u8>>,
}

impl BlockIterator {
    fn new(data: Vec<u8>) -> Self {
        Self {
            cursor: std::io::Cursor::new(data),
        }
    }
}

impl Iterator for BlockIterator {
    type Item = Result<(Key, Value), DbError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor.position() >= self.cursor.get_ref().len() as u64 {
            return None;
        }

        let key = match Key::decode(&mut self.cursor) {
            Ok(k) => k,
            Err(e) => return Some(Err(e)),
        };

        let val = match Value::decode(&mut self.cursor) {
            Ok(v) => v,
            Err(e) => return Some(Err(e)),
        };

        Some(Ok((key, val)))
    }
}

pub(crate) struct SstableIterator {
    sstable: Arc<Sstable>,
    current_block_idx: usize,
    current_block_iter: Option<BlockIterator>,
}

impl SstableIterator {
    pub fn new(sstable: Arc<Sstable>) -> Self {
        Self {
            sstable,
            current_block_idx: 0,
            current_block_iter: None,
        }
    }

    fn ensure_block_loaded(&mut self) -> Result<bool, DbError> {
        if self.current_block_iter.as_ref().map_or(true, |i| {
            i.cursor.position() >= i.cursor.get_ref().len() as u64
        }) {
            // Need next block
            if self.current_block_idx >= self.sstable.index.len() {
                return Ok(false); // EOF
            }

            let (_, _, offset) = self.sstable.index[self.current_block_idx];
            let data = Sstable::read_block_with_crc_at(&self.sstable.file, offset)?;
            self.current_block_iter = Some(BlockIterator::new(data));
            self.current_block_idx += 1;
        }
        Ok(true)
    }
}

impl Iterator for SstableIterator {
    type Item = Result<(Key, Value), DbError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.ensure_block_loaded() {
            Ok(has_more) => {
                if !has_more {
                    return None;
                }
            }
            Err(e) => return Some(Err(e)),
        }

        self.current_block_iter.as_mut().unwrap().next()
    }
}

#[cfg(test)]
#[path = "sstable_tests.rs"]
mod sstable_tests;
