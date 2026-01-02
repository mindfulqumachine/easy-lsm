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
use crate::data_stores::key::Key;

pub(crate) struct Sstable {
    #[allow(dead_code)]
    file_pointer: std::fs::File,
    #[allow(dead_code)]
    bloom_filter: Vec<u8>,
    #[allow(dead_code)]
    sstable_min_key: Key,
    #[allow(dead_code)]
    sstable_max_key: Key,

    // Each elem represents (min_key, max_key, block_starting_offset)
    #[allow(dead_code)]
    block_level_min_max_keys: Vec<(Key, Key, usize)>,
}
