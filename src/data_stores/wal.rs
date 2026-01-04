use crate::{
    data_stores::{Loggable, key::Key, value::Value},
    err::DbError,
};
use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
};

pub(crate) const WAL_EXTENSION: &str = ".wal";
pub(crate) const WAL_MAGIC: u32 = 0xCAFE_BABE;

#[derive(Debug, Clone)]
pub struct Wal<S = Writable> {
    state: S,
    pub(crate) id: u32,
}

/// A proof that the WAL write has been persisted.
/// This type uses the affine type pattern: it cannot be constructed
/// outside of this module, ensuring that only a successful WAL write
/// can produce it.
pub mod wal_states {
    use std::fs::File;
    use std::io::BufWriter;
    use std::sync::{Arc, Mutex};

    /// State for a WAL that is active and being written to.
    #[derive(Debug, Clone)]
    pub struct Writable {
        pub(crate) file: Arc<Mutex<BufWriter<File>>>,
    }

    /// State for a WAL that is old, immutable, and only used for recovery.
    #[derive(Debug, Clone)]
    pub struct ReadOnly {
        pub(crate) file: Arc<File>,
    }
}

pub use wal_states::{ReadOnly, Writable};
pub(crate) struct WalReceipt;

struct CrcReader<R> {
    inner: R,
    hasher: crc32fast::Hasher,
}

impl<R: std::io::Read> CrcReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            hasher: crc32fast::Hasher::new(),
        }
    }
}

impl<R: std::io::Read> std::io::Read for CrcReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.hasher.update(&buf[..n]);
        Ok(n)
    }
}

struct HasherWriter<'a> {
    hasher: &'a mut crc32fast::Hasher,
}

impl<'a> std::io::Write for HasherWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.hasher.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Wal<Writable> {
    /// Opens a WAL file.
    /// If the file does not exist, it is created and initialized with the magic number.
    /// If it exists, it is verified against the magic number and opened for appending.
    pub fn open(base_dir: &std::path::Path, wal_id: u32) -> Result<Self, DbError> {
        let path = base_dir.join(format!("{:05}{WAL_EXTENSION}", wal_id));
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| DbError::Io(Arc::new(e)))?;

        let len = file.metadata().map_err(|e| DbError::Io(Arc::new(e)))?.len();

        if len == 0 {
            // New file: Write Magic
            file.write_all(&WAL_MAGIC.to_le_bytes())
                .map_err(|e| DbError::Io(Arc::new(e)))?;
            file.sync_all().map_err(|e| DbError::Io(Arc::new(e)))?;
        } else {
            // Existing file: Validate Magic
            let mut magic_buf = [0u8; std::mem::size_of_val(&WAL_MAGIC)];
            {
                use std::io::{Read, Seek};
                file.seek(std::io::SeekFrom::Start(0))
                    .map_err(|e| DbError::Io(Arc::new(e)))?;
                file.read_exact(&mut magic_buf)
                    .map_err(|e| DbError::Io(Arc::new(e)))?;
            }
            let magic = u32::from_le_bytes(magic_buf);
            if magic != WAL_MAGIC {
                return Err(DbError::DataCorrupted(
                    "Invalid WAL Magic in mutable WAL".to_string(),
                ));
            }
        }

        Ok(Self {
            state: Writable {
                file: Arc::new(Mutex::new(BufWriter::new(file))),
            },
            id: wal_id,
        })
    }
}

impl Wal<ReadOnly> {
    pub(crate) fn open(path: PathBuf) -> Result<Self, DbError> {
        if !path.exists() {
            return Err(DbError::from(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("WAL file not found: {:?}", path),
            )));
        }

        let file = OpenOptions::new()
            .read(true)
            .open(&path)
            .map_err(|e| DbError::Io(Arc::new(e)))?;

        let id = path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(0); // If valid WAL file, this should work. If not, 0 fallback or error?
        // Ideally error but `open` signature is generic DbError.

        Ok(Self {
            state: ReadOnly {
                file: Arc::new(file),
            },
            id,
        })
    }

    pub(crate) fn try_iter(&self) -> Result<WalIterator, DbError> {
        // Use try_clone to get an independent handle (though offset sharing applies to FD duplication,
        // we assume single threaded iteration per WAL instance).
        // Actually, with `BufReader`, we read sequentially.
        // If we clone the File, we get a new struct `File` but same underlying description.
        let mut file = self
            .state
            .file
            .try_clone()
            .map_err(|e| DbError::Io(Arc::new(e)))?;

        // Validate Magic
        let mut magic_buf = [0u8; std::mem::size_of_val(&WAL_MAGIC)];
        use std::io::Read;
        file.read_exact(&mut magic_buf)
            .map_err(|e| DbError::Io(Arc::new(e)))?;
        let magic = u32::from_le_bytes(magic_buf);
        if magic != WAL_MAGIC {
            return Err(DbError::DataCorrupted("Invalid WAL Magic".to_string()));
        }

        Ok(WalIterator {
            reader: std::io::BufReader::new(file),
        })
    }
}

impl Wal<Writable> {
    /// Returns a lock on the underlying file writer.
    pub(crate) fn lock(&self) -> std::sync::MutexGuard<'_, std::io::BufWriter<std::fs::File>> {
        self.state.file.lock().unwrap()
    }

    /// Encodes a key-value pair into the WAL format.
    /// Format:
    /// CRC (4 bytes)
    /// Key (Loggable)
    /// Value (Loggable)
    pub(crate) fn encode_entry(
        writer: &mut impl std::io::Write,
        key: &Key,
        value: &Value,
    ) -> std::io::Result<()> {
        // 1. Calculate CRC
        let mut crc_hasher = crc32fast::Hasher::new();
        {
            let mut writer = HasherWriter {
                hasher: &mut crc_hasher,
            };
            key.encode(&mut writer)?;
            value.encode(&mut writer)?;
        }
        let crc = crc_hasher.finalize();

        // 2. Write CRC
        crc.encode(writer)?;

        // 3. Write Key
        key.encode(writer)?;

        // 4. Write Value
        value.encode(writer)
    }

    pub(crate) fn write(&self, bytes: &[u8]) -> Result<WalReceipt, DbError> {
        let mut writer = self.state.file.lock().unwrap();
        writer
            .write_all(bytes)
            .map_err(|e| DbError::Io(Arc::new(e)))?;
        writer.flush().map_err(|e| DbError::Io(Arc::new(e)))?;
        writer
            .get_mut()
            .sync_all()
            .map_err(|e| DbError::Io(Arc::new(e)))?;

        // In a real implementation, we'd track the actual LSN.
        // For now we just return a receipt.
        Ok(WalReceipt)
    }
}

#[allow(dead_code)]
pub(crate) struct WalIterator {
    reader: std::io::BufReader<File>,
}

impl Iterator for WalIterator {
    type Item = Result<(crate::data_stores::key::Key, Value), DbError>;

    fn next(&mut self) -> Option<Self::Item> {
        use crate::data_stores::key::Key;
        use std::io::Read;

        // Read CRC (4 bytes)
        let mut crc_buf = [0u8; 4];
        if let Err(e) = self.reader.read_exact(&mut crc_buf) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return None;
            }
            return Some(Err(DbError::Io(Arc::new(e))));
        }
        let stored_crc = u32::from_le_bytes(crc_buf);

        let mut crc_reader = CrcReader::new(&mut self.reader);

        // Decode Key
        let key = match Key::decode(&mut crc_reader) {
            Ok(k) => k,
            Err(DbError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => return None, // Truncation during Key
            Err(e) => return Some(Err(e)),
        };

        // Decode Value
        let value = match Value::decode(&mut crc_reader) {
            Ok(v) => v,
            Err(DbError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => return None, // Truncation during Value
            Err(e) => return Some(Err(e)),
        };

        let calculated_crc = crc_reader.hasher.finalize();

        if calculated_crc != stored_crc {
            return Some(Err(DbError::DataCorrupted("CRC mismatch".to_string())));
        }

        Some(Ok((key, value)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_stores::wal::wal_states::{ReadOnly, Writable};
    use crate::data_stores::{key::Key, value::Value};
    use std::io::Seek;

    #[test]
    fn test_wal_write_read_correctness() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("00001.wal");

        let k1 = Key::new(b"key1", 1);
        let v1 = Value::new(b"value1");

        {
            let wal = Wal::<Writable>::open(dir.path(), 1).unwrap();
            let mut writer = wal.lock();
            Wal::<Writable>::encode_entry(&mut *writer, &k1, &v1).unwrap();
        }

        {
            let wal = Wal::<ReadOnly>::open(wal_path).unwrap();
            let mut iter = wal.try_iter().unwrap();
            let (rk1, rv1) = iter.next().unwrap().unwrap();

            assert_eq!(rk1.lsn.load(std::sync::atomic::Ordering::Relaxed), 1);
            match rv1 {
                Value::Bytes(b) => assert_eq!(&*b, b"value1"),
                _ => panic!("Wrong value type"),
            }
            assert!(iter.next().is_none());
        }
    }

    #[test]
    fn test_wal_crc_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("00002.wal");

        let k1 = Key::new(b"key1", 100);
        let v1 = Value::new(b"value1");

        // Write valid entry
        {
            let wal = Wal::<Writable>::open(dir.path(), 2).unwrap();
            let mut writer = wal.lock();
            Wal::<Writable>::encode_entry(&mut *writer, &k1, &v1).unwrap();
        }

        // Corrupt the file
        {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .open(&wal_path)
                .unwrap();
            // Skip past CRC (4 bytes) and corrupt data
            file.seek(std::io::SeekFrom::Start(4)).unwrap();
            file.write_all(b"\xFF").unwrap();
        }

        // Try read
        {
            let wal = Wal::<ReadOnly>::open(wal_path).unwrap();
            let mut iter = wal.try_iter().unwrap();
            let res = iter.next().unwrap();
            // Should be ChecksumMismatch error/DataCorrupted
            assert!(
                matches!(res, Err(DbError::DataCorrupted(msg)) if msg.contains("CRC mismatch"))
            );
        }
    }

    #[test]
    fn test_wal_truncated_entry() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("00003.wal");

        let k1 = Key::new(b"key1", 100);
        let v1 = Value::new(b"value1");

        {
            let wal = Wal::<Writable>::open(dir.path(), 3).unwrap();
            let mut writer = wal.lock();
            Wal::<Writable>::encode_entry(&mut *writer, &k1, &v1).unwrap();
        }

        // Truncate file in the middle of data
        let meta = std::fs::metadata(&wal_path).unwrap();
        let len = meta.len();
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&wal_path)
            .unwrap();
        file.set_len(len - 2).unwrap();

        {
            let wal = Wal::<ReadOnly>::open(wal_path).unwrap();
            match wal.try_iter() {
                Ok(mut iter) => {
                    // Truncation usually means next() returns None or Error depending on where it cuts.
                    // If it cuts in header, might be Error or None.
                    // My iterator implementation returns None on UnexpectedEof during Key/Value decode.
                    // Let's verify.
                    // Key decode -> unexpected EOF -> None.
                    // Value decode -> unexpected EOF -> None.
                    // So truncation should result in None (clean stop at last valid entry).
                    // But wait, if we truncate the ONLY entry, it should return None immediately on first next().
                    assert!(iter.next().is_none());
                }
                Err(_) => {
                    // Failed to open iterator (unlikely here)
                }
            }
        }
    }

    #[test]
    fn test_wal_truncated_payload() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("00004.wal");

        let k1 = Key::new(b"key1", 100);
        let v1 = Value::new(b"value1");

        {
            let wal = Wal::<Writable>::open(dir.path(), 4).unwrap();
            let mut writer = wal.lock();
            Wal::<Writable>::encode_entry(&mut *writer, &k1, &v1).unwrap();
        }

        // Truncate bytes
        let meta = std::fs::metadata(&wal_path).unwrap();
        let len = meta.len();
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&wal_path)
            .unwrap();
        // Cut off last byte of value
        file.set_len(len - 1).unwrap();

        {
            let wal = Wal::<ReadOnly>::open(wal_path).unwrap();
            let mut iter = wal.try_iter().unwrap();
            // Should be None (treated as incomplete entry = end of log)
            assert!(iter.next().is_none());
        }
    }

    #[test]
    fn test_wal_persistence_across_restarts() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("00005.wal");

        let k1 = Key::new(b"key1", 101);
        let v1 = Value::new(b"val1");

        {
            let wal = Wal::<Writable>::open(dir.path(), 5).unwrap();
            let mut writer = wal.lock();
            Wal::<Writable>::encode_entry(&mut *writer, &k1, &v1).unwrap();
        }

        // Re-open as writable (append mode simulation)
        {
            let wal = Wal::<Writable>::open(dir.path(), 5).unwrap();
            let k2 = Key::new(b"key2", 102);
            let v2 = Value::new(b"val2");
            let mut writer = wal.lock();
            Wal::<Writable>::encode_entry(&mut *writer, &k2, &v2).unwrap();
        }

        // Read all
        {
            let wal = Wal::<ReadOnly>::open(wal_path).unwrap();
            let mut iter = wal.try_iter().unwrap();

            let (rk1, rv1) = iter.next().unwrap().unwrap();
            assert_eq!(rk1.lsn.load(std::sync::atomic::Ordering::Relaxed), 101);
            match rv1 {
                Value::Bytes(b) => assert_eq!(&*b, b"val1"),
                _ => panic!("Wrong value type"),
            }

            let (rk2, rv2) = iter.next().unwrap().unwrap();
            assert_eq!(rk2.lsn.load(std::sync::atomic::Ordering::Relaxed), 102);
            match rv2 {
                Value::Bytes(b) => assert_eq!(&*b, b"val2"),
                _ => panic!("Wrong value type"),
            }

            assert!(iter.next().is_none());
        }
    }
}
