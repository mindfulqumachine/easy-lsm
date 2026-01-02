use crate::{
    data_stores::{Loggable, value::Value},
    err::DbError,
};
use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
};

pub(crate) struct Wal {
    #[allow(dead_code)]
    path: PathBuf,
    file: Arc<Mutex<BufWriter<File>>>,
}

/// A proof that the WAL write has been persisted.
/// This type uses the affine type pattern: it cannot be constructed
/// outside of this module, ensuring that only a successful WAL write
/// can produce it.
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

impl Wal {
    pub(crate) fn new(path: PathBuf) -> Result<Self, DbError> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| DbError::Io(Arc::new(e)))?;
        Ok(Self {
            path,
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    /// Encodes a key-value pair into the WAL format.
    /// Format:
    /// CRC (4 bytes)
    /// Key (Self-Encoded)
    /// Value (Self-Encoded)
    pub(crate) fn encode_entry(key: &[u8], lsn: u64, value: &Value, buf: &mut Vec<u8>) {
        use crate::data_stores::key::Key;
        let start_pos = buf.len();

        // Placeholder for CRC
        buf.extend_from_slice(&[0u8; 4]);

        // Key
        let k = Key::new(key, lsn);
        k.encode(buf).unwrap(); // Vec<u8> write impl shouldn't fail

        // Value
        value.encode(buf).unwrap();

        // Calculate CRC
        let checksum = crc32fast::hash(&buf[start_pos + 4..]);
        let crc_bytes = checksum.to_le_bytes();

        // Write CRC back to placeholder
        buf[start_pos..start_pos + 4].copy_from_slice(&crc_bytes);
    }

    pub(crate) fn write(&self, bytes: &[u8]) -> Result<WalReceipt, DbError> {
        let mut writer = self.file.lock().unwrap();
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

    #[allow(dead_code)]
    pub(crate) fn try_iter(&self) -> Result<WalIterator, DbError> {
        let file = File::open(&self.path).map_err(|e| DbError::Io(Arc::new(e)))?;
        Ok(WalIterator {
            reader: std::io::BufReader::new(file),
        })
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
    use std::fs;

    #[test]
    fn test_wal_write_read_correctness() {
        // Create a temporary file path
        let dir = std::env::temp_dir();
        let path = dir.join("test_wal_correctness.log");
        if path.exists() {
            fs::remove_file(&path).unwrap();
        }

        // Scope to ensure Wal drops file handle/flush
        {
            let wal = Wal::new(path.clone()).unwrap();

            // 1. Write Bytes
            let key1 = b"key1";
            let val1 = Value::new(b"value1");
            let mut buf = Vec::new();
            Wal::encode_entry(key1, 1, &val1, &mut buf);
            wal.write(&buf).unwrap();

            // 2. Write String
            let key2 = b"key2";
            let val2 = Value::Str(Arc::from("value2"));
            let mut buf = Vec::new();
            Wal::encode_entry(key2, 2, &val2, &mut buf);
            wal.write(&buf).unwrap();

            // 3. Write Int
            let key3 = b"key3";
            let val3 = Value::Int(42);
            let mut buf = Vec::new();
            Wal::encode_entry(key3, 3, &val3, &mut buf);
            wal.write(&buf).unwrap();

            // 3b. Write Negative Int
            let key3b = b"key3b";
            let val3b = Value::Int(-12345);
            let mut buf = Vec::new();
            Wal::encode_entry(key3b, 4, &val3b, &mut buf);
            wal.write(&buf).unwrap();

            // 4. Write Tombstone
            let key4 = b"key4";
            let val4 = Value::Tombstone;
            let mut buf = Vec::new();
            Wal::encode_entry(key4, 5, &val4, &mut buf);
            wal.write(&buf).unwrap();
        }

        // Re-open and verify
        let wal = Wal::new(path.clone()).unwrap();
        let mut iter = wal.try_iter().unwrap();

        // Check 1
        // Check 1
        let (k, v) = iter.next().expect("Should have entry 1").unwrap();
        assert_eq!(k.lsn.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(&k.bytes[..], b"key1");
        if let Value::Bytes(b) = v {
            assert_eq!(&b[..], b"value1");
        } else {
            panic!("Expected Bytes value");
        }

        // Check 2
        // Check 2
        let (k, v) = iter.next().expect("Should have entry 2").unwrap();
        assert_eq!(k.lsn.load(std::sync::atomic::Ordering::Relaxed), 2);
        assert_eq!(&k.bytes[..], b"key2");
        if let Value::Str(s) = v {
            assert_eq!(&s[..], "value2");
        } else {
            panic!("Expected Str value");
        }

        // Check 3
        // Check 3
        let (k, v) = iter.next().expect("Should have entry 3").unwrap();
        assert_eq!(k.lsn.load(std::sync::atomic::Ordering::Relaxed), 3);
        assert_eq!(&k.bytes[..], b"key3");
        if let Value::Int(i) = v {
            assert_eq!(i, 42);
        } else {
            panic!("Expected Int value");
        }

        // Check 3b
        // Check 3b
        let (k, v) = iter.next().expect("Should have entry 3b").unwrap();
        assert_eq!(k.lsn.load(std::sync::atomic::Ordering::Relaxed), 4);
        assert_eq!(&k.bytes[..], b"key3b");
        if let Value::Int(i) = v {
            assert_eq!(i, -12345);
        } else {
            panic!("Expected Int value (negative)");
        }

        // Check 4
        // Check 4
        let (k, v) = iter.next().expect("Should have entry 4").unwrap();
        assert_eq!(k.lsn.load(std::sync::atomic::Ordering::Relaxed), 5);
        assert_eq!(&k.bytes[..], b"key4");
        if let Value::Tombstone = v {
            // Match
        } else {
            panic!("Expected Tombstone value");
        }

        assert!(iter.next().is_none());

        // Cleanup
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_wal_persistence_across_restarts() {
        let dir = std::env::temp_dir();
        let path = dir.join("test_wal_persistence.log");
        if path.exists() {
            fs::remove_file(&path).unwrap();
        }

        // Run 1: Create and write Entry 1
        {
            let wal = Wal::new(path.clone()).unwrap();
            let key = b"split_key";
            let val = Value::new(b"run1_data");
            let mut buf = Vec::new();
            Wal::encode_entry(key, 100, &val, &mut buf);
            wal.write(&buf).unwrap();
        }

        // Run 2: Re-open and write Entry 2
        {
            let wal = Wal::new(path.clone()).unwrap();
            let key = b"split_key";
            let val = Value::new(b"run2_data");
            let mut buf = Vec::new();
            Wal::encode_entry(key, 101, &val, &mut buf);
            wal.write(&buf).unwrap();
        }

        // Validation: Read all
        let wal = Wal::new(path.clone()).unwrap();
        let mut iter = wal.try_iter().unwrap();

        // Expect Entry 1
        let (k1, v1) = iter.next().expect("Should have entry 1").unwrap();
        assert_eq!(k1.lsn.load(std::sync::atomic::Ordering::Relaxed), 100);
        if let Value::Bytes(b) = v1 {
            assert_eq!(&b[..], b"run1_data");
        } else {
            panic!("Wrong value type for entry 1");
        }

        // Expect Entry 2 (should be appended, NOT overwritten)
        let (k2, v2) = iter.next().expect("Should have entry 2").unwrap();
        assert_eq!(k2.lsn.load(std::sync::atomic::Ordering::Relaxed), 101);
        if let Value::Bytes(b) = v2 {
            assert_eq!(&b[..], b"run2_data");
        } else {
            panic!("Wrong value type for entry 2");
        }

        assert!(iter.next().is_none());

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_wal_crc_mismatch() {
        use std::io::{Seek, SeekFrom};

        let dir = std::env::temp_dir();
        let path = dir.join("test_wal_crc.log");
        if path.exists() {
            fs::remove_file(&path).unwrap();
        }

        // 1. Write a valid entry
        {
            let wal = Wal::new(path.clone()).unwrap();
            let key = b"key";
            let val = Value::new(b"val");
            let mut buf = Vec::new();
            Wal::encode_entry(key, 1, &val, &mut buf);
            wal.write(&buf).unwrap();
        }

        // 2. Corrupt the data (modify a byte in the key or value)
        {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            // Layout: CRC(4) | Key | Value
            // Key: LSN(8) + Len(2) + Bytes
            // Key Bytes start at 4 + 8 + 2 = 14.
            file.seek(SeekFrom::Start(14)).unwrap();
            file.write_all(b"X").unwrap(); // Original was 'k' (from "key")
        }

        // 3. Verify Error
        let wal = Wal::new(path.clone()).unwrap();
        let mut iter = wal.try_iter().unwrap();

        match iter.next() {
            Some(Err(DbError::DataCorrupted(msg))) => {
                assert_eq!(msg, "CRC mismatch");
            }
            Some(Ok(_)) => panic!("Expected CRC mismatch error, got Ok"),
            Some(Err(e)) => panic!("Expected DataCorrupted error, got {:?}", e),
            None => panic!("Expected entry, got None"),
        }

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_wal_truncated_entry() {
        use std::io::Write;

        let dir = std::env::temp_dir();
        let path = dir.join("test_wal_truncated.log");
        if path.exists() {
            fs::remove_file(&path).unwrap();
        }

        // 1. Write one valid entry
        {
            let wal = Wal::new(path.clone()).unwrap();
            let key = b"valid";
            let val = Value::new(b"valid_val");
            let mut buf = Vec::new();
            Wal::encode_entry(key, 1, &val, &mut buf);
            wal.write(&buf).unwrap();
        }

        // 2. Append half of a second entry
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            let key = b"partial";
            let val = Value::new(b"partial_val");
            let mut buf = Vec::new();
            Wal::encode_entry(key, 2, &val, &mut buf);
            // Write only first 10 bytes (CRC + Partial Key)
            file.write_all(&buf[0..10]).unwrap();
        }

        // 3. Verify interaction
        let wal = Wal::new(path.clone()).unwrap();
        let mut iter = wal.try_iter().unwrap();

        // Should get first entry
        assert!(iter.next().unwrap().is_ok());

        // Should get None for the second (truncated) entry, NOT an error
        match iter.next() {
            None => {} // Correct behavior for truncation
            Some(Err(e)) => panic!("Should treat truncation as EOF, got error: {:?}", e),
            Some(Ok(_)) => panic!("Should not return partial entry"),
        }

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_wal_truncated_payload() {
        // With the new streaming format, "truncated payload" is effectively the same as "truncated entry"
        // because we read Key then Value sequentially. Use the same logic.
        use std::io::Write;

        let dir = std::env::temp_dir();
        let path = dir.join("test_wal_truncated_payload.log");
        if path.exists() {
            fs::remove_file(&path).unwrap();
        }

        // 1. Write one valid entry
        {
            let wal = Wal::new(path.clone()).unwrap();
            let key = b"valid";
            let val = Value::new(b"valid_val");
            let mut buf = Vec::new();
            Wal::encode_entry(key, 1, &val, &mut buf);
            wal.write(&buf).unwrap();
        }

        // 2. Append header + partial payload (Partial Value)
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            let key = b"partial";
            let val = Value::new(b"partial_val");
            let mut buf = Vec::new();
            Wal::encode_entry(key, 2, &val, &mut buf);
            // Key is encoded first fully. Then Value starts.
            // Key size: 8(LSN)+2(Len)+7("partial") = 17 bytes.
            // CRC: 4 bytes.
            // Total before Value = 21 bytes.
            // Write 25 bytes (CRC + Key + Partial Value Header)
            file.write_all(&buf[0..25]).unwrap();
        }

        // 3. Verify interaction
        let wal = Wal::new(path.clone()).unwrap();
        let mut iter = wal.try_iter().unwrap();

        // Should get first entry
        assert!(iter.next().unwrap().is_ok());

        // Should get None for the second (truncated payload), NOT an error
        match iter.next() {
            None => {} // Correct behavior for truncation
            Some(Err(e)) => panic!("Should treat payload truncation as EOF, got error: {:?}", e),
            Some(Ok(_)) => panic!("Should not return partial entry"),
        }

        fs::remove_file(path).unwrap();
    }
}
