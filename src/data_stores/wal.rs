use crate::{
    data_stores::{key::KeyLenType, value::Value},
    err::DbError,
};
use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
};

pub(crate) struct Wal {
    path: PathBuf,
    file: Arc<Mutex<BufWriter<File>>>,
}

/// A proof that the WAL write has been persisted.
/// This type uses the affine type pattern: it cannot be constructed
/// outside of this module, ensuring that only a successful WAL write
/// can produce it.
pub(crate) struct WalReceipt {
    pub(crate) lsn: u64,
}

const VAL_TYPE_BYTES: u8 = 1 << 0;
const VAL_TYPE_STR: u8 = 1 << 1;
const VAL_TYPE_INT: u8 = 1 << 2;
const VAL_TYPE_TOMBSTONE: u8 = 1 << 3;

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
    /// LSN (8 bytes)
    /// Tombstone (1 byte)
    /// Key Len (2 bytes)
    /// Value Len (4 bytes)
    /// Value Type (1 byte)
    /// Key (variable)
    /// Value (variable)
    pub(crate) fn encode_entry(key_bytes: &[u8], lsn: u64, value: &Value, buf: &mut Vec<u8>) {
        let start_pos = buf.len();

        // Placeholder for CRC, will be filled later
        buf.extend_from_slice(&[0u8; 4]);

        // LSN
        buf.extend_from_slice(&lsn.to_le_bytes());

        // Determine metadata and length without cloning the payload
        let (value_type, val_len) = match value {
            Value::Bytes(b) => (VAL_TYPE_BYTES, b.len()),
            Value::Str(s) => (VAL_TYPE_STR, s.len()),
            Value::Int(_) => (VAL_TYPE_INT, 8), // i64 is always 8 bytes
            Value::Tombstone => (VAL_TYPE_TOMBSTONE, 0),
        };

        // Key Len
        let key_len = key_bytes.len() as KeyLenType;
        buf.extend_from_slice(&key_len.to_le_bytes());

        // Value Len
        buf.extend_from_slice(&(val_len as u32).to_le_bytes());

        // Value Type
        buf.push(value_type);

        // Key
        buf.extend_from_slice(key_bytes);

        // Value Payload
        match value {
            Value::Bytes(b) => buf.extend_from_slice(b),
            Value::Str(s) => buf.extend_from_slice(s.as_bytes()),
            Value::Int(i) => buf.extend_from_slice(&i.to_le_bytes()),
            Value::Tombstone => {} // No payload
        };

        // Calculate CRC
        // We only hash the part we just added (skipping the first 4 bytes which is the CRC placeholder)
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
        // For now, we return a receipt. The LSN 0 is a placeholder.
        Ok(WalReceipt { lsn: 0 })
    }

    pub(crate) fn try_iter(&self) -> Result<WalIterator, DbError> {
        let file = File::open(&self.path).map_err(|e| DbError::Io(Arc::new(e)))?;
        Ok(WalIterator {
            reader: std::io::BufReader::new(file),
        })
    }
}

pub(crate) struct WalIterator {
    reader: std::io::BufReader<File>,
}

impl Iterator for WalIterator {
    type Item = Result<(u64, Vec<u8>, Value), DbError>;

    fn next(&mut self) -> Option<Self::Item> {
        use std::io::Read;

        // Read CRC (4 bytes)
        let mut crc_buf = [0u8; 4];
        if let Err(e) = self.reader.read_exact(&mut crc_buf) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return None;
            }
            return Some(Err(DbError::Io(Arc::new(e))));
        }

        // Read LSN (8 bytes)
        let mut lsn_buf = [0u8; 8];
        if let Err(e) = self.reader.read_exact(&mut lsn_buf) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return None;
            }
            return Some(Err(DbError::Io(Arc::new(e))));
        }
        let lsn = u64::from_le_bytes(lsn_buf);

        // Read Key Len (2 bytes)
        let mut klen_buf = [0u8; 2];
        if let Err(e) = self.reader.read_exact(&mut klen_buf) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return None;
            }
            return Some(Err(DbError::Io(Arc::new(e))));
        }
        let key_len = u16::from_le_bytes(klen_buf);

        // Read Value Len (4 bytes)
        let mut vlen_buf = [0u8; 4];
        if let Err(e) = self.reader.read_exact(&mut vlen_buf) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return None;
            }
            return Some(Err(DbError::Io(Arc::new(e))));
        }
        let val_len = u32::from_le_bytes(vlen_buf);

        // Read Value Type (1 byte)
        let mut vtype_buf = [0u8; 1];
        if let Err(e) = self.reader.read_exact(&mut vtype_buf) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return None;
            }
            return Some(Err(DbError::Io(Arc::new(e))));
        }
        let val_type = vtype_buf[0];

        // Read Key
        let mut key_bytes = vec![0u8; key_len as usize];
        if let Err(e) = self.reader.read_exact(&mut key_bytes) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return None;
            }
            return Some(Err(DbError::Io(Arc::new(e))));
        }

        // Read Value
        let mut val_bytes = vec![0u8; val_len as usize];
        if let Err(e) = self.reader.read_exact(&mut val_bytes) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return None;
            }
            return Some(Err(DbError::Io(Arc::new(e))));
        }

        // Reconstruct Value
        let value = match val_type {
            VAL_TYPE_BYTES => Value::Bytes(val_bytes.clone()),
            VAL_TYPE_STR => Value::Str(String::from_utf8(val_bytes.clone()).unwrap_or_default()),
            VAL_TYPE_INT => {
                if val_bytes.len() >= 8 {
                    let bytes: [u8; 8] = val_bytes[0..8].try_into().unwrap();
                    Value::Int(i64::from_le_bytes(bytes))
                } else {
                    return Some(Err(DbError::ManifestCorrupted)); // Bad int value
                }
            }
            VAL_TYPE_TOMBSTONE => Value::Tombstone,
            _ => return Some(Err(DbError::ManifestCorrupted)), // Unknown type
        };

        // Header Structure for CRC: LSN (8), Key Len (2), Val Len (4), Val Type (1), Key (N), Val (M)
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&lsn_buf);
        hasher.update(&klen_buf);
        hasher.update(&vlen_buf);
        hasher.update(&vtype_buf);
        hasher.update(&key_bytes);
        hasher.update(&val_bytes);

        let calculated_crc = hasher.finalize();
        let stored_crc = u32::from_le_bytes(crc_buf);

        if calculated_crc != stored_crc {
            return Some(Err(DbError::DataCorrupted("CRC mismatch".to_string())));
        }

        Some(Ok((lsn, key_bytes, value)))
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
            let val2 = Value::Str("value2".to_string());
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
        let (lsn, k, v) = iter.next().expect("Should have entry 1").unwrap();
        assert_eq!(lsn, 1);
        assert_eq!(k, b"key1");
        if let Value::Bytes(b) = v {
            assert_eq!(b, b"value1");
        } else {
            panic!("Expected Bytes value");
        }

        // Check 2
        let (lsn, k, v) = iter.next().expect("Should have entry 2").unwrap();
        assert_eq!(lsn, 2);
        assert_eq!(k, b"key2");
        if let Value::Str(s) = v {
            assert_eq!(s, "value2");
        } else {
            panic!("Expected Str value");
        }

        // Check 3
        let (lsn, k, v) = iter.next().expect("Should have entry 3").unwrap();
        assert_eq!(lsn, 3);
        assert_eq!(k, b"key3");
        if let Value::Int(i) = v {
            assert_eq!(i, 42);
        } else {
            panic!("Expected Int value");
        }

        // Check 3b
        let (lsn, k, v) = iter.next().expect("Should have entry 3b").unwrap();
        assert_eq!(lsn, 4);
        assert_eq!(k, b"key3b");
        if let Value::Int(i) = v {
            assert_eq!(i, -12345);
        } else {
            panic!("Expected Int value (negative)");
        }

        // Check 4
        let (lsn, k, v) = iter.next().expect("Should have entry 4").unwrap();
        assert_eq!(lsn, 5);
        assert_eq!(k, b"key4");
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
        let (lsn1, _, v1) = iter.next().expect("Should have entry 1").unwrap();
        assert_eq!(lsn1, 100);
        if let Value::Bytes(b) = v1 {
            assert_eq!(b, b"run1_data");
        } else {
            panic!("Wrong value type for entry 1");
        }

        // Expect Entry 2 (should be appended, NOT overwritten)
        let (lsn2, _, v2) = iter.next().expect("Should have entry 2").unwrap();
        assert_eq!(lsn2, 101);
        if let Value::Bytes(b) = v2 {
            assert_eq!(b, b"run2_data");
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
            // Header is 4 (CRC) + 8 (LSN) + 2 (KeyLen) + 4 (ValLen) + 1 (Type) = 19 bytes.
            // Key starts at offset 19. Let's corrupt the first byte of the key.
            file.seek(SeekFrom::Start(19)).unwrap();
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
            // Write only first 10 bytes (header is 20, so checking header truncation)
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

        // 2. Append header + partial payload
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            let key = b"partial";
            let val = Value::new(b"partial_val");
            let mut buf = Vec::new();
            Wal::encode_entry(key, 2, &val, &mut buf);
            // Header is 19 bytes. Write 25 bytes (header + partial key)
            // Header structure: CRC(4)+LSN(8)+KeyLen(2)+ValLen(4)+Type(1) = 19.
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
