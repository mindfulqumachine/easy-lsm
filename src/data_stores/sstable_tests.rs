use crate::data_stores::key::Key;
use crate::data_stores::sstable::{Sstable, SstableWriter};
use crate::data_stores::value::Value;
use crate::err::DbError;
use tempfile::tempdir;

#[test]
fn test_sstable_write_read_cycle() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.sst");

    let mut writer = SstableWriter::new(&file_path).unwrap();

    let mut keys = Vec::new();
    for i in 0..1000u64 {
        let key_bytes = format!("key-{:05}", i).into_bytes();
        let key = Key::new(&key_bytes, i); // Use i as LSN
        let value = Value::new(format!("value-{}", i).as_bytes());
        writer.write(&key, &value).unwrap();
        keys.push((key, value));
    }
    writer.finalize().unwrap();

    let sstable = Sstable::new(&file_path).unwrap();

    for (key, expected_val) in keys {
        let val = sstable
            .search(&key.bytes)
            .unwrap()
            .expect("Key should exist");
        match (val, expected_val) {
            (Value::Bytes(b1), Value::Bytes(b2)) => assert_eq!(b1, b2),
            _ => panic!("Unexpected value type"),
        }
    }
}

#[test]
fn test_sstable_not_found() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test_nf.sst");
    let mut writer = SstableWriter::new(&file_path).unwrap();

    let key = Key::new(b"exists", 1);
    let value = Value::new(b"val");
    writer.write(&key, &value).unwrap();
    writer.finalize().unwrap();

    let mut sstable = Sstable::new(&file_path).unwrap();
    let missing_key = Key::new(b"missing", 1);
    assert!(sstable.search(&missing_key.bytes).unwrap().is_none());
}

#[test]
fn test_sstable_integrity_footer() {
    use std::io::{Seek, SeekFrom, Write};

    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test_corrupt_footer.sst");
    let mut writer = SstableWriter::new(&file_path).unwrap();
    let key = Key::new(b"k", 1);
    let value = Value::new(b"v");
    writer.write(&key, &value).unwrap();
    writer.finalize().unwrap();

    // Corrupt the footer (magic bytes at end)
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(&file_path)
        .unwrap();
    file.seek(SeekFrom::End(-1)).unwrap(); // Last byte of magic
    file.write_all(&[0x00]).unwrap();

    let res = Sstable::new(&file_path);
    assert!(matches!(res, Err(DbError::ManifestCorrupted)));
}

#[test]
fn test_sstable_integrity_block() {
    use std::io::{Read, Seek, SeekFrom, Write};

    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test_corrupt_block.sst");
    let mut writer = SstableWriter::new(&file_path).unwrap();
    let key = Key::new(b"k", 1);
    let value = Value::new(b"v");
    writer.write(&key, &value).unwrap();
    writer.finalize().unwrap();

    // Structure: [len:4][crc:4][data...]
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .read(true)
        .open(&file_path)
        .unwrap();
    file.seek(SeekFrom::Start(8)).unwrap(); // Skip len and crc
    let mut byte = [0u8];
    file.read_exact(&mut byte).unwrap();
    file.seek(SeekFrom::Start(8)).unwrap();
    file.write_all(&[byte[0] ^ 0xFF]).unwrap(); // Flip bits

    let sstable = Sstable::new(&file_path).unwrap();
    let res = sstable.search(&key.bytes);
    assert!(matches!(res, Err(DbError::ManifestCorrupted)));
}

#[test]
fn test_sstable_integrity_index() {
    use std::io::{Read, Seek, SeekFrom, Write};

    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test_corrupt_index.sst");
    let mut writer = SstableWriter::new(&file_path).unwrap();
    let key = Key::new(b"k", 1);
    let value = Value::new(b"v");
    writer.write(&key, &value).unwrap();
    writer.finalize().unwrap();

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .read(true)
        .open(&file_path)
        .unwrap();
    let len = file.metadata().unwrap().len();
    file.seek(SeekFrom::Start(len - 24)).unwrap();
    let mut footer = [0u8; 24];
    file.read_exact(&mut footer).unwrap();

    let index_offset = u64::from_le_bytes(footer[0..8].try_into().unwrap());

    // Corrupt index data (skip len:4, crc:4)
    file.seek(SeekFrom::Start(index_offset + 8)).unwrap();
    let mut byte = [0u8];
    file.read_exact(&mut byte).unwrap();
    file.seek(SeekFrom::Start(index_offset + 8)).unwrap();
    file.write_all(&[byte[0] ^ 0xFF]).unwrap();

    let res = Sstable::new(&file_path);
    assert!(matches!(res, Err(DbError::ManifestCorrupted)));
}
