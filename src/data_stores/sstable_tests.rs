use super::*;
use tempfile::tempdir;

#[test]
fn test_block_iterator() {
    let mut builder = BlockBuilder::new();
    let k1 = Key::new(b"key1", 100);
    // Value::new only takes bytes for Put.
    let v1 = Value::new(b"val1");
    let k2 = Key::new(b"key2", 90);
    // Use enum variant for Delete (Tombstone)
    let v2 = Value::Tombstone;

    builder.add(&k1, &v1).unwrap();
    builder.add(&k2, &v2).unwrap();

    let (block_data, _, _) = builder.flush();
    // Block format: [len: u32][crc: u32][data...]
    let payload = &block_data[8..];
    let mut iter = BlockIterator::new(payload.to_vec());

    let (rk1, rv1) = iter.next().unwrap().unwrap();
    assert_eq!(rk1, k1);
    // Value doesn't derive PartialEq? it derives Debug, Clone.
    // Let's check if we can compare. Value definition has no PartialEq.
    // We should probably check debug string or implement PartialEq.
    // For now, check debug representation.
    assert_eq!(format!("{:?}", rv1), format!("{:?}", v1));

    let (rk2, rv2) = iter.next().unwrap().unwrap();
    assert_eq!(rk2, k2);
    assert_eq!(format!("{:?}", rv2), format!("{:?}", v2));

    assert!(iter.next().is_none());
}

#[test]
fn test_sstable_iterator() {
    let dir = tempdir().unwrap();
    // Convert to string to avoid ownership issues if needed, but PathBuf is fine.
    let path = dir.path().join("00001.sst");

    let mut writer = SstableWriter::new(&path).unwrap();

    // Write enough data to span multiple blocks (assuming 4KB block size)
    let count = 1000;
    for i in 0..count {
        let key = Key::new(format!("key{:05}", i).as_bytes(), 100);
        let val = Value::new(format!("val{:05}", i).as_bytes());
        writer.write(&key, &val).unwrap();
    }
    writer.finalize().unwrap();

    // Read back
    let sst = Sstable::new(&path).unwrap();
    let mut iter = SstableIterator::new(Arc::new(sst));

    for i in 0..count {
        let (k, v) = iter
            .next()
            .expect("Should have item")
            .expect("Should verify");

        let expected_key_bytes = format!("key{:05}", i).into_bytes();
        // k.bytes is Arc<[u8]>. expected is different type.
        // Use slice comparison.
        assert_eq!(&k.bytes[..], &expected_key_bytes[..]);

        let expected_val_bytes = format!("val{:05}", i).into_bytes();
        match v {
            Value::Bytes(b) => assert_eq!(&b[..], &expected_val_bytes[..]),
            _ => panic!("Expected Value::Bytes"),
        }
    }

    assert!(iter.next().is_none());
}
