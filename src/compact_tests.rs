#[cfg(test)]
mod compact_tests {
    use crate::DbVersion;
    use crate::compact::MergeIterator;
    use crate::data_stores::{
        key::Key,
        memtable::Memtable,
        sstable::{Sstable, SstableIterator, SstableWriter},
        value::Value,
        wal::Wal,
        wal::wal_states::Writable,
    };
    use arc_swap::ArcSwap;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn create_test_sst(dir: &std::path::Path, id: u32, keys: Vec<(u64, u64)>) -> Sstable {
        let path = dir.join(format!("{:05}.sst", id));
        let mut writer = SstableWriter::new(&path).unwrap();
        for (k_suffix, lsn) in keys {
            let key_str = format!("key_{:05}", k_suffix);
            let key = Key::new(key_str.as_bytes(), lsn);
            let val = Value::new(format!("val_{}", k_suffix).as_bytes());
            writer.write(&key, &val).unwrap();
        }
        writer.finalize().unwrap();
        Sstable::new(&path).unwrap()
    }

    fn create_dummy_db_version(dir: &std::path::Path) -> Arc<ArcSwap<DbVersion>> {
        // Create dummy WAL (Writable)
        let wal = Wal::<Writable>::open(dir, 999).unwrap();
        let mem = Memtable::new();

        let ver = DbVersion {
            mutable_memtable: Arc::new(mem),
            mutable_wal: wal, // Wal<Writable> directly, not Arc
            frozen_memtables: Vec::new(),
            sstables: Vec::new(),
            next_lsn: 0,
        };
        Arc::new(ArcSwap::from(Arc::new(ver)))
    }

    #[test]
    fn test_merge_iterator() {
        let dir = tempdir().unwrap();

        // SST 1: keys 1, 3, 5 (LSN 10)
        let sst1 = create_test_sst(dir.path(), 1, vec![(1, 10), (3, 10), (5, 10)]);

        // SST 2: keys 2, 4, 6 (LSN 10)
        let sst2 = create_test_sst(dir.path(), 2, vec![(2, 10), (4, 10), (6, 10)]);

        // SST 3: keys 3 (LSN 20), 4 (LSN 5) - Overlap
        // Key 3 (LSN 20) should come BEFORE Key 3 (LSN 10).
        // Key 4 (LSN 5) should come AFTER Key 4 (LSN 10).
        let sst3 = create_test_sst(dir.path(), 3, vec![(3, 20), (4, 5)]);

        let iter1 = SstableIterator::new(Arc::new(sst1));
        let iter2 = SstableIterator::new(Arc::new(sst2));
        let iter3 = SstableIterator::new(Arc::new(sst3));

        let mut merge_iter = MergeIterator::new(vec![iter1, iter2, iter3]).unwrap();

        // Expected Order:
        // Key 1 (10)
        // Key 2 (10)
        // Key 3 (20)
        // Key 3 (10)
        // Key 4 (10)
        // Key 4 (5)
        // Key 5 (10)
        // Key 6 (10)

        // Helper to verify
        let expected = vec![
            ("key_00001", 10),
            ("key_00002", 10),
            ("key_00003", 20),
            ("key_00003", 10),
            ("key_00004", 10),
            ("key_00004", 5),
            ("key_00005", 10),
            ("key_00006", 10),
        ];

        for (exp_key, exp_lsn) in expected {
            let (k, _) = merge_iter.next().expect("Should have item").unwrap();
            assert_eq!(k.bytes.as_ref(), exp_key.as_bytes());
            assert_eq!(k.lsn.load(std::sync::atomic::Ordering::Relaxed), exp_lsn);
        }

        assert!(merge_iter.next().is_none());
    }

    #[test]
    fn test_compaction_integration() {
        // Setup
        let dir = tempdir().unwrap();
        let db_dir = dir.path();

        // Create 2 L0 files with MORE data
        let mut keys1 = Vec::new();
        for i in 100..150 {
            keys1.push((i, 10));
        }
        create_test_sst(db_dir, 1, keys1); // 50 keys

        let mut keys2 = Vec::new();
        for i in 200..250 {
            keys2.push((i, 10));
        }
        create_test_sst(db_dir, 2, keys2); // 50 keys

        // Create 1 overlapping L1 file
        let mut keys3 = Vec::new();
        for i in 125..225 {
            keys3.push((i, 5));
        }
        create_test_sst(db_dir, 3, keys3); // 100 keys overlap

        // Create Manifest
        use crate::data_stores::manifest::{FileMetadata, Level, Manifest};

        let mut levels = Vec::new();
        // L0
        let l0_files = vec![
            FileMetadata {
                file_id: 1,
                file_size: 1000,
                min_key: Key::new(b"key_00100", 10),
                max_key: Key::new(b"key_00149", 10),
            },
            FileMetadata {
                file_id: 2,
                file_size: 1000,
                min_key: Key::new(b"key_00200", 10),
                max_key: Key::new(b"key_00249", 10),
            },
        ];
        levels.push(Level { files: l0_files });

        // L1
        let l1_files = vec![FileMetadata {
            file_id: 3,
            file_size: 2000,
            min_key: Key::new(b"key_00125", 5),
            max_key: Key::new(b"key_00225", 5),
        }];
        levels.push(Level { files: l1_files });

        let manifest = Manifest {
            wals: vec![],
            levels,
            next_wal_id: 100,
            next_sstable_id: 4,
            version: 1,
        };

        let manifest_lock = std::sync::Arc::new(std::sync::Mutex::new(manifest));
        let db_current = create_dummy_db_version(db_dir);

        // Call compact_l0 with SMALL threshold (e.g. 1KB) to force split
        crate::compact::compact_l0(manifest_lock.clone(), db_current.clone(), db_dir, 1024)
            .unwrap(); // 1KB limit

        let manifest = manifest_lock.lock().unwrap();

        // Verify
        // L0 should be empty
        assert!(manifest.levels[0].files.is_empty(), "L0 should be empty");

        // L1 shoud have MULTIPLE files.
        let l1_count = manifest.levels[1].files.len();
        println!("Resulting L1 files: {}", l1_count);
        assert!(
            l1_count > 1,
            "Should have multiple merged L1 files due to splitting"
        );

        // Verify version bump
        assert_eq!(manifest.version, 2);

        // Verify DbVersion updated
        let current = db_current.load_full();
        // L0 empty
        if !current.sstables.is_empty() {
            assert!(current.sstables[0].is_empty());
        }
        // L1 has files
        assert!(current.sstables.len() > 1);
        assert!(!current.sstables[1].is_empty());
        assert_eq!(current.sstables[1].len(), l1_count);
    }

    #[test]
    fn test_compact_l1_to_l2() {
        let dir = tempdir().unwrap();
        let db_dir = dir.path();

        // Setup L1: 1 file (Keys 100-200)
        let mut keys1 = Vec::new();
        for i in 100..=200 {
            keys1.push((i, 20));
        }
        let sst1 = create_test_sst(db_dir, 1, keys1); // ID 1

        // Setup L2: 2 files (Keys 150-250, 300-400)
        let mut keys2 = Vec::new();
        for i in 150..=250 {
            keys2.push((i, 10));
        }
        let sst2 = create_test_sst(db_dir, 2, keys2); // ID 2

        let mut keys3 = Vec::new();
        for i in 300..=400 {
            keys3.push((i, 10));
        }
        let sst3 = create_test_sst(db_dir, 3, keys3); // ID 3

        use crate::data_stores::manifest::{FileMetadata, Level, Manifest};

        // Construct Manifest
        let mut levels = Vec::new(); // L0 (Empty)
        levels.push(Level::default());

        // L1
        levels.push(Level {
            files: vec![FileMetadata {
                file_id: 1,
                file_size: 1000,
                min_key: Key::new(b"key_00100", 20),
                max_key: Key::new(b"key_00200", 20),
            }],
        }); // Index 1

        // L2
        levels.push(Level {
            files: vec![
                FileMetadata {
                    file_id: 2,
                    file_size: 1000,
                    min_key: Key::new(b"key_00150", 10),
                    max_key: Key::new(b"key_00250", 10),
                },
                FileMetadata {
                    file_id: 3,
                    file_size: 1000,
                    min_key: Key::new(b"key_00300", 10),
                    max_key: Key::new(b"key_00400", 10),
                },
            ],
        }); // Index 2

        let manifest = Manifest {
            wals: vec![],
            levels,
            next_wal_id: 100,
            next_sstable_id: 4,
            version: 1,
        };

        let manifest_lock = std::sync::Arc::new(std::sync::Mutex::new(manifest));
        let db_current = create_dummy_db_version(db_dir);

        // Populate DbVersion with initial SSTables for correctness
        {
            let current = db_current.load_full();

            let mut sstables = Vec::new();
            sstables.push(Vec::new()); // L0
            sstables.push(vec![Arc::new(sst1)]); // L1
            sstables.push(vec![Arc::new(sst2), Arc::new(sst3)]); // L2

            let new_ver = Arc::new(crate::DbVersion {
                mutable_memtable: current.mutable_memtable.clone(),
                mutable_wal: current.mutable_wal.clone(),
                frozen_memtables: current.frozen_memtables.clone(),
                sstables,
                next_lsn: current.next_lsn,
            });
            db_current.store(new_ver);
        }

        // Call compact_ln(1) -> Compacts L1 to L2 using 64MB target (no split)
        crate::compact::compact_ln(
            manifest_lock.clone(),
            db_current.clone(),
            db_dir,
            1,
            64 * 1024 * 1024,
        )
        .unwrap();

        let manifest = manifest_lock.lock().unwrap();

        // Verify L1 is empty
        assert!(
            manifest.levels[1].files.is_empty(),
            "L1 should be empty after compaction"
        );

        // Verify L2
        let l2_files = &manifest.levels[2].files;
        assert!(
            l2_files.len() >= 2,
            "Should have at least merged file and separate file"
        );

        // Check sorting
        let first = &l2_files[0];
        let second = &l2_files[1];

        // Expected: Merged File First (100...), then File 3 (300...)
        assert!(first.min_key.bytes.as_ref() < second.min_key.bytes.as_ref());
        assert_eq!(
            second.file_id, 3,
            "Second file should be the untouched ID 3"
        );

        // Verify overlap removal
        assert!(
            !l2_files.iter().any(|f| f.file_id == 1),
            "ID 1 (L1) should be gone"
        );
        assert!(
            !l2_files.iter().any(|f| f.file_id == 2),
            "ID 2 (L2 overlap) should be gone"
        );

        // Verify DbVersion update
        let current = db_current.load_full();
        assert!(current.sstables[1].is_empty());
        assert_eq!(current.sstables[2].len(), l2_files.len());
        assert_eq!(
            current.sstables[2][0].min_key().bytes.as_ref(),
            first.min_key.bytes.as_ref()
        );
    }
}
