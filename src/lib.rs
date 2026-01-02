use std::{
    collections::VecDeque,
    marker::PhantomData,
    path::PathBuf,
    sync::{Arc, Condvar, Mutex},
};

use arc_swap::ArcSwap;
use rayon::prelude::*;

use crate::{
    data_stores::{
        key::{Key, LsnType},
        manifest::Manifest,
        memtable::{Memtable, state},
        sstable::Sstable,
        wal::{self, Wal, WalReceipt, wal_states},
    },
    write_req::{WriteRequest, write_states},
};

pub use data_stores::value::Value;

mod data_stores;
mod err;
mod write_req;

struct RecoveredState {
    mutable_memtable: Arc<Memtable<state::Mutable>>,
    mutable_wal: Wal<wal::Writable>,
    frozen_memtables: Vec<Arc<Memtable<state::Immutable>>>,
    next_lsn: LsnType,
}

pub(crate) struct DbVersion {
    // The version the database is at.
    next_lsn: LsnType,

    mutable_memtable: Arc<Memtable<state::Mutable>>,
    frozen_memtables: Vec<Arc<Memtable<state::Immutable>>>,

    mutable_wal: Wal<wal::Writable>,
    sstables: Vec<Vec<Arc<Sstable>>>,
}

pub(crate) mod db_states {
    pub struct ReadOnly;
    pub struct ReadWrite;
}

pub(crate) struct WriteSyncs {
    pub(crate) write_queue: VecDeque<Arc<WriteRequest>>,
    pub(crate) wal_busy: bool,

    // The ticket holder with permission to enter the memtable-write stage.
    pub(crate) ticket_to_mem: usize,

    // A writer to wal must collect this ticket for it to be able to
    // write to memtable after. Provides the memtable write ordering.
    pub(crate) next_ticket: usize,

    pub(crate) next_lsn: LsnType,
}

impl WriteSyncs {
    pub(crate) fn new(start_lsn: LsnType) -> Self {
        Self {
            write_queue: VecDeque::new(),
            wal_busy: false,
            ticket_to_mem: 0,
            next_ticket: 0,
            next_lsn: start_lsn,
        }
    }
}

pub struct Db<State> {
    pub(crate) current: ArcSwap<DbVersion>,

    // control plane mechanisms to orchestrate the writes.
    pub(crate) write_sync: Arc<Mutex<WriteSyncs>>,

    // writers waiting on wal availability are notified using this.
    pub(crate) wal_cv: Condvar,

    // writers waiting to write to memtable are notified using this.
    pub(crate) mem_cv: Condvar,

    // STRUCTURAL LOCK: For flushes, compaction, rotation.
    // The Memtable write leader only acquires this lock when memtable is full
    //  and it is time to freeze the memtable and add a new one.
    manifest_lock: Mutex<Manifest>,

    state: PhantomData<State>,
}

impl<State> Db<State> {
    /// Creates a new database instance and make it available for read-write operations.
    ///
    /// Initializing the database determines if this is a pristine start - at a new
    /// directory, or a start from a previous state.
    /// The manifest is the key here. The database looks for the most upto date and valid
    /// manifest. manifests have the .mf extension and the ELSM magic word as the first
    /// 4 bytes. Then there is the checksum which validates data integrity.
    /// If the DB finds an invalid manifest, it stops and asks user to delete that
    /// before starting again.
    ///
    /// After locating it, the DB will load it. The manifest is the ensamble of everything
    /// that makes this database work - the write-ahead-log, helps us construct the
    /// memtables - active and frozen. Then it has the list of sstables.
    ///
    /// Only when the database has constructed its full state, it moves from read-only to
    /// read-write state and becomes available for business.
    /// Creates a new database instance and make it available for read-write operations.
    ///
    /// Initializing the database determines if this is a pristine start - at a new
    /// directory, or a start from a previous state.
    /// The manifest is the key here. The database looks for the most upto date and valid
    /// manifest. manifests have the .mf extension and the ELSM magic word as the first
    /// 4 bytes. Then there is the checksum which validates data integrity.
    /// If the DB finds an invalid manifest, it stops and asks user to delete that
    /// before starting again.
    ///
    /// After locating it, the DB will load it. The manifest is the ensamble of everything
    /// that makes this database work - the write-ahead-log, helps us construct the
    /// memtables - active and frozen. Then it has the list of sstables.
    ///
    /// Only when the database has constructed its full state, it moves from read-only to
    /// read-write state and becomes available for business.
    pub fn new(db_dir: &str) -> Result<Db<db_states::ReadWrite>, err::DbError> {
        let path = std::path::Path::new(db_dir);
        if !path.exists() {
            return Err(err::DbError::DirectoryNotFound(db_dir.to_string()));
        }

        // 1. Discovery
        let manifest = Manifest::recover_or_init(path)?;

        // 2. Recovery
        let state = recover_state(path, &manifest)?;

        // 3. Launch
        let version = DbVersion {
            next_lsn: state.next_lsn,
            mutable_memtable: state.mutable_memtable,
            mutable_wal: state.mutable_wal,
            frozen_memtables: state.frozen_memtables,
            sstables: Vec::new(),
        };

        let write_sync = Arc::new(Mutex::new(WriteSyncs::new(state.next_lsn)));

        Ok(Db {
            current: ArcSwap::from_pointee(version),
            write_sync,
            wal_cv: Condvar::new(),
            mem_cv: Condvar::new(),
            manifest_lock: Mutex::new(manifest),
            state: PhantomData,
        })
    }

    /// Get a value from the active memtable.
    /// TODO: This should also check frozen memtables and SSTables.
    pub fn get(&self, key: &[u8]) -> Result<Option<Value>, err::DbError> {
        let current_version = self.current.load();
        let memtable = &current_version.mutable_memtable;

        if let Some(val) = memtable.get(key) {
            return Ok(Some(val));
        }

        // Iterate over frozen memtables in reverse order (newest to oldest)
        for memtable in current_version.frozen_memtables.iter().rev() {
            if let Some(val) = memtable.get(key) {
                return Ok(Some(val));
            }
        }

        // TODO: Check SSTables once implemented.

        Ok(None)
    }

    #[cfg(test)]
    pub(crate) fn new_test() -> Db<db_states::ReadWrite> {
        use crate::data_stores::{manifest::Manifest, memtable::Memtable, wal::wal_states};

        let write_sync = Arc::new(Mutex::new(WriteSyncs {
            write_queue: VecDeque::new(),
            wal_busy: false,
            ticket_to_mem: 0,
            next_ticket: 0,
            next_lsn: 100,
        }));

        let version = DbVersion {
            next_lsn: 0,
            mutable_memtable: Arc::new(Memtable::new()),
            mutable_wal: crate::data_stores::wal::Wal::<wal_states::Writable>::create_at(
                std::path::Path::new("/tmp"),
                0,
            )
            .unwrap(),
            frozen_memtables: Vec::new(),
            sstables: Vec::new(),
        };

        Db {
            current: ArcSwap::from_pointee(version),
            write_sync,
            wal_cv: Condvar::new(),
            mem_cv: Condvar::new(),
            manifest_lock: Mutex::new(Manifest::new(0)),
            state: PhantomData,
        }
    }
}

impl Db<db_states::ReadWrite> {
    pub fn put(&self, key: &[u8], value: Value) -> Result<(), err::DbError> {
        let key = Key::new(key, 0);
        self.write(key, value)
    }

    pub fn del(&self, key: &[u8]) -> Result<(), err::DbError> {
        self.put(key, Value::Tombstone)
    }

    // internal methods follow.

    fn write(&self, key: Key, val: Value) -> Result<(), err::DbError> {
        use crate::write_req::{SeekingMembershipResult, Writer};
        let writer = Writer::<write_states::WaitingToBeQueued>::new(key, val);

        // 1. Queue
        let w = writer.step(self)?;

        // 2. Seek Membership
        // This blocks until we are a Leader, Follower, or Success (early completion)
        let w_res = w.step(self)?;

        match w_res {
            SeekingMembershipResult::Leader(l) => {
                // Leader Path
                let l = l.step(self)?; // Wait for WAL
                let l = l.step(self)?; // Write WAL
                let l = l.step(self)?; // Wait for Memtable
                let l = l.step(self)?; // Write Memtable
                l.step()?; // Finish
                Ok(())
            }
            SeekingMembershipResult::Follower(f) => {
                // Follower Path
                f.step()?;
                Ok(())
            }
            SeekingMembershipResult::Success(_) => Ok(()),
        }
    }

    // get bytes representation from each key and value, lay them out as one large blob
    // of bytes and then call the wal.write()
    // get bytes representation from each key and value, lay them out as one large blob
    // of bytes and then call the wal.write()
    pub(crate) fn write_wal(&self, _bytes: &[u8]) -> Result<WalReceipt, err::DbError> {
        // now update the db.current.wal.append()
        self.current.load().mutable_wal.write(_bytes)
    }

    pub(crate) fn write_memtable(
        &self,
        reqs: Vec<Arc<WriteRequest>>,
        _receipt: WalReceipt,
    ) -> Result<(), err::DbError> {
        let current_version = self.current.load();
        let memtable = &current_version.mutable_memtable;
        memtable.put_batch(&reqs);
        Ok(())
    }
}

fn recover_state(
    db_dir: &std::path::Path,
    manifest: &Manifest,
) -> Result<RecoveredState, err::DbError> {
    if manifest.wals.is_empty() {
        return Err(err::DbError::DataCorrupted(
            "Manifest contains no WALs".to_string(),
        ));
    }

    let mutable_wal_idx = manifest.wals.len() - 1;
    let mutable_wal_id = manifest.wals[mutable_wal_idx];

    let wal_path = |wal_id| db_dir.join(format!("{:05}{}", wal_id, wal::WAL_EXTENSION));

    let mutable_wal_path = wal_path(mutable_wal_id);

    let (frozen_results, mutable_res) = rayon::join(
        || {
            manifest.wals[..mutable_wal_idx]
                .par_iter()
                .map(|&wal_id| replay_immutable_wal(wal_path(wal_id)))
                .collect::<Result<Vec<_>, err::DbError>>()
        },
        || replay_mutable_wal(mutable_wal_path),
    );

    let frozen_memtables = frozen_results?;
    let (mutable_memtable, max_lsn) = mutable_res?;

    let mutable_wal = Wal::<wal_states::Writable>::create_at(db_dir, mutable_wal_id)?;
    Ok(RecoveredState {
        mutable_memtable: Arc::new(mutable_memtable),
        mutable_wal,
        frozen_memtables,
        next_lsn: max_lsn + 1,
    })
}

fn replay_immutable_wal(
    wal_path: PathBuf,
) -> Result<Arc<Memtable<state::Immutable>>, err::DbError> {
    let wal = Wal::<wal::ReadOnly>::open(wal_path)?;
    let memtable = Memtable::new();
    for entry_res in wal.try_iter()? {
        let (key, value) = entry_res?;
        memtable.recover(key, value);
    }
    Ok(Arc::new(memtable.freeze()))
}

fn replay_mutable_wal(wal_path: PathBuf) -> Result<(Memtable<state::Mutable>, u64), err::DbError> {
    let wal = Wal::<wal::ReadOnly>::open(wal_path)?;

    let memtable = Memtable::new();

    let mut last_lsn = 0;
    for entry_res in wal.try_iter()? {
        let (key, value) = entry_res?;
        last_lsn = key.lsn.load(std::sync::atomic::Ordering::Relaxed);
        memtable.recover(key, value);
    }
    Ok((memtable, last_lsn))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_invalid_manifest_errors() {
        let dir = std::env::temp_dir().join("test_invalid_manifest_errors");
        if dir.exists() {
            fs::remove_dir_all(&dir).unwrap();
        }
        fs::create_dir(&dir).unwrap();

        // Create a corrupt manifest file
        let manifest_path = dir.join("00001.mf");
        fs::write(&manifest_path, b"garbage data").unwrap();

        let result = Db::<db_states::ReadWrite>::new(dir.to_str().unwrap());
        match result {
            Err(err::DbError::ManifestReadError(msg)) => {
                assert!(msg.contains("Failed to open manifest"));
            }
            Ok(_) => panic!("Expected ManifestReadError, got Ok"),
            Err(e) => panic!("Expected ManifestReadError, got {:?}", e),
        }

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_recovery_from_wal() {
        let dir = std::env::temp_dir().join("test_recovery_from_wal");
        if dir.exists() {
            fs::remove_dir_all(&dir).unwrap();
        }
        fs::create_dir(&dir).unwrap();

        // 1. Start DB, write data, close DB
        {
            let db = Db::<db_states::ReadWrite>::new(dir.to_str().unwrap()).unwrap();
            db.put(b"key1", Value::new(b"val1")).unwrap();
            db.put(b"key2", Value::new(b"val2")).unwrap();
            // Wait for persistence (writes are durable in this mock if they return Ok)
        }

        // 2. Restart DB
        {
            let db = Db::<db_states::ReadWrite>::new(dir.to_str().unwrap()).unwrap();

            // 3. Verify data recovery
            let val1 = db.get(b"key1").unwrap();
            match val1 {
                Some(Value::Bytes(b)) => assert_eq!(&*b, b"val1"),
                _ => panic!("Failed to recover key1"),
            }

            let val2 = db.get(b"key2").unwrap();
            match val2 {
                Some(Value::Bytes(b)) => assert_eq!(&*b, b"val2"),
                _ => panic!("Failed to recover key2"),
            }
        }

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_get_from_frozen() {
        use std::sync::Arc;

        let dir = std::env::temp_dir().join("test_get_from_frozen");
        if dir.exists() {
            fs::remove_dir_all(&dir).unwrap();
        }
        fs::create_dir(&dir).unwrap();

        let db = Db::<db_states::ReadWrite>::new_test();

        // 1. Create a memtable, put some data, and freeze it
        let frozen_mem = Arc::new(Memtable::new());
        frozen_mem.recover(Key::new(b"key_frozen", 100), Value::new(b"val_frozen"));
        let frozen_mem = Arc::new(Arc::try_unwrap(frozen_mem).unwrap().freeze());

        // 2. Inject it into DB
        let current_guard = db.current.load();
        let new_version = DbVersion {
            next_lsn: current_guard.next_lsn,
            mutable_memtable: current_guard.mutable_memtable.clone(),
            frozen_memtables: vec![frozen_mem],
            mutable_wal: Wal::<wal_states::Writable>::create_at(std::path::Path::new("/tmp"), 999)
                .unwrap(),
            sstables: Vec::new(),
        };
        db.current.store(Arc::new(new_version));

        // 3. Test get
        let val = db.get(b"key_frozen").unwrap();
        match val {
            Some(Value::Bytes(b)) => assert_eq!(&*b, b"val_frozen"),
            _ => panic!("Failed to get key from frozen memtable"),
        }

        // 4. Test precedence (Mutable > Frozen)
        db.put(b"key_frozen", Value::new(b"val_active")).unwrap();

        let val = db.get(b"key_frozen").unwrap();
        match val {
            Some(Value::Bytes(b)) => assert_eq!(&*b, b"val_active"),
            _ => panic!("Should get active value"),
        }

        fs::remove_dir_all(&dir).unwrap();
    }
}
