use std::{
    collections::VecDeque,
    marker::PhantomData,
    path::PathBuf,
    sync::atomic::{AtomicBool, Ordering},
    sync::{Arc, Condvar, Mutex},
    thread,
};

use arc_swap::ArcSwap;
use rayon::prelude::*;

use crate::{
    data_stores::{
        key::{Key, LsnType},
        manifest::Manifest,
        memtable::{Memtable, state},
        sstable::{SSTABLE_EXTENSION, Sstable},
        wal::{self, Wal, WalReceipt, wal_states},
    },
    write_req::{WriteRequest, write_states},
};
mod compact;
pub use data_stores::value::Value;

pub const MAX_MEMTABLE_SIZE: usize = 4 * 1024 * 1024; // 4MB

mod data_stores;
mod err;
mod write_req;

struct RecoveredState {
    mutable_memtable: Arc<Memtable<state::Mutable>>,
    mutable_wal: Wal<wal::Writable>,
    frozen_memtables: Vec<Arc<Memtable<state::Immutable>>>,
    sstables: Vec<Vec<Arc<Sstable>>>,
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
    pub(crate) current: Arc<ArcSwap<DbVersion>>,

    // control plane mechanisms to orchestrate the writes.
    pub(crate) write_sync: Arc<Mutex<WriteSyncs>>,

    // writers waiting on wal availability are notified using this.
    pub(crate) wal_cv: Condvar,

    // writers waiting to write to memtable are notified using this.
    pub(crate) mem_cv: Condvar,

    // STRUCTURAL LOCK: For flushes, compaction, rotation.
    // The Memtable write leader only acquires this lock when memtable is full
    //  and it is time to freeze the memtable and add a new one.
    manifest_lock: Arc<Mutex<Manifest>>,

    // Condition variable to wake up the compaction thread.
    compaction_cv: Arc<Condvar>,
    // Signal to the compaction thread to exit.
    shutdown: Arc<AtomicBool>,
    // Handle to the compaction thread.
    // Wrapped in Mutex to allow restarting if it crashes.
    compaction_thread: Mutex<Option<thread::JoinHandle<()>>>,

    // The directory where the database is stored.
    db_dir: PathBuf,

    state: PhantomData<State>,

    pub(crate) max_memtable_size: usize,
}

impl<State> Drop for Db<State> {
    fn drop(&mut self) {
        // Signal shutdown
        self.shutdown.store(true, Ordering::Relaxed);
        self.compaction_cv.notify_all();

        if let Some(handle) = self.compaction_thread.lock().unwrap().take() {
            let _ = handle.join();
        }
    }
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
            sstables: state.sstables,
        };

        let write_sync = Arc::new(Mutex::new(WriteSyncs::new(state.next_lsn)));

        let manifest_lock = Arc::new(Mutex::new(manifest));
        let compaction_cv = Arc::new(Condvar::new());
        let shutdown = Arc::new(AtomicBool::new(false));

        let m_lock = manifest_lock.clone();
        let c_cv = compaction_cv.clone();
        let s_down = shutdown.clone();
        let d_dir = path.to_path_buf();

        // 4. Initialize Db state
        let current = Arc::new(ArcSwap::from_pointee(version));
        let db_current = current.clone();

        let compaction_thread = thread::Builder::new()
            .name("compaction".to_string())
            .spawn(move || {
                crate::compact::run(m_lock, db_current, c_cv, s_down, d_dir);
            })
            .map_err(|e| err::DbError::Io(Arc::new(e)))?;

        Ok(Db {
            current,
            write_sync,
            wal_cv: Condvar::new(),
            mem_cv: Condvar::new(),
            manifest_lock,
            compaction_cv,
            shutdown,
            compaction_thread: Mutex::new(Some(compaction_thread)),
            db_dir: path.to_path_buf(),
            state: PhantomData,
            max_memtable_size: MAX_MEMTABLE_SIZE,
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

        // Check SSTables
        for level in &current_version.sstables {
            for sstable in level {
                if let Some(val) = sstable.search(key)? {
                    return Ok(Some(val));
                }
            }
        }

        Ok(None)
    }

    #[cfg(test)]
    pub(crate) fn new_test() -> Db<db_states::ReadWrite> {
        use crate::data_stores::{manifest::Manifest, memtable::Memtable, wal::wal_states};
        use tempfile::tempdir;

        // Leak the tempdir so it persists for the duration of the test.
        // The OS will clean it up typically, or we should wrap Db in a helper that drops the dir.
        // But for unit tests, leaking a few MBs of temp dirs is acceptable vs complexity.
        // A better approach is `DbTestGuard` but changing return type affects all calls.
        // We'll just leak the tempdir path string/creation but keep the integrity.
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        // Prevent deletion at end of scope by converting to "into_path" or similar?
        // tempfile::TempDir deletes on drop.
        // We need the directory to persist while Db is alive.
        // Db owns `db_dir`.
        // If we drop `dir` here, the directory is deleted.
        // So we must persist `dir` somewhere or let it leak.
        let path = dir.into_path(); // Consumes TempDir, persists path.

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
            mutable_wal: crate::data_stores::wal::Wal::<wal_states::Writable>::open(&path, 0)
                .unwrap(),
            frozen_memtables: Vec::new(),
            sstables: Vec::new(),
        };

        let manifest_lock = Arc::new(Mutex::new(Manifest::new(0)));
        // Note: In tests we might not want the thread, but to keep consistent behavior we should spawn it.
        // Or we can leave it None if we had a builder. But here we must populate.
        // Spawning thread in unit tests is fine if we clean up (which we do in Drop).

        let compaction_cv = Arc::new(Condvar::new());
        let shutdown = Arc::new(AtomicBool::new(false));

        let m_lock = manifest_lock.clone();
        let c_cv = compaction_cv.clone();
        let s_down = shutdown.clone();
        let d_dir = path.clone();

        let current = Arc::new(ArcSwap::from_pointee(version));
        let db_current = current.clone();

        let compaction_thread = thread::Builder::new()
            .name("compaction_test".to_string())
            .spawn(move || {
                crate::compact::run(m_lock, db_current, c_cv, s_down, d_dir);
            })
            .unwrap();

        Db {
            current,
            write_sync,
            wal_cv: Condvar::new(),
            mem_cv: Condvar::new(),
            manifest_lock,
            compaction_cv,
            shutdown,
            compaction_thread: Mutex::new(Some(compaction_thread)),
            db_dir: path,
            state: PhantomData,
            max_memtable_size: MAX_MEMTABLE_SIZE,
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

    pub(crate) fn active_memtable_size(&self) -> usize {
        self.current.load().mutable_memtable.size()
    }

    /// Rotates the memtable:
    /// 1. Adds a new WAL to manifest (persisting it).
    /// 2. Creates the new WAL file.
    /// 3. Freezes current memtable.
    /// 4. Creates new mutable memtable.
    /// 5. Updates DbVersion.
    pub(crate) fn rotate_memtable(&self) -> Result<(), err::DbError> {
        // We need to persist the new WAL to the manifest atomically.
        // We use the helper `apply_atomic_update`.
        // Note: This helper might retry. If it retries, we might create multiple WAL files.
        // This is acceptable (orphaned files) for correctness.

        let manifest_lock = &self.manifest_lock;
        let db_dir = &self.db_dir;

        let new_manifest = crate::data_stores::manifest::apply_atomic_update(
            manifest_lock,
            db_dir,
            |manifest_candidate| {
                let new_wal_id = manifest_candidate.next_wal_id();
                // Create valid WAL file first (IO outside lock, effectively)
                // content of apply_atomic_update closure runs unlocked.
                // We open it to ensure it exists.
                use crate::data_stores::wal::wal_states;
                let _ =
                    crate::data_stores::wal::Wal::<wal_states::Writable>::open(db_dir, new_wal_id)?;

                // Update candidate
                manifest_candidate.wals.push(new_wal_id);
                Ok(())
            },
        )?;

        // Success. `new_manifest` is the committed state.
        // Get the new WAL ID (last one).
        let new_wal_id = *new_manifest.wals.last().ok_or(err::DbError::DataCorrupted(
            "No WALs after rotation".to_string(),
        ))?;

        // Open the WAL for `DbVersion`
        // We just created it, so open should succeed.
        use crate::data_stores::wal::wal_states;
        let new_wal =
            crate::data_stores::wal::Wal::<wal_states::Writable>::open(db_dir, new_wal_id)?;

        // Update DbVersion
        // Atomic swap of the version.
        let current = self.current.load();

        // Freeze current memtable
        let mut frozen_memtables = current.frozen_memtables.clone();
        let frozen = current.mutable_memtable.freeze_from_ref();
        frozen_memtables.push(Arc::new(frozen));

        let new_version = DbVersion {
            // next_lsn is managed by WriteSyncs, but we carry over the value from previous version
            // or we could read it from WriteSyncs if we had access, but `current.next_lsn` is fine specific to version.
            next_lsn: current.next_lsn,
            mutable_memtable: Arc::new(Memtable::new()),
            mutable_wal: new_wal,
            frozen_memtables,
            sstables: current.sstables.clone(),
        };

        self.current.store(Arc::new(new_version));

        // Ensure compaction thread is running (restart if crashed)
        let _ = self.ensure_compaction_thread();

        // Notify compaction thread that a new frozen memtable is available
        self.compaction_cv.notify_one();

        Ok(())
    }

    /// Checks if the compaction thread is running and restarts it if it has finished (crashed).
    /// Does nothing if the shutdown signal is set.
    fn ensure_compaction_thread(&self) -> Result<(), err::DbError> {
        // If we are shutting down, do not restart.
        if self.shutdown.load(Ordering::Relaxed) {
            return Ok(());
        }

        let mut handle_guard = self.compaction_thread.lock().unwrap();
        let need_restart = if let Some(handle) = handle_guard.as_ref() {
            handle.is_finished()
        } else {
            true
        };

        if need_restart {
            // Join the old thread to clean up resources (if it finished)
            if let Some(handle) = handle_guard.take() {
                let _ = handle.join(); // Ignore panic payload
            }

            let m_lock = self.manifest_lock.clone();
            let c_cv = self.compaction_cv.clone();
            let s_down = self.shutdown.clone();
            let d_dir = self.db_dir.clone();

            let db_current = self.current.clone();

            let new_thread = thread::Builder::new()
                .name("compaction_restarted".to_string())
                .spawn(move || {
                    crate::compact::run(m_lock, db_current, c_cv, s_down, d_dir);
                })
                .map_err(|e| err::DbError::Io(Arc::new(e)))?;

            *handle_guard = Some(new_thread);
        }

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

    let mutable_wal = Wal::<wal_states::Writable>::open(db_dir, mutable_wal_id)?;

    // Recover SSTables
    // Iterate manifest levels and open sstables
    let mut sstables = Vec::new();
    for level in &manifest.levels {
        let mut level_sstables = Vec::new();
        for file_meta in &level.files {
            let file_id = file_meta.file_id;
            let path = db_dir.join(format!("{:05}.{}", file_id, SSTABLE_EXTENSION));
            let sstable = Sstable::new(&path)?;
            level_sstables.push(Arc::new(sstable));
        }
        sstables.push(level_sstables);
    }

    Ok(RecoveredState {
        mutable_memtable: Arc::new(mutable_memtable),
        mutable_wal,
        frozen_memtables,
        sstables,
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
            mutable_wal: Wal::<wal_states::Writable>::open(&db.db_dir, 999).unwrap(),
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
    }

    #[test]
    fn test_memtable_rotation() {
        // Use tempfile for isolation
        let dir = tempfile::tempdir().unwrap();
        let db =
            Db::<db_states::ReadWrite>::new(dir.path().to_str().unwrap()).expect("Db new failed");

        // 1. Write enough data to trigger rotation
        // MAX_MEMTABLE_SIZE is 4MB.
        // We need to write > 4MB.
        // Let's make a large value.
        let large_val_size = 1024 * 1024; // 1MB
        let val_bytes = vec![0u8; large_val_size];
        let val = Value::new(&val_bytes);

        // 5 writes should exceed 4MB
        for i in 0..5 {
            db.put(format!("key{}", i).as_bytes(), val.clone()).unwrap();
        }

        // 2. Verification
        let current = db.current.load();

        // Should have at least 1 frozen memtable
        assert!(
            !current.frozen_memtables.is_empty(),
            "Should have frozen memtables"
        );

        // Mutable memtable should be relatively small (fresh)
        assert!(
            current.mutable_memtable.size() < 2 * 1024 * 1024,
            "New mutable memtable should be small"
        );

        // 3. Verify data persistence and recovery
        // Drop db to close headers/files? (Not strict in this mock, but good practice)
        drop(db);

        // Reopen
        let db = Db::<db_states::ReadWrite>::new(dir.path().to_str().unwrap()).unwrap();

        // Check keys
        for i in 0..5 {
            let key = format!("key{}", i);
            let res = db.get(key.as_bytes()).unwrap();
            assert!(res.is_some(), "Key {} should exist", key);
        }

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_compaction_thread_lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        // create Db - thread starts
        let db = Db::<db_states::ReadWrite>::new(dir.path().to_str().unwrap()).unwrap();

        // do some work
        db.put(b"foo", Value::new(b"bar")).unwrap();

        // drop db - thread stops. If this hangs, the test times out.
        drop(db);
    }

    #[test]
    fn test_compaction_thread_restart() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::<db_states::ReadWrite>::new(dir.path().to_str().unwrap()).unwrap();

        // Simulate crash by taking the handle and joining it manually
        {
            let mut g = db.compaction_thread.lock().unwrap();
            let handle = g.take().expect("Thread should exist");
            // Determine thread ID
            println!("Old thread id: {:?}", handle.thread().id());
        }

        // Now handle is None. `ensure_compaction_thread` should restart it.
        db.ensure_compaction_thread().unwrap();

        {
            let g = db.compaction_thread.lock().unwrap();
            assert!(g.is_some(), "Thread should have been restarted");
            println!("New thread id: {:?}", g.as_ref().unwrap().thread().id());
        }
    }
    #[test]
    fn test_memtable_flush_flow() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = Db::<db_states::ReadWrite>::new(dir.path().to_str().unwrap()).unwrap();
        db.max_memtable_size = 4096; // 4KB for easy rotation

        // 1. Write data to fill memtables and trigger rotations.
        // Value size ~1KB. Memtable limit 4KB.
        // 5 writes => > 4KB. Triggers rotation.
        // We want > 4 WALs frozen.
        // So we need 5 rotations.
        // 5 rotations * 5 writes = 25 writes?

        let val_bytes = vec![0u8; 1000]; // 1KB
        let val = Value::new(&val_bytes);

        for i in 0..30 {
            db.put(format!("key{:05}", i).as_bytes(), val.clone())
                .unwrap();
        }

        // Now we should have created multiple frozen memtables.
        // Compaction thread should wake up and flush them.

        // Wait for flush.
        // Condition: `levels[0]` has files. We don't strictly check `frozen_count == 0` because
        // flush policy might leave some frozen memtables if they don't meet the batch size.
        // We just want to ensure flushing IS happening.
        let start = std::time::Instant::now();
        loop {
            let current = db.current.load();
            let frozen_count = current.frozen_memtables.len();
            let l0_count = if !current.sstables.is_empty() {
                current.sstables[0].len()
            } else {
                0
            };

            if l0_count > 0 {
                // Success!
                // println!("Flush confirmed: {} frozen, {} SSTables", frozen_count, l0_count);
                break;
            }

            if start.elapsed().as_secs() > 10 {
                panic!(
                    "Timeout waiting for flush. Frozen: {}, L0: {}",
                    frozen_count, l0_count
                );
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        // Verify Data Availability (Read from SST)
        // Check "key00000" (oldest, should be in SST)
        let res = db.get(b"key00000").unwrap();
        assert!(res.is_some(), "Should find key00000 even after flush");

        // Verify GC
        {
            let lock = db.manifest_lock.lock().unwrap();
            // If we flushed, we should have removed some WALs.
            // println!("WALs remaining: {}", lock.wals.len());
            // Should be smaller than initial (6)
            assert!(lock.wals.len() < 10, "GC should keep WALs under control");
        }
    }
}
