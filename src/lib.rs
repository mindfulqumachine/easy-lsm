use std::{
    collections::VecDeque,
    marker::PhantomData,
    sync::{Arc, Condvar, Mutex},
};

use arc_swap::ArcSwap;

use crate::{
    data_stores::{
        key::{Key, LsnType},
        manifest::Manifest,
        memtable::{Memtable, state},
        sstable::Sstable,
        wal::{Wal, WalReceipt},
    },
    write_req::WriteRequest,
};

pub use data_stores::value::Value;

mod data_stores;
mod err;
mod write_req;

pub(crate) struct DbVersion {
    // The version the database is at.
    lsn: LsnType,

    memtable: Arc<Memtable<state::Mutable>>,
    wal: Wal,

    frozen_memtables: Vec<Arc<Memtable<state::Immutable>>>,

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

pub struct Db<State> {
    current: ArcSwap<DbVersion>,

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
    pub fn new(db_dir: &str) -> Result<Db<db_states::ReadWrite>, err::DbError> {
        let path = std::path::Path::new(db_dir);
        if !path.exists() {
            std::fs::create_dir_all(path).map_err(|e| err::DbError::Io(Arc::new(e)))?;
        }
        let wal_path = path.join("wal.log");
        let wal = crate::data_stores::wal::Wal::new(wal_path)?;
        let memtable = crate::data_stores::memtable::Memtable::new();
        // For now using in-memory manifest as imported
        let manifest = crate::data_stores::manifest::Manifest::new();

        let version = DbVersion {
            lsn: 0,
            memtable: Arc::new(memtable),
            wal,
            frozen_memtables: Vec::new(),
            sstables: Vec::new(),
        };

        let write_sync = Arc::new(Mutex::new(WriteSyncs {
            write_queue: VecDeque::new(),
            wal_busy: false,
            ticket_to_mem: 0,
            next_ticket: 0,
            next_lsn: 0,
        }));

        Ok(Db {
            current: ArcSwap::from_pointee(version),
            write_sync,
            wal_cv: Condvar::new(),
            mem_cv: Condvar::new(),
            manifest_lock: Mutex::new(manifest),
            state: PhantomData,
        })
    }

    #[cfg(test)]
    pub(crate) fn new_test() -> Db<db_states::ReadWrite> {
        let write_sync = Arc::new(Mutex::new(WriteSyncs {
            write_queue: VecDeque::new(),
            wal_busy: false,
            ticket_to_mem: 0,
            next_ticket: 0,
            next_lsn: 100,
        }));

        let version = DbVersion {
            lsn: 0,
            memtable: Arc::new(crate::data_stores::memtable::Memtable::new()),
            wal: crate::data_stores::wal::Wal::new(std::path::PathBuf::from("/tmp/test_wal.log"))
                .unwrap(),
            frozen_memtables: Vec::new(),
            sstables: Vec::new(),
        };

        Db {
            current: ArcSwap::from_pointee(version),
            write_sync,
            wal_cv: Condvar::new(),
            mem_cv: Condvar::new(),
            manifest_lock: Mutex::new(crate::data_stores::manifest::Manifest::new()),
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
        let writer = Writer::<crate::write_req::write_states::WaitingToBeQueued>::new(key, val);

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
        self.current.load().wal.write(_bytes)
    }

    pub(crate) fn write_memtable(
        &self,
        reqs: Vec<Arc<WriteRequest>>,
        _receipt: WalReceipt,
    ) -> Result<(), err::DbError> {
        let current_version = self.current.load();
        let memtable = &current_version.memtable;
        memtable.put_batch(&reqs);
        Ok(())
    }

    /// Get a value from the active memtable.
    /// TODO: This should also check frozen memtables and SSTables.
    pub fn get(&self, key: &[u8]) -> Result<Option<Value>, err::DbError> {
        let current_version = self.current.load();
        let memtable = &current_version.memtable;
        Ok(memtable.get(key))
    }
}
