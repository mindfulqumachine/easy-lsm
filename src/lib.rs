use std::{
    collections::VecDeque,
    marker::PhantomData,
    sync::{Arc, Condvar, Mutex},
};

use arc_swap::ArcSwap;

use crate::{
    data_stores::{
        key::{Key, LsnType},
        manifest::in_memory::Manifest,
        memtable::{Memtable, state},
        sstable::Sstable,
        wal::{Wal, WalReceipt},
    },
    write_req::{WriteRequest, Writer},
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
    manifest: Manifest,
}

pub(crate) mod db_states {
    pub struct ReadOnly;
    pub struct ReadWrite;
}

pub(crate) struct WriteSyncs {
    pub(crate) write_queue: VecDeque<Arc<WriteRequest>>,
    pub(crate) wal_busy: bool,
    pub(crate) mem_busy: bool,

    // The ticket holder with permission to enter the memtable-write stage.
    pub(crate) ticket_to_mem: usize,

    // A writer to wal must collect this ticket for it to be able to
    // write to memtable after. Provides the memtable write ordering.
    pub(crate) next_ticket: usize,

    pub(crate) next_lsn: LsnType,
}

pub struct Db<State, ReqState> {
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
    version_lock: Mutex<()>,

    state: PhantomData<State>,
    req_state: PhantomData<ReqState>,
}

impl<State, ReqState> Db<State, ReqState> {
    pub fn new(_db_dir: &str) -> Result<Db<db_states::ReadWrite, ReqState>, err::DbError> {
        todo!()
    }

    pub fn get(&self, _key: &[u8]) -> Result<Option<Value>, err::DbError> {
        todo!()
    }

    #[cfg(test)]
    pub(crate) fn new_test()
    -> Db<db_states::ReadWrite, crate::write_req::write_states::WaitingToBeQueued> {
        let write_sync = Arc::new(Mutex::new(WriteSyncs {
            write_queue: VecDeque::new(),
            wal_busy: false,
            mem_busy: false,
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
            manifest: crate::data_stores::manifest::in_memory::Manifest::new(),
        };

        Db {
            current: ArcSwap::from_pointee(version),
            write_sync,
            wal_cv: Condvar::new(),
            mem_cv: Condvar::new(),
            version_lock: Mutex::new(()),
            state: PhantomData,
            req_state: PhantomData,
        }
    }
}

impl<ReqState> Db<db_states::ReadWrite, ReqState> {
    pub fn put(&self, _key: &[u8], _value: Value) -> Result<(), err::DbError> {
        todo!()
    }
    pub fn del(&self, _key: &[u8]) -> Result<(), err::DbError> {
        todo!()
    }
    // internal methods follow.

    fn write(&mut self, key: Key, val: Value) -> Result<(), err::DbError> {
        let _write_req =
            Arc::new(Writer::<crate::write_req::write_states::WaitingToBeQueued>::new(key, val));
        todo!()
    }

    // get bytes representation from each key and value, lay them out as one large blob
    // of bytes and then call the wal.write()
    // get bytes representation from each key and value, lay them out as one large blob
    // of bytes and then call the wal.write()
    pub(crate) fn write_wal(&self, _bytes: &[u8]) -> Result<WalReceipt, err::DbError> {
        #[cfg(test)]
        return Ok(WalReceipt { lsn: 101 });

        // now update the db.current.wal.append()
        self.current.load().wal.write(_bytes)
    }

    pub(crate) fn write_memtable(
        &self,
        _reqs: Vec<Arc<WriteRequest>>,
        _receipt: WalReceipt,
    ) -> Result<(), err::DbError> {
        #[cfg(test)]
        return Ok(());

        todo!()
    }
}
