use std::marker::PhantomData;

use crossbeam_skiplist::SkipMap;

use crate::data_stores::{key::Key, value::Value};
use crate::err::DbError;

pub mod state {
    /// Designator for an active memtable.
    pub struct Mutable;
    /// This memtable is frozen and does not accept writes.
    pub struct Immutable;
}

/// A memTable representation for a LSM tree.
///
/// State: One of state::Mutable | state::Immutable | state::Flushed
/// N:  Total allowed size for the memtable in bytes.
/// K: Key type. Must implement Ord for the inherent ordering requirements of the LSM tree.
/// V: Value type.
pub(crate) struct Memtable<State> {
    // The primary storage for keys and values.
    store: SkipMap<Key, Value>,

    size_bytes: usize,
    state: PhantomData<State>,
}

impl Memtable<state::Mutable> {
    /// Creates a new mutable memtable.
    pub fn new() -> Memtable<state::Mutable> {
        Self {
            store: SkipMap::new(),
            size_bytes: 0,
            state: PhantomData,
        }
    }

    /// Inserts a key-value pair into the memtable.
    ///
    /// If the key already exists, its value is updated, or else a new value is added.
    pub fn put(&mut self, key: Key, value: Value) {
        // let entry = MemtableEntry::Value(value);
        // if let Some(existing) = self.store.get(&key) {
        //     self.size_bytes -= std::mem::size_of_val(existing.value());
        // }
        // self.size_bytes += std::mem::size_of_val(&key) + std::mem::size_of_val(&entry);
        // self.store.insert(key, entry);
        todo!()
    }

    /// Pretends to delete a key-value pair by marking the key as tombstoned.
    pub fn del(&mut self, key: &Key) -> Result<(), DbError> {
        // if let Some(entry) = self.store.get(key) {
        //     match entry.value() {
        //         MemtableEntry::Value(_) => {
        //             self.size_bytes -= std::mem::size_of_val(entry.value());
        //             let tombstone = MemtableEntry::Tombstone;
        //             self.size_bytes += std::mem::size_of_val(&tombstone);
        //             self.store.insert(key.clone(), tombstone);
        //             Ok(())
        //         }
        //         MemtableEntry::Tombstone => Err(LsmError::KeyNotFound(format!("{:?}", key))),
        //     }
        // } else {
        //     Err(LsmError::KeyNotFound(format!("{:?}", key)))
        // }
        todo!()
    }

    /// Freezes the memtable, preventing further writes.
    pub fn freeze(self) -> Memtable<state::Immutable> {
        Memtable {
            store: self.store,
            size_bytes: self.size_bytes,
            state: PhantomData,
        }
    }
}

impl Memtable<state::Immutable> {
    /// Retrieves a value by key from the memtable.
    pub fn flush(&self) -> Result<(), DbError> {
        unimplemented!()
    }
}

impl<State> Memtable<State> {
    pub fn get(&self, key: &Key) -> Option<&Value> {
        //     self.store.get(key).and_then(|entry| match entry.value() {
        //         MemtableEntry::Value(v) => Some(v),
        //         MemtableEntry::Tombstone => None,
        //     })
        todo!()
    }
}
