use std::marker::PhantomData;

use crossbeam_skiplist::{SkipList, SkipMap};

mod state {
    /// Designator for an active memtable.
    pub struct Mutable;
    /// This memtable is frozen and does not accept writes.
    pub struct Immutable;
}

enum Value<V> {
    Value(V),
    Tombstone,
}

/// A memTable representation for a LSM tree.
///
/// State: One of state::Mutable | state::Immutable | state::Flushed
/// N:  Total allowed size for the memtable in bytes.
/// K: Key type. Must implement Ord for the inherent ordering requirements of the LSM tree.
/// V: Value type.
struct Memtable<State, K: Ord, V> {
    // The primary storage for keys and values.
    store: SkipMap<K, MemtableEntry<V>>,

    size_bytes: usize,
    state: PhantomData<State>,
}

impl<K: Ord + Send + Clone, V: Send> Memtable<state::Mutable, K, V> {
    /// Creates a new mutable memtable.
    pub fn new() -> Memtable<state::Mutable, K, V> {
        Self {
            store: SkipMap::new(),
            size_bytes: 0,
            state: PhantomData,
        }
    }

    /// Inserts a key-value pair into the memtable.
    ///
    /// If the key already exists, its value is updated, or else a new value is added.
    pub fn put(&mut self, key: K, value: V) {
        let entry = MemtableEntry::Value(value);
        if let Some(existing) = self.store.get(&key) {
            self.size_bytes -= std::mem::size_of_val(existing.value());
        }
        self.size_bytes += std::mem::size_of_val(&key) + std::mem::size_of_val(&entry);
        self.store.insert(key, entry);
    }

    /// Pretends to delete a key-value pair by marking the key as tombstoned.
    pub fn del(&mut self, key: &K) -> Result<(), LsmError> {
        if let Some(entry) = self.store.get(key) {
            match entry.value() {
                MemtableEntry::Value(_) => {
                    self.size_bytes -= std::mem::size_of_val(entry.value());
                    let tombstone = MemtableEntry::Tombstone;
                    self.size_bytes += std::mem::size_of_val(&tombstone);
                    self.store.insert(key.clone(), tombstone);
                    Ok(())
                }
                MemtableEntry::Tombstone => Err(LsmError::KeyNotFound(format!("{:?}", key))),
            }
        } else {
            Err(LsmError::KeyNotFound(format!("{:?}", key)))
        }
    }

    /// Freezes the memtable, preventing further writes.
    pub fn freeze(self) -> Memtable<state::Immutable, K, V> {
        Memtable {
            store: self.store,
            size_bytes: self.size_bytes,
            state: PhantomData,
        }
    }
}

impl<const N: u64, K: Ord + Send, V: Send> Memtable<state::Immutable, K, V> {
    /// Retrieves a value by key from the memtable.
    pub fn flush(&self) -> Result<(), LsmError> {
        unimplemented!()
    }
}

impl<const N: u64, K: Ord + Send, V: Send> Memtable<State, K, V> {
    pub fn get(&self, key: &K) -> Option<&V> {
        self.store.get(key).and_then(|entry| match entry.value() {
            MemtableEntry::Value(v) => Some(v),
            MemtableEntry::Tombstone => None,
        })
    }
}
