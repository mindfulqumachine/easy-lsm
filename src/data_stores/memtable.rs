use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_skiplist::SkipMap;

use crate::data_stores::{key::Key, value::Value};
use crate::err::DbError;
use crate::write_req::WriteRequest;

pub mod state {
    /// Designator for an active memtable.
    #[derive(Debug)]
    pub struct Mutable;
    /// This memtable is frozen and does not accept writes.
    #[derive(Debug)]
    pub struct Immutable;
}

/// A memTable representation for a LSM tree.
///
/// State: One of state::Mutable | state::Immutable | state::Flushed
/// N:  Total allowed size for the memtable in bytes.
/// K: Key type. Must implement Ord for the inherent ordering requirements of the LSM tree.
/// V: Value type.
#[derive(Debug)]
pub(crate) struct Memtable<State> {
    // The primary storage for keys and values.
    store: SkipMap<Key, Value>,

    // Use atomic for size tracking to avoid &mut requirement if we want concurrent reads/size checks?
    // put_batch takes &self because SkipMap handles concurrency.
    size_bytes: AtomicUsize,
    state: PhantomData<State>,
}

impl Memtable<state::Mutable> {
    /// Creates a new mutable memtable.
    pub fn new() -> Memtable<state::Mutable> {
        Self {
            store: SkipMap::new(),
            size_bytes: AtomicUsize::new(0),
            state: PhantomData,
        }
    }

    /// Inserts a batch of write requests into the memtable.
    ///
    /// This utilizes `Arc` in `Key` and `Value` to avoid deep copying.
    /// It calculates the total size delta and updates `size_bytes`.
    pub fn put_batch(&self, reqs: &[Arc<WriteRequest>]) {
        for req in reqs {
            self.recover(req.key.clone(), req.value.clone());
        }
    }

    /// Inserts a key-value pair directly into the memtable.
    /// Used for recovery from WAL.
    pub fn recover(&self, key: Key, value: Value) {
        let k_len = key.bytes.len();
        let v_len = match &value {
            Value::Bytes(b) => b.len(),
            Value::Str(s) => s.len(),
            Value::Int(_) => 8,
            Value::Tombstone => 0,
        };

        // Overhead: Key + Value headers
        let entry_size =
            Key::SERIALIZED_HEADER_SIZE + k_len + Value::SERIALIZED_HEADER_SIZE + v_len;

        self.size_bytes.fetch_add(entry_size, Ordering::Relaxed);
        self.store.insert(key, value);
    }

    /// Freezes the memtable, preventing further writes.
    #[allow(dead_code)]
    pub fn freeze(self) -> Memtable<state::Immutable> {
        Memtable {
            store: self.store,
            size_bytes: self.size_bytes,
            state: PhantomData,
        }
    }

    #[allow(dead_code)]
    pub fn del(&mut self, _key: &Key) -> Result<(), DbError> {
        // TODO: Implement delete if needed, or remove.
        Ok(())
    }
}

impl Memtable<state::Immutable> {
    /// Retrieves a value by key from the memtable.
    #[allow(dead_code)]
    pub fn flush(&self) -> Result<(), DbError> {
        unimplemented!()
    }
}

impl<State> Memtable<State> {
    pub fn get(&self, key_bytes: &[u8]) -> Option<Value> {
        // Range scan to find the entry with correct key and highest LSN.
        // Key sorts by bytes ASC, then lsn DESC.
        // Latest version is the "smallest" key in sorting order for this byte-key.
        // Range: [Key(k, MAX), Key(k, 0)]

        let start = Key::new(key_bytes, u64::MAX);
        let end = Key::new(key_bytes, 0);

        // range is (Bound<T>, Bound<T>)
        // The range method on a skip-list does notiterate through the
        // the theoritical gap between start and end. It finds the first
        // actual item matching the start bound and then iterates only
        // through the actual items present. because we call next() immediate,
        // it is guarantted to only return the latest version of the key with
        // O(log N) time complexity where N being the total number of keys in
        // memtable.
        let range = self.store.range(start..=end);

        // Get the first entry (latest version)
        if let Some(entry) = range.into_iter().next() {
            let val = entry.value();
            match val {
                Value::Tombstone => None,
                // Large value types are wrapped in Arc.
                // Str(Arc<str>), Bytes(Arc<Vec<u8>>)
                // so they are cheap to clone.
                _ => Some(val.clone()),
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_stores::key::Key;
    use crate::data_stores::value::Value;
    use crate::write_req::{Writer, write_states};
    use std::sync::Arc;

    // Helper to create a request using Writer::new
    fn create_req(lsn: u64, key: &[u8], val: Value) -> Arc<WriteRequest> {
        let key_struct = Key {
            lsn: std::sync::atomic::AtomicU64::new(lsn),
            bytes: Arc::from(key),
        };
        // We use Writer::new but we need to inject our LSN into the key properly.
        // Writer::new initally sets key.lsn to 0 (default in Key definition? No we pass Key)
        // Key construction above sets lsn.
        let writer = Writer::<write_states::WaitingToBeQueued>::new(key_struct, val);
        writer.req
    }

    #[test]
    fn test_memtable_basics() {
        let mem = Memtable::new();

        let req1 = create_req(1, b"key1", Value::Bytes(Arc::from(b"value1".as_slice())));
        let req2 = create_req(2, b"key2", Value::Str(Arc::from("value2")));

        mem.put_batch(&[req1, req2]);

        // Check finding key1
        // We need to use exact key bytes
        let res1 = mem.get(b"key1");
        match res1 {
            Some(Value::Bytes(b)) => assert_eq!(&*b, b"value1"),
            v => panic!("Expected Bytes(value1), got {:?}", v),
        }

        // Check finding key2
        let res2 = mem.get(b"key2");
        match res2 {
            Some(Value::Str(s)) => assert_eq!(&*s, "value2"),
            v => panic!("Expected Str(value2), got {:?}", v),
        }

        // Check not finding key3
        assert!(mem.get(b"key3").is_none());
    }

    #[test]
    fn test_memtable_overwrites() {
        let mem = Memtable::new();

        // Write version 1
        let req1 = create_req(100, b"key1", Value::Bytes(Arc::from(b"v1".as_slice())));
        mem.put_batch(&[req1]);

        if let Some(Value::Bytes(b)) = mem.get(b"key1") {
            assert_eq!(&*b, b"v1");
        } else {
            panic!("Should find v1");
        }

        // Write version 2
        let req2 = create_req(200, b"key1", Value::Bytes(Arc::from(b"v2".as_slice())));
        mem.put_batch(&[req2]);

        // Should find v2 (highest lsn)
        if let Some(Value::Bytes(b)) = mem.get(b"key1") {
            assert_eq!(&*b, b"v2");
        } else {
            panic!("Should find v2");
        }

        // Write version 3 (Str)
        let req3 = create_req(300, b"key1", Value::Str(Arc::from("v3")));
        mem.put_batch(&[req3]);

        if let Some(Value::Str(s)) = mem.get(b"key1") {
            assert_eq!(&*s, "v3");
        } else {
            panic!("Should find v3");
        }
    }

    #[test]
    fn test_memtable_tombstones() {
        let mem = Memtable::new();

        // Write version 1
        let req1 = create_req(100, b"keyX", Value::Bytes(Arc::from(b"exist".as_slice())));
        mem.put_batch(&[req1]);
        assert!(mem.get(b"keyX").is_some());

        // Write tombstone
        let req2 = create_req(101, b"keyX", Value::Tombstone);
        mem.put_batch(&[req2]);

        // Should return None
        assert!(mem.get(b"keyX").is_none());
    }

    #[test]
    #[ignore]
    fn test_read_from_frozen_memtable() {
        todo!("Verify Db::get checks frozen memtables");
    }
}
