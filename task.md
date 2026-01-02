Tasks:
[x] 1. Implement the data serialization/deserialization organization into key.rs and value.rs.
[x] 2. Implement the logic to read from the manifest at startup.
    a. Manifest in memory and on-disk structure.
    b. reading and writing to manifest.
[x] 6. Incorporate manifest searching and reading as part of startup.
    a. [x] Refactor Manifest discovery logic (recover_or_init).
    b. [x] Refactor Db::new to use unified recovery flow.
    c. [x] Implement LSN consistency checks.
[x] 3. Memtable size check and freeze
    a. check if a memtable has reached the max size in write_req's write_states::LeaderWritingMemtable state.
    b. if so, freeze it and start a new memtable with the corresponding wal.
    c. update the manifest with the new items and persist to disk.
[x] 4. Implement the sstable logic
    a. the structure on disk. Read the README.md for details.
    b. loading the sstable's index and bloom filter to disk only.
    c. efficient searching using the index and bloom filter.
[] 5. Compaction logic.
    a. start  in a thread with the database.
    b. handle thread crashes.
    c. Flush memtable to disk.
    d. updating manifest.
    e. merge for L0 sstables to L1.
    f. comapct L_N sstables to L_(N+1) (where N >= 1).
