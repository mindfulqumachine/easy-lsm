# A Toy LSM database

Goal: To implement an LSM tree based database with
- put(key, value)
- get(key)
- del(key)

The LSM tree (Log-Structured Merge-tree) is a data structure optimized for write-heavy workloads by ensuring all disk writes are sequential. It achieves this using two main components:
1. **MemTable (Memory Table)**: An in-memory buffer where data is accumulated.
2. **SSTable (Sorted String Table)**: Immutable, sorted files on disk organized into levels.

**Write Path**: Data is written to the **WAL** (Write-Ahead Log) for durability and then inserted into the **MemTable**.
**Read Path**: Reads check the **MemTable** first. If not found, the search continues through the **SSTables** on disk, starting from Level 0 down to the deepest level.

## Architecture

![](imgs/lsm.drawio.png)
_Credit:_ https://skyzh.github.io/mini-lsm/00-overview.html
Credit for  the WAL format: https://adambcomer.com/blog/simple-database/wal/

### Data Stores

1. Memtable
    In-memory structure (SkipList) for fast access. There can be multiple memtables, but only one is mutable; others are immutable (frozen) waiting to be flushed.
2. WAL (Write-Ahead Log)
    Append-only file on disk ensuring durability of the Memtable.
3. SSTable
    Stores sorted key-value pairs on disk. Organized into 4KB Blocks, an Index Block, and a Bloom Filter Block.
4. Manifest
    The source of truth. Tracks the active SSTables, their levels, and the active WALs.

### Actors:
- Writer: Writes the data into the memtable and the WAL.
- Reader: Reads the data. Looks for the key in the memtables (newest to oldest). Then into sstables
    level 0 to N. For each ss-table:
    1. Checks if the key is within the file's min/max range (cached in memory).
    2. Checks the Bloom filter.
    3. If potentially present, loads the Index Block to find the specific Data Block.
    4. Reads the Data Block to find the key.
- Compactor: Background process that flushes frozen Memtables to L0 and merges SSTables between levels to maintain the LSM tree structure and reclaim space.

We use message passing heavily. Actors manage the state of the data stores and communicate via messages.

Let's talk about the state transitions:

1. Init: The database starts in this state. It reads the manifest to figure out where it left off,
and tries to get back to the last stable state. The DB also reads the WAL and constructs the Memtable
state.
2. RW: Read + write. The database is available for operations.
3. RWC: Read + write + Compaction. The database is serving requests while background compaction is running.

![](imgs/state-transition%20diagrams.drawio.png)

## Compaction

Compaction is done by a background thread. This is the gist of the steps it takes. Details after.

1. Flush the memtable to disk as a new L0 SSTable.
2. Update the Manifest to track the new L0 file.
3. Delete the WALs corresponding to the flushed Memtables.
4. Check compaction triggers recursively (L0 -> L1, L1 -> L2, etc.). Perform compaction if a trigger is met, update the Manifest, and then check the next level. Stop when a level's trigger is not met.

### Memtable Flush: Creating the L0 sstable files

Memtable is flushed to disk when the total memory footprint of the database
exceeds the configured threshold. This needs to be fast to release memory quickly.

When a memtable flush is triggered, the compactor thread picks up all of
the frozen memtables and attempts to flush them to disk to create new L0 sstables, one new sstable
per frozen memtable.

Let's start with the invariants:
1. All memtables are sorted.
2. All sstables are sorted internally.
3. L0 sstables may have overlapping key ranges (because they are raw dumps of memtables).
4. L1 and deeper levels have NO overlapping key ranges between files within the same level.

The steps to flush the frozen memtables are as follows:
1. Iterate over the frozen memtable(s) using a standard iterator.
2. Write the key-value pairs sequentially to a new SSTable file on disk.
3. Once the file is written, update the Manifest to include this new file in Level 0.
4. Delete the WALs corresponding to the flushed memtables.

Note: We do NOT merge with existing L0 files during flush. This ensures the flush is fast
(sequential write).

### L0 to L1 Compaction.
Trigger: When the _number_ of files in L0 exceeds a threshold (e.g., 4).

1. Pick all files in L0.
2. Determine the key range covered by these L0 files (min_key to max_key).
3. Find all files in L1 that overlap with this range.
4. Perform a K-way merge-sort of all L0 files and the overlapping L1 files.
5. Write the result to new L1 SSTable file(s). We enforce a max SSTable size (e.g., 2MB). If the data exceeds this, we cut a new file. This results in multiple files in L1.
6. Update Manifest: Remove the old L0 files and the overlapping L1 files, and add the new L1 files.

### Compaction: L1 -> L2
Trigger: When the _total size_ (sum of all file sizes) of L1 exceeds a threshold (e.g., 10MB).

1. Pick one file from L1 (usually the one with the oldest data or via round-robin).
2. Find all files in L2 that overlap with the key range of the chosen L1 file.
3. Merge-sort the L1 file and the overlapping L2 files.
4. Write the result to new L2 file(s). We enforce a max SSTable size (e.g., 2MB). If the data exceeds this, we cut a new file. This results in multiple files in L2.
5. Update Manifest: Remove the compacted L1 file and the old overlapping L2 files, and add the new L2 files.

### Compaction: L2 -> L3 (and deeper levels)
Trigger: Same as it is for L1 -> L2.

Compaction Strategy: Same as it is for L1 -> L2.

**Garbage Collection (Tombstones):**
When compacting to the bottom-most level (e.g., if L3 is the max level), we can permanently remove keys marked with tombstones, as we are guaranteed that no older version of the key exists in lower levels. This allows the database to reclaim space.

## Structures on Disk

### Manifest

Manifest contains a list of all information in the database. This includes the list of sstables and the WAL files. Anything the untouched by the manifest, is not required for the database anymore and can be deleted.

At any point there are two manifest files. The Nth and the N-1 th.
At the start, the database reads the versions of the manifest to determine the latest manifest and reads it into memory. New Manifests are written after compaction. This will create the third manifest, so as the last step of compaction, the oldest one will be deleted.
For some reason if the compaction failed and could not delete the oldest manifest, the next database startup will clear the oldest one.

#### The manifest structure:
|Field|Size|Rust Type|
|---|---|---|
|magic_bytes|4 bytes| [u8; 4] (e.g. "ELSM")|
|version|4 bytes| u32|
|num_wals|4 bytes|u32|
|wal_ids|num_wals * 4 bytes|Vec<u32>|
|num_levels|4 bytes| u32|
|... per level ...|||
|level_id|1 byte|u8|
|file_count|4 bytes|u32|
|file_ids|file_count * 4 bytes|Vec<u32>|
|checksum|4 bytes|u32 (CRC32)|

...
The same repeats for L1 files and so on.

### SStable
The full sstable is a few megabytes large. It is broken down
into several Blocks (typically 4KB).

#### File Layout
The file consists of a sequence of blocks:
1. **Data Blocks**: [Block 0, Block 1, ... Block N]
2. **Index Block**: Stores the start key and offset for every Data Block.
3. **Bloom Filter Block**: Optimization for non-existent keys.
4. **Footer**: Fixed size, contains offsets to the Index and Bloom Filter.

#### Footer
- offset to the start of the index block: 8 bytes
- offset to the start of the bloom filter block: 8 bytes
- checksum: 4 bytes (CRC32 of the offsets)
- magic number: 4 bytes

#### Data Block Format
Each Data Block contains a header and a list of entries. The block size is typically 4KB, but can be larger for large values.
- block-len: 4 bytes (u32) - Length of the remaining block (checksum + entries).
- checksum: 4 bytes (CRC32) - Checksum of the entries.
- entries:
  - key-len: 2 bytes
  - key: `key-len` bytes
  - tombstone: 1 byte (1 if deleted, 0 otherwise)
  - value-len: 4 bytes
  - value: `value-len` bytes
... repeated until block is full.

##### Blocks with keys or values larger than the default block size
The 4KB block size is a "soft limit" or target size.
1. If a key-value pair fits in the remaining space of the current 4KB block, it is added.
2. If it does not fit:
   - The current block is finalized and written to disk.
   - A new block is started.
   - If the key-value pair is larger than 4KB itself, it is written to this new block, and the block is allowed to exceed the 4KB limit. It will take up as much space as needed (header + key + value).
   - The next key-value pair starts a new block.

#### Index Block Format
The Index Block follows the same structure as the Data Block (block-len, checksum, entries). The entries are:
- key-len: 2 bytes
- key: `key-len` bytes (The first key of a data block)
- offset: 8 bytes (The offset in the file where that data block starts)

This helps us navigate the smallest key per block (keys are sorted) and avoid paging in blocks that
we do not need.

#### Bloom Filter Block Format
The Bloom Filter Block follows the same structure as the Data Block (block-len, checksum, data). The data is the raw bitset of the filter.

### WALs
Every write request is persisted in the WAL first and then written to
the memtable. There are two kinds of writes:
- inserts
- deletes

Everything is an insert or put. Updates are inserts of an existing key with a new value. Deletes are
updates of an existing key with the tombstone set.

In the rest of the section, we will go over how to represent this.

A WAL has one-to-one correspondence with the memtable. So, there is a WAL file created for each
memtable.

The structure of a WAL entry:
- CRC: 4 bytes (Checksum of the entry)
- timestamp: 8 bytes
- tombstone: 1 byte
- key-len: 2 bytes (limits key size to 65535 bytes or 65 KB).
- value-len: 4 bytes (limits value size to 4GB).
- key: `key-len` bytes
- value: `value-len` bytes

The fixed length fields are organized as a header, followed by the variable length key and value.
This makes parsing easier.

A WAL is written and read from start to finish. Therefore, we
do not need clever mechanisms like in sstable to compare and skip
ahead.
