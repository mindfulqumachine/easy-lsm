//! The compaction logic.
//!
//! The compaction helps in reclaiming space from the deleted keys
//! and memory by flushing the memtables to disk.
//!
//! The compaction happens in its own thread in the background.
//! The thread listens on a condvar which gets a notification
//! when a new memtable is created and the current one is frozen.
//!
//! # Compaction Strategy
//!
//! ## Triggers
//! - **Memtable Flush**: Triggered when a new frozen memtable is available and the count of
//!     frozen memtables exceeds a threshold (e.g., 4).
//! - **L0 -> L1**: Triggered when the *number* of L0 files exceeds a threshold (e.g., 4).
//! - **L(N) -> L(N+1)**: Triggered when the *total size* of level N exceeds a threshold.
//!
//! ## 1. Memtable Flush (Level 0 Generation)
//!
//! **Goal**: Fast, sequential write of frozen memtables to L0 SSTables to free up memory.
//!
//! **Algorithm**:
//!
//! 1.  **Preparation (Locked)**:
//!     -   Acquire `Manifest` lock.
//!     -   Identify candidate frozen memtables from `DbVersion`.
//!     -   **Reserve IDs**: Increment `manifest.next_sstable_id` to reserve a file ID for the new SSTable.
//!     -   **Capture Version**: Record `manifest.version` to use for optimistic concurrency verification later.
//!     -   Release lock.
//!
//! 2.  **Flush (Unlocked - I/O Heavy)**:
//!     -   Iterate through frozen memtables (oldest to newest).
//!     -   Stream Key-Value pairs to a new SSTable file (id: `next_sstable_id`).
//!     -   Compute metadata (min/max keys, size, filters).
//!     -   *Note*: This happens without blocking writers.
//!
//! 3.  **Commit (Locked - Critical Section)**:
//!     -   Acquire `Manifest` lock.
//!     -   **Optimistic Verification**: Check if `manifest.version == start_version`.
//!         -   *Mismatch?*: A Writer may have rotated a memtable in the interim.
//!             This is safe (additive updates), but we must ensure we work on the *latest* state.
//!     -   **Update In-Memory Manifest**:
//!         -   Add new `FileMetadata` to L0.
//!         -   Remove flushed WAL IDs (`manifest.wals`).
//!     -   **Update On-Disk Manifest**:
//!         -   Increment `manifest.version` (e.g., `v10` -> `v11`).
//!         -   Write new manifest file: `00011.mf`.
//!         -   *Note*: `next_wal_id` and `next_sstable_id` are implicitly persisted via the file lists in the manifest.
//!     -   **Update Memory View (`DbVersion`)**:
//!         -   Construct new `DbVersion` reflecting the new SSTable and removed memtables.
//!         -   Atomically swap `Db.current`.
//!
//! ## 2. Compaction (L0 -> L1, L(N) -> L(N+1))
//! *Standard merging logic applies.*
//!
//! ## Garbage Collection (Safety)
//! We employ a **"Last-2 Versions"** retention policy for crash recoverability.
//!
//! -   **Rule**: A file (WAL or SSTable) can only be deleted if it is **NOT** referenced by:
//!     1.  The **Current Manifest** (e.g., `v11`).
//!     2.  The **Previous Manifest** (e.g., `v10`).
//!
//! -   **Process**:
//!     -   Scan `db_dir` for manifest files. Sort by version.
//!     -   Keep top 2. Delete older manifests.
//!     -   Union the set of referenced files (WALs, SSTs) from the top 2 manifests.
//!     -   Delete any on-disk file *not* in this set.
//!
//! This ensures that if `v11` is corrupted during write, we can rollback to `v10` and still have all necessary data.
use crate::DbVersion;
use crate::data_stores::manifest::{FileMetadata, Manifest};
use crate::data_stores::sstable::{Sstable, SstableWriter};
use crate::data_stores::{key::Key, sstable::SstableIterator, value::Value};
use crate::err::DbError;
use arc_swap::ArcSwap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};

fn flush_memtables(
    manifest: Arc<Mutex<Manifest>>,
    db_current: Arc<ArcSwap<DbVersion>>,
    db_dir: PathBuf,
) -> Result<(), DbError> {
    // Phase 1: Preparation (Locked)
    // We lock briefly to identify work and reserve resources.
    let (memtables_to_flush, start_sst_id, start_version, wals_to_delete) = {
        let mut manifest_guard = manifest.lock().expect("Manifest lock poisoned");

        // Trigger Check: Do we have enough frozen memtables?
        let current_version = db_current.load();
        if current_version.frozen_memtables.is_empty() {
            return Ok(());
        }

        let sst_id = manifest_guard.next_sstable_id;
        manifest_guard.next_sstable_id += 1;
        let start_version = manifest_guard.version;

        // Clone the Arcs to the memtables we need to flush so we can work on them unlocked
        let memtables = current_version.frozen_memtables.clone();

        // Capture WAL IDs that we expect to delete.
        let wals_to_delete: Vec<u32> = manifest_guard
            .wals
            .iter()
            .take(memtables.len())
            .cloned()
            .collect();

        if wals_to_delete.len() != memtables.len() {
            return Err(DbError::DataCorrupted(format!(
                "CRITICAL ERROR: WAL len {} mismatch Memtable len {}",
                manifest_guard.wals.len(),
                memtables.len()
            )));
        }

        (memtables, sst_id, start_version, wals_to_delete)
    };

    println!(
        "WORKER: Flushing {} memtables to SST ID {}",
        memtables_to_flush.len(),
        start_sst_id
    );

    // Phase 2: Flush (Unlocked)
    // We flush each memtable to its own SSTable.
    let mut new_sstables = Vec::with_capacity(memtables_to_flush.len());

    println!("FLUSH: Phase 2 Start (Iterating)");
    for (i, memtable) in memtables_to_flush.iter().enumerate() {
        let sst_id = start_sst_id + i as u32;
        let sst_path = db_dir.join(format!("{:05}.sst", sst_id));

        let mut writer = match SstableWriter::new(&sst_path) {
            Ok(w) => w,
            Err(e) => {
                return Err(DbError::Io(Arc::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to create SSTable {}: {}", sst_path.display(), e),
                ))));
            }
        };

        for entry in memtable.iter() {
            if let Err(e) = writer.write(entry.key(), entry.value()) {
                return Err(DbError::Io(Arc::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to write to SSTable {}: {}", sst_path.display(), e),
                ))));
            }
        }

        match writer.finalize() {
            Ok(meta) => {
                let mut valid_meta = meta;
                valid_meta.file_id = sst_id;
                new_sstables.push(valid_meta);
            }
            Err(e) => {
                return Err(DbError::Io(Arc::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to finalize SSTable {}: {}", sst_path.display(), e),
                ))));
            }
        }
    }

    // Pre-Open SSTables for DbVersion (I/O outside lock)
    let mut new_sst_objs = Vec::new();
    for meta in &new_sstables {
        let path = db_dir.join(format!("{:05}.sst", meta.file_id));
        match Sstable::new(&path) {
            Ok(sst) => new_sst_objs.push(Arc::new(sst)),
            Err(e) => {
                // If we fail here, we must abort because we can't construct DbVersion
                return Err(DbError::Io(Arc::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to open new SSTable {}: {}", path.display(), e),
                ))));
            }
        }
    }

    // Phase 3: Optimistic Commit Loop via Helper
    let update_result = crate::data_stores::manifest::apply_atomic_update(
        &manifest,
        &db_dir,
        |manifest_candidate| {
            // 2. Apply Changes to Candidate (Unlocked)
            // Add new SSTables
            for meta in &new_sstables {
                if manifest_candidate.levels.is_empty() {
                    manifest_candidate
                        .levels
                        .push(crate::data_stores::manifest::Level::default());
                }
                manifest_candidate.levels[0].files.push(meta.clone());
            }

            // Remove flushed WALs
            let current_wals: Vec<u32> = manifest_candidate
                .wals
                .iter()
                .take(wals_to_delete.len())
                .cloned()
                .collect();

            if current_wals != wals_to_delete {
                return Err(DbError::DataCorrupted(format!(
                    "Manifest mismatch during flush commit. Expected WALs {:?}, found {:?}",
                    wals_to_delete, current_wals
                )));
            }

            // Drain the WALs
            manifest_candidate.wals.drain(0..wals_to_delete.len());
            Ok(())
        },
    );

    match update_result {
        Ok(_) => {
            // Update DbVersion
            let current = db_current.load();
            let num_flushed = new_sstables.len();

            let new_frozen = current
                .frozen_memtables
                .iter()
                .skip(num_flushed)
                .cloned()
                .collect::<Vec<_>>();

            let mut new_sstables_levels = current.sstables.clone();
            if new_sstables_levels.is_empty() {
                new_sstables_levels.push(Vec::new());
            }
            new_sstables_levels[0].extend(new_sst_objs);

            let new_version = Arc::new(DbVersion {
                mutable_memtable: current.mutable_memtable.clone(),
                mutable_wal: current.mutable_wal.clone(),
                frozen_memtables: new_frozen,
                sstables: new_sstables_levels,
                next_lsn: current.next_lsn,
            });

            db_current.store(new_version);
        }

        Err(e) => {
            return Err(e);
        }
    }

    // Phase 4: Garbage Collection (Explict Snapshot for Safety)
    // We must NOT hold the lock during I/O.
    // So we capture the "Keep Set" while locked, then run GC.
    let (wals_keep, ssts_keep, manifests_keep) = {
        let guard = manifest.lock().map_err(|_| DbError::WriterPanic)?;
        prepare_gc_snapshot(&guard)?
    };

    garbage_collect(&db_dir, &manifests_keep, &wals_keep, &ssts_keep);
    Ok(())
}

fn prepare_gc_snapshot(
    manifest: &Manifest,
) -> Result<
    (
        std::collections::HashSet<u32>,
        std::collections::HashSet<u32>,
        Vec<String>,
    ),
    DbError,
> {
    // "Last-2 Versions" retention policy.
    // 1. Snapshot current version ID.
    // 2. We can't know "previous" versions easily from just one Manifest struct in memory unless we track history.
    //    However, the requirement is "referenced by Last-2 Manifests ON DISK".
    //    The `manifest.lock()` guards the IN-MEMORY state.
    //    The GC function needs to scan disk for manifests.
    //
    //    Wait, the original logic scanned disk for manifests, sorted them, kept top 2, and THEN loaded the previous one.
    //    The "Current" manifest in memory IS the latest one (v11).
    //    The disk might have v11 and v10.
    //
    //    So `prepare_gc_snapshot` needs to:
    //    1. Capture "Current In-Memory" references (WALs, SSTs).
    //    2. Return them.
    //
    //    ACTUALLY, to do this SAFELY without holding the lock during disk scan:
    //    The critical thing we need to know is: "What does the CURRENT manifest reference?"
    //    The "Previous" manifest is on disk. We can read it without the global lock (it's immutable if it's an old version).
    //
    //    So the snapshot should just be: "What is currently live in memory?"
    //    And strictly speaking, we want to know what the `Manifest` struct *thinks* are the live files.

    let mut wals = std::collections::HashSet::new();
    let mut ssts = std::collections::HashSet::new();

    for wal in &manifest.wals {
        wals.insert(*wal);
    }
    for level in &manifest.levels {
        for file in &level.files {
            ssts.insert(file.file_id);
        }
    }

    // We also need to know the *current version* to identify "obsolete" manifest files.
    // But `garbage_collect` logic was: "List all .mf files, sort, keep 2".
    // We can do that WITHOUT the lock, provided we trust that we don't delete the *active* one.
    // The active one is the highest version.
    //
    // Wait, if we release the lock, a new version might be created (v12).
    // Then `garbage_collect` sees v12, v11, v10. Keeps v12, v11. Deletes v10.
    // This is fine.
    //
    // The only race is:
    // 1. Thread A (GC) lists files: v10, v11.
    // 2. Thread B (Flush) creates v12.
    // 3. Thread A decides to keep v11, v10.
    // This is also fine.
    //
    // The dangerous race is:
    // 1. Thread A calculates "Live Set" from v10.
    // 2. Thread B upgrades to v11, deletes WAL 1 (which was live in v10).
    // 3. Thread A deletes WAL 1 because it thinks it's done? No.
    //
    // The Safe logic is:
    // Union(
    //   Resources referenced by Top Manifest (Disk),
    //   Resources referenced by Top-1 Manifest (Disk),
    //   Resources referenced by In-Memory Manifest (Lock Snapshot) <-- Crucial?
    // )
    //
    // Actually, the In-Memory manifest corresponds to the "Pending" or "Just written" state.
    // It should match the highest version on disk (or be ahead by 1 if not yet caught up, but here we write then update memory).
    //
    // Let's stick to the "Snapshot" approach requested:
    // Pass the "Live Set according to In-Memory Manifest" to GC.
    // GC then *also* checks the disk-based history.

    // For simplicity and safety matching the critique:
    // We just return the sets from the *current* manifest.

    // We also need to pass the "Current Version" so we don't accidentally delete the manifest file itself
    // if the directory listing is weirdly stale? Unlikely.
    // The "Keep Top 2" logic usually handles it.

    // Wait, the original `garbage_collect` logic did:
    // 1. List manifests.
    // 2. Sort.
    // 3. Keep top 2.
    // 4. Read them to find refs.
    // 5. Delete others.
    //
    // It did NOT use `manifest` argument for anything other than... checks?
    // Looking at original code:
    // `let mut referenced_wals = ...; for wal in &manifest.wals { ... }`
    // YES, it added the in-memory manifest's refs.

    Ok((
        wals,
        ssts,
        vec![], /* Manifests handled by disk scan mostly, but we could enforce keeping current version */
    ))
}

fn garbage_collect(
    db_dir: &std::path::Path,
    _keep_manifests_hint: &[String],
    live_wals_snapshot: &std::collections::HashSet<u32>,
    live_ssts_snapshot: &std::collections::HashSet<u32>,
) {
    let entries = match fs::read_dir(db_dir) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("GC: Failed to read db_dir: {}", e);
            return;
        }
    };

    let mut manifest_files = Vec::new(); // (version, filename)
    let mut other_files = Vec::new(); // (filename)

    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        let path = entry.path();
        let file_name = match path.file_name().and_then(|s| s.to_str()) {
            Some(n) => n,
            None => continue,
        };

        if file_name.ends_with(".mf") {
            let version_str = file_name.trim_end_matches(".mf");
            if let Ok(v) = version_str.parse::<u64>() {
                manifest_files.push((v, file_name.to_string()));
            }
        } else {
            other_files.push(file_name.to_string());
        }
    }

    // Sort manifests descending
    manifest_files.sort_by(|a, b| b.0.cmp(&a.0));

    // Keep top 2
    let keep_count = 2;
    let (keep_manifests, delete_manifests) = if manifest_files.len() > keep_count {
        manifest_files.split_at(keep_count)
    } else {
        (manifest_files.as_slice(), &[][..])
    };

    // Collect referenced IDs
    let mut referenced_wals = live_wals_snapshot.clone();
    let mut referenced_ssts = live_ssts_snapshot.clone();

    // Also scan the *disk* manifests we are keeping, in case they reference things
    // that the in-memory snapshot doesn't (though in-memory should be strictly newer/superset or identical).
    // Actually, in-memory might have *released* a WAL that the previous manifest still needs.
    // So we MUST scan the previous manifest on disk.

    for (_, fname) in keep_manifests {
        let p = db_dir.join(fname);
        if let Ok(file) = std::fs::File::open(&p) {
            let mut reader = std::io::BufReader::new(file);
            use crate::data_stores::Loggable;
            // We reuse Manifest decode, but generic verify might fail if we don't have full context?
            // Manifest::decode is self-contained.
            if let Ok(m) = Manifest::decode(&mut reader) {
                for wal in m.wals {
                    referenced_wals.insert(wal);
                }
                for level in m.levels {
                    for file in level.files {
                        referenced_ssts.insert(file.file_id);
                    }
                }
            }
        }
    }

    // Delete obsolete manifests
    for (_, fname) in delete_manifests {
        let p = db_dir.join(fname);
        if let Err(e) = fs::remove_file(&p) {
            eprintln!("GC: Failed to delete obsolete manifest {}: {}", fname, e);
        } else {
            println!("GC: Deleted obsolete manifest {}", fname);
        }
    }

    // Delete obsolete WALs and SSTs
    for fname in other_files {
        if fname.ends_with(".wal") {
            let id_str = fname.trim_end_matches(".wal");
            if let Ok(id) = id_str.parse::<u32>() {
                if !referenced_wals.contains(&id) {
                    let p = db_dir.join(&fname);
                    if let Err(e) = fs::remove_file(&p) {
                        eprintln!("GC: Failed to delete WAL {}: {}", fname, e);
                    } else {
                        println!("GC: Deleted obsolete WAL {}", fname);
                    }
                }
            }
        } else if fname.ends_with(".sst") {
            let id_str = fname.trim_end_matches(".sst");
            if let Ok(id) = id_str.parse::<u32>() {
                if !referenced_ssts.contains(&id) {
                    let p = db_dir.join(&fname);
                    if let Err(e) = fs::remove_file(&p) {
                        eprintln!("GC: Failed to delete SST {}: {}", fname, e);
                    } else {
                        println!("GC: Deleted obsolete SST {}", fname);
                    }
                }
            }
        }
    }
}

// Implementation of a merging iterator for compaction.
use std::collections::BinaryHeap;

struct MergeIterItem {
    key: Key,
    value: Value,
    iter_idx: usize,
}

impl PartialEq for MergeIterItem {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for MergeIterItem {}

impl PartialOrd for MergeIterItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MergeIterItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // BinaryHeap is MaxHeap, we want MinHeap behavior (smallest key first).
        // So we reverse the comparison.
        other.key.cmp(&self.key)
    }
}

pub(crate) struct MergeIterator {
    peek_heap: BinaryHeap<MergeIterItem>,
    iters: Vec<SstableIterator>,
}

impl MergeIterator {
    pub(crate) fn new(mut iters: Vec<SstableIterator>) -> Result<Self, DbError> {
        let mut heap = BinaryHeap::new();
        // Initial population
        for (i, iter) in iters.iter_mut().enumerate() {
            if let Some(res) = iter.next() {
                let (key, value) = res?;
                heap.push(MergeIterItem {
                    key,
                    value,
                    iter_idx: i,
                });
            }
        }

        Ok(Self {
            peek_heap: heap,
            iters,
        })
    }
}

impl Iterator for MergeIterator {
    type Item = Result<(Key, Value), DbError>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.peek_heap.pop()?;

        // Advance the iterator that provided this item
        let iter_idx = item.iter_idx;
        let iter = &mut self.iters[iter_idx];

        match iter.next() {
            Some(Ok((next_key, next_val))) => {
                self.peek_heap.push(MergeIterItem {
                    key: next_key,
                    value: next_val,
                    iter_idx,
                });
            }
            Some(Err(e)) => return Some(Err(e)),
            None => { /* Iterator exhausted, do nothing */ }
        }

        Some(Ok((item.key, item.value)))
    }
}

pub(crate) fn compact_l0(
    manifest_lock: Arc<Mutex<Manifest>>,
    db_current: Arc<ArcSwap<DbVersion>>,
    db_dir: &Path,
    target_file_size: u64,
) -> Result<(), DbError> {
    // 1. Selection Phase (Locked)
    let (files_l0, files_l1_overlap) = {
        let manifest = manifest_lock.lock().unwrap();

        // Validation
        if manifest.levels.first().map_or(true, |l| l.files.is_empty()) {
            return Ok(());
        }

        let l0 = &manifest.levels[0];
        let files_l0 = l0.files.clone();

        if files_l0.is_empty() {
            return Ok(());
        }

        let min_l0 = files_l0.iter().map(|f| &f.min_key).min().unwrap().clone();
        let max_l0 = files_l0.iter().map(|f| &f.max_key).max().unwrap().clone();

        let files_l1_overlap: Vec<FileMetadata> = if manifest.levels.len() > 1 {
            manifest.levels[1]
                .files
                .iter()
                .filter(|f| f.max_key >= min_l0 && f.min_key <= max_l0)
                .cloned()
                .collect()
        } else {
            Vec::new()
        };
        (files_l0, files_l1_overlap)
    }; // Unlock

    println!(
        "WORKER: Compacting L0 ({} files) + L1 ({} files)",
        files_l0.len(),
        files_l1_overlap.len()
    );

    let files_to_compact = files_l0
        .iter()
        .chain(files_l1_overlap.iter())
        .cloned()
        .collect();
    execute_compaction(
        manifest_lock,
        db_current,
        db_dir,
        0,
        files_to_compact,
        files_l0,
        files_l1_overlap,
        target_file_size,
    )
}

/// Generic compaction for Level N -> Level N+1 (where N >= 1)
pub(crate) fn compact_ln(
    manifest_lock: Arc<Mutex<Manifest>>,
    db_current: Arc<ArcSwap<DbVersion>>,
    db_dir: &Path,
    level_idx: usize,
    target_file_size: u64,
) -> Result<(), DbError> {
    // 1. Selection Phase (Locked)
    // Pick the first file from level N and find overlaps in N+1
    let (candidate_file, files_overlap) = {
        let manifest = manifest_lock.lock().unwrap();
        if manifest.levels.len() <= level_idx {
            return Ok(());
        }
        let level_files = &manifest.levels[level_idx].files;
        if level_files.is_empty() {
            return Ok(());
        }

        // Strategy: Pick the first file.
        // Improvement: We could pick the file with max overlap, or round robin,
        // but "first file" is simple and effective for now as files are sorted.
        let candidate = level_files[0].clone();

        let overlaps: Vec<FileMetadata> = if manifest.levels.len() > level_idx + 1 {
            manifest.levels[level_idx + 1]
                .files
                .iter()
                .filter(|f| f.max_key >= candidate.min_key && f.min_key <= candidate.max_key)
                .cloned()
                .collect()
        } else {
            Vec::new()
        };

        (candidate, overlaps)
    };

    println!(
        "WORKER: Compacting L{} (1 file) + L{} ({} files)",
        level_idx,
        level_idx + 1,
        files_overlap.len()
    );

    let mut files_to_compact = vec![candidate_file.clone()];
    files_to_compact.extend(files_overlap.iter().cloned());

    execute_compaction(
        manifest_lock,
        db_current,
        db_dir,
        level_idx,
        files_to_compact,
        vec![candidate_file],
        files_overlap,
        target_file_size,
    )
}

// Helper to execute merge and commit results
fn execute_compaction(
    manifest_lock: Arc<Mutex<Manifest>>,
    db_current: Arc<ArcSwap<DbVersion>>,
    db_dir: &Path,
    source_level_idx: usize,
    all_files_to_merge: Vec<FileMetadata>,
    files_to_remove_from_source: Vec<FileMetadata>,
    files_to_remove_from_target: Vec<FileMetadata>,
    target_file_size: u64,
) -> Result<(), DbError> {
    // 2. Open Iterators (Unlocked I/O)
    let mut inputs: Vec<SstableIterator> = Vec::new();
    for meta in &all_files_to_merge {
        let path = db_dir.join(format!("{:05}.sst", meta.file_id));
        match Sstable::new(&path) {
            Ok(sst) => inputs.push(SstableIterator::new(Arc::new(sst))),
            Err(e) => {
                return Err(DbError::Io(Arc::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to open SST {}: {}", path.display(), e),
                ))));
            }
        }
    }

    // 3. Merge
    let mut merge_iter = match MergeIterator::new(inputs) {
        Ok(i) => i,
        Err(e) => {
            return Err(DbError::Io(Arc::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to create merge iterator: {}", e),
            ))));
        }
    };

    // 4. Capture Start Version for optimistic check
    let start_version = { manifest_lock.lock().unwrap().version };

    // 5. Write Output
    let mut new_files = Vec::new();
    let mut current_writer: Option<(SstableWriter, u32, PathBuf)> = None;

    while let Some(res) = merge_iter.next() {
        let (key, val) = match res {
            Ok(kv) => kv,
            Err(e) => {
                // Iteration error implies IO or corruption
                return Err(e);
            }
        };

        let should_rotate = if let Some((iter_writer, _, _)) = current_writer.as_mut() {
            match iter_writer.current_size() {
                Ok(size) => size >= target_file_size,
                Err(e) => {
                    return Err(DbError::Io(Arc::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to get writer size: {}", e),
                    ))));
                }
            }
        } else {
            true
        };

        if should_rotate {
            if let Some((iter_writer, id, _path)) = current_writer.take() {
                match iter_writer.finalize() {
                    Ok(mut meta) => {
                        meta.file_id = id;
                        new_files.push(meta);
                    }
                    Err(e) => {
                        return Err(DbError::Io(Arc::new(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("Finalize error: {}", e),
                        ))));
                    }
                }
            }

            let next_id = {
                let mut guard = manifest_lock.lock().unwrap();
                let id = guard.next_sstable_id;
                guard.next_sstable_id += 1;
                id
            };

            let path = db_dir.join(format!("{:05}.sst", next_id));
            match SstableWriter::new(&path) {
                Ok(w) => current_writer = Some((w, next_id, path)),
                Err(e) => {
                    return Err(DbError::Io(Arc::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to create SST: {}", e),
                    ))));
                }
            }
        }

        if let Some((iter_writer, _, _)) = current_writer.as_mut() {
            if let Err(e) = iter_writer.write(&key, &val) {
                return Err(DbError::Io(Arc::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Write error: {}", e),
                ))));
            }
        }
    }

    // Finalize last file
    if let Some((iter_writer, id, _)) = current_writer.take() {
        match iter_writer.finalize() {
            Ok(mut meta) => {
                meta.file_id = id;
                new_files.push(meta);
            }
            Err(e) => {
                return Err(DbError::Io(Arc::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Finalize error: {}", e),
                ))));
            }
        }
    }

    // Pre-Open New SSTables (IO outside lock)
    let mut new_sst_objs = Vec::new();
    for meta in &new_files {
        let path = db_dir.join(format!("{:05}.sst", meta.file_id));
        match Sstable::new(&path) {
            Ok(sst) => new_sst_objs.push(Arc::new(sst)),
            Err(e) => {
                return Err(DbError::Io(Arc::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to open new SST {}: {}", path.display(), e),
                ))));
            }
        }
    }

    // 6. Optimistic Commit Loop via Helper
    let update_result = crate::data_stores::manifest::apply_atomic_update(
        &manifest_lock,
        &db_dir,
        |manifest_candidate| {
            let target_level_idx = source_level_idx + 1;

            // Ensure target level exists
            while manifest_candidate.levels.len() <= target_level_idx {
                manifest_candidate
                    .levels
                    .push(crate::data_stores::manifest::Level::default());
            }

            // Remove source files
            manifest_candidate.levels[source_level_idx]
                .files
                .retain(|f| {
                    !files_to_remove_from_source
                        .iter()
                        .any(|rem| rem.file_id == f.file_id)
                });

            // Remove target overlap files
            manifest_candidate.levels[target_level_idx]
                .files
                .retain(|f| {
                    !files_to_remove_from_target
                        .iter()
                        .any(|rem| rem.file_id == f.file_id)
                });

            // Add new files
            manifest_candidate.levels[target_level_idx]
                .files
                .extend(new_files.clone());

            // CRITICAL: Sort by min_key
            manifest_candidate.levels[target_level_idx]
                .files
                .sort_by(|a, b| a.min_key.cmp(&b.min_key));

            Ok(())
        },
    );

    match update_result {
        Ok(_) => {
            // Update DbVersion
            let current_db_ver = db_current.load();
            let mut new_levels = current_db_ver.sstables.clone();

            // Ensure capacity
            let target_level_idx = source_level_idx + 1;
            while new_levels.len() <= target_level_idx {
                new_levels.push(Vec::new());
            }

            // Filter source
            if source_level_idx < new_levels.len() {
                new_levels[source_level_idx].retain(|sst| {
                    !files_to_remove_from_source
                        .iter()
                        .any(|f| f.file_id == sst.id)
                });
            }

            // Filter target
            if target_level_idx < new_levels.len() {
                new_levels[target_level_idx].retain(|sst| {
                    !files_to_remove_from_target
                        .iter()
                        .any(|f| f.file_id == sst.id)
                });
            }

            // Add new to target
            new_levels[target_level_idx].extend(new_sst_objs);

            // Sort target level
            new_levels[target_level_idx].sort_by(|a, b| a.min_key().cmp(b.min_key()));

            let new_version = Arc::new(DbVersion {
                mutable_memtable: current_db_ver.mutable_memtable.clone(),
                mutable_wal: current_db_ver.mutable_wal.clone(),
                frozen_memtables: current_db_ver.frozen_memtables.clone(),
                sstables: new_levels,
                next_lsn: current_db_ver.next_lsn,
            });

            db_current.store(new_version);
        }
        Err(e) => {
            return Err(e);
        }
    }

    // 8. Cleanup (Unlocked)
    // 8. Cleanup (Explict Snapshot for Safety)
    let (wals_keep, ssts_keep, manifests_keep) = {
        let guard = manifest_lock.lock().map_err(|_| DbError::WriterPanic)?;
        prepare_gc_snapshot(&guard)?
    };

    garbage_collect(&db_dir, &manifests_keep, &wals_keep, &ssts_keep);
    Ok(())
}

/// The entry point for the compaction thread.
pub fn run(
    manifest_lock: Arc<Mutex<Manifest>>,
    db_current: Arc<ArcSwap<DbVersion>>,
    compaction_cv: Arc<Condvar>,
    shutdown: Arc<AtomicBool>,
    db_dir: PathBuf,
) {
    // Triggers Configuration
    const L0_SIZE_TRIGGER_MB: u64 = 128;
    const L0_SIZE_TRIGGER: u64 = L0_SIZE_TRIGGER_MB * 1024 * 1024;
    const TARGET_FILE_SIZE: u64 = 64 * 1024 * 1024;
    const MAX_LEVELS: usize = 4; // L0 + 4 levels (1..4)

    let mut manifest_guard = manifest_lock.lock().unwrap();

    loop {
        // Check for shutdown signal before waiting
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        // Check Flush Trigger
        if manifest_guard.wals.len() > 1 {
            if manifest_guard.wals.len() > 4 {
                println!("Compaction Trigger: Memtable flush needed");
                drop(manifest_guard); // Unlock
                if let Err(e) =
                    flush_memtables(manifest_lock.clone(), db_current.clone(), db_dir.clone())
                {
                    eprintln!("Flush failed: {}", e);
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }
                // Re-acquire lock
                manifest_guard = manifest_lock.lock().unwrap();
                continue;
            }
        }

        // Check Compaction Trigger
        // 1. L0 Trigger
        let l0_size: u64 = manifest_guard
            .levels
            .first()
            .map_or(0, |l0| l0.files.iter().map(|f| f.file_size as u64).sum());

        if l0_size >= L0_SIZE_TRIGGER {
            println!(
                "Compaction Trigger: L0 Compaction needed (Size: {} bytes)",
                l0_size
            );
            drop(manifest_guard);
            if let Err(e) = compact_l0(
                manifest_lock.clone(),
                db_current.clone(),
                &db_dir,
                TARGET_FILE_SIZE,
            ) {
                eprintln!("L0 Compaction failed: {}", e);
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
            manifest_guard = manifest_lock.lock().unwrap();
            continue;
        }

        // 2. Ln Triggers
        let mut triggered_level = None;
        for level in 1..MAX_LEVELS {
            if level >= manifest_guard.levels.len() {
                break;
            }
            let ln_size: u64 = manifest_guard.levels[level]
                .files
                .iter()
                .map(|f| f.file_size as u64)
                .sum();
            let target_size = L0_SIZE_TRIGGER * 10u64.pow(level as u32);

            if ln_size >= target_size {
                triggered_level = Some(level);
                break;
            }
        }

        if let Some(level) = triggered_level {
            println!("Compaction Trigger: L{} Compaction needed", level);
            drop(manifest_guard);
            if let Err(e) = compact_ln(
                manifest_lock.clone(),
                db_current.clone(),
                &db_dir,
                level,
                TARGET_FILE_SIZE,
            ) {
                eprintln!("Ln Compaction failed: {}", e);
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
            manifest_guard = manifest_lock.lock().unwrap();
            continue;
        }

        // No work? Wait.
        manifest_guard = compaction_cv.wait(manifest_guard).unwrap();
    }
}

#[cfg(test)]
#[path = "compact_tests.rs"]
mod compact_tests;
