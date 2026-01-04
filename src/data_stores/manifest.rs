use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use crate::data_stores::Loggable;
use crate::data_stores::key::Key;
use crate::err::DbError;

const MANIFEST_MAGIC: u32 = 0x4D534C45; // "ELSM" in little-endian (M, S, L, E) -> E, L, S, M

// Update this version when you change the on-disk
// format of manifest.
const MANIFEST_FORMAT_VERSION: u32 = 0;

#[derive(Debug, Clone)]
pub(crate) struct FileMetadata {
    pub(crate) file_id: u32,
    pub(crate) file_size: u32,
    pub(crate) min_key: Key,
    pub(crate) max_key: Key,
}

impl Loggable for FileMetadata {
    fn encode(&self, writer: &mut impl Write) -> std::io::Result<()> {
        self.file_id.encode(writer)?;
        self.file_size.encode(writer)?;
        self.min_key.encode(writer)?;
        self.max_key.encode(writer)?;
        Ok(())
    }

    fn decode(reader: &mut impl Read) -> Result<Self, DbError> {
        let file_id = u32::decode(reader)?;
        let file_size = u32::decode(reader)?;
        let min_key = Key::decode(reader)?;
        let max_key = Key::decode(reader)?;
        Ok(Self {
            file_id,
            file_size,
            min_key,
            max_key,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct Level {
    pub(crate) files: Vec<FileMetadata>,
}

impl Loggable for Level {
    fn encode(&self, writer: &mut impl Write) -> std::io::Result<()> {
        // Vec<FileMetadata> will write [Len: u32][FileMetadata...]
        self.files.encode(writer)
    }

    fn decode(reader: &mut impl Read) -> Result<Self, DbError> {
        let files = Vec::<FileMetadata>::decode(reader)?;
        Ok(Self { files })
    }
}

/// The Manifest represents the snapshot of the database state.
#[derive(Debug, Clone)]
pub struct Manifest {
    pub(crate) wals: Vec<u32>,     // Active WAL IDs
    pub(crate) levels: Vec<Level>, // Levels of SSTables

    // Transient IDs (recovered at startup)
    pub(crate) next_wal_id: u32,
    pub(crate) next_sstable_id: u32,
    pub(crate) version: u32,
}

impl Manifest {
    pub(crate) const MANIFEST_EXTENSION: &str = ".mf";

    /// Creates a new, empty manifest.
    /// This is called during the pristine start of the database.
    /// Most likely you are looking for the
    /// try_open() function.
    /// Creates a new manifest.
    /// Requires the ID of the first WAL to ensure valid state.
    pub(crate) fn new(initial_wal_id: u32) -> Self {
        Self {
            wals: vec![initial_wal_id],
            levels: Vec::new(),
            next_wal_id: initial_wal_id + 1,
            next_sstable_id: 0,
            version: 0,
        }
    }

    // Read the manifest file off the disk and initialize the manifest structure.
    pub(crate) fn try_open(manifest_file_path: &Path) -> Result<Self, DbError> {
        let file = File::open(manifest_file_path).map_err(|e| DbError::Io(Arc::new(e)))?;
        let mut reader = BufReader::new(file);
        let mut manifest = Self::decode(&mut reader)?;

        let manifest_version = Path::new(manifest_file_path)
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.parse::<u32>().ok())
            .ok_or(DbError::ManifestReadError(format!(
                "Invalid manifest file name: {}",
                manifest_file_path.display()
            )))?;

        manifest.version = manifest_version;

        // Initialize transient counters based on loaded data
        let max_wal_id = manifest.wals.iter().max().copied().unwrap_or(0);
        manifest.next_wal_id = max_wal_id + 1;

        let max_sst_id = manifest
            .levels
            .iter()
            .flat_map(|l| &l.files)
            .map(|f| f.file_id)
            .max()
            .unwrap_or(0);
        manifest.next_sstable_id = max_sst_id + 1;

        Ok(manifest)
    }

    /// Attempts to load the latest valid manifest from `db_dir`.
    ///
    /// - **Scans** for `*.mf` files.
    /// - **Validates**: If a corrupt/invalid manifest is found, returns `Err` asking user to delete it.
    /// - **Pristine Case**: If no manifest exists, creates a new one, initializes `00000.wal`, persists both, and returns the new manifest.
    /// - **Existing Case**: Returns the latest valid manifest.
    pub(crate) fn recover_or_init(db_dir: &Path) -> Result<Self, DbError> {
        std::fs::read_dir(db_dir)
            .map_err(|e| DbError::Io(Arc::new(e)))?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry.path().extension().map_or(false, |ext| {
                    ext == Manifest::MANIFEST_EXTENSION.trim_start_matches('.')
                })
            })
            .filter_map(|entry| {
                entry
                    .path()
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .and_then(|stem| stem.parse::<u32>().ok())
                    .map(|id| (id, entry.path()))
            })
            .max_by_key(|(id, _)| *id) // gives the manifest with the highest ID (version) if exists.
            .map(|(_id, path)| {
                Manifest::try_open(&path).map_err(|e| {
                    DbError::ManifestReadError(format!(
                        "Failed to open manifest at {:?}: {}. Please delete this file and retry.",
                        path, e
                    ))
                })
            }) // If found, try to open it.
            // If not found, create a new manifest and  return it.
            .unwrap_or_else(|| Manifest::prepare_pristine_start(db_dir))
    }

    fn prepare_pristine_start(db_dir: &Path) -> Result<Manifest, DbError> {
        let wal_id = 0;

        // Create the first WAL on disk.
        // We use Wal::create_at to ensure correct naming and initialization.
        // We drop the resulting Wal object because Db::new will re-open it as part of startup.
        use crate::data_stores::wal::Wal;
        let wal = Wal::<crate::data_stores::wal::wal_states::Writable>::open(db_dir, wal_id)?;

        // Create manifest with the ID of the WAL we just created.
        let m = Manifest::new(wal.id);

        // Persist the new manifest
        m.write_to_disk(
            db_dir.to_str().ok_or(DbError::DirectoryNotFound(
                "Invalid path encoding".to_string(),
            ))?,
            0,
        )?;

        Ok(m)
    }
    // This writes the current manifest data to disk.
    pub(crate) fn write_to_disk(&self, base_dir: &str, new_version: u32) -> Result<(), DbError> {
        let filename = format!("{:05}{}", new_version, Self::MANIFEST_EXTENSION);
        let path = Path::new(base_dir).join(filename);
        self.write_to_path(&path)
    }

    pub(crate) fn write_to_path(&self, path: &Path) -> Result<(), DbError> {
        let file = File::create(path).map_err(DbError::from)?;
        let mut writer = BufWriter::new(file);
        self.encode(&mut writer).map_err(DbError::from)?;
        writer.flush().map_err(DbError::from)?;
        writer.get_ref().sync_all().map_err(DbError::from)?;
        Ok(())
    }

    /// Returns the next available WAL ID.
    pub(crate) fn next_wal_id(&self) -> u32 {
        self.next_wal_id
    }

    // / Commits the new WAL ID to the manifest and persists.
    // / This should be called AFTER the WAL file is successfully created on disk.
    // / We require the `Wal` reference as proof that it has been created.
    pub fn commit_new_wal(
        &mut self,
        wal: &crate::data_stores::wal::Wal<crate::data_stores::wal::wal_states::Writable>,
        base_dir: &str,
    ) -> Result<(), DbError> {
        self.wals.push(wal.id);
        self.write_to_disk(base_dir, self.version + 1)?;
        self.version += 1;
        Ok(())
    }
}

pub fn apply_atomic_update<F>(
    manifest_lock: &std::sync::Arc<std::sync::Mutex<Manifest>>,
    db_dir: &Path,
    mutator: F,
) -> Result<Manifest, DbError>
where
    F: Fn(&mut Manifest) -> Result<(), DbError>,
{
    let mut loop_count = 0;
    loop {
        loop_count += 1;
        if loop_count > 3 {
            return Err(DbError::Io(Arc::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Manifest commit loop exceeded retry limit",
            ))));
        }

        // 1. Snapshot
        let (current_version, mut candidate) = {
            let guard = manifest_lock.lock().map_err(|_| DbError::WriterPanic)?;
            (guard.version, guard.clone())
        };

        // 2. Mutate
        mutator(&mut candidate)?;

        // Ensure version increments
        candidate.version += 1;

        // 3. Serialize to TMP
        let new_version_num = candidate.version;
        let tmp_filename = format!("{:05}.mf.tmp", new_version_num);
        let tmp_path = db_dir.join(&tmp_filename);
        let final_filename = format!("{:05}.mf", new_version_num);
        let final_path = db_dir.join(&final_filename);

        candidate.write_to_path(&tmp_path)?;

        // 4. Commit (Compare & Swap)
        let mut guard = manifest_lock.lock().map_err(|_| DbError::WriterPanic)?;
        if guard.version != current_version {
            // Conflict: Version changed.
            // Cleanup temp file and retry.
            let _ = std::fs::remove_file(&tmp_path);
            continue;
        }

        // Atomic Rename
        if let Err(e) = std::fs::rename(&tmp_path, &final_path) {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(DbError::from(e));
        }

        // Update Memory
        *guard = candidate.clone();

        // Return the new manifest state
        return Ok(candidate);
    }
}

impl Loggable for Manifest {
    fn encode(&self, writer: &mut impl Write) -> std::io::Result<()> {
        // Buffer the body to calculate size and checksum
        let mut body_buf = Vec::new();

        // Body:
        // 1. Wals (Vec<u32>)
        self.wals.encode(&mut body_buf)?;
        // 2. Levels (Vec<Level>)
        self.levels.encode(&mut body_buf)?;

        // Checksum
        let checksum = crc32fast::hash(&body_buf);
        let size = body_buf.len() as u32;

        // Header:
        // 1. Magic
        MANIFEST_MAGIC.encode(writer)?;
        // 2. Checksum
        checksum.encode(writer)?;
        // 3. Size
        size.encode(writer)?;
        // 4. Format Version
        MANIFEST_FORMAT_VERSION.encode(writer)?;
        // 5. Num Wals (redundant but in spec)
        (self.wals.len() as u32).encode(writer)?;
        // 6. Num Levels (redundant but in spec)
        (self.levels.len() as u32).encode(writer)?;

        // Write Body
        writer.write_all(&body_buf)?;

        Ok(())
    }

    fn decode(reader: &mut impl Read) -> Result<Self, DbError> {
        // Header
        let magic = u32::decode(reader)?;
        if magic != MANIFEST_MAGIC {
            return Err(DbError::ManifestCorrupted);
        }

        let checksum = u32::decode(reader)?;
        let size = u32::decode(reader)?;
        let format_version = u32::decode(reader)?;
        if format_version != MANIFEST_FORMAT_VERSION {
            return Err(DbError::ManifestCorrupted);
        }
        let _num_wals = u32::decode(reader)?; // We trust the Vec's built-in length
        let _num_levels = u32::decode(reader)?;

        // Body
        let mut body_buf = vec![0u8; size as usize];
        reader
            .read_exact(&mut body_buf)
            .map_err(|e| DbError::Io(Arc::new(e)))?;

        // Validation
        let calculated_crc = crc32fast::hash(&body_buf);
        if calculated_crc != checksum {
            return Err(DbError::ManifestCorrupted);
        }

        // Decode from buffer
        let mut body_reader = &body_buf[..];
        let wals = Vec::<u32>::decode(&mut body_reader)?;
        let levels = Vec::<Level>::decode(&mut body_reader)?;

        Ok(Self {
            wals,
            levels,
            // Transient fields are initialized by try_open / recover_or_init
            next_wal_id: 0,
            next_sstable_id: 0,
            version: 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_stores::key::Key;
    use crc32fast;

    #[test]
    fn test_manifest_serialization() {
        let mut m = Manifest::new(1);
        m.wals = vec![1, 2, 3];

        let k1 = Key::new(b"a", 100);
        let k2 = Key::new(b"z", 200);

        m.levels.push(Level {
            files: vec![FileMetadata {
                file_id: 99,
                file_size: 1024,
                min_key: k1.clone(),
                max_key: k2.clone(),
            }],
        });

        let mut buf = Vec::new();
        m.encode(&mut buf).unwrap();

        let mut reader = &buf[..];
        let decoded = Manifest::decode(&mut reader).unwrap();

        assert_eq!(decoded.wals, vec![1, 2, 3]);
        assert_eq!(decoded.levels.len(), 1);
        assert_eq!(decoded.levels[0].files.len(), 1);
        assert_eq!(decoded.levels[0].files[0].file_id, 99);

        // Verify Magic Number
        let magic_slice = &buf[0..4];
        let magic = u32::from_le_bytes(magic_slice.try_into().unwrap());
        assert_eq!(magic, MANIFEST_MAGIC, "Magic number mismatch");

        // Verify Checksum
        // Header is 6 * 4 = 24 bytes (Magic, Crc, Size, Ver, NumWals, NumLevels)
        // Body starts at 24.
        let body_slice = &buf[24..];
        let computed_crc = crc32fast::hash(body_slice);
        let stored_crc_slice = &buf[4..8];
        let stored_crc = u32::from_le_bytes(stored_crc_slice.try_into().unwrap());
        assert_eq!(stored_crc, computed_crc, "CRC mismatch");
    }
}
