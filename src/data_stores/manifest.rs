use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use crate::data_stores::Loggable;
use crate::data_stores::key::Key;
use crate::err::DbError;

const MANIFEST_MAGIC: u32 = 0x4D534C45; // "ELSM" in little-endian (M, S, L, E) -> E, L, S, M

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
    pub(crate) format_version: u32,
    pub(crate) wals: Vec<u32>,     // Active WAL IDs
    pub(crate) levels: Vec<Level>, // Levels of SSTables
}

impl Manifest {
    pub(crate) fn new() -> Self {
        Self {
            format_version: 1,
            wals: Vec::new(),
            levels: Vec::new(),
        }
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
        self.format_version.encode(writer)?;
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
            format_version,
            wals,
            levels,
        })
    }
}

impl Manifest {
    /// Writes the current manifest state to a new file in `base_dir`
    /// Returns the new manifest version number.
    pub(crate) fn write_to_disk(&self, base_dir: &str, next_version: u32) -> Result<(), DbError> {
        let path = Path::new(base_dir).join(format!("{:05}.mf", next_version));
        let file = File::create(&path).map_err(|e| DbError::Io(Arc::new(e)))?;
        let mut writer = BufWriter::new(file);

        self.encode(&mut writer)
            .map_err(|e| DbError::Io(Arc::new(e)))?;

        writer.flush().map_err(|e| DbError::Io(Arc::new(e)))?;
        writer
            .get_mut()
            .sync_all()
            .map_err(|e| DbError::Io(Arc::new(e)))?;

        Ok(())
    }

    /// Opens the latest manifest from `base_dir`.
    /// Returns (Manifest, current_version_number).
    pub(crate) fn open(base_dir: &str) -> Result<(Self, u32), DbError> {
        let dir = fs::read_dir(base_dir).map_err(|e| DbError::Io(Arc::new(e)))?;

        let mut manifest_files = Vec::new();

        for entry in dir {
            let entry = entry.map_err(|e| DbError::Io(Arc::new(e)))?;
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("mf") {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Ok(version) = stem.parse::<u32>() {
                        manifest_files.push((version, path));
                    }
                }
            }
        }

        // Sort by version descending
        manifest_files.sort_by(|a, b| b.0.cmp(&a.0));

        for (version, path) in manifest_files {
            let file = File::open(&path).map_err(|e| DbError::Io(Arc::new(e)))?;
            let mut reader = BufReader::new(file);

            // Attempt decode
            match Manifest::decode(&mut reader) {
                Ok(m) => return Ok((m, version)),
                Err(_) => continue, // Try older version if corrupt
            }
        }

        // If no valid manifest found, return default (new db)
        Ok((Manifest::new(), 0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_stores::key::Key;
    use crc32fast;

    #[test]
    fn test_manifest_serialization() {
        let mut m = Manifest::new();
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
        assert_eq!(decoded.format_version, 1);

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
