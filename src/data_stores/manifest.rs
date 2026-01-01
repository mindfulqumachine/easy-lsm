use crate::err::DbError;

// manifest encapsulates all the files that make up the database.
// It consists of the WAL tables, SSTables.
const MAGIC: [u8; 4] = *b"ELSM";
const MANIFEST_EXTENSION: &str = "mf";

const MANIFEST_FORMAT_VERSION: u32 = 1;

type MagicType = [u8; 4];
type ChecksumType = u32;

mod on_disk {
    use std::{
        io::{Read, Seek},
        mem::size_of,
        ptr,
    };

    use super::*;
    use crate::err::DbError;

    #[repr(C)]
    pub(super) struct MfHdr {
        pub(super) magic: MagicType, // 4 bytes
        // The checksum of the manifest file skipping the first 64 bytes:
        // 4 bytes for magic and 4 bytes for checksum.
        pub(super) checksum: ChecksumType, // 4 bytes
        pub(super) format_version: u32,    // 4 bytes
        // The size of the manifest file.
        // u32 means max size of manifest can be 4GB which is enough for our use case.
        pub(super) size_bytes: u32, // 4 bytes
        pub(super) version: u32,    // 4 bytes
        pub(super) num_wals: u32,   // 4 bytes

        // The levels of SSTables in the database.
        pub(super) num_levels: u32, // 4 bytes
    }

    impl MfHdr {
        fn size() -> u32 {
            std::mem::size_of::<Self>() as u32
        }

        const fn offset_for_checksum() -> u64 {
            (std::mem::size_of::<MagicType>() + std::mem::size_of::<ChecksumType>()) as u64
        }
    }

    pub(super) struct Manifest {
        pub(crate) header: MfHdr,
        // Each elem of the vec represents
        // how many sstable files are in that level.
        // the len of vec is num_levels.
        pub(crate) files_per_level: Vec<u32>,

        pub(crate) wal_ids: Vec<u64>,

        // The size of the outer vec is num_levels.
        // size of the inner vecs are files_per_level[i]
        pub(crate) sstable_ids_per_level: Vec<Vec<u64>>,
    }

    impl Manifest {
        /// Instantiates the manifest for the database.
        ///
        /// # Arguments
        /// - `base_dir`: The base directory for the database.
        ///
        /// There can be multiple manifest files in the base directory. This method goes
        /// through all the files with extension `MANIFEST_EXTENSION` and picks the one with
        /// the max version.
        /// cases:
        /// - Pristine database: No manifest file. Create a new one, write to disk and return.
        /// - Existing database: Read the manifest file with max version and return.
        /// - corrupted manifest:
        ///  - magic number mismatch: Ask the user to delete the manifest. This should
        ///     bring up the next highest version manifest.
        ///  - checksum mismatch: Ask the user to delete the manifest. This should
        ///     bring up the next highest version manifest.
        ///  - format version mismatch: The database was written with an old version. Ask the
        ///     user to delete the database and start over.
        ///   We continue to ask the user to delete the corrupted manifests, until we find a
        ///   valid one or the user clears up the directory and starts over.
        fn new(base_dir: &str) -> Result<Self, DbError> {
            // 1. List all files with MANIFEST_EXTENSION in base_dir.
            let (_latest_manifest, _stats) =
                match validate_directory(base_dir, MANIFEST_EXTENSION)?
                    .map(|f| Self::verify_and_read_header(&f).map(|hdr| (f, hdr)))
                    .try_fold(None, |acc: Option<(String, MfHdr)>, res| {
                        let item = res?;
                        match acc {
                            Some(max_item) => {
                                if item.1.version > max_item.1.version {
                                    Ok(Some(item))
                                } else {
                                    Ok(Some(max_item))
                                }
                            }
                            None => Ok(Some(item)),
                        }
                    }) {
                    Ok(it) => it,
                    Err(err) => return Err(err),
                }
                .ok_or(DbError::ManifestNotFound)?;

            // 2. Filter the manifest files for:
            //   - magic number
            //   - checksum
            //   - format version

            todo!()
        }

        /// Writes the manifest to disk.
        /// It just writes it to the disk. The caller should ensures that the version is
        /// incremented before calling this method.
        fn write_to_disk(&self, _base_dir: &str) -> Result<(), DbError> {
            unimplemented!()
        }

        /// Read the head bytes of the manifest file and validate
        /// the magic number, checksum and format version.
        fn verify_and_read_header(file: &str) -> Result<MfHdr, DbError> {
            let size = size_of::<super::on_disk::MfHdr>();
            let mut buf = vec![0; size];

            let mut file_handle = std::fs::File::open(file)?;
            file_handle.read_exact(&mut buf)?;
            let hdr = unsafe { ptr::read(buf.as_ptr() as *const super::on_disk::MfHdr) };

            if hdr.magic != super::MAGIC {
                return Err(DbError::ManifestCorrupted);
            }
            if hdr.format_version != super::MANIFEST_FORMAT_VERSION {
                return Err(DbError::ManifestCorrupted);
            }

            // Now we read all the bytes of the file.
            // Then we skip the magic and checksum fields and calculate the checksum.
            // If the checksum matches, we return true.
            // Otherwise, we return false.
            let offset = super::on_disk::MfHdr::offset_for_checksum();
            let len = hdr.size_bytes as u64 - offset;
            let mut buf = vec![0; len as usize];
            file_handle.seek(std::io::SeekFrom::Start(offset))?;
            file_handle.read_exact(&mut buf)?;
            let crc32 = super::checksum(&buf);
            if crc32 == hdr.checksum {
                Ok(hdr)
            } else {
                Err(DbError::ManifestCorrupted)
            }
        }
    }
} // mod on_disk

// The in-memory representation of the manifest.

pub(crate) mod in_memory {
    pub(crate) struct Manifest {
        pub(super) version: u32,
        pub(super) wal_ids: Vec<u64>,
        pub(super) sstable_ids_per_level: Vec<Vec<u64>>,
    }

    impl Manifest {
        pub(crate) fn new() -> Self {
            Self {
                version: 0,
                wal_ids: Vec::new(),
                sstable_ids_per_level: Vec::new(),
            }
        }

        fn add_wal_id(&mut self, wal_id: u64) {
            self.wal_ids.push(wal_id);
        }

        fn create_sstable_level(&mut self) {
            self.sstable_ids_per_level.push(Vec::new());
        }
        fn add_sstable_id(&mut self, level: usize, sstable_id: u64) {
            self.sstable_ids_per_level[level].push(sstable_id);
        }
    }
} // mod in_memory

impl From<&on_disk::Manifest> for in_memory::Manifest {
    fn from(disk_mf: &on_disk::Manifest) -> Self {
        in_memory::Manifest {
            version: disk_mf.header.version,
            wal_ids: disk_mf.wal_ids.clone(),
            sstable_ids_per_level: disk_mf.sstable_ids_per_level.clone(),
        }
    }
}

impl From<&in_memory::Manifest> for on_disk::Manifest {
    fn from(mf: &in_memory::Manifest) -> Self {
        let num_levels = mf.sstable_ids_per_level.len() as u32;
        let files_per_level = mf
            .sstable_ids_per_level
            .iter()
            .map(|level_vec| level_vec.len() as u32)
            .collect();

        on_disk::Manifest {
            header: on_disk::MfHdr {
                magic: MAGIC,
                checksum: 0,
                format_version: MANIFEST_FORMAT_VERSION,
                size_bytes: 0,
                version: mf.version,
                num_wals: mf.wal_ids.len() as u32,
                num_levels,
            },
            files_per_level,
            wal_ids: mf.wal_ids.clone(),
            sstable_ids_per_level: mf.sstable_ids_per_level.clone(),
        }
    }
}

fn validate_directory(path: &str, ext: &str) -> Result<impl Iterator<Item = String>, DbError> {
    let dir = std::path::Path::new(path);
    if !dir.exists() {
        return Err(DbError::DirectoryNotFound(path.to_string()));
    }

    let ext = ext.to_string();
    let files = std::fs::read_dir(path)?.filter_map(move |res| {
        let path = res.ok()?.path();
        if path.extension().map_or(false, |e| e == ext.as_str()) {
            path.into_os_string().into_string().ok()
        } else {
            None
        }
    });

    Ok(files)
}

fn checksum(buf: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(buf);
    hasher.finalize()
}
