use std::{
    cmp::Ordering,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering as AtomicOrdering},
    },
};

pub(crate) type KeyLenType = u16;
pub(crate) type LsnType = u64;

#[derive(Debug)]
pub(crate) struct Key {
    pub lsn: AtomicU64,
    pub bytes: Arc<[u8]>,
}

impl Clone for Key {
    fn clone(&self) -> Self {
        Self {
            lsn: AtomicU64::new(self.lsn.load(AtomicOrdering::Relaxed)),
            bytes: self.bytes.clone(),
        }
    }
}

impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes
            && self.lsn.load(AtomicOrdering::Relaxed) == other.lsn.load(AtomicOrdering::Relaxed)
    }
}

impl Eq for Key {}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        // Sort by bytes ASC, then lsn ASC
        match self.bytes.cmp(&other.bytes) {
            Ordering::Equal => {
                let l1 = self.lsn.load(std::sync::atomic::Ordering::Relaxed);
                let l2 = other.lsn.load(std::sync::atomic::Ordering::Relaxed);
                // Sort LSN in descending order so latest version comes first.
                l2.cmp(&l1)
            }
            other => other,
        }
    }
}

impl Key {
    pub(crate) fn new(bytes: &[u8], lsn: LsnType) -> Self {
        Self {
            lsn: AtomicU64::new(lsn),
            bytes: Arc::from(bytes),
        }
    }

    pub(crate) fn update_lsn(&self, lsn: u64) {
        self.lsn.store(lsn, AtomicOrdering::Relaxed);
    }
}

impl super::Loggable for Key {
    // convert to on disk representation.
    fn encode(&self, writer: &mut impl std::io::Write) -> std::io::Result<()> {
        // 1. LSN (u64)
        self.lsn.load(AtomicOrdering::Relaxed).encode(writer)?;

        // 2. Length (u16)
        let key_len = self.bytes.len() as u16;
        key_len.encode(writer)?;

        // 3. Bytes (Raw) - Not using Vec<u8> Loggable because that prefixes u32 length.
        // We already wrote u16 length.
        writer.write_all(&self.bytes)
    }

    fn decode(reader: &mut impl std::io::Read) -> Result<Self, crate::err::DbError> {
        // 1. LSN (u64)
        let lsn = u64::decode(reader)?;

        // 2. Length (u16)
        let key_len = u16::decode(reader)?;

        // 3. Bytes
        let mut key_bytes = vec![0u8; key_len as usize];
        reader
            .read_exact(&mut key_bytes)
            .map_err(|e| crate::err::DbError::Io(Arc::new(e)))?;

        Ok(Self {
            lsn: AtomicU64::new(lsn),
            bytes: Arc::from(key_bytes),
        })
    }
}
