pub(crate) type KeyLenType = u16;
pub(crate) type LsnType = u64;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Key {
    pub lsn: LsnType,
    pub bytes: Vec<u8>,
}

impl Key {
    pub(crate) fn new(bytes: &[u8], lsn: LsnType) -> Self {
        Self {
            lsn,
            bytes: bytes.to_vec(),
        }
    }

    // convert to on disk representation.
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.lsn.to_le_bytes());
        let key_len = self.bytes.len() as u16;
        buf.extend_from_slice(&key_len.to_le_bytes());
        buf.extend_from_slice(&self.bytes);
        buf
    }
}

pub(crate) mod on_disk {
    use super::{KeyLenType, LsnType};
    #[repr(C)]
    pub(crate) struct Key {
        lsn: LsnType,
        key_len: KeyLenType,
        key_data: [u8; 0],
    }

    impl Key {
        // Get byte representation from the repr C struct.
        pub(crate) fn to_bytes(&self) -> &[u8] {
            todo!()
        }
    }
}

impl From<&Key> for on_disk::Key {
    fn from(_key: &Key) -> Self {
        todo!()
    }
}
