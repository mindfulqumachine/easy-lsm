type ValueLenType = u32;

use std::sync::Arc;

#[derive(Clone, Debug)]
pub enum Value {
    Bytes(Arc<[u8]>),
    // Optimization: Use Arc<str> to avoid expensive string cloning when queueing writes.
    // This allows the write path to be zero-copy for the payload until persistence.
    Str(Arc<str>),
    Int(i64),
    Tombstone,
}

impl Value {
    // Bit 0 = Tombstone
    // Bits 1..7 = Type
    pub(crate) const WAL_ID_TOMBSTONE: u8 = 1 << 0;

    pub(crate) const WAL_TYPE_BYTES: u8 = 0 << 1;
    pub(crate) const WAL_TYPE_STR: u8 = 1 << 1;
    pub(crate) const WAL_TYPE_INT: u8 = 2 << 1;

    pub(crate) fn new(bytes: &[u8]) -> Self {
        Value::Bytes(Arc::from(bytes))
    }
}

impl super::Loggable for Value {
    fn encode(&self, writer: &mut impl std::io::Write) -> std::io::Result<()> {
        match self {
            Value::Bytes(b) => {
                Self::WAL_TYPE_BYTES.encode(writer)?;
                // Arc<[u8]> doesn't implement Loggable directly, convert to vec or write manually.
                // But efficient way is to cast functionality.
                // We want [Len: u32][Bytes]. Vec<u8> does exactly this.
                let v = b.to_vec();
                v.encode(writer)?;
            }
            Value::Str(s) => {
                Self::WAL_TYPE_STR.encode(writer)?;
                let v = s.as_bytes().to_vec();
                v.encode(writer)?;
            }
            Value::Int(i) => {
                Self::WAL_TYPE_INT.encode(writer)?;
                // Int is special: [Len: u32 = 8][Val: i64]
                // We can't use Vec<u8> generic here easily because it would write len=8.
                // Wait, Vec<u8>::encode writes [Len:u32][Bytes].
                // So if we have 8 bytes of int, Vec<u8> will write 8 then 8 bytes.
                // Perfect.
                let bytes = i.to_le_bytes().to_vec();
                bytes.encode(writer)?;
            }
            Value::Tombstone => {
                Self::WAL_ID_TOMBSTONE.encode(writer)?;
                // Tombstone payload is [Len: 0].
                let v: Vec<u8> = Vec::new();
                v.encode(writer)?;
            }
        }
        Ok(())
    }

    fn decode(reader: &mut impl std::io::Read) -> Result<Self, crate::err::DbError> {
        // 1. Meta (u8)
        let meta = u8::decode(reader)?;

        // Check Tombstone (Bit 0)
        if meta & Self::WAL_ID_TOMBSTONE != 0 {
            // Tombstone entry still has a length field (0).
            // We must consume it.
            let _ = Vec::<u8>::decode(reader)?;
            return Ok(Value::Tombstone);
        }

        let val_type = meta & !Self::WAL_ID_TOMBSTONE;

        // 2. Data (Vec<u8> handles Len + Bytes)
        let val_bytes = Vec::<u8>::decode(reader)?;

        match val_type {
            Self::WAL_TYPE_BYTES => Ok(Value::Bytes(Arc::from(val_bytes))),
            Self::WAL_TYPE_STR => {
                let s = String::from_utf8(val_bytes).unwrap_or_default();
                Ok(Value::Str(Arc::from(s)))
            }
            Self::WAL_TYPE_INT => {
                if val_bytes.len() >= 8 {
                    let bytes: [u8; 8] = val_bytes[0..8].try_into().unwrap();
                    Ok(Value::Int(i64::from_le_bytes(bytes)))
                } else {
                    Err(crate::err::DbError::ManifestCorrupted)
                }
            }
            _ => Err(crate::err::DbError::ManifestCorrupted),
        }
    }
}
