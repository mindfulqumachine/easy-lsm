pub(crate) mod block;
pub mod bloom_filter;
pub(crate) mod manifest;
pub(crate) mod memtable;
pub(crate) mod sstable;
pub(crate) mod wal;

pub(crate) mod key;
pub(crate) mod value;

/// A trait for objects that can be serialized to a log (WAL, Manifest, etc).
/// This unifies the "Value" concept on disk: everything is a byte sequence.
pub(crate) trait Loggable: Sized {
    /// Encode the object into the writer.
    fn encode(&self, writer: &mut impl std::io::Write) -> std::io::Result<()>;

    /// Decode the object from the reader.
    fn decode(reader: &mut impl std::io::Read) -> Result<Self, crate::err::DbError>;
}

// --- Primitive Implementations ---

impl Loggable for u8 {
    fn encode(&self, writer: &mut impl std::io::Write) -> std::io::Result<()> {
        writer.write_all(&[*self])
    }

    fn decode(reader: &mut impl std::io::Read) -> Result<Self, crate::err::DbError> {
        let mut buf = [0u8; 1];
        reader
            .read_exact(&mut buf)
            .map_err(|e| crate::err::DbError::Io(std::sync::Arc::new(e)))?;
        Ok(buf[0])
    }
}

impl Loggable for u16 {
    fn encode(&self, writer: &mut impl std::io::Write) -> std::io::Result<()> {
        writer.write_all(&self.to_le_bytes())
    }

    fn decode(reader: &mut impl std::io::Read) -> Result<Self, crate::err::DbError> {
        let mut buf = [0u8; 2];
        reader
            .read_exact(&mut buf)
            .map_err(|e| crate::err::DbError::Io(std::sync::Arc::new(e)))?;
        Ok(u16::from_le_bytes(buf))
    }
}

impl Loggable for u32 {
    fn encode(&self, writer: &mut impl std::io::Write) -> std::io::Result<()> {
        writer.write_all(&self.to_le_bytes())
    }

    fn decode(reader: &mut impl std::io::Read) -> Result<Self, crate::err::DbError> {
        let mut buf = [0u8; 4];
        reader
            .read_exact(&mut buf)
            .map_err(|e| crate::err::DbError::Io(std::sync::Arc::new(e)))?;
        Ok(u32::from_le_bytes(buf))
    }
}

impl Loggable for u64 {
    fn encode(&self, writer: &mut impl std::io::Write) -> std::io::Result<()> {
        writer.write_all(&self.to_le_bytes())
    }

    fn decode(reader: &mut impl std::io::Read) -> Result<Self, crate::err::DbError> {
        let mut buf = [0u8; 8];
        reader
            .read_exact(&mut buf)
            .map_err(|e| crate::err::DbError::Io(std::sync::Arc::new(e)))?;
        Ok(u64::from_le_bytes(buf))
    }
}

// Generic implementation for Vectors (Arrays)
// Format: [Len: u32][Item 0]...[Item N]
impl<T: Loggable> Loggable for Vec<T> {
    fn encode(&self, writer: &mut impl std::io::Write) -> std::io::Result<()> {
        let len = self.len() as u32;
        len.encode(writer)?;
        for item in self {
            item.encode(writer)?;
        }
        Ok(())
    }

    fn decode(reader: &mut impl std::io::Read) -> Result<Self, crate::err::DbError> {
        let len = u32::decode(reader)?;
        let mut vec = Vec::with_capacity(len as usize);
        for _ in 0..len {
            vec.push(T::decode(reader)?);
        }
        Ok(vec)
    }
}

// Helper wrapper for raw byte arrays that don't need the u32 length prefix
// or need a specific length type (like u16 for Keys).
// However, strictly following "Everything is a Value", we should use the standard Vec implementation
// where possible.
// For now, String is useful for file paths etc.
impl Loggable for String {
    fn encode(&self, writer: &mut impl std::io::Write) -> std::io::Result<()> {
        let bytes = self.as_bytes().to_vec();
        bytes.encode(writer)
    }

    fn decode(reader: &mut impl std::io::Read) -> Result<Self, crate::err::DbError> {
        let bytes = Vec::<u8>::decode(reader)?;
        String::from_utf8(bytes).map_err(|_| crate::err::DbError::ManifestCorrupted)
    }
}
