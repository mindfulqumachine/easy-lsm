type ValueLenType = u32;

#[derive(Clone)]
pub enum Value {
    Bytes(Vec<u8>),
    Str(String),
    Int(i64),
    Tombstone,
}

impl Value {
    pub(crate) fn new(bytes: &[u8]) -> Self {
        Value::Bytes(bytes.to_vec())
    }

    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            Value::Bytes(b) => {
                buf.push(0); // not tombstoned
                buf.push(0); // type bytes
                buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                buf.extend_from_slice(b);
            }
            Value::Str(s) => {
                buf.push(0);
                buf.push(1); // type str
                buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
                buf.extend_from_slice(s.as_bytes());
            }
            Value::Int(i) => {
                buf.push(0);
                buf.push(2); // type int
                buf.extend_from_slice(&(8u32).to_le_bytes());
                buf.extend_from_slice(&i.to_le_bytes());
            }
            Value::Tombstone => {
                buf.push(1); // tombstoned
                buf.push(0); // type irrelevant
                buf.extend_from_slice(&(0u32).to_le_bytes());
            }
        }
        buf
    }
}

pub(crate) mod on_disk {
    use super::ValueLenType;
    #[repr(C)]
    pub(crate) struct Value {
        tombstoned: u8,
        value_type: u8,
        value_len: ValueLenType,
        value_data: [u8; 0],
    }

    impl Value {
        // Get byte representation from the repr C struct.
        pub(crate) fn to_bytes(&self) -> &[u8] {
            todo!()
        }
    }
}

impl From<&Value> for on_disk::Value {
    fn from(_value: &Value) -> Self {
        todo!()
    }
}
