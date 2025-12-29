type KEY_LEN_TYPE = u16;
type VALUE_LEN_TYPE = u16;
type CHECKSUM_TYPE = u32;
type ENTRIES_TYPE = u32;

struct Key {
    key_len: KEY_LEN_TYPE,
    key_data: &[u8],
}

struct Value {
    value_len: VALUE_LEN_TYPE,
    value_data: &[u8],
}

#[repr(C)]
struct Header {
    checksum: u32,
    entries: u32,
}

impl Key {
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let len_size = std::mem::size_of::<KEY_LEN_TYPE>();
        let key_len = KEY_LEN_TYPE::from_le_bytes(bytes[0..len_size].try_into().unwrap());
        let key_data = &bytes[len_size..len_size + key_len as usize];
        Key { key_len, key_data }
    }

    // Returns the size of the bytes occupied by the key.
    // This is the sum of the key_len field
    fn size(&self) -> usize {
        std::mem::size_of::<KEY_LEN_TYPE>() + self.key_len as usize
    }
}

impl Value {
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let len_size = std::mem::size_of::<VALUE_LEN_TYPE>();
        let value_len = VALUE_LEN_TYPE::from_le_bytes(bytes[0..len_size].try_into().unwrap());
        let value_data = &bytes[len_size..len_size + value_len as usize];
        Value {
            value_len,
            value_data,
        }
    }
}

impl Header {
    const SIZE: usize = std::mem::size_of::<Self>();

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let checksum_size = std::mem::size_of::<CHECKSUM_TYPE>();
        let entries_size = std::mem::size_of::<ENTRIES_TYPE>();

        let checksum = CHECKSUM_TYPE::from_le_bytes(bytes[0..checksum_size].try_into().unwrap());
        let entries = ENTRIES_TYPE::from_le_bytes(
            bytes[checksum_size..checksum_size + entries_size]
                .try_into()
                .unwrap(),
        );
        Header { checksum, entries }
    }
}

/// A block is a collection of key and value pairs stored together.
struct Block<const BLOCK_SIZE: usize> {
    buffer: &mut [u8; BLOCK_SIZE],
}

// Implement an iterator for the Block to read key-value pairs.
impl<const BLOCK_SIZE: usize> Block<BLOCK_SIZE> {
    pub fn iter(&self) -> BlockIterator {
        let header = Header::from_bytes(&self.buffer[0..Header::SIZE]);
        BlockIterator {
            buffer: self.buffer + Header::SIZE, // Skip the header worth of bytes as we have parsed it
            position: 0,
            index: 0,
            entries: header.entries as usize,
        }
    }
}

struct BlockIterator<'a> {
    buffer: &'a [u8],
    position: usize,

    // Which entry we are currently on, out of the total number of entries.
    index: usize,
    entries: usize,
}

impl<'a> Iterator for BlockIterator<'a> {
    type Item = (Key, Value);

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.buffer.len() {
            return None;
        } else if self.index >= self.entries {
            return None;
        } else {
            // skip the header worth of bytes.
            // Then read the next 2bytes(u16) as key_len.
            // Then read the next key_len bytes worth as the key.
            // Then read the next 2bytes(u16) as value_len.
            // Then read the next value_len bytes worth as the value.
            let key = Key::from_bytes(&self.buffer[self.position..]);
            self.position += key.size();
            let value = Value::from_bytes(&self.buffer[self.position..]);
            self.position += value.size();
            self.index += 1;
            Some((key, value))
        }
    }
}
