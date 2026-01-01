use crate::data_stores::key::Key;

pub(crate) struct Sstable {
    file_pointer: std::fs::File,
    bloom_filter: Vec<u8>,
    sstable_min_key: Key,
    sstable_max_key: Key,

    // Each elem represents (min_key, max_key, block_starting_offset)
    block_level_min_max_keys: Vec<(Key, Key, usize)>,
}
