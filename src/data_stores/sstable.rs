use crate::data_stores::key::Key;

pub(crate) struct Sstable {
    #[allow(dead_code)]
    file_pointer: std::fs::File,
    #[allow(dead_code)]
    bloom_filter: Vec<u8>,
    #[allow(dead_code)]
    sstable_min_key: Key,
    #[allow(dead_code)]
    sstable_max_key: Key,

    // Each elem represents (min_key, max_key, block_starting_offset)
    #[allow(dead_code)]
    block_level_min_max_keys: Vec<(Key, Key, usize)>,
}
