use serde::{Deserialize, Serialize};
use siphasher::sip::SipHasher13;
use std::hash::{Hash, Hasher};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BloomFilter {
    bitmap: Vec<u64>,
    bitmap_bits: u64,
    k_num: u32,
    sip_keys: [(u64, u64); 2],
}

impl BloomFilter {
    pub fn new(items_count: usize, fp_rate: f64) -> Self {
        let ln2 = 0.69314718056;
        let bits_per_item = -1.0 * fp_rate.ln() / (ln2 * ln2);
        let num_bits = (items_count as f64 * bits_per_item).ceil() as u64;
        let k_num = (ln2 * (num_bits as f64) / items_count as f64).ceil() as u32;

        // Align to u64
        let bitmap_size = ((num_bits + 63) / 64) as usize;
        let bitmap = vec![0u64; bitmap_size];

        // Generate random keys for SipHash
        // Using fixed keys for determinism in this implementation or random?
        // Ideally random, but for simple tests fixed is fine.
        // Let's use pseudo-random for now to avoid 'rand' dependency if not present.
        let sip_keys = [
            (0x5068486940525249, 0x1234567890ABCDEF),
            (0xFEDCBA0987654321, 0x1029384756102938),
        ];

        Self {
            bitmap,
            bitmap_bits: num_bits,
            k_num,
            sip_keys,
        }
    }

    pub fn set<Q: ?Sized + Hash>(&mut self, key: &Q) {
        let (h1, h2) = self.hash_kernel(key);

        for i in 0..self.k_num {
            let idx = (h1.wrapping_add((i as u64).wrapping_mul(h2))) % self.bitmap_bits;
            self.set_bit(idx);
        }
    }

    pub fn check<Q: ?Sized + Hash>(&self, key: &Q) -> bool {
        let (h1, h2) = self.hash_kernel(key);

        for i in 0..self.k_num {
            let idx = (h1.wrapping_add((i as u64).wrapping_mul(h2))) % self.bitmap_bits;
            if !self.get_bit(idx) {
                return false;
            }
        }
        true
    }

    fn hash_kernel<Q: ?Sized + Hash>(&self, key: &Q) -> (u64, u64) {
        let mut s1 = SipHasher13::new_with_keys(self.sip_keys[0].0, self.sip_keys[0].1);
        key.hash(&mut s1);
        let h1 = s1.finish();

        let mut s2 = SipHasher13::new_with_keys(self.sip_keys[1].0, self.sip_keys[1].1);
        key.hash(&mut s2);
        let h2 = s2.finish();

        (h1, h2)
    }

    fn set_bit(&mut self, idx: u64) {
        let word_idx = (idx / 64) as usize;
        let bit_idx = idx % 64;
        self.bitmap[word_idx] |= 1 << bit_idx;
    }

    fn get_bit(&self, idx: u64) -> bool {
        let word_idx = (idx / 64) as usize;
        let bit_idx = idx % 64;
        (self.bitmap[word_idx] & (1 << bit_idx)) != 0
    }
}
