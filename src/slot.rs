//! Valkey key hash slot computation (CRC16-CCITT/XMODEM % 16384) with `{hashtag}`
//! support — identical to Valkey's `keyHashSlot` / `ClusterKeySlot`.
//!
//! We compute it in-module (Valkey does not pre-store the slot per key in
//! standalone mode). Used to shard objects into per-slot subdirectories so that
//! concurrent open()/create() calls hit different directory inodes instead of
//! serializing on one shared directory lock.

/// Number of Valkey cluster slots.
pub const NUM_SLOTS: u16 = 16384;

/// CRC16-CCITT (XMODEM): poly 0x1021, init 0x0000, no reflection, no xorout.
/// Bit-for-bit identical to Valkey's `crc16()` table.
fn crc16(bytes: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &b in bytes {
        crc ^= (b as u16) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
        }
    }
    crc
}

/// Extract the hashtag range per Valkey semantics: if the key contains `{`
/// followed by a `}` with at least one byte between them, only the bytes
/// between are hashed; otherwise the whole key is hashed.
fn hashtag<'a>(key: &'a [u8]) -> &'a [u8] {
    if let Some(open) = key.iter().position(|&c| c == b'{') {
        if let Some(rel) = key[open + 1..].iter().position(|&c| c == b'}') {
            if rel > 0 {
                return &key[open + 1..open + 1 + rel];
            }
        }
    }
    key
}

/// Valkey hash slot for a key: `crc16(hashtag(key)) % 16384`. Range 0..16383.
pub fn key_hash_slot(key: &[u8]) -> u16 {
    crc16(hashtag(key)) % NUM_SLOTS
}

#[cfg(test)]
mod tests {
    use super::*;

    // Known Valkey slot values (from redis-cli CLUSTER KEYSLOT).
    #[test]
    fn known_slots() {
        assert_eq!(key_hash_slot(b"123456789"), 12739);
        assert_eq!(key_hash_slot(b"foo"), 12182);
        assert_eq!(key_hash_slot(b""), 0);
    }

    #[test]
    fn hashtag_groups_keys() {
        // Same {tag} => same slot regardless of surrounding bytes.
        assert_eq!(key_hash_slot(b"{user1000}.foo"), key_hash_slot(b"{user1000}.bar"));
        // Empty tag {} => hash the whole key.
        assert_ne!(key_hash_slot(b"{}foo"), key_hash_slot(b"{}bar"));
    }
}
