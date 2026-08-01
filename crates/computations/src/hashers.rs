//! A tiny hand-rolled `Hasher`/`BuildHasher` pair for maps and sets whose
//! keys are dominated by an already-uniformly-distributed content hash --
//! see [`crate::key::Hash128`].
//!
//! ## Why this is safe
//!
//! `Hash128` is the first 16 bytes of a blake3 digest (see its own docs):
//! cryptographically uniform over its output space *by construction*,
//! regardless of what was hashed to produce it. Feeding an already-uniform
//! 128-bit value through `std`'s default SipHash-1-3 -- a hasher designed to
//! resist an adversary who chooses the *input* bytes to engineer bucket
//! collisions in an attacker-controlled `HashMap` (the classic HashDoS
//! attack, e.g. a web server hashing untrusted request headers) -- buys
//! nothing here: nothing in this engine's key space is adversary-controlled
//! independently of also controlling the (already blake3-hashed) value the
//! `Hash128` identifies, so there is no HashDoS surface beyond what the
//! 128-bit content hash already implies. Using the hash's own bits verbatim
//! as the map's bucket hash is therefore exactly as safe as the 128-bit
//! hash itself already is, while skipping a second, redundant cryptographic
//! mixing pass -- Stage 6 profiling (see
//! `docs/persistence-benchmark-notes.md`) measured `std`'s SipHash costing
//! 5.6-8.7% of self-time across every phase profiled, largely spent
//! re-hashing these already-hashed keys.
//!
//! **This hasher must never be applied to a key type containing genuinely
//! attacker-controlled or low-entropy bytes** (e.g. `crate::source::RawDep`
//! or other string/byte-keyed side tables) -- those still need `std`'s
//! default SipHash and are deliberately left alone; see the call sites that
//! use this module for which key types qualify. `crate::interner::SrcKeyId`
//! is the same shape of exception as `Hash128`: it is a dense, process-local
//! `u32` handle assigned by `crate::interner::SrcKeyInterner`, never a value
//! an adversary chooses directly, so `crate::engine::EngineInner`'s
//! `source_index` (keyed on it, Stage 13 -- see
//! `docs/persistence-benchmark-notes.md`) uses this hasher too. The raw
//! key/version *bytes* a `SrcKeyId` stands in for stay off this hasher
//! entirely -- interning them is exactly what removes them from any hot,
//! per-dependent hash path.
//!
//! ## Design
//!
//! [`IdentityHasher`] keeps a single `u64` accumulator, initialized to `0`:
//! - `write_u64`/`write_u128` -- what [`crate::key::Hash128`]'s `Hash` impl
//!   calls (see its docs) -- fold the incoming bits in with a cheap
//!   rotate-xor rather than overwrite. For the common case of a key hashed
//!   via exactly one `write_u64` call from a fresh accumulator (any
//!   `HashMap<Hash128, _>`), this reduces to the identity function: the
//!   map's bucket hash *is* the `Hash128`'s own first 8 bytes, verbatim, no
//!   mixing at all. For a multi-field key (e.g. `crate::key::CompKey`,
//!   whose `Hash` impl also hashes a short `DefId` string first), the
//!   rotate-xor still combines both fields' contributions correctly.
//! - `write_u32` -- what a bare `u32` newtype (e.g.
//!   `crate::interner::SrcKeyId`) hashes through -- folds in via the same
//!   rotate-xor as `write_u64`/`write_u128` rather than falling through to
//!   the default trait method (which would `write()` its 4 bytes through
//!   the FNV fold below instead: correct, but a multi-step loop where one
//!   `rotate_left`/`xor` does the same job in one instruction).
//! - `write` (arbitrary bytes) falls back to a plain FNV-1a fold, cheap and
//!   correct for the short, non-adversarial byte spans this hasher
//!   otherwise ever sees (a `DefId`'s name, a handful of bytes) -- still no
//!   SipHash-style multi-round finalization.
use std::hash::{BuildHasherDefault, Hasher};

/// See the module docs.
#[derive(Default)]
pub(crate) struct IdentityHasher(u64);

impl Hasher for IdentityHasher {
    fn write(&mut self, bytes: &[u8]) {
        // FNV-1a, folded into whatever's already accumulated so this
        // combines correctly with another field's contribution hashed
        // before or after it in the same `Hash::hash` call (e.g. `CompKey`
        // hashing its `DefId` field via `write`, then its `Hash128` field
        // via `write_u64`).
        let mut h = self.0;
        for &b in bytes {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x0000_0100_0000_01B3); // FNV-1a prime
        }
        self.0 = h;
    }

    fn write_u32(&mut self, i: u32) {
        self.write_u64(u64::from(i));
    }

    fn write_u64(&mut self, i: u64) {
        self.0 = self.0.rotate_left(5) ^ i;
    }

    fn write_u128(&mut self, i: u128) {
        self.write_u64(i as u64);
        self.write_u64((i >> 64) as u64);
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

/// `BuildHasher` for [`IdentityHasher`]. Use as a `HashMap`/`HashSet`'s
/// third type parameter for a key type dominated by a
/// [`crate::key::Hash128`] (see this module's docs) -- construct the
/// collection with `::default()`, not `::new()` (which only exists for the
/// standard `RandomState` hasher).
pub(crate) type IdentityBuildHasher = BuildHasherDefault<IdentityHasher>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::hash::Hash;

    use crate::key::{CompKey, DefId, Hash128};

    fn hash_of<T: Hash>(v: &T) -> u64 {
        let mut h = IdentityHasher::default();
        v.hash(&mut h);
        h.finish()
    }

    #[test]
    fn hash128_hash_is_first_8_bytes_verbatim() {
        let mut bytes = [0u8; 16];
        for (i, b) in bytes.iter_mut().enumerate() {
            *b = i as u8;
        }
        let h128 = Hash128::from_bytes(bytes);
        let expected = u64::from_le_bytes(bytes[..8].try_into().unwrap());
        assert_eq!(hash_of(&h128), expected);
    }

    #[test]
    fn write_u32_matches_write_u64_zero_extended() {
        // A bare `u32` newtype (e.g. `crate::interner::SrcKeyId`) must fold
        // in exactly like `write_u64` would on its zero-extended value, not
        // fall through to the default trait method's byte-wise FNV fold.
        let mut via_u32 = IdentityHasher::default();
        via_u32.write_u32(0x1234_5678);
        let mut via_u64 = IdentityHasher::default();
        via_u64.write_u64(0x1234_5678);
        assert_eq!(via_u32.finish(), via_u64.finish());
    }

    #[test]
    fn equal_hash128_values_hash_equal() {
        let a = Hash128::from_bytes([7u8; 16]);
        let b = Hash128::from_bytes([7u8; 16]);
        assert_eq!(hash_of(&a), hash_of(&b));
    }

    #[test]
    fn equal_comp_keys_hash_equal() {
        let def = DefId::new("probe");
        let key_a = CompKey::new(def, &5i32);
        let key_b = CompKey::new(def, &5i32);
        assert_eq!(hash_of(&key_a), hash_of(&key_b));
    }

    #[test]
    fn different_param_hash_usually_changes_the_hash() {
        let def = DefId::new("probe");
        let key_a = CompKey::new(def, &5i32);
        let key_b = CompKey::new(def, &6i32);
        // Not a strict guarantee (hash collisions are always possible) but
        // exercises the combining path for a real difference.
        assert_ne!(hash_of(&key_a), hash_of(&key_b));
    }
}
