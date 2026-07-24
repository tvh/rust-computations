//! Identifiers naming computations and data sources in the dependency graph.
//!
//! A computation application `c p` (in the terminology of the self-adjusting
//! computation literature) is identified by the name of its definition
//! together with a content hash of its parameter. This module provides that
//! identity: [`Hash128`] is the content hash primitive (the analogue of
//! `LargeHashable`), [`DefId`] names a definition, and [`CompKey`] names a
//! specific application of a definition to a parameter (the analogue of
//! `CompKey` / `c p`).

use std::collections::{HashMap, HashSet};
use std::fmt;

use serde::de::DeserializeOwned;

use crate::hashers::IdentityBuildHasher;

/// A 128-bit content hash: the first 16 bytes of a blake3 digest of a
/// canonical byte encoding.
///
/// 128 bits (rather than the full 256-bit blake3 output) is a deliberate
/// size/collision-risk tradeoff: at even billions of distinct computation
/// applications, the birthday-bound collision probability of a 128-bit hash
/// is still astronomically negligible, and 128 bits is what the original
/// self-adjusting-computation paper's own `LargeHashable` used. Truncating a
/// blake3 digest to its first 16 bytes is a safe, standard use of blake3's
/// extensible output (any prefix of a longer blake3 output is itself a
/// valid, independently-secure shorter output).
///
/// `Hash128` is a plain value type: cheap to copy, comparable, hashable, and
/// totally ordered so it can be used as a map/set key or sorted for stable
/// output.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Hash128([u8; 16]);

/// Hashes via a single `write_u64` of this hash's own first 8 bytes,
/// instead of `#[derive(Hash)]`'s default of writing all 16 raw bytes.
///
/// `Hash128` is already a uniformly-distributed content hash (see the type
/// docs): folding only its first 8 bytes into a `Hasher` loses no
/// practically-relevant bucket-distribution quality versus folding all 16 --
/// 64 bits of already-random entropy is astronomically more than any
/// in-process `HashMap` needs to keep bucket collisions negligible, and
/// full 128-bit equality is still exactly what `Eq` checks; this only
/// narrows what feeds the *hash*, never what decides equality. Writing
/// exactly one `u64` (rather than 16 raw bytes) is also what lets
/// [`crate::hashers::IdentityHasher`] treat a lone `Hash128` key as a true
/// identity hash, with no mixing at all -- see that module's docs for the
/// full rationale (this is the profiler-identified optimization from
/// `docs/persistence-benchmark-notes.md`'s Stage 6).
impl std::hash::Hash for Hash128 {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let mut low8 = [0u8; 8];
        low8.copy_from_slice(&self.0[..8]);
        state.write_u64(u64::from_le_bytes(low8));
    }
}

/// A `HashMap` keyed by [`Hash128`] using
/// [`crate::hashers::IdentityBuildHasher`] instead of `std`'s default
/// SipHash -- see that module's docs for why this is safe for a key type
/// whose hash is already uniformly-distributed content. Construct with
/// `::default()`, not `::new()`.
pub(crate) type Hash128Map<V> = HashMap<Hash128, V, IdentityBuildHasher>;

impl Hash128 {
    /// Builds a `Hash128` from raw bytes (typically a truncated blake3
    /// digest; see [`Self::from_blake3`]).
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Hash128(bytes)
    }

    /// Builds a `Hash128` by truncating a full 256-bit blake3 digest to its
    /// first 16 bytes. See the type-level docs for why this truncation is
    /// safe.
    pub fn from_blake3(hash: blake3::Hash) -> Self {
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&hash.as_bytes()[..16]);
        Hash128(bytes)
    }

    /// Returns the raw bytes of this hash.
    pub const fn as_bytes(&self) -> [u8; 16] {
        self.0
    }

    /// Writes the first `len` bytes of this hash as lowercase hex.
    fn write_hex(&self, f: &mut fmt::Formatter<'_>, len: usize) -> fmt::Result {
        for byte in &self.0[..len] {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }

    /// Renders the first 4 bytes (8 hex characters) of this hash, for
    /// tracing fields and other diagnostics that want a short, stable id
    /// without the full digest.
    pub(crate) fn short_hex(&self) -> String {
        use std::fmt::Write;
        let mut s = String::with_capacity(8);
        for byte in &self.0[..4] {
            let _ = write!(s, "{byte:02x}");
        }
        s
    }
}

impl fmt::Display for Hash128 {
    /// Prints the full hash as 32 lowercase hex characters.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.write_hex(f, self.0.len())
    }
}

impl fmt::Debug for Hash128 {
    /// Prints a short, human-scannable form: the first 8 hex characters.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Hash128(")?;
        self.write_hex(f, 4)?;
        write!(f, ")")
    }
}

/// A type that can be deterministically hashed to a stable [`Hash128`].
///
/// The hash is derived from a canonical (postcard) serialization of the
/// value, so it is stable across process runs and platforms as long as the
/// value's `Serialize` implementation is deterministic (e.g. it must not
/// depend on hash-map iteration order).
pub trait StableHash {
    /// Computes the stable content hash of `self`.
    fn stable_hash(&self) -> Hash128;
}

impl<T: serde::Serialize + ?Sized> StableHash for T {
    fn stable_hash(&self) -> Hash128 {
        let bytes = postcard::to_stdvec(self)
            .expect("postcard serialization of a well-formed value should not fail");
        Hash128::from_blake3(blake3::hash(&bytes))
    }
}

/// The stable identity of a computation *definition*.
///
/// A `DefId` names a definition (e.g. `"fibonacci"`), independent of any
/// particular parameter it might be applied to; see [`CompKey`] for the
/// identity of a specific application. `DefId` is `Copy`: it wraps a
/// `&'static str` rather than an owned/reference-counted string, since
/// computation names are startup-time string literals in practice. A caller
/// that truly needs a runtime-generated name can `Box::leak` it into a
/// `&'static str` — this keeps identity `Copy` everywhere without needing an
/// intern table.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct DefId(&'static str);

impl DefId {
    /// Creates a `DefId` for the definition named `name`.
    pub const fn new(name: &'static str) -> Self {
        DefId(name)
    }

    /// Returns the definition's name.
    pub fn name(&self) -> &str {
        self.0
    }
}

impl fmt::Debug for DefId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DefId({})", self.0)
    }
}

impl fmt::Display for DefId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Identifies a specific application of a computation definition to a
/// parameter value (`c p`): the definition's [`DefId`] plus a stable hash of
/// the parameter.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CompKey {
    def: DefId,
    param_hash: Hash128,
}

impl CompKey {
    /// Builds the key for `def` applied to `param`, content-hashing the
    /// parameter to obtain a stable identity.
    pub fn new(def: DefId, param: &impl serde::Serialize) -> Self {
        let param_hash = param.stable_hash();
        CompKey { def, param_hash }
    }

    /// Returns the identity of the underlying definition.
    pub fn def(&self) -> &DefId {
        &self.def
    }

    /// Returns the content hash of the parameter.
    pub fn param_hash(&self) -> Hash128 {
        self.param_hash
    }

    /// Reconstructs a `CompKey` directly from its constituent parts, without
    /// hashing a live parameter value.
    ///
    /// Used by `crate::persist` to rebuild a dependency edge's `CompKey`
    /// from its persisted `(def name, param hash)` pair, where the
    /// dependency's own parameter value was never stored (only ever the
    /// identity of the dependency, which is exactly `def` + `param_hash`).
    pub(crate) fn from_parts(def: DefId, param_hash: Hash128) -> Self {
        CompKey { def, param_hash }
    }
}

impl fmt::Debug for CompKey {
    /// Prints as `name#shorthash`, e.g. `fibonacci#a1b2c3d4`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}#", self.def.name())?;
        self.param_hash.write_hex(f, 4)
    }
}

/// A `HashSet<CompKey>` using [`crate::hashers::IdentityBuildHasher`]: safe
/// for the same reason [`Hash128Map`] is, since `CompKey`'s `Hash` impl
/// (derived) is dominated by its [`Hash128`] `param_hash` field (its other
/// field, `DefId`, wraps a short definition name). Construct with
/// `::default()`, not `::new()`.
pub(crate) type CompKeySet = HashSet<CompKey, IdentityBuildHasher>;

/// A `HashMap<CompKey, V>` using [`crate::hashers::IdentityBuildHasher`];
/// see [`CompKeySet`].
pub(crate) type CompKeyMap<V> = HashMap<CompKey, V, IdentityBuildHasher>;

/// Bound satisfied by every valid computation parameter type.
///
/// `DeserializeOwned` (in addition to `Serialize`) is required so a param
/// can be revived from its persisted postcard bytes when restoring a node
/// from a previous run (see `crate::persist`).
pub trait CompParam: serde::Serialize + DeserializeOwned + Clone + fmt::Debug + Send + Sync + 'static {}

impl<T> CompParam for T where T: serde::Serialize + DeserializeOwned + Clone + fmt::Debug + Send + Sync + 'static {}

/// Bound satisfied by every valid computation result type.
///
/// `DeserializeOwned` (in addition to `Serialize`) is required so a cached
/// value can be revived from its persisted postcard bytes when restoring a
/// node from a previous run (see `crate::persist`).
pub trait CompResult: serde::Serialize + DeserializeOwned + Clone + fmt::Debug + Send + Sync + 'static {}

impl<T> CompResult for T where T: serde::Serialize + DeserializeOwned + Clone + fmt::Debug + Send + Sync + 'static {}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    #[derive(Serialize)]
    struct Point {
        x: i32,
        y: i32,
    }

    #[test]
    fn same_value_same_hash() {
        let a = Point { x: 1, y: 2 };
        let b = Point { x: 1, y: 2 };
        assert_eq!(a.stable_hash(), b.stable_hash());
    }

    #[test]
    fn different_value_different_hash() {
        let a = Point { x: 1, y: 2 };
        let b = Point { x: 1, y: 3 };
        assert_ne!(a.stable_hash(), b.stable_hash());
    }

    #[test]
    fn string_and_str_produce_same_stable_hash() {
        let owned: String = String::from("hello world");
        let borrowed: &str = "hello world";
        assert_eq!(owned.stable_hash(), borrowed.stable_hash());
    }

    #[test]
    fn comp_key_equality_tracks_name_and_param() {
        let square = DefId::new("square");
        let cube = DefId::new("cube");

        let key_a = CompKey::new(square, &5i32);
        let key_a_again = CompKey::new(square, &5i32);
        let key_diff_param = CompKey::new(square, &6i32);
        let key_diff_name = CompKey::new(cube, &5i32);

        assert_eq!(key_a, key_a_again);
        assert_ne!(key_a, key_diff_param);
        assert_ne!(key_a, key_diff_name);
    }

    #[test]
    fn debug_formatting_smoke_test() {
        let def = DefId::new("square");
        let key = CompKey::new(def, &5i32);

        let key_debug = format!("{key:?}");
        assert!(key_debug.starts_with("square#"));
        assert_eq!(key_debug.len(), "square#".len() + 8);

        let hash = 5i32.stable_hash();
        let hash_debug = format!("{hash:?}");
        assert!(hash_debug.starts_with("Hash128("));
        assert!(hash_debug.ends_with(')'));

        let hash_display = format!("{hash}");
        assert_eq!(hash_display.len(), 32);

        let def_debug = format!("{def:?}");
        assert_eq!(def_debug, "DefId(square)");
    }
}
