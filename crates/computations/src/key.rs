//! Identifiers naming computations and data sources in the dependency graph.
//!
//! A computation application `c p` (in the terminology of the self-adjusting
//! computation literature) is identified by the name of its definition
//! together with a content hash of its parameter. This module provides that
//! identity: [`Hash256`] is the content hash primitive (the analogue of
//! `LargeHashable`), [`DefId`] names a definition, and [`CompKey`] names a
//! specific application of a definition to a parameter (the analogue of
//! `CompKey` / `c p`).

use std::fmt;
use std::sync::Arc;

/// A 256-bit content hash (a blake3 digest of a canonical byte encoding).
///
/// `Hash256` is a plain value type: cheap to copy, comparable, hashable, and
/// totally ordered so it can be used as a map/set key or sorted for stable
/// output.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Hash256([u8; 32]);

impl Hash256 {
    /// Builds a `Hash256` from raw bytes (typically a blake3 digest).
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Hash256(bytes)
    }

    /// Returns the raw bytes of this hash.
    pub const fn as_bytes(&self) -> [u8; 32] {
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
    /// without the full 64-character digest.
    pub(crate) fn short_hex(&self) -> String {
        use std::fmt::Write;
        let mut s = String::with_capacity(8);
        for byte in &self.0[..4] {
            let _ = write!(s, "{byte:02x}");
        }
        s
    }
}

impl fmt::Display for Hash256 {
    /// Prints the full hash as 64 lowercase hex characters.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.write_hex(f, self.0.len())
    }
}

impl fmt::Debug for Hash256 {
    /// Prints a short, human-scannable form: the first 8 hex characters.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Hash256(")?;
        self.write_hex(f, 4)?;
        write!(f, ")")
    }
}

/// A type that can be deterministically hashed to a stable [`Hash256`].
///
/// The hash is derived from a canonical (postcard) serialization of the
/// value, so it is stable across process runs and platforms as long as the
/// value's `Serialize` implementation is deterministic (e.g. it must not
/// depend on hash-map iteration order).
pub trait StableHash {
    /// Computes the stable content hash of `self`.
    fn stable_hash(&self) -> Hash256;
}

impl<T: serde::Serialize + ?Sized> StableHash for T {
    fn stable_hash(&self) -> Hash256 {
        let bytes = postcard::to_stdvec(self)
            .expect("postcard serialization of a well-formed value should not fail");
        Hash256::from_bytes(*blake3::hash(&bytes).as_bytes())
    }
}

/// The stable identity of a computation *definition*.
///
/// A `DefId` names a definition (e.g. `"fibonacci"`), independent of any
/// particular parameter it might be applied to; see [`CompKey`] for the
/// identity of a specific application. Cloning a `DefId` is cheap (an `Arc`
/// bump).
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct DefId(Arc<str>);

impl DefId {
    /// Creates a `DefId` for the definition named `name`.
    pub fn new(name: &str) -> Self {
        DefId(Arc::from(name))
    }

    /// Returns the definition's name.
    pub fn name(&self) -> &str {
        &self.0
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
    param_hash: Hash256,
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
    pub fn param_hash(&self) -> Hash256 {
        self.param_hash
    }
}

impl fmt::Debug for CompKey {
    /// Prints as `name#shorthash`, e.g. `fibonacci#a1b2c3d4`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}#", self.def.name())?;
        self.param_hash.write_hex(f, 4)
    }
}

/// Bound satisfied by every valid computation parameter type.
pub trait CompParam: serde::Serialize + Clone + fmt::Debug + Send + Sync + 'static {}

impl<T> CompParam for T where T: serde::Serialize + Clone + fmt::Debug + Send + Sync + 'static {}

/// Bound satisfied by every valid computation result type.
pub trait CompResult: serde::Serialize + Clone + fmt::Debug + Send + Sync + 'static {}

impl<T> CompResult for T where T: serde::Serialize + Clone + fmt::Debug + Send + Sync + 'static {}

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

        let key_a = CompKey::new(square.clone(), &5i32);
        let key_a_again = CompKey::new(square.clone(), &5i32);
        let key_diff_param = CompKey::new(square.clone(), &6i32);
        let key_diff_name = CompKey::new(cube, &5i32);

        assert_eq!(key_a, key_a_again);
        assert_ne!(key_a, key_diff_param);
        assert_ne!(key_a, key_diff_name);
    }

    #[test]
    fn debug_formatting_smoke_test() {
        let def = DefId::new("square");
        let key = CompKey::new(def.clone(), &5i32);

        let key_debug = format!("{key:?}");
        assert!(key_debug.starts_with("square#"));
        assert_eq!(key_debug.len(), "square#".len() + 8);

        let hash = 5i32.stable_hash();
        let hash_debug = format!("{hash:?}");
        assert!(hash_debug.starts_with("Hash256("));
        assert!(hash_debug.ends_with(')'));

        let hash_display = format!("{hash}");
        assert_eq!(hash_display.len(), 64);

        let def_debug = format!("{def:?}");
        assert_eq!(def_debug, "DefId(square)");
    }
}
