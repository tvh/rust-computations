//! Interns `(SourceId, key bytes)` pairs into a dense [`SrcKeyId`] — Stage
//! 13, see `docs/persistence-benchmark-notes.md`.
//!
//! ## Why
//!
//! `crate::engine::EngineInner::record_source_deps` used to store a full
//! `crate::source::RawDep` (a `SourceId` plus two owned `Vec<u8>`s) on
//! *every* node that reads a source key, and `crate::engine::EngineInner`'s
//! `source_index` keyed its reverse map on the same `(SourceId, KeyBytes)`
//! pair. On a shared-key workload (many nodes reading a small set of
//! distinct keys — 205,000 dependents across 300 keys on `persist_bench`),
//! that meant ~683x heap duplication of the same bytes, and every
//! `source_index` insert/lookup hashed a `SourceId` plus a full key byte
//! vector through `std`'s SipHash — the Haskell reference engine's Stage 4e
//! found the identical shape of duplication cost 54% of peak live memory
//! (`docs/benchmark-notes.md` in the sibling `haskell-computations` repo),
//! and this crate's own Stage 12 lock-hold attribution found
//! `record_source_deps`'s `source_index` critical section the single
//! hottest lock on `hospital_bench` (35.29% of held time).
//!
//! Versions churn, keys don't — a node re-reads the same key at a new
//! version far more often than it starts or stops reading a key entirely —
//! so only the `(SourceId, key)` identity is interned here.
//! [`crate::interner::SrcDep`] leaves the version as raw bytes alongside the
//! interned id.
//!
//! ## Reclamation
//!
//! An interner that only ever grows leaks for any workload whose key set
//! turns over (files deleted, patients discharged, keys reread under a new
//! identity). Every live [`SrcKeyId`] is refcounted: `crate::engine`'s
//! `reconcile_source_deps` retains a fresh reference the first time a node's
//! dependency set gains a key and releases one the moment it loses it
//! (comparing the *set* of distinct ids old vs. new, not raw `(key, ver)`
//! pairs — a node re-reading the same key at a new version must never
//! transiently look "unreferenced"), and `crate::driver`'s `liveness_gc`
//! releases every distinct id a collected node held. [`SrcKeyInterner`]
//! itself never scans for zero-refcount entries — release is always an
//! explicit, O(1) call on a path that already knows a dependency just
//! dropped, exactly the design the Haskell reference engine converged on in
//! its own Stage 4g (precise and prompt over a threshold sweep).
//!
//! **Ordering hazard, carried over from that same Stage 4g finding**: a
//! rewritten dependency span usually shares most of its ids with the old
//! one (the common "re-read the same keys, drop a few, gain a few" case).
//! Retaining every newly-referenced id *before* releasing every
//! no-longer-referenced one is therefore load-bearing: releasing first could
//! transiently zero a refcount for a key this exact call is about to
//! re-reference, freeing (and potentially recycling) its id out from under
//! a dependency that is, from the caller's point of view, still live for
//! the whole duration of this reconciliation.
//!
//! ## Persistence stays byte-based
//!
//! `SrcKeyId` is a process-local handle — it means nothing across a
//! restart. `crate::persist` never stores one: every persisted
//! `source_deps` entry still round-trips the real `(source, key, ver)`
//! bytes (see `crate::persist::RawDepRepr`, unchanged by this module), and
//! restoring a node re-interns its keys fresh, exactly as a live
//! `record_source_deps` call would.

use std::collections::HashMap;

use crate::source::{KeyBytes, SourceId, VerBytes};

/// A dense, process-local id standing in for one `(SourceId, key bytes)`
/// pair, assigned by [`SrcKeyInterner::intern`]. Never persisted, never
/// compared across restarts or across two different `Engine`s — see this
/// module's docs. Cheap to copy (a bare `u32`) and, per
/// `crate::hashers`'s docs, safe to hash with
/// [`crate::hashers::IdentityBuildHasher`]: it is an opaque handle this
/// process itself assigned, never adversary-chosen content.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct SrcKeyId(u32);

/// A node's dependency on one interned source key: identity via
/// [`SrcKeyId`], version left as raw bytes — see this module's docs on why
/// only the key half is interned.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SrcDep {
    pub(crate) key_id: SrcKeyId,
    pub(crate) ver: VerBytes,
}

/// Refcounted `(SourceId, key bytes) <-> SrcKeyId` interner — see this
/// module's docs for the reclamation design.
///
/// `forward` is nested (`SourceId -> (key bytes -> id)`) rather than a flat
/// `HashMap<(SourceId, KeyBytes), SrcKeyId>` so that
/// [`Self::intern`]'s common case — the key has been seen before, which is
/// every call once a workload's key set has stabilized — never allocates a
/// probe key: the inner map's `Vec<u8>` keys are looked up through a
/// borrowed `&[u8]` via `Vec<u8>: Borrow<[u8]>`, so a hit costs one hash of
/// the caller's own bytes and nothing else. Only a genuine miss (a key seen
/// for the first time) pays for an owned `key.to_vec()`.
#[derive(Default)]
pub(crate) struct SrcKeyInterner {
    forward: HashMap<SourceId, HashMap<KeyBytes, SrcKeyId>>,
    /// `SrcKeyId(i) -> Some((source, key))` while live, `None` once
    /// [`Self::release`] has freed slot `i` back to `free`. Needed to
    /// serialize real bytes back out (persistence, `Source::unregister`)
    /// without keeping every consumer holding its own copy, and to know
    /// which `forward` bucket to clean up on release.
    entries: Vec<Option<(SourceId, KeyBytes)>>,
    refcounts: Vec<u32>,
    /// Freed slot indices, available for [`Self::intern`] to reuse before
    /// growing `entries`.
    free: Vec<u32>,
}

impl SrcKeyInterner {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Looks up or creates the id for `(source, key)`, *without* touching
    /// its refcount — interning alone never implies a live reference (a
    /// call that only wants to resolve an id, e.g.
    /// `crate::driver::EngineInner::affected_keys`, must not accidentally
    /// keep a dead key alive; a call that does want a reference must follow
    /// up with [`Self::retain`], as `crate::engine`'s
    /// `reconcile_source_deps` does).
    pub(crate) fn intern(&mut self, source: &SourceId, key: &[u8]) -> SrcKeyId {
        if let Some(&id) = self.forward.get(source).and_then(|m| m.get(key)) {
            return id;
        }
        let id = match self.free.pop() {
            Some(slot) => {
                self.entries[slot as usize] = Some((source.clone(), key.to_vec()));
                self.refcounts[slot as usize] = 0;
                SrcKeyId(slot)
            }
            None => {
                self.entries.push(Some((source.clone(), key.to_vec())));
                self.refcounts.push(0);
                SrcKeyId((self.entries.len() - 1) as u32)
            }
        };
        self.forward.entry(source.clone()).or_default().insert(key.to_vec(), id);
        id
    }

    /// [`Self::intern`] plus an immediate [`Self::retain`], for a caller
    /// establishing a brand-new reference with no prior state to diff
    /// against (`crate::persist`'s restore path: a freshly restored node's
    /// dependency set has no "old" to compare with — every id in it is a
    /// fresh reference).
    pub(crate) fn intern_retain(&mut self, source: &SourceId, key: &[u8]) -> SrcKeyId {
        let id = self.intern(source, key);
        self.retain(id);
        id
    }

    /// Adds one live reference to `id`. Must only be called with an id this
    /// interner actually produced.
    pub(crate) fn retain(&mut self, id: SrcKeyId) {
        self.refcounts[id.0 as usize] += 1;
    }

    /// Drops one live reference to `id`, reclaiming its slot (removing it
    /// from `forward` and pushing the slot onto `free`) the moment the
    /// count reaches zero. A release with no matching prior retain is a
    /// reclamation-path bug — loud (`debug_assert!`) in debug builds,
    /// saturating rather than wrapping/panicking in release (this crate
    /// never panics under a lock it doesn't own the poisoning of; see the
    /// "Harden API against silent misuse" commit).
    pub(crate) fn release(&mut self, id: SrcKeyId) {
        let i = id.0 as usize;
        debug_assert!(
            self.refcounts[i] > 0,
            "SrcKeyInterner::release: refcount underflow for {id:?} -- released without a matching \
             prior retain (every dependency removal must release exactly once per retain)"
        );
        self.refcounts[i] = self.refcounts[i].saturating_sub(1);
        if self.refcounts[i] == 0 {
            if let Some((source, key)) = self.entries[i].take()
                && let Some(m) = self.forward.get_mut(&source)
            {
                m.remove(&key);
                if m.is_empty() {
                    self.forward.remove(&source);
                }
            }
            self.free.push(id.0);
        }
    }

    /// The real `(source, key bytes)` an id stands for, `None` if it has
    /// already been released. Never allocates.
    pub(crate) fn resolve(&self, id: SrcKeyId) -> Option<(&SourceId, &[u8])> {
        self.entries[id.0 as usize].as_ref().map(|(source, key)| (source, key.as_slice()))
    }

    /// Looks up `(source, key)`'s id without creating one — for a caller
    /// that only wants to know whether *anything* has ever referenced this
    /// key (`crate::driver::EngineInner::affected_keys`: a key nothing
    /// depends on can't affect any node, whether or not it happens to be
    /// interned).
    pub(crate) fn lookup(&self, source: &SourceId, key: &[u8]) -> Option<SrcKeyId> {
        self.forward.get(source).and_then(|m| m.get(key)).copied()
    }

    /// The number of currently-live (non-released) ids. Test-only: proves
    /// the interner actually shrinks rather than only ever growing.
    #[cfg(test)]
    pub(crate) fn live_len(&self) -> usize {
        self.entries.len() - self.free.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn src(name: &str) -> SourceId {
        SourceId::new(name)
    }

    #[test]
    fn interning_the_same_key_twice_returns_the_same_id() {
        let mut interner = SrcKeyInterner::new();
        let a = interner.intern(&src("fs"), b"file1");
        let b = interner.intern(&src("fs"), b"file1");
        assert_eq!(a, b);
        assert_eq!(interner.live_len(), 1);
    }

    #[test]
    fn different_keys_get_different_ids() {
        let mut interner = SrcKeyInterner::new();
        let a = interner.intern(&src("fs"), b"file1");
        let b = interner.intern(&src("fs"), b"file2");
        assert_ne!(a, b);
    }

    #[test]
    fn the_same_key_bytes_under_different_sources_are_distinct() {
        let mut interner = SrcKeyInterner::new();
        let a = interner.intern(&src("fs"), b"key");
        let b = interner.intern(&src("http"), b"key");
        assert_ne!(a, b);
        assert_eq!(interner.live_len(), 2);
    }

    #[test]
    fn resolve_round_trips_the_original_bytes() {
        let mut interner = SrcKeyInterner::new();
        let id = interner.intern(&src("fs"), b"file1");
        let (source, key) = interner.resolve(id).unwrap();
        assert_eq!(source, &src("fs"));
        assert_eq!(key, b"file1");
    }

    #[test]
    fn lookup_finds_an_interned_key_without_creating_one() {
        let mut interner = SrcKeyInterner::new();
        let id = interner.intern(&src("fs"), b"file1");
        assert_eq!(interner.lookup(&src("fs"), b"file1"), Some(id));
        assert_eq!(interner.lookup(&src("fs"), b"never-seen"), None);
    }

    /// The reclamation test the task calls out explicitly: releasing a
    /// key's last live reference must actually shrink the interner, not
    /// just decrement a counter nobody reads.
    #[test]
    fn releasing_the_last_reference_shrinks_the_interner() {
        let mut interner = SrcKeyInterner::new();
        let id = interner.intern_retain(&src("fs"), b"file1");
        assert_eq!(interner.live_len(), 1);
        interner.release(id);
        assert_eq!(interner.live_len(), 0, "the interner must actually drop the entry, not just zero its refcount");
        assert_eq!(
            interner.lookup(&src("fs"), b"file1"),
            None,
            "a released key must not still resolve via lookup"
        );
    }

    #[test]
    fn a_key_survives_while_any_dependent_still_holds_it() {
        let mut interner = SrcKeyInterner::new();
        let id = interner.intern_retain(&src("fs"), b"shared");
        interner.retain(id); // a second dependent
        interner.release(id); // first dependent drops it
        assert_eq!(interner.live_len(), 1, "still referenced by the second dependent");
        assert_eq!(interner.lookup(&src("fs"), b"shared"), Some(id));
        interner.release(id); // second dependent drops it too
        assert_eq!(interner.live_len(), 0);
    }

    #[test]
    fn a_released_slot_is_reused_by_a_fresh_intern() {
        let mut interner = SrcKeyInterner::new();
        let id = interner.intern_retain(&src("fs"), b"file1");
        interner.release(id);
        assert_eq!(interner.live_len(), 0);
        let fresh = interner.intern(&src("fs"), b"file2");
        // Not asserting the numeric id is recycled (an implementation
        // detail) -- only that the freed slot doesn't leave the interner's
        // storage growing unboundedly for a workload that churns keys.
        assert_eq!(interner.live_len(), 1);
        assert_eq!(interner.resolve(fresh).unwrap().1, b"file2");
    }

    #[test]
    fn re_interning_after_release_gets_a_fresh_reference_not_a_dangling_one() {
        let mut interner = SrcKeyInterner::new();
        let id = interner.intern_retain(&src("fs"), b"file1");
        interner.release(id);
        // The exact re-read pattern `reconcile_source_deps` guards against:
        // the same real key is referenced again shortly after its last
        // reference was dropped.
        let again = interner.intern_retain(&src("fs"), b"file1");
        assert_eq!(interner.live_len(), 1);
        assert_eq!(interner.resolve(again).unwrap().1, b"file1");
    }

    #[test]
    #[should_panic(expected = "refcount underflow")]
    fn release_without_a_prior_retain_is_a_loud_bug_in_debug_builds() {
        let mut interner = SrcKeyInterner::new();
        let id = interner.intern(&src("fs"), b"file1"); // intern alone does not retain
        interner.release(id);
    }

    #[test]
    fn many_keys_interleaved_retain_release_leaves_only_the_still_live_ones() {
        let mut interner = SrcKeyInterner::new();
        let ids: Vec<SrcKeyId> =
            (0..50).map(|i| interner.intern_retain(&src("fs"), format!("key{i}").as_bytes())).collect();
        assert_eq!(interner.live_len(), 50);
        for &id in ids.iter().step_by(2) {
            interner.release(id);
        }
        assert_eq!(interner.live_len(), 25);
        for &id in ids.iter().skip(1).step_by(2) {
            interner.release(id);
        }
        assert_eq!(interner.live_len(), 0);
    }
}
