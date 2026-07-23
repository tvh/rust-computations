//! A wall-clock time source, driven by one background timer task.
//!
//! [`TimeSource`] serves two [`Request`] kinds — [`RoundedTime`] (the
//! current time rounded down to a [`Bucket`] granularity) and [`IsAfter`]
//! (has wall-clock time passed a given instant) — pushing a change
//! notification for every watched bucket boundary crossed and every
//! deadline passed, until [`SourceBase::unregister`] is called for that key
//! (or, for a deadline, until it has fired once — see below).
//!
//! ## Scheduling strategy
//!
//! There is no polling loop. A single background task (spawned in
//! [`TimeSource::new`]) owns the schedule of pending wakeups and sleeps via
//! [`tokio::time::sleep_until`] until the *earliest* one, computed fresh
//! from the wall clock every time it wakes (never by accumulating ticks) so
//! that clock drift, a suspended machine, or a jumped system clock can never
//! desynchronize it from reality. A [`tokio::sync::Notify`] interrupts the
//! sleep whenever [`Source::execute`]/[`SourceBase::unregister`] change the
//! schedule (a newly watched bucket or deadline, or a key dropping out) —
//! including the case where nothing is left to wait for, in which case the
//! task sleeps forever until notified.
//!
//! A watched [`Bucket`]'s next wakeup is the next boundary after now
//! (`Bucket::next_boundary_after`); a watched [`IsAfter`] deadline's only
//! wakeup is the deadline itself, and once it fires (the reported version
//! becomes `true`, which it will remain forever) the deadline is dropped
//! from the schedule permanently — a passed deadline costs nothing from
//! then on. A deadline already in the past when [`IsAfter`] is first
//! executed never enters the schedule at all.
//!
//! Only ever one timer is scheduled at a time (the single raced
//! `sleep_until` in [`run_timer`]) — everything else lives in queues.
//! Watched buckets stay in a small `HashMap` (there are only ever as many
//! distinct buckets as distinct granularities actually requested, and each
//! recurs forever, so a full scan of it is cheap and unavoidable). Watched
//! deadlines, which can be numerous and are each one-shot, are kept in a
//! real priority queue — see [`WatchState`]'s doc comment for the min-heap
//! and lazy-deletion scheme.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use computations::error::{CompError, SourceError};
use computations::{Ctx, Dep, Request, Source, SourceBase, SourceId};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex as AsyncMutex, Notify, mpsc};

/// The shortest granularity [`Bucket::new`] accepts. Anything finer is
/// almost certainly a busy-poll in disguise.
const MIN_BUCKET: Duration = Duration::from_millis(100);

/// Rounding granularity for [`RoundedTime`].
///
/// A newtype over [`Duration`] with named convenience constants for the
/// common granularities. Arbitrary durations are allowed via [`Bucket::new`]
/// (minimum 100ms).
///
/// ## Alignment is to the Unix epoch (UTC), not to any calendar
///
/// A bucket boundary is computed by pure epoch arithmetic — `floor(nanos
/// since `UNIX_EPOCH` / bucket duration) * bucket duration` (see
/// [`round_down`](Bucket::round_down)) — so every boundary is a fixed
/// distance from `UNIX_EPOCH`, which is itself a UTC instant. That makes
/// [`Bucket::HOUR`] boundaries line up with UTC hour boundaries, which is
/// *not* the same as local hour boundaries in any timezone with a
/// fractional UTC offset (e.g. `+05:30`, `+05:45`) — the mismatch just
/// happens to be invisible for whole-hour offsets.
///
/// There is deliberately no `Bucket::DAY` (nor any other constant a day or
/// longer): a fixed 24h duration since the epoch is "midnight UTC", not
/// midnight in any local timezone, and local calendar days are not even
/// uniformly 24h once daylight saving time is involved. `Bucket` cannot
/// deliver calendar-aware rounding — local midnight, DST-aware days,
/// months — because the arithmetic has no notion of a calendar at all, only
/// of elapsed duration since the epoch.
///
/// A caller who wants "daily at local midnight" (or any other
/// calendar-relative instant) should compute that instant themselves
/// (timezone- and DST-aware) and watch it with [`IsAfter`], rescheduling
/// the next one after each firing. A caller who just wants *some* recurring
/// ~24h-ish boundary and doesn't care that it's UTC-aligned can still use
/// `Bucket::new(Duration::from_secs(24 * 60 * 60))` — just knowing it is
/// epoch-aligned, not calendar-aligned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Bucket(Duration);

impl Bucket {
    pub const SECOND: Bucket = Bucket(Duration::from_secs(1));
    pub const MINUTE: Bucket = Bucket(Duration::from_secs(60));
    pub const FIVE_MINUTES: Bucket = Bucket(Duration::from_secs(5 * 60));
    pub const FIFTEEN_MINUTES: Bucket = Bucket(Duration::from_secs(15 * 60));
    pub const HOUR: Bucket = Bucket(Duration::from_secs(60 * 60));

    /// Builds a `Bucket` for an arbitrary granularity.
    ///
    /// # Errors
    /// Returns [`InvalidBucket`] if `duration` is shorter than 100ms.
    pub fn new(duration: Duration) -> Result<Bucket, InvalidBucket> {
        if duration < MIN_BUCKET {
            return Err(InvalidBucket(duration));
        }
        Ok(Bucket(duration))
    }

    /// The granularity this bucket rounds to.
    pub fn duration(&self) -> Duration {
        self.0
    }

    /// Rounds `t` down to this bucket's most recent boundary at or before
    /// `t`.
    ///
    /// Boundaries are fixed durations aligned to `UNIX_EPOCH` (UTC) —
    /// `floor(nanos since epoch / bucket duration) * bucket duration` —
    /// never to a local calendar. See the [type docs](Bucket) for what that
    /// does and doesn't mean for timezones and daylight saving time.
    pub fn round_down(&self, t: SystemTime) -> SystemTime {
        let nanos = nanos_since_epoch(t);
        let bucket_nanos = self.0.as_nanos();
        let floor = (nanos / bucket_nanos) * bucket_nanos;
        UNIX_EPOCH + nanos_to_duration(floor)
    }

    /// The next bucket boundary strictly after `t` — used by the timer task
    /// to schedule this bucket's next wakeup.
    fn next_boundary_after(&self, t: SystemTime) -> SystemTime {
        let nanos = nanos_since_epoch(t);
        let bucket_nanos = self.0.as_nanos();
        let next = (nanos / bucket_nanos + 1) * bucket_nanos;
        UNIX_EPOCH + nanos_to_duration(next)
    }
}

fn nanos_since_epoch(t: SystemTime) -> u128 {
    t.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO).as_nanos()
}

/// Converts a nanosecond count back to a `Duration`, saturating rather than
/// panicking in the (practically unreachable, at any date this code will
/// ever run against) case it doesn't fit in a `u64`.
fn nanos_to_duration(nanos: u128) -> Duration {
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

/// [`Bucket::new`] was given a duration shorter than the 100ms minimum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("bucket granularity {0:?} is below the 100ms minimum")]
pub struct InvalidBucket(pub Duration);

/// Current wall-clock time, rounded down to `Bucket`'s boundary.
/// `Output = SystemTime`.
///
/// Corresponds to the paper's built-in `compGetTime` source, applied to a
/// granularity (1min, 5min, ...).
///
/// Rounding is epoch-aligned (UTC), not calendar-aligned — see the
/// [`Bucket`] type docs. For a calendar-relative instant (local midnight, a
/// DST-aware day, a month boundary), compute that instant yourself and
/// watch it with [`IsAfter`] instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RoundedTime(pub Bucket);

impl Request for RoundedTime {
    type Output = SystemTime;
}

/// Has wall-clock time passed `t`? `Output = bool`.
///
/// Flips from `false` to `true` exactly once, at `t`, and never changes
/// again afterwards.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IsAfter(pub SystemTime);

impl Request for IsAfter {
    type Output = bool;
}

/// What [`TimeSource`] is watching: either a rounding [`Bucket`] or an
/// [`IsAfter`] deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TimeKey {
    Bucket(Bucket),
    Deadline(SystemTime),
}

/// The version [`TimeSource`] reports for a [`TimeKey`]: the currently
/// observed rounded time, or whether the deadline has passed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TimeVer {
    Rounded(SystemTime),
    After(bool),
}

/// Mutable scheduling state, shared between [`TimeSource`] and its
/// background timer task.
///
/// Deadlines are kept in a real priority queue rather than a flat set, so
/// finding the earliest one is O(1) instead of an O(n) scan:
///
/// - `deadline_set` is the source of truth for *membership* — is this
///   deadline currently pending? — and is what `unregister` and a firing
///   both mutate directly.
/// - `deadline_heap` is a min-heap (via `Reverse`, since `BinaryHeap` is a
///   max-heap by default) ordered by time, so its peek is always the
///   earliest deadline *that was ever inserted* — but `BinaryHeap` has no
///   efficient way to remove an arbitrary element, so `unregister` only
///   ever touches `deadline_set`, leaving a now-stale entry sitting in the
///   heap. This is lazy deletion: a heap entry is only actually discarded
///   when it reaches the top (in `peek_earliest_deadline` /
///   `pop_due_deadlines`) and is found to no longer be in `deadline_set` —
///   at which point it is popped and skipped rather than treated as live.
///   The invariant this maintains is that `deadline_set` and the *live*
///   (non-stale) entries of `deadline_heap` always describe exactly the
///   same set of pending deadlines.
#[derive(Default)]
struct WatchState {
    /// Watched buckets and the rounded value last reported for each. Kept
    /// as a plain map: there are only ever as many distinct buckets as
    /// distinct granularities actually requested (typically a handful),
    /// each recurs forever, and a full scan of it on every wakeup is cheap
    /// and unavoidable (unlike deadlines, there is no "pop the earliest and
    /// discard" operation that would make sense for a recurring key).
    buckets: HashMap<Bucket, SystemTime>,
    /// Pending (not yet fired) `IsAfter` deadlines, as a min-heap plus a
    /// membership set. See this struct's doc comment for the lazy-deletion
    /// invariant between the two fields.
    deadline_heap: BinaryHeap<Reverse<SystemTime>>,
    deadline_set: HashSet<SystemTime>,
}

impl WatchState {
    /// Registers `t` as a pending deadline if it isn't already. Returns
    /// whether it was newly inserted (i.e. whether the schedule actually
    /// changed).
    fn insert_deadline(&mut self, t: SystemTime) -> bool {
        if self.deadline_set.insert(t) {
            self.deadline_heap.push(Reverse(t));
            true
        } else {
            false
        }
    }

    /// Unregisters `t` as a pending deadline (lazy deletion: only removes
    /// it from `deadline_set` — see this struct's doc comment). Returns
    /// whether it was actually pending.
    fn remove_deadline(&mut self, t: &SystemTime) -> bool {
        self.deadline_set.remove(t)
    }

    /// The earliest still-pending deadline, if any, discarding any
    /// lazily-deleted (unregistered) stale entries found at the top of the
    /// heap along the way.
    fn peek_earliest_deadline(&mut self) -> Option<SystemTime> {
        while let Some(&Reverse(t)) = self.deadline_heap.peek() {
            if self.deadline_set.contains(&t) {
                return Some(t);
            }
            self.deadline_heap.pop();
        }
        None
    }

    /// Pops and returns every deadline at or before `now`, discarding (and
    /// skipping) any lazily-deleted stale entries encountered along the
    /// way. Every deadline returned is removed from `deadline_set` too — it
    /// has fired and, per `IsAfter`'s one-shot semantics, will never be
    /// scheduled again.
    fn pop_due_deadlines(&mut self, now: SystemTime) -> Vec<SystemTime> {
        let mut due = Vec::new();
        while let Some(&Reverse(t)) = self.deadline_heap.peek() {
            if t > now {
                break;
            }
            self.deadline_heap.pop();
            if self.deadline_set.remove(&t) {
                due.push(t);
            }
            // else: a stale (already-unregistered) entry — discard it and
            // keep going rather than reporting a firing for it.
        }
        due
    }
}

/// State shared between [`TimeSource`] and its background timer task.
struct Shared {
    watched: Mutex<WatchState>,
    /// Interrupts the timer task's sleep when `execute`/`unregister` change
    /// the schedule (a newly watched key with an earlier wakeup, or nothing
    /// left to wait for).
    reschedule: Notify,
}

/// A source reporting wall-clock time: current time rounded to a [`Bucket`]
/// ([`RoundedTime`]), and whether a given instant has passed ([`IsAfter`]).
///
/// See the [module docs](self) for the no-polling scheduling strategy.
pub struct TimeSource {
    id: SourceId,
    shared: Arc<Shared>,
    changes_rx: AsyncMutex<mpsc::UnboundedReceiver<Dep<TimeKey, TimeVer>>>,
}

impl TimeSource {
    /// Creates a new `TimeSource` with the given instance id, spawning its
    /// background timer task.
    ///
    /// # Panics
    /// Spawns a task, so this must be called from within a running Tokio
    /// runtime (per `tokio::spawn`'s usual rules).
    pub fn new(id: &str) -> Arc<Self> {
        let shared = Arc::new(Shared {
            watched: Mutex::new(WatchState::default()),
            reschedule: Notify::new(),
        });
        // Only the timer task ever sends on this channel; the sending half
        // is moved entirely into it below, so the channel stays open for
        // `TimeSource`'s whole lifetime without `TimeSource` needing to
        // hold a sender itself (mirrors `computations_fs::FsSource`).
        let (tx, rx) = mpsc::unbounded_channel();

        let task_shared = shared.clone();
        tokio::spawn(async move { run_timer(task_shared, tx).await });

        Arc::new(TimeSource {
            id: SourceId::new(id),
            shared,
            changes_rx: AsyncMutex::new(rx),
        })
    }

    /// Current wall-clock time rounded down to `bucket`'s boundary,
    /// recording the read (and its eventual changes) as a dependency of the
    /// currently executing computation. A typed convenience over
    /// `ctx.src_req(source, RoundedTime(bucket))`.
    pub async fn rounded(self: &Arc<Self>, ctx: &Ctx, bucket: Bucket) -> Result<SystemTime, CompError> {
        ctx.src_req(self, RoundedTime(bucket)).await
    }

    /// Whether wall-clock time has passed `t`, recording the read (and its
    /// eventual one-shot change, if any) as a dependency of the currently
    /// executing computation. A typed convenience over `ctx.src_req(source,
    /// IsAfter(t))`.
    pub async fn is_after(self: &Arc<Self>, ctx: &Ctx, t: SystemTime) -> Result<bool, CompError> {
        ctx.src_req(self, IsAfter(t)).await
    }
}

impl SourceBase for TimeSource {
    type Key = TimeKey;
    type Ver = TimeVer;

    fn instance_id(&self) -> SourceId {
        self.id.clone()
    }

    async fn wait_changes(&self) -> HashSet<Dep<TimeKey, TimeVer>> {
        let mut rx = self.changes_rx.lock().await;
        let mut batch = HashSet::new();
        // Await the first item (cancel-safe: tokio's mpsc recv is
        // cancel-safe), then opportunistically drain any additional queued
        // items into the same batch without blocking further.
        match rx.recv().await {
            Some(dep) => {
                batch.insert(dep);
            }
            None => return batch,
        }
        while let Ok(dep) = rx.try_recv() {
            batch.insert(dep);
        }
        batch
    }

    fn unregister(&self, keys: &HashSet<TimeKey>) {
        let mut changed = false;
        {
            let mut watched = self.shared.watched.lock().unwrap();
            for key in keys {
                match key {
                    TimeKey::Bucket(b) => {
                        if watched.buckets.remove(b).is_some() {
                            changed = true;
                        }
                    }
                    TimeKey::Deadline(t) => {
                        if watched.remove_deadline(t) {
                            changed = true;
                        }
                    }
                }
            }
        }
        if changed {
            tracing::debug!("time source: keys unregistered, rearming timer");
            self.shared.reschedule.notify_one();
        }
    }
}

impl Source<RoundedTime> for TimeSource {
    async fn execute(
        &self,
        req: RoundedTime,
    ) -> (Result<SystemTime, SourceError>, HashSet<Dep<TimeKey, TimeVer>>) {
        let RoundedTime(bucket) = req;
        let now = SystemTime::now();
        let rounded = bucket.round_down(now);

        let is_new = {
            let mut watched = self.shared.watched.lock().unwrap();
            let is_new = !watched.buckets.contains_key(&bucket);
            watched.buckets.insert(bucket, rounded);
            is_new
        };
        if is_new {
            tracing::debug!(?bucket, "time source: bucket registered");
            self.shared.reschedule.notify_one();
        }

        let mut deps = HashSet::new();
        deps.insert(Dep {
            key: TimeKey::Bucket(bucket),
            ver: TimeVer::Rounded(rounded),
        });
        (Ok(rounded), deps)
    }
}

impl Source<IsAfter> for TimeSource {
    async fn execute(&self, req: IsAfter) -> (Result<bool, SourceError>, HashSet<Dep<TimeKey, TimeVer>>) {
        let IsAfter(deadline) = req;
        let now = SystemTime::now();
        let after = now >= deadline;

        // An already-past deadline never enters the schedule: there is
        // nothing left to wait for, it is simply reported as fired.
        if !after {
            let is_new = {
                let mut watched = self.shared.watched.lock().unwrap();
                watched.insert_deadline(deadline)
            };
            if is_new {
                tracing::debug!(?deadline, "time source: deadline registered");
                self.shared.reschedule.notify_one();
            }
        }

        let mut deps = HashSet::new();
        deps.insert(Dep {
            key: TimeKey::Deadline(deadline),
            ver: TimeVer::After(after),
        });
        (Ok(after), deps)
    }
}

/// Computes the earliest pending wakeup across every watched bucket and
/// deadline (`None` if nothing is watched), recomputed fresh from the wall
/// clock every call so that drift, suspend, or a jumped clock can never
/// desync it from reality (see the [module docs](self)).
///
/// Buckets stay a full (but small) scan; the deadline side is a single
/// heap-peek (after lazily discarding any stale top entries) rather than a
/// scan of every pending deadline.
fn next_wakeup(watched: &mut WatchState, now: SystemTime) -> Option<SystemTime> {
    let earliest_bucket = watched.buckets.keys().map(|b| b.next_boundary_after(now)).min();
    let earliest_deadline = watched.peek_earliest_deadline();
    match (earliest_bucket, earliest_deadline) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

/// Recomputes bucket roundings and pops every deadline at or before now,
/// emitting a change dep for everything that actually changed, and
/// permanently dropping any deadline that just fired (per `pop_due_deadlines`,
/// they're already removed from the schedule).
fn process_wakeup(shared: &Shared, tx: &mpsc::UnboundedSender<Dep<TimeKey, TimeVer>>) {
    let now = SystemTime::now();
    let mut fired = Vec::new();
    {
        let mut watched = shared.watched.lock().unwrap();
        for (bucket, last) in watched.buckets.iter_mut() {
            let rounded = bucket.round_down(now);
            if rounded != *last {
                *last = rounded;
                fired.push(Dep {
                    key: TimeKey::Bucket(*bucket),
                    ver: TimeVer::Rounded(rounded),
                });
            }
        }
        for deadline in watched.pop_due_deadlines(now) {
            fired.push(Dep {
                key: TimeKey::Deadline(deadline),
                ver: TimeVer::After(true),
            });
        }
    }
    for dep in fired {
        tracing::debug!(key = ?dep.key, "time source: change notification");
        let _ = tx.send(dep);
    }
}

/// The single background task driving all of `TimeSource`'s change
/// notifications. Sleeps until the earliest pending wakeup (recomputed from
/// the wall clock on every iteration — never accumulated ticks), fires
/// whatever actually changed, and loops; interrupted early by
/// `shared.reschedule` whenever `execute`/`unregister` change what's being
/// watched (a new, earlier wakeup, or nothing left to wait for).
async fn run_timer(shared: Arc<Shared>, tx: mpsc::UnboundedSender<Dep<TimeKey, TimeVer>>) {
    loop {
        let now = SystemTime::now();
        let target = {
            let mut watched = shared.watched.lock().unwrap();
            next_wakeup(&mut watched, now)
        };

        match target {
            Some(target) => {
                // SystemTime can jump around (unlike tokio's Instant-based
                // sleeps), so convert the wall-clock target to a sleep
                // duration on every iteration, clamping a target already in
                // the past to zero (fires immediately) rather than erroring.
                let sleep_for = target.duration_since(now).unwrap_or(Duration::ZERO);
                let deadline = tokio::time::Instant::now() + sleep_for;
                tokio::select! {
                    () = tokio::time::sleep_until(deadline) => {
                        tracing::debug!("time source: timer fired");
                        process_wakeup(&shared, &tx);
                    }
                    () = shared.reschedule.notified() => {
                        tracing::debug!("time source: schedule changed, rearming");
                    }
                }
            }
            None => {
                tracing::debug!("time source: nothing scheduled, sleeping until notified");
                shared.reschedule.notified().await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_new_rejects_sub_100ms_granularity() {
        assert!(Bucket::new(Duration::from_millis(99)).is_err());
        assert!(Bucket::new(Duration::from_millis(100)).is_ok());
    }

    #[test]
    fn round_down_is_a_bucket_multiple_within_range() {
        let now = SystemTime::now();
        let bucket = Bucket::new(Duration::from_millis(300)).unwrap();
        let rounded = bucket.round_down(now);

        assert!(rounded <= now);
        assert!(now < rounded + bucket.duration());
        let nanos = rounded.duration_since(UNIX_EPOCH).unwrap().as_nanos();
        assert_eq!(nanos % bucket.duration().as_nanos(), 0);
    }

    #[test]
    fn different_buckets_give_different_keys() {
        let a = Bucket::new(Duration::from_millis(300)).unwrap();
        let b = Bucket::new(Duration::from_millis(500)).unwrap();
        assert_ne!(TimeKey::Bucket(a), TimeKey::Bucket(b));
    }

    #[tokio::test]
    async fn rounded_time_execute_reports_dep_and_registers_bucket() {
        let src = TimeSource::new("time");
        let bucket = Bucket::new(Duration::from_millis(300)).unwrap();

        let (result, deps) = src.execute(RoundedTime(bucket)).await;
        let rounded = result.unwrap();
        assert_eq!(deps.len(), 1);
        let dep = deps.into_iter().next().unwrap();
        assert_eq!(dep.key, TimeKey::Bucket(bucket));
        assert_eq!(dep.ver, TimeVer::Rounded(rounded));
    }

    #[tokio::test]
    async fn is_after_execute_reports_dep() {
        let src = TimeSource::new("time");
        let future = SystemTime::now() + Duration::from_secs(3600);

        let (result, deps) = src.execute(IsAfter(future)).await;
        assert!(!result.unwrap());
        let dep = deps.into_iter().next().unwrap();
        assert_eq!(dep.key, TimeKey::Deadline(future));
        assert_eq!(dep.ver, TimeVer::After(false));

        let past = SystemTime::now() - Duration::from_secs(3600);
        let (result, deps) = src.execute(IsAfter(past)).await;
        assert!(result.unwrap());
        let dep = deps.into_iter().next().unwrap();
        assert_eq!(dep.ver, TimeVer::After(true));
    }
}
