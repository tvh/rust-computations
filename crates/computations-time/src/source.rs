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

use std::collections::{HashMap, HashSet};
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Bucket(Duration);

impl Bucket {
    pub const SECOND: Bucket = Bucket(Duration::from_secs(1));
    pub const MINUTE: Bucket = Bucket(Duration::from_secs(60));
    pub const FIVE_MINUTES: Bucket = Bucket(Duration::from_secs(5 * 60));
    pub const FIFTEEN_MINUTES: Bucket = Bucket(Duration::from_secs(15 * 60));
    pub const HOUR: Bucket = Bucket(Duration::from_secs(60 * 60));
    pub const DAY: Bucket = Bucket(Duration::from_secs(24 * 60 * 60));

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
#[derive(Default)]
struct WatchState {
    /// Watched buckets and the rounded value last reported for each.
    buckets: HashMap<Bucket, SystemTime>,
    /// Deadlines currently pending (not yet fired). A fired deadline is
    /// removed permanently: it will never change again, so keeping it
    /// around would cost forever for no further benefit.
    deadlines: HashSet<SystemTime>,
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
                        if watched.deadlines.remove(t) {
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
                watched.deadlines.insert(deadline)
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
fn next_wakeup(watched: &WatchState, now: SystemTime) -> Option<SystemTime> {
    let bucket_wakeups = watched.buckets.keys().map(|b| b.next_boundary_after(now));
    let deadline_wakeups = watched.deadlines.iter().copied();
    bucket_wakeups.chain(deadline_wakeups).min()
}

/// Recomputes bucket roundings and checks deadlines against the current
/// wall clock, emitting a change dep for everything that actually changed,
/// and permanently dropping any deadline that just fired.
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
        let due: Vec<SystemTime> = watched.deadlines.iter().copied().filter(|d| now >= *d).collect();
        for deadline in due {
            watched.deadlines.remove(&deadline);
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
            let watched = shared.watched.lock().unwrap();
            next_wakeup(&watched, now)
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
