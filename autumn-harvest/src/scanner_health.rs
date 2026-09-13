//! Background control-loop liveness heartbeats (issue #797).
//!
//! Harvest's correctness in production depends on a fleet of background
//! control loops that run as bare spawned Tokio tasks inside the
//! worker/plugin process: timeout enforcement, the soft-SLA scanner, the
//! poison-pill orphan reclaimer, the external signal/cancel outboxes, the
//! retention janitor, the schedule ticker, and the bounded-pause
//! auto-resumer. If one of them panics, deadlocks on a poisoned connection,
//! or stalls on a never-returning query, it fails **silently** — and the
//! operator only finds out when a downstream correctness symptom (a workflow
//! that should have timed out but didn't, an SLA that never fired) surfaces
//! minutes to hours later.
//!
//! The pre-existing loop metrics cannot catch this: `record_retention_tick`,
//! `record_schedule_fire_attempt`, and the timeout/SLA/quarantine counters
//! only emit **when there is work to do**, so a healthy idle loop and a
//! wedged loop both emit zero — indistinguishable.
//!
//! This module closes that gap with two halves that share **one choke point**
//! ([`record_scanner_tick`]):
//!
//! 1. an unconditional `harvest.scanner.tick` counter on every loop
//!    iteration, under the bounded [`Scanner`] label — so a flat-lined
//!    counter *is* the wedge signal; and
//! 2. an in-process last-tick registry ([`ScannerLiveness`]) whose staleness
//!    verdict is surfaced by the `scanner_liveness` check in
//!    `GET /admin/preflight`.
//!
//! Because both are driven from the same call, a call site can never emit one
//! without the other. The **timestamp is recorded unconditionally**, so the
//! health check keeps working on a deployment that has configured no metrics
//! recorder at all.
//!
//! # Scope
//!
//! Detection only. Auto-restart of a wedged loop, cross-replica scanner
//! ownership, and any persisted (cross-process) heartbeat row are explicitly
//! out of scope for issue #797 — this module introduces **no new
//! `WorkflowEvent` variant, no `harvest_events` footprint, no migration, and
//! no replay-determinism impact**.
//!
//! Two consequences of "in-process" worth stating plainly, because both can
//! read as false assurance:
//!
//! * The registry is **per process**. Pointed at an API-only replica — or any
//!   process that is not the wedged one — the `scanner_liveness` check reports
//!   zero registered scanners and therefore `pass`. In a split API/worker
//!   topology, run the check against the worker process, and use the
//!   `harvest.scanner.tick` counter (which carries the scrape target's own
//!   `instance` label) to find *which* replica went quiet.
//! * A [`Scanner`] label may cover **more than one loop instance** in one
//!   process: a multi-shard worker spawns a timeout checker, poison-pill
//!   reclaimer, and pause auto-resumer per assigned shard. Each instance is
//!   tracked separately and the health check reports the **worst** of them, so
//!   a healthy shard cannot mask a wedged sibling. The *metric* carries a
//!   bounded `shard` label for the same reason: without it every per-shard
//!   instance would increment one series on one scrape target, so a healthy
//!   shard's ticks would hold `rate(...) > 0` while a sibling's loop was dead
//!   and the paging alert would never fire — masking, not merely a lost
//!   localization. The health check additionally names the shard in its
//!   payload and lists **every** stale shard in `stale_shards`, so an
//!   operator sees the full blast radius rather than one database.
//!
//! # Residual: two runtimes on one shard in one process
//!
//! `(instance, scanner, shard)` separates every instance the deployment shapes
//! Harvest supports actually produce — one worker process per shard, or one
//! multi-shard worker — because a duplicate `shard_assignments` entry is
//! deduplicated at the config boundary (`builder::dedup_shard_assignments`,
//! applied by both `WorkerConfig::with_shard_assignments` and the
//! `WorkerConfig -> WorkerRuntimeConfig` conversion).
//!
//! What it does **not** separate is two *independently constructed* runtimes
//! (two `Worker`s, or two `RetentionRuntime`s) living in one process and
//! polling the **same** shard. Their ticks land on one series, so a healthy
//! one holds `rate(...) > 0` after its peer wedges and the alert stays quiet.
//!
//! This is not closed with a per-owner metric label on purpose. Owner ids are
//! monotonic and never reused, so a process that stops and restarts a runtime
//! would mint label values without bound — an ADR-0001 §7 violation, and a
//! worse bug than the one it would fix. A separate "worst registered owner"
//! gauge was likewise rejected: exporting it needs its own sampler loop, which
//! is another background loop carrying the very wedge risk this module exists
//! to detect, whereas a per-instance counter is self-reporting.
//!
//! The capability is not absent, only on a different surface: the registry
//! tracks these owners **separately** (that is what [`ScannerOwner`] ids are
//! for), so the `scanner_liveness` preflight check reports the worst of them
//! and names the affected shard. An operator running two runtimes of the same
//! kind in one process should treat that check — not the counter — as the
//! authority for those scanners.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, RwLock};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};

use crate::telemetry::{MetricsRecorder, SCANNER_SHARD_LABEL_NONE};
use crate::types::ShardId;

/// Lower bound on the staleness threshold, regardless of how fast a loop
/// polls. Keeps a sub-second poll interval from producing a hair-trigger
/// threshold that would flap on ordinary scheduler jitter.
///
/// # Deliberate deviation from a literal "2 x poll interval" bound
///
/// For the seven sub-minute loops this floor, not the `2 x` multiplier, is
/// what binds: a 500 ms loop is detected in 60 s, **not** 1 s. That is intentional,
/// and `success_metric_detection_bound_holds_for_every_shipped_interval` pins
/// it explicitly so the bound is never over-claimed.
///
/// The reason is that a loop's *iteration period* is not its poll interval.
/// Each pass sleeps `poll_interval` and **then** does real work: the timeout
/// checker calls `pool.get().await` (deadpool waits for a free connection --
/// by default with no timeout at all) and then runs a multi-query enforcement
/// pass. Under a saturated pool or a slow query a perfectly healthy 500 ms
/// loop can legitimately take seconds per iteration, so a 1 s threshold would
/// report `stale` continuously on a busy-but-working system.
///
/// The cost asymmetry decides it. This verdict feeds `GET /admin/preflight`,
/// and the CLI exits `2` on `warn` / `1` on `fail`, so a false `warn` **breaks
/// a deploy pipeline**. A missed 60 s of detection latency is a far cheaper
/// error than a check that flaps under load. Sixty seconds absolute is also
/// still well inside the issue's intent (catch a wedge in ~a minute, not in
/// hours) -- and is *better* in absolute terms than the literal `2 x` bound
/// would give the hourly retention janitor, which is 2 hours.
pub const MIN_SCANNER_STALENESS_THRESHOLD: Duration = Duration::from_secs(60);

/// The bounded `scanner` label value set for
/// [`METRIC_SCANNER_TICK`](crate::telemetry::METRIC_SCANNER_TICK).
///
/// Deliberately a closed enum rather than a free string (mirroring
/// [`SlotType`](crate::telemetry::SlotType)): per ADR-0001 §7 a metric label
/// must have bounded cardinality, and an enum makes that true *by
/// construction* rather than by convention.
///
/// # Shared liveness fate
///
/// There are eight labels but **six** spawned loops: [`Sla`](Self::Sla) and
/// [`ExternalOutbox`](Self::ExternalOutbox) are enforcement responsibilities
/// *inside* the timeout loop, not tasks of their own. All three are registered
/// and ticked together by `spawn_timeout_checker`, so they share one loop's
/// liveness and **cannot diverge**: `timeout` healthy implies `sla` and
/// `external_outbox` healthy.
///
/// They keep distinct labels because issue #797 names them in the bounded set
/// and because they name distinct responsibilities an operator reasons about —
/// not because a divergence between them is observable. Ticking them from the
/// loop body rather than mid-pass is deliberate: a mid-pass tick would sit
/// behind a `?` and be skipped on a transient DB error, which would make a
/// flat-lined counter mean two different things. Attributing a *partial* pass
/// failure to a specific sub-pass is the job of that pass's own work counters
/// and its `tracing::error!`, not of a liveness heartbeat.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Scanner {
    /// The timeout-enforcement loop (`spawn_timeout_checker`), which drives
    /// activity/workflow timeouts and every sub-pass below it.
    Timeout,
    /// The soft-SLA breach sub-pass (`enforce_workflow_sla_breaches`), ticked
    /// by the timeout loop that drives it.
    Sla,
    /// The poison-pill orphan reclaimer (`spawn_poison_pill_reclaimer`).
    PoisonPill,
    /// The external signal/cancel/await outbox sub-passes, ticked by the
    /// timeout loop that drives them.
    ExternalOutbox,
    /// The retention janitor loop (`RetentionRuntime::spawn`).
    Retention,
    /// The schedule ticker loop (`Scheduler::spawn_sharded`).
    Schedule,
    /// The bounded-pause auto-resumer (`spawn_pause_auto_resumer`).
    PauseAutoResume,
    /// The dedicated audit-export task
    /// (`crate::audit_export::spawn_audit_export_checker_for_shard`, issue
    /// #1269). Previously folded into the timeout loop; split out to its own
    /// task so a slow sink delays nothing but its own next tick.
    AuditExport,
}

impl Scanner {
    /// Every scanner, in a stable order. Used by tests and docs to enumerate
    /// the bounded label set.
    pub const ALL: [Self; 8] = [
        Self::Timeout,
        Self::Sla,
        Self::PoisonPill,
        Self::ExternalOutbox,
        Self::Retention,
        Self::Schedule,
        Self::PauseAutoResume,
        Self::AuditExport,
    ];

    /// Stable string representation, suitable for a metric label value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::Sla => "sla",
            Self::PoisonPill => "poison_pill",
            Self::ExternalOutbox => "external_outbox",
            Self::Retention => "retention",
            Self::Schedule => "schedule",
            Self::PauseAutoResume => "pause_auto_resume",
            Self::AuditExport => "audit_export",
        }
    }
}

/// Staleness threshold for a loop polling every `poll_interval`.
///
/// `max(2 × poll_interval, 60s)` — two missed iterations before we call a
/// loop stale, floored so a fast loop does not flap. Saturating throughout,
/// so a pathological configured interval degrades instead of panicking.
#[must_use]
pub fn staleness_threshold(poll_interval: Duration) -> Duration {
    poll_interval
        .saturating_mul(2)
        .max(MIN_SCANNER_STALENESS_THRESHOLD)
}

/// How healthy a single scanner's last-tick age looks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScannerLivenessVerdict {
    /// Ticked within one staleness threshold.
    Healthy,
    /// Has not ticked for at least one threshold — surfaced as `warn`.
    Stale,
    /// Has not ticked for at least two thresholds — surfaced as `fail`.
    Wedged,
}

impl ScannerLivenessVerdict {
    /// Severity order, for folding several instances of one [`Scanner`] down
    /// to the worst. Deliberately not a derived `Ord` on the public enum: the
    /// ordering is a severity policy, not an inherent property of the values.
    const fn rank(self) -> u8 {
        match self {
            Self::Healthy => 0,
            Self::Stale => 1,
            Self::Wedged => 2,
        }
    }
}

/// The one staleness policy, applied to a single instance's own reading.
///
/// Shared by [`classify_scanner`] and the per-instance fold in
/// [`ScannerLiveness::snapshot_as_of`], so a folded status and the public
/// classifier can never disagree about the same numbers.
fn classify_reading(age: Duration, poll_interval: Duration) -> ScannerLivenessVerdict {
    let threshold = staleness_threshold(poll_interval);
    if age >= threshold.saturating_mul(2) {
        ScannerLivenessVerdict::Wedged
    } else if age >= threshold {
        ScannerLivenessVerdict::Stale
    } else {
        ScannerLivenessVerdict::Healthy
    }
}

/// A point-in-time liveness reading for one scanner.
///
/// `age` is resolved from a **monotonic** clock at snapshot time (immune to
/// wall-clock steps), so [`classify_scanner`] is a pure function of this
/// struct. `last_tick_at` is the wall-clock companion, for human display
/// only — never for staleness math.
#[derive(Debug, Clone)]
pub struct ScannerStatus {
    /// Which control loop this reading describes.
    pub scanner: Scanner,
    /// The loop's configured poll interval, as registered.
    pub poll_interval: Duration,
    /// How many iterations this scanner has completed since process start.
    pub tick_count: u64,
    /// Wall-clock stamp of the most recent tick, for display. `None` until
    /// the first tick.
    pub last_tick_at: Option<DateTime<Utc>>,
    /// Monotonic time since the most recent tick, or since registration when
    /// the scanner has not ticked yet (the boot-grace window).
    pub age: Duration,
    /// Whether this scanner has completed at least one iteration.
    pub has_ticked: bool,
    /// Which shard the reported instance polls, when this loop is one of the
    /// per-shard set a multi-shard worker spawns (timeout, poison-pill,
    /// pause-auto-resume). `None` for the process-wide loops (retention,
    /// schedule) and for single-shard deployments.
    ///
    /// This is the *worst* instance's shard, so it names the single database
    /// most in need of attention. When more than one shard is affected, read
    /// [`stale_shards`](Self::stale_shards) instead — this field alone
    /// understates the blast radius.
    pub shard: Option<ShardId>,
    /// **Every** shard whose instance of this scanner is currently
    /// `Stale` or `Wedged`, sorted and deduplicated.
    ///
    /// The worst-instance fold that produces [`shard`](Self::shard) is right
    /// for the *verdict* — one verdict per scanner, reproducible from the
    /// folded status — but wrong for the *blast radius*: two shards can be
    /// wedged at once, and reporting only the worse of them tells an operator
    /// to fix one database while the other stays unprotected. This field is
    /// what populates the preflight check's standard `affected_shards`.
    ///
    /// Empty when the scanner is healthy, and for the process-wide loops
    /// (`retention`, `schedule`) and single-shard deployments, which carry no
    /// shard to report.
    pub stale_shards: Vec<ShardId>,
}

/// Classify one scanner's liveness reading.
///
/// Pure: takes an already-resolved [`ScannerStatus`], so the whole
/// warn/fail policy is unit-testable without a clock or the process-global
/// registry.
#[must_use]
pub fn classify_scanner(status: &ScannerStatus) -> ScannerLivenessVerdict {
    classify_reading(status.age, status.poll_interval)
}

/// Opaque identity of one *registered loop instance*.
///
/// Minted by [`ScannerLiveness::register`]. Distinct instances of the same
/// [`Scanner`] — the per-shard timeout checkers, poison-pill reclaimers, and
/// pause auto-resumers a multi-shard worker spawns one of per assigned shard —
/// each get their own id, which is what stops a healthy peer from masking a
/// wedged sibling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ScannerOwner {
    scanner: Scanner,
    id: u64,
    shard: Option<ShardId>,
}

impl ScannerOwner {
    /// Which control loop this registration is for.
    #[must_use]
    pub const fn scanner(self) -> Scanner {
        self.scanner
    }

    /// Which shard this instance polls, for the per-shard loops.
    ///
    /// Carried on the handle rather than looked up at tick time so
    /// [`record_scanner_tick`] can label the counter without touching the
    /// registry lock on the hot path.
    #[must_use]
    pub const fn shard(self) -> Option<ShardId> {
        self.shard
    }

    /// This instance's `shard` metric label value.
    ///
    /// [`SCANNER_SHARD_LABEL_NONE`] for a process-wide loop, so every
    /// `harvest.scanner.tick` series carries the same label set.
    #[must_use]
    pub fn shard_label(self) -> String {
        self.shard.map_or_else(
            || SCANNER_SHARD_LABEL_NONE.to_owned(),
            |shard| shard.as_i32().to_string(),
        )
    }
}

/// Per-instance liveness state.
#[derive(Debug)]
struct OwnerState {
    poll_interval: Duration,
    registered_at: Instant,
    last_tick: Option<Instant>,
    last_tick_at: Option<DateTime<Utc>>,
    tick_count: u64,
    /// The shard this instance polls, for the per-shard loops. Recorded here
    /// rather than derived, because the id is only knowable at the spawn site.
    shard: Option<ShardId>,
}

impl OwnerState {
    /// The instant staleness is measured from, for this instance.
    ///
    /// `max(last_tick, registered_at)` rather than just `last_tick`: a fresh
    /// registration must re-grant the grace window. Falls back to
    /// `registered_at` before the first tick, which is what gives a
    /// just-booted loop its one-interval grace.
    ///
    /// Because this is *per instance*, a newly starting peer's grace can no
    /// longer mask an already-wedged sibling — the fold in
    /// [`ScannerLiveness::snapshot_as_of`] reports the **worst** owner.
    fn reference(&self) -> Instant {
        self.last_tick
            .map_or(self.registered_at, |tick| tick.max(self.registered_at))
    }
}

/// Live instances of one [`Scanner`], keyed by the id minted at registration.
///
/// A map rather than a count so liveness folds by the **worst** instance
/// instead of last-write-wins: on a multi-shard worker N loops share one
/// `Scanner` label, and a shared last-tick would let three healthy shards hide
/// the fourth's panicked loop — exactly the failure this module exists to
/// detect. The map being empty is also what marks the scanner as no longer
/// expected in this process.
type Owners = BTreeMap<u64, OwnerState>;

/// In-process registry of background control-loop liveness.
///
/// Each loop [`register`](Self::register)s itself at spawn time with its poll
/// interval, receiving a [`ScannerOwner`] handle, then [`tick`](Self::tick)s
/// **that handle** at the end of every iteration. Registration is what defines
/// the *expected* scanner set, so a process that runs no control loops (an
/// API-only replica) reports an empty snapshot rather than seven phantom
/// wedged scanners; and because a tick is only reachable through a handle, a
/// direct call to a `pub` enforcement primitive can never poison the registry.
///
/// Ordinarily accessed through the process-global instance
/// ([`global_scanner_liveness`]); it is a plain instantiable struct so tests
/// can exercise it in isolation.
#[derive(Debug, Default)]
pub struct ScannerLiveness {
    entries: RwLock<BTreeMap<Scanner, Owners>>,
    /// Source of per-instance ids. Monotonic and never reused, so a stale
    /// handle from a already-deregistered loop can never resurrect or
    /// mistakenly refresh a live peer.
    next_id: AtomicU64,
}

impl ScannerLiveness {
    /// A registry with no scanners registered.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            entries: RwLock::new(BTreeMap::new()),
            next_id: AtomicU64::new(0),
        }
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, BTreeMap<Scanner, Owners>> {
        self.entries
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, BTreeMap<Scanner, Owners>> {
        self.entries
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Declare that one instance of `scanner` is running in this process,
    /// polling every `poll_interval`.
    ///
    /// Call once at spawn time and keep the returned handle for the loop's
    /// lifetime. Each call mints a **distinct** instance: two workers in one
    /// process, or the per-shard loops a multi-shard worker spawns, are
    /// tracked independently rather than collapsing onto one shared last-tick.
    pub fn register(&self, scanner: Scanner, poll_interval: Duration) -> ScannerOwner {
        self.register_at(scanner, poll_interval, Instant::now())
    }

    /// [`register`](Self::register) for one of the **per-shard** loops.
    ///
    /// A multi-shard worker spawns one timeout checker, poison-pill reclaimer,
    /// and pause auto-resumer per assigned shard, all sharing one [`Scanner`]
    /// label. Recording which shard each instance polls does two things: it
    /// lets the liveness snapshot name the database left unprotected, and it
    /// supplies the `shard` metric label that keeps each instance on its own
    /// `harvest.scanner.tick` series so a healthy sibling cannot mask a
    /// wedged one.
    pub fn register_for_shard(
        &self,
        scanner: Scanner,
        poll_interval: Duration,
        shard: Option<ShardId>,
    ) -> ScannerOwner {
        self.register_for_shard_at(scanner, poll_interval, shard, Instant::now())
    }

    /// [`register`](Self::register) with an injected monotonic clock reading.
    ///
    /// Exposed so liveness policy can be tested deterministically without
    /// sleeping; not part of the operator-facing surface.
    #[doc(hidden)]
    pub fn register_at(
        &self,
        scanner: Scanner,
        poll_interval: Duration,
        now: Instant,
    ) -> ScannerOwner {
        self.register_for_shard_at(scanner, poll_interval, None, now)
    }

    /// [`register_for_shard`](Self::register_for_shard) with an injected
    /// monotonic clock reading, for deterministic tests.
    #[doc(hidden)]
    pub fn register_for_shard_at(
        &self,
        scanner: Scanner,
        poll_interval: Duration,
        shard: Option<ShardId>,
        now: Instant,
    ) -> ScannerOwner {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut entries = self.write();
        entries.entry(scanner).or_default().insert(
            id,
            OwnerState {
                poll_interval,
                registered_at: now,
                last_tick: None,
                last_tick_at: None,
                tick_count: 0,
                shard,
            },
        );
        drop(entries);
        ScannerOwner { scanner, id, shard }
    }

    /// Declare that one instance has stopped **gracefully**.
    ///
    /// The scanner leaves the expected set only once its last instance
    /// deregisters, so a process running two workers keeps reporting the
    /// surviving one.
    ///
    /// Call this **only** on a clean loop exit (cancellation / shutdown).
    /// A loop that panics or wedges must stay registered — going stale is
    /// precisely the signal issue #797 exists to produce.
    ///
    /// Deregistering an already-deregistered handle is a no-op, so an
    /// unbalanced call can never panic or disturb a live peer.
    pub fn deregister(&self, owner: ScannerOwner) {
        let mut entries = self.write();
        if let std::collections::btree_map::Entry::Occupied(mut occupied) =
            entries.entry(owner.scanner)
        {
            occupied.get_mut().remove(&owner.id);
            if occupied.get().is_empty() {
                occupied.remove();
            }
        }
        drop(entries);
    }

    /// Record that this instance completed one iteration.
    pub fn tick(&self, owner: ScannerOwner) {
        self.tick_at(owner, Instant::now());
    }

    /// [`tick`](Self::tick) with an injected monotonic clock reading.
    ///
    /// A tick from a handle that has already been deregistered is dropped:
    /// ids are never reused, so it can neither resurrect the retired instance
    /// nor refresh a live peer's clock.
    ///
    /// The injected clock exists for deterministic tests; not part of the
    /// operator-facing surface.
    #[doc(hidden)]
    pub fn tick_at(&self, owner: ScannerOwner, now: Instant) {
        // Resolve the wall clock BEFORE taking the write lock: this runs on
        // every iteration of every control loop, so the critical section is
        // kept to the map mutation alone.
        let wall_now = Utc::now();
        let mut entries = self.write();
        if let Some(state) = entries
            .get_mut(&owner.scanner)
            .and_then(|owners| owners.get_mut(&owner.id))
        {
            state.last_tick = Some(now);
            state.last_tick_at = Some(wall_now);
            state.tick_count = state.tick_count.saturating_add(1);
        }
        drop(entries);
    }

    /// How many live instances of `scanner` are registered.
    ///
    /// Exposed so a test can assert that a loop released its own registration
    /// on shutdown without being perturbed by *other* registrations held
    /// concurrently in the same process (parallel tests in one binary); not
    /// part of the operator-facing surface.
    #[doc(hidden)]
    #[must_use]
    pub fn registrations(&self, scanner: Scanner) -> usize {
        self.read().get(&scanner).map_or(0, BTreeMap::len)
    }

    /// Readings for every registered scanner, ordered by [`Scanner`].
    ///
    /// One reading per scanner, folded across its live instances so the
    /// existing single-status-per-scanner health-check payload is unchanged.
    ///
    /// The fold classifies **each instance against its own registered poll
    /// interval** and then keeps the instance with the **worst verdict** —
    /// rather than folding `age` and `poll_interval` independently and
    /// classifying once. That distinction is load-bearing: two `Worker`s (or
    /// two `RetentionRuntime`s) in one process may register the same
    /// [`Scanner`] with *different* intervals, and taking `age` from the worst
    /// instance while taking `poll_interval` from the most lenient one would
    /// judge the wedged instance against a threshold it never agreed to — a
    /// 30 s loop silent for 200 s would read `Healthy` next to an hourly
    /// sibling. Every field below therefore describes **one** instance, so
    /// `classify_scanner(&status)` reproduces the folded verdict exactly.
    ///
    /// - `poll_interval`, `age`, `last_tick_at`, `has_ticked` — the worst
    ///   instance's own values, which is the configuration an operator needs
    ///   to look at;
    /// - `tick_count` — the **sum** across instances, matching the counter
    ///   metric (which likewise carries no per-instance label).
    ///
    /// Ties are broken by how far past its *own* threshold an instance has
    /// drifted, then by raw age, so the reported instance is the most
    /// meaningfully-worst rather than merely the oldest.
    #[must_use]
    pub fn snapshot(&self) -> Vec<ScannerStatus> {
        self.snapshot_as_of(Instant::now())
    }

    /// [`snapshot`](Self::snapshot) with an injected monotonic clock reading,
    /// for deterministic tests; not part of the operator-facing surface.
    #[doc(hidden)]
    #[must_use]
    pub fn snapshot_as_of(&self, now: Instant) -> Vec<ScannerStatus> {
        self.read()
            .iter()
            .filter_map(|(scanner, owners)| {
                let tick_count = owners
                    .values()
                    .fold(0_u64, |acc, state| acc.saturating_add(state.tick_count));
                let (worst, age) = owners
                    .values()
                    .map(|state| (state, now.saturating_duration_since(state.reference())))
                    .max_by_key(|(state, age)| {
                        (
                            classify_reading(*age, state.poll_interval).rank(),
                            age.saturating_sub(staleness_threshold(state.poll_interval)),
                            *age,
                        )
                    })?;
                // Every stale instance's shard, not just the worst one's: two
                // shards can be wedged at once, and the worst-instance fold
                // that decides the *verdict* would otherwise silently drop the
                // second unprotected database from the blast radius.
                let mut stale_shards: Vec<ShardId> = owners
                    .values()
                    .filter(|state| {
                        classify_reading(
                            now.saturating_duration_since(state.reference()),
                            state.poll_interval,
                        ) != ScannerLivenessVerdict::Healthy
                    })
                    .filter_map(|state| state.shard)
                    .collect();
                stale_shards.sort_unstable();
                stale_shards.dedup();
                Some(ScannerStatus {
                    scanner: *scanner,
                    poll_interval: worst.poll_interval,
                    tick_count,
                    last_tick_at: worst.last_tick_at,
                    age,
                    has_ticked: worst.last_tick.is_some(),
                    shard: worst.shard,
                    stale_shards,
                })
            })
            .collect()
    }
}

/// The process-global control-loop liveness registry.
///
/// Follows the crate's established cross-crate in-process state pattern (see
/// `admission_gate::GLOBAL_ADMISSION_GATE_CACHE`): the loops live in the core
/// crate while the health check that reads them lives in the plugin, and
/// threading a handle through six spawn signatures buys nothing over a
/// single append-only registry.
///
/// Instances are tracked individually: a scanner leaves the expected set only
/// when its **last** instance deregisters on a clean shutdown, so a
/// two-runtime (or multi-shard) process keeps reporting the survivors. A loop
/// that panics or wedges never deregisters and correctly ages into
/// `Stale`/`Wedged`.
static GLOBAL_SCANNER_LIVENESS: LazyLock<ScannerLiveness> = LazyLock::new(ScannerLiveness::new);

/// Access the process-global control-loop liveness registry.
#[must_use]
pub fn global_scanner_liveness() -> &'static ScannerLiveness {
    &GLOBAL_SCANNER_LIVENESS
}

/// Declare a control loop running in this process (process-global registry).
///
/// Call once at spawn time, before the loop's first iteration, and keep the
/// returned handle: it is what [`record_scanner_tick`] and
/// [`deregister_scanner`] address.
///
/// Also **initializes the scanner's tick series at zero**. Registration is the
/// earliest moment the process knows the loop is supposed to be running, and a
/// loop that panics or hangs on its *first* iteration never reaches a tick —
/// so without this it would export no series at all and `rate(...) == 0` would
/// match nothing, silently. See
/// [`MetricsRecorder::record_scanner_registered`].
#[must_use]
pub fn register_scanner(
    metrics: &dyn MetricsRecorder,
    scanner: Scanner,
    poll_interval: Duration,
) -> ScannerOwner {
    register_scanner_on(
        global_scanner_liveness(),
        metrics,
        scanner,
        poll_interval,
        None,
    )
}

/// [`register_scanner`] for one of the **per-shard** loops.
///
/// The timeout checker, poison-pill reclaimer, and pause auto-resumer are
/// spawned one-per-assigned-shard, all under one [`Scanner`] label. Passing the
/// shard here both lets `scanner_liveness` name the unprotected database and
/// puts each instance on its own `harvest.scanner.tick` series, so a healthy
/// shard's ticks cannot hold the alert quiet while a sibling is wedged.
#[must_use]
pub fn register_scanner_for_shard(
    metrics: &dyn MetricsRecorder,
    scanner: Scanner,
    poll_interval: Duration,
    shard: Option<ShardId>,
) -> ScannerOwner {
    register_scanner_on(
        global_scanner_liveness(),
        metrics,
        scanner,
        poll_interval,
        shard,
    )
}

/// [`register_scanner`] against an explicit registry, for tests that must not
/// touch process-global state.
#[doc(hidden)]
#[must_use]
pub fn register_scanner_on(
    liveness: &ScannerLiveness,
    metrics: &dyn MetricsRecorder,
    scanner: Scanner,
    poll_interval: Duration,
    shard: Option<ShardId>,
) -> ScannerOwner {
    let owner = liveness.register_for_shard(scanner, poll_interval, shard);
    metrics.record_scanner_registered(scanner.as_str(), &owner.shard_label());
    owner
}

/// Declare that a control loop has stopped **gracefully** (process-global
/// registry).
///
/// Call on a clean loop exit only — never on the panic path, where remaining
/// registered (and therefore going stale) is the intended signal. See
/// [`ScannerLiveness::deregister`].
pub fn deregister_scanner(owner: ScannerOwner) {
    global_scanner_liveness().deregister(owner);
}

/// Record one completed control-loop iteration: the single choke point for
/// both halves of issue #797.
///
/// Bumps the process-global liveness timestamp **and** increments
/// `harvest.scanner.tick`. Because one call drives both, a call site cannot
/// emit the counter without the timestamp (or vice versa), and the liveness
/// half keeps working when no metrics recorder is configured.
///
/// Call **at the end** of each iteration — "one full pass completed" is a
/// strictly stronger liveness signal than "one pass was scheduled" — and
/// call it unconditionally, whether or not the iteration found work.
pub fn record_scanner_tick(metrics: &dyn MetricsRecorder, owner: ScannerOwner) {
    record_scanner_tick_on(global_scanner_liveness(), metrics, owner);
}

/// [`record_scanner_tick`] against an explicit registry, for tests that must
/// not touch process-global state.
#[doc(hidden)]
pub fn record_scanner_tick_on(
    liveness: &ScannerLiveness,
    metrics: &dyn MetricsRecorder,
    owner: ScannerOwner,
) {
    liveness.tick(owner);
    metrics.record_scanner_tick(owner.scanner().as_str(), &owner.shard_label());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn threshold_floor_applies_below_thirty_second_intervals() {
        assert_eq!(
            staleness_threshold(Duration::from_millis(500)),
            MIN_SCANNER_STALENESS_THRESHOLD
        );
    }

    #[test]
    fn reference_prefers_the_later_of_tick_and_registration() {
        let t0 = Instant::now();
        let state = OwnerState {
            poll_interval: Duration::from_secs(30),
            registered_at: t0 + Duration::from_secs(100),
            last_tick: Some(t0),
            last_tick_at: None,
            tick_count: 1,
            shard: None,
        };
        // A stale tick from a stopped runtime must not beat a fresh
        // registration.
        assert_eq!(state.reference(), t0 + Duration::from_secs(100));
    }

    #[test]
    fn tick_from_a_deregistered_handle_is_dropped() {
        // Ids are never reused, so a straggling tick from a retired loop can
        // neither resurrect it nor refresh a live peer's clock.
        let liveness = ScannerLiveness::new();
        let t0 = Instant::now();
        let retired = liveness.register_at(Scanner::Sla, Duration::from_secs(30), t0);
        let live = liveness.register_at(Scanner::Sla, Duration::from_secs(30), t0);
        liveness.deregister(retired);

        liveness.tick_at(retired, t0 + Duration::from_secs(10));

        assert_eq!(liveness.registrations(Scanner::Sla), 1);
        let status = &liveness.snapshot_as_of(t0)[0];
        assert_eq!(status.tick_count, 0, "the retired tick must not be counted");
        assert!(!status.has_ticked);
        liveness.deregister(live);
        assert!(liveness.snapshot_as_of(t0).is_empty());
    }

    #[test]
    fn snapshot_age_never_goes_backwards_on_an_early_clock_reading() {
        // `saturating_duration_since` guards a caller that passes a `now`
        // older than the reference instant.
        let liveness = ScannerLiveness::new();
        let earlier = Instant::now();
        let t0 = earlier + Duration::from_secs(60);
        let _owner = liveness.register_at(Scanner::Timeout, Duration::from_secs(30), t0);
        let status = &liveness.snapshot_as_of(earlier)[0];
        assert_eq!(status.age, Duration::ZERO);
    }
}
