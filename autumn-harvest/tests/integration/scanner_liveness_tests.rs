//! Background control-loop liveness heartbeats (issue #797).
//!
//! Harvest's correctness depends on ~7 background control loops that run as
//! bare spawned Tokio tasks. A wedged loop fails *silently* today: the
//! existing loop metrics only emit when there is work to do, so a healthy
//! idle loop and a dead one are indistinguishable.
//!
//! These tests pin the two halves of the fix:
//!
//! 1. every loop emits `harvest.scanner.tick` on **every** iteration —
//!    including no-work iterations — under a bounded `scanner` label; and
//! 2. an in-process last-tick registry classifies a scanner that has not
//!    ticked within `max(2 × poll_interval, 60s)` as stale.
//!
//! No DB required — the registry and the classifier are pure.

use std::time::Duration;

use autumn_harvest::scanner_health::{
    MIN_SCANNER_STALENESS_THRESHOLD, Scanner, ScannerLiveness, ScannerLivenessVerdict,
    classify_scanner, staleness_threshold,
};
use autumn_harvest::telemetry::{METRIC_LABEL_SCANNER, METRIC_SCANNER_TICK, MetricsRecorder};
use autumn_harvest::types::ShardId;

// ---------------------------------------------------------------------------
// AC1: bounded `scanner` label value set
// ---------------------------------------------------------------------------

#[test]
fn scanner_label_values_match_the_issue_bounded_set() {
    // The issue named exactly seven values; issue #1269 added an eighth
    // (`audit_export`) for the task it split out of the timeout loop. A new
    // variant without a deliberate decision here turns this red.
    let names: Vec<&str> = Scanner::ALL.iter().map(|s| s.as_str()).collect();
    assert_eq!(
        names,
        vec![
            "timeout",
            "sla",
            "poison_pill",
            "external_outbox",
            "retention",
            "schedule",
            "pause_auto_resume",
            "audit_export",
        ],
        "harvest.scanner.tick `scanner` label must carry exactly the bounded \
         value set from issue #797, plus issue #1269's addition"
    );
}

#[test]
fn scanner_label_values_are_unique_and_lowercase() {
    let mut seen = std::collections::BTreeSet::new();
    for scanner in Scanner::ALL {
        let value = scanner.as_str();
        assert!(
            seen.insert(value),
            "duplicate scanner label value `{value}` — the label set must be injective"
        );
        assert!(
            value
                .chars()
                .all(|c| c.is_ascii_lowercase() || c == '_' || c.is_ascii_digit()),
            "scanner label `{value}` must be lower_snake_case for metric hygiene"
        );
    }
}

#[test]
fn metric_and_label_constants_have_the_documented_names() {
    assert_eq!(METRIC_SCANNER_TICK, "harvest.scanner.tick");
    assert_eq!(METRIC_LABEL_SCANNER, "scanner");
}

// ---------------------------------------------------------------------------
// AC2: additive no-op default on MetricsRecorder
// ---------------------------------------------------------------------------

#[test]
fn record_scanner_tick_has_a_noop_default() {
    // A recorder that implements NOTHING but the required methods must still
    // compile and accept record_scanner_tick — proving the trait method is
    // purely additive and breaks no downstream implementor.
    struct Minimal;
    impl MetricsRecorder for Minimal {}

    Minimal.record_scanner_tick(Scanner::Timeout.as_str(), "0");
    Minimal.record_scanner_tick(Scanner::Retention.as_str(), "none");
}

// ---------------------------------------------------------------------------
// AC4: staleness threshold = max(2 × poll_interval, 60s)
// ---------------------------------------------------------------------------

#[test]
fn staleness_threshold_is_twice_the_poll_interval_with_a_60s_floor() {
    // Fast loop: the 60s floor dominates.
    assert_eq!(
        staleness_threshold(Duration::from_secs(1)),
        Duration::from_secs(60)
    );
    // Exactly at the floor boundary (2 × 30s == 60s).
    assert_eq!(
        staleness_threshold(Duration::from_secs(30)),
        Duration::from_secs(60)
    );
    // Slow loop: 2× dominates.
    assert_eq!(
        staleness_threshold(Duration::from_secs(300)),
        Duration::from_secs(600)
    );
    // Degenerate zero interval still yields the floor, never zero (a zero
    // threshold would classify every scanner as instantly wedged).
    assert_eq!(
        staleness_threshold(Duration::ZERO),
        MIN_SCANNER_STALENESS_THRESHOLD
    );
}

/// The issue's success metric — "detect a wedged loop within <= 2x that loop's
/// poll interval" — held honestly against the **real shipped intervals**.
///
/// It is satisfied for every loop, but by two different mechanisms, and the
/// distinction matters to an operator sizing an alert window:
///
/// * a loop polling at or above 30 s is bounded by the `2x` multiplier, so
///   detection latency is literally `2 x poll_interval`;
/// * a loop polling faster than 30 s is bounded by the 60 s floor, so its
///   detection latency is *far better in absolute terms* (60 s rather than
///   hours) but *worse as a multiple* of its interval. The floor is deliberate:
///   `2 x 500ms` would flap on ordinary scheduler jitter and page on a healthy
///   loop, which is a strictly worse failure than a 60 s detection delay.
///
/// Pinned against the actual constants rather than a convenient 30 s example,
/// so a future change to a poll interval surfaces here.
#[test]
fn success_metric_detection_bound_holds_for_every_shipped_interval() {
    // (loop, its shipped default poll interval)
    let shipped = [
        // WorkerConfig::poll_interval — timeout / sla / external_outbox
        (Scanner::Timeout, Duration::from_millis(500)),
        // worker_heartbeat_interval — poison_pill / pause_auto_resume
        (Scanner::PoisonPill, Duration::from_secs(5)),
        // DEFAULT_SCHEDULER_TICK_INTERVAL
        (Scanner::Schedule, Duration::from_secs(1)),
        // RetentionConfig's DEFAULT_TICK_INTERVAL
        (Scanner::Retention, Duration::from_secs(60 * 60)),
    ];

    for (scanner, interval) in shipped {
        let threshold = staleness_threshold(interval);
        let liveness = ScannerLiveness::new();
        let t0 = std::time::Instant::now();
        let owner = liveness.register_at(scanner, interval, t0);
        liveness.tick_at(owner, t0);

        // Just before the threshold the loop is still healthy...
        let just_before = (t0 + threshold)
            .checked_sub(Duration::from_millis(1))
            .expect("threshold is at least 60s, so backing off 1ms cannot underflow");
        let status = &liveness.snapshot_as_of(just_before)[0];
        assert_eq!(
            classify_scanner(status),
            ScannerLivenessVerdict::Healthy,
            "{scanner:?} must not be flagged before its threshold elapses"
        );
        // ...and at the threshold it is flagged.
        let status = &liveness.snapshot_as_of(t0 + threshold)[0];
        assert_ne!(
            classify_scanner(status),
            ScannerLivenessVerdict::Healthy,
            "{scanner:?} must be flagged once silent for one threshold"
        );

        // The honest bound: detection is within 2x the interval OR within the
        // 60s floor, whichever is larger. Never unbounded, never hours.
        assert!(
            threshold
                <= interval
                    .saturating_mul(2)
                    .max(MIN_SCANNER_STALENESS_THRESHOLD),
            "{scanner:?} detection latency must be bounded by max(2x interval, 60s)"
        );
    }

    // The floor genuinely dominates for the sub-minute loops: their detection
    // latency is 60s, NOT 2x their (sub-second) interval. Asserted explicitly
    // so the success metric is not quietly over-claimed.
    assert_eq!(
        staleness_threshold(Duration::from_millis(500)),
        MIN_SCANNER_STALENESS_THRESHOLD,
        "a 500ms loop is detected in 60s (the floor), not in 1s"
    );
    // And the 2x multiplier genuinely dominates for the hourly janitor.
    assert_eq!(
        staleness_threshold(Duration::from_secs(60 * 60)),
        Duration::from_secs(2 * 60 * 60),
        "the hourly retention janitor is detected in 2h — 2x its interval"
    );
}

#[test]
fn the_floor_tolerates_a_slow_but_healthy_iteration() {
    // Guards the deliberate deviation in the other direction: someone reading
    // the issue's literal "2x poll interval" success metric might be tempted
    // to drop or shrink the floor so a 500 ms loop is flagged in 1 s. That
    // would make the check flap on a busy-but-working system and, because the
    // CLI exits 2 on `warn`, break deploy pipelines.
    //
    // A loop's ITERATION PERIOD is not its poll interval: each pass sleeps the
    // interval and *then* does real work — the timeout checker calls
    // `pool.get().await` (deadpool waits for a free connection, by default
    // with no timeout at all) and then runs a multi-query enforcement pass. So
    // the threshold must comfortably exceed a plausible slow-but-healthy
    // iteration, not merely two sleeps.
    const PLAUSIBLE_SLOW_ITERATION: Duration = Duration::from_secs(30);
    assert!(
        MIN_SCANNER_STALENESS_THRESHOLD >= PLAUSIBLE_SLOW_ITERATION,
        "the floor must tolerate a single slow-but-healthy iteration \
         (saturated pool / slow query), or the check flaps under load and \
         fails deploy pipelines that gate on `harvest preflight`"
    );

    // Concretely, for every shipped sub-minute loop the threshold must absorb
    // many consecutive missed iterations before it says a word.
    for interval in [
        Duration::from_millis(500),
        Duration::from_secs(1),
        Duration::from_secs(5),
    ] {
        let missed = staleness_threshold(interval).as_secs_f64() / interval.as_secs_f64();
        assert!(
            missed >= 10.0,
            "a {interval:?} loop must be allowed to miss >= 10 iterations \
             before being flagged (got {missed:.1}); a hair-trigger threshold \
             flags healthy loops under load"
        );
    }
}

#[test]
fn staleness_threshold_saturates_instead_of_overflowing() {
    // A pathological interval must not panic in release-mode wrap or debug
    // overflow — an operator misconfiguration should degrade, not crash.
    let threshold = staleness_threshold(Duration::MAX);
    assert!(threshold >= MIN_SCANNER_STALENESS_THRESHOLD);
}

// ---------------------------------------------------------------------------
// AC4: classification (healthy / stale / wedged)
// ---------------------------------------------------------------------------

#[test]
fn a_scanner_that_ticked_recently_is_healthy() {
    let liveness = ScannerLiveness::new();
    let t0 = std::time::Instant::now();
    let owner = liveness.register_at(Scanner::Timeout, Duration::from_secs(30), t0);
    liveness.tick_at(owner, t0 + Duration::from_secs(30));

    let status = &liveness.snapshot_as_of(t0 + Duration::from_secs(45))[0];
    assert_eq!(classify_scanner(status), ScannerLivenessVerdict::Healthy);
    assert!(status.has_ticked);
    assert_eq!(status.tick_count, 1);
}

#[test]
fn a_scanner_past_one_threshold_is_stale_and_past_two_is_wedged() {
    let liveness = ScannerLiveness::new();
    let t0 = std::time::Instant::now();
    // 30s interval => 60s threshold.
    let owner = liveness.register_at(Scanner::Timeout, Duration::from_secs(30), t0);
    liveness.tick_at(owner, t0);

    // Just under threshold: healthy.
    let status = &liveness.snapshot_as_of(t0 + Duration::from_secs(59))[0];
    assert_eq!(classify_scanner(status), ScannerLivenessVerdict::Healthy);

    // At threshold: stale (warn).
    let status = &liveness.snapshot_as_of(t0 + Duration::from_secs(60))[0];
    assert_eq!(classify_scanner(status), ScannerLivenessVerdict::Stale);

    // At 2x threshold: wedged (fail).
    let status = &liveness.snapshot_as_of(t0 + Duration::from_secs(120))[0];
    assert_eq!(classify_scanner(status), ScannerLivenessVerdict::Wedged);
}

#[test]
fn a_just_registered_scanner_is_healthy_before_its_first_tick() {
    // Boot grace: a loop that sleeps its interval before the first iteration
    // must not make a preflight run immediately after boot report the fleet as
    // wedged.
    let liveness = ScannerLiveness::new();
    let t0 = std::time::Instant::now();
    let _owner = liveness.register_at(Scanner::Retention, Duration::from_secs(30), t0);

    let status = &liveness.snapshot_as_of(t0 + Duration::from_secs(5))[0];
    assert!(!status.has_ticked, "no tick has happened yet");
    assert_eq!(
        classify_scanner(status),
        ScannerLivenessVerdict::Healthy,
        "a scanner still inside its first interval must not be reported stale"
    );
}

#[test]
fn a_registered_scanner_that_never_ticks_eventually_goes_wedged() {
    // The complement of the boot-grace test: a loop that wedges on its very
    // FIRST iteration must still be caught, not excused forever.
    let liveness = ScannerLiveness::new();
    let t0 = std::time::Instant::now();
    let _owner = liveness.register_at(Scanner::Schedule, Duration::from_secs(30), t0);

    let status = &liveness.snapshot_as_of(t0 + Duration::from_secs(600))[0];
    assert!(!status.has_ticked);
    assert_eq!(classify_scanner(status), ScannerLivenessVerdict::Wedged);
}

// ---------------------------------------------------------------------------
// Per-instance isolation: one wedged loop is never masked by a healthy peer
// ---------------------------------------------------------------------------

#[test]
fn a_wedged_instance_is_not_masked_by_a_healthy_peer() {
    // THE multi-shard money test. A worker assigned N shards spawns one
    // timeout checker / poison-pill reclaimer / pause auto-resumer PER SHARD,
    // all sharing one `Scanner` label. If the registry collapsed them onto a
    // single last-tick, three healthy shards would hide the fourth's panicked
    // loop — precisely the failure this feature exists to detect.
    let liveness = ScannerLiveness::new();
    let t0 = std::time::Instant::now();
    let healthy = liveness.register_at(Scanner::Timeout, Duration::from_secs(30), t0);
    let wedged = liveness.register_at(Scanner::Timeout, Duration::from_secs(30), t0);

    // Both tick once, then the second instance stops (panicked / hung).
    liveness.tick_at(healthy, t0);
    liveness.tick_at(wedged, t0);

    // The healthy peer keeps ticking well past the wedged one's threshold.
    let now = t0 + Duration::from_secs(300);
    liveness.tick_at(healthy, now);

    let snapshot = liveness.snapshot_as_of(now);
    assert_eq!(snapshot.len(), 1, "one status per scanner label");
    let status = &snapshot[0];
    assert_eq!(
        classify_scanner(status),
        ScannerLivenessVerdict::Wedged,
        "the WORST instance must decide the verdict"
    );
    assert_eq!(
        status.age,
        Duration::from_secs(300),
        "age comes from the wedged instance, not the healthy one"
    );
    assert_eq!(
        status.tick_count, 3,
        "tick_count sums across instances, matching the counter metric"
    );
}

#[test]
fn a_freshly_registered_peer_does_not_regrant_grace_to_a_wedged_sibling() {
    // Restarting one loop (or spawning an extra shard's) must not reset the
    // clock on a sibling that is already wedged: per-instance state is what
    // keeps a new registration's boot grace from laundering a stuck peer.
    let liveness = ScannerLiveness::new();
    let t0 = std::time::Instant::now();
    let wedged = liveness.register_at(Scanner::Timeout, Duration::from_secs(30), t0);
    liveness.tick_at(wedged, t0);

    let late = t0 + Duration::from_secs(3600);
    assert_eq!(
        classify_scanner(&liveness.snapshot_as_of(late)[0]),
        ScannerLivenessVerdict::Wedged
    );

    let _fresh = liveness.register_at(Scanner::Timeout, Duration::from_secs(30), late);
    assert_eq!(
        classify_scanner(&liveness.snapshot_as_of(late + Duration::from_secs(5))[0]),
        ScannerLivenessVerdict::Wedged,
        "a new instance's grace window must not mask the wedged one"
    );
}

#[test]
fn a_clean_restart_regrants_grace_without_losing_tick_history() {
    // The legitimate restart shape: the old instance deregisters (clean stop)
    // BEFORE the new one registers, so no wedged instance is left behind.
    let liveness = ScannerLiveness::new();
    let t0 = std::time::Instant::now();
    let old = liveness.register_at(Scanner::Timeout, Duration::from_secs(30), t0);
    liveness.tick_at(old, t0);

    let late = t0 + Duration::from_secs(3600);
    assert_eq!(
        classify_scanner(&liveness.snapshot_as_of(late)[0]),
        ScannerLivenessVerdict::Wedged
    );

    liveness.deregister(old);
    let fresh = liveness.register_at(Scanner::Timeout, Duration::from_secs(30), late);
    let status = &liveness.snapshot_as_of(late + Duration::from_secs(5))[0];
    assert_eq!(classify_scanner(status), ScannerLivenessVerdict::Healthy);
    assert_eq!(
        status.tick_count, 0,
        "the retired instance's history retires with it"
    );

    liveness.tick_at(fresh, late + Duration::from_secs(5));
    assert_eq!(
        liveness.snapshot_as_of(late + Duration::from_secs(5))[0].tick_count,
        1
    );
}

#[test]
fn a_slow_healthy_poller_is_not_falsely_stale_next_to_a_fast_sibling() {
    // Two workers in one process may poll the same scanner class at different
    // cadences. The slow one must be judged against ITS OWN threshold: 100s of
    // silence is stale for a 1s poller but perfectly healthy for a 300s one.
    let liveness = ScannerLiveness::new();
    let t0 = std::time::Instant::now();
    let fast = liveness.register_at(Scanner::Timeout, Duration::from_secs(1), t0);
    let slow = liveness.register_at(Scanner::Timeout, Duration::from_secs(300), t0);
    liveness.tick_at(slow, t0);
    // The fast poller is up to date; only the slow one carries any age.
    liveness.tick_at(fast, t0 + Duration::from_secs(100));

    let status = &liveness.snapshot_as_of(t0 + Duration::from_secs(100))[0];
    assert_eq!(
        classify_scanner(status),
        ScannerLivenessVerdict::Healthy,
        "a 300s poller silent for 100s is well inside its own 600s threshold"
    );
}

/// A wedged instance must be judged against **its own** poll interval, never a
/// lenient sibling's.
///
/// The fold reports one `ScannerStatus` per `Scanner`, but the verdict inputs
/// (`age` and `poll_interval`) must come from the **same** instance. Sourcing
/// `age` from the worst instance while sourcing `poll_interval` from the most
/// lenient one silently launders a genuine outage: a 30s loop silent for 200s
/// is `Wedged` by its own configuration, yet reads `Healthy` when measured
/// against an hourly sibling's 7200s threshold — and `scanner_liveness`
/// reports `pass` on a dead loop, the exact failure this feature exists to
/// catch.
#[test]
fn a_wedged_instance_is_not_laundered_by_a_lenient_siblings_interval() {
    let liveness = ScannerLiveness::new();
    let t0 = std::time::Instant::now();
    // A: 30s cadence => 60s threshold => wedged at >= 120s. Silent for 200s.
    let wedged = liveness.register_at(Scanner::Timeout, Duration::from_secs(30), t0);
    liveness.tick_at(wedged, t0);
    // B: hourly cadence => 7200s threshold. Ticking normally.
    let lenient = liveness.register_at(Scanner::Timeout, Duration::from_secs(60 * 60), t0);

    let now = t0 + Duration::from_secs(200);
    liveness.tick_at(lenient, now);

    let status = &liveness.snapshot_as_of(now)[0];
    assert_eq!(
        classify_scanner(status),
        ScannerLivenessVerdict::Wedged,
        "the 30s instance is 200s silent — past 2x its own 60s threshold — so \
         the fold must not measure it against the hourly sibling's threshold"
    );
    assert_eq!(
        status.poll_interval,
        Duration::from_secs(30),
        "the reported interval must be the worst instance's own, so an \
         operator sees the configuration that was actually violated"
    );
    assert_eq!(status.age, Duration::from_secs(200));
}

/// Issue #797, Codex review: the fold already reports the *worst* instance,
/// but reporting only the `Scanner` type leaves an operator unable to act on a
/// multi-shard worker — which spawns one timeout/poison-pill/pause-auto-resume
/// loop **per assigned shard**, all under one label. Both the dashboard and the
/// alert notes send operators to `scanner_liveness` precisely because the
/// counter carries no shard label and so cannot localize a single-shard wedge;
/// the check has to actually deliver that.
///
/// The `ShardId` is already in hand at the spawn site (`shard_pools_for_monitors`
/// is built by mapping over `shard_assignments`), so this is pure plumbing.
#[test]
fn the_reported_instance_names_the_shard_that_is_wedged() {
    let liveness = ScannerLiveness::new();
    let t0 = std::time::Instant::now();
    let interval = Duration::from_secs(30);

    // Three per-shard timeout checkers, exactly as a multi-shard worker spawns
    // them. Shards 0 and 2 keep ticking; shard 1's loop wedges.
    let healthy_a =
        liveness.register_for_shard_at(Scanner::Timeout, interval, Some(ShardId::new(0)), t0);
    let wedged =
        liveness.register_for_shard_at(Scanner::Timeout, interval, Some(ShardId::new(1)), t0);
    let healthy_b =
        liveness.register_for_shard_at(Scanner::Timeout, interval, Some(ShardId::new(2)), t0);
    liveness.tick_at(wedged, t0);

    let now = t0 + Duration::from_secs(200);
    liveness.tick_at(healthy_a, now);
    liveness.tick_at(healthy_b, now);

    let status = &liveness.snapshot_as_of(now)[0];
    assert_eq!(
        classify_scanner(status),
        ScannerLivenessVerdict::Wedged,
        "shard 1's loop is 200s silent against its own 60s threshold"
    );
    assert_eq!(
        status.shard,
        Some(ShardId::new(1)),
        "the folded status must name the shard whose loop is wedged, not just \
         the scanner type -- otherwise an operator on a multi-shard worker \
         knows a timeout checker died but not which database is unprotected"
    );
}

/// Issue #797, Codex review (P1): a multi-shard worker spawns one `timeout`
/// loop **per assigned shard**. Without a per-shard dimension on the counter
/// they all increment one series on one scrape target, so a healthy shard's
/// ticks keep `rate(...) > 0` while another shard's loop is dead — the shipped
/// paging alert would never fire. That is *masking*, not merely an inability
/// to localize, so the metric carries a bounded `shard` label.
///
/// This is the falsifying test: drop the label from the recorder call and the
/// two instances become indistinguishable at the metric layer.
#[test]
fn each_per_shard_instance_ticks_its_own_metric_series() {
    #[derive(Default)]
    struct LabelCapture(std::sync::Mutex<Vec<(String, String)>>);
    impl MetricsRecorder for LabelCapture {
        fn record_scanner_tick(&self, scanner: &str, shard: &str) {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push((scanner.to_owned(), shard.to_owned()));
        }
    }

    let liveness = ScannerLiveness::new();
    let capture = LabelCapture::default();
    let interval = Duration::from_secs(30);

    let shard_0 = autumn_harvest::scanner_health::register_scanner_on(
        &liveness,
        &capture,
        Scanner::Timeout,
        interval,
        Some(ShardId::new(0)),
    );
    let shard_1 = autumn_harvest::scanner_health::register_scanner_on(
        &liveness,
        &capture,
        Scanner::Timeout,
        interval,
        Some(ShardId::new(1)),
    );

    // Shard 0 keeps ticking; shard 1's loop is wedged and never ticks again.
    autumn_harvest::scanner_health::record_scanner_tick_on(&liveness, &capture, shard_0);
    autumn_harvest::scanner_health::record_scanner_tick_on(&liveness, &capture, shard_0);
    autumn_harvest::scanner_health::record_scanner_tick_on(&liveness, &capture, shard_1);

    let ticks = capture
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    assert_eq!(
        ticks,
        vec![
            ("timeout".to_owned(), "0".to_owned()),
            ("timeout".to_owned(), "0".to_owned()),
            ("timeout".to_owned(), "1".to_owned()),
        ],
        "each per-shard instance must tick its OWN series -- sharing one series \
         lets a healthy shard's rate mask a wedged sibling and the paging alert \
         never fires"
    );

    // The distinguishing property, stated directly: shard 0 and shard 1 are
    // separate series, so `rate(...) == 0` evaluates shard 1 on its own.
    assert_ne!(shard_0.shard_label(), shard_1.shard_label());
}

/// The process-wide loops (`retention`, `schedule`) and single-shard
/// deployments have no shard id, but must still carry the label so the series
/// family has a uniform label set rather than a sometimes-present dimension.
#[test]
fn a_process_wide_loop_ticks_the_none_shard_label() {
    let liveness = ScannerLiveness::new();
    let owner = liveness.register(Scanner::Retention, Duration::from_secs(30));
    assert_eq!(
        owner.shard_label(),
        autumn_harvest::SCANNER_SHARD_LABEL_NONE,
        "a process-wide loop must emit the sentinel, not omit the label"
    );
}

/// Issue #797, Codex review: the worst-instance fold decides the *verdict*,
/// but two shards can be wedged at once. Folding to one owner and reporting
/// only its shard understates the blast radius on the surface an operator uses
/// to decide what to restart -- so every stale instance's shard is carried
/// through the snapshot alongside the worst one.
#[test]
fn the_snapshot_carries_every_stale_shard_not_just_the_worst() {
    let liveness = ScannerLiveness::new();
    let t0 = std::time::Instant::now();
    let interval = Duration::from_secs(30);

    // Three per-shard timeout checkers: shard 0 keeps ticking, shards 1 and 2
    // both go silent -- 1 for longer, so it is the worst instance.
    let healthy =
        liveness.register_for_shard_at(Scanner::Timeout, interval, Some(ShardId::new(0)), t0);
    let wedged_worse =
        liveness.register_for_shard_at(Scanner::Timeout, interval, Some(ShardId::new(1)), t0);
    let wedged_also =
        liveness.register_for_shard_at(Scanner::Timeout, interval, Some(ShardId::new(2)), t0);
    liveness.tick_at(wedged_worse, t0);
    liveness.tick_at(wedged_also, t0 + Duration::from_secs(60));

    let now = t0 + Duration::from_secs(200);
    liveness.tick_at(healthy, now);

    let status = &liveness.snapshot_as_of(now)[0];
    assert_eq!(
        status.shard,
        Some(ShardId::new(1)),
        "the worst instance still drives the single-shard field"
    );
    assert_eq!(
        status.stale_shards,
        vec![ShardId::new(1), ShardId::new(2)],
        "both silent shards must survive the fold -- reporting only shard 1 \
         sends the operator to one unprotected database while shard 2 stays \
         equally unprotected"
    );
}

/// The blast-radius list must stay empty while every instance is healthy, so a
/// healthy multi-shard worker renders SCOPE as `-` rather than naming shards.
#[test]
fn the_snapshot_reports_no_stale_shards_while_every_instance_is_healthy() {
    let liveness = ScannerLiveness::new();
    let t0 = std::time::Instant::now();
    let interval = Duration::from_secs(30);

    let a = liveness.register_for_shard_at(Scanner::Timeout, interval, Some(ShardId::new(0)), t0);
    let b = liveness.register_for_shard_at(Scanner::Timeout, interval, Some(ShardId::new(1)), t0);
    let now = t0 + Duration::from_secs(5);
    liveness.tick_at(a, now);
    liveness.tick_at(b, now);

    let status = &liveness.snapshot_as_of(now)[0];
    assert_eq!(classify_scanner(status), ScannerLivenessVerdict::Healthy);
    assert!(
        status.stale_shards.is_empty(),
        "a healthy fleet must report no affected shards: {:?}",
        status.stale_shards
    );
}

/// A process-wide loop (retention, schedule) has no shard to report, and must
/// say so rather than inventing one. Keeps the payload honest on the
/// single-shard deployments that are the common case.
#[test]
fn a_process_wide_loop_reports_no_shard() {
    let liveness = ScannerLiveness::new();
    let t0 = std::time::Instant::now();
    let owner = liveness.register_at(Scanner::Retention, Duration::from_secs(30), t0);
    liveness.tick_at(owner, t0);

    let status = &liveness.snapshot_as_of(t0)[0];
    assert_eq!(
        status.shard, None,
        "a loop that is not per-shard must report no shard"
    );
}

/// Every folded `ScannerStatus` must be internally consistent: re-classifying
/// it reproduces the worst per-instance verdict. This is what lets the
/// preflight check call `classify_scanner` on the folded status and trust the
/// answer.
#[test]
fn a_folded_status_reclassifies_to_the_worst_per_instance_verdict() {
    let liveness = ScannerLiveness::new();
    let t0 = std::time::Instant::now();

    // Healthy (1s cadence, just ticked), Stale (30s => 60s threshold, 90s
    // silent), Wedged (30s => 60s threshold, 200s silent).
    let healthy = liveness.register_at(Scanner::Timeout, Duration::from_secs(1), t0);
    let stale = liveness.register_at(Scanner::Timeout, Duration::from_secs(30), t0);
    let wedged = liveness.register_at(Scanner::Timeout, Duration::from_secs(30), t0);
    let now = t0 + Duration::from_secs(200);
    liveness.tick_at(healthy, now);
    liveness.tick_at(stale, t0 + Duration::from_secs(110));
    liveness.tick_at(wedged, t0);

    let status = &liveness.snapshot_as_of(now)[0];
    assert_eq!(classify_scanner(status), ScannerLivenessVerdict::Wedged);
    assert_eq!(
        status.tick_count, 3,
        "tick_count still sums across every instance, matching the counter"
    );
    // Self-consistency: the reported (age, poll_interval) pair belongs to one
    // real instance, so the classifier's inputs are never mixed.
    assert_eq!(status.age, Duration::from_secs(200));
    assert_eq!(status.poll_interval, Duration::from_secs(30));
}

#[test]
fn the_threshold_narrows_again_when_the_slow_instance_stops() {
    // The widened interval must not be sticky: once the slow poller retires,
    // the surviving fast one is judged on its own (floored) threshold rather
    // than inheriting a permanently lenient one.
    let liveness = ScannerLiveness::new();
    let t0 = std::time::Instant::now();
    let fast = liveness.register_at(Scanner::Timeout, Duration::from_secs(1), t0);
    let slow = liveness.register_at(Scanner::Timeout, Duration::from_secs(300), t0);
    liveness.tick_at(fast, t0);
    liveness.tick_at(slow, t0);

    liveness.deregister(slow);
    let status = &liveness.snapshot_as_of(t0)[0];
    assert_eq!(status.poll_interval, Duration::from_secs(1));
    assert_eq!(
        staleness_threshold(status.poll_interval),
        MIN_SCANNER_STALENESS_THRESHOLD
    );
}

// ---------------------------------------------------------------------------
// Snapshot shape
// ---------------------------------------------------------------------------

#[test]
fn snapshot_only_reports_registered_scanners() {
    // An API-only replica runs no control loops; it must report an EMPTY
    // snapshot rather than seven phantom wedged scanners.
    let liveness = ScannerLiveness::new();
    assert!(liveness.snapshot().is_empty());

    let _owner = liveness.register(Scanner::Timeout, Duration::from_secs(30));
    let snapshot = liveness.snapshot();
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].scanner, Scanner::Timeout);
}

#[test]
fn snapshot_is_ordered_deterministically() {
    // Stable output so the preflight `details` payload does not churn between
    // otherwise-identical calls.
    let liveness = ScannerLiveness::new();
    let _a = liveness.register(Scanner::Retention, Duration::from_secs(30));
    let _b = liveness.register(Scanner::Timeout, Duration::from_secs(30));
    let _c = liveness.register(Scanner::Schedule, Duration::from_secs(30));

    let first: Vec<Scanner> = liveness.snapshot().iter().map(|s| s.scanner).collect();
    let second: Vec<Scanner> = liveness.snapshot().iter().map(|s| s.scanner).collect();
    assert_eq!(first, second);
    let mut sorted = first.clone();
    sorted.sort_unstable();
    assert_eq!(first, sorted, "snapshot must be ordered by scanner");
}

#[test]
fn repeated_ticks_advance_the_count_and_reset_the_age() {
    let liveness = ScannerLiveness::new();
    let t0 = std::time::Instant::now();
    let owner = liveness.register_at(Scanner::PoisonPill, Duration::from_secs(30), t0);
    liveness.tick_at(owner, t0 + Duration::from_secs(30));
    liveness.tick_at(owner, t0 + Duration::from_secs(60));

    let status = &liveness.snapshot_as_of(t0 + Duration::from_secs(70))[0];
    assert_eq!(status.tick_count, 2);
    assert_eq!(status.age, Duration::from_secs(10));
    assert!(
        status.last_tick_at.is_some(),
        "wall-clock stamp for display"
    );
}

// ---------------------------------------------------------------------------
// Graceful shutdown must not leave a phantom stale scanner behind
// ---------------------------------------------------------------------------

#[test]
fn deregistering_the_last_instance_removes_the_scanner() {
    // A worker that drains cleanly is no longer running its loops. Leaving
    // the entry behind would report `fail` on a process that deliberately
    // stopped its worker while keeping the API up.
    let liveness = ScannerLiveness::new();
    let owner = liveness.register(Scanner::Timeout, Duration::from_secs(30));
    liveness.tick(owner);
    assert_eq!(liveness.snapshot().len(), 1);

    liveness.deregister(owner);
    assert!(
        liveness.snapshot().is_empty(),
        "a cleanly stopped loop must not linger as a phantom stale scanner"
    );
}

#[test]
fn deregistering_one_of_two_runtimes_keeps_the_scanner_alive() {
    // Two runtimes in one process share the registry. One stopping must not
    // blind the health check to the other, which is still running the loop.
    let liveness = ScannerLiveness::new();
    let first = liveness.register(Scanner::Retention, Duration::from_secs(30));
    let second = liveness.register(Scanner::Retention, Duration::from_secs(30));

    liveness.deregister(first);
    assert_eq!(
        liveness.snapshot().len(),
        1,
        "the surviving runtime's loop must still be reported"
    );

    liveness.deregister(second);
    assert!(liveness.snapshot().is_empty());
}

#[test]
fn deregistering_the_same_instance_twice_is_a_noop() {
    // Defensive: an unbalanced deregister (a shutdown path that runs twice)
    // must not panic, and must not retire a live peer.
    let liveness = ScannerLiveness::new();
    let doomed = liveness.register(Scanner::Schedule, Duration::from_secs(30));
    let survivor = liveness.register(Scanner::Schedule, Duration::from_secs(30));

    liveness.deregister(doomed);
    liveness.deregister(doomed);
    assert_eq!(
        liveness.registrations(Scanner::Schedule),
        1,
        "a repeated deregister must not take the surviving instance with it"
    );

    liveness.deregister(survivor);
    assert!(liveness.snapshot().is_empty());
}

#[test]
fn registrations_counts_live_instances() {
    let liveness = ScannerLiveness::new();
    assert_eq!(liveness.registrations(Scanner::Retention), 0);

    let first = liveness.register(Scanner::Retention, Duration::from_secs(30));
    let second = liveness.register(Scanner::Retention, Duration::from_secs(30));
    assert_eq!(liveness.registrations(Scanner::Retention), 2);

    liveness.deregister(first);
    assert_eq!(liveness.registrations(Scanner::Retention), 1);

    liveness.deregister(second);
    assert_eq!(liveness.registrations(Scanner::Retention), 0);
    assert!(
        liveness.snapshot().is_empty(),
        "the entry is dropped once the last instance deregisters"
    );
}

// ---------------------------------------------------------------------------
// AC1 + AC4 share one choke point
// ---------------------------------------------------------------------------

#[test]
fn record_scanner_tick_bumps_liveness_even_with_a_noop_recorder() {
    // The health check must not silently depend on telemetry being wired:
    // liveness is recorded unconditionally, the counter only when a recorder
    // is configured.
    let liveness = ScannerLiveness::new();
    let owner = liveness.register(Scanner::ExternalOutbox, Duration::from_secs(30));
    autumn_harvest::scanner_health::record_scanner_tick_on(
        &liveness,
        &autumn_harvest::telemetry::NoOpMetrics,
        owner,
    );

    let snapshot = liveness.snapshot();
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].scanner, Scanner::ExternalOutbox);
    assert_eq!(snapshot[0].tick_count, 1);
}

#[test]
fn record_scanner_tick_emits_the_counter_and_the_timestamp_together() {
    // One choke point drives both AC1 (counter) and AC4 (timestamp), so the
    // two can never drift apart at a call site.
    #[derive(Default)]
    struct Capture(std::sync::Mutex<Vec<String>>);
    impl MetricsRecorder for Capture {
        fn record_scanner_tick(&self, scanner: &str, shard: &str) {
            let _ = shard;
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(scanner.to_owned());
        }
    }

    let liveness = ScannerLiveness::new();
    let capture = Capture::default();
    let owner = liveness.register(Scanner::PauseAutoResume, Duration::from_secs(30));
    autumn_harvest::scanner_health::record_scanner_tick_on(&liveness, &capture, owner);

    assert_eq!(
        capture
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_slice(),
        ["pause_auto_resume"]
    );
    assert_eq!(liveness.snapshot()[0].tick_count, 1);
}

// ---------------------------------------------------------------------------
// Series initialization: a first-iteration wedge must still be visible
// ---------------------------------------------------------------------------

/// Records `register`/`tick` events in order so a test can assert both *that*
/// the series is initialized and *when* relative to the first tick.
#[derive(Default)]
struct RegisterCapture(std::sync::Mutex<Vec<String>>);

impl MetricsRecorder for RegisterCapture {
    fn record_scanner_registered(&self, scanner: &str, shard: &str) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(format!("register:{scanner}@{shard}"));
    }

    fn record_scanner_tick(&self, scanner: &str, shard: &str) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(format!("tick:{scanner}@{shard}"));
    }
}

impl RegisterCapture {
    fn events(&self) -> Vec<String> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

#[test]
fn registration_initializes_the_series_before_the_first_iteration() {
    // The shipped alert is `rate(harvest_scanner_tick_total{...}[5m]) == 0`, and
    // PromQL comparisons only evaluate series that *exist*. A loop that panics
    // or hangs on its very first iteration never reaches `record_scanner_tick`,
    // so without an explicit registration sample no series is ever created and
    // the paging alert stays silent forever — the exact failure the alert is
    // meant to catch. Registration therefore emits its own sample, at spawn
    // time, before the loop body runs.
    let liveness = ScannerLiveness::new();
    let capture = RegisterCapture::default();
    let owner = autumn_harvest::scanner_health::register_scanner_on(
        &liveness,
        &capture,
        Scanner::Retention,
        Duration::from_secs(3600),
        None,
    );

    // A wedge on iteration one: registered, never ticked.
    assert_eq!(capture.events(), ["register:retention@none"]);
    let status = &liveness.snapshot()[0];
    assert!(
        !status.has_ticked,
        "a loop wedged on its first iteration has registered but never ticked"
    );
    assert_eq!(status.tick_count, 0);

    // And once it does run, the tick follows the registration.
    autumn_harvest::scanner_health::record_scanner_tick_on(&liveness, &capture, owner);
    assert_eq!(
        capture.events(),
        ["register:retention@none", "tick:retention@none"]
    );
}

#[test]
fn every_scanner_variant_initializes_its_own_series() {
    // The series is keyed by the bounded `scanner` label, so a first-iteration
    // wedge must be visible for *every* loop, not just the one that happens to
    // be covered by a test.
    let liveness = ScannerLiveness::new();
    let capture = RegisterCapture::default();
    for scanner in Scanner::ALL {
        let _owner = autumn_harvest::scanner_health::register_scanner_on(
            &liveness,
            &capture,
            scanner,
            Duration::from_secs(30),
            None,
        );
    }

    let expected: Vec<String> = Scanner::ALL
        .iter()
        .map(|s| format!("register:{}@none", s.as_str()))
        .collect();
    assert_eq!(capture.events(), expected);
}

#[test]
fn record_scanner_registered_has_a_noop_default() {
    // Additive, no breaking change: a recorder that predates this method still
    // compiles and is simply silent, exactly like `record_scanner_tick`.
    struct Silent;
    impl MetricsRecorder for Silent {}

    let liveness = ScannerLiveness::new();
    let owner = autumn_harvest::scanner_health::register_scanner_on(
        &liveness,
        &Silent,
        Scanner::Timeout,
        Duration::from_secs(30),
        None,
    );

    // Liveness is recorded regardless of whether telemetry is wired.
    assert_eq!(liveness.snapshot()[0].scanner, Scanner::Timeout);
    liveness.deregister(owner);
    assert!(liveness.snapshot().is_empty());
}

// ---------------------------------------------------------------------------
// Wiring guard: every spawned loop registers, ticks, and deregisters
// ---------------------------------------------------------------------------

/// Anti-drift source scan over every spawn site.
///
/// Two of the loops (`Scheduler::spawn_sharded`'s ticker and
/// `spawn_pause_auto_resumer`) are not directly constructible from a test —
/// the first needs a full sharded runtime, the second is private — and a third
/// (`RetentionRuntime::spawn`) needs a live pool. Rather than leave them
/// unguarded, this scans the source for the register / tick / deregister triple
/// at each site, mirroring the established source-extraction precedent in
/// `dashboard_pack_docs.rs`.
///
/// It catches the specific drift this feature is vulnerable to: a loop wired
/// to *tick* but not to *register* would never be part of the expected set (so
/// a wedge would go unreported), and one wired to register but never
/// deregister would report a phantom `wedged` scanner after a clean drain.
/// It is a coarse guard by design — it proves the triple is present in the
/// owning file, not that it is correctly sequenced; the behavioural tests in
/// `scanner_tick_db_tests.rs` cover sequencing for the two loops a test can
/// drive directly.
#[test]
fn every_spawned_loop_registers_ticks_and_deregisters() {
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    // Which file owns which scanner's loop. `sla` and `external_outbox` are
    // ticked by the timeout loop that drives them, so they live in timeout.rs.
    let owners = [
        ("timeout.rs", &["Timeout", "Sla", "ExternalOutbox"][..]),
        ("poison_pill.rs", &["PoisonPill"][..]),
        ("retention.rs", &["Retention"][..]),
        ("scheduler.rs", &["Schedule"][..]),
        ("worker.rs", &["PauseAutoResume"][..]),
        ("audit_export.rs", &["AuditExport"][..]),
    ];

    // Every `Scanner` variant must be owned by exactly one file, so a newly
    // added variant cannot be silently left unwired.
    let claimed: Vec<&str> = owners
        .iter()
        .flat_map(|(_, vs)| vs.iter().copied())
        .collect();
    assert_eq!(
        claimed.len(),
        Scanner::ALL.len(),
        "every Scanner variant must be claimed by exactly one spawn site; \
         claimed {claimed:?}"
    );

    for (file, variants) in owners {
        let text = std::fs::read_to_string(src.join(file))
            .unwrap_or_else(|e| panic!("{file} must be readable: {e}"));
        for variant in variants {
            assert!(
                text.contains(&format!("Scanner::{variant}")),
                "{file} owns Scanner::{variant}'s loop, so it must name that variant"
            );
        }
        assert!(
            text.contains("register_scanner("),
            "{file} must register its scanner(s) at spawn time, before the \
             loop's first iteration — a loop that ticks without registering is \
             never part of the expected set, so a wedge would go unreported"
        );
        assert!(
            text.contains("record_scanner_tick("),
            "{file} registers a scanner, so it must also tick it every iteration"
        );
        assert!(
            text.contains("deregister_scanner("),
            "{file} registers a scanner, so a graceful stop must release it — \
             otherwise a cleanly drained worker reports a phantom wedged scanner"
        );
    }

    // The pure enforcement primitive must stay decoupled: it is `pub` and
    // driven by ~40 tests and by embedders, so a tick there would fabricate a
    // scanner the process is not running (which would then age into `Wedged`).
    let timeout_src = std::fs::read_to_string(src.join("timeout.rs")).expect("timeout.rs");
    let enforce = timeout_src
        .split("pub async fn enforce_timeouts_once")
        .nth(1)
        .and_then(|rest| rest.split("\npub ").next())
        .expect("enforce_timeouts_once must exist");
    assert!(
        !enforce.contains("record_scanner_tick("),
        "enforce_timeouts_once must not tick: it is a `pub` primitive driven by \
         hand, and a tick there would register a phantom scanner"
    );
}
