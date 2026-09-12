//! End-to-end proof that background control loops tick even with **no work
//! to do** (issue #797, AC3).
//!
//! The pure tests in `scanner_liveness_tests.rs` cover the registry and the
//! staleness policy. This file covers the property that actually
//! distinguishes issue #797 from the metrics that came before it: a scanner
//! pass that finds *nothing* still emits `harvest.scanner.tick` and still
//! advances its liveness timestamp. Every pre-existing loop metric
//! (`harvest.retention.deleted`, `harvest.schedule.fire_attempts`, the
//! timeout/SLA/quarantine counters) emits only when there is work, which is
//! exactly why a wedged loop was previously indistinguishable from an idle
//! one.
//!
//! Requires a database, because the no-work path runs the real
//! `enforce_timeouts_once` sub-passes against a real (empty) schema.
#![cfg(feature = "db")]

use std::sync::Mutex;
use std::time::Duration;

use autumn_harvest::scanner_health::{Scanner, ScannerLiveness, global_scanner_liveness};
use autumn_harvest::telemetry::MetricsRecorder;
use autumn_harvest::types::ShardId;
use autumn_harvest::worker::DbPool;
use autumn_harvest::{ShardedDbPool, timeout};
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

fn init_sql() -> Vec<u8> {
    autumn_harvest::test_init_sql().as_bytes().to_vec()
}

/// Prefer an operator-supplied database (a local Postgres) and fall back to
/// testcontainers, mirroring `retention_overrides_tests.rs`.
async fn setup_test_db_url() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        use diesel_async::{AsyncConnection, SimpleAsyncConnection};
        let mut conn = AsyncPgConnection::establish(&url)
            .await
            .expect("HARVEST_TEST_DATABASE_URL must be reachable");
        // Idempotent: several tests in this file share one operator-supplied
        // database, and re-applying the migration bundle would fail on the
        // second test with "relation already exists". A failed probe poisons
        // the connection, so the apply runs on a fresh one.
        let already_migrated = conn
            .batch_execute("SELECT 1 FROM harvest_workflow_executions LIMIT 0")
            .await
            .is_ok();
        if !already_migrated {
            let mut fresh = AsyncPgConnection::establish(&url)
                .await
                .expect("HARVEST_TEST_DATABASE_URL must be reachable");
            fresh
                .batch_execute(&autumn_harvest::test_init_sql())
                .await
                .expect("migrations should apply");
        }
        return (url, None);
    }

    let container = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("container port");
    (
        format!("postgres://postgres:postgres@{host}:{port}/postgres"),
        Some(container),
    )
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    DbPool::builder(manager)
        .max_size(4)
        .build()
        .expect("failed to build pool")
}

/// The scanners `spawn_timeout_checker` owns: it drives the SLA and external
/// outbox passes as part of its own iteration, so all three share its liveness.
const OWNED: [Scanner; 3] = [Scanner::Timeout, Scanner::Sla, Scanner::ExternalOutbox];

/// Serializes every test in this file that spawns a **real** loop against the
/// **process-global** scanner registry.
///
/// These tests assert on registration-count *deltas* (`before` vs `after`)
/// around a spawn/drain, which is only sound if no sibling test registers or
/// deregisters the same `Scanner` in between. CI runs this suite with
/// `--test-threads=1`, but that is a property of the runner invocation, not of
/// the tests: a developer running `cargo test ... scanner_tick_db_tests`
/// locally gets the default parallel harness and would see genuine flakes.
/// Correctness should not depend on a CLI flag, so the guard lives here.
///
/// Mirrors the `TEST_SERIAL` precedent in `completion_callback_tests.rs`,
/// which guards the same class of process-global state.
static TEST_SERIAL: std::sync::LazyLock<tokio::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

/// Records every `harvest.scanner.tick` label it sees.
#[derive(Default)]
struct TickRecorder {
    ticks: Mutex<Vec<String>>,
}

impl TickRecorder {
    fn ticks(&self) -> Vec<String> {
        self.ticks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl MetricsRecorder for TickRecorder {
    fn record_scanner_tick(&self, scanner: &str, shard: &str) {
        let _ = shard;
        self.ticks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(scanner.to_owned());
    }
}

/// AC3, the headline property, through a **real spawned loop**: a pass over a
/// completely empty database — zero timed-out tasks, zero SLA breaches, zero
/// outbox rows — still emits a tick for every scanner the loop owns.
#[tokio::test]
async fn spawned_timeout_checker_ticks_all_owned_scanners_with_no_work() {
    // Serialize against sibling tests that touch the process-global registry;
    // the delta assertions below are only sound with exclusive access.
    let _serial = TEST_SERIAL.lock().await;
    let (url, _container) = setup_test_db_url().await;
    let pool = build_pool(&url);
    let recorder = std::sync::Arc::new(TickRecorder::default());
    let telemetry = std::sync::Arc::new(autumn_harvest::telemetry::TelemetryConfig {
        metrics: recorder.clone(),
        ..Default::default()
    });
    let cancel = tokio_util::sync::CancellationToken::new();

    let handle = timeout::spawn_timeout_checker_for_shard(
        pool.clone(),
        cancel.clone(),
        Duration::from_millis(50),
        telemetry,
        Duration::from_secs(5),
        None,
        vec![ShardId::new(0)],
        std::sync::Arc::new(autumn_harvest::circuit_breaker::CircuitBreakerRegistry::default()),
        None,
        60,
        Some(ShardId::new(0)),
        autumn_harvest::payload_codec::PayloadCodecs::default(),
        0,
    );

    // Poll (bounded) rather than sleeping a fixed span: fast when the loop is
    // healthy, and a clear timeout rather than a flake when it is not.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let ticks = recorder.ticks();
        if OWNED
            .iter()
            .all(|s| ticks.iter().filter(|t| *t == s.as_str()).count() >= 2)
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the loop must tick every owned scanner on every iteration even with \
             no work present; got {:?}",
            recorder.ticks()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // AC4: a ticking loop classifies as healthy. Sampled while it is still
    // running, because a graceful stop retires it from the expected set.
    for scanner in OWNED {
        let status = global_scanner_liveness()
            .snapshot()
            .into_iter()
            .find(|s| s.scanner == scanner)
            .unwrap_or_else(|| panic!("{scanner:?} must be registered by the running loop"));
        assert!(status.has_ticked, "{scanner:?} must have ticked");
        assert_eq!(
            autumn_harvest::scanner_health::classify_scanner(&status),
            autumn_harvest::scanner_health::ScannerLivenessVerdict::Healthy,
            "{scanner:?} must be healthy while its loop is ticking"
        );
    }

    let held_before: Vec<usize> = OWNED
        .iter()
        .map(|s| global_scanner_liveness().registrations(*s))
        .collect();
    assert!(
        held_before.iter().all(|n| *n >= 1),
        "the running loop must hold a registration for each scanner it owns; got {held_before:?}"
    );

    cancel.cancel();
    let _ = handle.await;

    // A *graceful* stop releases this loop's own registration for every scanner
    // it owns, so draining a worker while keeping the API up does not leave
    // phantom scanners aging into `Wedged`. Asserted by refcount delta rather
    // than by absence, because other tests in this binary may hold their own
    // concurrent registrations.
    for (scanner, before) in OWNED.iter().zip(&held_before) {
        assert_eq!(
            global_scanner_liveness().registrations(*scanner),
            before - 1,
            "{scanner:?} must be released on graceful shutdown"
        );
    }
}

/// The pure enforcement primitive is **decoupled** from liveness.
///
/// `enforce_timeouts_once` is `pub` and driven directly by ~40 tests and by
/// embedders. If it recorded ticks, a hand-driven call would fabricate a
/// scanner the process is not actually running — which would then age into
/// `Wedged` and report a phantom outage. Liveness is owned by the loop that
/// spawns it, never by the pass it drives.
#[tokio::test]
async fn enforce_timeouts_once_records_no_scanner_ticks() {
    // Serialize against sibling tests that touch the process-global registry;
    // the delta assertions below are only sound with exclusive access.
    let _serial = TEST_SERIAL.lock().await;
    let (url, _container) = setup_test_db_url().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.expect("connection");

    let recorder = TickRecorder::default();
    let sharded_pool = Option::<ShardedDbPool>::None;

    let before: Vec<usize> = OWNED
        .iter()
        .map(|s| global_scanner_liveness().registrations(*s))
        .collect();

    let enforced = timeout::enforce_timeouts_once(
        &mut conn,
        &recorder,
        Duration::from_secs(5),
        &sharded_pool,
        &[ShardId::new(0)],
        None,
        None,
        60,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
        0,
    )
    .await
    .expect("a no-work enforcement pass must succeed");

    assert_eq!(
        enforced, 0,
        "precondition: this pass must genuinely have found no work"
    );
    assert!(
        recorder.ticks().is_empty(),
        "the pass itself must emit no liveness ticks; got {:?}",
        recorder.ticks()
    );
    let after: Vec<usize> = OWNED
        .iter()
        .map(|s| global_scanner_liveness().registrations(*s))
        .collect();
    assert_eq!(
        before, after,
        "a hand-driven pass must not register a phantom scanner"
    );
}

/// The choke point advances the liveness registry even with **no metrics
/// recorder configured**, so `GET /admin/preflight`'s `scanner_liveness` check
/// works on a deployment that never wired telemetry.
#[tokio::test]
async fn the_loop_advances_liveness_without_a_metrics_recorder() {
    // Serialize against sibling tests that touch the process-global registry;
    // the delta assertions below are only sound with exclusive access.
    let _serial = TEST_SERIAL.lock().await;
    let (url, _container) = setup_test_db_url().await;
    let pool = build_pool(&url);
    let cancel = tokio_util::sync::CancellationToken::new();

    let handle = timeout::spawn_timeout_checker_for_shard(
        pool.clone(),
        cancel.clone(),
        Duration::from_millis(50),
        std::sync::Arc::new(autumn_harvest::telemetry::TelemetryConfig::default()),
        Duration::from_secs(5),
        None,
        vec![ShardId::new(0)],
        std::sync::Arc::new(autumn_harvest::circuit_breaker::CircuitBreakerRegistry::default()),
        None,
        60,
        Some(ShardId::new(0)),
        autumn_harvest::payload_codec::PayloadCodecs::default(),
        0,
    );

    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let ticked = loop {
        let ticked = global_scanner_liveness()
            .snapshot()
            .into_iter()
            .find(|s| s.scanner == Scanner::Sla)
            .is_some_and(|s| s.tick_count > 0);
        if ticked {
            break true;
        }
        if std::time::Instant::now() >= deadline {
            break false;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    };

    cancel.cancel();
    let _ = handle.await;

    assert!(
        ticked,
        "liveness must advance with the default (no-op) metrics recorder"
    );
}

/// Wiring guard for a second, independently spawned loop (issue #797 AC1/AC4).
///
/// The timeout checker is covered above; this proves the register / tick /
/// deregister triple is not unique to it. `spawn_poison_pill_reclaimer` is the
/// cheapest of the remaining loops to drive directly.
#[tokio::test]
async fn spawned_poison_pill_reclaimer_registers_ticks_and_deregisters() {
    // Serialize against sibling tests that touch the process-global registry;
    // the delta assertions below are only sound with exclusive access.
    let _serial = TEST_SERIAL.lock().await;
    let (url, _container) = setup_test_db_url().await;
    let pool = build_pool(&url);
    let recorder = std::sync::Arc::new(TickRecorder::default());
    let telemetry = std::sync::Arc::new(autumn_harvest::telemetry::TelemetryConfig {
        metrics: recorder.clone(),
        ..Default::default()
    });
    let cancel = tokio_util::sync::CancellationToken::new();

    let before = global_scanner_liveness().registrations(Scanner::PoisonPill);
    let handle = autumn_harvest::poison_pill::spawn_poison_pill_reclaimer_for_shard(
        pool.clone(),
        cancel.clone(),
        Duration::from_millis(50),
        3,
        60,
        None,
        telemetry,
        Some(ShardId::new(0)),
        autumn_harvest::payload_codec::PayloadCodecs::default(),
    );
    assert_eq!(
        global_scanner_liveness().registrations(Scanner::PoisonPill),
        before + 1,
        "the loop must register itself at spawn time, before its first iteration"
    );
    // Issue #797, Codex review: the shard the spawner was given must survive
    // into the snapshot, so `scanner_liveness` can name the unprotected
    // database on a multi-shard worker. Asserted through a REAL spawner rather
    // than a hand-built registry, so the plumbing is covered end to end.
    assert!(
        global_scanner_liveness()
            .snapshot()
            .iter()
            .any(|status| status.scanner == Scanner::PoisonPill
                && status.shard == Some(ShardId::new(0))),
        "the spawner's shard must reach the liveness snapshot"
    );

    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        if recorder
            .ticks()
            .iter()
            .filter(|t| *t == "poison_pill")
            .count()
            >= 2
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the reclaimer must tick on every iteration with no orphans present; got {:?}",
            recorder.ticks()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    cancel.cancel();
    let _ = handle.await;
    assert_eq!(
        global_scanner_liveness().registrations(Scanner::PoisonPill),
        before,
        "a graceful stop must release the registration"
    );
}

/// A wedged loop is detectable within `2 x` its poll interval — the issue's
/// success metric, expressed against the real classifier rather than a
/// hand-computed threshold.
#[test]
fn a_wedged_loop_is_detected_within_two_poll_intervals_of_its_threshold() {
    use autumn_harvest::scanner_health::{
        ScannerLivenessVerdict, classify_scanner, staleness_threshold,
    };

    // A 30s loop: threshold 60s == 2 x poll interval.
    let poll_interval = Duration::from_secs(30);
    assert_eq!(
        staleness_threshold(poll_interval),
        poll_interval * 2,
        "for a 30s loop the threshold IS 2x the poll interval, so detection \
         latency is bounded by the issue's success metric"
    );

    let liveness = ScannerLiveness::new();
    let t0 = std::time::Instant::now();
    let owner = liveness.register_at(Scanner::Timeout, poll_interval, t0);
    liveness.tick_at(owner, t0);

    // The loop wedges immediately after this tick. 60s later it is flagged.
    let status = &liveness.snapshot_as_of(t0 + poll_interval * 2)[0];
    assert_ne!(
        classify_scanner(status),
        ScannerLivenessVerdict::Healthy,
        "a loop silent for 2x its poll interval must already be flagged"
    );
}

/// The pre-#797 public spawn signatures still exist, unchanged.
///
/// `timeout` and `poison_pill` are `pub mod`s, so `spawn_timeout_checker` and
/// `spawn_poison_pill_reclaimer` are reachable public API that an embedder
/// running its own worker loop may call directly. Issue #797 needed a shard to
/// label the loop in the `scanner_liveness` check, but adding it as a mandatory
/// argument would have broken every such caller for an observability feature
/// they never asked for. The shard-aware variants are additive siblings
/// (`*_for_shard`) and these two keep their original argument lists.
///
/// This is a **compile-time** assertion: coercing each function to a fn pointer
/// of its exact expected type fails to build if any argument is added, removed,
/// or reordered. It needs no database to be meaningful -- it is checked by
/// `cargo clippy -p autumn-harvest --all-features --tests`, which CI runs on
/// every push.
#[test]
fn the_public_spawn_signatures_are_unchanged_by_shard_attribution() {
    type TimeoutCheckerFn = fn(
        diesel_async::pooled_connection::deadpool::Pool<diesel_async::AsyncPgConnection>,
        tokio_util::sync::CancellationToken,
        Duration,
        std::sync::Arc<autumn_harvest::telemetry::TelemetryConfig>,
        Duration,
        Option<autumn_harvest::shard::ShardedDbPool>,
        Vec<ShardId>,
        std::sync::Arc<autumn_harvest::circuit_breaker::CircuitBreakerRegistry>,
        Option<u64>,
        i64,
    ) -> tokio::task::JoinHandle<()>;

    type PoisonPillReclaimerFn = fn(
        diesel_async::pooled_connection::deadpool::Pool<diesel_async::AsyncPgConnection>,
        tokio_util::sync::CancellationToken,
        Duration,
        i32,
        i64,
        std::sync::Arc<autumn_harvest::telemetry::TelemetryConfig>,
        autumn_harvest::payload_codec::PayloadCodecs,
    ) -> tokio::task::JoinHandle<()>;

    let _: TimeoutCheckerFn = timeout::spawn_timeout_checker;
    let _: PoisonPillReclaimerFn = autumn_harvest::poison_pill::spawn_poison_pill_reclaimer;
}
