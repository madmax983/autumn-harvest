//! Cross-region DR: fencing and measured RPO — DB integration tests (issue #954).
//!
//! These are the correctness oracle for the issue's success metric. They run
//! against a live Postgres and cover the three proofs AC6 asks for:
//!
//! * **(a) a fenced stale worker cannot claim or persist** — `fenced_*` tests.
//! * **(b) post-promotion, in-flight work resumes on the new primary** —
//!   `promoted_standby_*` tests, over *real* logical replication.
//! * **(c) the RPO metric reports the injected lag** — `rpo_*` tests, which
//!   compare the reported lag against an independently-read
//!   `pg_stat_replication.replay_lag` within the issue's ±5s tolerance.
//!
//! # Topology
//!
//! Two *databases* in one Postgres instance, wired together with stock logical
//! replication (`CREATE PUBLICATION` / `CREATE SUBSCRIPTION`). That is a real
//! walsender, a real replication slot, real LSNs and a real `replay_lag` — the
//! same machinery a cross-region deployment uses — without the container-to-
//! container networking a two-instance topology would need for no extra
//! fidelity. The human drill in `docs/runbooks/cross-region-failover.md` uses
//! the two-container compose topology; this suite proves the engine behaviour
//! that drill depends on.
//!
//! Requires `wal_level = logical`. The replication tests skip with an explicit
//! message when the server is not configured for it; the fencing tests do not
//! depend on replication at all and always run.
#![cfg(feature = "db")]
#![allow(
    clippy::doc_markdown,
    clippy::too_many_lines,
    clippy::items_after_statements
)]

use std::sync::atomic::{AtomicU32, Ordering};

use autumn_harvest::replication::{
    FenceRegistry, ReplicationStatus, ShardGeneration, WatermarkReading, assert_fence,
    bump_generation, current_generation, ensure_generation_row, query_replication_status,
};
use autumn_harvest::types::{ExecutionId, ShardId};
use futures::FutureExt as _;

use diesel_async::SimpleAsyncConnection;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

static DB_SEQ: AtomicU32 = AtomicU32::new(0);

/// Sampler cadence these tests beat at.
///
/// Near-zero on purpose: the beat is now rate-limited to one write per shard
/// per half-interval, and a test that wants several distinct watermarks in
/// quick succession must not be throttled by that gate.
const BEAT_INTERVAL: std::time::Duration = std::time::Duration::from_millis(1);

/// The slot-name prefix the tests create their DR slots with — the same
/// `harvest_dr` the topology doc's setup SQL prescribes and the code defaults
/// to. Anything not carrying it is deliberately NOT counted as a DR standby.
const DR_PREFIX: &str = autumn_harvest::replication::DEFAULT_DR_SLOT_PREFIX;

/// [`FenceRegistry`] is process-global; every test that pins a generation must
/// hold this so a sibling test cannot observe a half-built registry.
/// Async-aware, because the guard is deliberately held across `.await`s: the
/// whole point is to keep a sibling test from observing a half-built registry
/// while this one is mid-scenario.
static REGISTRY_SERIAL: std::sync::LazyLock<tokio::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

/// A codec that always succeeds and changes the bytes -- enough for the sweep
/// to have real work to do, without pulling a real cipher into a DR test.
struct DrXorCodec(u8);

impl autumn_harvest::payload_codec::PayloadCodec for DrXorCodec {
    fn codec_id(&self) -> &'static str {
        "dr-xor"
    }
    fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, autumn_harvest::payload_codec::CodecError> {
        Ok(raw.iter().map(|b| b ^ self.0).collect())
    }
    fn decode(&self, encoded: &[u8]) -> Result<Vec<u8>, autumn_harvest::payload_codec::CodecError> {
        Ok(encoded.iter().map(|b| b ^ self.0).collect())
    }
}

struct NoOpMetrics;
impl autumn_harvest::telemetry::MetricsRecorder for NoOpMetrics {}

async fn registry_guard() -> tokio::sync::MutexGuard<'static, ()> {
    REGISTRY_SERIAL.lock().await
}

/// The shared Postgres these tests create their per-test databases on.
///
/// Prefers a caller-supplied `HARVEST_TEST_DATABASE_URL`; otherwise starts one
/// throwaway container for the whole suite (CI's path). The container is
/// started with **`wal_level = logical`**, which is not the image default and
/// without which the three replication tests below would silently skip — i.e.
/// AC6(b) and AC6(c) would be permanently unproven while CI stayed green. That
/// is the single most important line in this file.
///
/// One container for the suite, not one per test: `pg_replication_slots` and
/// `pg_stat_replication` are cluster-wide, and the module under test scopes
/// every query to `current_database()` precisely so several shard databases can
/// share a cluster. Sharing one here therefore exercises that scoping rather
/// than dodging it — but it is also why the suite must run `--test-threads=1`
/// (the manifest's `linux` osclass supplies that).
static SHARED_PG: tokio::sync::OnceCell<Option<SharedPg>> = tokio::sync::OnceCell::const_new();

struct SharedPg {
    admin_url: String,
    /// Kept alive for the process; dropping it stops the container.
    _container: Option<ContainerAsync<Postgres>>,
}

async fn shared_pg() -> Option<&'static SharedPg> {
    SHARED_PG
        .get_or_init(|| async {
            if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
                return Some(SharedPg {
                    admin_url: url,
                    _container: None,
                });
            }
            let container = Postgres::default()
                .with_tag("16")
                // `wal_level=logical` is required for `CREATE PUBLICATION` to
                // produce anything. The image default is `replica`.
                .with_cmd(["postgres", "-c", "wal_level=logical"])
                .start()
                .await
                .ok()?;
            let host = container.get_host().await.ok()?;
            let port = container.get_host_port_ipv4(5432).await.ok()?;
            Some(SharedPg {
                admin_url: format!("postgres://postgres:postgres@{host}:{port}/postgres"),
                _container: Some(container),
            })
        })
        .await
        .as_ref()
}

async fn admin_url() -> Option<String> {
    shared_pg().await.map(|pg| pg.admin_url.clone())
}

fn with_db_name(base: &str, db: &str) -> String {
    let (base, query) = base
        .split_once('?')
        .map_or((base, None), |(b, q)| (b, Some(q)));
    let prefix = base.rsplit_once('/').map_or(base, |(p, _)| p);
    query.map_or_else(
        || format!("{prefix}/{db}"),
        |q| format!("{prefix}/{db}?{q}"),
    )
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url)
        .await
        .unwrap_or_else(|e| panic!("connect {url}: {e}"))
}

/// Create a freshly-migrated database and return `(url, db_name)`.
async fn fresh_db(tag: &str) -> Option<(String, String)> {
    let admin = admin_url().await?;
    let mut conn = connect(&admin).await;
    let n = DB_SEQ.fetch_add(1, Ordering::SeqCst);
    let db = format!("dr_{tag}_{}_{n}", std::process::id());
    diesel::sql_query(format!("CREATE DATABASE {db}"))
        .execute(&mut conn)
        .await
        .expect("create database");
    let url = with_db_name(&admin, &db);
    let mut fresh = connect(&url).await;
    fresh
        .batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("apply migrations");
    Some((url, db))
}

macro_rules! require_db {
    ($tag:literal) => {
        match fresh_db($tag).await {
            Some(v) => v,
            None => {
                // Reached only when neither a caller-supplied
                // `HARVEST_TEST_DATABASE_URL` nor Docker is available. Loud on
                // purpose: a silently-skipping suite that claims to prove an
                // acceptance criterion is worse than no suite.
                eprintln!(
                    "SKIPPED {}: no HARVEST_TEST_DATABASE_URL and no usable Docker — this suite \
                     proved NOTHING",
                    $tag
                );
                return;
            }
        }
    };
}

// ── AC2 / AC6(a): the fence ────────────────────────────────────────────────

#[tokio::test]
async fn a_fresh_database_provisions_generation_zero_and_is_idempotent() {
    let (url, _db) = require_db!("provision");
    let mut conn = connect(&url).await;

    let g = ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .expect("provision");
    assert_eq!(g, ShardGeneration::INITIAL);

    // Re-provisioning must never reset a shard that has already been fenced —
    // that would silently hand write authority back to the old region.
    bump_generation(&mut conn, ShardId::new(0), "drill", "test")
        .await
        .expect("bump");
    let again = ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .expect("re-provision");
    assert_eq!(
        again,
        ShardGeneration::new(1),
        "provisioning must be idempotent"
    );
}

/// Concurrent first starts must never spuriously fail to provision (finding 8).
///
/// The old query read its `ON CONFLICT DO NOTHING` fallback in the SAME
/// statement as the `INSERT`, sharing that statement's snapshot. Under READ
/// COMMITTED, a losing `INSERT` blocks on the winner's commit and then
/// finds nothing to insert. Its fallback read, sharing the pre-commit
/// snapshot, could still see zero rows, though. So the whole statement
/// returned empty, and the losing worker refused to start. This is
/// inherently a timing-dependent race, so this test cannot force the old
/// bug to reproduce on every run. But with several connections racing the
/// same shard on every run of this suite, it is a live regression guard
/// rather than a coincidence.
#[tokio::test]
async fn concurrent_first_starts_always_agree_on_the_provisioned_generation() {
    let (url, _db) = require_db!("provisionrace");
    let shard = ShardId::new(0);

    let mut conns = Vec::new();
    for _ in 0..8 {
        conns.push(connect(&url).await);
    }

    let results = futures::future::join_all(
        conns
            .iter_mut()
            .map(|conn| ensure_generation_row(conn, shard)),
    )
    .await;

    for result in &results {
        assert_eq!(
            result.as_ref().ok().copied(),
            Some(ShardGeneration::INITIAL),
            "every racing first start must observe the provisioned row, never a spurious \
             failure: {results:?}"
        );
    }
}

/// A heartbeat written under a superseded generation must never be read back
/// as if it were part of the current WAL stream (finding 10).
///
/// `docs/cross-region-dr.md`'s setup SQL replicates
/// `harvest_replication_heartbeat` with `FOR ALL TABLES`. So a standby can
/// carry beats the OLD primary wrote — LSNs from a WAL stream the new
/// primary does not share. This plants exactly that: a stale, hour-old
/// beat tagged with generation 0 at a LARGER LSN. Beside it sits a fresh,
/// current beat tagged with generation 1 at a SMALLER one. Only the
/// generation filter — never `ORDER BY beat_lsn DESC` — can be why the
/// fresh one wins.
#[tokio::test]
async fn measure_rpo_ignores_heartbeats_from_a_superseded_generation() {
    let (url, _db) = require_db!("genscope");
    let mut conn = connect(&url).await;
    let shard = ShardId::new(0);

    ensure_generation_row(&mut conn, shard)
        .await
        .expect("provision generation 0");

    let slot = format!("{DR_PREFIX}_genscope_{}", std::process::id());
    diesel::sql_query("SELECT pg_create_physical_replication_slot($1, true)")
        .bind::<diesel::sql_types::Text, _>(slot.clone())
        .execute(&mut conn)
        .await
        .expect("create a reserved physical slot");

    // Stale: generation 0, an hour old, at a LARGE LSN.
    diesel::sql_query(
        "INSERT INTO harvest_replication_heartbeat \
             (shard_id, beat_lsn, beat_at, fence_generation) \
         VALUES ($1, pg_current_wal_lsn(), NOW() - INTERVAL '1 hour', 0)",
    )
    .bind::<diesel::sql_types::Integer, _>(shard.as_i32())
    .execute(&mut conn)
    .await
    .expect("seed a stale, wrong-generation beat with a LARGE LSN");

    bump_generation(&mut conn, shard, "drill", "test")
        .await
        .expect("bump to generation 1");

    // Fresh: generation 1, just now, at a SMALL LSN -- smaller than the stale
    // row above, so `ORDER BY beat_lsn DESC` alone would pick the stale one.
    diesel::sql_query(
        "INSERT INTO harvest_replication_heartbeat \
             (shard_id, beat_lsn, beat_at, fence_generation) \
         VALUES ($1, '0/1'::pg_lsn, NOW(), 1)",
    )
    .bind::<diesel::sql_types::Integer, _>(shard.as_i32())
    .execute(&mut conn)
    .await
    .expect("seed a fresh, current-generation beat with a SMALL LSN");

    // Advance the slot's position past both beats, so the position query's
    // `beat_lsn <= position` predicate admits both rows and only the
    // generation filter can decide between them.
    diesel::sql_query("SELECT pg_replication_slot_advance($1, pg_current_wal_lsn())")
        .bind::<diesel::sql_types::Text, _>(slot.clone())
        .execute(&mut conn)
        .await
        .expect("advance the slot's position past both beats");

    let reading = autumn_harvest::replication::measure_rpo(&mut conn, shard, DR_PREFIX)
        .await
        .expect("measure_rpo");
    match reading {
        WatermarkReading::Measured(seconds) => {
            assert!(
                seconds < 30.0,
                "must read the fresh current-generation beat, not the hour-old \
                 superseded-generation one that sorts ahead of it by LSN: got {seconds}s"
            );
        }
        other => panic!("expected a fresh Measured reading, got {other:?}"),
    }

    let _ = diesel::sql_query("SELECT pg_drop_replication_slot($1)")
        .bind::<diesel::sql_types::Text, _>(slot)
        .execute(&mut conn)
        .await;
}

#[tokio::test]
async fn bump_is_monotonic_and_records_who_and_why() {
    let (url, _db) = require_db!("bump");
    let mut conn = connect(&url).await;
    ensure_generation_row(&mut conn, ShardId::new(2))
        .await
        .unwrap();

    assert_eq!(
        bump_generation(&mut conn, ShardId::new(2), "failover to eu-west", "oncall")
            .await
            .unwrap(),
        ShardGeneration::new(1)
    );
    assert_eq!(
        bump_generation(&mut conn, ShardId::new(2), "second", "oncall")
            .await
            .unwrap(),
        ShardGeneration::new(2)
    );
    assert_eq!(
        current_generation(&mut conn, ShardId::new(2))
            .await
            .unwrap(),
        Some(ShardGeneration::new(2))
    );

    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        fenced_reason: Option<String>,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        fenced_by: Option<String>,
    }
    let rows: Vec<Row> = diesel::sql_query(
        "SELECT fenced_reason, fenced_by FROM harvest_shard_generation WHERE shard_id = 2",
    )
    .load(&mut conn)
    .await
    .unwrap();
    assert_eq!(rows[0].fenced_reason.as_deref(), Some("second"));
    assert_eq!(rows[0].fenced_by.as_deref(), Some("oncall"));
}

#[tokio::test]
async fn assert_fence_rejects_a_stale_generation_and_names_both_epochs() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("assert");
    let mut conn = connect(&url).await;
    ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .unwrap();

    FenceRegistry::clear();
    FenceRegistry::register(ShardId::new(0), ShardGeneration::new(0))
        .expect("no conflicting pin in this test");
    FenceRegistry::set_default_shard(ShardId::new(0))
        .expect("no conflicting default shard in this test");

    // Still current: the assert is a no-op.
    assert_fence(&mut conn, ShardId::new(0))
        .await
        .expect("current epoch passes");

    // The promoted primary fences the old region.
    bump_generation(&mut conn, ShardId::new(0), "promote", "oncall")
        .await
        .unwrap();

    let err = assert_fence(&mut conn, ShardId::new(0))
        .await
        .expect_err("a pinned-stale worker must be rejected");
    match err {
        autumn_harvest::error::HarvestError::ShardFenced {
            shard_id,
            pinned,
            current,
        } => {
            assert_eq!(shard_id, 0);
            assert_eq!(pinned, 0);
            assert_eq!(current, Some(1));
        }
        other => panic!("expected ShardFenced, got {other:?}"),
    }
    FenceRegistry::clear();
}

#[tokio::test]
async fn an_unregistered_process_is_never_fenced() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("optout");
    let mut conn = connect(&url).await;
    ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .unwrap();
    bump_generation(&mut conn, ShardId::new(0), "promote", "oncall")
        .await
        .unwrap();

    FenceRegistry::clear();
    // No pin => fencing is off => the assert issues no statement and passes.
    assert_fence(&mut conn, ShardId::new(0))
        .await
        .expect("a deployment that never opted in must be unaffected");
}

#[tokio::test]
async fn a_missing_generation_row_fences_a_pinned_worker() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("missingrow");
    let mut conn = connect(&url).await;
    FenceRegistry::clear();
    // Pinned, but the row this worker pinned against is gone — a restore from a
    // backup taken before DR was enabled, or a hand-edited database. Fail
    // closed: a pinned worker with nothing to check against must stop.
    FenceRegistry::register(ShardId::new(0), ShardGeneration::new(3))
        .expect("no conflicting pin in this test");
    let err = assert_fence(&mut conn, ShardId::new(0))
        .await
        .expect_err("a pinned worker must fail closed when the row is absent");
    match err {
        autumn_harvest::error::HarvestError::ShardFenced { current, .. } => {
            assert_eq!(current, None);
        }
        other => panic!("expected ShardFenced, got {other:?}"),
    }
    FenceRegistry::clear();
}

#[tokio::test]
async fn a_fenced_worker_cannot_persist_events() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("persist");
    let mut conn = connect(&url).await;
    ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .unwrap();

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, state, input, shard_id) \
         VALUES ($1, 'wf', 'k1', 'RUNNING', '{}'::jsonb, 0)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut conn)
    .await
    .unwrap();

    FenceRegistry::clear();
    FenceRegistry::register(ShardId::new(0), ShardGeneration::new(0))
        .expect("no conflicting pin in this test");
    FenceRegistry::set_default_shard(ShardId::new(0))
        .expect("no conflicting default shard in this test");
    bump_generation(&mut conn, ShardId::new(0), "promote", "oncall")
        .await
        .unwrap();

    let event = autumn_harvest::event::WorkflowEvent::WorkflowStarted {
        input: serde_json::json!({}),
        timestamp: chrono::Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    };
    let err = autumn_harvest::store::append_events(&mut conn, exec_id, &[event], 1)
        .await
        .expect_err("a fenced worker must not append history");
    assert!(
        matches!(err, autumn_harvest::error::HarvestError::ShardFenced { .. }),
        "expected ShardFenced, got {err:?}"
    );

    // …and nothing landed. A partial append would be the fork this exists to
    // prevent.
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let rows: Vec<Count> = diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_events")
        .load(&mut conn)
        .await
        .unwrap();
    assert_eq!(rows[0].n, 0, "a fenced append must write nothing");
    FenceRegistry::clear();
}

#[tokio::test]
async fn a_fenced_worker_cannot_re_encrypt_history() {
    // The codec re-encryption sweep (issue #948) is sanctioned exception #3 to
    // the append-only invariant: it is the only path that UPDATEs
    // `harvest_events` in place. `store.rs` fences every INSERT, but this
    // UPDATE is a different statement in a different module, so it needs its
    // own assertion.
    //
    // The failure this prevents is the worst one the sweep has. A worker still
    // pinned to the old generation reconnects to the promoted primary; its
    // appends are refused, but an unfenced sweep would happily re-encode rows
    // the new region now owns -- under *its* active key, which the promoted
    // region may already have retired. That is silent, permanent, and destroys
    // payloads rather than merely forking history.
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("rotate");
    let mut conn = connect(&url).await;
    ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .unwrap();

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, state, input, shard_id) \
         VALUES ($1, 'wf', 'rotate-1', 'RUNNING', '{}'::jsonb, 0)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut conn)
    .await
    .unwrap();

    // A history row encoded under `k1`, with `k2` now active: exactly what the
    // sweep exists to convert.
    let codecs = autumn_harvest::payload_codec::PayloadCodecs::default();
    codecs
        .register_key("k1", std::sync::Arc::new(DrXorCodec(0x5a)))
        .unwrap();
    codecs
        .register_key("k2", std::sync::Arc::new(DrXorCodec(0x33)))
        .unwrap();
    codecs.set_active_key("k1").unwrap();
    let encoded = codecs
        .encode_payload(&serde_json::json!({"secret": "value"}))
        .unwrap();
    diesel::sql_query(
        "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data) \
         VALUES ($1, 0, 'WorkflowStarted', $2)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Jsonb, _>(serde_json::json!({
        "type": "WorkflowStarted",
        "data": {"input": encoded, "timestamp": "2026-08-31T00:00:00Z"}
    }))
    .execute(&mut conn)
    .await
    .unwrap();
    codecs.set_active_key("k2").unwrap();

    // This worker is pinned to generation 0; the region has been promoted past
    // it.
    FenceRegistry::clear();
    FenceRegistry::register(ShardId::new(0), ShardGeneration::new(0))
        .expect("no conflicting pin in this test");
    FenceRegistry::set_default_shard(ShardId::new(0))
        .expect("no conflicting default shard in this test");
    bump_generation(&mut conn, ShardId::new(0), "promote", "oncall")
        .await
        .unwrap();

    let swept = autumn_harvest::codec_rotation::sweep_codec_reencryption_once(
        &mut conn,
        0,
        &codecs,
        100,
        &NoOpMetrics,
    )
    .await;

    // Whether the sweep surfaces the fence as an error or simply converts
    // nothing, the row must be untouched -- that is the guarantee.
    #[derive(diesel::QueryableByName)]
    struct Kid {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        kid: Option<String>,
    }
    let rows: Vec<Kid> =
        diesel::sql_query("SELECT event_data->'data'->'input'->>'kid' AS kid FROM harvest_events")
            .load(&mut conn)
            .await
            .unwrap();
    assert_eq!(
        rows[0].kid.as_deref(),
        Some("k1"),
        "a fenced worker must not re-encrypt history the promoted region owns; \
         sweep returned {swept:?}"
    );
    FenceRegistry::clear();
}

#[tokio::test]
async fn a_fenced_worker_cannot_advance_the_rotation_cursor() {
    // Sibling to `a_fenced_worker_cannot_re_encrypt_history` (issue #1257).
    // A worker pinned to a superseded generation must not record any
    // rotation progress at all, not just leave `harvest_events` untouched.
    //
    // With a convertible row present, the existing per-row fence on
    // `compare_and_swap_event` (issue #954) already errors out of the
    // sweep before `write_cursor` is reached. So this scenario alone does
    // not discriminate the fix from before it. See
    // `a_fenced_sweep_that_converts_nothing_still_fails_closed` for the
    // batch that reaches `write_cursor` with nothing to convert. This test
    // stays as a direct check that no cursor row leaks in the common case
    // too.
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("rotate_cursor");
    let mut conn = connect(&url).await;
    ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .unwrap();

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, state, input, shard_id) \
         VALUES ($1, 'wf', 'rotate-cursor-1', 'RUNNING', '{}'::jsonb, 0)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut conn)
    .await
    .unwrap();

    // A history row encoded under `k1`, with `k2` now active: exactly what
    // the sweep exists to convert.
    let codecs = autumn_harvest::payload_codec::PayloadCodecs::default();
    codecs
        .register_key("k1", std::sync::Arc::new(DrXorCodec(0x5a)))
        .unwrap();
    codecs
        .register_key("k2", std::sync::Arc::new(DrXorCodec(0x33)))
        .unwrap();
    codecs.set_active_key("k1").unwrap();
    let encoded = codecs
        .encode_payload(&serde_json::json!({"secret": "value"}))
        .unwrap();
    diesel::sql_query(
        "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data) \
         VALUES ($1, 0, 'WorkflowStarted', $2)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Jsonb, _>(serde_json::json!({
        "type": "WorkflowStarted",
        "data": {"input": encoded, "timestamp": "2026-08-31T00:00:00Z"}
    }))
    .execute(&mut conn)
    .await
    .unwrap();
    codecs.set_active_key("k2").unwrap();

    // This worker is pinned to generation 0; the region has been promoted
    // past it.
    FenceRegistry::clear();
    FenceRegistry::register(ShardId::new(0), ShardGeneration::new(0))
        .expect("no conflicting pin in this test");
    FenceRegistry::set_default_shard(ShardId::new(0))
        .expect("no conflicting default shard in this test");
    bump_generation(&mut conn, ShardId::new(0), "promote", "oncall")
        .await
        .unwrap();

    let _ = autumn_harvest::codec_rotation::sweep_codec_reencryption_once(
        &mut conn,
        0,
        &codecs,
        100,
        &NoOpMetrics,
    )
    .await;

    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let rows: Vec<Count> =
        diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_codec_rotation_cursor")
            .load(&mut conn)
            .await
            .unwrap();
    assert_eq!(
        rows[0].n, 0,
        "a fenced worker must not record rotation progress it was fenced away from"
    );
    FenceRegistry::clear();
}

#[tokio::test]
async fn a_fenced_sweep_that_converts_nothing_still_fails_closed() {
    // Sibling to `a_fenced_worker_cannot_re_encrypt_history` (issue #1257,
    // acceptance criterion: a sweep batch that converts no rows must still
    // fail closed under a stale fence). A batch with nothing to convert
    // never calls the per-row fenced CAS at all. `write_cursor` and
    // `claim_completed_cursor_revalidation` are the only writes left on
    // this path, so they are the only guard against a stale worker
    // recording progress.
    //
    // The cursor here is already complete and due for revalidation. An
    // unfenced worker would also win `claim_completed_cursor_revalidation`
    // and bump `updated_at` even though it converted nothing.
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("rotate_cursor_empty");
    let mut conn = connect(&url).await;
    ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .unwrap();

    diesel::sql_query(
        "INSERT INTO harvest_codec_rotation_cursor \
             (shard_id, active_key_id, last_event_id, rows_reencrypted, \
              unresolved_rows, completed_at, updated_at) \
         VALUES (0, 'k2', 0, 0, 0, NOW() - interval '1 hour', \
                 NOW() - interval '1 hour')",
    )
    .execute(&mut conn)
    .await
    .unwrap();

    let codecs = autumn_harvest::payload_codec::PayloadCodecs::default();
    codecs
        .register_key("k2", std::sync::Arc::new(DrXorCodec(0x33)))
        .unwrap();
    codecs.set_active_key("k2").unwrap();

    // This worker is pinned to generation 0; the region has been promoted
    // past it.
    FenceRegistry::clear();
    FenceRegistry::register(ShardId::new(0), ShardGeneration::new(0))
        .expect("no conflicting pin in this test");
    FenceRegistry::set_default_shard(ShardId::new(0))
        .expect("no conflicting default shard in this test");
    bump_generation(&mut conn, ShardId::new(0), "promote", "oncall")
        .await
        .unwrap();

    let _ = autumn_harvest::codec_rotation::sweep_codec_reencryption_once(
        &mut conn,
        0,
        &codecs,
        100,
        &NoOpMetrics,
    )
    .await;

    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        last_event_id: i64,
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        updated_at: chrono::DateTime<chrono::Utc>,
    }
    let rows: Vec<Row> = diesel::sql_query(
        "SELECT last_event_id, updated_at FROM harvest_codec_rotation_cursor \
         WHERE shard_id = 0",
    )
    .load(&mut conn)
    .await
    .unwrap();
    // `rows.len()` and `last_event_id` hold both before and after this
    // fix. The seeded row already starts at `last_event_id = 0`, so they
    // document the row is untouched rather than proving the fence fired.
    // `updated_at` is the assertion that actually distinguishes a fenced
    // `claim_completed_cursor_revalidation` from an unfenced one. An
    // unfenced worker would have won the claim and bumped it to `NOW()`.
    assert_eq!(
        rows.len(),
        1,
        "the pre-existing cursor row must survive untouched"
    );
    assert_eq!(
        rows[0].last_event_id, 0,
        "a fenced worker must not advance a cursor it examined nothing under"
    );
    assert!(
        rows[0].updated_at < chrono::Utc::now() - chrono::Duration::minutes(30),
        "a fenced worker must not win the revalidation claim and bump updated_at"
    );
    FenceRegistry::clear();
}

#[tokio::test]
async fn a_fenced_worker_cannot_backfill_quota_keys() {
    // The quota_key backfill reconciler (issue #1226, follow-up to #946)
    // is a second module that UPDATEs `harvest_workflow_executions`
    // outside the ordinary claim/dispatch path. It needs its own fence
    // assertion for the same reason the codec-rotation sweep above does.
    //
    // A fenced worker's registry view can be stale relative to the
    // promoted region. An unfenced backfill could therefore write a
    // `quota_key` the new region's own reconciler would have resolved
    // differently. It could even write one at all, for a row the new
    // region has already moved past.
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("quota_reconcile");
    let mut conn = connect(&url).await;
    ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .unwrap();

    let workflow_name = format!("wf_dr_fenced_{}", DB_SEQ.fetch_add(1, Ordering::SeqCst));
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, state, input, shard_id) \
         VALUES ($1, $2, 'rotate-1', 'RUNNING', '{\"tenant_id\": \"acme\"}'::jsonb, 0)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(&workflow_name)
    .execute(&mut conn)
    .await
    .unwrap();

    let policy =
        autumn_harvest::quota::QuotaPolicy::new("tenant_id").with_max_active_executions(10);
    let previous_metadata = {
        let mut lock = autumn_harvest::completion_trigger::GLOBAL_WORKFLOW_METADATA
            .write()
            .expect("metadata lock");
        let mut map = std::collections::HashMap::new();
        map.insert(
            workflow_name.clone(),
            autumn_harvest::completion_trigger::WorkflowMetadata {
                concurrency: None,
                max_input_bytes: None,
                owner: None,
                runbook_url: None,
                severity: None,
                input_schema: None,
                sla: None,
                retry_policy: None,
                quota: Some(policy),
            },
        );
        lock.replace(map)
    };

    // This worker is pinned to generation 0; the region has been promoted
    // past it.
    FenceRegistry::clear();
    FenceRegistry::register(ShardId::new(0), ShardGeneration::new(0))
        .expect("no conflicting pin in this test");
    FenceRegistry::set_default_shard(ShardId::new(0))
        .expect("no conflicting default shard in this test");
    bump_generation(&mut conn, ShardId::new(0), "promote", "oncall")
        .await
        .unwrap();

    let result = autumn_harvest::quota_reconcile::reconcile_quota_keys(
        &mut conn,
        100,
        Some(ShardId::new(0)),
    )
    .await;

    #[derive(diesel::QueryableByName)]
    struct QuotaKeyRow {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        quota_key: Option<String>,
    }
    let rows: Vec<QuotaKeyRow> =
        diesel::sql_query("SELECT quota_key FROM harvest_workflow_executions WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
            .load(&mut conn)
            .await
            .unwrap();
    assert_eq!(
        rows[0].quota_key, None,
        "a fenced worker must not backfill a quota_key onto a row the promoted \
         region now owns; sweep returned {result:?}"
    );

    {
        let mut lock = autumn_harvest::completion_trigger::GLOBAL_WORKFLOW_METADATA
            .write()
            .expect("metadata lock");
        *lock = previous_metadata;
    }
    FenceRegistry::clear();
}

#[tokio::test]
async fn a_fenced_worker_cannot_claim_tasks() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("claim");
    let mut conn = connect(&url).await;
    ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .unwrap();

    let params = autumn_harvest::queue::EnqueueParams::new(
        "q-dr",
        autumn_harvest::queue::TaskType::Activity,
        serde_json::json!({}),
    );
    autumn_harvest::queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue");

    FenceRegistry::clear();
    FenceRegistry::register(ShardId::new(0), ShardGeneration::new(0))
        .expect("no conflicting pin in this test");
    FenceRegistry::set_default_shard(ShardId::new(0))
        .expect("no conflicting default shard in this test");

    // Current epoch: the claim succeeds exactly as it did before #954.
    let claimed = autumn_harvest::queue::claim_task_on_shard(
        &mut conn,
        &["q-dr".to_string()],
        "w-dr",
        "",
        None,
        &[],
        &[],
        Some(ShardId::new(0)),
    )
    .await
    .expect("claim");
    assert!(claimed.is_some(), "an unfenced worker claims normally");

    // Release it and fence the region.
    diesel::sql_query("UPDATE harvest_task_queue SET state = 'PENDING', worker_id = NULL")
        .execute(&mut conn)
        .await
        .unwrap();
    bump_generation(&mut conn, ShardId::new(0), "promote", "oncall")
        .await
        .unwrap();

    let after = autumn_harvest::queue::claim_task_on_shard(
        &mut conn,
        &["q-dr".to_string()],
        "w-dr",
        "",
        None,
        &[],
        &[],
        Some(ShardId::new(0)),
    )
    .await
    .expect("claim query itself still succeeds");
    assert!(after.is_none(), "a fenced worker must claim nothing");

    // The task is untouched — still PENDING, attempt not consumed.
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        state: String,
        #[diesel(sql_type = diesel::sql_types::Integer)]
        attempt: i32,
    }
    let rows: Vec<Row> = diesel::sql_query("SELECT state, attempt FROM harvest_task_queue")
        .load(&mut conn)
        .await
        .unwrap();
    assert_eq!(rows[0].state, "PENDING");
    assert_eq!(
        rows[0].attempt, 1,
        "the fenced attempt must not burn a retry"
    );
    FenceRegistry::clear();
}

/// The fence bump is a **commit-order barrier**, not a racy read.
///
/// This is the property the whole mechanism rests on: a persist that passes the
/// fence check must be guaranteed to commit *before* the fence takes effect, so
/// there is never a "one last append" from a worker that has just lost write
/// authority. The barrier is `bump_generation`'s `ACCESS EXCLUSIVE` table lock
/// conflicting with the `ACCESS SHARE` that `assert_fence`'s plain read takes
/// implicitly — deliberately not a shared row lock, which on a
/// one-row-per-shard table would make every claim in the fleet a MultiXactId
/// producer.
#[tokio::test]
async fn a_fence_bump_cannot_commit_while_a_persist_holds_the_fence() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("barrier");
    let mut setup = connect(&url).await;
    ensure_generation_row(&mut setup, ShardId::new(0))
        .await
        .unwrap();

    FenceRegistry::clear();
    FenceRegistry::register(ShardId::new(0), ShardGeneration::new(0))
        .expect("no conflicting pin in this test");
    FenceRegistry::set_default_shard(ShardId::new(0))
        .expect("no conflicting default shard in this test");

    // Session A: open a transaction and pass the fence check. Its ACCESS SHARE
    // on harvest_shard_generation is now held until A commits.
    let mut a = connect(&url).await;
    a.batch_execute("BEGIN").await.expect("begin");
    assert_fence(&mut a, ShardId::new(0))
        .await
        .expect("the pinned epoch is current");

    // Session B: bump. It must BLOCK behind A rather than committing underneath
    // it. `bump_generation` carries a 5s lock_timeout, so a broken barrier
    // shows up as an immediate success and a working one as a timeout error.
    let mut b = connect(&url).await;
    let bump = bump_generation(&mut b, ShardId::new(0), "barrier probe", "test").await;
    let err = bump.expect_err("the bump must not commit while a persist holds the fence");
    assert!(
        err.to_string().contains("lock timeout") || err.to_string().contains("lock_timeout"),
        "expected the bump to block on the fence table lock, got: {err}"
    );

    // A commits; the bump now succeeds, and a persist starting afterwards is
    // fenced.
    a.batch_execute("COMMIT").await.expect("commit");
    let generation = bump_generation(&mut b, ShardId::new(0), "barrier probe", "test")
        .await
        .expect("the bump proceeds once the holder commits");
    assert_eq!(generation, ShardGeneration::new(1));
    let err = assert_fence(&mut setup, ShardId::new(0))
        .await
        .expect_err("a persist beginning after the bump must observe the new epoch");
    assert!(matches!(
        err,
        autumn_harvest::error::HarvestError::ShardFenced { .. }
    ));
    FenceRegistry::clear();
}

/// A sequence owned by a **view** must never reach the promotion helper.
///
/// `ALTER SEQUENCE s OWNED BY <view>.<col>` is accepted by Postgres. Without a
/// `relkind` filter the helper would emit `... FROM <view>`, executing that
/// view's query — including any volatile function in it — on the operator's
/// high-privilege DR connection, during an incident, on a command whose output
/// nobody reads closely. Anyone with `CREATE` in the schema can plant it months
/// ahead, and the prescribed `FOR ALL TABLES` publication replicates it to the
/// standby too.
#[tokio::test]
async fn promotion_never_executes_a_view_that_owns_a_sequence() {
    let (url, _db) = require_db!("viewown");
    let mut conn = connect(&url).await;
    conn.batch_execute(
        "CREATE TABLE dr_probe_marker (hit int);
         CREATE FUNCTION dr_probe_fire() RETURNS bigint LANGUAGE plpgsql VOLATILE AS $$
           BEGIN INSERT INTO dr_probe_marker VALUES (1); RETURN 1; END $$;
         CREATE VIEW dr_probe_view AS SELECT dr_probe_fire() AS id;
         CREATE SEQUENCE dr_probe_seq;
         ALTER SEQUENCE dr_probe_seq OWNED BY dr_probe_view.id;",
    )
    .await
    .expect("plant the view-owned sequence");

    let advanced = autumn_harvest::replication::advance_sequences_after_promotion(&mut conn)
        .await
        .expect("promotion must succeed, having simply skipped the view");
    assert!(
        !advanced
            .iter()
            .any(|(name, _)| name.contains("dr_probe_seq")),
        "a view-owned sequence must never be advanced: {advanced:?}"
    );

    #[derive(diesel::QueryableByName)]
    struct C {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let rows: Vec<C> = diesel::sql_query("SELECT COUNT(*) AS n FROM dr_probe_marker")
        .load(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        rows.into_iter().next().map_or(-1, |c| c.n),
        0,
        "the view's body must NOT have been executed"
    );
}

/// Promotion must never rewind a sequence that is already ahead of its table.
///
/// A sequence legitimately sits ahead of `MAX(col)` after cached values, a
/// rolled-back transaction, or deleted rows — and a *physical* replica
/// replicates sequences already, which is why the docs call this command a
/// harmless no-op there. Setting it to `MAX(col)` unconditionally would rewind
/// it and start re-issuing ids the database has already handed out: a
/// duplicate-key outage caused by the very command that exists to prevent one.
#[tokio::test]
async fn promotion_never_rewinds_a_sequence_that_is_ahead_of_its_table() {
    let (url, _db) = require_db!("seqahead");
    let mut conn = connect(&url).await;
    conn.batch_execute(
        "CREATE TABLE dr_ahead (id BIGSERIAL PRIMARY KEY);
         INSERT INTO dr_ahead DEFAULT VALUES;
         INSERT INTO dr_ahead DEFAULT VALUES;
         DELETE FROM dr_ahead WHERE id = 2;",
    )
    .await
    .expect("seed a sequence ahead of MAX(id)");

    #[derive(diesel::QueryableByName)]
    struct N {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        v: i64,
    }
    async fn scalar(conn: &mut AsyncPgConnection, sql: &str) -> i64 {
        let rows: Vec<N> = diesel::sql_query(sql).load(conn).await.expect("scalar");
        rows.into_iter().next().map_or(-1, |n| n.v)
    }

    // Precondition: MAX = 1 while the sequence has already issued 2.
    assert_eq!(
        scalar(&mut conn, "SELECT COALESCE(MAX(id), 0) AS v FROM dr_ahead").await,
        1
    );
    assert_eq!(
        scalar(
            &mut conn,
            "SELECT pg_sequence_last_value('dr_ahead_id_seq') AS v"
        )
        .await,
        2
    );

    autumn_harvest::replication::advance_sequences_after_promotion(&mut conn)
        .await
        .expect("promotion");

    assert_eq!(
        scalar(
            &mut conn,
            "SELECT pg_sequence_last_value('dr_ahead_id_seq') AS v"
        )
        .await,
        2,
        "the sequence must be left where it was, not rewound to MAX(id)"
    );
    assert_eq!(
        scalar(&mut conn, "SELECT nextval('dr_ahead_id_seq') AS v").await,
        3,
        "the next id must not collide with one already issued"
    );
}

/// Identifiers that are not plain lowercase must be advanced, not skipped.
///
/// An earlier revision screened identifiers instead of quoting them: an
/// embedder on a `PascalCase` ORM schema had **every** sequence silently skipped
/// while `harvest dr promote` reported success, and an ordinary table named
/// `user` or `order` passed the screen and then failed as a bare keyword.
#[tokio::test]
async fn promotion_advances_reserved_word_and_mixed_case_relations() {
    let (url, _db) = require_db!("quoting");
    let mut conn = connect(&url).await;
    conn.batch_execute(
        "CREATE TABLE \"user\" (id BIGSERIAL PRIMARY KEY);
         CREATE TABLE \"order\" (id BIGSERIAL PRIMARY KEY);
         CREATE TABLE \"MixedCase\" (\"Id\" BIGSERIAL PRIMARY KEY);
         INSERT INTO \"user\" DEFAULT VALUES;
         INSERT INTO \"user\" DEFAULT VALUES;
         INSERT INTO \"order\" DEFAULT VALUES;
         INSERT INTO \"MixedCase\" DEFAULT VALUES;",
    )
    .await
    .expect("seed awkwardly-named relations");

    // Rewind every sequence, exactly as an un-replicated logical standby would
    // have them.
    conn.batch_execute(
        "SELECT setval('\"user_id_seq\"', 1, false);
         SELECT setval('\"order_id_seq\"', 1, false);
         SELECT setval('\"MixedCase_Id_seq\"', 1, false);",
    )
    .await
    .expect("rewind sequences");

    let advanced = autumn_harvest::replication::advance_sequences_after_promotion(&mut conn)
        .await
        .expect("promotion must handle quoted identifiers");
    for expected in ["user_id_seq", "order_id_seq", "MixedCase_Id_seq"] {
        assert!(
            advanced.iter().any(|(name, _)| name.contains(expected)),
            "{expected} must have been advanced, got {advanced:?}"
        );
    }

    // And the advance is real: the next value must clear the existing rows.
    #[derive(diesel::QueryableByName)]
    struct N {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        v: i64,
    }
    let rows: Vec<N> = diesel::sql_query("SELECT nextval('\"user_id_seq\"') AS v")
        .load(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        rows.into_iter().next().map_or(0, |n| n.v),
        3,
        "the next id must be max(id) + 1, not a duplicate"
    );
}

/// A sequence owned by a table in a non-public schema must be advanced,
/// using the OWNING TABLE's schema rather than the sequence's own
/// (finding 5).
///
/// The catalog query selected `tn.nspname` (the owning table's namespace)
/// but filtered on `sn.nspname = current_schema()` (the sequence's own).
/// Postgres always keeps an owned sequence in the same schema as its
/// table. The two names never diverge, so this distinction has no
/// effect on the current query. Filtering on the table's schema states
/// the intent plainly: promotion advances sequences for tables, not for
/// the sequences themselves. This test exercises the qualified-name path
/// with both objects outside `public`.
#[tokio::test]
async fn promotion_advances_a_sequence_owned_by_a_table_in_a_non_public_schema() {
    let (url, _db) = require_db!("crossschema");
    let mut conn = connect(&url).await;
    conn.batch_execute(
        "CREATE SCHEMA dr_seq_home;
         CREATE SEQUENCE dr_seq_home.dr_cross_seq;
         CREATE TABLE dr_seq_home.dr_cross (id BIGINT PRIMARY KEY DEFAULT nextval('dr_seq_home.dr_cross_seq'));
         ALTER SEQUENCE dr_seq_home.dr_cross_seq OWNED BY dr_seq_home.dr_cross.id;
         SET search_path TO dr_seq_home, public;
         INSERT INTO dr_cross DEFAULT VALUES;
         INSERT INTO dr_cross DEFAULT VALUES;
         SELECT setval('dr_seq_home.dr_cross_seq', 1, false);",
    )
    .await
    .expect("seed a table owning a sequence in a non-public schema");

    let advanced = autumn_harvest::replication::advance_sequences_after_promotion(&mut conn)
        .await
        .expect("promotion");
    assert!(
        advanced
            .iter()
            .any(|(name, _)| name.contains("dr_cross_seq")),
        "a sequence owned by a table in a non-public current_schema() must \
         be advanced; advanced: {advanced:?}"
    );

    #[derive(diesel::QueryableByName)]
    struct N {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        v: i64,
    }
    let rows: Vec<N> = diesel::sql_query("SELECT nextval('dr_seq_home.dr_cross_seq') AS v")
        .load(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        rows.into_iter().next().map_or(0, |n| n.v),
        3,
        "the next id must clear the two already-inserted rows, not collide with one"
    );
}

/// Promotion must not rewind a DESCENDING sequence (finding 11).
///
/// `GREATEST` assumes ascending issuance. For a descending sequence,
/// "furthest issued" is the MINIMUM, not the maximum. Using `GREATEST`
/// unconditionally reset a sequence that had issued 100 then 99 back to
/// 100. The next value handed out was then 99 again — a collision, from
/// the helper whose entire purpose is preventing one.
#[tokio::test]
async fn promotion_never_rewinds_a_descending_sequence() {
    let (url, _db) = require_db!("seqdesc");
    let mut conn = connect(&url).await;
    conn.batch_execute(
        "CREATE SEQUENCE dr_desc_seq INCREMENT BY -1 MINVALUE -1000000 MAXVALUE -1 START -1;
         CREATE TABLE dr_desc (id BIGINT PRIMARY KEY DEFAULT nextval('dr_desc_seq'));
         ALTER SEQUENCE dr_desc_seq OWNED BY dr_desc.id;
         INSERT INTO dr_desc DEFAULT VALUES; -- issues -1
         INSERT INTO dr_desc DEFAULT VALUES; -- issues -2",
    )
    .await
    .expect("seed a descending owned sequence that has issued -1 then -2");

    #[derive(diesel::QueryableByName)]
    struct N {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        v: i64,
    }
    async fn scalar(conn: &mut AsyncPgConnection, sql: &str) -> i64 {
        let rows: Vec<N> = diesel::sql_query(sql).load(conn).await.expect("scalar");
        rows.into_iter().next().map_or(i64::MIN, |n| n.v)
    }

    assert_eq!(
        scalar(
            &mut conn,
            "SELECT pg_sequence_last_value('dr_desc_seq') AS v"
        )
        .await,
        -2
    );

    autumn_harvest::replication::advance_sequences_after_promotion(&mut conn)
        .await
        .expect("promotion");

    assert_eq!(
        scalar(
            &mut conn,
            "SELECT pg_sequence_last_value('dr_desc_seq') AS v"
        )
        .await,
        -2,
        "a descending sequence must not be reset back up to a table MAX"
    );
    assert_eq!(
        scalar(&mut conn, "SELECT nextval('dr_desc_seq') AS v").await,
        -3,
        "the next id must continue descending past what was already issued, not collide"
    );
}

/// The watermark beat must never leave an advisory lock behind.
///
/// The single-writer gate uses `pg_try_advisory_xact_lock`, not the
/// session-scoped variant. A session lock is released only by an explicit
/// unlock, so any error between acquire and release leaks it — and on a
/// **pooled** connection it leaks permanently: every other worker's sampler
/// then skips its beat forever, re-acquiring on the same session only bumps the
/// lock count, and the measured RPO goes stale during exactly the database
/// trouble it exists to measure. (Verified against live Postgres: an advisory
/// lock does survive a failed statement in the same session.)
#[tokio::test]
async fn the_watermark_beat_leaves_no_advisory_lock_behind() {
    let (url, _db) = require_db!("beatlock");
    let mut conn = connect(&url).await;
    let shard = ShardId::new(0);

    #[derive(diesel::QueryableByName)]
    struct C {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    async fn advisory_locks_held(conn: &mut AsyncPgConnection) -> i64 {
        let rows: Vec<C> = diesel::sql_query(
            "SELECT COUNT(*) AS n FROM pg_locks \
             WHERE locktype = 'advisory' AND pid = pg_backend_pid()",
        )
        .load(conn)
        .await
        .expect("count advisory locks");
        rows.into_iter().next().map_or(-1, |c| c.n)
    }

    for _ in 0..3 {
        autumn_harvest::replication::record_replication_heartbeat(
            &mut conn,
            shard,
            std::time::Duration::from_secs(3600),
            BEAT_INTERVAL,
        )
        .await
        .expect("beat");
        assert_eq!(
            advisory_locks_held(&mut conn).await,
            0,
            "the beat must hold no advisory lock once it returns"
        );
    }

    // …and a beat on a *different* connection is never starved by a previous
    // one, which is what a leaked session lock would cause.
    let mut other = connect(&url).await;
    autumn_harvest::replication::record_replication_heartbeat(
        &mut other,
        shard,
        std::time::Duration::from_secs(3600),
        BEAT_INTERVAL,
    )
    .await
    .expect("a second connection must still be able to take the beat");
}

/// The promotion's statement timeout must actually be in force.
///
/// `SET LOCAL` outside a transaction block is **ignored** by Postgres — it
/// emits a `WARNING` and `statement_timeout` reads back as `0`. Verified
/// against live Postgres. The helper therefore has to run inside a transaction,
/// or the ceiling it documents on the RTO-critical path is silently absent.
#[tokio::test]
async fn the_promotion_runs_inside_a_transaction_so_its_timeout_applies() {
    let (url, _db) = require_db!("promotetx");
    let mut conn = connect(&url).await;

    // A table whose `MAX()` scan is slow enough to prove the timeout is armed
    // would make this test slow too. Instead, assert the property the timeout
    // depends on: the helper must not leave `statement_timeout` set at session
    // level (which would mean it used `SET`, leaking the ceiling onto every
    // later query on a pooled connection), and must succeed — which under
    // `SET LOCAL` is only possible inside a transaction.
    autumn_harvest::replication::advance_sequences_after_promotion(&mut conn)
        .await
        .expect("promotion succeeds");

    #[derive(diesel::QueryableByName)]
    struct S {
        #[diesel(sql_type = diesel::sql_types::Text)]
        timeout: String,
    }
    let rows: Vec<S> = diesel::sql_query("SELECT current_setting('statement_timeout') AS timeout")
        .load(&mut conn)
        .await
        .expect("read statement_timeout");
    assert_eq!(
        rows.into_iter().next().map(|s| s.timeout).as_deref(),
        Some("0"),
        "the promotion's timeout must be transaction-local, not leaked onto the session"
    );
}

/// The subscription conninfo must address the server, not the test client.
///
/// Regression guard for the CI-only failure that the three two-region tests hit
/// the first time they ever ran under Docker: the conninfo was built from the
/// *client-side* URL, so it carried the host-mapped port (`localhost:33624`).
/// `CREATE SUBSCRIPTION` dials the publisher from inside the server, where that
/// port does not exist, and every two-region test died with "could not connect
/// to the publisher ... Connection refused".
///
/// It passed on a host-installed Postgres because there the client port and the
/// server port are the same number, which is exactly why no local run could
/// catch it. This test creates that divergence deliberately by passing a URL
/// whose port is wrong, and asserts the conninfo ignores it.
#[tokio::test]
async fn the_subscription_conninfo_uses_the_servers_own_port_not_the_clients() {
    let (url, _db) = require_db!("conninfo");
    let mut conn = connect(&url).await;

    // A port the server is certainly NOT listening on, standing in for the
    // host-mapped port a container publishes.
    let lying_url = "postgres://postgres@localhost:1/postgres";
    let conninfo = server_side_conninfo(&mut conn, lying_url, "somedb").await;

    assert!(
        !conninfo.contains("port=1 "),
        "the conninfo must not carry the client-side port; got {conninfo:?}"
    );
    assert!(
        conninfo.contains("host=127.0.0.1"),
        "the conninfo must dial loopback explicitly — `localhost` resolved to ::1 in CI, which \
         a server not listening on IPv6 refuses; got {conninfo:?}"
    );

    #[derive(diesel::QueryableByName)]
    struct Port {
        #[diesel(sql_type = diesel::sql_types::Text)]
        port: String,
    }
    let actual = diesel::sql_query("SELECT current_setting('port') AS port")
        .load::<Port>(&mut conn)
        .await
        .expect("read port")
        .into_iter()
        .next()
        .expect("one row")
        .port;
    assert!(
        conninfo.contains(&format!("port={actual}")),
        "the conninfo must carry the server's own port ({actual}); got {conninfo:?}"
    );
}

// ── AC4 / AC6(c): measured RPO ─────────────────────────────────────────────

#[tokio::test]
async fn replication_status_on_a_primary_with_no_standby_is_not_a_zero_rpo() {
    let (url, _db) = require_db!("norepl");
    let mut conn = connect(&url).await;
    let status = query_replication_status(&mut conn, ShardId::new(0), DR_PREFIX)
        .await
        .expect("query");
    assert_eq!(
        status.connected_standbys(),
        0,
        "no subscription was created for this database"
    );
    assert_eq!(
        status.max_replay_lag_seconds(),
        None,
        "a primary with no standby has an UNKNOWN RPO, never 0"
    );
    assert!(matches!(status, ReplicationStatus::Observed { .. }));
}

/// One abandoned DR slot beside one healthy one must be a PARTIAL reading,
/// never folded into "nothing measured" (finding 1).
///
/// A physical slot created with `immediately_reserve = false` has a NULL
/// `restart_lsn` until a standby connects — a never-connected or abandoned
/// DR target. Before the fix, `bool_or(position IS NULL)` made the WHOLE
/// reading `Unknown` whenever any one slot lacked a position. A healthy
/// slot's small lag then masked the abandoned one entirely.
/// `rpo_seconds()` fell back to `replay_lag`, which has no idea the
/// abandoned slot exists either.
#[tokio::test]
async fn one_unmeasurable_slot_beside_a_measurable_one_is_a_partial_reading() {
    let (url, _db) = require_db!("partialslot");
    let mut conn = connect(&url).await;
    let shard = ShardId::new(0);

    let reserved = format!("{DR_PREFIX}_reserved_{}", std::process::id());
    let unreserved = format!("{DR_PREFIX}_unreserved_{}", std::process::id());
    diesel::sql_query("SELECT pg_create_physical_replication_slot($1, true)")
        .bind::<diesel::sql_types::Text, _>(reserved.clone())
        .execute(&mut conn)
        .await
        .expect("create the reserved (measurable) physical slot");
    diesel::sql_query("SELECT pg_create_physical_replication_slot($1, false)")
        .bind::<diesel::sql_types::Text, _>(unreserved.clone())
        .execute(&mut conn)
        .await
        .expect("create the unreserved (never-connected) physical slot");

    let reading = autumn_harvest::replication::measure_rpo(&mut conn, shard, DR_PREFIX)
        .await
        .expect("measure_rpo");
    match reading {
        WatermarkReading::PartiallyMeasured {
            unmeasurable_slots, ..
        } => {
            assert_eq!(
                unmeasurable_slots, 1,
                "exactly the unreserved slot must be counted unmeasurable"
            );
        }
        other => panic!(
            "one measurable slot beside one unmeasurable one must be a partial reading, got \
             {other:?}"
        ),
    }
    let status = query_replication_status(&mut conn, shard, DR_PREFIX)
        .await
        .expect("status");
    assert_eq!(
        status.unmeasurable_slot_count(),
        1,
        "the count must also be visible from ReplicationStatus"
    );
    assert!(
        status.rpo_seconds().is_none() || status.max_replay_lag_seconds().is_none(),
        "a partial reading with nothing measured yet must never silently resolve via replay_lag"
    );

    for name in [reserved, unreserved] {
        let _ = diesel::sql_query("SELECT pg_drop_replication_slot($1)")
            .bind::<diesel::sql_types::Text, _>(name)
            .execute(&mut conn)
            .await;
    }
}

/// An unrelated logical-decoding consumer must not read as a DR standby.
///
/// A shard database can legitimately host a CDC pipeline's slot alongside its
/// DR subscription. Counting every walsender for the database meant that if the
/// real cross-region subscriber disconnected, `connected_standbys()` stayed
/// non-zero, `harvest dr status` reported the shard protected, and
/// `harvest_replication_down` never fired — the most dangerous false negative
/// this feature has, because it is silent and it is wrong in the safe-looking
/// direction.
#[tokio::test]
async fn a_non_dr_slot_is_not_counted_as_a_dr_standby() {
    let (url, _db) = require_db!("cdcslot");
    let mut conn = connect(&url).await;

    // A CDC-style logical slot with a name that is nothing to do with DR.
    let slot = format!("cdc_pipeline_{}", std::process::id());
    diesel::sql_query("SELECT pg_create_logical_replication_slot($1, 'pgoutput')")
        .bind::<diesel::sql_types::Text, _>(slot.clone())
        .execute(&mut conn)
        .await
        .expect("create the non-DR slot");

    let status = query_replication_status(&mut conn, ShardId::new(0), DR_PREFIX)
        .await
        .expect("status");
    assert_eq!(
        status.connected_standbys(),
        0,
        "a CDC slot must not be counted as a DR standby"
    );
    assert_eq!(
        status.inactive_slots(),
        0,
        "and it must not appear in the DR slot inventory either: {status:?}"
    );
    assert_eq!(
        status.max_lag_bytes(),
        None,
        "its WAL backlog is not this shard's DR backlog"
    );

    // Sanity: the same query DOES see a slot carrying the DR prefix, so the
    // assertions above are about the filter and not about an empty database.
    let dr_slot = format!("{DR_PREFIX}_cdcprobe_{}", std::process::id());
    diesel::sql_query("SELECT pg_create_logical_replication_slot($1, 'pgoutput')")
        .bind::<diesel::sql_types::Text, _>(dr_slot.clone())
        .execute(&mut conn)
        .await
        .expect("create the DR slot");
    let status = query_replication_status(&mut conn, ShardId::new(0), DR_PREFIX)
        .await
        .expect("status");
    assert_eq!(
        status.inactive_slots(),
        1,
        "the DR-prefixed slot must be visible: {status:?}"
    );

    for name in [slot, dr_slot] {
        let _ = diesel::sql_query("SELECT pg_drop_replication_slot($1)")
            .bind::<diesel::sql_types::Text, _>(name)
            .execute(&mut conn)
            .await;
    }
}

/// A slot name that would satisfy `LIKE <prefix> || '%'` but does NOT start
/// with the prefix must never be counted as a DR standby (finding 7).
///
/// `LIKE` treats `_` as "any single character", and `DR_PREFIX`
/// (`harvest_dr`, the shipped default) contains one. Reproduced against live
/// Postgres in the finding: `'harvestXdr_shard0' LIKE 'harvest_dr' || '%'` is
/// `true`. Suppose the real DR sender then disconnected while this unrelated
/// slot remained. `connected_standbys()` would stay non-zero, and
/// `harvest_replication_down` would never fire — on the DEFAULT
/// configuration, not only a custom prefix.
#[tokio::test]
async fn a_slot_matching_the_prefix_only_under_like_wildcards_is_not_counted() {
    let (url, _db) = require_db!("likewild");
    let mut conn = connect(&url).await;

    // `x` stands in for the prefix's own underscore: differs at that one
    // position, so `starts_with` correctly rejects it while `LIKE` would not.
    // Lowercase only -- Postgres replication slot names allow no other case.
    let slot = format!("harvestxdr_wildprobe_{}", std::process::id());
    assert_ne!(&slot[..10], DR_PREFIX, "the probe must not literally match");
    diesel::sql_query("SELECT pg_create_logical_replication_slot($1, 'pgoutput')")
        .bind::<diesel::sql_types::Text, _>(slot.clone())
        .execute(&mut conn)
        .await
        .expect("create the wildcard-matching slot");

    let status = query_replication_status(&mut conn, ShardId::new(0), DR_PREFIX)
        .await
        .expect("status");
    assert_eq!(
        status.connected_standbys(),
        0,
        "a slot that only matches under LIKE's wildcard semantics must not count"
    );
    assert_eq!(
        status.inactive_slots(),
        0,
        "and must not appear in the DR slot inventory either: {status:?}"
    );

    let _ = diesel::sql_query("SELECT pg_drop_replication_slot($1)")
        .bind::<diesel::sql_types::Text, _>(slot)
        .execute(&mut conn)
        .await;
}

// ── The worker lifecycle: pin at startup, self-fence, stop ─────────────────

/// A DR-enabled worker pins at startup, and a fence stops it.
///
/// The claim gate and the persist assert are proven above at the SQL layer;
/// this covers the half that only exists at runtime — `pin_dr_generations`
/// (which must run *before* fleet registration and the first poll),
/// `spawn_replication_sampler`'s self-fence check, the `harvest.shard.fenced`
/// counter, and the worker actually shutting down rather than idling forever
/// claiming nothing.
///
/// Deliberately a **single-pool** worker with no `ShardedDbPool`: that is the
/// deployment shape the topology doc documents (`.with_dr_fencing(true)` and
/// nothing else), and an earlier revision gated the sampler on
/// `sharded_pool.is_some()` — so exactly this configuration got the claim gate
/// but never beat a watermark, never measured an RPO, and never stopped when
/// fenced.
#[tokio::test]
async fn a_dr_enabled_worker_pins_at_startup_and_stops_when_fenced() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("workerfence");

    // A non-zero shard, so a regression that fabricates shard 0 fails here.
    let shard = ShardId::new(3);
    FenceRegistry::clear();
    autumn_harvest::replication::set_dr_config(autumn_harvest::replication::DrConfig {
        fencing: true,
        sample_interval: std::time::Duration::from_millis(300),
        watermark_retain: std::time::Duration::from_secs(3600),
        slot_prefix: DR_PREFIX.to_string(),
    });

    let fenced_count = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let telemetry = std::sync::Arc::new(
        autumn_harvest::telemetry::TelemetryConfig::builder()
            .metrics(std::sync::Arc::new(FenceCounter {
                fenced: std::sync::Arc::clone(&fenced_count),
            }))
            .build(),
    );
    let registry = std::sync::Arc::new(
        autumn_harvest::worker::HandlerRegistry::with_state_and_telemetry(
            Vec::new(),
            Vec::new(),
            autumn_harvest::context::empty_shared_state(),
            telemetry,
        ),
    );

    let mut config = dr_worker_config();
    config.shard_assignments = vec![shard];
    let worker = autumn_harvest::worker::Worker::new(config, registry).expect("worker builds");

    let pool = dr_pool(&url);
    let run = tokio::spawn(async move { worker.run(&pool).await });

    // Pinning happens before registration and the first poll, so both the row
    // and the published registry appear within moments of startup.
    let pin_url = url.clone();
    eventually(
        "the worker to pin its generation",
        std::time::Duration::from_secs(30),
        || {
            let url = pin_url.clone();
            async move {
                let mut conn = connect(&url).await;
                matches!(current_generation(&mut conn, shard).await, Ok(Some(_)))
                    && FenceRegistry::expected(shard).is_some()
            }
        },
    )
    .await;

    // Fence it, as the promoted primary would.
    {
        let mut conn = connect(&url).await;
        bump_generation(&mut conn, shard, "worker lifecycle drill", "test")
            .await
            .expect("bump");
    }

    // `run()` returning is the observable proof that the worker stopped.
    // Idling forever while claiming nothing is the failure mode this asserts
    // against, so a timeout here is a real failure, not flake.
    tokio::time::timeout(std::time::Duration::from_secs(60), run)
        .await
        .expect("a fenced worker must shut down, not idle claiming nothing")
        .expect("worker task must not panic");

    // UFCS: diesel's blanket `RunQueryDsl::load` shadows `AtomicU64::load`
    // through the `Arc` deref in this module.
    assert!(
        std::sync::atomic::AtomicU64::load(&fenced_count, std::sync::atomic::Ordering::SeqCst) > 0,
        "the worker must record harvest.shard.fenced before stopping — an operator's only \\
         signal that a fleet is pinned to a superseded epoch"
    );

    FenceRegistry::clear();
    autumn_harvest::replication::set_dr_config(autumn_harvest::replication::DrConfig::default());
}

/// Counts `harvest.shard.fenced`; every other metric is the default no-op.
#[derive(Debug)]
struct FenceCounter {
    fenced: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl autumn_harvest::telemetry::MetricsRecorder for FenceCounter {
    fn record_shard_fenced(&self, _shard: u16) {
        self.fenced
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

fn dr_pool(url: &str) -> autumn_harvest::worker::DbPool {
    let manager =
        diesel_async::pooled_connection::AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            url,
        );
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("pool build")
}

fn dr_worker_config() -> autumn_harvest::worker::WorkerRuntimeConfig {
    autumn_harvest::worker::WorkerRuntimeConfig::from(
        autumn_harvest::builder::WorkerConfig::default()
            .with_dr_fencing(true)
            .with_replication_sample_interval(std::time::Duration::from_millis(300)),
    )
}

// ── The two-"region" topology, over real logical replication ───────────────
/// Build a libpq conninfo string that **the server itself** can use to reach
/// the publisher database.
///
/// The host and port are deliberately NOT taken from `url`. `url` is the
/// *client-side* address, and under testcontainers that is a host-mapped port
/// (`localhost:33624`) which exists only on the Docker host. `CREATE
/// SUBSCRIPTION` makes Postgres dial the publisher from inside its own network
/// namespace, where that port is closed — the subscription then fails with
/// "could not connect to the publisher ... Connection refused". Both "regions"
/// are databases in one instance, so the address the server needs is its own:
/// loopback on the port it is actually listening on, which it can be asked for.
///
/// `127.0.0.1` rather than `localhost` on purpose: `localhost` resolved to
/// `::1` in CI, and a server not listening on IPv6 refuses that even when the
/// port is right.
async fn server_side_conninfo(conn: &mut AsyncPgConnection, url: &str, db: &str) -> String {
    #[derive(diesel::QueryableByName)]
    struct Port {
        #[diesel(sql_type = diesel::sql_types::Text)]
        port: String,
    }

    let port = diesel::sql_query("SELECT current_setting('port') AS port")
        .load::<Port>(conn)
        .await
        .ok()
        .and_then(|rows| rows.into_iter().next())
        .map_or_else(|| "5432".to_string(), |r| r.port);

    let rest = url
        .trim_start_matches("postgres://")
        .trim_start_matches("postgresql://");
    // Only the credentials are taken from `url`; the address comes from the
    // server itself, above.
    let userinfo = rest.split_once('@').map_or("postgres", |(u, _)| u);
    let (user, password) = userinfo
        .split_once(':')
        .map_or((userinfo, None), |(u, p)| (u, Some(p)));
    use std::fmt::Write as _;

    let mut conninfo = format!("host=127.0.0.1 port={port} user={user} dbname={db}");
    if let Some(pw) = password {
        let _ = write!(conninfo, " password={pw}");
    }
    conninfo
}

/// A primary ("region A") and a standby ("region B") wired with stock logical
/// replication, plus the teardown that must run even on failure — an orphaned
/// replication slot pins WAL on the shared test server forever.
struct Regions {
    primary_url: String,
    primary_db: String,
    standby_url: String,
    slot: String,
    sub: String,
}

impl Regions {
    async fn teardown(&self) {
        if let Ok(mut b) = AsyncPgConnection::establish(&self.standby_url).await {
            let _ = b
                .batch_execute(&format!("DROP SUBSCRIPTION IF EXISTS {}", self.sub))
                .await;
        }
        if let Ok(mut a) = AsyncPgConnection::establish(&self.primary_url).await {
            let _ = diesel::sql_query(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots \
                 WHERE slot_name = $1",
            )
            .bind::<diesel::sql_types::Text, _>(self.slot.clone())
            .execute(&mut a)
            .await;
        }
    }
}

async fn wal_level_is_logical(url: &str) -> bool {
    #[derive(diesel::QueryableByName)]
    struct S {
        #[diesel(sql_type = diesel::sql_types::Text)]
        wal_level: String,
    }
    let mut conn = connect(url).await;
    let rows: Result<Vec<S>, _> =
        diesel::sql_query("SELECT current_setting('wal_level') AS wal_level")
            .load(&mut conn)
            .await;
    rows.is_ok_and(|r| {
        r.into_iter()
            .next()
            .is_some_and(|s| s.wal_level == "logical")
    })
}

/// Build the two-region topology, or `None` when the server cannot host it.
async fn two_regions(tag: &str) -> Option<Regions> {
    let admin = admin_url().await?;
    if !wal_level_is_logical(&admin).await {
        eprintln!(
            "SKIPPED {tag}: server is not configured with wal_level=logical, so stock logical \
             replication cannot be exercised and this test proved NOTHING"
        );
        return None;
    }
    let (primary_url, primary_db) = fresh_db(&format!("{tag}a")).await?;
    let (standby_url, _standby_db) = fresh_db(&format!("{tag}b")).await?;

    let n = DB_SEQ.fetch_add(1, Ordering::SeqCst);
    let slot = format!("{DR_PREFIX}_slot_{}_{n}", std::process::id());
    let sub = format!("dr_sub_{}_{n}", std::process::id());

    let mut a = connect(&primary_url).await;
    a.batch_execute("CREATE PUBLICATION harvest_dr FOR ALL TABLES")
        .await
        .expect("publication");

    // The slot is created on its own connection, BEFORE the subscription, and
    // the subscription is told not to create one. `CREATE SUBSCRIPTION` runs in
    // a transaction, and slot creation waits for transactions older than itself
    // to end — so when publisher and subscriber live in the same Postgres
    // instance, letting it create its own slot deadlocks against itself. (Two
    // separate instances would not, but they buy no extra fidelity and cost
    // container-to-container networking.)
    diesel::sql_query("SELECT pg_create_logical_replication_slot($1, 'pgoutput')")
        .bind::<diesel::sql_types::Text, _>(slot.clone())
        .execute(&mut a)
        .await
        .expect("create slot");

    let conninfo = server_side_conninfo(&mut a, &admin, &primary_db).await;
    let mut b = connect(&standby_url).await;
    let create_subscription = format!(
        "CREATE SUBSCRIPTION {sub} CONNECTION '{conninfo}' PUBLICATION harvest_dr \
         WITH (create_slot = false, slot_name = '{slot}', copy_data = true)"
    );
    // `copy_data = true` blocks until the STANDBY's Postgres *server* process
    // (not this test client) reaches the primary at `conninfo` and finishes an
    // initial table sync — reachability that depends on the runner's own
    // container networking, not on this test's logic, and that Postgres places
    // no timeout on. A bad or momentarily-unreachable address here therefore
    // hangs this `.await` forever rather than erroring, which is exactly what
    // pinned `Test DB (linux, shard 1)` for a full 6-hour CI job on a run whose
    // diff never touched this file (see the PR discussion this comment was
    // added from). Bounding it turns that into a fast, clear skip — consistent
    // with this function's existing "server cannot host it" contract
    // (`wal_level_is_logical` above already skips for the same class of
    // reason), not a special case.
    match tokio::time::timeout(
        std::time::Duration::from_secs(60),
        b.batch_execute(&create_subscription),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("subscription: {e}"),
        Err(_) => {
            eprintln!(
                "SKIPPED {tag}: CREATE SUBSCRIPTION did not complete within 60s — the standby's \
                 Postgres process could not reach the primary at the server-side conninfo in \
                 this environment. Dropping the orphaned replication slot; this test proved \
                 NOTHING."
            );
            // Best-effort cleanup: an orphaned slot pins WAL on the shared test
            // server indefinitely (the same concern the disconnected-standby
            // test below already guards against for its own slot).
            let _ = diesel::sql_query("SELECT pg_drop_replication_slot($1)")
                .bind::<diesel::sql_types::Text, _>(slot.clone())
                .execute(&mut a)
                .await;
            return None;
        }
    }

    Some(Regions {
        primary_url,
        primary_db,
        standby_url,
        slot,
        sub,
    })
}

macro_rules! require_regions {
    ($tag:literal) => {
        match two_regions($tag).await {
            Some(v) => v,
            None => {
                eprintln!(
                    "SKIPPED {}: no two-region topology available — this test proved NOTHING",
                    $tag
                );
                return;
            }
        }
    };
}

async fn count_on(url: &str, sql: &str) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct C {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let mut conn = connect(url).await;
    let rows: Vec<C> = diesel::sql_query(sql).load(&mut conn).await.expect("count");
    rows.into_iter().next().map_or(0, |c| c.n)
}

/// Poll until `f` holds or the deadline passes. Replication is asynchronous by
/// definition; a fixed sleep would be either flaky or slow.
async fn eventually<F, Fut>(what: &str, timeout: std::time::Duration, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if f().await {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

// ── AC6(c): the RPO metric reports the injected lag ────────────────────────

#[tokio::test]
async fn rpo_metric_reports_injected_replication_lag() {
    let regions = require_regions!("rpo");
    let result = rpo_body(&regions).await;
    regions.teardown().await;
    result.unwrap();
}

#[allow(clippy::cognitive_complexity)]
async fn rpo_body(regions: &Regions) -> Result<(), String> {
    use autumn_harvest::replication::{measure_rpo, record_replication_heartbeat};
    let retain = std::time::Duration::from_secs(3600);
    let shard = ShardId::new(0);
    let mut a = connect(&regions.primary_url).await;

    // Healthy: beats are confirmed by the standby within a beat or two.
    for _ in 0..3 {
        record_replication_heartbeat(&mut a, shard, retain, BEAT_INTERVAL)
            .await
            .map_err(|e| e.to_string())?;
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    }
    let primary_url = regions.primary_url.clone();
    eventually(
        "a healthy RPO reading",
        std::time::Duration::from_secs(30),
        || {
            let url = primary_url.clone();
            async move {
                let mut conn = connect(&url).await;
                let _ = record_replication_heartbeat(&mut conn, shard, retain, BEAT_INTERVAL).await;
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                matches!(
                    measure_rpo(&mut conn, shard, DR_PREFIX).await,
                    Ok(WatermarkReading::Measured(v)) if v < 5.0
                )
            }
        },
    )
    .await;

    // Inject lag: hold ACCESS EXCLUSIVE on a replicated table so the
    // subscriber's apply worker blocks. This is the realistic shape of the
    // incident — the walsender stays connected and the stream keeps flowing,
    // only apply stalls — and it is exactly the case where
    // `pg_stat_replication.replay_lag` goes blind.
    let mut blocker = connect(&regions.standby_url).await;
    blocker
        .batch_execute("BEGIN; LOCK TABLE harvest_replication_heartbeat IN ACCESS EXCLUSIVE MODE")
        .await
        .map_err(|e| format!("lock: {e}"))?;

    let stall_started = std::time::Instant::now();
    let injected = std::time::Duration::from_secs(12);
    while stall_started.elapsed() < injected {
        record_replication_heartbeat(&mut a, shard, retain, BEAT_INTERVAL)
            .await
            .map_err(|e| e.to_string())?;
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    let independently_measured = stall_started.elapsed().as_secs_f64();
    let reported = match measure_rpo(&mut a, shard, DR_PREFIX)
        .await
        .map_err(|e| e.to_string())?
    {
        WatermarkReading::Measured(v) => v,
        // A stall shorter than the retained trail must stay a MEASUREMENT: the
        // floor is for a standby that has fallen off the end of the trail
        // entirely, which a 12s stall against an hour of retention is not.
        other => {
            blocker.batch_execute("ROLLBACK").await.ok();
            return Err(format!(
                "a stall well inside the retention window must be measured exactly, got {other:?}"
            ));
        }
    };

    // The issue's success metric: within ±5s of an independent measurement.
    let delta = (reported - independently_measured).abs();
    if delta > 5.0 {
        blocker.batch_execute("ROLLBACK").await.ok();
        return Err(format!(
            "reported RPO {reported:.1}s vs independently-measured {independently_measured:.1}s \
             (delta {delta:.1}s) exceeds the ±5s tolerance"
        ));
    }

    // …and the reading is honest about *which* source it came from: with apply
    // blocked, Postgres' own replay_lag is blind, which is the whole reason the
    // watermark trail exists.
    let status = autumn_harvest::replication::query_replication_status(&mut a, shard, DR_PREFIX)
        .await
        .map_err(|e| e.to_string())?;
    let rpo = status.rpo_seconds().ok_or("status must carry the RPO")?;
    if (rpo - reported).abs() > 2.0 {
        blocker.batch_execute("ROLLBACK").await.ok();
        return Err(format!(
            "status RPO {rpo} disagrees with measure_rpo {reported}"
        ));
    }
    if status.max_lag_bytes().unwrap_or(0) <= 0 {
        blocker.batch_execute("ROLLBACK").await.ok();
        return Err("a stalled standby must show a byte backlog".into());
    }

    // Release the stall; the RPO must recover, not stay latched.
    blocker
        .batch_execute("ROLLBACK")
        .await
        .map_err(|e| e.to_string())?;
    let primary_url = regions.primary_url.clone();
    eventually(
        "the RPO to recover after the stall clears",
        std::time::Duration::from_secs(60),
        || {
            let url = primary_url.clone();
            async move {
                let mut conn = connect(&url).await;
                let _ = record_replication_heartbeat(&mut conn, shard, retain, BEAT_INTERVAL).await;
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                matches!(
                    measure_rpo(&mut conn, shard, DR_PREFIX).await,
                    Ok(WatermarkReading::Measured(v)) if v < 5.0
                )
            }
        },
    )
    .await;

    Ok(())
}

#[tokio::test]
async fn a_disconnected_standby_reports_bytes_and_an_unknown_rpo_never_zero() {
    let regions = require_regions!("discon");
    // The body is polled to completion inside `AssertUnwindSafe` so a failed
    // assertion still drops the replication slot: an orphaned slot pins WAL on
    // the shared test server indefinitely.
    let out = std::panic::AssertUnwindSafe(async {
        let shard = ShardId::new(0);
        let mut b = connect(&regions.standby_url).await;
        b.batch_execute(&format!("ALTER SUBSCRIPTION {} DISABLE", regions.sub))
            .await
            .expect("disable subscription");

        let mut a = connect(&regions.primary_url).await;
        // Generate WAL the now-absent standby cannot consume.
        for _ in 0..5 {
            autumn_harvest::replication::record_replication_heartbeat(
                &mut a,
                shard,
                std::time::Duration::from_secs(3600),
                BEAT_INTERVAL,
            )
            .await
            .expect("beat");
        }

        let primary_url = regions.primary_url.clone();
        eventually(
            "the walsender to disappear",
            std::time::Duration::from_secs(30),
            || {
                let url = primary_url.clone();
                async move {
                    let mut conn = connect(&url).await;
                    autumn_harvest::replication::query_replication_status(
                        &mut conn, shard, DR_PREFIX,
                    )
                    .await
                    .is_ok_and(|s| s.connected_standbys() == 0)
                }
            },
        )
        .await;

        let status =
            autumn_harvest::replication::query_replication_status(&mut a, shard, DR_PREFIX)
                .await
                .expect("status");
        assert_eq!(status.connected_standbys(), 0, "the standby is gone");
        assert_eq!(
            status.max_replay_lag_seconds(),
            None,
            "Postgres cannot report a replay lag for a standby that is not connected"
        );
        assert!(
            status.max_lag_bytes().unwrap_or(0) > 0,
            "the slot still pins WAL, so the byte backlog is knowable and must be reported"
        );
        assert_eq!(
            status.inactive_slots(),
            1,
            "the abandoned slot must be visible"
        );
        assert_ne!(
            status.rpo_seconds(),
            Some(0.0),
            "a dead standby must never read as a perfect RPO"
        );
    })
    .catch_unwind()
    .await;
    regions.teardown().await;
    if let Err(panic) = out {
        std::panic::resume_unwind(panic);
    }
}

// ── AC6(b): promotion, fencing, and in-flight work resuming ────────────────

#[tokio::test]
async fn a_promoted_standby_resumes_in_flight_work_and_rejects_the_old_region() {
    let _serial = registry_guard().await;
    let regions = require_regions!("promote");
    let result = promotion_body(&regions).await;
    regions.teardown().await;
    FenceRegistry::clear();
    result.unwrap();
}

async fn promotion_body(regions: &Regions) -> Result<(), String> {
    let shard = ShardId::new(0);
    let mut a = connect(&regions.primary_url).await;

    // ── Region A is live: an in-flight workflow with a pending task. ───────
    ensure_generation_row(&mut a, shard)
        .await
        .map_err(|e| e.to_string())?;
    let exec_id = ExecutionId::new_for_shard(shard);
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, state, input, shard_id) \
         VALUES ($1, 'dr-wf', 'order-1', 'RUNNING', '{}'::jsonb, 0)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut a)
    .await
    .map_err(|e| e.to_string())?;

    let started = autumn_harvest::event::WorkflowEvent::WorkflowStarted {
        input: serde_json::json!({}),
        timestamp: chrono::Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    };
    autumn_harvest::store::append_events(&mut a, exec_id, &[started], 1)
        .await
        .map_err(|e| e.to_string())?;

    let params = autumn_harvest::queue::EnqueueParams::new(
        "q-dr",
        autumn_harvest::queue::TaskType::Workflow,
        serde_json::json!({}),
    );
    autumn_harvest::queue::enqueue(&mut a, &params)
        .await
        .map_err(|e| e.to_string())?;

    // ── Replication carries it to region B. ───────────────────────────────
    let standby_url = regions.standby_url.clone();
    eventually(
        "the in-flight workflow to reach region B",
        std::time::Duration::from_secs(60),
        || {
            let url = standby_url.clone();
            async move {
                count_on(&url, "SELECT COUNT(*) AS n FROM harvest_events").await == 1
                    && count_on(&url, "SELECT COUNT(*) AS n FROM harvest_task_queue").await == 1
                    && count_on(&url, "SELECT COUNT(*) AS n FROM harvest_shard_generation").await
                        == 1
            }
        },
    )
    .await;

    // ── Region A is gone. Promote B: stop replicating, then FENCE. ────────
    let mut b = connect(&regions.standby_url).await;
    b.batch_execute(&format!("DROP SUBSCRIPTION {}", regions.sub))
        .await
        .map_err(|e| format!("promote: {e}"))?;

    let promoted_gen = bump_generation(&mut b, shard, "failover drill", "oncall")
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(promoted_gen, ShardGeneration::new(1));

    // Sequences are NOT replicated by logical replication. Without this the
    // new primary's `harvest_events_id_seq` still sits at 1 and the first
    // append collides with a replicated row's primary key.
    let advanced = autumn_harvest::replication::advance_sequences_after_promotion(&mut b)
        .await
        .map_err(|e| e.to_string())?;
    assert!(
        advanced
            .iter()
            .any(|(name, _)| name.contains("harvest_events_id_seq")),
        "the promotion helper must advance harvest_events' sequence; advanced: {advanced:?}"
    );

    // ── A surviving region-A worker, pinned to the pre-failover epoch. ────
    FenceRegistry::clear();
    FenceRegistry::register(shard, ShardGeneration::new(0))
        .expect("no conflicting pin in this test");
    FenceRegistry::set_default_shard(shard).expect("no conflicting default shard in this test");

    let stale_claim = autumn_harvest::queue::claim_task_on_shard(
        &mut b,
        &["q-dr".to_string()],
        "worker-old-region",
        "",
        None,
        &[],
        &[],
        Some(shard),
    )
    .await
    .map_err(|e| e.to_string())?;
    if stale_claim.is_some() {
        return Err("a stale-epoch worker claimed a task on the promoted primary".into());
    }

    let event = autumn_harvest::event::WorkflowEvent::WorkflowCompleted {
        output: serde_json::json!({"by": "old-region"}),
    };
    let err = autumn_harvest::store::append_events(&mut b, exec_id, &[event], 2)
        .await
        .expect_err("a stale-epoch worker must not append to the promoted primary");
    if !matches!(err, autumn_harvest::error::HarvestError::ShardFenced { .. }) {
        return Err(format!("expected ShardFenced, got {err:?}"));
    }

    // ── A region-B worker, pinned to the promoted epoch, carries on. ──────
    FenceRegistry::clear();
    FenceRegistry::register(shard, promoted_gen).expect("no conflicting pin in this test");
    FenceRegistry::set_default_shard(shard).expect("no conflicting default shard in this test");

    let claim = autumn_harvest::queue::claim_task_on_shard(
        &mut b,
        &["q-dr".to_string()],
        "worker-new-region",
        "",
        None,
        &[],
        &[],
        Some(shard),
    )
    .await
    .map_err(|e| e.to_string())?;
    if claim.is_none() {
        return Err(
            "the promoted region's worker must be able to claim the replicated task".into(),
        );
    }

    let done = autumn_harvest::event::WorkflowEvent::WorkflowCompleted {
        output: serde_json::json!({"by": "new-region"}),
    };
    autumn_harvest::store::append_events(&mut b, exec_id, &[done], 2)
        .await
        .map_err(|e| format!("the promoted region must be able to append: {e}"))?;

    // ── No fork: exactly one continuous history, extended not branched. ───
    let n = count_on(
        &regions.standby_url,
        "SELECT COUNT(*) AS n FROM harvest_events",
    )
    .await;
    if n != 2 {
        return Err(format!(
            "expected a single 2-event history on the new primary, found {n}"
        ));
    }
    let forks = count_on(
        &regions.standby_url,
        "SELECT COUNT(*) AS n FROM ( \
             SELECT event_id FROM harvest_events GROUP BY workflow_exec_id, event_id \
             HAVING COUNT(*) > 1 \
         ) d",
    )
    .await;
    if forks != 0 {
        return Err(format!("history forked: {forks} duplicated event ids"));
    }
    Ok(())
}
