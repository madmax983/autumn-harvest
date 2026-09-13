#![cfg(feature = "db")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

//! DB-backed integration tests for payload-codec key rotation and the lazy
//! re-encryption sweep — issue #948.
//!
//! # AC coverage map
//!
//! - **AC1** (`kid` in the envelope; kid-less rows resolve to the legacy key
//!   id) — [`a_kidless_pre_upgrade_row_is_swept_onto_the_active_key`] proves the
//!   stored-bytes half end to end; the envelope shape itself is pinned by
//!   `payload_codec.rs`'s own unit tests.
//! - **AC2** (a flip takes effect for all new writes, no restart window) —
//!   [`new_writes_land_under_the_new_key_immediately_after_a_flip`].
//! - **AC3** (decode resolves any registered key; mixed histories replay) —
//!   [`a_mixed_key_history_loads_transparently`].
//! - **AC4** (batched, rate-limitable, idempotent, resumable via a durable
//!   per-shard cursor) — [`the_sweep_is_batched_and_resumes_from_its_cursor`],
//!   [`a_zero_batch_size_disables_the_sweep`],
//!   [`re_running_the_sweep_rewrites_nothing`],
//!   [`flipping_the_active_key_starts_a_fresh_pass`].
//! - **AC5** (replay fidelity across the in-place mutation) —
//!   [`replay_fidelity_is_byte_identical_across_a_sweep`], plus
//!   [`a_stale_read_can_never_overwrite_a_committed_erasure`] for the CAS guard
//!   that keeps exception #3 from resurrecting what exception #2 destroyed.
//! - **AC6** (fail-closed retirement gate) —
//!   [`retirement_is_refused_while_rows_remain_and_succeeds_at_zero`] and
//!   [`retirement_fails_closed_on_an_unreachable_shard`].
//! - **AC7** (per-shard rows-remaining per key id; the sweep metric) —
//!   [`rotation_progress_reports_rows_per_key_id_and_the_cursor`] and
//!   [`the_sweep_records_the_reencrypted_metric`]. The HTTP route itself is
//!   covered in the plugin crate's `codec_rotation_admin_integration.rs`.
//! - **AC8** (composition with offload / erasure) —
//!   [`offload_envelopes_and_tombstones_survive_a_sweep_untouched`].
//! - **Issue #1251** (a sweep batch cannot commit a row under a key retired
//!   mid-batch) —
//!   [`a_batch_pinned_to_a_key_blocks_its_retirement_through_a_double_rotation`].
//! - **Issue #1257** (`write_cursor` is a compare-and-swap; DR fencing on the
//!   cursor writers) — [`a_stale_cursor_write_cannot_overwrite_newer_progress`],
//!   [`a_stale_rewind_cannot_decrease_rows_reencrypted`],
//!   [`a_deliberate_rewind_to_zero_always_applies`], and
//!   [`a_new_active_key_always_starts_a_fresh_pass`] cover the CAS guard.
//!   `cross_region_dr_tests.rs`'s `a_fenced_worker_cannot_advance_the_rotation_cursor`
//!   and `a_fenced_sweep_that_converts_nothing_still_fails_closed` cover the
//!   fencing.
//!
//! # Issue #1244: the two structural fleet-wide preconditions
//!
//! - **Durable write fence, replacing operator attestation** —
//!   [`retirement_waits_for_the_staleness_window_even_at_a_zero_census`],
//!   [`retirement_via_the_structural_gate_refuses_a_purely_local_flip`]
//!   (the "another live writer" hazard), and
//!   [`retirement_recheck_catches_a_row_that_commits_after_the_first_zero_census`]
//!   (the "uncommitted append" hazard).
//! - **Reader-capability handshake before activation** —
//!   [`activation_is_refused_while_a_live_worker_cannot_read_the_keyed_envelope`],
//!   [`activation_succeeds_once_every_live_worker_advertises_the_keyed_envelope`],
//!   [`activation_ignores_a_worker_whose_heartbeat_is_stale`].
//! - **Bounded staleness** —
//!   [`refresh_active_codec_key_picks_up_a_fleet_wide_activation_from_another_process`].
//!
//! Runs against `HARVEST_TEST_DATABASE_URL` when set (each test gets its own
//! throwaway database, because the rotation census is shard-wide by design),
//! otherwise against a per-test Postgres container.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::codec_rotation::{
    FleetWriteFence, activate_codec_key, load_shard_rotation_progress,
    load_shard_rotation_progress_against, refresh_active_codec_key, retire_codec_key,
    sweep_codec_reencryption_once, write_cursor,
};
use autumn_harvest::erase::erasure_tombstone;
use autumn_harvest::error::HarvestError;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::payload_codec::{
    CODEC_ENVELOPE_KID_KEY, CODEC_LEGACY_KEY_ID, CodecError, PayloadCodec, PayloadCodecs,
};
use autumn_harvest::shard::ShardedDbPool;
use autumn_harvest::store;
use autumn_harvest::telemetry::{MetricsRecorder, NoOpMetrics};
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest::types::{ExecutionId, ShardId};

use chrono::Utc;
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

// ── codecs ───────────────────────────────────────────────────────────────────

/// Two instances differ only in key material — exactly the shape rotation has
/// to cope with, and exactly why a key id cannot live in `codec_id`.
#[derive(Debug)]
struct XorCodec(u8);

impl PayloadCodec for XorCodec {
    fn codec_id(&self) -> &'static str {
        "xor"
    }
    fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(raw.iter().map(|b| b ^ self.0).collect())
    }
    fn decode(&self, encoded: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(encoded.iter().map(|b| b ^ self.0).collect())
    }
}

/// Counts `record_codec_reencrypted` calls so AC7's metric can be asserted
/// without a Prometheus scrape.
#[derive(Default)]
struct CountingMetrics {
    reencrypted: Mutex<Vec<(String, u64)>>,
}

impl MetricsRecorder for CountingMetrics {
    fn record_codec_reencrypted(&self, shard: &str, count: u64) {
        self.reencrypted
            .lock()
            .unwrap()
            .push((shard.to_string(), count));
    }
}

// ── harness ──────────────────────────────────────────────────────────────────

/// A migrated Postgres that no other test shares.
///
/// The rotation census counts every `harvest_events` row on the shard — that is
/// the point of it — so these tests cannot share a database with anything else.
async fn setup_isolated_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        let db_name = format!("harvest_codec_rot_{}", Uuid::new_v4().simple());
        let mut admin = <AsyncPgConnection as AsyncConnection>::establish(&admin_url)
            .await
            .expect("HARVEST_TEST_DATABASE_URL must be reachable");
        admin
            .batch_execute(&format!("CREATE DATABASE \"{db_name}\""))
            .await
            .expect("create throwaway database");
        let url = swap_database(&admin_url, &db_name);
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
            .await
            .expect("connect to throwaway database");
        conn.batch_execute(autumn_harvest::full_migrations_sql())
            .await
            .expect("apply migrations");
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(autumn_harvest::full_migrations_sql().as_bytes().to_vec())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("get host");
    let port = container.get_host_port_ipv4(5432).await.expect("get port");
    (
        format!("postgres://postgres:postgres@{host}:{port}/postgres"),
        Some(container),
    )
}

/// Replace the database component of a `postgres://` URL.
fn swap_database(url: &str, db_name: &str) -> String {
    let (base, _) = url.split_once('?').unwrap_or((url, ""));
    let cut = base.rfind('/').expect("a postgres URL has a database path");
    format!("{}/{db_name}", &base[..cut])
}

fn build_pool(url: &str) -> autumn_harvest::worker::DbPool {
    let manager =
        diesel_async::pooled_connection::AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            url,
        );
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("build pool")
}

async fn connect(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect")
}

async fn insert_execution(conn: &mut AsyncPgConnection, name: &str) -> ExecutionId {
    use autumn_harvest::schema::harvest_workflow_executions;
    let exec_id = ExecutionId::new();
    let row = NewWorkflowExecution {
        quota_key: None,
        continued_from_exec_id: None,
        first_exec_id: None,
        id: exec_id.as_uuid(),
        workflow_name: name,
        workflow_id: &Uuid::new_v4().to_string(),
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: json!({}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        deadline_at: None,
        chain_execution_timeout: None,
        chain_deadline_at: None,
        memo: None,
        search_attrs: None,
        assigned_build_id: None,
        parent_close_policy: None,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,
        sla: None,
        sla_deadline_at: None,
        schedule_id: None,
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        origin: None,
        completion_callbacks: None,
        start_source: None,
        start_source_ref: None,
        started_by: None,
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("insert execution");
    exec_id
}

/// Append `events` encoded under `key_id`, restoring the previously active key
/// so a test can compose a genuinely mixed-key history.
async fn append_under_key(
    conn: &mut AsyncPgConnection,
    codecs: &PayloadCodecs,
    exec_id: ExecutionId,
    key_id: &str,
    start_id: i32,
    events: &[WorkflowEvent],
) {
    let restore = codecs.active_key_id();
    codecs.set_active_key(key_id).expect("activate for fixture");
    store::append_events_with_codecs(conn, exec_id, events, start_id, codecs)
        .await
        .expect("append events");
    codecs.set_active_key(&restore).expect("restore active key");
}

fn started(input: Value) -> WorkflowEvent {
    WorkflowEvent::WorkflowStarted {
        input,
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }
}

const fn completed(output: Value) -> WorkflowEvent {
    WorkflowEvent::WorkflowCompleted { output }
}

/// A registry holding `k1` (outgoing) and `k2` (incoming), active on `k1`.
fn two_key_registry() -> PayloadCodecs {
    let codecs = PayloadCodecs::default();
    codecs
        .register_key("k1", Arc::new(XorCodec(0x11)))
        .expect("register k1");
    codecs
        .register_key("k2", Arc::new(XorCodec(0x22)))
        .expect("register k2");
    codecs.set_active_key("k1").expect("activate k1");
    codecs
}

async fn raw_event_data(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> Vec<Value> {
    use autumn_harvest::schema::harvest_events;
    harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .order(harvest_events::event_id.asc())
        .select(harvest_events::event_data)
        .load::<Value>(conn)
        .await
        .expect("load raw events")
}

async fn cursor_row_count(conn: &mut AsyncPgConnection) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let row: Count =
        diesel::sql_query("SELECT COUNT(*)::BIGINT AS n FROM harvest_codec_rotation_cursor")
            .get_result(conn)
            .await
            .expect("count cursor rows");
    row.n
}

/// A shard's raw cursor row, unfiltered by active key.
///
/// [`load_shard_rotation_progress`] only reports a cursor that matches the
/// caller's active key. A CAS test needs the stored row regardless of which
/// key it names, so it reads the table directly instead.
struct CursorSnapshot {
    active_key_id: String,
    last_event_id: i64,
    rows_reencrypted: i64,
    unresolved_rows: i64,
}

async fn cursor_row(conn: &mut AsyncPgConnection, shard_id: i32) -> Option<CursorSnapshot> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        active_key_id: String,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        last_event_id: i64,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        rows_reencrypted: i64,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        unresolved_rows: i64,
    }
    let rows: Vec<Row> = diesel::sql_query(
        "SELECT active_key_id, last_event_id, rows_reencrypted, unresolved_rows \
         FROM harvest_codec_rotation_cursor WHERE shard_id = $1",
    )
    .bind::<diesel::sql_types::Integer, _>(shard_id)
    .load(conn)
    .await
    .expect("load cursor row");
    rows.into_iter().next().map(|r| CursorSnapshot {
        active_key_id: r.active_key_id,
        last_event_id: r.last_event_id,
        rows_reencrypted: r.rows_reencrypted,
        unresolved_rows: r.unresolved_rows,
    })
}

/// `harvest_codec_key_state.state` for `key_id`, or `None` when no row exists.
async fn key_state(conn: &mut AsyncPgConnection, key_id: &str) -> Option<String> {
    #[derive(diesel::QueryableByName)]
    struct State {
        #[diesel(sql_type = diesel::sql_types::Text)]
        state: String,
    }
    let rows: Vec<State> =
        diesel::sql_query("SELECT state FROM harvest_codec_key_state WHERE key_id = $1")
            .bind::<diesel::sql_types::Text, _>(key_id)
            .load(conn)
            .await
            .expect("load key state");
    rows.into_iter().next().map(|r| r.state)
}

fn kid_of(event_data: &Value, field: &str) -> Option<String> {
    event_data["data"][field][CODEC_ENVELOPE_KID_KEY]
        .as_str()
        .map(str::to_string)
}

// ── AC1 / AC4: the sweep converts stored history ─────────────────────────────

#[tokio::test]
async fn a_kidless_pre_upgrade_row_is_swept_onto_the_active_key() {
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;

    // A pre-#948 deployment: one codec, registered under the legacy key id, so
    // its envelopes carry no `kid` at all.
    let codecs = PayloadCodecs::default();
    codecs
        .register_key(CODEC_LEGACY_KEY_ID, Arc::new(XorCodec(0x11)))
        .expect("register legacy");
    let exec_id = insert_execution(&mut conn, "rotate_me").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        CODEC_LEGACY_KEY_ID,
        0,
        &[started(json!({"user": "alice"}))],
    )
    .await;
    let before = raw_event_data(&mut conn, exec_id).await;
    assert_eq!(
        kid_of(&before[0], "input"),
        None,
        "the fixture really is a kid-less pre-upgrade row"
    );

    // Rotate onto k2 and sweep.
    codecs
        .register_key("k2", Arc::new(XorCodec(0x22)))
        .expect("register k2");
    codecs.set_active_key("k2").expect("activate k2");
    let swept = sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
        .await
        .expect("sweep");

    assert_eq!(swept, 1);
    let after = raw_event_data(&mut conn, exec_id).await;
    assert_eq!(kid_of(&after[0], "input"), Some("k2".to_string()));
    // And the plaintext survived the trip.
    let history = store::load_history_with_codecs(&mut conn, exec_id, &codecs)
        .await
        .expect("load history");
    match &history.events[0] {
        WorkflowEvent::WorkflowStarted { input, .. } => {
            assert_eq!(*input, json!({"user": "alice"}));
        }
        other => panic!("unexpected event: {other:?}"),
    }
}

#[tokio::test]
async fn new_writes_land_under_the_new_key_immediately_after_a_flip() {
    // AC2: no restart-ordering window. The registry clone handed to the write
    // path was taken BEFORE the flip.
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let captured_at_boot = codecs.clone();

    let exec_id = insert_execution(&mut conn, "flip").await;
    store::append_events_with_codecs(
        &mut conn,
        exec_id,
        &[started(json!({"n": 1}))],
        0,
        &captured_at_boot,
    )
    .await
    .expect("append pre-flip");

    codecs.set_active_key("k2").expect("flip to k2");

    store::append_events_with_codecs(
        &mut conn,
        exec_id,
        &[completed(json!({"n": 2}))],
        1,
        &captured_at_boot,
    )
    .await
    .expect("append post-flip");

    let rows = raw_event_data(&mut conn, exec_id).await;
    assert_eq!(kid_of(&rows[0], "input"), Some("k1".to_string()));
    assert_eq!(
        kid_of(&rows[1], "output"),
        Some("k2".to_string()),
        "a write through a pre-flip clone must still use the new key"
    );
}

#[tokio::test]
async fn a_mixed_key_history_loads_transparently() {
    // AC3.
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "mixed").await;

    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"first": true}))],
    )
    .await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k2",
        1,
        &[completed(json!({"second": true}))],
    )
    .await;

    let history = store::load_history_with_codecs(&mut conn, exec_id, &codecs)
        .await
        .expect("mixed-key history must load");
    assert_eq!(history.events.len(), 2);
    match (&history.events[0], &history.events[1]) {
        (
            WorkflowEvent::WorkflowStarted { input, .. },
            WorkflowEvent::WorkflowCompleted { output, .. },
        ) => {
            assert_eq!(*input, json!({"first": true}));
            assert_eq!(*output, json!({"second": true}));
        }
        other => panic!("unexpected history: {other:?}"),
    }
}

#[tokio::test]
async fn the_sweep_is_batched_and_resumes_from_its_cursor() {
    // AC4: bounded per call, and the durable cursor makes the next call pick up
    // exactly where the last one stopped.
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "batched").await;
    let events: Vec<WorkflowEvent> = (0..5).map(|i| started(json!({ "i": i }))).collect();
    append_under_key(&mut conn, &codecs, exec_id, "k1", 0, &events).await;
    codecs.set_active_key("k2").expect("flip");

    let first = sweep_codec_reencryption_once(&mut conn, 0, &codecs, 2, &NoOpMetrics)
        .await
        .expect("batch 1");
    assert_eq!(first, 2, "the batch limit really bounds the work");

    let progress = load_shard_rotation_progress(&mut conn, 0, &codecs)
        .await
        .expect("progress");
    let cursor = progress.cursor.expect("a cursor row exists after a batch");
    assert!(cursor.last_event_id > 0);
    assert_eq!(cursor.rows_reencrypted, 2);
    assert_eq!(progress.rows_by_key_id.get("k1"), Some(&3));
    assert_eq!(progress.rows_by_key_id.get("k2"), Some(&2));

    let second = sweep_codec_reencryption_once(&mut conn, 0, &codecs, 2, &NoOpMetrics)
        .await
        .expect("batch 2");
    let third = sweep_codec_reencryption_once(&mut conn, 0, &codecs, 2, &NoOpMetrics)
        .await
        .expect("batch 3");
    assert_eq!(
        second + third,
        3,
        "the remaining rows convert across batches"
    );

    let done = load_shard_rotation_progress(&mut conn, 0, &codecs)
        .await
        .expect("progress");
    assert_eq!(done.rows_remaining(), 0);
    assert_eq!(done.rows_by_key_id.get("k2"), Some(&5));
}

#[tokio::test]
async fn a_zero_batch_size_disables_the_sweep() {
    // AC4: rate-limitable, down to "off", with no redeploy.
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "throttled").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"a": 1}))],
    )
    .await;
    codecs.set_active_key("k2").expect("flip");

    let swept = sweep_codec_reencryption_once(&mut conn, 0, &codecs, 0, &NoOpMetrics)
        .await
        .expect("sweep");

    assert_eq!(swept, 0);
    let rows = raw_event_data(&mut conn, exec_id).await;
    assert_eq!(kid_of(&rows[0], "input"), Some("k1".to_string()));
}

#[tokio::test]
async fn a_converted_shard_stops_rewriting_its_cursor_every_tick() {
    // Codex round 9 (P2). On a fully-converted shard the batch query returns
    // nothing, the pass is already stamped complete, and the computed cursor is
    // byte-identical to the stored one -- but the sweep upserted it anyway on
    // every scanner tick. For a deployment that simply keeps a keyed codec
    // configured that is unbounded WAL and dead tuples per shard, and an
    // `updated_at` that reads as freshly active while no work is happening.
    //
    // `updated_at` is the observable: it moves only when the row is actually
    // written, so it is the thing to pin.
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "cursor_churn").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"a": 1}))],
    )
    .await;
    codecs.set_active_key("k2").expect("flip");

    // First pass converts the row and completes.
    sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
        .await
        .expect("first sweep");
    let first = load_shard_rotation_progress(&mut conn, 0, &codecs)
        .await
        .expect("progress")
        .cursor
        .expect("cursor written");
    assert!(
        first.completed_at.is_some(),
        "a pass that converted everything must be stamped complete"
    );

    // A later tick over a converted shard must not touch the row at all.
    sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
        .await
        .expect("idle sweep");
    let second = load_shard_rotation_progress(&mut conn, 0, &codecs)
        .await
        .expect("progress")
        .cursor
        .expect("cursor still there");

    assert_eq!(
        first.updated_at, second.updated_at,
        "an idle tick on a converted shard must not rewrite the cursor; \
         updated_at moving means the row was upserted for nothing"
    );
    assert_eq!(first.last_event_id, second.last_event_id);
    assert_eq!(first.rows_reencrypted, second.rows_reencrypted);
}

#[tokio::test]
async fn re_running_the_sweep_rewrites_nothing() {
    // AC4: idempotent.
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "idempotent").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"a": 1}))],
    )
    .await;
    codecs.set_active_key("k2").expect("flip");

    assert_eq!(
        sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
            .await
            .expect("sweep 1"),
        1
    );
    let after_first = raw_event_data(&mut conn, exec_id).await;

    assert_eq!(
        sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
            .await
            .expect("sweep 2"),
        0
    );
    assert_eq!(
        raw_event_data(&mut conn, exec_id).await,
        after_first,
        "a re-run leaves the stored bytes byte-identical"
    );
}

#[tokio::test]
async fn flipping_the_active_key_starts_a_fresh_pass() {
    // AC4: the cursor is keyed on (shard, active_key_id), so a second rotation
    // rescans from the start with no reset step to forget.
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    codecs
        .register_key("k3", Arc::new(XorCodec(0x33)))
        .expect("register k3");
    let exec_id = insert_execution(&mut conn, "twice").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"a": 1}))],
    )
    .await;

    codecs.set_active_key("k2").expect("flip to k2");
    assert_eq!(
        sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
            .await
            .expect("sweep onto k2"),
        1
    );

    codecs.set_active_key("k3").expect("flip to k3");
    assert_eq!(
        sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
            .await
            .expect("sweep onto k3"),
        1,
        "the second rotation must rescan from the start of the shard"
    );
    let rows = raw_event_data(&mut conn, exec_id).await;
    assert_eq!(kid_of(&rows[0], "input"), Some("k3".to_string()));
}

// ── AC5: the fidelity proof behind sanctioned exception #3 ───────────────────

#[tokio::test]
async fn replay_fidelity_is_byte_identical_across_a_sweep() {
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "fidelity_workflow").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[
            started(json!({"user": "alice", "amounts": [1, 2, 3]})),
            completed(json!({"ok": true, "nested": {"k": null}})),
        ],
    )
    .await;

    let before = store::load_history_with_codecs(&mut conn, exec_id, &codecs)
        .await
        .expect("history before");
    let before_json = serde_json::to_string(&before.events).expect("serialize before");
    let report_before = WorkflowReplayer::new()
        .register_fn("fidelity_workflow", |_ctx, input| {
            Box::pin(async move { Ok(json!({"ok": true, "nested": {"k": null}, "echo": input})) })
        })
        .replay_from_events(before.events.clone())
        .await;
    assert!(
        matches!(report_before.status, ReplayStatus::ReplaySucceeded),
        "pre-sweep replay must succeed:\n{report_before}"
    );

    codecs.set_active_key("k2").expect("flip");
    assert_eq!(
        sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
            .await
            .expect("sweep"),
        2,
        "the sweep really did rewrite the stored bytes"
    );

    let after = store::load_history_with_codecs(&mut conn, exec_id, &codecs)
        .await
        .expect("history after");
    let after_json = serde_json::to_string(&after.events).expect("serialize after");
    assert_eq!(
        before_json, after_json,
        "the DECODED history must be byte-identical across the in-place mutation"
    );

    let report_after = WorkflowReplayer::new()
        .register_fn("fidelity_workflow", |_ctx, input| {
            Box::pin(async move { Ok(json!({"ok": true, "nested": {"k": null}, "echo": input})) })
        })
        .replay_from_events(after.events)
        .await;
    assert!(
        matches!(report_after.status, ReplayStatus::ReplaySucceeded),
        "post-sweep replay must succeed:\n{report_after}"
    );
}

// ── issue #1243: the production start path must honor a builder-configured
// codec, not the identity default ────────────────────────────────────────────

#[tokio::test]
async fn a_builder_configured_codec_encrypts_the_start_input_and_replay_round_trips_it() {
    // Drives the real production start entry point
    // (`execution::start_or_load_workflow_execution_collect_with_codecs`).
    // The codec is configured the way an embedder actually configures one,
    // via `HarvestBuilder::payload_codec_key`. Other tests in this file call
    // `store::append_events_with_codecs` directly instead.
    // `WorkflowStarted.input` is the first event of every execution; before
    // issue #1243 it always went through the identity registry.
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;

    let builder = autumn_harvest::HarvestBuilder::new().payload_codec_key("k1", XorCodec(0x11));
    let codecs = builder.payload_codecs().clone();

    let exec_id = ExecutionId::new();
    let params = autumn_harvest::execution::StartWorkflowParams {
        workflow_name: "codec_boundary_wf",
        workflow_id: "codec-boundary-1",
        exec_id,
        input: json!({"ssn": "111-22-3333"}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: autumn_harvest::types::WorkflowIdReusePolicy::AllowDuplicate,
        conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        inherited_chain_deadline_at: None,
        concurrency_key: None,
        concurrency_limit: None,
        concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
        priority: autumn_harvest::types::Priority::default(),
        max_workflow_input_bytes: 0,
        start_at: None,
        delay: None,
        max_workflow_start_delay: None,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,
        sla: None,
        schedule_id: None,
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        max_workflow_attempts_ceiling: None,
        origin: None,
        completion_callbacks: None,
        start_source: autumn_harvest::StartSource::Api,
        start_source_ref: None,
        started_by: None,
    };

    autumn_harvest::execution::start_or_load_workflow_execution_collect_with_codecs(
        &mut conn, params, false, false, None, None, &codecs,
    )
    .await
    .expect("start with a configured codec");

    // Ciphertext on disk: the raw row must carry a keyed envelope, not plaintext.
    let raw = raw_event_data(&mut conn, exec_id).await;
    assert_eq!(
        kid_of(&raw[0], "input"),
        Some("k1".to_string()),
        "WorkflowStarted.input must be a keyed codec envelope on disk: {:?}",
        raw[0]
    );

    // Replay round-trips: decoding through the same registry recovers the input.
    let history = store::load_history_with_codecs(&mut conn, exec_id, &codecs)
        .await
        .expect("load history");
    match &history.events[0] {
        WorkflowEvent::WorkflowStarted { input, .. } => {
            assert_eq!(*input, json!({"ssn": "111-22-3333"}));
        }
        other => panic!("unexpected event: {other:?}"),
    }
    let report = WorkflowReplayer::new()
        .register_fn("codec_boundary_wf", |_ctx, input| {
            Box::pin(async move { Ok(input) })
        })
        .replay_from_events(history.events)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "replay must succeed decoding through the configured registry:\n{report}"
    );
}

// ── issue #1243 review: continue-as-new must not double-encode a carried
// `last_completion_result` ──────────────────────────────────────────────────

#[tokio::test]
async fn continue_as_new_decodes_the_carried_codec_envelope_before_reencoding_it() {
    // `persist_workflow_continue_as_new` forwards the predecessor's STORED
    // `last_completion_result` verbatim (issue #524), to preserve scheduled
    // carryover across a fork without re-resolving it. Under a real codec that
    // stored value is already a ciphertext envelope. Encoding it again on the
    // successor's write would wrap ciphertext in ciphertext. Replay would
    // then decode only the outer layer and hand workflow code a codec
    // envelope instead of the real prior output.
    use std::time::Duration;

    use autumn_harvest::models::TaskQueueItem;
    use autumn_harvest::queue::{self, EnqueueParams, TaskType};
    use autumn_harvest::schema::{harvest_task_queue, harvest_workflow_executions};
    use autumn_harvest::worker::{
        HandlerRegistry, WorkflowTaskPersistence, persist_workflow_continue_as_new,
    };

    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;

    let codecs = PayloadCodecs::default();
    codecs
        .register_key("k1", Arc::new(XorCodec(0x11)))
        .expect("register k1");
    codecs.set_active_key("k1").expect("activate k1");

    let secret_output = json!({"secret": "prior-output"});
    let exec_id = insert_execution(&mut conn, "cx1243_continue_as_new").await;
    store::append_events_with_codecs(
        &mut conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: json!({}),
            timestamp: Utc::now(),
            last_completion_result: Some(secret_output.clone()),
            last_error: None,
            scheduled_time: None,
        }],
        0,
        &codecs,
    )
    .await
    .expect("append predecessor WorkflowStarted");

    let mut enqueue = EnqueueParams::new("default", TaskType::Workflow, json!({}));
    enqueue.workflow_exec_id = Some(exec_id.as_uuid());
    enqueue.scheduled_at = Utc::now() - chrono::Duration::seconds(5);
    queue::enqueue(&mut conn, &enqueue)
        .await
        .expect("enqueue task");
    diesel::update(
        harvest_task_queue::table
            .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid()))),
    )
    .set((
        harvest_task_queue::state.eq("RUNNING"),
        harvest_task_queue::worker_id.eq(Some("worker-a")),
        harvest_task_queue::started_at.eq(Some(Utc::now())),
    ))
    .execute(&mut conn)
    .await
    .expect("claim task");
    let task = harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
        .select(TaskQueueItem::as_select())
        .first(&mut conn)
        .await
        .expect("load claimed task");
    let execution = harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(autumn_harvest::models::WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .expect("reload execution");

    let registry = HandlerRegistry::new(Vec::new(), Vec::new()).with_payload_codecs(codecs.clone());
    // `carryover_result: None` forces the raw-carryover path under test --
    // the decoded fallback is not exercised here.
    let persistence = WorkflowTaskPersistence::new_for_test(
        &task,
        "worker-a",
        exec_id,
        1,
        Duration::ZERO,
        None,
        None,
        None,
    );
    let redirected_to_failure = persist_workflow_continue_as_new(
        &mut conn,
        &registry,
        persistence,
        &execution,
        json!({}),
        None,
    )
    .await
    .expect("continue-as-new persists");
    assert!(
        !redirected_to_failure,
        "a same-type continuation with no target constraints must create a successor, \
         not redirect to a terminal failure"
    );

    let predecessor_history = store::load_history_with_codecs(&mut conn, exec_id, &codecs)
        .await
        .expect("load predecessor history");
    let new_exec_id = predecessor_history
        .events
        .iter()
        .find_map(|e| match e {
            WorkflowEvent::WorkflowContinuedAsNew { new_exec_id, .. } => Some(*new_exec_id),
            _ => None,
        })
        .expect("predecessor must carry a WorkflowContinuedAsNew marker");

    let successor_history = store::load_history_with_codecs(&mut conn, new_exec_id, &codecs)
        .await
        .expect("load successor history");
    match &successor_history.events[0] {
        WorkflowEvent::WorkflowStarted {
            last_completion_result,
            ..
        } => {
            assert_eq!(
                *last_completion_result,
                Some(secret_output),
                "the successor must see the real prior output, not a codec envelope"
            );
        }
        other => panic!("unexpected successor event: {other:?}"),
    }
}

#[tokio::test]
async fn an_erasure_tombstone_committed_before_the_sweep_is_never_overwritten() {
    // The ordinary (non-racing) half: a row already tombstoned carries no
    // ciphertext, so the sweep skips it outright. The racing half — a sweep
    // that read the row BEFORE the tombstone committed — is
    // [`a_stale_read_can_never_overwrite_a_committed_erasure`] below.
    use autumn_harvest::schema::harvest_events;

    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "raced").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"ssn": "123-45-6789"}))],
    )
    .await;
    codecs.set_active_key("k2").expect("flip");

    // Simulate the interleaving: the row changes under the sweep between its
    // read and its write. Tombstoning it directly is exactly what erase.rs does.
    let row_id: i64 = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .select(harvest_events::id)
        .first(&mut conn)
        .await
        .expect("row id");
    let mut tombstoned: Value = harvest_events::table
        .find(row_id)
        .select(harvest_events::event_data)
        .first(&mut conn)
        .await
        .expect("row");
    tombstoned["data"]["input"] = erasure_tombstone();
    diesel::update(harvest_events::table.find(row_id))
        .set(harvest_events::event_data.eq(&tombstoned))
        .execute(&mut conn)
        .await
        .expect("tombstone");

    let swept = sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
        .await
        .expect("sweep");

    assert_eq!(swept, 0, "there is nothing left to rotate on a tombstone");
    let after = raw_event_data(&mut conn, exec_id).await;
    assert_eq!(
        after[0]["data"]["input"],
        erasure_tombstone(),
        "the erasure tombstone must survive the sweep"
    );
}

#[tokio::test]
async fn a_stale_read_can_never_overwrite_a_committed_erasure() {
    // The compare-and-swap guard itself, exercised directly. This is the
    // interleaving the batch-oriented sweep entry point cannot express: the
    // sweep reads a row, a PII erasure (#495) tombstones it and COMMITS, and
    // only then does the sweep try to write its re-encrypted copy back.
    // Without the CAS that write would resurrect payload data the erasure had
    // just destroyed — the P1 this design exists to foreclose.
    use autumn_harvest::codec_rotation::{compare_and_swap_event, reencrypt_event_payload_fields};
    use autumn_harvest::schema::harvest_events;

    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "cas").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"ssn": "123-45-6789"}))],
    )
    .await;
    codecs.set_active_key("k2").expect("flip");

    let row_id: i64 = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .select(harvest_events::id)
        .first(&mut conn)
        .await
        .expect("row id");

    // 1. The sweep reads the row and prepares its re-encrypted copy.
    let stale: Value = harvest_events::table
        .find(row_id)
        .select(harvest_events::event_data)
        .first(&mut conn)
        .await
        .expect("stale read");
    let mut candidate = stale.clone();
    let outcome = reencrypt_event_payload_fields(&codecs, &mut candidate).expect("reencrypt");
    assert!(outcome.changed(), "the sweep really did produce a rewrite");

    // 2. An erasure tombstones the row and commits, under the sweep.
    let mut tombstoned = stale.clone();
    tombstoned["data"]["input"] = erasure_tombstone();
    diesel::update(harvest_events::table.find(row_id))
        .set(harvest_events::event_data.eq(&tombstoned))
        .execute(&mut conn)
        .await
        .expect("erasure commits");

    // 3. The sweep's write must lose.
    let swapped = compare_and_swap_event(
        &mut conn,
        autumn_harvest::types::ShardId::new(0),
        row_id,
        &stale,
        &candidate,
    )
    .await
    .expect("cas");

    assert!(!swapped, "a stale compare-and-swap must not take effect");
    let after = raw_event_data(&mut conn, exec_id).await;
    assert_eq!(
        after[0]["data"]["input"],
        erasure_tombstone(),
        "the erasure must survive; ciphertext must never be resurrected"
    );
}

// ── AC8: composition with offload and erasure ────────────────────────────────

#[tokio::test]
async fn offload_envelopes_and_tombstones_survive_a_sweep_untouched() {
    use autumn_harvest::schema::harvest_events;

    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "composed").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"a": 1})), completed(json!({"b": 2}))],
    )
    .await;

    let ids: Vec<i64> = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .order(harvest_events::event_id.asc())
        .select(harvest_events::id)
        .load(&mut conn)
        .await
        .expect("ids");

    // Row 0's payload becomes an offload reference envelope; row 1's becomes an
    // erasure tombstone.
    let offload_envelope = json!({
        "_harvest_offload_envelope": 1,
        "store_id": "mem",
        "key": "blob/abc",
        "len": 4096,
        "checksum": "deadbeef",
    });
    for (row_id, field, replacement) in [
        (ids[0], "input", offload_envelope.clone()),
        (ids[1], "output", erasure_tombstone()),
    ] {
        let mut data: Value = harvest_events::table
            .find(row_id)
            .select(harvest_events::event_data)
            .first(&mut conn)
            .await
            .expect("row");
        data["data"][field] = replacement;
        diesel::update(harvest_events::table.find(row_id))
            .set(harvest_events::event_data.eq(&data))
            .execute(&mut conn)
            .await
            .expect("update");
    }

    codecs.set_active_key("k2").expect("flip");
    let swept = sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
        .await
        .expect("sweep");

    assert_eq!(swept, 0, "neither field carries rotatable ciphertext");
    let after = raw_event_data(&mut conn, exec_id).await;
    assert_eq!(
        after[0]["data"]["input"], offload_envelope,
        "the offload reference envelope is passed through, never double-encrypted"
    );
    assert_eq!(after[1]["data"]["output"], erasure_tombstone());
    let progress = load_shard_rotation_progress(&mut conn, 0, &codecs)
        .await
        .expect("progress");
    assert_eq!(
        progress.rows_remaining(),
        0,
        "rows with no ciphertext must not block retirement forever"
    );
}

// ── AC6: the fail-closed retirement gate ─────────────────────────────────────

#[tokio::test]
async fn retirement_is_refused_while_rows_remain_and_succeeds_at_zero() {
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "retire").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"a": 1}))],
    )
    .await;
    codecs.set_active_key("k2").expect("flip");

    let pool = build_pool(&url);
    let sharded = ShardedDbPool::single(pool);
    let shards = [ShardId::new(0)];

    let err = retire_codec_key(
        &sharded,
        &shards,
        &codecs,
        "k1",
        FleetWriteFence::ConfirmedByOperator,
        std::time::Duration::ZERO,
        std::time::Duration::ZERO,
    )
    .await
    .expect_err("retirement must be refused while a row remains");
    match err {
        HarvestError::CodecKeyRetirementBlocked { key_id, remaining } => {
            assert_eq!(key_id, "k1");
            assert_eq!(remaining.len(), 1);
            assert_eq!(remaining[0].shard_id, 0);
            assert_eq!(remaining[0].rows, 1, "the error names the remaining count");
            assert!(remaining[0].reachable);
        }
        other => panic!("expected CodecKeyRetirementBlocked, got {other:?}"),
    }
    assert!(
        codecs.codec_for_key("k1").is_some(),
        "a refused retirement must not drop the key"
    );

    sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
        .await
        .expect("sweep");

    retire_codec_key(
        &sharded,
        &shards,
        &codecs,
        "k1",
        FleetWriteFence::ConfirmedByOperator,
        std::time::Duration::ZERO,
        std::time::Duration::ZERO,
    )
    .await
    .expect("retirement must succeed at exactly zero remaining rows");
    assert!(codecs.codec_for_key("k1").is_none());
}

// ── issue #1244: the structural write fence ──────────────────────────────────

/// `activate_codec_key` is what durably marks the superseded key "retiring".
/// `retire_codec_key`'s default (structural) path refuses until that has held
/// for `staleness_window` -- even though the census is a genuine zero. That
/// is exactly the case the old boolean `FleetWriteFence` could not
/// distinguish from "another worker has not rolled forward yet".
#[tokio::test]
async fn retirement_waits_for_the_staleness_window_even_at_a_zero_census() {
    let (url, _c) = setup_isolated_db().await;
    let codecs = two_key_registry();
    let pool = build_pool(&url);
    let sharded = ShardedDbPool::single(pool);
    let shards = [ShardId::new(0)];

    // `two_key_registry` bootstraps k1 active only in this process's local
    // memory (mirroring `register_key`'s "first key becomes active"
    // convenience). An operator durably activates it too, so it has a
    // `harvest_codec_key_state` row to demote when a newer key supersedes it.
    activate_codec_key(&sharded, &shards, &codecs, "k1", 60)
        .await
        .expect("no live workers to block activation");
    activate_codec_key(&sharded, &shards, &codecs, "k2", 60)
        .await
        .expect("no live workers to block activation");
    assert_eq!(codecs.active_key_id(), "k2");

    let window = Duration::from_millis(200);
    let err = retire_codec_key(
        &sharded,
        &shards,
        &codecs,
        "k1",
        FleetWriteFence::NotConfirmed,
        window,
        Duration::ZERO,
    )
    .await
    .expect_err("the staleness window has not elapsed yet");
    match err {
        HarvestError::CodecKeyRetirementBlocked { remaining, .. } => {
            assert!(
                remaining
                    .iter()
                    .all(|r| r.reason.as_deref().is_some_and(|r| r.contains("staleness"))),
                "{remaining:?}"
            );
        }
        other => panic!("expected CodecKeyRetirementBlocked, got {other:?}"),
    }
    assert!(codecs.codec_for_key("k1").is_some());

    tokio::time::sleep(window + Duration::from_millis(100)).await;
    retire_codec_key(
        &sharded,
        &shards,
        &codecs,
        "k1",
        FleetWriteFence::NotConfirmed,
        window,
        Duration::ZERO,
    )
    .await
    .expect("the window has now elapsed and the census is zero");
    assert!(codecs.codec_for_key("k1").is_none());
}

/// A **local-only** flip (the old #948 behaviour, `PayloadCodecs::set_active_key`
/// called directly rather than through `activate_codec_key`) never durably
/// marks the superseded key "retiring". The structural gate refuses on
/// exactly that basis. This is the regression test for the hazard #1244
/// exists to close: a per-process flip must never be mistaken for a
/// fleet-wide one.
#[tokio::test]
async fn retirement_via_the_structural_gate_refuses_a_purely_local_flip() {
    let (url, _c) = setup_isolated_db().await;
    let codecs = two_key_registry();
    codecs.set_active_key("k2").expect("local-only flip");

    let pool = build_pool(&url);
    let sharded = ShardedDbPool::single(pool);
    let shards = [ShardId::new(0)];

    let err = retire_codec_key(
        &sharded,
        &shards,
        &codecs,
        "k1",
        FleetWriteFence::NotConfirmed,
        Duration::ZERO,
        Duration::ZERO,
    )
    .await
    .expect_err("no durable key state was ever written for k1");
    match err {
        HarvestError::CodecKeyRetirementBlocked { remaining, .. } => {
            assert!(
                remaining
                    .iter()
                    .all(|r| r.reason.as_deref().is_some_and(|r| r.contains("durable"))),
                "{remaining:?}"
            );
        }
        other => panic!("expected CodecKeyRetirementBlocked, got {other:?}"),
    }
    assert!(codecs.codec_for_key("k1").is_some());

    // The escape hatch is unaffected: an operator who has confirmed the fence
    // out of band still bypasses the durable-state requirement entirely.
    retire_codec_key(
        &sharded,
        &shards,
        &codecs,
        "k1",
        FleetWriteFence::ConfirmedByOperator,
        Duration::ZERO,
        Duration::ZERO,
    )
    .await
    .expect("the escape hatch skips the structural gate");
    assert!(codecs.codec_for_key("k1").is_none());
}

// ── issue #1251: retirement racing a pinned in-flight sweep batch ───────────

#[tokio::test]
async fn a_batch_pinned_to_a_key_blocks_its_retirement_through_a_double_rotation() {
    // Issue #1251. A batch resolves its target key once and pins it before
    // writing any row. Without the pin, this exact interleaving lets a row
    // land under a key that retirement already dropped:
    //
    //   1. A batch starts, targets k2 (the active key), and pins it.
    //   2. The active key rotates AWAY from k2 twice (k2 -> k1 -> k3), so k2
    //      is no longer active and looks retirable.
    //   3. `retire_codec_key("k2")` census reads zero rows -- the batch has
    //      not committed anything yet -- and WOULD succeed without the pin.
    //   4. The batch's write lands under k2, a key that retirement just
    //      dropped: silent, permanent data loss.
    //
    // The pin closes step 3: retirement is refused for as long as the batch
    // holds it, by construction, not by timing.
    use autumn_harvest::codec_rotation::{
        compare_and_swap_event, reencrypt_event_payload_fields_under,
    };
    use autumn_harvest::schema::harvest_events;

    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "pinned_retire").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"ssn": "123-45-6789"}))],
    )
    .await;
    codecs.set_active_key("k2").expect("flip to k2");

    let pool = build_pool(&url);
    let sharded = ShardedDbPool::single(pool);
    let shards = [ShardId::new(0)];

    // 1. A batch starts: it resolves k2 as its target and pins it, before
    // reading or writing a single row.
    let pin = codecs.pin_key_for_sweep("k2");

    // 2. A double rotation moves the active key away from k2.
    codecs.set_active_key("k1").expect("rotation 1");
    codecs
        .register_key("k3", Arc::new(XorCodec(0x33)))
        .expect("register k3");
    codecs.set_active_key("k3").expect("rotation 2");

    // 3. The census would read zero rows under k2 -- the batch has not
    // written anything yet -- but the pin refuses the retirement outright.
    let err = retire_codec_key(
        &sharded,
        &shards,
        &codecs,
        "k2",
        FleetWriteFence::ConfirmedByOperator,
        Duration::ZERO,
        Duration::ZERO,
    )
    .await
    .expect_err("a pinned key must not be retirable, even at a zero census");
    match err {
        HarvestError::Config(msg) => {
            assert!(
                msg.contains("pinned"),
                "the refusal must name the pin, got {msg:?}"
            );
        }
        other => panic!("expected Config, got {other:?}"),
    }
    assert!(
        codecs.codec_for_key("k2").is_some(),
        "the refused retirement must not have dropped the decoder"
    );

    // 4. The batch continues and commits its row under its pinned target,
    // k2 -- exactly the write the bug let land under an already-retired key.
    let row_id: i64 = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .select(harvest_events::id)
        .first(&mut conn)
        .await
        .expect("row id");
    let original: Value = harvest_events::table
        .find(row_id)
        .select(harvest_events::event_data)
        .first(&mut conn)
        .await
        .expect("read row");
    let mut candidate = original.clone();
    reencrypt_event_payload_fields_under(&codecs, "k2", &mut candidate)
        .expect("the pinned target is still registered, so this succeeds");
    let swapped = compare_and_swap_event(&mut conn, ShardId::new(0), row_id, &original, &candidate)
        .await
        .expect("cas");
    assert!(swapped, "the batch's write must land");

    let rows = raw_event_data(&mut conn, exec_id).await;
    assert_eq!(
        kid_of(&rows[0], "input"),
        Some("k2".to_string()),
        "the row committed under its pinned target, not a retired key"
    );

    // History must still be fully decodable: k2 was never actually retired.
    let history = store::load_history_with_codecs(&mut conn, exec_id, &codecs)
        .await
        .expect("history must decode: k2 is still registered");
    match &history.events[0] {
        WorkflowEvent::WorkflowStarted { input, .. } => {
            assert_eq!(input, &json!({"ssn": "123-45-6789"}));
        }
        other => panic!("unexpected event: {other:?}"),
    }

    // While the pin is still held, retirement of k2 stays refused for the
    // same reason, even though a row now genuinely exists under it too.
    assert!(
        retire_codec_key(
            &sharded,
            &shards,
            &codecs,
            "k2",
            FleetWriteFence::ConfirmedByOperator,
            Duration::ZERO,
            Duration::ZERO,
        )
        .await
        .is_err()
    );

    // Once the batch finishes and releases its pin, the ordinary census-based
    // gate takes back over. It correctly refuses for the ordinary reason,
    // because a row genuinely references k2 now.
    drop(pin);
    assert!(!codecs.is_pinned_by_sweep("k2"));
    let err = retire_codec_key(
        &sharded,
        &shards,
        &codecs,
        "k2",
        FleetWriteFence::ConfirmedByOperator,
        Duration::ZERO,
        Duration::ZERO,
    )
    .await
    .expect_err("k2 genuinely has a row now; retirement must still refuse it");
    assert!(
        matches!(err, HarvestError::CodecKeyRetirementBlocked { .. }),
        "the refusal reason must now be the ordinary census, not the pin: {err:?}"
    );

    // A later pass converges the row onto the current active key. Only
    // then does retirement of k2 succeed -- the fix does not deadlock it.
    sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
        .await
        .expect("sweep onto k3");
    retire_codec_key(
        &sharded,
        &shards,
        &codecs,
        "k2",
        FleetWriteFence::ConfirmedByOperator,
        Duration::ZERO,
        Duration::ZERO,
    )
    .await
    .expect("k2 is retirable once no row references it and no pin holds it");
    assert!(codecs.codec_for_key("k2").is_none());
}

/// The escape hatch skips only the staleness-window *wait* (see
/// `FleetWriteFence`'s doc). It must not also skip durably recording the
/// retirement, or `harvest_codec_key_state` would claim a destroyed key is
/// still merely "retiring" forever.
#[tokio::test]
async fn retirement_via_the_escape_hatch_still_records_the_durable_retirement() {
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let pool = build_pool(&url);
    let sharded = ShardedDbPool::single(pool);
    let shards = [ShardId::new(0)];

    activate_codec_key(&sharded, &shards, &codecs, "k1", 60)
        .await
        .expect("no live workers to block activation");
    activate_codec_key(&sharded, &shards, &codecs, "k2", 60)
        .await
        .expect("no live workers to block activation");
    assert_eq!(
        key_state(&mut conn, "k1").await.as_deref(),
        Some("retiring")
    );

    retire_codec_key(
        &sharded,
        &shards,
        &codecs,
        "k1",
        FleetWriteFence::ConfirmedByOperator,
        Duration::ZERO,
        Duration::ZERO,
    )
    .await
    .expect("zero rows and the escape hatch retire the key");

    assert_eq!(
        key_state(&mut conn, "k1").await.as_deref(),
        Some("retired"),
        "the escape hatch must not leave the durable row stuck at \"retiring\" \
         after the key is actually gone"
    );
}

/// The very first `activate_codec_key` call an embedder ever makes finds an
/// empty `harvest_codec_key_state`. Nothing is there to demote. So the key
/// this process is rotating *away from* must be seeded as `"retiring"`
/// directly -- otherwise it would never be retirable.
#[tokio::test]
async fn first_activation_ever_seeds_the_outgoing_key_as_retiring() {
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let pool = build_pool(&url);
    let sharded = ShardedDbPool::single(pool);
    let shards = [ShardId::new(0)];

    assert_eq!(
        key_state(&mut conn, "k1").await,
        None,
        "harvest_codec_key_state starts empty -- \"k1\" was never durable"
    );

    // The first-ever call, activating "k2" directly -- never a prior call
    // activating the already-active "k1" first.
    activate_codec_key(&sharded, &shards, &codecs, "k2", 60)
        .await
        .expect("no live workers to block activation");

    assert_eq!(
        key_state(&mut conn, "k1").await.as_deref(),
        Some("retiring"),
        "the outgoing key must be seeded as retiring even though it was \
         never durably active"
    );

    retire_codec_key(
        &sharded,
        &shards,
        &codecs,
        "k1",
        FleetWriteFence::NotConfirmed,
        Duration::ZERO,
        Duration::ZERO,
    )
    .await
    .expect(
        "the seeded row must satisfy the real structural staleness gate, not just the \
         escape hatch",
    );
    assert_eq!(key_state(&mut conn, "k1").await.as_deref(), Some("retired"));
}

/// AC5's second required interleaving: a row that was not even written at the
/// first census commits **during** the recheck delay. `retire_codec_key` must
/// catch it on the second pass rather than finalizing on the first zero.
#[tokio::test]
async fn retirement_recheck_catches_a_row_that_commits_after_the_first_zero_census() {
    let (url, _c) = setup_isolated_db().await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut connect(&url).await, "race").await;
    let pool = build_pool(&url);
    let sharded = ShardedDbPool::single(pool);
    let shards = [ShardId::new(0)];

    activate_codec_key(&sharded, &shards, &codecs, "k1", 60)
        .await
        .expect("no live workers to block activation");
    activate_codec_key(&sharded, &shards, &codecs, "k2", 60)
        .await
        .expect("no live workers to block activation");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let write_url = url.clone();
    let writer_codecs = two_key_registry();
    writer_codecs
        .set_active_key("k2")
        .expect("mirror the fleet's active key");
    let writer = tokio::spawn(async move {
        // Lands after the first (zero) census but well inside the recheck
        // delay below -- the straggling write AC5 asks for.
        tokio::time::sleep(Duration::from_millis(75)).await;
        let mut conn = connect(&write_url).await;
        append_under_key(
            &mut conn,
            &writer_codecs,
            exec_id,
            "k1",
            0,
            &[started(json!({"straggler": true}))],
        )
        .await;
    });

    let err = retire_codec_key(
        &sharded,
        &shards,
        &codecs,
        "k1",
        FleetWriteFence::NotConfirmed,
        Duration::ZERO,
        Duration::from_millis(300),
    )
    .await
    .expect_err("the recheck must observe the row that committed mid-delay");
    match err {
        HarvestError::CodecKeyRetirementBlocked { remaining, .. } => {
            assert_eq!(remaining.len(), 1);
            assert_eq!(remaining[0].rows, 1);
        }
        other => panic!("expected CodecKeyRetirementBlocked, got {other:?}"),
    }
    writer.await.expect("writer task");
}

#[tokio::test]
async fn retirement_fails_closed_on_an_unreachable_shard() {
    // AC6: an unreachable shard blocks retirement — it is never read as zero.
    let (url, _c) = setup_isolated_db().await;
    let codecs = two_key_registry();
    codecs.set_active_key("k2").expect("flip");

    let pool = build_pool(&url);
    let sharded = ShardedDbPool::single(pool);
    // Shard 0 is reachable and empty; shard 7 has no pool in this process.
    let shards = [ShardId::new(0), ShardId::new(7)];

    let err = retire_codec_key(
        &sharded,
        &shards,
        &codecs,
        "k1",
        FleetWriteFence::ConfirmedByOperator,
        std::time::Duration::ZERO,
        std::time::Duration::ZERO,
    )
    .await
    .expect_err("an unreadable shard must block retirement");
    match err {
        HarvestError::CodecKeyRetirementBlocked { remaining, .. } => {
            assert_eq!(remaining.len(), 1);
            assert_eq!(remaining[0].shard_id, 7);
            assert!(!remaining[0].reachable);
            assert_eq!(
                remaining[0].rows, 0,
                "unknown is reported as 0-but-unreachable"
            );
        }
        other => panic!("expected CodecKeyRetirementBlocked, got {other:?}"),
    }
    assert!(codecs.codec_for_key("k1").is_some());
}

#[tokio::test]
async fn retirement_with_no_shards_to_inspect_is_refused() {
    let (url, _c) = setup_isolated_db().await;
    let codecs = two_key_registry();
    codecs.set_active_key("k2").expect("flip");
    let pool = build_pool(&url);
    let sharded = ShardedDbPool::single(pool);

    let err = retire_codec_key(
        &sharded,
        &[],
        &codecs,
        "k1",
        FleetWriteFence::ConfirmedByOperator,
        std::time::Duration::ZERO,
        std::time::Duration::ZERO,
    )
    .await
    .expect_err("proving nothing must not be treated as proving zero");
    assert!(matches!(err, HarvestError::Config(_)), "{err:?}");
}

#[tokio::test]
async fn the_active_key_can_never_be_retired() {
    let (url, _c) = setup_isolated_db().await;
    let codecs = two_key_registry();
    let pool = build_pool(&url);
    let sharded = ShardedDbPool::single(pool);

    let err = retire_codec_key(
        &sharded,
        &[ShardId::new(0)],
        &codecs,
        "k1",
        FleetWriteFence::ConfirmedByOperator,
        std::time::Duration::ZERO,
        std::time::Duration::ZERO,
    )
    .await
    .expect_err("k1 is active");
    assert!(matches!(err, HarvestError::Config(_)), "{err:?}");
}

// ── issue #1244: the reader-capability handshake ──────────────────────────────

/// Insert a minimal `harvest_workers` row directly, bypassing `register_worker`,
/// so the test controls `last_heartbeat_at` and `labels` precisely.
#[allow(clippy::cast_precision_loss)] // a heartbeat age in seconds never approaches 2^53
async fn insert_worker_row(
    conn: &mut AsyncPgConnection,
    worker_id: &str,
    heartbeat_age_secs: i64,
    labels: &Value,
) {
    diesel::sql_query(
        "INSERT INTO harvest_workers \
             (worker_id, last_heartbeat_at, max_concurrency, host, build_id, labels) \
         VALUES ($1, NOW() - make_interval(secs => $2::float8), 1, 'test-host', 'test-build', $3)",
    )
    .bind::<diesel::sql_types::Text, _>(worker_id)
    .bind::<diesel::sql_types::Double, _>(heartbeat_age_secs as f64)
    .bind::<diesel::sql_types::Jsonb, _>(labels)
    .execute(conn)
    .await
    .expect("insert worker row");
}

#[tokio::test]
async fn activation_is_refused_while_a_live_worker_cannot_read_the_keyed_envelope() {
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    // No `codec_envelope_version` label at all -- exactly what a pre-#948
    // binary's row looks like.
    insert_worker_row(&mut conn, "worker-old", 0, &json!({})).await;

    let codecs = two_key_registry();
    let pool = build_pool(&url);
    let sharded = ShardedDbPool::single(pool);

    let err = activate_codec_key(&sharded, &[ShardId::new(0)], &codecs, "k2", 60)
        .await
        .expect_err("a live worker cannot read a version-2 envelope");
    match err {
        HarvestError::CodecKeyActivationBlocked { key_id, blockers } => {
            assert_eq!(key_id, "k2");
            assert_eq!(blockers.len(), 1);
            assert_eq!(blockers[0].worker_id.as_deref(), Some("worker-old"));
            assert!(blockers[0].reachable);
        }
        other => panic!("expected CodecKeyActivationBlocked, got {other:?}"),
    }
    assert_eq!(
        codecs.active_key_id(),
        "k1",
        "a refused activation must not flip the local registry"
    );
}

#[tokio::test]
async fn activation_succeeds_once_every_live_worker_advertises_the_keyed_envelope() {
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    insert_worker_row(
        &mut conn,
        "worker-new",
        0,
        &json!({"codec_envelope_version": 2, "codec_registered_key_ids": ["k2"]}),
    )
    .await;

    let codecs = two_key_registry();
    let pool = build_pool(&url);
    let sharded = ShardedDbPool::single(pool);

    activate_codec_key(&sharded, &[ShardId::new(0)], &codecs, "k2", 60)
        .await
        .expect("every live worker advertises version 2 and has k2 registered");
    assert_eq!(codecs.active_key_id(), "k2");
}

/// A worker's binary can support the version-2 envelope's syntax fleet-wide
/// before the target key's material reaches every worker's config.
/// Envelope support alone must not be read as proof this worker can decode
/// payloads written under the specific key being activated.
#[tokio::test]
async fn activation_is_refused_while_a_live_worker_lacks_the_target_key() {
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    // Envelope v2 capable, but only "k1" ever reached this worker's config --
    // "k2" is the key this test activates.
    insert_worker_row(
        &mut conn,
        "worker-partial",
        0,
        &json!({"codec_envelope_version": 2, "codec_registered_key_ids": ["k1"]}),
    )
    .await;

    let codecs = two_key_registry();
    let pool = build_pool(&url);
    let sharded = ShardedDbPool::single(pool);

    let err = activate_codec_key(&sharded, &[ShardId::new(0)], &codecs, "k2", 60)
        .await
        .expect_err("a live worker without k2 registered cannot decode payloads written under it");
    match err {
        HarvestError::CodecKeyActivationBlocked { key_id, blockers } => {
            assert_eq!(key_id, "k2");
            assert_eq!(blockers.len(), 1);
            assert_eq!(blockers[0].worker_id.as_deref(), Some("worker-partial"));
            assert!(blockers[0].reachable);
            assert!(
                blockers[0]
                    .reason
                    .as_deref()
                    .is_some_and(|r| r.contains("k2") && r.contains("not registered")),
                "{:?}",
                blockers[0].reason
            );
        }
        other => panic!("expected CodecKeyActivationBlocked, got {other:?}"),
    }
    assert_eq!(
        codecs.active_key_id(),
        "k1",
        "a refused activation must not flip the local registry"
    );
}

#[tokio::test]
async fn activation_ignores_a_worker_whose_heartbeat_is_stale() {
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    // Version-1-only, but its heartbeat is far older than the 60s liveness
    // window below -- a dead worker cannot silently mis-decode anything.
    insert_worker_row(&mut conn, "worker-dead", 999, &json!({})).await;

    let codecs = two_key_registry();
    let pool = build_pool(&url);
    let sharded = ShardedDbPool::single(pool);

    activate_codec_key(&sharded, &[ShardId::new(0)], &codecs, "k2", 60)
        .await
        .expect("a stale worker must not block activation");
    assert_eq!(codecs.active_key_id(), "k2");
}

// ── issue #1244: bounded-staleness refresh ────────────────────────────────────

/// `refresh_active_codec_key` is the mechanism that turns a durable
/// `activate_codec_key` write into a fact another process's `PayloadCodecs`
/// observes. Simulated here as two independent registries sharing a
/// database: one calls `activate_codec_key`, the other only refreshes.
#[tokio::test]
async fn refresh_active_codec_key_picks_up_a_fleet_wide_activation_from_another_process() {
    let (url, _c) = setup_isolated_db().await;
    let activator = two_key_registry();
    let observer = two_key_registry();
    let pool = build_pool(&url);
    let sharded = ShardedDbPool::single(pool);

    activate_codec_key(&sharded, &[ShardId::new(0)], &activator, "k2", 60)
        .await
        .expect("no live workers to block activation");
    assert_eq!(observer.active_key_id(), "k1", "unaffected until refreshed");

    let mut conn = connect(&url).await;
    let flipped = refresh_active_codec_key(&mut conn, &observer)
        .await
        .expect("refresh");
    assert!(flipped);
    assert_eq!(observer.active_key_id(), "k2");

    // Idempotent: refreshing again with nothing new to observe is a no-op.
    let flipped_again = refresh_active_codec_key(&mut conn, &observer)
        .await
        .expect("refresh");
    assert!(!flipped_again);
}

// ── AC7: progress reporting and the metric ───────────────────────────────────

#[tokio::test]
async fn rotation_progress_reports_rows_per_key_id_and_the_cursor() {
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "progress").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"a": 1})), completed(json!({"b": 2}))],
    )
    .await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k2",
        2,
        &[WorkflowEvent::SideEffectRecorded {
            kind: autumn_harvest::event::SideEffectKind::Custom,
            name: Some("x".to_string()),
            value: json!({"c": 3}),
        }],
    )
    .await;
    codecs.set_active_key("k2").expect("flip");

    let progress = load_shard_rotation_progress(&mut conn, 0, &codecs)
        .await
        .expect("progress");

    assert_eq!(progress.active_key_id, "k2");
    assert_eq!(progress.rows_by_key_id.get("k1"), Some(&2));
    assert_eq!(progress.rows_by_key_id.get("k2"), Some(&1));
    assert_eq!(progress.rows_remaining(), 2);
    assert!(
        progress.cursor.is_none(),
        "no cursor row exists before the first batch"
    );
}

#[tokio::test]
async fn the_sweep_records_the_reencrypted_metric() {
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "metered").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"a": 1})), completed(json!({"b": 2}))],
    )
    .await;
    codecs.set_active_key("k2").expect("flip");

    let metrics = CountingMetrics::default();
    sweep_codec_reencryption_once(&mut conn, 3, &codecs, 100, &metrics)
        .await
        .expect("sweep");

    let recorded = metrics.reencrypted.lock().unwrap().clone();
    assert_eq!(
        recorded,
        vec![("3".to_string(), 2u64)],
        "harvest.codec.reencrypted is labelled by shard and counts swept rows"
    );
}

#[tokio::test]
async fn a_near_envelope_is_neither_counted_nor_swept() {
    // The census SQL mirrors `codec_envelope_parts` exactly: a four-key object
    // whose fourth key is not a string `kid` is not an envelope, in Postgres
    // just as in Rust. If the two drifted, the retirement gate would either
    // block forever or open early.
    use autumn_harvest::schema::harvest_events;

    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "near").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"a": 1}))],
    )
    .await;

    let row_id: i64 = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .select(harvest_events::id)
        .first(&mut conn)
        .await
        .expect("row id");
    let mut data: Value = harvest_events::table
        .find(row_id)
        .select(harvest_events::event_data)
        .first(&mut conn)
        .await
        .expect("row");
    // Business data that merely *looks* like an envelope.
    data["data"]["input"] = json!({
        "_harvest_codec_envelope": 2,
        "codec_id": "xor",
        "data": "AAAA",
        "something_else": true,
    });
    diesel::update(harvest_events::table.find(row_id))
        .set(harvest_events::event_data.eq(&data))
        .execute(&mut conn)
        .await
        .expect("update");

    codecs.set_active_key("k2").expect("flip");
    let progress = load_shard_rotation_progress(&mut conn, 0, &codecs)
        .await
        .expect("progress");
    assert_eq!(
        progress.rows_remaining(),
        0,
        "a near-envelope must not be counted by the census"
    );
    assert_eq!(
        sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
            .await
            .expect("sweep"),
        0,
        "and must not be swept either"
    );
}

/// The SQL census must agree with Rust that a four-key **version 1** value is
/// plaintext, not an envelope — otherwise it would count business data that the
/// sweep can never convert and block retirement forever.
#[tokio::test]
async fn a_four_key_version_1_payload_is_not_counted_by_the_census() {
    use autumn_harvest::schema::harvest_events;

    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "v1_business").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"a": 1}))],
    )
    .await;

    let row_id: i64 = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .select(harvest_events::id)
        .first(&mut conn)
        .await
        .expect("row id");
    let mut data: Value = harvest_events::table
        .find(row_id)
        .select(harvest_events::event_data)
        .first(&mut conn)
        .await
        .expect("row");
    // Exactly what a pre-#948 identity deployment could legitimately have
    // stored as business plaintext.
    data["data"]["input"] = json!({
        "_harvest_codec_envelope": 1,
        "codec_id": "xor",
        "data": "AAAA",
        "kid": "k1",
    });
    diesel::update(harvest_events::table.find(row_id))
        .set(harvest_events::event_data.eq(&data))
        .execute(&mut conn)
        .await
        .expect("update");

    codecs.set_active_key("k2").expect("flip");
    let progress = load_shard_rotation_progress(&mut conn, 0, &codecs)
        .await
        .expect("progress");
    assert_eq!(
        progress.rows_remaining(),
        0,
        "four-key version-1 plaintext must not be counted: {:?}",
        progress.rows_by_key_id
    );
    assert_eq!(
        sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
            .await
            .expect("sweep"),
        0,
        "and must not be rewritten"
    );
}

#[tokio::test]
async fn a_registry_with_no_keyed_codecs_sweeps_nothing_and_writes_no_cursor() {
    // The zero-overhead default: an un-rotated deployment pays nothing.
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let mut codecs = PayloadCodecs::default();
    codecs.set_default(Arc::new(XorCodec(0x11)));
    let exec_id = insert_execution(&mut conn, "unrotated").await;
    store::append_events_with_codecs(&mut conn, exec_id, &[started(json!({"a": 1}))], 0, &codecs)
        .await
        .expect("append");
    let before = raw_event_data(&mut conn, exec_id).await;

    assert_eq!(
        sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
            .await
            .expect("sweep"),
        0
    );
    assert_eq!(raw_event_data(&mut conn, exec_id).await, before);
    // The early return happens before any bookkeeping, so the sweep leaves no
    // trace at all — the observable half of "not one statement issued".
    assert_eq!(
        cursor_row_count(&mut conn).await,
        0,
        "an un-rotated deployment must not even create a cursor row"
    );
    let progress = load_shard_rotation_progress(&mut conn, 0, &codecs)
        .await
        .expect("progress");
    assert!(
        progress.rows_by_key_id.is_empty(),
        "the admin read must not run the census when rotation was never adopted"
    );
}

/// A rotation that is later ROLLED BACK must rescan the shard.
///
/// The cursor deliberately carries the target key id as a column rather than as
/// part of its key: resuming a rolled-back-to key's own already-completed pass
/// would skip every row written under the key being rolled back FROM, and leave
/// that key permanently unretirable.
#[tokio::test]
async fn rolling_back_to_a_previous_key_rescans_the_shard() {
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "rollback").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"a": 1}))],
    )
    .await;

    // Forward: k1 -> k2, pass completes.
    codecs.set_active_key("k2").expect("flip to k2");
    assert_eq!(
        sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
            .await
            .expect("forward sweep"),
        1
    );
    assert!(
        load_shard_rotation_progress(&mut conn, 0, &codecs)
            .await
            .expect("progress")
            .cursor
            .expect("cursor")
            .completed_at
            .is_some(),
        "the forward pass completed"
    );

    // Roll back: k2 -> k1. The k1 pass must start over, not resume.
    codecs.set_active_key("k1").expect("roll back to k1");
    assert_eq!(
        sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
            .await
            .expect("rollback sweep"),
        1,
        "a rollback must rescan the shard, not resume the old k1 cursor"
    );
    let rows = raw_event_data(&mut conn, exec_id).await;
    assert_eq!(kid_of(&rows[0], "input"), Some("k1".to_string()));
    assert_eq!(
        load_shard_rotation_progress(&mut conn, 0, &codecs)
            .await
            .expect("progress")
            .rows_remaining(),
        0
    );
}

/// A row the pass could not convert must not be abandoned behind the cursor.
///
/// The failure this guards is a two-phase rollout that activates the new key
/// before every process has the outgoing key registered: without the
/// unresolved-row accounting the pass would log each undecodable row, march the
/// cursor to the end of the shard, stamp itself complete, and leave those rows
/// on the retired key forever — with a manual `DELETE` on the cursor table as
/// the only recovery.
#[tokio::test]
async fn an_unconvertible_row_is_retried_once_its_key_comes_back() {
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;

    // Written under a key the sweeping process does not (yet) know.
    let writer = PayloadCodecs::default();
    writer
        .register_key("k0", Arc::new(XorCodec(0x00)))
        .expect("register k0");
    let exec_id = insert_execution(&mut conn, "late_key").await;
    append_under_key(
        &mut conn,
        &writer,
        exec_id,
        "k0",
        0,
        &[started(json!({"a": 1}))],
    )
    .await;

    let codecs = PayloadCodecs::default();
    codecs
        .register_key("k2", Arc::new(XorCodec(0x22)))
        .expect("register k2");

    assert_eq!(
        sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
            .await
            .expect("sweep with the key missing"),
        0
    );
    let stalled = load_shard_rotation_progress(&mut conn, 0, &codecs)
        .await
        .expect("progress")
        .cursor
        .expect("cursor");
    assert!(
        stalled.completed_at.is_none(),
        "a pass that left a row unconverted must NOT report itself complete"
    );
    assert_eq!(
        stalled.last_event_id, 0,
        "and must rewind so the row gets another attempt"
    );

    // The operator puts the key back, exactly as the runbook says.
    codecs
        .register_key("k0", Arc::new(XorCodec(0x00)))
        .expect("re-register k0");
    assert_eq!(
        sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
            .await
            .expect("sweep after re-registering"),
        1,
        "the previously-unconvertible row must be picked up with no manual intervention"
    );
    let done = load_shard_rotation_progress(&mut conn, 0, &codecs)
        .await
        .expect("progress");
    assert_eq!(done.rows_remaining(), 0);
    assert!(done.cursor.expect("cursor").completed_at.is_some());
}

/// The third documented fail-closed path: the shard is reachable but its census
/// errors. An unreadable shard is a blocker, never a zero.
#[tokio::test]
async fn retirement_fails_closed_when_a_shards_census_errors() {
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    codecs.set_active_key("k2").expect("flip");

    // Make the census fail on an otherwise-reachable shard.
    conn.batch_execute("DROP TABLE harvest_events CASCADE")
        .await
        .expect("drop events table");

    let pool = build_pool(&url);
    let sharded = ShardedDbPool::single(pool);
    let err = retire_codec_key(
        &sharded,
        &[ShardId::new(0)],
        &codecs,
        "k1",
        FleetWriteFence::ConfirmedByOperator,
        std::time::Duration::ZERO,
        std::time::Duration::ZERO,
    )
    .await
    .expect_err("a failed census must block retirement");
    match err {
        HarvestError::CodecKeyRetirementBlocked { remaining, .. } => {
            assert_eq!(remaining.len(), 1);
            assert!(!remaining[0].reachable);
            assert!(
                remaining[0]
                    .reason
                    .as_deref()
                    .is_some_and(|r| r.contains("census")),
                "the error must say why: {:?}",
                remaining[0].reason
            );
        }
        other => panic!("expected CodecKeyRetirementBlocked, got {other:?}"),
    }
    assert!(codecs.codec_for_key("k1").is_some());
}

/// Retirement must refuse a shard list that omits a shard this process can see:
/// an omitted shard is never censused, so `Ok` for it would be vacuous.
#[tokio::test]
async fn retirement_refuses_a_shard_list_that_omits_a_known_shard() {
    let (url, _c) = setup_isolated_db().await;
    let codecs = two_key_registry();
    codecs.set_active_key("k2").expect("flip");
    let pool = build_pool(&url);
    let sharded = ShardedDbPool::single(pool);

    // Shard 0 exists in the pool but is not in the supplied list.
    let err = retire_codec_key(
        &sharded,
        &[ShardId::new(9)],
        &codecs,
        "k1",
        FleetWriteFence::ConfirmedByOperator,
        std::time::Duration::ZERO,
        std::time::Duration::ZERO,
    )
    .await
    .expect_err("an incomplete shard list must block retirement");
    assert!(
        matches!(err, HarvestError::CodecKeyRetirementBlocked { .. }),
        "{err:?}"
    );
}

/// Retiring a key that was never registered must not report a vacuous success.
#[tokio::test]
async fn retirement_refuses_an_unregistered_key_id() {
    let (url, _c) = setup_isolated_db().await;
    let codecs = two_key_registry();
    let pool = build_pool(&url);
    let sharded = ShardedDbPool::single(pool);

    let err = retire_codec_key(
        &sharded,
        &[ShardId::new(0)],
        &codecs,
        "never-registered",
        FleetWriteFence::ConfirmedByOperator,
        std::time::Duration::ZERO,
        std::time::Duration::ZERO,
    )
    .await
    .expect_err("an unregistered key proves nothing");
    assert!(matches!(err, HarvestError::Config(_)), "{err:?}");
}

/// AC4's "resident of the existing scanner cadence" half: the sweep must
/// actually run from `enforce_timeouts_once`, not only when called directly.
#[tokio::test]
async fn a_failing_sweep_does_not_strand_the_later_timeout_residents() {
    // Codex round 5 (P1): the sweep runs before `reclaim_expired_leases_and_wake`
    // in `enforce_timeouts_once`. Propagating its error would return from the
    // pass early, so a PERSISTENT rotation failure -- missing grants on the
    // cursor table or on `UPDATE harvest_events`, which repeat identically
    // every tick -- would stop expired durable-mutex leases being reclaimed on
    // that shard, stranding the workflows waiting on them. Rotation is new and
    // optional; mutex reclamation is neither, and a new feature must not be
    // able to break an old one by failing.
    //
    // Breaking the cursor table's SCHEMA (rather than dropping it) is what
    // makes this test target the sweep alone: `cursor_table_present` still
    // reports true, so the sweep proceeds and fails on the column, while every
    // other resident of the pass is untouched.
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "sweep_failure_isolation").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"a": 1}))],
    )
    .await;
    codecs.set_active_key("k2").expect("flip");

    diesel::sql_query("ALTER TABLE harvest_codec_rotation_cursor DROP COLUMN rows_reencrypted")
        .execute(&mut conn)
        .await
        .expect("break the cursor table for the sweep only");

    // The pass must still succeed: the sweep's failure is logged and skipped.
    autumn_harvest::timeout::enforce_timeouts_once(
        &mut conn,
        &NoOpMetrics,
        std::time::Duration::from_secs(5),
        &None,
        &[],
        None,
        None,
        60,
        &codecs,
        100,
    )
    .await
    .expect("a failing rotation sweep must not fail the whole timeout pass");

    // And the row is genuinely unconverted -- proving the sweep really did
    // fail, so the Ok above is isolation and not an accidental no-op.
    let rows = raw_event_data(&mut conn, exec_id).await;
    assert_eq!(
        kid_of(&rows[0], "input"),
        Some("k1".to_string()),
        "the sweep must have failed, leaving the row on the old key"
    );
}

#[tokio::test]
async fn the_sweep_runs_as_a_resident_of_the_timeout_scanner() {
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "scanner_resident").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"a": 1}))],
    )
    .await;
    codecs.set_active_key("k2").expect("flip");

    autumn_harvest::timeout::enforce_timeouts_once(
        &mut conn,
        &NoOpMetrics,
        std::time::Duration::from_secs(5),
        &None,
        &[],
        None,
        None,
        60,
        &codecs,
        100,
    )
    .await
    .expect("timeout tick");

    let rows = raw_event_data(&mut conn, exec_id).await;
    assert_eq!(
        kid_of(&rows[0], "input"),
        Some("k2".to_string()),
        "the scanner tick must drive the sweep"
    );
}

/// A `kid` read back out of STORAGE is untrusted input.
///
/// On a deployment with no non-identity codec, a caller's workflow input is
/// stored verbatim — so envelope-shaped input carrying an arbitrary `kid` would
/// otherwise inject an attacker-chosen, unbounded key into the rotation census
/// and keep `rows_remaining` permanently non-zero, denying the retirement
/// procedure outright.
#[tokio::test]
async fn a_crafted_key_id_in_stored_input_is_not_counted() {
    use autumn_harvest::schema::harvest_events;

    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "crafted").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"a": 1}))],
    )
    .await;

    let row_id: i64 = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .select(harvest_events::id)
        .first(&mut conn)
        .await
        .expect("row id");
    let mut data: Value = harvest_events::table
        .find(row_id)
        .select(harvest_events::event_data)
        .first(&mut conn)
        .await
        .expect("row");
    data["data"]["input"] = json!({
        "_harvest_codec_envelope": 2,
        "codec_id": "xor",
        "data": "AAAA",
        "kid": "A".repeat(4096),
    });
    diesel::update(harvest_events::table.find(row_id))
        .set(harvest_events::event_data.eq(&data))
        .execute(&mut conn)
        .await
        .expect("update");

    codecs.set_active_key("k2").expect("flip");
    let progress = load_shard_rotation_progress(&mut conn, 0, &codecs)
        .await
        .expect("progress");
    assert!(
        progress.rows_by_key_id.keys().all(|k| k.len() <= 64),
        "an over-long crafted key id must never reach the census: {:?}",
        progress.rows_by_key_id
    );
    assert_eq!(
        progress.rows_remaining(),
        0,
        "crafted input must not be able to hold the retirement gate open"
    );
}

/// Every shard in one fan-out must classify against the **same** active key.
///
/// `GET /admin/codec/rotation` reads each shard on its own connection, so a
/// `set_active_key` landing mid-fan-out would otherwise have some shards
/// classify their rows against the outgoing key (counting them as converted)
/// and later shards against the incoming one (counting them as remaining) --
/// with the response advertising whichever key was active when the aggregate
/// was assembled. `rows_remaining_total: 0` under the *new* key, while rows
/// under the old key are still out there, is exactly the reading the runbook
/// tells an operator to treat as "safe to retire".
///
/// This pins the mechanism the endpoint relies on: classification is against a
/// caller-supplied key, so the caller can pin one for the whole fan-out.
#[tokio::test]
async fn shard_progress_classifies_against_the_pinned_key_not_the_live_one() {
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "pinned_key").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"a": 1}))],
    )
    .await;

    // The live registry has moved on to k2, so an unpinned read reports the k1
    // row as still requiring rotation.
    codecs.set_active_key("k2").expect("flip");
    let live = load_shard_rotation_progress(&mut conn, 0, &codecs)
        .await
        .expect("live progress");
    assert_eq!(live.active_key_id, "k2");
    assert_eq!(
        live.rows_remaining(),
        1,
        "against the live key, the k1 row is outstanding"
    );

    // Pinned to k1 -- the key that was active when an earlier shard in the same
    // fan-out was read -- the very same row classifies as already converted.
    let pinned = load_shard_rotation_progress_against(&mut conn, 0, &codecs, "k1")
        .await
        .expect("pinned progress");
    assert_eq!(
        pinned.active_key_id, "k1",
        "the pinned key must be what the shard reports, so the aggregate cannot \
         mix classifications from either side of a flip"
    );
    assert_eq!(
        pinned.rows_remaining(),
        0,
        "against the pinned key, the k1 row is converted"
    );
}

/// Encode `value` under `key_id` using the registry's real encoder, without
/// disturbing which key is active.
fn encode_under(codecs: &PayloadCodecs, key_id: &str, value: &Value) -> Value {
    let restore = codecs.active_key_id();
    codecs.set_active_key(key_id).expect("activate for fixture");
    let encoded = codecs.encode_payload(value).expect("encode");
    codecs.set_active_key(&restore).expect("restore active key");
    encoded
}

/// Insert one `WorkflowStarted` row at an explicit `harvest_events.id`.
///
/// Explicit ids are what let this file construct the id *gap* a late-committing
/// `BIGSERIAL` insert leaves behind; the sequence alone never yields one.
async fn insert_event_at_id(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    row_id: i64,
    event_id: i32,
    encoded_input: &Value,
) {
    diesel::sql_query(
        "INSERT INTO harvest_events (id, workflow_exec_id, event_id, event_type, event_data) \
         VALUES ($1, $2, $3, 'WorkflowStarted', $4)",
    )
    .bind::<diesel::sql_types::BigInt, _>(row_id)
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Integer, _>(event_id)
    .bind::<diesel::sql_types::Jsonb, _>(json!({
        "type": "WorkflowStarted",
        "data": {"input": encoded_input, "timestamp": "2026-08-31T00:00:00Z"}
    }))
    .execute(conn)
    .await
    .expect("insert event at explicit id");
}

/// A row that commits *below* the cursor must still be converted.
///
/// `harvest_events.id` is a `BIGSERIAL`, so an INSERT allocates its id before it
/// commits. A sweep running concurrently can scan past an id that is not yet
/// visible, mark the pass complete, and leave that row behind the cursor once it
/// does commit -- where `WHERE id > resume_from` never looks again.
///
/// The existing reset covers rows a pass *saw and failed to convert*
/// (`unresolved_rows`). It cannot cover a row the pass never saw: nothing
/// counted it, so the pass looked clean and recorded `completed_at`.
///
/// The consequence is a deadlock of the procedure rather than data loss: the
/// census keeps reporting the outgoing key, `retire_codec_key` keeps refusing,
/// and rotation can never finish without someone resetting the cursor by hand.
///
/// This builds the end state directly -- a committed old-key row below a
/// completed cursor -- rather than racing an uncommitted INSERT against the
/// sweep, which would be timing-dependent and flaky in both directions.
#[tokio::test]
async fn a_row_committing_below_the_cursor_is_still_converted() {
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "late_commit").await;

    // Push the cursor high with an explicit id, so an id well below it is
    // guaranteed free for the late arrival.
    let encoded = encode_under(&codecs, "k1", &json!({"early": true}));
    insert_event_at_id(&mut conn, exec_id, 10_000, 0, &encoded).await;

    codecs.set_active_key("k2").expect("flip");
    sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
        .await
        .expect("first pass");

    let cursor = load_shard_rotation_progress(&mut conn, 0, &codecs)
        .await
        .expect("progress")
        .cursor
        .expect("a cursor exists");
    assert_eq!(cursor.last_event_id, 10_000);
    assert!(
        cursor.completed_at.is_some(),
        "the first pass must complete, or this test is not exercising the hazard"
    );

    // The late arrival: below the cursor, still on the outgoing key.
    let late = encode_under(&codecs, "k1", &json!({"late": true}));
    insert_event_at_id(&mut conn, exec_id, 5_000, 1, &late).await;

    // A converged shard must NOT re-census on every tick -- that is a full scan
    // of `harvest_events` at the scanner's interval, forever. So immediately
    // after completion the late row is expected to still be there: the
    // revalidation clock has not come round.
    for _ in 0..3 {
        sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
            .await
            .expect("tick inside the revalidation window");
    }
    let throttled = load_shard_rotation_progress(&mut conn, 0, &codecs)
        .await
        .expect("progress")
        .rows_remaining();
    assert_eq!(
        throttled, 1,
        "inside the revalidation window the sweep must not re-census; if this \
         is 0 the throttle is gone and every tick is scanning the table"
    );

    // Wind the cursor's clock back past the revalidation interval. That is the
    // only thing standing between the stranded row and recovery.
    diesel::sql_query(
        "UPDATE harvest_codec_rotation_cursor \
         SET updated_at = now() - interval '10 minutes' WHERE shard_id = 0",
    )
    .execute(&mut conn)
    .await
    .expect("age the cursor");

    // One tick to revalidate and re-open the pass, another to walk it.
    for _ in 0..3 {
        sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
            .await
            .expect("tick after the revalidation window");
    }

    let remaining = load_shard_rotation_progress(&mut conn, 0, &codecs)
        .await
        .expect("progress")
        .rows_remaining();
    assert_eq!(
        remaining, 0,
        "once the revalidation clock comes round, a row that committed below \
         the cursor must still be swept; leaving it there deadlocks retirement \
         forever"
    );
}

/// A cursor belonging to a *different* key must not be reported as the active
/// key's.
///
/// `ShardRotationProgress::cursor` is documented as the resume cursor for the
/// active key's pass, and the sweep already honours that: it filters a stored
/// cursor by `active_key_id` before resuming, precisely so a rollback cannot
/// resume a pass the rolled-back-to key finished long ago.
///
/// The reporting path did not apply the same filter. Between a key flip and
/// that shard's next sweep tick, `GET /admin/codec/rotation` would hand back
/// the *previous* key's completed cursor beside the new `active_key_id` --
/// which reads as "the new rotation is already done" to anyone consuming the
/// endpoint, including the operator the runbook sends there.
#[tokio::test]
async fn a_cursor_from_another_key_is_not_reported_as_the_active_keys() {
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "cursor_key_scope").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"a": 1}))],
    )
    .await;

    // Complete a pass under k2, leaving a k2 cursor with `completed_at` set.
    codecs.set_active_key("k2").expect("flip to k2");
    sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
        .await
        .expect("k2 pass");
    let k2_view = load_shard_rotation_progress(&mut conn, 0, &codecs)
        .await
        .expect("progress under k2");
    let k2_cursor = k2_view.cursor.expect("k2 pass wrote a cursor");
    assert_eq!(k2_cursor.active_key_id, "k2");
    assert!(k2_cursor.completed_at.is_some());

    // Roll forward to k1 -- a different key. The k2 cursor is still the only
    // row in the table, and this read happens before k1's first tick.
    codecs.set_active_key("k1").expect("flip to k1");
    let k1_view = load_shard_rotation_progress(&mut conn, 0, &codecs)
        .await
        .expect("progress under k1");

    assert_eq!(k1_view.active_key_id, "k1");
    assert!(
        k1_view.cursor.is_none(),
        "a cursor recorded for a different key must not be reported as this \
         key's; a completed k2 cursor beside active_key_id=k1 reads as a \
         finished rotation that never ran"
    );
}

/// A completed cursor must still advance over rows it has just examined.
///
/// A converged shard keeps receiving new events. They arrive already under the
/// active key, so there is nothing to convert -- but the sweep still reads and
/// deserializes them. If the cursor does not advance past them, the next tick
/// re-reads the same rows, and the one after that re-reads them plus whatever
/// arrived since, until the batch limit or the five-minute revalidation clock
/// finally moves it. On a continuously active shard at a 500 ms tick that is
/// read amplification bounded only by `batch_limit`.
///
/// `highest_id` is the right value in both cases by construction: it is the max
/// id examined, and falls back to the resume point when the batch was empty --
/// so using it costs nothing in the steady state and fixes the active one.
#[tokio::test]
async fn a_completed_cursor_advances_over_rows_it_has_examined() {
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "advance_after_complete").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"a": 1}))],
    )
    .await;

    codecs.set_active_key("k2").expect("flip");
    sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
        .await
        .expect("first pass");
    let first_pass = load_shard_rotation_progress(&mut conn, 0, &codecs)
        .await
        .expect("progress")
        .cursor
        .expect("cursor");
    assert!(first_pass.completed_at.is_some());
    let settled_at = first_pass.last_event_id;

    // New traffic on a converged shard: already under the active key, so
    // nothing to convert -- but the sweep reads them all the same. Fewer than
    // `batch_limit`, so the batch is underfilled and `reached_end` is true.
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k2",
        1,
        &[completed(json!({"b": 2}))],
    )
    .await;

    sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
        .await
        .expect("tick over the new rows");

    let after = load_shard_rotation_progress(&mut conn, 0, &codecs)
        .await
        .expect("progress")
        .cursor
        .expect("cursor");
    assert!(
        after.last_event_id > settled_at,
        "the cursor must advance over rows the tick already examined ({} -> {}); \
         standing still re-reads them on every tick until the batch limit or the \
         revalidation clock moves it",
        settled_at,
        after.last_event_id
    );
    assert!(
        after.completed_at.is_some(),
        "advancing over already-converted rows must not un-complete the pass"
    );
}

// ── issue #1257: the cursor write is a compare-and-swap ──────────────────────

#[tokio::test]
async fn a_stale_cursor_write_cannot_overwrite_newer_progress() {
    // Two sweepers can read the same cursor row and each compute their own
    // next state from it. Without a guard, whichever write commits last wins
    // outright, even when it started from an older read. The cursor then
    // moves backward and `rows_reencrypted` can decrease -- the race issue
    // #1257 reports.
    //
    // Exercised directly against `write_cursor`, the same way
    // `a_stale_read_can_never_overwrite_a_committed_erasure` exercises
    // `compare_and_swap_event` directly. The batch-oriented sweep entry
    // point runs single-threaded on one connection. It cannot express two
    // writers racing the same read.
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let shard = ShardId::new(0);

    // The baseline both sweepers read: last_event_id 100, 50 rows converted.
    let applied = write_cursor(&mut conn, shard, "k2", 100, 50, 0, None)
        .await
        .expect("baseline write");
    assert!(applied, "an insert with no prior row must always apply");

    // The winner commits progress computed from that baseline first.
    let applied = write_cursor(&mut conn, shard, "k2", 200, 60, 0, None)
        .await
        .expect("winner write");
    assert!(applied, "a forward write over the row it read must apply");

    // The loser also read the 100 / 50 baseline, and only now commits its
    // own, smaller, progress.
    let applied = write_cursor(&mut conn, shard, "k2", 150, 55, 0, None)
        .await
        .expect("stale write");
    assert!(
        !applied,
        "a write computed from a stale read must be dropped, not applied"
    );

    let cursor = cursor_row(&mut conn, 0).await.expect("cursor row");
    assert_eq!(
        cursor.last_event_id, 200,
        "the cursor must not move backward"
    );
    assert_eq!(
        cursor.rows_reencrypted, 60,
        "the rewrite total must not decrease"
    );
}

#[tokio::test]
async fn a_deliberate_rewind_to_zero_always_applies() {
    // `last_event_id = 0` is the deliberate reset a pass takes when it
    // leaves rows unresolved (see `sweep_codec_reencryption_once`). The CAS
    // guard must not mistake that reset for a stale write and drop it.
    // That holds even over a stored `last_event_id` that is higher, as
    // long as `rows_reencrypted` still holds or grows (held equal here).
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let shard = ShardId::new(0);

    let applied = write_cursor(&mut conn, shard, "k2", 500, 40, 0, None)
        .await
        .expect("baseline write");
    assert!(applied, "the baseline write must apply");

    let applied = write_cursor(&mut conn, shard, "k2", 0, 40, 3, None)
        .await
        .expect("rewind write");
    assert!(applied, "a rewind to last_event_id = 0 must always apply");

    let cursor = cursor_row(&mut conn, 0).await.expect("cursor row");
    assert_eq!(cursor.last_event_id, 0);
    assert_eq!(cursor.unresolved_rows, 3);
}

#[tokio::test]
async fn a_stale_rewind_cannot_decrease_rows_reencrypted() {
    // A rewind resets `last_event_id` to 0 unconditionally, but the row it
    // writes is still one write. The `WHERE` guard must not let that
    // exemption carry `rows_reencrypted` down with it.
    //
    // Two sweepers can each read the same baseline. Each loses some of
    // its own rows to the other's `compare_and_swap_event`. Each then
    // lands in the `unresolved_total > 0` rewind branch with a different
    // `rows_reencrypted_total`, computed from that one shared read.
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let shard = ShardId::new(0);

    // The baseline both sweepers read: 100 rows already converted.
    let applied = write_cursor(&mut conn, shard, "k2", 500, 100, 0, None)
        .await
        .expect("baseline write");
    assert!(applied, "the baseline write must apply");

    // The winner converts 30 more of its own rows, then loses the rest of
    // its batch to the other sweeper. It rewinds at 100 + 30 = 130.
    let applied = write_cursor(&mut conn, shard, "k2", 0, 130, 20, None)
        .await
        .expect("winner rewind");
    assert!(applied, "the winner's rewind must apply");

    // The loser read the SAME 100-row baseline. It converted only 20 of
    // its own rows, and also rewinds at 100 + 20 = 120. That is lower than
    // what is now stored, even though its own `last_event_id` write is
    // the deliberate reset that always applies.
    let applied = write_cursor(&mut conn, shard, "k2", 0, 120, 30, None)
        .await
        .expect("stale rewind");
    assert!(
        !applied,
        "a rewind computed from a stale read must not decrease rows_reencrypted"
    );

    let cursor = cursor_row(&mut conn, 0).await.expect("cursor row");
    assert_eq!(
        cursor.rows_reencrypted, 130,
        "the winner's higher count must survive the loser's rewind"
    );
}

#[tokio::test]
async fn a_new_active_key_always_starts_a_fresh_pass() {
    // A cursor recorded against a different key belongs to a different pass
    // entirely. Its `last_event_id` and `rows_reencrypted` are not
    // comparable to the new key's. A fresh pass must apply even when both
    // read lower than the old key's.
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let shard = ShardId::new(0);

    write_cursor(&mut conn, shard, "k1", 900, 80, 0, None)
        .await
        .expect("k1 pass write");

    let applied = write_cursor(&mut conn, shard, "k2", 10, 1, 0, None)
        .await
        .expect("k2 pass write");
    assert!(
        applied,
        "a cursor write for a different active key must always apply"
    );

    let cursor = cursor_row(&mut conn, 0).await.expect("cursor row");
    assert_eq!(cursor.active_key_id, "k2");
    assert_eq!(cursor.last_event_id, 10);
    assert_eq!(
        cursor.rows_reencrypted, 1,
        "the fresh pass's own count must land, not a value carried over"
    );
}
