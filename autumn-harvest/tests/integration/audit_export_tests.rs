#![cfg(feature = "db")]
// Test-code style lints (consistent with the other integration suites here).
#![allow(
    clippy::doc_markdown,
    clippy::items_after_statements,
    clippy::missing_const_for_fn,
    clippy::too_many_lines,
    clippy::significant_drop_tightening
)]
//! Audit-record export to an external SIEM sink — issue #953.
//!
//! Verifies the claim/deliver/acknowledge pipeline against a real Postgres
//! container. The invariants under test are the compliance ones:
//!
//! - `unconfigured_export_never_touches_anything` — AC8's "byte-identical
//!   behavior when no sink is registered", asserted on the column, the cursor
//!   table, and the scanner's return value.
//! - `every_record_is_exported_with_a_dense_monotonic_sequence` — AC4.
//! - `a_failing_sink_never_advances_the_cursor` and
//!   `the_same_batch_is_retried_after_a_failure` — AC2's "never advances past
//!   the failure".
//! - `records_committed_after_a_batch_are_never_skipped` — the property that
//!   rules out a `BIGSERIAL`: a row that becomes visible after an export tick
//!   gets a LATER sequence and is still delivered.
//! - `a_crash_between_delivery_and_acknowledgement_redelivers` — AC9's restart
//!   survival: at-least-once, and the cursor never skipped.
//! - `a_stale_claim_cannot_acknowledge_over_a_fresher_one` — the claim-epoch
//!   guard.
//! - `redrive_re_exports_byte_identical_records`,
//!   `redrive_refuses_to_move_the_cursor_forward`,
//!   `redrive_by_timestamp_re_exports_from_that_instant` — AC6.
//! - `an_in_flight_acknowledgement_cannot_undo_a_redrive`.
//! - `export_status_reports_cursor_lag_and_state` — AC7.
//! - `retention_never_purges_an_unexported_record` and its
//!   unconfigured counterpart — the silent-loss hole a naive retention sweep
//!   would open.
//! - `every_batch_is_hmac_signed_and_carries_its_shard_and_seq_range` — AC1/AC4.
//! - `a_shard_the_scanner_cannot_acquire_a_connection_for_is_marked_unobserved`
//!   and `a_shard_whose_database_is_unreachable_is_marked_unobserved` — issue
//!   #1268: `harvest.audit.export_observed` reports an unreachable shard, so
//!   the lag gauge is never the only signal.

use std::sync::{Arc, Mutex};

use autumn_harvest::audit::{
    self, OP_WORKFLOW_CANCEL, STATUS_SUCCEEDED, TARGET_WORKFLOW, purge_old_audit_records,
};
use autumn_harvest::audit_export::{
    AuditBatch, AuditExportRecord, AuditExportRuntimeConfig, AuditSink, ExportBackoff,
    ExportOutcome, GLOBAL_AUDIT_EXPORT_CONFIG, RewindOutcome, RewindRequest, SinkAttempt,
    SinkFuture, apply_outcome, claim_shard, classify_export_outcome, ensure_cursor_row,
    export_status, fire_due_audit_exports, rewind_cursor, serialize_batch,
};
use autumn_harvest::completion_callback::CallbackSecret;
use autumn_harvest::models::NewAuditRecord;
use autumn_harvest::schema::harvest_audit_log;
use autumn_harvest::shard::ShardedDbPool;
use autumn_harvest::telemetry::{MetricsRecorder, NoOpMetrics};
use autumn_harvest::types::ShardId;
use autumn_harvest::worker::DbPool;

use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

// `GLOBAL_AUDIT_EXPORT_CONFIG` is a process-wide static (by design — it mirrors
// `GLOBAL_CALLBACK_CONFIG`'s "set once at startup" contract in production), so
// every test that installs one must serialize against the others.
static TEST_SERIAL: std::sync::LazyLock<tokio::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

// ── harness ──────────────────────────────────────────────────────────────────

async fn make_conn() -> (
    diesel_async::AsyncPgConnection,
    testcontainers::ContainerAsync<Postgres>,
) {
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres start");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgresql://postgres:postgres@127.0.0.1:{port}/postgres");
    let mut conn = diesel_async::AsyncPgConnection::establish(&url)
        .await
        .expect("connect");
    conn.batch_execute(autumn_harvest::full_migrations_sql())
        .await
        .expect("migration");
    (conn, container)
}

/// A pool for the same database that can hand out exactly **one** connection.
///
/// The shape that matters for the scoped scanner: with the scanner already
/// holding the only connection, any second checkout is unsatisfiable.
async fn single_connection_pool(
    container: &testcontainers::ContainerAsync<Postgres>,
) -> autumn_harvest::worker::DbPool {
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgresql://postgres:postgres@127.0.0.1:{port}/postgres");
    let manager = diesel_async::pooled_connection::AsyncDieselConnectionManager::<
        diesel_async::AsyncPgConnection,
    >::new(url);
    autumn_harvest::worker::DbPool::builder(manager)
        .max_size(1)
        .build()
        .expect("single-connection pool")
}

/// One captured delivery: what the sink was handed.
#[derive(Debug, Clone)]
struct CapturedBatch {
    shard: i32,
    first_seq: i64,
    last_seq: i64,
    body: Vec<u8>,
    headers: Vec<(&'static str, String)>,
    seqs: Vec<i64>,
    ids: Vec<uuid::Uuid>,
}

/// Test sink: records every batch and answers with a scripted status.
struct RecordingSink {
    batches: Mutex<Vec<CapturedBatch>>,
    status: Mutex<u16>,
}

impl RecordingSink {
    fn new(status: u16) -> Self {
        Self {
            batches: Mutex::new(Vec::new()),
            status: Mutex::new(status),
        }
    }

    fn set_status(&self, status: u16) {
        *self.status.lock().expect("status lock") = status;
    }

    fn captured(&self) -> Vec<CapturedBatch> {
        self.batches.lock().expect("batch lock").clone()
    }

    /// Every sequence the sink has ever been handed, in delivery order.
    fn all_seqs(&self) -> Vec<i64> {
        self.captured().into_iter().flat_map(|b| b.seqs).collect()
    }
}

impl AuditSink for RecordingSink {
    fn deliver<'a>(&'a self, batch: &'a AuditBatch<'a>) -> SinkFuture<'a> {
        let captured = CapturedBatch {
            shard: batch.shard,
            first_seq: batch.first_seq,
            last_seq: batch.last_seq,
            body: batch.body.to_vec(),
            headers: batch.headers.to_vec(),
            seqs: batch.records.iter().map(|r| r.seq).collect(),
            ids: batch.records.iter().map(|r| r.id).collect(),
        };
        self.batches.lock().expect("batch lock").push(captured);
        let status = *self.status.lock().expect("status lock");
        Box::pin(async move { SinkAttempt::success(status) })
    }
}

/// Records `harvest.audit.export_lag` / `harvest.audit.export_observed` /
/// `harvest.audit.exported` samples.
#[derive(Default)]
struct RecordingMetrics {
    lag: Mutex<Vec<(u16, f64)>>,
    observed: Mutex<Vec<(u16, bool)>>,
    exported: Mutex<Vec<(u16, u64)>>,
}

impl MetricsRecorder for RecordingMetrics {
    fn record_audit_export_lag(&self, shard: u16, seconds: f64) {
        self.lag.lock().expect("lag lock").push((shard, seconds));
    }
    fn record_audit_export_observed(&self, shard: u16, observed: bool) {
        self.observed
            .lock()
            .expect("observed lock")
            .push((shard, observed));
    }
    fn record_audit_exported(&self, shard: u16, count: u64) {
        self.exported
            .lock()
            .expect("exported lock")
            .push((shard, count));
    }
}

/// Install a runtime config and return the sink for assertions.
fn install(sink: Arc<RecordingSink>, batch_size: i64) -> Arc<RecordingSink> {
    install_with_lease(sink, batch_size, std::time::Duration::from_secs(60))
}

fn install_with_lease(
    sink: Arc<RecordingSink>,
    batch_size: i64,
    lease: std::time::Duration,
) -> Arc<RecordingSink> {
    let mut lock = GLOBAL_AUDIT_EXPORT_CONFIG
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *lock = Some(Arc::new(AuditExportRuntimeConfig {
        sink: sink.clone(),
        secret: CallbackSecret::new(b"test-secret".to_vec()),
        batch_size,
        backoff: ExportBackoff::default(),
        lease,
    }));
    sink
}

fn uninstall() {
    let mut lock = GLOBAL_AUDIT_EXPORT_CONFIG
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *lock = None;
}

async fn insert_audit_rows(conn: &mut diesel_async::AsyncPgConnection, count: usize) {
    for i in 0..count {
        let target = format!("exec-{i}");
        let record = NewAuditRecord {
            actor: "alice",
            operation: OP_WORKFLOW_CANCEL,
            target_type: TARGET_WORKFLOW,
            target_id: Some(target.as_str()),
            route_or_command: "POST /workflows/{id}/cancel",
            request_id: None,
            idempotency_key: None,
            status: STATUS_SUCCEEDED,
            error_summary: None,
            shard_id: Some(0),
            source: "api",
        };
        audit::insert_audit(conn, &record)
            .await
            .expect("audit insert");
    }
}

async fn export_seqs(conn: &mut diesel_async::AsyncPgConnection) -> Vec<Option<i64>> {
    harvest_audit_log::table
        .select(harvest_audit_log::export_seq)
        .order(harvest_audit_log::occurred_at.asc())
        .load::<Option<i64>>(conn)
        .await
        .expect("load export_seq")
}

async fn cursor_acked(conn: &mut diesel_async::AsyncPgConnection, shard: i32) -> i64 {
    use autumn_harvest::schema::harvest_audit_export_cursor::dsl as cur;
    cur::harvest_audit_export_cursor
        .find(shard)
        .select(cur::last_acked_seq)
        .first::<i64>(conn)
        .await
        .expect("cursor row")
}

// ── AC8: opt-in and zero-cost when unconfigured ──────────────────────────────

#[tokio::test]
async fn unconfigured_export_never_touches_anything() {
    let _guard = TEST_SERIAL.lock().await;
    uninstall();
    let (mut conn, _c) = make_conn().await;
    insert_audit_rows(&mut conn, 5).await;

    let metrics = RecordingMetrics::default();
    let processed = fire_due_audit_exports(&mut conn, &None, &[], &metrics)
        .await
        .expect("scanner runs");
    assert_eq!(processed, 0, "no sink configured means no work at all");

    assert!(
        export_seqs(&mut conn).await.iter().all(Option::is_none),
        "no sequence may be assigned when no sink is configured"
    );
    assert_eq!(
        export_status(&mut conn, 0, chrono::Utc::now())
            .await
            .expect("status query"),
        None,
        "no cursor row is created when no sink is configured"
    );
    assert!(
        metrics.lag.lock().expect("lag").is_empty(),
        "the lag gauge must not be touched when no sink is configured"
    );
    assert!(
        metrics.observed.lock().expect("observed").is_empty(),
        "harvest.audit.export_observed is part of the same AC8 contract: an \
         embedder who never configures a sink must see zero metrics, not \
         just zero database writes"
    );
}

// ── AC4: dense, strictly monotonic per-shard sequences ───────────────────────

#[tokio::test]
async fn every_record_is_exported_with_a_dense_monotonic_sequence() {
    let _guard = TEST_SERIAL.lock().await;
    let sink = install(Arc::new(RecordingSink::new(200)), 100);
    let (mut conn, _c) = make_conn().await;
    insert_audit_rows(&mut conn, 7).await;

    let processed = fire_due_audit_exports(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("scanner runs");
    uninstall();

    assert_eq!(processed, 7);
    assert_eq!(
        sink.all_seqs(),
        (1..=7).collect::<Vec<i64>>(),
        "sequences must be dense and start at 1, so a receiver can check \
         contiguity and not merely detect gaps"
    );
    assert_eq!(cursor_acked(&mut conn, 0).await, 7);

    let batches = sink.captured();
    assert_eq!(batches.len(), 1, "one batch under the batch-size cap");
    assert_eq!(batches[0].shard, 0);
    assert_eq!((batches[0].first_seq, batches[0].last_seq), (1, 7));
}

#[tokio::test]
async fn every_batch_is_hmac_signed_and_carries_its_shard_and_seq_range() {
    let _guard = TEST_SERIAL.lock().await;
    let sink = install(Arc::new(RecordingSink::new(200)), 100);
    let (mut conn, _c) = make_conn().await;
    insert_audit_rows(&mut conn, 3).await;
    fire_due_audit_exports(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("scanner runs");
    uninstall();

    let batch = sink.captured().pop().expect("one batch");
    let header = |name: &str| {
        batch.headers.iter().find(|(n, _)| *n == name).map_or_else(
            || panic!("{name} header present"),
            |(_, value)| value.clone(),
        )
    };
    assert_eq!(
        header(autumn_harvest::audit_export::SIGNATURE_HEADER),
        autumn_harvest::completion_callback::sign(
            &CallbackSecret::new(b"test-secret".to_vec()),
            &batch.body
        ),
        "the signature must cover the exact bytes delivered"
    );
    assert_eq!(header(autumn_harvest::audit_export::SHARD_HEADER), "0");
    assert_eq!(header(autumn_harvest::audit_export::FIRST_SEQ_HEADER), "1");
    assert_eq!(header(autumn_harvest::audit_export::LAST_SEQ_HEADER), "3");

    // JSON lines: one record per line, newline-terminated.
    let text = String::from_utf8(batch.body.clone()).expect("utf8 body");
    assert_eq!(text.lines().count(), 3);
    for line in text.lines() {
        let value: serde_json::Value = serde_json::from_str(line).expect("each line is JSON");
        assert_eq!(value["shard"], serde_json::json!(0));
        assert!(value["seq"].is_i64());
    }
}

// ── AC2: never advance past a failure ────────────────────────────────────────

#[tokio::test]
async fn a_failing_sink_never_advances_the_cursor() {
    let _guard = TEST_SERIAL.lock().await;
    let sink = install(Arc::new(RecordingSink::new(500)), 100);
    let (mut conn, _c) = make_conn().await;
    insert_audit_rows(&mut conn, 4).await;

    let processed = fire_due_audit_exports(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("scanner runs");
    uninstall();

    assert_eq!(processed, 0, "a 500 delivers nothing");
    assert_eq!(
        cursor_acked(&mut conn, 0).await,
        0,
        "the cursor must not move past a failed delivery"
    );
    assert_eq!(
        sink.all_seqs(),
        vec![1, 2, 3, 4],
        "the batch was attempted; it simply was not acknowledged"
    );

    let status = export_status(&mut conn, 0, chrono::Utc::now())
        .await
        .expect("status")
        .expect("cursor exists");
    assert_eq!(status.consecutive_failures, 1);
    assert_eq!(status.last_status, Some(500));
    assert_eq!(status.pending_records, 4, "nothing was acknowledged");
}

#[tokio::test]
async fn the_same_batch_is_retried_after_a_failure() {
    let _guard = TEST_SERIAL.lock().await;
    let sink = install(Arc::new(RecordingSink::new(503)), 100);
    let (mut conn, _c) = make_conn().await;
    insert_audit_rows(&mut conn, 3).await;

    fire_due_audit_exports(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("first tick");

    // The failure scheduled a backoff. Clear it (a test stands in for waiting
    // out the capped exponential delay) and let the now-healthy sink retry.
    {
        use autumn_harvest::schema::harvest_audit_export_cursor::dsl as cur;
        diesel::update(cur::harvest_audit_export_cursor.find(0))
            .set(cur::next_attempt_at.eq(chrono::Utc::now()))
            .execute(&mut conn)
            .await
            .expect("clear backoff");
    }
    sink.set_status(200);
    let processed = fire_due_audit_exports(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("second tick");
    uninstall();

    assert_eq!(processed, 3);
    assert_eq!(cursor_acked(&mut conn, 0).await, 3);
    let batches = sink.captured();
    assert_eq!(batches.len(), 2, "the failed batch was retried");
    assert_eq!(
        batches[0].seqs, batches[1].seqs,
        "the retry re-sends exactly the same records"
    );
    assert_eq!(
        batches[0].body, batches[1].body,
        "and byte-identical bytes, so the receiver's (shard, seq) dedup sees \
         the same payload it already stored"
    );
}

// ── The property that rules out a BIGSERIAL ──────────────────────────────────

#[tokio::test]
async fn records_committed_after_a_batch_are_never_skipped() {
    let _guard = TEST_SERIAL.lock().await;
    let sink = install(Arc::new(RecordingSink::new(200)), 100);
    let (mut conn, _c) = make_conn().await;
    insert_audit_rows(&mut conn, 3).await;
    fire_due_audit_exports(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("first tick");

    // A record that becomes visible only AFTER the first export tick. With a
    // pre-commit BIGSERIAL it could have taken a sequence below the cursor and
    // been skipped forever; with exporter-assigned sequences it is simply
    // still NULL and gets a later one.
    insert_audit_rows(&mut conn, 2).await;
    let processed = fire_due_audit_exports(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("second tick");
    uninstall();

    assert_eq!(processed, 2);
    assert_eq!(
        sink.all_seqs(),
        vec![1, 2, 3, 4, 5],
        "the late records get LATER sequences and are delivered, never skipped"
    );
    assert_eq!(cursor_acked(&mut conn, 0).await, 5);
    let seqs = export_seqs(&mut conn).await;
    assert_eq!(
        seqs,
        vec![Some(1), Some(2), Some(3), Some(4), Some(5)],
        "every row carries exactly one sequence"
    );
}

// ── AC9: restart survival ────────────────────────────────────────────────────

#[tokio::test]
async fn a_crash_between_delivery_and_acknowledgement_redelivers() {
    let _guard = TEST_SERIAL.lock().await;
    // A zero-length lease stands in for "the process died holding this claim
    // and the lease has since expired" without needing to kill a real process.
    let sink = install_with_lease(
        Arc::new(RecordingSink::new(200)),
        100,
        std::time::Duration::from_secs(0),
    );
    let (mut conn, _c) = make_conn().await;
    insert_audit_rows(&mut conn, 4).await;

    // Phase 1: claim and deliver by hand, then simply never acknowledge —
    // exactly what a `kill -9` between the POST and the cursor write leaves
    // behind.
    ensure_cursor_row(&mut conn, 0).await.expect("cursor row");
    let claim = claim_shard(
        &mut conn,
        0,
        100,
        std::time::Duration::from_secs(0),
        chrono::Utc::now(),
    )
    .await
    .expect("claim")
    .expect("something to claim");
    let body = serialize_batch(&claim.records).expect("serialize");
    let headers = autumn_harvest::audit_export::export_headers(
        &CallbackSecret::new(b"test-secret".to_vec()),
        &body,
        0,
        claim.records[0].seq,
        claim.records[claim.records.len() - 1].seq,
        chrono::Utc::now(),
    );
    let batch = AuditBatch {
        shard: 0,
        first_seq: claim.records[0].seq,
        last_seq: claim.records[claim.records.len() - 1].seq,
        records: &claim.records,
        body: &body,
        headers: &headers,
    };
    let attempt = sink.deliver(&batch).await;
    assert!(attempt.is_success(), "the sink DID accept this batch");
    // ...and then the process dies. No `apply_outcome` call.

    assert_eq!(
        cursor_acked(&mut conn, 0).await,
        0,
        "the cursor never advanced, so nothing is marked delivered that was \
         not durably recorded as delivered"
    );

    // Phase 2: restart. The expired lease lets a fresh claim take the shard.
    let processed = fire_due_audit_exports(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("post-restart tick");
    uninstall();

    assert_eq!(processed, 4);
    assert_eq!(cursor_acked(&mut conn, 0).await, 4);

    // At-least-once: every record reached the sink at least once, and the
    // pre-crash batch was simply re-sent with the same sequences.
    let seqs = sink.all_seqs();
    for expected in 1..=4_i64 {
        assert!(
            seqs.contains(&expected),
            "seq {expected} must be delivered at least once; got {seqs:?}"
        );
    }
    let batches = sink.captured();
    assert_eq!(batches.len(), 2, "the pre-crash batch was redelivered");
    assert_eq!(batches[0].seqs, batches[1].seqs);
    assert_eq!(batches[0].ids, batches[1].ids);
    assert_eq!(
        batches[0].body, batches[1].body,
        "redelivery is byte-identical, so the receiver dedupes on (shard, seq)"
    );
}

#[tokio::test]
async fn a_stale_claim_cannot_acknowledge_over_a_fresher_one() {
    let _guard = TEST_SERIAL.lock().await;
    uninstall();
    let (mut conn, _c) = make_conn().await;
    insert_audit_rows(&mut conn, 2).await;
    ensure_cursor_row(&mut conn, 0).await.expect("cursor row");

    let stale = claim_shard(
        &mut conn,
        0,
        100,
        std::time::Duration::from_secs(0),
        chrono::Utc::now(),
    )
    .await
    .expect("first claim")
    .expect("something to claim");

    // The lease has already expired, so a second exporter claims the shard.
    let fresh = claim_shard(
        &mut conn,
        0,
        100,
        std::time::Duration::from_secs(60),
        chrono::Utc::now(),
    )
    .await
    .expect("second claim")
    .expect("still claimable");
    assert!(fresh.claim_epoch > stale.claim_epoch);

    // The stale attempt's HTTP call finally returns. Its acknowledgement must
    // be refused outright — not merged, not partially applied.
    let applied = apply_outcome(
        &mut conn,
        0,
        stale.claim_epoch,
        &ExportOutcome::Advance {
            through_seq: 2,
            status: 200,
        },
        chrono::Utc::now(),
    )
    .await
    .expect("apply");
    assert!(!applied, "a superseded claim may not write the cursor");
    assert_eq!(cursor_acked(&mut conn, 0).await, 0);

    // The fresh claim's acknowledgement applies normally.
    let applied = apply_outcome(
        &mut conn,
        0,
        fresh.claim_epoch,
        &ExportOutcome::Advance {
            through_seq: 2,
            status: 200,
        },
        chrono::Utc::now(),
    )
    .await
    .expect("apply");
    assert!(applied);
    assert_eq!(cursor_acked(&mut conn, 0).await, 2);
}

// ── AC6: redrive ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn redrive_re_exports_byte_identical_records() {
    let _guard = TEST_SERIAL.lock().await;
    let sink = install(Arc::new(RecordingSink::new(200)), 100);
    let (mut conn, _c) = make_conn().await;
    insert_audit_rows(&mut conn, 5).await;
    fire_due_audit_exports(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("first tick");
    assert_eq!(cursor_acked(&mut conn, 0).await, 5);

    // The SIEM lost everything after seq 2.
    let outcome = rewind_cursor(&mut conn, 0, RewindRequest::Seq(2), chrono::Utc::now())
        .await
        .expect("rewind");
    assert_eq!(outcome, RewindOutcome::Rewound { from: 5, to: 2 });

    let processed = fire_due_audit_exports(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("re-export tick");
    uninstall();

    assert_eq!(processed, 3);
    let batches = sink.captured();
    assert_eq!(batches.len(), 2);
    assert_eq!(
        batches[1].seqs,
        vec![3, 4, 5],
        "only the lost tail re-exports"
    );

    // Byte-identity: the re-exported lines are exactly the lines originally
    // sent for those sequences.
    let original = String::from_utf8(batches[0].body.clone()).expect("utf8");
    let replayed = String::from_utf8(batches[1].body.clone()).expect("utf8");
    let original_tail: Vec<&str> = original.lines().skip(2).collect();
    assert_eq!(
        original_tail,
        replayed.lines().collect::<Vec<&str>>(),
        "a re-exported record must be byte-identical to its first delivery"
    );
    assert_eq!(cursor_acked(&mut conn, 0).await, 5);
}

#[tokio::test]
async fn redrive_refuses_to_move_the_cursor_forward() {
    let _guard = TEST_SERIAL.lock().await;
    let sink = install(Arc::new(RecordingSink::new(200)), 100);
    let (mut conn, _c) = make_conn().await;
    insert_audit_rows(&mut conn, 4).await;
    fire_due_audit_exports(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("tick");
    let _ = &sink;

    let outcome = rewind_cursor(&mut conn, 0, RewindRequest::Seq(99), chrono::Utc::now())
        .await
        .expect("rewind");
    uninstall();
    assert_eq!(
        outcome,
        RewindOutcome::NoOp {
            cursor: 4,
            requested: 99
        },
        "advancing the cursor would mark records delivered that never were"
    );
    assert_eq!(cursor_acked(&mut conn, 0).await, 4, "cursor unchanged");
}

#[tokio::test]
async fn redrive_of_a_shard_that_never_exported_is_reported_not_configured() {
    let _guard = TEST_SERIAL.lock().await;
    uninstall();
    let (mut conn, _c) = make_conn().await;
    let outcome = rewind_cursor(&mut conn, 7, RewindRequest::Seq(0), chrono::Utc::now())
        .await
        .expect("rewind");
    assert_eq!(outcome, RewindOutcome::NotConfigured);
}

#[tokio::test]
async fn redrive_by_timestamp_re_exports_from_that_instant() {
    let _guard = TEST_SERIAL.lock().await;
    let sink = install(Arc::new(RecordingSink::new(200)), 100);
    let (mut conn, _c) = make_conn().await;
    insert_audit_rows(&mut conn, 3).await;
    // A clock boundary between the first three records and the next three.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let boundary = chrono::Utc::now();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    insert_audit_rows(&mut conn, 3).await;

    fire_due_audit_exports(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("tick");
    assert_eq!(cursor_acked(&mut conn, 0).await, 6);

    let outcome = rewind_cursor(
        &mut conn,
        0,
        RewindRequest::Before(boundary),
        chrono::Utc::now(),
    )
    .await
    .expect("rewind");
    assert_eq!(
        outcome,
        RewindOutcome::Rewound { from: 6, to: 3 },
        "rewinding to an instant re-exports everything at or after it"
    );

    fire_due_audit_exports(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("re-export");
    uninstall();
    let batches = sink.captured();
    assert_eq!(batches[batches.len() - 1].seqs, vec![4, 5, 6]);
}

#[tokio::test]
async fn an_in_flight_acknowledgement_cannot_undo_a_redrive() {
    let _guard = TEST_SERIAL.lock().await;
    uninstall();
    let (mut conn, _c) = make_conn().await;
    insert_audit_rows(&mut conn, 5).await;
    ensure_cursor_row(&mut conn, 0).await.expect("cursor row");

    // A delivery is claimed and in flight...
    let claim = claim_shard(
        &mut conn,
        0,
        100,
        std::time::Duration::from_secs(60),
        chrono::Utc::now(),
    )
    .await
    .expect("claim")
    .expect("claimable");
    apply_outcome(
        &mut conn,
        0,
        claim.claim_epoch,
        &ExportOutcome::Advance {
            through_seq: 5,
            status: 200,
        },
        chrono::Utc::now(),
    )
    .await
    .expect("first ack");
    assert_eq!(cursor_acked(&mut conn, 0).await, 5);

    // ...an operator redrives...
    let redrive = rewind_cursor(&mut conn, 0, RewindRequest::Seq(1), chrono::Utc::now())
        .await
        .expect("rewind");
    assert_eq!(redrive, RewindOutcome::Rewound { from: 5, to: 1 });

    // ...and a straggler from the pre-redrive claim finally acknowledges. It
    // must not silently re-raise the cursor and undo the operator's redrive.
    let applied = apply_outcome(
        &mut conn,
        0,
        claim.claim_epoch,
        &ExportOutcome::Advance {
            through_seq: 5,
            status: 200,
        },
        chrono::Utc::now(),
    )
    .await
    .expect("stale ack");
    assert!(
        !applied,
        "the redrive bumped the epoch, refusing the stale ack"
    );
    assert_eq!(cursor_acked(&mut conn, 0).await, 1, "the redrive stands");
}

// ── AC5/AC7: metrics and status ──────────────────────────────────────────────

#[tokio::test]
async fn export_status_reports_cursor_lag_and_state() {
    let _guard = TEST_SERIAL.lock().await;
    let sink = install(Arc::new(RecordingSink::new(200)), 2);
    let (mut conn, _c) = make_conn().await;
    insert_audit_rows(&mut conn, 5).await;

    let metrics = RecordingMetrics::default();
    fire_due_audit_exports(&mut conn, &None, &[], &metrics)
        .await
        .expect("tick");

    // A batch size of 2 leaves a backlog after one tick.
    let status = export_status(&mut conn, 0, chrono::Utc::now())
        .await
        .expect("status")
        .expect("cursor");
    assert_eq!(status.shard, 0);
    assert_eq!(status.cursor_seq, 2, "one batch acknowledged");
    assert_eq!(status.pending_records, 3);
    assert!(status.lag_seconds >= 0.0);
    assert_eq!(status.delivery_state, "IDLE");
    assert_eq!(status.consecutive_failures, 0);
    assert_eq!(status.last_status, Some(200));

    // The lag gauge is emitted on every tick, and the exported counter only
    // for the acknowledged batch.
    assert!(
        !metrics.lag.lock().expect("lag").is_empty(),
        "harvest.audit.export_lag must be emitted every tick"
    );
    assert_eq!(
        *metrics.observed.lock().expect("observed"),
        vec![(0_u16, true)],
        "a successful tick reports the shard as observed"
    );
    assert_eq!(
        *metrics.exported.lock().expect("exported"),
        vec![(0_u16, 2)]
    );

    // Drain the rest and confirm the lag returns to zero.
    while fire_due_audit_exports(&mut conn, &None, &[], &metrics)
        .await
        .expect("tick")
        > 0
    {}
    uninstall();
    let status = export_status(&mut conn, 0, chrono::Utc::now())
        .await
        .expect("status")
        .expect("cursor");
    assert_eq!(status.pending_records, 0);
    assert!(
        (status.lag_seconds - 0.0).abs() < f64::EPSILON,
        "lag is 0 when nothing is pending, got {}",
        status.lag_seconds
    );
    assert_eq!(sink.all_seqs(), vec![1, 2, 3, 4, 5]);
}

// ── Retention must never silently drop an unexported record ──────────────────

#[tokio::test]
async fn retention_never_purges_an_unexported_record() {
    let _guard = TEST_SERIAL.lock().await;
    let sink = install(Arc::new(RecordingSink::new(200)), 2);
    let (mut conn, _c) = make_conn().await;
    insert_audit_rows(&mut conn, 5).await;

    // Export only the first two, then age every row past the retention window.
    fire_due_audit_exports(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("tick");
    assert_eq!(cursor_acked(&mut conn, 0).await, 2);
    diesel::update(harvest_audit_log::table)
        .set(harvest_audit_log::occurred_at.eq(chrono::Utc::now() - chrono::Duration::days(365)))
        .execute(&mut conn)
        .await
        .expect("age rows");

    let deleted = purge_old_audit_records(&mut conn, 90)
        .await
        .expect("purge runs");
    assert_eq!(
        deleted, 2,
        "only the acknowledged records may be purged; the three the sink has \
         never seen must survive, or they would be gone from the database AND \
         absent from the SIEM with nothing anywhere to show it"
    );

    let remaining: i64 = harvest_audit_log::table
        .count()
        .get_result(&mut conn)
        .await
        .expect("count");
    assert_eq!(remaining, 3);

    // And they still export afterwards.
    while fire_due_audit_exports(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("tick")
        > 0
    {}
    uninstall();
    assert_eq!(sink.all_seqs(), vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn retention_is_unchanged_when_export_is_unconfigured() {
    let _guard = TEST_SERIAL.lock().await;
    uninstall();
    let (mut conn, _c) = make_conn().await;
    insert_audit_rows(&mut conn, 5).await;
    diesel::update(harvest_audit_log::table)
        .set(harvest_audit_log::occurred_at.eq(chrono::Utc::now() - chrono::Duration::days(365)))
        .execute(&mut conn)
        .await
        .expect("age rows");

    let deleted = purge_old_audit_records(&mut conn, 90)
        .await
        .expect("purge runs");
    assert_eq!(
        deleted, 5,
        "with no export cursor the guard finds nothing and the purge is \
         byte-identical to its pre-#953 behaviour"
    );
}

// ── Backoff decision is applied, not merely computed ─────────────────────────

#[tokio::test]
async fn a_failure_schedules_a_future_retry_without_moving_the_cursor() {
    let _guard = TEST_SERIAL.lock().await;
    uninstall();
    let (mut conn, _c) = make_conn().await;
    insert_audit_rows(&mut conn, 1).await;
    ensure_cursor_row(&mut conn, 0).await.expect("cursor");
    let claim = claim_shard(
        &mut conn,
        0,
        10,
        std::time::Duration::from_secs(60),
        chrono::Utc::now(),
    )
    .await
    .expect("claim")
    .expect("claimable");

    let now = chrono::Utc::now();
    let outcome = classify_export_outcome(
        &SinkAttempt::transport_error("connection refused".to_string()),
        1,
        claim.consecutive_failures,
        &ExportBackoff::default(),
        now,
    );
    assert!(
        apply_outcome(&mut conn, 0, claim.claim_epoch, &outcome, now)
            .await
            .expect("apply")
    );

    let status = export_status(&mut conn, 0, chrono::Utc::now())
        .await
        .expect("status")
        .expect("cursor");
    assert_eq!(status.cursor_seq, 0);
    assert_eq!(status.consecutive_failures, 1);
    assert_eq!(status.last_error.as_deref(), Some("connection refused"));
    assert!(status.next_attempt_at > now);
    assert_eq!(status.delivery_state, "BACKOFF");

    // While backing off, a tick claims nothing rather than hammering the sink.
    let claimed = claim_shard(
        &mut conn,
        0,
        10,
        std::time::Duration::from_secs(60),
        chrono::Utc::now(),
    )
    .await
    .expect("claim");
    assert!(claimed.is_none(), "a shard in backoff is not re-claimed");
}

// ── The lag gauge measures the OLDEST unacknowledged record ─────────────────

// The feature's central SLO signal. Defined against the *newest* unexported
// record instead, this gauge would read ~0 under sustained load during exactly
// the outage an operator needs to see, because a stuck exporter always has a
// brand-new unexported record.
#[tokio::test]
async fn lag_is_the_age_of_the_oldest_unacknowledged_record_not_the_newest() {
    let _guard = TEST_SERIAL.lock().await;
    uninstall();
    let (mut conn, _c) = make_conn().await;
    insert_audit_rows(&mut conn, 2).await;

    // Backdate exactly one row by two minutes and leave the other at "now".
    let oldest_id: uuid::Uuid = harvest_audit_log::table
        .select(harvest_audit_log::id)
        .order(harvest_audit_log::occurred_at.asc())
        .first(&mut conn)
        .await
        .expect("first row");
    diesel::update(harvest_audit_log::table.filter(harvest_audit_log::id.eq(oldest_id)))
        .set(harvest_audit_log::occurred_at.eq(chrono::Utc::now() - chrono::Duration::seconds(120)))
        .execute(&mut conn)
        .await
        .expect("backdate");

    let lag = autumn_harvest::audit_export::export_lag_seconds(&mut conn, 0, chrono::Utc::now())
        .await
        .expect("lag query");
    assert!(
        lag > 100.0,
        "lag must track the OLDEST unacknowledged record (~120s); a \
         newest-based definition would report ~0 here, which is exactly the \
         reading that would hide a stalled exporter under load. got {lag}"
    );

    // The admin view's O(backlog) variant must agree with the cheap one.
    let (pending, admin_lag) =
        autumn_harvest::audit_export::pending_and_lag(&mut conn, 0, chrono::Utc::now())
            .await
            .expect("admin lag");
    assert_eq!(pending, 2);
    assert!(
        (admin_lag - lag).abs() < 5.0,
        "the per-tick gauge and the admin view must not disagree: {lag} vs {admin_lag}"
    );
}

#[tokio::test]
async fn lag_covers_records_that_are_sequenced_but_not_yet_acknowledged() {
    let _guard = TEST_SERIAL.lock().await;
    // Batch of 1 against 2 records: the second is sequenced on the first tick
    // but left unacknowledged, so it must still count toward lag.
    let _sink = install(Arc::new(RecordingSink::new(200)), 1);
    let (mut conn, _c) = make_conn().await;
    insert_audit_rows(&mut conn, 2).await;
    diesel::update(harvest_audit_log::table)
        .set(harvest_audit_log::occurred_at.eq(chrono::Utc::now() - chrono::Duration::seconds(90)))
        .execute(&mut conn)
        .await
        .expect("age rows");

    fire_due_audit_exports(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("tick");
    let acked = cursor_acked(&mut conn, 0).await;
    assert_eq!(acked, 1);
    uninstall();

    let lag =
        autumn_harvest::audit_export::export_lag_seconds(&mut conn, acked, chrono::Utc::now())
            .await
            .expect("lag");
    assert!(
        lag > 60.0,
        "a sequenced-but-unacknowledged record is undelivered and must count \
         toward lag; got {lag}"
    );
}

// ── The lease is what stops two exporters delivering a shard concurrently ───

#[tokio::test]
async fn a_live_lease_blocks_a_second_claim_until_it_expires() {
    let _guard = TEST_SERIAL.lock().await;
    uninstall();
    let (mut conn, _c) = make_conn().await;
    insert_audit_rows(&mut conn, 3).await;
    ensure_cursor_row(&mut conn, 0).await.expect("cursor row");

    let now = chrono::Utc::now();
    let lease = std::time::Duration::from_secs(60);
    let first = claim_shard(&mut conn, 0, 100, lease, now)
        .await
        .expect("first claim")
        .expect("claimable");

    // Same instant: the lease is live, so a second exporter must get nothing
    // rather than delivering the same batch concurrently.
    assert!(
        claim_shard(&mut conn, 0, 100, lease, now)
            .await
            .expect("second claim")
            .is_none(),
        "a live lease must block a concurrent claim"
    );

    // A moment before expiry: still blocked.
    assert!(
        claim_shard(
            &mut conn,
            0,
            100,
            lease,
            now + chrono::Duration::seconds(59)
        )
        .await
        .expect("claim")
        .is_none()
    );

    // After expiry: reclaimable, with a fresh epoch, so a crashed exporter's
    // shard self-heals.
    let second = claim_shard(
        &mut conn,
        0,
        100,
        lease,
        now + chrono::Duration::seconds(61),
    )
    .await
    .expect("claim")
    .expect("reclaimable once the lease expires");
    assert!(second.claim_epoch > first.claim_epoch);
    assert_eq!(
        second.records.iter().map(|r| r.seq).collect::<Vec<_>>(),
        first.records.iter().map(|r| r.seq).collect::<Vec<_>>(),
        "the reclaim re-delivers the same batch (at-least-once), it does not \
         skip past it"
    );
}

// ── Multi-shard fan-out ─────────────────────────────────────────────────────

#[tokio::test]
async fn a_non_default_shard_labels_its_own_records_and_keeps_its_own_cursor() {
    let _guard = TEST_SERIAL.lock().await;
    let sink = install(Arc::new(RecordingSink::new(200)), 100);
    let (conn, container) = make_conn().await;
    drop(conn);
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgresql://postgres:postgres@127.0.0.1:{port}/postgres");
    let pool = DbPool::builder(AsyncDieselConnectionManager::<AsyncPgConnection>::new(&url))
        .build()
        .expect("pool");
    let mut conn = pool.get().await.expect("conn");
    insert_audit_rows(&mut conn, 3).await;

    // Shard 3 is the only assigned shard, and it is not the hardcoded 0.
    let sharded = Some(ShardedDbPool::from_map(
        std::collections::BTreeMap::from([(ShardId::new(3), pool.clone())]),
        ShardId::new(3),
    ));
    let processed = fire_due_audit_exports(
        &mut conn,
        &sharded,
        &[ShardId::new(3), ShardId::new(9)],
        &NoOpMetrics,
    )
    .await
    .expect("sharded tick");
    uninstall();

    assert_eq!(
        processed, 3,
        "shard 9 has no pool in this process and must be skipped without \
         stopping shard 3's export"
    );
    let batch = sink.captured().pop().expect("one batch");
    assert_eq!(
        batch.shard, 3,
        "records must carry the shard they came from"
    );
    let text = String::from_utf8(batch.body.clone()).expect("utf8");
    for line in text.lines() {
        let value: serde_json::Value = serde_json::from_str(line).expect("json");
        assert_eq!(value["shard"], serde_json::json!(3));
    }
    let header = batch
        .headers
        .iter()
        .find(|(n, _)| *n == autumn_harvest::audit_export::SHARD_HEADER)
        .map(|(_, v)| v.clone())
        .expect("shard header");
    assert_eq!(header, "3");
    assert_eq!(cursor_acked(&mut conn, 3).await, 3);
    assert_eq!(
        export_status(&mut conn, 0, chrono::Utc::now())
            .await
            .expect("status"),
        None,
        "no cursor row may be created for a shard this exporter never scanned"
    );
}

// ── The exported record carries the audit row's own shard_id ────────────────

#[tokio::test]
async fn the_exported_record_carries_both_the_source_shard_and_the_rows_own_shard_id() {
    let _guard = TEST_SERIAL.lock().await;
    let sink = install(Arc::new(RecordingSink::new(200)), 100);
    let (mut conn, _c) = make_conn().await;
    // A control-plane mutation records the shard it acted on while its audit
    // row lands on the default shard — the two genuinely differ, and a
    // receiver correlating them must see both.
    let record = NewAuditRecord {
        actor: "alice",
        operation: "audit_export.redrive",
        target_type: "audit_export",
        target_id: Some("shard=5;to_seq=1"),
        route_or_command: "POST /admin/audit-export/redrive",
        request_id: None,
        idempotency_key: None,
        status: STATUS_SUCCEEDED,
        error_summary: None,
        shard_id: Some(5),
        source: "api",
    };
    audit::insert_audit(&mut conn, &record)
        .await
        .expect("audit insert");

    fire_due_audit_exports(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("tick");
    uninstall();

    let batch = sink.captured().pop().expect("one batch");
    let text = String::from_utf8(batch.body).expect("utf8");
    let value: serde_json::Value = serde_json::from_str(text.trim_end_matches('\n')).expect("json");
    assert_eq!(
        value["shard"],
        serde_json::json!(0),
        "`shard` is the database the record was read from -- the dedup dimension"
    );
    assert_eq!(
        value["shard_id"],
        serde_json::json!(5),
        "`shard_id` is the shard the operation itself named, and must not be \
         silently replaced by the source shard"
    );
}

// ── Retention must fail safe before the exporter has reached a shard ────────

#[tokio::test]
async fn retention_purges_nothing_when_export_is_configured_but_has_not_run_yet() {
    let _guard = TEST_SERIAL.lock().await;
    // A sink is installed but no tick has happened, so no cursor row exists.
    // A cursor-only guard would be vacuous here and delete everything.
    let _sink = install(Arc::new(RecordingSink::new(200)), 100);
    let (mut conn, _c) = make_conn().await;
    insert_audit_rows(&mut conn, 5).await;
    diesel::update(harvest_audit_log::table)
        .set(harvest_audit_log::occurred_at.eq(chrono::Utc::now() - chrono::Duration::days(365)))
        .execute(&mut conn)
        .await
        .expect("age rows");

    let deleted = purge_old_audit_records(&mut conn, 90)
        .await
        .expect("purge runs");
    uninstall();
    assert_eq!(
        deleted, 0,
        "with export configured and no cursor yet, every record is unexported; \
         purging any of them would be the silent loss the guard exists to \
         prevent"
    );
    let remaining: i64 = harvest_audit_log::table
        .count()
        .get_result(&mut conn)
        .await
        .expect("count");
    assert_eq!(remaining, 5);
}

#[tokio::test]
async fn removing_the_sink_alone_does_not_resume_retention() {
    let _guard = TEST_SERIAL.lock().await;
    let _sink = install(Arc::new(RecordingSink::new(200)), 100);
    let (mut conn, _c) = make_conn().await;
    insert_audit_rows(&mut conn, 3).await;
    fire_due_audit_exports(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("tick");

    // The operator removes the sink from THIS process and stops there. That is
    // deliberately not enough: the guard keys on the shard's live cursor row,
    // not on process-local configuration, precisely so a worker outage cannot
    // let a web process delete the records that outage stranded. Retiring the
    // cursor is the second, explicit step -- see
    // `retention_resumes_only_after_the_cursor_is_decommissioned` and the
    // escalation section of `docs/runbooks/harvest-alerts.md`.
    uninstall();
    insert_audit_rows(&mut conn, 2).await;
    diesel::update(harvest_audit_log::table)
        .set(harvest_audit_log::occurred_at.eq(chrono::Utc::now() - chrono::Duration::days(365)))
        .execute(&mut conn)
        .await
        .expect("age rows");

    let deleted = purge_old_audit_records(&mut conn, 90)
        .await
        .expect("purge runs");
    assert_eq!(
        deleted, 3,
        "only the three the sink acknowledged may go. The two later records \
         are still unexported, and dropping the sink locally is not a \
         statement that nobody owes them -- an operator who wants the disk \
         back must retire the cursor explicitly"
    );

    let remaining: i64 = harvest_audit_log::table
        .count()
        .get_result(&mut conn)
        .await
        .expect("count");
    assert_eq!(remaining, 2);
}

// A retired shard's status must not keep reporting a growing backlog: no
// exporter owes those records, so an alert on `pending_records` there could
// never be cleared by any action.
#[tokio::test]
async fn a_retired_shard_reports_no_backlog() {
    let _guard = TEST_SERIAL.lock().await;
    let _sink = install(Arc::new(RecordingSink::new(200)), 100);
    let (mut conn, _c) = make_conn().await;
    insert_audit_rows(&mut conn, 3).await;
    fire_due_audit_exports(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("tick");
    uninstall();

    autumn_harvest::audit_export::decommission_cursor(&mut conn, 0)
        .await
        .expect("decommission");

    // Audited operations keep happening after the retirement.
    insert_audit_rows(&mut conn, 4).await;

    let status = autumn_harvest::audit_export::export_status(&mut conn, 0, chrono::Utc::now())
        .await
        .expect("status")
        .expect("a retired cursor is still reported");

    assert_eq!(status.delivery_state, "RETIRED");
    assert_eq!(
        status.pending_records, 0,
        "four unsequenced records exist, but no exporter owes them -- \
         reporting them as pending delivery would raise a backlog alert that \
         no action could clear"
    );
    assert!((status.lag_seconds - 0.0).abs() < f64::EPSILON);
    assert_eq!(
        status.last_assigned_seq, 3,
        "the high-water mark is still reported: it is why the row is kept"
    );
}

// Retiring a cursor must invalidate a delivery already in flight. `apply_outcome`
// is guarded on (shard, claim_epoch) alone, so without an epoch bump an attempt
// claimed before the retirement lands after it -- moving a cursor the status
// route now reports as a frozen RETIRED snapshot, and racing retention, which
// is free to purge the shard the moment it is retired.
#[tokio::test]
async fn retiring_a_cursor_invalidates_a_delivery_already_in_flight() {
    let _guard = TEST_SERIAL.lock().await;
    uninstall();
    let (mut conn, _c) = make_conn().await;
    insert_audit_rows(&mut conn, 3).await;
    ensure_cursor_row(&mut conn, 0).await.expect("cursor");

    // An exporter claims the shard and is mid-delivery.
    let claim = claim_shard(
        &mut conn,
        0,
        100,
        std::time::Duration::from_secs(60),
        chrono::Utc::now(),
    )
    .await
    .expect("claim")
    .expect("claimable");
    assert_eq!(claim.records.len(), 3);

    // The operator retires the cursor while that delivery is outstanding.
    autumn_harvest::audit_export::decommission_cursor(&mut conn, 0)
        .await
        .expect("decommission");

    // The in-flight attempt now reports success against its stale epoch.
    apply_outcome(
        &mut conn,
        0,
        claim.claim_epoch,
        &ExportOutcome::Advance {
            through_seq: 3,
            status: 200,
        },
        chrono::Utc::now(),
    )
    .await
    .expect("apply is not an error, it simply matches nothing");

    assert_eq!(
        cursor_acked(&mut conn, 0).await,
        0,
        "a delivery claimed before the retirement must not move the cursor \
         afterwards: the shard is reported as a frozen RETIRED snapshot and \
         retention may already have purged those records"
    );

    let status = autumn_harvest::audit_export::export_status(&mut conn, 0, chrono::Utc::now())
        .await
        .expect("status")
        .expect("row");
    assert_eq!(status.delivery_state, "RETIRED");
    assert_eq!(
        status.consecutive_failures, 0,
        "nor may it write failure or backoff state onto a retired row"
    );
}

// The other half of the epoch bump: a retirement that commits between
// `ensure_cursor_row` and the claim's locked read must stop the scanner taking
// a NEW claim, not merely invalidate the old one. Retention may purge the
// shard's records the moment it is retired.
#[tokio::test]
async fn a_retired_cursor_cannot_be_claimed() {
    let _guard = TEST_SERIAL.lock().await;
    uninstall();
    let (mut conn, _c) = make_conn().await;
    insert_audit_rows(&mut conn, 3).await;
    ensure_cursor_row(&mut conn, 0).await.expect("cursor");

    // Claimable while live.
    let claim = claim_shard(
        &mut conn,
        0,
        100,
        std::time::Duration::from_secs(60),
        chrono::Utc::now(),
    )
    .await
    .expect("claim")
    .expect("a live cursor is claimable");
    apply_outcome(
        &mut conn,
        0,
        claim.claim_epoch,
        &ExportOutcome::Advance {
            through_seq: 3,
            status: 200,
        },
        chrono::Utc::now(),
    )
    .await
    .expect("ack");

    autumn_harvest::audit_export::decommission_cursor(&mut conn, 0)
        .await
        .expect("decommission");
    insert_audit_rows(&mut conn, 2).await;

    // Two new records exist and the lease is clear, so only the retired check
    // stands between the scanner and another delivery.
    let claim = claim_shard(
        &mut conn,
        0,
        100,
        std::time::Duration::from_secs(60),
        chrono::Utc::now(),
    )
    .await
    .expect("claim query succeeds");
    assert!(
        claim.is_none(),
        "a retired cursor must not be claimable: delivering after retirement \
         moves a row reported as a frozen RETIRED snapshot, for records \
         retention is already free to purge"
    );

    // And nothing was stamped by the refused claim.
    let unsequenced: i64 = harvest_audit_log::table
        .filter(harvest_audit_log::export_seq.is_null())
        .count()
        .get_result(&mut conn)
        .await
        .expect("count");
    assert_eq!(unsequenced, 2, "a refused claim must assign no sequences");
}

// The sibling invariant -- a resident of `enforce_timeouts_once` that fails on
// every tick must not stop audit export, and an export failure must not stop
// timeout enforcement -- is guarded in
// `autumn_harvest::timeout::tests::audit_export_runs_before_the_first_fallible_resident`.
//
// It lives there as a source-order assertion rather than here as a DB test
// because the scenario cannot be seeded: `harvest_task_queue.workflow_exec_id`
// is a foreign key, so a task naming a nonexistent execution is rejected by the
// schema, and an unregistered payload codec decodes to the `undecodable_marker`
// instead of erroring (issue #608). No supported insert produces a
// deterministically-failing resident.

// ── Redrive edge cases ──────────────────────────────────────────────────────

#[tokio::test]
async fn redrive_before_an_instant_predating_every_record_re_exports_everything() {
    let _guard = TEST_SERIAL.lock().await;
    let sink = install(Arc::new(RecordingSink::new(200)), 100);
    let (mut conn, _c) = make_conn().await;
    insert_audit_rows(&mut conn, 3).await;
    fire_due_audit_exports(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("tick");

    // An operator who lost their whole SIEM over-rewinds deliberately.
    let outcome = rewind_cursor(
        &mut conn,
        0,
        RewindRequest::Before(chrono::Utc::now() - chrono::Duration::days(365)),
        chrono::Utc::now(),
    )
    .await
    .expect("rewind");
    assert_eq!(outcome, RewindOutcome::Rewound { from: 3, to: 0 });

    fire_due_audit_exports(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("re-export");
    uninstall();
    assert_eq!(sink.captured().pop().expect("batch").seqs, vec![1, 2, 3]);
}

#[tokio::test]
async fn redrive_before_a_future_instant_is_refused_rather_than_skipping_records() {
    let _guard = TEST_SERIAL.lock().await;
    let _sink = install(Arc::new(RecordingSink::new(200)), 100);
    let (mut conn, _c) = make_conn().await;
    insert_audit_rows(&mut conn, 3).await;
    fire_due_audit_exports(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("tick");
    uninstall();

    let outcome = rewind_cursor(
        &mut conn,
        0,
        RewindRequest::Before(chrono::Utc::now() + chrono::Duration::days(1)),
        chrono::Utc::now(),
    )
    .await
    .expect("rewind");
    assert_eq!(
        outcome,
        RewindOutcome::NoOp {
            cursor: 3,
            requested: 3
        },
        "no sequenced record at or after a future instant means there is \
         nothing to re-export from there; the cursor must stay put rather \
         than move in either direction"
    );
    assert_eq!(cursor_acked(&mut conn, 0).await, 3);
}

// ── The exported counter tracks acknowledgements, not attempts ──────────────

#[tokio::test]
async fn the_exported_counter_is_not_bumped_when_delivery_fails() {
    let _guard = TEST_SERIAL.lock().await;
    let _sink = install(Arc::new(RecordingSink::new(500)), 100);
    let (mut conn, _c) = make_conn().await;
    insert_audit_rows(&mut conn, 4).await;

    let metrics = RecordingMetrics::default();
    fire_due_audit_exports(&mut conn, &None, &[], &metrics)
        .await
        .expect("tick");
    uninstall();

    assert!(
        metrics.exported.lock().expect("exported").is_empty(),
        "harvest.audit.exported counts acknowledged records, so a failed \
         delivery must not increment it -- otherwise it is an attempt rate \
         wearing a delivery rate's name"
    );
    assert!(
        !metrics.lag.lock().expect("lag").is_empty(),
        "the lag gauge must still be emitted on a tick that delivered nothing"
    );
    assert_eq!(
        *metrics.observed.lock().expect("observed"),
        vec![(0_u16, true)],
        "a sink rejecting the batch is not the same as the exporter being \
         unable to observe the shard: the cursor and lag were read fine"
    );
}

#[tokio::test]
async fn an_idle_tick_never_calls_the_sink_but_still_emits_lag() {
    let _guard = TEST_SERIAL.lock().await;
    let sink = install(Arc::new(RecordingSink::new(200)), 100);
    let (mut conn, _c) = make_conn().await;
    // A sink is configured and there is nothing at all to export.
    let metrics = RecordingMetrics::default();
    let processed = fire_due_audit_exports(&mut conn, &None, &[], &metrics)
        .await
        .expect("idle tick");
    uninstall();

    assert_eq!(processed, 0);
    assert!(
        sink.captured().is_empty(),
        "an empty batch must never be POSTed; a signed empty body on every \
         scanner tick is pure noise at the receiver"
    );
    assert_eq!(
        *metrics.lag.lock().expect("lag"),
        vec![(0_u16, 0.0)],
        "the gauge is emitted with 0 rather than going stale"
    );
    assert_eq!(
        *metrics.observed.lock().expect("observed"),
        vec![(0_u16, true)],
        "an idle tick still observed the shard fine, so the availability \
         gauge reports true alongside the lag gauge"
    );
}

// Issue #1268: a shard the scanner cannot acquire a connection for must not
// leave `harvest.audit.export_lag` as the only signal. Before this fix, the
// sharded loop `continue`d past an unreachable shard without touching either
// gauge. The lag gauge then kept serving a stale, commonly caught-up value
// forever, and no alert could tell "healthy" apart from "unobservable".
#[tokio::test]
async fn a_shard_the_scanner_cannot_acquire_a_connection_for_is_marked_unobserved() {
    let _guard = TEST_SERIAL.lock().await;
    let _installed = install(Arc::new(RecordingSink::new(200)), 100);
    let (mut conn, container) = make_conn().await;

    // The sharded pool only maps shard 0; shard 7 is assigned but has no
    // pool, the shape `sp.exact_pool_for` reports for an unreachable shard.
    let pool = single_connection_pool(&container).await;
    let sharded = autumn_harvest::shard::ShardedDbPool::single(pool);

    let metrics = RecordingMetrics::default();
    let _ = fire_due_audit_exports(
        &mut conn,
        &Some(sharded),
        &[autumn_harvest::types::ShardId::new(7)],
        &metrics,
    )
    .await;
    uninstall();

    assert_eq!(
        *metrics.observed.lock().expect("observed"),
        vec![(7_u16, false)],
        "an unreachable shard must be reported unobserved, loudly, rather \
         than silently skipped"
    );
    assert!(
        metrics.lag.lock().expect("lag").is_empty(),
        "the lag gauge must not be given a fabricated reading for a shard \
         that was never actually queried"
    );
}

// The sibling of the test above. That one covers a shard with no pool at
// all. This one covers a shard that IS mapped to a pool, but whose database
// has gone away. Both must be marked unobserved, through the two different
// arms of the connection-acquire match. Stopping the container gives an
// immediate connection refusal, so this exercises the `Ok(Err(e))` arm
// without waiting out `SHARD_ACQUIRE_BOUND`'s five-second timeout.
#[tokio::test]
async fn a_shard_whose_database_is_unreachable_is_marked_unobserved() {
    let _guard = TEST_SERIAL.lock().await;
    let _installed = install(Arc::new(RecordingSink::new(200)), 100);
    let (mut conn, container) = make_conn().await;

    let pool = single_connection_pool(&container).await;
    container
        .stop_with_timeout(Some(0))
        .await
        .expect("stop container");

    let sharded = autumn_harvest::shard::ShardedDbPool::single(pool);
    let metrics = RecordingMetrics::default();
    let _ = fire_due_audit_exports(
        &mut conn,
        &Some(sharded),
        &[autumn_harvest::types::ShardId::new(0)],
        &metrics,
    )
    .await;
    uninstall();

    assert_eq!(
        *metrics.observed.lock().expect("observed"),
        vec![(0_u16, false)],
        "a connection error acquiring a mapped shard's pool must be \
         reported unobserved, exactly like a shard with no pool at all"
    );
    assert!(
        metrics.lag.lock().expect("lag").is_empty(),
        "the lag gauge must not be given a fabricated reading for a shard \
         the exporter could never connect to"
    );
}

// Codex review on PR #1505: the two tests above cover a failure INSIDE
// `fire_due_audit_exports`'s own per-shard loop. But `enforce_timeouts_once`
// -- and therefore `fire_due_audit_exports` -- is only reached after the
// timeout checker's OWN, earlier connection acquisition succeeds. If a
// shard's database is unreachable even for that first checkout, the inner
// mechanism never runs at all this tick.
// `spawn_timeout_checker_for_shard` must mark the shard unobserved itself.
#[tokio::test]
async fn a_checkers_own_connection_failure_marks_its_shard_unobserved() {
    let _guard = TEST_SERIAL.lock().await;
    let _installed = install(Arc::new(RecordingSink::new(200)), 100);

    let (_conn, container) = make_conn().await;
    let pool = single_connection_pool(&container).await;
    container
        .stop_with_timeout(Some(0))
        .await
        .expect("stop container");

    let metrics = Arc::new(RecordingMetrics::default());
    let telemetry = Arc::new(autumn_harvest::telemetry::TelemetryConfig {
        metrics: metrics.clone(),
        ..Default::default()
    });
    let cancel = tokio_util::sync::CancellationToken::new();

    let handle = autumn_harvest::timeout::spawn_timeout_checker_for_shard(
        pool,
        cancel.clone(),
        std::time::Duration::from_millis(50),
        telemetry,
        std::time::Duration::from_secs(5),
        None,
        vec![autumn_harvest::types::ShardId::new(0)],
        Arc::new(autumn_harvest::circuit_breaker::CircuitBreakerRegistry::default()),
        None,
        60,
        Some(autumn_harvest::types::ShardId::new(0)),
        autumn_harvest::payload_codec::PayloadCodecs::default(),
        0,
    );

    // Poll (bounded) rather than sleeping a fixed span: fast when the fix
    // works, a clear timeout rather than a flake when it does not.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if metrics
            .observed
            .lock()
            .expect("observed")
            .contains(&(0_u16, false))
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the checker must mark its own shard unobserved when it cannot even \
             acquire its own connection; got {:?}",
            metrics.observed.lock().expect("observed")
        );
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert!(
        metrics.lag.lock().expect("lag").is_empty(),
        "the lag gauge must not be given a fabricated reading for a shard \
         whose checker could never even acquire a connection"
    );

    cancel.cancel();
    let _ = handle.await;
    uninstall();
}

// Issue #1268: the legacy `spawn_timeout_checker` entry point passes
// `shard: None` for a process-wide loop. That loop can still cover a real,
// non-default `shard_assignments` (e.g. `[7]`) when driving a sharded pool.
// The fix above must label the shards `shard_assignments` actually names.
// It must never fall back to the pool's own default shard. Otherwise a
// connection failure here would mark the WRONG shard (0) unobserved, while
// the actually-affected shard (7) stays frozen.
#[tokio::test]
async fn a_process_wide_checkers_connection_failure_marks_its_real_shards_unobserved() {
    let _guard = TEST_SERIAL.lock().await;
    let _installed = install(Arc::new(RecordingSink::new(200)), 100);

    let (_conn, container) = make_conn().await;
    let pool = single_connection_pool(&container).await;
    // `ShardedDbPool::single` always defaults to shard 0. That is exactly
    // the wrong-shard label a naive fix would fall back to, so the
    // assignment below names a different shard on purpose. Built before
    // the container stops: `get_host_port_ipv4` needs the live mapping.
    let sharded =
        autumn_harvest::shard::ShardedDbPool::single(single_connection_pool(&container).await);
    container
        .stop_with_timeout(Some(0))
        .await
        .expect("stop container");

    let metrics = Arc::new(RecordingMetrics::default());
    let telemetry = Arc::new(autumn_harvest::telemetry::TelemetryConfig {
        metrics: metrics.clone(),
        ..Default::default()
    });
    let cancel = tokio_util::sync::CancellationToken::new();

    let handle = autumn_harvest::timeout::spawn_timeout_checker_for_shard(
        pool,
        cancel.clone(),
        std::time::Duration::from_millis(50),
        telemetry,
        std::time::Duration::from_secs(5),
        Some(sharded),
        vec![autumn_harvest::types::ShardId::new(7)],
        Arc::new(autumn_harvest::circuit_breaker::CircuitBreakerRegistry::default()),
        None,
        60,
        None, // the legacy, process-wide entry point's own shard label
        autumn_harvest::payload_codec::PayloadCodecs::default(),
        0,
    );

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if metrics
            .observed
            .lock()
            .expect("observed")
            .contains(&(7_u16, false))
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the checker must mark shard 7 -- the shard `shard_assignments` \
             actually names -- unobserved, never the pool's own default \
             shard 0; got {:?}",
            metrics.observed.lock().expect("observed")
        );
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert!(
        !metrics
            .observed
            .lock()
            .expect("observed")
            .contains(&(0_u16, false)),
        "shard 0 was never assigned to this loop and must not be reported \
         at all; got {:?}",
        metrics.observed.lock().expect("observed")
    );

    cancel.cancel();
    let _ = handle.await;
    uninstall();
}

// A mismatched (conn, shard_assignments) pair must never stamp rows in the
// connection's own database under the assigned shard's key.
//
// `enforce_timeouts_once` is a `pub` primitive an embedder may drive by hand,
// and nothing inside it can verify which database `conn` points at. An earlier
// revision inferred from `shard_assignments.len() == 1` that `conn` belonged
// to that shard and exported through it directly; this pins the reason that
// inference is gone. Getting it wrong is silent cross-shard corruption of the
// `(shard, seq)` identity the whole feature rests on, whereas acquiring the
// exact pool can at worst skip a shard, loudly.
#[tokio::test]
async fn a_mismatched_connection_never_stamps_another_shards_rows() {
    let _guard = TEST_SERIAL.lock().await;
    let _installed = install(Arc::new(RecordingSink::new(200)), 100);
    let (mut conn, container) = make_conn().await;
    insert_audit_rows(&mut conn, 3).await;

    // The sharded pool maps shard 0 to this same database, but we claim the
    // assignment is shard 7 -- the shape a hand-driving embedder can produce.
    let pool = single_connection_pool(&container).await;
    let sharded = autumn_harvest::shard::ShardedDbPool::single(pool);

    let _ = fire_due_audit_exports(
        &mut conn,
        &Some(sharded),
        &[autumn_harvest::types::ShardId::new(7)],
        &NoOpMetrics,
    )
    .await;
    uninstall();

    // Whatever happened, nothing in THIS database may carry shard 7's key.
    let stamped: i64 = harvest_audit_log::table
        .filter(harvest_audit_log::export_seq.is_not_null())
        .count()
        .get_result(&mut conn)
        .await
        .expect("count");
    assert_eq!(
        stamped, 0,
        "rows in this database must not be sequenced on behalf of shard 7: \
         the exporter cannot know that `conn` points here, so it must acquire \
         shard 7's own pool rather than assume"
    );

    use autumn_harvest::schema::harvest_audit_export_cursor::dsl as cur;
    let cursors: i64 = cur::harvest_audit_export_cursor
        .filter(cur::shard_id.eq(7))
        .count()
        .get_result(&mut conn)
        .await
        .expect("count");
    assert_eq!(
        cursors, 0,
        "nor may a cursor for shard 7 be provisioned in this database"
    );
}

// ── Retention must not depend on the local process's sink registration ─────
//
// Split web/worker deployment: only the worker configures the sink, and the
// web app runs the retention sweep. The sweeping process therefore answers
// "is export configured?" with `false`, so the guard has to come from durable,
// shared state — the exporter's heartbeat on the cursor row.
#[tokio::test]
async fn retention_respects_a_live_exporter_heartbeat_from_another_process() {
    let _guard = TEST_SERIAL.lock().await;
    let _sink = install(Arc::new(RecordingSink::new(200)), 2);
    let (mut conn, _c) = make_conn().await;
    insert_audit_rows(&mut conn, 5).await;

    // The "worker process" exports two records and heartbeats the cursor.
    fire_due_audit_exports(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("tick");
    assert_eq!(cursor_acked(&mut conn, 0).await, 2);
    diesel::update(harvest_audit_log::table)
        .set(harvest_audit_log::occurred_at.eq(chrono::Utc::now() - chrono::Duration::days(365)))
        .execute(&mut conn)
        .await
        .expect("age rows");

    // Now the "retention process": no sink registered here at all.
    uninstall();
    assert!(
        !autumn_harvest::audit_export::is_configured(),
        "this stands in for the process that runs retention"
    );

    let deleted = purge_old_audit_records(&mut conn, 90)
        .await
        .expect("purge runs");
    assert_eq!(
        deleted, 2,
        "the live heartbeat must protect the three unexported records even \
         though THIS process has no sink configured; only the acknowledged \
         two may be purged"
    );
}

// The other half of the same knob. Retiring export is an EXPLICIT operator
// action, never an inferred one: an earlier revision expired the guard 24h
// after the last heartbeat, which meant a worker outage longer than a day
// deleted the very records the outage had stranded.
#[tokio::test]
async fn retention_resumes_only_after_the_cursor_is_decommissioned() {
    let _guard = TEST_SERIAL.lock().await;
    let _sink = install(Arc::new(RecordingSink::new(200)), 2);
    let (mut conn, _c) = make_conn().await;
    insert_audit_rows(&mut conn, 5).await;
    fire_due_audit_exports(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("tick");
    uninstall();

    diesel::update(harvest_audit_log::table)
        .set(harvest_audit_log::occurred_at.eq(chrono::Utc::now() - chrono::Duration::days(365)))
        .execute(&mut conn)
        .await
        .expect("age rows");

    // A very old heartbeat is NOT consent to delete: the exporter may simply
    // have been down. The three unexported records must survive.
    {
        use autumn_harvest::schema::harvest_audit_export_cursor::dsl as cur;
        diesel::update(cur::harvest_audit_export_cursor.find(0))
            .set(cur::updated_at.eq(chrono::Utc::now() - chrono::Duration::days(400)))
            .execute(&mut conn)
            .await
            .expect("age heartbeat");
    }
    let deleted = purge_old_audit_records(&mut conn, 90)
        .await
        .expect("purge runs");
    assert_eq!(
        deleted, 2,
        "a long exporter outage must never license deleting the records it \
         stranded; only the two acknowledged rows may go"
    );

    // Only an explicit decommission lifts the guard.
    assert!(
        autumn_harvest::audit_export::decommission_cursor(&mut conn, 0)
            .await
            .expect("decommission"),
        "the cursor row existed, so it must report as removed"
    );
    let deleted = purge_old_audit_records(&mut conn, 90)
        .await
        .expect("purge runs");
    assert_eq!(
        deleted, 3,
        "once an operator retires the cursor, retention resumes over the \
         remaining aged rows"
    );
}

#[tokio::test]
async fn every_tick_refreshes_the_exporter_heartbeat() {
    let _guard = TEST_SERIAL.lock().await;
    let _sink = install(Arc::new(RecordingSink::new(200)), 100);
    let (mut conn, _c) = make_conn().await;
    // No audit rows at all: an idle tick must still heartbeat, or an idle but
    // perfectly healthy exporter would look dead to the retention sweep.
    fire_due_audit_exports(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("idle tick");

    use autumn_harvest::schema::harvest_audit_export_cursor::dsl as cur;
    let stale = chrono::Utc::now() - chrono::Duration::days(3);
    diesel::update(cur::harvest_audit_export_cursor.find(0))
        .set(cur::updated_at.eq(stale))
        .execute(&mut conn)
        .await
        .expect("age heartbeat");

    fire_due_audit_exports(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("second idle tick");
    uninstall();

    let refreshed: chrono::DateTime<chrono::Utc> = cur::harvest_audit_export_cursor
        .find(0)
        .select(cur::updated_at)
        .first(&mut conn)
        .await
        .expect("cursor row");
    assert!(
        refreshed > stale,
        "a tick that claims nothing must still refresh the heartbeat: an idle \
         exporter is alive, and `GET /admin/audit-export` reports this as \
         liveness. Retention no longer reads it -- see \
         `retention_resumes_only_after_the_cursor_is_decommissioned`"
    );
}

// Decommissioning must be reversible without corrupting the sequence. If a
// recreated cursor restarted at 0, the next records would take `(shard, seq)`
// pairs that already name DIFFERENT records -- and a receiver deduping on that
// pair, exactly as this feature instructs, would discard the new ones.
#[tokio::test]
async fn a_recreated_cursor_continues_the_sequence_it_left_off_at() {
    let _guard = TEST_SERIAL.lock().await;
    let sink = Arc::new(RecordingSink::new(200));
    let _installed = install(sink.clone(), 100);
    let (mut conn, _c) = make_conn().await;
    insert_audit_rows(&mut conn, 4).await;
    fire_due_audit_exports(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("tick");
    assert_eq!(sink.all_seqs(), vec![1, 2, 3, 4]);
    uninstall();

    // The operator retires export; the four stamped rows stay in the table.
    autumn_harvest::audit_export::decommission_cursor(&mut conn, 0)
        .await
        .expect("decommission");

    // Later, export is re-enabled and new audited operations arrive.
    let sink2 = Arc::new(RecordingSink::new(200));
    let _reinstalled = install(sink2.clone(), 100);
    insert_audit_rows(&mut conn, 2).await;
    fire_due_audit_exports(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("tick after re-enable");
    uninstall();

    let seqs = sink2.all_seqs();
    let fresh: Vec<i64> = seqs.iter().copied().filter(|s| *s > 4).collect();
    assert_eq!(
        fresh,
        vec![5, 6],
        "the two new records must continue from 5, not restart at 1 and \
         collide with the sequences already assigned to different records; \
         got {seqs:?}"
    );

    // Every stamped row still has a distinct sequence -- the invariant a
    // restarted counter would break.
    let stamped: Vec<Option<i64>> = harvest_audit_log::table
        .filter(harvest_audit_log::export_seq.is_not_null())
        .select(harvest_audit_log::export_seq)
        .order(harvest_audit_log::export_seq.asc())
        .load(&mut conn)
        .await
        .expect("load sequences");
    let stamped: Vec<i64> = stamped.into_iter().flatten().collect();
    assert_eq!(stamped, vec![1, 2, 3, 4, 5, 6]);
}

// The high-water mark must survive retention deleting every stamped row.
// Decommissioning is what PERMITS that purge, so a counter derived from the
// surviving rows is a counter that decommissioning can destroy.
#[tokio::test]
async fn the_sequence_survives_a_decommission_that_purges_every_stamped_row() {
    let _guard = TEST_SERIAL.lock().await;
    let sink = Arc::new(RecordingSink::new(200));
    let _installed = install(sink.clone(), 100);
    let (mut conn, _c) = make_conn().await;
    insert_audit_rows(&mut conn, 3).await;
    fire_due_audit_exports(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("tick");
    assert_eq!(sink.all_seqs(), vec![1, 2, 3]);
    uninstall();

    // Operator retires export to relieve disk pressure, exactly as the runbook
    // describes, and retention then purges the whole aged window.
    autumn_harvest::audit_export::decommission_cursor(&mut conn, 0)
        .await
        .expect("decommission");
    diesel::update(harvest_audit_log::table)
        .set(harvest_audit_log::occurred_at.eq(chrono::Utc::now() - chrono::Duration::days(365)))
        .execute(&mut conn)
        .await
        .expect("age rows");
    let deleted = purge_old_audit_records(&mut conn, 90).await.expect("purge");
    assert_eq!(deleted, 3, "retiring the cursor must permit the purge");

    let remaining: i64 = harvest_audit_log::table
        .count()
        .get_result(&mut conn)
        .await
        .expect("count");
    assert_eq!(
        remaining, 0,
        "no stamped row survives to derive a counter from"
    );

    // Export is re-enabled later. The SIEM still holds records at seq 1-3.
    let sink2 = Arc::new(RecordingSink::new(200));
    let _reinstalled = install(sink2.clone(), 100);
    insert_audit_rows(&mut conn, 2).await;
    fire_due_audit_exports(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("tick after re-enable");
    uninstall();

    assert_eq!(
        sink2.all_seqs(),
        vec![4, 5],
        "the new records must continue from 4. Restarting at 1 would re-issue \
         sequences the SIEM already holds against different records, and a \
         receiver deduping on (shard, seq) -- as this feature instructs -- \
         would silently discard these genuine new audit events"
    );
}

// Retiring the cursor keeps the row (for its sequence high-water mark), so the
// redrive must check `retired_at` explicitly. Otherwise it answers 200
// "rewound" for a shard whose records retention is free to purge and where no
// exporter is running -- a promise the system cannot keep.
#[tokio::test]
async fn a_retired_shard_cannot_be_redriven() {
    let _guard = TEST_SERIAL.lock().await;
    let _installed = install(Arc::new(RecordingSink::new(200)), 100);
    let (mut conn, _c) = make_conn().await;
    insert_audit_rows(&mut conn, 3).await;
    fire_due_audit_exports(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("tick");
    uninstall();

    // While live, a rewind is honoured.
    assert_eq!(
        autumn_harvest::audit_export::rewind_cursor_locked(
            &mut conn,
            0,
            RewindRequest::Seq(1),
            chrono::Utc::now(),
        )
        .await
        .expect("rewind"),
        RewindOutcome::Rewound { from: 3, to: 1 },
    );

    autumn_harvest::audit_export::decommission_cursor(&mut conn, 0)
        .await
        .expect("decommission");

    // Retired: refused, exactly as an unconfigured shard was before the row
    // began to outlive a decommission.
    assert_eq!(
        autumn_harvest::audit_export::rewind_cursor_locked(
            &mut conn,
            0,
            RewindRequest::Seq(0),
            chrono::Utc::now(),
        )
        .await
        .expect("rewind"),
        RewindOutcome::NotConfigured,
        "a retired shard must not report a successful rewind: retention may \
         purge the records it names and no exporter will ship them"
    );

    // And the refusal did not move the cursor.
    assert_eq!(cursor_acked(&mut conn, 0).await, 1);
}

// ── The redrive's audit row commits with the rewind, on one connection ─────

#[tokio::test]
async fn a_redrive_and_its_audit_record_land_together_on_the_target_shard() {
    let _guard = TEST_SERIAL.lock().await;
    let _sink = install(Arc::new(RecordingSink::new(200)), 100);
    let (mut conn, _c) = make_conn().await;
    insert_audit_rows(&mut conn, 4).await;
    fire_due_audit_exports(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("tick");
    uninstall();

    // The management handler runs the rewind and the audit insert in ONE
    // transaction on the target shard's connection. Reproduce that shape here
    // (the handler itself is exercised by the auth-boundary and contract
    // suites) and assert the pairing.
    use diesel_async::AsyncConnection as _;
    let outcome = Box::pin(
        conn.transaction::<RewindOutcome, autumn_harvest::error::HarvestError, _>(async |conn| {
            let outcome = autumn_harvest::audit_export::rewind_cursor_locked(
                conn,
                0,
                RewindRequest::Seq(1),
                chrono::Utc::now(),
            )
            .await?;
            let record = NewAuditRecord {
                actor: "alice",
                operation: autumn_harvest::audit::OP_AUDIT_EXPORT_REDRIVE,
                target_type: autumn_harvest::audit::TARGET_AUDIT_EXPORT,
                target_id: Some("shard=0;to_seq=1"),
                route_or_command: "POST /admin/audit-export/redrive",
                request_id: None,
                idempotency_key: None,
                status: STATUS_SUCCEEDED,
                error_summary: Some("cursor rewound from 4 to 1"),
                shard_id: Some(0),
                source: "api",
            };
            audit::insert_audit(conn, &record).await?;
            Ok(outcome)
        }),
    )
    .await
    .expect("transaction commits");

    assert_eq!(outcome, RewindOutcome::Rewound { from: 4, to: 1 });
    assert_eq!(cursor_acked(&mut conn, 0).await, 1);

    // The audit row landed on the SAME shard as the cursor it describes, so it
    // is picked up by that shard's own exporter like any other audit record.
    let redrive_rows: i64 = harvest_audit_log::table
        .filter(harvest_audit_log::operation.eq(autumn_harvest::audit::OP_AUDIT_EXPORT_REDRIVE))
        .count()
        .get_result(&mut conn)
        .await
        .expect("count");
    assert_eq!(redrive_rows, 1);
}

// A rewind whose audit insert fails must leave the cursor untouched: an
// applied-but-unaudited privileged mutation is the thing the single
// transaction exists to make unrepresentable.
#[tokio::test]
async fn a_failed_audit_write_rolls_the_rewind_back() {
    let _guard = TEST_SERIAL.lock().await;
    uninstall();
    let (mut conn, _c) = make_conn().await;
    insert_audit_rows(&mut conn, 3).await;
    ensure_cursor_row(&mut conn, 0).await.expect("cursor");
    let claim = claim_shard(
        &mut conn,
        0,
        100,
        std::time::Duration::from_secs(60),
        chrono::Utc::now(),
    )
    .await
    .expect("claim")
    .expect("claimable");
    apply_outcome(
        &mut conn,
        0,
        claim.claim_epoch,
        &ExportOutcome::Advance {
            through_seq: 3,
            status: 200,
        },
        chrono::Utc::now(),
    )
    .await
    .expect("ack");
    assert_eq!(cursor_acked(&mut conn, 0).await, 3);

    // Rewind, then fail the audit insert inside the same transaction. A NULL
    // `operation` violates the table's NOT NULL, standing in for any reason
    // the audit write could fail.
    use diesel_async::AsyncConnection as _;
    let result = Box::pin(
        conn.transaction::<(), autumn_harvest::error::HarvestError, _>(async |conn| {
            autumn_harvest::audit_export::rewind_cursor_locked(
                conn,
                0,
                RewindRequest::Seq(0),
                chrono::Utc::now(),
            )
            .await?;
            diesel::sql_query(
                "INSERT INTO harvest_audit_log (actor, operation, target_type, \
                 route_or_command, status, source) \
                 VALUES ('alice', NULL, 'audit_export', 'POST /x', 'succeeded', 'api')",
            )
            .execute(conn)
            .await
            .map_err(autumn_harvest::error::database_error)?;
            Ok(())
        }),
    )
    .await;

    assert!(
        result.is_err(),
        "the audit insert must fail this transaction"
    );
    assert_eq!(
        cursor_acked(&mut conn, 0).await,
        3,
        "the rewind must roll back with its audit write: a cursor moved with \
         no trail is exactly what the single transaction prevents"
    );
}

// ── The issue's success metric, at CI-affordable scale ─────────────────────
//
// The issue asks for receiver-side `(shard, seq)` accounting over >= 100k
// records with a forced restart mid-stream and a 10-minute sink outage,
// proving 0 lost records and 0 gaps. That volume and those wall-clock
// durations are not a CI test; this encodes the same *shape* at 5,000 records,
// with the restart and the outage injected deterministically rather than
// waited out. What it proves is exactly what the metric asks: every sequence
// arrives at least once, the set is contiguous with no hole, and the cursor
// never moved past a record the sink had not acknowledged.
//
// Run the full-scale version against a real sink before claiming the metric;
// this is the regression guard that keeps the mechanism honest between runs.
#[tokio::test]
async fn receiver_side_accounting_survives_a_restart_and_a_sink_outage() {
    const RECORDS: i64 = 5_000;
    const BATCH: i64 = 500;

    let _guard = TEST_SERIAL.lock().await;
    // A short lease so the injected "crash" is reclaimable on the next tick
    // without waiting out a real 60s lease.
    let sink = install_with_lease(
        Arc::new(RecordingSink::new(200)),
        BATCH,
        std::time::Duration::from_secs(0),
    );
    let (mut conn, _c) = make_conn().await;

    // Bulk-insert; every column but the defaults is uniform, so the shape of
    // the record is irrelevant to what this test measures.
    diesel::sql_query(
        "INSERT INTO harvest_audit_log \
             (actor, operation, target_type, target_id, route_or_command, status, source) \
         SELECT 'alice', 'workflow.cancel', 'workflow', 'exec-' || g, \
                'POST /workflows/{id}/cancel', 'succeeded', 'api' \
         FROM generate_series(1, $1) AS g",
    )
    .bind::<diesel::sql_types::BigInt, _>(RECORDS)
    .execute(&mut conn)
    .await
    .expect("bulk insert");

    // ── Phase 1: export a few batches normally ──────────────────────────────
    for _ in 0..3 {
        fire_due_audit_exports(&mut conn, &None, &[], &NoOpMetrics)
            .await
            .expect("tick");
    }
    let after_normal = cursor_acked(&mut conn, 0).await;
    assert!(after_normal > 0, "some progress before the failures");

    // ── Phase 2: a process death between the POST and the cursor write ──────
    // Claim and deliver by hand, then never acknowledge.
    ensure_cursor_row(&mut conn, 0).await.expect("cursor");
    let claim = claim_shard(
        &mut conn,
        0,
        BATCH,
        std::time::Duration::from_secs(0),
        chrono::Utc::now(),
    )
    .await
    .expect("claim")
    .expect("claimable");
    let body = serialize_batch(&claim.records).expect("serialize");
    let headers = autumn_harvest::audit_export::export_headers(
        &CallbackSecret::new(b"test-secret".to_vec()),
        &body,
        0,
        claim.records[0].seq,
        claim.records[claim.records.len() - 1].seq,
        chrono::Utc::now(),
    );
    let crashed_batch = AuditBatch {
        shard: 0,
        first_seq: claim.records[0].seq,
        last_seq: claim.records[claim.records.len() - 1].seq,
        records: &claim.records,
        body: &body,
        headers: &headers,
    };
    assert!(
        sink.deliver(&crashed_batch).await.is_success(),
        "the sink DID accept the pre-crash batch"
    );
    assert_eq!(
        cursor_acked(&mut conn, 0).await,
        after_normal,
        "the cursor must not have moved for a batch that was never acknowledged"
    );

    // ── Phase 3: a sink outage ──────────────────────────────────────────────
    // Ten minutes of 503s, compressed: three failing ticks with the backoff
    // deadline cleared between them, which is what a long outage looks like to
    // the cursor.
    sink.set_status(503);
    for _ in 0..3 {
        fire_due_audit_exports(&mut conn, &None, &[], &NoOpMetrics)
            .await
            .expect("tick during outage");
        clear_backoff(&mut conn).await;
    }
    let after_outage = cursor_acked(&mut conn, 0).await;
    assert_eq!(
        after_outage, after_normal,
        "an outage must not advance the cursor by a single sequence"
    );

    // ── Phase 4: the sink returns; drain to completion ──────────────────────
    sink.set_status(200);
    let mut ticks = 0;
    loop {
        clear_backoff(&mut conn).await;
        let n = fire_due_audit_exports(&mut conn, &None, &[], &NoOpMetrics)
            .await
            .expect("recovery tick");
        ticks += 1;
        if cursor_acked(&mut conn, 0).await >= RECORDS {
            break;
        }
        assert!(
            ticks < 200,
            "the exporter must drain the backlog; stalled after {ticks} ticks \
             with {n} delivered on the last one"
        );
    }
    uninstall();

    // ── Receiver-side accounting ────────────────────────────────────────────
    let delivered = sink.all_seqs();
    let seen: std::collections::BTreeSet<i64> = delivered.iter().copied().collect();

    // 0 lost records: every sequence arrived at least once.
    assert_eq!(
        i64::try_from(seen.len()).expect("count fits"),
        RECORDS,
        "every record must reach the sink at least once"
    );
    // 0 gaps: the set is contiguous from 1, so a receiver checking contiguity
    // sees no hole (which is stronger than mere gap detection).
    assert_eq!(*seen.iter().next().expect("first"), 1);
    assert_eq!(*seen.iter().next_back().expect("last"), RECORDS);
    for (expected, seq) in (1_i64..).zip(seen.iter()) {
        assert_eq!(*seq, expected, "sequence {expected} is missing");
    }
    // The duplicates are the at-least-once contract, not a defect: the
    // pre-crash batch and the outage's re-attempts are re-sent, and the
    // receiver dedupes on `(shard, seq)`.
    assert!(
        delivered.len() > seen.len(),
        "this scenario must actually exercise redelivery, or it is not testing \
         at-least-once at all"
    );
    // The cursor never skipped: every sequence at or below it was delivered.
    assert_eq!(cursor_acked(&mut conn, 0).await, RECORDS);
    // And nothing is left pending.
    let (pending, _) =
        autumn_harvest::audit_export::pending_and_lag(&mut conn, RECORDS, chrono::Utc::now())
            .await
            .expect("pending");
    assert_eq!(pending, 0);
}

/// Clear a shard's backoff deadline. Stands in for waiting out the capped
/// exponential delay, which is wall-clock time a test must not spend.
async fn clear_backoff(conn: &mut diesel_async::AsyncPgConnection) {
    use autumn_harvest::schema::harvest_audit_export_cursor::dsl as cur;
    diesel::update(cur::harvest_audit_export_cursor.find(0))
        .set(cur::next_attempt_at.eq(chrono::Utc::now()))
        .execute(conn)
        .await
        .expect("clear backoff");
}

/// Compile-time proof that an embedder can supply a sink with no HTTP client
/// anywhere in sight (AC1: the trait lives in core with no `reqwest`
/// dependency).
#[test]
fn an_embedder_sink_needs_no_http_client() {
    struct FileSink;
    impl AuditSink for FileSink {
        fn deliver<'a>(&'a self, batch: &'a AuditBatch<'a>) -> SinkFuture<'a> {
            // A real implementation would append `batch.body` to a file, hand
            // `batch.records` to an OTLP-logs exporter, or PutRecords to
            // Kinesis. Nothing here needs HTTP.
            let count = batch.records.len();
            Box::pin(async move {
                if count == 0 {
                    SinkAttempt::transport_error("empty".to_string())
                } else {
                    SinkAttempt::success(200)
                }
            })
        }
    }
    let _: Arc<dyn AuditSink> = Arc::new(FileSink);
    // And the record type is plain data an embedder can map freely.
    fn assert_serde<T: serde::Serialize + serde::de::DeserializeOwned>() {}
    assert_serde::<AuditExportRecord>();
}
