#![cfg(feature = "db")]
//! Workflow-task timeout quarantine integration tests — issue #494.
//!
//! The worker wraps each workflow-task dispatch in a bounded wall-clock budget
//! (`WorkerConfig::workflow_task_timeout`, default 10 s). A hung body that
//! exceeds the budget has its concurrency slot reclaimed and its execution's
//! consecutive-timeout counter incremented. At or above
//! `poison_pill_threshold` (default 3) the task is quarantined to the DLQ and
//! the owning execution is failed terminally.
//!
//! These tests exercise the DB-layer helpers directly against a real Postgres
//! container:
//!
//! - `reset_timed_out_workflow_task` reverts a RUNNING task to PENDING so it
//!   can be re-claimed without waiting for the orphan-reclaim staleness window.
//! - `quarantine_workflow_task_timeout` moves the task to the DLQ, fails the
//!   execution, and emits `harvest.workflow.task_timeout` + terminal metrics.
//! - `WorkerConfig::workflow_task_timeout` defaults to 10 s; zero disables.

use std::sync::Mutex;

use autumn_harvest::dlq::{DeadLetterReason, dead_letter_count};
use autumn_harvest::telemetry::MetricsRecorder;
use autumn_harvest::worker::{
    DbPool, quarantine_workflow_task_timeout, reset_timed_out_workflow_task,
};
use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

/// Keepalive handle for either a testcontainers container or a local
/// per-test database. Dropping this stops the container (testcontainers path).
/// For the local-PG path we keep `()` — each test uses a unique UUID-named
/// database so leftover databases are harmless.
enum Keepalive {
    #[allow(dead_code)]
    Container(Box<ContainerAsync<Postgres>>),
    /// No cleanup needed; the test database is abandoned (unique UUID name).
    LocalDb,
}

// ---------------------------------------------------------------------------
// Diesel helper types
// ---------------------------------------------------------------------------

#[derive(QueryableByName)]
struct ReasonRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    error: String,
}

// ---------------------------------------------------------------------------
// Recording metrics stub
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct RecordingMetrics {
    task_timeouts: Mutex<Vec<(String, String)>>,
    terminal: Mutex<Vec<(String, String, String)>>,
}

impl MetricsRecorder for RecordingMetrics {
    fn record_workflow_task_timeout(&self, workflow_name: &str, queue: &str) {
        self.task_timeouts
            .lock()
            .unwrap()
            .push((workflow_name.to_owned(), queue.to_owned()));
    }

    fn record_workflow_terminal(
        &self,
        workflow_name: &str,
        queue: &str,
        status: autumn_harvest::telemetry::WorkflowStatus,
    ) {
        self.terminal.lock().unwrap().push((
            workflow_name.to_owned(),
            queue.to_owned(),
            format!("{status:?}"),
        ));
    }
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

async fn setup() -> (AsyncPgConnection, DbPool, Keepalive) {
    // When `TEST_DATABASE_URL` is set (e.g. in environments where Docker image
    // pulls are blocked), create a fresh per-test database on the local
    // Postgres instance instead of spinning up a container.
    if let Ok(admin_url) = std::env::var("TEST_DATABASE_URL") {
        let db_name = format!("harvest_test_{}", Uuid::new_v4().simple());
        // Create the fresh test database.
        let mut admin_conn = AsyncPgConnection::establish(&admin_url)
            .await
            .expect("connect to admin DB");
        admin_conn
            .batch_execute(&format!("CREATE DATABASE \"{db_name}\";"))
            .await
            .expect("create test database");

        // Build a URL pointing at the new database.
        // Strip trailing "/postgres" and replace with the new db name.
        let test_url = {
            let base = admin_url.trim_end_matches('/');
            let without_db = base.rfind('/').map_or(base, |i| &base[..i]);
            format!("{without_db}/{db_name}")
        };

        let mut conn = AsyncPgConnection::establish(&test_url)
            .await
            .expect("connect to test DB");
        conn.batch_execute(&autumn_harvest::test_init_sql())
            .await
            .expect("migration");

        let mgr = AsyncDieselConnectionManager::<AsyncPgConnection>::new(test_url);
        let pool = deadpool::managed::Pool::builder(mgr)
            .max_size(4)
            .build()
            .expect("pool build");

        return (conn, pool, Keepalive::LocalDb);
    }

    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres start");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgresql://postgres:postgres@{host}:{port}/postgres");
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("migration");

    let mgr = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    let pool = deadpool::managed::Pool::builder(mgr)
        .max_size(4)
        .build()
        .expect("pool build");

    (conn, pool, Keepalive::Container(Box::new(container)))
}

/// Insert a RUNNING workflow execution and return its UUID.
async fn insert_running_workflow(conn: &mut AsyncPgConnection) -> Uuid {
    let id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
         (id, workflow_name, workflow_id, shard_id, state, input, queue_name) \
         VALUES ($1, 'timeout_wf', 'wf-timeout-1', 0, 'RUNNING', '{}'::jsonb, 'default')",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .execute(conn)
    .await
    .expect("insert execution");
    id
}

/// Insert a RUNNING task claimed by `worker_id` linked to `workflow_exec_id`.
async fn insert_running_workflow_task(
    conn: &mut AsyncPgConnection,
    workflow_exec_id: Uuid,
    worker_id: &str,
) -> Uuid {
    let id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
         (id, queue_name, task_type, workflow_exec_id, input, state, worker_id, \
          attempt, max_attempts, started_at) \
         VALUES ($1, 'default', 'workflow', $2, '{}'::jsonb, 'RUNNING', $3, \
                 1, 3, NOW() - INTERVAL '30 seconds')",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .bind::<diesel::sql_types::Uuid, _>(workflow_exec_id)
    .bind::<diesel::sql_types::Text, _>(worker_id)
    .execute(conn)
    .await
    .expect("insert task");
    id
}

async fn task_state(conn: &mut AsyncPgConnection, task_id: Uuid) -> String {
    diesel::sql_query("SELECT state FROM harvest_task_queue WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .load::<TaskStateRow>(conn)
        .await
        .expect("query")
        .into_iter()
        .next()
        .expect("row")
        .state
}

async fn execution_state(conn: &mut AsyncPgConnection, exec_id: Uuid) -> String {
    diesel::sql_query("SELECT state FROM harvest_workflow_executions WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(exec_id)
        .load::<ExecStateRow>(conn)
        .await
        .expect("query")
        .into_iter()
        .next()
        .expect("row")
        .state
}

#[derive(QueryableByName)]
struct TaskStateRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    state: String,
}

#[derive(QueryableByName)]
struct ExecStateRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    state: String,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// `reset_timed_out_workflow_task` reverts a RUNNING task to PENDING so the
/// next poll cycle can re-claim it without waiting for the orphan-reclaim
/// staleness window.
#[tokio::test]
async fn reset_reverts_running_task_to_pending() {
    let (mut conn, pool, _container) = setup().await;

    let exec_id = insert_running_workflow(&mut conn).await;
    let task_id = insert_running_workflow_task(&mut conn, exec_id, "worker-1").await;

    assert_eq!(task_state(&mut conn, task_id).await, "RUNNING");

    reset_timed_out_workflow_task(&pool, task_id, "worker-1", 0).await;

    assert_eq!(
        task_state(&mut conn, task_id).await,
        "PENDING",
        "reset should move RUNNING → PENDING"
    );
    // Execution stays RUNNING (timeout below threshold doesn't fail it).
    assert_eq!(execution_state(&mut conn, exec_id).await, "RUNNING");
}

/// `reset_timed_out_workflow_task` is a no-op when the task is already
/// PENDING or was claimed by a different worker — the optimistic guard prevents
/// a stale reset from clobbering a fresh claim.
#[tokio::test]
async fn reset_is_idempotent_on_wrong_state_or_worker() {
    let (mut conn, pool, _container) = setup().await;

    let exec_id = insert_running_workflow(&mut conn).await;
    let task_id = insert_running_workflow_task(&mut conn, exec_id, "worker-1").await;

    // Wrong worker_id — should not transition.
    reset_timed_out_workflow_task(&pool, task_id, "worker-99", 0).await;
    assert_eq!(
        task_state(&mut conn, task_id).await,
        "RUNNING",
        "wrong worker_id must not reset the task"
    );

    // Correct worker — transitions.
    reset_timed_out_workflow_task(&pool, task_id, "worker-1", 0).await;
    assert_eq!(task_state(&mut conn, task_id).await, "PENDING");

    // Second call on PENDING task — no crash, still PENDING.
    reset_timed_out_workflow_task(&pool, task_id, "worker-1", 0).await;
    assert_eq!(task_state(&mut conn, task_id).await, "PENDING");
}

/// A stale reset must not clobber a re-claim taken at a newer claim epoch.
///
/// `poison_pill::requeue_orphan` hands an orphan back as `PENDING` with
/// `crash_strikes + 1`, and nothing stops the same worker from winning it
/// again. A `(state, worker_id)` guard alone matches that new claim. A reset
/// still in flight from the previous dispatch would then re-`PENDING` a row
/// whose replacement handler already runs. That invites a second concurrent
/// claim of one workflow task.
///
/// `crash_strikes` is the discriminator, because the requeue that creates the
/// race is what bumps it (issue #1459). It is the same argument
/// `queue::release_task_for_capability_miss` already records.
#[tokio::test]
async fn reset_does_not_clobber_a_reclaim_at_a_newer_claim_epoch() {
    let (mut conn, pool, _container) = setup().await;

    let exec_id = insert_running_workflow(&mut conn).await;
    let task_id = insert_running_workflow_task(&mut conn, exec_id, "worker-1").await;

    // The orphan requeue bumped the epoch, and the same worker re-claimed.
    diesel::sql_query("UPDATE harvest_task_queue SET crash_strikes = 1 WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .execute(&mut conn)
        .await
        .expect("bump the claim epoch");

    // The in-flight reset still carries the epoch it claimed at.
    reset_timed_out_workflow_task(&pool, task_id, "worker-1", 0).await;
    assert_eq!(
        task_state(&mut conn, task_id).await,
        "RUNNING",
        "a reset from the previous claim epoch must not release the new claim"
    );

    // The reset that belongs to the current claim still applies.
    reset_timed_out_workflow_task(&pool, task_id, "worker-1", 1).await;
    assert_eq!(task_state(&mut conn, task_id).await, "PENDING");
}

/// `quarantine_workflow_task_timeout` moves the task to the dead-letter queue,
/// fails the owning execution, and emits the `workflow.task_timeout` +
/// `workflow.terminal` metrics.
#[tokio::test]
async fn quarantine_writes_dlq_and_fails_execution() {
    let (mut conn, pool, _container) = setup().await;

    let exec_id = insert_running_workflow(&mut conn).await;
    let task_id = insert_running_workflow_task(&mut conn, exec_id, "worker-1").await;

    let metrics = std::sync::Arc::new(RecordingMetrics::default());

    quarantine_workflow_task_timeout(
        &pool,
        task_id,
        Some(exec_id),
        "worker-1",
        3,  // new_strikes (= threshold)
        10, // timeout_secs
        "timeout_wf",
        "default",
        &*metrics,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await;

    // Task row must be FAILED.
    assert_eq!(
        task_state(&mut conn, task_id).await,
        "FAILED",
        "timed-out task must be quarantined (FAILED)"
    );

    // Owning execution must be FAILED.
    assert_eq!(
        execution_state(&mut conn, exec_id).await,
        "FAILED",
        "owning execution must be failed after quarantine"
    );

    // DLQ must have one entry.
    let dlq_count = dead_letter_count(&mut conn).await.expect("dlq count");
    assert_eq!(dlq_count, 1, "exactly one DLQ entry expected");

    // Terminal metric emitted.
    let has_terminal = {
        let terminal = metrics.terminal.lock().unwrap();
        terminal
            .iter()
            .any(|(wf, q, _)| wf == "timeout_wf" && q == "default")
    };
    assert!(
        has_terminal,
        "record_workflow_terminal should be called with (timeout_wf, default)"
    );
}

/// The DLQ entry carries a `WorkflowTaskTimeout` typed reason so operators
/// can distinguish this from poison-pill quarantines.
#[tokio::test]
async fn quarantine_writes_typed_dlq_reason() {
    let (mut conn, pool, _container) = setup().await;

    let exec_id = insert_running_workflow(&mut conn).await;
    let task_id = insert_running_workflow_task(&mut conn, exec_id, "worker-1").await;

    let metrics = RecordingMetrics::default();
    quarantine_workflow_task_timeout(
        &pool,
        task_id,
        Some(exec_id),
        "worker-1",
        3,
        10,
        "timeout_wf",
        "default",
        &metrics,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await;

    // Inspect the raw reason JSON stored in harvest_dead_letters.
    let rows: Vec<ReasonRow> = diesel::sql_query("SELECT error FROM harvest_dead_letters LIMIT 1")
        .load(&mut conn)
        .await
        .expect("query dlq");
    let reason_json = rows.into_iter().next().expect("dlq row").error;

    let reason: DeadLetterReason =
        serde_json::from_str(&reason_json).expect("reason must deserialize");
    assert!(
        matches!(
            reason,
            DeadLetterReason::WorkflowTaskTimeout {
                task_timeout_strikes: 3,
                timeout_secs: 10
            }
        ),
        "DLQ reason must be WorkflowTaskTimeout, got {reason:?}"
    );
}

/// `quarantine_workflow_task_timeout` with no `exec_id` still marks the task
/// FAILED and writes a DLQ entry, but does not try to fail any execution.
#[tokio::test]
async fn quarantine_with_no_exec_id_only_fails_task() {
    let (mut conn, pool, _container) = setup().await;

    // Task with no execution association.
    let task_id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
         (id, queue_name, task_type, input, state, worker_id, attempt, max_attempts, started_at) \
         VALUES ($1, 'default', 'workflow', '{}'::jsonb, 'RUNNING', 'worker-1', 1, 3, NOW())",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .execute(&mut conn)
    .await
    .expect("insert orphan task");

    let metrics = RecordingMetrics::default();
    quarantine_workflow_task_timeout(
        &pool,
        task_id,
        None, // no exec_id
        "worker-1",
        3,
        10,
        "unknown",
        "default",
        &metrics,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await;

    assert_eq!(task_state(&mut conn, task_id).await, "FAILED");
    let dlq_count = dead_letter_count(&mut conn).await.expect("dlq count");
    assert_eq!(dlq_count, 1);
}

/// Count `harvest_events` rows for an execution.
async fn event_count(conn: &mut AsyncPgConnection, exec_id: Uuid) -> i64 {
    #[derive(QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_events WHERE workflow_exec_id = $1")
        .bind::<diesel::sql_types::Uuid, _>(exec_id)
        .load::<Count>(conn)
        .await
        .expect("event count query")
        .into_iter()
        .next()
        .expect("row")
        .n
}

/// Regression (issue #605 code review): a workflow-task timeout can race a
/// legitimate completion that commits between the timeout firing and this
/// quarantine's own transaction acquiring its lock. Quarantining must never
/// clobber (or misreport, via a spurious `WorkflowFailed` event and a
/// re-triggered completion-callback delivery) an execution that already
/// reached a different terminal state.
#[tokio::test]
async fn quarantine_does_not_overwrite_an_execution_that_already_completed() {
    let (mut conn, pool, _container) = setup().await;

    let exec_id = insert_running_workflow(&mut conn).await;
    let task_id = insert_running_workflow_task(&mut conn, exec_id, "worker-1").await;

    // Simulate the race: a legitimate completion commits before the
    // quarantine transaction acquires its row lock.
    diesel::sql_query(
        "UPDATE harvest_workflow_executions \
         SET state = 'COMPLETED', output = '{}'::jsonb, completed_at = NOW() \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id)
    .execute(&mut conn)
    .await
    .expect("simulate concurrent completion");

    let events_before = event_count(&mut conn, exec_id).await;

    let metrics = std::sync::Arc::new(RecordingMetrics::default());
    quarantine_workflow_task_timeout(
        &pool,
        task_id,
        Some(exec_id),
        "worker-1",
        3,
        10,
        "timeout_wf",
        "default",
        &*metrics,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await;

    // The timed-out task row is still failed/DLQ'd — that part is about the
    // task, not the execution's own terminal outcome.
    assert_eq!(task_state(&mut conn, task_id).await, "FAILED");
    let dlq_count = dead_letter_count(&mut conn).await.expect("dlq count");
    assert_eq!(dlq_count, 1);

    // But the execution itself must be untouched: still COMPLETED, and no
    // spurious WorkflowFailed event appended.
    assert_eq!(
        execution_state(&mut conn, exec_id).await,
        "COMPLETED",
        "a legitimately completed execution must not be overwritten to FAILED \
         by a racing quarantine"
    );
    assert_eq!(
        event_count(&mut conn, exec_id).await,
        events_before,
        "quarantine must not append a WorkflowFailed event once the execution \
         has already left RUNNING/PAUSED"
    );

    // No terminal("failed") metric should have been recorded for this race.
    let has_failed_terminal = {
        let terminal = metrics.terminal.lock().unwrap();
        terminal
            .iter()
            .any(|(wf, _, status)| wf == "timeout_wf" && status == "Failed")
    };
    assert!(
        !has_failed_terminal,
        "no failed-terminal metric should be recorded when quarantine loses the race"
    );
}
