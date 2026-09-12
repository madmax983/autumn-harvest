#![cfg(feature = "db")]
//! Poison-pill task quarantine integration tests — issue #367.
//!
//! A poison-pill task crashes the worker process instead of returning a clean
//! `Err`, leaving its row stuck in `RUNNING`. These tests exercise the
//! worker-liveness-driven orphan-reclaim scanner end-to-end against a real
//! Postgres container:
//!
//! - An orphan under the threshold is re-queued with an incremented strike.
//! - An orphan that reaches the threshold is quarantined to the DLQ, its queue
//!   row is FAILED, its owning workflow is failed terminally, and the
//!   `harvest.task.quarantined` metric is emitted.
//! - A task whose worker is still heartbeating is never reclaimed.
//! - A threshold of 0 disables quarantine (legacy requeue-forever behaviour).

use std::sync::Mutex;

use autumn_harvest::dlq::DeadLetterReason;
use autumn_harvest::poison_pill::{QUARANTINE_REASON, reclaim_orphaned_tasks};
use autumn_harvest::telemetry::MetricsRecorder;
use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

#[derive(Debug, Default)]
struct RecordingMetrics {
    quarantined: Mutex<Vec<(String, String)>>,
}

impl MetricsRecorder for RecordingMetrics {
    fn record_task_quarantined(&self, queue: &str, reason: &str) {
        self.quarantined
            .lock()
            .unwrap()
            .push((queue.to_owned(), reason.to_owned()));
    }
}

async fn setup_db() -> (AsyncPgConnection, ContainerAsync<Postgres>) {
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
    (conn, container)
}

/// Insert a RUNNING workflow execution and return its id.
async fn insert_running_workflow(conn: &mut AsyncPgConnection, workflow_id: &str) -> Uuid {
    insert_workflow_with_state(conn, workflow_id, "RUNNING").await
}

/// Insert a workflow execution in the given state and return its id.
async fn insert_workflow_with_state(
    conn: &mut AsyncPgConnection,
    workflow_id: &str,
    state: &str,
) -> Uuid {
    let id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
         (id, workflow_name, workflow_id, shard_id, state, input) \
         VALUES ($1, 'crasher_wf', $2, 0, $3, '{}'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .bind::<diesel::sql_types::Text, _>(workflow_id)
    .bind::<diesel::sql_types::Text, _>(state)
    .execute(conn)
    .await
    .expect("insert workflow execution");
    id
}

/// Insert a RUNNING task claimed by `worker_id` with the given crash-strike
/// count and no live worker row. Returns the task id.
async fn insert_running_task(
    conn: &mut AsyncPgConnection,
    workflow_exec_id: Option<Uuid>,
    worker_id: &str,
    crash_strikes: i32,
) -> Uuid {
    let id = Uuid::new_v4();
    // A stale last_heartbeat_at from the dead attempt is set so the requeue
    // path can be observed to clear it.
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
         (id, queue_name, task_type, workflow_exec_id, input, state, worker_id, \
          attempt, max_attempts, started_at, last_heartbeat_at, crash_strikes) \
         VALUES ($1, 'default', 'activity', $2, '{}'::jsonb, 'RUNNING', $3, \
                 1, 3, NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour', $4)",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Uuid>, _>(workflow_exec_id)
    .bind::<diesel::sql_types::Text, _>(worker_id)
    .bind::<diesel::sql_types::Integer, _>(crash_strikes)
    .execute(conn)
    .await
    .expect("insert running task");
    id
}

/// Insert a PENDING sibling activity task for `workflow_exec_id`. Returns its id.
async fn insert_pending_task(conn: &mut AsyncPgConnection, workflow_exec_id: Uuid) -> Uuid {
    let id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
         (id, queue_name, task_type, workflow_exec_id, input, state, \
          attempt, max_attempts) \
         VALUES ($1, 'default', 'activity', $2, '{}'::jsonb, 'PENDING', 0, 3)",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .bind::<diesel::sql_types::Uuid, _>(workflow_exec_id)
    .execute(conn)
    .await
    .expect("insert pending sibling task");
    id
}

/// Register a worker row with a fresh heartbeat (i.e. a live worker).
async fn insert_live_worker(conn: &mut AsyncPgConnection, worker_id: &str) {
    diesel::sql_query(
        "INSERT INTO harvest_workers \
         (worker_id, last_heartbeat_at, max_concurrency, host) \
         VALUES ($1, NOW(), 10, 'localhost')",
    )
    .bind::<diesel::sql_types::Text, _>(worker_id)
    .execute(conn)
    .await
    .expect("insert live worker");
}

#[derive(QueryableByName)]
struct DlqRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    error: String,
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    original_task_id: Uuid,
}

async fn task_state(conn: &mut AsyncPgConnection, task_id: Uuid) -> (String, i32, Option<String>) {
    #[derive(QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        state: String,
        #[diesel(sql_type = diesel::sql_types::Integer)]
        crash_strikes: i32,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        worker_id: Option<String>,
    }
    let row: Row = diesel::sql_query(
        "SELECT state, crash_strikes, worker_id FROM harvest_task_queue WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .get_result(conn)
    .await
    .expect("load task state");
    (row.state, row.crash_strikes, row.worker_id)
}

async fn task_last_heartbeat(
    conn: &mut AsyncPgConnection,
    task_id: Uuid,
) -> Option<chrono::DateTime<chrono::Utc>> {
    #[derive(QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
        last_heartbeat_at: Option<chrono::DateTime<chrono::Utc>>,
    }
    let row: Row =
        diesel::sql_query("SELECT last_heartbeat_at FROM harvest_task_queue WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(task_id)
            .get_result(conn)
            .await
            .expect("load task heartbeat");
    row.last_heartbeat_at
}

async fn workflow_state(conn: &mut AsyncPgConnection, exec_id: Uuid) -> String {
    #[derive(QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        state: String,
    }
    let row: Row = diesel::sql_query("SELECT state FROM harvest_workflow_executions WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(exec_id)
        .get_result(conn)
        .await
        .expect("load workflow state");
    row.state
}

#[tokio::test]
async fn orphan_under_threshold_is_requeued() {
    let (mut conn, _container) = setup_db().await;
    let exec_id = insert_running_workflow(&mut conn, "wf-requeue").await;
    let task_id = insert_running_task(&mut conn, Some(exec_id), "dead-worker-1", 0).await;
    let metrics = RecordingMetrics::default();

    let summary = reclaim_orphaned_tasks(
        &mut conn,
        3,
        10,
        None,
        &metrics,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("reclaim");

    assert_eq!(summary.requeued, 1, "orphan should be re-queued");
    assert_eq!(summary.quarantined, 0);

    let (state, strikes, worker) = task_state(&mut conn, task_id).await;
    assert_eq!(state, "PENDING", "re-queued back to PENDING");
    assert_eq!(strikes, 1, "crash strike recorded");
    assert_eq!(worker, None, "dead worker's claim cleared");
    // The dead attempt's stale heartbeat must be cleared so the fresh attempt
    // is not immediately timed out by the COALESCE scanner.
    let heartbeat = task_last_heartbeat(&mut conn, task_id).await;
    assert!(heartbeat.is_none(), "stale heartbeat cleared on requeue");
    // Workflow keeps running — a requeue is not terminal.
    assert_eq!(workflow_state(&mut conn, exec_id).await, "RUNNING");
    assert!(metrics.quarantined.lock().unwrap().is_empty());
}

#[tokio::test]
async fn orphan_at_threshold_is_quarantined() {
    let (mut conn, _container) = setup_db().await;
    let exec_id = insert_running_workflow(&mut conn, "wf-quarantine").await;
    // crash_strikes = 2; the next reclaim makes it 3 == threshold → quarantine.
    let task_id = insert_running_task(&mut conn, Some(exec_id), "dead-worker-2", 2).await;
    // A sibling activity still PENDING for the same execution.
    let sibling_id = insert_pending_task(&mut conn, exec_id).await;
    let metrics = RecordingMetrics::default();

    let summary = reclaim_orphaned_tasks(
        &mut conn,
        3,
        10,
        None,
        &metrics,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("reclaim");

    assert_eq!(summary.quarantined, 1, "orphan should be quarantined");
    assert_eq!(summary.requeued, 0);

    // Queue row is terminal FAILED — never re-dispatched again.
    let (state, _strikes, _worker) = task_state(&mut conn, task_id).await;
    assert_eq!(state, "FAILED");

    // Sibling task is drained so it can never be claimed after the workflow
    // has already failed.
    let (sibling_state, _, _) = task_state(&mut conn, sibling_id).await;
    assert_eq!(sibling_state, "FAILED", "sibling task drained on poison");

    // Owning workflow failed terminally (AC4).
    assert_eq!(workflow_state(&mut conn, exec_id).await, "FAILED");

    // DLQ row carries the typed PoisonPill reason with strikes + worker (AC5).
    let rows: Vec<DlqRow> =
        diesel::sql_query("SELECT error, original_task_id FROM harvest_dead_letters")
            .get_results(&mut conn)
            .await
            .expect("load dlq");
    assert_eq!(rows.len(), 1, "exactly one DLQ row");
    assert_eq!(rows[0].original_task_id, task_id);
    let reason: DeadLetterReason =
        serde_json::from_str(&rows[0].error).expect("DLQ error is typed reason");
    match reason {
        DeadLetterReason::PoisonPill {
            crash_strikes,
            last_worker_id,
        } => {
            assert_eq!(crash_strikes, 3);
            assert_eq!(last_worker_id.as_deref(), Some("dead-worker-2"));
        }
        DeadLetterReason::HistoryCapExceeded { .. } => {
            panic!("expected PoisonPill reason, got HistoryCapExceeded")
        }
        DeadLetterReason::WorkflowTaskTimeout { .. } => {
            panic!("expected PoisonPill reason, got WorkflowTaskTimeout")
        }
        DeadLetterReason::CallbackDeliveryExhausted { .. } => {
            panic!("expected PoisonPill reason, got CallbackDeliveryExhausted")
        }
    }

    // Metric emitted with queue + reason labels (AC6).
    let recorded = metrics.quarantined.lock().unwrap().clone();
    assert_eq!(
        recorded,
        vec![("default".to_string(), QUARANTINE_REASON.to_string())]
    );
}

#[tokio::test]
async fn live_worker_task_is_not_reclaimed() {
    let (mut conn, _container) = setup_db().await;
    let task_id = insert_running_task(&mut conn, None, "live-worker", 0).await;
    insert_live_worker(&mut conn, "live-worker").await;
    let metrics = RecordingMetrics::default();

    let summary = reclaim_orphaned_tasks(
        &mut conn,
        3,
        10,
        None,
        &metrics,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("reclaim");

    assert_eq!(
        summary.total(),
        0,
        "live worker's task must not be reclaimed"
    );
    let (state, strikes, worker) = task_state(&mut conn, task_id).await;
    assert_eq!(state, "RUNNING");
    assert_eq!(strikes, 0);
    assert_eq!(worker.as_deref(), Some("live-worker"));
}

#[tokio::test]
async fn threshold_zero_never_quarantines() {
    let (mut conn, _container) = setup_db().await;
    let exec_id = insert_running_workflow(&mut conn, "wf-disabled").await;
    // Very high strike count, but quarantine disabled (threshold 0).
    let task_id = insert_running_task(&mut conn, Some(exec_id), "dead-worker-3", 99).await;
    let metrics = RecordingMetrics::default();

    let summary = reclaim_orphaned_tasks(
        &mut conn,
        0,
        10,
        None,
        &metrics,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("reclaim");

    assert_eq!(summary.quarantined, 0, "threshold 0 disables quarantine");
    assert_eq!(summary.requeued, 1, "still re-queued (legacy loop)");
    let (state, strikes, _worker) = task_state(&mut conn, task_id).await;
    assert_eq!(state, "PENDING");
    assert_eq!(strikes, 100);
    assert_eq!(workflow_state(&mut conn, exec_id).await, "RUNNING");
}

#[tokio::test]
async fn clean_reschedule_resets_crash_streak() {
    let (mut conn, _container) = setup_db().await;
    let exec_id = insert_running_workflow(&mut conn, "wf-reset").await;
    // A task that accrued strikes from prior crashes.
    let task_id = insert_running_task(&mut conn, Some(exec_id), "some-worker", 2).await;

    // A clean continuation (suspension / retryable-error reschedule) must reset
    // the streak so the threshold measures *consecutive* crashes.
    autumn_harvest::queue::reschedule_task(&mut conn, task_id, chrono::Utc::now())
        .await
        .expect("reschedule");

    let (state, strikes, _worker) = task_state(&mut conn, task_id).await;
    assert_eq!(state, "PENDING");
    assert_eq!(strikes, 0, "clean progress resets the crash streak");
}

#[tokio::test]
async fn replay_into_terminal_workflow_is_rejected() {
    use autumn_harvest::dlq::{NewDeadLetterEntry, dead_letter, replay_dead_letter};
    use autumn_harvest::error::HarvestError;

    let (mut conn, _container) = setup_db().await;
    // Owning workflow is already terminal (as it is right after poison-pill
    // quarantine fails it).
    let exec_id = insert_workflow_with_state(&mut conn, "wf-terminal", "FAILED").await;
    let dlq_id = dead_letter(
        &mut conn,
        &NewDeadLetterEntry {
            original_task_id: Uuid::new_v4(),
            queue_name: "default".to_string(),
            task_type: "ACTIVITY".to_string(),
            workflow_exec_id: Some(exec_id),
            activity_name: Some("charge_card".to_string()),
            input: serde_json::json!({}),
            error: "poison".to_string(),
            attempts: 3,

            owner: None,
            severity: None,
        },
    )
    .await
    .expect("dlq insert");

    let result = replay_dead_letter(&mut conn, dlq_id, None).await;
    assert!(
        matches!(result, Err(HarvestError::WorkflowNotRunning(_))),
        "replay into a terminal workflow must be rejected, got {result:?}"
    );

    // The DLQ row is left intact — nothing was enqueued or deleted.
    let remaining: i64 =
        diesel::sql_query("SELECT COUNT(*)::bigint AS count FROM harvest_dead_letters")
            .get_result::<CountRow>(&mut conn)
            .await
            .expect("count dlq")
            .count;
    assert_eq!(remaining, 1, "rejected replay must not delete the DLQ row");
    let enqueued: i64 =
        diesel::sql_query("SELECT COUNT(*)::bigint AS count FROM harvest_task_queue")
            .get_result::<CountRow>(&mut conn)
            .await
            .expect("count tasks")
            .count;
    assert_eq!(enqueued, 0, "rejected replay must not enqueue a task");
}

#[tokio::test]
async fn poison_pill_counts_toward_schedule_auto_pause() {
    let (mut conn, _container) = setup_db().await;

    // A schedule with an auto-pause failure limit.
    let schedule_id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_schedules \
         (id, workflow_name, schedule_expr, timezone, catchup, max_active_runs, is_paused, \
          next_run_at, jitter_secs, overlap_policy, buffered_runs, buffer_all_max, skip_policy, \
          consecutive_failure_limit) \
         VALUES ($1, 'crasher_wf', 'interval:60', 'UTC', false, 10, false, \
                 NOW(), 0, 'skip', '[]'::jsonb, 100, 'skip', 5)",
    )
    .bind::<diesel::sql_types::Uuid, _>(schedule_id)
    .execute(&mut conn)
    .await
    .expect("insert schedule");

    // A schedule-triggered execution: workflow_id encodes the schedule UUID.
    let workflow_id = format!("sched:{schedule_id}:crasher_wf:0");
    let exec_id = insert_running_workflow(&mut conn, &workflow_id).await;
    // crash_strikes = 2 → next reclaim makes 3 == threshold → quarantine.
    let _task_id = insert_running_task(&mut conn, Some(exec_id), "dead-worker-x", 2).await;
    let metrics = RecordingMetrics::default();

    let summary = reclaim_orphaned_tasks(
        &mut conn,
        3,
        10,
        None,
        &metrics,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("reclaim");
    assert_eq!(summary.quarantined, 1);

    // The terminal poison-pill failure was counted toward the schedule's
    // consecutive-failure tally (issue #360 interaction).
    let row: FailRow =
        diesel::sql_query("SELECT consecutive_failure_count FROM harvest_schedules WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(schedule_id)
            .get_result(&mut conn)
            .await
            .expect("load schedule");
    assert_eq!(
        row.consecutive_failure_count, 1,
        "poison-pill workflow failure must count toward schedule auto-pause"
    );
}

#[derive(QueryableByName)]
struct FailRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    consecutive_failure_count: i32,
}

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}
