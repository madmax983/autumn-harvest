#![cfg(feature = "db")]
//! Capability-miss release-for-a-capable-peer integration tests — issue #804.
//!
//! A worker that claims a task whose handler it does not register used to
//! **fail the execution terminally**. During a rolling deploy that is a
//! self-inflicted outage: the old pods are perfectly healthy, they simply have
//! not been given the new handler yet, and `SKIP LOCKED` hands them the new
//! build's work anyway (the claim query has no capability predicate, and — by
//! construction — cannot have one: you cannot enumerate the handlers you have
//! *not* registered).
//!
//! These tests drive the **real worker poll loop** against a real Postgres and
//! prove the release-then-escalate contract end to end:
//!
//! - **AC1** — a workflow task whose `workflow_name` is unregistered is released
//!   back to `PENDING` (no `WorkflowFailed` appended, no `FAILED` transition),
//!   and a capable peer then drives the very same execution to `COMPLETED`.
//! - **AC2** — the same for an activity task whose `activity_name` is
//!   unregistered, using a realistic split-queue fleet.
//! - **AC3** — with **zero** capable workers the release is bounded: after the
//!   configured redelivery budget the task escalates through the ordinary
//!   terminal-failure path with the greppable `no_capable_worker:` reason.
//! - **AC4** — a capability miss is distinguishable from a poison-pill crash
//!   (#367) and a hung body (#494): `crash_strikes` is never incremented, the
//!   retry budget (`attempt`) is never drained, and no `PoisonPill` dead-letter
//!   row is written.
//! - **AC5** — `harvest.task.capability_miss` is emitted on every release and
//!   once with the distinct `escalated` outcome at the bound.
//! - **AC7** — no new `WorkflowEvent` variant: a release appends **nothing** to
//!   `harvest_events`, so replay determinism is untouched.
//!
//! Plus the session hard-pin carve-out: a session-pinned task (#606) can only
//! ever return to the *same* host, so "release for a capable peer" is false by
//! construction and it escalates immediately regardless of the budget.
//!
//! **Determinism.** Every test is *phased*: the incapable worker runs alone
//! until the release is observed, is shut down, and only then is the capable
//! worker started. Running both concurrently would make it a coin flip whether
//! the incapable worker ever sees the task at all.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it directly; otherwise a fresh testcontainers Postgres is booted
//! with the full migration bundle.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::error::CapabilityMissPhase;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::models::{NewWorkflowExecution, TaskQueueItem, WorkflowExecution};
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::schema::{
    harvest_dead_letters, harvest_rate_limit_buckets, harvest_task_queue, harvest_workers,
    harvest_workflow_executions,
};
use autumn_harvest::telemetry::{
    CAPABILITY_MISS_OUTCOME_ESCALATED, CAPABILITY_MISS_OUTCOME_ESCALATED_NEVER_OFFERED,
    CAPABILITY_MISS_OUTCOME_RELEASED, MetricsRecorder, TelemetryConfig,
};
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::{
    CapabilityMissPolicy, DbPool, EscalationCommit, EscalationEvidenceRecheck, HandlerRegistry,
    NO_CAPABLE_WORKER_PREFIX, SESSION_PINNED_ESCALATION_MARKER, TerminalWriteOutcome, Worker,
    WorkerRuntimeConfig, commit_terminal_failure_if_still_claimed,
    fail_suspended_workflow_if_still_claimed, preload_failure_history,
};
use autumn_harvest::{RetryPolicy, WorkflowContext, store};

use chrono::Utc;
use diesel::prelude::*;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// DB setup — env-var-redirectable, otherwise testcontainers.
// ---------------------------------------------------------------------------

fn init_sql() -> Vec<u8> {
    autumn_harvest::test_init_sql().as_bytes().to_vec()
}

async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool build failed")
}

async fn connect(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("failed to connect to Postgres")
}

// ---------------------------------------------------------------------------
// Capturing metrics recorder for `harvest.task.capability_miss` (AC5).
// ---------------------------------------------------------------------------

#[derive(Default)]
struct CapabilityMetrics {
    /// `(queue, task_type, outcome)` per emission, in order.
    misses: Mutex<Vec<(String, String, String)>>,
    /// Poison-pill quarantine emissions — must stay empty (AC4).
    quarantined: Mutex<Vec<(String, String)>>,
    /// `harvest.workflow.started` emissions as `(workflow, queue)`. A release
    /// must never let this fire twice for one execution (Codex round-17 P2).
    started: Mutex<Vec<(String, String)>>,
    /// `harvest.workflow.task_timeout` emissions as `(workflow, queue)` — the
    /// issue #494 signal, kept separate from `misses` so the two paths can be
    /// told apart (Codex round-25 P1).
    task_timeouts: Mutex<Vec<(String, String)>>,
}

impl CapabilityMetrics {
    fn samples(&self) -> Vec<(String, String, String)> {
        self.misses.lock().unwrap().clone()
    }

    fn count_with_outcome(&self, outcome: &str) -> usize {
        self.misses
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, _, o)| o == outcome)
            .count()
    }

    fn quarantine_count(&self) -> usize {
        self.quarantined.lock().unwrap().len()
    }

    fn started_samples(&self) -> Vec<(String, String)> {
        self.started.lock().unwrap().clone()
    }

    fn task_timeout_count(&self) -> usize {
        self.task_timeouts.lock().unwrap().len()
    }
}

impl MetricsRecorder for CapabilityMetrics {
    fn record_task_capability_miss(&self, queue: &str, task_type: &str, outcome: &str) {
        self.misses.lock().unwrap().push((
            queue.to_owned(),
            task_type.to_owned(),
            outcome.to_owned(),
        ));
    }

    fn record_task_quarantined(&self, queue: &str, reason: &str) {
        self.quarantined
            .lock()
            .unwrap()
            .push((queue.to_owned(), reason.to_owned()));
    }

    fn record_workflow_started(&self, workflow: &str, queue: &str) {
        self.started
            .lock()
            .unwrap()
            .push((workflow.to_owned(), queue.to_owned()));
    }

    fn record_workflow_task_timeout(&self, workflow: &str, queue: &str) {
        self.task_timeouts
            .lock()
            .unwrap()
            .push((workflow.to_owned(), queue.to_owned()));
    }
}

// ---------------------------------------------------------------------------
// Worker + registry construction.
// ---------------------------------------------------------------------------

fn build_registry(
    workflows: Vec<WorkflowInfo>,
    activities: Vec<ActivityInfo>,
    metrics: Arc<CapabilityMetrics>,
) -> Arc<HandlerRegistry> {
    let telemetry = Arc::new(
        TelemetryConfig::builder()
            .metrics(metrics as Arc<dyn MetricsRecorder>)
            .build(),
    );
    Arc::new(HandlerRegistry::with_state_and_telemetry(
        workflows,
        activities,
        autumn_harvest::context::empty_shared_state(),
        telemetry,
    ))
}

/// Registry for the AC2 split-queue fleet: the workflow plus the activity it
/// dispatches. Shared by the workflow-queue worker (which enqueues the activity
/// but never polls its queue) and the capable `acts` worker, so the two can
/// never drift into disagreeing about what the "capable" handler set is.
fn activity_peer_registry(metrics: Arc<CapabilityMetrics>) -> Arc<HandlerRegistry> {
    build_registry(
        vec![workflow_info(
            "activity_peer_wf",
            workflow_calls_peer_activity,
        )],
        vec![activity_info(
            "peer_only_activity",
            peer_only_activity,
            Q_ACT_ACTS,
        )],
        metrics,
    )
}

fn build_worker(
    worker_id: &str,
    queues: &[&str],
    registry: Arc<HandlerRegistry>,
    capability_miss_max_redeliveries: u32,
) -> Arc<Worker> {
    build_worker_tuned(
        worker_id,
        queues,
        registry,
        capability_miss_max_redeliveries,
        Duration::from_secs(30),
        Duration::from_secs(60),
        3,
    )
}

/// `build_worker` with the two issue #494 knobs exposed.
///
/// Only the round-25 P1 regression guard needs them: it has to make the
/// workflow decision cycle overrun a *short* budget and quarantine on the
/// *first* strike, so the timeout path is reached deterministically rather
/// than after 90 seconds and three redeliveries.
fn build_worker_tuned(
    worker_id: &str,
    queues: &[&str],
    registry: Arc<HandlerRegistry>,
    capability_miss_max_redeliveries: u32,
    workflow_task_timeout: Duration,
    max_local_activity_start_to_close: Duration,
    poison_pill_threshold: i32,
) -> Arc<Worker> {
    Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                codec_rotation_batch_size: 0,
                dr: autumn_harvest::replication::DrConfig::default(),
                worker_id: worker_id.to_string(),
                queues: queues.iter().map(|q| (*q).to_string()).collect(),
                notification_database_url: None,
                max_concurrent_workflows: 1,
                max_concurrent_activities: 1,
                poll_interval: Duration::from_millis(25),
                shutdown_timeout: Duration::from_secs(1),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close,
                shard_assignments: vec![autumn_harvest::types::ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
                build_id: String::new(),
                deployment_name: None,
                workflow_cache_size: 1000,
                priority_aging_secs: None,
                unknown_target_grace_window: Duration::from_secs(5),
                poison_pill_threshold,
                capability_miss_max_redeliveries,
                workflow_task_timeout,
                workflow_panic_max_attempts: 3,
                labels: HashMap::new(),
                queue_weights: HashMap::new(),
                max_workflow_pause_duration: Duration::from_secs(24 * 3600),
                max_workflow_history_events: None,
                shard_notification_database_urls: Vec::new(),
                sharded_pool: None,
                slot_tuner: None,
                max_concurrent_sessions: 0,
            },
            registry,
        )
        .expect("worker should build"),
    )
}

/// Run `worker` against `pool` until `until` resolves, then shut it down.
///
/// Every test phases its workers this way so the "incapable worker sees the
/// task first" precondition is deterministic rather than a race.
async fn with_worker_running<F, T>(worker: &Arc<Worker>, pool: &DbPool, until: F) -> T
where
    F: std::future::Future<Output = T>,
{
    let runner = Arc::clone(worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move { runner.run(&pool_for_run).await });
    let out = until.await;
    worker.shutdown();
    handle.await.expect("worker task joins cleanly");
    out
}

// ---------------------------------------------------------------------------
// Seeding + read helpers.
// ---------------------------------------------------------------------------

async fn seed_execution(
    conn: &mut AsyncPgConnection,
    workflow_name: &'static str,
    input: serde_json::Value,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(autumn_harvest::types::ShardId::new(0));
    let row = NewWorkflowExecution {
        quota_key: None,
        id: exec_id.as_uuid(),
        workflow_name,
        workflow_id: &format!("wf-{}", exec_id.as_uuid()),
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: input.clone(),
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
        continued_from_exec_id: None,
        first_exec_id: None,
        start_source: None,
        start_source_ref: None,
        started_by: None,
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("insert workflow execution");

    store::append_events(
        conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted");

    exec_id
}

/// Seed an execution and enqueue its initial workflow task on `queue`.
async fn seed_workflow(
    conn: &mut AsyncPgConnection,
    workflow_name: &'static str,
    input: serde_json::Value,
    queue_name: &str,
) -> ExecutionId {
    let exec_id = seed_execution(conn, workflow_name, input.clone()).await;
    let mut params = EnqueueParams::new(queue_name, TaskType::Workflow, input);
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);
    queue::enqueue(conn, &params)
        .await
        .expect("enqueue workflow task");
    exec_id
}

async fn load_execution(url: &str, exec_id: ExecutionId) -> WorkflowExecution {
    let mut conn = connect(url).await;
    load_execution_on(&mut conn, exec_id).await
}

/// [`load_execution`] on a caller-owned connection.
///
/// The polling helpers below reuse one connection for the whole wait instead of
/// establishing a fresh one per iteration. At a 25ms poll interval a 30s bound
/// is up to 1,200 TCP+auth handshakes against the database for a single wait,
/// and this suite runs against a *shared* Postgres. That storm competes for
/// connection slots with the very worker the wait is waiting on — including its
/// startup fleet registration, which gates task claiming (issue #804, round 54)
/// — so the polling can starve the progress it is polling for.
async fn load_execution_on(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> WorkflowExecution {
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(conn)
        .await
        .expect("reload workflow execution")
}

async fn load_tasks(url: &str, exec_id: ExecutionId) -> Vec<TaskQueueItem> {
    let mut conn = connect(url).await;
    load_tasks_on(&mut conn, exec_id).await
}

/// [`load_tasks`] on a caller-owned connection. See [`load_execution_on`].
async fn load_tasks_on(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> Vec<TaskQueueItem> {
    harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
        .order(harvest_task_queue::scheduled_at.asc())
        .select(TaskQueueItem::as_select())
        .load(conn)
        .await
        .expect("reload task rows")
}

async fn load_task(url: &str, task_id: Uuid) -> TaskQueueItem {
    let mut conn = connect(url).await;
    load_task_on(&mut conn, task_id).await
}

/// [`load_task`] on a caller-owned connection. See [`load_execution_on`].
async fn load_task_on(conn: &mut AsyncPgConnection, task_id: Uuid) -> TaskQueueItem {
    harvest_task_queue::table
        .find(task_id)
        .select(TaskQueueItem::as_select())
        .first(conn)
        .await
        .expect("reload task row")
}

async fn load_history(url: &str, exec_id: ExecutionId) -> Vec<WorkflowEvent> {
    let mut conn = connect(url).await;
    store::load_history(&mut conn, exec_id)
        .await
        .expect("load_history")
        .events
}

async fn dead_letter_errors(url: &str, exec_id: ExecutionId) -> Vec<String> {
    let mut conn = connect(url).await;
    harvest_dead_letters::table
        .filter(harvest_dead_letters::workflow_exec_id.eq(Some(exec_id.as_uuid())))
        .select(harvest_dead_letters::error)
        .load(&mut conn)
        .await
        .expect("load dead letters")
}

/// Seed a task row's distinct-miss set as if `workers` had each already claimed
/// and released it (issue #804 round 6).
///
/// The redelivery budget is consumed per DISTINCT worker, so an all-incapable
/// *fleet* is what exhausts it — a single worker bouncing never can. Seeding the
/// prior set models the fleet without standing up N worker runtimes, and drives
/// the real decision path: `resolve_capability_miss` reads this column back off
/// the claimed row.
/// The frontier key the direct-`queue` release tests below pass.
///
/// These tests exercise clauses that are not about the frontier (cardinality,
/// the error column, crash strikes, the claim guard), so they all use one key
/// and seed it where the row's counters have to survive the release -- the
/// SAME-frontier arm (issue #804, Codex round-46 P1).
const TEST_FRONTIER: &str = "workflow:noop_wf";

async fn seed_prior_missing_workers(
    url: &str,
    exec_id: ExecutionId,
    workers: &[&str],
    // The workflow these misses were recorded against (issue #804, Codex
    // round-46 P1). Evidence is keyed by frontier, so seeding it without the
    // handler it is about would read as a mismatch -- a fresh budget -- and
    // every escalation these tests assert would silently become a release.
    missing_workflow: &str,
) {
    let mut conn = connect(url).await;
    let owned: Vec<String> = workers.iter().map(|w| (*w).to_string()).collect();
    let misses = i32::try_from(owned.len()).expect("small");
    diesel::update(
        harvest_task_queue::table
            .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid()))),
    )
    .set((
        harvest_task_queue::capability_miss_workers.eq(owned),
        harvest_task_queue::capability_misses.eq(misses),
        harvest_task_queue::capability_miss_handler.eq(Some(format!(
            "{}:{missing_workflow}",
            autumn_harvest::worker::CapabilityMissKind::Workflow.as_str()
        ))),
    ))
    .execute(&mut conn)
    .await
    .expect("seed prior missing workers");
}

/// Poll until at least one task row for `exec_id` reports `capability_misses >= want`.
async fn wait_for_capability_misses(
    url: &str,
    exec_id: ExecutionId,
    want: i32,
    timeout: Duration,
) -> TaskQueueItem {
    let mut conn = connect(url).await;
    tokio::time::timeout(timeout, async {
        loop {
            if let Some(task) = load_tasks_on(&mut conn, exec_id)
                .await
                .into_iter()
                .find(|t| t.capability_misses >= want)
            {
                break task;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no task reached capability_misses >= {want} within {timeout:?}"))
}

/// Wait until `worker_id` has recorded a miss on the task, or the execution
/// went terminal trying.
///
/// The frontier-scoping test cannot wait on `capability_misses` reaching a
/// count: the whole point of the fix is that the counter RESETS on durable
/// inline progress, so the target value is identical before and after it. It
/// also cannot wait on the release alone — under the bug the miss escalates and
/// the row never returns to `PENDING`, which would surface a crisp accounting
/// regression as a bare timeout. Waiting on "this worker's miss is resolved,
/// either way" terminates promptly under both outcomes and lets the assertions
/// name the difference.
async fn wait_for_miss_by(
    url: &str,
    exec_id: ExecutionId,
    worker_id: &str,
    timeout: Duration,
) -> Option<TaskQueueItem> {
    let mut conn = connect(url).await;
    tokio::time::timeout(timeout, async {
        loop {
            if let Some(task) = load_tasks_on(&mut conn, exec_id)
                .await
                .into_iter()
                .find(|t| t.capability_miss_workers.iter().any(|w| w == worker_id))
            {
                break Some(task);
            }
            if autumn_harvest::erase::is_terminal_state(
                &load_execution_on(&mut conn, exec_id).await.state,
            ) {
                break None;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!("worker {worker_id} neither recorded a miss nor sealed the run within {timeout:?}")
    })
}

/// Poll until some task row for `exec_id` is claimed by `worker_id`.
///
/// The claim-theft test needs a positive observable for "the workflow body has
/// run", and the body's own effect — handing the row to another owner — is
/// exactly that.
async fn wait_for_task_owner(
    url: &str,
    exec_id: ExecutionId,
    worker_id: &str,
    timeout: Duration,
) -> TaskQueueItem {
    let mut conn = connect(url).await;
    let waited = tokio::time::timeout(timeout, async {
        loop {
            if let Some(task) = load_tasks_on(&mut conn, exec_id)
                .await
                .into_iter()
                .find(|t| t.worker_id.as_deref() == Some(worker_id))
            {
                break task;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;

    if let Ok(task) = waited {
        return task;
    }
    // Dump what distinguishes the plausible causes rather than panicking bare
    // (same rationale as `wait_for_terminal`): a row still `PENDING` with a
    // future `scheduled_at` is a backoff, a row owned by someone else is a lost
    // race, and an EMPTY worker registry is a registration that never committed
    // — which gates claiming outright (issue #804, round 54) and is invisible
    // from the task row alone.
    let mut diag = connect(url).await;
    let tasks = load_tasks_on(&mut diag, exec_id).await;
    let workers: Vec<(String, String)> = harvest_workers::table
        .select((harvest_workers::worker_id, harvest_workers::status))
        .load(&mut diag)
        .await
        .expect("dump worker registry");
    let execution = load_execution_on(&mut diag, exec_id).await;
    panic!(
        "no task row was claimed by {worker_id} within {timeout:?}; tasks={:?}; \
         execution=({}, {:?}); registry={workers:?}",
        tasks
            .iter()
            .map(|t| (
                t.id,
                t.state.clone(),
                t.worker_id.clone(),
                t.attempt,
                t.crash_strikes,
                t.capability_misses,
                t.error.clone(),
            ))
            .collect::<Vec<_>>(),
        execution.state,
        execution.error,
    )
}

async fn wait_for_state(
    url: &str,
    exec_id: ExecutionId,
    want: &str,
    timeout: Duration,
) -> WorkflowExecution {
    let mut conn = connect(url).await;
    tokio::time::timeout(timeout, async {
        loop {
            let e = load_execution_on(&mut conn, exec_id).await;
            if e.state == want {
                break e;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("execution did not reach state {want} within {timeout:?}"))
}

/// Wait for ANY terminal state and return the row.
///
/// The success-metric test must distinguish "still running" from "went FAILED":
/// waiting for `COMPLETED` specifically would report a spurious escalation as a
/// bare timeout, hiding the `no_capable_worker:` reason that explains it.
async fn wait_for_terminal(
    url: &str,
    exec_id: ExecutionId,
    timeout: Duration,
) -> WorkflowExecution {
    let mut conn = connect(url).await;
    let waited = tokio::time::timeout(timeout, async {
        loop {
            let e = load_execution_on(&mut conn, exec_id).await;
            if autumn_harvest::erase::is_terminal_state(&e.state) {
                break e;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;

    if let Ok(execution) = waited {
        execution
    } else {
        {
            // Dump the state that distinguishes the plausible causes instead of
            // panicking bare: a `PENDING` row with `scheduled_at` in the future
            // and a high `capability_misses` is the backoff tail; a `RUNNING`
            // row owned by a live worker is a stranded claim (a real defect).
            // A bare timeout costs a whole round of log archaeology to tell
            // those apart.
            let execution = load_execution(url, exec_id).await;
            let tasks = load_tasks(url, exec_id).await;
            let rows: Vec<String> = tasks
                .iter()
                .map(|t| {
                    format!(
                        "task {} type={} state={} worker={:?} attempt={} \
                         capability_misses={} miss_workers={:?} crash_strikes={} \
                         scheduled_at={} error={:?}",
                        t.id,
                        t.task_type,
                        t.state,
                        t.worker_id,
                        t.attempt,
                        t.capability_misses,
                        t.capability_miss_workers,
                        t.crash_strikes,
                        t.scheduled_at,
                        t.error,
                    )
                })
                .collect();
            panic!(
                "execution {exec_id} reached no terminal state within {timeout:?}\n  \
                 execution state={} error={:?}\n  now={}\n  {}",
                execution.state,
                execution.error,
                Utc::now(),
                rows.join("\n  "),
            )
        }
    }
}

// ---------------------------------------------------------------------------
// Shared assertions.
//
// The release contract is identical for workflow and activity tasks, so both
// AC1 and AC2 assert it through one helper — a divergence between them would
// then be a compile-visible edit, not a silently-drifting copy.
// ---------------------------------------------------------------------------

/// Assert the invariants every capability-miss release must satisfy, whatever
/// the task type *or phase*: the claim is dropped for a peer, none of the
/// *crash* accounting is touched (AC4), and the row is not marked as having
/// failed.
///
/// `attempt` is deliberately **not** asserted here, because it is the one
/// clause that legitimately differs by phase (Codex round-17 P2): a
/// pre-handler release restores it, a post-handler release must not. Use
/// [`assert_released_before_the_handler`] for the pre-handler case, which is
/// where restoring it is the AC4 retry-budget guarantee.
fn assert_released_for_a_peer(task: &TaskQueueItem) {
    assert_eq!(
        task.state, "PENDING",
        "a capability miss must release the claim, not hold or fail it"
    );
    assert!(
        task.worker_id.is_none(),
        "the releasing worker must drop ownership so a peer can claim"
    );
    assert!(
        task.started_at.is_none(),
        "started_at must be cleared so the row is not mistaken for in-flight work"
    );
    assert!(
        task.sticky_worker_id.is_none(),
        "sticky affinity must be cleared or the incapable worker keeps first refusal"
    );

    // AC4 — distinguishable from a poison-pill crash (#367) and a hung body (#494).
    assert_eq!(
        task.crash_strikes, 0,
        "a clean capability miss must never increment crash strikes (AC4)"
    );
    // The `error` column is reserved for genuine task failures and must stay
    // untouched. `/stack` (issue #773) renders any non-null `error` on a pending
    // activity as a `last_failure` — and because a pre-handler release also
    // restores `attempt` to 0, a breadcrumb here would surface as a failure at
    // an otherwise-unreachable attempt 0 for work that never ran, violating
    // #773 AC3. The operator breadcrumb reaches them through the release's
    // `tracing::info!` and the `harvest.task.capability_miss` counter instead;
    // `release_never_writes_the_error_column` covers both directions.
    assert!(
        task.error.is_none(),
        "a capability miss is not a task failure and must not write `error`; got {:?}",
        task.error
    );
}

/// [`assert_released_for_a_peer`] plus the clause that is specific to a
/// **pre-handler** miss: the claim-time `attempt` increment is undone.
///
/// This is the AC4 retry-budget guarantee, and it belongs to exactly the phase
/// where nothing ran — which is every activity-task miss, and a workflow task
/// whose *type* was unregistered. A post-handler release deliberately leaves
/// `attempt` alone; see `CapabilityMissPhase::restores_dispatch_attempt`.
fn assert_released_before_the_handler(task: &TaskQueueItem) {
    assert_released_for_a_peer(task);
    assert_eq!(
        task.attempt, 0,
        "a pre-handler release must restore the attempt `claim_task` consumed, \
         or the retry budget silently drains (AC4)"
    );
}

/// Assert the capability-miss counter recorded a release for this task shape,
/// and that nothing was escalated or quarantined (AC4 + AC5).
fn assert_released_sample(metrics: &CapabilityMetrics, queue: &str, task_type: &str) {
    let samples = metrics.samples();
    assert!(
        samples
            .iter()
            .any(|(q, t, o)| q == queue && t == task_type && o == CAPABILITY_MISS_OUTCOME_RELEASED),
        "expected a released capability-miss sample for {queue}/{task_type}, got {samples:?}"
    );
    assert_eq!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_ESCALATED),
        0,
        "must not escalate while the budget is nowhere near exhausted, got {samples:?}"
    );
    assert_eq!(
        metrics.quarantine_count(),
        0,
        "a capability miss is not a poison pill (AC4)"
    );
}

// ---------------------------------------------------------------------------
// Handlers.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Per-test queue names.
//
// The whole module shares one database (and, under `--test-threads=1`, runs
// back to back), so every test gets its own queue. Without this a worker left
// polling `default` could claim a *sibling* test's seeded task and skew its
// capability-miss counts.
// ---------------------------------------------------------------------------

const Q_WF_MISS: &str = "q804-wf-miss";
const Q_ACT_WF: &str = "q804-act-wf";
const Q_ACT_ACTS: &str = "q804-act-acts";
const Q_ESCALATE: &str = "q804-escalate";
const Q_SESSION: &str = "q804-session";
const Q_MIXED: &str = "q804-mixed";

/// Budget for the success-metric test. Deliberately far above the shipped
/// default: see the rationale at its use site.
const MIXED_FLEET_BUDGET: u32 = 50;

/// How long `mixed_fleet_completes_every_execution_with_zero_spurious_failures`
/// waits for one execution to reach a terminal state.
///
/// **Sized from the backoff curve, not picked as a round number.** The claim
/// query has no capability filter — that is issue #804's premise — so on every
/// redelivery the incapable and the capable worker race 50/50 for the task, and
/// a run of consecutive incapable claims is a normal, expected outcome rather
/// than a fault. Each miss defers the task by `capability_miss_backoff`, which
/// runs 1s, 2s, 4s, 8s, 16s and then caps at 30s, so the cumulative deferral
/// after `k` misses is `31 + 30(k - 5)` seconds for `k >= 5`:
///
/// | misses | cumulative | P(a given task) | P(any of the 8) |
/// |--------|-----------|-----------------|-----------------|
/// | 6      | 61 s      | 1.56 %          | **11.8 %**      |
/// | 10     | 181 s     | 0.098 %         | 0.78 %          |
/// | 14     | 301 s     | 0.0061 %        | **0.049 %**     |
///
/// At the previous 60s bound the test therefore failed roughly **one run in
/// eight** — observed once locally and once on CI — with a bare timeout that
/// looked like a hang but was just the feature's own (deliberate, capped)
/// backoff outrunning the harness. 300s tolerates 14 misses per task, putting
/// the false-failure rate near 1-in-2000, and CI latency on a contended runner
/// stacks on top of the backoff, which is why the margin is generous.
///
/// This costs nothing in the common case: the wait returns the moment the row
/// goes terminal, so a healthy run still finishes in seconds. Only a genuine
/// stall spends the full budget — and `wait_for_terminal` now dumps the task
/// rows on timeout, so that case is diagnosable in one shot instead of needing
/// a round of log archaeology to tell a backoff tail from a stranded claim.
const MIXED_FLEET_TERMINAL_WAIT: Duration = Duration::from_secs(300);
const Q_NOOP: &str = "q804-noop";
const Q_PARK: &str = "q804-park";
const Q_LIVE_PEER: &str = "q804-live-peer";
const Q_STARTED_ONCE: &str = "q804-started-once";
const Q_BODY_BUDGET: &str = "q804-body-budget";
const Q_STARTED_ONCE_ACTS: &str = "q804-started-once-acts";
const Q_LOCAL_MISS: &str = "q804-local-miss";
/// The breadcrumb whose presence proves the inline arm's commit block ran.
const LOCAL_MISS_BREADCRUMB: &str = "about to run the local activity";

type BoxFut<'a> =
    Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>>;

/// Trivial workflow used as the "new build only" handler.
fn peer_only_workflow(_ctx: &WorkflowContext, _input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move { Ok(serde_json::json!("done")) })
}

/// Decoy so an "incapable" worker still has a non-empty registry — a capability
/// miss must be about *this* handler being absent, not about the worker having
/// no handlers at all.
fn decoy_workflow(_ctx: &WorkflowContext, _input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move { Ok(serde_json::json!("decoy")) })
}

/// Where `workflow_invalidates_peer_then_misses` reaches the database, and the
/// peer whose evidence it clears (issue #804 round 27).
///
/// A **pool**, not a URL, for the exact reason [`CLAIM_THEFT_INJECTION`]
/// documents: the whole body has only `executor::SUSPENSION_TIMEOUT` (100 ms)
/// to reach a command-emitting suspension, and a fresh
/// `AsyncPgConnection::establish` per dispatch pays a full TCP+auth handshake
/// inside that window — slow enough under load to blow the budget outright
/// (issue #1182, Codex round-3: this test failed the exact same way the
/// primary #1182 regression did, `"workflow suspended without emitted
/// commands"`, because the raw connect never resolved in time and the miss
/// this test exists to exercise was never reached). A pooled, pre-warmed
/// checkout reuses a live connection and pays only the `UPDATE`.
///
/// A `OnceLock` rather than a parameter because a `WorkflowHandlerFn` is a bare
/// `fn` pointer with a fixed signature and cannot capture.
///
/// A **pool**, not a URL, for exactly the reason [`CLAIM_THEFT_INJECTION`]
/// documents one round later: the invalidation has to land inside the dispatch
/// window, and the whole body has only `executor::SUSPENSION_TIMEOUT` (100 ms)
/// to reach a command-emitting suspension. A fresh
/// `AsyncPgConnection::establish` pays a full TCP+auth handshake *inside* that
/// window on a database this suite shares; blow the 100 ms and the dispatch is
/// failed with "workflow suspended without emitted commands", the run goes
/// terminal, and this test fails claiming the decision used a stale snapshot
/// when in truth the body never reached the miss at all. The pool must be
/// pre-warmed before the worker starts — see [`prewarm_race_injection`].
static RACE_INJECTION: std::sync::OnceLock<(DbPool, String)> = std::sync::OnceLock::new();

/// Force [`RACE_INJECTION`]'s lazy `deadpool` to establish a connection now, so
/// the in-body invalidation pays only a warm checkout plus one statement.
///
/// Round-trips rather than merely checking out, for the same reason
/// [`prewarm_claim_theft_injection`] does: `deadpool` hands back a connection
/// object before the first statement proves the TCP+auth handshake actually
/// completed, and it is the handshake — not the checkout — that is slow under
/// load.
async fn prewarm_race_injection() {
    let (pool, _) = RACE_INJECTION
        .get()
        .expect("race injection is armed before it is pre-warmed");
    let mut conn = pool.get().await.expect("race pre-warm connects");
    diesel::sql_query("SELECT 1")
        .execute(&mut conn)
        .await
        .expect("race pre-warm round-trips");
}

/// The deterministic injection point for the round-27 race: a workflow body
/// that clears a peer's capability-miss evidence **while its own dispatch holds
/// the claim**, then misses on an activity this worker does not register.
///
/// This is the real interleaving, not a simulation of it. The body runs strictly
/// between `claim_task` (which took the snapshot naming the peer) and
/// `persist_scheduled_activities` (which raises the `AfterHandler` miss and
/// decides), so a registration landing here is exactly a pod restarting
/// mid-dispatch. Injecting it from inside the body is what makes the race
/// deterministic instead of a sleep-and-hope.
fn workflow_invalidates_peer_then_misses(
    ctx: &WorkflowContext,
    input: serde_json::Value,
) -> BoxFut<'_> {
    Box::pin(async move {
        if let Some((pool, peer)) = RACE_INJECTION.get() {
            let mut conn = pool.get().await.expect("race injection connects");
            queue::invalidate_capability_miss_evidence_for_worker(
                &mut conn,
                peer,
                &[Q_CLAIM_RACE.to_string()],
            )
            .await
            .expect("race injection invalidates the peer's evidence");
        }
        // Now miss: this activity is registered nowhere on this worker, so the
        // enqueue preflight raises an `AfterHandler` capability miss.
        ctx.execute_activity_raw("never_registered_activity", input, Q_CLAIM_RACE)
            .await
            .map_err(|e| e.to_string())
    })
}

/// Where `workflow_steals_own_claim_then_misses` reaches the database (issue
/// #804 round 28). Same `OnceLock` rationale as [`RACE_INJECTION`].
/// A **pool**, not a URL, deliberately. The theft has to land in the narrow
/// window between the claim committing and the miss being decided, and it is
/// the only thing standing between the stolen row and a terminal failure. A
/// fresh `AsyncPgConnection::establish` per dispatch pays a full TCP+auth
/// handshake inside that window on a database this suite shares — slow enough
/// under load to miss the window (the theft then matches zero rows, because the
/// row is no longer `RUNNING`), and able to fail outright, which panics the body
/// and turns a precise assertion into a bare timeout. A pooled checkout reuses a
/// warm connection and waits rather than failing.
///
/// The pool must be **pre-warmed** by the test before the worker starts — see
/// [`prewarm_claim_theft_injection`]. `deadpool` builds lazily, so an unwarmed
/// pool pays that same TCP+auth handshake on its *first* `get()`, and this test
/// performs exactly one dispatch: without the pre-warm the handshake lands
/// inside the very window the pool exists to protect. The whole body has only
/// `executor::SUSPENSION_TIMEOUT` (100 ms) to reach a command-emitting
/// suspension; blow that and the dispatch is failed with "workflow suspended
/// without emitted commands", the theft never happens, and this test fails as a
/// bare `wait_for_task_owner` timeout that names none of the above.
static CLAIM_THEFT_INJECTION: std::sync::OnceLock<DbPool> = std::sync::OnceLock::new();

/// Force [`CLAIM_THEFT_INJECTION`]'s lazy `deadpool` to establish a connection
/// now, so the in-body theft pays only a warm checkout plus one `UPDATE`.
///
/// Round-trips rather than merely checking out: `deadpool` hands back a
/// connection object before the first statement proves the TCP+auth handshake
/// actually completed, and it is the handshake — not the checkout — that is
/// slow under load.
async fn prewarm_claim_theft_injection() {
    let pool = CLAIM_THEFT_INJECTION
        .get()
        .expect("claim-theft injection is armed before it is pre-warmed");
    let mut conn = pool.get().await.expect("claim-theft pre-warm connects");
    diesel::sql_query("SELECT 1")
        .execute(&mut conn)
        .await
        .expect("claim-theft pre-warm round-trips");
}

/// The deterministic injection point for the round-28 lost-claim P1: a workflow
/// body that hands its own task row to another worker **while its dispatch is
/// still running**, then misses.
///
/// This is what a concurrent poison-pill reclaim or an operator action looks
/// like from the dispatcher's side, hit exactly rather than raced for: the body
/// runs strictly between `claim_task` and the `AfterHandler` miss raised by
/// `persist_scheduled_activities`, so by the time the decision runs the row is
/// genuinely owned by someone else.
fn workflow_steals_own_claim_then_misses(
    ctx: &WorkflowContext,
    input: serde_json::Value,
) -> BoxFut<'_> {
    Box::pin(async move {
        if let Some(pool) = CLAIM_THEFT_INJECTION.get() {
            let mut conn = pool.get().await.expect("claim-theft injection connects");
            let stolen = diesel::sql_query(
                "UPDATE harvest_task_queue SET worker_id = 'thief' \
                 WHERE queue_name = $1 AND state = 'RUNNING'",
            )
            .bind::<diesel::sql_types::Text, _>(Q_LOST_CLAIM)
            .execute(&mut conn)
            .await
            .expect("claim-theft injection reassigns the row");
            // A zero-row theft is the one outcome that silently invalidates the
            // whole test: the dispatch then still owns the row, the escalation
            // is correct rather than suppressed, and the wait for the new owner
            // times out with nothing to say. Fail on the spot instead.
            assert_eq!(
                stolen, 1,
                "claim-theft injection must reassign exactly the claimed row"
            );
        }
        ctx.execute_activity_raw("never_registered_activity", input, Q_LOST_CLAIM)
            .await
            .map_err(|e| e.to_string())
    })
}

/// Workflow that dispatches one activity onto the dedicated activity queue and
/// returns its output (AC2's realistic split-queue fleet).
fn workflow_calls_peer_activity(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.execute_activity_raw("peer_only_activity", input, Q_ACT_ACTS)
            .await
            .map_err(|e| e.to_string())
    })
}

/// Sibling of [`workflow_calls_peer_activity`] on its own dedicated activity
/// queue, so the round-17 start-metric test cannot claim (or be blocked by) an
/// adjacent test's activity row in the shared database.
fn workflow_calls_started_once_activity(
    ctx: &WorkflowContext,
    input: serde_json::Value,
) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.execute_activity_raw("started_once_activity", input, Q_STARTED_ONCE_ACTS)
            .await
            .map_err(|e| e.to_string())
    })
}

/// Workflow that parks on a signal that never arrives — the cleanest
/// `park_workflow_task` shape, because unlike an activity dispatch it needs no
/// second handler lookup (which would itself be a capability miss).
fn workflow_waits_for_signal(ctx: &WorkflowContext, _input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.wait_for_signal("never_arrives")
            .await
            .map_err(|e| e.to_string())
    })
}

fn peer_only_activity(
    _ctx: &autumn_harvest::ActivityContext,
    _input: serde_json::Value,
) -> BoxFut<'_> {
    Box::pin(async move { Ok(serde_json::json!("activity done")) })
}

fn decoy_activity(_ctx: &autumn_harvest::ActivityContext, _input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move { Ok(serde_json::json!("decoy")) })
}

fn workflow_info(
    name: &'static str,
    handler: autumn_harvest::info::WorkflowHandlerFn,
) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "capability_miss_tests",
        handler,
        execution_timeout: None,
        chain_execution_timeout: None,
        sla: None,
        concurrency: None,
        debounce: None,
        batch: None,
        throttle: None,
        max_input_bytes: None,
        owner: None,
        runbook_url: None,
        severity: None,
        description: None,
        input_schema: None,
        output_schema: None,
        error_schema: None,
        retry_policy: None,
    }
}

fn activity_info(
    name: &'static str,
    handler: autumn_harvest::info::ActivityHandlerFn,
    default_queue: &'static str,
) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "capability_miss_tests",
        default_retry_policy: Some(RetryPolicy::fixed(3, Duration::from_millis(50))),
        default_start_to_close: Some(Duration::from_secs(5)),
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: Some(default_queue),
        max_concurrent: None,
        concurrency_key: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        rate_limit_key_expr: None,
        circuit_breaker: None,
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        requires: None,
        handler,
    }
}

// ---------------------------------------------------------------------------
// AC1 — a workflow-task capability miss is released for a capable peer.
// ---------------------------------------------------------------------------

/// The headline #804 claim: a task for an unregistered workflow handler is
/// **released**, not failed, and the very same execution then reaches a normal
/// terminal outcome once a capable peer claims it. Zero spurious `FAILED`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_capability_miss_is_released_for_a_capable_peer() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);

    let exec_id = seed_workflow(
        &mut conn,
        "peer_only_wf",
        serde_json::json!({"n": 1}),
        Q_WF_MISS,
    )
    .await;

    // --- Phase 1: only the INCAPABLE worker is live. ------------------------
    let old_build_metrics = Arc::new(CapabilityMetrics::default());
    let old_build = build_worker(
        "worker-old-build",
        &[Q_WF_MISS],
        build_registry(
            vec![workflow_info("some_other_wf", decoy_workflow)],
            vec![],
            Arc::clone(&old_build_metrics),
        ),
        // Generous budget so phase 1 provably cannot escalate while we watch.
        50,
    );

    let released = with_worker_running(&old_build, &pool, async {
        wait_for_capability_misses(&url, exec_id, 1, Duration::from_secs(20)).await
    })
    .await;

    // AC1 — released back to PENDING for a peer, not failed.
    assert_released_before_the_handler(&released);
    assert_eq!(released.task_type, "workflow");
    assert_eq!(
        released.capability_misses, 1,
        "exactly one capability miss should have been recorded"
    );
    assert_eq!(
        released.max_attempts, 3,
        "the retry budget itself is untouched"
    );

    // The execution itself is untouched: still RUNNING, no terminal event.
    let execution = load_execution(&url, exec_id).await;
    assert_eq!(
        execution.state, "RUNNING",
        "a capability miss must not transition the execution (AC1)"
    );

    // AC7 — no new event variant, and in fact no event at all.
    let history = load_history(&url, exec_id).await;
    assert_eq!(
        history.len(),
        1,
        "a release must append nothing to harvest_events; got {history:?}"
    );
    assert!(
        matches!(history[0], WorkflowEvent::WorkflowStarted { .. }),
        "only the seeded WorkflowStarted should be present, got {:?}",
        history[0]
    );

    // AC5 — the release is observable.
    assert_released_sample(&old_build_metrics, Q_WF_MISS, "workflow");

    // --- Phase 2: the CAPABLE peer picks up the very same task. -------------
    let new_build_metrics = Arc::new(CapabilityMetrics::default());
    let new_build = build_worker(
        "worker-new-build",
        &[Q_WF_MISS],
        build_registry(
            vec![workflow_info("peer_only_wf", peer_only_workflow)],
            vec![],
            Arc::clone(&new_build_metrics),
        ),
        50,
    );

    let completed = with_worker_running(&new_build, &pool, async {
        wait_for_state(&url, exec_id, "COMPLETED", Duration::from_secs(30)).await
    })
    .await;

    assert_eq!(completed.state, "COMPLETED");
    assert_eq!(
        completed.output,
        Some(serde_json::json!("done")),
        "the capable peer ran the real handler"
    );
    assert_eq!(
        new_build_metrics.samples().len(),
        0,
        "the capable worker had nothing to release"
    );
    assert!(
        dead_letter_errors(&url, exec_id).await.is_empty(),
        "no dead-letter row for a run that completed normally"
    );
}

// ---------------------------------------------------------------------------
// AC2 — an activity-task capability miss is released for a capable peer.
// ---------------------------------------------------------------------------

/// Realistic split-queue fleet: the workflow runs on `default` and dispatches
/// its activity to `acts`. An old-build worker polling `acts` claims the
/// activity task it cannot run and releases it; a capable `acts` worker then
/// completes it and the workflow finishes normally.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn activity_capability_miss_is_released_for_a_capable_peer() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);

    let exec_id = seed_workflow(
        &mut conn,
        "activity_peer_wf",
        serde_json::json!({"n": 2}),
        Q_ACT_WF,
    )
    .await;

    // --- Phase 1: a workflow-only worker enqueues the activity onto `acts`,
    //     and an incapable `acts` worker claims and releases it. -------------
    //
    // The workflow worker must itself register the activity (the enqueue path
    // resolves the activity's declared defaults from the registry) but never
    // polls `acts`, so it can never run it.
    let wf_worker = build_worker(
        "worker-workflow-queue",
        &[Q_ACT_WF],
        activity_peer_registry(Arc::new(CapabilityMetrics::default())),
        50,
    );

    let old_acts_metrics = Arc::new(CapabilityMetrics::default());
    let old_acts_worker = build_worker(
        "worker-acts-old-build",
        &[Q_ACT_ACTS],
        build_registry(
            vec![],
            vec![activity_info(
                "some_other_activity",
                decoy_activity,
                Q_ACT_ACTS,
            )],
            Arc::clone(&old_acts_metrics),
        ),
        50,
    );

    let released = with_worker_running(&wf_worker, &pool, async {
        with_worker_running(
            &old_acts_worker,
            &pool,
            Box::pin(async {
                wait_for_capability_misses(&url, exec_id, 1, Duration::from_secs(30)).await
            }),
        )
        .await
    })
    .await;

    // AC2 — the ACTIVITY task is the one released.
    assert_eq!(released.task_type, "activity");
    assert_eq!(released.queue_name, Q_ACT_ACTS);
    assert_released_before_the_handler(&released);
    assert_eq!(
        released.activity_name.as_deref(),
        Some("peer_only_activity"),
        "the release must preserve activity_name on an activity row (unlike a \
         workflow row, where it can carry a stale suspension sentinel)"
    );
    assert_released_sample(&old_acts_metrics, Q_ACT_ACTS, "activity");

    // The execution is still RUNNING and carries no terminal event.
    assert_eq!(load_execution(&url, exec_id).await.state, "RUNNING");
    let history = load_history(&url, exec_id).await;
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. })),
        "a released activity must not fail the owning workflow, got {history:?}"
    );

    // --- Phase 2: a capable `acts` worker finishes the job. -----------------
    let full_fleet = build_worker(
        "worker-full-fleet",
        &[Q_ACT_WF, Q_ACT_ACTS],
        activity_peer_registry(Arc::new(CapabilityMetrics::default())),
        50,
    );

    let completed = with_worker_running(&full_fleet, &pool, async {
        wait_for_state(&url, exec_id, "COMPLETED", Duration::from_secs(30)).await
    })
    .await;

    assert_eq!(
        completed.output,
        Some(serde_json::json!("activity done")),
        "the workflow returned the capable peer's activity output"
    );
    assert!(dead_letter_errors(&url, exec_id).await.is_empty());
}

// ---------------------------------------------------------------------------
// A post-handler release must not let `harvest.workflow.started` fire twice.
// ---------------------------------------------------------------------------

/// `record_workflow_started` fires exactly once per execution, gated on
/// `task.attempt == 1 && no scheduling events in history`. A **post-handler**
/// capability miss (the persist-time pre-pass finding an unregistered activity)
/// happens *after* that gate has already fired, and its persistence transaction
/// rolls back — so nothing lands in history either. If the release also rolled
/// `attempt` back to `0`, the next capable claim would see `attempt == 1` with
/// no scheduling events and emit the counter a **second** time, inflating
/// workflow-start counts and every SLO derived from them (Codex round-17 P2).
///
/// The fix is phase-gated: only a `BeforeHandler` release restores `attempt`
/// (nothing ran, and the metric was never emitted, so the next dispatch *must*
/// still look like the first). `During`/`AfterHandler` releases leave it alone.
/// That is safe because `task.attempt` is a retry budget only for **activity**
/// tasks — and every activity-task miss is `BeforeHandler` — while on a
/// workflow row it is read solely by this metric gate and the informational DLQ
/// `attempts` field. The #523 workflow-level retry loop counts
/// `executions.workflow_attempt`, a different column entirely.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_post_handler_release_does_not_re_emit_workflow_started() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);

    let exec_id = seed_workflow(
        &mut conn,
        "started_once_wf",
        serde_json::json!({"n": 17}),
        Q_STARTED_ONCE,
    )
    .await;

    // ONE recorder across both phases: the assertion is about the total number
    // of `harvest.workflow.started` emissions for this execution, however many
    // workers observed it.
    let metrics = Arc::new(CapabilityMetrics::default());

    // --- Phase 1: a worker that CAN run the body but NOT the activity. ------
    //
    // The body runs to a conclusion (so the start metric fires), then the
    // persist-time capability pre-pass finds `started_once_activity`
    // unregistered and releases the workflow task — an `AfterHandler` miss.
    // It polls the activity queue too, so a later inability to *complete* the
    // run can only be the capability miss, never a routing gap.
    let half_capable = build_worker(
        "worker-body-only",
        &[Q_STARTED_ONCE, Q_STARTED_ONCE_ACTS],
        build_registry(
            vec![workflow_info(
                "started_once_wf",
                workflow_calls_started_once_activity,
            )],
            vec![activity_info(
                "some_other_activity",
                decoy_activity,
                Q_STARTED_ONCE_ACTS,
            )],
            Arc::clone(&metrics),
        ),
        50,
    );

    let released = with_worker_running(&half_capable, &pool, async {
        wait_for_capability_misses(&url, exec_id, 1, Duration::from_secs(30)).await
    })
    .await;

    assert_eq!(
        released.task_type, "workflow",
        "the persist-time pre-pass releases the WORKFLOW task, not an activity \
         row -- the activity was never enqueued"
    );
    assert_released_for_a_peer(&released);
    assert_eq!(
        metrics.started_samples().len(),
        1,
        "the body ran, so the start metric fired exactly once in phase 1: {:?}",
        metrics.started_samples()
    );
    assert_eq!(
        released.attempt, 1,
        "a post-handler release must LEAVE `attempt` at its claimed value: \
         rolling it back to 0 is precisely what makes the next capable claim \
         look like a first dispatch and re-emit the start metric"
    );
    // The rolled-back persist means history is still just the seeded start
    // event -- which is the other half of the metric gate's condition, and why
    // `attempt` alone has to carry the "already dispatched" signal here.
    assert_eq!(
        load_history(&url, exec_id).await.len(),
        1,
        "the persist transaction rolled back, so no scheduling event landed"
    );

    // --- Phase 2: the fully capable worker finishes the job. ----------------
    let capable = build_worker(
        "worker-full",
        &[Q_STARTED_ONCE, Q_STARTED_ONCE_ACTS],
        build_registry(
            vec![workflow_info(
                "started_once_wf",
                workflow_calls_started_once_activity,
            )],
            vec![activity_info(
                "started_once_activity",
                peer_only_activity,
                Q_STARTED_ONCE_ACTS,
            )],
            Arc::clone(&metrics),
        ),
        50,
    );

    let completed = with_worker_running(&capable, &pool, async {
        wait_for_state(&url, exec_id, "COMPLETED", Duration::from_secs(30)).await
    })
    .await;

    assert_eq!(
        completed.output,
        Some(serde_json::json!("activity done")),
        "the capable worker ran the real activity"
    );
    assert_eq!(
        metrics.started_samples(),
        vec![("started_once_wf".to_string(), Q_STARTED_ONCE.to_string())],
        "`harvest.workflow.started` must be emitted EXACTLY ONCE for this \
         execution across the whole incapable-then-capable dispatch sequence"
    );
    assert!(dead_letter_errors(&url, exec_id).await.is_empty());
}

// ---------------------------------------------------------------------------
// Codex round-19 P2 — a local-activity miss releases BEFORE its batch's
// sibling commands are committed.
// ---------------------------------------------------------------------------

/// A workflow that publishes an operator breadcrumb and then runs a **local**
/// activity, so both commands arrive in one suspension batch — the shape whose
/// commits used to run ahead of the handler lookup.
fn workflow_details_then_local_activity(
    ctx: &WorkflowContext,
    input: serde_json::Value,
) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.set_current_details(LOCAL_MISS_BREADCRUMB);
        ctx.execute_local_activity_raw("local_only_activity", input, None, None)
            .await
            .map_err(|e| e.to_string())
    })
}

/// A local activity — `is_local: true` is what routes it through the inline arm
/// rather than the task queue.
fn local_activity_info(name: &'static str) -> ActivityInfo {
    ActivityInfo {
        is_local: true,
        default_queue: None,
        ..activity_info(name, peer_only_activity, Q_LOCAL_MISS)
    }
}

/// The inline local-activity arm commits search-attribute patches, the
/// `current_details` breadcrumb, durable logs (#790), progress frames (#791),
/// and any early mutex release (#691) *before* `run_local_activity_inline`
/// resolves the handler. Once a miss became releasable, an incapable worker
/// paid for all five and handed the task on, and the next worker redid them.
///
/// `current_details` is the cheapest durable, directly-queryable witness for the
/// whole block: it is `NULL` until that commit runs. Phase 1 asserts the
/// incapable worker left it `NULL`; phase 2 asserts the capable worker *does*
/// write it, so the phase-1 assertion cannot be passing merely because the
/// workflow never sets it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_local_activity_miss_releases_before_committing_its_batch() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);

    let exec_id = seed_workflow(
        &mut conn,
        "local_miss_wf",
        serde_json::json!({"n": 19}),
        Q_LOCAL_MISS,
    )
    .await;

    let metrics = Arc::new(CapabilityMetrics::default());

    // --- Phase 1: registers the workflow, NOT the local activity. -----------
    let incapable = build_worker(
        "worker-no-local",
        &[Q_LOCAL_MISS],
        build_registry(
            vec![workflow_info(
                "local_miss_wf",
                workflow_details_then_local_activity,
            )],
            vec![local_activity_info("some_other_local_activity")],
            Arc::clone(&metrics),
        ),
        50,
    );

    let released = with_worker_running(&incapable, &pool, async {
        wait_for_capability_misses(&url, exec_id, 1, Duration::from_secs(30)).await
    })
    .await;

    assert_eq!(
        released.task_type, "workflow",
        "a local activity runs inline on the WORKFLOW task, so that is the row \
         released -- no activity row is ever enqueued"
    );
    assert_released_for_a_peer(&released);
    assert_eq!(
        load_execution(&url, exec_id).await.current_details,
        None,
        "the incapable worker must release BEFORE the batch's sibling commands \
         are committed -- a written breadcrumb means it patched attributes, \
         appended logs, and fired progress frames on its way to discovering it \
         could not run the local activity at all"
    );
    // The body began and did not conclude, so `attempt` stays at its claimed
    // value: this is a `DuringHandler` miss, not a `BeforeHandler` one.
    assert_eq!(
        released.attempt, 1,
        "a mid-handler release must not roll `attempt` back -- the body ran"
    );

    // --- Phase 2: a worker that registers the local activity finishes. ------
    let capable = build_worker(
        "worker-with-local",
        &[Q_LOCAL_MISS],
        build_registry(
            vec![workflow_info(
                "local_miss_wf",
                workflow_details_then_local_activity,
            )],
            vec![local_activity_info("local_only_activity")],
            Arc::clone(&metrics),
        ),
        50,
    );

    let completed = with_worker_running(&capable, &pool, async {
        wait_for_state(&url, exec_id, "COMPLETED", Duration::from_secs(30)).await
    })
    .await;

    assert_eq!(
        completed.output,
        Some(serde_json::json!("activity done")),
        "the capable worker ran the local activity inline"
    );
    assert_eq!(
        completed.current_details.as_deref(),
        Some(LOCAL_MISS_BREADCRUMB),
        "the capable worker DOES commit the breadcrumb -- without this the \
         phase-1 assertion would hold for the wrong reason"
    );
    assert!(dead_letter_errors(&url, exec_id).await.is_empty());
}

// ---------------------------------------------------------------------------
// Frontier-scoped miss accounting (issue #804, Codex round-20 P1).
// ---------------------------------------------------------------------------

const Q_TWO_FRONTIER: &str = "q804-two-frontier";

fn second_frontier_activity(
    _ctx: &autumn_harvest::ActivityContext,
    _input: serde_json::Value,
) -> BoxFut<'_> {
    Box::pin(async move { Ok(serde_json::json!("second frontier done")) })
}

/// Two local-activity frontiers in one workflow. The inline drive loop resolves
/// them in the SAME dispatch without parking, so a worker can clear the first
/// and still miss the second.
fn workflow_two_local_frontiers(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.execute_local_activity_raw("first_local", input.clone(), None, None)
            .await
            .map_err(|e| e.to_string())?;
        ctx.execute_local_activity_raw("second_local", input, None, None)
            .await
            .map_err(|e| e.to_string())
    })
}

fn local_activity_info_on(
    name: &'static str,
    handler: autumn_harvest::info::ActivityHandlerFn,
    queue: &'static str,
) -> ActivityInfo {
    ActivityInfo {
        is_local: true,
        default_queue: None,
        ..activity_info(name, handler, queue)
    }
}

/// One member of the deliberately non-nested two-frontier fleet: registers the
/// shared workflow and exactly ONE of the two local activities.
///
/// Budget of 1 throughout, so the distinct-worker bound escalates on the SECOND
/// distinct misser — conflating the two frontiers therefore fails the run at the
/// earliest possible point rather than somewhere downstream.
fn two_frontier_worker(
    worker_id: &str,
    local: &'static str,
    handler: autumn_harvest::info::ActivityHandlerFn,
    metrics: &Arc<CapabilityMetrics>,
) -> Arc<Worker> {
    build_worker(
        worker_id,
        &[Q_TWO_FRONTIER],
        build_registry(
            vec![workflow_info(
                "two_frontier_wf",
                workflow_two_local_frontiers,
            )],
            vec![local_activity_info_on(local, handler, Q_TWO_FRONTIER)],
            Arc::clone(metrics),
        ),
        1,
    )
}

const Q_EXHAUST_FRONTIER: &str = "q804-exhaust-frontier";

fn always_failing_local_activity(
    _ctx: &autumn_harvest::ActivityContext,
    _input: serde_json::Value,
) -> BoxFut<'_> {
    Box::pin(async move { Err("frontier 1 always fails".to_string()) })
}

/// Frontier 1 exhausts its retries and the workflow CATCHES that, so the run
/// carries on to frontier 2 rather than sealing. That is what makes exhaustion
/// observable as a frontier transition instead of a terminal outcome.
fn workflow_exhausting_then_second(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        let _ = ctx
            .execute_local_activity_raw("failing_local", input.clone(), None, None)
            .await;
        ctx.execute_local_activity_raw("second_local", input, None, None)
            .await
            .map_err(|e| e.to_string())
    })
}

/// As [`two_frontier_worker`], but for the exhaustion fleet: one retry attempt
/// so frontier 1 reaches `LocalActivityExhausted` promptly, and a budget of 1 so
/// conflating the two frontiers fails the run at the earliest possible point.
fn exhausting_frontier_worker(
    worker_id: &str,
    local: &'static str,
    handler: autumn_harvest::info::ActivityHandlerFn,
    metrics: &Arc<CapabilityMetrics>,
) -> Arc<Worker> {
    let info = ActivityInfo {
        default_retry_policy: Some(RetryPolicy::fixed(1, Duration::from_millis(10))),
        ..local_activity_info_on(local, handler, Q_EXHAUST_FRONTIER)
    };
    build_worker(
        worker_id,
        &[Q_EXHAUST_FRONTIER],
        build_registry(
            vec![workflow_info(
                "exhausting_frontier_wf",
                workflow_exhausting_then_second,
            )],
            vec![info],
            Arc::clone(metrics),
        ),
        1,
    )
}

/// `capability_misses` / `capability_miss_workers` describe ONE frontier — the
/// position the workflow is currently stuck at. Durable inline progress moves
/// that position, so every miss recorded before it was recorded against a
/// *different* handler and must not count against the new one.
///
/// The fleet here is deliberately non-nested, which is what makes the run
/// completable only by successive claims: worker A has `second_local` but not
/// `first_local`; worker B has `first_local` but not `second_local`. Neither can
/// finish the run alone, but B can clear frontier 1 and A can then clear
/// frontier 2 — so a correct engine completes it.
///
/// Without frontier scoping, B's miss on `second_local` is added to A's miss on
/// `first_local`, the registry reports every live worker as having missed "the
/// task", and the run is terminally failed at a budget of 1 while A — which
/// registers the handler the run is actually waiting on — is live and polling.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_inline_progress_starts_the_next_frontier_with_a_clean_budget() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);

    let exec_id = seed_workflow(
        &mut conn,
        "two_frontier_wf",
        serde_json::json!({"n": 20}),
        Q_TWO_FRONTIER,
    )
    .await;

    let metrics = Arc::new(CapabilityMetrics::default());
    let has_second = || {
        two_frontier_worker(
            "worker-has-second",
            "second_local",
            second_frontier_activity,
            &metrics,
        )
    };

    // --- Phase 1: A misses frontier 1 (`first_local`). ----------------------
    let released_a = with_worker_running(&has_second(), &pool, async {
        wait_for_capability_misses(&url, exec_id, 1, Duration::from_secs(30)).await
    })
    .await;
    assert_released_for_a_peer(&released_a);
    assert_eq!(
        released_a.capability_miss_workers,
        vec!["worker-has-second".to_string()],
        "phase 1 records A as the misser of frontier 1"
    );

    // --- Phase 2: B clears frontier 1 durably, then misses frontier 2. ------
    let has_first = two_frontier_worker(
        "worker-has-first",
        "first_local",
        peer_only_activity,
        &metrics,
    );

    let released_b = with_worker_running(&has_first, &pool, async {
        wait_for_miss_by(&url, exec_id, "worker-has-first", Duration::from_secs(30)).await
    })
    .await;

    // The premise: B really did make durable progress before missing.
    let history = load_history(&url, exec_id).await;
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::LocalActivityCompleted { .. })),
        "phase 2's premise is that B durably completed frontier 1 before \
         missing frontier 2; without that event this test proves nothing: {history:?}"
    );
    assert_eq!(
        load_execution(&url, exec_id).await.state,
        "RUNNING",
        "B missed a frontier NO worker has yet been asked about -- A has never \
         been shown `second_local`. Failing here asserts the exact opposite of \
         the truth while a capable worker is live"
    );
    let released_b = released_b.expect(
        "B must record a miss and release; a sealed run here IS the bug -- the \
         miss was charged to the frontier A already missed",
    );
    assert_released_for_a_peer(&released_b);
    assert_eq!(
        released_b.capability_miss_workers,
        vec!["worker-has-first".to_string()],
        "durable inline progress starts a NEW frontier, so the set must hold \
         only the workers that missed THIS one -- A's miss was at frontier 1"
    );
    assert_eq!(
        released_b.capability_misses, 1,
        "the count is scoped to the current frontier too, or the configured-total \
         bound inherits the prior frontier's spend"
    );

    // --- Phase 3: A replays past frontier 1 and clears frontier 2. ----------
    let completed = with_worker_running(&has_second(), &pool, async {
        wait_for_state(&url, exec_id, "COMPLETED", Duration::from_secs(30)).await
    })
    .await;

    assert_eq!(
        completed.output,
        Some(serde_json::json!("second frontier done")),
        "A replays past the durably-completed frontier 1 and runs frontier 2"
    );
    assert!(
        dead_letter_errors(&url, exec_id).await.is_empty(),
        "a run the fleet could finish must never reach the DLQ"
    );
}

/// Retry **exhaustion** retires a frontier as decisively as success does — the
/// local activity will never be attempted again, so the run's stuck position has
/// moved. Both outcomes therefore reset the accounting, and both do it in the
/// same transaction as the append that makes the progress durable (issue #804,
/// Codex round-21 P1).
///
/// Same non-nested fleet as the success case above, but frontier 1 is a local
/// activity that always fails and a workflow that catches the exhaustion and
/// carries on. Threading the reset into only the success append leaves this path
/// charging B's miss on frontier 2 to A's miss on frontier 1, and a budget of 1
/// then fails a run the fleet can still finish.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exhausting_a_local_activity_also_starts_the_next_frontier_clean() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);

    let exec_id = seed_workflow(
        &mut conn,
        "exhausting_frontier_wf",
        serde_json::json!({"n": 21}),
        Q_EXHAUST_FRONTIER,
    )
    .await;

    let metrics = Arc::new(CapabilityMetrics::default());
    let has_second = || {
        exhausting_frontier_worker(
            "worker-has-second",
            "second_local",
            second_frontier_activity,
            &metrics,
        )
    };

    // --- Phase 1: A misses frontier 1 (`failing_local`). --------------------
    let released_a = with_worker_running(&has_second(), &pool, async {
        wait_for_capability_misses(&url, exec_id, 1, Duration::from_secs(30)).await
    })
    .await;
    assert_released_for_a_peer(&released_a);

    // --- Phase 2: B exhausts frontier 1 durably, then misses frontier 2. ----
    let has_first = exhausting_frontier_worker(
        "worker-has-first",
        "failing_local",
        always_failing_local_activity,
        &metrics,
    );

    let released_b = with_worker_running(&has_first, &pool, async {
        wait_for_miss_by(&url, exec_id, "worker-has-first", Duration::from_secs(30)).await
    })
    .await;

    // The premise: B really did exhaust frontier 1 before missing frontier 2.
    let history = load_history(&url, exec_id).await;
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::LocalActivityExhausted { .. })),
        "this test's premise is the EXHAUSTION append, not the success one; \
         without that event it re-tests the success path: {history:?}"
    );

    let released_b = released_b.expect(
        "B must record a miss and release; a sealed run here means the exhaustion \
         append did not carry the frontier reset",
    );
    assert_released_for_a_peer(&released_b);
    assert_eq!(
        released_b.capability_miss_workers,
        vec!["worker-has-first".to_string()],
        "an exhausted frontier is behind us, so its misser set must not follow \
         the run to the next one"
    );
    assert_eq!(
        released_b.capability_misses, 1,
        "the count is scoped to the current frontier on the exhaustion path too"
    );

    // --- Phase 3: A replays past the exhausted frontier and clears frontier 2.
    let completed = with_worker_running(&has_second(), &pool, async {
        wait_for_state(&url, exec_id, "COMPLETED", Duration::from_secs(30)).await
    })
    .await;

    assert_eq!(
        completed.output,
        Some(serde_json::json!("second frontier done")),
        "A replays past the durably-exhausted frontier 1 and runs frontier 2"
    );
    assert!(
        dead_letter_errors(&url, exec_id).await.is_empty(),
        "a run the fleet could finish must never reach the DLQ"
    );
}

// ---------------------------------------------------------------------------
// AC3 + AC5 — bounded release: escalate when no capable worker ever appears.
// ---------------------------------------------------------------------------

/// With **zero** capable workers the release must not loop forever. Once the
/// budget's worth of DISTINCT workers have each missed the task, the next one
/// escalates through the ordinary terminal-failure path, tagged with the
/// greppable `no_capable_worker:` prefix.
///
/// The budget is consumed per distinct worker (issue #804 round 6), so it is an
/// all-incapable *fleet* that exhausts it. Three prior workers are seeded onto
/// the row and this test's worker is the fourth — one past a budget of 3.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capability_miss_escalates_after_the_budget_with_no_capable_worker() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);

    let exec_id = seed_workflow(
        &mut conn,
        "never_registered_wf",
        serde_json::json!({}),
        Q_ESCALATE,
    )
    .await;
    // A budget of 3 that three distinct workers have already spent.
    seed_prior_missing_workers(
        &url,
        exec_id,
        &["pod-a-incapable", "pod-b-incapable", "pod-c-incapable"],
        "never_registered_wf",
    )
    .await;

    let metrics = Arc::new(CapabilityMetrics::default());
    let worker = build_worker(
        "worker-no-capable-peer",
        &[Q_ESCALATE],
        build_registry(
            vec![workflow_info("some_other_wf", decoy_workflow)],
            vec![],
            Arc::clone(&metrics),
        ),
        3,
    );

    let failed = with_worker_running(&worker, &pool, async {
        wait_for_state(&url, exec_id, "FAILED", Duration::from_secs(60)).await
    })
    .await;

    // AC3 — the terminal reason is unambiguous and greppable.
    let error = failed.error.unwrap_or_default();
    assert!(
        error.starts_with(NO_CAPABLE_WORKER_PREFIX),
        "escalation must carry the stable `no_capable_worker:` prefix, got {error:?}"
    );
    assert!(
        error.contains("never_registered_wf"),
        "the reason must name the missing handler, got {error:?}"
    );
    assert!(
        error.contains("after 3 capability-miss redeliveries"),
        "the reason must state the exact number of releases that completed \
         (a single-char check cannot catch an off-by-one), got {error:?}"
    );
    // The knob is reported separately from the release count (Codex round-23
    // P1). They coincide here -- a distinct sweep where each worker missed once
    // -- but the string must name the knob in its own labelled position, so an
    // operator can tell a real 3-release sweep from a budget that happens to be
    // 3.
    assert!(
        error.contains("capability_miss_max_redeliveries = 3"),
        "the configured knob must be named so the fix is actionable, got {error:?}"
    );

    // AC3 — it routes through the EXISTING terminal path, not a new one.
    let history = load_history(&url, exec_id).await;
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. })),
        "escalation appends the ordinary WorkflowFailed event, got {history:?}"
    );

    // AC5 — escalated exactly once, with the fleet-exhaustion outcome. The knob
    // is named `capability_miss_max_redeliveries`, so a budget of N grants N
    // DISTINCT-worker releases and escalates on the N+1th distinct worker. This
    // worker is the fourth against a budget of 3, so it never releases at all.
    assert_eq!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_ESCALATED),
        1,
        "exactly one escalation, got {:?}",
        metrics.samples()
    );
    assert_eq!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_RELEASED),
        0,
        "the budget was already spent by three distinct peers, so the fourth \
         must escalate on its first claim rather than release again, got {:?}",
        metrics.samples()
    );

    // AC4 — never mistaken for a poison pill, even at escalation.
    assert_eq!(metrics.quarantine_count(), 0);
    // Escalation routes through the ordinary terminal-failure path
    // (`fail_task_and_execution` -> `persist_workflow_failure`), which writes
    // NO dead-letter row. So the whole DLQ must stay empty -- in particular
    // there is no `PoisonPill` quarantine (#367).
    let dlq = dead_letter_errors(&url, exec_id).await;
    assert!(
        dlq.is_empty(),
        "escalation must not dead-letter; the reason lives on the execution row, got {dlq:?}"
    );
    let tasks = load_tasks(&url, exec_id).await;
    assert!(
        tasks.iter().all(|t| t.crash_strikes == 0),
        "crash strikes must stay at zero across the whole release cycle (AC4)"
    );
}

/// **Issue #804 round-6 P1.** One incapable worker repeatedly winning the claim
/// race must never exhaust the redelivery budget.
///
/// The release backoff makes the task eligible to *every* worker again — the
/// one that just released it included — and the claim query has no capability
/// filter. Under plain total-miss accounting, `budget + 1` consecutive wins by
/// a single incapable pod terminally failed the run, which during a rolling
/// deploy is exactly the outage #804 exists to prevent: a capable peer is live
/// and idle the whole time, it just keeps losing a coin flip.
///
/// The budget is therefore consumed per DISTINCT worker. This test runs a
/// single incapable worker against a budget of 2 and lets it bounce the task
/// far past that budget: the distinct set stays at one entry, so it releases
/// every time and never escalates.
///
/// A live peer that never claims is seeded so the fleet evidence stays
/// `CapablePeerMayExist` — which is exactly the state this property is about.
/// The concern round 6 fixed is "a capable peer may be live and merely losing
/// the claim race", so the test has to be run in a fleet where that is true.
/// With the registry instead CONFIRMING that the lone worker is the whole live
/// fleet there is provably no peer to protect, and the configured budget bounds
/// total releases too (round 15) — see
/// `single_worker_fleet_escalates_at_the_configured_budget` below and the pure
/// `configured_total_bound_requires_confirmed_fleet_coverage`.
///
/// Falsifiable — verified by reverting `capability_miss_decision` to the
/// total-miss bound and re-running: with a budget of 2 the task escalates on
/// the third claim, so it never accrues a fourth miss and the row is gone.
/// `wait_for_capability_misses` times out rather than the `RUNNING` assertion
/// firing, but the cause is the same terminal failure this test exists to
/// forbid.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_incapable_worker_cannot_exhaust_the_shared_redelivery_budget() {
    // Budget 2. Backoff is 1s, 2s, 4s, 8s..., so waiting for 4 total misses
    // takes ~7s and puts the task two claims past a budget that a single
    // worker would previously have exhausted.
    const BUDGET: u32 = 2;
    const MISSES: i32 = 4;

    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);

    let exec_id = seed_workflow(
        &mut conn,
        "never_registered_wf",
        serde_json::json!({}),
        Q_ESCALATE,
    )
    .await;

    // A live worker on this queue that never claims, so the registry can never
    // conclude the fleet has been swept. Without it the lone incapable pod IS
    // the confirmed fleet, and the round-15 total bound would (correctly)
    // escalate at BUDGET + 1 -- a different property, covered separately.
    seed_live_worker(&url, "pod-peer-live-never-claims", Q_ESCALATE).await;

    let metrics = Arc::new(CapabilityMetrics::default());
    let worker = build_worker(
        "the-one-incapable-pod",
        &[Q_ESCALATE],
        build_registry(
            vec![workflow_info("some_other_wf", decoy_workflow)],
            vec![],
            Arc::clone(&metrics),
        ),
        BUDGET,
    );

    let task = with_worker_running(&worker, &pool, async {
        wait_for_capability_misses(&url, exec_id, MISSES, Duration::from_secs(90)).await
    })
    .await;

    // The distinct set is the budget's unit of account, and one worker only
    // ever contributes one entry however many times it bounces the task.
    assert_eq!(
        task.capability_miss_workers,
        vec!["the-one-incapable-pod".to_string()],
        "a repeat miss by the same worker must not grow the distinct set"
    );
    assert!(
        task.capability_misses >= MISSES,
        "the total counter still advances on every bounce (it drives the \
         backoff and the absolute ceiling), got {}",
        task.capability_misses
    );

    // The headline: the run is untouched. Under the old accounting it would
    // have been terminally failed after `BUDGET + 1` claims.
    let execution = load_execution(&url, exec_id).await;
    assert_eq!(
        execution.state, "RUNNING",
        "a single incapable worker must never terminally fail a run that a \
         capable peer could still pick up; error was {:?}",
        execution.error
    );
    assert_eq!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_ESCALATED),
        0,
        "no escalation may be recorded, got {:?}",
        metrics.samples()
    );
    assert!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_RELEASED)
            >= usize::try_from(MISSES).expect("small"),
        "every bounce releases, got {:?}",
        metrics.samples()
    );
    // AC4 stays true throughout: a capability miss is never a crash.
    assert_eq!(metrics.quarantine_count(), 0);
}

/// A dedicated queue so the *only* live worker on it is this test's. The
/// database is shared across the suite when `HARVEST_TEST_DATABASE_URL` is set,
/// and `live_workers_on_queue` filters by queue name — reusing `Q_ESCALATE`
/// would let another test's registered pod (or its deliberately-seeded
/// never-claiming peer) sit in this test's live set and flip the evidence to
/// `CapablePeerMayExist`, which is the state this test is the contrast for.
const Q_SOLO_FLEET: &str = "q804-solo-fleet";

/// **Issue #804 round-15 P1.** `capability_miss_max_redeliveries` must remain a
/// *maximum* in a fleet smaller than the budget.
///
/// The budget's unit of account is DISTINCT workers (round 6), so a fleet of
/// one can never satisfy `distinct_after > budget` however many times it
/// bounces the task. Before round 15 the only remaining bound was the absolute
/// `10x` release ceiling: with the default budget of 5 that is 51 claims and,
/// under the capped backoff, roughly 23 minutes before anyone is paged — for a
/// knob documented as five releases and ~31 seconds.
///
/// Round 15 applies the configured budget to *total* releases as well, but only
/// once the worker registry CONFIRMS every live worker on the queue has missed
/// the task (`AllLiveWorkersMissed`). That gate is what keeps round 6 intact:
/// on unconfirmed evidence the same total could have been run up by one pod
/// while a capable peer merely kept losing the claim race.
///
/// This is the exact contrast to
/// `one_incapable_worker_cannot_exhaust_the_shared_redelivery_budget`: same
/// single incapable pod, same repeated self-claim — the only difference is that
/// there a live peer is seeded so the fleet is never confirmed swept, and here
/// the lone pod IS the confirmed fleet, so the budget is free to bound it.
///
/// Falsifiable — verified by reverting the round-15 branch in
/// `capability_miss_decision`: the run stays `RUNNING` past the budget and
/// `wait_for_state(.., "FAILED", ..)` times out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_worker_fleet_escalates_at_the_configured_budget() {
    // Budget 2: releases at 1s and 2s of backoff, escalating on the third
    // claim ~3s in. The `10x` ceiling would need 21 claims and ~2 minutes, so
    // the assertion below distinguishes the two bounds by wall clock as well as
    // by wording.
    const BUDGET: u32 = 2;

    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);

    let exec_id = seed_workflow(
        &mut conn,
        "never_registered_wf",
        serde_json::json!({}),
        Q_SOLO_FLEET,
    )
    .await;

    // Deliberately NO seeded peer. The worker registers itself in
    // `harvest_workers` before its poll loop starts, so from its very first
    // miss the live set is exactly `[the-only-pod]` and the evidence is
    // `AllLiveWorkersMissed`.
    let metrics = Arc::new(CapabilityMetrics::default());
    let worker = build_worker(
        "the-only-pod",
        &[Q_SOLO_FLEET],
        build_registry(
            vec![workflow_info("some_other_wf", decoy_workflow)],
            vec![],
            Arc::clone(&metrics),
        ),
        BUDGET,
    );

    let failed = with_worker_running(&worker, &pool, async {
        wait_for_state(&url, exec_id, "FAILED", Duration::from_secs(60)).await
    })
    .await;

    let error = failed.error.unwrap_or_default();
    assert!(
        error.starts_with(NO_CAPABLE_WORKER_PREFIX),
        "escalation must carry the stable prefix, got {error:?}"
    );
    // The headline: the reason names the CONFIGURED budget, not the ceiling.
    assert!(
        error.contains("escalated after 2 capability-miss redeliveries"),
        "the reason must name the releases that actually completed, got {error:?}"
    );
    // A single-pod fleet exhausts on TOTAL releases, so the distinct count stays
    // at 1 while the release count reaches the budget. Reporting them separately
    // is what lets an operator tell this sub-case apart from a real N-worker
    // sweep at a glance (Codex round-23 P1) -- with only one number printed the
    // two are indistinguishable.
    assert!(
        error.contains("across 1 distinct worker(s)"),
        "a one-pod fleet must report its real distinct count, not the budget, \
         got {error:?}"
    );
    assert!(
        error.contains("capability_miss_max_redeliveries = 2"),
        "the configured knob must still be named, got {error:?}"
    );
    assert!(
        !error.contains("absolute release ceiling"),
        "a confirmed-fleet budget exhaustion must not inherit the ceiling's \
         wording -- that would send the operator to a knob 10x away from the \
         one that actually bounded this, got {error:?}"
    );
    assert!(
        error.contains("every worker with a live heartbeat here has now missed it"),
        "confirmed fleet coverage is what licenses applying the budget to \
         total releases, so the reason must say so, got {error:?}"
    );

    // Exactly BUDGET releases, then one escalation. `capability_misses` counts
    // the escalating claim too, so it lands at BUDGET + 1 while the released
    // count is BUDGET.
    assert_eq!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_RELEASED),
        usize::try_from(BUDGET).expect("small"),
        "a budget of {BUDGET} must grant exactly {BUDGET} releases, got {:?}",
        metrics.samples()
    );
    assert_eq!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_ESCALATED),
        1,
        "exactly one escalation, and the fleet-confirmed one (never \
         `escalated_never_offered`, which is a ticket rather than a page), got {:?}",
        metrics.samples()
    );
    assert_eq!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_ESCALATED_NEVER_OFFERED),
        0,
        "the task WAS offered to a peer {BUDGET} times, so the never-offered \
         outcome would misroute the alert, got {:?}",
        metrics.samples()
    );

    // AC4 holds at escalation too: a capability miss is never a crash.
    assert_eq!(metrics.quarantine_count(), 0);
    assert!(
        dead_letter_errors(&url, exec_id).await.is_empty(),
        "escalation routes through the ordinary terminal-failure path, which \
         writes no dead-letter row"
    );
}

// ---------------------------------------------------------------------------
// Same-id restart onto a capable build (issue #804, Codex round-26 P1)
// ---------------------------------------------------------------------------

const Q_RESTART: &str = "q804-restart";
const Q_CLAIM_RACE: &str = "q804-claim-race";

/// Dedicated queue for the round-28 lost-claim test, so stealing every RUNNING
/// row on it cannot disturb an adjacent test sharing the database.
const Q_LOST_CLAIM: &str = "q804-lost-claim";

/// Move the execution's task rows out of (or back into) claim range.
///
/// The restarted worker in these tests must REGISTER without CLAIMING: it is
/// deliberately the capable one, so a claim would complete the run and the
/// escalation path under test would never be reached. `claim_task` gates on
/// `scheduled_at <= NOW()`, so deferring the row is the precise, race-free way
/// to phase "this worker came up" ahead of "this worker polled".
async fn set_task_scheduled_at(url: &str, exec_id: ExecutionId, at: chrono::DateTime<Utc>) {
    let mut conn = connect(url).await;
    diesel::update(
        harvest_task_queue::table
            .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid()))),
    )
    .set(harvest_task_queue::scheduled_at.eq(at))
    .execute(&mut conn)
    .await
    .expect("reschedule task");
}

/// **Control for `restart_onto_a_capable_build_clears_its_stale_miss_evidence`.**
///
/// Identical fixture, with the one difference that `pod-a` never restarts: it is
/// seeded straight into `harvest_workers`, so no registration runs and no
/// evidence is invalidated. The stale entry therefore still covers a live
/// worker, `pod-b`'s miss completes the sweep, and the run is failed terminally.
///
/// This is what makes the pair meaningful. Without it the sibling test could
/// pass because the fixture never escalates at all — here the same seeded
/// evidence, the same budget, and the same incapable claimant DO escalate, so
/// the sibling's release is attributable to the re-registration and nothing
/// else.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_miss_evidence_from_a_worker_that_never_restarted_still_escalates() {
    const BUDGET: u32 = 1;

    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);

    let exec_id = seed_workflow(
        &mut conn,
        "restart_target_wf",
        serde_json::json!({}),
        Q_RESTART,
    )
    .await;

    // `pod-a` missed this frontier on a previous deploy...
    seed_prior_missing_workers(&url, exec_id, &["pod-a"], "restart_target_wf").await;
    // ...and is live, but was never restarted, so its evidence is still true.
    seed_live_worker(&url, "pod-a", Q_RESTART).await;

    let metrics = Arc::new(CapabilityMetrics::default());
    let pod_b = build_worker(
        "pod-b",
        &[Q_RESTART],
        build_registry(
            vec![workflow_info("some_other_wf", decoy_workflow)],
            vec![],
            Arc::clone(&metrics),
        ),
        BUDGET,
    );

    let failed = with_worker_running(&pod_b, &pool, async {
        wait_for_state(&url, exec_id, "FAILED", Duration::from_secs(60)).await
    })
    .await;

    let error = failed.error.unwrap_or_default();
    assert!(
        error.starts_with(NO_CAPABLE_WORKER_PREFIX),
        "the control must escalate through the capability-miss path, got {error:?}"
    );
    assert_eq!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_ESCALATED),
        1,
        "exactly one escalation, got {:?}",
        metrics.samples()
    );
}

/// **Issue #804, Codex round-26 P1.** A worker that restarts onto a build which
/// registers the missing handler must not still be counted as incapable.
///
/// `workers::register_worker` upserts on `worker_id` and refreshes that row's
/// `build_id`/`started_at`, so restarting a pod under the same configured
/// `worker_id` onto a new build is explicitly supported. `capability_miss_workers`
/// stores only the bare id, though, so before this fix the new capable instance
/// was indistinguishable from the old incapable one.
///
/// The harm lands at exactly the wrong moment. With the release budget already
/// spent, the next incapable claimant reads the live fleet, finds every id in it
/// already covered by the stale evidence, derives `AllLiveWorkersMissed`, and
/// terminally fails the execution — while the pod that can actually run it is up
/// and polling. That is the precise failure issue #804 exists to prevent, so the
/// fix has to land before the claim, not after.
///
/// Phasing is deliberate and race-free rather than timing-based: the task is
/// deferred out of claim range while `pod-a` comes up, so it registers (clearing
/// its own stale entry) without claiming, and only then is the row made eligible
/// for the incapable `pod-b`. `pod-a`'s heartbeat stays fresh across its
/// shutdown — `live_workers_on_queue_query` filters on heartbeat age alone, not
/// status — so it is still a live peer when `pod-b` weighs the evidence, which is
/// the whole point.
///
/// Falsifiable — verified by reverting the invalidation call in
/// `Worker::register_in_fleet`: `pod-a`'s entry survives, `pod-b`'s miss reads as
/// a completed fleet sweep, and the execution lands in `FAILED` with a
/// `no_capable_worker:` reason instead of the task returning to `PENDING`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_onto_a_capable_build_clears_its_stale_miss_evidence() {
    // Budget 1, already spent by the seeded miss: the very next miss escalates
    // unless the evidence is invalidated, so the two outcomes are one claim
    // apart and nothing else can explain the difference.
    const BUDGET: u32 = 1;

    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);

    let exec_id = seed_workflow(
        &mut conn,
        "restart_target_wf",
        serde_json::json!({}),
        Q_RESTART,
    )
    .await;

    // `pod-a` missed this frontier on its OLD build, exhausting the budget.
    seed_prior_missing_workers(&url, exec_id, &["pod-a"], "restart_target_wf").await;

    // Phase 1: `pod-a` restarts onto a build that DOES register the handler.
    // Deferred out of claim range so it registers without claiming — a claim
    // would simply run the workflow and never reach the decision under test.
    set_task_scheduled_at(&url, exec_id, Utc::now() + chrono::Duration::hours(1)).await;
    let restart_metrics = Arc::new(CapabilityMetrics::default());
    let pod_a = build_worker(
        "pod-a",
        &[Q_RESTART],
        build_registry(
            vec![workflow_info("restart_target_wf", decoy_workflow)],
            vec![],
            Arc::clone(&restart_metrics),
        ),
        BUDGET,
    );
    with_worker_running(&pod_a, &pool, async {
        // Registration is the first thing `run` does, so waiting for the row to
        // carry a fresh heartbeat is waiting for the invalidation to have run.
        wait_for_live_worker(&url, "pod-a", Duration::from_secs(30)).await;
    })
    .await;

    // The evidence must be gone the moment the worker registered -- not on some
    // later claim, because the whole point is that the escalation below happens
    // BEFORE this pod gets one.
    let after_restart = load_tasks(&url, exec_id).await;
    assert!(
        after_restart
            .iter()
            .all(|t| !t.capability_miss_workers.iter().any(|w| w == "pod-a")),
        "re-registration must drop this id's stale evidence, got {:?}",
        after_restart
            .iter()
            .map(|t| t.capability_miss_workers.clone())
            .collect::<Vec<_>>()
    );
    assert!(
        after_restart.iter().all(|t| t.capability_misses >= 1),
        "the historical release count must be preserved -- it backs the ungated \
         absolute ceiling that still bounds a pod crash-looping on the SAME \
         incapable build, got {:?}",
        after_restart
            .iter()
            .map(|t| t.capability_misses)
            .collect::<Vec<_>>()
    );

    // Phase 2: the task becomes eligible and the still-incapable `pod-b` wins
    // the claim. `pod-a` remains a live peer (fresh heartbeat, shut down or not).
    set_task_scheduled_at(&url, exec_id, Utc::now() - chrono::Duration::seconds(5)).await;
    let metrics = Arc::new(CapabilityMetrics::default());
    let pod_b = build_worker(
        "pod-b",
        &[Q_RESTART],
        build_registry(
            vec![workflow_info("some_other_wf", decoy_workflow)],
            vec![],
            Arc::clone(&metrics),
        ),
        BUDGET,
    );

    let observed = with_worker_running(&pod_b, &pool, async {
        wait_for_miss_by(&url, exec_id, "pod-b", Duration::from_secs(60)).await
    })
    .await;

    let task = observed.expect(
        "the execution went terminal instead of releasing -- `pod-a`'s stale \
         evidence made `pod-b`'s miss look like a completed fleet sweep",
    );

    // The headline: released for the restarted pod, not failed out from under it.
    assert_eq!(
        task.state, "PENDING",
        "the task must return to the queue for the capable pod to claim"
    );
    let execution = load_execution(&url, exec_id).await;
    assert_eq!(
        execution.state, "RUNNING",
        "a live worker on a build that registers the handler means the fleet is \
         NOT swept, so the run must survive"
    );
    assert_eq!(
        task.capability_miss_workers,
        vec!["pod-b".to_string()],
        "only the worker that actually missed on its CURRENT build may carry \
         evidence"
    );
    assert_eq!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_ESCALATED),
        0,
        "no escalation may occur while a live peer is unaccounted for, got {:?}",
        metrics.samples()
    );
    assert_eq!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_RELEASED),
        1,
        "the miss must be released, got {:?}",
        metrics.samples()
    );
    // AC4: a capability miss is never a crash, on this path either.
    assert_eq!(metrics.quarantine_count(), 0);
}

/// **Issue #804, Codex round-27 P1.** The release-vs-escalate decision must not
/// be made on a claim-time snapshot that a peer's re-registration has since
/// invalidated.
///
/// Round 26's invalidation is deliberately **not** ownership-guarded — it has to
/// reach a row another worker is mid-dispatch on, because that is precisely the
/// row whose decision stale evidence poisons. That made it the one writer that
/// can change these columns between a claim and its miss, breaking the invariant
/// `frontier_miss_state` had documented and relied on.
///
/// The consequence is that round 26 fixes nothing in its own headline scenario
/// whenever the restart lands mid-dispatch: `pod-b` claimed while the row still
/// named `pod-a`, `pod-a` restarts onto a capable build and clears its entry,
/// and `pod-b` — still deciding on its snapshot — reads `pod-a` as incapable
/// *and* the registry reports it live, derives `AllLiveWorkersMissed`, and
/// terminally fails the run. Same terminal failure, one layer down.
///
/// The interleaving here is **deterministic, not timed**: the workflow body
/// itself performs the invalidation, and a body runs strictly between
/// `claim_task` and the `AfterHandler` miss raised by
/// `persist_scheduled_activities`. That is the real window, hit exactly, with no
/// sleep and no second worker to race.
///
/// Falsifiable — verified by reverting `current_frontier_miss_state` to the bare
/// `frontier_miss_state` snapshot: the execution lands in `FAILED` with a
/// `no_capable_worker:` reason instead of the task returning to `PENDING`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peer_reregistering_mid_dispatch_is_seen_by_the_decision() {
    // Budget 1, already spent by the seeded miss: without the re-read the very
    // next miss escalates, so the two outcomes are one claim apart.
    const BUDGET: u32 = 1;

    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);

    let exec_id = seed_workflow(
        &mut conn,
        "claim_race_wf",
        serde_json::json!({}),
        Q_CLAIM_RACE,
    )
    .await;

    // `pod-a` missed this frontier on its old build, exhausting the budget, and
    // is live (it is about to restart onto a capable build, mid-dispatch).
    seed_prior_missing_workers(&url, exec_id, &["pod-a"], "claim_race_wf").await;
    seed_live_worker(&url, "pod-a", Q_CLAIM_RACE).await;

    // Arm the injection the workflow body performs while holding the claim.
    // `assert!` rather than `expect()` because `OnceLock::set` hands the
    // rejected value back in the `Err`, and a `DbPool` is not `Debug`
    // (`AsyncPgConnection` is not).
    assert!(
        RACE_INJECTION
            .set((build_pool(&url), "pod-a".to_string()))
            .is_ok(),
        "race injection is armed exactly once per process"
    );
    // Pay the TCP+auth handshake HERE, not inside the dispatch window. See
    // `RACE_INJECTION`'s docs: the body has 100 ms total.
    prewarm_race_injection().await;

    let metrics = Arc::new(CapabilityMetrics::default());
    let pod_b = build_worker(
        "pod-b",
        &[Q_CLAIM_RACE],
        build_registry(
            // `pod-b` CAN run the workflow (so the body executes and the miss is
            // `AfterHandler`), but registers no activity at all.
            vec![workflow_info(
                "claim_race_wf",
                workflow_invalidates_peer_then_misses,
            )],
            vec![],
            Arc::clone(&metrics),
        ),
        BUDGET,
    );

    let observed = with_worker_running(&pod_b, &pool, async {
        wait_for_miss_by(&url, exec_id, "pod-b", Duration::from_secs(60)).await
    })
    .await;

    let task = observed.expect(
        "the execution went terminal instead of releasing -- the decision used \
         the claim-time snapshot, which still named `pod-a` even though its \
         evidence had been invalidated during this very dispatch",
    );

    assert_eq!(
        task.state, "PENDING",
        "the task must return to the queue for the restarted, capable pod"
    );
    let execution = load_execution(&url, exec_id).await;
    assert_eq!(
        execution.state, "RUNNING",
        "a live worker whose evidence was invalidated mid-dispatch means the \
         fleet is NOT swept, so the run must survive"
    );
    assert_eq!(
        task.capability_miss_workers,
        vec!["pod-b".to_string()],
        "the release appends to the CURRENT array, so the invalidated peer must \
         not reappear"
    );
    assert_eq!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_ESCALATED),
        0,
        "no escalation may occur while a live peer is unaccounted for, got {:?}",
        metrics.samples()
    );
    assert_eq!(metrics.quarantine_count(), 0);
}

// ---------------------------------------------------------------------------
// Codex round-28 P1s: a lost claim makes no decision; registration is atomic.
// ---------------------------------------------------------------------------

/// A dispatcher that lost its claim mid-dispatch must make **no terminal
/// decision** about the task (issue #804, Codex round-28 P1).
///
/// The release path is ownership-guarded (`worker_id = $2`), but the escalation
/// path is not: `queue::fail_task` accepts any `PENDING`/`RUNNING` row
/// regardless of `worker_id`, and `fail_task_and_execution` transitions the
/// execution. So a stale dispatcher deciding `Escalate` terminally fails a task
/// another worker now owns.
///
/// Budget `0` is the sharpest possible case and the one Codex named: it
/// escalates on the very first miss, so *every* protection except the ownership
/// probe is out of the way, and the decision the fix suppresses is the only
/// thing standing between the stolen row and a terminal failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_claim_lost_mid_dispatch_makes_no_terminal_decision() {
    const BUDGET: u32 = 0;

    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);

    let exec_id = seed_workflow(
        &mut conn,
        "lost_claim_wf",
        serde_json::json!({}),
        Q_LOST_CLAIM,
    )
    .await;

    // Its own pool, not the worker's: the theft must not have to wait behind
    // the dispatch it is racing for a connection. `is_ok()` rather than
    // `expect()` because `OnceLock::set` hands the rejected value back in the
    // `Err`, and a `DbPool` is not `Debug` (`AsyncPgConnection` is not).
    assert!(
        CLAIM_THEFT_INJECTION.set(build_pool(&url)).is_ok(),
        "claim-theft injection is armed exactly once per process"
    );
    // Pay the TCP+auth handshake HERE, not inside the dispatch window. See
    // `CLAIM_THEFT_INJECTION`'s docs: the body has 100 ms total.
    prewarm_claim_theft_injection().await;

    let metrics = Arc::new(CapabilityMetrics::default());
    let worker = build_worker(
        "pod-loses-claim",
        &[Q_LOST_CLAIM],
        build_registry(
            vec![workflow_info(
                "lost_claim_wf",
                workflow_steals_own_claim_then_misses,
            )],
            vec![],
            Arc::clone(&metrics),
        ),
        BUDGET,
    );

    with_worker_running(&worker, &pool, async {
        // This test asserts that NOTHING happens, so there is no completion
        // event to wait on — a lost claim deliberately records no metric. Wait
        // instead on the precondition the body establishes (the row changing
        // hands), which is the last observable moment before the decision, then
        // allow a small fixed grace for the decision itself: the miss is raised
        // synchronously in the same dispatch, microseconds after the body
        // returns. A single long blind sleep would be both slower and less
        // reliable, since it hopes the claim, body and miss all fit inside it.
        wait_for_task_owner(&url, exec_id, "thief", Duration::from_secs(30)).await;
        tokio::time::sleep(Duration::from_millis(1_500)).await;
    })
    .await;

    let execution = load_execution(&url, exec_id).await;
    assert_eq!(
        execution.state, "RUNNING",
        "a dispatcher that no longer owns the row must not fail the run it \
         stopped being responsible for"
    );
    assert_eq!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_ESCALATED),
        0,
        "no escalation may be decided on a lost claim, got {:?}",
        metrics.samples()
    );

    // And the row itself is untouched by this dispatch: still owned by the
    // thief, never failed.
    let task = load_tasks(&url, exec_id)
        .await
        .into_iter()
        .next()
        .expect("the workflow task row survives an undecided dispatch");
    assert_eq!(
        task.state, "RUNNING",
        "the stolen row belongs to its new owner, not to this dispatch"
    );
    assert_eq!(
        task.worker_id.as_deref(),
        Some("thief"),
        "sanity: the injection really did hand the claim away"
    );
    assert_eq!(
        task.capability_miss_workers,
        Vec::<String>::new(),
        "a dispatch that owns nothing records nothing"
    );
}

/// Deterministic companion to
/// [`a_claim_lost_mid_dispatch_makes_no_terminal_decision`] (issue #1182).
///
/// That test relies on winning a real race between a slow in-body database
/// round trip and `executor::SUSPENSION_TIMEOUT`, so it exercises
/// [`handle_suspended_workflow`]'s catch-all branch -- reached when a live
/// dispatch cycle suspends having emitted **zero** commands at all -- only
/// when the round trip happens to land on the wrong side of that 100 ms
/// budget. Under a loaded CI runner it reliably does (issue #1182); under a
/// fast one it can just as reliably not, silently skipping this branch for
/// months without anyone noticing. This test drives the exact same primitive
/// the branch now delegates to,
/// [`fail_suspended_workflow_if_still_claimed`], directly and
/// unconditionally: the claim theft is committed for real *before* the call,
/// so there is no timing to win or lose.
///
/// Before issue #1182's fix, this branch called `persist_workflow_failure`
/// with no ownership check at all, so a stale dispatcher whose claim had
/// already moved to another worker would terminally fail the run regardless.
///
/// Codex review round 2: the function now surfaces this as
/// `Err(HarvestError::SuspendedClaimAmbiguous)` rather than performing a
/// release and returning `Ok(())` itself -- it must never touch the row a
/// genuine transfer already handed to a new owner, and propagating instead
/// of writing lets a real caller's enclosing transaction (which is never
/// present here) roll back cleanly first. This test therefore asserts the
/// error shape directly; the release path itself has its own dedicated test
/// below.
#[tokio::test]
async fn a_stale_dispatcher_with_zero_emitted_commands_makes_no_terminal_decision_when_the_claim_moved()
 {
    let (url, _container) = setup_db().await;

    let (exec_id, task) = seed_claimed_task(&url, "q1182-empty-commands", "dispatcher-a").await;

    // The claim moves to another worker while this dispatcher is still
    // mid-flight -- exactly the shape of a concurrent poison-pill reclaim, an
    // operator action, or (issue #1182) a slow in-body database round trip
    // racing the same row. `task` is the STALE snapshot `dispatcher-a`
    // claimed with; it has no way to know this happened.
    let mut mover = connect(&url).await;
    diesel::sql_query("UPDATE harvest_task_queue SET worker_id = 'thief' WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task.id)
        .execute(&mut mover)
        .await
        .expect("transfer the claim");

    // The live dispatch cycle suspended having emitted no commands at all --
    // the shape produced when a handler's whole `SUSPENSION_TIMEOUT` budget is
    // spent on something other than a command-emitting await point. This is
    // `handle_suspended_workflow`'s catch-all `else` branch, driven here via
    // the exact error text it builds for an empty command set.
    let error = "workflow suspended without emitted commands; resumption is not implemented yet";
    let mut conn = connect(&url).await;
    let outcome = fail_suspended_workflow_if_still_claimed(
        &mut conn,
        &task,
        exec_id,
        1, // next_event_id: irrelevant on ClaimLost, and re-derived under the guard
        "dispatcher-a",
        error,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await;

    assert_eq!(
        outcome
            .expect_err("an ambiguous claim must surface, not be swallowed to Ok(())")
            .suspended_claim_ambiguous(),
        Some(task.id),
        "the sentinel must name the exact task whose ownership is ambiguous"
    );

    // The function itself performs no write at all now -- the row and run
    // the new owner picked up are untouched by this call.
    let history = load_history(&url, exec_id).await;
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. })),
        "a dispatcher that lost the claim must append no terminal event; got {history:?}"
    );
    let execution = load_execution(&url, exec_id).await;
    assert_eq!(
        execution.state, "RUNNING",
        "a dispatcher that no longer owns the row must not fail the run it \
         stopped being responsible for"
    );
    let reloaded = load_tasks(&url, exec_id)
        .await
        .into_iter()
        .next()
        .expect("the task row survives an undecided dispatch");
    assert_eq!(
        reloaded.state, "RUNNING",
        "the stolen row belongs to its new owner, not to this dispatch"
    );
    assert_eq!(
        reloaded.worker_id.as_deref(),
        Some("thief"),
        "the claim transfer must be untouched by the stale dispatcher's write"
    );
}

/// A `ClaimLost` result is ambiguous between a genuine ownership transfer and
/// a concurrent, *unrelated* transaction merely holding the row's lock for a
/// moment -- `claim_still_held_for_update`'s `SKIP LOCKED` guard reads both
/// the same way (Codex review of #1182). This reproduces the second case
/// deterministically: a still-legitimate owner (`worker_id` and
/// `crash_strikes` both untouched) is falsely reported as having lost the
/// claim purely because another transaction (standing in for
/// `wake_workflow_task`'s `wake_requested` fallback) happens to be holding
/// the row's lock at that exact instant.
///
/// If the fix simply logged and returned on `ClaimLost` -- which is what an
/// incomplete version of this change did -- the task would strand: still
/// `RUNNING`, still claimed by `dispatcher-a`, but with this dispatcher
/// having already made its "no terminal decision" call and moved on. Nothing
/// would ever re-claim a `RUNNING` row, so it would sit forever. The
/// production fix instead attempts a blocking, ownership-guarded release,
/// which must succeed once the contending lock is released, since ownership
/// never actually moved.
///
/// Codex review round 2 split the fix into two calls:
/// [`fail_suspended_workflow_if_still_claimed`] itself must return
/// **immediately**, without blocking on anything -- it only probes via
/// `SKIP LOCKED`, so it can never be the thing waiting inside a transaction
/// that also holds another lock -- and the blocking, ownership-guarded
/// release is a **separate**, standalone call
/// ([`queue::release_suspended_workflow_claim`]) made only afterward. Codex
/// review round 3 relocated that standalone call from `process_workflow_
/// task`'s own post-transaction `match` arm out to `process_task` -- one
/// layer further out, after the issue #494 `workflow_task_timeout` budget
/// has already been unwound -- but the two-call shape this test drives is
/// unchanged by that move. This test drives both calls in that order and
/// asserts the fast
/// return is genuinely fast (bounded well under the lock-hold window) before
/// asserting the release itself blocks and then succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dispatcher_that_still_owns_the_claim_is_released_when_skip_locked_is_a_false_positive() {
    let (url, _container) = setup_db().await;

    let (exec_id, task) = seed_claimed_task(&url, "q1182-lock-contention", "dispatcher-a").await;

    // Stand in for a concurrent, unrelated transaction (e.g.
    // `wake_workflow_task`'s `wake_requested` fallback) that happens to hold
    // this exact row's lock for a moment -- without touching `worker_id` or
    // `crash_strikes`, so ownership never actually changes hands.
    let mut conn_lock = connect(&url).await;
    conn_lock
        .batch_execute("BEGIN")
        .await
        .expect("begin should succeed");
    diesel::sql_query("UPDATE harvest_task_queue SET wake_requested = TRUE WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task.id)
        .execute(&mut conn_lock)
        .await
        .expect("hold the row's lock without changing ownership");

    // The live dispatch cycle suspended having emitted no commands, exactly
    // as in the test above -- but this time the SKIP LOCKED guard will read
    // the row as contended (by `conn_lock`, above), not transferred. The
    // second call (the standalone release) blocks on the held lock rather
    // than skipping it, so this task must be spawned to run concurrently
    // with the lock hold.
    let dispatch_handle = tokio::spawn({
        let url = url.clone();
        let task = task.clone();
        async move {
            let mut conn = connect(&url).await;

            let probe_started = std::time::Instant::now();
            let ambiguous_task_id = fail_suspended_workflow_if_still_claimed(
                &mut conn,
                &task,
                exec_id,
                1,
                "dispatcher-a",
                "workflow suspended without emitted commands; resumption is not implemented yet",
                &autumn_harvest::payload_codec::PayloadCodecs::default(),
            )
            .await
            .expect_err("SKIP LOCKED never waits, so the row reads as contended immediately")
            .suspended_claim_ambiguous()
            .expect("must be the ambiguous-claim sentinel");
            let probe_elapsed = probe_started.elapsed();

            // The standalone release, exactly as `process_task` performs it
            // after `process_workflow_task`'s own transaction has already
            // ended (Codex review round 3 moved it one layer further out,
            // past the issue #494 timeout budget too -- see
            // `run_under_workflow_body_budget`'s doc comment) -- this is the
            // call that actually blocks on `conn_lock`'s held lock, never
            // the probe above.
            let released = queue::release_suspended_workflow_claim(
                &mut conn,
                ambiguous_task_id,
                "dispatcher-a",
                task.crash_strikes,
            )
            .await?;
            Ok::<_, autumn_harvest::HarvestError>((probe_elapsed, released))
        }
    });

    // Give the spawned probe time to run and reach the blocking release
    // before the lock is released.
    tokio::time::sleep(Duration::from_millis(300)).await;

    conn_lock
        .batch_execute("COMMIT")
        .await
        .expect("release the held row lock");

    let (probe_elapsed, released) = dispatch_handle
        .await
        .expect("the dispatch task must not panic")
        .expect("the guarded write and its release fallback must not error");
    assert!(
        probe_elapsed < Duration::from_millis(250),
        "the ambiguity probe must never wait on the row's lock -- it took \
         {probe_elapsed:?}, but the lock was held for 300ms before this test even \
         released it, so a probe that blocked would exceed that bound"
    );
    assert!(
        released,
        "a claim that was never actually lost must be released"
    );

    // No terminal decision was made -- the run is untouched.
    let history = load_history(&url, exec_id).await;
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. })),
        "a false-positive ClaimLost must never terminally fail the run; got {history:?}"
    );
    let execution = load_execution(&url, exec_id).await;
    assert_eq!(execution.state, "RUNNING");

    // The task was released back to the pool for a fresh dispatch attempt --
    // this is the part an incomplete fix (log-and-return on `ClaimLost`)
    // would fail: the row would still show `state = "RUNNING"`,
    // `worker_id = Some("dispatcher-a")`, forever.
    let reloaded = load_tasks(&url, exec_id)
        .await
        .into_iter()
        .next()
        .expect("the task row survives an undecided dispatch");
    assert_eq!(
        reloaded.state, "PENDING",
        "a claim that was never actually lost must be released, not stranded RUNNING"
    );
    assert_eq!(
        reloaded.worker_id, None,
        "the release must clear ownership so any capable worker can re-claim it"
    );
    assert!(
        !reloaded.wake_requested,
        "the wake that landed in the contention window must be reconciled by the \
         release, not left set with nothing left to ever consume it"
    );
}

/// A rolled-back ambiguous-claim decision discards **every** write made
/// earlier in the same enclosing transaction, not just its own (issue #1182,
/// Codex review round 2, "Finding 2").
///
/// `process_workflow_task` wraps its whole persistence flow -- including
/// bookkeeping unrelated to this dispatch's own outcome, e.g. `clear_nd_block`
/// clearing a previously-blocked execution's non-determinism marker (issue
/// #603) when this cycle happened to replay clean -- in one
/// `conn.transaction::<WorkflowPersistFlow, HarvestError, _>(async |conn| {
/// .. })` closure. `fail_suspended_workflow_if_still_claimed` is reached deep
/// inside that same closure; its doc comment states that propagating
/// `SuspendedClaimAmbiguous` via `?` "forces the *entire* enclosing
/// transaction to roll back .. discarding any bookkeeping write
/// [`handle_suspended_workflow`] made earlier in the same dispatch cycle, so
/// a rolled-back 'no decision' can never commit half of one" -- but neither
/// existing test above drives that claim through an actual `conn.
/// transaction()` closure: both call the guard directly, with nothing else in
/// scope to roll back. This test reproduces the shape byte-for-byte -- an
/// unrelated write, then the ambiguous-claim guard, both inside one
/// transaction closure whose `Err` is propagated with `?` -- and asserts the
/// unrelated write is genuinely gone afterward, not merely that the function
/// itself returns an error.
///
/// `current_details` (issue #473/#593) stands in for the earlier bookkeeping
/// write: an ordinary, freely-writable execution-row column with no relation
/// to the claim guard at all, so a survival here could only be explained by
/// the transaction failing to roll back.
#[tokio::test]
async fn an_ambiguous_claim_rolls_back_every_earlier_write_in_the_same_transaction() {
    let (url, _container) = setup_db().await;

    let (exec_id, task) = seed_claimed_task(&url, "q1182-rollback-scope", "dispatcher-a").await;

    // The claim moves to another worker while this dispatcher is still
    // mid-flight, exactly as in the sibling tests above.
    let mut mover = connect(&url).await;
    diesel::sql_query("UPDATE harvest_task_queue SET worker_id = 'thief' WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task.id)
        .execute(&mut mover)
        .await
        .expect("transfer the claim");

    let mut conn = connect(&url).await;
    let error = "workflow suspended without emitted commands; resumption is not implemented yet";

    // Reproduce `process_workflow_task`'s shape exactly: one
    // `conn.transaction(async |conn| { .. })` closure containing an unrelated
    // write followed by the ambiguous-claim guard, with the guard's `Err`
    // propagated out of the closure with `?` -- never caught or logged inside
    // it, exactly as the real code does.
    let result: Result<(), autumn_harvest::HarvestError> =
        Box::pin(conn.transaction(async |conn| {
            diesel::sql_query(
                "UPDATE harvest_workflow_executions SET current_details = $1 WHERE id = $2",
            )
            .bind::<diesel::sql_types::Text, _>("about to be rolled back")
            .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
            .execute(conn)
            .await
            .map_err(autumn_harvest::error::database_error)?;

            fail_suspended_workflow_if_still_claimed(
                conn,
                &task,
                exec_id,
                1,
                "dispatcher-a",
                error,
                &autumn_harvest::payload_codec::PayloadCodecs::default(),
            )
            .await?;

            Ok(())
        }))
        .await;

    assert_eq!(
        result
            .expect_err("the ambiguous-claim guard must still surface")
            .suspended_claim_ambiguous(),
        Some(task.id),
        "the propagated error must be the exact sentinel the guard produced"
    );

    // The unrelated write made earlier in the SAME transaction closure must
    // not have survived the rollback the propagated `?` triggered.
    let execution = load_execution(&url, exec_id).await;
    assert_eq!(
        execution.current_details, None,
        "an earlier write in the same enclosing transaction as an ambiguous \
         claim decision must be rolled back with it, not committed alone -- \
         got {:?}",
        execution.current_details
    );

    // The claim transfer itself, made on an entirely separate connection
    // before the transaction ever opened, is of course untouched.
    let reloaded = load_tasks(&url, exec_id)
        .await
        .into_iter()
        .next()
        .expect("the task row survives an undecided dispatch");
    assert_eq!(reloaded.worker_id.as_deref(), Some("thief"));
}

/// A concurrent task-row-first actor (standing in for
/// [`crate::poison_pill::quarantine_orphan`], which locks the task row and
/// then goes on to touch the execution row) never deadlocks against this
/// dispatch's claim-release path, because this dispatch never holds the
/// execution row while it waits on the task row (issue #1182, Codex review
/// round 2, "Finding 1").
///
/// The doc comments on [`fail_suspended_workflow_if_still_claimed`] and
/// [`queue::release_suspended_workflow_claim`] both describe the cycle a
/// broken implementation would create: `process_workflow_task`'s persistence
/// transaction holds the **execution** row (via `check_paused_and_park`'s
/// `FOR UPDATE`) for its whole duration, so if the blocking
/// [`queue::release_suspended_workflow_claim`] were ever called from *inside*
/// that transaction, this dispatch would hold the execution row while
/// blocking on the **task** row -- and a peer that locks the task row first
/// (poison-pill's own order) and then needs the execution row would be
/// blocked on this dispatch in turn. Two connections each holding one lock
/// and waiting on the other is a textbook ABBA deadlock, and Postgres would
/// eventually abort one side with a `deadlock detected` error rather than let
/// either progress.
///
/// The production fix's two-call split makes that cycle structurally
/// impossible: `fail_suspended_workflow_if_still_claimed`'s own probe is
/// `SKIP LOCKED` (never blocks, so it never becomes half of a cycle), and the
/// blocking release only runs *after* the whole enclosing transaction --
/// execution-row lock included -- has already ended. This test drives both
/// halves of the would-be cycle concurrently and for real: connection A locks
/// the task row **then** the execution row (poison-pill's own order), while
/// the dispatch's blocking release waits on connection A's task-row lock
/// while holding nothing at all. If the fix regressed and the release call
/// were ever made from inside a transaction still holding the execution row,
/// this reproduces the exact interleaving that would deadlock; bounding the
/// whole thing in [`tokio::time::timeout`] turns a would-be hang (or a
/// silent, slow `deadlock detected` abort several seconds later) into an
/// immediate, loud test failure instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_task_row_first_peer_touching_the_execution_row_never_deadlocks_the_release() {
    let (url, _container) = setup_db().await;

    let (exec_id, task) = seed_claimed_task(&url, "q1182-abba", "dispatcher-a").await;

    // Connection A: acquire the locks in poison-pill's own order -- task row
    // first, execution row second -- and hold both for a window long enough
    // for the dispatch below to reach its blocking release call and start
    // waiting on the task row.
    let mut conn_a = connect(&url).await;
    conn_a
        .batch_execute("BEGIN")
        .await
        .expect("begin should succeed");
    diesel::sql_query("SELECT id FROM harvest_task_queue WHERE id = $1 FOR UPDATE")
        .bind::<diesel::sql_types::Uuid, _>(task.id)
        .execute(&mut conn_a)
        .await
        .expect("lock the task row first, mirroring quarantine_orphan's own order");

    // The live dispatch cycle suspended having emitted no commands, and the
    // task row now reads as contended by connection A rather than
    // transferred -- the same false-positive `ClaimLost` shape as the
    // sibling lock-contention test above, so the guard's own probe never
    // blocks.
    let dispatch_handle = tokio::spawn({
        let url = url.clone();
        let task = task.clone();
        async move {
            let mut conn = connect(&url).await;

            let ambiguous_task_id = fail_suspended_workflow_if_still_claimed(
                &mut conn,
                &task,
                exec_id,
                1,
                "dispatcher-a",
                "workflow suspended without emitted commands; resumption is not implemented yet",
                &autumn_harvest::payload_codec::PayloadCodecs::default(),
            )
            .await
            .expect_err("SKIP LOCKED never waits, so the row reads as contended immediately")
            .suspended_claim_ambiguous()
            .expect("must be the ambiguous-claim sentinel");

            // The standalone, blocking release -- reached only after
            // `process_workflow_task`'s whole enclosing transaction (execution
            // row included) has already ended, so this dispatch holds no lock
            // at all while it blocks here.
            queue::release_suspended_workflow_claim(
                &mut conn,
                ambiguous_task_id,
                "dispatcher-a",
                task.crash_strikes,
            )
            .await
        }
    });

    // Give the release time to reach and start blocking on the task row
    // before connection A goes on to also lock the execution row -- if the
    // fix regressed and the dispatch were somehow holding the execution row
    // at this point, this is exactly where the second half of the cycle
    // would close.
    tokio::time::sleep(Duration::from_millis(200)).await;

    diesel::sql_query("SELECT id FROM harvest_workflow_executions WHERE id = $1 FOR UPDATE")
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .execute(&mut conn_a)
        .await
        .expect(
            "connection A's own execution-row lock must be acquired uncontended -- the \
             dispatch never holds it, so there is nothing on this side of a cycle to wait on",
        );

    tokio::time::sleep(Duration::from_millis(100)).await;

    conn_a
        .batch_execute("COMMIT")
        .await
        .expect("release both of connection A's locks");

    // Bounded well above the ~300ms this scenario needs when nothing
    // deadlocks, and well below Postgres's own multi-second deadlock-timeout
    // retry/abort cycle -- a regression here fails loudly and immediately
    // instead of hanging the test suite or surfacing as a slow, confusing
    // `deadlock detected` database error several seconds later.
    let released = tokio::time::timeout(Duration::from_secs(5), dispatch_handle)
        .await
        .expect("the release must not deadlock against connection A")
        .expect("the dispatch task must not panic")
        .expect("the release itself must not error");

    assert!(
        released,
        "the claim was never actually lost (only contended), so the release must succeed \
         once connection A commits"
    );

    let history = load_history(&url, exec_id).await;
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. })),
        "no terminal decision was made on either side of this race; got {history:?}"
    );
    let execution = load_execution(&url, exec_id).await;
    assert_eq!(execution.state, "RUNNING");

    let reloaded = load_tasks(&url, exec_id)
        .await
        .into_iter()
        .next()
        .expect("the task row survives an undecided dispatch");
    assert_eq!(
        reloaded.state, "PENDING",
        "released back to the queue, never stranded RUNNING under either connection"
    );
}

/// Registration and capability-miss cleanup are one commit (issue #804, Codex
/// round-28 P1).
///
/// Publishing a worker as live while its stale evidence survives makes it
/// affirmative fleet evidence *against itself*: another claimant reads the whole
/// live fleet as already covered and terminally fails a run at exactly the
/// moment the capable build arrived. So a failed cleanup must take the
/// registration with it.
///
/// The failure is injected by renaming `harvest_task_queue` out from under the
/// invalidation on a separate connection — deterministic, and reversible within
/// the test. The DB suite runs `--test-threads=1`, so no sibling can observe it.
#[tokio::test]
async fn a_failed_evidence_cleanup_rolls_back_the_registration() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;

    let registration = autumn_harvest::workers::WorkerRegistration {
        worker_id: "pod-atomic".to_string(),
        queues: vec![Q_LOST_CLAIM.to_string()],
        shard_assignments: vec![0],
        max_concurrency: 1,
        host: "test-host".to_string(),
        version: None,
        build_id: String::new(),
        deployment_name: None,
        labels: std::collections::HashMap::new(),
        max_concurrent_sessions: 0,
    };

    // Control: with the table present, both writes land.
    let cleared = autumn_harvest::workers::register_worker_and_clear_stale_miss_evidence(
        &mut conn,
        &registration,
        &[],
    )
    .await
    .expect("the happy path commits both writes");
    assert_eq!(cleared, 0, "no task carries this id's evidence yet");
    assert_eq!(
        live_worker_count(&url, "pod-atomic").await,
        1,
        "control: a successful pair publishes the worker"
    );

    // Now remove the row so the rollback has something to prove, and break the
    // invalidation's target table.
    let mut side = connect(&url).await;
    diesel::sql_query("DELETE FROM harvest_workers WHERE worker_id = 'pod-atomic'")
        .execute(&mut side)
        .await
        .expect("clears the control row");
    diesel::sql_query("ALTER TABLE harvest_task_queue RENAME TO harvest_task_queue_hidden")
        .execute(&mut side)
        .await
        .expect("hides the invalidation's target");

    let failed = autumn_harvest::workers::register_worker_and_clear_stale_miss_evidence(
        &mut conn,
        &registration,
        &[],
    )
    .await;

    diesel::sql_query("ALTER TABLE harvest_task_queue_hidden RENAME TO harvest_task_queue")
        .execute(&mut side)
        .await
        .expect("restores the table for any later test");

    assert!(
        failed.is_err(),
        "the pair must surface the cleanup failure rather than swallowing it"
    );
    assert_eq!(
        live_worker_count(&url, "pod-atomic").await,
        0,
        "a worker published while its stale evidence survives is affirmative \
         fleet evidence against itself, so the registration must roll back with \
         the cleanup rather than being left committed"
    );
}

/// Dedicated queue for the round-49 reused-`worker_id` test, so seeding a row
/// that advertises it cannot perturb an adjacent test's fleet evidence.
const Q_REUSED_ID: &str = "q804-reused-id";
/// The pod name reused across the two instances — a *configured stable* id,
/// which is what makes the old row survive at all.
const REUSED_WORKER_ID: &str = "pod-reused";
/// What the surviving row advertises, and what the new instance is running.
const OLD_BUILD: &str = "build-old-804r49";
const NEW_BUILD: &str = "build-new-804r49";

/// Drive one real heartbeat tick with everything but the pending flag fixed.
async fn tick_once(
    conn: &mut AsyncPgConnection,
    registration: &autumn_harvest::workers::WorkerRegistration,
    registration_pending: &std::sync::atomic::AtomicBool,
) {
    autumn_harvest::workers::do_heartbeat_tick(
        conn,
        registration,
        0,
        &serde_json::json!({}),
        &tokio_util::sync::CancellationToken::new(),
        &Mutex::new(None),
        &Mutex::new(None),
        0,
        registration_pending,
        &[],
    )
    .await;
}

/// Does the task still carry this worker's capability-miss evidence?
async fn evidence_names_worker(url: &str, exec_id: ExecutionId, worker_id: &str) -> bool {
    load_tasks(url, exec_id).await[0]
        .capability_miss_workers
        .iter()
        .any(|w| w == worker_id)
}

/// A failed startup registration is retried on the next heartbeat even when a
/// PREVIOUS row for the same `worker_id` survived (issue #804, Codex round-49
/// P1).
///
/// [`autumn_harvest::workers::do_heartbeat_tick`] already heals an **absent**
/// row through its `Ok(0)` re-registration arm. But a reused `worker_id` — a
/// *configured stable* id such as a pod name or hostname, not the random UUID
/// default — leaves the OLD row alive when the atomic register+invalidate pair
/// rolls back. `heartbeat_worker` then updates that surviving row, returns
/// `Ok(1)`, and the `Ok(0)` arm is never reached.
///
/// The harm is the exact false terminal failure issue #804 exists to prevent:
/// the worker polls indefinitely while the registry advertises the OLD build's
/// `build_id`/queues *and* its id stays in `capability_miss_workers` as
/// affirmative fleet evidence against itself, so a peer reads the live fleet as
/// [`autumn_harvest::worker::FleetCapabilityEvidence::AllLiveWorkersMissed`] and
/// terminally fails a task this worker can run.
///
/// This drives the **real** tick rather than a reimplementation of it, because
/// the defect is in that function's arm selection — recreating the arms would
/// prove nothing. The control leg runs the identical tick with the flag CLEAR
/// (exactly the pre-fix behaviour) and asserts the stale state survives, so the
/// test fails against the unfixed function rather than passing vacuously.
#[tokio::test]
async fn a_surviving_old_row_still_retries_the_failed_startup_registration() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;

    let worker_id = REUSED_WORKER_ID;

    // The row that survived the rolled-back registration: same pod name, same
    // queue, the PREVIOUS build.
    diesel::sql_query(
        "INSERT INTO harvest_workers \
         (worker_id, started_at, last_heartbeat_at, queues, shard_assignments, \
          max_concurrency, in_flight_count, host, status, build_id, labels) \
         VALUES ($1, NOW(), NOW(), $2::jsonb, '[]'::jsonb, 4, 0, 'test-host', 'Active', $3, '{}'::jsonb) \
         ON CONFLICT (worker_id) DO UPDATE SET build_id = EXCLUDED.build_id",
    )
    .bind::<diesel::sql_types::Text, _>(worker_id)
    .bind::<diesel::sql_types::Text, _>(format!("[\"{Q_REUSED_ID}\"]"))
    .bind::<diesel::sql_types::Text, _>(OLD_BUILD)
    .execute(&mut conn)
    .await
    .expect("seed the surviving old worker row");

    // A task on that queue carrying this id's stale capability-miss evidence.
    let exec_id = seed_execution(&mut conn, "reused_id_wf", serde_json::json!({})).await;
    let mut params = EnqueueParams::new(Q_REUSED_ID, TaskType::Workflow, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id.as_uuid());
    queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue the evidence-carrying task");
    seed_prior_missing_workers(&url, exec_id, &[worker_id], "reused_id_wf").await;

    let registration = autumn_harvest::workers::WorkerRegistration {
        worker_id: worker_id.to_string(),
        queues: vec![Q_REUSED_ID.to_string()],
        shard_assignments: vec![0],
        max_concurrency: 1,
        host: "test-host".to_string(),
        version: None,
        build_id: NEW_BUILD.to_string(),
        deployment_name: None,
        labels: HashMap::new(),
        max_concurrent_sessions: 0,
    };

    // --- Control: flag CLEAR is the pre-fix arm selection. The surviving row
    //     makes `heartbeat_worker` return `Ok(1)`, so nothing is re-registered.
    let not_pending = AtomicBool::new(false);
    tick_once(&mut conn, &registration, &not_pending).await;

    assert_eq!(
        live_build_id(&url, worker_id).await,
        OLD_BUILD,
        "control: a plain heartbeat over a surviving row must not re-register, \
         which is precisely why the `Ok(0)` arm alone cannot heal a reused id"
    );
    assert!(
        evidence_names_worker(&url, exec_id, worker_id).await,
        "control: without the retry the stale evidence survives, so this id is \
         still affirmative fleet evidence against itself"
    );

    // --- Fix: the flag records that startup's atomic pair rolled back, so the
    //     tick retries it before the heartbeat can mask the absence.
    let pending = AtomicBool::new(true);
    tick_once(&mut conn, &registration, &pending).await;

    assert_eq!(
        live_build_id(&url, worker_id).await,
        NEW_BUILD,
        "the retried registration must publish the build this worker is actually \
         running, not the one whose row happened to survive"
    );
    assert!(
        !evidence_names_worker(&url, exec_id, worker_id).await,
        "the retry must carry its evidence invalidation with it -- publishing the \
         new build while the stale miss survives is the `AllLiveWorkersMissed` \
         false terminal this pair exists to prevent"
    );
    // Fully qualified: `diesel::prelude::*` puts a blanket `RunQueryDsl::load`
    // in scope, which shadows the inherent atomic `load` by method resolution.
    assert!(
        !AtomicBool::load(&pending, Ordering::Relaxed),
        "a successful retry must clear the flag so later ticks are unchanged"
    );
}

/// A retry that fails again must withdraw the unverified row (issue #804,
/// Codex rounds 50 and 51 P1).
///
/// With a reused `worker_id` the previous row is still there, so falling
/// through to `heartbeat_worker` would *succeed* — republishing the old build's
/// `build_id`/queues as live while this id's stale capability-miss evidence is
/// still uncleared. That is affirmative false fleet evidence, and it is exactly
/// what produces `AllLiveWorkersMissed` and a terminal failure of a task this
/// worker can run.
///
/// Not heartbeating is necessary but not sufficient (round 51): the
/// capability-miss fleet window is floored at 120 s, so for that whole window
/// the surviving row is still read as a live worker that has already missed.
/// The tick therefore *withdraws* the row outright, which is the truthful
/// state while the registration is unverified — an absent worker is simply not
/// part of the fleet read.
///
/// The failure is injected the same way round 28's atomicity test does —
/// renaming `harvest_task_queue` out from under the invalidation on a separate
/// connection, deterministic and reversible within the test. The DB suite runs
/// `--test-threads=1`, so no sibling can observe it.
#[tokio::test]
async fn a_failed_retry_does_not_refresh_the_unverified_row() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;

    let worker_id = "pod-unverified";
    diesel::sql_query(
        "INSERT INTO harvest_workers \
         (worker_id, started_at, last_heartbeat_at, queues, shard_assignments, \
          max_concurrency, in_flight_count, host, status, build_id, labels) \
         VALUES ($1, NOW(), NOW() - INTERVAL '90 seconds', $2::jsonb, '[]'::jsonb, 4, 0, \
                 'test-host', 'Active', $3, '{}'::jsonb)",
    )
    .bind::<diesel::sql_types::Text, _>(worker_id)
    .bind::<diesel::sql_types::Text, _>(format!("[\"{Q_REUSED_ID}\"]"))
    .bind::<diesel::sql_types::Text, _>(OLD_BUILD)
    .execute(&mut conn)
    .await
    .expect("seed the surviving old worker row");

    assert_eq!(
        live_worker_count(&url, worker_id).await,
        1,
        "precondition: the previous instance's row survives the rolled-back pair"
    );

    let registration = autumn_harvest::workers::WorkerRegistration {
        worker_id: worker_id.to_string(),
        queues: vec![Q_REUSED_ID.to_string()],
        shard_assignments: vec![0],
        max_concurrency: 1,
        host: "test-host".to_string(),
        version: None,
        build_id: NEW_BUILD.to_string(),
        deployment_name: None,
        labels: HashMap::new(),
        max_concurrent_sessions: 0,
    };

    // Break the invalidation's target table so the retry fails.
    let mut side = connect(&url).await;
    diesel::sql_query("ALTER TABLE harvest_task_queue RENAME TO harvest_task_queue_hidden")
        .execute(&mut side)
        .await
        .expect("hides the invalidation's target");

    let pending = AtomicBool::new(true);
    tick_once(&mut conn, &registration, &pending).await;

    diesel::sql_query("ALTER TABLE harvest_task_queue_hidden RENAME TO harvest_task_queue")
        .execute(&mut side)
        .await
        .expect("restores the table for any later test");

    assert_eq!(
        live_worker_count(&url, worker_id).await,
        0,
        "a failed retry must WITHDRAW the unverified row: leaving it live for the \
         120s fleet window keeps it readable as a worker that already missed, which \
         is what lets a peer derive AllLiveWorkersMissed"
    );
    // This also proves the heartbeat never ran: `heartbeat_worker` refreshes the
    // surviving row, it never removes one.
    assert!(
        AtomicBool::load(&pending, Ordering::Relaxed),
        "the flag must stay armed so the next tick retries rather than giving up"
    );
}

/// The `build_id` `harvest_workers` currently advertises for `worker_id`.
async fn live_build_id(url: &str, worker_id: &str) -> String {
    let mut conn = connect(url).await;
    harvest_workers::table
        .find(worker_id)
        .select(harvest_workers::build_id)
        .first(&mut conn)
        .await
        .expect("read advertised build_id")
}

// ---------------------------------------------------------------------------
// Session hard-pin carve-out (#606) — release is impossible, so escalate now.
// ---------------------------------------------------------------------------

/// A session-pinned activity row can only ever be claimed by its acquiring
/// host, so releasing it "for a capable peer" would strand it forever. Such a
/// task escalates on the FIRST miss regardless of the budget.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_pinned_capability_miss_escalates_immediately() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);

    let exec_id = seed_execution(&mut conn, "session_host_wf", serde_json::json!({})).await;

    // Hard-pin an activity task to the (incapable) worker, exactly as the
    // session dispatch path does.
    let worker_id = "worker-session-host";
    let mut params = EnqueueParams::new(Q_SESSION, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.activity_name = Some("session_only_activity".to_string());
    params.activity_id = Some(Uuid::new_v4());
    params.session_id = Some(Uuid::new_v4());
    params.sticky_worker_id = Some(worker_id.to_string());
    params.sticky_timeout = Some(Duration::from_secs(300));
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);
    queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue session-pinned activity task");

    let metrics = Arc::new(CapabilityMetrics::default());
    let worker = build_worker(
        worker_id,
        &[Q_SESSION],
        build_registry(
            vec![],
            vec![activity_info(
                "some_other_activity",
                decoy_activity,
                Q_SESSION,
            )],
            Arc::clone(&metrics),
        ),
        // A generous budget that must be IGNORED: the pin makes release unsound.
        50,
    );

    let failed = with_worker_running(&worker, &pool, async {
        wait_for_state(&url, exec_id, "FAILED", Duration::from_secs(30)).await
    })
    .await;

    let error = failed.error.unwrap_or_default();
    assert!(error.starts_with(NO_CAPABLE_WORKER_PREFIX), "got {error:?}");

    // The reason must describe THIS branch, not the budget-exhausted one. The
    // budget above is 50 and zero releases happened, so the shared wording would
    // have claimed "after 50 capability-miss redeliveries" — a release count
    // that never occurred — and asserted a fleet-wide conclusion that may be
    // false, since a capable peer can exist and simply be ineligible for a
    // pinned row. An operator following the runbook would then check
    // `reachability`, get `in_use`, and be stranded.
    assert!(
        !error.contains("after 50"),
        "the pinned task was released ZERO times; the reason must not name the \
         configured budget: {error}"
    );
    assert!(
        !error.contains("no live worker on this queue has the handler"),
        "a capable peer may exist and merely be ineligible for a pinned row: {error}"
    );
    assert!(
        error.contains(SESSION_PINNED_ESCALATION_MARKER),
        "the pin is the actual cause and must be named: {error}"
    );

    assert_eq!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_ESCALATED_NEVER_OFFERED),
        1,
        "the pinned task escalates on the first miss"
    );
    assert_eq!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_RELEASED),
        0,
        "a session-pinned task must never be released: no peer can claim it"
    );

    // The metric must agree with the reason string, or the page and the error
    // column tell an operator two different stories. `harvest_no_capable_worker`
    // is severity `page` and selects `outcome="escalated"` exactly; its whole
    // narrative is "no live worker on this queue registers the handler", which
    // the reason above explicitly refuses to claim for a pinned task. Recording
    // the paging value here would page on-call and then send them to
    // `GET /admin/workflow-types/reachability`, which reports `in_use` and
    // contradicts the page they are holding.
    assert_eq!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_ESCALATED),
        0,
        "a session-pinned escalation must NOT record the fleet-exhaustion value \
         that pages harvest_no_capable_worker: the task was never offered to a \
         peer, so it is not evidence the fleet lacks the handler"
    );
}

// ---------------------------------------------------------------------------
// Success metric — zero spurious FAILED while a capable worker is live.
// ---------------------------------------------------------------------------

/// Issue #804's success metric, measured: with a mixed fleet (one incapable
/// worker, one capable), **100%** of executions reach a normal terminal outcome
/// and **zero** are spuriously failed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_fleet_completes_every_execution_with_zero_spurious_failures() {
    const RUNS: usize = 8;

    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);

    let mut exec_ids = Vec::with_capacity(RUNS);
    for _ in 0..RUNS {
        exec_ids.push(
            seed_workflow(
                &mut conn,
                "peer_only_wf",
                serde_json::json!({"n": 1}),
                Q_MIXED,
            )
            .await,
        );
    }

    let old_metrics = Arc::new(CapabilityMetrics::default());
    let old_build = build_worker(
        "mixed-old-build",
        &[Q_MIXED],
        build_registry(
            vec![workflow_info("some_other_wf", decoy_workflow)],
            vec![],
            Arc::clone(&old_metrics),
        ),
        // Generous budget, matching the AC1/AC2 convention in this file. Both
        // workers poll concurrently here (the realistic mid-deploy fleet), so
        // the claim race is genuinely stochastic: at the shipped default of 5
        // the incapable worker can win six consecutive races for one task and
        // escalate it, making this test flaky at a few percent per run. This
        // test measures "does EVERY execution reach a normal terminal outcome
        // while a capable worker is live" — the budget bound is measured by the
        // AC3 test, deterministically, with no capable worker at all.
        MIXED_FLEET_BUDGET,
    );
    let new_metrics = Arc::new(CapabilityMetrics::default());
    let new_build = build_worker(
        "mixed-new-build",
        &[Q_MIXED],
        build_registry(
            vec![workflow_info("peer_only_wf", peer_only_workflow)],
            vec![],
            Arc::clone(&new_metrics),
        ),
        MIXED_FLEET_BUDGET,
    );

    // Both live simultaneously — the realistic mid-deploy fleet. The inner
    // future is boxed: nesting two `with_worker_running` scopes around an
    // 8-execution wait otherwise builds a single very large stack future.
    with_worker_running(&old_build, &pool, async {
        with_worker_running(
            &new_build,
            &pool,
            Box::pin(async {
                for exec_id in &exec_ids {
                    wait_for_terminal(&url, *exec_id, MIXED_FLEET_TERMINAL_WAIT).await;
                }
            }),
        )
        .await;
    })
    .await;

    for exec_id in &exec_ids {
        let e = load_execution(&url, *exec_id).await;
        assert_eq!(
            e.state, "COMPLETED",
            "every execution must reach a normal terminal outcome while a capable \
             worker is live; {exec_id} ended {} ({:?})",
            e.state, e.error
        );
    }
    assert_eq!(
        old_metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_ESCALATED)
            + new_metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_ESCALATED),
        0,
        "no execution may escalate while a capable worker is live"
    );
}

/// A capable worker that PARKS the task must reset the capability-miss budget.
///
/// This is the counter's whole contract: it measures *consecutive* misses. A
/// workflow task row is long-lived — parked and re-pended in place for the
/// execution's entire life — and parking is the dominant suspension path
/// (activity, signal, child workflow, mutex). If parking did not reset, the
/// counter would be cumulative and a long-lived execution would accumulate one
/// miss per deploy until it terminally failed with `no_capable_worker:` while a
/// capable worker was demonstrably live — the exact outage #804 exists to
/// prevent, inverted.
///
/// Reaching a park PROVES capability: `process_workflow_task` resolves the
/// workflow handler and runs a decision cycle before any park can happen.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_capable_worker_that_parks_resets_the_capability_miss_budget() {
    let (url, _container) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;

    // A workflow that suspends on a signal wait — a `park_workflow_task` shape.
    let exec_id = seed_workflow(
        &mut conn,
        "signal_park_wf",
        serde_json::json!({"n": 1}),
        Q_PARK,
    )
    .await;

    // Misses already absorbed during an earlier deploy window.
    let task_id = load_tasks(&url, exec_id).await[0].id;
    diesel::sql_query("UPDATE harvest_task_queue SET capability_misses = 3 WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .execute(&mut conn)
        .await
        .expect("seed prior misses");
    assert_eq!(load_task(&url, task_id).await.capability_misses, 3);

    // A worker that HAS the workflow handler claims it, runs a decision cycle,
    // and parks waiting for a signal that never arrives.
    let metrics = Arc::new(CapabilityMetrics::default());
    let capable = build_worker(
        "park-capable",
        &[Q_PARK],
        build_registry(
            vec![workflow_info("signal_park_wf", workflow_waits_for_signal)],
            vec![],
            Arc::clone(&metrics),
        ),
        5,
    );

    let mut watch = connect(&url).await;
    with_worker_running(&capable, &pool, async {
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if load_task_on(&mut watch, task_id).await.capability_misses == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("parking must reset capability_misses to 0 within the window");
    })
    .await;

    let task = load_task(&url, task_id).await;
    assert_eq!(
        task.capability_misses, 0,
        "a park proves a capable worker handled this row, so the consecutive-miss \
         budget must start clean"
    );
    assert_eq!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_ESCALATED),
        0,
        "the capable worker must not escalate anything"
    );
    // The run is genuinely parked mid-flight, not terminal.
    assert_eq!(load_execution(&url, exec_id).await.state, "RUNNING");
}

/// The **workflow-task-timeout** re-pend (#494) must NOT touch the budget.
///
/// It is tempting to reset here — a timeout usually means the handler was found
/// and ran long, which would be proof of capability. That premise does not hold
/// on this path. The #494 budget is armed around the whole of `process_task`,
/// and `pool.get()` plus the full history load both sit inside it, strictly
/// *before* the registry lookup that defines a capability miss. Under pool
/// starvation or a slow shard the timeout fires with the lookup never having
/// run, so reaching here proves nothing about the claiming worker.
///
/// Resetting on that false premise is the worse of the two errors: a genuinely
/// unregistered type under load would have its consecutive-miss streak zeroed
/// indefinitely and never escalate, so the run never reaches the
/// `no_capable_worker:` terminal AC3 promises and the operator sees only the
/// ticket-severity sustained-release rule instead of the page. It also erases
/// an in-flight miss when the release UPDATE itself is what timed out.
///
/// The accepted cost runs the other way: a capable-but-slow worker's timeout
/// leaves a stale streak, so a later genuine miss can escalate before spending
/// the full budget. That direction is fail-safe — a loud, actionable terminal
/// failure rather than a silently un-escalating task.
///
/// Driven against the real `pub` entry point, so the assertion cannot drift
/// from the statement the way a SQL-shape test could.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_timed_out_workflow_task_reset_preserves_the_capability_miss_budget() {
    let (url, _container) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;

    let exec_id = seed_workflow(&mut conn, "timeout_reset_wf", serde_json::json!({}), Q_NOOP).await;
    let task_id = load_tasks(&url, exec_id).await[0].id;

    // The exact state the #494 timeout handler acts on: claimed by *this*
    // worker, with misses already absorbed during an earlier deploy window.
    diesel::sql_query(
        "UPDATE harvest_task_queue \
            SET state = 'RUNNING', worker_id = 'slow-but-capable', started_at = NOW(), \
                capability_misses = 4 \
          WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .execute(&mut conn)
    .await
    .expect("seed a claimed, previously-missed task");
    assert_eq!(load_task(&url, task_id).await.capability_misses, 4);

    autumn_harvest::worker::reset_timed_out_workflow_task(&pool, task_id, "slow-but-capable", 0)
        .await;

    let task = load_task(&url, task_id).await;
    assert_eq!(
        task.state, "PENDING",
        "the timed-out task must be re-pended for any worker to re-claim"
    );
    assert_eq!(
        task.capability_misses, 4,
        "a timeout can fire before the handler lookup ever runs (pool starvation, \
         slow shard), so it is NOT proof of capability; zeroing here would let an \
         unregistered type reset its streak indefinitely and never escalate"
    );
    assert!(
        task.worker_id.is_none(),
        "ownership must be released so any worker can re-claim: {:?}",
        task.worker_id
    );
    assert_eq!(
        load_execution(&url, exec_id).await.state,
        "RUNNING",
        "a timeout re-pend must not terminate the execution"
    );
}

/// A stale dispatcher must not release a claim that is no longer its own
/// (issue #804, Codex round-37 P1).
///
/// `poison_pill::requeue_orphan` hands an orphaned row back as `PENDING` /
/// `worker_id = NULL` with `crash_strikes + 1`, and nothing stops the SAME
/// worker from winning it again. Guarding the release on `(state, worker_id)`
/// alone matches that new claim, so the stale dispatcher would re-`PENDING` a
/// row whose replacement handler is already running -- inviting a second
/// concurrent claim of the same task, and decrementing an `attempt` belonging
/// to the new dispatch.
///
/// This is the release-path twin of
/// `a_same_worker_reclaim_after_a_requeue_is_not_failed_by_the_stale_escalation`,
/// which pins the same rule for the terminal write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_same_worker_reclaim_after_a_requeue_is_not_released_by_the_stale_dispatcher() {
    let (url, _container) = setup_db().await;

    let (_exec_id, task) = seed_claimed_task(&url, "q804-round37", "worker-a").await;
    assert_eq!(
        task.crash_strikes, 0,
        "the dispatcher's snapshot is the pre-requeue claim"
    );

    // Requeue (strike bumped), then the SAME worker re-claims and starts running.
    let mut mover = connect(&url).await;
    diesel::sql_query(
        "UPDATE harvest_task_queue \
         SET state = 'RUNNING', worker_id = 'worker-a', crash_strikes = crash_strikes + 1, \
             attempt = 3 \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task.id)
    .execute(&mut mover)
    .await
    .expect("requeue then re-claim with the same worker");

    // The stale dispatcher, still holding its pre-requeue snapshot, releases.
    let mut conn = connect(&url).await;
    let released = queue::release_task_for_capability_miss(
        &mut conn,
        task.id,
        "worker-a",
        Duration::from_secs(1),
        CapabilityMissPhase::BeforeHandler,
        task.crash_strikes,
        TEST_FRONTIER,
    )
    .await
    .expect("release query runs");

    assert!(
        released.is_none(),
        "the strike bump means this is a NEW claim -- matching worker_id alone \
         would re-PENDING a row whose replacement handler is already running"
    );

    let after = load_task(&url, task.id).await;
    assert_eq!(
        after.state, "RUNNING",
        "the live replacement claim must survive; releasing it would let a \
         second worker claim the same task concurrently"
    );
    assert_eq!(
        after.worker_id.as_deref(),
        Some("worker-a"),
        "the replacement claim's ownership must be untouched"
    );
    assert_eq!(
        after.attempt, 3,
        "the stale dispatcher must not decrement an attempt belonging to the \
         new dispatch"
    );
    assert_eq!(
        after.capability_misses, task.capability_misses,
        "a withdrawn release must not burn the redelivery budget either"
    );
}

/// The number the release logs must be the one its own `UPDATE` wrote
/// (issue #804, Codex round-36 P2).
///
/// `observe_miss_and_fleet` snapshots `capability_miss_workers` and derives
/// `distinct_after` from it. A peer restarting onto a capable build then runs
/// `invalidate_capability_miss_evidence_for_worker`, which `array_remove`s its
/// own id from exactly these `RUNNING` rows -- so by the time the release
/// `UPDATE` appends this claimant, the array it appends to is smaller than the
/// snapshot predicted.
///
/// This reproduces that interleaving in order: snapshot `{stale-a, stale-b}`
/// (so a snapshot-derived count would say 3), invalidate `stale-a`, then
/// release as `incapable`. The durable set is `{stale-b, incapable}`, and the
/// release must report **2**.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_release_reports_the_cardinality_it_actually_committed() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;

    let exec_id = seed_workflow(&mut conn, "noop_wf", serde_json::json!({}), Q_NOOP).await;
    let task_id = load_tasks(&url, exec_id).await[0].id;
    diesel::sql_query(
        "UPDATE harvest_task_queue \
         SET state = 'RUNNING', worker_id = 'incapable', started_at = NOW(), attempt = 1, \
             capability_miss_workers = ARRAY['stale-a', 'stale-b'], \
             capability_miss_handler = 'workflow:noop_wf' \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .execute(&mut conn)
    .await
    .expect("simulate a claim over prior miss evidence");

    // The caller's snapshot at this instant: two prior missers, so appending
    // this claimant would make three.
    let snapshot: Vec<String> = load_task(&url, task_id).await.capability_miss_workers;
    assert_eq!(
        snapshot.len(),
        2,
        "precondition: the snapshot a caller would read here"
    );

    // A peer restarts onto a capable build BEFORE the release commits.
    let cleared = queue::invalidate_capability_miss_evidence_for_worker(
        &mut conn,
        "stale-a",
        &[Q_NOOP.to_string()],
    )
    .await
    .expect("peer re-registration clears its own stale evidence");
    assert_eq!(
        cleared, 1,
        "precondition: the peer's evidence was invalidated"
    );

    let released = queue::release_task_for_capability_miss(
        &mut conn,
        task_id,
        "incapable",
        Duration::from_secs(1),
        CapabilityMissPhase::BeforeHandler,
        // The claim epoch this seeded row was claimed at (Codex round-37 P1).
        0,
        TEST_FRONTIER,
    )
    .await
    .expect("release query runs");

    let durable = load_task(&url, task_id).await.capability_miss_workers;
    assert_eq!(
        durable.len(),
        2,
        "the durable set is the post-invalidation array plus this claimant; \
         got {durable:?}"
    );
    assert_eq!(
        released,
        Some(2),
        "the release must report the cardinality it committed ({durable:?}), not \
         the snapshot-derived 3 -- a peer invalidated an entry in between"
    );
}

/// A release must leave the task row's `error` column **untouched** so the
/// issue #773 `/stack` surface does not fabricate a failure that never happened.
///
/// `/stack` renders any non-null `error` on a pending activity as a
/// `last_failure`, with `error_type: "Error"` and the row's raw `attempt`.
/// Because a release also restores `attempt` to `0`, writing the diagnostic
/// there would report a failure at an otherwise-unreachable `attempt: 0` for an
/// activity that never executed — violating #773 AC3 ("a never-failed pending
/// activity omits `last_failure`") and making a blameless deploy skew look like
/// an application bug on the one surface whose runbook question is "why is this
/// activity retrying?".
///
/// A real prior failure must survive the release untouched too: the next
/// attempt reads it through `ActivityContext::previous_failure()`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_never_writes_the_error_column() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;

    // (a) A fresh, never-failed row must stay never-failed.
    let exec_id = seed_workflow(&mut conn, "noop_wf", serde_json::json!({}), Q_NOOP).await;
    let task_id = load_tasks(&url, exec_id).await[0].id;
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = 'incapable', \
         started_at = NOW(), attempt = 1 WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .execute(&mut conn)
    .await
    .expect("simulate a claim");

    let released = queue::release_task_for_capability_miss(
        &mut conn,
        task_id,
        "incapable",
        Duration::from_secs(1),
        // A workflow-type lookup miss is `BeforeHandler`: the crash-strike
        // counter is preserved (Codex round-12 P1) and `attempt` is restored
        // (Codex round-17 P2).
        CapabilityMissPhase::BeforeHandler,
        // The claim epoch this seeded row was claimed at (Codex round-37 P1).
        0,
        TEST_FRONTIER,
    )
    .await
    .expect("release query runs");
    assert!(
        released.is_some(),
        "the release must report the claim was actually released"
    );

    let task = load_task(&url, task_id).await;
    assert!(
        task.error.is_none(),
        "a capability miss is not a task failure; /stack would render this as a \
         last_failure at attempt 0 for work that never ran (#773 AC3): {:?}",
        task.error
    );
    assert_eq!(
        task.attempt, 0,
        "the claim's increment is undone, which is exactly why an `error` here \
         would surface at an impossible attempt 0"
    );

    // (b) A genuine prior failure must survive untouched.
    let exec2 = seed_workflow(&mut conn, "noop_wf", serde_json::json!({ "n": 2 }), Q_NOOP).await;
    let task2 = load_tasks(&url, exec2).await[0].id;
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = 'incapable', \
         started_at = NOW(), attempt = 2, error = 'downstream 503' WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task2)
    .execute(&mut conn)
    .await
    .expect("simulate a claim over a real prior failure");

    let released2 = queue::release_task_for_capability_miss(
        &mut conn,
        task2,
        "incapable",
        Duration::from_secs(1),
        // A workflow-type lookup miss is `BeforeHandler`: the crash-strike
        // counter is preserved (Codex round-12 P1) and `attempt` is restored
        // (Codex round-17 P2).
        CapabilityMissPhase::BeforeHandler,
        // The claim epoch this seeded row was claimed at (Codex round-37 P1).
        0,
        TEST_FRONTIER,
    )
    .await
    .expect("release query runs");
    assert!(
        released2.is_some(),
        "the release must report the claim was actually released"
    );
    assert_eq!(
        load_task(&url, task2).await.error.as_deref(),
        Some("downstream 503"),
        "the real failure the next attempt branches on via previous_failure() \
         must not be clobbered by an infrastructure string"
    );
}

// ---------------------------------------------------------------------------
// Release is idempotent against a stolen claim.
// ---------------------------------------------------------------------------

/// The release UPDATE is ownership-guarded (`state = 'RUNNING' AND worker_id =
/// $2`). If a concurrent poison-pill reclaim or operator action already took
/// the row, the release matches nothing, counts nothing, and is a clean no-op.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_is_a_noop_when_the_claim_was_already_taken() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;

    let exec_id = seed_workflow(&mut conn, "noop_wf", serde_json::json!({}), Q_NOOP).await;
    let task_id = load_tasks(&url, exec_id).await[0].id;

    // Simulate a foreign owner: the row is RUNNING under someone else.
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = 'someone-else', \
         started_at = NOW() WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .execute(&mut conn)
    .await
    .expect("simulate foreign claim");

    let released = queue::release_task_for_capability_miss(
        &mut conn,
        task_id,
        "worker-we-are",
        Duration::from_secs(1),
        CapabilityMissPhase::BeforeHandler,
        // The claim epoch this seeded row was claimed at (Codex round-37 P1).
        0,
        TEST_FRONTIER,
    )
    .await
    .expect("release query runs");

    assert!(
        released.is_none(),
        "release must report no-op when the row is owned by another worker"
    );
    let task = load_task(&url, task_id).await;
    assert_eq!(
        task.state, "RUNNING",
        "the foreign owner's claim must be untouched"
    );
    assert_eq!(
        task.worker_id.as_deref(),
        Some("someone-else"),
        "ownership must not be stolen back"
    );
    assert_eq!(
        task.capability_misses, 0,
        "a no-op release must not inflate the miss counter"
    );
}

/// Register a worker row in `harvest_workers` advertising `queue` (issue #804).
///
/// The capability-miss fleet gate asks the registry which workers *could* claim
/// this task; a row inserted here is indistinguishable from a real worker that
/// has never happened to win a claim race for it.
async fn seed_live_worker(url: &str, worker_id: &str, queue: &str) {
    let mut conn = connect(url).await;
    diesel::sql_query(
        "INSERT INTO harvest_workers \
         (worker_id, started_at, last_heartbeat_at, queues, shard_assignments, \
          max_concurrency, in_flight_count, host, status, build_id, labels) \
         VALUES ($1, NOW(), NOW(), $2::jsonb, '[]'::jsonb, 4, 0, 'test-host', 'Active', '', '{}'::jsonb) \
         ON CONFLICT (worker_id) DO UPDATE SET last_heartbeat_at = NOW(), queues = EXCLUDED.queues",
    )
    .bind::<diesel::sql_types::Text, _>(worker_id)
    .bind::<diesel::sql_types::Text, _>(format!("[\"{queue}\"]"))
    .execute(&mut conn)
    .await
    .expect("seed live worker row");
}

/// Poll until `worker_id` has a row in `harvest_workers` (issue #804 round 26).
///
/// `Worker::run` registers before entering its poll loop, so this is the
/// observable edge for "this worker has come up" — and therefore for "its stale
/// capability-miss evidence has been invalidated", which happens in the same
/// registration step.
/// How many rows `harvest_workers` holds for `worker_id` (issue #804 round 28).
async fn live_worker_count(url: &str, worker_id: &str) -> i64 {
    let mut conn = connect(url).await;
    harvest_workers::table
        .filter(harvest_workers::worker_id.eq(worker_id))
        .count()
        .get_result(&mut conn)
        .await
        .expect("count worker rows")
}

async fn wait_for_live_worker(url: &str, worker_id: &str, timeout: Duration) {
    let mut conn = connect(url).await;
    tokio::time::timeout(timeout, async {
        loop {
            let found: i64 = harvest_workers::table
                .filter(harvest_workers::worker_id.eq(worker_id))
                .count()
                .get_result(&mut conn)
                .await
                .expect("count worker rows");
            if found > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("worker {worker_id} never registered within {timeout:?}"));
}

/// Codex round-8 P1 / the success metric's load-bearing half: **zero spurious
/// FAILED executions as long as >= 1 capable worker is live.**
///
/// A fixed redelivery budget has no relationship to the live fleet, so a
/// rollout with `budget + 1` incapable pods could hand `budget + 1` DISTINCT
/// worker ids to the decision and terminally fail the run — while the capable
/// pod was live and polling the whole time, and while the error column asserted
/// the exact opposite.
///
/// The contrast case is
/// `capability_miss_escalates_after_the_budget_with_no_capable_worker`: same
/// shape, no live capable peer registered, and it escalates as AC3 requires.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn budget_never_escalates_while_a_capable_worker_is_live() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);

    let exec_id = seed_workflow(
        &mut conn,
        "never_registered_wf",
        serde_json::json!({}),
        Q_LIVE_PEER,
    )
    .await;
    // Budget of 1, already spent by one distinct incapable worker: the next
    // distinct worker crosses the raw bound.
    seed_prior_missing_workers(&url, exec_id, &["pod-a-incapable"], "never_registered_wf").await;
    // ... but a capable worker is live on this queue and has never missed it.
    seed_live_worker(&url, "pod-capable-live", Q_LIVE_PEER).await;

    let metrics = Arc::new(CapabilityMetrics::default());
    let worker = build_worker(
        "worker-incapable-second",
        &[Q_LIVE_PEER],
        build_registry(
            vec![workflow_info("some_other_wf", decoy_workflow)],
            vec![],
            Arc::clone(&metrics),
        ),
        1,
    );

    with_worker_running(&worker, &pool, async {
        // Two releases PAST the point the ungated budget would have escalated.
        wait_for_capability_misses(&url, exec_id, 3, Duration::from_secs(60)).await;
    })
    .await;

    let execution = load_execution(&url, exec_id).await;
    assert_eq!(
        execution.state, "RUNNING",
        "a live capable peer must keep the task in play, got {:?} / {:?}",
        execution.state, execution.error
    );
    assert_eq!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_ESCALATED),
        0,
        "the distinct-worker budget must be withheld while a live worker has \
         never missed this task, got {:?}",
        metrics.samples()
    );
    assert_eq!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_ESCALATED_NEVER_OFFERED),
        0
    );
}

// ---------------------------------------------------------------------------
// Rate-limit token refund on release (#804 x #332, Codex round-11 P2)
// ---------------------------------------------------------------------------

const Q_RATE: &str = "q804-rate";

/// Read a rate-limit bucket's stored token count.
///
/// The stored value is only accurate as of `last_refilled_at` in general, but
/// the bucket this helper is used against is seeded with `refill_rate = 0.0`,
/// so no time-based refill can occur and the stored value *is* the effective
/// one. That is deliberate: a bucket that refills would replace the token on
/// its own and mask a missing refund entirely.
async fn stored_tokens(url: &str, key: &str) -> f64 {
    let mut conn = connect(url).await;
    harvest_rate_limit_buckets::table
        .find(key)
        .select(harvest_rate_limit_buckets::tokens)
        .first(&mut conn)
        .await
        .expect("read rate-limit bucket")
}

/// Releasing a rate-limited activity for a capable peer must give its token
/// back (issue #804, Codex round-11 P2).
///
/// `claim_task` debits a token for any rate-limited activity that is not
/// circuit-breaker-tracked, and an incapable worker's claim is no exception —
/// it is not tracked *because* the worker does not register the activity at
/// all. The activity never runs, so keeping the token charges real capacity to
/// a dispatch that did nothing. With a slow refill that is not cosmetic: the
/// capable peer waits a full refill interval it should not have to, and the
/// incapable worker (whose release backoff starts at 1 s) is positioned to take
/// each newly minted token again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn releasing_a_rate_limited_activity_refunds_its_token() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);

    let exec_id = seed_execution(&mut conn, "rate_limited_wf", serde_json::json!({})).await;

    // burst 1 / refill 0: exactly one token, ever. Any refill would mask a
    // missing refund by replacing the token on its own.
    //
    // The key is unique per run. A shared key would let a PENDING task left
    // behind by an earlier run race for the single token, so a later run could
    // fail for having lost that race rather than for the property under test.
    let key = format!("q804-rate-bucket-{exec_id}");
    diesel::sql_query(
        "INSERT INTO harvest_rate_limit_buckets (key, refill_rate, burst, tokens, last_refilled_at) \
         VALUES ($1, 0.0, 1.0, 1.0, NOW())",
    )
    .bind::<diesel::sql_types::Text, _>(key.as_str())
    .execute(&mut conn)
    .await
    .expect("seed rate-limit bucket");

    let mut params = EnqueueParams::new(Q_RATE, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.activity_name = Some("rate_limited_activity".to_string());
    params.activity_id = Some(Uuid::new_v4());
    params.rate_limit_key = Some(key.clone());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);
    queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue rate-limited activity task");

    let metrics = Arc::new(CapabilityMetrics::default());
    let worker = build_worker(
        "worker-rate-incapable",
        &[Q_RATE],
        build_registry(
            vec![],
            // Registers a DIFFERENT activity, so `rate_limited_activity` is a
            // capability miss -- and, being unregistered, is necessarily absent
            // from this worker's circuit-breaker tracked set, so its claim took
            // the debiting branch.
            vec![activity_info("some_other_activity", decoy_activity, Q_RATE)],
            Arc::clone(&metrics),
        ),
        50,
    );

    with_worker_running(&worker, &pool, async {
        wait_for_capability_misses(&url, exec_id, 1, Duration::from_secs(30)).await;
    })
    .await;

    assert!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_RELEASED) >= 1,
        "the task must have been released, got {:?}",
        metrics.samples()
    );

    // The refund is the whole point: with refill 0, a bucket that is not
    // refunded stays at 0.0 forever and the capable peer can never claim.
    let tokens = stored_tokens(&url, &key).await;
    assert!(
        tokens >= 0.99,
        "releasing an activity that never ran must refund its claim-time token, \
         leaving the bucket full; got {tokens}"
    );
}

/// A stale dispatcher's refund leaves exactly ONE outstanding debit for the
/// one live claim (issue #804, Codex round-42 P2).
///
/// The finding read the unconditional refund in `handle_capability_miss` as an
/// over-credit: a dispatcher that resumes after `reclaim_orphaned_tasks` has
/// re-pended its task would "refund the replacement claim's token", letting an
/// extra activity through. Tokens are fungible, so the property that actually
/// matters is the balance — outstanding debits must equal the activities that
/// ran or are running — and this drives the exact interleaving to measure it.
///
/// The bucket is `burst 2 / refill 0`: two tokens, ever. A refilling bucket
/// would mint the difference on its own and make every assertion below vacuous.
///
/// Both failure directions are asserted, because they sit on opposite sides of
/// the shipped behaviour:
///
///   * `2.0` would be the over-credit the finding describes — a token returned
///     that no claim ever spent.
///   * `0.0` is what gating the refund on still owning the claim would produce:
///     `requeue_orphan` re-pends the row without touching the bucket, so the
///     stale dispatcher's debit is stranded, and on `refill_rate = 0` it never
///     comes back. That is the direction that starves the capable peer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_dispatcher_refund_leaves_one_debit_for_the_live_claim() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;

    let exec_id = seed_execution(&mut conn, "stale_refund_wf", serde_json::json!({})).await;

    // Unique per run: a shared key would let a task left behind by an earlier
    // run race for these tokens, so a later run could fail for losing that race
    // rather than for the property under test.
    let key = format!("q804-stale-refund-{exec_id}");
    diesel::sql_query(
        "INSERT INTO harvest_rate_limit_buckets (key, refill_rate, burst, tokens, last_refilled_at) \
         VALUES ($1, 0.0, 2.0, 2.0, NOW())",
    )
    .bind::<diesel::sql_types::Text, _>(key.as_str())
    .execute(&mut conn)
    .await
    .expect("seed rate-limit bucket");

    let mut params = EnqueueParams::new(Q_RATE, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.activity_name = Some("stale_refund_activity".to_string());
    params.activity_id = Some(Uuid::new_v4());
    params.rate_limit_key = Some(key.clone());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);
    queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue rate-limited activity task");

    let queues = vec![Q_RATE.to_string()];

    // 1. The stale dispatcher `A` claims. `claim_task` debits: 2 -> 1.
    let stale_claim = queue::claim_task(&mut conn, &queues, "worker-stale-A", "", None, &[], &[])
        .await
        .expect("claim as A")
        .expect("a claimable task");
    assert!(
        (stored_tokens(&url, &key).await - 1.0).abs() < 0.01,
        "A's claim must debit one token"
    );

    // 2. Orphan reclaim re-pends the row. Verified against `requeue_orphan`:
    //    it rewrites state/worker_id/started_at and bumps `crash_strikes`, and
    //    touches no bucket — so A's debit is now stranded.
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'PENDING', worker_id = NULL, started_at = NULL, \
         crash_strikes = crash_strikes + 1, scheduled_at = NOW() - INTERVAL '5 seconds' \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(stale_claim.id)
    .execute(&mut conn)
    .await
    .expect("simulate orphan re-pend");

    // 3. The replacement `B` claims and (in the real fleet) runs it: 1 -> 0.
    let live_claim = queue::claim_task(&mut conn, &queues, "worker-live-B", "", None, &[], &[])
        .await
        .expect("claim as B")
        .expect("the re-pended task must be claimable");
    assert_eq!(
        live_claim.id, stale_claim.id,
        "B must have taken the same row"
    );
    let both_debited = stored_tokens(&url, &key).await;
    assert!(
        both_debited.abs() < 0.01,
        "both claims debited, so the bucket is empty before the refund; got {both_debited}"
    );

    // 4. `A` resumes and takes the capability-miss refund with its now-stale
    //    snapshot — the exact call the finding flagged.
    autumn_harvest::worker::refund_capability_miss_rate_limit_token(&mut conn, &stale_claim).await;

    let after = stored_tokens(&url, &key).await;
    assert!(
        (after - 1.0).abs() < 0.01,
        "the stale refund must leave exactly one outstanding debit for B's one live claim: \
         2.0 would be the over-credit the finding describes, and 0.0 the permanent leak that \
         gating the refund on ownership would cause; got {after}"
    );
}

// ---------------------------------------------------------------------------
// A release must not launder a poison task's crash history (issue #804 x #367).
// ---------------------------------------------------------------------------

/// A pre-handler capability miss must **preserve** `crash_strikes` (Codex
/// round-12 P1).
///
/// `crash_strikes` is issue #367's evidence that a task keeps killing the worker
/// process that runs it: `reclaim_orphaned_tasks` increments it each time the
/// row is found orphaned under a worker that stopped heartbeating, and
/// quarantines the task once it reaches `poison_pill_threshold`.
///
/// A capability miss ran nothing, so it is evidence about *neither* direction.
/// Zeroing the column on it is not merely generous — in a heterogeneous fleet it
/// is a silent defeat of the quarantine: an incapable claim landing between two
/// capable crashes resets the streak to zero every time, so the threshold is
/// never reached and the poison task goes on crashing replacement workers
/// indefinitely.
///
/// AC4 requires only that a clean miss never *increments* a crash counter, and
/// preserving satisfies that strictly more conservatively than zeroing did.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn releasing_a_pre_handler_miss_preserves_the_poison_pill_crash_strikes() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;

    let exec_id = seed_workflow(&mut conn, "noop_wf", serde_json::json!({}), Q_NOOP).await;
    let task_id = load_tasks(&url, exec_id).await[0].id;

    // The exact interleaving the finding describes: two capable workers have
    // already died on this row (so `reclaim_orphaned_tasks` banked two strikes),
    // and an incapable worker now wins the claim race.
    diesel::sql_query(
        "UPDATE harvest_task_queue \
            SET state = 'RUNNING', worker_id = 'incapable', started_at = NOW(), \
                crash_strikes = 2 \
          WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .execute(&mut conn)
    .await
    .expect("seed a claimed row that already crashed two workers");
    assert_eq!(load_task(&url, task_id).await.crash_strikes, 2);

    let released = queue::release_task_for_capability_miss(
        &mut conn,
        task_id,
        "incapable",
        Duration::from_secs(1),
        CapabilityMissPhase::BeforeHandler,
        // The claim epoch this seeded row was claimed at (Codex round-37 P1).
        2,
        TEST_FRONTIER,
    )
    .await
    .expect("release query runs");
    assert!(
        released.is_some(),
        "the release must report the claim was actually released"
    );

    let task = load_task(&url, task_id).await;
    assert_eq!(
        task.crash_strikes, 2,
        "the incapable worker ran nothing, so it must not erase the two worker \
         deaths this task has already caused; zeroing here means the next \
         capable worker's crash restarts the streak at 1 and \
         `poison_pill_threshold` is never reached"
    );
    assert_eq!(
        task.state, "PENDING",
        "the release itself must still work exactly as before"
    );
    assert_eq!(
        task.capability_misses, 1,
        "the redelivery counter is the ONLY counter a capability miss advances"
    );

    // The complement: a post-handler miss DID watch the body run to a
    // conclusion without killing this worker, so the streak is genuinely broken.
    let exec2 = seed_workflow(&mut conn, "noop_wf", serde_json::json!({ "n": 2 }), Q_NOOP).await;
    let task2 = load_tasks(&url, exec2).await[0].id;
    diesel::sql_query(
        "UPDATE harvest_task_queue \
            SET state = 'RUNNING', worker_id = 'incapable', started_at = NOW(), \
                crash_strikes = 2, attempt = 1 \
          WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task2)
    .execute(&mut conn)
    .await
    .expect("seed a second claimed row");

    let released2 = queue::release_task_for_capability_miss(
        &mut conn,
        task2,
        "incapable",
        Duration::from_secs(1),
        CapabilityMissPhase::AfterHandler,
        // The claim epoch this seeded row was claimed at (Codex round-37 P1).
        2,
        TEST_FRONTIER,
    )
    .await
    .expect("release query runs");
    assert!(
        released2.is_some(),
        "the release must report the claim was actually released"
    );
    let after = load_task(&url, task2).await;
    assert_eq!(
        after.crash_strikes, 0,
        "the body returned its commands on this very dispatch, so this worker \
         demonstrably survived running it"
    );
    assert_eq!(
        after.attempt, 1,
        "the SAME phase that clears the crash strikes must PRESERVE `attempt`: \
         the body ran, so `record_workflow_started` already fired and rolling \
         back to 0 would let the next capable claim re-emit it (round-17 P2)"
    );
}

// ---------------------------------------------------------------------------
// Codex round-25 P1 — the issue #494 budget bounds the decision cycle, and
// nothing after it.
// ---------------------------------------------------------------------------

/// A workflow with nothing to do: the decision cycle is the DB work around it
/// (load the execution, load history, run, persist the terminal event), which
/// is several round-trips and therefore comfortably longer than the 1ms budget
/// the guard below hands it.
fn trivial_workflow(_ctx: &WorkflowContext, _input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move { Ok(serde_json::json!("done")) })
}

/// Moving the issue #494 budget out of the dispatch site and into
/// `process_task` must not weaken issue #494 itself.
///
/// The budget used to be a `tokio::time::timeout` wrapped around the whole of
/// `process_task`, which meant it also bounded the issue #804 capability-miss
/// release/escalation that runs *after* the decision cycle — so a workflow that
/// returned near its deadline and only then tripped the post-handler preflight
/// could have that blameless cleanup cancelled, bank a timeout strike it never
/// earned, and at `poison_pill_threshold` be terminally failed (Codex round-25
/// P1). It now wraps the decision cycle alone, inside `process_task`.
///
/// The scoping rule itself is pinned without a database by
/// `worker::tests::cleanup_sequenced_after_the_budget_is_not_cancelled_by_it`,
/// whose control shows the old spanning shape cancelling the cleanup. What is
/// only observable end to end — and is the regression risk of *moving* a
/// timeout — is that an overrunning cycle is still cut, still counted as an
/// issue #494 timeout, and still quarantined. Every handler here is registered,
/// so this is the pure hang path with no capability miss anywhere in it: AC4
/// says the two must stay tellable apart, and the assertions below check both
/// halves of that.
///
/// The budget is 1ms rather than something scaled off a slow handler because
/// `effective_workflow_task_timeout` floors it at
/// `max_local_activity_start_to_close` — a local activity runs inline inside
/// the cycle, so the budget may never sit below the local cap. Both are set to
/// 1ms here; no local activity is involved.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_overrunning_decision_cycle_is_still_cut_by_the_workflow_task_budget() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);

    let exec_id = seed_workflow(
        &mut conn,
        "body_budget_wf",
        serde_json::json!({"n": 25}),
        Q_BODY_BUDGET,
    )
    .await;

    let metrics = Arc::new(CapabilityMetrics::default());
    let registry = build_registry(
        vec![workflow_info("body_budget_wf", trivial_workflow)],
        vec![],
        Arc::clone(&metrics),
    );
    // Quarantine on the first strike so the terminal outcome is reached in one
    // pass rather than three.
    let worker = build_worker_tuned(
        "w-body-budget",
        &[Q_BODY_BUDGET],
        registry,
        5,
        Duration::from_millis(1),
        Duration::from_millis(1),
        1,
    );

    let execution = with_worker_running(&worker, &pool, async {
        wait_for_terminal(&url, exec_id, Duration::from_secs(60)).await
    })
    .await;

    assert!(
        metrics.task_timeout_count() >= 1,
        "the overrunning cycle must still emit harvest.workflow.task_timeout; got {}",
        metrics.task_timeout_count()
    );
    assert_eq!(
        execution.state, "FAILED",
        "issue #494 must still quarantine a cycle that overruns its budget"
    );
    assert!(
        metrics.samples().is_empty(),
        "a pure hang is not a capability miss and must emit no \
         harvest.task.capability_miss sample; got {:?}",
        metrics.samples()
    );
    let dlq = dead_letter_errors(&url, exec_id).await;
    assert!(
        dlq.iter().any(|e| e.contains("WorkflowTaskTimeout")),
        "the dead letter must name the issue #494 reason so the hang path stays \
         distinguishable from a capability miss (AC4); got {dlq:?}"
    );
}

// ---------------------------------------------------------------------------
// Codex round-31 P1: the ownership check is ATOMIC with the terminal failure.
//
// Every earlier round narrowed the window between an escalation's last
// ownership check and its terminal write; none closed it, because the check and
// the write stayed separate database operations and `queue::fail_task` accepts
// any `PENDING`/`RUNNING` row regardless of `worker_id`. These two tests drive
// the guard directly, staging the transfer INSIDE the window with the
// lock-holding pattern the pause/child-timeout suites already use: a second
// connection holds the task row's `FOR UPDATE` lock, the terminal write queues
// behind it, and the transfer commits while it waits.
// ---------------------------------------------------------------------------

/// Seed an execution plus a `RUNNING` task row claimed by `worker_id`, and
/// return the row exactly as a dispatcher would hold it.
async fn seed_claimed_task(
    url: &str,
    queue_name: &str,
    worker_id: &str,
) -> (ExecutionId, TaskQueueItem) {
    let mut conn = connect(url).await;
    let exec_id = seed_workflow(&mut conn, "round31_wf", serde_json::json!({}), queue_name).await;

    // Claim it by hand so the test owns the timing rather than a poll loop.
    diesel::update(
        harvest_task_queue::table
            .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid()))),
    )
    .set((
        harvest_task_queue::state.eq("RUNNING"),
        harvest_task_queue::worker_id.eq(Some(worker_id)),
        harvest_task_queue::started_at.eq(Some(Utc::now())),
    ))
    .execute(&mut conn)
    .await
    .expect("claim task");

    let task = load_tasks(url, exec_id)
        .await
        .into_iter()
        .next()
        .expect("the seeded task row");
    (exec_id, task)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_claim_transferred_before_the_terminal_write_is_not_failed_by_the_stale_dispatcher() {
    let (url, _container) = setup_db().await;

    let (exec_id, task) = seed_claimed_task(&url, "q804-round31", "worker-a").await;

    // A poison-pill reclaim / operator action transfers the claim after the
    // dispatcher decided to escalate but before it writes. The dispatcher still
    // holds the row it loaded at claim time -- that snapshot is what makes the
    // stale write possible, and what the guard has to catch.
    let mut mover = connect(&url).await;
    diesel::sql_query("UPDATE harvest_task_queue SET worker_id = 'thief' WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task.id)
        .execute(&mut mover)
        .await
        .expect("transfer the claim");

    let mut conn = connect(&url).await;
    let preloaded = preload_failure_history(&mut conn, &task).await;
    let recorded = Arc::new(Mutex::new(0usize));
    let recorded_writer = Arc::clone(&recorded);
    let outcome = commit_terminal_failure_if_still_claimed(
        &mut conn,
        &task,
        "worker-a",
        "no_capable_worker: activity 'charge' is not registered",
        preloaded,
        // These pin the CLAIM guard specifically; the evidence re-decision has
        // its own test below.
        None,
        |_| {
            *recorded_writer
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) += 1;
        },
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("the guarded write must not error");

    assert_eq!(
        outcome,
        TerminalWriteOutcome::ClaimLost,
        "the dispatcher that lost the claim must withdraw, not fail the new owner's task"
    );

    // Nothing was written: the run the new owner picked up is untouched.
    let history = load_history(&url, exec_id).await;
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. })),
        "a withdrawn escalation must append no terminal event; got {history:?}"
    );
    let execution = load_execution(&url, exec_id).await;
    assert_eq!(
        execution.state, "RUNNING",
        "the execution must stay runnable for the worker that now owns the task"
    );
    let after = load_task(&url, task.id).await;
    assert_eq!(after.state, "RUNNING", "the new owner's claim must survive");
    assert_eq!(after.worker_id.as_deref(), Some("thief"));
    assert!(
        after.error.is_none(),
        "the new owner's task must not carry the stale dispatcher's reason; got {:?}",
        after.error
    );
    assert_eq!(
        *recorded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        0,
        "a withdrawn escalation must not fire the page-severity counter"
    );
}

/// The guard must never *wait* on the task row (round-32 F1).
///
/// `poison_pill::quarantine_orphan` locks the task row first and the execution
/// row second (via the dead-letter FK and `fail_owning_workflow`), the exact
/// inverse of the order this commit boundary takes. A blocking `FOR UPDATE`
/// here would give the two paths a genuine ABBA cycle -- on precisely the pair
/// that races, since the worker being quarantined is the one whose heartbeat
/// went stale while it was escalating.
///
/// `SKIP LOCKED` removes the waiting edge: a row someone else holds reads as
/// "not ours right now" and the escalation withdraws. This test proves the
/// non-blocking part directly -- the write completes *while the lock is still
/// held*, which a blocking guard could not do.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_task_row_locked_by_a_concurrent_transaction_withdraws_without_waiting() {
    let (url, _container) = setup_db().await;

    let (exec_id, task) = seed_claimed_task(&url, "q804-round32-skip", "worker-a").await;

    let mut holder = connect(&url).await;
    diesel::sql_query("BEGIN")
        .execute(&mut holder)
        .await
        .expect("begin holder txn");
    diesel::sql_query("SELECT id FROM harvest_task_queue WHERE id = $1 FOR UPDATE")
        .bind::<diesel::sql_types::Uuid, _>(task.id)
        .execute(&mut holder)
        .await
        .expect("hold the task row lock");

    let write_url = url.clone();
    let write_task = task.clone();
    let recorded = Arc::new(Mutex::new(0usize));
    let recorded_writer = Arc::clone(&recorded);
    let writer = tokio::spawn(async move {
        let mut conn = connect(&write_url).await;
        let preloaded = preload_failure_history(&mut conn, &write_task).await;
        commit_terminal_failure_if_still_claimed(
            &mut conn,
            &write_task,
            "worker-a",
            "no_capable_worker: activity 'charge' is not registered",
            preloaded,
            None,
            |_| {
                *recorded_writer
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) += 1;
            },
            &autumn_harvest::payload_codec::PayloadCodecs::default(),
        )
        .await
        .expect("the guarded write must not error")
    });

    // The lock is STILL HELD here. A blocking guard would sit on it until the
    // holder commits and blow this timeout; the non-blocking one must answer.
    let outcome = tokio::time::timeout(Duration::from_secs(10), writer)
        .await
        .expect(
            "the guard must not wait on the task row -- waiting is the ABBA edge \
             against poison_pill's task -> execution order",
        )
        .expect("writer task");
    assert_eq!(
        outcome,
        TerminalWriteOutcome::ClaimLost,
        "a row held by another transaction must read as a lost claim and withdraw"
    );

    diesel::sql_query("COMMIT")
        .execute(&mut holder)
        .await
        .expect("release the lock");

    // Withdrawing wrote nothing, so the task stays claimable/runnable.
    let history = load_history(&url, exec_id).await;
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. })),
        "a withdrawn escalation must append no terminal event; got {history:?}"
    );
    assert_eq!(load_execution(&url, exec_id).await.state, "RUNNING");
    assert_eq!(load_task(&url, task.id).await.state, "RUNNING");
    assert_eq!(
        *recorded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        0,
        "a withdrawn escalation must not fire the page-severity counter"
    );
}

/// A poison-pill requeue hands the row back to the pool with the SAME worker
/// still eligible to re-claim it (round-32 F3).
///
/// `poison_pill::requeue_orphan` sets `PENDING` / `worker_id = NULL` and bumps
/// `crash_strikes`; nothing stops the original worker from winning the row
/// again. A guard keyed on `(state, worker_id)` alone would pass for that NEW
/// claim and let the stale escalation terminally fail work that had just been
/// legitimately restarted -- which is why the guard also matches
/// `crash_strikes`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_same_worker_reclaim_after_a_requeue_is_not_failed_by_the_stale_escalation() {
    let (url, _container) = setup_db().await;

    let (exec_id, task) = seed_claimed_task(&url, "q804-round32-strikes", "worker-a").await;
    assert_eq!(
        task.crash_strikes, 0,
        "the dispatcher's snapshot is the pre-requeue claim"
    );

    // Requeue (strike bumped), then the SAME worker re-claims it.
    let mut mover = connect(&url).await;
    diesel::sql_query(
        "UPDATE harvest_task_queue \
         SET state = 'RUNNING', worker_id = 'worker-a', crash_strikes = crash_strikes + 1 \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task.id)
    .execute(&mut mover)
    .await
    .expect("requeue then re-claim with the same worker");

    let mut conn = connect(&url).await;
    let preloaded = preload_failure_history(&mut conn, &task).await;
    let recorded = Arc::new(Mutex::new(0usize));
    let recorded_writer = Arc::clone(&recorded);
    let outcome = commit_terminal_failure_if_still_claimed(
        &mut conn,
        &task,
        "worker-a",
        "no_capable_worker: activity 'charge' is not registered",
        preloaded,
        // These pin the CLAIM guard specifically; the evidence re-decision has
        // its own test below.
        None,
        |_| {
            *recorded_writer
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) += 1;
        },
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("the guarded write must not error");

    assert_eq!(
        outcome,
        TerminalWriteOutcome::ClaimLost,
        "the strike bump means this is a NEW claim -- matching worker_id alone \
         would wrongly fail freshly restarted work"
    );
    let history = load_history(&url, exec_id).await;
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. })),
        "a withdrawn escalation must append no terminal event; got {history:?}"
    );
    assert_eq!(load_execution(&url, exec_id).await.state, "RUNNING");
    let after = load_task(&url, task.id).await;
    assert_eq!(after.state, "RUNNING");
    assert!(after.error.is_none());
    assert_eq!(
        *recorded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        0,
        "a withdrawn escalation must not fire the page-severity counter"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_claim_still_held_through_the_terminal_write_still_fails_the_task() {
    // The control for the three tests above: same guard, same call shape, but
    // the claim the dispatcher holds is still the live one -- so the escalation
    // must commit. Without it, a guard that simply always withdrew would pass
    // all three and silently break AC3's boundedness.
    let (url, _container) = setup_db().await;

    let (exec_id, task) = seed_claimed_task(&url, "q804-round31-ok", "worker-a").await;

    let mut conn = connect(&url).await;
    let preloaded = preload_failure_history(&mut conn, &task).await;
    let recorded = Arc::new(Mutex::new(0usize));
    let recorded_writer = Arc::clone(&recorded);
    let outcome = commit_terminal_failure_if_still_claimed(
        &mut conn,
        &task,
        "worker-a",
        "no_capable_worker: activity 'charge' is not registered",
        preloaded,
        // These pin the CLAIM guard specifically; the evidence re-decision has
        // its own test below.
        None,
        |_| {
            *recorded_writer
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) += 1;
        },
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("the guarded write must not error");

    assert!(
        matches!(outcome, TerminalWriteOutcome::Committed(_)),
        "got {outcome:?}"
    );

    let history = load_history(&url, exec_id).await;
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. })),
        "an escalation whose claim survived must still terminate the run (AC3); got {history:?}"
    );
    assert_eq!(load_execution(&url, exec_id).await.state, "FAILED");
    let after = load_task(&url, task.id).await;
    assert_eq!(after.state, "FAILED");
    assert!(
        after
            .error
            .as_deref()
            .is_some_and(|e| e.starts_with(NO_CAPABLE_WORKER_PREFIX)),
        "the terminal reason must keep the AC5 prefix; got {:?}",
        after.error
    );
    assert_eq!(
        *recorded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        1,
        "the page-severity counter must fire exactly once, and only for an \
         escalation that actually committed"
    );
}

/// Codex round-31 P1 (second finding): the preloaded `next_event_id` is stale by
/// the time the terminal write runs, so it must be re-derived under the
/// execution row's lock.
///
/// Round 29 deliberately hoisted the history load OUT of the window between the
/// escalation's last evidence check and its terminal write. That made the id it
/// produced older, not fresher: a parallel activity completing in the widened
/// window consumes the cached id, and appending `WorkflowFailed` at a consumed
/// id aborts the whole terminal transaction on
/// `UNIQUE(workflow_exec_id, event_id)`. The row is then left `RUNNING` under a
/// LIVE incapable worker -- unclaimable, and invisible to orphan reclamation,
/// which requires a dead heartbeat. That is a *worse* outcome than the
/// escalation this code exists to perform.
///
/// The window is staged directly rather than raced: preload, then append, then
/// commit.
#[tokio::test]
async fn a_terminal_write_appends_at_the_event_id_current_under_the_lock() {
    let (url, _container) = setup_db().await;

    let (exec_id, task) = seed_claimed_task(&url, "q804-round32", "worker-a").await;

    let mut conn = connect(&url).await;
    // What the escalation does first (round 29): pay the history read up front.
    let preloaded = preload_failure_history(&mut conn, &task).await;

    // Exactly the interleaving the finding names: a parallel activity completes
    // in the window and consumes the id the preload cached. `seed_execution`
    // appended `WorkflowStarted` at 0, so the preload cached 1.
    store::append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::ActivityCompleted {
            activity_id: autumn_harvest::types::ActivityExecId::new(),
            output: serde_json::json!("done"),
        }],
        1,
    )
    .await
    .expect("the parallel completion must land at the cached id");

    let outcome = commit_terminal_failure_if_still_claimed(
        &mut conn,
        &task,
        "worker-a",
        "no_capable_worker: activity 'charge' is not registered",
        preloaded,
        // These pin the CLAIM guard specifically; the evidence re-decision has
        // its own test below.
        None,
        |_| {},
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("the terminal write must not collide with the consumed event id");
    assert!(
        matches!(outcome, TerminalWriteOutcome::Committed(_)),
        "got {outcome:?}"
    );

    // The escalation actually happened: the run is terminal, not stranded.
    let history = load_history(&url, exec_id).await;
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. })),
        "the terminal event must be appended at the CURRENT id, not the stale \
         one (AC3 must still terminate); got {history:?}"
    );
    // And the parallel completion survived -- the refresh appended after it
    // rather than colliding with it.
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. })),
        "the parallel completion must not be clobbered; got {history:?}"
    );
    assert_eq!(load_execution(&url, exec_id).await.state, "FAILED");
    let after = load_task(&url, task.id).await;
    assert_eq!(
        after.state, "FAILED",
        "a collided append would roll the transaction back and strand this row \
         RUNNING under a live worker, where orphan reclamation cannot see it"
    );
}

// ---------------------------------------------------------------------------
// Codex round-33 P1: the evidence re-decision must happen UNDER the task lock.
//
// A peer that re-registers clears itself from `capability_miss_workers` without
// touching `worker_id` or `crash_strikes`, so the round-31/32 claim guard is
// blind to it: ownership is unchanged, the guard passes, and the escalation
// commits -- terminally failing a run the newly-capable peer could serve. These
// two drive the guard with an evidence recheck attached and stage the clear on
// either side of the decision.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peer_that_re_registers_before_the_lock_withdraws_the_escalation() {
    let (url, _container) = setup_db().await;
    let queue = "q804-round33";

    let (exec_id, task) = seed_claimed_task(&url, queue, "worker-a").await;
    // Both live workers have missed: the evidence supports escalating.
    seed_live_worker(&url, "worker-a", queue).await;
    seed_live_worker(&url, "worker-b", queue).await;
    seed_prior_missing_workers(&url, exec_id, &["worker-a", "worker-b"], "round31_wf").await;

    // worker-b re-registers onto a capable build: registration atomically drops
    // its stale evidence. Ownership is untouched, so the claim guard cannot see
    // this -- only an evidence read can.
    let mut peer = connect(&url).await;
    diesel::sql_query(
        "UPDATE harvest_task_queue \
         SET capability_miss_workers = array_remove(capability_miss_workers, 'worker-b') \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task.id)
    .execute(&mut peer)
    .await
    .expect("peer re-registration clears its evidence");

    let mut conn = connect(&url).await;
    let preloaded = preload_failure_history(&mut conn, &task).await;
    let recorded = Arc::new(Mutex::new(0usize));
    let recorded_writer = Arc::clone(&recorded);
    let outcome = commit_terminal_failure_if_still_claimed(
        &mut conn,
        &task,
        "worker-a",
        "no_capable_worker: activity 'charge' is not registered",
        preloaded,
        Some(EscalationCommit {
            recheck: EscalationEvidenceRecheck {
                task: &task,
                worker_id: "worker-a",
                policy: CapabilityMissPolicy {
                    max_redeliveries: 2,
                    worker_stale_secs: 600,
                },
                frontier_reset_committed: false,
                // The frontier the seeded evidence is recorded against
                // (Codex round-46 P1): a mismatch would read as a fresh
                // budget and the escalation these tests are about would
                // never be reached.
                frontier: "workflow:round31_wf",
            },
            derive_reason: &|_| {
                "no_capable_worker: activity 'charge' is not registered".to_string()
            },
        }),
        |_| {
            *recorded_writer
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) += 1;
        },
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("the guarded write must not error");

    assert!(
        matches!(outcome, TerminalWriteOutcome::EvidenceChanged(_)),
        "a peer that became capable before the lock must veto the escalation; got {outcome:?}"
    );
    let history = load_history(&url, exec_id).await;
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. })),
        "a run a capable peer can serve must not be failed terminally; got {history:?}"
    );
    assert_eq!(load_execution(&url, exec_id).await.state, "RUNNING");
    let after = load_task(&url, task.id).await;
    assert_eq!(after.state, "RUNNING");
    assert!(after.error.is_none());
    assert_eq!(
        *recorded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        0,
        "a withdrawn escalation must not fire the page-severity counter"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn evidence_that_still_supports_escalation_under_the_lock_still_commits() {
    // The control: identical setup and an attached recheck, but nobody becomes
    // capable -- so the escalation must still commit. Without it, a recheck
    // that always withdrew would satisfy the test above and silently break
    // AC3's boundedness.
    let (url, _container) = setup_db().await;
    let queue = "q804-round33-ok";

    let (exec_id, task) = seed_claimed_task(&url, queue, "worker-a").await;
    seed_live_worker(&url, "worker-a", queue).await;
    seed_live_worker(&url, "worker-b", queue).await;
    seed_prior_missing_workers(&url, exec_id, &["worker-a", "worker-b"], "round31_wf").await;

    let mut conn = connect(&url).await;
    let preloaded = preload_failure_history(&mut conn, &task).await;
    let recorded = Arc::new(Mutex::new(0usize));
    let recorded_writer = Arc::clone(&recorded);
    let outcome = commit_terminal_failure_if_still_claimed(
        &mut conn,
        &task,
        "worker-a",
        "no_capable_worker: activity 'charge' is not registered",
        preloaded,
        Some(EscalationCommit {
            recheck: EscalationEvidenceRecheck {
                task: &task,
                worker_id: "worker-a",
                policy: CapabilityMissPolicy {
                    max_redeliveries: 2,
                    worker_stale_secs: 600,
                },
                frontier_reset_committed: false,
                // The frontier the seeded evidence is recorded against
                // (Codex round-46 P1): a mismatch would read as a fresh
                // budget and the escalation these tests are about would
                // never be reached.
                frontier: "workflow:round31_wf",
            },
            derive_reason: &|_| {
                "no_capable_worker: activity 'charge' is not registered".to_string()
            },
        }),
        |_| {
            *recorded_writer
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) += 1;
        },
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("the guarded write must not error");

    assert!(
        matches!(outcome, TerminalWriteOutcome::Committed(_)),
        "evidence that still names every live worker as incapable must escalate (AC3); got {outcome:?}"
    );
    let history = load_history(&url, exec_id).await;
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. })),
        "the escalation must terminate the run; got {history:?}"
    );
    assert_eq!(load_execution(&url, exec_id).await.state, "FAILED");
    assert_eq!(
        *recorded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        1,
        "the page-severity counter must fire exactly once for a committed escalation"
    );
}

/// Codex round-34 P2: the terminal reason must come from the resolution the
/// boundary decided on, not the one from a moment earlier.
///
/// An escalation can still *stand* under the lock but stand for a different
/// cause — Codex's example is a task above the absolute release ceiling whose
/// pre-transaction decision is `BudgetExhausted`, until a never-tried capable
/// peer registers and the fresh decision becomes `ReleaseCeilingExhausted`.
/// Both escalate, so a `Stands`-only check sees no change; but the two produce
/// opposite reason strings, and the older one tells an operator that no live
/// worker has the handler when one demonstrably might.
///
/// Asserted at the mechanism rather than by reproducing that arithmetic: the
/// caller supplies a stale `error` and a deriver, and what lands in the row must
/// be the derived one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_persisted_reason_comes_from_the_in_transaction_resolution() {
    let (url, _container) = setup_db().await;
    let queue = "q804-round34";

    let (exec_id, task) = seed_claimed_task(&url, queue, "worker-a").await;
    seed_live_worker(&url, "worker-a", queue).await;
    seed_live_worker(&url, "worker-b", queue).await;
    seed_prior_missing_workers(&url, exec_id, &["worker-a", "worker-b"], "round31_wf").await;

    let fresh_reason = format!("{NO_CAPABLE_WORKER_PREFIX} derived-under-the-lock");
    let expected = fresh_reason.clone();
    let mut conn = connect(&url).await;
    let preloaded = preload_failure_history(&mut conn, &task).await;
    let seen = Arc::new(Mutex::new(0usize));
    let seen_writer = Arc::clone(&seen);
    let outcome = commit_terminal_failure_if_still_claimed(
        &mut conn,
        &task,
        "worker-a",
        // The pre-transaction reason -- deliberately distinguishable, and
        // deliberately wrong by the time the boundary runs.
        &format!("{NO_CAPABLE_WORKER_PREFIX} decided-before-the-lock"),
        preloaded,
        Some(EscalationCommit {
            recheck: EscalationEvidenceRecheck {
                task: &task,
                worker_id: "worker-a",
                policy: CapabilityMissPolicy {
                    max_redeliveries: 2,
                    worker_stale_secs: 600,
                },
                frontier_reset_committed: false,
                // The frontier the seeded evidence is recorded against
                // (Codex round-46 P1): a mismatch would read as a fresh
                // budget and the escalation these tests are about would
                // never be reached.
                frontier: "workflow:round31_wf",
            },
            derive_reason: &|_resolution| fresh_reason.clone(),
        }),
        |fresh| {
            // The metric label is chosen from the same resolution the reason is.
            assert!(
                fresh.is_some(),
                "an attached recheck must hand the counter its fresh resolution"
            );
            *seen_writer
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) += 1;
        },
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("the guarded write must not error");

    assert!(
        matches!(outcome, TerminalWriteOutcome::Committed(Some(_))),
        "a committed escalation must carry the resolution it decided on, so the \
         caller's log reports the same story the row does; got {outcome:?}"
    );
    let after = load_task(&url, task.id).await;
    assert_eq!(
        after.error.as_deref(),
        Some(expected.as_str()),
        "the persisted reason must be the one derived under the lock, not the \
         pre-transaction one"
    );
    assert_eq!(
        *seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        1
    );
}

/// `EXPLAIN` a statement whose `$n` placeholders are filled with harmless
/// literals, and return the plan text.
///
/// The bind values are irrelevant to plan *shape* for this query — what is
/// under test is whether an index can serve the predicate at all — but they
/// must be well-typed, so `$2` is cast to `text[]` to match `queue_name`.
async fn explain_plan(conn: &mut AsyncPgConnection, statement: &str) -> String {
    #[derive(diesel::QueryableByName)]
    struct PlanLine {
        #[diesel(sql_type = diesel::sql_types::Text)]
        #[diesel(column_name = "QUERY PLAN")]
        line: String,
    }

    let filled = statement
        .replace("$1", "'probe-worker'")
        .replace("$2", "ARRAY['scan-probe-q0','scan-probe-q1']::text[]");

    // `enable_seqscan = off` only penalises sequential scans; Postgres still
    // falls back to one when no index *can* serve the predicate. That is what
    // makes this assertion independent of table size and planner cost
    // thresholds.
    //
    // The explicit `BEGIN`/`ROLLBACK` is load-bearing, not decoration. `SET
    // LOCAL` is scoped to the enclosing transaction, and this connection is in
    // autocommit — so issuing it as a bare statement scopes it to its own
    // one-statement transaction and it is already gone by the time `EXPLAIN`
    // runs on the next. That leaves the probe silently measuring the planner's
    // ordinary cost choice, which on a small table is a `Seq Scan` whether or
    // not an index exists: the assertion passes or fails on incidental row
    // counts left by whichever tests ran before it, and says nothing about
    // index-servability either way. `ROLLBACK` rather than `COMMIT` because
    // `EXPLAIN` without `ANALYZE` executes nothing — this is a read-only probe
    // and should leave no trace.
    diesel::sql_query("BEGIN")
        .execute(conn)
        .await
        .expect("open a transaction so SET LOCAL survives to the EXPLAIN");
    diesel::sql_query("SET LOCAL enable_seqscan = off")
        .execute(conn)
        .await
        .expect("disable seq scans for the duration of this transaction");

    let plan = diesel::sql_query(format!("EXPLAIN {filled}"))
        .load::<PlanLine>(conn)
        .await;

    // Close the transaction before unwrapping, so a failed EXPLAIN does not
    // strand the connection mid-transaction for the rest of the test.
    diesel::sql_query("ROLLBACK")
        .execute(conn)
        .await
        .expect("close the probe transaction");

    plan.expect("EXPLAIN must succeed")
        .into_iter()
        .map(|row| row.line)
        .collect::<Vec<_>>()
        .join("\n")
}

/// The startup invalidation must be servable by an index, not a full scan of
/// the queue backlog (issue #804, Codex round-39 P2).
///
/// `invalidate_capability_miss_evidence_for_worker` runs inside
/// `register_worker`'s transaction, so the worker is **not published to the
/// fleet until it finishes**. Every pre-existing `harvest_task_queue` index
/// narrows only by queue/state, and `$1 = ANY(capability_miss_workers)` is not
/// an indexable predicate on its own — so before this fix Postgres had to
/// inspect every `PENDING`/`RUNNING` row on each advertised queue. On a queue
/// with a large backlog a rolling restart therefore serialised expensive scans
/// and delayed the very workers meant to resolve the capability misses: the
/// #804 remediation got slower exactly when it was needed most.
///
/// Measured on a 200k-row backlog with 20 matching rows, before the fix:
/// `Seq Scan`, 3847 shared buffers, 84 ms. After: `Index Scan`, 9 buffers,
/// 1.7 ms — and the partial index is 16 KB against a 45 MB table, because a row
/// whose `capability_miss_workers` is empty never enters it at all. Capability
/// misses are exceptional, so the discriminating predicate is "has this row
/// recorded a miss?", not the array's contents; that is why a partial B-tree
/// beats the GIN index review suggested, which would tax every enqueue into the
/// hottest table in the system for no extra selectivity.
///
/// # Why `enable_seqscan = off`
///
/// It makes the assertion independent of table size and planner cost
/// thresholds. Postgres falls back to a sequential scan when no index *can*
/// serve a predicate, so with sequential scans penalised, "still a `Seq Scan`"
/// means precisely "no index is able to serve this query" — the property under
/// test, and the exact defect. Verified both ways on a 50-row table: without
/// the index the plan is a `Seq Scan` even with the penalty; with it, an
/// `Index Scan`.
#[tokio::test]
async fn the_startup_invalidation_is_index_servable_not_a_backlog_scan() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;

    // A handful of rows is enough: `enable_seqscan = off` removes the size
    // dependence, so this asserts index *availability*, not a cost outcome.
    for i in 0..8 {
        seed_workflow(
            &mut conn,
            "scan_probe_wf",
            serde_json::json!({}),
            &format!("scan-probe-q{}", i % 2),
        )
        .await;
    }

    let plan = explain_plan(
        &mut conn,
        queue::invalidate_capability_miss_evidence_for_worker_query(),
    )
    .await;

    assert!(
        !plan.contains("Seq Scan on harvest_task_queue"),
        "the startup invalidation must be servable by an index — it runs inside \
         register_worker's transaction, so a backlog scan delays publishing the \
         very worker that resolves the miss. Plan was:\n{plan}"
    );
}

/// The empty-array guard in the invalidation's `WHERE` clause is
/// **load-bearing**, not redundant (issue #804, Codex round-39 P2).
///
/// The partial index is gated on that predicate, and Postgres's implication
/// prover cannot derive it from the array-membership test alone: dropping the
/// conjunct silently returns the statement to a full backlog scan. Measured on
/// the same 200k-row fixture, `Index Scan` / 9 buffers / 1.7 ms becomes
/// `Seq Scan` / 3848 buffers / 83 ms.
///
/// This test exists so a future "simplify the WHERE clause" refactor fails
/// loudly instead of quietly reintroducing the regression. It is also the
/// control for the test above: without it, an always-index-scan mutant (an
/// unconditional index over the whole table, say) would satisfy that assertion
/// while hiding the fact that it is the *query shape* that makes a cheap
/// partial index usable at all.
#[tokio::test]
async fn dropping_the_empty_array_guard_returns_the_invalidation_to_a_full_scan() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;

    for i in 0..8 {
        seed_workflow(
            &mut conn,
            "scan_probe_wf",
            serde_json::json!({}),
            &format!("scan-probe-q{}", i % 2),
        )
        .await;
    }

    let guard = "AND capability_miss_workers <> '{}' ";
    let statement = queue::invalidate_capability_miss_evidence_for_worker_query();
    assert!(
        statement.contains(guard),
        "the guard this control removes must be present in the real statement; \
         got:\n{statement}"
    );
    let without_guard = statement.replace(guard, "");

    let plan = explain_plan(&mut conn, &without_guard).await;
    assert!(
        plan.contains("Seq Scan on harvest_task_queue"),
        "without the empty-array guard the planner cannot match the partial \
         index, so this variant must fall back to a scan — that is precisely \
         why the conjunct is not redundant. Plan was:\n{plan}"
    );
}

// ---------------------------------------------------------------------------
// Codex round-45 P1 — a cross-type continue-as-new (issue #803) whose TARGET
// type this worker does not register is a capability miss, not a terminal
// failure.
// ---------------------------------------------------------------------------

const Q_CAN_CROSS: &str = "q804-can-cross";

/// The source phase: it immediately continues into a DIFFERENT registered
/// workflow type, which is what makes the target's handler load-bearing on the
/// worker running the transition.
fn workflow_continues_into_target(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.continue_as_new_as_type("can_cross_target_wf", input)
            .await
            .map_err(|e| e.to_string())?;
        // `continue_as_new*` suspends the run and never resolves.
        unreachable!("continue_as_new_as_type must suspend")
    })
}

/// The target phase, registered only on the capable worker.
fn workflow_target_phase(_ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move { Ok(serde_json::json!({"phase": "target", "carried": input})) })
}

/// Issue #803 requires the cross-type target to be registered on the worker
/// that runs the transition, and shipped that as a **terminal** failure of the
/// predecessor. During a rolling deploy that is precisely #804's blameless
/// fleet condition — the old pod runs the source phase, only the new peer
/// registers the target phase — so an entity that happens to transition while
/// the deploy is in flight is killed by whichever worker claimed it first.
///
/// The command carrying the target never reaches the command-scanning
/// pre-pass: `drive_workflow` `swap_remove`s `ContinueAsNew` out of the batch
/// and returns it as the OUTCOME. So this needs its own check, and this test is
/// what proves the check is on the live dispatch path rather than only in the
/// pure matrix.
///
/// Phase 2 is load-bearing twice over: it shows the *same* execution completes
/// once a peer registers both types (so phase 1 was a release, not a stall),
/// and that the transition itself still works — a check that released
/// unconditionally would pass phase 1 and hang here.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_type_continue_as_new_missing_target_is_released_for_a_capable_peer() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);

    let exec_id = seed_workflow(
        &mut conn,
        "can_cross_source_wf",
        serde_json::json!({"entity": "sub-1"}),
        Q_CAN_CROSS,
    )
    .await;

    let metrics = Arc::new(CapabilityMetrics::default());

    // --- Phase 1: a worker that registers the SOURCE type only. ------------
    let source_only = build_worker(
        "worker-source-only",
        &[Q_CAN_CROSS],
        build_registry(
            vec![workflow_info(
                "can_cross_source_wf",
                workflow_continues_into_target,
            )],
            vec![],
            Arc::clone(&metrics),
        ),
        50,
    );

    let released = with_worker_running(&source_only, &pool, async {
        wait_for_capability_misses(&url, exec_id, 1, Duration::from_secs(30)).await
    })
    .await;

    assert_eq!(
        released.task_type, "workflow",
        "the transition is decided on the WORKFLOW task; no successor row exists yet"
    );
    assert_released_for_a_peer(&released);

    // AC1 proper: the predecessor must NOT have been sealed. Before this fix
    // `check_continue_as_new_type` reached `persist_workflow_failure` and this
    // execution was FAILED with an "is not registered on this worker" reason.
    let predecessor = load_execution(&url, exec_id).await;
    assert_eq!(
        predecessor.state, "RUNNING",
        "the predecessor must stay live for a capable peer, not be sealed by a \
         worker that merely lacks the target handler"
    );
    assert!(
        predecessor.error.is_none(),
        "no terminal reason may be recorded for a release: {:?}",
        predecessor.error
    );
    let history = load_history(&url, exec_id).await;
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. })),
        "AC1/AC7: a capability miss appends no terminal event: {history:?}"
    );
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowContinuedAsNew { .. })),
        "the seal must not have been written either -- the whole point is that \
         nothing is persisted before the target is known dispatchable: {history:?}"
    );

    // --- Phase 2: a peer registering BOTH phases completes the transition. --
    let capable = build_worker(
        "worker-both-phases",
        &[Q_CAN_CROSS],
        build_registry(
            vec![
                workflow_info("can_cross_source_wf", workflow_continues_into_target),
                workflow_info("can_cross_target_wf", workflow_target_phase),
            ],
            vec![],
            Arc::clone(&metrics),
        ),
        50,
    );

    let sealed = with_worker_running(&capable, &pool, async {
        wait_for_state(&url, exec_id, "CONTINUED_AS_NEW", Duration::from_secs(30)).await
    })
    .await;
    assert!(
        sealed.error.is_none(),
        "the predecessor seals cleanly once a capable peer runs the transition: {:?}",
        sealed.error
    );
    assert!(dead_letter_errors(&url, exec_id).await.is_empty());
}

// -- Codex round-46 P1: evidence is keyed to the handler it is about ---------

const Q_FRONTIER_KEY: &str = "q804-frontier-key";

/// A frontier the fleet has never been offered must not inherit a *different*
/// frontier's spent budget (issue #804, Codex round-46 P1).
///
/// The counters describe ONE frontier — "the position the workflow is stuck
/// at, and therefore the single handler a claiming worker must register to move
/// it". Two reset paths retire a frontier explicitly (a park, and inline
/// local-activity progress), but a third retires one with neither: newly
/// ingested history. `prepare_workflow_task_with_cache` appends a due timer
/// fire, a pending signal, or an external delta *before* the capability gate,
/// so the very next replay can be stuck on a different handler with nothing
/// having reset the columns. Without the key, the first worker to miss the NEW
/// handler inherits the old one's exhausted budget and can terminally fail a
/// run the fleet could still finish.
///
/// Driven end to end through the real dispatch path, so the assertion is about
/// what a worker actually does with a row rather than what a pure function
/// returns for a struct.
///
/// Falsifiable: with the key removed (in `frontier_miss_state` and the two SQL
/// statements), the seeded budget applies and the very first claim escalates —
/// the execution goes FAILED and the `RUNNING` assertion below fires.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn evidence_recorded_against_another_handler_does_not_spend_this_frontiers_budget() {
    let (url, _container) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;

    let exec_id = seed_workflow(
        &mut conn,
        "never_registered_wf",
        serde_json::json!({}),
        Q_FRONTIER_KEY,
    )
    .await;
    let task_id = load_tasks(&url, exec_id).await[0].id;

    // A budget of 1, fully spent — but against a handler this dispatch is NOT
    // missing. This is the state newly ingested history leaves behind: the
    // counters are real, they are simply about a position the run has moved
    // past.
    seed_prior_missing_workers(
        &url,
        exec_id,
        &["pod-a-incapable", "pod-b-incapable"],
        "a_frontier_we_moved_past",
    )
    .await;
    let seeded = load_task(&url, task_id).await;
    assert_eq!(seeded.capability_misses, 2);
    assert_eq!(
        seeded.capability_miss_handler.as_deref(),
        Some("workflow:a_frontier_we_moved_past"),
    );

    // A single incapable worker, with a budget SMALLER than the seeded spend.
    // If the stale evidence applied, this worker's first claim would escalate.
    let metrics = Arc::new(CapabilityMetrics::default());
    let worker = build_worker(
        "worker-on-the-new-frontier",
        &[Q_FRONTIER_KEY],
        build_registry(
            vec![workflow_info("some_other_wf", decoy_workflow)],
            vec![],
            Arc::clone(&metrics),
        ),
        1,
    );

    let mut watch = connect(&url).await;
    let released = with_worker_running(&worker, &pool, async {
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let task = load_task_on(&mut watch, task_id).await;
                // The release rewrites the frontier to the one actually missed.
                if task.capability_miss_handler.as_deref() == Some("workflow:never_registered_wf") {
                    return task;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("the miss must RELEASE and re-key the evidence to this frontier")
    })
    .await;

    // The count restarts at 1: this frontier has been offered to exactly one
    // worker, not to the two that missed the previous one.
    assert_eq!(
        released.capability_misses, 1,
        "a frontier no peer has been offered starts its budget at the base, \
         rather than inheriting a spend nobody made against it"
    );
    assert_eq!(
        released.capability_miss_workers,
        vec!["worker-on-the-new-frontier".to_string()],
        "the distinct-worker set must be the workers that missed THIS handler, \
         got {:?}",
        released.capability_miss_workers
    );

    // AC1: released for a peer, not terminally failed.
    assert_eq!(
        load_execution(&url, exec_id).await.state,
        "RUNNING",
        "the run stays alive for a capable peer"
    );
    let history = load_history(&url, exec_id).await;
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. })),
        "no terminal event may be appended while the budget is genuinely \
         unspent for this frontier, got {history:?}"
    );
    assert_eq!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_ESCALATED),
        0,
        "another frontier's spend must not escalate this one, got {:?}",
        metrics.samples()
    );
    assert!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_RELEASED) >= 1,
        "the miss must be counted as a release (AC5), got {:?}",
        metrics.samples()
    );
    // AC4: still never mistaken for a poison pill.
    assert_eq!(metrics.quarantine_count(), 0);
    assert_eq!(load_task(&url, task_id).await.crash_strikes, 0);
}

/// Dedicated queue for the round-55 runtime re-registration test, so blocking
/// its task row cannot stall an adjacent test.
const Q_RUNTIME_REREG: &str = "q804-runtime-rereg";
/// The worker whose fleet row vanishes at runtime.
const RUNTIME_REREG_WORKER_ID: &str = "pod-runtime-rereg";

/// A **runtime** re-registration failure arms the claim gate, not just a startup
/// one (issue #804, Codex round-55 P1).
///
/// Round 54 gated claiming on `may_claim_tasks`, but seeded the flag only from
/// the *startup* `register_in_fleet` result. [`do_heartbeat_tick`]'s `Ok(0)`
/// arm heals a fleet row that vanishes later — an operator cleanup, a retention
/// sweep, a truncated table — and when that heal fails it logged a warning and
/// left the flag clear. The worker then kept claiming while absent from
/// `harvest_workers`, which is the exact state round 54 exists to prevent, and
/// it is harmful twice over: the worker is invisible to a peer's fleet
/// evidence (so an incapable claimant can derive `AllLiveWorkersMissed` and
/// terminally fail work this worker can run), and its own claims match
/// `orphaned_running_tasks_query()`, so `reclaim_orphaned_tasks` burns a
/// `crash_strikes` increment on each until `poison_pill_threshold` quarantines
/// the task — manufacturing a #367 quarantine out of a capability problem,
/// which is the AC4 confusion in reverse.
///
/// This drives the **real** tick, because the defect is in that function's arm
/// handling; recreating the arms would prove nothing. The failure is forced the
/// way the fleet would actually see it — the invalidation half of the atomic
/// pair blocks on a row another transaction holds, and `lock_timeout` fires —
/// rather than by injecting an error type the production path cannot produce.
/// The control leg runs the identical tick with nothing blocking and asserts
/// the gate stays **open**, so a fix that armed unconditionally would fail.
///
/// [`do_heartbeat_tick`]: autumn_harvest::workers::do_heartbeat_tick
#[tokio::test]
async fn a_failed_runtime_reregistration_arms_the_claim_gate() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;

    let worker_id = RUNTIME_REREG_WORKER_ID;

    // The runtime case: this worker registered cleanly, so its startup flag is
    // clear -- and then its row vanished.
    diesel::sql_query("DELETE FROM harvest_workers WHERE worker_id = $1")
        .bind::<diesel::sql_types::Text, _>(worker_id)
        .execute(&mut conn)
        .await
        .expect("ensure the fleet row is absent");

    // A task on this worker's queue carrying its stale evidence, so the
    // invalidation half of the pair has a row it must lock -- and can block on.
    let exec_id = seed_execution(&mut conn, "runtime_rereg_wf", serde_json::json!({})).await;
    let mut params = EnqueueParams::new(Q_RUNTIME_REREG, TaskType::Workflow, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id.as_uuid());
    queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue the evidence-carrying task");
    seed_prior_missing_workers(&url, exec_id, &[worker_id], "runtime_rereg_wf").await;

    let registration = autumn_harvest::workers::WorkerRegistration {
        worker_id: worker_id.to_string(),
        queues: vec![Q_RUNTIME_REREG.to_string()],
        shard_assignments: vec![0],
        max_concurrency: 1,
        host: "test-host".to_string(),
        version: None,
        build_id: "build-804r55".to_string(),
        deployment_name: None,
        labels: HashMap::new(),
        max_concurrent_sessions: 0,
    };

    // --- Fix leg: hold the invalidation's target row so the atomic pair cannot
    //     commit, exactly as a contended database would.
    let mut blocker = connect(&url).await;
    diesel::sql_query("BEGIN")
        .execute(&mut blocker)
        .await
        .expect("open the blocking transaction");
    diesel::sql_query("SELECT id FROM harvest_task_queue WHERE queue_name = $1 FOR UPDATE")
        .bind::<diesel::sql_types::Text, _>(Q_RUNTIME_REREG)
        .execute(&mut blocker)
        .await
        .expect("hold the row the invalidation must update");

    diesel::sql_query("SET lock_timeout = '250ms'")
        .execute(&mut conn)
        .await
        .expect("bound the wait so the pair fails rather than hanging");

    let gate = AtomicBool::new(false);
    tick_once(&mut conn, &registration, &gate).await;

    diesel::sql_query("SET lock_timeout = DEFAULT")
        .execute(&mut conn)
        .await
        .expect("restore the session default");
    diesel::sql_query("ROLLBACK")
        .execute(&mut blocker)
        .await
        .expect("release the held row");

    assert_eq!(
        live_worker_count(&url, worker_id).await,
        0,
        "precondition: the pair must have rolled back, leaving this worker \
         absent from the fleet -- otherwise there is nothing to gate"
    );
    assert!(
        // Fully qualified: with `diesel::prelude::*` in scope, a bare
        // `.load(..)` resolves to diesel's blanket `RunQueryDsl::load`.
        AtomicBool::load(&gate, Ordering::Relaxed),
        "a worker whose runtime re-registration failed is absent from \
         harvest_workers, so it must stop claiming: its claims are invisible to \
         a peer's fleet evidence and are reclaimed as orphans, burning crash \
         strikes toward a poison-pill quarantine it never earned"
    );

    // --- Control: nothing blocking, so the same arm heals the row and the gate
    //     stays open. Guards against a fix that simply always arms.
    let gate = AtomicBool::new(true);
    tick_once(&mut conn, &registration, &gate).await;

    assert_eq!(
        live_worker_count(&url, worker_id).await,
        1,
        "control: with nothing blocking, the Ok(0) arm must heal the fleet row"
    );
    assert!(
        !AtomicBool::load(&gate, Ordering::Relaxed),
        "control: a re-registration that committed must reopen the gate, or a \
         worker that healed itself would never claim again"
    );
}
