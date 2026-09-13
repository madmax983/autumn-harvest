//! Per-tenant resource quota admission tests — issue #946.
//!
//! # AC coverage map
//!
//! - **AC1** (dot-path key resolver reuse, no second resolver) — every test
//!   below resolves its tenant key via a plain `"tenant_id"` expression,
//!   exercised through the SAME [`autumn_harvest::quota::resolve_quota_key`]
//!   (a one-line delegate to [`autumn_harvest::concurrency::resolve_concurrency_key`])
//!   the admission path itself calls; no test constructs a key any other way.
//! - **AC2** (≥3 independent optional caps) —
//!   [`active_executions_cap_admits_exactly_n_then_rejects_the_next`] (money
//!   test), [`history_bytes_cap_rejects_once_exceeded`],
//!   [`dead_letters_cap_rejects_once_reached`], and
//!   [`policy_with_no_caps_declared_is_a_noop`] each exercise ONE resource in
//!   isolation from the other two (a policy declaring only that resource's
//!   `with_max_*`), proving the three caps are independent, not a single
//!   combined budget.
//! - **AC3** (enforcement at admission, before `WorkflowStarted`, on every
//!   registry-aware start path) — the direct-admission tests below drive the
//!   real [`start_or_load_workflow_execution`] entry point; the
//!   `continue_as_new_*` tests drive a genuine worker end to end, proving the
//!   SAME `quota_key` resolution that governs a fresh start also governs an
//!   in-flight continuation's successor row. Batch-start, schedule
//!   tick/backfill, and debounce/throttle scanner-fire coverage is tracked
//!   separately (issue #946 Task 7) — every one of those paths funnels
//!   through the identical `start_or_load_workflow_execution_collect` choke
//!   point this file exercises directly, so enforcement there is structural,
//!   not per-call-site.
//! - **AC4** (typed error, never silent, never a `500`) — every rejection
//!   test destructures the exact
//!   [`autumn_harvest::error::HarvestError::QuotaExceeded`] shape
//!   (`workflow_name`/`key`/`resource`/`limit`/`current`), and
//!   [`rejected_start_creates_no_execution_or_task_row`] proves the
//!   rejection rolls back atomically — no phantom execution or task row
//!   survives a rejected attempt.
//! - **AC7** ("cheap by construction… never a full-table scan per
//!   admission") — exercised transitively (every admission attempt here
//!   drives the real, single-round-trip `QUOTA_USAGE_SQL` query); the query
//!   shape itself is asserted directly in `quota.rs`'s own unit tests.
//! - **AC8** (shard-local scope) — out of scope for a single-shard suite;
//!   documented in `docs/sharding.md`.
//! - **AC9** (no-policy workflow byte-for-byte unchanged, zero default
//!   overhead) — [`no_policy_workflow_is_unaffected`].
//!
//! Deliberately **not** covered here, per issue #946's own scope split: the
//! HTTP `429` mapping, the `harvest.quota.rejected` metric, and
//! `GET /admin/quotas` (Task 6, plugin-layer); the literal 10,000-start
//! success-metric load test and the batch/schedule/debounce/throttle
//! path-by-path coverage sweep (Task 7).

#![cfg(feature = "db")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    // Diesel `#[derive(QueryableByName)] struct XxxRow { .. }` helpers are
    // conventionally defined right where they're used, mid-function, across
    // every integration-test file in this repo that reads a raw column via
    // `sql_query` (see e.g. `child_policy_tests.rs`, `pause_tests.rs`).
    clippy::items_after_statements
)]

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};

use autumn_harvest::completion_trigger::{
    GLOBAL_WORKFLOW_METADATA, WorkflowMetadata, enforce_completion_triggers_outbox,
};
use autumn_harvest::debounce::DebounceStartOptions;
use autumn_harvest::dlq::{NewDeadLetterEntry, dead_letter};
use autumn_harvest::error::{HarvestError, HarvestResult, PayloadKind};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::event_batch::{AdmitBatchParams, admit_batched_start};
use autumn_harvest::execution::{StartWorkflowParams, start_or_load_workflow_execution};
use autumn_harvest::info::{ActivityHandlerFn, ActivityInfo, WorkflowHandlerFn};
use autumn_harvest::models::{
    CompletionTriggerOutboxDb, NewCompletionTriggerOutboxDb, WorkflowExecution,
};
use autumn_harvest::quota::{MAX_QUOTA_KEY_BYTES, QuotaPolicy, QuotaResource};
use autumn_harvest::schema::{harvest_completion_trigger_outbox, harvest_workflow_executions};
use autumn_harvest::shard::{ShardRouter, ShardedDbPool, install_global_router};
use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::types::{
    ExecutionId, ParentClosePolicy, Priority, ShardId, StartSource, WorkflowIdConflictPolicy,
    WorkflowIdReusePolicy,
};
use autumn_harvest::worker::HandlerRegistry;
use autumn_harvest::{ActivityContext, WorkflowContext, WorkflowInfo};
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use uuid::Uuid;

use crate::integration_e2e::{
    build_runtime_worker, build_test_pool, load_history_from_url, setup_test_database_url_or_env,
    spawn_test_worker, wait_for_execution_state,
};

// ---------------------------------------------------------------------------
// Shared harness
// ---------------------------------------------------------------------------

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url)
        .await
        .expect("connect to test database")
}

/// A unique workflow-type name per test — the registry and
/// [`GLOBAL_WORKFLOW_METADATA`] are process-global, so every test needs its
/// own namespace to avoid cross-test interference.
fn leaked(prefix: &str) -> &'static str {
    Box::leak(format!("{prefix}_{}", Uuid::new_v4().simple()).into_boxed_str())
}

fn wf_meta(quota: QuotaPolicy) -> WorkflowMetadata {
    WorkflowMetadata {
        concurrency: None,
        max_input_bytes: None,
        owner: None,
        runbook_url: None,
        severity: None,
        input_schema: None,
        sla: None,
        retry_policy: None,
        quota: Some(quota),
    }
}

/// Serializes access to the process-global [`GLOBAL_WORKFLOW_METADATA`]
/// mirror across this file's tests. CI runs `linux` integration suites with
/// `--test-threads=1` (see `.github/ci/integration-suites.txt`), so this is
/// primarily a local-`cargo test`-without-that-flag safeguard, mirroring the
/// `TEST_SERIAL` convention already used by `completion_callback_tests.rs`.
static TEST_SERIAL: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

/// RAII installer for [`GLOBAL_WORKFLOW_METADATA`]: installs the given map,
/// and restores whatever was there before on drop — including on a mid-test
/// panic, unlike a bare manual take/restore pair.
struct MetadataGuard {
    previous: Option<HashMap<String, WorkflowMetadata>>,
    _permit: tokio::sync::MutexGuard<'static, ()>,
}

impl MetadataGuard {
    async fn install(map: HashMap<String, WorkflowMetadata>) -> Self {
        let permit = TEST_SERIAL.lock().await;
        let previous = {
            let mut lock = GLOBAL_WORKFLOW_METADATA.write().expect("metadata lock");
            lock.take()
        };
        {
            let mut lock = GLOBAL_WORKFLOW_METADATA.write().expect("metadata lock");
            *lock = Some(map);
        }
        Self {
            previous,
            _permit: permit,
        }
    }

    /// Convenience for the common single-workflow-type case.
    async fn install_one(workflow_name: &'static str, quota: QuotaPolicy) -> Self {
        let mut map = HashMap::new();
        map.insert(workflow_name.to_string(), wf_meta(quota));
        Self::install(map).await
    }
}

impl Drop for MetadataGuard {
    fn drop(&mut self) {
        if let Ok(mut lock) = GLOBAL_WORKFLOW_METADATA.write() {
            *lock = self.previous.take();
        }
    }
}

/// Build a [`StartWorkflowParams`] with every non-essential field at its
/// production default, mirroring `cross_type_continue_as_new_tests.rs`'s
/// `start_root` literal exactly (so this stays a faithful production shape,
/// not a hand-trimmed one).
fn params<'a>(
    workflow_name: &'a str,
    workflow_id: &'a str,
    exec_id: ExecutionId,
    input: serde_json::Value,
) -> StartWorkflowParams<'a> {
    StartWorkflowParams {
        workflow_name,
        workflow_id,
        exec_id,
        input,
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
        conflict_policy: WorkflowIdConflictPolicy::Unspecified,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        inherited_chain_deadline_at: None,
        concurrency_key: None,
        concurrency_limit: None,
        concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
        priority: Priority::default(),
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
        start_source: StartSource::Api,
        start_source_ref: None,
        started_by: None,
    }
}

/// Attempt a start through the real production entry point. Returns the
/// pre-generated candidate `exec_id` alongside the outcome so a REJECTED
/// attempt can still be checked for "no row exists under this id" (the
/// `Err` variant itself carries no `exec_id`).
async fn try_start(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    input: serde_json::Value,
) -> (
    ExecutionId,
    HarvestResult<autumn_harvest::execution::StartedWorkflowExecution>,
) {
    let exec_id = ExecutionId::new();
    let outcome = start_or_load_workflow_execution(
        conn,
        params(workflow_name, workflow_id, exec_id, input),
        None,
    )
    .await;
    (exec_id, outcome)
}

/// A fresh, uniquely-`workflow_id`'d start that must succeed — the common
/// case in every test below.
async fn start_ok(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    input: serde_json::Value,
) -> ExecutionId {
    let workflow_id = format!("wid-{}", Uuid::new_v4().simple());
    let (exec_id, outcome) = try_start(conn, workflow_name, &workflow_id, input).await;
    outcome.unwrap_or_else(|e| panic!("expected a successful start, got {e:?}"));
    exec_id
}

async fn count_rows(conn: &mut AsyncPgConnection, sql: &str, binds: &[&str]) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let mut query = diesel::sql_query(sql).into_boxed();
    for b in binds {
        query = query.bind::<diesel::sql_types::Text, _>((*b).to_string());
    }
    let row: Count = query.get_result(conn).await.expect("count rows");
    row.n
}

#[derive(diesel::QueryableByName)]
struct TaskQueueStateRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    state: String,
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    scheduled_at: chrono::DateTime<chrono::Utc>,
}

async fn task_queue_state(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> TaskQueueStateRow {
    diesel::sql_query(
        "SELECT state, scheduled_at FROM harvest_task_queue WHERE workflow_exec_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .get_result(conn)
    .await
    .expect("parent task row must exist")
}

/// Wait for the parent's task row to complete at least one `QuotaExceeded`
/// retry cycle after `since` (its `scheduled_at` at/before the cycle started),
/// observed while `PENDING` (i.e. between cycles, not mid-claim), and return
/// that new `scheduled_at` (issue #1227, Findings 1 & 2).
///
/// A `QuotaExceeded` catch that re-implements `park_workflow_task` + an
/// unconditional `wake_workflow_task` never touches this column, so a retry
/// cycle leaves it unchanged from `since` (or advances it only to
/// approximately now) -- the row is immediately reclaimable, a zero-delay
/// retry loop. Routing through `recover_from_child_quota_exceeded` instead
/// calls `queue::requeue_for_retry`, which stamps `scheduled_at = now() +
/// backoff` (`QUOTA_RETRY_BACKOFF_MIN..MAX`, 500ms-3s) every single cycle it
/// re-hits the same still-exhausted quota. So once a cycle has actually run,
/// its resulting `scheduled_at` must be in the future -- the hot-spin bug's
/// exact opposite.
///
/// Requiring `scheduled_at != since` (not just "next `PENDING` sample") rules
/// out trivially observing the row's PRE-worker value -- e.g. if the test's
/// own poll happens to run before the worker's first claim, which would
/// otherwise flakily read a stale, never-retried timestamp instead of one an
/// actual retry cycle produced. Sampling only while `PENDING` additionally
/// avoids a read landing mid-cycle, between a backoff elapsing and the retry's
/// own `QuotaExceeded` catch re-stamping a fresh one.
///
/// Returns `(scheduled_at, observed_now)`: `observed_now` is captured
/// immediately after the qualifying read, in the same call, so the caller's
/// "is this in the future" comparison isn't stretched by whatever happens
/// between this function returning and the caller's own `Utc::now()` call --
/// immaterial given the backoff's 500ms floor, but free to close out.
async fn task_scheduled_at_after_a_retry_cycle(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    since: chrono::DateTime<chrono::Utc>,
) -> (chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let row = task_queue_state(conn, exec_id).await;
        if row.state == "PENDING" && row.scheduled_at != since {
            return (row.scheduled_at, chrono::Utc::now());
        }
        assert!(
            std::time::Instant::now() < deadline,
            "parent task row never completed a retry cycle (scheduled_at \
             never moved off its pre-worker value {since:?})"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

async fn active_count(conn: &mut AsyncPgConnection, workflow_name: &str, quota_key: &str) -> i64 {
    count_rows(
        conn,
        "SELECT COUNT(*)::BIGINT AS n FROM harvest_workflow_executions \
         WHERE workflow_name = $1 AND quota_key = $2 AND state IN ('RUNNING', 'PAUSED')",
        &[workflow_name, quota_key],
    )
    .await
}

/// Read a single execution row's persisted `state` column -- used by the
/// `replace_execution` (issue #946 P1) regression tests below to prove a
/// REJECTED replace rolls back the whole transaction, including the seal of
/// the row being replaced (it must still read its pre-replace state, never
/// left half-sealed).
async fn row_state(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> String {
    #[derive(diesel::QueryableByName)]
    struct State {
        #[diesel(sql_type = diesel::sql_types::Text)]
        state: String,
    }
    let row: State =
        diesel::sql_query("SELECT state FROM harvest_workflow_executions WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
            .get_result(conn)
            .await
            .expect("row must exist");
    row.state
}

async fn assert_no_execution_row(conn: &mut AsyncPgConnection, exec_id: ExecutionId) {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let row: Count = diesel::sql_query(
        "SELECT COUNT(*)::BIGINT AS n FROM harvest_workflow_executions WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .get_result(conn)
    .await
    .expect("count execution rows");
    assert_eq!(
        row.n, 0,
        "a rejected quota admission must roll back atomically -- no phantom execution row"
    );
}

async fn assert_no_task_row(conn: &mut AsyncPgConnection, exec_id: ExecutionId) {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let row: Count = diesel::sql_query(
        "SELECT COUNT(*)::BIGINT AS n FROM harvest_task_queue WHERE workflow_exec_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .get_result(conn)
    .await
    .expect("count task-queue rows");
    assert_eq!(
        row.n, 0,
        "a rejected quota admission must roll back atomically -- no phantom task-queue row"
    );
}

async fn mark_terminal(conn: &mut AsyncPgConnection, exec_id: ExecutionId, state: &str) {
    diesel::sql_query(
        "UPDATE harvest_workflow_executions SET state = $1, completed_at = NOW() WHERE id = $2",
    )
    .bind::<diesel::sql_types::Text, _>(state)
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(conn)
    .await
    .expect("mark terminal");
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'COMPLETED' WHERE workflow_exec_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(conn)
    .await
    .expect("close tasks");
}

/// Assert a rejection carries the exact expected [`HarvestError::QuotaExceeded`]
/// shape. `current` is checked via a caller-supplied predicate rather than
/// exact equality where the value is implementation-detail-fragile (e.g.
/// `pg_column_size` byte counts).
fn assert_quota_exceeded(
    err: &HarvestError,
    expected_workflow_name: &str,
    expected_key: &str,
    expected_resource: QuotaResource,
    expected_limit: u64,
    current_ok: impl FnOnce(u64) -> bool,
) {
    match err {
        HarvestError::QuotaExceeded {
            workflow_name,
            key,
            resource,
            limit,
            current,
        } => {
            assert_eq!(workflow_name, expected_workflow_name);
            assert_eq!(key, expected_key);
            assert_eq!(*resource, expected_resource);
            assert_eq!(*limit, expected_limit);
            assert!(
                current_ok(*current),
                "current={current} failed the caller's predicate for resource {resource:?}"
            );
        }
        other => panic!("expected HarvestError::QuotaExceeded, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// AC2 / AC4 -- max_active_executions, the headline success-metric shape
// ---------------------------------------------------------------------------

/// The money test: a `max_active_executions = 5` policy admits exactly 5
/// concurrent starts for one key, and the 6th is rejected with the exact
/// typed error -- the small-N analogue of the issue's "10,000 starts capped
/// at exactly 100" success metric (the full-scale load test is Task 7).
#[tokio::test]
async fn active_executions_cap_admits_exactly_n_then_rejects_the_next() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf = leaked("quota_active_cap");
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(5);
    let _guard = MetadataGuard::install_one(wf, policy).await;

    for i in 0..5 {
        start_ok(&mut conn, wf, serde_json::json!({"tenant_id": "acme"})).await;
        assert_eq!(
            active_count(&mut conn, wf, "acme").await,
            i64::from(i) + 1,
            "admission {i} must bring the active count to exactly {}",
            i + 1
        );
    }
    assert_eq!(active_count(&mut conn, wf, "acme").await, 5);

    let (rejected_id, outcome) = try_start(
        &mut conn,
        wf,
        &format!("wid-{}", Uuid::new_v4().simple()),
        serde_json::json!({"tenant_id": "acme"}),
    )
    .await;
    let err = outcome.expect_err("the 6th admission for a cap of 5 must be rejected");
    assert_quota_exceeded(&err, wf, "acme", QuotaResource::ActiveExecutions, 5, |c| {
        c == 5
    });

    // Capped, not merely slowed: still exactly 5, and the rejected attempt
    // left no trace of itself.
    assert_eq!(active_count(&mut conn, wf, "acme").await, 5);
    assert_no_execution_row(&mut conn, rejected_id).await;
}

/// AC4: the rejection rolls back atomically -- no phantom execution row and
/// no phantom task-queue row survive a rejected attempt.
#[tokio::test]
async fn rejected_start_creates_no_execution_or_task_row() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf = leaked("quota_no_phantom_rows");
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1);
    let _guard = MetadataGuard::install_one(wf, policy).await;

    start_ok(&mut conn, wf, serde_json::json!({"tenant_id": "acme"})).await;

    let (rejected_id, outcome) = try_start(
        &mut conn,
        wf,
        &format!("wid-{}", Uuid::new_v4().simple()),
        serde_json::json!({"tenant_id": "acme"}),
    )
    .await;
    outcome.expect_err("the 2nd admission for a cap of 1 must be rejected");
    assert_no_execution_row(&mut conn, rejected_id).await;
    assert_no_task_row(&mut conn, rejected_id).await;
}

/// Two distinct resolved keys under one policy are independently capped.
#[tokio::test]
async fn active_executions_cap_isolates_per_key() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf = leaked("quota_isolate_per_key");
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1);
    let _guard = MetadataGuard::install_one(wf, policy).await;

    start_ok(&mut conn, wf, serde_json::json!({"tenant_id": "acme"})).await;
    // A different key is unaffected by "acme" being at its cap.
    start_ok(&mut conn, wf, serde_json::json!({"tenant_id": "beta"})).await;

    let (_, acme_second) = try_start(
        &mut conn,
        wf,
        &format!("wid-{}", Uuid::new_v4().simple()),
        serde_json::json!({"tenant_id": "acme"}),
    )
    .await;
    acme_second.expect_err("acme is already at its cap of 1");

    let (_, beta_second) = try_start(
        &mut conn,
        wf,
        &format!("wid-{}", Uuid::new_v4().simple()),
        serde_json::json!({"tenant_id": "beta"}),
    )
    .await;
    beta_second.expect_err("beta is already at its cap of 1");
}

/// Two different workflow TYPES resolving the same key value are
/// independently capped: accounting is keyed on `(workflow_name, quota_key)`,
/// never `quota_key` alone.
#[tokio::test]
async fn active_executions_cap_isolates_per_workflow_type() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf_a = leaked("quota_type_a");
    let wf_b = leaked("quota_type_b");
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1);
    let mut map = HashMap::new();
    map.insert(wf_a.to_string(), wf_meta(policy));
    map.insert(wf_b.to_string(), wf_meta(policy));
    let _guard = MetadataGuard::install(map).await;

    start_ok(&mut conn, wf_a, serde_json::json!({"tenant_id": "acme"})).await;
    // Type B, same resolved key "acme", is a DIFFERENT (workflow_name, key)
    // pair and so is unaffected by type A being at its cap.
    start_ok(&mut conn, wf_b, serde_json::json!({"tenant_id": "acme"})).await;

    let (_, a_second) = try_start(
        &mut conn,
        wf_a,
        &format!("wid-{}", Uuid::new_v4().simple()),
        serde_json::json!({"tenant_id": "acme"}),
    )
    .await;
    a_second.expect_err("type A/acme is already at its cap of 1");
}

// ---------------------------------------------------------------------------
// AC9 -- no policy, or a policy with no caps, is a byte-for-byte no-op
// ---------------------------------------------------------------------------

#[tokio::test]
async fn no_policy_workflow_is_unaffected() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    // No `GLOBAL_WORKFLOW_METADATA` entry at all for this type -- the
    // process-global map may be `None`, or `Some` without this key; either
    // way `quota_policy` resolves to `None` and enforcement is skipped.
    let wf = leaked("quota_no_policy");
    for _ in 0..20 {
        start_ok(&mut conn, wf, serde_json::json!({"tenant_id": "acme"})).await;
    }
    assert_eq!(
        active_count(&mut conn, wf, "acme").await,
        0,
        "no policy means no quota_key is ever stamped"
    );
}

#[tokio::test]
async fn policy_with_no_caps_declared_is_a_noop() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf = leaked("quota_no_caps");
    // `QuotaPolicy::new(key)` with zero `with_max_*` calls -- resolves a
    // key but `has_any_cap() == false`, so `check_quota` is never even
    // reached.
    let policy = QuotaPolicy::new("tenant_id");
    assert!(!policy.has_any_cap());
    let _guard = MetadataGuard::install_one(wf, policy).await;

    for _ in 0..20 {
        start_ok(&mut conn, wf, serde_json::json!({"tenant_id": "acme"})).await;
    }
    assert_eq!(active_count(&mut conn, wf, "acme").await, 20);
}

// ---------------------------------------------------------------------------
// Unresolvable key -- fail open, mirroring `concurrency_key IS NULL`
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unresolvable_key_fails_open() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf = leaked("quota_unresolvable_key");
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1);
    let _guard = MetadataGuard::install_one(wf, policy).await;

    // The input has no `tenant_id` field at all -- `resolve_quota_key`
    // returns `None`, so enforcement is skipped for every one of these
    // starts regardless of the declared cap of 1.
    for _ in 0..5 {
        start_ok(&mut conn, wf, serde_json::json!({"other_field": 1})).await;
    }
}

// ---------------------------------------------------------------------------
// Batched-start quota key resolution (issue #1230 Finding 1)
// ---------------------------------------------------------------------------

/// Admit one payload into a batch, sharing `batch_key` and `workflow_id`
/// across calls so repeated admissions collapse into one pending row.
async fn admit_batch(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    batch_key: &str,
    workflow_id: &str,
    payload: serde_json::Value,
    max_size: usize,
) -> autumn_harvest::event_batch::BatchAdmitOutcome {
    let params = AdmitBatchParams {
        workflow_name: workflow_name.to_string(),
        batch_key: batch_key.to_string(),
        workflow_id: workflow_id.to_string(),
        queue_name: "default".to_string(),
        payload,
        start_options: DebounceStartOptions::default(),
        max_wait: std::time::Duration::from_secs(3600),
        max_size,
        shard_id: 0,
    };
    admit_batched_start(conn, params, None)
        .await
        .expect("admission must not error")
        .expect("admission must return an outcome")
        .0
}

#[tokio::test]
async fn batched_start_at_max_size_stamps_quota_key_from_first_admission() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf = leaked("quota_batched_start");
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(100);
    let _guard = MetadataGuard::install_one(wf, policy).await;

    let batch_key = format!("batch-{}", Uuid::new_v4().simple());
    let workflow_id = format!("wid-{}", Uuid::new_v4().simple());

    // Admission 1 of 2: below `max_size`, buffered but not fired.
    let first = admit_batch(
        &mut conn,
        wf,
        &batch_key,
        &workflow_id,
        serde_json::json!({"tenant_id": "acme"}),
        2,
    )
    .await;
    assert!(!first.is_flushed);

    // Admission 2 of 2 reaches `max_size` and fires SYNCHRONOUSLY inside
    // this call. `event_batch.rs` merges both admissions' payloads into
    // one JSON ARRAY. It passes that array as the fired execution's
    // `input` -- the exact shape issue #1230 Finding 1 describes.
    let second = admit_batch(
        &mut conn,
        wf,
        &batch_key,
        &workflow_id,
        serde_json::json!({"tenant_id": "someone_else"}),
        2,
    )
    .await;
    assert!(second.is_flushed);

    // Before the fix, `resolve_quota_key` required an object at the first
    // path segment. It returned `None` for this array `input` -- silently
    // bypassing all three quota dimensions and leaving `quota_key = NULL`
    // on the fired row. `active_count` below reads 0 regardless of tenant
    // in that case. The fix resolves against the FIRST admission's
    // payload, so the batch's charge lands on "acme".
    assert_eq!(
        active_count(&mut conn, wf, "acme").await,
        1,
        "the first-admitted payload's tenant_id must be the fired batch's \
         resolved quota key (issue #1230 Finding 1)"
    );
    assert_eq!(
        active_count(&mut conn, wf, "someone_else").await,
        0,
        "the second admission's tenant_id must NOT be picked up -- \
         first-admission-wins, matching harvest_event_batches' own rule for \
         every other captured start option"
    );
}

#[tokio::test]
async fn batched_start_over_cap_is_rejected_at_fire_time() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf = leaked("quota_batched_start_over_cap");
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1);
    let _guard = MetadataGuard::install_one(wf, policy).await;

    // Fill the cap of 1 with a direct (non-batched) start for the same key.
    start_ok(&mut conn, wf, serde_json::json!({"tenant_id": "acme"})).await;
    assert_eq!(active_count(&mut conn, wf, "acme").await, 1);

    // A batched start for the SAME tenant, flushed at max_size, must now
    // observe the cap. Before the fix this was unreachable. The fired
    // batch's `quota_key` always resolved to `None`, so
    // `enforce_quota_admission` returned `Ok(())` unconditionally. The
    // batch fired regardless of the tenant's already-exhausted cap.
    let batch_key = format!("batch-{}", Uuid::new_v4().simple());
    let workflow_id = format!("wid-{}", Uuid::new_v4().simple());
    admit_batch(
        &mut conn,
        wf,
        &batch_key,
        &workflow_id,
        serde_json::json!({"tenant_id": "acme"}),
        2,
    )
    .await;

    // The second admission reaches `max_size` and attempts the SYNCHRONOUS
    // in-request flush. `admit_batched_start` has no dedicated
    // `QuotaExceeded` catch, unlike the scanner's `fire_claimed_batch_row`,
    // which re-defers. It propagates the rejection as an `Err` instead,
    // rolling back the whole admission transaction, batch row included.
    // That transactional propagation is pre-existing, correct behavior: an
    // in-request caller gets an authoritative rejection, not a silent
    // buffer into a batch that can never fire. This test's job is only to
    // prove the cap is observed at all. It could not be observed before
    // the fix, since `quota_key` always resolved to `None` for a batched
    // fire.
    let params = AdmitBatchParams {
        workflow_name: wf.to_string(),
        batch_key: batch_key.clone(),
        workflow_id: workflow_id.clone(),
        queue_name: "default".to_string(),
        payload: serde_json::json!({"tenant_id": "acme"}),
        start_options: DebounceStartOptions::default(),
        max_wait: std::time::Duration::from_secs(3600),
        max_size: 2,
        shard_id: 0,
    };
    let err = admit_batched_start(&mut conn, params, None)
        .await
        .expect_err("the tenant's cap of 1 is already exhausted");
    assert!(
        matches!(
            err,
            HarvestError::QuotaExceeded {
                resource: QuotaResource::ActiveExecutions,
                ..
            }
        ),
        "expected QuotaExceeded, got {err:?}"
    );
    assert_eq!(
        active_count(&mut conn, wf, "acme").await,
        1,
        "the batch must not be admitted on top of an already-exhausted cap"
    );
}

// ---------------------------------------------------------------------------
// AC2 -- max_history_bytes, isolated from the other two caps
// ---------------------------------------------------------------------------

#[tokio::test]
async fn history_bytes_cap_rejects_once_exceeded() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf = leaked("quota_history_bytes");
    // A tiny cap: the very first execution's own `WorkflowStarted` event
    // already exceeds 1 byte, so the SECOND start for the same key must be
    // rejected on `HistoryBytes` alone (active_executions/dead_letters are
    // uncapped for this policy).
    let policy = QuotaPolicy::new("tenant_id").with_max_history_bytes(1);
    let _guard = MetadataGuard::install_one(wf, policy).await;

    start_ok(&mut conn, wf, serde_json::json!({"tenant_id": "acme"})).await;

    let (_, second) = try_start(
        &mut conn,
        wf,
        &format!("wid-{}", Uuid::new_v4().simple()),
        serde_json::json!({"tenant_id": "acme"}),
    )
    .await;
    let err = second.expect_err("the 2nd start must be rejected on history_bytes");
    assert_quota_exceeded(&err, wf, "acme", QuotaResource::HistoryBytes, 1, |c| c >= 1);
}

// ---------------------------------------------------------------------------
// AC2 -- max_dead_letters, isolated from the other two caps
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dead_letters_cap_rejects_once_reached() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf = leaked("quota_dead_letters");
    let policy = QuotaPolicy::new("tenant_id").with_max_dead_letters(3);
    let _guard = MetadataGuard::install_one(wf, policy).await;

    // A seed execution to hang the DLQ rows off of -- `dead_letter()`
    // resolves `workflow_name`/`quota_key` from this exec_id's OWN row, so
    // the seed must be of the SAME workflow type and the SAME resolved key
    // as the admission attempt below.
    let seed_id = start_ok(&mut conn, wf, serde_json::json!({"tenant_id": "acme"})).await;

    for i in 0..3 {
        dead_letter(
            &mut conn,
            &NewDeadLetterEntry {
                original_task_id: Uuid::new_v4(),
                queue_name: "default".to_string(),
                task_type: "activity".to_string(),
                workflow_exec_id: Some(seed_id.as_uuid()),
                activity_name: Some("do_thing".to_string()),
                input: serde_json::json!({"i": i}),
                error: "boom".to_string(),
                attempts: 3,
                owner: None,
                severity: None,
            },
        )
        .await
        .expect("insert dead letter");
    }

    // Rejected on the FIRST attempt -- no active-execution starts needed to
    // reach the cap, unlike the active_executions test above.
    let (_, outcome) = try_start(
        &mut conn,
        wf,
        &format!("wid-{}", Uuid::new_v4().simple()),
        serde_json::json!({"tenant_id": "acme"}),
    )
    .await;
    let err = outcome.expect_err("3 dead letters already meets a cap of 3");
    assert_quota_exceeded(&err, wf, "acme", QuotaResource::DeadLetters, 3, |c| c == 3);
}

// ---------------------------------------------------------------------------
// A completed run frees its slot for a later start
// ---------------------------------------------------------------------------

#[tokio::test]
async fn active_executions_cap_frees_up_when_a_run_completes() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf = leaked("quota_frees_on_completion");
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1);
    let _guard = MetadataGuard::install_one(wf, policy).await;

    let first = start_ok(&mut conn, wf, serde_json::json!({"tenant_id": "acme"})).await;

    let (_, blocked) = try_start(
        &mut conn,
        wf,
        &format!("wid-{}", Uuid::new_v4().simple()),
        serde_json::json!({"tenant_id": "acme"}),
    )
    .await;
    blocked.expect_err("acme is at its cap of 1 while the first run is RUNNING");

    // `RUNNING` -> `COMPLETED` excludes it from the active-count filter
    // (`state IN ('RUNNING', 'PAUSED')`), freeing the slot.
    mark_terminal(&mut conn, first, "COMPLETED").await;
    assert_eq!(active_count(&mut conn, wf, "acme").await, 0);

    start_ok(&mut conn, wf, serde_json::json!({"tenant_id": "acme"})).await;
    assert_eq!(active_count(&mut conn, wf, "acme").await, 1);
}

// ---------------------------------------------------------------------------
// AC3 -- continue-as-new (in-flight continuation, not a fresh admission):
// `quota_key` propagation on the successor row. Worker-driven, mirroring
// `cross_type_continue_as_new_tests.rs`'s harness pattern exactly.
// ---------------------------------------------------------------------------

fn phase_one<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let target = input["next_type"].as_str().map(str::to_string);
        if let Some(target) = target {
            let target: &'static str = Box::leak(target.into_boxed_str());
            ctx.continue_as_new_as_type(target, serde_json::json!({"phase": "two"}))
                .await
                .map_err(|e| e.to_string())?;
            unreachable!("continue_as_new_as_type suspends the run and never resolves");
        }
        ctx.continue_as_new(serde_json::json!({"phase": "two"}))
            .await
            .map_err(|e| e.to_string())?;
        unreachable!("continue_as_new suspends the run and never resolves");
    })
}

fn phase_two<'a>(
    _ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(serde_json::json!({"ran": "phase_two", "input": input})) })
}

fn wf_info(name: &'static str, handler: WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "quota_enforcement_tests",
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

fn act_info(name: &'static str, handler: ActivityHandlerFn) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "quota_enforcement_tests",
        default_retry_policy: None,
        default_start_to_close: None,
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: Some("default"),
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

fn registry(infos: Vec<WorkflowInfo>) -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(infos, vec![]))
}

async fn load_execution(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> WorkflowExecution {
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(conn)
        .await
        .expect("load execution")
}

async fn recorded_transition(url: &str, predecessor: ExecutionId) -> (ExecutionId, Option<String>) {
    load_history_from_url(url, predecessor)
        .await
        .events
        .iter()
        .find_map(|e| match e {
            WorkflowEvent::WorkflowContinuedAsNew {
                new_exec_id,
                new_workflow_type,
                ..
            } => Some((*new_exec_id, new_workflow_type.clone())),
            _ => None,
        })
        .expect("predecessor history must contain WorkflowContinuedAsNew")
}

/// Run a worker until `predecessor` seals, then return the successor id and
/// the recorded target type.
async fn drive_transition(
    url: &str,
    predecessor: ExecutionId,
    reg: Arc<HandlerRegistry>,
    worker_id: &str,
) -> (ExecutionId, Option<String>) {
    let worker = build_runtime_worker(worker_id, 2, 1, reg);
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(url));
    let _sealed = wait_for_execution_state(url, predecessor, "CONTINUED_AS_NEW").await;
    let transition = recorded_transition(url, predecessor).await;
    worker.shutdown();
    handle.await.expect("worker join");
    transition
}

/// Start a root execution through the real start path (not the direct
/// `try_start`/`params` helpers above, since the worker needs a genuinely
/// dispatchable task -- `quota_key` resolution is identical either way).
async fn start_root(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    input: serde_json::Value,
) -> ExecutionId {
    start_or_load_workflow_execution(
        conn,
        params(workflow_name, workflow_id, ExecutionId::new(), input),
        None,
    )
    .await
    .expect("start root execution")
    .exec_id
}

/// Same-type `continue_as_new`: the successor carries the predecessor's
/// `quota_key` verbatim -- in-flight continuation of an already-admitted
/// run, not a fresh admission, so it never re-runs `check_quota`.
#[tokio::test]
async fn continue_as_new_same_type_propagates_quota_key() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let name = leaked("quota_can_same_type");
    let workflow_id = format!("loop-{}", Uuid::new_v4().simple());

    let predecessor = start_root(
        &mut conn,
        name,
        &workflow_id,
        serde_json::json!({"phase": "one"}),
    )
    .await;

    // No `GLOBAL_WORKFLOW_METADATA` entry is installed at all for this run
    // -- `start_root` above therefore stamps `quota_key = NULL`. Stamp a
    // key directly on the predecessor's row (mirroring how
    // `same_type_continue_as_new_is_unchanged` stamps a per-start override
    // the type never declared) to prove the same-type path carries
    // whatever is ALREADY on the row verbatim, regardless of any live
    // policy.
    diesel::update(harvest_workflow_executions::table.find(predecessor.as_uuid()))
        .set(harvest_workflow_executions::quota_key.eq(Some("acme")))
        .execute(&mut conn)
        .await
        .expect("stamp predecessor quota_key");

    let reg = registry(vec![wf_info(name, phase_one)]);
    let (successor, recorded_type) =
        drive_transition(&url, predecessor, reg, "w-946-same-type").await;

    assert!(
        recorded_type.is_none(),
        "a same-type continuation records no target type"
    );
    let after = load_execution(&mut conn, successor).await;
    assert_eq!(
        after.quota_key.as_deref(),
        Some("acme"),
        "same-type continuation must carry the predecessor's quota_key verbatim"
    );
}

/// Cross-type `continue_as_new_as_type`: `quota_key` is RE-RESOLVED against
/// the TARGET type's own declared policy and the new input, exercising the
/// `worker.rs` fix that reads `registry` directly (mirroring
/// `resolve_workflow_concurrency`) rather than the process-global
/// `GLOBAL_WORKFLOW_METADATA` mirror, which this test's `registry()` helper
/// never populates (it uses the raw `HandlerRegistry::new` constructor).
#[tokio::test]
async fn continue_as_new_cross_type_re_resolves_quota_key() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let phase1 = leaked("quota_can_cross_from");
    let phase2 = leaked("quota_can_cross_to");
    let workflow_id = format!("sub-{}", Uuid::new_v4().simple());

    let predecessor = start_root(
        &mut conn,
        phase1,
        &workflow_id,
        serde_json::json!({"next_type": phase2, "tenant_id": "acme"}),
    )
    .await;
    // Predecessor's row carries an unrelated key from a different policy --
    // this must NOT survive the cross-type transition.
    diesel::update(harvest_workflow_executions::table.find(predecessor.as_uuid()))
        .set(harvest_workflow_executions::quota_key.eq(Some("stale-key")))
        .execute(&mut conn)
        .await
        .expect("stamp predecessor quota_key");

    // Phase 2 declares its OWN quota policy directly on the `WorkflowInfo`
    // (not via `GLOBAL_WORKFLOW_METADATA`, which this registry never
    // populates) over the successor's own input shape: `{"phase": "two"}`
    // has no `tenant_id`, so resolve against a field that IS present.
    let mut target = wf_info(phase2, phase_two);
    target.quota = Some(QuotaPolicy::new("phase").with_max_active_executions(9));

    let reg = registry(vec![wf_info(phase1, phase_one), target]);
    let (successor, _) = drive_transition(&url, predecessor, reg, "w-946-cross-type").await;

    let after = load_execution(&mut conn, successor).await;
    assert_eq!(
        after.quota_key.as_deref(),
        Some("two"),
        "the key must be re-resolved from the NEW type's policy against the new \
         input (\"phase\": \"two\" -> resolved key \"two\"), not carried from \
         the predecessor's stale row value"
    );
}

/// Cross-type transition into a type with NO declared quota policy clears
/// the key -- "presence decides", not "inherit unless overridden".
#[tokio::test]
async fn continue_as_new_cross_type_to_no_quota_workflow_clears_quota_key() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let phase1 = leaked("quota_can_clears_from");
    let phase2 = leaked("quota_can_clears_to");
    let workflow_id = format!("sub-{}", Uuid::new_v4().simple());

    let predecessor = start_root(
        &mut conn,
        phase1,
        &workflow_id,
        serde_json::json!({"next_type": phase2}),
    )
    .await;
    diesel::update(harvest_workflow_executions::table.find(predecessor.as_uuid()))
        .set(harvest_workflow_executions::quota_key.eq(Some("acme")))
        .execute(&mut conn)
        .await
        .expect("stamp predecessor quota_key");

    // Phase 2's `WorkflowInfo.quota` is `None` (the `wf_info` default).
    let reg = registry(vec![wf_info(phase1, phase_one), wf_info(phase2, phase_two)]);
    let (successor, _) = drive_transition(&url, predecessor, reg, "w-946-cross-clear").await;

    let after = load_execution(&mut conn, successor).await;
    assert_eq!(
        after.quota_key, None,
        "a target type with no declared quota policy must clear the key, \
         never inherit the predecessor's"
    );
}

// ---------------------------------------------------------------------------
// Issue #946, Codex round-3 review — a `SpawnDetachedChildWorkflow` command
// (issue #347) resolves and enforces the TARGET workflow type's own declared
// quota exactly like a fresh admission, worker-driven end to end.
// `ParentClosePolicy` governs the detached child's *lifecycle*; the target
// type's tenant-quota footprint is an orthogonal concern this exercises.
// ---------------------------------------------------------------------------

fn detached_quota_parent<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let child_type = input["child_type"]
            .as_str()
            .expect("input.child_type must be present")
            .to_string();
        // Leaked once per invocation is fine -- this handler only runs once
        // per test (a detached spawn is a fire-and-forget command, not a
        // suspend point the workflow re-enters).
        let child_type: &'static str = Box::leak(child_type.into_boxed_str());
        ctx.spawn_child_workflow_detached_raw(
            child_type,
            serde_json::json!({"tenant_id": "acme"}),
            ParentClosePolicy::Abandon,
        )
        .map_err(|e| e.to_string())?;
        Ok(serde_json::json!("parent_done"))
    })
}

fn detached_quota_child<'a>(
    _ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(serde_json::json!("child_done")) })
}

/// A detached child spawn (issue #347) whose target type's quota is at cap
/// must not terminally fail the PARENT -- that would seal a healthy
/// execution over an UNRELATED tenant's capacity signal. Instead the
/// parent's own workflow task is parked and woken (`worker.rs`'s
/// `recover_from_child_quota_exceeded`) so a later poll re-drives the
/// identical decision cycle; once the blocking execution frees its slot,
/// the retry succeeds and the detached child is created with the resolved
/// `quota_key` stamped on its own row -- the exact
/// `create_detached_child_executions` code path (issue #946, Codex round-3
/// review).
#[tokio::test]
async fn detached_child_spawn_honors_target_quota_parks_parent_then_succeeds() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let parent_wf_name = leaked("quota_detached_parent");
    let child_wf_name = leaked("quota_detached_child");

    let child_quota_policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1);
    let mut child_info = wf_info(child_wf_name, detached_quota_child);
    child_info.quota = Some(child_quota_policy);

    // Occupy the ONE `max_active_executions` slot for key "acme" with a
    // blocker execution of the SAME target type, started directly (not via
    // the parent) so the quota is already saturated before the parent ever
    // runs. `start_root` drives the BARE admission path
    // (`start_or_load_workflow_execution`), which resolves its quota policy
    // from the process-global `GLOBAL_WORKFLOW_METADATA` mirror -- NOT from
    // any `HandlerRegistry` -- so the blocker's `quota_key` is only stamped
    // correctly while this guard is installed (mirrors the pre-existing
    // `concurrent_runaway_tenant_is_capped_...` test's established pattern).
    let blocker_guard = MetadataGuard::install_one(child_wf_name, child_quota_policy).await;
    let blocker = start_root(
        &mut conn,
        child_wf_name,
        &format!("blocker-{}", Uuid::new_v4().simple()),
        serde_json::json!({"tenant_id": "acme"}),
    )
    .await;
    drop(blocker_guard);

    // The blocker's own `harvest_task_queue` row must never be claimed by the
    // worker below (which necessarily has `detached_quota_child`'s handler
    // registered, so it CAN run the blocker to completion) -- that would free
    // the quota slot almost instantly and defeat the whole test. Delete it so
    // the blocker stays stuck `RUNNING` until `mark_terminal` explicitly
    // seals it further down.
    diesel::sql_query("DELETE FROM harvest_task_queue WHERE workflow_exec_id = $1")
        .bind::<diesel::sql_types::Uuid, _>(blocker.as_uuid())
        .execute(&mut conn)
        .await
        .expect("delete blocker task row");

    let parent = start_root(
        &mut conn,
        parent_wf_name,
        &format!("parent-{}", Uuid::new_v4().simple()),
        serde_json::json!({"child_type": child_wf_name}),
    )
    .await;

    let reg = registry(vec![
        wf_info(parent_wf_name, detached_quota_parent),
        child_info,
    ]);
    let worker = build_runtime_worker("w-946-detached-quota", 2, 1, reg);
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&url));

    // While the blocker still holds the quota slot, the parent's spawn
    // attempt is rejected with `QuotaExceeded`, caught by
    // `recover_from_child_quota_exceeded`, and the parent's own workflow
    // task is parked (never terminally failed) so a later poll retries. The
    // worker polls every 25ms (`runtime_config`'s test default), so a 600ms
    // window is dozens of retry cycles -- ample time to observe the
    // negative assertions below without any explicit "parked" detection.
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;

    let child_row_count_sql =
        "SELECT COUNT(*)::BIGINT AS n FROM harvest_workflow_executions WHERE workflow_name = $1";

    assert_eq!(
        load_execution(&mut conn, parent).await.state,
        "RUNNING",
        "parent must stay RUNNING (parked/retrying) rather than terminally \
         failing over the child target's quota"
    );
    assert_eq!(
        count_rows(&mut conn, child_row_count_sql, &[child_wf_name]).await,
        1, // only the blocker
        "no detached child row should exist while the target quota is at cap"
    );

    // Free the quota slot: the blocker execution goes terminal, so the next
    // reclaim of the parent's parked task succeeds.
    mark_terminal(&mut conn, blocker, "CANCELLED").await;

    wait_for_execution_state(&url, parent, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("worker join");

    // The retried spawn resolved and stamped the tenant key on the child's
    // own row -- the exact code path this test exercises: `quota_key:
    // child_quota_key.as_deref()` in `create_detached_child_executions`.
    #[derive(diesel::QueryableByName)]
    struct ChildRow {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        quota_key: Option<String>,
    }
    let child_row: ChildRow = diesel::sql_query(
        "SELECT quota_key FROM harvest_workflow_executions \
         WHERE workflow_name = $1 AND id != $2",
    )
    .bind::<diesel::sql_types::Text, _>(child_wf_name)
    .bind::<diesel::sql_types::Uuid, _>(blocker.as_uuid())
    .get_result(&mut conn)
    .await
    .expect("detached child row must exist");

    assert_eq!(
        child_row.quota_key.as_deref(),
        Some("acme"),
        "detached child row must carry the tenant key resolved from its OWN \
         declared policy"
    );
    assert_eq!(
        count_rows(&mut conn, child_row_count_sql, &[child_wf_name]).await,
        2, // the (now-cancelled) blocker + the newly-created detached child
        "exactly one detached child should exist once quota capacity freed up"
    );
}

fn detached_quota_mixed_parent<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let child_type = input["child_type"]
            .as_str()
            .expect("input.child_type must be present")
            .to_string();
        let child_type: &'static str = Box::leak(child_type.into_boxed_str());
        ctx.spawn_child_workflow_detached_raw(
            child_type,
            serde_json::json!({"tenant_id": "acme"}),
            ParentClosePolicy::Abandon,
        )
        .map_err(|e| e.to_string())?;
        // Unlike `detached_quota_parent` above, this handler does NOT return
        // immediately after the (synchronous, non-suspending) detached-spawn
        // command -- it also durably suspends on a timer in the SAME decision
        // cycle. The pending-commands buffer for this cycle therefore carries
        // BOTH a `SpawnDetachedChildWorkflow` command and a `StartTimer`
        // command: a MIXED suspension batch, persisted via
        // `handle_suspended_workflow`'s `persist_started_timer` branch (which
        // itself calls `DetachedSpawnPersistence::persist` for the co-batched
        // detached spawn) rather than the terminal-with-commands path the
        // sibling test above exercises. A `QuotaExceeded` surfacing from
        // THAT call is caught by `recover_from_child_quota_exceeded`'s OTHER
        // call site -- `handle_suspended_workflow`'s dispatch tail (issue
        // #946, Codex round-3 review).
        ctx.timer("mixed-batch-tick", 1)
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!("parent_done"))
    })
}

/// A detached child spawn that shares a decision cycle with another
/// suspending command (a mixed batch -- a detached spawn followed by an
/// awaited durable timer before the workflow itself suspends) is persisted
/// via `handle_suspended_workflow`'s dispatch tail, a DIFFERENT
/// `recover_from_child_quota_exceeded` call site from the
/// terminal-with-commands one
/// [`detached_child_spawn_honors_target_quota_parks_parent_then_succeeds`]
/// exercises above. When the detached child's target quota is at cap, this
/// path must ALSO park + wake the parent rather than terminally failing it
/// (issue #946, Codex round-3 review).
#[tokio::test]
async fn detached_child_spawn_in_mixed_batch_honors_target_quota_parks_parent_then_succeeds() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let parent_wf_name = leaked("quota_detached_mixed_parent");
    let child_wf_name = leaked("quota_detached_mixed_child");

    let child_quota_policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1);
    let mut child_info = wf_info(child_wf_name, detached_quota_child);
    child_info.quota = Some(child_quota_policy);

    let blocker_guard = MetadataGuard::install_one(child_wf_name, child_quota_policy).await;
    let blocker = start_root(
        &mut conn,
        child_wf_name,
        &format!("blocker-{}", Uuid::new_v4().simple()),
        serde_json::json!({"tenant_id": "acme"}),
    )
    .await;
    drop(blocker_guard);

    diesel::sql_query("DELETE FROM harvest_task_queue WHERE workflow_exec_id = $1")
        .bind::<diesel::sql_types::Uuid, _>(blocker.as_uuid())
        .execute(&mut conn)
        .await
        .expect("delete blocker task row");

    let parent = start_root(
        &mut conn,
        parent_wf_name,
        &format!("parent-{}", Uuid::new_v4().simple()),
        serde_json::json!({"child_type": child_wf_name}),
    )
    .await;

    let reg = registry(vec![
        wf_info(parent_wf_name, detached_quota_mixed_parent),
        child_info,
    ]);
    let worker = build_runtime_worker("w-946-detached-mixed-quota", 2, 1, reg);
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&url));

    // While the blocker still holds the quota slot, the parent's mixed-batch
    // spawn attempt is rejected with `QuotaExceeded`, caught by
    // `recover_from_child_quota_exceeded`'s `handle_suspended_workflow`
    // dispatch-tail call site, and the parent's own workflow task is parked
    // (never terminally failed) so a later poll retries.
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;

    let child_row_count_sql =
        "SELECT COUNT(*)::BIGINT AS n FROM harvest_workflow_executions WHERE workflow_name = $1";

    assert_eq!(
        load_execution(&mut conn, parent).await.state,
        "RUNNING",
        "parent must stay RUNNING (parked/retrying) rather than terminally \
         failing over the child target's quota"
    );
    assert_eq!(
        count_rows(&mut conn, child_row_count_sql, &[child_wf_name]).await,
        1, // only the blocker
        "no detached child row should exist while the target quota is at cap"
    );

    // Free the quota slot: the blocker execution goes terminal, so the next
    // reclaim of the parent's parked task succeeds and re-drives the SAME
    // mixed-batch decision cycle from the unchanged recorded history.
    mark_terminal(&mut conn, blocker, "CANCELLED").await;

    wait_for_execution_state(&url, parent, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("worker join");

    assert_eq!(
        count_rows(&mut conn, child_row_count_sql, &[child_wf_name]).await,
        2, // the (now-cancelled) blocker + the newly-created detached child
        "exactly one detached child should exist once quota capacity freed up"
    );
}

// ---------------------------------------------------------------------------
// Issue #946, Codex round-4 review — `enforce_quota_admission`'s documented
// "the just-inserted row has appended no events yet" contract was violated
// at all three child-spawn call sites: each appended the child's own
// `WorkflowStarted`/`ChildWorkflowStarted` event BEFORE calling
// `enforce_quota_admission`, so `load_quota_usage`'s `history_bytes` SUM
// counted that just-appended event against the very admission deciding
// whether to allow it -- an off-by-one that could wrongly REJECT a child
// spawn that should have succeeded (usage BEFORE this admission was truly
// zero). Fixed by reordering each site to insert-the-row -> enforce-quota ->
// THEN append the event, matching the plain start path's established
// ordering (`start_or_load_workflow_execution_collect`: `enforce_quota_
// admission` runs before `WorkflowStarted` is appended). A
// `max_history_bytes(1)` cap makes this deterministic: ANY single
// `WorkflowStarted`/`ChildWorkflowStarted` event's `pg_column_size` is
// comfortably over 1 byte, so the pre-fix ordering rejects the spawn on
// EVERY attempt (the parent never completes, hanging the test until
// `wait_for_execution_state`'s 10s bound trips) while the fix admits it on
// the first attempt (zero prior usage for a never-before-seen key, no
// blocker execution needed).
// ---------------------------------------------------------------------------

/// A detached child spawn (`create_detached_child_executions`) must not be
/// wrongly rejected by its OWN just-appended `WorkflowStarted` event
/// counting toward the `history_bytes` admission it is itself part of.
#[tokio::test]
async fn detached_child_spawn_quota_check_excludes_its_own_just_appended_history_bytes() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let parent_wf_name = leaked("quota_detached_hb_parent");
    let child_wf_name = leaked("quota_detached_hb_child");

    let child_quota_policy = QuotaPolicy::new("tenant_id").with_max_history_bytes(1);
    let mut child_info = wf_info(child_wf_name, detached_quota_child);
    child_info.quota = Some(child_quota_policy);

    let parent = start_root(
        &mut conn,
        parent_wf_name,
        &format!("parent-{}", Uuid::new_v4().simple()),
        serde_json::json!({"child_type": child_wf_name}),
    )
    .await;

    let reg = registry(vec![
        wf_info(parent_wf_name, detached_quota_parent),
        child_info,
    ]);
    let worker = build_runtime_worker("w-946-detached-hb", 2, 1, reg);
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&url));

    // No blocker: with zero prior executions for this never-before-seen
    // key, admission must succeed on the FIRST attempt. Before the fix,
    // `history_bytes` counted the just-appended event and rejected every
    // retry forever, hanging here until the 10s bound trips.
    wait_for_execution_state(&url, parent, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("worker join");

    let child_row_count_sql =
        "SELECT COUNT(*)::BIGINT AS n FROM harvest_workflow_executions WHERE workflow_name = $1";
    assert_eq!(
        count_rows(&mut conn, child_row_count_sql, &[child_wf_name]).await,
        1,
        "the detached child must be created on the first attempt despite \
         max_history_bytes(1) -- its own start event must not count against \
         the admission deciding whether to allow it"
    );
}

fn detached_quota_multi_key_parent<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let child_a = input["child_a"]
            .as_str()
            .expect("input.child_a")
            .to_string();
        let child_b = input["child_b"]
            .as_str()
            .expect("input.child_b")
            .to_string();
        let child_a: &'static str = Box::leak(child_a.into_boxed_str());
        let child_b: &'static str = Box::leak(child_b.into_boxed_str());
        // Four detached-spawn commands run in ONE decision cycle, across
        // TWO workflow types and TWO tenant keys. `(child_a, "acme")` and
        // `(child_b, "acme")` share a key STRING. They are still distinct
        // `(workflow_name, quota_key)` pairs. `(child_a, "acme")` and
        // `(child_a, "beta")` share a workflow type but differ by key.
        // Both dimensions must dedup and sort correctly in the
        // pre-acquisition `BTreeSet`. A lone spawn degenerates to a single
        // pair and never exercises this.
        for (child_type, tenant) in [
            (child_a, "acme"),
            (child_a, "beta"),
            (child_b, "acme"),
            (child_b, "beta"),
        ] {
            ctx.spawn_child_workflow_detached_raw(
                child_type,
                serde_json::json!({"tenant_id": tenant}),
                ParentClosePolicy::Abandon,
            )
            .map_err(|e| e.to_string())?;
        }
        Ok(serde_json::json!("parent_done"))
    })
}

/// Issue #1228, Finding 2 regression: a batch with MULTIPLE detached-spawn
/// commands, across two workflow types and two tenant keys. It must lock
/// every distinct `(workflow_name, quota_key)` pair in the new
/// pre-acquisition pass. It must still admit every child.
///
/// The pre-existing detached-quota tests above each spawn exactly one
/// child. Their pre-acquisition `BTreeSet` degenerates to a single pair.
/// This test exercises its dedup and sort over several pairs instead.
#[tokio::test]
async fn detached_child_multi_spawn_batch_locks_every_distinct_key_and_admits_all() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let parent_wf_name = leaked("quota_detached_multikey_parent");
    let child_alpha_name = leaked("quota_detached_multikey_child_a");
    let child_beta_name = leaked("quota_detached_multikey_child_b");

    // Generous caps -- this test is about lock coverage, not rejection.
    let quota_policy = QuotaPolicy::new("tenant_id").with_max_active_executions(10);
    let mut child_alpha_info = wf_info(child_alpha_name, detached_quota_child);
    child_alpha_info.quota = Some(quota_policy);
    let mut child_beta_info = wf_info(child_beta_name, detached_quota_child);
    child_beta_info.quota = Some(quota_policy);

    let parent = start_root(
        &mut conn,
        parent_wf_name,
        &format!("parent-{}", Uuid::new_v4().simple()),
        serde_json::json!({"child_a": child_alpha_name, "child_b": child_beta_name}),
    )
    .await;

    let reg = registry(vec![
        wf_info(parent_wf_name, detached_quota_multi_key_parent),
        child_alpha_info,
        child_beta_info,
    ]);
    let worker = build_runtime_worker("w-1228-detached-multikey", 2, 1, reg);
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&url));

    // A longer bound than the usual 10s default. This decision cycle does
    // FOUR lock acquisitions and four inserts, not one. It needs more
    // margin under a busy CI runner. This mirrors
    // `wait_for_execution_state_with_timeout`'s own documented reason for
    // existing.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        if load_execution(&mut conn, parent).await.state == "COMPLETED" {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "parent must reach COMPLETED within 30s"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    worker.shutdown();
    handle.await.expect("worker join");

    #[derive(diesel::QueryableByName, Debug, PartialEq, Eq)]
    struct ChildRow {
        #[diesel(sql_type = diesel::sql_types::Text)]
        workflow_name: String,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        quota_key: Option<String>,
    }
    let rows: Vec<ChildRow> = diesel::sql_query(
        "SELECT workflow_name, quota_key FROM harvest_workflow_executions \
         WHERE workflow_name = $1 OR workflow_name = $2",
    )
    .bind::<diesel::sql_types::Text, _>(child_alpha_name)
    .bind::<diesel::sql_types::Text, _>(child_beta_name)
    .load(&mut conn)
    .await
    .expect("load children");

    assert_eq!(
        rows.len(),
        4,
        "all four detached children, across two types and two keys, must be \
         created -- got {rows:?}"
    );
    for (name, key) in [
        (child_alpha_name, "acme"),
        (child_alpha_name, "beta"),
        (child_beta_name, "acme"),
        (child_beta_name, "beta"),
    ] {
        assert!(
            rows.contains(&ChildRow {
                workflow_name: name.to_string(),
                quota_key: Some(key.to_string()),
            }),
            "expected a child of type {name} keyed {key} -- got {rows:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Issue #946, Codex round-3 review — an AWAITED child spawn (`ctx.
// spawn_child_workflow_raw`, whether a lone spawn or one of a genuine
// fan-out, both dispatch through the same `persist_all_started_child_workflows`
// suspension-batch handler) resolves and enforces the TARGET workflow type's
// own declared quota, exactly like the detached-spawn path above, worker-
// driven end to end.
// ---------------------------------------------------------------------------

fn awaited_quota_parent<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let child_type = input["child_type"]
            .as_str()
            .expect("input.child_type must be present")
            .to_string();
        let output = ctx
            .spawn_child_workflow_raw(&child_type, serde_json::json!({"tenant_id": "acme"}))
            .await
            .map_err(|e| e.to_string())?;
        Ok(output)
    })
}

fn awaited_quota_child<'a>(
    _ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(serde_json::json!("awaited_child_done")) })
}

/// An awaited child spawn (`persist_all_started_child_workflows`) whose
/// target type's quota is at cap must not terminally fail the PARENT.
/// Mirrors [`detached_child_spawn_honors_target_quota_parks_parent_then_succeeds`]
/// exactly, but exercises the DIFFERENT dispatch function that handles a
/// `StartChildWorkflow` suspension batch (the local `match` arm inline in
/// `persist_all_started_child_workflows`, not the shared
/// `recover_from_child_quota_exceeded` helper).
#[tokio::test]
async fn awaited_child_spawn_honors_target_quota_parks_parent_then_succeeds() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let parent_wf_name = leaked("quota_awaited_parent");
    let child_wf_name = leaked("quota_awaited_child");

    let child_quota_policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1);
    let mut child_info = wf_info(child_wf_name, awaited_quota_child);
    child_info.quota = Some(child_quota_policy);

    // Occupy the ONE `max_active_executions` slot for key "acme" with a
    // blocker of the SAME target type -- see the detached-spawn test above
    // for why the `MetadataGuard` install and the task-row deletion are both
    // required for a correct blocker.
    let blocker_guard = MetadataGuard::install_one(child_wf_name, child_quota_policy).await;
    let blocker = start_root(
        &mut conn,
        child_wf_name,
        &format!("blocker-{}", Uuid::new_v4().simple()),
        serde_json::json!({"tenant_id": "acme"}),
    )
    .await;
    drop(blocker_guard);
    diesel::sql_query("DELETE FROM harvest_task_queue WHERE workflow_exec_id = $1")
        .bind::<diesel::sql_types::Uuid, _>(blocker.as_uuid())
        .execute(&mut conn)
        .await
        .expect("delete blocker task row");

    let parent = start_root(
        &mut conn,
        parent_wf_name,
        &format!("parent-{}", Uuid::new_v4().simple()),
        serde_json::json!({"child_type": child_wf_name}),
    )
    .await;
    // Captured before the worker starts, so a retry cycle's own stamp is
    // provably distinguishable from this pre-worker value (Finding 1 check
    // below).
    let parent_pre_worker_scheduled_at = task_queue_state(&mut conn, parent).await.scheduled_at;

    let reg = registry(vec![
        wf_info(parent_wf_name, awaited_quota_parent),
        child_info,
    ]);
    let worker = build_runtime_worker("w-946-awaited-quota", 2, 1, reg);
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&url));

    // While the blocker still holds the quota slot, the parent's spawn
    // attempt inside `persist_all_started_child_workflows` is rejected with
    // `QuotaExceeded`, and the WHOLE transaction (including the parent's own
    // `ChildWorkflowStarted` append) rolls back -- so the parent never even
    // reaches a parked-on-child-completion state; it stays exactly where it
    // started, with the decision cycle retried on every subsequent poll.
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;

    let child_row_count_sql =
        "SELECT COUNT(*)::BIGINT AS n FROM harvest_workflow_executions WHERE workflow_name = $1";

    assert_eq!(
        load_execution(&mut conn, parent).await.state,
        "RUNNING",
        "parent must stay RUNNING (parked/retrying) rather than terminally \
         failing over the child target's quota"
    );
    assert_eq!(
        count_rows(&mut conn, child_row_count_sql, &[child_wf_name]).await,
        1, // only the blocker
        "no child row should exist while the target quota is at cap"
    );

    // Issue #1227 Finding 1: the local `QuotaExceeded` catch in
    // `persist_all_started_child_workflows` used to park + immediately wake
    // the parent's task -- a zero-delay retry loop. Now routed through
    // `recover_from_child_quota_exceeded`'s bounded jittered backoff, so
    // while the blocker still holds the slot, a completed retry cycle's
    // `scheduled_at` must sit in the future, not be immediately claimable.
    let (retried_scheduled_at, observed_now) =
        task_scheduled_at_after_a_retry_cycle(&mut conn, parent, parent_pre_worker_scheduled_at)
            .await;
    assert!(
        retried_scheduled_at > observed_now,
        "a QuotaExceeded catch that hot-spins (park + immediate wake) never \
         advances scheduled_at into the future; the bounded-backoff requeue \
         must"
    );

    // Free the quota slot.
    mark_terminal(&mut conn, blocker, "CANCELLED").await;

    wait_for_execution_state(&url, parent, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("worker join");

    let final_state = load_execution(&mut conn, parent).await;
    assert_eq!(
        final_state.output,
        Some(serde_json::json!("awaited_child_done")),
        "parent's spawn_child_workflow_raw().await must resolve to the \
         child's real completed output once the retry succeeds"
    );

    #[derive(diesel::QueryableByName)]
    struct ChildRow {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        quota_key: Option<String>,
    }
    let child_row: ChildRow = diesel::sql_query(
        "SELECT quota_key FROM harvest_workflow_executions \
         WHERE workflow_name = $1 AND id != $2",
    )
    .bind::<diesel::sql_types::Text, _>(child_wf_name)
    .bind::<diesel::sql_types::Uuid, _>(blocker.as_uuid())
    .get_result(&mut conn)
    .await
    .expect("awaited child row must exist");

    assert_eq!(
        child_row.quota_key.as_deref(),
        Some("acme"),
        "awaited child row must carry the tenant key resolved from its OWN \
         declared policy"
    );
    assert_eq!(
        count_rows(&mut conn, child_row_count_sql, &[child_wf_name]).await,
        2, // the (now-cancelled) blocker + the newly-created awaited child
        "exactly one child should exist once quota capacity freed up"
    );
}

/// An awaited child spawn (`persist_all_started_child_workflows`) must not
/// be wrongly rejected by its OWN just-appended `WorkflowStarted` event
/// counting toward the `history_bytes` admission it is itself part of
/// (issue #946, Codex round-4 review — see the section comment above
/// [`detached_child_spawn_quota_check_excludes_its_own_just_appended_history_bytes`]
/// for the full rationale).
#[tokio::test]
async fn awaited_child_spawn_quota_check_excludes_its_own_just_appended_history_bytes() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let parent_wf_name = leaked("quota_awaited_hb_parent");
    let child_wf_name = leaked("quota_awaited_hb_child");

    let child_quota_policy = QuotaPolicy::new("tenant_id").with_max_history_bytes(1);
    let mut child_info = wf_info(child_wf_name, awaited_quota_child);
    child_info.quota = Some(child_quota_policy);

    let parent = start_root(
        &mut conn,
        parent_wf_name,
        &format!("parent-{}", Uuid::new_v4().simple()),
        serde_json::json!({"child_type": child_wf_name}),
    )
    .await;

    let reg = registry(vec![
        wf_info(parent_wf_name, awaited_quota_parent),
        child_info,
    ]);
    let worker = build_runtime_worker("w-946-awaited-hb", 2, 1, reg);
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&url));

    // No blocker: with zero prior executions for this never-before-seen
    // key, admission must succeed on the FIRST attempt.
    let final_state = wait_for_execution_state(&url, parent, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("worker join");

    assert_eq!(
        final_state.output,
        Some(serde_json::json!("awaited_child_done")),
        "the parent's spawn_child_workflow_raw().await must resolve to the \
         child's real completed output on the first attempt"
    );
    let child_row_count_sql =
        "SELECT COUNT(*)::BIGINT AS n FROM harvest_workflow_executions WHERE workflow_name = $1";
    assert_eq!(
        count_rows(&mut conn, child_row_count_sql, &[child_wf_name]).await,
        1,
        "the awaited child must be created on the first attempt despite \
         max_history_bytes(1) -- its own start event must not count against \
         the admission deciding whether to allow it"
    );
}

// ---------------------------------------------------------------------------
// Issue #946, Codex round-3 review — the child-timeout-race primitive
// (`ctx.spawn_child_workflow_timeout`, issue #779) dispatches through
// `persist_child_timeout_race` -> `insert_awaited_child_execution`, a THIRD
// distinct child-spawn code path from the two above (fan-out and detached).
// Its own local `QuotaExceeded` catch must likewise park rather than fail
// the parent.
// ---------------------------------------------------------------------------

fn child_timeout_race_quota_parent<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let child_type = input["child_type"]
            .as_str()
            .expect("input.child_type must be present")
            .to_string();
        // A generous 600s deadline: this test only needs to prove the CHILD
        // branch wins once quota capacity frees up, never the timeout branch.
        let outcome = ctx
            .spawn_child_workflow_timeout(
                &child_type,
                serde_json::json!({"tenant_id": "acme"}),
                std::time::Duration::from_secs(600),
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"child_won": outcome.is_some(), "value": outcome}))
    })
}

fn child_timeout_race_quota_child<'a>(
    _ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(serde_json::json!("race_child_done")) })
}

/// A child-timeout-race child spawn (`persist_child_timeout_race` ->
/// `insert_awaited_child_execution`) whose target type's quota is at cap
/// must not terminally fail the PARENT. Same shape as the two tests above,
/// exercising the third and last distinct dispatch function that can create
/// a child execution row.
#[tokio::test]
async fn child_timeout_race_spawn_honors_target_quota_parks_parent_then_succeeds() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let parent_wf_name = leaked("quota_race_parent");
    let child_wf_name = leaked("quota_race_child");

    let child_quota_policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1);
    let mut child_info = wf_info(child_wf_name, child_timeout_race_quota_child);
    child_info.quota = Some(child_quota_policy);

    let blocker_guard = MetadataGuard::install_one(child_wf_name, child_quota_policy).await;
    let blocker = start_root(
        &mut conn,
        child_wf_name,
        &format!("blocker-{}", Uuid::new_v4().simple()),
        serde_json::json!({"tenant_id": "acme"}),
    )
    .await;
    drop(blocker_guard);
    diesel::sql_query("DELETE FROM harvest_task_queue WHERE workflow_exec_id = $1")
        .bind::<diesel::sql_types::Uuid, _>(blocker.as_uuid())
        .execute(&mut conn)
        .await
        .expect("delete blocker task row");

    let parent = start_root(
        &mut conn,
        parent_wf_name,
        &format!("parent-{}", Uuid::new_v4().simple()),
        serde_json::json!({"child_type": child_wf_name}),
    )
    .await;
    // Captured before the worker starts -- see Finding 1's identical comment
    // above.
    let parent_pre_worker_scheduled_at = task_queue_state(&mut conn, parent).await.scheduled_at;

    let reg = registry(vec![
        wf_info(parent_wf_name, child_timeout_race_quota_parent),
        child_info,
    ]);
    let worker = build_runtime_worker("w-946-race-quota", 2, 1, reg);
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&url));

    // While the blocker holds the quota slot, `insert_awaited_child_execution`
    // rejects with `QuotaExceeded` inside `persist_child_timeout_race`'s
    // transaction; the whole transaction (child row, timer row, parent event
    // appends) rolls back and the parent's task is parked, never terminally
    // failed.
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;

    let child_row_count_sql =
        "SELECT COUNT(*)::BIGINT AS n FROM harvest_workflow_executions WHERE workflow_name = $1";

    assert_eq!(
        load_execution(&mut conn, parent).await.state,
        "RUNNING",
        "parent must stay RUNNING (parked/retrying) rather than terminally \
         failing over the child target's quota"
    );
    assert_eq!(
        count_rows(&mut conn, child_row_count_sql, &[child_wf_name]).await,
        1, // only the blocker
        "no child row should exist while the target quota is at cap"
    );

    // Issue #1227 Finding 2: same hot-spin bug as Finding 1, in the
    // child-timeout-race spawn path's local `QuotaExceeded` catch. Now routed
    // through the same bounded-backoff helper, so a completed retry cycle's
    // `scheduled_at` must sit in the future while the blocker still holds the
    // slot.
    let (retried_scheduled_at, observed_now) =
        task_scheduled_at_after_a_retry_cycle(&mut conn, parent, parent_pre_worker_scheduled_at)
            .await;
    assert!(
        retried_scheduled_at > observed_now,
        "a QuotaExceeded catch that hot-spins (park + immediate wake) never \
         advances scheduled_at into the future; the bounded-backoff requeue \
         must"
    );

    // Free the quota slot.
    mark_terminal(&mut conn, blocker, "CANCELLED").await;

    wait_for_execution_state(&url, parent, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("worker join");

    let final_state = load_execution(&mut conn, parent).await;
    assert_eq!(
        final_state.output,
        Some(serde_json::json!({"child_won": true, "value": "race_child_done"})),
        "parent's spawn_child_workflow_timeout().await must resolve on the \
         CHILD branch (not the 600s deadline) once the retry succeeds"
    );

    #[derive(diesel::QueryableByName)]
    struct ChildRow {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        quota_key: Option<String>,
    }
    let child_row: ChildRow = diesel::sql_query(
        "SELECT quota_key FROM harvest_workflow_executions \
         WHERE workflow_name = $1 AND id != $2",
    )
    .bind::<diesel::sql_types::Text, _>(child_wf_name)
    .bind::<diesel::sql_types::Uuid, _>(blocker.as_uuid())
    .get_result(&mut conn)
    .await
    .expect("race child row must exist");

    assert_eq!(
        child_row.quota_key.as_deref(),
        Some("acme"),
        "race child row must carry the tenant key resolved from its OWN \
         declared policy"
    );
    assert_eq!(
        count_rows(&mut conn, child_row_count_sql, &[child_wf_name]).await,
        2, // the (now-cancelled) blocker + the newly-created race child
        "exactly one child should exist once quota capacity freed up"
    );
}

/// A child-timeout-race child spawn (`persist_child_timeout_race` ->
/// `insert_awaited_child_execution`) must not be wrongly rejected by its OWN
/// just-appended `ChildWorkflowStarted`/`WorkflowStarted` events counting
/// toward the `history_bytes` admission it is itself part of (issue #946,
/// Codex round-4 review — see the section comment above
/// [`detached_child_spawn_quota_check_excludes_its_own_just_appended_history_bytes`]
/// for the full rationale).
#[tokio::test]
async fn child_timeout_race_spawn_quota_check_excludes_its_own_just_appended_history_bytes() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let parent_wf_name = leaked("quota_race_hb_parent");
    let child_wf_name = leaked("quota_race_hb_child");

    let child_quota_policy = QuotaPolicy::new("tenant_id").with_max_history_bytes(1);
    let mut child_info = wf_info(child_wf_name, child_timeout_race_quota_child);
    child_info.quota = Some(child_quota_policy);

    let parent = start_root(
        &mut conn,
        parent_wf_name,
        &format!("parent-{}", Uuid::new_v4().simple()),
        serde_json::json!({"child_type": child_wf_name}),
    )
    .await;

    let reg = registry(vec![
        wf_info(parent_wf_name, child_timeout_race_quota_parent),
        child_info,
    ]);
    let worker = build_runtime_worker("w-946-race-hb", 2, 1, reg);
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&url));

    // No blocker: with zero prior executions for this never-before-seen
    // key, admission must succeed on the FIRST attempt.
    let final_state = wait_for_execution_state(&url, parent, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("worker join");

    assert_eq!(
        final_state.output,
        Some(serde_json::json!({"child_won": true, "value": "race_child_done"})),
        "parent's spawn_child_workflow_timeout().await must resolve on the \
         CHILD branch on the first attempt"
    );
    let child_row_count_sql =
        "SELECT COUNT(*)::BIGINT AS n FROM harvest_workflow_executions WHERE workflow_name = $1";
    assert_eq!(
        count_rows(&mut conn, child_row_count_sql, &[child_wf_name]).await,
        1,
        "the race child must be created on the first attempt despite \
         max_history_bytes(1) -- its own start events must not count \
         against the admission deciding whether to allow it"
    );
}

fn mixed_batch_quota_noop_activity(
    _ctx: &ActivityContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send>> {
    Box::pin(async move { Ok(serde_json::json!({"noop": true})) })
}

fn mixed_batch_quota_parent<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let child_type = input["child_type"]
            .as_str()
            .expect("input.child_type must be present")
            .to_string();
        // "activity x child" -- neither `extract_child_timeout_race` (child +
        // TIMER only) nor `extract_all_started_child_workflows` (every
        // command must be a child start) matches this shape, so it falls
        // through to `extract_mixed_suspension_batch` ->
        // `persist_mixed_suspension_batch` (issue #950), the third
        // `QuotaExceeded` catch site issue #1227's initial fix missed.
        let winner = ctx
            .race()
            .activity_raw(
                "mixed_batch_quota_noop_activity",
                serde_json::json!({}),
                "default",
            )
            .label("work")
            .child_workflow_raw(&child_type, serde_json::json!({"tenant_id": "acme"}))
            .label("child")
            .run()
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"label": winner.label}))
    })
}

fn mixed_batch_quota_child<'a>(
    _ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(serde_json::json!("mixed_child_done")) })
}

/// Issue #1227 follow-up sweep: a THIRD `worker.rs` `QuotaExceeded` catch
/// site (`persist_mixed_suspension_batch`, reached for a heterogeneous
/// "activity x child" suspension batch -- issue #950) had the identical
/// park-then-immediately-wake hot-spin bug as the two sites the issue itself
/// named, but was missed by the initial fix because its own comment called it
/// a "mirror" of those two without anyone checking it was actually routed
/// through the shared backoff helper.
#[tokio::test]
async fn mixed_batch_child_spawn_honors_target_quota_parks_parent_then_succeeds() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let parent_wf_name = leaked("quota_mixed_batch_parent");
    let child_wf_name = leaked("quota_mixed_batch_child");

    let child_quota_policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1);
    let mut child_info = wf_info(child_wf_name, mixed_batch_quota_child);
    child_info.quota = Some(child_quota_policy);

    // Occupy the ONE `max_active_executions` slot for key "acme" -- see the
    // detached-spawn test above for why the `MetadataGuard` install and the
    // task-row deletion are both required for a correct blocker.
    let blocker_guard = MetadataGuard::install_one(child_wf_name, child_quota_policy).await;
    let blocker = start_root(
        &mut conn,
        child_wf_name,
        &format!("blocker-{}", Uuid::new_v4().simple()),
        serde_json::json!({"tenant_id": "acme"}),
    )
    .await;
    drop(blocker_guard);
    diesel::sql_query("DELETE FROM harvest_task_queue WHERE workflow_exec_id = $1")
        .bind::<diesel::sql_types::Uuid, _>(blocker.as_uuid())
        .execute(&mut conn)
        .await
        .expect("delete blocker task row");

    let parent = start_root(
        &mut conn,
        parent_wf_name,
        &format!("parent-{}", Uuid::new_v4().simple()),
        serde_json::json!({"child_type": child_wf_name}),
    )
    .await;
    let parent_pre_worker_scheduled_at = task_queue_state(&mut conn, parent).await.scheduled_at;

    let reg = Arc::new(HandlerRegistry::new(
        vec![
            wf_info(parent_wf_name, mixed_batch_quota_parent),
            child_info,
        ],
        vec![act_info(
            "mixed_batch_quota_noop_activity",
            mixed_batch_quota_noop_activity,
        )],
    ));
    let worker = build_runtime_worker("w-1227-mixed-batch-quota", 2, 1, reg);
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&url));

    // While the blocker still holds the quota slot, `persist_mixed_suspension_batch`'s
    // attempt to start the child is rejected with `QuotaExceeded`, and the
    // WHOLE transaction (including the co-batched activity dispatch) rolls
    // back -- so the parent never even reaches a parked-on-branch-completion
    // state; it stays exactly where it started, with the decision cycle
    // retried on every subsequent poll.
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;

    let child_row_count_sql =
        "SELECT COUNT(*)::BIGINT AS n FROM harvest_workflow_executions WHERE workflow_name = $1";

    assert_eq!(
        load_execution(&mut conn, parent).await.state,
        "RUNNING",
        "parent must stay RUNNING (parked/retrying) rather than terminally \
         failing over the child target's quota"
    );
    assert_eq!(
        count_rows(&mut conn, child_row_count_sql, &[child_wf_name]).await,
        1, // only the blocker
        "no child row should exist while the target quota is at cap"
    );

    // The regression check: a completed retry cycle's `scheduled_at` must sit
    // in the future, not be immediately claimable -- the hot-spin bug's exact
    // opposite.
    let (retried_scheduled_at, observed_now) =
        task_scheduled_at_after_a_retry_cycle(&mut conn, parent, parent_pre_worker_scheduled_at)
            .await;
    assert!(
        retried_scheduled_at > observed_now,
        "a QuotaExceeded catch that hot-spins (park + immediate wake) never \
         advances scheduled_at into the future; the bounded-backoff requeue \
         must"
    );

    // Free the quota slot.
    mark_terminal(&mut conn, blocker, "CANCELLED").await;

    wait_for_execution_state(&url, parent, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("worker join");

    assert_eq!(
        count_rows(&mut conn, child_row_count_sql, &[child_wf_name]).await,
        2, // the (now-cancelled) blocker + the newly-created child
        "exactly one child should exist once quota capacity freed up"
    );
}

// ---------------------------------------------------------------------------
// Success metric — the issue's own runaway-tenant scenario, driven with
// genuine concurrency (not the sequential admission loop
// `active_executions_cap_admits_exactly_n_then_rejects_the_next` already
// covers above).
// ---------------------------------------------------------------------------

/// Issue #946's success metric: "tenant A submits [a burst of] starts against
/// `max_active_executions=N` while tenant B operates normally: tenant A
/// capped at exactly N active executions with 100% of overflow starts
/// receiving typed 429 [here: the typed `QuotaExceeded` `Err` the HTTP layer
/// maps to 429]; tenant B's start... success rate unchanged".
///
/// Scaled down from the issue's literal 10,000/100 for CI runtime (the
/// admission-time behaviour under concurrency does not change with scale —
/// the SQL-level advisory lock + indexed count this test exercises is the
/// same code path regardless of burst size), but driven with GENUINE
/// concurrency: every attempt races against every other attempt on its own
/// connection via `tokio::spawn`, not a sequential loop.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
// `tenant_a_*`/`tenant_b_*` are deliberately parallel-named (the whole point
// of the test is a side-by-side comparison of the two tenants' outcomes) --
// renaming them to satisfy clippy's Levenshtein-distance heuristic would
// make the assertions below harder to read, not clearer.
#[allow(clippy::similar_names)]
async fn concurrent_runaway_tenant_is_capped_while_a_second_tenant_is_unaffected() {
    const CAP: usize = 20;
    const OVERFLOW_ATTEMPTS: usize = 60; // total burst >> cap, guarantees rejections
    const TENANT_B_ATTEMPTS: usize = 15; // a well-behaved sibling tenant, unaffected

    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf = leaked("quota_runaway");

    let policy = QuotaPolicy::new("tenant_id")
        .with_max_active_executions(u32::try_from(CAP).expect("CAP fits in u32"));
    let _guard = MetadataGuard::install_one(wf, policy).await;

    // Tenant A: a burst of concurrent starts, all sharing one quota key.
    let mut tasks = Vec::with_capacity(OVERFLOW_ATTEMPTS + TENANT_B_ATTEMPTS);
    for _ in 0..OVERFLOW_ATTEMPTS {
        let url = url.clone();
        tasks.push(tokio::spawn(async move {
            let mut conn = connect(&url).await;
            let workflow_id = format!("wid-{}", Uuid::new_v4().simple());
            try_start(
                &mut conn,
                wf,
                &workflow_id,
                serde_json::json!({"tenant_id": "runaway-tenant"}),
            )
            .await
            .1
        }));
    }
    // Tenant B: a small, well-behaved concurrent burst on a DIFFERENT quota
    // key of the SAME workflow type, interleaved with tenant A's storm so it
    // genuinely races against the saturated key rather than running before
    // or after it.
    for _ in 0..TENANT_B_ATTEMPTS {
        let url = url.clone();
        tasks.push(tokio::spawn(async move {
            let mut conn = connect(&url).await;
            let workflow_id = format!("wid-{}", Uuid::new_v4().simple());
            try_start(
                &mut conn,
                wf,
                &workflow_id,
                serde_json::json!({"tenant_id": "well-behaved-tenant"}),
            )
            .await
            .1
        }));
    }

    let mut tenant_a_ok = 0usize;
    let mut tenant_a_rejected = 0usize;
    let mut tenant_b_ok = 0usize;
    let mut tenant_b_rejected = 0usize;
    for (i, task) in tasks.into_iter().enumerate() {
        let outcome = task.await.expect("spawned start task must not panic");
        let is_tenant_a = i < OVERFLOW_ATTEMPTS;
        match outcome {
            Ok(_) => {
                if is_tenant_a {
                    tenant_a_ok += 1;
                } else {
                    tenant_b_ok += 1;
                }
            }
            Err(HarvestError::QuotaExceeded {
                key,
                resource,
                limit,
                ..
            }) => {
                assert_eq!(
                    resource,
                    QuotaResource::ActiveExecutions,
                    "the only declared cap is active_executions"
                );
                assert_eq!(limit, u64::try_from(CAP).expect("CAP fits in u64"));
                if is_tenant_a {
                    assert_eq!(key, "runaway-tenant");
                    tenant_a_rejected += 1;
                } else {
                    // Tenant B never has a policy of its own key saturated —
                    // if it were ever rejected it would prove cross-tenant
                    // bleed, which the assertions below independently rule
                    // out via `tenant_b_ok == TENANT_B_ATTEMPTS`.
                    tenant_b_rejected += 1;
                }
            }
            Err(e) => panic!("unexpected error kind: {e:?}"),
        }
    }

    // 100% of tenant A's overflow burst was either admitted (up to the cap)
    // or received the typed rejection — never anything else, never silently
    // dropped or a generic 500-class error.
    assert_eq!(
        tenant_a_ok + tenant_a_rejected,
        OVERFLOW_ATTEMPTS,
        "every tenant-A attempt must resolve to exactly one of admitted/rejected"
    );
    assert_eq!(
        tenant_a_ok, CAP,
        "tenant A must be capped at EXACTLY the declared limit, not fewer \
         (a false rejection under contention) and not more (a lost-update \
         race past the advisory-lock admission check)"
    );
    assert_eq!(
        tenant_a_rejected,
        OVERFLOW_ATTEMPTS - CAP,
        "every overflow start beyond the cap must receive the typed 429-mapped rejection"
    );

    // Tenant B's success rate is completely unaffected by tenant A's
    // concurrent saturation — the isolation the issue's success metric
    // requires (a different key on the same workflow type, not merely a
    // different workflow type, so this proves key-level isolation under
    // real contention, not just type-level isolation).
    assert_eq!(
        tenant_b_ok, TENANT_B_ATTEMPTS,
        "tenant B (a different quota key) must see a 100% success rate \
         while tenant A's key is saturated by a concurrent burst"
    );
    assert_eq!(tenant_b_rejected, 0);

    // The persisted state agrees with the in-flight admission decisions —
    // "capped, not merely slowed" (mirrors the sequential test's own
    // invariant, now proven to hold under genuine concurrent contention).
    assert_eq!(
        active_count(&mut conn, wf, "runaway-tenant").await,
        i64::try_from(CAP).expect("CAP fits in i64")
    );
    assert_eq!(
        active_count(&mut conn, wf, "well-behaved-tenant").await,
        i64::try_from(TENANT_B_ATTEMPTS).expect("TENANT_B_ATTEMPTS fits in i64")
    );
}

// ---------------------------------------------------------------------------
// P1 regression -- `replace_execution` is a SECOND row-creation branch
// inside `start_or_load_workflow_execution_collect`'s transaction (reached
// by `AllowDuplicateFailedOnly`/`TerminateIfRunning`/a conflict-driven
// `Terminate`), and it originally bypassed quota enforcement entirely.
// Every test above exercises only the `on_conflict_do_nothing()`
// fresh-insert branch (via `AllowDuplicate`, the default reuse policy) --
// none of them would have caught this. Each test below targets exactly one
// of the three `replace_execution` call sites and would have FAILED before
// the fix (the replace silently succeeded instead of being rejected).
// ---------------------------------------------------------------------------

/// Site 2 (`AllowDuplicateFailedOnly` over a FAILED prior): resurrecting a
/// terminal row into a fresh ACTIVE execution is a pure net **+1** to the
/// key's active population (the terminal prior contributed zero before the
/// replace, so nothing offsets the new row) -- looped across N distinct
/// `workflow_id`s, each with its own already-failed prior, this is the
/// concrete "accumulate unbounded active executions well past the declared
/// cap" vector review agent 1 identified.
#[tokio::test]
async fn replace_execution_allow_duplicate_failed_only_enforces_quota_on_resurrection() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let wf = leaked("quota_replace_afo");

    // 1. No quota policy installed yet -- an unconstrained start, then a
    //    terminal failure (an ordinary completed-with-failure run).
    let workflow_id = format!("wid-{}", Uuid::new_v4().simple());
    let (exec_id, outcome) = try_start(
        &mut conn,
        wf,
        &workflow_id,
        serde_json::json!({"tenant_id": "t1"}),
    )
    .await;
    outcome.expect("initial start (no policy yet) must succeed");
    mark_terminal(&mut conn, exec_id, "FAILED").await;

    // 2. NOW install a quota policy whose cap is ALREADY saturated by an
    //    unrelated, distinct-`workflow_id` execution -- so any further
    //    active admission for key "t1" (including a resurrection of the
    //    just-failed row above) must be rejected.
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1);
    let _guard = MetadataGuard::install_one(wf, policy).await;
    start_ok(&mut conn, wf, serde_json::json!({"tenant_id": "t1"})).await;
    assert_eq!(active_count(&mut conn, wf, "t1").await, 1);

    // 3. Before the P1 fix, `replace_execution` (reached here via
    //    `AllowDuplicateFailedOnly` over a FAILED prior) never called
    //    `enforce_quota_admission` at all -- this would have silently
    //    resurrected the failed row into a fresh ACTIVE execution.
    let exec_id2 = ExecutionId::new();
    let mut p = params(
        wf,
        &workflow_id,
        exec_id2,
        serde_json::json!({"tenant_id": "t1"}),
    );
    p.reuse_policy = WorkflowIdReusePolicy::AllowDuplicateFailedOnly;
    let outcome2 = start_or_load_workflow_execution(&mut conn, p, None).await;

    let err = outcome2.expect_err(
        "resurrecting a FAILED row into a fresh active execution must still \
         be quota-checked, exactly like any other admission",
    );
    assert_quota_exceeded(&err, wf, "t1", QuotaResource::ActiveExecutions, 1, |c| {
        c == 1
    });

    // Rolled back atomically: no phantom row for the rejected attempt, the
    // original row is STILL FAILED (never resurrected), and the key's
    // active population is untouched.
    assert_no_execution_row(&mut conn, exec_id2).await;
    assert_eq!(row_state(&mut conn, exec_id).await, "FAILED");
    assert_eq!(active_count(&mut conn, wf, "t1").await, 1);
}

/// Site 3 (`TerminateIfRunning` over a genuinely TERMINAL prior, e.g.
/// COMPLETED): the identical bypass shape as Site 2 above, reached through
/// the OTHER reuse policy that routes a terminal-existing row into
/// `replace_execution`.
#[tokio::test]
async fn replace_execution_terminate_if_running_enforces_quota_over_a_terminal_prior() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let wf = leaked("quota_replace_tir_terminal");

    let workflow_id = format!("wid-{}", Uuid::new_v4().simple());
    let (exec_id, outcome) = try_start(
        &mut conn,
        wf,
        &workflow_id,
        serde_json::json!({"tenant_id": "t1"}),
    )
    .await;
    outcome.expect("initial start (no policy yet) must succeed");
    mark_terminal(&mut conn, exec_id, "COMPLETED").await;

    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1);
    let _guard = MetadataGuard::install_one(wf, policy).await;
    start_ok(&mut conn, wf, serde_json::json!({"tenant_id": "t1"})).await;
    assert_eq!(active_count(&mut conn, wf, "t1").await, 1);

    // Before the P1 fix, `replace_execution` (reached here via
    // `TerminateIfRunning` over a terminal COMPLETED prior) never called
    // `enforce_quota_admission`.
    let exec_id2 = ExecutionId::new();
    let mut p = params(
        wf,
        &workflow_id,
        exec_id2,
        serde_json::json!({"tenant_id": "t1"}),
    );
    p.reuse_policy = WorkflowIdReusePolicy::TerminateIfRunning;
    let outcome2 = start_or_load_workflow_execution(&mut conn, p, None).await;

    let err = outcome2.expect_err(
        "TerminateIfRunning over a terminal (COMPLETED) prior must still be \
         quota-checked when it creates a fresh active execution",
    );
    assert_quota_exceeded(&err, wf, "t1", QuotaResource::ActiveExecutions, 1, |c| {
        c == 1
    });

    assert_no_execution_row(&mut conn, exec_id2).await;
    assert_eq!(row_state(&mut conn, exec_id).await, "COMPLETED");
    assert_eq!(active_count(&mut conn, wf, "t1").await, 1);
}

/// Site 1 (`ActiveConflictBehavior::Terminate`, existing RUNNING/PAUSED):
/// unlike Sites 2/3, replacing an ALREADY-active row is a quota-**neutral**
/// swap for the SAME key (the old row's -1 offsets the new row's +1) -- so
/// the real bypass here is not about looping against one stable
/// `workflow_id`, but about the request's resolved key CHANGING between the
/// original start and the `TerminateIfRunning` replace call. Before the P1
/// fix this let a caller grow an ALREADY-SATURATED key's population by
/// retargeting an unrelated, still-running execution at it via
/// `TerminateIfRunning`, with zero quota check anywhere in the path.
#[tokio::test]
async fn replace_execution_terminate_if_running_enforces_the_new_requests_resolved_key() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let wf = leaked("quota_replace_tir_crosskey");

    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1);
    let _guard = MetadataGuard::install_one(wf, policy).await;

    // Saturate "victim-tenant"'s cap of 1 via an unrelated, distinct
    // `workflow_id`.
    start_ok(
        &mut conn,
        wf,
        serde_json::json!({"tenant_id": "victim-tenant"}),
    )
    .await;
    assert_eq!(active_count(&mut conn, wf, "victim-tenant").await, 1);

    // A SEPARATE, still-RUNNING execution E, originally started under a
    // DIFFERENT key ("attacker-tenant") that is itself exactly at its own
    // (unrelated) cap of 1.
    let workflow_id_e = format!("wid-{}", Uuid::new_v4().simple());
    let (exec_id_e, outcome_e) = try_start(
        &mut conn,
        wf,
        &workflow_id_e,
        serde_json::json!({"tenant_id": "attacker-tenant"}),
    )
    .await;
    outcome_e.expect("E must start under its own, unsaturated key");
    assert_eq!(active_count(&mut conn, wf, "attacker-tenant").await, 1);

    // Now re-target E's SAME `workflow_id` with `TerminateIfRunning`, but
    // this request body resolves to "victim-tenant" -- the
    // ALREADY-SATURATED key. Before the P1 fix, `replace_execution`
    // (`ActiveConflictBehavior::Terminate`) never called
    // `enforce_quota_admission`, so E would have been silently sealed and
    // replaced by a fresh execution counted against "victim-tenant",
    // growing that key's population to 2 past its declared cap of 1.
    let exec_id_e2 = ExecutionId::new();
    let mut p = params(
        wf,
        &workflow_id_e,
        exec_id_e2,
        serde_json::json!({"tenant_id": "victim-tenant"}),
    );
    p.reuse_policy = WorkflowIdReusePolicy::TerminateIfRunning;
    let outcome2 = start_or_load_workflow_execution(&mut conn, p, None).await;

    let err = outcome2.expect_err(
        "a TerminateIfRunning replace that resolves to an ALREADY-saturated \
         key must be rejected, exactly like a fresh admission would be",
    );
    assert_quota_exceeded(
        &err,
        wf,
        "victim-tenant",
        QuotaResource::ActiveExecutions,
        1,
        |c| c == 1,
    );

    // The rejection rolled back atomically: no phantom row for the failed
    // attempt, E is STILL RUNNING under its original key (never sealed --
    // the whole `replace_execution` call, including the seal step, rolled
    // back together with the failed quota check), and both keys' active
    // populations are untouched.
    assert_no_execution_row(&mut conn, exec_id_e2).await;
    assert_eq!(row_state(&mut conn, exec_id_e).await, "RUNNING");
    assert_eq!(active_count(&mut conn, wf, "victim-tenant").await, 1);
    assert_eq!(active_count(&mut conn, wf, "attacker-tenant").await, 1);
}

/// The fix must not over-reject: a `replace_execution` admission still
/// succeeds normally when the resolved key is well under its cap (Site 2),
/// and a quota-**neutral** same-key refresh (Site 1) succeeds even when the
/// key is already exactly at its cap, since it is a net-zero swap.
#[tokio::test]
async fn replace_execution_paths_still_succeed_when_not_over_cap() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let wf = leaked("quota_replace_under_cap");

    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(2);
    let _guard = MetadataGuard::install_one(wf, policy).await;

    // AllowDuplicateFailedOnly over a FAILED prior, well under cap.
    let wid1 = format!("wid-{}", Uuid::new_v4().simple());
    let (exec1, o1) = try_start(
        &mut conn,
        wf,
        &wid1,
        serde_json::json!({"tenant_id": "roomy"}),
    )
    .await;
    o1.expect("initial start");
    mark_terminal(&mut conn, exec1, "FAILED").await;

    let exec1b = ExecutionId::new();
    let mut p1 = params(wf, &wid1, exec1b, serde_json::json!({"tenant_id": "roomy"}));
    p1.reuse_policy = WorkflowIdReusePolicy::AllowDuplicateFailedOnly;
    start_or_load_workflow_execution(&mut conn, p1, None)
        .await
        .expect("a replace well under cap must still succeed -- the fix must not over-reject");
    assert_eq!(active_count(&mut conn, wf, "roomy").await, 1);

    // TerminateIfRunning over a RUNNING existing, bringing the key to
    // EXACTLY its cap first, then a same-key refresh at the cap boundary
    // (net-zero on active count) must still succeed.
    let wid2 = format!("wid-{}", Uuid::new_v4().simple());
    let (_exec2, o2) = try_start(
        &mut conn,
        wf,
        &wid2,
        serde_json::json!({"tenant_id": "roomy"}),
    )
    .await;
    o2.expect("initial start");
    assert_eq!(active_count(&mut conn, wf, "roomy").await, 2);

    let exec2b = ExecutionId::new();
    let mut p2 = params(wf, &wid2, exec2b, serde_json::json!({"tenant_id": "roomy"}));
    p2.reuse_policy = WorkflowIdReusePolicy::TerminateIfRunning;
    start_or_load_workflow_execution(&mut conn, p2, None)
        .await
        .expect("a same-key refresh replace must succeed -- it is net-zero on active count");
    assert_eq!(active_count(&mut conn, wf, "roomy").await, 2);
}

/// Codex P2 (issue #946, round 1): `history_bytes`, not just
/// `active_executions`, must be exempted for the row a `TerminateIfRunning`
/// replace is about to seal -- and specifically on Site 1 (the
/// `ActiveConflictBehavior::Terminate` path over a RUNNING/PAUSED existing,
/// reached via the pre-check-cancel shortcut before the P1/P2 fix and via
/// the atomic `inline_cancel` + `replace_execution` fallthrough after it).
///
/// Before the fix, `enforce_quota_before_terminate_pre_check` only ever
/// subtracted the existing row's own contribution from
/// `usage.active_executions`, never from `usage.history_bytes` -- so a
/// same-key refresh under a tight `max_history_bytes` cap would be WRONGLY
/// REJECTED the moment the row being replaced had accumulated ANY history of
/// its own, even though that row is about to be sealed out of existence and
/// contributes nothing to the key's population going forward. The fix
/// (routing quota-governed keys through the atomic `inline_cancel` +
/// `replace_execution` path instead) closes this for free: that path seals
/// the existing row to CANCELLED *before* `enforce_quota_admission` runs, so
/// the row is excluded from EVERY resource `QUOTA_USAGE_SQL`'s `active` CTE
/// scopes by state -- `active_executions` and `history_bytes` alike -- with
/// no special-cased exemption logic required for either.
#[tokio::test]
async fn replace_execution_terminate_if_running_exempts_the_replaced_runs_own_history_bytes() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let wf = leaked("quota_replace_tir_history_bytes");

    // A 1-byte cap: any single row's own `WorkflowStarted` event already
    // exceeds it (mirrors `history_bytes_cap_rejects_once_exceeded`'s
    // pattern), so this key is only ever "under cap" while it has zero
    // RUNNING/PAUSED rows of its own.
    let policy = QuotaPolicy::new("tenant_id").with_max_history_bytes(1);
    let _guard = MetadataGuard::install_one(wf, policy).await;

    let workflow_id = format!("wid-{}", Uuid::new_v4().simple());
    let (exec_id, outcome) = try_start(
        &mut conn,
        wf,
        &workflow_id,
        serde_json::json!({"tenant_id": "acme"}),
    )
    .await;
    outcome.expect("the FIRST start for a key must succeed: usage is zero before it exists");
    assert_eq!(active_count(&mut conn, wf, "acme").await, 1);

    // Sanity check the trap is real: a FRESH, unrelated `workflow_id` under
    // the SAME key is rejected on `history_bytes` alone, proving the cap is
    // genuinely breached by the first row's own recorded history (so the
    // same-key replace below is not vacuously "under cap the whole time").
    let (_unrelated_exec, unrelated_outcome) = try_start(
        &mut conn,
        wf,
        &format!("wid-{}", Uuid::new_v4().simple()),
        serde_json::json!({"tenant_id": "acme"}),
    )
    .await;
    let unrelated_err = unrelated_outcome.expect_err(
        "an UNRELATED fresh start under the same key must be rejected on history_bytes",
    );
    assert_quota_exceeded(
        &unrelated_err,
        wf,
        "acme",
        QuotaResource::HistoryBytes,
        1,
        |c| c >= 1,
    );

    // Now replace the SAME row via `TerminateIfRunning` (Site 1 -- the
    // existing row is still RUNNING at this point). Before the P1/P2 fix,
    // this would ALSO have been wrongly rejected on `history_bytes`, since
    // the pre-check helper only exempted `active_executions`. After the
    // fix, the existing row is sealed to CANCELLED before the quota check
    // runs, so it (and its history) is excluded entirely.
    let exec_id2 = ExecutionId::new();
    let mut p = params(
        wf,
        &workflow_id,
        exec_id2,
        serde_json::json!({"tenant_id": "acme"}),
    );
    p.reuse_policy = WorkflowIdReusePolicy::TerminateIfRunning;
    let outcome2 = start_or_load_workflow_execution(&mut conn, p, None).await;

    outcome2.expect(
        "a same-key TerminateIfRunning replace must succeed: the row being \
         replaced -- and its own history -- must be excluded from the \
         history_bytes check, not just active_executions",
    );
    // `inline_cancel` appends a `WorkflowCancelled` event and sets CANCELLED
    // first, but `replace_execution` then unconditionally seals the SAME
    // row to CONTINUED_AS_NEW (its own doc comment: "existing is already
    // sealed above (CONTINUED_AS_NEW) by the time this runs") -- the final
    // observable state of the atomic `inline_cancel` + `replace_execution`
    // sequence, pre-existing and unrelated to this fix. Either state
    // excludes the row from `state IN ('RUNNING', 'PAUSED')`, which is all
    // that matters for the quota exemption this test proves.
    assert_eq!(row_state(&mut conn, exec_id).await, "CONTINUED_AS_NEW");
    assert_eq!(row_state(&mut conn, exec_id2).await, "RUNNING");
    assert_eq!(active_count(&mut conn, wf, "acme").await, 1);
}

/// A workflow-level retry (issue #523) continuation must NOT be silently
/// dropped by a per-tenant quota that has since filled up (issue #946, Codex
/// round-2 review). `start_or_load_workflow_execution_collect` already
/// exempts a retry continuation from the admission gate (`gate: None`) and
/// from concurrency-supersede (`concurrency_on_conflict: Defer`) with an
/// explicit "in-flight continuation, not a fresh admission" rationale --
/// quota enforcement must follow the same rule, or a legitimate retry with
/// attempts remaining can be permanently, silently skipped the moment the
/// tenant's quota happens to be at cap, defeating the workflow's configured
/// retry policy with no error surfaced anywhere (`worker.rs`'s retry driver
/// only `tracing::warn!`s and gives up on any `Err`).
///
/// This test also proves the exemption is scoped correctly: a genuinely
/// FRESH start for the SAME over-cap key is still rejected -- the retry
/// exemption is not a blanket bypass -- and the retry's row is still
/// correctly tagged with `quota_key` for future usage accounting, even
/// though this one admission was not checked against it.
#[tokio::test]
async fn active_executions_cap_does_not_block_a_workflow_level_retry_continuation() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let wf = leaked("quota_retry_exemption");

    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1);
    let _guard = MetadataGuard::install_one(wf, policy).await;

    let input = serde_json::json!({"tenant_id": "acme"});

    // Fill the cap: one active execution for tenant "acme".
    start_ok(&mut conn, wf, input.clone()).await;
    assert_eq!(active_count(&mut conn, wf, "acme").await, 1);

    // A genuinely FRESH start for the same over-cap key is still rejected --
    // the retry exemption below must not become a blanket bypass.
    let (_fresh_id, fresh_outcome) = try_start(
        &mut conn,
        wf,
        &format!("wid-fresh-{}", Uuid::new_v4().simple()),
        input.clone(),
    )
    .await;
    let fresh_err =
        fresh_outcome.expect_err("a genuinely fresh start over the cap must still be rejected");
    assert_quota_exceeded(
        &fresh_err,
        wf,
        "acme",
        QuotaResource::ActiveExecutions,
        1,
        |c| c == 1,
    );

    // A workflow-level retry continuation for the SAME over-cap tenant key
    // must succeed despite the cap: `retry_of_exec_id` is `Some` at exactly
    // one call site in the whole engine -- the retry driver in `worker.rs`
    // -- so setting it here is a faithful stand-in for a real retry.
    let retry_exec_id = ExecutionId::new();
    let retry_workflow_id = format!("wid-retry-{}", Uuid::new_v4().simple());
    let mut retry_params = params(wf, &retry_workflow_id, retry_exec_id, input);
    retry_params.retry_of_exec_id = Some(Uuid::new_v4());
    retry_params.workflow_attempt = 2;

    let retry_outcome = start_or_load_workflow_execution(&mut conn, retry_params, None).await;
    retry_outcome.unwrap_or_else(|e| {
        panic!(
            "a workflow-level retry continuation must not be blocked by a \
             quota at cap for its tenant key, got {e:?}"
        )
    });

    // The retry's own row is still correctly tagged for FUTURE usage
    // accounting -- only this one admission was exempt from enforcement,
    // not from the key resolution/tagging itself.
    let retry_row = load_execution(&mut conn, retry_exec_id).await;
    assert_eq!(
        retry_row.quota_key.as_deref(),
        Some("acme"),
        "a retry-exempt admission must still stamp quota_key for later accounting"
    );

    // The key's active population is now 2 (the original + the retry),
    // genuinely over the declared cap of 1 -- the exemption let the cap be
    // exceeded for this in-flight continuation, exactly as intended.
    assert_eq!(active_count(&mut conn, wf, "acme").await, 2);
}

// ---------------------------------------------------------------------------
// Resolved quota-key length bound (issue #946, Codex round-2 review:
// "bound resolved quota keys before indexing them")
// ---------------------------------------------------------------------------
//
// `key_expr` is an author-declared, trusted dot-path, but the VALUE it
// resolves to comes straight from caller-controlled workflow input, and is
// otherwise unbounded before it reaches the INDEXED `quota_key` column on
// `harvest_workflow_executions`. An oversized resolved key must be rejected
// cleanly at admission time -- before any DB write -- rather than risk a raw
// Postgres "index row size exceeds maximum for index" error surfacing as an
// unhandled `500`.

/// The money test: a resolved key longer than [`MAX_QUOTA_KEY_BYTES`] is
/// rejected with a typed [`HarvestError::PayloadTooLarge`] (never an
/// uncontrolled database error), and the rejection rolls back atomically --
/// no phantom execution or task-queue row survives it, exactly like a
/// resource-cap rejection (AC4).
#[tokio::test]
async fn oversized_resolved_quota_key_is_rejected_before_any_db_write() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let wf = leaked("quota_key_length_oversized");

    // A generous resource cap -- the rejection below must be attributable to
    // the KEY LENGTH bound, not the active-executions count.
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1000);
    let _guard = MetadataGuard::install_one(wf, policy).await;

    let oversized_tenant_id = "x".repeat(usize::try_from(MAX_QUOTA_KEY_BYTES).expect("small") + 1);
    let (rejected_id, outcome) = try_start(
        &mut conn,
        wf,
        &format!("wid-{}", Uuid::new_v4().simple()),
        serde_json::json!({"tenant_id": oversized_tenant_id}),
    )
    .await;

    let err = outcome.expect_err(
        "a resolved quota key over the length bound must be rejected, not silently indexed",
    );
    match &err {
        HarvestError::PayloadTooLarge {
            kind,
            observed_bytes,
            cap_bytes,
            workflow_type,
            activity_name,
        } => {
            assert_eq!(*kind, PayloadKind::QuotaKey);
            assert_eq!(*observed_bytes, MAX_QUOTA_KEY_BYTES + 1);
            assert_eq!(*cap_bytes, MAX_QUOTA_KEY_BYTES);
            assert_eq!(workflow_type, wf);
            assert_eq!(*activity_name, None);
        }
        other => {
            panic!("expected HarvestError::PayloadTooLarge{{kind: QuotaKey, ..}}, got {other:?}")
        }
    }

    // Never a silent aliasing/truncation -- and never a phantom row either.
    assert_no_execution_row(&mut conn, rejected_id).await;
    assert_no_task_row(&mut conn, rejected_id).await;
}

/// The bound itself must be admissible end to end through the real admission
/// path (not just the pure `quota_key_over_cap` helper, which `quota.rs`'s
/// own unit tests already cover in isolation) -- only a key STRICTLY over
/// [`MAX_QUOTA_KEY_BYTES`] is rejected.
#[tokio::test]
async fn quota_key_exactly_at_the_length_bound_is_admitted() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let wf = leaked("quota_key_length_at_bound");

    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1000);
    let _guard = MetadataGuard::install_one(wf, policy).await;

    let exact_tenant_id = "x".repeat(usize::try_from(MAX_QUOTA_KEY_BYTES).expect("small"));
    assert_eq!(exact_tenant_id.len() as u64, MAX_QUOTA_KEY_BYTES);

    let exec_id = start_ok(
        &mut conn,
        wf,
        serde_json::json!({"tenant_id": exact_tenant_id.clone()}),
    )
    .await;

    let row = load_execution(&mut conn, exec_id).await;
    assert_eq!(row.quota_key.as_deref(), Some(exact_tenant_id.as_str()));
}

// ---------------------------------------------------------------------------
// Issue #946, Codex round-3 review (P2) — "Validate cross-type continuation
// quota keys before insertion". Mirrors
// `oversized_resolved_quota_key_is_rejected_before_any_db_write` above, but
// for the CROSS-TYPE `continue_as_new_as_type` path
// (`persist_workflow_continue_as_new`'s successor-key bound check), which
// resolves the key against the TARGET type's own declared policy and the
// FRESH successor input rather than the fresh-admission path this file's
// other oversized-key test drives directly.
// ---------------------------------------------------------------------------

fn oversized_key_phase_one<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let target = input["next_type"]
            .as_str()
            .expect("input.next_type must be present")
            .to_string();
        let oversized_tenant_id = input["oversized_tenant_id"]
            .as_str()
            .expect("input.oversized_tenant_id must be present")
            .to_string();
        let target: &'static str = Box::leak(target.into_boxed_str());
        ctx.continue_as_new_as_type(
            target,
            serde_json::json!({"tenant_id": oversized_tenant_id}),
        )
        .await
        .map_err(|e| e.to_string())?;
        unreachable!("continue_as_new_as_type suspends the run and never resolves");
    })
}

/// A cross-type continuation whose successor quota key -- resolved against
/// the TARGET type's own declared policy and the FRESH successor input --
/// exceeds [`MAX_QUOTA_KEY_BYTES`] must be rejected before the successor row
/// is ever inserted, terminally failing the PREDECESSOR with the exact typed
/// <code>[HarvestError::PayloadTooLarge]{kind: QuotaKey, ..}</code> shape -- never a
/// silent truncation/aliasing, and never a raw Postgres index-size error
/// surfacing as an unhandled worker crash. Worker-driven end to end, mirroring
/// `continue_as_new_cross_type_re_resolves_quota_key`'s harness pattern.
#[tokio::test]
async fn continue_as_new_cross_type_oversized_quota_key_is_rejected() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let phase1 = leaked("quota_can_oversized_from");
    let phase2 = leaked("quota_can_oversized_to");
    let workflow_id = format!("sub-{}", Uuid::new_v4().simple());

    let oversized_tenant_id = "x".repeat(usize::try_from(MAX_QUOTA_KEY_BYTES).expect("small") + 1);

    let predecessor = start_root(
        &mut conn,
        phase1,
        &workflow_id,
        serde_json::json!({
            "next_type": phase2,
            "oversized_tenant_id": oversized_tenant_id,
        }),
    )
    .await;

    // A generous resource cap on the TARGET type -- the rejection below must
    // be attributable to the KEY LENGTH bound, not any active-executions
    // count (mirrors `oversized_resolved_quota_key_is_rejected_before_any_db_write`).
    let mut target = wf_info(phase2, phase_two);
    target.quota = Some(QuotaPolicy::new("tenant_id").with_max_active_executions(1000));

    let reg = registry(vec![wf_info(phase1, oversized_key_phase_one), target]);
    let worker = build_runtime_worker("w-946-cross-type-oversized", 2, 1, reg);
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&url));
    let failed = wait_for_execution_state(&url, predecessor, "FAILED").await;
    worker.shutdown();
    handle.await.expect("worker join");

    let error = failed
        .error
        .expect("a terminal failure must carry an error");
    assert!(
        error.contains(phase2) && error.contains("QuotaKey"),
        "the failure must name the target type and the QuotaKey payload kind, got {error}"
    );

    // No continue-as-new was ever recorded on the predecessor -- the bound
    // check runs BEFORE any event/row is persisted.
    let history = load_history_from_url(&url, predecessor).await;
    assert!(
        !history
            .events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowContinuedAsNew { .. })),
        "a rejected cross-type transition must record no WorkflowContinuedAsNew"
    );

    // No successor row of the target type exists at all.
    let successor_count_sql =
        "SELECT COUNT(*)::BIGINT AS n FROM harvest_workflow_executions WHERE workflow_name = $1";
    assert_eq!(
        count_rows(&mut conn, successor_count_sql, &[phase2]).await,
        0,
        "a rejected cross-type transition must create no successor row"
    );
}

// ---------------------------------------------------------------------------
// Issue #946, Codex round-3 review — a SAME-SHARD completion trigger whose
// TARGET's per-tenant quota is exhausted at fire time must defer the start
// to the durable outbox for retry, NOT propagate `Err` out of
// `evaluate_triggers_for_execution` and roll back the SOURCE execution's own
// terminal commit along with it. Worker-driven end to end.
// ---------------------------------------------------------------------------

fn quota_trigger_source<'a>(
    _ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(serde_json::json!({"source": "done"})) })
}

fn quota_trigger_target<'a>(
    _ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(serde_json::json!("target_done")) })
}

/// A completion trigger firing on the SAME shard as its source, whose TARGET
/// workflow type's per-tenant quota is exhausted, must not roll back the
/// source's own terminal commit. `evaluate_triggers_for_execution`'s
/// `QuotaExceeded` arm falls back to the SAME durable outbox +
/// `DeferredTriggerStart` retry machinery the cross-shard branch already
/// uses (`target_shard == source_shard` here), rather than returning `Err`
/// and letting it propagate out of the whole terminal-sealing transaction.
#[tokio::test]
async fn completion_trigger_defers_to_outbox_when_target_quota_exceeded() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    // `evaluate_triggers_for_execution` (issue #605's completion-trigger
    // machinery) resolves shard routing via the process-global
    // `GLOBAL_SHARD_ROUTER`/`GLOBAL_SHARDED_POOL` statics -- unlike the
    // direct `start_or_load_workflow_execution`/child-spawn paths this
    // file's other tests exercise, which need no router at all. Install a
    // clean single-shard topology so `target_shard == source_shard` and the
    // SAME-SHARD inline-start branch (the one carrying this test's P1 #1
    // fix under test) is what actually runs, mirroring the convention in
    // `workflow_id_targeted_tests.rs`/`transactional_start_tests.rs`.
    autumn_harvest::shard::install_global_router(autumn_harvest::shard::ShardRouter::single());
    let _sharded_pool = autumn_harvest::shard::ShardedDbPool::single(build_test_pool(&url));

    let source_wf = leaked("quota_trigger_source");
    let target_wf = leaked("quota_trigger_target");

    let target_quota_policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1);
    let mut target_info = wf_info(target_wf, quota_trigger_target);
    target_info.quota = Some(target_quota_policy);

    // `evaluate_triggers_for_execution` (like every other worker-driven admit
    // path -- awaited/detached/timeout-race child spawn) resolves the
    // target's declared quota policy from `GLOBAL_WORKFLOW_METADATA`, but
    // that global is UNCONDITIONALLY REBUILT from the registry's own
    // `WorkflowInfo.quota` fields the moment `HandlerRegistry::with_state*`
    // runs (`worker.rs`, mirroring how it also seeds `concurrency`/`sla`/
    // `retry_policy`) -- so a `MetadataGuard` held across `build_runtime_worker`
    // below would be silently clobbered the instant the worker's registry is
    // constructed. The guard is therefore used ONLY to seed the blocker's own
    // admission via `start_root` (the bare/fresh path, which reads the SAME
    // global directly with no registry in the loop) and dropped immediately
    // after, exactly like the passing
    // `awaited_child_spawn_honors_target_quota_parks_parent_then_succeeds`
    // test above; `target_info.quota` is what makes the trigger's own
    // admission see the policy once the worker is running.
    let guard = MetadataGuard::install_one(target_wf, target_quota_policy).await;

    // Occupy the ONE `max_active_executions` slot for key "acme", then
    // delete its task row so it can never complete/free the slot on its own.
    let blocker = start_root(
        &mut conn,
        target_wf,
        &format!("blocker-{}", Uuid::new_v4().simple()),
        serde_json::json!({"tenant_id": "acme"}),
    )
    .await;
    drop(guard);
    diesel::sql_query("DELETE FROM harvest_task_queue WHERE workflow_exec_id = $1")
        .bind::<diesel::sql_types::Uuid, _>(blocker.as_uuid())
        .execute(&mut conn)
        .await
        .expect("delete blocker task row");

    // Registered before the source starts, mirroring production order
    // (`sync_completion_triggers` runs at startup, well before any source
    // completes).
    autumn_harvest::completion_trigger::sync_completion_triggers(
        &mut conn,
        &[
            autumn_harvest::completion_trigger::CompletionTrigger::new(source_wf, target_wf)
                .with_input_mapping(autumn_harvest::completion_trigger::InputMapping::Static(
                    serde_json::json!({"tenant_id": "acme"}),
                ))
                .with_queue_name("default"),
        ],
    )
    .await
    .expect("register completion trigger");

    let source = start_root(
        &mut conn,
        source_wf,
        &format!("source-{}", Uuid::new_v4().simple()),
        serde_json::json!({}),
    )
    .await;

    let reg = registry(vec![wf_info(source_wf, quota_trigger_source), target_info]);
    let worker = build_runtime_worker("w-946-trigger-quota", 2, 1, reg);
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&url));

    // The money assertion: the source reaches COMPLETED even though its
    // trigger's target is at quota cap. Pre-fix, `Err(QuotaExceeded)`
    // propagating out of `evaluate_triggers_for_execution` rolled back the
    // WHOLE persist transaction -- including the source's own
    // `WorkflowCompleted` append -- leaving it stuck RUNNING forever with no
    // error ever recorded.
    wait_for_execution_state(&url, source, "COMPLETED").await;

    #[derive(diesel::QueryableByName)]
    struct OutboxCount {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let outbox_count = async |conn: &mut AsyncPgConnection| -> i64 {
        diesel::sql_query(
            "SELECT COUNT(*)::BIGINT AS n FROM harvest_completion_trigger_outbox \
             WHERE source_exec_id = $1",
        )
        .bind::<diesel::sql_types::Uuid, _>(source.as_uuid())
        .get_result::<OutboxCount>(conn)
        .await
        .expect("count outbox rows")
        .n
    };
    assert_eq!(
        outbox_count(&mut conn).await,
        1,
        "a same-shard trigger blocked by the target's quota must be deferred \
         to the durable outbox for retry"
    );

    let target_row_count_sql =
        "SELECT COUNT(*)::BIGINT AS n FROM harvest_workflow_executions WHERE workflow_name = $1";
    assert_eq!(
        count_rows(&mut conn, target_row_count_sql, &[target_wf]).await,
        1, // only the blocker
        "no target execution should exist while the target's quota is at cap"
    );

    // Free the quota slot -- the outbox sweep (`enforce_completion_triggers_outbox`,
    // folded into the worker's background timeout-checker loop, ticking every
    // `poll_interval`) should now retry the deferred row and successfully
    // start the target.
    mark_terminal(&mut conn, blocker, "CANCELLED").await;

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let n = count_rows(&mut conn, target_row_count_sql, &[target_wf]).await;
        if n == 2 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "target row was never created by the outbox retry; last count was {n}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    worker.shutdown();
    handle.await.expect("worker join");

    #[derive(diesel::QueryableByName)]
    struct TargetRow {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        quota_key: Option<String>,
    }
    let target_row: TargetRow = diesel::sql_query(
        "SELECT quota_key FROM harvest_workflow_executions \
         WHERE workflow_name = $1 AND id != $2",
    )
    .bind::<diesel::sql_types::Text, _>(target_wf)
    .bind::<diesel::sql_types::Uuid, _>(blocker.as_uuid())
    .get_result(&mut conn)
    .await
    .expect("retried target row must exist");
    assert_eq!(
        target_row.quota_key.as_deref(),
        Some("acme"),
        "the deferred target must still carry the tenant key resolved at \
         trigger-fire time"
    );

    assert_eq!(
        outbox_count(&mut conn).await,
        0,
        "the outbox row must be consumed once the deferred start succeeds"
    );
}

// ── Outbox backoff/starvation test helpers (issue #1227 Finding 4) ─────────

async fn insert_outbox_row(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    input: serde_json::Value,
) -> Uuid {
    diesel::insert_into(harvest_completion_trigger_outbox::table)
        .values(&NewCompletionTriggerOutboxDb {
            source_exec_id: Uuid::new_v4(),
            trigger_id: Uuid::new_v4(),
            target_shard: 0,
            target_workflow_name: workflow_name.to_string(),
            target_workflow_id: format!("target-{}", Uuid::new_v4().simple()),
            target_input: input,
            queue_name: None,
            concurrency_key: None,
            concurrency_limit: None,
            priority: serde_json::to_value(Priority::default()).unwrap(),
            max_workflow_input_bytes: 1_000_000,
        })
        .get_result::<CompletionTriggerOutboxDb>(conn)
        .await
        .expect("insert outbox row")
        .id
}

async fn outbox_next_attempt_at(
    conn: &mut AsyncPgConnection,
    id: Uuid,
) -> Option<chrono::DateTime<chrono::Utc>> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
        next_attempt_at: Option<chrono::DateTime<chrono::Utc>>,
    }
    diesel::sql_query("SELECT next_attempt_at FROM harvest_completion_trigger_outbox WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(id)
        .get_result::<Row>(conn)
        .await
        .expect("row must still exist")
        .next_attempt_at
}

async fn outbox_row_exists(conn: &mut AsyncPgConnection, id: Uuid) -> bool {
    #[derive(diesel::QueryableByName)]
    struct IdRow {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        #[allow(dead_code)]
        id: Uuid,
    }
    diesel::sql_query("SELECT id FROM harvest_completion_trigger_outbox WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(id)
        .get_result::<IdRow>(conn)
        .await
        .is_ok()
}

// `created_at` defaults to `now()` at insertion, which is NOT a reliable
// ordering signal for these tests: several inserts issued back-to-back on the
// same connection can land in the same microsecond (more likely still under a
// loaded CI host running the rest of this suite concurrently), and a
// `created_at` tie makes `ORDER BY created_at ASC` pick an unspecified order
// among the tied rows -- silently breaking a test's ordering assumption.
// Stamp `created_at` explicitly instead.
async fn set_outbox_created_at(
    conn: &mut AsyncPgConnection,
    id: Uuid,
    created_at: chrono::DateTime<chrono::Utc>,
) {
    diesel::sql_query("UPDATE harvest_completion_trigger_outbox SET created_at = $2 WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(id)
        .bind::<diesel::sql_types::Timestamptz, _>(created_at)
        .execute(conn)
        .await
        .expect("stamp created_at");
}

/// Issue #1227 Finding 4: pre-fix, `enforce_completion_triggers_outbox`'s
/// claim query had no `ORDER BY` and no per-row backoff tracking at all -- a
/// `QuotaBlocked` outcome left the outbox row completely untouched (neither
/// deleted nor timestamped). A row blocked against a durably exhausted quota
/// could then dominate every unordered `LIMIT 50` claim batch on every
/// scanner tick, starving any OTHER, unrelated relay sharing the batch.
///
/// This inserts outbox rows directly (bypassing
/// `evaluate_triggers_for_execution`, whose own quota-block-to-outbox path is
/// covered by `completion_trigger_defers_to_outbox_when_target_quota_exceeded`
/// above) so it isolates `enforce_completion_triggers_outbox`'s own
/// claim/backoff mechanics.
///
/// Proving "does not starve a sibling row" needs genuine batch pressure: the
/// claim query is `LIMIT 50`, and even the PRE-fix code moved on to the next
/// row in an already-loaded batch on a `QuotaBlocked` outcome (nothing
/// aborted the loop) -- so two rows sharing one small batch would pass
/// identically before and after this fix. This inserts 60 quota-blocked rows
/// (all against the SAME durably-exhausted target/tenant, exceeding the
/// LIMIT-50 window) followed by one free-target row, so the first scan's
/// batch is entirely blocked rows and the free row is provably NOT reached --
/// then shows the backoff filter is what lets it surface on a LATER scan
/// instead of being starved forever.
#[tokio::test]
async fn quota_blocked_outbox_row_gets_backoff_and_does_not_starve_a_sibling_row() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    install_global_router(ShardRouter::single());
    let sharded_pool = Some(ShardedDbPool::single(build_test_pool(&url)));

    let blocked_wf = leaked("outbox_backoff_blocked");
    let free_wf = leaked("outbox_backoff_free");

    let quota_policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1);
    let guard = MetadataGuard::install_one(blocked_wf, quota_policy).await;

    // Occupy the one slot for tenant "acme" so any fresh admission of
    // `blocked_wf` under that key is rejected with `QuotaExceeded`.
    let blocker = start_root(
        &mut conn,
        blocked_wf,
        &format!("blocker-{}", Uuid::new_v4().simple()),
        serde_json::json!({"tenant_id": "acme"}),
    )
    .await;

    // The claim query's `LIMIT`, kept in lockstep with
    // `enforce_completion_triggers_outbox`'s hardcoded `.limit(50)` so this
    // test fails loudly (not silently under-provisions the batch) if that
    // constant ever changes.
    const CLAIM_BATCH_LIMIT: usize = 50;
    const BLOCKED_ROW_COUNT: usize = CLAIM_BATCH_LIMIT + 10;

    // `created_at` defaults to `now()` at insertion, which is NOT a reliable
    // ordering signal here: several inserts issued back-to-back on the same
    // connection can land in the same microsecond (more likely still under a
    // loaded CI host running the rest of this suite concurrently), and a
    // `created_at` tie makes `ORDER BY created_at ASC` pick an unspecified
    // order among the tied rows -- silently breaking the "free row sorts
    // last" assumption this test depends on. Stamp `created_at` explicitly,
    // strictly increasing by a whole second per row, so the intended order is
    // exact regardless of real wall-clock resolution.
    let base_created_at = chrono::Utc::now() - chrono::Duration::hours(1);

    let mut blocked_outbox_ids = Vec::with_capacity(BLOCKED_ROW_COUNT);
    for i in 0..BLOCKED_ROW_COUNT {
        let id = insert_outbox_row(
            &mut conn,
            blocked_wf,
            serde_json::json!({"tenant_id": "acme"}),
        )
        .await;
        set_outbox_created_at(
            &mut conn,
            id,
            base_created_at + chrono::Duration::seconds(i64::try_from(i).expect("small index")),
        )
        .await;
        blocked_outbox_ids.push(id);
    }
    let oldest_blocked_outbox_id = blocked_outbox_ids[0];
    let free_outbox_id = insert_outbox_row(&mut conn, free_wf, serde_json::json!({})).await;
    set_outbox_created_at(
        &mut conn,
        free_outbox_id,
        base_created_at
            + chrono::Duration::seconds(i64::try_from(BLOCKED_ROW_COUNT).expect("small count")),
    )
    .await;

    // First scan: the batch (`ORDER BY created_at ASC LIMIT 50`) is entirely
    // the 50 OLDEST blocked rows -- the free row (youngest of all 61) is
    // provably NOT in it. This is the starvation this fix addresses: without
    // it, every future scan would reload this exact same dominant batch
    // forever.
    enforce_completion_triggers_outbox(&mut conn, &NoOpMetrics, &sharded_pool, &[ShardId::new(0)])
        .await
        .expect("first outbox scan");

    assert!(
        outbox_row_exists(&mut conn, free_outbox_id).await,
        "the free row sorts after 60 blocked rows, so a LIMIT-50 batch \
         cannot reach it on the first scan -- confirms the batch really is \
         dominated, the precondition for the starvation this test proves is \
         fixed"
    );

    let first_backoff = outbox_next_attempt_at(&mut conn, oldest_blocked_outbox_id)
        .await
        .expect(
            "a QuotaBlocked outcome must stamp next_attempt_at into the future, \
             not leave the row untouched (issue #1227 Finding 4)",
        );
    assert!(
        first_backoff > chrono::Utc::now(),
        "next_attempt_at must be in the future immediately after a quota block"
    );

    // Second scan: the 50 rows stamped above are now excluded (their backoff
    // hasn't elapsed), so the batch is the remaining 10 blocked rows plus the
    // free row -- well under the limit, so the free row is finally reached
    // and delivered. This is the actual non-starvation proof: the backoff
    // filter is what lets a sibling row surface on a LATER scan instead of
    // being crowded out forever by the same dominant batch.
    enforce_completion_triggers_outbox(&mut conn, &NoOpMetrics, &sharded_pool, &[ShardId::new(0)])
        .await
        .expect("second outbox scan");

    assert!(
        !outbox_row_exists(&mut conn, free_outbox_id).await,
        "once the backoff filter excludes the first batch's blocked rows, \
         the free row must be delivered on the very next scan -- proving the \
         fix stops the blocked rows from starving it indefinitely"
    );
    let second_backoff = outbox_next_attempt_at(&mut conn, oldest_blocked_outbox_id)
        .await
        .expect("still blocked, still stamped");
    assert_eq!(
        second_backoff, first_backoff,
        "a row whose backoff has not elapsed must be excluded from the claim \
         query, not reclaimed and re-stamped on every tick"
    );

    // Free the quota slot and force the backoff to have already elapsed
    // (avoids a real sleep in the test) -- the next scan must now deliver it.
    mark_terminal(&mut conn, blocker, "CANCELLED").await;
    diesel::sql_query(
        "UPDATE harvest_completion_trigger_outbox SET next_attempt_at = $2 WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(oldest_blocked_outbox_id)
    .bind::<diesel::sql_types::Timestamptz, _>(chrono::Utc::now() - chrono::Duration::seconds(1))
    .execute(&mut conn)
    .await
    .expect("force backoff elapsed");

    enforce_completion_triggers_outbox(&mut conn, &NoOpMetrics, &sharded_pool, &[ShardId::new(0)])
        .await
        .expect("third outbox scan");
    assert!(
        !outbox_row_exists(&mut conn, oldest_blocked_outbox_id).await,
        "once the backoff has elapsed and the quota has freed up, the row \
         must be reclaimed and delivered"
    );

    drop(guard);
}

/// Issue #1227 Finding 4, Codex round-1 P1 (PR #1386): ordering the claim
/// batch by `created_at` ALONE (the initial fix above) is not enough. Once
/// `WorkerRuntimeConfig::poll_interval` is at or above `QUOTA_REDEFER_BACKOFF`
/// (5s), a persistently-blocked row's stamped backoff has always re-elapsed
/// by the NEXT scan -- so it goes right back to being one of the 50 OLDEST
/// eligible rows, the exact same batch reloads forever, and a newer, healthy
/// row still never gets a turn. This reproduces exactly that: a large batch
/// of blocked rows whose backoff has ALREADY expired (simulating "the next
/// scan after a slow poll interval"), all older by `created_at` than one
/// never-before-attempted fresh row -- proving the fresh row is still
/// reached on the very next scan rather than waiting behind the re-eligible
/// backlog.
#[tokio::test]
async fn quota_blocked_outbox_never_attempted_rows_outrank_expired_quota_retries() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    install_global_router(ShardRouter::single());
    let sharded_pool = Some(ShardedDbPool::single(build_test_pool(&url)));

    let blocked_wf = leaked("outbox_fairness_blocked");
    let free_wf = leaked("outbox_fairness_free");

    // A live blocker still occupies the ONE `max_active_executions` slot for
    // tenant "acme" throughout this test, so every one of the 60 rows below
    // is a GENUINE re-attempt against a still-exhausted quota once reclaimed
    // -- simulating a batch that already had one failed attempt and is now
    // due for another (a slow poll interval's steady state), not a
    // one-off block that clears on its own.
    let quota_policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1);
    let _guard = MetadataGuard::install_one(blocked_wf, quota_policy).await;
    start_root(
        &mut conn,
        blocked_wf,
        &format!("blocker-{}", Uuid::new_v4().simple()),
        serde_json::json!({"tenant_id": "acme"}),
    )
    .await;

    const CLAIM_BATCH_LIMIT: usize = 50;
    const EXPIRED_RETRY_ROW_COUNT: usize = CLAIM_BATCH_LIMIT + 10;

    let base_created_at = chrono::Utc::now() - chrono::Duration::hours(1);
    let expired_next_attempt_at = chrono::Utc::now() - chrono::Duration::seconds(1);

    let mut expired_retry_ids = Vec::with_capacity(EXPIRED_RETRY_ROW_COUNT);
    for i in 0..EXPIRED_RETRY_ROW_COUNT {
        let id = insert_outbox_row(
            &mut conn,
            blocked_wf,
            serde_json::json!({"tenant_id": "acme"}),
        )
        .await;
        set_outbox_created_at(
            &mut conn,
            id,
            base_created_at + chrono::Duration::seconds(i64::try_from(i).expect("small index")),
        )
        .await;
        diesel::sql_query(
            "UPDATE harvest_completion_trigger_outbox SET next_attempt_at = $2 WHERE id = $1",
        )
        .bind::<diesel::sql_types::Uuid, _>(id)
        .bind::<diesel::sql_types::Timestamptz, _>(expired_next_attempt_at)
        .execute(&mut conn)
        .await
        .expect("stamp an already-expired backoff");
        expired_retry_ids.push(id);
    }

    // Older than every expired-retry row by `created_at`, but NEVER
    // attempted (`next_attempt_at IS NULL`) -- under `created_at`-only
    // ordering this would still lose to all 60 of them; under the fixed
    // `next_attempt_at NULLS FIRST` ordering it must win regardless.
    let fresh_outbox_id = insert_outbox_row(&mut conn, free_wf, serde_json::json!({})).await;
    set_outbox_created_at(
        &mut conn,
        fresh_outbox_id,
        base_created_at - chrono::Duration::hours(1),
    )
    .await;

    enforce_completion_triggers_outbox(&mut conn, &NoOpMetrics, &sharded_pool, &[ShardId::new(0)])
        .await
        .expect("single outbox scan");

    assert!(
        !outbox_row_exists(&mut conn, fresh_outbox_id).await,
        "a never-before-attempted row must outrank a backlog of \
         already-expired quota retries in the claim batch, even when it is \
         younger by created_at -- otherwise a persistently re-eligible \
         backlog starves every healthy row behind it forever once the poll \
         interval is at or above the quota backoff (issue #1227 Finding 4, \
         Codex round-1 P1)"
    );

    // A representative sample of the expired-retry rows must still have been
    // reclaimed (re-stamped with a fresh backoff) despite losing the race for
    // the fresh row's slot -- the fairness fix must not starve them either.
    for id in expired_retry_ids.iter().take(5) {
        let next = outbox_next_attempt_at(&mut conn, *id).await;
        assert!(
            next.is_some_and(|t| t > expired_next_attempt_at),
            "an expired-retry row filling the rest of the batch must still \
             be reclaimed and re-stamped, not starved by the fresh row's \
             new priority"
        );
    }
}

/// Issue #1227 Finding 4, Codex round-2 P2 (PR #1386): the round-1 fix
/// (order never-attempted rows strictly ahead of every retry) traded one
/// starvation direction for the other. With no reserved floor for retries, a
/// batch full of fresh rows (`next_attempt_at IS NULL`) can fill every one of
/// the 50 slots, and a previously-blocked row is never reclaimed again even
/// after its target's quota frees up -- indefinitely, for as long as fresh
/// work keeps arriving. This proves the fix (a reserved minimum of retry
/// slots per batch): a single scan with far more fresh rows than the batch
/// limit must still reclaim a lone retry-eligible row rather than letting the
/// fresh flood claim the whole batch.
#[tokio::test]
async fn quota_blocked_outbox_retry_row_is_not_starved_by_a_flood_of_fresh_rows() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    install_global_router(ShardRouter::single());
    let sharded_pool = Some(ShardedDbPool::single(build_test_pool(&url)));

    let flood_wf = leaked("outbox_fairness_flood");
    let retry_wf = leaked("outbox_fairness_retry");

    // 55 never-attempted rows, no quota policy on `flood_wf` at all -- each
    // succeeds (Delivered) the instant it is claimed, but there are enough of
    // them to fill the ENTIRE 50-row batch limit on their own, let alone the
    // 40 non-reserved slots.
    const FLOOD_ROW_COUNT: usize = 55;
    for _ in 0..FLOOD_ROW_COUNT {
        insert_outbox_row(&mut conn, flood_wf, serde_json::json!({})).await;
    }

    // One row whose quota WAS blocking it, but has since freed up -- an
    // already-past `next_attempt_at` and no live blocker. Under round-1's
    // NULLS-FIRST-only ordering, 55 fresh rows would fill every one of the
    // 50 slots and this row would never be reached, no matter how long its
    // quota has been free.
    let retry_outbox_id = insert_outbox_row(&mut conn, retry_wf, serde_json::json!({})).await;
    diesel::sql_query(
        "UPDATE harvest_completion_trigger_outbox SET next_attempt_at = $2 WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(retry_outbox_id)
    .bind::<diesel::sql_types::Timestamptz, _>(chrono::Utc::now() - chrono::Duration::seconds(1))
    .execute(&mut conn)
    .await
    .expect("stamp an already-expired, now-eligible backoff");

    enforce_completion_triggers_outbox(&mut conn, &NoOpMetrics, &sharded_pool, &[ShardId::new(0)])
        .await
        .expect("single outbox scan");

    assert!(
        !outbox_row_exists(&mut conn, retry_outbox_id).await,
        "a retry-eligible row whose quota has freed up must be reclaimed \
         within a bounded number of scans even when it is vastly \
         outnumbered by never-attempted rows in the same batch -- a floor \
         reserved for retries must survive a fresh-row flood, not just the \
         reverse (issue #1227 Finding 4, Codex round-2 P2)"
    );
}

/// Issue #1227 Finding 4, Codex round-3 P2 (PR #1386): the round-2 fix's
/// reservation is a FLOOR for retries, not a fixed carve-out. When the retry
/// backlog is smaller than `OUTBOX_RETRY_RESERVED_SLOTS` -- the common case,
/// since most scans have no quota-blocked backlog at all -- the unused
/// reservation must go back to fresh work instead of silently capping every
/// scan at 40 of the configured 50, permanently cutting outbox throughput by
/// up to 20%. This proves 45 fresh rows (no retry-eligible rows at all) are
/// ALL delivered in a single scan, not just the first 40.
#[tokio::test]
async fn quota_blocked_outbox_backfills_unused_retry_capacity_with_fresh_rows() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    install_global_router(ShardRouter::single());
    let sharded_pool = Some(ShardedDbPool::single(build_test_pool(&url)));

    let fresh_wf = leaked("outbox_backfill_fresh");

    // More than the 40-slot fresh reservation, but fewer than the full
    // 50-row batch limit -- with no retry-eligible rows at all, all 45 must
    // still be reachable in one scan if the unused retry reservation is
    // correctly backfilled.
    const FRESH_ROW_COUNT: usize = 45;
    let mut fresh_ids = Vec::with_capacity(FRESH_ROW_COUNT);
    for _ in 0..FRESH_ROW_COUNT {
        fresh_ids.push(insert_outbox_row(&mut conn, fresh_wf, serde_json::json!({})).await);
    }

    enforce_completion_triggers_outbox(&mut conn, &NoOpMetrics, &sharded_pool, &[ShardId::new(0)])
        .await
        .expect("single outbox scan");

    for id in &fresh_ids {
        assert!(
            !outbox_row_exists(&mut conn, *id).await,
            "with no retry-eligible rows competing for the batch, all 45 \
             fresh rows must be delivered in a single scan -- capping at the \
             40-slot fresh reservation would silently waste the other 10 \
             slots the empty retry tier never needed (issue #1227 Finding 4, \
             Codex round-3 P2)"
        );
    }
}

/// Issue #1227 Finding 4, Codex round-4 P1 (PR #1386): the `QuotaExceeded`
/// backoff stamp lives inside `relay_gate_checked_start`, so it never covers
/// a row that fails BEFORE that point -- a target shard with no configured
/// pool, or a connection-acquisition failure. Pre-fix, those `continue`
/// branches left the row untouched (`next_attempt_at` still `NULL`), so it
/// stayed in the "fresh" tier forever and, being older, would keep winning
/// the deterministic `created_at` ordering every single scan -- permanently
/// starving a newer row targeting a healthy shard, the exact same failure
/// mode Finding 4 fixes for quota, just for a different failure class.
///
/// This reproduces it: 55 rows targeting a shard with NO configured pool
/// (more than the claim batch limit) followed by one healthy row on a
/// reachable shard. Without a backoff stamp on the unreachable rows, EVERY
/// scan would reselect the identical oldest 50 unreachable rows forever and
/// the healthy row would never be reached.
#[tokio::test]
async fn quota_blocked_outbox_backs_off_rows_targeting_an_unconfigured_shard() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    install_global_router(ShardRouter::single());
    // Deliberately configures ONLY shard 0's pool -- shard 1 is a valid
    // claim-eligible target (included in `shard_assignments` below) but has
    // no pool to relay through, reproducing "target shard unreachable".
    let sharded_pool = Some(ShardedDbPool::single(build_test_pool(&url)));

    let unreachable_wf = leaked("outbox_backoff_unreachable_shard");
    let healthy_wf = leaked("outbox_backoff_healthy_shard");

    const UNREACHABLE_SHARD: i32 = 1;
    const UNREACHABLE_ROW_COUNT: usize = 55;

    let base_created_at = chrono::Utc::now() - chrono::Duration::hours(1);
    let mut unreachable_ids = Vec::with_capacity(UNREACHABLE_ROW_COUNT);
    for i in 0..UNREACHABLE_ROW_COUNT {
        let id = diesel::insert_into(harvest_completion_trigger_outbox::table)
            .values(&NewCompletionTriggerOutboxDb {
                source_exec_id: Uuid::new_v4(),
                trigger_id: Uuid::new_v4(),
                target_shard: UNREACHABLE_SHARD,
                target_workflow_name: unreachable_wf.to_string(),
                target_workflow_id: format!("target-{}", Uuid::new_v4().simple()),
                target_input: serde_json::json!({}),
                queue_name: None,
                concurrency_key: None,
                concurrency_limit: None,
                priority: serde_json::to_value(Priority::default()).unwrap(),
                max_workflow_input_bytes: 1_000_000,
            })
            .get_result::<CompletionTriggerOutboxDb>(&mut conn)
            .await
            .expect("insert unreachable-shard outbox row")
            .id;
        set_outbox_created_at(
            &mut conn,
            id,
            base_created_at + chrono::Duration::seconds(i64::try_from(i).expect("small index")),
        )
        .await;
        unreachable_ids.push(id);
    }

    let healthy_id = insert_outbox_row(&mut conn, healthy_wf, serde_json::json!({})).await;
    set_outbox_created_at(
        &mut conn,
        healthy_id,
        base_created_at
            + chrono::Duration::seconds(i64::try_from(UNREACHABLE_ROW_COUNT).expect("small count")),
    )
    .await;

    let shards = [ShardId::new(0), ShardId::new(UNREACHABLE_SHARD)];

    // First scan: the batch is entirely the 50 oldest unreachable-shard rows;
    // none can be relayed (no pool), and each must be backed off rather than
    // left fresh.
    enforce_completion_triggers_outbox(&mut conn, &NoOpMetrics, &sharded_pool, &shards)
        .await
        .expect("first outbox scan");

    assert!(
        outbox_row_exists(&mut conn, healthy_id).await,
        "the healthy row sorts after 55 unreachable-shard rows, so a \
         LIMIT-50 batch cannot reach it on the first scan -- confirms the \
         batch really is dominated, the precondition for the starvation \
         this test proves is fixed"
    );
    let backed_off = outbox_next_attempt_at(&mut conn, unreachable_ids[0]).await;
    assert!(
        backed_off.is_some_and(|t| t > chrono::Utc::now()),
        "a row that could not even be attempted (no pool for its target \
         shard) must still be stamped with a future next_attempt_at -- \
         otherwise it stays 'fresh' forever and keeps winning the \
         deterministic claim-batch ordering on every scan (issue #1227 \
         Finding 4, Codex round-4 P1)"
    );

    // Second scan: the 50 rows backed off above are now excluded, so the
    // batch is the remaining 5 unreachable rows plus the healthy row --
    // well under the limit, so the healthy row is finally reached and
    // delivered despite its target being on an entirely different shard
    // from the still-stuck backlog.
    enforce_completion_triggers_outbox(&mut conn, &NoOpMetrics, &sharded_pool, &shards)
        .await
        .expect("second outbox scan");

    assert!(
        !outbox_row_exists(&mut conn, healthy_id).await,
        "once the backoff excludes the first batch's unreachable-shard rows, \
         the healthy row on a DIFFERENT shard must be delivered on the very \
         next scan -- proving an unreachable-shard backlog cannot starve a \
         newer row on a healthy shard indefinitely"
    );
}

/// Issue #1227 Finding 4, Codex round-5 P2 (PR #1386): the round-4 backoff
/// stamp for a row this scan could not even attempt (missing target-shard
/// pool, connection-acquisition failure) used a plain `UPDATE ... WHERE id =
/// $1`. The batch `SELECT` that loads a scan's candidate rows takes no lock
/// (round-1 P2's own rationale), so the SAME row can simultaneously be the
/// one a PEER replica's `relay_gate_checked_start` is holding under `FOR
/// UPDATE SKIP LOCKED` for the entire relay (issue #618 F-round19) -- a
/// bounded operation, but one that spans a cross-shard target start and so
/// is not instantaneous. A plain `UPDATE` has no "skip" option: it simply
/// blocks until the peer's claim transaction commits or rolls back, stalling
/// this replica's ENTIRE scan (and every scanner duty behind it) on someone
/// else's in-flight relay -- defeating the exact non-blocking guarantee
/// `SKIP LOCKED` exists to provide.
///
/// This reproduces the row-lock contention directly (rather than trying to
/// land a real peer inside `relay_gate_checked_start` mid-relay, which needs
/// its own cross-shard target start to be paused at a precise instant): a
/// second connection takes the identical `FOR UPDATE` lock
/// `relay_gate_checked_start` would hold, and a scan that must back the same
/// row off (via the missing-pool branch) runs concurrently under a timeout.
/// Pre-fix, the plain `UPDATE` blocks on that lock and the scan never
/// returns within the timeout; fixed, the `SKIP LOCKED` stamp is skipped
/// (0 rows affected) and the scan returns immediately, leaving the row's
/// `next_attempt_at` exactly as the lock holder will decide it, not
/// clobbered by a stale reader waiting behind it.
#[tokio::test]
async fn quota_blocked_outbox_relay_backoff_stamp_skips_a_concurrently_claimed_row() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let mut locker_conn = connect(&url).await;

    install_global_router(ShardRouter::single());
    // No pool configured for shard 1 -- the row targets it, so the scan hits
    // the missing-pool `continue` branch that calls the backoff stamp.
    let sharded_pool = Some(ShardedDbPool::single(build_test_pool(&url)));

    const UNREACHABLE_SHARD: i32 = 1;
    let unreachable_wf = leaked("outbox_backoff_lock_contention");
    let id = diesel::insert_into(harvest_completion_trigger_outbox::table)
        .values(&NewCompletionTriggerOutboxDb {
            source_exec_id: Uuid::new_v4(),
            trigger_id: Uuid::new_v4(),
            target_shard: UNREACHABLE_SHARD,
            target_workflow_name: unreachable_wf.to_string(),
            target_workflow_id: format!("target-{}", Uuid::new_v4().simple()),
            target_input: serde_json::json!({}),
            queue_name: None,
            concurrency_key: None,
            concurrency_limit: None,
            priority: serde_json::to_value(Priority::default()).unwrap(),
            max_workflow_input_bytes: 1_000_000,
        })
        .get_result::<CompletionTriggerOutboxDb>(&mut conn)
        .await
        .expect("insert outbox row")
        .id;

    let shards = [ShardId::new(UNREACHABLE_SHARD)];

    // Hold the same row-level lock `relay_gate_checked_start` would hold for
    // an entire in-flight relay, simulating a peer replica mid-relay on this
    // row when this scan reaches its missing-pool branch.
    diesel::sql_query("BEGIN")
        .execute(&mut locker_conn)
        .await
        .expect("begin locker transaction");
    #[derive(diesel::QueryableByName)]
    struct LockedId {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        #[allow(dead_code)]
        id: Uuid,
    }
    diesel::sql_query("SELECT id FROM harvest_completion_trigger_outbox WHERE id = $1 FOR UPDATE")
        .bind::<diesel::sql_types::Uuid, _>(id)
        .get_result::<LockedId>(&mut locker_conn)
        .await
        .expect("locker holds the row");

    let scan = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        enforce_completion_triggers_outbox(&mut conn, &NoOpMetrics, &sharded_pool, &shards),
    )
    .await;

    // Release the lock before asserting -- a failing assertion must not leave
    // the locker's transaction open across the rest of the test binary.
    diesel::sql_query("ROLLBACK")
        .execute(&mut locker_conn)
        .await
        .expect("rollback locker transaction");

    scan.expect(
        "the scan must not block waiting on a row a peer replica's relay \
         holds under FOR UPDATE -- a plain (non-SKIP-LOCKED) backoff stamp \
         would stall this entire scan, and every scanner duty behind it, on \
         someone else's in-flight relay (issue #1227 Finding 4, Codex \
         round-5 P2)",
    )
    .expect("outbox scan");

    assert_eq!(
        outbox_next_attempt_at(&mut conn, id).await,
        None,
        "the scan's SKIP LOCKED stamp must be skipped while a peer holds the \
         row's lock, not silently overwrite whatever next_attempt_at the \
         lock holder is about to decide"
    );

    // With the lock released, a fresh scan can now claim and back the row
    // off normally.
    enforce_completion_triggers_outbox(&mut conn, &NoOpMetrics, &sharded_pool, &shards)
        .await
        .expect("outbox scan after lock release");
    assert!(
        outbox_next_attempt_at(&mut conn, id)
            .await
            .is_some_and(|t| t > chrono::Utc::now()),
        "once the lock is released, the row must still receive its backoff \
         stamp normally"
    );
}
