#![cfg(feature = "chaos")]
//! Deterministic chaos / fault-injection reproducers and convergence sweep
//! (issue #940).
//!
//! Each of the four historical race classes is reproduced here as a *seeded /
//! scripted* fault at a named injection point (`autumn_harvest::chaos::points`),
//! driven against a real Postgres. Every reproducer FAILS on the pre-fix engine
//! shape and PASSES on the current one; the RED procedure is documented inline
//! next to each so a reviewer can reproduce the failing shape in one edit.
//!
//! The convergence sweep ([`chaos_seeded_convergence_sweep`]) runs a bounded
//! workload under `ChaosPlan::seeded(seed)` for each of N documented seeds and
//! asserts the engine still reaches the convergence invariant: every workflow
//! terminal-or-parked, no task stranded `RUNNING` with a dead worker, no
//! `ExternalSignalRequested` without an eventual terminal (issue #940 AC5).
//!
//! ## Determinism / replay (AC3)
//!
//! Every failure prints the [`ChaosGuard::diagnostics`] string, which embeds the
//! seed and the fired-action trace, so a failure is replayable with one command
//! (`CHAOS_SEEDS=<seed> cargo test --features chaos ...`).

use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::chaos::points::{
    ChaosPoint, OUTBOX_INLINE_AFTER_REQUESTED, QUEUE_PARK_BEFORE_UPDATE, SCHED_AFTER_CLAIM,
    SCHED_AFTER_START_BEFORE_ADVANCE, WORKER_PERSIST_BEFORE_COMMIT,
};
use autumn_harvest::chaos::{ChaosPlan, arm};
use autumn_harvest::prelude::*;
use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::worker::{DbPool, HandlerRegistry, chaos_drive_one_workflow_task};
use autumn_harvest::{
    DagCatalog, ExecutionId, SchedulerMonitor, ShardId, StartWorkflowParams, WorkflowIdReusePolicy,
    tick_once,
};
use chrono::Utc;
use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;

// ── Test workflows (macro-generated companions are field-growth-resilient) ──

/// Suspends on a signal — the simplest workflow that reaches the suspension
/// persist path (`WORKER_PERSIST_BEFORE_COMMIT`) and can be parked
/// (`QUEUE_PARK_BEFORE_UPDATE`).
#[workflow]
async fn chaos_wait_signal(
    ctx: &WorkflowContext,
    input: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let _ = input;
    let sig = ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    Ok(sig)
}

/// Completes in a single decision cycle — the convergence-sweep workload unit.
#[workflow]
#[allow(clippy::unused_async)] // the #[workflow] macro requires an `async fn` handler.
async fn chaos_noop(
    _ctx: &WorkflowContext,
    input: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let _ = input;
    Ok(serde_json::json!("ok"))
}

/// Sends one external signal to the `target` execution named in its input, then
/// completes — the caller that reaches `persist_external_signal_inline` and so
/// the `OUTBOX_INLINE_AFTER_REQUESTED` window (issue #492 / AC4). The target is
/// passed as an `ExecutionId` string in `input["target"]`.
#[workflow]
async fn chaos_signal_external(
    ctx: &WorkflowContext,
    input: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let target: ExecutionId = input["target"]
        .as_str()
        .ok_or("missing target")?
        .parse()
        .map_err(|e: uuid::Error| e.to_string())?;
    ctx.signal_external_workflow(target, "go", serde_json::json!({ "from": "chaos" }))
        .await
        .map_err(|e| e.to_string())?;
    Ok(serde_json::json!("signaled"))
}

// ── DB setup ────────────────────────────────────────────────────────────────

/// Serialises entire chaos-test bodies against each other on a *shared*
/// database. Acquired at the top of every chaos test (inside [`chaos_db`],
/// before the scrub) and held — via the returned guard — for the test's whole
/// lifetime.
///
/// Without this, the `HARVEST_TEST_DATABASE_URL` path run with the integration
/// binary's default (parallel) test threads is nondeterministic: [`scrub`]'s
/// global `TRUNCATE` happens *before* the harness's own `SERIAL` mutex is
/// acquired (that lock only guards armed plans, from `arm()` onward), and the
/// session-lease test never arms at all — so one test's scrub could erase
/// another test's live workflows mid-run. This lock is a strict superset of the
/// harness `SERIAL`: it also covers the pre-arm scrub and the non-arming tests,
/// so the documented shared-DB invocation is deterministic regardless of
/// `--test-threads`. On the testcontainers path each test owns its database, so
/// the only effect there is a harmless whole-suite serialisation (and CI already
/// runs `--test-threads=1`). Acquisition order is always this lock first, then
/// the harness `SERIAL` inside `arm()`, so the two can never deadlock.
static DB_BODY_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// A whole-test-body isolation guard, a live DB URL, and (when spun locally) the
/// container keeping it alive.
///
/// Honours `HARVEST_TEST_DATABASE_URL` (assumed already migrated) for fast
/// local RED/GREEN iteration; otherwise spins a fresh migrated Postgres 16
/// container (the CI path). Acquires [`DB_BODY_SERIAL`] *first* so the scrub —
/// and the whole test body, via the returned `_body` guard — runs with exclusive
/// access to a shared DB; the env DB is then scrubbed so each run starts clean.
async fn chaos_db() -> (
    tokio::sync::MutexGuard<'static, ()>,
    String,
    Option<ContainerAsync<Postgres>>,
) {
    // Held for the caller's whole body (bound as `_body`): serialises shared-DB
    // access across chaos tests, so a concurrent test's scrub can't erase this
    // test's live workflows. Must be acquired before the scrub below.
    let body = DB_BODY_SERIAL.lock().await;
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        let mut conn = AsyncPgConnection::establish(&url)
            .await
            .expect("connect to HARVEST_TEST_DATABASE_URL");
        scrub(&mut conn).await;
        (body, url, None)
    } else {
        use testcontainers::ImageExt;
        use testcontainers_modules::testcontainers::runners::AsyncRunner;
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
        (body, url, Some(container))
    }
}

/// Truncate the workflow-state tables so an env-DB run starts clean.
async fn scrub(conn: &mut AsyncPgConnection) {
    conn.batch_execute(
        "TRUNCATE harvest_workflow_executions, harvest_events, harvest_task_queue, \
         harvest_signals, harvest_timers, harvest_dead_letters, harvest_workers, \
         harvest_schedules CASCADE",
    )
    .await
    .expect("scrub");
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url)
        .await
        .expect("establish test connection")
}

/// Build the standard `StartWorkflowParams` for a chaos test workflow.
fn base_params(
    workflow_name: &'static str,
    workflow_id: &'static str,
    exec_id: ExecutionId,
    input: serde_json::Value,
) -> StartWorkflowParams<'static> {
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
        conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
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
        start_source: autumn_harvest::StartSource::Api,
        start_source_ref: None,
        started_by: None,
    }
}

/// Read a task-queue row's `(state, worker_id)` by task id.
async fn task_state(conn: &mut AsyncPgConnection, task_id: uuid::Uuid) -> (String, Option<String>) {
    use autumn_harvest::schema::harvest_task_queue::dsl;
    dsl::harvest_task_queue
        .find(task_id)
        .select((dsl::state, dsl::worker_id))
        .first(conn)
        .await
        .expect("load task state")
}

/// Read an execution's `state`.
async fn exec_state(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> String {
    use autumn_harvest::schema::harvest_workflow_executions::dsl;
    dsl::harvest_workflow_executions
        .find(exec_id.as_uuid())
        .select(dsl::state)
        .first(conn)
        .await
        .expect("load exec state")
}

/// Count `ExternalSignalRequested` events with no matching terminal for the
/// same execution — the AC5 "no `*Requested` without eventual terminal"
/// invariant probe.
async fn dangling_external_requests(conn: &mut AsyncPgConnection) -> i64 {
    diesel::sql_query(
        "SELECT COUNT(*)::bigint AS n FROM harvest_events r \
         WHERE r.event_type = 'ExternalSignalRequested' \
           AND NOT EXISTS ( \
             SELECT 1 FROM harvest_events t \
             WHERE t.workflow_exec_id = r.workflow_exec_id \
               AND t.event_type IN ('ExternalSignalDelivered', 'ExternalSignalFailed') \
           )",
    )
    .get_result::<CountRow>(conn)
    .await
    .expect("count dangling external requests")
    .n
}

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

/// A pooled `DbPool` for the scheduler tick (which takes a pool by value).
fn make_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("pool")
}

/// Insert a due workflow schedule (fires 5 s ago) and return its id. Mirrors the
/// `scheduler_ha_tests` helper so the fired execution counts are comparable.
async fn insert_due_schedule(conn: &mut AsyncPgConnection, wf_name: &str) -> uuid::Uuid {
    use autumn_harvest::schema::harvest_schedules::dsl;
    let now = Utc::now();
    let id = uuid::Uuid::new_v4();
    diesel::insert_into(dsl::harvest_schedules)
        .values((
            dsl::id.eq(id),
            dsl::workflow_name.eq(wf_name),
            dsl::schedule_expr.eq("interval:60"),
            dsl::timezone.eq("UTC"),
            dsl::catchup.eq(false),
            dsl::max_active_runs.eq(10),
            dsl::is_paused.eq(false),
            dsl::next_run_at.eq(now - chrono::Duration::seconds(5)),
            dsl::jitter_secs.eq(0_i64),
            dsl::overlap_policy.eq("skip"),
            dsl::buffered_runs.eq(serde_json::json!([])),
            dsl::buffer_all_max.eq(100),
            dsl::skip_policy.eq("skip"),
        ))
        .execute(conn)
        .await
        .expect("insert schedule");
    id
}

/// Count executions of a workflow type.
async fn exec_count(conn: &mut AsyncPgConnection, wf_name: &str) -> i64 {
    use autumn_harvest::schema::harvest_workflow_executions::dsl;
    dsl::harvest_workflow_executions
        .filter(dsl::workflow_name.eq(wf_name))
        .count()
        .get_result(conn)
        .await
        .expect("count executions")
}

/// Read a schedule's `(fire_claim_token, live)` where `live` is true iff the
/// claim is held and unexpired.
async fn schedule_claim(
    conn: &mut AsyncPgConnection,
    id: uuid::Uuid,
) -> (Option<uuid::Uuid>, bool) {
    use autumn_harvest::schema::harvest_schedules::dsl;
    let (token, until): (Option<uuid::Uuid>, Option<chrono::DateTime<Utc>>) =
        dsl::harvest_schedules
            .find(id)
            .select((dsl::fire_claim_token, dsl::fire_claimed_until))
            .first(conn)
            .await
            .expect("load schedule claim");
    let live = token.is_some() && until.is_some_and(|u| u > Utc::now());
    (token, live)
}

/// Count events of a given `event_type` on one execution.
async fn event_count(conn: &mut AsyncPgConnection, exec_id: ExecutionId, event_type: &str) -> i64 {
    diesel::sql_query(
        "SELECT COUNT(*)::bigint AS n FROM harvest_events \
         WHERE workflow_exec_id = $1 AND event_type = $2",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(event_type)
    .get_result::<CountRow>(conn)
    .await
    .expect("count events")
    .n
}

/// Count queued signal rows for a target execution — the immediate delivery
/// artifact (`harvest_signals`) the inline path writes via `send_signal_*`.
async fn signals_for(conn: &mut AsyncPgConnection, target: ExecutionId) -> i64 {
    use autumn_harvest::schema::harvest_signals::dsl;
    dsl::harvest_signals
        .filter(dsl::workflow_exec_id.eq(target.as_uuid()))
        .count()
        .get_result(conn)
        .await
        .expect("count signals")
}

// ── Reproducer 1 — issue #601 lost-wake (`wake_requested`) window ────────────

/// A wake that lands while the task is still claimed (mid-cycle) must not be
/// lost. HOLD the park at `QUEUE_PARK_BEFORE_UPDATE` (the row is still `RUNNING`
/// with its `worker_id`), fire a concurrent `wake_workflow_task` — whose primary
/// re-pend matches zero rows and must fall back to `wake_requested = TRUE` —
/// then release: the park atomically reads-and-clears `wake_requested` and
/// reports it, so the caller re-pends instead of stranding the task parked.
///
/// RED procedure: revert `queue::park_workflow_task_inner`'s `candidate` CTE to
/// stop reading `wake_requested` (return `false`), or drop
/// `wake_workflow_task`'s fallback UPDATE — the assert `had_wake_requested`
/// then fails, and the task stays parked with the wake lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// `guard` intentionally lives to end-of-scope: it keeps the harness armed and
// holds the process-wide chaos serialization lock for the whole test body, and
// its `diagnostics()`/`hits()` are read by the trailing asserts.
#[allow(clippy::significant_drop_tightening)]
async fn chaos_repro_601_lost_wake_is_recovered_via_wake_requested() {
    let (_body, url, _c) = chaos_db().await;
    let mut conn = connect(&url).await;

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let params = base_params(
        "chaos_wait_signal",
        "c601-wf",
        exec_id,
        serde_json::json!(null),
    );
    autumn_harvest::execution::start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("start chaos_wait_signal");

    // Claim the workflow task → RUNNING with a worker_id, exactly the state a
    // decision cycle holds when it decides to park.
    let task = autumn_harvest::queue::claim_task(
        &mut conn,
        &["default".to_string()],
        "c601-worker",
        "",
        None,
        &[],
        &[],
    )
    .await
    .expect("claim")
    .expect("a workflow task must be claimable");

    let guard = arm(ChaosPlan::scripted().hold_at(QUEUE_PARK_BEFORE_UPDATE)).await;
    let hold = guard.hold(QUEUE_PARK_BEFORE_UPDATE);

    // Park on its own owned connection so the HOLD can block it without
    // borrowing our connection.
    let park_url = url.clone();
    let task_id = task.id;
    let park = tokio::spawn(async move {
        let mut park_conn = connect(&park_url).await;
        autumn_harvest::queue::park_workflow_task(&mut park_conn, task_id, None).await
    });

    // The park has reached the rendezvous with the row still RUNNING+worker_id.
    hold.reached().await;

    // A concurrent wake: primary re-pend matches 0 rows (row not yet parked),
    // so it must fall back to marking `wake_requested = TRUE`.
    autumn_harvest::queue::wake_workflow_task(&mut conn, exec_id)
        .await
        .expect("concurrent wake");

    hold.release();
    let had_wake_requested = park.await.expect("park task join").expect("park succeeds");

    assert!(
        had_wake_requested,
        "the mid-cycle wake must be recovered via wake_requested, not lost; {}",
        guard.diagnostics()
    );
    assert_eq!(
        guard.hits(QUEUE_PARK_BEFORE_UPDATE),
        1,
        "the park injection point must have been hit exactly once; {}",
        guard.diagnostics()
    );
    assert!(
        guard.actions_fired() >= 1,
        "the HOLD must have fired (anti-vacuity); {}",
        guard.diagnostics()
    );

    // Convergence: the caller re-pends *because* `had_wake_requested` came back
    // true — faithfully modelling the worker's park path, which ORs the returned
    // flag into its re-wake decision rather than re-pending unconditionally. On
    // the pre-fix shape the flag would be false and this branch would be skipped,
    // leaving the task stranded parked (the RED shape).
    if had_wake_requested {
        autumn_harvest::queue::wake_workflow_task(&mut conn, exec_id)
            .await
            .expect("re-pend after wake_requested");
    }
    let (state, worker) = task_state(&mut conn, task.id).await;
    assert_eq!(
        state,
        "PENDING",
        "task must be re-pended, not stranded; {}",
        guard.diagnostics()
    );
    assert!(
        worker.is_none(),
        "re-pended task must have no worker_id; {}",
        guard.diagnostics()
    );
}

// ── Reproducer 2 — issue #367 poison-pill orphan reclaim ─────────────────────

/// A KILL mid-decision-cycle (before the persist commit) leaves the task
/// stranded `RUNNING` with a now-dead worker. The poison-pill reclaimer must
/// recover it: increment `crash_strikes` and re-queue it `PENDING` (under the
/// threshold). This exercises the KILL capability (AC1(a)) and the #367 fix.
///
/// RED procedure (production-shape revert, symmetric with the other three):
/// short-circuit `poison_pill::reclaim_orphaned_tasks` to an early
/// `return Ok(ReclaimSummary::default())` (the pre-#367 shape, before the
/// reclaimer existed). The KILL-orphaned task then stays `RUNNING` with a dead
/// worker forever and the `PENDING` convergence assertion fails. (Equivalently,
/// test-side: comment out the `reclaim_orphaned_tasks` call below *and its two
/// `summary` asserts* — same failing shape, no production edit.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// `_body` (the shared-DB isolation guard from `chaos_db`) intentionally lives to
// end-of-scope; see `DB_BODY_SERIAL`.
#[allow(clippy::significant_drop_tightening)]
async fn chaos_repro_367_crash_orphan_is_reclaimed() {
    let (_body, url, _c) = chaos_db().await;
    let mut conn = connect(&url).await;

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let params = base_params(
        "chaos_wait_signal",
        "c367-wf",
        exec_id,
        serde_json::json!(null),
    );
    autumn_harvest::execution::start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("start chaos_wait_signal");

    let registry = Arc::new(HandlerRegistry::new(vec![chaos_wait_signal_info()], vec![]));

    // Claim → RUNNING with the (soon-to-crash) worker id.
    let task = autumn_harvest::queue::claim_task(
        &mut conn,
        &["default".to_string()],
        "c367-crash-worker",
        "",
        None,
        &[],
        &[],
    )
    .await
    .expect("claim")
    .expect("workflow task claimable");

    // Drive the cycle with a KILL at the persist point → crash, owned conn drop
    // → server rollback → row stranded RUNNING with the dead worker.
    let guard = arm(ChaosPlan::scripted().kill_at(WORKER_PERSIST_BEFORE_COMMIT)).await;
    let outcome = chaos_drive_one_workflow_task(
        &url,
        Arc::clone(&registry),
        task.clone(),
        "c367-crash-worker".to_string(),
    )
    .await;
    assert!(
        outcome.is_err(),
        "the KILL must crash the decision cycle (JoinError); {}",
        guard.diagnostics()
    );
    assert!(
        guard.actions_fired() >= 1,
        "the KILL must have fired (anti-vacuity); {}",
        guard.diagnostics()
    );
    let diag = guard.diagnostics();
    drop(guard);

    // The orphan condition: RUNNING, still owned by the dead worker, no forward
    // progress (the persist rolled back).
    let (state, worker) = task_state(&mut conn, task.id).await;
    assert_eq!(
        state, "RUNNING",
        "crash must strand the task RUNNING; {diag}"
    );
    assert_eq!(worker.as_deref(), Some("c367-crash-worker"), "{diag}");

    // The dead worker was never registered → no live heartbeat → orphaned.
    let summary = autumn_harvest::poison_pill::reclaim_orphaned_tasks(
        &mut conn,
        3,
        0,
        None,
        &NoOpMetrics,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("reclaim");
    assert_eq!(
        summary.requeued, 1,
        "the orphan must be re-queued once; {diag}"
    );
    assert_eq!(summary.quarantined, 0, "{diag}");

    let (state, worker) = task_state(&mut conn, task.id).await;
    assert_eq!(
        state, "PENDING",
        "the orphan must be recovered to PENDING; {diag}"
    );
    assert!(
        worker.is_none(),
        "recovered task must have no worker_id; {diag}"
    );
}

// ── Reproducer 3 — issue #492 outbox vs inline external-signal persist ───────

/// A same-shard external signal appends `ExternalSignalRequested` and its
/// `ExternalSignalDelivered` terminal inside ONE transaction
/// (`persist_external_signal_inline`, the #492 fix). HOLD at
/// `OUTBOX_INLINE_AFTER_REQUESTED` — the `Requested` event is written but the
/// txn is uncommitted — and run the background outbox on a separate connection:
/// under READ COMMITTED it cannot observe the half-written request, so it
/// delivers nothing (returns 0) and the signal lands exactly once. Exercises the
/// ERROR-free HOLD/DELAY window (AC1(b)/AC1(d) shape) and the #492 fix (AC4).
///
/// RED procedure: remove the `conn.transaction(...)` wrapper in
/// `persist_external_signal_inline` so `ExternalSignalRequested` commits in its
/// own statement before the terminal. During the hold the outbox then sees a
/// committed `Requested`-without-terminal on a `RUNNING` execution and delivers
/// it a SECOND time — two `harvest_signals` rows for the target and a second
/// `ExternalSignalDelivered` — and the `== 1` / `outbox == 0` asserts fail.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
// linear reproducer: setup, hold, outbox probe, release, exactly-once asserts.
// `_body` (the shared-DB isolation guard from `chaos_db`) intentionally lives to
// end-of-scope; see `DB_BODY_SERIAL`.
#[allow(clippy::significant_drop_tightening)]
async fn chaos_repro_492_outbox_cannot_double_deliver_inline_external_signal() {
    let (_body, url, _c) = chaos_db().await;
    let mut conn = connect(&url).await;

    // Target (same shard 0) — a live RUNNING execution to receive the signal.
    // Parked on its own queue so the `default` claim below can only pick the
    // caller (the target's task is never polled here).
    let target_id = ExecutionId::new_for_shard(ShardId::new(0));
    let mut target_params = base_params(
        "chaos_wait_signal",
        "c492-target",
        target_id,
        serde_json::json!(null),
    );
    target_params.queue_name = "chaos-target-q";
    autumn_harvest::execution::start_or_load_workflow_execution(&mut conn, target_params, None)
        .await
        .expect("start target");

    // Caller (same shard 0) that signals the target.
    let caller_id = ExecutionId::new_for_shard(ShardId::new(0));
    let caller_input = serde_json::json!({ "target": target_id.to_string() });
    autumn_harvest::execution::start_or_load_workflow_execution(
        &mut conn,
        base_params(
            "chaos_signal_external",
            "c492-caller",
            caller_id,
            caller_input,
        ),
        None,
    )
    .await
    .expect("start caller");

    let registry = Arc::new(HandlerRegistry::new(
        vec![chaos_signal_external_info(), chaos_wait_signal_info()],
        vec![],
    ));

    // Claim the caller's workflow task for the drive worker.
    let task = autumn_harvest::queue::claim_task(
        &mut conn,
        &["default".to_string()],
        "c492-caller-worker",
        "",
        None,
        &[],
        &[],
    )
    .await
    .expect("claim")
    .expect("caller task claimable");
    assert_eq!(
        task.workflow_exec_id,
        Some(caller_id.as_uuid()),
        "must claim the caller task (target parks on its signal)"
    );

    // HOLD inside the inline persist txn, after Requested is appended.
    let guard = arm(ChaosPlan::scripted().hold_at(OUTBOX_INLINE_AFTER_REQUESTED)).await;
    let hold = guard.hold(OUTBOX_INLINE_AFTER_REQUESTED);

    let drive_url = url.clone();
    let drive_registry = Arc::clone(&registry);
    let drive = tokio::spawn(async move {
        chaos_drive_one_workflow_task(
            &drive_url,
            drive_registry,
            task,
            "c492-caller-worker".to_string(),
        )
        .await
    });

    hold.reached().await;

    // The outbox on a separate connection cannot see the uncommitted Requested —
    // the #492 invariant: zero deliveries during the open inline txn.
    let delivered = autumn_harvest::timeout::enforce_external_signals_outbox(
        &mut conn,
        &NoOpMetrics,
        Duration::from_secs(300),
        &None,
        &[],
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("outbox sweep");
    assert_eq!(
        delivered,
        0,
        "the outbox must not observe (or double-deliver) the half-written inline \
         signal; {}",
        guard.diagnostics()
    );
    assert_eq!(
        signals_for(&mut conn, target_id).await,
        0,
        "no signal may be queued to the target while the inline txn is open; {}",
        guard.diagnostics()
    );

    hold.release();
    let outcome = drive.await.expect("drive join").expect("drive spawn join");
    outcome.expect("caller cycle must persist cleanly");

    assert_eq!(
        guard.hits(OUTBOX_INLINE_AFTER_REQUESTED),
        1,
        "the inline persist point must be hit exactly once; {}",
        guard.diagnostics()
    );
    assert!(
        guard.actions_fired() >= 1,
        "the HOLD must have fired (anti-vacuity); {}",
        guard.diagnostics()
    );
    let diag = guard.diagnostics();
    drop(guard);

    // Exactly-once: one Requested + one Delivered on the caller, one queued
    // signal on the target. A double-delivery would show 2 signals / 2 Delivered.
    assert_eq!(
        event_count(&mut conn, caller_id, "ExternalSignalRequested").await,
        1,
        "exactly one ExternalSignalRequested; {diag}"
    );
    assert_eq!(
        event_count(&mut conn, caller_id, "ExternalSignalDelivered").await,
        1,
        "exactly one ExternalSignalDelivered; {diag}"
    );
    assert_eq!(
        signals_for(&mut conn, target_id).await,
        1,
        "the signal must land on the target exactly once; {diag}"
    );
    assert_eq!(
        dangling_external_requests(&mut conn).await,
        0,
        "no ExternalSignalRequested without a terminal; {diag}"
    );
}

// ── Reproducer 4 — issue #350 schedule fire claim expiring mid-fire ──────────

/// A scheduler replica that crashes after winning the HA claim but before firing
/// must leave the slot fire-able by a healthy peer *exactly once*, and no peer
/// may double-fire while the claim is still live. KILL at `SCHED_AFTER_CLAIM`
/// (the claim UPDATE has committed in autocommit, the fire has not) via a spawned
/// `tick_once` → `JoinError`; the live claim then blocks a peer tick, and only
/// after the 30 s claim TTL expires does a peer fire the slot once (AC4).
///
/// RED procedure: remove the HA claim guard in
/// `claim_and_fire_workflow_schedule` (fire unconditionally, ignore
/// `fire_claim_token`/`fire_claimed_until`). The live-claim peer tick then fires
/// a SECOND time while the first fire is still in flight — the `exec_count == 0`
/// (live-claim) or final `== 1` (single fire) assertion fails with 2 executions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// `_body` (the shared-DB isolation guard from `chaos_db`) intentionally lives to
// end-of-scope; see `DB_BODY_SERIAL`.
#[allow(clippy::significant_drop_tightening)]
async fn chaos_repro_350_crashed_fire_claim_is_refired_exactly_once() {
    let (_body, url, _c) = chaos_db().await;
    let wf = "chaos_sched_350";
    let sched_id = {
        let mut conn = connect(&url).await;
        insert_due_schedule(&mut conn, wf).await
    };
    let registry = Arc::new(HandlerRegistry::new(vec![chaos_noop_info()], vec![]));

    // Crash mid-fire: KILL after the claim commits, before the fire. The tick is
    // spawned so the panic surfaces as a JoinError instead of aborting the test.
    let guard = arm(ChaosPlan::scripted().kill_at(SCHED_AFTER_CLAIM)).await;
    let crash = tokio::spawn(tick_once(
        make_pool(&url),
        Arc::clone(&registry),
        Arc::new(DagCatalog::default()),
        Arc::new(vec![]),
        SchedulerMonitor::offline(),
    ))
    .await;
    assert!(
        crash.is_err(),
        "the KILL must crash the tick mid-fire (JoinError); {}",
        guard.diagnostics()
    );
    assert!(
        guard.actions_fired() >= 1,
        "the KILL must have fired (anti-vacuity); {}",
        guard.diagnostics()
    );
    let diag = guard.diagnostics();
    drop(guard);

    // The crash left the claim held (committed in autocommit) with no fire.
    {
        let mut conn = connect(&url).await;
        let (token, live) = schedule_claim(&mut conn, sched_id).await;
        assert!(
            token.is_some(),
            "the HA claim must be committed by the crash; {diag}"
        );
        assert!(
            live,
            "the claim TTL must still be live right after the crash; {diag}"
        );
        assert_eq!(
            exec_count(&mut conn, wf).await,
            0,
            "the crash must have fired nothing; {diag}"
        );
    }

    // A healthy peer tick while the claim is live must NOT double-fire.
    tick_once(
        make_pool(&url),
        Arc::clone(&registry),
        Arc::new(DagCatalog::default()),
        Arc::new(vec![]),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("peer tick (live claim)");
    {
        let mut conn = connect(&url).await;
        assert_eq!(
            exec_count(&mut conn, wf).await,
            0,
            "a live claim must block the peer from firing; {diag}"
        );
    }

    // Expire the claim (the 30 s TTL elapses) → a peer re-fires the slot ONCE.
    {
        let mut conn = connect(&url).await;
        diesel::sql_query(
            "UPDATE harvest_schedules SET fire_claimed_until = NOW() - INTERVAL '1 minute' \
             WHERE id = $1",
        )
        .bind::<diesel::sql_types::Uuid, _>(sched_id)
        .execute(&mut conn)
        .await
        .expect("expire claim");
    }
    tick_once(
        make_pool(&url),
        registry,
        Arc::new(DagCatalog::default()),
        Arc::new(vec![]),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("peer tick (expired claim)");

    let mut conn = connect(&url).await;
    assert_eq!(
        exec_count(&mut conn, wf).await,
        1,
        "the crashed slot must be re-fired by a peer exactly once after the claim expires; {diag}"
    );
}

// ── Reproducer 4b — issue #350 post-start crash (SCHED_AFTER_START_BEFORE_ADVANCE) ──

/// The #350 double-fire window *after* the start commits. A replica that crashes
/// after firing this tick's scheduled start — but before advancing
/// `next_run_at`/`last_run_at` — leaves the slot still "due" with an execution
/// already created. Recovery must NOT create a *second* execution: the dedupe
/// (the deterministic `sched:{id}:{name}:{slot}` workflow id) attaches the
/// peer's re-fire to the run the crashed replica already started, so the slot
/// yields **exactly one** execution (AC4).
///
/// This complements `chaos_repro_350_crashed_fire_claim_is_refired_exactly_once`
/// (which KILLs at `SCHED_AFTER_CLAIM`, *before* any start, so its crash fires
/// nothing) by arming the sibling `SCHED_AFTER_START_BEFORE_ADVANCE` point: the
/// crash lands *after* one start committed (`exec_count == 1` right after the
/// `JoinError`), directly exercising the post-start window that point exists to
/// model — and, by construction, only reachable because a start committed
/// (`dispatched > 0`).
///
/// RED procedure: break the per-slot dedupe so the expired-claim peer re-fire
/// creates a fresh run — e.g. change the scheduled start's reuse policy away from
/// the sched-id idempotent path (return `outcome.created() == true` on the
/// re-fire). The final `exec_count == 1` assertion then fails with 2 executions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// `_body` (the shared-DB isolation guard from `chaos_db`) intentionally lives to
// end-of-scope; see `DB_BODY_SERIAL`.
#[allow(clippy::significant_drop_tightening)]
async fn chaos_repro_350_post_start_crash_dedupes_to_exactly_one() {
    let (_body, url, _c) = chaos_db().await;
    let wf = "chaos_sched_350_post_start";
    let sched_id = {
        let mut conn = connect(&url).await;
        insert_due_schedule(&mut conn, wf).await
    };
    let registry = Arc::new(HandlerRegistry::new(vec![chaos_noop_info()], vec![]));

    // Crash AFTER the start commits, BEFORE next_run_at advances. Only reachable
    // because a start committed this tick (the `dispatched > 0` gate); the tick is
    // spawned so the panic surfaces as a JoinError instead of aborting the test.
    let guard = arm(ChaosPlan::scripted().kill_at(SCHED_AFTER_START_BEFORE_ADVANCE)).await;
    let crash = tokio::spawn(tick_once(
        make_pool(&url),
        Arc::clone(&registry),
        Arc::new(DagCatalog::default()),
        Arc::new(vec![]),
        SchedulerMonitor::offline(),
    ))
    .await;
    assert!(
        crash.is_err(),
        "the KILL must crash the tick after the start (JoinError); {}",
        guard.diagnostics()
    );
    assert!(
        guard.actions_fired() >= 1,
        "the KILL must have fired (anti-vacuity); {}",
        guard.diagnostics()
    );
    let diag = guard.diagnostics();
    drop(guard);

    // The crash committed exactly one start (the point fires AFTER the start), but
    // did NOT advance the schedule — so the claim is still held and the slot is
    // still due. This is the post-start window `SCHED_AFTER_CLAIM` cannot reach.
    {
        let mut conn = connect(&url).await;
        assert_eq!(
            exec_count(&mut conn, wf).await,
            1,
            "the crash must have committed exactly one start before the kill; {diag}"
        );
        let (token, live) = schedule_claim(&mut conn, sched_id).await;
        assert!(
            token.is_some() && live,
            "the HA claim must still be held+live right after the post-start crash; {diag}"
        );
    }

    // A healthy peer tick while the claim is live must NOT fire again.
    tick_once(
        make_pool(&url),
        Arc::clone(&registry),
        Arc::new(DagCatalog::default()),
        Arc::new(vec![]),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("peer tick (live claim)");
    {
        let mut conn = connect(&url).await;
        assert_eq!(
            exec_count(&mut conn, wf).await,
            1,
            "a live claim must block the peer from firing again; {diag}"
        );
    }

    // Expire the claim → a peer re-fires the still-due slot. The per-slot dedupe
    // must attach to the already-started run, NOT create a second execution.
    {
        let mut conn = connect(&url).await;
        diesel::sql_query(
            "UPDATE harvest_schedules SET fire_claimed_until = NOW() - INTERVAL '1 minute' \
             WHERE id = $1",
        )
        .bind::<diesel::sql_types::Uuid, _>(sched_id)
        .execute(&mut conn)
        .await
        .expect("expire claim");
    }
    tick_once(
        make_pool(&url),
        registry,
        Arc::new(DagCatalog::default()),
        Arc::new(vec![]),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("peer tick (expired claim)");

    let mut conn = connect(&url).await;
    assert_eq!(
        exec_count(&mut conn, wf).await,
        1,
        "a post-start crash must yield exactly one execution through recovery (no double-fire); {diag}"
    );
}

// ── AC1(d) — expire a lease/heartbeat early ──────────────────────────────────

/// AC1(d) names four lease/heartbeat surfaces to expire early:
///
/// - **schedule fire claim** — [`chaos_repro_350_crashed_fire_claim_is_refired_exactly_once`]
///   crashes the claiming replica mid-fire, then deterministically expires the
///   dead claim (a test-side `UPDATE harvest_schedules SET fire_claimed_until`
///   into the past) and asserts a healthy peer re-fires exactly once.
/// - **worker liveness heartbeat** — [`chaos_repro_367_crash_orphan_is_reclaimed`]
///   leaves a `RUNNING` task owned by a worker with no live `harvest_workers`
///   heartbeat, and asserts `reclaim_orphaned_tasks` reclaims it.
/// - **session lease** — *this test* (below).
/// - **delivery in-flight lease** — the identical technique: a `harvest_completion_deliveries`
///   row left `INFLIGHT` with `next_attempt_at` in the past is re-claimed by the
///   delivery scanner's `state IN ('PENDING','INFLIGHT') AND next_attempt_at <= NOW()`
///   candidate CTE (`completion_callback::claim_due_deliveries`). It is not
///   reproduced as a dedicated chaos test here because the delivery scanner
///   performs real HTTP POSTs (a network dependency out of scope for a
///   deterministic, offline chaos reproducer).
///
/// This test demonstrates the early-lease-expiry capability for the **session
/// lease** deterministically and offline: an `ACTIVE` `harvest_sessions` row
/// whose host worker is provably live (fresh heartbeat, `Active` status) and
/// whose owning workflow is non-terminal, but whose `expires_at` is in the past,
/// is reclaimed by `enforce_broken_sessions` as `BROKEN` with the `LeaseExpired`
/// reason — isolating the lease-expiry path from the (higher-priority)
/// dead-host / draining / terminal-workflow reasons.
///
/// By design this lease-expiry surface is exercised *without* the point-injection
/// framework (`chaos::points` / `arm` / `ChaosPlan`): "expire a lease early" is a
/// durable DB-column edit (`expires_at` into the past), not a mid-execution
/// interception, so a direct row `UPDATE` is the faithful, deterministic way to
/// model it — the same approach the schedule-fire-claim reproducer takes for its
/// `fire_claimed_until` lease. The injection framework covers the `KILL` /
/// `ERROR` / `DROP_NOTIFY` / `DELAY` surfaces (AC1 a–c); the lease surfaces
/// (AC1 d) are modelled by expiring the durable lease column directly.
///
/// RED procedure: revert the `s.expires_at < NOW()` disjunct in
/// `sessions::broken_session_candidates_query()` (or `lease_expired` in
/// `resolve_broken_reason`) and the session stays `ACTIVE` — the assert fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
// `_body` (the shared-DB isolation guard from `chaos_db`) intentionally lives to
// end-of-scope; see `DB_BODY_SERIAL`.
#[allow(clippy::significant_drop_tightening)]
async fn chaos_ac1d_session_lease_expiry_marks_broken() {
    let (_body, url, _c) = chaos_db().await;
    let mut conn = connect(&url).await;

    // A non-terminal (RUNNING) owning workflow — so the break is not attributed
    // to `OwningWorkflowTerminal`.
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let params = base_params(
        "chaos_noop",
        "ac1d-session",
        exec_id,
        serde_json::json!(null),
    );
    autumn_harvest::execution::start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("start owning workflow");

    // A provably-live host worker: fresh `last_heartbeat_at`, `Active` status —
    // so the break is not attributed to a dead/draining host.
    let host = "ac1d-session-host";
    autumn_harvest::workers::register_worker(
        &mut conn,
        host,
        &["default".to_string()],
        &[0],
        4,
        "localhost",
        None,
        "",
        None,
        &std::collections::HashMap::new(),
        1,
        &[],
    )
    .await
    .expect("register live host worker");

    // An `ACTIVE` session whose lease is already in the past — the AC1(d)
    // early-lease-expiry we are exercising.
    let session_id = autumn_harvest::types::SessionId::new();
    let expired_at = Utc::now() - chrono::Duration::seconds(60);
    let outcome = autumn_harvest::sessions::record_session_acquired(
        &mut conn, session_id, exec_id, host, "default", expired_at,
    )
    .await
    .expect("record session with an expired lease");
    // The row inserts `ACTIVE` regardless of the lease (the scanner, not the
    // insert, breaks it) — sanity-check that precondition.
    assert!(
        matches!(
            outcome,
            autumn_harvest::sessions::SessionAcquireRecordOutcome::Active { .. }
        ),
        "session must insert ACTIVE before the scanner runs; got {outcome:?}"
    );

    // Run the broken-session scanner with a generous worker-staleness window
    // (120 s) so the fresh host heartbeat is NOT stale — the only broken reason
    // that can apply is the expired lease.
    let member_tasks_failed = autumn_harvest::sessions::enforce_broken_sessions(
        &mut conn,
        120,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("enforce_broken_sessions");
    // `enforce_broken_sessions` returns the count of member *tasks* failed, not
    // the count of sessions reclaimed. This session is intentionally memberless
    // (it seeds no `harvest_task_queue` rows) to isolate the pure lease-expiry
    // path, so the correct return is 0 even though the session row IS
    // transitioned to `BROKEN` below. Asserting the documented member-task
    // semantics here is itself a regression guard against that contract drifting.
    assert_eq!(
        member_tasks_failed, 0,
        "a memberless reclaimed session fails zero member tasks",
    );

    // The AC1(d) evidence: the ACTIVE→BROKEN transition attributed to the
    // expired lease. The row was proven ACTIVE above (the `record_session_acquired`
    // outcome), and the scanner is the only thing that ran since, so this
    // transition is caused solely by the early lease expiry.
    let (state, reason): (String, Option<String>) =
        diesel::sql_query("SELECT state, broken_reason FROM harvest_sessions WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(session_id.as_uuid())
            .get_result::<SessionStateRow>(&mut conn)
            .await
            .map(|r| (r.state, r.broken_reason))
            .expect("read session state");
    assert_eq!(state, "BROKEN", "the expired-lease session must be BROKEN");
    assert_eq!(
        reason.as_deref(),
        Some("session lease expired"),
        "the break must be attributed to the expired lease, not a dead/draining host",
    );
}

#[derive(diesel::QueryableByName)]
struct SessionStateRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    state: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    broken_reason: Option<String>,
}

// ── AC5 — seeded convergence sweep ───────────────────────────────────────────

/// The point whose KILL actually *strands an orphan* the recovery loop must
/// clean up. The sweep drives single-cycle `chaos_noop` workflows, so both
/// worker persist points are hit — but only a KILL at
/// `WORKER_PERSIST_BEFORE_COMMIT` (before the decision-cycle commit, while the
/// claim's `state='RUNNING'` is already durable) leaves a `RUNNING` row with a
/// dead worker: exactly the #367 recovery path the convergence invariant
/// exercises. A KILL at `WORKER_AFTER_OUTER_COMMIT` is post-commit (the
/// execution is already `COMPLETED`), and a `Delay` merely perturbs timing —
/// both are *convergence-benign* and would let the sweep assert convergence for
/// an effectively un-faulted run. So the non-vacuity predicate demands this
/// disruptive fault specifically, not just "any activation" (issue #940, review
/// P2-1). The other five catalogue points are each covered precisely by a
/// dedicated reproducer above; a parking/external-signal/scheduler workload
/// cannot be folded into this sweep because a seeded plan never selects `Hold`
/// and delivers no signals, so a parked workflow would never reach `COMPLETED`.
const WORKLOAD_ORPHAN_POINT: ChaosPoint = WORKER_PERSIST_BEFORE_COMMIT;

/// How many default seeds to run (AC5 requires N ≥ 5).
const DEFAULT_SWEEP_SEED_COUNT: usize = 7;

/// A seed is non-vacuous for this sweep iff its seeded plan arms a **KILL** at
/// [`WORKLOAD_ORPHAN_POINT`] — a disruptive pre-commit crash, not a
/// convergence-benign `Delay` or a post-commit kill. Seeded triggers are always
/// `OnHit(1..=3)` and the 6-workflow workload produces one hit on that point per
/// drive (≥ 6 hits total), so the armed KILL is guaranteed to fire exactly once
/// and strand exactly one orphan — making this a sound, guaranteed predicate for
/// the pre-recovery orphan assertion in the sweep.
fn seed_strands_an_orphan(seed: u64) -> bool {
    ChaosPlan::seeded(seed).kills_at(WORKLOAD_ORPHAN_POINT)
}

/// The seed set for the sweep. Every default seed strands an orphan against the
/// workload; the sweep asserts a real orphan exists pre-recovery (and that a
/// fault fired) per seed, so a convergence-benign seed fails loudly rather than
/// passing convergence for a healthy run.
///
/// - **Default (the CI path):** the first [`DEFAULT_SWEEP_SEED_COUNT`] seeds
///   (from 1 upward) that [`seed_strands_an_orphan`] against the workload —
///   **computed**, not hardcoded, so it is non-vacuous *by construction* (no
///   magic numbers that could silently go vacuous if the seeded logic or the
///   catalogue changes) and ≥ 5 by construction (AC5). Fully deterministic and
///   reproducible (AC3).
/// - **Explicit override (`CHAOS_SEEDS="8"`, replay):** trusted verbatim, any
///   count. AC3 wants one-command single-seed replay, so the ≥ 5 floor is *not*
///   imposed on an operator-chosen override — the per-seed anti-vacuity assert in
///   the sweep still protects correctness, and a single failing seed printed by a
///   CI failure can be replayed directly.
fn sweep_seeds() -> Vec<u64> {
    if let Some(seeds) = env_override_seeds() {
        return seeds;
    }
    let seeds = default_sweep_seeds();
    assert!(
        seeds.len() >= 5,
        "AC5 requires N >= 5 distinct seeds; the computed default is {} ({seeds:?})",
        seeds.len(),
    );
    seeds
}

/// Parse an explicit `CHAOS_SEEDS="1 2 3 ..."` override, or `None` when unset or
/// empty (fall back to the computed default).
fn env_override_seeds() -> Option<Vec<u64>> {
    let raw = std::env::var("CHAOS_SEEDS").ok()?;
    let seeds: Vec<u64> = raw
        .split_whitespace()
        .filter_map(|t| t.parse::<u64>().ok())
        .collect();
    (!seeds.is_empty()).then_some(seeds)
}

/// The computed default seed set: the first [`DEFAULT_SWEEP_SEED_COUNT`]
/// non-vacuous seeds from 1 upward.
fn default_sweep_seeds() -> Vec<u64> {
    (1u64..)
        .filter(|&s| seed_strands_an_orphan(s))
        .take(DEFAULT_SWEEP_SEED_COUNT)
        .collect()
}

/// AC5 (default path) is guaranteed *by construction*, not by a magic list: the
/// computed default is at least 5 distinct seeds, and each strands an orphan
/// against the sweep's workload (so the per-seed pre-recovery orphan assert in
/// [`chaos_seeded_convergence_sweep`] can never fail vacuously for the default
/// set). No DB needed — this is a pure property of the deterministic seeded
/// logic.
#[test]
fn default_sweep_seeds_are_at_least_five_and_strand_an_orphan() {
    let seeds = default_sweep_seeds();
    assert!(
        seeds.len() >= 5,
        "AC5 requires >= 5 default seeds; computed {} ({seeds:?})",
        seeds.len(),
    );
    let distinct: std::collections::BTreeSet<u64> = seeds.iter().copied().collect();
    assert_eq!(
        distinct.len(),
        seeds.len(),
        "default seeds must be distinct"
    );
    for &s in &seeds {
        assert!(
            seed_strands_an_orphan(s),
            "default seed {s} is convergence-benign against the workload (no pre-commit KILL)",
        );
    }
}

/// For each seed: arm a randomised plan, drive a bounded workload of
/// single-cycle workflows (KILLs caught as `JoinError`), disarm, run the
/// recovery loop (reclaim orphans + re-drive), and assert the convergence
/// invariant — every workflow terminal, no task stranded `RUNNING` with a dead
/// worker, no dangling `ExternalSignalRequested`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// `_body` (the shared-DB isolation guard from `chaos_db`) intentionally lives to
// end-of-scope; see `DB_BODY_SERIAL`.
#[allow(clippy::significant_drop_tightening)]
// Issue #1459: one line added to a `reclaim_orphaned_tasks` call site pushed
// this test one line over the limit.
#[allow(clippy::too_many_lines)]
async fn chaos_seeded_convergence_sweep() {
    const WORKLOAD: usize = 6;

    let (_body, url, _c) = chaos_db().await;
    let registry = Arc::new(HandlerRegistry::new(vec![chaos_noop_info()], vec![]));

    for seed in sweep_seeds() {
        // Whether this seed is *guaranteed* to strand a pre-commit orphan (true
        // for every computed default seed; possibly false for an arbitrary
        // operator override). When true we additionally assert an orphan really
        // existed pre-recovery — the direct proof that the recovery loop had
        // real work, not just that a directive fired (review P2-1).
        let expects_orphan = seed_strands_an_orphan(seed);

        // Fresh DB slate per seed so the invariant probe is unambiguous.
        {
            let mut conn = connect(&url).await;
            scrub(&mut conn).await;
        }

        // Start the workload.
        let mut execs = Vec::new();
        {
            let mut conn = connect(&url).await;
            for i in 0..WORKLOAD {
                let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
                let wid: &'static str = Box::leak(format!("sweep-{seed}-{i}").into_boxed_str());
                let params = base_params("chaos_noop", wid, exec_id, serde_json::json!(null));
                autumn_harvest::execution::start_or_load_workflow_execution(
                    &mut conn, params, None,
                )
                .await
                .expect("start sweep workflow");
                execs.push(exec_id);
            }
        }

        // Drive every task once under the seeded plan; KILLs surface as
        // JoinErrors and are swallowed (a crashed cycle is a valid fault).
        let guard = arm(ChaosPlan::seeded(seed)).await;
        for i in 0..WORKLOAD {
            let mut conn = connect(&url).await;
            let worker: &'static str = Box::leak(format!("sweepw-{seed}-{i}").into_boxed_str());
            if let Some(task) = autumn_harvest::queue::claim_task(
                &mut conn,
                &["default".to_string()],
                worker,
                "",
                None,
                &[],
                &[],
            )
            .await
            .expect("claim in sweep")
            {
                let _ = chaos_drive_one_workflow_task(
                    &url,
                    Arc::clone(&registry),
                    task,
                    worker.to_string(),
                )
                .await;
            }
        }
        let fired = guard.actions_fired();
        let diag = guard.diagnostics();
        drop(guard);

        // Anti-vacuity (P2): `ChaosPlan::seeded` is a pure function of the seed,
        // so `fired` is deterministic per seed — this can never flake. A seed
        // that drives zero honored faults would let the convergence invariant
        // below pass for a healthy, un-faulted run ("passes for the wrong
        // reason"). Fail loudly instead, naming the seed for replay.
        assert!(
            fired >= 1,
            "seed {seed}: injected zero honored faults against the workload — the \
             convergence invariant would pass vacuously; {diag}"
        );

        // Stronger, direct anti-vacuity for an orphan-stranding seed: a
        // pre-commit KILL must have left a `RUNNING` row owned by a
        // never-registered (dead) worker *before* the recovery loop runs. This
        // proves the recovery loop below has real work — not just that some
        // (possibly convergence-benign) directive fired — so the final
        // `stranded == 0` post-recovery assert genuinely exercises reclaim.
        if expects_orphan {
            let mut conn = connect(&url).await;
            let stranded_pre = stranded_running_with_dead_worker(&mut conn).await;
            assert!(
                stranded_pre >= 1,
                "seed {seed}: an orphan-stranding seed must leave >= 1 task RUNNING with a dead \
                 worker before recovery, but found {stranded_pre} — the recovery loop would have \
                 nothing to reclaim and convergence would pass vacuously; {diag}"
            );
        }

        // Recovery loop (chaos disarmed): reclaim crash orphans and re-drive any
        // remaining claimable tasks until quiescent.
        for _round in 0..(WORKLOAD + 4) {
            let mut conn = connect(&url).await;
            autumn_harvest::poison_pill::reclaim_orphaned_tasks(
                &mut conn,
                3,
                0,
                None,
                &NoOpMetrics,
                &autumn_harvest::payload_codec::PayloadCodecs::default(),
            )
            .await
            .expect("reclaim in sweep");
            let claimed = autumn_harvest::queue::claim_task(
                &mut conn,
                &["default".to_string()],
                "sweep-recover",
                "",
                None,
                &[],
                &[],
            )
            .await
            .expect("claim in recovery");
            let Some(task) = claimed else { break };
            let _ = chaos_drive_one_workflow_task(
                &url,
                Arc::clone(&registry),
                task,
                "sweep-recover".to_string(),
            )
            .await;
        }

        // Convergence invariant.
        assert_converged(&url, seed, &execs, &diag).await;
    }
}

/// Assert the post-recovery convergence invariant for one sweep seed: every
/// workflow terminal (`COMPLETED`), no task stranded `RUNNING` with a dead
/// worker, and no `ExternalSignalRequested` without an eventual terminal.
async fn assert_converged(url: &str, seed: u64, execs: &[ExecutionId], diag: &str) {
    let mut conn = connect(url).await;
    for exec_id in execs {
        let state = exec_state(&mut conn, *exec_id).await;
        assert_eq!(
            state, "COMPLETED",
            "seed {seed}: workflow {exec_id:?} must converge to terminal; got {state}; {diag}"
        );
    }
    let stranded = stranded_running_with_dead_worker(&mut conn).await;
    assert_eq!(
        stranded, 0,
        "seed {seed}: no task may be stranded RUNNING with a dead worker; {diag}"
    );
    let dangling = dangling_external_requests(&mut conn).await;
    assert_eq!(
        dangling, 0,
        "seed {seed}: no ExternalSignalRequested without a terminal; {diag}"
    );
}

/// Count `RUNNING` tasks whose `worker_id` has no live `harvest_workers`
/// heartbeat — the stranded-orphan invariant probe. Same *shape* as the
/// poison-pill reclaimer's `NOT EXISTS`-a-live-heartbeat liveness test
/// (`orphaned_running_tasks_query`), but with a fixed 30 s window here rather
/// than the reclaimer's configurable stale threshold. Sweep workers are never
/// registered in `harvest_workers`, so any non-null `worker_id` on a `RUNNING`
/// row counts as stranded regardless of the exact window.
async fn stranded_running_with_dead_worker(conn: &mut AsyncPgConnection) -> i64 {
    diesel::sql_query(
        "SELECT COUNT(*)::bigint AS n FROM harvest_task_queue t \
         WHERE t.state = 'RUNNING' AND t.worker_id IS NOT NULL \
           AND NOT EXISTS ( \
             SELECT 1 FROM harvest_workers w \
             WHERE w.worker_id = t.worker_id \
               AND w.last_heartbeat_at > NOW() - INTERVAL '30 seconds' \
           )",
    )
    .get_result::<CountRow>(conn)
    .await
    .expect("count stranded")
    .n
}
