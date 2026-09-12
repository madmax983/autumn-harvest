//! Admission-ordering regression — issue #1228, Finding 1.
//!
//! `enforce_quota_admission` (issue #946) and the latest-wins supersede pass
//! (issue #811, `on_conflict = "cancel_running"`) both run inside the same
//! fresh-insert admission. Before this fix, the quota check ran FIRST. It
//! counted the incumbent run supersede was about to cancel. So a tight
//! quota cap silently defeated `cancel_running`. The newer request was
//! rejected with `QuotaExceeded` instead of replacing the incumbent.
//!
//! These tests drive the real admission entry point. Both a
//! `max_active_executions` cap and a `cancel_running` policy are declared
//! on the SAME key. They assert the newer request always wins.

#![cfg(feature = "db")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::missing_panics_doc)]

use std::collections::HashMap;
use std::sync::LazyLock;

use autumn_harvest::completion_trigger::{GLOBAL_WORKFLOW_METADATA, WorkflowMetadata};
use autumn_harvest::concurrency::ConcurrencyOnConflict;
use autumn_harvest::error::HarvestError;
use autumn_harvest::execution::{StartWorkflowParams, start_or_load_workflow_execution};
use autumn_harvest::quota::QuotaPolicy;
use autumn_harvest::types::{
    ExecutionId, StartSource, WorkflowIdConflictPolicy, WorkflowIdReusePolicy,
};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use uuid::Uuid;

use crate::integration_e2e::setup_test_database_url_or_env;

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url)
        .await
        .expect("connect to test database")
}

fn leaked(prefix: &str) -> &'static str {
    Box::leak(format!("{prefix}_{}", Uuid::new_v4().simple()).into_boxed_str())
}

/// Serializes this file's `GLOBAL_WORKFLOW_METADATA` installs against each
/// other, mirroring the identical convention in `quota_enforcement_tests.rs`
/// and `concurrency_supersede_tests.rs`.
static TEST_SERIAL: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

struct MetadataGuard {
    previous: Option<HashMap<String, WorkflowMetadata>>,
    _permit: tokio::sync::MutexGuard<'static, ()>,
}

impl MetadataGuard {
    async fn install_one(workflow_name: &str, quota: QuotaPolicy) -> Self {
        let permit = TEST_SERIAL.lock().await;
        let mut map = HashMap::new();
        map.insert(
            workflow_name.to_string(),
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
            },
        );
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
}

impl Drop for MetadataGuard {
    fn drop(&mut self) {
        if let Ok(mut lock) = GLOBAL_WORKFLOW_METADATA.write() {
            *lock = self.previous.take();
        }
    }
}

/// A [`StartWorkflowParams`] declaring BOTH a `concurrency_key` (for the
/// `cancel_running` supersede pass) and a `tenant_id` input field. The
/// installed [`QuotaPolicy`] resolves the SAME string from that field —
/// the "natural pairing" the issue describes. One key governs both
/// concerns.
fn params<'a>(
    workflow_name: &'a str,
    workflow_id: &'a str,
    exec_id: ExecutionId,
    key: &'a str,
    on_conflict: ConcurrencyOnConflict,
) -> StartWorkflowParams<'a> {
    StartWorkflowParams {
        workflow_name,
        workflow_id,
        exec_id,
        input: serde_json::json!({ "tenant_id": key }),
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
        concurrency_key: Some(key.to_string()),
        concurrency_limit: Some(1),
        concurrency_on_conflict: on_conflict,
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
        start_source: StartSource::Api,
        start_source_ref: None,
        started_by: None,
    }
}

/// Same as [`params`], but lets the concurrency key and the quota-resolving
/// `tenant_id` diverge. Issue #1228 covers exactly this mismatched shape:
/// two tenants can share one `concurrency_key` while each keeps its own
/// `tenant_id`-derived quota key.
fn params_with_distinct_keys<'a>(
    workflow_name: &'a str,
    workflow_id: &'a str,
    exec_id: ExecutionId,
    tenant_id: &'a str,
    concurrency_key: &'a str,
    on_conflict: ConcurrencyOnConflict,
    concurrency_limit: u32,
) -> StartWorkflowParams<'a> {
    let mut request = params(
        workflow_name,
        workflow_id,
        exec_id,
        concurrency_key,
        on_conflict,
    );
    request.input = serde_json::json!({ "tenant_id": tenant_id });
    request.concurrency_limit = Some(concurrency_limit);
    request
}

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

async fn active_count(conn: &mut AsyncPgConnection, workflow_name: &str, key: &str) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let row: Count = diesel::sql_query(
        "SELECT COUNT(*)::BIGINT AS n FROM harvest_workflow_executions \
         WHERE workflow_name = $1 AND quota_key = $2 AND state IN ('RUNNING', 'PAUSED')",
    )
    .bind::<diesel::sql_types::Text, _>(workflow_name)
    .bind::<diesel::sql_types::Text, _>(key)
    .get_result(conn)
    .await
    .expect("count active rows");
    row.n
}

/// The money test (issue #1228, Finding 1): a `max_active_executions = 1`
/// quota cap and a `cancel_running` concurrency policy on the SAME key. The
/// second admission must CANCEL the incumbent and succeed, never reject with
/// `QuotaExceeded`.
#[tokio::test]
async fn cancel_running_supersede_wins_over_a_tight_quota_cap_on_the_same_key() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf = leaked("supersede_vs_quota");
    let key = "acme";
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1);
    let _guard = MetadataGuard::install_one(wf, policy).await;

    let first_id = ExecutionId::new();
    let first = start_or_load_workflow_execution(
        &mut conn,
        params(
            wf,
            &format!("wid-{}", Uuid::new_v4().simple()),
            first_id,
            key,
            ConcurrencyOnConflict::CancelRunning,
        ),
        None,
    )
    .await
    .expect("first admission must succeed");
    assert!(first.created, "first admission must create a fresh run");

    let second_id = ExecutionId::new();
    let second = start_or_load_workflow_execution(
        &mut conn,
        params(
            wf,
            &format!("wid-{}", Uuid::new_v4().simple()),
            second_id,
            key,
            ConcurrencyOnConflict::CancelRunning,
        ),
        None,
    )
    .await
    .unwrap_or_else(|e| {
        panic!(
            "the second admission must supersede the incumbent, not be \
             rejected on quota -- got {e:?}"
        )
    });
    assert!(second.created, "second admission must create a fresh run");

    assert_eq!(
        row_state(&mut conn, ExecutionId::from_uuid(first.exec_id.as_uuid())).await,
        "CANCELLED",
        "the incumbent must be cancelled by latest-wins supersede"
    );
    assert_eq!(
        row_state(&mut conn, ExecutionId::from_uuid(second.exec_id.as_uuid())).await,
        "RUNNING",
        "the newer run must be admitted and left RUNNING"
    );
    assert_eq!(
        active_count(&mut conn, wf, key).await,
        1,
        "the key must settle at exactly one active run, never zero (both \
         rejected) or two (cap bypassed)"
    );
}

/// Same shape as the money test, but repeated N times over one key. Every
/// admission after the first must supersede its predecessor and land in
/// exactly one active run. This proves the fix holds under a
/// `cancel_running` chain, not just a single hand-off.
#[tokio::test]
async fn cancel_running_supersede_chain_never_trips_the_quota_cap() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf = leaked("supersede_vs_quota_chain");
    let key = "acme";
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1);
    let _guard = MetadataGuard::install_one(wf, policy).await;

    let mut last_id = None;
    for i in 0..10 {
        let exec_id = ExecutionId::new();
        let outcome = start_or_load_workflow_execution(
            &mut conn,
            params(
                wf,
                &format!("wid-{}", Uuid::new_v4().simple()),
                exec_id,
                key,
                ConcurrencyOnConflict::CancelRunning,
            ),
            None,
        )
        .await
        .unwrap_or_else(|e| panic!("admission {i} must supersede, not reject on quota: {e:?}"));
        last_id = Some(ExecutionId::from_uuid(outcome.exec_id.as_uuid()));
        assert_eq!(
            active_count(&mut conn, wf, key).await,
            1,
            "admission {i} must leave exactly one active run for the key"
        );
    }
    assert_eq!(
        row_state(&mut conn, last_id.expect("at least one admission ran")).await,
        "RUNNING"
    );
}

/// The dry-run credit must generalize past a single incumbent. With
/// `concurrency_limit = 2`, a THIRD run on a key already holding two sheds
/// exactly the oldest one. That is down to the limit, not down to zero. A
/// `max_active_executions = 2` quota cap must see that one-run credit. It
/// must not see the raw pre-shed count of three.
#[tokio::test]
async fn cancel_running_supersede_credit_generalizes_past_a_single_incumbent() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf = leaked("supersede_vs_quota_limit2");
    let key = "acme";
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(2);
    let _guard = MetadataGuard::install_one(wf, policy).await;

    let wid_a = format!("a-{}", Uuid::new_v4().simple());
    let mut p_a = params(
        wf,
        &wid_a,
        ExecutionId::new(),
        key,
        ConcurrencyOnConflict::CancelRunning,
    );
    p_a.concurrency_limit = Some(2);
    let first = start_or_load_workflow_execution(&mut conn, p_a, None)
        .await
        .expect("first admission must succeed");

    let wid_b = format!("b-{}", Uuid::new_v4().simple());
    let mut p_b = params(
        wf,
        &wid_b,
        ExecutionId::new(),
        key,
        ConcurrencyOnConflict::CancelRunning,
    );
    p_b.concurrency_limit = Some(2);
    let second = start_or_load_workflow_execution(&mut conn, p_b, None)
        .await
        .expect("second admission must succeed, limit is 2");
    assert_eq!(active_count(&mut conn, wf, key).await, 2);

    let wid_c = format!("c-{}", Uuid::new_v4().simple());
    let mut p_c = params(
        wf,
        &wid_c,
        ExecutionId::new(),
        key,
        ConcurrencyOnConflict::CancelRunning,
    );
    p_c.concurrency_limit = Some(2);
    let third = start_or_load_workflow_execution(&mut conn, p_c, None)
        .await
        .unwrap_or_else(|e| {
            panic!(
                "the third admission must shed the oldest incumbent down to \
                 the limit of 2, not be rejected on quota -- got {e:?}"
            )
        });

    assert_eq!(
        row_state(&mut conn, ExecutionId::from_uuid(first.exec_id.as_uuid())).await,
        "CANCELLED",
        "the OLDEST incumbent must be the one shed, not the newer second run"
    );
    assert_eq!(
        row_state(&mut conn, ExecutionId::from_uuid(second.exec_id.as_uuid())).await,
        "RUNNING",
        "the second run is younger than the shed target and must survive"
    );
    assert_eq!(
        row_state(&mut conn, ExecutionId::from_uuid(third.exec_id.as_uuid())).await,
        "RUNNING"
    );
    assert_eq!(
        active_count(&mut conn, wf, key).await,
        2,
        "the key must settle at exactly the limit of 2, not 1 (over-shed) or \
         3 (quota wrongly rejected the shed credit)"
    );
}

/// `Defer` (the default, non-`cancel_running` policy) must be byte-for-byte
/// unaffected by this reordering. With no supersede to run, the quota cap
/// still rejects the second admission exactly as before.
#[tokio::test]
async fn defer_policy_still_enforces_the_quota_cap_unchanged() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf = leaked("supersede_vs_quota_defer");
    let key = "acme";
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1);
    let _guard = MetadataGuard::install_one(wf, policy).await;

    let first_id = ExecutionId::new();
    start_or_load_workflow_execution(
        &mut conn,
        params(
            wf,
            &format!("wid-{}", Uuid::new_v4().simple()),
            first_id,
            key,
            ConcurrencyOnConflict::Defer,
        ),
        None,
    )
    .await
    .expect("first admission must succeed");

    let second_id = ExecutionId::new();
    let err = start_or_load_workflow_execution(
        &mut conn,
        params(
            wf,
            &format!("wid-{}", Uuid::new_v4().simple()),
            second_id,
            key,
            ConcurrencyOnConflict::Defer,
        ),
        None,
    )
    .await
    .expect_err("Defer declares no supersede, so the cap must still reject the 2nd admission");
    assert!(
        matches!(err, HarvestError::QuotaExceeded { .. }),
        "expected QuotaExceeded, got {err:?}"
    );
    assert_eq!(
        active_count(&mut conn, wf, key).await,
        1,
        "the rejected attempt must leave the incumbent untouched"
    );
}

/// Credit must not cross tenants (issue #1228 review, P1). Two tenants can
/// share one `concurrency_key` while each keeps its own `tenant_id`-derived
/// quota key. A `cancel_running` supersede shedding the OTHER tenant's
/// incumbent under that shared key must never credit THIS tenant's own
/// quota check.
#[tokio::test]
async fn supersede_credit_does_not_cross_a_mismatched_quota_key() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf = leaked("supersede_vs_quota_mismatch");
    let shared_concurrency_key = leaked("shared_lock");
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1);
    let _guard = MetadataGuard::install_one(wf, policy).await;

    // Tenant "acme" holds the shared concurrency key.
    let acme_id = ExecutionId::new();
    start_or_load_workflow_execution(
        &mut conn,
        params_with_distinct_keys(
            wf,
            &format!("acme-{}", Uuid::new_v4().simple()),
            acme_id,
            "acme",
            shared_concurrency_key,
            ConcurrencyOnConflict::CancelRunning,
            1,
        ),
        None,
    )
    .await
    .expect("acme's first admission must succeed");

    // Tenant "beta" already sits at its own quota cap, on an unrelated
    // concurrency key nothing else will ever touch.
    let beta_solo_key = leaked("beta_solo");
    let beta_first_id = ExecutionId::new();
    start_or_load_workflow_execution(
        &mut conn,
        params_with_distinct_keys(
            wf,
            &format!("beta-solo-{}", Uuid::new_v4().simple()),
            beta_first_id,
            "beta",
            beta_solo_key,
            ConcurrencyOnConflict::Defer,
            1,
        ),
        None,
    )
    .await
    .expect("beta's first admission must succeed");

    // Beta now admits a second run under the SAME shared concurrency key
    // acme holds. Supersede would shed acme's incumbent to honor
    // `concurrency_limit = 1` on that shared key. But acme's row carries
    // quota_key "acme", not "beta". Beta's own quota already sits at its
    // cap of 1 (the solo row above), so this admission must still be
    // rejected.
    let beta_second_id = ExecutionId::new();
    let err = start_or_load_workflow_execution(
        &mut conn,
        params_with_distinct_keys(
            wf,
            &format!("beta-shared-{}", Uuid::new_v4().simple()),
            beta_second_id,
            "beta",
            shared_concurrency_key,
            ConcurrencyOnConflict::CancelRunning,
            1,
        ),
        None,
    )
    .await
    .expect_err(
        "acme's shed credit must not cross into beta's quota check -- beta \
         is genuinely at its own cap and must be rejected",
    );
    assert!(
        matches!(err, HarvestError::QuotaExceeded { .. }),
        "expected QuotaExceeded, got {err:?}"
    );

    assert_eq!(
        row_state(&mut conn, acme_id).await,
        "RUNNING",
        "acme's incumbent must survive -- the rejected admission rolls back \
         the whole transaction before supersede ever cancels anything"
    );
    assert_eq!(
        active_count(&mut conn, wf, "acme").await,
        1,
        "acme's quota usage is untouched by beta's rejected admission"
    );
    assert_eq!(
        active_count(&mut conn, wf, "beta").await,
        1,
        "beta's quota usage stays at its pre-existing solo row -- the \
         rejected second admission never persisted"
    );
}

async fn begin(conn: &mut AsyncPgConnection) {
    diesel::sql_query("BEGIN")
        .execute(conn)
        .await
        .expect("begin transaction");
}

async fn end(conn: &mut AsyncPgConnection) {
    let _ = diesel::sql_query("ROLLBACK").execute(conn).await;
}

/// `dry_run_supersede_credit`'s scan must NOT take a row lock (issue
/// #1228 review). An earlier fix tried `FOR UPDATE ... SKIP LOCKED` to
/// keep the scanned population stable. That lock cannot tell a row
/// genuinely leaving the population apart from one merely held by
/// [`autumn_harvest::store`]'s own ordinary `FOR UPDATE`, during ANY
/// unrelated decision cycle. `next_event_id_for` takes exactly that lock,
/// without ever changing the workflow's state. `SKIP LOCKED` would have
/// skipped such a row, undercounting the credit. It could have made
/// `enforce_quota_admission` reject an otherwise-healthy `cancel_running`
/// admission with `QuotaExceeded`. `credited_ids` reconciliation (see
/// `dry_run_supersede_credit`'s own doc comment) now covers the staleness
/// a lock was meant to prevent, so no lock is needed here at all.
///
/// Proven deterministically, not by timing: start two runs sharing one
/// concurrency key and one quota key, so a `limit = 2` credits exactly the
/// older one. Hold that older run's row locked from a SEPARATE connection,
/// simulating an unrelated ordinary decision cycle, for the whole scan.
/// The scan must still credit it -- neither blocking on the lock nor
/// skipping the row it guards.
#[tokio::test]
async fn dry_run_credit_counts_a_row_locked_for_unrelated_reasons() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut setup_conn = connect(&url).await;

    let wf = leaked("supersede_credit_unrelated_lock");
    let shared_key = leaked("shared_key");
    // Registers `tenant_id` as the quota-resolving expression. That makes
    // the two rows below persist `quota_key = shared_key` on insert --
    // `params()` sets `input.tenant_id` to the same `shared_key` passed as
    // the concurrency key. The cap value is irrelevant here: this test
    // calls `dry_run_supersede_credit` directly, never the real admission
    // check.
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1000);
    let _guard = MetadataGuard::install_one(wf, policy).await;

    let x_id = ExecutionId::new();
    start_or_load_workflow_execution(
        &mut setup_conn,
        params(
            wf,
            &format!("x-{}", Uuid::new_v4().simple()),
            x_id,
            shared_key,
            ConcurrencyOnConflict::Defer,
        ),
        None,
    )
    .await
    .expect("x must start");

    let y_id = ExecutionId::new();
    start_or_load_workflow_execution(
        &mut setup_conn,
        params(
            wf,
            &format!("y-{}", Uuid::new_v4().simple()),
            y_id,
            shared_key,
            ConcurrencyOnConflict::Defer,
        ),
        None,
    )
    .await
    .expect("y must start");

    // Lock x -- the OLDER run, and therefore the one the credit below
    // sheds -- from a separate connection. Hold the transaction open for
    // the whole scan below. This mirrors `next_event_id_for`'s ordinary
    // `FOR UPDATE` during an unrelated decision cycle: x stays `RUNNING`,
    // nothing about it changes, only its row lock is held.
    let mut lock_conn = connect(&url).await;
    begin(&mut lock_conn).await;
    diesel::sql_query("SELECT id FROM harvest_workflow_executions WHERE id = $1 FOR UPDATE")
        .bind::<diesel::sql_types::Uuid, _>(x_id.as_uuid())
        .execute(&mut lock_conn)
        .await
        .expect("lock x from the separate connection");

    let mut scan_conn = connect(&url).await;
    let credit = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        autumn_harvest::concurrency::dry_run_supersede_credit(
            &mut scan_conn,
            wf,
            shared_key,
            // limit = 2, not 1. There are 2 pre-existing candidates (x, y)
            // plus the hypothetical new admission. `supersede_count` sheds
            // down to the LIMIT, not to "limit minus the population".
            // A limit of 1 here would shed BOTH x and y, not just the
            // older one. limit = 2 sheds exactly 1 (the oldest, x). That is
            // the scenario this test wants: one locked incumbent, one
            // credit.
            2,
            ExecutionId::new(), // excludes neither x nor y
            shared_key,         // params() resolves quota_key from tenant_id == shared_key
        ),
    )
    .await
    .expect("the scan must not block on x's unrelated lock")
    .expect("dry run must scan the shared-key population");

    assert_eq!(
        credit.credited_ids.len(),
        1,
        "the locked incumbent (x, the older run) must still be credited, \
         not silently omitted because another connection holds its row lock"
    );
    assert_eq!(
        credit.credited_ids,
        vec![x_id.as_uuid()],
        "the credited id must be x specifically -- the oldest of the two, \
         and the one held under the unrelated lock"
    );

    end(&mut lock_conn).await;
}
