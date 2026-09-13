//! Cross-shard child workflows (issue #956).
//!
//! Children are pinned to the parent's shard by default, and that default is
//! permanent. When a spawn opts in to [`ChildPlacement::Distributed`](crate::shard::ChildPlacement::Distributed) (or an
//! explicit pin) and the resolved shard is not the parent's, the child cannot be
//! created inside the parent's decision transaction — per-execution ACID is
//! shard-local by design and never spans two databases.
//!
//! # The one-row lifecycle
//!
//! Instead, the spawn writes **one row** into `harvest_cross_shard_children` on
//! the parent's shard, in the *same transaction* as the parent's
//! `ChildWorkflowStarted` / `ChildWorkflowSpawnedDetached` event. That row is
//! not a message: it is the cross-shard child's lifecycle record on the parent's
//! side, and all four cross-shard edges are transitions of it.
//!
//! | Edge | Transition | Dedupe key |
//! |---|---|---|
//! | Child start | `PENDING_START` → `STARTED` | the child's `ExecutionId` is the PK on the target shard |
//! | Cancel | `cancel_requested` → cleared | `cancel_workflow_execution` is idempotent on a terminal target |
//! | Terminal notify | row deleted | the append + delete commit together on the parent's shard |
//! | Close cascade | row deleted | the cascade only acts on a `RUNNING`/`PAUSED` child |
//!
//! # Why the terminal notify is a *pull*
//!
//! The obvious design pushes a notify from the child's shard when it goes
//! terminal. That re-introduces the exact crash window AC3 rules out: a worker
//! that dies between the child's terminal commit and the parent's notify loses
//! the wake. Here the relay instead *reads* the child's state from the target
//! shard and appends the parent's terminal event and deletes the row in one
//! transaction on the parent's shard. Nothing is ever in flight, so there is
//! nothing to lose: a crash at any instant leaves the row exactly where it was,
//! and the next sweep re-observes the same durable fact.
//!
//! # Consistency contract
//!
//! - The parent's decision transaction is shard-local. Always.
//! - Cross-shard effects are **at-least-once with dedupe** (the table above).
//! - A cross-shard child's start and terminal wake are each one scanner tick
//!   away rather than one transaction away. That latency is the price of the
//!   placement, and it is the same price `enforce_external_signals_outbox` /
//!   `enforce_external_cancels_outbox` (issue #492) already pay.
//! - Placement never falls back silently: an unreachable target shard fails the
//!   spawn with the typed, retryable [`HarvestError::ShardUnavailable`].

use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};

use crate::error::{HarvestError, HarvestResult};
use crate::event::WorkflowEvent;
use crate::models::{CrossShardChildRow, NewCrossShardChildRow, NewWorkflowExecution};
use crate::queue::TaskType;
use crate::schema::{harvest_cross_shard_children, harvest_events, harvest_workflow_executions};
use crate::shard::{
    CrossShardChildAction, CrossShardChildObservation, CrossShardChildStatus, ShardedDbPool,
    next_cross_shard_child_action,
};
use crate::types::{ExecutionId, ParentClosePolicy, ShardId};
use crate::{queue, store};

/// How many **actionable** outbox rows one sweep handles per shard.
///
/// Actionable means the row's own columns say work is owed: a child that has
/// not been created yet, or a pending cancel. Bounds a single tick so the relay
/// can never monopolise a scanner thread under a 10k-child fan-out.
const RELAY_BATCH: i64 = 200;

/// How many **already-started** rows one sweep polls for their child's terminal.
///
/// Deliberately larger than [`RELAY_BATCH`]: these rows are usually answered by
/// one batched `id = ANY(...)` read per target shard that returns only the
/// children that actually finished, so the cost is a wide read and a narrow
/// result rather than per-row work.
const POLL_BATCH: i64 = 1_000;

/// Per-row retry backoff, as a SQL due-predicate.
///
/// A row that keeps failing is re-tried after `min(attempts, 6) * 5s`, so a
/// permanently-broken row (an unreachable shard, a poison spec, an unparseable
/// stored policy) backs off to one attempt every 30s instead of being re-driven
/// at full poll cadence — and, more importantly, stops consuming a slot in every
/// single sweep and starving newer rows behind it.
const DUE_PREDICATE: &str = "(last_attempt_at IS NULL OR last_attempt_at < NOW() - \
     (LEAST(attempts, 6) * INTERVAL '5 seconds'))";

/// Recorded as the `WorkflowCancelled` reason for a child born cancelled
/// (issue #1263 item 13). See [`start_child_on_target`]'s cancellation arm.
const CROSS_SHARD_BORN_CANCELLED_REASON: &str =
    "parent requested cancellation before the relay created this child";

/// Everything the relay needs to create the child on the target shard, with
/// every default **already resolved** at spawn time.
///
/// Resolution happens on the spawning worker, which has the handler registry, at
/// the same moment and through the same `resolve_child_workflow_defaults` call
/// the same-shard path uses. The relay never re-derives a default, so a
/// cross-shard child cannot silently differ from the same-shard twin it would
/// otherwise have been.
///
/// Serialized into the row's `child_spec` JSONB. Every field is `Option` or has
/// a `#[serde(default)]` so an older row stays readable across an upgrade.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CrossShardChildSpec {
    /// The child's input payload.
    pub input: serde_json::Value,
    /// Queue the child's workflow task is enqueued on (inherited from the parent).
    pub queue_name: String,
    /// Build id the child's task requires, inherited from the parent.
    #[serde(default)]
    pub assigned_build_id: Option<String>,
    /// Ambient context headers inherited from the parent (issue #481).
    #[serde(default)]
    pub context_headers: Option<serde_json::Value>,
    #[serde(default)]
    pub owner: Option<String>,
    #[serde(default)]
    pub runbook_url: Option<String>,
    #[serde(default)]
    pub severity: Option<String>,
    #[serde(default)]
    pub sla_secs: Option<i64>,
    #[serde(default)]
    pub execution_timeout_secs: Option<i64>,
    /// The chain-execution-timeout DURATION, never an absolute deadline.
    ///
    /// A child is its own logical chain origin (issue #617). Unlike a
    /// continue-as-new successor, it never inherits an existing chain budget.
    /// So, exactly like [`Self::execution_timeout_secs`] and [`Self::sla_secs`],
    /// only the duration travels here. The relay turns it into an absolute
    /// `chain_deadline_at` at the moment it actually creates the child.
    ///
    /// This field used to carry the resolved absolute `chain_deadline_at`
    /// instead (issue #1263 item 7). That anchored the chain deadline at
    /// the PARENT's decision instant, not the child's own creation. A
    /// relay running late — an unreachable target shard, a backlog, a
    /// worker restart — could then hand the child a deadline already past.
    ///
    /// The per-run deadlines were fixed the same way earlier in issue #956.
    /// This field was missed then. A chain cap is anchored differently for
    /// a CONTINUE-AS-NEW successor, which does inherit its predecessor's
    /// absolute deadline. A child is not a successor.
    #[serde(default)]
    pub chain_execution_timeout_secs: Option<i64>,
    #[serde(default)]
    pub retry_policy: Option<serde_json::Value>,
    /// The child's OWN resolved quota key (issue #946), never the parent's.
    #[serde(default)]
    pub quota_key: Option<String>,
    /// The child's own declared quota **caps**, enforced on the target shard at
    /// creation time exactly as the same-shard path enforces them inline.
    ///
    /// Only the caps travel, not the whole [`crate::quota::QuotaPolicy`: its
    /// `key_expr` is a `&'static str` that cannot round-trip through JSON, and
    /// it would be dead weight anyway — the key it names was already resolved at
    /// spawn time into [`Self::quota_key`], and `enforce_quota_admission`
    /// consumes the resolved key, never the expression.
    #[serde(default)]
    pub quota: Option<QuotaCaps>,
    /// Pre-resolved concurrency group key for the child's task row (issue #247).
    #[serde(default)]
    pub concurrency_key: Option<String>,
    #[serde(default)]
    pub max_concurrent: Option<u32>,
    /// The `harvest.child_workflow.start` producer context captured at spawn.
    ///
    /// Carried on the row because the relay creates the child later, on another
    /// connection, long after the span that produced it has gone. Without it a
    /// remotely placed child begins a disconnected trace — breaking
    /// parent-to-child correlation for precisely the distributed fan-outs this
    /// feature exists to enable.
    #[serde(default)]
    pub trace_context: Option<crate::telemetry::TraceContextCarrier>,
}

/// The cap half of a [`crate::quota::QuotaPolicy`], in a form that survives a
/// JSON round trip (issue #956).
///
/// `QuotaPolicy::key_expr` is a `&'static str` pointing at registry-owned
/// storage, so the policy itself cannot be persisted. The expression is not
/// needed on the relay path regardless: it was already resolved against the
/// child's input at spawn time.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct QuotaCaps {
    /// See [`crate::quota::QuotaPolicy::max_active_executions`].
    #[serde(default)]
    pub max_active_executions: Option<u32>,
    /// See [`crate::quota::QuotaPolicy::max_history_bytes`].
    #[serde(default)]
    pub max_history_bytes: Option<u64>,
    /// See [`crate::quota::QuotaPolicy::max_dead_letters`].
    #[serde(default)]
    pub max_dead_letters: Option<u32>,
}

impl QuotaCaps {
    /// Capture the caps of a resolved policy.
    #[must_use]
    pub const fn from_policy(policy: &crate::quota::QuotaPolicy) -> Self {
        Self {
            max_active_executions: policy.max_active_executions,
            max_history_bytes: policy.max_history_bytes,
            max_dead_letters: policy.max_dead_letters,
        }
    }

    /// Rebuild a policy for `enforce_quota_admission`.
    ///
    /// `key_expr` is deliberately empty: the admission call takes the already
    /// resolved key as a separate argument and never re-resolves the
    /// expression, so there is nothing for it to be wrong about here.
    #[must_use]
    pub const fn to_policy(self) -> crate::quota::QuotaPolicy {
        crate::quota::QuotaPolicy {
            key_expr: "",
            max_active_executions: self.max_active_executions,
            max_history_bytes: self.max_history_bytes,
            max_dead_letters: self.max_dead_letters,
        }
    }
}

/// Refuse a cross-shard spawn whose target shard this process cannot reach
/// (issue #956 AC8).
///
/// Called at **spawn time**, inside the parent's decision cycle, before the
/// outbox row is written. Failing here rolls the parent's decision transaction
/// back with nothing recorded, so the spawn is retried later rather than
/// silently landing the child on the parent's shard — a fallback would break the
/// placement contract without trace, which is precisely the failure mode this
/// check exists to prevent.
///
/// Fails **closed** on a `None` pool. The router and the pool map are two
/// independent globals with two independent installers, so "a multi-shard router
/// with no `ShardedDbPool`" is a reachable misconfiguration (an API-only runtime,
/// an embedder, a half-wired test harness) — and in that state the relay would
/// return `Ok(0)` forever while the row sat there and the parent parked
/// indefinitely. This function is only ever called for a target that already
/// resolved *away* from the parent's shard, so there is no legitimate no-pool
/// case to admit.
///
/// # Why the writability check lives here and not in the resolver
///
/// A shard that is readable but **drained** out of `writable_shards` must not
/// accept a new child. That check is deliberately made *here*, at the persist
/// boundary, rather than in
/// [`resolve_child_placement`](crate::shard::resolve_child_placement), which
/// runs inside the workflow handler. The handler ABI erases the error type — a
/// workflow's `?` turns any `HarvestError` into a `String`, which the executor
/// maps to a terminal `WorkflowOutcome::Failed` — so a drain rejected there
/// would *permanently* fail every workflow that spawned a placed child during a
/// maintenance window. Rejected here, it is a typed `ShardUnavailable` that the
/// spawn paths requeue with a bounded backoff, which is the documented
/// behaviour. Nothing has been recorded at this point, so the resolved child id
/// never reaches history.
///
/// Scope note: this rejects a **cross-shard** target that is drained. It does not
/// (and must not) reject a child resolving to the parent's *own* drained shard —
/// that path never reaches here, and refusing it would deadlock the drain, since
/// a drained shard is one that should let its in-flight work finish and a parent
/// cannot finish while the children it awaits are refused. The fully-drained
/// `Distributed` degenerate case is handled in
/// [`resolve_child_placement`](crate::shard::resolve_child_placement), which
/// traces it rather than failing it.
///
/// A `None` router skips only the writability half — a deployment with a pool map
/// and no router cannot have produced a cross-shard target in the first place.
///
/// # Errors
///
/// [`HarvestError::ShardUnavailable`] — typed and retryable — when there is no
/// pool map, the map has no entry for `target`, or `target` is not currently
/// writable.
pub fn preflight_target_shard(
    sharded_pool: Option<&ShardedDbPool>,
    router: Option<&crate::shard::ShardRouter>,
    target: ShardId,
) -> HarvestResult<()> {
    let unavailable = |reason: &str| HarvestError::ShardUnavailable {
        shard_id: target.as_i32(),
        reason: reason.to_string(),
    };
    let pool = sharded_pool.ok_or_else(|| {
        unavailable(
            "this process has no sharded database pool, so a child cannot be \
             placed off the parent's shard",
        )
    })?;
    if pool.exact_pool_for(target).is_none() {
        return Err(unavailable(
            "no database pool is configured for this shard on this node",
        ));
    }
    if let Some(router) = router
        && !router.is_writable(target)
    {
        return Err(unavailable(
            "shard is not currently accepting new workflows; it is being drained",
        ));
    }
    Ok(())
}

/// Record one cross-shard child on the parent's shard.
///
/// MUST be called inside the parent's own decision transaction, so the row and
/// the parent's `ChildWorkflowStarted` / `ChildWorkflowSpawnedDetached` event
/// commit together or not at all. That atomicity is what makes an orphaned child
/// impossible: no committed row means no child was ever promised.
///
/// Idempotent by `child_exec_id`: a re-park that re-emits the same
/// `StartChildWorkflow` command for an already-recorded child is a no-op, which
/// mirrors the same-shard path's "which children are genuinely new?" filter.
///
/// # Errors
///
/// Propagates database errors.
pub async fn record_cross_shard_child(
    conn: &mut AsyncPgConnection,
    parent_exec_id: ExecutionId,
    child_exec_id: ExecutionId,
    workflow_name: &str,
    parent_close_policy: Option<ParentClosePolicy>,
    spec: &CrossShardChildSpec,
) -> HarvestResult<()> {
    let row = NewCrossShardChildRow {
        child_exec_id: child_exec_id.as_uuid(),
        parent_exec_id: parent_exec_id.as_uuid(),
        target_shard: child_exec_id.shard().as_i32(),
        status: CrossShardChildStatus::PendingStart.as_db_str().to_string(),
        parent_close_policy: parent_close_policy.map(|p| p.to_string()),
        workflow_name: workflow_name.to_string(),
        child_spec: serde_json::to_value(spec).map_err(HarvestError::Serialization)?,
    };
    diesel::insert_into(harvest_cross_shard_children::table)
        .values(&row)
        .on_conflict(harvest_cross_shard_children::child_exec_id)
        .do_nothing()
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(())
}

/// Durably request cancellation of a cross-shard child.
///
/// Called inside whatever parent-side transaction decided to cancel (a race
/// loser, an over-deadline child, an operator cancel), so the request commits
/// with that decision. The relay delivers it to the target shard on its next
/// sweep; delivery is idempotent, so an at-least-once redelivery is harmless.
///
/// Clears `last_attempt_at` so a row that had backed off after an earlier
/// failure is picked up on the very next sweep — a cancel is latency-sensitive
/// in a way a routine poll is not.
///
/// Returns the number of rows flagged — `0` means the child is not (or is no
/// longer) a tracked cross-shard child, which the caller treats as "nothing to
/// do here".
///
/// # Errors
///
/// Propagates database errors.
pub async fn request_cross_shard_cancel(
    conn: &mut AsyncPgConnection,
    child_exec_id: ExecutionId,
) -> HarvestResult<usize> {
    diesel::update(harvest_cross_shard_children::table.find(child_exec_id.as_uuid()))
        .set((
            harvest_cross_shard_children::cancel_requested.eq(true),
            harvest_cross_shard_children::last_attempt_at.eq(None::<DateTime<Utc>>),
            harvest_cross_shard_children::attempts.eq(0),
        ))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)
}

/// Which of `child_ids` the parent has ALREADY recorded a `ChildWorkflowStarted`
/// for, read from the parent's own history (issue #956).
///
/// This is the "has this child already been started?" test for a **cross-shard**
/// child, and it replaces the obvious-looking one that came before it.
///
/// A same-shard child is deduped by its execution row, which lives on the
/// parent's shard and is never removed while the parent runs. A cross-shard
/// child has no row here, so the first implementation used the
/// `harvest_cross_shard_children` lifecycle row instead — written in the same
/// transaction as the first `ChildWorkflowStarted`, so "row exists" did imply
/// "already started".
///
/// The flaw is that the converse does not hold, because the row is **deleted**
/// when the terminal is delivered while a decision cycle spans TWO
/// transactions. History is loaded at T0 (the child is still in progress, so the
/// handler re-issues its `StartChildWorkflow` command); the relay delivers the
/// terminal and deletes the row at T1; the persist transaction at T2 then finds
/// the child in neither the executions table (its row is on another shard) nor
/// the outbox (deleted), calls it new, and appends a SECOND
/// `ChildWorkflowStarted`. Measured against four real shard databases: a
/// 32-child distributed fan-out produced 49 start events, every duplicate a
/// cross-shard child, and the parent then parked forever.
///
/// The parent's own history has none of that fragility: `ChildWorkflowStarted`
/// is append-only, is never deleted or rewritten (the engine's
/// `harvest_events` invariant), and is written in the same transaction as the
/// child's creation. So "the event is there" and "the child was started" are the
/// same fact, for every child, at every point in its lifetime — which is exactly
/// what a de-duplication test needs and what the outbox row could not provide.
///
/// `child_id` is a non-payload field, so it is read straight out of the stored
/// JSON without a codec pass — no key is needed, and none of the deployments a
/// codec would affect behave differently here.
///
/// # Errors
///
/// Returns [`HarvestError::Database`](crate::error::HarvestError::Database) if
/// the history read fails.
pub async fn already_started_child_ids(
    conn: &mut AsyncPgConnection,
    parent_exec_id: ExecutionId,
    child_ids: &[uuid::Uuid],
) -> HarvestResult<Vec<uuid::Uuid>> {
    if child_ids.is_empty() {
        return Ok(Vec::new());
    }
    // Extract `child_id` in SQL rather than loading whole events: a 10k-child
    // fan-out's history carries 10k `input` payloads this test has no use for.
    // Raw SQL because that is how every other JSONB path in the engine is read
    // (`execution.rs`, `timeout.rs`, `backup_verify.rs`).
    let recorded: Vec<StartedChildIdRow> = diesel::sql_query(
        "SELECT e.event_data->'data'->>'child_id' AS child_id \
         FROM harvest_events e \
         WHERE e.workflow_exec_id = $1 \
           AND e.event_type = 'ChildWorkflowStarted'",
    )
    .bind::<diesel::sql_types::Uuid, _>(parent_exec_id.as_uuid())
    .load(conn)
    .await
    .map_err(crate::error::database_error)?;

    let wanted: std::collections::HashSet<uuid::Uuid> = child_ids.iter().copied().collect();
    Ok(recorded
        .into_iter()
        .filter_map(|row| row.child_id)
        .filter_map(|s| uuid::Uuid::parse_str(&s).ok())
        .filter(|id| wanted.contains(id))
        .collect())
}

/// One `child_id` read out of a stored `ChildWorkflowStarted` event.
#[derive(diesel::QueryableByName)]
struct StartedChildIdRow {
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    child_id: Option<String>,
}

/// `(id, state, output, error)` as read from a target shard.
type ChildStateRow = (
    uuid::Uuid,
    String,
    Option<serde_json::Value>,
    Option<String>,
);

/// One in-flight child observed on its target shard.
#[derive(Debug, Clone)]
struct TargetChildState {
    state: String,
    output: Option<serde_json::Value>,
    /// The child's `error` COLUMN, which holds the human message only.
    error: Option<String>,
    /// The typed failure recovered from the child's own `WorkflowFailed` event,
    /// when one was loaded (issue #767 parity, issue #956 Codex rounds 4 and 6).
    ///
    /// The `error` COLUMN stores `decoded.message`, not the envelope, so
    /// re-decoding it yields an *untyped* failure and the parent would silently
    /// lose `error_type` / `details` / `non_retryable` — a different observable
    /// surface than the same-shard path, which forwards the raw envelope through
    /// a function argument and never round-trips it through storage.
    ///
    /// `WorkflowFailed` already stores the DECODED fields rather than the
    /// envelope string, so this holds a `DecodedWorkflowFailure` directly and
    /// needs no second `decode_workflow_failure` pass.
    typed_failure: Option<crate::failure::DecodedWorkflowFailure>,
}

/// One sweep of the cross-shard child relay.
///
/// Runs on the parent's shard (this connection) and reaches out to each target
/// shard through `sharded_pool`. Returns how many rows made observable progress.
///
/// Failure of one row never aborts the sweep: a target shard that is down is
/// logged onto the row (`attempts` / `last_error` / `last_attempt_at`) and
/// retried after a backoff, which mirrors `attempt_signal_delivery`'s "one row's
/// transient failure must not abort the scan of every other row" contract from
/// issue #492.
///
/// # Errors
///
/// Only propagates a failure to read this shard's own work-list; every per-row
/// and per-target-shard failure is absorbed onto the row.
// Long by construction: the sweep is a linear sequence of clearly-named phases
// (resolve the pool, load the batch, stamp it, read both sides, decide and act
// per row). Splitting it would scatter that order across call sites without
// making any phase easier to check.
#[allow(clippy::too_many_lines)]
pub async fn enforce_cross_shard_children(
    conn: &mut AsyncPgConnection,
    sharded_pool: &Option<ShardedDbPool>,
    codecs: &crate::payload_codec::PayloadCodecs,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
) -> HarvestResult<usize> {
    // Every cross-shard checkout in this sweep is bounded. Harvest configures no
    // deadpool `Timeouts`, so a bare `pool.get().await` is an *unbounded* wait,
    // and the relay holds a connection on the parent's shard for the whole sweep
    // while reaching across to others — see `acquire_bounded` for the two-pool
    // wait-for cycle that creates. The relay only ever runs with a
    // `ShardedDbPool` present, so the multi-shard bound always applies; the
    // floor (rather than a poll interval) is used because a bounded pool busy
    // dispatching legitimately takes far longer than one poll to hand a
    // connection over.
    let acquire_bound = Some(crate::worker::MIN_SHARD_ACQUIRE_BOUND);
    let active_pool = sharded_pool.clone().or_else(|| {
        crate::shard::GLOBAL_SHARDED_POOL
            .read()
            .ok()
            .and_then(|guard| guard.clone())
    });
    let Some(pool) = active_pool else {
        // No sharded pool means no second database to relay to. Any row here
        // would be unroutable; leave it for a node that has the pools.
        return Ok(0);
    };

    // Only sweep rows whose target shard this worker actually holds a pool for.
    // A row for a shard this node cannot see is left for a node that can — the
    // same "leave pending for other workers" contract the #492 outbox scanners
    // use.
    //
    // Deliberately NOT filtered by the caller's shard assignments: this row
    // lives on the PARENT's shard, which is already the connection we are
    // handed, and `monitor_shard_scope` narrows each per-shard timeout checker's
    // assignment list to that one shard. Intersecting `target_shard` with it
    // would keep only rows whose target IS the parent's shard — i.e. exactly the
    // rows that are never cross-shard — and the relay would sweep nothing at all
    // in the multi-shard deployments it exists for. The union across the fleet's
    // per-shard checkers still covers every shard's rows exactly once, because
    // each checker only ever sees its own database's table.
    let reachable: Vec<i32> = pool.shard_ids().into_iter().map(ShardId::as_i32).collect();
    if reachable.is_empty() {
        return Ok(0);
    }

    // A missing `harvest_cross_shard_children` is "this deployment has not run
    // #956's migration yet", NOT a failure — and it must not be one, because
    // this relay is sequenced inside `enforce_timeouts_once`. Propagating the
    // error there aborts the WHOLE tick: activity/workflow timeout enforcement,
    // and every scanner ordered after this one (debounce, throttle, event
    // batches). A deployment whose code is ahead of its migrations — the
    // ordinary rolling-upgrade window — would lose its entire timeout subsystem,
    // not merely cross-shard delivery.
    //
    // Detected by re-checking the catalog only AFTER a failed sweep, so the
    // happy path pays nothing and the check never depends on parsing a
    // localised Postgres message.
    let rows = match load_sweep_batch(conn, &reachable).await {
        Ok(rows) => rows,
        Err(e) => {
            if !cross_shard_table_exists(conn).await {
                tracing::warn!(
                    "harvest: harvest_cross_shard_children is absent; skipping the                      cross-shard child relay until this deployment's migrations                      have run. Cross-shard children (#956) cannot be delivered                      until then; every other scanner duty is unaffected."
                );
                return Ok(0);
            }
            return Err(e);
        }
    };
    if rows.is_empty() {
        return Ok(0);
    }

    // Stamp every row this sweep looked at BEFORE acting on it. Two things
    // depend on this: the `last_attempt_at NULLS FIRST` ordering below rotates
    // through a large backlog instead of re-reading the same head every tick
    // (without it a handful of long-running children at the head of
    // `created_at` starve every newer row indefinitely), and a row whose target
    // shard is unreadable this sweep still gets a visible breadcrumb.
    let swept_ids: Vec<uuid::Uuid> = rows.iter().map(|r| r.child_exec_id).collect();
    mark_swept(conn, &swept_ids).await;

    // One batched read per target shard, not one per row: a 10k-child fan-out
    // must not become a 10k-round-trip sweep (the `O(nodes x shards)` shape the
    // children-traversal N+1 fix already called out in this repo).
    let (mut child_states, readable_shards) =
        load_child_states(&pool, &rows, acquire_bound, codecs).await;

    // A `STARTED` row whose child is absent from a shard we READ SUCCESSFULLY is
    // not "still running" — it is gone. The status is only set after the child's
    // insert commits, so on a readable shard absence means the row was collected
    // (retention, erase). Left as `None` it would look identical to "the shard
    // was unreachable this sweep", the state machine would `Wait`, and an awaited
    // parent would park forever on a child that no longer exists.
    //
    // Synthesising a terminal here converts a permanent hang into a typed
    // `ChildWorkflowFailed` the parent can actually observe and handle. This is
    // reachable only when the relay is down for longer than the target shard's
    // whole retention window, which is days — but "the parent hangs forever" is
    // not an acceptable outcome for any window.
    for row in &rows {
        if CrossShardChildStatus::from_db(&row.status) == Some(CrossShardChildStatus::Started)
            && !child_states.contains_key(&row.child_exec_id)
            && readable_shards.contains(&row.target_shard)
        {
            tracing::warn!(
                child_exec_id = %row.child_exec_id,
                parent_exec_id = %row.parent_exec_id,
                target_shard = row.target_shard,
                "cross-shard child no longer exists on its target shard (collected \
                 by retention before the relay could deliver its terminal); \
                 reporting it to the parent as failed rather than parking forever"
            );
            child_states.insert(
                row.child_exec_id,
                TargetChildState {
                    state: "TERMINATED".to_string(),
                    output: None,
                    error: Some(
                        "child workflow execution no longer exists on its shard \
                         (collected before its terminal was delivered)"
                            .to_string(),
                    ),
                    typed_failure: None,
                },
            );
        }
    }
    let parent_terminal = load_parent_terminal_states(conn, &rows).await;

    let mut progressed = 0;
    for row in rows {
        let Some(status) = CrossShardChildStatus::from_db(&row.status) else {
            let reason = format!("unrecognised status {:?}", row.status);
            tracing::error!(
                child_exec_id = %row.child_exec_id,
                status = %row.status,
                "cross-shard child relay: unrecognised status; the row is stuck"
            );
            record_attempt_failure(conn, row.child_exec_id, &reason).await;
            continue;
        };
        let policy = match row
            .parent_close_policy
            .as_deref()
            .map(str::parse::<ParentClosePolicy>)
        {
            None => None,
            Some(Ok(policy)) => Some(policy),
            Some(Err(e)) => {
                tracing::error!(
                    child_exec_id = %row.child_exec_id,
                    error = %e,
                    "cross-shard child relay: unparseable parent_close_policy; \
                     the row is stuck"
                );
                record_attempt_failure(conn, row.child_exec_id, &e).await;
                continue;
            }
        };
        let observed_child = child_states.get(&row.child_exec_id);
        let observation = CrossShardChildObservation {
            status,
            cancel_requested: row.cancel_requested,
            parent_close_policy: policy,
            // `None` when this sweep could not read the parents at all. Only a
            // SUCCESSFUL read that lacks the id means the parent row has
            // genuinely vanished (retention collection, erase) and there is
            // nobody left to wake.
            parent_terminal: parent_terminal
                .as_ref()
                .map(|states| states.get(&row.parent_exec_id).copied().unwrap_or(true)),
            child_state: observed_child.map(|c| c.state.as_str()),
        };

        let action = next_cross_shard_child_action(&observation);
        match apply_action(
            conn,
            &pool,
            &row,
            action,
            &observation,
            observed_child,
            acquire_bound,
            codecs,
            metrics,
        )
        .await
        {
            Ok(true) => progressed += 1,
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(
                    child_exec_id = %row.child_exec_id,
                    parent_exec_id = %row.parent_exec_id,
                    target_shard = row.target_shard,
                    action = ?action,
                    error = %e,
                    "cross-shard child relay: step failed; retrying after a backoff"
                );
                record_attempt_failure(conn, row.child_exec_id, &e.to_string()).await;
            }
        }
    }

    Ok(progressed)
}

/// This sweep's work-list: actionable rows first, then a rotating window of
/// already-started rows.
///
/// Splitting the two is what stops head-of-line starvation. A single
/// `ORDER BY created_at LIMIT N` fills its whole window with rows that are
/// merely *waiting* — an awaited child that is still running is re-read every
/// tick and never deleted until it finishes — so under a 10k-child fan-out the
/// oldest N rows would occupy every slot and rows N+1.. would never be started
/// at all. Actionable rows are selected by their own columns (`PENDING_START` or
/// a pending cancel), so the start backlog always drains; waiting rows are then
/// polled least-recently-swept first, so the poll rotates through the whole
/// backlog rather than re-reading one end of it.
async fn load_sweep_batch(
    conn: &mut AsyncPgConnection,
    reachable: &[i32],
) -> HarvestResult<Vec<CrossShardChildRow>> {
    use diesel::dsl::sql;
    use diesel::sql_types::Bool;

    let mut rows: Vec<CrossShardChildRow> = harvest_cross_shard_children::table
        .filter(harvest_cross_shard_children::target_shard.eq_any(reachable))
        .filter(
            harvest_cross_shard_children::status
                .eq(CrossShardChildStatus::PendingStart.as_db_str())
                .or(harvest_cross_shard_children::cancel_requested.eq(true)),
        )
        .filter(sql::<Bool>(DUE_PREDICATE))
        .order(harvest_cross_shard_children::created_at.asc())
        .limit(RELAY_BATCH)
        .select(CrossShardChildRow::as_select())
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    let started: Vec<CrossShardChildRow> = harvest_cross_shard_children::table
        .filter(harvest_cross_shard_children::target_shard.eq_any(reachable))
        .filter(harvest_cross_shard_children::status.eq(CrossShardChildStatus::Started.as_db_str()))
        .filter(harvest_cross_shard_children::cancel_requested.eq(false))
        .filter(sql::<Bool>(DUE_PREDICATE))
        .order((
            harvest_cross_shard_children::last_attempt_at
                .asc()
                .nulls_first(),
            harvest_cross_shard_children::created_at.asc(),
        ))
        .limit(POLL_BATCH)
        .select(CrossShardChildRow::as_select())
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    rows.extend(started);
    Ok(rows)
}

/// Stamp `last_attempt_at` on every row this sweep examined.
///
/// Best effort: failing to record that we looked is not worth aborting a sweep
/// whose real work has not started yet.
async fn mark_swept(conn: &mut AsyncPgConnection, ids: &[uuid::Uuid]) {
    if ids.is_empty() {
        return;
    }
    let _ = diesel::update(
        harvest_cross_shard_children::table
            .filter(harvest_cross_shard_children::child_exec_id.eq_any(ids)),
    )
    .set(harvest_cross_shard_children::last_attempt_at.eq(Some(Utc::now())))
    .execute(conn)
    .await;
}

/// Batched `child_id -> (state, output, error)` read, one query per distinct
/// target shard.
///
/// The terminal payload is fetched here rather than re-read in
/// `deliver_terminal` so a delivery costs no second round trip to the target
/// shard, and so the state the action was *decided* from is the state it is
/// *delivered* from.
///
/// An unreachable shard contributes no entries rather than failing the sweep, so
/// its rows simply observe `child_state: None` and wait — degrading exactly like
/// the read path (AC7) rather than aborting every healthy shard's work.
async fn load_child_states(
    pool: &ShardedDbPool,
    rows: &[CrossShardChildRow],
    acquire_bound: Option<std::time::Duration>,
    codecs: &crate::payload_codec::PayloadCodecs,
) -> (
    std::collections::HashMap<uuid::Uuid, TargetChildState>,
    std::collections::HashSet<i32>,
) {
    use std::collections::{HashMap, HashSet};
    let mut by_shard: HashMap<i32, Vec<uuid::Uuid>> = HashMap::new();
    for row in rows {
        by_shard
            .entry(row.target_shard)
            .or_default()
            .push(row.child_exec_id);
    }

    let mut states: HashMap<uuid::Uuid, TargetChildState> = HashMap::new();
    // Which shards actually answered. "No row for this child" means something
    // completely different depending on whether we could read the shard at all,
    // so the caller needs both facts, not just the map.
    let mut readable: HashSet<i32> = HashSet::new();
    for (shard, ids) in by_shard {
        let Some(shard_pool) = pool.exact_pool_for(ShardId::new(shard)) else {
            continue;
        };
        let mut target_conn = match acquire_bounded(shard_pool, shard, acquire_bound).await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(
                    target_shard = shard,
                    error = %e,
                    "cross-shard child relay: target shard unreachable this sweep"
                );
                continue;
            }
        };
        let loaded: Result<Vec<ChildStateRow>, _> = harvest_workflow_executions::table
            .filter(harvest_workflow_executions::id.eq_any(&ids))
            .select((
                harvest_workflow_executions::id,
                harvest_workflow_executions::state,
                harvest_workflow_executions::output,
                harvest_workflow_executions::error,
            ))
            .load(&mut *target_conn)
            .await;
        match loaded {
            Ok(pairs) => {
                readable.insert(shard);
                // Recover the typed envelope for the children that actually
                // failed, in ONE extra query for the whole shard. Only terminal
                // non-`COMPLETED` children can have a `WorkflowFailed` event, so
                // a healthy fan-out of completions costs nothing.
                let failed_ids: Vec<uuid::Uuid> = pairs
                    .iter()
                    .filter(|(_, state, _, _)| {
                        state != "COMPLETED" && crate::erase::is_terminal_state(state)
                    })
                    .map(|(id, _, _, _)| *id)
                    .collect();
                let mut typed =
                    load_child_typed_failures(&mut target_conn, &failed_ids, codecs, shard).await;
                for (id, state, output, error) in pairs {
                    states.insert(
                        id,
                        TargetChildState {
                            state,
                            output,
                            error,
                            typed_failure: typed.remove(&id),
                        },
                    );
                }
            }
            Err(e) => tracing::warn!(
                target_shard = shard,
                error = %e,
                "cross-shard child relay: failed to read child states"
            ),
        }
    }
    (states, readable)
}

/// Batched `child_id -> DecodedWorkflowFailure` read of the children's own
/// `WorkflowFailed` events on ONE target shard (issue #767 parity).
///
/// The same-shard spawn path forwards the raw failure envelope to the parent
/// through a function argument, so it never needs to round-trip it through
/// storage. The relay has no such channel — it sees the child only through what
/// the child durably wrote — and the execution row's `error` column holds
/// `decoded.message` alone. Reading it back therefore yields an *untyped*
/// failure, silently dropping `error_type` / `details` / `non_retryable` for
/// exactly the children that failed.
///
/// `WorkflowFailed` stores the decoded fields themselves, so no second
/// `decode_workflow_failure` pass is needed: the event *is* the envelope.
///
/// **Best-effort by design.** Every failure here (unreadable history, an
/// undecodable payload under a codec this node lacks) degrades to an empty
/// entry, and `deliver_terminal` falls back to the `error` column — still a
/// correct failure, merely untyped. Losing the typing is bad; losing the
/// terminal delivery entirely, which propagating the error would do, is worse.
///
/// The last `WorkflowFailed` wins: a run that failed, was retried and failed
/// again carries more than one, and the parent is owed the terminal one.
async fn load_child_typed_failures(
    target_conn: &mut AsyncPgConnection,
    child_ids: &[uuid::Uuid],
    codecs: &crate::payload_codec::PayloadCodecs,
    shard: i32,
) -> std::collections::HashMap<uuid::Uuid, crate::failure::DecodedWorkflowFailure> {
    use std::collections::HashMap;

    let mut out: HashMap<uuid::Uuid, crate::failure::DecodedWorkflowFailure> = HashMap::new();
    if child_ids.is_empty() {
        return out;
    }

    let loaded: Result<Vec<(uuid::Uuid, serde_json::Value)>, _> = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq_any(child_ids))
        .filter(harvest_events::event_type.eq("WorkflowFailed"))
        .order(harvest_events::event_id.asc())
        .select((harvest_events::workflow_exec_id, harvest_events::event_data))
        .load(target_conn)
        .await;

    let rows = match loaded {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!(
                target_shard = shard,
                error = %e,
                "cross-shard child relay: failed to read child WorkflowFailed events; \
                 terminals will be delivered untyped"
            );
            return out;
        }
    };

    for (child_id, event_data) in rows {
        match codecs.decode_event(event_data) {
            Ok(WorkflowEvent::WorkflowFailed {
                error,
                error_type,
                details,
                non_retryable,
            }) => {
                out.insert(
                    child_id,
                    crate::failure::DecodedWorkflowFailure {
                        message: error,
                        error_type,
                        details,
                        non_retryable,
                    },
                );
            }
            // The `event_type` filter makes any other variant impossible; a
            // decode failure means this node lacks the child's codec key.
            Ok(_) => {}
            Err(e) => tracing::warn!(
                target_shard = shard,
                child_exec_id = %child_id,
                error = %e,
                "cross-shard child relay: could not decode a child's WorkflowFailed \
                 event; its terminal will be delivered untyped"
            ),
        }
    }
    out
}

/// Batched `parent_id -> is_terminal` read on this (the parent's) shard.
///
/// Returns `None` when the read itself failed — **not** an empty map. The
/// distinction is a correctness one, not a stylistic one: the call site treats a
/// missing id as "the parent row is gone, so it is closed", which is right for a
/// *successful* read (retention collection, erase) and catastrophically wrong
/// for a failed one. `Retire` deletes the outbox row outright with no second
/// look at the parent, so collapsing a transient read error into "terminal"
/// would permanently lose the terminal wake of every awaited cross-shard child
/// in the batch, and would cascade-cancel detached children whose parents are
/// alive. `None` propagates as `parent_terminal: None`, from which the decision
/// table never decides anything destructive.
///
/// The failure is absorbed rather than propagated because this runs inside
/// `enforce_timeouts_once`'s `?`-chain: a propagated error would skip every
/// scanner duty ordered after the relay (debounce, throttle, event batches, the
/// idempotency sweep) for the whole tick. Start and cancel steps do not consult
/// the parent, so they still make progress during a parent-read outage.
async fn load_parent_terminal_states(
    conn: &mut AsyncPgConnection,
    rows: &[CrossShardChildRow],
) -> Option<std::collections::HashMap<uuid::Uuid, bool>> {
    let ids: Vec<uuid::Uuid> = rows.iter().map(|r| r.parent_exec_id).collect();
    let loaded: Result<Vec<(uuid::Uuid, String)>, _> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::id.eq_any(&ids))
        .select((
            harvest_workflow_executions::id,
            harvest_workflow_executions::state,
        ))
        .load(conn)
        .await;
    match loaded {
        Ok(pairs) => Some(
            pairs
                .into_iter()
                .map(|(id, state)| (id, crate::erase::is_terminal_state(&state)))
                .collect(),
        ),
        Err(e) => {
            tracing::warn!(
                error = %e,
                "cross-shard child relay: failed to read parent states; this \
                 sweep decides nothing that depends on them"
            );
            None
        }
    }
}

/// Execute one decided action. Returns whether the row made observable progress.
#[allow(clippy::too_many_arguments)]
async fn apply_action(
    conn: &mut AsyncPgConnection,
    pool: &ShardedDbPool,
    row: &CrossShardChildRow,
    action: CrossShardChildAction,
    observation: &CrossShardChildObservation<'_>,
    observed_child: Option<&TargetChildState>,
    acquire_bound: Option<std::time::Duration>,
    codecs: &crate::payload_codec::PayloadCodecs,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
) -> HarvestResult<bool> {
    match action {
        CrossShardChildAction::Wait => Ok(false),
        CrossShardChildAction::Retire => {
            // Conditional on `NOT cancel_requested`, and that condition is
            // load-bearing rather than defensive.
            //
            // One sweep reads its work-list and the parents' terminal states in
            // TWO statements, deliberately holding no transaction on the parent's
            // shard while it reaches across to another database. Under READ
            // COMMITTED each statement therefore gets its own snapshot, and a
            // parent that completes BETWEEN them is observed with a fresh
            // terminal state against a stale row. The parent's terminal and its
            // race-loser cancel flag commit together, so the flag is exactly what
            // goes missing from that torn read — and `Retire` would then delete
            // the row, discarding a cancellation that was durably requested and
            // abandoning a child that runs forever with nothing tracking it.
            //
            // Observed against real shard databases, 21ms apart:
            //   flag committed        cancel_requested = true
            //   sweep decided         cancel=false parent_terminal=Some(true) -> Retire
            //
            // Re-checking the flag inside the DELETE closes it without a lock or
            // a wider snapshot: the row either still has no cancel pending (safe
            // to retire) or has gained one (leave it; the next sweep cancels).
            // Losing this CAS is not a failure — it is the flag arriving.
            let deleted = diesel::delete(
                harvest_cross_shard_children::table
                    .find(row.child_exec_id)
                    .filter(harvest_cross_shard_children::cancel_requested.eq(false)),
            )
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;
            Ok(deleted > 0)
        }
        CrossShardChildAction::StartChild => {
            start_child_on_target(conn, pool, row, acquire_bound, codecs, metrics).await?;
            // Only after the child is durably committed on the target shard.
            // A crash before this update simply re-runs the insert, which the
            // child's primary key makes a no-op.
            diesel::update(harvest_cross_shard_children::table.find(row.child_exec_id))
                .set((
                    harvest_cross_shard_children::status
                        .eq(CrossShardChildStatus::Started.as_db_str()),
                    harvest_cross_shard_children::last_error.eq(None::<String>),
                    harvest_cross_shard_children::attempts.eq(0),
                ))
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
            Ok(true)
        }
        CrossShardChildAction::CancelChild => {
            // Clearing `cancel_requested` is what takes this row OUT of the
            // actionable set, so it must not happen until the cancel is
            // observable on the target shard. Clearing it eagerly is a silent
            // child leak, not a cosmetic ordering choice: with the flag gone and
            // the parent already terminal, the very next sweep reads an awaited
            // child that is neither cancelled nor finished and correctly decides
            // `Retire` — deleting the row and abandoning a child that then runs
            // forever with nothing left tracking it (issue #956).
            //
            // Leaving the flag set costs one more sweep in the common case and
            // is idempotent by construction: `cancel_workflow_execution` on an
            // already-`CANCELLED` execution returns the idempotent success arm.
            if cancel_child_on_target(pool, row, acquire_bound, metrics).await? {
                diesel::update(harvest_cross_shard_children::table.find(row.child_exec_id))
                    .set((
                        harvest_cross_shard_children::cancel_requested.eq(false),
                        harvest_cross_shard_children::last_error.eq(None::<String>),
                        harvest_cross_shard_children::attempts.eq(0),
                    ))
                    .execute(conn)
                    .await
                    .map_err(crate::error::database_error)?;
                return Ok(true);
            }
            // Not settled yet. Record it as an attempt so the row backs off
            // rather than spinning at full poll cadence, and stays actionable.
            record_attempt_failure(
                conn,
                row.child_exec_id,
                "cancel issued but the child is not terminal yet",
            )
            .await;
            Ok(false)
        }
        CrossShardChildAction::ApplyCloseCascade => {
            let policy = observation
                .parent_close_policy
                .expect("ApplyCloseCascade is only decided for a detached child");
            cascade_child_on_target(pool, row, policy, acquire_bound, metrics).await?;
            apply_cascade_bookkeeping(conn, row, policy).await?;
            Ok(true)
        }
        CrossShardChildAction::DeliverTerminal => {
            let Some(child) = observed_child else {
                // `DeliverTerminal` is only decided from an observed terminal
                // state, so this is unreachable; treat it as "wait" rather than
                // panicking inside a scanner.
                return Ok(false);
            };
            deliver_terminal(conn, row, child, codecs).await?;
            Ok(true)
        }
    }
}

/// Record a completed cross-shard cascade on the parent, then retire the row.
///
/// Lock order is **execution row -> outbox row**, matching every other path in
/// the engine (the parent's own persist appends events — taking the parent row
/// lock — before it writes or flags an outbox row). Taking the outbox row first
/// here would let a relay sweep and a concurrent parent decision cycle form a
/// wait-for cycle; Postgres would abort one with a raw `deadlock_detected`,
/// which is neither `QuotaExceeded` nor `ShardUnavailable` and would therefore
/// terminally fail a perfectly healthy parent.
///
/// The claim-by-delete inside the transaction is what makes the append
/// exactly-once: every worker assigned this shard sweeps the same rows, so two
/// sweeps can decide the same cascade in the same tick. Their effect on the
/// target shard is idempotent; a history append is not.
async fn apply_cascade_bookkeeping(
    conn: &mut AsyncPgConnection,
    row: &CrossShardChildRow,
    policy: ParentClosePolicy,
) -> HarvestResult<()> {
    let child_exec_id = ExecutionId::from_uuid(row.child_exec_id);
    let parent_exec_id = ExecutionId::from_uuid(row.parent_exec_id);
    let action_str = match policy {
        ParentClosePolicy::RequestCancel => "request_cancel",
        ParentClosePolicy::Terminate => "terminate",
        ParentClosePolicy::Abandon => unreachable!("Abandon never reaches the cascade"),
    };
    Box::pin(conn.transaction::<(), HarvestError, _>(async |conn| {
        // Execution row first (see the fn doc). A parent whose row is gone
        // entirely — retention-collected while its detached child's shard was
        // down — has no history to append to; retire the row rather than
        // failing forever on a `NotFound` from `append_single_event`.
        let parent_exists: Option<uuid::Uuid> = harvest_workflow_executions::table
            .find(parent_exec_id.as_uuid())
            .select(harvest_workflow_executions::id)
            .for_update()
            .first(conn)
            .await
            .optional()
            .map_err(crate::error::database_error)?;

        if !claim_row_by_delete(conn, child_exec_id.as_uuid()).await? {
            // A peer sweep already recorded this cascade.
            return Ok(());
        }
        if parent_exists.is_some() {
            store::append_single_event(
                conn,
                parent_exec_id,
                WorkflowEvent::ChildWorkflowCascadeApplied {
                    child_id: child_exec_id,
                    policy,
                    action: action_str.to_string(),
                },
            )
            .await?;
        }
        Ok(())
    }))
    .await
}

/// Is this database's `harvest_cross_shard_children` present?
///
/// Asked only after a sweep query has already failed, to tell "the migration has
/// not run here yet" (skip quietly) from a real database fault (propagate). Uses
/// `to_regclass`, which answers from the catalog and returns NULL rather than
/// raising for an absent relation — so this check cannot itself fail the way the
/// query it is diagnosing did. A failure to answer is reported as "present", so
/// an ambiguous catalog read surfaces the ORIGINAL error rather than silently
/// disabling the relay.
async fn cross_shard_table_exists(conn: &mut AsyncPgConnection) -> bool {
    use diesel::dsl::sql;
    use diesel::sql_types::{Nullable, Text};

    diesel::select(sql::<Nullable<Text>>(
        "to_regclass('harvest_cross_shard_children')::text",
    ))
    .get_result::<Option<String>>(conn)
    .await
    .map_or(true, |found| found.is_some())
}

/// Delete the outbox row and report whether **this** transaction removed it.
///
/// This is the exactly-once gate for the two relay steps that append to the
/// parent's history (`DeliverTerminal` and `ApplyCloseCascade`). Every worker
/// assigned the parent's shard runs the relay over the same rows — the work-list
/// read is deliberately lock-free so a sweep never holds a transaction on the
/// parent's shard while it reaches across to another database — so two workers
/// can decide the same action in the same tick. Their cross-shard *effects* are
/// idempotent, but a history append is not: two appends would put two
/// `ChildWorkflowCompleted` events for one child into the parent's history and
/// break replay.
///
/// Making the delete the gate closes that: it takes a row lock, so the second
/// transaction blocks until the first commits and then deletes zero rows,
/// telling it to append nothing. The delete and the append commit together, so
/// the pair is atomic in both directions.
///
/// Callers must take the parent execution row **first** — see
/// [`apply_cascade_bookkeeping`]'s note on lock order.
async fn claim_row_by_delete(
    conn: &mut AsyncPgConnection,
    child_exec_id: uuid::Uuid,
) -> HarvestResult<bool> {
    let deleted = diesel::delete(harvest_cross_shard_children::table.find(child_exec_id))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(deleted > 0)
}

/// Record a failed relay step on the row, so an operator can see *why* a
/// cross-shard child is not progressing without reading logs.
///
/// Also drives the retry backoff: `attempts` is the exponent in `DUE_PREDICATE`,
/// so a row that keeps failing both backs off and stops occupying a slot in
/// every sweep. Best effort — a failure to record a failure is not worth failing
/// the sweep over.
async fn record_attempt_failure(
    conn: &mut AsyncPgConnection,
    child_exec_id: uuid::Uuid,
    error: &str,
) {
    let truncated: String = error.chars().take(500).collect();
    let _ = diesel::update(harvest_cross_shard_children::table.find(child_exec_id))
        .set((
            harvest_cross_shard_children::attempts.eq(harvest_cross_shard_children::attempts + 1),
            harvest_cross_shard_children::last_error.eq(Some(truncated)),
            harvest_cross_shard_children::last_attempt_at.eq(Some(Utc::now())),
        ))
        .execute(conn)
        .await;
}

/// Create the child execution on its target shard.
///
/// Idempotent by the child's primary key: an existence pre-check plus
/// `ON CONFLICT DO NOTHING` makes a repeated relay (a crash between this commit
/// and the row's status update) a no-op rather than a duplicate child. The whole
/// creation — row, its own `WorkflowStarted` event, its queue task — is one
/// transaction on the target shard, so a partially-created child is impossible.
// Long by construction: `NewWorkflowExecution` is a wide, fully-explicit row
// literal (every column named, no `..Default`), exactly as at the two
// same-shard child-insert sites. Splitting it would hide which columns a
// cross-shard child gets, which is the one thing a reader needs to check here.
#[allow(clippy::too_many_lines)]
async fn start_child_on_target(
    parent_conn: &mut AsyncPgConnection,
    pool: &ShardedDbPool,
    row: &CrossShardChildRow,
    acquire_bound: Option<std::time::Duration>,
    codecs: &crate::payload_codec::PayloadCodecs,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
) -> HarvestResult<()> {
    let mut conn = target_conn(pool, row, acquire_bound).await?;

    let spec: CrossShardChildSpec =
        serde_json::from_value(row.child_spec.clone()).map_err(HarvestError::Serialization)?;
    let child_exec_id = ExecutionId::from_uuid(row.child_exec_id);
    let parent_exec_id = ExecutionId::from_uuid(row.parent_exec_id);
    let child_workflow_id = child_exec_id.to_string();
    let parent_exec_id_str = parent_exec_id.to_string();
    let workflow_name = row.workflow_name.clone();
    let parent_close_policy = row.parent_close_policy.clone();

    // Re-read the cancel flag now rather than trust `row`'s snapshot from
    // the top of this sweep (issue #1263 item 13 follow-up). The snapshot
    // cannot see a cancellation that commits after the sweep's batch read
    // but before this specific child's creation. A target worker could
    // otherwise claim and run a task the parent already cancelled. This
    // narrows that window to the time between this query and the target
    // transaction's own commit. It runs on `parent_conn`'s own connection,
    // held in no transaction of its own, so it never blocks on the target
    // shard's work below.
    let cancel_requested: bool = harvest_cross_shard_children::table
        .find(row.child_exec_id)
        .select(harvest_cross_shard_children::cancel_requested)
        .first(parent_conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?
        .unwrap_or(row.cancel_requested);

    // The child's queue task is written inside this transaction, so it raises a
    // dispatch hint (issue #1312). The buffering scope holds the hint until the
    // transaction commits on the target shard.
    // The born-cancelled path's terminal metric is deferred past this call
    // (issue #1263 item 13 follow-up). Emitting it INSIDE the transaction
    // would double-count on a retry after a rollback here. This matches
    // the same "record after commit" contract `cancel_workflow_execution`
    // already documents for the same-shard path.
    let (deferred_terminal, pending_cancel_metrics) =
        crate::dispatch::buffered_settled(Box::pin(conn.transaction::<(
            Option<(String, String)>,
            Vec<crate::execution::StartCancelledRun>,
        ), HarvestError, _>(async |conn| {
            let spec = spec.clone();
            {
                let already: Option<uuid::Uuid> = harvest_workflow_executions::table
                    .find(child_exec_id.as_uuid())
                    .select(harvest_workflow_executions::id)
                    .first(conn)
                    .await
                    .optional()
                    .map_err(crate::error::database_error)?;
                if already.is_some() {
                    return Ok((None, Vec::new()));
                }

                // Anchor every deadline at creation, not at the parent's
                // decision. The relay can be minutes behind that decision —
                // an unreachable target shard, a large backlog, a worker
                // restart. An absolute deadline computed back then can
                // already be in the past by the time the row lands. The
                // timeout, SLA, and chain scanners would then seal a child
                // that has not run a single step. The normal start path
                // derives every deadline from the target's own start time,
                // for exactly this reason. Only durations travel on the
                // spec, and they become absolute here. Issue #1263 item 7
                // extended this to the chain deadline, which used to be the
                // one exception.
                let created_at = Utc::now();
                // `checked_add_signed` (not `+`): `DateTime + Duration` panics
                // on overflow, and these seconds values are caller-supplied.
                // `execution::start_or_load_workflow_execution`'s chain-ceiling
                // computation uses the same guard for the same reason. `None`
                // on overflow means no deadline, not a crashed relay sweep.
                let deadline_at = spec.execution_timeout_secs.and_then(|secs| {
                    created_at.checked_add_signed(chrono::Duration::seconds(secs))
                });
                let sla_deadline_at = spec.sla_secs.and_then(|secs| {
                    created_at.checked_add_signed(chrono::Duration::seconds(secs))
                });
                let chain_deadline_at = spec.chain_execution_timeout_secs.and_then(|secs| {
                    created_at.checked_add_signed(chrono::Duration::seconds(secs))
                });

                let child_row = NewWorkflowExecution {
                    continued_from_exec_id: None,
                    first_exec_id: None,
                    chain_execution_timeout: spec
                        .chain_execution_timeout_secs
                        .map(chrono::Duration::seconds),
                    chain_deadline_at,
                    id: child_exec_id.as_uuid(),
                    workflow_name: &workflow_name,
                    workflow_id: &child_workflow_id,
                    run_id: uuid::Uuid::new_v4(),
                    // The child's row lives on the TARGET shard and must say so:
                    // its `ExecutionId` already encodes this shard, and a mismatched
                    // column would make every shard-filtered scanner query (timeouts,
                    // outboxes, the SLA sweep) skip it.
                    shard_id: row.target_shard,
                    input: spec.input.clone(),
                    parent_id: Some(parent_exec_id.as_uuid()),
                    queue_name: &spec.queue_name,
                    execution_timeout: spec.execution_timeout_secs.map(chrono::Duration::seconds),
                    deadline_at,
                    sla: spec.sla_secs.map(chrono::Duration::seconds),
                    sla_deadline_at,
                    memo: None,
                    search_attrs: None,
                    assigned_build_id: spec.assigned_build_id.clone(),
                    parent_close_policy: parent_close_policy.clone(),
                    owner: spec.owner.as_deref(),
                    runbook_url: spec.runbook_url.as_deref(),
                    severity: spec.severity.as_deref(),
                    context_headers: spec.context_headers.clone(),
                    schedule_id: None,
                    scheduled_for: None,
                    workflow_attempt: 1,
                    workflow_retry_policy: spec.retry_policy.clone(),
                    retry_of_exec_id: None,
                    origin: None,
                    completion_callbacks: None,
                    start_source: Some(crate::types::StartSource::Child.as_str()),
                    start_source_ref: Some(parent_exec_id_str.as_str()),
                    started_by: None,
                    quota_key: spec.quota_key.as_deref(),
                };
                let inserted = diesel::insert_into(harvest_workflow_executions::table)
                    .values(&child_row)
                    .on_conflict(harvest_workflow_executions::id)
                    .do_nothing()
                    .execute(conn)
                    .await
                    .map_err(crate::error::database_error)?;
                if inserted == 0 {
                    // Another sweep won the race; its transaction owns the child's
                    // event and task.
                    return Ok((None, Vec::new()));
                }

                // The CONFIGURED codec registry, never `PayloadCodecs::default()`.
                // The child's `WorkflowStarted` carries its input, so writing it
                // through the identity codec would store that payload in the clear
                // on a deployment that has a keyed codec registered (#948) —
                // silently, and only for children that opted into cross-shard
                // placement. Every same-shard spawn path resolves its codecs from
                // the runtime for exactly this reason.
                //
                // A row flagged for cancellation before this sweep ever
                // created it (issue #1263 item 13) gets its
                // `WorkflowCancelled` appended right behind `WorkflowStarted`.
                // Both land in the SAME batch — never a separate append after
                // the task below is enqueued. See the cancellation arm's own
                // comment for why.
                //
                // KNOWN GAP: the large-payload *offloader* is not applied here. It
                // lives on the handler registry, which a scanner does not hold, and
                // threading it would touch ~29 call sites across the repo for what
                // is a storage optimisation rather than a correctness or
                // confidentiality property — the child-input cap is already enforced
                // at spawn time, so an over-cap payload never becomes a cross-shard
                // child in the first place. Tracked as a follow-up.
                let started_event = WorkflowEvent::WorkflowStarted {
                    input: spec.input.clone(),
                    timestamp: created_at,
                    last_completion_result: None,
                    last_error: None,
                    scheduled_time: None,
                };
                if cancel_requested {
                    store::append_events_offloaded_with_codecs(
                        conn,
                        child_exec_id,
                        &[
                            started_event,
                            WorkflowEvent::WorkflowCancelled {
                                reason: CROSS_SHARD_BORN_CANCELLED_REASON.to_string(),
                            },
                        ],
                        0,
                        None,
                        codecs,
                    )
                    .await?;
                    // Sealed CANCELLED with no task ever enqueued (issue
                    // #1263 item 13). The decision table
                    // (`shard::next_cross_shard_child_action`) sends every
                    // `PENDING_START` row here regardless of
                    // `cancel_requested`: a cancel that lands mid-creation
                    // still needs a row to act on.
                    //
                    // Enqueuing the task first, then cancelling it right
                    // after (mirroring a same-shard race loser), would still
                    // leave a real window open. On the same-shard path the
                    // loser is cancelled inside the PARENT's own transaction,
                    // before any task for it exists. Here THIS transaction is
                    // the one that would create that task. A worker could
                    // claim it the instant this transaction committed, and
                    // run a live decision cycle for a child that had already
                    // lost its race. Never creating the task closes that
                    // window outright.
                    //
                    // This check runs BEFORE quota admission below (issue
                    // #1263 item 13 follow-up). A race loser that is ALSO
                    // over quota must still settle into CANCELLED. It will
                    // never run, so it never actually consumes the quota it
                    // would otherwise be rejected for. Enforcing admission
                    // first would instead roll back this transaction every
                    // sweep, leaving the loser stuck `PENDING_START` for as
                    // long as the quota stays exceeded.
                    diesel::update(
                        harvest_workflow_executions::table.find(child_exec_id.as_uuid()),
                    )
                    .set((
                        harvest_workflow_executions::state.eq("CANCELLED"),
                        harvest_workflow_executions::error
                            .eq(Some(CROSS_SHARD_BORN_CANCELLED_REASON)),
                        harvest_workflow_executions::completed_at.eq(Some(created_at)),
                    ))
                    .execute(conn)
                    .await
                    .map_err(crate::error::database_error)?;
                    // A born-cancelled child bypasses `cancel_workflow_execution`.
                    // So its terminal metric and completion triggers need this
                    // explicit handling (issue #1263 item 13 follow-up).
                    // Otherwise the fleet-wide cancelled count would silently
                    // undercount, and a workflow configured to start on this
                    // child's cancellation never would. Every trigger start
                    // returned already has its own durable outbox row,
                    // committed by this same call. `spawn()` here is a
                    // best-effort latency nudge, not the only path to it.
                    // The terminal metric itself is NOT emitted here — see
                    // this function's own call site, past the transaction's
                    // commit.
                    //
                    // `_collecting`, not the plain wrapper, and `Some(metrics)`
                    // (issue #1263 item 13 follow-up). This relay has the real
                    // recorder in scope. Passing `None` would silently drop
                    // every `record_completion_trigger_fired` /
                    // `_skipped` sample below. A same-shard trigger start can
                    // also latest-wins supersede an incumbent run (issue
                    // #811). Those samples must wait for this transaction to
                    // commit, exactly like the terminal metric above. They
                    // are returned rather than emitted inline.
                    let mut pending_cancel_metrics = Vec::new();
                    for start in
                        crate::completion_trigger::evaluate_triggers_for_execution_collecting(
                            conn,
                            child_exec_id,
                            crate::completion_trigger::TerminalState::Cancelled,
                            Some(metrics),
                            &mut pending_cancel_metrics,
                        )
                        .await?
                    {
                        start.spawn();
                    }
                    return Ok((
                        Some((workflow_name.clone(), spec.queue_name.clone())),
                        pending_cancel_metrics,
                    ));
                }

                // The child's OWN declared quota (issue #946), enforced against the
                // row this transaction just inserted and BEFORE its `WorkflowStarted`
                // event is appended — the identical insert-then-enforce ordering the
                // same-shard child path uses, so `history_bytes` reports usage
                // strictly before this admission. Skipped entirely above when the
                // child is already cancelled — see that branch's own comment.
                crate::execution::enforce_quota_admission(
                    conn,
                    spec.quota.map(QuotaCaps::to_policy),
                    spec.quota_key.as_deref(),
                    &workflow_name,
                    Some(metrics),
                    None, // no dry-run credit on a cross-shard child spawn (children never declare cancel_running)
                    child_exec_id,
                )
                .await?;
                store::append_events_offloaded_with_codecs(
                    conn,
                    child_exec_id,
                    &[started_event],
                    0,
                    None,
                    codecs,
                )
                .await?;

                let mut params = queue::EnqueueParams::new(
                    spec.queue_name.clone(),
                    TaskType::Workflow,
                    spec.input.clone(),
                );
                params.workflow_exec_id = Some(child_exec_id.as_uuid());
                params.required_build_id = spec.assigned_build_id.clone();
                params.concurrency_key = spec.concurrency_key.clone();
                params.max_concurrent = spec.max_concurrent;
                params.trace_context = spec.trace_context.clone();
                queue::enqueue(conn, &params).await?;
                Ok((None, Vec::new()))
            }
        })))
        .await?;

    // Only reached once the transaction above has actually committed.
    // Neither the terminal metric nor the trigger-supersede samples below
    // can double-count on a retry after a rollback.
    if let Some((workflow_name, queue_name)) = deferred_terminal {
        crate::telemetry::emit_workflow_terminal(
            metrics,
            &workflow_name,
            &queue_name,
            crate::telemetry::WorkflowStatus::Cancelled,
        );
    }
    crate::execution::emit_start_cancel_metrics(metrics, &pending_cancel_metrics);
    Ok(())
}

/// Deliver an idempotent cancel to a cross-shard child on its target shard.
///
/// Takes the scanner's REAL metrics recorder, not a no-op. `cancel_workflow_execution`
/// emits `harvest.workflow.terminal{outcome="cancelled"}` itself, so swallowing
/// the recorder here would make fleet-wide terminal counts depend on whether a
/// child happened to be placed locally or remotely — silently under-counting
/// exactly the cancellations a distributed fan-out produces.
async fn cancel_child_on_target(
    pool: &ShardedDbPool,
    row: &CrossShardChildRow,
    acquire_bound: Option<std::time::Duration>,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
) -> HarvestResult<bool> {
    let mut conn = target_conn(pool, row, acquire_bound).await?;
    let child_exec_id = ExecutionId::from_uuid(row.child_exec_id);
    let result = crate::execution::cancel_workflow_execution(
        &mut conn,
        child_exec_id,
        "parent requested cancellation",
        metrics,
    )
    .await;
    absorb_already_settled(&mut conn, child_exec_id, result).await?;

    // Re-read the child under the SAME connection and report whether the cancel
    // is actually observable yet. The caller clears `cancel_requested` only on
    // `true`; see its call site for why an unobserved cancel must NOT clear it.
    let state: Option<String> = harvest_workflow_executions::table
        .find(child_exec_id.as_uuid())
        .select(harvest_workflow_executions::state)
        .first(&mut *conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?;

    // An absent row is settled as far as the parent is concerned: retention or
    // erase collected it, and the vanished-child arm reports that separately.
    Ok(state.is_none_or(|s| crate::erase::is_terminal_state(&s)))
}

/// Apply a `ParentClosePolicy` to a detached cross-shard child.
async fn cascade_child_on_target(
    pool: &ShardedDbPool,
    row: &CrossShardChildRow,
    policy: ParentClosePolicy,
    acquire_bound: Option<std::time::Duration>,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
) -> HarvestResult<()> {
    let mut conn = target_conn(pool, row, acquire_bound).await?;
    let child_exec_id = ExecutionId::from_uuid(row.child_exec_id);
    let result = match policy {
        ParentClosePolicy::RequestCancel => {
            crate::execution::cancel_workflow_execution(
                &mut conn,
                child_exec_id,
                "parent closed",
                metrics,
            )
            .await
        }
        ParentClosePolicy::Terminate => {
            crate::execution::terminate_workflow_execution(
                &mut conn,
                child_exec_id,
                "ParentClosed",
                metrics,
            )
            .await
        }
        ParentClosePolicy::Abandon => unreachable!("Abandon never reaches the cascade"),
    };
    absorb_already_settled(&mut conn, child_exec_id, result).await
}

/// Treat "the child is already gone or already terminal" as success — but only
/// after **confirming** it.
///
/// Both cross-shard mutations (cancel and cascade) are delivered at-least-once,
/// so a redelivery must be indistinguishable from the first delivery.
/// `NotFound` says outright that the child row is gone, which meets the goal.
/// `Config` does not: the engine uses it both for "already terminal for another
/// reason" *and* for genuinely unrelated failures — `apply_parent_close_cascade`
/// returns `Config` when a **grandchild's** stored `parent_close_policy` string
/// will not parse, and swallowing that would have us record a successful cascade
/// while the child kept running, untracked. So a `Config` is only absorbed when
/// a re-read proves the child really is terminal; otherwise it is a real failure
/// and the row retries after a backoff.
async fn absorb_already_settled<T>(
    conn: &mut AsyncPgConnection,
    child_exec_id: ExecutionId,
    result: HarvestResult<T>,
) -> HarvestResult<()> {
    match result {
        Ok(_) | Err(HarvestError::NotFound(_)) => Ok(()),
        Err(HarvestError::Config(reason)) => {
            let state: Option<String> = harvest_workflow_executions::table
                .find(child_exec_id.as_uuid())
                .select(harvest_workflow_executions::state)
                .first(conn)
                .await
                .optional()
                .map_err(crate::error::database_error)?;
            match state {
                None => Ok(()),
                Some(state) if crate::erase::is_terminal_state(&state) => Ok(()),
                Some(state) => Err(HarvestError::Config(format!(
                    "cross-shard child {child_exec_id} is still {state} after a failed \
                     cancel/terminate: {reason}"
                ))),
            }
        }
        Err(e) => Err(e),
    }
}

/// Deliver a terminal child's outcome to its awaiting parent.
///
/// The child's terminal payload was already read from the target shard by
/// `load_child_states`, so this costs no second cross-shard round trip and
/// delivers exactly the state the action was decided from. The parent's
/// `ChildWorkflowCompleted`/`ChildWorkflowFailed` append, its wake, and the
/// outbox row's delete all commit **in one transaction on the parent's shard**.
///
/// Lock order is **execution row -> outbox row**, matching every other path in
/// the engine (see [`apply_cascade_bookkeeping`]). Within that order the
/// claim-by-delete is the exactly-once gate: two concurrent sweeps can decide
/// the same delivery, and while their observation of the child is
/// at-least-once, the parent must see exactly one terminal event.
///
/// A parent that has already sealed is skipped (the append would add
/// replay-visible history past closure) but its row is still deleted — the same
/// "append only to a live parent" rule `notify_awaited_parent_of_child_terminal`
/// enforces on the same-shard path.
async fn deliver_terminal(
    conn: &mut AsyncPgConnection,
    row: &CrossShardChildRow,
    child: &TargetChildState,
    // Issue #1243: `ChildWorkflowCompleted.output` / a typed
    // `ChildWorkflowFailed.details` are payload-bearing.
    codecs: &crate::payload_codec::PayloadCodecs,
) -> HarvestResult<()> {
    let child_exec_id = ExecutionId::from_uuid(row.child_exec_id);
    let parent_exec_id = ExecutionId::from_uuid(row.parent_exec_id);
    let state = child.state.clone();
    let output = child.output.clone();
    let error = child.error.clone();
    let typed_failure = child.typed_failure.clone();

    Box::pin(conn.transaction::<(), HarvestError, _>(async |conn| {
        {
            // Parent execution row FIRST — the engine-wide lock order. The
            // batched pre-read is only a hint: a parent that sealed between the
            // two must not receive history past closure, and this lock also
            // serialises the append against a concurrent parent termination,
            // exactly as the same-shard notify path does.
            let parent_state: Option<String> = harvest_workflow_executions::table
                .find(parent_exec_id.as_uuid())
                .select(harvest_workflow_executions::state)
                .for_update()
                .first(conn)
                .await
                .optional()
                .map_err(crate::error::database_error)?;

            // Then claim the row. Losing the claim means a peer sweep already
            // delivered this terminal; there is nothing left to do.
            if !claim_row_by_delete(conn, child_exec_id.as_uuid()).await? {
                return Ok(());
            }

            let parent_live =
                matches!(parent_state, Some(ref s) if !crate::erase::is_terminal_state(s));
            if !parent_live {
                return Ok(());
            }

            // Order any DUE child-deadline timer BEFORE the child terminal so
            // `match_child_or_timer` resolves an over-deadline child to the
            // timeout branch on pure recorded order — the same #779 ordering
            // rule every same-shard wake site applies.
            crate::worker::materialize_due_child_timeout_deadlines(conn, parent_exec_id).await?;
            let event = if state == "COMPLETED" {
                WorkflowEvent::ChildWorkflowCompleted {
                    child_id: child_exec_id,
                    output: output.unwrap_or(serde_json::Value::Null),
                }
            } else {
                // Cancel, terminate, timeout and failure all surface to the
                // parent as `ChildWorkflowFailed` — there is no
                // `ChildWorkflowCancelled` variant and issue #956 adds none.
                // The wording mirrors the same-shard operator-cancel path so a
                // parent cannot tell where its child lived from the message.
                // Prefer the typed failure recovered from the child's own
                // `WorkflowFailed` event; fall back to the `error` column, which
                // carries only the human message and therefore decodes untyped.
                let decoded = typed_failure.clone().unwrap_or_else(|| {
                    let raw = error
                        .clone()
                        .unwrap_or_else(|| format!("child workflow {}", state.to_lowercase()));
                    crate::failure::decode_workflow_failure(&raw)
                });
                WorkflowEvent::child_workflow_failed_typed(child_exec_id, &decoded)
            };
            store::append_single_event_with_codecs(conn, parent_exec_id, event, codecs).await?;
            queue::wake_workflow_task(conn, parent_exec_id).await?;
            Ok(())
        }
    }))
    .await
}

/// Check out a connection to the target shard, under the multi-shard
/// acquisition bound.
async fn target_conn(
    pool: &ShardedDbPool,
    row: &CrossShardChildRow,
    acquire_bound: Option<std::time::Duration>,
) -> HarvestResult<diesel_async::pooled_connection::deadpool::Object<AsyncPgConnection>> {
    let shard_pool = pool
        .exact_pool_for(ShardId::new(row.target_shard))
        .ok_or_else(|| HarvestError::ShardUnavailable {
            shard_id: row.target_shard,
            reason: "no database pool is configured for this shard on this node".to_string(),
        })?;
    acquire_bounded(shard_pool, row.target_shard, acquire_bound).await
}

/// `pool.get()` under an optional deadline.
///
/// Harvest configures no deadpool `Timeouts`, so a bare `pool.get().await` is an
/// **unbounded** wait. That matters more here than almost anywhere else: the
/// relay holds a checked-out connection on the *parent's* shard for the whole
/// sweep while reaching across to other shards, and `Distributed` placement is
/// symmetric — shard A's parents target B while B's parents target A — so two
/// per-shard checkers on the same node can form a wait-for cycle across two
/// pools with no timeout on either side. Bounding the acquisition converts that
/// from a permanent hang into "skip this shard, retry next sweep", which is
/// exactly what `shard_acquire_bound` (issue #961) exists for.
async fn acquire_bounded(
    shard_pool: &crate::worker::DbPool,
    shard: i32,
    bound: Option<std::time::Duration>,
) -> HarvestResult<diesel_async::pooled_connection::deadpool::Object<AsyncPgConnection>> {
    let unavailable = |reason: String| HarvestError::ShardUnavailable {
        shard_id: shard,
        reason,
    };
    match bound {
        None => shard_pool
            .get()
            .await
            .map_err(|e| unavailable(format!("pool checkout failed: {e}"))),
        Some(bound) => match tokio::time::timeout(bound, shard_pool.get()).await {
            Ok(Ok(conn)) => Ok(conn),
            Ok(Err(e)) => Err(unavailable(format!("pool checkout failed: {e}"))),
            Err(_) => Err(unavailable(format!(
                "pool checkout did not complete within {bound:?}"
            ))),
        },
    }
}
