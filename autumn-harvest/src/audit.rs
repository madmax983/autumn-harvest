//! Management API audit trail (issue #158).
//!
//! Records who did what, when, and whether it succeeded for every high-impact
//! management mutation. Does not store raw workflow payloads, activity inputs,
//! or signal bodies — the audit trail is not a second PII store.
//!
//! ## Covered operations
//!
//! workflow.start, workflow.signal, workflow.cancel, workflow.reset,
//! dag.trigger, dag.patch, schedule.create, schedule.pause, schedule.resume,
//! schedule.delete, dlq.replay, dlq.replay.bulk, dlq.discard.bulk,
//! batch.submit, retention.run_now, external_activity.complete,
//! external_activity.fail.
//!
//! ## Explicitly excluded (never produce audit rows)
//!
//! Health checks, all list/get/query routes, worker heartbeats, activity
//! heartbeats, read-only UI page loads, worker fleet reads, and the audit
//! list endpoint itself. See [`crate::audit::EXCLUDED_ROUTES`] for the full list.

use chrono::{DateTime, Utc};
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel::SelectableHelper;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use uuid::Uuid;

use crate::error::{HarvestResult, database_error};
use crate::models::{AuditRecord, NewAuditRecord};
use crate::schema::harvest_audit_log;

// ── Operation name constants ──────────────────────────────────────────────────

/// Audit operation: Started a new workflow execution.
pub const OP_WORKFLOW_START: &str = "workflow.start";
/// Audit operation: Signaled a running workflow execution.
pub const OP_WORKFLOW_SIGNAL: &str = "workflow.signal";
/// Audit operation: Atomic start-or-attach + signal (issue #244).
pub const OP_WORKFLOW_SIGNAL_WITH_START: &str = "workflow.signal_with_start";
/// Audit operation: Atomic start-or-attach + update admission (issue #479).
pub const OP_WORKFLOW_UPDATE_WITH_START: &str = "workflow.update_with_start";
/// Audit operation: Cancelled a workflow execution.
pub const OP_WORKFLOW_CANCEL: &str = "workflow.cancel";
/// Audit operation: Force-terminated a single workflow execution (issue #504).
pub const OP_WORKFLOW_TERMINATE: &str = "workflow.terminate";
/// Audit operation: Reset a workflow execution to a previous state.
pub const OP_WORKFLOW_RESET: &str = "workflow.reset";
/// Audit operation: Operator re-run of a terminal workflow (issue #777).
///
/// Distinct from [`OP_WORKFLOW_RESET`], which forks an execution mid-history;
/// a re-run starts a brand-new execution of the whole workflow. The audit
/// row's `target_id` is the SOURCE execution id.
pub const OP_WORKFLOW_RERUN: &str = "workflow.rerun";
/// Audit operation: Paused an individual workflow execution (issue #383).
pub const OP_WORKFLOW_PAUSE: &str = "workflow.pause";
/// Audit operation: Resumed a paused workflow execution (issue #383).
pub const OP_WORKFLOW_RESUME: &str = "workflow.resume";
/// Audit operation: Set, updated, or cleared an execution's operator-mutable
/// triage tags -- owner, severity, note (issue #759).
pub const OP_WORKFLOW_ANNOTATE: &str = "workflow.annotate";
/// Audit operation: Manually triggered a DAG execution.
pub const OP_DAG_TRIGGER: &str = "dag.trigger";
/// Audit operation: Retried a DAG run from a failed node (issue #366).
pub const OP_DAG_RETRY: &str = "dag.retry";
/// Audit operation: Applied a hot-patch to an active DAG.
pub const OP_DAG_PATCH: &str = "dag.patch";
/// Audit operation: Created a new workflow schedule.
pub const OP_SCHEDULE_CREATE: &str = "schedule.create";
/// Audit operation: Edited an existing workflow schedule in place (issue #771).
pub const OP_SCHEDULE_UPDATE: &str = "schedule.update";
/// Audit operation: Paused an active workflow schedule.
pub const OP_SCHEDULE_PAUSE: &str = "schedule.pause";
/// Audit operation: Resumed a paused workflow schedule.
pub const OP_SCHEDULE_RESUME: &str = "schedule.resume";
/// Audit operation: Deleted a workflow schedule.
pub const OP_SCHEDULE_DELETE: &str = "schedule.delete";
/// Audit operation: Triggered a backfill for a workflow schedule.
pub const OP_SCHEDULE_BACKFILL: &str = "schedule.backfill";
/// Audit operation: Triggered an immediate one-off run of a schedule (issue #343).
pub const OP_SCHEDULE_TRIGGER: &str = "schedule.trigger";
/// Audit operation: Replayed a dead-letter queue (DLQ) task.
pub const OP_DLQ_REPLAY: &str = "dlq.replay";
/// Audit operation: Bulk-replayed dead-letter queue (DLQ) tasks.
pub const OP_DLQ_REPLAY_BULK: &str = "dlq.replay.bulk";
/// Audit operation: Bulk-discarded dead-letter queue (DLQ) tasks.
pub const OP_DLQ_DISCARD_BULK: &str = "dlq.discard.bulk";
/// Audit operation: Redrove (re-enqueued) dead-letter queue (DLQ) tasks (issue #510).
pub const OP_DLQ_REDRIVE: &str = "dlq.redrive";
/// Audit operation: Submitted a batch processing job.
pub const OP_BATCH_SUBMIT: &str = "batch.submit";
/// Audit operation: Atomically started a batch of workflow executions (issue #357).
pub const OP_BATCH_START: &str = "batch.start";
/// Audit operation: Triggered a retention sweep manually.
pub const OP_RETENTION_RUN_NOW: &str = "retention.run_now";
/// Audit operation: Completed an external activity.
pub const OP_EXTERNAL_ACTIVITY_COMPLETE: &str = "external_activity.complete";
/// Audit operation: Failed an external activity.
pub const OP_EXTERNAL_ACTIVITY_FAIL: &str = "external_activity.fail";
/// Audit operation: Initiated draining of a worker fleet.
pub const OP_WORKER_DRAIN: &str = "worker.drain";
/// Audit operation: Overrode rate-limiting parameters.
pub const OP_RATE_LIMIT_OVERRIDE: &str = "rate_limit_override";
/// Audit operation: Set (or updated) a TTL'd runtime pacing override on a
/// declared per-activity rate limit (issue #945).
///
/// Distinct from the legacy [`OP_RATE_LIMIT_OVERRIDE`] (issue #332), which is
/// a permanent write to a bucket's baseline `refill_rate`/`burst`: this
/// records a *temporary*, self-expiring layer on top of that baseline that
/// requires no worker restart to take effect and no operator action to
/// revert.
pub const OP_RATE_LIMIT_PACING_OVERRIDE_SET: &str = "rate_limit.pacing_override.set";
/// Audit operation: Cleared a TTL'd runtime pacing override on a declared
/// per-activity rate limit before its TTL elapsed (issue #945).
pub const OP_RATE_LIMIT_PACING_OVERRIDE_CLEAR: &str = "rate_limit.pacing_override.clear";
/// Audit operation: Rewound a shard's audit-export cursor so already-delivered
/// audit records are re-exported to the SIEM sink (issue #953).
///
/// The redrive is itself a privileged action — it causes records to be
/// re-shipped, and a cursor an operator can move is a control an auditor will
/// ask about — so it produces an audit record like any other mutation. Note
/// that a rewind can only ever move a cursor BACKWARDS; the route refuses a
/// forward request rather than recording one.
pub const OP_AUDIT_EXPORT_REDRIVE: &str = "audit_export.redrive";
/// Audit operation: Retired a shard's audit-export cursor, permitting
/// retention to purge its aged records (issue #953, issue #1273).
///
/// This discards the compliance guarantee over any record the shard had not
/// yet shipped. An auditor must be able to name who authorised that, so the
/// route is admin-gated and audited like every other privileged action here.
pub const OP_AUDIT_EXPORT_DECOMMISSION: &str = "audit_export.decommission";
/// Audit operation: Reactivated a shard's retired audit-export cursor
/// (issue #1273).
///
/// The inverse of [`OP_AUDIT_EXPORT_DECOMMISSION`]. Resuming export used to
/// be an implicit side effect of the exporter's next scanner tick; it is now
/// its own explicit, audited operator action, for the same reason retirement
/// is: an auditor must be able to name who decided to resume it.
pub const OP_AUDIT_EXPORT_REACTIVATE: &str = "audit_export.reactivate";
/// Audit operation: Set (or updated) a TTL'd runtime pacing override on a
/// declared workflow-start throttle (issue #945).
///
/// See [`OP_RATE_LIMIT_PACING_OVERRIDE_SET`] -- same TTL'd-layer semantics,
/// applied to a `#[workflow(throttle(...))]` bucket instead of an activity's.
pub const OP_START_THROTTLE_PACING_OVERRIDE_SET: &str = "start_throttle.pacing_override.set";
/// Audit operation: Cleared a TTL'd runtime pacing override on a declared
/// workflow-start throttle before its TTL elapsed (issue #945).
pub const OP_START_THROTTLE_PACING_OVERRIDE_CLEAR: &str = "start_throttle.pacing_override.clear";
/// Audit operation: Opened an SSE execution event stream (issue #324).
pub const OP_EXECUTION_STREAM_OPEN: &str = "execution.stream.open";
/// Audit operation: Closed an SSE execution event stream (issue #324).
pub const OP_EXECUTION_STREAM_CLOSE: &str = "execution.stream.close";
/// Audit operation: Set the active build policy for a queue (issue #362).
pub const OP_BUILD_POLICY_SET: &str = "build_routing.policy.set";
/// Audit operation: Declared a build compatibility entry (issue #362).
pub const OP_BUILD_COMPAT_DECLARE: &str = "build_routing.compat.declare";
/// Audit operation: Revoked a build compatibility entry (issue #362).
pub const OP_BUILD_COMPAT_REVOKE: &str = "build_routing.compat.revoke";
/// Audit operation: Forced an activity circuit breaker open (issue #369).
pub const OP_CIRCUIT_FORCE_OPEN: &str = "circuit.force_open";
/// Audit operation: Forced an activity circuit breaker closed (issue #369).
pub const OP_CIRCUIT_FORCE_CLOSE: &str = "circuit.force_close";
/// Audit operation: Re-prioritized a pending task queue item (issue #249).
pub const OP_TASK_REPRIORITIZE: &str = "task.reprioritize";
/// Audit operation: Created an admission gate (issue #377).
pub const OP_GATE_CREATE: &str = "gate.create";
/// Audit operation: Lifted (removed) an admission gate (issue #377).
pub const OP_GATE_LIFT: &str = "gate.lift";
/// Audit operation: Erased PII payload fields from a completed workflow
/// execution (issue #495). Terminal-only, irreversible.
pub const OP_WORKFLOW_ERASE_PAYLOADS: &str = "workflow.erase_payloads";
/// Audit operation: Placed a per-execution legal hold (issue #747).
pub const OP_LEGAL_HOLD_SET: &str = "legal_hold.set";
/// Audit operation: Released a per-execution legal hold (issue #747).
pub const OP_LEGAL_HOLD_RELEASE: &str = "legal_hold.release";
/// Audit operation: Ran the replay compatibility canary.
pub const OP_WORKFLOW_REPLAY_CANARY: &str = "workflow.replay_canary";
/// Audit operation: Force-retried a backing-off activity task (issue #516).
pub const OP_ACTIVITY_RETRY_NOW: &str = "activity.retry_now";
/// Audit operation: Force-failed a hung in-flight activity task (issue #765).
pub const OP_ACTIVITY_FAIL_NOW: &str = "activity.fail_now";
/// Audit operation: an operator held dispatch for one activity **type**
/// (issue #807).
///
/// Distinct from [`OP_ACTIVITY_RETRY_NOW`] / [`OP_ACTIVITY_FAIL_NOW`], which
/// act on a single in-flight *task*: this is the type-wide, fleet-wide hold an
/// operator reaches for when one downstream dependency is in a known outage.
pub const OP_ACTIVITY_PAUSE: &str = "activity.pause";
/// Audit operation: an operator released a per-activity-type hold (issue #807).
pub const OP_ACTIVITY_RESUME: &str = "activity.resume";
/// Audit operation: Batch-reset workflow executions to a semantic point (issue #538).
pub const OP_BATCH_RESET: &str = "batch.reset";
/// Audit operation: Set (or updated) a queue's percentage build ramp (issue #604).
pub const OP_BUILD_RAMP_SET: &str = "build_routing.ramp.set";
/// Audit operation: Cleared a queue's percentage build ramp (issue #604).
pub const OP_BUILD_RAMP_CLEAR: &str = "build_routing.ramp.clear";
/// Audit operation: Manually redrove a dead-lettered completion-callback
/// delivery (issue #605).
pub const OP_CALLBACK_REDRIVE: &str = "completion_callback.redrive";
/// Audit operation: an inbound webhook trigger dispatched a workflow start or signal (issue #344).
///
/// Written only for dispatch *attempts* that pass autumn-web's signature
/// verification -- an unauthenticated sender cannot generate audit rows by
/// sending unsigned/badly-signed requests. No
/// `ALL_MUTATION_ROUTES`/`CLASSIFIED_ROUTES` entry exists for this operation:
/// webhook binding paths are user-defined and registered as app-level routes
/// outside `harvest_api_router`, so they are invisible to (and correctly
/// excluded from) those management-router manifests. See
/// `docs/getting-started/12-webhooks.md`.
pub const OP_WEBHOOK_TRIGGER: &str = "webhook.trigger";
/// Audit operation: an operator read decoded codec-encrypted payloads on the
/// management API / Vantage UI read path (issue #608).
///
/// A **read** audit like [`OP_EXECUTION_STREAM_OPEN`]: written at most once
/// per request, and only when at least one codec envelope was actually
/// decoded or degraded to an `_harvest_undecodable` marker (the SSE stream
/// audits once at stream open whenever decode mode is active for that
/// stream, since frame counts are unknowable up front). The record names the
/// actor, route, and target execution — never the payload content. No
/// `CLASSIFIED_ROUTES`/`ALL_MUTATION_ROUTES` change: no route mutates and no
/// new route exists.
pub const OP_PAYLOAD_DECODE_READ: &str = "payload.decode_read";
/// Audit operation: minted a scoped API token (issue #942).
/// Operation: an operator paused dispatch on a task queue (issue #619).
pub const OP_QUEUE_PAUSE: &str = "queue.pause";
/// Operation: an operator resumed dispatch on a task queue (issue #619).
pub const OP_QUEUE_RESUME: &str = "queue.resume";

pub const OP_TOKEN_CREATE: &str = "token.create";
/// Audit operation: revoked a scoped API token (issue #942).
pub const OP_TOKEN_REVOKE: &str = "token.revoke";

// ── Target type constants ─────────────────────────────────────────────────────

pub const TARGET_CIRCUIT: &str = "circuit";
/// Audit target type for task queue operations (issue #249).
pub const TARGET_TASK: &str = "task";
/// Audit target type for admission gate operations (issue #377).
pub const TARGET_GATE: &str = "gate";
pub const TARGET_WORKFLOW: &str = "workflow";
pub const TARGET_DAG: &str = "dag";
pub const TARGET_SCHEDULE: &str = "schedule";
pub const TARGET_DEAD_LETTER: &str = "dead_letter";
pub const TARGET_BATCH: &str = "batch";
pub const TARGET_RETENTION: &str = "retention";
pub const TARGET_EXTERNAL_ACTIVITY: &str = "external_activity";
/// Audit target type for individual activity task operations (issue #516).
pub const TARGET_ACTIVITY: &str = "activity";
pub const TARGET_WORKER: &str = "worker";
pub const TARGET_RATE_LIMIT: &str = "rate_limit";
/// Audit target type for workflow-start throttle operations (issues #607, #945).
pub const TARGET_THROTTLE: &str = "throttle";
pub const TARGET_BUILD_ROUTING: &str = "build_routing";
/// Audit target type for completion-callback delivery operations (issue #605).
pub const TARGET_CALLBACK_DELIVERY: &str = "completion_callback_delivery";
/// Audit target type for scoped API token operations (issue #942).
pub const TARGET_TOKEN: &str = "token";
/// Target type: a named task queue (issue #619).
pub const TARGET_QUEUE: &str = "queue";
/// Audit target type: a shard's audit-export cursor (issue #953).
pub const TARGET_AUDIT_EXPORT: &str = "audit_export";

// ── Status constants ──────────────────────────────────────────────────────────

pub const STATUS_SUCCEEDED: &str = "succeeded";
pub const STATUS_FAILED: &str = "failed";

// ── Source constants ──────────────────────────────────────────────────────────

pub const SOURCE_API: &str = "api";
pub const SOURCE_CLI: &str = "cli";
pub const SOURCE_UI: &str = "ui";

// ── HTTP header names ─────────────────────────────────────────────────────────

/// Embedder-supplied operator identity header.
///
/// Set this on outgoing requests from the CLI or UI to distinguish call
/// origins. When absent, records default to `"anonymous"` — only acceptable
/// for local/dev deployments.
pub const HEADER_ACTOR: &str = "x-harvest-actor";

/// Correlation header forwarded from the originating request.
pub const HEADER_REQUEST_ID: &str = "x-request-id";

/// Call-origin hint: `"api"` (default), `"cli"`, or `"ui"`.
pub const HEADER_SOURCE: &str = "x-harvest-source";

/// Out-of-band exactly-once delivery key for the standalone signal route.
pub const HEADER_IDEMPOTENCY_KEY: &str = "idempotency-key";

// ── Retention ─────────────────────────────────────────────────────────────────

/// Default audit record retention in days (90 days ≈ 3 months).
///
/// This is intentionally longer than most incident review windows. Operators
/// can override this via `HarvestApiState::set_audit_retention_days`.
pub const DEFAULT_AUDIT_RETENTION_DAYS: i64 = 90;

// ── Security classification ───────────────────────────────────────────────────

/// Three-tier security classification for every Harvest management API route.
///
/// Embedders use this to decide which protection level applies to each route
/// category. The recommended production posture gates all three tiers behind
/// the host application's authentication middleware; the distinction matters
/// for graduated roll-out and for documenting intentional exposure choices.
///
/// See `docs/security-posture.md` for the full mounting recipe, CLI/token
/// semantics, and a production-readiness checklist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteClass {
    /// Always safe to expose without authentication.
    ///
    /// `GET /health` and `GET /openapi.json`. Kubernetes liveness/readiness
    /// probes and load-balancer health checks commonly require the health
    /// endpoint to be reachable without credentials. The `OpenAPI` document
    /// describes the route surface only and carries no execution state.
    /// Exposing both is an explicit product decision, not an oversight.
    PublicSafe,

    /// Reads operator state but does not modify workflow execution.
    ///
    /// List, get, query, and export routes. These do not trigger side effects
    /// but may expose sensitive operational data (execution IDs, input/output
    /// payloads, schedule definitions). Protect these in production with the
    /// same middleware layer as mutating routes.
    ReadOnly,

    /// Modifies workflow execution or system configuration.
    ///
    /// Includes workflow start/signal/cancel/reset, DLQ replay/discard,
    /// schedule mutation, batch submission, retention run-now, external
    /// activity completion, and worker drain. Every route in this class that
    /// carries production risk is covered by the audit trail (`harvest_audit_log`).
    /// These routes MUST be protected by authentication middleware in any
    /// non-local deployment.
    Mutating,
}

/// Pure access-control decision for the read-only operator role (issue #776).
///
/// Returns `true` when a read-only principal must be **denied (403)** the
/// route it is calling. A read-only principal may reach [`RouteClass::ReadOnly`]
/// and [`RouteClass::PublicSafe`] routes but never a [`RouteClass::Mutating`]
/// one.
///
/// **Fail closed by construction.** The enforcement layer resolves an
/// unclassified path to [`RouteClass::Mutating`] before calling this function,
/// so a route a contributor forgot to classify is denied to read-only
/// principals — never accidentally exposed.
///
/// This is deliberately a pure, `const` function keyed only on
/// `RouteClass` (the source-of-truth verb classification) so the security
/// decision is pinned by a truth-table unit test with no session or router
/// harness. Principal classification (read-only vs admin vs anonymous) is a
/// separate concern resolved by the enforcement layer and passed in as
/// `is_readonly_principal`.
#[must_use]
pub const fn deny_readonly_mutation(is_readonly_principal: bool, class: RouteClass) -> bool {
    is_readonly_principal && !matches!(class, RouteClass::ReadOnly | RouteClass::PublicSafe)
}

/// Security classification for every route registered in `harvest_api_router`.
///
/// Each entry is `(route_template, RouteClass)`. The exhaustiveness guard test
/// (`route_classification_covers_all_known_routes`) verifies that this slice
/// and [`ALL_MUTATION_ROUTES`] contain exactly the same route set, so adding a
/// new route to `harvest_api_router` without classifying it here causes a
/// compile-time-visible test failure.
///
/// **When you add a new route to `harvest_api_router`, you MUST add an entry
/// here AND in [`ALL_MUTATION_ROUTES`].**
pub const CLASSIFIED_ROUTES: &[(&str, RouteClass)] = &[
    // ── PublicSafe ── always safe to expose, even without auth ───────────────
    // Kubernetes liveness/readiness probes and load-balancer health checks
    // require /health to be reachable without credentials.
    ("GET /health", RouteClass::PublicSafe),
    // The published OpenAPI document. Route surface only, no execution state,
    // and a client generator must reach it before it holds a credential.
    ("GET /openapi.json", RouteClass::PublicSafe),
    // ── ReadOnly ── reads state, does not modify workflow execution ───────────
    // Audit-export status (issue #953): read-only, admin-gated. Reports cursor
    // position, lag, and last error — never audit record contents.
    ("GET /admin/audit-export", RouteClass::ReadOnly),
    ("GET /workflows/count", RouteClass::ReadOnly),
    // Tiered/summary retention list (issue #752): read-only, admin-guarded.
    ("GET /workflows/summaries", RouteClass::ReadOnly),
    ("GET /workflows", RouteClass::ReadOnly),
    ("GET /workflows/{id}", RouteClass::ReadOnly),
    ("GET /workflows/{id}/children", RouteClass::ReadOnly),
    // Recursive cross-shard lineage tree (issue #621): a point-in-time read of
    // `harvest_workflow_executions` rows over the existing `parent_id` edges.
    // Appends no events, performs no writes.
    ("GET /workflows/{id}/tree", RouteClass::ReadOnly),
    ("GET /workflows/{id}/stack", RouteClass::ReadOnly),
    ("GET /workflows/{id}/timeline", RouteClass::ReadOnly),
    // Durable per-execution author log lines (issue #790): admin-gated,
    // read-only projection of `harvest_workflow_logs`. Appends nothing.
    ("GET /workflows/{id}/logs", RouteClass::ReadOnly),
    // Open-awaitables diagnostic (issue #615): read-only replay projection of
    // what an execution is parked on; appends no events, performs no writes.
    ("GET /workflows/{id}/awaitables", RouteClass::ReadOnly),
    // Per-execution stall diagnosis (issue #809): read-only root-cause verdict
    // for a single stuck run. Reads only the owning shard; appends no events,
    // performs no task-queue mutation.
    ("GET /workflows/{id}/diagnose", RouteClass::ReadOnly),
    ("GET /workflows/{id}/run-chain", RouteClass::ReadOnly),
    // Single-execution replay diagnosis (issue #614): POST for the replay action
    // but read-only (appends no events, performs no writes, no audit trail).
    (
        "POST /workflows/{id}/replay-diagnosis",
        RouteClass::ReadOnly,
    ),
    (
        "GET /workflows/{id}/query/{query_name}",
        RouteClass::ReadOnly,
    ),
    // POST query accepts typed args but never mutates workflow state.
    (
        "POST /workflows/{id}/query/{query_name}",
        RouteClass::ReadOnly,
    ),
    ("GET /workflows/{id}/queries", RouteClass::ReadOnly),
    (
        "GET /workflows/{id}/update/{update_id}/result",
        RouteClass::ReadOnly,
    ),
    // Workflow terminal-output projection (issue #527): read-only long-poll.
    ("GET /workflows/{id}/result", RouteClass::ReadOnly),
    // Paginated, filterable single-execution history (issue #529): read-only cursor walk.
    ("GET /workflows/{id}/history", RouteClass::ReadOnly),
    ("GET /workflows/{id}/history/export", RouteClass::ReadOnly),
    // Completion-callback delivery listing (issue #605): read-only, no audit.
    (
        "GET /workflows/{id}/completion-deliveries",
        RouteClass::ReadOnly,
    ),
    ("GET /dags", RouteClass::ReadOnly),
    ("GET /dags/{dag_name}/runs", RouteClass::ReadOnly),
    // DAG run graph view (issue #690): read-only projection of a run's node
    // topology + status; no audit trail.
    (
        "GET /dags/{dag_name}/runs/{run_exec_id}",
        RouteClass::ReadOnly,
    ),
    ("GET /dead-letters", RouteClass::ReadOnly),
    ("GET /admin/preflight", RouteClass::ReadOnly),
    ("GET /admin/shards/health", RouteClass::ReadOnly),
    // Fleet-wide task-queue coverage (issue #774): read-only.
    ("GET /admin/queue-coverage", RouteClass::ReadOnly),
    ("GET /admin/status", RouteClass::ReadOnly),
    // Effective runtime-config introspection (issue #695): read-only, secret-free.
    ("GET /admin/config", RouteClass::ReadOnly),
    // Synthetic liveness canary freshness report (issue #796): read-only.
    ("GET /admin/canary", RouteClass::ReadOnly),
    ("GET /admin/version-gates/usage", RouteClass::ReadOnly),
    (
        "GET /admin/version-gates/retirement-check",
        RouteClass::ReadOnly,
    ),
    ("GET /admin/retention", RouteClass::ReadOnly),
    ("GET /admin/concurrency", RouteClass::ReadOnly),
    // Historical per-tenant/per-workflow usage report (issue #596): read-only
    // aggregation over already-durable data, companion to /admin/concurrency.
    ("GET /admin/usage", RouteClass::ReadOnly),
    ("GET /admin/debounce", RouteClass::ReadOnly),
    ("GET /admin/start-throttle", RouteClass::ReadOnly),
    // Per-tenant resource quota usage-vs-limit report (issue #946): read-only.
    ("GET /admin/quotas", RouteClass::ReadOnly),
    // Payload-codec key rotation progress (issue #948): read-only. Classifying
    // it matters — an unclassified path defaults to `Mutating`, which would
    // deny a read-only operator the very screen they watch to decide when an
    // outgoing key is safe to retire.
    ("GET /admin/codec/rotation", RouteClass::ReadOnly),
    // Workflow-type handler reachability (issue #520): read-only, no state mutation.
    (
        "GET /admin/workflow-types/reachability",
        RouteClass::ReadOnly,
    ),
    ("GET /admin/history/exports", RouteClass::ReadOnly),
    ("GET /admin/history/export-sample", RouteClass::ReadOnly),
    ("GET /admin/external-handoffs", RouteClass::ReadOnly),
    ("GET /admin/external-handoffs/{token}", RouteClass::ReadOnly),
    // Admission gates (issue #377)
    ("GET /admin/gates", RouteClass::ReadOnly),
    ("POST /admin/gates", RouteClass::Mutating),
    ("DELETE /admin/gates/{id}", RouteClass::Mutating),
    // Scoped API tokens (issue #942).
    ("GET /admin/tokens", RouteClass::ReadOnly),
    ("POST /admin/tokens", RouteClass::Mutating),
    ("DELETE /admin/tokens/{id}", RouteClass::Mutating),
    ("GET /admin/schedules", RouteClass::ReadOnly),
    ("GET /admin/rate-limits", RouteClass::ReadOnly),
    ("GET /admin/audit", RouteClass::ReadOnly),
    ("GET /workers/health", RouteClass::ReadOnly),
    ("GET /workers/drain-preview", RouteClass::ReadOnly),
    ("GET /workers", RouteClass::ReadOnly),
    ("GET /workers/{worker_id}", RouteClass::ReadOnly),
    ("GET /batch-operations", RouteClass::ReadOnly),
    ("GET /batch-operations/{id}", RouteClass::ReadOnly),
    // SSE execution event stream (issue #324): read-only long-poll, never mutates state.
    (
        "GET /executions/{exec_id}/events/stream",
        RouteClass::ReadOnly,
    ),
    // SSE ephemeral workflow progress stream (issue #791): read-only side
    // channel over LISTEN/NOTIFY, never mutates state. NOT admin-gated (AC5).
    ("GET /workflows/{id}/stream", RouteClass::ReadOnly),
    // ── Mutating ── modifies workflow execution or system configuration ───────
    // All of these are covered by the audit trail (harvest_audit_log) or are
    // explicitly listed in EXCLUDED_ROUTES with an audit disposition note.
    (
        "POST /workflows/{workflow_name}/start",
        RouteClass::Mutating,
    ),
    (
        "POST /workflows/{workflow_name}/signal-with-start",
        RouteClass::Mutating,
    ),
    ("POST /workflows/{id}/cancel", RouteClass::Mutating),
    ("POST /workflows/{id}/terminate", RouteClass::Mutating),
    ("POST /workflows/{id}/rerun", RouteClass::Mutating),
    ("POST /workflows/{id}/pause", RouteClass::Mutating),
    ("POST /workflows/{id}/resume", RouteClass::Mutating),
    ("PATCH /workflows/{id}/triage", RouteClass::Mutating),
    ("POST /workflows/{id}/reset", RouteClass::Mutating),
    (
        "POST /workflows/{id}/signal/{signal_name}",
        RouteClass::Mutating,
    ),
    // Update appends UpdateAdmitted to history and wakes the workflow — mutating.
    // Audit disposition: excluded (synchronous RPC; the event history is the record).
    (
        "POST /workflows/{id}/update/{update_name}",
        RouteClass::Mutating,
    ),
    ("POST /dags/{dag_name}/trigger", RouteClass::Mutating),
    ("PATCH /dags/{dag_name}", RouteClass::Mutating),
    ("POST /dead-letters/replay", RouteClass::Mutating),
    ("POST /dead-letters/discard", RouteClass::Mutating),
    ("POST /dead-letters/{id}/replay", RouteClass::Mutating),
    ("POST /dlq/redrive", RouteClass::Mutating),
    // Completion-callback delivery redrive (issue #605).
    (
        "POST /workflows/{id}/completion-deliveries/{delivery_id}/redrive",
        RouteClass::Mutating,
    ),
    ("POST /admin/retention/run-now", RouteClass::Mutating),
    ("POST /admin/schedules/workflow", RouteClass::Mutating),
    ("PATCH /admin/schedules/{id}", RouteClass::Mutating),
    ("POST /admin/schedules/{id}/pause", RouteClass::Mutating),
    ("POST /admin/schedules/{id}/resume", RouteClass::Mutating),
    ("POST /admin/schedules/{id}/backfill", RouteClass::Mutating),
    ("POST /admin/schedules/{id}/trigger", RouteClass::Mutating),
    ("DELETE /admin/schedules/{id}", RouteClass::Mutating),
    // External activity completion — task-token callbacks from remote workers.
    (
        "POST /activities/external/{token}/complete",
        RouteClass::Mutating,
    ),
    (
        "POST /activities/external/{token}/fail",
        RouteClass::Mutating,
    ),
    // Heartbeat writes liveness state but is intentionally excluded from audit
    // (high-volume, not a control-plane mutation). See EXCLUDED_ROUTES.
    (
        "POST /activities/external/{token}/heartbeat",
        RouteClass::Mutating,
    ),
    // Per-activity-type pause/resume (issue #807): the operator's hand on one
    // activity's dispatch valve during a scoped downstream outage. The two
    // reads are admin-gated but audit-excluded (a diagnostic read, like every
    // other read model); the two mutations are the fleet-wide hold control.
    ("GET /activities", RouteClass::ReadOnly),
    ("GET /activities/{activity_name}", RouteClass::ReadOnly),
    (
        "POST /activities/{activity_name}/pause",
        RouteClass::Mutating,
    ),
    (
        "POST /activities/{activity_name}/resume",
        RouteClass::Mutating,
    ),
    ("POST /workers/{worker_id}/drain", RouteClass::Mutating),
    ("POST /admin/rate-limits/{key}", RouteClass::Mutating),
    // TTL'd runtime pacing overrides for a declared per-activity rate limit
    // (issue #945): a temporary layer atop the baseline, not a permanent
    // write like the legacy route directly above.
    (
        "POST /admin/rate-limits/{activity_name}/override",
        RouteClass::Mutating,
    ),
    (
        "DELETE /admin/rate-limits/{activity_name}/override",
        RouteClass::Mutating,
    ),
    // TTL'd runtime pacing overrides for a declared workflow-start throttle
    // (issue #945). Same semantics as the rate-limit pair above.
    (
        "POST /admin/start-throttle/{workflow_name}/override",
        RouteClass::Mutating,
    ),
    (
        "DELETE /admin/start-throttle/{workflow_name}/override",
        RouteClass::Mutating,
    ),
    // Read-only companion: resolves an override's live state directly,
    // independent of any current deferred-start backlog (issue #945).
    (
        "GET /admin/start-throttle/{workflow_name}/override",
        RouteClass::ReadOnly,
    ),
    ("POST /batch-operations", RouteClass::Mutating),
    // Batch workflow start (issue #357)
    ("POST /workflows/batch_start", RouteClass::Mutating),
    // Build routing management (issue #362)
    ("GET /admin/build-routing", RouteClass::ReadOnly),
    ("POST /admin/build-routing/policies", RouteClass::Mutating),
    ("GET /admin/build-routing/compat", RouteClass::ReadOnly),
    ("POST /admin/build-routing/compat", RouteClass::Mutating),
    (
        "DELETE /admin/build-routing/compat/{build_id}/{compat_with}",
        RouteClass::Mutating,
    ),
    // Retire is a read-only reachability check; no DB state is written.
    ("POST /admin/build-routing/retire", RouteClass::ReadOnly),
    // Percentage build ramp (issue #604)
    ("POST /admin/build-routing/ramp", RouteClass::Mutating),
    (
        "DELETE /admin/build-routing/ramp/{queue_name}",
        RouteClass::Mutating,
    ),
    // Task queue management (issue #249)
    ("PATCH /tasks/{id}", RouteClass::Mutating),
    // PII erasure (issue #495): admin-only, irreversible, terminal-only.
    ("POST /workflows/{id}/erase-payloads", RouteClass::Mutating),
    // Per-execution legal hold (issue #747): admin-only.
    ("POST /workflows/{id}/legal-hold", RouteClass::Mutating),
    (
        "POST /workflows/{id}/legal-hold/release",
        RouteClass::Mutating,
    ),
    // Replay canary (issue #512): admin-only.
    ("POST /admin/workflows/replay-canary", RouteClass::Mutating),
    // Force-retry backing-off activity (issue #516): admin-only.
    (
        "POST /workflows/{id}/activities/{activity_exec_id}/retry-now",
        RouteClass::Mutating,
    ),
    // Force-fail hung in-flight activity (issue #765): admin-only.
    (
        "POST /workflows/{id}/activities/{activity_exec_id}/fail-now",
        RouteClass::Mutating,
    ),
    // Batch reset by semantic point (issue #538): admin-only.
    ("POST /workflows/batch_reset", RouteClass::Mutating),
    // ── Business-id ("latest run") route variants (issue #805) ────────────────
    // Each mirrors its exec-id counterpart's class exactly.
    (
        "GET /workflows/by-id/{workflow_name}/{workflow_id}",
        RouteClass::ReadOnly,
    ),
    // Empty-workflow_id guard (issue #1353): literal trailing-slash form of
    // the base route above; always returns 400 (no read, no write).
    (
        "GET /workflows/by-id/{workflow_name}/",
        RouteClass::ReadOnly,
    ),
    (
        "GET /workflows/by-id/{workflow_name}/{workflow_id}/result",
        RouteClass::ReadOnly,
    ),
    (
        "GET /workflows/by-id/{workflow_name}/{workflow_id}/stack",
        RouteClass::ReadOnly,
    ),
    (
        "GET /workflows/by-id/{workflow_name}/{workflow_id}/children",
        RouteClass::ReadOnly,
    ),
    (
        "GET /workflows/by-id/{workflow_name}/{workflow_id}/query/{query_name}",
        RouteClass::ReadOnly,
    ),
    // POST query accepts typed args but never mutates workflow state.
    (
        "POST /workflows/by-id/{workflow_name}/{workflow_id}/query/{query_name}",
        RouteClass::ReadOnly,
    ),
    (
        "POST /workflows/by-id/{workflow_name}/{workflow_id}/signal/{signal_name}",
        RouteClass::Mutating,
    ),
    (
        "POST /workflows/by-id/{workflow_name}/{workflow_id}/cancel",
        RouteClass::Mutating,
    ),
    (
        "POST /workflows/by-id/{workflow_name}/{workflow_id}/pause",
        RouteClass::Mutating,
    ),
    (
        "POST /workflows/by-id/{workflow_name}/{workflow_id}/resume",
        RouteClass::Mutating,
    ),
    // ── Read-only operator role (issue #776) — routes that were mounted in
    // harvest_api_router but had drifted without a classification entry. Under
    // #776 fail-closed enforcement an unclassified route is treated as
    // Mutating; these are classified explicitly per verified handler behavior
    // so the read-only tier reaches every genuine read (AC1) and never a
    // mutation (AC2). Every "Mutating" handler below was verified to write
    // state; every "ReadOnly" handler was verified not to.
    //
    // ReadOnly — discovery / reads (no state mutation):
    ("GET /batches/pending", RouteClass::ReadOnly),
    ("GET /workflows/registered", RouteClass::ReadOnly),
    (
        "GET /workflows/registered/{name}/schema",
        RouteClass::ReadOnly,
    ),
    (
        "GET /workflows/registered/{name}/interface",
        RouteClass::ReadOnly,
    ),
    // Enumerates a workflow type's query/update handlers (read). Router-only:
    // not in management_api_routes() before #776.
    (
        "GET /workflows/types/{workflow_name}/handlers",
        RouteClass::ReadOnly,
    ),
    // Missing-workflow_id guard: always returns 400 (no read, no write). Both
    // verbs share one handler; classified ReadOnly so a read-only principal
    // receives the intended 400 rather than a 403. Router-only before #776.
    ("GET /workflows/by-id/{workflow_name}", RouteClass::ReadOnly),
    (
        "POST /workflows/by-id/{workflow_name}",
        RouteClass::ReadOnly,
    ),
    ("GET /dead-letters/aggregate", RouteClass::ReadOnly),
    // Circuit-breaker snapshots (read); force-open/close are the mutations below.
    ("GET /admin/circuits", RouteClass::ReadOnly),
    ("GET /admin/circuits/{activity_name}", RouteClass::ReadOnly),
    ("GET /admin/queues/scaling", RouteClass::ReadOnly),
    // Task-queue pause/resume (issue #619): the hold control for a scoped
    // downstream outage. GET is read-only; the two mutations are admin-gated.
    ("GET /admin/queues/paused", RouteClass::ReadOnly),
    (
        "POST /admin/queues/{queue_name}/pause",
        RouteClass::Mutating,
    ),
    (
        "POST /admin/queues/{queue_name}/resume",
        RouteClass::Mutating,
    ),
    ("GET /admin/metrics", RouteClass::ReadOnly),
    ("GET /admin/completion-triggers", RouteClass::ReadOnly),
    ("GET /admin/schedules/{id}", RouteClass::ReadOnly),
    ("GET /admin/schedules/{id}/runs", RouteClass::ReadOnly),
    ("GET /admin/schedules/decisions", RouteClass::ReadOnly),
    ("GET /admin/schedules/{id}/decisions", RouteClass::ReadOnly),
    ("GET /admin/schedules/{id}/preview", RouteClass::ReadOnly),
    // Preview a candidate schedule: POST carries the candidate in the body but
    // only validates + projects fire times; no state is written.
    ("POST /admin/schedules/preview", RouteClass::ReadOnly),
    ("GET /calendars", RouteClass::ReadOnly),
    ("GET /calendars/{name}", RouteClass::ReadOnly),
    ("GET /workers/{worker_id}/pinned", RouteClass::ReadOnly),
    (
        "GET /admin/queues/{queue_name}/eligibility",
        RouteClass::ReadOnly,
    ),
    ("GET /admin/tasks/{id}/eligibility", RouteClass::ReadOnly),
    // Mutating — these write state. A read-only principal is denied (403).
    // Update-with-start admits an update and may start a fresh run.
    (
        "POST /workflows/{workflow_name}/update-with-start",
        RouteClass::Mutating,
    ),
    // DAG retry-from-failed-node (reset internals).
    (
        "POST /dags/{dag_name}/runs/{run_exec_id}/retry",
        RouteClass::Mutating,
    ),
    // Operator force-open / force-close of a circuit breaker.
    (
        "POST /admin/circuits/{activity_name}/force-open",
        RouteClass::Mutating,
    ),
    (
        "POST /admin/circuits/{activity_name}/force-close",
        RouteClass::Mutating,
    ),
    // Rewinds a shard's audit-export cursor (issue #953): mutating, audited.
    ("POST /admin/audit-export/redrive", RouteClass::Mutating),
    // Retires or reactivates a shard's audit-export cursor (issue #1273):
    // mutating, audited.
    ("POST /admin/audit-export/decommission", RouteClass::Mutating),
    ("POST /admin/audit-export/reactivate", RouteClass::Mutating),
    // Calendar + completion-trigger CRUD. No dedicated audit op constant yet
    // (audit wiring is out of scope for #776); disposition is EXCLUDED_ROUTES.
    ("POST /admin/completion-triggers", RouteClass::Mutating),
    ("POST /calendars", RouteClass::Mutating),
    ("PUT /calendars/{name}", RouteClass::Mutating),
    ("DELETE /calendars/{name}", RouteClass::Mutating),
];

// ── Declarative route manifest ────────────────────────────────────────────────

/// Every operation name covered by the audit trail.
///
/// The coverage guard test (`audit_coverage_all_mutation_routes_declared`)
/// verifies that every entry in [`ALL_MUTATION_ROUTES`] that declares an
/// operation references a name in this slice.
pub const AUDITED_OPERATIONS: &[&str] = &[
    OP_WORKFLOW_START,
    OP_WORKFLOW_SIGNAL,
    OP_WORKFLOW_SIGNAL_WITH_START,
    OP_WORKFLOW_CANCEL,
    OP_WORKFLOW_TERMINATE,
    OP_WORKFLOW_PAUSE,
    OP_WORKFLOW_RESUME,
    OP_WORKFLOW_ANNOTATE,
    OP_WORKFLOW_RESET,
    OP_WORKFLOW_RERUN,
    OP_DAG_TRIGGER,
    OP_DAG_PATCH,
    OP_SCHEDULE_CREATE,
    OP_SCHEDULE_UPDATE,
    OP_SCHEDULE_PAUSE,
    OP_SCHEDULE_RESUME,
    OP_SCHEDULE_DELETE,
    OP_SCHEDULE_BACKFILL,
    OP_SCHEDULE_TRIGGER,
    OP_DLQ_REPLAY,
    OP_DLQ_REPLAY_BULK,
    OP_DLQ_DISCARD_BULK,
    OP_DLQ_REDRIVE,
    OP_BATCH_SUBMIT,
    OP_BATCH_START,
    OP_RETENTION_RUN_NOW,
    OP_EXTERNAL_ACTIVITY_COMPLETE,
    OP_EXTERNAL_ACTIVITY_FAIL,
    OP_WORKER_DRAIN,
    OP_RATE_LIMIT_OVERRIDE,
    // TTL'd runtime pacing overrides for rate limits and start-throttles
    // (issue #945)
    OP_RATE_LIMIT_PACING_OVERRIDE_SET,
    OP_RATE_LIMIT_PACING_OVERRIDE_CLEAR,
    OP_START_THROTTLE_PACING_OVERRIDE_SET,
    OP_START_THROTTLE_PACING_OVERRIDE_CLEAR,
    // Audit-export cursor redrive (issue #953), decommission and reactivate
    // (issue #1273)
    OP_AUDIT_EXPORT_REDRIVE,
    OP_AUDIT_EXPORT_DECOMMISSION,
    OP_AUDIT_EXPORT_REACTIVATE,
    OP_BUILD_POLICY_SET,
    OP_BUILD_COMPAT_DECLARE,
    OP_BUILD_COMPAT_REVOKE,
    // Admission gates (issue #377)
    OP_GATE_CREATE,
    OP_GATE_LIFT,
    // Task queue management (issue #249)
    OP_TASK_REPRIORITIZE,
    // PII erasure (issue #495)
    OP_WORKFLOW_ERASE_PAYLOADS,
    // Per-execution legal hold (issue #747)
    OP_LEGAL_HOLD_SET,
    OP_LEGAL_HOLD_RELEASE,
    OP_WORKFLOW_REPLAY_CANARY,
    // Force-retry backing-off activity (issue #516)
    OP_ACTIVITY_RETRY_NOW,
    // Force-fail hung in-flight activity (issue #765)
    OP_ACTIVITY_FAIL_NOW,
    // Per-activity-type pause/resume (issue #807)
    OP_ACTIVITY_PAUSE,
    OP_ACTIVITY_RESUME,
    // Batch reset by semantic point (issue #538)
    OP_BATCH_RESET,
    // Percentage build ramp (issue #604)
    OP_BUILD_RAMP_SET,
    OP_BUILD_RAMP_CLEAR,
    // Completion-callback delivery redrive (issue #605)
    OP_CALLBACK_REDRIVE,
    // Inbound webhook receiver (issue #344). No ALL_MUTATION_ROUTES entry --
    // webhook paths are user-defined, app-level routes; see the doc comment
    // on OP_WEBHOOK_TRIGGER.
    OP_WEBHOOK_TRIGGER,
    // Operator read-path payload decoding (issue #608). Read audit — no
    // ALL_MUTATION_ROUTES entry; see the doc comment on OP_PAYLOAD_DECODE_READ.
    OP_PAYLOAD_DECODE_READ,
    // Read-only operator role (issue #776): these four constants pre-existed
    // but had never been wired into a route manifest entry. Their handlers
    // already write audit rows under these ops; classifying the routes (below)
    // requires an ALL_MUTATION_ROUTES entry, which in turn requires the op be
    // registered here.
    OP_WORKFLOW_UPDATE_WITH_START,
    OP_DAG_RETRY,
    OP_CIRCUIT_FORCE_OPEN,
    OP_CIRCUIT_FORCE_CLOSE,
    // Scoped API tokens (issue #942)
    // Task-queue pause/resume (issue #619)
    OP_QUEUE_PAUSE,
    OP_QUEUE_RESUME,
    OP_TOKEN_CREATE,
    OP_TOKEN_REVOKE,
];

/// Routes explicitly excluded from audit.
///
/// This slice documents the intentional exclusions. It is consumed by the
/// coverage guard test to ensure no route is accidentally omitted from either
/// [`ALL_MUTATION_ROUTES`] or this exclusion list.
pub const EXCLUDED_ROUTES: &[&str] = &[
    "GET /admin/audit-export",
    "GET /workflows/count",
    "GET /workflows/summaries",
    "GET /workflows",
    "GET /workflows/{id}",
    "GET /workflows/{id}/children",
    "GET /workflows/{id}/tree",
    "GET /workflows/{id}/stack",
    "GET /workflows/{id}/timeline",
    "GET /workflows/{id}/logs",
    "GET /workflows/{id}/awaitables",
    "GET /workflows/{id}/diagnose",
    "GET /workflows/{id}/run-chain",
    "POST /workflows/{id}/replay-diagnosis",
    "GET /workflows/{id}/query/{query_name}",
    "POST /workflows/{id}/query/{query_name}",
    "GET /workflows/{id}/queries",
    "GET /workflows/{id}/update/{update_id}/result",
    // Updates are synchronous request/response, not tracked as operator
    // audit events in this slice; they appear in the workflow event history.
    "POST /workflows/{id}/update/{update_name}",
    "GET /workflows/{id}/history",
    "GET /workflows/{id}/history/export",
    "GET /workflows/{id}/completion-deliveries",
    "GET /dags",
    "GET /dags/{dag_name}/runs",
    "GET /dags/{dag_name}/runs/{run_exec_id}",
    "GET /dead-letters",
    "GET /health",
    "GET /openapi.json",
    "GET /admin/preflight",
    "GET /admin/shards/health",
    "GET /admin/queue-coverage",
    "GET /admin/status",
    "GET /admin/config",
    "GET /admin/canary",
    "GET /admin/version-gates/usage",
    "GET /admin/version-gates/retirement-check",
    "GET /admin/retention",
    "GET /admin/concurrency",
    "GET /admin/usage",
    "GET /admin/debounce",
    "GET /admin/start-throttle",
    // Per-tenant resource quota usage-vs-limit report (issue #946): read-only.
    "GET /admin/quotas",
    // Payload-codec key rotation progress (issue #948): read-only, and it
    // surfaces only key IDENTIFIERS and row counts — no key material, no
    // payload content — so there is nothing here to audit that the admin gate
    // does not already cover.
    "GET /admin/codec/rotation",
    // Read-only pacing-override lookup (issue #945); the SET/DELETE mutations
    // above (OP_START_THROTTLE_PACING_OVERRIDE_SET/CLEAR) are audited.
    "GET /admin/start-throttle/{workflow_name}/override",
    "GET /admin/history/exports",
    "GET /admin/history/export-sample",
    "GET /admin/external-handoffs",
    "GET /admin/external-handoffs/{token}",
    "GET /admin/schedules",
    "GET /admin/rate-limits",
    // Heartbeats are high-volume liveness pings, not operator mutations.
    "POST /activities/external/{token}/heartbeat",
    // Activity-pause catalogue reads (issue #807) — diagnostic, no audit trail.
    "GET /activities",
    "GET /activities/{activity_name}",
    "GET /workers",
    "GET /workers/health",
    "GET /workers/{worker_id}",
    "GET /workers/drain-preview",
    "GET /batch-operations",
    "GET /batch-operations/{id}",
    // The audit list endpoint itself is read-only.
    "GET /admin/audit",
    // SSE stream is read-only; stream open/close are audited manually in the handler.
    "GET /executions/{exec_id}/events/stream",
    // Ephemeral progress stream (issue #791): read-only, no audit trail
    // (disposable side channel; no open/close audit rows are written).
    "GET /workflows/{id}/stream",
    // Build routing reads and the retire safety check never write audit rows.
    "GET /admin/build-routing",
    "GET /admin/build-routing/compat",
    "POST /admin/build-routing/retire",
    // Admission gate list is read-only.
    "GET /admin/gates",
    // Scoped API token list is read-only (issue #942).
    "GET /admin/tokens",
    // Business-id ("latest run") read-only variants (issue #805). Each mirrors
    // its exec-id counterpart's EXCLUDED_ROUTES membership exactly. Note the
    // `.../result` variant is deliberately absent — its exec-id counterpart
    // (`GET /workflows/{id}/result`) is classified ReadOnly and appears in
    // ALL_MUTATION_ROUTES with `None` (both mean "not audited") but is NOT in
    // EXCLUDED_ROUTES, so the by-id variant must not be either.
    "GET /workflows/by-id/{workflow_name}/{workflow_id}",
    // Empty-workflow_id guard (issue #1353): always 400, no read, no write.
    "GET /workflows/by-id/{workflow_name}/",
    "GET /workflows/by-id/{workflow_name}/{workflow_id}/stack",
    "GET /workflows/by-id/{workflow_name}/{workflow_id}/children",
    "GET /workflows/by-id/{workflow_name}/{workflow_id}/query/{query_name}",
    // POST query is read-only (typed args, no state mutation).
    "POST /workflows/by-id/{workflow_name}/{workflow_id}/query/{query_name}",
    // ── Read-only operator role (issue #776) ──────────────────────────────────
    // ReadOnly routes (no audit trail — reads):
    "GET /batches/pending",
    "GET /workflows/registered",
    "GET /workflows/registered/{name}/schema",
    "GET /workflows/registered/{name}/interface",
    "GET /workflows/types/{workflow_name}/handlers",
    "GET /workflows/by-id/{workflow_name}",
    "POST /workflows/by-id/{workflow_name}",
    "GET /dead-letters/aggregate",
    "GET /admin/circuits",
    "GET /admin/circuits/{activity_name}",
    "GET /admin/queues/scaling",
    // Paused-queue list is read-only.
    "GET /admin/queues/paused",
    "GET /admin/metrics",
    "GET /admin/completion-triggers",
    "GET /admin/schedules/{id}",
    "GET /admin/schedules/{id}/runs",
    "GET /admin/schedules/decisions",
    "GET /admin/schedules/{id}/decisions",
    "GET /admin/schedules/{id}/preview",
    "POST /admin/schedules/preview",
    "GET /calendars",
    "GET /calendars/{name}",
    "GET /workers/{worker_id}/pinned",
    "GET /admin/queues/{queue_name}/eligibility",
    "GET /admin/tasks/{id}/eligibility",
    // Mutating routes with no dedicated audit op constant (calendar +
    // completion-trigger CRUD). Audit wiring for these is out of scope for
    // #776; their audit disposition is "explicitly excluded" so the
    // mutating_routes_are_audited_or_explicitly_excluded guard passes.
    "POST /admin/completion-triggers",
    "POST /calendars",
    "PUT /calendars/{name}",
    "DELETE /calendars/{name}",
];

/// Declarative manifest of every route in `harvest_api_router`.
///
/// Each entry is `(route_template, Option<operation_name>)`:
/// - `Some(op)` — this route is audited under the named operation.
/// - `None` — this route is explicitly excluded from audit.
///
/// **When you add a new route to `harvest_api_router`, you MUST add an entry
/// here.** The coverage guard test (`audit_coverage_all_mutation_routes_declared`)
/// will fail if any expected mutation route is missing or declared as excluded.
pub const ALL_MUTATION_ROUTES: &[(&str, Option<&str>)] = &[
    // Workflow management
    ("GET /workflows/count", None),
    ("GET /workflows/summaries", None),
    ("GET /workflows", None),
    ("GET /workflows/{id}", None),
    ("GET /workflows/{id}/children", None),
    // Issue #621: read-only, no audit operation.
    ("GET /workflows/{id}/tree", None),
    ("GET /workflows/{id}/stack", None),
    ("GET /workflows/{id}/timeline", None),
    ("GET /workflows/{id}/logs", None),
    // Issue #615: read-only, no audit operation.
    ("GET /workflows/{id}/awaitables", None),
    // Issue #809: read-only, no audit operation.
    ("GET /workflows/{id}/diagnose", None),
    ("GET /workflows/{id}/run-chain", None),
    // Issue #614: read-only, no audit operation.
    ("POST /workflows/{id}/replay-diagnosis", None),
    (
        "POST /workflows/{workflow_name}/start",
        Some(OP_WORKFLOW_START),
    ),
    (
        "POST /workflows/{workflow_name}/signal-with-start",
        Some(OP_WORKFLOW_SIGNAL_WITH_START),
    ),
    ("POST /workflows/{id}/cancel", Some(OP_WORKFLOW_CANCEL)),
    (
        "POST /workflows/{id}/terminate",
        Some(OP_WORKFLOW_TERMINATE),
    ),
    ("POST /workflows/{id}/rerun", Some(OP_WORKFLOW_RERUN)),
    ("POST /workflows/{id}/pause", Some(OP_WORKFLOW_PAUSE)),
    ("POST /workflows/{id}/resume", Some(OP_WORKFLOW_RESUME)),
    ("PATCH /workflows/{id}/triage", Some(OP_WORKFLOW_ANNOTATE)),
    ("POST /workflows/{id}/reset", Some(OP_WORKFLOW_RESET)),
    (
        "POST /workflows/{id}/signal/{signal_name}",
        Some(OP_WORKFLOW_SIGNAL),
    ),
    ("GET /workflows/{id}/query/{query_name}", None),
    ("POST /workflows/{id}/query/{query_name}", None),
    ("GET /workflows/{id}/queries", None),
    ("POST /workflows/{id}/update/{update_name}", None),
    ("GET /workflows/{id}/update/{update_id}/result", None),
    ("GET /workflows/{id}/result", None),
    ("GET /workflows/{id}/history", None),
    ("GET /workflows/{id}/history/export", None),
    ("GET /workflows/{id}/completion-deliveries", None),
    (
        "POST /workflows/{id}/completion-deliveries/{delivery_id}/redrive",
        Some(OP_CALLBACK_REDRIVE),
    ),
    // DAG management
    ("GET /dags", None),
    ("GET /dags/{dag_name}/runs", None),
    ("GET /dags/{dag_name}/runs/{run_exec_id}", None),
    ("POST /dags/{dag_name}/trigger", Some(OP_DAG_TRIGGER)),
    ("PATCH /dags/{dag_name}", Some(OP_DAG_PATCH)),
    // Dead-letter queue
    ("GET /dead-letters", None),
    ("POST /dead-letters/replay", Some(OP_DLQ_REPLAY_BULK)),
    ("POST /dead-letters/discard", Some(OP_DLQ_DISCARD_BULK)),
    ("POST /dead-letters/{id}/replay", Some(OP_DLQ_REPLAY)),
    ("POST /dlq/redrive", Some(OP_DLQ_REDRIVE)),
    // Health / observability (read-only)
    ("GET /health", None),
    ("GET /openapi.json", None),
    ("GET /admin/preflight", None),
    ("GET /admin/shards/health", None),
    ("GET /admin/queue-coverage", None),
    ("GET /admin/status", None),
    ("GET /admin/config", None),
    ("GET /admin/canary", None),
    ("GET /admin/version-gates/usage", None),
    ("GET /admin/version-gates/retirement-check", None),
    ("GET /admin/retention", None),
    ("POST /admin/retention/run-now", Some(OP_RETENTION_RUN_NOW)),
    ("GET /admin/concurrency", None),
    ("GET /admin/usage", None),
    ("GET /admin/debounce", None),
    ("GET /admin/start-throttle", None),
    // Per-tenant resource quota usage-vs-limit report (issue #946): read-only.
    ("GET /admin/quotas", None),
    // Issue #948: read-only, no audit operation.
    ("GET /admin/codec/rotation", None),
    // Workflow-type handler reachability (issue #520): read-only.
    ("GET /admin/workflow-types/reachability", None),
    ("GET /admin/history/exports", None),
    ("GET /admin/history/export-sample", None),
    ("GET /admin/external-handoffs", None),
    ("GET /admin/external-handoffs/{token}", None),
    // Schedule management
    ("GET /admin/schedules", None),
    ("GET /admin/rate-limits", None),
    ("POST /admin/schedules/workflow", Some(OP_SCHEDULE_CREATE)),
    ("PATCH /admin/schedules/{id}", Some(OP_SCHEDULE_UPDATE)),
    ("POST /admin/schedules/{id}/pause", Some(OP_SCHEDULE_PAUSE)),
    (
        "POST /admin/schedules/{id}/resume",
        Some(OP_SCHEDULE_RESUME),
    ),
    ("DELETE /admin/schedules/{id}", Some(OP_SCHEDULE_DELETE)),
    (
        "POST /admin/schedules/{id}/backfill",
        Some(OP_SCHEDULE_BACKFILL),
    ),
    (
        "POST /admin/schedules/{id}/trigger",
        Some(OP_SCHEDULE_TRIGGER),
    ),
    // External activity completion
    (
        "POST /activities/external/{token}/complete",
        Some(OP_EXTERNAL_ACTIVITY_COMPLETE),
    ),
    (
        "POST /activities/external/{token}/fail",
        Some(OP_EXTERNAL_ACTIVITY_FAIL),
    ),
    ("POST /activities/external/{token}/heartbeat", None),
    // Per-activity-type pause/resume (issue #807)
    ("GET /activities", None),
    ("GET /activities/{activity_name}", None),
    (
        "POST /activities/{activity_name}/pause",
        Some(OP_ACTIVITY_PAUSE),
    ),
    (
        "POST /activities/{activity_name}/resume",
        Some(OP_ACTIVITY_RESUME),
    ),
    // Worker fleet
    ("GET /workers/health", None),
    ("GET /workers", None),
    ("GET /workers/{worker_id}", None),
    ("GET /workers/drain-preview", None),
    ("POST /workers/{worker_id}/drain", Some(OP_WORKER_DRAIN)),
    (
        "POST /admin/rate-limits/{key}",
        Some(OP_RATE_LIMIT_OVERRIDE),
    ),
    // TTL'd runtime pacing overrides (issue #945)
    (
        "POST /admin/rate-limits/{activity_name}/override",
        Some(OP_RATE_LIMIT_PACING_OVERRIDE_SET),
    ),
    (
        "DELETE /admin/rate-limits/{activity_name}/override",
        Some(OP_RATE_LIMIT_PACING_OVERRIDE_CLEAR),
    ),
    (
        "POST /admin/start-throttle/{workflow_name}/override",
        Some(OP_START_THROTTLE_PACING_OVERRIDE_SET),
    ),
    (
        "DELETE /admin/start-throttle/{workflow_name}/override",
        Some(OP_START_THROTTLE_PACING_OVERRIDE_CLEAR),
    ),
    ("GET /admin/start-throttle/{workflow_name}/override", None),
    // Batch operations
    ("GET /batch-operations", None),
    ("POST /batch-operations", Some(OP_BATCH_SUBMIT)),
    ("GET /batch-operations/{id}", None),
    // Batch workflow start (issue #357)
    ("POST /workflows/batch_start", Some(OP_BATCH_START)),
    // Audit log (read-only)
    ("GET /admin/audit", None),
    // SSE execution event stream (issue #324): read-only; open/close audited in handler.
    ("GET /executions/{exec_id}/events/stream", None),
    // Ephemeral progress stream (issue #791): read-only, not audited.
    ("GET /workflows/{id}/stream", None),
    // Build routing management (issue #362)
    ("GET /admin/build-routing", None),
    (
        "POST /admin/build-routing/policies",
        Some(OP_BUILD_POLICY_SET),
    ),
    ("GET /admin/build-routing/compat", None),
    (
        "POST /admin/build-routing/compat",
        Some(OP_BUILD_COMPAT_DECLARE),
    ),
    (
        "DELETE /admin/build-routing/compat/{build_id}/{compat_with}",
        Some(OP_BUILD_COMPAT_REVOKE),
    ),
    // retire is a read-only safety check — no state is mutated.
    ("POST /admin/build-routing/retire", None),
    // Percentage build ramp (issue #604)
    ("POST /admin/build-routing/ramp", Some(OP_BUILD_RAMP_SET)),
    (
        "DELETE /admin/build-routing/ramp/{queue_name}",
        Some(OP_BUILD_RAMP_CLEAR),
    ),
    // Admission gates (issue #377)
    ("GET /admin/gates", None),
    ("POST /admin/gates", Some(OP_GATE_CREATE)),
    ("DELETE /admin/gates/{id}", Some(OP_GATE_LIFT)),
    // Scoped API tokens (issue #942)
    ("GET /admin/tokens", None),
    ("POST /admin/tokens", Some(OP_TOKEN_CREATE)),
    ("DELETE /admin/tokens/{id}", Some(OP_TOKEN_REVOKE)),
    // Task queue management (issue #249)
    ("PATCH /tasks/{id}", Some(OP_TASK_REPRIORITIZE)),
    // PII erasure (issue #495)
    (
        "POST /workflows/{id}/erase-payloads",
        Some(OP_WORKFLOW_ERASE_PAYLOADS),
    ),
    // Per-execution legal hold (issue #747)
    ("POST /workflows/{id}/legal-hold", Some(OP_LEGAL_HOLD_SET)),
    (
        "POST /workflows/{id}/legal-hold/release",
        Some(OP_LEGAL_HOLD_RELEASE),
    ),
    // Replay canary (issue #512)
    (
        "POST /admin/workflows/replay-canary",
        Some(OP_WORKFLOW_REPLAY_CANARY),
    ),
    // Force-retry backing-off activity (issue #516)
    (
        "POST /workflows/{id}/activities/{activity_exec_id}/retry-now",
        Some(OP_ACTIVITY_RETRY_NOW),
    ),
    // Force-fail hung in-flight activity (issue #765)
    (
        "POST /workflows/{id}/activities/{activity_exec_id}/fail-now",
        Some(OP_ACTIVITY_FAIL_NOW),
    ),
    // Batch reset by semantic point (issue #538)
    ("POST /workflows/batch_reset", Some(OP_BATCH_RESET)),
    // ── Business-id ("latest run") route variants (issue #805) ────────────────
    // Audit operations mirror the exec-id counterparts (the by-id handlers
    // delegate to them; the delegated handler writes the audit row under the
    // exec-id route string with the resolved exec_id as the target).
    ("GET /workflows/by-id/{workflow_name}/{workflow_id}", None),
    // Empty-workflow_id guard (issue #1353): always 400, never audited.
    ("GET /workflows/by-id/{workflow_name}/", None),
    (
        "GET /workflows/by-id/{workflow_name}/{workflow_id}/result",
        None,
    ),
    (
        "GET /workflows/by-id/{workflow_name}/{workflow_id}/stack",
        None,
    ),
    (
        "GET /workflows/by-id/{workflow_name}/{workflow_id}/children",
        None,
    ),
    (
        "GET /workflows/by-id/{workflow_name}/{workflow_id}/query/{query_name}",
        None,
    ),
    (
        "POST /workflows/by-id/{workflow_name}/{workflow_id}/query/{query_name}",
        None,
    ),
    (
        "POST /workflows/by-id/{workflow_name}/{workflow_id}/signal/{signal_name}",
        Some(OP_WORKFLOW_SIGNAL),
    ),
    (
        "POST /workflows/by-id/{workflow_name}/{workflow_id}/cancel",
        Some(OP_WORKFLOW_CANCEL),
    ),
    (
        "POST /workflows/by-id/{workflow_name}/{workflow_id}/pause",
        Some(OP_WORKFLOW_PAUSE),
    ),
    (
        "POST /workflows/by-id/{workflow_name}/{workflow_id}/resume",
        Some(OP_WORKFLOW_RESUME),
    ),
    // ── Read-only operator role (issue #776) — classification manifest ────────
    // ReadOnly routes (None = not audited):
    ("GET /batches/pending", None),
    ("GET /workflows/registered", None),
    ("GET /workflows/registered/{name}/schema", None),
    ("GET /workflows/registered/{name}/interface", None),
    ("GET /workflows/types/{workflow_name}/handlers", None),
    ("GET /workflows/by-id/{workflow_name}", None),
    ("POST /workflows/by-id/{workflow_name}", None),
    ("GET /dead-letters/aggregate", None),
    ("GET /admin/circuits", None),
    ("GET /admin/circuits/{activity_name}", None),
    ("GET /admin/queues/scaling", None),
    ("GET /admin/queues/paused", None),
    (
        "POST /admin/queues/{queue_name}/pause",
        Some(OP_QUEUE_PAUSE),
    ),
    (
        "POST /admin/queues/{queue_name}/resume",
        Some(OP_QUEUE_RESUME),
    ),
    ("GET /admin/metrics", None),
    ("GET /admin/completion-triggers", None),
    ("GET /admin/schedules/{id}", None),
    ("GET /admin/schedules/{id}/runs", None),
    ("GET /admin/schedules/decisions", None),
    ("GET /admin/schedules/{id}/decisions", None),
    ("GET /admin/schedules/{id}/preview", None),
    ("POST /admin/schedules/preview", None),
    ("GET /calendars", None),
    ("GET /calendars/{name}", None),
    ("GET /workers/{worker_id}/pinned", None),
    ("GET /admin/queues/{queue_name}/eligibility", None),
    ("GET /admin/tasks/{id}/eligibility", None),
    // Mutating routes — the four with pre-existing op constants audit under
    // them (their handlers already write the audit row); the calendar +
    // completion-trigger CRUD have no op and are excluded (None).
    (
        "POST /workflows/{workflow_name}/update-with-start",
        Some(OP_WORKFLOW_UPDATE_WITH_START),
    ),
    (
        "POST /dags/{dag_name}/runs/{run_exec_id}/retry",
        Some(OP_DAG_RETRY),
    ),
    (
        "POST /admin/circuits/{activity_name}/force-open",
        Some(OP_CIRCUIT_FORCE_OPEN),
    ),
    (
        "POST /admin/circuits/{activity_name}/force-close",
        Some(OP_CIRCUIT_FORCE_CLOSE),
    ),
    // Audit export (issue #953): read-only status, and the audited redrive.
    // Decommission and reactivate (issue #1273) are audited the same way.
    ("GET /admin/audit-export", None),
    (
        "POST /admin/audit-export/redrive",
        Some(OP_AUDIT_EXPORT_REDRIVE),
    ),
    (
        "POST /admin/audit-export/decommission",
        Some(OP_AUDIT_EXPORT_DECOMMISSION),
    ),
    (
        "POST /admin/audit-export/reactivate",
        Some(OP_AUDIT_EXPORT_REACTIVATE),
    ),
    ("POST /admin/completion-triggers", None),
    ("POST /calendars", None),
    ("PUT /calendars/{name}", None),
    ("DELETE /calendars/{name}", None),
];

// ── Query filters ─────────────────────────────────────────────────────────────

/// Filters for `list_audit`.
#[derive(Debug, Clone)]
pub struct AuditFilters {
    pub actor: Option<String>,
    pub operation: Option<String>,
    pub target_type: Option<String>,
    pub target_id: Option<String>,
    pub status: Option<String>,
    pub since: Option<DateTime<Utc>>,
    pub before: Option<DateTime<Utc>>,
    /// Maximum number of records to return. Clamped to [1, 500].
    pub limit: i64,
}

impl AuditFilters {
    #[must_use]
    pub const fn default_limit() -> i64 {
        50
    }
}

impl Default for AuditFilters {
    fn default() -> Self {
        Self {
            actor: None,
            operation: None,
            target_type: None,
            target_id: None,
            status: None,
            since: None,
            before: None,
            limit: Self::default_limit(),
        }
    }
}

// ── Database operations ───────────────────────────────────────────────────────

/// Insert a single audit record. Returns the generated audit row id.
///
/// Called after every covered management mutation. For successful mutations,
/// the caller **must** ensure this returns `Ok` before reporting success to
/// the HTTP client — the audit record must be durable before the response is
/// sent.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] if the insert fails.
pub async fn insert_audit(
    conn: &mut AsyncPgConnection,
    record: &NewAuditRecord<'_>,
) -> HarvestResult<Uuid> {
    diesel::insert_into(harvest_audit_log::table)
        .values(record)
        .returning(harvest_audit_log::id)
        .get_result::<Uuid>(conn)
        .await
        .map_err(database_error)
}

/// `PostgreSQL`'s wire protocol caps a single statement at 65 535 bound
/// parameters. `NewAuditRecord` has 11 columns, so a chunk of this many
/// rows binds at most 54 989 parameters. That stays comfortably under the
/// limit, even when every column binds a value rather than falling back to
/// its SQL `DEFAULT`. Issue #1407 review: a batch at or beyond the raw
/// limit used to silently drop every audit row for its shard. Callers
/// discard this function's error, so nothing surfaced the drop.
const MAX_AUDIT_BATCH_ROWS: usize = 4999;

/// Insert several audit records in one round trip per chunk of at most
/// [`MAX_AUDIT_BATCH_ROWS`] records. Returns the generated ids in the same
/// order as `records`.
///
/// An empty slice never sends a statement. An empty `VALUES` list has no
/// `Insertable` representation. This returns `Ok(vec![])` without touching
/// the connection.
///
/// Same durability contract as [`insert_audit`]: the caller must ensure this
/// returns `Ok` before reporting success for every mutation it covers.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] if any chunk's insert
/// fails. A failure partway through leaves earlier chunks committed. Each
/// chunk is its own statement, matching the per-row loop this replaces,
/// which offered no cross-row atomicity either.
pub async fn insert_audit_batch(
    conn: &mut AsyncPgConnection,
    records: &[NewAuditRecord<'_>],
) -> HarvestResult<Vec<Uuid>> {
    let mut ids = Vec::with_capacity(records.len());
    for chunk in records.chunks(MAX_AUDIT_BATCH_ROWS) {
        let chunk_ids = diesel::insert_into(harvest_audit_log::table)
            .values(chunk)
            .returning(harvest_audit_log::id)
            .get_results::<Uuid>(conn)
            .await
            .map_err(database_error)?;
        ids.extend(chunk_ids);
    }
    Ok(ids)
}

/// List audit records matching the given filters, ordered by `occurred_at DESC`.
///
/// The `limit` in `filters` is clamped to [1, 500]. The caller is responsible
/// for merging and re-sorting results when aggregating across multiple shards.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] if the query fails.
pub async fn list_audit(
    conn: &mut AsyncPgConnection,
    filters: &AuditFilters,
) -> HarvestResult<Vec<AuditRecord>> {
    let limit = filters.limit.clamp(1, 500);

    let mut query = harvest_audit_log::table
        .into_boxed()
        .order(harvest_audit_log::occurred_at.desc())
        .limit(limit);

    if let Some(actor) = &filters.actor {
        query = query.filter(harvest_audit_log::actor.eq(actor.clone()));
    }
    if let Some(op) = &filters.operation {
        query = query.filter(harvest_audit_log::operation.eq(op.clone()));
    }
    if let Some(tt) = &filters.target_type {
        query = query.filter(harvest_audit_log::target_type.eq(tt.clone()));
    }
    if let Some(tid) = &filters.target_id {
        query = query.filter(harvest_audit_log::target_id.eq(tid.clone()));
    }
    if let Some(status) = &filters.status {
        query = query.filter(harvest_audit_log::status.eq(status.clone()));
    }
    if let Some(since) = filters.since {
        query = query.filter(harvest_audit_log::occurred_at.ge(since));
    }
    if let Some(before) = filters.before {
        query = query.filter(harvest_audit_log::occurred_at.lt(before));
    }

    query
        .select(AuditRecord::as_select())
        .load(conn)
        .await
        .map_err(database_error)
}

/// Delete audit records older than `retention_days` days.
///
/// Called by the retention subsystem on its configured cadence. Returns the
/// number of rows deleted.
///
/// **Never deletes a record the audit exporter has not shipped** (issue #953).
/// A retention sweep that removed an unexported row would be a silent
/// compliance gap — the record would be gone from the database *and* absent
/// from the SIEM, with nothing anywhere to show it was lost, which is exactly
/// what "zero silent record loss by construction" rules out.
///
/// ## Deciding whether export is live
///
/// The guard applies when **either** signal says an exporter owes this shard
/// records:
///
/// - **A live (non-retired) cursor row exists** for the shard. Durable, shared
///   state, so it works
///   when retention and export run in **different processes** (issue #953,
///   Codex review P1): a split web/worker deployment where only the worker
///   configures the sink would otherwise have the web app's retention sweep
///   delete rows the worker still owes.
/// - **A sink is configured in this process**
///   ([`crate::audit_export::is_configured`]) — covers the window before the
///   exporter's first tick on a shard has created the cursor row at all
///   (freshly enabled, newly added to the fleet, or a shard whose pool has
///   been failing).
///
/// Deliberately **not** time-based. An earlier revision expired the guard 24h
/// after the exporter's last heartbeat, so a long worker outage lifted it; a
/// timeout cannot distinguish "export was intentionally removed" from "the
/// worker has been down since Friday", and it resolves that ambiguity by
/// deleting audit records during exactly the outage where they matter most
/// (issue #953, Codex review round 3 P1).
///
/// Retiring export is therefore an explicit operator action —
/// [`crate::audit_export::decommission_cursor`] — not an inferred one. The
/// cost is that a sink left down indefinitely lets the audit table grow past
/// its retention window. That is the deliberate trade, and the growth is
/// bounded by the genuine unexported backlog rather than the whole table:
/// fully-acknowledged records are purged on the normal schedule. Dropping a
/// privileged-action log to reclaim disk is not a choice this function gets to
/// make on an operator's behalf; `harvest.audit.export_lag` and the
/// `last_error` on `GET /admin/audit-export` are how the condition is
/// surfaced. See `docs/audit-export.md`.
///
/// With export inactive by both signals the guard is skipped entirely and the
/// delete is byte-identical to the pre-#953 statement.
///
/// The pending-check subquery is uncorrelated by shard because a shard's
/// database holds at most one cursor row (the exporter provisions only the
/// shard it is scanning). Should two ever coexist, `EXISTS` errs toward
/// retaining, never deleting.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] if the delete fails.
pub async fn purge_old_audit_records(
    conn: &mut AsyncPgConnection,
    retention_days: i64,
) -> HarvestResult<usize> {
    use diesel_async::RunQueryDsl as _;

    let cutoff = chrono::Utc::now() - chrono::Duration::days(retention_days);

    // Delete an aged row UNLESS export is live AND that row is still pending.
    diesel::sql_query(
        "DELETE FROM harvest_audit_log a \
         WHERE a.occurred_at < $1 \
           AND NOT ( \
                 ( \
                   $2::BOOLEAN \
                   OR EXISTS ( \
                        SELECT 1 FROM harvest_audit_export_cursor \
                        WHERE retired_at IS NULL \
                   ) \
                 ) \
                 AND ( \
                   a.export_seq IS NULL \
                   OR EXISTS ( \
                        SELECT 1 FROM harvest_audit_export_cursor c \
                        WHERE a.export_seq > c.last_acked_seq \
                   ) \
                 ) \
           )",
    )
    .bind::<diesel::sql_types::Timestamptz, _>(cutoff)
    .bind::<diesel::sql_types::Bool, _>(crate::audit_export::is_configured())
    .execute(conn)
    .await
    .map_err(database_error)
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_constants_are_distinct() {
        let mut seen = std::collections::HashSet::new();
        for op in AUDITED_OPERATIONS {
            assert!(seen.insert(*op), "duplicate operation: {op}");
        }
    }

    #[test]
    fn all_routes_in_manifest_reference_known_operations() {
        for (route, declared_op) in ALL_MUTATION_ROUTES {
            if let Some(op) = declared_op {
                assert!(
                    AUDITED_OPERATIONS.contains(op),
                    "route '{route}' declares operation '{op}' which is not in AUDITED_OPERATIONS"
                );
            }
        }
    }

    #[test]
    fn route_manifest_has_no_duplicates() {
        let mut seen = std::collections::HashSet::new();
        for (route, _) in ALL_MUTATION_ROUTES {
            assert!(seen.insert(*route), "duplicate route in manifest: {route}");
        }
    }

    #[test]
    fn audit_filters_default_limit_is_50() {
        assert_eq!(AuditFilters::default_limit(), 50);
    }

    #[test]
    fn audit_filters_default_uses_default_limit_not_zero() {
        // Derived Default would give limit=0, which list_audit clamps to 1.
        // The manual Default implementation must use default_limit() instead.
        assert_eq!(AuditFilters::default().limit, AuditFilters::default_limit());
    }

    #[test]
    fn status_constants_are_correct() {
        assert_eq!(STATUS_SUCCEEDED, "succeeded");
        assert_eq!(STATUS_FAILED, "failed");
    }

    #[test]
    fn source_constants_are_correct() {
        assert_eq!(SOURCE_API, "api");
        assert_eq!(SOURCE_CLI, "cli");
        assert_eq!(SOURCE_UI, "ui");
    }

    #[test]
    fn header_constants_are_lowercase() {
        assert_eq!(HEADER_ACTOR, "x-harvest-actor");
        assert_eq!(HEADER_REQUEST_ID, "x-request-id");
        assert_eq!(HEADER_SOURCE, "x-harvest-source");
    }

    #[test]
    fn default_retention_is_90_days() {
        assert_eq!(DEFAULT_AUDIT_RETENTION_DAYS, 90);
    }

    #[test]
    fn replay_canary_route_is_classified_correctly() {
        let route = "POST /admin/workflows/replay-canary";
        let classification = CLASSIFIED_ROUTES
            .iter()
            .find(|(r, _)| *r == route)
            .map(|(_, c)| *c);
        assert_eq!(
            classification,
            Some(RouteClass::Mutating),
            "Replay canary route must be classified as Mutating"
        );

        let mutation_op = ALL_MUTATION_ROUTES
            .iter()
            .find(|(r, _)| *r == route)
            .and_then(|(_, op)| *op);
        assert_eq!(
            mutation_op,
            Some("workflow.replay_canary"),
            "Replay canary route must have the workflow.replay_canary audit operation"
        );
    }

    // ── Route classification exhaustiveness guards ────────────────────────────
    //
    // These tests enforce that CLASSIFIED_ROUTES and ALL_MUTATION_ROUTES stay
    // in sync. Adding a route to harvest_api_router without classifying it
    // causes one of these tests to fail.

    #[test]
    fn route_classification_covers_all_known_routes() {
        let classified: std::collections::HashSet<&str> =
            CLASSIFIED_ROUTES.iter().map(|(r, _)| *r).collect();

        for (route, _) in ALL_MUTATION_ROUTES {
            assert!(
                classified.contains(route),
                "route '{route}' is in ALL_MUTATION_ROUTES but has no entry in \
                 CLASSIFIED_ROUTES — add it with the correct RouteClass"
            );
        }
    }

    #[test]
    fn legal_hold_routes_are_classified_and_audited() {
        // Per-execution legal hold (issue #747): both routes are admin-only
        // mutations and must be classified + audited. This dedicated test —
        // not just the general exhaustiveness guards, which only cross-check
        // CLASSIFIED_ROUTES and ALL_MUTATION_ROUTES against each other — is what
        // catches a route dropped from BOTH lists.
        for route in [
            "POST /workflows/{id}/legal-hold",
            "POST /workflows/{id}/legal-hold/release",
        ] {
            assert!(
                CLASSIFIED_ROUTES
                    .iter()
                    .any(|(r, c)| *r == route && *c == RouteClass::Mutating),
                "{route} must be classified RouteClass::Mutating in CLASSIFIED_ROUTES (issue #747)"
            );
        }
        assert!(
            ALL_MUTATION_ROUTES
                .iter()
                .any(|(r, op)| *r == "POST /workflows/{id}/legal-hold"
                    && *op == Some(OP_LEGAL_HOLD_SET)),
            "legal-hold set route must map to OP_LEGAL_HOLD_SET (issue #747)"
        );
        assert!(
            ALL_MUTATION_ROUTES
                .iter()
                .any(|(r, op)| *r == "POST /workflows/{id}/legal-hold/release"
                    && *op == Some(OP_LEGAL_HOLD_RELEASE)),
            "legal-hold release route must map to OP_LEGAL_HOLD_RELEASE (issue #747)"
        );
        assert!(AUDITED_OPERATIONS.contains(&OP_LEGAL_HOLD_SET));
        assert!(AUDITED_OPERATIONS.contains(&OP_LEGAL_HOLD_RELEASE));
    }

    #[test]
    fn activity_pause_routes_are_classified_and_audited() {
        // Per-activity-type pause/resume (issue #807). This dedicated test --
        // not just the general exhaustiveness guards, which only cross-check
        // CLASSIFIED_ROUTES and ALL_MUTATION_ROUTES against each OTHER -- is
        // what catches a route dropped from BOTH lists at once.
        //
        // The mutations are a fleet-wide dispatch kill switch for one activity
        // type, so they must be classified Mutating (which is what makes the
        // read-only-operator role, issue #776, deny them) and audited.
        for (route, op) in [
            ("POST /activities/{activity_name}/pause", OP_ACTIVITY_PAUSE),
            (
                "POST /activities/{activity_name}/resume",
                OP_ACTIVITY_RESUME,
            ),
        ] {
            assert!(
                CLASSIFIED_ROUTES
                    .iter()
                    .any(|(r, c)| *r == route && *c == RouteClass::Mutating),
                "{route} must be classified RouteClass::Mutating in CLASSIFIED_ROUTES (issue #807)"
            );
            assert!(
                ALL_MUTATION_ROUTES
                    .iter()
                    .any(|(r, mapped)| *r == route && *mapped == Some(op)),
                "{route} must map to {op} in ALL_MUTATION_ROUTES (issue #807)"
            );
            assert!(
                AUDITED_OPERATIONS.contains(&op),
                "{op} must be registered in AUDITED_OPERATIONS (issue #807)"
            );
        }

        // The two catalogue reads are diagnostics: classified ReadOnly, mapped
        // to no operation, and explicitly audit-excluded.
        for route in ["GET /activities", "GET /activities/{activity_name}"] {
            assert!(
                CLASSIFIED_ROUTES
                    .iter()
                    .any(|(r, c)| *r == route && *c == RouteClass::ReadOnly),
                "{route} must be classified RouteClass::ReadOnly in CLASSIFIED_ROUTES (issue #807)"
            );
            assert!(
                ALL_MUTATION_ROUTES
                    .iter()
                    .any(|(r, op)| *r == route && op.is_none()),
                "{route} must appear in ALL_MUTATION_ROUTES with no audit operation (issue #807)"
            );
            assert!(
                EXCLUDED_ROUTES.contains(&route),
                "{route} must appear in EXCLUDED_ROUTES (read-only, no audit trail; issue #807)"
            );
        }
    }

    #[test]
    fn pacing_override_routes_are_classified_and_audited() {
        // TTL'd runtime pacing overrides for a declared per-activity rate
        // limit and a declared workflow-start throttle (issue #945). This
        // dedicated test -- not just the general exhaustiveness guards, which
        // only cross-check CLASSIFIED_ROUTES and ALL_MUTATION_ROUTES against
        // each OTHER -- is what catches a route dropped from BOTH lists at
        // once.
        //
        // Both the set and the clear are audited under NEW, distinct ops from
        // the legacy `OP_RATE_LIMIT_OVERRIDE` (a permanent baseline write),
        // since a TTL'd override is a fundamentally different, self-expiring
        // operation.
        for (route, op) in [
            (
                "POST /admin/rate-limits/{activity_name}/override",
                OP_RATE_LIMIT_PACING_OVERRIDE_SET,
            ),
            (
                "DELETE /admin/rate-limits/{activity_name}/override",
                OP_RATE_LIMIT_PACING_OVERRIDE_CLEAR,
            ),
            (
                "POST /admin/start-throttle/{workflow_name}/override",
                OP_START_THROTTLE_PACING_OVERRIDE_SET,
            ),
            (
                "DELETE /admin/start-throttle/{workflow_name}/override",
                OP_START_THROTTLE_PACING_OVERRIDE_CLEAR,
            ),
        ] {
            assert!(
                CLASSIFIED_ROUTES
                    .iter()
                    .any(|(r, c)| *r == route && *c == RouteClass::Mutating),
                "{route} must be classified RouteClass::Mutating in CLASSIFIED_ROUTES (issue #945)"
            );
            assert!(
                ALL_MUTATION_ROUTES
                    .iter()
                    .any(|(r, mapped)| *r == route && *mapped == Some(op)),
                "{route} must map to {op} in ALL_MUTATION_ROUTES (issue #945)"
            );
            assert!(
                AUDITED_OPERATIONS.contains(&op),
                "{op} must be registered in AUDITED_OPERATIONS (issue #945)"
            );
            assert_ne!(
                op, OP_RATE_LIMIT_OVERRIDE,
                "a TTL'd pacing override must use a distinct op from the legacy permanent override (issue #945)"
            );
        }

        // Read-only companion to the start-throttle SET/DELETE pair above
        // (issue #945): resolves an override's live state directly, so it is
        // ReadOnly (never Mutating -- it writes nothing) and, having no audit
        // op, must map to `None` in ALL_MUTATION_ROUTES rather than being
        // silently absent from the manifest entirely.
        let get_route = "GET /admin/start-throttle/{workflow_name}/override";
        assert!(
            CLASSIFIED_ROUTES
                .iter()
                .any(|(r, c)| *r == get_route && *c == RouteClass::ReadOnly),
            "{get_route} must be classified RouteClass::ReadOnly in CLASSIFIED_ROUTES (issue #945)"
        );
        assert!(
            ALL_MUTATION_ROUTES
                .iter()
                .any(|(r, mapped)| *r == get_route && mapped.is_none()),
            "{get_route} must map to None in ALL_MUTATION_ROUTES -- it is read-only (issue #945)"
        );
        assert!(
            EXCLUDED_ROUTES.contains(&get_route),
            "{get_route} should be listed in EXCLUDED_ROUTES, matching its GET /admin/circuits/{{activity_name}} sibling (issue #945)"
        );
    }

    #[test]
    fn rerun_route_is_classified_and_audited() {
        // Operator re-run of a terminal workflow (issue #777): the route is an
        // admin-only mutation and must be classified + audited. This dedicated
        // test -- not just the general exhaustiveness guards, which only
        // cross-check CLASSIFIED_ROUTES and ALL_MUTATION_ROUTES against each
        // other -- is what catches a route dropped from BOTH lists.
        let route = "POST /workflows/{id}/rerun";
        assert!(
            CLASSIFIED_ROUTES
                .iter()
                .any(|(r, c)| *r == route && *c == RouteClass::Mutating),
            "{route} must be classified RouteClass::Mutating in CLASSIFIED_ROUTES (issue #777)"
        );
        assert!(
            ALL_MUTATION_ROUTES
                .iter()
                .any(|(r, op)| *r == route && *op == Some(OP_WORKFLOW_RERUN)),
            "{route} must map to OP_WORKFLOW_RERUN in ALL_MUTATION_ROUTES (issue #777)"
        );
        assert!(AUDITED_OPERATIONS.contains(&OP_WORKFLOW_RERUN));
    }

    #[test]
    fn triage_route_is_classified_and_audited() {
        // Operator-mutable triage tags (issue #759): the route is an
        // admin-only mutation and must be classified + audited. This
        // dedicated test -- not just the general exhaustiveness guards,
        // which only cross-check CLASSIFIED_ROUTES and ALL_MUTATION_ROUTES
        // against each other -- is what catches a route dropped from BOTH
        // lists.
        let route = "PATCH /workflows/{id}/triage";
        assert!(
            CLASSIFIED_ROUTES
                .iter()
                .any(|(r, c)| *r == route && *c == RouteClass::Mutating),
            "{route} must be classified RouteClass::Mutating in CLASSIFIED_ROUTES (issue #759)"
        );
        assert!(
            ALL_MUTATION_ROUTES
                .iter()
                .any(|(r, op)| *r == route && *op == Some(OP_WORKFLOW_ANNOTATE)),
            "{route} must map to OP_WORKFLOW_ANNOTATE in ALL_MUTATION_ROUTES (issue #759)"
        );
        assert!(AUDITED_OPERATIONS.contains(&OP_WORKFLOW_ANNOTATE));
    }

    #[test]
    fn token_routes_are_classified_and_audited() {
        // Scoped API tokens (issue #942): create + revoke are admin-only
        // mutations; list is a read. This dedicated pin — not just the general
        // exhaustiveness guards, which only cross-check CLASSIFIED_ROUTES and
        // ALL_MUTATION_ROUTES against each other — is what catches a route
        // dropped from BOTH lists (the recurring gap the codebase pins
        // per-route).
        assert!(
            CLASSIFIED_ROUTES
                .iter()
                .any(|(r, c)| *r == "POST /admin/tokens" && *c == RouteClass::Mutating),
            "POST /admin/tokens must be classified RouteClass::Mutating (issue #942)"
        );
        assert!(
            CLASSIFIED_ROUTES
                .iter()
                .any(|(r, c)| *r == "GET /admin/tokens" && *c == RouteClass::ReadOnly),
            "GET /admin/tokens must be classified RouteClass::ReadOnly (issue #942)"
        );
        assert!(
            CLASSIFIED_ROUTES
                .iter()
                .any(|(r, c)| *r == "DELETE /admin/tokens/{id}" && *c == RouteClass::Mutating),
            "DELETE /admin/tokens/{{id}} must be classified RouteClass::Mutating (issue #942)"
        );
        assert!(
            ALL_MUTATION_ROUTES
                .iter()
                .any(|(r, op)| *r == "POST /admin/tokens" && *op == Some(OP_TOKEN_CREATE)),
            "create-token route must map to OP_TOKEN_CREATE (issue #942)"
        );
        assert!(
            ALL_MUTATION_ROUTES
                .iter()
                .any(|(r, op)| *r == "GET /admin/tokens" && op.is_none()),
            "list-tokens route is a read — must map to None in ALL_MUTATION_ROUTES (issue #942)"
        );
        assert!(
            ALL_MUTATION_ROUTES
                .iter()
                .any(|(r, op)| *r == "DELETE /admin/tokens/{id}" && *op == Some(OP_TOKEN_REVOKE)),
            "revoke-token route must map to OP_TOKEN_REVOKE (issue #942)"
        );
        assert!(AUDITED_OPERATIONS.contains(&OP_TOKEN_CREATE));
        assert!(AUDITED_OPERATIONS.contains(&OP_TOKEN_REVOKE));
        // The list read is explicitly excluded from audit.
        assert!(
            EXCLUDED_ROUTES.contains(&"GET /admin/tokens"),
            "GET /admin/tokens must be in EXCLUDED_ROUTES (read, no audit) (issue #942)"
        );
    }

    #[test]
    fn workflow_result_route_is_classified_read_only() {
        // The workflow-result endpoint (issue #527) is a read-only projection of the
        // execution's terminal output. It must be classified so the route-exhaustiveness
        // guards below cover it.
        assert!(
            CLASSIFIED_ROUTES
                .iter()
                .any(|(r, c)| *r == "GET /workflows/{id}/result" && *c == RouteClass::ReadOnly),
            "GET /workflows/{{id}}/result must be classified RouteClass::ReadOnly in \
             CLASSIFIED_ROUTES (issue #527)"
        );
    }

    #[test]
    fn workflow_count_route_is_classified_read_only() {
        // The grouped workflow-count fleet snapshot (issue #544) is a
        // read-only fan-out projection, no different from the other cross-shard
        // read models it's modeled after. This test — not just the general
        // exhaustiveness guards below, which only cross-check CLASSIFIED_ROUTES
        // and ALL_MUTATION_ROUTES against each other rather than against the
        // live router — is what actually catches a new route shipping with no
        // classification at all.
        assert!(
            CLASSIFIED_ROUTES
                .iter()
                .any(|(r, c)| *r == "GET /workflows/count" && *c == RouteClass::ReadOnly),
            "GET /workflows/count must be classified RouteClass::ReadOnly in \
             CLASSIFIED_ROUTES (issue #544)"
        );
        assert!(
            ALL_MUTATION_ROUTES
                .iter()
                .any(|(r, op)| *r == "GET /workflows/count" && op.is_none()),
            "GET /workflows/count must appear in ALL_MUTATION_ROUTES with no audit \
             operation (issue #544)"
        );
    }

    #[test]
    fn dag_run_graph_route_is_classified_read_only() {
        // The DAG run graph view (issue #690) is a read-only projection of a
        // run's node topology + status. This pinned test — not just the general
        // exhaustiveness guards below, which only cross-check CLASSIFIED_ROUTES
        // and ALL_MUTATION_ROUTES against each other rather than against the
        // live router — is what actually catches the route being dropped from
        // BOTH lists at once.
        let route = "GET /dags/{dag_name}/runs/{run_exec_id}";
        assert!(
            CLASSIFIED_ROUTES
                .iter()
                .any(|(r, c)| *r == route && *c == RouteClass::ReadOnly),
            "{route} must be classified RouteClass::ReadOnly in CLASSIFIED_ROUTES (issue #690)"
        );
        assert!(
            ALL_MUTATION_ROUTES
                .iter()
                .any(|(r, op)| *r == route && op.is_none()),
            "{route} must appear in ALL_MUTATION_ROUTES with no audit operation (issue #690)"
        );
        assert!(
            EXCLUDED_ROUTES.contains(&route),
            "{route} must appear in EXCLUDED_ROUTES (read-only, no audit trail; issue #690)"
        );
    }

    #[test]
    fn queue_coverage_route_is_classified_read_only() {
        // The fleet-wide task-queue coverage read model (issue #774) is a
        // read-only fan-out projection, no different from the other cross-shard
        // read models it's modeled after (workflow_reachability, workflow_count,
        // shard_health). This pinned test — not just the general exhaustiveness
        // guards below, which only cross-check CLASSIFIED_ROUTES and
        // ALL_MUTATION_ROUTES against each other rather than against the live
        // router — is what actually catches the route being dropped from BOTH
        // lists at once.
        let route = "GET /admin/queue-coverage";
        assert!(
            CLASSIFIED_ROUTES
                .iter()
                .any(|(r, c)| *r == route && *c == RouteClass::ReadOnly),
            "{route} must be classified RouteClass::ReadOnly in CLASSIFIED_ROUTES (issue #774)"
        );
        assert!(
            ALL_MUTATION_ROUTES
                .iter()
                .any(|(r, op)| *r == route && op.is_none()),
            "{route} must appear in ALL_MUTATION_ROUTES with no audit operation (issue #774)"
        );
        assert!(
            EXCLUDED_ROUTES.contains(&route),
            "{route} must appear in EXCLUDED_ROUTES (read-only, no audit trail; issue #774)"
        );
    }

    #[test]
    fn lineage_tree_route_is_classified_read_only() {
        // The recursive cross-shard lineage tree (issue #621) is a
        // point-in-time read over the existing `parent_id` edges: it appends no
        // events, performs no writes, and adds no `WorkflowEvent` variant.
        // Pinned here rather than relying on the general exhaustiveness guards,
        // which only cross-check CLASSIFIED_ROUTES against ALL_MUTATION_ROUTES
        // and therefore stay green if the route is dropped from BOTH lists.
        let route = "GET /workflows/{id}/tree";
        assert!(
            CLASSIFIED_ROUTES
                .iter()
                .any(|(r, c)| *r == route && *c == RouteClass::ReadOnly),
            "{route} must be classified RouteClass::ReadOnly in CLASSIFIED_ROUTES (issue #621)"
        );
        assert!(
            ALL_MUTATION_ROUTES
                .iter()
                .any(|(r, op)| *r == route && op.is_none()),
            "{route} must appear in ALL_MUTATION_ROUTES with no audit operation (issue #621)"
        );
        assert!(
            EXCLUDED_ROUTES.contains(&route),
            "{route} must appear in EXCLUDED_ROUTES (read-only, no audit trail; issue #621)"
        );
    }

    #[test]
    fn awaitables_route_is_classified_read_only() {
        // The open-awaitables diagnostic (issue #615) replays recorded history
        // to the current suspension point and enumerates every open awaitable.
        // It is read-only (appends no events, performs no writes) and writes no
        // audit rows. This pinned test — not just the general exhaustiveness
        // guards below, which only cross-check CLASSIFIED_ROUTES and
        // ALL_MUTATION_ROUTES against each other rather than against the live
        // router — is what actually catches the route being dropped from BOTH
        // lists at once.
        let route = "GET /workflows/{id}/awaitables";
        assert!(
            CLASSIFIED_ROUTES
                .iter()
                .any(|(r, c)| *r == route && *c == RouteClass::ReadOnly),
            "{route} must be classified RouteClass::ReadOnly in CLASSIFIED_ROUTES (issue #615)"
        );
        assert!(
            ALL_MUTATION_ROUTES
                .iter()
                .any(|(r, op)| *r == route && op.is_none()),
            "{route} must appear in ALL_MUTATION_ROUTES with no audit operation (issue #615)"
        );
        assert!(
            EXCLUDED_ROUTES.contains(&route),
            "{route} must appear in EXCLUDED_ROUTES (read-only, no audit trail; issue #615)"
        );
    }

    #[test]
    fn diagnose_route_is_classified_read_only() {
        // The per-execution stall diagnosis endpoint (issue #809) resolves the
        // owning shard from the ExecutionId and reads only that shard: it
        // appends no WorkflowEvent, performs no task-queue mutation, and adds
        // no migration, so it is safe to call repeatedly. Pinned here (not only
        // via the general exhaustiveness guards, which cross-check
        // CLASSIFIED_ROUTES and ALL_MUTATION_ROUTES against each other rather
        // than against the live router) so dropping it from BOTH lists at once
        // is caught.
        let route = "GET /workflows/{id}/diagnose";
        assert!(
            CLASSIFIED_ROUTES
                .iter()
                .any(|(r, c)| *r == route && *c == RouteClass::ReadOnly),
            "{route} must be classified RouteClass::ReadOnly in CLASSIFIED_ROUTES (issue #809)"
        );
        assert!(
            ALL_MUTATION_ROUTES
                .iter()
                .any(|(r, op)| *r == route && op.is_none()),
            "{route} must appear in ALL_MUTATION_ROUTES with no audit operation (issue #809)"
        );
        assert!(
            EXCLUDED_ROUTES.contains(&route),
            "{route} must appear in EXCLUDED_ROUTES (read-only, no audit trail; issue #809)"
        );
    }

    #[test]
    fn replay_diagnosis_route_is_classified_read_only() {
        // The single-execution replay-diagnosis endpoint (issue #614) loads one
        // execution's recorded history and replays it against the currently
        // registered handler, returning a structured verdict. It is read-only
        // (appends no events, performs no writes) and writes no audit rows. This
        // pinned test — not just the general exhaustiveness guards below, which
        // only cross-check CLASSIFIED_ROUTES and ALL_MUTATION_ROUTES against each
        // other rather than against the live router — is what actually catches
        // the route being dropped from BOTH lists at once.
        let route = "POST /workflows/{id}/replay-diagnosis";
        assert!(
            CLASSIFIED_ROUTES
                .iter()
                .any(|(r, c)| *r == route && *c == RouteClass::ReadOnly),
            "{route} must be classified RouteClass::ReadOnly in CLASSIFIED_ROUTES (issue #614)"
        );
        assert!(
            ALL_MUTATION_ROUTES
                .iter()
                .any(|(r, op)| *r == route && op.is_none()),
            "{route} must appear in ALL_MUTATION_ROUTES with no audit operation (issue #614)"
        );
        assert!(
            EXCLUDED_ROUTES.contains(&route),
            "{route} must appear in EXCLUDED_ROUTES (read-only, no audit trail; issue #614)"
        );
    }

    #[test]
    fn progress_stream_route_is_classified_read_only() {
        // The ephemeral workflow progress SSE stream (issue #791) is a
        // read-only, best-effort side channel; it never mutates state and
        // writes no audit rows. This pinned test — not just the general
        // exhaustiveness guards below, which only cross-check CLASSIFIED_ROUTES
        // and ALL_MUTATION_ROUTES against each other rather than against the
        // live router — is what actually catches the route being dropped from
        // BOTH lists at once.
        let route = "GET /workflows/{id}/stream";
        assert!(
            CLASSIFIED_ROUTES
                .iter()
                .any(|(r, c)| *r == route && *c == RouteClass::ReadOnly),
            "{route} must be classified RouteClass::ReadOnly in CLASSIFIED_ROUTES (issue #791)"
        );
        assert!(
            ALL_MUTATION_ROUTES
                .iter()
                .any(|(r, op)| *r == route && op.is_none()),
            "{route} must appear in ALL_MUTATION_ROUTES with no audit operation (issue #791)"
        );
        assert!(
            EXCLUDED_ROUTES.contains(&route),
            "{route} must appear in EXCLUDED_ROUTES (read-only, no audit trail; issue #791)"
        );
    }

    #[test]
    fn admin_status_route_is_classified_read_only() {
        // The rolled-up health summary (issue #679) is a read-only fan-out
        // projection. This pin — not just the general exhaustiveness guards,
        // which only cross-check CLASSIFIED_ROUTES and ALL_MUTATION_ROUTES
        // against each other — is what catches the route shipping unclassified.
        assert!(
            CLASSIFIED_ROUTES
                .iter()
                .any(|(r, c)| *r == "GET /admin/status" && *c == RouteClass::ReadOnly),
            "GET /admin/status must be classified RouteClass::ReadOnly in \
             CLASSIFIED_ROUTES (issue #679)"
        );
        assert!(
            ALL_MUTATION_ROUTES
                .iter()
                .any(|(r, op)| *r == "GET /admin/status" && op.is_none()),
            "GET /admin/status must appear in ALL_MUTATION_ROUTES with no audit \
             operation (issue #679)"
        );
    }

    #[test]
    fn admin_canary_route_is_classified_read_only() {
        // The synthetic liveness canary freshness report (issue #796) is a
        // read-only cross-shard projection. This pin — not just the general
        // exhaustiveness guards, which only cross-check CLASSIFIED_ROUTES and
        // ALL_MUTATION_ROUTES against each other — is what catches the route
        // shipping unclassified.
        assert!(
            CLASSIFIED_ROUTES
                .iter()
                .any(|(r, c)| *r == "GET /admin/canary" && *c == RouteClass::ReadOnly),
            "GET /admin/canary must be classified RouteClass::ReadOnly in \
             CLASSIFIED_ROUTES (issue #796)"
        );
        assert!(
            ALL_MUTATION_ROUTES
                .iter()
                .any(|(r, op)| *r == "GET /admin/canary" && op.is_none()),
            "GET /admin/canary must appear in ALL_MUTATION_ROUTES with no audit \
             operation (issue #796)"
        );
        assert!(
            EXCLUDED_ROUTES.contains(&"GET /admin/canary"),
            "GET /admin/canary must appear in EXCLUDED_ROUTES (read-only, no \
             audit trail; issue #796)"
        );
    }

    #[test]
    fn schedule_update_route_is_classified_mutating() {
        // The in-place schedule update (issue #771) is a mutating, audited
        // operator route. This pinned test — not just the general
        // exhaustiveness guards below, which only cross-check
        // CLASSIFIED_ROUTES and ALL_MUTATION_ROUTES against each other rather
        // than against the live router — is what actually catches the route
        // being dropped from BOTH lists at once.
        assert!(
            CLASSIFIED_ROUTES
                .iter()
                .any(|(r, c)| *r == "PATCH /admin/schedules/{id}" && *c == RouteClass::Mutating),
            "PATCH /admin/schedules/{{id}} must be classified RouteClass::Mutating in \
             CLASSIFIED_ROUTES (issue #771)"
        );
        assert!(
            ALL_MUTATION_ROUTES
                .iter()
                .any(|(r, op)| *r == "PATCH /admin/schedules/{id}"
                    && *op == Some(OP_SCHEDULE_UPDATE)),
            "PATCH /admin/schedules/{{id}} must appear in ALL_MUTATION_ROUTES mapped to \
             OP_SCHEDULE_UPDATE (issue #771)"
        );
        assert!(
            AUDITED_OPERATIONS.contains(&OP_SCHEDULE_UPDATE),
            "schedule.update must be a registered audited operation (issue #771)"
        );
    }

    #[test]
    fn usage_report_route_is_classified_read_only() {
        // The historical per-tenant usage report (issue #596) is a read-only
        // fan-out aggregation, no different from workflow_count / debounce /
        // concurrency. This test — not just the general exhaustiveness guards
        // below, which only cross-check CLASSIFIED_ROUTES and
        // ALL_MUTATION_ROUTES against each other rather than against the live
        // router — is what actually catches a new route shipping with no
        // classification at all.
        assert!(
            CLASSIFIED_ROUTES
                .iter()
                .any(|(r, c)| *r == "GET /admin/usage" && *c == RouteClass::ReadOnly),
            "GET /admin/usage must be classified RouteClass::ReadOnly in \
             CLASSIFIED_ROUTES (issue #596)"
        );
        assert!(
            ALL_MUTATION_ROUTES
                .iter()
                .any(|(r, op)| *r == "GET /admin/usage" && op.is_none()),
            "GET /admin/usage must appear in ALL_MUTATION_ROUTES with no audit \
             operation (issue #596)"
        );
        assert!(
            EXCLUDED_ROUTES.contains(&"GET /admin/usage"),
            "GET /admin/usage must appear in EXCLUDED_ROUTES (read-only, no audit trail \
             entry expected, issue #596)"
        );
    }

    #[test]
    fn completion_delivery_routes_are_classified() {
        // Issue #605 code review: the completion-callback delivery list and
        // redrive routes shipped registered in the router (management_api_routes
        // in autumn-harvest-plugin/src/api.rs) but absent from
        // CLASSIFIED_ROUTES/AUDITED_OPERATIONS/ALL_MUTATION_ROUTES entirely —
        // exactly the gap issues #544/#601 each independently hit and had to
        // retroactively patch, since the general exhaustiveness guards below
        // only cross-check these lists against each other, never against the
        // live router. This test is what actually catches that class of gap
        // for these two specific routes.
        assert!(
            CLASSIFIED_ROUTES.iter().any(|(r, c)| {
                *r == "GET /workflows/{id}/completion-deliveries" && *c == RouteClass::ReadOnly
            }),
            "GET /workflows/{{id}}/completion-deliveries must be classified \
             RouteClass::ReadOnly in CLASSIFIED_ROUTES (issue #605)"
        );
        assert!(
            ALL_MUTATION_ROUTES.iter().any(|(r, op)| {
                *r == "GET /workflows/{id}/completion-deliveries" && op.is_none()
            }),
            "GET /workflows/{{id}}/completion-deliveries must appear in \
             ALL_MUTATION_ROUTES with no audit operation (issue #605)"
        );
        assert!(
            EXCLUDED_ROUTES.contains(&"GET /workflows/{id}/completion-deliveries"),
            "GET /workflows/{{id}}/completion-deliveries must appear in EXCLUDED_ROUTES \
             (read-only, no audit trail entry expected, issue #605)"
        );

        let redrive_route = "POST /workflows/{id}/completion-deliveries/{delivery_id}/redrive";
        assert!(
            CLASSIFIED_ROUTES
                .iter()
                .any(|(r, c)| *r == redrive_route && *c == RouteClass::Mutating),
            "{redrive_route} must be classified RouteClass::Mutating in CLASSIFIED_ROUTES \
             (issue #605)"
        );
        assert!(
            ALL_MUTATION_ROUTES
                .iter()
                .any(|(r, op)| *r == redrive_route && *op == Some(OP_CALLBACK_REDRIVE)),
            "{redrive_route} must appear in ALL_MUTATION_ROUTES with audit operation \
             OP_CALLBACK_REDRIVE (issue #605)"
        );
        assert!(
            AUDITED_OPERATIONS.contains(&OP_CALLBACK_REDRIVE),
            "OP_CALLBACK_REDRIVE must appear in AUDITED_OPERATIONS (issue #605)"
        );
    }

    #[test]
    fn payload_decode_read_op_is_registered_in_audited_operations() {
        // Issue #608: operator read-path decoding of codec-encrypted payloads
        // writes one best-effort audit row per request that decoded/marked
        // ≥1 envelope (SSE: one row at stream open when decode mode is
        // active). It is a *read* audit like OP_EXECUTION_STREAM_OPEN — no
        // CLASSIFIED_ROUTES/ALL_MUTATION_ROUTES change (no route mutates and
        // no new route exists), but the operation name itself must be in the
        // registry so audit consumers can enumerate it.
        assert_eq!(OP_PAYLOAD_DECODE_READ, "payload.decode_read");
        assert!(
            AUDITED_OPERATIONS.contains(&OP_PAYLOAD_DECODE_READ),
            "OP_PAYLOAD_DECODE_READ must appear in AUDITED_OPERATIONS (issue #608)"
        );
    }

    #[test]
    fn all_classified_routes_are_in_route_manifest() {
        let manifest: std::collections::HashSet<&str> =
            ALL_MUTATION_ROUTES.iter().map(|(r, _)| *r).collect();

        for (route, _) in CLASSIFIED_ROUTES {
            assert!(
                manifest.contains(route),
                "route '{route}' is in CLASSIFIED_ROUTES but missing from \
                 ALL_MUTATION_ROUTES — add it to both slices"
            );
        }
    }

    #[test]
    fn classified_routes_has_no_duplicates() {
        let mut seen = std::collections::HashSet::new();
        for (route, _) in CLASSIFIED_ROUTES {
            assert!(
                seen.insert(*route),
                "duplicate route in CLASSIFIED_ROUTES: '{route}'"
            );
        }
    }

    #[test]
    fn classified_routes_count_matches_route_manifest() {
        assert_eq!(
            CLASSIFIED_ROUTES.len(),
            ALL_MUTATION_ROUTES.len(),
            "CLASSIFIED_ROUTES has {} entries but ALL_MUTATION_ROUTES has {} — \
             the two slices must cover exactly the same route set",
            CLASSIFIED_ROUTES.len(),
            ALL_MUTATION_ROUTES.len()
        );
    }

    #[test]
    fn public_safe_routes_are_excluded_from_audit() {
        let excluded: std::collections::HashSet<&str> = EXCLUDED_ROUTES.iter().copied().collect();
        for (route, class) in CLASSIFIED_ROUTES {
            if *class == RouteClass::PublicSafe {
                assert!(
                    excluded.contains(route),
                    "PublicSafe route '{route}' should be in EXCLUDED_ROUTES \
                     since it is never a mutation that needs auditing"
                );
            }
        }
    }

    #[test]
    fn mutating_routes_are_audited_or_explicitly_excluded() {
        let audited: std::collections::HashSet<&str> = ALL_MUTATION_ROUTES
            .iter()
            .filter_map(|(r, op)| op.map(|_| *r))
            .collect();
        let excluded: std::collections::HashSet<&str> = EXCLUDED_ROUTES.iter().copied().collect();

        for (route, class) in CLASSIFIED_ROUTES {
            if *class == RouteClass::Mutating {
                assert!(
                    audited.contains(route) || excluded.contains(route),
                    "Mutating route '{route}' is neither audited (Some(op) in \
                     ALL_MUTATION_ROUTES) nor listed in EXCLUDED_ROUTES — \
                     explicitly declare its audit disposition"
                );
            }
        }
    }

    // ── Read-only operator role (issue #776) ──────────────────────────────────

    #[test]
    fn deny_readonly_mutation_truth_table() {
        // A read-only principal is denied every Mutating route, allowed every
        // ReadOnly/PublicSafe route. A non-read-only principal is never denied
        // by this decision (the enforcement layer leaves them to the existing
        // admin gates).
        assert!(
            deny_readonly_mutation(true, RouteClass::Mutating),
            "read-only principal must be denied a Mutating route (403)"
        );
        assert!(
            !deny_readonly_mutation(true, RouteClass::ReadOnly),
            "read-only principal must reach a ReadOnly route"
        );
        assert!(
            !deny_readonly_mutation(true, RouteClass::PublicSafe),
            "read-only principal must reach a PublicSafe route"
        );
        // A non-read-only (admin/anon) principal is never denied by THIS fn —
        // their access is decided by the existing admin gates, not this layer.
        assert!(!deny_readonly_mutation(false, RouteClass::Mutating));
        assert!(!deny_readonly_mutation(false, RouteClass::ReadOnly));
        assert!(!deny_readonly_mutation(false, RouteClass::PublicSafe));
    }

    #[test]
    fn newly_classified_mutations_are_mutating_and_covered() {
        // Privilege-escalation guard (issue #776): every route added to the
        // classification table as a mutation in this slice MUST be
        // RouteClass::Mutating — a wrong Mutating→ReadOnly entry would let a
        // read-only principal force-open a circuit / create a calendar / retry
        // a DAG / start-via-update. Dedicated pin (like the legal-hold pin):
        // the general exhaustiveness guards only cross-check the two lists
        // against each other, so this catches a class regression on these
        // specific routes.
        let audited: std::collections::HashSet<&str> = ALL_MUTATION_ROUTES
            .iter()
            .filter_map(|(r, op)| op.map(|_| *r))
            .collect();
        let excluded: std::collections::HashSet<&str> = EXCLUDED_ROUTES.iter().copied().collect();
        for route in [
            "POST /workflows/{workflow_name}/update-with-start",
            "POST /dags/{dag_name}/runs/{run_exec_id}/retry",
            "POST /admin/circuits/{activity_name}/force-open",
            "POST /admin/circuits/{activity_name}/force-close",
            "POST /admin/completion-triggers",
            "POST /calendars",
            "PUT /calendars/{name}",
            "DELETE /calendars/{name}",
            // Operator re-run of a terminal workflow (issue #777): starts a
            // brand-new execution, so a read-only principal must never reach it.
            "POST /workflows/{id}/rerun",
        ] {
            assert!(
                CLASSIFIED_ROUTES
                    .iter()
                    .any(|(r, c)| *r == route && *c == RouteClass::Mutating),
                "{route} must be classified RouteClass::Mutating (issue #776) — \
                 a read-only principal must never reach it"
            );
            assert!(
                audited.contains(route) || excluded.contains(route),
                "{route} must declare its audit disposition (Some(op) or EXCLUDED_ROUTES)"
            );
        }
        // The four circuit/dag/update ops reuse pre-existing constants that had
        // never been wired into a route manifest entry; confirm the wiring.
        for op in [
            OP_WORKFLOW_UPDATE_WITH_START,
            OP_DAG_RETRY,
            OP_CIRCUIT_FORCE_OPEN,
            OP_CIRCUIT_FORCE_CLOSE,
        ] {
            assert!(
                AUDITED_OPERATIONS.contains(&op),
                "operation '{op}' must be in AUDITED_OPERATIONS (issue #776 wiring)"
            );
        }
    }

    #[test]
    fn newly_classified_reads_are_read_only() {
        // The read grant surface for the read-only tier: each must be
        // RouteClass::ReadOnly so a read-only principal reaches it (AC1). A
        // wrong ReadOnly→Mutating here would only over-restrict (403 a read),
        // not escalate, but it breaks the "100% of ReadOnly reachable" bar.
        for route in [
            "GET /batches/pending",
            "GET /workflows/registered",
            "GET /workflows/registered/{name}/schema",
            "GET /workflows/registered/{name}/interface",
            "GET /workflows/types/{workflow_name}/handlers",
            "GET /workflows/by-id/{workflow_name}",
            "POST /workflows/by-id/{workflow_name}",
            "GET /dead-letters/aggregate",
            "GET /admin/circuits",
            "GET /admin/circuits/{activity_name}",
            "GET /admin/queues/scaling",
            "GET /admin/metrics",
            "GET /admin/completion-triggers",
            "GET /admin/schedules/{id}",
            "GET /admin/schedules/{id}/runs",
            "GET /admin/schedules/decisions",
            "GET /admin/schedules/{id}/decisions",
            "GET /admin/schedules/{id}/preview",
            "POST /admin/schedules/preview",
            "GET /calendars",
            "GET /calendars/{name}",
            "GET /workers/{worker_id}/pinned",
            "GET /admin/queues/{queue_name}/eligibility",
            "GET /admin/tasks/{id}/eligibility",
        ] {
            assert!(
                CLASSIFIED_ROUTES
                    .iter()
                    .any(|(r, c)| *r == route && *c == RouteClass::ReadOnly),
                "{route} must be classified RouteClass::ReadOnly (issue #776)"
            );
        }
    }
}
