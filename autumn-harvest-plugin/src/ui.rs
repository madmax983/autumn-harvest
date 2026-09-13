//! Vantage — an embedded, read-only HTML dashboard for Harvest workflows.
//!
//! Mounts alongside the management API (e.g. `/api/harvest/ui`). Renders a
//! paginated workflow list and a per-workflow detail page showing inputs,
//! outputs, and the full event history. Assets are inlined so the dashboard
//! works in network-restricted environments.
#![allow(clippy::literal_string_with_formatting_args)]

use std::fmt::Write as _;
use std::sync::Arc;

use std::collections::HashMap;

use autumn_web::AppState;
use autumn_web::error::AutumnError;
use autumn_web::extract::{Path, Query};
use autumn_web::reexports::axum;
use autumn_web::session::Session;
use axum::Extension;
use axum::Form;
use axum::Router;
use axum::middleware;
use axum::response::IntoResponse as _;
use axum::routing::{get, post};
use chrono::{DateTime, Utc};
use diesel::BoolExpressionMethods;
use diesel::dsl::sql;
use diesel::sql_types::{Bool, Text};
use diesel::{ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper};
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use maud::{Markup, PreEscaped, html};
use serde::Deserialize;
use serde_json::Value;
use tracing::warn;

use autumn_harvest::Schedule;
use autumn_harvest::ShardRouter;
use autumn_harvest::audit::{
    OP_BUILD_COMPAT_DECLARE, OP_BUILD_COMPAT_REVOKE, OP_BUILD_POLICY_SET, OP_GATE_LIFT,
    OP_SCHEDULE_DELETE, OP_SCHEDULE_PAUSE, OP_SCHEDULE_RESUME, OP_SCHEDULE_TRIGGER,
    OP_WORKFLOW_CANCEL, OP_WORKFLOW_PAUSE, OP_WORKFLOW_RESET, OP_WORKFLOW_RESUME,
    OP_WORKFLOW_SIGNAL, OP_WORKFLOW_TERMINATE, SOURCE_UI, STATUS_FAILED, STATUS_SUCCEEDED,
    TARGET_BUILD_ROUTING, TARGET_DEAD_LETTER, TARGET_GATE, TARGET_SCHEDULE, TARGET_WORKFLOW,
    insert_audit, insert_audit_batch,
};
use autumn_harvest::build_routing::{
    BuildCompatEntry, BuildPolicy, BuildReachability, all_build_reachability, declare_compat,
    list_build_compat, list_build_policies, merge_reachability, revoke_compat, set_build_policy,
};
use autumn_harvest::error::{HarvestResult, database_error};
use autumn_harvest::execution::StartWorkflowParams;
use autumn_harvest::models::{
    DeadLetter, ExternalTask, HarvestEvent, HarvestSchedule, HarvestSignal, HarvestTimer,
    NewAuditRecord, ScheduleDecision, TaskQueueItem, WorkflowExecution,
};
use autumn_harvest::payload_codec::{LossyDecodeOutcome, PayloadCodecs};
use autumn_harvest::reset::{
    ResetSignalReapplyPolicy, WorkflowResetRequest, reset_workflow_execution,
};
use autumn_harvest::scheduler::RegisteredDag;
use autumn_harvest::schema::{
    harvest_dead_letters, harvest_events, harvest_external_tasks, harvest_schedules,
    harvest_signals, harvest_task_queue, harvest_timers, harvest_workflow_executions,
};
use autumn_harvest::signal::send_signal;
use autumn_harvest::start_or_load_workflow_execution_with_metrics_and_codecs;
use autumn_harvest::store::admit_update_event_with_codecs;
use autumn_harvest::types::{
    ExecutionId as HarvestExecutionId, Priority, ShardId, UpdateId, WorkflowIdReusePolicy,
};
use autumn_harvest::workers::{WorkerFilters, WorkerHealth, WorkerRow, list_workers};
use autumn_harvest::{
    StepKind, StepOutcome, Timeline, TimelineRollup, TimelineStep, derive_timeline,
};
use autumn_harvest::{
    cancel_workflow_execution, pause_workflow_execution, resume_workflow_execution,
    terminate_workflow_execution,
};

use crate::api::{
    DagRetryFailure, DagRetryResponse, HarvestApiRuntime, HarvestApiState, KNOWN_WORKFLOW_STATES,
    WorkflowFilters, acquire_conn, audit_decoded_read, db_conn_for_execution, db_conn_for_shard,
    decode_error_field, decode_workflow_execution_fields, extension_session, load_execution,
    load_workflows, load_workflows_from_shards, map_error, parse_execution_id, read_path_decoder,
    require_harvest_admin, retry_dag_run_inner,
};
use crate::dag_graph::{DagNodeStatus, DagRunNode, build_run_graph};
use crate::shard_fanout::UnavailableShard;

const DEFAULT_PAGE_SIZE: i64 = 25;
const DEFAULT_DLQ_PAGE_SIZE: i64 = 50;
const MAX_PAGE_SIZE: i64 = 200;
const DLQ_BULK_ACTION_LIMIT: usize = autumn_harvest::dlq::MAX_BULK_LIMIT as usize;
/// Default grouping for the DLQ summary view (issue #385).
const DEFAULT_DLQ_SUMMARY_GROUP_BY: &str = "workflow_name,failure_signature";
/// Top-N groups rendered in the DLQ summary view before long-tail rollup.
const DLQ_SUMMARY_GROUP_LIMIT: u32 = 25;
/// Sample dead-letter IDs surfaced per summary group.
const DLQ_SUMMARY_SAMPLES_PER_GROUP: u32 = 3;

const KNOWN_STATES: &[&str] = KNOWN_WORKFLOW_STATES;

const STYLE: &str = r#"
*,*::before,*::after{box-sizing:border-box}
body{margin:0;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;background:#0f172a;color:#e2e8f0}
a{color:#93c5fd;text-decoration:none}
a:hover{text-decoration:underline}
header{background:#1e293b;border-bottom:1px solid #334155;padding:16px 24px;display:flex;align-items:center;justify-content:space-between}
header h1{margin:0;font-size:20px;font-weight:600}
header h1 a{color:#f8fafc;text-decoration:none}
header .subtitle{color:#94a3b8;font-size:13px;margin-left:8px}
header nav{display:flex;gap:16px;font-size:13px}
header nav a{color:#cbd5e1}
header nav a.active{color:#f8fafc;font-weight:600}
main{padding:24px;max-width:1200px;margin:0 auto}
h2{font-size:18px;margin:0 0 16px;color:#f8fafc}
h3{font-size:14px;margin:24px 0 8px;color:#cbd5e1;text-transform:uppercase;letter-spacing:.06em}
.filters{display:flex;gap:12px;align-items:flex-end;margin-bottom:16px;flex-wrap:wrap}
.filters label{display:flex;flex-direction:column;font-size:12px;color:#94a3b8}
.filters select,.filters input{background:#1e293b;color:#e2e8f0;border:1px solid #334155;border-radius:6px;padding:6px 10px;font-size:13px;margin-top:4px}
.filters button{background:#2563eb;color:#fff;border:0;border-radius:6px;padding:8px 14px;font-size:13px;cursor:pointer;align-self:flex-end;height:32px}
.filters button:hover{background:#1d4ed8}
.filters .reset{background:transparent;color:#94a3b8;border:1px solid #334155}
.actions{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.actions form{margin:0}
.actions button{background:#2563eb;color:#fff;border:0;border-radius:6px;padding:6px 10px;font-size:12px;cursor:pointer}
.actions button:hover{background:#1d4ed8}
.actions button.danger{background:#991b1b}
.actions button.danger:hover{background:#7f1d1d}
.bulk-actions{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin:0 0 16px}
.bulk-actions form{margin:0}
.bulk-actions button{background:#2563eb;color:#fff;border:0;border-radius:6px;padding:8px 12px;font-size:13px;cursor:pointer}
.bulk-actions button.danger{background:#991b1b}
.bulk-actions button:disabled{background:#334155;color:#64748b;cursor:not-allowed}
table{width:100%;border-collapse:collapse;background:#1e293b;border-radius:8px;overflow:hidden;font-size:13px}
th,td{padding:10px 14px;text-align:left;border-bottom:1px solid #334155;vertical-align:top}
th{background:#0f172a;color:#94a3b8;font-weight:500;text-transform:uppercase;letter-spacing:.05em;font-size:11px}
tbody tr:last-child td{border-bottom:0}
tbody tr:hover{background:#263449}
tbody tr.stale-row{background:#1c1917}
tbody tr.stale-row:hover{background:#292524}
td code{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px;color:#cbd5e1}
.badge{display:inline-block;padding:2px 8px;border-radius:999px;font-size:11px;font-weight:600;letter-spacing:.03em}
.badge.RUNNING{background:#1d4ed8;color:#dbeafe}
.badge.COMPLETED{background:#166534;color:#dcfce7}
.badge.FAILED{background:#991b1b;color:#fee2e2}
.badge.CANCELLED{background:#4b5563;color:#f3f4f6}
.badge.TERMINATED{background:#52525b;color:#f4f4f5}
.badge.UNKNOWN{background:#334155;color:#e2e8f0}
.badge.Active{background:#166534;color:#dcfce7}
.badge.Draining{background:#92400e;color:#fef3c7}
.badge.Stopped{background:#334155;color:#e2e8f0}
.badge.timezone{background:#1e3a8a;color:#93c5fd;border:1px solid #3b82f6}
.timezone-utc{color:#94a3b8;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:11px}
.badge-owner{background:#312e81;color:#c7d2fe;border:1px solid #4338ca}
.badge-sev-sev1{background:#7f1d1d;color:#fee2e2;border:1px solid #b91c1c}
.badge-sev-sev2{background:#7c2d12;color:#ffedd5;border:1px solid #c2410c}
.badge-sev-sev3{background:#713f12;color:#fef9c3;border:1px solid #a16207}
.badge-sev-sev4{background:#065f46;color:#d1fae5;border:1px solid #047857}
.banner{padding:12px 16px;border-radius:8px;margin-bottom:20px;font-size:13px;font-weight:500}
.banner.Healthy{background:#14532d;color:#bbf7d0;border:1px solid #166534}
.banner.Degraded{background:#431407;color:#fed7aa;border:1px solid #92400e}
.banner.Unhealthy{background:#450a0a;color:#fecaca;border:1px solid #991b1b}
.flash{background:#172554;color:#bfdbfe;border:1px solid #1d4ed8;padding:10px 14px;border-radius:6px;margin-bottom:16px;font-size:13px}
.shard-header{margin:20px 0 8px;font-size:13px;color:#94a3b8;font-weight:600;text-transform:uppercase;letter-spacing:.06em;border-bottom:1px solid #1e293b;padding-bottom:6px}
.shard-error{background:#1c1917;border:1px solid #57534e;border-radius:6px;padding:12px 16px;color:#a8a29e;font-size:13px;margin-bottom:12px}
.degraded-banner{background:#422006;border:1px solid #a16207;border-radius:6px;padding:12px 16px;color:#fef3c7;font-size:13px;margin-bottom:16px}
.degraded-banner strong{color:#fde68a}
.unhealthy-summary{border-color:#b45309;background:#292524;color:#fed7aa;font-size:13px}
.unhealthy-summary strong{color:#fdba74}
.subtle{color:#94a3b8;font-size:11px;margin-top:2px}
.table-scroll{overflow-x:auto;max-width:100%}
.health-badges{display:flex;flex-direction:column;gap:4px;align-items:flex-start}
tr.schedule-unhealthy td{background:#1c1917}
a.drilldown{font-size:12px;color:#93c5fd;border:1px solid #334155;border-radius:6px;padding:5px 9px}
a.drilldown:hover{background:#1e293b;text-decoration:none}
.kv dt{color:#94a3b8}
.kv dd{margin:0;color:#e2e8f0}
.view-toggle{display:inline-flex;gap:2px;margin:0 0 16px;border:1px solid #334155;border-radius:6px;overflow:hidden;font-size:13px}
.view-toggle a,.view-toggle span{padding:6px 14px;display:inline-block}
.view-toggle a{color:#93c5fd;text-decoration:none}
.view-toggle a:hover{background:#1e293b}
.view-toggle span.active{background:#2563eb;color:#fff;font-weight:600}
.log-filters a{color:#93c5fd;text-decoration:none;padding:2px 8px;border-radius:4px}
.log-filters a:hover{background:#1e293b}
.log-filters a.active{background:#2563eb;color:#fff;font-weight:600}
.summary-stats{display:flex;gap:18px;flex-wrap:wrap;margin:0 0 16px;font-size:13px;color:#94a3b8}
.summary-stats strong{color:#e2e8f0}
.summary-stats .note{color:#fbbf24}
code.sample{display:inline-block;margin:0 4px 2px 0;font-size:11px;color:#cbd5e1}
.pagination{display:flex;gap:8px;align-items:center;margin-top:16px;font-size:13px;color:#94a3b8}
.pagination a,.pagination span{padding:6px 10px;border-radius:6px;border:1px solid #334155}
.pagination a{color:#93c5fd}
.pagination span.disabled{color:#475569}
.card{background:#1e293b;border:1px solid #334155;border-radius:8px;padding:16px;margin-bottom:16px}
.card h3{margin-top:0}
.kv{display:grid;grid-template-columns:180px 1fr;row-gap:6px;column-gap:16px;font-size:13px}
.kv .k{color:#94a3b8}
.kv .v{color:#e2e8f0;word-break:break-all}
pre{background:#0f172a;color:#e2e8f0;border:1px solid #334155;border-radius:6px;padding:12px;overflow:auto;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px;margin:0}
.error-banner{background:#7f1d1d;color:#fee2e2;padding:10px 14px;border-radius:6px;margin-bottom:16px;font-size:13px}
.filters .field-error{display:block;background:#7f1d1d;color:#fee2e2;padding:2px 8px;border-radius:4px;font-size:11px;margin-top:4px}
.empty{color:#94a3b8;font-style:italic;padding:24px;text-align:center}
.detail-row{display:flex;gap:16px;align-items:center;margin-bottom:16px;flex-wrap:wrap}
.detail-row .back{color:#93c5fd;font-size:13px}
details{margin-top:8px}
details summary{cursor:pointer;color:#93c5fd;font-size:12px;font-family:ui-monospace,SFMono-Regular,Menlo,monospace}
.detail-block{display:grid;gap:10px;margin-top:10px}
footer{padding:20px 24px;color:#94a3b8;font-size:12px;text-align:center;border-top:1px solid #1e293b;margin-top:32px}
.event-label{font-size:13px}
.event-label code{font-size:11px;color:#94a3b8;margin-left:4px}
.operator-actions{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px}
.operator-actions form{margin:0}
.operator-actions button,.operator-actions a.btn{background:#1e3a5f;color:#93c5fd;border:1px solid #2563eb;border-radius:6px;padding:6px 12px;font-size:12px;cursor:pointer;text-decoration:none;display:inline-block}
.operator-actions button:hover,.operator-actions a.btn:hover{background:#2563eb;color:#fff}
.operator-actions button.danger{background:#450a0a;color:#fca5a5;border-color:#991b1b}
.operator-actions button.danger:hover{background:#991b1b;color:#fff}
.dag-graph-scroll{overflow:auto;max-width:100%;max-height:70vh;border:1px solid #334155;border-radius:8px;background:#0f172a;padding:8px}
.dag-node{stroke:#0f172a;stroke-width:1}
.dag-node.selected{stroke:#f8fafc;stroke-width:2}
.dag-node-label{fill:#f8fafc;font:12px system-ui,-apple-system,sans-serif}
.dag-edge{stroke:#64748b;stroke-width:1.5}
.dag-legend,.timeline-legend{display:flex;flex-wrap:wrap;gap:12px;margin:8px 0;font-size:.8rem;color:#cbd5e1}
.dag-legend span,.timeline-legend span{display:inline-flex;align-items:center;gap:4px}
.dag-legend .swatch,.timeline-legend .swatch{width:12px;height:12px;border-radius:3px;display:inline-block}
.dag-run-current{color:#94a3b8;font-size:11px;margin-left:6px}
.dag-run-detail{color:#94a3b8;font-size:11px;margin-left:8px}
.gantt-scroll{overflow:auto;max-width:100%;max-height:75vh;border:1px solid #334155;border-radius:8px;background:#0f172a;padding:8px}
.gantt-lane-label{fill:#cbd5e1;font:11px system-ui,-apple-system,sans-serif}
.gantt-lane-group{fill:#93c5fd;font:600 11px system-ui,-apple-system,sans-serif}
.gantt-axis-tick{stroke:#334155;stroke-width:1}
.gantt-axis-label{fill:#94a3b8;font:10px system-ui,-apple-system,sans-serif}
.gantt-span{stroke:#0f172a;stroke-width:1}
.gantt-span-open{stroke-dasharray:4 3;stroke:#94a3b8;stroke-width:1.5}
.gantt-span-slowest{stroke:#f8fafc;stroke-width:2}
.gantt-seg-wait{opacity:.55}
.gantt-seg-exec{opacity:1}
.gantt-seg-whole{opacity:1}
.gantt-badge{fill:#f8fafc;font:10px system-ui,-apple-system,sans-serif}
.gantt-pause-band{fill:#78350f;opacity:.28}
.gantt-pause-label{fill:#fbbf24;font:10px system-ui,-apple-system,sans-serif}
.gantt-nd-marker{stroke:#f97316;stroke-width:2;stroke-dasharray:3 2}
.gantt-nd-label{fill:#f97316;font:10px system-ui,-apple-system,sans-serif}
.timeline-rollup{display:flex;flex-wrap:wrap;gap:16px;margin:8px 0;font-size:.85rem;color:#cbd5e1}
.timeline-rollup .stat{display:flex;flex-direction:column;gap:2px}
.timeline-rollup .stat .label{font-size:.7rem;color:#94a3b8;text-transform:uppercase;letter-spacing:.04em}
.timeline-rollup .stat .value{font-size:1rem;color:#e2e8f0}
"#;

#[derive(Debug, Deserialize)]
pub(crate) struct WorkflowListParams {
    #[serde(default)]
    page: Option<i64>,
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    workflow_name: Option<String>,
    #[serde(default)]
    search_attr_key: Option<String>,
    #[serde(default)]
    search_attr_value: Option<String>,
    /// ISO 8601 / RFC 3339 lower bound on `started_at`.
    #[serde(default)]
    started_after: Option<String>,
    /// ISO 8601 / RFC 3339 upper bound on `started_at`.
    #[serde(default)]
    started_before: Option<String>,
    /// Free-text prefix/substring match on execution id (UUID string).
    #[serde(default)]
    exec_id_search: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct WorkflowDetailParams {
    /// Zero-based page index for the event timeline. Raw submitted text, not
    /// `i64`. A malformed value must reach the handler as text. It then
    /// falls back to page zero with a flash message. It must not abort the
    /// whole page at the `Query` extractor. See
    /// `parse_event_page_query_field`.
    #[serde(default)]
    event_page: Option<String>,
    /// Flash message to display at the top of the detail page.
    #[serde(default)]
    flash: Option<String>,
    /// Jump to the page containing this 1-based event number. Raw submitted
    /// text, not `i64`, for the same reason as `event_page`.
    #[serde(default)]
    jump_event: Option<String>,
    /// Level filter for the durable workflow-logs panel (issue #790):
    /// `info` | `warn` | `error`. Absent or unrecognised means "all levels".
    #[serde(default)]
    log_level: Option<String>,
}

// ---------------------------------------------------------------------------
// Workflow detail action form structs
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct WorkflowCancelForm {
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct WorkflowPauseForm {
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct WorkflowTerminateForm {
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WorkflowSignalForm {
    signal_name: String,
    #[serde(default)]
    payload: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WorkflowResetForm {
    /// Raw submitted text, not `i64`. A malformed value must reach the
    /// handler as text. It then redisplays as a flash error, instead of
    /// aborting the request at the `Form` extractor. See
    /// `parse_reset_to_event_id`.
    #[serde(default)]
    reset_to_event_id: String,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WorkflowTriggerUpdateForm {
    update_name: String,
    #[serde(default)]
    payload: Option<String>,
}

// ---------------------------------------------------------------------------
// Blocked-on panel data
// ---------------------------------------------------------------------------

struct BlockedOnData {
    activities: Vec<TaskQueueItem>,
    external_tasks: Vec<ExternalTask>,
    timers: Vec<HarvestTimer>,
    signals: Vec<HarvestSignal>,
    /// The replay-derived open-awaitables report (issue #615) — the SAME
    /// report `GET /workflows/{id}/awaitables` serves (built by
    /// `crate::api::build_awaitables_report`), so the UI panel and the API can
    /// never disagree about what the run is parked on. Unlike the side-table
    /// lists above it also names awaited-but-UNSENT signals, pending child
    /// workflows, and `await_condition` parks. `None` for terminal executions
    /// or when the report could not be built (best-effort — the page still
    /// renders the side-table inventory).
    awaitables: Option<crate::api::WorkflowAwaitablesResponse>,
    /// Default response-side byte cap applied to a pending activity's heartbeat
    /// checkpoint payload before rendering (global #252 activity-result cap,
    /// #503). `0` = uncapped. Used when no per-activity override applies.
    heartbeat_details_cap: u64,
    /// Per-activity effective heartbeat checkpoint cap, keyed by activity name
    /// (per-activity `max_result_bytes` raised against the global ceiling),
    /// matching the API stack handler so configured large-payload activities
    /// keep full checkpoint visibility (#503 review). Missing names fall back to
    /// `heartbeat_details_cap`.
    heartbeat_caps: std::collections::HashMap<String, u64>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct WorkerListParams {
    #[serde(default)]
    page: Option<i64>,
    #[serde(default)]
    limit: Option<i64>,
    /// Filter by lifecycle status: `Active`, `Draining`, or `Stopped`.
    #[serde(default)]
    status: Option<String>,
    /// Filter by source shard id.
    #[serde(default)]
    shard: Option<String>,
    /// Set to `"true"` to show only stale workers.
    #[serde(default)]
    stale: Option<String>,
    /// Filter by build ID (exact match).
    #[serde(default)]
    build_id: Option<String>,
    /// Auto-refresh interval in seconds (emits a `<meta http-equiv="refresh">` tag).
    #[serde(default)]
    refresh: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct BuildRoutingListParams {
    /// Flash message forwarded after a form action redirect.
    #[serde(default)]
    flash: Option<String>,
    /// When set, filter tables to entries related to this build ID.
    #[serde(default)]
    build_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BuildRoutingSetPolicyForm {
    queue_name: String,
    build_id: String,
    #[serde(default)]
    deployment_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BuildRoutingCompatForm {
    build_id: String,
    compatible_with: String,
}

#[derive(Debug, Deserialize)]
struct BuildRoutingRetireForm {
    build_id: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct DeadLetterListParams {
    #[serde(default)]
    page: Option<i64>,
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    workflow_name: Option<String>,
    #[serde(default)]
    task_kind: Option<String>,
    #[serde(default)]
    failed_after: Option<String>,
    #[serde(default)]
    failed_before: Option<String>,
    #[serde(default)]
    shard_id: Option<String>,
    #[serde(default)]
    refresh: Option<u64>,
    #[serde(default)]
    flash: Option<String>,
    /// `summary` switches to the root-cause aggregation view (issue #385).
    #[serde(default)]
    view: Option<String>,
    /// Comma-separated grouping dimensions for the summary view.
    #[serde(default)]
    group_by: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeadLetterTaskKind {
    Activity,
    Workflow,
}

impl DeadLetterTaskKind {
    fn parse(raw: &str) -> Result<Self, AutumnError> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "activity" => Ok(Self::Activity),
            "workflow" => Ok(Self::Workflow),
            other => Err(AutumnError::bad_request_msg(format!(
                "unknown task_kind '{other}'; expected Activity or Workflow"
            ))),
        }
    }

    const fn as_db_value(self) -> &'static str {
        match self {
            Self::Activity => "ACTIVITY",
            Self::Workflow => "WORKFLOW",
        }
    }

    const fn as_label(self) -> &'static str {
        match self {
            Self::Activity => "Activity",
            Self::Workflow => "Workflow",
        }
    }
}

#[derive(Debug, Clone, Default)]
struct DeadLetterUiFilters {
    workflow_name: Option<String>,
    task_kind: Option<DeadLetterTaskKind>,
    failed_after: Option<DateTime<Utc>>,
    failed_before: Option<DateTime<Utc>>,
    shard_id: Option<i32>,
}

impl DeadLetterUiFilters {
    const fn is_empty(&self) -> bool {
        self.workflow_name.is_none()
            && self.task_kind.is_none()
            && self.failed_after.is_none()
            && self.failed_before.is_none()
            && self.shard_id.is_none()
    }
}

#[derive(Debug, Clone)]
struct DeadLetterUiRow {
    shard_id: ShardId,
    dead_letter: DeadLetter,
    workflow_name: Option<String>,
    events: Vec<HarvestEvent>,
}

// ---------------------------------------------------------------------------
// Per-shard query result (Ok = rows, Err = error message)
// ---------------------------------------------------------------------------

type ShardWorkerResult = (ShardId, Result<Vec<WorkerRow>, String>);
type ShardDeadLetterResult = (ShardId, Result<Vec<DeadLetterUiRow>, String>);

// ---------------------------------------------------------------------------
// Internal fleet stats computed from the full unfiltered worker list
// ---------------------------------------------------------------------------

struct WorkerFleetStats {
    total: usize,
    active: usize,
    draining: usize,
    stopped: usize,
    stale: usize,
    any_shard_errored: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BannerState {
    Healthy,
    Degraded,
    Unhealthy,
}

impl BannerState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "Healthy",
            Self::Degraded => "Degraded",
            Self::Unhealthy => "Unhealthy",
        }
    }
}

fn compute_fleet_stats(shard_results: &[ShardWorkerResult]) -> WorkerFleetStats {
    let any_shard_errored = shard_results.iter().any(|(_, r)| r.is_err());
    let mut total = 0usize;
    let mut active = 0usize;
    let mut draining = 0usize;
    let mut stopped = 0usize;
    let mut stale = 0usize;

    for (_, result) in shard_results {
        if let Ok(rows) = result {
            for row in rows {
                total += 1;
                match row.worker.status.as_str() {
                    "Active" => active += 1,
                    "Draining" => draining += 1,
                    _ => stopped += 1,
                }
                if row.health == WorkerHealth::Stale {
                    stale += 1;
                }
            }
        }
    }

    WorkerFleetStats {
        total,
        active,
        draining,
        stopped,
        stale,
        any_shard_errored,
    }
}

const fn determine_banner_state(stats: &WorkerFleetStats) -> Option<BannerState> {
    if stats.total == 0 && !stats.any_shard_errored {
        return None;
    }
    // Shard errors mean partial visibility — we can't rule out active workers
    // on the unreachable shard, so cap severity at Degraded.
    if stats.any_shard_errored {
        return Some(BannerState::Degraded);
    }
    if stats.active == 0 {
        return Some(BannerState::Unhealthy);
    }
    if stats.stale > 0 {
        return Some(BannerState::Degraded);
    }
    Some(BannerState::Healthy)
}

// Numeric sort key for a worker: (status_rank, is_healthy, worker_id).
// Stale workers sort before healthy within the same status bucket.
fn worker_sort_key(row: &WorkerRow) -> (u8, u8, &str) {
    let status_rank = match row.worker.status.as_str() {
        "Active" => 0,
        "Draining" => 1,
        _ => 2,
    };
    let health_rank = u8::from(row.health != WorkerHealth::Stale);
    (status_rank, health_rank, row.worker.worker_id.as_str())
}

/// Build the Vantage dashboard router.
pub fn harvest_ui_router(api_state: HarvestApiState) -> Router<AppState> {
    let require_admin = middleware::from_fn_with_state(api_state.clone(), require_harvest_admin);

    Router::new()
        .route("/", get(index))
        .route("/dags", get(list_dags_ui))
        .route("/dags/{dag_name}", get(dag_detail_ui))
        // issue #957: DAG-run-graph retry flow (dry-run confirm → admin commit).
        // Both surface a single audited mutation via the extracted
        // `retry_dag_run_inner`, so the UI introduces no unaudited path; both
        // are admin-gated to match the API's admin-auth posture.
        .route(
            "/dags/{dag_name}/runs/{run_exec_id}/retry",
            get(dag_retry_confirm_ui)
                .post(dag_retry_commit_ui)
                .route_layer(require_admin.clone()),
        )
        .route("/workflows", get(list_workflows_ui))
        .route("/workflows/{id}", get(workflow_detail_ui))
        // issue #960: standalone execution timeline / Gantt view (read-only,
        // non-admin — parity with the #739 API and the detail page).
        .route("/workflows/{id}/timeline", get(workflow_timeline_ui))
        .route("/workflows/{id}/cancel", post(cancel_workflow_ui))
        .route(
            "/workflows/{id}/terminate",
            post(terminate_workflow_ui).route_layer(require_admin.clone()),
        )
        .route("/workflows/{id}/pause", post(pause_workflow_ui))
        .route("/workflows/{id}/resume", post(resume_workflow_ui))
        .route("/workflows/{id}/signal", post(signal_workflow_ui))
        .route("/workflows/{id}/reset", post(reset_workflow_ui))
        .route("/workflows/{id}/trigger-update", post(trigger_update_ui))
        .route("/workers", get(list_workers_ui))
        .route(
            "/dead-letters",
            get(list_dead_letters_ui).route_layer(require_admin.clone()),
        )
        .route("/build-routing", get(list_build_routing_ui))
        .route(
            "/build-routing/set-policy",
            post(build_routing_set_policy_ui).route_layer(require_admin.clone()),
        )
        .route(
            "/build-routing/declare-compat",
            post(build_routing_declare_compat_ui).route_layer(require_admin.clone()),
        )
        .route(
            "/build-routing/revoke-compat",
            post(build_routing_revoke_compat_ui).route_layer(require_admin.clone()),
        )
        .route(
            "/build-routing/retire",
            post(build_routing_retire_ui).route_layer(require_admin.clone()),
        )
        .route("/schedules", get(list_schedules_ui))
        .route("/schedules/bulk-pause", post(schedule_bulk_pause_ui))
        .route("/schedules/bulk-resume", post(schedule_bulk_resume_ui))
        .route("/schedules/{id}/pause", post(schedule_pause_ui))
        .route("/schedules/{id}/resume", post(schedule_resume_ui))
        .route("/schedules/{id}/delete", post(schedule_delete_ui))
        .route("/schedules/{id}/trigger-now", post(schedule_trigger_now_ui))
        // issue #951: schedule drill-downs. Each is a presentation slice over an
        // already-shipped endpoint, and each mirrors that endpoint's admin-auth
        // posture: `GET /admin/schedules/{id}/runs` is the one admin-gated
        // schedule read route, so the run history is gated here too; preview and
        // backfill are not gated, matching their ungated API routes.
        .route("/schedules/{id}/preview", get(schedule_preview_ui))
        .route(
            "/schedules/{id}/runs",
            get(schedule_runs_ui).route_layer(require_admin.clone()),
        )
        .route(
            "/schedules/{id}/backfill",
            get(schedule_backfill_form_ui).post(schedule_backfill_ui),
        )
        // issue #377: admission gates UI page and one-click lift (lift requires admin)
        .route("/admin/gates", get(list_gates_ui))
        .route(
            "/admin/gates/{id}/lift",
            post(lift_gate_ui).route_layer(require_admin),
        )
        .layer(Extension(api_state))
}

async fn index() -> axum::response::Redirect {
    axum::response::Redirect::to("workflows")
}

#[derive(Clone)]
struct DagUiSummary {
    name: String,
    schedule_expr: Option<String>,
    task_count: usize,
    is_paused: bool,
    next_run_at: Option<DateTime<Utc>>,
    max_active_runs: i32,
    catchup: bool,
}

#[derive(Debug, Deserialize, Default)]
struct DagDetailParams {
    #[serde(default)]
    run: Option<String>,
    #[serde(default)]
    node: Option<usize>,
    #[serde(default)]
    refresh: Option<u64>,
    #[serde(default)]
    flash: Option<String>,
}

async fn list_dags_ui(
    Extension(api_state): Extension<HarvestApiState>,
) -> Result<Markup, AutumnError> {
    let runtime = api_state.runtime().map_err(map_error)?;
    let schedules = load_schedules_from_shards_ui(&api_state).await;
    let mut dags: HashMap<String, DagUiSummary> = runtime
        .dags()
        .iter()
        .map(|(name, dag)| (name.clone(), dag_summary_from_registered(name, dag)))
        .collect();
    let mut shard_errors = Vec::new();

    for (shard_id, shard_result) in schedules {
        match shard_result {
            Ok(rows) => {
                for row in rows {
                    let Some(dag_name) = row.dag_name.clone() else {
                        continue;
                    };
                    let entry = dags
                        .entry(dag_name.clone())
                        .or_insert_with(|| DagUiSummary {
                            name: dag_name.clone(),
                            schedule_expr: row.schedule_expr.clone(),
                            task_count: 0,
                            is_paused: row.is_paused,
                            next_run_at: row.next_run_at,
                            max_active_runs: row.max_active_runs,
                            catchup: row.catchup,
                        });
                    merge_dag_schedule_row(entry, &row);
                }
            }
            Err(error) => shard_errors.push((shard_id, error)),
        }
    }
    let mut dags = dags.into_values().collect::<Vec<_>>();
    dags.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(render_dag_list(&dags, &shard_errors))
}

async fn dag_detail_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Path(dag_name): Path<String>,
    Query(params): Query<DagDetailParams>,
) -> Result<Markup, AutumnError> {
    let runtime = api_state.runtime().map_err(map_error)?;
    let dag =
        runtime.dags().get(&dag_name).cloned().ok_or_else(|| {
            AutumnError::not_found_msg(format!("DAG '{dag_name}' is not registered"))
        })?;
    let filters = WorkflowFilters {
        workflow_name: Some(dag_name.clone()),
        limit: 50,
        ..WorkflowFilters::default()
    };
    let runs = load_dag_runs_from_owning_shard(&api_state, &runtime, &dag_name, &filters).await?;
    // Resolve which run to render. A `?run=` that is present-but-unknown is a
    // 404 (issue #957 AC7: "unknown run ids render the 404 message"), NOT a
    // silent fallback to a different run. An omitted `?run=` defaults to the
    // latest run. The DB existence check only runs for a parseable run id that
    // isn't already in the displayed page.
    // A parseable run id counts as valid only when it names a run of *this* DAG
    // (in the displayed page, or on the owning shard). An unparseable or unknown
    // run id leaves this `None` — the resolution below maps a present-but-unknown
    // `?run=` to a 404.
    let parsed_run = params
        .run
        .as_deref()
        .and_then(|raw| uuid::Uuid::parse_str(raw).ok());
    let requested_valid_run = match parsed_run {
        Some(run_id)
            if runs.iter().any(|run| run.id == run_id)
                || dag_run_exists_for_dag(&api_state, &dag_name, run_id).await? =>
        {
            Some(run_id)
        }
        _ => None,
    };
    let Ok(selected_run) = resolve_dag_run_selection(
        params.run.is_some(),
        requested_valid_run,
        runs.as_slice().first().map(|r| r.id),
    ) else {
        let raw = params.run.as_deref().unwrap_or_default();
        return Err(AutumnError::not_found_msg(format!(
            "run '{raw}' is not a run of DAG '{dag_name}'"
        )));
    };

    // Issue #957: render the node topology straight from
    // `dag_graph::build_run_graph` — the same in-process derivation the #690 API
    // handler uses — rather than a UI-side status fork. For a unified DAG with a
    // selected run, this loads the run's timestamped history and annotates the
    // registered `DagDefinition`; classic (non-unified) DAGs have no unified
    // topology and render the degraded message (matching #690's 400).
    let graph_data: Option<(Vec<DagRunNode>, String)> = if !dag.is_unified {
        None
    } else if let Some(run_uuid) = selected_run {
        let exec_id = autumn_harvest::types::ExecutionId::from_uuid(run_uuid);
        let mut conn = db_conn_for_execution(&api_state, exec_id).await?;
        let execution = load_execution(&mut conn, exec_id)
            .await
            .map_err(map_error)?;
        let timestamped = autumn_harvest::store::load_history_with_timestamps(&mut conn, exec_id)
            .await
            .map_err(map_error)?;
        // No further DB reads: release the pool slot before any payload-store
        // fetch, then decode so the issue #780 compensation filter inside
        // `build_run_graph` sees real payloads rather than opaque codec /
        // offload envelopes. Shared with the #690 API handler so the two
        // surfaces can never disagree. See `api::decode_graph_history`.
        drop(conn);
        let runtime = api_state.runtime().map_err(map_error)?;
        let codecs = api_state.payload_codecs();
        let offloader = runtime.registry().payload_offloader();
        let timestamped = crate::api::decode_graph_history(
            timestamped,
            exec_id,
            &codecs,
            offloader,
            "dag detail UI",
        )
        .await;
        let nodes = build_run_graph(&dag.definition, &timestamped, &execution.state);
        Some((nodes, execution.state))
    } else {
        None
    };

    let view = if !dag.is_unified {
        DagGraphView::Classic
    } else if let Some((nodes, run_state)) = graph_data.as_ref() {
        DagGraphView::Run {
            nodes: nodes.as_slice(),
            run_state: run_state.as_str(),
        }
    } else {
        DagGraphView::NoRun
    };

    Ok(render_dag_detail(
        &dag_name,
        &dag,
        &runs,
        selected_run,
        params.node,
        params.refresh,
        params.flash.as_deref(),
        view,
    ))
}

/// Sentinel: a `?run=` param was explicitly provided but names no run of this
/// DAG. The caller renders the 404 message (issue #957 AC7), never a silent
/// substitution of a different run.
#[derive(Debug)]
struct DagRunNotFound;

/// Resolve which DAG run to render from the `?run=` param state:
/// - **omitted** (`run_param_present == false`) → default to `fallback_run`
///   (the latest run), or `None` when the DAG has no runs.
/// - **present and valid** (`requested_valid_run == Some`) → that run.
/// - **present but unknown** (`run_param_present && requested_valid_run.is_none()`)
///   → `Err(DagRunNotFound)`, so an explicit unknown run id is never silently
///   swapped for a different run.
fn resolve_dag_run_selection(
    run_param_present: bool,
    requested_valid_run: Option<uuid::Uuid>,
    fallback_run: Option<uuid::Uuid>,
) -> Result<Option<uuid::Uuid>, DagRunNotFound> {
    if run_param_present {
        // Present: must resolve to a valid run of this DAG, else render 404.
        requested_valid_run.map_or(Err(DagRunNotFound), |run| Ok(Some(run)))
    } else {
        // Omitted: default to the latest run (or `None` when the DAG has none).
        Ok(fallback_run)
    }
}

fn dag_run_shard(router: &ShardRouter, dag_name: &str) -> ShardId {
    router.pick_for_dag(dag_name)
}

async fn load_dag_runs_from_owning_shard(
    api_state: &HarvestApiState,
    runtime: &HarvestApiRuntime,
    dag_name: &str,
    filters: &WorkflowFilters,
) -> Result<Vec<WorkflowExecution>, AutumnError> {
    let shard = dag_run_shard(runtime.router(), dag_name);
    let mut conn = db_conn_for_shard(api_state, shard).await?;
    load_workflows(&mut conn, filters).await.map_err(map_error)
}

async fn dag_run_exists_for_dag(
    api_state: &HarvestApiState,
    dag_name: &str,
    run_id: uuid::Uuid,
) -> Result<bool, AutumnError> {
    let mut conn = db_conn_for_execution(
        api_state,
        autumn_harvest::types::ExecutionId::from_uuid(run_id),
    )
    .await?;
    harvest_workflow_executions::table
        .find(run_id)
        .filter(harvest_workflow_executions::workflow_name.eq(dag_name))
        .select(harvest_workflow_executions::id)
        .first::<uuid::Uuid>(&mut conn)
        .await
        .optional()
        .map(|row| row.is_some())
        .map_err(database_error)
        .map_err(map_error)
}

// ── Issue #957 — DAG retry flow (dry-run confirm → admin commit) ─────────────

#[derive(Debug, Deserialize, Default)]
struct DagRetryConfirmParams {
    #[serde(default)]
    from_node: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DagRetryCommitForm {
    #[serde(default)]
    from_node: String,
    #[serde(default)]
    reason: String,
}

/// GET the retry confirm page: run a **dry-run** retry through the shared,
/// audited `retry_dag_run_inner` so the operator sees the authoritative widened
/// node list (`nodes_to_re_execute`) before committing. On any endpoint error
/// (400/404/409) the panel renders a human-readable message rather than raw
/// JSON. Admin-gated at the router.
async fn dag_retry_confirm_ui(
    Extension(api_state): Extension<HarvestApiState>,
    headers: axum::http::HeaderMap,
    Path((dag_name, run_exec_id)): Path<(String, String)>,
    Query(params): Query<DagRetryConfirmParams>,
) -> Result<Markup, AutumnError> {
    let from_node = params.from_node.unwrap_or_default();
    let actor = api_state.extract_actor(&headers);
    let reason = dag_retry_default_reason(&from_node);
    let outcome = retry_dag_run_inner(
        &api_state,
        &dag_name,
        &run_exec_id,
        &headers,
        vec![from_node.clone()],
        reason.clone(),
        actor,
        true,
        "GET /ui/dags/{dag_name}/runs/{run_exec_id}/retry",
        Some(SOURCE_UI),
    )
    .await;
    Ok(render_dag_retry_confirm(
        &dag_name,
        &run_exec_id,
        &from_node,
        &reason,
        outcome,
    ))
}

/// POST the retry commit: run the fork through `retry_dag_run_inner`
/// (`dry_run = false`), which writes the `OP_DAG_RETRY` audit row with
/// `source = ui`. Redirects back to the DAG page with a success flash naming
/// the new run. If the fork committed but the audit row failed to write
/// (a partial success), redirects to the *new* run with a warning flash rather
/// than misreporting it as a failure. A genuine failure (400/404/409) redirects
/// to the source run with a "Retry failed" flash. Admin-gated at the router.
async fn dag_retry_commit_ui(
    Extension(api_state): Extension<HarvestApiState>,
    headers: axum::http::HeaderMap,
    Path((dag_name, run_exec_id)): Path<(String, String)>,
    Form(form): Form<DagRetryCommitForm>,
) -> Result<axum::response::Response, AutumnError> {
    let actor = api_state.extract_actor(&headers);
    let reason = if form.reason.trim().is_empty() {
        dag_retry_default_reason(&form.from_node)
    } else {
        form.reason.clone()
    };
    let outcome = retry_dag_run_inner(
        &api_state,
        &dag_name,
        &run_exec_id,
        &headers,
        vec![form.from_node.clone()],
        reason,
        actor,
        false,
        "POST /ui/dags/{dag_name}/runs/{run_exec_id}/retry",
        Some(SOURCE_UI),
    )
    .await;
    let (target_run, flash_text) = dag_retry_commit_redirect(outcome, &run_exec_id);
    let flash = url_encode(&flash_text);
    let redirect_url = dag_detail_relative_url(&dag_name, &target_run, Some(&flash));
    Ok(axum::response::Redirect::to(&redirect_url).into_response())
}

/// Build a relative URL back to the DAG detail page from a retry route
/// (`/dags/{dag}/runs/{run}/retry`): `../../../{dag}?run={run}` (+ an optional
/// pre-encoded flash). Single-sources the relative depth so the confirm
/// back-link and the commit redirect can never drift.
fn dag_detail_relative_url(dag_name: &str, run: &str, flash: Option<&str>) -> String {
    let mut url = format!("../../../{}?run={}", url_encode(dag_name), url_encode(run));
    if let Some(flash) = flash {
        url.push_str("&flash=");
        url.push_str(flash);
    }
    url
}

/// Render the retry confirm page from a dry-run outcome: on success, the
/// widened re-execute list + carried-over list + an editable required reason and
/// a Confirm form (`POSTing` to the same URL); on failure, the human message.
fn render_dag_retry_confirm(
    dag_name: &str,
    run_exec_id: &str,
    from_node: &str,
    default_reason: &str,
    outcome: Result<DagRetryResponse, DagRetryFailure>,
) -> Markup {
    let body = match outcome {
        Ok(plan) => html! {
            h2 { "Confirm retry of DAG " code { (dag_name) } }
            p { "Source run: " code { (run_exec_id) } }
            p { "Retry from node: " code { (from_node) } }
            div class="card" {
                h3 { "Nodes that will re-execute" }
                p class="detail-row" {
                    "The retry auto-widens to the node's full execution level plus its \
                     downstream closure. These nodes will re-run:"
                }
                ul {
                    @for node in &plan.nodes_to_re_execute {
                        li { code { (node) } }
                    }
                }
                @if !plan.nodes_carried_over.is_empty() {
                    h3 { "Nodes carried over" }
                    ul {
                        @for node in &plan.nodes_carried_over {
                            li { code { (node) } }
                        }
                    }
                }
            }
            form method="post" {
                input type="hidden" name="from_node" value=(from_node);
                p {
                    label {
                        "Reason (required) "
                        textarea name="reason" required[true] rows="2" cols="60" { (default_reason) }
                    }
                }
                button type="submit" class="btn reset" { "Confirm retry" }
            }
        },
        Err(failure) => html! {
            div class="banner Warning" { (failure.human_message()) }
            p {
                a class="back" href=(dag_detail_relative_url(dag_name, run_exec_id, None)) {
                    "← Back to run"
                }
            }
        },
    };
    layout_dag_detail(
        &format!("Retry DAG {dag_name} · Vantage"),
        &body,
        "../../../../",
        None,
    )
}

#[allow(clippy::too_many_lines)]
async fn list_workflows_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Query(params): Query<WorkflowListParams>,
) -> Result<Markup, AutumnError> {
    let limit = params
        .limit
        .unwrap_or(DEFAULT_PAGE_SIZE)
        .clamp(1, MAX_PAGE_SIZE);
    let page = params.page.unwrap_or(0).max(0);
    let offset = page.saturating_mul(limit);

    let state_filter = params
        .state
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    let workflow_name_filter = params
        .workflow_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    let search_attr_key = params
        .search_attr_key
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let search_attr_value = params
        .search_attr_value
        .as_deref()
        .map(str::to_string)
        .unwrap_or_default();
    // Only enforce the search_attr predicate when the user supplied a key.
    // The value may legitimately be the empty string.
    let search_attr_pair = search_attr_key
        .as_ref()
        .map(|key| (key.clone(), search_attr_value.clone()));

    // Issue: a malformed started_after/started_before used to `?`-abort the
    // whole page (bare 400, no HTML) before the filter form was ever
    // rendered, discarding every other filter the operator had entered.
    // `parse_started_bound` instead degrades to "filter not applied" and
    // hands back the raw text plus an error to redisplay inline, so a typo
    // costs one field, not the page.
    let (started_after, started_after_raw, started_after_error) =
        parse_started_bound(params.started_after.as_deref(), "started_after");
    let (started_before, started_before_raw, started_before_error) =
        parse_started_bound(params.started_before.as_deref(), "started_before");
    let exec_id_search = params
        .exec_id_search
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_lowercase);

    let fetch_limit = offset.saturating_add(limit).saturating_add(1);
    let mut filters = WorkflowFilters::default().with_limit(fetch_limit);
    if let Some(state) = state_filter.as_deref() {
        filters.states.push(state.to_string());
    }
    filters.workflow_name.clone_from(&workflow_name_filter);
    if let Some((key, value)) = search_attr_pair.clone() {
        let mut object = serde_json::Map::with_capacity(1);
        object.insert(key, Value::String(value));
        filters.search_attrs.push(Value::Object(object));
    }
    filters.started_after = started_after;
    filters.started_before = started_before;
    filters.exec_id_prefix = exec_id_search.clone();

    // Issue #756: an unreachable shard degrades to a partial page rather than
    // failing the whole render; the UI shows the reachable-shard rows and a
    // banner naming the unreachable shard(s) so a partial list is not mistaken
    // for the authoritative fleet state.
    let fanout_page = load_workflows_from_shards(&api_state, &filters).await?;
    let unavailable_shards = fanout_page.unavailable_shards;
    let workflows = fanout_page.executions;

    let limit_usize = usize::try_from(limit).unwrap_or(usize::MAX);
    let offset_usize = usize::try_from(offset).unwrap_or(usize::MAX);
    let has_next = workflows.len() > offset_usize.saturating_add(limit_usize);
    let workflows = workflows
        .into_iter()
        .skip(offset_usize)
        .take(limit_usize)
        .collect::<Vec<_>>();

    // issue #377: check active gate count for the UI banner (no DB call — uses in-process cache).
    let active_gate_count = api_state.gate_cache().active_count();

    Ok(render_workflow_list(
        &workflows,
        page,
        limit,
        has_next,
        state_filter.as_deref(),
        workflow_name_filter.as_deref(),
        search_attr_pair.as_ref(),
        &started_after_raw,
        started_after_error.as_deref(),
        &started_before_raw,
        started_before_error.as_deref(),
        exec_id_search.as_deref(),
        active_gate_count,
        &unavailable_shards,
    ))
}

/// Parses an optional RFC 3339 `started_after`/`started_before` filter bound
/// from a raw query-string value. Returns `(parsed, raw_display, error)`:
/// on success `raw_display` echoes the canonical value and `error` is `None`;
/// on a parse failure `parsed` is `None` (the bound is not applied to the
/// query) while `raw_display` echoes exactly what the operator typed and
/// `error` carries a message to render next to the field — so a typo drops
/// one filter instead of the whole page (see `list_workflows_ui`).
fn parse_started_bound(
    raw: Option<&str>,
    field: &str,
) -> (Option<DateTime<Utc>>, String, Option<String>) {
    let Some(trimmed) = raw.map(str::trim).filter(|v| !v.is_empty()) else {
        return (None, String::new(), None);
    };
    DateTime::parse_from_rfc3339(trimmed).map_or_else(
        |_| {
            (
                None,
                trimmed.to_string(),
                Some(format!(
                    "Invalid {field} — expected RFC 3339, e.g. 2026-01-01T00:00:00Z. Filter not applied."
                )),
            )
        },
        |dt| (Some(dt.with_timezone(&Utc)), trimmed.to_string(), None),
    )
}

/// Parses an event-number query field (`event_page` or `jump_event`) on the
/// workflow detail page from its raw submitted text.
///
/// `WorkflowDetailParams` carries both fields as `String`, not `i64`. axum's
/// `Query` extractor runs full struct deserialization before
/// `workflow_detail_ui` ever executes. A field typed directly as `i64` would
/// abort the *entire* detail page on a non-numeric value: metadata,
/// timeline, every panel. The abort is a bare, unstyled 400, before the
/// handler reads anything. `jump_event` has a real operator-typed
/// `type="number"` input, the "Jump to event" control. A pasted or
/// hand-typed non-numeric value is therefore a reachable path, not a
/// hypothetical one. This closes the same page-abort mechanism
/// `parse_reset_to_event_id` already closes for the "Reset to event N"
/// action. It applies one layer earlier here: a GET page render, not a POST
/// action.
///
/// Returns `Ok(None)` when the field is absent or blank, `Ok(Some(n))` on a
/// valid whole number, and `Err(message)` on malformed text. The caller
/// falls back to page zero and surfaces the message through the page's
/// existing flash banner instead of aborting.
fn parse_event_page_query_field(field: &str, raw: Option<&str>) -> Result<Option<i64>, String> {
    let Some(trimmed) = raw.map(str::trim).filter(|v| !v.is_empty()) else {
        return Ok(None);
    };
    trimmed
        .parse::<i64>()
        .map(Some)
        .map_err(|_| format!("invalid {field} '{trimmed}'; expected a whole number"))
}

#[allow(clippy::too_many_lines)]
async fn workflow_detail_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id): Path<String>,
    Query(params): Query<WorkflowDetailParams>,
    headers: axum::http::HeaderMap,
    maybe_session: Option<Extension<Session>>,
) -> Result<Markup, AutumnError> {
    let exec_id = parse_execution_id(&id)?;
    let exec_uuid = exec_id.as_uuid();
    let mut conn = db_conn_for_execution(&api_state, exec_id).await?;
    let execution = load_execution(&mut conn, exec_id)
        .await
        .map_err(map_error)?;

    // Resolve event_page before any DB queries so we can use OFFSET/LIMIT
    // directly. A malformed `jump_event`/`event_page` falls back to page
    // zero with a flash message, instead of aborting the page. See
    // `parse_event_page_query_field`.
    let page_size = DETAIL_EVENT_PAGE_SIZE;
    let mut page_param_error: Option<String> = None;
    let event_page = match parse_event_page_query_field("jump_event", params.jump_event.as_deref())
    {
        Ok(Some(jump)) => {
            let jump_zero = (jump - 1).max(0);
            jump_zero / page_size
        }
        Ok(None) => {
            match parse_event_page_query_field("event_page", params.event_page.as_deref()) {
                Ok(page) => page.unwrap_or(0).max(0),
                Err(e) => {
                    page_param_error = Some(e);
                    0
                }
            }
        }
        Err(e) => {
            page_param_error = Some(e);
            0
        }
    };

    // Total event count — used for pagination controls.
    let total_events: i64 = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_uuid))
        .count()
        .get_result(&mut conn)
        .await
        .map_err(database_error)
        .map_err(map_error)?;

    // Page of events for the timeline — only the current page is fetched.
    let page_offset = event_page.saturating_mul(page_size);
    let page_events: Vec<HarvestEvent> = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_uuid))
        .order(harvest_events::event_id.asc())
        .offset(page_offset)
        .limit(page_size)
        .select(HarvestEvent::as_select())
        .load(&mut conn)
        .await
        .map_err(database_error)
        .map_err(map_error)?;

    // Activity-type events for the attempts panel. Heartbeats are excluded from
    // the type filter. We fetch the most recent ACTIVITY_PANEL_MAX_EVENTS rows
    // (DESC) and reverse them so collect_activity_attempts sees chronological
    // order; this ensures activities scheduled near the end of a long history
    // are never silently dropped by the cap.
    let mut activity_events: Vec<HarvestEvent> = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_uuid))
        .filter(harvest_events::event_type.eq_any(ACTIVITY_PANEL_EVENT_TYPES))
        .order(harvest_events::event_id.desc())
        .limit(ACTIVITY_PANEL_MAX_EVENTS)
        .select(HarvestEvent::as_select())
        .load(&mut conn)
        .await
        .map_err(database_error)
        .map_err(map_error)?;
    activity_events.reverse();

    // Signal/update events for the signals panel. Fetch DESC so the most recent
    // entries are kept when the panel is capped; reverse before rendering so the
    // table reads oldest→newest.
    let signal_update_events_raw: Vec<HarvestEvent> = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_uuid))
        .filter(harvest_events::event_type.eq_any(SIGNAL_UPDATE_TYPES))
        .order(harvest_events::event_id.desc())
        .limit(i64::try_from(SIGNAL_UPDATE_PANEL_LIMIT).unwrap_or(20) + 1)
        .select(HarvestEvent::as_select())
        .load(&mut conn)
        .await
        .map_err(database_error)
        .map_err(map_error)?;

    let signal_update_overflow = signal_update_events_raw.len() > SIGNAL_UPDATE_PANEL_LIMIT;
    // Take the most recent SIGNAL_UPDATE_PANEL_LIMIT entries (raw is DESC) and
    // restore chronological order for the panel table.
    let mut signal_update_events: Vec<HarvestEvent> = signal_update_events_raw
        .into_iter()
        .take(SIGNAL_UPDATE_PANEL_LIMIT)
        .collect();
    signal_update_events.reverse();

    // Load direct children (cap at 50 to avoid overwhelming the UI).
    let children: Vec<WorkflowExecution> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::parent_id.eq(Some(exec_uuid)))
        .order(harvest_workflow_executions::created_at.asc())
        .limit(50)
        .select(WorkflowExecution::as_select())
        .load(&mut conn)
        .await
        .map_err(database_error)
        .map_err(map_error)?;

    // Load blocked-on data for non-terminal workflows. Reuse the #252
    // activity-result payload cap as the response-side guard for heartbeat
    // checkpoints (#503); fall back to the default when no runtime is installed.
    let heartbeat_details_cap = api_state.runtime().ok().map_or(
        autumn_harvest::builder::DEFAULT_MAX_ACTIVITY_RESULT_BYTES,
        |r| r.registry().max_activity_result_bytes,
    );
    let mut blocked_on = load_blocked_on_data(
        &mut conn,
        exec_uuid,
        &execution.state,
        heartbeat_details_cap,
    )
    .await?;
    resolve_blocked_on_heartbeat_caps(&api_state, &mut blocked_on);

    // Resolve the continue-as-new threshold from the runtime registry if available.
    // This is a lightweight read of an in-memory value — no extra DB query.
    let continue_as_new_threshold = api_state
        .runtime()
        .ok()
        .map(|r| r.registry().history_policy().continue_as_new_threshold());

    // Read-path payload decoding (issue #608): decode the loaded copies only
    // (stored rows are never touched); one best-effort audit row per page
    // render that decoded or marked ≥1 envelope. Only the fields the page
    // actually renders are decoded — the attempts/signals panel event copies
    // (`activity_events`/`signal_update_events`) are deliberately excluded
    // since those panels never render payload fields (PR #936 review).
    let mut execution = execution;
    let mut page_events = page_events;
    let session = extension_session(maybe_session);
    decode_and_audit_workflow_detail(
        &api_state,
        &mut conn,
        &headers,
        session.clone(),
        exec_id,
        &mut execution,
        &mut page_events,
        &mut blocked_on,
    )
    .await;

    // Open-awaitables diagnostic (issue #615): re-source the blocked-on panel
    // from the SAME replay-derived report `GET /workflows/{id}/awaitables`
    // serves, so the UI and the API can never disagree about why a run is
    // parked. Gated on harvest-admin access to mirror the API route's
    // `require_admin` posture (the #608 `read_path_decoder` pattern in this
    // same handler) — a non-admin principal sees the side-table panels only,
    // never the replay-derived report the API would deny them. Best-effort: a
    // failure degrades to the side-table panels rather than failing the page,
    // with a warn so a persistent failure is diagnosable. The pooled
    // connection is dropped first so the report's own connection checkout
    // never overlaps ours (pool-size-1 safety).
    // Durable per-execution author log lines (issue #790, AC5). Admin-gated to
    // mirror `GET /workflows/{id}/logs`'s `require_admin` posture — a non-admin
    // principal must not read through the UI what the API would deny them.
    // Loaded on the page's own connection before it is dropped. Best-effort: a
    // failure hides the panel rather than failing the page (logs are
    // observational, AC7), with a warn so a persistent failure is diagnosable.
    let logs_admin = crate::api::has_harvest_admin_access(&api_state, session.clone()).await;
    let log_level_filter =
        autumn_harvest::WorkflowLogLevel::from_wire(params.log_level.as_deref().unwrap_or(""));
    let mut log_read_failed = false;
    let mut log_truncated = false;
    let log_lines: Vec<autumn_harvest::models::HarvestWorkflowLog> = if logs_admin {
        let level_refs: Vec<&str> = log_level_filter
            .map(autumn_harvest::WorkflowLogLevel::as_str)
            .into_iter()
            .collect();
        // Probe for the cap marker DIRECTLY rather than scanning the page. The
        // marker sits at `seq = i64::MAX` so it sorts last, and this panel loads
        // only the first `WORKFLOW_LOG_PANEL_LIMIT` rows -- under the default
        // 1,000-line cap a truncated run's marker can never land in a 200-row
        // page, so a scan would report "not truncated" for exactly the runs that
        // dropped the most. Mirrors the API route's own filter-independent probe.
        match autumn_harvest::store::load_workflow_logs(
            &mut conn,
            exec_id,
            &autumn_harvest::store::WorkflowLogQuery {
                after_seq: Some(autumn_harvest::store::WORKFLOW_LOG_TRUNCATION_SEQ - 1),
                limit: 1,
                ..Default::default()
            },
        )
        .await
        {
            Ok(rows) => log_truncated = !rows.is_empty(),
            Err(err) => {
                tracing::warn!(
                    execution_id = %exec_id,
                    error = ?err,
                    "workflow detail: durable log truncation probe failed"
                );
                log_read_failed = true;
            }
        }
        match autumn_harvest::store::load_workflow_logs(
            &mut conn,
            exec_id,
            &autumn_harvest::store::WorkflowLogQuery {
                levels: &level_refs,
                limit: WORKFLOW_LOG_PANEL_LIMIT,
                ..Default::default()
            },
        )
        .await
        {
            Ok(rows) => rows,
            Err(err) => {
                // Best-effort: never fail the page over an observational read.
                // But surface it AS a failure -- the empty state otherwise
                // affirmatively claims the sink is disabled, which during an
                // incident is the opposite of what happened.
                tracing::warn!(
                    execution_id = %exec_id,
                    error = ?err,
                    "workflow detail: durable log read failed"
                );
                log_read_failed = true;
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    drop(conn);
    if !is_terminal_workflow_state(&execution.state)
        && crate::api::has_harvest_admin_access(&api_state, session).await
    {
        blocked_on.awaitables = match crate::api::build_awaitables_report(&api_state, exec_id).await
        {
            Ok(report) => Some(report),
            Err(err) => {
                tracing::warn!(
                    execution_id = %exec_id,
                    error = ?err,
                    "workflow detail: awaitables report failed; falling back to side tables"
                );
                None
            }
        };
    }

    Ok(render_workflow_detail(
        &execution,
        total_events,
        &page_events,
        &activity_events,
        &signal_update_events,
        signal_update_overflow,
        &children,
        event_page,
        &blocked_on,
        // A malformed `jump_event`/`event_page` takes priority. It is the
        // reason this exact render fell back to page zero. That is more
        // relevant right now than a flash carried over from an earlier
        // redirect.
        page_param_error.as_deref().or(params.flash.as_deref()),
        continue_as_new_threshold,
        &WorkflowLogsPanelData {
            lines: &log_lines,
            level_filter: log_level_filter,
            admin: logs_admin,
            truncated: log_truncated,
            read_failed: log_read_failed,
        },
    ))
}

/// Decode only the workflow-detail fields the renderer actually displays
/// (PR #936 review, round 5): the execution row's payload fields (the
/// Input/Output/Memo/Search-attributes cards + the error banner), the
/// timeline page's event payloads (rendered in full under "view payload"),
/// and the blocked-on panel's heartbeat checkpoints. Hidden fields are
/// deliberately NOT decoded — the pending-activity `input`, pending-signal
/// payloads, and the attempts/signals panel event copies render only
/// names / error strings / timestamps, never payload fields, so decoding
/// them would burn codec/KMS work and count envelopes in the
/// `payload.decode_read` audit outcome for plaintext the operator is never
/// shown. Returns the merged outcome for the page's single audit row, so the
/// audit accounting covers exactly the surfaced fields.
fn decode_workflow_detail_rendered_fields(
    codecs: &PayloadCodecs,
    execution: &mut WorkflowExecution,
    timeline_events: &mut [HarvestEvent],
    blocked_on: &mut BlockedOnData,
) -> LossyDecodeOutcome {
    let mut outcome = decode_workflow_execution_fields(execution, codecs);
    for event in timeline_events.iter_mut() {
        outcome = outcome.merged(codecs.decode_value_lossy(&mut event.event_data));
    }
    for task in &mut blocked_on.activities {
        if let Some(checkpoint) = task.heartbeat_details.as_mut() {
            outcome = outcome.merged(codecs.decode_value_lossy(checkpoint));
        }
    }
    outcome
}

/// Read-path payload decoding for the workflow-detail page (issue #608):
/// resolves the decode-only-when-admin gate ([`read_path_decoder`]) and, when
/// active, tolerantly decodes the loaded copies of the rendered fields only
/// (see [`decode_workflow_detail_rendered_fields`]), then writes the page's
/// single best-effort audit row when ≥1 surfaced envelope was touched.
/// Operates on in-memory copies only; stored rows are untouched.
///
/// `conn` is the page handler's own (execution-shard) pooled connection —
/// the audit row is written through it rather than acquiring a second
/// connection while the caller's is still live (PR #936 review).
#[allow(clippy::too_many_arguments)]
async fn decode_and_audit_workflow_detail(
    api_state: &HarvestApiState,
    conn: &mut AsyncPgConnection,
    headers: &axum::http::HeaderMap,
    session: Option<Session>,
    exec_id: HarvestExecutionId,
    execution: &mut WorkflowExecution,
    timeline_events: &mut [HarvestEvent],
    blocked_on: &mut BlockedOnData,
) {
    let Some(codecs) = read_path_decoder(api_state, session).await else {
        return;
    };
    let outcome =
        decode_workflow_detail_rendered_fields(&codecs, execution, timeline_events, blocked_on);
    let target = exec_id.to_string();
    audit_decoded_read(
        api_state,
        Some(conn),
        headers,
        TARGET_WORKFLOW,
        Some(&target),
        "GET /ui/workflows/{id}",
        Some(exec_id.shard()),
        outcome,
        Some(SOURCE_UI),
    )
    .await;
}

fn is_terminal_workflow_state(state: &str) -> bool {
    matches!(
        state,
        "COMPLETED" | "FAILED" | "CANCELLED" | "TIMED_OUT" | "CONTINUED_AS_NEW" | "TERMINATED"
    )
}

async fn load_blocked_on_data(
    conn: &mut AsyncPgConnection,
    exec_uuid: uuid::Uuid,
    state: &str,
    heartbeat_details_cap: u64,
) -> Result<BlockedOnData, AutumnError> {
    if is_terminal_workflow_state(state) {
        return Ok(BlockedOnData {
            activities: vec![],
            external_tasks: vec![],
            timers: vec![],
            signals: vec![],
            heartbeat_details_cap,
            heartbeat_caps: std::collections::HashMap::new(),
            awaitables: None,
        });
    }

    let activities: Vec<TaskQueueItem> = harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_uuid)))
        .filter(
            harvest_task_queue::state
                .eq("PENDING")
                .or(harvest_task_queue::state.eq("CLAIMED"))
                .or(harvest_task_queue::state.eq("RUNNING"))
                .or(harvest_task_queue::state.eq("BACKOFF")),
        )
        .filter(harvest_task_queue::task_type.eq("activity"))
        // Stable ordering (id tiebreaker) so the per-page checkpoint budget
        // (#503) keeps/omits the same checkpoints deterministically.
        .order((
            harvest_task_queue::scheduled_at.asc(),
            harvest_task_queue::id.asc(),
        ))
        .limit(20)
        .select(TaskQueueItem::as_select())
        .load::<TaskQueueItem>(conn)
        .await
        .map_err(database_error)
        .map_err(map_error)?;

    let timers: Vec<HarvestTimer> = harvest_timers::table
        .filter(harvest_timers::workflow_exec_id.eq(exec_uuid))
        .filter(harvest_timers::fired.eq(false))
        .order(harvest_timers::fires_at.asc())
        .limit(20)
        .select(HarvestTimer::as_select())
        .load(conn)
        .await
        .map_err(database_error)
        .map_err(map_error)?;

    let signals: Vec<HarvestSignal> = harvest_signals::table
        .filter(harvest_signals::workflow_exec_id.eq(exec_uuid))
        .filter(harvest_signals::consumed.eq(false))
        .order(harvest_signals::received_at.asc())
        .limit(20)
        .select(HarvestSignal::as_select())
        .load(conn)
        .await
        .map_err(database_error)
        .map_err(map_error)?;

    // External activities awaiting a third-party result (state = 'PENDING').
    let external_tasks: Vec<ExternalTask> = harvest_external_tasks::table
        .filter(harvest_external_tasks::workflow_exec_id.eq(exec_uuid))
        .filter(harvest_external_tasks::state.eq("PENDING"))
        .order(harvest_external_tasks::created_at.asc())
        .limit(20)
        .select(ExternalTask::as_select())
        .load(conn)
        .await
        .map_err(database_error)
        .map_err(map_error)?;

    Ok(BlockedOnData {
        activities,
        external_tasks,
        timers,
        signals,
        heartbeat_details_cap,
        heartbeat_caps: std::collections::HashMap::new(),
        awaitables: None,
    })
}

/// Resolve each pending activity's effective heartbeat checkpoint cap
/// (per-activity `max_result_bytes` raised against the global ceiling),
/// mirroring the stack API so a configured large-payload activity keeps full
/// checkpoint visibility in the UI too (#503 review). No-op when no runtime is
/// installed (the global default cap then applies at render time).
fn resolve_blocked_on_heartbeat_caps(api_state: &HarvestApiState, blocked_on: &mut BlockedOnData) {
    if let Ok(rt) = api_state.runtime() {
        let registry = rt.registry();
        blocked_on.heartbeat_caps = blocked_on
            .activities
            .iter()
            .filter_map(|t| t.activity_name.clone())
            .map(|name| {
                let cap = registry.activity_result_cap(&name);
                (name, cap)
            })
            .collect();
    }
}

// ---------------------------------------------------------------------------
// Workflow UI action handlers
// ---------------------------------------------------------------------------

async fn cancel_workflow_ui(
    Extension(api_state): Extension<HarvestApiState>,
    headers: axum::http::HeaderMap,
    Path(id): Path<String>,
    Form(form): Form<WorkflowCancelForm>,
) -> Result<axum::response::Response, AutumnError> {
    let exec_id = parse_execution_id(&id)?;
    let mut conn = db_conn_for_execution(&api_state, exec_id).await?;
    let actor = api_state.extract_actor(&headers);
    let exec_id_str = exec_id.as_uuid().to_string();
    let reason = form.reason.as_deref().unwrap_or("").trim().to_string();

    let metrics_ref: Arc<dyn autumn_harvest::telemetry::MetricsRecorder> =
        api_state.runtime().map_or_else(
            |_| Arc::new(autumn_harvest::telemetry::NoOpMetrics) as _,
            |rt| Arc::clone(&rt.registry().telemetry().metrics),
        );
    let cancel_result =
        cancel_workflow_execution(&mut conn, exec_id, &reason, metrics_ref.as_ref()).await;
    let (status, error_summary, flash) = match &cancel_result {
        Ok(_) => (STATUS_SUCCEEDED, None, url_encode("Workflow cancelled")),
        Err(e) => {
            let msg = e.to_string();
            (
                STATUS_FAILED,
                Some(msg.clone()),
                url_encode(&format!("Cancel failed: {msg}")),
            )
        }
    };
    let _ = insert_audit(
        &mut conn,
        &NewAuditRecord {
            actor: &actor,
            operation: OP_WORKFLOW_CANCEL,
            target_type: TARGET_WORKFLOW,
            target_id: Some(&exec_id_str),
            route_or_command: "POST /workflows/{id}/cancel",
            request_id: None,
            idempotency_key: None,
            status,
            error_summary: error_summary.as_deref(),
            shard_id: None,
            source: SOURCE_UI,
        },
    )
    .await;

    let redirect_url = format!("../../workflows/{id}?flash={flash}");
    Ok(axum::response::Redirect::to(&redirect_url).into_response())
}

/// Force-terminate a single workflow execution from the detail page (issue #788).
///
/// Mirrors [`cancel_workflow_ui`] exactly — same shard resolution, actor
/// extraction, metrics fallback, audit plumbing, and detail-page redirect — but
/// delegates to the forceful [`terminate_workflow_execution`] core path (#504)
/// and records the `OP_WORKFLOW_TERMINATE` audit op.
async fn terminate_workflow_ui(
    Extension(api_state): Extension<HarvestApiState>,
    headers: axum::http::HeaderMap,
    Path(id): Path<String>,
    Form(form): Form<WorkflowTerminateForm>,
) -> Result<axum::response::Response, AutumnError> {
    let exec_id = parse_execution_id(&id)?;
    let mut conn = db_conn_for_execution(&api_state, exec_id).await?;
    let actor = api_state.extract_actor(&headers);
    let exec_id_str = exec_id.as_uuid().to_string();
    let reason = form.reason.as_deref().unwrap_or("").trim().to_string();

    let metrics_ref: Arc<dyn autumn_harvest::telemetry::MetricsRecorder> =
        api_state.runtime().map_or_else(
            |_| Arc::new(autumn_harvest::telemetry::NoOpMetrics) as _,
            |rt| Arc::clone(&rt.registry().telemetry().metrics),
        );
    let terminate_result =
        terminate_workflow_execution(&mut conn, exec_id, &reason, metrics_ref.as_ref()).await;
    let (status, error_summary, flash) = match &terminate_result {
        Ok(r) if r.newly_cancelled => (STATUS_SUCCEEDED, None, url_encode("Workflow terminated")),
        Ok(_) => (
            STATUS_SUCCEEDED,
            None,
            url_encode("Workflow was already terminal — no change made"),
        ),
        Err(e) => {
            let msg = e.to_string();
            (
                STATUS_FAILED,
                Some(msg.clone()),
                url_encode(&format!("Terminate failed: {msg}")),
            )
        }
    };
    if let Err(audit_err) = insert_audit(
        &mut conn,
        &NewAuditRecord {
            actor: &actor,
            operation: OP_WORKFLOW_TERMINATE,
            target_type: TARGET_WORKFLOW,
            target_id: Some(&exec_id_str),
            route_or_command: "POST /workflows/{id}/terminate",
            request_id: None,
            idempotency_key: None,
            status,
            error_summary: error_summary.as_deref(),
            shard_id: None,
            source: SOURCE_UI,
        },
    )
    .await
    {
        warn!(
            error = %audit_err,
            exec_id = %exec_id_str,
            "audit insert failed for workflow.terminate"
        );
    }

    let redirect_url = format!("../../workflows/{id}?flash={flash}");
    Ok(axum::response::Redirect::to(&redirect_url).into_response())
}

/// Flash text for the pause action: an idempotent repeat (`newly_paused:
/// false`) is called out so the operator isn't misled into thinking their
/// click performed the transition (issue #609 post-review hardening).
const fn pause_flash_message(newly_paused: bool) -> &'static str {
    if newly_paused {
        "Workflow paused"
    } else {
        "Workflow was already paused"
    }
}

/// Flash text for the resume action: since issue #609, resuming a non-paused
/// run is a success no-op (`newly_resumed: false`) — the flash must say so
/// rather than claiming a resume happened.
const fn resume_flash_message(newly_resumed: bool) -> &'static str {
    if newly_resumed {
        "Workflow resumed"
    } else {
        "Workflow was not paused; nothing to resume"
    }
}

async fn pause_workflow_ui(
    Extension(api_state): Extension<HarvestApiState>,
    headers: axum::http::HeaderMap,
    Path(id): Path<String>,
    Form(form): Form<WorkflowPauseForm>,
) -> Result<axum::response::Response, AutumnError> {
    let exec_id = parse_execution_id(&id)?;
    let mut conn = db_conn_for_execution(&api_state, exec_id).await?;
    let actor = api_state.extract_actor(&headers);
    let exec_id_str = exec_id.as_uuid().to_string();
    let reason = form
        .reason
        .as_deref()
        .map(str::trim)
        .filter(|r| !r.is_empty());

    let metrics_ref: Arc<dyn autumn_harvest::telemetry::MetricsRecorder> =
        api_state.runtime().map_or_else(
            |_| Arc::new(autumn_harvest::telemetry::NoOpMetrics) as _,
            |rt| Arc::clone(&rt.registry().telemetry().metrics),
        );
    let result =
        pause_workflow_execution(&mut conn, exec_id, reason, &actor, metrics_ref.as_ref()).await;
    let (status, error_summary, flash) = match &result {
        Ok(paused) => (
            STATUS_SUCCEEDED,
            None,
            url_encode(pause_flash_message(paused.newly_paused)),
        ),
        Err(e) => {
            let msg = e.to_string();
            (
                STATUS_FAILED,
                Some(msg.clone()),
                url_encode(&format!("Pause failed: {msg}")),
            )
        }
    };
    let _ = insert_audit(
        &mut conn,
        &NewAuditRecord {
            actor: &actor,
            operation: OP_WORKFLOW_PAUSE,
            target_type: TARGET_WORKFLOW,
            target_id: Some(&exec_id_str),
            route_or_command: "POST /workflows/{id}/pause",
            request_id: None,
            idempotency_key: None,
            status,
            error_summary: error_summary.as_deref(),
            shard_id: None,
            source: SOURCE_UI,
        },
    )
    .await;

    let redirect_url = format!("../../workflows/{id}?flash={flash}");
    Ok(axum::response::Redirect::to(&redirect_url).into_response())
}

async fn resume_workflow_ui(
    Extension(api_state): Extension<HarvestApiState>,
    headers: axum::http::HeaderMap,
    Path(id): Path<String>,
) -> Result<axum::response::Response, AutumnError> {
    let exec_id = parse_execution_id(&id)?;
    let mut conn = db_conn_for_execution(&api_state, exec_id).await?;
    let actor = api_state.extract_actor(&headers);
    let exec_id_str = exec_id.as_uuid().to_string();

    let metrics_ref: Arc<dyn autumn_harvest::telemetry::MetricsRecorder> =
        api_state.runtime().map_or_else(
            |_| Arc::new(autumn_harvest::telemetry::NoOpMetrics) as _,
            |rt| Arc::clone(&rt.registry().telemetry().metrics),
        );
    let result = resume_workflow_execution(&mut conn, exec_id, &actor, metrics_ref.as_ref()).await;
    let (status, error_summary, flash) = match &result {
        Ok(resumed) => (
            STATUS_SUCCEEDED,
            None,
            url_encode(resume_flash_message(resumed.newly_resumed)),
        ),
        Err(e) => {
            let msg = e.to_string();
            (
                STATUS_FAILED,
                Some(msg.clone()),
                url_encode(&format!("Resume failed: {msg}")),
            )
        }
    };
    let _ = insert_audit(
        &mut conn,
        &NewAuditRecord {
            actor: &actor,
            operation: OP_WORKFLOW_RESUME,
            target_type: TARGET_WORKFLOW,
            target_id: Some(&exec_id_str),
            route_or_command: "POST /workflows/{id}/resume",
            request_id: None,
            idempotency_key: None,
            status,
            error_summary: error_summary.as_deref(),
            shard_id: None,
            source: SOURCE_UI,
        },
    )
    .await;

    let redirect_url = format!("../../workflows/{id}?flash={flash}");
    Ok(axum::response::Redirect::to(&redirect_url).into_response())
}

async fn signal_workflow_ui(
    Extension(api_state): Extension<HarvestApiState>,
    headers: axum::http::HeaderMap,
    Path(id): Path<String>,
    Form(form): Form<WorkflowSignalForm>,
) -> Result<axum::response::Response, AutumnError> {
    let exec_id = parse_execution_id(&id)?;
    let mut conn = db_conn_for_execution(&api_state, exec_id).await?;
    let actor = api_state.extract_actor(&headers);
    let exec_id_str = exec_id.as_uuid().to_string();

    let payload_str = form.payload.as_deref().unwrap_or("").trim();
    let payload_result: Result<serde_json::Value, String> = if payload_str.is_empty() {
        Ok(serde_json::Value::Null)
    } else {
        serde_json::from_str(payload_str).map_err(|e| format!("Invalid JSON payload: {e}"))
    };
    let (status, error_summary, flash) = match payload_result {
        Err(e) => (STATUS_FAILED, Some(e.clone()), url_encode(&e)),
        Ok(payload_json) => {
            let cap = api_state
                .runtime()
                .ok()
                .map_or(0, |r| r.registry().max_signal_payload_bytes);
            let observed = serde_json::to_string(&payload_json).map_or(0, |s| s.len() as u64);
            if cap > 0 && observed > cap {
                let msg = format!(
                    "signal payload too large: {observed} bytes exceeds cap of {cap} bytes"
                );
                (STATUS_FAILED, Some(msg.clone()), url_encode(&msg))
            } else {
                match send_signal(&mut conn, exec_id, &form.signal_name, payload_json).await {
                    Ok(()) => (
                        STATUS_SUCCEEDED,
                        None,
                        url_encode(&format!("Signal '{}' sent", form.signal_name)),
                    ),
                    Err(e) => {
                        let msg = e.to_string();
                        (
                            STATUS_FAILED,
                            Some(msg.clone()),
                            url_encode(&format!("Signal failed: {msg}")),
                        )
                    }
                }
            }
        }
    };
    let _ = insert_audit(
        &mut conn,
        &NewAuditRecord {
            actor: &actor,
            operation: OP_WORKFLOW_SIGNAL,
            target_type: TARGET_WORKFLOW,
            target_id: Some(&exec_id_str),
            route_or_command: "POST /workflows/{id}/signal",
            request_id: None,
            idempotency_key: None,
            status,
            error_summary: error_summary.as_deref(),
            shard_id: None,
            source: SOURCE_UI,
        },
    )
    .await;

    let redirect_url = format!("../../workflows/{id}?flash={flash}");
    Ok(axum::response::Redirect::to(&redirect_url).into_response())
}

/// Parse the "Reset to event N" field (1-based, matching the timeline "#"
/// column) from its raw submitted text.
///
/// `WorkflowResetForm` types this field as `String`, not `i64`. axum's
/// `Form` extractor runs `serde` deserialization before the handler body
/// executes. A field typed directly as `i64` therefore rejects the whole
/// request with a bare, unstyled 400 on a non-numeric value. No HTML
/// renders, and the operator's entered reason is never read. That is the
/// same page-abort mechanism #1333/#1378/#1420/#1437 fixed for the list
/// pages' filter fields.
///
/// This form differs from those filters. It is not a filter; it is the
/// runbook's destructive recovery action. Operators use it for a stuck
/// child workflow or a non-determinism failure (`docs/vantage-ui.md`
/// scenarios 3 and 4). Parsing here keeps a malformed value inside the
/// handler. It then renders as the same flash-redirect error
/// `signal_workflow_ui` already produces for an invalid JSON payload.
///
/// Range and existence validation — does this event id exist on this
/// execution — stays downstream in `validate_reset_point`. This function
/// rejects only text that is not a whole number.
fn parse_reset_to_event_id(raw: &str) -> Result<i64, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("event number is required".to_string());
    }
    trimmed
        .parse::<i64>()
        .map_err(|_| format!("invalid event number '{trimmed}'; expected a whole number"))
}

async fn reset_workflow_ui(
    Extension(api_state): Extension<HarvestApiState>,
    headers: axum::http::HeaderMap,
    Path(id): Path<String>,
    Form(form): Form<WorkflowResetForm>,
) -> Result<axum::response::Response, AutumnError> {
    let exec_id = parse_execution_id(&id)?;
    let mut conn = db_conn_for_execution(&api_state, exec_id).await?;
    let actor = api_state.extract_actor(&headers);
    let exec_id_str = exec_id.as_uuid().to_string();

    let reason = form
        .reason
        .as_deref()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or("workflow reset requested")
        .to_string();

    // The form shows 1-based event numbers (matching the timeline "#" column).
    // The reset API accepts 0-based event IDs. A malformed value is rejected
    // here, inside the handler, instead of guessing an event number the
    // operator never typed. This mirrors the reject-rather-than-guess rule
    // #1437's bulk-action fix applied to a mutating endpoint.
    let reset_result = match parse_reset_to_event_id(&form.reset_to_event_id) {
        Ok(event_number) => {
            let request = WorkflowResetRequest {
                reset_to_event_id: Some(event_number.saturating_sub(1)),
                reset_point: None,
                reason,
                operator_id: actor.clone(),
                signal_reapply: ResetSignalReapplyPolicy::default(),
                allow_terminal_source: false,
                refuse_erased_source: false,
            };
            let runtime = api_state.runtime().ok();
            let registry = runtime.as_ref().map(|r| r.registry().as_ref());
            reset_workflow_execution(&mut conn, exec_id, request, registry)
                .await
                .map_err(|e| e.to_string())
        }
        Err(e) => Err(e),
    };
    let (status, error_summary, flash) = match &reset_result {
        Ok(result) => (
            STATUS_SUCCEEDED,
            None,
            url_encode(&format!(
                "Reset complete — new execution {}",
                result.new_exec_id
            )),
        ),
        Err(msg) => (
            STATUS_FAILED,
            Some(msg.clone()),
            url_encode(&format!("Reset failed: {msg}")),
        ),
    };
    let _ = insert_audit(
        &mut conn,
        &NewAuditRecord {
            actor: &actor,
            operation: OP_WORKFLOW_RESET,
            target_type: TARGET_WORKFLOW,
            target_id: Some(&exec_id_str),
            route_or_command: "POST /workflows/{id}/reset",
            request_id: None,
            idempotency_key: None,
            status,
            error_summary: error_summary.as_deref(),
            shard_id: None,
            source: SOURCE_UI,
        },
    )
    .await;

    let redirect_url = format!("../../workflows/{id}?flash={flash}");
    Ok(axum::response::Redirect::to(&redirect_url).into_response())
}

async fn trigger_update_ui(
    Extension(api_state): Extension<HarvestApiState>,
    headers: axum::http::HeaderMap,
    Path(id): Path<String>,
    Form(form): Form<WorkflowTriggerUpdateForm>,
) -> Result<axum::response::Response, AutumnError> {
    let exec_id = parse_execution_id(&id)?;
    let mut conn = db_conn_for_execution(&api_state, exec_id).await?;
    let actor = api_state.extract_actor(&headers);
    let exec_id_str = exec_id.as_uuid().to_string();

    let payload_str = form.payload.as_deref().unwrap_or("").trim();
    let payload_json: serde_json::Value = if payload_str.is_empty() {
        serde_json::Value::Null
    } else {
        match serde_json::from_str(payload_str) {
            Ok(v) => v,
            Err(e) => {
                let err_msg = format!("Invalid JSON payload: {e}");
                let _ = insert_audit(
                    &mut conn,
                    &NewAuditRecord {
                        actor: &actor,
                        operation: "workflow.update",
                        target_type: TARGET_WORKFLOW,
                        target_id: Some(&exec_id_str),
                        route_or_command: "POST /workflows/{id}/trigger-update",
                        request_id: None,
                        idempotency_key: None,
                        status: STATUS_FAILED,
                        error_summary: Some(&err_msg),
                        shard_id: None,
                        source: SOURCE_UI,
                    },
                )
                .await;
                let flash = url_encode(&err_msg);
                let redirect_url = format!("../../workflows/{id}?flash={flash}");
                return Ok(axum::response::Redirect::to(&redirect_url).into_response());
            }
        }
    };

    let update_id = UpdateId::new();
    // Resolve the engine recorder (if the runtime is installed) so
    // admit_update_event emits harvest.update.admitted post-commit (issue #684).
    let ui_runtime = api_state.runtime().ok();
    let ui_metrics = ui_runtime
        .as_ref()
        .map(|r| r.registry().telemetry().metrics.as_ref());
    let ui_codecs = api_state.payload_codecs();
    let (status, error_summary, flash) = match admit_update_event_with_codecs(
        &mut conn,
        exec_id,
        update_id,
        form.update_name.clone(),
        payload_json,
        ui_metrics,
        &ui_codecs,
    )
    .await
    {
        Ok(()) => {
            // Wake the workflow task so it picks up the admitted update immediately.
            // Surface any wake failure in the flash so the operator knows to retry.
            let wake_note =
                match autumn_harvest::queue::wake_workflow_task(&mut conn, exec_id).await {
                    Ok(()) => String::new(),
                    Err(e) => format!(" (wake failed: {e})"),
                };
            (
                STATUS_SUCCEEDED,
                None,
                url_encode(&format!(
                    "Update '{}' admitted{}",
                    form.update_name, wake_note
                )),
            )
        }
        Err(e) => {
            let msg = e.to_string();
            (
                STATUS_FAILED,
                Some(msg.clone()),
                url_encode(&format!("Update failed: {msg}")),
            )
        }
    };
    let _ = insert_audit(
        &mut conn,
        &NewAuditRecord {
            actor: &actor,
            operation: "workflow.update",
            target_type: TARGET_WORKFLOW,
            target_id: Some(&exec_id_str),
            route_or_command: "POST /workflows/{id}/trigger-update",
            request_id: None,
            idempotency_key: None,
            status,
            error_summary: error_summary.as_deref(),
            shard_id: None,
            source: SOURCE_UI,
        },
    )
    .await;

    let redirect_url = format!("../../workflows/{id}?flash={flash}");
    Ok(axum::response::Redirect::to(&redirect_url).into_response())
}

// ---------------------------------------------------------------------------
// Dead-letter UI
// ---------------------------------------------------------------------------

async fn list_dead_letters_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Query(params): Query<DeadLetterListParams>,
    headers: axum::http::HeaderMap,
    maybe_session: Option<Extension<Session>>,
) -> Result<Markup, AutumnError> {
    // Read-path payload decoding (issue #608): the page is admin-gated, so an
    // arriving request passes the same predicate the decoder re-checks.
    let decoder = read_path_decoder(&api_state, extension_session(maybe_session)).await;
    let limit = params
        .limit
        .unwrap_or(DEFAULT_DLQ_PAGE_SIZE)
        .clamp(1, MAX_PAGE_SIZE);
    let page = params.page.unwrap_or(0).max(0);
    let offset = page.saturating_mul(limit);
    let (filters, filter_raw) = parse_dead_letter_ui_filters(
        params.workflow_name.as_deref(),
        params.task_kind.as_deref(),
        params.failed_after.as_deref(),
        params.failed_before.as_deref(),
        params.shard_id.as_deref(),
    );

    let pool = api_state.storage_pool().map_err(map_error)?;

    // Summary toggle (issue #385): the root-cause aggregation view.
    if params.view.as_deref() == Some("summary") {
        return render_dead_letters_summary_view(
            &pool,
            &filters,
            &filter_raw,
            params.group_by.as_deref(),
            limit,
            params.refresh,
            params.flash.as_deref(),
        )
        .await;
    }

    let fetch_limit = offset.saturating_add(limit).saturating_add(1);
    let shard_results = load_dead_letters_from_shards_for_ui(&pool, &filters, fetch_limit).await;
    let is_multi_shard = shard_results.len() > 1;

    let mut all_rows: Vec<DeadLetterUiRow> = shard_results
        .iter()
        .flat_map(|(_, result)| result.iter().flat_map(|rows| rows.iter().cloned()))
        .collect();
    all_rows.sort_by(|left, right| {
        right
            .dead_letter
            .failed_at
            .cmp(&left.dead_letter.failed_at)
            .then_with(|| right.dead_letter.id.cmp(&left.dead_letter.id))
            .then_with(|| left.shard_id.as_i32().cmp(&right.shard_id.as_i32()))
    });

    let total_matching = count_dead_letters_from_shards_for_ui(&pool, &filters).await;
    let total_for_pagination = total_matching.unwrap_or(all_rows.len());
    let offset_usize = usize::try_from(offset).unwrap_or(usize::MAX);
    let limit_usize = usize::try_from(limit).unwrap_or(usize::MAX);
    let has_next = total_for_pagination > offset_usize.saturating_add(limit_usize);
    let mut page_rows = all_rows
        .into_iter()
        .skip(offset_usize)
        .take(limit_usize)
        .collect::<Vec<_>>();

    if let Some(codecs) = decoder.as_ref() {
        // Read-path payload decoding (issue #608): decode each rendered row's
        // JSONB input, TEXT error, and last-events copies; one best-effort
        // audit row per page render that touched ≥1 envelope.
        let mut outcome = LossyDecodeOutcome::default();
        for row in &mut page_rows {
            outcome = outcome.merged(codecs.decode_value_lossy(&mut row.dead_letter.input));
            outcome = outcome.merged(decode_error_field(codecs, &mut row.dead_letter.error));
            for event in &mut row.events {
                outcome = outcome.merged(codecs.decode_value_lossy(&mut event.event_data));
            }
        }
        // No live connection here: the per-shard DLQ loads are scoped inside
        // their `_from_shards_for_ui` helpers, so the pool-acquiring branch
        // is safe (PR #936 review).
        audit_decoded_read(
            &api_state,
            None,
            &headers,
            TARGET_DEAD_LETTER,
            None,
            "GET /ui/dead-letters",
            None,
            outcome,
            Some(SOURCE_UI),
        )
        .await;
    }
    let page_rows = page_rows;

    let shard_errors: Vec<(ShardId, &str)> = shard_results
        .iter()
        .filter_map(|(shard_id, result)| result.as_ref().err().map(|e| (*shard_id, e.as_str())))
        .collect();

    Ok(render_dead_letters_page(
        &filters,
        &filter_raw,
        &page_rows,
        &shard_errors,
        is_multi_shard,
        page,
        limit,
        has_next,
        total_for_pagination,
        params.refresh,
        params.flash.as_deref(),
    ))
}

/// Raw text and validation errors for the DLQ filter fields that can fail
/// to parse: `task_kind`, `failed_after`, `failed_before`. Carried alongside
/// `DeadLetterUiFilters`, which holds only the successfully parsed values.
/// This lets an invalid value's inline error and its exact typed text
/// persist. They survive the filter form, pagination, and the bulk-action
/// forms. Without this, they would revert the moment the request moves past
/// the initial submit. Same `(parsed, raw_display, error)` contract as
/// `parse_worker_status_filter` uses on the Workers page (#1378).
#[derive(Debug, Clone, Default)]
struct DeadLetterUiFilterRaw {
    task_kind: String,
    task_kind_error: Option<String>,
    failed_after: String,
    failed_after_error: Option<String>,
    failed_before: String,
    failed_before_error: Option<String>,
    shard_id: String,
    shard_id_error: Option<String>,
}

/// Parses the DLQ page's filters from raw query-string values. An
/// unrecognized `task_kind`, or an unparseable `failed_after`/`failed_before`,
/// used to `?`-abort the whole page. This happened before the filter form
/// ever rendered. It discarded whichever of the five filters the operator
/// had already typed. This now degrades each bad field to "not applied"
/// instead. It hands back the raw text plus an error to redisplay inline. A
/// bad value now costs one field, not the page. Same fix as
/// `parse_started_bound` (#1333) and `parse_worker_status_filter` (#1378)
/// use on the sibling list pages.
fn parse_dead_letter_ui_filters(
    workflow_name: Option<&str>,
    task_kind: Option<&str>,
    failed_after: Option<&str>,
    failed_before: Option<&str>,
    shard_id: Option<&str>,
) -> (DeadLetterUiFilters, DeadLetterUiFilterRaw) {
    let workflow_name = workflow_name
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let (task_kind, task_kind_raw, task_kind_error) = parse_dead_letter_task_kind_filter(task_kind);
    let (failed_after, failed_after_raw, failed_after_error) =
        parse_dead_letter_time_filter("failed_after", failed_after);
    let (failed_before, failed_before_raw, failed_before_error) =
        parse_dead_letter_time_filter("failed_before", failed_before);
    let (shard_id, shard_id_raw, shard_id_error) = parse_shard_id_filter("shard_id", shard_id);

    (
        DeadLetterUiFilters {
            workflow_name,
            task_kind,
            failed_after,
            failed_before,
            shard_id,
        },
        DeadLetterUiFilterRaw {
            task_kind: task_kind_raw,
            task_kind_error,
            failed_after: failed_after_raw,
            failed_after_error,
            failed_before: failed_before_raw,
            failed_before_error,
            shard_id: shard_id_raw,
            shard_id_error,
        },
    )
}

/// Parses the DLQ page's `task_kind` filter. Returns `(parsed, raw_display,
/// error)`. On an unrecognized value, `parsed` is `None`, so the filter is
/// not applied. `error` then carries a message to render next to the field.
/// `raw_display` echoes the operator's exact trimmed input. The caller uses
/// it to carry the value through pagination and resubmission.
fn parse_dead_letter_task_kind_filter(
    raw: Option<&str>,
) -> (Option<DeadLetterTaskKind>, String, Option<String>) {
    let Some(trimmed) = raw.map(str::trim).filter(|v| !v.is_empty()) else {
        return (None, String::new(), None);
    };
    match trimmed.to_ascii_lowercase().as_str() {
        "activity" => (
            Some(DeadLetterTaskKind::Activity),
            trimmed.to_string(),
            None,
        ),
        "workflow" => (
            Some(DeadLetterTaskKind::Workflow),
            trimmed.to_string(),
            None,
        ),
        other => (
            None,
            trimmed.to_string(),
            Some(format!(
                "Unknown task_kind '{other}'; expected Activity or Workflow. Filter not applied."
            )),
        ),
    }
}

/// Parses one of the DLQ page's `failed_after`/`failed_before` filters with
/// the same "filter not applied, raw input carried through, error
/// redisplayed inline" contract as [`parse_dead_letter_task_kind_filter`].
fn parse_dead_letter_time_filter(
    field: &str,
    raw: Option<&str>,
) -> (Option<DateTime<Utc>>, String, Option<String>) {
    let Some(trimmed) = raw.map(str::trim).filter(|v| !v.is_empty()) else {
        return (None, String::new(), None);
    };
    let Ok(parsed) = DateTime::parse_from_rfc3339(trimmed) else {
        return (
            None,
            trimmed.to_string(),
            Some(format!(
                "Invalid {field}; expected RFC 3339 timestamp. Filter not applied."
            )),
        );
    };
    (Some(parsed.with_timezone(&Utc)), trimmed.to_string(), None)
}

/// Parses a `shard`/`shard_id` filter shared by the Workers, Dead-Letters,
/// and Schedules list pages. Returns `(parsed, raw_display, error)` with the
/// same "filter not applied, raw input carried through, error redisplayed
/// inline" contract as [`parse_dead_letter_time_filter`].
///
/// Issue: on all three pages this field used to be typed `Option<i32>`
/// directly on the `Query<..>` extractor struct. Axum deserializes query
/// structs before the handler body runs, so a non-numeric value never
/// reached the page's own graceful-degradation code. It failed the
/// extractor itself instead, aborting the request with a bare framework
/// 400. That happened before any `HarvestApiState`, any HTML, or any of
/// the operator's other filters were even looked at. It is the same
/// page-abort defect the sibling string filters already fix, but one
/// layer earlier and with no styled error at all.
///
/// The fix retypes the field `Option<String>` on the params struct and
/// parses it here, like every other filter on these pages. That moves the
/// failure from the extractor into the handler, where it can degrade
/// gracefully. `field` names the query parameter in the error message,
/// since the pages spell it `shard` or `shard_id`.
fn parse_shard_id_filter(field: &str, raw: Option<&str>) -> (Option<i32>, String, Option<String>) {
    let Some(trimmed) = raw.map(str::trim).filter(|v| !v.is_empty()) else {
        return (None, String::new(), None);
    };
    let Ok(parsed) = trimmed.parse::<i32>() else {
        return (
            None,
            trimmed.to_string(),
            Some(format!(
                "Invalid {field} '{trimmed}'; expected a whole number. Filter not applied."
            )),
        );
    };
    (Some(parsed), trimmed.to_string(), None)
}

async fn load_dead_letters_from_shards_for_ui(
    pool: &crate::HarvestDbPool,
    filters: &DeadLetterUiFilters,
    limit: i64,
) -> Vec<ShardDeadLetterResult> {
    let futs: Vec<_> = pool
        .iter_shards()
        .map(|(shard_id, shard_pool)| async move {
            if filters
                .shard_id
                .is_some_and(|wanted| wanted != shard_id.as_i32())
            {
                return (shard_id, Ok(Vec::new()));
            }
            let result = async {
                let mut conn = acquire_conn(shard_pool).await.map_err(|e| e.to_string())?;
                let rows = query_dead_letters_for_ui(&mut conn, filters, limit)
                    .await
                    .map_err(|e| e.to_string())?;
                let mut out = Vec::with_capacity(rows.len());
                for dead_letter in rows {
                    let workflow_name = load_dead_letter_workflow_name(&mut conn, &dead_letter)
                        .await
                        .map_err(|e| e.to_string())?;
                    let events = load_dead_letter_events(&mut conn, &dead_letter)
                        .await
                        .map_err(|e| e.to_string())?;
                    out.push(DeadLetterUiRow {
                        shard_id,
                        dead_letter,
                        workflow_name,
                        events,
                    });
                }
                Ok(out)
            }
            .await;
            (shard_id, result)
        })
        .collect();
    futures::future::join_all(futs).await
}

async fn count_dead_letters_from_shards_for_ui(
    pool: &crate::HarvestDbPool,
    filters: &DeadLetterUiFilters,
) -> Result<usize, String> {
    let futs: Vec<_> = pool
        .iter_shards()
        .map(|(shard_id, shard_pool)| async move {
            if filters
                .shard_id
                .is_some_and(|wanted| wanted != shard_id.as_i32())
            {
                return Ok(0usize);
            }
            let mut conn = acquire_conn(shard_pool).await.map_err(|e| e.to_string())?;
            count_dead_letters_for_ui(&mut conn, filters)
                .await
                .map(|count| usize::try_from(count).unwrap_or(0))
                .map_err(|e| e.to_string())
        })
        .collect();
    let counts = futures::future::join_all(futs).await;
    counts.into_iter().try_fold(0usize, |acc, count| {
        count.map(|count| acc.saturating_add(count))
    })
}

macro_rules! apply_dead_letter_ui_filters {
    ($query:ident, $filters:expr) => {
        if let Some(ref workflow_name) = $filters.workflow_name {
            $query = $query.filter(
                sql::<Bool>("workflow_exec_id IN (SELECT id FROM harvest_workflow_executions WHERE workflow_name = ")
                    .bind::<Text, _>(workflow_name.clone())
                    .sql(")"),
            );
        }
        if let Some(task_kind) = $filters.task_kind {
            $query = $query.filter(
                sql::<Bool>("LOWER(task_type) = LOWER(")
                    .bind::<Text, _>(task_kind.as_db_value().to_string())
                    .sql(")"),
            );
        }
        if let Some(failed_after) = $filters.failed_after {
            $query = $query.filter(harvest_dead_letters::failed_at.ge(failed_after));
        }
        if let Some(failed_before) = $filters.failed_before {
            $query = $query.filter(harvest_dead_letters::failed_at.lt(failed_before));
        }
    };
}

async fn query_dead_letters_for_ui(
    conn: &mut AsyncPgConnection,
    filters: &DeadLetterUiFilters,
    limit: i64,
) -> HarvestResult<Vec<DeadLetter>> {
    let mut query = harvest_dead_letters::table
        .into_boxed()
        .order(harvest_dead_letters::failed_at.desc())
        .limit(limit);
    apply_dead_letter_ui_filters!(query, filters);
    query
        .select(DeadLetter::as_select())
        .load(conn)
        .await
        .map_err(database_error)
}

async fn count_dead_letters_for_ui(
    conn: &mut AsyncPgConnection,
    filters: &DeadLetterUiFilters,
) -> HarvestResult<i64> {
    let mut query = harvest_dead_letters::table.into_boxed();
    apply_dead_letter_ui_filters!(query, filters);
    query.count().get_result(conn).await.map_err(database_error)
}

async fn load_dead_letter_workflow_name(
    conn: &mut AsyncPgConnection,
    dead_letter: &DeadLetter,
) -> HarvestResult<Option<String>> {
    let Some(exec_id) = dead_letter.workflow_exec_id else {
        return Ok(None);
    };
    harvest_workflow_executions::table
        .find(exec_id)
        .select(harvest_workflow_executions::workflow_name)
        .first(conn)
        .await
        .optional()
        .map_err(database_error)
}

async fn load_dead_letter_events(
    conn: &mut AsyncPgConnection,
    dead_letter: &DeadLetter,
) -> HarvestResult<Vec<HarvestEvent>> {
    let Some(exec_id) = dead_letter.workflow_exec_id else {
        return Ok(Vec::new());
    };
    let mut events = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id))
        .order(harvest_events::event_id.desc())
        .limit(10)
        .select(HarvestEvent::as_select())
        .load(conn)
        .await
        .map_err(database_error)?;
    events.reverse();
    Ok(events)
}

// ---------------------------------------------------------------------------
// Workers UI
// ---------------------------------------------------------------------------

async fn list_workers_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Query(params): Query<WorkerListParams>,
) -> Result<Markup, AutumnError> {
    // Issue: an unrecognized status/stale value used to `?`-abort the whole
    // page (bare 400, no HTML) before the filter form was ever rendered,
    // discarding the build_id/shard filters the operator had already
    // entered. `parse_worker_status_filter`/`parse_worker_stale_filter`
    // instead degrade to "filter not applied" and hand back an error to
    // redisplay inline, so a bad value costs one field, not the page — same
    // fix as `parse_started_bound` on the Workflows page (#1333). The raw
    // text is carried alongside the parsed value so pagination and form
    // resubmission don't silently drop it (Codex review, #1378 P2).
    let (status_filter, status_raw, status_error) =
        parse_worker_status_filter(params.status.as_deref());
    let (stale_only, stale_raw, stale_error) = parse_worker_stale_filter(params.stale.as_deref());
    // Same fix, applied to the numeric `shard` filter. `shard` was still
    // typed `Option<i32>` directly on the `Query<..>` extractor struct. A
    // non-numeric value aborted with a bare framework 400 before this
    // handler ever ran. That is one layer earlier than the page-abort bug
    // status/stale already fix, with no styled error at all.
    let (shard_filter, shard_raw, shard_error) =
        parse_shard_id_filter("shard", params.shard.as_deref());

    let limit = params
        .limit
        .unwrap_or(DEFAULT_PAGE_SIZE)
        .clamp(1, MAX_PAGE_SIZE);
    let page = params.page.unwrap_or(0).max(0);
    let offset = page.saturating_mul(limit);

    let stale_threshold = api_state.worker_stale_threshold();
    let pool = api_state.storage_pool().map_err(map_error)?;

    // Load all workers without status filter so fleet stats reflect true fleet
    // state regardless of the active UI filter.
    let shard_results = load_workers_from_shards(&pool, None, stale_threshold).await;

    // Issue #619: paused queues are the most likely explanation for idle
    // workers alongside a full queue, so surface them on the fleet page.
    let paused_queues = load_paused_queues_from_shards(&api_state).await;

    let stats = compute_fleet_stats(&shard_results);
    let banner_state = determine_banner_state(&stats);
    let is_multi_shard = shard_results.len() > 1;

    let mut all_workers: Vec<(ShardId, WorkerRow)> = shard_results
        .iter()
        .flat_map(|(shard_id, result)| {
            let shard_id = *shard_id;
            result
                .iter()
                .flat_map(move |rows| rows.iter().map(move |r| (shard_id, r.clone())))
        })
        .filter(|(shard_id, row)| {
            if shard_filter.is_some_and(|f| shard_id.as_i32() != f) {
                return false;
            }
            if let Some(sf) = status_filter
                && row.worker.status != sf
            {
                return false;
            }
            if stale_only && row.health != WorkerHealth::Stale {
                return false;
            }
            if let Some(ref bf) = params.build_id
                && !bf.is_empty()
                && row.worker.build_id != *bf
            {
                return false;
            }
            true
        })
        .collect();

    all_workers.sort_by(|(sa, a), (sb, b)| {
        sa.as_i32()
            .cmp(&sb.as_i32())
            .then_with(|| worker_sort_key(a).cmp(&worker_sort_key(b)))
    });

    let total_filtered = all_workers.len();
    let offset_usize = usize::try_from(offset).unwrap_or(usize::MAX);
    let limit_usize = usize::try_from(limit).unwrap_or(usize::MAX);
    let has_next = total_filtered > offset_usize.saturating_add(limit_usize);
    let page_workers: Vec<(ShardId, WorkerRow)> = all_workers
        .into_iter()
        .skip(offset_usize)
        .take(limit_usize)
        .collect();

    let mut grouped: Vec<(ShardId, Vec<WorkerRow>)> = Vec::new();
    for (shard_id, row) in page_workers {
        match grouped.last_mut() {
            Some((sid, rows)) if *sid == shard_id => rows.push(row),
            _ => grouped.push((shard_id, vec![row])),
        }
    }

    let shard_errors: Vec<(ShardId, &str)> = shard_results
        .iter()
        .filter_map(|(shard_id, result)| result.as_ref().err().map(|e| (*shard_id, e.as_str())))
        .collect();

    let build_id_filter = params.build_id.as_deref().filter(|s| !s.is_empty());

    Ok(render_workers_page(
        &stats,
        banner_state,
        &paused_queues,
        &grouped,
        &shard_errors,
        is_multi_shard,
        page,
        limit,
        has_next,
        status_filter,
        &status_raw,
        status_error.as_deref(),
        &shard_raw,
        shard_error.as_deref(),
        stale_only,
        &stale_raw,
        stale_error.as_deref(),
        build_id_filter,
        params.refresh,
    ))
}

/// Parses the Workers page's `status` filter from a raw query-string value.
/// Returns `(parsed, raw_display, error)`: on success `error` is `None`; on
/// an unrecognized value `parsed` is `None` (the filter is not applied)
/// while `error` carries a message to render next to the field — so a bad
/// value drops one filter instead of the whole page (see `list_workers_ui`).
/// `raw_display` echoes the operator's exact trimmed input in both cases (a
/// no-op on success, since the only valid inputs are the canonical labels
/// modulo case) so the caller can carry it through pagination and
/// resubmission — Codex review on #1378 P2: without this, a bad value's
/// error vanished on the next Next/Previous click or Apply resubmit,
/// because the `<select>` and the pagination query string were both built
/// from the already-`None`d parsed value, silently discarding the operator's
/// input rather than persisting the error "until resolved" as intended.
fn parse_worker_status_filter(raw: Option<&str>) -> (Option<&'static str>, String, Option<String>) {
    let Some(trimmed) = raw.map(str::trim).filter(|v| !v.is_empty()) else {
        return (None, String::new(), None);
    };
    match trimmed.to_lowercase().as_str() {
        "active" => (Some("Active"), trimmed.to_string(), None),
        "draining" => (Some("Draining"), trimmed.to_string(), None),
        "stopped" => (Some("Stopped"), trimmed.to_string(), None),
        other => (
            None,
            trimmed.to_string(),
            Some(format!(
                "Unknown status '{other}'; expected Active, Draining, or Stopped. Filter not applied."
            )),
        ),
    }
}

/// Parses the Workers page's `stale` filter from a raw query-string value.
/// Returns `(parsed, raw_display, error)` with the same "filter not
/// applied, raw input carried through, error redisplayed inline" contract
/// as [`parse_worker_status_filter`].
fn parse_worker_stale_filter(raw: Option<&str>) -> (bool, String, Option<String>) {
    let Some(trimmed) = raw.map(str::trim).filter(|v| !v.is_empty()) else {
        return (false, String::new(), None);
    };
    match trimmed {
        "false" => (false, trimmed.to_string(), None),
        "true" => (true, trimmed.to_string(), None),
        other => (
            false,
            trimmed.to_string(),
            Some(format!(
                "Unknown stale value '{other}'; expected 'true' or 'false'. Filter not applied."
            )),
        ),
    }
}

/// A paused queue as rendered on the Workers page, merged across shards.
///
/// Issue #619: an operator queue pause is the single most likely explanation
/// for "workers are idle but the queue is full", so it is surfaced on the
/// fleet page an operator lands on during exactly that incident.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PausedQueueBannerRow {
    pub queue_name: String,
    pub reason: String,
    pub paused_by: String,
    pub paused_at: chrono::DateTime<chrono::Utc>,
    pub scope_shard_id: Option<i32>,
    pub held_task_count: i64,
    /// False when the shards holding this queue disagree on
    /// `(reason, paused_by, scope_shard_id)` — the displayed provenance then
    /// describes only part of the fleet and the banner says so.
    pub provenance_uniform: bool,
    /// What the hold **actually** covers, derived from the shards that hold it
    /// versus the expected shard set — not from the stored `scope_shard_id`,
    /// which records only the intent of the request that wrote each row.
    pub coverage: autumn_harvest::queue_pause::PauseCoverage,
}

/// Outcome of the Workers-page paused-queue scan: the merged banner rows plus
/// the shards whose pause state could not be read at all.
///
/// Issue #619 review: a shard whose connection or pause read fails contributes
/// **no rows**, which is indistinguishable from "that shard is not holding
/// anything". For *coverage classification* that is the safe reading (a
/// not-holding shard makes the hold `partial_fleet`, i.e. possibly still
/// dispatching). For *presence* it is not safe at all: a hold that exists
/// **only** on an unread shard vanishes from the banner entirely, so an
/// operator investigating idle workers sees a clean page and looks elsewhere
/// while dispatch is in fact held. The failed shard ids are therefore carried
/// alongside the rows and rendered as a warning even when `rows` is empty.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct PausedQueueScan {
    pub rows: Vec<PausedQueueBannerRow>,
    /// Shards whose pause state is **unknown**, not "not paused". Sorted.
    pub unreadable_shards: Vec<i32>,
}

/// Merge per-shard paused-queue rows into one banner row per queue name.
///
/// Held counts are summed across shards. The top-level provenance is the
/// **earliest** hold (the one an operator reasons about — "how long has this
/// been held?"), tie-broken by shard id so the choice never depends on
/// fan-out order. Rows are sorted by queue name so the banner is stable
/// across refreshes.
///
/// The shards' rows are **not** guaranteed to agree: `pause_queue` is
/// idempotent and preserves the *original* reason and operator on a re-pause,
/// and the `shard_id` parameter explicitly supports holding the same queue on
/// two shards separately. Summing every shard's held tasks under a single
/// shard's reason would then tell the operator a story that is simply not true
/// for part of the fleet, so `provenance_uniform` records the disagreement and
/// `render_paused_queues_banner` marks it visibly.
///
/// The uniformity rule is identical to the one `GET /admin/queues/paused` uses
/// (`api::merge_paused_queue_rows`) — including comparing
/// `(reason, paused_by, scope_shard_id)` and deliberately **not** `paused_at`,
/// since each shard stamps its own `NOW()` — so the two surfaces can never
/// disagree about whether a hold is uniform.
///
/// The rendered **scope** is likewise derived from real coverage rather than the
/// stored `scope_shard_id`, via the shared
/// [`autumn_harvest::queue_pause::classify_pause_coverage`]: a fleet-wide pause
/// that only reached some shards persists `scope_shard_id = NULL` on the shards
/// it reached and no row on the ones it missed, so labelling it "fleet-wide"
/// would tell an operator the fleet is held while part of it keeps dispatching
/// (issue #619 review).
pub(crate) fn merge_paused_queue_banner_rows(
    per_shard: Vec<(i32, autumn_harvest::queue_pause::PausedQueue)>,
    expected_shards: &[i32],
) -> Vec<PausedQueueBannerRow> {
    let mut by_queue: std::collections::BTreeMap<
        String,
        Vec<(i32, autumn_harvest::queue_pause::PausedQueue)>,
    > = std::collections::BTreeMap::new();
    for (shard_id, row) in per_shard {
        by_queue
            .entry(row.queue_name.clone())
            .or_default()
            .push((shard_id, row));
    }

    by_queue
        .into_values()
        .map(|shard_rows| {
            let earliest = shard_rows
                .iter()
                .min_by_key(|(shard_id, row)| (row.paused_at, *shard_id))
                .map(|(_, row)| row)
                .expect("group is non-empty by construction");
            let provenance_uniform = shard_rows.iter().all(|(_, row)| {
                row.reason == earliest.reason
                    && row.paused_by == earliest.paused_by
                    && row.scope_shard_id == earliest.scope_shard_id
            });
            let holding: Vec<i32> = shard_rows.iter().map(|(shard_id, _)| *shard_id).collect();
            let scopes: Vec<Option<i32>> = shard_rows
                .iter()
                .map(|(_, row)| row.scope_shard_id)
                .collect();
            PausedQueueBannerRow {
                queue_name: earliest.queue_name.clone(),
                reason: earliest.reason.clone(),
                paused_by: earliest.paused_by.clone(),
                paused_at: earliest.paused_at,
                scope_shard_id: earliest.scope_shard_id,
                held_task_count: shard_rows.iter().map(|(_, row)| row.held_task_count).sum(),
                provenance_uniform,
                coverage: autumn_harvest::queue_pause::classify_pause_coverage(
                    &holding,
                    &scopes,
                    expected_shards,
                ),
            }
        })
        .collect()
}

/// Render the paused-queues banner (issue #619). Empty when nothing is paused
/// **and** every shard's pause state was readable, so a healthy fleet page is
/// byte-identical to before.
///
/// A row whose shards disagree on provenance is marked "mixed across shards"
/// rather than silently attributing the fleet-wide held total to one shard's
/// reason and operator — see [`merge_paused_queue_banner_rows`].
///
/// `unreadable_shards` renders its own warning **even when `rows` is empty**:
/// an unread shard's pause state is unknown, so an absent banner would otherwise
/// read as "nothing is held" on exactly the page an operator lands on when
/// investigating idle workers — see [`PausedQueueScan`].
pub(crate) fn render_paused_queues_banner(
    rows: &[PausedQueueBannerRow],
    unreadable_shards: &[i32],
) -> Markup {
    if rows.is_empty() && unreadable_shards.is_empty() {
        return html! {};
    }
    let unreadable_list = unreadable_shards
        .iter()
        .map(i32::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let held_total: i64 = rows.iter().map(|r| r.held_task_count).sum();
    let mixed = rows.iter().filter(|r| !r.provenance_uniform).count();
    // A fleet-wide hold that did not reach every shard leaves part of the fleet
    // dispatching into the outage the operator is trying to hold back -- the
    // single most important thing to say on this banner (issue #619 review).
    let incomplete = rows
        .iter()
        .filter(|r| r.coverage.is_incomplete_fleet())
        .count();
    html! {
        // Rendered FIRST and unconditionally on a read failure: if this is the
        // only thing on the banner, the honest answer is "we do not know whether
        // dispatch is held", not the silent clean page an absent banner implies.
        @if !unreadable_shards.is_empty() {
            div.banner.Degraded {
                strong { "Queue pause state incomplete" }
                " — could not read shard(s) " (unreadable_list) ". "
                "A hold that exists only on an unread shard is MISSING from this \
                 banner, and a hold shown below may cover more shards than its \
                 Scope column says. Check GET /admin/queues/paused before \
                 concluding dispatch is flowing."
            }
        }
        @if !rows.is_empty() {
            div.banner.Degraded {
                strong { "Queue dispatch paused" }
                " — "
                (rows.len()) " queue(s) held | " (held_total) " task(s) waiting"
                @if incomplete > 0 {
                    " | " (incomplete) " only PARTIALLY applied — some shards are \
                           still dispatching; re-issue the pause"
                }
                @if mixed > 0 {
                    " | " (mixed) " with mixed provenance across shards \
                           (see GET /admin/queues/paused for the per-shard holds)"
                }
            }
            table {
                thead {
                    tr {
                        th { "Queue" }
                        th { "Scope" }
                        th { "Held tasks" }
                        th { "Paused at" }
                        th { "By" }
                        th { "Reason" }
                    }
                }
                tbody {
                    @for row in rows {
                        tr {
                            td { code { (row.queue_name) } }
                            td {
                                // Real coverage, not the stored intent: a
                                // fleet-wide request that missed a shard must not
                                // read "fleet-wide".
                                (row.coverage.label())
                                @if let Some(shard) = row.scope_shard_id {
                                    " (shard " (shard) ")"
                                }
                            }
                            td { (row.held_task_count) }
                            td { (row.paused_at.to_rfc3339()) }
                            td {
                                (row.paused_by)
                                @if !row.provenance_uniform { " (mixed across shards)" }
                            }
                            td {
                                (row.reason)
                                @if !row.provenance_uniform { " (mixed across shards)" }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Load paused queues across every shard for the Workers-page banner.
///
/// Best-effort: an unreachable shard never fails the page — the banner is a
/// diagnostic aid, never a gate on rendering the fleet. It is **not** silent
/// about it either: the failed shard ids come back in
/// [`PausedQueueScan::unreadable_shards`] and are rendered as their own warning,
/// because a hold that exists only on an unread shard is otherwise invisible
/// here (issue #619 review).
///
/// The **expected** shard set (pools plus every shard the router knows about) is
/// resolved from `api_state`, identical to `GET /admin/queues/paused`, so a
/// partially-applied fleet-wide hold is labelled the same way on both surfaces.
/// An unread shard counts as not-holding for *coverage*, which is the safe
/// direction (it may still be dispatching) — the warning is what tells the
/// operator the shown `Scope` could understate the real coverage.
async fn load_paused_queues_from_shards(api_state: &HarvestApiState) -> PausedQueueScan {
    let pools = crate::shard_fanout::pools_by_shard(api_state);
    let expected_set = crate::shard_fanout::expected_shards(api_state, &pools);

    // Fan out over the EXPECTED shard set, not just `pool.iter_shards()`.
    //
    // `expected_shards` deliberately includes a shard the router advertises but
    // this process has no pool for yet (mid a shard-add rollout). Iterating only
    // the pools would give such a shard no future at all, so it could never
    // enter `unreadable_shards` — and if it is the ONLY shard holding a queue,
    // the Workers page would again render no pause warning whatsoever. That is
    // the same presence-vs-coverage gap the unreadable-shard warning exists to
    // close, reached through a missing pool instead of a failing read.
    //
    // Resolution goes through the API's own `resolve_expected_shard_pools` (one
    // shared function, not a second copy of the rule) so a poolless shard maps
    // to `None` and is reported, never silently resolved through
    // `ShardedDbPool::pool_for`'s default-shard fallback — which would query the
    // DEFAULT shard's database in its place and let coverage read `fleet`.
    let expected: Vec<i32> = expected_set.iter().copied().collect();
    let resolved = crate::api::resolve_expected_shard_pools(&expected_set, &pools);
    scan_paused_queues(resolved, &expected).await
}

/// Read the pause table on each resolved shard and fold the outcomes into a
/// [`PausedQueueScan`].
///
/// Split out from [`load_paused_queues_from_shards`] so the reporting half is
/// directly testable: reaching a poolless-but-expected shard through
/// `api_state` requires an installed runtime with a widened router, but the
/// rule under test — *a `None` pool is reported, never dropped* — needs
/// neither. Its counterpart, that a router-known poolless shard resolves to
/// `None` rather than a default-fallback pool, is guarded on the API side by
/// `resolve_expected_shard_pools_flags_poolless_shard_not_default_fallback`.
///
/// `expected` is the same shard set `resolved` was built from, threaded through
/// for the coverage calculation in [`merge_paused_queue_banner_rows`].
async fn scan_paused_queues(
    resolved: Vec<(i32, Option<&autumn_harvest::worker::DbPool>)>,
    expected: &[i32],
) -> PausedQueueScan {
    let futs: Vec<_> = resolved
        .into_iter()
        .map(|(shard, maybe_pool)| async move {
            // No pool for a shard the router advertises: unread, not "holding
            // nothing". Dropping it here is exactly the invisible-hold bug.
            let Some(shard_pool) = maybe_pool else {
                return Err(shard);
            };
            let read = async {
                let mut conn = acquire_conn(shard_pool).await.ok()?;
                autumn_harvest::queue_pause::list_paused_queues(&mut conn)
                    .await
                    .ok()
            }
            .await;
            // Err(shard) carries WHICH shard failed, so the banner can say so
            // instead of silently reporting it as holding nothing.
            read.map_or(Err(shard), |rows| {
                Ok(rows.into_iter().map(|row| (shard, row)).collect::<Vec<_>>())
            })
        })
        .collect();
    let mut per_shard: Vec<(i32, autumn_harvest::queue_pause::PausedQueue)> = Vec::new();
    let mut unreadable_shards: Vec<i32> = Vec::new();
    for outcome in futures::future::join_all(futs).await {
        match outcome {
            Ok(rows) => per_shard.extend(rows),
            Err(shard) => unreadable_shards.push(shard),
        }
    }
    unreadable_shards.sort_unstable();
    PausedQueueScan {
        rows: merge_paused_queue_banner_rows(per_shard, expected),
        unreadable_shards,
    }
}

async fn load_workers_from_shards(
    pool: &crate::HarvestDbPool,
    status_filter: Option<&str>,
    stale_threshold: std::time::Duration,
) -> Vec<ShardWorkerResult> {
    let futs: Vec<_> = pool
        .iter_shards()
        .map(|(shard_id, shard_pool)| {
            let unlimited = WorkerFilters {
                limit: i64::MAX,
                status: status_filter.map(str::to_string),
                ..WorkerFilters::new()
            };
            async move {
                let result = async {
                    let mut conn = acquire_conn(shard_pool).await.map_err(|e| e.to_string())?;
                    list_workers(&mut conn, &unlimited, stale_threshold)
                        .await
                        .map_err(|e| e.to_string())
                }
                .await;
                (shard_id, result)
            }
        })
        .collect();
    futures::future::join_all(futs).await
}

#[allow(clippy::too_many_arguments)]
fn render_dead_letters_page(
    filters: &DeadLetterUiFilters,
    filter_raw: &DeadLetterUiFilterRaw,
    rows: &[DeadLetterUiRow],
    shard_errors: &[(ShardId, &str)],
    is_multi_shard: bool,
    page: i64,
    limit: i64,
    has_next: bool,
    total_matching: usize,
    refresh: Option<u64>,
    flash: Option<&str>,
) -> Markup {
    let body = html! {
        h2 { "Dead Letters" }
        @if let Some(message) = flash {
            div.flash role="status" tabindex="-1" autofocus { (message) }
        }
        (render_dead_letter_view_toggle(filters, filter_raw, limit, refresh, None, false))
        (render_dead_letter_filters(filters, filter_raw, limit, refresh))
        (render_dead_letter_bulk_actions(filters, filter_raw, limit, refresh, total_matching))

        @if rows.is_empty() && shard_errors.is_empty() {
            div.card.empty {
                @if filters.is_empty() {
                    "No dead-lettered tasks. Healthy."
                } @else {
                    "No entries match this filter."
                }
            }
        } @else {
            @for (shard_id, error) in shard_errors {
                div.shard-error {
                    @if is_multi_shard {
                        strong { "Shard " (shard_id.as_i32()) " unavailable: " }
                    } @else {
                        strong { "Shard unavailable: " }
                    }
                    (error)
                }
            }

            (render_dead_letter_table(rows, filters, filter_raw, limit, refresh))
        }

        (render_dead_letter_pagination(page, limit, has_next, filters, filter_raw, refresh))
    };

    // `dead_letter_return_to_path` deliberately excludes `page`. It names
    // the one-time redirect target after an action, and landing back on
    // page 0 there is fine.
    //
    // Auto-refresh is different: it must keep the operator on the page
    // they were reading. So it builds its own target here, matching
    // `render_dead_letter_pagination`'s own link construction, instead of
    // reusing that path (found in review, PR #1396).
    let refresh_target = format!(
        "../ui/dead-letters?page={page}{}",
        build_dead_letter_query_string(limit, filters, filter_raw, refresh)
    );
    layout_dead_letters("Dead Letters · Vantage", &body, refresh, &refresh_target)
}

// ---------------------------------------------------------------------------
// DLQ root-cause summary view (issue #385)
// ---------------------------------------------------------------------------

/// Render the DLQ summary view: in-process root-cause aggregation, the same
/// computation behind `GET /dead-letters/aggregate`, surfaced as a UI toggle.
#[allow(clippy::too_many_arguments)]
async fn render_dead_letters_summary_view(
    pool: &crate::HarvestDbPool,
    filters: &DeadLetterUiFilters,
    filter_raw: &DeadLetterUiFilterRaw,
    group_by_raw: Option<&str>,
    limit: i64,
    refresh: Option<u64>,
    flash: Option<&str>,
) -> Result<Markup, AutumnError> {
    let group_by = parse_dlq_summary_group_by(group_by_raw)?;
    let group_by_value = group_by
        .iter()
        .map(|dim| dim.as_wire())
        .collect::<Vec<_>>()
        .join(",");

    let params = autumn_harvest::dlq::DlqAggregateParams {
        group_by: group_by.clone(),
        time_bucket: autumn_harvest::dlq::TimeBucketGranularity::Hour,
        workflow_name: filters.workflow_name.clone(),
        activity_name: None,
        queue_name: None,
        task_type: filters.task_kind.map(|k| k.as_db_value().to_string()),
        since: filters.failed_after,
        until: filters.failed_before,
        min_attempts: None,
        limit_groups: DLQ_SUMMARY_GROUP_LIMIT,
        samples_per_group: DLQ_SUMMARY_SAMPLES_PER_GROUP,
    };

    let (response, shard_errors) =
        aggregate_dead_letters_for_ui(pool, &params, filters.shard_id).await;

    let body = html! {
        h2 { "Dead Letters" }
        @if let Some(message) = flash {
            div.flash role="status" tabindex="-1" autofocus { (message) }
        }
        (render_dead_letter_view_toggle(filters, filter_raw, limit, refresh, Some(&group_by_value), true))
        (render_dead_letter_filters(filters, filter_raw, limit, refresh))
        (render_dlq_summary_group_by_form(filters, filter_raw, limit, refresh, &group_by))

        @for (shard_id, error) in &shard_errors {
            div.shard-error {
                strong { "Shard " (shard_id.as_i32()) " unavailable: " }
                (error)
            }
        }

        (render_dlq_summary_stats(&response))

        @if response.groups.is_empty() {
            div.card.empty {
                @if filters.is_empty() {
                    "No dead-lettered tasks. Healthy."
                } @else {
                    "No entries match this filter."
                }
            }
        } @else {
            (render_dlq_summary_table(&response, &group_by, filters, filter_raw, limit, refresh))
        }
    };

    let group_by_query = if group_by_value.is_empty() {
        String::new()
    } else {
        format!("&group_by={}", url_encode(&group_by_value))
    };
    let refresh_target = format!(
        "../ui/dead-letters?view=summary{}{group_by_query}",
        build_dead_letter_query_string(limit, filters, filter_raw, refresh)
    );
    Ok(layout_dead_letters(
        "Dead Letters · Summary · Vantage",
        &body,
        refresh,
        &refresh_target,
    ))
}

/// Parse the comma-separated `group_by` query value into validated dimensions,
/// falling back to [`DEFAULT_DLQ_SUMMARY_GROUP_BY`] when empty. Mirrors the
/// `400`-on-unknown-dimension contract of the aggregation endpoint.
fn parse_dlq_summary_group_by(
    raw: Option<&str>,
) -> Result<Vec<autumn_harvest::dlq::DlqGroupDimension>, AutumnError> {
    let raw = raw
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(DEFAULT_DLQ_SUMMARY_GROUP_BY);

    let mut dims: Vec<autumn_harvest::dlq::DlqGroupDimension> = Vec::new();
    for part in raw.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let dim = autumn_harvest::dlq::DlqGroupDimension::from_wire(part).ok_or_else(|| {
            AutumnError::bad_request_msg(format!(
                "unknown group_by dimension '{part}'; expected one of: workflow_name, \
                 activity_name, queue_name, task_type, time_bucket, failure_signature"
            ))
        })?;
        if !dims.contains(&dim) {
            dims.push(dim);
        }
    }

    if dims.is_empty() {
        return Err(AutumnError::bad_request_msg(
            "at least one group_by dimension is required",
        ));
    }
    Ok(dims)
}

/// Fan out the per-shard aggregation and merge into a single response, mirroring
/// the management endpoint's `iter_shards()` merge. Per-shard errors are
/// surfaced rather than failing the whole view.
async fn aggregate_dead_letters_for_ui(
    pool: &crate::HarvestDbPool,
    params: &autumn_harvest::dlq::DlqAggregateParams,
    shard_filter: Option<i32>,
) -> (
    autumn_harvest::dlq::DlqAggregateResponse,
    Vec<(ShardId, String)>,
) {
    let futs: Vec<_> = pool
        .iter_shards()
        .map(|(shard_id, shard_pool)| async move {
            if shard_filter.is_some_and(|wanted| wanted != shard_id.as_i32()) {
                return (shard_id, Ok(None));
            }
            let result = async {
                let mut conn = acquire_conn(shard_pool).await.map_err(|e| e.to_string())?;
                autumn_harvest::dlq::aggregate_dead_letters(&mut conn, params)
                    .await
                    .map_err(|e| e.to_string())
            }
            .await;
            (shard_id, result.map(Some))
        })
        .collect();

    let results = futures::future::join_all(futs).await;
    let mut partials = Vec::new();
    let mut errors = Vec::new();
    for (shard_id, result) in results {
        match result {
            Ok(Some(partial)) => partials.push(partial),
            Ok(None) => {}
            Err(error) => errors.push((shard_id, error)),
        }
    }

    (
        autumn_harvest::dlq::merge_dlq_aggregates(params, partials),
        errors,
    )
}

fn render_dead_letter_view_toggle(
    filters: &DeadLetterUiFilters,
    filter_raw: &DeadLetterUiFilterRaw,
    limit: i64,
    refresh: Option<u64>,
    group_by_value: Option<&str>,
    summary_active: bool,
) -> Markup {
    let base = build_dead_letter_query_string(limit, filters, filter_raw, refresh);
    let list_href = if base.is_empty() {
        "dead-letters".to_string()
    } else {
        format!("dead-letters?{}", &base[1..])
    };
    let group_by_query = group_by_value
        .filter(|value| !value.is_empty())
        .map(|value| format!("&group_by={}", url_encode(value)))
        .unwrap_or_default();
    let summary_href = format!("dead-letters?view=summary{base}{group_by_query}");

    html! {
        div."view-toggle" {
            @if summary_active {
                a href=(list_href) { "List" }
                span.active { "Summary" }
            } @else {
                span.active { "List" }
                a href=(summary_href) { "Summary" }
            }
        }
    }
}

fn render_dlq_summary_group_by_form(
    filters: &DeadLetterUiFilters,
    filter_raw: &DeadLetterUiFilterRaw,
    limit: i64,
    refresh: Option<u64>,
    selected: &[autumn_harvest::dlq::DlqGroupDimension],
) -> Markup {
    // Presets cover the high-value triage cuts; the selected value is preserved
    // even if it is not one of the presets (custom query string).
    const PRESETS: &[(&str, &str)] = &[
        ("workflow_name,failure_signature", "Workflow × signature"),
        ("failure_signature", "Failure signature"),
        ("workflow_name", "Workflow"),
        ("activity_name", "Activity"),
        ("activity_name,failure_signature", "Activity × signature"),
        ("queue_name", "Queue"),
        ("task_type", "Task type"),
        ("time_bucket", "Time bucket (hour)"),
    ];
    let selected_value = selected
        .iter()
        .map(|dim| dim.as_wire())
        .collect::<Vec<_>>()
        .join(",");
    let selected_is_preset = PRESETS.iter().any(|(value, _)| *value == selected_value);

    html! {
        form.filters method="get" action="dead-letters" {
            input type="hidden" name="view" value="summary";
            (render_dead_letter_hidden_filters_raw(filters, filter_raw))
            @if limit != DEFAULT_DLQ_PAGE_SIZE {
                input type="hidden" name="limit" value=(limit);
            }
            @if let Some(refresh) = refresh {
                input type="hidden" name="refresh" value=(refresh);
            }
            label {
                "Group by"
                select name="group_by" {
                    @for (value, label) in PRESETS {
                        option value=(value) selected[*value == selected_value] { (label) }
                    }
                    @if !selected_is_preset {
                        option value=(selected_value) selected { (selected_value) }
                    }
                }
            }
            button type="submit" { "Group" }
        }
    }
}

fn render_dlq_summary_stats(response: &autumn_harvest::dlq::DlqAggregateResponse) -> Markup {
    html! {
        div."summary-stats" {
            span { strong { (response.filtered_total) } " matching" }
            span { strong { (response.total) } " total in DLQ" }
            span { strong { (response.groups.len()) } " groups" }
            @if response.truncated {
                span.note { "long tail rolled into “other”" }
            }
        }
    }
}

fn render_dlq_summary_table(
    response: &autumn_harvest::dlq::DlqAggregateResponse,
    group_by: &[autumn_harvest::dlq::DlqGroupDimension],
    filters: &DeadLetterUiFilters,
    filter_raw: &DeadLetterUiFilterRaw,
    limit: i64,
    refresh: Option<u64>,
) -> Markup {
    html! {
        table {
            thead {
                tr {
                    @for dim in group_by {
                        th { (dim.as_wire()) }
                    }
                    th { "count" }
                    th { "first_seen" }
                    th { "last_seen" }
                    th { "samples" }
                    th { "actions" }
                }
            }
            tbody {
                @for group in &response.groups {
                    @let is_other = group
                        .key
                        .get("_other")
                        .and_then(serde_json::Value::as_bool)
                        .unwrap_or(false);
                    tr {
                        @if is_other {
                            td colspan=(group_by.len()) { em { "other (long tail)" } }
                        } @else {
                            @for dim in group_by {
                                td { (dlq_summary_key_cell(&group.key, dim.as_wire())) }
                            }
                        }
                        td { (group.count) }
                        td { (format_timestamp(group.first_seen)) }
                        td { (format_timestamp(group.last_seen)) }
                        td {
                            @if group.sample_dead_letter_ids.is_empty() {
                                "—"
                            } @else {
                                @for sample in &group.sample_dead_letter_ids {
                                    code.sample { (sample) }
                                }
                            }
                        }
                        td {
                            @if is_other {
                                "—"
                            } @else {
                                @let (href, partial) = dlq_summary_drilldown_href(&group.key, group_by, filters, filter_raw, limit, refresh);
                                a href=(href) title=[partial.then_some("Some dimensions have no list-view filter — results may include extra rows from other groups")] {
                                    @if partial {
                                        "View entries (partial filter) →"
                                    } @else {
                                        "View entries →"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn dlq_summary_key_cell(key: &serde_json::Value, dim: &str) -> String {
    match key.get(dim) {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(serde_json::Value::Null) | None => "—".to_string(),
        Some(other) => other.to_string(),
    }
}

/// Build a click-through link into the list view with whatever filters the list
/// view can express pre-applied from this group's key.
///
/// Returns `(href, is_partial)`. `is_partial` is `true` when one or more
/// group dimensions (`activity_name`, `queue_name`, `time_bucket`,
/// `failure_signature`) have no equivalent list-view filter — the resulting
/// link will show a superset of the selected group.
fn dlq_summary_drilldown_href(
    key: &serde_json::Value,
    group_by: &[autumn_harvest::dlq::DlqGroupDimension],
    filters: &DeadLetterUiFilters,
    filter_raw: &DeadLetterUiFilterRaw,
    limit: i64,
    refresh: Option<u64>,
) -> (String, bool) {
    use autumn_harvest::dlq::DlqGroupDimension;

    // Start from the filters already applied to the summary so drill-down
    // narrows rather than widens. `drill_raw` starts as a clone of the
    // summary's own raw state, not a derivation from `drill`. Codex review
    // on #1420 found the bug in a derived-only `drill_raw`: it silently
    // dropped an invalid failed_after/failed_before, and its error, on
    // every "View entries" link. This function never touches those two
    // fields. The view toggle, refresh, and group-by form all preserve
    // that same invalid value. The drilldown link must not be the one
    // exception.
    let mut drill = filters.clone();
    let mut drill_raw = filter_raw.clone();
    let mut partial = false;
    for dim in group_by {
        match dim {
            DlqGroupDimension::WorkflowName => {
                if let Some(serde_json::Value::String(name)) = key.get("workflow_name") {
                    drill.workflow_name = Some(name.clone());
                }
            }
            DlqGroupDimension::TaskType => {
                if let Some(serde_json::Value::String(task_type)) = key.get("task_type") {
                    // This field IS synthesized fresh from the group's own
                    // key, unlike failed_after/failed_before above. It is
                    // always valid (or absent), so its raw text and error
                    // are overwritten to match, not merely inherited.
                    drill.task_kind = DeadLetterTaskKind::parse(task_type).ok();
                    drill_raw.task_kind = drill
                        .task_kind
                        .map(DeadLetterTaskKind::as_label)
                        .unwrap_or_default()
                        .to_string();
                    drill_raw.task_kind_error = None;
                }
            }
            // No list-view filter exists for these dimensions; the link will
            // show more rows than belong to this exact group.
            DlqGroupDimension::ActivityName
            | DlqGroupDimension::QueueName
            | DlqGroupDimension::TimeBucket
            | DlqGroupDimension::FailureSignature
            | DlqGroupDimension::DlqReason
            | DlqGroupDimension::ErrorClass => {
                partial = true;
            }
        }
    }

    let query = build_dead_letter_query_string(limit, &drill, &drill_raw, refresh);
    let href = if query.is_empty() {
        "dead-letters".to_string()
    } else {
        format!("dead-letters?{}", &query[1..])
    };
    (href, partial)
}

fn render_dead_letter_filters(
    filters: &DeadLetterUiFilters,
    filter_raw: &DeadLetterUiFilterRaw,
    limit: i64,
    refresh: Option<u64>,
) -> Markup {
    let workflow_name = filters.workflow_name.as_deref().unwrap_or("");
    let task_kind = filters.task_kind.map(DeadLetterTaskKind::as_label);
    let refresh_value = refresh.map(|secs| secs.to_string()).unwrap_or_default();

    html! {
        form.filters method="get" action="dead-letters" {
            label {
                "Workflow name"
                input type="text" name="workflow_name" value=(workflow_name) placeholder="e.g. invoice_workflow";
            }
            label {
                "Task kind"
                select name="task_kind" {
                    option value="" selected[task_kind.is_none() && filter_raw.task_kind_error.is_none()] { "All" }
                    option value="Activity" selected[task_kind == Some("Activity")] { "Activity" }
                    option value="Workflow" selected[task_kind == Some("Workflow")] { "Workflow" }
                    // An unrecognized value is rendered as its own option.
                    // This makes the select echo it back instead of silently
                    // reverting to "All" — same treatment as the Workers
                    // page's status filter (#1378).
                    @if filter_raw.task_kind_error.is_some() {
                        option value=(filter_raw.task_kind) selected { (filter_raw.task_kind) }
                    }
                }
                @if let Some(error) = &filter_raw.task_kind_error {
                    span.field-error role="alert" { (error) }
                }
            }
            label {
                "Failed after"
                input type="text" name="failed_after" value=(filter_raw.failed_after) placeholder="2026-05-10T00:00:00Z";
                @if let Some(error) = &filter_raw.failed_after_error {
                    span.field-error role="alert" { (error) }
                }
            }
            label {
                "Failed before"
                input type="text" name="failed_before" value=(filter_raw.failed_before) placeholder="2026-05-11T00:00:00Z";
                @if let Some(error) = &filter_raw.failed_before_error {
                    span.field-error role="alert" { (error) }
                }
            }
            label {
                "Shard"
                input type="text" inputmode="numeric" pattern="-?[0-9]*" name="shard_id" value=(filter_raw.shard_id) placeholder="e.g. 0";
                @if let Some(error) = &filter_raw.shard_id_error {
                    span.field-error role="alert" { (error) }
                }
            }
            label {
                "Per page"
                input type="number" name="limit" min="1" max=(MAX_PAGE_SIZE) value=(limit);
            }
            label {
                "Refresh"
                select name="refresh" {
                    option value="" selected[refresh.is_none()] { "Off" }
                    option value="30" selected[refresh == Some(30)] { "30s" }
                    option value="60" selected[refresh == Some(60)] { "60s" }
                    @if refresh.is_some_and(|secs| secs != 30 && secs != 60) {
                        option value=(refresh_value) selected { (refresh_value) "s" }
                    }
                }
            }
            button type="submit" { "Apply" }
            a.reset href="dead-letters" { "Reset" }
        }
    }
}

fn render_dead_letter_bulk_actions(
    filters: &DeadLetterUiFilters,
    filter_raw: &DeadLetterUiFilterRaw,
    limit: i64,
    refresh: Option<u64>,
    total_matching: usize,
) -> Markup {
    let return_to = dead_letter_return_to_path(filters, filter_raw, limit, refresh);
    let action_limit = dead_letter_bulk_action_limit(total_matching);
    let replay_label = dead_letter_bulk_action_label("Replay", action_limit, total_matching);
    let discard_label = dead_letter_bulk_action_label("Discard", action_limit, total_matching);
    let replay_confirm = dead_letter_bulk_action_confirm("Replay", action_limit, total_matching);
    let discard_confirm = dead_letter_bulk_action_confirm("Discard", action_limit, total_matching);
    html! {
        div."bulk-actions" {
            form method="post" action="../dead-letters/replay" onsubmit={ "return confirm('" (replay_confirm) "')" } {
                (render_dead_letter_hidden_filters(filters))
                input type="hidden" name="limit" value=(action_limit);
                input type="hidden" name="return_to" value=(return_to);
                button type="submit" disabled[total_matching == 0 || filters.is_empty()] {
                    (replay_label)
                }
            }
            form method="post" action="../dead-letters/discard" onsubmit={ "return confirm('" (discard_confirm) "')" } {
                (render_dead_letter_hidden_filters(filters))
                input type="hidden" name="limit" value=(action_limit);
                input type="hidden" name="return_to" value=(return_to);
                button.danger type="submit" disabled[total_matching == 0 || filters.is_empty()] {
                    (discard_label)
                }
            }
        }
    }
}

fn dead_letter_bulk_action_limit(total_matching: usize) -> usize {
    total_matching.clamp(1, DLQ_BULK_ACTION_LIMIT)
}

fn dead_letter_bulk_action_label(verb: &str, action_limit: usize, total_matching: usize) -> String {
    if total_matching > action_limit {
        format!("{verb} first {action_limit} matching ({total_matching} total)")
    } else {
        format!("{verb} all matching ({total_matching})")
    }
}

fn dead_letter_bulk_action_confirm(
    verb: &str,
    action_limit: usize,
    total_matching: usize,
) -> String {
    if total_matching > action_limit {
        format!("{verb} first {action_limit} of {total_matching} matching dead-letter entries?")
    } else {
        format!("{verb} {total_matching} matching dead-letter entries?")
    }
}

fn render_dead_letter_table(
    rows: &[DeadLetterUiRow],
    filters: &DeadLetterUiFilters,
    filter_raw: &DeadLetterUiFilterRaw,
    limit: i64,
    refresh: Option<u64>,
) -> Markup {
    let return_to = dead_letter_return_to_path(filters, filter_raw, limit, refresh);
    html! {
        table {
            thead {
                tr {
                    th { "dead_letter_id" }
                    th { "workflow_name" }
                    th { "workflow_exec_id" }
                    th { "task_kind" }
                    th { "attempt" }
                    th { "failed_at" }
                    th { "error_message" }
                    th { "shard_id" }
                    th { "actions" }
                }
            }
            tbody {
                @for row in rows {
                    @let id = row.dead_letter.id.to_string();
                    @let workflow_name = row.workflow_name.as_deref().unwrap_or("unknown");
                    @let task_kind = dead_letter_task_kind_label(&row.dead_letter.task_type);
                    tr {
                        td { code { (id) } }
                        td { (workflow_name) }
                        td {
                            @if let Some(exec_id) = row.dead_letter.workflow_exec_id {
                                @let exec = exec_id.to_string();
                                a href={ "workflows/" (exec) } { code { (exec) } }
                            } @else {
                                "—"
                            }
                        }
                        td { (task_kind) }
                        td { (row.dead_letter.attempts) }
                        td { (format_timestamp(Some(row.dead_letter.failed_at))) }
                        td {
                            (truncate_error(&row.dead_letter.error))
                            (render_dead_letter_detail(row))
                        }
                        td { (row.shard_id.as_i32()) }
                        td {
                            div.actions {
                                form method="post" action="../dead-letters/replay" onsubmit="return confirm('Replay this dead-letter entry?')" {
                                    input type="hidden" name="dead_letter_id" value=(id);
                                    input type="hidden" name="return_to" value=(return_to);
                                    button type="submit" { "Replay" }
                                }
                                form method="post" action="../dead-letters/discard" onsubmit="return confirm('Discard this dead-letter entry?')" {
                                    input type="hidden" name="dead_letter_id" value=(id);
                                    input type="hidden" name="return_to" value=(return_to);
                                    button.danger type="submit" { "Discard" }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn render_dead_letter_detail(row: &DeadLetterUiRow) -> Markup {
    html! {
        details {
            summary { "details" }
            div."detail-block" {
                div {
                    h3 { "Full error" }
                    pre { (row.dead_letter.error) }
                }
                div {
                    h3 { "Original payload" }
                    pre { (pretty_json(&row.dead_letter.input)) }
                }
                div {
                    h3 { "Last 10 events" }
                    @if row.events.is_empty() {
                        div.empty { "No workflow events found." }
                    } @else {
                        table {
                            thead {
                                tr {
                                    th { "#" }
                                    th { "Type" }
                                    th { "Timestamp" }
                                    th { "Data" }
                                }
                            }
                            tbody {
                                @for event in &row.events {
                                    tr {
                                        td { (event.event_id) }
                                        td { code { (event.event_type) } }
                                        td { (format_timestamp(Some(event.timestamp))) }
                                        td { pre { (pretty_json(&event.event_data)) } }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Hidden filter fields for the DLQ page's GET forms — the group-by
/// resubmit form. It routes back through `list_dead_letters_ui`, so it
/// handles an invalid value gracefully like every other GET on this page.
/// Carries the raw text, not the parsed value. This lets an invalid value's
/// inline error survive resubmission, instead of being silently dropped.
/// Same reasoning as the Workers page's `build_worker_query_string` (Codex
/// review, #1378 P2).
///
/// Do NOT use this for the bulk-action POST forms — see
/// [`render_dead_letter_hidden_filters`], which those forms need instead.
fn render_dead_letter_hidden_filters_raw(
    filters: &DeadLetterUiFilters,
    filter_raw: &DeadLetterUiFilterRaw,
) -> Markup {
    html! {
        @if let Some(workflow_name) = filters.workflow_name.as_deref() {
            input type="hidden" name="workflow_name" value=(workflow_name);
        }
        @if !filter_raw.task_kind.is_empty() {
            input type="hidden" name="task_kind" value=(filter_raw.task_kind);
        }
        @if !filter_raw.failed_after.is_empty() {
            input type="hidden" name="failed_after" value=(filter_raw.failed_after);
        }
        @if !filter_raw.failed_before.is_empty() {
            input type="hidden" name="failed_before" value=(filter_raw.failed_before);
        }
        @if !filter_raw.shard_id.is_empty() {
            input type="hidden" name="shard_id" value=(filter_raw.shard_id);
        }
    }
}

/// Hidden filter fields for the DLQ page's bulk-action POST forms
/// (`../dead-letters/replay`, `../dead-letters/discard`). Carries only the
/// successfully parsed values, never raw text.
///
/// `parse_bulk_dlq_form` (autumn-harvest-plugin/src/api.rs) re-validates
/// `task_kind`/`failed_after`/`failed_before` strictly and 400s on a bad
/// value. [`render_dead_letter_hidden_filters_raw`] submits an invalid raw
/// value on the GET group-by form, which is safe there. Doing the same
/// here would reintroduce the exact bug this PR fixes, one layer down. The
/// bulk action would abort instead of running, or redisplaying the inline
/// error (Codex review, #1420). An invalid field is "filter not applied"
/// on this page, so it is simply omitted here. The operator's raw text and
/// the error still redisplay from `return_to`, which is built from the raw
/// query string.
fn render_dead_letter_hidden_filters(filters: &DeadLetterUiFilters) -> Markup {
    html! {
        @if let Some(workflow_name) = filters.workflow_name.as_deref() {
            input type="hidden" name="workflow_name" value=(workflow_name);
        }
        @if let Some(task_kind) = filters.task_kind.map(DeadLetterTaskKind::as_label) {
            input type="hidden" name="task_kind" value=(task_kind);
        }
        @if let Some(failed_after) = filters.failed_after.map(|ts| ts.to_rfc3339()) {
            input type="hidden" name="failed_after" value=(failed_after);
        }
        @if let Some(failed_before) = filters.failed_before.map(|ts| ts.to_rfc3339()) {
            input type="hidden" name="failed_before" value=(failed_before);
        }
        @if let Some(shard_id) = filters.shard_id {
            input type="hidden" name="shard_id" value=(shard_id);
        }
    }
}

fn render_dead_letter_pagination(
    page: i64,
    limit: i64,
    has_next: bool,
    filters: &DeadLetterUiFilters,
    filter_raw: &DeadLetterUiFilterRaw,
    refresh: Option<u64>,
) -> Markup {
    let base = build_dead_letter_query_string(limit, filters, filter_raw, refresh);
    html! {
        div.pagination {
            @if page > 0 {
                a href={ "dead-letters?page=" (page - 1) (PreEscaped(&base)) } {
                    (PreEscaped("&larr;")) " Previous"
                }
            } @else {
                span.disabled { (PreEscaped("&larr;")) " Previous" }
            }

            span { "Page " (page + 1) }

            @if has_next {
                a href={ "dead-letters?page=" (page + 1) (PreEscaped(&base)) } {
                    "Next " (PreEscaped("&rarr;"))
                }
            } @else {
                span.disabled { "Next " (PreEscaped("&rarr;")) }
            }
        }
    }
}

fn build_dead_letter_query_string(
    limit: i64,
    filters: &DeadLetterUiFilters,
    filter_raw: &DeadLetterUiFilterRaw,
    refresh: Option<u64>,
) -> String {
    let mut out = String::new();
    if limit != DEFAULT_DLQ_PAGE_SIZE {
        let _ = write!(out, "&limit={limit}");
    }
    if let Some(workflow_name) = filters.workflow_name.as_deref() {
        let _ = write!(out, "&workflow_name={}", url_encode(workflow_name));
    }
    // Carry the raw text, not the parsed value. This lets an invalid
    // value's inline error persist across pagination, instead of being
    // silently dropped. Same reasoning as `build_query_string`'s
    // started_after/started_before handling on the Workflows page (Codex
    // review, #1378 P2).
    if !filter_raw.task_kind.is_empty() {
        let _ = write!(out, "&task_kind={}", url_encode(&filter_raw.task_kind));
    }
    if !filter_raw.failed_after.is_empty() {
        let _ = write!(
            out,
            "&failed_after={}",
            url_encode(&filter_raw.failed_after)
        );
    }
    if !filter_raw.failed_before.is_empty() {
        let _ = write!(
            out,
            "&failed_before={}",
            url_encode(&filter_raw.failed_before)
        );
    }
    if !filter_raw.shard_id.is_empty() {
        let _ = write!(out, "&shard_id={}", url_encode(&filter_raw.shard_id));
    }
    if let Some(refresh) = refresh {
        let _ = write!(out, "&refresh={refresh}");
    }
    out
}

fn dead_letter_return_to_path(
    filters: &DeadLetterUiFilters,
    filter_raw: &DeadLetterUiFilterRaw,
    limit: i64,
    refresh: Option<u64>,
) -> String {
    let query = build_dead_letter_query_string(limit, filters, filter_raw, refresh);
    if query.is_empty() {
        "../ui/dead-letters".to_string()
    } else {
        format!("../ui/dead-letters?{}", &query[1..])
    }
}

fn dead_letter_task_kind_label(task_type: &str) -> &'static str {
    if task_type.eq_ignore_ascii_case("activity") {
        "Activity"
    } else if task_type.eq_ignore_ascii_case("workflow") {
        "Workflow"
    } else if task_type.eq_ignore_ascii_case("callback") {
        // Issue #605 completion-callback exhaustion. The generic "Replay"
        // action still works for this row -- `dlq::replay_dead_letter`
        // delegates it to the completion-delivery redrive primitive (issue
        // #921 review) -- so no separate UI treatment is needed beyond a
        // clear label.
        "Callback"
    } else {
        "Unknown"
    }
}

fn truncate_error(error: &str) -> String {
    const LIMIT: usize = 96;
    let mut chars = error.chars();
    let truncated = chars.by_ref().take(LIMIT).collect::<String>();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

/// `refresh_target` is the current filtered view's URL with no `flash`
/// param. The caller builds it from the same filters, limit, and refresh
/// already in its own scope.
///
/// A dead-letter action redirects here with `flash` appended to
/// `return_to`. `return_to` itself preserves `refresh`. An operator with
/// auto-refresh on would otherwise see this page's targetless `meta
/// refresh` reload that same URL, flash included, on every interval. Each
/// reload would re-announce and re-focus a stale message.
///
/// An explicit `url=` on the tag breaks that loop. The flash still shows
/// and takes focus on the load right after the action. Every reload after
/// that lands on the flash-free URL instead (found in review, PR #1396).
fn layout_dead_letters(
    title: &str,
    body: &Markup,
    refresh: Option<u64>,
    refresh_target: &str,
) -> Markup {
    html! {
        (PreEscaped("<!DOCTYPE html>"))
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width,initial-scale=1";
                @if let Some(secs) = refresh {
                    meta http-equiv="refresh" content={ (secs) "; url=" (refresh_target) };
                }
                title { (title) }
                style { (PreEscaped(STYLE)) }
            }
            body {
                header {
                    h1 {
                        a href="workflows" { "🔭 Vantage" }
                        span.subtitle { "Harvest dashboard" }
                    }
                    nav {
                        a href="workflows" { "Workflows" }
                        a href="workers" { "Workers" }
                        a href="schedules" { "Schedules" }
                        a.active href="dead-letters" { "Dead Letters" }
                        a href="build-routing" { "Build Routing" }
                    }
                }
                main { (body) }
                footer { "Operational dashboard — autumn-harvest" }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn render_workers_page(
    stats: &WorkerFleetStats,
    banner_state: Option<BannerState>,
    paused_queues: &PausedQueueScan,
    grouped: &[(ShardId, Vec<WorkerRow>)],
    shard_errors: &[(ShardId, &str)],
    is_multi_shard: bool,
    page: i64,
    limit: i64,
    has_next: bool,
    status_filter: Option<&str>,
    status_raw: &str,
    status_error: Option<&str>,
    shard_raw: &str,
    shard_error: Option<&str>,
    stale_only: bool,
    stale_raw: &str,
    stale_error: Option<&str>,
    build_id_filter: Option<&str>,
    refresh: Option<u64>,
) -> Markup {
    let total_workers: usize = grouped.iter().map(|(_, rows)| rows.len()).sum();

    let body = html! {
        h2 { "Workers" }

        // Fleet health banner
        (render_fleet_banner(stats, banner_state))

        // Paused-queue banner (issue #619) -- empty when nothing is held.
        (render_paused_queues_banner(&paused_queues.rows, &paused_queues.unreadable_shards))

        // Filters
        (render_worker_filters(status_filter, status_raw, status_error, shard_raw, shard_error, stale_only, stale_raw, stale_error, build_id_filter, limit))

        // Worker table (grouped by shard if multi-shard)
        @if total_workers == 0 && shard_errors.is_empty() {
            div.card.empty {
                @if stats.total == 0 {
                    "No workers registered. Start a worker to see it here."
                } @else {
                    "No workers match this filter."
                }
            }
        } @else {
            // Shard error stubs
            @for (shard_id, error) in shard_errors {
                div.shard-error {
                    @if is_multi_shard {
                        strong { "Shard " (shard_id.as_i32()) " unavailable: " }
                    } @else {
                        strong { "Shard unavailable: " }
                    }
                    (error)
                }
            }

            // Worker rows grouped by shard
            @for (shard_id, rows) in grouped {
                @if is_multi_shard {
                    div.shard-header { "Shard " (shard_id.as_i32()) }
                }
                (render_worker_table(rows, *shard_id))
            }
        }

        (render_worker_pagination(page, limit, has_next, status_raw, shard_raw, stale_raw, build_id_filter))
    };

    layout_workers("Workers · Vantage", &body, refresh)
}

fn render_fleet_banner(stats: &WorkerFleetStats, banner_state: Option<BannerState>) -> Markup {
    let Some(verdict) = banner_state else {
        return html! {
            div.banner.Healthy { "Healthy — 0 workers registered" }
        };
    };

    let label = verdict.as_str();
    let class = format!("banner {label}");
    html! {
        div class=(class) {
            strong { (label) }
            " — "
            (stats.total) " workers | "
            (stats.active) " active | "
            (stats.draining) " draining | "
            (stats.stopped) " stopped | "
            (stats.stale) " stale"
        }
    }
}

fn render_worker_table(rows: &[WorkerRow], shard_id: ShardId) -> Markup {
    html! {
        table {
            thead {
                tr {
                    th { "Worker ID" }
                    th { "Status" }
                    th { "Build ID" }
                    th { "Deployment" }
                    th { "Last Heartbeat" }
                    th { "Shard" }
                    th { "In-Flight" }
                }
            }
            tbody {
                @for row in rows {
                    @let is_stale = row.health == WorkerHealth::Stale;
                    @let row_class = if is_stale { "stale-row" } else { "" };
                    tr class=(row_class) {
                        td { code { (short_id(&row.worker.worker_id)) } }
                        td { (worker_status_badge(&row.worker.status, is_stale)) }
                        td {
                            @if row.worker.build_id.is_empty() {
                                span style="color:#94a3b8" { "—" }
                            } @else {
                                a href={ "build-routing?build_id=" (url_encode(&row.worker.build_id)) }
                                  title="View in Build Routing" {
                                    code { (row.worker.build_id.chars().take(16).collect::<String>()) }
                                }
                            }
                        }
                        td {
                            @if let Some(ref dep) = row.worker.deployment_name {
                                code { (dep) }
                            } @else {
                                span style="color:#94a3b8" { "—" }
                            }
                        }
                        td {
                            @let rel = relative_time(row.worker.last_heartbeat_at);
                            @let abs = format_timestamp(Some(row.worker.last_heartbeat_at));
                            time datetime=(row.worker.last_heartbeat_at.to_rfc3339()) title=(abs) {
                                (rel)
                            }
                        }
                        td { (shard_id.as_i32()) }
                        td { (row.worker.in_flight_count) }
                    }
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn render_worker_filters(
    status_filter: Option<&str>,
    status_raw: &str,
    status_error: Option<&str>,
    shard_raw: &str,
    shard_error: Option<&str>,
    stale_only: bool,
    stale_raw: &str,
    stale_error: Option<&str>,
    build_id_filter: Option<&str>,
    limit: i64,
) -> Markup {
    let build_id_value = build_id_filter.unwrap_or("");
    html! {
        form.filters method="get" action="workers" {
            label {
                "Status"
                select name="status" {
                    option value="" selected[status_filter.is_none() && status_error.is_none()] { "All" }
                    @for s in ["Active", "Draining", "Stopped"] {
                        option value=(s) selected[status_filter == Some(s)] { (s) }
                    }
                    // An unrecognized value is rendered as its own option so
                    // the select echoes it back (rather than silently
                    // reverting to "All") until the operator picks a valid
                    // one — the `<select>` equivalent of a text input's
                    // `value=` (Codex review, #1378 P2).
                    @if status_error.is_some() {
                        option value=(status_raw) selected { (status_raw) }
                    }
                }
                @if let Some(error) = status_error {
                    span.field-error role="alert" { (error) }
                }
            }
            label {
                "Build ID"
                input type="text" name="build_id" value=(build_id_value) placeholder="e.g. abc123";
            }
            label {
                "Shard"
                input type="text" inputmode="numeric" pattern="-?[0-9]*" name="shard" value=(shard_raw) placeholder="e.g. 0";
                @if let Some(error) = shard_error {
                    span.field-error role="alert" { (error) }
                }
            }
            label {
                "Stale only"
                select name="stale" {
                    option value="" selected[!stale_only && stale_error.is_none()] { "All" }
                    option value="true" selected[stale_only] { "Stale only" }
                    @if stale_error.is_some() {
                        option value=(stale_raw) selected { (stale_raw) }
                    }
                }
                @if let Some(error) = stale_error {
                    span.field-error role="alert" { (error) }
                }
            }
            label {
                "Per page"
                input type="number" name="limit" min="1" max=(MAX_PAGE_SIZE) value=(limit);
            }
            button type="submit" { "Apply" }
            a.reset href="workers" { "Reset" }
        }
    }
}

fn render_worker_pagination(
    page: i64,
    limit: i64,
    has_next: bool,
    status_raw: &str,
    shard_raw: &str,
    stale_raw: &str,
    build_id_filter: Option<&str>,
) -> Markup {
    let base = build_worker_query_string(limit, status_raw, shard_raw, stale_raw, build_id_filter);
    html! {
        div.pagination {
            @if page > 0 {
                a href={ "workers?page=" (page - 1) (PreEscaped(&base)) } {
                    (PreEscaped("&larr;")) " Previous"
                }
            } @else {
                span.disabled { (PreEscaped("&larr;")) " Previous" }
            }

            span { "Page " (page + 1) }

            @if has_next {
                a href={ "workers?page=" (page + 1) (PreEscaped(&base)) } {
                    "Next " (PreEscaped("&rarr;"))
                }
            } @else {
                span.disabled { "Next " (PreEscaped("&rarr;")) }
            }
        }
    }
}

fn build_worker_query_string(
    limit: i64,
    status_raw: &str,
    shard_raw: &str,
    stale_raw: &str,
    build_id_filter: Option<&str>,
) -> String {
    let mut out = String::new();
    if limit != DEFAULT_PAGE_SIZE {
        let _ = write!(out, "&limit={limit}");
    }
    // Carry the raw text (not the parsed value) so an invalid value's inline
    // error persists across pagination instead of being silently dropped —
    // same reasoning as `build_query_string`'s started_after/started_before
    // handling on the Workflows page (Codex review, #1378 P2).
    if !status_raw.is_empty() {
        let _ = write!(out, "&status={}", url_encode(status_raw));
    }
    if let Some(build_id) = build_id_filter {
        let _ = write!(out, "&build_id={}", url_encode(build_id));
    }
    if !shard_raw.is_empty() {
        let _ = write!(out, "&shard={}", url_encode(shard_raw));
    }
    if !stale_raw.is_empty() {
        let _ = write!(out, "&stale={}", url_encode(stale_raw));
    }
    out
}

fn worker_status_badge(status: &str, stale: bool) -> Markup {
    let class = format!("badge {status}");
    html! {
        span class=(class) {
            (status)
            @if stale { " (stale)" }
        }
    }
}

/// Format a `DateTime<Utc>` as a human-readable relative time string.
fn relative_time(ts: DateTime<Utc>) -> String {
    let elapsed = Utc::now()
        .signed_duration_since(ts)
        .to_std()
        .unwrap_or(std::time::Duration::ZERO);
    let secs = elapsed.as_secs();
    if secs < 5 {
        "just now".to_string()
    } else if secs < 60 {
        format!("{secs}s ago")
    } else if secs < 3600 {
        format!("{}m ago", secs / 60)
    } else if secs < 86400 {
        format!("{}h ago", secs / 3600)
    } else {
        format!("{}d ago", secs / 86400)
    }
}

fn layout_workers(title: &str, body: &Markup, refresh: Option<u64>) -> Markup {
    html! {
        (PreEscaped("<!DOCTYPE html>"))
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width,initial-scale=1";
                @if let Some(secs) = refresh {
                    meta http-equiv="refresh" content=(secs);
                }
                title { (title) }
                style { (PreEscaped(STYLE)) }
            }
            body {
                header {
                    h1 {
                        a href="workflows" { "🔭 Vantage" }
                        span.subtitle { "Harvest dashboard" }
                    }
                    nav {
                        a href="workflows" { "Workflows" }
                        a.active href="workers" { "Workers" }
                        a href="schedules" { "Schedules" }
                        a href="dead-letters" { "Dead Letters" }
                        a href="build-routing" { "Build Routing" }
                    }
                }
                main { (body) }
                footer { "Read-only dashboard — autumn-harvest" }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn render_workflow_list(
    workflows: &[WorkflowExecution],
    page: i64,
    limit: i64,
    has_next: bool,
    state_filter: Option<&str>,
    workflow_name_filter: Option<&str>,
    search_attr_filter: Option<&(String, String)>,
    started_after_raw: &str,
    started_after_error: Option<&str>,
    started_before_raw: &str,
    started_before_error: Option<&str>,
    exec_id_search: Option<&str>,
    active_gate_count: usize,
    unavailable_shards: &[UnavailableShard],
) -> Markup {
    // Issue #756: name the unreachable shard(s) so a partial list is not read
    // as the authoritative fleet state.
    let unavailable_summary = unavailable_shards
        .iter()
        .map(|s| s.shard_id.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let body = html! {
        h2 { "Workflows" }

        // issue #756: partial cross-shard read banner — shown when a shard was
        // unreachable, so the list below is known to be incomplete.
        @if !unavailable_shards.is_empty() {
            div class="banner Warning" {
                strong { "⚠ Partial results" }
                " — "
                (unavailable_shards.len())
                @if unavailable_shards.len() == 1 { " shard is" } @else { " shards are" }
                " unreachable; this list may be incomplete. Unavailable shard(s): "
                (unavailable_summary)
            }
        }

        // issue #377: admission gate banner — shown when any gate is active.
        @if active_gate_count > 0 {
            div class="banner Unhealthy" {
                strong { "⚠ Admission gate active" }
                " — "
                (active_gate_count)
                @if active_gate_count == 1 { " gate is" } @else { " gates are" }
                " blocking new workflow starts. "
                a href="../admin/gates" { "Manage gates →" }
            }
        }

        (render_filters(state_filter, workflow_name_filter, search_attr_filter, started_after_raw, started_after_error, started_before_raw, started_before_error, exec_id_search, limit))

        @if workflows.is_empty() {
            div.card.empty { "No workflows match this filter." }
        } @else {
            table {
                thead {
                    tr {
                        th { "ID" }
                        th { "Workflow" }
                        th { "State" }
                        th { "Queue" }
                        th { "Started" }
                        th { "Completed" }
                    }
                }
                tbody {
                    @for execution in workflows {
                        @let id = execution.id.to_string();
                        tr {
                            td {
                                a href={ "workflows/" (id) } { code { (short_id(&id)) } }
                            }
                            td { (execution.workflow_name) }
                            td { (state_badge(&execution.state)) }
                            td { code { (execution.queue_name) } }
                            td { (format_timestamp(Some(execution.started_at))) }
                            td { (format_timestamp(execution.completed_at)) }
                        }
                    }
                }
            }
        }

        (render_pagination(page, limit, has_next, state_filter, workflow_name_filter, search_attr_filter, started_after_raw, started_before_raw, exec_id_search))
    };

    layout("Workflows · Vantage", &body, "")
}

#[allow(clippy::too_many_arguments)]
fn render_filters(
    state_filter: Option<&str>,
    workflow_name_filter: Option<&str>,
    search_attr_filter: Option<&(String, String)>,
    started_after_raw: &str,
    started_after_error: Option<&str>,
    started_before_raw: &str,
    started_before_error: Option<&str>,
    exec_id_search: Option<&str>,
    limit: i64,
) -> Markup {
    let (attr_key, attr_value) =
        search_attr_filter.map_or(("", ""), |(k, v)| (k.as_str(), v.as_str()));
    let workflow_name_value = workflow_name_filter.unwrap_or("");
    let exec_id_search_value = exec_id_search.unwrap_or("");

    html! {
        form.filters method="get" action="workflows" {
            label {
                "State"
                select name="state" {
                    option value="" { "All" }
                    @for state in KNOWN_STATES {
                        @let selected = state_filter.is_some_and(|filter| filter == *state);
                        @if selected {
                            option value=(*state) selected { (*state) }
                        } @else {
                            option value=(*state) { (*state) }
                        }
                    }
                }
            }
            label {
                "Workflow name"
                input type="text" name="workflow_name" value=(workflow_name_value) placeholder="e.g. onboarding";
            }
            label {
                "Started after"
                input type="text" name="started_after" value=(started_after_raw) placeholder="2026-01-01T00:00:00Z";
                @if let Some(error) = started_after_error {
                    span.field-error role="alert" { (error) }
                }
            }
            label {
                "Started before"
                input type="text" name="started_before" value=(started_before_raw) placeholder="2026-12-31T23:59:59Z";
                @if let Some(error) = started_before_error {
                    span.field-error role="alert" { (error) }
                }
            }
            label {
                "Exec ID search"
                input type="text" name="exec_id_search" value=(exec_id_search_value) placeholder="UUID prefix…";
            }
            label {
                "Search attr key"
                input type="text" name="search_attr_key" value=(attr_key) placeholder="e.g. tenant";
            }
            label {
                "Search attr value"
                input type="text" name="search_attr_value" value=(attr_value) placeholder="e.g. acme";
            }
            label {
                "Per page"
                input type="number" name="limit" min="1" max=(MAX_PAGE_SIZE) value=(limit);
            }
            button type="submit" { "Apply" }
            a.reset href="workflows" { "Reset" }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn render_pagination(
    page: i64,
    limit: i64,
    has_next: bool,
    state_filter: Option<&str>,
    workflow_name_filter: Option<&str>,
    search_attr_filter: Option<&(String, String)>,
    started_after_raw: &str,
    started_before_raw: &str,
    exec_id_search: Option<&str>,
) -> Markup {
    let base_query = build_query_string(
        limit,
        state_filter,
        workflow_name_filter,
        search_attr_filter,
        started_after_raw,
        started_before_raw,
        exec_id_search,
    );

    html! {
        div.pagination {
            @if page > 0 {
                a href={ "workflows?page=" (page - 1) (PreEscaped(&base_query)) } {
                    (PreEscaped("&larr;")) " Previous"
                }
            } @else {
                span.disabled { (PreEscaped("&larr;")) " Previous" }
            }

            span { "Page " (page + 1) }

            @if has_next {
                a href={ "workflows?page=" (page + 1) (PreEscaped(&base_query)) } {
                    "Next " (PreEscaped("&rarr;"))
                }
            } @else {
                span.disabled { "Next " (PreEscaped("&rarr;")) }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn build_query_string(
    limit: i64,
    state_filter: Option<&str>,
    workflow_name_filter: Option<&str>,
    search_attr_filter: Option<&(String, String)>,
    started_after_raw: &str,
    started_before_raw: &str,
    exec_id_search: Option<&str>,
) -> String {
    let mut out = String::new();
    if limit != DEFAULT_PAGE_SIZE {
        let _ = write!(out, "&limit={limit}");
    }
    if let Some(state) = state_filter {
        let _ = write!(out, "&state={}", url_encode(state));
    }
    if let Some(name) = workflow_name_filter {
        let _ = write!(out, "&workflow_name={}", url_encode(name));
    }
    if let Some((key, value)) = search_attr_filter {
        let _ = write!(out, "&search_attr_key={}", url_encode(key));
        let _ = write!(out, "&search_attr_value={}", url_encode(value));
    }
    // Carries the raw text through, valid or not — a bound the operator is
    // still correcting (an invalid value with its inline error, see
    // `parse_started_bound`) must not silently vanish from a Next/Previous
    // link before they've resolved it.
    if !started_after_raw.is_empty() {
        let _ = write!(out, "&started_after={}", url_encode(started_after_raw));
    }
    if !started_before_raw.is_empty() {
        let _ = write!(out, "&started_before={}", url_encode(started_before_raw));
    }
    if let Some(search) = exec_id_search {
        let _ = write!(out, "&exec_id_search={}", url_encode(search));
    }
    out
}

const DETAIL_EVENT_PAGE_SIZE: i64 = 100;
/// Maximum activity-type events fetched for the attempts panel.
/// Heartbeats are excluded from the filter, so this cap is only reached
/// on executions with a very large number of distinct activity attempts.
const ACTIVITY_PANEL_MAX_EVENTS: i64 = 2000;
const SIGNAL_UPDATE_TYPES: &[&str] = &[
    "SignalReceived",
    "UpdateAdmitted",
    "UpdateCompleted",
    "UpdateFailed",
];
const SIGNAL_UPDATE_PANEL_LIMIT: usize = 20;
/// Maximum durable workflow-log lines rendered on the detail page (issue #790).
/// The API route (`GET /workflows/{id}/logs`) paginates the full set; the panel
/// shows the FIRST window (oldest-first, matching the store's `seq` order) and
/// links out for the rest.
const WORKFLOW_LOG_PANEL_LIMIT: i64 = 200;
const ACTIVITY_PANEL_EVENT_TYPES: &[&str] = &[
    "ActivityScheduled",
    "ActivityStarted",
    "ActivityCompleted",
    "ActivityFailed",
    "ActivityTimedOut",
    // ActivityHeartbeat intentionally excluded — heartbeats are high-cardinality
    // and carry no information useful for the attempts panel triage view.
    "ActivityAwaitingExternal",
    "ActivityCompletedExternally",
    "ActivityFailedExternally",
    "ActivityExternalDeadlineExtended",
    "LocalActivityScheduled",
    "LocalActivityCompleted",
    "LocalActivityFailed",
    "LocalActivityExhausted",
];

/// Extract a string field from the inner `data` object of an adjacently-tagged event payload.
///
/// Events are stored as `{"type": "...", "data": {...}}`. This helper reaches through the outer
/// wrapper so callers don't have to repeat the two-step lookup everywhere.
fn event_data_field<'a>(event_data: &'a Value, field: &str) -> Option<&'a str> {
    event_data.get("data")?.get(field)?.as_str()
}

/// Extract a numeric field from the inner `data` object of an adjacently-tagged event payload.
fn event_data_u64(event_data: &Value, field: &str) -> Option<u64> {
    event_data.get("data")?.get(field)?.as_u64()
}

/// Map a raw `event_type` string to a human-readable label.
///
/// `execution_state` disambiguates the `WorkflowCancelled` event, which is
/// reused for force-terminate (issue #504, no new event variant): a terminal
/// `WorkflowCancelled` on a `TERMINATED` execution is labelled "Workflow
/// terminated" so the timeline matches the (already correct) state badge.
fn event_human_label(event_type: &str, event_data: &Value, execution_state: &str) -> String {
    match event_type {
        "WorkflowStarted" => "Workflow started".to_string(),
        "WorkflowCompleted" => "Workflow completed".to_string(),
        "WorkflowFailed" => "Workflow failed".to_string(),
        "WorkflowCancelled" => {
            if execution_state == "TERMINATED" {
                "Workflow terminated".to_string()
            } else {
                "Workflow cancelled".to_string()
            }
        }
        "WorkflowTerminated" => "Workflow terminated".to_string(),
        "ActivityScheduled" => {
            let name = event_data_field(event_data, "name").unwrap_or("?");
            format!("Activity scheduled: {name}")
        }
        "ActivityStarted" => "Activity started".to_string(),
        "ActivityCompleted" => "Activity completed".to_string(),
        "ActivityFailed" => {
            let err = event_data_field(event_data, "error").unwrap_or("error");
            format!("Activity failed: {}", truncate_error(err))
        }
        "ActivityTimedOut" => "Activity timed out".to_string(),
        "ActivityHeartbeat" => "Activity heartbeat".to_string(),
        "ActivityAwaitingExternal" => {
            let name = event_data_field(event_data, "name").unwrap_or("?");
            format!("Activity awaiting external: {name}")
        }
        "ActivityCompletedExternally" => "Activity completed externally".to_string(),
        "ActivityFailedExternally" => "Activity failed externally".to_string(),
        "ActivityExternalDeadlineExtended" => "External activity deadline extended".to_string(),
        "TimerStarted" => "Timer started".to_string(),
        "TimerFired" => "Timer fired".to_string(),
        "TimerCancelled" => "Timer cancelled".to_string(),
        "SignalReceived" => {
            let name = event_data_field(event_data, "signal_name").unwrap_or("?");
            format!("Signal received: {name}")
        }
        "ChildWorkflowStarted" => "Child workflow started".to_string(),
        "ChildWorkflowCompleted" => "Child workflow completed".to_string(),
        "ChildWorkflowFailed" => "Child workflow failed".to_string(),
        "UpdateAdmitted" => "Update admitted".to_string(),
        "UpdateCompleted" => "Update completed".to_string(),
        "UpdateFailed" => "Update failed".to_string(),
        "LocalActivityScheduled" => "Local activity scheduled".to_string(),
        "LocalActivityCompleted" => "Local activity completed".to_string(),
        "LocalActivityFailed" => "Local activity failed".to_string(),
        "LocalActivityExhausted" => "Local activity exhausted".to_string(),
        "VersionMarker" => "Version marker".to_string(),
        "ContinueAsNew" => "Continue as new".to_string(),
        other => other.to_string(),
    }
}

/// A grouped summary row for the activity attempts panel.
struct ActivityAttemptRow {
    name: String,
    attempt_count: usize,
    last_status: String,
    last_ts: String,
    last_error: Option<String>,
}

fn collect_activity_attempts(events: &[HarvestEvent]) -> Vec<ActivityAttemptRow> {
    let mut order: Vec<String> = Vec::new();
    let mut groups: HashMap<String, ActivityAttemptRow> = HashMap::new();

    for event in events {
        if !ACTIVITY_PANEL_EVENT_TYPES.contains(&event.event_type.as_str()) {
            continue;
        }
        let Some(aid) = event_data_field(&event.event_data, "activity_id") else {
            continue;
        };
        let aid = aid.to_string();
        if !groups.contains_key(&aid) {
            let name = event_data_field(&event.event_data, "name")
                .unwrap_or("")
                .to_string();
            order.push(aid.clone());
            groups.insert(
                aid.clone(),
                ActivityAttemptRow {
                    name,
                    attempt_count: 0,
                    last_status: event.event_type.clone(),
                    last_ts: format_timestamp(Some(event.timestamp)),
                    last_error: None,
                },
            );
        }
        if let Some(row) = groups.get_mut(&aid) {
            // Scheduling events: one per attempt for regular activities, one total
            // for local activities (retries are tracked via the attempt field on
            // LocalActivityFailed/LocalActivityExhausted instead).
            if matches!(
                event.event_type.as_str(),
                "ActivityScheduled" | "ActivityAwaitingExternal" | "LocalActivityScheduled"
            ) {
                row.attempt_count += 1;
                if row.name.is_empty() {
                    row.name = event_data_field(&event.event_data, "name")
                        .unwrap_or("")
                        .to_string();
                }
            }
            // Failure events carry an authoritative `attempt` count. Use max() so the
            // panel is accurate whether or not every scheduled event was captured.
            // This covers regular retries, external failures, and local activity retries.
            // `attempt` is serialized as a JSON number, so use the u64 accessor.
            if matches!(
                event.event_type.as_str(),
                "ActivityFailed" | "LocalActivityFailed" | "LocalActivityExhausted"
            ) && let Some(n) = event_data_u64(&event.event_data, "attempt")
            {
                row.attempt_count = row
                    .attempt_count
                    .max(usize::try_from(n).unwrap_or(usize::MAX));
            }
            // Copy the error message from any failure event type.
            if matches!(
                event.event_type.as_str(),
                "ActivityFailed"
                    | "ActivityFailedExternally"
                    | "LocalActivityFailed"
                    | "LocalActivityExhausted"
            ) {
                row.last_error =
                    event_data_field(&event.event_data, "error").map(ToOwned::to_owned);
            }
            row.last_status.clone_from(&event.event_type);
            row.last_ts = format_timestamp(Some(event.timestamp));
        }
    }

    order
        .into_iter()
        .filter_map(|id| groups.remove(&id))
        .collect()
}

#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
fn render_workflow_detail(
    execution: &WorkflowExecution,
    total_events: i64,
    page_events: &[HarvestEvent],
    activity_events: &[HarvestEvent],
    signal_update_events: &[HarvestEvent],
    signal_update_overflow: bool,
    children: &[WorkflowExecution],
    event_page: i64,
    blocked_on: &BlockedOnData,
    flash: Option<&str>,
    continue_as_new_threshold: Option<u64>,
    logs: &WorkflowLogsPanelData<'_>,
) -> Markup {
    let exec_id_str = execution.id.to_string();
    let title = format!("{} · Vantage", execution.workflow_name);
    let detail_badge_class = format!("badge {}", badge_class(&execution.state));

    // Duration string.
    let duration = execution.completed_at.map(|end| {
        let secs = (end - execution.started_at).num_seconds().max(0);
        if secs < 60 {
            format!("{secs}s")
        } else if secs < 3600 {
            format!("{}m {}s", secs / 60, secs % 60)
        } else {
            format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
        }
    });

    // Activity attempts from the pre-filtered activity events.
    let activity_attempts = collect_activity_attempts(activity_events);

    // Signal/update panel counts.
    let signal_update_shown = signal_update_events.len();
    let signal_update_label_total = if signal_update_overflow {
        signal_update_shown + 1
    } else {
        signal_update_shown
    };

    // Pagination arithmetic based on the DB-level total.
    let total_events_usize = usize::try_from(total_events).unwrap_or(usize::MAX);
    let page_size = usize::try_from(DETAIL_EVENT_PAGE_SIZE).unwrap_or(100);
    // Preserved across every event-pagination link so paging the timeline does
    // not silently reset the log-level filter (and vice-versa).
    let selected_log_level = logs
        .level_filter
        .map(autumn_harvest::WorkflowLogLevel::as_str);
    let event_page_idx = usize::try_from(event_page).unwrap_or(0);
    let page_start = event_page_idx
        .saturating_mul(page_size)
        .min(total_events_usize);
    let page_end = (page_start + page_events.len()).min(total_events_usize);
    let has_prev_page = event_page > 0;
    let has_next_page = page_end < total_events_usize;
    let last_page = if total_events == 0 {
        0_i64
    } else {
        (total_events - 1) / DETAIL_EVENT_PAGE_SIZE
    };

    let body = html! {
        div.detail-row { a.back href="../workflows" { (PreEscaped("&larr;")) " Back to workflows" } }

        h2 {
            (execution.workflow_name) " "
            span class=(detail_badge_class) aria-label={ "Status: " (execution.state) } role="status" {
                (execution.state)
            }
        }

        @if let Some(message) = flash {
            div.flash role="status" tabindex="-1" autofocus { (message) }
        }

        @if let Some(error) = execution.error.as_deref() {
            div."error-banner" {
                strong { "Error:" } " " (error)
            }
        }

        // Operator actions — use the exec_id in action URLs so they resolve correctly
        // whether the router is mounted at "/" or at a subpath like "/api/harvest/ui".
        div."operator-actions" {
            form method="post" action={ (exec_id_str) "/cancel" }
                  onsubmit="return confirm('Cancel this workflow execution?')" {
                button.danger type="submit" { "Cancel" }
            }
            @let terminal = is_terminal_workflow_state(&execution.state);
            @if execution.state == "PAUSED" {
                // Paused executions show a Resume action (issue #383).
                form method="post" action={ (exec_id_str) "/resume" } {
                    button type="submit" { "Resume" }
                }
            } @else {
                // Pause is disabled once the workflow is terminal.
                form method="post" action={ (exec_id_str) "/pause" }
                      onsubmit="return confirm('Pause this workflow execution?')" {
                    button type="submit" disabled[terminal]
                        title=[terminal.then_some("Workflow is terminal")] { "Pause" }
                }
            }
            // Forceful sibling of Cancel — seals the run TERMINATED (issue #788).
            // Disabled once the workflow is terminal, exactly like Pause.
            form method="post" action={ (exec_id_str) "/terminate" }
                  onsubmit="return confirm('Force-terminate this workflow execution? This seals it as TERMINATED.')" {
                button.danger type="submit" disabled[terminal]
                    title=[terminal.then_some("Workflow is terminal")] { "Terminate" }
            }
            details style="display:inline-block" {
                summary style="cursor:pointer;color:#93c5fd;font-size:12px;display:inline-block;padding:6px 12px;border:1px solid #2563eb;border-radius:6px" { "Send signal" }
                form method="post" action={ (exec_id_str) "/signal" } style="margin-top:8px;background:#1e293b;border:1px solid #334155;border-radius:6px;padding:12px;display:flex;flex-direction:column;gap:8px;min-width:280px" {
                    label style="font-size:12px;color:#94a3b8" {
                        "Signal name"
                        input type="text" name="signal_name" required placeholder="e.g. approve" style="display:block;width:100%;margin-top:4px;background:#0f172a;color:#e2e8f0;border:1px solid #334155;border-radius:4px;padding:6px 8px;font-size:12px";
                    }
                    label style="font-size:12px;color:#94a3b8" {
                        "Payload (JSON)"
                        textarea name="payload" placeholder="{}" rows="3" style="display:block;width:100%;margin-top:4px;background:#0f172a;color:#e2e8f0;border:1px solid #334155;border-radius:4px;padding:6px 8px;font-family:ui-monospace,monospace;font-size:12px" {}
                    }
                    button type="submit" style="background:#2563eb;color:#fff;border:0;border-radius:6px;padding:6px 12px;font-size:12px;cursor:pointer;align-self:flex-start" { "Send" }
                }
            }
            details style="display:inline-block" {
                summary style="cursor:pointer;color:#93c5fd;font-size:12px;display:inline-block;padding:6px 12px;border:1px solid #2563eb;border-radius:6px" { "Reset to event N" }
                form method="post" action={ (exec_id_str) "/reset" } style="margin-top:8px;background:#1e293b;border:1px solid #334155;border-radius:6px;padding:12px;display:flex;flex-direction:column;gap:8px;min-width:280px" {
                    label style="font-size:12px;color:#94a3b8" {
                        "Event # (1-based, as shown in timeline)"
                        input type="number" name="reset_to_event_id" min="1" required placeholder="1" style="display:block;width:100%;margin-top:4px;background:#0f172a;color:#e2e8f0;border:1px solid #334155;border-radius:4px;padding:6px 8px;font-size:12px";
                    }
                    label style="font-size:12px;color:#94a3b8" {
                        "Reason"
                        input type="text" name="reason" placeholder="rollback" style="display:block;width:100%;margin-top:4px;background:#0f172a;color:#e2e8f0;border:1px solid #334155;border-radius:4px;padding:6px 8px;font-size:12px";
                    }
                    button type="submit" style="background:#92400e;color:#fff;border:0;border-radius:6px;padding:6px 12px;font-size:12px;cursor:pointer;align-self:flex-start" onclick="return confirm('Reset this workflow execution? This is destructive.')" { "Reset" }
                }
            }
            details style="display:inline-block" {
                summary style="cursor:pointer;color:#93c5fd;font-size:12px;display:inline-block;padding:6px 12px;border:1px solid #2563eb;border-radius:6px" { "Trigger update" }
                form method="post" action={ (exec_id_str) "/trigger-update" } style="margin-top:8px;background:#1e293b;border:1px solid #334155;border-radius:6px;padding:12px;display:flex;flex-direction:column;gap:8px;min-width:280px" {
                    label style="font-size:12px;color:#94a3b8" {
                        "Update name"
                        input type="text" name="update_name" required placeholder="e.g. set_priority" style="display:block;width:100%;margin-top:4px;background:#0f172a;color:#e2e8f0;border:1px solid #334155;border-radius:4px;padding:6px 8px;font-size:12px";
                    }
                    label style="font-size:12px;color:#94a3b8" {
                        "Payload (JSON)"
                        textarea name="payload" placeholder="{}" rows="3" style="display:block;width:100%;margin-top:4px;background:#0f172a;color:#e2e8f0;border:1px solid #334155;border-radius:4px;padding:6px 8px;font-family:ui-monospace,monospace;font-size:12px" {}
                    }
                    button type="submit" style="background:#2563eb;color:#fff;border:0;border-radius:6px;padding:6px 12px;font-size:12px;cursor:pointer;align-self:flex-start" { "Submit" }
                }
            }
            // issue #960: link to the standalone execution timeline (Gantt) —
            // reads as a "Timeline" tab of the execution detail view. The
            // #slowest fragment scroll-focuses the slowest span on load.
            a.btn href={ (exec_id_str) "/timeline#slowest" } {
                "Timeline"
            }
            a.btn href={ "../../workflows/" (exec_id_str) "/history/export" } {
                "Export history"
            }
        }

        div.card {
            h3 { "Metadata" }
            div.kv {
                (kv("Execution ID", &exec_id_str, true))
                (kv("Workflow ID", &execution.workflow_id, true))
                (kv("Run ID", &execution.run_id.to_string(), true))
                (kv("Shard ID", &execution.shard_id.to_string(), true))
                (kv("Queue", &execution.queue_name, true))
                (kv("Started", &format_timestamp(Some(execution.started_at)), false))
                (kv("Completed", &format_timestamp(execution.completed_at), false))
                @if let Some(dur) = &duration {
                    (kv("Duration", dur, false))
                }
                @if let Some(parent) = execution.parent_id {
                    div.k { "Parent" }
                    div.v {
                        a href={ "../../workflows/" (parent.to_string()) } {
                            code { (short_id(&parent.to_string())) }
                        }
                    }
                }
                @if let Some(worker) = execution.sticky_worker_id.as_deref() {
                    (kv("Current worker", worker, true))
                }
                @if let Some(timeout) = execution.execution_timeout {
                    (kv("Execution timeout", &format!("{}s", timeout.num_seconds()), false))
                }
                @if let Some(ref build_id) = execution.assigned_build_id {
                    div.k { "Assigned build" }
                    div.v {
                        a href={ "../build-routing?build_id=" (url_encode(build_id)) }
                           title="View in Build Routing" {
                            code { (build_id) }
                        }
                    }
                }
                @if let Some(threshold) = continue_as_new_threshold {
                    (kv("History events", &format!("{total_events} / threshold: {threshold}"), false))
                } @else {
                    (kv("History events", &total_events.to_string(), false))
                }
                @if let Some(ref owner) = execution.owner {
                    div.k { "Owner" }
                    div.v {
                        span class="badge badge-owner" { (owner) }
                    }
                }
                @if let Some(ref sev) = execution.severity {
                    @let sev_class = match sev.to_lowercase().as_str() {
                        "sev1" => "badge-sev-sev1",
                        "sev2" => "badge-sev-sev2",
                        "sev3" => "badge-sev-sev3",
                        "sev4" => "badge-sev-sev4",
                        _ => "",
                    };
                    div.k { "Severity" }
                    div.v {
                        span class={ "badge " (sev_class) } { (sev.to_uppercase()) }
                    }
                }
                @if let Some(ref rb) = execution.runbook_url {
                    div.k { "Runbook" }
                    div.v {
                        a href=(rb) target="_blank" rel="noopener noreferrer" { (rb) }
                    }
                }
            }
        }

        // Blocked-on panel
        (render_blocked_on_panel(blocked_on))

        (json_card("Input", &execution.input))
        @if let Some(output) = execution.output.as_ref() {
            (json_card("Output", output))
        }
        @if let Some(memo) = execution.memo.as_ref() {
            (json_card("Memo", memo))
        }
        @if let Some(attrs) = execution.search_attrs.as_ref() {
            (json_card("Search attributes", attrs))
        }

        // Activity attempts panel
        @if !activity_attempts.is_empty() {
            div.card {
                h3 { "Activity attempts" }
                table {
                    thead {
                        tr {
                            th { "Activity" }
                            th { "Attempts" }
                            th { "Last status" }
                            th { "Last updated" }
                        }
                    }
                    tbody {
                        @for row in &activity_attempts {
                            @let display_name = if row.name.is_empty() { "—".to_string() } else { row.name.clone() };
                            tr {
                                td { (display_name) }
                                td { (row.attempt_count.max(1)) }
                                td {
                                    code { (row.last_status) }
                                    @if let Some(err) = &row.last_error {
                                        " — " (truncate_error(err))
                                    }
                                }
                                td { (row.last_ts) }
                            }
                        }
                    }
                }
            }
        }

        // Children panel
        @if !children.is_empty() {
            div.card {
                h3 { "Children (" (children.len()) ")" }
                table {
                    thead {
                        tr {
                            th { "Exec ID" }
                            th { "Workflow" }
                            th { "Status" }
                            th { "Started" }
                        }
                    }
                    tbody {
                        @for child in children {
                            @let child_id = child.id.to_string();
                            tr {
                                td {
                                    a href={ "../../workflows/" (child_id) } {
                                        code { (short_id(&child_id)) }
                                    }
                                }
                                td { (child.workflow_name) }
                                td { (state_badge(&child.state)) }
                                td { (format_timestamp(Some(child.started_at))) }
                            }
                        }
                    }
                }
            }
        }

        // Signals & updates panel
        @if !signal_update_events.is_empty() {
            div.card {
                @if signal_update_overflow {
                    h3 { "Signals & Updates (showing " (SIGNAL_UPDATE_PANEL_LIMIT) " of " (signal_update_label_total) "+)" }
                } @else {
                    h3 { "Signals & Updates" }
                }
                table {
                    thead {
                        tr {
                            th { "Type" }
                            th { "Name / ID" }
                            th { "Timestamp" }
                        }
                    }
                    tbody {
                        @for event in signal_update_events {
                            @let label =
                                event_human_label(&event.event_type, &event.event_data, &execution.state);
                            @let name_or_id = event_data_field(&event.event_data, "signal_name")
                                .or_else(|| event_data_field(&event.event_data, "update_id"))
                                .unwrap_or("—");
                            tr {
                                td { (label) }
                                td { code { (name_or_id) } }
                                td { (format_timestamp(Some(event.timestamp))) }
                            }
                        }
                    }
                }
            }
        }

        // Durable workflow logs panel (issue #790, AC5)
        @if logs.admin {
            (render_workflow_logs_panel(&exec_id_str, logs, event_page))
        }

        // Event timeline
        div.card {
            h3 { "Event history (" (total_events) " events)" }
            @if total_events == 0 {
                div.empty { "No events recorded yet." }
            } @else {
                // Jump controls for large histories
                @if total_events > DETAIL_EVENT_PAGE_SIZE {
                    div.pagination style="margin-bottom:12px" {
                        @if has_prev_page {
                            a href=(workflow_detail_href(event_page - 1, selected_log_level)) {
                                (PreEscaped("&larr;")) " Previous"
                            }
                        } @else {
                            span.disabled { (PreEscaped("&larr;")) " Previous" }
                        }
                        span { " Events " (page_start + 1) "–" (page_end) " of " (total_events) " " }
                        @if has_next_page {
                            a href=(workflow_detail_href(event_page + 1, selected_log_level)) {
                                "Next " (PreEscaped("&rarr;"))
                            }
                        } @else {
                            span.disabled { "Next " (PreEscaped("&rarr;")) }
                        }
                        a href=(workflow_detail_href(last_page, selected_log_level)) { "Jump to latest" }
                    }
                }
                table {
                    thead {
                        tr {
                            th { "#" }
                            th { "Type" }
                            th { "Timestamp" }
                            th { "Data" }
                        }
                    }
                    tbody {
                        @for event in page_events {
                            @let label =
                                event_human_label(&event.event_type, &event.event_data, &execution.state);
                            @let ts = format_timestamp(Some(event.timestamp));
                            tr {
                                td { (event.event_id + 1) }
                                td title=(event.event_type) {
                                    span.event-label {
                                        (label)
                                        code { "(" (event.event_type) ")" }
                                    }
                                }
                                td { (ts) }
                                td {
                                    details {
                                        summary { "view payload" }
                                        pre { (pretty_json(&event.event_data)) }
                                    }
                                }
                            }
                        }
                    }
                }
                // Bottom pagination with jump-to-event control
                @if total_events > DETAIL_EVENT_PAGE_SIZE {
                    div.pagination style="margin-top:12px" {
                        @if has_prev_page {
                            a href=(workflow_detail_href(event_page - 1, selected_log_level)) {
                                (PreEscaped("&larr;")) " Previous"
                            }
                        } @else {
                            span.disabled { (PreEscaped("&larr;")) " Previous" }
                        }
                        span { "Page " (event_page + 1) }
                        @if has_next_page {
                            a href=(workflow_detail_href(event_page + 1, selected_log_level)) {
                                "Next " (PreEscaped("&rarr;"))
                            }
                        } @else {
                            span.disabled { "Next " (PreEscaped("&rarr;")) }
                        }
                        a href=(workflow_detail_href(last_page, selected_log_level)) { "Jump to latest" }
                        form method="get" style="display:inline-flex;gap:6px;align-items:center;margin-left:8px" {
                            label style="font-size:12px;color:#94a3b8;display:inline-flex;align-items:center;gap:6px" {
                                "Jump to event:"
                                input type="number" name="jump_event" min="1" max=(total_events) placeholder="N"
                                    style="width:70px;background:#1e293b;color:#e2e8f0;border:1px solid #334155;border-radius:4px;padding:4px 6px;font-size:12px";
                            }
                            button type="submit" style="background:#2563eb;color:#fff;border:0;border-radius:4px;padding:4px 10px;font-size:12px;cursor:pointer" { "Go" }
                        }
                    }
                }
            }
        }
    };

    layout(&title, &body, "../")
}

/// Per-row checkpoint rendering decision for the pending-activities table, after
/// applying both the per-activity cap and the cumulative per-page budget (#503
/// review). Carries the observed byte size for the marker text.
#[derive(Clone, Copy)]
enum CheckpointCellState {
    /// No heartbeat checkpoint has been flushed.
    Absent,
    /// Render the checkpoint JSON (within its cap and the page budget).
    Show,
    /// Withheld because this payload's own size exceeded its activity cap.
    Truncated(u64),
    /// Withheld because the cumulative per-page checkpoint budget was exhausted.
    OmittedForBudget(u64),
}

/// Build the per-row checkpoint decisions for the pending-activities table,
/// mirroring the stack API (#503 review): each checkpoint is judged against its
/// activity's effective cap (per-activity `max_result_bytes` raised against the
/// global ceiling), then a cumulative per-page budget — the global cap — bounds
/// the total rendered bytes so a large fan-out can't generate a huge HTML page.
/// The first payload-bearing checkpoint is always shown. The byte size is
/// measured once here via the shared non-allocating counter; the renderer reads
/// the resulting state without re-serializing.
fn plan_checkpoint_cells(blocked_on: &BlockedOnData) -> Vec<CheckpointCellState> {
    let per_item: Vec<(bool, Option<u64>)> = blocked_on
        .activities
        .iter()
        .map(|item| {
            let cap = item
                .activity_name
                .as_deref()
                .and_then(|n| blocked_on.heartbeat_caps.get(n).copied())
                .unwrap_or(blocked_on.heartbeat_details_cap);
            crate::api::heartbeat_details_truncation(item.heartbeat_details.as_ref(), cap)
        })
        .collect();
    // Only present, not-individually-truncated checkpoints participate in the
    // cumulative budget.
    let sizes: Vec<Option<u64>> = per_item
        .iter()
        .map(|(truncated, bytes)| match bytes {
            Some(b) if !truncated => Some(*b),
            _ => None,
        })
        .collect();
    let omit = crate::api::checkpoint_budget_decisions(&sizes, blocked_on.heartbeat_details_cap);
    per_item
        .iter()
        .zip(omit)
        .map(
            |((truncated, bytes), omit_budget)| match (bytes, truncated, omit_budget) {
                (None, _, _) => CheckpointCellState::Absent,
                (Some(b), true, _) => CheckpointCellState::Truncated(*b),
                (Some(b), false, true) => CheckpointCellState::OmittedForBudget(*b),
                (Some(_), false, false) => CheckpointCellState::Show,
            },
        )
        .collect()
}

/// Render the latest heartbeat checkpoint payload for a pending activity as a
/// collapsible JSON cell, from the precomputed [`CheckpointCellState`] (#503).
/// Shows `"—"` when absent, a truncation marker when over the activity cap, and
/// an omission marker when withheld by the per-page budget.
fn render_heartbeat_checkpoint_cell(item: &TaskQueueItem, state: CheckpointCellState) -> Markup {
    html! {
        @match state {
            CheckpointCellState::Absent => "—",
            CheckpointCellState::Show => {
                @if let Some(value) = item.heartbeat_details.as_ref() {
                    details {
                        summary { "checkpoint" }
                        pre { (pretty_json(value)) }
                    }
                } @else {
                    "—"
                }
            }
            CheckpointCellState::Truncated(bytes) => {
                span title="heartbeat payload exceeds the response size cap" {
                    "truncated (" (bytes) " bytes)"
                }
            }
            CheckpointCellState::OmittedForBudget(bytes) => {
                span title="omitted: per-page checkpoint budget exceeded" {
                    "omitted (" (bytes) " bytes)"
                }
            }
        }
    }
}

/// Render the "Pending activities" table, including each activity's latest
/// heartbeat checkpoint judged against its effective (per-activity) cap and the
/// cumulative per-page checkpoint budget (#503).
/// Build a workflow-detail URL that preserves BOTH view dimensions.
///
/// The detail page now has two independent filters -- the event-history page
/// and the log-level filter (issue #790) -- and a bare `?log_level=` /
/// `?event_page=` link silently resets the other one. Every link that changes
/// one dimension goes through here so it carries the other.
///
/// `jump_event` is deliberately NOT preserved: it is a one-shot "take me to
/// event N" action that `event_page` already resolves to a concrete page, so
/// carrying it would re-trigger the jump on every subsequent click.
fn workflow_detail_href(event_page: i64, log_level: Option<&str>) -> String {
    let mut url = format!("?event_page={event_page}");
    if let Some(level) = log_level {
        url.push_str("&log_level=");
        url.push_str(level);
    }
    url
}

/// Inputs for the durable workflow-logs panel (issue #790, AC5).
///
/// Bundled rather than passed as three more positional parameters:
/// `render_workflow_detail` already takes eleven.
#[derive(Debug, Default)]
pub(crate) struct WorkflowLogsPanelData<'a> {
    /// The page of log lines, in emission order (`seq`).
    pub lines: &'a [autumn_harvest::models::HarvestWorkflowLog],
    /// The active `?log_level=` filter; `None` means "all levels".
    pub level_filter: Option<autumn_harvest::WorkflowLogLevel>,
    /// Whether the viewing principal has harvest-admin access. The panel is
    /// rendered only when `true`, mirroring the API route's `require_admin`.
    pub admin: bool,
    /// Whether this execution hit its per-execution log cap.
    ///
    /// Resolved by the caller with a direct probe for the marker row, NOT by
    /// scanning `lines`: the marker sits at `seq = i64::MAX` so it sorts last,
    /// and the panel only loads the FIRST `WORKFLOW_LOG_PANEL_LIMIT` rows. With
    /// the default cap (1,000) a truncated run's marker can never appear in a
    /// 200-row page, so a scan-based check would render `false` for exactly the
    /// runs that dropped the most -- silently, which is the one failure mode
    /// the marker exists to prevent.
    pub truncated: bool,
    /// Whether the log read itself failed (as opposed to returning no rows).
    ///
    /// Without this the empty state affirmatively tells the operator "the sink
    /// is disabled" during an incident where the read actually errored -- e.g.
    /// a node that has not yet run the migration, where every detail page would
    /// claim every run logged nothing.
    pub read_failed: bool,
}

/// Renders the durable per-execution author-log panel (issue #790, AC5).
///
/// Sourced from the SAME `harvest_workflow_logs` rows `GET /workflows/{id}/logs`
/// serves, in the same emission order (`seq`), so the UI and the API can never
/// disagree about what a run logged. Level filtering is a plain link-based
/// filter (`?log_level=`) — no JS, matching the Workers/DLQ page precedent.
///
/// The panel is rendered only for a harvest-admin principal (the caller gates
/// it), mirroring the API route's `require_admin`.
///
/// Logs are observational only (AC7): a run that never logged, or a deployment
/// with the durable sink disabled, shows the empty state rather than an error —
/// there is nothing wrong with either.
fn render_workflow_logs_panel(
    exec_id_str: &str,
    logs: &WorkflowLogsPanelData<'_>,
    event_page: i64,
) -> Markup {
    use autumn_harvest::WorkflowLogLevel;

    let WorkflowLogsPanelData {
        lines,
        level_filter,
        admin: _,
        truncated,
        read_failed,
    } = *logs;

    let capped = i64::try_from(lines.len()).unwrap_or(i64::MAX) >= WORKFLOW_LOG_PANEL_LIMIT;
    let selected = level_filter.map(WorkflowLogLevel::as_str);

    html! {
        div.card {
            h3 { "Logs" }
            div.log-filters style="margin-bottom:12px" {
                @let all_class = if selected.is_none() { "active" } else { "" };
                a class=(all_class) href=(workflow_detail_href(event_page, None)) { "All" }
                @for level in [WorkflowLogLevel::Info, WorkflowLogLevel::Warn, WorkflowLogLevel::Error] {
                    @let wire = level.as_str();
                    @let class = if selected == Some(wire) { "active" } else { "" };
                    " "
                    a class=(class) href=(workflow_detail_href(event_page, Some(wire))) { (wire) }
                }
            }
            @if truncated {
                div.empty {
                    "This execution reached its per-execution log cap; later lines were dropped."
                }
            }
            @if read_failed {
                div.empty {
                    "Could not read this execution's durable log lines. This is a read "
                    "failure, not an empty log \u{2014} see the server logs. If this node has "
                    "not yet run the "
                    code { "20260719000000_harvest_workflow_logs" }
                    " migration, apply it."
                }
            } @else if lines.is_empty() {
                div.empty {
                    @if selected.is_some() {
                        "No log lines at this level."
                    } @else {
                        "No durable log lines recorded. "
                        "Author lines are persisted only when the opt-in sink is enabled "
                        "(HarvestPlugin/HarvestBuilder::workflow_log_persistence)."
                    }
                }
            } @else {
                table {
                    thead {
                        tr {
                            th { "Level" }
                            th { "Time" }
                            th { "Message" }
                        }
                    }
                    tbody {
                        @for line in lines {
                            tr {
                                td { code { (&line.level) } }
                                td { (format_timestamp(Some(line.occurred_at))) }
                                td { (&line.message) }
                            }
                        }
                    }
                }
                @if capped {
                    div.empty style="margin-top:8px" {
                        "Showing the first " (WORKFLOW_LOG_PANEL_LIMIT) " lines. "
                        "Page the rest via "
                        code {
                            "GET /api/harvest/workflows/" (exec_id_str) "/logs"
                            @if let Some(wire) = selected { "?level=" (wire) }
                        }
                        "."
                    }
                }
            }
        }
    }
}

fn render_pending_activities_table(blocked_on: &BlockedOnData) -> Markup {
    let cell_states = plan_checkpoint_cells(blocked_on);
    html! {
        table {
            thead {
                tr {
                    th { "Activity" }
                    th { "State" }
                    th { "Attempt" }
                    th { "Scheduled" }
                    th { "Last heartbeat" }
                    th { "Checkpoint" }
                }
            }
            tbody {
                @for (item, state) in blocked_on.activities.iter().zip(cell_states) {
                    tr {
                        td { (item.activity_name.as_deref().unwrap_or("—")) }
                        td { code { (&item.state) } }
                        td { (item.attempt) }
                        td { (format_timestamp(Some(item.scheduled_at))) }
                        td { (format_timestamp(item.last_heartbeat_at)) }
                        td { (render_heartbeat_checkpoint_cell(item, state)) }
                    }
                }
            }
        }
    }
}

/// Renders the replay-derived open-awaitables section of the blocked-on panel
/// (issue #615). This surfaces the categories the side-table panels below
/// cannot see: awaited-but-unsent signals, pending child workflows, and
/// `await_condition` parks. Sourced from the SAME report the
/// `GET /workflows/{id}/awaitables` endpoint serves.
fn render_awaitables_table(report: &crate::api::WorkflowAwaitablesResponse) -> Markup {
    html! {
        h3 style="margin-top:8px" { "Waiting on (replay-derived)" }
        @if report.wait_set == "history_only" {
            div.empty {
                "Best-effort view from the event log"
                @if let Some(reason) = report.wait_set_reason.as_deref() {
                    " (" code { (reason) } ")"
                }
                "."
            }
        }
        table {
            thead {
                tr {
                    th { "Kind" }
                    th { "Name" }
                    th { "ID" }
                    th { "Since" }
                    th { "Deadline" }
                }
            }
            tbody {
                @for item in &report.awaitables {
                    tr {
                        td {
                            code { (item.kind.as_str()) }
                            @if item.local { " (local)" }
                            @if item.external { " (external)" }
                        }
                        td { (item.name.as_deref().unwrap_or("—")) }
                        td {
                            @if let Some(id) = item.id.as_deref() {
                                code { (id) }
                            } @else {
                                "—"
                            }
                        }
                        td { (format_timestamp(item.since)) }
                        td { (format_timestamp(item.deadline)) }
                    }
                }
            }
        }
        @if report.truncated {
            div.empty {
                "Truncated to the first " (report.category_cap)
                " per category ("
                @for (i, kind) in report.truncated_kinds.iter().enumerate() {
                    @if i > 0 { ", " }
                    code { (kind.as_str()) }
                }
                ")."
            }
        }
    }
}

fn render_blocked_on_panel(blocked_on: &BlockedOnData) -> Markup {
    let has_awaitables = blocked_on
        .awaitables
        .as_ref()
        .is_some_and(|r| !r.awaitables.is_empty());
    let has_anything = !blocked_on.activities.is_empty()
        || !blocked_on.external_tasks.is_empty()
        || !blocked_on.timers.is_empty()
        || !blocked_on.signals.is_empty()
        || has_awaitables;

    html! {
        div.card {
            h3 { "Blocked on" }
            @if !has_anything {
                div.empty { "No pending work items." }
            } @else {
                @if let Some(report) = blocked_on.awaitables.as_ref() {
                    @if !report.awaitables.is_empty() {
                        (render_awaitables_table(report))
                    }
                }
                @if !blocked_on.activities.is_empty() {
                    h3 style="margin-top:8px" { "Pending activities" }
                    (render_pending_activities_table(blocked_on))
                }
                @if !blocked_on.external_tasks.is_empty() {
                    h3 style="margin-top:8px" { "Pending external activities" }
                    table {
                        thead {
                            tr {
                                th { "Activity" }
                                th { "Deadline" }
                                th { "Scheduled" }
                            }
                        }
                        tbody {
                            @for task in &blocked_on.external_tasks {
                                tr {
                                    td { (&task.name) }
                                    td { (format_timestamp(Some(task.schedule_to_close_at))) }
                                    td { (format_timestamp(Some(task.created_at))) }
                                }
                            }
                        }
                    }
                }
                @if !blocked_on.timers.is_empty() {
                    h3 style="margin-top:8px" { "Pending timers" }
                    table {
                        thead {
                            tr {
                                th { "Timer ID" }
                                th { "Fires at" }
                            }
                        }
                        tbody {
                            @for timer in &blocked_on.timers {
                                tr {
                                    td { code { (&timer.timer_id) } }
                                    td { (format_timestamp(Some(timer.fires_at))) }
                                }
                            }
                        }
                    }
                }
                @if !blocked_on.signals.is_empty() {
                    h3 style="margin-top:8px" { "Pending signals" }
                    table {
                        thead {
                            tr {
                                th { "Signal name" }
                                th { "Received at" }
                            }
                        }
                        tbody {
                            @for sig in &blocked_on.signals {
                                tr {
                                    td { (&sig.signal_name) }
                                    td { (format_timestamp(Some(sig.received_at))) }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn kv(key: &str, value: &str, mono: bool) -> Markup {
    html! {
        div.k { (key) }
        @if mono {
            div.v { code { (value) } }
        } @else {
            div.v { (value) }
        }
    }
}

fn json_card(title: &str, value: &Value) -> Markup {
    html! {
        div.card {
            h3 { (title) }
            pre { (pretty_json(value)) }
        }
    }
}

fn pretty_json(value: &Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
}

fn format_timestamp(ts: Option<DateTime<Utc>>) -> String {
    ts.map_or_else(
        || "—".to_string(),
        |ts| ts.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
    )
}

fn state_badge(state: &str) -> Markup {
    let class = format!("badge {}", badge_class(state));
    let aria = format!("Status: {state}");
    html! {
        span class=(class) aria-label=(aria) role="status" { (state) }
    }
}

fn badge_class(state: &str) -> &'static str {
    match state {
        "RUNNING" => "RUNNING",
        "COMPLETED" => "COMPLETED",
        "FAILED" => "FAILED",
        "CANCELLED" => "CANCELLED",
        "TERMINATED" => "TERMINATED",
        _ => "UNKNOWN",
    }
}

fn short_id(id: &str) -> String {
    id.chars().take(8).collect::<String>() + "…"
}

fn url_encode(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for byte in input.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(byte as char);
        } else {
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

/// Escape a string for safe embedding inside a single-quoted JavaScript string literal.
///
/// Replaces `\` with `\\` and `'` with `\'` so the value cannot break out of the
/// surrounding `confirm('...')` or similar inline handler, preventing XSS via
/// operator-supplied build IDs or queue names.
fn js_escape(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('\'', "\\'")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029")
}

fn layout(title: &str, body: &Markup, base_href: &str) -> Markup {
    html! {
        (PreEscaped("<!DOCTYPE html>"))
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width,initial-scale=1";
                title { (title) }
                style { (PreEscaped(STYLE)) }
            }
            body {
                header {
                    h1 {
                        a href={ (base_href) "workflows" } { "🔭 Vantage" }
                        span.subtitle { "Harvest dashboard" }
                    }
                    nav {
                        a.active href={ (base_href) "workflows" } { "Workflows" }
                        a href={ (base_href) "dags" } { "DAGs" }
                        a href={ (base_href) "workers" } { "Workers" }
                        a href={ (base_href) "schedules" } { "Schedules" }
                        a href={ (base_href) "dead-letters" } { "Dead Letters" }
                        a href={ (base_href) "build-routing" } { "Build Routing" }
                    }
                }
                main { (body) }
                footer { "Read-only dashboard — autumn-harvest" }
            }
        }
    }
}

fn render_dag_list(dags: &[DagUiSummary], shard_errors: &[(ShardId, String)]) -> Markup {
    let body = html! {
        h2 { "DAGs" }
        @for (shard_id, error) in shard_errors {
            div.shard-error {
                strong { "Shard " (shard_id.as_i32()) " unavailable: " }
                (error)
            }
        }
        table {
            thead {
                tr {
                    th { "Name" }
                    th { "Schedule" }
                    th { "Paused" }
                    th { "Next Run" }
                    th { "Max Active" }
                    th { "Catchup" }
                    th { "Task Count" }
                }
            }
            tbody {
                @for dag in dags {
                    tr {
                        td { a href={ "dags/" (&dag.name) } { (&dag.name) } }
                        td { (dag.schedule_expr.clone().unwrap_or_else(|| "—".to_string())) }
                        td { (if dag.is_paused { "Yes" } else { "No" }) }
                        td { (format_timestamp(dag.next_run_at)) }
                        td { (dag.max_active_runs) }
                        td { (if dag.catchup { "Yes" } else { "No" }) }
                        td { (dag.task_count) }
                    }
                }
            }
        }
    };
    layout_dag_detail("DAGs · Vantage", &body, "", None)
}

fn dag_summary_from_registered(name: &str, dag: &RegisteredDag) -> DagUiSummary {
    DagUiSummary {
        name: name.to_string(),
        schedule_expr: dag.schedule.as_ref().map(schedule_expr_for_ui_summary),
        task_count: dag.task_count(),
        is_paused: false,
        next_run_at: None,
        max_active_runs: i32::try_from(dag.max_active_runs).unwrap_or(i32::MAX),
        catchup: dag.catchup,
    }
}

fn merge_dag_schedule_row(entry: &mut DagUiSummary, row: &HarvestSchedule) {
    if entry.schedule_expr.is_none() {
        entry.schedule_expr.clone_from(&row.schedule_expr);
    }
    entry.is_paused = row.is_paused;
    entry.next_run_at = row.next_run_at;
    entry.max_active_runs = row.max_active_runs;
    entry.catchup = row.catchup;
}

fn schedule_expr_for_ui_summary(schedule: &Schedule) -> String {
    match schedule {
        Schedule::Cron(expr) => expr.clone(),
        Schedule::Interval(duration) => {
            if duration.subsec_nanos() == 0 {
                format!("@every {}s", duration.as_secs())
            } else {
                format!(
                    "@every {}.{:09}s",
                    duration.as_secs(),
                    duration.subsec_nanos()
                )
            }
        }
        Schedule::Manual => "@manual".to_string(),
        Schedule::CronInTimezone { expr, tz } => format!("{expr} [{tz}]"),
    }
}

// ── Issue #957 — DAG run graph rendering ─────────────────────────────────────
//
// Node statuses are NOT re-derived here: they come straight from
// `dag_graph::build_run_graph` (the same in-process call the #690 API handler
// makes), so the graph can never disagree with the API or the #366 retry
// resolver. The helpers below turn that `Vec<DagRunNode>` into an inline,
// server-rendered SVG laid out level-by-level, plus a click-through node detail
// panel and a retry action.

/// Node graph state for the DAG detail page, resolved by `dag_detail_ui`.
/// `Copy` (all fields are shared references) so it can be passed by value into
/// `render_dag_detail` without tripping `needless_pass_by_value`.
#[derive(Clone, Copy)]
enum DagGraphView<'a> {
    /// A unified DAG with a selected run: annotated node graph.
    Run {
        nodes: &'a [DagRunNode],
        run_state: &'a str,
    },
    /// A classic (non-unified) DAG: #690 rejects these with `400`, so there is
    /// no unified topology to render.
    Classic,
    /// A unified DAG with no run selected.
    NoRun,
}

/// Node counts at or above this threshold render inside a scrollable container
/// with an explicit note rather than being disabled (issue #957 AC).
const DAG_GRAPH_LARGE_THRESHOLD: usize = 200;

/// SVG geometry constants (px).
const DAG_NODE_W: f64 = 160.0;
const DAG_NODE_H: f64 = 40.0;
const DAG_COL_W: f64 = 210.0;
const DAG_ROW_H: f64 = 64.0;
const DAG_PAD: f64 = 20.0;

/// Fill colour keyed to node status. Distinct per status (colour is a secondary
/// cue only; `dag_node_status_icon` carries the accessible, colour-independent
/// distinction).
const fn dag_node_status_fill(status: DagNodeStatus) -> &'static str {
    match status {
        DagNodeStatus::Succeeded => "#166534",
        DagNodeStatus::Failed => "#991b1b",
        DagNodeStatus::TimedOut => "#b45309",
        DagNodeStatus::Cancelled => "#6b7280",
        DagNodeStatus::Running => "#1d4ed8",
        DagNodeStatus::Pending => "#334155",
        DagNodeStatus::Skipped => "#475569",
        DagNodeStatus::Waiting => "#7c3aed",
    }
}

/// A distinct glyph per status so the graph is legible without colour (a11y).
const fn dag_node_status_icon(status: DagNodeStatus) -> &'static str {
    match status {
        DagNodeStatus::Succeeded => "✓",
        DagNodeStatus::Failed => "✗",
        DagNodeStatus::TimedOut => "⏱",
        DagNodeStatus::Cancelled => "⊘",
        DagNodeStatus::Running => "●",
        DagNodeStatus::Pending => "○",
        DagNodeStatus::Skipped => "↳",
        DagNodeStatus::Waiting => "⧗",
    }
}

/// Human-readable label per status (matches the #690 bounded enum).
const fn dag_node_status_label(status: DagNodeStatus) -> &'static str {
    match status {
        DagNodeStatus::Succeeded => "Succeeded",
        DagNodeStatus::Failed => "Failed",
        DagNodeStatus::TimedOut => "Timed out",
        DagNodeStatus::Cancelled => "Cancelled",
        DagNodeStatus::Running => "Running",
        DagNodeStatus::Pending => "Pending",
        DagNodeStatus::Skipped => "Skipped",
        DagNodeStatus::Waiting => "Waiting",
    }
}

/// Whether a "Retry from this node" action should be offered: the run must be
/// terminally retryable (matching #366's accepted source states) and the node
/// must be an attempted-but-not-succeeded node (so the #366 resolver won't
/// reject it as never-attempted / already-succeeded).
fn node_retry_offered(run_state: &str, status: DagNodeStatus) -> bool {
    matches!(run_state, "FAILED" | "CANCELLED" | "TIMED_OUT")
        && matches!(
            status,
            DagNodeStatus::Failed | DagNodeStatus::TimedOut | DagNodeStatus::Cancelled
        )
}

/// Computed SVG geometry for a run graph: one `(x, y)` per node index, plus the
/// overall canvas size. Deterministic (pure function of the level structure).
#[derive(Debug, Clone, PartialEq)]
struct GraphLayout {
    pos: Vec<(f64, f64)>,
    width: f64,
    height: f64,
}

/// Lay nodes out level-by-level: each execution level is a column (x by level
/// index), each node within a level is a row (y by position). Matches the
/// executor's level semantics (#256/#366). Deterministic.
fn build_graph_layout(levels: &[Vec<usize>], node_count: usize) -> GraphLayout {
    let mut pos = vec![(0.0_f64, 0.0_f64); node_count];
    let mut max_rows = 0usize;
    for (col, level) in levels.iter().enumerate() {
        for (row, &idx) in level.iter().enumerate() {
            if idx < node_count {
                #[allow(clippy::cast_precision_loss)]
                let x = (col as f64).mul_add(DAG_COL_W, DAG_PAD);
                #[allow(clippy::cast_precision_loss)]
                let y = (row as f64).mul_add(DAG_ROW_H, DAG_PAD);
                pos[idx] = (x, y);
            }
        }
        max_rows = max_rows.max(level.len());
    }
    #[allow(clippy::cast_precision_loss)]
    let width =
        DAG_PAD.mul_add(2.0, levels.len().max(1) as f64 * DAG_COL_W) - (DAG_COL_W - DAG_NODE_W);
    #[allow(clippy::cast_precision_loss)]
    let height =
        DAG_PAD.mul_add(2.0, max_rows.max(1) as f64 * DAG_ROW_H) - (DAG_ROW_H - DAG_NODE_H);
    GraphLayout { pos, width, height }
}

/// Format an SVG coordinate (whole px).
fn dag_svg_coord(value: f64) -> String {
    format!("{value:.0}")
}

/// Render the run graph as an inline, server-computed SVG. `upstreams[i]` holds
/// the task-index upstreams of node `i` (used for edges); `selected` highlights
/// the clicked node. Wrapped in a scrollable container so a large DAG stays
/// navigable rather than being disabled. maud auto-escapes every text/attribute
/// interpolation (node names, tooltips), so untrusted node names cannot inject
/// markup.
fn render_dag_run_graph_svg(
    nodes: &[DagRunNode],
    levels: &[Vec<usize>],
    upstreams: &[Vec<usize>],
    selected: Option<usize>,
    run_id: uuid::Uuid,
) -> Markup {
    let layout = build_graph_layout(levels, nodes.len());
    // Precompute edge endpoints so the maud macro stays declarative.
    let mut edges: Vec<(f64, f64, f64, f64)> = Vec::new();
    for (idx, ups) in upstreams.iter().enumerate() {
        let Some(&(nx, ny)) = layout.pos.get(idx) else {
            continue;
        };
        for &u in ups {
            if let Some(&(ux, uy)) = layout.pos.get(u) {
                edges.push((
                    ux + DAG_NODE_W,
                    uy + DAG_NODE_H / 2.0,
                    nx,
                    ny + DAG_NODE_H / 2.0,
                ));
            }
        }
    }
    html! {
        div class="dag-graph-scroll" {
            svg xmlns="http://www.w3.org/2000/svg"
                width=(dag_svg_coord(layout.width))
                height=(dag_svg_coord(layout.height))
                role="img"
                aria-label="DAG run graph" {
                @for (x1, y1, x2, y2) in &edges {
                    line x1=(dag_svg_coord(*x1)) y1=(dag_svg_coord(*y1))
                         x2=(dag_svg_coord(*x2)) y2=(dag_svg_coord(*y2))
                         class="dag-edge" {}
                }
                @for (idx, node) in nodes.iter().enumerate() {
                    @let (x, y) = layout.pos.get(idx).copied().unwrap_or((0.0, 0.0));
                    @let selected_class = if selected == Some(idx) { "dag-node selected" } else { "dag-node" };
                    a href=(format!("?run={run_id}&node={idx}")) aria-label=(format!("{} — {}", node.node_name, dag_node_status_label(node.status))) {
                        rect x=(dag_svg_coord(x)) y=(dag_svg_coord(y))
                             width=(dag_svg_coord(DAG_NODE_W)) height=(dag_svg_coord(DAG_NODE_H))
                             rx="6" fill=(dag_node_status_fill(node.status))
                             class=(selected_class) {}
                        text x=(dag_svg_coord(x + 8.0)) y=(dag_svg_coord(y + 25.0)) class="dag-node-label" {
                            (dag_node_status_icon(node.status)) " " (node.node_name)
                        }
                        title { (node.node_name) " — " (dag_node_status_label(node.status)) }
                    }
                }
            }
        }
    }
}

/// A compact status legend for the graph (colour + icon + label per status).
fn dag_status_legend() -> Markup {
    const LEGEND: [DagNodeStatus; 8] = [
        DagNodeStatus::Succeeded,
        DagNodeStatus::Failed,
        DagNodeStatus::TimedOut,
        DagNodeStatus::Cancelled,
        DagNodeStatus::Running,
        DagNodeStatus::Pending,
        DagNodeStatus::Skipped,
        DagNodeStatus::Waiting,
    ];
    html! {
        div class="dag-legend" {
            @for status in LEGEND {
                span {
                    span class="swatch" style=(format!("background:{}", dag_node_status_fill(status))) {}
                    (dag_node_status_icon(status)) " " (dag_node_status_label(status))
                }
            }
        }
    }
}

/// The click-through detail panel for a selected node: status, timing,
/// attempts, dependencies, error (when failed), and — for an
/// attempted-but-not-succeeded node on a terminally-failed run — the
/// "Retry from this node" link into the dry-run confirm page.
fn render_dag_node_panel(
    node: &DagRunNode,
    idx: usize,
    run_state: &str,
    dag_name: &str,
    run_id: uuid::Uuid,
) -> Markup {
    html! {
        div class="card" {
            h3 { "Node " (idx) " · " code { (node.node_name) } }
            p { "Status: " (dag_node_status_icon(node.status)) " " (dag_node_status_label(node.status)) }
            @if let Some(started) = node.started_at {
                p { "Started: " (format_timestamp(Some(started))) }
            }
            @if let Some(finished) = node.finished_at {
                p { "Finished: " (format_timestamp(Some(finished))) }
            }
            p { "Attempts: " (node.attempts) }
            @if !node.depends_on.is_empty() {
                p { "Depends on: " (node.depends_on.join(", ")) }
            }
            @if let Some(error_type) = &node.error_type {
                p { "Error type: " code { (error_type) } }
            }
            @if let Some(error) = &node.error {
                p { "Error: " (error) }
            }
            @if node_retry_offered(run_state, node.status) {
                p {
                    a class="btn reset"
                      href=(format!(
                          "{}/runs/{}/retry?from_node={}",
                          url_encode(dag_name),
                          run_id,
                          url_encode(&node.node_name)
                      )) {
                        "Retry from this node"
                    }
                }
            }
        }
    }
}

/// Success-flash text naming the new (forked) run id (issue #957).
fn dag_retry_success_flash(new_run: &str) -> String {
    format!("Retry started — new run {new_run}")
}

/// Warning-flash text for the audit-write-after-fork case (issue #957, Codex
/// review): the fork **committed** (a new run exists) but the `dag.retry` audit
/// row could not be written. This is a partial success — the operator must be
/// able to find the run that actually started, so we name it and flag the
/// missing audit record distinctly from the hard "Retry failed" message used
/// for real failures (400/404/409).
fn dag_retry_audit_warning_flash(new_run: &str) -> String {
    format!("Retry started — new run {new_run} — but the audit record could not be written")
}

/// Pure decision for the retry-commit redirect: which run to open next and what
/// flash text to show, given the shared `retry_dag_run_inner` outcome and the
/// source run id. A successful fork opens the new run with the success flash; an
/// [`DagRetryFailure::AuditFailed`] is a **partial success** (the fork committed)
/// so it opens the *new* run with a warning flash rather than hiding it behind a
/// failure message; every other failure opens the source run with the hard
/// "Retry failed" message. Returns unencoded flash text — the caller percent-
/// encodes it once.
fn dag_retry_commit_redirect(
    outcome: Result<DagRetryResponse, DagRetryFailure>,
    source_run: &str,
) -> (String, String) {
    match outcome {
        Ok(plan) => {
            let new_run = plan
                .new_run_exec_id
                .unwrap_or_else(|| source_run.to_string());
            let flash = dag_retry_success_flash(&new_run);
            (new_run, flash)
        }
        Err(DagRetryFailure::AuditFailed { new_exec_id, .. }) => {
            let flash = dag_retry_audit_warning_flash(&new_exec_id);
            (new_exec_id, flash)
        }
        Err(failure) => (
            source_run.to_string(),
            format!("Retry failed: {}", failure.human_message()),
        ),
    }
}

/// Default, editable retry reason pre-filled into the confirm-page textarea.
fn dag_retry_default_reason(from_node: &str) -> String {
    format!("retry from node {from_node} via Vantage")
}

// ── Issue #960: execution timeline / Gantt view ────────────────────────────
//
// A standalone, read-only page at `GET /workflows/{id}/timeline` that renders
// the shipped `autumn_harvest::derive_timeline` output as a server-computed
// inline SVG Gantt. Pause (#383) and ND-block (#603) bands come from the
// execution-row columns the handler already loads — never from the step data,
// which carries no such fields. Payload-free (no #608 decode, no audit).

/// Left gutter width (px) reserved for lane/step labels.
const TL_GUTTER: f64 = 230.0;
/// Wall-clock axis width (px) — the drawable time span.
const TL_AXIS_W: f64 = 900.0;
/// Per-step row height (px).
const TL_ROW_H: f64 = 26.0;
/// Span bar height (px) within a row.
const TL_SPAN_H: f64 = 14.0;
/// Header band height (px) holding the axis ticks.
const TL_HEADER_H: f64 = 44.0;
/// Minimum rendered span width (px) so sub-second slivers stay visible.
const TL_MIN_SPAN_W: f64 = 3.0;
/// Bottom padding (px).
const TL_PAD: f64 = 14.0;
/// Upper bound on rendered step rows; beyond this the view truncates with a
/// note (the rollup is still computed over the full set by `derive_timeline`).
const MAX_RENDER_STEPS: usize = 500;

/// Fixed lane order for the Gantt (one lane per `step_kind`). Activity-family
/// lanes lead so the common queue-wait/exec story reads first.
const TIMELINE_LANES: [StepKind; 6] = [
    StepKind::Activity,
    StepKind::LocalActivity,
    StepKind::ChildWorkflow,
    StepKind::Timer,
    StepKind::SignalWait,
    StepKind::SideEffect,
];

/// Fill colour keyed to a step's outcome (distinct per outcome; the label is
/// the accessible, colour-independent cue).
const fn step_outcome_fill(outcome: StepOutcome) -> &'static str {
    match outcome {
        StepOutcome::Completed => "#166534",
        StepOutcome::Failed => "#991b1b",
        StepOutcome::TimedOut => "#b45309",
        StepOutcome::Cancelled => "#6b7280",
        StepOutcome::Fired => "#0e7490",
        StepOutcome::Pending => "#1d4ed8",
    }
}

/// Human-readable outcome label (matches the #739 bounded enum).
const fn step_outcome_label(outcome: StepOutcome) -> &'static str {
    match outcome {
        StepOutcome::Completed => "Completed",
        StepOutcome::Failed => "Failed",
        StepOutcome::TimedOut => "Timed out",
        StepOutcome::Cancelled => "Cancelled",
        StepOutcome::Fired => "Fired",
        StepOutcome::Pending => "Pending",
    }
}

/// Lane label per step kind.
const fn step_kind_lane_label(kind: StepKind) -> &'static str {
    match kind {
        StepKind::Activity => "Activity",
        StepKind::LocalActivity => "Local activity",
        StepKind::Timer => "Timer",
        StepKind::ChildWorkflow => "Child workflow",
        StepKind::SignalWait => "Signal wait",
        StepKind::SideEffect => "Side effect",
    }
}

/// Map a millisecond offset onto an axis of `axis_width` px, guarding a
/// zero/negative span (an instantaneous or zero-duration run) so there is no
/// divide-by-zero / NaN, and clamping the result inside `[0, axis_width]`.
fn x_scale(offset_ms: i64, span_ms: i64, axis_width: f64) -> f64 {
    if span_ms <= 0 {
        return 0.0;
    }
    #[allow(clippy::cast_precision_loss)]
    let frac = offset_ms as f64 / span_ms as f64;
    (frac * axis_width).clamp(0.0, axis_width)
}

/// A within-span segment: the queue-wait vs execution split (only when the API
/// provides both), else one undivided whole span. Never fabricated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SegKind {
    Wait,
    Exec,
    Whole,
}

/// Split a step into its rendered segments. Two segments (`Wait`, `Exec`) only
/// when the timeline recorded both `wait_ms` and `exec_ms` (a started regular
/// activity); otherwise a single undivided `Whole` span — the split is never
/// fabricated (#739 contract).
fn span_segments(step: &TimelineStep) -> Vec<(SegKind, i64)> {
    match (step.wait_ms, step.exec_ms) {
        (Some(wait), Some(exec)) => vec![(SegKind::Wait, wait), (SegKind::Exec, exec)],
        _ => vec![(SegKind::Whole, step.total_ms)],
    }
}

/// CSS class for a segment kind.
const fn seg_class(seg: SegKind) -> &'static str {
    match seg {
        SegKind::Wait => "gantt-seg-wait",
        SegKind::Exec => "gantt-seg-exec",
        SegKind::Whole => "gantt-seg-whole",
    }
}

/// The rect class for a span segment (base + segment + open/slowest modifiers).
fn span_rect_class(seg: SegKind, open: bool, slowest: bool) -> String {
    let mut class = format!("gantt-span {}", seg_class(seg));
    if open {
        class.push_str(" gantt-span-open");
    }
    if slowest {
        class.push_str(" gantt-span-slowest");
    }
    class
}

/// Format a millisecond duration compactly (`ms` under 1 s, `Ns` under a
/// minute, `Nm Ns` beyond).
fn format_ms(ms: i64) -> String {
    let ms = ms.max(0);
    if ms < 1000 {
        format!("{ms}ms")
    } else if ms < 60_000 {
        #[allow(clippy::cast_precision_loss)]
        let secs = ms as f64 / 1000.0;
        format!("{secs:.1}s")
    } else {
        let secs = ms / 1000;
        format!("{}m {}s", secs / 60, secs % 60)
    }
}

/// Index into `timeline.steps` of the slowest step by `total_ms` (first on
/// ties). `None` when there are no steps. Matches the rollup's `slowest_step`.
fn slowest_step_index(timeline: &Timeline) -> Option<usize> {
    let mut best: Option<(usize, i64)> = None;
    for (idx, step) in timeline.steps.iter().enumerate() {
        if best.is_none_or(|(_, best_ms)| step.total_ms > best_ms) {
            best = Some((idx, step.total_ms));
        }
    }
    best.map(|(idx, _)| idx)
}

/// The wall-clock axis geometry for the Gantt: `[start, end]` mapped onto
/// `[gutter, gutter + width]`, with `height` the full SVG height (used by the
/// full-height pause band and ND marker).
#[derive(Debug, Clone, Copy)]
struct GanttAxis {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    gutter: f64,
    width: f64,
    height: f64,
}

impl GanttAxis {
    /// Build an axis, ensuring `end > start` so a zero-duration run still has a
    /// usable (1 ms) span rather than a degenerate axis.
    fn new(start: DateTime<Utc>, end: DateTime<Utc>, gutter: f64, width: f64, height: f64) -> Self {
        let end = if end > start {
            end
        } else {
            start + chrono::Duration::milliseconds(1)
        };
        Self {
            start,
            end,
            gutter,
            width,
            height,
        }
    }

    fn span_ms(&self) -> i64 {
        (self.end - self.start).num_milliseconds().max(1)
    }

    /// The x coordinate (px, including the gutter offset) for an instant.
    fn x_for(&self, t: DateTime<Utc>) -> f64 {
        let offset = (t - self.start).num_milliseconds();
        self.gutter + x_scale(offset, self.span_ms(), self.width)
    }
}

/// The full-height pause band (#383), sourced from the execution row's active
/// pause columns. `None` when the row is not currently paused. The row retains
/// only the *active* pause (resume nulls `paused_at`), so historical/resolved
/// pauses are not renderable — a documented limitation.
fn pause_band_markup(exec: &WorkflowExecution, axis: &GanttAxis) -> Option<Markup> {
    let paused_at = exec.paused_at?;
    let x1 = axis.x_for(paused_at);
    // An active pause extends to the current clock end (the axis end).
    let x2 = axis.gutter + axis.width;
    let width = (x2 - x1).max(1.0);
    let mut label = String::from("Paused");
    if let Some(reason) = exec.pause_reason.as_deref() {
        label.push_str(": ");
        label.push_str(reason);
    }
    if let Some(actor) = exec.pause_actor.as_deref() {
        label.push_str(" (by ");
        label.push_str(actor);
        label.push(')');
    }
    Some(html! {
        rect class="gantt-pause-band" x=(dag_svg_coord(x1)) y="0"
             width=(dag_svg_coord(width)) height=(dag_svg_coord(axis.height)) {}
        text class="gantt-pause-label" x=(dag_svg_coord(x1 + 4.0))
             y=(dag_svg_coord(axis.height - 6.0)) { (label) }
    })
}

/// The vertical ND-block marker (#603), sourced from the execution row's
/// `nd_blocked_at`/`nd_block_reason`. `None` when the row is not ND-blocked.
/// Surfaces the `nondeterminism-block` runbook *path* as inline SVG text — not a
/// clickable link, since the runbook is a repo path, not a served URL (a relative
/// `<a href>` here would 404 during ND-block triage). Matches the non-clickable
/// `code { "docs/runbooks/safe-deploy.md" }` convention elsewhere in this file.
fn nd_marker_markup(exec: &WorkflowExecution, axis: &GanttAxis) -> Option<Markup> {
    let nd_at = exec.nd_blocked_at?;
    let x = axis.x_for(nd_at);
    let reason = exec
        .nd_block_reason
        .as_deref()
        .unwrap_or("non-determinism block");
    Some(html! {
        line class="gantt-nd-marker" x1=(dag_svg_coord(x)) y1="0"
             x2=(dag_svg_coord(x)) y2=(dag_svg_coord(axis.height)) {}
        text class="gantt-nd-label" x=(dag_svg_coord(x + 4.0)) y="14" {
            "⚠ ND-block: " (reason) " — see docs/runbooks/nondeterminism-block.md"
        }
    })
}

/// The rollup header: total wall-clock, busy vs wait, and the slowest step.
fn render_timeline_rollup(rollup: &TimelineRollup) -> Markup {
    html! {
        div class="timeline-rollup" {
            div class="stat" {
                span class="label" { "Total wall-clock" }
                span class="value" { (format_ms(rollup.total_wall_clock_ms)) }
            }
            div class="stat" {
                span class="label" { "Busy (exec)" }
                span class="value" { (format_ms(rollup.busy_ms)) }
            }
            div class="stat" {
                span class="label" { "Wait (queue/timer/signal)" }
                span class="value" { (format_ms(rollup.wait_ms)) }
            }
            @if let Some(slow) = &rollup.slowest_step {
                div class="stat" {
                    span class="label" { "Slowest step" }
                    span class="value" {
                        (slow.name.as_deref().unwrap_or("(unnamed)"))
                        " · " (step_kind_lane_label(slow.step_kind))
                        " · " (format_ms(slow.total_ms))
                    }
                }
            }
        }
    }
}

/// A compact legend mapping outcome → colour + label.
fn timeline_outcome_legend() -> Markup {
    const OUTCOMES: [StepOutcome; 6] = [
        StepOutcome::Completed,
        StepOutcome::Failed,
        StepOutcome::TimedOut,
        StepOutcome::Cancelled,
        StepOutcome::Fired,
        StepOutcome::Pending,
    ];
    html! {
        div class="timeline-legend" {
            @for outcome in OUTCOMES {
                span {
                    span class="swatch" style=(format!("background:{}", step_outcome_fill(outcome))) {}
                    (step_outcome_label(outcome))
                }
            }
        }
    }
}

/// Render one step row: the gutter labels (lane group label once per group,
/// then the step name) plus the span segment rects grouped under an anchor
/// `<g>` (carrying `id="slowest"` for the slowest step), and the retry badge
/// for a genuine retry (`attempt > 1`). Segment x offsets are accumulated in
/// plain Rust (maud cannot mutate inside a `@for`).
fn render_timeline_row(
    step: &TimelineStep,
    kind: StepKind,
    is_head: bool,
    row: usize,
    axis: &GanttAxis,
    is_slowest: bool,
) -> Markup {
    let name = step.name.as_deref().unwrap_or("(unnamed)");
    #[allow(clippy::cast_precision_loss)]
    let y = TL_ROW_H.mul_add(row as f64, TL_HEADER_H);
    let open = step.ended_at.is_none();

    let x1 = axis.x_for(step.scheduled_at);
    let step_end = step.ended_at.unwrap_or(axis.end);
    let x2 = axis.x_for(step_end);
    let span_w = (x2 - x1).max(TL_MIN_SPAN_W);
    let span_y = y + (TL_ROW_H - TL_SPAN_H) / 2.0;

    // The span's pixel width comes from the axis geometry (`span_w`), never from
    // `total_ms`. A single `Whole` segment fills the whole geometric extent — so
    // an open/in-flight step (whose `total_ms` may be 0) still renders visibly.
    // Only a wait/exec split proportions `span_w` by the two segments' ms ratio.
    let segs = span_segments(step);
    let seg_total = segs.iter().map(|(_, ms)| *ms).sum::<i64>().max(1);
    let single = segs.len() == 1;
    let mut seg_markup: Vec<Markup> = Vec::with_capacity(segs.len());
    let mut seg_x = x1;
    for (seg, ms) in &segs {
        #[allow(clippy::cast_precision_loss)]
        let seg_w = if single {
            span_w
        } else {
            (span_w * (*ms as f64 / seg_total as f64)).max(0.5)
        };
        seg_markup.push(html! {
            rect x=(dag_svg_coord(seg_x)) y=(dag_svg_coord(span_y))
                 width=(dag_svg_coord(seg_w)) height=(dag_svg_coord(TL_SPAN_H))
                 rx="3" fill=(step_outcome_fill(step.outcome))
                 class=(span_rect_class(*seg, open, is_slowest)) {
                title {
                    (name) " — " (step_outcome_label(step.outcome))
                    " · " (format_ms(step.total_ms))
                    @if let (Some(w), Some(e)) = (step.wait_ms, step.exec_ms) {
                        " (wait " (format_ms(w)) " / exec " (format_ms(e)) ")"
                    }
                }
            }
        });
        seg_x += seg_w;
    }

    html! {
        // Gutter labels: lane group label once per group + step name.
        @if is_head {
            text class="gantt-lane-group" x="6" y=(dag_svg_coord(y + 10.0)) {
                (step_kind_lane_label(kind))
            }
            text class="gantt-lane-label" x="16" y=(dag_svg_coord(y + 22.0)) { (name) }
        } @else {
            text class="gantt-lane-label" x="16" y=(dag_svg_coord(y + 16.0)) { (name) }
        }
        g id=[is_slowest.then_some("slowest")] {
            @for m in &seg_markup { (m) }
            @if let Some(att) = step.attempt {
                @if att > 1 {
                    text class="gantt-badge" x=(dag_svg_coord(x1 + span_w + 4.0))
                         y=(dag_svg_coord(span_y + TL_SPAN_H - 2.0)) { "×" (att) }
                }
            }
        }
    }
}

/// Render the execution timeline as an inline, server-computed SVG Gantt.
///
/// Steps are lane-grouped by `step_kind` (in `TIMELINE_LANES` order), one row
/// per step on a shared wall-clock axis `[started_at, completed_at|now]`. A
/// started regular activity's `wait_ms`/`exec_ms` split renders as two
/// segments; every other step renders as one undivided span (never fabricated).
/// Open (in-flight) steps extend to the axis end and are dashed. The slowest
/// step carries `id="slowest"` (for a no-JS fragment scroll) and a highlight
/// outline. Pause and ND-block bands overlay the axis, sourced from the
/// execution row. maud auto-escapes every text/attribute interpolation.
#[allow(clippy::too_many_lines)]
fn render_timeline_gantt(
    timeline: &Timeline,
    exec: &WorkflowExecution,
    now: DateTime<Utc>,
) -> Markup {
    // Lane-group the steps (stable, in TIMELINE_LANES order), marking the first
    // of each present lane so the lane label renders once per group.
    let mut order: Vec<(StepKind, usize, bool)> = Vec::new();
    for lane in TIMELINE_LANES {
        let mut is_head = true;
        for (idx, step) in timeline.steps.iter().enumerate() {
            if step.step_kind == lane {
                order.push((lane, idx, is_head));
                is_head = false;
            }
        }
    }
    let total_steps = order.len();
    let truncated = total_steps > MAX_RENDER_STEPS;
    let slowest = slowest_step_index(timeline);

    // Render the first `MAX_RENDER_STEPS` lane-grouped rows. Lane grouping orders
    // rows by lane, not by duration, so the slowest step can fall outside the
    // prefix (#960). The rollup and the execution-detail Timeline link both
    // advertise `#slowest`, so the row carrying `id="slowest"` must always be
    // rendered — otherwise the anchor dangles. When truncating, append the
    // slowest step's lane-grouped entry as one extra row if it is not already in
    // the rendered set. (`slowest`, when present, always names a real entry in
    // `order`: every `StepKind` is a `TIMELINE_LANES` lane, so `order` holds one
    // entry per step.)
    let mut render_order: Vec<(StepKind, usize, bool)> =
        order.iter().take(MAX_RENDER_STEPS).copied().collect();
    let slowest_appended = if truncated
        && let Some(slow_idx) = slowest
        && !render_order.iter().any(|(_, idx, _)| *idx == slow_idx)
        && let Some(entry) = order.iter().find(|(_, idx, _)| *idx == slow_idx)
    {
        render_order.push(*entry);
        true
    } else {
        false
    };
    #[allow(clippy::cast_precision_loss)]
    let height = TL_ROW_H.mul_add(render_order.len() as f64, TL_HEADER_H) + TL_PAD;
    let svg_width = TL_GUTTER + TL_AXIS_W + TL_PAD;

    let axis_end = exec.completed_at.unwrap_or(now);
    let axis = GanttAxis::new(exec.started_at, axis_end, TL_GUTTER, TL_AXIS_W, height);
    let span_ms = axis.span_ms();

    // Axis tick labels at 0 / 50 / 100 % of the wall-clock span.
    let ticks: Vec<(f64, String)> = [0.0_f64, 0.5, 1.0]
        .iter()
        .map(|frac| {
            let x = axis.gutter + axis.width * frac;
            #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
            let off = (span_ms as f64 * frac) as i64;
            (x, format!("+{}", format_ms(off)))
        })
        .collect();

    html! {
        (render_timeline_rollup(&timeline.rollup))

        @if let Some(details) = exec.current_details.as_deref() {
            div class="card" {
                strong { "Now: " } (details)
                " · "
                // Link to the execution-detail route. The timeline page is served
                // at `{api_base}/workflows/{id}/timeline`, so its parent route is
                // `{api_base}/workflows/{id}` — spelled out from the api base
                // (mirroring the page's own `base_href = "../../"` nav) rather than
                // a bare `../{id}`, which relies on `..` stripping exactly the
                // `timeline` segment (Codex review, #960).
                a href={ "../../workflows/" (timeline.exec_id) } { "what is it blocked on?" }
            }
        }

        @if let Some(nd_at) = exec.nd_blocked_at {
            div class="banner Warning" {
                "This execution is non-determinism-blocked (since "
                (format_timestamp(Some(nd_at))) ", "
                (exec.nd_block_count) " occurrence(s)). "
                @if let Some(reason) = exec.nd_block_reason.as_deref() { (reason) " · " }
                // Runbook path as non-clickable code text (repo path, not a served
                // URL) — matches the `docs/runbooks/safe-deploy.md` convention.
                "See runbook " code { "docs/runbooks/nondeterminism-block.md" }
            }
        }

        (timeline_outcome_legend())

        @if truncated {
            div class="banner Warning" {
                "Showing first " (MAX_RENDER_STEPS) " of " (total_steps)
                " steps"
                @if slowest_appended { " (plus the slowest)" }
                ". The rollup above covers all steps."
            }
        }

        @if timeline.steps.is_empty() {
            div class="empty" { "No steps recorded for this execution yet." }
        } @else {
            div class="gantt-scroll" {
                svg xmlns="http://www.w3.org/2000/svg"
                    width=(dag_svg_coord(svg_width))
                    height=(dag_svg_coord(height))
                    role="img"
                    aria-label="Execution timeline Gantt" {
                    // Axis ticks (behind everything).
                    @for (x, label) in &ticks {
                        line class="gantt-axis-tick" x1=(dag_svg_coord(*x)) y1=(dag_svg_coord(TL_HEADER_H - 6.0))
                             x2=(dag_svg_coord(*x)) y2=(dag_svg_coord(height)) {}
                        text class="gantt-axis-label" x=(dag_svg_coord(*x + 2.0)) y=(dag_svg_coord(TL_HEADER_H - 10.0)) {
                            (label)
                        }
                    }

                    // Pause band (behind the spans).
                    @if let Some(band) = pause_band_markup(exec, &axis) { (band) }

                    // Step rows.
                    @for (row, (kind, idx, is_head)) in render_order.iter().enumerate() {
                        (render_timeline_row(
                            &timeline.steps[*idx],
                            *kind,
                            *is_head,
                            row,
                            &axis,
                            slowest == Some(*idx),
                        ))
                    }

                    // ND-block marker (on top).
                    @if let Some(marker) = nd_marker_markup(exec, &axis) { (marker) }
                }
            }
        }
    }
}

/// `GET /workflows/{id}/timeline` — the standalone execution timeline (Gantt).
///
/// Consumes the shipped `autumn_harvest::derive_timeline` in-process (exactly as
/// the #739 API handler does): loads the execution row + timestamped history,
/// derives the timeline, and renders the server-computed inline-SVG Gantt.
/// Pause/ND-block bands come from the execution row's columns. Read-only, and
/// payload-free by the API's design — no #608 read-path decode, no audit row.
/// An unknown execution (including a classic DAG run, which is not on the
/// execution path) surfaces `load_execution`'s `NotFound` as a 404.
async fn workflow_timeline_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id): Path<String>,
) -> Result<Markup, AutumnError> {
    let exec_id = parse_execution_id(&id)?;
    let mut conn = db_conn_for_execution(&api_state, exec_id).await?;
    let execution = load_execution(&mut conn, exec_id)
        .await
        .map_err(map_error)?;
    let rows = autumn_harvest::store::load_timestamped_history(&mut conn, exec_id)
        .await
        .map_err(map_error)?;
    let now = Utc::now();
    let timeline = derive_timeline(
        &rows,
        execution.started_at,
        now,
        execution.completed_at,
        exec_id.to_string(),
        execution.workflow_id.clone(),
        execution.workflow_name.clone(),
        execution.state.clone(),
    );
    let title = format!("Timeline · {} · Vantage", execution.workflow_name);
    let body = render_timeline_body(&timeline, &execution, now);
    Ok(layout(&title, &body, "../../"))
}

/// Build the timeline page body (back link + heading + Gantt). Extracted from
/// `workflow_timeline_ui` so the navigation link is covered by a pure render
/// test (Codex review, #960): both the "Back to execution" control here and the
/// current-details card inside `render_timeline_gantt` must resolve to the
/// execution-detail route `{api_base}/workflows/{id}` — not
/// `{api_base}/workflows/{id}/{id}`. The timeline page is served at
/// `{api_base}/workflows/{id}/timeline`, so the link spells the full path from
/// the api base (matching the page's `base_href = "../../"` nav) rather than a
/// bare `../{id}` that depends on `..` stripping exactly the `timeline` segment.
fn render_timeline_body(
    timeline: &Timeline,
    execution: &WorkflowExecution,
    now: DateTime<Utc>,
) -> Markup {
    html! {
        div.detail-row {
            a.back href={ "../../workflows/" (timeline.exec_id) } {
                (PreEscaped("&larr;")) " Back to execution"
            }
        }
        h2 {
            "Timeline — " (execution.workflow_name) " "
            (state_badge(&execution.state))
        }
        (render_timeline_gantt(timeline, execution, now))
    }
}

#[allow(clippy::too_many_arguments)]
fn render_dag_detail(
    dag_name: &str,
    dag: &RegisteredDag,
    runs: &[WorkflowExecution],
    selected_run: Option<uuid::Uuid>,
    selected_node: Option<usize>,
    refresh: Option<u64>,
    flash: Option<&str>,
    view: DagGraphView<'_>,
) -> Markup {
    let body = html! {
        @if let Some(message) = flash {
            div class="flash" role="status" tabindex="-1" autofocus { (message) }
        }
        h2 { "DAG " code { (dag_name) } " runs" }
        @if let Some(run_id) = selected_run {
            p { "Selected run: " code { (run_id) } }
        }
        @match view {
            DagGraphView::Classic => {
                div class="banner Warning" {
                    "No topology available for classic DAG runs — classic DAGs are being retired."
                }
            }
            DagGraphView::NoRun => {
                p class="empty" { "Select a run below to view its node graph." }
            }
            DagGraphView::Run { nodes, run_state } => {
                (render_dag_run_graph_section(dag, nodes, run_state, selected_run, selected_node, dag_name))
            }
        }
        table {
            thead {
                tr {
                    th { "Execution" }
                    th { "State" }
                    th { "Started" }
                    th { "Duration" }
                }
            }
            (render_dag_run_rows(runs, selected_run))
        }
    };
    layout_dag_detail(&format!("DAG {dag_name} · Vantage"), &body, "../", refresh)
}

/// Render the `<tbody>` of the DAG run list (issue #957).
///
/// Each run's **primary** affordance re-renders THIS page's node graph for that
/// run via a same-page `?run=` query (the same selector the graph honors, and the
/// same bare-relative form the SVG node links use). Without this, older runs' graphs
/// were only reachable by hand-constructing the query URL. The currently-shown run
/// is marked "(current)" instead of linked, so it is clear which run the graph
/// reflects. A secondary "detail" link still opens each run's workflow-detail page.
fn render_dag_run_rows(runs: &[WorkflowExecution], selected_run: Option<uuid::Uuid>) -> Markup {
    html! {
        tbody {
            @for run in runs {
                @let is_current = selected_run == Some(run.id);
                tr {
                    td {
                        @if is_current {
                            code { (run.id) }
                            span class="dag-run-current" { "(current)" }
                        } @else {
                            a href={ "?run=" (url_encode(&run.id.to_string())) } { code { (run.id) } }
                        }
                        a class="dag-run-detail" href={ "../workflows/" (run.id) } { "detail" }
                    }
                    td { span class={ "badge " (run.state.to_uppercase()) } { (run.state.as_str()) } }
                    td { (format_timestamp(Some(run.started_at))) }
                    td { (format_run_duration(run.started_at, run.completed_at)) }
                }
            }
        }
    }
}

/// Render the graph SVG + legend + optional large-DAG note + selected-node
/// panel for a unified DAG run.
fn render_dag_run_graph_section(
    dag: &RegisteredDag,
    nodes: &[DagRunNode],
    run_state: &str,
    selected_run: Option<uuid::Uuid>,
    selected_node: Option<usize>,
    dag_name: &str,
) -> Markup {
    let levels = dag.definition.execution_levels();
    let upstreams: Vec<Vec<usize>> = dag
        .definition
        .tasks()
        .iter()
        .map(|task| task.upstreams.clone())
        .collect();
    let run_id = selected_run.unwrap_or_else(uuid::Uuid::nil);
    let large = nodes.len() >= DAG_GRAPH_LARGE_THRESHOLD;
    html! {
        h3 { "Run graph" }
        (dag_status_legend())
        @if large {
            p class="banner Warning" {
                "Large DAG (" (nodes.len()) " nodes) — scroll to explore the graph below."
            }
        }
        (render_dag_run_graph_svg(nodes, levels, &upstreams, selected_node, run_id))
        @if let Some(idx) = selected_node {
            @if let Some(node) = nodes.get(idx) {
                (render_dag_node_panel(node, idx, run_state, dag_name, run_id))
            }
        }
    }
}

fn layout_dag_detail(title: &str, body: &Markup, base_href: &str, refresh: Option<u64>) -> Markup {
    html! {
        (PreEscaped("<!DOCTYPE html>"))
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width,initial-scale=1";
                title { (title) }
                style { (PreEscaped(STYLE)) }
                @if let Some(secs) = refresh { meta http-equiv="refresh" content=(secs); }
            }
            body {
                header {
                    h1 { a href={ (base_href) "workflows" } { "🔭 Vantage" } span.subtitle { "Harvest dashboard" } }
                    nav {
                        a href={ (base_href) "workflows" } { "Workflows" }
                        a.active href={ (base_href) "dags" } { "DAGs" }
                        a href={ (base_href) "workers" } { "Workers" }
                        a href={ (base_href) "schedules" } { "Schedules" }
                        a href={ (base_href) "dead-letters" } { "Dead Letters" }
                        a href={ (base_href) "build-routing" } { "Build Routing" }
                    }
                }
                main { (body) }
                footer { "Read-only dashboard — autumn-harvest" }
            }
        }
    }
}

fn format_run_duration(started_at: DateTime<Utc>, completed_at: Option<DateTime<Utc>>) -> String {
    let Some(end) = completed_at else {
        return "—".to_string();
    };
    let secs = (end - started_at).num_seconds().max(0);
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else {
        format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
    }
}

// ---------------------------------------------------------------------------
// Build Routing UI page (issue #362)
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_lines)]
async fn list_build_routing_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Query(params): Query<BuildRoutingListParams>,
) -> Result<Markup, AutumnError> {
    let pool = api_state.storage_pool().map_err(map_error)?;
    let is_multi_shard = pool.iter_shards().count() > 1;
    let stale_threshold = api_state.worker_stale_threshold();

    // Fan out to every shard to read policies, compat, and reachability.
    // Policy and compat mutations go to all shards; reading from a single shard
    // can hide partial-write divergence. We merge by queue_name / (build_id,
    // compatible_with) and detect queues whose active build_id differs across shards.
    let mut shard_errors: Vec<(ShardId, String)> = Vec::new();
    let mut policy_map: std::collections::HashMap<String, BuildPolicy> =
        std::collections::HashMap::new();
    let mut diverged_queues: std::collections::HashSet<String> = std::collections::HashSet::new();
    // Per-shard presence tracking for policy and compat: each entry is the set of
    // queue names / (build_id, compatible_with) pairs returned by one shard that
    // successfully responded. Used to detect absent rows on healthy shards.
    let mut per_shard_policy_seen: Vec<std::collections::HashSet<String>> = Vec::new();
    let mut compat_map: std::collections::HashMap<(String, String), BuildCompatEntry> =
        std::collections::HashMap::new();
    let mut per_shard_compat_seen: Vec<std::collections::HashSet<(String, String)>> = Vec::new();
    let mut per_shard_reach: Vec<Vec<BuildReachability>> = Vec::new();

    for (shard_id, shard_pool) in pool.iter_shards() {
        match acquire_conn(shard_pool).await {
            Ok(mut conn) => {
                match list_build_policies(&mut conn).await {
                    Ok(shard_policies) => {
                        let mut seen: std::collections::HashSet<String> =
                            std::collections::HashSet::new();
                        for policy in shard_policies {
                            seen.insert(policy.queue_name.clone());
                            match policy_map.get(&policy.queue_name) {
                                Some(existing) if existing.build_id != policy.build_id => {
                                    diverged_queues.insert(policy.queue_name.clone());
                                    if policy.updated_at > existing.updated_at {
                                        policy_map.insert(policy.queue_name.clone(), policy);
                                    }
                                }
                                Some(existing) if policy.updated_at > existing.updated_at => {
                                    policy_map.insert(policy.queue_name.clone(), policy);
                                }
                                None => {
                                    policy_map.insert(policy.queue_name.clone(), policy);
                                }
                                _ => {}
                            }
                        }
                        per_shard_policy_seen.push(seen);
                    }
                    Err(e) => shard_errors.push((shard_id, e.to_string())),
                }
                match list_build_compat(&mut conn).await {
                    Ok(entries) => {
                        let mut seen: std::collections::HashSet<(String, String)> =
                            std::collections::HashSet::new();
                        for entry in entries {
                            let key = (entry.build_id.clone(), entry.compatible_with.clone());
                            seen.insert(key.clone());
                            compat_map
                                .entry(key)
                                .and_modify(|e| {
                                    if entry.declared_at > e.declared_at {
                                        *e = entry.clone();
                                    }
                                })
                                .or_insert(entry);
                        }
                        per_shard_compat_seen.push(seen);
                    }
                    Err(e) => shard_errors.push((shard_id, e.to_string())),
                }
                match all_build_reachability(&mut conn, stale_threshold).await {
                    Ok(r) => per_shard_reach.push(r),
                    Err(e) => shard_errors.push((shard_id, e.to_string())),
                }
            }
            Err(e) => shard_errors.push((shard_id, e.to_string())),
        }
    }

    // Detect absent-row policy divergence (queue on some shards, missing on others).
    if per_shard_policy_seen.len() > 1 {
        for queue_name in policy_map.keys() {
            if per_shard_policy_seen
                .iter()
                .any(|seen| !seen.contains(queue_name.as_str()))
            {
                diverged_queues.insert(queue_name.clone());
            }
        }
    }

    // Detect compat pairs present on some shards but absent on others.
    let mut diverged_compat_pairs: Vec<String> = if per_shard_compat_seen.len() > 1 {
        let mut pairs: Vec<String> = compat_map
            .keys()
            .filter(|key| {
                per_shard_compat_seen
                    .iter()
                    .any(|seen| !seen.contains(*key))
            })
            .map(|(b, c)| format!("{b} \u{2192} {c}"))
            .collect();
        pairs.sort();
        pairs
    } else {
        vec![]
    };
    diverged_compat_pairs.dedup();

    let mut policies: Vec<BuildPolicy> = policy_map.into_values().collect();
    policies.sort_by(|a, b| a.queue_name.cmp(&b.queue_name));
    let mut all_compat: Vec<BuildCompatEntry> = compat_map.into_values().collect();
    all_compat.sort_by(|a, b| {
        a.build_id
            .cmp(&b.build_id)
            .then(a.compatible_with.cmp(&b.compatible_with))
    });
    let mut diverged_list: Vec<String> = diverged_queues.into_iter().collect();
    diverged_list.sort();
    let reachability = merge_reachability(per_shard_reach);

    let shard_error_refs: Vec<(ShardId, &str)> =
        shard_errors.iter().map(|(s, e)| (*s, e.as_str())).collect();

    // Apply optional build_id filter to narrow tables for drill-down from workers/executions.
    let build_id_filter = params.build_id.as_deref().filter(|s| !s.is_empty());
    let filtered_policies: Vec<BuildPolicy> = if let Some(bid) = build_id_filter {
        policies.into_iter().filter(|p| p.build_id == bid).collect()
    } else {
        policies
    };
    let filtered_compat: Vec<BuildCompatEntry> = if let Some(bid) = build_id_filter {
        all_compat
            .into_iter()
            .filter(|e| e.build_id == bid || e.compatible_with == bid)
            .collect()
    } else {
        all_compat
    };
    let filtered_reach: Vec<BuildReachability> = if let Some(bid) = build_id_filter {
        reachability
            .into_iter()
            .filter(|r| r.build_id == bid)
            .collect()
    } else {
        reachability
    };

    Ok(render_build_routing_page(
        &filtered_policies,
        &filtered_compat,
        &filtered_reach,
        &shard_error_refs,
        &diverged_list,
        &diverged_compat_pairs,
        is_multi_shard,
        params.flash.as_deref(),
        build_id_filter,
    ))
}

async fn build_routing_set_policy_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Form(form): Form<BuildRoutingSetPolicyForm>,
) -> Result<axum::response::Response, AutumnError> {
    let queue_name = form.queue_name.trim().to_string();
    let build_id = form.build_id.trim().to_string();
    if queue_name.is_empty() || build_id.is_empty() {
        let flash = url_encode("queue_name and build_id must not be empty");
        return Ok(
            axum::response::Redirect::to(&format!("../build-routing?flash={flash}"))
                .into_response(),
        );
    }
    let pool = api_state.storage_pool().map_err(map_error)?;
    let deployment_name = form.deployment_name.as_deref().filter(|s| !s.is_empty());
    // Fan out to all shards so every shard's get_build_policy() sees the new policy
    // when evaluating assigned_build_id at workflow start time.
    let mut last_policy = None;
    let mut shard_errors: Vec<String> = Vec::new();
    for (shard_id, shard_pool) in pool.iter_shards() {
        match acquire_conn(shard_pool).await {
            Ok(mut conn) => {
                match set_build_policy(&mut conn, &queue_name, &build_id, deployment_name)
                    .await
                    .map_err(map_error)
                {
                    Ok(p) => last_policy = Some(p),
                    Err(e) => shard_errors.push(format!("shard {}: {e}", shard_id.as_i32())),
                }
            }
            Err(e) => shard_errors.push(format!("shard {}: {e}", shard_id.as_i32())),
        }
    }
    let audit_status = if shard_errors.is_empty() {
        STATUS_SUCCEEDED
    } else {
        STATUS_FAILED
    };
    if let Ok(mut conn) = acquire_conn(pool.default_pool()).await {
        let error_summary = shard_errors.join("; ");
        let _ = insert_audit(
            &mut conn,
            &NewAuditRecord {
                actor: "ui",
                operation: OP_BUILD_POLICY_SET,
                target_type: TARGET_BUILD_ROUTING,
                target_id: Some(queue_name.as_str()),
                route_or_command: "POST /ui/build-routing/set-policy",
                request_id: None,
                idempotency_key: None,
                status: audit_status,
                error_summary: if error_summary.is_empty() {
                    None
                } else {
                    Some(error_summary.as_str())
                },
                shard_id: None,
                source: SOURCE_UI,
            },
        )
        .await;
    }
    let flash = if shard_errors.is_empty() {
        match last_policy {
            Some(p) => url_encode(&format!(
                "Build policy for queue '{}' set to '{}'",
                p.queue_name, p.build_id
            )),
            None => url_encode("No shards configured"),
        }
    } else {
        url_encode(&format!(
            "Partial failure setting build policy: {}",
            shard_errors.join("; ")
        ))
    };
    Ok(axum::response::Redirect::to(&format!("../build-routing?flash={flash}")).into_response())
}

async fn build_routing_declare_compat_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Form(form): Form<BuildRoutingCompatForm>,
) -> Result<axum::response::Response, AutumnError> {
    let build_id = form.build_id.trim().to_string();
    let compatible_with = form.compatible_with.trim().to_string();
    if build_id.is_empty() || compatible_with.is_empty() {
        let flash = url_encode("build_id and compatible_with must not be empty");
        return Ok(
            axum::response::Redirect::to(&format!("../build-routing?flash={flash}"))
                .into_response(),
        );
    }
    let pool = api_state.storage_pool().map_err(map_error)?;
    // Fan out to all shards so load_compat_set() on each shard picks up the declaration.
    let mut last_entry = None;
    let mut shard_errors: Vec<String> = Vec::new();
    for (shard_id, shard_pool) in pool.iter_shards() {
        match acquire_conn(shard_pool).await {
            Ok(mut conn) => {
                match declare_compat(&mut conn, &build_id, &compatible_with)
                    .await
                    .map_err(map_error)
                {
                    Ok(e) => last_entry = Some(e),
                    Err(e) => shard_errors.push(format!("shard {}: {e}", shard_id.as_i32())),
                }
            }
            Err(e) => shard_errors.push(format!("shard {}: {e}", shard_id.as_i32())),
        }
    }
    let audit_status = if shard_errors.is_empty() {
        STATUS_SUCCEEDED
    } else {
        STATUS_FAILED
    };
    if let Ok(mut conn) = acquire_conn(pool.default_pool()).await {
        let error_summary = shard_errors.join("; ");
        let target = format!("{build_id}→{compatible_with}");
        let _ = insert_audit(
            &mut conn,
            &NewAuditRecord {
                actor: "ui",
                operation: OP_BUILD_COMPAT_DECLARE,
                target_type: TARGET_BUILD_ROUTING,
                target_id: Some(target.as_str()),
                route_or_command: "POST /ui/build-routing/declare-compat",
                request_id: None,
                idempotency_key: None,
                status: audit_status,
                error_summary: if error_summary.is_empty() {
                    None
                } else {
                    Some(error_summary.as_str())
                },
                shard_id: None,
                source: SOURCE_UI,
            },
        )
        .await;
    }
    let flash = if shard_errors.is_empty() {
        match last_entry {
            Some(e) => url_encode(&format!(
                "Declared: '{}' compatible with '{}'",
                e.build_id, e.compatible_with
            )),
            None => url_encode("No shards configured"),
        }
    } else {
        url_encode(&format!(
            "Partial failure declaring compat: {}",
            shard_errors.join("; ")
        ))
    };
    Ok(axum::response::Redirect::to(&format!("../build-routing?flash={flash}")).into_response())
}

async fn build_routing_revoke_compat_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Form(form): Form<BuildRoutingCompatForm>,
) -> Result<axum::response::Response, AutumnError> {
    let pool = api_state.storage_pool().map_err(map_error)?;
    // Fan out revoke to all shards; collect errors rather than aborting.
    let mut any_revoked = false;
    let mut shard_errors: Vec<String> = Vec::new();
    for (shard_id, shard_pool) in pool.iter_shards() {
        match acquire_conn(shard_pool).await {
            Ok(mut conn) => {
                match revoke_compat(&mut conn, form.build_id.trim(), form.compatible_with.trim())
                    .await
                    .map_err(map_error)
                {
                    Ok(r) => any_revoked |= r,
                    Err(e) => shard_errors.push(format!("shard {}: {e}", shard_id.as_i32())),
                }
            }
            Err(e) => shard_errors.push(format!("shard {}: {e}", shard_id.as_i32())),
        }
    }
    let audit_status = if shard_errors.is_empty() {
        STATUS_SUCCEEDED
    } else {
        STATUS_FAILED
    };
    if let Ok(mut conn) = acquire_conn(pool.default_pool()).await {
        let error_summary = shard_errors.join("; ");
        let target = format!("{}→{}", form.build_id.trim(), form.compatible_with.trim());
        let _ = insert_audit(
            &mut conn,
            &NewAuditRecord {
                actor: "ui",
                operation: OP_BUILD_COMPAT_REVOKE,
                target_type: TARGET_BUILD_ROUTING,
                target_id: Some(target.as_str()),
                route_or_command: "POST /ui/build-routing/revoke-compat",
                request_id: None,
                idempotency_key: None,
                status: audit_status,
                error_summary: if error_summary.is_empty() {
                    None
                } else {
                    Some(error_summary.as_str())
                },
                shard_id: None,
                source: SOURCE_UI,
            },
        )
        .await;
    }
    let flash = if !shard_errors.is_empty() {
        url_encode(&format!(
            "Partial failure revoking compat: {}",
            shard_errors.join("; ")
        ))
    } else if any_revoked {
        url_encode(&format!(
            "Revoked compatibility: '{}' → '{}'",
            form.build_id, form.compatible_with
        ))
    } else {
        url_encode(&format!(
            "No compatibility declaration found for '{}' → '{}'",
            form.build_id, form.compatible_with
        ))
    };
    Ok(axum::response::Redirect::to(&format!("../build-routing?flash={flash}")).into_response())
}

async fn build_routing_retire_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Form(form): Form<BuildRoutingRetireForm>,
) -> Result<axum::response::Response, AutumnError> {
    if form.build_id.trim().is_empty() {
        let flash = url_encode("build_id must not be empty");
        return Ok(
            axum::response::Redirect::to(&format!("../build-routing?flash={flash}"))
                .into_response(),
        );
    }
    let pool = api_state.storage_pool().map_err(map_error)?;
    let stale_threshold = api_state.worker_stale_threshold();

    // Check reachability across all shards before allowing retire. Any shard
    // error propagates immediately — silently skipping a shard could allow
    // retire when that shard still has active executions.
    let mut per_shard_reach: Vec<Vec<BuildReachability>> = Vec::new();
    for (_, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        let r = all_build_reachability(&mut conn, stale_threshold)
            .await
            .map_err(map_error)?;
        per_shard_reach.push(r);
    }
    let merged = merge_reachability(per_shard_reach);
    let build_reach = merged.iter().find(|r| r.build_id == form.build_id.trim());

    let flash = match build_reach {
        Some(r) if !r.safe_to_retire => url_encode(&format!(
            "Cannot retire build '{}': {} open executions, {} pending tasks remain",
            form.build_id, r.open_executions, r.pending_tasks
        )),
        _ => {
            // Build is safe to retire (or not found, meaning nothing is running on it).
            // The retire action itself is a no-op at the DB level — the operator
            // removes their old workers out-of-band. We surface a confirmation message.
            url_encode(&format!(
                "Build '{}' is safe to retire — no open executions or pending tasks remain. \
                 You may now stop all workers running this build.",
                form.build_id.trim()
            ))
        }
    };
    Ok(axum::response::Redirect::to(&format!("../build-routing?flash={flash}")).into_response())
}

#[allow(clippy::too_many_arguments)]
fn render_build_policies_card(policies: &[BuildPolicy]) -> Markup {
    html! {
        div.card {
            h3 { "Build Policies" }
            @if policies.is_empty() {
                p.empty { "No build policies registered." }
            } @else {
                table {
                    thead { tr { th { "Queue" } th { "Active Build ID" } th { "Deployment" } th { "Last Updated" } } }
                    tbody {
                        @for policy in policies {
                            tr {
                                td { code { (policy.queue_name.clone()) } }
                                td { code { (policy.build_id.clone()) } }
                                td {
                                    @if let Some(ref dep) = policy.deployment_name {
                                        code { (dep) }
                                    } @else {
                                        span style="color:#94a3b8" { "—" }
                                    }
                                }
                                td { (format_timestamp(Some(policy.updated_at))) }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn render_build_reachability_card(reachability: &[BuildReachability]) -> Markup {
    html! {
        div.card {
            h3 { "Build Reachability" }
            @if reachability.is_empty() {
                p.empty { "No build-tagged executions or workers found." }
            } @else {
                table {
                    thead { tr { th { "Build ID" } th { "Open Executions" } th { "Pending Tasks" } th { "Active Workers" } th { "Stale Workers" } th { "Status" } th { "Actions" } } }
                    tbody {
                        @for r in reachability {
                            @let status_color = if r.safe_to_retire { "#166534" } else { "#991b1b" };
                            @let status_bg = if r.safe_to_retire { "#dcfce7" } else { "#fee2e2" };
                            @let status_label = if r.safe_to_retire { "✓ Safe to retire" } else { "⚠ In use" };
                            tr {
                                td { code { (r.build_id.clone()) } }
                                td { (r.open_executions) }
                                td { (r.pending_tasks) }
                                td { (r.active_workers) }
                                td { (r.stale_workers) }
                                td {
                                    span style={ "background:" (status_bg) ";color:" (status_color) ";padding:2px 8px;border-radius:999px;font-size:11px;font-weight:600" } {
                                        (status_label)
                                    }
                                }
                                td {
                                    @if r.safe_to_retire {
                                        form method="post" action="build-routing/retire"
                                              onsubmit={ "return confirm('Confirm retirement of build " (js_escape(&r.build_id)) "? All workers running this build should be stopped after confirmation.')" }
                                              style="margin:0" {
                                            input type="hidden" name="build_id" value=(r.build_id.clone());
                                            button.danger type="submit"
                                                style="background:#166534;color:#dcfce7;border:0;border-radius:6px;padding:4px 10px;font-size:11px;cursor:pointer" {
                                                "Retire"
                                            }
                                        }
                                    } @else {
                                        span style="color:#94a3b8;font-size:12px" { "Not yet safe" }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn render_compat_card(all_compat: &[BuildCompatEntry]) -> Markup {
    html! {
        div.card {
            h3 { "Compatibility Declarations" }
            p style="color:#94a3b8;font-size:12px;margin-bottom:12px" {
                "Workers running build " strong { "A" } " can claim tasks assigned to build " strong { "B" }
                " when a declaration " code { "A → B" } " exists here."
            }
            @if all_compat.is_empty() {
                p.empty { "No compatibility declarations. Workers only claim tasks assigned to their own build." }
            } @else {
                table {
                    thead { tr { th { "Worker Build (A)" } th { "Compatible With (B)" } th { "Declared" } th { "Actions" } } }
                    tbody {
                        @for entry in all_compat {
                            tr {
                                td { code { (entry.build_id.clone()) } }
                                td { code { (entry.compatible_with.clone()) } }
                                td { (format_timestamp(Some(entry.declared_at))) }
                                td {
                                    form method="post" action="build-routing/revoke-compat"
                                          onsubmit={ "return confirm('Revoke compatibility: " (js_escape(&entry.build_id)) " → " (js_escape(&entry.compatible_with)) "?')" }
                                          style="margin:0" {
                                        input type="hidden" name="build_id" value=(entry.build_id.clone());
                                        input type="hidden" name="compatible_with" value=(entry.compatible_with.clone());
                                        button type="submit"
                                            style="background:#450a0a;color:#fca5a5;border:1px solid #991b1b;border-radius:6px;padding:3px 8px;font-size:11px;cursor:pointer" {
                                            "Revoke"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn render_build_routing_action_forms() -> Markup {
    let input_style = "display:block;width:100%;margin-top:4px;background:#0f172a;color:#e2e8f0;border:1px solid #334155;border-radius:4px;padding:6px 8px;font-size:12px";
    let btn_style = "background:#2563eb;color:#fff;border:0;border-radius:6px;padding:8px 14px;font-size:13px;cursor:pointer;align-self:flex-start";
    let label_style = "font-size:12px;color:#94a3b8";
    html! {
        div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-top:16px" {
            div.card {
                h3 style="margin-top:0" { "Set Build Policy" }
                p style="color:#94a3b8;font-size:12px;margin-bottom:12px" {
                    "Sets which build ID is assigned to new workflow starts on a queue. "
                    "Does not affect in-flight executions."
                }
                form method="post" action="build-routing/set-policy"
                      style="display:flex;flex-direction:column;gap:10px" {
                    label style=(label_style) { "Queue name"
                        input type="text" name="queue_name" required placeholder="e.g. default" style=(input_style);
                    }
                    label style=(label_style) { "Build ID"
                        input type="text" name="build_id" required placeholder="e.g. sha-abc123" style=(input_style);
                    }
                    label style=(label_style) { "Deployment name (optional)"
                        input type="text" name="deployment_name" placeholder="e.g. prod-v2" style=(input_style);
                    }
                    button type="submit" style=(btn_style)
                        onclick="return confirm('Set build policy? New executions on this queue will use the specified build ID.')" {
                        "Set Policy"
                    }
                }
            }
            div.card {
                h3 style="margin-top:0" { "Declare Compatibility" }
                p style="color:#94a3b8;font-size:12px;margin-bottom:12px" {
                    "Declares that workers running build " strong { "A" }
                    " can safely replay histories assigned to build " strong { "B" }
                    ". Only declare after replay tests confirm safety."
                }
                form method="post" action="build-routing/declare-compat"
                      style="display:flex;flex-direction:column;gap:10px" {
                    label style=(label_style) { "Worker build (A)"
                        input type="text" name="build_id" required placeholder="e.g. sha-new" style=(input_style);
                    }
                    label style=(label_style) { "Compatible with (B)"
                        input type="text" name="compatible_with" required placeholder="e.g. sha-old" style=(input_style);
                    }
                    button type="submit" style=(btn_style)
                        onclick="return confirm('Declare compatibility? Ensure replay tests have confirmed the new build can handle histories from the old build.')" {
                        "Declare"
                    }
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn render_build_routing_page(
    policies: &[BuildPolicy],
    all_compat: &[BuildCompatEntry],
    reachability: &[BuildReachability],
    shard_errors: &[(ShardId, &str)],
    diverged_queues: &[String],
    diverged_compat_pairs: &[String],
    is_multi_shard: bool,
    flash: Option<&str>,
    build_id_filter: Option<&str>,
) -> Markup {
    let is_empty = policies.is_empty() && reachability.is_empty() && all_compat.is_empty();

    let body = html! {
        h2 { "Build Routing" }

        @if let Some(bid) = build_id_filter {
            div style="background:#1e293b;border:1px solid #334155;border-radius:8px;padding:10px 14px;margin-bottom:12px;font-size:12px;color:#94a3b8" {
                "Filtered to build " code style="color:#e2e8f0" { (bid) }
                " · "
                a href="build-routing" style="color:#60a5fa" { "Show all builds" }
            }
        }

        @if let Some(msg) = flash {
            div.flash role="status" tabindex="-1" autofocus { (msg) }
        }

        @if !diverged_queues.is_empty() {
            div style="background:#431407;border:1px solid #ea580c;border-radius:8px;padding:10px 14px;margin-bottom:12px;font-size:13px;color:#fed7aa" {
                strong { "Policy divergence detected" }
                " — the following queues have different active build IDs across shards, "
                "indicating a partial write failure. Re-apply the policy to resync: "
                @for (i, q) in diverged_queues.iter().enumerate() {
                    @if i > 0 { ", " }
                    code style="color:#fdba74" { (q) }
                }
            }
        }

        @if !diverged_compat_pairs.is_empty() {
            div style="background:#431407;border:1px solid #ea580c;border-radius:8px;padding:10px 14px;margin-bottom:12px;font-size:13px;color:#fed7aa" {
                strong { "Compat divergence detected" }
                " — the following pairs are declared on some shards but missing on others. "
                "Re-declare each pair to resync: "
                @for (i, pair) in diverged_compat_pairs.iter().enumerate() {
                    @if i > 0 { ", " }
                    code style="color:#fdba74" { (pair) }
                }
            }
        }

        @for (shard_id, error) in shard_errors {
            div.shard-error {
                @if is_multi_shard {
                    strong { "Shard " (shard_id.as_i32()) " error: " }
                } @else {
                    strong { "Shard error: " }
                }
                (error)
            }
        }

        @if is_empty && shard_errors.is_empty() {
            div.card {
                @if build_id_filter.is_some() {
                    h3 { "No results" }
                    p style="color:#94a3b8;font-size:13px;line-height:1.6" {
                        "No policies, reachability entries, or compat declarations match the active filter. "
                        "The build may not exist or may already be retired."
                    }
                } @else {
                    h3 { "No build routing configured" }
                    p style="color:#94a3b8;font-size:13px;line-height:1.6" {
                        "No build policies have been set and no executions carry a build tag. "
                        "Build routing is inactive — all workers can claim any task."
                    }
                    p style="color:#94a3b8;font-size:13px" {
                        "To start a rolling deploy, follow the operator playbook in "
                        code { "docs/runbooks/safe-deploy.md" }
                        "."
                    }
                }
            }
        } @else {
            (render_build_policies_card(policies))
            (render_build_reachability_card(reachability))
            (render_compat_card(all_compat))
        }

        (render_build_routing_action_forms())
    };

    layout_build_routing("Build Routing · Vantage", &body, None)
}

fn layout_build_routing(title: &str, body: &Markup, refresh: Option<u64>) -> Markup {
    html! {
        (PreEscaped("<!DOCTYPE html>"))
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width,initial-scale=1";
                @if let Some(secs) = refresh {
                    meta http-equiv="refresh" content=(secs);
                }
                title { (title) }
                style { (PreEscaped(STYLE)) }
            }
            body {
                header {
                    h1 {
                        a href="workflows" { "🔭 Vantage" }
                        span.subtitle { "Harvest dashboard" }
                    }
                    nav {
                        a href="workflows" { "Workflows" }
                        a href="workers" { "Workers" }
                        a href="schedules" { "Schedules" }
                        a href="dead-letters" { "Dead Letters" }
                        a.active href="build-routing" { "Build Routing" }
                    }
                }
                main { (body) }
                footer { "Operational dashboard — autumn-harvest" }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Schedules UI page
// ---------------------------------------------------------------------------

const DEFAULT_SCHEDULE_PAGE_SIZE: i64 = 50;

type ShardScheduleResult = (ShardId, Result<Vec<HarvestSchedule>, String>);

#[derive(Debug, Deserialize)]
pub(crate) struct ScheduleListParams {
    #[serde(default)]
    page: Option<i64>,
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    target: Option<String>,
    /// "Workflow", "Dag", or empty/absent for All.
    #[serde(default)]
    kind: Option<String>,
    /// "Paused", "Active", or empty/absent for All.
    #[serde(default)]
    paused: Option<String>,
    /// "Unhealthy", "Healthy", or empty/absent for All (issue #951).
    #[serde(default)]
    health: Option<String>,
    #[serde(default)]
    shard_id: Option<String>,
    #[serde(default)]
    refresh: Option<u64>,
    #[serde(default)]
    flash: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct ScheduleBulkParams {
    #[serde(default)]
    target: Option<String>,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    paused: Option<String>,
    /// Health filter carried through a bulk action so "pause all matching"
    /// means the same set the operator is looking at (issue #951).
    #[serde(default)]
    health: Option<String>,
    #[serde(default)]
    shard_id: Option<String>,
    /// The filtered list-page path to redirect back to after the action,
    /// including an unresolved invalid value's raw text. Without it, the
    /// redirect always lands on a bare, unfiltered `schedules?flash=…`
    /// (Codex review, #1437 P2). See `schedule_bulk_redirect_to`.
    #[serde(default)]
    return_to: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum ScheduleKindFilter {
    #[default]
    All,
    Workflow,
    Dag,
}

impl ScheduleKindFilter {
    fn parse(raw: &str) -> Result<Self, AutumnError> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" => Ok(Self::All),
            "workflow" => Ok(Self::Workflow),
            "dag" => Ok(Self::Dag),
            other => Err(AutumnError::bad_request_msg(format!(
                "unknown kind '{other}'; expected Workflow, Dag, or empty"
            ))),
        }
    }

    const fn as_label(self) -> &'static str {
        match self {
            Self::All => "",
            Self::Workflow => "Workflow",
            Self::Dag => "Dag",
        }
    }
}

/// Which of the self-inflicted unhealthy states a schedule is currently in
/// (issue #951 AC3).
///
/// These are the states that make an operator's 3 a.m. question — "did the
/// nightly billing schedule fire, and if not, why not?" — answerable at a
/// glance: a schedule that is *not* firing is almost always paused,
/// auto-paused after repeated failures (#360), exhausted against its `end_at`
/// or run budget (#478), or silently dropping missed slots under its catchup
/// policy (#484). Anything else reads as one calm row.
// The four flags are deliberately independent booleans rather than a state
// enum: a schedule can be paused *and* exhausted *and* dropping catchup slots
// at once, and an operator needs to see all of them.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct ScheduleHealth {
    /// The schedule is paused (operator pause, or the auto-pause below).
    paused: bool,
    /// The pause was applied automatically after `consecutive_failure_limit`
    /// failures (#360) — a louder signal than a hand pause.
    auto_paused: bool,
    /// `end_at` or the `max_runs` budget has been reached (#478); the schedule
    /// will never fire again.
    exhausted: bool,
    /// The most recent recovery tick dropped missed slots (#484) — runs the
    /// operator expected that never happened.
    catchup_dropped: bool,
}

impl ScheduleHealth {
    const fn is_healthy(self) -> bool {
        !(self.paused || self.auto_paused || self.exhausted || self.catchup_dropped)
    }

    /// Sort rank: `0` for unhealthy, `1` for healthy, so unhealthy schedules
    /// float to the top of the list (AC3) while healthy rows keep their
    /// existing `next_run_at`-ascending order among themselves.
    const fn rank(self) -> u8 {
        if self.is_healthy() { 1 } else { 0 }
    }
}

/// Whether a schedule is currently held back from firing, and so is offered
/// **Resume** rather than **Pause**.
///
/// The scheduler's auto-pause (#360) sets `auto_paused_at` and deliberately
/// does **not** set `is_paused` — so a row can be non-firing with
/// `is_paused = false`. Keying the row actions on `is_paused` alone would show
/// an "Auto-paused" badge next to a Pause button and leave the operator with no
/// way to restore firing from this page at all.
/// `POST /admin/schedules/{id}/resume` treats
/// `is_paused = true OR auto_paused_at IS NOT NULL` as resumable; this mirrors it.
const fn schedule_is_resumable(row: &HarvestSchedule) -> bool {
    row.is_paused || row.auto_paused_at.is_some()
}

/// Whether a schedule has run out of budget or passed its cutoff, whether or
/// not a scheduler tick has got round to stamping `exhausted_at`.
///
/// `exhausted_at` is written *asynchronously* by the tick that observes the
/// bound. A tick that dies first leaves a row that is already terminal —
/// `runs_started >= max_runs`, or `now >= end_at` — with the column still NULL.
/// The engine never trusts the column alone: `schedule_backfill_inner` and
/// `trigger_schedule_now` both reject on `exhausted_at.is_some() ||
/// live_end_at_exceeded || live_budget_exhausted`, and the scheduler's own
/// `schedule_overdue` derives the same thing from the raw fields for exactly
/// this reason. Reading the column alone here would render such a row as a calm
/// `Active` schedule, exclude it from `health=Unhealthy`, and sort it *below*
/// the unhealthy rows — while this page's own preview for it correctly reports
/// no upcoming fire times.
///
/// `max_runs = 0` is **unlimited**, not "spent": the `max > 0` guard is the
/// engine's convention at every bound check (and is pinned by
/// `backfill_max_runs_zero_is_treated_as_unlimited`).
fn schedule_is_bounded_out(row: &HarvestSchedule, now: DateTime<Utc>) -> bool {
    if row.exhausted_at.is_some() {
        return true;
    }
    if row
        .max_runs
        .is_some_and(|max| max > 0 && row.runs_started >= max)
    {
        return true;
    }
    // The `end_at` bound is about the **pending slot**, not the wall clock —
    // `schedule_overdue` tests `next_run_at >= end_at`, and the tick refuses a
    // fire whose `effective_fire_time >= end_at`. Comparing `now` instead is
    // wrong in both directions: a schedule whose next slot is already past the
    // cutoff will never fire again while the clock is still short of it (we
    // would call it healthy), and after downtime an overdue slot from *before*
    // the cutoff is still legal and will be processed once the clock has passed
    // it (we would call it exhausted). Fall back to the wall clock only when
    // there is no pending slot to judge.
    row.end_at.is_some_and(|end_at| {
        row.next_run_at
            .map_or(now >= end_at, |next_run_at| next_run_at >= end_at)
    })
}

/// Derive a row's health flags. Pure: every badge, sort and summary decision on
/// the page goes through this one function, so they can never disagree.
///
/// `now` is a parameter rather than read inside so the bounded-out branch is
/// testable without sleeping.
fn schedule_health_at(row: &HarvestSchedule, now: DateTime<Utc>) -> ScheduleHealth {
    ScheduleHealth {
        paused: row.is_paused,
        auto_paused: row.auto_paused_at.is_some(),
        exhausted: schedule_is_bounded_out(row, now),
        catchup_dropped: row.last_catchup_dropped > 0,
    }
}

/// [`schedule_health_at`] anchored to the current instant.
fn schedule_health(row: &HarvestSchedule) -> ScheduleHealth {
    schedule_health_at(row, Utc::now())
}

/// Filter the list by health (issue #951 AC3): "show me only what is wrong".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum ScheduleHealthFilter {
    #[default]
    All,
    Unhealthy,
    Healthy,
}

impl ScheduleHealthFilter {
    fn parse(raw: &str) -> Result<Self, AutumnError> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" => Ok(Self::All),
            "unhealthy" => Ok(Self::Unhealthy),
            "healthy" => Ok(Self::Healthy),
            other => Err(AutumnError::bad_request_msg(format!(
                "unknown health '{other}'; expected Unhealthy, Healthy, or empty"
            ))),
        }
    }

    const fn as_label(self) -> &'static str {
        match self {
            Self::All => "",
            Self::Unhealthy => "Unhealthy",
            Self::Healthy => "Healthy",
        }
    }

    fn matches(self, row: &HarvestSchedule) -> bool {
        match self {
            Self::All => true,
            Self::Unhealthy => !schedule_health(row).is_healthy(),
            Self::Healthy => schedule_health(row).is_healthy(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum SchedulePausedFilter {
    #[default]
    All,
    Paused,
    Active,
}

impl SchedulePausedFilter {
    fn parse(raw: &str) -> Result<Self, AutumnError> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" => Ok(Self::All),
            "paused" => Ok(Self::Paused),
            "active" => Ok(Self::Active),
            other => Err(AutumnError::bad_request_msg(format!(
                "unknown paused value '{other}'; expected Paused, Active, or empty"
            ))),
        }
    }

    const fn as_label(self) -> &'static str {
        match self {
            Self::All => "",
            Self::Paused => "Paused",
            Self::Active => "Active",
        }
    }
}

#[derive(Debug, Clone, Default)]
struct ScheduleUiFilters {
    target: Option<String>,
    kind: ScheduleKindFilter,
    paused: SchedulePausedFilter,
    health: ScheduleHealthFilter,
    shard_id: Option<i32>,
}

impl ScheduleUiFilters {
    fn matches(&self, shard_id: ShardId, row: &HarvestSchedule) -> bool {
        let name = row
            .workflow_name
            .as_deref()
            .or(row.dag_name.as_deref())
            .unwrap_or("");

        if !self
            .target
            .as_deref()
            .is_none_or(|t| name.to_lowercase().contains(&t.to_lowercase()))
        {
            return false;
        }
        match self.kind {
            ScheduleKindFilter::Workflow if row.workflow_name.is_none() => return false,
            ScheduleKindFilter::Dag if row.dag_name.is_none() => return false,
            _ => {}
        }
        match self.paused {
            SchedulePausedFilter::Paused if !row.is_paused => return false,
            SchedulePausedFilter::Active if row.is_paused => return false,
            _ => {}
        }
        if self.shard_id.is_some_and(|sid| shard_id.as_i32() != sid) {
            return false;
        }
        if !self.health.matches(row) {
            return false;
        }
        true
    }

    const fn is_empty(&self) -> bool {
        self.target.is_none()
            && matches!(self.kind, ScheduleKindFilter::All)
            && matches!(self.paused, SchedulePausedFilter::All)
            && matches!(self.health, ScheduleHealthFilter::All)
            && self.shard_id.is_none()
    }
}

/// Raw text and validation errors for the Schedules page's `kind`, `paused`,
/// `health`, and `shard_id` filters. Carried alongside `ScheduleUiFilters`,
/// which holds only the successfully parsed values. Same `(parsed,
/// raw_display, error)` contract, and the same reason for existing, as
/// `DeadLetterUiFilterRaw` on the DLQ page.
#[derive(Debug, Clone, Default)]
struct ScheduleUiFilterRaw {
    kind: String,
    kind_error: Option<String>,
    paused: String,
    paused_error: Option<String>,
    health: String,
    health_error: Option<String>,
    shard_id: String,
    shard_id_error: Option<String>,
}

/// Parses the Schedules page's `kind` filter from a raw query-string value.
/// Returns `(parsed, raw_display, error)`. On success `error` is `None`.
/// On an unrecognized value `parsed` is `ScheduleKindFilter::All`, so the
/// filter is not applied, and `error` carries a message to render next to
/// the field.
///
/// Issue: `list_schedules_ui` used to `?`-propagate `ScheduleKindFilter::
/// parse`'s `Result` directly. A bad value aborted the whole page with a
/// bare 400. That happened before the filter form, the table, or the
/// operator's other filters ever rendered. It is the exact page-abort
/// defect already fixed on this page's three sibling list pages: Workflows
/// #1333, Workers #1378, Dead-Letters #1420. It is the one page those PRs
/// never reached.
fn parse_schedule_kind_filter(raw: Option<&str>) -> (ScheduleKindFilter, String, Option<String>) {
    let Some(trimmed) = raw.map(str::trim).filter(|v| !v.is_empty()) else {
        return (ScheduleKindFilter::All, String::new(), None);
    };
    match trimmed.to_ascii_lowercase().as_str() {
        "workflow" => (ScheduleKindFilter::Workflow, trimmed.to_string(), None),
        "dag" => (ScheduleKindFilter::Dag, trimmed.to_string(), None),
        other => (
            ScheduleKindFilter::All,
            trimmed.to_string(),
            Some(format!(
                "Unknown kind '{other}'; expected Workflow, Dag, or empty. Filter not applied."
            )),
        ),
    }
}

/// Parses the Schedules page's `paused` filter. Same contract and same fix
/// as [`parse_schedule_kind_filter`].
fn parse_schedule_paused_filter(
    raw: Option<&str>,
) -> (SchedulePausedFilter, String, Option<String>) {
    let Some(trimmed) = raw.map(str::trim).filter(|v| !v.is_empty()) else {
        return (SchedulePausedFilter::All, String::new(), None);
    };
    match trimmed.to_ascii_lowercase().as_str() {
        "paused" => (SchedulePausedFilter::Paused, trimmed.to_string(), None),
        "active" => (SchedulePausedFilter::Active, trimmed.to_string(), None),
        other => (
            SchedulePausedFilter::All,
            trimmed.to_string(),
            Some(format!(
                "Unknown paused value '{other}'; expected Paused, Active, or empty. Filter not applied."
            )),
        ),
    }
}

/// Parses the Schedules page's `health` filter. Same contract and same fix
/// as [`parse_schedule_kind_filter`].
fn parse_schedule_health_filter(
    raw: Option<&str>,
) -> (ScheduleHealthFilter, String, Option<String>) {
    let Some(trimmed) = raw.map(str::trim).filter(|v| !v.is_empty()) else {
        return (ScheduleHealthFilter::All, String::new(), None);
    };
    match trimmed.to_ascii_lowercase().as_str() {
        "unhealthy" => (ScheduleHealthFilter::Unhealthy, trimmed.to_string(), None),
        "healthy" => (ScheduleHealthFilter::Healthy, trimmed.to_string(), None),
        other => (
            ScheduleHealthFilter::All,
            trimmed.to_string(),
            Some(format!(
                "Unknown health '{other}'; expected Unhealthy, Healthy, or empty. Filter not applied."
            )),
        ),
    }
}

async fn load_schedules_from_shards_ui(api_state: &HarvestApiState) -> Vec<ShardScheduleResult> {
    let pool = match api_state.storage_pool() {
        Ok(p) => p,
        Err(e) => {
            return vec![(ShardId::UNENCODED, Err(e.to_string()))];
        }
    };

    let futs: Vec<_> = pool
        .iter_shards()
        .map(|(shard_id, shard_pool)| async move {
            let result = async {
                let mut conn = acquire_conn(shard_pool).await.map_err(|e| e.to_string())?;
                harvest_schedules::table
                    .order(harvest_schedules::next_run_at.asc())
                    .select(HarvestSchedule::as_select())
                    .load(&mut conn)
                    .await
                    .map_err(|e| e.to_string())
            }
            .await;
            (shard_id, result)
        })
        .collect();

    futures::future::join_all(futs).await
}

async fn load_recent_decisions(
    api_state: &HarvestApiState,
    schedule_ids: &[uuid::Uuid],
) -> std::collections::HashMap<uuid::Uuid, Vec<ScheduleDecision>> {
    use autumn_harvest::schema::harvest_schedule_decisions::dsl;

    if schedule_ids.is_empty() {
        return std::collections::HashMap::new();
    }
    let Ok(pool) = api_state.storage_pool() else {
        return std::collections::HashMap::new();
    };

    let mut all_rows: Vec<ScheduleDecision> = Vec::new();

    for (_shard, shard_pool) in pool.iter_shards() {
        let Ok(mut conn) = acquire_conn(shard_pool).await else {
            continue;
        };
        let mut rows: Vec<ScheduleDecision> = dsl::harvest_schedule_decisions
            .filter(dsl::schedule_id.eq_any(schedule_ids))
            .select(ScheduleDecision::as_select())
            .load(&mut conn)
            .await
            .unwrap_or_default();

        all_rows.append(&mut rows);
    }

    let mut map: std::collections::HashMap<uuid::Uuid, Vec<ScheduleDecision>> =
        std::collections::HashMap::new();

    for row in all_rows {
        if let Some(sched_id) = row.schedule_id {
            map.entry(sched_id).or_default().push(row);
        }
    }

    for decisions in map.values_mut() {
        decisions.sort_by(|a, b| {
            b.occurred_at
                .cmp(&a.occurred_at)
                .then_with(|| b.id.cmp(&a.id))
        });
        decisions.truncate(10);
    }

    map
}

/// Order the schedules list: unhealthy first (issue #951 AC3), then the
/// pre-existing `next_run_at`-ascending / name / id order.
///
/// The health rank is a *prefix* on the existing comparator rather than a
/// replacement for it, so healthy rows keep exactly the relative order they had
/// before this page grew a health column.
fn sort_schedule_rows(rows: &mut [(ShardId, HarvestSchedule)]) {
    rows.sort_by(|(_, a), (_, b)| {
        schedule_health(a)
            .rank()
            .cmp(&schedule_health(b).rank())
            .then_with(|| match (a.next_run_at, b.next_run_at) {
                (Some(a_ts), Some(b_ts)) => a_ts.cmp(&b_ts),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => {
                    let a_name = a
                        .workflow_name
                        .as_deref()
                        .or(a.dag_name.as_deref())
                        .unwrap_or("");
                    let b_name = b
                        .workflow_name
                        .as_deref()
                        .or(b.dag_name.as_deref())
                        .unwrap_or("");
                    a_name.cmp(b_name)
                }
            })
            .then_with(|| a.id.cmp(&b.id))
    });
}

async fn list_schedules_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Query(params): Query<ScheduleListParams>,
) -> Result<Markup, AutumnError> {
    let limit = params
        .limit
        .unwrap_or(DEFAULT_SCHEDULE_PAGE_SIZE)
        .clamp(1, MAX_PAGE_SIZE);
    let page = params.page.unwrap_or(0).max(0);
    let offset = page.saturating_mul(limit);

    // The page used to `?`-propagate each of these on a bad value. That
    // aborted the whole request with a bare 400 before the filter form
    // ever rendered. It discarded whichever of the five filters the
    // operator had already typed. Each bad field now degrades to "not
    // applied" instead, handing back the raw text plus an error to
    // redisplay inline. Same fix as `parse_worker_status_filter` (#1378)
    // and `parse_dead_letter_ui_filters` (#1420) use on the sibling list
    // pages.
    let (kind, kind_raw, kind_error) = parse_schedule_kind_filter(params.kind.as_deref());
    let (paused_filter, paused_raw, paused_error) =
        parse_schedule_paused_filter(params.paused.as_deref());
    let (health_filter, health_raw, health_error) =
        parse_schedule_health_filter(params.health.as_deref());
    let (shard_id, shard_id_raw, shard_id_error) =
        parse_shard_id_filter("shard_id", params.shard_id.as_deref());
    let target = params
        .target
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);

    let filters = ScheduleUiFilters {
        target,
        kind,
        paused: paused_filter,
        health: health_filter,
        shard_id,
    };
    let filter_raw = ScheduleUiFilterRaw {
        kind: kind_raw,
        kind_error,
        paused: paused_raw,
        paused_error,
        health: health_raw,
        health_error,
        shard_id: shard_id_raw,
        shard_id_error,
    };

    let shard_results = load_schedules_from_shards_ui(&api_state).await;
    let is_multi_shard = shard_results.len() > 1;

    let shard_errors: Vec<(ShardId, String)> = shard_results
        .iter()
        .filter_map(|(sid, r)| r.as_ref().err().map(|e| (*sid, e.clone())))
        .collect();

    // Flatten + filter across all shards.
    let mut all_rows: Vec<(ShardId, HarvestSchedule)> = shard_results
        .into_iter()
        .flat_map(|(shard_id, result)| {
            result
                .into_iter()
                .flat_map(move |rows| rows.into_iter().map(move |r| (shard_id, r)))
        })
        .filter(|(sid, row)| filters.matches(*sid, row))
        .collect();

    sort_schedule_rows(&mut all_rows);

    let total_filtered = all_rows.len();
    // Computed over the whole filtered set, before pagination slices it.
    let unhealthy_summary = schedule_health_summary(&all_rows);
    let distribution = schedule_kind_distribution(&all_rows);
    let offset_usize = usize::try_from(offset).unwrap_or(usize::MAX);
    let limit_usize = usize::try_from(limit).unwrap_or(usize::MAX);
    let has_next = total_filtered > offset_usize.saturating_add(limit_usize);
    let page_rows: Vec<(ShardId, HarvestSchedule)> = all_rows
        .into_iter()
        .skip(offset_usize)
        .take(limit_usize)
        .collect();

    let schedule_ids: Vec<uuid::Uuid> = page_rows.iter().map(|(_, r)| r.id).collect();
    let decisions = load_recent_decisions(&api_state, &schedule_ids).await;

    Ok(render_schedules_page(
        &page_rows,
        &shard_errors,
        is_multi_shard,
        &filters,
        &filter_raw,
        &decisions,
        page,
        limit,
        has_next,
        total_filtered,
        &unhealthy_summary,
        &distribution,
        params.refresh,
        params.flash.as_deref(),
    ))
}

/// Parse a `ScheduleUiFilters` from optional string fields.
/// Parses a `ScheduleUiFilters` for the bulk-action POST forms
/// (`../schedules/bulk-pause`, `../schedules/bulk-resume`).
///
/// `kind`/`paused`/`health` keep the pre-existing "unrecognized value
/// omits that filter" leniency. This PR does not touch that behavior. It
/// matches the GET list page's "filter not applied" contract for a bad
/// value.
///
/// `shard_id` does not keep that leniency. Unlike the other three
/// fields, a broadened `shard_id` does not just show the operator a
/// bigger table. It *pauses or resumes schedules on every shard*, not
/// just the one they scoped the action to. Before this PR, `shard_id:
/// Option<i32>` was typed directly on `ScheduleBulkParams`. A non-numeric
/// value therefore failed axum's `Form<..>` extraction, and the whole
/// request 400ed before any schedule was touched. Retyping it
/// `Option<String>` fixes the GET-page 400 (see `parse_shard_id_filter`).
/// Silently dropping a parse failure to `None` here would mean "no shard
/// restriction". That would reopen the same gap one layer down. Here,
/// "not applied" would mean "every shard", not "not this shard" (Codex
/// review, P1). A malformed `shard_id` in a bulk form is therefore
/// rejected outright, restoring the pre-PR behavior for this one field.
fn parse_schedule_bulk_filters(
    params: &ScheduleBulkParams,
) -> Result<ScheduleUiFilters, AutumnError> {
    let kind = params
        .kind
        .as_deref()
        .and_then(|s| ScheduleKindFilter::parse(s).ok())
        .unwrap_or(ScheduleKindFilter::All);
    let paused = params
        .paused
        .as_deref()
        .and_then(|s| SchedulePausedFilter::parse(s).ok())
        .unwrap_or(SchedulePausedFilter::All);
    let health = params
        .health
        .as_deref()
        .and_then(|s| ScheduleHealthFilter::parse(s).ok())
        .unwrap_or(ScheduleHealthFilter::All);
    let target = params
        .target
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let shard_id = match params.shard_id.as_deref().map(str::trim) {
        None | Some("") => None,
        Some(raw) => Some(raw.parse::<i32>().map_err(|_| {
            AutumnError::bad_request_msg(format!(
                "invalid shard_id '{raw}'; expected a whole number"
            ))
        })?),
    };
    Ok(ScheduleUiFilters {
        target,
        kind,
        paused,
        health,
        shard_id,
    })
}

/// Find a schedule by id across all shards. Returns the row, the shard it lives
/// on, and a conn to that shard on success.
#[allow(clippy::result_large_err)]
async fn find_schedule_row(
    api_state: &HarvestApiState,
    id_str: &str,
) -> Result<
    Option<(
        HarvestSchedule,
        autumn_harvest::types::ShardId,
        crate::api::PoolConn,
    )>,
    axum::response::Response,
> {
    use autumn_harvest::schema::harvest_schedules::dsl;
    use axum::response::IntoResponse as _;

    let Ok(id) = id_str.parse::<uuid::Uuid>() else {
        return Err(
            AutumnError::bad_request_msg(format!("invalid schedule id '{id_str}'")).into_response(),
        );
    };
    let pool = api_state
        .storage_pool()
        .map_err(|e| map_error(e).into_response())?;

    for (shard, shard_pool) in pool.iter_shards() {
        let Ok(mut conn) = acquire_conn(shard_pool).await else {
            continue;
        };
        let row: Option<HarvestSchedule> = dsl::harvest_schedules
            .find(id)
            .select(HarvestSchedule::as_select())
            .first(&mut conn)
            .await
            .optional()
            .unwrap_or(None);
        if let Some(row) = row {
            return Ok(Some((row, shard, conn)));
        }
    }
    Ok(None)
}

fn schedule_name(row: &HarvestSchedule) -> String {
    row.workflow_name
        .as_deref()
        .or(row.dag_name.as_deref())
        .unwrap_or("")
        .to_string()
}

async fn schedule_pause_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id_str): Path<String>,
) -> axum::response::Response {
    use autumn_harvest::schema::harvest_schedules::dsl;

    let found = match find_schedule_row(&api_state, &id_str).await {
        Ok(f) => f,
        Err(response) => return response,
    };

    let flash = if let Some((row, _shard, mut conn)) = found {
        let name = schedule_name(&row);
        let now = Utc::now();
        let _ = diesel::update(
            dsl::harvest_schedules
                .find(row.id)
                .filter(dsl::is_paused.ne(true)),
        )
        .set((
            dsl::is_paused.eq(true),
            dsl::paused_at.eq(Some(now)),
            dsl::paused_by.eq(Some("ui")),
            dsl::updated_at.eq(now),
        ))
        .execute(&mut conn)
        .await;
        let ar = NewAuditRecord {
            actor: "ui",
            operation: OP_SCHEDULE_PAUSE,
            target_type: TARGET_SCHEDULE,
            target_id: Some(id_str.as_str()),
            route_or_command: "POST /ui/schedules/pause",
            request_id: None,
            idempotency_key: None,
            status: STATUS_SUCCEEDED,
            error_summary: None,
            shard_id: None,
            source: SOURCE_UI,
        };
        let _ = insert_audit(&mut conn, &ar).await;
        format!("Paused {name}")
    } else {
        format!("Paused schedule {}", &id_str[..8.min(id_str.len())])
    };
    schedule_redirect(&flash)
}

async fn schedule_resume_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id_str): Path<String>,
) -> axum::response::Response {
    use autumn_harvest::schema::harvest_schedules::dsl;

    let found = match find_schedule_row(&api_state, &id_str).await {
        Ok(f) => f,
        Err(response) => return response,
    };

    let flash = if let Some((row, _shard, mut conn)) = found {
        let name = schedule_name(&row);
        let now = Utc::now();
        // Mirrors `set_schedule_paused(.., false, ..)`: the predicate matches an
        // auto-paused row (`is_paused = false`, `auto_paused_at` set), and the
        // update clears the auto-pause state and resets the failure counter so
        // the next tick does not immediately re-trigger auto-pause (#360).
        let _ = diesel::update(
            dsl::harvest_schedules.find(row.id).filter(
                dsl::is_paused
                    .ne(false)
                    .or(dsl::auto_paused_at.is_not_null()),
            ),
        )
        .set((
            dsl::is_paused.eq(false),
            dsl::paused_at.eq(None::<chrono::DateTime<Utc>>),
            dsl::paused_by.eq(None::<&str>),
            dsl::pause_reason.eq(None::<&str>),
            dsl::auto_paused_at.eq(None::<chrono::DateTime<Utc>>),
            dsl::consecutive_failure_count.eq(0),
            dsl::updated_at.eq(now),
        ))
        .execute(&mut conn)
        .await;
        let ar = NewAuditRecord {
            actor: "ui",
            operation: OP_SCHEDULE_RESUME,
            target_type: TARGET_SCHEDULE,
            target_id: Some(id_str.as_str()),
            route_or_command: "POST /ui/schedules/resume",
            request_id: None,
            idempotency_key: None,
            status: STATUS_SUCCEEDED,
            error_summary: None,
            shard_id: None,
            source: SOURCE_UI,
        };
        let _ = insert_audit(&mut conn, &ar).await;
        format!("Resumed {name}")
    } else {
        format!("Resumed schedule {}", &id_str[..8.min(id_str.len())])
    };
    schedule_redirect(&flash)
}

async fn schedule_delete_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id_str): Path<String>,
) -> axum::response::Response {
    use autumn_harvest::schema::harvest_schedules::dsl;

    let found = match find_schedule_row(&api_state, &id_str).await {
        Ok(f) => f,
        Err(response) => return response,
    };

    let flash = if let Some((row, _shard, mut conn)) = found {
        let name = schedule_name(&row);
        let n = diesel::delete(dsl::harvest_schedules.find(row.id))
            .execute(&mut conn)
            .await
            .unwrap_or(0);
        if n > 0 {
            let ar = NewAuditRecord {
                actor: "ui",
                operation: OP_SCHEDULE_DELETE,
                target_type: TARGET_SCHEDULE,
                target_id: Some(id_str.as_str()),
                route_or_command: "POST /ui/schedules/delete",
                request_id: None,
                idempotency_key: None,
                status: STATUS_SUCCEEDED,
                error_summary: None,
                shard_id: None,
                source: SOURCE_UI,
            };
            let _ = insert_audit(&mut conn, &ar).await;
            format!("Deleted {name}")
        } else {
            format!(
                "Schedule {} was already deleted",
                &id_str[..8.min(id_str.len())]
            )
        }
    } else {
        format!("Schedule {} not found", &id_str[..8.min(id_str.len())])
    };
    schedule_redirect(&flash)
}

/// Inner logic for `schedule_trigger_now_ui` after the connection is acquired.
/// Handles the Skip overlap check, start call, audit write, and metric emit,
/// then returns the redirect response so the outer handler stays compact.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn execute_schedule_trigger_ui(
    conn: &mut crate::api::PoolConn,
    pool: &crate::HarvestDbPool,
    runtime: &HarvestApiRuntime,
    gate_cache: &autumn_harvest::admission_gate::AdmissionGateCache,
    row: &HarvestSchedule,
    id_str: &str,
    name: &str,
    workflow_name: &str,
    input: serde_json::Value,
    queue: &str,
) -> axum::response::Response {
    // Pre-generate workflow_id and triggered_at so the gate check uses the
    // actual execution shard (determined by the router) rather than the shard
    // where the schedule row was found.  The values are reused below.
    let triggered_at = chrono::Utc::now();
    let workflow_id = format!(
        "manual-{}-{}-{}",
        row.id,
        triggered_at.timestamp_millis(),
        uuid::Uuid::new_v4().simple()
    );
    let exec_shard = runtime
        .router()
        .pick_for_new_workflow(workflow_name, &workflow_id);

    // issue #377: check admission gates before firing this manual schedule trigger.
    {
        let dag_name_for_owner = row.dag_name.as_deref().unwrap_or(workflow_name);
        let wf_owner = runtime
            .registry()
            .workflows
            .get(workflow_name)
            .and_then(|i| i.owner)
            .or_else(|| {
                runtime
                    .dags()
                    .get(dag_name_for_owner)
                    .and_then(|d| d.owner.as_deref())
            });
        if let Some((gate_id, gate_reason, scope_kind)) =
            gate_cache.check(workflow_name, queue, exec_shard.as_i32(), wf_owner)
        {
            let reason_label = match gate_reason.char_indices().nth(64) {
                Some((idx, _)) => &gate_reason[..idx],
                None => &gate_reason,
            };
            runtime
                .registry()
                .telemetry()
                .metrics
                .record_admission_blocked(scope_kind, reason_label);
            let ar = build_trigger_audit("ui", id_str, STATUS_FAILED, Some("admission_blocked"));
            let _ = insert_audit(conn, &ar).await;
            return schedule_redirect(&format!("Trigger blocked by gate {gate_id}: {gate_reason}"));
        }
    }

    // Count active (RUNNING or PAUSED) executions across ALL shards. A PAUSED run
    // still occupies an active slot for overlap/Skip enforcement (issue #383),
    // matching the scheduler and backfill counters. The async block returns None
    // if any shard is unreachable — used for fail-closed Skip enforcement.
    // The `schedule_id` disjunct (issue #1160) also counts a cross-type
    // continue-as-new successor of this schedule -- otherwise a manual trigger
    // could double-dispatch a schedule whose active run has already changed
    // type mid-chain, matching `scheduler::schedule_running_basis`.
    let running_count: Option<i64> = async {
        let mut total: i64 = 0;
        for (_, shard_pool) in pool.iter_shards() {
            let mut c = acquire_conn(shard_pool).await.ok()?;
            let n: i64 = harvest_workflow_executions::table
                .filter(
                    harvest_workflow_executions::workflow_name
                        .eq(workflow_name)
                        .or(harvest_workflow_executions::schedule_id.eq(Some(row.id))),
                )
                .filter(harvest_workflow_executions::state.eq_any(["RUNNING", "PAUSED"]))
                .count()
                .get_result(&mut c)
                .await
                .ok()?;
            total += n;
        }
        Some(total)
    }
    .await;
    if autumn_harvest::OverlapPolicy::from_db(&row.overlap_policy)
        == autumn_harvest::OverlapPolicy::Skip
    {
        match running_count {
            None => {
                let ar = build_trigger_audit("ui", id_str, STATUS_FAILED, Some("count_failed"));
                let _ = insert_audit(conn, &ar).await;
                runtime
                    .registry()
                    .telemetry()
                    .metrics
                    .record_schedule_manual_trigger(name, "start_failed");
                return schedule_redirect(&format!(
                    "Failed to trigger {name}: could not count active runs"
                ));
            }
            Some(n) if n >= i64::from(row.max_active_runs) => {
                let ar =
                    build_trigger_audit("ui", id_str, STATUS_SUCCEEDED, Some("skipped_overlap"));
                let _ = insert_audit(conn, &ar).await;
                runtime
                    .registry()
                    .telemetry()
                    .metrics
                    .record_schedule_manual_trigger(name, "skipped_overlap");
                return schedule_redirect(&format!(
                    "Skipped {name}: max_active_runs already reached"
                ));
            }
            Some(_) => {}
        }
    }
    // triggered_at and workflow_id were pre-generated above for the gate check.
    let exec_id = HarvestExecutionId::new();
    let (owner, runbook_url, severity) = {
        let wf_meta = runtime
            .registry()
            .workflows
            .get(workflow_name)
            .map(|info| (info.owner, info.runbook_url, info.severity));
        let dag_meta = runtime.dags().get(workflow_name).map(|dag| {
            (
                dag.owner.as_deref(),
                dag.runbook_url.as_deref(),
                dag.severity.as_deref(),
            )
        });
        match (wf_meta, dag_meta) {
            (Some((o, r, s)), Some((dag_owner, dag_runbook, dag_severity))) => {
                (o.or(dag_owner), r.or(dag_runbook), s.or(dag_severity))
            }
            (Some((o, r, s)), None) => (o, r, s),
            (None, Some((dag_owner, dag_runbook, dag_severity))) => {
                (dag_owner, dag_runbook, dag_severity)
            }
            (None, None) => (None, None, None),
        }
    };
    // `workflow_name` here is either a registered workflow's own name, or --
    // for a DAG-backed schedule, per `resolve_trigger_params` above -- the
    // `dag_name`, which is also the key `DagInfo::as_workflow_info()`
    // registers a DAG's shadow `WorkflowInfo` under in `registry.workflows`.
    // So this ONE lookup already resolves both a workflow's AND a DAG's
    // declared `sla`/`execution_timeout` (issue #743 review, PR #1141
    // finding #6) -- the previous "DAGs have no SLA concept" framing predates
    // DAG-level `sla`/`execution_timeout` support and only ever described the
    // caller's mental model, not an actual code gap; `execution_timeout`
    // itself was genuinely never resolved here, unlike `sla`.
    let (sla, wf_default_retry_policy, execution_timeout) = runtime
        .registry()
        .workflows
        .get(workflow_name)
        .map_or((None, None, None), |info| {
            (
                crate::api::clamp_info_default_sla(info.sla, info.execution_timeout),
                info.retry_policy.clone(),
                info.execution_timeout
                    .and_then(|d| chrono::Duration::from_std(d).ok()),
            )
        });
    let max_execution_timeout_ceiling = runtime
        .registry()
        .max_workflow_execution_timeout
        .and_then(|d| chrono::Duration::from_std(d).ok());
    // Schedule-level retry_policy takes precedence over the workflow-type default,
    // mirroring the automated tick, backfill, and API trigger-now paths.
    let ui_trigger_retry_policy = row
        .retry_policy
        .as_ref()
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .or(wf_default_retry_policy);

    // Provenance ref for a manual UI schedule trigger is the schedule id (#740).
    let ui_schedule_id_str = row.id.to_string();
    let result = start_or_load_workflow_execution_with_metrics_and_codecs(
        conn,
        StartWorkflowParams {
            workflow_name,
            workflow_id: &workflow_id,
            exec_id,
            input,
            parent_id: None,
            queue_name: queue,
            execution_timeout,
            memo: None,
            search_attrs: None,
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
            trace_context: None,
            max_execution_timeout_ceiling,
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
            owner,
            runbook_url,
            severity,
            context_headers: None,
            sla,
            // Manual trigger-now fires are attributed to the schedule (schedule_id is
            // set) so they appear in GET /admin/schedules/{id}/runs, but scheduled_for
            // stays None so resolve_carryover (issue #488) still short-circuits for
            // this run — NULL slot comparisons are false, so carryover is never
            // resolved for a manual fire.
            schedule_id: Some(row.id),
            scheduled_for: None,
            workflow_attempt: 1,
            workflow_retry_policy: ui_trigger_retry_policy,
            retry_of_exec_id: None,
            max_workflow_attempts_ceiling: runtime.registry().max_workflow_attempts_ceiling,
            origin: Some(autumn_harvest::execution::ORIGIN_MANUAL_TRIGGER),
            completion_callbacks: None,
            // Manual UI schedule trigger (issue #740): provenance is `schedule`,
            // referencing the schedule id, attributed to the UI operator.
            start_source: autumn_harvest::StartSource::Schedule,
            start_source_ref: Some(ui_schedule_id_str.as_str()),
            started_by: Some("ui"),
        },
        Some(runtime.registry().telemetry().metrics.as_ref()),
        None,
        runtime.registry().payload_codecs(),
    )
    .await;
    let (status, outcome) = if result.is_ok() {
        (STATUS_SUCCEEDED, "fired")
    } else {
        (STATUS_FAILED, "start_failed")
    };
    let ar = build_trigger_audit(
        "ui",
        id_str,
        status,
        result.is_err().then_some("start_failed"),
    );
    let _ = insert_audit(conn, &ar).await;
    runtime
        .registry()
        .telemetry()
        .metrics
        .record_schedule_manual_trigger(name, outcome);
    schedule_redirect(&match result {
        Ok(_) => format!("Triggered run of {name}"),
        Err(e) => format!("Failed to trigger {name}: {e}"),
    })
}

/// Build a `NewAuditRecord` for UI schedule trigger operations.
const fn build_trigger_audit<'a>(
    actor: &'a str,
    target_id: &'a str,
    status: &'a str,
    error_summary: Option<&'a str>,
) -> NewAuditRecord<'a> {
    NewAuditRecord {
        actor,
        operation: OP_SCHEDULE_TRIGGER,
        target_type: TARGET_SCHEDULE,
        target_id: Some(target_id),
        route_or_command: "POST /ui/schedules/trigger-now",
        request_id: None,
        idempotency_key: None,
        status,
        error_summary,
        shard_id: None,
        source: SOURCE_UI,
    }
}

/// Resolve `(workflow_name, input, queue)` for a manual trigger, consulting the
/// runtime registry for DAG-backed schedules so the correct default queue is used.
fn resolve_trigger_params(
    row: &HarvestSchedule,
    runtime: &HarvestApiRuntime,
) -> Result<(String, serde_json::Value, String), String> {
    match (row.workflow_name.as_deref(), row.dag_name.as_deref()) {
        (Some(wf), _) => {
            let q = row.queue_name.as_deref().unwrap_or("default").to_string();
            Ok((
                wf.to_string(),
                row.workflow_input
                    .clone()
                    .unwrap_or(serde_json::Value::Null),
                q,
            ))
        }
        (None, Some(dag)) => {
            let q = runtime
                .dags()
                .get(dag)
                .and_then(|d| d.default_queue.as_deref())
                .or(row.queue_name.as_deref())
                .unwrap_or("default")
                .to_string();
            Ok((dag.to_string(), serde_json::Value::Null, q))
        }
        (None, None) => Err("schedule has no workflow or dag name".to_string()),
    }
}

async fn schedule_trigger_now_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id_str): Path<String>,
) -> axum::response::Response {
    let found = match find_schedule_row(&api_state, &id_str).await {
        Ok(f) => f,
        Err(response) => return response,
    };
    let Some((row, _found_shard, _)) = found else {
        return schedule_redirect(&format!(
            "Schedule {} not found",
            &id_str[..8.min(id_str.len())]
        ));
    };
    let name = schedule_name(&row);
    let runtime = match api_state.runtime() {
        Ok(r) => r,
        Err(e) => return schedule_redirect(&format!("Failed to trigger {name}: {e}")),
    };
    let (workflow_name, input, queue) = match resolve_trigger_params(&row, &runtime) {
        Ok(p) => p,
        Err(e) => return schedule_redirect(&format!("Failed to trigger {name}: {e}")),
    };
    let pool = match api_state.storage_pool() {
        Ok(p) => p,
        Err(e) => return schedule_redirect(&format!("Failed to trigger {name}: {e}")),
    };
    // Use default_pool() so ExecutionId::new() (ShardId::UNENCODED) and the
    // connection target agree — consistent with the API handler's routing.
    let mut conn = match acquire_conn(pool.default_pool()).await {
        Ok(c) => c,
        Err(e) => return schedule_redirect(&format!("Failed to trigger {name}: {e}")),
    };
    execute_schedule_trigger_ui(
        &mut conn,
        &pool,
        &runtime,
        &api_state.gate_cache(),
        &row,
        &id_str,
        &name,
        &workflow_name,
        input,
        &queue,
    )
    .await
}

async fn schedule_bulk_pause_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Form(params): Form<ScheduleBulkParams>,
) -> axum::response::Response {
    use autumn_harvest::schema::harvest_schedules::dsl;
    use axum::response::IntoResponse as _;

    let filters = match parse_schedule_bulk_filters(&params) {
        Ok(f) => f,
        Err(e) => return e.into_response(),
    };

    let pool = match api_state.storage_pool() {
        Ok(p) => p,
        Err(e) => return map_error(e).into_response(),
    };

    let now = Utc::now();
    let mut acted_on = 0usize;

    for (shard_id, shard_pool) in pool.iter_shards() {
        if filters.shard_id.is_some_and(|sid| shard_id.as_i32() != sid) {
            continue;
        }
        let Ok(mut conn) = acquire_conn(shard_pool).await else {
            continue;
        };
        // Select whole rows and reuse `ScheduleUiFilters::matches` — the *same*
        // predicate the list page applies (issue #951). A partial projection
        // cannot see the health filter, and the button's count and confirmation
        // text come from the list's filtered total: a bulk action that matched a
        // wider set than the operator was shown would pause schedules the dialog
        // never mentioned.
        let candidates: Vec<HarvestSchedule> = dsl::harvest_schedules
            .filter(dsl::is_paused.ne(true))
            .select(HarvestSchedule::as_select())
            .load(&mut conn)
            .await
            .unwrap_or_default();
        let matching_ids: Vec<uuid::Uuid> = candidates
            .into_iter()
            .filter(|row| filters.matches(shard_id, row))
            .map(|row| row.id)
            .collect();
        if matching_ids.is_empty() {
            continue;
        }
        let updated_ids: Vec<uuid::Uuid> = diesel::update(
            dsl::harvest_schedules
                .filter(dsl::id.eq_any(&matching_ids))
                .filter(dsl::is_paused.ne(true)),
        )
        .set((
            dsl::is_paused.eq(true),
            dsl::paused_at.eq(Some(now)),
            dsl::paused_by.eq(Some("ui-bulk")),
            dsl::updated_at.eq(now),
        ))
        .returning(dsl::id)
        .get_results(&mut conn)
        .await
        .unwrap_or_default();
        acted_on += updated_ids.len();
        // One multi-row insert per shard, not one round trip per updated
        // schedule (issue #1399). Every record shares the same
        // actor/operation/route/status/shard, so only the target id varies.
        // `insert_audit_batch` preserves that shape exactly.
        let id_strs: Vec<String> = updated_ids.iter().map(ToString::to_string).collect();
        let records: Vec<NewAuditRecord<'_>> = id_strs
            .iter()
            .map(|id_str| NewAuditRecord {
                actor: "ui",
                operation: OP_SCHEDULE_PAUSE,
                target_type: TARGET_SCHEDULE,
                target_id: Some(id_str.as_str()),
                route_or_command: "POST /ui/schedules/bulk-pause",
                request_id: None,
                idempotency_key: None,
                status: STATUS_SUCCEEDED,
                error_summary: None,
                shard_id: Some(shard_id.as_i32()),
                source: SOURCE_UI,
            })
            .collect();
        let _ = insert_audit_batch(&mut conn, &records).await;
    }

    schedule_bulk_redirect_to(
        params.return_to.as_deref(),
        &format!("Paused {acted_on} schedule(s)"),
    )
}

async fn schedule_bulk_resume_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Form(params): Form<ScheduleBulkParams>,
) -> axum::response::Response {
    use autumn_harvest::schema::harvest_schedules::dsl;
    use axum::response::IntoResponse as _;

    let filters = match parse_schedule_bulk_filters(&params) {
        Ok(f) => f,
        Err(e) => return e.into_response(),
    };

    let pool = match api_state.storage_pool() {
        Ok(p) => p,
        Err(e) => return map_error(e).into_response(),
    };

    let now = Utc::now();
    let mut acted_on = 0usize;

    for (shard_id, shard_pool) in pool.iter_shards() {
        if filters.shard_id.is_some_and(|sid| shard_id.as_i32() != sid) {
            continue;
        }
        let Ok(mut conn) = acquire_conn(shard_pool).await else {
            continue;
        };
        // Whole rows + the list's own matcher, so the health filter applies and
        // the acted-on set is exactly the set the confirmation counted (#951).
        // `is_paused = true OR auto_paused_at IS NOT NULL`, matching the API's
        // resume predicate — an auto-paused schedule has `is_paused = false`
        // and would otherwise be unreachable from a bulk resume (#360).
        let candidates: Vec<HarvestSchedule> = dsl::harvest_schedules
            .filter(
                dsl::is_paused
                    .eq(true)
                    .or(dsl::auto_paused_at.is_not_null()),
            )
            .select(HarvestSchedule::as_select())
            .load(&mut conn)
            .await
            .unwrap_or_default();
        let matching_ids: Vec<uuid::Uuid> = candidates
            .into_iter()
            .filter(|row| filters.matches(shard_id, row))
            .map(|row| row.id)
            .collect();
        if matching_ids.is_empty() {
            continue;
        }
        let updated_ids: Vec<uuid::Uuid> = diesel::update(
            dsl::harvest_schedules
                .filter(dsl::id.eq_any(&matching_ids))
                .filter(
                    dsl::is_paused
                        .eq(true)
                        .or(dsl::auto_paused_at.is_not_null()),
                ),
        )
        .set((
            dsl::is_paused.eq(false),
            dsl::paused_at.eq(None::<chrono::DateTime<Utc>>),
            dsl::paused_by.eq(None::<&str>),
            dsl::pause_reason.eq(None::<&str>),
            dsl::auto_paused_at.eq(None::<chrono::DateTime<Utc>>),
            dsl::consecutive_failure_count.eq(0),
            dsl::updated_at.eq(now),
        ))
        .returning(dsl::id)
        .get_results(&mut conn)
        .await
        .unwrap_or_default();
        acted_on += updated_ids.len();
        // Same batching rationale as `schedule_bulk_pause_ui` above (issue
        // #1399): one multi-row insert per shard, not one per resumed row.
        let id_strs: Vec<String> = updated_ids.iter().map(ToString::to_string).collect();
        let records: Vec<NewAuditRecord<'_>> = id_strs
            .iter()
            .map(|id_str| NewAuditRecord {
                actor: "ui",
                operation: OP_SCHEDULE_RESUME,
                target_type: TARGET_SCHEDULE,
                target_id: Some(id_str.as_str()),
                route_or_command: "POST /ui/schedules/bulk-resume",
                request_id: None,
                idempotency_key: None,
                status: STATUS_SUCCEEDED,
                error_summary: None,
                shard_id: Some(shard_id.as_i32()),
                source: SOURCE_UI,
            })
            .collect();
        let _ = insert_audit_batch(&mut conn, &records).await;
    }

    schedule_bulk_redirect_to(
        params.return_to.as_deref(),
        &format!("Resumed {acted_on} schedule(s)"),
    )
}

/// Redirect back to the schedules list with a flash message.
///
/// `depth` is how many path segments below the UI mount point the *redirecting*
/// route sits, because the `Location` header is resolved relative to the
/// request URL. `/schedules/bulk-pause` is one segment deep, so a bare
/// `schedules?flash=…` is right there; `/schedules/{id}/pause` is **two**, where
/// the same string resolves to `<mount>/schedules/{id}/schedules` and 404s.
fn schedule_redirect_from(depth: usize, flash: &str) -> axum::response::Response {
    use axum::response::IntoResponse as _;
    let up = "../".repeat(depth.saturating_sub(1));
    let location = format!("{up}schedules?flash={}", url_encode(flash));
    axum::response::Redirect::to(&location).into_response()
}

/// Redirect from a `/schedules/{id}/…` per-row action (two segments deep).
fn schedule_redirect(flash: &str) -> axum::response::Response {
    schedule_redirect_from(2, flash)
}

/// Redirect from a bulk-action POST (`/schedules/bulk-pause`,
/// `/schedules/bulk-resume`) back to the operator's filtered view instead
/// of always landing on a bare, unfiltered `schedules?flash=…`.
///
/// Before this fix, the bulk forms submitted only the parsed filters
/// (`render_schedule_hidden_filters`). An operator with an unresolved
/// invalid filter and its inline error lost both the moment they paused
/// or resumed anything. That is the same "action discards what you were
/// looking at" gap. #1420 already fixed it for the DLQ page's own bulk
/// actions (Codex review, #1437 P2).
///
/// `return_to` is the bulk form's own hidden field. It is built by
/// `schedule_return_to_path` from the same filters the list page just
/// rendered — raw text, invalid values included. It is validated against
/// the expected `../schedules[?...]` shape before use. That is the same
/// guard `is_dead_letter_ui_return_path` applies to the DLQ page's
/// `return_to`, so this operator-supplied field can never redirect
/// anywhere else.
///
/// The `../` matters. The `Location` header resolves relative to the URL
/// this handler was posted to (`.../schedules/bulk-pause`), not to the
/// list page. A bare `schedules?...` would merge onto that path's own
/// directory instead. It would land on `.../schedules/schedules?...` — a
/// 404 after a mutation that otherwise succeeded (Codex review, #1437
/// P2, verified against `urllib.parse.urljoin`).
///
/// `is_schedule_ui_return_path` also rejects a control character. A raw
/// `\n` could reach it from a hand-crafted or malformed POST. The
/// `Redirect`'s own `into_response` builds the `Location` header with
/// `HeaderValue::try_from`, which rejects those bytes and falls back to
/// a bare `500` (Codex review, #1437 P2). That is not a panic in this
/// axum version, verified by reading `axum-0.8.9`'s own `Redirect::
/// into_response`. It is still the wrong response after a mutation that
/// already succeeded.
fn schedule_bulk_redirect_to(return_to: Option<&str>, flash: &str) -> axum::response::Response {
    use axum::response::IntoResponse as _;
    let base = return_to
        .map(str::trim)
        .filter(|value| is_schedule_ui_return_path(value))
        .map_or_else(|| "../schedules".to_string(), str::to_string);
    let separator = if base.contains('?') { '&' } else { '?' };
    let location = format!("{base}{separator}flash={}", url_encode(flash));
    axum::response::Redirect::to(&location).into_response()
}

fn is_schedule_ui_return_path(value: &str) -> bool {
    if value.bytes().any(|b| b.is_ascii_control()) {
        return false;
    }
    match value.strip_prefix("../schedules") {
        Some("") => true,
        Some(rest) => rest.starts_with('?'),
        None => false,
    }
}

/// Built for the bulk-action forms' `return_to` hidden field, so its base
/// carries the `../` those forms need — see `schedule_bulk_redirect_to`.
fn schedule_return_to_path(
    filters: &ScheduleUiFilters,
    filter_raw: &ScheduleUiFilterRaw,
    limit: i64,
    refresh: Option<u64>,
) -> String {
    let query = build_schedule_query_string(limit, filters, filter_raw, refresh);
    if query.is_empty() {
        "../schedules".to_string()
    } else {
        format!("../schedules?{}", &query[1..])
    }
}

// ---------------------------------------------------------------------------
// Schedule rendering helpers
// ---------------------------------------------------------------------------

/// Returns a short distribution string like "3 Workflow, 2 Dag" for all matching rows.
fn schedule_kind_distribution(rows: &[(ShardId, HarvestSchedule)]) -> String {
    let mut wf = 0usize;
    let mut dag = 0usize;
    for (_, row) in rows {
        if row.workflow_name.is_some() {
            wf += 1;
        } else {
            dag += 1;
        }
    }
    match (wf, dag) {
        (0, 0) => String::new(),
        (w, 0) => format!("{w} Workflow"),
        (0, d) => format!("{d} Dag"),
        (w, d) => format!("{w} Workflow, {d} Dag"),
    }
}

#[allow(clippy::too_many_arguments)]
fn render_schedules_page(
    rows: &[(ShardId, HarvestSchedule)],
    shard_errors: &[(ShardId, String)],
    is_multi_shard: bool,
    filters: &ScheduleUiFilters,
    filter_raw: &ScheduleUiFilterRaw,
    decisions: &std::collections::HashMap<uuid::Uuid, Vec<ScheduleDecision>>,
    page: i64,
    limit: i64,
    has_next: bool,
    total_filtered: usize,
    // Unhealthy counts over the whole *filtered* set, not just this page: the
    // strip is the first thing an operator reads, so a page-scoped count would
    // understate a fleet-wide problem.
    unhealthy_summary: &str,
    distribution: &str,
    refresh: Option<u64>,
    flash: Option<&str>,
) -> Markup {
    // The "show only unhealthy" link forces `health=Unhealthy`, so it clears
    // any stale health error the same way it clears the parsed override.
    let unhealthy_link_raw = ScheduleUiFilterRaw {
        health: String::new(),
        health_error: None,
        ..filter_raw.clone()
    };
    let body = html! {
        h2 { "Schedules" }

        @if let Some(message) = flash {
            div.flash role="status" tabindex="-1" autofocus { (message) }
        }

        @if !unhealthy_summary.is_empty() {
            div.card.unhealthy-summary role="status" {
                strong { "Needs attention: " }
                (unhealthy_summary)
                " — "
                a href={ "schedules?health=Unhealthy" (PreEscaped(&build_schedule_query_string(limit, &ScheduleUiFilters { health: ScheduleHealthFilter::All, ..filters.clone() }, &unhealthy_link_raw, refresh))) } {
                    "show only unhealthy"
                }
            }
        }

        (render_schedule_filters(filters, filter_raw, limit, refresh))
        (render_schedule_bulk_actions(filters, filter_raw, limit, refresh, total_filtered, distribution))

        @if rows.is_empty() && shard_errors.is_empty() {
            div.card.empty {
                @if filters.is_empty() {
                    "No schedules registered."
                } @else {
                    "No schedules match this filter."
                }
            }
        } @else {
            @for (shard_id, error) in shard_errors {
                div.shard-error {
                    @if is_multi_shard {
                        strong { "Shard " (shard_id.as_i32()) " unavailable: " }
                    } @else {
                        strong { "Shard unavailable: " }
                    }
                    (error)
                }
            }
            // 13 columns (14 multi-shard) overflow a narrow viewport; scroll the
            // table rather than the page.
            div."table-scroll" { (render_schedule_table(rows, is_multi_shard, decisions)) }
        }

        (render_schedule_pagination(page, limit, has_next, filters, filter_raw, refresh))
    };

    // Auto-refresh must keep the operator on the page they were reading,
    // with no `flash` carried forward — see `layout_schedules`'s own doc
    // comment. It keeps `page`, unlike `schedule_return_to_path`, which
    // deliberately excludes it (a one-time post-action redirect can land
    // back on page 0 without harm; a repeating reload cannot).
    let refresh_target = format!(
        "schedules?page={page}{}",
        build_schedule_query_string(limit, filters, filter_raw, refresh)
    );
    layout_schedules("Schedules · Vantage", &body, refresh, "", &refresh_target)
}

fn render_schedule_filters(
    filters: &ScheduleUiFilters,
    filter_raw: &ScheduleUiFilterRaw,
    limit: i64,
    refresh: Option<u64>,
) -> Markup {
    let target_val = filters.target.as_deref().unwrap_or("");
    let refresh_value = refresh.map(|s| s.to_string()).unwrap_or_default();

    html! {
        form.filters method="get" action="schedules" {
            label {
                "Target"
                input type="text" name="target" value=(target_val) placeholder="e.g. payment_workflow";
            }
            label {
                "Kind"
                select name="kind" {
                    option value="" selected[filter_raw.kind.is_empty() && filter_raw.kind_error.is_none()] { "All" }
                    option value="Workflow" selected[filters.kind == ScheduleKindFilter::Workflow] { "Workflow" }
                    option value="Dag" selected[filters.kind == ScheduleKindFilter::Dag] { "Dag" }
                    @if filter_raw.kind_error.is_some() {
                        option value=(filter_raw.kind) selected { (filter_raw.kind) }
                    }
                }
                @if let Some(error) = &filter_raw.kind_error {
                    span.field-error role="alert" { (error) }
                }
            }
            label {
                "Paused"
                select name="paused" {
                    option value="" selected[filter_raw.paused.is_empty() && filter_raw.paused_error.is_none()] { "All" }
                    option value="Paused" selected[filters.paused == SchedulePausedFilter::Paused] { "Paused" }
                    option value="Active" selected[filters.paused == SchedulePausedFilter::Active] { "Active" }
                    @if filter_raw.paused_error.is_some() {
                        option value=(filter_raw.paused) selected { (filter_raw.paused) }
                    }
                }
                @if let Some(error) = &filter_raw.paused_error {
                    span.field-error role="alert" { (error) }
                }
            }
            label {
                "Health"
                select name="health" {
                    option value="" selected[filter_raw.health.is_empty() && filter_raw.health_error.is_none()] { "All" }
                    option value="Unhealthy" selected[filters.health == ScheduleHealthFilter::Unhealthy] { "Unhealthy" }
                    option value="Healthy" selected[filters.health == ScheduleHealthFilter::Healthy] { "Healthy" }
                    @if filter_raw.health_error.is_some() {
                        option value=(filter_raw.health) selected { (filter_raw.health) }
                    }
                }
                @if let Some(error) = &filter_raw.health_error {
                    span.field-error role="alert" { (error) }
                }
            }
            label {
                "Shard"
                input type="text" inputmode="numeric" pattern="-?[0-9]*" name="shard_id" value=(filter_raw.shard_id) placeholder="e.g. 0";
                @if let Some(error) = &filter_raw.shard_id_error {
                    span.field-error role="alert" { (error) }
                }
            }
            label {
                "Per page"
                input type="number" name="limit" min="1" max=(MAX_PAGE_SIZE) value=(limit);
            }
            label {
                "Refresh"
                select name="refresh" {
                    option value="" selected[refresh.is_none()] { "Off" }
                    option value="30" selected[refresh == Some(30)] { "30s" }
                    option value="60" selected[refresh == Some(60)] { "60s" }
                    @if refresh.is_some_and(|secs| secs != 30 && secs != 60) {
                        option value=(refresh_value) selected { (refresh_value) "s" }
                    }
                }
            }
            button type="submit" { "Apply" }
            a.reset href="schedules" { "Reset" }
        }
    }
}

fn render_schedule_bulk_actions(
    filters: &ScheduleUiFilters,
    filter_raw: &ScheduleUiFilterRaw,
    limit: i64,
    refresh: Option<u64>,
    total_matching: usize,
    distribution: &str,
) -> Markup {
    let return_qs = build_schedule_query_string(limit, filters, filter_raw, refresh);
    let return_to = schedule_return_to_path(filters, filter_raw, limit, refresh);
    let dist_suffix = if distribution.is_empty() {
        String::new()
    } else {
        format!(" ({distribution})")
    };
    html! {
        div."bulk-actions" {
            form method="post" action="schedules/bulk-pause"
                onsubmit={ "return confirm('Pause " (total_matching) " matching schedule(s)" (&dist_suffix) "?')" } {
                (render_schedule_hidden_filters(filters))
                input type="hidden" name="return_to" value=(return_to);
                button type="submit" disabled[total_matching == 0] {
                    "Pause all matching (" (total_matching) ")"
                }
            }
            form method="post" action="schedules/bulk-resume"
                onsubmit={ "return confirm('Resume " (total_matching) " matching schedule(s)" (&dist_suffix) "?')" } {
                (render_schedule_hidden_filters(filters))
                input type="hidden" name="return_to" value=(return_to);
                button type="submit" disabled[total_matching == 0] {
                    "Resume all matching (" (total_matching) ")"
                }
            }
            @if !return_qs.is_empty() {
                span { "Filters active" }
            }
        }
    }
}

fn render_schedule_hidden_filters(filters: &ScheduleUiFilters) -> Markup {
    html! {
        @if let Some(ref target) = filters.target {
            input type="hidden" name="target" value=(target);
        }
        @if !matches!(filters.kind, ScheduleKindFilter::All) {
            input type="hidden" name="kind" value=(filters.kind.as_label());
        }
        @if !matches!(filters.paused, SchedulePausedFilter::All) {
            input type="hidden" name="paused" value=(filters.paused.as_label());
        }
        @if !matches!(filters.health, ScheduleHealthFilter::All) {
            input type="hidden" name="health" value=(filters.health.as_label());
        }
        @if let Some(shard_id) = filters.shard_id {
            input type="hidden" name="shard_id" value=(shard_id);
        }
    }
}

#[allow(clippy::too_many_lines)]
fn render_schedule_table(
    rows: &[(ShardId, HarvestSchedule)],
    is_multi_shard: bool,
    decisions: &std::collections::HashMap<uuid::Uuid, Vec<ScheduleDecision>>,
) -> Markup {
    html! {
        table {
            thead {
                tr {
                    th { "Schedule ID" }
                    th { "Kind" }
                    th { "Target" }
                    th { "Expression" }
                    th { "Timezone" }
                    th { "Next Run" }
                    th { "Last Run" }
                    th { "Health" }
                    th { "Overlap" }
                    th { "Catchup" }
                    th { "Bounded runs" }
                    th { "Created" }
                    @if is_multi_shard { th { "Shard" } }
                    th { "Actions" }
                }
            }
            tbody {
                @for (shard_id, row) in rows {
                    @let id_str = row.id.to_string();
                    @let kind_label = if row.dag_name.is_some() { "Dag" } else { "Workflow" };
                    @let target_name = row.workflow_name.as_deref()
                        .or(row.dag_name.as_deref())
                        .unwrap_or("—");
                    @let expr = row.schedule_expr.as_deref().unwrap_or("—");
                    @let health = schedule_health(row);
                    tr class=[(!health.is_healthy()).then_some("schedule-unhealthy")] {
                        td { code { (short_id(&id_str)) } }
                        td { (kind_label) }
                        td {
                            code { (target_name) }
                            @if let Some(s_decisions) = decisions.get(&row.id) {
                                @if !s_decisions.is_empty() {
                                    div style="margin-top: 6px" {
                                        details {
                                            summary style="font-size: 11px; color: #93c5fd; cursor: pointer" { "Recent Decisions (" (s_decisions.len()) ")" }
                                            div style="display: flex; flex-direction: column; gap: 4px; padding: 6px; background: #0f172a; border-radius: 4px; margin-top: 4px; font-size: 11px; max-width: 400px" {
                                                @for dec in s_decisions {
                                                    @let dec_badge_class = match dec.decision.as_str() {
                                                        "fired" => "badge COMPLETED",
                                                        "skipped" => "badge Active",
                                                        "suppressed_paused" => "badge CANCELLED",
                                                        "backfilled" => "badge RUNNING",
                                                        _ => "badge UNKNOWN",
                                                    };
                                                    div style="display: flex; align-items: center; justify-content: space-between; gap: 8px" {
                                                        span class=(dec_badge_class) style="font-size: 10px; padding: 1px 6px" { (dec.decision) }
                                                        span style="color: #cbd5e1; font-family: monospace" { (dec.reason_code) }
                                                        span style="color: #94a3b8" { (format_timestamp(Some(dec.occurred_at))) }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        td { code { (expr) } }
                        td {
                            @if row.timezone == "UTC" {
                                span class="timezone-utc" { "UTC" }
                            } @else {
                                span.badge.timezone { (row.timezone) }
                            }
                        }
                        td { (schedule_next_fire_cell(row)) }
                        td { (format_timestamp(row.last_run_at)) }
                        td {
                            @if health.is_healthy() {
                                (schedule_state_badge(false))
                            } @else {
                                div.health-badges { (render_schedule_health_badges(row)) }
                            }
                        }
                        td { code { (schedule_overlap_label(row)) } }
                        td { code { (schedule_catchup_label(row)) } }
                        td { (schedule_bounded_runs_label(row)) }
                        td { (format_timestamp(Some(row.created_at))) }
                        @if is_multi_shard { td { (shard_id.as_i32()) } }
                        td {
                            div.actions {
                                a.drilldown href={ (schedule_leaf_path(&id_str, "preview")) } {
                                    "Preview"
                                }
                                a.drilldown href={ (schedule_leaf_path(&id_str, "runs")) } {
                                    "Runs"
                                }
                                a.drilldown href={ (schedule_leaf_path(&id_str, "backfill")) } {
                                    "Backfill"
                                }
                                @if schedule_is_resumable(row) {
                                    form method="post"
                                        action={ "schedules/" (id_str) "/resume" }
                                        onsubmit="return confirm('Resume this schedule?')" {
                                        button type="submit" { "Resume" }
                                    }
                                } @else {
                                    form method="post"
                                        action={ "schedules/" (id_str) "/pause" }
                                        onsubmit="return confirm('Pause this schedule?')" {
                                        button type="submit" { "Pause" }
                                    }
                                }
                                form method="post"
                                    action={ "schedules/" (id_str) "/trigger-now" }
                                    onsubmit={
                                        @if row.is_paused {
                                            "return confirm('This schedule is paused. Force a manual run anyway?')"
                                        } @else {
                                            "return confirm('Trigger a one-off run of this schedule now?')"
                                        }
                                    } {
                                    button.secondary type="submit" { "Run now" }
                                }
                                form method="post"
                                    action={ "schedules/" (id_str) "/delete" }
                                    onsubmit={ "return confirm('Delete schedule " (id_str) "? This cannot be undone.')" } {
                                    button.danger type="submit" { "Delete" }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

// ── issue #951: schedule health, policy cells and drill-down links ───────────

/// Badges for a row's unhealthy states (issue #951 AC3).
///
/// Renders **nothing** for a healthy schedule — the calm-row requirement — so
/// the caller decides what a healthy row shows instead (the list shows the
/// existing "Active" state badge).
fn render_schedule_health_badges(row: &HarvestSchedule) -> Markup {
    let health = schedule_health(row);
    html! {
        @if health.auto_paused {
            span.badge.FAILED role="status"
                aria-label="Health: auto-paused after consecutive failures" {
                "Auto-paused"
            }
        } @else if health.paused {
            span.badge.CANCELLED role="status" aria-label="Health: paused" { "Paused" }
        }
        @if health.exhausted {
            // No `aria-label`: it would *replace* the accessible name, and the
            // visible text already carries the reason. `state_badge`'s label is
            // a superset of its text; here it would be a subset.
            span.badge.TERMINATED role="status" {
                "Exhausted"
                @match row.exhausted_reason {
                    Some(ref reason) => { ": " (reason) }
                    // Bounded out on the live fields, before a tick stamped
                    // `exhausted_at`/`exhausted_reason` — say which bound.
                    None => {
                        @if row.max_runs.is_some_and(|max| max > 0 && row.runs_started >= max) {
                            ": run budget spent"
                        } @else if row.end_at.is_some() {
                            ": past end_at"
                        }
                    }
                }
            }
        }
        @if health.catchup_dropped {
            span.badge.FAILED role="status" {
                "Catchup dropped ×" (row.last_catchup_dropped)
            }
        }
    }
}

/// The effective catchup policy (#484) plus its window and most-recent drop
/// count, as one compact cell.
fn schedule_catchup_label(row: &HarvestSchedule) -> String {
    let policy = autumn_harvest::policy::CatchupPolicy::from_db(
        row.catchup_policy.as_deref(),
        row.catchup_window_secs,
        row.catchup,
    );
    let mut out = policy.as_str().to_string();
    if matches!(policy, autumn_harvest::policy::CatchupPolicy::Window(_)) {
        let secs = row.catchup_window_secs.unwrap_or(0);
        let _ = write!(out, " ({secs}s)");
    }
    if row.last_catchup_dropped > 0 {
        let _ = write!(out, " · dropped {}", row.last_catchup_dropped);
    }
    out
}

/// The bounded-run state (#478): remaining budget, `end_at` cutoff, and the
/// machine-readable exhaustion reason once it has fired for the last time.
fn schedule_bounded_runs_label(row: &HarvestSchedule) -> String {
    let mut parts: Vec<String> = Vec::new();
    // `max_runs = 0` is the engine's "unlimited", not a spent budget — every
    // bound check guards on `max > 0`. Rendering it as "0 of 0 left" would tell
    // an operator a schedule that fires forever has stopped.
    if let Some(max) = row.max_runs.filter(|max| *max > 0) {
        let remaining = crate::api::remaining_runs_budget(max, row.runs_started);
        parts.push(format!("{remaining} of {max} left"));
    }
    if let Some(end_at) = row.end_at {
        parts.push(format!("ends {}", format_timestamp(Some(end_at))));
    }
    if let Some(ref reason) = row.exhausted_reason {
        parts.push(format!("exhausted: {reason}"));
    }
    if parts.is_empty() {
        "—".to_string()
    } else {
        parts.join(" · ")
    }
}

/// The overlap policy (#241), with the buffered depth (and cap) when the policy
/// is one that buffers.
fn schedule_overlap_label(row: &HarvestSchedule) -> String {
    let mut out = row.overlap_policy.clone();
    match row.overlap_policy.as_str() {
        "buffer_one" | "buffer_all" => {
            let depth =
                autumn_harvest::scheduler::parse_buffered_runs_pub(&row.buffered_runs).len();
            let _ = write!(out, " · buffered {depth}");
            if row.overlap_policy == "buffer_all" {
                let _ = write!(out, "/{}", row.buffer_all_max);
            }
        }
        _ => {}
    }
    out
}

/// The next-fire cell: `next_run_at`, plus the jitter-adjusted
/// `effective_fire_time` (#240) when — and only when — jitter is configured.
///
/// Delegates to `api::effective_fire_time` rather than recomputing the jitter
/// offset, so the page can never disagree with `GET /admin/schedules`.
fn schedule_next_fire_cell(row: &HarvestSchedule) -> Markup {
    let effective = crate::api::effective_fire_time(row.id, row.next_run_at, row.jitter_secs);
    html! {
        (format_timestamp(row.next_run_at))
        @if let Some(effective_at) = effective {
            div.subtle {
                "effective " (format_timestamp(Some(effective_at)))
                " (jitter ≤ " (row.jitter_secs) "s)"
            }
        }
    }
}

/// One-line count of the unhealthy schedules in the current result set, or the
/// empty string when everything is healthy (AC3: nothing to shout about).
fn schedule_health_summary(rows: &[(ShardId, HarvestSchedule)]) -> String {
    let (mut paused, mut exhausted, mut dropped) = (0usize, 0usize, 0usize);
    for (_, row) in rows {
        let health = schedule_health(row);
        if health.paused || health.auto_paused {
            paused += 1;
        }
        if health.exhausted {
            exhausted += 1;
        }
        if health.catchup_dropped {
            dropped += 1;
        }
    }
    let mut parts: Vec<String> = Vec::new();
    if paused > 0 {
        parts.push(format!("{paused} paused"));
    }
    if exhausted > 0 {
        parts.push(format!("{exhausted} exhausted"));
    }
    if dropped > 0 {
        parts.push(format!("{dropped} catchup-dropped"));
    }
    parts.join(" · ")
}

/// The schedule's display name (workflow or DAG target), or `—`.
fn schedule_target_name(row: &HarvestSchedule) -> &str {
    row.workflow_name
        .as_deref()
        .or(row.dag_name.as_deref())
        .unwrap_or("—")
}

/// Relative prefix reaching the UI mount point from a `/schedules/{id}/{leaf}`
/// drill-down.
///
/// Vantage mounts under a caller-configured prefix, so every link is relative.
/// A drill-down is served at `<mount>/schedules/{id}/{leaf}`, whose RFC 3986
/// base directory is `<mount>/schedules/{id}/` — two segments below the mount
/// point. Every link *out* of a drill-down (nav chrome included) must carry
/// this prefix; a link that forgets it silently resolves under the schedule id
/// and 404s.
const SCHEDULE_DRILLDOWN_BASE: &str = "../../";

/// Href from a drill-down page to another `schedules/{id}/{leaf}` page.
fn schedule_drilldown_href(id: &str, leaf: &str) -> String {
    format!("{SCHEDULE_DRILLDOWN_BASE}{}", schedule_leaf_path(id, leaf))
}

/// The `schedules/{id}/{leaf}` path fragment. Correct as-is from the list page
/// (base `<mount>/`); prefix it with [`SCHEDULE_DRILLDOWN_BASE`] from a
/// drill-down.
fn schedule_leaf_path(id: &str, leaf: &str) -> String {
    format!("schedules/{id}/{leaf}")
}

fn schedule_state_badge(is_paused: bool) -> Markup {
    if is_paused {
        html! { span.badge.CANCELLED { "Paused" } }
    } else {
        html! { span.badge.Active { "Active" } }
    }
}

fn render_schedule_pagination(
    page: i64,
    limit: i64,
    has_next: bool,
    filters: &ScheduleUiFilters,
    filter_raw: &ScheduleUiFilterRaw,
    refresh: Option<u64>,
) -> Markup {
    let base = build_schedule_query_string(limit, filters, filter_raw, refresh);
    html! {
        div.pagination {
            @if page > 0 {
                a href={ "schedules?page=" (page - 1) (PreEscaped(&base)) } {
                    (PreEscaped("&larr;")) " Previous"
                }
            } @else {
                span.disabled { (PreEscaped("&larr;")) " Previous" }
            }
            span { "Page " (page + 1) }
            @if has_next {
                a href={ "schedules?page=" (page + 1) (PreEscaped(&base)) } {
                    "Next " (PreEscaped("&rarr;"))
                }
            } @else {
                span.disabled { "Next " (PreEscaped("&rarr;")) }
            }
        }
    }
}

fn build_schedule_query_string(
    limit: i64,
    filters: &ScheduleUiFilters,
    filter_raw: &ScheduleUiFilterRaw,
    refresh: Option<u64>,
) -> String {
    let mut out = String::new();
    if limit != DEFAULT_SCHEDULE_PAGE_SIZE {
        let _ = write!(out, "&limit={limit}");
    }
    if let Some(ref target) = filters.target {
        let _ = write!(out, "&target={}", url_encode(target));
    }
    // Carry the raw text, not the parsed value. This lets an invalid
    // value's inline error persist across pagination, instead of being
    // silently dropped. Same reasoning as `build_dead_letter_query_string`'s
    // task_kind/failed_after/failed_before handling on the DLQ page (Codex
    // review, #1378 P2, #1420).
    if !filter_raw.kind.is_empty() {
        let _ = write!(out, "&kind={}", url_encode(&filter_raw.kind));
    }
    if !filter_raw.paused.is_empty() {
        let _ = write!(out, "&paused={}", url_encode(&filter_raw.paused));
    }
    if !filter_raw.health.is_empty() {
        let _ = write!(out, "&health={}", url_encode(&filter_raw.health));
    }
    if !filter_raw.shard_id.is_empty() {
        let _ = write!(out, "&shard_id={}", url_encode(&filter_raw.shard_id));
    }
    if let Some(secs) = refresh {
        let _ = write!(out, "&refresh={secs}");
    }
    out
}

// ── issue #951: schedule drill-downs — preview, run history, backfill ────────
//
// Every one of these is a *presentation* slice over an already-shipped,
// already-audited API:
//
//   * preview      → `api::compute_schedule_preview_for` (`GET /admin/schedules/{id}/preview`, #348/#543)
//   * run history  → `api::load_schedule_runs`        (`GET /admin/schedules/{id}/runs`, #534/#762)
//   * backfill     → `api::schedule_backfill`         (`POST /admin/schedules/{id}/backfill`, #337)
//
// They call those functions directly rather than reimplementing them, so the
// page cannot drift from the API on bounded-run truncation, cross-shard
// partial results, or the audit trail. No new endpoint, no new event variant,
// no migration.

/// Query parameters for the preview drill-down.
#[derive(Debug, Deserialize)]
pub(crate) struct SchedulePreviewUiParams {
    /// Number of fire times to project. Clamped to 1..=100 by the API.
    #[serde(default)]
    count: Option<usize>,
}

/// Query parameters for the run-history drill-down.
#[derive(Debug, Deserialize)]
pub(crate) struct ScheduleRunsUiParams {
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    origin: Option<String>,
    #[serde(default)]
    state: Option<String>,
    /// Flash message carried over from a committed backfill's redirect.
    #[serde(default)]
    flash: Option<String>,
}

/// The run-history view's own filter state, kept so the page can re-emit it on
/// its "Next" link — a keyset cursor is only meaningful under the filters it
/// was computed with, so dropping them would silently page into different data.
#[derive(Debug, Clone, Default)]
struct ScheduleRunsView {
    limit: Option<i64>,
    origin: Option<String>,
    state: Option<String>,
}

impl ScheduleRunsView {
    /// Query-string suffix (leading `&`) carrying the filters, for the next-page link.
    fn query_suffix(&self) -> String {
        let mut out = String::new();
        if let Some(limit) = self.limit {
            let _ = write!(out, "&limit={limit}");
        }
        if let Some(ref origin) = self.origin {
            let _ = write!(out, "&origin={}", url_encode(origin));
        }
        if let Some(ref state) = self.state {
            let _ = write!(out, "&state={}", url_encode(state));
        }
        out
    }
}

/// Resolve the schedule row for a drill-down page.
///
/// Delegates to the API's own `resolve_schedule_with_shard` rather than the
/// list page's `find_schedule_row`, so the three outcomes stay distinct and
/// match the endpoints these pages render:
///
/// * an unparseable id is a `400`;
/// * "checked every expected shard, no such row" is a `404`;
/// * "a shard could not be checked, so existence is indeterminate" is a `503`.
///
/// `find_schedule_row` collapses the last two into "not found", which during a
/// shard outage would tell an operator that a schedule they can see on the list
/// page has been deleted — the precise false negative the API's `503` exists to
/// prevent.
async fn load_schedule_for_drilldown(
    api_state: &HarvestApiState,
    id_str: &str,
) -> Result<(HarvestSchedule, ShardId), AutumnError> {
    let id = uuid::Uuid::parse_str(id_str.trim()).map_err(|_| {
        AutumnError::bad_request_msg(format!("invalid schedule id '{id_str}'; expected a UUID"))
    })?;
    crate::api::resolve_schedule_with_shard(api_state, id).await
}

/// `GET /schedules/{id}/preview` — the next N fire times for one schedule
/// (issue #951 AC5, over the #348 preview endpoint).
///
/// Read-only and ungated, matching `GET /admin/schedules/{id}/preview`.
async fn schedule_preview_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id_str): Path<String>,
    Query(params): Query<SchedulePreviewUiParams>,
) -> Result<Markup, AutumnError> {
    let (row, shard_id) = load_schedule_for_drilldown(&api_state, &id_str).await?;
    let count = params
        .count
        .unwrap_or(SCHEDULE_PREVIEW_DEFAULT_COUNT)
        .clamp(1, 100);
    // Pass the row through rather than the id: `compute_schedule_preview`'s own
    // lookup stops at the first unreachable shard, which would fail a preview
    // for a schedule the resilient resolver above already found on a later one.
    let preview = crate::api::compute_schedule_preview_for(
        &api_state,
        row.clone(),
        count,
        chrono::Utc::now(),
    )
    .await?;
    Ok(render_schedule_preview_page(
        &row, shard_id, &preview, count,
    ))
}

/// Default number of projected fire times on the preview drill-down.
const SCHEDULE_PREVIEW_DEFAULT_COUNT: usize = 10;

#[allow(clippy::too_many_lines)]
fn render_schedule_preview_page(
    row: &HarvestSchedule,
    shard_id: ShardId,
    preview: &crate::api::SchedulePreview,
    count: usize,
) -> Markup {
    let id_str = row.id.to_string();
    let body = html! {
        h2 { "Fire-time preview — " code { (schedule_target_name(row)) } }
        (render_schedule_drilldown_header(row, shard_id, "preview"))

        @if preview.is_paused || row.auto_paused_at.is_some() {
            div.degraded-banner role="status" tabindex="-1" autofocus {
                @if row.auto_paused_at.is_some() && !preview.is_paused {
                    strong { "Schedule is auto-paused. " }
                    "The scheduler excludes auto-paused schedules from firing (#360), "
                    "so no fire times are projected until it is resumed."
                } @else {
                    strong { "Schedule is paused. " }
                    "No fire times are projected while a schedule is paused."
                }
                @if let Some(ref reason) = preview.pause_reason {
                    " Reason: " (reason)
                }
            }
        }
        @if let Some(ref reason) = preview.exhausted_reason {
            div.degraded-banner role="status" tabindex="-1" autofocus {
                strong { "Schedule is exhausted. " }
                "It will never fire again (" (reason) ")."
            }
        }
        @if preview.entries.is_empty() {
            div.card.empty {
                "No upcoming fire times."
                @if preview.remaining_runs == Some(0) {
                    " The run budget is spent."
                } @else if row.auto_paused_at.is_some() {
                    " The schedule is auto-paused."
                } @else if let Some(end_at) = preview.end_at {
                    " The window ends " (format_timestamp(Some(end_at))) "."
                } @else if !preview.is_paused && preview.exhausted_reason.is_none() {
                    " The schedule expression produces no future firings "
                    "(a manual-only or unparseable expression)."
                }
            }
        } @else {
            div.card {
                dl.kv {
                    dt { "Projected from" } dd { (format_timestamp(Some(preview.from))) }
                    dt { "Entries requested" } dd { (count) }
                    @if let Some(remaining) = preview.remaining_runs {
                        dt { "Remaining run budget" } dd { (remaining) }
                    }
                    @if let Some(end_at) = preview.end_at {
                        dt { "Ends at" } dd { (format_timestamp(Some(end_at))) }
                    }
                }
            }
            table {
                thead {
                    tr {
                        th { "#" }
                        th { "Scheduled (UTC)" }
                        th { "Local (" (row.timezone) ")" }
                        th { "Effective (UTC)" }
                        th { "Reason" }
                        th { "Jitter window" }
                        th { "Overlap risk" }
                    }
                }
                tbody {
                    @for (idx, entry) in preview.entries.iter().enumerate() {
                        tr {
                            td { (idx + 1) }
                            td { (format_timestamp(Some(entry.scheduled_at))) }
                            td { code { (entry.local_at) } }
                            td {
                                @match entry.effective_at {
                                    Some(effective_at) => {
                                        (format_timestamp(Some(effective_at)))
                                    }
                                    None => {
                                        span.badge.CANCELLED role="status"
                                            aria-label="Firing suppressed" { "suppressed" }
                                    }
                                }
                            }
                            td { code { (entry.reason) } }
                            td {
                                @match (entry.jitter_earliest_at, entry.jitter_latest_at) {
                                    (Some(earliest), Some(latest)) => {
                                        (format_timestamp(Some(earliest)))
                                        " → "
                                        (format_timestamp(Some(latest)))
                                    }
                                    _ => { "—" }
                                }
                            }
                            td {
                                @if entry.would_skip_if_active {
                                    span.badge.FAILED role="status"
                                        aria-label="May be skipped by the overlap policy" {
                                        "may be skipped"
                                    }
                                } @else {
                                    "—"
                                }
                            }
                        }
                    }
                }
            }
            p.note {
                "\"Overlap risk\" is advisory: the preview is stateless and cannot know how "
                "many runs will be active when the slot arrives."
            }
        }

        div.actions {
            a.drilldown href=(schedule_drilldown_href(&id_str, "runs")) { "Run history" }
            a.drilldown href=(schedule_drilldown_href(&id_str, "backfill")) { "Backfill" }
        }
    };
    layout_schedules(
        "Schedule preview · Vantage",
        &body,
        None,
        SCHEDULE_DRILLDOWN_BASE,
        "",
    )
}

/// `GET /schedules/{id}/runs` — per-schedule run history (issue #951 AC7, over
/// the #534/#762 runs endpoint).
///
/// Admin-gated to match `GET /admin/schedules/{id}/runs`, which is the only
/// schedule read route the API gates.
async fn schedule_runs_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id_str): Path<String>,
    Query(params): Query<ScheduleRunsUiParams>,
) -> Result<Markup, AutumnError> {
    let (row, shard_id) = load_schedule_for_drilldown(&api_state, &id_str).await?;

    // Build the query through the endpoint's own parser so the UI applies the
    // same clamping, vocabulary validation and cursor format as the API.
    let mut pairs: Vec<(String, String)> = Vec::new();
    if let Some(limit) = params.limit {
        pairs.push(("limit".to_string(), limit.to_string()));
    }
    if let Some(ref cursor) = params.cursor {
        pairs.push(("cursor".to_string(), cursor.clone()));
    }
    if let Some(ref origin) = params.origin
        && !origin.trim().is_empty()
    {
        pairs.push(("origin".to_string(), origin.clone()));
    }
    if let Some(ref state) = params.state
        && !state.trim().is_empty()
    {
        pairs.push(("state".to_string(), state.clone()));
    }
    let runs_params =
        crate::schedule_runs::ScheduleRunsParams::from_query_pairs(&pairs, chrono::Utc::now())
            .map_err(AutumnError::bad_request_msg)?;

    let view = ScheduleRunsView {
        limit: params.limit,
        origin: params
            .origin
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string),
        state: params
            .state
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string),
    };

    let response = crate::api::load_schedule_runs(&api_state, row.id, runs_params).await?;
    Ok(render_schedule_runs_page(
        &row,
        shard_id,
        &response,
        &view,
        params.flash.as_deref(),
    ))
}

#[allow(clippy::too_many_lines)]
fn render_schedule_runs_page(
    row: &HarvestSchedule,
    shard_id: ShardId,
    response: &crate::schedule_runs::ScheduleRunsResponse,
    view: &ScheduleRunsView,
    flash: Option<&str>,
) -> Markup {
    use crate::shard_fanout::FanoutStatus;

    let id_str = row.id.to_string();
    let unavailable: Vec<&crate::schedule_runs::RunsShardInspection> = response
        .shards
        .iter()
        .filter(|s| s.status != "inspected")
        .collect();
    let any_shard_inspected = !matches!(response.status, FanoutStatus::Unavailable);

    let body = html! {
        h2 { "Run history — " code { (schedule_target_name(row)) } }

        // A committed backfill redirects here with its dispatch counts; the
        // "failed" count is the only place the operator is told about a partial
        // dispatch, so the message must not be dropped.
        @if let Some(message) = flash {
            div.flash role="status" tabindex="-1" autofocus { (message) }
        }

        (render_schedule_drilldown_header(row, shard_id, "runs"))
        (render_schedule_runs_filters(&id_str, view))

        // AC7: a partial cross-shard answer is always visible, never silently
        // truncated data.
        @match response.status {
            FanoutStatus::Partial => {
                div.degraded-banner role="status" tabindex="-1" autofocus {
                    strong { "Some shards unreachable. " }
                    "This history and its summary cover only the shards that answered; "
                    "counts may be understated."
                    (render_unavailable_shard_list(&unavailable))
                }
            }
            FanoutStatus::Unavailable => {
                div.degraded-banner role="status" tabindex="-1" autofocus {
                    strong { "No shard could be reached. " }
                    "No run history could be read, so this page shows nothing rather "
                    "than an empty history — retry once shards recover."
                    (render_unavailable_shard_list(&unavailable))
                }
            }
            FanoutStatus::Complete => {}
        }

        div.card {
            h3 { "Scheduled-run summary" }
            p.note {
                "Counts " strong { "scheduled-origin" } " runs only, so a backfill storm "
                "or an ad-hoc trigger never inflates the failure ratio."
            }
            dl.kv {
                dt { "Succeeded" } dd { (response.summary.succeeded) }
                dt { "Failed" } dd { (response.summary.failed) }
                dt { "Timed out" } dd { (response.summary.timed_out) }
                dt { "Cancelled" } dd { (response.summary.cancelled) }
                dt { "Terminated" } dd { (response.summary.terminated) }
                dt { "Running" } dd { (response.summary.running) }
                dt { "Total" } dd { (response.summary.total) }
                dt { "Next run" } dd { (format_timestamp(response.next_run_at)) }
            }
            @if !response.summary.summary_complete {
                p.note { "Some shards were unavailable, so these counts may be understated." }
            }
        }

        @if response.runs.is_empty() {
            @if any_shard_inspected {
                div.card.empty {
                    "No runs yet. This schedule has not started an execution "
                    "in the queried window."
                }
            }
        } @else {
            table {
                thead {
                    tr {
                        th { "Nominal fire time" }
                        th { "Started" }
                        th { "Completed" }
                        th { "State" }
                        th { "Origin" }
                        th { "Error" }
                        th { "Execution" }
                    }
                }
                tbody {
                    @for run in &response.runs {
                        @let exec_id = run.execution_id.to_string();
                        tr {
                            td {
                                @match run.nominal_fire_time {
                                    Some(fire_time) => { (format_timestamp(Some(fire_time))) }
                                    // A manual trigger has no logical slot (#534).
                                    None => { span.subtle { "— (no slot)" } }
                                }
                            }
                            td { (format_timestamp(Some(run.started_at))) }
                            td { (format_timestamp(run.completed_at)) }
                            td { (state_badge(&run.state)) }
                            td { code { (run.origin.as_deref().unwrap_or("—")) } }
                            td {
                                @match run.error {
                                    Some(ref error) => { span.subtle { (error) } }
                                    None => { "—" }
                                }
                            }
                            td {
                                a href={ "../../workflows/" (exec_id) } {
                                    code { (short_id(&exec_id)) }
                                }
                            }
                        }
                    }
                }
            }
            div.pagination {
                span { "Showing " (response.runs.len()) " of at most " (response.limit) }
                @if let Some(ref cursor) = response.next_cursor {
                    // `schedule_drilldown_href`, not the bare leaf path: this
                    // page is itself a drill-down, so the link needs the
                    // `../../` prefix. The filters ride along because the
                    // cursor is only meaningful under them.
                    a href={ (schedule_drilldown_href(&id_str, "runs"))
                             "?cursor=" (url_encode(cursor))
                             (PreEscaped(&view.query_suffix())) } {
                        "Next " (PreEscaped("&rarr;"))
                    }
                } @else {
                    span.disabled { "Next " (PreEscaped("&rarr;")) }
                }
            }
        }

        div.actions {
            a.drilldown href=(schedule_drilldown_href(&id_str, "preview")) { "Fire-time preview" }
            a.drilldown href=(schedule_drilldown_href(&id_str, "backfill")) { "Backfill" }
        }
    };
    layout_schedules(
        "Schedule runs · Vantage",
        &body,
        None,
        SCHEDULE_DRILLDOWN_BASE,
        "",
    )
}

/// Filter/limit controls for the run history. The endpoint has always accepted
/// `limit`/`origin`/`state`; without a form they were reachable only by editing
/// the URL by hand.
fn render_schedule_runs_filters(id_str: &str, view: &ScheduleRunsView) -> Markup {
    let limit_val = view.limit.map(|l| l.to_string()).unwrap_or_default();
    let origin_val = view.origin.as_deref().unwrap_or("");
    let state_val = view.state.as_deref().unwrap_or("");
    html! {
        form.filters method="get" action=(schedule_drilldown_href(id_str, "runs")) {
            label {
                "Rows"
                input type="number" name="limit" min="1"
                    max=(crate::schedule_runs::MAX_LIMIT) value=(limit_val)
                    placeholder=(crate::schedule_runs::DEFAULT_LIMIT);
            }
            label {
                "Origin"
                select name="origin" {
                    option value="" selected[origin_val.is_empty()] { "All" }
                    @for origin in ["scheduled", "backfill", "manual_trigger"] {
                        option value=(origin) selected[origin_val == origin] { (origin) }
                    }
                }
            }
            label {
                "State"
                select name="state" {
                    option value="" selected[state_val.is_empty()] { "All" }
                    @for state in KNOWN_STATES {
                        option value=(state) selected[state_val == *state] { (state) }
                    }
                }
            }
            button type="submit" { "Apply" }
            a.reset href=(schedule_drilldown_href(id_str, "runs")) { "Reset" }
        }
    }
}

fn render_unavailable_shard_list(
    unavailable: &[&crate::schedule_runs::RunsShardInspection],
) -> Markup {
    html! {
        @if !unavailable.is_empty() {
            ul {
                @for shard in unavailable {
                    li {
                        "Shard " (shard.shard_id) ": "
                        (shard.error.as_deref().unwrap_or("unavailable"))
                    }
                }
            }
        }
    }
}

/// Identity strip shared by the three drill-down pages, so an operator always
/// knows which schedule they are looking at and can get back to the list.
fn render_schedule_drilldown_header(
    row: &HarvestSchedule,
    shard_id: ShardId,
    current: &str,
) -> Markup {
    let id_str = row.id.to_string();
    let health = schedule_health(row);
    html! {
        div.card {
            dl.kv {
                dt { "Schedule" } dd { code { (id_str) } }
                dt { "Kind" }
                dd { (if row.dag_name.is_some() { "Dag" } else { "Workflow" }) }
                dt { "Target" } dd { code { (schedule_target_name(row)) } }
                dt { "Expression" }
                dd { code { (row.schedule_expr.as_deref().unwrap_or("—")) } }
                dt { "Timezone" } dd { (row.timezone) }
                dt { "Shard" } dd { (shard_id.as_i32()) }
                dt { "Health" }
                dd {
                    @if health.is_healthy() {
                        (schedule_state_badge(false))
                    } @else {
                        div.health-badges { (render_schedule_health_badges(row)) }
                    }
                }
            }
            p.note {
                a href="../../schedules" { (PreEscaped("&larr;")) " All schedules" }
                " · viewing " (current)
            }
        }
    }
}

// ── Backfill launcher (issue #951 AC6) ──────────────────────────────────────

/// The backfill window an operator typed, normalised and re-checked.
///
/// Kept as strings so the confirmation step can round-trip the *exact* window
/// that was previewed into its hidden fields — the committed backfill is then
/// provably the one whose counts the operator was shown.
#[derive(Debug, Clone, PartialEq, Eq)]
struct BackfillFormParams {
    from: String,
    to: String,
    max_count: Option<usize>,
    include_paused: bool,
}

impl BackfillFormParams {
    /// Validate and normalise a submitted window.
    ///
    /// # Errors
    ///
    /// Returns a human-readable message for an unparseable instant or an
    /// inverted window, which the caller renders as a form error — never a 500.
    fn parse(
        from: &str,
        to: &str,
        max_count: Option<usize>,
        include_paused: bool,
    ) -> Result<Self, String> {
        let parse_one = |label: &str, raw: &str| {
            chrono::DateTime::parse_from_rfc3339(raw.trim())
                .map(|d| d.with_timezone(&chrono::Utc))
                .map_err(|_| {
                    format!(
                        "invalid {label} '{raw}': expected an RFC 3339 instant, \
                         e.g. 2026-08-01T00:00:00Z"
                    )
                })
        };
        let from_at = parse_one("start", from)?;
        let to_at = parse_one("end", to)?;
        if to_at < from_at {
            return Err("the backfill end must be at or after its start".to_string());
        }
        // `AutoSi`, not `Secs`: truncating to whole seconds would change the
        // *window*, not merely its spelling. An `interval:` backfill treats
        // `from` as its first slot, so a submitted `…00.900Z` normalised to
        // `…00Z` shifts every slot in the plan. `AutoSi` keeps a whole-second
        // window spelled `…:00Z` and preserves fractional digits when present.
        Ok(Self {
            from: from_at.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
            to: to_at.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
            max_count,
            include_paused,
        })
    }

    /// Build the request for the shared backfill implementation.
    ///
    /// `dry_run` is the API's own flag: `true` projects, `false` dispatches. It
    /// is deliberately *not* the same polarity as the UI's `commit` stage —
    /// `commit` must map to `dry_run: false` — so callers pass `!commit`.
    fn to_request(&self, dry_run: bool) -> Result<crate::api::ScheduleBackfillRequest, String> {
        let parse_one = |raw: &str| {
            chrono::DateTime::parse_from_rfc3339(raw)
                .map(|d| d.with_timezone(&chrono::Utc))
                .map_err(|_| "the backfill window is no longer parseable".to_string())
        };
        Ok(crate::api::ScheduleBackfillRequest {
            from: parse_one(&self.from)?,
            to: parse_one(&self.to)?,
            dry_run,
            include_paused: self.include_paused,
            max_count: self.max_count,
        })
    }
}

/// Submitted backfill form. `stage` decides what happens, and **defaults to the
/// dry run**: a POST that omits it can never dispatch work.
#[derive(Debug, Deserialize)]
pub(crate) struct ScheduleBackfillForm {
    #[serde(default)]
    from: String,
    #[serde(default)]
    to: String,
    #[serde(default)]
    max_count: Option<String>,
    #[serde(default)]
    include_paused: Option<String>,
    #[serde(default)]
    stage: Option<String>,
}

/// `GET /schedules/{id}/backfill` — the empty launcher form.
///
/// Purely a read: the dry run itself writes a `harvest_backfill_log` row and an
/// audit record, so it may not sit on a GET (issue #951 AC9, "read path stays
/// read-only and side-effect-free").
async fn schedule_backfill_form_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id_str): Path<String>,
) -> Result<Markup, AutumnError> {
    let (row, shard_id) = load_schedule_for_drilldown(&api_state, &id_str).await?;
    Ok(render_schedule_backfill_form(
        &row,
        shard_id,
        None,
        &BackfillFormEcho::default(),
    ))
}

/// `POST /schedules/{id}/backfill` — the two-stage backfill launcher.
///
/// `stage=commit` dispatches; anything else (including an absent `stage`) runs
/// the dry run and renders the preview-count confirmation. Both stages go
/// through `api::schedule_backfill`, so the audit record, the backfill log row
/// and every guard (paused, exhausted, `max_active_runs`, `max_runs`) are the
/// API's, not a second copy.
async fn schedule_backfill_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id_str): Path<String>,
    headers: axum::http::HeaderMap,
    Form(form): Form<ScheduleBackfillForm>,
) -> axum::response::Response {
    let (row, shard_id) = match load_schedule_for_drilldown(&api_state, &id_str).await {
        Ok(found) => found,
        Err(e) => return e.into_response(),
    };

    // Echoed back verbatim on every rejection below, so a typo in one field
    // does not cost the operator the whole window.
    let echo = BackfillFormEcho::from(&form);

    let max_count = match form
        .max_count
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        // Clamped: `max_count` *replaces* the endpoint's default planning
        // guard rather than being capped by it, so an unbounded value from a
        // form would let a wide window enumerate millions of timestamps.
        Some(raw) => match raw.parse::<usize>() {
            Ok(n) if n > 0 => Some(n.min(SCHEDULE_BACKFILL_MAX_SLOTS)),
            _ => {
                return render_schedule_backfill_form(
                    &row,
                    shard_id,
                    Some(&format!(
                        "invalid max count '{raw}': expected a positive integer"
                    )),
                    &echo,
                )
                .into_response();
            }
        },
        None => None,
    };
    let include_paused = form.include_paused.is_some();

    let params = match BackfillFormParams::parse(&form.from, &form.to, max_count, include_paused) {
        Ok(params) => params,
        Err(message) => {
            return render_schedule_backfill_form(&row, shard_id, Some(&message), &echo)
                .into_response();
        }
    };

    // Default to the dry run: only an explicit `stage=commit` dispatches.
    let commit = form.stage.as_deref() == Some("commit");

    // `dry_run` is the inverse of `commit`: the preview stage projects, the
    // commit stage dispatches.
    let request = match params.to_request(!commit) {
        Ok(request) => request,
        Err(message) => {
            return render_schedule_backfill_form(&row, shard_id, Some(&message), &echo)
                .into_response();
        }
    };

    // Attribute the audit record to the UI, exactly as the pause/resume actions do.
    let mut headers = headers;
    headers.insert(
        autumn_harvest::audit::HEADER_SOURCE,
        axum::http::HeaderValue::from_static(SOURCE_UI),
    );

    // Shares the endpoint's body (every guard, the backfill log row and the
    // audit record) but labels the audit with the UI's own route, so a
    // dashboard-initiated backfill is not recorded as an API call. Mirrors
    // `dag_retry_commit_ui`.
    let result = crate::api::schedule_backfill_inner(
        &api_state,
        &id_str,
        &headers,
        request,
        "POST /ui/schedules/{id}/backfill",
    )
    .await;

    match result {
        Ok(response) => {
            if commit {
                // AC6: land the operator on this schedule's run history so the
                // runs they just launched are one click from the confirmation.
                let flash = format!(
                    "Backfill dispatched {} of {} planned run(s); {} skipped, {} failed.",
                    response.dispatched, response.total, response.skipped, response.failed
                );
                axum::response::Redirect::to(&format!(
                    "../../{}?flash={}",
                    schedule_leaf_path(&id_str, "runs"),
                    url_encode(&flash)
                ))
                .into_response()
            } else {
                render_schedule_backfill_confirm(&row, shard_id, &response, &params).into_response()
            }
        }
        // A rejected backfill (paused, exhausted, window too large, unknown DAG)
        // comes back as the API's own message, rendered on the form rather than
        // as a bare error page.
        Err(e) => render_schedule_backfill_form(&row, shard_id, Some(&e.to_string()), &echo)
            .into_response(),
    }
}

/// Raw, unvalidated form values echoed back when a submission is rejected, so
/// the operator does not have to retype two RFC 3339 instants.
#[derive(Debug, Clone, Default)]
struct BackfillFormEcho {
    from: String,
    to: String,
    max_count: String,
    include_paused: bool,
}

impl From<&ScheduleBackfillForm> for BackfillFormEcho {
    fn from(form: &ScheduleBackfillForm) -> Self {
        Self {
            from: form.from.clone(),
            to: form.to.clone(),
            max_count: form.max_count.clone().unwrap_or_default(),
            include_paused: form.include_paused.is_some(),
        }
    }
}

fn render_schedule_backfill_form(
    row: &HarvestSchedule,
    shard_id: ShardId,
    error: Option<&str>,
    echo: &BackfillFormEcho,
) -> Markup {
    let id_str = row.id.to_string();
    let body = html! {
        h2 { "Backfill — " code { (schedule_target_name(row)) } }
        (render_schedule_drilldown_header(row, shard_id, "backfill"))

        @if let Some(message) = error {
            div.degraded-banner role="status" tabindex="-1" autofocus {
                strong { "Backfill not started. " } (message)
            }
        }

        div.card {
            p.note {
                "A backfill replays this schedule's missed slots over a window. "
                "Submitting runs a "
                strong { "dry run" }
                " first: nothing is dispatched until you confirm the planned count."
            }
            form method="post" action={ "../../" (schedule_leaf_path(&id_str, "backfill")) } {
                input type="hidden" name="stage" value="preview";
                div.filters {
                    label {
                        "Start (RFC 3339 UTC)"
                        input type="text" name="from" required value=(echo.from)
                            placeholder="2026-08-01T00:00:00Z";
                    }
                    label {
                        "End (RFC 3339 UTC)"
                        input type="text" name="to" required value=(echo.to)
                            placeholder="2026-08-02T00:00:00Z";
                    }
                    label {
                        "Max slots"
                        input type="number" name="max_count" min="1"
                            max=(SCHEDULE_BACKFILL_MAX_SLOTS) value=(echo.max_count)
                            placeholder=(SCHEDULE_BACKFILL_MAX_SLOTS);
                    }
                    label {
                        "Include paused"
                        input type="checkbox" name="include_paused" value="1"
                            checked[echo.include_paused];
                    }
                }
                div.actions { button type="submit" { "Preview backfill" } }
            }
        }

        div.actions {
            a.drilldown href=(schedule_drilldown_href(&id_str, "preview")) { "Fire-time preview" }
            a.drilldown href=(schedule_drilldown_href(&id_str, "runs")) { "Run history" }
        }
    };
    layout_schedules(
        "Schedule backfill · Vantage",
        &body,
        None,
        SCHEDULE_DRILLDOWN_BASE,
        "",
    )
}

fn render_schedule_backfill_confirm(
    row: &HarvestSchedule,
    shard_id: ShardId,
    dry_run: &crate::api::ScheduleBackfillResponse,
    form: &BackfillFormParams,
) -> Markup {
    let id_str = row.id.to_string();
    // `schedule_backfill` rejects a paused *DAG* schedule in non-dry-run mode
    // outright, so the dry run can report a healthy `dispatched` count for a
    // commit that can only ever 400. Say so instead of offering a button that
    // cannot succeed.
    let paused_dag = row.is_paused && row.dag_name.is_some();
    let nothing_to_do = dry_run.total == 0 || dry_run.dispatched == 0;
    let mut skip_reasons: Vec<(&String, &usize)> = dry_run.skipped_reasons.iter().collect();
    skip_reasons.sort_by(|a, b| a.0.cmp(b.0));

    let body = html! {
        h2 { "Confirm backfill — " code { (schedule_target_name(row)) } }
        (render_schedule_drilldown_header(row, shard_id, "backfill"))

        div.card {
            h3 { "Dry run" }
            p.note { "Nothing has been dispatched yet." }
            dl.kv {
                dt { "Window" }
                dd { code { (form.from) } " → " code { (form.to) } }
                dt { "Planned slots" } dd { (dry_run.total) }
                dt { "Would dispatch" } dd { (dry_run.dispatched) }
                dt { "Would skip" } dd { (dry_run.skipped) }
                @if let Some(max_count) = form.max_count {
                    dt { "Max slots" } dd { (max_count) }
                }
                dt { "Include paused" }
                dd { (if form.include_paused { "yes" } else { "no" }) }
            }
            @if !skip_reasons.is_empty() {
                h3 { "Skip reasons" }
                ul {
                    @for (reason, count) in &skip_reasons {
                        li { code { (reason) } ": " (count) }
                    }
                }
            }
            @if let Some(ref warning) = dry_run.paused_schedule_warning {
                div.degraded-banner role="status" tabindex="-1" autofocus { (warning) }
            }
            @if !dry_run.planned_timestamps.is_empty() {
                h3 { "Planned fire times" }
                ul {
                    @for ts in dry_run.planned_timestamps.iter().take(SCHEDULE_BACKFILL_PREVIEW_ROWS) {
                        li { (format_timestamp(Some(*ts))) }
                    }
                }
                @if dry_run.planned_timestamps.len() > SCHEDULE_BACKFILL_PREVIEW_ROWS {
                    p.note {
                        "… and " (dry_run.planned_timestamps.len() - SCHEDULE_BACKFILL_PREVIEW_ROWS)
                        " more."
                    }
                }
            }
        }

        @if paused_dag {
            div.card.empty {
                "This DAG schedule is paused, so a backfill cannot be dispatched: "
                "backfilled runs would sit QUEUED and never execute. "
                "Resume the schedule first, then preview again."
            }
        } @else if nothing_to_do {
            div.card.empty {
                "Nothing to backfill in this window — no slot would be dispatched. "
                "Widen the window, or clear whatever is skipping these slots, and preview again."
            }
        } @else {
            div.card {
                form method="post" action={ "../../" (schedule_leaf_path(&id_str, "backfill")) }
                    onsubmit={
                        "return confirm('Dispatch " (dry_run.dispatched)
                        " backfill run(s) for schedule " (js_escape(&id_str)) "?')"
                    } {
                    input type="hidden" name="stage" value="commit";
                    input type="hidden" name="from" value=(form.from);
                    input type="hidden" name="to" value=(form.to);
                    @if let Some(max_count) = form.max_count {
                        input type="hidden" name="max_count" value=(max_count);
                    }
                    @if form.include_paused {
                        input type="hidden" name="include_paused" value="1";
                    }
                    div.actions {
                        button type="submit" { "Dispatch " (dry_run.dispatched) " run(s)" }
                        a.drilldown href=(schedule_drilldown_href(&id_str, "backfill")) {
                            "Cancel"
                        }
                    }
                }
            }
        }
    };
    layout_schedules(
        "Confirm backfill · Vantage",
        &body,
        None,
        SCHEDULE_DRILLDOWN_BASE,
        "",
    )
}

/// How many planned fire times the confirmation lists before rolling up.
const SCHEDULE_BACKFILL_PREVIEW_ROWS: usize = 20;

/// Ceiling on the launcher's `max_count`.
///
/// The endpoint treats `max_count` as the *planning* limit — supplying one
/// replaces `DEFAULT_BACKFILL_MAX_COUNT` rather than being capped by it — so an
/// unbounded value from a browser form could enumerate millions of timestamps
/// in one request. The UI caps it at the endpoint's own default.
const SCHEDULE_BACKFILL_MAX_SLOTS: usize = autumn_harvest::scheduler::DEFAULT_BACKFILL_MAX_COUNT;

// ── Admission gates UI (issue #377) ──────────────────────────────────────────

async fn list_gates_ui(
    Extension(api_state): Extension<HarvestApiState>,
) -> Result<Markup, AutumnError> {
    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut conn = acquire_conn(pool.default_pool()).await?;

    let rows = autumn_harvest::admission_gate::db::list_gates(&mut conn)
        .await
        .map_err(map_error)?;

    Ok(render_gates_page(&rows))
}

/// `POST /admin/gates/{id}/lift` — Vantage UI lift action (redirects back to gates list).
async fn lift_gate_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id): Path<uuid::Uuid>,
) -> axum::response::Response {
    let Ok(pool) = api_state.storage_pool() else {
        return axum::response::Redirect::to("../../admin/gates").into_response();
    };
    let Ok(mut conn) = acquire_conn(pool.default_pool()).await else {
        return axum::response::Redirect::to("../../admin/gates").into_response();
    };

    let id_str = id.to_string();
    if let Ok(Some(_gate)) =
        autumn_harvest::admission_gate::db::lift_gate(&mut conn, id, "ui").await
    {
        if let Ok(fresh) = autumn_harvest::admission_gate::db::load_active_gates(&mut conn).await {
            api_state.gate_cache().refresh(fresh);
        }
        let ar = autumn_harvest::models::NewAuditRecord {
            actor: "ui",
            operation: OP_GATE_LIFT,
            target_type: TARGET_GATE,
            target_id: Some(id_str.as_str()),
            route_or_command: "POST /ui/admin/gates/{id}/lift",
            request_id: None,
            idempotency_key: None,
            status: STATUS_SUCCEEDED,
            error_summary: None,
            shard_id: None,
            source: SOURCE_UI,
        };
        let _ = insert_audit(&mut conn, &ar).await;
    }

    // Correct relative target from /ui/admin/gates/{id}/lift back to the
    // gates list at /ui/admin/gates.  "../../admin/gates" would resolve to
    // /ui/admin/admin/gates (one "admin" too many).
    axum::response::Redirect::to("../../gates").into_response()
}

fn render_gates_page(rows: &[autumn_harvest::models::AdmissionGateRow]) -> Markup {
    let body = html! {
        h2 { "Admission Gates" }
        p.note {
            "Active gates block new workflow starts. In-flight executions are unaffected. "
            "Use the "
            a href="../../admin/gates" { "management API" }
            " ("
            code { "POST /admin/gates" }
            ", "
            code { "DELETE /admin/gates/{id}" }
            ") to create or lift gates."
        }

        @if rows.iter().all(|r| r.lifted_at.is_some()) {
            div.card.empty { "No active admission gates." }
        }

        @for row in rows.iter().filter(|r| r.lifted_at.is_none()) {
            @let id_str = row.id.to_string();
            @let id_short = &id_str[..8];
            @let now = chrono::Utc::now();
            @let is_expired = row.expires_at.is_some_and(|exp| exp <= now);
            div.card {
                div style="display:flex;justify-content:space-between;align-items:center" {
                    div {
                        strong { code { (id_short) } }
                        " "
                        @if is_expired {
                            span.badge style="background:#374151;color:#e2e8f0" { "EXPIRED" }
                        } @else {
                            span.badge.FAILED { "ACTIVE" }
                        }
                        " "
                        span { (row.scope_kind) }
                        @if let Some(ref v) = row.scope_value {
                            " = "
                            code { (v) }
                        }
                    }
                    div style="display:flex;gap:16px;align-items:center" {
                        div style="font-size:12px;color:#94a3b8" {
                            "created by " (row.created_by)
                            " at " (row.created_at.format("%Y-%m-%d %H:%M:%S UTC"))
                            @if let Some(exp) = row.expires_at {
                                @if is_expired {
                                    " · expired " (exp.format("%Y-%m-%d %H:%M:%S UTC"))
                                } @else {
                                    " · expires " (exp.format("%Y-%m-%d %H:%M:%S UTC"))
                                }
                            }
                        }
                        @if !is_expired {
                            form method="POST" action={ "gates/" (id_str) "/lift" }
                                onsubmit="return confirm('Lift this gate?')" {
                                button type="submit"
                                    style="background:#15803d;color:#dcfce7;border:none;border-radius:4px;padding:4px 12px;cursor:pointer;font-size:12px" {
                                    "Lift"
                                }
                            }
                        }
                    }
                }
                @if !row.reason.is_empty() {
                    p style="margin:8px 0 0;color:#e2e8f0" { (row.reason) }
                }
                @if let Some(ref msg) = row.message {
                    p style="margin:4px 0 0;color:#94a3b8;font-size:12px" { (msg) }
                }
            }
        }

        @let lifted: Vec<_> = rows.iter().filter(|r| r.lifted_at.is_some()).collect();
        @if !lifted.is_empty() {
            details style="margin-top:20px" {
                summary style="cursor:pointer;color:#94a3b8" {
                    (lifted.len()) " lifted gate(s)"
                }
                @for row in &lifted {
                    @let id_short = &row.id.to_string()[..8];
                    div.card style="opacity:0.8" {
                        code { (id_short) }
                        " "
                        span.badge.COMPLETED { "LIFTED" }
                        " "
                        (row.scope_kind)
                        @if let Some(ref v) = row.scope_value {
                            " = " code { (v) }
                        }
                        " — " (row.reason)
                    }
                }
            }
        }
    };
    layout_gates("Admission Gates · Vantage", &body)
}

fn layout_gates(title: &str, body: &Markup) -> Markup {
    html! {
        (PreEscaped("<!DOCTYPE html>"))
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width,initial-scale=1";
                title { (title) }
                style { (PreEscaped(STYLE)) }
            }
            body {
                header {
                    h1 {
                        a href="../workflows" { "🔭 Vantage" }
                        span.subtitle { "Harvest dashboard" }
                    }
                    nav {
                        a href="../workflows" { "Workflows" }
                        a href="../workers" { "Workers" }
                        a href="../schedules" { "Schedules" }
                        a href="../dead-letters" { "Dead Letters" }
                        a href="../build-routing" { "Build Routing" }
                        a.active href="gates" { "Gates" }
                    }
                }
                main { (body) }
                footer { "Operational dashboard — autumn-harvest" }
            }
        }
    }
}

/// Chrome for the schedules pages.
///
/// `base_href` is the relative prefix that reaches the UI mount point from the
/// page being rendered, exactly as [`layout`] takes one: the list at
/// `/schedules` passes `""`, and the `/schedules/{id}/{leaf}` drill-downs pass
/// [`SCHEDULE_DRILLDOWN_BASE`] (`../../`). Without it every nav link on a
/// drill-down resolves relative to `/schedules/{id}/` and 404s.
/// `refresh_target` is the current filtered list view's URL, page
/// included, with no `flash` param. Only the list page passes a real
/// one. The drill-down pages never enable `refresh`, so an empty string
/// is fine there. The `@if let Some(secs)` guard below never renders the
/// tag in that case.
///
/// A bulk action redirects here with `flash` appended to `return_to`.
/// `return_to` itself preserves `refresh`. Without an explicit target,
/// an operator with auto-refresh on would see this page's meta refresh
/// reload that same flash-bearing URL on every interval. Each reload
/// would re-announce and re-focus a stale message. Same fix as
/// `layout_dead_letters` already applies, found in review there as PR
/// #1396. Codex review on #1437 P2: newly reachable once the bulk
/// actions started preserving `refresh` through `return_to`.
fn layout_schedules(
    title: &str,
    body: &Markup,
    refresh: Option<u64>,
    base_href: &str,
    refresh_target: &str,
) -> Markup {
    html! {
        (PreEscaped("<!DOCTYPE html>"))
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width,initial-scale=1";
                @if let Some(secs) = refresh {
                    meta http-equiv="refresh" content={ (secs) "; url=" (refresh_target) };
                }
                title { (title) }
                style { (PreEscaped(STYLE)) }
            }
            body {
                header {
                    h1 {
                        a href={ (base_href) "workflows" } { "🔭 Vantage" }
                        span.subtitle { "Harvest dashboard" }
                    }
                    nav {
                        a href={ (base_href) "workflows" } { "Workflows" }
                        a href={ (base_href) "workers" } { "Workers" }
                        a.active href={ (base_href) "schedules" } { "Schedules" }
                        a href={ (base_href) "dead-letters" } { "Dead Letters" }
                        a href={ (base_href) "build-routing" } { "Build Routing" }
                    }
                }
                main { (body) }
                footer { "Operational dashboard — autumn-harvest" }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// GREEN: a valid bound parses, and the raw display echoes the
    /// caller-supplied text (not a re-formatted RFC 3339 string) with no error.
    #[test]
    fn parse_started_bound_accepts_valid_rfc3339() {
        let (parsed, raw, error) =
            parse_started_bound(Some("2026-01-01T00:00:00Z"), "started_after");
        assert_eq!(raw, "2026-01-01T00:00:00Z");
        assert!(error.is_none());
        assert_eq!(
            parsed.map(|d| d.to_rfc3339()),
            Some("2026-01-01T00:00:00+00:00".to_string())
        );
    }

    /// GREEN — the fix under test: a malformed bound no longer aborts the
    /// caller. It degrades to "filter not applied" (`parsed` is `None`) while
    /// echoing the operator's exact raw input and a recovery message, so the
    /// caller can redisplay the field inline instead of discarding the page.
    /// Before this change, `list_workflows_ui` `?`-propagated a bare
    /// `AutumnError` here, which aborted the whole `/workflows` response
    /// before the filter form (or any other filter the operator had typed)
    /// was ever rendered — see the RED baseline in
    /// `tests/ui_integration.rs::ui_workflows_invalid_started_after_*`.
    #[test]
    fn parse_started_bound_rejects_invalid_value_without_erroring() {
        let (parsed, raw, error) = parse_started_bound(Some("yesterday"), "started_after");
        assert_eq!(
            parsed, None,
            "an invalid bound must not be applied to the query"
        );
        assert_eq!(
            raw, "yesterday",
            "the operator's exact raw input is echoed back"
        );
        let error = error.expect("an invalid bound must carry a redisplayable error");
        assert!(
            error.contains("started_after") && error.contains("RFC 3339"),
            "error names the field and the expected format: {error}"
        );
    }

    #[test]
    fn parse_started_bound_blank_or_missing_is_not_an_error() {
        assert_eq!(
            parse_started_bound(None, "started_after"),
            (None, String::new(), None)
        );
        assert_eq!(
            parse_started_bound(Some("   "), "started_after"),
            (None, String::new(), None)
        );
    }

    /// GREEN — the fix under test. `event_page`/`jump_event` used to be typed
    /// `i64` straight on `WorkflowDetailParams`, a `Query<..>` extractor
    /// struct. A non-numeric value failed axum's own query deserialization.
    /// That aborted the request with a bare framework 400 before
    /// `workflow_detail_ui` ran at all. No metadata, timeline, or panel
    /// rendered. A malformed value must instead fall back to page zero with
    /// a flash message.
    #[test]
    fn parse_event_page_query_field_accepts_valid_values() {
        assert_eq!(
            parse_event_page_query_field("jump_event", Some("1")),
            Ok(Some(1))
        );
        assert_eq!(
            parse_event_page_query_field("event_page", Some("  42  ")),
            Ok(Some(42))
        );
        assert_eq!(
            parse_event_page_query_field("event_page", Some("0")),
            Ok(Some(0))
        );
    }

    #[test]
    fn parse_event_page_query_field_rejects_non_numeric_text() {
        let err = parse_event_page_query_field("jump_event", Some("abc"))
            .expect_err("must reject non-numeric text");
        assert!(
            err.contains("jump_event") && err.contains("abc"),
            "the error must name the field and the bad value: {err}"
        );
    }

    #[test]
    fn parse_event_page_query_field_rejects_a_fraction() {
        // A `type="number"` input's `step="1"` default blocks this in a real
        // browser. A bare `Query` GET from any other client is still a
        // reachable path. It must not 400 before the handler runs.
        assert!(parse_event_page_query_field("jump_event", Some("1.5")).is_err());
    }

    #[test]
    fn parse_event_page_query_field_rejects_i64_overflow() {
        assert!(parse_event_page_query_field("event_page", Some("99999999999999999999")).is_err());
    }

    #[test]
    fn parse_event_page_query_field_blank_or_missing_is_not_an_error() {
        assert_eq!(parse_event_page_query_field("jump_event", None), Ok(None));
        assert_eq!(
            parse_event_page_query_field("jump_event", Some("")),
            Ok(None)
        );
        assert_eq!(
            parse_event_page_query_field("jump_event", Some("   ")),
            Ok(None)
        );
    }

    /// Issue #619: with nothing paused **and** every shard readable, the banner
    /// must render nothing at all, so a healthy Workers page is byte-identical to
    /// before the feature.
    #[test]
    fn paused_queues_banner_is_empty_when_nothing_is_paused() {
        assert_eq!(render_paused_queues_banner(&[], &[]).into_string(), "");
    }

    /// A pool that is never connected to; `.build()` is lazy, so constructing it
    /// performs no I/O and `acquire_conn` on it fails fast.
    fn never_connecting_pool() -> autumn_harvest::worker::DbPool {
        let manager = diesel_async::pooled_connection::AsyncDieselConnectionManager::<
            diesel_async::AsyncPgConnection,
        >::new("postgres://harvest:harvest@127.0.0.1:1/never");
        deadpool::managed::Pool::builder(manager)
            .max_size(1)
            .build()
            .expect("lazy pool builds without connecting")
    }

    /// Issue #619 review (round 11): mid a shard-add rollout the router
    /// advertises a shard this process has no pool for. Fanning out over the
    /// pools alone would give that shard no future at all, so it could never
    /// reach `unreadable_shards` — and if it is the only shard holding a queue,
    /// the Workers page renders no pause warning whatsoever, the exact
    /// invisible-hold bug the warning exists to prevent.
    ///
    /// This guards the *reporting* half (a `None` pool is reported, never
    /// dropped). Its counterpart — that a router-known poolless shard resolves
    /// to `None` rather than a default-fallback pool — is guarded on the API
    /// side by `resolve_expected_shard_pools_flags_poolless_shard_not_default_fallback`.
    #[tokio::test]
    async fn scan_reports_an_expected_shard_that_has_no_local_pool() {
        let live = never_connecting_pool();
        // Shard 0 has a pool (whose read will fail); shard 1 is the poolless,
        // router-known shard. Both must be reported.
        let scan = scan_paused_queues(vec![(0, Some(&live)), (1, None)], &[0, 1]).await;
        assert!(
            scan.unreadable_shards.contains(&1),
            "a poolless expected shard must be reported unreadable, not dropped: {:?}",
            scan.unreadable_shards
        );
        assert_eq!(
            scan.unreadable_shards,
            vec![0, 1],
            "both an unreadable pool and a missing pool are reported, sorted"
        );
        assert!(
            scan.rows.is_empty(),
            "no shard was read, so there are no rows to show"
        );
        // And the banner is therefore non-empty: silence here is the lie.
        assert!(
            !render_paused_queues_banner(&scan.rows, &scan.unreadable_shards)
                .into_string()
                .is_empty(),
            "an unread/poolless shard must never render as a clean page"
        );
    }

    /// The poolless shard is reported even when it is the ONLY expected shard —
    /// i.e. the fold does not depend on some other shard producing a future.
    #[tokio::test]
    async fn scan_reports_a_lone_poolless_shard() {
        let scan = scan_paused_queues(vec![(7, None)], &[7]).await;
        assert_eq!(scan.unreadable_shards, vec![7]);
    }

    /// Issue #619 review: a shard whose pause state could not be read makes the
    /// banner's silence a lie — a hold that exists ONLY on that shard is absent
    /// from `rows` entirely. The warning must therefore render even with zero
    /// rows, because that is exactly the case an operator misreads as "dispatch
    /// is flowing, look elsewhere".
    #[test]
    fn unreadable_shard_warns_even_with_no_visible_holds() {
        let html = render_paused_queues_banner(&[], &[1, 3]).into_string();
        assert!(
            !html.is_empty(),
            "an unread shard must never render as a clean page"
        );
        assert!(
            html.contains("Queue pause state incomplete"),
            "must name the condition: {html}"
        );
        assert!(html.contains("1, 3"), "must name the failed shards: {html}");
        assert!(
            html.contains("MISSING"),
            "must say a hold could be missing entirely: {html}"
        );
        assert!(
            html.contains("/admin/queues/paused"),
            "must point at the authoritative per-shard read: {html}"
        );
        // No hold is visible, so there is nothing to tabulate.
        assert!(
            !html.contains("Queue dispatch paused") && !html.contains("<table"),
            "with zero rows there must be no hold banner or table: {html}"
        );
    }

    /// The two warnings are independent: a visible hold PLUS an unread shard must
    /// show both, because the shown `Scope` can understate the real coverage.
    #[test]
    fn unreadable_shard_warning_composes_with_a_visible_hold() {
        let rows = vec![PausedQueueBannerRow {
            queue_name: "email-workers".to_string(),
            reason: "SMTP provider outage".to_string(),
            paused_by: "alice".to_string(),
            paused_at: chrono::Utc::now(),
            scope_shard_id: None,
            held_task_count: 7,
            provenance_uniform: true,
            coverage: autumn_harvest::queue_pause::PauseCoverage::PartialFleet,
        }];
        let html = render_paused_queues_banner(&rows, &[2]).into_string();
        assert!(
            html.contains("Queue pause state incomplete"),
            "read-failure warning: {html}"
        );
        assert!(
            html.contains("Queue dispatch paused"),
            "hold banner still rendered: {html}"
        );
        assert!(
            html.contains("email-workers"),
            "table still rendered: {html}"
        );
        // The incomplete-read warning must come FIRST: "we do not know" outranks
        // the partial-coverage detail it may itself explain.
        let read_at = html.find("Queue pause state incomplete").unwrap();
        let hold_at = html.find("Queue dispatch paused").unwrap();
        assert!(
            read_at < hold_at,
            "the unknown-state warning must precede the hold banner: {html}"
        );
    }

    /// AC6: the banner surfaces the queue name, held count, actor and reason so
    /// an operator can see WHO paused WHAT and WHY without leaving the page.
    #[test]
    fn paused_queues_banner_surfaces_reason_actor_and_held_count() {
        let rows = vec![PausedQueueBannerRow {
            queue_name: "email-workers".to_string(),
            reason: "SMTP provider outage".to_string(),
            paused_by: "alice".to_string(),
            paused_at: chrono::Utc::now(),
            scope_shard_id: None,
            held_task_count: 42,
            provenance_uniform: true,
            coverage: autumn_harvest::queue_pause::PauseCoverage::Fleet,
        }];
        let html = render_paused_queues_banner(&rows, &[]).into_string();
        assert!(html.contains("email-workers"), "queue name: {html}");
        assert!(html.contains("SMTP provider outage"), "reason: {html}");
        assert!(html.contains("alice"), "actor: {html}");
        assert!(html.contains("42"), "held count: {html}");
        assert!(html.contains("fleet-wide"), "scope: {html}");
        assert!(
            !html.contains("mixed across shards"),
            "a uniform hold must NOT be marked mixed: {html}"
        );
    }

    /// AC7: pause is fleet-wide by default, so the same queue paused on two
    /// shards is ONE banner row whose held count is the fleet-wide sum.
    #[test]
    fn paused_queue_rows_merge_across_shards_summing_held_counts() {
        let earlier = chrono::Utc::now() - chrono::Duration::seconds(600);
        let later = chrono::Utc::now();
        let merged = merge_paused_queue_banner_rows(
            vec![
                (
                    1,
                    autumn_harvest::queue_pause::PausedQueue {
                        queue_name: "email-workers".to_string(),
                        reason: "later shard".to_string(),
                        paused_by: "bob".to_string(),
                        paused_at: later,
                        scope_shard_id: Some(1),
                        held_task_count: 3,
                    },
                ),
                (
                    0,
                    autumn_harvest::queue_pause::PausedQueue {
                        queue_name: "email-workers".to_string(),
                        reason: "earliest pause wins".to_string(),
                        paused_by: "alice".to_string(),
                        paused_at: earlier,
                        scope_shard_id: Some(0),
                        held_task_count: 4,
                    },
                ),
            ],
            &[0, 1],
        );
        assert_eq!(merged.len(), 1, "one row per queue name");
        assert_eq!(
            merged[0].held_task_count, 7,
            "held counts sum across shards"
        );
        assert_eq!(
            merged[0].paused_at, earlier,
            "the earliest pause instant is the one an operator reasons about"
        );
        assert_eq!(
            merged[0].reason, "earliest pause wins",
            "provenance travels with the earliest pause, not a mix"
        );
        assert_eq!(merged[0].paused_by, "alice");
    }

    /// Issue #619 review: two shards holding the same queue with *different*
    /// reasons/actors is a supported case (`shard_id` scoping, plus the
    /// idempotent re-pause that preserves the original provenance). The banner
    /// sums both shards' held tasks, so labelling that fleet-wide total with
    /// only one shard's story would tell the operator something untrue about
    /// the rest of the fleet. It must be flagged instead.
    #[test]
    fn divergent_shard_provenance_is_flagged_and_visibly_marked() {
        let earlier = chrono::Utc::now() - chrono::Duration::seconds(600);
        let later = chrono::Utc::now();
        let merged = merge_paused_queue_banner_rows(
            vec![
                (
                    0,
                    autumn_harvest::queue_pause::PausedQueue {
                        queue_name: "payments".to_string(),
                        reason: "shard-0 provider degraded".to_string(),
                        paused_by: "alice".to_string(),
                        paused_at: earlier,
                        scope_shard_id: Some(0),
                        held_task_count: 4,
                    },
                ),
                (
                    1,
                    autumn_harvest::queue_pause::PausedQueue {
                        queue_name: "payments".to_string(),
                        reason: "shard-1 unrelated incident".to_string(),
                        paused_by: "bob".to_string(),
                        paused_at: later,
                        scope_shard_id: Some(1),
                        held_task_count: 9,
                    },
                ),
            ],
            &[0, 1],
        );
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].held_task_count, 13, "the total is fleet-wide");
        assert!(
            !merged[0].provenance_uniform,
            "the shards disagree on reason, actor AND scope"
        );

        let html = render_paused_queues_banner(&merged, &[]).into_string();
        assert!(
            html.contains("mixed across shards"),
            "divergent provenance must be visible, not silently attributed to \
             one shard: {html}"
        );
        assert!(
            html.contains("mixed provenance across shards"),
            "the banner summary must count the divergent queue(s): {html}"
        );
    }

    /// The uniformity rule must match `api::merge_paused_queue_rows` exactly,
    /// including comparing `(reason, paused_by, scope_shard_id)` and NOT
    /// `paused_at` — each shard stamps its own `NOW()`, so a fleet-wide hold
    /// whose shards agree on the story must never be flagged as mixed.
    #[test]
    fn differing_paused_at_alone_does_not_flag_divergence() {
        let merged = merge_paused_queue_banner_rows(
            vec![
                (
                    0,
                    autumn_harvest::queue_pause::PausedQueue {
                        queue_name: "payments".to_string(),
                        reason: "provider outage".to_string(),
                        paused_by: "alice".to_string(),
                        paused_at: chrono::Utc::now() - chrono::Duration::seconds(3),
                        scope_shard_id: None,
                        held_task_count: 2,
                    },
                ),
                (
                    1,
                    autumn_harvest::queue_pause::PausedQueue {
                        queue_name: "payments".to_string(),
                        reason: "provider outage".to_string(),
                        paused_by: "alice".to_string(),
                        paused_at: chrono::Utc::now(),
                        scope_shard_id: None,
                        held_task_count: 5,
                    },
                ),
            ],
            &[0, 1],
        );
        assert!(
            merged[0].provenance_uniform,
            "a fleet-wide hold's per-shard NOW() skew is not a divergence"
        );
        let html = render_paused_queues_banner(&merged, &[]).into_string();
        assert!(!html.contains("mixed across shards"), "{html}");
    }

    /// Issue #619 review: a fleet-wide pause that only reached SOME shards must
    /// not read as "fleet-wide".
    ///
    /// The shards it reached persist `scope_shard_id = NULL` and the ones it
    /// missed persist **no row at all**, so the stored intent is indistinguishable
    /// from a complete hold — while the missed shards keep dispatching into the
    /// very outage the operator is trying to hold back. Scope is therefore derived
    /// from real coverage against the expected shard set.
    #[test]
    fn a_partially_applied_fleet_pause_is_not_rendered_as_fleet_wide() {
        let row = |shard: i32| {
            (
                shard,
                autumn_harvest::queue_pause::PausedQueue {
                    queue_name: "payments".to_string(),
                    reason: "stripe outage".to_string(),
                    paused_by: "alice".to_string(),
                    paused_at: chrono::Utc::now(),
                    // Fleet-wide INTENT -- what a fleet-wide request writes.
                    scope_shard_id: None,
                    held_task_count: 3,
                },
            )
        };

        // Shard 1 was missed: it has no row and is still dispatching.
        let partial = merge_paused_queue_banner_rows(vec![row(0)], &[0, 1]);
        assert_eq!(
            partial[0].coverage,
            autumn_harvest::queue_pause::PauseCoverage::PartialFleet,
            "one of two expected shards holds the queue -- intent says \
             fleet-wide, reality does not"
        );
        let html = render_paused_queues_banner(&partial, &[]).into_string();
        assert!(
            html.contains("partially applied"),
            "the Scope cell must not claim fleet-wide coverage: {html}"
        );
        assert!(
            html.contains("still dispatching"),
            "the banner summary must tell the operator to re-issue the pause: \
             {html}"
        );

        // Both shards held: the same stored intent now really is fleet-wide.
        let complete = merge_paused_queue_banner_rows(vec![row(0), row(1)], &[0, 1]);
        assert_eq!(
            complete[0].coverage,
            autumn_harvest::queue_pause::PauseCoverage::Fleet
        );
        let html = render_paused_queues_banner(&complete, &[]).into_string();
        assert!(html.contains("fleet-wide"), "{html}");
        assert!(
            !html.contains("partially applied") && !html.contains("still dispatching"),
            "a complete hold must not be marked partial: {html}"
        );
    }

    /// Merged rows are sorted by queue name so the banner is stable across
    /// refreshes (shard iteration order must not shuffle it).
    #[test]
    fn paused_queue_rows_are_sorted_by_queue_name() {
        let now = chrono::Utc::now();
        let mk = |name: &str| autumn_harvest::queue_pause::PausedQueue {
            queue_name: name.to_string(),
            reason: "r".to_string(),
            paused_by: "a".to_string(),
            paused_at: now,
            scope_shard_id: None,
            held_task_count: 1,
        };
        let merged = merge_paused_queue_banner_rows(
            vec![(0, mk("zeta")), (0, mk("alpha")), (0, mk("mid"))],
            &[0],
        );
        let names: Vec<&str> = merged.iter().map(|r| r.queue_name.as_str()).collect();
        assert_eq!(names, vec!["alpha", "mid", "zeta"]);
    }

    #[test]
    fn url_encode_preserves_unreserved_and_encodes_space() {
        assert_eq!(url_encode("foo-bar.BAZ_1~"), "foo-bar.BAZ_1~");
        assert_eq!(url_encode("a b"), "a%20b");
        assert_eq!(url_encode("é"), "%C3%A9");
    }

    #[test]
    fn badge_class_buckets_known_and_unknown_states() {
        assert_eq!(badge_class("RUNNING"), "RUNNING");
        assert_eq!(badge_class("COMPLETED"), "COMPLETED");
        assert_eq!(badge_class("FAILED"), "FAILED");
        assert_eq!(badge_class("CANCELLED"), "CANCELLED");
        assert_eq!(badge_class("TERMINATED"), "TERMINATED");
        assert_eq!(badge_class("MYSTERY"), "UNKNOWN");
    }

    #[test]
    fn dead_letter_task_kind_label_recognizes_callback_rows() {
        // Issue #921 review: a completion-callback dead letter (issue #605)
        // used to render as "Unknown" instead of a recognized kind.
        assert_eq!(dead_letter_task_kind_label("CALLBACK"), "Callback");
        assert_eq!(dead_letter_task_kind_label("callback"), "Callback");
        assert_eq!(dead_letter_task_kind_label("ACTIVITY"), "Activity");
        assert_eq!(dead_letter_task_kind_label("WORKFLOW"), "Workflow");
        assert_eq!(dead_letter_task_kind_label("TIMER"), "Unknown");
    }

    #[test]
    fn pause_and_resume_flashes_distinguish_noops() {
        // Issue #609 post-review hardening: a no-op resume (the run was not
        // paused) must not flash "Workflow resumed", and an idempotent
        // repeat pause must not claim it performed the transition.
        assert_eq!(resume_flash_message(true), "Workflow resumed");
        assert_eq!(
            resume_flash_message(false),
            "Workflow was not paused; nothing to resume"
        );
        assert_eq!(pause_flash_message(true), "Workflow paused");
        assert_eq!(pause_flash_message(false), "Workflow was already paused");
    }

    #[test]
    fn event_label_disambiguates_terminate_from_cancel() {
        // The WorkflowCancelled event is reused for force-terminate (#504): the
        // timeline label must follow the authoritative execution state so a
        // terminated run reads "Workflow terminated", matching its badge.
        let data = serde_json::json!({});
        assert_eq!(
            event_human_label("WorkflowCancelled", &data, "TERMINATED"),
            "Workflow terminated"
        );
        assert_eq!(
            event_human_label("WorkflowCancelled", &data, "CANCELLED"),
            "Workflow cancelled"
        );
    }

    #[test]
    fn layout_escapes_title_but_keeps_body_markup() {
        let body = html! { p { "hello" } };
        let html = layout("<evil>", &body, "").into_string();
        assert!(html.contains("<title>&lt;evil&gt;</title>"));
        assert!(html.contains("<p>hello</p>"));
        assert!(html.contains("🔭 Vantage"));
    }

    #[test]
    fn build_query_string_omits_default_limit() {
        assert_eq!(
            build_query_string(DEFAULT_PAGE_SIZE, None, None, None, "", "", None),
            ""
        );
        assert_eq!(
            build_query_string(10, None, None, None, "", "", None),
            "&limit=10"
        );
        assert_eq!(
            build_query_string(DEFAULT_PAGE_SIZE, Some("FAILED"), None, None, "", "", None),
            "&state=FAILED"
        );
        assert_eq!(
            build_query_string(50, Some("with space"), None, None, "", "", None),
            "&limit=50&state=with%20space"
        );
    }

    #[test]
    fn build_query_string_includes_workflow_name_and_search_attrs() {
        assert_eq!(
            build_query_string(
                DEFAULT_PAGE_SIZE,
                None,
                Some("onboarding"),
                None,
                "",
                "",
                None
            ),
            "&workflow_name=onboarding"
        );
        let pair = ("tenant".to_string(), "acme".to_string());
        assert_eq!(
            build_query_string(DEFAULT_PAGE_SIZE, None, None, Some(&pair), "", "", None),
            "&search_attr_key=tenant&search_attr_value=acme"
        );
    }

    /// The Codex review finding on this PR: a Next/Previous link must not
    /// silently drop an invalid `started_after`/`started_before` an operator
    /// is still correcting — that would clear the value and its inline error
    /// (see `parse_started_bound`) via a click that looks unrelated to the
    /// filter form, one page after the operator typed it.
    #[test]
    fn build_query_string_preserves_invalid_date_text_for_pagination() {
        assert_eq!(
            build_query_string(DEFAULT_PAGE_SIZE, None, None, None, "yesterday", "", None),
            "&started_after=yesterday"
        );
        assert_eq!(
            build_query_string(DEFAULT_PAGE_SIZE, None, None, None, "", "not-a-date", None),
            "&started_before=not-a-date"
        );
    }

    /// `started_after_raw`/`started_before_raw` is the operator's exact typed
    /// text on success too (`parse_started_bound` returns `trimmed`, not a
    /// reformatted `DateTime`), so a link preserves e.g. the `Z` suffix as
    /// typed rather than normalizing it to `+00:00`.
    #[test]
    fn build_query_string_includes_valid_started_after_before() {
        assert_eq!(
            build_query_string(
                DEFAULT_PAGE_SIZE,
                None,
                None,
                None,
                "2026-01-01T00:00:00Z",
                "2026-12-31T23:59:59Z",
                None
            ),
            "&started_after=2026-01-01T00%3A00%3A00Z&started_before=2026-12-31T23%3A59%3A59Z"
        );
    }

    #[test]
    fn dead_letter_bulk_actions_submit_explicit_limit_for_matching_rows() {
        let filters = DeadLetterUiFilters {
            workflow_name: Some("invoice_workflow".to_string()),
            ..DeadLetterUiFilters::default()
        };

        let filter_raw = DeadLetterUiFilterRaw::default();
        let html = render_dead_letter_bulk_actions(
            &filters,
            &filter_raw,
            DEFAULT_DLQ_PAGE_SIZE,
            None,
            250,
        )
        .into_string();

        assert!(html.contains("name=\"limit\" value=\"250\""));
        assert!(html.contains("Replay all matching (250)"));
        assert!(html.contains("Discard all matching (250)"));
        assert!(html.contains("Replay 250 matching dead-letter entries?"));
        assert!(html.contains("Discard 250 matching dead-letter entries?"));
    }

    /// Codex review on #1420: `parse_bulk_dlq_form` (autumn-harvest-plugin/
    /// src/api.rs) re-validates `task_kind`/`failed_after`/`failed_before`
    /// strictly and 400s on a bad value. The bulk-action forms must never
    /// submit an invalid raw value as a hidden field. Otherwise
    /// replay/discard aborts instead of running — the exact bug this PR
    /// fixes, one layer down. An invalid field is "filter not applied"
    /// here, so it must be omitted, not echoed with its raw, unparseable
    /// text.
    #[test]
    fn dead_letter_bulk_actions_omit_invalid_filter_instead_of_submitting_raw_value() {
        let filters = DeadLetterUiFilters {
            workflow_name: Some("invoice_workflow".to_string()),
            ..DeadLetterUiFilters::default()
        };
        let filter_raw = DeadLetterUiFilterRaw {
            task_kind: "zombie".to_string(),
            task_kind_error: Some("bad task_kind".to_string()),
            failed_after: "not-a-date".to_string(),
            failed_after_error: Some("bad failed_after".to_string()),
            failed_before: String::new(),
            failed_before_error: None,
            shard_id: String::new(),
            shard_id_error: None,
        };
        let html =
            render_dead_letter_bulk_actions(&filters, &filter_raw, DEFAULT_DLQ_PAGE_SIZE, None, 5)
                .into_string();
        assert!(
            !html.contains("name=\"task_kind\""),
            "the invalid task_kind must never be submitted as a bulk-action selector: {html}"
        );
        assert!(
            !html.contains("name=\"failed_after\""),
            "the invalid failed_after must never be submitted as a bulk-action selector: {html}"
        );
        assert!(
            html.contains("value=\"invoice_workflow\""),
            "the valid workflow_name filter must still be carried: {html}"
        );
        // The raw invalid text may still appear in `return_to`. It is a
        // GET redirect target, not a bulk selector field. Carrying it there
        // is how the inline error redisplays after the action completes.
        assert!(
            html.contains("return_to") && html.contains("zombie"),
            "the raw value is expected to survive in return_to, just not as a selector field: {html}"
        );
    }

    #[test]
    fn dead_letter_bulk_actions_label_when_limited_by_api_cap() {
        let filters = DeadLetterUiFilters {
            workflow_name: Some("invoice_workflow".to_string()),
            ..DeadLetterUiFilters::default()
        };

        let filter_raw = DeadLetterUiFilterRaw::default();
        let html = render_dead_letter_bulk_actions(
            &filters,
            &filter_raw,
            DEFAULT_DLQ_PAGE_SIZE,
            None,
            1_200,
        )
        .into_string();

        assert!(html.contains("name=\"limit\" value=\"1000\""));
        assert!(html.contains("Replay first 1000 matching (1200 total)"));
        assert!(html.contains("Discard first 1000 matching (1200 total)"));
        assert!(html.contains("Replay first 1000 of 1200 matching dead-letter entries?"));
        assert!(html.contains("Discard first 1000 of 1200 matching dead-letter entries?"));
    }

    #[test]
    fn parse_dead_letter_task_kind_filter_accepts_known_values_case_insensitively() {
        assert_eq!(
            parse_dead_letter_task_kind_filter(Some("Activity")),
            (
                Some(DeadLetterTaskKind::Activity),
                "Activity".to_string(),
                None
            )
        );
        assert_eq!(
            parse_dead_letter_task_kind_filter(Some("workflow")),
            (
                Some(DeadLetterTaskKind::Workflow),
                "workflow".to_string(),
                None
            )
        );
    }

    /// GREEN — the fix under test: an unrecognized `task_kind` no longer
    /// aborts `list_dead_letters_ui`. It degrades to "filter not applied"
    /// (parsed is `None`) while carrying the raw text and a recovery
    /// message. The page can then redisplay the form inline, instead of
    /// discarding it. Same contract as `parse_worker_status_filter` on the
    /// Workers page (#1378). Before this change, `parse_dead_letter_ui_filters`
    /// `?`-propagated `DeadLetterTaskKind::parse`'s bare
    /// `AutumnError::bad_request_msg` here. That aborted the whole
    /// `/dead-letters` response before the filter form was ever rendered —
    /// along with the `workflow_name`/`shard_id` filters the operator had
    /// already typed.
    #[test]
    fn parse_dead_letter_task_kind_filter_rejects_unknown_value_without_erroring() {
        let (parsed, raw, error) = parse_dead_letter_task_kind_filter(Some("zombie"));
        assert_eq!(parsed, None, "an invalid task_kind must not be applied");
        assert_eq!(
            raw, "zombie",
            "the operator's exact raw input is echoed back"
        );
        let error = error.expect("an invalid task_kind must carry a redisplayable error");
        assert!(
            error.contains("zombie") && error.contains("Activity"),
            "error names the bad value and a valid option: {error}"
        );
    }

    #[test]
    fn parse_dead_letter_task_kind_filter_blank_or_missing_is_not_an_error() {
        assert_eq!(
            parse_dead_letter_task_kind_filter(None),
            (None, String::new(), None)
        );
        assert_eq!(
            parse_dead_letter_task_kind_filter(Some("   ")),
            (None, String::new(), None)
        );
    }

    #[test]
    fn parse_dead_letter_time_filter_accepts_rfc3339() {
        let (parsed, raw, error) =
            parse_dead_letter_time_filter("failed_after", Some("2026-05-10T00:00:00Z"));
        assert!(parsed.is_some());
        assert_eq!(raw, "2026-05-10T00:00:00Z");
        assert_eq!(error, None);
    }

    /// GREEN — the fix under test: a malformed `failed_after`/`failed_before`
    /// no longer aborts the page. Before this change,
    /// `parse_dead_letter_time_filter` returned `Result<_, AutumnError>`.
    /// `parse_dead_letter_ui_filters` then propagated it with a bare `?`.
    /// That matched the same discard-the-page-on-bad-filter pattern already
    /// fixed on the Workflows page's `started_after`/`started_before` (#1333).
    #[test]
    fn parse_dead_letter_time_filter_rejects_malformed_value_without_erroring() {
        let (parsed, raw, error) =
            parse_dead_letter_time_filter("failed_after", Some("not-a-date"));
        assert_eq!(parsed, None, "an invalid timestamp must not be applied");
        assert_eq!(
            raw, "not-a-date",
            "the operator's exact raw input is echoed back"
        );
        let error = error.expect("an invalid timestamp must carry a redisplayable error");
        assert!(
            error.contains("failed_after") && error.contains("RFC 3339"),
            "error names the field and the expected format: {error}"
        );
    }

    #[test]
    fn parse_dead_letter_time_filter_blank_or_missing_is_not_an_error() {
        assert_eq!(
            parse_dead_letter_time_filter("failed_after", None),
            (None, String::new(), None)
        );
        assert_eq!(
            parse_dead_letter_time_filter("failed_after", Some("   ")),
            (None, String::new(), None)
        );
    }

    /// GREEN — the fix under test. `reset_to_event_id` used to be typed
    /// `i64` straight on the `Form<..>` extractor struct for the "Reset to
    /// event N" action. A non-numeric value failed axum's own form
    /// deserialization. That aborted the request with a bare framework 400.
    /// The handler never ran. The operator's reason was never read, and no
    /// flash message could render. Reset is not a filter; it is the
    /// runbook's destructive recovery action for a stuck child workflow or
    /// a non-determinism failure. A malformed value must be rejected with a
    /// clear error. It must never be silently defaulted to some other
    /// event.
    #[test]
    fn parse_reset_to_event_id_accepts_valid_values() {
        assert_eq!(parse_reset_to_event_id("1"), Ok(1));
        assert_eq!(parse_reset_to_event_id("  42  "), Ok(42));
        assert_eq!(parse_reset_to_event_id("0"), Ok(0));
        assert_eq!(parse_reset_to_event_id("-3"), Ok(-3));
    }

    #[test]
    fn parse_reset_to_event_id_rejects_non_numeric_text() {
        let err = parse_reset_to_event_id("abc").expect_err("must reject non-numeric text");
        assert!(
            err.contains("abc"),
            "the error must name the bad value: {err}"
        );
    }

    #[test]
    fn parse_reset_to_event_id_rejects_a_fraction() {
        // A `type="number"` input's `step="1"` default blocks this in a
        // real browser. A bare `Form` POST from any other client is still a
        // reachable path. It must not 400 before the handler runs.
        assert!(parse_reset_to_event_id("1.5").is_err());
    }

    #[test]
    fn parse_reset_to_event_id_rejects_i64_overflow() {
        assert!(parse_reset_to_event_id("99999999999999999999").is_err());
    }

    #[test]
    fn parse_reset_to_event_id_rejects_blank_or_missing() {
        let err = parse_reset_to_event_id("").expect_err("empty text must be rejected");
        assert!(err.contains("required"), "error must explain why: {err}");
        assert!(parse_reset_to_event_id("   ").is_err());
    }

    /// GREEN — the fix under test: `shard`/`shard_id` used to be typed
    /// `Option<i32>` straight on the `Query<..>` extractor struct on all
    /// three list pages. A non-numeric value failed axum's own query
    /// deserialization, aborting the request with a bare framework 400.
    /// That happened before any handler, filter form, or the operator's
    /// other filters ever rendered. It is one layer earlier than the
    /// page-abort bug the `task_kind`/`failed_after`/`failed_before`/
    /// `status`/`stale` filters already fix, and with no styled error at
    /// all.
    #[test]
    fn parse_shard_id_filter_accepts_valid_values() {
        assert_eq!(
            parse_shard_id_filter("shard_id", Some("0")),
            (Some(0), "0".to_string(), None)
        );
        assert_eq!(
            parse_shard_id_filter("shard_id", Some("  3  ")),
            (Some(3), "3".to_string(), None)
        );
        assert_eq!(
            parse_shard_id_filter("shard_id", Some("-1")),
            (Some(-1), "-1".to_string(), None)
        );
    }

    #[test]
    fn parse_shard_id_filter_rejects_invalid_value_without_erroring() {
        let (parsed, raw, error) = parse_shard_id_filter("shard_id", Some("north"));
        assert_eq!(parsed, None, "an invalid shard_id must not be applied");
        assert_eq!(raw, "north", "the raw text must echo the operator's input");
        let message = error.expect("an invalid shard_id must carry a redisplayable error");
        assert!(
            message.contains("north") && message.contains("shard_id"),
            "the error must name the bad value and the field: {message}"
        );
    }

    #[test]
    fn parse_shard_id_filter_blank_or_missing_is_not_an_error() {
        assert_eq!(
            parse_shard_id_filter("shard_id", None),
            (None, String::new(), None)
        );
        assert_eq!(
            parse_shard_id_filter("shard_id", Some("   ")),
            (None, String::new(), None)
        );
    }

    #[test]
    fn render_dead_letter_filters_shows_inline_errors() {
        let filters = DeadLetterUiFilters::default();
        let filter_raw = DeadLetterUiFilterRaw {
            task_kind: "zombie".to_string(),
            task_kind_error: Some(
                "Unknown task_kind 'zombie'; expected Activity or Workflow. Filter not applied."
                    .to_string(),
            ),
            failed_after: "not-a-date".to_string(),
            failed_after_error: Some(
                "Invalid failed_after; expected RFC 3339 timestamp. Filter not applied."
                    .to_string(),
            ),
            failed_before: String::new(),
            failed_before_error: None,
            shard_id: "north".to_string(),
            shard_id_error: Some(
                "Invalid shard_id 'north'; expected a whole number. Filter not applied."
                    .to_string(),
            ),
        };
        let html = render_dead_letter_filters(&filters, &filter_raw, DEFAULT_DLQ_PAGE_SIZE, None)
            .into_string();
        assert!(
            html.contains("field-error") && html.contains("zombie"),
            "task_kind error must render inline: {html}"
        );
        assert!(
            html.contains("option value=\"zombie\" selected"),
            "the invalid task_kind must be echoed back as the selected option: {html}"
        );
        assert!(
            html.contains("not-a-date"),
            "failed_after error and raw text must render inline: {html}"
        );
        assert!(
            html.contains("north") && html.contains("Invalid shard_id"),
            "shard_id error and raw text must render inline: {html}"
        );
    }

    /// Codex-review-class regression guard, matching #1378 P2/#1333's own
    /// follow-up. An invalid filter's raw text — not the parsed value,
    /// always `None` — must survive into pagination and bulk-action hidden
    /// fields. Otherwise the inline error vanishes on the very next click.
    #[test]
    fn build_dead_letter_query_string_carries_invalid_raw_values() {
        let filters = DeadLetterUiFilters::default();
        let filter_raw = DeadLetterUiFilterRaw {
            task_kind: "zombie".to_string(),
            task_kind_error: Some("bad task_kind".to_string()),
            failed_after: "not-a-date".to_string(),
            failed_after_error: Some("bad failed_after".to_string()),
            failed_before: String::new(),
            failed_before_error: None,
            shard_id: "north".to_string(),
            shard_id_error: Some("bad shard_id".to_string()),
        };
        let query =
            build_dead_letter_query_string(DEFAULT_DLQ_PAGE_SIZE, &filters, &filter_raw, None);
        assert!(
            query.contains("task_kind=zombie"),
            "invalid task_kind must round-trip: {query}"
        );
        assert!(
            query.contains("failed_after=not-a-date"),
            "invalid failed_after must round-trip: {query}"
        );
        assert!(
            query.contains("shard_id=north"),
            "invalid shard_id must round-trip: {query}"
        );
    }

    /// Codex review on #1420: a summary drilldown's "View entries" link
    /// used to derive its `drill_raw` solely from the successfully parsed
    /// filters. This silently dropped an invalid `failed_after`/
    /// `failed_before` and its error, even though this function never
    /// touches those two fields. The view toggle, refresh, and group-by
    /// form all preserve that same invalid value. The drilldown link must
    /// not be the one exception.
    #[test]
    fn dlq_summary_drilldown_href_preserves_invalid_failed_after() {
        use autumn_harvest::dlq::DlqGroupDimension;

        let filters = DeadLetterUiFilters {
            workflow_name: Some("invoice_workflow".to_string()),
            ..DeadLetterUiFilters::default()
        };
        let filter_raw = DeadLetterUiFilterRaw {
            task_kind: String::new(),
            task_kind_error: None,
            failed_after: "not-a-date".to_string(),
            failed_after_error: Some("bad failed_after".to_string()),
            failed_before: String::new(),
            failed_before_error: None,
            shard_id: String::new(),
            shard_id_error: None,
        };
        let key = serde_json::json!({"workflow_name": "invoice_workflow"});
        let (href, _partial) = dlq_summary_drilldown_href(
            &key,
            &[DlqGroupDimension::WorkflowName],
            &filters,
            &filter_raw,
            DEFAULT_DLQ_PAGE_SIZE,
            None,
        );
        assert!(
            href.contains("failed_after=not-a-date"),
            "the drilldown link must preserve the invalid failed_after the \
             summary view had, not silently drop it: {href}"
        );
    }

    /// Same review: the `task_kind` field IS synthesized by this function,
    /// for the `TaskType` group-by dimension. Its raw text is overwritten
    /// to match the group's own key. It must not inherit a stale, unrelated
    /// error the summary view happened to be showing.
    #[test]
    fn dlq_summary_drilldown_href_overwrites_task_kind_synthesized_from_group() {
        use autumn_harvest::dlq::DlqGroupDimension;

        let filters = DeadLetterUiFilters::default();
        let filter_raw = DeadLetterUiFilterRaw {
            task_kind: "zombie".to_string(),
            task_kind_error: Some("stale error from an unrelated typo".to_string()),
            failed_after: String::new(),
            failed_after_error: None,
            failed_before: String::new(),
            failed_before_error: None,
            shard_id: String::new(),
            shard_id_error: None,
        };
        let key = serde_json::json!({"task_type": "ACTIVITY"});
        let (href, _partial) = dlq_summary_drilldown_href(
            &key,
            &[DlqGroupDimension::TaskType],
            &filters,
            &filter_raw,
            DEFAULT_DLQ_PAGE_SIZE,
            None,
        );
        assert!(
            href.contains("task_kind=Activity"),
            "the drilldown must use the group's own task_type, not the \
             stale raw value: {href}"
        );
    }

    #[test]
    fn state_badge_emits_class_and_label() {
        let html = state_badge("COMPLETED").into_string();
        assert!(html.contains("class=\"badge COMPLETED\""));
        assert!(html.contains(">COMPLETED<"));
    }

    #[test]
    fn json_card_escapes_quotes_in_payload() {
        let value = serde_json::json!({ "hello": "world" });
        let html = json_card("Input", &value).into_string();
        assert!(html.contains("&quot;hello&quot;"));
        assert!(html.contains("&quot;world&quot;"));
        assert!(!html.contains("<script"));
    }

    // -- Workers page pure-logic unit tests --

    fn fleet(total: usize, active: usize, stale: usize, errored: bool) -> WorkerFleetStats {
        WorkerFleetStats {
            total,
            active,
            draining: 0,
            stopped: total.saturating_sub(active),
            stale,
            any_shard_errored: errored,
        }
    }

    #[test]
    fn banner_healthy_when_active_no_stale() {
        let stats = fleet(2, 2, 0, false);
        assert_eq!(determine_banner_state(&stats), Some(BannerState::Healthy));
    }

    #[test]
    fn banner_degraded_when_stale_workers_exist() {
        let stats = fleet(3, 2, 1, false);
        assert_eq!(determine_banner_state(&stats), Some(BannerState::Degraded));
    }

    #[test]
    fn banner_unhealthy_when_no_active_workers() {
        let stats = fleet(2, 0, 0, false);
        assert_eq!(determine_banner_state(&stats), Some(BannerState::Unhealthy));
    }

    #[test]
    fn banner_none_when_empty_fleet_and_no_errors() {
        let stats = fleet(0, 0, 0, false);
        assert_eq!(determine_banner_state(&stats), None);
    }

    #[test]
    fn banner_degraded_when_shard_error_and_no_active_workers() {
        // Shard errored: state is partially unknown, so Degraded not Unhealthy.
        let stats = fleet(0, 0, 0, true);
        assert_eq!(determine_banner_state(&stats), Some(BannerState::Degraded));
    }

    #[test]
    fn banner_degraded_when_shard_errored_even_if_healthy_otherwise() {
        let stats = fleet(3, 3, 0, true);
        assert_eq!(determine_banner_state(&stats), Some(BannerState::Degraded));
    }

    #[test]
    fn banner_as_str_round_trips() {
        assert_eq!(BannerState::Healthy.as_str(), "Healthy");
        assert_eq!(BannerState::Degraded.as_str(), "Degraded");
        assert_eq!(BannerState::Unhealthy.as_str(), "Unhealthy");
    }

    #[test]
    fn relative_time_just_now_for_recent() {
        let ts = chrono::Utc::now() - chrono::Duration::seconds(2);
        assert_eq!(relative_time(ts), "just now");
    }

    #[test]
    fn relative_time_seconds_ago() {
        let ts = chrono::Utc::now() - chrono::Duration::seconds(20);
        assert_eq!(relative_time(ts), "20s ago");
    }

    #[test]
    fn relative_time_minutes_ago() {
        let ts = chrono::Utc::now() - chrono::Duration::seconds(90);
        assert_eq!(relative_time(ts), "1m ago");
    }

    #[test]
    fn relative_time_hours_ago() {
        let ts = chrono::Utc::now() - chrono::Duration::seconds(7200);
        assert_eq!(relative_time(ts), "2h ago");
    }

    #[test]
    fn build_worker_query_string_empty_defaults() {
        assert_eq!(
            build_worker_query_string(DEFAULT_PAGE_SIZE, "", "", "", None),
            ""
        );
    }

    #[test]
    fn build_worker_query_string_includes_all_params() {
        let q = build_worker_query_string(10, "Active", "1", "true", None);
        assert!(q.contains("limit=10"));
        assert!(q.contains("status=Active"));
        assert!(q.contains("shard=1"));
        assert!(q.contains("stale=true"));
    }

    /// GREEN — the fix under test: an invalid raw value (which a caller would
    /// otherwise have parsed to `None`/`false` and lost) is carried through
    /// verbatim, so a Next/Previous click doesn't drop the still-unresolved
    /// filter and its inline error (Codex review, #1378 P2).
    #[test]
    fn build_worker_query_string_carries_invalid_raw_values() {
        let q = build_worker_query_string(DEFAULT_PAGE_SIZE, "zombie", "north", "True", None);
        assert!(
            q.contains("status=zombie"),
            "an invalid status must still round-trip through pagination: {q}"
        );
        assert!(
            q.contains("shard=north"),
            "an invalid shard must still round-trip through pagination: {q}"
        );
        assert!(
            q.contains("stale=True"),
            "an invalid stale value must still round-trip through pagination: {q}"
        );
    }

    #[test]
    fn layout_includes_workers_nav_link() {
        let body = html! { p { "test" } };
        let html = layout("Test", &body, "").into_string();
        assert!(
            html.contains("workers"),
            "layout must include a Workers nav link"
        );
    }

    #[test]
    fn worker_status_badge_shows_stale_annotation() {
        let html = worker_status_badge("Active", true).into_string();
        assert!(html.contains("stale"));
        assert!(html.contains("Active"));
    }

    #[test]
    fn worker_status_badge_no_annotation_when_healthy() {
        let html = worker_status_badge("Active", false).into_string();
        assert!(html.contains("Active"));
        assert!(!html.contains("stale"));
    }

    // -- Schedule page pure-logic unit tests --

    fn make_schedule(
        workflow_name: Option<&str>,
        dag_name: Option<&str>,
        is_paused: bool,
    ) -> HarvestSchedule {
        HarvestSchedule {
            id: uuid::Uuid::new_v4(),
            dag_name: dag_name.map(str::to_string),
            schedule_expr: Some("0 * * * *".to_string()),
            timezone: "UTC".to_string(),
            catchup: false,
            max_active_runs: 1,
            is_paused,
            last_run_at: None,
            next_run_at: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            workflow_name: workflow_name.map(str::to_string),
            workflow_input: None,
            queue_name: None,
            paused_at: None,
            paused_by: None,
            pause_reason: None,
            jitter_secs: 0,
            overlap_policy: "skip".to_string(),
            buffered_runs: serde_json::json!([]),
            buffer_all_max: 100,
            calendar_name: None,
            skip_policy: "skip".to_string(),
            fire_claim_token: None,
            fire_claimed_until: None,
            consecutive_failure_limit: None,
            consecutive_failure_count: 0,
            auto_paused_at: None,
            end_at: None,
            max_runs: None,
            runs_started: 0,
            exhausted_at: None,
            exhausted_reason: None,
            catchup_policy: None,
            catchup_window_secs: None,
            last_catchup_dropped: 0,
            last_catchup_at: None,
            retry_policy: None,
        }
    }

    #[test]
    fn schedule_filter_matches_all_when_empty() {
        let filters = ScheduleUiFilters::default();
        let wf = make_schedule(Some("my_workflow"), None, false);
        let dag = make_schedule(None, Some("my_dag"), true);
        assert!(filters.matches(ShardId::new(0), &wf));
        assert!(filters.matches(ShardId::new(0), &dag));
    }

    #[test]
    fn schedule_filter_kind_workflow_excludes_dags() {
        let filters = ScheduleUiFilters {
            kind: ScheduleKindFilter::Workflow,
            ..Default::default()
        };
        let wf = make_schedule(Some("wf"), None, false);
        let dag = make_schedule(None, Some("dag"), false);
        assert!(filters.matches(ShardId::new(0), &wf));
        assert!(!filters.matches(ShardId::new(0), &dag));
    }

    #[test]
    fn schedule_filter_kind_dag_excludes_workflows() {
        let filters = ScheduleUiFilters {
            kind: ScheduleKindFilter::Dag,
            ..Default::default()
        };
        let wf = make_schedule(Some("wf"), None, false);
        let dag = make_schedule(None, Some("dag"), false);
        assert!(!filters.matches(ShardId::new(0), &wf));
        assert!(filters.matches(ShardId::new(0), &dag));
    }

    #[test]
    fn schedule_filter_paused_excludes_active() {
        let filters = ScheduleUiFilters {
            paused: SchedulePausedFilter::Paused,
            ..Default::default()
        };
        let active = make_schedule(Some("wf"), None, false);
        let paused = make_schedule(Some("wf2"), None, true);
        assert!(!filters.matches(ShardId::new(0), &active));
        assert!(filters.matches(ShardId::new(0), &paused));
    }

    #[test]
    fn schedule_filter_active_excludes_paused() {
        let filters = ScheduleUiFilters {
            paused: SchedulePausedFilter::Active,
            ..Default::default()
        };
        let active = make_schedule(Some("wf"), None, false);
        let paused = make_schedule(Some("wf2"), None, true);
        assert!(filters.matches(ShardId::new(0), &active));
        assert!(!filters.matches(ShardId::new(0), &paused));
    }

    #[test]
    fn schedule_filter_target_substring_match() {
        let filters = ScheduleUiFilters {
            target: Some("payment".to_string()),
            ..Default::default()
        };
        let matching = make_schedule(Some("payment_workflow"), None, false);
        let other = make_schedule(Some("invoice_workflow"), None, false);
        assert!(filters.matches(ShardId::new(0), &matching));
        assert!(!filters.matches(ShardId::new(0), &other));
    }

    #[test]
    fn schedule_filter_target_case_insensitive() {
        let filters = ScheduleUiFilters {
            target: Some("PAYMENT".to_string()),
            ..Default::default()
        };
        let matching = make_schedule(Some("payment_workflow"), None, false);
        assert!(filters.matches(ShardId::new(0), &matching));
    }

    #[test]
    fn schedule_filter_shard_id_match() {
        let filters = ScheduleUiFilters {
            shard_id: Some(1),
            ..Default::default()
        };
        let row = make_schedule(Some("wf"), None, false);
        assert!(filters.matches(ShardId::new(1), &row));
        assert!(!filters.matches(ShardId::new(0), &row));
    }

    #[test]
    fn schedule_state_badge_paused() {
        let html = schedule_state_badge(true).into_string();
        assert!(html.contains("Paused"));
    }

    #[test]
    fn schedule_state_badge_active() {
        let html = schedule_state_badge(false).into_string();
        assert!(html.contains("Active"));
    }

    #[test]
    fn schedule_kind_distribution_empty() {
        assert_eq!(schedule_kind_distribution(&[]), "");
    }

    #[test]
    fn schedule_kind_distribution_workflow_only() {
        let row = make_schedule(Some("wf"), None, false);
        assert_eq!(
            schedule_kind_distribution(&[(ShardId::UNENCODED, row)]),
            "1 Workflow"
        );
    }

    #[test]
    fn schedule_kind_distribution_dag_only() {
        let row = make_schedule(None, Some("my_dag"), false);
        assert_eq!(
            schedule_kind_distribution(&[(ShardId::UNENCODED, row)]),
            "1 Dag"
        );
    }

    #[test]
    fn schedule_kind_distribution_mixed() {
        let wf = make_schedule(Some("wf"), None, false);
        let dag1 = make_schedule(None, Some("dag_a"), false);
        let dag2 = make_schedule(None, Some("dag_b"), false);
        let rows = vec![
            (ShardId::UNENCODED, wf),
            (ShardId::UNENCODED, dag1),
            (ShardId::UNENCODED, dag2),
        ];
        assert_eq!(schedule_kind_distribution(&rows), "1 Workflow, 2 Dag");
    }

    #[test]
    fn build_schedule_query_string_omits_defaults() {
        let filters = ScheduleUiFilters::default();
        let filter_raw = ScheduleUiFilterRaw::default();
        assert_eq!(
            build_schedule_query_string(DEFAULT_SCHEDULE_PAGE_SIZE, &filters, &filter_raw, None),
            ""
        );
    }

    #[test]
    fn build_schedule_query_string_includes_all_params() {
        let filters = ScheduleUiFilters {
            target: Some("payment".to_string()),
            kind: ScheduleKindFilter::Workflow,
            paused: SchedulePausedFilter::Paused,
            health: ScheduleHealthFilter::Unhealthy,
            shard_id: Some(2),
        };
        let filter_raw = ScheduleUiFilterRaw {
            kind: "Workflow".to_string(),
            paused: "Paused".to_string(),
            health: "Unhealthy".to_string(),
            shard_id: "2".to_string(),
            ..ScheduleUiFilterRaw::default()
        };
        let q = build_schedule_query_string(10, &filters, &filter_raw, Some(30));
        assert!(q.contains("health=Unhealthy"), "missing health: {q}");
        assert!(q.contains("limit=10"), "missing limit: {q}");
        assert!(q.contains("target=payment"), "missing target: {q}");
        assert!(q.contains("kind=Workflow"), "missing kind: {q}");
        assert!(q.contains("paused=Paused"), "missing paused: {q}");
        assert!(q.contains("shard_id=2"), "missing shard_id: {q}");
        assert!(q.contains("refresh=30"), "missing refresh: {q}");
    }

    /// GREEN — the fix under test: an invalid raw value is carried through
    /// verbatim. A caller would otherwise have parsed it to `All`/`None`
    /// and lost it. A Next/Previous click or bulk-action resubmit must not
    /// drop the still-unresolved filter and its inline error. Same
    /// contract as `build_dead_letter_query_string` on the DLQ page.
    #[test]
    fn build_schedule_query_string_carries_invalid_raw_values() {
        let filters = ScheduleUiFilters::default();
        let filter_raw = ScheduleUiFilterRaw {
            kind: "zombie".to_string(),
            kind_error: Some("bad kind".to_string()),
            shard_id: "north".to_string(),
            shard_id_error: Some("bad shard_id".to_string()),
            ..ScheduleUiFilterRaw::default()
        };
        let q =
            build_schedule_query_string(DEFAULT_SCHEDULE_PAGE_SIZE, &filters, &filter_raw, None);
        assert!(
            q.contains("kind=zombie"),
            "an invalid kind must still round-trip through pagination: {q}"
        );
        assert!(
            q.contains("shard_id=north"),
            "an invalid shard_id must still round-trip through pagination: {q}"
        );
    }

    #[test]
    fn layout_schedules_has_nav_link() {
        let body = html! { p { "test" } };
        let html = layout_schedules("Test", &body, None, "", "").into_string();
        assert!(
            html.contains("schedules"),
            "layout_schedules must include schedules link"
        );
        assert!(
            html.contains("Workflows"),
            "layout_schedules must include workflows link"
        );
        assert!(
            html.contains("Workers"),
            "layout_schedules must include workers link"
        );
    }

    #[test]
    fn layout_schedules_auto_refresh_tag() {
        let body = html! { p { "test" } };
        let html_with =
            layout_schedules("T", &body, Some(30), "", "schedules?page=0").into_string();
        assert!(html_with.contains("http-equiv=\"refresh\""));
        assert!(html_with.contains(r#"content="30; url=schedules?page=0""#));
        let html_without = layout_schedules("T", &body, None, "", "").into_string();
        assert!(!html_without.contains("http-equiv=\"refresh\""));
    }

    /// Codex review on #1437 (P2): a targetless `meta refresh` would
    /// reload this page's own URL. If that URL still carries `flash=...`
    /// (as it does right after a bulk pause/resume redirect), every
    /// auto-refresh interval re-announces and re-focuses the same stale
    /// message. The tag must instead point `url=` at the flash-free
    /// target the caller supplies — same fix as `layout_dead_letters`
    /// already applies (PR #1396).
    #[test]
    fn layout_schedules_refresh_tag_targets_flash_free_url() {
        let body = html! { p { "test" } };
        let html =
            layout_schedules("Test", &body, Some(30), "", "schedules?kind=Workflow").into_string();
        assert!(
            html.contains(r#"content="30; url=schedules?kind=Workflow""#),
            "refresh tag must target the flash-free URL: {html}"
        );
    }

    /// PR #1396's own review class, applied to the Schedules page. The
    /// auto-refresh target must keep the operator on the page they were
    /// reading, not bounce them to page 0.
    #[test]
    fn schedules_page_refresh_target_preserves_current_page() {
        let filters = ScheduleUiFilters::default();
        let filter_raw = ScheduleUiFilterRaw::default();
        let html = render_schedules_page(
            &[],
            &[],
            false,
            &filters,
            &filter_raw,
            &std::collections::HashMap::new(),
            2,
            50,
            false,
            0,
            "",
            "",
            Some(30),
            None,
        )
        .into_string();
        assert!(
            html.contains("url=schedules?page=2"),
            "refresh target must preserve page=2: {html}"
        );
    }

    #[test]
    fn layout_includes_schedules_nav_link() {
        let body = html! { p { "test" } };
        let html = layout("Test", &body, "").into_string();
        assert!(
            html.contains("schedules"),
            "layout must include schedules nav link"
        );
        assert!(html.contains("dags"), "layout must include dags nav link");
    }

    #[test]
    fn render_dag_list_includes_operational_columns() {
        let dags = vec![DagUiSummary {
            name: "payments".to_string(),
            schedule_expr: Some("0 * * * *".to_string()),
            task_count: 9,
            is_paused: true,
            next_run_at: None,
            max_active_runs: 3,
            catchup: false,
        }];
        let html = render_dag_list(&dags, &[]).into_string();
        assert!(html.contains("Paused"));
        assert!(html.contains("Next Run"));
        assert!(html.contains("Max Active"));
        assert!(html.contains("Catchup"));
        assert!(html.contains("payments"));
    }

    #[test]
    fn render_dag_list_surfaces_schedule_shard_errors() {
        let html = render_dag_list(&[], &[(ShardId::new(2), "connection refused".to_string())])
            .into_string();

        assert!(html.contains("Shard 2 unavailable"));
        assert!(html.contains("connection refused"));
    }

    #[test]
    fn dag_schedule_row_merge_preserves_runtime_formatted_expression() {
        let mut summary = DagUiSummary {
            name: "subsecond".to_string(),
            schedule_expr: Some("@every 0.500000000s".to_string()),
            task_count: 1,
            is_paused: false,
            next_run_at: None,
            max_active_runs: 1,
            catchup: false,
        };
        let mut row = make_schedule(None, Some("subsecond"), true);
        row.schedule_expr = Some("interval:0".to_string());
        row.max_active_runs = 3;
        row.catchup = true;

        merge_dag_schedule_row(&mut summary, &row);

        assert_eq!(
            summary.schedule_expr.as_deref(),
            Some("@every 0.500000000s")
        );
        assert!(summary.is_paused);
        assert_eq!(summary.max_active_runs, 3);
        assert!(summary.catchup);
    }

    #[test]
    fn dag_schedule_row_merge_uses_persisted_expression_when_runtime_has_none() {
        let mut summary = DagUiSummary {
            name: "manualish".to_string(),
            schedule_expr: None,
            task_count: 1,
            is_paused: false,
            next_run_at: None,
            max_active_runs: 1,
            catchup: false,
        };
        let row = make_schedule(None, Some("manualish"), false);

        merge_dag_schedule_row(&mut summary, &row);

        assert_eq!(summary.schedule_expr.as_deref(), Some("0 * * * *"));
    }

    #[test]
    fn layout_dag_detail_refresh_tag() {
        let body = html! { p { "x" } };
        let html = layout_dag_detail("D", &body, "", Some(30)).into_string();
        assert!(html.contains("http-equiv=\"refresh\""));
        assert!(html.contains("content=\"30\""));
    }

    // Issue #957 (Codex review): every non-current run in the list must link to
    // its own graph via `?run=`, and the currently-shown run must be marked
    // "current" rather than linked — so an operator can inspect an older run's
    // graph from the page instead of hand-building the query URL.
    #[test]
    fn render_dag_run_rows_links_each_run_to_its_graph_and_marks_current() {
        let mut run_a = stub_execution();
        run_a.id = uuid::Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
        let mut run_b = stub_execution();
        run_b.id = uuid::Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();
        let runs = vec![run_a.clone(), run_b.clone()];

        // run_a is the selected/current run.
        let html = render_dag_run_rows(&runs, Some(run_a.id)).into_string();

        // The non-current run (run_b) is reachable as a graph via `?run=<id>`,
        // with the id url-encoded.
        let expected_href = format!("?run={}", url_encode(&run_b.id.to_string()));
        assert!(
            html.contains(&format!("href=\"{expected_href}\"")),
            "non-current run must link to its graph via ?run=; html={html}"
        );

        // The current run is marked, not linked to a graph.
        assert!(
            html.contains("(current)"),
            "current run must be marked; html={html}"
        );
        assert!(
            !html.contains(&format!("?run={}", url_encode(&run_a.id.to_string()))),
            "current run must NOT carry a ?run= graph link; html={html}"
        );

        // The secondary workflow-detail link is preserved for every run.
        assert!(html.contains(&format!("../workflows/{}", run_a.id)));
        assert!(html.contains(&format!("../workflows/{}", run_b.id)));
    }

    #[test]
    fn render_dag_list_uses_dag_active_nav() {
        let dags = vec![];
        let html = render_dag_list(&dags, &[]).into_string();
        assert!(html.contains("class=\"active\" href=\"dags\""));
    }

    #[test]
    fn dag_run_selection_accepts_valid_requested_run_not_in_display_page() {
        let requested = uuid::Uuid::from_u128(1);
        let newest_listed = uuid::Uuid::from_u128(2);

        assert_eq!(
            resolve_dag_run_selection(true, Some(requested), Some(newest_listed)).unwrap(),
            Some(requested)
        );
    }

    #[test]
    fn dag_run_selection_unknown_requested_run_is_not_found_never_falls_back() {
        // Issue #957 AC7: an explicitly-provided-but-unknown `?run=` renders the
        // 404 message; it must NOT silently substitute the latest run.
        let newest_listed = uuid::Uuid::from_u128(2);
        assert!(
            resolve_dag_run_selection(true, None, Some(newest_listed)).is_err(),
            "an unknown present `?run=` must be a not-found, never a fallback"
        );
    }

    #[test]
    fn dag_run_selection_omitted_run_defaults_to_latest() {
        let newest_listed = uuid::Uuid::from_u128(2);
        assert_eq!(
            resolve_dag_run_selection(false, None, Some(newest_listed)).unwrap(),
            Some(newest_listed),
            "an omitted `?run=` defaults to the latest run"
        );
    }

    #[test]
    fn dag_run_selection_omitted_run_with_no_runs_is_none() {
        assert_eq!(
            resolve_dag_run_selection(false, None, None).unwrap(),
            None,
            "an omitted `?run=` on a DAG with no runs selects nothing (no 404)"
        );
    }

    #[test]
    fn dag_run_shard_matches_router_dag_owner() {
        let router = autumn_harvest::ShardRouter::new(
            vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)],
            vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)],
            ShardId::new(0),
        );

        assert_eq!(
            dag_run_shard(&router, "daily_etl"),
            router.pick_for_dag("daily_etl")
        );
    }

    fn task_queue_item_for_activity(activity_name: &str, state: &str) -> TaskQueueItem {
        let now = Utc::now();
        TaskQueueItem {
            id: uuid::Uuid::new_v4(),
            queue_name: "default".to_string(),
            task_type: "ACTIVITY".to_string(),
            workflow_exec_id: Some(uuid::Uuid::new_v4()),
            activity_name: Some(activity_name.to_string()),
            activity_id: None,
            input: Value::Null,
            state: state.to_string(),
            priority: 0,
            worker_id: None,
            attempt: 0,
            max_attempts: 1,
            scheduled_at: now,
            started_at: None,
            completed_at: None,
            last_heartbeat_at: None,
            heartbeat_details: None,
            heartbeat_timeout: None,
            start_to_close: None,
            schedule_to_start: None,
            retry_policy: None,
            output: None,
            error: None,
            sticky_worker_id: None,
            sticky_until: None,
            sticky_timeout: None,
            trace_context: None,
            concurrency_key: None,
            concurrency_cap: None,
            required_build_id: None,
            rate_limit_key: None,
            crash_strikes: 0,
            schedule_to_close_at: None,
            required_capabilities: None,
            context_headers: None,
            created_at: Some(now),
            wake_requested: false,
            session_id: None,
            capability_misses: 0,
            capability_miss_workers: Vec::new(),
            capability_miss_handler: None,
        }
    }

    #[test]
    fn format_run_duration_renders_elapsed_time() {
        let start = Utc::now();
        let end = start + chrono::Duration::seconds(125);
        assert_eq!(format_run_duration(start, Some(end)), "2m 5s");
    }

    #[test]
    fn schedule_expr_preserves_subsecond_interval() {
        let expr = schedule_expr_for_ui_summary(&Schedule::Interval(
            std::time::Duration::from_millis(500),
        ));
        assert_eq!(expr, "@every 0.500000000s");
    }

    #[test]
    fn schedule_kind_filter_parse_roundtrips() {
        assert!(matches!(
            ScheduleKindFilter::parse("").unwrap(),
            ScheduleKindFilter::All
        ));
        assert!(matches!(
            ScheduleKindFilter::parse("Workflow").unwrap(),
            ScheduleKindFilter::Workflow
        ));
        assert!(matches!(
            ScheduleKindFilter::parse("Dag").unwrap(),
            ScheduleKindFilter::Dag
        ));
        assert!(ScheduleKindFilter::parse("bogus").is_err());
    }

    #[test]
    fn schedule_paused_filter_parse_roundtrips() {
        assert!(matches!(
            SchedulePausedFilter::parse("").unwrap(),
            SchedulePausedFilter::All
        ));
        assert!(matches!(
            SchedulePausedFilter::parse("Paused").unwrap(),
            SchedulePausedFilter::Paused
        ));
        assert!(matches!(
            SchedulePausedFilter::parse("Active").unwrap(),
            SchedulePausedFilter::Active
        ));
        assert!(SchedulePausedFilter::parse("maybe").is_err());
    }

    // -- Issue #279: history event count and continue-as-new threshold --

    fn stub_execution() -> autumn_harvest::models::WorkflowExecution {
        use chrono::Utc;
        use uuid::Uuid;
        autumn_harvest::models::WorkflowExecution {
            migrated_to_shard: None,
            migrated_at: None,
            migrated_from_shards: None,
            quota_key: None,
            id: Uuid::new_v4(),
            workflow_name: "test_workflow".to_string(),
            workflow_id: "wf-1".to_string(),
            run_id: Uuid::new_v4(),
            shard_id: 0,
            state: "RUNNING".to_string(),
            input: serde_json::json!(null),
            output: None,
            error: None,
            parent_id: None,
            sticky_worker_id: None,
            queue_name: "default".to_string(),
            started_at: Utc::now(),
            completed_at: None,
            execution_timeout: None,
            deadline_at: None,
            chain_execution_timeout: None,
            chain_deadline_at: None,
            memo: None,
            search_attrs: None,
            created_at: Utc::now(),
            assigned_build_id: None,
            parent_close_policy: None,
            owner: None,
            runbook_url: None,
            severity: None,
            context_headers: None,
            sla: None,
            sla_deadline_at: None,
            sla_breached: false,
            sla_breached_at: None,
            paused_at: None,
            pause_reason: None,
            pause_actor: None,
            current_details: None,
            schedule_id: None,
            scheduled_for: None,
            workflow_attempt: 1,
            workflow_retry_policy: None,
            retry_of_exec_id: None,
            origin: None,
            nd_blocked_at: None,
            nd_block_reason: None,
            nd_block_count: 0,
            completion_callbacks: None,
            continued_from_exec_id: None,
            first_exec_id: None,
            legal_hold_set_at: None,
            legal_hold_until: None,
            legal_hold_reason: None,
            legal_hold_actor: None,
            start_source: None,
            start_source_ref: None,
            started_by: None,
            history_bloat_warned_at: None,
            triage_note: None,
        }
    }

    fn stub_blocked_on() -> BlockedOnData {
        BlockedOnData {
            activities: vec![],
            external_tasks: vec![],
            timers: vec![],
            signals: vec![],
            heartbeat_details_cap: 0,
            heartbeat_caps: std::collections::HashMap::new(),
            awaitables: None,
        }
    }

    // ── Issue #608 / PR #936 round 5: decode only rendered detail fields ─────

    /// Reversing test codec so an envelope is distinguishable from plaintext.
    #[derive(Debug)]
    struct ReverseUiCodec;

    impl autumn_harvest::payload_codec::PayloadCodec for ReverseUiCodec {
        fn codec_id(&self) -> &'static str {
            "reverse"
        }
        fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, autumn_harvest::payload_codec::CodecError> {
            let mut v = raw.to_vec();
            v.reverse();
            Ok(v)
        }
        fn decode(
            &self,
            encoded: &[u8],
        ) -> Result<Vec<u8>, autumn_harvest::payload_codec::CodecError> {
            let mut v = encoded.to_vec();
            v.reverse();
            Ok(v)
        }
    }

    fn ui_test_codecs() -> PayloadCodecs {
        let mut codecs = PayloadCodecs::default();
        codecs.set_default(Arc::new(ReverseUiCodec));
        codecs
    }

    /// Builds a well-formed `reverse` codec envelope for `plain` via the
    /// public `encode_event` round-trip (mirrors the integration fixture).
    fn ui_envelope(plain: &Value) -> Value {
        let event = autumn_harvest::WorkflowEvent::WorkflowCompleted {
            output: plain.clone(),
        };
        let encoded = ui_test_codecs().encode_event(&event).expect("encode event");
        encoded["data"]["output"].clone()
    }

    fn stub_timeline_event(event_data: Value) -> HarvestEvent {
        HarvestEvent {
            id: 1,
            workflow_exec_id: uuid::Uuid::new_v4(),
            event_id: 0,
            event_type: "ActivityScheduled".to_string(),
            event_data,
            timestamp: Utc::now(),
        }
    }

    fn stub_pending_signal(payload: Value) -> HarvestSignal {
        HarvestSignal {
            id: uuid::Uuid::new_v4(),
            workflow_exec_id: uuid::Uuid::new_v4(),
            signal_name: "approval".to_string(),
            payload,
            received_at: Utc::now(),
            consumed: false,
            idempotency_key: None,
        }
    }

    /// PR #936 round 5: the pending-activity `input` and pending-signal
    /// payloads are never rendered by the detail page, so they must not be
    /// decoded and must not count toward the `payload.decode_read` audit
    /// outcome — envelopes there stay untouched and the outcome stays empty
    /// (no audit row would be written for a page whose only envelopes are
    /// hidden).
    #[test]
    fn hidden_detail_fields_are_not_decoded_and_never_touch_the_audit_outcome() {
        let codecs = ui_test_codecs();
        let mut execution = stub_execution();
        let mut timeline: Vec<HarvestEvent> = vec![];

        let input_envelope = ui_envelope(&serde_json::json!({"card": "pii-task-input"}));
        let signal_envelope = ui_envelope(&serde_json::json!({"approver": "pii-signal"}));
        let mut task = task_queue_item_for_activity("charge_card", "PENDING");
        task.input = input_envelope.clone();
        let mut blocked_on = stub_blocked_on();
        blocked_on.activities.push(task);
        blocked_on
            .signals
            .push(stub_pending_signal(signal_envelope.clone()));

        let outcome = decode_workflow_detail_rendered_fields(
            &codecs,
            &mut execution,
            &mut timeline,
            &mut blocked_on,
        );

        assert_eq!(outcome.decoded, 0, "hidden fields must not be decoded");
        assert_eq!(outcome.failed, 0, "hidden fields must not be marked");
        assert!(
            !outcome.touched(),
            "a page whose only envelopes are hidden must not write an audit row"
        );
        assert_eq!(
            blocked_on.activities[0].input, input_envelope,
            "the pending-activity input must keep its stored envelope"
        );
        assert_eq!(
            blocked_on.signals[0].payload, signal_envelope,
            "the pending-signal payload must keep its stored envelope"
        );
    }

    /// The fields the detail page actually renders — execution payload
    /// fields, timeline event payloads, and heartbeat checkpoints — are
    /// decoded, and the audit outcome counts exactly those.
    #[test]
    fn rendered_detail_fields_are_decoded_and_counted_exactly() {
        let codecs = ui_test_codecs();
        let mut execution = stub_execution();
        execution.input = ui_envelope(&serde_json::json!({"user": "pii-exec-input"}));
        let mut timeline = vec![stub_timeline_event(serde_json::json!({
            "type": "ActivityScheduled",
            "data": { "input": ui_envelope(&serde_json::json!({"card": "pii-event"})) },
        }))];

        let checkpoint_envelope = ui_envelope(&serde_json::json!({"progress": "pii-checkpoint"}));
        let input_envelope = ui_envelope(&serde_json::json!({"card": "pii-task-input"}));
        let mut task = task_queue_item_for_activity("charge_card", "RUNNING");
        task.input = input_envelope.clone();
        task.heartbeat_details = Some(checkpoint_envelope);
        let mut blocked_on = stub_blocked_on();
        blocked_on.activities.push(task);

        let outcome = decode_workflow_detail_rendered_fields(
            &codecs,
            &mut execution,
            &mut timeline,
            &mut blocked_on,
        );

        assert_eq!(
            (outcome.decoded, outcome.failed),
            (3, 0),
            "exactly the three surfaced envelopes are decoded"
        );
        assert_eq!(
            execution.input,
            serde_json::json!({"user": "pii-exec-input"}),
            "the rendered execution input must be decoded"
        );
        assert_eq!(
            timeline[0].event_data["data"]["input"],
            serde_json::json!({"card": "pii-event"}),
            "the rendered timeline event payload must be decoded"
        );
        assert_eq!(
            blocked_on.activities[0].heartbeat_details,
            Some(serde_json::json!({"progress": "pii-checkpoint"})),
            "the rendered heartbeat checkpoint must be decoded"
        );
        assert_eq!(
            blocked_on.activities[0].input, input_envelope,
            "the hidden pending-activity input must stay an envelope even when \
             its sibling checkpoint is decoded"
        );
    }

    #[test]
    fn render_detail_shows_history_event_count_with_threshold() {
        let execution = stub_execution();
        let blocked = stub_blocked_on();
        let html = render_workflow_detail(
            &execution,
            42,
            &[],
            &[],
            &[],
            false,
            &[],
            0,
            &blocked,
            None,
            Some(10_000),
            &WorkflowLogsPanelData::default(),
        )
        .into_string();

        assert!(
            html.contains("History events"),
            "metadata card must have a 'History events' label"
        );
        assert!(
            html.contains("42"),
            "metadata card must show the event count 42"
        );
        assert!(
            html.contains("10000") || html.contains("10,000"),
            "metadata card must show the threshold 10000"
        );
    }

    #[test]
    fn render_detail_shows_threshold_absent_when_none() {
        let execution = stub_execution();
        let blocked = stub_blocked_on();
        let html = render_workflow_detail(
            &execution,
            5,
            &[],
            &[],
            &[],
            false,
            &[],
            0,
            &blocked,
            None,
            None,
            &WorkflowLogsPanelData::default(),
        )
        .into_string();

        assert!(
            html.contains("History events"),
            "label should still appear when threshold is None"
        );
        assert!(
            html.contains('5'),
            "event count 5 should appear even without threshold"
        );
    }

    #[test]
    fn render_detail_shows_custom_threshold() {
        let execution = stub_execution();
        let blocked = stub_blocked_on();
        let html = render_workflow_detail(
            &execution,
            300,
            &[],
            &[],
            &[],
            false,
            &[],
            0,
            &blocked,
            None,
            Some(500),
            &WorkflowLogsPanelData::default(),
        )
        .into_string();

        assert!(html.contains("300"), "event count 300 must appear in HTML");
        assert!(
            html.contains("500"),
            "custom threshold 500 must appear in HTML"
        );
    }

    // ── Build Routing page unit tests (issue #362 — Red Phase) ─────────────

    #[test]
    fn layout_build_routing_has_active_nav_link() {
        let body = html! { p { "test" } };
        let html = layout_build_routing("Build Routing · Vantage", &body, None).into_string();
        assert!(
            html.contains("build-routing"),
            "layout_build_routing must include the build-routing nav link"
        );
        // The active link must be present
        assert!(
            html.contains("Build Routing"),
            "layout_build_routing must show 'Build Routing' label"
        );
    }

    #[test]
    fn layout_includes_build_routing_nav_link() {
        let body = html! { p { "test" } };
        let html = layout("Test", &body, "").into_string();
        assert!(
            html.contains("build-routing"),
            "base layout must include a Build Routing nav link"
        );
    }

    #[test]
    fn layout_workers_includes_build_routing_nav_link() {
        let body = html! { p { "test" } };
        let html = layout_workers("Test", &body, None).into_string();
        assert!(
            html.contains("build-routing"),
            "layout_workers must include a Build Routing nav link"
        );
    }

    #[test]
    fn layout_dead_letters_includes_build_routing_nav_link() {
        let body = html! { p { "test" } };
        let html = layout_dead_letters("Test", &body, None, "").into_string();
        assert!(
            html.contains("build-routing"),
            "layout_dead_letters must include a Build Routing nav link"
        );
    }

    #[test]
    fn layout_dead_letters_refresh_tag_targets_flash_free_url() {
        // A targetless `meta refresh` would reload this page's own URL.
        // If that URL still carries `flash=...`, every auto-refresh
        // interval re-announces and re-focuses the same stale message.
        // The tag must instead point `url=` at the flash-free target the
        // caller supplies.
        let body = html! { p { "test" } };
        let html = layout_dead_letters("Test", &body, Some(30), "../ui/dead-letters?limit=50")
            .into_string();
        assert!(
            html.contains(r#"content="30; url=../ui/dead-letters?limit=50""#),
            "refresh tag must target the flash-free URL: {html}"
        );
    }

    #[test]
    fn dead_letters_page_refresh_target_preserves_current_page() {
        // PR #1396 review: the auto-refresh target must keep the operator
        // on the page they were reading, not bounce them to page 0.
        let filters = DeadLetterUiFilters::default();
        let filter_raw = DeadLetterUiFilterRaw::default();
        let html = render_dead_letters_page(
            &filters,
            &filter_raw,
            &[],
            &[],
            false,
            2,
            50,
            false,
            0,
            Some(30),
            None,
        )
        .into_string();
        assert!(
            html.contains(r"url=../ui/dead-letters?page=2"),
            "refresh target must preserve page=2: {html}"
        );
    }

    #[test]
    fn layout_schedules_includes_build_routing_nav_link() {
        let body = html! { p { "test" } };
        let html = layout_schedules("Test", &body, None, "", "").into_string();
        assert!(
            html.contains("build-routing"),
            "layout_schedules must include a Build Routing nav link"
        );
    }

    #[test]
    fn render_build_routing_page_empty_state_shows_docs_link() {
        let html = render_build_routing_page(&[], &[], &[], &[], &[], &[], false, None, None)
            .into_string();
        assert!(
            html.contains("No build routing configured") || html.contains("No build policies"),
            "empty state must show a 'no policies' message"
        );
        assert!(
            html.contains("safe-deploy"),
            "empty state must link to safe-deploy runbook"
        );
    }

    #[test]
    fn render_build_routing_page_shows_policy_details() {
        let policy = BuildPolicy {
            id: uuid::Uuid::new_v4(),
            queue_name: "test-queue".to_string(),
            build_id: "abc123".to_string(),
            deployment_name: Some("prod-v2".to_string()),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            target_build_id: None,
            ramp_percent: None,
        };
        let html = render_build_routing_page(&[policy], &[], &[], &[], &[], &[], false, None, None)
            .into_string();
        assert!(html.contains("test-queue"), "must show queue name");
        assert!(html.contains("abc123"), "must show build_id");
        assert!(html.contains("prod-v2"), "must show deployment name");
    }

    #[test]
    fn render_build_routing_page_shows_reachability() {
        let reach = BuildReachability {
            build_id: "sha-old".to_string(),
            open_executions: 42,
            pending_tasks: 5,
            active_workers: 2,
            stale_workers: 1,
            safe_to_retire: false,
        };
        let html = render_build_routing_page(&[], &[], &[reach], &[], &[], &[], false, None, None)
            .into_string();
        assert!(html.contains("sha-old"), "must show build_id");
        assert!(html.contains("42"), "must show open_executions count");
        assert!(
            html.contains("In use"),
            "non-safe build must show In use status"
        );
    }

    #[test]
    fn render_build_routing_page_retire_enabled_when_safe() {
        let reach = BuildReachability {
            build_id: "sha-done".to_string(),
            open_executions: 0,
            pending_tasks: 0,
            active_workers: 0,
            stale_workers: 0,
            safe_to_retire: true,
        };
        let html = render_build_routing_page(&[], &[], &[reach], &[], &[], &[], false, None, None)
            .into_string();
        assert!(
            html.contains("Retire"),
            "retire button must appear when safe_to_retire"
        );
        assert!(
            html.contains("Safe to retire"),
            "status must show Safe to retire"
        );
    }

    #[test]
    fn render_build_routing_page_shows_compat_entries() {
        let entry = BuildCompatEntry {
            id: uuid::Uuid::new_v4(),
            build_id: "sha-new".to_string(),
            compatible_with: "sha-old".to_string(),
            declared_at: chrono::Utc::now(),
        };
        let html = render_build_routing_page(&[], &[entry], &[], &[], &[], &[], false, None, None)
            .into_string();
        assert!(
            html.contains("sha-new"),
            "must show worker build in compat table"
        );
        assert!(
            html.contains("sha-old"),
            "must show compatible_with in compat table"
        );
        assert!(
            html.contains("Revoke"),
            "must show revoke button for each entry"
        );
    }

    #[test]
    fn render_build_routing_page_flash_message_shown() {
        let html = render_build_routing_page(
            &[],
            &[],
            &[],
            &[],
            &[],
            &[],
            false,
            Some("Policy updated"),
            None,
        )
        .into_string();
        assert!(
            html.contains("Policy updated"),
            "flash message must appear on page"
        );
    }

    #[test]
    fn render_build_routing_page_has_set_policy_form() {
        let html = render_build_routing_page(&[], &[], &[], &[], &[], &[], false, None, None)
            .into_string();
        assert!(
            html.contains("set-policy"),
            "page must include Set Policy form action"
        );
        assert!(
            html.contains("queue_name"),
            "Set Policy form must include queue_name field"
        );
        assert!(
            html.contains("build_id"),
            "Set Policy form must include build_id field"
        );
    }

    #[test]
    fn render_build_routing_page_has_declare_compat_form() {
        let html = render_build_routing_page(&[], &[], &[], &[], &[], &[], false, None, None)
            .into_string();
        assert!(
            html.contains("declare-compat"),
            "page must include Declare Compat form action"
        );
        assert!(
            html.contains("compatible_with"),
            "Declare Compat form must include compatible_with field"
        );
    }

    #[test]
    fn render_worker_table_includes_build_id_column() {
        let html = render_worker_table(&[], ShardId::new(0)).into_string();
        assert!(
            html.contains("Build ID"),
            "worker table header must include Build ID column"
        );
        assert!(
            html.contains("Deployment"),
            "worker table header must include Deployment column"
        );
    }

    #[test]
    fn render_worker_filters_includes_build_id_filter() {
        let html = render_worker_filters(
            None,
            "",
            None,
            "",
            None,
            false,
            "",
            None,
            None,
            DEFAULT_PAGE_SIZE,
        )
        .into_string();
        assert!(
            html.contains("build_id"),
            "worker filters must include build_id input"
        );
    }

    #[test]
    fn render_worker_filters_shows_inline_errors() {
        let html = render_worker_filters(
            None,
            "zombie",
            Some("Unknown status 'zombie'; expected Active, Draining, or Stopped. Filter not applied."),
            "north",
            Some("Invalid shard 'north'; expected a whole number. Filter not applied."),
            false,
            "True",
            Some("Unknown stale value 'True'; expected 'true' or 'false'. Filter not applied."),
            None,
            DEFAULT_PAGE_SIZE,
        )
        .into_string();
        assert!(
            html.contains("field-error") && html.contains("zombie"),
            "status error must render inline: {html}"
        );
        assert!(
            html.contains("north") && html.contains("Invalid shard"),
            "shard error must render inline: {html}"
        );
        assert!(
            html.contains("True"),
            "stale error must render inline: {html}"
        );
    }

    /// GREEN — the fix under test: the invalid raw value is echoed back as
    /// the `<select>`'s selected option (not silently reverted to "All"), so
    /// resubmitting the form unchanged resends the same bad value and the
    /// operator sees the same error again rather than it vanishing (Codex
    /// review, #1378 P2).
    #[test]
    fn render_worker_filters_echoes_invalid_raw_value_as_selected_option() {
        let html = render_worker_filters(
            None,
            "zombie",
            Some("Unknown status 'zombie'; expected Active, Draining, or Stopped. Filter not applied."),
            "",
            None,
            false,
            "",
            None,
            None,
            DEFAULT_PAGE_SIZE,
        )
        .into_string();
        assert!(
            html.contains("option value=\"zombie\" selected"),
            "the invalid status must be echoed back as the selected option: {html}"
        );
    }

    #[test]
    fn parse_worker_status_filter_accepts_known_values_case_insensitively() {
        assert_eq!(
            parse_worker_status_filter(Some("Active")),
            (Some("Active"), "Active".to_string(), None)
        );
        assert_eq!(
            parse_worker_status_filter(Some("draining")),
            (Some("Draining"), "draining".to_string(), None)
        );
        assert_eq!(
            parse_worker_status_filter(Some("STOPPED")),
            (Some("Stopped"), "STOPPED".to_string(), None)
        );
    }

    /// GREEN — the fix under test: an unrecognized status no longer aborts
    /// `list_workers_ui`. It degrades to "filter not applied" (parsed is
    /// `None`) while carrying the raw text and a recovery message, so the
    /// page can redisplay the form inline instead of discarding it — same
    /// contract as `parse_started_bound` on the Workflows page (#1333).
    /// Before this change, `parse_worker_ui_filters` `?`-propagated a bare
    /// `AutumnError::bad_request_msg` here, which aborted the whole
    /// `/workers` response before the filter form (or the `build_id`/`shard`
    /// filters the operator had already typed) was ever rendered — see the
    /// RED baseline in
    /// `tests/ui_integration.rs::ui_workers_unknown_status_value_redisplays_form_instead_of_aborting_page`.
    #[test]
    fn parse_worker_status_filter_rejects_unknown_value_without_erroring() {
        let (parsed, raw, error) = parse_worker_status_filter(Some("zombie"));
        assert_eq!(parsed, None, "an invalid status must not be applied");
        assert_eq!(
            raw, "zombie",
            "the operator's exact raw input is echoed back"
        );
        let error = error.expect("an invalid status must carry a redisplayable error");
        assert!(
            error.contains("zombie") && error.contains("Active"),
            "error names the bad value and a valid option: {error}"
        );
    }

    #[test]
    fn parse_worker_status_filter_blank_or_missing_is_not_an_error() {
        assert_eq!(
            parse_worker_status_filter(None),
            (None, String::new(), None)
        );
        assert_eq!(
            parse_worker_status_filter(Some("   ")),
            (None, String::new(), None)
        );
    }

    #[test]
    fn parse_worker_stale_filter_accepts_true_and_false() {
        assert_eq!(
            parse_worker_stale_filter(Some("true")),
            (true, "true".to_string(), None)
        );
        assert_eq!(
            parse_worker_stale_filter(Some("false")),
            (false, "false".to_string(), None)
        );
    }

    /// Same fix, applied to the `stale` field: an unrecognized value (e.g.
    /// the very plausible `True`, since matching is case-sensitive by
    /// design — see the field's existing semantics) no longer `?`-aborts the
    /// page.
    #[test]
    fn parse_worker_stale_filter_rejects_unknown_value_without_erroring() {
        let (parsed, raw, error) = parse_worker_stale_filter(Some("True"));
        assert!(!parsed, "an invalid stale value must not be applied");
        assert_eq!(raw, "True", "the operator's exact raw input is echoed back");
        let error = error.expect("an invalid stale value must carry a redisplayable error");
        assert!(
            error.contains("True") && error.contains("true"),
            "error names the bad value and the expected values: {error}"
        );
    }

    #[test]
    fn parse_worker_stale_filter_blank_or_missing_is_not_an_error() {
        assert_eq!(
            parse_worker_stale_filter(None),
            (false, String::new(), None)
        );
        assert_eq!(
            parse_worker_stale_filter(Some("   ")),
            (false, String::new(), None)
        );
    }

    #[test]
    fn build_worker_query_string_includes_build_id() {
        let q = build_worker_query_string(DEFAULT_PAGE_SIZE, "", "", "", Some("abc123"));
        assert!(
            q.contains("build_id=abc123"),
            "query string must include build_id"
        );
    }

    // ── Issue #957 — DAG run graph rendering (consumes dag_graph::build_run_graph) ──
    // `DagNodeStatus`/`DagRunNode`/`build_run_graph` come in via `super::*`
    // (the module-level `use crate::dag_graph::…`); only `DagNodeKind` is
    // test-only.

    use crate::dag_graph::DagNodeKind;

    const ALL_DAG_NODE_STATUSES: [DagNodeStatus; 8] = [
        DagNodeStatus::Succeeded,
        DagNodeStatus::Failed,
        DagNodeStatus::TimedOut,
        DagNodeStatus::Cancelled,
        DagNodeStatus::Running,
        DagNodeStatus::Pending,
        DagNodeStatus::Skipped,
        DagNodeStatus::Waiting,
    ];

    fn dag_run_node(name: &str, status: DagNodeStatus, depends_on: Vec<String>) -> DagRunNode {
        DagRunNode {
            node_name: name.to_string(),
            kind: DagNodeKind::Activity,
            status,
            depends_on,
            started_at: None,
            finished_at: None,
            attempts: 0,
            error_type: None,
            error: None,
        }
    }

    // P1
    #[test]
    fn dag_node_status_fill_covers_all_eight_variants() {
        let fills: Vec<&str> = ALL_DAG_NODE_STATUSES
            .iter()
            .map(|s| dag_node_status_fill(*s))
            .collect();
        for fill in &fills {
            assert!(
                fill.starts_with('#') && fill.len() >= 4,
                "fill must be a non-empty hex colour: {fill:?}"
            );
        }
        let unique: std::collections::HashSet<&&str> = fills.iter().collect();
        assert_eq!(
            unique.len(),
            8,
            "each status must have a distinct fill: {fills:?}"
        );
    }

    // P2
    #[test]
    fn dag_node_status_icon_distinguishes_without_color() {
        let icons: Vec<&str> = ALL_DAG_NODE_STATUSES
            .iter()
            .map(|s| dag_node_status_icon(*s))
            .collect();
        for icon in &icons {
            assert!(!icon.is_empty(), "icon must be non-empty");
        }
        let unique: std::collections::HashSet<&&str> = icons.iter().collect();
        assert_eq!(
            unique.len(),
            8,
            "each status must have a distinct glyph: {icons:?}"
        );
    }

    // P3
    #[test]
    fn dag_node_status_label_all_variants() {
        assert_eq!(dag_node_status_label(DagNodeStatus::TimedOut), "Timed out");
        assert_eq!(dag_node_status_label(DagNodeStatus::Waiting), "Waiting");
        assert_eq!(dag_node_status_label(DagNodeStatus::Pending), "Pending");
        assert_eq!(dag_node_status_label(DagNodeStatus::Skipped), "Skipped");
        let labels: Vec<&str> = ALL_DAG_NODE_STATUSES
            .iter()
            .map(|s| dag_node_status_label(*s))
            .collect();
        for label in &labels {
            assert!(!label.is_empty(), "label must be non-empty");
        }
        let unique: std::collections::HashSet<&&str> = labels.iter().collect();
        assert_eq!(
            unique.len(),
            8,
            "each status must have a distinct label: {labels:?}"
        );
    }

    // P4
    #[test]
    fn node_retry_offered_matrix() {
        // Terminal-failed run + attempted-not-succeeded node → offer retry.
        assert!(node_retry_offered("FAILED", DagNodeStatus::Failed));
        assert!(node_retry_offered("FAILED", DagNodeStatus::TimedOut));
        assert!(node_retry_offered("FAILED", DagNodeStatus::Cancelled));
        assert!(node_retry_offered("CANCELLED", DagNodeStatus::Failed));
        assert!(node_retry_offered("TIMED_OUT", DagNodeStatus::Failed));
        // Run not terminal-eligible → never offer.
        assert!(!node_retry_offered("COMPLETED", DagNodeStatus::Failed));
        assert!(!node_retry_offered("RUNNING", DagNodeStatus::Failed));
        // Node not a retry candidate → never offer.
        assert!(!node_retry_offered("FAILED", DagNodeStatus::Succeeded));
        assert!(!node_retry_offered("FAILED", DagNodeStatus::Pending));
        assert!(!node_retry_offered("FAILED", DagNodeStatus::Skipped));
        assert!(!node_retry_offered("FAILED", DagNodeStatus::Waiting));
        assert!(!node_retry_offered("FAILED", DagNodeStatus::Running));
    }

    // P5
    #[test]
    fn build_graph_layout_columns_by_level() {
        // 3-level linear chain → 3 monotonically increasing x-columns.
        let layout = build_graph_layout(&[vec![0], vec![1], vec![2]], 3);
        assert_eq!(layout.pos.len(), 3);
        assert!(layout.pos[0].0 < layout.pos[1].0);
        assert!(layout.pos[1].0 < layout.pos[2].0);

        // Fan-out level → same column x, distinct rows y.
        let layout2 = build_graph_layout(&[vec![0], vec![1, 2], vec![3]], 4);
        assert!(
            (layout2.pos[1].0 - layout2.pos[2].0).abs() < f64::EPSILON,
            "same-level nodes share a column x"
        );
        assert!(
            (layout2.pos[1].1 - layout2.pos[2].1).abs() > f64::EPSILON,
            "same-level nodes occupy distinct rows"
        );
        assert!(layout2.width > 0.0 && layout2.height > 0.0);
    }

    // P6
    #[test]
    fn build_graph_layout_is_deterministic() {
        let a = build_graph_layout(&[vec![0], vec![1, 2], vec![3]], 4);
        let b = build_graph_layout(&[vec![0], vec![1, 2], vec![3]], 4);
        assert_eq!(a.pos, b.pos);
        assert!((a.width - b.width).abs() < f64::EPSILON);
        assert!((a.height - b.height).abs() < f64::EPSILON);
    }

    // P7
    #[test]
    fn render_dag_run_graph_svg_contains_nodes_edges_fills() {
        let nodes = vec![
            dag_run_node("step_a", DagNodeStatus::Succeeded, vec![]),
            dag_run_node("step_b", DagNodeStatus::Failed, vec!["step_a".to_string()]),
        ];
        let levels = vec![vec![0], vec![1]];
        let upstreams = vec![vec![], vec![0]];
        let svg = render_dag_run_graph_svg(&nodes, &levels, &upstreams, None, uuid::Uuid::nil())
            .into_string();

        assert!(svg.contains("<svg"), "must be an inline svg");
        assert!(svg.contains("step_a"));
        assert!(svg.contains("step_b"));
        assert!(svg.contains(dag_node_status_fill(DagNodeStatus::Succeeded)));
        assert!(svg.contains(dag_node_status_fill(DagNodeStatus::Failed)));
        assert!(svg.contains("<line"), "edges rendered as <line>");
        assert!(svg.contains("<a "), "each node is a click-through link");
        assert!(svg.contains("?run="), "node links carry the run id");
        assert!(svg.contains("node=0"));
        assert!(svg.contains("node=1"));
    }

    // P8
    #[test]
    fn render_dag_run_graph_svg_escapes_node_names() {
        let nodes = vec![dag_run_node("<b>evil</b>", DagNodeStatus::Failed, vec![])];
        let svg = render_dag_run_graph_svg(&nodes, &[vec![0]], &[vec![]], None, uuid::Uuid::nil())
            .into_string();
        assert!(
            !svg.contains("<b>evil</b>"),
            "a markup-char node name must be escaped, not injected: {svg}"
        );
        assert!(svg.contains("&lt;b&gt;evil"), "escaped form present");
    }

    // P8b — accessibility: the per-status glyph is actually embedded in the
    // rendered node SVG (not just returned by the helper), so dropping the icon
    // from the render would fail this test. #957 AC: distinguishable without
    // colour alone.
    #[test]
    fn render_dag_run_graph_svg_embeds_status_icon_glyphs() {
        let nodes = vec![
            dag_run_node("step_ok", DagNodeStatus::Succeeded, vec![]),
            dag_run_node(
                "step_bad",
                DagNodeStatus::Failed,
                vec!["step_ok".to_string()],
            ),
        ];
        let svg = render_dag_run_graph_svg(
            &nodes,
            &[vec![0], vec![1]],
            &[vec![], vec![0]],
            None,
            uuid::Uuid::nil(),
        )
        .into_string();
        // The glyph rides on the node label text `(icon) " " (name)`.
        assert!(
            svg.contains(&format!(
                "{} step_ok",
                dag_node_status_icon(DagNodeStatus::Succeeded)
            )),
            "succeeded node label carries its icon glyph: {svg}"
        );
        assert!(
            svg.contains(&format!(
                "{} step_bad",
                dag_node_status_icon(DagNodeStatus::Failed)
            )),
            "failed node label carries its icon glyph: {svg}"
        );
    }

    // P9
    #[test]
    fn render_dag_node_panel_failed_shows_error_and_retry_link() {
        let mut node = dag_run_node("step_b", DagNodeStatus::Failed, vec!["step_a".to_string()]);
        node.attempts = 2;
        node.error_type = Some("S3Error".to_string());
        node.error = Some("transient S3 500".to_string());
        let panel =
            render_dag_node_panel(&node, 1, "FAILED", "graph_linear_dag", uuid::Uuid::nil())
                .into_string();

        assert!(panel.contains("S3Error"));
        assert!(panel.contains("transient S3 500"));
        assert!(panel.contains("/retry"), "retry link present: {panel}");
        assert!(
            panel.contains("from_node=step_b"),
            "retry link names the source node: {panel}"
        );
    }

    // P10
    #[test]
    fn render_dag_node_panel_no_retry_when_not_offered() {
        // Succeeded node on a FAILED run → no retry.
        let ok_node = dag_run_node("step_a", DagNodeStatus::Succeeded, vec![]);
        let ok_panel =
            render_dag_node_panel(&ok_node, 0, "FAILED", "d", uuid::Uuid::nil()).into_string();
        assert!(!ok_panel.contains("/retry"));

        // Failed node on a RUNNING run → no retry.
        let live_node = dag_run_node("step_a", DagNodeStatus::Failed, vec![]);
        let live_panel =
            render_dag_node_panel(&live_node, 0, "RUNNING", "d", uuid::Uuid::nil()).into_string();
        assert!(!live_panel.contains("/retry"));
    }

    // P11
    #[test]
    fn render_dag_node_panel_pending_vs_skipped_distinct() {
        let pending = dag_run_node("n", DagNodeStatus::Pending, vec![]);
        let skipped = dag_run_node("n", DagNodeStatus::Skipped, vec![]);
        let pending_panel =
            render_dag_node_panel(&pending, 0, "RUNNING", "d", uuid::Uuid::nil()).into_string();
        let skipped_panel =
            render_dag_node_panel(&skipped, 0, "COMPLETED", "d", uuid::Uuid::nil()).into_string();
        assert!(pending_panel.contains(dag_node_status_label(DagNodeStatus::Pending)));
        assert!(skipped_panel.contains(dag_node_status_label(DagNodeStatus::Skipped)));
        assert_ne!(
            dag_node_status_label(DagNodeStatus::Pending),
            dag_node_status_label(DagNodeStatus::Skipped)
        );
    }

    // P12
    #[test]
    fn dag_retry_success_flash_names_new_run() {
        let flash = dag_retry_success_flash("new-run-abc-123");
        assert!(
            flash.contains("new-run-abc-123"),
            "success flash must name the new run id: {flash}"
        );
    }

    // Codex review (issue #957): a successful fork opens the NEW run.
    #[test]
    fn dag_retry_commit_redirect_opens_new_run_on_success() {
        let plan = DagRetryResponse {
            dry_run: false,
            dag_name: "graph_linear".to_string(),
            source_run_exec_id: "source-run".to_string(),
            reset_to_event_id: 3,
            nodes_to_re_execute: vec!["step_b".to_string()],
            nodes_carried_over: vec!["step_a".to_string()],
            new_run_exec_id: Some("forked-run-xyz".to_string()),
            events_carried_over: Some(2),
        };
        let (target, flash) = dag_retry_commit_redirect(Ok(plan), "source-run");
        assert_eq!(target, "forked-run-xyz", "success must open the new run");
        assert!(
            flash.contains("forked-run-xyz"),
            "success flash must name the new run: {flash}"
        );
        assert!(
            !flash.contains("Retry failed"),
            "success must not use the failure message: {flash}"
        );
    }

    // Codex review (issue #957): AuditFailed is a PARTIAL SUCCESS — the fork
    // committed, so redirect to the NEW run with a distinct warning, never the
    // hard "Retry failed" pointing back at the source (which would hide the run
    // that actually started).
    #[test]
    fn dag_retry_commit_redirect_surfaces_new_run_on_audit_failure() {
        let failure = DagRetryFailure::AuditFailed {
            message: "audit insert failed: boom".to_string(),
            new_exec_id: "forked-run-xyz".to_string(),
        };
        let (target, flash) = dag_retry_commit_redirect(Err(failure), "source-run");
        assert_eq!(
            target, "forked-run-xyz",
            "audit failure must still open the NEW forked run, not the source"
        );
        assert_ne!(
            target, "source-run",
            "audit failure must not redirect back to the source run"
        );
        assert!(
            flash.contains("forked-run-xyz"),
            "warning flash must name the new run so the operator can find it: {flash}"
        );
        assert!(
            !flash.contains("Retry failed"),
            "audit failure is a partial success and must NOT use the hard failure message: {flash}"
        );
        assert!(
            flash.contains("audit record"),
            "warning flash must call out the missing audit record: {flash}"
        );
    }

    // Codex review (issue #957): a genuine failure (400/404/409) redirects back
    // to the SOURCE run with the hard "Retry failed" message.
    #[test]
    fn dag_retry_commit_redirect_opens_source_run_on_real_failure() {
        let failure = DagRetryFailure::StateConflict("DAG run succeeded".to_string());
        let (target, flash) = dag_retry_commit_redirect(Err(failure), "source-run");
        assert_eq!(
            target, "source-run",
            "a real failure must redirect back to the source run"
        );
        assert!(
            flash.contains("Retry failed"),
            "a real failure must use the hard failure message: {flash}"
        );
    }

    #[test]
    fn dag_detail_relative_url_builds_back_link_and_flash() {
        assert_eq!(
            dag_detail_relative_url("graph_linear", "run-1", None),
            "../../../graph_linear?run=run-1"
        );
        assert_eq!(
            dag_detail_relative_url("graph_linear", "run-2", Some("done")),
            "../../../graph_linear?run=run-2&flash=done"
        );
        // A `run` value carrying reserved chars (e.g. a raw path segment that
        // failed to parse as an exec id on the retry-commit Err branch) must be
        // percent-encoded — mirroring `dag_name` — so it cannot alter the
        // redirect URL's query/fragment semantics (security P2-1).
        assert_eq!(
            dag_detail_relative_url("graph_linear", "run#x&y?z", None),
            "../../../graph_linear?run=run%23x%26y%3Fz"
        );
    }

    // ── Issue #960 — execution timeline / Gantt view ───────────────────────
    //
    // Pure render/geometry unit tests (run without Docker). The Gantt consumes
    // the shipped `autumn_harvest::derive_timeline` output plus the execution
    // row's pause/ND-block columns — it never forks history-derivation.

    use autumn_harvest::{
        SlowestStep, StepKind, StepOutcome, Timeline, TimelineRollup, TimelineStep,
    };

    const ALL_STEP_OUTCOMES: [StepOutcome; 6] = [
        StepOutcome::Completed,
        StepOutcome::Failed,
        StepOutcome::TimedOut,
        StepOutcome::Cancelled,
        StepOutcome::Fired,
        StepOutcome::Pending,
    ];

    const ALL_STEP_KINDS: [StepKind; 6] = [
        StepKind::Activity,
        StepKind::LocalActivity,
        StepKind::Timer,
        StepKind::ChildWorkflow,
        StepKind::SignalWait,
        StepKind::SideEffect,
    ];

    fn tl_base() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-07-15T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[allow(clippy::too_many_arguments)]
    fn tl_step(
        kind: StepKind,
        name: Option<&str>,
        sched_off_ms: i64,
        end_off_ms: Option<i64>,
        wait_ms: Option<i64>,
        exec_ms: Option<i64>,
        outcome: StepOutcome,
        attempt: Option<i32>,
    ) -> TimelineStep {
        let base = tl_base();
        let scheduled_at = base + chrono::Duration::milliseconds(sched_off_ms);
        let ended_at = end_off_ms.map(|o| base + chrono::Duration::milliseconds(o));
        let total_ms = end_off_ms.map_or(0, |e| (e - sched_off_ms).max(0));
        TimelineStep {
            step_kind: kind,
            name: name.map(ToString::to_string),
            scheduled_at,
            ended_at,
            total_ms,
            wait_ms,
            exec_ms,
            outcome,
            attempt,
        }
    }

    fn tl_rollup(slowest: Option<SlowestStep>) -> TimelineRollup {
        TimelineRollup {
            total_wall_clock_ms: 12_000,
            busy_ms: 8_000,
            wait_ms: 3_000,
            slowest_step: slowest,
            step_count_by_kind: std::collections::BTreeMap::new(),
        }
    }

    fn tl_timeline(steps: Vec<TimelineStep>, slowest: Option<SlowestStep>) -> Timeline {
        Timeline {
            exec_id: "exec-1".to_string(),
            workflow_id: "wf-1".to_string(),
            workflow_name: "order_flow".to_string(),
            state: "RUNNING".to_string(),
            steps,
            rollup: tl_rollup(slowest),
        }
    }

    // Q1
    #[test]
    fn step_outcome_fill_and_label_all_variants() {
        let fills: Vec<&str> = ALL_STEP_OUTCOMES
            .iter()
            .map(|o| step_outcome_fill(*o))
            .collect();
        for fill in &fills {
            assert!(
                fill.starts_with('#') && fill.len() >= 4,
                "outcome fill must be non-empty hex: {fill:?}"
            );
        }
        let unique: std::collections::HashSet<&&str> = fills.iter().collect();
        assert_eq!(
            unique.len(),
            6,
            "each outcome has a distinct fill: {fills:?}"
        );

        let labels: Vec<&str> = ALL_STEP_OUTCOMES
            .iter()
            .map(|o| step_outcome_label(*o))
            .collect();
        let unique_labels: std::collections::HashSet<&&str> = labels.iter().collect();
        assert_eq!(unique_labels.len(), 6, "distinct labels: {labels:?}");
        assert!(labels.iter().all(|l| !l.is_empty()));
    }

    // Q2
    #[test]
    fn timeline_lanes_fixed_order_and_labels() {
        // Fixed lane order, matching the AC's step_kind list.
        assert_eq!(
            TIMELINE_LANES,
            [
                StepKind::Activity,
                StepKind::LocalActivity,
                StepKind::ChildWorkflow,
                StepKind::Timer,
                StepKind::SignalWait,
                StepKind::SideEffect,
            ]
        );
        let labels: Vec<&str> = ALL_STEP_KINDS
            .iter()
            .map(|k| step_kind_lane_label(*k))
            .collect();
        let unique: std::collections::HashSet<&&str> = labels.iter().collect();
        assert_eq!(unique.len(), 6, "distinct lane labels: {labels:?}");
        assert!(labels.iter().all(|l| !l.is_empty()));
    }

    // Q3
    #[test]
    fn x_scale_maps_endpoints() {
        assert!((x_scale(0, 1000, 800.0) - 0.0).abs() < f64::EPSILON);
        assert!((x_scale(1000, 1000, 800.0) - 800.0).abs() < f64::EPSILON);
        assert!((x_scale(500, 1000, 800.0) - 400.0).abs() < f64::EPSILON);
    }

    // Q4
    #[test]
    fn x_scale_zero_duration_guard() {
        // span_ms == 0 must not divide by zero / NaN / panic.
        let a = x_scale(0, 0, 800.0);
        let b = x_scale(5, 0, 800.0);
        assert!(a.is_finite() && b.is_finite(), "no NaN/inf on zero span");
        // Clamped inside the axis regardless.
        assert!((0.0..=800.0).contains(&x_scale(9_000, 1_000, 800.0)));
        assert!((0.0..=800.0).contains(&x_scale(-9_000, 1_000, 800.0)));
    }

    // Q5a
    #[test]
    fn span_segments_split_when_both_present() {
        let step = tl_step(
            StepKind::Activity,
            Some("charge"),
            0,
            Some(1000),
            Some(300),
            Some(700),
            StepOutcome::Completed,
            Some(1),
        );
        let segs = span_segments(&step);
        assert_eq!(segs.len(), 2);
        assert_eq!(segs[0], (SegKind::Wait, 300));
        assert_eq!(segs[1], (SegKind::Exec, 700));
        assert_eq!(segs.iter().map(|(_, ms)| *ms).sum::<i64>(), 1000);
    }

    // Q5b
    #[test]
    fn span_segments_single_when_absent() {
        // No split recorded → one undivided Whole segment (never fabricated).
        let child = tl_step(
            StepKind::ChildWorkflow,
            Some("sub"),
            0,
            Some(500),
            None,
            None,
            StepOutcome::Completed,
            None,
        );
        assert_eq!(span_segments(&child), vec![(SegKind::Whole, 500)]);

        // Only one of wait/exec present → still a single Whole span.
        let half = tl_step(
            StepKind::Activity,
            Some("a"),
            0,
            Some(400),
            Some(100),
            None,
            StepOutcome::Completed,
            Some(1),
        );
        assert_eq!(span_segments(&half), vec![(SegKind::Whole, 400)]);
    }

    // Q6
    #[test]
    fn format_ms_units() {
        assert!(format_ms(0).contains("ms"));
        assert!(format_ms(500).contains("ms"));
        assert!(format_ms(1500).contains('s') && !format_ms(1500).contains("ms"));
        let long = format_ms(90_000);
        assert!(long.contains('m') && long.contains('s'), "minutes: {long}");
    }

    // Q7
    #[test]
    fn render_timeline_gantt_lanes_and_spans() {
        let steps = vec![
            tl_step(
                StepKind::Activity,
                Some("charge"),
                0,
                Some(1000),
                Some(300),
                Some(700),
                StepOutcome::Completed,
                Some(1),
            ),
            tl_step(
                StepKind::Timer,
                Some("wait_24h"),
                1000,
                Some(2000),
                None,
                None,
                StepOutcome::Fired,
                None,
            ),
            tl_step(
                StepKind::ChildWorkflow,
                Some("fulfill"),
                2000,
                Some(3000),
                None,
                None,
                StepOutcome::Completed,
                None,
            ),
            tl_step(
                StepKind::SignalWait,
                Some("approve"),
                3000,
                Some(4000),
                None,
                None,
                StepOutcome::Completed,
                None,
            ),
        ];
        let mut exec = stub_execution();
        exec.started_at = tl_base();
        exec.completed_at = Some(tl_base() + chrono::Duration::milliseconds(4000));
        exec.state = "COMPLETED".to_string();
        let timeline = tl_timeline(steps, None);
        let html = render_timeline_gantt(&timeline, &exec, tl_base()).into_string();

        assert!(html.contains("<svg"), "inline svg present");
        // A lane label per present kind.
        assert!(html.contains(step_kind_lane_label(StepKind::Activity)));
        assert!(html.contains(step_kind_lane_label(StepKind::Timer)));
        assert!(html.contains(step_kind_lane_label(StepKind::ChildWorkflow)));
        assert!(html.contains(step_kind_lane_label(StepKind::SignalWait)));
        // A span per step (by name).
        assert!(html.contains("charge"));
        assert!(html.contains("wait_24h"));
        assert!(html.contains("fulfill"));
        assert!(html.contains("approve"));
        // Outcome fills present.
        assert!(html.contains(step_outcome_fill(StepOutcome::Completed)));
        assert!(html.contains(step_outcome_fill(StepOutcome::Fired)));
    }

    // Codex review (#960): the timeline page's "Back to execution" control and
    // the current-details card must both resolve to the execution-detail route
    // `{api_base}/workflows/{id}` — NOT `{api_base}/workflows/{id}/{id}`. Both
    // are built from the api base (`../../workflows/{id}`), matching the page's
    // own `base_href = "../../"` nav, rather than a bare `../{id}` that relies on
    // `..` stripping exactly the `timeline` segment.
    #[test]
    fn timeline_nav_links_point_at_execution_detail_route() {
        let steps = vec![tl_step(
            StepKind::Activity,
            Some("charge"),
            0,
            Some(1000),
            Some(300),
            Some(700),
            StepOutcome::Completed,
            Some(1),
        )];
        let mut exec = stub_execution();
        exec.started_at = tl_base();
        exec.completed_at = Some(tl_base() + chrono::Duration::milliseconds(1000));
        // Non-empty current_details makes the "what is it blocked on?" card render.
        exec.current_details = Some("charging card".to_string());
        let timeline = tl_timeline(steps, None); // Timeline::exec_id == "exec-1"
        let html = render_timeline_body(&timeline, &exec, tl_base()).into_string();

        // Both nav links (back control + current-details card) resolve to the
        // execution-detail route via the same api-base-relative href.
        let want = r#"href="../../workflows/exec-1""#;
        assert_eq!(
            html.matches(want).count(),
            2,
            "both the back link and the current-details card must use {want:?}: {html}"
        );
        assert!(
            html.contains(" Back to execution"),
            "back control present: {html}"
        );
        assert!(
            html.contains("what is it blocked on?"),
            "current-details card link present: {html}"
        );

        // The old bare-`..` form (which relied on `..` stripping exactly the
        // `timeline` segment) must be gone, and the href must NEVER produce the
        // doubled `/{id}/{id}` execution segment Codex flagged.
        assert!(
            !html.contains(r#"href="../exec-1""#),
            "must not use the fragile bare `../{{id}}` form: {html}"
        );
        assert!(
            !html.contains("workflows/exec-1/exec-1"),
            "href must not resolve to a doubled execution segment: {html}"
        );
    }

    // Q8
    #[test]
    fn render_timeline_gantt_split_only_when_present() {
        let split = tl_step(
            StepKind::Activity,
            Some("charge"),
            0,
            Some(1000),
            Some(300),
            Some(700),
            StepOutcome::Completed,
            Some(1),
        );
        let whole = tl_step(
            StepKind::ChildWorkflow,
            Some("sub"),
            1000,
            Some(1500),
            None,
            None,
            StepOutcome::Completed,
            None,
        );
        let mut exec = stub_execution();
        exec.started_at = tl_base();
        exec.completed_at = Some(tl_base() + chrono::Duration::milliseconds(1500));
        let timeline = tl_timeline(vec![split, whole], None);
        let html = render_timeline_gantt(&timeline, &exec, tl_base()).into_string();
        // Split activity → both wait and exec segment classes.
        assert!(
            html.contains("gantt-seg-wait"),
            "wait segment rendered: {html}"
        );
        assert!(html.contains("gantt-seg-exec"), "exec segment rendered");
        // Un-split child → the undivided whole class.
        assert!(html.contains("gantt-seg-whole"), "whole span rendered");
    }

    // Q9
    #[test]
    fn render_timeline_gantt_open_span_to_now() {
        // In-flight step: ended_at = None, outcome Pending → open-ended span.
        let pending = tl_step(
            StepKind::Activity,
            Some("running"),
            0,
            None,
            None,
            None,
            StepOutcome::Pending,
            Some(1),
        );
        let mut exec = stub_execution();
        exec.started_at = tl_base();
        exec.completed_at = None;
        exec.state = "RUNNING".to_string();
        let timeline = tl_timeline(vec![pending], None);
        let now = tl_base() + chrono::Duration::milliseconds(5000);
        let html = render_timeline_gantt(&timeline, &exec, now).into_string();
        assert!(html.contains("gantt-span-open"), "open span styled: {html}");
        // Regression: an open step (total_ms == 0 in the fixture) must still render
        // at its axis-derived width, not collapse to width="0". Scheduled at the
        // axis start and open to `now` == full TL_AXIS_W (900px).
        assert!(
            html.contains("width=\"900\""),
            "open span fills its geometric extent, not the (zero) total_ms: {html}"
        );
    }

    // Q10
    #[test]
    fn render_timeline_gantt_attempt_badge() {
        let retried = tl_step(
            StepKind::Activity,
            Some("flaky"),
            0,
            Some(1000),
            Some(100),
            Some(900),
            StepOutcome::Completed,
            Some(3),
        );
        let mut exec = stub_execution();
        exec.started_at = tl_base();
        exec.completed_at = Some(tl_base() + chrono::Duration::milliseconds(1000));
        let timeline = tl_timeline(vec![retried], None);
        let html = render_timeline_gantt(&timeline, &exec, tl_base()).into_string();
        assert!(html.contains("×3"), "attempt badge visible: {html}");
    }

    // Q11a
    #[test]
    fn pause_band_present_when_paused() {
        let mut exec = stub_execution();
        exec.started_at = tl_base();
        exec.paused_at = Some(tl_base() + chrono::Duration::milliseconds(2000));
        exec.pause_reason = Some("incident-4821".to_string());
        exec.pause_actor = Some("oncall@corp".to_string());
        let axis = GanttAxis::new(
            tl_base(),
            tl_base() + chrono::Duration::milliseconds(6000),
            220.0,
            900.0,
            300.0,
        );
        let band = pause_band_markup(&exec, &axis).expect("band present when paused");
        let html = band.into_string();
        assert!(html.contains("gantt-pause-band"), "band rect class: {html}");
        assert!(html.contains("incident-4821"), "reason labelled");
        assert!(html.contains("oncall@corp"), "actor labelled");
    }

    // Q11b
    #[test]
    fn pause_band_absent_when_not_paused() {
        let mut exec = stub_execution();
        exec.started_at = tl_base();
        exec.paused_at = None;
        let axis = GanttAxis::new(
            tl_base(),
            tl_base() + chrono::Duration::milliseconds(6000),
            220.0,
            900.0,
            300.0,
        );
        assert!(pause_band_markup(&exec, &axis).is_none());
    }

    // Q12a
    #[test]
    fn nd_marker_present_when_blocked() {
        let mut exec = stub_execution();
        exec.started_at = tl_base();
        exec.nd_blocked_at = Some(tl_base() + chrono::Duration::milliseconds(3000));
        exec.nd_block_reason = Some("expected ActivityScheduled got TimerStarted".to_string());
        exec.nd_block_count = 4;
        let axis = GanttAxis::new(
            tl_base(),
            tl_base() + chrono::Duration::milliseconds(6000),
            220.0,
            900.0,
            300.0,
        );
        let marker = nd_marker_markup(&exec, &axis).expect("marker present when nd-blocked");
        let html = marker.into_string();
        assert!(html.contains("gantt-nd-marker"), "marker class: {html}");
        assert!(html.contains("expected ActivityScheduled"), "reason shown");
        assert!(
            html.contains("nondeterminism-block"),
            "runbook path surfaced (as text, not a link): {html}"
        );
        assert!(
            !html.contains("<a "),
            "runbook path is not a clickable link (would 404 during triage): {html}"
        );
    }

    // Q12b
    #[test]
    fn nd_marker_absent_when_not_blocked() {
        let mut exec = stub_execution();
        exec.started_at = tl_base();
        exec.nd_blocked_at = None;
        let axis = GanttAxis::new(
            tl_base(),
            tl_base() + chrono::Duration::milliseconds(6000),
            220.0,
            900.0,
            300.0,
        );
        assert!(nd_marker_markup(&exec, &axis).is_none());
    }

    // Q12c — operator-controlled `pause_reason`/`pause_actor`/`nd_block_reason`
    // are assembled into SVG `<text>` bodies via `String::push_str`; prove maud
    // still HTML-escapes them (a future refactor to `PreEscaped`/an attribute
    // would regress this). Security review nit-4.
    #[test]
    fn pause_and_nd_reason_are_html_escaped_in_svg_text() {
        let mut exec = stub_execution();
        exec.started_at = tl_base();
        exec.paused_at = Some(tl_base() + chrono::Duration::milliseconds(1000));
        exec.pause_reason = Some("<script>alert(1)</script>".to_string());
        exec.pause_actor = Some("<b>bad</b>".to_string());
        exec.nd_blocked_at = Some(tl_base() + chrono::Duration::milliseconds(2000));
        exec.nd_block_reason = Some("<img src=x onerror=1>".to_string());
        let axis = GanttAxis::new(
            tl_base(),
            tl_base() + chrono::Duration::milliseconds(6000),
            220.0,
            900.0,
            300.0,
        );

        let pause = pause_band_markup(&exec, &axis)
            .expect("band present")
            .into_string();
        assert!(
            !pause.contains("<script>alert(1)</script>"),
            "pause reason must be escaped, not injected into the SVG <text>: {pause}"
        );
        assert!(
            !pause.contains("<b>bad</b>"),
            "pause actor must be escaped: {pause}"
        );
        assert!(
            pause.contains("&lt;script&gt;alert(1)&lt;/script&gt;"),
            "escaped pause reason present: {pause}"
        );

        let nd = nd_marker_markup(&exec, &axis)
            .expect("marker present")
            .into_string();
        assert!(
            !nd.contains("<img src=x onerror=1>"),
            "nd reason must be escaped in the SVG <text>: {nd}"
        );
        assert!(
            nd.contains("&lt;img src=x onerror=1&gt;"),
            "escaped nd reason present: {nd}"
        );
    }

    // Q13
    #[test]
    fn render_timeline_rollup_totals_and_slowest() {
        let rollup = TimelineRollup {
            total_wall_clock_ms: 42_000,
            busy_ms: 30_000,
            wait_ms: 9_000,
            slowest_step: Some(SlowestStep {
                name: Some("bottleneck_activity".to_string()),
                step_kind: StepKind::Activity,
                total_ms: 25_000,
            }),
            step_count_by_kind: std::collections::BTreeMap::new(),
        };
        let html = render_timeline_rollup(&rollup).into_string();
        assert!(html.contains(&format_ms(42_000)), "total wall-clock shown");
        assert!(html.contains(&format_ms(30_000)), "busy total shown");
        assert!(html.contains(&format_ms(9_000)), "wait total shown");
        assert!(html.contains("bottleneck_activity"), "slowest name shown");
        assert!(html.contains(&format_ms(25_000)), "slowest duration shown");
    }

    // Q14
    #[test]
    fn slowest_step_highlighted_with_id() {
        let steps = vec![
            tl_step(
                StepKind::Activity,
                Some("quick"),
                0,
                Some(100),
                Some(10),
                Some(90),
                StepOutcome::Completed,
                Some(1),
            ),
            tl_step(
                StepKind::Activity,
                Some("slow_one"),
                100,
                Some(9100),
                Some(50),
                Some(9050),
                StepOutcome::Completed,
                Some(1),
            ),
        ];
        // slowest_step_index picks the max total_ms step (index 1).
        let timeline = tl_timeline(
            steps,
            Some(SlowestStep {
                name: Some("slow_one".to_string()),
                step_kind: StepKind::Activity,
                total_ms: 9000,
            }),
        );
        assert_eq!(slowest_step_index(&timeline), Some(1));
        let mut exec = stub_execution();
        exec.started_at = tl_base();
        exec.completed_at = Some(tl_base() + chrono::Duration::milliseconds(9100));
        let html = render_timeline_gantt(&timeline, &exec, tl_base()).into_string();
        assert!(
            html.contains("id=\"slowest\""),
            "slowest anchor id present: {html}"
        );
        assert!(
            html.contains("gantt-span-slowest"),
            "slowest highlight class present"
        );
    }

    // Q15
    #[test]
    fn render_timeline_gantt_escapes_names() {
        let step = tl_step(
            StepKind::Activity,
            Some("<b>evil</b>"),
            0,
            Some(100),
            None,
            None,
            StepOutcome::Completed,
            None,
        );
        let mut exec = stub_execution();
        exec.started_at = tl_base();
        exec.completed_at = Some(tl_base() + chrono::Duration::milliseconds(100));
        let timeline = tl_timeline(vec![step], None);
        let html = render_timeline_gantt(&timeline, &exec, tl_base()).into_string();
        assert!(
            !html.contains("<b>evil</b>"),
            "a markup-char step name must be escaped: {html}"
        );
        assert!(html.contains("&lt;b&gt;evil"), "escaped form present");
    }

    // Q16
    #[test]
    fn timeline_step_cap_truncates_and_notes() {
        let n = MAX_RENDER_STEPS + 25;
        let steps: Vec<TimelineStep> = (0..n)
            .map(|i| {
                let off = i64::try_from(i).unwrap() * 10;
                tl_step(
                    StepKind::Activity,
                    Some("s"),
                    off,
                    Some(off + 5),
                    None,
                    None,
                    StepOutcome::Completed,
                    Some(1),
                )
            })
            .collect();
        let mut exec = stub_execution();
        exec.started_at = tl_base();
        exec.completed_at =
            Some(tl_base() + chrono::Duration::milliseconds(i64::try_from(n).unwrap() * 10));
        let timeline = tl_timeline(steps, None);
        let html = render_timeline_gantt(&timeline, &exec, tl_base()).into_string();
        // A "showing N of M" note when capped.
        assert!(
            html.contains(&MAX_RENDER_STEPS.to_string()) && html.contains(&n.to_string()),
            "truncation note names both the cap and the full count: {}",
            &html[..html.len().min(400)]
        );
    }

    // Q17 (#960): when the step list is capped, the slowest step must stay in the
    // rendered set even though lane grouping can push it past the prefix — the
    // rollup and the execution-detail Timeline link both advertise `#slowest`, so
    // the `id="slowest"` element must always resolve.
    #[test]
    fn timeline_cap_keeps_slowest_step_rendered() {
        // MAX_RENDER_STEPS + 1 short Activity steps fill the whole first-lane
        // prefix, then a single slow SideEffect step (last lane) is lane-grouped
        // *after* every Activity — beyond the rendered prefix.
        let mut steps: Vec<TimelineStep> = (0..=MAX_RENDER_STEPS)
            .map(|i| {
                let off = i64::try_from(i).unwrap() * 10;
                tl_step(
                    StepKind::Activity,
                    Some("act"),
                    off,
                    Some(off + 5),
                    None,
                    None,
                    StepOutcome::Completed,
                    Some(1),
                )
            })
            .collect();
        // The slowest step: distinctly named, largest total_ms, last (SideEffect)
        // lane. Its index in `steps` is > MAX_RENDER_STEPS, and lane grouping
        // places it after all Activities, so it falls outside the prefix.
        steps.push(tl_step(
            StepKind::SideEffect,
            Some("SLOWEST_STEP"),
            0,
            Some(999_999),
            None,
            None,
            StepOutcome::Completed,
            None,
        ));
        let mut exec = stub_execution();
        exec.started_at = tl_base();
        exec.completed_at = Some(tl_base() + chrono::Duration::milliseconds(999_999));
        exec.state = "COMPLETED".to_string();
        let timeline = tl_timeline(steps, None);
        // Sanity: the slowest step is genuinely the SideEffect, and it is past the
        // rendered prefix (so the pre-fix code would have dropped its anchor).
        assert_eq!(slowest_step_index(&timeline), Some(MAX_RENDER_STEPS + 1));

        let html = render_timeline_gantt(&timeline, &exec, tl_base()).into_string();

        // Exactly one `#slowest` anchor is emitted — never zero (dangling link),
        // never more than one.
        assert_eq!(
            html.matches("id=\"slowest\"").count(),
            1,
            "exactly one id=\"slowest\" element: {}",
            &html[..html.len().min(400)]
        );
        // The anchored `<g id="slowest">…</g>` belongs to the actual slowest step:
        // its distinctly-named title is inside the anchored group (and would be
        // absent entirely without the fix, being past the prefix).
        let anchor = html.find("id=\"slowest\"").expect("anchor present");
        let group_end = html[anchor..].find("</g>").expect("anchored group closes");
        let anchored_group = &html[anchor..anchor + group_end];
        assert!(
            anchored_group.contains("SLOWEST_STEP"),
            "the #slowest anchor wraps the slowest step's span: {anchored_group}"
        );
        // The truncation note is accurate about the extra row.
        assert!(
            html.contains("(plus the slowest)"),
            "truncation note flags the appended slowest step: {}",
            &html[..html.len().min(600)]
        );
    }

    // Q18 (#960): under the cap the slowest step is rendered in place, so the
    // `#slowest` anchor resolves without any special-casing.
    #[test]
    fn timeline_slowest_anchor_present_under_cap() {
        let steps = vec![
            tl_step(
                StepKind::Activity,
                Some("quick"),
                0,
                Some(10),
                None,
                None,
                StepOutcome::Completed,
                Some(1),
            ),
            tl_step(
                StepKind::ChildWorkflow,
                Some("BIGGEST"),
                10,
                Some(5_000),
                None,
                None,
                StepOutcome::Completed,
                None,
            ),
        ];
        let mut exec = stub_execution();
        exec.started_at = tl_base();
        exec.completed_at = Some(tl_base() + chrono::Duration::milliseconds(5_000));
        exec.state = "COMPLETED".to_string();
        let timeline = tl_timeline(steps, None);
        let html = render_timeline_gantt(&timeline, &exec, tl_base()).into_string();
        assert_eq!(
            html.matches("id=\"slowest\"").count(),
            1,
            "exactly one id=\"slowest\" element under the cap"
        );
        // No "(plus the slowest)" note — the slowest is in place, not appended.
        assert!(!html.contains("(plus the slowest)"));
        let anchor = html.find("id=\"slowest\"").expect("anchor present");
        let group_end = html[anchor..].find("</g>").expect("group closes");
        assert!(
            html[anchor..anchor + group_end].contains("BIGGEST"),
            "the #slowest anchor wraps the actual slowest (child) step"
        );
    }

    // -----------------------------------------------------------------------
    // Durable workflow-logs panel (issue #790, AC5)
    // -----------------------------------------------------------------------

    fn stub_log_line(
        seq: i64,
        level: &str,
        message: &str,
    ) -> autumn_harvest::models::HarvestWorkflowLog {
        autumn_harvest::models::HarvestWorkflowLog {
            id: seq,
            workflow_exec_id: uuid::Uuid::nil(),
            seq,
            level: level.to_string(),
            message: message.to_string(),
            occurred_at: chrono::Utc::now(),
        }
    }

    fn render_logs_detail(logs: &WorkflowLogsPanelData<'_>) -> String {
        let execution = stub_execution();
        let blocked = stub_blocked_on();
        render_workflow_detail(
            &execution,
            0,
            &[],
            &[],
            &[],
            false,
            &[],
            0,
            &blocked,
            None,
            None,
            logs,
        )
        .into_string()
    }

    #[test]
    fn logs_panel_is_not_rendered_for_a_non_admin() {
        // Mirrors the API route's `require_admin`: a log message is free-form
        // author text, so a non-admin must not read through the UI what the
        // API would deny. `admin: false` is the `Default`, which is also what
        // every non-log detail test passes.
        let lines = [stub_log_line(0, "info", "SENSITIVE-BUSINESS-DETAIL")];
        let html = render_logs_detail(&WorkflowLogsPanelData {
            lines: &lines,
            admin: false,
            ..Default::default()
        });
        assert!(!html.contains("SENSITIVE-BUSINESS-DETAIL"));
        assert!(
            !html.contains(">Logs<"),
            "the panel heading must not render for a non-admin"
        );
    }

    #[test]
    fn logs_panel_renders_lines_in_order_for_an_admin() {
        let lines = [
            stub_log_line(0, "info", "first-line"),
            stub_log_line(1, "warn", "second-line"),
            stub_log_line(2, "error", "third-line"),
        ];
        let html = render_logs_detail(&WorkflowLogsPanelData {
            lines: &lines,
            admin: true,
            ..Default::default()
        });
        assert!(html.contains(">Logs<"), "the panel heading must render");
        let first = html.find("first-line").expect("first line rendered");
        let second = html.find("second-line").expect("second line rendered");
        let third = html.find("third-line").expect("third line rendered");
        assert!(
            first < second && second < third,
            "lines must render in emission (seq) order"
        );
        for level in ["info", "warn", "error"] {
            assert!(html.contains(level), "level {level} must be shown");
        }
    }

    #[test]
    fn logs_panel_escapes_a_hostile_author_message() {
        // A log message is arbitrary author text and can embed anything a
        // workflow formats into it -- including attacker-controlled input.
        let lines = [stub_log_line(0, "info", "<script>alert('xss')</script>")];
        let html = render_logs_detail(&WorkflowLogsPanelData {
            lines: &lines,
            admin: true,
            ..Default::default()
        });
        assert!(
            !html.contains("<script>alert"),
            "a log message must never render as live markup"
        );
        assert!(
            html.contains("&lt;script&gt;"),
            "the message must still be visible, escaped"
        );
    }

    #[test]
    fn logs_panel_marks_the_active_level_filter() {
        // AC5's "with level filtering". The active link must be visually
        // distinguishable -- `class="active"` under a rule that actually
        // matches, or the operator has no feedback about what is applied.
        let html = render_logs_detail(&WorkflowLogsPanelData {
            lines: &[],
            level_filter: Some(autumn_harvest::WorkflowLogLevel::Warn),
            admin: true,
            ..Default::default()
        });
        assert!(
            html.contains("log-filters"),
            "the filter row must carry the scoped class its CSS rule targets"
        );
        assert!(
            html.contains(".log-filters a.active"),
            "a scoped stylesheet rule must exist for the active filter link, \
             or `class=\"active\"` renders identically to the others"
        );
        let warn_link = html
            .find("log_level=warn")
            .expect("the warn filter link must render");
        let link_start = html[..warn_link].rfind("<a").expect("anchor open tag");
        assert!(
            html[link_start..warn_link].contains("active"),
            "the selected level's link must be marked active"
        );
    }

    #[test]
    fn logs_panel_filter_links_preserve_the_event_page() {
        // The detail page has two independent view dimensions; a bare
        // `?log_level=` link would silently reset the event pager.
        let execution = stub_execution();
        let blocked = stub_blocked_on();
        let html = render_workflow_detail(
            &execution,
            0,
            &[],
            &[],
            &[],
            false,
            &[],
            3, // event_page
            &blocked,
            None,
            None,
            &WorkflowLogsPanelData {
                lines: &[],
                admin: true,
                ..Default::default()
            },
        )
        .into_string();
        // maud escapes `&` inside an attribute value, which is the correct
        // HTML -- assert the escaped form rather than weakening the check.
        assert!(
            html.contains("?event_page=3&amp;log_level=warn"),
            "a level-filter link must carry the current event page"
        );
    }

    #[test]
    fn jump_to_event_control_has_a_programmatically_associated_label() {
        // Every other `label`/control pair in this file relies on the
        // dashboard's own convention: a `<label>` wraps its control. The
        // browser associates the two even with no `for`/`id` pair. This
        // control alone rendered the label and the input as siblings, so a
        // screen reader announced the field with no accessible name at all.
        // Assert the wrapping structurally: the `jump_event` input must sit
        // between the `<label>` carrying "Jump to event:" and its close tag.
        let execution = stub_execution();
        let blocked = stub_blocked_on();
        let html = render_workflow_detail(
            &execution,
            150, // total_events, past DETAIL_EVENT_PAGE_SIZE so the control renders
            &[],
            &[],
            &[],
            false,
            &[],
            0,
            &blocked,
            None,
            None,
            &WorkflowLogsPanelData {
                lines: &[],
                admin: true,
                ..Default::default()
            },
        )
        .into_string();

        let label_text_pos = html
            .find("Jump to event:")
            .expect("the jump-to-event control must render past the pagination threshold");
        let label_open = html[..label_text_pos]
            .rfind("<label")
            .expect("\"Jump to event:\" must be inside a <label>");
        let label_close = label_text_pos
            + html[label_text_pos..]
                .find("</label>")
                .expect("the label must be closed");
        let input_pos = html
            .find("name=\"jump_event\"")
            .expect("the jump_event input must render");
        assert!(
            label_open < input_pos && input_pos < label_close,
            "the jump_event input must be a descendant of its <label>, not a \
             sibling -- otherwise it has no programmatic accessible name"
        );
    }

    #[test]
    fn logs_panel_shows_the_truncation_banner_from_the_caller_probe() {
        // The marker sits at `seq = i64::MAX` and sorts LAST, while the panel
        // loads only the first `WORKFLOW_LOG_PANEL_LIMIT` rows -- so under the
        // default 1,000-line cap it can never appear in the page. The flag must
        // therefore come from the caller's direct probe, not from scanning
        // `lines`; this fixture has a full page with NO marker in it.
        let lines: Vec<_> = (0..3).map(|i| stub_log_line(i, "info", "l")).collect();
        let html = render_logs_detail(&WorkflowLogsPanelData {
            lines: &lines,
            admin: true,
            truncated: true,
            ..Default::default()
        });
        assert!(
            html.contains("reached its per-execution log cap"),
            "a truncated run must show the banner even though the marker row is \
             not in this page"
        );
    }

    #[test]
    fn logs_panel_distinguishes_a_read_failure_from_an_empty_log() {
        // During an incident -- e.g. a node that has not run the migration --
        // the empty state would otherwise affirmatively claim the sink is
        // disabled, which is the opposite of what happened.
        let failed = render_logs_detail(&WorkflowLogsPanelData {
            lines: &[],
            admin: true,
            read_failed: true,
            ..Default::default()
        });
        assert!(failed.contains("read failure, not an empty log"));
        assert!(
            !failed.contains("opt-in sink is enabled"),
            "a read failure must not be reported as a disabled sink"
        );

        let empty = render_logs_detail(&WorkflowLogsPanelData {
            lines: &[],
            admin: true,
            ..Default::default()
        });
        assert!(empty.contains("opt-in sink is enabled"));
        assert!(!empty.contains("read failure, not an empty log"));
    }

    #[test]
    fn logs_panel_empty_state_names_the_active_filter() {
        let html = render_logs_detail(&WorkflowLogsPanelData {
            lines: &[],
            level_filter: Some(autumn_harvest::WorkflowLogLevel::Error),
            admin: true,
            ..Default::default()
        });
        assert!(
            html.contains("No log lines at this level."),
            "an empty FILTERED view must not claim the run logged nothing"
        );
    }

    #[test]
    fn workflow_detail_href_preserves_both_dimensions() {
        assert_eq!(workflow_detail_href(0, None), "?event_page=0");
        assert_eq!(
            workflow_detail_href(2, Some("error")),
            "?event_page=2&log_level=error"
        );
    }

    // ── issue #951: schedules management page — health, policy and drill-downs ──

    /// A schedule with nothing wrong reports no health flags, so a healthy row
    /// stays calm (AC3: "a healthy schedule reads as one calm row").
    #[test]
    fn schedule_health_healthy_row_has_no_flags() {
        let row = make_schedule(Some("payments"), None, false);
        let health = schedule_health(&row);
        assert!(
            health.is_healthy(),
            "an untouched schedule must read healthy"
        );
        assert_eq!(
            render_schedule_health_badges(&row).into_string(),
            "",
            "a healthy row must render no health badges"
        );
    }

    /// Each unhealthy condition the AC names gets its own flag + badge.
    #[test]
    fn schedule_health_flags_paused_exhausted_and_catchup_dropped() {
        let paused = make_schedule(Some("wf"), None, true);
        assert!(schedule_health(&paused).paused);
        assert!(!schedule_health(&paused).is_healthy());
        assert!(
            render_schedule_health_badges(&paused)
                .into_string()
                .contains("Paused")
        );

        let exhausted = HarvestSchedule {
            exhausted_at: Some(chrono::Utc::now()),
            exhausted_reason: Some("max_runs_exhausted".to_string()),
            ..make_schedule(Some("wf"), None, false)
        };
        assert!(schedule_health(&exhausted).exhausted);
        let html = render_schedule_health_badges(&exhausted).into_string();
        assert!(
            html.contains("Exhausted"),
            "exhausted badge missing: {html}"
        );
        assert!(
            html.contains("max_runs_exhausted"),
            "exhaustion reason must be surfaced: {html}"
        );

        let dropped = HarvestSchedule {
            last_catchup_dropped: 7,
            last_catchup_at: Some(chrono::Utc::now()),
            ..make_schedule(Some("wf"), None, false)
        };
        assert!(schedule_health(&dropped).catchup_dropped);
        let html = render_schedule_health_badges(&dropped).into_string();
        assert!(
            html.contains("Catchup dropped"),
            "catchup-dropped badge missing: {html}"
        );
        assert!(html.contains('7'), "dropped count must be shown: {html}");
    }

    /// An auto-paused schedule (#360) is unhealthy and distinguishable from a
    /// hand-paused one.
    #[test]
    fn schedule_health_flags_auto_paused_distinctly() {
        let row = HarvestSchedule {
            is_paused: true,
            auto_paused_at: Some(chrono::Utc::now()),
            consecutive_failure_count: 3,
            ..make_schedule(Some("wf"), None, true)
        };
        let health = schedule_health(&row);
        assert!(health.auto_paused && health.paused);
        let html = render_schedule_health_badges(&row).into_string();
        assert!(
            html.contains("Auto-paused"),
            "auto-paused badge missing: {html}"
        );
    }

    /// AC3: unhealthy rows sort above healthy ones, and healthy rows keep their
    /// existing `next_run_at`-ascending relative order.
    #[test]
    fn schedule_sort_puts_unhealthy_first_without_reordering_healthy_rows() {
        let t = |mins: i64| Some(chrono::Utc::now() + chrono::Duration::minutes(mins));
        let healthy_soon = HarvestSchedule {
            next_run_at: t(1),
            ..make_schedule(Some("a_soon"), None, false)
        };
        let healthy_later = HarvestSchedule {
            next_run_at: t(60),
            ..make_schedule(Some("b_later"), None, false)
        };
        let paused = HarvestSchedule {
            next_run_at: t(600),
            ..make_schedule(Some("z_paused"), None, true)
        };
        let mut rows = vec![
            (ShardId::new(0), healthy_soon.clone()),
            (ShardId::new(0), healthy_later.clone()),
            (ShardId::new(0), paused.clone()),
        ];
        sort_schedule_rows(&mut rows);
        assert_eq!(
            rows[0].1.id, paused.id,
            "unhealthy row must sort to the top"
        );
        assert_eq!(rows[1].1.id, healthy_soon.id);
        assert_eq!(rows[2].1.id, healthy_later.id);
    }

    /// The health filter narrows to unhealthy-only / healthy-only rows.
    #[test]
    fn schedule_health_filter_selects_unhealthy_rows() {
        let healthy = make_schedule(Some("ok"), None, false);
        let paused = make_schedule(Some("bad"), None, true);
        let filters = ScheduleUiFilters {
            health: ScheduleHealthFilter::Unhealthy,
            ..Default::default()
        };
        assert!(!filters.matches(ShardId::new(0), &healthy));
        assert!(filters.matches(ShardId::new(0), &paused));

        let filters = ScheduleUiFilters {
            health: ScheduleHealthFilter::Healthy,
            ..Default::default()
        };
        assert!(filters.matches(ShardId::new(0), &healthy));
        assert!(!filters.matches(ShardId::new(0), &paused));
    }

    #[test]
    fn schedule_health_filter_parses_and_rejects_unknown_values() {
        assert_eq!(
            ScheduleHealthFilter::parse("Unhealthy").unwrap(),
            ScheduleHealthFilter::Unhealthy
        );
        assert_eq!(
            ScheduleHealthFilter::parse("Healthy").unwrap(),
            ScheduleHealthFilter::Healthy
        );
        assert_eq!(
            ScheduleHealthFilter::parse("").unwrap(),
            ScheduleHealthFilter::All
        );
        assert!(ScheduleHealthFilter::parse("bogus").is_err());
    }

    /// AC2: the catchup cell shows the effective policy (#484), its window, and
    /// the drop count from the most recent recovery.
    #[test]
    fn schedule_catchup_label_reports_effective_policy_and_drops() {
        let skip_all = make_schedule(Some("wf"), None, false);
        assert!(schedule_catchup_label(&skip_all).contains("skip_all"));

        let windowed = HarvestSchedule {
            catchup_policy: Some("window".to_string()),
            catchup_window_secs: Some(3600),
            last_catchup_dropped: 4,
            ..make_schedule(Some("wf"), None, false)
        };
        let label = schedule_catchup_label(&windowed);
        assert!(label.contains("window"), "policy missing: {label}");
        assert!(label.contains("3600"), "window seconds missing: {label}");
        assert!(label.contains('4'), "drop count missing: {label}");

        // Legacy bool fallback: catchup = true with no policy column.
        let legacy = HarvestSchedule {
            catchup: true,
            ..make_schedule(Some("wf"), None, false)
        };
        assert!(schedule_catchup_label(&legacy).contains("unbounded"));
    }

    /// AC2: bounded-run state — remaining budget, `end_at`, and exhaustion reason.
    #[test]
    fn schedule_bounded_runs_label_reports_budget_end_at_and_reason() {
        let unbounded = make_schedule(Some("wf"), None, false);
        assert_eq!(schedule_bounded_runs_label(&unbounded), "—");

        let bounded = HarvestSchedule {
            max_runs: Some(10),
            runs_started: 4,
            ..make_schedule(Some("wf"), None, false)
        };
        let label = schedule_bounded_runs_label(&bounded);
        assert!(label.contains('6'), "remaining budget missing: {label}");
        assert!(label.contains("10"), "max_runs missing: {label}");

        let ends = HarvestSchedule {
            end_at: chrono::DateTime::parse_from_rfc3339("2027-01-01T00:00:00Z")
                .ok()
                .map(|d| d.with_timezone(&chrono::Utc)),
            ..make_schedule(Some("wf"), None, false)
        };
        assert!(schedule_bounded_runs_label(&ends).contains("2027-01-01"));

        let spent = HarvestSchedule {
            max_runs: Some(3),
            runs_started: 3,
            exhausted_at: Some(chrono::Utc::now()),
            exhausted_reason: Some("max_runs_exhausted".to_string()),
            ..make_schedule(Some("wf"), None, false)
        };
        let label = schedule_bounded_runs_label(&spent);
        assert!(
            label.contains("max_runs_exhausted"),
            "exhausted reason missing: {label}"
        );
    }

    /// AC2: the next-fire cell shows the jitter-adjusted effective fire time when
    /// jitter is configured, and only `next_run_at` when it is not.
    #[test]
    fn schedule_next_fire_cell_shows_effective_time_only_with_jitter() {
        let at = chrono::DateTime::parse_from_rfc3339("2026-09-01T12:00:00Z")
            .expect("valid fixture timestamp")
            .with_timezone(&chrono::Utc);

        let no_jitter = HarvestSchedule {
            next_run_at: Some(at),
            jitter_secs: 0,
            ..make_schedule(Some("wf"), None, false)
        };
        let html = schedule_next_fire_cell(&no_jitter).into_string();
        assert!(
            html.contains("2026-09-01 12:00:00"),
            "next_run_at missing: {html}"
        );
        assert!(
            !html.contains("effective"),
            "no effective line without jitter: {html}"
        );

        let jittered = HarvestSchedule {
            next_run_at: Some(at),
            jitter_secs: 300,
            ..make_schedule(Some("wf"), None, false)
        };
        let html = schedule_next_fire_cell(&jittered).into_string();
        assert!(
            html.contains("effective"),
            "jittered row must show the effective fire time: {html}"
        );
        assert!(html.contains("300s"), "jitter window missing: {html}");
    }

    /// AC2: the overlap policy (#241) is rendered, with the buffered depth when
    /// the policy buffers.
    #[test]
    fn schedule_overlap_label_reports_policy_and_buffer_depth() {
        let skip = make_schedule(Some("wf"), None, false);
        assert!(schedule_overlap_label(&skip).contains("skip"));

        let buffered = HarvestSchedule {
            overlap_policy: "buffer_all".to_string(),
            buffered_runs: serde_json::json!(["2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z"]),
            buffer_all_max: 50,
            ..make_schedule(Some("wf"), None, false)
        };
        let label = schedule_overlap_label(&buffered);
        assert!(label.contains("buffer_all"), "policy missing: {label}");
        assert!(label.contains('2'), "buffered depth missing: {label}");
        assert!(label.contains("50"), "buffer cap missing: {label}");
    }

    /// AC3: the page header counts unhealthy schedules so triage starts before
    /// the operator reads a single row.
    #[test]
    fn schedule_health_summary_counts_each_unhealthy_bucket() {
        let rows = vec![
            (ShardId::new(0), make_schedule(Some("ok"), None, false)),
            (ShardId::new(0), make_schedule(Some("p"), None, true)),
            (
                ShardId::new(0),
                HarvestSchedule {
                    exhausted_at: Some(chrono::Utc::now()),
                    ..make_schedule(Some("e"), None, false)
                },
            ),
            (
                ShardId::new(0),
                HarvestSchedule {
                    last_catchup_dropped: 2,
                    ..make_schedule(Some("c"), None, false)
                },
            ),
        ];
        let summary = schedule_health_summary(&rows);
        assert!(
            summary.contains("1 paused"),
            "paused count missing: {summary}"
        );
        assert!(
            summary.contains("1 exhausted"),
            "exhausted count missing: {summary}"
        );
        assert!(
            summary.contains("1 catchup-dropped"),
            "catchup-dropped count missing: {summary}"
        );

        let all_healthy = vec![(ShardId::new(0), make_schedule(Some("ok"), None, false))];
        assert_eq!(
            schedule_health_summary(&all_healthy),
            "",
            "a healthy fleet needs no unhealthy summary"
        );
    }

    /// AC2/AC5/AC6/AC7: each row links to its preview, backfill and run-history
    /// drill-downs.
    #[test]
    fn schedule_table_row_links_to_every_drill_down() {
        let row = make_schedule(Some("payments"), None, false);
        let id = row.id.to_string();
        let html = render_schedule_table(
            &[(ShardId::new(0), row)],
            false,
            &std::collections::HashMap::new(),
        )
        .into_string();
        for suffix in ["preview", "runs", "backfill"] {
            assert!(
                html.contains(&format!("schedules/{id}/{suffix}")),
                "row must link to the {suffix} drill-down: {html}"
            );
        }
    }

    // -- Preview drill-down (AC5) --

    fn preview_entry(
        scheduled_at: &str,
        effective_at: Option<&str>,
        reason: &str,
    ) -> crate::api::ScheduleFirePreviewEntry {
        let parse = |s: &str| {
            chrono::DateTime::parse_from_rfc3339(s)
                .expect("valid fixture timestamp")
                .with_timezone(&chrono::Utc)
        };
        let effective = effective_at.map(parse);
        crate::api::ScheduleFirePreviewEntry {
            scheduled_at: parse(scheduled_at),
            local_at: scheduled_at.to_string(),
            effective_at: effective,
            effective_local_at: effective_at.map(str::to_string),
            reason: reason.to_string(),
            jitter_earliest_at: None,
            jitter_latest_at: None,
            would_skip_if_active: false,
        }
    }

    /// AC5: the preview shows effective vs. original vs. calendar-skipped entries.
    #[test]
    fn preview_page_distinguishes_effective_original_and_skipped_entries() {
        let row = make_schedule(Some("payments"), None, false);
        let preview = crate::api::SchedulePreview {
            entries: vec![
                preview_entry(
                    "2026-09-01T12:00:00Z",
                    Some("2026-09-01T12:02:00Z"),
                    "cron+jitter",
                ),
                preview_entry("2026-09-02T12:00:00Z", None, "skipped:calendar-excluded"),
                preview_entry("2026-09-03T12:00:00Z", Some("2026-09-03T12:00:00Z"), "cron"),
            ],
            is_paused: false,
            pause_reason: None,
            from: chrono::Utc::now(),
            count_requested: 10,
            end_at: None,
            remaining_runs: None,
            exhausted_reason: None,
        };
        let html = render_schedule_preview_page(&row, ShardId::new(0), &preview, 10).into_string();
        assert!(
            html.contains("2026-09-01 12:00:00"),
            "original instant missing: {html}"
        );
        assert!(
            html.contains("2026-09-01 12:02:00"),
            "effective instant missing: {html}"
        );
        assert!(
            html.contains("cron+jitter"),
            "jitter reason missing: {html}"
        );
        assert!(
            html.contains("skipped:calendar-excluded"),
            "calendar-skip reason missing: {html}"
        );
        assert!(!html.contains("<script"), "no script tags: {html}");
    }

    /// AC5 (#543): a bounded schedule whose preview truncates to zero entries must
    /// say *why*, not render a blank panel (AC8).
    #[test]
    fn preview_page_explains_zero_entries_for_a_bounded_schedule() {
        let row = HarvestSchedule {
            max_runs: Some(5),
            runs_started: 5,
            exhausted_at: Some(chrono::Utc::now()),
            exhausted_reason: Some("max_runs_exhausted".to_string()),
            ..make_schedule(Some("payments"), None, false)
        };
        let preview = crate::api::SchedulePreview {
            entries: vec![],
            is_paused: false,
            pause_reason: None,
            from: chrono::Utc::now(),
            count_requested: 10,
            end_at: None,
            remaining_runs: Some(0),
            exhausted_reason: Some("max_runs_exhausted".to_string()),
        };
        let html = render_schedule_preview_page(&row, ShardId::new(0), &preview, 10).into_string();
        assert!(
            html.contains("max_runs_exhausted"),
            "must name the exhaustion reason: {html}"
        );
        assert!(
            html.contains("no upcoming fire times") || html.contains("No upcoming fire times"),
            "must render an explicit empty state: {html}"
        );
    }

    /// A paused schedule's preview says so rather than rendering an empty table.
    #[test]
    fn preview_page_explains_zero_entries_for_a_paused_schedule() {
        let row = make_schedule(Some("payments"), None, true);
        let preview = crate::api::SchedulePreview {
            entries: vec![],
            is_paused: true,
            pause_reason: Some("operator hold".to_string()),
            from: chrono::Utc::now(),
            count_requested: 10,
            end_at: None,
            remaining_runs: None,
            exhausted_reason: None,
        };
        let html = render_schedule_preview_page(&row, ShardId::new(0), &preview, 10).into_string();
        assert!(html.contains("paused") || html.contains("Paused"));
        assert!(
            html.contains("operator hold"),
            "pause reason missing: {html}"
        );
    }

    // -- Run history drill-down (AC7) --

    fn run_entry(
        state: &str,
        origin: &str,
        nominal: Option<&str>,
    ) -> crate::schedule_runs::ScheduleRunEntry {
        let parse = |s: &str| {
            chrono::DateTime::parse_from_rfc3339(s)
                .expect("valid fixture timestamp")
                .with_timezone(&chrono::Utc)
        };
        crate::schedule_runs::ScheduleRunEntry {
            execution_id: uuid::Uuid::new_v4(),
            nominal_fire_time: nominal.map(parse),
            started_at: parse("2026-09-01T12:00:00Z"),
            completed_at: Some(parse("2026-09-01T12:01:00Z")),
            state: state.to_string(),
            outcome: crate::schedule_runs::collapse_outcome(state),
            error: None,
            origin: Some(origin.to_string()),
        }
    }

    fn runs_response(
        runs: Vec<crate::schedule_runs::ScheduleRunEntry>,
        status: crate::shard_fanout::FanoutStatus,
        shards: Vec<crate::schedule_runs::RunsShardInspection>,
    ) -> crate::schedule_runs::ScheduleRunsResponse {
        crate::schedule_runs::ScheduleRunsResponse {
            schedule_id: uuid::Uuid::new_v4(),
            status,
            next_run_at: None,
            runs,
            summary: crate::schedule_runs::ScheduleRunSummary {
                succeeded: 3,
                failed: 1,
                total: 4,
                summary_complete: matches!(status, crate::shard_fanout::FanoutStatus::Complete),
                ..Default::default()
            },
            limit: 20,
            next_cursor: None,
            shards,
        }
    }

    /// AC7: newest-first rows carry nominal fire time, origin, a terminal state
    /// badge, and link to the execution detail view.
    #[test]
    fn runs_page_renders_rows_with_origin_state_and_execution_links() {
        let row = make_schedule(Some("payments"), None, false);
        let runs = vec![
            run_entry("COMPLETED", "scheduled", Some("2026-09-01T12:00:00Z")),
            run_entry("FAILED", "backfill", Some("2026-08-31T12:00:00Z")),
            run_entry("RUNNING", "manual_trigger", None),
        ];
        let exec_ids: Vec<_> = runs.iter().map(|r| r.execution_id).collect();
        let response = runs_response(runs, crate::shard_fanout::FanoutStatus::Complete, vec![]);
        let html = render_schedule_runs_page(
            &row,
            ShardId::new(0),
            &response,
            &ScheduleRunsView::default(),
            None,
        )
        .into_string();

        for id in &exec_ids {
            assert!(
                html.contains(&format!("workflows/{id}")),
                "run must link to the execution detail view: {html}"
            );
        }
        // Origin names and state names also appear in the page chrome (the
        // summary note, the footer's "Backfill" link) and in the inlined
        // stylesheet's `.COMPLETED` / `.FAILED` rules, so assert on the actual
        // table cells.
        for origin in ["scheduled", "backfill", "manual_trigger"] {
            assert!(
                html.contains(&format!("<td><code>{origin}</code></td>")),
                "{origin} origin cell missing: {html}"
            );
        }
        for state in ["COMPLETED", "FAILED", "RUNNING"] {
            assert!(
                html.contains(&format!(">{state}</span>")),
                "{state} badge missing: {html}"
            );
        }
        assert!(
            html.contains("2026-09-01 12:00:00"),
            "nominal fire time missing: {html}"
        );
        // Scheduled-only cadence summary.
        assert!(
            html.contains("Scheduled-run summary") || html.contains("scheduled-run summary"),
            "cadence summary heading missing: {html}"
        );
    }

    /// AC7: a `status: partial` response renders a visible "some shards
    /// unreachable" banner rather than silently truncated data.
    #[test]
    fn runs_page_renders_partial_shard_banner() {
        let row = make_schedule(Some("payments"), None, false);
        let response = runs_response(
            vec![run_entry(
                "COMPLETED",
                "scheduled",
                Some("2026-09-01T12:00:00Z"),
            )],
            crate::shard_fanout::FanoutStatus::Partial,
            vec![
                crate::schedule_runs::RunsShardInspection {
                    shard_id: 0,
                    status: "inspected",
                    error: None,
                },
                crate::schedule_runs::RunsShardInspection {
                    shard_id: 1,
                    status: "unavailable",
                    error: Some("connection refused".to_string()),
                },
            ],
        );
        let html = render_schedule_runs_page(
            &row,
            ShardId::new(0),
            &response,
            &ScheduleRunsView::default(),
            None,
        )
        .into_string();
        assert!(
            html.contains("some shards unreachable") || html.contains("Some shards unreachable"),
            "partial-shard banner missing: {html}"
        );
        assert!(
            html.contains("connection refused"),
            "shard error detail missing: {html}"
        );
        assert!(
            html.contains("counts may be understated") || html.contains("may be understated"),
            "summary must be flagged as possibly understated: {html}"
        );
    }

    /// AC8: a schedule that has never run renders an explicit message, not a
    /// blank panel.
    #[test]
    fn runs_page_renders_no_runs_yet_empty_state() {
        let row = make_schedule(Some("payments"), None, false);
        let response = runs_response(vec![], crate::shard_fanout::FanoutStatus::Complete, vec![]);
        let html = render_schedule_runs_page(
            &row,
            ShardId::new(0),
            &response,
            &ScheduleRunsView::default(),
            None,
        )
        .into_string();
        assert!(
            html.contains("no runs yet") || html.contains("No runs yet"),
            "no-runs empty state missing: {html}"
        );
    }

    /// AC7: an `unavailable` status is louder still — no shard could be inspected.
    #[test]
    fn runs_page_renders_unavailable_banner_when_no_shard_answered() {
        let row = make_schedule(Some("payments"), None, false);
        let response = runs_response(
            vec![],
            crate::shard_fanout::FanoutStatus::Unavailable,
            vec![crate::schedule_runs::RunsShardInspection {
                shard_id: 0,
                status: "unavailable",
                error: Some("pool timeout".to_string()),
            }],
        );
        let html = render_schedule_runs_page(
            &row,
            ShardId::new(0),
            &response,
            &ScheduleRunsView::default(),
            None,
        )
        .into_string();
        assert!(
            html.contains("No shard could be reached")
                || html.contains("no shard could be reached"),
            "unavailable banner missing: {html}"
        );
        assert!(
            !html.contains("No runs yet"),
            "must not claim 'no runs' when nothing could be read: {html}"
        );
    }

    // -- Backfill launcher (AC6) --

    /// AC6: the form collects a start/end window and posts to the dry-run preview
    /// step, never straight to the dispatching endpoint.
    #[test]
    fn backfill_form_posts_to_the_preview_step_first() {
        let row = make_schedule(Some("payments"), None, false);
        let id = row.id.to_string();
        let html = render_schedule_backfill_form(
            &row,
            ShardId::new(0),
            None,
            &BackfillFormEcho::default(),
        )
        .into_string();
        assert!(
            html.contains(&format!("schedules/{id}/backfill")),
            "form must post to the backfill route: {html}"
        );
        assert!(
            html.contains("name=\"stage\" value=\"preview\""),
            "the form must submit the dry-run stage, never a bare commit: {html}"
        );
        assert!(
            !html.contains("value=\"commit\""),
            "the launcher form must never offer a direct commit: {html}"
        );
        assert!(
            html.contains("name=\"from\""),
            "start field missing: {html}"
        );
        assert!(html.contains("name=\"to\""), "end field missing: {html}");
        assert!(!html.contains("<script"), "no script tags: {html}");
    }

    /// AC6: the confirmation step shows the dry-run's planned count before the
    /// operator can dispatch anything.
    #[test]
    fn backfill_confirm_shows_planned_count_before_dispatch() {
        let row = make_schedule(Some("payments"), None, false);
        let id = row.id.to_string();
        let parse = |s: &str| {
            chrono::DateTime::parse_from_rfc3339(s)
                .expect("valid fixture timestamp")
                .with_timezone(&chrono::Utc)
        };
        let dry_run = crate::api::ScheduleBackfillResponse {
            status: "dry_run".to_string(),
            schedule_id: row.id,
            kind: crate::api::ScheduleKind::Workflow,
            name: "payments".to_string(),
            from: parse("2026-08-01T00:00:00Z"),
            to: parse("2026-08-02T00:00:00Z"),
            planned_timestamps: vec![parse("2026-08-01T00:00:00Z"), parse("2026-08-01T01:00:00Z")],
            total: 24,
            dispatched: 20,
            skipped: 4,
            failed: 0,
            skipped_reasons: std::collections::HashMap::from([(
                "already_exists".to_string(),
                4usize,
            )]),
            partial_shard_failures: vec![],
            paused_schedule_warning: None,
        };
        let form = BackfillFormParams {
            from: "2026-08-01T00:00:00Z".to_string(),
            to: "2026-08-02T00:00:00Z".to_string(),
            max_count: Some(100),
            include_paused: false,
        };
        let html =
            render_schedule_backfill_confirm(&row, ShardId::new(0), &dry_run, &form).into_string();
        // Assert on the labelled cells: the inlined stylesheet contains bare
        // "24"/"20" on several lines, so a plain `contains` proves nothing.
        assert!(
            html.contains("<dt>Planned slots</dt><dd>24</dd>"),
            "planned total missing: {html}"
        );
        assert!(
            html.contains("<dt>Would dispatch</dt><dd>20</dd>"),
            "would-dispatch count missing: {html}"
        );
        assert!(
            html.contains("already_exists"),
            "skip reasons must be shown: {html}"
        );
        assert!(
            html.contains(&format!("action=\"../../schedules/{id}/backfill\"")),
            "confirm must post to the dispatching endpoint: {html}"
        );
        assert!(
            html.contains("name=\"stage\" value=\"commit\""),
            "confirm must carry the explicit commit stage: {html}"
        );
        // The window must round-trip so the committed backfill is the one previewed.
        assert!(html.contains("2026-08-01T00:00:00Z"));
        assert!(html.contains("2026-08-02T00:00:00Z"));
    }

    /// A backfill window with nothing in it must not offer a dispatch button.
    #[test]
    fn backfill_confirm_disables_dispatch_for_an_empty_window() {
        let row = make_schedule(Some("payments"), None, false);
        let parse = |s: &str| {
            chrono::DateTime::parse_from_rfc3339(s)
                .expect("valid fixture timestamp")
                .with_timezone(&chrono::Utc)
        };
        let dry_run = crate::api::ScheduleBackfillResponse {
            status: "dry_run".to_string(),
            schedule_id: row.id,
            kind: crate::api::ScheduleKind::Workflow,
            name: "payments".to_string(),
            from: parse("2026-08-01T00:00:00Z"),
            to: parse("2026-08-01T00:00:01Z"),
            planned_timestamps: vec![],
            total: 0,
            dispatched: 0,
            skipped: 0,
            failed: 0,
            skipped_reasons: std::collections::HashMap::new(),
            partial_shard_failures: vec![],
            paused_schedule_warning: None,
        };
        let form = BackfillFormParams {
            from: "2026-08-01T00:00:00Z".to_string(),
            to: "2026-08-01T00:00:01Z".to_string(),
            max_count: None,
            include_paused: false,
        };
        let html =
            render_schedule_backfill_confirm(&row, ShardId::new(0), &dry_run, &form).into_string();
        assert!(
            html.contains("Nothing to backfill"),
            "empty window must render an explicit message: {html}"
        );
        // The page inlines the stylesheet, so a bare `contains("disabled")`
        // would be satisfied by a CSS rule. Assert the button is simply absent.
        assert!(
            !html.contains("type=\"submit\""),
            "an empty window must not offer a dispatch button at all: {html}"
        );
        assert!(
            !html.contains("value=\"commit\""),
            "an empty window must not carry a commit stage: {html}"
        );
    }

    /// The backfill form parses its own inputs; a malformed window is a rendered
    /// error, never a 500.
    #[test]
    fn backfill_form_params_reject_a_malformed_window() {
        assert!(
            BackfillFormParams::parse("not-a-date", "2026-08-02T00:00:00Z", None, false).is_err()
        );
        assert!(
            BackfillFormParams::parse("2026-08-02T00:00:00Z", "2026-08-01T00:00:00Z", None, false)
                .is_err()
        );
        let ok = BackfillFormParams::parse(
            "2026-08-01T00:00:00Z",
            "2026-08-02T00:00:00Z",
            Some(50),
            true,
        )
        .expect("a well-formed window parses");
        assert_eq!(ok.max_count, Some(50));
        assert!(ok.include_paused);
    }

    /// Collect the contents of every `onsubmit="…"` attribute in a document, so a
    /// test can assert on what actually reaches the inline JavaScript context
    /// rather than on the document as a whole (where an escaped name is harmless
    /// display text).
    fn onsubmit_attribute_values(html: &str) -> Vec<String> {
        html.match_indices("onsubmit=\"")
            .filter_map(|(start, marker)| {
                let value_start = start + marker.len();
                html[value_start..]
                    .find('"')
                    .map(|end| html[value_start..value_start + end].to_string())
            })
            .collect()
    }

    /// Names never reach a JavaScript string literal: confirmations interpolate
    /// only the schedule UUID, so a hostile workflow name cannot break out of the
    /// inline `confirm('...')` handler.
    #[test]
    fn schedule_row_confirmations_never_interpolate_names() {
        let hostile = "evil'); alert(1);//";
        let row = make_schedule(Some(hostile), None, false);
        let html = render_schedule_table(
            &[(ShardId::new(0), row)],
            false,
            &std::collections::HashMap::new(),
        )
        .into_string();

        let handlers = onsubmit_attribute_values(&html);
        assert!(
            !handlers.is_empty(),
            "the row is expected to carry confirmation handlers: {html}"
        );
        for handler in &handlers {
            assert!(
                !handler.contains("alert"),
                "a workflow name must never be interpolated into an inline handler: {handler}"
            );
            assert!(
                !handler.contains("evil"),
                "a workflow name must never be interpolated into an inline handler: {handler}"
            );
        }
        // A markup-bearing name is escaped into display text, never rendered raw.
        let markup_name = "</code><script>alert(1)</script>";
        let markup_row = make_schedule(Some(markup_name), None, false);
        let markup_html = render_schedule_table(
            &[(ShardId::new(0), markup_row)],
            false,
            &std::collections::HashMap::new(),
        )
        .into_string();
        assert!(
            !markup_html.contains("<script"),
            "a markup-bearing name must be escaped, not rendered: {markup_html}"
        );
        assert!(
            markup_html.contains("&lt;script&gt;"),
            "the name must appear escaped as display text: {markup_html}"
        );
    }

    // -- issue #951 review follow-ups: bugs the first round shipped --

    /// The polarity of `dry_run` versus the UI's `commit` stage.
    ///
    /// These are opposites, and getting them the same way round makes the
    /// "Preview backfill" button *dispatch* while the confirmation button only
    /// projects — with the confirmation page still reading "Nothing has been
    /// dispatched yet." over the counts of runs it just launched. Pinned here as
    /// a pure test because the end-to-end version needs a database.
    #[test]
    fn backfill_request_dry_run_is_the_inverse_of_the_commit_stage() {
        let params = BackfillFormParams {
            from: "2026-08-01T00:00:00Z".to_string(),
            to: "2026-08-02T00:00:00Z".to_string(),
            max_count: None,
            include_paused: false,
        };
        // The preview stage must project.
        let commit = false;
        let request = params
            .to_request(!commit)
            .expect("a normalised window converts");
        assert!(
            request.dry_run,
            "the preview stage must send dry_run = true"
        );
        // The commit stage must dispatch.
        let commit = true;
        let request = params
            .to_request(!commit)
            .expect("a normalised window converts");
        assert!(
            !request.dry_run,
            "the commit stage must send dry_run = false"
        );
    }

    /// `to_request` carries the window and options through unchanged, so the
    /// committed backfill is provably the one that was previewed.
    #[test]
    fn backfill_request_round_trips_the_previewed_window() {
        let params = BackfillFormParams::parse(
            "2026-08-01T00:00:00Z",
            "2026-08-02T06:30:00Z",
            Some(42),
            true,
        )
        .expect("a well-formed window parses");
        let request = params.to_request(true).expect("converts");
        assert_eq!(request.from.to_rfc3339(), "2026-08-01T00:00:00+00:00");
        assert_eq!(request.to.to_rfc3339(), "2026-08-02T06:30:00+00:00");
        assert_eq!(request.max_count, Some(42));
        assert!(request.include_paused);
        // And the normalised strings the confirmation round-trips are the same
        // instants, so a re-parse cannot drift.
        let reparsed = BackfillFormParams::parse(
            &params.from,
            &params.to,
            params.max_count,
            params.include_paused,
        )
        .expect("normalised values re-parse");
        assert_eq!(reparsed, params);
    }

    /// Every link out of a drill-down page carries the `../../` that reaches the
    /// UI mount point. A link that forgets it resolves under the schedule id and
    /// 404s — which is invisible to a `contains` assertion on the path itself.
    #[test]
    fn drilldown_pages_link_out_with_the_mount_point_prefix() {
        let row = make_schedule(Some("payments"), None, false);
        let id = row.id.to_string();
        let response = runs_response(
            vec![run_entry(
                "COMPLETED",
                "scheduled",
                Some("2026-09-01T12:00:00Z"),
            )],
            crate::shard_fanout::FanoutStatus::Complete,
            vec![],
        );
        let response = crate::schedule_runs::ScheduleRunsResponse {
            next_cursor: Some(
                "2026-09-01T12:00:00.000000Z|00000000-0000-0000-0000-000000000001".to_string(),
            ),
            ..response
        };
        let html = render_schedule_runs_page(
            &row,
            ShardId::new(0),
            &response,
            &ScheduleRunsView {
                limit: Some(5),
                origin: Some("scheduled".to_string()),
                state: None,
            },
            None,
        )
        .into_string();

        // Nav chrome.
        for leaf in [
            "workflows",
            "workers",
            "schedules",
            "dead-letters",
            "build-routing",
        ] {
            assert!(
                html.contains(&format!("href=\"../../{leaf}\"")),
                "nav link to {leaf} must be mount-relative: {html}"
            );
        }
        assert!(
            !html.contains("href=\"workflows\""),
            "a bare depth-0 nav href would 404 from a drill-down: {html}"
        );
        // The next-page link, and the filters it must preserve.
        assert!(
            html.contains(&format!("href=\"../../schedules/{id}/runs?cursor=")),
            "the next-page link must be mount-relative: {html}"
        );
        assert!(
            html.contains("&limit=5") && html.contains("&origin=scheduled"),
            "the next-page link must carry the filters the cursor was computed under: {html}"
        );
    }

    /// A committed backfill's flash reaches the run-history page it redirects to.
    #[test]
    fn runs_page_renders_the_backfill_flash() {
        let row = make_schedule(Some("payments"), None, false);
        let response = runs_response(vec![], crate::shard_fanout::FanoutStatus::Complete, vec![]);
        let html = render_schedule_runs_page(
            &row,
            ShardId::new(0),
            &response,
            &ScheduleRunsView::default(),
            Some("Backfill dispatched 6 of 6 planned run(s); 0 skipped, 1 failed."),
        )
        .into_string();
        assert!(
            html.contains("Backfill dispatched 6 of 6 planned run(s); 0 skipped, 1 failed."),
            "the flash must be rendered, not silently dropped: {html}"
        );
    }

    /// A per-row action redirects to the list, not to a path nested under the
    /// schedule id.
    #[test]
    fn schedule_redirect_depth_resolves_to_the_list() {
        let per_row = schedule_redirect_from(2, "Paused it");
        let location = per_row
            .headers()
            .get("location")
            .and_then(|v| v.to_str().ok())
            .expect("a redirect carries a location");
        assert!(
            location.starts_with("../schedules?flash="),
            "a /schedules/{{id}}/pause redirect must climb one segment: {location}"
        );

        let bulk = schedule_redirect_from(1, "Paused 3");
        let location = bulk
            .headers()
            .get("location")
            .and_then(|v| v.to_str().ok())
            .expect("a redirect carries a location");
        assert!(
            location.starts_with("schedules?flash="),
            "a /schedules/bulk-pause redirect is already at the right depth: {location}"
        );
    }

    /// The bulk actions must act on exactly the set the list counted, health
    /// filter included — the confirmation dialog quotes that count.
    #[test]
    fn bulk_filters_apply_the_health_filter() {
        let healthy = make_schedule(Some("healthy"), None, false);
        let unhealthy = HarvestSchedule {
            last_catchup_dropped: 3,
            ..make_schedule(Some("dropping"), None, false)
        };
        let filters = parse_schedule_bulk_filters(&ScheduleBulkParams {
            target: None,
            kind: None,
            paused: None,
            health: Some("Unhealthy".to_string()),
            shard_id: None,
            return_to: None,
        })
        .unwrap();
        assert!(
            !filters.matches(ShardId::new(0), &healthy),
            "a healthy schedule must not be swept up by a health=Unhealthy bulk action"
        );
        assert!(filters.matches(ShardId::new(0), &unhealthy));
    }

    /// Codex review on #1437 (P1): a malformed `shard_id` in a bulk-action
    /// POST must reject the request. It must not silently drop to "no
    /// shard restriction" and pause/resume schedules on every shard,
    /// instead of the one the operator scoped the action to.
    #[test]
    fn parse_schedule_bulk_filters_rejects_invalid_shard_id() {
        let result = parse_schedule_bulk_filters(&ScheduleBulkParams {
            target: None,
            kind: None,
            paused: None,
            health: None,
            shard_id: Some("north".to_string()),
            return_to: None,
        });
        assert!(
            result.is_err(),
            "an invalid shard_id must reject the bulk action, not broaden it to all shards"
        );
    }

    #[test]
    fn parse_schedule_bulk_filters_accepts_valid_shard_id() {
        let filters = parse_schedule_bulk_filters(&ScheduleBulkParams {
            target: None,
            kind: None,
            paused: None,
            health: None,
            shard_id: Some("2".to_string()),
            return_to: None,
        })
        .unwrap();
        assert_eq!(filters.shard_id, Some(2));
    }

    #[test]
    fn parse_schedule_bulk_filters_blank_or_missing_shard_id_is_not_an_error() {
        let filters = parse_schedule_bulk_filters(&ScheduleBulkParams {
            target: None,
            kind: None,
            paused: None,
            health: None,
            shard_id: None,
            return_to: None,
        })
        .unwrap();
        assert_eq!(filters.shard_id, None);

        let filters = parse_schedule_bulk_filters(&ScheduleBulkParams {
            target: None,
            kind: None,
            paused: None,
            health: None,
            shard_id: Some("   ".to_string()),
            return_to: None,
        })
        .unwrap();
        assert_eq!(filters.shard_id, None);
    }

    /// Codex review on #1437 (P2): a bulk-action redirect used to always
    /// land on a bare, unfiltered `schedules?flash=…`. That dropped
    /// whatever the operator was filtered to, including an unresolved
    /// invalid value and its inline error. `schedule_bulk_redirect_to`
    /// must preserve a valid `return_to` instead.
    #[test]
    fn schedule_bulk_redirect_to_preserves_a_valid_return_to() {
        let response =
            schedule_bulk_redirect_to(Some("../schedules?kind=zombie&target=billing"), "Paused 3");
        let location = response
            .headers()
            .get(axum::http::header::LOCATION)
            .expect("redirect must set Location")
            .to_str()
            .unwrap();
        assert!(
            location.starts_with("../schedules?kind=zombie&target=billing&flash="),
            "the operator's filtered view, invalid value included, must survive \
             the redirect: {location}"
        );
    }

    /// A `return_to` that does not match the Schedules page's own path
    /// shape must never be trusted as a redirect target. It is an
    /// operator-supplied form field, so a hand-crafted or foreign value
    /// falls back to the safe default instead of an open redirect.
    #[test]
    fn schedule_bulk_redirect_to_rejects_a_foreign_return_to() {
        for unsafe_value in [
            "https://evil.example/phish",
            "//evil.example",
            "workflows",
            "schedulesXYZ",
            // Bare "schedules" (no "../") is the pre-fix shape. It 404s
            // when resolved against the bulk-action POST URL, so it must
            // not be trusted either — see `schedule_bulk_redirect_to`'s
            // own doc comment.
            "schedules?kind=Workflow",
            // A form-decoded control character (a raw newline, here)
            // would make `HeaderValue::try_from` reject the `Location`
            // header. That is an internal error after a mutation that
            // already succeeded (Codex review, #1437 P2).
            "../schedules?x=a\nb",
        ] {
            let response = schedule_bulk_redirect_to(Some(unsafe_value), "Paused 1");
            let location = response
                .headers()
                .get(axum::http::header::LOCATION)
                .expect("redirect must set Location")
                .to_str()
                .unwrap();
            assert!(
                location.starts_with("../schedules?flash="),
                "an unrecognized return_to ({unsafe_value:?}) must fall back to the \
                 safe default, not redirect off the Schedules page: {location}"
            );
        }
    }

    #[test]
    fn schedule_bulk_redirect_to_falls_back_when_return_to_is_absent() {
        let response = schedule_bulk_redirect_to(None, "Resumed 2");
        let location = response
            .headers()
            .get(axum::http::header::LOCATION)
            .expect("redirect must set Location")
            .to_str()
            .unwrap();
        assert!(location.starts_with("../schedules?flash="), "{location}");
    }

    #[test]
    fn schedule_return_to_path_round_trips_invalid_filters() {
        let filters = ScheduleUiFilters::default();
        let filter_raw = ScheduleUiFilterRaw {
            kind: "zombie".to_string(),
            kind_error: Some("bad kind".to_string()),
            ..ScheduleUiFilterRaw::default()
        };
        let path = schedule_return_to_path(&filters, &filter_raw, DEFAULT_SCHEDULE_PAGE_SIZE, None);
        assert_eq!(path, "../schedules?kind=zombie");
    }

    #[test]
    fn schedule_return_to_path_is_bare_schedules_when_no_filters_are_set() {
        let filters = ScheduleUiFilters::default();
        let filter_raw = ScheduleUiFilterRaw::default();
        let path = schedule_return_to_path(&filters, &filter_raw, DEFAULT_SCHEDULE_PAGE_SIZE, None);
        assert_eq!(path, "../schedules");
    }

    #[test]
    fn render_schedule_bulk_actions_includes_return_to_for_both_forms() {
        let filters = ScheduleUiFilters {
            target: Some("billing".to_string()),
            ..ScheduleUiFilters::default()
        };
        let filter_raw = ScheduleUiFilterRaw::default();
        let html = render_schedule_bulk_actions(
            &filters,
            &filter_raw,
            DEFAULT_SCHEDULE_PAGE_SIZE,
            None,
            3,
            "3 Workflow",
        )
        .into_string();
        assert_eq!(
            html.matches("name=\"return_to\" value=\"../schedules?target=billing\"")
                .count(),
            2,
            "both the pause and resume forms must carry the filtered return_to: {html}"
        );
    }

    /// A paused DAG schedule cannot be committed (the endpoint rejects it), so
    /// the confirmation must not offer a button that can only fail.
    #[test]
    fn backfill_confirm_refuses_to_offer_a_commit_for_a_paused_dag() {
        let row = make_schedule(None, Some("nightly_etl"), true);
        let parse = |s: &str| {
            chrono::DateTime::parse_from_rfc3339(s)
                .expect("valid fixture timestamp")
                .with_timezone(&chrono::Utc)
        };
        let dry_run = crate::api::ScheduleBackfillResponse {
            status: "dry_run".to_string(),
            schedule_id: row.id,
            kind: crate::api::ScheduleKind::Dag,
            name: "nightly_etl".to_string(),
            from: parse("2026-08-01T00:00:00Z"),
            to: parse("2026-08-02T00:00:00Z"),
            planned_timestamps: vec![parse("2026-08-01T00:00:00Z")],
            total: 24,
            dispatched: 24,
            skipped: 0,
            failed: 0,
            skipped_reasons: std::collections::HashMap::new(),
            partial_shard_failures: vec![],
            paused_schedule_warning: Some("Schedule is paused; …".to_string()),
        };
        let form = BackfillFormParams {
            from: "2026-08-01T00:00:00Z".to_string(),
            to: "2026-08-02T00:00:00Z".to_string(),
            max_count: None,
            include_paused: true,
        };
        let html =
            render_schedule_backfill_confirm(&row, ShardId::new(0), &dry_run, &form).into_string();
        assert!(
            html.contains("paused, so a backfill cannot be dispatched"),
            "must explain why the commit is unavailable: {html}"
        );
        assert!(
            !html.contains("value=\"commit\""),
            "must not offer a commit that the endpoint will reject: {html}"
        );
    }

    /// The rejection path echoes the operator's window back into the form.
    #[test]
    fn backfill_form_echoes_submitted_values_on_rejection() {
        let row = make_schedule(Some("payments"), None, false);
        let echo = BackfillFormEcho {
            from: "2026-08-01T00:00:00Z".to_string(),
            to: "not-a-date".to_string(),
            max_count: "50".to_string(),
            include_paused: true,
        };
        let html = render_schedule_backfill_form(
            &row,
            ShardId::new(0),
            Some("invalid end 'not-a-date'"),
            &echo,
        )
        .into_string();
        assert!(
            html.contains("value=\"2026-08-01T00:00:00Z\""),
            "start lost: {html}"
        );
        assert!(html.contains("value=\"not-a-date\""), "end lost: {html}");
        assert!(html.contains("value=\"50\""), "max count lost: {html}");
        assert!(html.contains("checked"), "include-paused lost: {html}");
        assert!(
            html.contains("Backfill not started"),
            "error missing: {html}"
        );
    }

    /// The "Needs attention" strip counts the whole filtered set, so it cannot
    /// under-report a fleet-wide problem when the page is one of many.
    #[test]
    fn health_summary_is_computed_over_the_filtered_set_not_the_page() {
        let all: Vec<(ShardId, HarvestSchedule)> = (0..30)
            .map(|i| {
                (
                    ShardId::new(0),
                    make_schedule(Some(&format!("wf_{i}")), None, true),
                )
            })
            .collect();
        let summary = schedule_health_summary(&all);
        assert!(
            summary.contains("30 paused"),
            "the summary must count every filtered row: {summary}"
        );
        // A page slice would report 25 with the default page size; the page
        // renderer is handed the full-set summary, so the value it displays is
        // the one computed here.
        let page: Vec<(ShardId, HarvestSchedule)> = all.iter().take(25).cloned().collect();
        assert!(schedule_health_summary(&page).contains("25 paused"));
    }

    /// An exhausted schedule with no recorded reason still gets a badge.
    #[test]
    fn health_badges_render_bare_exhausted_without_a_reason() {
        let row = HarvestSchedule {
            exhausted_at: Some(chrono::Utc::now()),
            exhausted_reason: None,
            ..make_schedule(Some("wf"), None, false)
        };
        let html = render_schedule_health_badges(&row).into_string();
        assert!(
            html.contains("Exhausted"),
            "bare exhausted badge missing: {html}"
        );
        assert!(
            !html.contains("Exhausted:"),
            "no dangling separator: {html}"
        );
    }

    /// `next_run_at` absent but jitter configured: nothing to offset, so no
    /// effective line.
    #[test]
    fn next_fire_cell_has_no_effective_line_without_a_next_run() {
        let row = HarvestSchedule {
            next_run_at: None,
            jitter_secs: 600,
            ..make_schedule(Some("wf"), None, false)
        };
        let html = schedule_next_fire_cell(&row).into_string();
        assert!(
            !html.contains("effective"),
            "no next_run_at means no effective fire time: {html}"
        );
    }

    /// The "show only unhealthy" shortcut keeps the other active filters and
    /// does not stack a second `health=` param.
    #[test]
    fn unhealthy_shortcut_link_preserves_other_filters() {
        let filters = ScheduleUiFilters {
            target: Some("billing".to_string()),
            kind: ScheduleKindFilter::Workflow,
            paused: SchedulePausedFilter::All,
            health: ScheduleHealthFilter::All,
            shard_id: Some(1),
        };
        let filter_raw = ScheduleUiFilterRaw {
            kind: "Workflow".to_string(),
            shard_id: "1".to_string(),
            ..ScheduleUiFilterRaw::default()
        };
        let qs = build_schedule_query_string(
            DEFAULT_SCHEDULE_PAGE_SIZE,
            &ScheduleUiFilters {
                health: ScheduleHealthFilter::All,
                ..filters
            },
            &filter_raw,
            None,
        );
        assert!(qs.contains("target=billing"), "target lost: {qs}");
        assert!(qs.contains("kind=Workflow"), "kind lost: {qs}");
        assert!(qs.contains("shard_id=1"), "shard lost: {qs}");
        assert!(
            !qs.contains("health="),
            "the shortcut supplies health itself; the suffix must not repeat it: {qs}"
        );
    }

    // -- Codex round 1 regressions --

    /// Codex #1: a submitted window with sub-second precision must reach the API
    /// unchanged. `SecondsFormat::Secs` truncated both bounds, and an `interval:`
    /// backfill treats `from` as its first slot — so `…00.900Z` normalised to
    /// `…00Z` shifts every slot in the plan rather than merely respelling it.
    #[test]
    fn backfill_window_preserves_sub_second_precision() {
        let params = BackfillFormParams::parse(
            "2026-08-01T00:00:00.900Z",
            "2026-08-01T06:00:00.250Z",
            None,
            false,
        )
        .expect("a fractional-second window parses");
        assert_eq!(
            params.from, "2026-08-01T00:00:00.900Z",
            "the start must not be truncated to whole seconds"
        );
        assert_eq!(params.to, "2026-08-01T06:00:00.250Z");

        let request = params.to_request(true).expect("converts");
        assert_eq!(
            request.from.timestamp_subsec_millis(),
            900,
            "the request must carry the submitted precision"
        );
        assert_eq!(request.to.timestamp_subsec_millis(), 250);

        // A whole-second window still normalises to the clean spelling.
        let whole =
            BackfillFormParams::parse("2026-08-01T00:00:00Z", "2026-08-02T00:00:00Z", None, false)
                .expect("parses");
        assert_eq!(whole.from, "2026-08-01T00:00:00Z");
        assert_eq!(whole.to, "2026-08-02T00:00:00Z");
    }

    /// Codex #3: the scheduler's auto-pause (#360) sets `auto_paused_at` without
    /// setting `is_paused`, so a row can be non-firing with `is_paused = false`.
    /// Keying the row actions on `is_paused` alone showed an "Auto-paused" badge
    /// next to a **Pause** button, leaving no way to restore firing.
    #[test]
    fn an_auto_paused_schedule_is_offered_resume_not_pause() {
        let auto_paused = HarvestSchedule {
            is_paused: false,
            auto_paused_at: Some(chrono::Utc::now()),
            consecutive_failure_count: 5,
            ..make_schedule(Some("flaky_wf"), None, false)
        };
        assert!(
            schedule_is_resumable(&auto_paused),
            "an auto-paused schedule must be treated as resumable"
        );

        let id = auto_paused.id.to_string();
        let html = render_schedule_table(
            &[(ShardId::new(0), auto_paused)],
            false,
            &std::collections::HashMap::new(),
        )
        .into_string();
        assert!(
            html.contains(&format!("schedules/{id}/resume")),
            "an auto-paused row must offer Resume: {html}"
        );
        assert!(
            !html.contains(&format!("schedules/{id}/pause")),
            "an auto-paused row must not offer Pause: {html}"
        );
        assert!(
            html.contains("Auto-paused"),
            "the badge must still say why: {html}"
        );
    }

    /// A plainly active schedule is still offered Pause, and a hand-paused one
    /// Resume — the fix must not invert the ordinary cases.
    #[test]
    fn resumability_is_unchanged_for_ordinary_schedules() {
        let active = make_schedule(Some("active_wf"), None, false);
        assert!(!schedule_is_resumable(&active));

        let paused = make_schedule(Some("paused_wf"), None, true);
        assert!(schedule_is_resumable(&paused));

        let active_id = active.id.to_string();
        let html = render_schedule_table(
            &[(ShardId::new(0), active)],
            false,
            &std::collections::HashMap::new(),
        )
        .into_string();
        assert!(html.contains(&format!("schedules/{active_id}/pause")));
        assert!(!html.contains(&format!("schedules/{active_id}/resume")));
    }

    // -- Codex round 2 regressions --

    /// Codex #1: `max_runs = 0` is the engine's "unlimited" (every bound check
    /// guards on `max > 0`, pinned by `backfill_max_runs_zero_is_treated_as_unlimited`),
    /// so it must not render as a spent budget.
    #[test]
    fn a_zero_run_cap_reads_as_unlimited_not_spent() {
        let unlimited_by_zero = HarvestSchedule {
            max_runs: Some(0),
            runs_started: 12,
            ..make_schedule(Some("legacy_wf"), None, false)
        };
        assert_eq!(
            schedule_bounded_runs_label(&unlimited_by_zero),
            "—",
            "max_runs = 0 must not render a budget at all"
        );
        assert!(
            !schedule_is_bounded_out(&unlimited_by_zero, chrono::Utc::now()),
            "max_runs = 0 must never count as bounded out"
        );
        assert!(
            schedule_health(&unlimited_by_zero).is_healthy(),
            "a zero-cap schedule is unlimited, so it reads healthy"
        );
    }

    /// Codex #3: `exhausted_at` is stamped asynchronously, so a row can be
    /// terminal on its live bounds while the column is still NULL. Reading the
    /// column alone rendered such a row as a calm Active schedule that the
    /// health filter excluded and the sort put below the unhealthy rows.
    #[test]
    fn a_row_bounded_out_before_its_tick_stamped_it_reads_exhausted() {
        let now = chrono::Utc::now();

        let budget_spent = HarvestSchedule {
            max_runs: Some(5),
            runs_started: 5,
            exhausted_at: None,
            ..make_schedule(Some("spent_wf"), None, false)
        };
        assert!(schedule_is_bounded_out(&budget_spent, now));
        assert!(schedule_health_at(&budget_spent, now).exhausted);
        let html = render_schedule_health_badges(&budget_spent).into_string();
        assert!(html.contains("Exhausted"), "badge missing: {html}");
        assert!(
            html.contains("run budget spent"),
            "an unstamped exhaustion must still name its bound: {html}"
        );

        let past_cutoff = HarvestSchedule {
            end_at: Some(now - chrono::Duration::hours(1)),
            exhausted_at: None,
            ..make_schedule(Some("cutoff_wf"), None, false)
        };
        assert!(schedule_is_bounded_out(&past_cutoff, now));
        let html = render_schedule_health_badges(&past_cutoff).into_string();
        assert!(html.contains("past end_at"), "cutoff bound missing: {html}");

        // A cutoff still in the future is not bounded out.
        let future_cutoff = HarvestSchedule {
            end_at: Some(now + chrono::Duration::hours(1)),
            ..make_schedule(Some("future_wf"), None, false)
        };
        assert!(!schedule_is_bounded_out(&future_cutoff, now));
        assert!(schedule_health_at(&future_cutoff, now).is_healthy());
    }

    /// The live-bounds derivation must also drive the filter and the sort, not
    /// just the badge — that was the substance of the finding.
    #[test]
    fn a_live_bounded_out_row_is_filtered_and_sorted_as_unhealthy() {
        let now = chrono::Utc::now();
        let bounded_out = HarvestSchedule {
            max_runs: Some(3),
            runs_started: 3,
            exhausted_at: None,
            next_run_at: Some(now + chrono::Duration::hours(10)),
            ..make_schedule(Some("z_bounded"), None, false)
        };
        let healthy = HarvestSchedule {
            next_run_at: Some(now + chrono::Duration::minutes(1)),
            ..make_schedule(Some("a_healthy"), None, false)
        };

        let filters = ScheduleUiFilters {
            health: ScheduleHealthFilter::Unhealthy,
            ..Default::default()
        };
        assert!(
            filters.matches(ShardId::new(0), &bounded_out),
            "health=Unhealthy must include a row bounded out on live fields"
        );
        assert!(!filters.matches(ShardId::new(0), &healthy));

        let mut rows = vec![
            (ShardId::new(0), healthy),
            (ShardId::new(0), bounded_out.clone()),
        ];
        sort_schedule_rows(&mut rows);
        assert_eq!(
            rows[0].1.id, bounded_out.id,
            "a live-bounded-out row must sort above a healthy one despite firing later"
        );
    }

    // -- Codex round 3 regressions --

    /// Codex r3 #1: the `end_at` bound is about the pending slot, not the wall
    /// clock. `schedule_overdue` tests `next_run_at >= end_at`; comparing `now`
    /// is wrong in both directions.
    #[test]
    fn end_at_exhaustion_is_judged_on_the_pending_slot() {
        let now = chrono::Utc::now();
        let cutoff = now + chrono::Duration::hours(2);

        // Next slot is already past the cutoff, but the clock is not: the
        // scheduler will never fire this again, so it is bounded out.
        let slot_past_cutoff = HarvestSchedule {
            end_at: Some(cutoff),
            next_run_at: Some(cutoff + chrono::Duration::minutes(1)),
            ..make_schedule(Some("slot_past"), None, false)
        };
        assert!(
            schedule_is_bounded_out(&slot_past_cutoff, now),
            "a next slot at/past end_at means no legal slot remains"
        );

        // The clock has passed the cutoff, but an overdue slot from before it is
        // still legal and the tick will process it: NOT bounded out.
        let overdue_legal_slot = HarvestSchedule {
            end_at: Some(now - chrono::Duration::hours(1)),
            next_run_at: Some(now - chrono::Duration::hours(2)),
            ..make_schedule(Some("overdue_legal"), None, false)
        };
        assert!(
            !schedule_is_bounded_out(&overdue_legal_slot, now),
            "an overdue slot from before the cutoff is still fireable"
        );

        // A slot comfortably before the cutoff is fine.
        let healthy = HarvestSchedule {
            end_at: Some(cutoff),
            next_run_at: Some(now + chrono::Duration::minutes(5)),
            ..make_schedule(Some("healthy"), None, false)
        };
        assert!(!schedule_is_bounded_out(&healthy, now));

        // No pending slot at all: fall back to the wall clock.
        let no_slot_past_cutoff = HarvestSchedule {
            end_at: Some(now - chrono::Duration::hours(1)),
            next_run_at: None,
            ..make_schedule(Some("no_slot"), None, false)
        };
        assert!(schedule_is_bounded_out(&no_slot_past_cutoff, now));

        let no_slot_before_cutoff = HarvestSchedule {
            end_at: Some(cutoff),
            next_run_at: None,
            ..make_schedule(Some("no_slot_ok"), None, false)
        };
        assert!(!schedule_is_bounded_out(&no_slot_before_cutoff, now));
    }

    /// Codex r3 #2: `max_runs = 0` is unlimited, so the *preview* must not report
    /// a spent budget either. The list cell was corrected in round 2 while the
    /// shared computation still mapped the raw cap.
    #[test]
    fn a_zero_run_cap_is_unlimited_in_the_shared_budget_helper() {
        assert_eq!(
            crate::api::schedule_remaining_runs(Some(0), 12),
            None,
            "max_runs = 0 is unlimited, not a spent budget"
        );
        assert_eq!(crate::api::schedule_remaining_runs(Some(-1), 3), None);
        assert_eq!(crate::api::schedule_remaining_runs(None, 3), None);
        assert_eq!(crate::api::schedule_remaining_runs(Some(10), 4), Some(6));
        assert_eq!(
            crate::api::schedule_remaining_runs(Some(3), 9),
            Some(0),
            "a genuinely spent positive cap still reports zero"
        );
    }

    /// Codex r3 #3: an auto-paused schedule previews empty, and the page says
    /// why rather than blaming the expression.
    #[test]
    fn preview_page_explains_an_auto_paused_schedule() {
        let row = HarvestSchedule {
            is_paused: false,
            auto_paused_at: Some(chrono::Utc::now()),
            consecutive_failure_count: 4,
            ..make_schedule(Some("flaky_wf"), None, false)
        };
        let preview = crate::api::SchedulePreview {
            entries: vec![],
            is_paused: false,
            pause_reason: Some(
                "auto-paused after 4 consecutive failures; resume to restore firing".to_string(),
            ),
            from: chrono::Utc::now(),
            count_requested: 10,
            end_at: None,
            remaining_runs: None,
            exhausted_reason: None,
        };
        let html = render_schedule_preview_page(&row, ShardId::new(0), &preview, 10).into_string();
        assert!(
            html.contains("auto-paused"),
            "the banner must name auto-pause: {html}"
        );
        assert!(
            html.contains("4 consecutive failures"),
            "the reason must be surfaced: {html}"
        );
        assert!(
            !html.contains("produces no future firings"),
            "must not blame the expression for an auto-pause: {html}"
        );
    }

    /// The backfill confirmation interpolates the schedule UUID into its
    /// `confirm(...)` string, and passes it through `js_escape` on the way.
    #[test]
    fn backfill_confirm_handler_carries_only_escaped_identifiers() {
        let row = make_schedule(Some("evil'); alert(1);//"), None, false);
        let parse = |s: &str| {
            chrono::DateTime::parse_from_rfc3339(s)
                .expect("valid fixture timestamp")
                .with_timezone(&chrono::Utc)
        };
        let dry_run = crate::api::ScheduleBackfillResponse {
            status: "dry_run".to_string(),
            schedule_id: row.id,
            kind: crate::api::ScheduleKind::Workflow,
            name: "evil'); alert(1);//".to_string(),
            from: parse("2026-08-01T00:00:00Z"),
            to: parse("2026-08-02T00:00:00Z"),
            planned_timestamps: vec![parse("2026-08-01T00:00:00Z")],
            total: 1,
            dispatched: 1,
            skipped: 0,
            failed: 0,
            skipped_reasons: std::collections::HashMap::new(),
            partial_shard_failures: vec![],
            paused_schedule_warning: None,
        };
        let form = BackfillFormParams {
            from: "2026-08-01T00:00:00Z".to_string(),
            to: "2026-08-02T00:00:00Z".to_string(),
            max_count: None,
            include_paused: false,
        };
        let html =
            render_schedule_backfill_confirm(&row, ShardId::new(0), &dry_run, &form).into_string();
        for handler in onsubmit_attribute_values(&html) {
            assert!(
                !handler.contains("alert") && !handler.contains("evil"),
                "the schedule name must never reach the inline handler: {handler}"
            );
        }
    }
}
