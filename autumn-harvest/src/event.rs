//! Event types for the workflow event-sourcing engine.
//!
//! Every state change in a workflow execution is represented as an event
//! appended to `harvest_events`. Replay re-executes the workflow function
//! from the beginning, feeding recorded results back instead of re-executing
//! activities.
//!
//! **Append-only invariant:** Never remove or reorder variants. Stored JSON
//! must deserialize into the same variants after deployment.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::TimeoutType;
use crate::types::{
    ActivityExecId, ExecutionId, ExternalActivityToken, ExternalAwaitId, ExternalCancelId,
    ExternalSignalId, TimerId, UpdateId, WorkerId,
};

fn default_error_type() -> String {
    "Error".to_string()
}

/// Failure reason recorded on the synthetic terminal event of a dispatch that a
/// **failing decision cycle abandoned** (issue #952).
///
/// A workflow that dispatches awaited work (a child workflow, an activity) and
/// then returns `Err` from a sibling branch in the same decision cycle never
/// suspends, so the dispatch never becomes a queue row or an execution row. The
/// terminal persist path records what the code actually did — a
/// `ChildWorkflowStarted`/`ActivityScheduled` at the command's emission position
/// — immediately followed by a terminal carrying this reason, so the audit trail
/// neither drops the dispatch nor claims work that is still in flight.
///
/// Deliberately a plain message on the **existing** `ChildWorkflowFailed` /
/// `ActivityFailed` variants: issue #952 adds no `WorkflowEvent` variant, no
/// migration, and no new `reason_code` vocabulary.
pub const ABANDONED_DISPATCH_REASON: &str =
    "dispatch abandoned: the workflow failed in the same decision cycle";

/// Which deterministic built-in produced a [`WorkflowEvent::SideEffectRecorded`]
/// event (issue #384).
///
/// This is a **bounded** enum — it has a fixed, small set of variants and is
/// safe to use as a low-cardinality `OTel` attribute value (ADR-0001 §7). Each of
/// the `WorkflowContext` deterministic primitives lowers onto a single
/// `SideEffectRecorded` event and stamps the originating helper here so replay
/// diagnostics and metrics can distinguish a clock read from a UUID mint without
/// inspecting the recorded value.
///
/// **Append-only invariant:** never remove or rename a variant. The serialised
/// form is the bare variant name (`"Now"`, `"Uuid"`, …); stored events depend on
/// it. New helper kinds are added at the end.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SideEffectKind {
    /// `ctx.system_now()` / `ctx.system_time_now()` — a captured wall-clock instant.
    Now,
    /// `ctx.new_uuid()` — a captured `UUIDv7`.
    Uuid,
    /// `ctx.random_u64()` / `ctx.random_f64()` / `ctx.random_range(..)` — a captured draw.
    Random,
    /// `ctx.side_effect(name, f)` — an author-named one-shot value capture.
    Custom,
}

impl SideEffectKind {
    /// Stable, low-cardinality string label for metrics / diagnostics.
    ///
    /// These values are bounded (one per variant) and safe to use as an `OTel`
    /// attribute value per ADR-0001 §7.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Now => "now",
            Self::Uuid => "uuid",
            Self::Random => "random",
            Self::Custom => "custom",
        }
    }
}

/// All possible events in a workflow's history.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum WorkflowEvent {
    // ── Lifecycle ──────────────────────────────────────────────────
    /// A new workflow execution has started.
    WorkflowStarted {
        /// The JSON payload used to start the workflow.
        input: serde_json::Value,
        /// Time when the workflow was initiated.
        timestamp: DateTime<Utc>,
        /// Output of the most recent prior COMPLETED run of the same schedule (issue #488).
        /// `None` for the first run, for manual (non-scheduled) starts, and when no prior
        /// run succeeded. Frozen at workflow start time; replay always returns this value.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        last_completion_result: Option<serde_json::Value>,
        /// Failure summary of the most recent terminal run if it ended `FAILED` or `TIMED_OUT`
        /// (issue #488). `None` when the most recent terminal run `COMPLETED` (i.e. recovered),
        /// and `None` for manual starts. Frozen at workflow start time.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        last_error: Option<String>,
        /// The nominal scheduled fire-time (logical slot) this run is responsible for
        /// (issue #508). `Some` for scheduled / backfilled / caught-up runs; `None` for
        /// direct/manual API starts, ad-hoc trigger-now, and pre-#508 histories. This is
        /// the pre-jitter logical slot (`scheduled_for`), NOT `effective_fire_time` and
        /// NOT the execution start wall-clock (`timestamp`). Frozen at start; replay-stable.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        scheduled_time: Option<DateTime<Utc>>,
    },
    /// The workflow ran to completion without an error.
    WorkflowCompleted {
        /// The JSON result returned by the workflow function.
        output: serde_json::Value,
    },
    /// The workflow panicked or returned a non-recoverable error.
    ///
    /// ## Backward-compatibility note (issue #767)
    ///
    /// `error_type`, `details`, and `non_retryable` were added after the
    /// initial release. Old events stored without these fields deserialise
    /// cleanly via `#[serde(default)]` to `None`. Unlike `ActivityFailed`,
    /// `error_type` is `Option<String>` (not defaulted to `"Error"`) so a
    /// pre-#767 untyped failure is distinguishable from a typed one. The
    /// append-only invariant is preserved — no variants removed, no renames.
    WorkflowFailed {
        /// Human-readable string representation of the failure.
        error: String,
        /// Low-cardinality error-type name from a typed
        /// [`WorkflowFailure`](crate::failure::WorkflowFailure).
        ///
        /// `None` for events stored before issue #767 or for failures returned
        /// via the legacy `Err(String)` path.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error_type: Option<String>,
        /// Optional structured details preserved from
        /// [`WorkflowFailure::with_details`](crate::failure::WorkflowFailure::with_details).
        ///
        /// `None` for untyped or pre-#767 failures. Omitted from the serialised
        /// form when `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        details: Option<serde_json::Value>,
        /// Advisory non-retryable classification hint from a typed
        /// [`WorkflowFailure`](crate::failure::WorkflowFailure).
        ///
        /// `None` for untyped or pre-#767 failures. This is a downstream
        /// classification hint (caller / completion-trigger), **not** a control
        /// input to the engine's workflow-level retry (#523) loop (issue #767
        /// scope).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        non_retryable: Option<bool>,
    },
    /// The workflow was intentionally cancelled (e.g., via API or parent workflow).
    WorkflowCancelled {
        /// The reason given for cancellation.
        reason: String,
    },

    // ── Activities ────────────────────────────────────────────────
    /// An activity was requested by the workflow.
    ActivityScheduled {
        /// Unique ID for this specific activity attempt.
        activity_id: ActivityExecId,
        /// The name of the registered activity handler.
        name: String,
        /// JSON input for the activity.
        input: serde_json::Value,
        /// Target worker queue.
        queue: String,
    },
    /// A worker picked up the activity and began executing it.
    ActivityStarted {
        /// Unique ID for this specific activity attempt.
        activity_id: ActivityExecId,
        /// The worker instance running the activity.
        worker_id: WorkerId,
    },
    /// The activity finished executing successfully.
    ActivityCompleted {
        /// Unique ID for this specific activity attempt.
        activity_id: ActivityExecId,
        /// The JSON result returned by the activity.
        output: serde_json::Value,
    },
    /// The activity returned an error or panicked.
    ///
    /// ## Backward-compatibility note (issue #227)
    ///
    /// `error_type` and `non_retryable` were added after the initial release.
    /// Old events stored without these fields deserialise cleanly via
    /// `#[serde(default)]`: `error_type` falls back to `"Error"` and
    /// `non_retryable` falls back to `false`. The append-only invariant is
    /// preserved — no variants removed, no renames.
    ActivityFailed {
        /// Unique ID for this specific activity attempt.
        activity_id: ActivityExecId,
        /// Human-readable string representation of the failure.
        error: String,
        /// How many times the activity has failed so far.
        attempt: u32,
        /// Low-cardinality error-type name for metrics and policy matching.
        ///
        /// Defaults to `"Error"` for events stored before issue #227.
        #[serde(default = "default_error_type")]
        error_type: String,
        /// When `true`, the worker skipped retry and routed to DLQ immediately.
        ///
        /// Defaults to `false` for events stored before issue #227.
        #[serde(default)]
        non_retryable: bool,
        /// Optional structured details preserved from
        /// [`ActivityFailure::with_details`](crate::failure::ActivityFailure::with_details).
        ///
        /// Defaults to `None` for events stored before issue #227 or for
        /// failures returned via the legacy `Err(String)` path. Omitted from
        /// the serialised form when `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        details: Option<serde_json::Value>,
    },
    /// The activity exceeded its allocated `start_to_close` or `heartbeat` timeout.
    ActivityTimedOut {
        /// Unique ID for this specific activity attempt.
        activity_id: ActivityExecId,
        /// Which timeout triggered the failure.
        timeout_type: TimeoutType,
    },
    /// The activity successfully sent a heartbeat.
    ActivityHeartbeat {
        /// Unique ID for this specific activity attempt.
        activity_id: ActivityExecId,
        /// JSON payload attached to the heartbeat, used to resume progress after failures.
        details: serde_json::Value,
    },

    // ── Timers ────────────────────────────────────────────────────
    /// The workflow requested a durable sleep/timer.
    TimerStarted {
        /// The ID used to wake the workflow later.
        timer_id: TimerId,
        /// Duration in seconds (Duration is not JSON-serializable natively).
        duration_secs: u64,
    },
    /// The requested timer elapsed and the workflow can wake up.
    TimerFired {
        /// The ID that just finished waiting.
        timer_id: TimerId,
    },

    // ── Signals ───────────────────────────────────────────────────
    /// An external signal arrived while the workflow was waiting.
    SignalReceived {
        /// Name of the signal channel.
        signal_name: String,
        /// JSON payload delivered by the signal.
        payload: serde_json::Value,
    },

    // ── Child workflows ───────────────────────────────────────────
    /// A sub-workflow was scheduled by a parent workflow.
    ChildWorkflowStarted {
        /// The ID of the spawned execution.
        child_id: ExecutionId,
        /// The target child workflow handler.
        workflow_name: String,
        /// The input passed to the child workflow.
        input: serde_json::Value,
    },
    /// The spawned sub-workflow completed successfully.
    ChildWorkflowCompleted {
        /// The ID of the spawned execution.
        child_id: ExecutionId,
        /// Result of the completed workflow.
        output: serde_json::Value,
    },
    /// The spawned sub-workflow encountered a fatal error.
    ///
    /// ## Backward-compatibility note (issue #767)
    ///
    /// `error_type`, `details`, and `non_retryable` were added after the
    /// initial release and deserialise to `None` for pre-#767 events. See
    /// [`WorkflowFailed`](Self::WorkflowFailed) for the full rationale.
    ChildWorkflowFailed {
        /// The ID of the spawned execution.
        child_id: ExecutionId,
        /// The reason the child failed (human-readable message).
        error: String,
        /// Low-cardinality error-type name from a typed
        /// [`WorkflowFailure`](crate::failure::WorkflowFailure).
        ///
        /// `None` for pre-#767 or untyped child failures.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error_type: Option<String>,
        /// Optional structured details from a typed
        /// [`WorkflowFailure`](crate::failure::WorkflowFailure).
        ///
        /// `None` for pre-#767 or untyped child failures.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        details: Option<serde_json::Value>,
        /// Advisory non-retryable classification hint from a typed
        /// [`WorkflowFailure`](crate::failure::WorkflowFailure).
        ///
        /// `None` for pre-#767 or untyped child failures. This is a downstream
        /// classification hint (caller / completion-trigger), **not** a control
        /// input to the engine's workflow-level retry (#523) loop (issue #767
        /// scope).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        non_retryable: Option<bool>,
    },

    // ── Markers ───────────────────────────────────────────────────
    /// An explicit version change or side-effect marker was recorded.
    MarkerRecorded {
        /// The name of the recorded side effect (e.g. version block name).
        name: String,
        /// Arbitrary data recorded with the marker.
        details: serde_json::Value,
    },

    // ── Continue-as-new ───────────────────────────────────────────
    /// The workflow signalled `continue_as_new`. This is the terminal event
    /// for the current execution; a fresh execution sharing the same logical
    /// `WorkflowId` is started in its place with the recorded `input`.
    WorkflowContinuedAsNew {
        /// The execution ID of the freshly started run that succeeds this one.
        new_exec_id: ExecutionId,
        /// The JSON payload passed to the next iteration of the workflow.
        input: serde_json::Value,
        /// Registered workflow type the successor runs as (issue #803).
        ///
        /// `None` — the overwhelmingly common case, and what every event
        /// written before #803 deserializes to — means **same type**: the
        /// successor inherits the predecessor's `workflow_name` and carries
        /// its lifecycle columns forward verbatim, exactly as before.
        ///
        /// `Some(type)` means the author called
        /// [`continue_as_new_as_type`](crate::context::WorkflowContext::continue_as_new_as_type)
        /// (or its typed sibling `continue_as_new_as`): the successor runs as
        /// `type` and resolves its lifecycle defaults from *that* type's
        /// [`WorkflowInfo`](crate::info::WorkflowInfo).
        ///
        /// Additive and `skip_serializing_if`-guarded, so a pre-#803 history
        /// round-trips byte-identically and replays unchanged (no new event
        /// variant, no migration).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        new_workflow_type: Option<String>,
    },

    // ── Local activities (issue #98) ─────────────────────────────────────
    /// A local activity was dispatched for inline execution on the workflow
    /// worker. Unlike regular activities, local activities never write a row to
    /// `harvest_task_queue`; the worker runs the handler in the same Tokio task
    /// that drives the workflow loop.
    LocalActivityScheduled {
        /// Unique ID for this local activity attempt sequence.
        activity_id: ActivityExecId,
        /// The name of the registered activity handler.
        name: String,
        /// JSON input for the activity.
        input: serde_json::Value,
        /// Disambiguation marker distinguishing a #620+ event whose defaults were
        /// fully resolved at first schedule from a genuine pre-#620 legacy event
        /// (issue #620, Codex P2). BOTH `retry_policy` and `start_to_close_nanos`
        /// below serialize as absent when they resolved to "no floor", so an
        /// absent value alone cannot tell a resolved-to-nothing #620+ event apart
        /// from a legacy event — the former MUST stay frozen (implicit 1-attempt /
        /// no STC floor) on recovery, the latter MUST fall back to live
        /// re-derivation for backward compat. This one marker covers both frozen
        /// fields: every #620+ schedule writes it `true` at the same first-schedule
        /// anchor; a pre-#620 event has the field absent → deserializes to `false`.
        #[serde(default)]
        resolved: bool,
        /// The fully-resolved retry policy this local activity was scheduled
        /// with (call-site → activity default → builder default; issue #620).
        ///
        /// Additive/optional: pre-#620 events deserialize to `None`. On a #620+
        /// event (`resolved == true`), a recorded `None` means "explicitly
        /// resolved to no retry floor" and MUST stay frozen (implicit 1 attempt);
        /// a legacy event (`resolved == false`) with `None` falls back to today's
        /// live re-derivation. Frozen here so a local activity mid-retry across a
        /// worker crash keeps its ORIGINAL retry budget even if the builder-level
        /// default (`WorkerConfig::with_default_activity_retry_policy`) is changed
        /// in the crash-recovery window (AC8: an in-flight execution is unaffected
        /// by later default changes). Recorded on the live frontier at first
        /// schedule; read on crash-recovery replay — the established side-effect
        /// pattern.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        retry_policy: Option<crate::policy::RetryPolicy>,
        /// The fully-resolved `start_to_close` this local activity was scheduled
        /// with, in **nanoseconds** (call-site → activity default → builder
        /// default; issue #620, Codex P2). Nanoseconds — the FULL `Duration`
        /// precision — so no floor is ever truncated: the local per-attempt
        /// timeout is `Duration::from_nanos(this).min(cap)`. Truncating at any
        /// coarser unit is the same footgun one order down (a seconds field
        /// zeroes a subsecond floor; a millis field zeroes a sub-millisecond
        /// floor like `Duration::from_micros(500)`), so the full `Duration` is
        /// carried end-to-end (`u64` nanos ≈ 584 years — never saturates for a
        /// realistic timeout).
        ///
        /// Additive/optional, same freeze semantics as `retry_policy`: on a #620+
        /// event (`resolved == true`) a recorded value — `Some` OR `None` — is
        /// authoritative on recovery (`None` = "explicitly resolved to no STC
        /// floor", frozen); a legacy event (`resolved == false`) falls back to
        /// live re-derivation.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        start_to_close_nanos: Option<u64>,
    },
    /// A local activity finished executing successfully.
    LocalActivityCompleted {
        /// Unique ID matching the corresponding `LocalActivityScheduled`.
        activity_id: ActivityExecId,
        /// The JSON result returned by the activity handler.
        output: serde_json::Value,
    },
    /// A local activity attempt returned an error or the handler panicked.
    ///
    /// Multiple `LocalActivityFailed` events may appear in sequence (one per
    /// attempt) before a terminal `LocalActivityCompleted` (retry eventually
    /// succeeded) or before the retry budget is exhausted (followed by a
    /// `LocalActivityExhausted` event that marks the terminal state).
    LocalActivityFailed {
        /// Unique ID matching the corresponding `LocalActivityScheduled`.
        activity_id: ActivityExecId,
        /// String representation of the failure.
        error: String,
        /// How many attempts have been made so far (1-based).
        attempt: u32,
    },

    // ── External activity completion (issue #92) ──────────────────────
    /// An external activity was scheduled and is awaiting completion via the
    /// management API task-token endpoint. The `token` is a single-use handle
    /// that external systems round-trip through `/activities/external/{token}/complete`.
    ActivityAwaitingExternal {
        /// Unique ID for this specific activity attempt.
        activity_id: ActivityExecId,
        /// Opaque token that external systems use to complete or fail the activity.
        token: ExternalActivityToken,
        /// The name of the registered activity handler.
        name: String,
        /// JSON input for the activity.
        input: serde_json::Value,
        /// Target worker queue (informational; external activities don't occupy a slot).
        queue: String,
        /// Maximum seconds the external system has to deliver a result.
        schedule_to_close_secs: u64,
    },
    /// An external system delivered a successful result via the management API.
    ActivityCompletedExternally {
        /// Unique ID for this specific activity attempt.
        activity_id: ActivityExecId,
        /// Token that was used to complete the activity.
        token: ExternalActivityToken,
        /// The JSON result returned by the external system.
        output: serde_json::Value,
    },
    /// An external system reported a failure via the management API.
    ActivityFailedExternally {
        /// Unique ID for this specific activity attempt.
        activity_id: ActivityExecId,
        /// Token that was used to fail the activity.
        token: ExternalActivityToken,
        /// String representation of the failure.
        error: String,
        /// Whether the failure is retryable per the activity's `RetryPolicy`.
        retryable: bool,
    },
    /// The schedule-to-close deadline for an external activity was extended via
    /// the management API heartbeat endpoint.
    ActivityExternalDeadlineExtended {
        /// Unique ID for this specific activity attempt.
        activity_id: ActivityExecId,
        /// Token of the activity whose deadline was extended.
        token: ExternalActivityToken,
    },

    // ── Updates (issue #140) ──────────────────────────────────────────────
    /// An update request passed its validator and was durably admitted into
    /// the workflow's event history. The `update_id` correlates with the
    /// paired `UpdateCompleted` or `UpdateFailed` event.
    ///
    /// Validator failures leave **no trace** in history — only admitted updates
    /// produce this event.
    UpdateAdmitted {
        /// Unique ID for this update invocation. Stable across worker restarts.
        update_id: UpdateId,
        /// The name of the registered update handler.
        name: String,
        /// JSON payload delivered with the update request.
        input: serde_json::Value,
        /// Time when the update was admitted.
        timestamp: DateTime<Utc>,
    },
    /// The update handler ran to completion and returned a value.
    UpdateCompleted {
        /// Unique ID matching the corresponding `UpdateAdmitted`.
        update_id: UpdateId,
        /// The JSON result returned by the update handler.
        output: serde_json::Value,
    },
    /// The update handler returned an error.
    UpdateFailed {
        /// Unique ID matching the corresponding `UpdateAdmitted`.
        update_id: UpdateId,
        /// String representation of the handler error.
        error: String,
    },

    // ── Workflow reset (issue #148) ─────────────────────────────────────────
    /// Marker appended to the forked execution after the carried-over history.
    ///
    /// Replay treats this as informational: it records why the fork exists
    /// without corresponding to a workflow command.
    WorkflowResetFork {
        /// The source execution that was reset.
        reset_from_exec_id: ExecutionId,
        /// Last source event copied into this fork.
        reset_to_event_id: i64,
        /// Operator-supplied recovery reason.
        reason: String,
        /// Operator identity for audit.
        operator_id: String,
    },
    /// Marker appended to the source execution when a reset fork supersedes it.
    WorkflowResetTerminated {
        /// The forked execution that should continue forward.
        reset_to_exec_id: ExecutionId,
        /// Operator-supplied recovery reason.
        reason: String,
        /// Operator identity for audit.
        operator_id: String,
    },
    /// All retry attempts for a local activity were exhausted. Appended
    /// immediately after the final `LocalActivityFailed` event so replay can
    /// identify the terminal state without knowing the current retry policy.
    ///
    /// This makes the terminal-vs-in-progress distinction policy-invariant:
    /// if this event is present the activity is unambiguously done; if only
    /// `LocalActivityFailed` events are present (without a following
    /// `LocalActivityExhausted`) the worker crashed between retries and must
    /// continue from the next attempt.
    LocalActivityExhausted {
        /// Unique ID matching the corresponding `LocalActivityScheduled`.
        activity_id: ActivityExecId,
        /// Error from the final attempt.
        error: String,
        /// Total attempts that were made (equals `max_attempts`).
        attempt: u32,
    },

    // ── External workflow signals (issue #330, by-id targeting #751) ─────────
    /// A workflow requested delivery of a named signal to another workflow,
    /// addressed either by a specific `ExecutionId` or by a stable
    /// `(workflow_name, workflow_id)` business key resolved to the current
    /// run at delivery time (issue #751).
    ///
    /// The `signal_id` correlates this request with its terminal outcome event
    /// (`ExternalSignalDelivered` or `ExternalSignalFailed`). On replay the
    /// caller's context returns the recorded outcome without re-issuing the
    /// side effect.
    ///
    /// `target`'s type widened from a bare `ExecutionId` to
    /// [`crate::types::ExternalTarget`] in issue #751 — `#[serde(untagged)]`
    /// makes every pre-#751 stored event (a bare UUID string) deserialize
    /// unchanged into the `ExecutionId` variant.
    ExternalSignalRequested {
        /// Correlation ID linking this event to its terminal outcome.
        signal_id: ExternalSignalId,
        /// The workflow that should receive the signal.
        target: crate::types::ExternalTarget,
        /// Name of the signal channel on the receiving workflow.
        signal_name: String,
        /// JSON payload to deliver to the receiving workflow.
        payload: serde_json::Value,
        /// Additive optional exactly-once delivery key; dedups re-issued
        /// requests against `uq_harvest_signals_idem`. Older events load as `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        idempotency_key: Option<String>,
    },
    /// The signal was successfully inserted into the target workflow's signal
    /// queue (or durably queued via the outbox for cross-shard delivery).
    ExternalSignalDelivered {
        /// Correlation ID matching the corresponding `ExternalSignalRequested`.
        signal_id: ExternalSignalId,
    },
    /// The signal could not be delivered. The `reason_code` is one of:
    /// - `"target_terminal"` — the target execution is already in a terminal
    ///   state (`ExecutionId`-targeted requests only).
    /// - `"not_running"` — the resolved current run for a `workflow_id`
    ///   target is definitively terminal (issue #751; no grace window needed,
    ///   since a continue-as-new always leaves an active successor behind
    ///   atomically, so this observation can never be a false negative).
    /// - `"target_unknown"` — no execution with the given ID (or business key)
    ///   was ever found, after the configured grace window.
    ExternalSignalFailed {
        /// Correlation ID matching the corresponding `ExternalSignalRequested`.
        signal_id: ExternalSignalId,
        /// Machine-readable reason code (`"target_terminal"`,
        /// `"not_running"`, or `"target_unknown"`).
        reason_code: String,
    },

    // ── Detached child workflow spawn (issue #347) ───────────────────────────
    /// A child workflow was spawned in **detached** mode: the parent does not
    /// suspend awaiting the child's terminal result. The `parent_close_policy`
    /// determines what happens to this child when the parent reaches a terminal
    /// state.
    ///
    /// This is an **append-only** variant — old histories that contain only
    /// `ChildWorkflowStarted` rows will still deserialize correctly because
    /// this variant is new and independent.
    ChildWorkflowSpawnedDetached {
        /// The execution ID of the spawned child.
        child_id: ExecutionId,
        /// The name of the child workflow handler.
        workflow_name: String,
        /// The input passed to the child workflow.
        input: serde_json::Value,
        /// Policy applied to this child when the parent reaches a terminal state.
        parent_close_policy: crate::types::ParentClosePolicy,
    },

    /// The executor applied a parent-close cascade policy to a detached child.
    ///
    /// Recorded once per child immediately after the cascade action is taken so
    /// that replay is deterministic — re-running the history never re-fires the
    /// cascade.
    ///
    /// `action` is one of `"request_cancel"` or `"terminate"` (never `"abandon"`,
    /// which is a no-op).
    ChildWorkflowCascadeApplied {
        /// The execution ID of the child to which cascade was applied.
        child_id: ExecutionId,
        /// The policy that triggered this cascade.
        policy: crate::types::ParentClosePolicy,
        /// Machine-readable action taken: `"request_cancel"` or `"terminate"`.
        action: String,
    },

    // ── Workflow execution timeout (issue #243) ───────────────────────────────
    /// The workflow execution exceeded its configured `execution_timeout` wall-clock
    /// deadline and was forcibly terminated by the timeout scanner.
    ///
    /// This is a **terminal** lifecycle event: once appended, the execution row
    /// transitions to `TIMED_OUT` and no further events are written.
    ///
    /// ## Replay determinism
    ///
    /// The `deadline` field is the absolute UTC instant computed at start time
    /// (`started_at + execution_timeout`). Re-running history always produces the
    /// same `WorkflowExecutionTimedOut` event without consulting the live clock —
    /// the timeout decision is derivable from the stored `deadline` alone.
    WorkflowExecutionTimedOut {
        /// The absolute deadline that was exceeded (`started_at + execution_timeout`).
        deadline: DateTime<Utc>,
        /// The actual wall-clock time when the scanner detected and enforced the timeout.
        timed_out_at: DateTime<Utc>,
    },

    // ── Deterministic side-effect primitives (issue #384) ─────────────────────
    /// A deterministic value was captured during live execution and frozen into
    /// history so subsequent replays return the identical value.
    ///
    /// All of the `WorkflowContext` deterministic primitives — `system_now()`,
    /// `system_time_now()`, `new_uuid()`, `random_u64()`, `random_f64()`,
    /// `random_range()`, and `side_effect()` — lower onto this single variant so
    /// the event-schema cost of the feature is paid exactly once. The `kind`
    /// discriminator (a bounded enum) records which helper produced the value;
    /// `name` is `Some` only for `side_effect()` (the author-supplied dedup key)
    /// and `None` for the built-in unnamed helpers; `value` is the recorded JSON
    /// result returned on every replay.
    ///
    /// This is an **append-only** variant added at the end of the enum. The
    /// `harvest_events` table stores it as opaque JSON, so no migration is
    /// required. `name` is omitted from the serialised form when `None`.
    SideEffectRecorded {
        /// Which built-in helper produced this value.
        kind: SideEffectKind,
        /// Author-supplied dedup key for `side_effect()`; `None` for built-ins.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        /// The recorded JSON value, replayed verbatim on every subsequent pass.
        value: serde_json::Value,
    },

    // ── Pause / Resume (issue #383) ───────────────────────────────────────────
    /// An operator paused this execution. While paused the executor refuses to
    /// dispatch new commands (activities, timers, child workflows); in-flight
    /// activities continue to completion and their results are recorded
    /// normally. This is a **non-terminal** lifecycle event — the execution
    /// resumes from exactly this point on
    /// [`WorkflowExecutionResumed`](Self::WorkflowExecutionResumed).
    ///
    /// ## Replay determinism
    ///
    /// Pause/resume events are no-ops for command dispatch during replay: the
    /// [`crate::replay::HistoryMatcher`] skips them transparently, so a recorded
    /// pause/resume pair never alters the command sequence reconstructed from
    /// history.
    WorkflowExecutionPaused {
        /// Wall-clock time the pause was applied.
        paused_at: DateTime<Utc>,
        /// Optional operator-supplied reason (max 500 chars, enforced at the API
        /// boundary). `None` when no reason was given.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
        /// Identity of the operator (or `"auto-resume(timeout)"` peer) that
        /// requested the pause, captured from the audit trail.
        actor: String,
    },
    /// An operator (or the bounded-pause auto-resume scanner) resumed a paused
    /// execution. The executor re-arms and the workflow advances on its next
    /// decision attempt; timers whose fire time elapsed while paused fire
    /// immediately in their original order, and signals queued during the pause
    /// are delivered in order.
    WorkflowExecutionResumed {
        /// Wall-clock time the resume was applied.
        resumed_at: DateTime<Utc>,
        /// Identity that requested the resume. `"auto-resume(timeout)"` when the
        /// bounded-pause scanner resumed an over-long pause.
        actor: String,
    },

    // ── External workflow cancellation (issue #492, by-id targeting #751) ────
    /// A workflow requested cancellation of another workflow, addressed
    /// either by a specific `ExecutionId` or by a stable
    /// `(workflow_name, workflow_id)` business key resolved to the current
    /// run at delivery time (issue #751).
    ///
    /// The `cancel_id` correlates this request with its terminal outcome event
    /// (`ExternalCancelDelivered` or `ExternalCancelFailed`). On replay the
    /// caller's context returns the recorded outcome without re-issuing the
    /// side effect. Unlike signal, no payload is carried — the cancel is
    /// target-only.
    ///
    /// `target`'s type widened from a bare `ExecutionId` to
    /// [`crate::types::ExternalTarget`] in issue #751 — see
    /// `ExternalSignalRequested`'s doc comment for the backward-compatibility
    /// argument (identical here).
    ExternalCancelRequested {
        /// Correlation ID linking this event to its terminal outcome.
        cancel_id: ExternalCancelId,
        /// The workflow to cancel.
        target: crate::types::ExternalTarget,
    },
    /// The cancel was successfully applied to the target workflow (or the
    /// target was already in a terminal state — no-op success).
    ExternalCancelDelivered {
        /// Correlation ID matching the corresponding `ExternalCancelRequested`.
        cancel_id: ExternalCancelId,
    },
    /// The cancel could not be delivered. The `reason_code` is one of:
    /// - `"target_unknown"` — no execution with the given ID (or business
    ///   key) was ever found, after the configured grace window.
    ExternalCancelFailed {
        /// Correlation ID matching the corresponding `ExternalCancelRequested`.
        cancel_id: ExternalCancelId,
        /// Machine-readable reason code (`"target_unknown"`).
        reason_code: String,
    },

    // ── DLQ redrive reactivation (issue #510) ─────────────────────────────────
    /// An operator redrove a dead-lettered task whose owning execution had been
    /// sealed `FAILED` at quarantine time. This event reopens the execution: it
    /// is appended **after** the superseded terminal
    /// [`WorkflowFailed`](Self::WorkflowFailed) and the execution transitions
    /// `FAILED` → `RUNNING` so the re-enqueued task resumes from existing
    /// history (append-only; no prior event is rewritten).
    ///
    /// ## Replay determinism
    ///
    /// `WorkflowRedriven` is a **non-terminal** lifecycle event and is a no-op
    /// for command dispatch during replay: the
    /// [`crate::replay::HistoryMatcher`] marks both this event **and** the
    /// `WorkflowFailed` it supersedes as transparent, so the reconstructed
    /// command sequence advances past the reopened terminal instead of
    /// diverging against it. A bare trailing `WorkflowFailed` with no following
    /// `WorkflowRedriven` stays non-transparent (a genuinely failed run).
    ///
    /// The same superseded cycle can leave other records behind its last
    /// dispatch. Those are a marker, a side effect, a detached spawn, or
    /// a timer arm or cancel.
    /// [`crate::replay::HistoryMatcher::superseded_cycle_tail_indices`]
    /// marks those transparent too (issue #1262). A re-issued dispatch
    /// still advances past them instead of diverging.
    WorkflowRedriven {
        /// Wall-clock time the redrive reactivation was applied.
        redriven_at: DateTime<Utc>,
        /// The `harvest_dead_letters` row that triggered this reactivation.
        dead_letter_id: Uuid,
        /// Optional operator-supplied reason. `None` when no reason was given.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },

    // ── Workflow-level retry linkage (issue #523) ─────────────────────────────────
    /// Appended to a sealed `FAILED` execution immediately after `WorkflowFailed`
    /// when the engine auto-schedules a retry run.
    ///
    /// ## Replay determinism
    ///
    /// This event lives ONLY in the sealed failed run's history and is appended
    /// **after** the terminal `WorkflowFailed` event. The
    /// [`crate::replay::HistoryMatcher`] stops consuming events after a terminal
    /// lifecycle event, so this trailing event is transparent to replay: the
    /// failed run is already sealed; its retry is a separate execution.
    WorkflowRetryScheduled {
        /// The execution ID of the newly-created retry run.
        retry_exec_id: ExecutionId,
        /// Attempt number of the NEW run (= `failed_run.workflow_attempt` + 1).
        attempt: u32,
        /// Wall-clock instant when the retry becomes claimable by a worker.
        /// `Utc::now()` for immediate retries, or `now + backoff` for delayed ones.
        fire_at: DateTime<Utc>,
    },

    // ── Cancellable / renewable durable timers (issue #768) ───────────────────────
    /// Records that an author-controlled durable timer was cancelled (or reset).
    ///
    /// Emitted by the non-suspending [`crate::context::WorkflowCommand::CancelTimer`]
    /// bookkeeping command (via `handle.cancel()`, `ctx.cancel_timer(id)`, or the
    /// cancel half of `handle.reset(..)`). The corresponding `harvest_timers` row is
    /// deleted in the same transaction, so no [`Self::TimerFired`] is ever produced
    /// for a cancelled timer.
    ///
    /// ## Replay determinism
    ///
    /// A `TimerCancelled { timer_id }` recorded **before** any `TimerFired` with the
    /// same id resolves the timer's observed outcome to
    /// [`crate::context::TimerOutcome::Cancelled`]. It is a command-bearing event
    /// that may interleave with unrelated activity/timer/child scans, so every
    /// forward scan loop in [`crate::replay::HistoryMatcher`] skips it out of order
    /// (mirroring the `TimerFired` / external-cancel skip pattern).
    ///
    /// Appended at the END of the enum; pre-#768 histories that never contain this
    /// variant deserialize unchanged.
    TimerCancelled {
        /// The id of the timer that was cancelled.
        timer_id: TimerId,
    },

    // ── External workflow await (issue #757) ──────────────────────────────────
    /// A workflow requested to durably await the terminal outcome of another
    /// workflow execution by `ExecutionId`.
    ///
    /// The `await_id` correlates this request with its terminal outcome event
    /// (`ExternalAwaitResolved` or `ExternalAwaitFailed`). On replay the
    /// caller's context returns the recorded outcome without re-observing the
    /// target. Unlike cancel/signal, the await is **observe-only** — it never
    /// establishes parent/child linkage, cancels, or otherwise affects the
    /// target's lifecycle.
    ExternalAwaitRequested {
        /// Correlation ID linking this event to its terminal outcome.
        await_id: ExternalAwaitId,
        /// The execution ID of the workflow to await.
        target: ExecutionId,
    },
    /// The awaited target reached a `COMPLETED` terminal state. Carries the
    /// target's recorded output (inflated), frozen into the awaiter's own
    /// history so replay is deterministic.
    ExternalAwaitResolved {
        /// Correlation ID matching the corresponding `ExternalAwaitRequested`.
        await_id: ExternalAwaitId,
        /// The target workflow's terminal output value.
        output: serde_json::Value,
    },
    /// The awaited target reached a non-`COMPLETED` terminal state
    /// (`FAILED`/`TIMED_OUT`/`CANCELLED`/`TERMINATED`), or the target could not
    /// be found after the configured grace window.
    ///
    /// `reason_code` is one of:
    /// - `"target_failed"` — the target reached `FAILED`.
    /// - `"target_timed_out"` — the target reached `TIMED_OUT`.
    /// - `"target_cancelled"` — the target reached `CANCELLED`.
    /// - `"target_terminated"` — the target reached `TERMINATED`.
    /// - `"target_unknown"` — no execution matching `target` was found within
    ///   the grace window.
    ///
    /// For a `FAILED` target the `message`/`error_type`/`details`/`non_retryable`
    /// carry the target's typed [`crate::failure::WorkflowFailure`] (decoded via
    /// [`crate::failure::decode_workflow_failure`]); for transport failures or
    /// untyped failures they are `None`.
    ExternalAwaitFailed {
        /// Correlation ID matching the corresponding `ExternalAwaitRequested`.
        await_id: ExternalAwaitId,
        /// Machine-readable reason code (see variant docs).
        reason_code: String,
        /// Human-readable failure message from the target's terminal cause.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message: Option<String>,
        /// Stable error-type name from a typed target failure; `None` otherwise.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error_type: Option<String>,
        /// Structured details from a typed target failure; `None` otherwise.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        details: Option<serde_json::Value>,
        /// Advisory non-retryable flag from a typed target failure; `None` otherwise.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        non_retryable: Option<bool>,
    },
    /// A durable mutex was acquired (issue #691). Records exclusive access to
    /// `key` with the monotonic fencing token `lock_seq`. This is the replay
    /// anchor for `ctx.mutex(key).acquire()`.
    ///
    /// Release is EVENT-LESS bookkeeping (mirrors `SetCurrentDetails`) — there
    /// is intentionally NO `MutexReleased` event, so no forward-scan skip-list
    /// needs updating.
    MutexGranted {
        /// The lock key that was acquired.
        key: String,
        /// Monotonic fencing token minted for this grant.
        lock_seq: i64,
        /// Wall-clock instant the lock was granted.
        acquired_at: DateTime<Utc>,
    },
}

impl WorkflowEvent {
    /// Forward-compatible constructor for [`WorkflowEvent::WorkflowStarted`] that
    /// defaults the carryover (#488) and scheduled-time (#508) additive fields to
    /// `None`. Gives downstream code a non-breaking construction path across future
    /// additive field additions to this variant (issue #688).
    #[must_use]
    pub const fn workflow_started(
        input: serde_json::Value,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Self {
        Self::WorkflowStarted {
            input,
            timestamp,
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }
    }

    /// Construct an untyped [`WorkflowEvent::WorkflowFailed`] (issue #767): the
    /// typed fields default to `None`, preserving pre-#767 legacy semantics.
    #[must_use]
    pub fn workflow_failed(error: impl Into<String>) -> Self {
        Self::WorkflowFailed {
            error: error.into(),
            error_type: None,
            details: None,
            non_retryable: None,
        }
    }

    /// Construct a typed [`WorkflowEvent::WorkflowFailed`] from a decoded
    /// [`WorkflowFailure`](crate::failure::WorkflowFailure) payload (issue #767).
    #[must_use]
    pub fn workflow_failed_typed(decoded: &crate::failure::DecodedWorkflowFailure) -> Self {
        Self::WorkflowFailed {
            error: decoded.message.clone(),
            error_type: decoded.error_type.clone(),
            details: decoded.details.clone(),
            non_retryable: decoded.non_retryable,
        }
    }

    /// Construct an untyped [`WorkflowEvent::ChildWorkflowFailed`] (issue #767):
    /// the typed fields default to `None`, preserving pre-#767 legacy semantics.
    #[must_use]
    pub fn child_workflow_failed(child_id: ExecutionId, error: impl Into<String>) -> Self {
        Self::ChildWorkflowFailed {
            child_id,
            error: error.into(),
            error_type: None,
            details: None,
            non_retryable: None,
        }
    }

    /// Construct a typed [`WorkflowEvent::ChildWorkflowFailed`] from a decoded
    /// [`WorkflowFailure`](crate::failure::WorkflowFailure) payload (issue #767).
    #[must_use]
    pub fn child_workflow_failed_typed(
        child_id: ExecutionId,
        decoded: &crate::failure::DecodedWorkflowFailure,
    ) -> Self {
        Self::ChildWorkflowFailed {
            child_id,
            error: decoded.message.clone(),
            error_type: decoded.error_type.clone(),
            details: decoded.details.clone(),
            non_retryable: decoded.non_retryable,
        }
    }

    /// Stable string identifier for this event variant, stored in
    /// `harvest_events.event_type`.
    #[must_use]
    pub const fn type_name(&self) -> &'static str {
        match self {
            Self::WorkflowStarted { .. } => "WorkflowStarted",
            Self::WorkflowCompleted { .. } => "WorkflowCompleted",
            Self::WorkflowFailed { .. } => "WorkflowFailed",
            Self::WorkflowCancelled { .. } => "WorkflowCancelled",
            Self::ActivityScheduled { .. } => "ActivityScheduled",
            Self::ActivityStarted { .. } => "ActivityStarted",
            Self::ActivityCompleted { .. } => "ActivityCompleted",
            Self::ActivityFailed { .. } => "ActivityFailed",
            Self::ActivityTimedOut { .. } => "ActivityTimedOut",
            Self::ActivityHeartbeat { .. } => "ActivityHeartbeat",
            Self::TimerStarted { .. } => "TimerStarted",
            Self::TimerFired { .. } => "TimerFired",
            Self::SignalReceived { .. } => "SignalReceived",
            Self::ChildWorkflowStarted { .. } => "ChildWorkflowStarted",
            Self::ChildWorkflowCompleted { .. } => "ChildWorkflowCompleted",
            Self::ChildWorkflowFailed { .. } => "ChildWorkflowFailed",
            Self::MarkerRecorded { .. } => "MarkerRecorded",
            Self::WorkflowContinuedAsNew { .. } => "WorkflowContinuedAsNew",
            Self::LocalActivityScheduled { .. } => "LocalActivityScheduled",
            Self::LocalActivityCompleted { .. } => "LocalActivityCompleted",
            Self::LocalActivityFailed { .. } => "LocalActivityFailed",
            Self::ActivityAwaitingExternal { .. } => "ActivityAwaitingExternal",
            Self::ActivityCompletedExternally { .. } => "ActivityCompletedExternally",
            Self::ActivityFailedExternally { .. } => "ActivityFailedExternally",
            Self::ActivityExternalDeadlineExtended { .. } => "ActivityExternalDeadlineExtended",
            Self::UpdateAdmitted { .. } => "UpdateAdmitted",
            Self::UpdateCompleted { .. } => "UpdateCompleted",
            Self::UpdateFailed { .. } => "UpdateFailed",
            Self::WorkflowResetFork { .. } => "WorkflowResetFork",
            Self::WorkflowResetTerminated { .. } => "WorkflowResetTerminated",
            Self::LocalActivityExhausted { .. } => "LocalActivityExhausted",
            Self::ExternalSignalRequested { .. } => "ExternalSignalRequested",
            Self::ExternalSignalDelivered { .. } => "ExternalSignalDelivered",
            Self::ExternalSignalFailed { .. } => "ExternalSignalFailed",
            Self::WorkflowExecutionTimedOut { .. } => "WorkflowExecutionTimedOut",
            Self::ChildWorkflowSpawnedDetached { .. } => "ChildWorkflowSpawnedDetached",
            Self::ChildWorkflowCascadeApplied { .. } => "ChildWorkflowCascadeApplied",
            Self::SideEffectRecorded { .. } => "SideEffectRecorded",
            Self::WorkflowExecutionPaused { .. } => "WorkflowExecutionPaused",
            Self::WorkflowExecutionResumed { .. } => "WorkflowExecutionResumed",
            Self::ExternalCancelRequested { .. } => "ExternalCancelRequested",
            Self::ExternalCancelDelivered { .. } => "ExternalCancelDelivered",
            Self::ExternalCancelFailed { .. } => "ExternalCancelFailed",
            Self::WorkflowRedriven { .. } => "WorkflowRedriven",
            Self::WorkflowRetryScheduled { .. } => "WorkflowRetryScheduled",
            Self::TimerCancelled { .. } => "TimerCancelled",
            Self::ExternalAwaitRequested { .. } => "ExternalAwaitRequested",
            Self::ExternalAwaitResolved { .. } => "ExternalAwaitResolved",
            Self::ExternalAwaitFailed { .. } => "ExternalAwaitFailed",
            Self::MutexGranted { .. } => "MutexGranted",
        }
    }

    /// Returns `true` for terminal lifecycle events that are appended by the
    /// executor after a workflow finishes and are never consumed by workflow
    /// commands during replay.
    #[must_use]
    pub const fn is_terminal_lifecycle(&self) -> bool {
        matches!(
            self,
            Self::WorkflowCompleted { .. }
                | Self::WorkflowFailed { .. }
                | Self::WorkflowCancelled { .. }
                | Self::WorkflowContinuedAsNew { .. }
                | Self::WorkflowResetTerminated { .. }
                | Self::WorkflowExecutionTimedOut { .. }
                // Written to the parent history after the parent's own terminal event;
                // never consumed by the workflow function itself, so must be skipped
                // during unconsumed-event checks to avoid false non-determinism reports.
                | Self::ChildWorkflowCascadeApplied { .. }
                // Appended to the failed run's history after WorkflowFailed as a
                // durable linkage record; the failed run is sealed and the event is
                // never consumed by its workflow function on replay.
                | Self::WorkflowRetryScheduled { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ActivityExecId;
    use chrono::Utc;

    // ── WorkflowFailed / ChildWorkflowFailed typed-failure tests (issue #767) ─

    #[test]
    fn workflow_failed_pre_767_json_deserializes_with_none() {
        let old_json = r#"{"type":"WorkflowFailed","data":{"error":"boom"}}"#;
        let back: WorkflowEvent = serde_json::from_str(old_json).unwrap();
        match back {
            WorkflowEvent::WorkflowFailed {
                error,
                error_type,
                details,
                non_retryable,
            } => {
                assert_eq!(error, "boom");
                assert!(error_type.is_none());
                assert!(details.is_none());
                assert!(non_retryable.is_none());
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn child_workflow_failed_pre_767_json_deserializes_with_none() {
        let old_json = r#"{"type":"ChildWorkflowFailed","data":{"child_id":"00000000-0000-0000-0000-000000000001","error":"boom"}}"#;
        let back: WorkflowEvent = serde_json::from_str(old_json).unwrap();
        match back {
            WorkflowEvent::ChildWorkflowFailed {
                error,
                error_type,
                details,
                non_retryable,
                ..
            } => {
                assert_eq!(error, "boom");
                assert!(error_type.is_none());
                assert!(details.is_none());
                assert!(non_retryable.is_none());
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn workflow_failed_typed_round_trips() {
        use crate::failure::IntoWorkflowErrorString;
        let decoded = crate::failure::decode_workflow_failure(
            &crate::failure::WorkflowFailure::new("ValidationRejected", "bad")
                .with_details(serde_json::json!({"field": "x"}))
                .non_retryable()
                .into_workflow_error_payload(),
        );
        let event = WorkflowEvent::workflow_failed_typed(&decoded);
        let json = serde_json::to_string(&event).unwrap();
        let back: WorkflowEvent = serde_json::from_str(&json).unwrap();
        match back {
            WorkflowEvent::WorkflowFailed {
                error,
                error_type,
                details,
                non_retryable,
            } => {
                assert_eq!(error, "bad");
                assert_eq!(error_type, Some("ValidationRejected".to_string()));
                assert_eq!(details, Some(serde_json::json!({"field": "x"})));
                assert_eq!(non_retryable, Some(true));
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn workflow_failed_none_fields_omitted_from_json() {
        let event = WorkflowEvent::workflow_failed("x");
        let json = serde_json::to_string(&event).unwrap();
        assert!(!json.contains("error_type"));
        assert!(!json.contains("details"));
        assert!(!json.contains("non_retryable"));
    }

    // ── ActivityFailed typed-failure tests (issue #227) ──────────────────────

    #[test]
    fn activity_failed_has_error_type_and_non_retryable_fields() {
        let id = ActivityExecId::new();
        let event = WorkflowEvent::ActivityFailed {
            activity_id: id,
            error: "InvalidInput: bad value".into(),
            attempt: 1,
            error_type: "InvalidInput".into(),
            non_retryable: true,
            details: None,
        };
        let json = serde_json::to_string(&event).unwrap();
        let back: WorkflowEvent = serde_json::from_str(&json).unwrap();
        match back {
            WorkflowEvent::ActivityFailed {
                error_type,
                non_retryable,
                attempt,
                ..
            } => {
                assert_eq!(error_type, "InvalidInput");
                assert!(non_retryable);
                assert_eq!(attempt, 1);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn activity_failed_old_format_deserializes_with_defaults() {
        // Old events stored without error_type / non_retryable must deserialize
        // cleanly via serde(default).
        let old_json = r#"{"type":"ActivityFailed","data":{"activity_id":"00000000-0000-0000-0000-000000000001","error":"connection refused","attempt":2}}"#;
        let back: WorkflowEvent = serde_json::from_str(old_json).unwrap();
        match back {
            WorkflowEvent::ActivityFailed {
                error,
                attempt,
                error_type,
                non_retryable,
                ..
            } => {
                assert_eq!(error, "connection refused");
                assert_eq!(attempt, 2);
                assert_eq!(error_type, "Error", "default error_type must be 'Error'");
                assert!(!non_retryable, "default non_retryable must be false");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn activity_failed_retryable_round_trips() {
        let id = ActivityExecId::new();
        let event = WorkflowEvent::ActivityFailed {
            activity_id: id,
            error: "Transient: timeout".into(),
            attempt: 1,
            error_type: "Transient".into(),
            non_retryable: false,
            details: None,
        };
        let json = serde_json::to_string(&event).unwrap();
        let back: WorkflowEvent = serde_json::from_str(&json).unwrap();
        assert!(matches!(
            back,
            WorkflowEvent::ActivityFailed {
                non_retryable: false,
                ..
            }
        ));
    }

    #[test]
    fn workflow_started_round_trips_serde() -> Result<(), serde_json::Error> {
        let event = WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({"user_id": 42}),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        };
        let json = serde_json::to_string(&event)?;
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        assert!(matches!(back, WorkflowEvent::WorkflowStarted { .. }));
        Ok(())
    }

    // ── issue #508: scheduled_time field ────────────────────────────────────────

    /// Legacy JSON without `scheduled_time` deserializes to `None` (backward compat).
    #[test]
    fn workflow_started_legacy_json_scheduled_time_defaults_to_none() {
        let legacy_json =
            r#"{"type":"WorkflowStarted","data":{"input":{},"timestamp":"2026-01-01T00:00:00Z"}}"#;
        let event: WorkflowEvent = serde_json::from_str(legacy_json).unwrap();
        match event {
            WorkflowEvent::WorkflowStarted { scheduled_time, .. } => {
                assert!(
                    scheduled_time.is_none(),
                    "legacy JSON must deserialize to None"
                );
            }
            _ => panic!("wrong variant"),
        }
    }

    /// `scheduled_time: Some(t)` round-trips correctly through serde.
    #[test]
    fn workflow_started_scheduled_time_round_trips() {
        use chrono::TimeZone as _;
        let slot = Utc.with_ymd_and_hms(2026, 3, 15, 0, 0, 0).unwrap();
        let event = WorkflowEvent::WorkflowStarted {
            input: serde_json::json!(null),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: Some(slot),
        };
        let json = serde_json::to_string(&event).unwrap();
        let back: WorkflowEvent = serde_json::from_str(&json).unwrap();
        match back {
            WorkflowEvent::WorkflowStarted { scheduled_time, .. } => {
                assert_eq!(scheduled_time, Some(slot));
            }
            _ => panic!("wrong variant"),
        }
    }

    /// `scheduled_time: None` is omitted from JSON (no key emitted).
    #[test]
    fn workflow_started_scheduled_time_none_omitted_from_json() {
        let event = WorkflowEvent::WorkflowStarted {
            input: serde_json::json!(null),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(
            !json.contains("scheduled_time"),
            "None should be omitted from JSON, got: {json}"
        );
    }

    #[test]
    fn activity_scheduled_round_trips() -> Result<(), serde_json::Error> {
        let event = WorkflowEvent::ActivityScheduled {
            activity_id: ActivityExecId::new(),
            name: "send_email".into(),
            input: serde_json::Value::Null,
            queue: "default".into(),
        };
        let json = serde_json::to_string(&event)?;
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        assert!(matches!(back, WorkflowEvent::ActivityScheduled { .. }));
        Ok(())
    }

    #[test]
    fn mutex_granted_round_trips() -> Result<(), serde_json::Error> {
        // Issue #691: the durable-mutex replay anchor round-trips verbatim and
        // carries the adjacently-tagged `"type":"MutexGranted"` discriminator.
        let event = WorkflowEvent::MutexGranted {
            key: "ledger:acct-42".into(),
            lock_seq: 7,
            acquired_at: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
        };
        let value = serde_json::to_value(&event)?;
        assert_eq!(value["type"], "MutexGranted");
        assert_eq!(value["data"]["key"], "ledger:acct-42");
        assert_eq!(value["data"]["lock_seq"], 7);
        let back: WorkflowEvent = serde_json::from_value(value)?;
        let WorkflowEvent::MutexGranted {
            key,
            lock_seq,
            acquired_at,
        } = back
        else {
            panic!("expected MutexGranted, got {}", event.type_name());
        };
        assert_eq!(key, "ledger:acct-42");
        assert_eq!(lock_seq, 7);
        assert_eq!(
            acquired_at,
            DateTime::from_timestamp(1_700_000_000, 0).unwrap()
        );
        Ok(())
    }

    #[test]
    fn local_activity_scheduled_round_trips() -> Result<(), serde_json::Error> {
        // Issue #620 (Codex P2): a #620+ event carrying the resolution marker
        // plus frozen retry/STC must round-trip all three fields verbatim.
        let event = WorkflowEvent::LocalActivityScheduled {
            activity_id: ActivityExecId::new(),
            name: "format_data".into(),
            input: serde_json::json!({"x": 1}),
            resolved: true,
            retry_policy: Some(crate::policy::RetryPolicy::fixed(
                4,
                std::time::Duration::from_millis(10),
            )),
            // A sub-millisecond value (500µs = 500_000 ns): a millis field would
            // have zeroed it — the full-nanos field round-trips it exactly.
            start_to_close_nanos: Some(500_000),
        };
        let json = serde_json::to_string(&event)?;
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        match back {
            WorkflowEvent::LocalActivityScheduled {
                resolved,
                retry_policy,
                start_to_close_nanos,
                ..
            } => {
                assert!(resolved, "resolution marker must round-trip as true");
                assert_eq!(
                    retry_policy.map(|p| p.max_attempts),
                    Some(4),
                    "frozen retry policy must round-trip"
                );
                assert_eq!(
                    start_to_close_nanos,
                    Some(500_000),
                    "frozen sub-millisecond STC nanos must round-trip exactly"
                );
            }
            other => panic!("expected LocalActivityScheduled, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn pre_620_local_activity_scheduled_deserializes_as_unresolved() -> Result<(), serde_json::Error>
    {
        // A pre-#620 stored event has none of the three additive fields. It must
        // deserialize with `resolved = false` (legacy → re-derive on recovery)
        // and both frozen fields `None`, preserving the append-only invariant.
        let legacy = r#"{"type":"LocalActivityScheduled","data":{"activity_id":"00000000-0000-0000-0000-000000000001","name":"format_data","input":{"x":1}}}"#;
        let back: WorkflowEvent = serde_json::from_str(legacy)?;
        match back {
            WorkflowEvent::LocalActivityScheduled {
                resolved,
                retry_policy,
                start_to_close_nanos,
                ..
            } => {
                assert!(!resolved, "a legacy event must deserialize as unresolved");
                assert!(retry_policy.is_none());
                assert!(start_to_close_nanos.is_none());
            }
            other => panic!("expected LocalActivityScheduled, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn local_activity_completed_round_trips() -> Result<(), serde_json::Error> {
        let event = WorkflowEvent::LocalActivityCompleted {
            activity_id: ActivityExecId::new(),
            output: serde_json::json!({"result": 42}),
        };
        let json = serde_json::to_string(&event)?;
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        assert!(matches!(back, WorkflowEvent::LocalActivityCompleted { .. }));
        Ok(())
    }

    #[test]
    fn local_activity_failed_round_trips() -> Result<(), serde_json::Error> {
        let event = WorkflowEvent::LocalActivityFailed {
            activity_id: ActivityExecId::new(),
            error: "db connection refused".into(),
            attempt: 3,
        };
        let json = serde_json::to_string(&event)?;
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        assert!(matches!(
            back,
            WorkflowEvent::LocalActivityFailed { attempt: 3, .. }
        ));
        Ok(())
    }

    #[test]
    fn local_activity_exhausted_round_trips() -> Result<(), serde_json::Error> {
        let id = ActivityExecId::new();
        let event = WorkflowEvent::LocalActivityExhausted {
            activity_id: id,
            error: "always fails".into(),
            attempt: 3,
        };
        let json = serde_json::to_string(&event)?;
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        assert!(matches!(
            back,
            WorkflowEvent::LocalActivityExhausted { attempt: 3, .. }
        ));
        assert_eq!(event.type_name(), "LocalActivityExhausted");
        Ok(())
    }

    #[test]
    fn event_type_name_is_stable() {
        let e = WorkflowEvent::WorkflowCompleted {
            output: serde_json::Value::Null,
        };
        assert_eq!(e.type_name(), "WorkflowCompleted");
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn all_type_names_are_unique() {
        use crate::types::{ActivityExecId, ExecutionId, TimerId, WorkerId};
        use std::collections::HashSet;

        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: serde_json::Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::WorkflowCompleted {
                output: serde_json::Value::Null,
            },
            WorkflowEvent::workflow_failed("x"),
            WorkflowEvent::WorkflowCancelled { reason: "x".into() },
            WorkflowEvent::ActivityScheduled {
                activity_id: ActivityExecId::new(),
                name: "a".into(),
                input: serde_json::Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityStarted {
                activity_id: ActivityExecId::new(),
                worker_id: WorkerId::new("w"),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: ActivityExecId::new(),
                output: serde_json::Value::Null,
            },
            WorkflowEvent::ActivityFailed {
                activity_id: ActivityExecId::new(),
                error: "x".into(),
                attempt: 1,
                error_type: "Error".into(),
                non_retryable: false,
                details: None,
            },
            WorkflowEvent::ActivityTimedOut {
                activity_id: ActivityExecId::new(),
                timeout_type: crate::error::TimeoutType::StartToClose,
            },
            WorkflowEvent::ActivityHeartbeat {
                activity_id: ActivityExecId::new(),
                details: serde_json::Value::Null,
            },
            WorkflowEvent::TimerStarted {
                timer_id: TimerId::new("t"),
                duration_secs: 10,
            },
            WorkflowEvent::TimerFired {
                timer_id: TimerId::new("t"),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "s".into(),
                payload: serde_json::Value::Null,
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id: ExecutionId::new(),
                workflow_name: "w".into(),
                input: serde_json::Value::Null,
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id: ExecutionId::new(),
                output: serde_json::Value::Null,
            },
            WorkflowEvent::child_workflow_failed(ExecutionId::new(), "x"),
            WorkflowEvent::MarkerRecorded {
                name: "m".into(),
                details: serde_json::Value::Null,
            },
            WorkflowEvent::WorkflowContinuedAsNew {
                new_exec_id: ExecutionId::new(),
                input: serde_json::Value::Null,
                new_workflow_type: None,
            },
            WorkflowEvent::ActivityAwaitingExternal {
                activity_id: ActivityExecId::new(),
                token: crate::types::ExternalActivityToken::new(),
                name: "x".into(),
                input: serde_json::Value::Null,
                queue: "default".into(),
                schedule_to_close_secs: 0,
            },
            WorkflowEvent::ActivityCompletedExternally {
                activity_id: ActivityExecId::new(),
                token: crate::types::ExternalActivityToken::new(),
                output: serde_json::Value::Null,
            },
            WorkflowEvent::ActivityFailedExternally {
                activity_id: ActivityExecId::new(),
                token: crate::types::ExternalActivityToken::new(),
                error: "x".into(),
                retryable: false,
            },
            WorkflowEvent::ActivityExternalDeadlineExtended {
                activity_id: ActivityExecId::new(),
                token: crate::types::ExternalActivityToken::new(),
            },
            WorkflowEvent::LocalActivityScheduled {
                activity_id: ActivityExecId::new(),
                name: "format_data".into(),
                input: serde_json::Value::Null,
                resolved: false,
                retry_policy: None,
                start_to_close_nanos: None,
            },
            WorkflowEvent::LocalActivityCompleted {
                activity_id: ActivityExecId::new(),
                output: serde_json::Value::Null,
            },
            WorkflowEvent::LocalActivityFailed {
                activity_id: ActivityExecId::new(),
                error: "transient".into(),
                attempt: 1,
            },
            WorkflowEvent::LocalActivityExhausted {
                activity_id: ActivityExecId::new(),
                error: "permanent".into(),
                attempt: 3,
            },
            WorkflowEvent::UpdateAdmitted {
                update_id: crate::types::UpdateId::new(),
                name: "approve".into(),
                input: serde_json::Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::UpdateCompleted {
                update_id: crate::types::UpdateId::new(),
                output: serde_json::Value::Null,
            },
            WorkflowEvent::UpdateFailed {
                update_id: crate::types::UpdateId::new(),
                error: "x".into(),
            },
            WorkflowEvent::WorkflowResetFork {
                reset_from_exec_id: ExecutionId::new(),
                reset_to_event_id: 1,
                reason: "bad deploy".into(),
                operator_id: "ops".into(),
            },
            WorkflowEvent::WorkflowResetTerminated {
                reset_to_exec_id: ExecutionId::new(),
                reason: "bad deploy".into(),
                operator_id: "ops".into(),
            },
            WorkflowEvent::ExternalSignalRequested {
                signal_id: crate::types::ExternalSignalId::new(),
                target: crate::types::ExternalTarget::ExecutionId(ExecutionId::new()),
                signal_name: "cancel".into(),
                payload: serde_json::Value::Null,
                idempotency_key: None,
            },
            WorkflowEvent::ExternalSignalDelivered {
                signal_id: crate::types::ExternalSignalId::new(),
            },
            WorkflowEvent::ExternalSignalFailed {
                signal_id: crate::types::ExternalSignalId::new(),
                reason_code: "target_terminal".into(),
            },
            WorkflowEvent::WorkflowExecutionTimedOut {
                deadline: Utc::now(),
                timed_out_at: Utc::now(),
            },
            WorkflowEvent::SideEffectRecorded {
                kind: crate::event::SideEffectKind::Now,
                name: None,
                value: serde_json::Value::Null,
            },
            WorkflowEvent::WorkflowExecutionPaused {
                paused_at: Utc::now(),
                reason: Some("incident".into()),
                actor: "oncall".into(),
            },
            WorkflowEvent::WorkflowExecutionResumed {
                resumed_at: Utc::now(),
                actor: "oncall".into(),
            },
            WorkflowEvent::ChildWorkflowSpawnedDetached {
                child_id: ExecutionId::new(),
                workflow_name: "child_wf".into(),
                input: serde_json::Value::Null,
                parent_close_policy: crate::types::ParentClosePolicy::Abandon,
            },
            WorkflowEvent::ChildWorkflowCascadeApplied {
                child_id: ExecutionId::new(),
                policy: crate::types::ParentClosePolicy::RequestCancel,
                action: "request_cancel".into(),
            },
            WorkflowEvent::ExternalCancelRequested {
                cancel_id: crate::types::ExternalCancelId::new(),
                target: crate::types::ExternalTarget::ExecutionId(ExecutionId::new()),
            },
            WorkflowEvent::ExternalCancelDelivered {
                cancel_id: crate::types::ExternalCancelId::new(),
            },
            WorkflowEvent::ExternalCancelFailed {
                cancel_id: crate::types::ExternalCancelId::new(),
                reason_code: "target_unknown".into(),
            },
            WorkflowEvent::WorkflowRedriven {
                redriven_at: Utc::now(),
                dead_letter_id: Uuid::new_v4(),
                reason: None,
            },
            WorkflowEvent::WorkflowRetryScheduled {
                retry_exec_id: ExecutionId::new(),
                attempt: 2,
                fire_at: Utc::now(),
            },
            WorkflowEvent::TimerCancelled {
                timer_id: TimerId::new("t"),
            },
            WorkflowEvent::ExternalAwaitRequested {
                await_id: crate::types::ExternalAwaitId::new(),
                target: ExecutionId::new(),
            },
            WorkflowEvent::ExternalAwaitResolved {
                await_id: crate::types::ExternalAwaitId::new(),
                output: serde_json::Value::Null,
            },
            WorkflowEvent::ExternalAwaitFailed {
                await_id: crate::types::ExternalAwaitId::new(),
                reason_code: "target_failed".into(),
                message: None,
                error_type: None,
                details: None,
                non_retryable: None,
            },
        ];

        assert_eq!(events.len(), 49);
        let names: HashSet<_> = events.iter().map(WorkflowEvent::type_name).collect();
        assert_eq!(names.len(), 49, "duplicate type names detected");
    }

    // ── TimerCancelled tests (issue #768) ─────────────────────────────────────

    #[test]
    fn timer_cancelled_round_trips() -> Result<(), serde_json::Error> {
        let event = WorkflowEvent::TimerCancelled {
            timer_id: TimerId::new("idle-timeout"),
        };
        assert_eq!(event.type_name(), "TimerCancelled");
        let json = serde_json::to_string(&event)?;
        // Adjacently-tagged serde contract.
        assert!(
            json.contains("\"type\":\"TimerCancelled\""),
            "type tag must be present: {json}"
        );
        assert!(
            json.contains("\"timer_id\":\"idle-timeout\""),
            "timer_id must be nested under data: {json}"
        );
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        match back {
            WorkflowEvent::TimerCancelled { timer_id } => {
                assert_eq!(timer_id, TimerId::new("idle-timeout"));
            }
            other => panic!("wrong variant: {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn pre_cancellation_history_deserializes_unchanged() -> Result<(), serde_json::Error> {
        // A history captured before issue #768 — a TimerStarted followed by a
        // TimerFired — must still deserialize into exactly the same variants.
        let json = r#"[
            {"type":"TimerStarted","data":{"timer_id":"t","duration_secs":300}},
            {"type":"TimerFired","data":{"timer_id":"t"}}
        ]"#;
        let events: Vec<WorkflowEvent> = serde_json::from_str(json)?;
        assert_eq!(events.len(), 2);
        assert!(matches!(events[0], WorkflowEvent::TimerStarted { .. }));
        assert!(matches!(events[1], WorkflowEvent::TimerFired { .. }));
        Ok(())
    }

    // ── SideEffectRecorded tests (issue #384) ─────────────────────────────────

    #[test]
    fn side_effect_recorded_round_trips_without_name() -> Result<(), serde_json::Error> {
        let event = WorkflowEvent::SideEffectRecorded {
            kind: SideEffectKind::Now,
            name: None,
            value: serde_json::json!(1_700_000_000_u64),
        };
        assert_eq!(event.type_name(), "SideEffectRecorded");
        let json = serde_json::to_string(&event)?;
        // `name: None` must be omitted from the serialised form.
        assert!(
            !json.contains("\"name\""),
            "name should be skipped when None"
        );
        assert!(
            json.contains("\"kind\":\"Now\""),
            "kind tag must be present"
        );
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        match back {
            WorkflowEvent::SideEffectRecorded { kind, name, value } => {
                assert_eq!(kind, SideEffectKind::Now);
                assert_eq!(name, None);
                assert_eq!(value, serde_json::json!(1_700_000_000_u64));
            }
            _ => panic!("wrong variant"),
        }
        Ok(())
    }

    #[test]
    fn side_effect_recorded_round_trips_with_custom_name() -> Result<(), serde_json::Error> {
        let event = WorkflowEvent::SideEffectRecorded {
            kind: SideEffectKind::Custom,
            name: Some("env_lookup".into()),
            value: serde_json::json!({"region": "us-east-1"}),
        };
        let json = serde_json::to_string(&event)?;
        assert!(json.contains("\"name\":\"env_lookup\""));
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        assert!(matches!(
            back,
            WorkflowEvent::SideEffectRecorded {
                kind: SideEffectKind::Custom,
                ..
            }
        ));
        Ok(())
    }

    #[test]
    fn side_effect_recorded_is_not_terminal_lifecycle() {
        let event = WorkflowEvent::SideEffectRecorded {
            kind: SideEffectKind::Uuid,
            name: None,
            value: serde_json::Value::Null,
        };
        assert!(!event.is_terminal_lifecycle());
    }

    #[test]
    fn side_effect_kind_labels_are_bounded() {
        assert_eq!(SideEffectKind::Now.as_str(), "now");
        assert_eq!(SideEffectKind::Uuid.as_str(), "uuid");
        assert_eq!(SideEffectKind::Random.as_str(), "random");
        assert_eq!(SideEffectKind::Custom.as_str(), "custom");
    }

    #[test]
    fn workflow_reset_events_round_trip_and_type_names_are_stable() -> Result<(), serde_json::Error>
    {
        let source = ExecutionId::new();
        let fork = ExecutionId::new();
        let fork_event = WorkflowEvent::WorkflowResetFork {
            reset_from_exec_id: source,
            reset_to_event_id: 42,
            reason: "rolled back bad signal".into(),
            operator_id: "oncall".into(),
        };
        let terminated_event = WorkflowEvent::WorkflowResetTerminated {
            reset_to_exec_id: fork,
            reason: "rolled back bad signal".into(),
            operator_id: "oncall".into(),
        };

        assert_eq!(fork_event.type_name(), "WorkflowResetFork");
        assert_eq!(terminated_event.type_name(), "WorkflowResetTerminated");

        let json = serde_json::to_string(&fork_event)?;
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        assert!(matches!(
            back,
            WorkflowEvent::WorkflowResetFork {
                reset_from_exec_id,
                reset_to_event_id: 42,
                ..
            } if reset_from_exec_id == source
        ));

        Ok(())
    }

    // ── ExternalSignal event tests (issue #330) ───────────────────────────

    #[test]
    fn external_signal_requested_round_trips() -> Result<(), serde_json::Error> {
        let signal_id = crate::types::ExternalSignalId::new();
        let target = crate::types::ExternalTarget::ExecutionId(ExecutionId::new());
        let event = WorkflowEvent::ExternalSignalRequested {
            signal_id,
            target: target.clone(),
            signal_name: "tenant_cancel".into(),
            payload: serde_json::json!({"reason": "billing_lapse"}),
            idempotency_key: Some("evt_123".into()),
        };
        assert_eq!(event.type_name(), "ExternalSignalRequested");
        let json = serde_json::to_string(&event)?;
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        match back {
            WorkflowEvent::ExternalSignalRequested {
                signal_id: sid,
                target: t,
                signal_name,
                payload,
                idempotency_key,
            } => {
                assert_eq!(sid, signal_id);
                assert_eq!(t, target);
                assert_eq!(signal_name, "tenant_cancel");
                assert_eq!(payload["reason"], "billing_lapse");
                assert_eq!(idempotency_key.as_deref(), Some("evt_123"));
            }
            _ => panic!("wrong variant"),
        }
        Ok(())
    }

    #[test]
    fn external_signal_requested_pre_521_json_deserializes_without_key()
    -> Result<(), serde_json::Error> {
        // An older event has no `idempotency_key` field; the additive
        // `#[serde(default)]` must deserialize it to `None` (append-only
        // invariant: no new variant, old JSON still loads).
        let signal_id = crate::types::ExternalSignalId::new();
        let target = ExecutionId::new();
        let legacy = serde_json::json!({
            "type": "ExternalSignalRequested",
            "data": {
                "signal_id": signal_id,
                "target": target,
                "signal_name": "tenant_cancel",
                "payload": {"reason": "billing_lapse"}
            }
        });
        let back: WorkflowEvent = serde_json::from_value(legacy)?;
        match back {
            WorkflowEvent::ExternalSignalRequested {
                idempotency_key, ..
            } => assert_eq!(idempotency_key, None),
            _ => panic!("wrong variant"),
        }
        Ok(())
    }

    #[test]
    fn external_signal_delivered_round_trips() -> Result<(), serde_json::Error> {
        let signal_id = crate::types::ExternalSignalId::new();
        let event = WorkflowEvent::ExternalSignalDelivered { signal_id };
        assert_eq!(event.type_name(), "ExternalSignalDelivered");
        let json = serde_json::to_string(&event)?;
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        assert!(matches!(
            back,
            WorkflowEvent::ExternalSignalDelivered { signal_id: sid } if sid == signal_id
        ));
        Ok(())
    }

    #[test]
    fn external_signal_failed_round_trips() -> Result<(), serde_json::Error> {
        let signal_id = crate::types::ExternalSignalId::new();
        let event = WorkflowEvent::ExternalSignalFailed {
            signal_id,
            reason_code: "target_terminal".into(),
        };
        assert_eq!(event.type_name(), "ExternalSignalFailed");
        let json = serde_json::to_string(&event)?;
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        match back {
            WorkflowEvent::ExternalSignalFailed {
                signal_id: sid,
                reason_code,
            } => {
                assert_eq!(sid, signal_id);
                assert_eq!(reason_code, "target_terminal");
            }
            _ => panic!("wrong variant"),
        }
        Ok(())
    }

    #[test]
    fn external_signal_failed_unknown_target_reason_code() -> Result<(), serde_json::Error> {
        let signal_id = crate::types::ExternalSignalId::new();
        let event = WorkflowEvent::ExternalSignalFailed {
            signal_id,
            reason_code: "target_unknown".into(),
        };
        let json = serde_json::to_string(&event)?;
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        match back {
            WorkflowEvent::ExternalSignalFailed { reason_code, .. } => {
                assert_eq!(reason_code, "target_unknown");
            }
            _ => panic!("wrong variant"),
        }
        Ok(())
    }

    #[test]
    fn external_signal_events_are_not_terminal_lifecycle() {
        let signal_id = crate::types::ExternalSignalId::new();
        let target = crate::types::ExternalTarget::ExecutionId(ExecutionId::new());
        assert!(
            !WorkflowEvent::ExternalSignalRequested {
                signal_id,
                target,
                signal_name: "x".into(),
                payload: serde_json::Value::Null,
                idempotency_key: None,
            }
            .is_terminal_lifecycle()
        );
        assert!(!WorkflowEvent::ExternalSignalDelivered { signal_id }.is_terminal_lifecycle());
        assert!(
            !WorkflowEvent::ExternalSignalFailed {
                signal_id,
                reason_code: "target_terminal".into(),
            }
            .is_terminal_lifecycle()
        );
    }

    // ── WorkflowExecutionTimedOut tests (issue #243) ──────────────────────────

    #[test]
    fn workflow_execution_timed_out_round_trips() -> Result<(), serde_json::Error> {
        let deadline = Utc::now();
        let timed_out_at = Utc::now();
        let event = WorkflowEvent::WorkflowExecutionTimedOut {
            deadline,
            timed_out_at,
        };

        assert_eq!(event.type_name(), "WorkflowExecutionTimedOut");
        assert!(event.is_terminal_lifecycle(), "timed-out must be terminal");

        let json = serde_json::to_string(&event)?;
        assert!(
            json.contains("WorkflowExecutionTimedOut"),
            "type tag must appear in JSON"
        );
        assert!(
            json.contains("deadline"),
            "deadline field must be serialised"
        );
        assert!(
            json.contains("timed_out_at"),
            "timed_out_at field must be serialised"
        );

        let back: WorkflowEvent = serde_json::from_str(&json)?;
        assert!(
            matches!(back, WorkflowEvent::WorkflowExecutionTimedOut { .. }),
            "must deserialise to the correct variant"
        );
        Ok(())
    }

    #[test]
    fn workflow_execution_timed_out_type_name_is_stable() {
        let e = WorkflowEvent::WorkflowExecutionTimedOut {
            deadline: Utc::now(),
            timed_out_at: Utc::now(),
        };
        assert_eq!(e.type_name(), "WorkflowExecutionTimedOut");
    }

    // ── is_terminal_lifecycle exhaustive coverage (issue #612 seal check) ─────

    #[test]
    fn every_terminal_seal_variant_is_terminal_lifecycle() {
        let exec = ExecutionId::new();
        let terminal: Vec<WorkflowEvent> = vec![
            WorkflowEvent::WorkflowCompleted {
                output: serde_json::Value::Null,
            },
            WorkflowEvent::workflow_failed("boom"),
            WorkflowEvent::WorkflowCancelled {
                reason: "operator".into(),
            },
            WorkflowEvent::WorkflowContinuedAsNew {
                new_exec_id: exec,
                input: serde_json::Value::Null,
                new_workflow_type: None,
            },
            WorkflowEvent::WorkflowResetTerminated {
                reset_to_exec_id: exec,
                reason: "reset".into(),
                operator_id: "op".into(),
            },
            WorkflowEvent::WorkflowExecutionTimedOut {
                deadline: Utc::now(),
                timed_out_at: Utc::now(),
            },
            // Trailing bookkeeping events appended after the terminal seal.
            WorkflowEvent::ChildWorkflowCascadeApplied {
                child_id: exec,
                policy: crate::types::ParentClosePolicy::RequestCancel,
                action: "request_cancel".into(),
            },
            WorkflowEvent::WorkflowRetryScheduled {
                retry_exec_id: exec,
                attempt: 2,
                fire_at: Utc::now(),
            },
        ];
        for event in &terminal {
            assert!(
                event.is_terminal_lifecycle(),
                "{} must be a terminal lifecycle event",
                event.type_name()
            );
        }
    }

    #[test]
    fn non_terminal_events_are_not_terminal_lifecycle() {
        let id = ActivityExecId::new();
        let non_terminal: Vec<WorkflowEvent> = vec![
            WorkflowEvent::ActivityScheduled {
                activity_id: id,
                name: "a".into(),
                input: serde_json::Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: id,
                output: serde_json::Value::Null,
            },
            WorkflowEvent::TimerStarted {
                timer_id: crate::types::TimerId::new("t-1"),
                duration_secs: 60,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approved".into(),
                payload: serde_json::Value::Null,
            },
            WorkflowEvent::WorkflowStarted {
                input: serde_json::Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            // Redrive reopens a previously-failed run — must NOT count as a seal.
            WorkflowEvent::WorkflowRedriven {
                redriven_at: Utc::now(),
                dead_letter_id: Uuid::new_v4(),
                reason: None,
            },
        ];
        for event in &non_terminal {
            assert!(
                !event.is_terminal_lifecycle(),
                "{} must NOT be a terminal lifecycle event",
                event.type_name()
            );
        }
    }

    // ── Pause/Resume tests (issue #383) ───────────────────────────────────────

    #[test]
    fn workflow_execution_paused_round_trips() -> Result<(), serde_json::Error> {
        let paused_at = Utc::now();
        let event = WorkflowEvent::WorkflowExecutionPaused {
            paused_at,
            reason: Some("investigating runaway dispatch".into()),
            actor: "oncall@example.com".into(),
        };

        assert_eq!(event.type_name(), "WorkflowExecutionPaused");
        // Pause is NOT terminal — a paused workflow resumes and keeps running.
        assert!(
            !event.is_terminal_lifecycle(),
            "pause must not be a terminal lifecycle event"
        );

        let json = serde_json::to_string(&event)?;
        assert!(json.contains("WorkflowExecutionPaused"));
        assert!(json.contains("paused_at"));
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        match back {
            WorkflowEvent::WorkflowExecutionPaused { reason, actor, .. } => {
                assert_eq!(reason.as_deref(), Some("investigating runaway dispatch"));
                assert_eq!(actor, "oncall@example.com");
            }
            _ => panic!("wrong variant"),
        }
        Ok(())
    }

    #[test]
    fn workflow_execution_paused_round_trips_without_reason() -> Result<(), serde_json::Error> {
        let event = WorkflowEvent::WorkflowExecutionPaused {
            paused_at: Utc::now(),
            reason: None,
            actor: "auto".into(),
        };
        let json = serde_json::to_string(&event)?;
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        assert!(matches!(
            back,
            WorkflowEvent::WorkflowExecutionPaused { reason: None, .. }
        ));
        Ok(())
    }

    #[test]
    fn workflow_execution_resumed_round_trips() -> Result<(), serde_json::Error> {
        let resumed_at = Utc::now();
        let event = WorkflowEvent::WorkflowExecutionResumed {
            resumed_at,
            actor: "auto-resume(timeout)".into(),
        };

        assert_eq!(event.type_name(), "WorkflowExecutionResumed");
        assert!(
            !event.is_terminal_lifecycle(),
            "resume must not be a terminal lifecycle event"
        );

        let json = serde_json::to_string(&event)?;
        assert!(json.contains("WorkflowExecutionResumed"));
        assert!(json.contains("resumed_at"));
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        match back {
            WorkflowEvent::WorkflowExecutionResumed { actor, .. } => {
                assert_eq!(actor, "auto-resume(timeout)");
            }
            _ => panic!("wrong variant"),
        }
        Ok(())
    }

    // ── WorkflowRedriven (issue #510) ────────────────────────────────────────

    #[test]
    fn workflow_redriven_type_name_and_not_terminal() {
        let event = WorkflowEvent::WorkflowRedriven {
            redriven_at: Utc::now(),
            dead_letter_id: Uuid::new_v4(),
            reason: Some("downstream fixed".into()),
        };
        assert_eq!(event.type_name(), "WorkflowRedriven");
        assert!(
            !event.is_terminal_lifecycle(),
            "redrive reopens the run and must not be a terminal lifecycle event"
        );
    }

    #[test]
    fn workflow_redriven_round_trips_adjacently_tagged() -> Result<(), serde_json::Error> {
        let dead_letter_id = Uuid::new_v4();
        let event = WorkflowEvent::WorkflowRedriven {
            redriven_at: Utc::now(),
            dead_letter_id,
            reason: Some("credential rotated".into()),
        };
        let json = serde_json::to_string(&event)?;
        // Adjacently-tagged: {"type":"WorkflowRedriven","data":{...}}
        assert!(json.contains("\"type\":\"WorkflowRedriven\""));
        assert!(json.contains("\"data\""));
        assert!(json.contains("dead_letter_id"));
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        match back {
            WorkflowEvent::WorkflowRedriven {
                dead_letter_id: id,
                reason,
                ..
            } => {
                assert_eq!(id, dead_letter_id);
                assert_eq!(reason.as_deref(), Some("credential rotated"));
            }
            _ => panic!("wrong variant"),
        }
        Ok(())
    }

    #[test]
    fn workflow_redriven_omits_reason_when_none() -> Result<(), serde_json::Error> {
        let event = WorkflowEvent::WorkflowRedriven {
            redriven_at: Utc::now(),
            dead_letter_id: Uuid::new_v4(),
            reason: None,
        };
        let json = serde_json::to_string(&event)?;
        assert!(
            !json.contains("reason"),
            "reason must be skipped when None, got {json}"
        );
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        assert!(matches!(
            back,
            WorkflowEvent::WorkflowRedriven { reason: None, .. }
        ));
        Ok(())
    }

    #[test]
    fn workflow_started_constructor_defaults_additive_fields_to_none()
    -> Result<(), serde_json::Error> {
        let ts = Utc::now();
        let event = WorkflowEvent::workflow_started(serde_json::json!({"k": 1}), ts);
        match &event {
            WorkflowEvent::WorkflowStarted {
                input,
                timestamp,
                last_completion_result,
                last_error,
                scheduled_time,
            } => {
                assert_eq!(input, &serde_json::json!({"k": 1}));
                assert_eq!(timestamp, &ts);
                assert!(last_completion_result.is_none());
                assert!(last_error.is_none());
                assert!(scheduled_time.is_none());
            }
            _ => panic!("wrong variant"),
        }

        // The three additive optionals skip when None.
        let json = serde_json::to_string(&event)?;
        assert!(
            !json.contains("last_completion_result"),
            "last_completion_result must skip when None, got {json}"
        );
        assert!(
            !json.contains("last_error"),
            "last_error must skip when None, got {json}"
        );
        assert!(
            !json.contains("scheduled_time"),
            "scheduled_time must skip when None, got {json}"
        );

        // Round-trips back to an equivalent value.
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        match back {
            WorkflowEvent::WorkflowStarted {
                input,
                timestamp,
                last_completion_result,
                last_error,
                scheduled_time,
            } => {
                assert_eq!(input, serde_json::json!({"k": 1}));
                assert_eq!(timestamp, ts);
                assert!(last_completion_result.is_none());
                assert!(last_error.is_none());
                assert!(scheduled_time.is_none());
            }
            _ => panic!("wrong variant"),
        }
        Ok(())
    }

    // ── ExternalAwait event tests (issue #757) ────────────────────────────

    #[test]
    fn external_await_requested_round_trips() -> Result<(), serde_json::Error> {
        let await_id = crate::types::ExternalAwaitId::new();
        let target = ExecutionId::new();
        let event = WorkflowEvent::ExternalAwaitRequested { await_id, target };
        assert_eq!(event.type_name(), "ExternalAwaitRequested");
        let json = serde_json::to_string(&event)?;
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        match back {
            WorkflowEvent::ExternalAwaitRequested {
                await_id: aid,
                target: t,
            } => {
                assert_eq!(aid, await_id);
                assert_eq!(t, target);
            }
            _ => panic!("wrong variant"),
        }
        Ok(())
    }

    #[test]
    fn external_await_resolved_round_trips() -> Result<(), serde_json::Error> {
        let await_id = crate::types::ExternalAwaitId::new();
        let event = WorkflowEvent::ExternalAwaitResolved {
            await_id,
            output: serde_json::json!({"total": 42}),
        };
        assert_eq!(event.type_name(), "ExternalAwaitResolved");
        let json = serde_json::to_string(&event)?;
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        match back {
            WorkflowEvent::ExternalAwaitResolved {
                await_id: aid,
                output,
            } => {
                assert_eq!(aid, await_id);
                assert_eq!(output["total"], 42);
            }
            _ => panic!("wrong variant"),
        }
        Ok(())
    }

    #[test]
    fn external_await_failed_round_trips_with_typed_fields() -> Result<(), serde_json::Error> {
        let await_id = crate::types::ExternalAwaitId::new();
        let event = WorkflowEvent::ExternalAwaitFailed {
            await_id,
            reason_code: "target_failed".into(),
            message: Some("boom".into()),
            error_type: Some("PaymentDeclined".into()),
            details: Some(serde_json::json!({"code": 402})),
            non_retryable: Some(true),
        };
        assert_eq!(event.type_name(), "ExternalAwaitFailed");
        let json = serde_json::to_string(&event)?;
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        match back {
            WorkflowEvent::ExternalAwaitFailed {
                await_id: aid,
                reason_code,
                message,
                error_type,
                details,
                non_retryable,
            } => {
                assert_eq!(aid, await_id);
                assert_eq!(reason_code, "target_failed");
                assert_eq!(message.as_deref(), Some("boom"));
                assert_eq!(error_type.as_deref(), Some("PaymentDeclined"));
                assert_eq!(details.unwrap()["code"], 402);
                assert_eq!(non_retryable, Some(true));
            }
            _ => panic!("wrong variant"),
        }
        Ok(())
    }

    #[test]
    fn external_await_failed_pre_757_json_deserializes_without_optional_fields()
    -> Result<(), serde_json::Error> {
        // A minimal ExternalAwaitFailed with only reason_code — the additive
        // `#[serde(default)]` optional fields must deserialize to `None`.
        let await_id = crate::types::ExternalAwaitId::new();
        let legacy = serde_json::json!({
            "type": "ExternalAwaitFailed",
            "data": {
                "await_id": await_id,
                "reason_code": "target_unknown"
            }
        });
        let back: WorkflowEvent = serde_json::from_value(legacy)?;
        match back {
            WorkflowEvent::ExternalAwaitFailed {
                reason_code,
                message,
                error_type,
                details,
                non_retryable,
                ..
            } => {
                assert_eq!(reason_code, "target_unknown");
                assert_eq!(message, None);
                assert_eq!(error_type, None);
                assert_eq!(details, None);
                assert_eq!(non_retryable, None);
            }
            _ => panic!("wrong variant"),
        }
        Ok(())
    }

    #[test]
    fn external_await_events_are_not_terminal_lifecycle() {
        let await_id = crate::types::ExternalAwaitId::new();
        assert!(
            !WorkflowEvent::ExternalAwaitRequested {
                await_id,
                target: ExecutionId::new(),
            }
            .is_terminal_lifecycle()
        );
        assert!(
            !WorkflowEvent::ExternalAwaitResolved {
                await_id,
                output: serde_json::Value::Null,
            }
            .is_terminal_lifecycle()
        );
        assert!(
            !WorkflowEvent::ExternalAwaitFailed {
                await_id,
                reason_code: "target_failed".into(),
                message: None,
                error_type: None,
                details: None,
                non_retryable: None,
            }
            .is_terminal_lifecycle()
        );
    }

    // ── Cross-type continue-as-new (issue #803) ──────────────────────────

    /// A same-type continue-as-new records `new_workflow_type: None`, and the
    /// additive field must be **absent** from the serialized JSON so a
    /// pre-#803 reader (and a byte-comparison against a pre-#803 history) sees
    /// exactly today's shape.
    #[test]
    fn continued_as_new_same_type_omits_the_additive_field() -> Result<(), serde_json::Error> {
        let event = WorkflowEvent::WorkflowContinuedAsNew {
            new_exec_id: ExecutionId::new(),
            input: serde_json::json!({"cycle": 2}),
            new_workflow_type: None,
        };
        let json = serde_json::to_string(&event)?;
        assert!(
            !json.contains("new_workflow_type"),
            "new_workflow_type must skip when None, got {json}"
        );
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        match back {
            WorkflowEvent::WorkflowContinuedAsNew {
                new_workflow_type, ..
            } => assert!(new_workflow_type.is_none()),
            other => panic!("wrong variant: {}", other.type_name()),
        }
        Ok(())
    }

    /// A cross-type continue-as-new round-trips the target type verbatim.
    #[test]
    fn continued_as_new_cross_type_round_trips() -> Result<(), serde_json::Error> {
        let exec = ExecutionId::new();
        let event = WorkflowEvent::WorkflowContinuedAsNew {
            new_exec_id: exec,
            input: serde_json::json!({"tier": "paid"}),
            new_workflow_type: Some("paid_subscription".to_string()),
        };
        let json = serde_json::to_string(&event)?;
        assert!(json.contains("paid_subscription"), "got {json}");
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        match back {
            WorkflowEvent::WorkflowContinuedAsNew {
                new_exec_id,
                input,
                new_workflow_type,
            } => {
                assert_eq!(new_exec_id, exec);
                assert_eq!(input, serde_json::json!({"tier": "paid"}));
                assert_eq!(new_workflow_type.as_deref(), Some("paid_subscription"));
            }
            other => panic!("wrong variant: {}", other.type_name()),
        }
        Ok(())
    }

    /// **Append-only invariant (AC3).** A history written by a pre-#803 build
    /// carries no `new_workflow_type` key at all; it must deserialize to
    /// `None` (same-type) rather than failing, so in-flight executions replay
    /// identically across the upgrade.
    #[test]
    fn pre_803_continued_as_new_json_deserializes_to_none() -> Result<(), serde_json::Error> {
        let exec = ExecutionId::new();
        let legacy = format!(
            r#"{{"type":"WorkflowContinuedAsNew","data":{{"new_exec_id":"{exec}","input":{{"cycle":7}}}}}}"#
        );
        let back: WorkflowEvent = serde_json::from_str(&legacy)?;
        match back {
            WorkflowEvent::WorkflowContinuedAsNew {
                new_exec_id,
                input,
                new_workflow_type,
            } => {
                assert_eq!(new_exec_id, exec);
                assert_eq!(input, serde_json::json!({"cycle": 7}));
                assert!(
                    new_workflow_type.is_none(),
                    "a pre-#803 event must read as same-type"
                );
            }
            other => panic!("wrong variant: {}", other.type_name()),
        }
        Ok(())
    }
}
