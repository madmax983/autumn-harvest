//! Replay engine for deterministic workflow re-execution.
//!
//! The [`HistoryMatcher`] walks through previously recorded [`WorkflowEvent`]s
//! during replay, matching each workflow command against history to return
//! already-computed results instead of re-executing side effects.
//!
//! This is the brain of the durable execution model: when a workflow function
//! calls `execute_activity("send_email", ...)`, the matcher checks whether
//! history already contains a completed result for that activity and returns
//! it directly, avoiding duplicate side effects.

use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};

use crate::error::TimeoutType;
use crate::event::{SideEffectKind, WorkflowEvent};
use crate::types::{
    ActivityExecId, ExecutionId, ExternalActivityToken, ExternalAwaitId, ExternalCancelId,
    ExternalSignalId, ParentClosePolicy, UpdateId,
};

/// Result of matching a workflow command against the event history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HistoryMatch {
    /// History contains a completed result for this command.
    Matched {
        /// The JSON result value returned by the matched event.
        output: Value,
    },
    /// History contains a failure for this command.
    Failed {
        /// Human-readable failure message.
        error: String,
        /// Attempt number for the failed action.
        attempt: u32,
        /// Stable, low-cardinality error-type class recorded with the failure
        /// (issue #227 / #369), e.g. `"CircuitOpen"`. `"Error"` for legacy
        /// `Err(String)` failures. Carried so the consumer can build a typed
        /// [`HarvestError::ActivityFailed`] without parsing the message.
        error_type: String,
        /// Optional structured details recorded with a typed failure (e.g.
        /// `retry_after_secs` / `forced` for `CircuitOpen`). `None` otherwise.
        details: Option<Value>,
        /// Non-retryable flag recorded with a typed failure (issue #767). `true`
        /// only for a genuine typed `ActivityFailed`/`ChildWorkflowFailed` that
        /// marked itself permanent; `false` for legacy / untyped failures.
        non_retryable: bool,
    },
    /// History contains a timeout for this command.
    TimedOut {
        /// The type of timeout that occurred.
        timeout_type: TimeoutType,
    },
    /// Cursor is past the end of history — this is a new command.
    ActivityInProgress {
        /// The activity execution ID already recorded in history.
        activity_id: ActivityExecId,
    },
    NoMatch,
    /// The command does not match what was recorded at this position,
    /// indicating non-determinism in the workflow code.
    Diverged {
        /// What the history matcher expected to find based on recorded events.
        expected: String,
        /// What the workflow actually requested.
        actual: String,
        /// The event index where the divergence occurred.
        event_index: Option<i32>,
    },
    /// History shows an external activity was scheduled but no terminal event
    /// (completed/failed/timed-out) exists yet. The workflow should re-emit the
    /// schedule command with the same token and activity ID (idempotent at the
    /// worker level) and then suspend until the external completion arrives.
    AwaitingExternalCompletion {
        /// The activity execution ID already recorded in history.
        activity_id: ActivityExecId,
        /// The token already recorded in history. Must be reused to stay idempotent.
        token: ExternalActivityToken,
    },
    /// History shows a child workflow was started but no terminal event
    /// (completed/failed) exists yet.  This occurs when the parent wakes after
    /// one of several parallel children completes while others are still
    /// running.  The workflow should re-emit a `StartChildWorkflow` command
    /// carrying the **existing** `child_id` from history (idempotent at the
    /// worker level) and then suspend until the child's terminal event arrives.
    ChildInProgress {
        /// The child execution ID already recorded in history. Must be reused.
        child_id: ExecutionId,
    },
    /// History has a `LocalActivityScheduled` event but no `LocalActivityCompleted`
    /// event yet.  This covers two cases:
    ///
    /// 1. **Crash before first run** — the worker appended `LocalActivityScheduled`
    ///    then crashed before executing the handler. `failed_attempts` is 0.
    ///
    /// 2. **Crash between retries** — the worker recorded one or more
    ///    `LocalActivityFailed` events and then crashed before the next attempt.
    ///    `failed_attempts` reflects how many failures are already durable.
    ///
    ///    This case is also returned when all retry attempts have already been
    ///    recorded (`failed_attempts >= max_attempts`). In that situation the
    ///    worker compares `failed_attempts` against its retry policy and returns
    ///    `last_error` immediately without executing the handler.
    ///
    /// The caller must re-execute the local activity using the **same**
    /// `activity_id` so that the derived idempotency key is unchanged across
    /// the crash.
    LocalActivityInProgress {
        /// The `ActivityExecId` already recorded in history. Must be reused.
        activity_id: ActivityExecId,
        /// How many `LocalActivityFailed` events are already durable for this
        /// invocation. The worker starts its retry loop from `failed_attempts + 1`.
        failed_attempts: u32,
        /// Error from the last recorded `LocalActivityFailed`, if any.
        /// Returned by the worker when `failed_attempts >= max_attempts`.
        last_error: Option<String>,
    },
    /// History has an `ExternalSignalRequested` event but no terminal event
    /// (`ExternalSignalDelivered` or `ExternalSignalFailed`) yet.
    ///
    /// This occurs when the worker crashed after appending the request event
    /// but before recording the delivery outcome. The caller must re-attempt
    /// delivery using the **same** `signal_id` and the **same** `payload` so
    /// the idempotency key is unchanged, and must NOT append a second
    /// `ExternalSignalRequested` event.
    ExternalSignalInProgress {
        /// The `ExternalSignalId` already recorded in history. Must be reused.
        signal_id: ExternalSignalId,
        /// The payload stored in the durable `ExternalSignalRequested` event.
        /// The worker must re-send this exact payload rather than the current
        /// argument value, so that target receives consistent data if the
        /// workflow code changed the payload expression between crash and recovery.
        payload: serde_json::Value,
        /// Idempotency key recorded in the durable `ExternalSignalRequested`
        /// event, re-sent verbatim so re-delivery dedups against the target's
        /// partial unique index. `None` for older events / unkeyed sends.
        idempotency_key: Option<String>,
    },
    /// History contains an `ExternalSignalFailed` terminal event for a
    /// `signal_external_workflow` call.  Carries the original `signal_id` from
    /// history so the replayed error matches the durable event exactly.
    ExternalSignalFailed {
        /// The `ExternalSignalId` recorded in the originating `ExternalSignalRequested` event.
        signal_id: ExternalSignalId,
        /// The machine-readable reason code from history.
        reason_code: String,
    },
    /// History contains a `ChildWorkflowSpawnedDetached` event for a detached
    /// spawn.  Carries the `child_id` recorded in that event so the workflow
    /// function gets back the same [`ExecutionId`] it got on the first run.
    DetachedChildSpawned {
        /// The `ExecutionId` recorded in the `ChildWorkflowSpawnedDetached` event.
        child_id: ExecutionId,
    },
    /// History has an `ExternalCancelRequested` event but no terminal event yet.
    ///
    /// Crash-recovery path — mirrors `ExternalSignalInProgress`.
    ExternalCancelInProgress {
        /// The `ExternalCancelId` already recorded in history. Must be reused.
        cancel_id: ExternalCancelId,
    },
    /// History contains an `ExternalCancelFailed` terminal event for a
    /// `request_cancel_external_workflow` call.
    ExternalCancelFailed {
        /// The `ExternalCancelId` recorded in the originating event.
        cancel_id: ExternalCancelId,
        /// The machine-readable reason code from history.
        reason_code: String,
    },
    /// History has an `ExternalAwaitRequested` event but no terminal event yet
    /// (issue #757).
    ///
    /// Crash-recovery / still-pending path — mirrors `ExternalCancelInProgress`.
    /// The caller re-dispatches with the recorded `await_id` (idempotent) and
    /// suspends until the outbox appends a terminal to its own history.
    ExternalAwaitInProgress {
        /// The `ExternalAwaitId` already recorded in history. Must be reused.
        await_id: ExternalAwaitId,
    },
    /// History contains an `ExternalAwaitFailed` terminal event for an
    /// `await_external_workflow` call (issue #757). Carries the target's typed
    /// terminal cause so the replayed error matches the durable event exactly.
    ExternalAwaitFailed {
        /// The `ExternalAwaitId` recorded in the originating event.
        await_id: ExternalAwaitId,
        /// The machine-readable reason code from history.
        reason_code: String,
        /// Human-readable failure message from the target's terminal cause.
        message: Option<String>,
        /// Stable error-type name from a typed target failure.
        error_type: Option<String>,
        /// Structured details from a typed target failure.
        details: Option<Value>,
        /// Advisory non-retryable flag from a typed target failure.
        non_retryable: Option<bool>,
    },
}

/// Result of matching a signal-vs-timer race against the event history
/// (issue #476: `WorkflowContext::receive_signal_timeout`).
///
/// The race is resolved **deterministically by recorded history order**:
/// whichever of `SignalReceived` or `TimerFired` appears first in history
/// wins on every replay, regardless of wall-clock timing on the replaying
/// worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignalOrTimerMatch {
    /// The signal was recorded before the deadline timer fired (or before the
    /// timer was ever started). Carries the recorded signal payload.
    SignalWon {
        /// The JSON payload from the winning `SignalReceived` event.
        payload: Value,
    },
    /// The deadline timer fired before the signal arrived. No signal payload
    /// is consumed — a later delivery remains observable by a subsequent
    /// signal wait.
    TimerWon,
    /// `TimerStarted` is recorded but neither `TimerFired` nor a matching
    /// `SignalReceived` exists yet. The caller should re-emit the race
    /// commands (the worker dedupes the durable timer row by `timer_id`)
    /// and suspend again.
    InProgress,
    /// Cursor is past the end of history — this is the first live execution
    /// of the race.
    NoMatch,
    /// The recorded history does not match the requested race, indicating
    /// non-determinism in the workflow code.
    Diverged {
        /// What the history matcher expected to find based on recorded events.
        expected: String,
        /// What the workflow actually requested.
        actual: String,
        /// The event index where the divergence occurred.
        event_index: Option<i32>,
    },
}

/// Result of observing a child-workflow-vs-deadline race against recorded
/// history (issue #779: `ctx.execute_child_workflow_timeout`).
///
/// The race composes the existing
/// `ChildWorkflowStarted`/`ChildWorkflowCompleted`/`ChildWorkflowFailed` and
/// `TimerStarted`/`TimerFired` events — no new event variant. The winner is the
/// resolution event that appears **first in recorded history**, so the outcome
/// is deterministic across replays regardless of wall-clock timing on the
/// replaying worker.
// `serde_json::Value` fields (`output`, `details`) are `PartialEq` but not `Eq`,
// so `Eq` cannot be derived; silence the false-positive lint suggestion.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, PartialEq)]
pub enum ChildOrTimerMatch {
    /// The child workflow completed before the deadline timer fired. Carries the
    /// recorded child output.
    ChildCompleted {
        /// The JSON output from the winning `ChildWorkflowCompleted` event.
        output: Value,
    },
    /// The child workflow failed before the deadline timer fired. Carries the
    /// child's typed failure fields (issue #767); a legacy / untyped child
    /// failure decodes to the `"Error"` sentinel with no details and
    /// `non_retryable = false`.
    ChildFailed {
        /// Human-readable failure message from `ChildWorkflowFailed`.
        error: String,
        /// Stable error-type name; `"Error"` for a legacy / untyped failure.
        error_type: String,
        /// Structured details from a typed failure, if any.
        details: Option<Value>,
        /// Advisory non-retryable classification hint.
        non_retryable: bool,
    },
    /// The deadline timer fired before the child terminated. Carries the losing
    /// child's `ExecutionId` (so the caller can durably request-cancel it) and
    /// `child_already_terminal`, which is `true` iff the loser child's terminal
    /// is already recorded in history (the caller then suppresses re-pushing
    /// `CancelRaceLosers`).
    TimerFired {
        /// The `ExecutionId` of the losing child workflow.
        child_id: ExecutionId,
        /// Whether the loser child's terminal is already recorded.
        child_already_terminal: bool,
    },
    /// `ChildWorkflowStarted` and `TimerStarted` are both recorded but neither a
    /// child terminal nor `TimerFired` exists yet. The caller re-emits the race
    /// commands (the worker dedupes the child by `child_id` and the timer row by
    /// `timer_id`) and suspends again. Carries the recorded `child_id` so the
    /// re-emitted `StartChildWorkflow` reuses it.
    InProgress {
        /// The `ExecutionId` recorded in `ChildWorkflowStarted`.
        child_id: ExecutionId,
    },
    /// Cursor is past the end of history — this is the first live execution of
    /// the race.
    NoMatch,
    /// The recorded history does not match the requested race, indicating
    /// non-determinism in the workflow code.
    Diverged {
        /// What the history matcher expected to find based on recorded events.
        expected: String,
        /// What the workflow actually requested.
        actual: String,
        /// The event index where the divergence occurred.
        event_index: Option<i32>,
    },
}

/// Result of observing a cancellable durable timer's outcome against recorded
/// history (issue #768: `TimerHandle::await_fire`).
///
/// The fire-vs-cancel outcome is resolved **deterministically by recorded
/// history order**: whichever of `TimerFired` or `TimerCancelled` for the timer
/// id appears first in history wins on every replay, regardless of wall-clock
/// timing on the replaying worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimerFireMatch {
    /// A `TimerFired` for the id was recorded before any `TimerCancelled`.
    Fired,
    /// A `TimerCancelled` for the id was recorded before any `TimerFired`.
    Cancelled,
    /// The timer is armed but neither a fire nor a cancel is recorded yet, or
    /// the cursor is past the end of history (first live await). The caller
    /// re-arms (idempotently) and suspends again.
    NoMatch,
}

/// One step of the shared crossable-set discipline for the cancellable-timer
/// forward scans (issue #768).
///
/// See [`HistoryMatcher::timer_scan_cross_or_stop`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimerScanStep {
    /// The event at the scan position is genuinely transparent / interleavable —
    /// the scan may cross it (advancing the scan cursor).
    Cross,
    /// The event at the scan position is an UNCONSUMED command-ordering point the
    /// timer scan must not cross — stop the scan.
    Stop,
}

/// Result of matching a `patched()` call against recorded history (issue #687).
///
/// A deliberate three-state result rather than a bare `bool`: the caller
/// ([`crate::context::WorkflowContext::patched`]) must distinguish "the marker
/// was recorded" (return `true`, consume nothing new) from "we are on the live
/// frontier" (return `true` AND record a fresh `patch:{id}` marker) from
/// "replaying pre-patch history" (return `false`, record nothing). Collapsing
/// the first two into one boolean would force the caller to re-derive the
/// live-vs-replay distinction after the fact — the ambiguity `match_version`'s
/// "return max and let the caller re-check `is_replaying()`" trick papers over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatchMarkerMatch {
    /// A `patch:{id}` (or interop `version:{id}`) marker was consumed at the
    /// cursor — or the id was previously deprecated and the memo says a marker
    /// was present in history. The caller takes the new branch.
    Recorded,
    /// Replaying, but no marker for this id at this position — pre-patch
    /// history; the caller must take the old branch. The cursor is not
    /// advanced, so the actual recorded event still matches the next command.
    Absent,
    /// Past recorded history — live frontier. The caller records a fresh
    /// `patch:{id}` marker and takes the new branch.
    NewlyPatched,
}

/// Result of matching a durable-mutex acquire against recorded history
/// (issue #691).
///
/// A [`WorkflowEvent::MutexGranted`] is the replay anchor for
/// `ctx.mutex(key).acquire()`: it is appended at a clean resume cursor when the
/// lock is granted, so the matcher is strictly positional (mirrors
/// [`HistoryMatcher::match_timer_arm`]). Release is event-less bookkeeping, so
/// no forward-scan skip-list needs updating.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MutexGrantMatch {
    /// A `MutexGranted` for the key was recorded at the cursor and consumed. The
    /// caller rebuilds the held-lock guard using `lock_seq`.
    Granted {
        /// The fencing token recorded with the grant.
        lock_seq: i64,
        /// The wall-clock instant the lock was granted.
        acquired_at: chrono::DateTime<chrono::Utc>,
    },
    /// The cursor is past end of history (first live acquire) — the caller
    /// suspends on an `AcquireMutex` command.
    NoMatch,
    /// A different event / key is at the cursor — non-deterministic divergence.
    Diverged {
        /// What the workflow expected (`MutexGranted(key)`).
        expected: String,
        /// What was actually recorded at the cursor.
        actual: String,
        /// Index of the diverging event, if representable.
        event_index: Option<i32>,
    },
}

/// Outcome of a *tolerant* deadline-branch clock read (issue #772).
///
/// Used ONLY by `should_continue_as_new`'s internal deadline check
/// ([`crate::context::WorkflowContext::deadline_clock_read`]) — never by the
/// public `system_now()`/`time_until_deadline()`, which stay strict.
///
/// The critical difference from
/// [`match_side_effect_event`](HistoryMatcher::match_side_effect_event): a
/// cursor holding a *non-`Now`* recorded event resolves to
/// [`SideEffectNowMatch::NotRecorded`] (cursor left untouched, no divergence)
/// rather than [`HistoryMatch::Diverged`]. This is what lets a workflow that
/// declares an `execution_timeout` and calls `should_continue_as_new()` at the
/// top of a run resume gracefully under a new binary even when its recorded
/// history predates #772 (and so carries no `SideEffectRecorded{Now}` at that
/// position): the deadline branch simply degrades to history-count-only for the
/// pre-upgrade recorded portion instead of recording a non-determinism error
/// and nd-blocking the run (issue #603).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SideEffectNowMatch {
    /// The cursor held the probe's own sentinel-named
    /// `SideEffectRecorded { kind: Now, name: Some(DEADLINE_PROBE_SIDE_EFFECT_NAME) }`;
    /// its recorded millis value was consumed and the cursor advanced (identical
    /// to strict replay).
    Matched {
        /// The recorded wall-clock instant, as Unix milliseconds.
        millis: i64,
    },
    /// The cursor held some *other* recorded event — an old-history migration
    /// (the `should_continue_as_new()` call site recorded no probe clock read
    /// here) OR an author-side `system_now()` `Now` (`name: None`) that belongs
    /// to the workflow's own code, not the probe. The cursor is left untouched
    /// and no divergence is raised — the caller degrades the deadline branch to
    /// "unavailable". Fail-safe by construction: it can never emit a wrong
    /// command, never steals a user `Now`, and any genuine control-flow
    /// divergence still nd-errors at the next real command.
    NotRecorded,
    /// The cursor is at the live frontier (past the end of recorded history).
    /// The caller records a fresh sentinel-named `Now` side-effect (like
    /// `system_now()`, but under `DEADLINE_PROBE_SIDE_EFFECT_NAME`) and returns
    /// the captured instant.
    Frontier,
}

/// Reserved side-effect name for the engine-internal deadline-branch clock read
/// (issue #772).
///
/// The `should_continue_as_new()` deadline check records its wall-clock read as a
/// `SideEffectRecorded { kind: Now, name: Some(this) }`, so
/// [`match_side_effect_now_tolerant`](HistoryMatcher::match_side_effect_now_tolerant)
/// can tell the probe's own `Now` apart from an author-side `ctx.system_now()`
/// (recorded as `{ kind: Now, name: None }`). Follows the `__harvest_*` /
/// `__signal_timeout:` reserved-name convention; author code never records a
/// `Now` under this name.
pub const DEADLINE_PROBE_SIDE_EFFECT_NAME: &str = "__harvest_deadline_probe";

/// Marker name recorded by `ctx.patched(patch_id)` (issue #687).
pub(crate) fn patch_marker_name(patch_id: &str) -> String {
    format!("patch:{patch_id}")
}

/// Marker name recorded by `ctx.version(change_id, ..)` — also consumed by the
/// patch primitives for `version()` → `patched()` interop (issue #687).
pub(crate) fn version_marker_name(change_id: &str) -> String {
    format!("version:{change_id}")
}

/// Result of matching a saga compensation dedup marker against recorded
/// history (issue #801).
///
/// Mirrors [`PatchMarkerMatch`]'s tolerant shape (minus the patch-specific
/// deprecation memo / `version:` interop / same-cycle latch, none of which
/// apply here), extended post-review with a fourth state so the caller
/// ([`crate::context::WorkflowContext::observe_saga_unwind_start`] /
/// [`observe_saga_unwind_failed`](crate::context::WorkflowContext::observe_saga_unwind_failed))
/// can resolve the whole unwind's disposition **once** and keep the
/// compensated/failed counter pair coherent (invariant: `failed ≤
/// compensated`, per unwind):
///
/// - "the marker was recorded" → stay silent;
/// - "live frontier" → record a fresh marker AND emit (the exactly-once
///   point);
/// - "drained-signal frontier" → a recorded position whose only remaining
///   events were trailing un-awaited signals; whether to record is the
///   caller's call, keyed to the unwind's disposition;
/// - "pre-#801 marker-less history" → stay silent, touch nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SagaMarkerMatch {
    /// A saga marker with this exact name was consumed at the cursor (or,
    /// tolerantly, found past only command-less `WorkflowCancelled`
    /// lifecycle events / trailing signals and consumed out-of-order) — the
    /// unwind was already counted on a previous cycle. The caller emits
    /// nothing.
    Recorded,
    /// Replaying, but no marker with this name at this position — a pre-#801
    /// history (or an unwind entered with unconsumed non-transparent events
    /// at the cursor). The cursor is not advanced, so the recorded event
    /// still matches the next command; the caller records nothing and emits
    /// nothing.
    Absent,
    /// Past recorded history — the first live execution of this unwind —
    /// or separated from the frontier only by command-less
    /// `WorkflowCancelled` lifecycle events (the cancel-and-compensate
    /// pattern: the cancellation event has no workflow-command counterpart
    /// and never leaves the cursor, so it must not hide the frontier from a
    /// metrics-only marker). The caller records a fresh marker and emits the
    /// counter exactly here — **unless** the context is a `WorkflowReplayer`
    /// strict/canary probe: the matcher cannot distinguish the engine's
    /// genuinely-live cancel-and-compensate cycle from a probe's read of a
    /// pre-#801 marker-less *terminal* cancelled history (the two histories
    /// are byte-identical), so the caller additionally gates this arm on
    /// `WorkflowContext::is_replay_probe` (Codex P2, PR #973 review).
    LiveFrontier,
    /// Replaying pre-drain, but after stashing trailing un-awaited signal
    /// events the cursor is past the end of recorded history — a
    /// drained-signal frontier (canonically: a signal-with-start run whose
    /// unwind begins before the staged signal is awaited, or a duplicate
    /// webhook signal ingested at the final unwind cycle's wake).
    ///
    /// Recording a marker here is replay-consistent (the marker lands past
    /// the drained signals, exactly where the next cycle's drain re-finds
    /// it), but whether to record is the **caller's** decision, keyed to the
    /// unwind's disposition: `observe_saga_unwind_start` stays conservative
    /// (parity with `ctx.patched()`'s signal-with-start caveat — the whole
    /// unwind is uncounted), while `observe_saga_unwind_failed` records here
    /// for a **counted** unwind so a trailing duplicate signal can never
    /// suppress the page-severity failure counter (post-review P2-1).
    DrainedSignalFrontier,
}

/// Marker name recorded at unwind start by `Saga`'s compensation
/// instrumentation (issue #801). `seq` is the per-context saga sequence
/// number, deterministic per call site across replays.
pub(crate) fn saga_compensated_marker_name(seq: u32) -> String {
    format!("saga_compensated:{seq}")
}

/// Marker name recorded when a saga unwind finishes with at least one
/// compensation error (issue #801).
pub(crate) fn saga_compensation_failed_marker_name(seq: u32) -> String {
    format!("saga_compensation_failed:{seq}")
}

/// Terminal outcome for an early-drained external signal.
#[derive(Debug, Clone)]
enum StashedSignalTerminal {
    Delivered,
    Failed(String),
}

/// An `ExternalSignalRequested` event that was drained early by
/// [`HistoryMatcher::drain_early_signals`] before the cursor reached the
/// normal matching position.  Stored so that [`HistoryMatcher::match_external_signal`]
/// can return the correct result regardless of where in history the signal
/// pair falls relative to other durable events.
#[derive(Debug, Clone)]
struct StashedExternalSignal {
    signal_id: ExternalSignalId,
    target: crate::types::ExternalTarget,
    signal_name: String,
    /// Durable payload from the recorded `ExternalSignalRequested` event.
    /// Carried through so that crash-recovery re-dispatch uses the same
    /// payload that was originally sent, not whatever the workflow currently
    /// passes as an argument.
    payload: serde_json::Value,
    /// Durable idempotency key from the recorded `ExternalSignalRequested`
    /// event, reused verbatim on crash-recovery re-dispatch.
    idempotency_key: Option<String>,
    terminal: Option<StashedSignalTerminal>,
}

/// Terminal outcome for an early-drained external cancel.
#[derive(Debug, Clone)]
enum StashedCancelTerminal {
    Delivered,
    Failed(String),
}

/// An `ExternalCancelRequested` event that was drained early (mirrors
/// `StashedExternalSignal` for the cancel primitive, issue #492).
#[derive(Debug, Clone)]
struct StashedExternalCancel {
    cancel_id: ExternalCancelId,
    target: crate::types::ExternalTarget,
    terminal: Option<StashedCancelTerminal>,
}

/// Terminal outcome for an early-drained external await (issue #757).
#[derive(Debug, Clone)]
enum StashedAwaitTerminal {
    /// Target reached `COMPLETED` — carries the recorded output value.
    Resolved(Value),
    /// Target reached a non-`COMPLETED` terminal state (or was unknown) —
    /// carries the recorded reason code + typed failure fields.
    Failed {
        reason_code: String,
        message: Option<String>,
        error_type: Option<String>,
        details: Option<Value>,
        non_retryable: Option<bool>,
    },
}

/// An `ExternalAwaitRequested` event that was drained early (mirrors
/// `StashedExternalCancel` for the await primitive, issue #757).
#[derive(Debug, Clone)]
struct StashedExternalAwait {
    await_id: ExternalAwaitId,
    target: ExecutionId,
    terminal: Option<StashedAwaitTerminal>,
}

/// The frozen resolution recorded on a `LocalActivityScheduled` event
/// (issue #620, AC8; Codex P2). Returned by
/// [`HistoryMatcher::recorded_local_activity_resolution`].
///
/// `Default` yields the "unresolved legacy" shape (`resolved == false`, both
/// frozen fields `None`) so a pre-#620 event or an absent `activity_id` falls
/// back to live re-derivation on crash recovery.
#[derive(Debug, Clone, Default)]
pub(crate) struct LocalActivityResolution {
    /// `true` for a #620+ event whose frozen fields below are authoritative
    /// (even when `None`); `false` for a legacy event → re-derive live.
    pub(crate) resolved: bool,
    /// The frozen fully-resolved retry policy, or `None`.
    pub(crate) retry_policy: Option<crate::policy::RetryPolicy>,
    /// The frozen fully-resolved `start_to_close` in nanoseconds (full
    /// `Duration` precision), or `None`.
    pub(crate) start_to_close_nanos: Option<u64>,
}

/// Walks through recorded workflow events during replay, matching
/// commands against what was previously recorded.
///
/// The cursor advances through events sequentially. During replay
/// (`is_replaying() == true`), each workflow command must match the
/// corresponding event in history. Once the cursor reaches the end
/// of history, new commands produce [`HistoryMatch::NoMatch`] and
/// will be executed for real.
pub struct HistoryMatcher {
    events: Vec<WorkflowEvent>,
    cursor: usize,
    consumed_out_of_order_events: HashSet<usize>,
    consumed_signal_events: HashSet<usize>,
    pending_signals: VecDeque<(String, Value, usize)>,
    /// External signals drained before their natural cursor position,
    /// e.g. when signal events appear before `ActivityScheduled` or
    /// `TimerStarted` events in a mixed-batch history.
    pending_external_signals: Vec<StashedExternalSignal>,
    /// External cancels drained before their natural cursor position (issue #492).
    pending_external_cancels: Vec<StashedExternalCancel>,
    /// External awaits drained before their natural cursor position (issue #757).
    pending_external_awaits: Vec<StashedExternalAwait>,
    /// Indices of events that are transparent to command-dispatch replay and
    /// therefore pre-marked consumed (issue #383: `WorkflowExecutionPaused` /
    /// `WorkflowExecutionResumed`). These are pure operator-lifecycle no-ops:
    /// they have no workflow-command counterpart and must never flag
    /// non-determinism, so every scan loop skips them via [`Self::is_consumed`].
    transparent_events: HashSet<usize>,
    /// Event indices of the exact `SignalReceived` events that lost a
    /// signal-or-deadline race (issue #476): for each race whose `TimerFired`
    /// precedes a matching signal, the **first** such signal event is the
    /// loser. A late loser is a normal production occurrence on the timeout
    /// branch — the workflow may intentionally never consume it — so
    /// [`Self::has_non_lifecycle_unconsumed`] must not report that specific
    /// event as early-completion non-determinism. Any other unconsumed signal
    /// (a second same-name delivery, or the loser's exemption spent because a
    /// later wait consumed it) still flags. The excused signal stays
    /// deliverable to any subsequent signal wait (the exemption only
    /// suppresses the completed-history check, never consumption).
    late_race_signal_events: HashSet<usize>,
    /// `SignalReceived` event indices that fall inside a still-open
    /// signal-or-deadline race window (issue #476) for their signal name --
    /// i.e. a `TimerStarted { timer_id: "__signal_timeout:{seq}:{name}" }`
    /// precedes the index with no matching `TimerFired` recorded yet at or
    /// before it. These are reserved for `Self::match_signal_or_timer`'s own
    /// resolution and must never be claimed by `Self::claim_pending_signal`
    /// (issue #546): doing so would silently flip the race outcome to
    /// `TimerWon` even though the signal arrived first. A signal recorded
    /// *after* its race's `TimerFired` (the race already resolved
    /// `TimerWon`) is an ordinary "late loser" and is not reserved -- it
    /// stays fair game for a push handler exactly like it already stays
    /// fair game for a later plain signal wait.
    ///
    /// In practice a push handler can never reach a signal still inside an
    /// open race window anyway -- the race's own `TimerStarted` is not one
    /// of the "transparent" events `Self::drain_early_signals` skips over,
    /// so it hard-blocks the cursor until `Self::match_signal_or_timer`
    /// itself consumes it. This set is kept as an explicit, independently
    /// verified guarantee rather than an implicit consequence of that
    /// scan-ordering detail.
    race_reserved_signal_events: HashSet<usize>,
    /// Deprecated patch ids (issue #687), memoizing whether a `patch:{id}` /
    /// `version:{id}` marker was present anywhere in this history. Populated
    /// by [`Self::deprecate_patch`], which marks every such marker consumed
    /// (positional matching cannot apply — phase-2 code calls
    /// `deprecate_patch` at a different, usually earlier, position than the
    /// phase-1 `patched()` call that recorded the marker, so the marker must
    /// become transparent wherever it sits or it would trip the next
    /// `match_*` as a divergence). The memo keeps a *residual* `patched(id)`
    /// call after `deprecate_patch(id)` deterministic: `true` for phase-1
    /// histories, `false` for phase-0 histories AND for new executions
    /// started post-deprecation, on both live and replay passes.
    deprecated_patches: HashMap<String, bool>,
    /// Patch/version ids whose marker was recorded on the **live frontier of
    /// this very cycle** (issue #687, review hardening): a
    /// [`Self::match_patch_marker`] `NewlyPatched` result or a
    /// [`Self::match_version`] live-frontier `max_version` result means the
    /// context is about to push a `RecordMarker` command that exists only as
    /// a pending command — invisible to [`Self::deprecate_patch`]'s
    /// full-history scan. Without this latch, a same-cycle
    /// `patched(id)` → `deprecate_patch(id)` → `patched(id)` sandwich would
    /// memoize `false` on the live pass and `true` on every replay pass — a
    /// live/replay branch flip and a permanent nd-block (issue #603).
    /// `deprecate_patch` ORs this set into its presence computation so the
    /// live cycle agrees with every replay cycle.
    patch_ids_recorded_this_cycle: HashSet<String>,
    /// Whether the MOST RECENT cancellable-timer forward scan
    /// ([`Self::match_timer_cancel`] / [`Self::match_timer_or_cancel`], issue
    /// #768) STOPPED at an unconsumed command-bearing event
    /// ([`TimerScanStep::Stop`]) rather than claiming its target or running off
    /// the end of history. Both scans reset this to `false` on entry (after
    /// `prepare_match`) and set it `true` on the `Stop` break.
    ///
    /// A `NoMatch` return paired with this flag `true` means the scan was
    /// **blocked**: a same-id `TimerCancelled`/`TimerFired` (or a genuinely new
    /// cancel/await) is being emitted BEFORE a command history recorded first —
    /// a real non-determinism divergence, distinct from a genuine live-frontier
    /// `NoMatch` (scan ran off the end, cursor at the frontier) which is a
    /// legitimate new live append. [`Self::timer_scan_stopped_at_command`]
    /// exposes it so `cancel_timer`/`reset_timer`/`await_timer_fire` can treat a
    /// blocked scan as a divergence in NORMAL worker replay too, not only under
    /// strict `WorkflowReplayer` mode (Codex P2 round 12, issue #768).
    timer_scan_stopped_at_command: bool,
    /// Index of the first event of a **terminal-failure tail** (issue #952), or
    /// `None` when this history does not end in a failure.
    ///
    /// A failing decision cycle's history is truncated by construction: the
    /// commands the cycle issued were never turned into events, and no further
    /// event can ever be appended to the sealed run. So a trailing terminal
    /// `WorkflowFailed` (together with any post-terminal bookkeeping appended
    /// after it) is made transparent in [`Self::new`] — every `match_*` at that
    /// cursor answers `NoMatch` ("the run is failing") instead of `Diverged` —
    /// and, once every pre-terminal event is consumed, the cursor is said to be
    /// at the **failing frontier** ([`Self::at_terminal_failure_frontier`]),
    /// where the strict-replay layers above stop reporting divergence.
    ///
    /// Deliberately failure-only: a `WorkflowCompleted`/`WorkflowCancelled`
    /// history is verified in full, and a `WorkflowFailed` that is *reopened* by
    /// a `WorkflowRedriven` (#510) is not a tail — that case keeps its own,
    /// redrive-anchored transparency rule.
    terminal_failure_tail: Option<usize>,
}

impl HistoryMatcher {
    /// Create a new matcher from a list of recorded events.
    #[must_use]
    pub fn new(events: Vec<WorkflowEvent>) -> Self {
        // Pre-mark pause/resume events as consumed so they are transparent to
        // every cursor-based scan (issue #383). They carry no workflow command,
        // so settling them up front keeps the matcher's scan loops unchanged.
        //
        // Post-terminal bookkeeping gets the same treatment (issue #1262).
        // A workflow-level retry's `WorkflowRetryScheduled` (#523) and a
        // parent-close cascade's `ChildWorkflowCascadeApplied` (#347)
        // carry no workflow command. The workflow function never consumes
        // either. Both are always transparent, not only while searching
        // for a redrive's superseded terminal. Left opaque, a
        // retried-then-redriven run's cursor gets stuck on the
        // bookkeeping event itself, before it ever reaches the marker or
        // dispatch behind it.
        let mut transparent_events: HashSet<usize> = events
            .iter()
            .enumerate()
            .filter(|(_, e)| {
                Self::is_pause_lifecycle_event(e) || Self::is_post_terminal_bookkeeping(e)
            })
            .map(|(i, _)| i)
            .collect();
        // DLQ redrive (issue #510): a `WorkflowRedriven` event reopens a run that
        // was sealed `FAILED` at quarantine time. Mark the redrive event AND the
        // superseded terminal `WorkflowFailed` it sits behind as transparent so
        // command dispatch advances the cursor past the reopened terminal instead
        // of diverging when the re-enqueued task re-issues the failed command.
        //
        // This is **redrive-anchored**: only a `WorkflowFailed` immediately
        // preceding a `WorkflowRedriven` (skipping already-transparent events) is
        // made transparent. A bare trailing `WorkflowFailed` with no following
        // redrive stays non-transparent — it is a genuinely failed run and its
        // replay (queries, the replayer harness) must be unaffected.
        let mut last_redrive: Option<usize> = None;
        for (i, event) in events.iter().enumerate() {
            if Self::is_redrive_lifecycle_event(event) {
                last_redrive = Some(i);
                transparent_events.insert(i);
                // Scan backward to the nearest WorkflowFailed. Skip events
                // already settled transparent: an interleaved pause pair, or
                // post-terminal bookkeeping (already marked transparent
                // above) appended AFTER the terminal. Without that skip a
                // retried-then-redriven run would leave its superseded
                // terminal opaque and diverge against it.
                let mut j = i;
                while j > 0 {
                    j -= 1;
                    if transparent_events.contains(&j) {
                        continue;
                    }
                    if matches!(events[j], WorkflowEvent::WorkflowFailed { .. }) {
                        transparent_events.insert(j);
                    }
                    break;
                }
            }
        }
        // Issue #952 x #510: a redrive REOPENS a run that a failing cycle sealed,
        // and that cycle recorded the awaited work it abandoned (see
        // `crate::event::ABANDONED_DISPATCH_REASON`). Those records exist to keep
        // the audit trail honest about a run that is over — they must never
        // become history the reopened run replays, or the redriven cycle would
        // read back "this activity/child failed" and could never re-issue the
        // dispatch the operator redrove it to complete. Mark each abandoned pair
        // transparent so the reopened run emits it live, exactly as it did
        // before #952 recorded anything at all.
        //
        // Bounded by the LAST redrive (Codex P1 round 1): only a cycle a
        // redrive actually superseded is re-opened work. A run that was
        // redriven and then failed AGAIN wrote fresh abandoned pairs after that
        // redrive; those belong to the terminal-failure tail's own cycle and
        // stay opaque, so a replay resolves them from their synthetic terminal
        // and re-derives the failure path the recorded run took instead of
        // parking on a dispatch that never resolves.
        if let Some(redrive_idx) = last_redrive {
            for i in Self::abandoned_dispatch_indices(&events[..redrive_idx]) {
                transparent_events.insert(i);
            }
            // Issue #1262: an abandoned pair is not the only record a
            // failing cycle writes before its terminal `WorkflowFailed`.
            // The cycle can also record a `MarkerRecorded` or
            // `SideEffectRecorded` event after the dispatch a redrive
            // wants to re-issue — for example a `ctx.version()` call.
            // Left opaque, that record blocks the cursor. A re-issued
            // dispatch compares against it positionally and reports
            // `Diverged` instead of appending live. Swallow the rest of
            // that same superseded cycle's tail too.
            for i in
                Self::superseded_cycle_tail_indices(&events[..redrive_idx], &transparent_events)
            {
                transparent_events.insert(i);
            }
        }
        // Terminal-failure tail (issue #952): a run sealed FAILED can never
        // grow another event, so everything from its terminal event onward is
        // transparent to command-dispatch replay — mirroring how the
        // pause/resume and redrive events above are pre-marked consumed.
        let terminal_failure_tail = Self::terminal_failure_tail_start(&events);
        if let Some(start) = terminal_failure_tail {
            for i in start..events.len() {
                transparent_events.insert(i);
            }
        }
        let race_reserved_signal_events = Self::build_race_reserved_signal_events(&events);

        Self {
            events,
            cursor: 0,
            consumed_out_of_order_events: HashSet::new(),
            consumed_signal_events: HashSet::new(),
            pending_signals: VecDeque::new(),
            pending_external_signals: Vec::new(),
            pending_external_cancels: Vec::new(),
            pending_external_awaits: Vec::new(),
            transparent_events,
            late_race_signal_events: HashSet::new(),
            race_reserved_signal_events,
            deprecated_patches: HashMap::new(),
            patch_ids_recorded_this_cycle: HashSet::new(),
            timer_scan_stopped_at_command: false,
            terminal_failure_tail,
        }
    }

    /// Index of the first event of this history's **terminal-failure tail**, or
    /// `None` if it has none (issue #952).
    ///
    /// Walks backwards from the end over events that carry no workflow command
    /// and are never consumed by the workflow function — operator pause/resume
    /// (#383) and post-terminal bookkeeping appended *after* a terminal event
    /// (`WorkflowRetryScheduled` from a workflow-level retry (#523),
    /// `ChildWorkflowCascadeApplied` from a parent-close cascade (#347)). If the
    /// first event it lands on is a `WorkflowFailed`, that index opens the tail.
    ///
    /// A `WorkflowRedriven` (#510) is deliberately **not** skipped: a redriven
    /// run is reopened, not failing, and keeps the narrower redrive-anchored
    /// transparency instead.
    ///
    /// Public so [`crate::testing`] can ask the same question of a snapshot's
    /// events without building a matcher — one definition, two callers.
    #[must_use]
    pub fn terminal_failure_tail_start(events: &[WorkflowEvent]) -> Option<usize> {
        let mut idx = events.len();
        while idx > 0 {
            idx -= 1;
            let event = &events[idx];
            if Self::is_pause_lifecycle_event(event) || Self::is_post_terminal_bookkeeping(event) {
                continue;
            }
            // `idx > 0` guard: a history whose FIRST event is the terminal
            // failure is not a run that failed after doing something — it is a
            // truncated or corrupt export (a real run always opens with
            // `WorkflowStarted`). Excusing everything a candidate build does
            // against such a fixture would let a malformed export pass the gate
            // unconditionally, so it keeps the pre-#952 strict treatment.
            return (idx > 0 && matches!(event, WorkflowEvent::WorkflowFailed { .. }))
                .then_some(idx);
        }
        None
    }

    /// Events appended **after** a run's terminal event as durable bookkeeping.
    /// They have no workflow-command counterpart and are never consumed by the
    /// workflow function, so they cannot end a terminal-failure tail.
    const fn is_post_terminal_bookkeeping(event: &WorkflowEvent) -> bool {
        matches!(
            event,
            WorkflowEvent::WorkflowRetryScheduled { .. }
                | WorkflowEvent::ChildWorkflowCascadeApplied { .. }
        )
    }

    /// Indices of the **abandoned-dispatch records** a failing cycle wrote
    /// (issue #952): each `ActivityFailed` / `ChildWorkflowFailed` carrying
    /// [`crate::event::ABANDONED_DISPATCH_REASON`], together with the
    /// `ActivityScheduled` / `ChildWorkflowStarted` for the same id that
    /// precedes it.
    ///
    /// `events` is the **prefix of history the caller wants scanned** (see
    /// [`Self::new`]: everything before the last `WorkflowRedriven`), so the
    /// returned indices are always valid indices into the full history too.
    ///
    /// The reason constant is the engine's own reserved marker — the same device
    /// as the `__signal_timeout:{seq}:{name}` timer ids (#476) and the
    /// `fan_out:{n}` markers (#601). For a child terminal, whose `error` carries
    /// the CHILD's own author string, the reason is matched together with the
    /// exact shape the engine writes (untyped, non-retryable) so an author
    /// message that happens to collide keeps its genuine terminal.
    ///
    /// An activity author can also quote the reason AND match the shape (issue
    /// #1265): `ActivityFailure::non_retryable` reproduces both. The
    /// synthetic path never dispatches. It never writes `ActivityStarted` or
    /// `ActivityHeartbeat` for the id. A real attempt does. This is a
    /// structural check, not another field-value guess. A candidate activity
    /// with either event earlier in `events` keeps its genuine terminal
    /// instead of being marked transparent. History is chronological, so one
    /// forward scan sees a real start before its own first-attempt failure.
    ///
    /// See also [`Self::superseded_cycle_tail_indices`], called right after
    /// this function in [`Self::new`] (issue #1262). It covers the same
    /// failing cycle's other records: a marker, a side effect, a detached
    /// spawn, a timer arm or cancel. This function does not cover those.
    fn abandoned_dispatch_indices(events: &[WorkflowEvent]) -> Vec<usize> {
        let mut started_activities: HashSet<ActivityExecId> = HashSet::new();
        let mut abandoned_activities: HashSet<ActivityExecId> = HashSet::new();
        let mut abandoned_children: HashSet<ExecutionId> = HashSet::new();
        let mut indices: Vec<usize> = Vec::new();
        for (i, event) in events.iter().enumerate() {
            match event {
                WorkflowEvent::ActivityStarted { activity_id, .. }
                | WorkflowEvent::ActivityHeartbeat { activity_id, .. } => {
                    started_activities.insert(*activity_id);
                }
                // An activity's `error` is the ACTIVITY author's own message,
                // exactly as a child's is (Codex P2 round 2), so the reason
                // string alone is not proof the engine wrote this event. Pair it
                // with the full shape `abandoned_dispatch_events` writes —
                // first attempt, untyped `"Error"` class, non-retryable, no
                // structured details — so a genuine activity failure that
                // happens to return this message keeps its real terminal
                // instead of being re-dispatched (and its side effects
                // repeated) by a redriven run. A real `ActivityStarted` /
                // `ActivityHeartbeat` for this id is the same guard, checked
                // structurally instead of by field value (issue #1265).
                WorkflowEvent::ActivityFailed {
                    activity_id,
                    error,
                    attempt: 1,
                    error_type,
                    non_retryable: true,
                    details: None,
                } if error == crate::event::ABANDONED_DISPATCH_REASON
                    && error_type == "Error"
                    && !started_activities.contains(activity_id) =>
                {
                    abandoned_activities.insert(*activity_id);
                    indices.push(i);
                }
                // A child's `error` is normally the CHILD's own author-produced
                // string, so the reason alone is not proof the engine wrote this
                // event. Pair it with the exact shape the engine writes — untyped
                // and non-retryable — so a child that coincidentally returns the
                // same message keeps its genuine terminal.
                WorkflowEvent::ChildWorkflowFailed {
                    child_id,
                    error,
                    error_type: None,
                    details: None,
                    non_retryable: Some(true),
                } if error == crate::event::ABANDONED_DISPATCH_REASON => {
                    abandoned_children.insert(*child_id);
                    indices.push(i);
                }
                _ => {}
            }
        }
        if indices.is_empty() {
            return indices;
        }
        for (i, event) in events.iter().enumerate() {
            match event {
                WorkflowEvent::ActivityScheduled { activity_id, .. }
                    if abandoned_activities.contains(activity_id) =>
                {
                    indices.push(i);
                }
                WorkflowEvent::ChildWorkflowStarted { child_id, .. }
                    if abandoned_children.contains(child_id) =>
                {
                    indices.push(i);
                }
                _ => {}
            }
        }
        indices
    }

    /// Indices of a superseded failing cycle's remaining pre-terminal
    /// records (issue #1262).
    ///
    /// [`Self::abandoned_dispatch_indices`] already covers the abandoned
    /// dispatch pairs. This function covers the rest. `worker::terminal_command_policy`
    /// classifies these as `PreTerminalEvent`: a record a failing cycle
    /// writes plainly, at its own command-emission position. It has no
    /// synthetic terminal and no completion to pair it with. Today that
    /// set is `MarkerRecorded`, `SideEffectRecorded`,
    /// `ChildWorkflowSpawnedDetached`, `TimerStarted`, and
    /// `TimerCancelled`.
    ///
    /// A decision cycle's events are contiguous. No durable wait settles
    /// mid-cycle. A wait settles only between cycles. So this walks
    /// backward from `events.len()` (the caller passes the pre-redrive
    /// prefix) and swallows a run of these events. It skips an index the
    /// caller already marked transparent, such as the terminal
    /// `WorkflowFailed` or an abandoned pair.
    ///
    /// The walk cannot cross into an earlier cycle unless that cycle's own
    /// wait already resolved. It stops at the first event that is not
    /// already transparent and not one of the kinds above. A settled
    /// completion, such as `ActivityCompleted` or `TimerFired` from an
    /// earlier arm, marks the boundary of that earlier, non-superseded
    /// cycle. Its own markers must stay positionally matchable. If the
    /// walk swallows one of those, it silently breaks `ctx.version()` and
    /// `ctx.patched()` determinism for a cycle the redrive never touched
    /// (issues #687 and #603).
    ///
    /// The function fails closed. An event that is neither already
    /// transparent nor one of the recognized kinds stops the walk at
    /// once. The function does not guess at an event kind it does not
    /// recognize; it leaves that event opaque.
    fn superseded_cycle_tail_indices(
        events: &[WorkflowEvent],
        transparent_events: &HashSet<usize>,
    ) -> Vec<usize> {
        let mut indices = Vec::new();
        let mut idx = events.len();
        while idx > 0 {
            idx -= 1;
            if transparent_events.contains(&idx) {
                continue;
            }
            match events[idx] {
                WorkflowEvent::MarkerRecorded { .. }
                | WorkflowEvent::SideEffectRecorded { .. }
                | WorkflowEvent::ChildWorkflowSpawnedDetached { .. }
                | WorkflowEvent::TimerStarted { .. }
                | WorkflowEvent::TimerCancelled { .. } => {
                    indices.push(idx);
                }
                _ => break,
            }
        }
        indices
    }

    /// Whether this history ends in a terminal failure (issue #952).
    #[must_use]
    pub const fn has_terminal_failure_tail(&self) -> bool {
        self.terminal_failure_tail.is_some()
    }

    /// Whether the cursor has consumed every pre-terminal event of a history
    /// that ends in a terminal failure — the **failing frontier** (issue #952).
    ///
    /// Past this point the recorded history says only "the run failed here", so
    /// a new command, an early return, or a park is not a divergence: there is
    /// nothing left to compare against. Callers use it to suppress strict-replay
    /// divergence reporting; it is never used to skip a *positional* match
    /// against a real recorded event.
    #[must_use]
    pub fn at_terminal_failure_frontier(&self) -> bool {
        self.has_terminal_failure_tail() && !self.is_replaying()
    }

    /// Returns whether the most recent cancellable-timer forward scan STOPPED at
    /// an unconsumed command-bearing event (issue #768, Codex P2 round 12).
    ///
    /// Meaningful only immediately after a [`Self::match_timer_cancel`] /
    /// [`Self::match_timer_or_cancel`] call that returned `NoMatch`: `true`
    /// means the scan was **blocked** (a divergence — the cancel/await/fire is
    /// being emitted before a command history recorded first), `false` means the
    /// scan reached the live frontier (a legitimate new live append).
    #[must_use]
    pub const fn timer_scan_stopped_at_command(&self) -> bool {
        self.timer_scan_stopped_at_command
    }

    /// Extracts the signal name from a signal-or-deadline race timer ID
    /// (issue #476's `__signal_timeout:{seq}:{signal_name}` convention), or
    /// `None` if `timer_id` is not one of these internal race timers.
    fn signal_timeout_race_name(timer_id: &str) -> Option<&str> {
        timer_id
            .strip_prefix("__signal_timeout:")?
            .split_once(':')
            .map(|(_seq, name)| name)
    }

    /// One-time index build (issue #546): marks exactly which `SignalReceived`
    /// indices are reserved for an open signal-or-deadline race (issue #476)
    /// for their name -- see [`Self::race_reserved_signal_events`] for the
    /// full rationale.
    fn build_race_reserved_signal_events(events: &[WorkflowEvent]) -> HashSet<usize> {
        let mut race_reserved_signal_events: HashSet<usize> = HashSet::new();
        // signal_name -> currently-open race timer_ids for that name.
        let mut open_race_timers: HashMap<&str, Vec<&str>> = HashMap::new();

        for (idx, event) in events.iter().enumerate() {
            match event {
                WorkflowEvent::SignalReceived { signal_name, .. } => {
                    if let Some(timers) = open_race_timers.get_mut(signal_name.as_str())
                        && !timers.is_empty()
                    {
                        race_reserved_signal_events.insert(idx);
                        // This signal resolves the OLDEST still-open race for
                        // this name (its `match_signal_or_timer` scan started
                        // first in code-execution order, so it's the first to
                        // consume an available occurrence -- `open_race_timers`
                        // is in TimerStarted encounter order). Remove only
                        // that ONE race: with concurrent same-name races (two
                        // overlapping `receive_signal_timeout` calls), a later
                        // race still needs its OWN future occurrence, so
                        // closing every open race here would let a push
                        // handler steal it (PR #890 review follow-up).
                        timers.remove(0);
                    }
                }
                WorkflowEvent::TimerStarted { timer_id, .. } => {
                    if let Some(name) = Self::signal_timeout_race_name(timer_id.as_str()) {
                        open_race_timers
                            .entry(name)
                            .or_default()
                            .push(timer_id.as_str());
                    }
                }
                WorkflowEvent::TimerFired { timer_id } => {
                    if let Some(name) = Self::signal_timeout_race_name(timer_id.as_str())
                        && let Some(timers) = open_race_timers.get_mut(name)
                    {
                        timers.retain(|id| *id != timer_id.as_str());
                    }
                }
                _ => {}
            }
        }

        race_reserved_signal_events
    }

    /// Returns the IDs of all updates that were admitted but have not completed or failed
    /// up to the specified event index.
    #[must_use]
    pub fn unfinished_update_handlers_at_index(&self, index: usize) -> Vec<UpdateId> {
        let events_slice = &self.events[..index];
        let has_updates = events_slice
            .iter()
            .any(|event| matches!(event, WorkflowEvent::UpdateAdmitted { .. }));
        if !has_updates {
            return Vec::new();
        }

        let mut active = HashSet::new();
        for event in events_slice {
            match event {
                WorkflowEvent::UpdateAdmitted { update_id, .. } => {
                    active.insert(*update_id);
                }
                WorkflowEvent::UpdateCompleted { update_id, .. }
                | WorkflowEvent::UpdateFailed { update_id, .. } => {
                    active.remove(update_id);
                }
                _ => {}
            }
        }
        active.into_iter().collect()
    }

    /// Returns the IDs of all updates that were admitted but have not completed or failed.
    #[must_use]
    pub fn unfinished_update_handlers(&self) -> Vec<UpdateId> {
        self.unfinished_update_handlers_at_index(self.cursor)
    }

    /// Returns the IDs of all updates that were admitted but have not completed or failed in the full history.
    #[must_use]
    pub fn unfinished_update_handlers_at_end(&self) -> Vec<UpdateId> {
        self.unfinished_update_handlers_at_index(self.events.len())
    }

    /// Returns the number of unfinished update handlers up to the specified index.
    #[must_use]
    pub fn unfinished_update_handler_count_at_index(&self, index: usize) -> usize {
        let events_slice = &self.events[..index];
        let has_updates = events_slice
            .iter()
            .any(|event| matches!(event, WorkflowEvent::UpdateAdmitted { .. }));
        if !has_updates {
            return 0;
        }

        let mut active = HashSet::new();
        for event in events_slice {
            match event {
                WorkflowEvent::UpdateAdmitted { update_id, .. } => {
                    active.insert(*update_id);
                }
                WorkflowEvent::UpdateCompleted { update_id, .. }
                | WorkflowEvent::UpdateFailed { update_id, .. } => {
                    active.remove(update_id);
                }
                _ => {}
            }
        }
        active.len()
    }

    /// Returns the number of unfinished update handlers.
    #[must_use]
    pub fn unfinished_update_handler_count(&self) -> usize {
        self.unfinished_update_handler_count_at_index(self.cursor)
    }

    /// Returns the number of unfinished update handlers in the full history.
    #[must_use]
    pub fn unfinished_update_handler_count_at_end(&self) -> usize {
        self.unfinished_update_handler_count_at_index(self.events.len())
    }

    /// Returns `true` if all admitted update handlers have completed or failed up to the specified index.
    #[must_use]
    pub fn all_handlers_finished_at_index(&self, index: usize) -> bool {
        let events_slice = &self.events[..index];
        let has_updates = events_slice
            .iter()
            .any(|event| matches!(event, WorkflowEvent::UpdateAdmitted { .. }));
        if !has_updates {
            return true;
        }

        let mut active = HashSet::new();
        for event in events_slice {
            match event {
                WorkflowEvent::UpdateAdmitted { update_id, .. } => {
                    active.insert(*update_id);
                }
                WorkflowEvent::UpdateCompleted { update_id, .. }
                | WorkflowEvent::UpdateFailed { update_id, .. } => {
                    active.remove(update_id);
                }
                _ => {}
            }
        }
        active.is_empty()
    }

    /// Returns `true` if all admitted update handlers have completed or failed.
    #[must_use]
    pub fn all_handlers_finished(&self) -> bool {
        self.all_handlers_finished_at_index(self.cursor)
    }

    /// Returns `true` if all admitted update handlers have completed or failed in the full history.
    #[must_use]
    pub fn all_handlers_finished_at_end(&self) -> bool {
        self.all_handlers_finished_at_index(self.events.len())
    }

    /// Returns `true` for operator pause/resume lifecycle events (issue #383),
    /// which are transparent no-ops for command-dispatch replay.
    const fn is_pause_lifecycle_event(event: &WorkflowEvent) -> bool {
        matches!(
            event,
            WorkflowEvent::WorkflowExecutionPaused { .. }
                | WorkflowEvent::WorkflowExecutionResumed { .. }
        )
    }

    /// Returns `true` for the DLQ redrive reactivation event (issue #510), which
    /// reopens a `FAILED` run and is a transparent no-op for command-dispatch
    /// replay (together with the `WorkflowFailed` it supersedes).
    const fn is_redrive_lifecycle_event(event: &WorkflowEvent) -> bool {
        matches!(event, WorkflowEvent::WorkflowRedriven { .. })
    }

    /// Returns `true` if the event at `index` has already been consumed out-of-order.
    fn is_consumed(&self, index: usize) -> bool {
        self.consumed_out_of_order_events.contains(&index)
            || self.consumed_signal_events.contains(&index)
            || self.transparent_events.contains(&index)
    }

    fn stash_signal(&mut self, cursor: usize, signal_name: String, payload: Value) {
        self.consumed_signal_events.insert(cursor);
        // Carry the source event index so the late-race exemption (issue
        // #476) can follow the exact losing event into the stash.
        self.pending_signals
            .push_back((signal_name, payload, cursor));
    }

    fn stash_external_signal_request(
        &mut self,
        cursor: usize,
        signal_id: ExternalSignalId,
        target: crate::types::ExternalTarget,
        signal_name: String,
        payload: Value,
        idempotency_key: Option<String>,
    ) {
        self.pending_external_signals.push(StashedExternalSignal {
            signal_id,
            target,
            signal_name,
            payload,
            idempotency_key,
            terminal: None,
        });
        self.consumed_signal_events.insert(cursor);
    }

    fn stash_external_signal_terminal(
        &mut self,
        cursor: usize,
        signal_id: ExternalSignalId,
        terminal: StashedSignalTerminal,
    ) {
        if let Some(pending) = self
            .pending_external_signals
            .iter_mut()
            .find(|pending| pending.signal_id == signal_id)
        {
            pending.terminal = Some(terminal);
        }
        self.consumed_signal_events.insert(cursor);
    }

    fn stash_external_cancel_request(
        &mut self,
        cursor: usize,
        cancel_id: ExternalCancelId,
        target: crate::types::ExternalTarget,
    ) {
        self.pending_external_cancels.push(StashedExternalCancel {
            cancel_id,
            target,
            terminal: None,
        });
        self.consumed_signal_events.insert(cursor);
    }

    fn stash_external_cancel_terminal(
        &mut self,
        cursor: usize,
        cancel_id: ExternalCancelId,
        terminal: StashedCancelTerminal,
    ) {
        if let Some(pending) = self
            .pending_external_cancels
            .iter_mut()
            .find(|pending| pending.cancel_id == cancel_id)
        {
            pending.terminal = Some(terminal);
        }
        self.consumed_signal_events.insert(cursor);
    }

    fn stash_external_await_request(
        &mut self,
        cursor: usize,
        await_id: ExternalAwaitId,
        target: ExecutionId,
    ) {
        self.pending_external_awaits.push(StashedExternalAwait {
            await_id,
            target,
            terminal: None,
        });
        self.consumed_signal_events.insert(cursor);
    }

    fn stash_external_await_terminal(
        &mut self,
        cursor: usize,
        await_id: ExternalAwaitId,
        terminal: StashedAwaitTerminal,
    ) {
        if let Some(pending) = self
            .pending_external_awaits
            .iter_mut()
            .find(|pending| pending.await_id == await_id)
        {
            pending.terminal = Some(terminal);
        }
        self.consumed_signal_events.insert(cursor);
    }

    /// Stashes a "transparent" external-primitive event at `cursor` — a plain
    /// signal, or an `ExternalSignal`/`ExternalCancel`/`ExternalAwait` triplet
    /// member — so the owning primitive's own matcher can find it later, and
    /// marks the event consumed so the calling scan does not treat it as its
    /// own terminal or as an unconsumed ordering point (issue #492, issue
    /// #757).
    ///
    /// This only mutates stash/consumed-event state; it does not touch a
    /// cursor or return a value. Callers keep full control of their own
    /// control flow (loop-continue, an early `return`, or a `TimerScanStep`)
    /// and call this only for the subset of the 10 variants below that their
    /// own scan treats as transparent — e.g. `match_external_signal` excludes
    /// `ExternalSignalRequested`/`Delivered`/`Failed`, since those are its own
    /// subject rather than a transparent sibling event there. Panics (via the
    /// final `unreachable!`) if called for the event at `cursor` when that
    /// event is not one of the 10 handled variants — every call site's own
    /// match arm pattern is what guarantees that never happens.
    fn stash_transparent_external_event(&mut self, cursor: usize) {
        match &self.events[cursor] {
            WorkflowEvent::SignalReceived {
                signal_name,
                payload,
            } => {
                let signal_name = signal_name.clone();
                let payload = payload.clone();
                self.stash_signal(cursor, signal_name, payload);
            }
            WorkflowEvent::ExternalSignalRequested {
                signal_id,
                target,
                signal_name,
                payload,
                idempotency_key,
            } => {
                self.stash_external_signal_request(
                    cursor,
                    *signal_id,
                    target.clone(),
                    signal_name.clone(),
                    payload.clone(),
                    idempotency_key.clone(),
                );
            }
            WorkflowEvent::ExternalSignalDelivered { signal_id } => {
                self.stash_external_signal_terminal(
                    cursor,
                    *signal_id,
                    StashedSignalTerminal::Delivered,
                );
            }
            WorkflowEvent::ExternalSignalFailed {
                signal_id,
                reason_code,
            } => {
                self.stash_external_signal_terminal(
                    cursor,
                    *signal_id,
                    StashedSignalTerminal::Failed(reason_code.clone()),
                );
            }
            WorkflowEvent::ExternalCancelRequested { cancel_id, target } => {
                self.stash_external_cancel_request(cursor, *cancel_id, target.clone());
            }
            WorkflowEvent::ExternalCancelDelivered { cancel_id } => {
                self.stash_external_cancel_terminal(
                    cursor,
                    *cancel_id,
                    StashedCancelTerminal::Delivered,
                );
            }
            WorkflowEvent::ExternalCancelFailed {
                cancel_id,
                reason_code,
            } => {
                self.stash_external_cancel_terminal(
                    cursor,
                    *cancel_id,
                    StashedCancelTerminal::Failed(reason_code.clone()),
                );
            }
            WorkflowEvent::ExternalAwaitRequested { await_id, target } => {
                self.stash_external_await_request(cursor, *await_id, *target);
            }
            WorkflowEvent::ExternalAwaitResolved { await_id, output } => {
                self.stash_external_await_terminal(
                    cursor,
                    *await_id,
                    StashedAwaitTerminal::Resolved(output.clone()),
                );
            }
            WorkflowEvent::ExternalAwaitFailed {
                await_id,
                reason_code,
                message,
                error_type,
                details,
                non_retryable,
            } => {
                self.stash_external_await_terminal(
                    cursor,
                    *await_id,
                    StashedAwaitTerminal::Failed {
                        reason_code: reason_code.clone(),
                        message: message.clone(),
                        error_type: error_type.clone(),
                        details: details.clone(),
                        non_retryable: *non_retryable,
                    },
                );
            }
            other => unreachable!(
                "stash_transparent_external_event called for a non-transparent event: {other:?}"
            ),
        }
    }

    /// Returns `true` for events transparent to main workflow command replay.
    ///
    /// Update events are transparent to the main workflow replay sequence —
    /// they are skipped during activity/timer/signal/child-workflow matching
    /// and are consumed by the `match_update` / `drain_admitted_updates` APIs.
    /// `WorkflowResetFork` is informational and likewise has no workflow
    /// command counterpart.
    const fn is_update_event(event: &WorkflowEvent) -> bool {
        matches!(
            event,
            WorkflowEvent::UpdateAdmitted { .. }
                | WorkflowEvent::UpdateCompleted { .. }
                | WorkflowEvent::UpdateFailed { .. }
                | WorkflowEvent::WorkflowResetFork { .. }
        )
    }

    /// Format the `actual` field for a [`HistoryMatch::Diverged`] result.
    ///
    /// When the unexpected event is a `MarkerRecorded` its name is included so
    /// the replayer's `classify_kind` can recognise `"MarkerRecorded(version:…)"`
    /// and return [`crate::testing::NonDeterminismKind::VersionMarkerMismatch`]
    /// instead of the generic command-level mismatch kind.
    fn actual_event_name(event: &WorkflowEvent) -> String {
        match event {
            WorkflowEvent::MarkerRecorded { name, .. } => format!("MarkerRecorded({name})"),
            WorkflowEvent::SideEffectRecorded { kind, name, .. } => name.as_ref().map_or_else(
                || format!("SideEffectRecorded({})", kind.as_str()),
                |n| format!("SideEffectRecorded({}:{n})", kind.as_str()),
            ),
            other => other.type_name().to_string(),
        }
    }

    /// Prepares for matching by advancing past consumed events and draining early signals.
    /// Returns `true` if there are still events to replay.
    #[allow(clippy::too_many_lines)]
    fn scan_activity_terminal(
        &mut self,
        activity_id: ActivityExecId,
        mut scan_cursor: usize,
    ) -> HistoryMatch {
        let mut first_interleaved_command = None;

        // Scan forward for Completed or Failed with matching activity_id,
        // skipping Started, Heartbeat, and other intermediate events.
        while scan_cursor < self.events.len() {
            if self.is_consumed(scan_cursor) {
                scan_cursor += 1;
                continue;
            }

            match &self.events[scan_cursor] {
                WorkflowEvent::ActivityCompleted {
                    activity_id: id, ..
                } if *id == activity_id => {
                    // Take rather than clone the recorded output. This is the
                    // only production read of an already-matched
                    // `ActivityCompleted`'s `output` anywhere in this matcher
                    // -- `settle_terminal` below re-reads only `activity_id`
                    // from this same event, never `output` -- so once matched,
                    // the recorded payload is never observed again and cloning
                    // it (a deep clone for an arbitrary-shape `Value`, e.g. a
                    // `BTreeMap`-backed object) is pure waste. Re-indexed
                    // mutably here (rather than converting the whole match's
                    // scrutinee, which would force every other arm below to
                    // separately satisfy the borrow checker for no benefit):
                    // the shared borrow above ends at the guard check, since
                    // `id` is the only field bound from it. See
                    // `docs/performance-replay.md`.
                    let WorkflowEvent::ActivityCompleted { output, .. } =
                        &mut self.events[scan_cursor]
                    else {
                        unreachable!("just matched ActivityCompleted above")
                    };
                    let result = HistoryMatch::Matched {
                        output: std::mem::take(output),
                    };
                    return self.settle_terminal(scan_cursor, first_interleaved_command, result);
                }
                WorkflowEvent::ActivityFailed {
                    activity_id: id,
                    error,
                    attempt,
                    error_type,
                    details,
                    non_retryable,
                    ..
                } if *id == activity_id => {
                    let result = HistoryMatch::Failed {
                        error: error.clone(),
                        attempt: *attempt,
                        error_type: error_type.clone(),
                        details: details.clone(),
                        non_retryable: *non_retryable,
                    };
                    return self.settle_terminal(scan_cursor, first_interleaved_command, result);
                }
                WorkflowEvent::ActivityTimedOut {
                    activity_id: id,
                    timeout_type,
                } if *id == activity_id => {
                    let result = HistoryMatch::TimedOut {
                        timeout_type: timeout_type.clone(),
                    };
                    return self.settle_terminal(scan_cursor, first_interleaved_command, result);
                }
                // Skip heartbeats and started events for this activity.
                WorkflowEvent::ActivityHeartbeat {
                    activity_id: id, ..
                }
                | WorkflowEvent::ActivityStarted {
                    activity_id: id, ..
                } if *id == activity_id => {
                    scan_cursor += 1;
                }
                // Other activities may be scheduled and complete while this
                // activity is still running. Keep their scheduled event as the
                // next replay cursor, but scan past it to find this activity's
                // terminal event.
                WorkflowEvent::ActivityScheduled {
                    activity_id: id, ..
                } if *id != activity_id => {
                    first_interleaved_command.get_or_insert(scan_cursor);
                    scan_cursor += 1;
                }
                WorkflowEvent::ActivityCompleted {
                    activity_id: id, ..
                }
                | WorkflowEvent::ActivityFailed {
                    activity_id: id, ..
                }
                | WorkflowEvent::ActivityTimedOut {
                    activity_id: id, ..
                }
                | WorkflowEvent::ActivityHeartbeat {
                    activity_id: id, ..
                }
                | WorkflowEvent::ActivityStarted {
                    activity_id: id, ..
                } if *id != activity_id => {
                    scan_cursor += 1;
                }
                // Child workflows can run concurrently with activities.
                // Preserve replay by scanning past interleaved child starts.
                WorkflowEvent::ChildWorkflowStarted { .. }
                | WorkflowEvent::ChildWorkflowSpawnedDetached { .. } => {
                    first_interleaved_command.get_or_insert(scan_cursor);
                    scan_cursor += 1;
                }
                // Signals, and ExternalSignal/ExternalCancel/ExternalAwait event
                // triplets, can be interleaved with an in-flight activity (e.g.
                // tokio::join!(signal, activity)). Stash them so their own
                // matchers can find them later (issue #492, issue #757).
                WorkflowEvent::SignalReceived { .. }
                | WorkflowEvent::ExternalSignalRequested { .. }
                | WorkflowEvent::ExternalSignalDelivered { .. }
                | WorkflowEvent::ExternalSignalFailed { .. }
                | WorkflowEvent::ExternalCancelRequested { .. }
                | WorkflowEvent::ExternalCancelDelivered { .. }
                | WorkflowEvent::ExternalCancelFailed { .. }
                | WorkflowEvent::ExternalAwaitRequested { .. }
                | WorkflowEvent::ExternalAwaitResolved { .. }
                | WorkflowEvent::ExternalAwaitFailed { .. } => {
                    self.stash_transparent_external_event(scan_cursor);
                    scan_cursor += 1;
                }
                // Update events are transparent to the activity scan.
                ev if Self::is_update_event(ev) => {
                    scan_cursor += 1;
                }
                // Fan-out markers (and any other MarkerRecorded) can be
                // interleaved when a fan-out runs concurrently with this
                // activity (e.g. via tokio::join!). Track as an interleaved
                // command so the cursor returns to it after matching the
                // terminal event. Deterministic side-effect captures (issue
                // #384) are cursor-ordered like markers and treated the same. A
                // cancellable-timer arm/cancel (issue #768, e.g. a concurrent
                // `start_timer()`/`handle.cancel()`/`reset()` — a reset emits
                // both) is likewise an interleaved command: the rewind keeps it
                // at the cursor for its own claimer (`match_timer_arm` /
                // `match_timer_cancel`) to re-scan. `TimerStarted` is included
                // here for symmetry with `TimerCancelled` so a `reset`'s
                // `[TimerCancelled, TimerStarted]` pair doesn't break the scan
                // mid-way (the sibling scan loops already tolerate both).
                WorkflowEvent::MarkerRecorded { .. }
                | WorkflowEvent::SideEffectRecorded { .. }
                | WorkflowEvent::TimerStarted { .. }
                | WorkflowEvent::TimerCancelled { .. }
                | WorkflowEvent::LocalActivityScheduled { .. } => {
                    first_interleaved_command.get_or_insert(scan_cursor);
                    scan_cursor += 1;
                }
                // Progress/terminal events of concurrent siblings from a mixed
                // suspension batch — a sibling timer's fire (an expired timer
                // firing before this activity's terminal, e.g.
                // `tokio::join!(activity, ctx.timer(...))`), a child workflow's
                // terminal, or a local activity's terminal — are transparent to
                // this activity's terminal scan. Cross them NON-CONSUMINGLY,
                // leaving each for its own matcher to claim after the rewind
                // (issue #1071 manifestation #2). Keep this sibling-terminal set
                // in sync with `match_signal_or_timer`'s and
                // `match_timer_strict`'s (issue #1071).
                WorkflowEvent::TimerFired { .. }
                | WorkflowEvent::ChildWorkflowCompleted { .. }
                | WorkflowEvent::ChildWorkflowFailed { .. }
                | WorkflowEvent::LocalActivityCompleted { .. }
                | WorkflowEvent::LocalActivityFailed { .. } => {
                    scan_cursor += 1;
                }
                // Any other event type is unexpected mid-activity
                _ => break,
            }
        }

        // We found the Scheduled event but no terminal event — treat as
        // incomplete history (the activity was scheduled but never finished).
        if let Some(command_cursor) = first_interleaved_command {
            self.cursor = command_cursor;
            self.advance_to_next_unconsumed_event();
        }
        HistoryMatch::ActivityInProgress { activity_id }
    }

    #[allow(clippy::too_many_lines)]
    fn scan_local_activity_terminal(
        &mut self,
        activity_id: ActivityExecId,
        mut scan_cursor: usize,
    ) -> HistoryMatch {
        let mut failed_attempts: u32 = 0;
        let mut last_error: Option<String> = None;
        let mut first_interleaved_command = None;

        while scan_cursor < self.events.len() {
            if self.is_consumed(scan_cursor) {
                scan_cursor += 1;
                continue;
            }

            match &self.events[scan_cursor] {
                WorkflowEvent::LocalActivityCompleted {
                    activity_id: id,
                    output,
                } if *id == activity_id => {
                    let result = HistoryMatch::Matched {
                        output: output.clone(),
                    };
                    return self.settle_terminal(scan_cursor, first_interleaved_command, result);
                }
                // Terminal: all retries exhausted. This event is always
                // authoritative regardless of the current retry policy.
                WorkflowEvent::LocalActivityExhausted {
                    activity_id: id,
                    error,
                    attempt,
                } if *id == activity_id => {
                    // Local activities carry no typed failure payload (and may
                    // not declare a circuit breaker), so the error-type is the
                    // legacy "Error" fallback with no structured details.
                    let result = HistoryMatch::Failed {
                        error: error.clone(),
                        attempt: *attempt,
                        error_type: "Error".to_string(),
                        details: None,
                        non_retryable: false,
                    };
                    return self.settle_terminal(scan_cursor, first_interleaved_command, result);
                }
                WorkflowEvent::LocalActivityFailed {
                    activity_id: id,
                    error,
                    attempt: _,
                } if *id == activity_id => {
                    failed_attempts += 1;
                    last_error = Some(error.clone());
                    scan_cursor += 1;
                }
                WorkflowEvent::ChildWorkflowSpawnedDetached { .. } => {
                    first_interleaved_command.get_or_insert(scan_cursor);
                    scan_cursor += 1;
                }
                // Signals can be ingested while a local activity is retrying.
                // ExternalSignal events can be interleaved before the local
                // activity's terminal event when a crash recovery case writes
                // signal events first (the RunLocalActivity + SignalExternalWorkflow
                // mixed batch). ExternalCancel/ExternalAwait triplets are
                // likewise transparent here (issue #492, issue #757). Stash
                // them all so their own matchers can find them later.
                WorkflowEvent::SignalReceived { .. }
                | WorkflowEvent::ExternalSignalRequested { .. }
                | WorkflowEvent::ExternalSignalDelivered { .. }
                | WorkflowEvent::ExternalSignalFailed { .. }
                | WorkflowEvent::ExternalCancelRequested { .. }
                | WorkflowEvent::ExternalCancelDelivered { .. }
                | WorkflowEvent::ExternalCancelFailed { .. }
                | WorkflowEvent::ExternalAwaitRequested { .. }
                | WorkflowEvent::ExternalAwaitResolved { .. }
                | WorkflowEvent::ExternalAwaitFailed { .. } => {
                    self.stash_transparent_external_event(scan_cursor);
                    scan_cursor += 1;
                }
                ev if Self::is_update_event(ev) => {
                    scan_cursor += 1;
                }
                // Fan-out markers and deterministic side-effect captures (issue
                // #384) can be interleaved during concurrent execution. A
                // cancellable-timer arm/cancel (issue #768) — e.g. a concurrent
                // `start_timer()`/`handle.cancel()`/`reset()`, where a reset emits
                // both `[TimerCancelled, TimerStarted]` — is likewise interleavable
                // here (mirrors `scan_activity_terminal`): rewind without consuming
                // so `match_timer_arm` / `match_timer_cancel` claim each later.
                WorkflowEvent::MarkerRecorded { .. }
                | WorkflowEvent::SideEffectRecorded { .. }
                | WorkflowEvent::TimerStarted { .. }
                | WorkflowEvent::TimerCancelled { .. } => {
                    first_interleaved_command.get_or_insert(scan_cursor);
                    scan_cursor += 1;
                }
                _ => break,
            }
        }

        // No LocalActivityCompleted or LocalActivityExhausted found. The worker
        // either crashed before the first attempt or between retry attempts.
        // Return InProgress so the worker can resume from the right attempt.
        if let Some(command_cursor) = first_interleaved_command {
            self.cursor = command_cursor;
            self.advance_to_next_unconsumed_event();
        } else if failed_attempts > 0 {
            // Advance the cursor past the recorded failure events so the next
            // match picks up from the right position on the next worker call.
            self.cursor = scan_cursor;
            self.advance_to_next_unconsumed_event();
        }
        HistoryMatch::LocalActivityInProgress {
            activity_id,
            failed_attempts,
            last_error,
        }
    }

    /// Advances the cursor past already-consumed events, drains any signals
    /// (and update/external-signal/external-cancel events) sitting at the
    /// current cursor into their pending stashes, and resolves any external
    /// signal/cancel terminals visible further ahead. Returns whether the
    /// matcher is still replaying afterward.
    ///
    /// `pub(crate)` so [`WorkflowContext`](crate::context::WorkflowContext)'s
    /// signal-handler pump (issue #546) can trigger the same cursor-bound
    /// sweep `Self::claim_pending_signal` relies on, without duplicating this
    /// logic.
    pub(crate) fn prepare_match(&mut self) -> bool {
        self.advance_to_next_unconsumed_event();
        self.drain_early_signals();
        self.scan_ahead_for_external_signal_terminals();
        self.is_replaying()
    }

    /// Returns `true` if the cursor is still within the recorded history
    /// (cursor-based check only, does not inspect the early-drain stash).
    ///
    /// Used internally by `prepare_match` and other cursor-advancing methods.
    /// For the user-visible `ctx.is_replaying()` check, use
    /// [`has_buffered_history`](Self::has_buffered_history) instead.
    #[must_use]
    pub fn is_replaying(&self) -> bool {
        let mut cursor = self.cursor;
        while self.is_consumed(cursor) {
            cursor += 1;
        }
        cursor < self.events.len()
    }

    /// Returns `true` if there is any un-replayed history — either cursor-based
    /// events or signals/external-signals buffered in the early-drain stash.
    ///
    /// This answers the question "is there any recorded history not yet consumed?"
    /// and is used internally (e.g., by `history_has_unconsumed_events`). It is
    /// intentionally more conservative than the user-visible `ctx.is_replaying()`
    /// check, which uses the cursor-based [`is_replaying`](Self::is_replaying).
    ///
    /// The distinction matters for metrics suppression: `pending_signals` in the stash
    /// were drained from the event stream ahead of the cursor during
    /// `drain_early_signals`, but the workflow's current code position is at the
    /// live frontier. Using this method for `ctx.is_replaying()` would incorrectly
    /// suppress metrics emitted between an activity completion and a
    /// `wait_for_signal` call.
    #[must_use]
    pub fn has_buffered_history(&self) -> bool {
        self.is_replaying()
            || !self.pending_signals.is_empty()
            || !self.pending_external_signals.is_empty()
            || !self.pending_external_cancels.is_empty()
            || !self.pending_external_awaits.is_empty()
    }

    /// Number of events loaded into this replay matcher.
    #[must_use]
    pub fn event_count(&self) -> u64 {
        u64::try_from(self.events.len()).unwrap_or(u64::MAX)
    }

    /// Returns `true` if there are unconsumed events that are not terminal
    /// lifecycle events (`WorkflowCompleted`, `WorkflowFailed`,
    /// `WorkflowCancelled`, `WorkflowContinuedAsNew`), or if there are buffered
    /// signals that were never delivered via `wait_for_signal`.
    ///
    /// Used by [`crate::context::WorkflowContext::history_has_unconsumed_events`] to avoid
    /// false non-determinism reports when replaying full histories that include
    /// a terminal event appended after workflow completion.
    ///
    /// The pending-signal check is necessary because early `SignalReceived`
    /// events are moved into `consumed_signal_events` when buffered, so they
    /// are invisible to the cursor-based check.  If new code removes a
    /// `wait_for_signal` call, the buffered signal would be silently ignored
    /// without this additional check.
    #[must_use]
    pub fn has_non_lifecycle_unconsumed(&self) -> bool {
        // The exact SignalReceived events that lost a signal-or-deadline race
        // (issue #476) are excused: the timeout branch may intentionally never
        // consume them. Any other unconsumed signal still flags — including a
        // second same-name delivery when the loser was consumed by a later
        // wait (the exemption travels with the losing event, not the name).
        let mut cursor = self.cursor;
        while cursor < self.events.len() {
            if !self.is_consumed(cursor)
                && !self.events[cursor].is_terminal_lifecycle()
                && !Self::is_update_event(&self.events[cursor])
                && !self.late_race_signal_events.contains(&cursor)
            {
                return true;
            }
            cursor += 1;
        }
        // Signals buffered early (via drain_early_signals) that were never
        // consumed by wait_for_signal represent unconsumed history, except
        // for the exact events excused by a lost race.
        if self
            .pending_signals
            .iter()
            .any(|(_, _, idx)| !self.late_race_signal_events.contains(idx))
        {
            return true;
        }
        // External signals drained early that were never consumed by
        // signal_external_workflow represent unconsumed history.
        if !self.pending_external_signals.is_empty() {
            return true;
        }
        // External cancels drained early that were never consumed by
        // request_cancel_external_workflow represent unconsumed history.
        if !self.pending_external_cancels.is_empty() {
            return true;
        }
        // External awaits drained early that were never consumed by
        // await_external_workflow represent unconsumed history (issue #757).
        !self.pending_external_awaits.is_empty()
    }

    /// End-of-drive count of genuinely-unconsumed `SignalReceived` events,
    /// keyed by signal name (issue #684).
    ///
    /// Mirrors [`Self::has_non_lifecycle_unconsumed`]'s two sources — the
    /// cursor scan for still-in-history `SignalReceived` events, plus the
    /// `pending_signals` buffer for signals drained early by a `prepare_match`
    /// sweep but never consumed by a `wait_for_signal`/push handler — but
    /// restricted to `SignalReceived` and grouped by name. Signals excused by
    /// a lost signal-or-deadline race (issue #476, `late_race_signal_events`)
    /// are excluded, exactly as they are from the boolean check. A signal
    /// consumed by a wait or claimed by a push handler is in
    /// `consumed_signal_events` and removed from `pending_signals`, so it never
    /// appears in either source.
    ///
    /// Read-only (`&self`): the caller is expected to have already driven the
    /// matcher to the terminal frontier.
    #[must_use]
    pub fn unconsumed_signals_by_name(&self) -> std::collections::BTreeMap<String, u64> {
        let mut counts: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();

        // Source 1: SignalReceived events still at/after the cursor that were
        // never consumed and are not excused by a lost race.
        let mut cursor = self.cursor;
        while cursor < self.events.len() {
            if let WorkflowEvent::SignalReceived { signal_name, .. } = &self.events[cursor]
                && !self.is_consumed(cursor)
                && !self.late_race_signal_events.contains(&cursor)
            {
                *counts.entry(signal_name.clone()).or_insert(0) += 1;
            }
            cursor += 1;
        }

        // Source 2: signals drained early into pending_signals (index < cursor)
        // that were never consumed, minus the exact lost-race exemptions.
        for (signal_name, _, idx) in &self.pending_signals {
            if !self.late_race_signal_events.contains(idx) {
                *counts.entry(signal_name.clone()).or_insert(0) += 1;
            }
        }

        counts
    }

    /// Current cursor position in the event list.
    #[must_use]
    pub const fn position(&self) -> usize {
        self.cursor
    }

    /// Count unconsumed `ActivityScheduled` events at or after the current
    /// cursor, stopping early once `cap` is reached.
    ///
    /// **Read-only**: does *not* mutate the cursor or any consumed-set — it is
    /// safe to call as a pure inspection before deciding how to dispatch.
    ///
    /// Used by the windowed activity fan-out (issue #750) to size the
    /// already-scheduled prefix `[0..s)` that must be *resumed* (as one
    /// homogeneous batch) before any fresh remainder is dispatched. Called
    /// immediately after the fan-out's count marker has been consumed, so the
    /// cursor sits on the fan-out's first `ActivityScheduled` and the next `s`
    /// unconsumed `ActivityScheduled` events (a contiguous prefix, since the
    /// fan-out dispatches strictly in input order) are exactly its own
    /// already-dispatched slots. `cap` is the fan-out's own slot count, which
    /// both bounds the scan and prevents ever counting a *later*, unrelated
    /// activity that a fully-completed fan-out's continuation may have already
    /// scheduled into history.
    #[must_use]
    pub(crate) fn count_pending_scheduled_activities(&self, cap: usize) -> usize {
        let mut n = 0;
        let mut cursor = self.cursor;
        while cursor < self.events.len() && n < cap {
            if !self.is_consumed(cursor)
                && matches!(self.events[cursor], WorkflowEvent::ActivityScheduled { .. })
            {
                n += 1;
            }
            cursor += 1;
        }
        n
    }

    /// Total number of events in history.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.events.len()
    }

    /// Returns true if history has no events.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Advance the cursor by one, skipping the current event.
    ///
    /// Use this to skip lifecycle events like `WorkflowStarted` that
    /// don't correspond to a workflow command.
    pub fn advance(&mut self) {
        if self.cursor < self.events.len() {
            self.cursor += 1;
            self.advance_to_next_unconsumed_event();
        }
    }

    fn advance_to_next_unconsumed_event(&mut self) {
        while self.cursor < self.events.len() && self.is_consumed(self.cursor) {
            self.cursor += 1;
        }
    }

    /// Drain any `SignalReceived` and update events at the current cursor.
    ///
    /// Signals can be ingested into history at any point (when the worker
    /// picks up a task), so they may appear before an `ActivityScheduled`,
    /// `TimerStarted`, or `ChildWorkflowStarted` event even though the
    /// workflow code only calls `wait_for_signal` later.  This helper
    /// buffers those early signals so the normal matcher methods do not
    /// mis-report them as non-determinism.
    ///
    /// Update events (`UpdateAdmitted`, `UpdateCompleted`, `UpdateFailed`) are
    /// also transparent to the main workflow replay sequence and are consumed
    /// here so they don't cause spurious `Diverged` results in `prepare_match`.
    #[allow(clippy::too_many_lines)]
    fn drain_early_signals(&mut self) {
        while self.cursor < self.events.len() {
            match &self.events[self.cursor] {
                // Drain signals, and ExternalSignal/ExternalCancel/ExternalAwait
                // event pairs (issue #492, issue #757), so they can be matched
                // by their own primitives regardless of where they fall in
                // history relative to ActivityScheduled / TimerStarted events
                // (mixed batches).
                WorkflowEvent::SignalReceived { .. }
                | WorkflowEvent::ExternalSignalRequested { .. }
                | WorkflowEvent::ExternalSignalDelivered { .. }
                | WorkflowEvent::ExternalSignalFailed { .. }
                | WorkflowEvent::ExternalCancelRequested { .. }
                | WorkflowEvent::ExternalCancelDelivered { .. }
                | WorkflowEvent::ExternalCancelFailed { .. }
                | WorkflowEvent::ExternalAwaitRequested { .. }
                | WorkflowEvent::ExternalAwaitResolved { .. }
                | WorkflowEvent::ExternalAwaitFailed { .. } => {
                    self.stash_transparent_external_event(self.cursor);
                    self.cursor += 1;
                    self.advance_to_next_unconsumed_event();
                }
                ev if Self::is_update_event(ev) => {
                    // Consume the update event so it doesn't block the cursor.
                    self.consumed_signal_events.insert(self.cursor);
                    self.cursor += 1;
                    self.advance_to_next_unconsumed_event();
                }
                _ => break,
            }
        }
    }

    /// Scan remaining unconsumed history events to resolve and consume
    /// terminal events (delivered or failed) for any in-progress stashed signals.
    /// This ensures we resolve signals that are finished but blocked by subsequent
    /// un-fired timers or un-executed activities (mixed batch).
    fn scan_ahead_for_external_signal_terminals(&mut self) {
        if self.pending_external_signals.is_empty() && self.pending_external_cancels.is_empty() {
            return;
        }

        let mut scan_cursor = self.cursor;
        while scan_cursor < self.events.len() {
            if self.is_consumed(scan_cursor) {
                scan_cursor += 1;
                continue;
            }

            match &self.events[scan_cursor] {
                WorkflowEvent::ExternalSignalDelivered { signal_id } => {
                    let id = *signal_id;
                    if let Some(p) = self
                        .pending_external_signals
                        .iter_mut()
                        .find(|p| p.signal_id == id)
                    {
                        p.terminal = Some(StashedSignalTerminal::Delivered);
                        self.consumed_signal_events.insert(scan_cursor);
                    }
                }
                WorkflowEvent::ExternalSignalFailed {
                    signal_id,
                    reason_code,
                } => {
                    let id = *signal_id;
                    let code = reason_code.clone();
                    if let Some(p) = self
                        .pending_external_signals
                        .iter_mut()
                        .find(|p| p.signal_id == id)
                    {
                        p.terminal = Some(StashedSignalTerminal::Failed(code));
                        self.consumed_signal_events.insert(scan_cursor);
                    }
                }
                WorkflowEvent::ExternalCancelDelivered { cancel_id } => {
                    let id = *cancel_id;
                    if let Some(p) = self
                        .pending_external_cancels
                        .iter_mut()
                        .find(|p| p.cancel_id == id)
                    {
                        p.terminal = Some(StashedCancelTerminal::Delivered);
                        self.consumed_signal_events.insert(scan_cursor);
                    }
                }
                WorkflowEvent::ExternalCancelFailed {
                    cancel_id,
                    reason_code,
                } => {
                    let id = *cancel_id;
                    let code = reason_code.clone();
                    if let Some(p) = self
                        .pending_external_cancels
                        .iter_mut()
                        .find(|p| p.cancel_id == id)
                    {
                        p.terminal = Some(StashedCancelTerminal::Failed(code));
                        self.consumed_signal_events.insert(scan_cursor);
                    }
                }
                _ => {}
            }
            scan_cursor += 1;
        }
    }

    /// Advance the cursor after a terminal event, respecting any interleaved
    /// command that needs to remain at the cursor for later replay.
    ///
    /// If `first_interleaved_command` is set, the terminal event is marked
    /// consumed and the cursor is rewound so the matching command API can pick
    /// it up. Otherwise the cursor advances past the terminal event normally.
    fn settle_terminal(
        &mut self,
        terminal_cursor: usize,
        first_interleaved_command: Option<usize>,
        result: HistoryMatch,
    ) -> HistoryMatch {
        if let Some(command_cursor) = first_interleaved_command {
            self.consumed_out_of_order_events.insert(terminal_cursor);
            // Also mark any ActivityStarted/Heartbeat events for the settled
            // activity that appear between the interleaved command and the
            // terminal. Without this, after the cursor rewinds to the
            // interleaved command and the marker/fan-out branch is replayed,
            // those progress events remain unconsumed and the next workflow
            // command match diverges against them.
            let activity_id = match &self.events[terminal_cursor] {
                WorkflowEvent::ActivityCompleted { activity_id, .. }
                | WorkflowEvent::ActivityFailed { activity_id, .. }
                | WorkflowEvent::ActivityTimedOut { activity_id, .. } => Some(*activity_id),
                _ => None,
            };
            if let Some(aid) = activity_id {
                for idx in command_cursor..terminal_cursor {
                    match &self.events[idx] {
                        WorkflowEvent::ActivityStarted {
                            activity_id: id, ..
                        }
                        | WorkflowEvent::ActivityHeartbeat {
                            activity_id: id, ..
                        } if *id == aid => {
                            self.consumed_out_of_order_events.insert(idx);
                        }
                        _ => {}
                    }
                }
            }
            self.cursor = command_cursor;
        } else {
            self.cursor = terminal_cursor + 1;
        }
        self.advance_to_next_unconsumed_event();
        result
    }

    /// Match an `execute_activity` command against history.
    ///
    /// Expects `ActivityScheduled { name }` at the current cursor position,
    /// then scans forward for `ActivityCompleted` or `ActivityFailed` with
    /// the same `activity_id`, skipping heartbeat and started events.
    ///
    /// Returns:
    /// - [`HistoryMatch::Matched`] if a completed result is found
    /// - [`HistoryMatch::Failed`] if a failure is found
    /// - [`HistoryMatch::TimedOut`] if a timeout is found
    /// - [`HistoryMatch::NoMatch`] if past end of history
    /// - [`HistoryMatch::Diverged`] if the event at cursor is not the expected activity
    #[allow(clippy::too_many_lines)]
    pub fn match_activity(&mut self, activity_name: &str) -> HistoryMatch {
        if !self.prepare_match() {
            return HistoryMatch::NoMatch;
        }

        // Expect ActivityScheduled at cursor
        let WorkflowEvent::ActivityScheduled {
            activity_id,
            name: recorded_name,
            ..
        } = &self.events[self.cursor]
        else {
            return HistoryMatch::Diverged {
                expected: format!("ActivityScheduled({activity_name})"),
                actual: Self::actual_event_name(&self.events[self.cursor]),

                event_index: i32::try_from(self.cursor).ok(),
            };
        };
        let activity_id = *activity_id;

        // Verify activity name matches
        if recorded_name != activity_name {
            return HistoryMatch::Diverged {
                expected: format!("ActivityScheduled({activity_name})"),
                actual: format!("ActivityScheduled({recorded_name})"),

                event_index: i32::try_from(self.cursor).ok(),
            };
        }

        // Advance past the Scheduled event
        self.cursor += 1;
        self.scan_activity_terminal(activity_id, self.cursor)
    }

    /// Like [`match_activity`](Self::match_activity) but also verifies the input payload.
    ///
    /// Used by the [`WorkflowReplayer`](crate::testing::WorkflowReplayer) to detect
    /// non-determinism caused by changing an activity's input arguments across deployments.
    #[allow(clippy::too_many_lines)]
    pub fn match_activity_strict(&mut self, activity_name: &str, input: &Value) -> HistoryMatch {
        if !self.prepare_match() {
            return HistoryMatch::NoMatch;
        }

        // Extract fields as owned values so the immutable borrow ends before cursor mutation.
        let result = match &self.events[self.cursor] {
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: recorded_name,
                input: recorded_input,
                ..
            } => {
                if recorded_name != activity_name {
                    return HistoryMatch::Diverged {
                        expected: format!("ActivityScheduled({activity_name})"),
                        actual: format!("ActivityScheduled({recorded_name})"),

                        event_index: i32::try_from(self.cursor).ok(),
                    };
                }
                if recorded_input != input {
                    return HistoryMatch::Diverged {
                        expected: format!(
                            "ActivityScheduled({activity_name}, input={recorded_input})"
                        ),
                        actual: format!("ActivityScheduled({activity_name}, input={input})"),

                        event_index: i32::try_from(self.cursor).ok(),
                    };
                }
                Ok(*activity_id)
            }
            other => Err(HistoryMatch::Diverged {
                expected: format!("ActivityScheduled({activity_name})"),
                actual: Self::actual_event_name(other),

                event_index: i32::try_from(self.cursor).ok(),
            }),
        };
        let activity_id = match result {
            Ok(id) => id,
            Err(diverged) => return diverged,
        };

        self.cursor += 1;
        self.scan_activity_terminal(activity_id, self.cursor)
    }

    /// Match an `execute_activity_external` command against history.
    ///
    /// Expects `ActivityAwaitingExternal { name }` at the current cursor, then
    /// scans forward for a terminal event (`ActivityCompletedExternally`,
    /// `ActivityFailedExternally`, or `ActivityTimedOut`) with the same
    /// `activity_id`. `ActivityExternalDeadlineExtended` events are skipped.
    ///
    /// Returns:
    /// - [`HistoryMatch::Matched`] when the external system completed the activity
    /// - [`HistoryMatch::Failed`] when the external system failed the activity
    /// - [`HistoryMatch::TimedOut`] when the schedule-to-close clock expired
    /// - [`HistoryMatch::AwaitingExternalCompletion`] when scheduled but no terminal yet
    /// - [`HistoryMatch::NoMatch`] when past end of history (first-time scheduling)
    /// - [`HistoryMatch::Diverged`] when history has a different event at this position
    #[allow(clippy::too_many_lines)]
    pub fn match_external_activity(&mut self, activity_name: &str) -> HistoryMatch {
        if !self.prepare_match() {
            return HistoryMatch::NoMatch;
        }

        let WorkflowEvent::ActivityAwaitingExternal {
            activity_id,
            token,
            name: recorded_name,
            ..
        } = &self.events[self.cursor]
        else {
            return HistoryMatch::Diverged {
                expected: format!("ActivityAwaitingExternal({activity_name})"),
                actual: Self::actual_event_name(&self.events[self.cursor]),

                event_index: i32::try_from(self.cursor).ok(),
            };
        };

        if recorded_name != activity_name {
            return HistoryMatch::Diverged {
                expected: format!("ActivityAwaitingExternal({activity_name})"),
                actual: format!("ActivityAwaitingExternal({recorded_name})"),

                event_index: i32::try_from(self.cursor).ok(),
            };
        }

        let activity_id = *activity_id;
        let token = *token;

        // Advance past the ActivityAwaitingExternal event.
        self.cursor += 1;
        let mut scan_cursor = self.cursor;
        let mut first_interleaved_command = None;

        while scan_cursor < self.events.len() {
            if self.is_consumed(scan_cursor) {
                scan_cursor += 1;
                continue;
            }

            match &self.events[scan_cursor] {
                WorkflowEvent::ActivityCompletedExternally {
                    activity_id: id,
                    output,
                    ..
                } if *id == activity_id => {
                    let result = HistoryMatch::Matched {
                        output: output.clone(),
                    };
                    return self.settle_terminal(scan_cursor, first_interleaved_command, result);
                }
                WorkflowEvent::ActivityFailedExternally {
                    activity_id: id,
                    error,
                    ..
                } if *id == activity_id => {
                    let result = HistoryMatch::Failed {
                        error: error.clone(),
                        attempt: 1,
                        error_type: "Error".to_string(),
                        details: None,
                        non_retryable: false,
                    };
                    return self.settle_terminal(scan_cursor, first_interleaved_command, result);
                }
                WorkflowEvent::ActivityTimedOut {
                    activity_id: id,
                    timeout_type,
                } if *id == activity_id => {
                    let result = HistoryMatch::TimedOut {
                        timeout_type: timeout_type.clone(),
                    };
                    return self.settle_terminal(scan_cursor, first_interleaved_command, result);
                }
                // Deadline-extended events are informational; skip them.
                WorkflowEvent::ActivityExternalDeadlineExtended {
                    activity_id: id, ..
                } if *id == activity_id => {
                    scan_cursor += 1;
                }
                // Signals can arrive while an external activity is pending.
                WorkflowEvent::SignalReceived { .. } => {
                    self.stash_transparent_external_event(scan_cursor);
                    scan_cursor += 1;
                }
                // A second ActivityAwaitingExternal for the same activity can
                // appear when a workflow is woken by a signal while still
                // awaiting external completion: the worker re-runs
                // persist_scheduled_external_activity, but record_external_task
                // is idempotent (ON CONFLICT DO NOTHING).  Skip the duplicate.
                WorkflowEvent::ActivityAwaitingExternal {
                    activity_id: id, ..
                } if *id == activity_id => {
                    scan_cursor += 1;
                }
                WorkflowEvent::ChildWorkflowSpawnedDetached { .. } => {
                    first_interleaved_command.get_or_insert(scan_cursor);
                    scan_cursor += 1;
                }
                // Update events are transparent to the external activity scan.
                ev if Self::is_update_event(ev) => {
                    scan_cursor += 1;
                }
                // ExternalSignal event triplets can be interleaved when the
                // workflow sends a concurrent external signal while awaiting
                // external activity completion (e.g. tokio::join! with
                // signal_external_workflow), and ExternalCancel/ExternalAwait
                // triplets are likewise transparent here (issue #492, issue
                // #757). Stash them so their own matchers can find them after
                // the activity resolves, rather than breaking the scan
                // prematurely.
                WorkflowEvent::ExternalSignalRequested { .. }
                | WorkflowEvent::ExternalSignalDelivered { .. }
                | WorkflowEvent::ExternalSignalFailed { .. }
                | WorkflowEvent::ExternalCancelRequested { .. }
                | WorkflowEvent::ExternalCancelDelivered { .. }
                | WorkflowEvent::ExternalCancelFailed { .. }
                | WorkflowEvent::ExternalAwaitRequested { .. }
                | WorkflowEvent::ExternalAwaitResolved { .. }
                | WorkflowEvent::ExternalAwaitFailed { .. } => {
                    self.stash_transparent_external_event(scan_cursor);
                    scan_cursor += 1;
                }
                _ => break,
            }
        }

        // Awaiting event exists in history but no terminal found yet.
        if let Some(command_cursor) = first_interleaved_command {
            self.cursor = command_cursor;
            self.advance_to_next_unconsumed_event();
        }
        HistoryMatch::AwaitingExternalCompletion { activity_id, token }
    }

    /// Match a `signal_external_workflow` command against history.
    ///
    /// Expects `ExternalSignalRequested { target, signal_name }` at the current
    /// cursor, then scans forward for `ExternalSignalDelivered` or
    /// `ExternalSignalFailed` with the same `signal_id`.
    ///
    /// Returns:
    /// - [`HistoryMatch::Matched`] (output = `null`) when the signal was delivered
    /// - [`HistoryMatch::ExternalSignalFailed`] when `ExternalSignalFailed` is found in history
    /// - [`HistoryMatch::ExternalSignalInProgress`] when `ExternalSignalRequested`
    ///   exists but no terminal event yet (crash recovery path)
    /// - [`HistoryMatch::NoMatch`] when past end of history (first-time call)
    /// - [`HistoryMatch::Diverged`] when a different event is at this position
    #[allow(clippy::too_many_lines)]
    pub fn match_external_signal(
        &mut self,
        target: &crate::types::ExternalTarget,
        signal_name: &str,
    ) -> HistoryMatch {
        // Helper: drain a matching entry from the stash and return its result.
        let try_stash = |pending: &mut Vec<StashedExternalSignal>| {
            let pos = pending
                .iter()
                .position(|p| p.target == *target && p.signal_name == signal_name)?;
            let stashed = pending.remove(pos);
            Some(match stashed.terminal {
                Some(StashedSignalTerminal::Delivered) => HistoryMatch::Matched {
                    output: serde_json::Value::Null,
                },
                Some(StashedSignalTerminal::Failed(reason_code)) => {
                    HistoryMatch::ExternalSignalFailed {
                        signal_id: stashed.signal_id,
                        reason_code,
                    }
                }
                None => HistoryMatch::ExternalSignalInProgress {
                    signal_id: stashed.signal_id,
                    payload: stashed.payload,
                    idempotency_key: stashed.idempotency_key,
                },
            })
        };

        // prepare_match calls drain_early_signals which eagerly stashes any
        // ExternalSignal events sitting at the current cursor. This ensures
        // terminal events at the current cursor are paired with their start
        // events before we check the stash.
        // Track the stash size so we can distinguish "no history at all"
        // from "history had a different signal here" after the drain.
        let stash_size_before = self.pending_external_signals.len();
        let has_history = self.prepare_match();

        // Check the stash (which now includes any newly drained events from
        // the current cursor position, as well as events stashed by prior
        // prepare_match calls).
        //
        // Note: stash-based matching is position-independent — if two concurrent
        // signal_external calls share a drain batch, swapping their call order in
        // the workflow code will not be detected as non-determinism.  This is an
        // accepted trade-off: concurrent signals have no authoritative ordering in
        // history; sequential calls (each in their own execution cycle) always reach
        // cursor-based matching below and DO detect reordering.
        if let Some(result) = try_stash(&mut self.pending_external_signals) {
            return result;
        }
        if !has_history {
            // If prepare_match drained ExternalSignal events (stash grew) and
            // none matched, history recorded a *different* signal at this
            // position — report non-determinism instead of silently issuing a
            // new live signal.
            if self.pending_external_signals.len() > stash_size_before {
                let actual = &self.pending_external_signals[stash_size_before];
                return HistoryMatch::Diverged {
                    expected: format!(
                        "ExternalSignalRequested(target={target}, signal={signal_name})"
                    ),
                    actual: format!(
                        "ExternalSignalRequested(target={}, signal={})",
                        actual.target, actual.signal_name
                    ),

                    event_index: i32::try_from(self.cursor).ok(),
                };
            }
            return HistoryMatch::NoMatch;
        }

        // Cursor-based path: the ExternalSignalRequested event is ahead of the
        // current position and was not yet reached by drain_early_signals.
        // Sequential signal_external calls (not concurrent) always arrive here,
        // preserving ordering guarantees — a swapped call order returns Diverged.
        let result = match &self.events[self.cursor] {
            WorkflowEvent::ExternalSignalRequested {
                signal_id,
                target: recorded_target,
                signal_name: recorded_name,
                payload: recorded_payload,
                idempotency_key: recorded_idempotency_key,
            } => {
                if *recorded_target != *target {
                    return HistoryMatch::Diverged {
                        expected: format!(
                            "ExternalSignalRequested(target={target}, signal={signal_name})"
                        ),
                        actual: format!(
                            "ExternalSignalRequested(target={recorded_target}, signal={recorded_name})"
                        ),

                        event_index: i32::try_from(self.cursor).ok(),
                    };
                }
                if recorded_name != signal_name {
                    return HistoryMatch::Diverged {
                        expected: format!(
                            "ExternalSignalRequested(target={target}, signal={signal_name})"
                        ),
                        actual: format!(
                            "ExternalSignalRequested(target={target}, signal={recorded_name})"
                        ),

                        event_index: i32::try_from(self.cursor).ok(),
                    };
                }
                Ok((
                    *signal_id,
                    recorded_payload.clone(),
                    recorded_idempotency_key.clone(),
                ))
            }
            other => Err(HistoryMatch::Diverged {
                expected: format!("ExternalSignalRequested(target={target}, signal={signal_name})"),
                actual: Self::actual_event_name(other),

                event_index: i32::try_from(self.cursor).ok(),
            }),
        };

        let (signal_id, recorded_payload, recorded_idempotency_key) = match result {
            Ok(triple) => triple,
            Err(diverged) => return diverged,
        };

        // Advance past the ExternalSignalRequested event.
        self.cursor += 1;
        let mut scan_cursor = self.cursor;
        let mut first_interleaved_command = None;

        while scan_cursor < self.events.len() {
            if self.is_consumed(scan_cursor) {
                scan_cursor += 1;
                continue;
            }

            match &self.events[scan_cursor] {
                WorkflowEvent::ExternalSignalDelivered { signal_id: id } if *id == signal_id => {
                    let result = HistoryMatch::Matched {
                        output: serde_json::Value::Null,
                    };
                    return self.settle_terminal(scan_cursor, first_interleaved_command, result);
                }
                WorkflowEvent::ExternalSignalFailed {
                    signal_id: id,
                    reason_code,
                } if *id == signal_id => {
                    let reason_code = reason_code.clone();
                    let result = HistoryMatch::ExternalSignalFailed {
                        signal_id,
                        reason_code,
                    };
                    return self.settle_terminal(scan_cursor, first_interleaved_command, result);
                }
                // Signals can arrive while the external signal delivery is
                // in-flight, and ExternalCancel/ExternalAwait triplets are
                // likewise transparent to the signal forward scan (issue #492,
                // issue #757). ExternalSignal* itself is this function's own
                // subject, matched above — not part of this transparent set.
                WorkflowEvent::SignalReceived { .. }
                | WorkflowEvent::ExternalCancelRequested { .. }
                | WorkflowEvent::ExternalCancelDelivered { .. }
                | WorkflowEvent::ExternalCancelFailed { .. }
                | WorkflowEvent::ExternalAwaitRequested { .. }
                | WorkflowEvent::ExternalAwaitResolved { .. }
                | WorkflowEvent::ExternalAwaitFailed { .. } => {
                    self.stash_transparent_external_event(scan_cursor);
                    scan_cursor += 1;
                }
                // Update events are transparent to the external signal scan.
                ev if Self::is_update_event(ev) => {
                    scan_cursor += 1;
                }
                WorkflowEvent::ChildWorkflowSpawnedDetached { .. } => {
                    first_interleaved_command.get_or_insert(scan_cursor);
                    scan_cursor += 1;
                }
                _ => break,
            }
        }

        // ExternalSignalRequested found in history but no terminal event yet.
        // Worker crashed between recording the request and the delivery outcome.
        // Return the durable payload so the caller re-sends exactly what was
        // originally recorded, regardless of any code changes since the crash.
        if let Some(command_cursor) = first_interleaved_command {
            self.cursor = command_cursor;
            self.advance_to_next_unconsumed_event();
        }
        HistoryMatch::ExternalSignalInProgress {
            signal_id,
            payload: recorded_payload,
            idempotency_key: recorded_idempotency_key,
        }
    }

    /// Match `request_cancel_external_workflow(target)` against history (issue #492).
    ///
    /// Mirrors `match_external_signal` but keyed on `target` only (no `signal_name`/`payload`).
    ///
    /// Returns:
    /// - `Matched { output: Value::Null }` when `ExternalCancelDelivered` is in history
    ///   *or* when the target was already terminal (no-op success per cancel semantics).
    /// - `ExternalCancelFailed { cancel_id, reason_code }` when history shows the cancel failed.
    /// - `ExternalCancelInProgress { cancel_id }` when only `ExternalCancelRequested` is recorded
    ///   (crash recovery: re-dispatch with the recorded `cancel_id`).
    /// - `Diverged` when the history event mismatches the expected target.
    /// - `NoMatch` when there is no history at or beyond the cursor.
    #[allow(clippy::too_many_lines)]
    pub fn match_external_cancel(&mut self, target: &crate::types::ExternalTarget) -> HistoryMatch {
        // prepare_match calls drain_early_signals which eagerly stashes any
        // ExternalCancel events sitting at the current cursor. Call it first so
        // the stash check below sees freshly drained events, mirroring
        // match_external_signal's ordering (issue #492).
        let stash_size_before = self.pending_external_cancels.len();
        let has_history = self.prepare_match();

        // Check the stash (which now includes any newly drained events).
        if let Some(pos) = self
            .pending_external_cancels
            .iter()
            .position(|s| s.target == *target)
        {
            let stashed = self.pending_external_cancels.remove(pos);
            return match stashed.terminal {
                Some(StashedCancelTerminal::Delivered) => HistoryMatch::Matched {
                    output: serde_json::Value::Null,
                },
                Some(StashedCancelTerminal::Failed(reason_code)) => {
                    HistoryMatch::ExternalCancelFailed {
                        cancel_id: stashed.cancel_id,
                        reason_code,
                    }
                }
                None => HistoryMatch::ExternalCancelInProgress {
                    cancel_id: stashed.cancel_id,
                },
            };
        }

        if !has_history {
            // History recorded a *different* cancel at this position.
            if self.pending_external_cancels.len() > stash_size_before {
                let actual = &self.pending_external_cancels[stash_size_before];
                return HistoryMatch::Diverged {
                    expected: format!("ExternalCancelRequested(target={target})"),
                    actual: format!("ExternalCancelRequested(target={})", actual.target),
                    event_index: i32::try_from(self.cursor).ok(),
                };
            }
            return HistoryMatch::NoMatch;
        }

        // Cursor-based path: the ExternalCancelRequested event is at or ahead of cursor.
        let WorkflowEvent::ExternalCancelRequested {
            cancel_id,
            target: recorded_target,
        } = &self.events[self.cursor]
        else {
            return HistoryMatch::Diverged {
                expected: format!("ExternalCancelRequested(target={target})"),
                actual: Self::actual_event_name(&self.events[self.cursor]),
                event_index: i32::try_from(self.cursor).ok(),
            };
        };

        if *recorded_target != *target {
            return HistoryMatch::Diverged {
                expected: format!("ExternalCancelRequested(target={target})"),
                actual: format!("ExternalCancelRequested(target={recorded_target})"),
                event_index: i32::try_from(self.cursor).ok(),
            };
        }

        let cancel_id = *cancel_id;
        // Advance past ExternalCancelRequested (mirrors match_external_signal line 1651).
        self.cursor += 1;
        let mut scan_cursor = self.cursor;
        let mut first_interleaved_command = None;

        while scan_cursor < self.events.len() {
            if self.is_consumed(scan_cursor) {
                scan_cursor += 1;
                continue;
            }

            match &self.events[scan_cursor] {
                // Terminal events for this cancel — use settle_terminal to handle
                // out-of-order consumption correctly.
                WorkflowEvent::ExternalCancelDelivered { cancel_id: id } if *id == cancel_id => {
                    let result = HistoryMatch::Matched {
                        output: serde_json::Value::Null,
                    };
                    return self.settle_terminal(scan_cursor, first_interleaved_command, result);
                }
                WorkflowEvent::ExternalCancelFailed {
                    cancel_id: id,
                    reason_code,
                } if *id == cancel_id => {
                    let rc = reason_code.clone();
                    let result = HistoryMatch::ExternalCancelFailed {
                        cancel_id,
                        reason_code: rc,
                    };
                    return self.settle_terminal(scan_cursor, first_interleaved_command, result);
                }

                // Interleaved lifecycle / update events — skip transparently.
                WorkflowEvent::WorkflowStarted { .. }
                | WorkflowEvent::UpdateAdmitted { .. }
                | WorkflowEvent::UpdateCompleted { .. }
                | WorkflowEvent::UpdateFailed { .. }
                | WorkflowEvent::WorkflowExecutionPaused { .. }
                | WorkflowEvent::WorkflowExecutionResumed { .. } => {
                    scan_cursor += 1;
                }

                // Signals can arrive while the external cancel is in-flight — stash
                // them so a later `receive_signal` still observes them (mirrors
                // `match_external_signal`; without this the cursor would jump past
                // the signal when the cancel terminal settles and the signal would
                // be lost).
                WorkflowEvent::SignalReceived {
                    signal_name: sn,
                    payload,
                } => {
                    let sn = sn.clone();
                    let payload = payload.clone();
                    self.stash_signal(scan_cursor, sn, payload);
                    scan_cursor += 1;
                }

                // Interleaved external-signal triplets — stash for later.
                WorkflowEvent::ExternalSignalRequested {
                    signal_id,
                    target: sig_target,
                    signal_name,
                    payload,
                    idempotency_key,
                } => {
                    self.stash_external_signal_request(
                        scan_cursor,
                        *signal_id,
                        sig_target.clone(),
                        signal_name.clone(),
                        payload.clone(),
                        idempotency_key.clone(),
                    );
                    scan_cursor += 1;
                }
                WorkflowEvent::ExternalSignalDelivered { signal_id } => {
                    self.stash_external_signal_terminal(
                        scan_cursor,
                        *signal_id,
                        StashedSignalTerminal::Delivered,
                    );
                    scan_cursor += 1;
                }
                WorkflowEvent::ExternalSignalFailed {
                    signal_id,
                    reason_code,
                } => {
                    self.stash_external_signal_terminal(
                        scan_cursor,
                        *signal_id,
                        StashedSignalTerminal::Failed(reason_code.clone()),
                    );
                    scan_cursor += 1;
                }

                // Other ExternalCancel triplets (sibling cancels) — stash.
                WorkflowEvent::ExternalCancelRequested {
                    cancel_id: other_id,
                    target: other_target,
                } => {
                    self.pending_external_cancels.push(StashedExternalCancel {
                        cancel_id: *other_id,
                        target: other_target.clone(),
                        terminal: None,
                    });
                    self.consumed_signal_events.insert(scan_cursor);
                    scan_cursor += 1;
                }
                WorkflowEvent::ExternalCancelDelivered {
                    cancel_id: other_id,
                } => {
                    if let Some(s) = self
                        .pending_external_cancels
                        .iter_mut()
                        .find(|s| s.cancel_id == *other_id)
                    {
                        s.terminal = Some(StashedCancelTerminal::Delivered);
                    }
                    self.consumed_signal_events.insert(scan_cursor);
                    scan_cursor += 1;
                }
                WorkflowEvent::ExternalCancelFailed {
                    cancel_id: other_id,
                    reason_code,
                } => {
                    if let Some(s) = self
                        .pending_external_cancels
                        .iter_mut()
                        .find(|s| s.cancel_id == *other_id)
                    {
                        s.terminal = Some(StashedCancelTerminal::Failed(reason_code.clone()));
                    }
                    self.consumed_signal_events.insert(scan_cursor);
                    scan_cursor += 1;
                }
                WorkflowEvent::ExternalAwaitRequested { await_id, target } => {
                    self.pending_external_awaits.push(StashedExternalAwait {
                        await_id: *await_id,
                        target: *target,
                        terminal: None,
                    });
                    self.consumed_signal_events.insert(scan_cursor);
                    scan_cursor += 1;
                }
                WorkflowEvent::ExternalAwaitResolved { await_id, output } => {
                    if let Some(s) = self
                        .pending_external_awaits
                        .iter_mut()
                        .find(|s| s.await_id == *await_id)
                    {
                        s.terminal = Some(StashedAwaitTerminal::Resolved(output.clone()));
                    }
                    self.consumed_signal_events.insert(scan_cursor);
                    scan_cursor += 1;
                }
                WorkflowEvent::ExternalAwaitFailed {
                    await_id,
                    reason_code,
                    message,
                    error_type,
                    details,
                    non_retryable,
                } => {
                    if let Some(s) = self
                        .pending_external_awaits
                        .iter_mut()
                        .find(|s| s.await_id == *await_id)
                    {
                        s.terminal = Some(StashedAwaitTerminal::Failed {
                            reason_code: reason_code.clone(),
                            message: message.clone(),
                            error_type: error_type.clone(),
                            details: details.clone(),
                            non_retryable: *non_retryable,
                        });
                    }
                    self.consumed_signal_events.insert(scan_cursor);
                    scan_cursor += 1;
                }

                // Interleaved commands — note the position but keep scanning for
                // the terminal event.
                WorkflowEvent::ActivityScheduled { .. }
                | WorkflowEvent::ActivityCompleted { .. }
                | WorkflowEvent::ActivityFailed { .. }
                | WorkflowEvent::ActivityTimedOut { .. }
                | WorkflowEvent::LocalActivityScheduled { .. }
                | WorkflowEvent::LocalActivityCompleted { .. }
                | WorkflowEvent::LocalActivityFailed { .. }
                | WorkflowEvent::TimerStarted { .. }
                | WorkflowEvent::TimerFired { .. }
                // Cancellable-timer cancel (issue #768): interleavable like the
                // other timer-lifecycle events already listed here (rewind
                // without consuming so `match_timer_cancel` claims it later).
                | WorkflowEvent::TimerCancelled { .. }
                | WorkflowEvent::ChildWorkflowStarted { .. }
                | WorkflowEvent::ChildWorkflowCompleted { .. }
                | WorkflowEvent::ChildWorkflowFailed { .. }
                | WorkflowEvent::ChildWorkflowSpawnedDetached { .. }
                | WorkflowEvent::MarkerRecorded { .. }
                | WorkflowEvent::SideEffectRecorded { .. } => {
                    if first_interleaved_command.is_none() {
                        first_interleaved_command = Some(scan_cursor);
                    }
                    scan_cursor += 1;
                }

                _ => break,
            }
        }

        // ExternalCancelRequested found in history but no terminal event yet.
        if let Some(command_cursor) = first_interleaved_command {
            self.cursor = command_cursor;
            self.advance_to_next_unconsumed_event();
        }
        HistoryMatch::ExternalCancelInProgress { cancel_id }
    }

    /// Match `await_external_workflow(target)` against history (issue #757).
    ///
    /// Mirrors `match_external_cancel` but keyed on the awaited `target`, and
    /// carries the target's terminal cause (output on success, typed failure
    /// otherwise) frozen into the awaiter's own history.
    ///
    /// Returns:
    /// - `Matched { output }` when `ExternalAwaitResolved` is in history (target
    ///   reached `COMPLETED`) — carries the recorded output value.
    /// - `ExternalAwaitFailed { .. }` when history shows the await failed
    ///   (target reached a non-`COMPLETED` terminal state, or unknown).
    /// - `ExternalAwaitInProgress { await_id }` when only `ExternalAwaitRequested`
    ///   is recorded (crash recovery / still-pending: re-dispatch with the
    ///   recorded `await_id`).
    /// - `Diverged` when the history event mismatches the expected target.
    /// - `NoMatch` when there is no history at or beyond the cursor.
    #[allow(clippy::too_many_lines)]
    pub fn match_external_await(&mut self, target: ExecutionId) -> HistoryMatch {
        // prepare_match drains any ExternalAwait events sitting at the current
        // cursor into the stash first (mirrors match_external_cancel).
        let stash_size_before = self.pending_external_awaits.len();
        let has_history = self.prepare_match();

        // Check the stash (which now includes any newly drained events).
        if let Some(pos) = self
            .pending_external_awaits
            .iter()
            .position(|s| s.target == target)
        {
            let stashed = self.pending_external_awaits.remove(pos);
            return match stashed.terminal {
                Some(StashedAwaitTerminal::Resolved(output)) => HistoryMatch::Matched { output },
                Some(StashedAwaitTerminal::Failed {
                    reason_code,
                    message,
                    error_type,
                    details,
                    non_retryable,
                }) => HistoryMatch::ExternalAwaitFailed {
                    await_id: stashed.await_id,
                    reason_code,
                    message,
                    error_type,
                    details,
                    non_retryable,
                },
                None => HistoryMatch::ExternalAwaitInProgress {
                    await_id: stashed.await_id,
                },
            };
        }

        if !has_history {
            // History recorded a *different* await at this position.
            if self.pending_external_awaits.len() > stash_size_before {
                let actual = &self.pending_external_awaits[stash_size_before];
                return HistoryMatch::Diverged {
                    expected: format!("ExternalAwaitRequested(target={target})"),
                    actual: format!("ExternalAwaitRequested(target={})", actual.target),
                    event_index: i32::try_from(self.cursor).ok(),
                };
            }
            return HistoryMatch::NoMatch;
        }

        // Cursor-based path: the ExternalAwaitRequested event is at or ahead of cursor.
        let WorkflowEvent::ExternalAwaitRequested {
            await_id,
            target: recorded_target,
        } = &self.events[self.cursor]
        else {
            return HistoryMatch::Diverged {
                expected: format!("ExternalAwaitRequested(target={target})"),
                actual: Self::actual_event_name(&self.events[self.cursor]),
                event_index: i32::try_from(self.cursor).ok(),
            };
        };

        if *recorded_target != target {
            return HistoryMatch::Diverged {
                expected: format!("ExternalAwaitRequested(target={target})"),
                actual: format!("ExternalAwaitRequested(target={recorded_target})"),
                event_index: i32::try_from(self.cursor).ok(),
            };
        }

        let await_id = *await_id;
        // Advance past ExternalAwaitRequested.
        self.cursor += 1;
        let mut scan_cursor = self.cursor;
        let mut first_interleaved_command = None;

        while scan_cursor < self.events.len() {
            if self.is_consumed(scan_cursor) {
                scan_cursor += 1;
                continue;
            }

            match &self.events[scan_cursor] {
                // Terminal events for this await — settle_terminal handles
                // out-of-order consumption correctly.
                WorkflowEvent::ExternalAwaitResolved {
                    await_id: id,
                    output,
                } if *id == await_id => {
                    let result = HistoryMatch::Matched {
                        output: output.clone(),
                    };
                    return self.settle_terminal(scan_cursor, first_interleaved_command, result);
                }
                WorkflowEvent::ExternalAwaitFailed {
                    await_id: id,
                    reason_code,
                    message,
                    error_type,
                    details,
                    non_retryable,
                } if *id == await_id => {
                    let result = HistoryMatch::ExternalAwaitFailed {
                        await_id,
                        reason_code: reason_code.clone(),
                        message: message.clone(),
                        error_type: error_type.clone(),
                        details: details.clone(),
                        non_retryable: *non_retryable,
                    };
                    return self.settle_terminal(scan_cursor, first_interleaved_command, result);
                }

                // Interleaved lifecycle / update events — skip transparently.
                WorkflowEvent::WorkflowStarted { .. }
                | WorkflowEvent::UpdateAdmitted { .. }
                | WorkflowEvent::UpdateCompleted { .. }
                | WorkflowEvent::UpdateFailed { .. }
                | WorkflowEvent::WorkflowExecutionPaused { .. }
                | WorkflowEvent::WorkflowExecutionResumed { .. } => {
                    scan_cursor += 1;
                }

                // Signals can arrive while the external await is in-flight — stash
                // them so a later `receive_signal` still observes them.
                WorkflowEvent::SignalReceived {
                    signal_name: sn,
                    payload,
                } => {
                    let sn = sn.clone();
                    let payload = payload.clone();
                    self.stash_signal(scan_cursor, sn, payload);
                    scan_cursor += 1;
                }

                // Interleaved external-signal triplets — stash for later.
                WorkflowEvent::ExternalSignalRequested {
                    signal_id,
                    target: sig_target,
                    signal_name,
                    payload,
                    idempotency_key,
                } => {
                    self.stash_external_signal_request(
                        scan_cursor,
                        *signal_id,
                        sig_target.clone(),
                        signal_name.clone(),
                        payload.clone(),
                        idempotency_key.clone(),
                    );
                    scan_cursor += 1;
                }
                WorkflowEvent::ExternalSignalDelivered { signal_id } => {
                    self.stash_external_signal_terminal(
                        scan_cursor,
                        *signal_id,
                        StashedSignalTerminal::Delivered,
                    );
                    scan_cursor += 1;
                }
                WorkflowEvent::ExternalSignalFailed {
                    signal_id,
                    reason_code,
                } => {
                    self.stash_external_signal_terminal(
                        scan_cursor,
                        *signal_id,
                        StashedSignalTerminal::Failed(reason_code.clone()),
                    );
                    scan_cursor += 1;
                }

                // Interleaved external-cancel triplets — stash.
                WorkflowEvent::ExternalCancelRequested {
                    cancel_id: other_id,
                    target: other_target,
                } => {
                    self.pending_external_cancels.push(StashedExternalCancel {
                        cancel_id: *other_id,
                        target: other_target.clone(),
                        terminal: None,
                    });
                    self.consumed_signal_events.insert(scan_cursor);
                    scan_cursor += 1;
                }
                WorkflowEvent::ExternalCancelDelivered {
                    cancel_id: other_id,
                } => {
                    if let Some(s) = self
                        .pending_external_cancels
                        .iter_mut()
                        .find(|s| s.cancel_id == *other_id)
                    {
                        s.terminal = Some(StashedCancelTerminal::Delivered);
                    }
                    self.consumed_signal_events.insert(scan_cursor);
                    scan_cursor += 1;
                }
                WorkflowEvent::ExternalCancelFailed {
                    cancel_id: other_id,
                    reason_code,
                } => {
                    if let Some(s) = self
                        .pending_external_cancels
                        .iter_mut()
                        .find(|s| s.cancel_id == *other_id)
                    {
                        s.terminal = Some(StashedCancelTerminal::Failed(reason_code.clone()));
                    }
                    self.consumed_signal_events.insert(scan_cursor);
                    scan_cursor += 1;
                }

                // Other ExternalAwait triplets (sibling awaits) — stash.
                WorkflowEvent::ExternalAwaitRequested {
                    await_id: other_id,
                    target: other_target,
                } => {
                    self.pending_external_awaits.push(StashedExternalAwait {
                        await_id: *other_id,
                        target: *other_target,
                        terminal: None,
                    });
                    self.consumed_signal_events.insert(scan_cursor);
                    scan_cursor += 1;
                }
                WorkflowEvent::ExternalAwaitResolved {
                    await_id: other_id,
                    output,
                } => {
                    if let Some(s) = self
                        .pending_external_awaits
                        .iter_mut()
                        .find(|s| s.await_id == *other_id)
                    {
                        s.terminal = Some(StashedAwaitTerminal::Resolved(output.clone()));
                    }
                    self.consumed_signal_events.insert(scan_cursor);
                    scan_cursor += 1;
                }
                WorkflowEvent::ExternalAwaitFailed {
                    await_id: other_id,
                    reason_code,
                    message,
                    error_type,
                    details,
                    non_retryable,
                } => {
                    if let Some(s) = self
                        .pending_external_awaits
                        .iter_mut()
                        .find(|s| s.await_id == *other_id)
                    {
                        s.terminal = Some(StashedAwaitTerminal::Failed {
                            reason_code: reason_code.clone(),
                            message: message.clone(),
                            error_type: error_type.clone(),
                            details: details.clone(),
                            non_retryable: *non_retryable,
                        });
                    }
                    self.consumed_signal_events.insert(scan_cursor);
                    scan_cursor += 1;
                }

                // Interleaved commands — note the position but keep scanning for
                // the terminal event.
                WorkflowEvent::ActivityScheduled { .. }
                | WorkflowEvent::ActivityCompleted { .. }
                | WorkflowEvent::ActivityFailed { .. }
                | WorkflowEvent::ActivityTimedOut { .. }
                | WorkflowEvent::LocalActivityScheduled { .. }
                | WorkflowEvent::LocalActivityCompleted { .. }
                | WorkflowEvent::LocalActivityFailed { .. }
                | WorkflowEvent::TimerStarted { .. }
                | WorkflowEvent::TimerFired { .. }
                | WorkflowEvent::TimerCancelled { .. }
                | WorkflowEvent::ChildWorkflowStarted { .. }
                | WorkflowEvent::ChildWorkflowCompleted { .. }
                | WorkflowEvent::ChildWorkflowFailed { .. }
                | WorkflowEvent::ChildWorkflowSpawnedDetached { .. }
                | WorkflowEvent::MarkerRecorded { .. }
                | WorkflowEvent::SideEffectRecorded { .. } => {
                    if first_interleaved_command.is_none() {
                        first_interleaved_command = Some(scan_cursor);
                    }
                    scan_cursor += 1;
                }

                _ => break,
            }
        }

        // ExternalAwaitRequested found in history but no terminal event yet.
        if let Some(command_cursor) = first_interleaved_command {
            self.cursor = command_cursor;
            self.advance_to_next_unconsumed_event();
        }
        HistoryMatch::ExternalAwaitInProgress { await_id }
    }

    /// Peek forward to determine if `TimerStarted` for the requested ID is the next active deterministic event in history.
    #[must_use]
    pub fn is_timer_started_next(&self, timer_id: &str) -> bool {
        let mut idx = self.cursor;
        while idx < self.events.len() {
            if self.is_consumed(idx) {
                idx += 1;
                continue;
            }
            match &self.events[idx] {
                WorkflowEvent::SignalReceived { .. } => {
                    idx += 1;
                }
                ev if Self::is_update_event(ev) => {
                    idx += 1;
                }
                WorkflowEvent::ExternalSignalRequested { .. }
                | WorkflowEvent::ExternalSignalDelivered { .. }
                | WorkflowEvent::ExternalSignalFailed { .. }
                | WorkflowEvent::ExternalCancelRequested { .. }
                | WorkflowEvent::ExternalCancelDelivered { .. }
                | WorkflowEvent::ExternalCancelFailed { .. }
                // External await event triplets are transparent here (issue #757).
                | WorkflowEvent::ExternalAwaitRequested { .. }
                | WorkflowEvent::ExternalAwaitResolved { .. }
                | WorkflowEvent::ExternalAwaitFailed { .. }
                | WorkflowEvent::ChildWorkflowSpawnedDetached { .. }
                // A cancellable-timer cancel (issue #768) is transparent here.
                | WorkflowEvent::TimerCancelled { .. } => {
                    idx += 1;
                }
                WorkflowEvent::TimerStarted { timer_id: id, .. } => {
                    return id.as_str() == timer_id;
                }
                _ => return false,
            }
        }
        false
    }

    /// Pure, read-only scan: does recorded history contain **any**
    /// `TimerStarted` event for `timer_id`?
    ///
    /// Used by `wait_for_signal_timeout`'s signal-win branch (issue #476) to
    /// decide whether a durable deadline-timer row was ever armed and therefore
    /// needs a `CancelRaceLosers` teardown. When a signal arrived *before* the
    /// race started on the live run, `match_signal_or_timer` short-circuits to
    /// `SignalWon` without ever recording a `TimerStarted` — no timer row exists,
    /// so no teardown must be emitted. Does not touch the cursor or any
    /// consumption state.
    #[must_use]
    pub fn history_contains_timer_started(&self, timer_id: &str) -> bool {
        self.events.iter().any(|ev| {
            matches!(
                ev,
                WorkflowEvent::TimerStarted { timer_id: id, .. } if id.as_str() == timer_id
            )
        })
    }

    /// Match a timer command against history.
    ///
    /// Expects `TimerStarted { timer_id }` at cursor, then scans for
    /// `TimerFired` with the same `timer_id`.
    pub fn match_timer(&mut self, timer_id: &str) -> HistoryMatch {
        self.match_timer_strict(timer_id, None)
    }

    /// Match a timer command against history, strictly checking duration if provided.
    #[allow(clippy::too_many_lines)]
    pub fn match_timer_strict(
        &mut self,
        timer_id: &str,
        expected_duration: Option<u64>,
    ) -> HistoryMatch {
        if !self.prepare_match() {
            return HistoryMatch::NoMatch;
        }

        let WorkflowEvent::TimerStarted {
            timer_id: recorded_id,
            duration_secs: recorded_duration,
        } = &self.events[self.cursor]
        else {
            return HistoryMatch::Diverged {
                expected: format!("TimerStarted({timer_id})"),
                actual: Self::actual_event_name(&self.events[self.cursor]),

                event_index: i32::try_from(self.cursor).ok(),
            };
        };

        if recorded_id.as_str() != timer_id {
            return HistoryMatch::Diverged {
                expected: format!("TimerStarted({timer_id})"),
                actual: format!("TimerStarted({recorded_id})"),

                event_index: i32::try_from(self.cursor).ok(),
            };
        }

        if let Some(expected) = expected_duration
            && *recorded_duration != expected
        {
            return HistoryMatch::Diverged {
                expected: format!("TimerStarted({timer_id}, duration={expected}s)"),
                actual: format!("TimerStarted({recorded_id}, duration={recorded_duration}s)"),

                event_index: i32::try_from(self.cursor).ok(),
            };
        }

        // Advance past TimerStarted
        self.cursor += 1;
        let mut scan_cursor = self.cursor;
        let mut first_interleaved_command = None;

        // Scan forward for TimerFired, skipping consumed child terminals and signals.
        while scan_cursor < self.events.len() {
            if self.is_consumed(scan_cursor) {
                scan_cursor += 1;
                continue;
            }

            if let WorkflowEvent::TimerFired { timer_id: id } = &self.events[scan_cursor]
                && id.as_str() == timer_id
            {
                let result = HistoryMatch::Matched {
                    output: Value::Null,
                };
                return self.settle_terminal(scan_cursor, first_interleaved_command, result);
            }

            if matches!(
                self.events[scan_cursor],
                WorkflowEvent::ChildWorkflowStarted { .. }
                    | WorkflowEvent::ChildWorkflowSpawnedDetached { .. }
            ) {
                first_interleaved_command.get_or_insert(scan_cursor);
                scan_cursor += 1;
                continue;
            }

            // Signals can arrive while a timer is pending; stash them for
            // later wait_for_signal calls and continue scanning.
            if let WorkflowEvent::SignalReceived { .. } = &self.events[scan_cursor] {
                self.stash_transparent_external_event(scan_cursor);
                scan_cursor += 1;
                continue;
            }

            // ExternalSignal event triplets can be interleaved with an in-flight
            // timer (e.g. tokio::join!(signal_external, sleep)), and
            // ExternalCancel/ExternalAwait triplets are likewise transparent to
            // timer scans (issue #492, issue #757). Stash them so their own
            // matchers can find them after the timer resolves.
            match &self.events[scan_cursor] {
                WorkflowEvent::ExternalSignalRequested { .. }
                | WorkflowEvent::ExternalSignalDelivered { .. }
                | WorkflowEvent::ExternalSignalFailed { .. }
                | WorkflowEvent::ExternalCancelRequested { .. }
                | WorkflowEvent::ExternalCancelDelivered { .. }
                | WorkflowEvent::ExternalCancelFailed { .. }
                | WorkflowEvent::ExternalAwaitRequested { .. }
                | WorkflowEvent::ExternalAwaitResolved { .. }
                | WorkflowEvent::ExternalAwaitFailed { .. } => {
                    self.stash_transparent_external_event(scan_cursor);
                    scan_cursor += 1;
                    continue;
                }
                _ => {}
            }

            // Update events are transparent to the timer scan.
            if Self::is_update_event(&self.events[scan_cursor]) {
                scan_cursor += 1;
                continue;
            }

            // A cancellable-timer arm/cancel (issue #768) for another timer may be
            // interleaved while this timer is pending — e.g. a sibling branch
            // `reset()` records `[TimerCancelled(idle), TimerStarted(idle)]` between
            // this `ctx.timer`'s own `TimerStarted` and its `TimerFired`. Treat
            // BOTH as interleaved commands (rewind after the fire settles) so each
            // stays at the cursor for its own claimer (`match_timer_cancel` /
            // `match_timer_arm`) to re-scan. Skipping only the cancel would STOP
            // the scan on the paired re-arm and wrongly return NoMatch, so strict
            // replay would fail and a worker could re-park an already-fired timer
            // (Codex P2, issue #768).
            if matches!(
                &self.events[scan_cursor],
                WorkflowEvent::TimerCancelled { .. } | WorkflowEvent::TimerStarted { .. }
            ) {
                first_interleaved_command.get_or_insert(scan_cursor);
                scan_cursor += 1;
                continue;
            }

            // Interleaved sibling commands from a mixed suspension batch
            // (e.g. `tokio::join!(ctx.timer(...), ctx.execute_activity(...))`
            // where the timer command is emitted FIRST). Keep the first one as
            // the rewind cursor — mirroring `match_signal_or_timer` — and scan
            // past it so this timer's own `TimerFired` recorded later is still
            // found. `settle_terminal` rewinds to it so the sibling's own
            // matcher re-scans in program order (issue #1071).
            if matches!(
                &self.events[scan_cursor],
                WorkflowEvent::ActivityScheduled { .. }
                    | WorkflowEvent::LocalActivityScheduled { .. }
                    | WorkflowEvent::MarkerRecorded { .. }
                    | WorkflowEvent::SideEffectRecorded { .. }
            ) {
                first_interleaved_command.get_or_insert(scan_cursor);
                scan_cursor += 1;
                continue;
            }

            // Progress and terminal events of those concurrent siblings — and
            // fires of FOREIGN timers (id != timer_id; this timer's own fire is
            // matched above) — are transparent to this timer's scan. Cross them
            // NON-CONSUMINGLY, leaving each for its own matcher to claim after
            // the rewind (a foreign `TimerFired` belongs to the sibling timer's
            // own `match_timer_strict`; consuming it here would steal it). This
            // is the reversed-fire-order multi-timer case (issue #1071 #4).
            // Keep this sibling-terminal set in sync with
            // `match_signal_or_timer`'s and `scan_activity_terminal`'s (issue #1071).
            if matches!(
                &self.events[scan_cursor],
                WorkflowEvent::ActivityStarted { .. }
                    | WorkflowEvent::ActivityHeartbeat { .. }
                    | WorkflowEvent::ActivityCompleted { .. }
                    | WorkflowEvent::ActivityFailed { .. }
                    | WorkflowEvent::ActivityTimedOut { .. }
                    | WorkflowEvent::LocalActivityCompleted { .. }
                    | WorkflowEvent::LocalActivityFailed { .. }
                    | WorkflowEvent::ChildWorkflowCompleted { .. }
                    | WorkflowEvent::ChildWorkflowFailed { .. }
                    | WorkflowEvent::TimerFired { .. }
            ) {
                scan_cursor += 1;
                continue;
            }

            break;
        }

        // Timer was started but never fired — incomplete history.
        //
        // No rewind is needed here even though the scan may have crossed
        // interleaved sibling commands: `self.cursor` was never advanced past
        // them (only the local `scan_cursor` moved). Only `settle_terminal`, on
        // an actual win, persists scan progress by moving `self.cursor` — the
        // no-match path leaves the cursor at this timer's `TimerStarted` so the
        // suspending `ctx.timer` re-parks correctly and the interleaved siblings
        // stay claimable by their own matchers in program order.
        HistoryMatch::NoMatch
    }

    // ── Cancellable / renewable durable timers (issue #768) ──────────────────

    /// Match the *arm* of an author-controlled durable timer against history.
    ///
    /// Positional (like a marker): expects `TimerStarted { timer_id,
    /// duration_secs }` at the current cursor, consumes it, and returns
    /// [`HistoryMatch::Matched`]. Unlike [`Self::match_timer_strict`] it does
    /// **not** scan for a `TimerFired` — arming is non-suspending; the fire is
    /// observed later via [`Self::match_timer_or_cancel`].
    ///
    /// Returns [`HistoryMatch::NoMatch`] when the cursor is past end (first live
    /// arm) and [`HistoryMatch::Diverged`] when a different event / id / duration
    /// is at the cursor.
    pub fn match_timer_arm(&mut self, timer_id: &str, expected_duration: u64) -> HistoryMatch {
        if !self.prepare_match() {
            return HistoryMatch::NoMatch;
        }

        let WorkflowEvent::TimerStarted {
            timer_id: recorded_id,
            duration_secs: recorded_duration,
        } = &self.events[self.cursor]
        else {
            return HistoryMatch::Diverged {
                expected: format!("TimerStarted({timer_id})"),
                actual: Self::actual_event_name(&self.events[self.cursor]),
                event_index: i32::try_from(self.cursor).ok(),
            };
        };

        if recorded_id.as_str() != timer_id {
            return HistoryMatch::Diverged {
                expected: format!("TimerStarted({timer_id})"),
                actual: format!("TimerStarted({recorded_id})"),
                event_index: i32::try_from(self.cursor).ok(),
            };
        }

        if *recorded_duration != expected_duration {
            return HistoryMatch::Diverged {
                expected: format!("TimerStarted({timer_id}, duration={expected_duration}s)"),
                actual: format!("TimerStarted({timer_id}, duration={recorded_duration}s)"),
                event_index: i32::try_from(self.cursor).ok(),
            };
        }

        self.cursor += 1;
        self.advance_to_next_unconsumed_event();
        HistoryMatch::Matched {
            output: Value::Null,
        }
    }

    // ── Durable mutex (issue #691) ───────────────────────────────────────────

    /// Match a durable-mutex acquire against history (issue #691).
    ///
    /// Positional (like a marker / [`Self::match_timer_arm`]): expects a
    /// [`WorkflowEvent::MutexGranted`] with the matching `key` at the current
    /// cursor, consumes it, and returns [`MutexGrantMatch::Granted`] carrying the
    /// recorded `lock_seq`/`acquired_at`. Because acquire is a SOLO suspension
    /// (its `MutexGranted` anchor always lands at a clean resume cursor), no
    /// forward scan and no other-scan skip-list changes are needed.
    ///
    /// Returns [`MutexGrantMatch::NoMatch`] when the cursor is past end (first
    /// live acquire — the caller suspends on an `AcquireMutex` command) and
    /// [`MutexGrantMatch::Diverged`] when a different event / key is at the cursor.
    pub fn match_mutex_granted(&mut self, key: &str) -> MutexGrantMatch {
        if !self.prepare_match() {
            return MutexGrantMatch::NoMatch;
        }

        let WorkflowEvent::MutexGranted {
            key: recorded_key,
            lock_seq,
            acquired_at,
        } = &self.events[self.cursor]
        else {
            return MutexGrantMatch::Diverged {
                expected: format!("MutexGranted({key})"),
                actual: Self::actual_event_name(&self.events[self.cursor]),
                event_index: i32::try_from(self.cursor).ok(),
            };
        };

        if recorded_key.as_str() != key {
            return MutexGrantMatch::Diverged {
                expected: format!("MutexGranted({key})"),
                actual: format!("MutexGranted({recorded_key})"),
                event_index: i32::try_from(self.cursor).ok(),
            };
        }

        let lock_seq = *lock_seq;
        let acquired_at = *acquired_at;
        self.cursor += 1;
        self.advance_to_next_unconsumed_event();
        MutexGrantMatch::Granted {
            lock_seq,
            acquired_at,
        }
    }

    /// Match the *cancel* of an author-controlled durable timer against history.
    ///
    /// Forward-scans from the cursor for the first unconsumed
    /// `TimerCancelled { timer_id }`, claiming it out of order (marks it
    /// consumed) and returning [`HistoryMatch::Matched`].
    ///
    /// Returns [`HistoryMatch::NoMatch`] when no such event exists (live
    /// cancel — the caller emits a `CancelTimer` command; in strict replay the
    /// caller surfaces the divergence via
    /// [`crate::context::WorkflowContext::check_strict_replay_no_match`]).
    ///
    /// # Soundness — the scan STOPS at intervening commands (Codex P2 round 6)
    ///
    /// The forward scan is deliberately **not** a "skip everything until the
    /// cancel" loop. It shares [`Self::timer_scan_cross_or_stop`]'s crossable-set
    /// discipline **exactly** with [`Self::match_timer_or_cancel`]: it may cross
    /// only (a) already-`is_consumed` events (the workflow ran other operations —
    /// an activity, a side effect — BEFORE the cancel, so those were consumed by
    /// their own `match_*` calls in program order) and (b) a small allowlist of
    /// genuinely transparent / interleavable events (markers, side effects,
    /// detached-child spawns, sibling `reset()` arm/cancel interleaving, stashed
    /// signals & external-signal/cancel triplets, and update events). It STOPS
    /// (returns [`HistoryMatch::NoMatch`]) at any UNCONSUMED command-bearing event
    /// NOT on that allowlist. Without the stop, a code change that moved the
    /// `cancel_timer` BEFORE an activity recorded first could claim the trailing
    /// `TimerCancelled` across the unconsumed `ActivityScheduled` and pass strict
    /// replay despite a real command-order change (the false negative this fix
    /// closes — sibling of the round-5 `match_timer_or_cancel` fix).
    pub fn match_timer_cancel(&mut self, timer_id: &str) -> HistoryMatch {
        // Reset the blocked-scan flag on entry; the callers read it after a
        // NoMatch return to distinguish a blocked scan (divergence) from a
        // genuine live-frontier NoMatch (Codex P2 round 12, issue #768).
        self.timer_scan_stopped_at_command = false;
        if !self.prepare_match() {
            return HistoryMatch::NoMatch;
        }
        let mut scan = self.cursor;
        while scan < self.events.len() {
            if self.is_consumed(scan) {
                scan += 1;
                continue;
            }
            // The cancel for THIS timer — claim it out of order.
            if let WorkflowEvent::TimerCancelled { timer_id: id } = &self.events[scan]
                && id.as_str() == timer_id
            {
                self.consumed_out_of_order_events.insert(scan);
                self.advance_to_next_unconsumed_event();
                return HistoryMatch::Matched {
                    output: Value::Null,
                };
            }
            // Any other unconsumed event: cross it if transparent/interleavable,
            // otherwise STOP (a command-ordering point the cancel claim must not
            // cross).
            match self.timer_scan_cross_or_stop(scan, timer_id) {
                TimerScanStep::Cross => scan += 1,
                TimerScanStep::Stop => {
                    self.timer_scan_stopped_at_command = true;
                    break;
                }
            }
        }
        HistoryMatch::NoMatch
    }

    /// One step of the shared crossable-set discipline for the cancellable-timer
    /// forward scans ([`Self::match_timer_or_cancel`] / [`Self::match_timer_cancel`],
    /// issue #768). The caller has already handled its own timer-specific
    /// *target* event(s) and the `is_consumed` skip; this decides what to do with
    /// any OTHER unconsumed event at `scan`.
    ///
    /// Returns [`TimerScanStep::Cross`] — performing the same stashing side
    /// effects the two callers used inline — for the genuinely transparent /
    /// interleavable classes: bookkeeping markers & side effects, fire-and-forget
    /// detached child spawns, a **foreign-id** sibling `reset()`'s
    /// `[TimerCancelled(x), TimerStarted(x)]` arm/cancel interleaving, a
    /// **foreign-id** sibling's `TimerFired` (a concurrent `await_fire` sibling's
    /// own outcome — Codex P2 round 14), stashed signals, external signal/cancel
    /// triplets, and update events.
    /// Returns [`TimerScanStep::Stop`] for any other command-bearing event
    /// (`ActivityScheduled`/`Completed`/`Failed`, an attached `ChildWorkflow*`,
    /// `LocalActivity*`, ...): a timer outcome/cancel
    /// claim is NOT allowed to cross a real command-ordering point, or a code
    /// change that moved the await/cancel BEFORE such a command would silently
    /// pass strict replay (Codex P2 soundness fix). Factoring this into one shared
    /// helper keeps the two scans' crossable sets provably identical.
    ///
    /// # Same-id `TimerStarted`/`TimerCancelled` is an ANCHOR, not transparent (Codex P2 round 10)
    ///
    /// The scan is id-aware: an UNCONSUMED **same-id** `TimerStarted` (or
    /// `TimerCancelled`) is this timer's own command-ordering anchor — the arm
    /// that must precede its cancel/fire — so the scan STOPS at it. Only a
    /// **foreign** (different-id) sibling timer's arm/cancel lifecycle is
    /// transparent. Without this, strict replay of
    /// `start_timer("idle"); cancel_timer("idle")` with the two lines reordered to
    /// `cancel_timer("idle"); start_timer("idle")` would let
    /// `match_timer_cancel("idle")` skip the unconsumed same-id `TimerStarted`,
    /// claim the later `TimerCancelled`, and then let `start_timer` consume the
    /// start — accepting a real command-order change.
    #[allow(clippy::too_many_lines)]
    fn timer_scan_cross_or_stop(&mut self, scan: usize, timer_id: &str) -> TimerScanStep {
        match &self.events[scan] {
            // Bookkeeping / fire-and-forget — always cross.
            WorkflowEvent::MarkerRecorded { .. }
            | WorkflowEvent::SideEffectRecorded { .. }
            | WorkflowEvent::ChildWorkflowSpawnedDetached { .. } => TimerScanStep::Cross,
            // A FOREIGN sibling timer's arm/cancel/fire interleaving is
            // transparent; an UNCONSUMED SAME-id `TimerStarted`/`TimerCancelled` is
            // this id's own command-ordering anchor and falls through to `_ => Stop`
            // below (Codex P2 round 10). Our-id fire/cancel *targets* are claimed by
            // the caller BEFORE this call, so a same-id one reaching here is a
            // genuine unconsumed ordering point the scan must not cross.
            //
            // A FOREIGN `TimerFired` is a concurrent-await sibling's own outcome
            // (Codex P2 round 14, issue #768): with `tokio::join!(slow.await_fire(),
            // fast.await_fire())` both rows are armed and the worker planner wakes
            // at the minimum deadline, so `fast` may fire first. On replay the
            // `slow` branch is polled first; its outcome scan must CROSS the
            // unconsumed `TimerFired(fast)` NON-CONSUMINGLY (leaving it for `fast`'s
            // own `match_timer_or_cancel` to claim) rather than treating it as an
            // unrelated command and diverging. A sibling timer fire is a legitimate
            // interleaving, not a command-ordering point.
            WorkflowEvent::TimerStarted { timer_id: id, .. }
            | WorkflowEvent::TimerCancelled { timer_id: id }
            | WorkflowEvent::TimerFired { timer_id: id }
                if id.as_str() != timer_id =>
            {
                TimerScanStep::Cross
            }
            // Signals can arrive while a timer is armed, and ExternalSignal /
            // ExternalCancel / ExternalAwait triplets can interleave with an
            // in-flight timer too (issue #492, issue #757); stash them for
            // their own matchers and cross.
            WorkflowEvent::SignalReceived { .. }
            | WorkflowEvent::ExternalSignalRequested { .. }
            | WorkflowEvent::ExternalSignalDelivered { .. }
            | WorkflowEvent::ExternalSignalFailed { .. }
            | WorkflowEvent::ExternalCancelRequested { .. }
            | WorkflowEvent::ExternalCancelDelivered { .. }
            | WorkflowEvent::ExternalCancelFailed { .. }
            | WorkflowEvent::ExternalAwaitRequested { .. }
            | WorkflowEvent::ExternalAwaitResolved { .. }
            | WorkflowEvent::ExternalAwaitFailed { .. } => {
                self.stash_transparent_external_event(scan);
                TimerScanStep::Cross
            }
            // Update events are transparent to the timer scan.
            e if Self::is_update_event(e) => TimerScanStep::Cross,
            // Any other UNCONSUMED command-bearing event is a real ordering point
            // the timer scan is NOT allowed to cross.
            _ => TimerScanStep::Stop,
        }
    }

    /// Observe a cancellable durable timer's outcome (issue #768).
    ///
    /// Forward-scans from the cursor for the **first** unconsumed
    /// `TimerFired { timer_id }` or `TimerCancelled { timer_id }` — whichever
    /// appears first in history decides the outcome (`Fired` vs `Cancelled`),
    /// giving deterministic recorded-order resolution of a genuine fire-vs-cancel
    /// race. Claims the winning event (marks it consumed).
    ///
    /// Returns [`TimerFireMatch::NoMatch`] when neither is recorded yet (the
    /// timer is armed but unresolved, or the cursor is past end): the caller
    /// re-arms idempotently and suspends.
    ///
    /// # Soundness — the scan STOPS at intervening commands (Codex P2, issue #768)
    ///
    /// The forward scan is deliberately **not** a "skip everything until the
    /// outcome" loop. It may cross only (a) already-`is_consumed` events — the
    /// legitimate case where the workflow ran other operations (an activity, a
    /// side effect) BEFORE awaiting the timer, so those events were consumed by
    /// their own `match_*` calls in program order — and (b) a small allowlist of
    /// genuinely transparent / interleavable events (mirroring
    /// [`Self::match_timer_strict`]'s transparent set: markers, side effects,
    /// detached-child spawns, the reset `TimerCancelled`/`TimerStarted`
    /// interleaving, stashed signals & external-signal/cancel triplets, and
    /// update events, and a foreign sibling's `TimerFired`). It STOPS (returns
    /// [`TimerFireMatch::NoMatch`]) at any UNCONSUMED command-bearing event NOT on
    /// that allowlist
    /// (`ActivityScheduled`/`Completed`/`Failed`, attached `ChildWorkflow*`,
    /// `LocalActivity*`, ...). Without the stop, a code
    /// change that awaits the timer BEFORE such a command would let the scan claim
    /// the trailing `TimerFired` across the unrelated command and pass strict
    /// replay despite a real command-order change; stopping surfaces it as a
    /// non-determinism divergence instead. The crossable set is shared verbatim
    /// with [`Self::match_timer_cancel`] via [`Self::timer_scan_cross_or_stop`].
    pub fn match_timer_or_cancel(&mut self, timer_id: &str) -> TimerFireMatch {
        // Reset the blocked-scan flag on entry (see `match_timer_cancel` — Codex
        // P2 round 12, issue #768).
        self.timer_scan_stopped_at_command = false;
        if !self.prepare_match() {
            return TimerFireMatch::NoMatch;
        }
        let mut scan = self.cursor;
        while scan < self.events.len() {
            if self.is_consumed(scan) {
                scan += 1;
                continue;
            }
            // The resolving outcome for THIS timer — claim it (recorded-order
            // fire-vs-cancel resolution). A foreign `TimerFired` (a concurrent
            // `await_fire` sibling's outcome) is delegated to the shared
            // crossable-set helper below, which CROSSES it non-consumingly (Codex P2
            // round 14) so the sibling's own scan claims it; a genuine command-
            // bearing event STOPS the scan there.
            match &self.events[scan] {
                WorkflowEvent::TimerFired { timer_id: id } if id.as_str() == timer_id => {
                    self.consumed_out_of_order_events.insert(scan);
                    self.advance_to_next_unconsumed_event();
                    return TimerFireMatch::Fired;
                }
                WorkflowEvent::TimerCancelled { timer_id: id } if id.as_str() == timer_id => {
                    self.consumed_out_of_order_events.insert(scan);
                    self.advance_to_next_unconsumed_event();
                    return TimerFireMatch::Cancelled;
                }
                _ => {}
            }
            // Any other unconsumed event: cross it if transparent/interleavable,
            // otherwise STOP (a code change that awaited the timer BEFORE such a
            // command would otherwise silently claim a future TimerFired across it
            // and pass strict replay — Codex P2 soundness fix, issue #768). The
            // crossable set is shared verbatim with `match_timer_cancel`.
            match self.timer_scan_cross_or_stop(scan, timer_id) {
                TimerScanStep::Cross => scan += 1,
                TimerScanStep::Stop => {
                    self.timer_scan_stopped_at_command = true;
                    break;
                }
            }
        }
        TimerFireMatch::NoMatch
    }

    /// Match a signal wait command against history.
    ///
    /// Expects `SignalReceived { signal_name }` at the current cursor.
    ///
    /// # Known limitation (issue #1071 / #768 finding 5.1)
    ///
    /// The forward scan of this method (and of `match_timer_strict` /
    /// `scan_activity_terminal`) crosses an interleaved sibling's `TimerFired`
    /// NON-CONSUMINGLY when the awaited signal wins, advancing the cursor PAST
    /// the fire. If that fire belongs to a **cancellable timer** (issue #768:
    /// `ctx.start_timer(...)` + `TimerHandle::await_fire()`) composed with this
    /// plain signal wait in the SAME suspension batch — e.g.
    /// `join!(ctx.wait_for_signal("go"), handle.await_fire())` where the
    /// cancellable timer's `TimerFired` is recorded BEFORE the signal — the
    /// signal-win strands that fire BEHIND the cursor. `await_fire` resolves via
    /// `match_timer_or_cancel`, whose outcome scan only moves FORWARD from the
    /// cursor, so it misses the behind-cursor fire and cannot resolve —
    /// diverging strict `WorkflowReplayer` replay. On a live worker this
    /// self-heals (the arm re-fires), but the composition is unsupported;
    /// use `ctx.receive_signal_timeout` (issue #476) for a signal-or-deadline
    /// shape. Pinned by
    /// `known_limitation_signal_wait_composed_with_cancellable_await_fire_diverges_on_reversed_order`.
    #[allow(clippy::too_many_lines)]
    pub fn match_signal(&mut self, signal_name: &str) -> HistoryMatch {
        self.match_signal_inner(signal_name, None)
    }

    /// [`match_signal`](Self::match_signal) for a **race branch** (issue #950).
    ///
    /// Identical scanning, with one difference: an interleaved event this scan
    /// does not recognise never diverges — it is crossed as a sibling command
    /// and the scan continues, and reaching the end of history without the
    /// signal returns [`HistoryMatch::NoMatch`] rather than a divergence.
    ///
    /// `match_signal`'s divergence-on-stray-event behaviour exists for a *solo*
    /// `wait_for_signal`: if the workflow expected a signal at this cursor and
    /// history holds something else, that is a real code/history disagreement,
    /// and parking on a signal that will never arrive would hang the workflow
    /// forever (issue #768 round 13). Inside a `ctx.race()` neither premise
    /// holds. Sibling branches legitimately record their own dispatch and
    /// terminal events around this one — that is the whole point of a mixed
    /// batch — and a signal branch that simply lost the race has **no recorded
    /// event at all**, which is the normal outcome, not a divergence. A race
    /// branch also cannot hang the workflow on a never-arriving signal: some
    /// other branch resolves it, and the `race_winner:{seq}` marker fixes the
    /// outcome for every later replay.
    /// `settle_marker` is this race's own `race_winner:{seq}` marker name; it
    /// **bounds** the scan. A `SignalReceived` recorded at or after that marker
    /// was delivered once the race had already resolved, so it cannot be this
    /// branch's resolution — it belongs to a later waiter. Without the bound a
    /// *losing* signal branch would scan to end-of-history and silently consume
    /// that signal, leaving the real `ctx.wait_for_signal` that owns it parked on
    /// a signal already delivered.
    pub fn match_race_signal(&mut self, signal_name: &str, settle_marker: &str) -> HistoryMatch {
        self.match_signal_inner(signal_name, Some(settle_marker))
    }

    /// `tolerate_interleaved` carries the race's settlement-marker name when the
    /// caller is a `ctx.race()` branch, and is `None` for a solo
    /// `wait_for_signal`.
    #[allow(clippy::too_many_lines)]
    fn match_signal_inner(
        &mut self,
        signal_name: &str,
        tolerate_interleaved: Option<&str>,
    ) -> HistoryMatch {
        if let Some(index) = self
            .pending_signals
            .iter()
            .position(|(name, _, _)| name == signal_name)
            && let Some((_name, payload, _idx)) = self.pending_signals.remove(index)
        {
            return HistoryMatch::Matched { output: payload };
        }

        self.advance_to_next_unconsumed_event();
        if !self.is_replaying() {
            return HistoryMatch::NoMatch;
        }

        let mut scan_cursor = self.cursor;
        let mut first_interleaved_command = None;
        // `first_interleaved_command` is the rewind target: on a win the cursor
        // returns to the first sibling command crossed, so each one is claimed by
        // its own matcher in program order. It is NOT a verdict — a crossed
        // command may belong to a sibling branch of this decision (a #950 mixed
        // batch, which parks) or be stray (issue #768 round 13, which must
        // nd-block), and this scan runs too early to tell. See the sibling-command
        // arm below for why that verdict is deferred to the end of the cycle.
        while scan_cursor < self.events.len() {
            if self.is_consumed(scan_cursor) {
                scan_cursor += 1;
                continue;
            }

            match &self.events[scan_cursor] {
                WorkflowEvent::SignalReceived {
                    signal_name: recorded_name,
                    payload,
                } if recorded_name == signal_name => {
                    let output = payload.clone();
                    self.consumed_signal_events.insert(scan_cursor);
                    self.cursor =
                        first_interleaved_command.unwrap_or_else(|| scan_cursor.saturating_add(1));
                    self.advance_to_next_unconsumed_event();

                    return HistoryMatch::Matched { output };
                }
                WorkflowEvent::SignalReceived { .. } => {
                    self.stash_transparent_external_event(scan_cursor);
                    scan_cursor += 1;
                }
                ev if Self::is_update_event(ev) => {
                    // Update events are transparent to signal scanning.
                    scan_cursor += 1;
                }
                // A reserved deadline timer (issue #476's
                // `__signal_timeout:{seq}:{name}` convention) for THIS awaited
                // signal — a spent deadline from this wait's own signal-or-deadline
                // composition, recorded earlier in the batch, where the plain
                // signal ultimately won AFTER the deadline event. Cross it WITHOUT
                // setting `first_interleaved_command` (its paired `TimerFired`
                // crosses via the arm below), so the awaited signal recorded after
                // the spent deadline is found and the reserved events end up behind
                // the cursor on the win — never flagging early completion (issue
                // #1071 manifestation #3).
                //
                // The guard is scoped to THIS signal's OWN name. A reserved
                // deadline for a DIFFERENT signal name (e.g. matching
                // `wait_for_signal("a")` while a concurrent
                // `receive_signal_timeout("b", …)` armed `__signal_timeout:N:b`)
                // belongs to that sibling race's own `match_signal_or_timer("b")`,
                // which positionally re-anchors on its `TimerStarted`. Crossing a
                // FOREIGN reserved timer WITHOUT rewind here would advance the
                // cursor past the sibling's reserved events, so its later poll can
                // no longer find its own `TimerStarted` → false EarlyCompletion / a
                // re-armed spent timer (Codex P1 on PR #1084). So a foreign-name
                // reserved timer instead falls through to the generic
                // `TimerStarted` rewind arm below (as does a genuine user
                // `ctx.timer` sibling), which sets `first_interleaved_command` so
                // the sibling race / `match_timer_strict` re-matches after the win.
                //
                // NOTE (issue #1071): this arm assumes the SAME-name reserved
                // deadline is SPENT (the signal won after the deadline event was
                // recorded). It does not disambiguate a same-name race that is
                // still OPEN at this cursor and racing THIS plain wait for the same
                // signal (a plain `wait_for_signal("go")` interleaved with a
                // still-open `receive_signal_timeout("go", …)` for the same name in
                // the same batch) — that composition is an unsupported/exotic shape.
                WorkflowEvent::TimerStarted { timer_id, .. }
                    if Self::signal_timeout_race_name(timer_id.as_str()) == Some(signal_name) =>
                {
                    scan_cursor += 1;
                }
                // Issue #950: a race branch's scan stops at its OWN settlement
                // marker. This arm MUST precede the generic sibling-command arm
                // below, which also matches `MarkerRecorded` and would otherwise
                // swallow the bound — letting a losing signal branch scan past
                // the resolved race and consume a signal belonging to a later
                // `ctx.wait_for_signal`.
                WorkflowEvent::MarkerRecorded { name, .. }
                    if tolerate_interleaved == Some(name.as_str()) =>
                {
                    return HistoryMatch::NoMatch;
                }
                // An interleaved sibling COMMAND. Cross it as an interleaved
                // command (rewind to it on a win, so its own matcher claims it in
                // program order) and keep scanning for the awaited signal.
                //
                // Two histories put a command here and they are NOT
                // distinguishable at this point:
                //
                //   * a sibling branch of THIS decision (issue #950) —
                //     `join!(ctx.wait_for_signal(..), ctx.timer(..))` or
                //     `join!(ctx.wait_for_signal(..), ctx.execute_activity(..))`.
                //     `join!` polls the signal first, so the batch records the
                //     sibling's `TimerStarted`/`ActivityScheduled` and nothing for
                //     the signal wait; on the next drive this scan starts ON that
                //     event, and the sibling's own `match_timer_strict` /
                //     `scan_activity_terminal` claims it later in the same cycle.
                //     The correct outcome is a park.
                //   * a STRAY command left by code that no longer runs (issue #768
                //     round 13, and the `[TimerCancelled, TimerStarted]` pair a
                //     same-cycle `reset()` records). Nothing claims it, and
                //     parking would wait forever on a signal that never arrives.
                //
                // What separates them is not the event, it is whether anything in
                // this decision claims it — which this scan cannot know, because it
                // runs BEFORE the sibling branches are polled. So it crosses
                // everything NON-CONSUMINGLY (each event stays claimable exactly
                // once by `match_timer_cancel` / `match_timer_arm` / etc.) and
                // leaves the verdict to the end of the cycle, where `executor.rs`
                // fails the workflow if `history_has_unconsumed_events()`.
                //
                // Round 13's protection is preserved in OUTCOME — pinned by
                // `interleaved_sibling_signal_stray_timer_started_still_diverges`
                // — while the mixed batch parks, pinned by
                // `mixed_join_signal_timer_and_activity_parks_after_the_activity_resolves`.
                // Deciding here instead is what left an advertised mixed
                // composition nd-blocked on its first wake (Codex round 3 on PR
                // #1245); it is also the choice already made for
                // `wait_for_signal_timeout`'s strict-`Suspended` arm.
                //
                // This completes the "full mixed-batch parity for the signal
                // wait" that issue #1071 scoped out; the sets mirror
                // `match_timer_strict` / `scan_activity_terminal` — keep them in
                // sync.
                WorkflowEvent::ActivityScheduled { .. }
                | WorkflowEvent::ChildWorkflowStarted { .. }
                | WorkflowEvent::LocalActivityScheduled { .. }
                | WorkflowEvent::MarkerRecorded { .. }
                | WorkflowEvent::SideEffectRecorded { .. }
                | WorkflowEvent::ChildWorkflowSpawnedDetached { .. }
                | WorkflowEvent::TimerCancelled { .. }
                | WorkflowEvent::TimerStarted { .. } => {
                    first_interleaved_command.get_or_insert(scan_cursor);
                    scan_cursor += 1;
                }
                // Progress and terminal events of those concurrent siblings —
                // and a foreign timer's fire (a sibling deadline that fired
                // before the awaited signal arrived, e.g. a concurrent
                // `receive_signal_timeout`/`ctx.timer` in the same batch whose
                // `TimerFired` is recorded ahead of the delivered
                // `SignalReceived`, issue #1071 manifestation #3) — are
                // transparent to this scan. Cross them NON-CONSUMINGLY, leaving
                // each for its own matcher to claim, and keep scanning for the
                // awaited signal. The same treatment `match_timer_strict` and
                // `scan_activity_terminal` give their sibling terminals; keep the
                // three sets in sync (issue #1071 / #950).
                //
                // Crossing non-consumingly is what keeps round 13's protection
                // intact without a scan-time guess: a lone stale `TimerFired` (or
                // any other event here) that no branch of this decision claims is
                // still unconsumed at suspend and nd-blocks there.
                WorkflowEvent::ActivityStarted { .. }
                | WorkflowEvent::ActivityHeartbeat { .. }
                | WorkflowEvent::ActivityCompleted { .. }
                | WorkflowEvent::ActivityFailed { .. }
                | WorkflowEvent::ActivityTimedOut { .. }
                | WorkflowEvent::ChildWorkflowCompleted { .. }
                | WorkflowEvent::ChildWorkflowFailed { .. }
                | WorkflowEvent::LocalActivityCompleted { .. }
                | WorkflowEvent::LocalActivityFailed { .. }
                | WorkflowEvent::TimerFired { .. } => {
                    scan_cursor += 1;
                }
                // ExternalSignal event triplets can appear before SignalReceived
                // when a mixed batch (e.g. tokio::join!(wait_for_signal, signal_external))
                // wrote signal events first, and ExternalCancel/ExternalAwait
                // triplets are likewise transparent here (issue #492, issue
                // #757). Stash them for their own matchers.
                WorkflowEvent::ExternalSignalRequested { .. }
                | WorkflowEvent::ExternalSignalDelivered { .. }
                | WorkflowEvent::ExternalSignalFailed { .. }
                | WorkflowEvent::ExternalCancelRequested { .. }
                | WorkflowEvent::ExternalCancelDelivered { .. }
                | WorkflowEvent::ExternalCancelFailed { .. }
                | WorkflowEvent::ExternalAwaitRequested { .. }
                | WorkflowEvent::ExternalAwaitResolved { .. }
                | WorkflowEvent::ExternalAwaitFailed { .. } => {
                    self.stash_transparent_external_event(scan_cursor);
                    scan_cursor += 1;
                }
                other => {
                    // Issue #950: inside a race, an unrecognised event is a
                    // sibling branch's own command/terminal, not a divergence —
                    // cross it as an interleaved command (so a later win rewinds
                    // the cursor to it for its own claimer) and keep scanning,
                    // bounded by the race's own settlement marker.
                    //
                    // Checked BEFORE the `is_some()` early-return below. That
                    // guard is what makes the SOLO wait stop after one crossed
                    // event, and since this arm is what SETS
                    // `first_interleaved_command`, ordering it second would let a
                    // race branch cross exactly one event and then bail — while
                    // the normal shape is two (`ActivityScheduled` +
                    // `ActivityStarted`) before the signal.
                    // The settlement-marker bound is applied by its own arm
                    // above, ahead of the sibling-command arm.
                    if tolerate_interleaved.is_some() {
                        first_interleaved_command.get_or_insert(scan_cursor);
                        scan_cursor += 1;
                        continue;
                    }
                    if first_interleaved_command.is_some() {
                        return HistoryMatch::NoMatch;
                    }
                    return HistoryMatch::Diverged {
                        expected: format!("SignalReceived({signal_name})"),
                        actual: Self::actual_event_name(other),

                        event_index: i32::try_from(self.cursor).ok(),
                    };
                }
            }
        }

        // The signal scan reached the end of history without finding the
        // signal: the awaited signal has not arrived yet, so this wait parks.
        //
        // Any interleaved command crossed on the way (a sibling's
        // `ActivityScheduled`/`ChildWorkflowStarted`, or a `TimerStarted`/
        // `TimerCancelled`/detached spawn) was crossed NON-CONSUMINGLY, so it is
        // left for its own claimer. That is what makes returning `NoMatch` here
        // safe for BOTH readings of a crossed command:
        //
        //   * a sibling branch of this decision claims it later in the same
        //     cycle (the #950 mixed batch) → the frontier is clean when the body
        //     suspends and the park is correct;
        //   * nothing claims it (a stray left by code that no longer runs, issue
        //     #768 round 13) → it is still unconsumed at suspend, and
        //     `executor.rs`'s `history_has_unconsumed_events()` guard nd-blocks
        //     the workflow instead of letting it park forever on a signal that
        //     will never arrive.
        //
        // Deciding that here instead would mean guessing, since this scan runs
        // before the sibling branches of the same decision are polled — which is
        // exactly how an advertised `join!(wait_for_signal, ctx.timer)` came to
        // be nd-blocked on its first wake (Codex round 3 on PR #1245).
        HistoryMatch::NoMatch
    }

    /// Cursor-bound claim for push-based signal handler dispatch (issue #546).
    ///
    /// Unlike [`match_signal`](Self::match_signal), which resolves a single
    /// pull-based wait at the current cursor position, this claims **every**
    /// currently-stashed `SignalReceived { signal_name }` payload not yet
    /// claimed by anything else, in ascending event order. "Currently
    /// stashed" is the key constraint: this method calls
    /// [`prepare_match`](Self::prepare_match) (the same cursor-advancing,
    /// signal-draining sweep every other `match_*` method opens with) and
    /// then only inspects `pending_signals` -- it never reaches ahead of
    /// wherever the workflow's own code-driven cursor progression has
    /// carried the matcher so far.
    ///
    /// This is deliberate and is the fix for a real production bug (issue
    /// #546 post-ship hardening): an earlier version of this method also
    /// indexed and claimed *every* recorded `SignalReceived` for `name`
    /// regardless of cursor position, so a handler registered at the top of
    /// a workflow function could fire on a signal recorded *after* an
    /// activity or timer the workflow hadn't reached yet in this replay
    /// cycle -- silently reordering observable side effects relative to
    /// history. Because `prepare_match`'s `drain_early_signals` sweep halts
    /// at the first non-transparent event (an `ActivityScheduled`,
    /// `TimerStarted`, etc. not yet consumed), a signal recorded after such
    /// an event is invisible here until whatever `match_*` call the
    /// workflow body actually makes for that event advances the cursor past
    /// it -- exactly mirroring `wait_for_signal`'s own history-order
    /// contract.
    ///
    /// Claiming marks the returned event indices consumed, so a later
    /// `wait_for_signal`/`receive_signal` call for the same name will not see
    /// them again, and vice versa — the two consumption styles never
    /// double-deliver a single `SignalReceived` event. Calling this again
    /// immediately (with no intervening cursor advancement) for the same
    /// `signal_name` returns an empty `Vec`.
    ///
    /// An event reserved for an open signal-or-deadline race for the same
    /// name (see [`Self::race_reserved_signal_events`]) is never claimed
    /// here, regardless of call order: a push handler must not be able to
    /// silently steal the signal a concurrent `receive_signal_timeout` /
    /// `wait_for_signal_timeout` race is waiting to resolve on. In practice
    /// the race's own unconsumed `TimerStarted` already blocks the cursor
    /// from reaching that signal at all (see the field doc), so this is a
    /// second, independent layer of protection rather than the only one.
    ///
    /// Returns `(event_index, payload)` pairs -- not just payloads -- so a
    /// caller dispatching to *multiple* differently-named handlers in one
    /// pump (as [`WorkflowContext`](crate::context::WorkflowContext) does)
    /// can sort by index to dispatch in true historical order across
    /// handler names, not just within one name.
    pub(crate) fn claim_pending_signal(&mut self, signal_name: &str) -> Vec<(usize, Value)> {
        self.prepare_match();

        let (matched, remaining): (VecDeque<_>, VecDeque<_>) =
            std::mem::take(&mut self.pending_signals)
                .into_iter()
                .partition(|(name, _, idx)| {
                    name == signal_name && !self.race_reserved_signal_events.contains(idx)
                });
        self.pending_signals = remaining;
        // Every stashed entry already has its index in `consumed_signal_events`
        // (set by `stash_signal` at stash time), but re-asserting it here makes
        // the no-double-delivery invariant explicit at the point of use rather
        // than relying on the reader to trace it back to the stash call site.
        for (_, _, idx) in &matched {
            self.consumed_signal_events.insert(*idx);
        }
        matched
            .into_iter()
            .map(|(_, payload, idx)| (idx, payload))
            .collect()
    }

    /// Cursor-bound claim of the **single oldest** buffered signal for
    /// non-blocking drain (issue #775).
    ///
    /// This is the single-occurrence sibling of
    /// [`claim_pending_signal`](Self::claim_pending_signal) and the matcher
    /// engine for [`WorkflowContext::try_receive_signal`](crate::context::WorkflowContext::try_receive_signal).
    /// Like `match_signal`'s fast path it removes exactly one entry from
    /// `pending_signals`, but it adds the same two guards `claim_pending_signal`
    /// carries: it first runs [`prepare_match`](Self::prepare_match) (the
    /// cursor-advancing, signal-draining sweep every `match_*` method opens
    /// with) so a signal recorded at — but not yet drained to — the current
    /// cursor is visible, and it skips any event reserved for an open
    /// signal-or-deadline race (see [`Self::race_reserved_signal_events`]).
    ///
    /// Crucially, because it only inspects `pending_signals` after
    /// `prepare_match`, it can never reach ahead of the workflow's own
    /// code-driven cursor position: a signal recorded *after* an unconsumed
    /// activity/timer is invisible until the workflow's own `match_*` call for
    /// that event advances the cursor past it. It **never** falls through to a
    /// suspension — a `None` return means "nothing buffered right now", not
    /// "park until a signal arrives".
    ///
    /// The claimed event's index is marked consumed, so a later
    /// `match_signal`/`claim_pending_signal` call for the same name (and vice
    /// versa) will not re-deliver it.
    pub(crate) fn try_claim_pending_signal(&mut self, signal_name: &str) -> Option<Value> {
        self.prepare_match();
        let index = self.pending_signals.iter().position(|(name, _, idx)| {
            name == signal_name && !self.race_reserved_signal_events.contains(idx)
        })?;
        let (_name, payload, idx) = self.pending_signals.remove(index)?;
        // Already inserted by `stash_signal`, but re-asserting here makes the
        // no-double-delivery invariant explicit at the point of use (mirrors
        // `claim_pending_signal`).
        self.consumed_signal_events.insert(idx);
        Some(payload)
    }

    /// Settle the bookkeeping for a signal-branch win of a signal-or-deadline
    /// race (issue #476): consume the winning `SignalReceived` event, consume
    /// the stray `TimerFired` of the race timer if it is already recorded (if
    /// the timer fires only after this replay cycle, the next full replay
    /// re-runs the same scan and consumes it then), and settle the cursor on
    /// the first interleaved sibling command, or just past the winning signal.
    fn settle_race_signal_won(
        &mut self,
        signal_pos: usize,
        first_interleaved_command: Option<usize>,
        timer_id: &str,
    ) {
        self.consumed_signal_events.insert(signal_pos);
        let mut fired_scan = signal_pos + 1;
        while fired_scan < self.events.len() {
            if !self.is_consumed(fired_scan)
                && let WorkflowEvent::TimerFired { timer_id: id } = &self.events[fired_scan]
                && id.as_str() == timer_id
            {
                self.consumed_out_of_order_events.insert(fired_scan);
                break;
            }
            fired_scan += 1;
        }
        self.cursor = first_interleaved_command.unwrap_or_else(|| signal_pos.saturating_add(1));
        self.advance_to_next_unconsumed_event();
    }

    /// Match a signal-vs-deadline race against history (issue #476).
    ///
    /// The race composes the existing `TimerStarted`/`TimerFired` and
    /// `SignalReceived` events — no new event variant. The winner is the
    /// resolution event that appears **first in recorded history**:
    ///
    /// - A `SignalReceived { signal_name }` before `TimerFired { timer_id }`
    ///   (or a signal that was stashed/recorded before the race even started)
    ///   → [`SignalOrTimerMatch::SignalWon`]. A stray `TimerFired` for the
    ///   race's timer that lands later in history is marked consumed so
    ///   subsequent matches do not diverge against it.
    /// - A `TimerFired { timer_id }` before any matching signal
    ///   → [`SignalOrTimerMatch::TimerWon`]. A matching signal recorded
    ///   *after* the fire is **not** consumed: it stays observable by a
    ///   subsequent signal wait.
    ///
    /// Non-matching signals and external-signal triplets encountered during
    /// the scan are stashed exactly like in [`Self::match_timer_strict`].
    #[allow(clippy::too_many_lines)]
    pub fn match_signal_or_timer(
        &mut self,
        signal_name: &str,
        timer_id: &str,
        expected_duration: Option<u64>,
    ) -> SignalOrTimerMatch {
        let replaying = self.prepare_match();

        // A signal stashed by an earlier scan — or drained by prepare_match
        // just now — whose recorded position precedes this race point arrived
        // before the race started: the signal wins and the timer was never
        // started on the matching live run. Stashed signals recorded at or
        // after the race point must NOT short-circuit here — history order
        // decides, so they compete at their recorded index during the scan
        // below.
        let race_pos = self.cursor;
        if let Some(index) = self
            .pending_signals
            .iter()
            .position(|(name, _, idx)| name == signal_name && *idx < race_pos)
            && let Some((_name, payload, _idx)) = self.pending_signals.remove(index)
        {
            return SignalOrTimerMatch::SignalWon { payload };
        }

        if !replaying {
            return SignalOrTimerMatch::NoMatch;
        }

        let WorkflowEvent::TimerStarted {
            timer_id: recorded_id,
            duration_secs: recorded_duration,
        } = &self.events[self.cursor]
        else {
            return SignalOrTimerMatch::Diverged {
                expected: format!("TimerStarted({timer_id})"),
                actual: Self::actual_event_name(&self.events[self.cursor]),

                event_index: i32::try_from(self.cursor).ok(),
            };
        };

        if recorded_id.as_str() != timer_id {
            return SignalOrTimerMatch::Diverged {
                expected: format!("TimerStarted({timer_id})"),
                actual: format!("TimerStarted({recorded_id})"),

                event_index: i32::try_from(self.cursor).ok(),
            };
        }

        if let Some(expected) = expected_duration
            && *recorded_duration != expected
        {
            return SignalOrTimerMatch::Diverged {
                expected: format!("TimerStarted({timer_id}, duration={expected}s)"),
                actual: format!("TimerStarted({recorded_id}, duration={recorded_duration}s)"),

                event_index: i32::try_from(self.cursor).ok(),
            };
        }

        // Advance past TimerStarted, then scan for the first resolution event.
        self.cursor += 1;
        let mut scan_cursor = self.cursor;
        let mut first_interleaved_command = None;

        while scan_cursor < self.events.len() {
            if self.is_consumed(scan_cursor) {
                // A matching signal stashed by a sibling scan (consumed but
                // still undelivered in the pending buffer) competes at its
                // recorded position: if it precedes the race's TimerFired in
                // history, the signal branch wins.
                if let Some(index) = self
                    .pending_signals
                    .iter()
                    .position(|(name, _, idx)| *idx == scan_cursor && name == signal_name)
                    && let Some((_name, payload, _idx)) = self.pending_signals.remove(index)
                {
                    self.settle_race_signal_won(scan_cursor, first_interleaved_command, timer_id);
                    return SignalOrTimerMatch::SignalWon { payload };
                }
                scan_cursor += 1;
                continue;
            }

            match &self.events[scan_cursor] {
                WorkflowEvent::SignalReceived {
                    signal_name: recorded_name,
                    payload,
                } if recorded_name == signal_name => {
                    let payload = payload.clone();
                    self.settle_race_signal_won(scan_cursor, first_interleaved_command, timer_id);
                    return SignalOrTimerMatch::SignalWon { payload };
                }

                WorkflowEvent::TimerFired { timer_id: id } if id.as_str() == timer_id => {
                    // The first matching signal recorded after this fire is
                    // the exact event that lost the race. It stays deliverable
                    // to a later signal wait, but if the timeout branch
                    // intentionally ignores it, the completed history must
                    // still pass the strict unconsumed check. Each race claims
                    // a distinct loser, so skip indices already claimed.
                    let loser_index = (scan_cursor + 1..self.events.len()).find(|idx| {
                        !self.late_race_signal_events.contains(idx)
                            && matches!(
                                &self.events[*idx],
                                WorkflowEvent::SignalReceived { signal_name: n, .. } if n == signal_name
                            )
                    });
                    if let Some(idx) = loser_index {
                        self.late_race_signal_events.insert(idx);
                    }
                    if let Some(command_cursor) = first_interleaved_command {
                        self.consumed_out_of_order_events.insert(scan_cursor);
                        self.cursor = command_cursor;
                    } else {
                        self.cursor = scan_cursor + 1;
                    }
                    self.advance_to_next_unconsumed_event();
                    return SignalOrTimerMatch::TimerWon;
                }

                // Other signals can arrive while the race is pending; stash
                // them for later signal waits and continue scanning.
                WorkflowEvent::SignalReceived { .. } => {
                    self.stash_transparent_external_event(scan_cursor);
                    scan_cursor += 1;
                }

                ev if Self::is_update_event(ev) => {
                    scan_cursor += 1;
                }

                // Concurrent commands (tokio::join! siblings) can interleave
                // with the pending race. Keep the first one as the next replay
                // cursor — mirroring scan_activity_terminal — and scan past it
                // so a resolution event recorded later is still found.
                WorkflowEvent::ChildWorkflowStarted { .. }
                | WorkflowEvent::ChildWorkflowSpawnedDetached { .. }
                | WorkflowEvent::ActivityScheduled { .. }
                | WorkflowEvent::LocalActivityScheduled { .. }
                | WorkflowEvent::MarkerRecorded { .. }
                | WorkflowEvent::SideEffectRecorded { .. }
                | WorkflowEvent::TimerStarted { .. }
                // A cancellable-timer cancel (issue #768) interleaved with the
                // race is an interleaved command: rewind to it after the race
                // settles so its own claimer can re-scan, rather than skipping
                // past it (which would make it unreachable).
                | WorkflowEvent::TimerCancelled { .. } => {
                    first_interleaved_command.get_or_insert(scan_cursor);
                    scan_cursor += 1;
                }

                // Progress and terminal events of those concurrent siblings
                // (and fires of foreign timers) are transparent to the race
                // scan — their own matchers consume them after the rewind.
                WorkflowEvent::ActivityStarted { .. }
                | WorkflowEvent::ActivityHeartbeat { .. }
                | WorkflowEvent::ActivityCompleted { .. }
                | WorkflowEvent::ActivityFailed { .. }
                | WorkflowEvent::ActivityTimedOut { .. }
                | WorkflowEvent::LocalActivityCompleted { .. }
                | WorkflowEvent::LocalActivityFailed { .. }
                | WorkflowEvent::ChildWorkflowCompleted { .. }
                | WorkflowEvent::ChildWorkflowFailed { .. }
                | WorkflowEvent::TimerFired { .. } => {
                    scan_cursor += 1;
                }

                // ExternalSignal event triplets can be interleaved with the
                // pending race, and ExternalCancel/ExternalAwait triplets are
                // likewise transparent to signal-or-timer race scans (issue
                // #492, issue #757). Stash them for their own matchers.
                WorkflowEvent::ExternalSignalRequested { .. }
                | WorkflowEvent::ExternalSignalDelivered { .. }
                | WorkflowEvent::ExternalSignalFailed { .. }
                | WorkflowEvent::ExternalCancelRequested { .. }
                | WorkflowEvent::ExternalCancelDelivered { .. }
                | WorkflowEvent::ExternalCancelFailed { .. }
                | WorkflowEvent::ExternalAwaitRequested { .. }
                | WorkflowEvent::ExternalAwaitResolved { .. }
                | WorkflowEvent::ExternalAwaitFailed { .. } => {
                    self.stash_transparent_external_event(scan_cursor);
                    scan_cursor += 1;
                }

                _ => break,
            }
        }

        // Timer started but neither resolution event is recorded yet. Rewind
        // to the first interleaved command so a concurrent sibling's matcher
        // still finds its own events.
        if let Some(command_cursor) = first_interleaved_command {
            self.cursor = command_cursor;
            self.advance_to_next_unconsumed_event();
        }
        SignalOrTimerMatch::InProgress
    }

    /// Match a continue-as-new command against history.
    ///
    /// Expects `WorkflowContinuedAsNew { input, new_workflow_type }` at the
    /// current cursor.
    ///
    /// Both the input **and** the successor type are part of the recorded
    /// command (issue #803): a code change that redirects a continuation to a
    /// different workflow type — or that adds/removes the cross-type call
    /// entirely — surfaces as non-determinism rather than silently forking the
    /// chain into a different phase on replay. `None` (same type) and
    /// `Some(t)` are distinct recorded intents and never compare equal.
    pub fn match_continue_as_new(
        &mut self,
        input: &Value,
        new_workflow_type: Option<&str>,
    ) -> HistoryMatch {
        if !self.prepare_match() {
            return HistoryMatch::NoMatch;
        }

        let WorkflowEvent::WorkflowContinuedAsNew {
            input: recorded_input,
            new_workflow_type: recorded_type,
            ..
        } = &self.events[self.cursor]
        else {
            return HistoryMatch::Diverged {
                expected: format!("WorkflowContinuedAsNew({input})"),
                actual: Self::actual_event_name(&self.events[self.cursor]),

                event_index: i32::try_from(self.cursor).ok(),
            };
        };

        if recorded_type.as_deref() != new_workflow_type {
            return HistoryMatch::Diverged {
                expected: format!(
                    "WorkflowContinuedAsNewType({})",
                    new_workflow_type.unwrap_or("<same type>")
                ),
                actual: format!(
                    "WorkflowContinuedAsNewType({})",
                    recorded_type.as_deref().unwrap_or("<same type>")
                ),

                event_index: i32::try_from(self.cursor).ok(),
            };
        }

        if recorded_input != input {
            return HistoryMatch::Diverged {
                expected: format!("WorkflowContinuedAsNewInput({input})"),
                actual: format!("WorkflowContinuedAsNewInput({recorded_input})"),

                event_index: i32::try_from(self.cursor).ok(),
            };
        }

        let output = recorded_input.clone();
        self.cursor += 1;
        self.advance_to_next_unconsumed_event();
        HistoryMatch::Matched { output }
    }

    /// Whether the recorded continue-as-new target type equals `expected`
    /// (issue #803).
    ///
    /// Cursor-independent (it scans, rather than reading at the cursor) so it
    /// can be consulted *after* [`Self::match_continue_as_new`] has advanced
    /// past the event. Exists solely to back a `debug_assert!` on the
    /// `Matched` arm, where the successor type is emitted from the live
    /// argument rather than echoed from history; a history with no recorded
    /// continuation reports `true` (there is nothing to contradict).
    pub(crate) fn recorded_continue_as_new_type_matches(&self, expected: Option<&str>) -> bool {
        self.events
            .iter()
            .find_map(|e| match e {
                WorkflowEvent::WorkflowContinuedAsNew {
                    new_workflow_type, ..
                } => Some(new_workflow_type.as_deref() == expected),
                _ => None,
            })
            .unwrap_or(true)
    }

    /// Match a child workflow command against history.
    ///
    /// Expects `ChildWorkflowStarted { workflow_name }` at cursor, then scans for
    /// a terminal `ChildWorkflowCompleted` or `ChildWorkflowFailed` with the same
    /// `child_id`.
    pub fn match_child_workflow(&mut self, workflow_name: &str, input: &Value) -> HistoryMatch {
        if !self.prepare_match() {
            return HistoryMatch::NoMatch;
        }

        let start_cursor = self.cursor;
        let WorkflowEvent::ChildWorkflowStarted {
            child_id,
            workflow_name: recorded_name,
            input: recorded_input,
        } = &self.events[self.cursor]
        else {
            return HistoryMatch::Diverged {
                expected: format!("ChildWorkflowStarted({workflow_name})"),
                actual: Self::actual_event_name(&self.events[self.cursor]),

                event_index: i32::try_from(self.cursor).ok(),
            };
        };
        let child_id = *child_id;

        if recorded_name != workflow_name {
            return HistoryMatch::Diverged {
                expected: format!("ChildWorkflowStarted({workflow_name})"),
                actual: format!("ChildWorkflowStarted({recorded_name})"),

                event_index: i32::try_from(self.cursor).ok(),
            };
        }
        if recorded_input != input {
            return HistoryMatch::Diverged {
                expected: format!("ChildWorkflowInput({input})"),
                actual: format!("ChildWorkflowInput({recorded_input})"),

                event_index: i32::try_from(self.cursor).ok(),
            };
        }

        let mut scan_cursor = self.cursor + 1;

        while scan_cursor < self.events.len() {
            match &self.events[scan_cursor] {
                WorkflowEvent::ChildWorkflowCompleted {
                    child_id: id,
                    output,
                } if *id == child_id => {
                    let output = output.clone();
                    self.consumed_out_of_order_events.insert(scan_cursor);
                    self.cursor = start_cursor + 1;
                    self.advance_to_next_unconsumed_event();
                    return HistoryMatch::Matched { output };
                }
                WorkflowEvent::ChildWorkflowFailed {
                    child_id: id,
                    error,
                    error_type,
                    details,
                    non_retryable,
                } if *id == child_id => {
                    // Carry the child's typed failure fields (issue #767) through
                    // to the parent's `HistoryMatch::Failed`. A legacy / untyped
                    // child failure decodes to the `"Error"` sentinel with no
                    // details and `non_retryable = false`.
                    let error = error.clone();
                    let error_type = error_type.clone().unwrap_or_else(|| "Error".to_string());
                    let details = details.clone();
                    let non_retryable = non_retryable.unwrap_or(false);
                    self.consumed_out_of_order_events.insert(scan_cursor);
                    self.cursor = start_cursor + 1;
                    self.advance_to_next_unconsumed_event();
                    return HistoryMatch::Failed {
                        error,
                        attempt: 1,
                        error_type,
                        details,
                        non_retryable,
                    };
                }
                _ => scan_cursor += 1,
            }
        }

        // The start event was found and name+input matched, but the terminal
        // hasn't arrived yet.  This is the normal state when the parent wakes
        // after one of several parallel children completes while this child is
        // still running.  Return ChildInProgress so the caller can re-emit a
        // StartChildWorkflow command with the existing child_id rather than
        // treating the incomplete history as a non-determinism error.
        self.cursor = start_cursor + 1;
        self.advance_to_next_unconsumed_event();
        HistoryMatch::ChildInProgress { child_id }
    }

    /// Settle a child-vs-deadline race won by the child (issue #779).
    ///
    /// Marks the winning child terminal consumed (it is matched out of order),
    /// strays the race's deadline `TimerFired` if it was recorded after the
    /// child won (so a subsequent match does not diverge against it and the
    /// strict unconsumed check passes), and advances the cursor. Mirrors
    /// [`Self::settle_race_signal_won`] with the winner role swapped from a
    /// signal to a child terminal.
    fn settle_race_child_won(
        &mut self,
        terminal_pos: usize,
        resume_pos: usize,
        first_interleaved_command: Option<usize>,
        timer_id: &str,
    ) {
        self.consumed_out_of_order_events.insert(terminal_pos);
        let mut fired_scan = terminal_pos + 1;
        while fired_scan < self.events.len() {
            if !self.is_consumed(fired_scan)
                && let WorkflowEvent::TimerFired { timer_id: id } = &self.events[fired_scan]
                && id.as_str() == timer_id
            {
                self.consumed_out_of_order_events.insert(fired_scan);
                break;
            }
            fired_scan += 1;
        }
        // Intentional divergence from `settle_race_signal_won`, which resumes at
        // `signal_pos + 1` (just past the winning signal). Here the child/timer
        // start pair was matched positionally, so the cursor rewinds to
        // `resume_pos` (the first event after the started pair) and
        // `advance_to_next_unconsumed_event` then skips the just-consumed winning
        // terminal (marked above) plus any consumed interleaved events. Verified
        // equivalent: the winning terminal is `consumed_out_of_order_events`, so
        // both forms land the cursor at the same next-unconsumed event.
        self.cursor = first_interleaved_command.unwrap_or(resume_pos);
        self.advance_to_next_unconsumed_event();
    }

    /// Scan forward from `from` for the losing child's terminal
    /// (`ChildWorkflowCompleted`/`ChildWorkflowFailed` with `child_id`) after a
    /// deadline timer won the race (issue #779). Marks it consumed (a losing
    /// child terminal is deliverable to nobody, so it must be transparent to
    /// [`Self::has_non_lifecycle_unconsumed`]) and returns whether it was found.
    fn consume_loser_child_terminal(&mut self, from: usize, child_id: ExecutionId) -> bool {
        let mut scan = from;
        while scan < self.events.len() {
            if !self.is_consumed(scan)
                && matches!(
                    &self.events[scan],
                    WorkflowEvent::ChildWorkflowCompleted { child_id: id, .. }
                        | WorkflowEvent::ChildWorkflowFailed { child_id: id, .. }
                        if *id == child_id
                )
            {
                self.consumed_out_of_order_events.insert(scan);
                return true;
            }
            scan += 1;
        }
        false
    }

    /// After a `ctx.race()` (issue #600) records its winner on the first
    /// resolving cycle (the else branch of `WorkflowContext::settle_race`),
    /// consume the still in-flight LOSER branches' progress-frontier events so
    /// the matcher cursor advances past them, letting a follow-on positional
    /// command in the SAME decision cycle land cleanly (issue #1126).
    ///
    /// The winner's terminal (and its own `ActivityStarted`) are already
    /// consumed by its own `match_activity`. The loser branches, however,
    /// resolved as `ActivityInProgress` this cycle — their synthetic
    /// `ActivityFailed` (from the pending `CancelRaceLosers` command) is
    /// appended only at *persist* time and is invisible to this cycle's
    /// matcher, so their in-flight `ActivityStarted` / `ActivityHeartbeat`
    /// (and, defensively, a losing child's `ChildWorkflowStarted`) are left
    /// unconsumed straddling the cursor. Without consuming them, the next
    /// positional `match_*` call diverges on the leftover event -> nd-block.
    ///
    /// Consumes ONLY events belonging to the given loser ids, so it is safe
    /// under `futures::join!(race1, race2)` interleaving (a sibling race's
    /// events are left untouched). Called ONLY on the first winner-recording
    /// cycle; later replays take `settle_race`'s `peek_u64_marker` branch,
    /// where each loser resolves via its recorded terminal and everything is
    /// already positioned correctly (so this is never invoked then).
    pub(crate) fn consume_race_loser_frontier(
        &mut self,
        loser_activity_ids: &[ActivityExecId],
        loser_child_ids: &[ExecutionId],
    ) {
        if loser_activity_ids.is_empty() && loser_child_ids.is_empty() {
            return;
        }
        let mut scan = self.cursor;
        while scan < self.events.len() {
            if !self.is_consumed(scan) {
                match &self.events[scan] {
                    WorkflowEvent::ActivityStarted { activity_id, .. }
                    | WorkflowEvent::ActivityHeartbeat { activity_id, .. }
                        if loser_activity_ids.contains(activity_id) =>
                    {
                        self.consumed_out_of_order_events.insert(scan);
                    }
                    // Defensive: a losing CHILD branch resolves as
                    // `ChildInProgress`, whose `match_child_workflow` already
                    // advances the cursor PAST the child's own
                    // `ChildWorkflowStarted` (a child has no separate started
                    // event to strand the way an activity does), so in practice
                    // this arm is unreachable today. Kept so a future child-race
                    // shape that leaves a child's start unconsumed is handled
                    // by the same in-cycle consume rather than nd-blocking.
                    WorkflowEvent::ChildWorkflowStarted { child_id, .. }
                        if loser_child_ids.contains(child_id) =>
                    {
                        self.consumed_out_of_order_events.insert(scan);
                    }
                    _ => {}
                }
            }
            scan += 1;
        }
        self.advance_to_next_unconsumed_event();
    }

    /// Read-only peek: does a recorded `ChildWorkflowStarted` already sit at the
    /// cursor a [`Self::match_child_or_timer`] call would land on (issue #779)?
    ///
    /// Used by [`crate::context::WorkflowContext::spawn_child_workflow_timeout`]
    /// to decide, *before* calling `match_child_or_timer`, whether this call is a
    /// **fresh dispatch** (no recorded child-timeout start yet) so the
    /// payload-cap pre-check can run first — mirroring the fan-out's
    /// [`peek_fan_out_count`](crate::context::WorkflowContext) marker peek. On a
    /// live over-cap call the child is never dispatched, so no child/timer events
    /// are recorded; on replay of that history the matcher would otherwise see
    /// the next real event (`WorkflowFailed`, or a caught-and-continued activity)
    /// at the cursor and report a spurious non-determinism instead of
    /// reproducing the same `PayloadTooLarge`. Gating the cap check to a fresh
    /// dispatch also keeps an already-recorded child (under-cap at record time)
    /// from being re-judged against a possibly-changed cap.
    ///
    /// It never mutates the cursor and never consumes anything —
    /// `match_child_or_timer` remains the sole authority for resolving the race.
    /// The skip set mirrors [`Self::drain_early_signals`] so the peek agrees with
    /// where the real match will land even when signals or update events precede
    /// the recorded child start.
    ///
    /// The recorded start is **fingerprinted by `expected_timer_id`** (the call's
    /// `__child_timeout:{seq}:{name}` deadline timer, unique per call site because
    /// `seq` is a burned per-context counter). Without that fingerprint, a caught
    /// over-cap call — which records **nothing** — would peek the *next*
    /// child-timeout call's `ChildWorkflowStarted` at the cursor, wrongly conclude
    /// it was already dispatched, skip its own payload-cap re-check, and then
    /// diverge (spurious non-determinism) instead of reproducing the same
    /// `PayloadTooLarge`. This mirrors exactly how the fan-out's seq-keyed
    /// `fan_out:{seq}` marker peek keeps a caught call from being miscredited the
    /// *following* call's marker (issue #779, Codex round-12 P2).
    #[must_use]
    pub(crate) fn peek_child_start_at_cursor(&self, expected_timer_id: &str) -> bool {
        // Advance `cursor` past consumed indices and the leading run of
        // early-drainable signal / external-signal / external-cancel / update
        // events, exactly as `prepare_match` -> `drain_early_signals` will before
        // reading the structural event at the cursor. Returns `false` when the
        // history end is reached without a structural event.
        let skip_to_structural = |cursor: &mut usize| -> bool {
            while *cursor < self.events.len() {
                let ev = &self.events[*cursor];
                if self.is_consumed(*cursor)
                    || matches!(
                        ev,
                        WorkflowEvent::SignalReceived { .. }
                            | WorkflowEvent::ExternalSignalRequested { .. }
                            | WorkflowEvent::ExternalSignalDelivered { .. }
                            | WorkflowEvent::ExternalSignalFailed { .. }
                            | WorkflowEvent::ExternalCancelRequested { .. }
                            | WorkflowEvent::ExternalCancelDelivered { .. }
                            | WorkflowEvent::ExternalCancelFailed { .. }
                            | WorkflowEvent::ExternalAwaitRequested { .. }
                            | WorkflowEvent::ExternalAwaitResolved { .. }
                            | WorkflowEvent::ExternalAwaitFailed { .. }
                    )
                    || Self::is_update_event(ev)
                {
                    *cursor += 1;
                    continue;
                }
                return true;
            }
            false
        };

        let mut cursor = self.cursor;
        if !skip_to_structural(&mut cursor) {
            return false;
        }
        // The structural event at the cursor must be a ChildWorkflowStarted...
        if !matches!(
            self.events[cursor],
            WorkflowEvent::ChildWorkflowStarted { .. }
        ) {
            return false;
        }
        // ...immediately followed by THIS call's deadline TimerStarted. The worker
        // persists the `[ChildWorkflowStarted, TimerStarted]` pair adjacently in
        // one transaction, so the fingerprint check lands on the correct pair.
        cursor += 1;
        if !skip_to_structural(&mut cursor) {
            return false;
        }
        matches!(
            &self.events[cursor],
            WorkflowEvent::TimerStarted { timer_id, .. } if timer_id.as_str() == expected_timer_id
        )
    }

    /// Match a child-workflow-vs-deadline race against history (issue #779).
    ///
    /// Mirrors [`Self::match_signal_or_timer`] with the winner/loser roles
    /// swapped: a child terminal (`ChildWorkflowCompleted`/`ChildWorkflowFailed`)
    /// is the winner and `TimerFired` is the loser, or vice versa. The race
    /// composes the existing child-workflow and timer events — no new event
    /// variant. The winner is the resolution event that appears **first in
    /// recorded history**.
    ///
    /// Positional invariant: the worker persists `ChildWorkflowStarted`
    /// immediately followed by `TimerStarted` in one transaction, so both are
    /// matched positionally (Diverge on mismatch) before the forward scan for
    /// the first resolution event. Interleaved concurrent-sibling events are
    /// stashed / rewound exactly as in [`Self::match_signal_or_timer`].
    #[allow(clippy::too_many_lines)]
    pub fn match_child_or_timer(
        &mut self,
        workflow_name: &str,
        input: &Value,
        timer_id: &str,
        expected_duration: Option<u64>,
    ) -> ChildOrTimerMatch {
        if !self.prepare_match() {
            return ChildOrTimerMatch::NoMatch;
        }

        let child_pos = self.cursor;
        let WorkflowEvent::ChildWorkflowStarted {
            child_id,
            workflow_name: recorded_name,
            input: recorded_input,
        } = &self.events[self.cursor]
        else {
            return ChildOrTimerMatch::Diverged {
                expected: format!("ChildWorkflowStarted({workflow_name})"),
                actual: Self::actual_event_name(&self.events[self.cursor]),
                event_index: i32::try_from(self.cursor).ok(),
            };
        };
        let child_id = *child_id;
        if recorded_name != workflow_name {
            return ChildOrTimerMatch::Diverged {
                expected: format!("ChildWorkflowStarted({workflow_name})"),
                actual: format!("ChildWorkflowStarted({recorded_name})"),
                event_index: i32::try_from(self.cursor).ok(),
            };
        }
        if recorded_input != input {
            return ChildOrTimerMatch::Diverged {
                expected: format!("ChildWorkflowInput({input})"),
                actual: format!("ChildWorkflowInput({recorded_input})"),
                event_index: i32::try_from(self.cursor).ok(),
            };
        }

        // The deadline timer must be recorded immediately after the child start
        // (both appended in one worker transaction).
        let timer_pos = child_pos + 1;
        let Some(WorkflowEvent::TimerStarted {
            timer_id: recorded_id,
            duration_secs: recorded_duration,
        }) = self.events.get(timer_pos)
        else {
            let actual = self
                .events
                .get(timer_pos)
                .map_or_else(|| "<end of history>".to_string(), Self::actual_event_name);
            return ChildOrTimerMatch::Diverged {
                expected: format!("TimerStarted({timer_id})"),
                actual,
                event_index: i32::try_from(timer_pos).ok(),
            };
        };
        if recorded_id.as_str() != timer_id {
            return ChildOrTimerMatch::Diverged {
                expected: format!("TimerStarted({timer_id})"),
                actual: format!("TimerStarted({recorded_id})"),
                event_index: i32::try_from(timer_pos).ok(),
            };
        }
        if let Some(expected) = expected_duration
            && *recorded_duration != expected
        {
            return ChildOrTimerMatch::Diverged {
                expected: format!("TimerStarted({timer_id}, duration={expected}s)"),
                actual: format!("TimerStarted({recorded_id}, duration={recorded_duration}s)"),
                event_index: i32::try_from(timer_pos).ok(),
            };
        }

        // Advance past the started pair, then scan for the first resolution.
        let resume_pos = timer_pos + 1;
        let mut scan_cursor = resume_pos;
        let mut first_interleaved_command = None;

        while scan_cursor < self.events.len() {
            if self.is_consumed(scan_cursor) {
                scan_cursor += 1;
                continue;
            }

            match &self.events[scan_cursor] {
                // Child wins: its terminal was recorded before the deadline fired.
                WorkflowEvent::ChildWorkflowCompleted {
                    child_id: id,
                    output,
                } if *id == child_id => {
                    let output = output.clone();
                    self.settle_race_child_won(
                        scan_cursor,
                        resume_pos,
                        first_interleaved_command,
                        timer_id,
                    );
                    return ChildOrTimerMatch::ChildCompleted { output };
                }
                WorkflowEvent::ChildWorkflowFailed {
                    child_id: id,
                    error,
                    error_type,
                    details,
                    non_retryable,
                } if *id == child_id => {
                    // Carry the child's typed failure fields (issue #767); a
                    // legacy / untyped child failure decodes to the `"Error"`
                    // sentinel — identical to `match_child_workflow`.
                    let error = error.clone();
                    let error_type = error_type.clone().unwrap_or_else(|| "Error".to_string());
                    let details = details.clone();
                    let non_retryable = non_retryable.unwrap_or(false);
                    self.settle_race_child_won(
                        scan_cursor,
                        resume_pos,
                        first_interleaved_command,
                        timer_id,
                    );
                    return ChildOrTimerMatch::ChildFailed {
                        error,
                        error_type,
                        details,
                        non_retryable,
                    };
                }

                // Timer wins: the deadline fired before the child terminated.
                WorkflowEvent::TimerFired { timer_id: id } if id.as_str() == timer_id => {
                    // A loser child terminal recorded after the fire (the
                    // synthetic `ChildWorkflowFailed` from the race-loser
                    // cancellation) is deliverable to nobody, so consume it and
                    // report `child_already_terminal` so the caller pushes
                    // `CancelRaceLosers` only on the one live cycle the child is
                    // still running.
                    let child_already_terminal =
                        self.consume_loser_child_terminal(scan_cursor + 1, child_id);
                    if let Some(command_cursor) = first_interleaved_command {
                        self.consumed_out_of_order_events.insert(scan_cursor);
                        self.cursor = command_cursor;
                    } else {
                        self.cursor = scan_cursor + 1;
                    }
                    self.advance_to_next_unconsumed_event();
                    return ChildOrTimerMatch::TimerFired {
                        child_id,
                        child_already_terminal,
                    };
                }

                // Concurrent sibling commands (tokio::join!) can interleave with
                // the pending race. Keep the first as the next replay cursor and
                // scan past it so a resolution recorded later is still found.
                WorkflowEvent::ChildWorkflowStarted { .. }
                | WorkflowEvent::ChildWorkflowSpawnedDetached { .. }
                | WorkflowEvent::ActivityScheduled { .. }
                | WorkflowEvent::LocalActivityScheduled { .. }
                | WorkflowEvent::MarkerRecorded { .. }
                | WorkflowEvent::SideEffectRecorded { .. }
                | WorkflowEvent::TimerStarted { .. }
                | WorkflowEvent::TimerCancelled { .. } => {
                    first_interleaved_command.get_or_insert(scan_cursor);
                    scan_cursor += 1;
                }

                // Signals arriving while the race is pending are stashed for
                // later signal waits.
                WorkflowEvent::SignalReceived { .. } => {
                    self.stash_transparent_external_event(scan_cursor);
                    scan_cursor += 1;
                }

                ev if Self::is_update_event(ev) => {
                    scan_cursor += 1;
                }

                // Progress and terminal events of concurrent siblings (foreign
                // timer fires, terminals of other children) are transparent to
                // the race scan — their own matchers consume them after a rewind.
                WorkflowEvent::ActivityStarted { .. }
                | WorkflowEvent::ActivityHeartbeat { .. }
                | WorkflowEvent::ActivityCompleted { .. }
                | WorkflowEvent::ActivityFailed { .. }
                | WorkflowEvent::ActivityTimedOut { .. }
                | WorkflowEvent::LocalActivityCompleted { .. }
                | WorkflowEvent::LocalActivityFailed { .. }
                | WorkflowEvent::ChildWorkflowCompleted { .. }
                | WorkflowEvent::ChildWorkflowFailed { .. }
                | WorkflowEvent::TimerFired { .. } => {
                    scan_cursor += 1;
                }

                // ExternalSignal event triplets can be interleaved, and
                // ExternalCancel/ExternalAwait triplets are likewise
                // transparent to the race scan (issue #492, issue #757).
                // Stash them for their own matchers.
                WorkflowEvent::ExternalSignalRequested { .. }
                | WorkflowEvent::ExternalSignalDelivered { .. }
                | WorkflowEvent::ExternalSignalFailed { .. }
                | WorkflowEvent::ExternalCancelRequested { .. }
                | WorkflowEvent::ExternalCancelDelivered { .. }
                | WorkflowEvent::ExternalCancelFailed { .. }
                | WorkflowEvent::ExternalAwaitRequested { .. }
                | WorkflowEvent::ExternalAwaitResolved { .. }
                | WorkflowEvent::ExternalAwaitFailed { .. } => {
                    self.stash_transparent_external_event(scan_cursor);
                    scan_cursor += 1;
                }

                _ => break,
            }
        }

        // Both started but neither resolution event is recorded yet. Rewind to
        // the first interleaved command so a concurrent sibling's matcher still
        // finds its own events.
        self.cursor = first_interleaved_command.unwrap_or(resume_pos);
        self.advance_to_next_unconsumed_event();
        ChildOrTimerMatch::InProgress { child_id }
    }

    /// Match a detached child workflow spawn against history.
    ///
    /// Expects `ChildWorkflowSpawnedDetached { workflow_name, input,
    /// parent_close_policy }` at the current cursor position. Returns the
    /// recorded `child_id` so the workflow function gets back the same
    /// [`ExecutionId`] across replay cycles.
    ///
    /// Returns:
    /// - `DetachedChildSpawned { child_id }` when the event matches
    /// - `NoMatch` when the cursor is past the end of history (new live spawn)
    /// - `Diverged` when the event at cursor is not the expected spawn event
    pub fn match_detached_child_spawn(
        &mut self,
        workflow_name: &str,
        input: &Value,
        parent_close_policy: ParentClosePolicy,
    ) -> HistoryMatch {
        if !self.prepare_match() {
            return HistoryMatch::NoMatch;
        }

        match &self.events[self.cursor] {
            WorkflowEvent::ChildWorkflowSpawnedDetached {
                child_id,
                workflow_name: recorded_name,
                input: recorded_input,
                parent_close_policy: recorded_policy,
            } => {
                if recorded_name != workflow_name {
                    return HistoryMatch::Diverged {
                        expected: format!("ChildWorkflowSpawnedDetached({workflow_name})"),
                        actual: format!("ChildWorkflowSpawnedDetached({recorded_name})"),

                        event_index: i32::try_from(self.cursor).ok(),
                    };
                }
                if recorded_input != input {
                    return HistoryMatch::Diverged {
                        expected: format!("DetachedChildWorkflowInput({input})"),
                        actual: format!("DetachedChildWorkflowInput({recorded_input})"),

                        event_index: i32::try_from(self.cursor).ok(),
                    };
                }
                if *recorded_policy != parent_close_policy {
                    return HistoryMatch::Diverged {
                        expected: format!(
                            "DetachedChildWorkflowPolicy({})",
                            parent_close_policy.as_str()
                        ),
                        actual: format!(
                            "DetachedChildWorkflowPolicy({})",
                            recorded_policy.as_str()
                        ),

                        event_index: i32::try_from(self.cursor).ok(),
                    };
                }
                let child_id = *child_id;
                self.cursor += 1;
                self.advance_to_next_unconsumed_event();
                HistoryMatch::DetachedChildSpawned { child_id }
            }
            other => HistoryMatch::Diverged {
                expected: format!("ChildWorkflowSpawnedDetached({workflow_name})"),
                actual: Self::actual_event_name(other),

                event_index: i32::try_from(self.cursor).ok(),
            },
        }
    }

    /// Match a version gate against history.
    ///
    /// Looks for a `MarkerRecorded { name: "version:{change_id}" }` at
    /// the current cursor position.
    ///
    /// Returns:
    /// - The recorded version if a matching marker is found
    /// - `min_version` if no marker exists (old workflow before versioning)
    /// - `max_version` if past end of history (new code path)
    #[must_use]
    pub fn match_side_effect(&mut self, side_effect_id: &str) -> HistoryMatch {
        self.match_side_effect_event(SideEffectKind::Custom, Some(side_effect_id))
    }

    /// Match a deterministic side-effect capture against history (issue #384).
    ///
    /// All of the `WorkflowContext` deterministic primitives — `system_now()`,
    /// `new_uuid()`, `random_*()`, and `side_effect()` — lower onto a single
    /// [`WorkflowEvent::SideEffectRecorded`] variant and match through this
    /// method in command (cursor) order. The recorded `value` is returned via
    /// [`HistoryMatch::Matched`].
    ///
    /// `kind` distinguishes the built-in helper; `name` is `Some` only for the
    /// author-named `side_effect()` path. A mismatch in either the `kind` or the
    /// `name` at the current cursor surfaces as [`HistoryMatch::Diverged`] with
    /// the same diagnostic quality as the activity-mismatch path.
    ///
    /// **Backward compatibility:** the pre-#384 `side_effect()` implementation
    /// lowered onto `MarkerRecorded { name: "side_effect:{id}" }`. For the
    /// `Custom` + named case this method also accepts that legacy marker so
    /// in-flight executions recorded under the old engine replay unchanged.
    ///
    /// Uses `prepare_match` so `drain_early_signals` skips `ExternalSignal`
    /// events that may have been written before this capture in a mixed batch.
    #[must_use]
    pub fn match_side_effect_event(
        &mut self,
        kind: SideEffectKind,
        name: Option<&str>,
    ) -> HistoryMatch {
        if !self.prepare_match() {
            return HistoryMatch::NoMatch;
        }

        let expected = name.map_or_else(
            || format!("SideEffectRecorded({})", kind.as_str()),
            |n| format!("SideEffectRecorded({}:{n})", kind.as_str()),
        );

        // Legacy compatibility: the old side_effect() lowered to a MarkerRecorded
        // with name "side_effect:{id}". Only the Custom+named path ever produced it.
        let legacy_marker_name = name
            .filter(|_| kind == SideEffectKind::Custom)
            .map(|n| format!("side_effect:{n}"));

        match &self.events[self.cursor] {
            WorkflowEvent::SideEffectRecorded {
                kind: recorded_kind,
                name: recorded_name,
                value,
            } if *recorded_kind == kind && recorded_name.as_deref() == name => {
                let output = value.clone();
                self.cursor += 1;
                self.advance_to_next_unconsumed_event();
                HistoryMatch::Matched { output }
            }
            WorkflowEvent::MarkerRecorded {
                name: marker_name,
                details,
            } if legacy_marker_name.as_deref() == Some(marker_name.as_str()) => {
                let output = details.clone();
                self.cursor += 1;
                self.advance_to_next_unconsumed_event();
                HistoryMatch::Matched { output }
            }
            other => HistoryMatch::Diverged {
                expected,
                actual: Self::actual_event_name(other),

                event_index: i32::try_from(self.cursor).ok(),
            },
        }
    }

    /// Tolerant deadline-branch wall-clock read (issue #772).
    ///
    /// Peeks the cursor for the deadline probe's own recorded clock read — a
    /// `SideEffectRecorded { kind: Now, name: Some(DEADLINE_PROBE_SIDE_EFFECT_NAME) }`
    /// — and resolves to one of three states; see [`SideEffectNowMatch`] for the
    /// full rationale. Unlike the strict
    /// [`match_side_effect_event`](Self::match_side_effect_event), a cursor
    /// holding any *other* recorded event resolves to
    /// [`SideEffectNowMatch::NotRecorded`] (cursor untouched, no divergence)
    /// rather than [`HistoryMatch::Diverged`].
    ///
    /// **Reserved-name gating (migration safety).** The probe matches ONLY its
    /// own sentinel-named `Now`. A `SideEffectRecorded { kind: Now, name: None }`
    /// at the cursor is an author-side [`system_now`](crate::context::WorkflowContext::system_now)
    /// read — a real value the workflow's own code will consume — so it resolves
    /// to `NotRecorded` (cursor untouched), never stolen by the probe. This is
    /// what lets a pre-#772 history whose `should_continue_as_new()` check sits
    /// immediately before a `ctx.system_now()`/`sleep_until()` call replay
    /// cleanly on upgrade: the deadline branch degrades to history-count-only for
    /// the pre-upgrade recorded portion, and the user's own `Now` is matched by
    /// its own strict read.
    ///
    /// Uses the same [`prepare_match`](Self::prepare_match) preamble as the
    /// strict matcher, so it lands on exactly the same cursor position — the
    /// only difference is the non-diverging treatment of a non-probe event.
    #[must_use]
    pub fn match_side_effect_now_tolerant(&mut self) -> SideEffectNowMatch {
        if !self.prepare_match() {
            // Past the end of recorded history — the live frontier.
            return SideEffectNowMatch::Frontier;
        }
        match &self.events[self.cursor] {
            WorkflowEvent::SideEffectRecorded {
                kind: SideEffectKind::Now,
                name: Some(name),
                value,
            } if name == DEADLINE_PROBE_SIDE_EFFECT_NAME => {
                // Recorded `Now` values are always a JSON integer (millis). A
                // malformed value is treated as "unavailable" rather than
                // diverging (fail-safe; practically unreachable).
                match value.as_i64() {
                    Some(millis) => {
                        self.cursor += 1;
                        self.advance_to_next_unconsumed_event();
                        SideEffectNowMatch::Matched { millis }
                    }
                    None => SideEffectNowMatch::NotRecorded,
                }
            }
            // Any other recorded event — including a user `system_now()` Now
            // (`{ kind: Now, name: None }`) or a differently-named `Now` — do NOT
            // consume the cursor, do NOT diverge. The deadline branch degrades to
            // history-count-only for the recorded portion of a pre-#772 history,
            // and a user `Now` is left for its own strict read to consume.
            _ => SideEffectNowMatch::NotRecorded,
        }
    }

    /// Match a local activity command against history.
    ///
    /// Expects `LocalActivityScheduled { name }` at the current cursor, then
    /// scans forward for `LocalActivityCompleted` (returns [`HistoryMatch::Matched`])
    /// or exhausts `LocalActivityFailed` events and returns the last failure
    /// (returns [`HistoryMatch::Failed`]).
    ///
    /// Intermediate `LocalActivityFailed` events (when the handler was retried
    /// inline) are skipped; only the final outcome is surfaced to the workflow.
    ///
    /// Returns:
    /// - [`HistoryMatch::Matched`] if the activity eventually succeeded
    /// - [`HistoryMatch::Failed`] if retries were exhausted (last recorded failure)
    /// - [`HistoryMatch::NoMatch`] if past end of history (first-time execution)
    /// - [`HistoryMatch::Diverged`] if history has a different event at this position
    pub fn match_local_activity(&mut self, activity_name: &str) -> HistoryMatch {
        if !self.prepare_match() {
            return HistoryMatch::NoMatch;
        }

        let WorkflowEvent::LocalActivityScheduled {
            activity_id,
            name: recorded_name,
            ..
        } = &self.events[self.cursor]
        else {
            return HistoryMatch::Diverged {
                expected: format!("LocalActivityScheduled({activity_name})"),
                actual: Self::actual_event_name(&self.events[self.cursor]),

                event_index: i32::try_from(self.cursor).ok(),
            };
        };
        let activity_id = *activity_id;

        if recorded_name != activity_name {
            return HistoryMatch::Diverged {
                expected: format!("LocalActivityScheduled({activity_name})"),
                actual: format!("LocalActivityScheduled({recorded_name})"),

                event_index: i32::try_from(self.cursor).ok(),
            };
        }

        // Advance past LocalActivityScheduled
        self.cursor += 1;
        self.scan_local_activity_terminal(activity_id, self.cursor)
    }

    /// Like [`match_local_activity`](Self::match_local_activity) but also verifies the input payload.
    ///
    /// Used by the [`WorkflowReplayer`](crate::testing::WorkflowReplayer) in strict replay mode.
    pub fn match_local_activity_strict(
        &mut self,
        activity_name: &str,
        input: &Value,
    ) -> HistoryMatch {
        if !self.prepare_match() {
            return HistoryMatch::NoMatch;
        }

        let result = match &self.events[self.cursor] {
            WorkflowEvent::LocalActivityScheduled {
                activity_id,
                name: recorded_name,
                input: recorded_input,
                // Issue #620: the recorded resolved retry policy is not part of
                // strict-replay matching (name + input only), so ignore it here.
                ..
            } => {
                if recorded_name != activity_name {
                    return HistoryMatch::Diverged {
                        expected: format!("LocalActivityScheduled({activity_name})"),
                        actual: format!("LocalActivityScheduled({recorded_name})"),

                        event_index: i32::try_from(self.cursor).ok(),
                    };
                }
                if recorded_input != input {
                    return HistoryMatch::Diverged {
                        expected: format!(
                            "LocalActivityScheduled({activity_name}, input={recorded_input})"
                        ),
                        actual: format!("LocalActivityScheduled({activity_name}, input={input})"),

                        event_index: i32::try_from(self.cursor).ok(),
                    };
                }
                Ok(*activity_id)
            }
            other => Err(HistoryMatch::Diverged {
                expected: format!("LocalActivityScheduled({activity_name})"),
                actual: Self::actual_event_name(other),

                event_index: i32::try_from(self.cursor).ok(),
            }),
        };
        let activity_id = match result {
            Ok(id) => id,
            Err(diverged) => return diverged,
        };

        self.cursor += 1;
        match self.scan_local_activity_terminal(activity_id, self.cursor) {
            HistoryMatch::LocalActivityInProgress {
                failed_attempts: 0, ..
            } => HistoryMatch::NoMatch,
            other => other,
        }
    }

    /// The frozen resolution (retry policy + `start_to_close`) a local activity
    /// was scheduled with at first-schedule time (issue #620, AC8; Codex P2).
    /// Read-only, cursor-independent: scans recorded history for the
    /// `LocalActivityScheduled` event carrying `activity_id` and clones its
    /// three additive fields in one pass.
    ///
    /// `resolved` is the disambiguation marker: `true` for a #620+ event whose
    /// frozen `retry_policy`/`start_to_close_nanos` are authoritative on
    /// recovery even when `None` ("explicitly resolved to no floor"); `false`
    /// for a pre-#620 legacy event (all three fields absent → the marker
    /// deserializes to `false`), which the crash-recovery path in
    /// [`WorkflowContext::execute_local_activity_raw`](crate::context::WorkflowContext::execute_local_activity_raw)
    /// treats as "re-derive from the live defaults" for backward compatibility.
    /// A missing `activity_id` (no matching scheduled event) reports the same
    /// unresolved default.
    #[must_use]
    pub(crate) fn recorded_local_activity_resolution(
        &self,
        activity_id: ActivityExecId,
    ) -> LocalActivityResolution {
        self.events
            .iter()
            .find_map(|ev| match ev {
                WorkflowEvent::LocalActivityScheduled {
                    activity_id: recorded_id,
                    resolved,
                    retry_policy,
                    start_to_close_nanos,
                    ..
                } if *recorded_id == activity_id => Some(LocalActivityResolution {
                    resolved: *resolved,
                    retry_policy: retry_policy.clone(),
                    start_to_close_nanos: *start_to_close_nanos,
                }),
                _ => None,
            })
            .unwrap_or_default()
    }

    /// Versioning mechanism for safe workflow code changes.
    ///
    /// Checks the recorded history for a version marker. If the marker is present
    /// in history, returns the recorded version. If not in history (first execution
    /// or unversioned branch), records `max_version` as a marker and returns it.
    ///
    /// This ensures that existing non-deterministic executions continue correctly,
    /// while new executions start on the new `max_version` path.
    pub fn match_version(&mut self, change_id: &str, min_version: u32, max_version: u32) -> u32 {
        self.advance_to_next_unconsumed_event();
        let marker_name = format!("version:{change_id}");

        // Check BEFORE draining: if already past cursor-based history, this is
        // a genuinely new code path → record max_version. This is exactly the
        // case where the context will push a `version:{change_id}` marker
        // command, so latch the id for a same-cycle `deprecate_patch`
        // (issue #687 interop — see `patch_ids_recorded_this_cycle`).
        if !self.is_replaying() {
            self.patch_ids_recorded_this_cycle
                .insert(change_id.to_string());
            return max_version;
        }

        // Now safe to drain ExternalSignal events that may precede this marker
        // in a mixed batch (e.g. tokio::join!(ctx.version(...), signal_external)).
        self.drain_early_signals();

        // After draining: if cursor is past end (only stashed ExternalSignal
        // events were the remaining history), this is an unversioned position —
        // return min_version so existing executions stay on the old branch
        // instead of recording a new marker and jumping to max_version.
        if !self.is_replaying() {
            return min_version;
        }

        match &self.events[self.cursor] {
            WorkflowEvent::MarkerRecorded { name, details } if *name == marker_name => {
                let version = details
                    .as_u64()
                    .and_then(|v| u32::try_from(v).ok())
                    .unwrap_or(min_version);
                self.cursor += 1;
                self.advance_to_next_unconsumed_event();
                version
            }
            // No marker at current position — old workflow that didn't have
            // this version gate. Don't advance cursor.
            _ => min_version,
        }
    }

    // ── Patch markers (issue #687) ────────────────────────────────────────

    /// Match a `patched(patch_id)` call against recorded history.
    ///
    /// Mirrors [`Self::match_version`]'s scan discipline exactly, but returns
    /// the three-state [`PatchMarkerMatch`] instead of a numeric version so
    /// the caller never has to re-derive live-vs-replay after the fact:
    ///
    /// 1. If `patch_id` was previously deprecated (see
    ///    [`Self::deprecate_patch`]), the memoized presence is returned
    ///    immediately — [`PatchMarkerMatch::Recorded`] if a marker was in
    ///    history, [`PatchMarkerMatch::Absent`] otherwise — WITHOUT touching
    ///    the cursor. This is what keeps a residual `patched(id)` call after
    ///    `deprecate_patch(id)` deterministic.
    /// 2. Past the end of cursor-based history →
    ///    [`PatchMarkerMatch::NewlyPatched`] (live frontier — the caller
    ///    records a fresh marker).
    /// 3. After draining early signal events, if only stashed signal events
    ///    remained → [`PatchMarkerMatch::Absent`] (this is a recorded
    ///    position; the old branch applies, nothing is recorded).
    /// 4. A `MarkerRecorded` at the cursor named `patch:{id}` **or**
    ///    `version:{id}` (interop: a run that recorded a version marker under
    ///    the old `ctx.version()` API is observed as patched — presence alone
    ///    decides, regardless of the recorded number, because `version()`
    ///    only ever records a marker when it returned `max` on live
    ///    execution) → consume it, return [`PatchMarkerMatch::Recorded`].
    /// 5. Anything else at the cursor → [`PatchMarkerMatch::Absent`], cursor
    ///    untouched.
    pub fn match_patch_marker(&mut self, patch_id: &str) -> PatchMarkerMatch {
        if let Some(&present) = self.deprecated_patches.get(patch_id) {
            return if present {
                PatchMarkerMatch::Recorded
            } else {
                PatchMarkerMatch::Absent
            };
        }

        self.advance_to_next_unconsumed_event();

        // Check BEFORE draining: if already past cursor-based history, this is
        // a genuinely new code path → the caller records a fresh marker.
        // Latch the id so a same-cycle `deprecate_patch` sees the marker even
        // though it exists only as a pending command, not in history yet.
        if !self.is_replaying() {
            self.patch_ids_recorded_this_cycle
                .insert(patch_id.to_string());
            return PatchMarkerMatch::NewlyPatched;
        }

        // Now safe to drain ExternalSignal events that may precede this marker
        // in a mixed batch (e.g. tokio::join!(ctx.patched(...), signal_external)).
        self.drain_early_signals();

        // After draining: if the cursor is past the end (only stashed signal
        // events were the remaining history), this is an unpatched recorded
        // position — take the old branch instead of recording a new marker.
        if !self.is_replaying() {
            return PatchMarkerMatch::Absent;
        }

        let patch_name = patch_marker_name(patch_id);
        let version_name = version_marker_name(patch_id);
        match &self.events[self.cursor] {
            WorkflowEvent::MarkerRecorded { name, .. }
                if *name == patch_name || *name == version_name =>
            {
                self.cursor += 1;
                self.advance_to_next_unconsumed_event();
                PatchMarkerMatch::Recorded
            }
            // No marker at the current position — pre-patch history.
            // Don't advance the cursor.
            _ => PatchMarkerMatch::Absent,
        }
    }

    /// Deprecate a patch id (issue #687): make every recorded `patch:{id}` /
    /// `version:{id}` marker transparent to all subsequent scans, and memoize
    /// whether any such marker was present.
    ///
    /// Positional matching cannot apply here: a phase-1 run recorded the
    /// marker at the old `patched()` call position, while phase-2 code calls
    /// `deprecate_patch(id)` at a different (usually earlier) position — so
    /// the marker must become transparent wherever it sits, or it would trip
    /// the next `match_*` call as a divergence. The full-history scan marks
    /// each matching marker's index consumed via the same consumed-index set
    /// every scan loop already consults through [`Self::is_consumed`].
    ///
    /// Returns whether a marker was found (memoized — the call is
    /// idempotent). The memo also drives [`Self::match_patch_marker`] so a
    /// residual `patched(id)` call stays deterministic; note that on a NEW
    /// execution (empty history) the memo is `false`, so a residual
    /// `patched(id)` treats post-deprecation runs as unpatched — the
    /// documented footgun whose fix is deleting the residual call.
    pub fn deprecate_patch(&mut self, patch_id: &str) -> bool {
        if let Some(&present) = self.deprecated_patches.get(patch_id) {
            return present;
        }

        let patch_name = patch_marker_name(patch_id);
        let version_name = version_marker_name(patch_id);
        // A marker recorded earlier in this SAME cycle (by `patched()` or
        // `version()` on the live frontier) is not in history yet — it is a
        // pending `RecordMarker` command — but it counts as present, or the
        // live cycle's memo would disagree with every replay cycle's
        // (the sandwich flip, review finding on issue #687).
        let mut present = self.patch_ids_recorded_this_cycle.contains(patch_id);
        for (idx, event) in self.events.iter().enumerate() {
            if let WorkflowEvent::MarkerRecorded { name, .. } = event
                && (*name == patch_name || *name == version_name)
            {
                present = true;
                self.consumed_out_of_order_events.insert(idx);
            }
        }
        self.deprecated_patches
            .insert(patch_id.to_string(), present);
        present
    }

    // ── Saga compensation markers (issue #801) ────────────────────────────

    /// Match a saga compensation dedup marker (`saga_compensated:{seq}` or
    /// `saga_compensation_failed:{seq}`) against recorded history.
    ///
    /// Clones [`Self::match_patch_marker`]'s tolerant scan discipline —
    /// the mechanism that exists precisely to retrofit markers into an
    /// existing API without breaking already-recorded histories — extended
    /// post-review with two saga-specific tolerances (a distinguishable
    /// drained-signal frontier, and transparency to command-less
    /// `WorkflowCancelled` lifecycle events):
    ///
    /// 1. Past the end of cursor-based history →
    ///    [`SagaMarkerMatch::LiveFrontier`] (the caller records a fresh
    ///    marker and emits the counter — the exactly-once point).
    /// 2. After draining early signal events, if only stashed signal events
    ///    remained → [`SagaMarkerMatch::DrainedSignalFrontier`] (a recorded
    ///    position; the caller decides whether to record, keyed to the
    ///    unwind's disposition — see the variant docs).
    /// 3. A `MarkerRecorded` at the cursor with exactly `marker_name` →
    ///    consume it, return [`SagaMarkerMatch::Recorded`].
    /// 4. Otherwise, a bounded non-destructive lookahead skips
    ///    `WorkflowCancelled` lifecycle events (which have no
    ///    workflow-command counterpart, are never consumed by any other
    ///    matcher, and would otherwise permanently hide the frontier from
    ///    the cancel-and-compensate pattern) and any `SignalReceived`
    ///    recorded behind them (unreachable by `drain_early_signals`):
    ///    - exactly `marker_name` found → consume it **out-of-order**
    ///      (mirroring [`Self::deprecate_patch`]'s mechanism; the cursor is
    ///      untouched so every other match is unaffected), return
    ///      [`SagaMarkerMatch::Recorded`];
    ///    - the frontier reached past only cancellation events →
    ///      [`SagaMarkerMatch::LiveFrontier`] (countable: the
    ///      cancel-and-compensate unwind IS the live frontier);
    ///    - the frontier reached but a trailing signal was skipped →
    ///      [`SagaMarkerMatch::DrainedSignalFrontier`] (same conservative
    ///      treatment as arm 2);
    ///    - any other event → [`SagaMarkerMatch::Absent`], cursor untouched.
    ///      This is the backward-compat arm: a pre-#801 history mid-unwind
    ///      holds the first compensation's `ActivityScheduled` here (and a
    ///      full-history replay of a terminal run holds its terminal event)
    ///      and must proceed unharmed — no divergence, no emission, no
    ///      marker.
    pub fn match_saga_marker(&mut self, marker_name: &str) -> SagaMarkerMatch {
        self.advance_to_next_unconsumed_event();

        // Check BEFORE draining: if already past cursor-based history, this
        // is a genuinely new unwind → the caller records a fresh marker.
        if !self.is_replaying() {
            return SagaMarkerMatch::LiveFrontier;
        }

        // Now safe to drain ExternalSignal events that may precede this
        // marker in a mixed batch.
        self.drain_early_signals();

        // After draining: if the cursor is past the end (only stashed signal
        // events were the remaining history), report the drained-signal
        // frontier and let the caller resolve it against the unwind's
        // disposition.
        if !self.is_replaying() {
            return SagaMarkerMatch::DrainedSignalFrontier;
        }

        // Fast path: the marker sits exactly at the cursor — consume it in
        // place (identical cursor-advance semantics to match_patch_marker).
        if let WorkflowEvent::MarkerRecorded { name, .. } = &self.events[self.cursor]
            && *name == marker_name
        {
            self.cursor += 1;
            self.advance_to_next_unconsumed_event();
            return SagaMarkerMatch::Recorded;
        }

        // Tolerant lookahead past command-less cancellation lifecycle events
        // (and signals recorded behind them). Non-destructive except for the
        // exact-marker hit, which is consumed out-of-order.
        let mut scan = self.cursor;
        let mut skipped_signal = false;
        while scan < self.events.len() {
            if self.is_consumed(scan) {
                scan += 1;
                continue;
            }
            match &self.events[scan] {
                // A command-less `WorkflowCancelled`, or a cancellable-timer
                // arm/cancel (issue #768) interleaved in a compensation cycle, is
                // transparent to the saga marker scan. A `reset()` records
                // `[TimerCancelled, TimerStarted]`, so BOTH are skipped for
                // symmetry — stopping on the re-arm would wrongly return `Absent`
                // (Codex P2, issue #768). Left unconsumed so `match_timer_cancel`
                // / `match_timer_arm` claim them later.
                WorkflowEvent::WorkflowCancelled { .. }
                | WorkflowEvent::TimerCancelled { .. }
                | WorkflowEvent::TimerStarted { .. } => {
                    scan += 1;
                }
                WorkflowEvent::SignalReceived { .. } => {
                    skipped_signal = true;
                    scan += 1;
                }
                WorkflowEvent::MarkerRecorded { name, .. } if *name == marker_name => {
                    self.consumed_out_of_order_events.insert(scan);
                    return SagaMarkerMatch::Recorded;
                }
                // Any other event — pre-#801 history mid-unwind, a terminal
                // event of a fully-recorded run, or an unwind entered with
                // unconsumed events at the cursor. Don't touch anything.
                _ => return SagaMarkerMatch::Absent,
            }
        }
        if skipped_signal {
            SagaMarkerMatch::DrainedSignalFrontier
        } else {
            // Only cancellation events separate the cursor from the
            // frontier — the cancel-and-compensate pattern's live unwind.
            SagaMarkerMatch::LiveFrontier
        }
    }

    // ── Fan-out / parallel activities (issue #359) ───────────────────────────

    /// Match the count marker for a fan-out group against history.
    ///
    /// On the **first live execution** (past end of history) returns
    /// [`HistoryMatch::NoMatch`] so the caller can emit a `RecordMarker`
    /// command for replay.
    ///
    /// During **replay** expects a `MarkerRecorded { name: "fan_out:{seq}", … }`
    /// event at the current cursor. Returns:
    ///
    /// - [`HistoryMatch::Matched`] — marker found and recorded count equals `count`.
    /// - [`HistoryMatch::Diverged`] — recorded count differs from `count`
    ///   (non-deterministic collection resize detected), or a different event
    ///   type was at this cursor position.
    /// - [`HistoryMatch::NoMatch`] — past end of history (live execution).
    pub fn match_fan_out_marker(&mut self, seq: u32, count: usize) -> HistoryMatch {
        let marker_name = format!("fan_out:{seq}");
        if !self.prepare_match() {
            return HistoryMatch::NoMatch;
        }

        match &self.events[self.cursor] {
            WorkflowEvent::MarkerRecorded { name, details } if *name == marker_name => {
                let recorded_count = details
                    .as_u64()
                    .and_then(|n| usize::try_from(n).ok())
                    .unwrap_or(0);
                if recorded_count == count {
                    self.cursor += 1;
                    self.advance_to_next_unconsumed_event();
                    HistoryMatch::Matched {
                        output: serde_json::json!(count),
                    }
                } else {
                    HistoryMatch::Diverged {
                        expected: format!("fan_out:{seq}(count={recorded_count})"),
                        actual: format!("fan_out:{seq}(count={count})"),

                        event_index: i32::try_from(self.cursor).ok(),
                    }
                }
            }
            other => HistoryMatch::Diverged {
                expected: format!("MarkerRecorded({marker_name})"),
                actual: Self::actual_event_name(other),

                event_index: i32::try_from(self.cursor).ok(),
            },
        }
    }

    /// Match a `MarkerRecorded` event carrying a single `u64` payload at the
    /// current cursor position, keyed by an arbitrary caller-supplied name.
    ///
    /// Generalizes [`Self::match_fan_out_marker`]'s count-verification shape
    /// for any `u64`-valued marker. Used by `WorkflowContext::race` (issue
    /// #600) to record and verify both the race's branch count (the "open"
    /// marker) and its winning branch index (the "winner" marker) — mirroring
    /// the fan-out marker idiom without introducing a new event variant.
    ///
    /// Returns:
    /// - [`HistoryMatch::Matched`] — marker found and recorded value equals `expected`.
    /// - [`HistoryMatch::Diverged`] — recorded value differs from `expected`,
    ///   or a different event type was at this cursor position.
    /// - [`HistoryMatch::NoMatch`] — past end of history (live execution).
    pub fn match_u64_marker(&mut self, marker_name: &str, expected: u64) -> HistoryMatch {
        if !self.prepare_match() {
            return HistoryMatch::NoMatch;
        }

        match &self.events[self.cursor] {
            WorkflowEvent::MarkerRecorded { name, details } if name == marker_name => {
                let recorded = details.as_u64();
                if recorded == Some(expected) {
                    self.cursor += 1;
                    self.advance_to_next_unconsumed_event();
                    HistoryMatch::Matched {
                        output: serde_json::json!(expected),
                    }
                } else {
                    HistoryMatch::Diverged {
                        expected: format!("MarkerRecorded({marker_name}, {expected})"),
                        actual: format!("MarkerRecorded({marker_name}, {recorded:?})"),

                        event_index: i32::try_from(self.cursor).ok(),
                    }
                }
            }
            other => HistoryMatch::Diverged {
                expected: format!("MarkerRecorded({marker_name})"),
                actual: Self::actual_event_name(other),

                event_index: i32::try_from(self.cursor).ok(),
            },
        }
    }

    // ── Worker sessions (issue #606) ─────────────────────────────────────────

    /// Match (or discover) a worker-session identity marker at the current
    /// cursor position.
    ///
    /// A worker session's identity ([`crate::types::SessionId`]) is a
    /// randomly generated UUID, recorded once via `MarkerRecorded { name:
    /// "session:{seq}", details: <uuid string> }` on the session's first live
    /// dispatch. Unlike [`Self::match_fan_out_marker`] (which *verifies* a
    /// caller-supplied value against the recording) there is no independently
    /// derivable "expected" value here — the UUID exists only because it was
    /// recorded — so this method *discovers* the previously recorded id
    /// rather than comparing it to one, mirroring [`Self::peek_u64_marker`]'s
    /// discovery role but at a fixed cursor position (sessions are opened
    /// sequentially, not raced concurrently, so no interleave tolerance is
    /// needed).
    ///
    /// Returns:
    /// - [`HistoryMatch::Matched`] — marker found; `output` carries the
    ///   recorded `SessionId` as a JSON string.
    /// - [`HistoryMatch::Diverged`] — a different event, a marker with the
    ///   wrong name, or a non-UUID payload is at this cursor position.
    /// - [`HistoryMatch::NoMatch`] — past end of history (live execution):
    ///   the caller should generate a fresh `SessionId` and record it.
    pub fn match_session_marker(&mut self, seq: u32) -> HistoryMatch {
        let marker_name = format!("session:{seq}");
        if !self.prepare_match() {
            return HistoryMatch::NoMatch;
        }

        match &self.events[self.cursor] {
            WorkflowEvent::MarkerRecorded { name, details } if *name == marker_name => {
                let recorded = details.as_str().and_then(|s| s.parse::<uuid::Uuid>().ok());
                match recorded {
                    Some(uuid) => {
                        self.cursor += 1;
                        self.advance_to_next_unconsumed_event();
                        HistoryMatch::Matched {
                            output: serde_json::json!(uuid.to_string()),
                        }
                    }
                    None => HistoryMatch::Diverged {
                        expected: format!("MarkerRecorded({marker_name}, <uuid>)"),
                        actual: format!("MarkerRecorded({marker_name}, {details:?})"),

                        event_index: i32::try_from(self.cursor).ok(),
                    },
                }
            }
            other => HistoryMatch::Diverged {
                expected: format!("MarkerRecorded({marker_name})"),
                actual: Self::actual_event_name(other),

                event_index: i32::try_from(self.cursor).ok(),
            },
        }
    }

    /// Non-destructively check whether a `u64`-valued `MarkerRecorded` event
    /// named `marker_name` exists ahead in history, returning its value and
    /// consuming it if so.
    ///
    /// Unlike [`Self::match_u64_marker`] this takes no expected value, so it
    /// can be used to *discover* a previously recorded decision (e.g.
    /// `WorkflowContext::race`'s winner marker) rather than verify one already
    /// known to the caller.
    ///
    /// **Interleave-tolerant** (mirrors [`Self::scan_activity_terminal`]'s
    /// forward scan): when two `ctx.race()` calls (or a race alongside a
    /// fan-out / side-effect / child-workflow race) are driven concurrently
    /// via `futures::join!`, a sibling primitive's own marker or branch
    /// events can legitimately sit between the cursor and this marker's true
    /// recorded position — e.g. race #2's `race:2` open marker and branch
    /// schedules landing between race #1's own branch schedules and its
    /// `race_winner:1` marker. Those tolerated event kinds are scanned past
    /// (tracked, not consumed) rather than treated as an immediate miss; on
    /// a match, the cursor rewinds to the first such tolerated event (like
    /// [`Self::settle_terminal`]) so a sibling's own later scan still finds
    /// it. On a genuine miss (scan exhausted, or an event outside the
    /// tolerated set is encountered) the cursor is left unchanged if nothing
    /// was skipped, or parked at the first tolerated event otherwise — in
    /// both cases safe to call speculatively.
    /// Whether any unclaimed `SignalReceived { signal_name }` remains — either
    /// already stashed in `pending_signals`, or still unconsumed at/after the
    /// current cursor (issue #950).
    ///
    /// A **pure read**: consumes nothing and never moves the cursor. Used by the
    /// strict/canary replay frontier test for a signal wait, where "cursor at end
    /// of history" is the wrong question: a mixed suspension batch legitimately
    /// leaves a SIBLING branch's events unconsumed ahead of the cursor while the
    /// signal wait itself has nothing to match. What actually distinguishes a
    /// healthy in-flight park from a real divergence is whether a matching signal
    /// is still available anywhere.
    #[must_use]
    pub fn has_unconsumed_signal(&self, signal_name: &str) -> bool {
        if self
            .pending_signals
            .iter()
            .any(|(name, _, _)| name == signal_name)
        {
            return true;
        }
        self.events
            .iter()
            .enumerate()
            .skip(self.cursor)
            .any(|(i, e)| {
                !self.is_consumed(i)
                    && matches!(
                        e,
                        WorkflowEvent::SignalReceived { signal_name: n, .. } if n == signal_name
                    )
            })
    }

    /// Whether an unconsumed `MarkerRecorded { name }` exists anywhere at or
    /// after the current cursor (issue #950).
    ///
    /// A **pure read**: unlike [`Self::peek_u64_marker`] it consumes nothing and
    /// never moves the cursor, so it is safe to call mid-decision (e.g. between
    /// a race's branch checks) where a consuming peek would steal the marker
    /// from the settling step that owns it.
    #[must_use]
    pub fn has_unconsumed_marker(&self, marker_name: &str) -> bool {
        self.events
            .iter()
            .enumerate()
            .skip(self.cursor)
            .any(|(i, e)| {
                !self.is_consumed(i)
                    && matches!(e, WorkflowEvent::MarkerRecorded { name, .. } if name == marker_name)
            })
    }

    pub fn peek_u64_marker(&mut self, marker_name: &str) -> Option<u64> {
        if !self.prepare_match() {
            return None;
        }
        let mut scan_cursor = self.cursor;
        let mut first_interleaved_command = None;
        while scan_cursor < self.events.len() {
            if self.is_consumed(scan_cursor) {
                scan_cursor += 1;
                continue;
            }
            match &self.events[scan_cursor] {
                WorkflowEvent::MarkerRecorded { name, details } if name == marker_name => {
                    let value = details.as_u64();
                    if let Some(command_cursor) = first_interleaved_command {
                        self.consumed_out_of_order_events.insert(scan_cursor);
                        self.cursor = command_cursor;
                    } else {
                        self.cursor = scan_cursor + 1;
                    }
                    self.advance_to_next_unconsumed_event();
                    return value;
                }
                // Events a sibling ctx.race()/fan-out/side-effect/child-race
                // branch, driven concurrently via futures::join!, can
                // legitimately interleave with this marker's true position.
                WorkflowEvent::MarkerRecorded { .. }
                | WorkflowEvent::SideEffectRecorded { .. }
                | WorkflowEvent::ActivityScheduled { .. }
                | WorkflowEvent::ActivityStarted { .. }
                | WorkflowEvent::ActivityHeartbeat { .. }
                | WorkflowEvent::ActivityCompleted { .. }
                | WorkflowEvent::ActivityFailed { .. }
                | WorkflowEvent::ActivityTimedOut { .. }
                | WorkflowEvent::ChildWorkflowStarted { .. }
                | WorkflowEvent::ChildWorkflowSpawnedDetached { .. }
                | WorkflowEvent::ChildWorkflowCompleted { .. }
                | WorkflowEvent::ChildWorkflowFailed { .. }
                | WorkflowEvent::TimerStarted { .. }
                | WorkflowEvent::TimerFired { .. }
                // Cancellable-timer cancel (issue #768): a concurrent
                // `cancel_timer()`/`reset()` can interleave with this marker's
                // true position, like the other timer-lifecycle events above.
                | WorkflowEvent::TimerCancelled { .. } => {
                    first_interleaved_command.get_or_insert(scan_cursor);
                    scan_cursor += 1;
                }
                // Anything else (signals, external-signal/cancel triplets,
                // update events, terminal lifecycle events, ...) is outside
                // the tolerated set for this scan -- stop rather than
                // silently skipping past something that might matter.
                _ => break,
            }
        }
        if let Some(command_cursor) = first_interleaved_command {
            self.cursor = command_cursor;
            self.advance_to_next_unconsumed_event();
        }
        None
    }

    /// Match a named `MarkerRecorded` event at the current cursor position.
    ///
    /// Used by `WorkflowContext::dag_skip_marker` to record the condition-skip
    /// decision deterministically so replay always selects the identical branch.
    ///
    /// Returns:
    /// - [`HistoryMatch::Matched`] — the event at cursor is `MarkerRecorded`
    ///   with the exact `name`, `expected_task` (in `details.task`), and
    ///   matching `expected_upstreams` (in `details.upstreams`, when present).
    /// - [`HistoryMatch::Diverged`] — a different event, a marker with a
    ///   different name, a different task name, or (for new-format markers) a
    ///   different upstream set is at the cursor — a non-determinism violation.
    /// - [`HistoryMatch::NoMatch`] — past end of history (live execution).
    ///
    /// **Backward compatibility:** old markers without an `upstreams` field (written
    /// before this field was introduced) pass the upstream check unconditionally,
    /// so in-flight executions are not broken on upgrade.
    pub fn match_named_marker(
        &mut self,
        marker_name: &str,
        expected_task: &str,
        expected_upstreams: &[usize],
    ) -> HistoryMatch {
        if !self.prepare_match() {
            return HistoryMatch::NoMatch;
        }
        match &self.events[self.cursor] {
            WorkflowEvent::MarkerRecorded { name, details } if name == marker_name => {
                let recorded_task = details.get("task").and_then(|v| v.as_str()).unwrap_or("");
                if recorded_task != expected_task {
                    return HistoryMatch::Diverged {
                        expected: format!("MarkerRecorded({marker_name}, task={expected_task})"),
                        actual: format!("MarkerRecorded({marker_name}, task={recorded_task})"),
                        event_index: i32::try_from(self.cursor).ok(),
                    };
                }
                // Validate upstream fingerprint only when the stored marker has the
                // field (new-format markers); old markers without it pass through so
                // in-flight executions survive an upgrade.
                if let Some(arr) = details.get("upstreams").and_then(|v| v.as_array()) {
                    let recorded_upstreams: Vec<usize> = arr
                        .iter()
                        .filter_map(|v| v.as_u64().and_then(|n| usize::try_from(n).ok()))
                        .collect();
                    if recorded_upstreams != expected_upstreams {
                        return HistoryMatch::Diverged {
                            expected: format!(
                                "MarkerRecorded({marker_name}, task={expected_task}, upstreams={expected_upstreams:?})"
                            ),
                            actual: format!(
                                "MarkerRecorded({marker_name}, task={recorded_task}, upstreams={recorded_upstreams:?})"
                            ),
                            event_index: i32::try_from(self.cursor).ok(),
                        };
                    }
                }
                self.cursor += 1;
                self.advance_to_next_unconsumed_event();
                HistoryMatch::Matched {
                    output: serde_json::Value::Null,
                }
            }
            other => HistoryMatch::Diverged {
                expected: format!("MarkerRecorded({marker_name})"),
                actual: Self::actual_event_name(other),
                event_index: i32::try_from(self.cursor).ok(),
            },
        }
    }

    // ── Update primitive (issue #140) ─────────────────────────────────────

    /// Look up the recorded result for a specific update by `update_id`.
    ///
    /// Unlike the cursor-based activity/timer/signal matching methods, this
    /// performs a full-history scan keyed by `update_id`. It does **not**
    /// advance the cursor — update events are managed independently of the
    /// main workflow replay sequence.
    ///
    /// Returns:
    /// - [`HistoryMatch::Matched`] if `UpdateCompleted` with the given ID is found.
    /// - [`HistoryMatch::Failed`] if `UpdateFailed` with the given ID is found.
    /// - [`HistoryMatch::NoMatch`] if only `UpdateAdmitted` exists (in-flight) or
    ///   if the ID is entirely unknown.
    #[must_use]
    pub fn match_update(&self, update_id: UpdateId) -> HistoryMatch {
        for event in &self.events {
            match event {
                WorkflowEvent::UpdateCompleted {
                    update_id: id,
                    output,
                } if *id == update_id => {
                    return HistoryMatch::Matched {
                        output: output.clone(),
                    };
                }
                WorkflowEvent::UpdateFailed {
                    update_id: id,
                    error,
                } if *id == update_id => {
                    return HistoryMatch::Failed {
                        error: error.clone(),
                        attempt: 1,
                        error_type: "Error".to_string(),
                        details: None,
                        non_retryable: false,
                    };
                }
                _ => {}
            }
        }
        HistoryMatch::NoMatch
    }

    /// Return all `UpdateAdmitted` events that do not have a paired
    /// `UpdateCompleted` or `UpdateFailed` event in history.
    ///
    /// The worker calls this on restart to discover in-flight updates that
    /// must be re-dispatched to their registered handlers.
    ///
    /// Returns a `Vec` of `(update_id, handler_name, input)` tuples.
    #[must_use]
    pub fn drain_admitted_updates(&self) -> Vec<(UpdateId, String, Value)> {
        // Collect IDs of completed/failed updates.
        let mut resolved: std::collections::HashSet<UpdateId> = std::collections::HashSet::new();
        for event in &self.events {
            match event {
                WorkflowEvent::UpdateCompleted { update_id, .. }
                | WorkflowEvent::UpdateFailed { update_id, .. } => {
                    resolved.insert(*update_id);
                }
                _ => {}
            }
        }

        // Return admitted updates that are not yet resolved.
        self.events
            .iter()
            .filter_map(|event| {
                if let WorkflowEvent::UpdateAdmitted {
                    update_id,
                    name,
                    input,
                    ..
                } = event
                    && !resolved.contains(update_id)
                {
                    return Some((*update_id, name.clone(), input.clone()));
                }
                None
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::TimeoutType;
    use crate::types::{ActivityExecId, ExecutionId, TimerId, WorkerId};
    use chrono::Utc;

    /// Helper: build a minimal activity lifecycle (Scheduled -> Completed).
    fn activity_completed_events(
        name: &str,
        output: Value,
    ) -> (ActivityExecId, Vec<WorkflowEvent>) {
        let id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::ActivityScheduled {
                activity_id: id,
                name: name.into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: id,
                output,
            },
        ];
        (id, events)
    }

    /// Helper: build an activity lifecycle with failure.
    fn activity_failed_events(
        name: &str,
        error: &str,
        attempt: u32,
    ) -> (ActivityExecId, Vec<WorkflowEvent>) {
        let id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::ActivityScheduled {
                activity_id: id,
                name: name.into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityFailed {
                activity_id: id,
                error: error.into(),
                attempt,
                error_type: "Error".into(),
                non_retryable: false,
                details: None,
            },
        ];
        (id, events)
    }

    #[test]
    fn matcher_replays_completed_activity() {
        let output = serde_json::json!({"email_id": "msg-001"});
        let (_id, events) = activity_completed_events("send_email", output.clone());

        let mut matcher = HistoryMatcher::new(events);
        assert!(matcher.is_replaying());
        assert_eq!(matcher.position(), 0);

        let result = matcher.match_activity("send_email");
        assert_eq!(result, HistoryMatch::Matched { output });
        assert_eq!(matcher.position(), 2);
        assert!(!matcher.is_replaying());
    }

    #[test]
    fn matcher_replays_failed_activity() {
        let (_id, events) = activity_failed_events("send_email", "SMTP connection refused", 3);

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_activity("send_email");
        assert_eq!(
            result,
            HistoryMatch::Failed {
                error: "SMTP connection refused".into(),
                attempt: 3,
                error_type: "Error".into(),
                details: None,
                non_retryable: false,
            }
        );
    }

    #[test]
    fn matcher_replays_timed_out_activity() {
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "send_email".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityTimedOut {
                activity_id,
                timeout_type: TimeoutType::Heartbeat,
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_activity("send_email");
        assert_eq!(
            result,
            HistoryMatch::TimedOut {
                timeout_type: TimeoutType::Heartbeat,
            }
        );
    }

    #[test]
    fn matcher_returns_no_match_at_end_of_history() {
        let mut matcher = HistoryMatcher::new(vec![]);
        assert!(!matcher.is_replaying());
        assert_eq!(matcher.position(), 0);

        let result = matcher.match_activity("send_email");
        assert_eq!(result, HistoryMatch::NoMatch);
    }

    #[test]
    fn matcher_detects_non_determinism_wrong_event_type() {
        // History has a TimerStarted where we expect ActivityScheduled
        let events = vec![WorkflowEvent::TimerStarted {
            timer_id: TimerId::new("t1"),
            duration_secs: 60,
        }];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_activity("send_email");
        assert!(matches!(result, HistoryMatch::Diverged { .. }));

        if let HistoryMatch::Diverged {
            expected, actual, ..
        } = result
        {
            assert!(expected.contains("send_email"));
            assert!(actual.contains("TimerStarted"));
        }
    }

    #[test]
    fn matcher_detects_non_determinism_wrong_activity_name() {
        let id = ActivityExecId::new();
        let events = vec![WorkflowEvent::ActivityScheduled {
            activity_id: id,
            name: "charge_payment".into(),
            input: Value::Null,
            queue: "default".into(),
        }];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_activity("send_email");

        assert!(matches!(result, HistoryMatch::Diverged { .. }));
        if let HistoryMatch::Diverged {
            expected, actual, ..
        } = result
        {
            assert!(expected.contains("send_email"));
            assert!(actual.contains("charge_payment"));
        }
    }

    #[test]
    fn matcher_skips_heartbeats_during_replay() {
        let id = ActivityExecId::new();
        let output = serde_json::json!({"rows": 1000});

        let events = vec![
            WorkflowEvent::ActivityScheduled {
                activity_id: id,
                name: "import_data".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityStarted {
                activity_id: id,
                worker_id: WorkerId::new("worker-1"),
            },
            WorkflowEvent::ActivityHeartbeat {
                activity_id: id,
                details: serde_json::json!({"progress": 25}),
            },
            WorkflowEvent::ActivityHeartbeat {
                activity_id: id,
                details: serde_json::json!({"progress": 50}),
            },
            WorkflowEvent::ActivityHeartbeat {
                activity_id: id,
                details: serde_json::json!({"progress": 75}),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: id,
                output: output.clone(),
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_activity("import_data");
        assert_eq!(result, HistoryMatch::Matched { output });
        // Cursor should be past all 6 events
        assert_eq!(matcher.position(), 6);
        assert!(!matcher.is_replaying());
    }

    #[test]
    fn matcher_replays_timer() {
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: TimerId::new("cooldown"),
                duration_secs: 300,
            },
            WorkflowEvent::TimerFired {
                timer_id: TimerId::new("cooldown"),
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_timer("cooldown");
        assert_eq!(
            result,
            HistoryMatch::Matched {
                output: Value::Null
            }
        );
        assert_eq!(matcher.position(), 2);
    }

    #[test]
    fn matcher_timer_no_match_at_end() {
        let mut matcher = HistoryMatcher::new(vec![]);
        let result = matcher.match_timer("t1");
        assert_eq!(result, HistoryMatch::NoMatch);
    }

    // ── Cancellable / renewable durable timers (issue #768) ──────────────────

    fn ts(id: &str, dur: u64) -> WorkflowEvent {
        WorkflowEvent::TimerStarted {
            timer_id: TimerId::new(id),
            duration_secs: dur,
        }
    }
    fn tf(id: &str) -> WorkflowEvent {
        WorkflowEvent::TimerFired {
            timer_id: TimerId::new(id),
        }
    }
    fn tc(id: &str) -> WorkflowEvent {
        WorkflowEvent::TimerCancelled {
            timer_id: TimerId::new(id),
        }
    }

    #[test]
    fn matcher_timer_arm_consumes_started() {
        let mut m = HistoryMatcher::new(vec![ts("idle", 300)]);
        assert_eq!(
            m.match_timer_arm("idle", 300),
            HistoryMatch::Matched {
                output: Value::Null
            }
        );
        assert_eq!(m.position(), 1);
    }

    #[test]
    fn matcher_timer_arm_no_match_past_end() {
        let mut m = HistoryMatcher::new(vec![]);
        assert_eq!(m.match_timer_arm("idle", 300), HistoryMatch::NoMatch);
    }

    #[test]
    fn matcher_timer_arm_diverges_on_duration() {
        let mut m = HistoryMatcher::new(vec![ts("idle", 300)]);
        assert!(matches!(
            m.match_timer_arm("idle", 600),
            HistoryMatch::Diverged { .. }
        ));
    }

    #[test]
    fn matcher_timer_cancel_finds_and_consumes() {
        let mut m = HistoryMatcher::new(vec![ts("idle", 300), tc("idle")]);
        assert!(matches!(
            m.match_timer_arm("idle", 300),
            HistoryMatch::Matched { .. }
        ));
        assert!(matches!(
            m.match_timer_cancel("idle"),
            HistoryMatch::Matched { .. }
        ));
    }

    #[test]
    fn matcher_timer_cancel_no_match_when_absent() {
        let mut m = HistoryMatcher::new(vec![ts("idle", 300)]);
        assert!(matches!(
            m.match_timer_arm("idle", 300),
            HistoryMatch::Matched { .. }
        ));
        assert_eq!(m.match_timer_cancel("idle"), HistoryMatch::NoMatch);
    }

    #[test]
    fn matcher_timer_or_cancel_fired() {
        let mut m = HistoryMatcher::new(vec![ts("idle", 300), tf("idle")]);
        assert!(matches!(
            m.match_timer_arm("idle", 300),
            HistoryMatch::Matched { .. }
        ));
        assert_eq!(m.match_timer_or_cancel("idle"), TimerFireMatch::Fired);
    }

    #[test]
    fn matcher_timer_or_cancel_cancelled() {
        let mut m = HistoryMatcher::new(vec![ts("idle", 300), tc("idle")]);
        assert!(matches!(
            m.match_timer_arm("idle", 300),
            HistoryMatch::Matched { .. }
        ));
        assert_eq!(m.match_timer_or_cancel("idle"), TimerFireMatch::Cancelled);
    }

    #[test]
    fn matcher_timer_or_cancel_fire_wins_over_later_cancel() {
        let mut m = HistoryMatcher::new(vec![ts("idle", 300), tf("idle"), tc("idle")]);
        assert!(matches!(
            m.match_timer_arm("idle", 300),
            HistoryMatch::Matched { .. }
        ));
        assert_eq!(m.match_timer_or_cancel("idle"), TimerFireMatch::Fired);
    }

    #[test]
    fn matcher_timer_or_cancel_cancel_wins_over_later_fire() {
        let mut m = HistoryMatcher::new(vec![ts("idle", 300), tc("idle"), tf("idle")]);
        assert!(matches!(
            m.match_timer_arm("idle", 300),
            HistoryMatch::Matched { .. }
        ));
        assert_eq!(m.match_timer_or_cancel("idle"), TimerFireMatch::Cancelled);
    }

    #[test]
    fn matcher_timer_or_cancel_no_match_when_unresolved() {
        let mut m = HistoryMatcher::new(vec![ts("idle", 300)]);
        assert!(matches!(
            m.match_timer_arm("idle", 300),
            HistoryMatch::Matched { .. }
        ));
        assert_eq!(m.match_timer_or_cancel("idle"), TimerFireMatch::NoMatch);
    }

    #[test]
    fn matcher_timer_or_cancel_crosses_foreign_sibling_fire() {
        // Concurrent `join!(slow.await_fire(), fast.await_fire())`: both timers
        // armed, `fast` fires first, then `slow`. On replay the `slow` branch is
        // polled first; its outcome scan must CROSS the unconsumed foreign
        // `TimerFired(fast)` and claim its own `TimerFired(slow)`, leaving fast's
        // fire for fast's own scan (Codex P2 round 14, issue #768). Before the fix
        // the foreign `TimerFired` STOPPED the scan → NoMatch → false divergence.
        let mut m = HistoryMatcher::new(vec![
            ts("slow", 300),
            ts("fast", 60),
            tf("fast"),
            tf("slow"),
        ]);
        assert!(matches!(
            m.match_timer_arm("slow", 300),
            HistoryMatch::Matched { .. }
        ));
        assert!(matches!(
            m.match_timer_arm("fast", 60),
            HistoryMatch::Matched { .. }
        ));
        // slow polled first must cross the foreign fast fire and resolve Fired.
        assert_eq!(m.match_timer_or_cancel("slow"), TimerFireMatch::Fired);
        assert!(
            !m.timer_scan_stopped_at_command(),
            "crossing a foreign sibling fire must NOT set the blocked-scan flag"
        );
        // fast's own fire is still claimable afterward (crossed non-consumingly).
        assert_eq!(m.match_timer_or_cancel("fast"), TimerFireMatch::Fired);
    }

    #[test]
    fn matcher_timer_cancel_crosses_foreign_sibling_fire() {
        // The shared crossable set applies to the cancel scan too: a concurrent
        // sibling `await_fire` firing while this timer is being cancelled must be
        // crossed non-consumingly (Codex P2 round 14, issue #768).
        let mut m = HistoryMatcher::new(vec![
            ts("keep", 300),
            ts("drop", 60),
            tf("keep"),
            tc("drop"),
        ]);
        assert!(matches!(
            m.match_timer_arm("keep", 300),
            HistoryMatch::Matched { .. }
        ));
        assert!(matches!(
            m.match_timer_arm("drop", 60),
            HistoryMatch::Matched { .. }
        ));
        // The cancel scan for `drop` must cross the foreign `TimerFired(keep)`.
        assert!(matches!(
            m.match_timer_cancel("drop"),
            HistoryMatch::Matched { .. }
        ));
        assert!(!m.timer_scan_stopped_at_command());
        // keep's own fire is still claimable.
        assert_eq!(m.match_timer_or_cancel("keep"), TimerFireMatch::Fired);
    }

    #[test]
    fn matcher_timer_or_cancel_still_stops_at_foreign_command() {
        // A foreign `TimerFired` now crosses, but a genuine command (an activity)
        // still STOPS the outcome scan — the round-5/13 soundness gate is
        // preserved (Codex P2 round 14, issue #768).
        let aid = ActivityExecId::new();
        let mut m = HistoryMatcher::new(vec![
            ts("idle", 300),
            WorkflowEvent::ActivityScheduled {
                activity_id: aid,
                name: "work".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            tf("idle"),
        ]);
        assert!(matches!(
            m.match_timer_arm("idle", 300),
            HistoryMatch::Matched { .. }
        ));
        // await_fire BEFORE the recorded activity → scan STOPS at the activity.
        assert_eq!(m.match_timer_or_cancel("idle"), TimerFireMatch::NoMatch);
        assert!(
            m.timer_scan_stopped_at_command(),
            "a genuine command must still stop the scan"
        );
    }

    #[test]
    fn matcher_activity_scan_skips_interleaved_timer_cancel() {
        // A TimerCancelled interleaved between an activity's Scheduled and
        // Completed events must not break the activity scan, and must remain
        // claimable afterward (transparent, non-consuming skip).
        let aid = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::ActivityScheduled {
                activity_id: aid,
                name: "work".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            tc("idle"),
            WorkflowEvent::ActivityCompleted {
                activity_id: aid,
                output: serde_json::json!("done"),
            },
        ];
        let mut m = HistoryMatcher::new(events);
        assert_eq!(
            m.match_activity("work"),
            HistoryMatch::Matched {
                output: serde_json::json!("done")
            }
        );
        // The interleaved cancel is still claimable afterward.
        assert!(matches!(
            m.match_timer_cancel("idle"),
            HistoryMatch::Matched { .. }
        ));
    }

    #[test]
    fn matcher_signal_scan_skips_interleaved_timer_cancel() {
        // FINDING 3: a `[CancelTimer, WaitForSignal]` batch (a cancel_timer()/
        // reset() in the same cycle as a wait_for_signal, or a push signal
        // handler cancelling a timer) records `[TimerCancelled, SignalReceived]`.
        // The signal scan must step over the interleaved TimerCancelled and
        // still match the signal — before the fix it hit the `other =>` arm and
        // diverged.
        let events = vec![
            tc("idle"),
            WorkflowEvent::SignalReceived {
                signal_name: "approved".into(),
                payload: serde_json::json!({"ok": true}),
            },
        ];
        let mut m = HistoryMatcher::new(events);
        assert_eq!(
            m.match_signal("approved"),
            HistoryMatch::Matched {
                output: serde_json::json!({"ok": true})
            },
            "signal scan must step over an interleaved TimerCancelled"
        );
        // The interleaved cancel stays claimable afterward (non-consuming skip).
        assert!(matches!(
            m.match_timer_cancel("idle"),
            HistoryMatch::Matched { .. }
        ));
    }

    #[test]
    fn matcher_activity_scan_skips_interleaved_timer_started() {
        // FINDING 3: symmetry with the interleaved-TimerCancelled case — a
        // reset interleaved with an activity records `[TimerCancelled,
        // TimerStarted]` between Scheduled and Completed; the activity scan must
        // step over BOTH and still match the terminal (before the fix it rewound
        // on TimerCancelled but broke on TimerStarted → ActivityInProgress).
        let aid = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::ActivityScheduled {
                activity_id: aid,
                name: "work".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            tc("idle"),
            ts("idle", 300),
            WorkflowEvent::ActivityCompleted {
                activity_id: aid,
                output: serde_json::json!("done"),
            },
        ];
        let mut m = HistoryMatcher::new(events);
        assert_eq!(
            m.match_activity("work"),
            HistoryMatch::Matched {
                output: serde_json::json!("done")
            },
            "activity scan must step over an interleaved reset's TimerStarted"
        );
    }

    #[test]
    fn matcher_timer_detects_divergence() {
        let id = ActivityExecId::new();
        let events = vec![WorkflowEvent::ActivityScheduled {
            activity_id: id,
            name: "foo".into(),
            input: Value::Null,
            queue: "default".into(),
        }];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_timer("t1");
        assert!(matches!(result, HistoryMatch::Diverged { .. }));
    }

    #[test]
    fn matcher_replays_continue_as_new() {
        let payload = serde_json::json!({"phase": "next"});
        let events = vec![WorkflowEvent::WorkflowContinuedAsNew {
            new_exec_id: ExecutionId::new(),
            input: payload.clone(),
            new_workflow_type: None,
        }];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_continue_as_new(&payload, None);
        assert_eq!(result, HistoryMatch::Matched { output: payload });
        assert_eq!(matcher.position(), 1);
        assert!(!matcher.is_replaying());
    }

    #[test]
    fn matcher_continue_as_new_input_mismatch_diverges() {
        let events = vec![WorkflowEvent::WorkflowContinuedAsNew {
            new_exec_id: ExecutionId::new(),
            input: serde_json::json!({"phase": "next"}),
            new_workflow_type: None,
        }];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_continue_as_new(&serde_json::json!({"phase": "later"}), None);
        assert!(matches!(result, HistoryMatch::Diverged { .. }));
        assert_eq!(matcher.position(), 0);
    }

    #[test]
    fn matcher_continue_as_new_wrong_event_diverges() {
        let events = vec![WorkflowEvent::TimerStarted {
            timer_id: TimerId::new("t1"),
            duration_secs: 60,
        }];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_continue_as_new(&serde_json::json!({"phase": "next"}), None);
        assert!(matches!(result, HistoryMatch::Diverged { .. }));
        assert_eq!(matcher.position(), 0);
    }

    // ── Cross-type continue-as-new matching (issue #803) ────────────────

    /// AC6: a recorded cross-type continuation replays to the same successor
    /// type it was recorded with.
    #[test]
    fn matcher_replays_cross_type_continue_as_new() {
        let payload = serde_json::json!({"tier": "paid"});
        let events = vec![WorkflowEvent::WorkflowContinuedAsNew {
            new_exec_id: ExecutionId::new(),
            input: payload.clone(),
            new_workflow_type: Some("paid_subscription".to_string()),
        }];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_continue_as_new(&payload, Some("paid_subscription"));
        assert_eq!(result, HistoryMatch::Matched { output: payload });
        assert_eq!(matcher.position(), 1);
    }

    /// AC6: redirecting a continuation to a *different* type is a code change
    /// the recorded history cannot satisfy — it must surface as divergence,
    /// not silently fork the chain into another phase.
    #[test]
    fn matcher_continue_as_new_type_mismatch_diverges() {
        let payload = serde_json::json!({"tier": "paid"});
        let events = vec![WorkflowEvent::WorkflowContinuedAsNew {
            new_exec_id: ExecutionId::new(),
            input: payload.clone(),
            new_workflow_type: Some("paid_subscription".to_string()),
        }];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_continue_as_new(&payload, Some("churned"));
        match result {
            HistoryMatch::Diverged {
                expected, actual, ..
            } => {
                assert!(expected.contains("churned"), "got {expected}");
                assert!(actual.contains("paid_subscription"), "got {actual}");
            }
            other => panic!("expected divergence, got {other:?}"),
        }
        assert_eq!(matcher.position(), 0, "a divergence must not advance");
    }

    /// `None` (same type) and `Some(t)` are distinct recorded intents:
    /// *adding* a cross-type call over a same-type history diverges…
    #[test]
    fn matcher_same_type_history_diverges_against_a_cross_type_call() {
        let payload = serde_json::json!({"cycle": 2});
        let events = vec![WorkflowEvent::WorkflowContinuedAsNew {
            new_exec_id: ExecutionId::new(),
            input: payload.clone(),
            new_workflow_type: None,
        }];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_continue_as_new(&payload, Some("paid_subscription"));
        assert!(matches!(result, HistoryMatch::Diverged { .. }));
        assert_eq!(matcher.position(), 0);
    }

    /// …and *removing* it over a cross-type history diverges too.
    #[test]
    fn matcher_cross_type_history_diverges_against_a_same_type_call() {
        let payload = serde_json::json!({"cycle": 2});
        let events = vec![WorkflowEvent::WorkflowContinuedAsNew {
            new_exec_id: ExecutionId::new(),
            input: payload.clone(),
            new_workflow_type: Some("paid_subscription".to_string()),
        }];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_continue_as_new(&payload, None);
        assert!(matches!(result, HistoryMatch::Diverged { .. }));
        assert_eq!(matcher.position(), 0);
    }

    /// The type is compared **before** the input, so a run that changed both
    /// reports the type divergence — the actionable one for a phase redirect.
    #[test]
    fn matcher_continue_as_new_reports_type_divergence_before_input() {
        let events = vec![WorkflowEvent::WorkflowContinuedAsNew {
            new_exec_id: ExecutionId::new(),
            input: serde_json::json!({"a": 1}),
            new_workflow_type: Some("paid_subscription".to_string()),
        }];

        let mut matcher = HistoryMatcher::new(events);
        match matcher.match_continue_as_new(&serde_json::json!({"b": 2}), Some("churned")) {
            HistoryMatch::Diverged { expected, .. } => {
                assert!(expected.contains("Type"), "got {expected}");
            }
            other => panic!("expected divergence, got {other:?}"),
        }
    }

    #[test]
    fn matcher_replays_version_marker() {
        let events = vec![WorkflowEvent::MarkerRecorded {
            name: "version:billing_v2".into(),
            details: serde_json::json!(2),
        }];

        let mut matcher = HistoryMatcher::new(events);
        let version = matcher.match_version("billing_v2", 1, 3);
        assert_eq!(version, 2);
        assert_eq!(matcher.position(), 1);
    }

    #[test]
    fn matcher_version_returns_min_for_old_workflow() {
        // Old workflow has a different event at this position — no marker
        let events = vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }];

        let mut matcher = HistoryMatcher::new(events);
        let version = matcher.match_version("billing_v2", 1, 3);
        assert_eq!(version, 1);
        // Cursor should NOT advance — the event isn't consumed
        assert_eq!(matcher.position(), 0);
    }

    #[test]
    fn matcher_version_returns_max_past_history() {
        let mut matcher = HistoryMatcher::new(vec![]);
        let version = matcher.match_version("billing_v2", 1, 3);
        assert_eq!(version, 3);
    }

    // ── Patch markers (issue #687) ────────────────────────────────────────

    #[test]
    fn matcher_patch_marker_recorded_at_cursor() {
        let events = vec![WorkflowEvent::MarkerRecorded {
            name: "patch:billing_v2".into(),
            details: serde_json::json!(1),
        }];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_patch_marker("billing_v2");
        assert_eq!(result, PatchMarkerMatch::Recorded);
        assert_eq!(matcher.position(), 1);
    }

    #[test]
    fn matcher_patch_marker_absent_returns_absent_without_advancing() {
        // Pre-patch history: a timer where the patched() call would look.
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: TimerId::new("t1"),
                duration_secs: 60,
            },
            WorkflowEvent::TimerFired {
                timer_id: TimerId::new("t1"),
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_patch_marker("billing_v2");
        assert_eq!(result, PatchMarkerMatch::Absent);
        // Cursor must NOT advance — the event isn't consumed …
        assert_eq!(matcher.position(), 0);
        // … so the actual event at the cursor still matches cleanly.
        let timer = matcher.match_timer("t1");
        assert!(matches!(timer, HistoryMatch::Matched { .. }));
    }

    #[test]
    fn matcher_patch_marker_past_history_is_newly_patched() {
        let mut matcher = HistoryMatcher::new(vec![]);
        let result = matcher.match_patch_marker("billing_v2");
        assert_eq!(result, PatchMarkerMatch::NewlyPatched);
    }

    #[test]
    fn matcher_patch_marker_interop_consumes_version_marker() {
        // A run that recorded a `version:` marker under the old ctx.version()
        // API is observed as patched, regardless of the recorded number.
        let events = vec![WorkflowEvent::MarkerRecorded {
            name: "version:billing_v2".into(),
            details: serde_json::json!(2),
        }];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_patch_marker("billing_v2");
        assert_eq!(result, PatchMarkerMatch::Recorded);
        assert_eq!(matcher.position(), 1);
    }

    #[test]
    fn matcher_deprecate_patch_marks_all_matching_markers_consumed() {
        // Two markers for the same id (patched() called twice in phase 1),
        // one of them *later* in history than the cursor when deprecation
        // runs — both must become transparent.
        let events = vec![
            WorkflowEvent::MarkerRecorded {
                name: "patch:x".into(),
                details: serde_json::json!(1),
            },
            WorkflowEvent::TimerStarted {
                timer_id: TimerId::new("t1"),
                duration_secs: 60,
            },
            WorkflowEvent::TimerFired {
                timer_id: TimerId::new("t1"),
            },
            WorkflowEvent::MarkerRecorded {
                name: "patch:x".into(),
                details: serde_json::json!(1),
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        assert!(matcher.deprecate_patch("x"));
        // The first marker is transparent — the timer matches at the cursor.
        let timer = matcher.match_timer("t1");
        assert!(matches!(timer, HistoryMatch::Matched { .. }));
        // The trailing marker is transparent too — history is fully consumed.
        assert!(!matcher.is_replaying());
        // Idempotent: a second call reports the same memoized presence.
        assert!(matcher.deprecate_patch("x"));
    }

    #[test]
    fn matcher_deprecated_patch_memo_drives_match_patch_marker() {
        // (a) History WITH a marker: deprecate first, then a residual
        // patched() call → Recorded, without any cursor movement.
        let events = vec![WorkflowEvent::MarkerRecorded {
            name: "patch:x".into(),
            details: serde_json::json!(1),
        }];
        let mut matcher = HistoryMatcher::new(events);
        assert!(matcher.deprecate_patch("x"));
        let pos_before = matcher.position();
        assert_eq!(matcher.match_patch_marker("x"), PatchMarkerMatch::Recorded);
        assert_eq!(matcher.position(), pos_before);

        // (b) History WITHOUT a marker: memoized absence → Absent, even on
        // the live frontier (no fresh marker may be recorded).
        let mut matcher = HistoryMatcher::new(vec![]);
        assert!(!matcher.deprecate_patch("x"));
        assert_eq!(matcher.match_patch_marker("x"), PatchMarkerMatch::Absent);
    }

    #[test]
    fn matcher_patch_marker_trailing_signal_history_is_absent() {
        // Pinned deliberately (review finding F3, exact parity with
        // match_version): a fresh execution whose first-task history ends in
        // un-awaited signals at the gate point — canonically EVERY
        // signal-with-start run, since the signal is staged before first
        // dispatch — drains the signal, lands past cursor-based history, and
        // must conservatively take the OLD branch (Absent, no marker
        // recorded). The history is ambiguous with a phase-0 run parked at a
        // first-line wait_for_signal, so this is not treated as a live
        // frontier.
        let events = vec![WorkflowEvent::SignalReceived {
            signal_name: "kick".into(),
            payload: serde_json::json!({"n": 1}),
        }];

        let mut matcher = HistoryMatcher::new(events);
        assert_eq!(
            matcher.match_patch_marker("billing_v2"),
            PatchMarkerMatch::Absent,
            "a trailing-signal history must be treated as a recorded, \
             unpatched position — never as the live frontier"
        );
    }

    #[test]
    fn matcher_patch_marker_later_marker_does_not_match_at_cursor() {
        // Positional-semantics pin (review finding F5): the marker lookup is
        // cursor-based, not a whole-history scan. A `patch:x` marker recorded
        // LATER in history must not satisfy a patched() call at an earlier
        // position.
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: TimerId::new("t1"),
                duration_secs: 60,
            },
            WorkflowEvent::TimerFired {
                timer_id: TimerId::new("t1"),
            },
            WorkflowEvent::MarkerRecorded {
                name: "patch:x".into(),
                details: serde_json::json!(1),
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        assert_eq!(matcher.match_patch_marker("x"), PatchMarkerMatch::Absent);
        // Cursor untouched — the timer at position 0 still matches.
        assert_eq!(matcher.position(), 0);
        let timer = matcher.match_timer("t1");
        assert!(matches!(timer, HistoryMatch::Matched { .. }));
        // The trailing marker was left unconsumed: a patched() call at ITS
        // position still consumes it.
        assert_eq!(matcher.match_patch_marker("x"), PatchMarkerMatch::Recorded);
        assert_eq!(matcher.position(), 3);
    }

    #[test]
    fn matcher_deprecate_patch_sees_marker_recorded_this_cycle() {
        // The sandwich flip (review finding F1): on the LIVE cycle a
        // `patched(id)` call records its marker only as a pending
        // WorkflowCommand — a subsequent `deprecate_patch(id)`'s full-history
        // scan cannot see it. Without the this-cycle latch the memo latches
        // `false`, a residual `patched(id)` returns false on the live pass
        // and true on every replay pass → permanent nd-block.
        let mut matcher = HistoryMatcher::new(vec![]);
        assert_eq!(
            matcher.match_patch_marker("x"),
            PatchMarkerMatch::NewlyPatched
        );
        assert!(
            matcher.deprecate_patch("x"),
            "deprecate_patch must see a marker recorded earlier in the SAME cycle"
        );
        assert_eq!(
            matcher.match_patch_marker("x"),
            PatchMarkerMatch::Recorded,
            "the residual patched() call must agree with every replay cycle"
        );
    }

    #[test]
    fn matcher_deprecate_patch_sees_version_marker_recorded_this_cycle() {
        // Version-interop variant of the sandwich flip: a live
        // `ctx.version(id, ..)` call returning max is exactly the case where
        // the context pushes a `version:{id}` marker command — a same-cycle
        // `deprecate_patch(id)` must observe it as present.
        let mut matcher = HistoryMatcher::new(vec![]);
        assert_eq!(matcher.match_version("x", 1, 2), 2);
        assert!(
            matcher.deprecate_patch("x"),
            "deprecate_patch must see a version marker recorded this cycle"
        );
        assert_eq!(matcher.match_patch_marker("x"), PatchMarkerMatch::Recorded);
    }

    // ── Saga compensation markers (issue #801) ───────────────────────────

    #[test]
    fn matcher_saga_marker_live_frontier_on_empty_history() {
        // Past the end of history — the first live unwind. The caller records
        // a fresh marker and emits the counter exactly here.
        let mut matcher = HistoryMatcher::new(vec![]);
        assert_eq!(
            matcher.match_saga_marker("saga_compensated:1"),
            SagaMarkerMatch::LiveFrontier
        );
    }

    #[test]
    fn matcher_saga_marker_recorded_consumes_marker() {
        // A previously recorded marker at the cursor is consumed and reported
        // as Recorded — the caller stays silent (no re-emit on replay).
        let events = vec![WorkflowEvent::MarkerRecorded {
            name: "saga_compensated:1".into(),
            details: serde_json::json!(3),
        }];

        let mut matcher = HistoryMatcher::new(events);
        assert_eq!(
            matcher.match_saga_marker("saga_compensated:1"),
            SagaMarkerMatch::Recorded
        );
        assert_eq!(matcher.position(), 1);
    }

    #[test]
    fn matcher_saga_marker_absent_on_foreign_event_leaves_cursor() {
        // Backward-compat money arm: a pre-#801 history holds the unwind's
        // first compensation activity where the marker would sit. The match
        // must be non-mutating so the recorded event still matches the next
        // command cleanly — never a divergence, never an emit.
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "release_reservation".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: Value::Null,
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        assert_eq!(
            matcher.match_saga_marker("saga_compensated:1"),
            SagaMarkerMatch::Absent
        );
        // Cursor must NOT advance — the event isn't consumed …
        assert_eq!(matcher.position(), 0);
        // … so the compensation activity at the cursor still matches cleanly.
        let activity = matcher.match_activity("release_reservation");
        assert!(matches!(activity, HistoryMatch::Matched { .. }));
    }

    #[test]
    fn matcher_saga_marker_drained_signal_frontier_after_trailing_signals() {
        // Post-drain arm: a history whose tail is un-awaited signals at the
        // unwind point is reported as the distinguishable drained-signal
        // frontier — the CALLER resolves it against the unwind's disposition
        // (conservative for the start observe, coupled for the failed
        // observe; post-review P2-1/P2-2).
        let events = vec![WorkflowEvent::SignalReceived {
            signal_name: "kick".into(),
            payload: serde_json::json!({"n": 1}),
        }];

        let mut matcher = HistoryMatcher::new(events);
        assert_eq!(
            matcher.match_saga_marker("saga_compensated:1"),
            SagaMarkerMatch::DrainedSignalFrontier,
            "a trailing-signal history is a drained-signal frontier, \
             never a plain live frontier and never a plain Absent"
        );
    }

    #[test]
    fn matcher_saga_marker_recorded_past_drained_trailing_signal() {
        // The shape the P2-1 fix itself persists: the failure marker recorded
        // past a drained trailing signal must be found (and consumed) on the
        // next cycle so the counter never re-emits.
        let events = vec![
            WorkflowEvent::SignalReceived {
                signal_name: "dup_cancel".into(),
                payload: serde_json::json!({"n": 1}),
            },
            WorkflowEvent::MarkerRecorded {
                name: "saga_compensation_failed:1".into(),
                details: serde_json::json!(1),
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        assert_eq!(
            matcher.match_saga_marker("saga_compensation_failed:1"),
            SagaMarkerMatch::Recorded
        );
    }

    #[test]
    fn matcher_saga_marker_live_frontier_past_trailing_cancellation_event() {
        // Cancel-and-compensate: WorkflowCancelled has no workflow-command
        // counterpart and is never consumed by any matcher, so it must not
        // hide the frontier from a metrics-only marker — the unwind of a
        // freshly-cancelled run IS the live frontier.
        let events = vec![WorkflowEvent::WorkflowCancelled {
            reason: "operator shutdown".into(),
        }];

        let mut matcher = HistoryMatcher::new(events);
        assert_eq!(
            matcher.match_saga_marker("saga_compensated:1"),
            SagaMarkerMatch::LiveFrontier,
            "a trailing WorkflowCancelled must not suppress the cancel-and-compensate count"
        );
        // Non-destructive: the cancellation event is not consumed.
        assert_eq!(matcher.position(), 0);
    }

    #[test]
    fn matcher_saga_marker_recorded_out_of_order_past_cancellation_event() {
        // Next cycle of the cancel-and-compensate shape: the marker persisted
        // AFTER the (never-consumed) cancellation event must be found and
        // consumed out-of-order, leaving the cursor untouched.
        let events = vec![
            WorkflowEvent::WorkflowCancelled {
                reason: "operator shutdown".into(),
            },
            WorkflowEvent::MarkerRecorded {
                name: "saga_compensated:1".into(),
                details: serde_json::json!(2),
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        assert_eq!(
            matcher.match_saga_marker("saga_compensated:1"),
            SagaMarkerMatch::Recorded
        );
        assert_eq!(
            matcher.position(),
            0,
            "out-of-order consumption must leave the cursor at the cancellation event"
        );
        // Idempotent across a second unwind lookup for a different seq: the
        // consumed marker is skipped, the frontier is still reported.
        assert_eq!(
            matcher.match_saga_marker("saga_compensated:2"),
            SagaMarkerMatch::LiveFrontier
        );
    }

    #[test]
    fn matcher_saga_marker_absent_at_terminal_event_after_cancellation() {
        // A fully-recorded terminal run (e.g. a WorkflowReplayer fixture of a
        // pre-#801 cancelled run) must stay uncounted: the lookahead stops at
        // the terminal event — never a retroactive count.
        let events = vec![
            WorkflowEvent::WorkflowCancelled {
                reason: "operator shutdown".into(),
            },
            WorkflowEvent::WorkflowCompleted {
                output: Value::Null,
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        assert_eq!(
            matcher.match_saga_marker("saga_compensated:1"),
            SagaMarkerMatch::Absent
        );
        assert_eq!(matcher.position(), 0);
    }

    #[test]
    fn matcher_saga_marker_distinguishes_seq_by_name() {
        // Two unwinds in one workflow get distinct seq-numbered markers; a
        // lookup for seq 2 must not consume seq 1's marker.
        let events = vec![WorkflowEvent::MarkerRecorded {
            name: "saga_compensated:1".into(),
            details: serde_json::json!(2),
        }];

        let mut matcher = HistoryMatcher::new(events);
        assert_eq!(
            matcher.match_saga_marker("saga_compensated:2"),
            SagaMarkerMatch::Absent
        );
        assert_eq!(matcher.position(), 0);
        // The correctly-named lookup still consumes it.
        assert_eq!(
            matcher.match_saga_marker("saga_compensated:1"),
            SagaMarkerMatch::Recorded
        );
        assert_eq!(matcher.position(), 1);
    }

    #[test]
    fn matcher_replays_child_workflow_completion() {
        let child_id = crate::types::ExecutionId::new();
        let output = serde_json::json!({"result": "ok"});
        let events = vec![
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "process_order".into(),
                input: serde_json::json!({"id": 42}),
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id,
                output: output.clone(),
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_child_workflow("process_order", &serde_json::json!({"id": 42}));
        assert_eq!(result, HistoryMatch::Matched { output });
        assert_eq!(matcher.position(), 2);
    }

    #[test]
    fn matcher_replays_child_workflow_failure() {
        let child_id = crate::types::ExecutionId::new();
        let events = vec![
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "process_order".into(),
                input: Value::Null,
            },
            WorkflowEvent::child_workflow_failed(child_id, "child failed"),
        ];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_child_workflow("process_order", &Value::Null);
        assert_eq!(
            result,
            HistoryMatch::Failed {
                error: "child failed".into(),
                attempt: 1,
                error_type: "Error".into(),
                details: None,
                non_retryable: false,
            }
        );
    }

    #[test]
    fn matcher_replays_typed_child_workflow_failure() {
        // A child that failed with a typed `WorkflowFailure` surfaces its
        // error_type / details / non_retryable through `HistoryMatch::Failed`
        // (issue #767).
        let child_id = crate::types::ExecutionId::new();
        let decoded = crate::failure::DecodedWorkflowFailure {
            message: "card declined".into(),
            error_type: Some("ValidationRejected".into()),
            details: Some(serde_json::json!({ "code": 402 })),
            non_retryable: Some(true),
        };
        let events = vec![
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "charge_card".into(),
                input: Value::Null,
            },
            WorkflowEvent::child_workflow_failed_typed(child_id, &decoded),
        ];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_child_workflow("charge_card", &Value::Null);
        assert_eq!(
            result,
            HistoryMatch::Failed {
                error: "card declined".into(),
                attempt: 1,
                error_type: "ValidationRejected".into(),
                details: Some(serde_json::json!({ "code": 402 })),
                non_retryable: true,
            }
        );
    }

    #[test]
    fn matcher_child_workflow_without_terminal_returns_child_in_progress() {
        let child_id = crate::types::ExecutionId::new();
        let events = vec![WorkflowEvent::ChildWorkflowStarted {
            child_id,
            workflow_name: "process_order".into(),
            input: Value::Null,
        }];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_child_workflow("process_order", &Value::Null);
        assert!(
            matches!(result, HistoryMatch::ChildInProgress { child_id: id } if id == child_id),
            "should be ChildInProgress with the known child_id, got {result:?}"
        );
        // Cursor advances past the ChildWorkflowStarted event so subsequent
        // commands (e.g. other parallel children) can be matched correctly.
        assert_eq!(
            matcher.position(),
            1,
            "cursor must advance past started event"
        );
    }

    #[test]
    fn matcher_child_workflow_scans_past_interleaved_events() {
        let child_a = crate::types::ExecutionId::new();
        let child_b = crate::types::ExecutionId::new();
        let events = vec![
            WorkflowEvent::ChildWorkflowStarted {
                child_id: child_a,
                workflow_name: "process_order".into(),
                input: serde_json::json!({"id": 1}),
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id: child_b,
                workflow_name: "process_order".into(),
                input: serde_json::json!({"id": 2}),
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id: child_a,
                output: serde_json::json!({"ok": true}),
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_child_workflow("process_order", &serde_json::json!({"id": 1}));
        assert_eq!(
            result,
            HistoryMatch::Matched {
                output: serde_json::json!({"ok": true}),
            }
        );
        assert_eq!(matcher.position(), 1);
    }

    #[test]
    fn matcher_child_workflow_input_mismatch_diverges() {
        let child_id = crate::types::ExecutionId::new();
        let events = vec![
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "process_order".into(),
                input: serde_json::json!({"sku":"book"}),
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id,
                output: serde_json::json!({"ok": true}),
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        let result =
            matcher.match_child_workflow("process_order", &serde_json::json!({"sku":"magazine"}));
        assert!(matches!(result, HistoryMatch::Diverged { .. }));
        assert_eq!(matcher.position(), 0);
    }

    #[test]
    fn matcher_child_workflow_keeps_interleaved_starts_replayable() {
        let child_a = crate::types::ExecutionId::new();
        let child_b = crate::types::ExecutionId::new();
        let events = vec![
            WorkflowEvent::ChildWorkflowStarted {
                child_id: child_a,
                workflow_name: "process_order".into(),
                input: serde_json::json!({"id": "A"}),
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id: child_b,
                workflow_name: "process_order".into(),
                input: serde_json::json!({"id": "B"}),
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id: child_a,
                output: serde_json::json!({"id": "A", "ok": true}),
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id: child_b,
                output: serde_json::json!({"id": "B", "ok": true}),
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        let a = matcher.match_child_workflow("process_order", &serde_json::json!({"id":"A"}));
        assert_eq!(
            a,
            HistoryMatch::Matched {
                output: serde_json::json!({"id": "A", "ok": true}),
            }
        );
        // Cursor should stay at Started(B), not advance past it.
        assert_eq!(matcher.position(), 1);

        let b = matcher.match_child_workflow("process_order", &serde_json::json!({"id":"B"}));
        assert_eq!(
            b,
            HistoryMatch::Matched {
                output: serde_json::json!({"id": "B", "ok": true}),
            }
        );
        assert_eq!(matcher.position(), 4);
    }

    #[test]
    fn matcher_child_workflow_keeps_interleaved_activity_replayable() {
        let child_a = crate::types::ExecutionId::new();
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::ChildWorkflowStarted {
                child_id: child_a,
                workflow_name: "process_order".into(),
                input: serde_json::json!({"id": "A"}),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "send_email".into(),
                input: serde_json::json!({"id":"A"}),
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!({"sent": true}),
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id: child_a,
                output: serde_json::json!({"id": "A", "ok": true}),
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        let child = matcher.match_child_workflow("process_order", &serde_json::json!({"id":"A"}));
        assert!(matches!(child, HistoryMatch::Matched { .. }));
        // Cursor should remain at the interleaved activity schedule.
        assert_eq!(matcher.position(), 1);

        let activity = matcher.match_activity("send_email");
        assert_eq!(
            activity,
            HistoryMatch::Matched {
                output: serde_json::json!({"sent": true}),
            }
        );
        // The consumed child terminal event is skipped automatically.
        assert_eq!(matcher.position(), 4);
    }

    #[test]
    fn matcher_activity_scan_skips_consumed_interleaved_child_terminal() {
        let child_id = crate::types::ExecutionId::new();
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "process_order".into(),
                input: serde_json::json!({"id": "A"}),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "send_email".into(),
                input: serde_json::json!({"id":"A"}),
                queue: "default".into(),
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id,
                output: serde_json::json!({"id": "A", "ok": true}),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!({"sent": true}),
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        let child = matcher.match_child_workflow("process_order", &serde_json::json!({"id":"A"}));
        assert!(matches!(child, HistoryMatch::Matched { .. }));
        assert_eq!(matcher.position(), 1);

        let activity = matcher.match_activity("send_email");
        assert_eq!(
            activity,
            HistoryMatch::Matched {
                output: serde_json::json!({"sent": true}),
            }
        );
        assert_eq!(matcher.position(), 4);
    }

    #[test]
    fn matcher_activity_scan_skips_interleaved_child_start() {
        let child_id = crate::types::ExecutionId::new();
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "send_email".into(),
                input: serde_json::json!({"id":"A"}),
                queue: "default".into(),
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "process_order".into(),
                input: serde_json::json!({"id":"A"}),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!({"sent": true}),
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        let activity = matcher.match_activity("send_email");
        assert_eq!(
            activity,
            HistoryMatch::Matched {
                output: serde_json::json!({"sent": true}),
            }
        );
        // Cursor stays at interleaved child start so child replay remains deterministic.
        assert_eq!(matcher.position(), 1);
    }

    #[test]
    fn matcher_activity_replay_preserves_interleaved_child_start_for_later_child_match() {
        let child_id = crate::types::ExecutionId::new();
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "send_email".into(),
                input: serde_json::json!({"id":"A"}),
                queue: "default".into(),
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "process_order".into(),
                input: serde_json::json!({"id":"A"}),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!({"sent": true}),
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id,
                output: serde_json::json!({"id":"A","ok": true}),
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        let activity = matcher.match_activity("send_email");
        assert!(matches!(activity, HistoryMatch::Matched { .. }));
        // Cursor should stay on ChildWorkflowStarted for later child replay.
        assert_eq!(matcher.position(), 1);

        let child = matcher.match_child_workflow("process_order", &serde_json::json!({"id":"A"}));
        assert!(matches!(child, HistoryMatch::Matched { .. }));
        assert_eq!(matcher.position(), 4);
    }

    #[test]
    fn matcher_timer_scan_skips_consumed_interleaved_child_terminal() {
        let child_id = crate::types::ExecutionId::new();
        let timer_id = TimerId::new("cooldown");
        let events = vec![
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "process_order".into(),
                input: serde_json::json!({"id":"A"}),
            },
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 30,
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id,
                output: serde_json::json!({"ok": true}),
            },
            WorkflowEvent::TimerFired { timer_id },
        ];

        let mut matcher = HistoryMatcher::new(events);
        let child = matcher.match_child_workflow("process_order", &serde_json::json!({"id":"A"}));
        assert!(matches!(child, HistoryMatch::Matched { .. }));
        assert_eq!(matcher.position(), 1);

        let timer = matcher.match_timer("cooldown");
        assert_eq!(
            timer,
            HistoryMatch::Matched {
                output: Value::Null
            }
        );
        assert_eq!(matcher.position(), 4);
    }

    #[test]
    fn matcher_timer_replay_preserves_interleaved_child_start_for_later_child_match() {
        let child_id = crate::types::ExecutionId::new();
        let timer_id = TimerId::new("cooldown");
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 30,
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "process_order".into(),
                input: serde_json::json!({"id":"A"}),
            },
            WorkflowEvent::TimerFired { timer_id },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id,
                output: serde_json::json!({"id":"A","ok": true}),
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        let timer = matcher.match_timer("cooldown");
        assert!(matches!(timer, HistoryMatch::Matched { .. }));
        // Cursor should stay on ChildWorkflowStarted for later child replay.
        assert_eq!(matcher.position(), 1);

        let child = matcher.match_child_workflow("process_order", &serde_json::json!({"id":"A"}));
        assert!(matches!(child, HistoryMatch::Matched { .. }));
        assert_eq!(matcher.position(), 4);
    }

    #[test]
    fn advance_skips_current_event() {
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::WorkflowCompleted {
                output: serde_json::json!({"done": true}),
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        assert_eq!(matcher.position(), 0);

        matcher.advance();
        assert_eq!(matcher.position(), 1);

        matcher.advance();
        assert_eq!(matcher.position(), 2);
        assert!(!matcher.is_replaying());

        // Advance past end is a no-op
        matcher.advance();
        assert_eq!(matcher.position(), 2);
    }

    #[test]
    fn matcher_replays_multiple_activities_in_sequence() {
        let id1 = ActivityExecId::new();
        let id2 = ActivityExecId::new();
        let output1 = serde_json::json!({"email_id": "msg-001"});
        let output2 = serde_json::json!({"charge_id": "ch-999"});

        let events = vec![
            WorkflowEvent::ActivityScheduled {
                activity_id: id1,
                name: "send_email".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: id1,
                output: output1.clone(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: id2,
                name: "charge_payment".into(),
                input: Value::Null,
                queue: "billing".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: id2,
                output: output2.clone(),
            },
        ];

        let mut matcher = HistoryMatcher::new(events);

        let r1 = matcher.match_activity("send_email");
        assert_eq!(r1, HistoryMatch::Matched { output: output1 });

        let r2 = matcher.match_activity("charge_payment");
        assert_eq!(r2, HistoryMatch::Matched { output: output2 });

        assert!(!matcher.is_replaying());
    }

    #[test]
    fn matcher_rewinds_to_later_sibling_when_earlier_activity_is_in_progress() {
        let earlier_id = ActivityExecId::new();
        let later_id = ActivityExecId::new();
        let later_output = serde_json::json!({"done": "later"});

        let events = vec![
            WorkflowEvent::ActivityScheduled {
                activity_id: earlier_id,
                name: "slow_task".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityStarted {
                activity_id: earlier_id,
                worker_id: WorkerId::new("worker-a"),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: later_id,
                name: "fast_task".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: later_id,
                output: later_output.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        assert_eq!(
            matcher.match_activity("slow_task"),
            HistoryMatch::ActivityInProgress {
                activity_id: earlier_id
            }
        );
        assert_eq!(
            matcher.position(),
            2,
            "in-progress replay must rewind to the first interleaved sibling command"
        );

        assert_eq!(
            matcher.match_activity("fast_task"),
            HistoryMatch::Matched {
                output: later_output
            }
        );
        assert!(!matcher.is_replaying());
    }

    #[test]
    fn matcher_treats_reset_fork_marker_as_informational() {
        let activity_id = ActivityExecId::new();
        let output = serde_json::json!({"ok": true});
        let events = vec![
            WorkflowEvent::WorkflowResetFork {
                reset_from_exec_id: ExecutionId::new(),
                reset_to_event_id: 0,
                reason: "bad deploy".into(),
                operator_id: "oncall".into(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "resume_work".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: output.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_activity("resume_work");
        assert_eq!(result, HistoryMatch::Matched { output });
        assert!(!matcher.is_replaying());
    }

    #[test]
    fn matcher_timer_started_peek_skips_detached_spawn() {
        let child_id = ExecutionId::new();
        let events = vec![
            WorkflowEvent::ChildWorkflowSpawnedDetached {
                child_id,
                workflow_name: "condition_sidecar".into(),
                input: Value::Null,
                parent_close_policy: ParentClosePolicy::Abandon,
            },
            WorkflowEvent::TimerStarted {
                timer_id: TimerId::new("condition-timeout"),
                duration_secs: 30,
            },
        ];
        let matcher = HistoryMatcher::new(events);

        assert!(
            matcher.is_timer_started_next("condition-timeout"),
            "timer peek should skip an unconsumed detached-spawn event"
        );
    }

    #[test]
    fn matcher_history_contains_timer_started_scans_whole_history() {
        // Present regardless of cursor position or consumption.
        let events = vec![
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"ok": true}),
            },
            WorkflowEvent::TimerStarted {
                timer_id: TimerId::new("__signal_timeout:1:approval"),
                duration_secs: 300,
            },
            WorkflowEvent::TimerFired {
                timer_id: TimerId::new("__signal_timeout:1:approval"),
            },
        ];
        let matcher = HistoryMatcher::new(events);
        assert!(
            matcher.history_contains_timer_started("__signal_timeout:1:approval"),
            "the armed deadline timer must be found anywhere in history"
        );
        assert!(
            !matcher.history_contains_timer_started("__signal_timeout:1:other"),
            "a different timer id must not match"
        );
    }

    #[test]
    fn matcher_history_contains_timer_started_false_when_never_armed() {
        // The stashed-signal short-circuit case: a signal arrived before the
        // race started, so no TimerStarted was ever recorded.
        let events = vec![WorkflowEvent::SignalReceived {
            signal_name: "approval".into(),
            payload: serde_json::json!({"approved": false}),
        }];
        let matcher = HistoryMatcher::new(events);
        assert!(
            !matcher.history_contains_timer_started("__signal_timeout:1:approval"),
            "no teardown target exists when the deadline timer was never armed"
        );
    }

    #[test]
    fn matcher_replays_signal_payload() {
        let events = vec![WorkflowEvent::SignalReceived {
            signal_name: "approved".into(),
            payload: serde_json::json!({"ok": true}),
        }];
        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_signal("approved");
        assert_eq!(
            result,
            HistoryMatch::Matched {
                output: serde_json::json!({"ok": true}),
            }
        );
    }

    #[test]
    fn matcher_skips_unrelated_signals_while_waiting() {
        let events = vec![
            WorkflowEvent::SignalReceived {
                signal_name: "cancel".into(),
                payload: serde_json::json!({"reason": "manual"}),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approved".into(),
                payload: serde_json::json!({"ok": true}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_signal("approved");
        assert_eq!(
            result,
            HistoryMatch::Matched {
                output: serde_json::json!({"ok": true}),
            }
        );
        assert_eq!(
            matcher.position(),
            2,
            "cursor should advance beyond matched signal to avoid stale divergences"
        );
    }

    #[test]
    fn matcher_preserves_unrelated_signal_for_later_wait() {
        let events = vec![
            WorkflowEvent::SignalReceived {
                signal_name: "cancel".into(),
                payload: serde_json::json!({"reason": "manual"}),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approved".into(),
                payload: serde_json::json!({"ok": true}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let approved = matcher.match_signal("approved");
        assert_eq!(
            approved,
            HistoryMatch::Matched {
                output: serde_json::json!({"ok": true}),
            }
        );

        let cancel = matcher.match_signal("cancel");
        assert_eq!(
            cancel,
            HistoryMatch::Matched {
                output: serde_json::json!({"reason": "manual"}),
            }
        );
    }

    // ── claim_pending_signal (issue #546: push-based signal handlers) ──────

    /// Strips event indices for assertions that only care about payloads.
    fn payloads_only(claimed: Vec<(usize, Value)>) -> Vec<Value> {
        claimed.into_iter().map(|(_, payload)| payload).collect()
    }

    #[test]
    fn claim_pending_signal_returns_matching_payloads_in_order() {
        let events = vec![
            WorkflowEvent::SignalReceived {
                signal_name: "cancel".into(),
                payload: serde_json::json!({"seq": 1}),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "cancel".into(),
                payload: serde_json::json!({"seq": 2}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        let claimed = payloads_only(matcher.claim_pending_signal("cancel"));
        assert_eq!(
            claimed,
            vec![serde_json::json!({"seq": 1}), serde_json::json!({"seq": 2})]
        );
    }

    #[test]
    fn claim_pending_signal_ignores_other_names() {
        let events = vec![
            WorkflowEvent::SignalReceived {
                signal_name: "approved".into(),
                payload: serde_json::json!({"ok": true}),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "cancel".into(),
                payload: serde_json::json!({"reason": "manual"}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        let claimed = payloads_only(matcher.claim_pending_signal("cancel"));
        assert_eq!(claimed, vec![serde_json::json!({"reason": "manual"})]);
    }

    #[test]
    fn claim_pending_signal_is_idempotent_within_one_matcher() {
        let events = vec![WorkflowEvent::SignalReceived {
            signal_name: "cancel".into(),
            payload: serde_json::json!({"reason": "manual"}),
        }];
        let mut matcher = HistoryMatcher::new(events);
        let first = payloads_only(matcher.claim_pending_signal("cancel"));
        assert_eq!(first, vec![serde_json::json!({"reason": "manual"})]);

        // Calling again must not re-deliver the same event.
        let second = matcher.claim_pending_signal("cancel");
        assert!(second.is_empty());
    }

    #[test]
    fn claim_pending_signal_claims_events_already_stashed_by_another_scan() {
        // An earlier scan for an unrelated activity stashes the "cancel" signal
        // into `pending_signals` on its way past it. `claim_pending_signal` must
        // still find it there even though it never appeared at the raw scan
        // position the claim itself would have looked at.
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::SignalReceived {
                signal_name: "cancel".into(),
                payload: serde_json::json!({"reason": "manual"}),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "send_email".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!("sent"),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        // Scanning for the activity stashes the interleaved "cancel" signal.
        let activity_result = matcher.match_activity("send_email");
        assert!(matches!(activity_result, HistoryMatch::Matched { .. }));

        let claimed = payloads_only(matcher.claim_pending_signal("cancel"));
        assert_eq!(claimed, vec![serde_json::json!({"reason": "manual"})]);
    }

    #[test]
    fn claim_pending_signal_does_not_advance_past_an_unconsumed_activity() {
        // The core regression this refactor fixes (PR #890 review, "eager
        // dispatch ignores history order"): a signal recorded AFTER an
        // ActivityScheduled/Completed pair the workflow hasn't reached yet
        // in this replay cycle must NOT be visible to a push handler. Only
        // once the workflow's own code actually matches that activity (and
        // the cursor advances past it) does the signal become claimable.
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "send_email".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!("sent"),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "cancel".into(),
                payload: serde_json::json!({"reason": "manual"}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        // Registering (and pumping) a handler before the workflow body has
        // matched the activity must see nothing yet.
        let claimed = matcher.claim_pending_signal("cancel");
        assert!(
            claimed.is_empty(),
            "must not claim a signal recorded after an unconsumed activity: {claimed:?}"
        );

        // The workflow body now actually reaches the activity call.
        let activity_result = matcher.match_activity("send_email");
        assert!(matches!(activity_result, HistoryMatch::Matched { .. }));

        // Only now is the trailing signal visible.
        let claimed = payloads_only(matcher.claim_pending_signal("cancel"));
        assert_eq!(claimed, vec![serde_json::json!({"reason": "manual"})]);
    }

    #[test]
    fn claim_pending_signal_does_not_steal_from_pull_based_wait() {
        // A later `wait_for_signal`/`match_signal` call for the same name must
        // never see an event that a prior `claim_pending_signal` call already
        // claimed (issue #546 AC: no double-delivery between push and pull).
        let events = vec![WorkflowEvent::SignalReceived {
            signal_name: "cancel".into(),
            payload: serde_json::json!({"reason": "manual"}),
        }];
        let mut matcher = HistoryMatcher::new(events);
        let claimed = payloads_only(matcher.claim_pending_signal("cancel"));
        assert_eq!(claimed, vec![serde_json::json!({"reason": "manual"})]);

        let pulled = matcher.match_signal("cancel");
        assert_eq!(
            pulled,
            HistoryMatch::NoMatch,
            "the signal was already claimed by the push handler"
        );
    }

    #[test]
    fn match_signal_does_not_steal_from_a_prior_claim() {
        // Symmetric to the above: once `match_signal` (pull) has consumed an
        // event, a later `claim_pending_signal` call for the same name must not
        // see it again either.
        let events = vec![
            WorkflowEvent::SignalReceived {
                signal_name: "cancel".into(),
                payload: serde_json::json!({"seq": 1}),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "cancel".into(),
                payload: serde_json::json!({"seq": 2}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        let pulled = matcher.match_signal("cancel");
        assert_eq!(
            pulled,
            HistoryMatch::Matched {
                output: serde_json::json!({"seq": 1})
            }
        );

        let claimed = payloads_only(matcher.claim_pending_signal("cancel"));
        assert_eq!(
            claimed,
            vec![serde_json::json!({"seq": 2})],
            "only the not-yet-pulled event should be delivered to the handler"
        );
    }

    #[test]
    fn claim_pending_signal_returns_empty_when_none_recorded() {
        let events = vec![WorkflowEvent::SignalReceived {
            signal_name: "approved".into(),
            payload: serde_json::json!({"ok": true}),
        }];
        let mut matcher = HistoryMatcher::new(events);
        let claimed = matcher.claim_pending_signal("cancel");
        assert!(claimed.is_empty());
    }

    #[test]
    fn claim_pending_signal_does_not_steal_from_an_open_signal_timeout_race() {
        // Regression: a push handler registered for the same name as an
        // in-flight signal-or-deadline race (issue #476) must not claim the
        // race's own signal -- doing so used to silently flip the race
        // outcome from SignalWon to TimerWon even though the signal
        // genuinely arrived before the deadline.
        let timer_id = "__signal_timeout:1:approval";
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: TimerId::new(timer_id),
                duration_secs: 300,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"approved": true}),
            },
            WorkflowEvent::TimerFired {
                timer_id: TimerId::new(timer_id),
            },
        ];

        // A push handler registered first (the idiomatic top-of-function
        // ordering) must not be able to claim the race's own signal. The
        // still-open race's own TimerStarted also hard-blocks the cursor
        // from reaching it at all, so this asserts the belt-and-suspenders
        // reservation check independently of that blocking behavior.
        let mut matcher = HistoryMatcher::new(events);
        let claimed = matcher.claim_pending_signal("approval");
        assert!(
            claimed.is_empty(),
            "the signal is reserved for the open race and must not be claimed: {claimed:?}"
        );

        // The race must still resolve exactly as it would with no push
        // handler involved at all.
        let result = matcher.match_signal_or_timer("approval", timer_id, Some(300));
        assert_eq!(
            result,
            SignalOrTimerMatch::SignalWon {
                payload: serde_json::json!({"approved": true})
            },
            "the race must still see its own signal after a push-handler claim attempt"
        );
    }

    #[test]
    fn claim_pending_signal_reserves_only_the_racing_occurrence_not_other_signals() {
        // A second, unrelated "approval" delivery (recorded well after the
        // race already resolved) is a completely separate signal and must
        // still be freely available to a push handler once the workflow's
        // own code has driven the cursor past the resolved race.
        let timer_id = "__signal_timeout:1:approval";
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: TimerId::new(timer_id),
                duration_secs: 300,
            },
            WorkflowEvent::TimerFired {
                timer_id: TimerId::new(timer_id),
            },
            // Arrives only after the race already resolved (TimerWon) --
            // an ordinary, unreserved signal for a push handler to claim.
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"approved": true, "late": true}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_signal_or_timer("approval", timer_id, Some(300));
        assert_eq!(result, SignalOrTimerMatch::TimerWon);

        let claimed = payloads_only(matcher.claim_pending_signal("approval"));
        assert_eq!(
            claimed,
            vec![serde_json::json!({"approved": true, "late": true})],
            "a signal recorded after the race already resolved is not reserved"
        );
    }

    #[test]
    fn claim_pending_signal_claims_signals_unrelated_to_any_race() {
        // A push handler for a name that has no signal-or-timer race at all
        // must be entirely unaffected by the reservation check.
        let events = vec![WorkflowEvent::SignalReceived {
            signal_name: "cancel".into(),
            payload: serde_json::json!({"reason": "manual"}),
        }];
        let mut matcher = HistoryMatcher::new(events);
        let claimed = payloads_only(matcher.claim_pending_signal("cancel"));
        assert_eq!(claimed, vec![serde_json::json!({"reason": "manual"})]);
    }

    #[test]
    fn claim_pending_signal_only_reserves_the_first_racing_signal_not_a_later_one() {
        // Regression (PR #890 review): the race resolves at the FIRST matching
        // signal (match_signal_or_timer never looks past it), so only that
        // occurrence is reserved. A second same-name delivery recorded before
        // the timer fires is unrelated to the race and becomes claimable once
        // the race has resolved and the cursor has moved past it.
        let timer_id = "__signal_timeout:1:approval";
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: TimerId::new(timer_id),
                duration_secs: 300,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"seq": 1}),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"seq": 2}),
            },
            WorkflowEvent::TimerFired {
                timer_id: TimerId::new(timer_id),
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_signal_or_timer("approval", timer_id, Some(300));
        assert_eq!(
            result,
            SignalOrTimerMatch::SignalWon {
                payload: serde_json::json!({"seq": 1})
            },
            "the race must resolve on its own (first) signal"
        );

        let claimed = payloads_only(matcher.claim_pending_signal("approval"));
        assert_eq!(
            claimed,
            vec![serde_json::json!({"seq": 2})],
            "only the first occurrence was reserved for the race; the second is ordinary"
        );
    }

    #[test]
    fn claim_pending_signal_reserves_one_occurrence_per_concurrent_race() {
        // Regression (PR #890 review follow-up): two CONCURRENT
        // signal-or-deadline races for the same name each need their OWN
        // signal occurrence, in start order (match_signal_or_timer's scan for
        // the race started first runs first during replay and so consumes
        // the first available occurrence, leaving the second for the race
        // started second). Closing every open race for the name on the
        // first resolving signal (rather than just the oldest) let a push
        // handler steal the second race's own signal.
        let timer_a = "__signal_timeout:1:approval";
        let timer_b = "__signal_timeout:2:approval";
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: TimerId::new(timer_a),
                duration_secs: 300,
            },
            WorkflowEvent::TimerStarted {
                timer_id: TimerId::new(timer_b),
                duration_secs: 300,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"seq": 1}),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"seq": 2}),
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        let claimed = matcher.claim_pending_signal("approval");
        assert!(
            claimed.is_empty(),
            "both signals are reserved -- one per concurrent race, neither available to a push handler: {claimed:?}"
        );
    }

    // ── try_claim_pending_signal (issue #775: non-blocking signal drain) ────

    #[test]
    fn try_claim_pending_signal_returns_oldest_only_leaving_rest() {
        // The single-claim sibling of `claim_pending_signal`: it must return
        // exactly the OLDEST buffered matching signal (FIFO recorded-history
        // order) and leave every later occurrence claimable by a following
        // call. This is the matcher-level engine for `try_receive_signal`.
        let events = vec![
            WorkflowEvent::SignalReceived {
                signal_name: "event".into(),
                payload: serde_json::json!({"seq": 1}),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "event".into(),
                payload: serde_json::json!({"seq": 2}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let first = matcher.try_claim_pending_signal("event");
        assert_eq!(first, Some(serde_json::json!({"seq": 1})));

        // The remaining occurrence is still available to the next call — a
        // single try-claim never drains more than one.
        let rest = payloads_only(matcher.claim_pending_signal("event"));
        assert_eq!(rest, vec![serde_json::json!({"seq": 2})]);

        // And a third call sees nothing.
        assert_eq!(matcher.try_claim_pending_signal("event"), None);
    }

    #[test]
    fn try_claim_pending_signal_returns_none_when_none() {
        let events = vec![WorkflowEvent::SignalReceived {
            signal_name: "other".into(),
            payload: serde_json::json!({"ok": true}),
        }];
        let mut matcher = HistoryMatcher::new(events);
        assert_eq!(matcher.try_claim_pending_signal("event"), None);
    }

    #[test]
    fn try_claim_pending_signal_skips_an_open_signal_timeout_race() {
        // Symmetric to `claim_pending_signal_does_not_steal_from_an_open_signal_timeout_race`:
        // the non-blocking try-claim must never resolve a signal reserved for
        // an in-flight signal-or-deadline race (issue #476). The race's own
        // unconsumed `TimerStarted` also blocks the cursor from reaching the
        // signal, so this asserts the belt-and-suspenders reservation guard.
        let timer_id = "__signal_timeout:1:approval";
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: TimerId::new(timer_id),
                duration_secs: 300,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"approved": true}),
            },
            WorkflowEvent::TimerFired {
                timer_id: TimerId::new(timer_id),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        assert_eq!(
            matcher.try_claim_pending_signal("approval"),
            None,
            "the signal is reserved for the open race and must not be try-claimed"
        );

        // The race must still resolve exactly as it would with no drain attempt.
        let result = matcher.match_signal_or_timer("approval", timer_id, Some(300));
        assert_eq!(
            result,
            SignalOrTimerMatch::SignalWon {
                payload: serde_json::json!({"approved": true})
            }
        );
    }

    #[test]
    fn try_claim_pending_signal_does_not_advance_past_an_unconsumed_activity() {
        // Mirrors `claim_pending_signal_does_not_advance_past_an_unconsumed_activity`:
        // a signal recorded AFTER an activity the workflow has not reached yet
        // in this replay cycle is invisible to a non-blocking try-claim.
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "send_email".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!("sent"),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "event".into(),
                payload: serde_json::json!({"seq": 1}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        assert_eq!(
            matcher.try_claim_pending_signal("event"),
            None,
            "must not claim a signal recorded after an unconsumed activity"
        );

        // Once the workflow's own code matches the activity and the cursor
        // advances past it, the trailing signal becomes claimable.
        let activity_result = matcher.match_activity("send_email");
        assert!(matches!(activity_result, HistoryMatch::Matched { .. }));

        assert_eq!(
            matcher.try_claim_pending_signal("event"),
            Some(serde_json::json!({"seq": 1}))
        );
    }

    #[test]
    fn try_claim_then_claim_all_no_double_delivery() {
        // A try-claim (single) followed by a claim-all (rest) must never
        // re-deliver the already-claimed occurrence.
        let events = vec![
            WorkflowEvent::SignalReceived {
                signal_name: "event".into(),
                payload: serde_json::json!({"seq": 1}),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "event".into(),
                payload: serde_json::json!({"seq": 2}),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "event".into(),
                payload: serde_json::json!({"seq": 3}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let first = matcher.try_claim_pending_signal("event");
        assert_eq!(first, Some(serde_json::json!({"seq": 1})));

        let rest = payloads_only(matcher.claim_pending_signal("event"));
        assert_eq!(
            rest,
            vec![serde_json::json!({"seq": 2}), serde_json::json!({"seq": 3})],
            "the try-claimed occurrence must not reappear in the drain-all"
        );
    }

    #[test]
    fn try_claim_pending_signal_drains_a_signal_that_lost_a_resolved_timer_race() {
        // The AC7 "already lost a resolved TimerWon race is fair game" claim,
        // made concrete for the non-blocking drain. A signal recorded AFTER its
        // race timer FIRED (TimerStarted, TimerFired, then SignalReceived) never
        // reserved the occurrence — the timer resolved the race before the
        // signal arrived — so it is drainable once the workflow's own code has
        // driven the cursor past the resolved race.
        let timer_id = "__signal_timeout:0:approval";
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: TimerId::new(timer_id),
                duration_secs: 300,
            },
            WorkflowEvent::TimerFired {
                timer_id: TimerId::new(timer_id),
            },
            // The late loser: recorded after the race already resolved TimerWon.
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"approved": true, "late": true}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        // The reservation index build never reserved this occurrence — the
        // resolved timer isn't in `open_race_timers` when the signal is seen.
        assert!(
            matcher.race_reserved_signal_events.is_empty(),
            "a signal recorded after its race timer fired must not be reserved"
        );

        // Resolve the race exactly as the workflow's own code would (TimerWon).
        let result = matcher.match_signal_or_timer("approval", timer_id, Some(300));
        assert_eq!(result, SignalOrTimerMatch::TimerWon);

        // The late loser is now drainable via the non-blocking try-claim.
        assert_eq!(
            matcher.try_claim_pending_signal("approval"),
            Some(serde_json::json!({"approved": true, "late": true})),
            "a signal that lost a resolved TimerWon race is fair game for a drain"
        );
    }

    #[test]
    fn matcher_allows_non_signal_command_after_out_of_order_signal_match() {
        let timer_id = TimerId::new("cooldown");
        let events = vec![
            WorkflowEvent::SignalReceived {
                signal_name: "cancel".into(),
                payload: serde_json::json!({"reason": "manual"}),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approved".into(),
                payload: serde_json::json!({"ok": true}),
            },
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 5,
            },
            WorkflowEvent::TimerFired { timer_id },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let approved = matcher.match_signal("approved");
        assert!(matches!(approved, HistoryMatch::Matched { .. }));

        let timer = matcher.match_timer("cooldown");
        assert_eq!(
            timer,
            HistoryMatch::Matched {
                output: Value::Null
            }
        );
    }

    #[test]
    fn matcher_activity_skips_signal_ingested_before_activity_scheduled() {
        // A signal arrives before the workflow runs its first activity.
        // ingest_pending_signals would place SignalReceived at position 0
        // (the first position after WorkflowStarted is skipped).
        // match_activity must buffer the early signal and still find
        // ActivityScheduled + ActivityCompleted.
        let activity_id = ActivityExecId::new();
        let output = serde_json::json!({"email_id": "msg-001"});
        let events = vec![
            WorkflowEvent::SignalReceived {
                signal_name: "approved".into(),
                payload: serde_json::json!({"ok": true}),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "send_email".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: output.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let activity = matcher.match_activity("send_email");
        assert_eq!(
            activity,
            HistoryMatch::Matched { output },
            "match_activity should skip the early signal and replay the activity"
        );

        // The buffered signal must be deliverable via a subsequent match_signal call.
        let signal = matcher.match_signal("approved");
        assert_eq!(
            signal,
            HistoryMatch::Matched {
                output: serde_json::json!({"ok": true})
            },
            "signal buffered during match_activity should be returned by match_signal"
        );
    }

    #[test]
    fn matcher_timer_skips_signal_ingested_before_timer_started() {
        // Same scenario as above but for a timer: signal arrives before the
        // timer is started, so it sits before TimerStarted in history.
        let timer_id = TimerId::new("cooldown");
        let events = vec![
            WorkflowEvent::SignalReceived {
                signal_name: "approved".into(),
                payload: serde_json::json!({"ok": true}),
            },
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 30,
            },
            WorkflowEvent::TimerFired { timer_id },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let timer = matcher.match_timer("cooldown");
        assert_eq!(
            timer,
            HistoryMatch::Matched {
                output: Value::Null
            },
            "match_timer should skip the early signal and replay the timer"
        );

        let signal = matcher.match_signal("approved");
        assert_eq!(
            signal,
            HistoryMatch::Matched {
                output: serde_json::json!({"ok": true})
            },
            "signal buffered during match_timer should be returned by match_signal"
        );
    }

    #[test]
    fn matcher_timer_skips_signal_interleaved_between_started_and_fired() {
        // A signal is recorded while the timer is pending (between TimerStarted
        // and TimerFired in history). match_timer must skip it and still find
        // TimerFired.
        let timer_id = TimerId::new("cooldown");
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 30,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approved".into(),
                payload: serde_json::json!({"ok": true}),
            },
            WorkflowEvent::TimerFired { timer_id },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let timer = matcher.match_timer("cooldown");
        assert_eq!(
            timer,
            HistoryMatch::Matched {
                output: Value::Null
            },
            "match_timer should skip signals between TimerStarted and TimerFired"
        );

        let signal = matcher.match_signal("approved");
        assert_eq!(
            signal,
            HistoryMatch::Matched {
                output: serde_json::json!({"ok": true})
            },
            "signal buffered during timer scan should be returned by match_signal"
        );
    }

    #[test]
    fn matcher_timer_reaches_fire_past_interleaved_reset() {
        // Codex P2 (issue #768): a sibling branch `reset()` of another timer
        // records `[TimerCancelled(idle), TimerStarted(idle)]` between this
        // `ctx.timer("sleep")`'s own TimerStarted and its TimerFired. The fire
        // scan must step over BOTH the cancel AND the paired re-arm and still
        // reach the fire — skipping only the cancel would STOP on the re-arm and
        // return NoMatch, failing strict replay and re-parking a fired timer.
        let events = vec![ts("sleep", 30), tc("idle"), ts("idle", 300), tf("sleep")];
        let mut m = HistoryMatcher::new(events);
        assert_eq!(
            m.match_timer("sleep"),
            HistoryMatch::Matched {
                output: Value::Null
            },
            "the primary timer must reach its fire past an interleaved reset"
        );
        // The interleaved reset stays claimable afterward, in order (the reset's
        // cancel then its re-arm) — both were skipped without being consumed.
        assert!(
            matches!(m.match_timer_cancel("idle"), HistoryMatch::Matched { .. }),
            "reset's TimerCancelled must still be claimable"
        );
        assert!(
            matches!(m.match_timer_arm("idle", 300), HistoryMatch::Matched { .. }),
            "reset's re-arm TimerStarted must still be claimable"
        );
    }

    #[test]
    fn matcher_signal_scan_skips_interleaved_reset() {
        // Codex P2 (issue #768): a `wait_for_signal` polled before a same-cycle
        // `reset()` branch sees `[TimerCancelled(idle), TimerStarted(idle),
        // SignalReceived]`. The signal scan must step over BOTH timer events and
        // still match the signal — skipping only the cancel would STOP on the
        // re-arm and wrongly report a missing signal.
        let events = vec![
            tc("idle"),
            ts("idle", 300),
            WorkflowEvent::SignalReceived {
                signal_name: "approved".into(),
                payload: serde_json::json!({"ok": true}),
            },
        ];
        let mut m = HistoryMatcher::new(events);
        assert_eq!(
            m.match_signal("approved"),
            HistoryMatch::Matched {
                output: serde_json::json!({"ok": true})
            },
            "signal scan must step over an interleaved reset's [TimerCancelled, TimerStarted]"
        );
        // The interleaved reset stays claimable afterward (non-consuming skip).
        assert!(matches!(
            m.match_timer_cancel("idle"),
            HistoryMatch::Matched { .. }
        ));
        assert!(matches!(
            m.match_timer_arm("idle", 300),
            HistoryMatch::Matched { .. }
        ));
    }

    #[test]
    fn matcher_signal_scan_leaves_a_stray_timer_unconsumed_for_the_cycle_guard() {
        // Round 13 (issue #768) requires that a `wait_for_signal` over a history
        // carrying a STRAY, unconsumed `TimerStarted` and no matching signal
        // nd-blocks rather than parking forever on a signal that will never
        // arrive (#603).
        //
        // Issue #950 moved WHERE that is decided. The matcher used to diverge
        // here, but at scan time a stray timer is indistinguishable from the
        // timer of an advertised `join!(wait_for_signal, ctx.timer)` mixed batch,
        // whose sibling branch has not been polled yet — so diverging here
        // nd-blocked a supported composition on its first wake (Codex round 3 on
        // PR #1245). The scan now returns `NoMatch` and, crucially, leaves the
        // event UNCONSUMED; the decision is made at the end of the cycle, where
        // `executor.rs`'s `history_has_unconsumed_events()` sees that nothing
        // claimed it and fails the workflow.
        //
        // So this asserts the two halves the cycle guard needs, not a verdict:
        // park, and the stray event still unclaimed. The round-13 OUTCOME is
        // pinned end-to-end by the integration test
        // `interleaved_sibling_signal_stray_timer_started_still_diverges`, which
        // asserts the replay is reported as non-determinism.
        let events = vec![ts("timer-1", 10)];
        let mut m = HistoryMatcher::new(events);
        assert_eq!(
            m.match_signal("my-signal"),
            HistoryMatch::NoMatch,
            "the scan must park rather than guess; the stray timer is caught at \
             the end of the cycle"
        );
        assert!(
            !m.is_consumed(0),
            "the stray timer must be left UNCONSUMED — that is the whole signal \
             `history_has_unconsumed_events()` keys on to nd-block the run"
        );
    }

    #[test]
    fn matcher_signal_scan_parks_on_lone_open_deadline_timer() {
        // Determinism-review finding 1.2 (issue #1071): a LONE reserved
        // signal-or-deadline race timer (`__signal_timeout:{seq}:{name}`, issue
        // #476) with NO matching `TimerFired` and NO signal represents a
        // legitimate concurrent OPEN `receive_signal_timeout` race whose signal
        // has not arrived yet. A plain `match_signal` over that history must
        // return `NoMatch` (park until the signal arrives), NOT `Diverged` —
        // the reserved-timer arm crosses it WITHOUT setting
        // `first_interleaved_command`, so the round-13 stray-`TimerStarted`
        // park-forever guard does not falsely nd-block the open race.
        let events = vec![ts("__signal_timeout:0:go", 300)];
        let mut m = HistoryMatcher::new(events);
        assert_eq!(
            m.match_signal("go"),
            HistoryMatch::NoMatch,
            "a lone open signal-or-deadline race deadline timer must park (NoMatch), not diverge"
        );
    }

    #[test]
    fn matcher_signal_scan_sequential_reset_then_signal_matches() {
        // Legit case (round-7 reason preserved): a sequential
        // `reset_timer(); receive_signal().await` records
        // `[TimerCancelled, TimerStarted, SignalReceived]`. The reset's own
        // matchers run FIRST and consume the two timer events, so by the time
        // the signal scan runs they are already `is_consumed` (skipped at the
        // top of the loop). The signal must still be found → Matched.
        let events = vec![
            tc("idle"),
            ts("idle", 300),
            WorkflowEvent::SignalReceived {
                signal_name: "approved".into(),
                payload: serde_json::json!({"ok": true}),
            },
        ];
        let mut m = HistoryMatcher::new(events);
        // The reset's matchers claim the timer events first (sequential order).
        assert!(matches!(
            m.match_timer_cancel("idle"),
            HistoryMatch::Matched { .. }
        ));
        assert!(matches!(
            m.match_timer_arm("idle", 300),
            HistoryMatch::Matched { .. }
        ));
        assert_eq!(
            m.match_signal("approved"),
            HistoryMatch::Matched {
                output: serde_json::json!({"ok": true})
            },
            "sequential reset then wait_for_signal with a later signal must match"
        );
    }

    #[test]
    fn matcher_signal_scan_sequential_reset_no_signal_suspends() {
        // Legit suspend case: a sequential `reset_timer(); receive_signal()`
        // where the signal has NOT yet arrived records only
        // `[TimerCancelled, TimerStarted]`. The reset's matchers consume both
        // timer events first, so the signal scan crosses NOTHING unconsumed and
        // must return `NoMatch` (a genuine suspend) — NOT a false `Diverged`.
        let events = vec![tc("idle"), ts("idle", 300)];
        let mut m = HistoryMatcher::new(events);
        assert!(matches!(
            m.match_timer_cancel("idle"),
            HistoryMatch::Matched { .. }
        ));
        assert!(matches!(
            m.match_timer_arm("idle", 300),
            HistoryMatch::Matched { .. }
        ));
        assert_eq!(
            m.match_signal("approved"),
            HistoryMatch::NoMatch,
            "a consumed reset's timers with no signal yet must suspend, not diverge"
        );
    }

    // ── Pause/Resume replay transparency (issue #383) ─────────────────────

    #[test]
    fn matcher_timer_skips_pause_resume_between_started_and_fired() {
        // The operator paused while the workflow was waiting on a timer, then
        // resumed; the timer fired on resume. The pause/resume pair is recorded
        // between TimerStarted and TimerFired. match_timer must treat them as
        // no-ops and still find TimerFired — replay determinism is unchanged.
        let timer_id = TimerId::new("cooldown");
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 30,
            },
            WorkflowEvent::WorkflowExecutionPaused {
                paused_at: chrono::Utc::now(),
                reason: Some("incident".into()),
                actor: "oncall".into(),
            },
            WorkflowEvent::WorkflowExecutionResumed {
                resumed_at: chrono::Utc::now(),
                actor: "oncall".into(),
            },
            WorkflowEvent::TimerFired { timer_id },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let timer = matcher.match_timer("cooldown");
        assert_eq!(
            timer,
            HistoryMatch::Matched {
                output: Value::Null
            },
            "match_timer must skip pause/resume events and still find TimerFired"
        );
    }

    #[test]
    fn matcher_pause_resume_are_not_unconsumed_history() {
        // A trailing pause/resume pair (e.g. paused-and-resumed with no pending
        // work) must not be reported as unconsumed history, otherwise the
        // executor would flag spurious non-determinism / never-settle.
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: chrono::Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::WorkflowExecutionPaused {
                paused_at: chrono::Utc::now(),
                reason: None,
                actor: "oncall".into(),
            },
            WorkflowEvent::WorkflowExecutionResumed {
                resumed_at: chrono::Utc::now(),
                actor: "oncall".into(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        matcher.advance(); // skip WorkflowStarted
        assert!(
            !matcher.has_non_lifecycle_unconsumed(),
            "pause/resume events must be transparent to unconsumed-history checks"
        );
    }

    #[test]
    fn matcher_activity_skips_pause_resume_between_scheduled_and_completed() {
        // Pause/resume recorded while an activity was in flight.
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "charge".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::WorkflowExecutionPaused {
                paused_at: chrono::Utc::now(),
                reason: None,
                actor: "oncall".into(),
            },
            WorkflowEvent::WorkflowExecutionResumed {
                resumed_at: chrono::Utc::now(),
                actor: "oncall".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!({"charged": true}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_activity("charge");
        assert_eq!(
            result,
            HistoryMatch::Matched {
                output: serde_json::json!({"charged": true})
            },
            "match_activity must skip pause/resume and find the recorded completion"
        );
    }

    // ── DLQ redrive transparency tests (issue #510) ───────────────────────

    #[test]
    fn redrive_marks_superseded_failed_and_redrive_transparent() {
        // History: started → activity scheduled+failed → WorkflowFailed (sealed
        // at quarantine) → WorkflowRedriven (operator reactivation). Both the
        // superseded WorkflowFailed (index 3) and the WorkflowRedriven (index 4)
        // must be transparent so command dispatch advances past them.
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: chrono::Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "charge".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityFailed {
                activity_id,
                error: "downstream down".into(),
                attempt: 3,
                error_type: "Error".into(),
                details: None,
                non_retryable: false,
            },
            WorkflowEvent::workflow_failed("downstream down"),
            WorkflowEvent::WorkflowRedriven {
                redriven_at: chrono::Utc::now(),
                dead_letter_id: uuid::Uuid::new_v4(),
                reason: Some("downstream fixed".into()),
            },
        ];
        let matcher = HistoryMatcher::new(events);
        assert!(
            matcher.is_consumed(3),
            "the WorkflowFailed superseded by a redrive must be transparent"
        );
        assert!(
            matcher.is_consumed(4),
            "the WorkflowRedriven event must be transparent"
        );
        // The unrelated ActivityFailed must NOT be made transparent.
        assert!(
            !matcher.is_consumed(2),
            "only the immediately-preceding WorkflowFailed is superseded"
        );
    }

    #[test]
    fn redrive_pair_passes_unconsumed_check() {
        // WorkflowRedriven is NOT a terminal-lifecycle event, so without the
        // transparency marking it would be flagged as unconsumed non-lifecycle
        // history. The redrive-anchored transparency excuses it.
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: chrono::Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::workflow_failed("boom"),
            WorkflowEvent::WorkflowRedriven {
                redriven_at: chrono::Utc::now(),
                dead_letter_id: uuid::Uuid::new_v4(),
                reason: None,
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        matcher.advance(); // past WorkflowStarted
        assert!(
            !matcher.has_non_lifecycle_unconsumed(),
            "a redriven tail must be transparent to the unconsumed-history check"
        );
    }

    // ── Terminal-failure tail transparency tests (issue #952) ─────────────

    /// Superseded by issue #952: a bare trailing `WorkflowFailed` used to stay
    /// opaque, which made every in-progress `match_*` on a failed run diverge
    /// (see the two known-limitation tests in `tests/integration/replayer_tests.rs`).
    /// It is now transparent — a failing cycle's history is truncated by
    /// construction, so there is nothing past the failure point to verify.
    #[test]
    fn bare_trailing_failed_is_transparent() {
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: chrono::Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::workflow_failed("boom"),
        ];
        let matcher = HistoryMatcher::new(events);
        assert!(
            matcher.is_consumed(1),
            "a bare trailing WorkflowFailed must be transparent to in-progress matches"
        );
        assert!(matcher.has_terminal_failure_tail());
    }

    #[test]
    fn in_progress_match_past_a_trailing_failed_is_no_match_not_diverged() {
        // The whole point of the tail: a workflow that issues a match attempt
        // and only then fails (a payload-cap / missing-handler / missing-store
        // check) must observe "the run is failing" (NoMatch), not a divergence.
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: chrono::Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::workflow_failed("payload too large"),
        ];
        let mut matcher = HistoryMatcher::new(events);
        matcher.advance(); // past WorkflowStarted
        assert_eq!(
            matcher.match_child_workflow("some_child", &Value::Null),
            HistoryMatch::NoMatch,
            "a match attempt at the failing frontier must not diverge"
        );
        assert!(
            matcher.at_terminal_failure_frontier(),
            "all pre-terminal events consumed → the cursor is at the failing frontier"
        );
    }

    #[test]
    fn terminal_failure_tail_spans_post_terminal_bookkeeping() {
        // A workflow-level retry (#523) appends WorkflowRetryScheduled AFTER the
        // sealed run's WorkflowFailed. The tail must still be recognised, and
        // every event in it transparent, or the retried run's history would keep
        // false-flagging.
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: chrono::Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::workflow_failed("boom"),
            WorkflowEvent::WorkflowRetryScheduled {
                retry_exec_id: ExecutionId::new(),
                attempt: 2,
                fire_at: chrono::Utc::now(),
            },
        ];
        let matcher = HistoryMatcher::new(events);
        assert!(
            matcher.is_consumed(1),
            "the WorkflowFailed must be transparent"
        );
        assert!(
            matcher.is_consumed(2),
            "post-terminal bookkeeping in the tail must be transparent too"
        );
    }

    #[test]
    fn a_failed_event_followed_by_real_history_is_not_a_tail() {
        // Guard against over-reach: only the LAST terminal block is transparent.
        // A WorkflowFailed with genuine command history after it (the #510
        // redrive shape, here without the redrive marker) must keep today's
        // behaviour — the redrive-anchored rule, not the tail rule, decides.
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: chrono::Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::workflow_failed("boom"),
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "charge".into(),
                input: Value::Null,
                queue: "default".into(),
            },
        ];
        let matcher = HistoryMatcher::new(events);
        assert!(
            !matcher.is_consumed(1),
            "a WorkflowFailed with real history after it is not a terminal tail"
        );
        assert!(!matcher.has_terminal_failure_tail());
    }

    #[test]
    fn completed_and_cancelled_tails_are_not_terminal_failure_tails() {
        // The relaxation is failure-only: a COMPLETED history is verified in
        // full, so its terminal event must stay opaque (an extra command at that
        // cursor is a genuine early-completion divergence).
        for terminal in [
            WorkflowEvent::WorkflowCompleted {
                output: Value::Null,
            },
            WorkflowEvent::WorkflowCancelled {
                reason: "operator".into(),
            },
        ] {
            let events = vec![
                WorkflowEvent::WorkflowStarted {
                    input: Value::Null,
                    timestamp: chrono::Utc::now(),
                    last_completion_result: None,
                    last_error: None,
                    scheduled_time: None,
                },
                terminal,
            ];
            let matcher = HistoryMatcher::new(events);
            assert!(
                !matcher.has_terminal_failure_tail(),
                "only WorkflowFailed opens a terminal-failure tail"
            );
            assert!(!matcher.is_consumed(1));
        }
    }

    #[test]
    fn redrive_tail_is_not_a_terminal_failure_tail() {
        // A redriven run is REOPENED, not failing: the #510 rule already makes
        // its superseded terminal transparent, and the failing-frontier
        // relaxations (which suppress strict-replay divergence reporting) must
        // NOT apply to it.
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: chrono::Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::workflow_failed("boom"),
            WorkflowEvent::WorkflowRedriven {
                redriven_at: chrono::Utc::now(),
                dead_letter_id: uuid::Uuid::new_v4(),
                reason: None,
            },
        ];
        let matcher = HistoryMatcher::new(events);
        assert!(
            !matcher.has_terminal_failure_tail(),
            "a redrive reopens the run; it is not a failing tail"
        );
        assert!(
            matcher.is_consumed(1) && matcher.is_consumed(2),
            "the #510 redrive-anchored transparency is unchanged"
        );
    }

    /// Issue #952 x #510: a redrive REOPENS the run, so the abandoned-dispatch
    /// records its failing cycle wrote must become transparent — otherwise the
    /// reopened cycle reads back "this dispatch failed" and can never re-issue
    /// the work the operator redrove it to complete.
    #[test]
    fn a_redrive_makes_abandoned_dispatch_records_transparent() {
        let child_id = ExecutionId::new();
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: chrono::Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "worker_child".into(),
                input: Value::Null,
            },
            WorkflowEvent::ChildWorkflowFailed {
                child_id,
                error: crate::event::ABANDONED_DISPATCH_REASON.to_string(),
                error_type: None,
                details: None,
                non_retryable: Some(true),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "charge".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityFailed {
                activity_id,
                error: crate::event::ABANDONED_DISPATCH_REASON.to_string(),
                attempt: 1,
                error_type: "Error".into(),
                details: None,
                non_retryable: true,
            },
            WorkflowEvent::workflow_failed("budget exceeded"),
            WorkflowEvent::WorkflowRedriven {
                redriven_at: chrono::Utc::now(),
                dead_letter_id: uuid::Uuid::new_v4(),
                reason: None,
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        for idx in 1..=4 {
            assert!(
                matcher.is_consumed(idx),
                "event {idx} of the abandoned pair must be transparent after a redrive"
            );
        }
        matcher.advance(); // past WorkflowStarted
        assert_eq!(
            matcher.match_child_workflow("worker_child", &Value::Null),
            HistoryMatch::NoMatch,
            "the reopened run must re-dispatch the child live"
        );
    }

    /// Issue #1262: a failing cycle can record more than an abandoned
    /// dispatch pair before it fails. Here it also records a
    /// `MarkerRecorded` event after the dispatch. The marker is not an
    /// abandoned-dispatch shape, so it needs its own transparency rule.
    /// Without one, the cursor lands on the marker and reports `Diverged`
    /// instead of allowing a live re-dispatch.
    #[test]
    fn a_marker_recorded_after_an_abandoned_dispatch_still_re_dispatches_live() {
        let child_id = ExecutionId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: chrono::Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "worker_child".into(),
                input: Value::Null,
            },
            WorkflowEvent::ChildWorkflowFailed {
                child_id,
                error: crate::event::ABANDONED_DISPATCH_REASON.to_string(),
                error_type: None,
                details: None,
                non_retryable: Some(true),
            },
            WorkflowEvent::MarkerRecorded {
                name: "m".into(),
                details: Value::Null,
            },
            WorkflowEvent::workflow_failed("budget exceeded"),
            WorkflowEvent::WorkflowRedriven {
                redriven_at: chrono::Utc::now(),
                dead_letter_id: uuid::Uuid::new_v4(),
                reason: None,
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        assert!(
            matcher.is_consumed(3),
            "the marker the failing cycle wrote after the dispatch must be transparent too"
        );
        matcher.advance(); // past WorkflowStarted
        assert_eq!(
            matcher.match_child_workflow("worker_child", &Value::Null),
            HistoryMatch::NoMatch,
            "a durable record trailing the abandoned dispatch must not block re-dispatch"
        );
    }

    /// Issue #1262: the same defect occurs with no abandoned-dispatch pair
    /// at all. A failing cycle can record a plain marker and then fail,
    /// with no dispatch in between. The marker alone must not block the
    /// redrive.
    #[test]
    fn a_bare_marker_before_a_redriven_terminal_still_re_dispatches_live() {
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: chrono::Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::MarkerRecorded {
                name: "m".into(),
                details: Value::Null,
            },
            WorkflowEvent::workflow_failed("budget exceeded"),
            WorkflowEvent::WorkflowRedriven {
                redriven_at: chrono::Utc::now(),
                dead_letter_id: uuid::Uuid::new_v4(),
                reason: None,
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        assert!(
            matcher.is_consumed(1),
            "a bare marker recorded by the failing cycle must be transparent on redrive"
        );
        matcher.advance(); // past WorkflowStarted
        assert_eq!(
            matcher.match_child_workflow("worker_child", &Value::Null),
            HistoryMatch::NoMatch,
            "the reopened run must re-dispatch live instead of diverging on the marker"
        );
    }

    /// Negative control (issue #1262): a marker from an earlier,
    /// non-superseded cycle must stay opaque across the redrive. That
    /// cycle's own dispatch already completed before the failing cycle
    /// started. If the walk swallows this marker instead, it silently
    /// breaks `ctx.version()` and `ctx.patched()` positional matching for
    /// a cycle the redrive never touched.
    #[test]
    fn a_marker_from_an_earlier_completed_cycle_stays_opaque_across_a_redrive() {
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: chrono::Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::MarkerRecorded {
                name: "version:early".into(),
                details: Value::Null,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "charge".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            // This completion ends the earlier cycle: everything before it,
            // including the marker above, is settled, non-superseded history.
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: Value::Null,
            },
            WorkflowEvent::MarkerRecorded {
                name: "m".into(),
                details: Value::Null,
            },
            WorkflowEvent::workflow_failed("budget exceeded"),
            WorkflowEvent::WorkflowRedriven {
                redriven_at: chrono::Utc::now(),
                dead_letter_id: uuid::Uuid::new_v4(),
                reason: None,
            },
        ];
        let matcher = HistoryMatcher::new(events);
        assert!(
            !matcher.is_consumed(1),
            "the earlier cycle's own marker must stay positionally matchable"
        );
        assert!(
            matcher.is_consumed(4),
            "the failing cycle's own marker, past the last completion, must be transparent"
        );
    }

    /// Issue #1262, stronger negative control: an earlier cycle's version
    /// marker must not just stay unconsumed. It must still answer
    /// `match_version` with the recorded value after a redrive. A silent
    /// value flip would be worse than a divergence: `match_version` never
    /// reports `Diverged`, so a bug here has no loud symptom.
    #[test]
    fn a_redriven_run_still_reads_an_earlier_cycles_version_marker() {
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: chrono::Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::MarkerRecorded {
                name: "version:early".into(),
                details: serde_json::json!(1),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "charge".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            // This completion ends the earlier cycle: the version marker
            // above is settled, non-superseded history.
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: Value::Null,
            },
            WorkflowEvent::MarkerRecorded {
                name: "m".into(),
                details: Value::Null,
            },
            WorkflowEvent::workflow_failed("budget exceeded"),
            WorkflowEvent::WorkflowRedriven {
                redriven_at: chrono::Utc::now(),
                dead_letter_id: uuid::Uuid::new_v4(),
                reason: None,
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        matcher.advance(); // past WorkflowStarted, cursor at the version marker
        assert_eq!(
            matcher.match_version("early", 1, 1),
            1,
            "the earlier cycle's own version marker must still be read positionally"
        );
    }

    /// Issue #1262: `SideEffectRecorded` needs the same transparency as
    /// `MarkerRecorded`. Both are durable, completion-free records. A
    /// failing cycle can write either after the dispatch a redrive
    /// re-issues.
    #[test]
    fn a_side_effect_recorded_after_an_abandoned_dispatch_still_re_dispatches_live() {
        let child_id = ExecutionId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: chrono::Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "worker_child".into(),
                input: Value::Null,
            },
            WorkflowEvent::ChildWorkflowFailed {
                child_id,
                error: crate::event::ABANDONED_DISPATCH_REASON.to_string(),
                error_type: None,
                details: None,
                non_retryable: Some(true),
            },
            WorkflowEvent::SideEffectRecorded {
                kind: SideEffectKind::Uuid,
                name: None,
                value: serde_json::json!("11111111-1111-1111-1111-111111111111"),
            },
            WorkflowEvent::workflow_failed("budget exceeded"),
            WorkflowEvent::WorkflowRedriven {
                redriven_at: chrono::Utc::now(),
                dead_letter_id: uuid::Uuid::new_v4(),
                reason: None,
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        assert!(
            matcher.is_consumed(3),
            "a side effect the failing cycle wrote after the dispatch must be transparent too"
        );
        matcher.advance(); // past WorkflowStarted
        assert_eq!(
            matcher.match_child_workflow("worker_child", &Value::Null),
            HistoryMatch::NoMatch,
            "a trailing side effect must not block re-dispatch"
        );
    }

    /// Issue #1262: the failing cycle can write more than one trailing
    /// record. The walk must swallow the whole run, not just the last one.
    #[test]
    fn a_run_of_several_markers_after_an_abandoned_dispatch_is_fully_swallowed() {
        let child_id = ExecutionId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: chrono::Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "worker_child".into(),
                input: Value::Null,
            },
            WorkflowEvent::ChildWorkflowFailed {
                child_id,
                error: crate::event::ABANDONED_DISPATCH_REASON.to_string(),
                error_type: None,
                details: None,
                non_retryable: Some(true),
            },
            WorkflowEvent::MarkerRecorded {
                name: "m1".into(),
                details: Value::Null,
            },
            WorkflowEvent::MarkerRecorded {
                name: "m2".into(),
                details: Value::Null,
            },
            WorkflowEvent::MarkerRecorded {
                name: "m3".into(),
                details: Value::Null,
            },
            WorkflowEvent::workflow_failed("budget exceeded"),
            WorkflowEvent::WorkflowRedriven {
                redriven_at: chrono::Utc::now(),
                dead_letter_id: uuid::Uuid::new_v4(),
                reason: None,
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        for idx in [3_usize, 4, 5] {
            assert!(
                matcher.is_consumed(idx),
                "marker {idx} in the trailing run must be transparent"
            );
        }
        matcher.advance(); // past WorkflowStarted
        assert_eq!(
            matcher.match_child_workflow("worker_child", &Value::Null),
            HistoryMatch::NoMatch,
            "a run of trailing markers must not block re-dispatch"
        );
    }

    /// Issue #1262: a marker sandwiched between two abandoned-dispatch
    /// pairs. The walk must skip over the already-transparent second pair
    /// to keep swallowing the marker behind it, not stop at the first
    /// already-transparent index it meets.
    #[test]
    fn a_marker_sandwiched_between_two_abandoned_dispatch_pairs_is_swallowed() {
        let first_child = ExecutionId::new();
        let second_child = ExecutionId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: chrono::Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id: first_child,
                workflow_name: "worker_child".into(),
                input: Value::Null,
            },
            WorkflowEvent::ChildWorkflowFailed {
                child_id: first_child,
                error: crate::event::ABANDONED_DISPATCH_REASON.to_string(),
                error_type: None,
                details: None,
                non_retryable: Some(true),
            },
            WorkflowEvent::MarkerRecorded {
                name: "m".into(),
                details: Value::Null,
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id: second_child,
                workflow_name: "worker_child".into(),
                input: Value::Null,
            },
            WorkflowEvent::ChildWorkflowFailed {
                child_id: second_child,
                error: crate::event::ABANDONED_DISPATCH_REASON.to_string(),
                error_type: None,
                details: None,
                non_retryable: Some(true),
            },
            WorkflowEvent::workflow_failed("budget exceeded"),
            WorkflowEvent::WorkflowRedriven {
                redriven_at: chrono::Utc::now(),
                dead_letter_id: uuid::Uuid::new_v4(),
                reason: None,
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        for idx in 1..=6 {
            assert!(
                matcher.is_consumed(idx),
                "event {idx} must be transparent: two abandoned pairs sandwiching a marker"
            );
        }
        matcher.advance(); // past WorkflowStarted
        assert_eq!(
            matcher.match_child_workflow("worker_child", &Value::Null),
            HistoryMatch::NoMatch,
            "the sandwiched marker must not block re-dispatch of either child"
        );
    }

    /// Issue #1262: an abandoned ACTIVITY dispatch, not a child workflow,
    /// with a trailing marker. The new transparency rule is shape-agnostic,
    /// so it must cover this event kind too.
    #[test]
    fn a_marker_after_an_abandoned_activity_dispatch_still_re_dispatches_live() {
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: chrono::Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "charge".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityFailed {
                activity_id,
                error: crate::event::ABANDONED_DISPATCH_REASON.to_string(),
                attempt: 1,
                error_type: "Error".into(),
                details: None,
                non_retryable: true,
            },
            WorkflowEvent::MarkerRecorded {
                name: "m".into(),
                details: Value::Null,
            },
            WorkflowEvent::workflow_failed("budget exceeded"),
            WorkflowEvent::WorkflowRedriven {
                redriven_at: chrono::Utc::now(),
                dead_letter_id: uuid::Uuid::new_v4(),
                reason: None,
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        assert!(
            matcher.is_consumed(3),
            "the marker trailing an abandoned activity dispatch must be transparent too"
        );
        matcher.advance(); // past WorkflowStarted
        assert_eq!(
            matcher.match_activity("charge"),
            HistoryMatch::NoMatch,
            "a trailing marker must not block re-dispatch of an abandoned activity"
        );
    }

    /// Issue #1262: a SECOND failing cycle, after the first redrive, writes
    /// its own abandoned pair and marker. Those belong to the newest
    /// terminal-failure tail, not the superseded cycle the first redrive
    /// reopened, so they must stay positionally matchable.
    #[test]
    fn a_marker_written_after_the_last_redrive_stays_opaque() {
        let superseded_child = ExecutionId::new();
        let latest_child = ExecutionId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: chrono::Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id: superseded_child,
                workflow_name: "worker_child".into(),
                input: Value::Null,
            },
            WorkflowEvent::ChildWorkflowFailed {
                child_id: superseded_child,
                error: crate::event::ABANDONED_DISPATCH_REASON.to_string(),
                error_type: None,
                details: None,
                non_retryable: Some(true),
            },
            WorkflowEvent::MarkerRecorded {
                name: "m".into(),
                details: Value::Null,
            },
            WorkflowEvent::workflow_failed("budget exceeded"),
            WorkflowEvent::WorkflowRedriven {
                redriven_at: chrono::Utc::now(),
                dead_letter_id: uuid::Uuid::new_v4(),
                reason: None,
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id: latest_child,
                workflow_name: "worker_child".into(),
                input: Value::Null,
            },
            WorkflowEvent::ChildWorkflowFailed {
                child_id: latest_child,
                error: crate::event::ABANDONED_DISPATCH_REASON.to_string(),
                error_type: None,
                details: None,
                non_retryable: Some(true),
            },
            WorkflowEvent::MarkerRecorded {
                name: "m2".into(),
                details: Value::Null,
            },
            WorkflowEvent::workflow_failed("budget exceeded"),
        ];
        let matcher = HistoryMatcher::new(events);
        for idx in [1_usize, 2, 3] {
            assert!(
                matcher.is_consumed(idx),
                "event {idx} preceded the redrive, so it must be transparent"
            );
        }
        for idx in [6_usize, 7, 8] {
            assert!(
                !matcher.is_consumed(idx),
                "event {idx} was written by the cycle that failed AFTER the \
                 redrive, so it must stay positionally matchable"
            );
        }
    }

    /// Issue #1262: `TimerStarted`, `TimerCancelled`, and
    /// `ChildWorkflowSpawnedDetached` are the other event kinds
    /// `worker::terminal_command_policy` classifies `PreTerminalEvent` —
    /// the same class as `MarkerRecorded` / `SideEffectRecorded`. A
    /// failing cycle can write any of them after the dispatch a redrive
    /// re-issues, and the walk must swallow them too.
    #[test]
    fn timer_and_detached_spawn_records_after_an_abandoned_dispatch_are_swallowed() {
        let child_id = ExecutionId::new();
        let detached_child_id = ExecutionId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: chrono::Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "worker_child".into(),
                input: Value::Null,
            },
            WorkflowEvent::ChildWorkflowFailed {
                child_id,
                error: crate::event::ABANDONED_DISPATCH_REASON.to_string(),
                error_type: None,
                details: None,
                non_retryable: Some(true),
            },
            WorkflowEvent::TimerStarted {
                timer_id: TimerId::new("cooldown"),
                duration_secs: 30,
            },
            WorkflowEvent::TimerCancelled {
                timer_id: TimerId::new("cooldown"),
            },
            WorkflowEvent::ChildWorkflowSpawnedDetached {
                child_id: detached_child_id,
                workflow_name: "fire_and_forget".into(),
                input: Value::Null,
                parent_close_policy: crate::types::ParentClosePolicy::Abandon,
            },
            WorkflowEvent::workflow_failed("budget exceeded"),
            WorkflowEvent::WorkflowRedriven {
                redriven_at: chrono::Utc::now(),
                dead_letter_id: uuid::Uuid::new_v4(),
                reason: None,
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        for idx in 1..=6 {
            assert!(
                matcher.is_consumed(idx),
                "event {idx} must be transparent: a timer/detached-spawn tail"
            );
        }
        matcher.advance(); // past WorkflowStarted
        assert_eq!(
            matcher.match_child_workflow("worker_child", &Value::Null),
            HistoryMatch::NoMatch,
            "a trailing timer/detached-spawn run must not block re-dispatch"
        );
    }

    /// A redrive reopens only the cycles it superseded (Codex P1 round 1,
    /// issue #952 x #510). A run that was redriven and then failed AGAIN wrote
    /// fresh abandoned-dispatch records *after* that redrive: those belong to
    /// the terminal-failure tail's own cycle, so they stay positionally
    /// matchable and the replay resolves the branch from its synthetic terminal
    /// instead of parking on a dispatch that will never resolve.
    #[test]
    fn abandoned_records_written_after_the_last_redrive_stay_opaque() {
        let superseded_child = ExecutionId::new();
        let latest_child = ExecutionId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: chrono::Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id: superseded_child,
                workflow_name: "worker_child".into(),
                input: Value::Null,
            },
            WorkflowEvent::ChildWorkflowFailed {
                child_id: superseded_child,
                error: crate::event::ABANDONED_DISPATCH_REASON.to_string(),
                error_type: None,
                details: None,
                non_retryable: Some(true),
            },
            WorkflowEvent::workflow_failed("budget exceeded"),
            WorkflowEvent::WorkflowRedriven {
                redriven_at: chrono::Utc::now(),
                dead_letter_id: uuid::Uuid::new_v4(),
                reason: None,
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id: latest_child,
                workflow_name: "worker_child".into(),
                input: Value::Null,
            },
            WorkflowEvent::ChildWorkflowFailed {
                child_id: latest_child,
                error: crate::event::ABANDONED_DISPATCH_REASON.to_string(),
                error_type: None,
                details: None,
                non_retryable: Some(true),
            },
            WorkflowEvent::workflow_failed("budget exceeded"),
        ];
        let mut matcher = HistoryMatcher::new(events);
        for idx in [1_usize, 2] {
            assert!(
                matcher.is_consumed(idx),
                "event {idx} preceded the redrive, so it must be transparent"
            );
        }
        for idx in [5_usize, 6] {
            assert!(
                !matcher.is_consumed(idx),
                "event {idx} was written by the cycle that failed AFTER the \
                 redrive, so it must stay positionally matchable"
            );
        }
        matcher.advance(); // past WorkflowStarted
        assert!(
            matches!(
                matcher.match_child_workflow("worker_child", &Value::Null),
                HistoryMatch::Failed { ref error, .. } if error == crate::event::ABANDONED_DISPATCH_REASON
            ),
            "the re-dispatched child must resolve from its own synthetic terminal"
        );
    }

    /// An ACTIVITY's error string is the activity author's own message, so the
    /// reserved reason alone can be produced by ordinary user code (Codex P2
    /// round 2). Only the full shape the engine writes counts as synthetic — a
    /// genuine failure that happens to carry the same message keeps its real
    /// terminal, so a redriven run replays it instead of re-dispatching the
    /// activity and repeating its side effects.
    #[test]
    fn a_genuine_activity_failure_quoting_the_reason_is_not_an_abandoned_record() {
        let engine_written = ActivityExecId::new();
        let author_written = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: chrono::Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: author_written,
                name: "charge".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            // Same message, but a real activity terminal: a retried attempt,
            // retryable, and carrying structured details.
            WorkflowEvent::ActivityFailed {
                activity_id: author_written,
                error: crate::event::ABANDONED_DISPATCH_REASON.to_string(),
                attempt: 2,
                error_type: "Error".into(),
                details: Some(serde_json::json!({"code": 42})),
                non_retryable: false,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: engine_written,
                name: "refund".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityFailed {
                activity_id: engine_written,
                error: crate::event::ABANDONED_DISPATCH_REASON.to_string(),
                attempt: 1,
                error_type: "Error".into(),
                details: None,
                non_retryable: true,
            },
            WorkflowEvent::workflow_failed("budget exceeded"),
            WorkflowEvent::WorkflowRedriven {
                redriven_at: chrono::Utc::now(),
                dead_letter_id: uuid::Uuid::new_v4(),
                reason: None,
            },
        ];
        let matcher = HistoryMatcher::new(events);
        for idx in [1_usize, 2] {
            assert!(
                !matcher.is_consumed(idx),
                "event {idx} is a genuine activity failure and must stay matchable"
            );
        }
        for idx in [3_usize, 4] {
            assert!(
                matcher.is_consumed(idx),
                "event {idx} carries the engine's exact abandoned shape, so a redrive \
                 must make it transparent"
            );
        }
    }

    /// A genuine activity failure can quote the reserved reason. It can also
    /// land on attempt 1, non-retryable, with no details — the full shape
    /// the matcher checks (issue #1265). The synthetic path never writes an
    /// `ActivityStarted` between the schedule and the failure: an abandoned
    /// dispatch never actually ran. An intervening `ActivityStarted` is proof
    /// this is a real terminal, and it must stay matchable. Otherwise a
    /// redrive marks it transparent, the real `ActivityStarted` stays opaque,
    /// and the reopened run parks on it instead of re-dispatching.
    #[test]
    fn a_genuine_activity_failure_with_an_intervening_started_is_not_an_abandoned_record() {
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: chrono::Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "charge".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityStarted {
                activity_id,
                worker_id: WorkerId::new("worker-1"),
            },
            // Quotes the engine's exact abandoned shape, but a real activity
            // ran and failed this way on its own.
            WorkflowEvent::ActivityFailed {
                activity_id,
                error: crate::event::ABANDONED_DISPATCH_REASON.to_string(),
                attempt: 1,
                error_type: "Error".into(),
                details: None,
                non_retryable: true,
            },
            WorkflowEvent::workflow_failed("budget exceeded"),
            WorkflowEvent::WorkflowRedriven {
                redriven_at: chrono::Utc::now(),
                dead_letter_id: uuid::Uuid::new_v4(),
                reason: None,
            },
        ];
        let matcher = HistoryMatcher::new(events);
        for idx in [1_usize, 2, 3] {
            assert!(
                !matcher.is_consumed(idx),
                "event {idx} is a genuine activity failure and must stay matchable"
            );
        }
    }

    /// Two redrives, each with an activity, must not cross-contaminate (issue
    /// #1265). Activity ids are fresh per dispatch. A real `ActivityStarted`
    /// for one activity must never suppress the synthetic pair of an
    /// unrelated, earlier one, even across a redrive boundary.
    #[test]
    fn a_later_genuine_activity_does_not_mask_an_earlier_synthetic_pair() {
        let synthetic_activity = ActivityExecId::new();
        let genuine_activity = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: chrono::Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: synthetic_activity,
                name: "charge".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityFailed {
                activity_id: synthetic_activity,
                error: crate::event::ABANDONED_DISPATCH_REASON.to_string(),
                attempt: 1,
                error_type: "Error".into(),
                details: None,
                non_retryable: true,
            },
            WorkflowEvent::workflow_failed("budget exceeded"),
            WorkflowEvent::WorkflowRedriven {
                redriven_at: chrono::Utc::now(),
                dead_letter_id: uuid::Uuid::new_v4(),
                reason: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: genuine_activity,
                name: "refund".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityStarted {
                activity_id: genuine_activity,
                worker_id: WorkerId::new("worker-1"),
            },
            WorkflowEvent::ActivityFailed {
                activity_id: genuine_activity,
                error: crate::event::ABANDONED_DISPATCH_REASON.to_string(),
                attempt: 1,
                error_type: "Error".into(),
                details: None,
                non_retryable: true,
            },
            WorkflowEvent::workflow_failed("budget exceeded"),
            WorkflowEvent::WorkflowRedriven {
                redriven_at: chrono::Utc::now(),
                dead_letter_id: uuid::Uuid::new_v4(),
                reason: None,
            },
        ];
        let matcher = HistoryMatcher::new(events);
        for idx in [1_usize, 2] {
            assert!(
                matcher.is_consumed(idx),
                "event {idx} is the pre-redrive synthetic pair and must stay transparent"
            );
        }
        for idx in [5_usize, 6, 7] {
            assert!(
                !matcher.is_consumed(idx),
                "event {idx} is a genuine activity failure and must stay matchable"
            );
        }
    }

    /// Without a redrive the same records are ordinary, positionally-matched
    /// history: they are what makes a failing cycle's own replay resolvable.
    #[test]
    fn abandoned_dispatch_records_stay_opaque_without_a_redrive() {
        let child_id = ExecutionId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: chrono::Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "worker_child".into(),
                input: Value::Null,
            },
            WorkflowEvent::ChildWorkflowFailed {
                child_id,
                error: crate::event::ABANDONED_DISPATCH_REASON.to_string(),
                error_type: None,
                details: None,
                non_retryable: Some(true),
            },
            WorkflowEvent::workflow_failed("budget exceeded"),
        ];
        let mut matcher = HistoryMatcher::new(events);
        assert!(!matcher.is_consumed(1) && !matcher.is_consumed(2));
        matcher.advance();
        assert!(
            matches!(
                matcher.match_child_workflow("worker_child", &Value::Null),
                HistoryMatch::Failed { .. }
            ),
            "the failing cycle's own replay resolves the branch from its record"
        );
    }

    /// A retried-then-redriven run: the redrive scan must skip the
    /// post-terminal `WorkflowRetryScheduled` to reach the terminal it
    /// supersedes.
    #[test]
    fn a_redrive_after_a_workflow_retry_still_supersedes_the_terminal() {
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: chrono::Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::workflow_failed("boom"),
            WorkflowEvent::WorkflowRetryScheduled {
                retry_exec_id: ExecutionId::new(),
                attempt: 2,
                fire_at: chrono::Utc::now(),
            },
            WorkflowEvent::WorkflowRedriven {
                redriven_at: chrono::Utc::now(),
                dead_letter_id: uuid::Uuid::new_v4(),
                reason: None,
            },
        ];
        let matcher = HistoryMatcher::new(events);
        assert!(
            matcher.is_consumed(1),
            "the superseded terminal must be transparent even behind a retry record"
        );
    }

    /// Issue #1262: post-terminal bookkeeping (`WorkflowRetryScheduled`,
    /// `ChildWorkflowCascadeApplied`) carries no workflow command. The
    /// workflow function never consumes it. It must be transparent on its
    /// own, not only while a redrive searches for its superseded
    /// terminal. A bare retry-then-redrive history with nothing else in
    /// between must not diverge on the bookkeeping event itself.
    #[test]
    fn a_retry_then_redrive_with_no_dispatch_still_re_dispatches_live() {
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: chrono::Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::workflow_failed("boom"),
            WorkflowEvent::WorkflowRetryScheduled {
                retry_exec_id: ExecutionId::new(),
                attempt: 2,
                fire_at: chrono::Utc::now(),
            },
            WorkflowEvent::WorkflowRedriven {
                redriven_at: chrono::Utc::now(),
                dead_letter_id: uuid::Uuid::new_v4(),
                reason: None,
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        assert!(
            matcher.is_consumed(2),
            "post-terminal bookkeeping must be transparent on its own"
        );
        matcher.advance(); // past WorkflowStarted
        assert_eq!(
            matcher.match_activity("charge"),
            HistoryMatch::NoMatch,
            "a retry record between the terminal and the redrive must not block re-dispatch"
        );
    }

    /// Issue #1262: the exact combination that motivated the fix above. A
    /// failing cycle records an abandoned dispatch and a trailing marker.
    /// The sealed run also picks up a workflow-level retry record before
    /// the operator redrives it. The bookkeeping event must not stop the
    /// tail walk before it reaches the marker behind it.
    #[test]
    fn a_marker_behind_retry_bookkeeping_still_re_dispatches_live() {
        let child_id = ExecutionId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: chrono::Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "worker_child".into(),
                input: Value::Null,
            },
            WorkflowEvent::ChildWorkflowFailed {
                child_id,
                error: crate::event::ABANDONED_DISPATCH_REASON.to_string(),
                error_type: None,
                details: None,
                non_retryable: Some(true),
            },
            WorkflowEvent::MarkerRecorded {
                name: "m".into(),
                details: Value::Null,
            },
            WorkflowEvent::workflow_failed("budget exceeded"),
            WorkflowEvent::WorkflowRetryScheduled {
                retry_exec_id: ExecutionId::new(),
                attempt: 2,
                fire_at: chrono::Utc::now(),
            },
            WorkflowEvent::WorkflowRedriven {
                redriven_at: chrono::Utc::now(),
                dead_letter_id: uuid::Uuid::new_v4(),
                reason: None,
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        for idx in 1..=5 {
            assert!(
                matcher.is_consumed(idx),
                "event {idx} must be transparent behind the retry bookkeeping too"
            );
        }
        matcher.advance(); // past WorkflowStarted
        assert_eq!(
            matcher.match_child_workflow("worker_child", &Value::Null),
            HistoryMatch::NoMatch,
            "retry bookkeeping must not stop the walk before it reaches the marker"
        );
    }

    #[test]
    fn failing_frontier_requires_every_pre_terminal_event_consumed() {
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: chrono::Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "charge".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: Value::Null,
            },
            WorkflowEvent::workflow_failed("boom"),
        ];
        let mut matcher = HistoryMatcher::new(events);
        matcher.advance(); // past WorkflowStarted
        assert!(
            !matcher.at_terminal_failure_frontier(),
            "the recorded activity is still unconsumed — not at the frontier"
        );
        assert!(matches!(
            matcher.match_activity("charge"),
            HistoryMatch::Matched { .. }
        ));
        assert!(
            matcher.at_terminal_failure_frontier(),
            "with the activity consumed the cursor sits in the transparent tail"
        );
    }

    #[test]
    fn command_dispatch_past_redriven_tail_is_live_not_diverged() {
        // After a redrive, the re-enqueued task re-issues the next command. With
        // the redriven tail transparent the cursor runs off the end → not
        // replaying → NoMatch (execute live), never a divergence against the
        // reopened terminal.
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: chrono::Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::workflow_failed("boom"),
            WorkflowEvent::WorkflowRedriven {
                redriven_at: chrono::Utc::now(),
                dead_letter_id: uuid::Uuid::new_v4(),
                reason: None,
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        matcher.advance(); // past WorkflowStarted; transparent tail is skipped
        assert_eq!(
            matcher.match_activity("retry_me"),
            HistoryMatch::NoMatch,
            "a command re-issued past the redriven tail must execute live"
        );
    }

    // ── Local activity tests ──────────────────────────────────────────────

    #[test]
    fn matcher_replays_completed_local_activity() {
        let id = ActivityExecId::new();
        let output = serde_json::json!({"formatted": "hello"});
        let events = vec![
            WorkflowEvent::LocalActivityScheduled {
                activity_id: id,
                name: "format_data".into(),
                input: Value::Null,
                retry_policy: None,
                resolved: false,
                start_to_close_nanos: None,
            },
            WorkflowEvent::LocalActivityCompleted {
                activity_id: id,
                output: output.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_local_activity("format_data");
        assert_eq!(result, HistoryMatch::Matched { output });
        assert_eq!(matcher.position(), 2);
        assert!(!matcher.is_replaying());
    }

    #[test]
    fn matcher_local_activity_with_recorded_failures_returns_in_progress() {
        // The replay engine does not know max_attempts, so it always returns
        // LocalActivityInProgress when there is no completion event — even if
        // all retries may be exhausted. The worker checks max_attempts and
        // either returns last_error immediately or runs the next attempt.
        let id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::LocalActivityScheduled {
                activity_id: id,
                name: "format_data".into(),
                input: Value::Null,
                retry_policy: None,
                resolved: false,
                start_to_close_nanos: None,
            },
            WorkflowEvent::LocalActivityFailed {
                activity_id: id,
                error: "transient".into(),
                attempt: 1,
            },
            WorkflowEvent::LocalActivityFailed {
                activity_id: id,
                error: "still failing".into(),
                attempt: 2,
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_local_activity("format_data");
        assert!(
            matches!(
                &result,
                HistoryMatch::LocalActivityInProgress {
                    activity_id: rid,
                    failed_attempts: 2,
                    last_error: Some(e),
                } if *rid == id && e == "still failing"
            ),
            "expected LocalActivityInProgress with 2 failed attempts, got {result:?}"
        );
    }

    #[test]
    fn matcher_local_activity_exhausted_event_returns_failed() {
        // LocalActivityExhausted is the authoritative terminal marker. Replay
        // must return Failed regardless of any current retry-policy value.
        let id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::LocalActivityScheduled {
                activity_id: id,
                name: "format_data".into(),
                input: Value::Null,
                retry_policy: None,
                resolved: false,
                start_to_close_nanos: None,
            },
            WorkflowEvent::LocalActivityFailed {
                activity_id: id,
                error: "transient".into(),
                attempt: 1,
            },
            WorkflowEvent::LocalActivityExhausted {
                activity_id: id,
                error: "transient".into(),
                attempt: 1,
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_local_activity("format_data");
        assert_eq!(
            result,
            HistoryMatch::Failed {
                error: "transient".into(),
                attempt: 1,
                error_type: "Error".into(),
                details: None,
                non_retryable: false,
            }
        );
    }

    #[test]
    fn matcher_local_activity_skips_intermediate_failures_and_returns_completion() {
        let id = ActivityExecId::new();
        let output = serde_json::json!({"ok": true});
        let events = vec![
            WorkflowEvent::LocalActivityScheduled {
                activity_id: id,
                name: "format_data".into(),
                input: Value::Null,
                retry_policy: None,
                resolved: false,
                start_to_close_nanos: None,
            },
            WorkflowEvent::LocalActivityFailed {
                activity_id: id,
                error: "transient".into(),
                attempt: 1,
            },
            WorkflowEvent::LocalActivityFailed {
                activity_id: id,
                error: "still transient".into(),
                attempt: 2,
            },
            WorkflowEvent::LocalActivityCompleted {
                activity_id: id,
                output: output.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_local_activity("format_data");
        assert_eq!(result, HistoryMatch::Matched { output });
        assert_eq!(matcher.position(), 4);
    }

    #[test]
    fn matcher_local_activity_no_match_at_end_of_history() {
        let mut matcher = HistoryMatcher::new(vec![]);
        let result = matcher.match_local_activity("format_data");
        assert_eq!(result, HistoryMatch::NoMatch);
    }

    #[test]
    fn matcher_local_activity_detects_divergence_wrong_event_type() {
        let timer_id = TimerId::new("t1");
        let events = vec![WorkflowEvent::TimerStarted {
            timer_id,
            duration_secs: 10,
        }];
        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_local_activity("format_data");
        assert!(matches!(result, HistoryMatch::Diverged { .. }));
        if let HistoryMatch::Diverged {
            expected, actual, ..
        } = result
        {
            assert!(expected.contains("format_data"));
            assert!(actual.contains("TimerStarted"));
        }
    }

    #[test]
    fn matcher_local_activity_detects_divergence_wrong_name() {
        let id = ActivityExecId::new();
        let events = vec![WorkflowEvent::LocalActivityScheduled {
            activity_id: id,
            name: "other_activity".into(),
            input: Value::Null,
            retry_policy: None,
            resolved: false,
            start_to_close_nanos: None,
        }];
        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_local_activity("format_data");
        assert!(matches!(result, HistoryMatch::Diverged { .. }));
        if let HistoryMatch::Diverged {
            expected, actual, ..
        } = result
        {
            assert!(expected.contains("format_data"));
            assert!(actual.contains("other_activity"));
        }
    }

    #[test]
    fn matcher_local_activity_stashes_signals_during_retry_scan() {
        let id = ActivityExecId::new();
        let output = serde_json::json!({"ok": true});
        let events = vec![
            WorkflowEvent::LocalActivityScheduled {
                activity_id: id,
                name: "format_data".into(),
                input: Value::Null,
                retry_policy: None,
                resolved: false,
                start_to_close_nanos: None,
            },
            WorkflowEvent::LocalActivityFailed {
                activity_id: id,
                error: "transient".into(),
                attempt: 1,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "abort".into(),
                payload: serde_json::json!({"reason": "user"}),
            },
            WorkflowEvent::LocalActivityCompleted {
                activity_id: id,
                output: output.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_local_activity("format_data");
        assert_eq!(result, HistoryMatch::Matched { output });

        // Signal buffered during scan must be deliverable later.
        let signal = matcher.match_signal("abort");
        assert_eq!(
            signal,
            HistoryMatch::Matched {
                output: serde_json::json!({"reason": "user"})
            }
        );
    }

    #[test]
    fn matcher_replays_sequential_local_and_regular_activities() {
        let local_id = ActivityExecId::new();
        let regular_id = ActivityExecId::new();
        let local_out = serde_json::json!("formatted");
        let regular_out = serde_json::json!({"sent": true});
        let events = vec![
            WorkflowEvent::LocalActivityScheduled {
                activity_id: local_id,
                name: "format_data".into(),
                input: Value::Null,
                retry_policy: None,
                resolved: false,
                start_to_close_nanos: None,
            },
            WorkflowEvent::LocalActivityCompleted {
                activity_id: local_id,
                output: local_out.clone(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: regular_id,
                name: "send_email".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: regular_id,
                output: regular_out.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let r1 = matcher.match_local_activity("format_data");
        assert_eq!(r1, HistoryMatch::Matched { output: local_out });

        let r2 = matcher.match_activity("send_email");
        assert_eq!(
            r2,
            HistoryMatch::Matched {
                output: regular_out
            }
        );
        assert!(!matcher.is_replaying());
    }

    #[test]
    fn matcher_activity_skips_signal_interleaved_between_scheduled_and_completed() {
        // A signal arrives while an activity is running (between ActivityScheduled
        // and ActivityCompleted in history). match_activity must skip it and find
        // ActivityCompleted.
        let activity_id = ActivityExecId::new();
        let output = serde_json::json!({"rows": 100});
        let events = vec![
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "import_data".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "cancel".into(),
                payload: serde_json::json!({"reason": "manual"}),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: output.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let activity = matcher.match_activity("import_data");
        assert_eq!(
            activity,
            HistoryMatch::Matched { output },
            "match_activity should skip signals interleaved during activity execution"
        );

        let signal = matcher.match_signal("cancel");
        assert_eq!(
            signal,
            HistoryMatch::Matched {
                output: serde_json::json!({"reason": "manual"})
            },
            "signal buffered during activity scan should be returned by match_signal"
        );
    }

    #[test]
    fn matcher_activity_preserves_detached_spawn_interleaved_before_completion() {
        let activity_id = ActivityExecId::new();
        let child_id = ExecutionId::new();
        let output = serde_json::json!({"rows": 100});
        let events = vec![
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "import_data".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ChildWorkflowSpawnedDetached {
                child_id,
                workflow_name: "monitor".into(),
                input: Value::Null,
                parent_close_policy: ParentClosePolicy::Abandon,
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: output.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        assert_eq!(
            matcher.match_activity("import_data"),
            HistoryMatch::Matched { output }
        );
        assert_eq!(
            matcher
                .match_detached_child_spawn("monitor", &Value::Null, ParentClosePolicy::Abandon,),
            HistoryMatch::DetachedChildSpawned { child_id }
        );
        assert!(!matcher.is_replaying());
    }

    #[test]
    fn matcher_timer_preserves_detached_spawn_interleaved_before_fire() {
        let timer_id = TimerId::new("cooldown");
        let child_id = ExecutionId::new();
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 30,
            },
            WorkflowEvent::ChildWorkflowSpawnedDetached {
                child_id,
                workflow_name: "monitor".into(),
                input: Value::Null,
                parent_close_policy: ParentClosePolicy::Abandon,
            },
            WorkflowEvent::TimerFired { timer_id },
        ];
        let mut matcher = HistoryMatcher::new(events);

        assert_eq!(
            matcher.match_timer("cooldown"),
            HistoryMatch::Matched {
                output: Value::Null
            }
        );
        assert_eq!(
            matcher
                .match_detached_child_spawn("monitor", &Value::Null, ParentClosePolicy::Abandon,),
            HistoryMatch::DetachedChildSpawned { child_id }
        );
    }

    #[test]
    fn matcher_local_activity_preserves_detached_spawn_interleaved_before_completion() {
        let activity_id = ActivityExecId::new();
        let child_id = ExecutionId::new();
        let output = serde_json::json!({"formatted": true});
        let events = vec![
            WorkflowEvent::LocalActivityScheduled {
                activity_id,
                name: "format_data".into(),
                input: Value::Null,
                retry_policy: None,
                resolved: false,
                start_to_close_nanos: None,
            },
            WorkflowEvent::ChildWorkflowSpawnedDetached {
                child_id,
                workflow_name: "monitor".into(),
                input: Value::Null,
                parent_close_policy: ParentClosePolicy::Abandon,
            },
            WorkflowEvent::LocalActivityCompleted {
                activity_id,
                output: output.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        assert_eq!(
            matcher.match_local_activity("format_data"),
            HistoryMatch::Matched { output }
        );
        assert_eq!(
            matcher
                .match_detached_child_spawn("monitor", &Value::Null, ParentClosePolicy::Abandon,),
            HistoryMatch::DetachedChildSpawned { child_id }
        );
    }

    #[test]
    fn matcher_signal_wait_preserves_detached_spawn_before_later_signal() {
        let child_id = ExecutionId::new();
        let events = vec![
            WorkflowEvent::ChildWorkflowSpawnedDetached {
                child_id,
                workflow_name: "monitor".into(),
                input: Value::Null,
                parent_close_policy: ParentClosePolicy::Abandon,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approved".into(),
                payload: serde_json::json!({"ok": true}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        assert_eq!(
            matcher.match_signal("approved"),
            HistoryMatch::Matched {
                output: serde_json::json!({"ok": true})
            }
        );
        assert_eq!(
            matcher
                .match_detached_child_spawn("monitor", &Value::Null, ParentClosePolicy::Abandon,),
            HistoryMatch::DetachedChildSpawned { child_id }
        );
    }

    #[test]
    fn matcher_external_activity_preserves_detached_spawn_before_external_completion() {
        let activity_id = ActivityExecId::new();
        let token = ExternalActivityToken::new();
        let child_id = ExecutionId::new();
        let output = serde_json::json!({"accepted": true});
        let events = vec![
            WorkflowEvent::ActivityAwaitingExternal {
                activity_id,
                token,
                name: "ship_order".into(),
                input: Value::Null,
                queue: "fulfillment".into(),
                schedule_to_close_secs: 60,
            },
            WorkflowEvent::ChildWorkflowSpawnedDetached {
                child_id,
                workflow_name: "monitor".into(),
                input: Value::Null,
                parent_close_policy: ParentClosePolicy::Abandon,
            },
            WorkflowEvent::ActivityCompletedExternally {
                activity_id,
                token,
                output: output.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        assert_eq!(
            matcher.match_external_activity("ship_order"),
            HistoryMatch::Matched { output }
        );
        assert_eq!(
            matcher
                .match_detached_child_spawn("monitor", &Value::Null, ParentClosePolicy::Abandon,),
            HistoryMatch::DetachedChildSpawned { child_id }
        );
    }

    #[test]
    fn matcher_external_signal_preserves_detached_spawn_before_delivery() {
        let signal_id = ExternalSignalId::new();
        let target = ExecutionId::new();
        let child_id = ExecutionId::new();
        let events = vec![
            WorkflowEvent::ExternalSignalRequested {
                signal_id,
                target: crate::types::ExternalTarget::ExecutionId(target),
                signal_name: "poke".into(),
                payload: serde_json::json!({"n": 1}),
                idempotency_key: None,
            },
            WorkflowEvent::ChildWorkflowSpawnedDetached {
                child_id,
                workflow_name: "monitor".into(),
                input: Value::Null,
                parent_close_policy: ParentClosePolicy::Abandon,
            },
            WorkflowEvent::ExternalSignalDelivered { signal_id },
        ];
        let mut matcher = HistoryMatcher::new(events);

        assert_eq!(
            matcher
                .match_external_signal(&crate::types::ExternalTarget::ExecutionId(target), "poke"),
            HistoryMatch::Matched {
                output: Value::Null
            }
        );
        assert_eq!(
            matcher
                .match_detached_child_spawn("monitor", &Value::Null, ParentClosePolicy::Abandon,),
            HistoryMatch::DetachedChildSpawned { child_id }
        );
    }

    // ── match_signal_or_timer (issue #476) ────────────────────────────────

    #[test]
    fn signal_or_timer_signal_wins_when_recorded_before_timer_fired() {
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"approved": true}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(300));
        assert_eq!(
            result,
            SignalOrTimerMatch::SignalWon {
                payload: serde_json::json!({"approved": true})
            }
        );
    }

    #[test]
    fn signal_or_timer_signal_win_consumes_stray_timer_fired() {
        // The durable timer fires after the signal already won. The stray
        // TimerFired must be consumed so subsequent matches do not diverge.
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"approved": true}),
            },
            WorkflowEvent::TimerFired {
                timer_id: timer_id.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(300));
        assert_eq!(
            result,
            SignalOrTimerMatch::SignalWon {
                payload: serde_json::json!({"approved": true})
            }
        );
        assert!(
            !matcher.is_replaying(),
            "stray TimerFired must be consumed after the signal wins"
        );
    }

    #[test]
    fn signal_or_timer_timer_wins_when_fired_first() {
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::TimerFired {
                timer_id: timer_id.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(300));
        assert_eq!(result, SignalOrTimerMatch::TimerWon);
    }

    #[test]
    fn signal_or_timer_timer_win_does_not_consume_late_signal() {
        // The signal arrives after the timer already fired: the timer wins and
        // the late signal stays observable for a subsequent match_signal.
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::TimerFired {
                timer_id: timer_id.clone(),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"approved": true}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(300));
        assert_eq!(result, SignalOrTimerMatch::TimerWon);

        let late = matcher.match_signal("approval");
        assert_eq!(
            late,
            HistoryMatch::Matched {
                output: serde_json::json!({"approved": true})
            },
            "a signal that lost the race must remain observable later"
        );
    }

    #[test]
    fn signal_or_timer_signal_wins_when_received_before_race_started() {
        // The signal was ingested before the race point — no timer was ever
        // started on the corresponding live run.
        let events = vec![WorkflowEvent::SignalReceived {
            signal_name: "approval".into(),
            payload: serde_json::json!({"approved": false}),
        }];
        let mut matcher = HistoryMatcher::new(events);

        let result =
            matcher.match_signal_or_timer("approval", "__signal_timeout:1:approval", Some(300));
        assert_eq!(
            result,
            SignalOrTimerMatch::SignalWon {
                payload: serde_json::json!({"approved": false})
            }
        );
    }

    #[test]
    fn signal_or_timer_no_match_on_empty_history() {
        let mut matcher = HistoryMatcher::new(vec![]);
        let result =
            matcher.match_signal_or_timer("approval", "__signal_timeout:1:approval", Some(300));
        assert_eq!(result, SignalOrTimerMatch::NoMatch);
    }

    #[test]
    fn signal_or_timer_in_progress_when_neither_resolution_recorded() {
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let events = vec![WorkflowEvent::TimerStarted {
            timer_id: timer_id.clone(),
            duration_secs: 300,
        }];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(300));
        assert_eq!(result, SignalOrTimerMatch::InProgress);
    }

    #[test]
    fn signal_or_timer_diverges_on_unrelated_event() {
        let events = vec![WorkflowEvent::ActivityScheduled {
            activity_id: ActivityExecId::new(),
            name: "send_email".into(),
            input: Value::Null,
            queue: "default".into(),
        }];
        let mut matcher = HistoryMatcher::new(events);

        let result =
            matcher.match_signal_or_timer("approval", "__signal_timeout:1:approval", Some(300));
        assert!(matches!(result, SignalOrTimerMatch::Diverged { .. }));
    }

    #[test]
    fn signal_or_timer_diverges_on_duration_change() {
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::TimerFired {
                timer_id: timer_id.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(600));
        assert!(matches!(result, SignalOrTimerMatch::Diverged { .. }));
    }

    #[test]
    fn signal_or_timer_stashes_non_matching_signals_during_scan() {
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "other".into(),
                payload: serde_json::json!(1),
            },
            WorkflowEvent::TimerFired {
                timer_id: timer_id.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(300));
        assert_eq!(result, SignalOrTimerMatch::TimerWon);

        let other = matcher.match_signal("other");
        assert_eq!(
            other,
            HistoryMatch::Matched {
                output: serde_json::json!(1)
            },
            "non-matching signals scanned during the race must be stashed"
        );
    }

    #[test]
    fn signal_or_timer_signal_wins_across_interleaved_activity() {
        // tokio::join!(receive_signal_timeout, execute_activity): the sibling
        // activity's Scheduled event is interleaved between TimerStarted and
        // the race resolution. The scan must skip it (tracking it as the next
        // replay cursor) instead of reporting the race as still in progress.
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "audit_log".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"approved": true}),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!("logged"),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(300));
        assert_eq!(
            result,
            SignalOrTimerMatch::SignalWon {
                payload: serde_json::json!({"approved": true})
            }
        );

        // The cursor must rewind to the interleaved command so the join
        // sibling can match its own activity.
        let activity = matcher.match_activity("audit_log");
        assert_eq!(
            activity,
            HistoryMatch::Matched {
                output: serde_json::json!("logged")
            }
        );
    }

    #[test]
    fn signal_or_timer_timer_wins_across_interleaved_activity() {
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "audit_log".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::TimerFired {
                timer_id: timer_id.clone(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!("logged"),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(300));
        assert_eq!(result, SignalOrTimerMatch::TimerWon);

        let activity = matcher.match_activity("audit_log");
        assert_eq!(
            activity,
            HistoryMatch::Matched {
                output: serde_json::json!("logged")
            }
        );
    }

    #[test]
    fn signal_or_timer_in_progress_rewinds_to_interleaved_command() {
        // The race is unresolved but a concurrent activity already has events
        // in history: InProgress must leave the cursor on the interleaved
        // command so the sibling matcher still finds it.
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "audit_log".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!("logged"),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(300));
        assert_eq!(result, SignalOrTimerMatch::InProgress);

        let activity = matcher.match_activity("audit_log");
        assert_eq!(
            activity,
            HistoryMatch::Matched {
                output: serde_json::json!("logged")
            }
        );
    }

    #[test]
    fn signal_or_timer_scans_past_interleaved_bookkeeping_and_sibling_timer() {
        // Markers, side effects, and a sibling timer from concurrent branches
        // must not hide the race resolution.
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let sibling_timer = TimerId::new("cooldown");
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::MarkerRecorded {
                name: "fan_out:1".into(),
                details: Value::Null,
            },
            WorkflowEvent::TimerStarted {
                timer_id: sibling_timer.clone(),
                duration_secs: 60,
            },
            WorkflowEvent::TimerFired {
                timer_id: sibling_timer,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"approved": true}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(300));
        assert_eq!(
            result,
            SignalOrTimerMatch::SignalWon {
                payload: serde_json::json!({"approved": true})
            }
        );
    }

    #[test]
    fn signal_or_timer_timer_win_late_signal_is_not_flagged_unconsumed() {
        // Timeout branch with a late approval that the workflow intentionally
        // ignores (the documented auto-reject case): the leftover signal must
        // not be reported as early-completion non-determinism by the strict
        // replay check.
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::TimerFired {
                timer_id: timer_id.clone(),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"approved": true}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(300));
        assert_eq!(result, SignalOrTimerMatch::TimerWon);

        assert!(
            !matcher.has_non_lifecycle_unconsumed(),
            "a late signal that lost the race must not flag the completed history"
        );
    }

    #[test]
    fn signal_or_timer_timer_win_late_signal_exemption_survives_stashing() {
        // After the timeout branch, a subsequent matcher scan stashes the late
        // signal into the pending buffer. The exemption must still apply.
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::TimerFired {
                timer_id: timer_id.clone(),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"approved": true}),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "notify".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!("sent"),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(300));
        assert_eq!(result, SignalOrTimerMatch::TimerWon);

        // The next command's prepare_match drains the late signal into the
        // pending stash before matching the activity.
        let activity = matcher.match_activity("notify");
        assert_eq!(
            activity,
            HistoryMatch::Matched {
                output: serde_json::json!("sent")
            }
        );

        assert!(
            !matcher.has_non_lifecycle_unconsumed(),
            "a stashed late race signal must not flag the completed history"
        );
    }

    // ── unconsumed_signals_by_name (issue #684) ───────────────────────────

    #[test]
    fn unconsumed_signals_by_name_counts_an_undrained_signal() {
        let events = vec![WorkflowEvent::SignalReceived {
            signal_name: "approve".into(),
            payload: serde_json::json!({"ok": true}),
        }];
        let matcher = HistoryMatcher::new(events);
        let counts = matcher.unconsumed_signals_by_name();
        assert_eq!(counts.get("approve"), Some(&1));
        assert_eq!(counts.len(), 1);
    }

    #[test]
    fn unconsumed_signals_by_name_omits_a_signal_consumed_by_wait() {
        let events = vec![WorkflowEvent::SignalReceived {
            signal_name: "approve".into(),
            payload: serde_json::json!({"ok": true}),
        }];
        let mut matcher = HistoryMatcher::new(events);
        // A wait_for_signal consumes it.
        assert!(matches!(
            matcher.match_signal("approve"),
            HistoryMatch::Matched { .. }
        ));
        assert!(
            matcher.unconsumed_signals_by_name().is_empty(),
            "a signal consumed by wait_for_signal must not be reported unhandled"
        );
    }

    #[test]
    fn unconsumed_signals_by_name_omits_a_signal_consumed_by_push_handler() {
        // Issue #546: a push-based signal handler claims the buffered signal.
        let events = vec![WorkflowEvent::SignalReceived {
            signal_name: "cancel".into(),
            payload: serde_json::json!({"reason": "fraud"}),
        }];
        let mut matcher = HistoryMatcher::new(events);
        let claimed = matcher.claim_pending_signal("cancel");
        assert_eq!(claimed.len(), 1, "the push handler must claim the signal");
        assert!(
            matcher.unconsumed_signals_by_name().is_empty(),
            "a signal claimed by a push handler must not be reported unhandled"
        );
    }

    #[test]
    fn unconsumed_signals_by_name_respects_lost_race_carve_out() {
        // Issue #476: the timeout branch won; the late-arriving signal is
        // deliberately never consumed and must not count as unhandled.
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::TimerFired {
                timer_id: timer_id.clone(),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"approved": true}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        assert_eq!(
            matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(300)),
            SignalOrTimerMatch::TimerWon
        );
        assert!(
            matcher.unconsumed_signals_by_name().is_empty(),
            "a signal that lost a signal-or-deadline race must not be reported unhandled"
        );
    }

    #[test]
    fn unconsumed_signals_by_name_counts_multiple_same_name_signals() {
        let events = vec![
            WorkflowEvent::SignalReceived {
                signal_name: "tick".into(),
                payload: Value::Null,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "tick".into(),
                payload: Value::Null,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "tick".into(),
                payload: Value::Null,
            },
        ];
        let matcher = HistoryMatcher::new(events);
        let counts = matcher.unconsumed_signals_by_name();
        assert_eq!(counts.get("tick"), Some(&3));
    }

    #[test]
    fn unconsumed_signals_by_name_counts_a_stashed_but_unconsumed_signal() {
        // A signal drained into pending_signals by a subsequent match that is
        // never consumed by a wait/push must still be reported unhandled.
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::SignalReceived {
                signal_name: "extra".into(),
                payload: Value::Null,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "work".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!("done"),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        // match_activity's prepare_match drains the leading signal into the
        // pending stash (index now < cursor), so it is only reachable via the
        // pending_signals source.
        assert!(matches!(
            matcher.match_activity("work"),
            HistoryMatch::Matched { .. }
        ));
        let counts = matcher.unconsumed_signals_by_name();
        assert_eq!(counts.get("extra"), Some(&1));
    }

    #[test]
    fn late_race_exemption_is_scoped_to_one_occurrence() {
        // One race loss excuses exactly one unconsumed signal of that name. A
        // second same-name signal whose wait was removed/skipped must still be
        // reported as non-determinism.
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::TimerFired {
                timer_id: timer_id.clone(),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"n": 1}),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"n": 2}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(300));
        assert_eq!(result, SignalOrTimerMatch::TimerWon);

        assert!(
            matcher.has_non_lifecycle_unconsumed(),
            "only one approval lost the race — the second unconsumed approval must flag"
        );
    }

    #[test]
    fn late_race_exemption_is_voided_when_loser_is_consumed() {
        // The exemption tracks the exact event that lost the race. If a later
        // wait consumes that signal, the exemption is spent with it: a second
        // same-name unconsumed signal (e.g. from a removed/skipped wait) must
        // still be reported as non-determinism.
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::TimerFired {
                timer_id: timer_id.clone(),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"n": 1}),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"n": 2}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(300));
        assert_eq!(result, SignalOrTimerMatch::TimerWon);

        // The wait consumes the front-most approval — the one that lost the
        // race — so the exemption travels with it.
        let consumed = matcher.match_signal("approval");
        assert_eq!(
            consumed,
            HistoryMatch::Matched {
                output: serde_json::json!({"n": 1})
            }
        );

        assert!(
            matcher.has_non_lifecycle_unconsumed(),
            "the second approval is not excused once the race loser was consumed"
        );
    }

    #[test]
    fn unraced_unconsumed_signal_still_flags_history() {
        // The late-race exemption must not weaken the existing removed-wait
        // detection for signals that never lost a race.
        let events = vec![WorkflowEvent::SignalReceived {
            signal_name: "approval".into(),
            payload: serde_json::json!({"approved": true}),
        }];
        let matcher = HistoryMatcher::new(events);

        assert!(
            matcher.has_non_lifecycle_unconsumed(),
            "an unconsumed signal with no race must still be flagged"
        );
    }

    #[test]
    fn signal_or_timer_stashed_signal_after_fire_does_not_win() {
        // A sibling race's scan stashes an "approval" that was recorded AFTER
        // this race's TimerFired. The stash must not override recorded history
        // order: the timer won, and the late approval stays deliverable.
        let r1 = TimerId::new("__signal_timeout:1:other");
        let r2 = TimerId::new("__signal_timeout:2:approval");
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: r1.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::TimerStarted {
                timer_id: r2.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::TimerFired {
                timer_id: r2.clone(),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"approved": true}),
            },
            WorkflowEvent::TimerFired {
                timer_id: r1.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        // Race 1 ("other") scans across the whole history, stashing the
        // non-matching "approval" recorded at index 3.
        let first = matcher.match_signal_or_timer("other", r1.as_str(), Some(300));
        assert_eq!(first, SignalOrTimerMatch::TimerWon);

        // Race 2 ("approval"): its TimerFired (index 2) precedes the stashed
        // approval (index 3) — the timer won by history order.
        let second = matcher.match_signal_or_timer("approval", r2.as_str(), Some(300));
        assert_eq!(second, SignalOrTimerMatch::TimerWon);

        // The late approval remains deliverable to a subsequent wait.
        let late = matcher.match_signal("approval");
        assert_eq!(
            late,
            HistoryMatch::Matched {
                output: serde_json::json!({"approved": true})
            }
        );
    }

    #[test]
    fn signal_or_timer_stashed_signal_before_fire_still_wins() {
        // The mirror case: the stashed approval was recorded BEFORE this
        // race's TimerFired, so the signal branch wins even though a sibling
        // scan moved the event into the pending stash.
        let r1 = TimerId::new("__signal_timeout:1:other");
        let r2 = TimerId::new("__signal_timeout:2:approval");
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: r1.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::TimerStarted {
                timer_id: r2.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"approved": true}),
            },
            WorkflowEvent::TimerFired {
                timer_id: r2.clone(),
            },
            WorkflowEvent::TimerFired {
                timer_id: r1.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let first = matcher.match_signal_or_timer("other", r1.as_str(), Some(300));
        assert_eq!(first, SignalOrTimerMatch::TimerWon);

        let second = matcher.match_signal_or_timer("approval", r2.as_str(), Some(300));
        assert_eq!(
            second,
            SignalOrTimerMatch::SignalWon {
                payload: serde_json::json!({"approved": true})
            }
        );
        assert!(
            !matcher.has_non_lifecycle_unconsumed(),
            "winning signal and both timer events must all be settled"
        );
    }

    #[test]
    fn signal_or_timer_replays_same_branch_when_both_events_exist() {
        // Whichever event was recorded first wins on every replay regardless
        // of wall-clock timing on the replaying worker.
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"approved": true}),
            },
            WorkflowEvent::TimerFired {
                timer_id: timer_id.clone(),
            },
        ];

        for _ in 0..3 {
            let mut matcher = HistoryMatcher::new(events.clone());
            let result = matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(300));
            assert_eq!(
                result,
                SignalOrTimerMatch::SignalWon {
                    payload: serde_json::json!({"approved": true})
                }
            );
        }
    }

    // ── match_child_or_timer (issue #779) ─────────────────────────────────
    //
    // Deadline-bounded child-workflow awaits, mirroring match_signal_or_timer.
    // The race composes ChildWorkflowStarted/Completed/Failed with
    // TimerStarted/TimerFired — no new event variant. Winner = earliest
    // recorded history index. RED PHASE: ChildOrTimerMatch and
    // HistoryMatcher::match_child_or_timer do not exist yet.

    fn child_timer_id() -> TimerId {
        TimerId::new("__child_timeout:1:process_order")
    }

    fn child_started_event(child_id: ExecutionId) -> WorkflowEvent {
        WorkflowEvent::ChildWorkflowStarted {
            child_id,
            workflow_name: "process_order".into(),
            input: serde_json::json!({"id": 42}),
        }
    }

    #[test]
    fn child_or_timer_child_completes_before_timer_fired_wins() {
        let timer_id = child_timer_id();
        let child_id = ExecutionId::new();
        let events = vec![
            child_started_event(child_id),
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id,
                output: serde_json::json!({"processed": true}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_child_or_timer(
            "process_order",
            &serde_json::json!({"id": 42}),
            timer_id.as_str(),
            Some(300),
        );
        assert_eq!(
            result,
            ChildOrTimerMatch::ChildCompleted {
                output: serde_json::json!({"processed": true})
            }
        );
    }

    #[test]
    fn child_or_timer_child_win_consumes_stray_timer_fired() {
        // The durable deadline timer fires after the child already won. The
        // stray TimerFired must be consumed so subsequent matches do not
        // diverge and the strict unconsumed check passes.
        let timer_id = child_timer_id();
        let child_id = ExecutionId::new();
        let events = vec![
            child_started_event(child_id),
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id,
                output: serde_json::json!({"processed": true}),
            },
            WorkflowEvent::TimerFired {
                timer_id: timer_id.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_child_or_timer(
            "process_order",
            &serde_json::json!({"id": 42}),
            timer_id.as_str(),
            Some(300),
        );
        assert_eq!(
            result,
            ChildOrTimerMatch::ChildCompleted {
                output: serde_json::json!({"processed": true})
            }
        );
        assert!(
            !matcher.has_non_lifecycle_unconsumed(),
            "stray deadline TimerFired must be consumed after the child wins"
        );
    }

    #[test]
    fn child_or_timer_child_terminal_only_wins() {
        // A completed child with no timer ever fired resolves to ChildCompleted.
        let timer_id = child_timer_id();
        let child_id = ExecutionId::new();
        let events = vec![
            child_started_event(child_id),
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id,
                output: serde_json::json!("done"),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_child_or_timer(
            "process_order",
            &serde_json::json!({"id": 42}),
            timer_id.as_str(),
            Some(300),
        );
        assert_eq!(
            result,
            ChildOrTimerMatch::ChildCompleted {
                output: serde_json::json!("done")
            }
        );
    }

    #[test]
    fn child_or_timer_timer_wins_when_fired_before_child_terminal() {
        let timer_id = child_timer_id();
        let child_id = ExecutionId::new();
        let events = vec![
            child_started_event(child_id),
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::TimerFired {
                timer_id: timer_id.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_child_or_timer(
            "process_order",
            &serde_json::json!({"id": 42}),
            timer_id.as_str(),
            Some(300),
        );
        assert_eq!(
            result,
            ChildOrTimerMatch::TimerFired {
                child_id,
                child_already_terminal: false,
            },
            "timer wins and the still-running child is not yet terminal, so the \
             caller must push CancelRaceLosers on this cycle"
        );
    }

    #[test]
    fn child_or_timer_timer_win_consumes_loser_child_terminal() {
        // Timer fired first, then the losing child's terminal (a synthetic
        // ChildWorkflowFailed from the race-loser cancellation) was recorded.
        // The matcher must genuinely consume that loser terminal (it is
        // deliverable to nobody) and report child_already_terminal = true so
        // the caller does NOT re-push CancelRaceLosers.
        let timer_id = child_timer_id();
        let child_id = ExecutionId::new();
        let events = vec![
            child_started_event(child_id),
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::TimerFired {
                timer_id: timer_id.clone(),
            },
            WorkflowEvent::ChildWorkflowFailed {
                child_id,
                error: "lost race to a sibling branch".into(),
                error_type: None,
                details: None,
                non_retryable: None,
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_child_or_timer(
            "process_order",
            &serde_json::json!({"id": 42}),
            timer_id.as_str(),
            Some(300),
        );
        assert_eq!(
            result,
            ChildOrTimerMatch::TimerFired {
                child_id,
                child_already_terminal: true,
            }
        );
        assert!(
            !matcher.has_non_lifecycle_unconsumed(),
            "the losing child terminal must be genuinely consumed on timer-win"
        );
    }

    #[test]
    fn child_or_timer_late_child_terminal_after_timer_win_is_preserved_not_corrupted() {
        // The timer wins; a child terminal recorded AFTER the fire is the
        // losing child being sealed. It is consumed (transparent), and the
        // race still resolves to TimerFired — the late terminal must not flip
        // the winner or trip a divergence.
        let timer_id = child_timer_id();
        let child_id = ExecutionId::new();
        let events = vec![
            child_started_event(child_id),
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::TimerFired {
                timer_id: timer_id.clone(),
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id,
                output: serde_json::json!("raced-but-lost"),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_child_or_timer(
            "process_order",
            &serde_json::json!({"id": 42}),
            timer_id.as_str(),
            Some(300),
        );
        assert_eq!(
            result,
            ChildOrTimerMatch::TimerFired {
                child_id,
                child_already_terminal: true,
            }
        );
        assert!(
            !matcher.has_non_lifecycle_unconsumed(),
            "a late losing child terminal after timer-win must be consumed, not left dangling"
        );
    }

    #[test]
    fn child_or_timer_child_fails_before_deadline_returns_typed_fields() {
        let timer_id = child_timer_id();
        let child_id = ExecutionId::new();
        let events = vec![
            child_started_event(child_id),
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::ChildWorkflowFailed {
                child_id,
                error: "downstream 503".into(),
                error_type: Some("UpstreamUnavailable".into()),
                details: Some(serde_json::json!({"retry_after": 30})),
                non_retryable: Some(true),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_child_or_timer(
            "process_order",
            &serde_json::json!({"id": 42}),
            timer_id.as_str(),
            Some(300),
        );
        assert_eq!(
            result,
            ChildOrTimerMatch::ChildFailed {
                error: "downstream 503".into(),
                error_type: "UpstreamUnavailable".into(),
                details: Some(serde_json::json!({"retry_after": 30})),
                non_retryable: true,
            }
        );
    }

    #[test]
    fn child_or_timer_legacy_child_failure_maps_error_type_sentinel() {
        // A pre-#767 / untyped child failure decodes to the "Error" sentinel
        // with no details and not non-retryable, mirroring match_child_workflow.
        let timer_id = child_timer_id();
        let child_id = ExecutionId::new();
        let events = vec![
            child_started_event(child_id),
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::ChildWorkflowFailed {
                child_id,
                error: "boom".into(),
                error_type: None,
                details: None,
                non_retryable: None,
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_child_or_timer(
            "process_order",
            &serde_json::json!({"id": 42}),
            timer_id.as_str(),
            Some(300),
        );
        assert_eq!(
            result,
            ChildOrTimerMatch::ChildFailed {
                error: "boom".into(),
                error_type: "Error".into(),
                details: None,
                non_retryable: false,
            }
        );
    }

    #[test]
    fn child_or_timer_no_match_on_empty_history() {
        // Child not started yet — first live execution of the race.
        let mut matcher = HistoryMatcher::new(vec![]);
        let result = matcher.match_child_or_timer(
            "process_order",
            &serde_json::json!({"id": 42}),
            "__child_timeout:1:process_order",
            Some(300),
        );
        assert_eq!(result, ChildOrTimerMatch::NoMatch);
    }

    #[test]
    fn child_or_timer_in_progress_when_both_started_but_neither_resolved() {
        // ChildWorkflowStarted + TimerStarted recorded, but no terminal and no
        // TimerFired yet: the caller must re-park. InProgress carries the
        // recorded child_id so the re-emitted StartChildWorkflow reuses it.
        let timer_id = child_timer_id();
        let child_id = ExecutionId::new();
        let events = vec![
            child_started_event(child_id),
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_child_or_timer(
            "process_order",
            &serde_json::json!({"id": 42}),
            timer_id.as_str(),
            Some(300),
        );
        assert_eq!(
            result,
            ChildOrTimerMatch::InProgress { child_id },
            "InProgress must return the recorded child_id for the re-park"
        );
    }

    #[test]
    fn child_or_timer_diverges_on_wrong_child_start() {
        let events = vec![WorkflowEvent::ActivityScheduled {
            activity_id: ActivityExecId::new(),
            name: "send_email".into(),
            input: Value::Null,
            queue: "default".into(),
        }];
        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_child_or_timer(
            "process_order",
            &serde_json::json!({"id": 42}),
            "__child_timeout:1:process_order",
            Some(300),
        );
        assert!(matches!(result, ChildOrTimerMatch::Diverged { .. }));
    }

    #[test]
    fn child_or_timer_diverges_on_wrong_timer_id() {
        let child_id = ExecutionId::new();
        let events = vec![
            child_started_event(child_id),
            WorkflowEvent::TimerStarted {
                timer_id: TimerId::new("__child_timeout:99:process_order"),
                duration_secs: 300,
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_child_or_timer(
            "process_order",
            &serde_json::json!({"id": 42}),
            "__child_timeout:1:process_order",
            Some(300),
        );
        assert!(matches!(result, ChildOrTimerMatch::Diverged { .. }));
    }

    #[test]
    fn child_or_timer_diverges_on_duration_change() {
        let timer_id = child_timer_id();
        let child_id = ExecutionId::new();
        let events = vec![
            child_started_event(child_id),
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::TimerFired {
                timer_id: timer_id.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_child_or_timer(
            "process_order",
            &serde_json::json!({"id": 42}),
            timer_id.as_str(),
            Some(600),
        );
        assert!(matches!(result, ChildOrTimerMatch::Diverged { .. }));
    }

    #[test]
    fn child_or_timer_diverges_on_wrong_child_input() {
        let timer_id = child_timer_id();
        let child_id = ExecutionId::new();
        let events = vec![
            child_started_event(child_id),
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_child_or_timer(
            "process_order",
            &serde_json::json!({"id": 999}),
            timer_id.as_str(),
            Some(300),
        );
        assert!(matches!(result, ChildOrTimerMatch::Diverged { .. }));
    }

    #[test]
    fn child_or_timer_child_win_across_interleaved_sibling_activity() {
        // A concurrent sibling activity's events are interleaved between the
        // child/timer start pair and the child's terminal. The scan must skip
        // them (transparent) instead of reporting the race in progress, and
        // rewind to the interleaved command so the sibling's own matcher runs.
        let timer_id = child_timer_id();
        let child_id = ExecutionId::new();
        let activity_id = ActivityExecId::new();
        let events = vec![
            child_started_event(child_id),
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "audit_log".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id,
                output: serde_json::json!({"processed": true}),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!("logged"),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_child_or_timer(
            "process_order",
            &serde_json::json!({"id": 42}),
            timer_id.as_str(),
            Some(300),
        );
        assert_eq!(
            result,
            ChildOrTimerMatch::ChildCompleted {
                output: serde_json::json!({"processed": true})
            }
        );
        // The interleaved sibling activity must still be matchable afterwards.
        let activity = matcher.match_activity("audit_log");
        assert_eq!(
            activity,
            HistoryMatch::Matched {
                output: serde_json::json!("logged")
            }
        );
    }

    #[test]
    fn child_or_timer_child_win_across_interleaved_signal_is_transparent() {
        // A signal recorded between the child/timer start pair and the child's
        // terminal must be STASHED (transparent to the race scan) — the #476
        // "ignored late signal" analog — so the race still resolves to
        // ChildCompleted and the signal remains observable by a later
        // signal-wait. It must NOT flip the winner or trip a divergence.
        let timer_id = child_timer_id();
        let child_id = ExecutionId::new();
        let events = vec![
            child_started_event(child_id),
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"ok": true}),
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id,
                output: serde_json::json!({"processed": true}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_child_or_timer(
            "process_order",
            &serde_json::json!({"id": 42}),
            timer_id.as_str(),
            Some(300),
        );
        assert_eq!(
            result,
            ChildOrTimerMatch::ChildCompleted {
                output: serde_json::json!({"processed": true})
            },
            "an interleaved signal must not flip the child-win outcome"
        );
        // The stashed signal is still deliverable to a later signal wait.
        assert_eq!(
            matcher.match_signal("approval"),
            HistoryMatch::Matched {
                output: serde_json::json!({"ok": true}),
            },
            "the interleaved signal must remain observable after the race resolves"
        );
        assert!(
            !matcher.has_non_lifecycle_unconsumed(),
            "no dangling unconsumed events after the race + signal are matched"
        );
    }

    #[test]
    fn child_or_timer_replays_same_branch_when_both_events_exist() {
        // Whichever resolution event was recorded first wins on every replay,
        // regardless of wall-clock timing (R4: deterministic by history index).
        let timer_id = child_timer_id();
        let child_id = ExecutionId::new();
        let events = vec![
            child_started_event(child_id),
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id,
                output: serde_json::json!({"processed": true}),
            },
            WorkflowEvent::TimerFired {
                timer_id: timer_id.clone(),
            },
        ];

        for _ in 0..3 {
            let mut matcher = HistoryMatcher::new(events.clone());
            let result = matcher.match_child_or_timer(
                "process_order",
                &serde_json::json!({"id": 42}),
                timer_id.as_str(),
                Some(300),
            );
            assert_eq!(
                result,
                ChildOrTimerMatch::ChildCompleted {
                    output: serde_json::json!({"processed": true})
                }
            );
        }
    }

    // ── ctx.race() marker matcher (issue #600) ──────────────────────────────

    #[test]
    fn match_u64_marker_no_match_on_empty_history() {
        let mut matcher = HistoryMatcher::new(vec![]);
        assert_eq!(matcher.match_u64_marker("race:1", 2), HistoryMatch::NoMatch);
    }

    #[test]
    fn match_u64_marker_matches_recorded_value() {
        let events = vec![WorkflowEvent::MarkerRecorded {
            name: "race:1".into(),
            details: serde_json::json!(2),
        }];
        let mut matcher = HistoryMatcher::new(events);
        assert_eq!(
            matcher.match_u64_marker("race:1", 2),
            HistoryMatch::Matched {
                output: serde_json::json!(2)
            }
        );
    }

    #[test]
    fn match_u64_marker_diverges_on_value_mismatch() {
        let events = vec![WorkflowEvent::MarkerRecorded {
            name: "race:1".into(),
            details: serde_json::json!(2),
        }];
        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_u64_marker("race:1", 3);
        assert!(matches!(result, HistoryMatch::Diverged { .. }));
    }

    #[test]
    fn match_u64_marker_diverges_on_name_mismatch() {
        let events = vec![WorkflowEvent::MarkerRecorded {
            name: "race:1".into(),
            details: serde_json::json!(2),
        }];
        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_u64_marker("race:2", 2);
        assert!(matches!(result, HistoryMatch::Diverged { .. }));
    }

    #[test]
    fn match_u64_marker_diverges_on_unexpected_event() {
        let events = vec![WorkflowEvent::TimerFired {
            timer_id: TimerId::new("t1"),
        }];
        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_u64_marker("race:1", 2);
        assert!(matches!(result, HistoryMatch::Diverged { .. }));
    }

    #[test]
    fn match_u64_marker_advances_cursor_past_marker() {
        let events = vec![
            WorkflowEvent::MarkerRecorded {
                name: "race:1".into(),
                details: serde_json::json!(2),
            },
            WorkflowEvent::TimerFired {
                timer_id: TimerId::new("t1"),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        assert_eq!(
            matcher.match_u64_marker("race:1", 2),
            HistoryMatch::Matched {
                output: serde_json::json!(2)
            }
        );
        // Cursor should now sit at the TimerFired event, ready for the next matcher.
        assert_eq!(matcher.cursor, 1);
    }

    // ── Worker session identity marker (issue #606) ─────────────────────────

    #[test]
    fn match_session_marker_no_match_on_empty_history() {
        let mut matcher = HistoryMatcher::new(vec![]);
        assert_eq!(matcher.match_session_marker(1), HistoryMatch::NoMatch);
    }

    #[test]
    fn match_session_marker_matches_recorded_uuid() {
        let session_uuid = uuid::Uuid::new_v4();
        let events = vec![WorkflowEvent::MarkerRecorded {
            name: "session:1".into(),
            details: serde_json::json!(session_uuid.to_string()),
        }];
        let mut matcher = HistoryMatcher::new(events);
        assert_eq!(
            matcher.match_session_marker(1),
            HistoryMatch::Matched {
                output: serde_json::json!(session_uuid.to_string())
            }
        );
    }

    #[test]
    fn match_session_marker_diverges_on_name_mismatch() {
        let events = vec![WorkflowEvent::MarkerRecorded {
            name: "session:2".into(),
            details: serde_json::json!(uuid::Uuid::new_v4().to_string()),
        }];
        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_session_marker(1);
        assert!(matches!(result, HistoryMatch::Diverged { .. }));
    }

    #[test]
    fn match_session_marker_diverges_on_unexpected_event() {
        let events = vec![WorkflowEvent::TimerFired {
            timer_id: TimerId::new("t1"),
        }];
        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_session_marker(1);
        assert!(matches!(result, HistoryMatch::Diverged { .. }));
    }

    #[test]
    fn match_session_marker_diverges_on_non_uuid_payload() {
        let events = vec![WorkflowEvent::MarkerRecorded {
            name: "session:1".into(),
            details: serde_json::json!("not-a-uuid"),
        }];
        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_session_marker(1);
        assert!(matches!(result, HistoryMatch::Diverged { .. }));
    }

    #[test]
    fn match_session_marker_advances_cursor_past_marker() {
        let session_uuid = uuid::Uuid::new_v4();
        let events = vec![
            WorkflowEvent::MarkerRecorded {
                name: "session:1".into(),
                details: serde_json::json!(session_uuid.to_string()),
            },
            WorkflowEvent::TimerFired {
                timer_id: TimerId::new("t1"),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        assert_eq!(
            matcher.match_session_marker(1),
            HistoryMatch::Matched {
                output: serde_json::json!(session_uuid.to_string())
            }
        );
        assert_eq!(matcher.cursor, 1);
    }

    #[test]
    fn match_session_marker_distinguishes_by_seq() {
        // Two distinct sessions opened in one workflow must never collide on
        // marker name, mirroring fan_out:{seq}/race:{seq} numbering.
        let uuid1 = uuid::Uuid::new_v4();
        let uuid2 = uuid::Uuid::new_v4();
        let events = vec![
            WorkflowEvent::MarkerRecorded {
                name: "session:1".into(),
                details: serde_json::json!(uuid1.to_string()),
            },
            WorkflowEvent::MarkerRecorded {
                name: "session:2".into(),
                details: serde_json::json!(uuid2.to_string()),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        assert_eq!(
            matcher.match_session_marker(1),
            HistoryMatch::Matched {
                output: serde_json::json!(uuid1.to_string())
            }
        );
        assert_eq!(
            matcher.match_session_marker(2),
            HistoryMatch::Matched {
                output: serde_json::json!(uuid2.to_string())
            }
        );
    }

    // ── Tolerant deadline clock read (issue #772 Finding 1) ─────────────────

    #[test]
    fn match_side_effect_now_tolerant_matches_recorded_probe_now() {
        let mut matcher = HistoryMatcher::new(vec![WorkflowEvent::SideEffectRecorded {
            kind: SideEffectKind::Now,
            name: Some(DEADLINE_PROBE_SIDE_EFFECT_NAME.to_string()),
            value: serde_json::json!(1_700_000_000_123_i64),
        }]);
        assert_eq!(
            matcher.match_side_effect_now_tolerant(),
            SideEffectNowMatch::Matched {
                millis: 1_700_000_000_123
            }
        );
        // Cursor consumed the event.
        assert!(!matcher.is_replaying());
    }

    /// Migration-path fix (issue #772): a user-side `system_now()` Now
    /// (`{ kind: Now, name: None }`) at the cursor is NOT the probe's read — the
    /// tolerant matcher must leave it untouched (`NotRecorded`) so the user's own
    /// strict `system_now()` read consumes it, rather than the probe stealing it.
    #[test]
    fn match_side_effect_now_tolerant_does_not_consume_a_user_now() {
        let mut matcher = HistoryMatcher::new(vec![WorkflowEvent::SideEffectRecorded {
            kind: SideEffectKind::Now,
            name: None,
            value: serde_json::json!(1_700_000_000_123_i64),
        }]);
        let before = matcher.position();
        assert_eq!(
            matcher.match_side_effect_now_tolerant(),
            SideEffectNowMatch::NotRecorded
        );
        // Cursor NOT advanced: the user's system_now() will match it next.
        assert_eq!(matcher.position(), before);
        assert!(matcher.is_replaying());
    }

    /// A `Now` recorded under a *different* name (neither the probe sentinel nor
    /// a bare user `Now`) is likewise not the probe's read: `NotRecorded`,
    /// cursor untouched.
    #[test]
    fn match_side_effect_now_tolerant_does_not_consume_a_differently_named_now() {
        let mut matcher = HistoryMatcher::new(vec![WorkflowEvent::SideEffectRecorded {
            kind: SideEffectKind::Now,
            name: Some("some_other_name".to_string()),
            value: serde_json::json!(1_700_000_000_123_i64),
        }]);
        let before = matcher.position();
        assert_eq!(
            matcher.match_side_effect_now_tolerant(),
            SideEffectNowMatch::NotRecorded
        );
        assert_eq!(matcher.position(), before);
    }

    #[test]
    fn match_side_effect_now_tolerant_not_recorded_for_non_now_event_leaves_cursor() {
        let mut matcher = HistoryMatcher::new(vec![WorkflowEvent::TimerStarted {
            timer_id: TimerId::new("t"),
            duration_secs: 60,
        }]);
        let before = matcher.position();
        assert_eq!(
            matcher.match_side_effect_now_tolerant(),
            SideEffectNowMatch::NotRecorded
        );
        // Cursor NOT advanced: the next real match sees the TimerStarted.
        assert_eq!(matcher.position(), before);
        assert!(matcher.is_replaying());
    }

    #[test]
    fn match_side_effect_now_tolerant_not_recorded_for_other_side_effect_kind() {
        let mut matcher = HistoryMatcher::new(vec![WorkflowEvent::SideEffectRecorded {
            kind: SideEffectKind::Uuid,
            name: None,
            value: serde_json::json!("some-uuid"),
        }]);
        let before = matcher.position();
        assert_eq!(
            matcher.match_side_effect_now_tolerant(),
            SideEffectNowMatch::NotRecorded
        );
        assert_eq!(matcher.position(), before);
    }

    #[test]
    fn match_side_effect_now_tolerant_frontier_past_end() {
        let mut matcher = HistoryMatcher::new(vec![]);
        assert_eq!(
            matcher.match_side_effect_now_tolerant(),
            SideEffectNowMatch::Frontier
        );
    }

    // ── match_external_await tests (issue #757) ───────────────────────────

    #[test]
    fn match_external_await_no_history_is_no_match() {
        let target = ExecutionId::new();
        // Empty history: the first live await returns NoMatch.
        let mut matcher = HistoryMatcher::new(vec![]);
        assert_eq!(matcher.match_external_await(target), HistoryMatch::NoMatch);
    }

    #[test]
    fn match_external_await_resolved_returns_matched_with_output() {
        let target = ExecutionId::new();
        let await_id = ExternalAwaitId::new();
        let output = serde_json::json!({"tracking": "abc123"});
        let mut matcher = HistoryMatcher::new(vec![
            WorkflowEvent::ExternalAwaitRequested { await_id, target },
            WorkflowEvent::ExternalAwaitResolved {
                await_id,
                output: output.clone(),
            },
        ]);
        assert_eq!(
            matcher.match_external_await(target),
            HistoryMatch::Matched { output }
        );
    }

    #[test]
    fn match_external_await_failed_returns_typed_fields() {
        let target = ExecutionId::new();
        let await_id = ExternalAwaitId::new();
        let mut matcher = HistoryMatcher::new(vec![
            WorkflowEvent::ExternalAwaitRequested { await_id, target },
            WorkflowEvent::ExternalAwaitFailed {
                await_id,
                reason_code: "target_failed".into(),
                message: Some("payment declined".into()),
                error_type: Some("PaymentDeclined".into()),
                details: Some(serde_json::json!({"code": 402})),
                non_retryable: Some(true),
            },
        ]);
        match matcher.match_external_await(target) {
            HistoryMatch::ExternalAwaitFailed {
                await_id: aid,
                reason_code,
                message,
                error_type,
                details,
                non_retryable,
            } => {
                assert_eq!(aid, await_id);
                assert_eq!(reason_code, "target_failed");
                assert_eq!(message.as_deref(), Some("payment declined"));
                assert_eq!(error_type.as_deref(), Some("PaymentDeclined"));
                assert_eq!(details.unwrap()["code"], 402);
                assert_eq!(non_retryable, Some(true));
            }
            other => panic!("expected ExternalAwaitFailed, got {other:?}"),
        }
    }

    #[test]
    fn match_external_await_requested_no_terminal_is_in_progress() {
        let target = ExecutionId::new();
        let await_id = ExternalAwaitId::new();
        let mut matcher = HistoryMatcher::new(vec![WorkflowEvent::ExternalAwaitRequested {
            await_id,
            target,
        }]);
        assert_eq!(
            matcher.match_external_await(target),
            HistoryMatch::ExternalAwaitInProgress { await_id }
        );
    }

    #[test]
    fn match_external_await_wrong_target_diverges() {
        let recorded_target = ExecutionId::new();
        let requested_target = ExecutionId::new();
        let await_id = ExternalAwaitId::new();
        let mut matcher = HistoryMatcher::new(vec![WorkflowEvent::ExternalAwaitRequested {
            await_id,
            target: recorded_target,
        }]);
        assert!(matches!(
            matcher.match_external_await(requested_target),
            HistoryMatch::Diverged { .. }
        ));
    }

    #[test]
    fn match_external_await_concurrent_with_activity_replays_both() {
        // A workflow that awaits an external target concurrently with an
        // activity: the await's Requested + Resolved events are interleaved
        // around the activity's Scheduled/Completed. The activity scan must
        // transparently stash the await triplet, and vice versa — neither
        // diverges. (Regression test for a missed stash triplet.)
        let target = ExecutionId::new();
        let await_id = ExternalAwaitId::new();
        let act_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::ExternalAwaitRequested { await_id, target },
            WorkflowEvent::ActivityScheduled {
                activity_id: act_id,
                name: "compute".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ExternalAwaitResolved {
                await_id,
                output: serde_json::json!({"ok": true}),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: act_id,
                output: serde_json::json!(7),
            },
        ];
        // Match the activity first (interleaved await events must be stashed,
        // not break the scan).
        let mut matcher = HistoryMatcher::new(events.clone());
        assert_eq!(
            matcher.match_activity("compute"),
            HistoryMatch::Matched {
                output: serde_json::json!(7)
            }
        );
        // Then the await resolves from the stash.
        assert_eq!(
            matcher.match_external_await(target),
            HistoryMatch::Matched {
                output: serde_json::json!({"ok": true})
            }
        );

        // Reverse order: polling the await future first (before the concurrent
        // activity future has scanned forward past the interleaved
        // `ActivityScheduled`) yields `ExternalAwaitInProgress` — never a
        // divergence — exactly like the cancel primitive. In a real
        // `tokio::join!` re-drive the activity future settles the await's
        // terminal on a later fresh cycle; full end-to-end resolution across
        // randomized re-drives is covered by the `WorkflowReplayer` fixtures.
        let mut matcher2 = HistoryMatcher::new(events);
        assert!(matches!(
            matcher2.match_external_await(target),
            HistoryMatch::ExternalAwaitInProgress { .. }
        ));
    }

    // ── Issue #1338: characterization tests for the 10 sites consolidated onto
    // `stash_transparent_external_event`. Before this suite, `ExternalCancel`
    // triplet transparency (issue #492) had zero direct test coverage at any
    // of the 10 sites, and `ExternalAwait` triplet transparency (issue #757)
    // was covered only at `scan_activity_terminal` (the test above). These
    // pin per-site behavior so a future event-family addition that misses a
    // site is caught here instead of only in production (the exact failure
    // class issue #1338 documents from `33595ff5`). ──────────────────────

    /// Builds a fresh `ExternalCancelRequested` + `ExternalCancelDelivered`
    /// pair for the characterization tests below, returning the id/target a
    /// test needs for its own follow-up `match_external_cancel` assertion
    /// alongside the two events to splice into that test's own history.
    fn external_cancel_triplet() -> (
        ExternalCancelId,
        crate::types::ExternalTarget,
        WorkflowEvent,
        WorkflowEvent,
    ) {
        let cancel_id = ExternalCancelId::new();
        let target = crate::types::ExternalTarget::ExecutionId(ExecutionId::new());
        let requested = WorkflowEvent::ExternalCancelRequested {
            cancel_id,
            target: target.clone(),
        };
        let delivered = WorkflowEvent::ExternalCancelDelivered { cancel_id };
        (cancel_id, target, requested, delivered)
    }

    /// Builds a fresh `ExternalAwaitRequested` + `ExternalAwaitResolved` pair,
    /// mirroring `external_cancel_triplet` for issue #757's event family.
    fn external_await_triplet() -> (ExternalAwaitId, ExecutionId, WorkflowEvent, WorkflowEvent) {
        let await_id = ExternalAwaitId::new();
        let target = ExecutionId::new();
        let requested = WorkflowEvent::ExternalAwaitRequested { await_id, target };
        let resolved = WorkflowEvent::ExternalAwaitResolved {
            await_id,
            output: serde_json::json!({"done": true}),
        };
        (await_id, target, requested, resolved)
    }

    #[test]
    fn scan_activity_terminal_stashes_interleaved_external_cancel_triplet() {
        let activity_id = ActivityExecId::new();
        let (_, target, requested, delivered) = external_cancel_triplet();
        let events = vec![
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "compute".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            requested,
            delivered,
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!(7),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        assert_eq!(
            matcher.match_activity("compute"),
            HistoryMatch::Matched {
                output: serde_json::json!(7)
            }
        );
        assert_eq!(
            matcher.match_external_cancel(&target),
            HistoryMatch::Matched {
                output: Value::Null
            }
        );
    }

    #[test]
    fn scan_local_activity_terminal_stashes_interleaved_external_cancel_triplet() {
        let activity_id = ActivityExecId::new();
        let (_, target, requested, delivered) = external_cancel_triplet();
        let output = serde_json::json!({"ok": true});
        let events = vec![
            WorkflowEvent::LocalActivityScheduled {
                activity_id,
                name: "format_data".into(),
                input: Value::Null,
                retry_policy: None,
                resolved: false,
                start_to_close_nanos: None,
            },
            requested,
            delivered,
            WorkflowEvent::LocalActivityCompleted {
                activity_id,
                output: output.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        assert_eq!(
            matcher.match_local_activity("format_data"),
            HistoryMatch::Matched { output }
        );
        assert_eq!(
            matcher.match_external_cancel(&target),
            HistoryMatch::Matched {
                output: Value::Null
            }
        );
    }

    #[test]
    fn scan_local_activity_terminal_stashes_interleaved_external_await_triplet() {
        let activity_id = ActivityExecId::new();
        let (_, target, requested, resolved) = external_await_triplet();
        let output = serde_json::json!({"ok": true});
        let events = vec![
            WorkflowEvent::LocalActivityScheduled {
                activity_id,
                name: "format_data".into(),
                input: Value::Null,
                retry_policy: None,
                resolved: false,
                start_to_close_nanos: None,
            },
            requested,
            resolved,
            WorkflowEvent::LocalActivityCompleted {
                activity_id,
                output: output.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        assert_eq!(
            matcher.match_local_activity("format_data"),
            HistoryMatch::Matched { output }
        );
        assert_eq!(
            matcher.match_external_await(target),
            HistoryMatch::Matched {
                output: serde_json::json!({"done": true})
            }
        );
    }

    #[test]
    fn drain_early_signals_stashes_interleaved_external_cancel_triplet() {
        // Direct unit test of `drain_early_signals` (Site 3, issue #1338) —
        // this call site mutates `self.cursor` in place (rather than a local
        // scan_cursor) and does an extra `advance_to_next_unconsumed_event()`
        // per arm, so it is pinned directly rather than only through a public
        // wrapper that might resolve the pair via a different site first.
        let (cancel_id, _, requested, delivered) = external_cancel_triplet();
        let events = vec![requested, delivered];
        let mut matcher = HistoryMatcher::new(events);
        matcher.drain_early_signals();
        assert_eq!(matcher.cursor, 2);
        assert_eq!(matcher.pending_external_cancels.len(), 1);
        assert_eq!(matcher.pending_external_cancels[0].cancel_id, cancel_id);
        assert!(matches!(
            matcher.pending_external_cancels[0].terminal,
            Some(StashedCancelTerminal::Delivered)
        ));
    }

    #[test]
    fn drain_early_signals_stashes_interleaved_external_await_triplet() {
        // Direct unit test of `drain_early_signals` (Site 3, issue #1338) for
        // the `ExternalAwait` family (issue #757), mirroring the cancel test
        // above.
        let (await_id, _, requested, resolved) = external_await_triplet();
        let events = vec![requested, resolved];
        let mut matcher = HistoryMatcher::new(events);
        matcher.drain_early_signals();
        assert_eq!(matcher.cursor, 2);
        assert_eq!(matcher.pending_external_awaits.len(), 1);
        assert_eq!(matcher.pending_external_awaits[0].await_id, await_id);
        assert!(matches!(
            matcher.pending_external_awaits[0].terminal,
            Some(StashedAwaitTerminal::Resolved(_))
        ));
    }

    #[test]
    fn match_external_activity_stashes_interleaved_external_cancel_triplet() {
        let activity_id = ActivityExecId::new();
        let token = ExternalActivityToken::new();
        let (_, target, requested, delivered) = external_cancel_triplet();
        let output = serde_json::json!({"accepted": true});
        let events = vec![
            WorkflowEvent::ActivityAwaitingExternal {
                activity_id,
                token,
                name: "ship_order".into(),
                input: Value::Null,
                queue: "fulfillment".into(),
                schedule_to_close_secs: 60,
            },
            requested,
            delivered,
            WorkflowEvent::ActivityCompletedExternally {
                activity_id,
                token,
                output: output.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        assert_eq!(
            matcher.match_external_activity("ship_order"),
            HistoryMatch::Matched { output }
        );
        assert_eq!(
            matcher.match_external_cancel(&target),
            HistoryMatch::Matched {
                output: Value::Null
            }
        );
    }

    #[test]
    fn match_external_activity_stashes_interleaved_external_await_triplet() {
        let activity_id = ActivityExecId::new();
        let token = ExternalActivityToken::new();
        let (_, target, requested, resolved) = external_await_triplet();
        let output = serde_json::json!({"accepted": true});
        let events = vec![
            WorkflowEvent::ActivityAwaitingExternal {
                activity_id,
                token,
                name: "ship_order".into(),
                input: Value::Null,
                queue: "fulfillment".into(),
                schedule_to_close_secs: 60,
            },
            requested,
            resolved,
            WorkflowEvent::ActivityCompletedExternally {
                activity_id,
                token,
                output: output.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        assert_eq!(
            matcher.match_external_activity("ship_order"),
            HistoryMatch::Matched { output }
        );
        assert_eq!(
            matcher.match_external_await(target),
            HistoryMatch::Matched {
                output: serde_json::json!({"done": true})
            }
        );
    }

    #[test]
    fn match_external_signal_stashes_interleaved_external_cancel_triplet() {
        let signal_id = ExternalSignalId::new();
        let signal_target = ExecutionId::new();
        let (_, cancel_target, requested, delivered) = external_cancel_triplet();
        let events = vec![
            WorkflowEvent::ExternalSignalRequested {
                signal_id,
                target: crate::types::ExternalTarget::ExecutionId(signal_target),
                signal_name: "poke".into(),
                payload: serde_json::json!({"n": 1}),
                idempotency_key: None,
            },
            requested,
            delivered,
            WorkflowEvent::ExternalSignalDelivered { signal_id },
        ];
        let mut matcher = HistoryMatcher::new(events);
        assert_eq!(
            matcher.match_external_signal(
                &crate::types::ExternalTarget::ExecutionId(signal_target),
                "poke"
            ),
            HistoryMatch::Matched {
                output: Value::Null
            }
        );
        assert_eq!(
            matcher.match_external_cancel(&cancel_target),
            HistoryMatch::Matched {
                output: Value::Null
            }
        );
    }

    #[test]
    fn match_external_signal_stashes_interleaved_external_await_triplet() {
        let signal_id = ExternalSignalId::new();
        let signal_target = ExecutionId::new();
        let (_, await_target, requested, resolved) = external_await_triplet();
        let events = vec![
            WorkflowEvent::ExternalSignalRequested {
                signal_id,
                target: crate::types::ExternalTarget::ExecutionId(signal_target),
                signal_name: "poke".into(),
                payload: serde_json::json!({"n": 1}),
                idempotency_key: None,
            },
            requested,
            resolved,
            WorkflowEvent::ExternalSignalDelivered { signal_id },
        ];
        let mut matcher = HistoryMatcher::new(events);
        assert_eq!(
            matcher.match_external_signal(
                &crate::types::ExternalTarget::ExecutionId(signal_target),
                "poke"
            ),
            HistoryMatch::Matched {
                output: Value::Null
            }
        );
        assert_eq!(
            matcher.match_external_await(await_target),
            HistoryMatch::Matched {
                output: serde_json::json!({"done": true})
            }
        );
    }

    #[test]
    fn match_timer_strict_stashes_interleaved_external_cancel_triplet() {
        let timer_id = TimerId::new("cooldown");
        let (_, target, requested, delivered) = external_cancel_triplet();
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 30,
            },
            requested,
            delivered,
            WorkflowEvent::TimerFired { timer_id },
        ];
        let mut matcher = HistoryMatcher::new(events);
        assert_eq!(
            matcher.match_timer("cooldown"),
            HistoryMatch::Matched {
                output: Value::Null
            }
        );
        assert_eq!(
            matcher.match_external_cancel(&target),
            HistoryMatch::Matched {
                output: Value::Null
            }
        );
    }

    #[test]
    fn match_timer_strict_stashes_interleaved_external_await_triplet() {
        let timer_id = TimerId::new("cooldown");
        let (_, target, requested, resolved) = external_await_triplet();
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 30,
            },
            requested,
            resolved,
            WorkflowEvent::TimerFired { timer_id },
        ];
        let mut matcher = HistoryMatcher::new(events);
        assert_eq!(
            matcher.match_timer("cooldown"),
            HistoryMatch::Matched {
                output: Value::Null
            }
        );
        assert_eq!(
            matcher.match_external_await(target),
            HistoryMatch::Matched {
                output: serde_json::json!({"done": true})
            }
        );
    }

    #[test]
    fn timer_scan_cross_or_stop_stashes_interleaved_external_cancel_triplet() {
        let timer_id = TimerId::new("idle");
        let (_, target, requested, delivered) = external_cancel_triplet();
        let events = vec![
            requested,
            delivered,
            WorkflowEvent::TimerCancelled { timer_id },
        ];
        let mut matcher = HistoryMatcher::new(events);
        assert_eq!(
            matcher.match_timer_cancel("idle"),
            HistoryMatch::Matched {
                output: Value::Null
            }
        );
        assert_eq!(
            matcher.match_external_cancel(&target),
            HistoryMatch::Matched {
                output: Value::Null
            }
        );
    }

    #[test]
    fn timer_scan_cross_or_stop_stashes_interleaved_external_await_triplet() {
        let timer_id = TimerId::new("idle");
        let (_, target, requested, resolved) = external_await_triplet();
        let events = vec![
            requested,
            resolved,
            WorkflowEvent::TimerCancelled { timer_id },
        ];
        let mut matcher = HistoryMatcher::new(events);
        assert_eq!(
            matcher.match_timer_cancel("idle"),
            HistoryMatch::Matched {
                output: Value::Null
            }
        );
        assert_eq!(
            matcher.match_external_await(target),
            HistoryMatch::Matched {
                output: serde_json::json!({"done": true})
            }
        );
    }

    #[test]
    fn match_signal_inner_stashes_interleaved_external_cancel_triplet() {
        let timer_id = TimerId::new("distractor");
        let (_, target, requested, delivered) = external_cancel_triplet();
        let events = vec![
            // A non-transparent event first, so drain_early_signals halts here
            // and match_signal_inner's own scan is what crosses the pair below.
            WorkflowEvent::TimerCancelled { timer_id },
            requested,
            delivered,
            WorkflowEvent::SignalReceived {
                signal_name: "approved".into(),
                payload: serde_json::json!({"ok": true}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        assert_eq!(
            matcher.match_signal("approved"),
            HistoryMatch::Matched {
                output: serde_json::json!({"ok": true})
            }
        );
        assert_eq!(
            matcher.match_external_cancel(&target),
            HistoryMatch::Matched {
                output: Value::Null
            }
        );
    }

    #[test]
    fn match_signal_inner_stashes_interleaved_external_await_triplet() {
        let timer_id = TimerId::new("distractor");
        let (_, target, requested, resolved) = external_await_triplet();
        let events = vec![
            WorkflowEvent::TimerCancelled { timer_id },
            requested,
            resolved,
            WorkflowEvent::SignalReceived {
                signal_name: "approved".into(),
                payload: serde_json::json!({"ok": true}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        assert_eq!(
            matcher.match_signal("approved"),
            HistoryMatch::Matched {
                output: serde_json::json!({"ok": true})
            }
        );
        assert_eq!(
            matcher.match_external_await(target),
            HistoryMatch::Matched {
                output: serde_json::json!({"done": true})
            }
        );
    }

    #[test]
    fn signal_or_timer_stashes_interleaved_external_cancel_triplet() {
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let (_, target, requested, delivered) = external_cancel_triplet();
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            requested,
            delivered,
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"approved": true}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        assert_eq!(
            matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(300)),
            SignalOrTimerMatch::SignalWon {
                payload: serde_json::json!({"approved": true})
            }
        );
        assert_eq!(
            matcher.match_external_cancel(&target),
            HistoryMatch::Matched {
                output: Value::Null
            }
        );
    }

    #[test]
    fn signal_or_timer_stashes_interleaved_external_await_triplet() {
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let (_, target, requested, resolved) = external_await_triplet();
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            requested,
            resolved,
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"approved": true}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        assert_eq!(
            matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(300)),
            SignalOrTimerMatch::SignalWon {
                payload: serde_json::json!({"approved": true})
            }
        );
        assert_eq!(
            matcher.match_external_await(target),
            HistoryMatch::Matched {
                output: serde_json::json!({"done": true})
            }
        );
    }

    #[test]
    fn child_or_timer_stashes_interleaved_external_cancel_triplet() {
        let timer_id = child_timer_id();
        let child_id = ExecutionId::new();
        let (_, target, requested, delivered) = external_cancel_triplet();
        let events = vec![
            child_started_event(child_id),
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            requested,
            delivered,
            WorkflowEvent::ChildWorkflowCompleted {
                child_id,
                output: serde_json::json!({"processed": true}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_child_or_timer(
            "process_order",
            &serde_json::json!({"id": 42}),
            timer_id.as_str(),
            Some(300),
        );
        assert_eq!(
            result,
            ChildOrTimerMatch::ChildCompleted {
                output: serde_json::json!({"processed": true})
            }
        );
        assert_eq!(
            matcher.match_external_cancel(&target),
            HistoryMatch::Matched {
                output: Value::Null
            }
        );
    }

    #[test]
    fn child_or_timer_stashes_interleaved_external_await_triplet() {
        let timer_id = child_timer_id();
        let child_id = ExecutionId::new();
        let (_, target, requested, resolved) = external_await_triplet();
        let events = vec![
            child_started_event(child_id),
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            requested,
            resolved,
            WorkflowEvent::ChildWorkflowCompleted {
                child_id,
                output: serde_json::json!({"processed": true}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_child_or_timer(
            "process_order",
            &serde_json::json!({"id": 42}),
            timer_id.as_str(),
            Some(300),
        );
        assert_eq!(
            result,
            ChildOrTimerMatch::ChildCompleted {
                output: serde_json::json!({"processed": true})
            }
        );
        assert_eq!(
            matcher.match_external_await(target),
            HistoryMatch::Matched {
                output: serde_json::json!({"done": true})
            }
        );
    }
}
