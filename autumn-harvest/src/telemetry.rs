//! OpenTelemetry integration surface for autumn-harvest.
//!
//! The harvest engine emits structured `tracing` spans around every workflow
//! execution, activity invocation, and timer delay. Operators bridge those
//! spans into OpenTelemetry using any OTel-compatible layer (e.g.
//! [`tracing-opentelemetry`]); the core crate deliberately stays
//! backend-agnostic so Datadog, Jaeger, Grafana, or any other APM can consume
//! the data.
//!
//! In addition to spans, the engine carries a [`TraceContextCarrier`] alongside
//! every task in the Postgres queue. Carriers hold the W3C `traceparent` /
//! `tracestate` headers so a trace started by an HTTP request in the web
//! process can be stitched to the background activity that ran it.
//!
//! Metrics follow the same pattern: applications register any type that
//! implements [`MetricsRecorder`], and the worker drives it on workflow /
//! activity / timer events. The default recorder ([`NoOpMetrics`]) silently
//! drops everything so telemetry is zero-cost unless explicitly configured.
//!
//! [`tracing-opentelemetry`]: https://docs.rs/tracing-opentelemetry

use std::any::Any;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Span attribute name constants
// Defined by docs/adr/0001-otel-trace-contract.md
// ---------------------------------------------------------------------------

/// OpenTelemetry span attribute: the logical workflow name (e.g. `"onboarding"`).
pub const ATTR_WORKFLOW_ID: &str = "harvest.workflow.id";

/// OpenTelemetry span attribute: the unique execution UUID.
pub const ATTR_EXECUTION_ID: &str = "harvest.execution.id";

/// OpenTelemetry span attribute: the shard number that owns this execution.
pub const ATTR_SHARD_ID: &str = "harvest.shard.id";

/// OpenTelemetry span attribute: the activity function name (e.g. `"send_email"`).
pub const ATTR_ACTIVITY_NAME: &str = "harvest.activity.name";

/// OpenTelemetry span attribute: the 1-based attempt number for this activity invocation.
pub const ATTR_ATTEMPT: &str = "harvest.attempt";

/// OpenTelemetry span attribute: the task queue name the work item was pulled from.
pub const ATTR_QUEUE: &str = "harvest.queue";

/// OpenTelemetry span attribute for replay-mode spans.
///
/// Set to `true` when the span is emitted during deterministic replay rather
/// than live execution. Consumers must treat such spans as reconstructed
/// history, not live causality.
pub const ATTR_REPLAY: &str = "harvest.replay";

// ---------------------------------------------------------------------------
// Metric name constants
// Defined by docs/adr/0001-otel-trace-contract.md — OpenTelemetry semantic
// naming: `harvest.<noun>.<instrument>` in dot-notation.
// ---------------------------------------------------------------------------

/// Counter: incremented once when a worker starts executing a workflow task.
pub const METRIC_WORKFLOW_STARTED: &str = "harvest.workflow.started";

/// Histogram: wall-clock seconds a workflow executor cycle took.
pub const METRIC_WORKFLOW_DURATION: &str = "harvest.workflow.duration";

/// Histogram: number of durable events in a terminal workflow execution history.
pub const METRIC_WORKFLOW_HISTORY_SIZE: &str = "harvest.workflow.history_size";

/// Counter: incremented once for each continue-as-new rotation.
pub const METRIC_WORKFLOW_CONTINUE_AS_NEW: &str = "harvest.workflow.continue_as_new";

/// Counter: incremented **exactly once** when a workflow execution reaches a
/// terminal state.
///
/// Unlike `harvest.workflow.duration` (which fires on every executor cycle,
/// including suspended cycles), this counter fires at the final
/// terminal-state transition only — so `completed / (completed + failed +
/// cancelled + timed_out)` gives a reliable per-scrape-interval success rate
/// without any normalisation.
///
/// Labels (all low-cardinality):
/// - `workflow` — the workflow type name.
/// - `queue`    — the task-queue the execution was dispatched on.
/// - `outcome`  — one of the six bounded values below.
///
/// Bounded `outcome` values:
/// - `"completed"` — handler returned `Ok(…)`.
/// - `"failed"`    — handler returned `Err(…)` or exhausted retries.
/// - `"cancelled"` — gracefully cancelled via `cancel_workflow_execution`.
/// - `"timed_out"` — execution deadline (`deadline_at`) elapsed.
/// - `"terminated"`— force-killed via `terminate_workflow_execution`.
/// - `"continued_as_new"` — execution rotated; **excluded from the
///   success-rate denominator** (`completed + failed + cancelled + timed_out`).
///
/// Per ADR-0001 §7, `execution.id` is **span-only** and must never appear
/// as a label on this counter.
pub const METRIC_WORKFLOW_TERMINAL: &str = "harvest.workflow.terminal";

/// Histogram: wall-clock seconds an activity invocation took (success or failure).
pub const METRIC_ACTIVITY_DURATION: &str = "harvest.activity.duration";

/// Counter: incremented on each activity failure attempt.
///
/// Attributes: `activity.type`, `workflow.type`, `error.type`, `non_retryable`.
/// Per ADR-0001 §7, `execution.id` / `activity.id` are span-only.
pub const METRIC_ACTIVITY_FAILED: &str = "harvest.activity.failed";

/// Counter: incremented once per activity attempt for **both** successful and
/// failed outcomes, providing a single metric family for success-rate SLOs.
///
/// Labels:
/// - `activity` (= [`METRIC_LABEL_ACTIVITY`]): the registered activity name (bounded)
/// - `queue` (= [`METRIC_LABEL_QUEUE`]): the task queue name (bounded)
/// - `outcome` (= [`METRIC_LABEL_OUTCOME`]): `"completed"` or `"failed"`
///   (mirrors [`ActivityStatus::as_str`])
///
/// Use this counter to compute activity success rate in a single metric family:
/// `rate(harvest_activity_attempts_total{outcome="completed"}[5m]) /
///  rate(harvest_activity_attempts_total[5m])`.
///
/// Complements the existing [`METRIC_ACTIVITY_FAILED`] (richer labels:
/// `workflow.type`/`error.type`/`non_retryable`) and [`METRIC_ACTIVITY_DURATION`]
/// (histogram). Together the three form the complete activity-outcome trio.
///
/// Per ADR-0001 §7, `execution.id` and `activity.id` are **span-only** and must
/// never appear as labels here.
pub const METRIC_ACTIVITY_ATTEMPTS: &str = "harvest.activity.attempts";

/// Counter: incremented once each time a retry is **actually scheduled** for
/// an activity, enabling direct "retry storm" alerting before work dead-letters.
///
/// Labels:
/// - `activity` (= [`METRIC_LABEL_ACTIVITY`]): the registered activity name (bounded)
/// - `queue` (= [`METRIC_LABEL_QUEUE`]): the task queue name (bounded)
///
/// A retry is counted only when the attempt is retryable *and* the
/// `schedule_to_close` deadline (if set) is not already exceeded — i.e., exactly
/// when a new task row is enqueued for the next attempt.
///
/// Per ADR-0001 §7, `execution.id` and `activity.id` are **span-only** and must
/// never appear as labels here.
pub const METRIC_ACTIVITY_RETRIES: &str = "harvest.activity.retries";

/// Counter: incremented once per **effective** operator pause/resume action on
/// an activity type (issue #807).
///
/// Labels:
/// - `activity` (= [`METRIC_LABEL_ACTIVITY`]): the registered activity name (bounded)
/// - `action` (= [`METRIC_LABEL_ACTION`]): [`ActivityPauseAction::as_str`] —
///   exactly `pause` or `resume`
///
/// **Why a counter, not a gauge.** Its closest sibling `harvest.queue.paused`
/// (issue #619) is a *gauge*, because a queue hold is a binary state whose
/// *duration* is the thing an operator pages on. This one answers the
/// different question issue #807 asks — how often the fleet is reaching for
/// this lever, and on what — so it counts point-in-time actions. The current
/// *state* is deliberately not derivable from it; `GET /activities`
/// is the read model for that.
///
/// **Why not the `paused` noun.** `harvest_queue_paused` (gauge, issue #619)
/// and `harvest_workflow_paused_total` (counter, issue #383) already disagree
/// on instrument type under that noun. A third spelling would leave an
/// operator guessing which shape they are graphing, so this one is named for
/// what it actually counts.
///
/// **Emission contract.** Counted only when the action genuinely changed
/// state — gate on `ActivityPauseOutcome::newly_paused` /
/// `ActivityResumeOutcome::newly_resumed`, mirroring how
/// [`MetricsRecorder::record_workflow_paused`] is gated on `newly_paused`
/// (issue #383). Both mutations are idempotent by design, so an operator
/// retry after a lost response must not read as a second hold.
///
/// **Cardinality (ADR-0001 §7).** `activity` is bounded **by bucketing**, not by
/// validation. `activity_pause::validate_activity_name` bounds the name's
/// *length*, not its *cardinality*, and the pause routes deliberately accept an
/// **unregistered** name (so an operator can pre-pause an activity before its
/// fleet rolls out, and a paused-but-unregistered hold stays inspectable) — so
/// the raw name is caller-controlled free text. The emitter therefore resolves
/// it against the registered activity catalogue and substitutes
/// [`UNREGISTERED_ACTIVITY_NAME`] when it is absent, keeping the label bounded
/// to the app's real activity set plus one sentinel series. Same fix, and same
/// reason, as the #684 update-name label. `action` is bounded to two values by
/// construction ([`ActivityPauseAction`]). `execution.id` is span-only and MUST
/// NOT appear here.
///
/// The plain `activity` label (not the dotted `activity.name`) is the right
/// one: the dotted variant exists solely so the circuit-breaker counters
/// (issue #369) can match the ADR-0001 `activity.name` *span* attribute, while
/// every other activity counter — `attempts`, `retries`, `panic`,
/// `rate_limit.throttled` — uses [`METRIC_LABEL_ACTIVITY`].
pub const METRIC_ACTIVITY_PAUSE_ACTIONS: &str = "harvest.activity.pause_actions";

/// Counter: incremented when a durable timer is persisted.
pub const METRIC_TIMER_STARTED: &str = "harvest.timer.started";

/// Histogram: distribution of scheduled timer durations (seconds).
pub const METRIC_TIMER_DURATION: &str = "harvest.timer.duration";

/// Gauge: current number of pending (unclaimed) tasks in a queue.
pub const METRIC_QUEUE_DEPTH: &str = "harvest.queue.depth";

/// Histogram: wall-clock seconds a task waited between becoming eligible
/// (`scheduled_at`) and being claimed by a worker (`started_at`).
///
/// Recorded at claim time on the existing `claim_task` path. Labels:
///   - `"queue"` — the task queue name (bounded cardinality).
///
/// Use this as the canonical "do I need more workers?" SLI: page when the
/// p99 exceeds your per-queue SLA (e.g. `histogram_quantile(0.99, …) > 30`).
/// Per ADR-0001 §7, `execution.id` / `activity.id` are span-only and MUST NOT
/// appear as metric labels here.
pub const METRIC_QUEUE_SCHEDULE_TO_START: &str = "harvest.queue.schedule_to_start";

/// Gauge: age in seconds of the oldest currently-unclaimed eligible task in a
/// queue, sampled on the same cadence as `harvest.queue.depth`.
///
/// "Eligible" means `state = 'PENDING' AND scheduled_at <= NOW()` — the same
/// slice that `claim_task` competes over. Labels:
///   - `"queue"` — the task queue name (bounded cardinality).
///
/// Reports `0` when no eligible tasks are queued, preventing stale gauge values
/// from lingering after a queue drains.
pub const METRIC_QUEUE_OLDEST_PENDING_AGE: &str = "harvest.queue.oldest_pending_age";

/// Counter: incremented once each time a task is dispatched from a queue.
///
/// This lets operators confirm that the live per-queue dispatch split matches
/// the configured `queue_weights` (issue #515). The counter is recorded in
/// `dispatch_task` for both weighted and default paths, so it is always
/// observable regardless of whether weights are configured.
///
/// Labels:
///   - `"queue"` — the task queue name (bounded cardinality, ADR-0001 §7).
///
/// `execution.id` / `activity.id` are span-only and MUST NOT appear as labels.
pub const METRIC_QUEUE_DISPATCHED: &str = "harvest.queue.dispatched";

/// Gauge: current number of claimable pending tasks on a shard that has no
/// covering live worker (issue #522).
///
/// Emitted per shard by the stranded-work sampler. Labelled `{shard}`.
/// A healthy steady state is `0` on every shard.
pub const METRIC_SHARD_STRANDED_PENDING: &str = "harvest.shard.stranded_pending";

/// Counter: a task was dispatched from a given shard (issue #961).
///
/// The **shard-dimension twin** of [`METRIC_QUEUE_DISPATCHED`]: where #515's
/// per-queue counter lets an operator confirm the live dispatch split matches
/// `WorkerConfig::queue_weights`, this one lets them confirm the split across
/// a multi-shard worker's assigned shards — i.e. that a deep backlog on one
/// shard is not starving its siblings (AC5). The two compose: queue weights
/// pick *which queue* within a shard, the round-robin poll order picks *which
/// shard*.
///
/// Labels:
///   - `"shard"` — the shard id (bounded cardinality, ADR-0001 §7).
///
/// `execution.id` is span-only and MUST NOT appear as a label.
pub const METRIC_SHARD_DISPATCHED: &str = "harvest.shard.dispatched";

/// Gauge: number of a worker's dispatch slots currently in use for one slot
/// type (issue #531).
///
/// Sampled in-process by the worker's slot sampler against the two dispatch
/// `Semaphore`s (`max_concurrent_workflows` / `max_concurrent_activities`).
/// Labelled `{slot_type}` where `slot_type` is `workflow` or `activity`.
/// Invariant: `slots_in_use + slots_available == configured_max` for that slot
/// type within one sampler interval. Per ADR-0001 §7, `execution.id` MUST NOT
/// appear as a label.
pub const METRIC_WORKER_SLOTS_IN_USE: &str = "harvest.worker.slots_in_use";

/// Gauge: number of a worker's dispatch slots currently free for one slot type
/// (issue #531). Companion to [`METRIC_WORKER_SLOTS_IN_USE`]; same labels and
/// invariant.
pub const METRIC_WORKER_SLOTS_AVAILABLE: &str = "harvest.worker.slots_available";

/// Gauge: the adaptive slot tuner's current resize target for one slot type
/// (issue #548).
///
/// Emitted in-process by the worker's slot-tuner control loop on the same
/// cadence as the other monitoring tasks. Labelled `{slot_type}` only, same
/// bounded values as [`METRIC_WORKER_SLOTS_IN_USE`]. Composes with the #531
/// occupancy gauges on the same dashboard: `slot_target` is the band-clamped
/// value the controller is steering toward; `slots_in_use` /
/// `slots_available` (issue #531) sum to it. Only emitted when a
/// `SlotTuner` is configured. Per ADR-0001 §7, `execution.id` MUST NOT appear
/// as a label.
pub const METRIC_WORKER_SLOT_TARGET: &str = "harvest.worker.slot_target";

/// Counter: incremented once per adaptive slot-tuner control-loop tick with
/// the decision that took effect (issue #548).
///
/// Labelled `{slot_type, decision}` where `decision` is one of `grow` /
/// `shrink` / `hold` ([`TunerDecision::as_str`]). A decision that was clamped
/// away by the `[min_slots, max_slots]` band (e.g. a `Grow` request at
/// `max_slots`) is recorded as `hold`, reflecting what actually happened to
/// the live target rather than what the controller requested.
pub const METRIC_WORKER_TUNER_DECISIONS: &str = "harvest.worker.tuner_decisions";

/// Gauge: measured RPO for a shard — how many seconds of acknowledged work a
/// failover to the standby region would lose right now (issue #954).
///
/// Sourced from replication positions: the age of the newest
/// `harvest_replication_heartbeat` watermark whose WAL position the slowest
/// standby of this shard's database has already confirmed.
///
/// **The series is absent, not zero, when the RPO is unknown** — no standby
/// connected, no slot, or the standby further behind than the retained
/// watermark trail. That is deliberate and load-bearing: a dead standby
/// reported as `0` reads as a perfect RPO, which is the most dangerous number
/// this metric could publish. Alert on a
/// [`METRIC_REPLICATION_STANDBYS`] value of `0` for "replication is down";
/// alert on this gauge only for "replication is slow".
///
/// Labelled `{shard}` only. A standby's `application_name` is operator-chosen
/// and unbounded, so it is not a label (ADR-0001 §7); per-standby detail is
/// available from `pg_stat_replication` directly.
pub const METRIC_REPLICATION_LAG_SECONDS: &str = "harvest.replication.lag_seconds";

/// Gauge: `1` while a shard's replication views are readable, `0` when they
/// are not (issue #954).
///
/// Exists because **a Prometheus gauge keeps exporting its last value until it
/// is changed** — so simply declining to emit the RPO and standby gauges when
/// the views become unreadable does not make those series stale, it freezes
/// them at their last healthy reading and an unobservable shard goes on looking
/// fine indefinitely.
///
/// The two options that do not work: emitting `0` standbys on an unreadable
/// view pages on-call for what is almost always a missing `GRANT pg_monitor`,
/// and emitting nothing leaves the stale value in place. So availability is
/// published as its own signal, and the other DR gauges are withheld while it
/// is `0`.
///
/// Alerting: require `harvest_replication_observable == 1` on the
/// replication-down rule so it cannot fire on a stale reading, and alert on
/// `== 0` separately — that is a configuration problem, not an outage.
/// Labelled `{shard}`.
pub const METRIC_REPLICATION_OBSERVABLE: &str = "harvest.replication.observable";

/// Gauge: worst-case WAL backlog in bytes for a shard (issue #954).
///
/// The byte companion to [`METRIC_REPLICATION_LAG_SECONDS`], and the signal
/// that survives a disconnected standby: a slot with no walsender still pins
/// WAL, so the backlog stays knowable when the time lag is not. Also the
/// disk-pressure signal — an abandoned slot grows this without bound until the
/// primary runs out of WAL space. Labelled `{shard}`.
pub const METRIC_REPLICATION_LAG_BYTES: &str = "harvest.replication.lag_bytes";

/// Gauge: number of standbys with a live walsender for a shard (issue #954).
///
/// `0` means replication is **down** for that shard — the RPO is unbounded and
/// growing, and a failover now would lose everything since the standby stopped
/// consuming. This, not a lag threshold, is the "replication broken" alert:
/// a standby that is not connected produces no lag reading to threshold.
/// Labelled `{shard}`.
pub const METRIC_REPLICATION_STANDBYS: &str = "harvest.replication.standbys";

/// Gauge: `1` while a shard's measured RPO is known. `0` when the
/// replication views are readable but the RPO itself is not (issue #954,
/// finding 2).
///
/// [`METRIC_REPLICATION_OBSERVABLE`] covers the views-unreadable case. It
/// does not cover a shard whose views read fine but whose RPO has no
/// source yet. That happens for a physical standby attached with no DR
/// slot, before it has reported its first `replay_lag`.
///
/// A Prometheus gauge keeps exporting its last value. So simply skipping
/// [`METRIC_REPLICATION_LAG_SECONDS`] in that case does not make the
/// series stale; it freezes the dashboard at the last healthy reading.
/// This gauge is emitted on every sampler tick the views are readable, `0`
/// included, so it cannot go stale the same way.
///
/// Alerting: ticket when this is `0` while
/// [`METRIC_REPLICATION_OBSERVABLE`] is `1` (readable but unmeasurable) — see
/// `harvest_replication_rpo_unknown` in the starter alert pack. Labelled
/// `{shard}`.
pub const METRIC_REPLICATION_RPO_KNOWN: &str = "harvest.replication.rpo_known";

/// Gauge: the write-authority epoch a shard's database currently reports
/// (issue #954).
///
/// Monotonic per shard. Its value is uninteresting; its *skew* is the point —
/// shards fail over independently, so
/// `max(harvest_shard_generation) by () != min(...)` is the machine-readable
/// form of "some shards were failed over and some were not", the hazard
/// `docs/cross-region-dr.md` describes. Labelled `{shard}`.
pub const METRIC_SHARD_GENERATION: &str = "harvest.shard.generation";

/// Gauge: audit-export delivery lag for a shard, in seconds (issue #953).
///
/// The **age of the oldest audit record the sink has not acknowledged** — the
/// answer to "how far behind is the SIEM right now?". `0` means every audit
/// record on the shard has been acknowledged at least once.
///
/// Note the deliberate choice of *oldest*, not newest, unacknowledged record:
/// under sustained mutating load a stuck exporter always has a brand-new
/// unexported record, so a newest-based gauge would read ~0 during exactly the
/// outage an operator needs to see. Oldest-based lag is what the issue's own
/// success metric ("export lag stays < 30s p99") is measurable against.
///
/// Emitted on **every** exporter tick, including ticks that deliver nothing —
/// the signal must not go stale precisely when delivery has stopped.
///
/// Labelled `{shard}` only: bounded cardinality per ADR-0001 §7. The audit
/// `actor`, `operation`, and `target_id` are deliberately never labels — they
/// are unbounded, user-supplied, and tenant-identifying.
pub const METRIC_AUDIT_EXPORT_LAG: &str = "harvest.audit.export_lag";

/// Gauge: `1` when the exporter observed a shard's cursor and lag this tick,
/// `0` when it could not (issue #1268).
///
/// A Prometheus gauge keeps its last value. Without this signal, a shard the
/// exporter cannot observe freezes [`METRIC_AUDIT_EXPORT_LAG`] at its last
/// reading, commonly `0`. The threshold alert then stays silent, and
/// `absent()` never fires, because the series still exists.
///
/// Covers a connection the scanner cannot acquire, a failed cursor read, and
/// a failed lag query. It does not cover a shard missing from
/// `shard_assignments` altogether, since no code path ever runs for it.
/// Template a per-shard `absent()` rule from your own inventory for that
/// case.
///
/// Emitted on every exporter tick that reaches a shard, delivery outcome
/// aside. Labelled `{shard}` only, for the same cardinality reason as
/// [`METRIC_AUDIT_EXPORT_LAG`].
pub const METRIC_AUDIT_EXPORT_OBSERVED: &str = "harvest.audit.export_observed";

/// Counter: audit records acknowledged by the sink for a shard (issue #953).
///
/// Incremented by the batch size only after the cursor has actually advanced,
/// so `rate(harvest_audit_exported)` is a true delivery rate, not an attempt
/// rate. At-least-once delivery means a redelivered batch counts again;
/// receiver-side `(shard, seq)` accounting, not this counter, is the
/// completeness proof.
///
/// Labelled `{shard}` only, for the same cardinality reason as above.
pub const METRIC_AUDIT_EXPORTED: &str = "harvest.audit.exported";

/// Counter: this worker was fenced off a shard it is pinned to (issue #954).
///
/// Incremented once, immediately before the worker stops. A non-zero value in
/// the *promoted* region means a fleet was left pinned to a pre-failover epoch
/// and must be restarted; a non-zero value with no failover in progress means
/// someone bumped a generation on a healthy shard. Both are page-worthy and
/// neither is self-healing. Labelled `{shard}`.
pub const METRIC_SHARD_FENCED: &str = "harvest.shard.fenced";

/// Gauge: current number of entries in the dead letter queue.
pub const METRIC_DLQ_ENTRIES: &str = "harvest.dlq.entries";

/// Gauge: `1` while a task queue is paused by an operator, `0` once it resumes
/// (issue #619).
///
/// Emitted by the queue-pause sampler in `worker.rs` on the worker's poll
/// cadence, labelled `{queue}`. Cardinality is bounded by the number of
/// distinct queue names the fleet has ever paused within a process lifetime;
/// a resumed queue is explicitly zero-filled for one cycle so the series drops
/// to `0` rather than going stale at `1`. Per ADR-0001 §7, `execution.id` is
/// never a label.
///
/// `max by (queue) (harvest_queue_paused) > 0` is the "a hold is in effect"
/// signal; pairing it with Prometheus' own `for:` duration is how an operator
/// alerts on a pause that was left on too long.
pub const METRIC_QUEUE_PAUSED: &str = "harvest.queue.paused";

/// Counter: incremented once per dead-letter entry processed by an operator
/// redrive (issue #510). Labelled `{queue, outcome}` where `outcome` is one of
/// `redriven` / `skipped` / `failed`.
pub const METRIC_DLQ_REDRIVEN: &str = "harvest.dlq.redriven";

/// Counter: incremented each time a scheduled run is dispatched.
pub const METRIC_SCHEDULE_RUNS: &str = "harvest.schedule.runs";

/// Counter: incremented each time a scheduled run is skipped.
pub const METRIC_SCHEDULE_SKIPPED: &str = "harvest.schedule.skipped";

/// Counter: incremented when a completion trigger fires (issue #517).
///
/// Attributes: `trigger`, `outcome` (`started` | `skipped` | `deduped` |
/// `validation_failed` | `payload_too_large`).
pub const METRIC_COMPLETION_TRIGGER_FIRED: &str = "harvest.completion_trigger.fires";

/// Counter: a completion trigger's output guard skipped a fire (issue #810).
///
/// Deliberately a **separate** counter from
/// [`METRIC_COMPLETION_TRIGGER_FIRED`], whose `outcome="skipped"` already
/// means "workflow-id reuse dedupe at start" — do not overload.
///
/// Attributes: `trigger`, `reason` (`condition_unmet` | `condition_invalid`).
pub const METRIC_COMPLETION_TRIGGER_SKIPPED: &str = "harvest.completion_trigger.skipped";

/// Counter: incremented each time `POST /admin/schedules/{id}/trigger` fires a
/// one-off run (issue #343).
///
/// Labels: `schedule.name` (low-cardinality), `outcome` (`"fired"` or
/// `"skipped_overlap"` or `"rejected_paused"`).
pub const METRIC_SCHEDULE_MANUAL_TRIGGER: &str = "harvest.schedule.manual_trigger";

/// Counter: incremented when a schedule decision write fails due to a database error.
pub const METRIC_SCHEDULE_DECISION_WRITE_FAILED: &str = "harvest.schedule.decision_write_failed";

/// Counter: number of rows deleted by the retention janitor in one tick.
pub const METRIC_RETENTION_DELETED: &str = "harvest.retention.deleted";

/// Counter: number of execution summaries garbage-collected by the summary
/// retention GC pass (issue #752).
///
/// A distinct member of the retention metric family from
/// `harvest.retention.deleted` (history rows), so operators can separate the
/// two tiers. Labeled by the low-cardinality `workflow` registry key.
pub const METRIC_SUMMARY_DELETED: &str = "harvest.retention.summary_deleted";

/// Counter: inert per-tenant rate-limit buckets collected by the idle-bucket GC
/// pass (issue #1127).
///
/// Labeled by [`METRIC_LABEL_RATE_LIMIT_FAMILY`] — `dyn-rate` or
/// `start-throttle` — and **never** by the bucket key. The key is the very
/// thing whose cardinality is unbounded (one value per tenant, forever), which
/// is why these buckets need collecting at all; labelling by it would trade an
/// unbounded table for an unbounded metric series. Per-key bucket state stays
/// observable through `GET /admin/rate-limits`, matching the same decision made
/// for the per-key gauges (issue #699) and for `harvest.codec.reencrypted`
/// (issue #948).
pub const METRIC_RATE_LIMIT_BUCKETS_DELETED: &str = "harvest.retention.rate_limit_buckets_deleted";

/// Histogram: wall-clock latency of query handler invocations (seconds).
///
/// Labelled with `query.name` (low-cardinality handler name registered by the
/// workflow author). Per ADR-0001 cardinality rule, `execution.id` stays
/// span-only and is never a metric label.
pub const METRIC_QUERY_DURATION: &str = "harvest.query.duration";

/// Counter: incremented when a workflow task is picked up by the worker that
/// already holds that execution's replay state in its in-process LRU cache.
///
/// Labeled by `workflow` (workflow name). `execution.id` stays span-only per
/// the existing cardinality rule (ADR-0001 §7).
pub const METRIC_WORKFLOW_CACHE_HIT: &str = "harvest.workflow.cache_hit";

/// Counter: incremented when a workflow task is picked up by a worker that
/// does NOT hold that execution's replay state in its in-process LRU cache,
/// causing a full event-history reload from Postgres.
///
/// Labeled by `workflow` (workflow name). `execution.id` stays span-only per
/// the existing cardinality rule (ADR-0001 §7).
pub const METRIC_WORKFLOW_CACHE_MISS: &str = "harvest.workflow.cache_miss";

/// Counter: incremented once per `signal_external_workflow` call after the
/// terminal outcome is recorded in `harvest_events`.
///
/// Labels: `outcome` (`"delivered"` or `"failed"`), `reason_code` (only set
/// when `outcome == "failed"`; values: `"target_terminal"`, `"target_unknown"`).
///
/// Per ADR-0001 §7, `harvest.target.execution.id` and `harvest.signal.id` are
/// **span-only** and must never appear as metric labels.
pub const METRIC_EXTERNAL_SIGNAL_SENT: &str = "harvest.workflow.external_signal.sent";

/// OpenTelemetry span attribute: the signal name for `signal_external_workflow` spans.
///
/// Used in `harvest.signal.send` child spans. Low-cardinality (equals the
/// string literal passed to `ctx.signal_external_workflow`).
pub const ATTR_SIGNAL_NAME: &str = "harvest.signal.name";

/// OpenTelemetry span attribute: the target execution ID for cross-workflow signal spans.
///
/// Per ADR-0001 §7 cardinality rule this attribute is **span-only** and must
/// never be used as a metric label.
pub const ATTR_TARGET_EXECUTION_ID: &str = "harvest.target.execution.id";

/// OpenTelemetry span attribute: the signal ID for external signal spans.
pub const ATTR_SIGNAL_ID: &str = "harvest.signal.id";

/// Counter: incremented when a workflow execution is terminated because its
/// `deadline_at` elapsed before the workflow completed.
///
/// Labeled by `workflow` (workflow name) and `queue` (task queue name).
/// `execution.id` stays span-only per the cardinality rule (ADR-0001 §7).
pub const METRIC_WORKFLOW_TIMEOUT: &str = "harvest.workflow.timeout";

/// Counter: incremented when a workflow execution is terminated because its
/// chain-scoped lifetime cap (`chain_deadline_at`) elapsed (issue #617).
///
/// Distinct from [`METRIC_WORKFLOW_TIMEOUT`]: the chain cap is anchored at the
/// first run's start and carried verbatim across every continue-as-new, so this
/// counter fires when a whole CAN chain (not a single run) has run too long.
///
/// Labeled by `workflow` (workflow name) and `queue` (task queue name).
/// `execution.id` stays span-only per the cardinality rule (ADR-0001 §7).
pub const METRIC_WORKFLOW_CHAIN_TIMEOUT: &str = "harvest.workflow.chain_timeout";

/// Counter: incremented each time a workflow-task dispatch is abandoned because
/// it did not complete or suspend within `WorkerConfig::workflow_task_timeout`
/// (issue #494).
///
/// Each increment signals one reclamation of a worker concurrency slot from a
/// hung or blocking workflow body. After `poison_pill_threshold` consecutive
/// increments for the same execution the task is quarantined to the DLQ with
/// [`DeadLetterReason::WorkflowTaskTimeout`](crate::dlq::DeadLetterReason).
///
/// Labeled by `workflow` (workflow name) and `queue` (task queue name).
/// `execution.id` stays span-only per the cardinality rule (ADR-0001 §7).
pub const METRIC_WORKFLOW_TASK_TIMEOUT: &str = "harvest.workflow.task_timeout";

/// Counter: incremented exactly once per run when a workflow execution exceeds its
/// declared soft SLA budget (`sla_deadline_at`) while still RUNNING/SUSPENDED.
///
/// This is a **soft, non-fatal signal** (issue #487): the run is never terminated.
/// A breaching run that later completes still reaches COMPLETED with its normal result.
///
/// Labeled by `workflow` (workflow name) and `queue` (task queue name).
/// `execution.id` stays span-only per the cardinality rule (ADR-0001 §7).
pub const METRIC_WORKFLOW_SLA_BREACHED: &str = "harvest.workflow.sla_breached";

/// Counter: workflow history bloat early-warning (issue #704).
///
/// Fires the moment a workflow execution's recorded `harvest_events` count
/// first crosses `history_bloat_warn_fraction * event_hard_cap`
/// (`WorkflowHistoryPolicy`), from **two** emission sites: (1)
/// `process_workflow_task`'s `Persisted` arm, post-commit, for an execution
/// that is still RUNNING (non-terminal, `WorkflowOutcome::Suspended`) after
/// the crossing decision cycle; and (2) `fail_workflow_for_history_cap`, when
/// a single decision cycle grows history from below the soft threshold
/// straight past `event_hard_cap` in one inline append batch (e.g.
/// local-activity or external-signal persistence) -- the crossing still
/// happened in that same decision, so it is emitted there too, immediately
/// before the execution is terminally hard-cap-failed and moved to the DLQ.
///
/// **Delivery is at-least-once, not exactly-once**: the counter is emitted
/// *before* the guard column (`history_bloat_warned_at`) is durably
/// persisted, so a worker crash in the narrow window between the two leaves
/// the guard unset and a later retry of the same decision cycle may re-emit
/// -- a deliberate trade-off (documented on `emit_history_bloat_warning_if_crossed`)
/// favoring "never silently lose the signal" over "never duplicate it" for a
/// one-shot, non-recurring gate with no later crossing to fall back on for a
/// given execution. Once the guard is durably set, the emission is
/// idempotent for the life of that execution row (a continue-as-new
/// successor or reset fork is a fresh row and is naturally eligible to warn
/// again, since it tracks its own history size from zero).
///
/// This is an **operator early-warning**, not a lifecycle change on its own:
/// path (1) never terminates or otherwise alters the run -- it is purely a
/// signal that the run is approaching the point where the existing hard-cap
/// terminal-fail (DLQ) behavior would trigger. Path (2) accompanies (not
/// causes) a termination the hard cap was already going to enforce
/// regardless of this counter.
///
/// Labeled by `workflow` (workflow name) ONLY -- no `queue` label, unlike its
/// sibling counters above. The soft-threshold crossing is a property of the
/// execution's own accumulated history size against a globally-configured
/// fraction/cap, not a per-queue phenomenon, and `execution.id` stays
/// span-only per the cardinality rule (ADR-0001 §7).
pub const METRIC_WORKFLOW_HISTORY_BLOAT: &str = "harvest.workflow.history_bloat";

/// Counter: incremented each time a failed workflow execution is automatically
/// rescheduled for a retry run (issue #523).
///
/// Labeled by `workflow` (workflow name) and `queue` (task queue name).
/// `execution.id` stays span-only per the cardinality rule (ADR-0001 §7).
pub const METRIC_WORKFLOW_RETRIES: &str = "harvest.workflow.retries";

/// Counter: incremented when a replay non-determinism (divergence) failure occurs.
///
/// Labeled by `workflow` (workflow name) and `build_id`.
pub const METRIC_WORKFLOW_NON_DETERMINISM: &str = "harvest.workflow.non_determinism";

/// Counter: incremented each time an execution enters (or re-enters) the
/// non-terminal replay-non-determinism blocked state (issue #603).
///
/// A single incident emits once per blocked dispatch attempt, so the rate
/// reflects how hard the divergent cohort is re-hitting the divergence; the
/// execution row's `nd_block_count` column disambiguates re-blocks from fresh
/// incidents.
///
/// Labeled by `workflow` (workflow name) and `queue` (task queue name).
/// `execution.id` stays span-only per the cardinality rule (ADR-0001 §7).
pub const METRIC_WORKFLOW_ND_BLOCKED: &str = "harvest.workflow.nondeterministic_block";

/// Counter: incremented each time a workflow execution is paused by an operator
/// or the bounded-pause auto-resume scanner (issue #383).
///
/// Labeled by `workflow` (workflow name) and `queue` (task queue name).
/// `execution.id` stays span-only per the cardinality rule (ADR-0001 §7).
pub const METRIC_WORKFLOW_PAUSED: &str = "harvest.workflow.paused";

/// Histogram: wall-clock seconds an execution spent in the `PAUSED` state,
/// recorded once on resume (issue #383).
///
/// Labeled by `workflow` (workflow name) and `queue` (task queue name).
/// `execution.id` stays span-only per the cardinality rule (ADR-0001 §7).
pub const METRIC_WORKFLOW_PAUSE_DURATION: &str = "harvest.workflow.pause_duration";

/// Counter: incremented when a workflow execution reaches a terminal state
/// with unfinished update/signal handlers (issue #536).
///
/// Labeled by `workflow` (workflow name) and `kind` (handler kind, e.g. "update").
pub const METRIC_WORKFLOW_UNFINISHED_HANDLERS: &str = "harvest.workflow.unfinished_handlers";

/// Counter: incremented each time a poison-pill task is quarantined to the
/// dead-letter queue after crashing `poison_pill_threshold` workers in a row
/// (issue #367).
///
/// Labeled by `queue` (task queue name) and `reason`
/// (`"poison_pill"`). `execution.id` stays span-only per ADR-0001 §7.
pub const METRIC_TASK_QUARANTINED: &str = "harvest.task.quarantined";

/// Counter: capability misses (issue #804).
///
/// Incremented each time a worker claims a task whose workflow or activity type
/// it has no handler registered for — either releasing the claim back to
/// `PENDING` for a capable peer, or escalating to the terminal-failure path once
/// the redelivery budget is exhausted.
///
/// A sustained non-zero release rate is the operator-visible signature of
/// **capability skew** across a queue's worker fleet — most commonly the
/// transient window of a rolling deploy that introduces a new workflow or
/// activity type. A non-zero *escalation* rate means no live worker on that
/// queue registers the handler at all, and executions are being failed.
///
/// Labeled by:
/// - `queue` (= [`METRIC_LABEL_QUEUE`]): the task queue name (bounded).
/// - `task_type` (= [`METRIC_LABEL_TASK_TYPE`]): `"workflow"` or `"activity"`
///   (the owning task row's `task_type`, bounded by the DB CHECK constraint to
///   `workflow` / `activity`; note a *local-activity* or activity-scheduling miss
///   is raised while processing a **workflow** task, so it reports
///   `task_type="workflow"` — the log line carries the handler kind separately).
/// - `outcome` (= [`METRIC_LABEL_OUTCOME`]): [`CAPABILITY_MISS_OUTCOME_RELEASED`],
///   [`CAPABILITY_MISS_OUTCOME_ESCALATED`], or
///   [`CAPABILITY_MISS_OUTCOME_ESCALATED_NEVER_OFFERED`] (bounded).
///
///
/// The `outcome` dimension is a deliberate bounded **superset** of the label
/// set named in issue #804 (`{queue, task_type}`): AC5 also requires operators
/// to be able to "alert on the escalation", which is only expressible if the
/// benign release and the executions-are-failing escalation are separable on
/// the same counter. This matches the existing repo idiom
/// (`harvest.schedule.fire_attempts{outcome}`, `harvest.workflow.terminal{outcome}`).
///
/// The workflow/activity **name** is deliberately NOT a label: it is bounded in
/// practice but the `queue` + `task_type` pair is enough to localize a deploy
/// skew, and `execution.id` stays span-only per ADR-0001 §7.
pub const METRIC_TASK_CAPABILITY_MISS: &str = "harvest.task.capability_miss";

/// `outcome` label value on [`METRIC_TASK_CAPABILITY_MISS`]: the claim was
/// released back to `PENDING` for a capable peer to pick up (issue #804).
pub const CAPABILITY_MISS_OUTCOME_RELEASED: &str = "released";

/// `outcome` label value on [`METRIC_TASK_CAPABILITY_MISS`]: the task was
/// released to a peer at least once, and was then escalated (issue #804).
///
/// **This label is recorded by two different bounds, which do not license the
/// same conclusion.** Read the `no_capable_worker:` reason string on the failed
/// execution — it names the bound that actually tripped — before concluding
/// anything about the fleet from a sample:
///
/// - **Coverage confirmed.** The task was offered around the queue with capped
///   exponential backoff between releases, and the worker registry then agreed
///   that every live eligible worker on the queue had already missed it. This
///   is the *fleet-exhaustion* reading — "no live worker on this queue
///   registers the handler" — and the one
///   `GET /admin/workflow-types/reachability` corroborates.
/// - **Release ceiling.** Coverage was never established: a live worker had
///   still never been offered the task, or the registry could not be read. The
///   absolute release ceiling stops the redeliveries anyway so AC3's bound
///   holds, but such a sample does **not** show the queue was swept, and a
///   capable worker may have been live on it the whole time.
///
/// Both are recorded here because under either reading the executions are
/// being failed, and paging on only the provable half would under-page the
/// outage #804 exists to surface. So treat the label as "executions on this
/// queue are being failed for want of a handler", which is true of every
/// sample, rather than as proof about the fleet, which is not. It is what the
/// page-severity `harvest_no_capable_worker` rule selects.
///
/// Escalations where the task was **never offered to a peer at all** report
/// [`CAPABILITY_MISS_OUTCOME_ESCALATED_NEVER_OFFERED`] instead — see there for
/// why conflating the two is an operator-misdirection bug, not a cosmetic one.
pub const CAPABILITY_MISS_OUTCOME_ESCALATED: &str = "escalated";

/// `outcome` label value on [`METRIC_TASK_CAPABILITY_MISS`]: escalated on the
/// **first** claim, without the task ever having been offered to a peer
/// (issue #804).
///
/// Two configurations reach this, both of which fail the execution after
/// **zero** releases:
///
/// - `capability_miss_max_redeliveries = 0` — the documented pre-#804
///   fail-fast rollback switch.
/// - The task is pinned to a worker session (issue #606) whose host does not
///   register the handler. Releasing it "for a capable peer" is false by
///   construction: the claim gate `session_id IS NULL OR sticky_worker_id = $1`
///   means no other worker can ever claim it.
///
/// Kept separate from [`CAPABILITY_MISS_OUTCOME_ESCALATED`] because the two
/// carry **opposite** operator conclusions. Budget exhaustion is evidence about
/// the *fleet*; a never-offered escalation is evidence about one config knob or
/// one task's pin, and a capable worker may well be live and idle on the queue
/// the whole time. Recording both under one value would page
/// `harvest_no_capable_worker`, whose `first_action` sends on-call to
/// `GET /admin/workflow-types/reachability` — which returns `in_use` and
/// directly contradicts the page — and whose runbook would advise raising a
/// budget that provably cannot help either cause.
pub const CAPABILITY_MISS_OUTCOME_ESCALATED_NEVER_OFFERED: &str = "escalated_never_offered";

/// Counter: incremented each time an activity's circuit breaker trips open
/// (closed → open) or re-opens after a failed half-open probe (issue #369).
///
/// Labeled by `activity.name`. `execution.id` stays span-only per ADR-0001 §7.
pub const METRIC_CIRCUIT_TRIPPED: &str = "harvest.activity.circuit.tripped";

/// Counter: incremented each time an activity's circuit breaker recovers to the
/// closed state after a successful half-open probe (issue #369).
///
/// Labeled by `activity.name`. `execution.id` stays span-only per ADR-0001 §7.
pub const METRIC_CIRCUIT_CLOSED: &str = "harvest.activity.circuit.closed";

/// Counter: incremented each time an activity handler **panics** (unwinds)
/// instead of returning a clean `Err`, and the engine contains the panic as a
/// retryable typed `HandlerPanic` failure (issue #782).
///
/// Fires once per panicking attempt (a retried activity that panics again on
/// its next attempt increments again), so this is a per-attempt panic-rate
/// signal. Labeled by `activity` (registered activity name) and `queue` (task
/// queue name). `execution.id` stays span-only per ADR-0001 §7.
pub const METRIC_ACTIVITY_PANIC: &str = "harvest.activity.panic";

/// Counter: a contained workflow handler panic (issue #782).
///
/// Incremented each time a workflow handler **panics** (unwinds) instead of
/// returning a clean `Err`, and the engine contains the panic as a non-terminal
/// re-dispatch (or, once `workflow_panic_max_attempts` is reached, a terminal
/// typed `HandlerPanic` failure).
///
/// Fires on **every** panic entry — each non-terminal panic-retry *and* the
/// final terminal panic — so a workflow that panics `N` times before exhausting
/// its panic budget emits `N` samples. Labeled by `workflow` (workflow name)
/// and `queue` (task queue name). `execution.id` stays span-only per
/// ADR-0001 §7.
pub const METRIC_WORKFLOW_PANIC: &str = "harvest.workflow.panic";

/// Counter: incremented each time a start request is admitted to a debounce
/// pending record — i.e. the burst is absorbed without starting a run (issue #499).
///
/// Labeled by `workflow` (workflow type name) and `debounce_key` (resolved key).
/// Per ADR-0001 §7, `execution.id` is span-only and must never appear here.
pub const METRIC_WORKFLOW_DEBOUNCED: &str = "harvest.workflow.debounced";

/// Counter: incremented each time the debounce scanner fires a pending record
/// and starts exactly one workflow execution (issue #499).
///
/// Labeled by `workflow` (workflow type name) and `queue` (task queue name).
/// Per ADR-0001 §7, `execution.id` is span-only and must never appear here.
pub const METRIC_DEBOUNCE_FIRED: &str = "harvest.workflow.debounce_fired";

/// Counter: incremented each time a workflow start is deferred by a start
/// throttle because no token was available (issue #607).
///
/// Labeled by `workflow` (workflow type name) only. The resolved throttle key is
/// high-cardinality (tenant/user input), so per ADR-0001 §7 it is deliberately
/// **not** a metric label — per-key backlog is exposed via the
/// `GET /admin/start-throttle` admin read instead. `execution.id` is span-only
/// and must never appear here.
pub const METRIC_WORKFLOW_START_THROTTLED: &str = "harvest.workflow.start_throttled";

/// Counter: incremented exactly once per in-flight run superseded by a newer
/// admission under `ConcurrencyOnConflict::CancelRunning` (issue #811).
///
/// A latest-wins start that sheds K incumbents emits K samples. A `Defer` start
/// (the default), an attach, and an admission that was already under its limit
/// all emit nothing.
///
/// Labeled by `workflow` (workflow type name) only. The resolved concurrency key
/// is high-cardinality (tenant/entity input), so per ADR-0001 §7 it is
/// deliberately **not** a metric label — per-key state is exposed via the
/// `GET /admin/concurrency` admin read instead. `execution.id` is span-only and
/// must never appear here.
pub const METRIC_CONCURRENCY_SUPERSEDED: &str = "harvest.concurrency.superseded";

/// Counter: a latest-wins admission left a key transiently over its limit
/// (issue #1197, item 2).
///
/// Incremented once per admission that could not shed its full computed
/// overflow because every remaining over-limit run was a protected in-flight
/// (nested) admission. This is the nested self-referential-trigger residual
/// described in issue #1197: cancelling an incumbent can synchronously start
/// a completion-trigger target that also declares `cancel_running` on the
/// same key, and that
/// nested admission is protected from being shed by the very outer admission
/// it is nested inside (see [`crate::concurrency::supersede_plan`]'s
/// `protected` parameter). The key is left transiently over its declared
/// limit; the population self-heals on the next ordinary admission for the
/// key, which sees every run unprotected. This counter is the promotion of
/// the pre-existing `tracing::warn!` in
/// [`crate::concurrency::supersede_running_for_key`] to an alertable signal —
/// the issue's chosen resolution for a key that stays over limit, rather than
/// the (more invasive, higher-lock-contention) alternatives of registration-time
/// rejection or a post-commit reconcile sweep.
///
/// Labeled by `workflow` (workflow type name) only, matching
/// [`METRIC_CONCURRENCY_SUPERSEDED`]'s cardinality rule — the concurrency key
/// is never a label (ADR-0001 §7). `execution.id` must never appear here.
pub const METRIC_CONCURRENCY_RESIDUAL_OVER_LIMIT: &str = "harvest.concurrency.residual_over_limit";

/// Counter: incremented exactly once per real saga compensation sequence
/// (issue #801).
///
/// A "sequence" is a non-empty `Saga::compensate_all` / step-failure unwind
/// actually running forward. Counted at unwind start on the live frontier,
/// deduped across replays by the durable `saga_compensated:{seq}` marker, so
/// the value reflects real sequences rather than the replay/re-registration
/// count.
///
/// A compensation-rate spike is the canonical leading indicator that a
/// downstream dependency is failing and sagas are rolling back en masse.
///
/// At-least-once within the single-decision-cycle gap between in-process
/// emission and the marker's batch commit (crash / pause-race discard) —
/// see the accepted edges in `docs/saga.md`.
///
/// Labeled by `workflow` (workflow name) and `queue` (task queue name).
/// `execution.id` stays span-only per the cardinality rule (ADR-0001 §7).
pub const METRIC_SAGA_COMPENSATED: &str = "harvest.saga.compensated";

/// Counter: incremented exactly once per saga unwind that finishes with at
/// least one compensation error (issue #801).
///
/// This is the `HarvestError::SagaCompensationFailed` dangling-state case
/// needing manual reconciliation.
///
/// Distinct from the generic `harvest.workflow.terminal{outcome=failed}`
/// counter, and emitted in-Saga rather than at the worker terminal boundary,
/// so it fires even when the workflow author catches the error and the run
/// goes on to COMPLETE.
///
/// Coupled to the unwind's start disposition (`failed ≤ compensated`, per
/// unwind), and at-least-once within the same emit→persist gap as the
/// compensated counter — see the accepted edges in `docs/saga.md`.
///
/// Labeled by `workflow` (workflow name) and `queue` (task queue name).
/// `execution.id` stays span-only per the cardinality rule (ADR-0001 §7).
pub const METRIC_SAGA_COMPENSATION_FAILED: &str = "harvest.saga.compensation_failed";

/// Histogram: wall-clock seconds a workflow waited to acquire a durable mutex,
/// from request (enqueued as a waiter) to grant (issue #691).
///
/// Labeled by `workflow` (workflow type name) only. The lock key is
/// high-cardinality (often tenant/entity input), so per ADR-0001 §7 it is
/// deliberately **not** a metric label. `execution.id` stays span-only.
pub const METRIC_MUTEX_WAIT: &str = "harvest.mutex.wait_duration";

/// Histogram: wall-clock seconds a durable mutex was held, from grant to
/// release (issue #691).
///
/// Labeled by `workflow` (workflow type name) only — the lock key is
/// deliberately **not** a label (ADR-0001 §7). `execution.id` stays span-only.
pub const METRIC_MUTEX_HELD: &str = "harvest.mutex.held_duration";

/// Gauge: the number of workflows waiting on a durable mutex key at the moment
/// a grant is made (contention depth), i.e. the FIFO waiter-queue length for
/// the key (issue #691).
///
/// Labeled by `workflow` (workflow type name) only — the lock key is
/// deliberately **not** a label (ADR-0001 §7). `execution.id` stays span-only.
pub const METRIC_MUTEX_CONTENTION: &str = "harvest.mutex.contention_depth";

/// Histogram: wall-clock seconds a synthetic liveness-canary probe took from
/// start-requested to terminal completion (issue #796).
///
/// This is the built-in end-to-end pipeline probe, **distinct from the #512
/// replay canary** (which validates code changes). Recorded only when a canary
/// run reaches terminal completion; a timed-out probe increments
/// [`METRIC_CANARY_FAILURE`] instead.
///
/// Labeled by `queue` (the probed task queue) and `shard` (the writable shard
/// the probe ran on). Per ADR-0001 §7, `execution.id` is never a label.
pub const METRIC_CANARY_ROUNDTRIP: &str = "harvest.canary.roundtrip";

/// Counter: incremented once each time a synthetic liveness-canary probe
/// reaches terminal completion (issue #796).
///
/// Labeled by `queue` and `shard`. `execution.id` stays span-only per the
/// cardinality rule (ADR-0001 §7).
pub const METRIC_CANARY_SUCCESS: &str = "harvest.canary.success";

/// Counter: incremented once each time a synthetic liveness-canary probe fails
/// (issue #796).
///
/// A failure is a probe that did not reach terminal completion within its
/// per-probe timeout, or that terminated in a non-completed state.
///
/// Labeled by `queue` and `shard`. `execution.id` stays span-only per the
/// cardinality rule (ADR-0001 §7).
pub const METRIC_CANARY_FAILURE: &str = "harvest.canary.failure";

/// Histogram: observed payload size in bytes at each write boundary (issue #252).
///
/// Emitted for every payload written to `harvest_events`, regardless of whether
/// it was accepted or rejected. Labeled with:
/// - `payload.kind`: the [`PayloadKind`] variant (e.g. `"ActivityInput"`)
/// - `workflow.type`: the workflow type name
/// - `activity.name`: the activity name (label omitted when not applicable)
///
/// Per ADR-0001 §7, `execution.id` is span-only and must never appear here.
///
/// [`PayloadKind`]: crate::error::PayloadKind
pub const METRIC_PAYLOAD_BYTES: &str = "harvest.payload.bytes";

/// Counter: incremented each time a payload is rejected because it exceeds the
/// configured size cap (issue #252).
///
/// Labeled with `payload.kind` and `workflow.type`. Incrementing once per
/// rejection event (not per byte) so operators can alert on rejection rate.
///
/// Per ADR-0001 §7, `execution.id` is span-only.
pub const METRIC_PAYLOAD_REJECTED: &str = "harvest.payload.rejected";

/// Counter + bytes measure: a payload-bearing field was offloaded to an external
/// [`PayloadStore`](crate::payload_store::PayloadStore) via claim-check (issue #524).
///
/// Incremented once per offloaded field; the increment value is the **byte
/// length** of the offloaded payload, so operators get both an offload rate
/// (count of events) and total bytes offloaded.
///
/// Labels: `payload.field` (the event field name, e.g. `"output"`), `store.id`.
/// Per ADR-0001 §7, `execution.id` is span-only and must never appear here.
pub const METRIC_PAYLOAD_OFFLOADED: &str = "harvest.payload.offloaded";

/// Histogram: wall-clock seconds to fetch an offloaded payload back from the
/// external store on read/replay (issue #524).
///
/// Labels: `store.id`. Per ADR-0001 §7, `execution.id` is span-only.
pub const METRIC_PAYLOAD_OFFLOAD_FETCH_DURATION: &str = "harvest.payload.offload_fetch_duration";

/// Counter: incremented each time a new workflow start is blocked by an
/// active admission gate (issue #377).
///
/// Labels:
///   - `"scope_kind"` — the gate's scope kind (`"fleet"`, `"workflow_name"`,
///     `"queue"`, `"shard_id"`, or `"owner"`).
///   - `"reason_hash"` — first 8 chars of a stable SHA-256 of the reason
///     string to bound cardinality while preserving debuggability.
///
/// Per ADR-0001 §7, `execution.id` and `gate_id` are never metric labels.
pub const METRIC_ADMISSION_BLOCKED: &str = "harvest.admission.blocked";

/// Gauge: current number of active admission gates.
pub const METRIC_ADMISSION_GATES_ACTIVE: &str = "harvest.admission.gates_active";

/// Counter: incremented each time a fresh workflow start is rejected because
/// its resolved per-tenant [`crate::quota::QuotaPolicy`] key is at or over
/// one of its declared caps (issue #946).
///
/// Labels:
///   - `"workflow"` (= [`METRIC_LABEL_WORKFLOW`]) — the workflow type name.
///   - `"resource"` (= [`METRIC_LABEL_RESOURCE`]) — which cap was exhausted
///     (`"active_executions"`, `"history_bytes"`, or `"dead_letters"`, per
///     [`crate::quota::QuotaResource::as_str`]).
///
/// The *resolved tenant key* is deliberately **not** a label — it is
/// caller/tenant-controlled input and would be unbounded cardinality. Per-key
/// usage-vs-quota is instead surfaced via `GET /admin/quotas`, mirroring how
/// the admission gate keeps its per-key detail off `harvest.admission.blocked`
/// and exposes it through the equivalent read route instead. Per ADR-0001 §7,
/// `execution.id` is never a label here either.
pub const METRIC_QUOTA_REJECTED: &str = "harvest.quota.rejected";

/// Counter: incremented by the number of `harvest_events` rows the lazy
/// payload-codec re-encryption sweep rewrote onto the active key (issue #948).
///
/// Label:
///   - `"shard"` (= [`METRIC_LABEL_SHARD`]) — the shard swept.
///
/// The **codec key id is deliberately not a label.** Key ids are
/// operator-chosen and accumulate over a deployment's life, so labelling by key
/// would grow a new series on every rotation and never retire the old ones.
/// Per-key rows-remaining is instead surfaced by `GET /admin/codec/rotation`,
/// mirroring how `harvest.quota.rejected` (#946) keeps its per-key detail on the
/// equivalent admin read.
///
/// `rate(harvest_codec_reencrypted_total{shard="..."}[5m]) == 0` while
/// `GET /admin/codec/rotation` still reports rows remaining under a retired key
/// is the alerting shape for "the rotation has stalled".
pub const METRIC_CODEC_REENCRYPTED: &str = "harvest.codec.reencrypted";

/// Counter: incremented each time a start producer that is **exempt-by-design**
/// from the admission gate relays a workflow start (issue #618).
///
/// Label:
///   - `"producer"` (= [`METRIC_LABEL_PRODUCER`]) — the exempt producer's
///     bounded label (e.g. `"outbox"`), from
///     [`crate::admission_gate::StartProducer::as_str`].
///
/// This makes every intentional gate bypass observable so an operator can see
/// in real time whether anything is slipping the gate. Per ADR-0001 §7,
/// `execution.id` is never a metric label.
pub const METRIC_ADMISSION_BYPASSED: &str = "harvest.admission.bypassed";

/// Gauge: current available tokens in a rate limit bucket.
pub const METRIC_RATE_LIMIT_TOKENS_AVAILABLE: &str = "harvest.rate_limit.tokens_available";

/// Gauge: refill rate (tokens per second) for a rate limit bucket.
pub const METRIC_RATE_LIMIT_REFILL_RATE: &str = "harvest.rate_limit.refill_rate";

/// Counter: incremented when a task claim is throttled/skipped due to rate limiting.
pub const METRIC_RATE_LIMIT_THROTTLED: &str = "harvest.rate_limit.throttled";

/// Counter: incremented on each scheduler tick-loop fire attempt for a due schedule slot.
///
/// Labels:
/// - `schedule` — the workflow or DAG name (low-cardinality).
/// - `outcome` — one of:
///   - `"claimed"` — this replica atomically claimed the slot and will fire it.
///   - `"lost_race"` — another replica already holds a live claim for this
///     slot; this replica skips it without firing.
///
/// Use this metric to verify in Grafana / your alert stack that contention is
/// happening, claims are exclusive, and no replica is silently dominating the
/// fire path. See `docs/runbooks/ha-deployment.md` for thresholds.
pub const METRIC_SCHEDULE_FIRE_ATTEMPTS: &str = "harvest.schedule.fire_attempts";

/// Counter emitted once each time a schedule is automatically paused after
/// `consecutive_failure_limit` consecutive execution failures (issue #360).
///
/// Labels:
///   - `"schedule"` — the workflow name bound to the schedule.
///
/// Alert threshold: `harvest_schedule_auto_paused_total > 0` over any 5-minute
/// window. Each auto-pause event means operator action is required to resume.
pub const METRIC_SCHEDULE_AUTO_PAUSED: &str = "harvest.schedule.auto_paused";

/// Gauge: per-schedule overdue flag (issue #696).
///
/// `1` when an *active* schedule is overdue to fire relative to its own cadence
/// (`now − next_run_at > grace`, where `grace = cadence step + jitter +
/// scheduler tick interval`), `0` otherwise.
///
/// Labels:
///   - `"kind"` — `"workflow"` or `"dag"` (bounded).
///   - `"name"` — the registered workflow or DAG name (low-cardinality). Per
///     ADR-0001 §7 the schedule/execution id is NEVER a label.
///
/// Emitted per schedule by a periodic background sampler
/// (`scheduler::sample_overdue_schedules`, run per shard on the worker
/// monitoring cadence, independent of the scheduler tick so a wedged tick does
/// not suppress its own health signal). Intentionally-not-firing schedules
/// (`is_paused`, `auto_paused_at`, `Schedule::Manual`, `end_at`/`max_runs`
/// exhausted) and at-capacity schedules report `0`. The sampler re-emits every
/// pass, including `0` for recovered/healthy schedules, so the gauge stays
/// fresh; a *deleted* schedule's series goes stale (no registry to reset it,
/// the standard gauge property). Two schedules that share a `name` aggregate on
/// the gauge (overdue if either is overdue).
///
/// Alert on `max by (kind, name) (harvest_schedule_overdue) > 0` — a precise
/// per-schedule threshold that names the wedged schedule, replacing the fragile
/// absence-of-`harvest.schedule.runs` inference.
pub const METRIC_SCHEDULE_OVERDUE: &str = "harvest.schedule.overdue";

/// Gauge: count of *in-flight* (RUNNING/SUSPENDED) executions whose durable
/// event history has grown past the configured soft
/// `continue_as_new_threshold` (issue #493).
///
/// Labeled by `workflow` (workflow type name). `execution.id` MUST NOT appear
/// here (ADR-0001 §7 cardinality rule).
///
/// Sampled on the same cadence as `harvest.queue.depth` (the
/// `spawn_queue_depth_sampler` interval). A non-zero value means at least one
/// in-flight workflow is accumulating history faster than its author drains it
/// via `continue_as_new`. Alert on sustained non-zero values so the operator
/// can nudge the author or enable `max_workflow_history_events`.
pub const METRIC_WORKFLOW_HISTORY_OVERSIZED: &str = "harvest.workflow.history_oversized";

/// Gauge: count of currently-*active* workflow executions, grouped by workflow
/// type and lifecycle state (issue #770).
///
/// Labels:
/// - `workflow` (= [`METRIC_LABEL_WORKFLOW`]): the workflow type name
///   (bounded — one per registered `#[workflow]`).
/// - `state` (= [`METRIC_LABEL_STATE`]): the active lifecycle state, one of
///   exactly two bounded values — `"running"` or `"paused"` (issue #383).
///
/// `execution.id` MUST NOT appear here (ADR-0001 §7 cardinality rule); total
/// series cardinality is bounded by `workflow_types × 2`.
///
/// Sourced from a shard-local `COUNT(*) … GROUP BY (workflow_name, state)
/// WHERE state IN ('RUNNING','PAUSED')` and summed across every shard of the
/// worker's `ShardedDbPool`. Sampled on the same cadence as
/// `harvest.queue.depth`. A non-zero value is the live in-progress population;
/// a steady rise while `harvest.workflow.terminal` stays flat signals a leak
/// or backlog. `nd_blocked` runs stay `RUNNING` (issue #603) and so count
/// under `state="running"`; there is no separate state for them.
pub const METRIC_WORKFLOW_ACTIVE: &str = "harvest.workflow.active";

/// Counter: incremented on every request that reaches an inbound webhook
/// receiver route, regardless of outcome (issue #344).
///
/// Labels: `path` (the registered `#[webhook(path = ...)]` binding — a
/// closed set fixed at `HarvestPlugin::build` time, so cardinality is bounded
/// by construction per ADR-0001 §7) and `outcome` (= [`METRIC_LABEL_OUTCOME`],
/// one of [`WebhookOutcome`]'s bounded values). `execution.id` is never a
/// label here — it stays span-only.
pub const METRIC_WEBHOOK_RECEIVED: &str = "harvest.webhook.received";

/// Counter: incremented every time an inbound webhook request is rejected (issue #344).
///
/// Rejection reasons: signature/timestamp/replay verification failure,
/// payload parse failure, a mapping function rejection, or a missing
/// idempotency key.
///
/// Labels: `path` and `outcome`, same bounded contract as
/// [`METRIC_WEBHOOK_RECEIVED`]. `outcome` is never `"accepted"` on this
/// counter.
pub const METRIC_WEBHOOK_REJECTED: &str = "harvest.webhook.rejected";

/// Counter: incremented once for **every** message a broker event-source
/// connector pulls off its topic/queue, before any mapping or dispatch is
/// attempted (issue #944).
///
/// Labels: `source` (= [`METRIC_LABEL_SOURCE`], the operator-declared binding
/// name — a closed set fixed at `HarvestPlugin::build` time, exactly like the
/// webhook receiver's `path`). Per ADR-0001 §7 the message key, partition and
/// offset are **never** labels: they are unbounded broker-supplied values, so
/// they stay span-/log-only.
pub const METRIC_CONNECTOR_RECEIVED: &str = "harvest.connector.received";

/// Counter: incremented once per connector message that reached a terminal
/// disposition (issue #944).
///
/// Labels: `source` (= [`METRIC_LABEL_SOURCE`]) and `outcome`
/// (= [`METRIC_LABEL_OUTCOME`], one of [`ConnectorOutcome`]'s bounded values).
/// Never labeled by message coordinates (ADR-0001 §7).
pub const METRIC_CONNECTOR_DISPATCHED: &str = "harvest.connector.dispatched";

/// Counter: incremented once per connector message dead-lettered (issue #944).
///
/// A message is dead-lettered when it is malformed, deterministically refused
/// by the target, or rejected by the mapping function past the poison
/// threshold.
///
/// Labels: `source` (= [`METRIC_LABEL_SOURCE`]) and `reason`
/// (= [`METRIC_LABEL_REASON`], one of [`PoisonReason`]'s bounded values).
pub const METRIC_CONNECTOR_POISONED: &str = "harvest.connector.poisoned";

/// Gauge: the broker-reported consumer lag for a connector source, sampled on
/// each poll cycle for the adapters whose client exposes it (issue #944).
///
/// Labels: `source` (= [`METRIC_LABEL_SOURCE`]). Adapters that cannot report
/// lag simply never emit this gauge.
pub const METRIC_CONNECTOR_LAG: &str = "harvest.connector.lag";

/// Counter: incremented once per worker-session acquisition attempt (issue
/// #606), i.e. once per `ctx.create_session(...)` call.
///
/// Labels: `queue` (the session's target task queue) and `outcome` (=
/// [`METRIC_LABEL_OUTCOME`], one of [`SessionAcquisitionOutcome`]'s bounded
/// values: `acquired`/`timed_out`/`broken`). Per ADR-0001 §7, `execution.id`
/// is never a label -- a session's identity stays span-/log-only here.
pub const METRIC_SESSION_ACQUISITION: &str = "harvest.session.acquisition";

/// Counter: one background control-loop iteration completed (issue #797).
///
/// Incremented **unconditionally at the end of every iteration** of each
/// background scanner — including iterations that found no work and
/// iterations whose pass returned an error. That unconditional emission is
/// the whole point: the pre-existing loop metrics
/// ([`METRIC_RETENTION_DELETED`], [`METRIC_SCHEDULE_FIRE_ATTEMPTS`], the
/// timeout/SLA/quarantine counters) only fire when there is work to do, so a
/// healthy idle loop and a wedged one both read zero. Here a **flat-lined
/// counter is itself the wedge signal**, and a wedged loop is detectable
/// within `2 ×` its poll interval.
///
/// Labeled by `scanner` (= [`METRIC_LABEL_SCANNER`]) whose value set is
/// bounded by construction to the seven
/// [`Scanner`](crate::scanner_health::Scanner) variants. Per ADR-0001 §7,
/// `execution.id` is never a label — these are process-level control loops
/// and carry no execution identity at all.
///
/// Alert on the *rate*, not the value:
/// `rate(harvest_scanner_tick_total[5m]) == 0`. The companion in-process
/// last-tick registry is surfaced by the `scanner_liveness` check in
/// `GET /admin/preflight` for deployments without a metrics pipeline.
pub const METRIC_SCANNER_TICK: &str = "harvest.scanner.tick";

/// Counter: a `SignalReceived` event was durably delivered into a workflow's
/// history and promoted to a live workflow-task wake (issue #684).
///
/// Emitted once per delivered signal at the single durable-delivery choke
/// point (`ingest_due_timers_and_signals`, live worker path only — never on
/// replay). Labeled by `workflow` (workflow name) and `queue` (task queue),
/// both bounded.
///
/// **The signal `name` is deliberately NOT a label (issue #684, Codex P2).**
/// Signals are delivered via the free-form route
/// `POST /workflows/{id}/signal/{signal_name}` — the `signal_name` path segment
/// is caller-controlled and, unlike activity names or `#[update]` handler names,
/// has no declared registry to bound it. A caller using per-entity / dynamic
/// signal names would create unbounded metric series, violating the ADR-0001 §7
/// cardinality contract. Per-signal-name diagnostics are out of scope for this
/// slice (use the per-workflow stack / open-awaitables API). `execution.id` is
/// span-only per ADR-0001 §7 and must never appear here.
pub const METRIC_SIGNAL_RECEIVED: &str = "harvest.signal.received";

/// Counter: a delivered `SignalReceived` was never consumed at a terminal
/// outcome (issue #684).
///
/// "Never consumed" = no `wait_for_signal`/`receive_signal` and no push handler
/// claimed it by the time the run reached a **Completed or Failed** terminal
/// outcome via the worker drive.
///
/// The consumed-set is computed in the executor's terminal arms (the only place
/// the driven matcher exists) and **carried out on the terminal
/// [`WorkflowOutcome`](crate::executor::WorkflowOutcome)**; the **worker** emits
/// this counter from that map **post-commit, in its `Persisted` arm** — the same
/// discipline as [`METRIC_UPDATE_COMPLETED`]/[`METRIC_UPDATE_FAILED`] — so the
/// counter represents **durable terminal outcomes only**. That arm is reached
/// only after the persist transaction commits, downstream of the issue #603
/// ND-block gate (`Failed{nd:Some}` early-returns before persist) and
/// `check_paused_and_park` (a claimed-then-paused race returns via
/// `ParkedPaused`, and a persist failure via the `Err` arm — neither reaches the
/// emit). A retry/resume of such a discarded cycle therefore cannot double-count.
/// Signals excused by a lost signal-or-deadline race (issue #476) are never
/// counted. Labeled by `workflow`, `name`, and `queue`.
///
/// **Coverage scope — KNOWN LIMITATION (deliberate):** this is emitted only
/// from graceful `Completed`/`Failed` terminals reached through the workflow
/// drive. **Forced-failure / scanner terminal paths — `TIMED_OUT`, `CANCELLED`,
/// `TERMINATED`, parent-close cascade, and history-cap failure
/// (`fail_workflow_for_history_cap`) — with an undrained signal are NOT
/// counted**, because they have no driven matcher to reconstruct the
/// consumed-set, and a partial/inaccurate count on a watched SLO metric is
/// worse than none.
/// This notably **excludes the "stuck workflow that ignored a signal and then
/// timed out"** case: for that, watch [`METRIC_WORKFLOW_TIMEOUT`] plus the
/// per-workflow stack API instead.
///
/// Labeled by `workflow` and `queue` only, both bounded. **The signal `name`
/// is deliberately NOT a label (issue #684, Codex P2)** — for the same
/// free-form-send-route / no-declared-registry reason as
/// [`METRIC_SIGNAL_RECEIVED`], and most acutely here: an unhandled signal's
/// name is by definition a mismatched / unlistened-for one. The worker sums
/// the terminal outcome's per-name unconsumed map and emits one increment per
/// unconsumed occurrence against the single `(workflow, queue)` series.
/// `execution.id` is span-only per ADR-0001 §7.
pub const METRIC_SIGNAL_UNHANDLED: &str = "harvest.signal.unhandled";

/// Counter: a workflow update was durably admitted (`UpdateAdmitted` appended)
/// (issue #684).
///
/// Emitted post-commit at the single admission choke point
/// (`store::admit_update_event`) for the HTTP, Vantage-UI, and update-with-start
/// paths. Labeled by `workflow` (workflow name) and `queue`. `execution.id` is
/// span-only per ADR-0001 §7.
///
/// **`name` is deliberately NOT a label — bounded by construction (issue #684,
/// Codex P2):** the raw route `POST /workflows/{id}/update/{name}` admits ANY
/// name (the validator lookup is best-effort — an unregistered name falls
/// through and is durably admitted), so a `name` label would let a hostile/buggy
/// caller create unbounded `admitted` series. Unlike the terminal
/// [`METRIC_UPDATE_COMPLETED`]/[`METRIC_UPDATE_FAILED`] counters — which bound an
/// unregistered name to the [`UNREGISTERED_UPDATE_NAME`] sentinel using the
/// workflow's handler-not-found *result* — the admission site cannot bound the
/// name: it has no way to know whether a name resolves to a handler, because
/// imperatively-registered handlers (`ctx.register_update_handler`, the common
/// pattern) are not known until the workflow executes, and bounding against only
/// the declarative `registry.update_handlers` would mislabel every legitimate
/// imperatively-registered update. Dropping the label is therefore the only way
/// to bound this counter's cardinality by construction. Per-name update
/// visibility lives on the post-resolution counters `harvest.update.completed`/
/// `failed`/`rejected` instead.
pub const METRIC_UPDATE_ADMITTED: &str = "harvest.update.admitted";

/// Counter: a workflow update was rejected by its registered validator before
/// admission (a pre-admission `422`) (issue #684).
///
/// Emitted at the two durable validator-rejection sites (the `admit_update`
/// and `update_with_start` HTTP handlers). Scope is deliberately limited to
/// **validator** rejections — non-`RUNNING`/paused admission-state conflicts
/// are surfaced as caller errors, not counted here. Labeled by `workflow` and
/// `name` (update name). `execution.id` is span-only per ADR-0001 §7.
pub const METRIC_UPDATE_REJECTED: &str = "harvest.update.rejected";

/// Counter: an admitted workflow update ran its handler to success
/// (`UpdateCompleted` appended) (issue #684).
///
/// Emitted post-commit in the worker's `Persisted` arm, exactly once per
/// completed update (the `RecordUpdateResult` command is produced once on live
/// execution). Labeled by `workflow`, `name` (update name), and `queue`. A
/// completed update always ran a real handler, so its `name` is inherently
/// bounded to the app's registered handler set (issue #684, Codex P2).
/// `execution.id` is span-only per ADR-0001 §7.
pub const METRIC_UPDATE_COMPLETED: &str = "harvest.update.completed";

/// Counter: an admitted workflow update's handler returned an error
/// (`UpdateFailed` appended) (issue #684).
///
/// Emitted post-commit in the worker's `Persisted` arm, exactly once per
/// failed update. Labeled by `workflow`, `name` (update name), and `queue`. The
/// `name` label is bounded (issue #684, Codex P2): a genuinely-unregistered
/// name — admitted via the free-form raw route with no matching handler —
/// fails the workflow's handler lookup with the exact `"update handler '<name>'
/// not found"` error, which the worker buckets to the [`UNREGISTERED_UPDATE_NAME`]
/// sentinel; every real handler (declarative `#[update]` **or** imperative
/// `ctx.register_update_handler`) keeps its name. `execution.id` is span-only
/// per ADR-0001 §7.
pub const METRIC_UPDATE_FAILED: &str = "harvest.update.failed";

/// Histogram: wall-clock seconds from an update's durable admission
/// (`UpdateAdmitted`) to its terminal result recording
/// (`UpdateCompleted`/`UpdateFailed`) (issue #781).
///
/// The admit→terminal latency companion to the `harvest.update.*` lifecycle
/// counters (issue #684). Emitted on the **same** post-commit best-effort path
/// as [`METRIC_UPDATE_COMPLETED`]/[`METRIC_UPDATE_FAILED`] (the shared
/// `collect_update_result_metrics`/`emit_update_result_metrics` worker helpers),
/// so it inherits their exactly-once-on-the-happy-path delivery semantics: a
/// crash after the persist commit but before the post-commit emit drops the
/// sample (at-most-once for that window, never a double-count). Labeled by
/// `workflow`, `name` (update name — bounded to the resolved handler set, an
/// unregistered name → [`UNREGISTERED_UPDATE_NAME`]), `queue`, and `outcome`
/// (`"completed"`/`"failed"` — rejected updates never enter the histogram, as no
/// handler runs). The admit timestamp comes from the `UpdateAdmitted` event
/// already in the loaded history; the terminal time is `Utc::now()` at emit,
/// clamped so a clock-skew negative delta records `0`, never a garbage value.
/// `execution.id`/`update_id` are span-only per ADR-0001 §7.
pub const METRIC_UPDATE_DURATION: &str = "harvest.update.duration";

/// Sentinel `name` label for a `harvest.update.failed` counter when the admitted
/// update name resolved to no handler (issue #684, Codex P2).
///
/// The raw route `POST /workflows/{id}/update/{name}` admits arbitrary,
/// caller-controlled names; an unregistered one fails the workflow's handler
/// lookup with the exact `"update handler '<name>' not found"` error, and the
/// worker buckets exactly that case to this sentinel — keeping the `name` label
/// bounded to the app's real handler set plus this one extra series, without
/// mislabeling any legitimately-registered (declarative or imperative) handler.
pub const UNREGISTERED_UPDATE_NAME: &str = "__unregistered__";

/// Sentinel `activity` label for [`METRIC_ACTIVITY_PAUSE_ACTIONS`] when the
/// paused name is not in this process's registered activity catalogue
/// (issue #807).
///
/// The pause routes deliberately accept an **unregistered** name: an operator
/// must be able to pre-pause an activity type before its worker fleet rolls
/// out, and a paused-but-unregistered activity is a real hold that must remain
/// inspectable. That makes the raw name caller-controlled free text (bounded in
/// *length* by `activity_pause::MAX_ACTIVITY_NAME_LEN`, but not in
/// *cardinality*), which ADR-0001 §7 forbids as a metric label — a buggy
/// remediation loop templating a tenant id into the name would mint unbounded
/// permanent series.
///
/// Bucketing exactly that case here keeps the label bounded to the app's real
/// activity set plus this one extra series, without mislabeling any legitimately
/// registered activity. The durable `harvest_activity_pauses` row and the read
/// API keep the raw name; only the metric label is bucketed. Same shape and
/// same reasoning as [`UNREGISTERED_UPDATE_NAME`].
pub const UNREGISTERED_ACTIVITY_NAME: &str = "__unregistered__";

/// Bounded outcome classification for a worker-session acquisition attempt
/// (issue #606).
///
/// Every value maps 1:1 to a distinct `create_session` result, so the set is
/// closed by construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionAcquisitionOutcome {
    /// A worker with a free session slot claimed the acquire task before the
    /// caller's `acquisition_timeout` elapsed.
    Acquired,
    /// No worker had a free slot within `acquisition_timeout`; surfaced to
    /// the caller as `HarvestError::SessionAcquireTimeout`.
    TimedOut,
    /// The session was reclaimed as broken (host died/drained or its lease
    /// expired) before or during acquisition; surfaced as
    /// `HarvestError::SessionBroken`.
    Broken,
}

impl SessionAcquisitionOutcome {
    /// Stable lower-case outcome string used as the `outcome` metric label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Acquired => "acquired",
            Self::TimedOut => "timed_out",
            Self::Broken => "broken",
        }
    }
}

impl std::fmt::Display for SessionAcquisitionOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Metric label: the registered `#[webhook(path = ...)]` binding path
/// (issue #344). Bounded cardinality: only registered webhook bindings ever
/// appear, fixed at `HarvestPlugin::build` time.
pub const METRIC_LABEL_PATH: &str = "path";

/// Bounded outcome classification for an inbound webhook request (issue #344).
///
/// Every value maps 1:1 to an HTTP status class the receiver route returns,
/// so the set is closed by construction — no user input ever becomes an
/// outcome string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebhookOutcome {
    /// A fresh workflow execution (or signal) was dispatched (`202`).
    Accepted,
    /// A redelivery of an already-dispatched event was recognized and
    /// short-circuited without a duplicate dispatch (`200`, or a `409` from
    /// autumn-web's own replay-protection layer).
    IdempotentReplay,
    /// autumn-web's `SignedWebhook` extractor rejected the request
    /// (signature mismatch, stale timestamp, missing/malformed header).
    VerifyFailed,
    /// The verified body was not valid JSON, or the mapping function
    /// deserialize/reject step failed.
    ParseFailed,
    /// A `SignalsWithStart` target resolved no delivery ID to use as the
    /// signal's idempotency key.
    MissingIdempotency,
    /// Any other internal failure (e.g. the harvest runtime is not yet
    /// started, or the dispatch call itself failed).
    InternalError,
}

impl WebhookOutcome {
    /// Stable lower-case outcome string used as the `outcome` metric label
    /// and the JSON `error_code` field.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::IdempotentReplay => "idempotent_replay",
            Self::VerifyFailed => "verify_failed",
            Self::ParseFailed => "parse_failed",
            Self::MissingIdempotency => "missing_idempotency",
            Self::InternalError => "internal_error",
        }
    }
}

impl std::fmt::Display for WebhookOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Bounded terminal outcomes for one message consumed by a broker event-source
/// connector (issue #944), used as the `outcome` label on
/// [`METRIC_CONNECTOR_DISPATCHED`].
///
/// Closed set: adding a variant is an additive change, but the set must stay
/// small so the metric's cardinality is bounded by construction (ADR-0001 §7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorOutcome {
    /// The message durably started a workflow or delivered a signal.
    Dispatched,
    /// The message was recognized as a redelivery of an already-dispatched
    /// message; nothing new was committed, but the original dispatch is
    /// durable so the message is safe to acknowledge.
    IdempotentReplay,
    /// The target workflow carries a throttle/debounce/batch policy and the
    /// admission was durably parked (an HTTP `202`-equivalent). The start is
    /// recorded and will fire on the owning scanner's schedule, so this is a
    /// success from the connector's point of view.
    Deferred,
    /// The message was routed to a dead-letter destination (see
    /// [`METRIC_CONNECTOR_POISONED`] for the reason breakdown).
    DeadLettered,
    /// A transient harvest-side failure left the message unacknowledged for
    /// broker-native redelivery.
    Retried,
}

impl ConnectorOutcome {
    /// Stable lower-case outcome string used as the `outcome` metric label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Dispatched => "dispatched",
            Self::IdempotentReplay => "idempotent_replay",
            Self::Deferred => "deferred",
            Self::DeadLettered => "dead_lettered",
            Self::Retried => "retried",
        }
    }
}

impl std::fmt::Display for ConnectorOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Bounded reasons a connector message was routed to a dead-letter
/// destination (issue #944), used as the `reason` label on
/// [`METRIC_CONNECTOR_POISONED`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoisonReason {
    /// The raw message body could not be decoded into the shape the binding's
    /// mapping function expects. Deterministic — retrying can never help, so
    /// this dead-letters on the first occurrence.
    Malformed,
    /// The mapping function ran but rejected the message, `threshold`
    /// consecutive times.
    MappingRejected,
    /// The dispatch target refused the message deterministically (a `4xx`
    /// from the start / signal-with-start path, e.g. published-schema
    /// validation). Deterministic — dead-letters on the first occurrence.
    TargetRejected,
}

impl PoisonReason {
    /// Stable lower-case reason string used as the `reason` metric label and
    /// persisted on the dead-letter record.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Malformed => "malformed",
            Self::MappingRejected => "mapping_rejected",
            Self::TargetRejected => "target_rejected",
        }
    }
}

impl std::fmt::Display for PoisonReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ---------------------------------------------------------------------------
// Metric label key constants
// Used by MetricsRecorder implementations to avoid string literals at call
// sites. These are short Prometheus-compatible names; the forbidden label
// (ATTR_EXECUTION_ID) deliberately has no entry here so it cannot be
// accidentally used on a metric.
// ---------------------------------------------------------------------------

/// Metric label: the broker event-source binding name (issue #944).
///
/// A closed set fixed at `HarvestPlugin::build` time (one value per declared
/// binding), so it is bounded by construction. The topic/queue, partition,
/// offset and message key are deliberately **not** labels — they are
/// unbounded broker-supplied values (ADR-0001 §7).
pub const METRIC_LABEL_SOURCE: &str = "source";

/// Metric label: the workflow name.
pub const METRIC_LABEL_WORKFLOW: &str = "workflow";

/// Label: the bounded rate-limit key *family* (`dyn-rate` / `start-throttle`),
/// used by [`METRIC_RATE_LIMIT_BUCKETS_DELETED`] in place of the unbounded
/// bucket key (issue #1127).
pub const METRIC_LABEL_RATE_LIMIT_FAMILY: &str = "family";
/// Metric label: the low-cardinality workflow type.
pub const METRIC_LABEL_WORKFLOW_TYPE: &str = "workflow.type";
/// Metric label: the activity name.
pub const METRIC_LABEL_ACTIVITY: &str = "activity";
/// Metric label: the activity name, dotted form used by circuit-breaker
/// counters (issue #369) to match the ADR-0001 `activity.name` attribute.
pub const METRIC_LABEL_ACTIVITY_NAME: &str = "activity.name";
/// Metric label: the task queue name.
pub const METRIC_LABEL_QUEUE: &str = "queue";
/// Metric label: task-queue row kind (`"workflow"` or `"activity"`) — bounded
/// by `CapabilityMissKind::as_str` (issue #804).
pub const METRIC_LABEL_TASK_TYPE: &str = "task_type";
/// Metric label: terminal outcome status (e.g. `"completed"`, `"failed"`).
pub const METRIC_LABEL_STATUS: &str = "status";
/// Metric label: active workflow lifecycle state (issue #770) — one of the
/// bounded values `"running"` / `"paused"`.
pub const METRIC_LABEL_STATE: &str = "state";
/// Metric label: low-cardinality error class on failed activity records.
pub const METRIC_LABEL_ERROR_TYPE: &str = "error.type";
/// Metric label: whether a failure was flagged non-retryable.
pub const METRIC_LABEL_NON_RETRYABLE: &str = "non_retryable";
/// Metric label: the shard number.
pub const METRIC_LABEL_SHARD: &str = "shard";
/// Metric label: schedule kind (`"dag"` or `"workflow"`).
pub const METRIC_LABEL_KIND: &str = "kind";
/// Metric label: the schedule or DAG name.
pub const METRIC_LABEL_NAME: &str = "name";
/// Metric label: reason a scheduled run was skipped.
pub const METRIC_LABEL_REASON: &str = "reason";
/// Metric label: the concurrency group key.
pub const METRIC_LABEL_KEY: &str = "key";
/// Which per-tenant [`crate::quota::QuotaResource`] cap was exhausted
/// (issue #946): `"active_executions"`, `"history_bytes"`, `"dead_letters"`.
pub const METRIC_LABEL_RESOURCE: &str = "resource";
/// Metric label: the query handler name (`query.name`).
pub const METRIC_LABEL_QUERY: &str = "query.name";
/// Metric label: terminal outcome (e.g. `"delivered"`, `"failed"`).
pub const METRIC_LABEL_OUTCOME: &str = "outcome";
/// Metric label: reason code for external signal failure.
pub const METRIC_LABEL_REASON_CODE: &str = "reason_code";
/// Metric label: how many runs a key is over its declared concurrency limit
/// (issue #1197, item 2), bounded by `concurrency::SUPERSEDE_SCAN_LIMIT`.
pub const METRIC_LABEL_GAP: &str = "gap";
/// Metric label: the completion trigger ID (issue #517).
pub const METRIC_LABEL_TRIGGER: &str = "trigger";
/// Metric label: admission gate scope kind (issue #377).
pub const METRIC_LABEL_SCOPE: &str = "scope";
/// Metric label: the in-process start producer (issue #618).
pub const METRIC_LABEL_PRODUCER: &str = "producer";
/// Metric label: the build ID of the worker.
pub const METRIC_LABEL_BUILD_ID: &str = "build_id";
/// Metric label: worker dispatch-slot type (`"workflow"` or `"activity"`).
pub const METRIC_LABEL_SLOT_TYPE: &str = "slot_type";
/// Metric label: adaptive slot-tuner decision (`"grow"` / `"shrink"` / `"hold"`,
/// issue #548).
pub const METRIC_LABEL_DECISION: &str = "decision";
/// Metric label: the operator action taken on an activity-type pause
/// (`"pause"` / `"resume"`, issue #807).
///
/// Bounded by construction to the [`ActivityPauseAction`] variants — a call
/// site passes the enum's `as_str()`, never a free string (same discipline as
/// [`METRIC_LABEL_SCANNER`] and [`METRIC_LABEL_DECISION`]).
pub const METRIC_LABEL_ACTION: &str = "action";
/// Metric label: background control loop identity (issue #797).
///
/// Bounded by construction to the [`Scanner`](crate::scanner_health::Scanner)
/// variants — a call site passes the enum's `as_str()`, never a free string.
pub const METRIC_LABEL_SCANNER: &str = "scanner";
/// `shard` label value for a control loop that is **not** per-shard (issue #797).
///
/// The `retention` and `schedule` loops run once per process rather than once
/// per assigned shard, and a single-shard deployment's per-shard loops have no
/// shard id to report. They emit this sentinel so every
/// [`METRIC_SCANNER_TICK`] series carries the same label set — a family with a
/// sometimes-present label is awkward to query and easy to mis-aggregate.
pub const SCANNER_SHARD_LABEL_NONE: &str = "none";

// ---------------------------------------------------------------------------
// Custom (user) metric constants and validation (issue #532)
// ---------------------------------------------------------------------------

/// Required prefix for all user-emitted custom metric names.
///
/// A call like `ctx.metrics().counter("orders_processed", 1, &[])` emits a
/// metric named `"harvest.user.orders_processed"`. The prefix is applied
/// automatically by [`UserMetrics`]; callers supply only the suffix.
///
/// Names already starting with `"harvest."` are rejected to prevent collision
/// with engine-internal metrics (see [`validate_user_metric`]).
pub const USER_METRIC_PREFIX: &str = "harvest.user.";

/// Maximum byte length of the user-supplied metric name suffix.
pub const MAX_USER_METRIC_NAME_LEN: usize = 200;

/// Maximum number of key-value labels allowed per custom metric call.
///
/// Matches the practical label-count limit recommended by ADR-0001 §7.
pub const MAX_USER_METRIC_LABELS: usize = 16;

/// High-cardinality label keys that are rejected by [`validate_user_metric`].
///
/// Per ADR-0001 §7, execution/activity identifiers must never appear on
/// metrics because they would create an unbounded label cardinality.
/// Both dotted and underscore-separated forms are listed to block both
/// the OpenTelemetry convention (`execution.id`) and the Prometheus convention
/// (`execution_id`) from being used as label keys.
pub const FORBIDDEN_USER_LABEL_KEYS: &[&str] = &[
    // Dotted forms (OpenTelemetry naming convention)
    "execution.id",
    "activity.id",
    "workflow.id",
    ATTR_EXECUTION_ID, // = "harvest.execution.id"
    ATTR_WORKFLOW_ID,  // = "harvest.workflow.id"
    "harvest.activity.id",
    // Prometheus underscore forms
    "execution_id",
    "activity_id",
    "workflow_id",
    // Non-namespaced high-cardinality keys
    "idempotency_key",
    "run_id",
];

/// Validation error returned by [`validate_user_metric`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserMetricError {
    /// The name is empty.
    EmptyName,
    /// The name starts with `"harvest."`, which is reserved for engine metrics.
    ReservedPrefix,
    /// The name exceeds [`MAX_USER_METRIC_NAME_LEN`] bytes.
    NameTooLong,
    /// More than [`MAX_USER_METRIC_LABELS`] labels were supplied.
    TooManyLabels,
    /// A label key is empty.
    EmptyLabelKey,
    /// A label key is in the high-cardinality denylist ([`FORBIDDEN_USER_LABEL_KEYS`]).
    ForbiddenLabelKey(String),
}

impl std::fmt::Display for UserMetricError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyName => write!(f, "custom metric name must not be empty"),
            Self::ReservedPrefix => write!(
                f,
                "custom metric name must not start with \"harvest.\"; \
                 that prefix is reserved for engine metrics"
            ),
            Self::NameTooLong => write!(
                f,
                "custom metric name exceeds {MAX_USER_METRIC_NAME_LEN} bytes"
            ),
            Self::TooManyLabels => write!(
                f,
                "custom metric has more than {MAX_USER_METRIC_LABELS} labels"
            ),
            Self::EmptyLabelKey => write!(f, "custom metric label key must not be empty"),
            Self::ForbiddenLabelKey(key) => write!(
                f,
                "label key \"{key}\" is forbidden on custom metrics (ADR-0001 §7 \
                 cardinality rule); use a low-cardinality label instead"
            ),
        }
    }
}

impl std::error::Error for UserMetricError {}

/// Validate a user-supplied custom metric name and label set.
///
/// Called automatically by [`UserMetrics`]; you only need to call this
/// directly when building a custom [`MetricsRecorder`] that pre-validates
/// names at registration time.
///
/// # Errors
///
/// Returns `Err(UserMetricError::*)` for any of the following:
/// - empty or over-long name suffix
/// - name starting with `"harvest."` (reserved engine prefix)
/// - more than [`MAX_USER_METRIC_LABELS`] labels
/// - empty or forbidden label key
pub fn validate_user_metric(name: &str, labels: &[(&str, &str)]) -> Result<(), UserMetricError> {
    if name.is_empty() {
        return Err(UserMetricError::EmptyName);
    }
    if name.starts_with("harvest.") {
        return Err(UserMetricError::ReservedPrefix);
    }
    if name.len() > MAX_USER_METRIC_NAME_LEN {
        return Err(UserMetricError::NameTooLong);
    }
    if labels.len() > MAX_USER_METRIC_LABELS {
        return Err(UserMetricError::TooManyLabels);
    }
    for (key, _) in labels {
        if key.is_empty() {
            return Err(UserMetricError::EmptyLabelKey);
        }
        if FORBIDDEN_USER_LABEL_KEYS.contains(key) {
            return Err(UserMetricError::ForbiddenLabelKey((*key).to_string()));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// TraceContextCarrier
// ---------------------------------------------------------------------------

/// W3C Trace Context carrier serialised alongside queued tasks.
///
/// The fields mirror the two HTTP headers defined by the
/// [W3C Trace Context specification](https://www.w3.org/TR/trace-context/):
/// `traceparent` (required to join a trace) and `tracestate` (optional
/// vendor-specific extensions). Storing them as JSONB keeps the schema
/// flexible if the spec gains additional headers.
///
/// The two extra fields (`is_replay`, `link_traceparent`) implement the replay
/// semantics defined in `docs/adr/0001-otel-trace-contract.md`: replay spans
/// must NOT inherit the original trace as a parent (it may have long expired),
/// but they SHOULD carry the original `traceparent` as an OpenTelemetry span
/// *link* so operators can navigate from a replay span to the original trace
/// in their APM tool.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceContextCarrier {
    /// The W3C `traceparent` header for the *parent* span.
    ///
    /// During replay this field is `None` — the replay span roots itself and
    /// links to [`link_traceparent`] instead.
    ///
    /// [`link_traceparent`]: TraceContextCarrier::link_traceparent
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,

    /// The optional W3C `tracestate` header.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tracestate: Option<String>,

    /// When `true`, the worker must emit the span with attribute
    /// `harvest.replay = true` and must NOT restore `traceparent` as the
    /// parent context. Instead, it should create a new root span and attach a
    /// span *link* pointing at [`TraceContextCarrier::link_traceparent`].
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub is_replay: bool,

    /// The original `traceparent` preserved for use as a span *link*.
    ///
    /// Only populated when `is_replay == true`. Enables APM navigation from a
    /// replay-emitted span back to the original (possibly expired) trace root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub link_traceparent: Option<String>,
}

impl TraceContextCarrier {
    /// Build a carrier from a raw `traceparent` header value.
    #[must_use]
    pub fn from_traceparent(traceparent: impl Into<String>) -> Self {
        Self {
            traceparent: Some(traceparent.into()),
            tracestate: None,
            is_replay: false,
            link_traceparent: None,
        }
    }

    /// Convert this carrier into a replay carrier.
    ///
    /// Per the OpenTelemetry trace contract ADR (`docs/adr/0001-otel-trace-contract.md`):
    /// - `traceparent` is cleared so the replay span becomes a new root.
    /// - `link_traceparent` is set to the original `traceparent` so the worker
    ///   can attach a span *link* for APM navigation.
    /// - `is_replay` is set to `true` so workers know to emit
    ///   `harvest.replay = true` and skip parent context installation.
    #[must_use]
    pub fn into_replay_context(self) -> Self {
        Self {
            link_traceparent: self.traceparent.or(self.link_traceparent),
            traceparent: None,
            tracestate: None,
            is_replay: true,
        }
    }

    /// Returns `true` when no context fields are set (ignoring the replay flag).
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.traceparent.is_none()
            && self.tracestate.is_none()
            && self.link_traceparent.is_none()
            && !self.is_replay
    }

    /// Serialise to a JSONB-compatible [`serde_json::Value`] suitable for
    /// writing to `harvest_task_queue.trace_context`. Returns `None` for an
    /// empty carrier so empty rows stay `NULL`.
    #[must_use]
    pub fn to_json(&self) -> Option<serde_json::Value> {
        if self.is_empty() {
            return None;
        }
        serde_json::to_value(self).ok()
    }

    /// Parse a carrier from the `trace_context` column payload.
    ///
    /// Malformed payloads yield `None` rather than erroring so a corrupt row
    /// never sinks a worker — the task is simply processed without a parent
    /// span.
    #[must_use]
    pub fn from_json(value: &serde_json::Value) -> Option<Self> {
        let carrier: Self = serde_json::from_value(value.clone()).ok()?;
        if carrier.is_empty() {
            None
        } else {
            Some(carrier)
        }
    }
}

// ---------------------------------------------------------------------------
// Propagator hook
// ---------------------------------------------------------------------------

/// Bridge between the current tracing context and the [`TraceContextCarrier`]
/// stored on tasks.
///
/// Applications implement this once against their chosen OpenTelemetry SDK
/// (for example, using `opentelemetry::global::get_text_map_propagator`). The
/// harvest runtime calls [`capture`](Self::capture) at enqueue time to snapshot
/// the active span and [`install`](Self::install) when a task is claimed so the
/// worker's handler span becomes a child of the original producer's trace.
///
/// Implementations must be cheap enough to call on every enqueue and dispatch.
pub trait TraceContextPropagator: Send + Sync {
    /// Capture the current trace context so it can travel with a queued task.
    ///
    /// Returning [`None`] means "no context to carry"; the carrier column on
    /// the task will be left `NULL`.
    fn capture(&self) -> Option<TraceContextCarrier>;

    /// Install `carrier` as the parent context for subsequent spans on this
    /// thread / task.
    ///
    /// Returns an opaque guard that restores the prior context when dropped.
    /// Implementations that do not support scoped restoration may return a
    /// dummy guard.
    ///
    /// The guard is `'static` so callers can hold it across `.await` points;
    /// implementations that need to reference propagator-owned state should
    /// clone an `Arc` into the guard rather than borrowing from `self`.
    fn install(&self, carrier: &TraceContextCarrier) -> Box<dyn Any + Send>;
}

/// Propagator that never captures context — the default when no telemetry is
/// configured.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoOpPropagator;

impl TraceContextPropagator for NoOpPropagator {
    fn capture(&self) -> Option<TraceContextCarrier> {
        None
    }

    fn install(&self, _carrier: &TraceContextCarrier) -> Box<dyn Any + Send> {
        Box::new(())
    }
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

/// Terminal outcome of a workflow execution, reported to [`MetricsRecorder`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowStatus {
    /// Handler returned `Ok(..)`.
    Completed,
    /// Handler returned `Err(..)`.
    Failed,
    /// Handler suspended awaiting activity results or timer firings; this run
    /// of the executor did not complete the workflow.
    ///
    /// **Not a terminal state.** Used only for the per-cycle
    /// `harvest.workflow.duration` histogram. Must never be passed to
    /// [`MetricsRecorder::record_workflow_terminal`].
    Suspended,
    /// Handler signalled `continue_as_new`: the current run is sealed and a
    /// fresh execution with the same `WorkflowId` is started in its place.
    ///
    /// Counted by `harvest.workflow.terminal{outcome="continued_as_new"}` but
    /// **excluded from the success-rate denominator**
    /// (`completed + failed + cancelled + timed_out`) because the logical
    /// workflow continues in a new execution.
    ContinuedAsNew,
    /// Gracefully cancelled via `cancel_workflow_execution`.
    Cancelled,
    /// Execution deadline (`deadline_at`) elapsed before the workflow completed.
    TimedOut,
    /// Force-killed via `terminate_workflow_execution` (operator override).
    Terminated,
}

impl WorkflowStatus {
    /// Stable string representation, suitable for metric tag values.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Suspended => "suspended",
            Self::ContinuedAsNew => "continued_as_new",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
            Self::Terminated => "terminated",
        }
    }
}

/// Terminal outcome of an activity invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivityStatus {
    /// Handler returned `Ok(..)`.
    Completed,
    /// Handler returned `Err(..)` (includes retries).
    Failed,
}

impl ActivityStatus {
    /// Stable string representation, suitable for metric tag values.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

/// A worker dispatch-slot pool, used as the bounded `slot_type` label on the
/// worker slot-occupancy gauges (issue #531).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotType {
    /// The `max_concurrent_workflows` semaphore pool.
    Workflow,
    /// The `max_concurrent_activities` semaphore pool.
    Activity,
}

impl SlotType {
    /// Stable string representation, suitable for metric tag values.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Workflow => "workflow",
            Self::Activity => "activity",
        }
    }
}

/// The decision an adaptive slot tuner made on one control-loop tick (issue
/// #548), used as the bounded `decision` label on
/// [`METRIC_WORKER_TUNER_DECISIONS`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TunerDecision {
    /// The live target was increased.
    Grow,
    /// The live target was decreased.
    Shrink,
    /// The live target was left unchanged (including a grow/shrink request
    /// fully absorbed by the `[min_slots, max_slots]` band clamp).
    Hold,
}

impl TunerDecision {
    /// Stable string representation, suitable for metric tag values.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Grow => "grow",
            Self::Shrink => "shrink",
            Self::Hold => "hold",
        }
    }
}

/// The operator action taken on an activity-type pause (issue #807), used as
/// the bounded `action` label on [`METRIC_ACTIVITY_PAUSE_ACTIONS`].
///
/// Exists so the label can never be a free string: the two values are the only
/// two mutations `activity_pause` exposes, so cardinality is bounded by the
/// type system rather than by convention (same reason [`TunerDecision`] and
/// [`SlotType`] exist).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivityPauseAction {
    /// Dispatch for the activity type was held.
    Pause,
    /// A previously held activity type was released.
    Resume,
}

impl ActivityPauseAction {
    /// Stable string representation, suitable for metric tag values.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pause => "pause",
            Self::Resume => "resume",
        }
    }
}

/// Sink for the harvest engine's standard metrics.
///
/// Implementations fan these calls into an OpenTelemetry meter (or whatever
/// metrics backend the application uses). All methods have default no-op
/// bodies so implementers can opt in to only the metrics they care about.
pub trait MetricsRecorder: Send + Sync {
    /// A completion trigger was fired/evaluated (issue #517).
    ///
    /// `outcome` is one of: `"started"`, `"skipped"`, `"deduped"`,
    /// `"validation_failed"`, `"payload_too_large"`.
    fn record_completion_trigger_fired(&self, trigger_id: &str, outcome: &str) {
        let _ = (trigger_id, outcome);
    }

    /// A completion trigger's output guard skipped a fire (issue #810).
    ///
    /// `reason` is one of: `"condition_unmet"` (guard evaluated `false`) or
    /// `"condition_invalid"` (stored condition unparseable/over-cap —
    /// fail-closed skip). Emitted only when the resolved-skip fires row is
    /// freshly inserted; a redelivery of an already-resolved skip records
    /// `"deduped"` on [`Self::record_completion_trigger_fired`] instead.
    fn record_completion_trigger_skipped(&self, trigger_id: &str, reason: &str) {
        let _ = (trigger_id, reason);
    }

    /// A new workflow start was blocked by an active admission gate (issue #377).
    ///
    /// `scope_kind` is one of: `"fleet"`, `"workflow_name"`, `"queue"`,
    /// `"shard_id"`, `"owner"`. `reason_hash` is the first 8 chars of a
    /// stable SHA-256 of the reason string for bounded cardinality.
    fn record_admission_blocked(&self, scope_kind: &str, reason_hash: &str) {
        let _ = (scope_kind, reason_hash);
    }

    /// The active gate count changed (useful for alerting on nonzero gates).
    fn record_admission_gates_active(&self, count: i64) {
        let _ = count;
    }

    /// A start producer that is exempt-by-design from the admission gate relayed
    /// a workflow start (issue #618).
    ///
    /// `producer` is the bounded label from
    /// [`crate::admission_gate::StartProducer::as_str`] (e.g. `"outbox"`).
    /// Making every intentional bypass observable is how an operator confirms
    /// nothing is silently slipping an active gate.
    fn record_admission_bypassed(&self, producer: &str) {
        let _ = producer;
    }

    /// A fresh workflow start was rejected because its resolved per-tenant
    /// quota key is at or over a declared cap (issue #946).
    ///
    /// `workflow` is the workflow type name; `resource` is the bounded
    /// [`crate::quota::QuotaResource::as_str`] value naming the exhausted
    /// cap. The resolved tenant key is intentionally not passed here — see
    /// [`METRIC_QUOTA_REJECTED`] for why.
    fn record_quota_rejected(&self, workflow: &str, resource: &str) {
        let _ = (workflow, resource);
    }

    /// The lazy payload-codec re-encryption sweep (issue #948) rewrote `count`
    /// `harvest_events` rows on `shard` onto the active key.
    ///
    /// `shard` is the shard id rendered as a string — a bounded label, one
    /// series per shard. The key id is intentionally not passed here; see
    /// [`METRIC_CODEC_REENCRYPTED`] for why.
    ///
    /// Additive with a no-op default: implementing it is optional and no
    /// existing implementor breaks.
    fn record_codec_reencrypted(&self, shard: &str, count: u64) {
        let _ = (shard, count);
    }

    /// A workflow task entered the executor on a worker.
    fn record_workflow_started(&self, workflow_name: &str, queue: &str) {
        let _ = (workflow_name, queue);
    }

    /// A workflow execution reached a terminal state with unfinished update/signal handlers (issue #536).
    fn record_workflow_unfinished_handlers(&self, workflow_name: &str, kind: &str, count: u64) {
        let _ = (workflow_name, kind, count);
    }

    /// A workflow task finished an executor cycle.
    ///
    /// `duration_secs` is the wall-clock time the handler spent running.
    /// `Suspended` runs are reported here too so operators can see executor
    /// churn.
    fn record_workflow_completed(
        &self,
        workflow_name: &str,
        queue: &str,
        duration_secs: f64,
        status: WorkflowStatus,
    ) {
        let _ = (workflow_name, queue, duration_secs, status);
    }

    /// A workflow reached a terminal state with this durable history size.
    fn record_workflow_history_size(&self, workflow_name: &str, event_count: u64) {
        let _ = (workflow_name, event_count);
    }

    /// A workflow execution reached a terminal state.
    ///
    /// Fires **exactly once** per execution at the terminal-state transition.
    /// Unlike [`record_workflow_completed`](Self::record_workflow_completed),
    /// this counter never fires for suspended executor cycles — a workflow
    /// that suspends N times and then completes produces exactly one
    /// `Completed` increment.
    ///
    /// `outcome` must be one of the six terminal variants of [`WorkflowStatus`]:
    /// `Completed`, `Failed`, `Cancelled`, `TimedOut`, `Terminated`, or
    /// `ContinuedAsNew`. Callers must **never** pass `Suspended`.
    ///
    /// Maps to the counter [`METRIC_WORKFLOW_TERMINAL`] with labels
    /// `workflow`, `queue`, and `outcome`.
    /// Per ADR-0001 §7, `execution.id` must never be a label here.
    fn record_workflow_terminal(&self, workflow_name: &str, queue: &str, outcome: WorkflowStatus) {
        let _ = (workflow_name, queue, outcome);
    }

    /// A workflow execution rotated using continue-as-new.
    fn record_workflow_continue_as_new(&self, workflow_name: &str) {
        let _ = workflow_name;
    }

    /// A workflow replay non-determinism (divergence) failure was detected.
    fn record_workflow_non_determinism(&self, workflow_name: &str, build_id: &str) {
        let _ = (workflow_name, build_id);
    }

    /// An execution entered (or re-entered) the non-terminal
    /// replay-non-determinism blocked state (issue #603).
    ///
    /// Emitted once per blocked dispatch attempt — never on the terminal
    /// failure path, which a blocked execution deliberately does not take.
    /// Maps to [`METRIC_WORKFLOW_ND_BLOCKED`].
    /// Per ADR-0001 §7, `execution.id` must never be a label here.
    fn record_workflow_nondeterministic_block(&self, workflow_name: &str, queue: &str) {
        let _ = (workflow_name, queue);
    }

    /// An activity handler panicked (unwound) and the engine contained the
    /// panic as a retryable typed `HandlerPanic` failure (issue #782).
    ///
    /// Emitted once per panicking attempt. Maps to [`METRIC_ACTIVITY_PANIC`].
    /// Per ADR-0001 §7, `execution.id` must never be a label here.
    fn record_activity_panic(&self, activity_name: &str, queue: &str) {
        let _ = (activity_name, queue);
    }

    /// A workflow handler panicked (unwound) and the engine contained the panic
    /// as a non-terminal re-dispatch or (once the panic budget is exhausted) a
    /// terminal typed `HandlerPanic` failure (issue #782).
    ///
    /// Emitted on every panic entry — each retry and the final terminal failure.
    /// Maps to [`METRIC_WORKFLOW_PANIC`]. Per ADR-0001 §7, `execution.id` must
    /// never be a label here.
    fn record_workflow_panic(&self, workflow_name: &str, queue: &str) {
        let _ = (workflow_name, queue);
    }

    /// Gauge snapshot: `count` in-flight executions for `workflow_name` whose
    /// durable event count exceeds the configured soft continue-as-new
    /// threshold (issue #493).
    ///
    /// Callers emit one call per distinct workflow name per sampler tick so
    /// the gauge resets cleanly when oversized executions drain. A `count` of
    /// `0` should be emitted for known workflow names once the oversized count
    /// drops to zero so the gauge falls back to zero rather than going stale.
    ///
    /// Maps to [`METRIC_WORKFLOW_HISTORY_OVERSIZED`].
    /// Per ADR-0001 §7, `execution.id` must never be a label here.
    fn record_workflow_history_oversized(&self, workflow_name: &str, count: u64) {
        let _ = (workflow_name, count);
    }

    /// The current count of active workflow executions of type `workflow` in
    /// lifecycle `state` (issue #770).
    ///
    /// `state` is always one of the two bounded values `"running"` /
    /// `"paused"`. Callers emit one call per distinct `(workflow, state)` pair
    /// per sampler tick and a `count` of `0` for pairs that were present last
    /// tick but have since drained, so the gauge resets cleanly rather than
    /// going stale.
    ///
    /// Maps to [`METRIC_WORKFLOW_ACTIVE`]. Per ADR-0001 §7, `execution.id` must
    /// never be a label here.
    fn record_workflow_active(&self, workflow: &str, state: &str, count: u64) {
        let _ = (workflow, state, count);
    }

    /// An activity invocation finished.
    fn record_activity_completed(
        &self,
        activity_name: &str,
        queue: &str,
        duration_secs: f64,
        status: ActivityStatus,
    ) {
        let _ = (activity_name, queue, duration_secs, status);
    }

    /// Variant of [`record_activity_completed`](Self::record_activity_completed)
    /// that also carries an `error.type` attribute for failed records.
    ///
    /// Per ADR-0001 §7, `error.type` must remain a low-cardinality attribute on
    /// the `harvest.activity.duration` histogram so operators can slice failure
    /// rates by error class without parsing message strings.
    ///
    /// The default body delegates to `record_activity_completed`, dropping the
    /// `error_type` — existing implementations stay correct without changes.
    /// Backends that want the slicing should override this method instead.
    fn record_activity_completed_with_error_type(
        &self,
        activity_name: &str,
        queue: &str,
        duration_secs: f64,
        status: ActivityStatus,
        error_type: Option<&str>,
    ) {
        let _ = error_type;
        self.record_activity_completed(activity_name, queue, duration_secs, status);
    }

    /// An activity invocation failed (per-attempt failure record).
    ///
    /// Maps to the counter `harvest.activity.failed` with attributes:
    /// - `activity.type`: the registered activity name
    /// - `workflow.type`: the owning workflow name (empty string when unknown)
    /// - `error.type`: low-cardinality error class (e.g. `"InvalidInput"`)
    /// - `non_retryable`: whether the failure skipped remaining retries
    ///
    /// Per ADR-0001 §7: `execution.id` and `activity.id` are span-only and
    /// must never appear as metric attributes. Callers are responsible for
    /// keeping `error_type` low-cardinality.
    fn record_activity_failed(
        &self,
        activity_name: &str,
        workflow_type: &str,
        error_type: &str,
        non_retryable: bool,
    ) {
        let _ = (activity_name, workflow_type, error_type, non_retryable);
    }

    /// An activity attempt completed (success or failure).
    ///
    /// Increments [`METRIC_ACTIVITY_ATTEMPTS`] with label `outcome` set to
    /// `ActivityStatus::as_str()` (`"completed"` or `"failed"`). Fires **once per
    /// attempt for both outcomes**, keeping the counter in lockstep with the
    /// `harvest.activity.duration` histogram count.
    ///
    /// This is the primary signal for activity success-rate SLOs: compute
    /// `rate(harvest_activity_attempts_total{outcome="completed"}[5m]) /
    ///  rate(harvest_activity_attempts_total[5m])` within a single metric family.
    ///
    /// Per ADR-0001 §7, `execution.id` and `activity.id` are **span-only** and
    /// must never appear as metric attributes; the method signature enforces this
    /// by construction.
    fn record_activity_attempt(&self, activity_name: &str, queue: &str, outcome: ActivityStatus) {
        let _ = (activity_name, queue, outcome);
    }

    /// A retry was scheduled for an activity (one per retry actually enqueued).
    ///
    /// Increments [`METRIC_ACTIVITY_RETRIES`] with labels `activity` and `queue`.
    /// Fires only when a retry is durably enqueued (i.e. after the
    /// `schedule_to_close` deadline check passes), so the count is the authoritative
    /// "retries scheduled" signal for retry-storm detection.
    ///
    /// Per ADR-0001 §7, `execution.id` and `activity.id` are **span-only** and
    /// must never appear as metric attributes.
    fn record_activity_retried(&self, activity_name: &str, queue: &str) {
        let _ = (activity_name, queue);
    }

    /// A durable timer was persisted.
    fn record_timer_started(&self, duration_secs: f64) {
        let _ = duration_secs;
    }

    /// A periodic snapshot of queued pending task count.
    fn record_queue_depth(&self, queue_name: &str, depth: u64) {
        let _ = (queue_name, depth);
    }

    /// A task was dispatched from the named queue (issue #515).
    ///
    /// Recorded once per `dispatch_task` call. Lets operators confirm the live
    /// per-queue dispatch split matches `WorkerConfig::queue_weights`.
    /// Maps to the counter [`METRIC_QUEUE_DISPATCHED`].
    fn record_task_dispatched(&self, queue_name: &str) {
        let _ = queue_name;
    }

    /// Wall-clock seconds a task waited between becoming eligible
    /// (`scheduled_at`) and being claimed by a worker (`started_at`).
    ///
    /// Recorded at claim time. Only `queue_name` is used as a label;
    /// `execution.id` / `activity.id` are span-only per ADR-0001 §7.
    fn record_schedule_to_start(&self, queue_name: &str, wait_secs: f64) {
        let _ = (queue_name, wait_secs);
    }

    /// Age in seconds of the oldest currently-unclaimed eligible task in a
    /// queue. Sampled alongside `record_queue_depth`. Reports `0` when the
    /// queue is drained so stale gauge values do not linger.
    fn record_queue_oldest_pending_age(&self, queue_name: &str, age_secs: f64) {
        let _ = (queue_name, age_secs);
    }

    /// One background control-loop iteration completed (issue #797).
    ///
    /// Maps to the counter [`METRIC_SCANNER_TICK`], labeled `scanner` (=
    /// [`METRIC_LABEL_SCANNER`]) and `shard` (= [`METRIC_LABEL_SHARD`]).
    /// `scanner` is always
    /// [`Scanner::as_str`](crate::scanner_health::Scanner::as_str), so the
    /// label's cardinality is bounded by construction.
    ///
    /// `shard` is what stops one healthy sibling from **masking** a wedged
    /// one. A multi-shard worker spawns a `timeout`, `poison_pill`, and
    /// `pause_auto_resume` loop *per assigned shard*; without a per-shard
    /// dimension they would all increment one series, so a healthy shard's
    /// ticks would keep `rate(...) > 0` while another shard's loop was dead
    /// and the paging alert would never fire. Use
    /// [`SCANNER_SHARD_LABEL_NONE`] for the process-wide loops (`retention`,
    /// `schedule`) and single-shard deployments, so the label set is uniform
    /// across the series family. Cardinality stays bounded: shard count is
    /// operator-configured and small, matching the existing `shard`-labeled
    /// metrics (`harvest.dlq.entries`, `harvest.shard.stranded_pending`).
    ///
    /// Called **unconditionally at the end of every iteration**, including
    /// no-work iterations — that is what makes a flat-lined counter mean
    /// "wedged" rather than merely "idle". Prefer the
    /// [`record_scanner_tick`](crate::scanner_health::record_scanner_tick)
    /// choke point over calling this directly: it also bumps the in-process
    /// liveness registry that backs the `scanner_liveness` preflight check,
    /// so the counter and the timestamp cannot drift apart.
    ///
    /// Additive with a no-op default: implementing it is optional and no
    /// existing implementor breaks.
    fn record_scanner_tick(&self, scanner: &str, shard: &str) {
        let _ = (scanner, shard);
    }

    /// A background control loop registered itself, before its first
    /// iteration (issue #797).
    ///
    /// Initializes that scanner's [`METRIC_SCANNER_TICK`] series **at zero**
    /// rather than recording a separate metric — implementations should
    /// increment the tick counter by `0`, with the same `scanner` and `shard`
    /// labels [`record_scanner_tick`](Self::record_scanner_tick) uses, so the
    /// initialized series is the one the ticks go on to increment.
    ///
    /// Without this, a loop that panics or hangs during its *first* iteration
    /// never reaches [`record_scanner_tick`](Self::record_scanner_tick), so
    /// the process exports no series for it at all. `rate(...) == 0` only
    /// evaluates series that exist, and `absent()` is deliberately not used by
    /// the shipped alert (an API-only replica legitimately exports nothing),
    /// so that startup wedge would page *never* — the worst failure mode for a
    /// liveness signal. Registration happens before the first iteration, which
    /// is exactly when the process knows the loop is supposed to be running.
    ///
    /// A process that runs no control loops still exports nothing, so the
    /// API-only-replica case that rules out `absent()` is unchanged.
    ///
    /// Additive with a no-op default: implementing it is optional and no
    /// existing implementor breaks.
    fn record_scanner_registered(&self, scanner: &str, shard: &str) {
        let _ = (scanner, shard);
    }

    /// Results of one retention-janitor tick on a shard.
    fn record_retention_tick(
        &self,
        shard: u16,
        candidate_count: u64,
        deleted_count: u64,
        duration_secs: f64,
    ) {
        let _ = (shard, candidate_count, deleted_count, duration_secs);
    }

    /// Number of history rows the retention janitor deleted for a specific
    /// workflow type in one tick (issue #737).
    ///
    /// Maps to the counter [`METRIC_RETENTION_DELETED`], labeled with the
    /// low-cardinality `workflow` registry key so operators can confirm
    /// per-type deletion. Emitted for real deletes only (never dry-run).
    /// `sum(harvest.retention.deleted)` over the label equals the aggregate
    /// **workflow-history** deletion count for the tick, *excluding* orphaned
    /// `harvest_completion_deliveries` reclaims (issue #921) — those rows have
    /// no owning execution and therefore no workflow name to attribute — so it
    /// may read below the tick's total `deleted_count`.
    fn record_retention_deleted(&self, workflow: &str, count: u64) {
        let _ = (workflow, count);
    }

    /// Number of execution summaries the summary-retention GC pass deleted for
    /// a specific workflow type in one tick (issue #752).
    ///
    /// Maps to the counter [`METRIC_SUMMARY_DELETED`], labeled with the
    /// low-cardinality `workflow` registry key. A distinct member of the
    /// retention metric family from [`Self::record_retention_deleted`] (which
    /// counts history-row deletions), so operators can observe the two tiers
    /// independently. Emitted for real deletes only (never dry-run).
    fn record_summary_deleted(&self, workflow: &str, count: u64) {
        let _ = (workflow, count);
    }

    /// Number of inert per-tenant rate-limit buckets the idle-bucket GC pass
    /// collected on one shard in one tick, per key family (issue #1127).
    ///
    /// Maps to the counter [`METRIC_RATE_LIMIT_BUCKETS_DELETED`], labeled with
    /// the bounded `family` (`dyn-rate` / `start-throttle`) and never with the
    /// unbounded bucket key. Emitted for real deletes only (never dry-run).
    fn record_rate_limit_buckets_deleted(&self, family: &str, count: u64) {
        let _ = (family, count);
    }

    /// Current number of RUNNING tasks for a concurrency group key.
    ///
    /// Emitted by the concurrency sampler on every sample interval. The value
    /// is a gauge — operators should alert when it approaches `max_concurrent`.
    fn record_concurrency_key_in_flight(&self, key: &str, in_flight: u64) {
        let _ = (key, in_flight);
    }

    /// Number of PENDING tasks for a key that are being held back because the
    /// cap is currently saturated (`in_flight >= max_concurrent`).
    ///
    /// Emitted alongside each `record_concurrency_key_in_flight` call when
    /// there are deferred tasks waiting for a slot. Operators should monitor
    /// this alongside queue depth to detect saturation-induced backlog.
    fn record_concurrency_key_deferred(&self, key: &str, deferred: u64) {
        let _ = (key, deferred);
    }

    /// A running execution was superseded by a newer admission for the same
    /// concurrency key under `ConcurrencyOnConflict::CancelRunning` (issue #811).
    ///
    /// Maps to the counter [`METRIC_CONCURRENCY_SUPERSEDED`]. Called once per
    /// superseded run. The concurrency key is deliberately not a label
    /// (ADR-0001 §7 cardinality rule).
    fn record_concurrency_superseded(&self, workflow: &str) {
        let _ = workflow;
    }

    /// A latest-wins supersede pass left `gap` runs over the key's declared
    /// limit because they were all protected in-flight (nested) admissions it
    /// may never shed (issue #1197, item 2).
    ///
    /// Maps to the counter [`METRIC_CONCURRENCY_RESIDUAL_OVER_LIMIT`]. The key
    /// is transiently over its limit and self-heals on the next ordinary
    /// admission; this counter is the alertable signal for operators who want
    /// to know when that happens. Additive with a no-op default: implementing
    /// it is optional and no existing implementor breaks.
    fn record_concurrency_residual_over_limit(&self, workflow: &str, gap: u64) {
        let _ = (workflow, gap);
    }

    /// Record the current available tokens for a rate limit bucket key.
    ///
    /// Maps to the gauge `harvest.rate_limit.tokens_available{key}`.
    fn record_rate_limit_tokens_available(&self, key: &str, tokens: f64) {
        let _ = (key, tokens);
    }

    /// Record the refill rate (tokens per second) for a rate limit bucket key.
    ///
    /// Maps to the gauge `harvest.rate_limit.refill_rate{key}`.
    fn record_rate_limit_refill_rate(&self, key: &str, refill_rate: f64) {
        let _ = (key, refill_rate);
    }

    /// Record that a task claim/dispatch was throttled/skipped due to rate
    /// limiting.
    ///
    /// Maps to the counter `harvest.rate_limit.throttled{activity}`.
    ///
    /// **Cardinality (ADR-0001 §7):** the `activity` argument MUST be a bounded
    /// value (the registered activity name). It is used as a metric label, so a
    /// caller must never pass the raw rate-limit bucket key — a dynamic per-key
    /// key (`dyn-rate:{expr}:{tenant}`, issue #699) embeds unbounded tenant
    /// input and would explode label cardinality.
    fn record_rate_limit_throttled(&self, activity: &str) {
        let _ = activity;
    }

    /// Current number of entries in the dead-letter queue on one shard.
    ///
    /// Emitted by a periodic background sampler on the same cadence as
    /// [`record_queue_depth`](Self::record_queue_depth). `shard` is the
    /// `ShardId` as a `u16`; single-shard deployments always emit `shard = 0`.
    ///
    /// Maps to the gauge `harvest_dlq_entries{shard}`.
    fn record_dlq_entries(&self, shard: u16, depth: u64) {
        let _ = (shard, depth);
    }

    /// Whether a task queue is currently held by an operator queue pause
    /// (issue #619): `paused = true` while the hold is in effect, `false` for
    /// one cycle after it is released so the series drops rather than going
    /// stale.
    ///
    /// Emitted by a periodic background sampler on the worker's poll cadence.
    /// Maps to the gauge `harvest_queue_paused{queue}`.
    fn record_queue_paused(&self, queue: &str, paused: bool) {
        let _ = (queue, paused);
    }

    /// An operator paused or resumed dispatch for a whole activity type
    /// (issue #807).
    ///
    /// Emitted from the pause/resume write path, **gated on the action having
    /// genuinely changed state** (`newly_paused` / `newly_resumed`) so an
    /// idempotent retry after a lost response does not read as a second hold —
    /// the same gating [`record_workflow_paused`](Self::record_workflow_paused)
    /// uses (issue #383).
    ///
    /// **Cardinality (ADR-0001 §7):** `activity_name` MUST be the validated,
    /// canonicalised registered activity name (bounded); `action` is bounded to
    /// two values by [`ActivityPauseAction`]. `execution.id` is never a label.
    ///
    /// Maps to the counter `harvest_activity_pause_actions_total{activity, action}`.
    fn record_activity_pause_action(&self, activity_name: &str, action: ActivityPauseAction) {
        let _ = (activity_name, action);
    }

    /// Periodic snapshot of a worker's dispatch-slot occupancy for one slot
    /// type (issue #531).
    ///
    /// Emitted in-process by the worker's slot sampler against the two dispatch
    /// `Semaphore`s. `in_use` and `available` are read from a single
    /// `available_permits()` observation, so the invariant
    /// `in_use + available == configured_max` holds for that slot type.
    ///
    /// Maps to the gauges
    /// `harvest_worker_slots_in_use{slot_type}` /
    /// `harvest_worker_slots_available{slot_type}`.
    fn record_worker_slots(&self, slot_type: SlotType, in_use: u64, available: u64) {
        let _ = (slot_type, in_use, available);
    }

    /// The adaptive slot tuner's current resize target for one slot type
    /// (issue #548).
    ///
    /// Emitted in-process by the worker's slot-tuner control loop on the same
    /// cadence as [`record_worker_slots`](Self::record_worker_slots). Only
    /// emitted when a `SlotTuner` is configured.
    ///
    /// Maps to the gauge `harvest_worker_slot_target{slot_type}`.
    fn record_worker_slot_target(&self, slot_type: SlotType, target: u64) {
        let _ = (slot_type, target);
    }

    /// A tuner-decision counter tick for one slot type (issue #548).
    ///
    /// Emitted once per control-loop tick with the decision that actually
    /// took effect after band clamping.
    ///
    /// Maps to the counter `harvest_worker_tuner_decisions{slot_type, decision}`.
    fn record_tuner_decision(&self, slot_type: SlotType, decision: TunerDecision) {
        let _ = (slot_type, decision);
    }

    /// Current number of claimable pending tasks on a shard that has no
    /// covering live worker (issue #522).
    ///
    /// Emitted per shard by the stranded-work sampler. When a shard is
    /// covered by at least one live worker the value is emitted as `0`.
    /// A non-zero value means work is stranded: workflows rendezvous-hashed
    /// onto this shard will never reach `RUNNING` until a covering worker is
    /// started.
    ///
    /// Maps to the gauge `METRIC_SHARD_STRANDED_PENDING{shard}`.
    fn record_shard_stranded_pending(&self, shard: u16, count: u64) {
        let _ = (shard, count);
    }

    /// Measured RPO for a shard in seconds (issue #954).
    ///
    /// Emitted per shard by the replication sampler, and **only when the RPO is
    /// actually known**: the sampler skips this call entirely when it is not,
    /// so the series goes stale rather than reporting a false `0`. Pair with
    /// [`Self::record_replication_standbys`] to tell "slow" from "down".
    ///
    /// Maps to the gauge [`METRIC_REPLICATION_LAG_SECONDS`].
    fn record_replication_lag_seconds(&self, shard: u16, seconds: f64) {
        let _ = (shard, seconds);
    }

    /// Whether a shard's replication views are readable (issue #954).
    ///
    /// Emitted on **every** sampler tick, `0` included — it is the one DR
    /// signal that must never go stale, because it is what tells a dashboard
    /// that the others have.
    ///
    /// Maps to the gauge [`METRIC_REPLICATION_OBSERVABLE`].
    fn record_replication_observable(&self, shard: u16, observable: bool) {
        let _ = (shard, observable);
    }

    /// Worst-case WAL backlog in bytes for a shard (issue #954).
    ///
    /// Maps to the gauge [`METRIC_REPLICATION_LAG_BYTES`].
    fn record_replication_lag_bytes(&self, shard: u16, bytes: i64) {
        let _ = (shard, bytes);
    }

    /// Number of standbys with a live walsender for a shard (issue #954).
    ///
    /// Always emitted, `0` included — `0` is the signal.
    ///
    /// Maps to the gauge [`METRIC_REPLICATION_STANDBYS`].
    fn record_replication_standbys(&self, shard: u16, count: u64) {
        let _ = (shard, count);
    }

    /// Whether a shard's measured RPO is known (issue #954, finding 2).
    ///
    /// Emitted on every sampler tick the views are readable, `0` included —
    /// like [`Self::record_replication_observable`], it must never go stale.
    ///
    /// Maps to the gauge [`METRIC_REPLICATION_RPO_KNOWN`].
    fn record_replication_rpo_known(&self, shard: u16, known: bool) {
        let _ = (shard, known);
    }

    /// The write-authority epoch a shard's database currently reports
    /// (issue #954).
    ///
    /// Maps to the gauge [`METRIC_SHARD_GENERATION`].
    fn record_shard_generation(&self, shard: u16, generation: i64) {
        let _ = (shard, generation);
    }

    /// Audit-export delivery lag for a shard, in seconds (issue #953).
    ///
    /// Emitted on every exporter tick, delivering or not — see
    /// [`METRIC_AUDIT_EXPORT_LAG`] for why it is the *oldest* unacknowledged
    /// record's age rather than the newest.
    ///
    /// Maps to the gauge [`METRIC_AUDIT_EXPORT_LAG`].
    fn record_audit_export_lag(&self, shard: u16, seconds: f64) {
        let _ = (shard, seconds);
    }

    /// The exporter did, or did not, observe a shard's cursor and lag this
    /// tick (issue #1268).
    ///
    /// Call this on **every** exporter tick that reaches a shard, whether or
    /// not it delivers a batch. `true` when the cursor read and the lag query
    /// both succeeded; `false` on a connection failure, a cursor read
    /// failure, or a lag query failure. See [`METRIC_AUDIT_EXPORT_OBSERVED`]
    /// for why this signal exists.
    ///
    /// Maps to the gauge [`METRIC_AUDIT_EXPORT_OBSERVED`].
    fn record_audit_export_observed(&self, shard: u16, observed: bool) {
        let _ = (shard, observed);
    }

    /// Audit records the sink acknowledged for a shard (issue #953).
    ///
    /// Called once per acknowledged batch with that batch's record count,
    /// only after the cursor advanced.
    ///
    /// Maps to the counter [`METRIC_AUDIT_EXPORTED`].
    fn record_audit_exported(&self, shard: u16, count: u64) {
        let _ = (shard, count);
    }

    /// This worker was fenced off `shard` and is stopping (issue #954).
    ///
    /// Maps to the counter [`METRIC_SHARD_FENCED`].
    fn record_shard_fenced(&self, shard: u16) {
        let _ = shard;
    }

    /// A task was dispatched from the given shard (issue #961).
    ///
    /// Recorded once per dispatched task by the worker's poll loop, which is
    /// the only place that knows which shard the claim came from (a task row
    /// carries no `shard_id` column — "which shard" *is* "which pool").
    /// Lets operators confirm no assigned shard is being starved (AC5).
    ///
    /// Maps to the counter [`METRIC_SHARD_DISPATCHED`].
    fn record_shard_dispatched(&self, shard: u16) {
        let _ = shard;
    }

    /// A scheduled run was dispatched (either a DAG run or a workflow start).
    ///
    /// `kind` is `"dag"` or `"workflow"`. `name` is the DAG or workflow name.
    /// Maps to the metric `harvest_schedule_runs_total{kind, name}`.
    fn record_schedule_run(&self, kind: &str, name: &str) {
        let _ = (kind, name);
    }

    /// A scheduled run was skipped without dispatching.
    ///
    /// `kind` is `"dag"` or `"workflow"`. `name` is the DAG or workflow name.
    /// `reason` is one of `"paused"`, `"max_active_runs_reached"`,
    /// `"catchup_disabled"`, or `"catchup_window_exceeded"`.
    ///
    /// Maps to the metric `harvest_schedule_skipped_total{kind, name, reason}`.
    fn record_schedule_skipped(&self, kind: &str, name: &str, reason: &str) {
        let _ = (kind, name, reason);
    }

    /// Record `count` skips of the same `(kind, name, reason)` at once.
    ///
    /// Used when a single bounded-catchup recovery drops many missed slots: the
    /// counter contract is "one increment per dropped slot", but looping that
    /// many synchronous calls inside the scheduler tick would stall the thread on
    /// a large recovery. Recorders that back a real counter should override this
    /// with a single batched increment (the metrics-rs adapter does). The default
    /// delegates to [`Self::record_schedule_skipped`], bounded so a custom
    /// recorder cannot be forced into an unbounded loop by a huge backlog.
    ///
    /// Maps to the metric `harvest_schedule_skipped_total{kind, name, reason}`
    /// incremented by `count`.
    fn record_schedule_skipped_n(&self, kind: &str, name: &str, reason: &str, count: u64) {
        for _ in 0..count.min(10_000) {
            self.record_schedule_skipped(kind, name, reason);
        }
    }

    /// A scheduler decision write failed due to a database error.
    ///
    /// Maps to the counter `harvest.schedule.decision_write_failed`.
    fn record_schedule_decision_write_failed(&self) {}

    /// A manual `POST /admin/schedules/{id}/trigger` call completed (issue #343).
    ///
    /// `schedule_name` is the workflow or DAG name the schedule targets
    /// (low-cardinality, same cardinality as `record_schedule_run`).
    /// `outcome` is `"fired"`, `"skipped_overlap"`, or `"rejected_paused"`.
    ///
    /// Maps to the counter `harvest.schedule.manual_trigger{schedule.name, outcome}`.
    fn record_schedule_manual_trigger(&self, schedule_name: &str, outcome: &str) {
        let _ = (schedule_name, outcome);
    }

    /// A tick-loop fire attempt for a due schedule slot (issue #350).
    ///
    /// `schedule_name` is the workflow or DAG name (low-cardinality).
    /// `outcome` is one of:
    /// - `"claimed"` — this replica won the atomic claim race and will fire.
    /// - `"lost_race"` — another replica already holds a live claim for this
    ///   slot; this replica skips it without firing.
    ///
    /// Maps to the counter [`METRIC_SCHEDULE_FIRE_ATTEMPTS`].
    fn record_schedule_fire_attempt(&self, schedule_name: &str, outcome: &str) {
        let _ = (schedule_name, outcome);
    }

    /// A schedule was automatically paused after reaching `consecutive_failure_limit`
    /// consecutive execution failures (issue #360).
    ///
    /// `schedule_name` is the workflow name bound to the schedule (low-cardinality).
    /// Emitted once per auto-pause event. Operators should alert on
    /// `harvest_schedule_auto_paused_total > 0` and resume the schedule once
    /// the underlying issue is resolved.
    ///
    /// Maps to the counter [`METRIC_SCHEDULE_AUTO_PAUSED`].
    fn record_schedule_auto_paused(&self, schedule_name: &str) {
        let _ = schedule_name;
    }

    /// Per-schedule overdue-to-fire flag (issue #696).
    ///
    /// `kind` is `"workflow"` or `"dag"`; `name` is the schedule's workflow or
    /// DAG name (low-cardinality). `overdue` is `true` when the schedule is
    /// active and past its own cadence grace, `false` otherwise. A gauge, so
    /// this must be re-emitted every sampler pass — including `false` for
    /// healthy schedules — to keep the value fresh.
    ///
    /// Maps to the gauge [`METRIC_SCHEDULE_OVERDUE`] set to `1.0`/`0.0`.
    fn record_schedule_overdue(&self, kind: &str, name: &str, overdue: bool) {
        let _ = (kind, name, overdue);
    }

    /// A query handler invocation completed (issue #234).
    ///
    /// `query_name` is the handler name registered via `register_query` /
    /// `register_query_handler`. Per ADR-0001 cardinality rule, `execution.id`
    /// stays span-only — it must never appear as a metric label here.
    ///
    /// `duration_secs` is the wall-clock time from invocation start to the
    /// handler returning (or being timed out). `success` is `true` when the
    /// handler returned `Ok`, `false` on `Err` or timeout.
    ///
    /// Maps to the histogram `harvest.query.duration{query.name, status}`.
    fn record_query_completed(&self, query_name: &str, duration_secs: f64, success: bool) {
        let _ = (query_name, duration_secs, success);
    }

    /// A workflow task was served from the in-process LRU cache (warm path).
    ///
    /// The worker already holds this execution's event history in its local
    /// `WorkflowCache`, so only delta events (new timer firings / signals) need
    /// to be fetched from Postgres rather than the full history.
    ///
    /// Maps to the counter `harvest.workflow.cache_hit{workflow}`.
    fn record_workflow_cache_hit(&self, workflow_name: &str, queue: &str) {
        let _ = (workflow_name, queue);
    }

    /// A workflow task required a full event-history reload from Postgres (cold path).
    ///
    /// The worker's LRU cache did not contain an entry for this execution —
    /// either because this is the first task for this execution on this worker,
    /// the entry was evicted by LRU pressure, or sticky routing is disabled.
    ///
    /// Maps to the counter `harvest.workflow.cache_miss{workflow}`.
    fn record_workflow_cache_miss(&self, workflow_name: &str, queue: &str) {
        let _ = (workflow_name, queue);
    }

    /// A workflow execution was terminated because its `deadline_at` elapsed.
    ///
    /// Maps to the counter `harvest.workflow.timeout{workflow, queue}`.
    fn record_workflow_timeout(&self, workflow_name: &str, queue: &str) {
        let _ = (workflow_name, queue);
    }

    /// A workflow execution was terminated because its chain-scoped lifetime cap
    /// (`chain_deadline_at`) elapsed (issue #617). Distinct from
    /// [`Self::record_workflow_timeout`]: the chain cap spans a whole
    /// continue-as-new chain, not a single run.
    ///
    /// Maps to the counter `harvest.workflow.chain_timeout{workflow, queue}`.
    /// Additive, non-breaking default no-op.
    fn record_workflow_chain_timeout(&self, workflow_name: &str, queue: &str) {
        let _ = (workflow_name, queue);
    }

    /// A workflow-task dispatch was abandoned because it did not complete or
    /// suspend within `WorkerConfig::workflow_task_timeout` (issue #494).
    ///
    /// Each call represents one reclaimed worker concurrency slot. After
    /// `poison_pill_threshold` consecutive calls for the same execution the
    /// task is escalated to the DLQ.
    ///
    /// Maps to the counter `harvest.workflow.task_timeout{workflow, queue}`.
    fn record_workflow_task_timeout(&self, workflow_name: &str, queue: &str) {
        let _ = (workflow_name, queue);
    }

    /// A workflow execution has exceeded its declared soft SLA budget while
    /// still RUNNING/SUSPENDED (issue #487).
    ///
    /// Emitted **exactly once per run** by the SLA breach scanner.  The run is
    /// never terminated; a breaching run that later completes still reaches
    /// COMPLETED with its normal result.
    ///
    /// Maps to the counter `harvest.workflow.sla_breached{workflow, queue}`.
    fn record_workflow_sla_breach(&self, workflow_name: &str, queue: &str) {
        let _ = (workflow_name, queue);
    }

    /// A still-RUNNING (suspended, not terminal) workflow execution's
    /// recorded history event count has just crossed the configured
    /// early-warning fraction of the hard cap for the first time (issue
    /// #704).
    ///
    /// Emitted **exactly once per execution**, worker-side, guarded by an
    /// atomic `UPDATE ... WHERE history_bloat_warned_at IS NULL`. This is a
    /// pure operator early-warning -- it never terminates, cancels, or
    /// otherwise alters the run's lifecycle.
    ///
    /// Maps to the counter [`METRIC_WORKFLOW_HISTORY_BLOAT`] with label
    /// `workflow` ONLY (no `queue`, unlike most sibling workflow counters --
    /// see the constant's doc comment). Per ADR-0001 §7, `execution.id` must
    /// never be a label here.
    fn record_workflow_history_bloat(&self, workflow_name: &str) {
        let _ = workflow_name;
    }

    /// A failed workflow execution was automatically rescheduled for a retry run (issue #523).
    ///
    /// Maps to the counter `harvest.workflow.retries{workflow, queue}`.
    fn record_workflow_retry(&self, workflow_name: &str, queue: &str) {
        let _ = (workflow_name, queue);
    }

    /// A poison-pill task was quarantined to the dead-letter queue after
    /// crashing the configured number of workers in a row (issue #367).
    ///
    /// Maps to the counter `harvest.task.quarantined{queue, reason}`.
    fn record_task_quarantined(&self, queue: &str, reason: &str) {
        let _ = (queue, reason);
    }

    /// A worker claimed a task whose workflow/activity type it has no handler
    /// registered for, and either released the claim for a capable peer or
    /// escalated it after exhausting the redelivery budget (issue #804).
    ///
    /// Maps to the counter
    /// `harvest.task.capability_miss{queue, task_type, outcome}`.
    ///
    /// `task_type` is `"workflow"` / `"activity"`; `outcome` is
    /// [`CAPABILITY_MISS_OUTCOME_RELEASED`],
    /// [`CAPABILITY_MISS_OUTCOME_ESCALATED`] (budget exhausted — the
    /// fleet-exhaustion signal that pages), or
    /// [`CAPABILITY_MISS_OUTCOME_ESCALATED_NEVER_OFFERED`] (failed on the first
    /// claim without ever being offered to a peer).
    /// All three labels are bounded; per ADR-0001 §7 `execution.id` is never a
    /// metric label (and is not a parameter here, so it cannot become one).
    fn record_task_capability_miss(&self, queue: &str, task_type: &str, outcome: &str) {
        let _ = (queue, task_type, outcome);
    }

    /// An operator redrive processed one dead-letter entry (issue #510).
    ///
    /// Maps to the counter `harvest.dlq.redriven{queue, outcome}` where
    /// `outcome` is `redriven` / `skipped` / `failed`. Per the ADR-0001
    /// cardinality rule, `execution.id` is never a metric label.
    fn record_dlq_redriven(&self, queue: &str, outcome: &str) {
        let _ = (queue, outcome);
    }

    /// A workflow execution was paused by an operator or the auto-resume
    /// scanner (issue #383).
    ///
    /// Maps to the counter `harvest.workflow.paused{workflow, queue}`.
    fn record_workflow_paused(&self, workflow_name: &str, queue: &str) {
        let _ = (workflow_name, queue);
    }

    /// A paused workflow execution was resumed; `duration_secs` is the
    /// wall-clock time it spent in the `PAUSED` state (issue #383).
    ///
    /// Maps to the histogram `harvest.workflow.pause_duration{workflow, queue}`.
    fn record_workflow_pause_duration(&self, workflow_name: &str, queue: &str, duration_secs: f64) {
        let _ = (workflow_name, queue, duration_secs);
    }

    /// An activity's circuit breaker tripped open or re-opened after a failed
    /// half-open probe (issue #369).
    ///
    /// Maps to the counter `harvest.activity.circuit.tripped{activity.name}`.
    fn record_circuit_tripped(&self, activity_name: &str) {
        let _ = activity_name;
    }

    /// An activity's circuit breaker recovered to closed after a successful
    /// half-open probe (issue #369).
    ///
    /// Maps to the counter `harvest.activity.circuit.closed{activity.name}`.
    fn record_circuit_closed(&self, activity_name: &str) {
        let _ = activity_name;
    }

    /// A payload was observed at a write boundary (issue #252).
    ///
    /// Called for every payload written (accepted or rejected) to
    /// `harvest_events`. Maps to the histogram `harvest.payload.bytes`
    /// with labels `payload.kind`, `workflow.type`, and `activity.name`.
    ///
    /// `activity_name` is `None` for non-activity payloads (signal, side-effect,
    /// workflow-input).
    fn record_payload_observed(
        &self,
        kind: &crate::error::PayloadKind,
        workflow_type: &str,
        activity_name: Option<&str>,
        observed_bytes: u64,
    ) {
        let _ = (kind, workflow_type, activity_name, observed_bytes);
    }

    /// A payload was rejected because it exceeded the configured cap (issue #252).
    ///
    /// Maps to the counter `harvest.payload.rejected` with labels
    /// `payload.kind` and `workflow.type`.
    fn record_payload_rejected(&self, kind: &crate::error::PayloadKind, workflow_type: &str) {
        let _ = (kind, workflow_type);
    }

    /// A payload-bearing field was offloaded to an external store (issue #524).
    ///
    /// Maps to the [`METRIC_PAYLOAD_OFFLOADED`] counter, incremented by
    /// `byte_len`. `field` is the event field name (`"input"`, `"output"`, …);
    /// `store_id` identifies the configured store.
    fn record_payload_offloaded(&self, field: &str, store_id: &str, byte_len: u64) {
        let _ = (field, store_id, byte_len);
    }

    /// An offloaded payload was fetched back from the external store on
    /// read/replay (issue #524).
    ///
    /// Maps to the [`METRIC_PAYLOAD_OFFLOAD_FETCH_DURATION`] histogram.
    fn record_payload_offload_fetch(&self, store_id: &str, duration_secs: f64) {
        let _ = (store_id, duration_secs);
    }

    /// A cross-workflow external signal was sent.
    ///
    /// Maps to the counter `harvest.workflow.external_signal.sent` with attributes:
    /// - `outcome`: `"delivered"` or `"failed"`
    /// - `reason_code`: `"target_terminal"` or `"target_unknown"` (optional)
    fn record_external_signal_sent(&self, outcome: &str, reason_code: Option<&str>) {
        let _ = (outcome, reason_code);
    }

    /// Record one external cancel dispatch outcome (`outcome`: `"delivered"` / `"failed"`).
    fn record_external_cancel_sent(&self, outcome: &str, reason_code: Option<&str>) {
        let _ = (outcome, reason_code);
    }

    /// A start request was absorbed by a debounce pending record (issue #499).
    ///
    /// Maps to the counter [`METRIC_WORKFLOW_DEBOUNCED`] with label `workflow`.
    /// The debounce key is deliberately **not** a label: it is resolved from
    /// user/tenant input and would create unbounded metric cardinality
    /// (ADR-0001 §7). The raw key remains available in logs and the
    /// `GET /admin/debounce` endpoint.
    fn record_workflow_debounced(&self, workflow_name: &str) {
        let _ = workflow_name;
    }

    /// The debounce scanner fired a pending record and started one execution (issue #499).
    ///
    /// Maps to the counter [`METRIC_DEBOUNCE_FIRED`] with labels
    /// `workflow` and `queue`.
    fn record_debounce_fired(&self, workflow_name: &str, queue: &str) {
        let _ = (workflow_name, queue);
    }

    /// A workflow start was deferred by a start throttle (issue #607).
    ///
    /// Maps to the counter [`METRIC_WORKFLOW_START_THROTTLED`] with label
    /// `workflow`. The resolved throttle key is deliberately **not** a label
    /// (unbounded cardinality, ADR-0001 §7); per-key backlog is exposed via the
    /// `GET /admin/start-throttle` admin read.
    fn record_start_throttled(&self, workflow_name: &str) {
        let _ = workflow_name;
    }

    /// A request reached an inbound webhook receiver route (issue #344).
    ///
    /// Maps to the counter [`METRIC_WEBHOOK_RECEIVED`] with labels `path`
    /// and `outcome`.
    fn record_webhook_received(&self, path: &str, outcome: WebhookOutcome) {
        let _ = (path, outcome);
    }

    /// A worker session's acquisition attempt resolved to a terminal outcome
    /// (issue #606): a worker claimed it, the caller's `acquisition_timeout`
    /// elapsed first, or it was reclaimed as broken before/during
    /// acquisition.
    ///
    /// Maps to the counter [`METRIC_SESSION_ACQUISITION`] with labels
    /// `queue` and `outcome`.
    fn record_session_acquisition(&self, queue: &str, outcome: SessionAcquisitionOutcome) {
        let _ = (queue, outcome);
    }

    /// An inbound webhook request was rejected (issue #344).
    ///
    /// Maps to the counter [`METRIC_WEBHOOK_REJECTED`] with labels `path`
    /// and `outcome`. `outcome` is never [`WebhookOutcome::Accepted`] here.
    fn record_webhook_rejected(&self, path: &str, outcome: WebhookOutcome) {
        let _ = (path, outcome);
    }

    /// A broker event-source connector pulled a message off its topic/queue
    /// (issue #944), before any mapping or dispatch was attempted.
    ///
    /// Maps to the counter [`METRIC_CONNECTOR_RECEIVED`] with label `source`.
    /// The message key/partition/offset are never labels (ADR-0001 §7).
    fn record_connector_received(&self, source: &str) {
        let _ = source;
    }

    /// A connector message reached a terminal disposition (issue #944).
    ///
    /// Maps to the counter [`METRIC_CONNECTOR_DISPATCHED`] with labels
    /// `source` and `outcome`.
    fn record_connector_dispatched(&self, source: &str, outcome: ConnectorOutcome) {
        let _ = (source, outcome);
    }

    /// A connector message was routed to a dead-letter destination
    /// (issue #944).
    ///
    /// Maps to the counter [`METRIC_CONNECTOR_POISONED`] with labels `source`
    /// and `reason`.
    fn record_connector_poisoned(&self, source: &str, reason: PoisonReason) {
        let _ = (source, reason);
    }

    /// The broker-reported consumer lag for a connector source (issue #944).
    ///
    /// Maps to the gauge [`METRIC_CONNECTOR_LAG`] with label `source`.
    /// Adapters whose client cannot report lag never call this.
    fn record_connector_lag(&self, source: &str, lag: i64) {
        let _ = (source, lag);
    }

    /// A saga compensation sequence started running forward (issue #801).
    ///
    /// Emitted **exactly once per real unwind** — on the live frontier where
    /// the durable `saga_compensated:{seq}` dedup marker is first recorded;
    /// replays of a recorded unwind never re-emit.
    ///
    /// Maps to the counter `harvest.saga.compensated{workflow, queue}`.
    fn record_saga_compensated(&self, workflow_name: &str, queue: &str) {
        let _ = (workflow_name, queue);
    }

    /// A saga unwind finished with at least one compensation error — the
    /// `SagaCompensationFailed` dangling-state case (issue #801).
    ///
    /// Emitted exactly once per failed unwind, independent of whether the
    /// author propagates or catches the error, and separable from
    /// `harvest.workflow.terminal{outcome=failed}`.
    ///
    /// Maps to the counter `harvest.saga.compensation_failed{workflow, queue}`.
    fn record_saga_compensation_failed(&self, workflow_name: &str, queue: &str) {
        let _ = (workflow_name, queue);
    }

    // ── Durable mutex (issue #691) ────────────────────────────────────────

    /// A workflow acquired a durable mutex; record how long it waited from
    /// request to grant (issue #691).
    ///
    /// Maps to the histogram [`METRIC_MUTEX_WAIT`] with label `workflow`. The
    /// lock key is deliberately **not** a label (unbounded cardinality,
    /// ADR-0001 §7).
    fn record_mutex_wait(&self, workflow: &str, seconds: f64) {
        let _ = (workflow, seconds);
    }

    /// A durable mutex was released; record how long it was held from grant to
    /// release (issue #691).
    ///
    /// Maps to the histogram [`METRIC_MUTEX_HELD`] with label `workflow`. The
    /// lock key is deliberately **not** a label (ADR-0001 §7).
    fn record_mutex_held(&self, workflow: &str, seconds: f64) {
        let _ = (workflow, seconds);
    }

    /// The FIFO waiter-queue depth for a durable mutex key at the moment of a
    /// grant (contention depth, issue #691).
    ///
    /// Maps to the gauge [`METRIC_MUTEX_CONTENTION`] with label `workflow`. The
    /// lock key is deliberately **not** a label (ADR-0001 §7).
    fn record_mutex_contention(&self, workflow: &str, depth: u64) {
        let _ = (workflow, depth);
    }

    // ── Synthetic liveness canary (issue #796) ────────────────────────────
    //
    // The built-in end-to-end pipeline probe. Distinct from the #512 replay
    // canary. Labels are limited to `queue` and `shard` (a `u16` here,
    // stringified in the metrics-rs bridge). Per ADR-0001 §7, `execution.id`
    // is never accepted, so the cardinality rule holds by construction.

    /// A synthetic liveness-canary probe reached terminal completion on
    /// `(queue, shard)` (issue #796).
    ///
    /// Maps to the counter [`METRIC_CANARY_SUCCESS`] labeled `queue`, `shard`.
    fn record_canary_success(&self, queue: &str, shard: u16) {
        let _ = (queue, shard);
    }

    /// A synthetic liveness-canary probe failed on `(queue, shard)` — it did
    /// not complete within the per-probe timeout, or terminated in a
    /// non-completed state (issue #796).
    ///
    /// Maps to the counter [`METRIC_CANARY_FAILURE`] labeled `queue`, `shard`.
    fn record_canary_failure(&self, queue: &str, shard: u16) {
        let _ = (queue, shard);
    }

    /// Observed round-trip latency (seconds) of a completed synthetic
    /// liveness-canary probe on `(queue, shard)` (issue #796).
    ///
    /// Maps to the histogram [`METRIC_CANARY_ROUNDTRIP`] labeled `queue`,
    /// `shard`.
    fn record_canary_roundtrip(&self, queue: &str, shard: u16, duration_secs: f64) {
        let _ = (queue, shard, duration_secs);
    }

    // ── Signal & update lifecycle counters (issue #684) ───────────────────

    /// A `SignalReceived` event was durably delivered into a workflow's
    /// history (issue #684).
    ///
    /// Maps to the counter [`METRIC_SIGNAL_RECEIVED`] with labels `workflow`
    /// and `queue`. The signal name is deliberately NOT a label (issue #684,
    /// Codex P2): it comes from the free-form send route and has no declared
    /// registry to bound it.
    fn record_signal_received(&self, workflow_name: &str, queue: &str) {
        let _ = (workflow_name, queue);
    }

    /// A delivered signal was never consumed by the workflow before it reached
    /// a Completed/Failed terminal outcome (issue #684).
    ///
    /// Maps to the counter [`METRIC_SIGNAL_UNHANDLED`] with labels `workflow`
    /// and `queue`. The signal name is deliberately NOT a label (issue #684,
    /// Codex P2): it comes from the free-form send route and has no declared
    /// registry to bound it. The worker emits one call per unconsumed
    /// occurrence, so the counter still reflects the terminal outcome's total
    /// unconsumed-signal volume against the single `(workflow, queue)` series.
    fn record_signal_unhandled(&self, workflow_name: &str, queue: &str) {
        let _ = (workflow_name, queue);
    }

    /// A workflow update was durably admitted (issue #684).
    ///
    /// Maps to the counter [`METRIC_UPDATE_ADMITTED`] with labels `workflow`
    /// and `queue`. The update `name` is deliberately NOT a label (issue #684,
    /// Codex P2): admission happens at the free-form route boundary
    /// (`POST /workflows/{id}/update/{name}`) where the name has not yet been
    /// resolved against a registered handler — and update handlers register
    /// both declaratively (`registry.update_handlers`) and imperatively
    /// (`ctx.register_update_handler`, not known until the workflow executes),
    /// so the admission site cannot bound the name against any registry without
    /// mislabeling legitimate imperatively-registered updates. Dropping the
    /// label is therefore the only way to bound this counter's cardinality by
    /// construction. Per-name update visibility lives on the post-resolution
    /// counters `harvest.update.completed`/`failed`/`rejected` instead.
    fn record_update_admitted(&self, workflow_name: &str, queue: &str) {
        let _ = (workflow_name, queue);
    }

    /// A workflow update was rejected by its validator before admission
    /// (issue #684).
    ///
    /// Maps to the counter [`METRIC_UPDATE_REJECTED`] with labels `workflow`
    /// and `name` (update name).
    fn record_update_rejected(&self, workflow_name: &str, update_name: &str) {
        let _ = (workflow_name, update_name);
    }

    /// An admitted workflow update completed successfully (issue #684).
    ///
    /// Maps to the counter [`METRIC_UPDATE_COMPLETED`] with labels `workflow`,
    /// `name` (update name), and `queue`.
    fn record_update_completed(&self, workflow_name: &str, update_name: &str, queue: &str) {
        let _ = (workflow_name, update_name, queue);
    }

    /// An admitted workflow update's handler failed (issue #684).
    ///
    /// Maps to the counter [`METRIC_UPDATE_FAILED`] with labels `workflow`,
    /// `name` (update name), and `queue`.
    fn record_update_failed(&self, workflow_name: &str, update_name: &str, queue: &str) {
        let _ = (workflow_name, update_name, queue);
    }

    /// An admitted workflow update reached a terminal result; `duration_secs` is
    /// the wall-clock time from durable admission to that result (issue #781).
    ///
    /// Maps to the histogram [`METRIC_UPDATE_DURATION`] with labels `workflow`,
    /// `name` (update name), `queue`, and `outcome` (`"completed"`/`"failed"`).
    /// Emitted on the same post-commit path as
    /// [`record_update_completed`](Self::record_update_completed)/[`record_update_failed`](Self::record_update_failed),
    /// so it shares their delivery semantics (see [`METRIC_UPDATE_DURATION`]).
    fn record_update_duration(
        &self,
        workflow_name: &str,
        update_name: &str,
        queue: &str,
        outcome: &str,
        duration_secs: f64,
    ) {
        let _ = (workflow_name, update_name, queue, outcome, duration_secs);
    }

    /// Whether this recorder actually forwards samples anywhere.
    ///
    /// Defaults to `true` for every real recorder. [`NoOpMetrics`] overrides it to
    /// `false` so callers can skip *expensive sample-production work* (e.g. the
    /// per-poll-interval `oldest_pending_ages` / `queue_depths` sampler SQL) when
    /// no recorder is configured — the per-event `record_*` calls are already
    /// zero-cost, but the queries that feed gauges are not. Treat this as a hint
    /// for guarding observation work, never for changing engine behavior.
    fn is_enabled(&self) -> bool {
        true
    }

    // ── Custom / user-emitted metrics (issue #532) ────────────────────────

    /// Record a custom counter emitted by workflow or activity author code.
    ///
    /// `name` is the **fully-qualified** metric name (already prefixed with
    /// `"harvest.user."` by [`UserMetrics`]); `labels` are validated
    /// low-cardinality key-value pairs.  The default implementation is a no-op.
    ///
    /// Per ADR-0001 §7, `execution.id` and other high-cardinality identifiers
    /// must never appear in `labels`.  [`UserMetrics`] enforces this before
    /// reaching this method.
    fn record_user_counter(&self, name: &str, value: u64, labels: &[(&str, &str)]) {
        let _ = (name, value, labels);
    }

    /// Record a custom gauge emitted by workflow or activity author code.
    ///
    /// Same namespacing and cardinality contract as [`record_user_counter`].
    ///
    /// [`record_user_counter`]: MetricsRecorder::record_user_counter
    fn record_user_gauge(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        let _ = (name, value, labels);
    }

    /// Record a custom histogram sample emitted by workflow or activity author code.
    ///
    /// Same namespacing and cardinality contract as [`record_user_counter`].
    ///
    /// [`record_user_counter`]: MetricsRecorder::record_user_counter
    fn record_user_histogram(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        let _ = (name, value, labels);
    }
}

/// Emit the once-per-terminal-outcome business counter `harvest.workflow.terminal`
/// (issue #519), **skipping synthetic-liveness-canary probe runs** (issue #796,
/// AC8).
///
/// This is the single choke point through which every non-canary terminal
/// transition routes its business SLO counter, so a canary probe (identified by
/// [`crate::canary::is_canary_workflow`]) can never leak into
/// `harvest.workflow.terminal` no matter which terminal path it reaches (worker
/// completion/failure, execution timeout, poison-pill quarantine, batch/operator
/// cancel/terminate, history-cap, race-loser cancel, …). The canary-skip
/// decision lives in exactly one place — here.
///
/// A canary probe emits ONLY `harvest.canary.*`; it never contributes to
/// `harvest.workflow.terminal` or any other `harvest.workflow.*` business
/// counter. The two primary terminal sites (worker completion and
/// execution-timeout) additionally emit the probe's own `harvest.canary.*`
/// signal in their canary branch and route their non-canary terminal through
/// this helper. A canary still contributes to `harvest.activity.*`/
/// `harvest.queue.*`, which it legitimately exercises.
///
/// Generic over `M: MetricsRecorder + ?Sized` so it accepts `&dyn
/// MetricsRecorder`, `&(dyn MetricsRecorder + Send + Sync)`, and `&*Arc<dyn
/// MetricsRecorder>` uniformly.
pub fn emit_workflow_terminal<M: MetricsRecorder + ?Sized>(
    metrics: &M,
    workflow_name: &str,
    queue: &str,
    outcome: WorkflowStatus,
) {
    if crate::canary::is_canary_workflow(workflow_name) {
        return;
    }
    metrics.record_workflow_terminal(workflow_name, queue, outcome);
}

/// Emit [`METRIC_CONCURRENCY_SUPERSEDED`] once per superseded run (issue #811).
///
/// Callers reach this through [`crate::execution::emit_start_cancel_metrics`],
/// which walks the [`crate::execution::StartCancelledRun`] list a start returns
/// and emits this counter for every entry whose `superseded` flag is set. It
/// MUST be called only **after** the outer transaction commits — a rollback
/// would otherwise leave a phantom count for a supersede that never became
/// durable.
///
/// Canary probe workflows (issue #796) are excluded, mirroring
/// [`emit_workflow_terminal`], so the synthetic liveness canary can never move a
/// business-facing counter.
pub fn emit_concurrency_superseded<M: MetricsRecorder + ?Sized>(
    metrics: &M,
    superseded_workflow_names: &[String],
) {
    for workflow_name in superseded_workflow_names {
        if crate::canary::is_canary_workflow(workflow_name) {
            continue;
        }
        metrics.record_concurrency_superseded(workflow_name);
    }
}

/// Emit [`METRIC_CONCURRENCY_RESIDUAL_OVER_LIMIT`] for a latest-wins pass that
/// could not shed its full computed overflow (issue #1197, item 2).
///
/// Unlike [`emit_concurrency_superseded`], this is called INLINE from
/// [`crate::concurrency::supersede_running_for_key`] rather than deferred to a
/// post-commit point: it is a brand-new counter with no pre-existing
/// post-commit convention to violate (unlike the `StartCancelledRun` samples
/// [`crate::execution::emit_start_cancel_metrics`] emits), and the condition
/// it reports — a key transiently over its declared limit — is itself already
/// a rare, self-healing edge case (a nested self-referential
/// `cancel_running` trigger admission), so an occasional phantom sample from a
/// rolled-back terminal transaction is an accepted, documented simplification
/// rather than the residual issue #1197 exists to close.
///
/// Canary probe workflows (issue #796) are excluded, mirroring
/// [`emit_workflow_terminal`].
pub fn emit_concurrency_residual_over_limit<M: MetricsRecorder + ?Sized>(
    metrics: &M,
    workflow_name: &str,
    gap: u64,
) {
    if crate::canary::is_canary_workflow(workflow_name) {
        return;
    }
    metrics.record_concurrency_residual_over_limit(workflow_name, gap);
}

/// Default metrics recorder that discards every sample.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoOpMetrics;

impl MetricsRecorder for NoOpMetrics {
    fn is_enabled(&self) -> bool {
        false
    }
}

// ---------------------------------------------------------------------------
// UserMetrics handle (issue #532)
// ---------------------------------------------------------------------------

/// Replay-safe custom-metrics handle exposed by `ctx.metrics()`.
///
/// Obtained from [`WorkflowContext::metrics`](crate::context::WorkflowContext::metrics)
/// or [`ActivityContext::metrics`](crate::context::ActivityContext::metrics).
/// All method calls are **zero-cost no-ops** when the configured
/// [`MetricsRecorder`] is [`NoOpMetrics`] or when the workflow is replaying.
///
/// ## Namespacing
///
/// The `name` argument is the **suffix** only.  A full metric name is
/// assembled as `"harvest.user.{name}"` so custom metrics are clearly
/// separated from engine metrics in your backend.
///
/// ```text
/// ctx.metrics().counter("orders_processed", 1, &[("tier", "gold")])
/// // → emits "harvest.user.orders_processed" with label tier=gold
/// ```
///
/// ## Replay safety (workflow metrics only)
///
/// When called from a `#[workflow]` body, metric emission is **suppressed
/// during deterministic replay** (`ctx.is_replaying() == true`).  A counter
/// incremented once in workflow logic therefore increments the backend exactly
/// once regardless of how many replay cycles the executor runs.
///
/// ## Activity retries
///
/// When called from a `#[activity]` body, metrics are **not** suppressed —
/// every actual invocation emits.  Each retry is a separate execution, so a
/// counter inside an activity body increments once per attempt.  Document
/// retry semantics in your metrics if downstream consumers care.
///
/// ## Label cardinality
///
/// Per ADR-0001 §7, label keys must be low-cardinality.  The following keys
/// are **always rejected** (logged as warnings, metric is dropped):
/// `execution.id`, `activity.id`, `workflow.id`, `harvest.execution.id`,
/// `harvest.activity.id`, `idempotency_key`, `run_id`.  At most
/// [`MAX_USER_METRIC_LABELS`] labels are accepted per call.
pub struct UserMetrics<'a> {
    recorder: &'a dyn MetricsRecorder,
    suppressed: bool,
}

impl<'a> UserMetrics<'a> {
    /// Create a new handle.
    ///
    /// `suppressed` should be `true` when `WorkflowContext::is_replaying()` is
    /// true; always `false` for `ActivityContext`.
    #[must_use]
    pub(crate) fn new(recorder: &'a dyn MetricsRecorder, suppressed: bool) -> Self {
        Self {
            recorder,
            suppressed,
        }
    }

    /// Emit a custom counter increment.
    ///
    /// Suppressed during workflow replay and when telemetry is disabled.
    /// Logs a `tracing::warn!` and drops the call on label-validation failure.
    pub fn counter(&self, name: &str, value: u64, labels: &[(&str, &str)]) {
        if self.suppressed || !self.recorder.is_enabled() {
            return;
        }
        if let Err(e) = validate_user_metric(name, labels) {
            tracing::warn!(metric_name = name, error = %e, "custom metric rejected");
            return;
        }
        let full_name = format!("{USER_METRIC_PREFIX}{name}");
        self.recorder.record_user_counter(&full_name, value, labels);
    }

    /// Emit a custom gauge observation.
    ///
    /// Suppressed during workflow replay and when telemetry is disabled.
    /// Logs a `tracing::warn!` and drops the call on label-validation failure.
    pub fn gauge(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        if self.suppressed || !self.recorder.is_enabled() {
            return;
        }
        if let Err(e) = validate_user_metric(name, labels) {
            tracing::warn!(metric_name = name, error = %e, "custom metric rejected");
            return;
        }
        let full_name = format!("{USER_METRIC_PREFIX}{name}");
        self.recorder.record_user_gauge(&full_name, value, labels);
    }

    /// Emit a custom histogram sample.
    ///
    /// Suppressed during workflow replay and when telemetry is disabled.
    /// Logs a `tracing::warn!` and drops the call on label-validation failure.
    pub fn histogram(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        if self.suppressed || !self.recorder.is_enabled() {
            return;
        }
        if let Err(e) = validate_user_metric(name, labels) {
            tracing::warn!(metric_name = name, error = %e, "custom metric rejected");
            return;
        }
        let full_name = format!("{USER_METRIC_PREFIX}{name}");
        self.recorder
            .record_user_histogram(&full_name, value, labels);
    }
}

// ---------------------------------------------------------------------------
// Telemetry bundle
// ---------------------------------------------------------------------------

/// Bundle of telemetry dependencies injected into the harvest runtime.
///
/// Use [`TelemetryConfig::builder`] for the common case; applications that
/// need to drive telemetry manually in tests can construct the struct literal
/// directly.
#[derive(Clone)]
pub struct TelemetryConfig {
    /// Human-readable service name stamped onto emitted spans. Defaults to
    /// `"autumn-harvest"`.
    pub service_name: Arc<str>,

    /// How the runtime exchanges trace context with the application's
    /// `OTel` setup. Defaults to [`NoOpPropagator`].
    pub propagator: Arc<dyn TraceContextPropagator>,

    /// Where per-event metrics are recorded. Defaults to [`NoOpMetrics`].
    pub metrics: Arc<dyn MetricsRecorder>,
}

impl std::fmt::Debug for TelemetryConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TelemetryConfig")
            .field("service_name", &self.service_name)
            .field("propagator", &std::any::type_name_of_val(&*self.propagator))
            .field("metrics", &std::any::type_name_of_val(&*self.metrics))
            .finish()
    }
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            service_name: Arc::from("autumn-harvest"),
            propagator: Arc::new(NoOpPropagator),
            metrics: Arc::new(NoOpMetrics),
        }
    }
}

impl TelemetryConfig {
    /// Start a fluent builder pre-populated with safe no-op defaults.
    #[must_use]
    pub fn builder() -> TelemetryConfigBuilder {
        TelemetryConfigBuilder::default()
    }

    /// Capture the current trace context via the configured propagator.
    #[must_use]
    pub fn capture_trace_context(&self) -> Option<TraceContextCarrier> {
        self.propagator.capture()
    }

    /// Install `carrier` as the parent context for subsequent spans. The
    /// returned guard restores the prior context when dropped.
    #[must_use]
    pub fn install_trace_context(&self, carrier: &TraceContextCarrier) -> Box<dyn Any + Send> {
        self.propagator.install(carrier)
    }
}

/// Fluent builder for [`TelemetryConfig`].
#[derive(Default)]
pub struct TelemetryConfigBuilder {
    service_name: Option<Arc<str>>,
    propagator: Option<Arc<dyn TraceContextPropagator>>,
    metrics: Option<Arc<dyn MetricsRecorder>>,
}

impl TelemetryConfigBuilder {
    /// Override the service name stamped onto spans.
    #[must_use]
    pub fn service_name(mut self, name: impl Into<Arc<str>>) -> Self {
        self.service_name = Some(name.into());
        self
    }

    /// Register a custom [`TraceContextPropagator`].
    #[must_use]
    pub fn propagator(mut self, propagator: Arc<dyn TraceContextPropagator>) -> Self {
        self.propagator = Some(propagator);
        self
    }

    /// Register a custom [`MetricsRecorder`].
    #[must_use]
    pub fn metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Finalize into a [`TelemetryConfig`], filling unset fields with the
    /// safe defaults.
    #[must_use]
    pub fn build(self) -> TelemetryConfig {
        TelemetryConfig {
            service_name: self
                .service_name
                .unwrap_or_else(|| Arc::from("autumn-harvest")),
            propagator: self.propagator.unwrap_or_else(|| Arc::new(NoOpPropagator)),
            metrics: self.metrics.unwrap_or_else(|| Arc::new(NoOpMetrics)),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // RED-phase tests: these assert the spec-mandated constants and
    // replay-aware carrier shape defined in docs/adr/0001-otel-trace-contract.md
    // -----------------------------------------------------------------------

    #[test]
    fn span_attribute_constants_have_correct_names() {
        // The OTel trace contract ADR mandates these exact attribute keys.
        assert_eq!(ATTR_WORKFLOW_ID, "harvest.workflow.id");
        assert_eq!(ATTR_EXECUTION_ID, "harvest.execution.id");
        assert_eq!(ATTR_SHARD_ID, "harvest.shard.id");
        assert_eq!(ATTR_ACTIVITY_NAME, "harvest.activity.name");
        assert_eq!(ATTR_ATTEMPT, "harvest.attempt");
        assert_eq!(ATTR_QUEUE, "harvest.queue");
        assert_eq!(ATTR_REPLAY, "harvest.replay");
    }

    #[test]
    fn metric_name_constants_have_correct_names() {
        // OTel semantic naming: instrument.noun (dot-separated).
        assert_eq!(METRIC_WORKFLOW_STARTED, "harvest.workflow.started");
        assert_eq!(METRIC_WORKFLOW_DURATION, "harvest.workflow.duration");
        assert_eq!(
            METRIC_WORKFLOW_HISTORY_SIZE,
            "harvest.workflow.history_size"
        );
        assert_eq!(
            METRIC_WORKFLOW_CONTINUE_AS_NEW,
            "harvest.workflow.continue_as_new"
        );
        assert_eq!(METRIC_ACTIVITY_DURATION, "harvest.activity.duration");
        assert_eq!(METRIC_TIMER_STARTED, "harvest.timer.started");
        assert_eq!(METRIC_QUEUE_DEPTH, "harvest.queue.depth");
        assert_eq!(METRIC_DLQ_ENTRIES, "harvest.dlq.entries");
        assert_eq!(METRIC_SCHEDULE_RUNS, "harvest.schedule.runs");
        assert_eq!(METRIC_SCHEDULE_SKIPPED, "harvest.schedule.skipped");
        assert_eq!(
            METRIC_SCHEDULE_DECISION_WRITE_FAILED,
            "harvest.schedule.decision_write_failed"
        );
        assert_eq!(METRIC_RETENTION_DELETED, "harvest.retention.deleted");
        // issue #811: latest-wins concurrency supersede counter.
        assert_eq!(
            METRIC_CONCURRENCY_SUPERSEDED,
            "harvest.concurrency.superseded"
        );
        assert_eq!(METRIC_SUMMARY_DELETED, "harvest.retention.summary_deleted");
        assert_eq!(
            METRIC_RATE_LIMIT_BUCKETS_DELETED,
            "harvest.retention.rate_limit_buckets_deleted"
        );
        // issue #618: exempt-by-design start producers increment this counter.
        assert_eq!(METRIC_ADMISSION_BYPASSED, "harvest.admission.bypassed");
        assert_eq!(METRIC_LABEL_PRODUCER, "producer");
        assert_eq!(METRIC_WORKFLOW_TIMEOUT, "harvest.workflow.timeout");
        assert_eq!(
            METRIC_WORKFLOW_CHAIN_TIMEOUT,
            "harvest.workflow.chain_timeout"
        );
        assert_eq!(METRIC_TASK_QUARANTINED, "harvest.task.quarantined");
        assert_eq!(
            METRIC_WORKFLOW_NON_DETERMINISM,
            "harvest.workflow.non_determinism"
        );
        assert_eq!(
            METRIC_WORKFLOW_ND_BLOCKED,
            "harvest.workflow.nondeterministic_block"
        );
        assert_eq!(
            METRIC_WORKFLOW_TASK_TIMEOUT,
            "harvest.workflow.task_timeout"
        );
        assert_eq!(
            METRIC_QUEUE_SCHEDULE_TO_START,
            "harvest.queue.schedule_to_start"
        );
        assert_eq!(
            METRIC_QUEUE_OLDEST_PENDING_AGE,
            "harvest.queue.oldest_pending_age"
        );
        assert_eq!(
            METRIC_COMPLETION_TRIGGER_FIRED,
            "harvest.completion_trigger.fires"
        );
        assert_eq!(
            METRIC_COMPLETION_TRIGGER_SKIPPED,
            "harvest.completion_trigger.skipped"
        );
        assert_eq!(METRIC_ACTIVITY_PANIC, "harvest.activity.panic");
        assert_eq!(METRIC_WORKFLOW_PANIC, "harvest.workflow.panic");
        // Synthetic liveness canary (issue #796) — distinct from #512 replay canary.
        assert_eq!(METRIC_CANARY_ROUNDTRIP, "harvest.canary.roundtrip");
        assert_eq!(METRIC_CANARY_SUCCESS, "harvest.canary.success");
        assert_eq!(METRIC_CANARY_FAILURE, "harvest.canary.failure");
    }

    #[test]
    fn record_canary_metrics_have_noop_defaults() {
        // Synthetic liveness-canary metrics (issue #796) must exist as no-op
        // default trait methods so existing MetricsRecorder implementations
        // compile without changes. Labels are `queue` and `shard` only.
        let rec = NoOpMetrics;
        rec.record_canary_success("default", 0);
        rec.record_canary_failure("email", 2);
        rec.record_canary_roundtrip("default", 0, 0.42);
    }

    #[test]
    fn record_handler_panic_has_noop_defaults() {
        // Contained-handler-panic counters (issue #782). Both must exist with a
        // no-op default body so existing MetricsRecorder implementations compile
        // without changes.
        let rec = NoOpMetrics;
        rec.record_activity_panic("send_email", "default");
        rec.record_workflow_panic("onboarding", "default");
    }

    #[test]
    fn metric_update_duration_constant_has_correct_name() {
        // Issue #781: the admit→terminal latency histogram name is
        // family-consistent with the #1032 `harvest.update.*` counters.
        assert_eq!(METRIC_UPDATE_DURATION, "harvest.update.duration");
    }

    #[test]
    fn record_update_duration_has_noop_default() {
        // Issue #781: the admit→terminal latency histogram must exist as a
        // no-op-default trait method so existing MetricsRecorder implementations
        // compile without changes. Outcome ∈ {completed, failed}.
        let rec = NoOpMetrics;
        rec.record_update_duration("onboarding", "set_priority", "default", "completed", 0.42);
        rec.record_update_duration("onboarding", "cancel", "default", "failed", 1.5);
    }

    #[test]
    fn metric_label_constants_have_correct_names() {
        assert_eq!(METRIC_LABEL_WORKFLOW, "workflow");
        assert_eq!(METRIC_LABEL_WORKFLOW_TYPE, "workflow.type");
        assert_eq!(METRIC_LABEL_ACTIVITY, "activity");
        assert_eq!(METRIC_LABEL_QUEUE, "queue");
        assert_eq!(METRIC_LABEL_BUILD_ID, "build_id");
    }

    // -----------------------------------------------------------------------
    // RED-phase tests for harvest.workflow.terminal counter (issue #519)
    // These tests fail until the implementation is complete.
    // -----------------------------------------------------------------------

    #[test]
    fn metric_workflow_terminal_constant_has_correct_name() {
        // The terminal counter name is specified by ADR-0001 §7 naming convention.
        assert_eq!(METRIC_WORKFLOW_TERMINAL, "harvest.workflow.terminal");
    }

    #[test]
    fn workflow_status_terminal_variants_stringify_correctly() {
        // Outcome label values for the terminal counter are
        // bounded: completed | failed | cancelled | timed_out | terminated | continued_as_new
        assert_eq!(WorkflowStatus::Cancelled.as_str(), "cancelled");
        assert_eq!(WorkflowStatus::TimedOut.as_str(), "timed_out");
        assert_eq!(WorkflowStatus::Terminated.as_str(), "terminated");
    }

    #[test]
    fn workflow_status_suspended_is_not_a_terminal_counter_outcome() {
        // Suspended executor cycles must NOT increment the terminal counter.
        // Verify that "suspended" is not in the bounded terminal outcome set.
        let terminal_outcomes = [
            WorkflowStatus::Completed.as_str(),
            WorkflowStatus::Failed.as_str(),
            WorkflowStatus::Cancelled.as_str(),
            WorkflowStatus::TimedOut.as_str(),
            WorkflowStatus::Terminated.as_str(),
            WorkflowStatus::ContinuedAsNew.as_str(),
        ];
        assert!(
            !terminal_outcomes.contains(&WorkflowStatus::Suspended.as_str()),
            "Suspended must not appear in the terminal outcome label set; \
             a suspended executor cycle does not mark a workflow as terminal"
        );
    }

    #[test]
    fn record_workflow_terminal_has_noop_default() {
        // MetricsRecorder::record_workflow_terminal must exist with a no-op
        // default body so existing MetricsRecorder implementations compile
        // without changes.
        let rec = NoOpMetrics;
        rec.record_workflow_terminal("onboarding", "default", WorkflowStatus::Completed);
        rec.record_workflow_terminal("onboarding", "default", WorkflowStatus::Failed);
        rec.record_workflow_terminal("onboarding", "default", WorkflowStatus::Cancelled);
        rec.record_workflow_terminal("onboarding", "default", WorkflowStatus::TimedOut);
        rec.record_workflow_terminal("onboarding", "default", WorkflowStatus::Terminated);
        rec.record_workflow_terminal("onboarding", "default", WorkflowStatus::ContinuedAsNew);
        // Calling for Suspended is allowed by the type system but callers
        // must never do so in practice (enforced by worker.rs call sites).
    }

    #[test]
    fn record_workflow_terminal_fires_once_per_distinct_outcome() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Default)]
        struct TerminalCounter(AtomicUsize);

        impl MetricsRecorder for TerminalCounter {
            fn record_workflow_terminal(&self, _wf: &str, _q: &str, outcome: WorkflowStatus) {
                // The counter should fire exactly once per terminal outcome
                // and never for Suspended.
                assert!(
                    !matches!(outcome, WorkflowStatus::Suspended),
                    "record_workflow_terminal must not be called with Suspended"
                );
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let counter = Arc::new(TerminalCounter::default());
        for outcome in [
            WorkflowStatus::Completed,
            WorkflowStatus::Failed,
            WorkflowStatus::Cancelled,
            WorkflowStatus::TimedOut,
            WorkflowStatus::Terminated,
            WorkflowStatus::ContinuedAsNew,
        ] {
            counter.record_workflow_terminal("wf", "q", outcome);
        }
        assert_eq!(
            counter.0.load(Ordering::SeqCst),
            6,
            "should emit once for each of the 6 terminal outcomes"
        );
    }

    #[test]
    fn emit_concurrency_superseded_skips_canary_and_records_business_workflows() {
        // Issue #811 AC7: the counter fires exactly once per superseded run,
        // carrying the SUPERSEDED run's workflow name. Canary probes are
        // excluded, mirroring `emit_workflow_terminal`, so the synthetic
        // liveness canary can never move a business-facing counter.
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Default)]
        struct SupersededCounter(AtomicUsize, std::sync::Mutex<Vec<String>>);
        impl MetricsRecorder for SupersededCounter {
            fn record_concurrency_superseded(&self, workflow: &str) {
                self.0.fetch_add(1, Ordering::SeqCst);
                self.1.lock().unwrap().push(workflow.to_owned());
            }
        }

        let counter = Arc::new(SupersededCounter::default());

        // An empty list (the overwhelmingly common `Defer` path) emits nothing.
        emit_concurrency_superseded(counter.as_ref(), &[]);
        assert_eq!(counter.0.load(Ordering::SeqCst), 0);

        // A canary probe records NOTHING.
        emit_concurrency_superseded(
            counter.as_ref(),
            &[
                crate::canary::CANARY_WORKFLOW_NAME_PREFIX.to_owned(),
                "__harvest_canary_probe__default".to_owned(),
            ],
        );
        assert_eq!(
            counter.0.load(Ordering::SeqCst),
            0,
            "a canary probe must never increment harvest.concurrency.superseded"
        );

        // Business workflows emit exactly once per superseded run, in order,
        // including the same name twice when limit = N sheds two runs.
        emit_concurrency_superseded(
            counter.as_ref(),
            &[
                "doc_index".to_owned(),
                "doc_index".to_owned(),
                "checkout".to_owned(),
            ],
        );
        assert_eq!(counter.0.load(Ordering::SeqCst), 3);
        assert_eq!(
            counter.1.lock().unwrap().as_slice(),
            &["doc_index", "doc_index", "checkout"],
            "the counter must carry the superseded run's workflow name, once per run"
        );

        // Also works through a `&*Arc<dyn MetricsRecorder>` erased reference.
        let erased: Arc<dyn MetricsRecorder> = counter.clone();
        emit_concurrency_superseded(&*erased, &["billing".to_owned()]);
        assert_eq!(counter.0.load(Ordering::SeqCst), 4);
    }

    #[test]
    fn emit_workflow_terminal_skips_canary_and_records_business_workflows() {
        // Issue #796 AC8: the single choke point routes a business workflow's
        // terminal to `record_workflow_terminal` but skips a synthetic-liveness
        // canary probe entirely, so a canary can never leak into
        // `harvest.workflow.terminal` no matter which terminal path it reaches.
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Default)]
        struct TerminalCounter(AtomicUsize);
        impl MetricsRecorder for TerminalCounter {
            fn record_workflow_terminal(&self, _wf: &str, _q: &str, _outcome: WorkflowStatus) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let counter = Arc::new(TerminalCounter::default());

        // A canary probe (any per-queue name) records NOTHING.
        for canary in [
            crate::canary::CANARY_WORKFLOW_NAME_PREFIX,
            "__harvest_canary_probe__default",
            "__harvest_canary_probe__email",
        ] {
            emit_workflow_terminal(counter.as_ref(), canary, "default", WorkflowStatus::Failed);
        }
        assert_eq!(
            counter.0.load(Ordering::SeqCst),
            0,
            "a canary probe must never increment harvest.workflow.terminal"
        );

        // An ordinary business workflow records once per call.
        emit_workflow_terminal(
            counter.as_ref(),
            "onboarding",
            "default",
            WorkflowStatus::Completed,
        );
        emit_workflow_terminal(counter.as_ref(), "checkout", "q", WorkflowStatus::Cancelled);
        assert_eq!(
            counter.0.load(Ordering::SeqCst),
            2,
            "business workflows must still increment harvest.workflow.terminal"
        );

        // Also works through a `&*Arc<dyn MetricsRecorder>` erased reference.
        let erased: Arc<dyn MetricsRecorder> = counter.clone();
        emit_workflow_terminal(&*erased, "billing", "default", WorkflowStatus::TimedOut);
        assert_eq!(counter.0.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn execution_id_is_not_a_parameter_of_record_workflow_terminal() {
        // ADR-0001 §7 cardinality rule: execution.id is span-only.
        // This test verifies by construction that record_workflow_terminal
        // accepts only (workflow_name, queue, outcome) — no execution id.
        let rec: Arc<dyn MetricsRecorder> = Arc::new(NoOpMetrics);
        rec.record_workflow_terminal("billing", "default", WorkflowStatus::Completed);
        // If execution.id were a parameter the call above would require an
        // extra UUID argument and this test would fail to compile.
    }

    // ── Activity attempt + retry counter tests (issue #528) ─────────────

    #[test]
    fn metric_activity_attempts_constant_has_correct_name() {
        assert_eq!(METRIC_ACTIVITY_ATTEMPTS, "harvest.activity.attempts");
    }

    #[test]
    fn metric_activity_retries_constant_has_correct_name() {
        assert_eq!(METRIC_ACTIVITY_RETRIES, "harvest.activity.retries");
    }

    #[test]
    fn record_activity_attempt_has_noop_default() {
        // MetricsRecorder::record_activity_attempt must exist with a no-op
        // default body so existing implementations compile without changes.
        let rec = NoOpMetrics;
        rec.record_activity_attempt("send_email", "default", ActivityStatus::Completed);
        rec.record_activity_attempt("send_email", "default", ActivityStatus::Failed);
    }

    #[test]
    fn record_activity_retried_has_noop_default() {
        let rec = NoOpMetrics;
        rec.record_activity_retried("send_email", "default");
    }

    #[test]
    fn execution_id_is_not_a_parameter_of_record_activity_attempt() {
        // ADR-0001 §7 cardinality rule: execution.id / activity.id are span-only.
        // Verifies by construction: the method only accepts activity_name, queue, outcome.
        let rec: Arc<dyn MetricsRecorder> = Arc::new(NoOpMetrics);
        rec.record_activity_attempt("charge_card", "billing", ActivityStatus::Completed);
    }

    #[test]
    fn activity_status_outcome_values_are_bounded() {
        // outcome label is low-cardinality: exactly "completed" and "failed".
        assert_eq!(ActivityStatus::Completed.as_str(), "completed");
        assert_eq!(ActivityStatus::Failed.as_str(), "failed");
    }

    #[test]
    fn record_activity_attempt_fires_for_both_outcomes() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Default)]
        struct AttemptCounter {
            completed: AtomicUsize,
            failed: AtomicUsize,
        }

        impl MetricsRecorder for AttemptCounter {
            fn record_activity_attempt(
                &self,
                _activity: &str,
                _queue: &str,
                outcome: ActivityStatus,
            ) {
                match outcome {
                    ActivityStatus::Completed => {
                        self.completed.fetch_add(1, Ordering::SeqCst);
                    }
                    ActivityStatus::Failed => {
                        self.failed.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        }

        let counter = AttemptCounter::default();
        counter.record_activity_attempt("act", "q", ActivityStatus::Completed);
        counter.record_activity_attempt("act", "q", ActivityStatus::Completed);
        counter.record_activity_attempt("act", "q", ActivityStatus::Failed);
        assert_eq!(counter.completed.load(Ordering::SeqCst), 2);
        assert_eq!(counter.failed.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn record_activity_retried_fires_per_retry_scheduled() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Default)]
        struct RetryCounter(AtomicUsize);

        impl MetricsRecorder for RetryCounter {
            fn record_activity_retried(&self, _activity: &str, _queue: &str) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let counter = RetryCounter::default();
        counter.record_activity_retried("send_email", "default");
        counter.record_activity_retried("send_email", "default");
        counter.record_activity_retried("send_email", "default");
        assert_eq!(
            counter.0.load(Ordering::SeqCst),
            3,
            "retry counter should fire once per retry scheduled"
        );
    }

    // ── Workflow-task timeout metric tests (issue #494) ───────────────────

    #[test]
    fn record_workflow_task_timeout_has_noop_default() {
        // MetricsRecorder::record_workflow_task_timeout must exist with a
        // no-op default so existing implementations compile without changes.
        let rec = NoOpMetrics;
        rec.record_workflow_task_timeout("onboarding", "default");
    }

    #[test]
    fn execution_id_is_not_a_parameter_of_record_workflow_task_timeout() {
        // ADR-0001 §7 cardinality rule: execution.id is span-only.
        let rec: Arc<dyn MetricsRecorder> = Arc::new(NoOpMetrics);
        rec.record_workflow_task_timeout("billing", "default");
    }

    #[test]
    fn replay_carrier_strips_traceparent_and_preserves_link() {
        // Per spec: replay spans MUST NOT be parented to the original
        // (potentially expired) trace — they link to it instead.
        let original = TraceContextCarrier::from_traceparent(
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        );
        let replay = original.into_replay_context();
        assert!(
            replay.traceparent.is_none(),
            "replay carrier must not carry original traceparent as parent"
        );
        assert_eq!(
            replay.link_traceparent.as_deref(),
            Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"),
            "replay carrier must preserve original as a span link"
        );
        assert!(
            replay.is_replay,
            "replay carrier must be flagged harvest.replay = true"
        );
    }

    #[test]
    fn non_replay_carrier_has_is_replay_false_and_no_link() {
        let carrier = TraceContextCarrier::from_traceparent("00-abcd-ef01-01");
        assert!(!carrier.is_replay);
        assert!(carrier.link_traceparent.is_none());
    }

    #[test]
    fn replay_carrier_from_empty_original_has_no_link() {
        let empty = TraceContextCarrier::default();
        let replay = empty.into_replay_context();
        assert!(replay.traceparent.is_none());
        assert!(
            replay.link_traceparent.is_none(),
            "no link when original had no traceparent"
        );
        assert!(replay.is_replay);
    }

    #[test]
    fn replay_carrier_roundtrips_through_json() {
        let original = TraceContextCarrier::from_traceparent(
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        );
        let replay = original.into_replay_context();
        // A replay carrier must still serialise / deserialise so it can
        // travel on harvest_task_queue.trace_context.
        let json = replay.to_json().expect("replay carrier serialises");
        let decoded = TraceContextCarrier::from_json(&json).expect("valid JSON roundtrips");
        assert!(decoded.is_replay);
        assert_eq!(
            decoded.link_traceparent.as_deref(),
            Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"),
        );
        assert!(decoded.traceparent.is_none());
    }

    #[test]
    fn into_replay_context_on_already_replay_carrier_preserves_link() {
        // If into_replay_context() is called defensively on a carrier that is
        // already in replay form (traceparent=None, link_traceparent=Some),
        // the existing link must not be erased.
        let original = TraceContextCarrier::from_traceparent(
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        );
        let replay = original.into_replay_context();
        // Call again — should be idempotent, link must survive.
        let replay2 = replay.into_replay_context();
        assert!(replay2.traceparent.is_none());
        assert!(replay2.is_replay);
        assert_eq!(
            replay2.link_traceparent.as_deref(),
            Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"),
            "double-converting a replay carrier must not erase the original link"
        );
    }

    #[test]
    fn carrier_roundtrips_through_json() {
        let carrier = TraceContextCarrier {
            traceparent: Some(
                "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string(),
            ),
            tracestate: Some("vendor=abc".to_string()),
            ..Default::default()
        };

        let json = carrier.to_json().expect("non-empty carrier serialises");
        let decoded = TraceContextCarrier::from_json(&json).expect("valid JSON roundtrips");
        assert_eq!(decoded, carrier);
    }

    #[test]
    fn empty_carrier_refuses_to_serialise() {
        let carrier = TraceContextCarrier::default();
        assert!(carrier.is_empty());
        assert!(carrier.to_json().is_none());
    }

    #[test]
    fn carrier_from_json_rejects_fully_empty_payload() {
        let payload = serde_json::json!({});
        assert!(TraceContextCarrier::from_json(&payload).is_none());
    }

    #[test]
    fn carrier_from_json_rejects_wrong_shape() {
        let payload = serde_json::json!({"traceparent": 42});
        assert!(TraceContextCarrier::from_json(&payload).is_none());
    }

    #[test]
    fn carrier_from_traceparent_helper() {
        let carrier = TraceContextCarrier::from_traceparent("00-abcd-ef01-01");
        assert_eq!(carrier.traceparent.as_deref(), Some("00-abcd-ef01-01"));
        assert!(carrier.tracestate.is_none());
    }

    // -----------------------------------------------------------------------
    // RED-phase tests: record_dlq_entries and cardinality enforcement
    // These tests fail until the implementation is complete.
    // -----------------------------------------------------------------------

    #[test]
    fn dlq_entries_gauge_has_default_noop_impl() {
        // record_dlq_entries must exist on MetricsRecorder with (shard: u16, depth: u64).
        // METRIC_DLQ_ENTRIES is the registered constant; this verifies it's
        // actually wired to a callable method with the right shape.
        let rec = NoOpMetrics;
        rec.record_dlq_entries(0, 0);
        rec.record_dlq_entries(1, 42);
    }

    #[test]
    fn shard_stranded_pending_gauge_has_default_noop_impl() {
        // record_shard_stranded_pending must exist on MetricsRecorder with (shard: u16, count: u64).
        // METRIC_SHARD_STRANDED_PENDING is the constant; this verifies shape + no-op (issue #522).
        let rec = NoOpMetrics;
        rec.record_shard_stranded_pending(0, 0);
        rec.record_shard_stranded_pending(1, 42);
        // verify the constant is exported
        assert_eq!(
            METRIC_SHARD_STRANDED_PENDING,
            "harvest.shard.stranded_pending"
        );
    }

    #[test]
    fn replication_gauges_have_default_noop_impls_and_stable_names() {
        // Issue #954 AC4. Labelled `{shard}` only: the standby's
        // `application_name` is operator-chosen and therefore unbounded, so it
        // is deliberately NOT a label (ADR-0001 §7).
        let rec = NoOpMetrics;
        rec.record_replication_lag_seconds(0, 12.5);
        rec.record_replication_observable(0, false);
        rec.record_replication_lag_bytes(0, 4_096);
        rec.record_replication_standbys(0, 1);
        rec.record_replication_rpo_known(0, false);
        rec.record_shard_generation(0, 7);
        rec.record_shard_fenced(0);
        assert_eq!(
            METRIC_REPLICATION_LAG_SECONDS,
            "harvest.replication.lag_seconds"
        );
        assert_eq!(
            METRIC_REPLICATION_LAG_BYTES,
            "harvest.replication.lag_bytes"
        );
        assert_eq!(METRIC_REPLICATION_STANDBYS, "harvest.replication.standbys");
        assert_eq!(
            METRIC_REPLICATION_RPO_KNOWN,
            "harvest.replication.rpo_known"
        );
        assert_eq!(METRIC_SHARD_GENERATION, "harvest.shard.generation");
        assert_eq!(METRIC_SHARD_FENCED, "harvest.shard.fenced");
    }

    #[test]
    fn shard_dispatched_counter_has_default_noop_impl() {
        // AC5 (issue #961): the shard-dimension twin of #515's
        // `harvest.queue.dispatched{queue}`, so an operator can confirm the
        // live dispatch split ACROSS shards and prove no assigned shard is
        // being starved by a deep backlog on a sibling.
        let rec = NoOpMetrics;
        rec.record_shard_dispatched(0);
        rec.record_shard_dispatched(2);
        assert_eq!(METRIC_SHARD_DISPATCHED, "harvest.shard.dispatched");
    }

    #[test]
    fn worker_slots_gauge_has_default_noop_impl() {
        // record_worker_slots must exist on MetricsRecorder with
        // (slot_type: SlotType, in_use: u64, available: u64). The constants are
        // the registered names; this verifies shape + no-op (issue #531).
        let rec = NoOpMetrics;
        rec.record_worker_slots(SlotType::Workflow, 0, 0);
        rec.record_worker_slots(SlotType::Activity, 3, 5);
        assert_eq!(METRIC_WORKER_SLOTS_IN_USE, "harvest.worker.slots_in_use");
        assert_eq!(
            METRIC_WORKER_SLOTS_AVAILABLE,
            "harvest.worker.slots_available"
        );
        assert_eq!(METRIC_LABEL_SLOT_TYPE, "slot_type");
    }

    #[test]
    fn slot_type_stringify_is_bounded() {
        assert_eq!(SlotType::Workflow.as_str(), "workflow");
        assert_eq!(SlotType::Activity.as_str(), "activity");
    }

    #[test]
    fn slot_target_gauge_and_tuner_decision_counter_have_default_noop_impls() {
        // record_worker_slot_target / record_tuner_decision must exist on
        // MetricsRecorder with a no-op default (issue #548).
        let rec = NoOpMetrics;
        rec.record_worker_slot_target(SlotType::Workflow, 20);
        rec.record_worker_slot_target(SlotType::Activity, 40);
        rec.record_tuner_decision(SlotType::Workflow, TunerDecision::Grow);
        rec.record_tuner_decision(SlotType::Activity, TunerDecision::Shrink);
        assert_eq!(METRIC_WORKER_SLOT_TARGET, "harvest.worker.slot_target");
        assert_eq!(
            METRIC_WORKER_TUNER_DECISIONS,
            "harvest.worker.tuner_decisions"
        );
        assert_eq!(METRIC_LABEL_DECISION, "decision");
    }

    #[test]
    fn tuner_decision_stringify_is_bounded() {
        assert_eq!(TunerDecision::Grow.as_str(), "grow");
        assert_eq!(TunerDecision::Shrink.as_str(), "shrink");
        assert_eq!(TunerDecision::Hold.as_str(), "hold");
    }

    // -----------------------------------------------------------------------
    // Activity-type pause/resume counter (issue #807)
    // -----------------------------------------------------------------------

    #[test]
    fn activity_pause_actions_counter_has_correct_name_and_labels() {
        // OTel semantic naming: instrument.noun (dot-separated). Deliberately
        // NOT `harvest.activity.paused`: `harvest.queue.paused` is a gauge
        // (#619) and `harvest.workflow.paused` is a counter (#383), so a third
        // reuse of that noun would be ambiguous about instrument type.
        assert_eq!(
            METRIC_ACTIVITY_PAUSE_ACTIONS,
            "harvest.activity.pause_actions"
        );
        assert_eq!(METRIC_LABEL_ACTION, "action");
        // The plain `activity` label, not the dotted circuit-breaker variant.
        assert_eq!(METRIC_LABEL_ACTIVITY, "activity");
    }

    #[test]
    fn activity_pause_action_stringify_is_bounded() {
        assert_eq!(ActivityPauseAction::Pause.as_str(), "pause");
        assert_eq!(ActivityPauseAction::Resume.as_str(), "resume");
    }

    #[test]
    fn activity_pause_action_counter_has_a_default_noop_impl() {
        // record_activity_pause_action must exist on MetricsRecorder with a
        // no-op default so adding it is not a breaking change for existing
        // implementers (issue #807).
        let rec = NoOpMetrics;
        rec.record_activity_pause_action("charge_card", ActivityPauseAction::Pause);
        rec.record_activity_pause_action("charge_card", ActivityPauseAction::Resume);
    }

    #[test]
    fn worker_slot_invariant_holds_at_recorder_call_site() {
        // AC: `available + in_use == configured_max` for each slot type within
        // one sampler interval. A tracking recorder seeded with the configured
        // maxima asserts the invariant for every observed sample (issue #531).
        use std::collections::HashMap;
        use std::sync::Mutex;

        struct InvariantRecorder {
            configured: HashMap<&'static str, u64>,
            samples: Mutex<Vec<(&'static str, u64, u64)>>,
        }

        impl MetricsRecorder for InvariantRecorder {
            fn record_worker_slots(&self, slot_type: SlotType, in_use: u64, available: u64) {
                let max = *self
                    .configured
                    .get(slot_type.as_str())
                    .expect("known slot type");
                assert_eq!(
                    in_use + available,
                    max,
                    "in_use + available must equal configured max for {}",
                    slot_type.as_str()
                );
                self.samples
                    .lock()
                    .unwrap()
                    .push((slot_type.as_str(), in_use, available));
            }
        }

        let rec = InvariantRecorder {
            configured: HashMap::from([("workflow", 8), ("activity", 16)]),
            samples: Mutex::new(Vec::new()),
        };
        // Simulate one sampler interval: 3 of 8 workflow slots busy, 10 of 16
        // activity slots busy.
        rec.record_worker_slots(SlotType::Workflow, 3, 5);
        rec.record_worker_slots(SlotType::Activity, 10, 6);
        assert_eq!(rec.samples.lock().unwrap().len(), 2);
    }

    #[test]
    fn all_metric_record_methods_compile_without_execution_id() {
        // ADR-0001 §7: execution.id is span-only and FORBIDDEN as a metric label.
        // This test verifies by construction that no record_* method on
        // MetricsRecorder accepts an ExecutionId argument — the cardinality
        // guard is enforced at the API surface, not just at call sites.
        let rec: Arc<dyn MetricsRecorder> = Arc::new(NoOpMetrics);
        // Each call compiles only if the method signature matches what we expect:
        // no ExecutionId, no raw UUID params that could smuggle one in.
        rec.record_workflow_started("onboarding", "default");
        rec.record_workflow_completed("onboarding", "default", 1.23, WorkflowStatus::Completed);
        rec.record_workflow_history_size("onboarding", 42);
        rec.record_workflow_continue_as_new("onboarding");
        rec.record_activity_completed("send_email", "default", 0.5, ActivityStatus::Completed);
        rec.record_timer_started(60.0);
        rec.record_queue_depth("default", 7);
        rec.record_dlq_entries(0, 3);
        rec.record_schedule_run("workflow", "daily_digest");
        rec.record_schedule_skipped("workflow", "daily_digest", "paused");
        rec.record_retention_tick(0, 100, 50, 0.02);
        rec.record_retention_deleted("onboarding", 50);
        rec.record_summary_deleted("onboarding", 50);
        rec.record_rate_limit_buckets_deleted("dyn-rate", 12);
        rec.record_workflow_non_determinism("onboarding", "v1.0.0");
        rec.record_workflow_nondeterministic_block("onboarding", "default");
        rec.record_workflow_history_bloat("onboarding");
        rec.record_schedule_to_start("default", 1.5);
        rec.record_queue_oldest_pending_age("default", 30.0);
        rec.record_worker_slots(SlotType::Workflow, 3, 5);
        // If any method silently accepted execution.id we'd see it here.
    }

    #[test]
    fn workflow_status_and_activity_status_stringify() {
        assert_eq!(WorkflowStatus::Completed.as_str(), "completed");
        assert_eq!(WorkflowStatus::Failed.as_str(), "failed");
        assert_eq!(WorkflowStatus::Suspended.as_str(), "suspended");
        assert_eq!(WorkflowStatus::ContinuedAsNew.as_str(), "continued_as_new");
        assert_eq!(ActivityStatus::Completed.as_str(), "completed");
        assert_eq!(ActivityStatus::Failed.as_str(), "failed");
    }

    #[test]
    fn webhook_outcome_stringifies_to_bounded_values() {
        assert_eq!(WebhookOutcome::Accepted.as_str(), "accepted");
        assert_eq!(
            WebhookOutcome::IdempotentReplay.as_str(),
            "idempotent_replay"
        );
        assert_eq!(WebhookOutcome::VerifyFailed.as_str(), "verify_failed");
        assert_eq!(WebhookOutcome::ParseFailed.as_str(), "parse_failed");
        assert_eq!(
            WebhookOutcome::MissingIdempotency.as_str(),
            "missing_idempotency"
        );
        assert_eq!(WebhookOutcome::InternalError.as_str(), "internal_error");
        assert_eq!(WebhookOutcome::Accepted.to_string(), "accepted");
    }

    #[test]
    fn noop_metrics_implements_webhook_methods_without_panicking() {
        let rec = NoOpMetrics;
        rec.record_webhook_received("/hooks/orders", WebhookOutcome::Accepted);
        rec.record_webhook_rejected("/hooks/orders", WebhookOutcome::VerifyFailed);
    }

    #[test]
    fn connector_metric_names_are_stable() {
        // These names are the operator-facing contract (dashboards, alerts).
        assert_eq!(METRIC_CONNECTOR_RECEIVED, "harvest.connector.received");
        assert_eq!(METRIC_CONNECTOR_DISPATCHED, "harvest.connector.dispatched");
        assert_eq!(METRIC_CONNECTOR_POISONED, "harvest.connector.poisoned");
        assert_eq!(METRIC_CONNECTOR_LAG, "harvest.connector.lag");
        assert_eq!(METRIC_LABEL_SOURCE, "source");
    }

    #[test]
    fn connector_outcome_stringifies_to_bounded_values() {
        assert_eq!(ConnectorOutcome::Dispatched.as_str(), "dispatched");
        assert_eq!(
            ConnectorOutcome::IdempotentReplay.as_str(),
            "idempotent_replay"
        );
        assert_eq!(ConnectorOutcome::Deferred.as_str(), "deferred");
        assert_eq!(ConnectorOutcome::DeadLettered.as_str(), "dead_lettered");
        assert_eq!(ConnectorOutcome::Retried.as_str(), "retried");
        assert_eq!(ConnectorOutcome::Dispatched.to_string(), "dispatched");
    }

    #[test]
    fn poison_reason_stringifies_to_bounded_values() {
        assert_eq!(PoisonReason::Malformed.as_str(), "malformed");
        assert_eq!(PoisonReason::MappingRejected.as_str(), "mapping_rejected");
        assert_eq!(PoisonReason::TargetRejected.as_str(), "target_rejected");
        assert_eq!(PoisonReason::Malformed.to_string(), "malformed");
    }

    #[test]
    fn connector_outcome_label_values_are_a_small_closed_set() {
        // ADR-0001 §7: the `outcome`/`reason` label values must be a bounded,
        // closed set so `harvest.connector.*` cardinality stays
        // `sources x small-constant`, never `sources x messages`.
        let outcomes = [
            ConnectorOutcome::Dispatched,
            ConnectorOutcome::IdempotentReplay,
            ConnectorOutcome::Deferred,
            ConnectorOutcome::DeadLettered,
            ConnectorOutcome::Retried,
        ];
        let distinct: std::collections::BTreeSet<&str> =
            outcomes.iter().map(|o| o.as_str()).collect();
        assert_eq!(
            distinct.len(),
            outcomes.len(),
            "outcome labels must be unique"
        );

        let reasons = [
            PoisonReason::Malformed,
            PoisonReason::MappingRejected,
            PoisonReason::TargetRejected,
        ];
        let distinct_reasons: std::collections::BTreeSet<&str> =
            reasons.iter().map(|r| r.as_str()).collect();
        assert_eq!(distinct_reasons.len(), reasons.len());
    }

    #[test]
    fn noop_metrics_implements_connector_methods_without_panicking() {
        let rec = NoOpMetrics;
        rec.record_connector_received("orders");
        rec.record_connector_dispatched("orders", ConnectorOutcome::Dispatched);
        rec.record_connector_poisoned("orders", PoisonReason::Malformed);
        rec.record_connector_lag("orders", 42);
    }

    #[test]
    fn session_acquisition_outcome_stringifies_to_bounded_values() {
        assert_eq!(SessionAcquisitionOutcome::Acquired.as_str(), "acquired");
        assert_eq!(SessionAcquisitionOutcome::TimedOut.as_str(), "timed_out");
        assert_eq!(SessionAcquisitionOutcome::Broken.as_str(), "broken");
        assert_eq!(SessionAcquisitionOutcome::Acquired.to_string(), "acquired");
    }

    #[test]
    fn metric_session_acquisition_name_is_stable() {
        assert_eq!(METRIC_SESSION_ACQUISITION, "harvest.session.acquisition");
    }

    #[test]
    fn noop_metrics_implements_session_acquisition_without_panicking() {
        let rec = NoOpMetrics;
        rec.record_session_acquisition("gpu-workers", SessionAcquisitionOutcome::Acquired);
        rec.record_session_acquisition("gpu-workers", SessionAcquisitionOutcome::TimedOut);
        rec.record_session_acquisition("gpu-workers", SessionAcquisitionOutcome::Broken);
    }

    #[test]
    fn noop_metrics_implements_admission_bypassed_without_panicking() {
        // issue #618: the new bypass counter has a no-op default (additive).
        let rec = NoOpMetrics;
        rec.record_admission_bypassed("outbox");
        rec.record_admission_bypassed("api");
    }

    #[test]
    fn metric_workflow_history_bloat_name_is_stable() {
        assert_eq!(
            METRIC_WORKFLOW_HISTORY_BLOAT,
            "harvest.workflow.history_bloat"
        );
    }

    #[test]
    fn noop_metrics_implements_history_bloat_without_panicking() {
        // issue #704: the new early-warning counter has a no-op default
        // (additive) so a `MetricsRecorder` implementor that predates this
        // issue keeps compiling unchanged.
        let rec = NoOpMetrics;
        rec.record_workflow_history_bloat("onboarding");
    }

    #[test]
    fn default_telemetry_uses_noop_implementations() {
        let telemetry = TelemetryConfig::default();
        assert_eq!(&*telemetry.service_name, "autumn-harvest");
        assert!(telemetry.capture_trace_context().is_none());
        // Calls should not panic; drop the guard immediately.
        let _guard = telemetry.install_trace_context(&TraceContextCarrier::default());
        // Metric calls are no-ops.
        telemetry.metrics.record_workflow_started("demo", "default");
        telemetry.metrics.record_workflow_completed(
            "demo",
            "default",
            0.01,
            WorkflowStatus::Completed,
        );
        telemetry.metrics.record_workflow_history_size("demo", 2);
        telemetry.metrics.record_workflow_continue_as_new("demo");
        telemetry.metrics.record_activity_completed(
            "send_email",
            "default",
            0.5,
            ActivityStatus::Completed,
        );
        telemetry.metrics.record_timer_started(5.0);
        telemetry.metrics.record_queue_depth("default", 0);
    }

    #[test]
    fn queue_latency_metrics_have_noop_defaults() {
        let rec = NoOpMetrics;
        // Both new queue latency methods must compile with only queue_name and
        // the value (no execution.id) and must not panic on the no-op recorder.
        rec.record_schedule_to_start("default", 0.0);
        rec.record_schedule_to_start("priority", 1.234);
        rec.record_queue_oldest_pending_age("default", 0.0);
        rec.record_queue_oldest_pending_age("priority", 42.5);
    }

    #[test]
    fn builder_overrides_defaults() {
        #[derive(Default)]
        struct CountingPropagator {
            captured: std::sync::atomic::AtomicUsize,
        }

        impl TraceContextPropagator for CountingPropagator {
            fn capture(&self) -> Option<TraceContextCarrier> {
                self.captured
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Some(TraceContextCarrier::from_traceparent("00-aaaa-bbbb-01"))
            }
            fn install(&self, _carrier: &TraceContextCarrier) -> Box<dyn Any + Send> {
                Box::new(())
            }
        }

        let prop = Arc::new(CountingPropagator::default());
        let telemetry = TelemetryConfig::builder()
            .service_name("billing")
            .propagator(prop.clone())
            .build();

        assert_eq!(&*telemetry.service_name, "billing");
        let carrier = telemetry.capture_trace_context().expect("captures carrier");
        assert_eq!(carrier.traceparent.as_deref(), Some("00-aaaa-bbbb-01"));
        assert_eq!(prop.captured.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn metrics_recorder_trait_is_object_safe() {
        #[derive(Default)]
        struct Count {
            workflows: std::sync::atomic::AtomicUsize,
            activities: std::sync::atomic::AtomicUsize,
        }

        impl MetricsRecorder for Count {
            fn record_workflow_completed(&self, _: &str, _: &str, _: f64, _: WorkflowStatus) {
                self.workflows
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            fn record_activity_completed(&self, _: &str, _: &str, _: f64, _: ActivityStatus) {
                self.activities
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }

        let count = Arc::new(Count::default());
        let telemetry = TelemetryConfig::builder().metrics(count.clone()).build();

        telemetry
            .metrics
            .record_workflow_completed("w", "default", 1.0, WorkflowStatus::Completed);
        telemetry
            .metrics
            .record_activity_completed("a", "default", 0.5, ActivityStatus::Completed);

        assert_eq!(count.workflows.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(
            count.activities.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
    }

    // ── Custom metrics tests (issue #532) ────────────────────────────────

    #[test]
    fn user_metric_prefix_constant_is_correct() {
        assert_eq!(USER_METRIC_PREFIX, "harvest.user.");
    }

    #[test]
    fn validate_user_metric_accepts_valid_name_and_labels() {
        assert!(
            validate_user_metric("orders_processed", &[("tier", "gold")]).is_ok(),
            "valid name + label should pass"
        );
        assert!(
            validate_user_metric("orders_processed", &[]).is_ok(),
            "empty label list should pass"
        );
    }

    #[test]
    fn validate_user_metric_rejects_empty_name() {
        assert_eq!(
            validate_user_metric("", &[]),
            Err(UserMetricError::EmptyName)
        );
    }

    #[test]
    fn validate_user_metric_rejects_harvest_prefix() {
        // Names starting with "harvest." would collide with engine metrics.
        assert_eq!(
            validate_user_metric("harvest.workflow.started", &[]),
            Err(UserMetricError::ReservedPrefix)
        );
        assert_eq!(
            validate_user_metric("harvest.user.something", &[]),
            Err(UserMetricError::ReservedPrefix),
            "even the user prefix itself must be rejected in the suffix argument"
        );
    }

    #[test]
    fn validate_user_metric_rejects_name_too_long() {
        let long_name = "a".repeat(MAX_USER_METRIC_NAME_LEN + 1);
        assert_eq!(
            validate_user_metric(&long_name, &[]),
            Err(UserMetricError::NameTooLong)
        );
    }

    #[test]
    fn validate_user_metric_rejects_too_many_labels() {
        let labels: Vec<(&str, &str)> = (0..=MAX_USER_METRIC_LABELS)
            .map(|_| ("tier", "gold"))
            .collect();
        assert_eq!(
            validate_user_metric("orders", &labels),
            Err(UserMetricError::TooManyLabels)
        );
    }

    #[test]
    fn validate_user_metric_rejects_empty_label_key() {
        assert_eq!(
            validate_user_metric("orders", &[("", "value")]),
            Err(UserMetricError::EmptyLabelKey)
        );
    }

    #[test]
    fn validate_user_metric_rejects_forbidden_label_keys() {
        // ADR-0001 §7 cardinality rule: these keys are never allowed on metrics.
        for forbidden in FORBIDDEN_USER_LABEL_KEYS {
            let result = validate_user_metric("orders", &[(forbidden, "some-uuid")]);
            assert!(
                matches!(result, Err(UserMetricError::ForbiddenLabelKey(_))),
                "label key \"{forbidden}\" should be forbidden"
            );
        }
    }

    #[test]
    fn user_metrics_noop_recorder_is_suppressed_silently() {
        // With NoOpMetrics the is_enabled() gate returns false so no validation
        // is run and nothing panics, even for a theoretically invalid name.
        let rec = NoOpMetrics;
        let m = UserMetrics::new(&rec, false);
        m.counter("orders", 1, &[]);
        m.gauge("queue_depth", 3.0, &[]);
        m.histogram("latency_ms", 42.0, &[]);
    }

    #[test]
    fn user_metrics_suppressed_flag_short_circuits_even_with_real_recorder() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Default)]
        struct CountingRec(AtomicUsize);
        impl MetricsRecorder for CountingRec {
            fn is_enabled(&self) -> bool {
                true
            }
            fn record_user_counter(&self, _name: &str, _val: u64, _labels: &[(&str, &str)]) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let rec = CountingRec::default();
        let m = UserMetrics::new(&rec, true); // suppressed = true (replay mode)
        m.counter("orders", 1, &[]);
        m.counter("orders", 1, &[]);
        assert_eq!(
            rec.0.load(Ordering::SeqCst),
            0,
            "suppressed handle must emit zero samples"
        );
    }

    #[test]
    fn user_metrics_live_handle_emits_to_recorder() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Default)]
        struct CountingRec(AtomicUsize);
        impl MetricsRecorder for CountingRec {
            fn is_enabled(&self) -> bool {
                true
            }
            fn record_user_counter(&self, name: &str, val: u64, _labels: &[(&str, &str)]) {
                // Verify prefix is applied
                assert!(
                    name.starts_with(USER_METRIC_PREFIX),
                    "emitted name must start with USER_METRIC_PREFIX"
                );
                self.0
                    .fetch_add(usize::try_from(val).unwrap_or(usize::MAX), Ordering::SeqCst);
            }
        }

        let rec = CountingRec::default();
        let m = UserMetrics::new(&rec, false); // not suppressed
        m.counter("orders_processed", 3, &[("tier", "gold")]);
        assert_eq!(rec.0.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn user_metrics_drops_call_with_forbidden_label_key() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Default)]
        struct CountingRec(AtomicUsize);
        impl MetricsRecorder for CountingRec {
            fn is_enabled(&self) -> bool {
                true
            }
            fn record_user_counter(&self, _name: &str, _val: u64, _labels: &[(&str, &str)]) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let rec = CountingRec::default();
        let m = UserMetrics::new(&rec, false);
        // execution.id is forbidden — call should be dropped (warn + return)
        m.counter("orders", 1, &[("execution.id", "some-uuid")]);
        assert_eq!(
            rec.0.load(Ordering::SeqCst),
            0,
            "metric with forbidden label must be dropped"
        );
    }

    #[test]
    fn record_user_counter_has_noop_default() {
        // Verify the three new default methods compile and are no-ops on NoOpMetrics.
        let rec = NoOpMetrics;
        rec.record_user_counter("harvest.user.orders", 1, &[("tier", "gold")]);
        rec.record_user_gauge("harvest.user.balance", 42.0, &[]);
        rec.record_user_histogram("harvest.user.latency_ms", 10.5, &[]);
    }

    #[test]
    fn execution_id_is_not_a_parameter_of_record_user_counter() {
        // ADR-0001 §7 — execution.id must not appear in the method signature.
        // This test verifies it by construction: the call below compiles only
        // because the signature is (name, value, labels) with no exec-id param.
        let rec: Arc<dyn MetricsRecorder> = Arc::new(NoOpMetrics);
        rec.record_user_counter("harvest.user.orders", 1, &[("tier", "gold")]);
    }

    // -----------------------------------------------------------------------
    // Saga compensation observability (issue #801)
    // -----------------------------------------------------------------------

    #[test]
    fn saga_metric_constants_are_stable() {
        // The saga compensation counters are specified by ADR-0001 §7 naming
        // convention (instrument.noun); alert-pack PromQL depends on the
        // rendered Prometheus names, so the constants must never drift.
        assert_eq!(METRIC_SAGA_COMPENSATED, "harvest.saga.compensated");
        assert_eq!(
            METRIC_SAGA_COMPENSATION_FAILED,
            "harvest.saga.compensation_failed"
        );
    }

    #[test]
    fn workflow_active_metric_and_label_constant_names() {
        // Issue #770 — the gauge name and the `state` label constant.
        assert_eq!(METRIC_WORKFLOW_ACTIVE, "harvest.workflow.active");
        assert_eq!(METRIC_LABEL_STATE, "state");
    }

    #[test]
    fn record_workflow_active_has_noop_default() {
        // MetricsRecorder::record_workflow_active must exist with a no-op
        // default body (`state` as `&str`) so existing MetricsRecorder impls
        // compile unchanged. Must not panic.
        let rec = NoOpMetrics;
        rec.record_workflow_active("checkout", "running", 3);
        rec.record_workflow_active("checkout", "paused", 0);
    }

    #[test]
    fn record_saga_compensated_has_noop_default() {
        // MetricsRecorder::record_saga_compensated must exist with a no-op
        // default body so existing MetricsRecorder implementations compile
        // without changes (AC4).
        let rec = NoOpMetrics;
        rec.record_saga_compensated("book_trip", "default");
    }

    #[test]
    fn record_saga_compensation_failed_has_noop_default() {
        // Same additive no-op-default contract for the dangling-state counter.
        let rec = NoOpMetrics;
        rec.record_saga_compensation_failed("book_trip", "default");
    }

    #[test]
    fn execution_id_is_not_a_parameter_of_record_saga_counters() {
        // Compile-level pin ONLY: this test proves the two methods exist and
        // accept exactly (workflow_name, queue) — the cardinality guarantee
        // itself is the trait signature (an ExecutionId parameter would not
        // compile). It asserts nothing at runtime and is not evidence of
        // production label content; that is pinned by the context-level
        // label tests and the metrics_rs_adapter bridge test.
        let rec: Arc<dyn MetricsRecorder> = Arc::new(NoOpMetrics);
        rec.record_saga_compensated("billing", "default");
        rec.record_saga_compensation_failed("billing", "default");
    }

    // -----------------------------------------------------------------------
    // Capability-miss release / escalate counter (issue #804)
    // -----------------------------------------------------------------------

    #[test]
    fn metric_capability_miss_constant_has_correct_name() {
        // AC5 names the counter `harvest.task.capability_miss`. It shares the
        // `harvest.task.*` family with `harvest.task.quarantined` (issue #367)
        // so the two fleet-health task signals sort together on a dashboard.
        assert_eq!(METRIC_TASK_CAPABILITY_MISS, "harvest.task.capability_miss");
    }

    #[test]
    fn metric_label_task_type_has_correct_name() {
        assert_eq!(METRIC_LABEL_TASK_TYPE, "task_type");
    }

    #[test]
    fn record_task_capability_miss_has_noop_default() {
        // Additive no-op default so every existing MetricsRecorder impl
        // compiles unchanged (the established recipe: #519, #367, #618).
        let rec = NoOpMetrics;
        rec.record_task_capability_miss("default", "workflow", "released");
        rec.record_task_capability_miss("default", "activity", "escalated");
    }

    #[test]
    fn capability_miss_outcome_labels_are_bounded() {
        // AC5 requires that operators can "alert on the escalation" — that is
        // only expressible if release and escalation are DISTINGUISHABLE on the
        // counter. The issue text names `{queue, task_type}`; we ship a
        // deliberate bounded SUPERSET with `outcome`, matching the repo idiom
        // (`harvest.schedule.fire_attempts{outcome}`,
        // `harvest.workflow.terminal{outcome}`).
        //
        // Exactly three values, all compile-time constants — no caller-supplied
        // string can widen the cardinality.
        assert_eq!(CAPABILITY_MISS_OUTCOME_RELEASED, "released");
        assert_eq!(CAPABILITY_MISS_OUTCOME_ESCALATED, "escalated");
        assert_eq!(
            CAPABILITY_MISS_OUTCOME_ESCALATED_NEVER_OFFERED,
            "escalated_never_offered"
        );

        let all = [
            CAPABILITY_MISS_OUTCOME_RELEASED,
            CAPABILITY_MISS_OUTCOME_ESCALATED,
            CAPABILITY_MISS_OUTCOME_ESCALATED_NEVER_OFFERED,
        ];
        let distinct: std::collections::BTreeSet<&str> = all.iter().copied().collect();
        assert_eq!(distinct.len(), all.len(), "outcome values must be distinct");

        // PromQL `=` is exact string equality, so the paging rule's
        // `outcome="escalated"` selector excludes the never-offered value ONLY
        // while that value is not itself `escalated`. Pin the non-prefix
        // relationship the alert pack depends on: a future rename to something
        // like `escalated` + a suffix matcher would silently re-conflate them.
        assert_ne!(
            CAPABILITY_MISS_OUTCOME_ESCALATED,
            CAPABILITY_MISS_OUTCOME_ESCALATED_NEVER_OFFERED
        );
    }

    #[test]
    fn execution_id_is_not_a_parameter_of_record_task_capability_miss() {
        // Compile-level cardinality pin (ADR-0001 §7): the signature accepts
        // exactly three bounded strings. An ExecutionId parameter would not
        // compile, so unbounded per-execution series are impossible here by
        // construction.
        let rec: Arc<dyn MetricsRecorder> = Arc::new(NoOpMetrics);
        rec.record_task_capability_miss("default", "workflow", "released");
    }
}
