# Wiring harvest metrics into your Prometheus / OTLP pipeline

Autumn-harvest ships a `MetricsRecorder` trait and nine pre-defined metric
instruments that cover every production-observable aspect of the engine. The
default implementation (`NoOpMetrics`) is zero-cost; no samples are emitted
until you register a recorder.

## Recommended: the built-in scrape endpoint (issue #355)

If you're embedding Harvest via `autumn-harvest-plugin`'s `HarvestPlugin` in
an autumn-web app, this is the fastest path to a working Prometheus scrape —
one builder call, no exporter to install, no route to wire, and no new
dependency (rendering is done entirely through autumn-web's existing
`MetricsSource` contract):

```toml
[dependencies]
autumn-harvest-plugin = { version = "0.4", features = ["metrics"] }
```

```rust
let app = autumn_web::app().plugin(
    HarvestPlugin::new()
        .workflows(workflows![onboarding])
        .activities(activities![send_email])
        .api("/api/harvest")
        .with_metrics_scrape(),
);
```

`with_metrics_scrape()` registers a `HarvestMetricsRecorder` as both the
engine's `MetricsRecorder` (via `HarvestBuilder::telemetry`) and an
autumn-web `MetricsSource` (via `AppBuilder::metrics_source`). The metrics it
covers then appear on the app's **one, already-shared** `/actuator/prometheus`
endpoint, alongside the app's own `autumn_http_*` families and any other
plugin's metrics — there is deliberately no separate, Harvest-owned scrape
route. This is the "one endpoint for autumn-web apps and their plugins"
model: every plugin registers its metrics into the same scrape, instead of
each mounting its own.

```bash
curl http://localhost:8080/actuator/prometheus
```

Auth posture is governed entirely by the app's own `[actuator]` config
(`actuator.prometheus`, profile-aware sensitive-endpoint gating) — the same
policy that already protects the app's other actuator endpoints, rather than
a second, Harvest-specific toggle.

**Coverage:** this endpoint aggregates the nine ADR-0001 §7 catalogue
metrics named in issue #355 (`harvest.workflow.started`,
`harvest.workflow.duration`, `harvest.activity.duration`,
`harvest.timer.started`, `harvest.queue.depth`, `harvest.queue.paused`,
`harvest.dlq.entries`,
`harvest.schedule.runs`, `harvest.schedule.skipped`,
`harvest.retention.deleted`), plus `harvest.queue.oldest_pending_age`,
`harvest.worker.slots_in_use`/`slots_available`/`slot_target`, and
`harvest.shard.stranded_pending` — the engine's own background samplers
already compute these under the same `is_enabled()` gate the nine required
metrics live behind, so they're implemented too rather than sampled and
discarded. It also covers the four broker-connector families
(`harvest.connector.received`, `harvest.connector.dispatched`,
`harvest.connector.poisoned`, `harvest.connector.lag`), which back the shipped
connector dashboard panels — without them a dropped metric would be
indistinguishable from an idle consumer. **It does not back the full starter alert pack**
(`docs/alerts/starter-pack-v0.1.0.json`), which also references metrics this
endpoint never emits (e.g. `harvest_workflow_terminal_total`,
`harvest_activity_attempts_total`/`retries_total`,
`harvest_schedule_fire_attempts_total`, `harvest_no_active_workers`). If you
want the full starter alert pack to work, use the `metrics-rs` adapter
escape hatch below, which bridges every `MetricsRecorder` method.

**Trade-off:** `autumn_web::actuator::MetricsSource`'s `MetricKind` only
supports `Counter`/`Gauge` (no histogram variant), so `harvest.workflow.duration`
and `harvest.activity.duration` are rendered as `_count`/`_sum` counter pairs
rather than a bucketed histogram. That's enough for an average-latency query
(`rate(harvest_workflow_duration_sum[5m]) / rate(harvest_workflow_duration_count[5m])`)
but not for `histogram_quantile(...)`. If you need full bucketed histograms
(e.g. for the starter alert pack's `harvest_queue_schedule_to_start_high` p99
alert), OTLP export, or a custom recorder, use the `metrics-rs` adapter
escape hatch below instead — a fresh `HarvestBuilder::telemetry(...)` call
(or a second `HarvestPlugin` that never calls `.with_metrics_scrape()`)
overrides nothing else in your app.

See `autumn-harvest-plugin/examples/metrics_scrape_quickstart.rs` for a
complete runnable example: `cargo run`, `curl .../actuator/prometheus`, see
all nine metrics after a single workflow run.

## Escape hatch: `metrics-exporter-prometheus` for full histograms, OTLP, or a custom recorder

Reach for this path if you need bucketed histogram `_bucket` series, OTLP
push, or you already have a global `metrics`-crate recorder installed for
app-level metrics.

Enable the in-tree `metrics-rs` adapter in your application's `Cargo.toml`:

```toml
[dependencies]
autumn-harvest = { version = "0.2", features = ["metrics-rs"] }
metrics-exporter-prometheus = "0.16"
```

Then install an exporter and wire the recorder before starting any workers:

```rust
use autumn_harvest::metrics_rs_adapter::MetricsRsRecorder;
use autumn_harvest::telemetry::TelemetryConfig;
use metrics_exporter_prometheus::Matcher;
use std::sync::Arc;

// 1. Install the Prometheus exporter (do this once at process start).
//
//    `metrics-exporter-prometheus` renders every histogram as a Prometheus
//    *summary* (client-side quantiles) UNLESS you configure explicit bucket
//    boundaries. The starter alert pack pages on
//    `histogram_quantile(0.99, rate(harvest_queue_schedule_to_start_bucket[5m]))`,
//    which only exists when buckets are set — and server-side `_bucket` series
//    are the only form you can aggregate across replicas.
//
//    Scope the seconds-oriented boundaries to the *latency* histograms with
//    `set_buckets_for_metric` rather than the global `set_buckets`. A global
//    bucket set is applied to every histogram, including non-duration ones such
//    as `harvest.workflow.history_size` (durable event counts, soft threshold
//    10k) and the payload-size histogram (bytes) — seconds buckets would dump
//    every sample above 600 into `+Inf` and break count/byte dashboards.
//
//    NOTE: `Matcher::Full` is matched against the *sanitized* metric name —
//    `metrics-exporter-prometheus` calls `sanitize_metric_name` before looking up
//    the per-metric distribution — so the matcher must use the Prometheus form
//    with underscores (`harvest_queue_schedule_to_start`), NOT the dotted name
//    registered via the `metrics` macros. A dotted matcher silently fails to match
//    and the metric falls back to a summary (no `_bucket` series).
const LATENCY_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
    10.0, 30.0, 60.0, 120.0, 300.0, 600.0,
];
metrics_exporter_prometheus::PrometheusBuilder::new()
    .set_buckets_for_metric(
        Matcher::Full("harvest_queue_schedule_to_start".into()),
        LATENCY_BUCKETS,
    )
    .expect("valid bucket boundaries")
    .set_buckets_for_metric(
        Matcher::Full("harvest_workflow_duration".into()),
        LATENCY_BUCKETS,
    )
    .expect("valid bucket boundaries")
    .set_buckets_for_metric(
        Matcher::Full("harvest_activity_duration".into()),
        LATENCY_BUCKETS,
    )
    .expect("valid bucket boundaries")
    // history-size is a *count* histogram (durable events), not seconds — give
    // it its own boundaries so it stays usable for count dashboards/alerts.
    .set_buckets_for_metric(
        Matcher::Full("harvest_workflow_history_size".into()),
        &[10.0, 50.0, 100.0, 500.0, 1_000.0, 5_000.0, 10_000.0, 50_000.0],
    )
    .expect("valid bucket boundaries")
    .install()
    .expect("failed to install Prometheus exporter");

// 2. Build a TelemetryConfig that forwards samples to the global registry.
let telemetry = TelemetryConfig::builder()
    .metrics(Arc::new(MetricsRsRecorder))
    .build();

// 3. Pass it to HarvestBuilder.
let harvest = autumn_harvest::HarvestBuilder::new(pool)
    .telemetry(telemetry)
    // ... rest of your config
    .build();
```

The exporter now serves `GET /metrics` in Prometheus text format. Grafana /
your OTLP collector can scrape it directly.

## OTLP / OpenTelemetry

Replace step 1 with your OTel SDK setup and a `metrics-exporter-otlp` or
`opentelemetry-prometheus` bridge. The `MetricsRsRecorder` adapter is
backend-agnostic — it writes to whichever exporter the `metrics` crate's
global recorder is pointing at.

## Custom recorder

If you already have a metrics backend that does not use the `metrics` crate
facade, implement `MetricsRecorder` directly:

```rust
use autumn_harvest::telemetry::{
    ActivityStatus, MetricsRecorder, WorkflowStatus,
};

struct MyRecorder { /* … */ }

impl MetricsRecorder for MyRecorder {
    fn record_workflow_started(&self, workflow_name: &str, queue: &str) {
        my_counter!("harvest.workflow.started", workflow = workflow_name, queue = queue);
    }
    // implement the other methods you care about; all have default no-ops
}
```

---

## Custom workflow/activity metrics (issue #532)

Workflow and activity authors can emit business KPIs ("orders processed",
"dollars charged") into the same `MetricsRecorder` pipeline the engine uses —
without standing up a parallel metrics stack or risking double-counting on
replay.

### API

```rust
// Inside a #[workflow] function — emission is suppressed during replay:
#[workflow]
async fn checkout(ctx: &WorkflowContext, order: Order) -> Result<String, String> {
    let result = ctx.execute_activity(&charge_card_info(), order.clone()).await?;
    // Suppressed on every replay cycle; emitted at the live execution frontier.
    ctx.metrics().counter("orders_processed", 1, &[("tier", &order.tier)]);
    ctx.metrics().histogram("order_amount_usd", order.amount_usd, &[("tier", &order.tier)]);
    Ok(result)
}

// Inside a #[activity] function — always emitted (activities run once per attempt):
#[activity(start_to_close = "30s")]
async fn charge_card(ctx: &ActivityContext, order: Order) -> Result<String, String> {
    // Each retry of this activity is a separate execution — each emits once.
    ctx.metrics().counter("charge_attempts", 1, &[("payment_method", &order.payment_method)]);
    // … charge logic …
    Ok("txn-123".to_string())
}
```

### Replay safety

Workflow metrics are **suppressed during deterministic replay**
(`WorkflowContext::is_replaying() == true`). A workflow that has been suspended
and resumed 50 times will not emit `ctx.metrics()` calls during those 50
re-invocation cycles the replay engine uses to reconstruct state — only at the
live execution frontier are metrics emitted. No new `WorkflowEvent` variants are
produced; the history remains byte-identical whether or not the workflow author
emits custom metrics.

> **At-least-once in crash-recovery scenarios:** if the worker crashes after a
> `ctx.metrics()` call but before the *next* event (activity schedule, timer,
> completion) commits to the database, the task is retried from the same frontier.
> The replay engine re-runs to that point, finds no new committed history past it,
> and emits the metric again. This is the same at-least-once behaviour Temporal's
> own `workflow.GetMetricsHandler()` exhibits. In practice, workflow-task durations
> are measured in milliseconds, so duplicate emissions are rare transients; design
> counter dashboards to be idempotent (sum/rate) rather than expecting exact
> counts.

Activity functions always run exactly once per attempt. Retries are separate
executions and each emits independently — this is intentional and matches the
"each attempt counts" semantics activities already have.

### Namespacing

All names are automatically prefixed with `harvest.user.`:

| `ctx.metrics().counter("orders_processed", …)` | emits `harvest.user.orders_processed` |
|---|---|
| `ctx.metrics().gauge("queue_depth", …)` | emits `harvest.user.queue_depth` |
| `ctx.metrics().histogram("latency_ms", …)` | emits `harvest.user.latency_ms` |

Do **not** pass a name that starts with `harvest.` — it is reserved for engine
metrics and will be rejected with a `tracing::warn!` (the call is dropped, not
a hard error, so the workflow continues running).

### Label cardinality rule (ADR-0001 §7)

The following label keys are **forbidden** because they carry per-execution
cardinality that would blow up any metrics backend:

`execution.id`, `activity.id`, `workflow.id`, `harvest.execution.id`,
`harvest.workflow.id`, `harvest.activity.id`, `idempotency_key`, `run_id`

Using any of these keys logs a `tracing::warn!` and silently drops the metric
call. Use low-cardinality labels only (e.g. `tier`, `region`, `payment_method`).
Additional limits: up to 16 labels per call; label keys and metric names up to
200 characters.

### Zero-overhead when telemetry is off

If no `MetricsRecorder` is configured (the default no-op path), the
`is_enabled() → false` short-circuit exits before validation, so there is zero
overhead in the common "telemetry off" case.

### `metrics-rs` bridge

When the `metrics-rs` feature is enabled, the three user-metric methods are
bridged through `MetricsRsRecorder` using the low-level `metrics::with_recorder`
API with dynamic names and `Vec<Label>`:

```rust
// Emits to whatever metrics exporter is installed globally (e.g. Prometheus).
ctx.metrics().counter("orders_processed", 1, &[("tier", "gold")]);
```

---

## Metric catalogue (ADR-0001 §7)

See [`docs/adr/0001-otel-trace-contract.md`](adr/0001-otel-trace-contract.md)
for the canonical, authoritative catalogue. The table below lists where each
metric is emitted in the source code.

| Metric | Instrument | Call site |
|--------|-----------|-----------|
| `harvest.workflow.started` | Counter | `worker.rs` — `process_workflow_task`, on first live invocation |
| `harvest.workflow.duration` | Histogram | `worker.rs` — `process_workflow_task`, on executor cycle completion |
| `harvest.workflow.timeout` | Counter | `timeout.rs` — `enforce_workflow_execution_timeouts`, when a run's per-run `deadline_at` (issue #243) elapses. Labels: `workflow`, `queue` |
| `harvest.workflow.chain_timeout` | Counter | `timeout.rs` — `enforce_workflow_execution_timeouts`, when a run's chain-scoped `chain_deadline_at` (issue #617) elapses. The chain cap is anchored at the first run's start and carried verbatim across every continue-as-new, so this counter — distinct from `harvest.workflow.timeout` — fires when a whole continue-as-new chain (not a single run) outlives its lifetime cap. Labels: `workflow`, `queue`. Both a chain and a run timeout still emit `harvest.workflow.terminal{outcome="timed_out"}`; the chain-vs-run distinction lives only in these two counters |
| `harvest.workflow.history_bloat` | Counter | `worker.rs` — via the shared `emit_history_bloat_warning_if_crossed` helper, called from two places: (1) `process_workflow_task`'s `Persisted` arm, post-commit, for a still-**RUNNING** (non-terminal, `WorkflowOutcome::Suspended`) execution (same "compute pre-transaction, act only after commit" discipline as `harvest.signal.unhandled`/`harvest.update.completed`/`harvest.update.failed`, issue #684, so an ND-blocked / paused-parked / persist-failed cycle can never emit); and (2) `fail_workflow_for_history_cap`, when a single decision cycle grows history from below the soft threshold straight past `event_hard_cap` in one inline append batch (e.g. local-activity or external-signal persistence), bypassing (1) entirely — the crossing still happened in the same decision, so it is emitted there too, before the execution is terminally DLQ'd. Fires once per still-**RUNNING** or newly-hard-cap-terminal-failed execution the moment its recorded `harvest_events` count first crosses `history_bloat_warn_fraction * event_hard_cap` (`WorkflowHistoryPolicy`, default fraction `0.75`, clamped below `1.0`); a guarded `history_bloat_warned_at` column on the execution row makes the crossing idempotent across replays/retries once the mark is durably set. **Delivery is at-least-once, not exactly-once**: the counter is emitted BEFORE the guard is persisted, so a worker crash in the narrow window between the two leaves the guard unset and a future retry of the same decision cycle may re-emit rather than silently losing the signal forever — deliberate, since the guard is a one-shot, non-recurring gate with no later crossing to fall back on for a given execution (PR #1139 review). Observation-only — the run keeps executing normally; a permanently flat, never-incrementing series for any workflow type with no `event_hard_cap` configured, which is the disabled/no-op state, not a health issue. Pair with `GET /api/harvest/workflows?history_bloat_min_events=<N>` (or `harvest workflow list --history-bloat-min-events <N>`) to discover and rank the specific offending, still-live execution(s) by current history size (issue #704) |
| `harvest.activity.duration` | Histogram | `worker.rs` — `dispatch_activity_handler`, on activity completion (success or failure) |
| `harvest.activity.failed` | Counter | `worker.rs` — `dispatch_activity_handler`, on each failed attempt; richer labels than `harvest.activity.attempts` (`workflow.type`, `error.type`, `non_retryable`) |
| `harvest.activity.attempts` | Counter | `worker.rs` — `dispatch_activity_handler`, once per attempt for **both** outcomes; use for success-rate SLOs: `rate(attempts{outcome="completed"}[5m]) / rate(attempts[5m])` (issue #528) |
| `harvest.activity.retries` | Counter | `worker.rs` — `handle_activity_result`, once per retry actually scheduled (after the `schedule_to_close` deadline check); use for retry-storm detection (issue #528) |
| `harvest.activity.pause_actions` | Counter | `activity_pause.rs` — the `pause_activity` / `resume_activity` write path, once per operator action on a whole activity type (issue #807). **Gated on the action having genuinely changed state** (`ActivityPauseOutcome::newly_paused` / `ActivityResumeOutcome::newly_resumed`) so an idempotent retry after a lost response does not read as a second hold — the same gating `harvest.workflow.paused` uses (issue #383). Deliberately a counter, not a gauge: it records *actions*, so a flat line says nothing about whether a hold is currently in effect — `GET /api/harvest/activities` is the read model for the state. Contrast its sibling `harvest.queue.paused` (issue #619), a gauge, because there the alertable thing is the *duration* of the hold |
| `harvest.timer.started` | Counter | `worker.rs` — `persist_timer_command`, when a durable timer is written |
| `harvest.queue.depth` | Gauge | `worker.rs` — `spawn_queue_depth_sampler`, periodic (5 s default). Aggregated **across all shards** of the worker's `ShardedDbPool` (summed per queue) so multi-shard backlog is fleet-wide, not default-shard-only (issue #522) |
| `harvest.queue.schedule_to_start` | Histogram | `worker.rs` — `dispatch_task`, recorded after the concurrency permit is acquired so it captures worker-local backpressure; skew-discounted (issue #501) |
| `harvest.queue.oldest_pending_age` | Gauge | `worker.rs` — `spawn_queue_depth_sampler`, alongside depth; excludes PAUSED executions, skew-discounted, periodic (5 s default) (issue #501). Aggregated across all shards as the **max** age per queue (the single oldest task fleet-wide) (issue #522) |
| `harvest.queue.dispatched` | Counter | `worker.rs` — `dispatch_task`, once per dispatched task; lets operators confirm the live per-queue dispatch split matches `WorkerConfig::queue_weights` (issue #515) |
| `harvest.dlq.entries` | Gauge | `worker.rs` — `spawn_dlq_depth_sampler`, periodic (5 s default) |
| `harvest.shard.stranded_pending` | Gauge | `worker.rs` — `spawn_stranded_work_sampler`, periodic (`poll_interval`). Claimable pending tasks on a shard with **no covering live worker** (issue #522). Iterates **every** shard of the worker's `ShardedDbPool`, not just its assigned ones, so a writable shard nobody polls is still surfaced. Emits `0` for a covered shard so the gauge resets cleanly |
| `harvest.shard.dispatched` | Counter | `worker.rs` — the poll loop, once per dispatched task (issue #961). The shard-dimension twin of `harvest.queue.dispatched`: confirms the live dispatch split across a multi-shard worker's assigned shards, i.e. that a deep backlog on one shard is not starving its siblings. Emitted by the poll loop rather than `dispatch_task` because a task row carries no `shard_id` column — "which shard" *is* "which pool", and only the poll loop knows which pool it claimed from |
| `harvest.replication.lag_seconds` | Gauge | `worker.rs` — the DR sampler, per shard (issue #954). The **measured RPO**: how many seconds of acknowledged work a failover to the standby region would lose right now, from the age of the newest `harvest_replication_heartbeat` watermark the slowest standby has confirmed. **The series is ABSENT, not zero, when the RPO is unknown** (no standby, no slot, or a standby further behind than the retained trail) — a dead standby reported as `0` reads as a perfect RPO. Alert on `harvest.replication.standbys == 0` for "down"; alert on this for "slow" |
| `harvest.replication.lag_bytes` | Gauge | `worker.rs` — the DR sampler, per shard (issue #954). Worst-case WAL backlog. Survives a disconnected standby (a slot pins WAL with no walsender), so it stays real when the time lag is unknowable; also the disk-pressure signal for an abandoned slot |
| `harvest.replication.observable` | Gauge | `worker.rs` — the DR sampler, per shard, on **every** tick (issue #954). `1` while the shard's replication views are readable, `0` when they are not (usually a missing `GRANT pg_monitor`). Exists because a Prometheus gauge keeps exporting its last value: withholding the RPO and standby gauges does not make them stale, it freezes them at the last healthy reading. This is the signal that says the others cannot be trusted right now |
| `harvest.replication.standbys` | Gauge | `worker.rs` — the DR sampler, per shard (issue #954). Live walsenders. `0` means replication is **down** for that shard: the RPO is unbounded and growing. Always emitted, `0` included — `0` is the signal |
| `harvest.shard.generation` | Gauge | `worker.rs` — the DR sampler, per shard (issue #954). The write-authority epoch the shard's database reports. Its value is uninteresting; its **skew across shards** is the point, since shards fail over independently |
| `harvest.shard.fenced` | Counter | `worker.rs` — the DR sampler, once immediately before a worker stops because its pinned generation was superseded (issue #954). Never self-healing: a fenced worker is recovered by restarting it, never by re-pinning |
| `harvest.audit.export_lag` | Gauge | `audit_export.rs` — the audit exporter, per shard, on **every** tick (issue #953). Age in seconds of the **oldest** audit record the SIEM sink has not acknowledged; `0` when fully caught up. Oldest, not newest, deliberately: under sustained mutating load a stuck exporter always has a brand-new unexported record, so a newest-based lag would read ≈0 during exactly the outage that matters. Emitted on ticks that deliver nothing too — the signal must not go stale precisely when delivery has stopped. **Caveat:** it is written only on a *successful* observation, so a shard whose cursor cannot be read (failing query, unacquirable connection) keeps serving its previous value rather than going absent or high. `harvest.audit.export_observed` (below) is the metric-only signal for that case, closing the gap autumn-foundation/autumn-harvest#1268 tracked. See the runbook's coverage table. The common case — a sink that is down or rejecting — is unaffected: the exporter still reads the cursor and the gauge climbs as designed. A rising line means privileged-action logs are not reaching the SIEM; nothing is lost (the cursor is held, never advanced past the failure) but the window in which a compromise would be invisible is growing |
| `harvest.audit.export_observed` | Gauge | `audit_export.rs` — the audit exporter, per shard, on every tick that reaches a shard (issue #1268). `1` when the cursor read and the lag query both succeeded this tick; `0` on a connection failure, a cursor read failure, or a lag query failure. Exists for the same reason as `harvest.replication.observable`: a Prometheus gauge keeps its last value, so a shard the exporter cannot observe would otherwise leave `harvest.audit.export_lag` frozen — commonly at `0` — with no series ever going high or absent. Does not cover a shard missing from a worker's `shard_assignments` altogether, since no code path runs for it; see the runbook's coverage table |
| `harvest.audit.exported` | Counter | `audit_export.rs` — the audit exporter, once per acknowledged batch with that batch's record count (issue #953). Counted only **after** the cursor advanced, so this is a delivery rate, not an attempt rate. Delivery is at-least-once, so a redelivered batch counts again; receiver-side `(shard, seq)` accounting, not this counter, is the completeness proof. See [`docs/audit-export.md`](audit-export.md) |
| `harvest.task.capability_miss` | Counter | `worker.rs` — `process_task`, once per claim of a task whose workflow/activity type this worker has **no handler registered** for (issue #804). `outcome=released` when the claim is handed back to `PENDING` for a capable peer (the benign, self-healing rolling-deploy signature); `outcome=escalated` when the per-task redelivery budget (`WorkerConfig::capability_miss_max_redeliveries`, default 5, capped exponential backoff) is exhausted and the task falls through to the terminal-failure path with a `no_capable_worker:` reason — the *fleet-exhaustion* signal, and the only outcome that can support concluding no live worker on the queue has the handler. That budget is **gated on the live fleet**: it may only escalate once the workers recorded as having missed the task cover the live workers advertising its queue in `harvest_workers`, so a capable peer that is up and merely losing claim races can never be failed underneath (an ungated `10 ×` absolute ceiling on total releases remains as the bound for the cases coverage cannot reach). Read the reason string before acting: it states whether the fleet was actually confirmed, and reports the *persisted* release/worker counts rather than counting the claim that escalated; `outcome=escalated_never_offered` when the task was failed on its **first** claim after **zero** releases (`capability_miss_max_redeliveries = 0`, or a worker-session pin (#606) whose host lacks the handler), where a capable peer may be live and idle the whole time. Kept separate so the fleet-exhaustion page cannot fire on a cause that carries the opposite conclusion. A clean capability miss never increments `crash_strikes`, so it can never trip poison-pill quarantine (`harvest.task.quarantined`, issue #367) |
| `harvest.queue.paused` | Gauge | `worker.rs` — `spawn_queue_pause_sampler`, periodic (`poll_interval`, 5 s default). `1` while an operator hold is in effect on a queue, `0` otherwise. Read across all shards of the worker's `ShardedDbPool`. A hold observed on a readable shard is **always** emitted, even when another shard's read failed — pause is boolean per queue, so suppressing it would leave this gauge (and the `harvest_queue_paused_too_long` alert) silent for the duration of an unrelated shard outage. A read failure suppresses only the **zero-fill**, so an outage never false-clears the gauge; a queue absent from an incomplete scan is retained and zero-filled exactly once on a later complete scan rather than going stale at `1` (issue #619) |
| `harvest.worker.slots_in_use` | Gauge | `worker.rs` — `spawn_worker_slot_sampler`, periodic (5 s default). Pure in-memory read of the workflow/activity dispatch `Semaphore`s against their configured maxima — no DB access (issue #531) |
| `harvest.worker.slots_available` | Gauge | `worker.rs` — `spawn_worker_slot_sampler`, alongside `slots_in_use`. Invariant: `slots_in_use + slots_available == configured_max` per `slot_type` within one sampler interval (issue #531) |
| `harvest.workflow.active` | Gauge | `worker.rs` — `spawn_workflow_active_sampler`, periodic (`poll_interval`, 5 s default). Shard-local `COUNT(*) … GROUP BY (workflow_name, state) WHERE state IN ('RUNNING','PAUSED')`, aggregated **across all shards** of the worker's `ShardedDbPool` (summed per `(workflow, state)`) so the population is fleet-wide, not default-shard-only. A read failure skips the whole tick so an outage never false-clears the gauge; drained `(workflow, state)` pairs are zero-filled (issue #770) |
| `harvest.worker.slot_target` | Gauge | `slot_tuner.rs` — `spawn_slot_tuner_loop`, periodic (`poll_interval`). The adaptive slot tuner's current band-clamped resize target for one slot type; only emitted when `WorkerConfig::with_slot_tuner` is configured (issue #548) |
| `harvest.worker.tuner_decisions` | Counter | `slot_tuner.rs` — `spawn_slot_tuner_loop`, once per control-loop tick, with the decision that actually took effect after band clamping (issue #548) |
| `harvest.schedule.runs` | Counter | `scheduler.rs` — `tick_one_workflow_schedule` / DAG tick, on successful dispatch |
| `harvest.schedule.skipped` | Counter | `scheduler.rs` — `tick_one_workflow_schedule` / DAG tick, when a run is skipped |
| `harvest.schedule.overdue` | Gauge | `scheduler.rs` — `sample_overdue_schedules`, emitted per schedule by the worker's overdue sampler (`spawn_schedule_overdue_sampler`, on the `poll_interval` cadence, per shard). `1` when an *active* schedule is past its own cadence grace (`now − next_run_at > cadence step + jitter + tick`), `0` otherwise. Runs on the worker, not the scheduler tick, so a wedged tick cannot suppress its own health signal (issue #696). Paused / auto-paused / manual / exhausted / at-capacity schedules read `0`; deleted schedules go stale (standard gauge property). |
| `harvest.retention.deleted` | Counter | `retention.rs` — `RetentionRuntime` tick, once per workflow type with a real (non-dry-run) deletion; labeled by workflow type so per-type retention overrides are confirmable (issue #737). `sum(harvest.retention.deleted)` equals the aggregate **workflow-history** deletion count for the tick, excluding orphaned `harvest_completion_deliveries` reclaims (issue #921), which have no workflow to attribute. |
| `harvest.retention.summary_deleted` | Counter | `retention.rs` — summaries deleted by the tiered-retention summary GC pass, once per workflow type with a real (non-dry-run) deletion; labeled by workflow type. A distinct member of the retention metric family from `harvest.retention.deleted` (history rows), so the two tiers are observable independently (issue #752). |
| `harvest.retention.rate_limit_buckets_deleted` | Counter | `retention.rs` — inert per-tenant rate-limit buckets collected by the idle-bucket GC pass, once per key family with a real (non-dry-run) deletion, per shard (issue #1127). Labeled by the bounded `family` (`dyn-rate` / `start-throttle`), **never** by the bucket key — the key is the unbounded per-tenant value this pass exists to collect, so labelling by it would trade an unbounded table for an unbounded metric series. A flat zero while `harvest_rate_limit_buckets` keeps growing means either the GC is disabled (`RetentionConfig::rate_limit_bucket_retention_secs = None`) or every candidate is pinned by a live dependent; `GET /admin/retention` reports the per-shard count either way. |
| `harvest.scanner.tick` | Counter | `timeout.rs` / `poison_pill.rs` / `worker.rs` / `retention.rs` / `scheduler.rs` — incremented **unconditionally at the end of every background control-loop iteration**, including no-work iterations and iterations whose pass returned an error, via the single `scanner_health::record_scanner_tick` choke point (issue #797). Unlike every other loop metric here (which only emits when there is work), a **flat-lined series means the loop is wedged**, not idle. There are seven labels but **five** spawned loops: `sla` and `external_outbox` are enforcement responsibilities inside the `timeout` loop and are ticked **by that loop**, so the three share one liveness fate and cannot diverge (ticking them mid-pass instead would put them behind a `?` and skip them on a transient DB error, giving a flat-lined counter two possible meanings). A multi-shard worker runs one `timeout`/`poison_pill`/`pause_auto_resume` loop **per shard** under one label, so the counter carries a bounded `shard` label (the shard id, or `none` for the process-wide `retention`/`schedule` loops and single-shard deployments). Without it every per-shard instance would share one series on one scrape target and a healthy shard's ticks would hold `rate(...) > 0` while a sibling's loop was dead — **masking** the wedge, not merely failing to localise it. The `scanner_liveness` check complements this by tracking each instance, reporting the worst, and listing **every** stale shard in the check payload. The same choke point bumps an in-process last-tick registry surfaced by the `scanner_liveness` check in `GET /admin/preflight`, so liveness is observable with no metrics pipeline configured. |
| `harvest.workflow.nondeterministic_block` | Counter | `worker.rs` — `block_workflow_for_non_determinism`, once per non-terminal replay-divergence block entry (incl. re-blocks); the runtime companion to the `harvest.workflow.non_determinism` detection counter (issue #603) |
| `harvest.workflow.start_throttled` | Counter | `api.rs` (HTTP/batch) + `scheduler.rs` (scheduled/buffered fires) — once per workflow start deferred by a start throttle because the per-key token bucket was empty (issue #607) |
| `harvest.concurrency.superseded` | Counter | `execution.rs` — `emit_start_cancel_metrics`, called after the start transaction commits, once per running execution cancelled because a newer run for the same concurrency key superseded it under the latest-wins strategy (`on_conflict = cancel_running`, issue #811). A superseded run **also** increments `harvest.workflow.terminal{outcome="cancelled"}` — this counter isolates the supersede subset from operator/parent-close cancellations. A same-shard completion-trigger target's own supersede is collected (not emitted) by `completion_trigger::evaluate_triggers_for_execution_collecting` and emitted by its caller only after THAT caller's own enclosing transaction commits (issue #1197, item 1) — closing a residual where the prior eager, in-transaction emission could over-count on a rollback. |
| `harvest.concurrency.residual_over_limit` | Counter | `concurrency.rs` — `supersede_inner`, once per latest-wins admission that could not shed its full computed overflow because every remaining over-limit run was a protected in-flight (nested) admission (issue #1197, item 2). Promotes a pre-existing `tracing::warn!` to a counter for a nested self-referential `cancel_running` trigger admission (cancelling an incumbent synchronously starts a target that also declares `cancel_running` on the same key); the key stays transiently over its limit and self-heals on the next ordinary admission. The only reachable code path passes no caller-supplied recorder, so emission falls back to the process-global `admission_gate::global_admission_metrics()` recorder (mirroring the identical fallback `completion_trigger.rs`'s admission-gate-block branch already uses). |
| `harvest.webhook.received` | Counter | `webhook_receiver.rs` — every request that reaches an inbound webhook receiver route, regardless of outcome (issue #344) |
| `harvest.webhook.rejected` | Counter | `webhook_receiver.rs` — every inbound webhook request rejected: signature/timestamp/replay verification failure, payload parse failure, mapping-function rejection, or missing idempotency key. Never fires for `accepted`/`idempotent_replay` (issue #344) |
| `harvest.saga.compensated` | Counter | `saga.rs` — `run_compensations` (via `WorkflowContext::observe_saga_unwind_start`), exactly once per real compensation sequence: a non-empty `compensate_all` / step-failure unwind actually running forward (issue #801) |
| `harvest.saga.compensation_failed` | Counter | `saga.rs` — `run_compensations` (via `WorkflowContext::observe_saga_unwind_failed`), exactly once per unwind ending with ≥1 compensation error — the `SagaCompensationFailed` dangling-state case, counted even when the author catches the error (issue #801) |
| `harvest.activity.panic` | Counter | `worker.rs` — activity/local-activity dispatch boundary, once per panicking attempt: a `#[activity]` body panic caught via `catch_unwind` and converted to a retryable typed `HandlerPanic` failure, distinct from ordinary `Err` failures and from process-crash quarantine (issue #782) |
| `harvest.workflow.panic` | Counter | `worker.rs` — `process_workflow_task`, on every panic entry (each bounded re-dispatch and the final terminal failure): a `#[workflow]` body panic caught in the executor and contained under the panic-retry budget (issue #782) |
| `harvest.canary.roundtrip` | Histogram | `worker.rs` — `process_workflow_task` Completed arm, when the workflow is a built-in synthetic liveness canary (`canary::is_canary_workflow`): wall-clock seconds from start-requested to terminal completion of the throwaway probe workflow. Distinct from the #512 replay canary (issue #796) |
| `harvest.canary.success` | Counter | `worker.rs` — `process_workflow_task` Completed arm, once per canary probe reaching terminal completion (canary runs emit this **instead of** `harvest.workflow.terminal`, so probes never pollute business SLO counters — AC8) (issue #796) |
| `harvest.canary.failure` | Counter | `worker.rs` — `process_workflow_task` Failed arm, and `timeout.rs` — `enforce_workflow_execution_timeouts` (a probe that does not complete within its per-probe timeout is a failure, AC6): once per canary probe that did not reach terminal completion (issue #796) |
| `harvest.completion_trigger.skipped` | Counter | `completion_trigger.rs` — `evaluate_triggers_for_execution`, on output-guard skips (`condition_unmet` = guard evaluated false, once per fresh skip — a redelivered, already-resolved skip records `deduped` on `harvest.completion_trigger.fires` instead; `condition_invalid` = stored condition unparseable/over-cap, fail-closed with no fires row — the fire is lost for that terminal unless evaluation re-enters, so alert on this reason and re-trigger by hand; see [`docs/completion-triggers.md`](completion-triggers.md) "Fail-closed on invalid stored conditions"). Best-effort on the operator cancel/terminate and parent-close-cascade paths (no recorder threaded there); the fires-row `outcome` column is the authoritative skip record (issue #810) |
| `harvest.signal.received` | Counter | `worker.rs` — `process_workflow_task`, once per durably-delivered `SignalReceived` (the live-only `ingest_due_timers_and_signals` choke point, beside the `harvest.signal.deliver` span). Never on replay (issue #684). Labeled `workflow`+`queue` only — the signal name is a span-only attribute, never a metric label (issue #684, Codex P2: free-form send route, no declared registry to bound it) |
| `harvest.signal.unhandled` | Counter | `worker.rs` — `process_workflow_task`, emitted **post-commit in the `Persisted` arm** (same discipline as `harvest.update.completed/failed`), so it counts **durable terminal outcomes only**. The terminal outcome's `unhandled_signals` map is computed by `drive_workflow` after the #546 push-handler flush and collected before persist; the worker **sums** the per-name map into one increment per unconsumed occurrence against the single `(workflow, queue)` series — the signal name is NOT a metric label (issue #684, Codex P2). Emission is downstream of a successful commit — and therefore of the #603 ND-block gate (`Failed{nd:Some}` early-returns) and `check_paused_and_park` (a claimed-then-paused race returns via `ParkedPaused`, a persist failure via `Err` — neither reaches the emit), so a discarded cycle's retry/resume cannot double-count. Once per delivered signal left unconsumed at a **graceful Completed/Failed** terminal outcome reached through the workflow drive; lost signal-or-deadline races (#476) excluded. **Known limitation: forced-failure / scanner terminal paths are NOT counted** (they have no driven matcher) — `TIMED_OUT`, `CANCELLED`, `TERMINATED`, parent-close cascade, and history-cap failure. For a timed-out stuck run watch `harvest.workflow.timeout` + the stack API instead (issue #684) |
| `harvest.update.admitted` | Counter | `store.rs` — `admit_update_event`, post-commit, once per durably admitted update (HTTP `admit_update`, Vantage UI, `update_with_start` — the latter emits at its own outer-commit boundary — and the in-process typed client, which now threads a `MetricsRecorder` through `WorkflowHandleClient::execute_update_in_process` so this path is also counted). Labeled `workflow`+`queue` only — the update name is NOT a metric label (issue #684, Codex P2: admission is at the free-form update route boundary before the name is resolved against a declarative-or-imperative handler, so it cannot be bounded by construction) |
| `harvest.update.rejected` | Counter | `api.rs` (plugin) — the `admit_update` and `update_with_start` handlers, once per update rejected by its registered validator before admission (a durable pre-admission 422). Validator rejections only; non-RUNNING/paused state conflicts are caller errors, not counted (issue #684) |
| `harvest.update.completed` | Counter | `worker.rs` — `process_workflow_task` `Persisted` arm, post-commit, exactly once per `UpdateCompleted` (name resolved from the `UpdateAdmitted` events in history) (issue #684) |
| `harvest.update.failed` | Counter | `worker.rs` — `process_workflow_task` `Persisted` arm, post-commit, exactly once per `UpdateFailed` (issue #684) |
| `harvest.update.duration` | Histogram | `worker.rs` — `emit_update_result_metrics`, alongside `harvest.update.completed`/`failed` on the **same** post-commit path (the terminal/suspend `Persisted` arm and the two inline external-signal branches). Wall-clock seconds from the recorded `UpdateAdmitted.timestamp` to the terminal recording (`Utc::now()` at emit, clamped so a clock-skew negative delta records `0`). Rejected updates are excluded (no handler runs); an update whose admit is not in the loaded history skips the sample (the counter still fires). Shares the completed/failed counters' delivery semantics — exactly-once on the happy path; a crash after the persist commit but before the post-commit emit drops the sample (never a double-count) (issue #781) |
| `harvest.mutex.wait_duration` | Histogram | `worker.rs` — `persist_mutex_acquire_park`, on grant: wall-clock seconds a workflow waited to acquire a durable mutex, from request (enqueued as a FIFO waiter) to grant (issue #691) |
| `harvest.mutex.held_duration` | Histogram | `worker.rs` — `process_mutex_releases_from_commands`, on release: wall-clock seconds a durable mutex was held, from grant (`MutexGranted.acquired_at`) to release (drop / explicit / terminal sweep / lease reclaim) (issue #691) |
| `harvest.mutex.contention_depth` | Gauge | `worker.rs` — `persist_mutex_acquire_park`, at the moment a grant is made: the FIFO waiter-queue length for the key (number of workflows waiting on that mutex key) (issue #691) |
| `harvest.connector.received` | Counter | `connector/runtime.rs` (plugin) — once per message pulled from a broker source, before dispatch, regardless of outcome (issue #944) |
| `harvest.connector.dispatched` | Counter | `connector/runtime.rs` (plugin) — the **settlement breakdown**: exactly one sample per received message, so the series sums to `harvest.connector.received`. `dispatched` (a fresh execution), `idempotent_replay` (harvest recognised the redelivery and returned the existing run), `deferred` (a start throttle, issue #607, parked it — a **success** for ack purposes), `dead_lettered` (poison; see `harvest.connector.poisoned` for the reason), or `retried` (a transient harvest-side failure; the message is left un-acked for redelivery) (issue #944) |
| `harvest.connector.poisoned` | Counter | `connector/runtime.rs` (plugin) — once per message quarantined to a dead-letter destination: a deterministic decode failure (`malformed`), a mapping-function rejection repeated `poison_threshold` times (`mapping_rejected`, default 3, mirroring `poison_pill_threshold` #367), or a permanent harvest rejection (`target_rejected`). A poisoned message is **acked** so one bad message never wedges a partition (issue #944) |
| `harvest.connector.lag` | Gauge | `connector/runtime.rs` (plugin) — sampled per pass from `EventSource::lag()`, for sources whose client exposes it. **Kafka**: high watermark minus the committed offset, clamped up to the low watermark so records retention already deleted are not counted as a backlog the consumer can never work off. A partition the group has never committed is baselined by the **effective** `auto.offset.reset` (the `earliest` default, or a caller's `.property("auto.offset.reset", "latest")` override), so a latest-starting group owes nothing rather than reporting the topic's whole retained history forever. Summed across **every partition of the topic** (from `fetch_metadata` + `committed_offsets`, deliberately NOT `assignment()`). Consumer lag is a property of the *group*: an assignment-scoped sum is a property of one replica, so with N replicas each would report roughly `1/N` of the backlog and `max by (source)` would surface the largest subtotal while hiding a partition stalled on another replica. Group-wide means every replica reports the same number, so one aggregation is correct for both adapters. The cost is that broker calls per sample scale with replica count — bounded work on the lag interval, not the message path. **SQS**: `ApproximateNumberOfMessages` **plus** `ApproximateNumberOfMessagesNotVisible` — a message abandoned for visibility-timeout retry becomes *not visible*, so the visible count alone drains toward zero during an outage while the outstanding population is unchanged. Both read "still owed to this consumer", not "immediately fetchable". Sources with no lag concept simply never emit (issue #944) |

### Label sets

| Metric | Labels |
|--------|--------|
| `harvest.workflow.started` | `workflow`, `queue` |
| `harvest.workflow.duration` | `workflow`, `queue`, `status` (`completed\|failed\|suspended\|continued_as_new`) |
| `harvest.activity.duration` | `activity`, `queue`, `status` (`completed\|failed`) |
| `harvest.activity.failed` | `activity`, `workflow.type`, `error.type`, `non_retryable` |
| `harvest.activity.attempts` | `activity`, `queue`, `outcome` (`completed\|failed`) |
| `harvest.activity.retries` | `activity`, `queue` |
| `harvest.activity.pause_actions` | `activity` (bounded **by bucketing**: the pause routes accept an unregistered name on purpose, so the raw value is caller-controlled free text — the emitter resolves it against the registered activity catalogue and substitutes `__unregistered__` when absent, exactly as the #684 update-name label does), `action` (`pause\|resume`, bounded by `ActivityPauseAction`) |
| `harvest.timer.started` | _(none)_ |
| `harvest.queue.depth` | `queue` |
| `harvest.queue.schedule_to_start` | `queue` |
| `harvest.queue.oldest_pending_age` | `queue` |
| `harvest.dlq.entries` | `shard` |
| `harvest.shard.stranded_pending` | `shard` |
| `harvest.shard.dispatched` | `shard` |
| `harvest.replication.lag_seconds` | `shard` |
| `harvest.replication.lag_bytes` | `shard` |
| `harvest.replication.standbys` | `shard` |
| `harvest.replication.observable` | `shard` |
| `harvest.shard.generation` | `shard` |
| `harvest.shard.fenced` | `shard` |
| `harvest.audit.export_lag` | `shard` |
| `harvest.audit.export_observed` | `shard` |
| `harvest.audit.exported` | `shard` |
| `harvest.task.capability_miss` | `queue`, `task_type` (`workflow\|activity`), `outcome` (`released\|escalated\|escalated_never_offered`) |
| `harvest.queue.paused` | `queue` |
| `harvest.worker.slots_in_use` | `slot_type` (`workflow\|activity`) |
| `harvest.worker.slots_available` | `slot_type` (`workflow\|activity`) |
| `harvest.worker.slot_target` | `slot_type` (`workflow\|activity`) |
| `harvest.workflow.active` | `workflow`, `state` (`running\|paused`) |
| `harvest.worker.tuner_decisions` | `slot_type` (`workflow\|activity`), `decision` (`grow\|shrink\|hold`) |
| `harvest.schedule.runs` | `kind` (`workflow\|dag`), `name` |
| `harvest.schedule.skipped` | `kind`, `name`, `reason` (`paused\|max_active_runs_reached\|catchup_disabled`) |
| `harvest.schedule.overdue` | `kind` (`workflow\|dag`), `name` |
| `harvest.retention.deleted` | `workflow` |
| `harvest.scanner.tick` | `scanner` (`timeout\|sla\|poison_pill\|external_outbox\|retention\|schedule\|pause_auto_resume`) |
| `harvest.retention.summary_deleted` | `workflow` |
| `harvest.retention.rate_limit_buckets_deleted` | `family` (`dyn-rate\|start-throttle`) — never the bucket key (unbounded per tenant) |
| `harvest.workflow.nondeterministic_block` | `workflow`, `queue` |
| `harvest.workflow.history_bloat` | `workflow` (= `METRIC_LABEL_WORKFLOW`) — no `execution.id` label (ADR-0001 §7); see `GET /api/harvest/workflows?history_bloat_min_events=<N>` to find the specific execution(s) driving a crossing (issue #704) |
| `harvest.workflow.start_throttled` | `workflow` (the resolved throttle key is deliberately **not** a label — unbounded cardinality; see `GET /admin/start-throttle` for per-key backlog, issue #607) |
| `harvest.concurrency.superseded` | `workflow` (the **superseded** run's workflow type; the concurrency key is deliberately **not** a label — unbounded tenant input, per ADR-0001 §7. Use `GET /admin/concurrency` for the per-key view and the effective `on_conflict` strategy, issue #811) |
| `harvest.concurrency.residual_over_limit` | `workflow`, `gap` (how many runs the key is over its limit, bounded by `SUPERSEDE_SCAN_LIMIT`) — the concurrency key is never a label, same ADR-0001 §7 rule as its `superseded` sibling (issue #1197) |
| `harvest.webhook.received` | `path` (registered `#[webhook(path = ...)]` bindings only, closed set), `outcome` (`accepted\|idempotent_replay\|verify_failed\|parse_failed\|missing_idempotency\|internal_error`) |
| `harvest.webhook.rejected` | `path`, `outcome` (never `accepted`/`idempotent_replay`) |
| `harvest.saga.compensated` | `workflow`, `queue` |
| `harvest.saga.compensation_failed` | `workflow`, `queue` |
| `harvest.signal.received` | `workflow`, `queue` — **no `name`**: signal names come from the free-form send route and have no declared registry to bound them (issue #684, Codex P2) |
| `harvest.signal.unhandled` | `workflow`, `queue` — **no `name`** (same reason; the worker sums the per-name unconsumed map into the single `(workflow, queue)` series) |
| `harvest.update.admitted` | `workflow`, `queue` — **no `name`**: admission happens at the free-form update route `POST /workflows/{id}/update/{name}` before the name is resolved against a handler, and handlers register both declaratively AND imperatively (unknown until execution), so it cannot be bounded by construction; per-name visibility lives on `update.completed`/`failed`/`rejected` (issue #684, Codex P2) |
| `harvest.update.rejected` | `workflow`, `name` (update name, bounded — validator rejection fires only for a registered handler) |
| `harvest.update.completed` | `workflow`, `name` (update name — inherently bounded: a completed update always ran a real handler), `queue` |
| `harvest.update.failed` | `workflow`, `name` (update name, bounded — an unregistered name's handler-not-found failure → `__unregistered__`; real handlers, declarative or imperative, keep their name), `queue` |
| `harvest.update.duration` | `workflow`, `name` (update name — bounded exactly as `update.completed`/`failed`; unregistered → `__unregistered__`), `queue`, `outcome` (`completed\|failed` — bounded; rejected excluded) |
| `harvest.mutex.wait_duration` | `workflow` — the lock key is high-cardinality (often tenant/entity input) and is deliberately **not** a metric label (ADR-0001 §7) |
| `harvest.mutex.held_duration` | `workflow` — the lock key is deliberately **not** a label (ADR-0001 §7) |
| `harvest.mutex.contention_depth` | `workflow` — the lock key is deliberately **not** a label (ADR-0001 §7) |
| `harvest.completion_trigger.skipped` | `trigger` (trigger UUID — same precedent as `harvest.completion_trigger.fires`), `reason` (`condition_unmet\|condition_invalid`) |
| `harvest.activity.panic` | `activity`, `queue` |
| `harvest.workflow.panic` | `workflow`, `queue` |
| `harvest.canary.roundtrip` | `queue`, `shard` (probed task queue + writable shard; **no `execution.id`** — issue #796) |
| `harvest.canary.success` | `queue`, `shard` |
| `harvest.canary.failure` | `queue`, `shard` |
| `harvest.connector.received` | `source` (the binding's `source_name`, a registered `SourceBinding` — closed set) — the message key, partition, and offset are **never** labels (ADR-0001 §7, issue #944) |
| `harvest.connector.dispatched` | `source`, `outcome` (`dispatched\|idempotent_replay\|deferred\|dead_lettered\|retried` — bounded enum) |
| `harvest.connector.poisoned` | `source`, `reason` (`malformed\|mapping_rejected\|target_rejected` — bounded enum) |
| `harvest.connector.lag` | `source` |

**Cardinality rule:** `execution.id` is **never** a metric label. It is
span-only (see ADR-0001 §4). The `MetricsRecorder` API enforces this by
construction — no `record_*` method accepts an `ExecutionId`.

### Saga compensation metrics (issue #801)

`harvest.saga.compensated` counts **compensation sequences that started
running forward** (unwind start — the earliest outage signal), and
`harvest.saga.compensation_failed` counts **unwinds that finished with at
least one error** (the dangling-state page). The exactly-once contract is
keyed to durable `MarkerRecorded` dedup markers (`saga_compensated:{seq}` /
`saga_compensation_failed:{seq}`): the counter fires only when the marker is
first recorded on the live frontier, so the documented "compensations re-run
on every replay" contract never double-counts, and pre-#801 marker-less
histories are never counted retroactively. The failure counter is emitted
in-Saga rather than at the worker terminal boundary, so it is separable from
`harvest.workflow.terminal{outcome=failed}` and fires even when the workflow
author catches `SagaCompensationFailed` and the run completes.

**Per-unwind coherence (post-review hardening):** the unwind's disposition
is resolved once at unwind start and the failure counter follows it, so
`failed ≤ compensated` holds per unwind. A counted unwind's failure is never
suppressed by a trailing un-awaited signal (the failure marker is recorded
past the drained signal), the cancel-and-compensate pattern is counted (a
trailing `WorkflowCancelled` is transparent to the marker matcher), and an
unwind entered at a drained-signal frontier — canonically a
signal-with-start run whose unwind starts before the staged signal is
awaited — is conservatively uncounted **as a whole** (the `ctx.patched()`
signal-with-start caveat, inherited).

**Crash-window caveat (at-least-once, both counters, durable and in-memory
unwinds alike):** each sample is emitted in-process within the
single-decision-cycle gap before its dedup marker's batch commits — the
start marker persists with the unwind's first dispatch batch, the failure
marker with the post-unwind batch — so a worker crash (or a #383 pause-race
decision discard) inside that gap re-emits after resume. A crash
*mid-unwind* — between compensations, after the first batch committed — is
exactly-once. A *pure in-memory* unwind (zero durable footprint) is the
maximal case: a crash-resume anywhere within its single cycle can re-count —
the metric mirror of the "compensations re-run wholesale" idempotency
contract in `docs/saga.md`.

---

## Grafana dashboard example queries

```promql
# Activity failure rate
rate(harvest_activity_duration_count{status="failed"}[5m])

# Current queue backlog
harvest_queue_depth{queue="default"}

# p99 schedule-to-start latency per queue (canonical worker-capacity SLI, issue #501)
histogram_quantile(0.99, sum by (le, queue) (rate(harvest_queue_schedule_to_start_bucket[5m])))

# Age of the oldest unclaimed eligible task per queue (fast-detection gauge, issue #501)
harvest_queue_oldest_pending_age{queue="default"}

# DLQ depth per shard
harvest_dlq_entries{shard="0"}

# Workflow history-bloat early warnings per type, per 15m window (issue #704).
# One increase per still-running execution the moment it first crosses the
# configured soft threshold (default 75% of event_hard_cap) -- pair a
# crossing with `GET /workflows?history_bloat_min_events=<N>` to rank offenders.
sum by (workflow) (increase(harvest_workflow_history_bloat_total[15m]))

# Worker dispatch-slot utilization per slot type (issue #531):
# "are my workers saturated?" — pairs with queue.depth to tell a worker
# bottleneck apart from a queue/DB bottleneck.
harvest_worker_slots_in_use / (harvest_worker_slots_in_use + harvest_worker_slots_available)

# Adaptive slot tuner's current target vs. the operator-configured band
# (issue #548) — confirms live behaviour matches [min_slots, max_slots].
harvest_worker_slot_target{slot_type="activity"}

# Tuner grow/shrink/hold rate per slot type — a steady stream of grow/shrink
# under flat load usually means the band or step size needs retuning.
rate(harvest_worker_tuner_decisions_total{decision!="hold"}[5m])

# Effective schedule run rate (runs - skips)
rate(harvest_schedule_runs_total[1h]) - rate(harvest_schedule_skipped_total[1h])

# p99 durable-mutex acquire wait per workflow (issue #691) — how long
# contenders queue before entering the critical section.
histogram_quantile(0.99, sum by (le, workflow) (rate(harvest_mutex_wait_duration_bucket[5m])))

# p99 durable-mutex held duration per workflow — long holds serialize peers
# and are the first thing to look at when acquire waits climb.
histogram_quantile(0.99, sum by (le, workflow) (rate(harvest_mutex_held_duration_bucket[5m])))

# Live durable-mutex contention depth (FIFO waiter-queue length at last grant).
harvest_mutex_contention_depth{workflow="apply_ledger_op"}
```

## Full importable dashboard pack

The queries above are seed examples. A complete, versioned, importable
Grafana dashboard covering **the full metric catalogue** (every `METRIC_*`
constant in `telemetry.rs`, plus the literal-named concurrency gauges) ships
at `docs/dashboards/starter-pack-v0.1.0.json`, with import instructions,
prerequisites, and the alert-rule ↔ panel mapping table in
`docs/dashboards/README.md`. Coverage is CI-enforced by
`autumn-harvest/tests/integration/dashboard_pack_docs.rs`: a new `METRIC_*`
constant with no dashboard panel turns the test red.
