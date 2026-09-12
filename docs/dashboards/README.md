# Harvest Starter Dashboard Pack

This directory contains the versioned starter Grafana dashboard pack for
production Harvest deployments:

- `starter-pack-v0.1.0.json` is a single, raw, importable Grafana dashboard
  model (stable uid `harvest-starter-pack`).
- It is the visual companion to the alert pack in
  `../alerts/starter-pack-v0.1.0.json` and the response runbook in
  `../runbooks/harvest-alerts.md` — together they close the
  alert → dashboard → runbook loop.

Panel choices, aggregation choices, and any thresholds mentioned in panel
descriptions are starter defaults, not universal SLOs. Tune them to workload
volume, downstream SLAs, deployment topology, queue count, shard count, and
the cadence of your scheduled workflows — the same framing as the alert
pack's `threshold_policy`.

## Prerequisites

1. **Grafana ≥ 10** (the dashboard is authored at `schemaVersion` 39; the CI
   guard enforces ≥ 36).
2. **A Prometheus datasource** scraping your Harvest workers. The dashboard
   never hardcodes a datasource — every panel resolves through the
   `$datasource` template variable, so you pick the datasource after import
   (see the import steps below).
3. **The `metrics-rs` feature with `MetricsRsRecorder`** wired into your
   Harvest builder, exported through `metrics-exporter-prometheus` (or an
   equivalent `metrics`-crate exporter). See `docs/telemetry.md` for the
   full recipe. The plugin's built-in scrape endpoint alone is **not**
   sufficient for the histogram panels: it exposes no `_bucket` series.
4. **Histogram buckets**: `metrics-exporter-prometheus` renders a histogram
   as a Prometheus summary (no `_bucket` series) unless you configure bucket
   boundaries with `set_buckets_for_metric` using the *underscored* series
   names (e.g. `harvest_queue_schedule_to_start`). Every
   `histogram_quantile` panel in this pack therefore carries a second
   `_sum`/`_count` average target labelled "bucket-less fallback" that works
   without bucket configuration — but configure buckets for real quantiles.
   The recipe is in `docs/telemetry.md`.

## Importing the Dashboard

### Grafana UI

1. In Grafana: **Dashboards → New → Import**.
2. Upload `starter-pack-v0.1.0.json` (or paste its contents), then click
   **Load** and **Import**.
3. The import wizard shows **no datasource prompt**: the JSON deliberately
   ships without an `__inputs` block, and every panel resolves through the
   `$datasource` template variable instead. After import, pick your
   Prometheus datasource from the **Data source** dropdown at the top of the
   dashboard — nothing else is environment-specific.
4. Save (the variable selection persists with the dashboard). The dashboard
   keeps the stable uid `harvest-starter-pack`, so re-importing a newer pack
   version upgrades the same dashboard in place instead of creating a
   duplicate.

### HTTP API

The dashboards API expects the model wrapped in a `{"dashboard": …}`
envelope — POSTing the raw file body is rejected:

```bash
jq -n --slurpfile dash docs/dashboards/starter-pack-v0.1.0.json \
   '{dashboard: $dash[0], overwrite: true}' |
curl -sS -X POST "$GRAFANA_URL/api/dashboards/db" \
  -H "Authorization: Bearer $GRAFANA_TOKEN" \
  -H "Content-Type: application/json" \
  --data-binary @-
```

`overwrite: true` upgrades the existing `harvest-starter-pack` dashboard in
place. Select the Prometheus datasource via the **Data source** dropdown on
first view, exactly as with the UI import.

## Coverage

The pack covers **100% of the Prometheus-visible Harvest metric catalogue**:
every `METRIC_*` constant in `autumn-harvest/src/telemetry.rs` plus the two
literal-named concurrency gauges the metrics-rs adapter emits
(`harvest_concurrency_in_flight`, `harvest_concurrency_deferred`). That
constant list — not the (snapshot) table in ADR-0001 §7 — is the
authoritative catalogue definition.

Coverage is machine-enforced: the CI test
`autumn-harvest/tests/integration/dashboard_pack_docs.rs` extracts the
catalogue from `telemetry.rs` at test runtime and fails when a metric has no
panel, when a query uses a wrong series suffix (counters must be `_total`,
histograms `_bucket`/`_count`/`_sum`, gauges bare — the normalization table
in `docs/alerts/README.md`), when a counter is graphed without
`rate()`/`increase()`, when a quantile panel lacks its bucket-less fallback,
or when a template variable is applied to a series that does not carry the
label. A future `METRIC_*` constant with no panel turns CI red until the
dashboard grows one.

## Layout

One always-expanded **Overview** row curates the seven signals a first
responder needs (start rate, terminal outcomes, failure ratio, queue depth,
schedule-to-start p99, DLQ depth, worker slot utilization). Every other row
is collapsed and lazy-loaded by Grafana, grouped by subsystem: workflow
lifecycle; workflow health (timeouts / SLA / non-determinism); admission &
pacing; activities; circuit breakers; queues & workers; timers; schedules &
triggers; DLQ & quarantine; cache, retention & shards; concurrency & rate
limits; payloads & offload; webhooks, sessions & queries; and a final
readiness-checks row of text panels.

## Alert ↔ Panel Mapping

Every rule in `../alerts/starter-pack-v0.1.0.json` maps to a panel; the
panel's description carries `Alert: <rule_id>` so an operator landing on the
panel finds the way back to the rule and its runbook section.

| Alert rule id | Row | Panel | Runbook |
|---|---|---|---|
| `harvest_preflight_failed` | Readiness checks | Readiness: Deployment preflight (text) | [runbook](../runbooks/harvest-alerts.md#harvest_preflight_failed) |
| `harvest_no_active_workers` | Readiness checks | Readiness: Worker fleet coverage (text) | [runbook](../runbooks/harvest-alerts.md#harvest_no_active_workers) |
| `harvest_queue_uncovered` | Readiness checks | Readiness: Queue coverage (text) | [runbook](../runbooks/harvest-alerts.md#harvest_queue_uncovered) |
| `harvest_worker_saturation` | Readiness checks | Readiness: Worker fleet saturation (text; proxies: Overview → Worker slot utilization) | [runbook](../runbooks/harvest-alerts.md#harvest_worker_saturation) |
| `harvest_queue_schedule_to_start_high` | Overview | Schedule-to-start latency p99 | [runbook](../runbooks/harvest-alerts.md#harvest_queue_schedule_to_start_high) |
| `harvest_queue_backlog_growth` | Overview | Queue depth (companion: Queues & workers → Oldest pending task age) | [runbook](../runbooks/harvest-alerts.md#harvest_queue_backlog_growth) |
| `harvest_worker_slot_saturation` | Overview | Worker slot utilization | [runbook](../runbooks/harvest-alerts.md#harvest_worker_slot_saturation) |
| `harvest_activity_failure_surge` | Activities | Activity failure rate | [runbook](../runbooks/harvest-alerts.md#harvest_activity_failure_surge) |
| `harvest_dlq_growth` | Overview | DLQ entries by shard | [runbook](../runbooks/harvest-alerts.md#harvest_dlq_growth) |
| `harvest_schedule_missed_runs` | Schedules & triggers | Schedule runs vs skipped | [runbook](../runbooks/harvest-alerts.md#harvest_schedule_missed_runs) |
| `harvest_retention_lag` | Cache, retention & shards | Retention deletions by shard | [runbook](../runbooks/harvest-alerts.md#harvest_retention_lag) |
| `harvest_shard_unready` | Readiness checks | Readiness: Shard readiness (text) | [runbook](../runbooks/harvest-alerts.md#harvest_shard_unready) |
| `harvest_shard_undrained` | Cache, retention & shards | Stranded pending tasks by shard (companion: Dispatch rate by shard) | [runbook](../runbooks/harvest-alerts.md#harvest_shard_undrained) |
| `harvest_no_compatible_worker` | Readiness checks | Readiness: Build-routing compatibility (text) | [runbook](../runbooks/harvest-alerts.md#harvest_no_compatible_worker) |
| `harvest_schedule_ha_domination` | Schedules & triggers | Schedule HA fire attempts | [runbook](../runbooks/harvest-alerts.md#harvest_schedule_ha_domination) |
| `harvest_workflow_failure_rate` | Overview | Workflow failure ratio | [runbook](../runbooks/harvest-alerts.md#harvest_workflow_failure_rate) |
| `harvest_activity_success_ratio` | Activities | Activity success ratio | [runbook](../runbooks/harvest-alerts.md#harvest_activity_success_ratio) |
| `harvest_activity_retry_storm` | Activities | Activity retry rate | [runbook](../runbooks/harvest-alerts.md#harvest_activity_retry_storm) |
| `harvest_activity_retry_storm_critical` | Activities | Activity retry rate (same panel; page tier > 20/s) | [runbook](../runbooks/harvest-alerts.md#harvest_activity_retry_storm) |
| `harvest_workflow_non_determinism` | Workflow health | Non-determinism detections (+ Non-determinism blocks entered) | [runbook](../runbooks/harvest-alerts.md#harvest_workflow_non_determinism) |
| `harvest_saga_compensation_spike` | Sagas | Saga compensations | [runbook](../runbooks/harvest-alerts.md#harvest_saga_compensation_spike) |
| `harvest_saga_compensation_failed` | Sagas | Saga compensation failures | [runbook](../runbooks/harvest-alerts.md#harvest_saga_compensation_failed) |
| `harvest_update_rejected_rate` | Signal & update lifecycle | Update validator rejections | [runbook](../runbooks/harvest-alerts.md#harvest_update_rejected_rate) |
| `harvest_signal_unhandled_rate` | Signal & update lifecycle | Unhandled signals | [runbook](../runbooks/harvest-alerts.md#harvest_signal_unhandled_rate) |
| `harvest_workflow_population_leak` | Workflow lifecycle | Active workflows by type & state | [runbook](../runbooks/harvest-alerts.md#harvest_workflow_population_leak) |
| `harvest_workflow_history_bloat` | Workflow health | Workflow history bloat (early warning) | [runbook](../runbooks/harvest-alerts.md#harvest_workflow_history_bloat) |
| `harvest_queue_paused_too_long` | Queues & workers | Paused queues (dispatch held) | [runbook](../runbooks/harvest-alerts.md#harvest_queue_paused_too_long) |
| `harvest_scanner_stalled` | Background control loops (scanner liveness) | Scanner tick rate | [runbook](../runbooks/harvest-alerts.md#harvest_scanner_stalled) |
| `harvest_no_capable_worker` | DLQ & quarantine | Capability misses (handler not registered) | [runbook](../runbooks/harvest-alerts.md#harvest_no_capable_worker) |
| `harvest_capability_miss_never_offered` | DLQ & quarantine | Capability misses (handler not registered) | [runbook](../runbooks/harvest-alerts.md#harvest_capability_miss_never_offered) |
| `harvest_capability_miss_release_sustained` | DLQ & quarantine | Capability misses (handler not registered) | [runbook](../runbooks/harvest-alerts.md#harvest_capability_miss_release_sustained) |
| `harvest_replication_down` | Cross-region DR → *Connected standbys (0 = replication is DOWN)*, with *WAL backlog* for severity |
| `harvest_replication_lag_high` | Cross-region DR → *Measured RPO*, with *Shard write-authority generation* for failover skew |
| `harvest_shard_fenced` | Cross-region DR → *Workers fenced (never self-healing)* |
| `harvest_replication_unobservable` | Cross-region DR → *Replication observable (0 = the other DR panels are STALE)* |
| `harvest_replication_rpo_unknown` | Cross-region DR → *RPO known (0 = readable but unmeasurable)* |
| `harvest_audit_export_lag_high` | Audit export to SIEM → *Audit export lag (oldest unshipped audit record)*, with *Audit records exported* to tell a sink outage from a quiet fleet |
| `harvest_audit_export_unobservable` | Audit export to SIEM → *Audit export observed (0 = the lag panel above is STALE)* |

### Readiness-style alerts (no native metric)

Five alert rules (`harvest_preflight_failed`, `harvest_no_active_workers`,
`harvest_worker_saturation`, `harvest_shard_unready`,
`harvest_no_compatible_worker`) have **no native ADR-0001 metric** — their
signal source is the management API / CLI. Rather than inventing dishonest
proxy panels, the dashboard's final "Readiness checks" row carries one text
panel per rule with the rule's description, its `first_action` CLI command,
the API route, and the runbook link. Run those checks directly (or export
the API result through your own probe with bounded labels) —
`docs/alerts/README.md` documents the recommended cadence.

## Template Variables

| Variable | Type | Sourced from | Applied to |
|---|---|---|---|
| `$datasource` | datasource | your Prometheus datasources | every panel |
| `$workflow` | query, multi + All | `label_values(harvest_workflow_started_total, workflow)` | series carrying a `workflow` label, including `harvest_retention_deleted` (issue #737); series labelled `workflow_type` (history size, continue-as-new, payload metrics) use `workflow_type=~"$workflow"` |
| `$queue` | query, multi + All | `label_values(harvest_queue_depth, queue)` | series carrying a `queue` label |
| `$shard` | query, multi + All | `label_values(harvest_dlq_entries, shard)` | **only** series that carry a `shard` label (e.g. `harvest_dlq_entries`, `harvest_shard_stranded_pending`, `harvest_shard_dispatched_total`, the cross-region DR series (`harvest_replication_lag_seconds`, `harvest_replication_lag_bytes`, `harvest_replication_standbys`, `harvest_replication_observable`, `harvest_replication_rpo_known`, `harvest_shard_generation`, `harvest_shard_fenced_total`), and the canary series) |

Variables are applied per-panel only where the series actually carries the
label — applying `shard=~"$shard"` to an unlabelled series would silently
empty the panel, so label-less series (e.g. `harvest_timer_started_total`,
`harvest_admission_gates_active`) take no selector at all. `key`-labelled
series (concurrency / rate limits) deliberately get `topk(10, …)` panels and
**no** template variable: key values are derived from tenant input and are
only bounded by author discipline.

Multi-replica note: **replica-global** sampler gauges — every worker replica
samples the same shared DB-derived value (`harvest_queue_depth`,
`harvest_queue_oldest_pending_age`, `harvest_dlq_entries`,
`harvest_shard_stranded_pending`, `harvest_workflow_history_oversized`,
`harvest_admission_gates_active`, and the per-key concurrency / rate-limit
gauges) — aggregate with `max`, never `sum`, to avoid replica
double-counting. **Replica-local** gauges, where each replica owns its own
value (`harvest_worker_slots_in_use` / `_available`), sum correctly across
the fleet; the per-replica slot panels legend the `instance` label instead.

## Lint Validation

The pack lints clean under the official
[`grafana/dashboard-linter`](https://github.com/grafana/dashboard-linter)
(`dashboard-linter lint starter-pack-v0.1.0.json`, run from this
directory), which parse-validates every panel's PromQL, checks the
templated datasource, counter `rate()`/`increase()` aggregation,
`$__rate_interval` usage, units, and panel titles/descriptions. The
sibling `.lint` file excludes exactly five rules, each with a documented
reason: the `job`/`instance` matcher and template-variable rules (the
ADR-0001 §7 label contract has no job/instance labels, and issue #754
mandates `workflow`/`queue`/`shard` as the navigation dimensions) and the
`uneditable-dashboard` rule (a starter pack is meant to be tuned in
place). The linter validates the dashboard *model*; a manual import into a
real Grafana ≥ 10 instance remains the final pre-merge verification step.

## Versioning

The filename carries the pack version (`v0.1.0`); the dashboard `uid`
(`harvest-starter-pack`) and title stay stable across versions so importing
a newer pack upgrades in place. Grafana's own integer `version` field is an
edit counter, not the pack version. Pack versions follow the alert pack's
convention: a new version is a new file, and the CI test pins the current
one.

No panel in this pack requires a new `WorkflowEvent` variant, a migration,
or an exporter change beyond the issue #754 metrics-rs adapter bridge fix
(`harvest.workflow.timeout`, `harvest.payload.bytes`,
`harvest.payload.rejected` — previously catalogued but not bridged by the
adapter). Bridged ≠ emitted: `harvest.workflow.timeout` is emitted
end-to-end (the timeout scanner calls the recorder), so its panel populates
immediately; the two payload byte-cap metrics still have **no engine
emission call sites** as of v0.1.0 (a pre-existing issue #252 gap — the cap
sites construct the error without calling the recorder), so the two
payload-cap panels stay empty until emission is wired. Both panels say so in
their descriptions; wiring the emission is a known follow-up from the issue
#754 review.
