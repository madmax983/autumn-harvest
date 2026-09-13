# Runbook: Harvest Starter Alerts

Use this runbook with `docs/alerts/starter-pack-v0.1.0.json`. Every action is
read-only unless explicitly called out as a safe action. Start with the linked
first action, confirm the blast radius, then choose the smallest reversible
step. The pack protects workflow execution; it does not replace app-specific
SLOs.

First responders: import the starter Grafana dashboard pack
(`docs/dashboards/starter-pack-v0.1.0.json`) for the visual side of every
metric-backed rule below — each alert maps to a named panel (the mapping
table lives in `docs/dashboards/README.md`), and each mapped panel's
description links back to its section in this runbook.

## First 60 seconds — one-call incident triage

When an alert fires or a page lands, hit **one** endpoint first:

```bash
curl -s https://your-app/api/harvest/admin/status | jq
```

`GET /api/harvest/admin/status` (issue #679) is the single incident-triage
entry point. It rolls up five subsystems — workers, shards, dead-letters,
queues, and stalled workflows — into **one** JSON document with a single
cluster verdict (`healthy` / `degraded` / `critical`) and, per subsystem, a
verdict, machine-readable `reason_codes`, and a `drill_down` pointer to the
endpoint that investigates it. Read this first instead of correlating
`/workers/health`, `/admin/shards/health`, `/dead-letters/aggregate`, and
`/workflows?no_progress_minutes=N` by hand: it tells you the overall verdict
and the single worst subsystem in one request, then points you straight at
where to look next.

### Example response

```json
{
  "status": "degraded",
  "as_of": "2026-07-09T14:32:05Z",
  "subsystems": [
    {
      "name": "workers",
      "status": "healthy",
      "reason_codes": [],
      "drill_down": null,
      "active": 4,
      "draining": 0,
      "unhealthy": 0,
      "total": 4
    },
    {
      "name": "shards",
      "status": "healthy",
      "reason_codes": [],
      "drill_down": null,
      "ready": 2,
      "degraded": 0,
      "unavailable": 0
    },
    {
      "name": "dead_letters",
      "status": "degraded",
      "reason_codes": ["dlq_backlog"],
      "drill_down": "/dead-letters/aggregate",
      "total": 7,
      "newest_entry_age_secs": 5400
    },
    {
      "name": "queues",
      "status": "healthy",
      "reason_codes": [],
      "drill_down": null,
      "max_backlog": 12,
      "max_backlog_queue": "default"
    },
    {
      "name": "stalled_workflows",
      "status": "healthy",
      "reason_codes": [],
      "drill_down": null,
      "count": 0
    }
  ],
  "unavailable_shards": []
}
```

Each subsystem block carries its verdict, its `reason_codes`, its headline
numbers (flattened into the block), and a `drill_down` path — which is
populated **only when the subsystem is not `healthy`** and is `null`
otherwise. The `drill_down` paths are relative to the management-API mount
(e.g. `/api/harvest`).

### Subsystem → drill-down

| Subsystem | Headline metrics | `drill_down` when non-`healthy` |
|---|---|---|
| `workers` | `active` / `draining` / `unhealthy` / `total` | `/workers/health` |
| `shards` | `ready` / `degraded` / `unavailable` | `/admin/shards/health` |
| `dead_letters` | `total` / `newest_entry_age_secs` | `/dead-letters/aggregate` |
| `queues` | `max_backlog` / `max_backlog_queue` | `/admin/shards/health` |
| `stalled_workflows` | `count` | `/workflows?no_progress_minutes=N` (N = `stalled_no_progress_minutes`, default 60) |

### How the verdict is computed

The top-level `status` is the **worst of** the five subsystem verdicts —
`critical` dominates `degraded` dominates `healthy`. It never fails wholesale:
an **unreachable shard degrades the affected subsystems** (each non-shard
subsystem that could not read complete data drops to at least `degraded` and
carries the `shard_unreachable` reason code; the `shards` subsystem reflects it
through its own readiness) and the shard is **named in `unavailable_shards`**
rather than returning a `500`. So a partial answer is always honest, never a
silent success and never an error page.

Once you have the verdict, follow the worst subsystem's `drill_down` into the
matching per-domain endpoint — those map directly onto the alert sections
below (e.g. `dead_letters` → `harvest_dlq_growth`, `queues` /
`stalled_workflows` → `harvest_queue_schedule_to_start_high`, `workers` →
`harvest_no_active_workers` / `harvest_worker_saturation`, `shards` →
`harvest_shard_unready`).

### Partial cross-shard list reads (issue #756)

The cross-shard **list/aggregate read** endpoints — `GET /workflows` (including
the `?no_progress_minutes` stalled loader), `GET /workers`,
`GET /workers/health`, `GET /admin/schedules`, `GET /dead-letters`, and
`GET /dead-letters/aggregate` — **degrade instead of failing** when one shard's
pool is unreachable. Rather than turning a single down shard into a whole-request
`500`, they return `200 OK` with the union of the *reachable* shards' results
plus a machine-readable partiality indicator:

```json
{
  "workflows": [ /* … reachable-shard rows … */ ],
  "status": "partial",
  "unavailable_shards": [ { "shard_id": 3, "reason": "database connection for shard 3 could not be acquired" } ]
}
```

A `status` of `partial` (some shards read) or `unavailable` (none read), and a
non-empty `unavailable_shards`, is a **healthy degradation, not an error** — the
data you see is real, just incomplete, and the down shard is *named* rather than
silently dropped. On the happy path (every shard reachable) these endpoints
return their unchanged legacy shape (a bare JSON array, or the paginated
`{ workflows, next_cursor }` object), so there is no change for existing clients.

When you see `status: partial`/`unavailable`, drill into
**`GET /admin/shards/health`** to identify *why* the named shard is down
(unreachable pool, mid shard-add rollout, schema-unreadable) and act on the
`shards` subsystem verdict. The `harvest` CLI's `workflow list` / `worker list` /
`schedule list` / `dlq list` subcommands surface the same partiality as a
`WARNING: cross-shard read is partial; N shard(s) unavailable: …` notice line
above the data.

Note: cross-shard **writes** (batch reset, bulk DLQ replay/discard/redrive,
schedule pause/delete, completion-trigger create) deliberately keep strict
failure semantics — a write that could not reach every shard fails loudly rather
than silently applying to a subset. The single-execution business-id resolver
(`GET /workflows/by-id/…`, issue #805) likewise keeps returning `503` while a
shard is down, to avoid a false `404`.

To **halt new workflow starts** fleet-wide (or by name/queue/shard/owner) while
you investigate — the incident-containment lever above the per-run
pause/cancel/terminate — raise an admission gate. Which start producers honour a
gate, which are exempt-by-design, and how to watch the
`harvest.admission.bypassed` counter are documented in
`docs/operations/admission-gate-producers.md`.

### Configurable thresholds

The verdict boundaries are **starter defaults, not universal SLOs** — every
deployment tolerates a different DLQ depth, queue backlog, and stalled-run
count. Override them per environment with
`HarvestPlugin::with_status_thresholds(StatusThresholds { .. })`.

| Field | Starter default | Effect |
|---|---|---|
| `dlq_degraded_total` | `1` | DLQ total at/above ⇒ `degraded` (any dead letter is worth a look) |
| `dlq_critical_total` | `100` | DLQ total at/above ⇒ `critical` |
| `dlq_critical_recent_secs` | `300` | A dead letter newer than this ⇒ `critical` (active failure) |
| `queue_degraded_backlog` | `1000` | Busiest-queue claimable backlog at/above ⇒ `degraded` |
| `queue_critical_backlog` | `10000` | Busiest-queue claimable backlog at/above ⇒ `critical` |
| `stalled_no_progress_minutes` | `60` | No-progress window for the stalled count and drill-down link |
| `stalled_degraded_count` | `1` | Stalled-execution count at/above ⇒ `degraded` |
| `stalled_critical_count` | `50` | Stalled-execution count at/above ⇒ `critical` |
| `worker_degraded_unhealthy_fraction` | `0.25` | Unhealthy (stale) worker fraction at/above ⇒ `degraded` |
| `worker_critical_unhealthy_fraction` | `0.5` | Unhealthy fraction at/above ⇒ `critical` (zero active with any registered is always `critical`) |

### Where this fits among the health surfaces

- **`/admin/status` is the PULL triage surface** — one call, on demand, when you
  are already responding to an incident.
- The **static alert pack** (`docs/alerts/starter-pack-v0.1.0.json`, the rest of
  this runbook) owns **PUSH alerting** — it tells you *when* to look.
- **`/api/harvest/health`** is **liveness** — is the process up — not a rollup.
- **`/api/harvest/admin/preflight`** is **startup validation** — is this
  deployment safe to promote — run at deploy time, not during an incident.
  One exception, added by issue #797: its `scanner_liveness` check is a *live*
  signal about the answering process's background control loops, so it is also
  the fastest way to identify a wedged loop during an incident — but only for
  the process you point it at (the registry is in-process). See
  [`harvest_scanner_stalled`](#harvest_scanner_stalled).
- **`/api/harvest/admin/config`** is the **effective-config introspection**
  surface (issue #695): what config is this fleet actually running with, right
  now? Pull the resolved effective runtime configuration (secret-free,
  admin-gated) before guessing at a knob:
  `curl -s .../api/harvest/admin/config | jq`. Durations are milliseconds;
  unset ceilings are explicit `null`; secret-bearing fields (notification URLs,
  sharded pool) show only presence booleans/counts.

## harvest_preflight_failed

### Triage steps

1. Run `harvest preflight --output json`.
2. Inspect failing `checks[]` and affected `shards[]`.
3. If the failure is worker coverage, also run `harvest worker health --output json`.
4. If the failure is shard readiness, run `harvest shard health --fail-on-unready --output json`.

### Likely causes

Missing migrations, unreachable shard, no worker for a required queue, stale
workers, disabled scheduler path for registered schedules, DLQ read failure, or
an admin API mounted without the expected auth boundary.

A `catalog_consistency` failure means the registration itself is inconsistent —
a DAG or an opted-in workflow references an activity or child workflow that is
not registered in this process. Read the named references out of
`details.failures`; the fix is to add the handler to `activities![…]` /
`workflows![…]`, or to delete the now-stale declaration. See
[Chapter 10 — Operating the service](../getting-started/10-operations.md#catching-a-forgotten-registration-before-rollout).

### False positives

Warning-only reports can be expected during deploys when workers are draining.
A no-schedule deployment should not warn about scheduler coverage; if it does,
treat that as a preflight bug and keep the deploy gate closed.

### Safe actions

Block promotion, restore the last known-good config, restart only the broken
worker group, or re-run migrations on the affected shard. Do not start, cancel,
reset, or replay workflows as part of preflight triage.

### Escalation criteria

Escalate to the platform owner if any shard is unreachable or migration state
cannot be proven. Escalate to the workflow owner if catalog or schedule
registration is inconsistent with the deployed binary.

## harvest_no_active_workers

### Triage steps

1. Run `harvest worker health --output json`.
2. Run `harvest worker list --queue <queue> --output json` for the missing queue.
3. Check shard coverage with `harvest shard health --fail-on-unready --output json`.
4. Compare worker build/deployment identity against the current release plan.

### Likely causes

Worker deployment crashed, wrong queue name, shard assignment mismatch, workers
drained before replacements were active, or the process cannot heartbeat.

### False positives

Single-purpose queues may be idle by design, but required queues named by
registered workflows or schedules must always have fresh coverage.

### Safe actions

Restart or scale the worker deployment for the named queue, roll back a bad
worker config, or pause the deploy before more starts enter the uncovered
queue. Avoid bulk replay until coverage is fresh.

### Escalation criteria

Escalate if coverage is absent for more than two heartbeat windows after a
worker restart, or if multiple shards report the same missing queue.

## harvest_queue_uncovered

**Data-driven queue coverage gap** (issue #774). A queue has real pending
work but zero live workers are polling it right now. Unlike
`harvest_no_active_workers` above — which checks *static, declared*
required-queue coverage (derived from registered workflow/activity default
queues and schedules) regardless of whether any work is currently
pending — this alert is *data-driven*: it fires only when there is actual
stranded work, so it also catches ad-hoc or dynamically-named queues that
were never declared as required. It is distinct from build-id reachability
(#171) and the pre-cutover handler-coverage gate (#520/#700, see
`docs/runbooks/safe-deploy.md`): a queue can be reported uncovered even when
every worker in the fleet is fully build-compatible and handler-complete —
it simply is not subscribed to that queue name.

### Triage steps

1. Run `harvest queue coverage --json` (or `GET /api/harvest/admin/queue-coverage`).
2. Read `uncovered` / `total_uncovered_queues` for the single-field CI-gate
   answer, then walk `items[]` for the specific queue name(s), each carrying
   `pending_count` and a `shard_breakdown`.
3. Each uncovered item's `sample_task_ids` / `sample_execution_ids` (capped
   at 5) name real stranded rows — open one directly with
   `harvest workflow stack <execution_id>`.
4. Run `harvest worker health --output json` (or `harvest worker list --queue <queue>`)
   to confirm no worker is currently subscribed to the named queue.
5. Check `status` in the report: `partial`/`unavailable` means at least one
   shard could not be inspected — walk `shards[]` for the entry carrying
   `status: "unavailable"` to identify it; that shard's pending demand is
   **not** reflected in the report, so do not read a `partial` result as
   "fully covered".

### Likely causes

A worker deployment dropped a queue from its `--queues`/`with_queues(...)`
configuration (typo, config drift during a rolling deploy), the last worker
subscribed to the queue was drained or crashed with no replacement, a
schedule or webhook started routing work to a new/ad-hoc queue name that no
worker was ever configured to poll, or the queue was paused and then had its
pause lifted without a worker being re-added.

### False positives

None from staleness — the report is computed live from the current pending
set and the live worker registry on every call, not sampled. A queue that
is intentionally paused (`GET /admin/queues/paused`) is excluded from the
uncovered list by design (paused work is expected to sit idle, not
stranded) — confirm the queue is not paused before treating a report as a
false positive. Don't stop there, though: a paused queue with pending work
and no live poller is still surfaced, separately, in the report's
`excluded_paused_queues` array — a non-empty entry there is not a false
positive, it's a pre-unpause TODO (unpausing that queue today would make it
uncovered immediately). A `partial`/`unavailable` `status` can under-report
(an unreachable shard's pending demand is invisible), never over-report.

### Safe actions

Add or re-subscribe a worker to the named queue (`with_queues([...])` /
`--queues`), or resume routing if the queue was meant to be a synonym for
one an existing worker already polls. Do not adjust build policy or remove
workflow handlers to fix this — coverage is orthogonal to both (see the
["Queue-coverage check" section](safe-deploy.md#queue-coverage-check--confirm-every-queue-has-a-live-poller-issue-774)
of `docs/runbooks/safe-deploy.md`).

### Escalation criteria

Escalate if a required (previously-covered) queue stays uncovered for more
than two heartbeat windows after a worker restart or deploy, or if
`total_uncovered_queues` grows across successive polls rather than
shrinking once replacement workers come up.

## harvest_worker_saturation

### Triage steps

1. Run `harvest worker health --output json`.
2. Run `harvest worker list --status Active --output json`.
3. Run `harvest concurrency status --output json` to check caps and deferred work.
4. Compare draining workers against the deploy or maintenance window.

### Likely causes

Too many workers are stale or draining, downstream latency is holding
in-flight slots, a concurrency cap is saturated, or a rolling deploy drained
old workers too early.

### False positives

Draining saturation is expected during planned scale-in if backlog is flat and
replacement workers are already active. Stale workers can linger briefly for up
to the configured freshness window.

### Safe actions

Pause further drains, add replacement workers, raise a well-understood
concurrency cap, or roll back a worker release that introduced slow handlers.
Do not terminate draining workers while `in_flight_count` is nonzero unless the
incident is worse than retrying the work.

### Escalation criteria

Escalate when all workers for a required queue are stale or draining, or when
deferred concurrency work grows while downstream owners report elevated errors.

## harvest_worker_slot_saturation

**Worker dispatch-slot bottleneck** (issue #531). Fires when a worker's slot
utilisation (`slots_in_use / (slots_in_use + slots_available)`) for either
`slot_type` (`workflow` or `activity`) stays above 90 % for 5 minutes. This
means the worker itself is the bottleneck — not the queue or the database.

### Triage steps

1. **Read the alert labels.** The firing alert carries `slot_type=workflow` or
   `slot_type=activity` and the Prometheus `instance`/`job` labels that identify
   the saturated worker process. The `harvest.worker.slots_in_use` /
   `harvest.worker.slots_available` gauges are per-process in-memory reads, so
   the scrape target is the authoritative source for which worker and slot type
   is saturated. `harvest worker health --output json` returns aggregate fleet
   counts (healthy/stale/draining by queue/shard) and does **not** include
   per-worker slot-type breakdowns — use it for fleet context only.
2. Run `harvest worker list --output json` to confirm expected worker count per
   queue and check for stale heartbeats or workers already draining.
3. Run `harvest concurrency status --output json` (`GET /api/harvest/admin/concurrency`)
   to check whether per-key concurrency limits are deferring work on top of the
   slot pressure.
4. Compare `harvest.queue.depth` for the same queue:
   - **Slots saturated AND backlog growing** → worker is the bottleneck; raise
     `max_concurrent_workflows` / `max_concurrent_activities` or add workers.
   - **Backlog growing but slots are free** → bottleneck is downstream
     (DB, throttled dependency); adding workers will not help.

### Likely causes

Activity handlers that run slowly or block the executor leave activity slots
occupied for longer than expected. A concurrency cap set below the worker's
physical capacity. Too few workers for the offered throughput. A slow downstream
dependency extending handler latency.

### False positives

Sustained high utilisation is expected and healthy for throughput-oriented
fleets. Tune the threshold per queue and latency target before treating this as
a page. A brief spike during traffic bursts that self-resolves within the
5-minute window will not fire.

### Safe actions

Raise `WorkerConfig::max_concurrent_workflows` or `max_concurrent_activities` if
the host has headroom. Add worker replicas for the affected queue. Investigate
slow activity handlers with `harvest.activity.duration` histograms. Temporarily
lower the ingestion rate if downstream dependencies are the bottleneck.

### Escalation criteria

Escalate when slot saturation persists after adding workers, or when the
`harvest.queue.oldest_pending_age` gauge climbs alongside this alert, indicating
work is stalling rather than simply queuing briefly.

## harvest_queue_schedule_to_start_high

**Primary queue-saturation page** (issue #501). Fires when p99
`harvest.queue.schedule_to_start` for a queue exceeds your configured SLA
threshold (default 30 s). This is the canonical "do I need more workers?"
signal — it measures actual wait time rather than a depth heuristic.

### Triage steps

1. Check the `queue` label in the alert to identify the saturated queue.
2. Run `harvest worker health --output json` — look for stale heartbeats or
   workers draining.
3. Run `harvest worker list --queue <queue> --output json` — verify workers are
   claiming tasks. Zero claimers means workers are absent or filtered by build-id.
4. Run `harvest concurrency status --output json` — a concurrency cap at 100%
   will stall claims even when workers are healthy.
5. Check `harvest_queue_oldest_pending_age{queue=…}` — a large value here
   confirms a task has been stuck longer than the histogram quantile.
6. For a sample stuck execution, run `harvest workflow stack <execution_id>`.

### Likely causes

Worker count too low for current throughput, workers cannot claim the queue
(build-id mismatch, concurrency cap saturated, activity circuit breaker open),
downstream dependency latency inflating per-task duration and reducing worker
throughput, shard readiness issues, or a schedule burst flooding the queue.

### False positives

Planned maintenance windows, expected burst periods, or a very small number
of tasks with unusually large payloads that temporarily saturate workers. Alert
only when the p99 sustains above threshold for the full rule window.

### Safe actions

Scale the worker pool, roll back the deployment that changed queue assignment,
temporarily raise a proven concurrency cap, or pause a schedule that is flooding
the queue. Prefer pausing producers over replaying DLQ entries into an already
saturated queue.

### Escalation criteria

Escalate if wait times persist for two alert windows after fresh workers are
added, or if the queue backs a customer-facing workflow with a breached SLA.

## harvest_queue_backlog_growth

**Secondary signal** — superseded as the primary page by
`harvest_queue_schedule_to_start_high` (#501). Use this as a corroborating
signal or as a fallback when histogram metrics are not yet wired.

### Triage steps

1. Check the alert labels for the queue.
2. Run `harvest worker list --queue <queue> --output json`.
3. Run `harvest concurrency status --output json`.
4. For a sample stuck execution, run `harvest workflow stack <execution_id>`.

### Likely causes

Worker count too low, workers cannot claim the queue, downstream dependency
latency, concurrency caps, shard readiness issues, or incompatible build
routing for part of the fleet.

### False positives

Batch windows can create expected short spikes. Alert only when depth stays
high and growth remains positive for the rule window.

### Safe actions

Scale the worker pool, roll back the deployment that changed queue assignment,
temporarily raise a proven concurrency cap, or pause a schedule that is flooding
the queue. Prefer pausing producers over replaying DLQ entries into an already
backlogged queue.

### Escalation criteria

Escalate if backlog growth persists for two alert windows after workers are
fresh, or if the queue backs a customer-facing workflow with a breached SLA.

## harvest_activity_failure_surge

### Triage steps

1. Check the `activity`, `queue`, and `status` labels.
2. Run `harvest dlq list --limit 25`.
3. For active impacted executions, run `harvest workflow stack <execution_id>`.
4. Check application logs for the named activity handler and downstream calls.

### Likely causes

Downstream outage, bad credentials, schema or payload drift, code regression,
timeout too low, or retry policy that exhausts before the dependency recovers.

### False positives

New workflows can expose intentionally failing validation activities. Treat
failures as actionable when they affect production activities, grow quickly, or
match DLQ entries.

### Safe actions

Roll back the handler release, restore credentials, pause schedules that are
triggering the activity, or tune the timeout/retry policy in a follow-up
release. Replay dead letters only after the root cause is fixed.

### Escalation criteria

Escalate to the downstream owner for sustained external errors. Escalate to
the workflow owner if failures are deterministic payload or code regressions.

## harvest_dlq_growth

### DLQ flood — the first 60 seconds

When the DLQ entry count crosses the alert threshold during an incident, the
first question is *the shape of the fire*, not any single entry. Lead with the
aggregation endpoint instead of paging the flat list or opening `psql`:

```bash
# What is this fire made of? One root cause or ten?
harvest dlq aggregate \
  --group-by workflow_name,failure_signature \
  --since 24h --samples-per-group 3
```

Read the table top-down:

- **One dominant group** (e.g. `onboarding / "stripe: rate limited" → 1,842`):
  a single root cause. Fix the dependency, then bulk-replay with confidence
  using the sample IDs to spot-check first
  (`harvest dlq bulk-replay --dry-run …`).
- **A flat spread across many groups**: unrelated bugs. Do *not* bulk-replay;
  triage case by case from the largest groups down.

`failure_signature` normalizes UUIDs, hex, and numbers in the first line of
each error to fixed placeholders, so the same root cause aggregates identically
across queries and shards. Counts sum across shards; `_other` rolls up the long
tail so totals reconcile to `filtered_total`. Narrow with the same filters as
the list endpoint (`--workflow-name`, `--activity-name`, `--queue-name`,
`--since`, `--until`, `--min-attempts`) and pivot to a single entry via the
`sample_dead_letter_ids`. The endpoint backing this is
`GET /api/harvest/dead-letters/aggregate` (admin auth, parity with the list
endpoint). The same view is one click away in the Vantage UI: open the **Dead
Letters** page and flip the **Summary** toggle to see the top groups, switch the
`group_by` dimension, and drill into the filtered list.

### Triage steps

1. Run the `harvest dlq aggregate` summary above to classify the flood.
2. For the dominant group, pivot to its `sample_dead_letter_ids`, or list a
   slice with `harvest dlq list --limit 25` for full row detail.
3. Check whether the same activity failure alert is firing.
4. Pick one affected execution and run `harvest workflow stack <execution_id>`.

### Likely causes

Retry exhaustion from downstream outage, unrecoverable payload validation
error, worker panic, timeout misconfiguration, or a replay determinism error.

### False positives

Old DLQ entries are not growth. Page on new entries or increasing gauge depth,
not on a stable backlog that is already being drained under an incident ticket.

### Safe actions

Stop the producer or pause schedules if new DLQ entries are flooding in. Fix
the handler or dependency, then use `harvest dlq bulk-replay --dry-run` before
real replay. Discard only entries that are proven unrecoverable and approved by
the workflow owner.

### Escalation criteria

Escalate immediately for replay determinism failures or customer-visible
payment, fulfillment, identity, or notification workflows entering DLQ.

### Redrive — getting work back out after a fix (issue #510)

`bulk-replay` re-enqueues a dead-letter row only if its owning execution is
still `RUNNING`. Almost every real DLQ entry was sealed `FAILED` at quarantine
time (history cap, poison-pill, workflow-task timeout), so the recovery path is
**redrive**, which reactivates the `FAILED` execution before re-enqueuing:

```bash
# 1. Deploy the fix that addresses the root cause first.
#
# 2. Preview the blast radius — counts only, no mutation. matched > max means
#    there is more to do than this call will redrive.
harvest dlq redrive \
  --error-contains "stripe: rate limited" \
  --dead-lettered-after 2026-05-30T00:00:00Z \
  --max 500 --dry-run

# 3. Redrive for real once the sample looks right. Idempotent: re-running the
#    same command is a no-op for anything already redriven (reported skipped).
harvest dlq redrive \
  --error-contains "stripe: rate limited" \
  --dead-lettered-after 2026-05-30T00:00:00Z \
  --max 500 --reason "stripe rate-limit cleared, incident-1234"
```

The response distinguishes `matched` (total filtered), `redriven`,
`skipped`, and `failed`. Read it:

- **`redriven < matched`** — the `max` cap stopped early. Re-run to drain the
  rest (the filter is stable; already-redriven rows skip).
- **`skipped`** — rows that were already redriven, or whose execution has since
  progressed past that step. Expected on a re-run; never a duplicate enqueue.
- **`failed`** — per-row rejections (see `failures[].reason`). The most common
  is an owning execution in a non-`FAILED` terminal state
  (`COMPLETED`/`CANCELLED`/`TIMED_OUT`/`TERMINATED`): redrive **refuses to
  resurrect** these and leaves the row in place rather than silently
  re-running a finished workflow.

### Verify a redrive

- The owning execution flips `FAILED → RUNNING`
  (`harvest workflow get <execution_id>`); a single `WorkflowRedriven` event is
  appended after the superseded `WorkflowFailed`.
- The `harvest.dlq.redriven{queue, outcome}` counter increments once per row
  (one of `redriven` / `skipped` / `failed`).

### Guarantees

- **Append-only.** Redrive never rewrites, removes, or reorders an existing
  event. It appends one `WorkflowRedriven` reactivation marker; the fresh
  attempt then records new events (e.g. a new `ActivityScheduled`) as the
  re-enqueued task resumes from existing history. The replay engine treats the
  `WorkflowRedriven` event and the `WorkflowFailed` it supersedes as
  transparent, so the run resumes deterministically.
- **Idempotent.** The DLQ row is deleted on redrive, so redriving the same
  entry twice (or a row whose execution already progressed) is a no-op reported
  as `skipped` — never a duplicate side-effect.
- **Shard-aware.** A filtered redrive fans out across all shards; each row is
  re-enqueued on the shard that owns its `workflow_exec_id`.

The endpoint backing this is `POST /api/harvest/dlq/redrive` (admin auth,
audit op `dlq.redrive`).

## harvest_schedule_missed_runs

The primary signal is the server-side overdue gauge `harvest_schedule_overdue`
(issue #696): `1` for any active schedule that is past its own cadence grace
(`now − next_run_at > cadence step + jitter + scheduler tick`), computed from
the schedule's own `next_run_at` + cadence — no per-schedule interval needs
hand-encoding. Alert on `max by (kind, name) (harvest_schedule_overdue) > 0`; it
names the wedged schedule directly. The gauge is sampled by the worker (not the
scheduler tick), so a wedged tick or a dead scheduler is still detected as long
as any worker is alive; a *total* process outage (all workers and the scheduler
down) is caught only by the tertiary absence-of-runs expression, which must be
paired with an `up`/scrape-health signal.

### Triage steps

1. Run `harvest schedule list --output json` and look for `overdue: true`. The
   per-schedule `overdue` and `overdue_by_secs` fields (also on
   `GET /api/harvest/admin/schedules` and `/{id}`) name exactly which schedule is
   wedged and how many seconds past its slot it is.
2. Inspect `is_paused`, `auto_paused_at`, `next_run_at`, `last_run_at`,
   `exhausted_at`, and schedule kind for the overdue schedule.
3. Run `harvest preflight --output json` to confirm scheduler coverage.
4. Check queue backlog for the schedule's dispatch queue.

### Likely causes

Scheduler loop stalled or disabled, `next_run_at` wedged in the past, an HA fire
claim (#350) that never released, all scheduler replicas down, worker outage,
queue backlog, or shard readiness issue. (A schedule at `max_active_runs`, or one
paused/auto-paused/manual/exhausted, is deliberately not firing and reads
`overdue: false` — see below.)

### False positives

Intentionally-not-firing schedules are excluded from the overdue gauge by
construction (issue #696 AC3), so they never page: `is_paused`, `auto_paused_at`
set (#360), `Schedule::Manual`, and `end_at`/`max_runs`-exhausted schedules
(#478/#543) all read `overdue: false`. A schedule deliberately deferring because
it is at `max_active_runs` — including fires deferred into the #607 throttle
queue, which the at-capacity check counts exactly as the scheduler tick does — is
also suppressed; that is a capacity condition, not a stalled cron. The grace
window already absorbs jitter and one scheduler tick, so a healthy schedule
caught mid-tick is never flagged.

Transient / self-healing cases (add `for: 2m` to the primary rule so none of
these page — see below):
- **Just-resumed long-paused schedule** (F3): resume/unpause does not recompute
  `next_run_at`, so for ~1 scheduler tick after resuming a schedule that was
  paused for a long time, its stale far-past `next_run_at` can read `overdue`.
  It self-heals on the next tick (catchup advances `next_run_at`), well within
  the ~30s sampler cadence and a `for: 2m` hold.
- **Short-cadence clock skew** (F5): `lag = now − next_run_at` is measured on a
  different process (worker/API) than the one that wrote `next_run_at` (the
  scheduler), so fleet clock skew inflates the lag. Absorbed by grace's tick term
  and `for: 2m` for normal cadences; for very short cadences (few-second
  intervals) keep clocks NTP-synced and prefer a cadence ≥ your worst-case skew.

Stuck-firing case: a schedule that was overdue (gauge = 1) and then **deleted**
leaves its `harvest_schedule_overdue{kind,name}` series stuck at 1 until the
emitting worker process restarts (an inherent gauge property — no in-process
clear path for a vanished key). Cross-check the read: `GET /admin/schedules` no
longer lists the schedule, so the firing is for a ghost. A `for: 2m` hold plus a
metric-staleness/`up` check resolves it; treat it as cleared.

A *total* process outage (nothing emits the overdue gauge at all) is covered by
the tertiary absence-of-`harvest.schedule.runs` expression plus your
scrape-health/`up` signal, not by the overdue gauge.

### Safe actions

Configure the primary overdue rule with `for: 2m` (above the ~30s sampler cadence
and one scheduler tick) so the transient/self-healing cases above never page
while a genuine wedge (persists many minutes) still fires. Then: resume an
accidentally paused schedule, restore scheduler coverage, restart a wedged
scheduler replica (a stale HA claim self-releases after ≤30s), scale the dispatch
queue, or trigger a manual catchup only after idempotency is confirmed. Avoid
blind backfills while downstream systems are unhealthy.

### Escalation criteria

Escalate when a regulatory, billing, or customer-notification schedule reports
`overdue: true` for one required firing, or when catchup would exceed downstream
capacity.

## harvest_retention_lag

### Triage steps

1. Run `harvest retention status`.
2. Confirm retention is enabled and note `max_age`, `batch_size`, and dry-run state.
3. Run `harvest retention run-now` only if the status shows eligible rows and no shard blockers.
4. Check database storage pressure before changing retention settings.

### Likely causes

Retention disabled, dry-run mode left on, shard unavailable, batch size too
small, long-running terminal scan, or no eligible terminal executions.

### False positives

Zero deleted rows is normal when no terminal histories are older than the
configured max age. Dry-run deployments should ticket, not page, unless storage
pressure is active.

### Safe actions

Enable retention intentionally, lower max age only with product approval, raise
batch size gradually, or run a one-off retention tick during low traffic. Do not
delete active workflow histories.

### Escalation criteria

Escalate to the database owner if storage pressure is rising or retention
queries are impacting production latency.

## harvest_shard_unready

### Triage steps

1. Run `harvest shard health --fail-on-unready --output json`.
2. Identify affected shard ids, readiness verdict, and reason codes.
3. Check whether the shard is writable, readable-only, or a promotion candidate.
4. Run `harvest preflight --output json` to see whether the same blocker appears globally.

### Likely causes

Database unreachable, schema mismatch, missing worker coverage, stale scheduler
coverage, stale health sample, queue pressure, or DLQ pressure on the shard.

### False positives

Readable-only candidate shards can be unready while still safe for existing
traffic. Writable shard unready is never a cosmetic warning.

### Safe actions

Remove an unready candidate from promotion, keep `writable_shards` unchanged,
repair migrations, restore worker coverage, or roll back shard config. Do not
move existing executions across shards; Harvest does not rebalance them.

### Escalation criteria

Escalate if a writable shard is unavailable, if multiple shards are partial, or
if readiness cannot be proven during a rollout.

## harvest_shard_undrained

A writable shard has pending work that no live worker can claim. The cause can
be missing shard coverage or an eligibility mismatch.

This is the **shard-dimension** analogue of
[`harvest_queue_uncovered`](#harvest_queue_uncovered), which detects the same
condition per *queue*. Both can be true at once (a queue with no poller on a
shard with stranded work). Use shard health to decide whether to change shard
or queue coverage or fix worker eligibility.

### Triage steps

1. Run `harvest shard health --output json` and note which shard ids are
   affected and whether they are writable.
2. Inspect `no_live_worker` and its `blocking_reasons`. The reason code means no
   live worker can claim at least one demand. It does not prove poller absence.
3. Branch on `blocking_reasons`:
   - `polls queue(s)` means no shard-assigned worker polls the pending queue.
     Start or widen a worker's shard and queue coverage.
   - `capability/build/sticky requirements` means a covering poller is present
     but ineligible. Fix build compatibility, capabilities, or sticky ownership.
4. Use this optional expression to find stranded shards with no recent dispatch
   activity:

   ```promql
   # stranded work with no recent dispatch activity
   max by (shard) (harvest_shard_stranded_pending) > 0
     unless (sum by (shard) (rate(harvest_shard_dispatched_total[5m])) > 0)
   ```

   This expression does not prove poller absence. A covering worker can have no
   dispatches because it cannot claim these rows. `unless`, not `and ... == 0`,
   preserves a shard that has never had a dispatch series.
5. Read the worker's **effective** shard coverage:

   ```bash
   curl -s .../api/harvest/admin/config | jq '.worker.shard_assignments'
   ```

   Since issue #961 an **empty** `shard_assignments` means *auto*: the worker
   covers every shard in its pool. `GET /admin/config` reports the *resolved*
   list, so the shard ids printed here are exactly the ones the worker polls.
6. Confirm at least one live worker reports the shard:
   `harvest worker health --output json`.

### Likely causes

A shard was added to `writable_shards` without a covering worker; its worker
died or was drained; its worker was explicitly narrowed; or its worker cannot
claim the rows due to queue, build-id, capability, or sticky-lease constraints.
The shard can also lack a `ShardedDbPool` entry. In that case, startup is refused;
check the logs for `ShardRouter references shards ... that have no pool entry`.
(The sibling `shard_assignments are missing from the sharded_pool` rejection can
only fire for an **explicitly narrowed** worker: auto-derived assignments come
*from* the pool, so they can never name a shard the pool lacks.)

### False positives

A brief spike during a **rolling worker restart** is expected: the sampler can
observe a shard between the old worker's deregistration and the new worker's
registration. Size the alert's `for` window above your restart window; 2m is a
conservative starting point. The starter pack ships PromQL expressions only, so
the `for` window is yours to set. A shard being deliberately
drained (readable-but-not-writable) may also hold pending work legitimately
while it finishes — cross-check `harvest shard health` for its writable flag.

### Safe actions

For a `polls queue(s)` block, remove explicit `with_shard_assignments`
narrowing, add the shard, or add the queue. For a
`capability/build/sticky requirements` block, fix the named eligibility
constraint. Configuration changes take effect on worker restart. Do **not**
move executions across shards. Execution ids encode the original shard.

### Escalation criteria

Escalate if a writable shard stays undrained beyond one restart window, if
multiple shards are simultaneously undrained (suggesting a pool/router
misconfiguration rather than a single dead worker), or if `GET /admin/config`
shows a shard the router considers writable but no worker resolves.

## harvest_no_compatible_worker

### Triage steps

1. Confirm the alert status is still `pending-management-signal`; use it only if your deployment exports this signal.
2. Run `harvest workflow stack <execution_id>` for an affected execution.
3. Run `harvest worker health --output json` and inspect worker build/deployment identity.
4. Review `docs/runbooks/safe-deploy.md` for build compatibility and safe-to-retire checks.

### Likely causes

New build policy moved starts to a build with no active workers, old workers
were drained before in-flight executions finished, compat was not declared, or
legacy workers with empty build IDs are masking the real routing state.

### False positives

This rule is pending until Harvest exports a native bounded
`no_compatible_worker` management signal. Do not page on inferred symptoms
unless queue backlog or stack evidence confirms stuck work.

### Safe actions

Start workers for the required build, declare compatibility only after replay
fixtures prove it safe, shift new starts back to the previous build, or stop
draining old-build workers. Never route incompatible workers just to clear a
queue; replay safety is the reason this alert exists.

### Escalation criteria

Escalate to the release owner when production work is stuck behind build
compatibility or when rollback requires reverse compatibility declarations.

## harvest_schedule_ha_domination

### Triage steps

1. Check the `lost_race / (lost_race + claimed)` ratio in Grafana for the last 5–10 minutes.
2. Query Postgres for stuck claim tokens:
   ```sql
   SELECT id, workflow_name, fire_claim_token, fire_claimed_until, next_run_at
   FROM harvest_schedules
   WHERE fire_claimed_until IS NOT NULL
   ORDER BY fire_claimed_until DESC
   LIMIT 10;
   ```
3. Verify all replicas share the same `DATABASE_URL` and shard routing configuration.
4. Run `harvest worker health --output json` to confirm fleet coverage.

### Likely causes

- One or more replicas point to a **different Postgres instance** than the majority of the fleet (the "different DB" replica claims a disjoint set; others always see it as locked).
- Incorrect `shard_assignments` exclude most replicas from the affected shard.
- A stuck or crashed replica's claim token has not expired (token older than 30 s + tick interval is a bug; see escalation).

### False positives

In a **single-replica deployment** or **initial startup** before the first tick, `lost_race = 0` and `claimed = 1 per tick`. This alert should never fire for single-replica deployments (ratio is always 0).

In a **two-replica deployment**, healthy steady-state is approximately `lost_race ≈ claimed` (each replica wins about half the slots at the tick boundary). The 0.98 threshold ensures this alert does not fire for expected contention.

### Safe actions

Fix the database configuration so all replicas share the same shard pools. Do not manually clear `fire_claim_token` rows unless you have confirmed the claiming replica has stopped; the 30-second TTL handles crash recovery automatically.

### Escalation criteria

Escalate to the platform owner if:
- A `fire_claim_token` row has `fire_claimed_until` more than 2 minutes in the past and `next_run_at` has not advanced (indicates the claiming process is alive but wedged without completing the fire or clearing the claim — this should not happen with the current implementation and would indicate a bug).
- The alert fires on a single-replica deployment (indicates a misconfiguration or metric collection error).

## harvest_workflow_failure_rate

The fraction of terminal workflow executions ending `failed` has exceeded the
threshold (starter default: 10% over 5 minutes) for a workflow type. This is
the primary success-rate SLO signal, computed from the
`harvest.workflow.terminal{outcome}` counter (issue #519), which fires exactly
once per terminal outcome. The `outcome` label separates `failed` from
operator-driven `cancelled`/`terminated` and from `timed_out`, so the ratio
pages only on genuine failures.

### Triage steps

1. Identify the `workflow` label from the alert.
2. List recent failures: `harvest workflow list --state FAILED --workflow <name>`
   (or `GET /api/harvest/workflows?state=FAILED&workflow_name=<name>`) and read
   the `error` field on a sample of rows.
3. Check the DLQ for poison-pill or retry-exhaustion entries from the same
   workflow: `harvest dlq list --limit 25`, or aggregate root causes with
   `harvest dlq aggregate --group-by workflow_name,failure_signature`.
4. Compare the failure onset against recent deploys and against the
   `harvest.activity.failed{error_type}` breakdown — a workflow failure surge
   is usually downstream of an activity failure surge.
5. If failures are non-determinism related, follow
   `#harvest_workflow_non_determinism` instead (check
   `GET /api/harvest/workflows?nd_blocked=true`).

### Likely causes

A deployment regression in the workflow or one of its activities, a downstream
dependency outage exhausting activity retries, poison-pill quarantine failing
the owning workflows, a too-tight `execution_timeout` misclassifying slow runs
(those surface as `timed_out`, not `failed`, unless a handler maps them), or a
payload/schema drift that makes the workflow fail deterministically on new
input.

### False positives

Low-traffic workflow types produce unstable ratios over short windows — a
single failure in a 5-minute window with two terminal runs is 50%. Alert only
on workflow types with steady baseline traffic, or lengthen the window.
Deliberate operator terminations and cancellations are separate `outcome`
values and never count toward this ratio's numerator; `continued_as_new` is
excluded from the denominator by the **dashboard** pack's ratio panel — the
alert pack's default expression carries no such exclusion, so add it to your
alert expression if continue-as-new volume is significant.

### Safe actions

Roll back the offending workflow/activity release, force-open a circuit
breaker if a known-bad downstream is burning retries
(`POST /api/harvest/admin/circuits/{activity_name}/force-open`), pause the
schedules feeding the failing workflow type, or pause individual runaway
executions. Redrive dead letters only after the root cause is fixed.

### Escalation criteria

Escalate to the workflow owner when the failure ratio stays above the
threshold for more than two windows, and to the release owner when the onset
correlates with a deploy. Page immediately if the failing workflow type backs
a customer-facing SLO or if DLQ entries for it are accumulating rapidly.

## harvest_workflow_non_determinism

Since issue #603 an engine-detected replay divergence **blocks the affected
execution non-terminally** instead of failing it: the run stays `RUNNING`, no
`WorkflowFailed` event is written, and the workflow task is re-dispatched with
a capped-exponential backoff (5s doubling to a 300s ceiling, indefinitely).
Rolling back or fixing the offending build resumes the whole cohort with **no
per-execution operator action**. Full playbook:
`docs/runbooks/nondeterminism-block.md`.

### Triage steps

1. Locate blocked executions by querying the management API:
   ```bash
   GET /api/harvest/workflows?nd_blocked=true
   ```
   (Executions that terminally failed under the pre-#603 behavior remain
   discoverable via `GET /api/harvest/workflows?state=FAILED&failure_cause=non_determinism`.)
2. Read each row's divergence diagnostic: `nd_block_reason`, `nd_block_count`,
   `nd_blocked_at` on the execution, plus the structured fields in
   `search_attrs`:
   - `expected` (the event/command the current code generated)
   - `actual` (the event recorded in the history)
   - `event_index` (index where the divergence occurred)
   - `build_id` (the build ID of the worker that observed the divergence)
3. Diagnose one specific execution on demand against the **currently-deployed**
   code with `POST /api/harvest/workflows/{id}/replay-diagnosis` (issue #614) —
   it returns the same `{kind, event_index, expected, actual}` vocabulary and,
   after a candidate rollback/fix is deployed, a `clean` verdict confirms the
   run will resume. See the **"Diagnose the divergence"** section of
   [`docs/runbooks/nondeterminism-block.md`](nondeterminism-block.md#diagnose-the-divergence-issue-614)
   for the curl and the confirm-a-fix workflow.
4. Check recent deployment history to see if a new release was shipped without proper version gating or routing protection.
5. Run replay tests on the workflow using the exported history to reproduce the non-determinism error.

### Likely causes

- Code deployment that modifies workflow logic (adding, removing, or reordering activities, signals, timers, or child workflows) without updating the version gate.
- Side effects that are not wrapped in `WorkflowContext::side_effect()`, such as direct system calls, time queries (`Instant::now()`), or random number generation.
- Iteration order on non-deterministic collections (like `HashMap` or `HashSet`) in the workflow function.

### False positives

None. A non-determinism mismatch means the workflow code generated a different sequence of commands/actions than what was recorded in history, making replay safety impossible. Author `Err(...)` returns are never classified as divergence — they still fail terminally.

### Safe actions

1. Roll back the offending deployment immediately to the last known-good version. Blocked executions resume automatically on their next backoff re-dispatch (within ≤300s) — confirm `nd_blocked_at` clears and the cohort progresses.
2. If the deployment must stay, declare build compatibility appropriately or use version gates.
3. Escalation only — for a history that stays blocked even under the rolled-back build (e.g. the divergent build appended inline events before diverging), reset to the pre-divergence event index:
   ```bash
   POST /api/harvest/workflows/{execution_id}/reset
   ```
   Specifying the event index prior to the divergence.

### Escalation criteria

Escalate immediately to the release owner and the team who shipped the latest version. Replay divergence blocks execution progress for all active workflows of that type; while no work is destroyed (the block is recoverable), the cohort makes no forward progress until the build is rolled back or fixed.

## harvest_activity_success_ratio

### Triage steps

1. Identify the `activity` and `queue` labels from the alert.
2. Run `harvest dlq list --limit 25` to check for dead-letter accumulation from that activity.
3. Run `harvest workflow stack <execution_id>` on affected executions to see pending activity state.
4. Check `harvest.activity.failed{activity=<name>}` for `error.type` breakdown to classify the failure mode.
5. Inspect the circuit breaker state: `GET /api/harvest/admin/circuits/<activity_name>`.

### Likely causes

Downstream outage or degradation, bad credentials, schema or payload drift, deployment regression
in the activity handler, timeout configured too low, or retry exhaustion tipping into the DLQ.

### False positives

Newly deployed activities with sparse traffic produce unstable ratios over short windows. Alert
only when the activity has steady baseline traffic and the ratio drops for more than one 5-minute
window. A ratio of exactly 0 with no `outcome=completed` series usually indicates the activity
has never succeeded in the window — check if it is newly deployed or was just restarted.

### Safe actions

Roll back the handler release, restore downstream credentials, force-open the circuit breaker
(`POST /api/harvest/admin/circuits/{activity_name}/force-open`) if the downstream is known-bad,
or pause the schedules that are triggering the activity until the downstream recovers.
Replay dead letters only after the root cause is fixed.

### Escalation criteria

Escalate to the downstream team for sustained external errors. Escalate to the workflow owner
for payload or schema regressions. Page immediately if the success ratio stays below 50% for
more than 10 minutes or if DLQ entries are accumulating rapidly.

## harvest_activity_retry_storm

### Triage steps

1. Identify the `activity` and `queue` labels from the alert.
2. Check the retry rate trend: is it growing, stable, or receding?
3. Run `harvest dlq list --limit 25` to determine whether retries are eventually succeeding or tipping into the DLQ.
4. Check `harvest.activity.failed{activity=<name>,error.type=<type>}` to identify the failure class driving retries.
5. Inspect circuit breaker state: `GET /api/harvest/admin/circuits/<activity_name>`.
   If the breaker is closed and failures are consistent, consider force-opening it to stop flooding: `POST /api/harvest/admin/circuits/{activity_name}/force-open`.

### Likely causes

Intermittent downstream degradation that recovers before retry exhaustion, too-aggressive retry
policy (high max_attempts or very short backoff), cascading failure across many parallel workflow
executions, or a transient infrastructure event (restart, rebalance, network blip).

### False positives

A burst of retries after a brief dependency restart is expected and self-resolving. Alert only
when the retry rate is sustained (> 5 min) or growing. If the circuit breaker opens automatically,
the storm signal will drop even if the underlying dependency is still recovering.

### Safe actions

1. If the downstream is known-bad, force-open the circuit breaker to stop the retry flood immediately.
2. Reduce concurrency on the affected queue temporarily via `WorkerConfig::max_concurrent_activities`.
3. Pause schedules that are generating the affected work until the downstream recovers.
4. Tune the retry policy (`max_attempts`, `initial_interval`) in a follow-up release if the storm is structural.

### Escalation criteria

Escalate to the downstream owner for external dependency outages. Escalate to the workflow owner
if the retry policy is misconfigured. Page (`harvest_activity_retry_storm_critical`) when the retry
rate exceeds 20/s — at that scale the task queue backlog will grow and other queues will be starved.

## harvest_saga_compensation_spike

**What a compensation spike means:** sagas are rolling back en masse. Each
`harvest.saga.compensated` increment is one real `compensate_all` /
step-failure unwind actually running forward (exactly once per sequence —
replays never re-count), so an elevated rate means many workflows hit a
failing forward step and are undoing already-committed work. This is the
canonical *leading* indicator of a downstream-dependency outage: it fires as
soon as unwinds start, before retry exhaustion, DLQ growth, or terminal
workflow failures become visible.

### Triage steps

1. Read the `workflow` label on the spiking series to identify which saga
   workflow type is rolling back, and `queue` to locate the worker pool.
2. Find the failing forward step: check
   `harvest.activity.failed{workflow.type=<name>}` for the `error.type`
   breakdown, and `harvest.activity.retries` for a concurrent retry storm on
   the same activity.
3. Inspect recent failures directly: `harvest workflow list --state FAILED
   --workflow-name <name> | head -20` (or `GET /api/harvest/workflows?state=FAILED`)
   and read the `error` field for the original step error.
4. If the saga spawns child workflows, the failing step may be several levels
   down: `harvest workflow tree <exec_id> --summary` gives a per-state
   descendant roll-up in one call, and `harvest workflow tree <exec_id>`
   renders the whole family so you can find *which* descendant failed. See
   [trace-execution-lineage.md](trace-execution-lineage.md).
5. Check whether `harvest_saga_compensation_failed` is ALSO firing — a spike
   plus failed compensations means dangling state is accumulating; treat as
   the page, not the ticket.
6. Check the downstream dependency the failing step calls (status page,
   circuit breaker state via `GET /api/harvest/admin/circuits`).

### Likely causes

- A downstream dependency (payment gateway, inventory service, partner API)
  started erroring, so forward steps fail and every in-flight saga unwinds.
- A bad deploy of an activity that a mid-saga step depends on.
- A legitimate business burst of rollbacks (mass cancellation event,
  end-of-sale inventory exhaustion) — elevated but expected.

### False positives

A workflow type that compensates as part of its normal business flow (e.g. a
quote-then-release pattern) has a non-zero baseline compensation rate — the
starter expression is baseline-relative for exactly this reason, but tune the
multiplier per workflow. A short burst after a dependency restart that
self-resolves within one evaluation window is expected.

### Safe actions

1. If a downstream outage is confirmed, force-open the failing activity's
   circuit breaker (`POST /api/harvest/admin/circuits/{activity}/force-open`)
   so new sagas fail fast at the first step instead of committing work they
   will immediately unwind.
2. Pause the schedules or gate the admissions that feed the affected workflow
   type until the downstream recovers.
3. Let in-flight unwinds run — compensations are idempotent by contract and
   the counter confirms they are progressing.

### Escalation criteria

Escalate to the downstream owner when the failing step's dependency is
external. Page (rather than ticket) when the spike is sustained > 15 minutes,
when `harvest_saga_compensation_failed` fires alongside it, or when the
affected workflow type moves money or inventory.

## harvest_saga_compensation_failed

**What to do when a compensation fails:** a rollback itself failed
(`HarvestError::SagaCompensationFailed`), so the system is holding
partially-committed, dangling state — a charge that was not refunded, a
reservation that was not released. Nothing in the engine re-attempts a
failed compensation on its own timeline: for the documented durable pattern
(a compensation that invokes an activity), the activity's recorded
`ActivityFailed` terminal is **replayed, never re-executed** — deterministic
replay returns the recorded failure — and a pure in-memory compensation
closure only re-runs if the still-RUNNING execution replays again, which
does not survive an author-caught, completed run. Every increment therefore
represents work for a human until reconciled. The counter fires exactly once per failed unwind,
including unwinds whose error the workflow author caught — it is deliberately
separable from `harvest.workflow.terminal{outcome=failed}` so you can page on
it alone.

### Triage steps

1. Identify the workflow type and queue from the metric labels.
2. List candidate executions: `harvest workflow list --state FAILED
   --workflow-name <name>` — the execution `error` field carries the
   `SagaCompensationFailed` message with the original step error AND the
   per-compensation error strings. If nothing is FAILED, the author caught
   the error and the run COMPLETED normally: also list recent completions
   (`harvest workflow list --state COMPLETED --workflow-name <name>
   --limit 20`) and search each run's history
   (`GET /api/harvest/workflows/{execution_id}/history`) for the
   `saga_compensation_failed:` durable marker (`MarkerRecorded`) or the
   `SagaCompensationFailed` error string, plus the failing compensation
   activity's `ActivityFailed` event — either path always yields an
   execution id to inspect.
3. Read the `compensation_errors` list in the error message to see exactly
   which compensations failed and why.
4. Determine the dangling resource for each failed compensation (the forward
   step's recorded output in history names it: charge id, reservation id).

### Likely causes

- The same downstream outage that triggered the unwind also broke the
  compensation path (refund API down while charge API degraded).
- A compensation activity with a bug or a missing idempotency guard.
- A compensation depending on state the forward step no longer guarantees
  (release-most-recent anti-pattern — see docs/saga.md).

### False positives

None by design — each increment is a real failed unwind. (False *negatives*
are limited to the documented conservative edge: an unwind entered at an
unresolvable history position — e.g. a signal-with-start run whose unwind
starts before the staged signal is awaited — is uncounted as a whole; see
the accepted edges in `docs/saga.md`.) If an increment is expected (e.g. a
deliberately non-compensatable step), the workflow should not register a
compensation for that step at all.

### Safe actions

1. Manually reconcile each dangling resource (issue the refund, release the
   reservation) using the resource ids recorded in the execution history.
2. Re-driving the workflow task helps **only for pure in-memory
   compensation closures** in a still-RUNNING execution: those re-run
   wholesale on every replay (idempotent by contract), so a transient
   failure can clear. It is safe but **ineffective** for the documented
   durable pattern (activity-backed compensations): the compensation
   activity's retries are already exhausted and its recorded `ActivityFailed`
   is replayed, not re-executed — `retry-now` (#516) only helps a
   *backing-off PENDING* task, not a terminally-failed one. For durable
   compensations, reconcile manually (step 1) or reset the execution from
   history before the failed compensation (#148, `docs/runbooks/` reset
   tooling) after fixing the downstream.
3. Fix the compensation activity (or its downstream) before re-enabling the
   traffic source; a broken compensation path turns every future rollback
   into manual work.

### Escalation criteria

Page immediately — this is dangling state by definition. Escalate to the
workflow owner for the reconciliation runbook of the specific resource, and
to the downstream owner when the compensation path's dependency is the thing
that failed. Escalate to the on-call engineering lead if increments continue
accruing after the traffic source is paused (indicates unwinds still failing
mid-flight).

## harvest_update_rejected_rate

**What to do when workflow updates are being rejected by their validators:**
the `harvest.update.rejected` counter (issue #684) fires once per workflow
update rejected by its **registered validator** before admission (a durable
pre-admission `422`). The metric's `workflow` + `name` labels name the
workflow type and the update. Scope is deliberately validator-only:
non-`RUNNING`/paused admission-state conflicts are surfaced to the caller as
errors, not counted here. A rejection is cheap and safe (the update never
becomes durable history), so this alert is about *callers failing to drive
the workflow*, not about engine health.

### Triage steps

1. Read the `workflow` and `name` labels to identify the workflow type and
   the update handler being rejected.
2. Inspect the registered validator and the caller's payload shape:
   `GET /api/harvest/workflows/types/{workflow_name}/handlers` lists the
   workflow's declared update handlers.
3. Correlate with recent deploys: a client that started sending a new/changed
   payload, or a workflow whose validator got stricter, is the usual cause.
4. Capture a sample rejected payload from the caller's logs and run it
   against the validator locally to confirm the rejection reason.

### Likely causes

- A client/version drift after a deploy: the caller's update payload no longer
  matches the workflow's validator (renamed/removed field, tightened bound).
- A buggy or misconfigured caller sending malformed update requests.
- A retry storm of an already-invalid payload (the caller retries a request
  the validator will always reject).

### False positives

Expected during a controlled client/workflow migration window where old and
new payload shapes briefly coexist. A small steady trickle can also be a
mis-behaving but non-critical caller. Alert on a sustained rise above the
workflow's own baseline, not an absolute floor — a healthy client should see
near-zero validator rejections.

### Safe actions

1. Pause or roll back the offending caller (or its recent deploy) if it is
   sending an invalid payload shape.
2. Fix the caller's payload to match the validator, or (if the validator is
   wrong) fix and redeploy the workflow's update handler.
3. No engine-side remediation is needed: rejected updates append nothing to
   history and never wedge a workflow.

### Escalation criteria

Ticket the workflow owner with a sample rejected payload and the validator's
reason. Escalate to the caller's owning team when the source is a client
deploy. Page only if the rejected update path is on a critical control flow
(e.g. an operator-driven pause/adjust update) and legitimate operators are
being blocked.

## harvest_signal_unhandled_rate

**What to do when workflows are leaving signals unconsumed:** the
`harvest.signal.unhandled` counter (issue #684) fires once per delivered
signal a workflow left unconsumed (no `wait_for_signal`/`receive_signal`, no
push handler) by the time it reached a **Completed or Failed** terminal
outcome. The `workflow` + `queue` labels name the workflow type and its task
queue. The signal **name is not a label** (issue #684, Codex P2 — signal names
come from the free-form send route and have no declared registry to bound them,
so they cannot be a bounded metric label); identify *which* signal was dropped
from the run's history / stack (below), not from the metric. Signals excused by
a lost signal-or-deadline race (issue #476) are never counted.

> **Known scope limitation — read this before trusting a *low* number.** This
> counter is emitted **only** from graceful `Completed`/`Failed` terminals
> reached through the workflow drive. Forced-failure / scanner terminal paths —
> **`TIMED_OUT`, `CANCELLED`, `TERMINATED`, parent-close cascade, and
> history-cap failure** — are driven to termination without a workflow drive, so
> there is no matcher to reconstruct which signals were consumed; those runs'
> undrained signals are **NOT counted here**. In particular this **excludes the
> motivating "stuck workflow that ignored a signal and then timed out" case**:
> to catch a run that is wedged past its deadline, watch the
> `harvest.workflow.timeout` counter and inspect the run with
> `GET /api/harvest/workflows/{execution_id}/stack` (which shows what it is
> blocked on and any pending, never-consumed signals). A low
> `harvest.signal.unhandled` rate does **not** imply "no signals are being
> dropped" — only "no signals are being dropped on runs that finished
> gracefully."

### Triage steps

1. Read the `workflow` (and `queue`) label to identify the workflow type. The
   signal name is not a label — identify the specific dropped signal from a
   run's history / stack in step 3.
2. Confirm whether that workflow is *expected* to consume that signal on every
   run, or whether the signal is a best-effort/late notification the workflow
   deliberately ignores.
3. List recent runs of the workflow type and inspect a completed run's history
   for the delivered-but-unconsumed `SignalReceived`:
   `GET /api/harvest/workflows?workflow_name=<name>&state=COMPLETED`, then
   `GET /api/harvest/workflows/{execution_id}/history`.
4. Correlate with recent workflow deploys: a removed or renamed
   `wait_for_signal`/`receive_signal`/`register_signal_handler` call is a
   common cause.

### Likely causes

- A race between a signal-sending caller and the workflow's own control flow
  (the signal arrives after the workflow already decided to finish).
- A workflow deploy that removed or renamed the handler for a signal callers
  are still sending.
- A caller sending a signal the workflow never handled (wrong signal name).

### False positives

A small steady rate is often legitimate: late or duplicate signals a workflow
intentionally ignores (e.g. a second approval after the run already decided).
Alert on a change from the workflow's own baseline, not an absolute floor.

### Safe actions

1. If the workflow should handle the signal, restore or rename the
   `wait_for_signal`/`receive_signal`/`register_signal_handler` call and
   redeploy (in-flight runs replay deterministically; new runs pick it up).
2. If callers are sending the wrong signal, fix the caller.
3. No engine-side remediation is needed: an unhandled signal does not fail the
   run — the metric is purely an observability signal.

### Escalation criteria

Ticket the workflow owner with the signal name and a sample run id. Escalate
to the caller's owning team when the source is a caller sending an unexpected
signal. Page only if the dropped signal is on a critical control path (e.g. a
cancel/approval the run must observe) and business-critical runs are visibly
diverging.

## harvest_workflow_population_leak

**What to do when a workflow type's active population keeps growing:** the
`harvest.workflow.active` gauge (issue #770) reports the live count of
currently-active executions per workflow type, split by lifecycle state
(`running` / `paused`). This alert fires when a type's active population rises
steadily over an hour while its terminal completion rate (`harvest.workflow.terminal`)
stays effectively flat — i.e. starts are outpacing completions and executions
are accumulating rather than draining. That is the signature of a leak or a
backlog, not a healthy burst that later drains. `PAUSED` runs count under
`state="paused"`; nd-blocked runs (issue #603) stay `RUNNING` and count under
`state="running"`. The gauge is summed across every shard.

### Triage steps

1. Read the `workflow` and `state` labels to identify which type is growing and
   whether the growth is in `running` or `paused` executions.
2. List the active runs to see how long they have been in flight:
   `harvest workflow list --state RUNNING --limit 25` (or
   `GET /api/harvest/workflows?state=RUNNING`). A large paused population instead
   points at forgotten operator pauses (issue #383) — list `state=PAUSED`.
3. Compare the start rate against the terminal rate for the type
   (`harvest.workflow.started` vs `harvest.workflow.terminal`): a sustained gap
   confirms the imbalance.
4. Inspect a long-running instance with
   `GET /api/harvest/workflows/{execution_id}/stack` to see what it is blocked on
   (a slow/failing activity, an unfired timer, an unreceived signal, a stuck
   child workflow).
5. Check worker slot saturation (issue #531, `harvest.worker.slots_in_use` vs
   `slots_available`) and queue depth — a starved fleet lets active runs pile up
   even when each is healthy.

### Likely causes

- Workers are saturated or under-provisioned, so runs start faster than they can
  be driven to completion (a backlog, not a bug).
- A downstream dependency an activity calls is slow or failing, so runs park mid-
  flight and never finish.
- A workflow bug that leaves runs waiting forever (a signal that never arrives, a
  timer that is never set, an unbounded loop that never continues-as-new).
- A large batch of operator pauses that were never resumed (growth concentrated
  in `state="paused"`).

### False positives

A legitimate traffic surge (a scheduled batch, a marketing event) briefly raises
the active population and then drains as the runs complete — the terminal rate
rises alongside it, so the `and … terminal rate < 0.01` clause keeps the alert
quiet. Alert on population growth *paired with a flat terminal rate*, and tune
the `> 50` growth threshold to the workflow type's expected concurrency; a
high-throughput type may legitimately sit at thousands of active runs.

### Safe actions

1. If workers are saturated, scale the worker fleet (add replicas / raise
   `max_concurrent_workflows`) so the backlog drains.
2. If a downstream dependency is the bottleneck, remediate it; parked runs
   resume automatically once activities succeed.
3. If runs are genuinely stuck on a workflow bug, inspect one via `stack`, fix
   and redeploy the handler (in-flight runs replay deterministically), or
   cancel/terminate individual wedged runs.
4. If the growth is forgotten pauses, resume them
   (`POST /api/harvest/workflows/{execution_id}/resume`).

### Escalation criteria

Ticket the workflow owner with the type name and the current active count.
Escalate to page if the population growth is unbounded and accelerating, the
worker fleet is already at capacity, and business-critical runs are visibly
stalled (their `stack` shows no forward progress across successive checks).

## harvest_workflow_history_bloat

**What to do when a still-running workflow's history is approaching the hard
cap:** the `harvest.workflow.history_bloat` counter (issue #704) is an operator
**early-warning**, distinct from the terminal outcome it precedes. Harvest can
optionally enforce a hard cap on the number of recorded `harvest_events` an
in-flight execution may accumulate (`WorkflowHistoryPolicy::event_hard_cap`,
set once via `HarvestBuilder::history_event_hard_cap` at worker-registry
construction time — **registry-wide, not per-workflow-type**: `HandlerRegistry`
stores a single `WorkflowHistoryPolicy`, consulted with no workflow-name
parameter, so every workflow type registered on that worker shares the
identical cap and warn fraction; there is no per-type override, and raising or
lowering either value affects every workflow type that worker serves. Distinct
from the unrelated fleet-wide `HarvestBuilder::max_workflow_history_events`
ceiling from issue #493, which is sampled by a separate periodic scanner and
reported via the `harvest.workflow.history_oversized` gauge). When a hard cap
is configured,
the same still-`RUNNING` execution that would eventually hit it is instead
warned once — the first time its recorded history crosses a configurable
fraction of that cap (`history_bloat_warn_fraction`, default **75%**,
`0` disables the signal entirely). The counter increments once per crossing
per execution (delivery is at-least-once — see the last triage step below);
the run itself is completely unaffected and keeps executing normally. A
single decision cycle can also grow history from below the soft threshold
straight past the hard cap in one shot (e.g. a batched local-activity or
external-signal append); in that case the same crossing is caught and
emitted right before the execution is terminally force-failed, so the
warning still fires even for a run that never spends a cycle "just barely
over the soft line." If nothing intervenes, continued growth will eventually
reach the hard cap, at which point the execution is terminally force-failed
and moved to the dead-letter queue — this alert exists to give an operator a
window to act *before* that happens.

### Triage steps

1. Read the `workflow` label to identify which workflow type crossed the soft
   threshold.
2. Discover and rank the specific offending execution(s) with the dedicated
   discovery filter, sorted largest-first with each row's current
   `history_event_count`:
   `harvest workflow list --history-bloat-min-events <threshold>` (or
   `GET /api/harvest/workflows?history_bloat_min_events=<threshold>`). Start
   with a threshold near the configured hard cap's 75% soft mark and lower it
   if you need to see the full ranked population; every returned row is
   guaranteed non-terminal (`RUNNING`/`PAUSED`), sorted by history size
   descending. This is a DIFFERENT query parameter from the unrelated,
   pre-existing general-purpose `min_history_events` filter (issue #493,
   [`docs/runbooks/history-ceiling.md`](history-ceiling.md)), which composes with `state=`/
   pagination and does not restrict to live executions or sort by size.
3. For the largest offender(s), inspect `GET /api/harvest/workflows/{execution_id}/stack`
   and `GET /api/harvest/workflows/{execution_id}/history` to understand what
   is driving the growth — a tight retry/poll loop, an unbounded fan-out, or a
   long-lived entity workflow that has simply been running for a long time
   without ever checkpointing.
4. Check whether the execution has already been warned before
   (`history_bloat_warned_at` on the execution row, surfaced by the describe
   endpoint) — the guard field is set once and only once per execution
   (idempotent across replays/retries), so a repeat page naming the *same*
   execution means it kept growing past the first warning and is now closer
   to the hard cap. The counter itself is delivered at-least-once: it is
   emitted just before the guard is durably persisted, so a worker crash in
   that narrow window can rarely cause one extra increment on a retry —
   treat a lone duplicate-looking page for an execution whose
   `history_bloat_warned_at` is already set as this benign case, not a
   second distinct crossing.

### Likely causes

- A workflow that polls or loops without ever calling `continue_as_new` (issue
  #772's `should_continue_as_new()` exists precisely to recommend this
  checkpoint; a workflow that ignores the recommendation, or has no such
  check at all, keeps accumulating history indefinitely).
- An unexpectedly wide activity or child-workflow fan-out (issues #359/#601)
  recording far more events per decision cycle than the workflow author
  anticipated.
- A long-lived entity workflow (a subscription, a cart, a device) that is
  behaving correctly but was never given a deadline-aware checkpoint
  strategy (issue #772).
- The configured `event_hard_cap` (or its `history_bloat_warn_fraction`) is
  simply too tight for the crossing workflow type's normal, healthy history
  footprint — since the cap/fraction is registry-wide (not per-type), raising
  either affects every other workflow type sharing the same worker process;
  confirm the new value is still appropriate for them before tuning, or run
  the outlier workflow type on a separate worker/`HandlerRegistry` with its
  own cap if their footprints genuinely diverge.

### False positives

A single crossing for a workflow type known to run long and record many
events by design (e.g. a long-lived entity workflow deliberately operating
close to its configured cap) is expected, not an incident — the alert fires
once per execution and does not repeat unless the execution keeps growing
past the point already investigated. A worker whose `HandlerRegistry` has no
`event_hard_cap` configured will show a permanently flat, never-incrementing
series; that is the disabled/no-op state, not a health signal to chase.

### Safe actions

1. If the workflow is a candidate for `continue_as_new`, trigger it (either by
   waiting for the workflow's own `should_continue_as_new()` check to fire on
   its next decision cycle, or — for a workflow with no such check — by
   deploying a code change that adds one; in-flight executions replay
   deterministically against the recorded history).
2. If the growth is an unbounded fan-out, inspect and fix the workflow code
   and let the fix apply on the next deploy (existing in-flight executions
   are unaffected by the code change until they reach a fresh decision
   cycle).
3. If the cap/fraction is simply mistuned for the crossing workflow type's
   expected footprint, raise `event_hard_cap` or `history_bloat_warn_fraction`
   via `HarvestBuilder` on the next deploy — this is registry-wide (every
   workflow type sharing that worker gets the new value; there is no
   per-type override) and never requires touching in-flight executions.
4. If the execution is at real risk of hitting the hard cap before any of the
   above can land, and it is safe to interrupt, cancel or terminate it
   (`POST /api/harvest/workflows/{execution_id}/cancel` or `/terminate`)
   rather than let it dead-letter on the hard cap.

### Escalation criteria

Ticket the workflow owner with the type name and the ranked list from
`history_bloat_min_events`. Escalate if a specific execution keeps re-crossing
(growth continuing well past the first warning) and is on a clear trajectory
to reach the hard cap within the next alerting window, or if the growth is
concentrated across many concurrent executions of the same type rather than
one outlier — the latter usually means the workflow's checkpoint strategy
itself needs to change, not just the one instance.

## harvest_queue_paused_too_long

**What to do when a task queue has been held by an operator for too long:** the
`harvest.queue.paused` gauge (issue #619) reports `1` for every queue whose
dispatch is currently held and `0` otherwise. A pause is *safe* — nothing fails,
retries, or dead-letters, and already-`RUNNING` tasks finish naturally — but it
is also *silent*: work accumulates as `PENDING`, no other alert fires, and a
forgotten pause is indistinguishable from a healthy idle queue until the backlog
breaches an SLA. This alert exists to make a forgotten freeze loud.

> **"Nothing fails" has one exception: `schedule_to_close`.** A pause suspends
> only the *relative* `schedule_to_start` timer. An activity carrying an
> **absolute** `schedule_to_close` deadline (issue #378) is still timed out
> while the queue is held — a pause is not an SLA extension. The longer the
> hold, the more of the held backlog can cross its own absolute deadline, so
> treat this alert's `for` window as a real budget, not a formality. Crediting
> held time back to `schedule_to_close` is explicitly out of scope for issue
> #619.

Because the gauge is binary, the **`for` duration is the threshold**, not the
value. Alert on `max by (queue) (harvest_queue_paused) > 0` with `for: 1h`
(ticket) or `for: 4h` (page).

### Triage steps

1. **See what is held and why.** The read endpoint returns the reason, the
   operator who placed the hold, when it was placed, and how much work is
   waiting behind it:

   ```bash
   harvest queue list-paused
   # or: curl -s .../api/harvest/admin/queues/paused | jq
   ```

   The Vantage **Workers** page shows the same thing as a banner — the fleet
   page an operator lands on when investigating idle workers.

   Check `effective_scope` on each entry (the banner's **Scope** column shows
   the same thing). It reports what the hold *actually* covers, derived from the
   shards that hold the queue versus the expected shard set — not the
   `scope_shard_id` intent recorded when the row was written:

   - `fleet` — every expected shard holds it.
   - `partial_fleet` — **some shards are still dispatching.** A fleet-wide pause
     that only reached part of the fleet writes `scope_shard_id: null` on the
     shards it reached and *no row* on the ones it missed, so intent alone cannot
     tell this apart from a complete hold. Re-issue the pause (it is idempotent);
     the original reason and operator are preserved. An unreachable shard also
     reports `partial_fleet` — it may still be dispatching, so that is the safe
     reading.
   - `shard` — a deliberately shard-scoped hold.

2. **Confirm the downstream is actually back.** The reason field records why the
   hold was placed (`"SMTP provider outage"`). Verify that dependency is healthy
   before thawing, or the backlog will simply fail on release.

3. **Check how much is held.** `held_task_count` on the read endpoint is the
   number of `PENDING` tasks on the queue. A large number means the thaw will
   produce a burst — see *False positives* below for why that burst is safe.

4. **Resume.**

   ```bash
   harvest queue resume email-workers
   ```

   Held tasks become immediately claimable. The response echoes the
   `released_reason` and `released_paused_by` of the hold you just released —
   the pause row is deleted on resume, so this is the last moment the *why* is
   recoverable *from that row*.

   For a **post-incident** review after the row is gone, read the audit log
   instead — who/why/when survives the resume permanently there:

   ```bash
   harvest audit list --operation queue.pause --target-id email-workers
   harvest audit list --operation queue.resume --target-id email-workers
   ```

   The reason is carried in the record's **`error_summary`** field, which is the
   only free-text column on `harvest_audit_log` (there is no metadata column) —
   so it is populated on `succeeded` rows too, not just failures. It reads:

   - `reason: <why>` on a `queue.pause` row;
   - `reason: released hold by <actor>: <why>` on the matching `queue.resume`
     row;
   - when independently-paused shards carried **different** holds, the resume row
     instead lists every one of them, longest first, so no shard's *why* is lost
     once its pause row is deleted:
     `reason: released 2 holds (longest first); shards 0,2 by alice: stripe
     outage (held 900s); shard 1 by bob: pg failover (held 60s)`. Shards sharing
     one incident collapse into a single entry, so this grows with the number of
     distinct holds, not the shard count;
   - `... | failures: <detail>` appended on either when the operation only
     partially applied, naming the shards it did not reach.

5. **Confirm the thaw.** `harvest queue list-paused` should now be empty and the
   `harvest_queue_depth` / `harvest_queue_oldest_pending_age` panels should
   start falling.

### Likely causes

- **A genuine, still-ongoing downstream outage.** The hold is doing its job; the
  alert is telling you the outage has outlasted its expected window.
- **A forgotten hold.** The incident was resolved but nobody resumed the queue.
  This is the failure mode the alert exists for.
- **A pause placed on the wrong queue name.** Check `list-paused` against the
  queue you meant to freeze; a typo produces a hold on a queue nobody is
  watching. (A name with *surrounding whitespace* is rejected with a `400`
  rather than accepted: queue names are matched exactly, so a stray space would
  hold a different queue than the one intended.)
- **A shard-scoped pause left behind** after a fleet-wide one was released
  (`shard_id` is surfaced on the read endpoint).
- **A fleet-wide pause that only partially applied** — `effective_scope:
  "partial_fleet"`. The shards it missed never stopped dispatching; re-issue it.

### False positives

- **A short, deliberate freeze during a planned dependency migration.** Expected —
  this is why the rule uses a `for` duration rather than firing immediately.
  Tune `for` to the longest freeze your team plans for.
- **A pause on a genuinely idle queue.** The hold is harmless because there is
  nothing to hold. Use the sharper alert variant in the pack
  (`harvest_queue_paused > 0 and harvest_queue_depth > 1000`) to only fire when
  the hold is actually accumulating a backlog.
- **A rising `harvest_queue_depth` while this gauge is `1` is NOT a capacity
  incident.** It is the deliberate hold working as designed. This pairing is the
  single most common false page during an outage freeze — check this gauge
  before escalating a backlog alert.
- **`harvest_queue_schedule_to_start` does not spike on a thaw.** Resume credits
  the held time back to each task's `scheduled_at`, so a queue held for six
  hours does not report six hours of schedule-to-start latency on release. The
  credit is measured against the live clock at release, so the time the resume
  itself takes — waiting on its queue lock and shifting a large backlog — is
  credited too, and does not count against any task's `schedule_to_start`. A
  task *enqueued while the resume is running* is credited as well: the hold is in
  force until the resume commits, so a second pass picks up those late arrivals.
  The one residual is sub-millisecond (a row committed during that final pass's
  own execution), far below any usable `schedule_to_start`.

### Safe actions

- `harvest queue list-paused` — read-only.
- `harvest queue resume <queue>` — releases the hold. Idempotent: resuming a
  queue that is not paused is a success no-op, so a retry after a lost response
  is safe.
- `harvest queue pause <queue> --reason "<why>"` — re-freezing is idempotent and
  **preserves the original reason and operator**, so a re-pause never overwrites
  the provenance of an existing hold.
- Both mutating commands **exit non-zero when a fleet-wide hold only partially
  applied** (HTTP `207`): the shards the request missed keep dispatching into the
  outage, so a scripted `harvest queue pause` step fails loudly rather than
  reporting success. Re-issue it — both operations are idempotent — and confirm
  with `harvest queue list-paused` that `effective_scope` reads `fleet` rather
  than `partial_fleet`.
- Inspecting a held task with `harvest workflow stack <id>` or the eligibility
  explainer (`GET /admin/queues/{queue}/eligibility`), which reports
  `queue_paused` as the first impediment.

### Escalation criteria

Escalate to the team that owns the downstream dependency named in the pause
reason. Page if the held backlog is breaching a business SLA and the dependency
has no ETA — at that point the decision is a business one (keep holding and
accumulate, or resume and let the work fail into the DLQ where it can be
redriven later).

---

## harvest_scanner_stalled

**What to do when a background control loop stops ticking:** Harvest's
correctness in production depends on a fleet of control loops that run as
bare spawned Tokio tasks *inside the embedder's process* — timeout
enforcement, the soft-SLA scanner, poison-pill orphan reclaim, the external
signal/cancel/await outboxes, the retention janitor, the schedule ticker, and
the bounded-pause auto-resumer. If one panics, deadlocks on a poisoned
connection, or stalls on a never-returning query, it fails **silently**: the
work it owns simply stops happening, and every other part of the process keeps
running normally.

`harvest.scanner.tick` (issue #797) closes that blind spot. It is incremented
**unconditionally at the end of every iteration** — including iterations that
found no work and iterations whose pass returned an error — under a bounded
`scanner` label. That is what makes it different from every other loop metric
in the catalogue (`harvest.retention.deleted`,
`harvest.schedule.fire_attempts`, the timeout/SLA/quarantine counters): those
only emit when there *is* work, so a healthy idle loop and a dead one both read
zero. Here, **a flat-lined series is the wedge signal.**

There are seven `scanner` label values but **five** spawned loops: `sla` and
`external_outbox` are enforcement responsibilities *inside* the `timeout` loop,
not tasks of their own. All three are ticked together by that loop, so they
**share one liveness fate and cannot diverge** — `timeout` healthy implies `sla`
and `external_outbox` healthy. They keep distinct labels because they name
distinct responsibilities, not because a divergence between them is observable.
To attribute a *partial* pass failure to a specific sub-pass, use that pass's
own work counters and its `tracing::error!`, not this heartbeat.

| `scanner` | Loop | What silently stops |
| --- | --- | --- |
| `timeout` | `spawn_timeout_checker` | Activity/workflow timeouts, plus every sub-pass below |
| `sla` | `enforce_workflow_sla_breaches` (sub-pass) | Soft-SLA breach signals (#487) |
| `external_outbox` | external signal/cancel/await outboxes (sub-pass) | Cross-shard signal, cancel, and await delivery |
| `poison_pill` | `spawn_poison_pill_reclaimer` | Orphaned-task reclaim after a worker crash (#367) |
| `retention` | `RetentionRuntime::spawn` | History/audit/summary GC (#737, #752) |
| `schedule` | `Scheduler::spawn_sharded` | Every cron/interval schedule firing |
| `pause_auto_resume` | `spawn_pause_auto_resumer` | Bounded-pause auto-resume (#383) |

### Triage steps

1. Identify the stale loop(s) without a metrics pipeline:
   `harvest preflight --output json | jq '.checks[] | select(.name == "scanner_liveness")'`.
   The `details.stale_scanners` array names them; `details.scanners[]` carries
   per-loop `verdict`, `age_secs`, `tick_count`, and the
   `staleness_threshold_secs` it was judged against.

   **Point it at the right process.** The registry is in-process, so this check
   only ever describes the process that answers it. In a split API/worker
   topology, an API-only replica reports `scanners_registered: 0` and `pass` —
   which is correct for *that* process and says nothing about the worker. Use
   the metric (step 2) to find which replica went quiet, then run the check
   there.
2. With Prometheus, find the replica:
   `rate(harvest_scanner_tick_total{scanner!="retention"}[5m])` — the wedged
   loop reads `0` on the affected `instance` while its siblings and the other
   replicas keep incrementing. Deliberately **not** `sum by (scanner)`: every
   replica runs its own copy of all seven loops, so summing lets a healthy
   replica mask a wedged one. Use a wider window for `retention`
   (`increase(harvest_scanner_tick_total{scanner="retention"}[3h])`), which
   polls hourly by default.
3. Read the worker process logs around the time the series flat-lined. A
   panicked loop leaves a panic backtrace; a stalled one leaves nothing at all,
   which is itself diagnostic.
4. Confirm the downstream symptom the loop owns, using the table above. For
   `timeout`, look for `RUNNING` executions past their `deadline_at`; for
   `schedule`, check `GET /admin/schedules` for overdue rows
   (`harvest_schedule_missed_runs` usually fires alongside).

### Likely causes

- A panic inside the loop body that killed the spawned task while the process
  survived.
- A query that never returns (a lock wait with no `statement_timeout`, a
  pathological plan on a large `harvest_events` table).
- Deadpool connection exhaustion where the loop's `pool.get()` blocks
  indefinitely — check the pool sizing (`harvest.pool` config) and whether a
  hot-path caller is holding connections across an `.await`.
- A `db` blip that poisoned the loop's connection in a way it does not recover
  from.

### False positives

- **An API-only replica.** A process that runs no worker spawns no scanners.
  It exports no `harvest_scanner_tick` series at all and its `scanner_liveness`
  check reports `pass` with `scanners_registered: 0`. Alert on the *rate* of an
  existing series; do **not** use `absent()`, which would fire on every
  API-only pod.
- **A loop polling slower than the alert window.** The loops do *not* share a
  cadence: `timeout`/`sla`/`external_outbox` poll every 500 ms, `schedule`
  every 1 s, `poison_pill`/`pause_auto_resume` every 5 s — but `retention`
  polls **hourly** by default. That is why the shipped rule carries two
  expressions with different windows; a single 5-minute window would page
  continuously on a perfectly healthy retention janitor. Retune both if you
  have changed `WorkerConfig::poll_interval` or the retention tick interval.
  The preflight check needs no retuning — it derives its own threshold from
  each loop's registered interval (`max(2 × poll_interval, 60s)`).
- **The first minute after boot.** Most loops sleep one interval before their
  first iteration (the schedule ticker is the exception: it runs a pass first,
  then sleeps). The preflight check accounts for this with a grace window keyed
  to registration time — note that for the hourly retention janitor that grace
  is 2 hours, so a freshly booted process legitimately reports it as healthy
  long before its first tick. A rate-based alert evaluated during startup can
  transiently read zero. The tick series is created at **registration** time
  (see the next bullet), so it exists from process start rather than appearing
  only at the first tick — the boot window for a slow loop is therefore its
  first poll interval after start, and the hourly retention expression reads
  zero throughout hour 0–1 of a fresh process. Give the rule a `for:` of at
  least one poll interval to ride that out.
- **Not a false positive: a restart mid-window.** A container restart that
  preserves the label set resets the counter to zero, but `increase()` is
  reset-**aware** — it computes `last - first + correction`, adding the
  pre-reset value back at each reset. A healthy retention janitor that ticked
  at least once before the restart therefore reads `increase > 0`
  (`3,4,5,reset,0,0 → 0 - 3 + 5 = 2`), the `== 0` side fails, and the rule cannot
  fire; the startup gate is never even consulted. The only post-restart shape
  that fires is one whose pre-restart samples were **flat**
  (`5,5,5,reset,0,0 → 0 - 5 + 5 = 0`) — i.e. the janitor had already been stalled
  for the whole window and the rule was already correctly firing. It simply
  keeps firing, for at most one more hour, until the new process's first pass
  proves liveness. Do **not** "fix" this with `unless resets(...[3h]) > 0`:
  that blinds the alert for a full window after **every** deploy, hiding a
  genuine post-restart wedge — strictly worse than prolonging a page that was
  already true.
- **A missing series is not an alert.** `== 0` never fires on a series that
  does not exist. Two distinct cases:
  - *A wedge on the very first iteration.* This is **covered**: each loop
    initializes its own tick series at zero when it **registers**, at spawn
    time, before the loop body runs. So a loop that panics or hangs before it
    ever completes a pass still exports a flat series that `rate(...) == 0`
    matches, instead of exporting nothing and staying silent forever. (The
    `scanner_liveness` preflight check covered this case from the start —
    registration precedes the first iteration, so the loop ages into
    `stale`/`wedged` with `has_ticked: false`.)
  - *The wrong metrics backend.* This is **not** covered. If your metrics come
    from the plugin's built-in `with_metrics_scrape()` endpoint rather than the
    `metrics-rs` adapter, no `harvest_scanner_tick` series is exported at all
    (that endpoint bridges a deliberately narrow subset and does not back the
    full starter pack), so the alert is silently inert. Use the `metrics-rs`
    recorder, or rely on the preflight check.
  `absent()` is deliberately **not** used to paper over either case: a process
  that runs none of these loops (an API-only replica) legitimately exports no
  series, and would page falsely forever.
- **Graceful shutdown — the one case that needs a gate.** A draining worker
  stops its loops on purpose. The `scanner_liveness` check is not fooled — a
  clean stop deregisters each loop, so a process that drains its worker while
  continuing to serve HTTP reports `pass` with `scanners_registered: 0` rather
  than seven phantom wedged scanners. (A loop that *panics* deliberately does
  **not** deregister, so it still ages into `wedged` — that is the signal.)

  The rate-based alert has no such context. Deregistration is in-process only:
  Prometheus counters cannot be un-exported, so a drained loop's series stays
  in the exporter at its final value and reads `rate() == 0` forever. If the
  process **exits** after draining, the scrape target disappears and the alert
  resolves on its own; if it **keeps serving HTTP** after draining (a real
  shape — `stop_harvest_runtime` leaves the API up), the alert pages
  indefinitely for a loop that was stopped on purpose. Gate it on the process
  still owning worker duties. The worker slot gauges are the natural signal:
  they are sampled by the worker's own monitoring task, so they go stale and
  then absent once it drains, while a *wedged* loop leaves them reporting
  normally:

  ```promql
  (rate(harvest_scanner_tick_total{scanner!="retention"}[5m]) == 0)
    and on(instance) (count by (instance) (harvest_worker_slots_available) > 0)
  ```

  Since the tick series is created at registration (see below), this also
  covers the narrow case of a process that registers its loops and drains
  before any of them completes a first iteration. Adapt `instance` to whatever
  target label your scrape config uses. If your topology runs `retention` or
  `schedule` on a process with no worker, gate those two on that process's own
  identifying label instead — or rely on the `scanner_liveness` check, which
  needs no gate because it knows what is registered.
- **Not a false positive: one wedged shard.** A multi-shard worker spawns a
  `timeout`, `poison_pill`, and `pause_auto_resume` loop **per assigned shard**,
  all under one `scanner` label. Both surfaces handle this, and both have to:
  the counter carries a bounded **`shard`** label (the shard id, or `none` for
  the process-wide `retention`/`schedule` loops and single-shard deployments),
  so each instance lives on its own series and `rate(...) == 0` evaluates the
  wedged shard on its own. Without that label the instances would share one
  series on one scrape target and a healthy shard's ticks would hold the rate
  above zero while a sibling's loop was dead — the wedge would be **masked**,
  not merely unlocalised. If you write your own grouping, keep `shard` for the
  same reason you keep `instance`. A repeated entry in `shard_assignments` is
  deduplicated at the config boundary, so a duplicate can neither spawn a
  second set of loops against one database nor collapse two instances onto a
  single series.

  The preflight check tracks each instance separately, reports the **worst**,
  and **names the shard** in both the summary and the per-scanner entry — plus
  **every** stale shard in `affected_shards`, so a two-shard wedge shows both
  rather than sending you to one database while the other stays unprotected:

  ```console
  $ harvest preflight --output json | jq '.checks[] | select(.name == "scanner_liveness")'
  {
    "name": "scanner_liveness",
    "status": "fail",
    "summary": "1 of 5 background control loops are stale: timeout (shard 1)",
    "affected_shards": [1],
    "details": {
      "scanners": [
        { "scanner": "timeout", "shard": 1, "verdict": "wedged", "age_secs": 214, ... }
      ]
    }
  }
  ```

  `affected_shards` is the standard localization field every preflight check
  shares, so the plain table view carries it too -- `harvest preflight` renders
  it in the SCOPE column:

  ```console
  $ harvest preflight
  STATUS  CHECK              SCOPE     SUMMARY
  fail    scanner_liveness   shards=1  1 of 5 background control loops are stale: timeout (shard 1)
  ```

  When more than one shard is wedged, `affected_shards` carries **all** of
  them (`shards=1,2`) while the `shard` field names the single worst instance —
  the fold that decides the verdict picks one owner, but the blast radius must
  not be understated. It lists only the **stale** instances, so a healthy
  sibling shard is never reported as affected. `shard` / `affected_shards` are empty for the
  process-wide loops (`retention`, `schedule`) and on single-shard deployments,
  where there is no fan-out to disambiguate; the SCOPE column then reads `-`.

- **Known blind spot: two runtimes on one shard in one process.** The alert
  separates instances by `(instance, scanner, shard)`, which covers every
  deployment shape Harvest supports — one worker process per shard, or one
  multi-shard worker. It does **not** separate two *independently constructed*
  runtimes (two `Worker`s, or two `RetentionRuntime`s) polling the **same**
  shard inside one process: their ticks land on one series, so a healthy one
  holds the rate above zero after its peer wedges and this alert stays quiet.

  A per-owner metric label was rejected rather than overlooked — the registry's
  owner ids are monotonic and never reused, so labelling by them would grow
  without bound in any process that restarts a runtime, which is a worse bug
  (unbounded cardinality) than the one it would fix.

  The capability is not missing, only on the other surface: the registry tracks
  those owners **separately**, so `harvest preflight` still reports the worst
  of them and names the affected shard. If you run two runtimes of the same
  kind in one process, treat the `scanner_liveness` check — not this alert —
  as the authority for those scanners.

### Safe actions

- Restart the worker process. This is the primary remediation: the loops are
  spawned at worker start, and Harvest's durable state means a restart re-runs
  the enforcement that was missed with no data loss. Every pass is idempotent
  and driven off DB state.
- While a `timeout`/`sla` loop is wedged, nothing is lost — the deadlines are
  persisted columns (`deadline_at`, `sla_deadline_at`) and are enforced
  whenever the loop next runs. The same is true for the outboxes and the
  auto-resumer.
- A wedged `schedule` loop **does** skip firings; after restart, use
  `POST /admin/schedules/{id}/backfill` to replay the missed window if the
  schedule's catchup policy did not already cover it.
- Do **not** disable the alert to silence it. Detection here is the entire
  point: the alternative is discovering the outage through a missed SLA. If it
  is firing on a *healthy* loop, the window is mistuned for that loop's
  cadence — retune the window (see *False positives*), do not delete the rule.

**This check gates deploys.** `scanner_liveness` participates in the overall
`harvest preflight` verdict, and the CLI exits `2` on `warn` and `1` on `fail`.
A stale scanner will therefore **block a pipeline** that runs `harvest preflight`
as a gate — which is intended (deploying onto a process whose enforcement loops
are wedged is exactly what a preflight gate is for), but is worth knowing before
you wire the gate. See `docs/runbooks/safe-deploy.md`.

### Escalation criteria

Page immediately: a wedged enforcement loop is an active, ongoing correctness
outage for the work it owns, and its blast radius grows for as long as it lasts.
Escalate to the team owning the embedding service if a restart does not restore
ticking, or if the loop re-wedges after restart — that indicates a
reproducible stall (a pathological query or a connection-pool deadlock) rather
than a one-off panic, and needs a fix rather than a bounce.

## harvest_no_capable_worker

**What to do when tasks are being claimed by workers that cannot run them:** any
worker polling a queue can claim any task on it — the `SKIP LOCKED` claim query
has no capability filter, and cannot have one (a worker can enumerate the
handlers it *has* registered, never the ones it has not). When the claiming
worker has no handler for that task's workflow or activity type, the engine
**releases the claim back to `PENDING`** for a capable peer instead of terminally
failing the execution (issue #804).

That release is the benign, self-healing case and is what you will see during a
routine rolling deploy that introduces a new workflow or activity type: for a
window, some pods have the handler and some do not, and tasks bounce until they
land on a new pod. Nothing is lost, and the counter reports
`outcome="released"`.

The page is `outcome="escalated"`. Each release backs the task off (capped
exponential, 1 s doubling to 30 s) and records the releasing worker in the
task's **distinct-miss set**; once that set would exceed
`WorkerConfig::capability_miss_max_redeliveries` (default **5**) the task falls
through to the normal terminal-failure path with a stable, greppable
`no_capable_worker:` reason. Escalation means **executions are being failed**,
and the reason string on the execution row names *why* — read it before drawing
any conclusion about the fleet. Only one of the two bounds that reach this
outcome supports "no live worker here has the handler"; the other trips when
coverage could not be confirmed at all, and a live, untried worker may well be
capable. The cause table in triage step 4 below tells them apart. No dead-letter
row is written on this path — the reason lives on the failed execution, so
diagnose from `GET /api/harvest/workflows?state=FAILED`, not from the DLQ.

The budget counts *distinct* workers, not total releases, precisely so that one
incapable pod repeatedly winning the claim race cannot page you while a capable
peer is live: a repeat miss by a worker already in the set backs off but
consumes no budget. Reaching this page via that bound therefore means `N + 1`
**different** workers each failed to resolve the handler.

A distinct-worker count alone cannot bound a fleet **smaller** than the budget —
one incapable pod pins the set at 1 forever. So once the registry confirms every
live eligible worker on the queue has already missed the task, the same budget
value bounds *total* releases too, and a single-worker fleet escalates after `N`
releases (~31 s) rather than `10 × N`. It reports the same budget-exhausted
reason, because the registry confirmed the same fact.

That total bound requires **confirmed** coverage. If the registry cannot be read,
`N` total releases may all have been won by the same pod — the case the distinct
count exists to reject — so a third, deliberately generous **absolute ceiling**
of `10 ×` the budget remains as the backstop for the two states where coverage is
unprovable: a live worker that never missed the task, or an unreadable registry.
It escalates with the same `escalated` outcome, and its reason string names the
real release count and the distinct count (`spread across only D distinct
worker(s)`) rather than the budget, so it is distinguishable in triage; see the
four-way table in step 4.

Those conclusions are sound because the task was *offered around the queue*
first. Two escalation causes fail an execution on its **first** claim after
**zero** releases — a `capability_miss_max_redeliveries = 0` config, and a task
pinned to a worker session (#606) whose host lacks the handler — and for those, a
capable worker may be live and idle the entire time. They report a **different**
outcome value, `escalated_never_offered`, and fire the ticket-severity
[`harvest_capability_miss_never_offered`](#harvest_capability_miss_never_offered)
instead. This page selects `outcome="escalated"` with an exact matcher, so if
you are holding it, the task genuinely bounced around the queue first.

**Worker-fleet contract.** All workers polling a given queue should register the
same handler set. Harvest no longer punishes a *transient* mismatch (it is
released and retried), but a *durable* one is still fatal, only later and with a
much clearer reason. If you intentionally run a heterogeneous pool, give each
handler subset its own queue rather than relying on the redelivery budget to
route work.

| Signal | Meaning | Action |
| --- | --- | --- |
| `outcome="released"`, brief burst during a deploy | Capability skew mid-rollout | None — self-heals as the rollout completes |
| `outcome="released"`, sustained > ~15m | Rollout stalled, or a pod set permanently lacks the handler | Ticket — see [`harvest_capability_miss_release_sustained`](#harvest_capability_miss_release_sustained) |
| `outcome="escalated"`, any | Budget exhausted: no capable worker exists; executions failing | Page — see *Triage steps* |
| `outcome="escalated_never_offered"`, any | Executions failing, but released **zero** times: a `0` budget, or a session pin. Draw **no** fleet-wide conclusion | Ticket — see [`harvest_capability_miss_never_offered`](#harvest_capability_miss_never_offered) |

Distinct from two adjacent signals, deliberately:

- `harvest.task.quarantined` (issue #367, a metric — there is no quarantine
  alert in the starter pack) counts a worker **process crash**
  (panic/OOM/segfault). A clean "handler not registered" miss is not a crash and
  **never** increments `crash_strikes`, so a capability miss can never trip
  poison-pill quarantine.
- `harvest_no_compatible_worker` (issue #171) is **build-id routing**: the
  handler exists, but no worker with a *compatible build* is live. Capability
  miss is the coarser condition — the handler is not registered at all.

### Triage steps

1. Confirm which types are affected:
   `GET /api/harvest/admin/workflow-types/reachability` (issue #520). A verdict
   of `orphaned` means that type has live non-terminal executions but **no**
   registered handler anywhere in the fleet — the exact condition escalation
   reports.
2. Confirm the queue has any live worker at all:
   `GET /api/harvest/admin/queue-coverage` (issue #774). An uncovered queue is
   a different (and simpler) failure — see `harvest_queue_uncovered`.
3. Identify which builds are actually polling the affected queue:
   `GET /api/harvest/workers`. Compare `build_id` / `deployment_name` against
   the build you expect to carry the new handler.
4. Read the escalated executions:
   `GET /api/harvest/workflows?state=FAILED` — the `error` of an escalated run
   begins with `no_capable_worker:` and names the missing workflow/activity
   type. **Escalation does not write a dead-letter row**: it routes through the
   ordinary terminal-failure path (`WorkflowFailed` + the execution row's
   `error`), which is not the DLQ. Do not look in `GET /dead-letters` for these
   — it will be empty, and that emptiness is not evidence the alert is spurious.

   **What this alert already rules out.** The redelivery budget cannot escalate
   while the worker registry still lists a live worker on the queue that has
   never missed this task. Before the budget may terminate a task, the workers
   recorded as having missed it must *cover* the live fleet for its queue
   (`harvest_workers`, using `2 × worker_heartbeat_interval` **floored at
   120 s** — deliberately wider than the poison-pill reclaimer's window, since
   this query judges peers whose heartbeat cadence it cannot read; at the
   default 5 s cadence that is 120 s here against 10 s there). So "a capable pod
   was up the whole time and simply kept losing the claim race" is not a way to
   reach rows 1a/1b below — the only bound that can fire in that situation is
   the absolute ceiling, which says so explicitly (row 2b).

   That floor also sets how long the alert stays quiet after a pod genuinely
   dies: for up to 120 s its row still reads as "a capable peer may exist", and
   in that interval **both** evidence-derived bounds are withheld — the
   fleet-covering one *and* the distinct-worker one. Only the absolute release
   ceiling (`10 ×` the budget) can still escalate, so a task in that window
   waits on the ceiling rather than on the configured `max_redeliveries`.

   The `error` distinguishes the **four** escalation causes, which have
   different fixes; two of them additionally state what the registry could or
   could not confirm. **Only the first two causes reach this alert.** The other
   two report `outcome="escalated_never_offered"` and fire the ticket-severity
   [`harvest_capability_miss_never_offered`](#harvest_capability_miss_never_offered)
   instead — they are listed here so that, if you arrive at a `no_capable_worker:`
   error from a log or a support ticket rather than from this page, you can tell
   which alert should have fired and which triage to follow:

   | # | `error` says | Cause | Alert | Fix |
   | --- | --- | --- | --- | --- |
   | 1a | `escalated after R capability-miss redeliveries across D distinct worker(s); capability_miss_max_redeliveries = N; every worker with a live heartbeat here has now missed it, so no live worker on this queue has the handler` | The budget was exhausted **and** the registry confirmed the missers cover the live fleet. The queue really was swept. `D > N` means `N + 1` distinct workers each missed it; `D ≤ N` means a fleet smaller than the budget exhausted it on total releases. | **This page** | Deploy the handler / finish the rollout. Steps 1–3 apply as written. |
   | 1b | `escalated after R capability-miss redeliveries across D distinct worker(s); capability_miss_max_redeliveries = N; the worker registry could not be used to confirm the live fleet …` | The budget was exhausted, but the claiming worker was **not listed** against this queue in `harvest_workers`, so no fleet conclusion was available. `R` can exceed `N` here — with no registry evidence a repeat miss backs off but consumes no budget, so releases accumulate until a fresh distinct worker finally exhausts it. | **This page** | Fix the registry first: check worker heartbeats (`GET /api/harvest/workers`) and that workers advertise this queue name. The handler may be fine. |
   | 2a | `escalated after R capability-miss redeliveries spread across only D distinct worker(s), hitting the absolute release ceiling of C releases (10x capability_miss_max_redeliveries (N)) …; fewer distinct workers missed this task than the budget allows …` | The task bounced `R` times across **fewer** distinct workers than the budget allows **and fleet coverage was never established**, so neither gated bound could fire and only the ceiling was left. (A *registered* small fleet does not land here: once the registry confirms coverage, the configured-total bound escalates it at `N` — that is row 1a with `D ≤ N`.) `C` is the computed ceiling (`10 × N`), not `N`. This does **not** mean the queue was swept. | **This page** | Check those `D` workers first (step 3), *then* the fleet. See the note below. |
   | 2b | `… hitting the absolute release ceiling …; a live worker on this queue never missed this task …` | A live worker on the queue **never missed it**, so the fleet gate withheld the budget and only the ceiling could fire. That worker may well be capable. | **This page** | Go to that worker, not the deploy: is it saturated, draining, advertising a stale queue list, or ineligible for the activity's registered `requires` labels (a legacy row with no `required_capabilities` snapshot is gated on the peer's own registry, which the fleet read cannot see)? |
   | 3 | `escalated immediately after 0 redeliveries: capability-miss redelivery is disabled (capability_miss_max_redeliveries = 0) …` | Redelivery is switched off, so the task was failed on its first claim by a single incapable worker and never offered to a peer. | Ticket: `harvest_capability_miss_never_offered` | Raise `capability_miss_max_redeliveries` off `0`. Steps 1–3 may find nothing wrong — this is a config, not a missing deploy. |
   | 4 | `escalated immediately after 0 redeliveries: task is pinned to worker session {id} …` | The task is hard-pinned to one host (#606) and could never be offered to a peer at all. | Ticket: `harvest_capability_miss_never_offered` | Go to the pinned host, not the fleet. Raising the budget is a **guaranteed no-op** here. |

   The matching worker log carries a `session_pinned` boolean, a
   `distinct_incapable_workers` count, a `completed_releases` count, a
   `fleet_evidence` field (`AllLiveWorkersMissed` / `CapablePeerMayExist` /
   `Unavailable` — the same three states rows 1a/1b/2b are drawn from), an
   `outcome` field holding the same value as the metric label, and (when pinned)
   a `session_id`, so the same split is greppable in logs as well as in the
   execution's `error`.

   **Counts in the `error` are what actually happened.** `R` and `D` report the
   *persisted* record — redeliveries that completed and workers that released.
   The claim that escalated is not counted: it never released, so including it
   would have you looking for a redelivery that does not exist.

   **Row 2a has a second reading.** It fires when the *absolute* bound (`10 ×`
   the budget in total releases) trips while the distinct-misser set stayed
   within budget and the registry gave no usable answer — a worker not listed
   against this queue, so coverage could not be confirmed. The likely cause is a
   missing handler on the workers that actually claimed it, but it can also mean
   a capable peer kept losing the claim race while being absent from the
   registry. It pages anyway because the executions are failing either way and
   under-paging a genuinely missing handler is the worse error. Losing
   `10 × budget` consecutive races across ~25 minutes of backoff is not a
   realistic steady state, so treat row 2a as a real handler outage first, and
   check `distinct_incapable_workers` against `GET /api/harvest/workers` to rule
   out the race reading. Fix the registry gap too — with a readable registry this
   shape would have escalated at the configured budget with a much clearer
   reason. (Row 2b is the case where the registry *did* see the peer — there,
   start with the peer.)

   If this page is firing, you are in row 1 or 2 by construction: the alert
   selects `outcome="escalated"` with an exact matcher. Rows 3–4 are here for
   cross-reference only.

### Likely causes

- A rolling deploy that introduces a new workflow or activity type is **stalled
  or was rolled back halfway**, so the pods carrying the handler never became a
  majority (or were removed).
- A handler was **deleted or renamed** while in-flight executions of that type
  still existed. `GET /admin/workflow-types/reachability` before removing a
  handler is the pre-flight for exactly this (see
  `docs/runbooks/safe-handler-removal.md`).
- A **heterogeneous worker pool** shares one queue but registers different
  handler subsets, and the capable subset is too small (or scaled to zero) for
  the redelivery budget to find it.
- The task's owning **worker session** (issue #606) is hard-pinned to a host
  that lacks the handler. A session-pinned task cannot be released for a peer —
  the pin is the point — so it escalates immediately rather than bouncing.
  **This case is self-identifying**: its `error` says `pinned to worker session
  {id}` and reports `0 redeliveries`, instead of naming the budget. It is the
  one escalation where *step 1 is expected to disagree with you* —
  `reachability` may correctly report `in_use` because a capable worker does
  exist elsewhere in the fleet; the task simply cannot reach it. Go straight to
  the pinned host (`GET /api/harvest/workers`, match the session's host) and ask
  why *it* lacks the handler. Do not chase a missing deploy on the strength of
  the `no_capable_worker:` prefix alone.

### False positives

- **A brief `released` burst during any deploy that adds a type is expected and
  benign.** This paging rule therefore selects `outcome="escalated"` only. The
  sustained-release signal is a separate, ticket-severity rule
  ([`harvest_capability_miss_release_sustained`](#harvest_capability_miss_release_sustained))
  that exists to catch a rollout which never finished, not one in progress.
- A single-worker development fleet restarting mid-task will show one or two
  `released` samples as the task is re-claimed. Harmless.
- Escalation is **probabilistic, not exhaustive**: because releases have no
  affinity, a task can in principle exhaust its budget on incapable workers while
  a capable peer exists but never happened to claim it. Backoff makes this
  progressively unlikely — each redelivery waits longer than the last (1s, 2s,
  4s, 8s, 16s at the default budget of 5, ~31 s of total dwell; the curve caps
  at 30 s per redelivery for larger budgets) — but if you see escalation on a
  queue that `reachability` reports as `in_use`, that is the cause — raise
  `capability_miss_max_redeliveries` rather than treating it as a lost handler.

  This is the *only* false positive for this alert. The two other causes of an
  escalation on an `in_use` queue — a **session-pinned** task and a **zero
  budget**, both of which raising the budget cannot fix — no longer reach this
  rule at all: they report `outcome="escalated_never_offered"` and fire the
  ticket-severity
  [`harvest_capability_miss_never_offered`](#harvest_capability_miss_never_offered).
  If this page is firing, the `error` names a non-zero redelivery count, so
  probabilistic exhaustion is the right diagnosis and raising the budget is the
  right fix.

### Safe actions

- **Complete or roll back the deploy.** This is the fix in the overwhelming
  majority of cases; the alert clears on its own once every pod polling the
  queue registers the handler.
- **Scale up the capable pod set** so a released task is more likely to land on
  it within the budget.
- **Raise the budget** (`WorkerConfig::with_capability_miss_max_redeliveries`) if
  your rollouts legitimately take longer than the default dwell window. This
  trades a longer time-to-detect for fewer spurious escalations; it does not
  make a genuinely missing handler survivable.
- **Re-run escalated executions after the fix lands.** Escalation seals the run
  through the ordinary terminal-failure path — a `WorkflowFailed` event and a
  `FAILED` execution row — and writes **no** dead-letter entry, so the DLQ
  redrive routes do not apply. Recover an escalated run the way you would any
  other terminal failure: re-start it, or fork it from history with
  `POST /api/harvest/workflows/{id}/reset` (issue #148). Find them with
  `GET /api/harvest/workflows?state=FAILED` and match the `no_capable_worker:`
  prefix on `error`.
- Do **not** set `capability_miss_max_redeliveries` to `0` to "turn the feature
  off". `0` means *escalate on the first miss* — the pre-#804 behavior, which is
  strictly worse during a deploy.

### Escalation criteria

Page immediately on any `outcome="escalated"`: executions are being terminally
failed for a reason that is a fleet-configuration error, not a workload error,
and every further task of that type will fail the same way until a capable
worker appears. Escalate to the team owning the deploy if
`GET /admin/workflow-types/reachability` reports `orphaned` for a type that is
supposed to be live — that indicates a handler was removed while in-flight work
still needed it, and the executions cannot make progress until the handler is
redeployed.

## harvest_capability_miss_never_offered

**A capability miss failed an execution on its FIRST claim, without ever
offering it to a peer.** This is the ticket-severity sibling of
[`harvest_no_capable_worker`](#harvest_no_capable_worker) for the escalations
that carry the *opposite* conclusion. Read that section first for the mechanism.

Normally a capability miss is released back to `PENDING` for a capable peer, and
escalation only happens after the per-task redelivery budget is spent — which,
*when the registry confirmed the missers cover the live fleet*, is real evidence
that no live worker on that queue registers the handler. **This rule fires when
the task was released zero times**, so not even that much is available: a capable
worker may be live and idle on the queue the entire time.

Two causes reach it, and the `no_capable_worker:` reason on the execution row
names which:

| Reason contains | Cause | Fix |
|---|---|---|
| `capability-miss redelivery is disabled (capability_miss_max_redeliveries = 0)` | Redelivery is switched off, so any capability skew fails executions immediately. `0` is the documented pre-#804 fail-fast behaviour. | Raise `WorkerConfig::with_capability_miss_max_redeliveries` off `0` (default `5`). |
| `pinned to worker session {id}` | The task is pinned to a worker session (issue #606) whose host does not register the handler. The claim gate (`session_id IS NULL OR sticky_worker_id`) means no other worker can *ever* claim it, so releasing it "for a capable peer" would be false. | Deploy the handler to the host holding that session. Raising the budget **cannot** help — the pin outranks it. |

Ticket, not a page: the cause is one config knob or one task's pin, not a
fleet-wide capability gap, and one of the two is a switch an operator
deliberately flipped. Executions *are* failing, though, so it is not silent
either.

### Triage steps

1. **Read the reason, not the fleet.** `GET /api/harvest/workflows?state=FAILED`
   and grep `no_capable_worker:`. The parenthetical names the cause — match it
   against the table above. These runs are **not** dead-lettered; the reason is
   on the execution row.
2. **Do not start from reachability here.** Unlike
   [`harvest_no_capable_worker`](#harvest_no_capable_worker), a verdict of
   `in_use` from `GET /api/harvest/admin/workflow-types/reachability` is the
   **expected** answer and does not contradict this alert — the handler exists
   somewhere, the task simply never got offered to whoever has it.
3. For the **disabled-redelivery** cause, confirm the effective setting:
   `GET /api/harvest/admin/config` (issue #695). If it is `0`, that is the whole
   explanation.
4. For the **session-pinned** cause, take the `session_id` from the reason
   string and find its host, then check what build that host runs:
   `GET /api/harvest/workers`.
5. Check whether the fleet-exhaustion page is *also* firing
   (`harvest_no_capable_worker`). If both are firing, treat that one first — it
   is the stronger signal and the recovery is a superset.

### Likely causes

- **`capability_miss_max_redeliveries` was set to `0`**, usually as a rollback
  switch during an incident where #804's release behaviour was itself suspected.
  Every capability miss then fails an execution immediately, which is exactly
  the pre-#804 behaviour this feature exists to remove.
- **A worker session (#606) outlived a deploy**: the session was acquired on a
  host that has since been left behind by a rollout, and the pinned activities
  reference a handler that host does not have.
- **A session was acquired on a heterogeneous pool** where only some pods
  register the session's activity handlers.

### False positives

- **A deliberate `0` budget during a controlled rollback.** If an operator has
  intentionally disabled redelivery, this alert is reporting the accepted cost
  of that decision, not a new fault. Silence it for the duration of the rollback
  rather than reacting to it.
- **A single stale session at the tail of a deploy.** One pinned task failing as
  the last old pod drains is a bounded, self-limiting event; the executions are
  recoverable by reset and the rule clears when the session ends.
- This rule says **nothing** about whether the queue has a capable worker. Do
  not conclude a handler is missing fleet-wide from it — that is
  [`harvest_no_capable_worker`](#harvest_no_capable_worker), which pages.

### Safe actions

- **Raise the budget off `0`** (`WorkerConfig::with_capability_miss_max_redeliveries`,
  default `5`) once the reason for disabling it has passed. This fixes the
  disabled-redelivery cause outright and cannot make anything worse: a task that
  a capable peer claims is a task that never fails.
- **Deploy the handler to the session's host** for the pinned cause, or drain
  that host so no new sessions are acquired on it.
- **Reset the failed executions** once a capable worker (or the pinned host) has
  the handler: `POST /api/harvest/workflows/{id}/reset`. Escalation writes no
  dead-letter entry, so reset — not DLQ replay — is the recovery.
- Do **not** raise the budget expecting it to fix the *session-pinned* cause. It
  is a provable no-op: a pinned task escalates on its first miss at any budget.

### Escalation criteria

Escalate to a page if `harvest_no_capable_worker` starts firing for the same
`queue` / `task_type` — that is genuine fleet exhaustion and a different, larger
problem. Escalate to the team owning the workflow if session-pinned escalations
persist after the responsible host has been redeployed: a session that outlives
its host's build indicates the session lease (issue #606) is outlasting the
deploy cadence, which is a topology problem rather than a capability one.

## harvest_capability_miss_release_sustained

**What to do when capability-miss releases will not settle.** This is the
ticket-severity sibling of [`harvest_no_capable_worker`](#harvest_no_capable_worker).
Read that section first for the mechanism — this one covers only the
`outcome="released"` half.

A *release* is the benign outcome: a worker claimed a task whose workflow or
activity type it has no handler for, and handed the claim straight back to
`PENDING` for a capable peer (issue #804). No execution is failed, no event is
appended, nothing is lost. A short burst of releases is the **expected**
signature of a rolling deploy that introduces a new type.

This rule fires only when releases are **sustained**: the released rate was
non-zero at every step across a 15-minute window. That means the skew is not
resolving on its own. Nothing has failed yet — but each released task pays a
backoff interval of added latency (1 s, 2 s, 4 s, 8 s, 16 s at the default
budget), and its per-task redelivery budget is being spent. Left alone, this
becomes `harvest_no_capable_worker`, which pages.

Ticket, not a page: work is still making progress and the fix is a deploy
action, not an incident action.

### Triage steps

1. Compare builds actually polling the queue: `GET /api/harvest/workers`. Group
   by `build_id` / `deployment_name`. The pods that lack the handler are the
   ones to finish rolling forward or roll back.
2. Confirm the handler exists *somewhere*:
   `GET /api/harvest/admin/workflow-types/reachability` (issue #520). A verdict
   of `in_use` means at least one live worker registers it, so this is genuinely
   partial skew and not a removed handler. A verdict of `orphaned` means no
   worker registers it at all — treat that as
   [`harvest_no_capable_worker`](#harvest_no_capable_worker) and expect
   escalation shortly.
3. Confirm the queue is covered at all:
   `GET /api/harvest/admin/queue-coverage` (issue #774). An uncovered queue is a
   different (and simpler) failure — see `harvest_queue_uncovered`.
4. Check the escalation counter is still flat:
   `sum by (queue, task_type) (increase(harvest_task_capability_miss_total{outcome="escalated"}[5m]))`.
   Any non-zero value means budgets are now being exhausted and this has already
   graduated to a page. **A zero here is not proof of zero escalations**: a
   `(queue, task_type)` series that has never escalated is created by its first
   escalation *already at 1*, and `increase` reports last-minus-first, so that
   first sample reads as 0. Cross-check with the set-difference arm the alert
   itself carries —
   `sum by (queue, task_type) (max_over_time(harvest_task_capability_miss_total{outcome="escalated"}[5m]) unless max_over_time(harvest_task_capability_miss_total{outcome="escalated"}[1h] offset 5m))`
   — or, authoritatively, with `GET /api/harvest/workflows?state=FAILED` filtered
   on the `no_capable_worker:` reason, which does not depend on scrape timing at
   all. The right-hand side is a **range**, not a bare `offset 5m`, so that a
   scrape or remote-write outage cannot be mistaken for a newly created series:
   it asks whether *any* sample existed in the preceding hour. A gap longer than
   that hour will still read as new — inhibit it with an Alertmanager
   `inhibit_rule` keyed on your own scrape-health alert (`up == 0` /
   `TargetDown`), which is deployment-specific and therefore not in the starter
   pack.

### Likely causes

- A rolling deploy that introduces a new workflow or activity type is **stalled
  mid-rollout** — paused, blocked on a failing readiness probe, or waiting on a
  manual promotion gate.
- A deploy was **rolled back halfway**, leaving a mixed fleet where the capable
  pods were removed but the tasks they created remain.
- A **heterogeneous worker pool** shares one queue but registers different
  handler subsets. This is a standing misconfiguration, not a transient one:
  give each handler subset its own queue instead of relying on redelivery to
  route work.
- The **capable pod set is too small** relative to the incapable one, so a
  released task usually lands on another incapable worker. Releases stay
  non-zero even though the fleet is technically capable.

### False positives

- **A deploy that legitimately takes longer than 15 minutes** — a large fleet, a
  slow canary soak, or a deliberately staged rollout — will hold this rule true
  for the duration and clear on its own. Correlate against the rollout's own
  progress before acting.
- A **queue that is intentionally heterogeneous** and whose owners have accepted
  the latency cost will keep this rule permanently true. That is a real standing
  cost, not a false alarm — split the queue or silence the rule for it
  deliberately.
- This rule says nothing about failure. If it is firing and
  `harvest_no_capable_worker` is not, **no execution has been failed**.

### Safe actions

- **Complete or roll back the deploy.** This is the fix in the overwhelming
  majority of cases; the rule clears once every pod polling the queue registers
  the handler.
- **Scale up the capable pod set** so a released task is more likely to land on
  it before its budget runs out.
- **Split the queue** if the pool is intentionally heterogeneous: give each
  handler subset its own queue. This removes the class of problem rather than
  tuning around it.
- **Raise the budget** (`WorkerConfig::with_capability_miss_max_redeliveries`) if
  your rollouts legitimately outlast the default dwell window. This buys time
  before escalation; it does not reduce the release rate this rule measures.
- Do **not** silence this by removing the released outcome from the metric — it
  is the only signal that distinguishes "deploying" from "broken" before
  executions start failing.

### Escalation criteria

Escalate to a page if `harvest_no_capable_worker` starts firing for the same
`queue` / `task_type`: budgets are now being exhausted and executions are being
terminally failed. Escalate to the team owning the deploy if
`GET /admin/workflow-types/reachability` reports `orphaned` for a type that is
supposed to be live — a handler was removed while in-flight work still needed
it, and those executions cannot make progress until it is redeployed.

## harvest_replication_down

**What to do when a shard has no connected standby:** cross-region DR is not
protecting that shard. The RPO is unbounded and growing, a failover right now
would lose everything since the standby stopped consuming, and the WAL the
abandoned slot retains accumulates on the primary until it exhausts the disk.

This alert keys on `harvest_replication_standbys{shard} == 0`, **not** on a lag
threshold, and that is load-bearing. `harvest.replication.lag_seconds` is
deliberately **absent** rather than `0` when the RPO is unknown — a dead standby
reported as a perfect RPO is the most dangerous number the DR feature could
publish — so an alert written as `lag_seconds > N` produces no series and stays
silent through exactly this outage. `harvest.replication.lag_bytes` *does* stay
real, because a slot pins WAL whether or not a walsender is attached; that is
your severity signal here.

### Triage steps

1. `harvest dr status --shard <id>=<dsn> -o json`. `connected_standbys: 0` with
   a growing `lag_bytes` confirms it.
2. On the primary, **scoped to this shard's database**:
   `SELECT slot_name, database, active, pg_current_wal_lsn() -
   COALESCE(confirmed_flush_lsn, restart_lsn) AS backlog FROM
   pg_replication_slots WHERE database = current_database() OR database IS NULL;`
   An `active = false` slot with a growing backlog is an abandoned standby.
   The `WHERE` clause is load-bearing: `pg_replication_slots` is **cluster-wide**
   and one cluster can host several shard databases, so an unscoped listing
   shows you a sibling shard's slots alongside your own.
3. On the standby: is the subscription enabled?
   `SELECT subname, subenabled FROM pg_subscription;` Check the standby's log
   for apply-worker errors — a constraint violation or a missing table stops
   apply permanently and the slot then just accumulates.
4. Check the standby host itself: disk full, out of memory, restarted, or the
   network path from standby to primary broken.

### Likely causes

- The subscription was disabled (often by a `DISABLE` during maintenance that
  was never re-enabled).
- The apply worker is failing permanently: a schema mismatch after a migration
  was applied to the primary but not the standby (logical replication carries
  no DDL).
- The standby host is down, out of disk, or unable to reach the primary.
- The slot was created and then never consumed — a half-finished DR setup.
- The `pg_monitor` grant was revoked, so the views read as unavailable. This
  looks like `0` standbys and is a configuration problem, not an outage.

### False positives

- A deliberate maintenance window on the standby. Expected, and still worth the
  page: the RPO really is unbounded for its duration.
- A single sampler interval during a standby restart. The `for: 2m` window
  covers a normal restart.
- A deployment that never configured DR at all. If a shard has no standby by
  design, silence this alert for that shard explicitly rather than letting it
  become background noise everyone ignores.

### Safe actions

- Re-enable the subscription: `ALTER SUBSCRIPTION <name> ENABLE;` This is the
  fix for the most common cause (a maintenance `DISABLE` that was never undone)
  and it costs seconds.
- Apply any missing migrations to the standby, then re-enable.
- Rebuild the standby from a fresh base backup or a fresh `copy_data = true`
  subscription.

### Destructive — last resort only

`SELECT pg_drop_replication_slot('<name>');` is **irreversible** and is not a
safe action. Dropping a slot ends DR for that shard until the standby is
rebuilt from scratch, and converts a five-second `ALTER SUBSCRIPTION ... ENABLE`
into a multi-hour re-seed if the standby was merely disabled.

Preconditions, all of them:

1. You have confirmed the slot's `database` matches the affected shard. An
   unscoped listing will show sibling shards' slots, and destroying one of those
   ends a **healthy** shard's DR with no way to repair it short of a rebuild.
2. You have established the standby is genuinely unrecoverable, not disabled.
3. You have accepted that this shard has no DR until it is rebuilt, and said so
   in the incident channel.

The one situation that justifies it: retained WAL is about to exhaust the
primary's disk. A primary that runs out of WAL space is a full outage, which is
worse than a lost safety net — but that is a trade to make deliberately, not a
"safe action".

### Escalation criteria

Escalate to the platform team owning the DR topology when the backlog passes
the primary's WAL headroom, when the standby cannot be recovered in place, or
when this shard is under an RPO commitment and DR has been down long enough to
breach it. If a regional failure occurs while this is firing, the failover will
proceed with an **unmeasured** loss — say so in the incident channel rather
than recording "RPO: 0", and follow `docs/runbooks/cross-region-failover.md`.

## harvest_replication_lag_high

**What to do when the measured RPO is growing:** the standby is falling behind,
so the amount of acknowledged work a failover would lose is growing with it.
The number is literal: at failover, up to `lag` seconds of confirmed work is
gone, and per Harvest's at-least-once contract, side effects from that window
may re-execute on the new primary.

A large `harvest.replication.lag_seconds` next to a `NULL`
`pg_stat_replication.replay_lag` is the signature of a **stuck apply worker**.
That combination is not a contradiction: `replay_lag` is computed from the
subscriber's reply messages, so a subscriber whose apply worker is blocked stops
replying and the column goes blind, while Harvest's watermark trail — computed
on the primary from a position the standby confirmed — keeps measuring.

### Triage steps

1. `harvest dr status --shard <id>=<dsn> -o json` for the per-shard RPO and
   byte backlog.
2. On the primary: `SELECT application_name, state, replay_lag FROM
   pg_stat_replication;` Compare with step 1 — a `NULL` here beside a large RPO
   means apply is stuck, not slow.
3. On the standby, look for a long-running transaction or a lock the apply
   worker is waiting on: `SELECT pid, state, wait_event_type, wait_event, query
   FROM pg_stat_activity WHERE backend_type = 'client backend';` A reporting
   query holding a lock on a replicated table blocks apply completely. The
   `backend_type` filter matters before you terminate anything: the logical
   apply worker and the walreceiver are in that view too, and killing them is
   the opposite of the fix.
4. Check standby I/O and CPU. A standby on smaller hardware than its primary
   falls behind under write bursts and never catches up.

### Likely causes

- A long-running query or an idle-in-transaction session on the standby holding
  a lock the apply worker needs.
- The standby is under-provisioned relative to the primary's write rate.
- A large bulk write on the primary (a backfill, a retention sweep, a batch
  start) that the single-threaded logical apply worker cannot keep pace with.
- Network throughput between regions, especially with a burst of large payloads.
- The sampler interval is set very high, so the reported number is coarse. The
  RPO's resolution floor is one sampler interval.

### False positives

- A brief spike during a known bulk operation. Correlate with
  `harvest_workflow_started_total` and your own backfill schedule.
- A value between zero and one sampler interval on a healthy system. That is
  the resolution floor, not lag.
- A generation-skew alert firing *during* a planned failover. Skew is expected
  while shards are being promoted; it is only a problem if it persists after
  the failover is complete.

### Safe actions

- Terminate the blocking session on the standby:
  `SELECT pg_terminate_backend(<pid>);`
- Reduce write pressure: pause a backfill, lower a batch-start rate, or defer a
  retention sweep.
- Raise the standby's resources.
- Do **not** "fix" the alert by widening the threshold. The threshold should be
  the RPO the business agreed to accept; if the real lag exceeds it, the
  exposure is real.

### Escalation criteria

Escalate when the measured RPO exceeds the documented RPO budget for more than
one business hour, when it is rising monotonically with no identified cause, or
when a failover is being considered while it is elevated — the RPO reported at
that moment is the loss being accepted, and that is a decision for the service
owner, not for on-call.

## harvest_shard_fenced

**What to do when a worker was fenced:** the shard's write-authority generation
moved past the epoch that worker pinned at startup, so another region holds
write authority now. The worker stopped rather than appending to a history it no
longer owns. This is the cross-region DR fence working exactly as designed —
the question is only whether the fence was intentional.

### Triage steps

1. Is a failover in progress? If yes, this is expected for the **old** fleet and
   the remaining work is `docs/runbooks/cross-region-failover.md` step 4:
   restart workers against the region that now holds authority.
2. If no: `SELECT shard_id, generation, fenced_at, fenced_by, fenced_reason
   FROM harvest_shard_generation;` The row records who bumped it and why.
3. `harvest dr status --shard <id>=<dsn> -o json` across every shard. Compare
   generations: if they differ, some shards were fenced and some were not,
   which is a half-failed-over cluster.
4. `harvest worker health --output json` — expect the fenced workers to be
   absent or stale. They stopped; they are not slow.

### Likely causes

- A planned failover, with the old fleet still running.
- A `harvest dr fence` run against the wrong shard, the wrong region, or during
  a healthy week.
- A worker restarted against the **old** region after a failover, pinning an
  epoch that was already superseded.
- A fail-back where the old region was re-seeded (correctly) from the new
  primary, bringing the newer epoch with it, while a surviving worker there was
  never restarted.

### False positives

None. A fence is always a real loss of write authority. It can be *expected* —
during a failover it is the intended outcome — but it is never spurious, and
the count never decays on its own.

### Safe actions

- Restart the fenced workers against the region that currently holds authority,
  with `dr_fencing` still enabled. They pin the current epoch at startup.
- If the fence was a mistake, the recovery is still to **restart the fleet**.
  Generations only go up; there is no un-bump, and bumping again does not undo
  anything — it fences the fleet a second time.
- **Never** re-pin or adopt the new epoch in a running worker. That would
  re-admit a worker the promoted region just evicted, which is precisely the
  split-brain the epoch exists to prevent.
- Verify no fenced worker wrote anything: a fenced claim burns no attempt and a
  fenced append writes nothing, so its tasks should still be `PENDING` and
  claimable by the region that holds authority.

### Escalation criteria

Escalate immediately when no failover was planned — an unexplained generation
bump means someone has write authority you did not grant. Escalate when
generations are skewed across shards outside a failover window, and when fenced
workers keep reappearing after restart (they are being pointed at a region that
no longer holds authority — fix the DSN or DNS, not the worker).

## harvest_replication_unobservable

**What to do when a shard's replication views cannot be read:** the RPO and
standby count for that shard are **unknown** — not zero, and not evidence that
replication is down. Replication itself may be perfectly healthy; what has been
lost is the ability to see it. Almost always a missing `GRANT pg_monitor` on
the role Harvest connects as.

This is a ticket rather than a page for that reason, but it is not cosmetic. A
Prometheus gauge keeps exporting its last value until something changes it, so
while `harvest.replication.observable` is `0` the RPO, standby-count and
backlog panels are **frozen at their last healthy reading** rather than
reflecting reality. Harvest deliberately withholds those gauges instead of
publishing zeros — a zero standby count would page on-call for a permissions
problem — and this signal is what tells a dashboard that the silence means
"cannot see", not "nothing to report".

`harvest_replication_down` is gated on `observable == 1`, so the two rules are
mutually exclusive and neither can fire on a stale reading.

### Triage steps

1. `harvest dr status --shard <id>=<dsn> -o json`. The affected shard reports
   `unreadable` for standbys and carries an explicit `replication_error`.
2. Read that error. `permission denied for view pg_stat_replication` is the
   common case.
3. Confirm the role's membership:
   `SELECT pg_has_role(current_user, 'pg_monitor', 'member');`
4. If the grant is present, check whether the DSN points where you think — a
   shard pointed at a replica or a pooler that rewrites the session can produce
   the same symptom.

### Likely causes

- `GRANT pg_monitor TO harvest` was never run, or was lost when the role was
  recreated during a migration or a restore.
- The deployment connects through a connection pooler in a mode that does not
  preserve the expected role.
- A managed-Postgres provider that restricts `pg_stat_replication` to its own
  monitoring role.
- The DSN was repointed at a database whose role differs from the primary's.

### False positives

- A single tick during a role change or a failover, where the connection is
  re-established as a different role. The `for: 10m` window covers that.
- A deployment that has not enabled DR at all but did enable `dr_fencing`: the
  sampler runs and finds nothing to read. Silence this shard explicitly rather
  than letting it become background noise.

### Safe actions

- `GRANT pg_monitor TO <harvest role>;` — the fix in the overwhelming majority
  of cases, and it takes effect on the next connection.
- Repoint the DSN if the shard is aimed at the wrong database.
- Nothing here touches replication itself; all of it is read-permission
  configuration.

### Escalation criteria

Escalate when this shard is under an RPO commitment and the grant cannot be
made (a managed provider that will not expose the views): the commitment cannot
be *measured*, which is a contractual problem rather than an operational one,
and it should be recorded as accepted risk rather than left firing. Escalate
immediately if a failover is being considered while this is firing — the RPO
you would be accepting is unmeasured, and that must be said out loud in the
incident channel rather than recorded as zero.

---

## harvest_replication_rpo_unknown

**What to do when a shard's RPO has no source.** The replication views are
readable and a standby is connected, but no signal can produce an RPO number
yet: no DR slot has confirmed a position, and the standby has not reported a
`replay_lag` either. This differs from `harvest_replication_unobservable`: the
views are not the problem here, the RPO itself has no source.

`harvest.replication.lag_seconds` is withheld rather than published as a stale
or fabricated number. A Prometheus gauge keeps exporting its last value, so
withholding it alone does not make the panel stale — it freezes at the last
healthy reading. `harvest.replication.rpo_known` is the signal that breaks
that freeze: it is emitted every tick the views are readable, `0` included.

### Triage steps

1. `harvest dr status --shard <id>=<dsn> -o json`. Read the standby and slot
   list for the affected shard.
2. Confirm a DR slot exists and carries the configured prefix
   (`replication_slot_prefix`, default `harvest_dr`). A standby attached
   without one cannot report a watermark.
3. Check how long ago the standby connected. A `replay_lag` of `NULL` is
   normal until the first feedback round trip completes; give it one sampler
   interval before treating it as stuck.

### Likely causes

- A physical standby attached to the primary without a `primary_slot_name`,
  so no slot exists for the sampler to read a position from.
- A freshly created DR slot with no watermark beat written yet (the sampler
  writes at most one beat per shard per interval).
- A standby that connected moments ago and has not completed a feedback round
  trip.

### False positives

- The first sampler tick after a new standby connects. The `for: 10m` window
  covers ordinary startup.

### Safe actions

- Create the missing replication slot and reattach the standby with
  `primary_slot_name` set, per `docs/cross-region-dr.md`.
- Wait one sampler interval past standby connection before escalating.

### Escalation criteria

Escalate if this fires for longer than the standby's expected catch-up time,
or if a failover is being considered while this is firing — the RPO for this
shard is unmeasured, and that must be said out loud in the incident channel
rather than assumed to be small.

---

## harvest_audit_export_lag_high

`harvest.audit.export_lag` is the age of the **oldest** audit record the SIEM
sink has not acknowledged (issue #953; see `docs/audit-export.md`). A rising
line does **not** mean records are being lost — the export cursor is held
rather than advanced past a failure, and the retention sweep refuses to purge
an unexported record — but the window in which a privileged action would be
invisible to detection is growing, and the audit table is growing with it.

### Triage steps

1. Ask the exporter about itself:

   ```bash
   curl -s "$HARVEST/admin/audit-export" | jq '{sink_configured, status, shards}'
   ```

2. Read `delivery_state` and `last_error` on the lagging shard. Three states
   are worth telling apart:

   | What you see | What it means |
   |---|---|
   | `last_error` populated, `delivery_state: "BACKOFF"` | The sink is rejecting or unreachable. The cursor is parked, retrying with capped backoff. |
   | `delivery_state: "NOT_STARTED"` that persists across reads | No exporter has ever ticked this shard. Either export is configured nowhere, or it is configured only on a fleet that is not reaching this shard. |
   | `delivery_state: "RETIRED"` | An operator ran `POST /admin/audit-export/decommission` here. No exporter owes this shard records and retention may purge them — this is a deliberate state, not a fault. |
   | No `harvest_audit_export_lag` series at all | No exporter is running for that shard. **Worse than a high value**, and a threshold alert cannot see it. |

3. Check `pending_records` on the same response to size the backlog, and
   whether it is still growing between two reads.

   **Do not diagnose from `sink_configured`.** It reports only whether the
   process that served your request has a sink installed. In a split
   web/worker deployment the API process legitimately reports `false` while
   export is healthy on the worker fleet, and `true` there would not tell you
   the *worker* is configured. Use the database-backed signals instead —
   `pending_records` growing across two reads, `lag_seconds` rising, or a
   persistent `NOT_STARTED` — which describe the shard rather than whichever
   process answered.

### Likely causes

- The sink endpoint is down, rejecting (non-2xx), or answering with a redirect
  — redirects are never followed, so a 3xx is a failure by design.
- The HMAC secret was rotated on the receiver but not in the Harvest build, so
  every batch is rejected as unauthenticated.
- Audit export was configured on the web application but not on the worker
  build; the scanner that exports lives on the workers.
- A sink slower than the configured claim lease
  (`HarvestBuilder::audit_export_lease`), so every attempt is superseded before
  it lands. `last_error` names the lease timeout when this is the cause.
- A shard whose database has been unreachable to the exporter, so its cursor
  row was never provisioned.

### False positives

- A brief spike right after a deploy, while a restarted worker re-claims shards
  whose leases had not yet expired. It clears within one lease period.
- A backfill or bulk administrative operation that writes a burst of audit rows
  faster than one batch per scanner tick drains them. Lag rises and then falls;
  `harvest.audit.exported` stays non-zero throughout, which distinguishes it
  from a stall.
- A shard with genuinely no audited traffic reports `0`, not a missing series —
  do not read `0` as "broken".

### Safe actions

- Fix the sink. Recovery is automatic and needs no operator action: the cursor
  resumes from where it stopped and nothing was skipped.
- Configure `audit_export_*` on the worker build if `sink_configured` is
  `false` there.
- Raise `audit_export_lease` above the sink's own request timeout if
  `last_error` reports the lease timeout.
- The starter pack ships `absent(harvest_audit_export_lag)` as a companion rule
  (`harvest_audit_export_absent`) — the threshold rule structurally cannot fire
  when no exporter is running anywhere, because the gauge then has no series
  and `max(...)` evaluates over an empty vector.

  ⚠️ **`absent()` alone is not sufficient once a shard has reported.** The
  gauge is only written on a successful observation: if the exporter cannot
  read the cursor — a failing query, a connection it cannot acquire — the
  recorder keeps serving the last value it was given, commonly `0`. Prometheus
  then sees neither a high value nor an absent series while that shard is going
  unexported.

  `time() - timestamp(...)` does not provide a fix, either: Prometheus stamps
  each *scraped sample* with the scrape time, and the endpoint keeps exposing
  the stale value on every scrape, so such an expression stays near the
  scrape interval forever. Do not rely on it. `changes(...) == 0` fails the
  same way for the opposite reason — a healthy caught-up shard also holds a
  constant `0`.

  `harvest_audit_export_unobservable` (below) is the metric-only fix for this
  exact case (issue #1268): `harvest.audit.export_observed` is emitted every
  tick that reaches a shard, success or failure, so it cannot go stale the
  way the lag gauge can.

  What each alert actually covers:

  | Condition | Detected by |
  |---|---|
  | Process down | `up == 0` |
  | Exporter never ran for a shard | `absent(harvest_audit_export_lag{shard="N"})` |
  | Sink failing or slow, exporter observing normally | the lag threshold — the cursor is held, so the oldest unacknowledged record ages and the gauge climbs |
  | Exporter alive but **cannot observe the shard** | `harvest_audit_export_unobservable` — see the runbook section below |
  | **One shard** never scanned while others report | **nothing in the shipped rules.** `absent()` is false as soon as any shard reports. Template one absence rule per configured shard from your own inventory: `absent(harvest_audit_export_lag{shard="N"})` |

  The last row remains an open gap: it names a shard missing from a worker's
  `shard_assignments` altogether, which no exporter tick ever touches, so no
  series — lag or observed — exists for it either. That is a per-deployment
  inventory question the starter pack cannot answer generically. Note that
  the common outage — a sink that is down or rejecting — falls in the
  *third* row and is covered: the exporter still reads the cursor fine and
  the gauge climbs as designed.

**Do not reach for the redrive.** `POST /admin/audit-export/redrive` rewinds
the cursor so already-delivered records are re-exported; it is for **sink-side
data loss** (your SIEM lost a day and you need it back). It does nothing for a
stalled sink except queue more work behind the stall, and a cursor can only
ever move backwards, so it cannot be used to skip past a stuck batch. There is
no supported way to skip an audit record — that is the point of the feature.

### Escalation criteria

- Lag exceeds the window your compliance posture can tolerate privileged-action
  logs being absent from the SIEM. That number is a policy decision, not a
  Harvest default; write it down before the incident.
- `pending_records` grows without bound and the audit table is approaching its
  volume budget. Retention will not purge unexported records for as long as
  export is behind — that is the deliberate trade.

  **Disabling the sink is not enough to restore purging.** The guard keys on
  the shard's cursor row, not on whether a sink happens to be configured in the
  process running retention — deliberately, so that a worker outage cannot let
  a web process delete the records that outage stranded. Relieving the disk
  pressure takes two steps: stop the exporter, then explicitly retire the
  cursor:

  ```bash
  curl -X POST https://app.example.com/api/harvest/admin/audit-export/decommission \
    -H 'Content-Type: application/json' \
    -d '{"shard": <shard_id>}'
  ```

  The next retention tick then purges that shard's aged rows normally.

  This permanently gives up the un-exported window: those records will never
  reach the SIEM. Treat it as a decision with a security/compliance sign-off,
  not a cleanup step — the route writes its own audit record
  (`audit_export.decommission`), so the paper trail is automatic. It is
  reversible in the sense that `POST /admin/audit-export/reactivate` later
  continues the sequence correctly (the cursor's high-water mark survives
  the purge) — but the records purged in between are gone.
- Escalate to the security/compliance owner, not only the platform team: the
  question "were privileged actions logged during this window?" is theirs to
  answer.

### Confirming recovery

Lag returns to ~0 and `harvest.audit.exported` resumes. On the receiver, check
`(shard, seq)` contiguity across the outage window: sequences are dense per
shard, so a hole is a real gap and a duplicate is the expected at-least-once
behaviour.

---

## harvest_audit_export_unobservable

**What to do when a shard's audit-export cursor cannot be read:** the
exporter is running, but this tick could not read the shard's cursor row, or
could not compute its lag, or could not even acquire a connection to the
shard (issue #1268). `harvest.audit.export_lag` is not a reliable signal
here — a Prometheus gauge keeps its last value, so the lag reading for this
shard is frozen, commonly at `0`, and looks healthy.

This is a ticket rather than a page: the sink may be working fine, and
records already sequenced are not lost — the cursor is simply not advancing
because the exporter cannot currently reach this shard to advance it.

### Triage steps

1. `curl -s "$HARVEST/admin/audit-export" | jq '.shards[] | select(.shard == <id>)'`.
2. Read `last_error` on that shard. A connection-acquisition failure logs
   `[audit_export] failed to get connection to shard ...` or `timed out
   acquiring a connection for this shard` at `tracing::error!` level; search
   the worker logs for the shard id around the alert's firing window.
3. Confirm the shard's own database is reachable from the worker: the audit
   exporter uses the same `ShardedDbPool` as every other per-shard resident
   of `enforce_timeouts_once`, so a shard unreachable here is usually
   unreachable for claim/timeout processing too.
4. Check the shard's connection pool size. A pool with `max_size` of `1`
   cannot yield a second connection while the scanner already holds the
   first; see `SHARD_ACQUIRE_BOUND` in `audit_export.rs`.

### Likely causes

- The shard's database is down, unreachable over the network, or rejecting
  new connections.
- The shard's connection pool is undersized for the number of per-shard
  scanner residents sharing it.
- The shard is assigned to this worker but was never given a pool entry — a
  configuration mismatch between `shard_assignments` and the
  `ShardedDbPool`.
- A transient cursor-row read failure or lag-query failure under database
  load; this self-heals on the next tick without operator action.

### False positives

- A single tick during a brief connection blip. The `for: 10m` window
  covers that; watch for the gauge returning to `1` on its own.
- A deploy that briefly saturates a shard's pool while workers roll. It
  clears within one or two poll intervals.

### Safe actions

- Fix the underlying connectivity or pool-sizing problem; recovery is
  automatic once the shard is reachable again, and nothing was skipped —
  the cursor resumes from where it stopped.
- Widen the shard's connection pool if `SHARD_ACQUIRE_BOUND` timeouts recur
  under normal load.
- Nothing here calls for a redrive: no record was marked delivered that was
  not, so there is nothing to rewind.

### Escalation criteria

Escalate when the shard stays unobservable past the compliance window your
posture allows for privileged-action logs to sit unconfirmed, or when the
underlying database outage is itself a page-worthy incident. Escalate to
the team owning that shard's database first; this signal names an
availability problem with the shard, not with the SIEM sink.
