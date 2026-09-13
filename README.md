# autumn-harvest

[![Crates.io](https://img.shields.io/crates/v/autumn-harvest.svg)](https://crates.io/crates/autumn-harvest)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](#license)
[![CI](https://github.com/autumn-foundation/autumn-harvest/actions/workflows/ci.yml/badge.svg)](https://github.com/autumn-foundation/autumn-harvest/actions/workflows/ci.yml)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/autumn-foundation/autumn-harvest)

Postgres-backed durable workflow engine for Rust, designed as a companion to the
[Autumn](https://github.com/autumn-foundation/autumn) web framework. Provides
event-sourced workflow execution with activities, signals, timers, child
workflows, and DAG scheduling — Temporal-style durability semantics with a
single-Postgres operational footprint.

## Why

Most Rust async work is fire-and-forget. autumn-harvest is for the work that
*can't* be: long-running orchestrations that survive process restarts, retries
with durable history, signal-driven waits, queryable state, and scheduled DAGs.
If you've reached for Temporal, Cadence, or Inngest from a Rust service, this is
the same shape with one fewer service to operate.

Weighing harvest against Temporal, DBOS, Inngest, Hatchet, or Restate? See the
honest, evidence-linked [comparison page](docs/comparison.md) — it names
harvest's own gaps, not just its strengths.

Moving an existing Temporal deployment? See the
[Temporal migration guide](docs/migrating-from-temporal.md) — a concept
map, a workflow-porting checklist, and a dual-run cutover playbook.

## Quick example

Try it end-to-end with **no database, no Docker, and nothing to configure**:

```bash
cargo dev
```

That starts an ephemeral PostgreSQL, applies the migrations, runs a worker, and
serves the management API and the Vantage dashboard — then prints the dashboard
URL and one `curl` that starts a durable workflow. `Ctrl-C` reclaims everything
it created. It is development-only and refuses to point at anything that is not
a local database — and, when provisioning its own cluster, it refuses to run as `root`
too, since PostgreSQL itself won't; a real situation for Docker devcontainers
and many CI images, not a hypothetical. That check is scoped to provisioning
though: point it at a database you already have
(`HARVEST_DEV_DATABASE_URL`) and it runs fine as root. See
[Chapter 1](docs/getting-started/01-project-skeleton.md).

Running as root with nothing provisioned yet, or would rather bring your own
Postgres? `docker compose -f examples/quickstart/compose.yaml up -d`, then
`AUTUMN_MANIFEST_DIR=examples/quickstart AUTUMN_PROFILE=dev cargo run -p
quickstart` (see [`examples/quickstart/`](examples/quickstart/)) — Postgres
runs in its own container there, so root has no bearing on it.

For a chapter-by-chapter walkthrough — first workflow, durable timers, signals,
child workflows, idempotency, and operating the service — read
[`docs/getting-started/`](docs/getting-started/).

Upgrading an existing deployment? See the
[0.5.0 → 0.6.0 upgrade guide](docs/upgrading/0.6.0.md) — the previous
[0.4.0 → 0.5.0 upgrade guide](docs/upgrading/0.5.0.md) covers the hop before that.

Working on the engine itself? [`docs/architecture.md`](docs/architecture.md) is
the workspace, design-decision, module and macro reference, and
[`docs/shipped-work.md`](docs/shipped-work.md) is the verbatim record of what
has shipped.

Need a real reference instead of the tiny hello-world path? See:

- [`examples/billing-autumn-web/`](examples/billing-autumn-web/) for a full Autumn web billing
  integration with app routes, workflow outbox publication, `HarvestPlugin`, saga compensation,
  child workflows, version gates, signals, timers, deterministic side effects, and scheduled DAGs.
- [`examples/standalone-runner/`](examples/standalone-runner/) for the out-of-the-box runner path:
  no Autumn plugin, just `HarvestRunner` plus a manually mounted management API router.
- [`examples/claude-agent-daemon/`](examples/claude-agent-daemon/) for a local daemon that runs
  Claude agent sessions as durable workflows on the embedded SQLite backend — no Postgres, no
  Docker. Each model call and tool call is an activity, a workspace write parks on an
  approval signal with a deadline, and killing the daemon mid-session resumes by replay instead
  of paying for completed turns twice. It runs with no API key (a scripted offline model).

```rust
use autumn_harvest::prelude::*;

#[workflow]
async fn onboarding(ctx: &WorkflowContext, user_id: i64) -> HarvestResult<()> {
    ctx.execute_activity_raw(
        "send_welcome_email",
        serde_json::json!({ "user_id": user_id }),
        "default",
    )
    .await?;
    Ok(())
}

#[activity(start_to_close = "30s", retry = RetryPolicy::exponential(3, std::time::Duration::from_secs(1)))]
async fn send_welcome_email(_ctx: &ActivityContext, input: serde_json::Value)
    -> HarvestResult<serde_json::Value>
{
    // … real I/O. Failure here is retried per the policy above.
    Ok(serde_json::json!({ "sent": true }))
}
```

Wired into an Autumn app via the plugin:

```rust
use autumn_web::prelude::*;
use autumn_harvest_plugin::HarvestPlugin;

#[autumn_web::main]
async fn main() {
    autumn_web::app()
        .plugin(
            HarvestPlugin::new()
                .workflows(workflows![onboarding])
                .activities(activities![send_welcome_email])
                .api("/api/harvest"),
        )
        .run()
        .await;
}
```

## Request/response embedding

For short workflows behind an HTTP route, the plugin installs a shard-aware
`WorkflowHandleClient` into `AppState`. Start the workflow through the normal
storage pool, keep the returned handle, and await the terminal JSON result:

```rust
let started = workflow_handle_client
    .start_or_load(&mut conn, start_params)
    .await?;

let result = started.handle.result_raw().await?;
```

`result_raw()` blocks on Postgres LISTEN/NOTIFY wakeups from the event log, not
sleep polling. Successful workflows return their `output`; failed, cancelled,
timed-out, and terminated workflows return typed `HarvestError` variants.

Avoid the old polling anti-pattern of repeatedly calling
`GET /api/harvest/workflows/{id}` and scanning the returned full event history
for terminal events. That endpoint is for inspection. For HTTP clients that
already know an execution ID, use
`GET /api/harvest/workflows/{id}/result?wait=5s` instead. It returns only the
compact terminal body with `state`, `output`/`error`, and `completed_at`, or
`204 No Content` with `Retry-After` while the workflow is still running.

## Communicating with running workflows

Harvest supports two styles for registering query and update handlers on a
workflow. The **declarative style** (recommended) uses proc-macro attributes and
bang macros; the **imperative style** is supported for advanced cases such as
dynamic handler sets determined at runtime.

### Declarative style (recommended)

Annotate free-standing functions and register them on the builder:

```rust
use autumn_harvest::prelude::*;

#[update(workflow = "approval_workflow", validator = validate_decision)]
pub fn decide(ctx: &WorkflowContext, input: DecisionInput) -> Result<(), String> { /* … */ }

#[query(workflow = "approval_workflow")]
pub fn approval_status(ctx: &WorkflowContext) -> Result<StatusResponse, String> { /* … */ }

HarvestBuilder::new()
    .workflows(workflows![approval_workflow])
    .updates(updates![decide])
    .queries(queries![approval_status]);
```

See [`autumn-harvest/examples/approval_workflow.rs`](autumn-harvest/examples/approval_workflow.rs)
for a complete working example including a validator and `drain_admitted_updates`.

### Imperative style (supported for advanced cases)

Use `ctx.register_update_handler` / `ctx.register_query_handler` directly inside
the workflow body when handler sets are determined at runtime:

```rust
ctx.register_update_handler("decide", Some(validate_decision), |ctx, input: DecisionInput| {
    /* … */
});
ctx.register_query_handler("status", |_req: &()| Ok("running"));
```

### Handler discovery via management API

To enumerate all registered query and update handlers for a workflow type:

```
GET /api/harvest/workflows/types/{workflow_name}/handlers
```

Response shape:

```json
{
  "workflow": "approval_workflow",
  "queries": [{ "name": "approval_status", "input_type_hint": "…", "output_type_hint": "…" }],
  "updates": [{ "name": "decide", "input_type_hint": "…", "output_type_hint": "…", "has_validator": true }]
}
```

## Long-running workflows

Every workflow replay loads its durable event history. For pollers, monitors,
and other workflows that may run for weeks, keep that history bounded by
tail-calling `continue_as_new` once the loaded event count grows past the
configured soft threshold:

```rust
use autumn_harvest::prelude::*;

#[workflow]
async fn polling_loop(ctx: &WorkflowContext, mut state: serde_json::Value)
    -> HarvestResult<serde_json::Value>
{
    loop {
        let cycle = state["cycle"].as_u64().unwrap_or(0);
        let result = ctx
            .execute_activity_raw("poll_remote_system", state.clone(), "pollers")
            .await?;

        if result["done"].as_bool().unwrap_or(false) {
            return Ok(result);
        }

        let next_state = serde_json::json!({
            "cycle": cycle + 1,
            "cursor": result.get("next_cursor").cloned(),
        });

        if ctx.should_continue_as_new() {
            ctx.continue_as_new(next_state).await?;
        }

        let timer_id = format!("poll-delay-{cycle}");
        ctx.timer(&timer_id, 60).await?;
        state = next_state;
    }
}
```

`ctx.history_event_count()` reports the number of loaded durable history events.
`ctx.should_continue_as_new()` turns true once that count exceeds the
`HarvestBuilder::history_continue_as_new_threshold(...)` soft threshold. The
default threshold is `10_000` events.

```rust
let harvest = HarvestBuilder::new()
    .workflows(workflows![polling_loop])
    .history_continue_as_new_threshold(5_000)
    .history_event_hard_cap(20_000)
    .try_build()?;
```

The optional hard cap is a last-resort guardrail. If an execution reaches
`history_event_hard_cap` and the workflow does not issue `continue_as_new`, the
worker fails the execution and moves it to the DLQ with a typed
`HistoryCapExceeded { count, cap, workflow_type }` reason. No new workflow event
variant is used for this guardrail.

Harvest emits `harvest.workflow.history_size` for terminal executions and
`harvest.workflow.continue_as_new` when a workflow rotates. Both metrics use
only the workflow type label to keep cardinality boring in the useful way.

See [`autumn-harvest/examples/long_running.rs`](autumn-harvest/examples/long_running.rs)
for a compile-checked polling loop that works with and without the `db` feature.

## What you get

- **Event-sourced execution.** Workflows are deterministic functions; their
  history is a Postgres event log. Restart the process, replay the history, end
  up at the same state.
- **Activities with retries.** Side effects live in `#[activity]` functions
  with configurable `start_to_close`, `heartbeat_timeout`, and `retry` policies.
  Activities are **at-least-once**: a worker crash or timeout will trigger a
  retry. Use `ctx.idempotency_key()` (see below) to make downstream calls safe
  to retry without duplicate charges or duplicate emails.
- **Per-activity concurrency caps.** Declare `max_concurrent = N` on an
  activity to enforce a cluster-wide cap without spinning up dedicated worker
  processes. Activities sharing a rate-limited dependency can share a budget
  via `concurrency_key = "stripe"`.
- **Signals & queries.** Send a signal into a running workflow, query its
  state, or block on a timer.
- **Child workflows.** Compose orchestrations from smaller workflows and model
  recovery paths in normal workflow code.
- **Workflow cron schedules.** Register any workflow on a cron/interval
  expression with one builder call — no DAG wrapper required. Choose
  `WorkflowSchedule` when you want "run this workflow on a schedule";
  choose `DagBuilder` when you need dependency-graph fan-out and trigger
  rules between tasks.
- **DAG scheduling.** Declare DAGs of activities with trigger rules and
  cron/interval schedules; built-in scheduler dispatches them.
- **Management API.** Optional HTTP surface for inspecting executions, sending
  signals, querying state, triggering DAG runs, and managing dead letters.
- **SKIP LOCKED task queue + LISTEN/NOTIFY** for low-latency dispatch without
  polling backoff.
- **Dead letter queue** for tasks that exhaust their retry policy, with
  management endpoints to inspect and replay entries.
- **Retention janitor** (opt-in) to prune completed workflow histories older
  than a configured max age, with status and run-now controls on the admin API.
  Running workflows are unaffected because only terminal-state executions with
  `completed_at` older than the retention window are eligible.
- **Separate worker/web connection pools** with a shared ceiling so worker
  bursts can't starve HTTP request handling.

## Workspace

| Crate | Purpose |
|-------|---------|
| [`autumn-harvest`](autumn-harvest/) | Core engine — types, executor, replay, queue, worker runtime |
| [`autumn-harvest-plugin`](autumn-harvest-plugin/) | `HarvestPlugin` — wires the engine into an Autumn `AppBuilder`, mounts the management API, owns the runtime lifecycle |
| [`autumn-harvest-macros`](autumn-harvest-macros/) | `#[workflow]`, `#[activity]`, `#[dag]`, `workflows![]`, `activities![]` proc macros |
| [`autumn-harvest-cli`](autumn-harvest-cli/) | `harvest` CLI: thin operator client for the management API |
| [`autumn-harvest-redis`](autumn-harvest-redis/) | Optional Redis Streams dispatch channel — carries references to claimable rows; Postgres stays the source of truth |
| [`autumn-harvest-sqlite`](autumn-harvest-sqlite/) | Optional SQLite storage backend for single-process and embedded deployments |

Use `autumn-harvest-plugin` if you're building an Autumn app. Use the bare
`autumn-harvest` crate if you want to embed the engine in another framework or
a non-web context.

## CLI

The `harvest` binary is a thin HTTP client for the optional management API, so
workflow queries, DAG triggers, auth, and runtime-owned behavior stay behind the
same API surface your service exposes. A few commands take a database DSN
instead, because they exist for the moments when no app is up to ask:
[`harvest migrate`](#migrating-a-dedicated-harvest-database) (the schema step
before replicas roll) and `harvest backup verify` (a restore drill against
scratch databases).

```bash
cargo run -p autumn-harvest-cli -- health
cargo run -p autumn-harvest-cli -- preflight
cargo run -p autumn-harvest-cli -- workflow list --limit 25
cargo run -p autumn-harvest-cli -- workflow list --state RUNNING --search-attr tenant=acme
cargo run -p autumn-harvest-cli -- workflow get <execution-id>
cargo run -p autumn-harvest-cli -- workflow start approval_workflow --input-json '{"request_id":"42"}'
cargo run -p autumn-harvest-cli -- workflow signal <execution-id> approved --payload-json '{"approved":true}'
cargo run -p autumn-harvest-cli -- workflow query <execution-id> status
cargo run -p autumn-harvest-cli -- workflow cancel <execution-id> --reason "operator request"
cargo run -p autumn-harvest-cli -- workflow stack <execution-id>
cargo run -p autumn-harvest-cli -- workflow children <execution-id> --status Failed
cargo run -p autumn-harvest-cli -- workflow reset <execution-id> --to-event 42 --reason "bad deploy recovery" --operator-id mark
cargo run -p autumn-harvest-cli -- dag list
cargo run -p autumn-harvest-cli -- dag trigger daily_pipeline --conf-json '{"date":"2026-04-21"}'
cargo run -p autumn-harvest-cli -- dag pause daily_pipeline
cargo run -p autumn-harvest-cli -- dlq list --limit 25
cargo run -p autumn-harvest-cli -- dlq replay <dead-letter-id>
cargo run -p autumn-harvest-cli -- dlq bulk-replay --activity-name send_email --dry-run
cargo run -p autumn-harvest-cli -- dlq bulk-replay --activity-name send_email
cargo run -p autumn-harvest-cli -- dlq bulk-discard --activity-name send_email --failed-before 2026-04-27T00:00:00Z
cargo run -p autumn-harvest-cli -- retention status
cargo run -p autumn-harvest-cli -- retention run-now
cargo run -p autumn-harvest-cli -- concurrency status
```

Configure the API mount with `--base-url` or `HARVEST_URL` (default:
`http://localhost:3000/api/harvest`). Pass `--token` or `HARVEST_TOKEN` to send
a bearer token. Successful responses are printed as pretty JSON by default; use
`--output json` for compact script-friendly output. JSON request payloads accept
inline `--*-json` values or `--*-file PATH`; use `-` as the file path to read
from stdin.

### Migrating a dedicated Harvest database

In the default `harvest.mode = "embedded"`, Harvest's migrations are Autumn's:
they are registered with the framework and applied by `autumn migrate` (or
automatically under the `dev` profile). Under `harvest.mode = "split"` or
`"external"`, Harvest storage is a database Autumn has no handle on — that one
is `harvest migrate`'s job:

```bash
export HARVEST_DATABASE_URL=postgres://…   # == harvest.database.url

harvest migrate status          # what is applied, what is pending
harvest migrate status --check  # same, but exits non-zero while anything is pending
                                # (give it the same --include-dir you give `run`,
                                #  or it gates on fewer sets than you apply)
harvest migrate run             # apply every pending migration, in version order
harvest migrate run --dry-run   # print the plan, change nothing

# Sets the binary does not embed — the plugin's connector dead-letter table,
# or your application's own — ride along:
harvest migrate run --include-dir autumn-harvest-plugin/migrations/harvest

# One invocation per shard database. Pass each DSN through the environment,
# not `--database-url`: a command line is visible host-wide (`ps`, /proc) for
# as long as the migration runs, and a shard DSN carries a password. `set -e`
# keeps the repeated-flag behaviour of stopping before later shards on failure.
set -e
HARVEST_DATABASE_URL="$SHARD_A" harvest migrate run
HARVEST_DATABASE_URL="$SHARD_B" harvest migrate run
```

Harvest's own migrations are embedded in the binary, so this needs neither a
source tree nor the `diesel` CLI. A migration runs in one transaction together
with its row in `__diesel_schema_migrations` — the same ledger Autumn and
Diesel read — so it is applied exactly once whichever of them applies it, and a
failure leaves neither the schema change nor the record of it.

A migration whose `metadata.toml` says `run_in_transaction = false`, as a
`CREATE INDEX CONCURRENTLY` migration must, is applied without one, exactly as
Diesel applies it — and **the exactly-once guarantee above does not extend to
it**. With no transaction there is nothing to hold a lock in and nothing to
roll back, so two migrators racing can both execute its body; the loser reports
it under `applied_unserialized` rather than pretending otherwise. Run migrators
one at a time against a database, and write such migrations to be idempotent
(`IF NOT EXISTS`), exactly as `diesel migration run` requires.

TLS is supported and always *verified* — chain and hostname, against the
platform trust store — so `sslmode=require`, `verify-ca` and `verify-full` all
connect (a self-signed certificate has to be in that trust store).
`sslmode=disable` still connects in plaintext.

### Deployment preflight

Run preflight before promoting a Harvest-backed service:

```bash
cargo run -p autumn-harvest-cli -- --base-url http://localhost:3000/api/harvest preflight
cargo run -p autumn-harvest-cli -- --base-url http://localhost:3000/api/harvest --output json preflight
```

`harvest preflight` calls `GET /api/harvest/admin/preflight` and performs only
read-only checks. It reports API/runtime readiness, Harvest migrations on every
configured shard, shard read/write availability, catalog and schedule
resolvability, worker queue coverage and freshness, DLQ read access, retention
visibility, and whether the admin API has an auth boundary in non-dev profiles.
The default output is a compact table; `--output json` returns the same response
shape as the API for CI and release scripts.

**Authenticating the call.** `/admin/preflight` is admin-gated like every other
`/admin` route, and how you satisfy that gate depends on the profile:

- **`AUTUMN_PROFILE=dev` with no auth boundary** — nothing to do. The management
  API is served unauthenticated to any caller that can reach the socket, so the
  command above works as written against a local app (this is what makes the
  [quickstart](examples/quickstart/README.md)'s preflight step run). The app logs
  a warning at startup when it is in this state, and preflight's own
  `admin_auth_boundary` check reports it as `unauthenticated_access: true` —
  do not expose such a process beyond localhost.
- **Any other profile** — the gate is fail-closed. Pass a scoped API token with
  `--token` / `HARVEST_TOKEN` (a `read` scope is enough; see
  `harvest token --help`), or run the command from a context that carries an
  admin session for whatever middleware you mounted via
  `HarvestPlugin::api_with_auth`.

Exit codes are deploy-gate friendly: `0` means `overall_status = pass`, `2`
means `warn`, and `1` means `fail` or a transport/API error. Use warning exit
code `2` when your release process allows a separate "promote with caution"
branch; otherwise treat any nonzero exit as a failed gate.

### Starter alert rules and runbooks

Harvest ships a versioned starter alert pack for first production deployments:
[`docs/alerts/starter-pack-v0.1.0.json`](docs/alerts/starter-pack-v0.1.0.json).
The pack covers preflight failure, missing worker coverage, stale/draining
worker saturation, queue backlog growth, activity failures, DLQ growth, missed
schedules, retention lag, shard readiness, and the pending
`no_compatible_worker` build-routing signal. Pair it with
[`docs/runbooks/harvest-alerts.md`](docs/runbooks/harvest-alerts.md) and the
synthetic drills in
[`docs/runbooks/synthetic-incident-drills.md`](docs/runbooks/synthetic-incident-drills.md).

The thresholds are starter defaults, not universal SLOs. Tune them to workload
volume, downstream SLAs, shard count, queue topology, and schedule cadence. The
Prometheus examples use only ADR-0001/#138 metric names and bounded labels;
operators without Prometheus can run the equivalent CLI/API checks documented
in [`docs/alerts/README.md`](docs/alerts/README.md).

### Measured performance baselines

[`docs/benchmarks.md`](docs/benchmarks.md) publishes **end-to-end** numbers —
sustained workflows/sec for a canonical 3-activity workflow, activity dispatch
and signal round-trip percentiles, and replay throughput, each at 1, 2 and 4
shards — with a committed `docker-compose.yml` and a one-command runner so you
can reproduce any of them on your own hardware.

[`docs/performance.md`](docs/performance.md) is the component-level complement:
it publishes measured task-claim and enqueue baselines: how claim latency scales with pending-backlog depth (the
number that answers *"when do I add a shard?"*), what five representative
claim-path predicates cost (six more are in the query on every claim but are
left on their cheapest null/empty path, so they are evaluated rather than
measured — the page names them), and the `EXPLAIN (ANALYZE, BUFFERS)` plan
behind both. Like the
alert pack, these are starter reference numbers from one machine — reproduce
them on your own hardware before designing against them. A CI gate defends the
headline scenario so a change that collapses the claim path fails the build.

### Controlling duplicate workflow starts

By default, starting a workflow with a `(workflow_name, workflow_id)` pair that already exists returns the existing execution. This "allow duplicate" behaviour is correct for upstream services that retry a start whose response was lost. It is **not** correct when you need at-most-one, retry-after-failure, or terminate-and-replace semantics.

Pass `reuse_policy` in the HTTP body (or `--reuse-policy` on the CLI) to override the default:

| Value | Behaviour |
|---|---|
| `allow_duplicate` (default) | Return the existing execution unconditionally. |
| `reject_duplicate` | Return **409 Conflict** with `existing_execution_id` and `existing_state` in the body. Use for at-most-one semantics. |
| `allow_duplicate_failed_only` | Start fresh if the prior execution is FAILED or CANCELLED; return the existing execution if RUNNING or COMPLETED. Use for retry-after-failure. |
| `terminate_if_running` | Cancel a RUNNING prior execution and start a fresh run; start fresh unconditionally for any terminal prior state. |

An unknown `reuse_policy` value returns `400 Bad Request` with the offending value echoed in the error body; it does not silently fall back to the default.

**Retry-after-failure**: pass `reuse_policy: "allow_duplicate_failed_only"` so an upstream retry against a failed run gets a fresh execution while a successful or in-progress run is not superseded. Pass `reuse_policy: "reject_duplicate"` for strict at-most-one semantics.

```bash
# Retry-after-failure: start fresh only if the prior run failed or was cancelled
cargo run -p autumn-harvest-cli -- workflow start billing_workflow \
    --workflow-id "order-42" \
    --reuse-policy allow_duplicate_failed_only \
    --input-json '{"order_id": 42}'

# At-most-one: reject a second start with 409 while the workflow is still running
cargo run -p autumn-harvest-cli -- workflow start billing_workflow \
    --workflow-id "order-42" \
    --reuse-policy reject_duplicate

# Terminate-and-replace: cancel a stuck run and immediately start fresh
cargo run -p autumn-harvest-cli -- workflow start billing_workflow \
    --workflow-id "order-42" \
    --reuse-policy terminate_if_running
```

Via the HTTP API directly:

```json
POST /api/harvest/workflows/billing_workflow/start
{
  "workflow_id": "order-42",
  "reuse_policy": "allow_duplicate_failed_only",
  "input": {"order_id": 42}
}
```

A 409 response body looks like:

```json
{
  "existing_execution_id": "01234567-...",
  "existing_state": "RUNNING"
}
```

### Capping concurrency for rate-limited downstream dependencies

Workflows commonly integrate with rate-limited APIs (Stripe, OpenAI, SendGrid,
Twilio, S3 multipart, internal microservices with a `max_connections` cap).
Without a concurrency cap the cluster may fan out dozens of simultaneous calls,
triggering 429s or downstream timeouts.

The old workaround — a dedicated task queue with a single worker process set to
`max_concurrent_activities = N` — is operationally expensive and fragile
(autoscalers or accidental second instances break the invariant).

Instead, declare `max_concurrent = N` directly on the activity:

```rust
// At most 5 concurrent Stripe calls across the whole cluster.
#[activity(start_to_close = "30s", max_concurrent = 5)]
async fn charge_stripe(ctx: &ActivityContext, amount_cents: u64) -> HarvestResult<String> {
    // … Stripe SDK call
}
```

When multiple activities all touch the same rate-limited API, share the budget
with `concurrency_key`:

```rust
// "stripe" budget: combined in-flight count of both activities never exceeds 5.
#[activity(start_to_close = "30s", max_concurrent = 5, concurrency_key = "stripe")]
async fn charge_stripe(ctx: &ActivityContext, amount_cents: u64) -> HarvestResult<String> {
    // …
}

#[activity(start_to_close = "10s", max_concurrent = 5, concurrency_key = "stripe")]
async fn refund_stripe(ctx: &ActivityContext, charge_id: String) -> HarvestResult<()> {
    // …
}
```

Activities sharing a `concurrency_key` must declare the **same** `max_concurrent`
value. Disagreeing values are caught at `HarvestBuilder::build()` time:

```
HarvestBuilderError::ConcurrencyKeyMismatch { key: "stripe", activities: [...] }
```

The cap is enforced cluster-wide via a `WHERE NOT EXISTS` predicate on the
`harvest_task_queue` claim query. A task whose key is saturated is skipped and
re-evaluated on the next poll — no dedicated worker process, no extra table, no
background coordinator. The partial index
`harvest_task_queue_concurrency_key_running` makes the saturation check O(log n)
on RUNNING rows with a non-NULL key; activities without a cap pay zero overhead.

Inspect live stats with the CLI:

```bash
harvest concurrency status
# [{ "key": "stripe", "max_concurrent": 5, "in_flight": 3, "pending": 12 }, …]
```

Or via the management API directly:

```bash
GET /api/harvest/admin/concurrency
```

### Activity error handling

Activity handlers can return any error type that implements
`autumn_harvest::failure::IntoActivityErrorString`. Out of the box that means
**plain `String`** (the legacy shape — `Err("network down".to_string())`) **and
`ActivityFailure`** (the typed shape introduced in #227). The macro dispatch
chooses the right encoding at compile time, so authors never call serialisation
helpers directly.

```rust
use autumn_harvest::prelude::*;

#[activity(retry = RetryPolicy::exponential(5, Duration::from_secs(1)))]
async fn charge_card(ctx: &ActivityContext, amount: u32) -> Result<(), ActivityFailure> {
    // Transient — let the retry policy keep working.
    if amount == 0 {
        return Err(ActivityFailure::retryable(
            "UpstreamTimeout",
            "payment gateway timed out",
        ));
    }
    // Permanent — skip remaining retries, route straight to DLQ.
    if amount > 1_000_000 {
        return Err(ActivityFailure::non_retryable(
            "InvalidInput",
            "amount exceeds per-transaction ceiling",
        ));
    }
    Ok(())
}
```

`ActivityFailure` carries four fields:

| Field | Purpose |
|---|---|
| `error_type` | Stable, low-cardinality class name (e.g. `"InvalidInput"`, `"RateLimitExceeded"`). Used as the `error.type` attribute on `harvest.activity.duration` and `harvest.activity.failed`, and as the matcher input for `RetryPolicy::non_retryable_errors`. |
| `message` | Human-readable description. Shown in `Display` output (`"InvalidInput: amount exceeds per-transaction ceiling"`). |
| `details` | Optional `serde_json::Value` for structured context preserved on the `ActivityFailed` event in workflow history. |
| `non_retryable` | When `true`, the worker skips every remaining retry attempt regardless of `RetryPolicy.max_attempts` and fails the activity on this attempt. The workflow function then sees `Err(HarvestError::ActivityFailed { … })`. |

**Resolution order against `RetryPolicy::non_retryable_errors`**:

1. If `ActivityFailure.non_retryable == true`, retries are skipped immediately.
2. Otherwise the worker compares each entry in `non_retryable_errors` against
   `error_type` first (structured, stable across log-format changes).
3. If no `error_type` match, the worker falls back to a full-string match
   against the raw error payload (legacy back-compat).

So both of these halt retries on the first attempt:

```rust
// Typed surface (preferred).
let mut policy = RetryPolicy::exponential(5, Duration::from_secs(1));
policy.non_retryable_errors = vec!["InvalidInput".into()];
// Activity returns: ActivityFailure::retryable("InvalidInput", "...")
// → matched on error_type, no retries.

// Legacy surface (still works).
let mut policy = RetryPolicy::exponential(5, Duration::from_secs(1));
policy.non_retryable_errors = vec!["amount exceeds per-transaction ceiling".into()];
// Activity returns: Err("amount exceeds per-transaction ceiling".to_string())
// → matched on raw string, no retries.
```

**Why typed errors?** Hand-formatted error strings drift every time a log
message is reworded, silently breaking retry policies that depended on exact
equality. The same drift makes operators do regex queries over
`harvest_events.event_data->>'error'` to compute failure-class breakdowns.
Lifting the class into a typed field (`error_type`) — the same shape Temporal,
Cadence, and DBOS use — keeps retry policy stable across refactors and lets
the `harvest.activity.failed{workflow.type="...", error.type="..."}` counter
answer "what's the dominant failure class right now?" in one PromQL query.

**Backward compatibility.** Every activity returning `Err(String)` continues to
work unchanged: the engine wraps it as `ActivityFailure { error_type: "Error",
non_retryable: false, .. }`, so existing dashboards see `error.type=Error`
without code changes. Pre-#227 `ActivityFailed` events stored in the DB
deserialise via `serde(default)` on the new fields — no migration is needed.

### Activity idempotency keys

Harvest activities are **at-least-once**. A worker crash, a `start_to_close`
timeout, or a duplicate task-queue dispatch will cause the activity to run
again — potentially after the external system has already accepted the first
request.  Passing a stable idempotency key to the downstream API (Stripe,
SendGrid, Twilio, S3 multipart, your own mutation endpoint) converts
at-least-once into effectively-exactly-once by letting the provider
deduplicate.

Every activity — **regular and local** — receives a Harvest-provided key via
`ctx.idempotency_key()`.  The key is stable across:

- worker restarts
- duplicate task-queue dispatch of the same logical invocation
- deterministic replay
- every retry attempt for the same logical invocation

Two distinct activity invocations — even calling the same activity name with
the same input — always receive different keys.

**Local activities** (`#[activity(local = true)]`) are fully supported.  The
key is derived from the `ActivityExecId` recorded in the
`LocalActivityScheduled` event, so it is identical across all inline retry
attempts.  Local activities do not support heartbeating, but `idempotency_key()`
works identically to regular activities.  If a future activity type were ever
excluded, `idempotency_key()` would return a descriptive
`HarvestError::Config` rather than silently producing an unstable or
meaningless value.

#### Billing / payment example

```rust
#[activity(start_to_close = "30s", retry = RetryPolicy::exponential(3, Duration::from_secs(2)))]
async fn charge_card(
    ctx: &ActivityContext,
    amount_cents: u64,
    customer_id: String,
) -> HarvestResult<String> {
    // idempotency_key() is always Ok in production; only None in bare unit-test
    // contexts built without with_idempotency_key().
    let idem_key = ctx.idempotency_key()?.as_str().to_owned();

    let charge_id = stripe_client
        .charges()
        .create(CreateCharge {
            amount: amount_cents as i64,
            customer: customer_id,
            idempotency_key: Some(idem_key), // ← same on every retry attempt
            ..Default::default()
        })
        .await
        .map_err(|e| e.to_string())?;

    Ok(charge_id)
}
```

If the worker crashes after Stripe accepts the charge but before Harvest
records `ActivityCompleted`, the retry carries the same key and Stripe returns
the already-created charge — no duplicate payment.

#### Subkeys for multiple side effects

When one activity must produce several distinct outbound calls, derive named
subkeys from the base key:

```rust
#[activity(start_to_close = "60s")]
async fn provision_account(ctx: &ActivityContext, user_id: i64) -> HarvestResult<()> {
    let key = ctx.idempotency_key()?;

    // Each call gets a distinct, stable key derived from the same parent.
    create_db_user(&user_id, key.subkey("db").as_str()).await?;
    send_welcome_email(&user_id, key.subkey("email").as_str()).await?;
    create_billing_profile(&user_id, key.subkey("billing").as_str()).await?;

    Ok(())
}
```

#### Attempt-scoped keys (opt-in)

The default key is **retry-stable** — the same value across all attempts for
the same logical invocation. If a downstream API requires a fresh key on each
attempt (uncommon), derive an attempt-scoped subkey:

```rust
let idem_key = ctx
    .idempotency_key()?
    .subkey(&format!("attempt-{}", ctx.attempt().unwrap_or(1)));
```

### Filtering the workflow list

`workflow list` (and `GET /workflows`) accept three additional filter knobs on
top of `limit`:

| CLI flag | Query param | Behavior |
|---|---|---|
| `--state RUNNING` (repeatable, also accepts `RUNNING,FAILED`) | `?state=RUNNING,FAILED` (repeatable) | Exact match on the workflow execution state. Allowed values: `RUNNING`, `COMPLETED`, `FAILED`, `CANCELLED`, `TIMED_OUT`, `CONTINUED_AS_NEW`, `TERMINATED`. |
| `--workflow-name onboarding` | `?workflow_name=onboarding` | Exact match on the registered workflow name. |
| `--search-attr tenant=acme` (repeatable) | `?search_attr=tenant:acme` (repeatable) | JSONB containment predicate on `search_attrs`. Multiple flags AND together; repeating a key narrows. Hits the existing `idx_harvest_we_search` GIN index. |

Invalid values (unknown state, malformed `search_attr` missing the `:`
separator) return `400 Bad Request` instead of silently matching nothing.

Triage example — find the running onboarding workflows for a single tenant:

```bash
cargo run -p autumn-harvest-cli -- workflow list \
    --state RUNNING \
    --workflow-name onboarding \
    --search-attr tenant=acme
```

### Listing child workflows

`workflow children` and `GET /workflows/{execution_id}/children` expose the
parent -> child relationship recorded on workflow execution rows. The API fans
out across all configured shards, merges by `started_at DESC`, and paginates the
combined result.

| CLI flag | Query param | Behavior |
|---|---|---|
| `--status Failed` (repeatable, also accepts comma-separated values) | `?status=Failed&status=Running` | OR filter on child status. Allowed values: `Running`, `Paused`, `Failed`, `Completed`, `Cancelled`, `Terminated`, `TimedOut`, `ContinuedAsNew`. |
| `--workflow-name billing_child` | `?workflow_name=billing_child` | Exact match on the child workflow name. |
| `--limit 100` | `?limit=100` | Page size. Defaults to 50 and is capped at 500. |
| `--cursor <cursor>` | `?cursor=<cursor>` | Continue from the previous page's `next_cursor`. |
| `--depth 1` | `?depth=1` | Include recursive descendants through the given zero-based depth. Direct children are depth `0`; values above `5` return `400 Bad Request`. |
| `--json` | n/a | Print the raw JSON payload. Without `--json`, the CLI renders a table. |

Each child entry includes `exec_id`, `workflow_name`, `status`, `started_at`,
`completed_at`, `error_summary`, `shard_id`, and `depth`. `GET
/workflows/{execution_id}` also includes top-level `parent_id` alongside the
existing execution/history payload.

### Inspecting workflow stack (current wait-state)

Use the stack endpoint to inspect what an execution is currently waiting on
(pending activities, local activities, timers, buffered signals, child
workflows) plus the latest durable event cursor:

```bash
# CLI
cargo run -p autumn-harvest-cli -- workflow stack <execution-id>

# HTTP
curl -s http://localhost:3000/api/harvest/workflows/<execution-id>/stack | jq .
```

Route:

```text
GET /api/harvest/workflows/{execution_id}/stack
```

Forward-compatibility contract:

- Unknown top-level fields are additive; clients should ignore what they do not
  recognize.
- Collection item objects may gain new optional fields over time.
- Known fields retain their current meaning; optional fields may be `null` when
  data is unavailable.
- `last_event_id` is monotonic per execution and can be used as a lightweight
  change cursor.

### Post-incident DLQ drain

After an infrastructure incident (database blip, downstream 503, misconfigured retry policy), many activities end up in the dead-letter queue. Rather than replaying them one-by-one with `dlq replay <id>`, use the bulk endpoints to drain the queue by activity or time window.

**Always dry-run first** to see what will be acted on before committing:

```bash
# Preview — no writes, returns matched count and IDs
curl -s -X POST https://api.example.com/api/harvest/dead-letters/replay \
  -H 'Content-Type: application/json' \
  -d '{"activity_name":"send_email","failed_after":"2026-04-27T12:00:00Z","dry_run":true}' | jq .
```

The response reports `matched` (total rows satisfying the filter, before any limit clip), `acted_on`, and any per-row `failures`. When `matched` exceeds `acted_on` after a non-dry run, the limit clipped the result — call again until `matched == 0`.

```bash
# Real replay: re-enqueue all matching dead-letter entries
curl -s -X POST https://api.example.com/api/harvest/dead-letters/replay \
  -H 'Content-Type: application/json' \
  -d '{"activity_name":"send_email","failed_after":"2026-04-27T12:00:00Z"}' | jq .

# Discard (delete without re-enqueueing) — use when the work is no longer needed
curl -s -X POST https://api.example.com/api/harvest/dead-letters/discard \
  -H 'Content-Type: application/json' \
  -d '{"activity_name":"charge_card","failed_before":"2026-04-27T00:00:00Z"}' | jq .
```

Or via the CLI:

```bash
# Dry run first
cargo run -p autumn-harvest-cli -- dlq bulk-replay \
    --activity-name send_email \
    --failed-after 2026-04-27T12:00:00Z \
    --dry-run

# Commit the replay
cargo run -p autumn-harvest-cli -- dlq bulk-replay \
    --activity-name send_email \
    --failed-after 2026-04-27T12:00:00Z

# Discard stale failures
cargo run -p autumn-harvest-cli -- dlq bulk-discard \
    --activity-name charge_card \
    --failed-before 2026-04-27T00:00:00Z
```

At least one of `--activity-name`, `--workflow-name`, `--failed-after`, or `--failed-before` is required. A filter with only `--limit` or `--dry-run` is rejected with `400 Bad Request`. The default batch size is 100 rows; pass `--limit N` (max 1000) to replay more per call.

### Scheduling workflows on a cron

Use `WorkflowSchedule` when you want to fire a single workflow on a cron or
interval expression. Use `DagBuilder` when you need dependency-graph fan-out,
trigger rules, or multi-step pipelines between tasks.

```rust
use autumn_harvest::policy::{Schedule, WorkflowSchedule};

// Register a daily billing run at 03:00 UTC with at-most-1 concurrent run.
let sched = WorkflowSchedule::new(
    "daily_billing_report",
    Schedule::Cron("0 3 * * *".to_string()),
)
.with_input(serde_json::json!({"region": "us-east"}))
.with_max_active_runs(1);

// Wire it into the builder alongside your workflow registration.
let app = autumn_web::app()
    .workflows(workflows![daily_billing_report])
    .workflow_schedule(sched)
    .worker(WorkerConfig::default());
```

The scheduler tick derives a deterministic `workflow_id` of
`sched:{name}:{unix_ts}` so retries after a crashed tick are idempotent.
`max_active_runs = 1` (the default) means: if the previous run is still
`RUNNING` when the next cron fires, skip that firing rather than stack runs.
Set `catchup = true` to replay missed firings after a scheduler downtime window.

Manage schedules via the CLI:

```bash
# Create a workflow schedule via API
cargo run -p autumn-harvest-cli -- schedule create-workflow \
    --name daily_billing_report \
    --cron "0 3 * * *" \
    --max-active-runs 1

# List all schedules (both DAG and workflow kinds)
cargo run -p autumn-harvest-cli -- schedule list

# Pause / resume / delete by ID
cargo run -p autumn-harvest-cli -- schedule pause  <id>
cargo run -p autumn-harvest-cli -- schedule resume <id>
cargo run -p autumn-harvest-cli -- schedule delete <id>
```

Or via the HTTP API:

```bash
POST /api/harvest/admin/schedules/workflow
GET  /api/harvest/admin/schedules
POST /api/harvest/admin/schedules/{id}/pause
POST /api/harvest/admin/schedules/{id}/resume
DELETE /api/harvest/admin/schedules/{id}
```

### Worker Fleet

The management API exposes three routes for observing the live worker fleet. Workers register on startup and heartbeat every 5 s (configurable via `WorkerConfig`). Workers that have not heartbeated within `2 × heartbeat_interval` (default 10 s) are classified as `stale` at query time but are not auto-deleted.

```bash
# List all workers (supports ?queue=, ?shard_id=, ?status=, ?health= filters)
GET /api/harvest/workers

# Filter to workers polling a specific queue on a specific shard
GET /api/harvest/workers?queue=email-workers&shard_id=0

# Single worker detail — includes currently claimed task-queue item IDs
GET /api/harvest/workers/{worker_id}

# Fleet health roll-up
GET /api/harvest/workers/health
# → { "healthy": 4, "stale": 1, "draining": 0, "by_queue": {"default": 5}, "by_shard": {"0": 5} }
```

Equivalent CLI (thin HTTP client, no direct Postgres access required):

```bash
cargo run -p autumn-harvest-cli -- worker list
cargo run -p autumn-harvest-cli -- worker list --queue email-workers --shard-id 0
cargo run -p autumn-harvest-cli -- worker get <worker-id>
cargo run -p autumn-harvest-cli -- worker health
```

Worker lifecycle status values: `Active` (normal operation), `Draining` (received SIGTERM, waiting for in-flight tasks to complete), `Stopped` (shutdown complete). The `health` field in responses is derived at query time: `healthy` or `stale`.

In sharded deployments the API merges results across all shards via `iter_shards()`. The `harvest_workers` table lives on every shard; each worker row is pinned to the shard the worker is polling.

### Vantage dashboard — Workers tab

The embedded Vantage UI (`harvest_ui_router`, typically mounted at `/api/harvest/ui`) includes a **Workers** tab at `GET /ui/workers`. The page gives a single-screen answer to the canonical 2am question — "are my workers up?" — without any `curl | jq` round-trips:

- **Fleet health banner**: one of `Healthy` / `Degraded` / `Unhealthy` with absolute counts (total, active, draining, stopped, stale workers).
- **Worker table**: sorted stale-first within each status group (`Active` → `Draining` → `Stopped`), with worker ID (linked to the detail page), status pill, relative last-heartbeat time (e.g. `4m ago`), source shard, and in-flight task count. Stale workers are visually tinted and annotated `(stale)`.
- **Filters**: `?status=`, `?shard=`, `?stale=true`; they compose with AND. Unknown values return 400.
- **Pagination**: `?page=&limit=` matching the workflow list (max 200 per page).
- **Partial shard failure**: if one shard's pool errors the banner shows `Degraded` and the affected section renders a "shard unavailable" stub — the rest of the page is unaffected.
- **Auto-refresh**: add `?refresh=30` to emit a `<meta http-equiv="refresh" content="30">` tag; no JavaScript required.

```
┌─────────────────────────────────────────────────────────────┐
│ 🔭 Vantage  Harvest dashboard     Workflows  Workers        │
├─────────────────────────────────────────────────────────────┤
│ Workers                                                     │
│                                                             │
│ ┌─ Healthy ────────────────────────────────────────────┐   │
│ │ Healthy — 5 workers | 5 active | 0 draining |       │   │
│ │ 0 stopped | 0 stale                                  │   │
│ └──────────────────────────────────────────────────────┘   │
│                                                             │
│ [Status ▼] [Shard ____] [Stale ▼] [Per page ____] [Apply] │
│                                                             │
│ Worker ID   Status   Last Heartbeat   Shard   In-Flight    │
│ ─────────────────────────────────────────────────────────  │
│ abc123…     Active   just now         0       2            │
│ def456…     Active   3s ago           0       0            │
└─────────────────────────────────────────────────────────────┘
```

## Requirements

- Rust 1.88.0 or newer (MSRV)
- Postgres 12+ — except for `cargo dev`, whose `dev-runtime-managed` tier
  downloads one for you
- The `db` feature is enabled by default and pulls Diesel + diesel-async; build
  with `--no-default-features` for pure compile-checks on systems without
  libpq.

## Status

Version 0.6.0 builds on the Phase 4 surface (see
[`CHANGELOG.md`](CHANGELOG.md) for this release's full entry list). The core
surface is broad: DAG scheduling, `#[dag]`, trigger rules, signal delivery,
`ctx.wait_for_signal`, query registration/dispatch, the management API,
workflow result waiting, dead-letter list/replay/aggregation endpoints,
pause/resume controls, DAG retry from failed nodes, timezone-aware cron
schedules, scaling signals, and metrics endpoints are implemented and covered
by integration tests. Durable workflow cancellation is implemented with
management API support and activity heartbeat cancellation checks. First-class
Saga compensation is implemented through the `Saga` builder.

API stability: pre-1.0. Breaking changes happen in minor versions per Cargo's
0.x semver convention. Each release notes the migration where applicable.

## Architecture in one paragraph

Workflows are deterministic Rust async functions. When they hit
`ctx.execute_activity(...)` for the first time, the activity is enqueued to a
Postgres task queue and the workflow suspends. A worker claims the activity
(`SELECT … FOR UPDATE SKIP LOCKED`), runs it, and writes the result as an event
in the workflow's history. The workflow then resumes — on the *same* worker if
cached, or by replaying its history from scratch on any other worker. Replay is
deterministic because every non-deterministic decision (activity result, timer
fire, signal arrival, version branch) is recorded as an event the first time
and read back from history on subsequent invocations. This is the same model as
Temporal and Cadence; the operational difference is that you only need
Postgres, not a separate service.

## Sharding

Workflow state can be spread across multiple Postgres databases without
cross-shard transactions. Each `ExecutionId` carries its `ShardId` in the
first two bytes of the UUID, so any caller holding an id resolves to the
owning shard in O(1) — no directory table required. Single-shard deployments
keep working unchanged; the plugin wires a `ShardRouter::single()` and a
`ShardedDbPool::single(pool)` by default.

```rust
use autumn_harvest::{ExecutionId, ShardId, ShardRouter};

let router = ShardRouter::new(
    vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)], // readable
    vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)], // writable
    ShardId::new(0),                                         // default
);
let shard = router.pick_for_new_workflow("onboarding", "user-42");
let exec_id = ExecutionId::new_for_shard(shard);
assert_eq!(exec_id.shard(), shard);
```

Adding a shard (new workflows only): provision and migrate the new database,
add it to `readable_shards`, restart the plugin, then run the readiness gate
before promotion:

```bash
cargo run -p autumn-harvest-cli -- shard health --candidate-shard 1
cargo run -p autumn-harvest-cli -- --output json shard health --candidate-shard 1
```

The matching management API endpoint is `GET /api/harvest/admin/shards/health`.
It returns one row per configured shard, including shard roles, reachability,
schema readiness, active/stale worker counts, worker queue coverage, scheduler
freshness when schedules are enabled, queue/DLQ pressure, and machine-readable
reason codes. The CLI exits non-zero when any writable shard or named candidate
reports `degraded` or `unavailable`. Only after the candidate row reports
`readiness: "ready"` should you flip it into `writable_shards`. In-flight
workflows drain on their original shard.

By default `/api/harvest/health` stays a cheap liveness check for local
single-shard development. To make it a rollout/readiness probe that returns
`503` until writable shard readiness is `ready`, enable:

```toml
[harvest.readiness]
require_shard_readiness = true
```

The equivalent environment override is
`AUTUMN_HARVEST_READINESS__REQUIRE_SHARD_READINESS=true`.

## Redis dispatch (optional)

Postgres is the only required infrastructure dependency. On a deep backlog
the Postgres claim scans and sorts the queue on every claim. An optional
Redis Streams **dispatch channel** removes that scan: the engine publishes a
small reference (task id, queue, due time) for each claimable row, and a
worker claims the named row in Postgres with the full claim predicate before
it acks the reference. Postgres keeps every row, every claim gate and the
whole history write path.

Build `autumn-harvest-plugin` with the `redis` feature, then set the URL:

```toml
[harvest.redis]
url = "redis://cache:6379"
```

Leave `url` unset and every worker stays on the Postgres claim path, which
is the default. A build without the `redis` feature rejects a configured URL
at config validation rather than ignoring it.

The fallback covers the **running** state. A started process that loses Redis
returns to the Postgres claim path, so availability with Redis down equals
availability with Redis absent. It does not cover boot: a configured URL that
cannot connect **fails startup**, in every mode, with an error naming the
endpoint. A process that came up without its channel would look healthy and
publish nothing, so the failure is loud instead.

The connection is plaintext, and `redis://` sends the password in cleartext.
This release carries no TLS transport: a `rediss://` URL is rejected at
startup, and issue #1429 tracks TLS support. Keep Redis on a private network
or behind a TLS tunnel that terminates on the host. v1 targets a single Redis
instance and a single-shard runtime; Redis Cluster is not supported.

See [`docs/operations/redis-dispatch.md`](docs/operations/redis-dispatch.md)
for the key layout, the crash matrix, the failure modes and the v1 limits.

## Testing workflow code changes with the replayer

Before deploying any edit to a `#[workflow]` function, verify it is
replay-safe against recorded production histories using `WorkflowReplayer`
(available when the `testing` feature is enabled).

### CI pattern

```toml
# Cargo.toml — in your app's dev-dependencies
autumn-harvest = { version = "0.6", features = ["testing"] }
```

```rust
// tests/replay_regression.rs
use autumn_harvest::testing::{HistorySnapshot, ReplayStatus, WorkflowReplayer};

#[tokio::test]
async fn onboarding_is_replay_safe() {
    // Load a fixture exported from production or a previous run.
    let json = std::fs::read_to_string("fixtures/onboarding_history.json").unwrap();

    let report = WorkflowReplayer::new()
        .register_fn("onboarding", onboarding_handler)  // your #[workflow] fn
        .replay_from_json(&json)
        .await
        .expect("fixture must parse");

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "replay regression:\n{report}"
    );
}
```

### Exporting a history fixture

Use the read-only history export API or CLI to capture a fixture directly from
stored workflow history. Redacted exports are the default and are suitable for
support/debugging. CI replay fixtures must use `full` payloads and should be
stored only in private fixture storage because activity inputs, outputs, signal
payloads, and tokens may be present.

```sh
cargo run -p autumn-harvest-cli -- history export <execution-id> \
  --payload-policy full \
  --output-file fixtures/onboarding_history.json
```

For release gates, export a bounded batch from recent production histories:

```sh
cargo run -p autumn-harvest-cli -- history export-batch \
  --workflow-name onboarding \
  --state-group terminal \
  --updated-after 2026-05-01T00:00:00Z \
  --limit 1000 \
  --payload-policy full \
  --output-file fixtures/onboarding_batch.json
```

Batch responses include `schema`, `version`, `status`, `exports`, `failures`,
and `shard_coverage`. A `partial` status means at least one shard could not be
read; treat that as a blocked release gate until the missing shard is exported
or explicitly waived.

See `docs/runbooks/replay-fixture-export.md` for the release-safety playbook.

### CLI validator

```sh
cargo run --bin harvest-replay -- \
  --workflow onboarding \
  --history-source json \
  --json-path ./fixtures/onboarding_history.json
```

Exit code 0 = `ReplaySucceeded`. Exit code 1 = non-determinism or workflow
failure. Extend `harvest-replay/src/bin/harvest_replay.rs` with your own
workflow handlers so the binary can replay against live code.

### Resetting a workflow after a bad deploy

Use reset when a running top-level workflow is stuck on history produced by code
that the new workflow definition can no longer replay. Reset terminates the
source execution, creates a new execution on the same shard, copies history
through a valid event boundary, appends a `WorkflowResetFork` marker, and queues
the fork for execution under the current code.

The reset point must be a completed boundary: no open activity, local activity,
timer, child workflow, external completion, or update may still be unresolved at
or before `--to-event`. Terminal workflows, child workflows, and
continue-as-new histories are rejected; those are different demons.

Operator flow:

```sh
# Inspect the current wait-state and pick a candidate event cursor.
cargo run -p autumn-harvest-cli -- workflow stack <execution-id>

# Verify the boundary and side effects without writing anything.
cargo run -p autumn-harvest-cli -- workflow reset <execution-id> \
  --to-event 42 \
  --reason "recover from bad deploy 2026-05-03" \
  --operator-id "oncall-mark" \
  --signal-reapply buffer \
  --dry-run

# Apply the reset once the dry run reports a valid plan.
cargo run -p autumn-harvest-cli -- workflow reset <execution-id> \
  --to-event 42 \
  --reason "recover from bad deploy 2026-05-03" \
  --operator-id "oncall-mark" \
  --signal-reapply buffer
```

`--signal-reapply drop` consumes pending source signals during teardown.
`--signal-reapply buffer` copies pending source signals onto the fork so the new
run sees them after replay. The source execution ends in `TERMINATED`; verify
the returned `new_exec_id` reaches `RUNNING` and then normal terminal state.
For tests and pre-deploy checks, `WorkflowReplayer::replay_with_reset(history,
reset_to_event_id)` replays only the carried history plus the reset marker.

### What the replayer detects

| Kind | Trigger |
|---|---|
| `ActivityScheduleMismatch` | Activity name changed or order swapped |
| `LocalActivityScheduleMismatch` | Local activity name changed |
| `TimerMismatch` | Timer inserted / removed / reordered |
| `SignalMismatch` | Signal wait inserted where history has a non-signal event |
| `ChildWorkflowMismatch` | Child workflow name or input changed |
| `SideEffectMismatch` | `ctx.side_effect` ID changed |
| `ContinueAsNewMismatch` | `continue_as_new` input changed |

Changes that are safe without a `ctx.version()` fence: none of the above.
Use `ctx.version("change_id", 1, 2)` and guard the new code path behind the
returned version number; old histories replay with version 1 and skip the new
path.

### Retiring a version-gated branch

Use the version-gate usage report before deleting an old branch from workflow
code. The report is read-only: it scans existing `version:{change_id}` marker
events and never starts, cancels, signals, resets, replays, rewrites, or deletes
workflow executions.

Migration playbook:

1. Introduce `ctx.version("billing_checkout_v2_tax", 1, 2)` and branch on the
   returned version.
2. Deploy the gated workflow code.
3. Verify replay safety against representative histories with
   `WorkflowReplayer`.
4. Wait for active version-1 executions to drain.
5. Run the guard:

```bash
cargo run -p autumn-harvest-cli -- version-usage \
  --change-id billing_checkout_v2_tax \
  --version 1 \
  --guard
```

6. Remove the version-1 branch only after the guard exits successfully.

For release automation, use compact JSON:

```bash
cargo run -p autumn-harvest-cli -- --output json version-usage \
  --change-id billing_checkout_v2_tax \
  --version 1 \
  --state-group active
```

The management API endpoint is `GET /admin/version-gates/usage`. It supports
`workflow_name`, `change_id`, `recorded_version`, `state_group` (`active`,
`terminal`, or `all`), and `shard_id` filters. Response `status` is
machine-readable: `complete`, `no_matches`, `partial`, or `unavailable`.
Treat `partial` and `unavailable` as deploy blockers because one or more shards
could not be inspected and a false zero would be worse than noisy code.

## Telemetry

Harvest emits [OpenTelemetry](https://opentelemetry.io/)-compatible spans via the [`tracing`](https://docs.rs/tracing) crate. Eight named spans cover every durable boundary:

| Span | Kind | When |
|---|---|---|
| `harvest.workflow.execute` | INTERNAL | Every workflow executor cycle (live and replay) |
| `harvest.workflow.schedule` | PRODUCER | HTTP `POST /workflows/{name}/start` |
| `harvest.activity.execute` | INTERNAL | Activity handler dispatch |
| `harvest.activity.schedule` | PRODUCER | Activity enqueued to task queue |
| `harvest.signal.send` | PRODUCER | `send_signal` call |
| `harvest.signal.deliver` | CONSUMER | Signal ingested into workflow history |
| `harvest.timer.fire` | INTERNAL | Durable timer fires |
| `harvest.child_workflow.start` | PRODUCER | Child workflow enqueued |

Replay cycles emit `harvest.workflow.execute` as a **new root span** (`harvest.replay = true`) linked to the original via `link.traceparent` so APM backends can navigate between them.

### Wiring harvest spans into your OTel pipeline

Install [`tracing-opentelemetry`](https://docs.rs/tracing-opentelemetry) alongside your existing OTel SDK and implement `TraceContextPropagator`:

```rust
use autumn_harvest::{TelemetryConfig, TraceContextCarrier, TraceContextPropagator};
use opentelemetry::Context;
use opentelemetry_sdk::propagation::TraceContextPropagator as OtelPropagator;
use std::any::Any;
use std::sync::Arc;

struct OtelBridge;

impl TraceContextPropagator for OtelBridge {
    fn capture(&self) -> Option<TraceContextCarrier> {
        // Extract the active span context into a W3C traceparent.
        let cx = Context::current();
        let span = cx.span();
        let ctx = span.span_context();
        if !ctx.is_valid() {
            return None;
        }
        let traceparent = format!(
            "00-{}-{}-{:02x}",
            ctx.trace_id(),
            ctx.span_id(),
            ctx.trace_flags().to_u8(),
        );
        Some(TraceContextCarrier::from_traceparent(traceparent))
    }

    fn install(&self, carrier: &TraceContextCarrier) -> Box<dyn Any + Send> {
        // Restore the producer's span context as the OTel active context.
        if let Some(tp) = carrier.traceparent.as_deref() {
            let mut map = std::collections::HashMap::new();
            map.insert("traceparent".to_string(), tp.to_string());
            let cx = OtelPropagator::new().extract(&map);
            let guard = cx.attach();
            Box::new(guard)
        } else {
            Box::new(())
        }
    }
}

// Wire into HarvestBuilder:
HarvestBuilder::new()
    .telemetry(
        TelemetryConfig::builder()
            .propagator(Arc::new(OtelBridge))
            .build(),
    )
    // ...
```

With no `telemetry(...)` call (the default), all `info_span!` sites compile to branch-on-atomic no-ops — zero allocation, no subscriber lock taken.

## License

Dual-licensed under MIT or Apache 2.0 at your option.
