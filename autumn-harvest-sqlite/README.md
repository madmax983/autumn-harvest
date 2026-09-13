# autumn-harvest-sqlite

An **embedded, single-writer SQLite** persistence backend for the
[`autumn-harvest`](../autumn-harvest) durable-workflow engine — for edge,
local-first, and single-server deployments where there is no database server to
run.

The valuable, hard part of `autumn-harvest` — the deterministic replay engine
(`run_workflow`, `WorkflowEvent`, the history matcher, `WorkflowContext`) — is
backend-neutral. This crate reuses that core **wholesale** and reimplements only
*persistence* (the event store, the task queue + durable timers, and the worker
pass) on embedded SQLite via `rusqlite` (`bundled`, so **no system SQLite and no
Docker are required**). It pulls in `autumn-harvest` with
`default-features = false`, so no Diesel/Postgres comes along.

A history written by this backend is byte-identical, per event, to one the
Postgres backend would write, so it replays unchanged on the core
`WorkflowReplayer`.

## When to use it

- **Edge / local-first / embedded** — the workflow engine ships *inside* your
  binary; there is no separate service to deploy or operate.
- **Single server** — one writer process against one SQLite file.
- **Development, tests, demos, CLIs** — an in-memory database gives you a full
  durable-workflow runtime with zero setup.

## When *not* to use it

Reach for the Postgres core ([`autumn-harvest`](../autumn-harvest)) instead when
you need:

- **Distributed / multi-worker** execution or **multi-server** crash recovery.
- **High throughput** — a single writer with `BEGIN IMMEDIATE` is not built for
  a fleet claiming thousands of tasks per second.
- **`LISTEN`/`NOTIFY` push wake-ups**, schedules, DAGs, the management API,
  worker sessions, retention, or sharding — all v0.1 non-goals here (see below).

## Quickstart

```rust
use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::{ExecutionOutcome, SqliteError, SqliteRuntime};

#[workflow]
async fn process_order(ctx: &WorkflowContext, order: serde_json::Value) -> Result<String, String> {
    let reservation: String = ctx
        .execute_activity(&reserve_inventory_info(), order)
        .await
        .map_err(|e| e.to_string())?;
    Ok(format!("confirmed: {reservation}"))
}

// The `#[activity]` async body is a placeholder for the shared `ActivityInfo`;
// this backend runs a caller-supplied SYNCHRONOUS body registered against it.
#[activity]
async fn reserve_inventory(_ctx: &ActivityContext, _order: serde_json::Value) -> Result<String, String> {
    Ok(String::new())
}

#[tokio::main]
async fn main() -> Result<(), SqliteError> {
    let mut rt = SqliteRuntime::open_in_memory()?;    // or open("workflows.db") for durability
    rt.register_workflow(&process_order_info());
    rt.register_activity(&reserve_inventory_info(), |input| {
        Ok(serde_json::json!(format!("reserved {input}")))
    });

    let exec = rt.start_workflow("process_order", serde_json::json!({"item": "widget"}))?;
    rt.run_until_idle().await?;                        // drive the polling loop to quiescence

    if let ExecutionOutcome::Completed(output) = rt.outcome(exec)? {
        println!("{output}");
    }
    Ok(())
}
```

A complete, runnable version is in
[`examples/quickstart.rs`](examples/quickstart.rs).

## Key concepts

- **Single-writer contract (`BEGIN IMMEDIATE`).** One writer process owns one
  SQLite file. A task claim takes the database write lock up front and flips the
  oldest ready task to `RUNNING` — exactly-once by construction, replacing
  Postgres's `SELECT … FOR UPDATE SKIP LOCKED`.
- **Polling drive model.** SQLite has no push notification, so instead of
  `LISTEN`/`NOTIFY` you *drive* the runtime: `poll_once` / `run_until_blocked` /
  `run_until_idle` drain all ready work and re-run the workflow until every run
  is terminal or blocked on external input.
- **Durability & crash recovery.** All state is in the database. Dropping the
  runtime and re-`open`-ing the same file is a faithful restart: any task left
  `RUNNING` by a crash is reclaimed and the workflow resumes purely by
  deterministic replay. Activity execution is therefore **at-least-once** — write
  activity bodies to be idempotent. See
  [`examples/durability.rs`](examples/durability.rs).

## v0.1 non-goals

Out of scope for this backend (tracked as issue #1068 follow-ups):

- Distributed / multi-writer workers; multi-server crash recovery.
- `LISTEN`/`NOTIFY` push wake-ups.
- Schedules, the management API, DAGs, worker sessions, retention, sharding.
- Idempotent starts / the `WorkflowIdReusePolicy` matrix — every
  `start_workflow` call creates a new, independent execution; dedupe upstream.
- Child workflows, external signals/cancels, local activities, updates,
  search attributes, and `continue_as_new` — a workflow reaching one of these is
  rejected **loudly, by name**, never silently dropped.

## Learn more

- **Docs guide:** [`docs/sqlite-backend.md`](../docs/sqlite-backend.md) — a
  task-oriented walkthrough of the whole surface.
- **Runnable examples:** [`examples/quickstart.rs`](examples/quickstart.rs) and
  [`examples/durability.rs`](examples/durability.rs). For a whole application on
  this backend, see
  [`examples/claude-agent-daemon/`](../examples/claude-agent-daemon/) — a local
  daemon that runs Claude agent sessions as durable workflows.
- **API reference:** `cargo doc --open -p autumn-harvest-sqlite` — the
  crate-level docs are the canonical design/contract document.
- **Why this crate exists (and why it is not a core trait):**
  [`docs/rnd/sqlite-feasibility.md`](../docs/rnd/sqlite-feasibility.md) — the
  R&D decision record behind the companion-crate shape, including the
  module-by-module Postgres-coupling audit and the capabilities the
  single-writer model gives up.
