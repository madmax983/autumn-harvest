# The SQLite backend (edge / local-first)

`autumn-harvest-sqlite` is an **embedded, single-writer** persistence backend
for the `autumn-harvest` durable-workflow engine. It gives you the full
deterministic replay engine — activities, durable timers, signals, crash
recovery — inside your own binary, backed by one SQLite file, with **no server
to run and no Docker**.

This guide is task-oriented: it walks through building a small order-processing
runtime from an empty project to a crash-recoverable, signal-driven workflow. The
canonical API/contract reference is the crate-level rustdoc
(`cargo doc --open -p autumn-harvest-sqlite`); this guide shows how to use it.

For *why* this is a separate crate rather than a `StorageBackend` trait in core
— and which capabilities the single-writer model deliberately gives up — see the
R&D decision record [`docs/rnd/sqlite-feasibility.md`](rnd/sqlite-feasibility.md).

## Contents

1. [What it is, and when to reach for it](#1-what-it-is-and-when-to-reach-for-it)
2. [Adding the dependency](#2-adding-the-dependency)
3. [Defining a workflow and an activity](#3-defining-a-workflow-and-an-activity)
4. [Opening a runtime — file vs in-memory](#4-opening-a-runtime--file-vs-in-memory)
5. [Registering workflows and activities](#5-registering-workflows-and-activities)
6. [Starting a workflow](#6-starting-a-workflow)
7. [The drive model](#7-the-drive-model)
8. [Signals (pull-only)](#8-signals-pull-only)
9. [Durability and crash recovery](#9-durability-and-crash-recovery)
10. [The single-writer / single-server contract](#10-the-single-writer--single-server-contract)
11. [v0.1 non-goals and follow-ups](#11-v01-non-goals-and-follow-ups)
12. [Runnable examples](#12-runnable-examples)

---

## 1. What it is, and when to reach for it

The hard, valuable part of `autumn-harvest` — the deterministic replay engine
(`run_workflow`, `WorkflowEvent`, the history matcher, `WorkflowContext`) — is
backend-neutral. The SQLite backend reuses that core **wholesale** and
reimplements only *persistence*: the event store, the task queue + durable
timers, and the worker pass, on embedded SQLite via `rusqlite` (`bundled`). It
depends on `autumn-harvest` with `default-features = false`, so no
Diesel/Postgres is pulled in.

A history written here is byte-identical, per event, to one the Postgres backend
would write, so it replays unchanged on the core `WorkflowReplayer`.

**Reach for the SQLite backend when:**

- You want a durable workflow engine **embedded in your binary** — edge devices,
  desktop/CLI apps, local-first tools — with nothing external to deploy.
- You run a **single server** (one writer process, one database file).
- You want a full runtime in **tests and demos** with zero setup (in-memory).

**Reach for the Postgres core ([`autumn-harvest`](../autumn-harvest)) instead
when** you need distributed / multi-worker execution, multi-server crash
recovery, high throughput, `LISTEN`/`NOTIFY` push wake-ups, schedules, DAGs, the
management API, worker sessions, retention, or sharding.

| | SQLite backend | Postgres core |
| --- | --- | --- |
| Deployment | Embedded, no server | Postgres server(s) |
| Writers | **One** (single process) | Many (a fleet) |
| Wake-ups | Polling (you drive it) | `LISTEN`/`NOTIFY` push |
| Crash recovery | Single-process reopen | Multi-server heartbeat/reclaim |
| Throughput | Modest, single-writer | High, horizontally scaled |
| Extras (schedules, DAGs, mgmt API, sharding) | v0.1 non-goals | Yes |

---

## 2. Adding the dependency

```toml
[dependencies]
autumn-harvest = { version = "0.4", default-features = false }
autumn-harvest-sqlite = "0.4"
serde = { version = "1", features = ["derive"] }
serde_json = "1"

# The drivers are async; use any runtime. tokio is the simplest.
tokio = { version = "1", features = ["macros", "rt", "rt-multi-thread"] }
```

You do **not** need the `autumn-harvest` `db` feature — the SQLite backend uses
the backend-neutral core only. `rusqlite` is pulled in with the `bundled`
feature, so there is no system SQLite dependency.

---

## 3. Defining a workflow and an activity

Workflows and activities are written with the ordinary `#[workflow]` /
`#[activity]` macros from the core prelude — the SQLite backend does not change
the authoring surface.

```rust
use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
struct Order { id: String, item: String, qty: u32 }

#[workflow]
async fn process_order(ctx: &WorkflowContext, order: Order) -> Result<String, String> {
    let reservation: String = ctx
        .execute_activity(&reserve_inventory_info(), order.clone())
        .await
        .map_err(|e| e.to_string())?;
    Ok(format!("order {} confirmed — {reservation}", order.id))
}

#[activity]
async fn reserve_inventory(_ctx: &ActivityContext, _order: Order) -> Result<String, String> {
    Ok(String::new())
}
```

> **Important — activity bodies are separate.** On this backend an activity runs
> a caller-supplied **synchronous** body registered against the activity's
> `ActivityInfo`. The `#[activity]` async fn only supplies the macro-generated
> `*_info()` companion (the name plus any declared `#[activity(...)]` defaults);
> the closure you register in step 5 is what actually executes. There is no
> `ActivityContext` I/O framework here — you write plain `Fn(Value) -> Result<Value, String>`.

---

## 4. Opening a runtime — file vs in-memory

```rust
use autumn_harvest_sqlite::SqliteRuntime;

// Durable, restart-safe: reopening the same path resumes in-flight runs.
let mut rt = SqliteRuntime::open("workflows.db")?;

// Ephemeral: a fresh, empty, private database each call. Great for tests/demos.
let mut rt = SqliteRuntime::open_in_memory()?;
```

- Use **`open(path)`** for anything that must survive a process restart. Opening
  applies the schema idempotently and reclaims any task stranded `RUNNING` by a
  previous crash (see [§9](#9-durability-and-crash-recovery)).
- Use **`open_in_memory()`** for tests, demos, and throwaway runs. Each call is a
  brand-new database and is **not** reopen-safe — dropping the runtime discards
  all state.

Both return `SqliteResult<SqliteRuntime>` and are synchronous (only the *drivers*
are async).

---

## 5. Registering workflows and activities

Registrations live in process memory, not the database, so **every runtime
(including a reopened one) must re-register** its handlers.

```rust
rt.register_workflow(&process_order_info());

rt.register_activity(&reserve_inventory_info(), |input| {
    let order: Order = serde_json::from_value(input).map_err(|e| e.to_string())?;
    Ok(serde_json::json!(format!("reserved {}x {}", order.qty, order.item)))
});
```

- **`register_workflow(&WorkflowInfo)`** retains only the workflow's `name` +
  `handler`. If the `#[workflow(...)]` attributes declare an
  execution/admission-affecting feature this v0.1 backend cannot honor
  (`execution_timeout`, a workflow-level `retry_policy`, `concurrency`,
  `debounce`, `batch`, `throttle`, a raised `max_input_bytes`), registration
  **panics at setup time** naming the feature — never a silently-dropped setting.
- **`register_activity(&ActivityInfo, body)`** is the audited path: it honors the
  declared `#[activity(...)]` defaults it can (`schedule_to_close`, the full
  `retry` policy — `max_attempts` + backoff + non-retryable classification —
  `start_to_close`, `queue`) and **panics** for one it cannot
  (`heartbeat_timeout`, `schedule_to_start`, `circuit_breaker`, `rate_limit_*`,
  `max_concurrent`, `is_local`, …). This keeps a declared setting from taking
  silently-wrong effect.
- **`register_activity_raw(name, ActivitySpec::new(max_attempts, body))`** is the
  escape hatch — a hand-made body that carries no `ActivityInfo` and runs no
  audit. Use it when you don't have (or don't want) the macro companion:

```rust
use autumn_harvest_sqlite::ActivitySpec;

rt.register_activity_raw(
    "reserve_inventory",
    ActivitySpec::new(3 /* max_attempts */, |input| {
        Ok(serde_json::json!(format!("reserved {input}")))
    }),
);
```

---

## 6. Starting a workflow

```rust
let exec = rt.start_workflow("process_order", serde_json::json!({
    "id": "ORD-1001", "item": "widget", "qty": 3
}))?;
```

`start_workflow(name, input)` appends the `WorkflowStarted` event and returns the
run's `ExecutionId` immediately — **nothing has executed yet**; you drive it in
step 7.

`start_workflow_with_id(name, workflow_id, input)` lets you supply a business
`workflow_id` (what `ctx.info().workflow_id` reports — idempotency-key material,
cross-backend-identical).

> **v0.1 non-idempotent-start contract.** This backend does **not** enforce
> `(workflow_name, workflow_id)` uniqueness and does **not** apply the core's
> `WorkflowIdReusePolicy` matrix. **Every call creates a new, independent
> execution**, even when `workflow_id` matches a prior run — so a duplicate
> delivery (e.g. a retried webhook) starts a second run and repeats its side
> effects. The `workflow_id` is observability + idempotency-key *material*, not
> an enforced start-boundary uniqueness key. **Dedupe upstream for now.** The
> reuse-policy matrix is a tracked follow-up (issue #1068).

An oversized start input (over the 2 MiB default cap) is rejected with
`SqliteError::PayloadTooLarge` before anything is persisted, matching the core.

---

## 7. The drive model

SQLite has no push notification, so instead of the Postgres worker's
`LISTEN`/`NOTIFY` loop you **drive** the runtime. A drive is a polling loop: drain
all ready tasks, re-run the workflow (replay recorded history, then execute new
commands), persist the resulting events, and repeat until the run is terminal or
blocked on external input. All three drivers are `async`.

| Driver | Scope | Stops when | Returns |
| --- | --- | --- | --- |
| `poll_once().await` | Every running execution, **one cycle each** | after a single pass | `bool` — did anything make progress? |
| `run_until_blocked(exec).await` | **One** execution | that run is terminal or blocked | `RunState` |
| `run_until_idle().await` | **Every** execution, repeatedly | the whole fleet quiesces | `()` |

```rust
use autumn_harvest_sqlite::{RunState, ExecutionOutcome};

// Drive a single run to its next stopping point:
match rt.run_until_blocked(exec).await? {
    RunState::Completed(output) => println!("done: {output}"),
    RunState::Failed(err)       => println!("failed: {err}"),
    RunState::WaitingSignal(name) => println!("blocked on signal `{name}`"),
    RunState::WaitingTimer      => println!("blocked on a durable timer"),
    RunState::InProgress        => {} // not returned by run_until_blocked
}

// Or drive the whole fleet to quiescence, then read a stored outcome:
rt.run_until_idle().await?;
match rt.outcome(exec)? {
    ExecutionOutcome::Completed(output) => println!("{output}"),
    ExecutionOutcome::Failed(err)       => println!("{err}"),
    ExecutionOutcome::Running           => println!("still running"),
}
```

**Which to use:**

- **`run_until_blocked(exec)`** when you care about one specific run and want its
  `RunState` (including *why* it blocked). This is the natural choice right after
  a `start_workflow` or a `send_signal`.
- **`run_until_idle()`** to settle everything — the simplest "process all
  pending work now" call. Follow it with `outcome(exec)` for a pure read.
- **`poll_once()`** for a custom loop — e.g. a background tick where you decide
  the cadence and inspect the `bool` progress flag yourself.

`outcome(exec)`, `load_history(exec)`, and `activity_attempts(exec, name)` are
**pure reads** — they never advance a run.

Timers use the real wall clock (read once per decision cycle), so a run blocked
on a `ctx.timer(...)` that is not yet due returns `WaitingTimer`; drive it again
after the deadline passes and it proceeds.

---

## 8. Signals (pull-only)

Deliver a signal from outside with `send_signal`; consume it inside the workflow
with a **pull** primitive.

```rust
// Inside the workflow — block until the signal arrives:
let approved: bool = ctx.receive_signal("approve").await.map_err(|e| e.to_string())?;
// (or the untyped `ctx.wait_for_signal("approve")` returning a serde_json::Value)

// From outside — stage the signal, then drive the run forward:
rt.send_signal(exec, "approve", serde_json::json!(true))?;
rt.run_until_blocked(exec).await?;
```

A staged signal is appended to history (as `SignalReceived`) **only when a
workflow reaches a pull primitive that consumes it** — this backend is
pull-only. The supported pull surface is the plain waits
(`wait_for_signal` / `receive_signal`) and the signal-or-deadline waits
(`wait_for_signal_timeout` / `receive_signal_timeout`, which race the signal
against a durable deadline timer).

`send_signal` validates the target at the boundary: an unknown execution is
`SqliteError::ExecutionNotFound` and a terminal one is
`SqliteError::WorkflowNotRunning`, so a wakeup no live wait could ever consume is
rejected rather than silently lost.

> Push-based signal *handlers* (`register_signal_handler`) and the non-blocking
> drain APIs (`drain_signals` / `try_receive_signal`) depend on the Postgres
> task-preparation ingest and are **not** available here — restrict to the pull
> primitives.

---

## 9. Durability and crash recovery

All durable state lives in the database. Dropping a runtime and re-`open`-ing the
**same file** is a faithful crash/restart:

1. `open(path)` applies the schema and **reclaims any orphaned `RUNNING` task** —
   under the single-writer assumption, a `RUNNING` row at startup was claimed by
   a process that exited without finalizing, so it is flipped back to `PENDING`.
2. Re-register your handlers (registrations are in process memory, not the file).
3. Drive the run — it **resumes purely by deterministic replay** of the recorded
   history. Completed steps are replayed from history, not re-executed; only work
   scheduled after the last durable event runs fresh.

This makes activity execution **at-least-once**: a crash *after a body runs but
before its result commits* re-runs that body on resume. **Write activity bodies
to be idempotent.**

The [`examples/durability.rs`](../autumn-harvest-sqlite/examples/durability.rs)
example proves resume-vs-restart with side-effect counters: it runs a workflow to
a signal block, **drops the runtime**, reopens the same file, delivers the signal,
and finishes — showing the reserve body ran exactly once (session 1; replayed
from history in session 2) and the ship body ran exactly once (session 2 only).

---

## 10. The single-writer / single-server contract

This backend assumes **one writer process** against **one** SQLite file:

- **`BEGIN IMMEDIATE` replaces `SELECT … FOR UPDATE SKIP LOCKED`.** A task claim
  takes SQLite's database-level write lock up front, selects the oldest ready
  task, and flips it to `RUNNING`. Under the single-writer assumption this is
  exactly-once by construction — no two claimers race.
- **Polling replaces `LISTEN`/`NOTIFY`** (see [§7](#7-the-drive-model)).
- **Restart = drop the runtime + reopen the file** (see
  [§9](#9-durability-and-crash-recovery)).

The runtime opens the database with `journal_mode = WAL`, `synchronous = FULL`
(fsync on every commit — the crash tests depend on it), and a `busy_timeout` so
an occasional external *reader* (a monitoring/inspector connection) can coexist.

**Do not** point two writer processes at the same file, and **do not** use this
backend as a shared multi-server queue. The only residual crash window is *mid
activity body* — a crash while a body is executing leaves the task `RUNNING` and
is recovered (re-running the body, at-least-once) by the orphan reclaim on the
next `open`. A single-process design has no heartbeat-timeout reclaimer for a
*live* peer, by design; multi-writer / multi-server recovery is out of scope.

---

## 11. v0.1 non-goals and follow-ups

Out-of-subset workflow primitives are rejected **loudly, by name** — never
silently dropped. A workflow reaching one of these surfaces
`SqliteError::Unsupported` (or a setup-time panic at registration) naming the
specific command/feature:

- **Child workflows** (`spawn_child_workflow`, `spawn_child_workflow_detached`).
- **External signals / cancels** (`signal_external_workflow`,
  `request_cancel_external_workflow`).
- **Local activities** (`execute_local_activity`) and **external / task-token
  activities**.
- **Updates**, **search attributes** (`upsert_search_attributes`), and
  **`continue_as_new`**.
- **Worker sessions** (`create_session`) and **cancellable durable timers**
  (`start_timer` / `TimerHandle::…` — use the fire-once `ctx.timer(...)`).

Backend-level non-goals: distributed / multi-writer workers, `LISTEN`/`NOTIFY`
push wake-ups, multi-server crash recovery, schedules, the management API,
retention, worker sessions, sharding, DAGs, and the `WorkflowIdReusePolicy`
matrix (see [§6](#6-starting-a-workflow)). These are tracked as issue #1068
follow-ups — rejection is deliberate, so a partial, silently-wrong implementation
never ships.

Two benign bookkeeping commands are silently no-ops (they append no event and
gate no control flow): `ctx.set_current_details(...)` and a re-park
`WaitForActivity`.

---

## 12. Runnable examples

- **[`examples/quickstart.rs`](../autumn-harvest-sqlite/examples/quickstart.rs)**
  — a complete order-processing workflow: open in-memory → register a workflow +
  two activities → start → `run_until_idle` → print the terminal outcome.

  ```text
  cargo run -p autumn-harvest-sqlite --example quickstart
  ```

- **[`examples/durability.rs`](../autumn-harvest-sqlite/examples/durability.rs)**
  — crash recovery: drive to a signal block, drop the runtime, reopen the same
  file, deliver the signal, resume to completion — proving the run *resumed* (via
  replay) rather than *restarted*.

  ```text
  cargo run -p autumn-harvest-sqlite --example durability
  ```

- **[`examples/claude-agent-daemon/`](../examples/claude-agent-daemon/)** — a
  whole application on this backend: a local daemon that runs Claude agent
  sessions as durable workflows. It shows the drive loop
  ([§7](#7-the-drive-model)), pull signals with a deadline
  ([§8](#8-signals-pull-only)), and crash recovery
  ([§9](#9-durability-and-crash-recovery)) in one place, and it runs with no API
  key against a scripted offline model.

  ```text
  cargo run -p claude-agent-daemon -- serve --workspace /tmp/agent-demo
  ```

For the full API/contract reference, run
`cargo doc --open -p autumn-harvest-sqlite`.
