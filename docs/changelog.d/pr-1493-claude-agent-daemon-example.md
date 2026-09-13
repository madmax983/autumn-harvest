## Phase — Claude agent-daemon example on the SQLite backend (PR #1493)

A new top-level example, `examples/claude-agent-daemon/`: a local daemon
(`agentd`) that runs Claude agent sessions as **durable workflows** on the
embedded `autumn-harvest-sqlite` backend. No Postgres, no Docker, one binary
and one file.

An agent loop is exactly the workload that cannot be fire-and-forget: turns are
expensive, tools touch the real world, and a session can sit for minutes
waiting on a human. The example shows what the engine gives that loop — a
restart resumes by replay instead of re-paying for completed turns, a tool
parks on a durable signal with a deadline rather than an in-memory future, a
rate limit is an activity retry, and the event log is the audit trail
(`agentd history <id>`).

**The loop is the workflow** (`src/session.rs`). Ordinary Rust — a `for` loop
over turns, one model call, one tool call per `tool_use` block. The transcript
is rebuilt from activity results on every replay, so it is a projection of
history rather than daemon state.

- `claude_turn` — one Messages API request (`claude-opus-5`, adaptive thinking,
  no server-side fallbacks), `start_to_close = 15m`, exponential retry. A
  429 or a 5xx keeps the plain error string so the policy retries; a rejected
  request returns a typed non-retryable `ActivityFailure`, so bad bytes never
  burn the retry curve. The assistant content blocks are stored and replayed
  **verbatim**, which keeps thinking blocks valid across turns on one model.
- `run_tool` — `list_files`, `read_file`, `write_file`, each confined to one
  workspace directory. The confinement is lexical, plus a refusal of a symbolic
  link at the final component, plus a resolution of the deepest existing
  ancestor that must stay under the real root — so it holds for a path that does
  not exist yet. A tool failure is a `tool_result`, not an activity error, so
  the model can recover from it.
- `write_file` parks on one occurrence of the `tool_approval` signal with a
  durable deadline (`receive_signal_timeout`), so an unattended session denies
  the call and continues. The signal name carries the turn, the position in that
  turn and the tool-use id, and the daemon requires that whole name as the
  approval token, so a decision releases only the wait the operator read.
- A write lands whole or not at all: a scratch file on a unique name, given the
  target's mode, with the file and every directory up to the workspace root
  flushed before the rename is reported as done.

**The daemon is the single writer** (`src/daemon.rs`): a Unix-socket control
surface, a drive tick in place of `LISTEN`/`NOTIFY`, and a second READ-ONLY
connection (`src/inspect.rs`) for the session listing the runtime does not
expose. The main loop selects between a control command and the tick, both
taking the runtime mutably — the single-writer contract made visible.

**No API key is required.** Without `ANTHROPIC_API_KEY` the daemon registers a
scripted offline stub model that drives the same loop, so the durability story
is demonstrable and testable with no key and no network.

**Engine invariants:** none touched. No new `WorkflowEvent` variant, no
migration, no change to any published crate — the example is a workspace
member with `publish = false` and uses only the existing public surface of
`autumn-harvest` (`default-features = false`) and `autumn-harvest-sqlite`.

**Test evidence:** `cargo test -p claude-agent-daemon` — an offline suite that
needs no key and no network. It covers the happy path, a denied tool call, and
the restart proof (asserted by counting model calls in each process, so a
replayed turn provably never reaches the model). It also covers the workspace
sandbox, the session listing and history read back from the event log, the
request and response size caps, and one end-to-end run through the daemon
socket. CI gains a clippy step and a Linux-only test step (issue #962: a new
workspace member gets no coverage without its own steps; the control surface
is a Unix domain socket, hence the OS gate).

Cross-linked from the root `README.md`, `docs/sqlite-backend.md` §12, and the
backend crate README.
