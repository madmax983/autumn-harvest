## Phase 3.x — A redriven run re-dispatches past its failing cycle's trailing records (issue #1262)

**Bug fix**, composing only existing events — **zero new `WorkflowEvent`
variants, no migration, no contract change.**

### The bug

A DLQ redrive (#510) reopens a `FAILED` run so the re-enqueued task
re-issues the work its failing cycle abandoned. Issue #952 already made an
abandoned-dispatch pair (`ActivityScheduled`/`ChildWorkflowStarted` plus its
synthetic `ABANDONED_DISPATCH_REASON` terminal) transparent within the
pre-redrive prefix. But when the failing cycle recorded **any other**
pre-terminal record after that dispatch — a `ctx.version()` /
`ctx.patched()` / `side_effect()` marker, most commonly — the matcher
cursor landed on that record and reported `Diverged` instead of `NoMatch`.
The redrive nd-blocked instead of re-dispatching, reproducing with or
without #952's abandoned-dispatch pairs present.

### The fix

`HistoryMatcher::superseded_cycle_tail_indices` (`replay.rs`) walks
backward from the last `WorkflowRedriven`, over the same pre-redrive prefix
`abandoned_dispatch_indices` already scans, swallowing a trailing run of
every event kind `worker::terminal_command_policy` classifies
`PreTerminalEvent` — `MarkerRecorded`, `SideEffectRecorded`,
`ChildWorkflowSpawnedDetached`, `TimerStarted`, `TimerCancelled` — plus
anything the caller already marked transparent (an abandoned pair, the
superseded `WorkflowFailed`).

**Bounded correctly.** A decision cycle's events are contiguous: no
durable wait resolves mid-cycle, only between cycles. The walk stops at
the first event that is not already transparent and not one of the
recognized kinds — a settled completion (`ActivityCompleted`,
`TimerFired`, ...) marks the boundary of an earlier, non-superseded cycle.
That cycle's own `version()`/`patched()` markers stay positionally
matchable, so #687/#603 determinism for a cycle the redrive never touched
is unaffected. Verified end to end: a redriven run whose swallowed marker
recorded `version:gate = 1` re-derives the gate live and lands on `max`,
rather than reading the stale value back
(`a_redriven_run_re_derives_a_version_gate_past_the_swallowed_marker`).

**Root-cause fix folded in.** Chasing the same bug through a
retry-then-redrive history surfaced a second, independent gap: a
workflow-level retry's `WorkflowRetryScheduled` (#523) — or a parent-close
cascade's `ChildWorkflowCascadeApplied` (#347) — sitting between the
superseded terminal and the redrive was never itself marked transparent.
The existing backward scan for the superseded `WorkflowFailed` skipped
*past* it while searching, but never marked it consumed, so the cursor got
stuck on the bookkeeping event itself — with or without a trailing marker,
and independent of this fix. Both event kinds carry no workflow command
and are never consumed by the workflow function, so `HistoryMatcher::new`
now marks them transparent unconditionally, the same way it already
treats pause/resume (#383), rather than only while a redrive searches
behind them.

### Tests (TDD red → green → refactor)

* `replay.rs`: matcher-level tests reproducing both of the issue's repro
  cases (an abandoned pair followed by a marker; a bare marker with no
  abandoned pair at all) confirmed failing pre-fix. Plus: a negative
  control that an earlier, non-superseded cycle's `version()` marker
  stays both opaque *and* still answers `match_version` with its recorded
  value (a silent value flip would be worse than a divergence —
  `match_version` never reports `Diverged`); `SideEffectRecorded`,
  stacked markers, a marker sandwiched between two abandoned pairs, an
  abandoned *activity* dispatch variant, a marker written after the last
  of two redrives (must stay opaque); the retry/cascade bookkeeping gap in
  isolation and combined with a trailing marker; and `TimerStarted` /
  `TimerCancelled` / `ChildWorkflowSpawnedDetached` in the trailing run.
* `replayer_tests.rs`: an integration-level redrive-with-trailing-marker
  test, plus the end-to-end version-gate re-derivation test described
  above.
