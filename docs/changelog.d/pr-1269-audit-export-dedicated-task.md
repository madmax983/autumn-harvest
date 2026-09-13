## Fix — audit export runs on its own task (issue #1269)

Follow-up from the review of #1261 (issue #953). `fire_due_audit_exports` ran
inline inside `timeout::enforce_timeouts_once`, sharing that loop's
connection and cadence with timeout enforcement, SLA checks, session
cleanup, and codec rotation. The sink call was bounded by the claim lease,
but a bound is not cadence: a slow or blackholed sink delayed every other
resident of the loop for up to one lease, and on the multi-shard fan-out
arm a `max_size(1)` shard pool could never export at all, because the
export call asked the pool for a second connection while the checker's own
was still held.

Both hazards were coupling problems, not bugs in the claim/deliver/ack
pipeline itself, so the fix is architectural: give export its own task.

- **New `spawn_audit_export_checker_for_shard`** (`audit_export.rs`): one
  dedicated task per assigned shard, on the worker's existing poll
  interval, registered under the new `Scanner::AuditExport` liveness label.
  It owns its connection lifecycle end to end — never nested inside
  another resident's checkout — so a slow sink delays nothing but its own
  next tick, and a one-connection shard pool works: the export task and the
  timeout checker take turns on the pool instead of one needing a second
  connection while holding the first. A cheap `is_configured()` check skips
  the connection checkout entirely on an unconfigured deployment (AC8).
- **`enforce_timeouts_once` no longer calls `fire_due_audit_exports`.** The
  function, its claim/deliver/ack pipeline, and its `pub` primitive shape
  are unchanged — an embedder driving it by hand still works exactly as
  before — only the caller inside core's own scanner loop is gone.
- **`Scanner::AuditExport`** added to the bounded `scanner` label set
  (`scanner_health.rs`), bringing the total to eight labels / six spawned
  loops.
- Removed `timeout::mark_audit_export_unobserved_for_checker_shard` and its
  two call sites: the timeout checker's own connection failures no longer
  have anything to do with audit export's observability, since the two are
  independent tasks now. The export task marks its own shard unobserved on
  its own connection failure instead.
- **Test evidence:** `audit_export_tests.rs` adds
  `enforce_timeouts_once_no_longer_exports_audit_records` (decoupling),
  `spawn_audit_export_checker_for_shard_exports_independently`, and
  `a_dedicated_export_task_still_exports_on_a_size_one_pool_shared_with_the_timeout_checker`
  (the held-connection regression this issue exists to fix), plus the
  `AuditExport` scanner's own connection-failure-marks-unobserved pair
  (replacing the two tests that pinned the old, now-removed coupling).
  `scanner_liveness_tests.rs` extends the bounded label-set and
  spawn-site-ownership guards to the new variant.
