## Fix — audit-export unobservable-shard gauge (issue #1268)

Follow-up from the review of #1261 (issue #953). `harvest.audit.export_lag`
is written only on a successful observation, so a shard the exporter cannot
observe — a connection it cannot acquire, a failing cursor read, a failing
lag query — kept serving its last value, commonly `0`. Neither the threshold
alert nor the `absent()` companion could see that: a rising line never
happened, and the series never disappeared.

- **New gauge `harvest.audit.export_observed{shard}`.** `1` when the
  exporter read a shard's cursor and lag this tick, `0` otherwise. Emitted
  on every tick that reaches a shard, delivery outcome aside, mirroring the
  existing `harvest.replication.observable` pattern from issue #954. The
  lag gauge itself is left untouched on failure, deliberately: this is an
  availability signal, not a fabricated lag reading.
- **Every skip path in `fire_due_audit_exports` now reports `false`**: an
  unmapped shard pool, a bounded connection-acquire failure or timeout, and
  a propagated error from `export_once_on_conn` all mark the shard
  unobserved before moving on to the next one.
- **New alert `harvest_audit_export_unobservable`** (ticket severity) and
  runbook section, plus a new dashboard panel. Updates
  `harvest_audit_export_lag_high`'s own notes and the runbook's coverage
  table, which previously said this case had no metric-only detection.
- **Test evidence:** `audit_export_tests.rs` adds
  `a_shard_the_scanner_cannot_acquire_a_connection_for_is_marked_unobserved`
  and extends three existing tests (idle tick, successful delivery, failing
  sink) to assert `export_observed` reports `true` exactly when the shard
  was actually readable.
