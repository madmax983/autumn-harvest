## Phase — audit-export redrive reports what it can actually deliver (issue #1267)

`POST /admin/audit-export/redrive` rewinds a shard's export cursor under
`SELECT … FOR UPDATE` on the cursor row. `audit::purge_old_audit_records`
does not take that lock — it reads the cursor through a plain, uncorrelated
subquery.

A retention sweep can race a redrive: it reads the old, higher
`last_acked_seq`, purges records the redrive is about to promise back, and
the redrive still commits its lower cursor. The `200` response then claimed
full recovery of a window retention had already narrowed. Filed as a P2
follow-up to issue #953 (PR #1261, Codex review round 7) — the bug is in the
report, not in data loss beyond what retention already permitted.

Fix: the redrive now counts, inside the same transaction as the rewind, how
many `(to, from]` records still exist. `count_redrive_recoverable` is the
new primitive in `autumn_harvest::audit_export`; `redrive_recovery_counts`
wraps it over a `RewindOutcome` and returns `(0, 0)` for a refused rewind
(`NoOp`/`NotConfigured`), since a rewind that moved nothing has no window to
measure. The `POST /admin/audit-export/redrive` handler calls
`redrive_recovery_counts` right after `rewind_cursor_locked` and adds two
fields to the response:

- `recoverable_records` — records in the window that still exist and will
  re-export.
- `already_purged_records` — the rest of the window, gone before this
  redrive could reach it. `0` on the common path.

The `audit_export.redrive` audit record also names the gap when
`already_purged_records > 0`, so the compliance trail matches the API
response.

**The handler's transaction is now pinned to `READ COMMITTED`**
(`conn.build_transaction().read_committed()`, matching `queue::claim_task`,
`activity_pause`, `queue_pause`, the timeout enforcer, and the scheduler).
The recoverable-records count depends on seeing a purge that commits after
the transaction's first statement (the cursor's `FOR UPDATE`). An inherited
`REPEATABLE READ` (or `SERIALIZABLE`) session default — settable outside
this code, at the database or role — would instead hand every statement one
shared snapshot from that first `FOR UPDATE`, hiding a purge that committed
just after it and silently reporting full recovery of records already gone.
Caught in review (correctness pass): the count itself was right, but nothing
pinned the isolation level it depends on.

`RewindOutcome` is now `Copy` (all fields are `i64`), so `rewind_cursor`'s
and `rewind_cursor_locked`'s existing callers are unaffected and the new
plumbing does not need to clone it.

This closes the false-success report. It does not add locking to the
retention path — the issue named that as a separate possible follow-up,
worth measuring on a large audit table before committing to it.

No new `WorkflowEvent` variant, no migration — a read/report addition to
the existing redrive transaction.

Tests (`autumn-harvest/tests/integration/audit_export_tests.rs`, integration,
real Postgres):
- `redrive_recoverable_count_matches_the_full_window_when_nothing_was_purged`
  — no purge, no gap: the full window is recoverable, `already_purged_records`
  is `0`.
- `redrive_recovery_counts_is_zero_for_a_refused_rewind` — `NoOp` and
  `NotConfigured` both report `(0, 0)` without touching `harvest_audit_log`.
- `redrive_recoverable_count_falls_short_when_a_purge_already_removed_part_of_the_window`
  — reproduces the race's end state directly: purge deletes the aged,
  acknowledged records under the stale cursor before the redrive runs, and
  the redrive reports exactly the 2 survivors and 3 already-purged records,
  not the 5-record window it was asked for.

All three drive `redrive_recovery_counts` on the actual `RewindOutcome` a
prior call returned, rather than re-typing `(from, to)` by hand, so an
argument-order mistake in the handler's own call site would fail them too.

Also: `docs/api-contract.json`, `management_api_response_fields()`, and both
`openapi.json` copies updated and regenerated
(`scripts/regenerate-openapi.sh`); `contract_regression` and `openapi_spec`
suites pass. Fixed a pre-existing run-on sentence in the redrive route's
`success_response.notes` while it was already being edited.

**Known limitation, documented and filed rather than fixed here (issue
#1508):** `already_purged_records` is exact for a `to_seq` rewind, where `to`
is the operator's own number. For a `before` rewind, `to` is derived from the
lowest *surviving* record at or after the given instant, so an already-purged
prefix is invisible to both the resolver and to `already_purged_records` —
closing that needs a persisted purge watermark, since nothing in
`harvest_audit_log` records that a purged row ever existed. Caught in review
(Codex); documented in code (`audit_export.rs`, the `Before` branch of
`rewind_cursor_locked` and the `redrive_recovery_counts` doc comment),
`docs/audit-export.md`, and `docs/api-contract.json`.
