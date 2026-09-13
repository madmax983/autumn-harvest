## Phase 5.x — a coherent audit-export cursor lifecycle: close a revival race, audit retire/reactivate (issue #1273)

Two deferred findings from #1261's review (issue #953's audit-export cursor),
filed together because they are two symptoms of the same under-designed
mechanism: `decommission_cursor` retired a shard's export cursor by setting
`retired_at`, but the states a cursor can be in, who may transition it, and
what gets recorded were never written down.

- **Finding 1 (P1) fixed: a racing scanner pass can no longer revive a
  retired cursor.** `ensure_cursor_row`'s `ON CONFLICT DO UPDATE` used to
  clear `retired_at` unconditionally on every scanner tick, so a tick already
  under way when an operator retired a shard could silently un-retire it
  moments later. The `DO UPDATE` is now guarded by `WHERE retired_at IS
  NULL`, evaluated by Postgres under the same row lock that resolves the
  conflict — race-free by construction, not by convention. A retired cursor
  is now a complete no-op for `ensure_cursor_row`: no heartbeat, no
  un-retire.
- **Finding 2 (P2) fixed: retirement (and reactivation) are audited.**
  `decommission_cursor` was a bare library call with no route, no admin
  gate, and no audit trail — the one action that discards unexported audit
  records had no record of who authorised it. Two new admin routes,
  `POST /admin/audit-export/decommission` and `POST /admin/audit-export/reactivate`,
  are admin-gated and audited (`audit_export.decommission`,
  `audit_export.reactivate`) in the same one-transaction-one-connection
  shape as the existing redrive route: the mutation and its audit row commit
  together, on the target shard's own connection, so an
  applied-but-unaudited transition is not representable.
- **Reactivation is now explicit, not a side effect.** Resuming export used
  to happen implicitly on the next scanner tick after a re-enable — the same
  mechanism finding 1 closes. `POST /admin/audit-export/reactivate` is now
  the only way back from `RETIRED` to live, resuming from the preserved
  `last_assigned_seq` exactly as before, just as an explicit, audited
  operator action instead of an inferred one.
- **No migration.** `retired_at` and `claim_epoch` already existed on
  `harvest_audit_export_cursor`; the fix is a predicate on an existing
  `ON CONFLICT` statement plus two new locked-transaction functions
  (`decommission_cursor_locked`, `reactivate_cursor_locked`) reusing the
  existing columns.
- **Test evidence:** `autumn-harvest/tests/integration/audit_export_tests.rs`
  gained direct coverage for both findings —
  `ensure_cursor_row_never_revives_a_retired_cursor` and
  `a_scanner_tick_after_decommission_does_not_revive_the_cursor` pin finding
  1 at both the unit and scanner-tick level;
  `a_decommission_and_its_audit_record_land_together_on_the_target_shard`
  and its reactivate counterpart pin finding 2; idempotency
  (`AlreadyRetired`/`AlreadyActive`) and unconfigured-shard cases are
  covered separately. Two existing tests that relied on the old implicit
  reactivation (`a_recreated_cursor_continues_the_sequence_it_left_off_at`,
  `the_sequence_survives_a_decommission_that_purges_every_stamped_row`) were
  updated to call the new explicit `reactivate_cursor`.
