## Phase — Quota-vs-supersede ordering and a detached-child lock-ordering gap (issue #1228)

A seventh automated Codex review round on PR #1221 (issue #946, per-tenant
resource quotas) surfaced two more findings against commit `3bda6fc`, after
all 5 permitted review rounds for that PR were used and round 6's findings
were already tracked in #1227. Filed separately from #1227: these two are
about admission ORDER, not a missing retry backoff.

**Finding 1 (correctness) — `autumn-harvest/src/execution.rs`.** On the
plain fresh-insert admission path,
`start_or_load_workflow_execution_collect` ran `enforce_quota_admission`
BEFORE the issue #811 latest-wins supersede pass
(`run_latest_wins_supersede`). Quota admission counts every non-terminal
row sharing the resolved key, including an incumbent the supersede pass is
about to cancel a few lines later. A workflow declaring both a tight
`max_active_executions` quota cap and `on_conflict = "cancel_running"` on
the SAME key — a natural pairing: "at most one active run per tenant, and a
newer request replaces a stale one" — hit the cap on the incumbent's own
occupancy and was rejected with `QuotaExceeded` before supersede ever ran,
silently defeating `cancel_running` for exactly that configuration.

Fix, attempt 1 (reverted): the issue's own suggested fix — run
`run_latest_wins_supersede` before `enforce_quota_admission` on this path —
regresses `concurrency_supersede_tests::nested_self_referential_admission_
emits_residual_over_limit_counter`. A self-referential completion trigger
can start a NESTED admission synchronously, inside the cancellation this
pass performs. That nested admission's own candidate scan requires a
`harvest_task_queue` row to count the OUTER admission as a protected,
in-flight run (`active_runs_for_key`'s "Protected in-flight admissions"
note) — a row this pass's caller does not insert until AFTER `append_events`
and `queue::enqueue`. Moving supersede earlier runs it before that task row
exists, so the nested admission's scan never sees the outer admission as
protected, undercounting the shed target and silently dropping the residual
metric this test pins.

Fix, as shipped: a dry-run credit instead of a reorder. New
`crate::concurrency::dry_run_supersede_shed_count` takes the same
`lock_concurrency_key` and reads how many runs the pass WOULD shed right
now, without cancelling anything. `start_or_load_workflow_execution_collect`
calls it before `enforce_quota_admission`, which gained a
`pending_supersede_credit: u64` parameter subtracted from
`active_executions` alongside the pre-existing "subtract 1 for our own row"
adjustment. The real `run_latest_wins_supersede` call keeps its original
position, unchanged, after `append_events`/`queue::enqueue` — so the
nested-admission invariant above still holds. The advisory lock the dry run
takes is transaction-scoped and re-entrant, so the later real pass simply
re-acquires it; no new lock order, no double-counting. Every OTHER
`enforce_quota_admission` call site (three in `worker.rs`, one in
`cross_shard_child.rs`, one in `replace_execution`) passes `0` for the new
parameter — byte-for-byte unaffected.

**Finding 2 (correctness / liveness) — `autumn-harvest/src/worker.rs`.**
The round-5 fix for the AWAITED-child fan-out
(`persist_all_started_child_workflows`) pre-acquires every distinct
`(workflow_name, quota_key)` advisory lock this fan-out needs, in one
sorted order, before inserting any child row — closing an ABBA deadlock
where two concurrent parents fanning out to the same quota keys in
opposite command order could each hold one key while waiting on the other.
`create_detached_child_executions`, called earlier in the same enclosing
function, had no equivalent step: it called `enforce_quota_admission` (and
so `lock_quota_key`) once per detached child, in raw command order, inside
a single loop. Two parents each spawning two detached children under keys
`A` and `B`, in opposite order, could deadlock the identical way. Postgres
aborts one side with a raw `deadlock_detected` error — not
`HarvestError::QuotaExceeded`, so `recover_from_child_quota_exceeded`'s
existing catch never sees it, and it terminally fails an otherwise-healthy
parent over a transient conflict.

Fix: `create_detached_child_executions` now collects the distinct
`(workflow_name, quota_key)` pairs across every local (non-cross-shard)
`SpawnDetachedChildWorkflow` command whose target declares a capped,
resolvable quota, into a `BTreeSet`, and locks them all in that sorted
order before the per-child insert loop runs — mirroring the awaited-child
fix exactly. `lock_quota_key`'s `pg_advisory_xact_lock` is re-entrant
within one transaction, so the loop's own `enforce_quota_admission` call
simply re-acquires what is already held. A cross-shard child is excluded
(its own shard locks it, via the relay) and an already-created child
(crash-restart replay) is excluded via a best-effort snapshot query — one
batched lookup, skipped entirely when a decision cycle has no detached
spawns at all (zero default overhead). That snapshot is for lock planning
only: the per-child insert loop's own idempotency check stays a fresh
per-row query, unchanged, so an in-batch duplicate `child_id` the snapshot
might miss is still caught before a duplicate insert.

No new `WorkflowEvent` variant, no migration, no replay impact — both are
pure admission-ordering fixes.

**Tests, red → green.** Finding 1: new
`autumn-harvest/tests/integration/quota_supersede_ordering_tests.rs` drives
the real `start_or_load_workflow_execution` entry point with a
`max_active_executions = 1` quota and `on_conflict = "cancel_running"`
declared on the same key —
`cancel_running_supersede_wins_over_a_tight_quota_cap_on_the_same_key`
(the money test: second admission must cancel the incumbent and succeed,
never reject with `QuotaExceeded`),
`cancel_running_supersede_chain_never_trips_the_quota_cap` (ten admissions
in a row, each superseding the last), and
`defer_policy_still_enforces_the_quota_cap_unchanged` (a `Defer` policy
declares no supersede, so the cap must still reject the 2nd admission, byte
for byte as before). The money test and the chain test both fail against
unmodified `execution.rs` (confirmed by stashing the fix and re-running:
both reject with `QuotaExceeded` instead of superseding) and pass with the
fix restored. Re-running `concurrency_supersede_tests.rs`'s full issue #811
suite after the fix caught attempt 1's regression
(`nested_self_referential_admission_emits_residual_over_limit_counter`)
before it shipped; that suite is green against the dry-run-credit fix, and
was also confirmed green against unmodified `execution.rs` (so the fix
introduces no NEW dependency the suite would need to guard).

A follow-up multi-angle review (three independent agents: concurrency
correctness, test coverage, comment style) flagged that every Finding-1
test above shed exactly one incumbent, never proving the credit
generalizes past a single-incumbent shed. Added
`cancel_running_supersede_credit_generalizes_past_a_single_incumbent`:
`concurrency_limit = 2`, two admitted runs, then a THIRD admission that
must shed exactly the OLDEST of the two down to the limit -- pinning both
"shed more than one" and "shed the right one" in a single case.

Finding 2: `create_detached_child_executions` is private, so it cannot be
driven directly from an integration test. New
`autumn-harvest/tests/integration/quota_lock_ordering_tests.rs` instead
exercises `autumn_harvest::quota::lock_quota_key` — the exact advisory-lock
primitive both fan-outs call — under a deterministic, manually-sequenced
two-connection interleaving (not timing-dependent): two connections locking
the SAME two keys in OPPOSITE order are forced into a genuine ABBA cycle
and Postgres aborts exactly one side with a `deadlock_detected` error
(`opposite_order_lock_acquisition_deadlocks`); two connections locking the
SAME two keys in the SAME (sorted) order never deadlock even under real
concurrency — the later one simply blocks and then proceeds
(`same_order_lock_acquisition_never_deadlocks`). Together these pin the
exact property the `BTreeSet` pre-acquisition pass relies on. The existing
`quota_enforcement_tests.rs` detached-child coverage
(`detached_child_spawn_honors_target_quota_parks_parent_then_succeeds` and
its mixed-batch sibling) and `concurrency_supersede_tests.rs`'s full issue
#811 suite were re-run against the refactored function and stay green,
guarding against a regression in the existing per-child/per-key behavior.

The same review round flagged that both cited detached-child tests spawn
exactly ONE child per batch, so the new `BTreeSet` pre-acquisition pass's
own interesting behavior -- deduping and sorting across SEVERAL distinct
`(workflow_name, quota_key)` pairs in one batch -- was never exercised;
a subtly wrong cross-shard or already-created skip inside the new loop
would have passed both existing tests untouched. Added
`detached_child_multi_spawn_batch_locks_every_distinct_key_and_admits_all`:
one parent spawns four detached children across two workflow types and
two tenant keys in a single decision cycle (a shared key under different
types, and different keys under one type), and asserts all four are
created with the correct resolved `quota_key`.

Three independent review agents (concurrency correctness, test coverage,
comment style) found no other genuine defects in the shipped fix.

The multi-spawn test's four lock acquisitions and four inserts left it
occasionally over the file's usual 10s `wait_for_execution_state` bound
under a busy test run, despite completing in ~5s in isolation. Gave it
its own 30s poll loop instead, the same margin
`wait_for_execution_state_with_timeout` documents for exactly this
reason (that helper is private to `integration_e2e.rs`).

## Follow-up — PR #1484 review: quota-key scoping, lock order, history bytes

Automated review on PR #1484 (this fix) found three P1 defects and one P2
gap in the dry-run credit above. All four are fixed here. The dry-run
function is renamed `dry_run_supersede_credit` and returns a
`SupersedeCredit` struct (`active_executions`, `history_bytes`) instead of
a bare `usize`.

**P1 — credit leaked across quota keys.** `concurrency_key` and
`quota_key` resolve from two independent expressions. They can differ.
The old function credited every shed run on the concurrency key,
regardless of its own `quota_key`. Two tenants sharing one
`concurrency_key` could see tenant A's cancellation free capacity for
tenant B's unrelated quota. Fixed: the query also selects each
candidate's `quota_key` column. Only shed candidates whose `quota_key`
matches the checked one count toward the credit. New test
`supersede_credit_does_not_cross_a_mismatched_quota_key` pins this: tenant
"beta" sits at its own quota cap on an unrelated key, a shared
`concurrency_key` sheds tenant "acme"'s incumbent, and beta's admission
is still correctly rejected with `QuotaExceeded`.

**P1 — a new ABBA lock-order hazard.** The old dry run took
`lock_concurrency_key` before `enforce_quota_admission`'s
`lock_quota_key` on the fresh-insert path. `replace_execution`'s three
admission arms take the quota lock first and the concurrency lock later,
in the real supersede pass. The two paths disagreed on lock order -- a
genuine deadlock risk under concurrent admissions. Fixed by dropping the
lock from the dry run entirely. It now performs a plain, unlocked read.
The cost is a stale read under true concurrent contention on the same
key. That tolerance already exists (the "residual over limit" telemetry)
and self-corrects on the next admission.

**P1 — credit ignored `history_bytes`.** The old credit covered only
`active_executions`. A `max_history_bytes` cap stayed broken under
`cancel_running`, for the same reason Finding 1 broke
`max_active_executions`. Fixed: the dry run also sums
`pg_column_size(event_data)` for the matching shed candidates'
`harvest_events` rows. `enforce_quota_admission` subtracts that sum from
`usage.history_bytes` too.

**P2 — the dry run ran even when quota could not use it.** No policy, no
declared cap, or no resolvable quota key all made the credit dead weight.
The fresh-insert path still always paid the extra query. Fixed: the dry
run now runs only when `quota_enforcement_policy` declares a cap AND a
quota key resolves, matching AC9's zero-default-overhead rule for the
rest of this feature.

All existing Finding-1 tests in `quota_supersede_ordering_tests.rs`, the
new mismatched-key test above, and the full `concurrency_supersede_
tests.rs` and `quota_enforcement_tests.rs` suites stay green against the
corrected design.

## Follow-up 2 — PR #1484 review: replace_execution credit, lock-then-scan order

A second automated review round, on the follow-up above, found two more
P1 defects. Both are fixed here.

**P1 — `replace_execution` never got a credit.** Every one of
`replace_execution`'s three callers runs `run_latest_wins_supersede`
right after it returns, exactly like the fresh-insert path. But
`replace_execution` always passed `SupersedeCredit::default()` to
`enforce_quota_admission`, so a `cancel_running` replacement sharing a
concurrency key with a DIFFERENT `workflow_id`'s incumbent hit the same
bug Finding 1 fixed on the fresh-insert path. Fixed: `replace_execution`
now builds the same dry-run description the fresh-insert path does, from
its own `request` and the replacement's new row id.

**P1 — the scan could go stale before the quota lock.** The fresh-insert
path ran its dry-run scan BEFORE calling `enforce_quota_admission`,
unlocked. Two admissions resolving the SAME `quota_key` could race: one
scans and gets credited for a slot, then a second admission takes the
quota lock first and consumes that slot for real, and the first
admission's later check still spends a credit for a slot that is already
gone. Fixed by moving the scan itself inside `enforce_quota_admission`,
run right after it takes `lock_quota_key`. Every admission for one quota
key now serializes through that single lock, so no other admission can
consume a credited slot between the scan and the check.

Both fixes share one new type, `PendingSupersede` (`concurrency_key`,
`concurrency_limit`, `self_exec_id`): callers now describe a pending
supersede pass instead of pre-computing its credit, and
`enforce_quota_admission` dry-runs it itself, under its own lock. The
four child-spawn call sites (three in `worker.rs`, one in
`cross_shard_child.rs`) pass `None` -- children only ever declare
`ConcurrencyOnConflict::Defer`, so this is a type change only, not a
behavior change.

Re-ran `quota_supersede_ordering_tests.rs` (5/5), `quota_lock_ordering_
tests.rs` (2/2), `concurrency_supersede_tests.rs` (20/20, including
`nested_self_referential_admission_emits_residual_over_limit_counter`),
and `quota_enforcement_tests.rs` (36/39, the 3 failures a pre-existing,
already-tracked `quota_blocked_outbox_*` flake family unrelated to this
change) against the corrected design. All green apart from that known
flake family.

## Follow-up 3 — PR #1484 review: lock the rows the credit depends on

A third automated review round found one more P1 defect in the credit
above: it stayed vulnerable to a stale scan, just from a different
angle than Follow-up 2.

**P1 — an unrelated candidate could still shrink the population.**
Computing the credit under the quota lock stops a SECOND admission for
the same quota key from racing in. It does not stop an ORDINARY
candidate -- one in a DIFFERENT quota bucket, sharing only the
concurrency key -- from completing on its own between the dry run's
scan and the real pass's later, independent re-scan. That completion
shrinks `candidates.len()`, which can lower the `shed` target
`supersede_plan` computes at the real pass. The real pass then sheds
FEWER runs than the credit assumed, and the admission it already let
through commits over cap.

Fixed: the dry run's candidate query now takes `FOR UPDATE` on every row
it returns, not only the credited ones. No row in that population can
change state until the transaction ends, so the real pass's later
re-scan sees the identical population and computes the identical
`shed`. A row that starts existing only after the scan only grows the
population, which can only raise `shed`, never lower it -- the safe
direction, so it needs no lock.

New test `dry_run_credit_row_locks_the_scanned_population`
(`quota_supersede_ordering_tests.rs`) proves the lock is real, not
timing-dependent: it holds the dry run's transaction open on one
connection, attempts to complete one of the scanned rows from a SECOND
connection, and asserts that attempt blocks until the first transaction
ends.

Re-ran all four suites above against the row-locked design: `quota_
supersede_ordering_tests.rs` (6/6, the new test included),
`quota_lock_ordering_tests.rs` (2/2), `concurrency_supersede_tests.rs`
(20/20), and `quota_enforcement_tests.rs` (35/39, the same known
`quota_blocked_outbox_*` flake family). All green apart from that
family.

## Follow-up 4 — PR #1484 review: skip locked supersede rows, union child locks

A fourth automated review round found two more P1 defects, one in each
prior follow-up's own fix.

**P1 — the new row lock could deadlock against a nested admission.** A
concurrently completing incumbent can hold its own row lock while its
terminal chokepoint starts a nested admission for the SAME quota key.
That nested admission needs the SAME `lock_quota_key` this admission
already holds, while the credit's row-lock scan would wait on the
incumbent's row -- an ABBA cycle between a row lock and the quota
advisory lock. Fixed: the scan now takes `FOR UPDATE OF e SKIP LOCKED`.
An already-locked row is one already leaving `RUNNING`/`PAUSED`, so
skipping it only undercounts the credit, never overcounts it -- the
safe direction, the same one an unlocked new row already relies on.

**P1 — detached and awaited child locks stayed in two separate sorted
passes.** `create_detached_child_executions` and the awaited-child
fan-out each pre-acquired their own `BTreeSet` of quota locks. Sorting
within each phase does not prevent an ABBA cycle ACROSS the two phases:
a parent with detached key B and awaited key A can race a peer with
detached key A and awaited key B, each holding its own phase-one key
while it waits on the other's phase-two key. Fixed: the key
computation moved into a new `detached_child_quota_lock_keys` helper.
`persist_all_started_child_workflows` and `persist_mixed_suspension_
batch` -- the two call sites that spawn both detached and awaited
children in one transaction -- now union both key sets and lock the
combination in one sorted pass, before either insertion phase runs.
`persist_terminal_outcome_commands`, which only ever spawns detached
children, is unchanged.

Re-ran all four suites above (`quota_supersede_ordering_tests.rs` 6/6,
`quota_lock_ordering_tests.rs` 2/2, `concurrency_supersede_tests.rs`
20/20, `quota_enforcement_tests.rs` 35/39, the same known flake family)
against the corrected design. No new test covers the cross-phase
detached/awaited race directly; the fix relies on manual verification
of the lock-acquisition order plus the existing suites above staying
green.

## Follow-up 5 — PR #1484 review: concurrency-lock ordering, credit reconciliation

A fifth automated review round found a P1 in follow-up 4's own fix and a
P2 in the original supersede-credit design.

**P1 — the row lock could still deadlock, this time against the
concurrency lock.** `dry_run_supersede_credit`'s row lock (added in
follow-up 3, `SKIP LOCKED`-guarded in follow-up 4) is held for the rest
of the transaction -- past the point where the real supersede pass needs
`lock_concurrency_key`. A concurrent admission already holding that
concurrency lock, and cancelling THIS transaction's scanned row via
`cancel_workflow_execution_collect`'s own, non-`SKIP LOCKED` `FOR
UPDATE`, would wait on a row this transaction holds while this
transaction waits on the concurrency lock that other transaction holds.
Fixed: `dry_run_supersede_credit` now takes `lock_concurrency_key`
itself, before its row scan, right after the caller's `lock_quota_key`.
Every path now acquires the two advisory locks in the same order --
quota, then concurrency -- before any row lock, closing the cycle.

**P2 — a skipped cancellation still spent its credit.** `supersede_
inner` deliberately skips a candidate on a corrupted `parent_close_
policy` or an unexpected `Config` error, to keep one bad neighbor from
wedging every future admission on the key. The dry-run credit had
already assumed that candidate was gone, so the admission could commit
slightly over its declared cap in that rare case. Fixed with
reconciliation, not admission-time blocking (the admission is already
committed by the time a skip is knowable): `SupersedeCredit` now carries
the exact `credited_ids` it counted, `enforce_quota_admission` and
`replace_execution` return them, and `run_latest_wins_supersede`
compares them against the real pass's actual `outcome.superseded` once
it returns. Any credited id the real pass left running increments a new
counter, `harvest.quota.supersede_credit_not_shed` (workflow-labeled
only, mirroring `harvest.concurrency.residual_over_limit`'s cardinality
rule) -- the alertable signal for a rare, already-documented edge case,
not a rejection.

New tests: `credited_but_not_shed_count_reports_every_credited_id_
that_was_not_shed` (`concurrency.rs`, a pure unit test of the
reconciliation math) and `bridges_quota_supersede_credit_not_shed_
with_workflow_and_gap_labels` (`metrics_rs_adapter.rs`, proving the new
counter is actually wired to the `metrics-rs` backend and not silently
resolving to the trait's no-op default -- the exact bug class issue
#1367's review caught for `harvest.concurrency.residual_over_limit`).
No test exercises the underlying fault-injection scenario itself (a
genuinely corrupted `parent_close_policy` or a forced `Config` error
mid-cancel); none existed for that skip path before this change either.

Re-ran all four suites above (`quota_supersede_ordering_tests.rs` 6/6,
`quota_lock_ordering_tests.rs` 2/2, `concurrency_supersede_tests.rs`
20/20, `quota_enforcement_tests.rs` 36/39, the same known flake family)
plus the full `cargo test --lib` unit suite (3453/3453) against the
corrected design.

## Follow-up 6 — PR #1484 review: the row lock itself was the last defect

A sixth automated review round found the `FOR UPDATE ... SKIP LOCKED`
row lock, added two follow-ups earlier to keep the dry run's scanned
population stable, was itself the remaining defect.

**P1 — `SKIP LOCKED` also skips rows locked for ordinary reasons.**
`store::next_event_id_for` takes a plain `FOR UPDATE` on a workflow's
row during EVERY ordinary decision cycle, without ever transitioning it
out of `RUNNING`. `SKIP LOCKED` cannot tell that apart from a row
genuinely leaving the population. A `cancel_running` admission that
scans an incumbent at the exact moment some unrelated decision cycle
holds its lock would undercount the credit, and could see
`enforce_quota_admission` reject an otherwise-healthy admission with
`QuotaExceeded` -- defeating `cancel_running` far more often than the
deadlock the row lock itself was added to prevent.

Fixed by removing the row lock (and the `lock_concurrency_key` call
follow-up 5 added around it) entirely, going back to a plain, unlocked
scan. Follow-up 5's `credited_ids` reconciliation already built the
correct safety net for exactly this kind of staleness: it does not care
WHY a credited candidate went unshed, so a skipped cancellation and a
stale, unlocked scan both surface identically as a gap reported via
`harvest.quota.supersede_credit_not_shed`. No lock is needed here at
all -- an unlocked read can only make the scanned population MORE
stale, never less honest about what it saw, and the reconciliation
catches every resulting gap after the fact.

Rewrote `dry_run_credit_row_locks_the_scanned_population` (which
proved the lock blocked a second connection) into
`dry_run_credit_counts_a_row_locked_for_unrelated_reasons`: it holds
one incumbent's row locked from a separate connection, for the whole
scan, and asserts the scan still credits it -- neither blocking on the
lock nor skipping the row it guards.

Re-ran all four suites above (`quota_supersede_ordering_tests.rs` 6/6,
the new test included, `quota_lock_ordering_tests.rs` 2/2,
`concurrency_supersede_tests.rs` 20/20, `quota_enforcement_tests.rs`
35/39, the same known flake family) against the corrected design.

## Follow-up 7 — PR #1484 review: the reconciliation gap was rejectable, not just observable

A seventh automated review round challenged an assumption follow-up 5
made: that a credited-but-unshed run could only be reported, never
rejected, because the admission was "already committed" by the time the
gap was knowable. Verified directly against the code: that assumption
was wrong. `run_latest_wins_supersede` runs INSIDE the same open
`conn.transaction(...)` closure the admission's row insert, quota
check, `WorkflowStarted` event, and `queue::enqueue` call all share.
Nothing commits until that closure returns `Ok`. No non-transactional
side effect (a spawned completion-trigger task, an in-process dispatch
hint) fires before that point either -- both are gated behind the
`collected?` in the caller, unreachable on `Err`.

**P1 — a real over-cap gap only emitted a counter.** Fixed by
re-validating: when `credited_but_not_shed_count` finds a gap,
`run_latest_wins_supersede` now calls `enforce_quota_admission` again,
with `pending_supersede: None` (the real pass already ran, so there is
nothing left to dry-run), reloading usage against the real, final
population instead of the dry run's credit. A still-violating recheck
returns `QuotaExceeded`, which rolls the whole transaction back through
the exact path an ordinary quota rejection already uses -- no fresh
row, no event, no enqueued task. `quota_policy`/`quota_key` are threaded
through all four `run_latest_wins_supersede` call sites, the same
values each caller already passes to `enforce_quota_admission`. The
`harvest.quota.supersede_credit_not_shed` counter still fires on every
gap, since the rarer cancellation-skip case is worth alerting on even
when the recheck finds enough OTHER freed capacity to let the admission
stand.

Re-ran all four suites above (`quota_supersede_ordering_tests.rs` 6/6,
`quota_lock_ordering_tests.rs` 2/2, `concurrency_supersede_tests.rs`
20/20, `quota_enforcement_tests.rs` 35/39, the same known flake family)
against the corrected design. No new test exercises the recheck's own
rejection path directly -- doing so needs a genuine mid-transaction
state change between the dry run and the real pass, which (unlike the
row-lock tests earlier in this file) has no deterministic hook once the
scan is unlocked; the existing suites confirm the ordinary,
no-gap path stays byte-for-byte unchanged.

## Follow-up 8 — PR #1484 review: two-read subtraction was itself the staleness source

An eighth automated review round found two findings on the same
mechanism, both traced to one root cause: `enforce_quota_admission`
computed usage with one query, then adjusted it with numbers computed
by a SEPARATE, earlier query (or a caller-supplied constant). Two reads
of the same underlying rows, taken moments apart inside one open
transaction, can each see a different snapshot.

**P1 -- the usage read and the credit dry-run read could disagree.**
`load_quota_usage` ran after `dry_run_supersede_credit` computed its
credited ids, but neither shared a snapshot, so a row that changed
between the two reads (an incumbent completing on its own) made the
subsequent `active_executions`/`history_bytes` subtraction stale in
either direction.

**P2 -- `self_exec_id`'s own "-1" adjustment double-counted alongside
the credited exclusions.** The two adjustments were computed by
different code paths and merely summed, with no shared query to keep
them consistent as the credited-ids list grew.

Fixed both by construction rather than by adding a third adjustment:
added `quota::load_quota_usage_excluding`, a sibling to
`load_quota_usage` whose `active` CTE also excludes an explicit array
of execution ids, in the SAME query that computes `active_executions`
and `history_bytes`. `enforce_quota_admission` now takes an explicit
`self_exec_id: ExecutionId` parameter, builds one `excluded_ids` vector
(the dry-run's `credited_ids` plus `self_exec_id`), and passes it to
`load_quota_usage_excluding` in place of the old read-then-subtract
pair. `SupersedeCredit` drops its `active_executions`/`history_bytes`
fields entirely -- `credited_ids` is now the only output the caller
needs, since counting happens in the one query instead of being
assembled from two. `load_quota_usage` itself is untouched, so every
OTHER caller (the admin read-model included) keeps the exact query
shape `docs/performance-quota-history-bytes.md` measured.

All seven call sites of `enforce_quota_admission` (the fresh-insert
path, `replace_execution`, `run_latest_wins_supersede`'s recheck, both
awaited- and detached-child spawn paths in `worker.rs`, and the
cross-shard child spawn path) now pass the execution id of the row
each call is itself admitting.

Re-ran all four suites above (`quota_supersede_ordering_tests.rs` 6/6,
the updated `dry_run_credit_counts_a_row_locked_for_unrelated_reasons`
assertion included, `quota_lock_ordering_tests.rs` 2/2,
`concurrency_supersede_tests.rs` 20/20, `quota_enforcement_tests.rs`
37/39, the same known `quota_blocked_outbox_*` flake family) against
the corrected design. Confirmed the two flakes pre-exist against the
merge base, before this follow-up's changes, by re-running them in
isolation there.

## Follow-up 9 — PR #1484 review: the real supersede pass's own row lock, not the dry run's, closed the loop

A ninth automated review round found an ABBA cycle distinct from every
prior one in this file: not the dry run's candidate scan (unlocked since
follow-up 6), but the REAL supersede pass's row lock inside
`cancel_workflow_execution_collect`.

**P1 -- the quota lock and a real cancellation's row lock can form a
cycle.** A `cancel_running` admission's transaction holds the quota
advisory lock (`lock_quota_key`) for its whole duration, then later waits
on an incumbent's row lock to cancel it. An incumbent completing on its
own can hold that same row lock while its own inline, same-shard
completion-trigger admission (issue #618) waits on that SAME quota lock,
if the triggered start shares the checked admission's `(workflow_name,
quota_key)`. Postgres can only break the resulting wait-for cycle by
aborting one side with `40P01`.

Fixed by adding `try_claim_candidate_row`: a non-blocking `FOR UPDATE SKIP
LOCKED` probe run immediately before each candidate's real cancellation.
A claimed row proceeds exactly as before -- the immediately following
`cancel_workflow_execution_collect` call re-acquires the SAME lock inside
the SAME transaction, which Postgres grants at once. A row locked
elsewhere is simply not shed this round, through the new
`claim_candidate_row_or_warn` wrapper. That is the identical outcome
`supersede_inner`'s shed loop already tolerates for a corrupt neighbour
or a benign terminal race, and `credited_ids` reconciliation in
`run_latest_wins_supersede` (follow-up 5) already catches it the same way
regardless of cause.

Verified the cycle was real, not just plausible, before fixing it: traced
the exact `worker.rs`/`completion_trigger.rs` call chain showing a
same-shard completion-trigger start runs inline on the source execution's
own terminal transaction, pre-commit, reaching `enforce_quota_admission`
before that transaction releases its row lock on the source.

Re-ran all four suites above (`quota_supersede_ordering_tests.rs` 6/6,
`quota_lock_ordering_tests.rs` 2/2, `concurrency_supersede_tests.rs`
20/20, `quota_enforcement_tests.rs` 39/41 in isolation, the same known
`quota_blocked_outbox_*` flake pair) against the corrected design. A
third test, `completion_trigger_defers_to_outbox_when_target_quota_
exceeded`, failed once in a full-suite run alongside the other three
suites but passed in isolation both alone and together with only
`quota_enforcement_tests.rs` -- confirmed flaky, not a regression.

Also ports a second pre-existing trunk-dev CI defect surfaced only once
follow-up 8's Lint fix let the job run far enough to reach it:
`docs/rnd/sqlite-feasibility.md` still said 102 migrations against a live
count of 103. Fixed per the already-open autumn-foundation/autumn-harvest#1513.

## Follow-up 10 — PR #1484 review: the non-blocking probe skipped candidates it never needed to

A tenth automated review round found follow-up 9's own fix regressed a
case it never needed to touch.

**P1 -- a `cancel_running` workflow with no quota policy lost its
blocking guarantee for no safety benefit.** `enforce_quota_admission`
only calls `lock_quota_key` when a quota policy with a cap is in play
and its key resolves; a concurrency-only `cancel_running` workflow never
takes that lock, so the ABBA cycle follow-up 9 fixed cannot form there.
The unconditional `try_claim_candidate_row` probe still applied in that
case, so a candidate merely locked by an ordinary, unrelated decision
cycle -- previously waited out, then cancelled -- was now skipped
outright, with no quota-side reconciliation to catch the gap (credited
ids are always empty without a quota policy). A concurrency limit could
stay violated indefinitely instead of converging on the next admission.

Fixed by threading a new `quota_lock_held: bool` into
`supersede_running_for_key`/`supersede_inner`, computed by
`run_latest_wins_supersede` from the same `quota_policy`/`quota_key`
values already passed to this transaction's earlier
`enforce_quota_admission` call (the same values that decided whether
`lock_quota_key` was actually taken). The shed loop's probe now applies
only when `quota_lock_held` is true; otherwise it falls back to the
plain, blocking cancel every caller used before follow-up 9, exactly
preserving the pre-existing convergence guarantee where no cycle was
ever possible.

Also closes a genuine, unrelated coverage gap the same CI run surfaced:
`harvest.quota.supersede_credit_not_shed` (added in follow-up 5) had no
dashboard panel and no `SERIES_LABELS` entry, tripping the anti-drift
dashboard-coverage gate. Added a panel mirroring
`harvest_concurrency_residual_over_limit`'s, and the matching
`SERIES_LABELS`/`DASHBOARD_PROMETHEUS_SERIES` entries.

Re-ran `quota_supersede_ordering_tests.rs` (6/6),
`quota_lock_ordering_tests.rs` (2/2), `concurrency_supersede_tests.rs`
(20/20), `quota_enforcement_tests.rs` (38/41, the same known
`quota_blocked_outbox_*`/`completion_trigger_defers_to_outbox_...`
flakes), and all 18 `dashboard_pack_docs.rs` tests against the corrected
design.
