# 🚦 Semaphore CI health — `worker_completes_ten_child_fan_out_within_wall_clock_bound`
# is recurring at a higher rate than the issue that diagnosed it, plus two closed
# investigation threads

**Status:** health report / product-bug-frequency update — no PR opened against
`ci.yml`, no test changed. Continues the series in
`docs/rnd/2026-09-0[3-8]-ci-health-semaphore*.md` and
`docs/rnd/2026-09-11-ci-health-semaphore-cancelled-run-census.md`. That prior
report routed forward two open items and flagged one unconfirmed flake
candidate; this report closes one of the two open items (with a negative
result), reports no new occurrence of the flake candidate, and surfaces a
higher-priority finding found while checking CI history since: the exact test
that issue #1459 diagnosed as a **product** bug two days ago is now failing at
a materially higher rate than the single occurrence that prompted that issue,
and roughly matches its second, previously-secondary signature more often than
its primary one.

## 🎯 Verdict path

Same verdict path as every prior report in this series: `ci.yml`'s
`test-db-linux` (10 shards + 1 partitioned-layout leg) and `test-nodb` (12
shards) matrices. Cache-usage API access and branch-protection confirmation
remain unavailable to this session, unchanged from every report 09-03 through
09-11 — not re-described here.

## 🌡️ Symptom

### 1. `worker_completes_ten_child_fan_out_within_wall_clock_bound`: 5 occurrences in ~13.5 hours

Scanning `ci.yml` runs on `pull_request` events from 2026-09-11 15:35 UTC
through 2026-09-12 09:41 UTC (the ~130 most recent completed runs, plus
targeted job-log pulls on every run whose `Test DB (linux, ...)` or
`Test (no-db, ...)` job showed `conclusion: failure`; cancelled runs were
**not** re-audited at job level this session — see caveat in 📊 below) found
this test failing 5 times, on 5 different commits/branches:

| Run | Job | When (UTC) | `worker_id` | `wake_requested` | `attempt` | Signature |
|---|---|---|---|---|---|---|
| `34629265073` | Test DB shard 7 | 09-11 18:46:50 | `None` | `false` | 3 | B |
| `34644245680` | Test DB shard 0 | 09-11 22:24:40 | `Some("worker-e2e-ten-slow-children")` | `true` | 2 | **A** |
| `34647900372` | Test DB shard 8 | 09-11 22:56:29 | `None` | `false` | 3 | B |
| `34677404729` | Test DB shard 0, partitioned layout | 09-12 07:12:23 | `None` | `false` | 4 | B |
| `34680759205` | Test DB shard 8 | 09-12 08:04:54 | `None` | `false` | 4 | B |

"Signature A" and "Signature B" are the two failure shapes issue #1459
(filed 2026-09-10, still open, 0 linked PRs) already named from its own
controlled reproduction:

- **Signature A** — parent task `RUNNING`, claimed by a live worker,
  `wake_requested=true`, `attempt` frozen at 2. Issue #1459 diagnosed this as
  a confirmed **product** bug: `reset_timed_out_workflow_task`'s bounded
  pool-retry backoff (`[0, 200, 500, 2_000]` ms) can exhaust under
  contention, and the poison-pill orphan reclaimer only reclaims a `RUNNING`
  row on a **dead** worker's heartbeat gap — it does not reclaim a wedged row
  on a still-live worker. No other backstop exists once the reset write is
  dropped.
- **Signature B** — parent task `PENDING`/unclaimed (`worker_id=None`), a
  higher `attempt` count (the reset *did* fire at least once), still not
  picked back up by any worker before the 180s bound. Issue #1459's own
  report called this "a related but distinct starvation shape... noted for
  completeness, not the focus of this report," seen in 4/12 of its own
  controlled reproductions against 5/12 for Signature A.

**In this session's 5 fresh CI occurrences, the ratio inverts: 4/5 are
Signature B, 1/5 is Signature A.** That is a small sample and not a
same-commit rerun protocol, so it is reported as an observation, not a
measured rate — but it is large enough to say Signature B is not a rare edge
case next to Signature A; on this fresh sample it is the more common shape in
real CI, which issue #1459's own text does not currently reflect.

### 2. The `dispatch_tests` / `harvest_shard_generation` flake candidate: item 3 from the 09-11 report, closed with a negative result

The 09-11 report's routing item 3 asked for container-level logs from the two
prior occurrences (`34172807956`, `34167481977`) to check the
double-ready-message hypothesis for testcontainers' Postgres wait strategy.
Both jobs' full CI logs (not just the tailed excerpt) were pulled this
session and searched for any Postgres server log line (`database system is
ready`, `PostgreSQL init process`, etc.): **zero matches in either log.**

This is a real, useful negative result, not an inconclusive one: GitHub
Actions job logs capture the test process's own stdout, and testcontainers-rs
does not forward the managed Postgres container's internal log stream into
that stdout by default. The container-level evidence this item asked for is
**not obtainable from CI job logs with any tool available in this
environment** — confirming or denying the log-ordering hypothesis would
require either instrumenting the test to stream container logs explicitly, or
direct Docker daemon access on the runner, neither of which this session has.
Closing this routing item as blocked-by-tooling rather than carrying it
forward unchanged again.

While pulling those logs, the mechanism question the 09-11 report also left
open — "fresh container per test, or shared DB across the module's 18 tests"
— got a incidental answer: the whole `dispatch_tests` module runs as one
serial `cargo test -- dispatch_tests --test-threads=1` process, 18 tests
completing in ~28s end-to-end (`34172807956`) including whatever container
setup each test performs. That is consistent with the per-test
`setup_test_database_url_or_env()` container-per-test design already in the
source (confirmed by reading `integration_e2e.rs:498-504`), not with a single
shared database — a fast local Postgres image pull/cache makes 18 sequential
fresh containers plausible in that window. No new occurrence of this flake's
signature (`relation "harvest_shard_generation" does not exist`) was seen in
this session's scan window; it remains at the 2 total occurrences the 09-11
report measured.

Separately, the 09-11 report's diagnosis mentioned `INIT_SQL` is "deliberately
partial" and wondered whether the bundle omits the DR-fencing table. It does
not: `integration_e2e.rs:295-297` includes
`migrations/20260726000000_harvest_shard_generation/up.sql` as the last entry
in the `INIT_SQL` `concat!`, with a comment explaining why (issue #1312). That
migration file and the one immediately before it in the bundle
(`20260906014820_harvest_completion_trigger_outbox_backoff/up.sql`) both end
with a real trailing newline on disk, so the two entries concatenate cleanly
despite the bundle's inconsistent use of an explicit `"\n",` separator between
entries — checked directly (`od -c` on both files' tails) because a missing
separator was briefly considered as an explanation for the flake and ruled
out: it would fail every run, not 2 in several hundred.

### 3. Two other DB-test failures spotted in the same window, not triaged

While pulling job logs for the above, two other real failures appeared that
this session did not have budget to root-cause; noting them rather than
silently dropping them:

- `quota_enforcement_tests::{batched_start_at_max_size_stamps_quota_key_from_first_admission, batched_start_over_cap_is_rejected_at_fire_time}` — both failed together at `quota_enforcement_tests.rs:696` in run `34625115981`, shard 7.
- `eligibility_tests::{test_worker_capabilities_routing_and_triage, test_worker_heartbeat_updates_labels}` plus several unattributed `FAILED` lines in run `34629265073`, shard 2.

Neither was compared against prior reports' known-deterministic list or given
a rerun attempt; they are unclassified as flake vs. deterministic-defect as of
this report.

## 🔍 Diagnosis

**Item 1** is not a new bug: it is issue #1459, already open, already
diagnosed at the product level with a named mechanism and a controlled
reproduction (0/15 baseline → 9/12 under contention). Per this role's own
hard gate, the test-vs-product verdict for this failure was already rendered
by that issue, correctly, and this report does not re-litigate it — it
updates the issue with fresher frequency and signature-mix data. **This
report does not propose widening the 180s bound again; the test's own doc
comment explicitly forbids that ("The bound below is not widened again for
this cause"), and issue #1459 independently confirms why that would be the
wrong move: it would hide a live production-affecting recovery-path gap, not
fix it.**

**Item 2** required container-level evidence this session cannot obtain;
closing it as blocked rather than carrying it forward as if more investigation
here would help.

## 🔧 Treatment

None shipped, correctly:

- No test change — issue #1459 already established this is a product bug, and
  the test's own doc comment forbids the one change (widening the bound) this
  role must never make as a substitute for a real fix.
- No new fix for issue #1459 attempted — the suggested directions in that
  issue (a liveness-independent backstop in `poison_pill.rs`, a metric on
  `reset_timed_out_workflow_task`'s retry-budget exhaustion) are a product
  change to the scheduler/recovery path, outside a CI-health investigation's
  scope, and issue #1459 already routes them correctly.
- **Posted a comment on issue #1459** with the 5 fresh occurrences, the
  signature breakdown table above, and the observation that Signature B (the
  one issue #1459 called secondary) is the majority shape in this fresh
  sample — so whoever picks up that issue has current frequency data rather
  than only the single original occurrence.

## 📊 Measurement

- **Item 1:** 5/5 occurrences classified by signature from their own
  `wait_for_completion_with_diagnostics` diagnostic dump (not inferred) across
  5 distinct commits, 2026-09-11 18:46 through 2026-09-12 08:04 UTC. Not a
  same-commit rerun protocol — no single commit was rerun ≥20x — so this is
  frequency-in-the-wild evidence, not a measured rate in this role's Tier-1
  sense. It is offered as a reason to prioritize issue #1459, not as a
  standalone rate claim.
- **Scope caveat:** this session's window (09-11 15:35 through 09-12 09:41,
  ~130 runs) was scanned for explicit `conclusion: failure` runs and their
  job-level detail; it was **not** a full cancelled-run job-level audit like
  the 09-11 report's 54/54 census. Per that report's own finding (28% of
  cancelled runs hid a real job failure), this window's true failure count —
  including any further occurrences of either wall-clock-bound signature or
  the dispatch_tests flake — is a floor, not a ceiling. A full cancelled-run
  audit of this window is routed forward, not performed here, for budget
  reasons.
- **Item 2:** 2/2 full job logs for the flake candidate's two known
  occurrences searched end-to-end for Postgres server log lines: 0/2 found
  any. This is a complete negative result for the tooling available, not a
  partial one.
- No revert check applies — no fix in this report to verify red-then-green
  on.

## 🔬 Reproduce

```sh
# Item 1 census: actions_list(method="list_workflow_runs", resource_id="ci.yml",
#   workflow_runs_filter={event:"pull_request", status:"completed"}, perPage=100)
# across pages back to the 09-11 report's cutoff; for every run with a
# "failure" conclusion, list_workflow_jobs and pull get_job_logs
# (return_content=false, then curl the signed logs_url) for any
# "Test DB (linux, ...)" / "Test (no-db, ...)" job; grep for
# "worker_completes_ten_child_fan_out_within_wall_clock_bound" and the
# "parent task queue rows:" diagnostic line it prints on failure.

# Item 2: get_job_logs(job_id=<job for 34172807956's and 34167481977's
# "Test DB (linux, shard 5)">, return_content=false) then curl the full log
# and grep -i "database system is ready|postgresql init process" — 0 matches
# in either.

# INIT_SQL concatenation check:
od -c "autumn-harvest/migrations/20260906014820_harvest_completion_trigger_outbox_backoff/up.sql" | tail -3
head -c 200 "autumn-harvest/migrations/20260726000000_harvest_shard_generation/up.sql"
```
