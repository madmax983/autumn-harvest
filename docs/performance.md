# Task-claim and enqueue performance

Harvest publishes a CPU-path budget for replay (a 10 000-event history replays in
under 200 ms, issue #135), but until issue #786 it published nothing at all for
`queue::claim_task` — the single most scalability-critical query in the engine,
and the one that has accreted roughly a `WHERE` predicate per phase since 3.7:

| Predicate | Issue |
|:--|:--|
| build-id routing | #171 |
| per-key concurrency | #247 |
| rate-limit gate | #332 / #699 |
| circuit-breaker tracked set | #369 |
| `schedule_to_close` | #378 |
| PAUSED-execution skip | #383 |
| worker sessions | #606 |
| queue pauses | #619 |
| capability labels | #382 |
| sticky routing | #235 |
| activity pauses | #807 |

Each was added for correctness. None was measured. This page is the measurement
— **for five of them**. The attribution table below varies build-id routing,
per-key concurrency, the rate-limit gate, the circuit-breaker tracked set and
the PAUSED skip. The other six are present in the query and held constant, so
this page says nothing about what they cost in *that* table; see
[known limitations](#known-limitations).

> **Looking for end-to-end numbers?** This page measures the claim and enqueue
> path in isolation. [`benchmarks.md`](benchmarks.md) publishes what the engine
> does end to end — workflows/sec, dispatch and signal latency, replay
> throughput — at 1, 2 and 4 shards, with a one-command reproduction. The two
> are complements: when an end-to-end number there moves, this page is where you
> find out whether the claim path is why.

> **These are starter reference numbers, not an SLO.** They were taken on one
> machine with one Postgres configuration (below). Your hardware, your
> `shared_buffers`, your backlog shape, and your queue count all move them.
> Reproduce them on your own hardware before designing against them — the
> benchmark is in the repo precisely so you can.

## TL;DR

* **Claim latency scales superlinearly with pending-backlog depth.** 1k → 10k
  rows (10x) costs ~19x latency; 10k → 100k (10x) costs a further ~15x. Claim
  cost is a function of how deep your queue is, not how much work you dispatch.
  **This is the single biggest lever on this page** — bigger than any individual
  predicate, and it dominates the per-gate table below.
* **The cause is structural, not incidental — and it is not only the `CASE`
  key.** The claim query's `ORDER BY` leads with a non-indexable `CASE`
  expression, so `idx_harvest_tq_poll` cannot serve the ordering, and Postgres
  sequentially scans and sorts every eligible pending row on every single
  claim. See [the plan](#the-plan) below.
  **Fixing that key would not be sufficient on its own**: issue #1177 shows
  any single one of ten other residual `WHERE` predicates it tested
  independently defeats sort-elision and `LIMIT` pushdown too, even at zero
  selectivity — the query carries an eleventh, untested by that issue. See
  [any residual predicate defeats sort-elision](#any-residual-predicate-defeats-sort-elision-issue-1177).
* **Only one predicate is genuinely expensive: per-key concurrency (+644% p50).**
  Build-id routing (+13%), the rate-limit gate (+2%) and the circuit-breaker
  tracked set (+4%) are cheap or free.
* **The per-key concurrency predicate (#247) — flagged above as the one
  genuinely expensive gate — has since been fixed.** Its candidate-side gate
  was a correlated `COUNT(*)` subquery, re-evaluated once per candidate row a
  claim visits rather than once per claim. Materializing the `RUNNING` count
  once per claim into a small pre-aggregated CTE cuts total buffers touched
  across a real 10 000-row drain **-99.23%** at the headline scenario
  (1 385 001 432 → 10 727 317). See
  [the concurrency-key gate fix](#the-concurrency-key-gate-fix).
* **Paused executions cost as much as live ones (+1403% p50), and the cost is
  table depth rather than anything specific to pausing.** An equal-*depth*
  control with no PAUSED rows costs the same (+1383%), so the operational
  finding is about rows in the table. That control does **not** isolate the
  anti-join predicate itself — the two scenarios take different query plans —
  so this page does not publish a cost for the predicate in isolation. See
  [the control that changed the conclusion](#the-control-that-changed-the-conclusion).
* **The queue-pause anti-join (#619) — flagged above as accreted-but-unmeasured
  — has since been measured with an actively paused queue, and fixed.** It is
  a different predicate from the PAUSED-*execution* skip above (that one
  checks `harvest_workflow_executions.state`; this one checks
  `harvest_queue_pauses`, an operator-facing "pause this whole queue" switch).
  Pre-filtering it into a small array instead of re-probing it once per
  candidate row cuts buffers touched by a single claim **-98.05%** at the
  headline 10k-backlog scenario (12 743 → 248) with one of four polled queues
  paused. See
  [the queue-pause anti-join fix](#the-queue-pause-anti-join-fix).
* **Enqueue is not the problem.** ~4 800 rows/s sustained at p50 ~1.5 ms, flat
  from 1k to 100k backlog (inside run-to-run noise). At 100k the write side
  sustains ~4 600 rows/s while the read side manages ~3 claims/s — **a queue
  that deep does not drain.**
* Issue #786 deliberately **measures without tuning**: the claim query is
  byte-for-byte unchanged by this work.

## Reference environment

The tables on this page came from these two commands:

```bash
# The full exploratory report (the tables on this page).
# Expect this to take 15-30 minutes: it sweeps three backlog depths, eight gate
# scenarios and three enqueue depths, and a 100k-row scenario is slow on purpose.
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
HARVEST_BENCH_SCENARIO_SECS=180 \
  cargo bench -p autumn-harvest --features db --bench claim_bench

# The CI gate, byte-for-byte as `.github/ci/integration-suites.txt` runs it.
# `claim_budget_tests` is a substring filter, so this runs the whole gate
# module, not just the budget check: the headline scenario, the eight-scenario
# coverage sweep, the 250k-row enqueue cutoff, and the sweep/lease probes.
# ~80s here against a local server; longer in CI, which starts a container.
cargo test -p autumn-harvest --features db --test integration -- \
  claim_budget_tests --test-threads=1

# Just the headline p50-vs-budget check — the one assertion that fails when a
# regression lands. ~50s here.
cargo test -p autumn-harvest --features db --test integration -- \
  claim_budget_tests::claim_p50_at_headline_scenario_is_within_budget \
  --test-threads=1
```

Every table on this page is from **one** run of the first command, on an
otherwise-idle box. Figures that are *not* from that run say so where they
appear: the budget derivation (a distribution over repeated runs), the
reproducibility paragraph (three independent runs), the p50-vs-p99 comparison
(idle and loaded), and the debug-vs-release comparison (two profiles, back to
back).

| | |
|:--|:--|
| Machine | linux / 4 logical CPUs |
| Postgres | 16.13 (Ubuntu), default `shared_buffers` |
| Profile | `bench` (release). Debug was measured too — see [profile](#profile-does-not-matter) |
| Harness | `autumn-harvest/tests/integration/claim_bench_support.rs` |

`HARVEST_TEST_DATABASE_URL` is treated as an **admin** URL, not a target
database: the role it names **must be able to `CREATE DATABASE`**, because a
freshly-named database is created and migrated per run so a 100k-row backlog can
never leak into a shared one. A role without that privilege makes the harness
report a skip and exit 0 — a silent no-result rather than an error, so check the
privilege before concluding "the benchmark produced nothing".

Two more things worth knowing before pointing this at a server you care about:

* **Setup sweeps stale benchmark databases**, not just teardown — that is what
  reclaims databases orphaned by a run that panicked, which a teardown hook can
  never do. Only names this harness could itself have minted are ever eligible:
  the full shape `harvest_claim_bench_{pid}_{token}_{seq}`, where `pid` and
  `seq` are decimal and `token` is exactly 16 lowercase hex digits. Sharing the
  prefix is **not** enough — a database of your own called, say,
  `harvest_claim_bench_123_production` fails the token and sequence checks and
  is never a candidate. (The SQL prefilter matches on the prefix, but `_` is a
  single-character wildcard in `LIKE`, so the prefilter is deliberately not the
  authority; every candidate is re-checked against the whole shape before
  anything destructive runs.) The sole liveness authority is the **server**: the
  sweep drops a database only when nothing holds a backend against it, and each
  run keeps one idle connection open for the whole life of its database
  precisely so that question has an answer even between scenarios. That lease
  disables `idle_session_timeout` on its own session: the lease defends the run
  by *being* an idle backend, which is exactly what the reaper introduced in
  PostgreSQL 14 kills, so on a server that sets it the lease would otherwise be
  terminated mid-run and a concurrent sweep would then see the live database as
  abandoned. The override is session-local, needs no privilege, and touches
  nothing else on the server; if it cannot be applied on a 14+ server the run
  warns rather than proceeding quietly, since an unarmed lease is not a lease.
  A local pid
  is deliberately *not* consulted — it cannot answer for a run on another host,
  it cannot answer at all on non-Linux, and two containerised runs on different
  hosts both report pid 1, so treating it as a liveness signal would either veto
  every reclaim or protect nothing. There is one window the server cannot see —
  between `CREATE DATABASE` and the first connection to it, the database exists
  with zero backends and looks abandoned — so setup holds an advisory lock
  across that whole span, and every sweep takes the same lock first. The lock
  lives on its own connection to the `postgres` database rather than on the
  admin connection, because Postgres advisory locks are scoped to the session's
  database, not the cluster: taken on the admin connection, two runs reaching
  one server through different admin databases would not serialize at all. For
  the same reason there is no fallback — **a role that cannot connect to
  `postgres` makes the run refuse to start**, naming the missing grant, rather
  than proceeding with a lock that only coordinates a subset of clients.
  Credentials never reach a log line: the URL is redacted to
  `scheme://***@host:port/db` before it is printed.
* **Each scenario is bounded by a wall clock**, on the write path as well as
  the read path: every pool checkout, claim and enqueue is bounded by the
  scenario deadline, so a stalled server ends the scenario at the ceiling
  instead of hanging the run. A scenario that stops early is marked `⚠` in the
  report and fails the CI gate as unsound rather than publishing a percentile
  over a partial window.
* **`HARVEST_BENCH_SCENARIO_SECS`** caps each scenario's measured phase
  (default 240 s). It is the knob to raise when a row comes back marked `⚠` or
  `‡` — see [measurement hygiene](#measurement-hygiene).

With the variable unset the benchmark starts a `postgres:16` testcontainer
instead; with neither available it prints a skip notice and exits 0.

## Claim latency vs backlog depth

Baseline gate (no build policy, no concurrency key, no rate limit, no pauses),
8 concurrent claimers across 4 queues:

| backlog | n | p50 ms | p99 ms | max ms | claims/s |
|--:|--:|--:|--:|--:|--:|
| 1 000 | 184 | 10.44 | 23.49 | 26.53 | 640 |
| 10 000 | 720 | 200.03 | 239.90 | 283.73 | 29 |
| 100 000 ⚠ | 583 | 2 919.99 | 3 516.16 | 3 658.11 | 3 |

⚠ Cut short by the per-scenario wall-clock budget (180 s for this run; the
default is 240 s, override with `HARVEST_BENCH_SCENARIO_SECS`). The percentiles
describe the 583 claims it did observe. That the scenario *cannot* finish its
planned 800 claims in three minutes is itself the finding.

These rows run at 8 concurrent claimers against a 4-core box, so they measure
the claim path **under contention** — the operational number a worker actually
waits for, not the isolated query cost. The tail columns in particular carry
run-queue scheduling as well as query time; the p50 column is the more stable
comparison, and the per-gate table below deliberately runs below saturation for
the same reason.

**Read this table as a sharding trigger.** A queue that stays around a thousand
pending rows claims in single-digit milliseconds. A queue that sits at ten
thousand claims in roughly a quarter of a second — still workable, but each worker
poll is now a real cost. A queue parked at a hundred thousand pending rows spends
several seconds per claim, and the fleet's throughput collapses to a handful of
claims per second regardless of how many workers you add. If your steady-state
backlog is trending toward the 100k row, the answer is to shard
(`docs/sharding.md`) or to shed the backlog — adding workers will not help,
because every worker pays the full scan.

## Incremental cost of the accreted gates

10 000-row backlog, 4 queues, **2 claimers**.

Attribution deliberately runs below the headline scenario's 8 claimers. At 8
claimers on a 4-core box the tail columns stopped reproducing between runs (an
identical baseline scenario reported a 285 ms max in one run and 3 093 ms in the
next) even though the p50 deltas held steady — the tails were measuring
run-queue scheduling, not the query. Below saturation the numbers isolate
predicate cost.

**`p50 vs` is the attribution statistic**, and it is measured against the row
named in `vs what` — which is not always `baseline`. See
[the control that changed the conclusion](#the-control-that-changed-the-conclusion).

| gate | seeded rows | claimable | n | p50 ms | p50 vs | vs what |
|:--|--:|--:|--:|--:|--:|:--|
| `baseline` | 10 000 | 10 000 | 720 | 89.32 | — | |
| `rate_limited` | 10 000 | 10 000 | 720 | 90.68 | +2% | `baseline` |
| `circuit_breaker_set` | 10 000 | 10 000 | 720 | 92.97 | +4% | `baseline` |
| `build_policy` | 10 000 | 10 000 | 720 | 100.65 | +13% | `baseline` |
| `concurrency_key` ⚠ | 10 000 | 10 000 | 657 | 664.49 | **+644%** | `baseline` |
| `double_backlog` ⚠ | 20 000 | 20 000 | 325 | 1 324.88 | **+1383%** | `baseline` |
| `paused_rows` ⚠ | 20 000 | 10 000 | 322 | 1 342.69 | **+1%** | `double_backlog` |
| `all_gates` ⚠ | 20 000 | 10 000 | 257 | 1 694.20 | **+1797%** | `baseline` |

⚠ Cut short by the per-scenario wall-clock budget; the percentiles describe the
`n` samples shown. A scenario that cannot finish 800 claims in three minutes is
itself a finding.

`double_backlog` is a **control, not a gate** — it seeds no predicate at all.

**How much of this reproduces.** Across six independent runs on the reference
machine, `build_policy` is the only row that repeats to the point: +15%, +13%,
+15%, +13%, +12%, +13%. `rate_limited` and `circuit_breaker_set` each land
within a few points of zero and have **swapped rank with each other** between
runs (one run put `rate_limited` at −3%, two put `circuit_breaker_set` at −1% —
i.e. below `baseline`; the sixth reversed them again, +2% against +4%), so read
them as "free", not as an ordering: the gap between them is smaller than the
noise, and either can measure faster than a claim path that does strictly less
work. `concurrency_key` was +306%, +283%, +590%, +518%, +532% and +644%;
`paused_rows` vs `baseline` was +1319%, +1346%, +1321%, +1343%, +1301% and
+1403%. So: the
*classification* into free / modest / expensive is stable, and the expensive
rows are reproducibly expensive, but only `build_policy` and the paused-vs-
baseline figure are reproducible to better than a factor of two. Treat every
percentage here as one significant figure.

The table above is one representative run, not an average — averaging truncated
scenarios with different `n` would be worse than quoting one honestly. Every row
of the fourth and sixth runs fell inside the ranges quoted here, which is the
property that matters: the table is representative, not lucky.

### The control that changed the conclusion

An earlier revision of this page compared `paused_rows` against `baseline`,
measured +1319%, and concluded that the PAUSED-execution skip was the most
expensive predicate on the claim path. That conclusion was wrong, and the
`double_backlog` control is what caught it.

`paused_rows` seeds its PAUSED ballast *in addition to* the claimable backlog,
so it walks a table twice as deep as `baseline`. Claim latency is strongly
superlinear in depth (see [the sweep table](#claim-latency-vs-backlog-depth)),
so any delta measured against `baseline` charges the predicate for the extra
rows too. `double_backlog` removes that confound: same 20 000 total rows, all of
them plain and claimable, **no PAUSED predicate anywhere**.

It costs **+1383%**, within about a percent of what `paused_rows` costs. So the
operational finding stands on a depth-controlled comparison: at equal table
depth, a paused population costs what a live one does.

#### What that control does *not* establish

It is tempting to read the remaining **+1%** as "the anti-join predicate is
free". That reading does not survive looking at the two plans, and this page
does not make it.

`double_backlog` controls for *total rows in the table*. It does not control for
the population that reaches the sort, and that is where the claim query spends
its time (see [the plan](#the-plan)). The PAUSED anti-join is a `WHERE`
predicate, so it is evaluated *before* the `ORDER BY`:

| | `double_backlog` (control) | `paused_rows` |
|:--|:--|:--|
| rows scanned | 20 000 | 20 000 |
| rows surviving the filter | 20 000 | 10 000 |
| **rows fed to the sort** | **20 000** | **10 000** |
| PAUSED `SubPlan` | **never executed** | executed |

Two consequences. First, the control does not merely lack a PAUSED *population*
— its PAUSED `SubPlan` never runs at all, because every row it seeds is
`task_type = 'activity'` and the guard short-circuits on the type test (the same
mechanism described [at the end of the plan section](#the-plan)). Second, and
more importantly, the two scenarios sort *different numbers of rows*: the
anti-join removes half the table before the sort in `paused_rows` and removes
nothing in the control.

So the +1% is the sum of two effects with opposite signs — the anti-join's probe
cost, minus the sort saving from 10 000 fewer sorted rows — and this
measurement cannot separate them. (On a single-shot `EXPLAIN ANALYZE` the two
scenarios do not even take the same plan: the control gets a sequential scan
with a hash anti-join, `paused_rows` gets an index scan with a merge anti-join
and a hashed subplan.) The *sign* of the +1% also flips between runs — an
earlier run measured the control as the slower of the two, putting the figure at
−1% — which is what you would expect from two effects that roughly cancel,
measured at n=325/322 truncated samples. Do not read the ±1% as a direction, and
do not read it as a predicate cost.

**Isolating the predicate would need a control that matches the post-filter
population, not the pre-filter one** — same 20 000 rows scanned, same 10 000
sorted, excluded by a mechanism cheap enough not to be the thing under test.
That control does not exist yet, so no cost for the predicate in isolation is
published here.

So there are two different questions, and this page answers only one of them:

| comparison | question it answers | answer |
|:--|:--|--:|
| `paused_rows` vs `baseline` | what does a paused population cost an operator? | **+1403%** |
| `paused_rows` vs `double_backlog` | what does that cost at equal table depth? | **+1%** |

The second row is *not* the cost of the anti-join predicate; see the subsection
above for why the comparison cannot support that reading.

**The operational finding survives; the attribution is the part that does not.**
Pausing executions still does not take their work out of the claim path — the
rows stay `PENDING`, every worker still scans them on every claim, and a fleet
with a large paused population still pays roughly the same 15x that an equal
number of *live* rows would cost. That is the depth-controlled result, and it is
the actionable one: **drain the rows.** Whether the `NOT EXISTS (… state =
'PAUSED')` predicate is *additionally* expensive to evaluate is not settled by
these two scenarios, and it is not settled by the `all_gates` row either — that
row carries the same confound, in the same direction. `all_gates` also seeds
PAUSED ballast, so it too sorts 10 000 rows where `double_backlog` sorts 20 000;
comparing the two charges the predicates while *crediting* them with a sort half
the size. The +28% that comparison yields therefore bounds nothing in either
direction. Deriving a direction for it would mean adding the unmeasured
10 000-row sort saving back to the difference, which assumes the difference
decomposes into predicate cost plus sort cost. It does not: the two scenarios
also filter to different post-filter populations and can reach different plans,
so there is neither a measured term to add back nor an established direction for
the bias.

What *is* population-matched is the top of the table. `rate_limited`,
`circuit_breaker_set`, `build_policy` and `concurrency_key` all seed exactly the
10 000 claimable rows `baseline` does, so their deltas are clean — and they do
not support a "predicates are a rounding error" reading either:
`concurrency_key` costs **+644%** on an identical population. The defensible
statement is narrower than the one this page used to make: *depth* is the
dominant cost (~15x from 10k to 20k rows), *one* predicate is separately
expensive at this depth (`concurrency_key`, ~7.4x), and the combined cost of all
of them is not something these scenarios can bound in either direction.

What each row exercises, and what it means:

* **`circuit_breaker_set` (+4%)** — the worker passes a populated tracked-activity
  set, so the rate-limit gate and debit are skipped via `= ANY($5)` (#369).
  Free, as designed.
* **`rate_limited` (+2%)** — rows carry a `rate_limit_key` with a funded bucket,
  exercising the candidate-side `EXISTS` gate and the `rate_limit_debit` CTE
  (#332 / #699). Effectively free at this backlog.
* **`build_policy` (+13%)** — rows carry `required_build_id` and the worker's
  build matches only through a `harvest_build_compat` declaration, forcing the
  `EXISTS` branch rather than the cheap `required_build_id = $3` equality
  (#171). A real but modest cost; safe-deploy ramps (#604) are not expensive.
* **`concurrency_key` (+644%)** — rows carry `concurrency_key` + `concurrency_cap`,
  exercising both the candidate-side `COUNT(*)` subquery and the
  `pg_try_advisory_xact_lock` re-check in the `claimed` CTE (#247). This is
  measured with 256 distinct keys and a cap high enough never to block, so it is
  the *predicate* cost with contention deliberately minimised. **This is the one
  genuinely expensive predicate on the claim path — budget for it.** Its measured
  multiplier is also the least stable on this page: it grows as a run progresses
  (a shorter earlier run reported +306% over 174 samples; this 657-sample run
  reports +644%), which is consistent with the `COUNT(*)` subquery counting a
  `RUNNING` population that the benchmark itself is growing. Read it as
  "expensive and load-dependent", not as a fixed multiplier. **This
  predicate's candidate-side gate has since been fixed** — see
  [the concurrency-key gate fix](#the-concurrency-key-gate-fix).
* **`double_backlog` (+1383%)** — the control described above. Not a predicate:
  the cost of doubling table depth, full stop.
* **`paused_rows` (+1% vs the control)** — 10 000 rows belonging to PAUSED
  executions on top of the claimable backlog, exercising the `NOT EXISTS`
  anti-join (#383). Expensive as table depth. The +1% is *not* the predicate's
  isolated cost — the control sorts 20 000 rows where this scenario sorts
  10 000, so the two effects cancel to an unknown degree; see
  [what that control does not establish](#what-that-control-does-not-establish).
* **`all_gates` (+1797%)** — every gate at once. Read it as "a deployment using
  all of these features", **not** as a strict upper bound on the claim path: the
  circuit-breaker tracked set short-circuits the rate-limit `EXISTS` and the
  debit CTE (`= ANY($5)` wins, #369), so a deployment with rate limiting and
  *no* breaker executes strictly more work per claim than this row does. It is
  reported against `baseline` because there is no comparand that would make a
  depth-controlled reading sound: `all_gates` seeds the same PAUSED ballast
  `paused_rows` does, so it *scans* 20 000 rows like `double_backlog` but
  *sorts* only 10 000. Comparing the two (which yields +28%) charges this row
  for every predicate while crediting it with half the sort, so that figure is
  not interpretable as a bound in either direction and is not quoted as one.
  See [what that control does not
  establish](#what-that-control-does-not-establish).

## The plan

`EXPLAIN (ANALYZE, BUFFERS)` of a single headline claim, trimmed to the nodes
that matter (the benchmark prints it in full). The worker-id literal is
shortened to `'worker-0'` for width; the real bind is
`'harvest-bench-worker-0'`. Nothing else in the shown nodes is edited — the
absence of `cost=`/`actual time=` is `COSTS OFF, TIMING OFF`, and the folded
`priority DESC` is the planner constant-folding an unused bind.

Note that this plan is for the **claim statement**, which is the dominant part
of — but not all of — the operation the tables above time (see
[what is actually timed](#what-is-actually-timed)).

```text
->  Limit (actual rows=1 loops=1)
      Buffers: shared hit=20224
      ->  LockRows (actual rows=1 loops=1)
            ->  Sort (actual rows=1 loops=1)
                  Sort Key: (CASE WHEN ((sticky_worker_id = 'worker-0')
                             AND (sticky_until > now())) THEN 1 ELSE 0 END) DESC,
                            priority DESC, scheduled_at
                  Sort Method: quicksort  Memory: 1400kB
                  Buffers: shared hit=20223
                  ->  Nested Loop Anti Join (actual rows=10000 loops=1)
                        ->  Seq Scan on harvest_task_queue (actual rows=10000 loops=1)
                              Buffers: shared hit=223
                        ->  Index Scan using harvest_queue_pauses_pkey
                              on harvest_queue_pauses qp (actual rows=0 loops=10000)
                              Buffers: shared hit=20000
```

Three things to read here:

1. **`Seq Scan` + `Sort`, not an index scan.** `idx_harvest_tq_poll` is
   `(queue_name, state, priority DESC, scheduled_at) WHERE state = 'PENDING'` —
   exactly the right index for the *filter*, but it cannot serve the *ordering*,
   because the claim query's leading sort key is a `CASE` expression on
   `sticky_worker_id`/`sticky_until` (#235), which is not indexable. One
   non-indexable leading key is enough: the remaining `priority DESC,
   scheduled_at` cannot rescue it. So every claim reads and sorts all eligible
   pending rows to return one. That is the superlinear scaling in the table
   above.

   > **Follow-up (issue #1177):** the `CASE` key is *sufficient* to force this
   > plan shape, but it is not *necessary* — removing it would not restore
   > sort-elision, because any one of ten other residual `WHERE` predicates
   > issue #1177 tested independently forces the same collapsed shape (a
   > full-backlog scan plus `Sort`), including several that are total no-ops
   > at 100% selectivity; the query carries an eleventh, untested by that
   > issue. See
   > [any residual predicate defeats sort-elision](#any-residual-predicate-defeats-sort-elision-issue-1177)
   > below. Point 1 above remains accurate as far as it goes; it is incomplete
   > as an explanation of the superlinear scaling, since dropping the `CASE`
   > alone would not fix it.
2. **`actual rows=10000` feeding a `Limit 1`.** The plan materialises and sorts
   ten thousand rows in order to return a single task. That ratio — not the
   absolute time — is the shape of the problem, and it is why doubling the
   backlog doubles the work per claim.
3. **`loops=10000` on the queue-pause anti-join.** The `NOT EXISTS` against
   `harvest_queue_pauses` (#619) runs once per candidate row — 20 000 of the
   20 223 buffer hits. It is an index lookup, so it is cheap *per row*, but it is
   paid per row.

   > **Follow-up (issue #619 fix):** this node describes the *pre-fix* shape of
   > `claim_task_query()`. The scenario above — like every scenario on this
   > page — evaluates the predicate against an always-*empty*
   > `harvest_queue_pauses` (see [known limitations](#known-limitations)), so
   > even this `loops=10000` never ran against a queue that was genuinely
   > paused. [The queue-pause anti-join fix](#the-queue-pause-anti-join-fix)
   > below measures the identical `loops=N` pattern against an *actively
   > paused* queue directly, confirms the mechanism, and replaces the
   > correlated anti-join with a one-time prefilter. Points 1 and 2 above are
   > unaffected by that fix and remain accurate for the current query.

One number in the full output is deliberately **not** comparable to the tables
above: this plan's `Execution Time` was 1 287 ms, against a measured p50 of
89 ms for the same scenario. The `EXPLAIN` is a single *cold* claim on a fresh
connection against a freshly-seeded table — precisely the plan-cache and
buffer-cache cost the warmup trim exists to exclude. Read the plan for its
*shape*; read the tables for timing.

In the full plan (which the benchmark prints, and which is trimmed out of the
excerpt above) the `SubPlan` nodes for the concurrency, rate-limit, capability
and PAUSED predicates all show `never executed` on a baseline seed. Each is
guarded by a cheap leading test that is false for every baseline row, so the
subplan never runs: the concurrency and rate-limit guards short-circuit on
`concurrency_key IS NULL` / `rate_limit_key IS NULL`, while the PAUSED and
capability guards short-circuit on a **type** test (`task_type <> 'workflow'`),
not a NULL test — the baseline seeds activity rows. That is why the cheap gates
in the attribution table are cheap: you only pay for a predicate when you
actually use the feature.

**None of this is fixed by issue #786.** Per that issue's scope, the claim query
is left byte-for-byte unchanged; measuring it and tuning it are separate pieces
of work, and tuning without a published baseline is how you get an unfalsifiable
"optimisation". This page is the baseline.

## Any residual predicate defeats sort-elision (issue #1177)

[The plan](#the-plan) above shows the sticky-routing `CASE` expression
defeating `idx_harvest_tq_poll`'s ability to serve the `ORDER BY`. Read on its
own, that finding invites a natural next step: drop the `CASE` (or index the
sticky columns) and the ordering falls back to `priority DESC, scheduled_at` —
exactly `idx_harvest_tq_poll`'s key — so the cheap plan should return.

**It does not.** Issue #1177 reproduces that, with the `CASE` removed entirely
from `ORDER BY` (leaving only `priority DESC, scheduled_at` — an exact match
for `idx_harvest_tq_poll`'s key) and **no planner hints in play**, adding
**any single one** of ten other residual `WHERE` predicates it tested —
including several with **zero actual selectivity** (100% of rows pass the
filter) — is already enough on its own for the planner to choose a
full-backlog scan (`Seq Scan` for most predicates tested, `Bitmap Heap Scan`
for a few) plus a `Sort`, instead of the ordered index scan. This holds with
and without `FOR UPDATE SKIP LOCKED`. (The query carries an eleventh
residual predicate this reproduction did not test — see below.)

Ten predicates were tested independently against a 255 020-row fixture
(119 940 PENDING rows in the `default` queue), each added alone to the base
`queue_name = ANY($1) AND state = 'PENDING' AND scheduled_at <= NOW()` query
with `ORDER BY priority DESC, scheduled_at ASC LIMIT 1`: the sticky/session
OR-chains (#235, #606), the queue-pause anti-join (#619), the
`required_build_id` `EXISTS` (#171), the PAUSED-workflow `NOT EXISTS` (#383),
both capability-label predicates (#382), the `rate_limit_key` `EXISTS`
(#332/#699), the `schedule_to_close_at` check (#378), and the concurrency-key
gate (#247). **All ten** independently reproduce the collapse — even the
ones that are total no-ops in the fixture (`Rows Removed by Filter: 0`).
That figure is the *actual*, execution-time row count, not the planner's
pre-execution selectivity *estimate*; by itself it doesn't prove the
estimate was accurate, so it doesn't on its own rule out a selectivity
misestimate. What does rule that out is the separate diagnostic below: it
shows the sort-elision candidate isn't rejected on a cost comparison at all
— it is never generated as a candidate in the first place, regardless of
what any selectivity estimate says.

**These ten are not the query's complete set of residual predicates.**
`claim_task_query()` also carries an eleventh: the activity-pause exclusion,
`NOT (activity_name = ANY(paused_activities.names))` (issue #807) —
structurally the same array-membership anti-join shape as the queue-pause
predicate above (#619). Issue #1177's reproduction did not test it; nothing
above should be read as covering it. Issue #1215 tested it separately, with
`harvest_activity_pauses` actually populated (every predicate above was
tested against an empty pause table) rather than as a structural no-op, and
found it triggers the claim sort's disk spill at roughly 10x lower backlog
depth than this issue's own no-op-predicate threshold — a materially
different, and independently interesting, cost profile from the one
established here.

**A separate, narrower diagnostic goes further, for the sticky-routing
predicate specifically, under `FOR UPDATE SKIP LOCKED`.** With the competing
`idx_harvest_tq_coverage_sample` index hidden and `enable_seqscan=off;
enable_bitmapscan=off` set (session-local, inside a rolled-back transaction)
to bias the planner away from those plan types — these are cost penalties,
not a hard directive, which is exactly why the natural, unhinted
ten-predicate results above still show `Seq Scan`/`Bitmap Heap Scan` for most
rows rather than being universally overridden — Postgres does walk a
**serial** `Index Scan` on `idx_harvest_tq_poll` for the sticky predicate
(not a parallel one here, so there is no `Gather`/`Gather Merge` question to
resolve for this specific plan), already producing rows in the required
order. But it still inserts a `Sort` node on top and materialises every
matching row before applying `LIMIT 1` — genuinely redundant, since a serial
scan over an index whose key already matches the `ORDER BY` needs no further
sorting.

Two separate, compounding effects are at work, not one:

1. **In this reproduction, every residual `Filter` tested defeats
   sort-elision and `LIMIT` pushdown**, independent of `FOR UPDATE` — shown
   directly by the forced-index diagnostic above for the sticky predicate,
   and consistent with (though not independently re-run as the same
   diagnostic for) the other nine predicates' natural-planner results. For
   the sticky predicate, the sort-elision/limit-pushdown candidate plan is
   not generated at all once its residual `Filter` sits on the scan; this is
   not a cost-based choice of a worse plan over a better one the planner
   considered.

   **This is not a general Postgres rule, and this page does not claim it is
   one.** `idx_harvest_tq_coverage_sample`'s own migration
   (`20260718000000_harvest_queue_coverage_sample_index/up.sql`) documents
   the opposite case in this same codebase: `sample_execution_ids`'s
   `workflow_exec_id IS NOT NULL` filter is not itself index-satisfied, is
   evaluated per candidate row during the same ordered walk, and the scan
   *does* still stop early at `LIMIT 5` without a `Sort` node — for every
   queue except a pathological one. Whatever distinguishes
   `claim_task_query()`'s tested predicates from that case — `SubPlan`-bearing
   filters (`EXISTS`, `jsonb_array_elements`) versus a plain scalar NULL
   check, or something else — is not established here. What issue #1177
   establishes is narrower and still load-bearing: for the specific query and
   predicates tested, sort-elision does not survive adding any one of them;
   that is demonstrably not a `CASE`-key-specific problem, but it is not
   shown to be a universal one either. With or without the `CASE` key, this
   alone makes every claim O(backlog) in this fixture.
2. **`FOR UPDATE SKIP LOCKED` additionally disables the bounded Top-N sort**
   once (1) has already forced a `Sort` node to exist for the locked variant.
   Without `FOR UPDATE`, the same sticky-predicate diagnostic restores a
   bounded Top-N heapsort (in-memory, no disk spill) — it still scans the
   full eligible set to get there, but stays in memory, whereas the locked
   variant's sort is unbounded and spills to disk past a few hundred
   thousand rows (`Sort Method: external merge Disk: 5640-7057kB` in the
   #1177 fixture). This unlocked comparison uses a **parallel**
   `Parallel Index Scan using idx_harvest_tq_poll`, per the issue's own
   excerpt, rather than the serial scan in (1); the excerpt doesn't show
   whether a `Gather` or `Gather Merge` sits above it, so — unlike the locked
   case — this page does not claim that unlocked sort is redundant, only that
   it stays bounded and in-memory rather than spilling to disk. The
   bounded-versus-unbounded/disk-spill contrast holds regardless of that
   ambiguity, since both figures come from the same measured `EXPLAIN`
   output.

A semantically-identical rewrite — the ordered scan wrapped in a subquery,
with the residual filter applied as an outer `WHERE` — does not help either,
for the same sticky-predicate case; the planner flattens it back into the
identical collapsed shape. This is not a syntax-sensitivity quirk with a
free rewrite.

**This reproduction is issue #1177's own**, cited here rather than
independently re-run for this page. Unlike
[the queue-pause anti-join fix](#the-queue-pause-anti-join-fix) and
[the concurrency-key gate fix](#the-concurrency-key-gate-fix), it has not
(yet) been folded into `claim_bench_support.rs`'s scenario harness or given
a `docs/perf-artifacts/` capture of its own — doing so is future work, not a
blocker for correcting the attribution here. One predicate needs its own
caveat: the concurrency-key row above was captured against the correlated
`COUNT(*)` shape that predated
[the concurrency-key gate fix](#the-concurrency-key-gate-fix) below, which
has since replaced it with a CTE-backed lookup. That specific predicate's
contribution to the collapse has not been independently re-tested against
the current query; the other nine are unaffected by that fix and remain as
implemented today.

**Multiple queues: partially controlled for, not fully.**
`idx_harvest_tq_poll` leads with `queue_name`; for `queue_name = ANY($1)`
over several values, its output is grouped by queue rather than necessarily
a single global `priority`/`scheduled_at` order, and merging those groups
can itself require a `Sort` — independent of any residual predicate. Issue
#1177's own baseline (the identical `queue_name = ANY($1)` binding, no added
predicate — "each added alone to the base query") already functions as a
same-array no-residual control: it shows `Index Scan using
idx_harvest_tq_poll`, **no `Sort` node at all**, whatever `$1` held in that
reproduction. That rules out multi-queue ordering as the explanation for the
ten-predicate collapse *in that fixture specifically* — the `Sort` those ten
scenarios needed is absent from the zero-predicate baseline run against the
identical binding. What remains unconfirmed: issue #1177's own text does not
say how many queue names `$1` actually held (its fixture description
mentions rows seeded into a single `default` queue, which would make this a
non-issue for that reproduction specifically), and this page's own
attribution-table scenarios default `Scenario.queues` to 4 (see
[known limitations](#known-limitations)) — a separate harness this
reproduction was not run against. Whether the ten-predicate finding
transfers to a genuinely multi-queue bind has not been checked here.

**What this means for the query as it stands today:** there is no realistic
deployment shape that gets the cheap index-ordered plan back, because
`claim_task_query()` always carries at least the `schedule_to_close_at`
check, the sticky/session OR-chains, and the queue-pause and activity-pause
anti-joins unconditionally — dropping just the `CASE` key would not be
sufficient, and no single index can make the `sticky`/`session`/
`schedule_to_close` scalar checks, the concurrency-key gate, three different
`EXISTS` subqueries against three different tables, and the
`jsonb_array_elements` capability walk simultaneously sargable against one
ordered index — eleven residual predicates in total, not the ten this
section's own reproduction tested (see above).

**This page does not propose a query change for it.** Per the same
measure-before-tune discipline issue #786 established, a genuine fix here
looks architectural — e.g. a seek-and-refine restructuring (claim an ordered
batch of candidate ids, apply the residual filters and `FOR UPDATE SKIP
LOCKED` to the small batch, retry on an empty batch) — and that changes
claim-fairness/latency guarantees under contention in ways that need
checking against this hot path's documented advisory-lock-ordering,
exactly-once-claim, and `SKIP LOCKED`-concurrency-safety invariants by
someone with full context on `queue.rs`. It is out of scope for this page and
is not decided here; it is tracked separately as issue #1340.

This also corrects, without fully resolving, the
[known limitations](#known-limitations) bullet that called `schedule_to_close`
(#378), worker sessions (#606) and sticky routing (#235) "cheap inline column
tests": reproduced here, each independently defeats sort-elision regardless
of the value it is tested against, so "cheap" was never an established
finding — it was this page's own retracted reading of their *plan-eligibility*
effect. Their marginal *cost* on the attribution table above is a different
question: in the full production query the `CASE` key and the always-present
queue-pause/`schedule_to_close`-adjacent predicates already force the same
collapsed plan shape regardless of any one of these three predicates, so
none of their incremental contributions can be isolated through a
plan-shape change — that needs the seed-variant scenario work
[known limitations](#known-limitations) already calls for. That work has
since been done for all three — `schedule_to_close` (#378, PR #1339),
worker sessions (#606, PR #1358), and sticky routing (#235,
[`docs/performance-sticky-routing.md`](performance-sticky-routing.md)) — see
the [known limitations](#known-limitations) bullet above — by holding the
same already-collapsed plan shape fixed and measuring each column's
marginal buffer/storage cost directly, rather than trying to isolate it
through a plan-shape change that #1177 shows does not happen either way.

**Zero engine impact.** Like issue #786 and every fix on this page, this
finding changes nothing about `claim_task_query()`: no new `WorkflowEvent`
variant, no migration, no schema change, no public API change, and the claim
query is byte-for-byte unchanged. This page is the measurement, not the fix.

## The queue-pause anti-join fix

Point 3 above — `loops=10000` on the queue-pause anti-join — describes a real
cost, but every scenario elsewhere on this page evaluates it against an empty
`harvest_queue_pauses` (see [known limitations](#known-limitations)), so it was
flagged, never measured. This section closes that gap with a dedicated harness
variant that seeds an *active* pause on one of the worker's polled queues, then
fixes the predicate the measurement indicts.

**Mechanism.** `claim_task_query()`'s pre-fix anti-join
(`NOT EXISTS (SELECT 1 FROM harvest_queue_pauses qp WHERE qp.queue_name =
harvest_task_queue.queue_name)`) is *correlated*: Postgres re-evaluates it once
per candidate row the outer scan visits, not once per claim. The fix replaces
it with a `MATERIALIZED` CTE that reads the (small, low-cardinality) pause
table once per claim into an array, then tests membership with a plain
`<> ALL(...)`:

```sql
paused_queues AS MATERIALIZED (
    SELECT COALESCE(array_agg(queue_name), ARRAY[]::text[]) AS names
    FROM harvest_queue_pauses
    WHERE queue_name = ANY($2)
)
...
CROSS JOIN paused_queues
...
NOT (harvest_task_queue.queue_name = ANY(paused_queues.names))
```

The array is bounded by `$2` — the worker's own polled-queue list, typically
single digits — never a scan of the whole pause table.

**Measurement.** One cold claim (`EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS,
TIMING OFF)`), four polled queues, one of the four actively paused, against the
reference environment above. Full artifacts (before/after `EXPLAIN` at each
backlog depth, plus a `pg_stat_statements` snapshot) are committed under
[`docs/perf-artifacts/queue-pause-claim-anti-join/`](perf-artifacts/queue-pause-claim-anti-join/).

| Backlog | Buffers (before) | Buffers (after) | Δ |
|--:|--:|--:|--:|
| 1 000 | 1 292 | 47 | **-96.36%** |
| 10 000 (headline) | 12 743 | 248 | **-98.05%** |
| 100 000 | 9 008 | 2 251 | **-75.01%** |

At 1k and 10k the mechanism is exactly the one the plan above describes: the
anti-join subnode itself goes from `loops=10000, Buffers: shared hit=12500`
(98.1% of the statement's own buffer cost at 10k) to a CTE evaluated
`loops=1, Buffers: shared hit=5`. At 100k the picture is more interesting:
Postgres's *own* planner already escapes the `loops=N` shape in the **pre-fix**
plan — it switches to a `Merge Anti Join`, since one side (the one-row pause
set) sorts for free — so the anti-join itself is cheap there either way (~2
buffer hits before, ~5 after). The 100k delta instead comes from a secondary,
plan-shape effect the rewrite enables: once the correlated form is gone, the
planner no longer needs *sorted* input from the base table to support a merge,
and switches the base scan from an index scan (`idx_harvest_tq_poll`, 8 983
buffers) to a plain sequential scan (2 223 buffers) — cheaper here because the
table fits comfortably in a handful of large sequential reads. This is reported
honestly rather than folded into "the same mechanism at every scale": the fix
is unambiguously good everywhere measured, but *why* varies with the
scale-dependent plan Postgres already chooses.

**Corroboration.** Wall-clock execution time moved in the same direction as
buffers by more than 2x only at the headline scale (1 372 ms → 123 ms, 11.2x)
— the bar this page's own methodology sets for treating wall-clock as
corroborating evidence. At 1k it improved modestly (2.16 ms → 1.56 ms, well
under 2x) and at 100k it was flat (1 401 ms → 1 421 ms) — the "after" plan
spills *more* to a temp-file external sort at that scale (810 → 1 417 pages
written), because the wider intermediate row (each candidate row now carries
`paused_queues.names` through the join before the anti-join filter narrows it)
costs more to sort even though fewer buffers are touched to produce it.
Buffers, not wall-clock, is what this fix is measured against; the 100k
wall-clock flatness is reported for completeness, not hidden.

**Cumulative, real-claim-loop evidence.** A `pg_stat_statements` snapshot of
7 501 real `claim_task()` calls draining the full 10k-row headline backlog (one
queue paused throughout): total buffers **18 671 000 → 3 773 247**
(**-79.79%**). Lower than the single-cold-claim -98.05% because later claims in
the drain face a shrinking, already-less-pathological candidate set on both
sides of the fix — expected, not a discrepancy.

**Equivalence.** Both before and after runs claim the identical 7 500 of 10 000
rows and never touch the actively paused queue's rows (proven end-to-end by
`tests/integration/queue_pause_tests.rs::claim_query_excludes_paused_queues_end_to_end`,
which also covers resuming the queue). Reproduce with
`autumn-harvest/scripts/queue_pause_claim_perf_repro.sh`, which needs either
`HARVEST_TEST_DATABASE_URL` (an admin connection string) or a reachable Docker
daemon for its testcontainer fallback — not both.

## The pause-array-size sweep (issue #1215)

The fix above closes the queue-pause anti-join's per-row cost, but every
measurement on this page still tests both pause tables at a single array
size: one active pause, or none. Issue #1215 swept array size instead — 0,
1, 20 and 199 ballast rows, each excluding zero real candidate rows (0%
selectivity, isolating array width from backlog depth the same way issue
#1177 isolates predicate presence from selectivity). The `paused_activities`
sweep is crossed against every depth in the published `BACKLOG_SWEEP`
(1,000 / 10,000 / 100,000), not held at the headline depth alone — the
queue-pause sweeps stay at the 10,000-row headline, since they answer a
yes/no bound question the query already settles identically at every depth,
not a magnitude question depth could shift. Full artifacts are committed
under [`docs/perf-artifacts/pause-array-size/`](perf-artifacts/pause-array-size/),
reproducible via
`autumn-harvest/scripts/pause_array_size_claim_perf_repro.sh`.

| Predicate | Backlog | Worker's own `$2` | Ballast pauses seeded | `paused_*` array size | Sort method |
|:--|--:|:--|--:|--:|:--|
| `paused_activities` (#807) | 1 000 | 4 queues | 0 / 1 / 20 | 0 / 1 / 20 | quicksort, in memory |
| `paused_activities` (#807) | 1 000 | 4 queues | 199 | 199 | external merge, 6 368kB disk |
| `paused_activities` (#807) | 10 000 | 4 queues | 0 / 1 | 0 / 1 | quicksort, in memory |
| `paused_activities` (#807) | 10 000 | 4 queues | 20 | 20 | external merge, 7 504kB disk |
| `paused_activities` (#807) | 10 000 | 4 queues | 199 | 199 | external merge, 63 656kB disk |
| `paused_activities` (#807) | 100 000 | 4 queues | 0 | 0 | external merge, 15 280kB disk |
| `paused_activities` (#807) | 100 000 | 4 queues | 1 | 1 | external merge, 18 432kB disk |
| `paused_activities` (#807) | 100 000 | 4 queues | 20 | 20 | external merge, 74 992kB disk |
| `paused_activities` (#807) | 100 000 | 4 queues | 199 | 199 | external merge, 635 488kB disk |
| `paused_queues` (#619) | 10 000 | 4 queues (typical) | 0 / 1 / 20 / 199 | 0 (none of these ballast queues are in `$2`) | quicksort, in memory |
| `paused_queues` (#619) | 10 000 | 203 queues (atypical) | 0 | 0 | quicksort, in memory |
| `paused_queues` (#619) | 10 000 | 203 queues (atypical) | 199 | 199 | external merge, 40 704kB disk |

For `paused_activities`, ballast seeded and array size are always equal — it
reads the whole table unconditionally, so nothing filters the array down.
For `paused_queues`, they diverge exactly when `$2` excludes the ballast:
the typical-worker rows above seed up to 199 pauses but never widen the
array past zero, because `$2` (this worker's 4 polled queues) never
includes any of the seeded names. The Sort Method column tracks array
size, not ballast count, in every row — consistently zero disk cost while
the array stays at zero, regardless of how large the underlying pause
table grows.

**`paused_activities` has no bound to protect it, and the array-size
threshold that spills it is itself lower at greater backlog depth.** It
reads the whole `harvest_activity_pauses` table on every claim, so array
size tracks the pause table's total population directly. At the
10,000-row headline depth, twenty paused activity types — a realistic
response to a multi-service incident, not an edge case — is enough to
spill the claim sort to disk; that is far below the
[few-hundred-thousand-row depth](#any-residual-predicate-defeats-sort-elision-issue-1177)
issue #1177's own locked-scenario reproduction needed to trigger the same
spill against an empty pause table. At 1,000 rows the threshold is higher
(between 20 and 199). At 100,000 rows this sweep found the sort already
spilling with **zero** paused activities — a backlog-depth-driven spill
this page's own [claim-latency-vs-backlog-depth table](#claim-latency-vs-backlog-depth)
is already consistent with, independent of this predicate; array size
still compounds it further there, from 15 280kB at zero paused activities
to 635 488kB at 199.

**`paused_queues` stays cheap only while the worker's own bind stays
small.** [The `$2` bound above](#the-queue-pause-anti-join-fix) keeps a
typical worker's array width capped at its own polled-queue count, so 199
fleet-wide pauses on queues this worker never polls never widened its array
past zero real elements, and the sort stayed in-memory throughout. The same
mechanism does reappear once a worker's own `$2` bind is itself wide:
pairing 199 polled queues with 199 matching pauses reproduced the identical
disk-spill shape. A worker subscribed to hundreds of distinct queues is not
this page's measured or expected deployment shape (`Scenario.queues` holds
at 4 everywhere else on this page), so this is reported as a confirmed
mechanism, not a claimed realistic exposure — unlike `paused_activities`,
whose exposure needs no unusual worker shape at all.

**Zero engine impact.** Like every other finding on this page, this changes
nothing about `claim_task_query()`: no code-shape fix is proposed here, only
a documented cost and a committed regression surface (see
`tests/integration/claim_budget_tests.rs::zz_capture_pause_array_size_claim_evidence`).

## The concurrency-key gate fix

The `concurrency_key` row above — flagged as "the one genuinely expensive
predicate on the claim path" — was measured, not yet fixed, when this page
first shipped. This section closes that gap: it fixes the predicate the
attribution table indicts and measures the result against the same headline
scenario the +644% figure came from.

**Mechanism.** `claim_task_query()`'s pre-fix per-key concurrency gate
(issue #247) is a *correlated* `COUNT(*)` subquery on the candidate side:

```sql
( concurrency_key IS NULL
  OR concurrency_cap IS NULL
  OR ( SELECT COUNT(*) FROM harvest_task_queue inner_q
       WHERE inner_q.concurrency_key = harvest_task_queue.concurrency_key
         AND inner_q.task_type = harvest_task_queue.task_type
         AND inner_q.state = 'RUNNING'
         AND inner_q.worker_id IS NOT NULL
     ) < harvest_task_queue.concurrency_cap
)
```

Postgres re-evaluates this once per candidate row the outer scan visits, not
once per claim — the same anti-pattern as the queue-pause predicate above, but
here the subquery scans `harvest_task_queue` for `RUNNING` rows sharing the
same key rather than a small operator-facing pause table, so its cost is
*load*-dependent (it grows with the `RUNNING` population), not just
*depth*-dependent. The fix replaces it with two `MATERIALIZED` CTEs, computed
once per claim rather than once per candidate row:

```sql
concurrency_pending_keys AS MATERIALIZED (
    SELECT DISTINCT concurrency_key, task_type
    FROM harvest_task_queue
    WHERE queue_name = ANY($2)
      AND state = 'PENDING'
      AND scheduled_at <= NOW()
      AND concurrency_key IS NOT NULL
      AND concurrency_cap IS NOT NULL
),
concurrency_running_counts AS MATERIALIZED (
    SELECT t.concurrency_key, t.task_type, COUNT(*) AS running_count
    FROM harvest_task_queue t
    WHERE t.state = 'RUNNING'
      AND t.worker_id IS NOT NULL
      AND t.concurrency_key IN (SELECT concurrency_key FROM concurrency_pending_keys)
    GROUP BY t.concurrency_key, t.task_type
)
...
( concurrency_key IS NULL
  OR concurrency_cap IS NULL
  OR COALESCE((
       SELECT rc.running_count FROM concurrency_running_counts rc
       WHERE rc.concurrency_key = harvest_task_queue.concurrency_key
         AND rc.task_type = harvest_task_queue.task_type
     ), 0) < harvest_task_queue.concurrency_cap
)
```

`concurrency_pending_keys` bounds the second CTE's join to only the keys
actually present in the current backlog (never the whole `RUNNING`
population), and both CTEs are evaluated once regardless of how many
candidate rows are later filtered against them. The `claimed` CTE's
authoritative, race-safe recheck — the same correlated `COUNT(*)`, guarded by
`pg_try_advisory_xact_lock(hashtext(candidate.concurrency_key)::bigint)`, run
once on the single winning row after it is already locked — is untouched: the
fix rewrites only the *filtering* pass over many candidate rows, not the
*authoritative* recheck on the one row that wins the claim. Confirmed
byte-for-byte identical before/after: at the hot-contention scale below, both
plans show the identical
`Aggregate (actual rows=1 loops=1) Buffers: shared hit=10` /
`Bitmap Heap Scan ... Heap Blocks: exact=8` subtree for that recheck node.

**Measurement.** `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)`
of a single cold claim, isolated to the concurrency-check subtree specifically
— the part of the plan this fix changes — against the reference environment
above. Full artifacts (before/after `EXPLAIN` at each `BACKLOG_SWEEP` depth,
plus a hot-contention variant and a `pg_stat_statements` snapshot) are
committed under
[`docs/perf-artifacts/concurrency-key-claim-predicate/`](perf-artifacts/concurrency-key-claim-predicate/).

| Scenario | Buffers, concurrency-check subtree (before) | Buffers (after) | Δ |
|:--|--:|--:|--:|
| idle, backlog=1 000 | 1 000 (`loops=1000`) | 1 (CTE never executed; the `RUNNING`-side probe short-circuits) | **-99.9%** |
| idle, backlog=10 000 (headline) | 10 000 (`loops=10000`) | 1 (never executed) | **-99.99%** |
| idle, backlog=100 000 | 100 000 (`loops=100000`) | 1 (never executed) | **-99.999%** |
| hot contention (10 000 backlog + 2 000 `RUNNING` rows spread across the same ~256 keys) | 98 124 (`loops=10000`) | 733 (`concurrency_pending_keys`: 333 + `concurrency_running_counts`: 400, both `loops=1`) | **-99.25%** |

At every idle depth, Postgres's own planner *lazily skips*
`concurrency_pending_keys` entirely — the plan reports it `(never executed)`
— because the cheaper `concurrency_running_counts` probe returns 0 rows first
via `COALESCE(..., 0)`, and nothing in an idle backlog has a `RUNNING` peer to
check. The pre-fix plan has no equivalent short-circuit: the correlated
subquery runs once per candidate row regardless of whether any `RUNNING` row
could possibly exist, so its buffer cost tracks the loop count 1:1 (1 000 →
10 000 → 100 000, exactly matching backlog depth). Under hot contention, where
the short-circuit can't fire (`concurrency_pending_keys` materializes
`actual rows=256 loops=1`), the pre-fix candidate-side aggregate instead costs
98 124 buffers across its 10 000 loops — each of those 10 000 independent
per-row probes now does real index-scan work against the populated `RUNNING`
rows — where the fixed version pays that cost exactly once, covering all 256
distinct keys in one pass.

A pre-existing external-merge sort spill at the 100 000-row idle depth
(`Sort Method: external merge  Disk: 18384kB`) is present, at the identical
disk size, in both the before and after plans — this fix does not introduce or
change it (see issue #1215, which targets a different part of the query).
Likewise the base-table scan feeding the `ORDER BY … LIMIT` is unchanged in
shape between before and after at every depth (see issue #1177) — this fix
touches only the concurrency-check predicate, not the candidate
ordering/pushdown. The specific scan type at the 100 000-row depth can itself
vary *across* separate script runs (an earlier run captured a `Seq Scan`
where this run captured an `Index Scan using idx_harvest_tq_poll`, purely
from `ANALYZE` statistics/row-layout differences between fresh fixture
builds — the same class of variance noted for the cumulative buffer count
below); what stays constant within a single run's before/after pair, and
what this claim actually depends on, is that the *before* and *after* halves
of the same run always match each other.

**Corroboration.** Wall-clock execution time for a *single* cold claim is
**not** admissible corroboration at idle scale: it is flat to slightly worse
at backlog=10 000 (116.7 ms → 143.4 ms) and backlog=100 000 (1 629.0 ms →
1 855.9 ms), neither >2x nor in the same direction as the buffer win — the
concurrency-check subtree shrinks from tens/hundreds of thousands of buffers
to essentially nothing, but that subtree is a small fraction of a single
query's total cost at idle scale, where the `ORDER BY`/sort work
[the plan](#the-plan) already identifies as the dominant cost still
dominates. Buffers, not wall-clock, is what the idle-scale rows above are
measured against; this page does not claim a single-query wall-clock win it
did not observe.

At hot contention, wall-clock moves the same direction as buffers by more
than 2x — the bar this page's own methodology sets for treating it as
corroborating evidence: **1 573.3 ms → 311.5 ms (5.05x)**, alongside the
-99.25% buffer reduction above.

**Cumulative, real-claim-loop evidence.** A `pg_stat_statements` snapshot of
10 001 real `claim_task()` calls draining the full 10 000-row headline
backlog (the same `ClaimGate::ConcurrencyKey` scenario the +644% p50 figure
was measured under — 256 distinct keys, cap high enough never to block):
total buffers **1 385 001 432 → 10 727 317** (**-99.23%**, 129.1x fewer
buffers for the identical drain). Far larger than the single-cold-claim
reduction above, because the pre-fix cost *compounds* across the drain: each
of the 10 000 sequential `claim_task()` calls independently re-scans the
remaining candidate backlog through its own correlated subquery, so the total
work across a full drain grows roughly with the *square* of backlog depth
(10 000 + 9 999 + … candidate-row evaluations, each paying its own per-row
subquery cost), where the fix's per-call cost stays flat — bounded by
distinct-key cardinality and the `RUNNING` population, not by the shrinking
remaining backlog.

The absolute *before* buffer count is sensitive to physical row/page layout
after `ANALYZE`: an earlier run of this same script measured
**2 492 987 808 → 10 938 903** on an identically-shaped fixture, roughly 1.8x
higher on the *before* side than the **1 385 001 432** figure above, because
the correlated subquery's cost depends on how `RUNNING`/`PENDING` rows happen
to land on disk, while the fixed version's flat, bounded CTE cost barely
moved between runs (10 938 903 → 10 727 317, -1.9%). The *relative* reduction
is what to trust across reruns: both landed in the same -99%/100x+ regime,
comfortably clearing this page's impact floor either way; reproduce it
yourself rather than pinning to either absolute number.

**Equivalence.** Both before and after runs claim the identical
10 000-of-10 000 claimable rows (`claimed=10000 of 10000 claimable` in both
`{before,after}-fixture-summary.txt`), with identical seeded/claimable row
counts at every swept depth and an identical hot-contention seed shape
(`running_rows_added=2000` in both). `calls=10001` matches exactly between
before and after in the `pg_stat_statements` snapshot — the fix changes
per-call cost, not the number of claim attempts needed to drain the backlog.
Correctness of the underlying enforcement is unchanged and covered by
`tests/integration/integration_e2e.rs`'s existing
`concurrency_cap_limits_concurrent_claims_cluster_wide`,
`concurrency_cap_shared_key_budget_is_not_doubled`,
`concurrency_cap_failure_frees_slot_and_does_not_wedge_queue`,
`concurrency_cap_null_key_tasks_are_unaffected_by_saturated_key` and
`per_key_concurrency_cap_enforced_across_fleet` (unmodified by this change; all
five pass unchanged against the fixed query). Two new unit tests —
`queue::tests::claim_query_concurrency_gate_matches_the_authoritative_recheck`
and
`queue::tests::concurrency_gate_ctes_are_defined_and_referenced_exactly_once_each`
— pin the query's shape directly. Reproduce with
`autumn-harvest/scripts/concurrency_key_claim_perf_repro.sh`, which needs
either `HARVEST_TEST_DATABASE_URL` (an admin connection string) or a reachable
Docker daemon for its testcontainer fallback — not both.

### Known limitation: cost scales with distinct concurrency-key cardinality

The committed hot-contention fixture above uses 256 distinct concurrency
keys — the harness's fixed `KEY_CARDINALITY` constant
(`tests/integration/claim_bench_support.rs`) — and at that cardinality the
fix is unambiguously a win (733 buffers vs 98 124, -99.25%). That number does
not generalize to arbitrarily many distinct keys, and this is a real,
confirmed limit of the fix, not a hypothetical one.

The candidate-side gate is a correlated scalar subquery —
`COALESCE((SELECT rc.running_count FROM concurrency_running_counts rc
WHERE rc.concurrency_key = … AND rc.task_type = …), 0) < concurrency_cap` —
evaluated once per candidate row. `concurrency_running_counts` is a
`MATERIALIZED` CTE, and a CTE has no index: PostgreSQL always resolves a
lookup against one with a linear `CTE Scan`, regardless of `MATERIALIZED` or
the CTE's own size. The committed
`after-claim-backlog-10000-hot-contention.explain.txt` already shows this
node — `CTE Scan on concurrency_running_counts rc (loops=10000)`, filtering
out 255 of 256 rows on every one of the 10 000 loops — it just costs little
enough at 256 keys (≈1-2 pages, fully cached) to stay invisible in the
subtree-total table above. Re-running the same shape with 5 000 distinct
keys instead of 256 (all other parameters held fixed: 10 000-row backlog,
2 000 `RUNNING` rows, `NON_BLOCKING_CAP`) reproduces a **1 600 ms** single
claim, worse than the pre-fix baseline's own hot-contention wall-clock
(1 573.3 ms, see above) — the fix's own per-candidate-row `CTE Scan` becomes
the dominant cost once distinct-key cardinality is large enough. The
underlying shape is O(candidate rows × distinct running key/type pairs);
256 keys keeps that product small, thousands of keys does not.

Three pure query rewrites were evaluated as replacements for the candidate-
side gate and rejected, each confirmed by `EXPLAIN (ANALYZE, BUFFERS,
VERBOSE, SETTINGS)` against a from-scratch reproduction of both the idle
(10 000-row, 256-key, zero `RUNNING`) and high-cardinality (5 000-key)
scenarios:

- **`LEFT JOIN` to a `GROUP BY`-aggregated running-count subquery.** Turns
  the per-row `CTE Scan` into a single hash lookup: 5 000-key hot-contention
  cost drops to 24.1 ms (66x faster than the correlated form, and fewer
  buffers: 377 vs 713). But it regresses the *idle* case from ~1-4 buffers
  (the correlated form's lazy `(never executed)` short-circuit) to ~232
  buffers at a 10 000-row idle backlog, because any `LEFT JOIN`-shaped
  formulation defeats PostgreSQL's ability to push `ORDER BY … LIMIT`
  through the ordered `idx_harvest_tq_poll` scan feeding the candidate
  selection.
- **`LEFT JOIN LATERAL`** — the same regression, same magnitude (idle-depth
  buffers move from ~1-4 to ~230, scaling linearly with backlog depth from
  there).
- **`LEFT JOIN LATERAL` with `enable_hashjoin`/`enable_mergejoin` disabled**
  (forcing a Nested Loop), and again with `enable_seqscan`/`enable_bitmapscan`
  also disabled (forcing the planner onto `idx_harvest_tq_poll` — the same
  index whose ordering the correlated form exploits) — neither recovers the
  idle-case short-circuit. Even driven by a plain, ordered `Index Scan` on
  the outer side, PostgreSQL still inserts an explicit `Sort` and evaluates
  the full joined result before `LIMIT` applies (buffers actually rose to
  916, since the plain Index Scan touches every leaf and heap page the
  Bitmap/Seq Scan alternatives could skip). This is not a planner-tuning
  gap: PostgreSQL cannot apply `LIMIT`-pushdown-through-ordered-scan to a
  join whose filter references the joined side, independent of which
  physical join algorithm is chosen — only a scalar subquery evaluated
  lazily in the outer `WHERE` clause gets that optimization, and a CTE
  cannot back one with an index.

The one formulation that would plausibly get both properties — a correlated
subquery in the same shape as the *authoritative* recheck in the `claimed`
CTE below (which already scans the base table, not a CTE, and is cheap
because it runs at most `LIMIT`-many times, not once per candidate) — needs
a new supporting partial index (e.g. on
`(concurrency_key, task_type) WHERE state = 'RUNNING' AND worker_id IS NOT
NULL`) to make each per-candidate-row lookup an indexed probe instead of a
CTE linear scan. Adding an index is outside what this PR changes
unilaterally; see the review discussion on this PR for the concrete proposal
and open question.

**That proposal was measured and killed:**
`docs/assays/0003-concurrency-gate-cardinality-index.md` (ledger #3) found
the partial-index rewrite fixes this exact 5,000-key blowup (~48.8x faster
than control) without regressing the 256-key case, but at zero `RUNNING`
rows it costs ~10,000 real per-candidate-row index probes where the current
fix costs ~10,000 near-free probes of a small, resident, empty CTE — 200x+
over its pre-set idle-cost line, at any key cardinality. Re-assaying this
exact formulation without new information is a re-dig; see that report for
what else remains untested.

**The un-re-chartered pit ledger #3 left open was also measured and
killed, a different way:**
`docs/assays/0004-concurrency-gate-deferred-recheck.md` (ledger #4) tried
removing the candidate-side gate entirely — no predicate, no new index —
and enforcing the cap only in the `claimed` CTE's existing authoritative
recheck, retrying against the next candidate on a failed recheck. Idle cost,
the 5,000-key blowup, and the 256-key case all pass decisively (idle ties
the committed fix; 5,000-key is 218.5x faster than control; 256-key is 30.4x
faster than control). It still kills, on a line neither #3 nor the committed
fix needed: a 50-row adversarial fixture where the highest-priority PENDING
rows are themselves keyed to an already-saturated concurrency key costs
313.8ms against a 100ms line, because each retry re-runs the full
candidate-selection scan and nothing bounds how many consecutive
high-priority rows can share a saturated key — an unbounded,
workload-dependent worst case neither prior candidate has. (`LEFT JOIN
LATERAL` + planner hints, the *other* shape #3 named, was never re-tested:
the three-rewrites section above already closes it.)

**A third shape — batching #4's per-row retry into a single-round-trip
per-batch fetch, the specific rewrite issue #1340 was deferred pending —
was measured and also killed, on its pre-registration's arithmetic and on a
narrower mechanism than first reported:**
`docs/assays/0005-claim-batched-seek-and-refine.md` (ledger #5) fetches the
top 50 ordered candidates per round trip, then walks them procedurally
applying the production path's own per-candidate advisory-lock-and-recheck
(`queue.rs:750-770`) rather than a batch-wide snapshot. Idle cost, the
5,000-key blowup, the 256-key case, and both adversarial fixtures'
wall-clock all pass decisively; batch-count scaling under adversarial depth
is linear, not catastrophic. It still kills: both adversarial fixtures
resolved in one more batch than their pre-registered "exactly N" line
allowed, because that line's own formula undercounted by the one slot the
claimable row itself occupies. Five rounds of post-review (Codex) further
found: the report had mischaracterized the candidate fetch as an
index-ordered seek through `idx_harvest_tq_poll`; the archived `EXPLAIN`
output shows a `Seq Scan` of the whole matching backlog instead (the same
shape the committed fix's own control query plans as, at this apparatus's
10,000-row depth), and a forced-index diagnostic shows forcing the index
doesn't recover a bounded scan either — still reads every matching row,
costs more, no `LIMIT` pushdown (a proposed alternative explanation, that
the assay's own added tiebreak column caused this, was checked directly
and did not hold up). Separately, the first fix's winner-pick used a stale
batch-wide snapshot with no serialization at all instead of the advisory
lock above — a real concurrency-correctness gap a single-session apparatus
can't surface on its own, fixed to match the mechanism ledger #4 already
had right (grading the fix against the original lines rather than
re-chartering was itself reviewed and defended: the lines never changed,
only an unsound implementation was corrected, and the fixture's own
adversarial scenarios already exercise the corrected mechanism's cost
without regressing). And the recheck's own cost, cited as "~1 buffer," is
34 buffers once the `RUNNING` population reaches 2,000 rows — cardinality
independence holds for distinct key count, not for `RUNNING` population
size, a distinction this apparatus's fixtures didn't separate. So the
assay's surviving claim is narrower than first reported: the per-candidate
recheck's cost is independent of distinct key count, and batching doesn't
cost more than the current (already `O(backlog)` at this depth) fix — not
that batching bounds cost as backlog depth grows, which remains untested.
That gap also surfaces an unresolved discrepancy against this page's own
#1177 baseline (reported there as a clean index scan with no `Sort` node,
at a much larger fixture); a corrected-arithmetic re-charter, a
depth-varying re-charter, that discrepancy, real concurrent-claimer
throughput, and cost when many in-batch rejections coincide with a large
`RUNNING` population all remain open, un-run pits.

Until a fix clears every line of some registered assay, deployments with
concurrency-key cardinality in the low hundreds (the tested, committed
range) get the full measured win above; deployments with concurrency keys
numbering in the thousands or more should expect the candidate-side gate's
cost to grow with that cardinality and are not covered by this fix's
evidence.

## Enqueue throughput

8 concurrent writers enqueueing into an already-populated queue:

| backlog | rows | n | p50 ms | p99 ms | max ms | rows/s |
|--:|--:|--:|--:|--:|--:|--:|
| 1 000 | 800 | 720 | 1.61 | 3.32 | 3.86 | 4 540 |
| 10 000 | 800 | 720 | 1.39 | 3.46 | 4.22 | 5 122 |
| 100 000 | 800 | 720 | 1.58 | 3.14 | 5.42 | 4 647 |

Enqueue is **flat in backlog depth** — a 100x deeper queue moves p50 by 0.2 ms,
and the throughput spread across the sweep is inside this box's run-to-run
noise (the *middle* backlog measured fastest here, and the deepest beat the
shallowest — throughput does not order by depth at all, which is the tell that
the variation is noise rather than depth). That is the expected and desired
asymmetry: reads pay for depth, writes do not. A
start-storm is bounded by your connection pool and by Postgres write throughput,
not by anything Harvest does.

Put the two sides together and the operational picture is stark: at a 100 000-row
backlog this machine sustains ~4 600 enqueues/s against ~3 claims/s — three
orders of magnitude apart. **A queue that deep does not drain.** Nothing in the
write path warns you about it; the backlog table above is the warning.

Two caveats on this table. `queue::enqueue` is not a bare `INSERT`: it resolves
defaults and writes one row inside its own transaction, so the per-row latency
includes transaction and round-trip overhead — which is exactly why it is worth
measuring rather than assuming. And the throughput column is a **floor**, not a
peak: it divides *all* rows — warmup included — by the shared
barrier-to-completion window defined under [Measurement
hygiene](#measurement-hygiene), which ends at the *slowest* writer, so a writer
that finishes early still counts toward the denominator. Task spawn and join sit
outside that window by construction, so read this as a floor on sustained
throughput, not as an end-to-end figure. The `n` column (post-warmup samples
behind the latency columns) is below the `rows` column by design.

## The CI gate

`claim_budget_tests::claim_p50_at_headline_scenario_is_within_budget` runs via
the `linux` row in `.github/ci/integration-suites.txt` — so, on Linux, on
code-touching changes — and fails the build when **p50** claim latency at the
**headline scenario** (10 000 pending rows, 8 concurrent claimers, 4 queues)
exceeds its budget.

| | |
|:--|:--|
| Statistic | **p50** (see below — deliberately not p99) |
| Reference p50 | 200–234 ms across runs on a quiet reference box; ~516 ms observed on a loaded one |
| Budget | **1 500 ms** (~2.9x the worst observation, ~6.4–7.5x the quiet ones) |
| Override | `HARVEST_CLAIM_BUDGET_MS` |

### Why the gate asserts p50, not p99

The headline scenario runs 8 concurrent claimers against a 4-core box on
purpose — contention is the point of the scenario. But that makes the database
oversubscribed, and an oversubscribed tail measures the run queue rather than
the claim path. Measured across repeated runs on the reference machine:

| statistic | quiet box | loaded box | spread |
|:--|--:|--:|--:|
| p50 | ~200–234 ms | ~516 ms | **~2.3x** |
| p99 | ~300 ms | ~4 665 ms | **~15x** |

A p99 gate at this budget failed **2 runs out of 6** during review, on the same
hardware class and Postgres version that produced the published numbers. It was
not detecting regressions; it was detecting the scheduler. A separate run on a
moderately busy box measured p50 283 ms against a p99 of 1 652 ms at this same
scenario — the p99 alone would have failed a 1 500 ms gate while nothing about
the claim path had changed.

p99 is still measured, still published above, and printed in the gate's failure
message so a genuine tail regression is visible to whoever reads it. It is
simply not the assertion.

**The budget is a cliff detector, not a drift detector.** It will not catch a
predicate that makes claims 50% slower — the reference p50 itself moves 2.3x
with machine load, so no threshold on this hardware could. It catches the kind
of change that adds another per-row subplan to a scan already walking the whole
pending backlog. Being precise about how big that cliff has to be, since the
quiet-box reference spans 200–234 ms and the budget is therefore 6.4–7.5x it:

| regression scale | example | quiet box (200 ms) | quiet box (234 ms) | loaded box (516 ms) |
|:--|:--|:--|:--|:--|
| ~7.4x | a second `concurrency_key`-class subplan | 1 488 ms — **misses** | 1 741 ms — trips | 3 839 ms — trips |
| ~14.6x | doubling the rows every claim walks | 2 914 ms — trips | 3 409 ms — trips | 7 519 ms — trips |

So a depth-class regression trips everywhere, while a single-subplan-class one
trips on a loaded box and at the slow end of the quiet range but can slip
through on the fastest quiet runs. That is inherent rather than a tuning miss:
the reference moves 2.3x with load, so no single threshold separates 7.4x from
noise on this hardware. It also matters less than it looks, because the gate
runs in CI, and CI is the loaded case — 2.9x headroom, where even the smaller
cliff clears the budget by more than 2x. For drift below either cliff, run the
benchmark and compare against the per-gate table above, which runs below
saturation precisely so it can resolve smaller differences.

**The budget was derived on the reference machine, not on a CI runner.** CI
hardware is slower and shared, so the Linux CI runs are the real calibration.
The first such run (2026-08-10, `ubuntu-latest`, Docker-backed Postgres 16)
passed all seven gate tests in 108 s, with the headline scenario itself taking
about 78 s of that — comfortably inside both the 1 500 ms p50 budget and the
240 s per-scenario ceiling. So the budget derived here holds on CI hardware as
published; it has not been widened for it. If a later run proves flaky rather
than catching anything, the fix is to re-derive the number from CI observations
— not to widen it by guesswork and not to delete it; `HARVEST_CLAIM_BUDGET_MS`
exists for the one-off, and the failure message always carries the full stat
line.

Note that the measured stat line is only *printed* when the gate fails: the
manifest runner does not pass `--nocapture`, and Rust's test harness shows
captured output for failing tests only. That is the right default for a gate —
silence means "within budget" — but it does mean CI logs carry no trend data
between failures. Run the benchmark for that.

The gate also asserts five soundness properties, each of which fails loudly
rather than reporting a meaningless percentile:

* **at least 100 samples were collected** — a severe regression could otherwise
  leave the gate defending a two-sample percentile;
* **the run was not truncated** by the wall-clock ceiling — a partial run's
  percentiles describe a shorter, differently-warmed window than the published
  ones, so the gate defends a complete scenario or says so;
* **the scenario finished inside its wall-clock ceiling** (+30 s slack for task
  join). The truncation flag above is only set where the harness *checks* the
  deadline, so it cannot catch an `await` that never returns to a check; this
  assertion measures the clock directly and so does;
* **at least 90% of measured operations actually claimed a task.** Note what this
  does and does not prove: it rules out "the harness measured an empty queue",
  which is the failure mode that would silently make the gate pass. It does not
  prove each gate scenario put its *predicate* on the execution path — a
  scenario that stopped setting its trigger column would still claim 100% of its
  operations, just via the cheap `IS NULL` leg. That is what the separate
  seed-census test asserts;
* **the backlog was not drained below 80%.** `claim_task` is destructive, so a
  run that emptied the queue would be timing claims against an increasingly
  empty table. The bound is enforced two ways: the planned operation count is
  capped at `backlog / 5`, and the per-claimer split is exact, so the claimers
  between them can never execute more than that plan.

Locally, with neither Docker nor `HARVEST_TEST_DATABASE_URL`, the gate skips with
a notice. Under `CI` it **fails** instead: a performance gate that silently
no-ops when its dependency is missing is not a gate.

## Methodology

### Why not criterion

`claim_task` is **destructive** — it moves a row `PENDING → RUNNING`. Criterion
runs the measured closure thousands of times, which would drain the seeded
backlog and end up timing "claim against an empty queue", the opposite of the
thing under test. The harness instead seeds a backlog of N, performs a bounded
number of claims (never more than N/5), and reports true percentiles over the
collected per-claim latencies.

### What is actually timed

One `queue::claim_task(...)` call, wall clock, from the client. That call is **a
whole transaction**, not the single statement the `EXPLAIN` below shows — and not
a single round trip either. It issues, in order:

1. `BEGIN ISOLATION LEVEL READ COMMITTED`. The level is pinned on the `BEGIN`
   itself rather than inherited, so step 4 always gets a fresh snapshot.
2. **The claim CTE.** The rate-limit debit and the per-key concurrency
   advisory-lock re-check are branches *within* this statement, not extra ones —
   this is the statement the `EXPLAIN` below plans. With cross-region DR fencing
   enabled (issue #954, off by default) it carries one additional
   `MATERIALIZED` CTE probing `harvest_shard_generation` — still the same single
   statement and the same round-trip count, and the published figures below were
   measured with fencing off, which is the default and the configuration the
   plan applies to.
3. *(hit only)* `queue_pause::try_lock_queue_for_claim` — a
   `pg_try_advisory_xact_lock` on the queue. If it loses the race against a
   concurrent pause or resume, `queue_pause::release_claim` hands the row back
   and the call returns "no task": same round-trip count, no claim.
4. *(hit only)* `queue_pause::release_claim_if_queue_paused` — the authoritative
   queue-pause re-check. It is a *separate statement* precisely so it takes a
   snapshot the claim could not have; folding it into the CTE would defeat it.
5. `COMMIT`.

So a published number is **five** client↔server round trips when the claim lands
on a row and **three** when the queue is empty — plus transaction overhead — and
the `EXPLAIN` plan explains the *dominant* statement rather than the whole
measured operation. The seeded scenarios claim at most a fifth of the backlog
they seed, so their samples are overwhelmingly hits.

That distinction is the first thing to reason about when moving this workload to
a **remote** database: the round-trip count, not the query plan, is what network
latency multiplies. Five round trips at 1 ms of network RTT is 5 ms of floor per
claim that no amount of index tuning removes. Every number on this page was
measured against a loopback server, so that floor is ~0 here and the plan
dominates; that ordering inverts across a network.

Measuring the whole call is the right thing to do — it is what a worker actually
waits for — but it means these numbers are not directly comparable to a bare
`EXPLAIN ANALYZE` of the claim query.

### Measurement hygiene

* **Percentiles are nearest-rank**, so a reported p99 is an actually-observed
  claim someone waited for, not an interpolation between two.
* **A tenth of each claimer's observations is discarded as warmup**, applied
  after collection rather than by planned index. This matters more than it
  sounds: an earlier revision discarded a flat 3 samples per claimer, and the
  headline p99 read ~2 900 ms instead of ~300 ms — the metric had become a
  cold-start measurement. Applying the fraction post-hoc also means a scenario
  cut short by its wall-clock budget still reports the samples it took instead
  of discarding all of them and printing a confident-looking `0.00 ms`.
* **The wall-clock ceiling bounds each claim, not just the loop.** `claim_task`
  is an unbounded `await`, so checking the deadline only between calls would let
  a single stalled claim — exactly the regression or database stall the ceiling
  exists to catch — run for minutes past the advertised cap. Each call is
  wrapped in a timeout derived from the remaining budget; expiry marks the run
  truncated, which the gate treats as "measurement unsound" rather than
  publishing a percentile from a partial run.
* **One deadline for the scenario, established before any claimer starts.**
  Deriving it inside each claimer — after its pool checkout — would restart the
  clock behind an unbounded `await`: a stalled or exhausted pool parks every
  claimer with no deadline yet in existence. The checkout is bounded by the same
  deadline as the claims, and the gate additionally asserts the scenario's
  measured wall clock against that ceiling, because `truncated` is only set
  where the harness *checks* the deadline and so cannot catch an `await` that
  never returns to a check.
* **`ANALYZE` runs after every seed.** Without it the planner works from stale
  statistics on a freshly bulk-loaded table and picks plans that are neither
  representative nor stable.
* **Seeding is set-based** (`INSERT ... SELECT FROM generate_series`), so a
  100k-row backlog costs one round trip.
* **The pool is always sized above the claimer count**, so a measured claim never
  includes pool-checkout queueing.
* **Every scenario truncates first**, so scenarios cannot contaminate each other.
* **Zero samples renders as `n/a`, never `0.00`.** A scenario that measured
  nothing must not publish a number that looks instantaneous. A row cut short by
  the wall-clock budget is marked `⚠`, and a row with fewer than 100 samples —
  below the floor the CI gate itself will accept — is marked `‡` and should be
  read as directional only.
* **The enqueue table gets the same warmup trim** as the claim tables, so its
  `n` column is below its `rows` column. `rows/s` deliberately divides *all*
  rows by the *whole* wall clock, warmup included, so it is a conservative floor
  on sustained throughput rather than a peak.
* **Throughput and latency use different windows, on purpose.** `claims/s` and
  `rows/s` count *every* successful operation over the *whole* wall clock,
  warmup included, because the clock starts at the first warmup call — a
  fraction whose numerator and denominator cover different spans is not a rate.
  The percentile columns exclude warmup, because there the first calls on a
  fresh connection are exactly the unrepresentative ones. The `n` column belongs
  to the latency window, so it is below the operation count the throughput
  column divides.
* **Every worker starts together, and the clock starts with them.** Each claimer
  (or writer) checks out its pooled connection and then waits at a start
  barrier, so a row labelled "8 concurrent claimers" measures eight of them
  contending, not a ramp-up in which the first is already sampling while the
  last is still connecting. The throughput denominator is **one** span shared by
  every worker — earliest resume after the barrier through the last completion —
  so pool construction is not counted as measured work and no worker's
  release-to-resume delay escapes the denominator. Timing each worker from its
  own resume and taking the widest of those would drop exactly that delay while
  keeping all of its claims in the numerator, reporting a rate the run never
  achieved; with more workers than runtime threads most of them are not polled
  at release, so the effect is not marginal. This is a *different* clock from
  the per-scenario ceiling below, which deliberately starts *before* checkout —
  `pool.get()` is an unbounded await, so a ceiling that started after it would
  not bound it.

### Profile does not matter

The gate runs in the `test` (debug) profile and the benchmark in `bench`
(release). Measured back to back at the headline scenario, they agree: p50
228–256 ms in debug against 242–256 ms in release. The work is server-side, so
the client build profile is not a meaningful variable. Numbers from the gate and
from the benchmark are directly comparable.

### Known limitations

* **Single-shard, single-host.** Multi-shard distributed load is explicitly out
  of scope for issue #786. Per-shard numbers are what this page reports; a
  sharded deployment multiplies claim capacity by shard count, which is the
  entire point of `docs/sharding.md`.
* **Tail columns in the attribution table pick up background stalls** (autovacuum,
  the OS scheduler) and are reported for completeness only.
* **The seeded backlog has a degenerate sort-key distribution.** Every seeded row
  gets `priority = 0` and the same `scheduled_at`. Since the page's central
  finding is that Postgres sorts every eligible row by
  `(CASE …, priority DESC, scheduled_at)`, sorting N *identical* keys is not
  what a production backlog looks like. A real queue with mixed priorities and
  spread arrival times may sort differently — probably not cheaper, but this is
  measured on the degenerate case and should be read that way.
* **Half the claim-path predicates are varied; the other half are not measured
  at all.** The attribution table covers five: build-id routing (#171), per-key
  concurrency (#247), the rate-limit gate (#332/#699), the circuit-breaker
  tracked set (#369) and the PAUSED skip (#383). Six more are present in the
  query on every claim but are never given anything to match in *this*
  table, so their subplans run against empty or null input here and this
  table reports nothing about their cost. Ranked by how much that omission
  is likely to matter:
  * **Capability labels (#382)** — measured directly:
    [`docs/performance-capability-labels.md`](performance-capability-labels.md) seeds `required_capabilities`
    (rather than leaving it null) and finds a real, +24–36% buffer cost on the
    claim query across the same backlog-depth sweep used everywhere else on
    this page, corroborated three independent ways (`EXPLAIN` buffers,
    `pg_relation_size` row-width growth, and an aggregate `pg_stat_statements`
    drain). The mechanism is heap-page growth from the wider stored JSONB
    payload, not a plan inefficiency — no query-shape fix applies; see that
    page for the full measurement and why.
  * **Queue pauses (#619)** — the attribution-table sweep above still only ever
    `TRUNCATE`s `harvest_queue_pauses`, so it still says nothing about this
    predicate's cost on its own. A dedicated harness variant that actively
    pauses a queue closed that specific gap and, as a direct result, replaced
    the correlated anti-join with a one-time prefilter — see
    [the queue-pause anti-join fix](#the-queue-pause-anti-join-fix). That fix
    was measured against exactly one active pause. Issue #1215 swept the
    array wider — up to 199 paused queues — and confirms the fix holds at
    that scale for a typical worker: see
    [the pause-array-size sweep](#the-pause-array-size-sweep-issue-1215) for
    why, and for the one atypical worker shape where it does not.
  * **Activity pauses (#807)** — not previously in this list at all. Issue
    #1215 swept `harvest_activity_pauses`' array size, crossed against the
    full `BACKLOG_SWEEP`, and found the claim sort spills to disk once the
    array holds around 20 rows at the 10,000-row headline depth — far below
    the [few-hundred-thousand-row depth issue #1177's own locked-scenario
    reproduction needed](#any-residual-predicate-defeats-sort-elision-issue-1177)
    to trigger the same spill against an empty pause table. That threshold
    is depth-dependent, not fixed: higher at 1,000 rows, and already crossed
    at 100,000 rows with zero paused activities. Unlike queue pauses,
    `paused_activities` reads the whole table on every claim with no bind to
    keep the array small, so this exposure needs no unusual worker shape —
    pausing 20 or more activity types during a multi-service incident is
    realistic on its own, at the headline depth. See
    [the pause-array-size sweep](#the-pause-array-size-sweep-issue-1215) for
    the full measurement. No query-shape fix is proposed here.
  * **`schedule_to_close` (#378)** — measured directly:
    [`docs/performance-schedule-to-close.md`](performance-schedule-to-close.md) seeds `schedule_to_close_at`
    (rather than leaving it null) and **confirms this page's own suspicion on
    magnitude, but not on mechanism**: a small, real shared-buffer-hit cost
    (+3.6% to +7.5% across the two backlog depths where both labels land on
    the same plan — the 100,000-row depth's committed run has the two
    labels land on *different* plans for the candidate scan, so it does not
    get a clean percentage; see that page's "100,000-row plan choice"
    section), corroborated by two
    standalone MVCC-bloat scripts, one bulk and one per-row — heap +5.2%
    both, the partial index itself +90% (30→57 pages, a small base that
    reads as a large percentage for the same reason the `dirtied`/`written`
    EXPLAIN counters do below) —
    nowhere near the 20% impact floor, measured where a percentage is
    stable: shared-buffer-hit totals, and `harvest_task_queue`'s total
    on-disk footprint growth (+12.3%: heap plus every index plus TOAST,
    measured directly with `pg_total_relation_size` rather than summed
    from a chosen subset of relations — `no-schedule-to-close` grows 324
    pages total, `schedule-to-close` grows 364), not against the
    `dirtied`/`written` EXPLAIN counters' own small base (4→5, 2→3) or the
    index's own page count on its own, which that page reports as absolute
    counts instead of floor-compared percentages — Codex review flagged
    that a percentage on a base that small (+25%/+50% dirtied/written;
    +90% for the index alone) is unstable and would not track the real
    per-claim cost. Codex review
    caught that the predicate text alone (a plain inline column test) is not
    the whole story: `harvest_task_queue` carries a partial index on this
    column for the timeout scanner, and the claim `UPDATE` writes a new
    entry to it for every `schedule-to-close` row — a fixed, depth-independent
    +1 dirtied/+1 written page at every backlog depth tested, additive with a
    separate row-width effect on the candidate scan that *does* scale with
    depth. Review also caught that the harness's first seeded deadline gave
    every row the byte-identical value, letting B-tree deduplication
    understate the index's real growth by roughly 3x — fixed by seeding a
    distinct, per-row deadline instead. See that page's "Plan" and
    "Write-side cost" sections for the buffer- and storage-level evidence.
    One thing did **not** reproduce cleanly across this pass's several
    capture runs: the real 10,001-call `pg_stat_statements` drain's
    aggregate delta varied run to run, but only the most recent run's
    artifacts are ever committed -- the repro script overwrites the same
    canonical filenames each time -- so that page states only the one
    auditable, committed number for driving the real `claim_task()`
    function (**+4.2%**, combining `claim_task_query()`'s own SQL with the
    two post-claim queue-/activity-pause rechecks it also issues on every
    successful claim — `claim_task_query()` alone is +1.9%, reported
    separately since it's what the `EXPLAIN`-based evidence above is built
    on), without asserting a range, a frequency, or a direction (e.g.
    "always positive") for runs whose evidence no longer exists in the
    repository to audit. An earlier revision of this page's real-drain
    figures and buffer deltas used a confounded seeding methodology
    instead: the two labels had been seeded with independently-random
    `id`/`activity_id` values, and since every claim's non-HOT `UPDATE`
    touches every applicable index on the table, not just the one this
    predicate adds, some of what had looked like a `schedule_to_close_at`
    effect on the main query may have been that confound instead — see
    that page's "Workload" section for the fix. That earlier revision's
    own artifacts are no longer committed (the repro script overwrites
    the same canonical filenames every run), so this page does not cite
    its pre-fix percentages or draw a magnitude conclusion from the
    comparison. The committed run now shows the two labels landing on *different* plans at
    the 100,000-row depth, with the expensive one on `no-schedule-to-close`
    this time (an earlier, since-superseded committed run had neither
    label on the expensive plan, so this is the only committed data point
    for which label it lands on). That page's "100,000-row plan choice"
    section is explicit that this does **not** show the instability is
    unrelated to `schedule_to_close_at` — populating that column changes
    the planner's actual row-count estimate for the shared candidate scan
    (68,360 vs. 99,990 in this run's own committed plans, both against a
    real 100,000 rows), so a plan flip either way is equally consistent
    with that predicate's effect on planner inputs and with unrelated
    `ANALYZE`-sample noise; the page does not have the evidence to tell
    those apart. That same section also explains why it asserts
    no frequency, ratio, or before/after count for this, including why an
    earlier revision's "N of M runs" framing, and later a spelled-out
    sample-of-two-against-two restating the same statistic in prose, both
    had to be walked back once those runs' artifacts were no longer
    available to audit. **This is a different question from issue #1177's
    finding** (see
    [any residual predicate defeats sort-elision](#any-residual-predicate-defeats-sort-elision-issue-1177))
    that this same column independently defeats sort-elision/`LIMIT`
    pushdown regardless of its value — a plan-*eligibility* effect. This
    page's capture measures the column's marginal buffer/storage cost
    against `claim_task_query()` exactly as it stands today, where the
    `CASE` key and the other always-present residual predicates already
    force the collapsed plan shape in both the seeded and unseeded state
    (every committed plan needs the same external-merge `Sort` regardless
    of which scan feeds it, including the 100,000-row depth's committed
    run, where the two labels land on different scans but the identical
    sort either way) — so the two findings don't conflict: #1177 explains why
    dropping this predicate alone would not recover the cheap plan, while
    this page measures what it costs to keep it, holding the already-collapsed
    plan shape fixed.
  * **Worker sessions (#606)** — measured directly, on a genuinely different
    axis from issue #1177 just below: `docs/performance-worker-sessions.md`
    seeds `session_id` and `sticky_worker_id`/`sticky_until`/`sticky_timeout`
    via a per-row `INSERT`-then-`UPDATE`-then-`COMMIT` lifecycle matching
    `queue::enqueue()`'s real per-task write (as issue #606's hard-pin design
    always writes them) and finds a real, moderate-to-large buffer cost on the
    claim query — +40.9% on a single first claim against a cache-warm table
    at the 10,000-row headline depth, corroborated by a real 10,001-call
    production-shaped drain at +29.0% (same order of magnitude, unlike an
    earlier bulk-transaction capture this page's own history superseded).
    Mechanism: row-width growth compounded by MVCC bloat from the second
    write, not a plan inefficiency — no query-shape fix applies; see that
    page for the full measurement, including why it does not isolate worker
    sessions from ordinary sticky routing's own cost (measured separately,
    immediately below), and an open question about seeding transaction
    granularity for multi-activity decision fan-outs that a review round
    raised but this pass did not chase down.
    This is a buffer-cost measurement, not a plan-eligibility one — it does
    not supersede or overlap with issue #1177's finding that worker
    sessions' predicate, like `schedule_to_close`'s and sticky routing's,
    independently defeats sort-elision (see immediately below); the two are
    answers to different questions about the same predicate.
  * **Sticky routing (#235)** — measured directly:
    [`docs/performance-sticky-routing.md`](performance-sticky-routing.md)
    seeds `sticky_worker_id`/`sticky_until`/`sticky_timeout` (session_id left
    `NULL`, isolating this predicate from worker sessions' own) via the same
    per-row `INSERT`-then-`UPDATE`-then-`COMMIT` lifecycle
    `queue::enqueue()`'s real write uses for an ordinary sticky pin, reusing
    the `no-sticky` control's exact `id`/`activity_id` values in their
    original physical insertion order (a Codex review finding on this page's
    own PR caught an earlier revision seeding each label's B-trees with
    independently-random keys instead — see that page's Harness correction
    section), and finds a real, moderate buffer cost that **grows
    monotonically across every published depth** — +18.9% at 1,000 rows,
    +32.9% at the 10,000-row headline depth, +36.2% at 100,000 rows —
    corroborated by a real 10,001-call production-shaped drain at +18.3%.
    Mechanism: the same row-width/MVCC growth worker sessions' page
    documents, smaller in magnitude since only one column pair is set
    rather than two — no query-shape fix applies. Both labels choose the
    identical `Seq Scan` plan shape at every depth including 100,000 rows;
    an earlier revision of this measurement reported a plan-shape crossover
    there, which review traced to the same seeding confound rather than to
    `sticky_worker_id` itself — see that page for the corrected capture.
    What issue #1177 adds is a
    different kind of evidence, not a cost figure: in isolation, sticky
    routing's predicate — together with `schedule_to_close`'s and worker
    sessions', both also measured — independently defeats sort-elision and
    `LIMIT` pushdown regardless of the value it is tested against,
    reproducing the same collapsed plan shape this page's own headline
    finding describes. See
    [any residual predicate defeats sort-elision](#any-residual-predicate-defeats-sort-elision-issue-1177).
    In the full production query the `CASE` key and the always-present
    predicates already force that same collapse regardless of any one of
    these three, so this page's own cost measurement above is what fills
    the gap that plan-eligibility finding cannot. "cheap inline column
    tests" was this page's own now-retracted reading of their
    *plan-eligibility* effect, not a corrected *cost* measurement —
    replacing one unsupported cost claim with another would have been no
    improvement, which is why all three now carry a real measurement
    instead.
* **Queue count is a parameter, but it is not swept.** `Scenario.queues`
  parameterizes how many distinct queues the backlog spreads across, and every
  published row holds it at 4. Backlog depth and claimer count *are* varied.
  Spreading the same backlog over more queues does not obviously help — the
  claim filter is `queue_name = ANY($1)`, so a worker bound to all four still
  scans all four — but that is an expectation, not a measurement.
* **The scheduler tick and the timeout scanner are not benchmarked here.** They
  are separate hot paths and separate work.

## See also

* `benches/claim_bench.rs` — the report generator.
* `tests/integration/claim_bench_support.rs` — the harness, shared verbatim
  between the benchmark and the gate so published and gated numbers can never be
  produced by different code.
* `tests/integration/claim_budget_tests.rs` — the gate.
* `docs/sharding.md` — what to do when the backlog table says you have outgrown
  one shard.
* `docs/perf-artifacts/queue-pause-claim-anti-join/` — committed before/after
  `EXPLAIN`/`pg_stat_statements` evidence for
  [the queue-pause anti-join fix](#the-queue-pause-anti-join-fix).
* `autumn-harvest/scripts/queue_pause_claim_perf_repro.sh` — regenerates that
  evidence from a clean checkout.
* `docs/perf-artifacts/pause-array-size/` — committed `EXPLAIN` evidence for
  [the pause-array-size sweep](#the-pause-array-size-sweep-issue-1215).
* `autumn-harvest/scripts/pause_array_size_claim_perf_repro.sh` — regenerates
  that evidence from a clean checkout.
* `docs/perf-artifacts/concurrency-key-claim-predicate/` — committed
  before/after `EXPLAIN`/`pg_stat_statements` evidence for
  [the concurrency-key gate fix](#the-concurrency-key-gate-fix).
* `autumn-harvest/scripts/concurrency_key_claim_perf_repro.sh` — regenerates
  that evidence from a clean checkout.
* [`docs/performance-capability-labels.md`](performance-capability-labels.md) — the capability-labels claim
  predicate (#382) measurement referenced above.
* `docs/perf-artifacts/capability-labels-claim-predicate/` — committed
  `EXPLAIN`/`pg_stat_statements` evidence for that measurement.
* `autumn-harvest/scripts/capability_labels_claim_perf_repro.sh` — regenerates
  that evidence from a clean checkout.
* [`docs/performance-schedule-to-close.md`](performance-schedule-to-close.md) — the `schedule_to_close_at` claim
  predicate (#378) measurement referenced above.
* `docs/perf-artifacts/schedule-to-close-claim-predicate/` — committed
  `EXPLAIN`/`pg_stat_statements`/heap-growth evidence for that measurement.
* `autumn-harvest/scripts/schedule_to_close_claim_perf_repro.sh` — regenerates
  that evidence from a clean checkout.
* [`docs/performance-worker-sessions.md`](performance-worker-sessions.md) — the worker-sessions claim predicate
  (#606) measurement referenced above.
* `docs/perf-artifacts/worker-session-claim-predicate/` — committed
  `EXPLAIN`/`pg_stat_statements` evidence for that measurement.
* `autumn-harvest/scripts/worker_session_claim_perf_repro.sh` — regenerates
  that evidence from a clean checkout.
* [`docs/performance-history-ceiling.md`](performance-history-ceiling.md) — a separate scanner, not part of
  `claim_task_query()`: the workflow-history-ceiling check
  (`timeout::enforce_workflow_history_ceiling`, issue #493) fixed a
  correlated `harvest_events` event-count subquery that was evaluated twice
  per RUNNING execution on every timeout-scanner tick.
* Issue #1177 — reproduction and full `EXPLAIN` captures for
  [any residual predicate defeats sort-elision](#any-residual-predicate-defeats-sort-elision-issue-1177).

### Other profiling notes

Instruction/allocation-count profiling passes over other hot paths, each a
standalone note rather than part of the claim-path attribution table above:

* [`docs/performance-replay.md`](performance-replay.md) — `WorkflowReplayer`'s
  in-memory replay path against issue #135's CPU-path budget; shipped fix.
* [`docs/performance-verify.md`](performance-verify.md) —
  `ReplayVerifier::verify_dir`'s opaque-payload guard fast-path; shipped under
  maintainer override after falling short of the autonomous gate.
* [`docs/performance-schema-validation-lazy-path.md`](performance-schema-validation-lazy-path.md)
  — lazy JSON-Pointer path construction in schema validation (issue #373).
* [`docs/performance-det-check.md`](performance-det-check.md) — fusing a
  redundant per-line comment scan in `harvest det-check` (issue #778).
* [`docs/performance-dag-graph.md`](performance-dag-graph.md) — hoisting a
  per-node rebuild out of `GET /dag-run-graph` (issue #690).
* [`docs/performance-dlq-aggregate.md`](performance-dlq-aggregate.md) — DLQ
  aggregate grouping (issue #385/#613); a measured fix that was reverted after
  review found a regressing input shape — a negative result.
* [`docs/performance-dlq-merge.md`](performance-dlq-merge.md) — the DLQ
  cross-shard merge stage that runs after the grouping above; redundant key
  clones removed.
* [`docs/performance-stall-diagnosis.md`](performance-stall-diagnosis.md) — an
  allocation-free ranking pass over `GET /api/harvest/workflows/{id}/diagnose`
  (issue #809).
* [`docs/performance-diagnose-latency.md`](performance-diagnose-latency.md) —
  end-to-end wall-clock latency of that same endpoint against a real
  Postgres, confirming issue #809's published `p95 < 500 ms` claim with a
  measured number across fan-out width, fleet size, and the replay path
  (issue #1194).
* [`docs/performance-workflow-children-traversal.md`](performance-workflow-children-traversal.md)
  — batching the N+1 in `GET /workflows/{id}/children?depth=N` (issue #786-adjacent).
* [`docs/performance-schedule-overdue-aux.md`](performance-schedule-overdue-aux.md)
  — the same N+1 shape in `GET /admin/schedules`'s overdue-aux computation
  (issue #696).
* [`docs/performance-schedule-overdue-pass.md`](performance-schedule-overdue-pass.md)
  — the aux-lookup fix's own named follow-up: the identical N+1 shape in
  `scheduler::overdue_schedule_pass`, the scheduler tick's periodic
  overdue-gauge sampler (issue #696).
* [`docs/performance-usage-report-activity-lookback.md`](performance-usage-report-activity-lookback.md)
  — indexing the activity-attempt lookback LATERAL join in `GET /admin/usage`
  (issue #596), the one CTE the 2026-07 usage-report-indexes migration missed.
* [`docs/performance-external-outbox-scan.md`](performance-external-outbox-scan.md)
  — indexing both sides of the three external signal/cancel/await outbox claim
  queries, and pinning their plan against a stale row estimate (issue #1486).
* [`docs/performance-quota-history-bytes.md`](performance-quota-history-bytes.md)
  — measuring the `history_bytes` admission check's cost claim (issue #946
  AC7); partially inaccurate claim, no fix identified.
* [`docs/performance-codec-rotation-reencrypt.md`](performance-codec-rotation-reencrypt.md)
  — skipping a JSON round-trip in the codec-key-rotation re-encryption sweep
  (issue #948).
* [`docs/performance-sqlite-runtime-drive.md`](performance-sqlite-runtime-drive.md)
  — the first profiling harness for `autumn-harvest-sqlite`; findings only, no
  local fix cleared the floor.
* [`docs/performance-redis-claim-roundtrip.md`](performance-redis-claim-roundtrip.md)
  — a duplicate `ensure_group` round trip on every `RedisTaskQueue::claim`
  poll, measured in socket-syscall counts (PR #1387).
* [`docs/performance-schedule-bulk-audit.md`](performance-schedule-bulk-audit.md)
  — the per-row audit-insert N+1 in the Vantage schedules bulk-pause/resume
  actions (issue #951), batched into one chunked insert call per shard
  (multiple statements past 4,999 matched rows).
* [`docs/performance-dlq-bulk-discard.md`](performance-dlq-bulk-discard.md) —
  the per-row `DELETE` N+1 in `POST /dead-letters/discard` (issue #1421),
  batched into one `DELETE ... WHERE id = ANY($1)` call.
* [`docs/performance-activity-fanout-enqueue.md`](performance-activity-fanout-enqueue.md)
  — the per-activity `INSERT` N+1 in a workflow decision's
  `ScheduleActivity` fan-out (`persist_scheduled_activities` /
  `persist_mixed_suspension_batch`), batched into one multi-row `INSERT`
  call via `queue::enqueue_batch` (`enqueue_calls` n → 1 at every swept
  size).
* [`docs/performance-mutex-lease-reclaim.md`](performance-mutex-lease-reclaim.md)
  — the per-key three-statement N+1 in `mutex::reclaim_expired_leases_and_wake`,
  the durable-mutex lease scanner's crash-recovery sweep, collapsed into
  one statement per key (`calls` -66.7% at every swept size; buffers flat
  by design, so the fix is measured in DB-socket syscalls instead: `sendto`
  -44.5%, `recvfrom` -40.9%).
* [`docs/performance-metrics-sampler-guard.md`](performance-metrics-sampler-guard.md)
  — four worker samplers issuing SQL with no `metrics.is_enabled()` guard
  (issue #1428), eliminated entirely rather than reduced (pool-touch count
  and corroborating `strace` `connect` calls both N → 0).
* [`docs/performance-completion-trigger-outbox-queue.md`](performance-completion-trigger-outbox-queue.md)
  — the per-row `harvest_schedules` lookup in
  `completion_trigger::enforce_completion_triggers_outbox`'s cross-shard
  relay scan, batched into one `workflow_name = ANY($1)` call via
  `resolve_target_queues_batch` (`lookup_calls` n → 1 at every swept size).
