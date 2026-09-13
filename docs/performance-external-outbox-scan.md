# External signal/cancel/await outbox scans indexed and pinned (issue #1486)

`timeout::enforce_timeouts_once` runs three sibling outbox scanners on every
worker's periodic tick. Neither side of their claim query was indexed, so each
claim read the whole event table. Issue #1486 profiled that gap, measured the
obvious index fix as a **271% regression**, and filed a findings issue rather
than a PR. This page measures the fix that works: the indexes and a query
rewrite, together, because neither half stands alone.

## 🎯 Workload

`enforce_external_signals_outbox` (`timeout.rs`), and its `..._cancels_...` and
`..._awaits_...` siblings. Each drains its own outbox of pending cross-workflow
`ctx.signal()` / `ctx.cancel()` / `ctx.await_external()` requests: claim one
candidate row under `LIMIT 1 ... FOR UPDATE OF e SKIP LOCKED`, deliver it,
append a terminal event, loop until the outbox is empty.

The three queries are byte-for-byte the same shape. They differ in the request
type they claim, the two events that resolve it, and the payload key that
correlates them. The workload profiled here is one full drain of one outbox --
51 claims and 50 delivery markers -- which is what a worker does after any
period where requests accumulated.

The fixture is the one issue #1486 specified: 5,000 RUNNING executions with 200
events each, a 2,000-execution terminal tail with 10 events each, and 50
unresolved `ExternalSignalRequested` rows spread across the RUNNING population.
1,020,050 event rows in total -- a long-running-workflow population, which is
what this scanner searches.

## 📈 Profile

`EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)` of one cold claim, before:

```
->  Seq Scan on public.harvest_events e  (actual rows=1 loops=1)
      Filter: ((e.id <> ALL ('{}'::bigint[])) AND (e.event_type = 'ExternalSignalRequested'::text) AND ...)
      Rows Removed by Filter: 1020000
      Buffers: shared hit=7307 read=13730
```

21,037 buffers to find the one row `LIMIT 1` needed, out of the claim's 21,246
total. The cost scales with the size of the event table, not with the backlog
being searched.

`harvest_events` already carries partial indexes for exactly this problem on
other rare event types -- `idx_harvest_events_activity_type_ts` and
`idx_harvest_events_reset_terminated`, both from
`20260702000000_harvest_usage_report_indexes`. Nothing analogous existed for
these three.

**The outer scan is not the larger cost.** Index it and almost nothing
happens: a full drain falls from 299,018 buffers to 277,432, which is 7.2%.
The cold claim above collapses to 211 buffers, and the drain barely moves.
That is issue #1486's negative result, reproduced from the other side.

The remaining 92.8% is the resolution check. It is correlated per execution
and was unindexed, so each probe re-read every event of the owning execution.
A drain pays that once per already-resolved candidate a later claim steps
over, so the cost grows with the square of the backlog.

## 🧭 Plan: why the obvious fix regressed

Issue #1486 measured the partial index on the outer predicate by itself and
reported a full-drain regression of 271%, plus an unreliable oscillation when
the statistics target was raised. That result is real, and it has a mechanism:
the outer index changes the row estimate the planner uses to cost the paired
`NOT EXISTS`, and that estimate sits at this query's own
correlated-versus-materialised crossover. Past the crossover the planner stops
correlating the anti-join and reads the whole table once instead, which a drain
loop then pays on every iteration.

The issue's recommendation was to pin the anti-join structurally first, then
re-measure the index. That is what this change does.

## 🔧 Change

**Migration `20260911213344_harvest_external_outbox_scan_indexes`** -- four
partial indexes on `harvest_events`:

```sql
CREATE INDEX idx_harvest_events_external_outbox_pending
    ON harvest_events (event_type, timestamp, id)
    WHERE event_type IN ('ExternalSignalRequested',
                         'ExternalCancelRequested',
                         'ExternalAwaitRequested');

CREATE INDEX idx_harvest_events_external_signal_resolved
    ON harvest_events (workflow_exec_id, (event_data->'data'->>'signal_id'))
    WHERE event_type IN ('ExternalSignalDelivered', 'ExternalSignalFailed');
-- ... and the cancel and await equivalents, keyed on `cancel_id` / `await_id`.
```

One index serves all three outer scans: each scanner filters a single
`event_type`, which the leading column answers as a prefix. Three more key the
resolution check exactly as that check is written.

**Query rewrite (`timeout.rs`)** -- the three claim queries are now generated
from one `external_outbox_claim_query!` template, so the shape cannot drift
between siblings. Both joins are pinned to their correlated form:

```sql
SELECT e.* FROM harvest_events e
JOIN LATERAL (
    SELECT 1 AS running_exec FROM harvest_workflow_executions x
    WHERE x.id = e.workflow_exec_id AND x.state = 'RUNNING'
      AND x.shard_id = ANY($1)
    LIMIT 1
) running ON TRUE
WHERE e.event_type = 'ExternalSignalRequested'
  AND (e.event_data->'data'->>'signal_id') IS NOT NULL
  AND NOT (e.id = ANY($2))
  AND NOT EXISTS (
      SELECT 1 FROM harvest_events res
      WHERE res.workflow_exec_id = e.workflow_exec_id
        AND res.event_type IN ('ExternalSignalDelivered', 'ExternalSignalFailed')
        AND res.event_data->'data'->>'signal_id' = e.event_data->'data'->>'signal_id'
  )
ORDER BY e.timestamp, e.id
LIMIT 1
FOR UPDATE OF e SKIP LOCKED
```

A `LIMIT 1` inside a `LATERAL` cannot be pulled up into the outer query, so the
executions probe is structurally correlated rather than correlated by a plan
the planner happens to prefer today. `harvest_workflow_executions.id` is the
primary key, so the subquery returns at most one row either way, and the pin
changes no result.

**The pin is free, not a win, and the harness says so.** Its `stale: as
shipped, executions join plain` scenario is the identical query with this one
join written as an ordinary `INNER JOIN`: 6,350 buffers against the pinned
form's 6,358. An earlier draft of this page claimed it was worth 35% of a
drain, from a fixture carrying 200,000 resolution events; that figure is not
reproducible from the committed harness and is withdrawn. What the pin buys is
a plan that cannot change shape with the statistics, at no measured cost --
which is the argument for it, and the only one this page makes.

**The resolution check stays a `NOT EXISTS`, and that is measured too.** An
earlier revision of this change pinned it the same way, as a `LEFT JOIN
LATERAL ... WHERE res.resolved IS NULL`. The two forms select the same rows.
The outer join costs twice as much, and the reason is join order. As an
anti-join the planner runs the resolution probe first and lifts the executions
probe above it, so the executions probe runs once per claim. An outer join
cannot be reordered, so both probes run for every row about to be discarded --
and, per the next section, most rows are discarded. Over a history of 8,000
resolved requests: 24,253 buffers as an anti-join, 48,253 as an outer join.
`external_outbox_scan_tests::outbox_claim_plans_are_index_only` asserts the
plan keeps an `Anti Join`, so the regression cannot come back unnoticed.

`ORDER BY e.timestamp, e.id` pins the outer scan. Only the new partial index
produces that order, so every competing plan needs an explicit sort, and a sort
under `LIMIT 1` must read every candidate before it can return one. The ordered
index scan returns after one row instead. On a table large enough for the
choice to matter the planner takes the index under every estimate tested; see
Known limitations for the envelope.

**`ORDER BY e.id` alone does not pin it**, and this was measured rather than
assumed. `harvest_events_pkey` supplies `id` order too, so under an inflated
estimate the planner walked the primary key, filtering every unrelated event
out of a full ascending scan -- not a `Seq Scan`, and just as expensive.
Prefixing the order with `event_type` does not help either: the planner drops a
column that the `WHERE` clause pins to a constant from the ordering it has to
satisfy, which makes the primary key eligible again. `timestamp` is neither
constant nor served by any other index on this table.

## 📊 Measurement

Full 50-request drain, `pg_stat_statements` total buffers, Postgres 16.13.

Two harnesses measure this, over the same fixture, and both are committed.
`autumn-harvest/scripts/external_outbox_scan_matrix.sh` produces the tables
below: it swaps schema and query text freely, which is what a comparison of
combinations needs. The artifacts in `docs/perf-artifacts/external-outbox-scan/`
come from the Rust evidence-capture test, which drives the shipped query
through the real drain loop and cannot answer "what would the alternatives
have cost".

**Read these as orders of magnitude, not as six-figure quantities.** A drain
figure is not reproducible to the digit, in either direction. The pre-change
form varies because `synchronize_seqscans` moves where a sequential scan
starts. The as-shipped form varies because each claim walks the requests
earlier claims resolved, so its cost depends on what is already cached. Ranges
below are the spread over three runs on an otherwise idle server. The
per-claim *plan* is stable in a way the totals are not: the as-shipped cold
claim measured 6 to 8 buffers in every run of every fixture on this page.

| scenario | buffers (3 runs) |
|:--|--:|
| baseline -- no index, legacy query | ~299,000 -- ~1,079,000 |
| this change's outer index only, legacy query | ~277,400 -- ~277,800 |
| **rewrite only**, no indexes | ~1,605,000 -- ~2,134,000 |
| all four indexes, legacy query | ~5,900 -- ~15,100 |
| **all four indexes + rewrite (as shipped)** | ~5,800 -- ~15,200 |

Three things this table says, in order of how much they matter.

**The indexes are the fix.** Roughly 300,000 buffers become roughly 10,000, a
reduction around 95 to 97%. The committed evidence capture, through the real
query path, puts one instance at 299,082 → 16,679.

**With accurate statistics the rewrite adds nothing measurable.** The last two
rows overlap completely; across runs each is sometimes the lower one. This page
does not claim otherwise, and the next section is where the rewrite earns its
place.

**Each half alone is worse than shipping both.** The rewrite without the
indexes is 5 to 7 times the baseline, because a correlated `LATERAL` probe with
nothing to serve it is a scan. The outer index alone buys about 7% of the
workload, which is the same lesson issue #1486 drew from its own experiment.
That row is not a re-run of the issue's experiment: the issue indexed
`(event_type, workflow_exec_id)` with no resolution index, and a separate
re-run of that exact combination does reproduce its regression, at roughly
+260%.

Single cold claim:

| scenario | buffers | dominant node |
|:--|--:|:--|
| before | 21,245 | `Seq Scan on harvest_events` (`Rows Removed by Filter: 1,020,000`) |
| after | 7 | none -- every node is a keyed index scan |

### Under a stale row estimate

The fresh-statistics rows above are the easy case, and publishing only them
would have published plan-dependent luck. The hard case is the one issue #1486
ran into, and its cause is one a deployment meets in practice: an outage fills
the outbox, autovacuum's `ANALYZE` records the backlog, the backlog drains,
and the stored estimate stays orders of magnitude above the truth.

Same fixture, with the row estimate left 400x above the real pending count:

| scenario | drain buffers | cold claim |
|:--|--:|--:|
| baseline | 1,104,220 | 42,567 |
| all four indexes, legacy query | 27,605 | 21,042 |
| **all four indexes + rewrite (as shipped)** | **6,351** | **8** |

Unlike the table above, these reproduce closely: a second run gave 1,104,221,
27,606 and 6,364. Pinning the statistics removes the planner's freedom to pick
a different scan start, which is most of what made the other table move.

The index-only form gives back most of its win here: its cold claim is 21,042
buffers, because the planner abandons the partial index for a `Seq Scan`. The
pinned form stays at 8, and the drain is 4.3 times cheaper.

So the two halves are not load-bearing in the same way, and this page does not
claim they are. The indexes are what make the workload cheap. The rewrite is
what keeps it cheap when the planner works from a stale estimate, which is
exactly the condition a drained outage backlog creates.

### The cost that remains: request history

The numbers above come from a fixture with 50 external requests and no prior
request history. That is the best case for this design, and it is worth being
explicit about why.

`harvest_events` is append-only. An `ExternalSignalRequested` row is never
deleted, so `idx_harvest_events_external_outbox_pending` accumulates **every
external request the deployment has ever made**, not the pending ones. Each
claim walks the already-resolved ones before it reaches a pending one.

Same fixture, adding N already-resolved signal requests back-dated 30 days:

| resolved requests in the index | pre-change drain | as-shipped drain | pre-change cold claim | as-shipped cold claim |
|--:|--:|--:|--:|--:|
| 0 | ~300,000 | ~10,000 | 21,245 | 7 |
| 2,000 | 725,078 | 419,508 | 21,250 | 8,100 |
| 8,000 | 2,275,566 | 1,657,511 | 21,260 | 32,369 |
| 20,000 | 5,109,855 | 4,132,989 | 21,287 | 80,908 |

Three things follow, and the third is the one to plan around.

**The drain stays cheaper with the change at every history level tested**, by
19% to 42%. That is the figure that matters operationally, because a worker
drains rather than issuing one claim.

**But the advantage shrinks as history grows**, from roughly 97% at no history
to 19% at 20,000 resolved requests. Both forms become dominated by the same
walk over resolved requests, so the index advantage is progressively diluted.

**A single cold claim can be worse with the change once history is large.** At
8,000 resolved requests the as-shipped claim costs 32,369 buffers against the
pre-change 21,260. The cause is the `ORDER BY`: the ordered scan always starts
at the oldest request, so back-dated history is walked in full, while the
pre-change sequential scan reads in heap order and may meet a pending row
sooner. The change trades a cost that depends on physical layout for one that
is predictable and, over a drain, lower. It does not make a claim over a large
history cheap.

## ✅ Equivalence

`external_outbox_scan_tests::outbox_claim_queries_match_the_legacy_anti_join`
runs the pre-#1486 SQL and the rewritten SQL against the same fixture, for all
three families, and asserts they select the same rows. The fixture covers every
predicate the rewrite touches, including the two the issue named as needing
their own correctness review:

* **`NOT EXISTS` NULL semantics.** The check is still a `NOT EXISTS`, and never
  a `NOT IN`, which would change the answer whenever a correlation id is SQL
  NULL. The fixture seeds a request whose correlation key is absent and one
  whose key is JSON null; both stay excluded, as they were before.
* **The `FOR UPDATE OF e SKIP LOCKED` contract.** The rewrite locks the same
  single relation, and `e` remains on the non-nullable side of the outer join,
  which is the case Postgres permits. Nothing else in the query is locked.

The evidence capture asserts equivalence a second way, over a full drain: both
forms resolve the same 50 requests.

One behaviour does change. `ORDER BY e.timestamp, e.id` makes the drain order
oldest-first instead of arbitrary, so a backlog cannot be starved by newer
arrivals. `outbox_claim_returns_the_oldest_pending_request_first` asserts it.

## 💸 Write cost

All four indexes are partial on event types that are rare in any history, so
the cost falls on those rows alone.

* Build cost: all four indexes together took 642 ms on a 1.22M-row fixture
  holding 200,000 resolution events. Each build scans the whole table to
  evaluate its partial predicate, so the build window tracks total
  `harvest_events` rows, not matching ones. All four run inside one
  transaction, so the `SHARE` lock is held once across the four, not four
  times.
* Index size tracks matching rows only, at roughly 55 bytes per row: 11 MB for
  200,000 resolution events on that fixture, and 16 kB for a pending index
  holding 50 rows.
* WAL on the request path: **about 103 extra bytes per
  `ExternalSignalRequested` insert**, which is one index entry. That absolute
  figure is the stable one, and it is the one to plan with. The percentage is
  not: over 10,000 inserts with a `CHECKPOINT` before each run it reads +2.7%
  against a minimal payload and +1.8% against a payload padded by 1.8 kB,
  because only the denominator moves.
* WAL for 10,000 `ActivityStarted` inserts, an event type none of these indexes
  covers: 19,271,112 without and 19,133,120 with. No systematic cost, which is
  what partial scope is supposed to buy.

`CREATE INDEX` (not `CONCURRENTLY`) takes `SHARE` on `harvest_events` for the
build. On a live, already-large deployment, build the four out of band first
and this migration's guard accepts them; the recipe, including the partitioned
layout's per-leaf variant, is written out in full in
`20260905181020_harvest_usage_activity_lookback_index/up.sql`.

## 🔬 Reproduce

```bash
# Fast, always-run gates (plans, legacy equivalence, drain order):
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  cargo test -p autumn-harvest --features db --test integration external_outbox_scan

# Full evidence capture (seeds 1.02M event rows; about 40 seconds):
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  ./autumn-harvest/scripts/external_outbox_scan_perf_repro.sh
```

Artifacts land in `docs/perf-artifacts/external-outbox-scan/`.

## 🚧 Known limitations

* **The claim cost grows with lifetime request history, and this change does
  not fix that.** See the measured table above. The shape is O(H x B) for H
  lifetime requests of a family and B pending ones. This change lowers the
  constant; it does not change the shape, and its advantage decays from about
  97% at no history to about 19% at 20,000 resolved requests.

  An earlier draft of this page proposed adding successfully-resolved ids to
  the sweep's `excluded_event_ids` list as the follow-up. That is wrong, and
  the correction is worth recording: a historical request is never *claimed*,
  so it can never enter that list. Reading the loop, only the give-up paths
  push an id, and every successful step restarts the claim from the top. The
  exclusion list would remove the B² term within one sweep and leave the
  larger H x B term untouched.

  Closing H needs the pending set to stop containing resolved requests -- a
  resolved marker on the request row, or a separate outbox table. Both are
  writes to `harvest_events` or a schema change, so both are out of scope for
  a change that must not add a third writer to an append-only log.

* **The plan pin has an envelope, and it is not "any estimate".** On a table
  small enough that a sequential scan plus a sort genuinely is cheaper, the
  planner takes it: the crossover measured between 201 and 301 events. The
  claim this page makes is about a table large enough for the choice to
  matter. Within that envelope the pin held against a 405x over-estimate, a
  1-row under-estimate, `random_page_cost` raised from 4 to 100, and
  `cpu_operator_cost` raised to 1.

* **Measured in isolation.** This is the scanner's own cost, not its share of
  a mixed end-to-end workload. The same limitation issue #1486 stated stands.

* **The cancel and await siblings are covered by construction, not by their
  own drain measurement.** All three queries come from one template and are
  gated per family by the plan, equivalence and drain-order tests, so a
  difference between them fails CI. Only the signal family's drain was
  profiled.

* **Drain totals are not reproducible to the digit, in either direction.**
  Measured spreads over four runs: baseline about 299,000 to 1,079,000, as
  shipped about 5,800 to 15,200. The pre-change form moves because `synchronize_seqscans`
  changes where its sequential scan starts; the as-shipped form moves with
  cache state, because each claim walks what earlier claims resolved. Only the
  per-claim plan is stable: the as-shipped cold claim measured 6 to 8 buffers
  everywhere. Quote the order of magnitude, not the digits.

## See also

* [`docs/performance-usage-report-activity-lookback.md`](performance-usage-report-activity-lookback.md)
  -- the other `harvest_events` expression index, and the out-of-band build
  recipe this migration points at.
* [`docs/performance.md`](performance.md) -- the index of profiling passes.
