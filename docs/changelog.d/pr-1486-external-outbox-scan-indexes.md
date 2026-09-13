## Perf — Index both sides of the external signal/cancel/await outbox scans, and pin their plan (issue #1486)

`timeout::enforce_timeouts_once` runs three sibling outbox scanners on every
worker's periodic tick: `enforce_external_signals_outbox`,
`enforce_external_cancels_outbox` and `enforce_external_awaits_outbox`. Each
drains its own outbox of pending cross-workflow `ctx.signal()` /
`ctx.cancel()` / `ctx.await_external()` requests with the same query — claim
one candidate row under `LIMIT 1 ... FOR UPDATE OF e SKIP LOCKED`, act on it,
loop until empty. Neither side of that claim was indexed.

Issue #1486 profiled the outer scan and filed a **negative result**: the
obvious partial index on `event_type` regressed a full drain by 271%, and
raising the statistics target oscillated between plans mid-drain. It
recommended pinning the paired anti-join structurally before re-measuring the
index. This PR does both, and the measurements below confirm the issue's
reasoning: the outer index alone buys about 7% of a drain, and the rewrite
alone is 5 to 7 times worse than doing nothing.

**Migration `20260911213344_harvest_external_outbox_scan_indexes`** adds four
partial indexes on `harvest_events`. One,
`idx_harvest_events_external_outbox_pending` on `(event_type, timestamp, id)
WHERE event_type IN ('ExternalSignalRequested', 'ExternalCancelRequested',
'ExternalAwaitRequested')`, serves all three outer scans: each scanner filters
a single type, which the leading column answers as a prefix, and its
`(timestamp, id)` columns supply the order that pins the plan. Three more —
`idx_harvest_events_external_{signal,cancel,await}_resolved` — key the
resolution check on `(workflow_exec_id, (event_data->'data'->>'<id>'))`,
partial on that family's two terminal event types. The resolution check was
the larger of the two costs, by a wide margin: adding the outer index alone
moves a full drain by about 7%, leaving the rest on the resolution side. Correlated per execution and unindexed, it
re-read every event of the owning execution once per already-resolved
candidate a drain stepped over, so its cost grew with the square of the
backlog.

**Query rewrite (`timeout.rs`).** The three claim queries are now generated
from one `external_outbox_claim_query!` template, so the shape cannot drift
between siblings. The executions join is pinned to its correlated form by a
`LIMIT 1` inside a `LATERAL`, which cannot be pulled up into the outer query,
and the
outer scan is pinned by `ORDER BY e.timestamp, e.id`, which no other index on
this table can produce. The pins remove the plan's dependence on a row
estimate that swings across the outbox's own draining range. `ORDER BY e.id`
alone does **not** pin it — `harvest_events_pkey` supplies `id` order too, so
an inflated estimate sends the planner walking the primary key instead.

The resolution check deliberately stays a `NOT EXISTS`. An earlier revision
pinned it the same way, as a `LEFT JOIN LATERAL ... WHERE res.resolved IS
NULL`; the two select the same rows, but the outer join costs twice as much
once resolved requests accumulate, because it cannot be reordered and so pays
the executions probe for every row it is about to discard. `FOR UPDATE OF e
SKIP LOCKED` locks the same single relation as before.

**Measured** on the issue's own fixture (5,000 RUNNING executions x 200
events, plus a 2,000-execution terminal tail, 1.02M rows), full 50-request
drain, `pg_stat_statements` total buffers, measured with a plain `psql`
harness:

| scenario | buffers (3 runs) |
|:--|--:|
| baseline (no index, legacy query) | ~299,000 – ~1,079,000 |
| this change's outer index only, legacy query | ~277,400 – ~277,800 |
| rewrite only, no indexes | ~1,605,000 – ~2,134,000 |
| all four indexes, legacy query | ~5,900 – ~15,100 |
| **all four indexes + rewrite (as shipped)** | **~5,800 – ~15,200** |

Read these as orders of magnitude. A drain total is not reproducible to the
digit in either direction: the pre-change form moves with where
`synchronize_seqscans` starts its scan, and the as-shipped form moves with
cache state, since each claim walks what earlier claims resolved. The committed
evidence capture, driving the real query text through the real drain loop, put
one instance at 299,082 → 16,679. The per-claim plan is the stable part: a cold
claim falls from 21,245 buffers (`Seq Scan`, `Rows Removed by Filter:
1,020,000`) to 7, in every run of every fixture measured.

With accurate statistics the indexes carry the whole win and the rewrite adds
nothing measurable — the last two rows overlap. The rewrite earns its place
under a stale row estimate (400x the truth, the shape an outage backlog leaves
behind): there the drain costs 1,104,220 buffers at baseline, 27,605 with the
indexes alone, and 6,351 with both, because the index-only form reverts to a
sequential scan.

**Residual.** `harvest_events` is append-only, so the pending index holds every
request ever made and each claim walks the resolved ones first. The drain stays
cheaper at every history size measured, but the margin decays from about 97% at
no history to about 19% at 20,000 resolved requests. Closing that needs a
resolved marker or a separate outbox table, both out of scope here; see
`docs/performance-external-outbox-scan.md`.

**Behaviour.** No `WorkflowEvent` variant, no column change, no data
migration, no replay impact, and no change to which rows the scanners claim —
`external_outbox_scan_tests::outbox_claim_queries_match_the_legacy_anti_join`
asserts the rewritten SQL selects exactly the legacy row set across every
predicate the rewrite touches, for all three families. The one behaviour
change is drain order: `ORDER BY e.timestamp, e.id` claims the oldest pending
request first instead of an arbitrary one, so a backlog cannot be starved by
newer arrivals.

**Tests.** `autumn-harvest/tests/integration/external_outbox_scan_tests.rs` —
two plan gates (index-only plans, and the same plans under a stale row
estimate), the legacy-equivalence gate, a drain-order gate, and an
`#[ignore]`d evidence capture behind
`autumn-harvest/scripts/external_outbox_scan_perf_repro.sh`. Plans and
`pg_stat_statements` snapshots are committed under
`docs/perf-artifacts/external-outbox-scan/`; the writeup is
`docs/performance-external-outbox-scan.md`.
