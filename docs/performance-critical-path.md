# `critical_path::CriticalPathAnalyzer::analyze` — a redundant sink-detection pass

Wall-clock timing is not admissible evidence on this (shared-vCPU) machine —
every number below is a deterministic instruction count
(`valgrind --tool=callgrind`) or allocation count/bytes
(`valgrind --tool=dhat`), both reproducible run-to-run to within a fraction
of a percent (the only source of variance is `HashMap`'s per-process
`RandomState`, which affects the `activity_durations` table-build path, not
the loop this page changes).

## 🎯 Workload

`CriticalPathAnalyzer::analyze` is the longest-path computation behind the
crate's DAG bottleneck-analysis API (`crate::critical_path`, re-exported at
the crate root and consumed by
`dag_export::export_mermaid_with_critical_path`'s highlighting).

The harness is `autumn-harvest/benches/critical_path_profile.rs`, already
committed but never profiled before this pass. It builds a `40×40` dense
barrier DAG (`CRITICAL_PATH_PROFILE_STAGES` / `_WIDTH`, defaults 40 each) —
every node in stage *s* depends on every node in stage *s-1*, the shape a
map-then-synchronize batch/ETL pipeline produces — giving 1,600 tasks and
~62,400 upstream edges, and calls `analyze()` on the same fixed analyzer
`CRITICAL_PATH_PROFILE_REPS` times (default 50). Five of six activity types
are mocked (the `HashMap`-vs-linear-scan tuning documented at the top of
`analyze` already handles that path); the DAG and analyzer are built once,
outside the measured loop, so their one-time cost is not attributed to
`analyze()` itself.

```bash
BIN=$(cargo bench -p autumn-harvest --no-default-features \
  --bench critical_path_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="critical_path_profile") | .executable')
valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --threshold=98 cg.out
valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
```

## 📈 Profile

Flat profile, pre-fix (`docs/perf-artifacts/critical-path-analyze/before-callgrind-flat.txt`):

```
105,644,121 (100.0%)  PROGRAM TOTALS

81,022,150 (76.69%)  autumn_harvest::critical_path::CriticalPathAnalyzer::analyze
 6,583,200 ( 6.23%)  autumn_harvest::dag::DagTaskRef::upstream        <- one-time DAG build
 2,514,310 ( 2.38%)  _int_malloc
 2,034,938 ( 1.93%)  autumn_harvest::dag::DagBuilder::build           <- one-time DAG build
 1,360,544 ( 1.29%)  __memcmp_avx2_movbe
```

`analyze()` accounts for 76.69% of the profile — the DAG-construction lines
(`DagTaskRef::upstream`, `DagBuilder::build`) are the harness's documented
one-time setup cost, run once outside the 50-rep measured loop, not part of
the target.

A separate debug-info build (`CARGO_PROFILE_BENCH_DEBUG=true`, confirmed to
produce an identical 105,636,492 Ir — 0.007% run-to-run noise, matching the
harness's own documented `RandomState` variance) gives a line-level view of
`analyze()` itself
(`docs/perf-artifacts/critical-path-analyze/before-callgrind-annotated-src.txt`):

```
3,120,000 ( 2.95%)   for &up_idx in &task.upstreams {          <- main DP loop
3,120,000 ( 2.95%)       if distances[up_idx] >= max_upstream_dist {
2,496,000 ( 2.36%)           if distances[up_idx] > max_upstream_dist || best_pred.is_none() {
  156,000 ( 0.15%)               max_upstream_dist = distances[up_idx];
...
3,120,000 ( 2.95%)   for &up_idx in &task.upstreams {          <- separate sink-detection loop
3,120,000 ( 2.95%)       is_sink[up_idx] = false;
```

Both `for &up_idx in &task.upstreams` lines show the **identical** count,
3,120,000 — exactly 50 reps × 62,400 edges — because the sink-detection loop
(`for task in tasks { for &up_idx in &task.upstreams { is_sink[up_idx] =
false; } }`) walks the same edge set the main per-level DP loop already
walked, in a second, separate full pass. That's a real, not synthetic,
finding: two O(edges) traversals doing work that fits in one.

## 💡 Hypothesis

A node is a sink iff no other node names it as an upstream — a property of
the edge set, not of traversal order. The main DP loop already visits every
`(task, upstream)` edge exactly once while computing `distances`/
`predecessors`; marking `is_sink[up_idx] = false` inside that same loop
produces the identical result as the separate pass, because
`execution_levels()` partitions every task index across `levels` exactly
once (the same set `tasks` iterates), so the DP loop's `for level in levels
{ for &task_index in level { ... } }` already touches every task and every
edge the sink-detection loop's `for task in tasks { ... }` would. Folding
one loop into the other removes a full redundant edge-count traversal —
including its share of the bounds-checked slice indexing and iterator
plumbing the flat profile's `core::slice::index`/`slice::iter::macros`/
`ptr::non_null` lines already show dominating `analyze`'s inlined cost.

## 🔧 Change

`autumn-harvest/src/critical_path.rs`, `CriticalPathAnalyzer::analyze`:

* `is_sink` is allocated before the main per-level loop instead of after it.
* The main loop's `for &up_idx in &task.upstreams` body gains one line,
  `is_sink[up_idx] = false;`, alongside the existing distance/predecessor
  bookkeeping.
* The old second pass (`for task in tasks { for &up_idx in &task.upstreams
  { is_sink[up_idx] = false; } }`) is deleted.

No behavior change: `is_sink` ends up holding the exact same values either
way, since which indices get named as an upstream does not depend on
traversal order, only the (unchanged) edge set. All four existing
`critical_path::tests::*` unit tests, plus
`dag_export::tests::test_export_mermaid_with_critical_path` (the one other
caller in the crate), pass unmodified — no test's expected value needed to
change.

## 📊 Measurement

Same harness, same machine and session, differing only by the diff above.

### Instructions (Ir), `valgrind --tool=callgrind --branch-sim=no --cache-sim=no`

| | Instructions (Ir) |
|---|---|
| Before | 105,644,121 |
| After  | 89,324,425 |
| **Reduction** | **16,319,696 (15.45%)** |

Re-run twice more post-fix to bound run-to-run noise: 89,330,375 and
89,321,830 — a ±0.006% spread, three orders of magnitude below the 15.45%
delta this change produces. `analyze()`'s own self-cost (flat profile) drops
from 81,022,150 to 64,708,150 Ir (-20.13%), and no other listed function's
self-cost moves at all (`DagTaskRef::upstream`, `DagBuilder::build`,
`_int_malloc` etc. are byte-for-byte identical before and after) — the
change's effect is isolated to exactly the function it targets, as expected
of a change that touches nothing outside `analyze`.

Clears the ≥5%-of-instructions floor (measured on a benchmark that is
76.69% of its own profile, clearing the ≥5%-of-workload gate by >15×) by a
wide margin.

### Allocations (`valgrind --tool=dhat`)

| dhat | Before | After |
|---|---|---|
| Total blocks | 23,133 | 23,133 |
| Total bytes  | 8,291,384 | 8,291,384 |

Unchanged, as expected: this change removes redundant *iteration*, not any
allocation — `is_sink`'s single `vec![true; tasks.len()]` allocation is
simply hoisted earlier in the same function, not duplicated or removed. The
claim here is an instruction-count claim only; allocation counts are
reported to show they did not regress.

### Correctness

* `cargo fmt -p autumn-harvest -- --check` — clean.
* `cargo test -p autumn-harvest --lib --no-default-features` —
  **2,393 passed, 0 failed**, including all `critical_path::tests::*` and
  `dag_export::tests::test_export_mermaid_with_critical_path`, unchanged in
  expectation.
* `python3 docs/audits/comment-hygiene.py --base origin/trunk-dev` — OK, no
  Tier A findings, no Tier B regressions.

## 🔬 Reproduce

```bash
BIN=$(cargo bench -p autumn-harvest --no-default-features \
  --bench critical_path_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="critical_path_profile") | .executable')

# Instructions:
valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --threshold=98 cg.out | head -10

# Allocations:
valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
```

Full artifacts: `docs/perf-artifacts/critical-path-analyze/{before,after}-callgrind-flat.txt`,
`before-callgrind-annotated-src.txt`, `{before,after}-dhat.json`.
