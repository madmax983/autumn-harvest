//! Deterministic (non-criterion) instruction/allocation-count profiling
//! harness for `autumn_harvest::critical_path::CriticalPathAnalyzer::analyze`
//! — the longest-path computation behind the crate's DAG bottleneck-analysis
//! API (`crate::critical_path`, re-exported at the crate root and consumed by
//! `dag_export::export_mermaid_with_critical_path`'s highlighting). Wall-clock
//! timing is not admissible evidence on this (shared-vCPU) machine — every
//! number this harness produces evidence for is a deterministic instruction
//! count (`valgrind --tool=callgrind`) or allocation count/bytes
//! (`valgrind --tool=dhat`), both reproducible bit-for-bit on any machine.
//!
//! # Workload
//!
//! A realistic wide, multi-stage DAG: `STAGES` sequential barrier stages,
//! each with `WIDTH` parallel activity nodes that ALL depend on EVERY node in
//! the previous stage (a full fan-in/fan-out barrier) — the shape a
//! map-then-synchronize batch/ETL pipeline produces when each stage must
//! fully complete before the next begins (e.g. "extract N shards, then
//! transform N shards, then load N shards", repeated). This is deliberately
//! dense (not a sparse chain): `analyze`'s inner upstream-scan loop is the
//! part under profile, so the workload needs many (task, upstream-edge)
//! pairs, not just many tasks. Activity durations are set via
//! `mock_duration` (by name, the same API `dag_export`'s own tests and any
//! real caller uses) for FIVE of the six activity types, which forces the
//! analyzer's per-task name-keyed lookup path rather than the per-task
//! `start_to_close` override shortcut — the common case for a caller
//! comparing named-activity cost estimates rather than per-instance
//! overrides. The sixth type is deliberately left unmocked so ~1/6 of tasks
//! exercise the `default_duration` fallback (a lookup miss) every call,
//! alongside the other five (hits) — ordinary usage, and the regime that
//! bounds how large `CriticalPathAnalyzer::LINEAR_SCAN_CARDINALITY_LIMIT`
//! can safely be (a miss must scan every candidate; a hit can stop early).
//!
//! `CRITICAL_PATH_PROFILE_STAGES` (default `40`) and
//! `CRITICAL_PATH_PROFILE_WIDTH` (default `40`) size the DAG (`40×40` = 1,600
//! tasks, ~62,400 upstream edges — stage 0 has no upstream, so 39 dense
//! stages × 40×40 edges each). `CRITICAL_PATH_PROFILE_REPS` (default `50`)
//! sets how many times the same (fixed) `CriticalPathAnalyzer` is asked to
//! `analyze()` — the DAG and the analyzer are built ONCE, outside the
//! measured loop, isolating `analyze()`'s own cost from `DagBuilder::build`'s
//! (already covered by a prior Bolt fix in `dag.rs`, see its
//! `execution_levels.push` comment) and from `mock_duration`'s registration
//! cost.
//!
//! # Baseline
//!
//! This harness was committed but never run against a profiler until the
//! pass recorded in `docs/performance-critical-path.md`. Fresh baseline on
//! that pass's starting commit, default sizing, `valgrind --tool=callgrind
//! --branch-sim=no --cache-sim=no`:
//!
//! ```text
//! 105,644,121 (100.0%)  PROGRAM TOTALS
//! 81,022,150  ( 76.69%)  autumn_harvest::critical_path::CriticalPathAnalyzer::analyze
//! ```
//!
//! See that document for the full profile, the fix, and the after numbers.
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest --no-default-features \
//!   --bench critical_path_profile --no-run --message-format=json 2>/dev/null \
//!   | jq -r 'select(.reason=="compiler-artifact" and .target.name=="critical_path_profile") | .executable')
//! valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
//! callgrind_annotate --threshold=98 cg.out
//! valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
//! ```

use std::time::Duration;

use autumn_harvest::critical_path::CriticalPathAnalyzer;
use autumn_harvest::dag::{DagBuilder, DagTaskRef};

/// Reads `key` as a `usize`, using `default` only when the variable is
/// genuinely *absent*. A *present but malformed* value is a configuration
/// error, not silently substituted for the default.
fn env_usize(key: &str, default: usize) -> usize {
    match std::env::var(key) {
        Ok(raw) => raw
            .parse()
            .unwrap_or_else(|e| panic!("{key}={raw:?} is not a valid usize: {e}")),
        Err(std::env::VarError::NotPresent) => default,
        Err(std::env::VarError::NotUnicode(raw)) => {
            panic!("{key}={} is not valid Unicode", raw.to_string_lossy())
        }
    }
}

// Six distinct activity functions, cycled by column index within a stage, so
// the DAG carries a realistic small set of repeated named activities (a
// wide fan-out map stage in production runs the SAME handful of activity
// types across many parallel shards) rather than either one universal name
// or an unrealistic one-name-per-node cardinality that would never
// legitimately collide in `activity_durations`.
fn extract_shard() {}
fn transform_shard() {}
fn validate_shard() {}
fn enrich_shard() {}
fn load_shard() {}
fn checkpoint_shard() {}

fn push_activity(builder: &mut DagBuilder, column: usize) -> DagTaskRef {
    match column % 6 {
        0 => builder.activity(extract_shard),
        1 => builder.activity(transform_shard),
        2 => builder.activity(validate_shard),
        3 => builder.activity(enrich_shard),
        4 => builder.activity(load_shard),
        _ => builder.activity(checkpoint_shard),
    }
}

/// Build a `stages`-deep, `width`-wide dense barrier DAG: every node in stage
/// `s` (`s > 0`) depends on every node in stage `s - 1`.
fn build_dag(stages: usize, width: usize) -> autumn_harvest::dag::DagDefinition {
    let mut builder = DagBuilder::new();
    let mut previous_stage: Vec<DagTaskRef> = Vec::new();
    for _stage in 0..stages {
        let mut current_stage = Vec::with_capacity(width);
        for column in 0..width {
            let mut node = push_activity(&mut builder, column);
            for upstream in &previous_stage {
                node = node.upstream(upstream);
            }
            current_stage.push(node);
        }
        previous_stage = current_stage;
    }
    builder
        .build()
        .expect("dense barrier DAG must be a valid DAG (no cycle, all indices in range)")
}

fn main() {
    let stages = env_usize("CRITICAL_PATH_PROFILE_STAGES", 40);
    let width = env_usize("CRITICAL_PATH_PROFILE_WIDTH", 40);
    let reps = env_usize("CRITICAL_PATH_PROFILE_REPS", 50);

    assert!(
        stages > 0,
        "CRITICAL_PATH_PROFILE_STAGES must be at least 1, got 0"
    );
    assert!(
        width > 0,
        "CRITICAL_PATH_PROFILE_WIDTH must be at least 1, got 0"
    );
    // reps=0 would exit having measured nothing but DAG construction, and
    // could be mistaken for a valid (implausibly fast) measurement.
    assert!(
        reps > 0,
        "CRITICAL_PATH_PROFILE_REPS must be at least 1, got 0"
    );

    let dag = build_dag(stages, width);
    let expected_tasks = stages * width;
    assert_eq!(
        dag.tasks().len(),
        expected_tasks,
        "build_dag(stages={stages}, width={width}) should produce {expected_tasks} tasks"
    );

    // Five of the six activity types are mocked; `checkpoint_shard` is
    // deliberately left unmocked so ~1/6 of tasks exercise the
    // `default_duration` fallback (a miss) on every `analyze()` call --
    // ordinary usage (an operator typically has historical timing data for
    // some activity types, not all), and the shape `analyze`'s duration
    // lookup must stay correct and non-regressive under (see
    // `CriticalPathAnalyzer`'s `LINEAR_SCAN_CARDINALITY_LIMIT` doc comment).
    let analyzer = CriticalPathAnalyzer::new(dag)
        .with_default_duration(Duration::from_millis(100))
        .mock_duration("extract_shard", Duration::from_millis(120))
        .mock_duration("transform_shard", Duration::from_millis(340))
        .mock_duration("validate_shard", Duration::from_millis(80))
        .mock_duration("enrich_shard", Duration::from_millis(210))
        .mock_duration("load_shard", Duration::from_millis(150));

    let mut total_path_len = 0usize;
    for _ in 0..reps {
        let result = analyzer.analyze();
        // The dense barrier shape always terminates at a stage-(stages-1)
        // node, so the critical path always has exactly `stages` hops
        // (one winning node per stage) — self-checked so a change that
        // silently altered predecessor selection (not just its cost) would
        // fail this harness rather than produce a quietly-wrong "faster"
        // number.
        assert_eq!(
            result.path_indices.len(),
            stages,
            "a dense {stages}x{width} barrier DAG's critical path should visit exactly one node \
             per stage, got {} hops",
            result.path_indices.len()
        );
        total_path_len += result.path_indices.len();
        std::hint::black_box(&result);
    }

    println!(
        "critical_path_profile: stages={stages} width={width} reps={reps} \
         total_tasks={expected_tasks} total_path_hops={total_path_len}"
    );
}
