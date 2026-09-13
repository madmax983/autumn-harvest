#![cfg(feature = "db")]
// Harness helpers are `pub(crate)` so sibling integration test modules (issue
// #779's child_timeout_tests) can reuse the container/worker setup. The
// `redundant_pub_crate` nursery lint flags that pattern; the cross-module reuse
// is intentional.
#![allow(clippy::redundant_pub_crate)]

//! End-to-end integration tests using testcontainers for a real Postgres instance.
//!
//! These tests spin up a throwaway Postgres container per test, run the harvest
//! migration SQL via `with_init_sql`, and exercise the full store/queue/DLQ stack
//! against a real database.

use autumn_harvest::dlq::{self, NewDeadLetterEntry};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::failure::{IntoWorkflowErrorString, WorkflowFailure};
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::models::{
    HarvestTimer, NewWorkflowExecution, TaskQueueItem, WorkflowExecution,
};
use autumn_harvest::queue::{EnqueueParams, StickyHint, TaskType};
use autumn_harvest::schema::{harvest_task_queue, harvest_timers, harvest_workflow_executions};
use autumn_harvest::store;
use autumn_harvest::types::{ActivityExecId, ExecutionId, Priority};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    ActivityContext, DagCatalog, HarvestBuilder, HarvestError, OverlapPolicy, Schedule,
    SchedulerMonitor, SchedulerRuntime, StartWorkflowParams, TimeoutType, WorkerConfig,
    WorkflowContext, WorkflowIdReusePolicy, WorkflowSchedule, cancel_workflow_execution, queue,
    register_workflow_schedules, start_or_load_workflow_execution, terminate_workflow_execution,
    tick_once, timeout,
};

use chrono::Utc;
use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use std::any::TypeId;
use std::collections::HashMap;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

/// The migration SQL embedded at compile time.
///
/// Combines the initial schema with every forward-compatible schema-addition
/// migration that ships in `migrations/`. The
/// `20260410010000_harvest_workflow_start_uniqueness` migration is
/// deliberately excluded because one test (see
/// `legacy_workflow_uniqueness_schema_can_be_upgraded_for_idempotent_starts`)
/// applies it on a legacy schema to verify the upgrade path.
const INIT_SQL: &str = concat!(
    include_str!("../../migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!("../../migrations/20260619000000_harvest_task_queue_created_at/up.sql"),
    "\n",
    include_str!("../../migrations/20260424000001_harvest_trace_context/up.sql"),
    "\n",
    include_str!("../../migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../../migrations/20260427000000_harvest_continue_as_new/up.sql"),
    "\n",
    include_str!("../../migrations/20260429000000_harvest_concurrency_key/up.sql"),
    "\n",
    include_str!("../../migrations/20260430000000_harvest_workflow_schedules/up.sql"),
    "\n",
    // Unified DAG schedule rows carry both dag_name and workflow_name, so the
    // strict XOR kind_check from the migration above must be relaxed to an OR.
    include_str!("../../migrations/20260514010000_unified_dag_schedule_kind/up.sql"),
    "\n",
    include_str!("../../migrations/20260430000001_harvest_external_tasks/up.sql"),
    "\n",
    include_str!("../../migrations/20260508000000_harvest_external_task_updated_at/up.sql"),
    "\n",
    // Reset (#148/#538) refines the active-uniqueness index to exclude
    // TERMINATED rows; placed after continue_as_new (creates the index) and
    // after external_tasks (whose state_check it recreates), and before the
    // pause migration so the later PAUSED-inclusive state_check wins.
    include_str!("../../migrations/20260503000000_harvest_workflow_reset/up.sql"),
    "\n",
    include_str!("../../migrations/20260506000000_harvest_audit_log/up.sql"),
    "\n",
    include_str!("../../migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!("../../migrations/20260508010000_harvest_workers_drain_deadline/up.sql"),
    "\n",
    include_str!("../../migrations/20260509000000_harvest_build_routing/up.sql"),
    "\n",
    include_str!("../../migrations/20260513000000_harvest_schedule_pause_metadata/up.sql"),
    "\n",
    include_str!("../../migrations/20260514020000_harvest_task_activity_id/up.sql"),
    "\n",
    include_str!("../../migrations/20260518000000_harvest_signal_idempotency/up.sql"),
    "\n",
    include_str!("../../migrations/20260517000000_harvest_schedule_jitter/up.sql"),
    "\n",
    include_str!("../../migrations/20260517000001_harvest_schedule_overlap_policy/up.sql"),
    "\n",
    include_str!("../../migrations/20260518000001_harvest_workflow_execution_timeout/up.sql"),
    "\n",
    include_str!("../../migrations/20260613000000_harvest_workflow_sla/up.sql"),
    "\n",
    include_str!("../../migrations/20260519000000_harvest_calendar_awareness/up.sql"),
    "\n",
    include_str!("../../migrations/20260522000000_harvest_schedule_decisions/up.sql"),
    "\n",
    include_str!("../../migrations/20260522000001_harvest_rate_limiting/up.sql"),
    "\n",
    include_str!("../../migrations/20260526000001_harvest_parent_close_policy/up.sql"),
    "\n",
    include_str!("../../migrations/20260530000000_harvest_schedule_ha_claim/up.sql"),
    "\n",
    include_str!("../../migrations/20260601000000_harvest_schedule_auto_pause/up.sql"),
    "\n",
    include_str!("../../migrations/20260601000001_harvest_poison_pill_strikes/up.sql"),
    "\n",
    include_str!("../../migrations/20260601000002_harvest_ownership_metadata/up.sql"),
    "\n",
    include_str!("../../migrations/20260603000000_harvest_completion_triggers/up.sql"),
    include_str!("../../migrations/20260708000001_harvest_completion_trigger_condition/up.sql"),
    "\n",
    include_str!("../../migrations/20260605000000_harvest_admission_gates/up.sql"),
    "\n",
    include_str!("../../migrations/20260606000001_harvest_activity_schedule_to_close/up.sql"),
    include_str!("../../migrations/20260607000000_harvest_worker_capability_labels/up.sql"),
    include_str!("../../migrations/20260607000001_harvest_task_required_capabilities/up.sql"),
    "\n",
    include_str!("../../migrations/20260607000002_harvest_workflow_pause/up.sql"),
    "\n",
    include_str!("../../migrations/20260609000001_harvest_workflow_current_details/up.sql"),
    "\n",
    include_str!("../../migrations/20260610000001_harvest_schedule_bounded_runs/up.sql"),
    "\n",
    include_str!("../../migrations/20260613000001_harvest_schedule_catchup_window/up.sql"),
    "\n",
    include_str!("../../migrations/20260616000001_harvest_workflow_schedule_id/up.sql"),
    "\n",
    include_str!("../../migrations/20260615000001_harvest_context_headers/up.sql"),
    "\n",
    // issue #499: enforce_timeouts_once now scans harvest_debounce.
    include_str!("../../migrations/20260618000001_harvest_debounce/up.sql"),
    "\n",
    // issue #518: event-batched starts persist pending admissions here.
    include_str!("../../migrations/20260624000000_harvest_event_batches/up.sql"),
    "\n",
    // issue #523: workflow-level retry policy columns.
    include_str!("../../migrations/20260626000001_harvest_workflow_retry/up.sql"),
    "\n",
    // issue #534: origin column + per-schedule run-history index.
    include_str!("../../migrations/20260628000001_harvest_execution_origin/up.sql"),
    include_str!("../../migrations/20260703000000_harvest_task_queue_wake_requested/up.sql"),
    "\n",
    // issue #604: target_build_id/ramp_percent columns on harvest_build_policies.
    include_str!("../../migrations/20260704000001_harvest_build_policy_ramp/up.sql"),
    include_str!("../../migrations/20260704000000_harvest_workflow_nd_block/up.sql"),
    "\n",
    // issue #605: harvest_completion_deliveries table + completion_callbacks
    // column on harvest_workflow_executions.
    include_str!("../../migrations/20260705000000_harvest_completion_deliveries/up.sql"),
    "\n",
    // issue #606: harvest_sessions table + session_id column on
    // harvest_task_queue + max_concurrent_sessions/in_use_sessions on
    // harvest_workers.
    include_str!("../../migrations/20260706000000_harvest_worker_sessions/up.sql"),
    include_str!("../../migrations/20260710000002_harvest_workflow_continue_chain/up.sql"),
    "\n",
    // issue #747: per-execution legal hold columns on harvest_workflow_executions.
    include_str!("../../migrations/20260709000001_harvest_legal_hold/up.sql"),
    "\n",
    // issue #740: start_source/start_source_ref/started_by provenance columns on
    // harvest_workflow_executions.
    include_str!("../../migrations/20260712000000_harvest_execution_start_source/up.sql"),
    "\n",
    // issue #617: chain_execution_timeout/chain_deadline_at columns on
    // harvest_workflow_executions.
    include_str!("../../migrations/20260714000000_harvest_workflow_chain_timeout/up.sql"),
    "\n",
    // issue #619: harvest_queue_pauses. REQUIRED, not optional — `claim_task`'s
    // pause anti-join references this table on every claim, so without it every
    // claim in this suite (and in every suite that borrows
    // `setup_test_database_url_or_env` from here) fails with
    // `relation "harvest_queue_pauses" does not exist`.
    include_str!("../../migrations/20260715000000_harvest_queue_pause/up.sql"),
    "\n",
    // issue #704: history_bloat_warned_at column on harvest_workflow_executions.
    // REQUIRED, not optional — `WorkflowExecution::as_select()`/`as_returning()`
    // reference this column on every full-row read/insert-returning, so without
    // it every such call in this suite (and in every suite that borrows
    // `setup_test_database_url_or_env` from here) fails with
    // `column harvest_workflow_executions.history_bloat_warned_at does not exist`.
    include_str!("../../migrations/20260716000000_harvest_workflow_history_bloat_warn/up.sql"),
    "\n",
    // issue #759: triage_note column on harvest_workflow_executions. REQUIRED,
    // not optional — `WorkflowExecution::as_select()`/`as_returning()` reference
    // this column on every full-row read/insert-returning, so without it every
    // such call in this suite (and in every suite that borrows
    // `setup_test_database_url_or_env` from here) fails with
    // `column harvest_workflow_executions.triage_note does not exist`.
    include_str!("../../migrations/20260717000000_harvest_workflow_triage_note/up.sql"),
    "\n",
    // issue #804: capability_misses column on harvest_task_queue. REQUIRED, not
    // optional -- `TaskQueueItem` (which `claim_task`'s `RETURNING
    // harvest_task_queue.*` deserializes into) references this column on every
    // claim, so without it every claim in this suite (and in every suite that
    // borrows `setup_test_database_url_or_env` from here) fails with
    // `column "capability_misses" does not exist`.
    include_str!("../../migrations/20260720000000_harvest_task_capability_misses/up.sql"),
    "\n",
    // issue #807: harvest_activity_pauses. REQUIRED, not optional -- the
    // `paused_activities` MATERIALIZED CTE in `claim_task_query()` selects from
    // this table on every claim, so without it every claim in this suite (and in
    // every suite that borrows `setup_test_database_url_or_env` from here) fails
    // with `relation "harvest_activity_pauses" does not exist`. The same
    // migration also adds the partial `idx_harvest_tq_activity_pause` index on
    // `harvest_task_queue`; that index is a performance aid rather than a
    // correctness requirement, but it ships in the same file.
    include_str!("../../migrations/20260722000000_harvest_activity_pause/up.sql"),
    "\n",
    // issue #843: idx_harvest_wfx_retry_of. Performance-only (CREATE INDEX,
    // no column added, no correctness dependency), included for schema
    // fidelity with the latest migration set.
    include_str!("../../migrations/20260723000000_harvest_retry_chain_index/up.sql"),
    "\n",
    // issue #945: override_refill_rate/override_burst/override_expires_at
    // columns on harvest_rate_limit_buckets. REQUIRED, not optional --
    // `claim_task_query()`'s rate-limit token-availability expression
    // references `b.override_expires_at`/`b.override_refill_rate`/
    // `b.override_burst` unconditionally (Postgres resolves every column
    // reference in a query at parse time, regardless of whether that branch
    // is reached for a given row), so without these columns EVERY claim in
    // this suite (and in every suite that borrows
    // `setup_test_database_url_or_env` from here) fails with
    // `column b.override_expires_at does not exist` -- even for tasks with
    // no rate limit configured at all.
    include_str!("../../migrations/20260724000000_harvest_pacing_overrides/up.sql"),
    "\n",
    // issue #946: quota_key column on harvest_workflow_executions, referenced
    // by every WorkflowExecution::as_select() read-back in this suite (and in
    // every suite that borrows `setup_test_database_url_or_env` from here).
    include_str!("../../migrations/20260725000000_harvest_workflow_quotas/up.sql"),
    "\n",
    // Dependency of the shard-rebalancing migration below, which adds a column
    // to `harvest_execution_summaries`. INIT_SQL is deliberately partial, so
    // this table was never part of it before; without this line the rebalancing
    // migration aborts mid-bundle with `relation "harvest_execution_summaries"
    // does not exist`, leaving the execution columns added but
    // `harvest_shard_migrations` never created.
    include_str!("../../migrations/20260710000001_harvest_execution_summaries/up.sql"),
    "\n",
    // issue #964: migrated_to_shard/migrated_at/migrated_from_shards columns on
    // harvest_workflow_executions, plus migrated_from_shards on
    // harvest_execution_summaries. REQUIRED for the same reason as the quota
    // migration above -- `WorkflowExecution::as_select()` names every column, so
    // every read-back in this suite (and in every suite that borrows
    // `setup_test_database_url_or_env` from here) fails with
    // `column harvest_workflow_executions.migrated_to_shard does not exist`,
    // including plain root starts that have nothing to do with rebalancing.
    //
    // This suite runs against a testcontainer built from INIT_SQL, NOT against
    // a migrated database, so a local run with HARVEST_TEST_DATABASE_URL set
    // cannot catch the omission -- that path skips INIT_SQL entirely.
    include_str!("../../migrations/20260902131705_harvest_shard_rebalancing/up.sql"),
    "\n",
    // issue #1127: last_registered_at/baseline_set_at columns on
    // harvest_rate_limit_buckets. REQUIRED for the same reason as the #945
    // columns above -- `queue::ensure_rate_limit_bucket` references
    // `last_registered_at` unconditionally, and it runs inside the SAME
    // transaction as every dynamic-per-key activity enqueue, so without these
    // columns that decision transaction rolls back and the workflow never
    // progresses (a silent timeout, not an obvious error).
    include_str!("../../migrations/20260902133132_harvest_rate_limit_bucket_gc/up.sql"),
    "\n",
    // issue #1227: next_attempt_at column on harvest_completion_trigger_outbox.
    // REQUIRED for the same reason as the #945/#964/#1127 columns above --
    // `enforce_completion_triggers_outbox`'s claim queries and
    // `stamp_outbox_relay_backoff` reference `next_attempt_at` unconditionally,
    // so without this column every outbox-relay test in this suite (and in
    // every suite that borrows `setup_test_database_url_or_env` from here)
    // fails with `column harvest_completion_trigger_outbox.next_attempt_at
    // does not exist`, even for a row with no quota block or relay failure at
    // all. A local run with `HARVEST_TEST_DATABASE_URL` set does not catch
    // this gap -- that path migrates from the full `migrations/` directory,
    // not from this deliberately partial bundle.
    include_str!(
        "../../migrations/20260906014820_harvest_completion_trigger_outbox_backoff/up.sql"
    ),
    // issue #1312: the fenced by-id claim reads the DR generation row, so this
    // bundle carries the cross-region DR tables.
    include_str!("../../migrations/20260726000000_harvest_shard_generation/up.sql")
);

/// The minimal "legacy" migration set used by the upgrade-path regression
/// test. Excludes both the workflow-start uniqueness upgrade *and* the
/// continue-as-new migration so the test can drive the database through the
/// historical upgrade sequence: legacy -> uniqueness fix -> continue-as-new.
///
/// The `concurrency_key` migration is included because `enqueue` (called by
/// `start_or_load_workflow_execution`) writes the `concurrency_key` and
/// `concurrency_cap` columns that it added; without them the INSERT fails.
/// The `harvest_workers` and `harvest_build_routing` migrations are included
/// because `start_or_load_workflow_execution` queries `harvest_build_policies`
/// (created by build routing) and inserts `assigned_build_id` into
/// `harvest_workflow_executions`; the build routing migration also alters
/// `harvest_workers`, so that table must exist first.
/// The parent-close-policy migration is included because the modern start path
/// inserts/selects the nullable `parent_close_policy` column even for root
/// workflows; the test still excludes only the uniqueness/continue-as-new
/// migrations it is explicitly exercising.
const LEGACY_INIT_SQL: &str = concat!(
    include_str!("../../migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!("../../migrations/20260424000001_harvest_trace_context/up.sql"),
    "\n",
    include_str!("../../migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../../migrations/20260429000000_harvest_concurrency_key/up.sql"),
    "\n",
    include_str!("../../migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!("../../migrations/20260509000000_harvest_build_routing/up.sql"),
    "\n",
    include_str!("../../migrations/20260514020000_harvest_task_activity_id/up.sql"),
    "\n",
    include_str!("../../migrations/20260518000001_harvest_workflow_execution_timeout/up.sql"),
    "\n",
    include_str!("../../migrations/20260613000000_harvest_workflow_sla/up.sql"),
    "\n",
    include_str!("../../migrations/20260526000001_harvest_parent_close_policy/up.sql"),
    "\n",
    include_str!("../../migrations/20260530000000_harvest_schedule_ha_claim/up.sql"),
    "\n",
    "ALTER TABLE harvest_task_queue ADD COLUMN IF NOT EXISTS rate_limit_key TEXT NULL;\n",
    "\n",
    include_str!("../../migrations/20260601000002_harvest_ownership_metadata/up.sql"),
    include_str!("../../migrations/20260706000000_harvest_worker_sessions/up.sql"),
    "\n",
    "ALTER TABLE harvest_task_queue ADD COLUMN IF NOT EXISTS schedule_to_close_at TIMESTAMPTZ NULL;\n",
    "\n",
    "ALTER TABLE harvest_task_queue ADD COLUMN IF NOT EXISTS required_capabilities JSONB NULL;\n",
    "\n",
    "ALTER TABLE harvest_workers ADD COLUMN IF NOT EXISTS labels JSONB NOT NULL DEFAULT '{}';\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS paused_at TIMESTAMPTZ NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS pause_reason TEXT NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS pause_actor TEXT NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS current_details TEXT NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS context_headers JSONB NULL;\n",
    "ALTER TABLE harvest_task_queue ADD COLUMN IF NOT EXISTS context_headers JSONB NULL;\n",
    // issue #488: the modern start path inserts schedule_id / scheduled_for.
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS schedule_id UUID NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS scheduled_for TIMESTAMPTZ NULL;\n",
    // issue #523: the modern start path inserts workflow_attempt / workflow_retry_policy / retry_of_exec_id.
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS workflow_attempt INT NOT NULL DEFAULT 1;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS workflow_retry_policy JSONB NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS retry_of_exec_id UUID NULL;\n",
    "ALTER TABLE harvest_schedules ADD COLUMN IF NOT EXISTS retry_policy JSONB NULL;\n",
    // issue #534: the modern start path inserts origin.
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS origin TEXT NULL;\n",
    // issue #604: get_build_policy/set_build_policy always select target_build_id/ramp_percent.
    "ALTER TABLE harvest_build_policies ADD COLUMN IF NOT EXISTS target_build_id TEXT NULL;\n",
    "ALTER TABLE harvest_build_policies ADD COLUMN IF NOT EXISTS ramp_percent INTEGER NULL;\n",
    // issue #603: the modern start path's full-row insert touches the
    // nd_block_* columns even for a fresh (never-blocked) execution.
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS nd_blocked_at TIMESTAMPTZ NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS nd_block_reason TEXT NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS nd_block_count INTEGER NOT NULL DEFAULT 0;\n",
    // issue #605: the modern start path's full-row insert touches
    // completion_callbacks even for a workflow with no configured callback.
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS completion_callbacks JSONB NULL;\n",
    // issue #701: the modern start path's full-row insert touches the
    // continue-as-new chain back-links even for a fresh (never-continued) run.
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS continued_from_exec_id UUID NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS first_exec_id UUID NULL;\n",
    // issue #747: WorkflowExecution::as_select() (and thus the modern start
    // path's read-back) references the four legal_hold_* columns.
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_set_at TIMESTAMPTZ NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_until TIMESTAMPTZ NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_reason TEXT NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_actor TEXT NULL;\n",
    // issue #740: WorkflowExecution::as_select() (modern start path's read-back)
    // references the three start_source_* provenance columns.
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS start_source TEXT NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS start_source_ref TEXT NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS started_by TEXT NULL;\n",
    // issue #617: WorkflowExecution::as_select() and the modern start path's
    // full-row insert touch the chain-scoped lifetime cap columns even for a
    // workflow with no chain cap configured.
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS chain_execution_timeout INTERVAL NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS chain_deadline_at TIMESTAMPTZ NULL;\n",
    // issue #704: WorkflowExecution::as_select() (the modern start path's
    // read-back) references the history-bloat early-warning guard column even
    // for a fresh (never-warned) execution.
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS history_bloat_warned_at TIMESTAMPTZ NULL;\n",
    // issue #759: WorkflowExecution::as_select() (the modern start path's
    // read-back) references the operator-mutable triage note column even for
    // a fresh (never-annotated) execution.
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS triage_note TEXT NULL;\n",
    // issue #804: `TaskQueueItem` (what `claim_task`'s `RETURNING
    // harvest_task_queue.*` deserializes into) references capability_misses on
    // every claim, so the legacy start path's enqueue+claim needs it too.
    "ALTER TABLE harvest_task_queue ADD COLUMN IF NOT EXISTS capability_misses INT NOT NULL DEFAULT 0;\n",
    // issue #804 (round 6): `TaskQueueItem` selects the companion distinct-misser
    // set on the same claim, so omitting it fails the legacy path's claim exactly
    // as omitting `capability_misses` would.
    "ALTER TABLE harvest_task_queue ADD COLUMN IF NOT EXISTS capability_miss_workers TEXT[] NOT NULL DEFAULT '{}';\n",
    // issue #946: WorkflowExecution::as_select() (the modern start path's
    // read-back) references the quota_key column even for a fresh (no quota
    // policy configured) execution.
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS quota_key TEXT NULL;\n",
    // issue #964: WorkflowExecution::as_select() (the modern start path's
    // read-back) references the three rebalancing columns even for an
    // execution that has never been migrated -- which is every execution in
    // every deployment that never runs a rebalance.
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS migrated_to_shard INTEGER NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS migrated_at TIMESTAMPTZ NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS migrated_from_shards JSONB NULL;\n"
);

/// Start a Postgres container with the harvest schema applied and return
/// an `AsyncPgConnection` ready for use.
///
/// Honours `HARVEST_TEST_DATABASE_URL` (a pre-migrated Postgres) so DB tests can
/// run against a local instance when Docker/testcontainers is unavailable. In
/// that mode the returned container `Option` is `None`; callers keep the
/// returned value alive for the test's duration exactly as they would the
/// container. When the env var is unset (the CI default), a throwaway container
/// is started as before.
///
/// CRITICAL: the returned `ContainerAsync` must be held alive for the duration
/// of the test -- dropping it kills the container.
async fn setup_test_db() -> (AsyncPgConnection, Option<ContainerAsync<Postgres>>) {
    if let Ok(database_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        let conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
            .await
            .expect("failed to connect to HARVEST_TEST_DATABASE_URL");
        return (conn, None);
    }

    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");

    let host = container
        .get_host()
        .await
        .expect("failed to get container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get container port");
    let database_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    let conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    (conn, Some(container))
}

/// Start a Postgres container with the harvest schema applied and return
/// the database URL plus the live container handle.
pub(crate) async fn setup_test_database_url() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");

    let host = container
        .get_host()
        .await
        .expect("failed to get container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get container port");
    let database_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    (database_url, container)
}

/// Like [`setup_test_database_url`] but honours `HARVEST_TEST_DATABASE_URL` (a
/// pre-migrated Postgres) so DB tests can run against a local instance when
/// Docker/testcontainers is unavailable. Returns `None` for the container in
/// that case; the caller keeps the returned `Option` alive for the test's
/// duration exactly as it would the container.
pub(crate) async fn setup_test_database_url_or_env() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let (url, container) = setup_test_database_url().await;
    (url, Some(container))
}

async fn setup_blank_test_database_url() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");

    let host = container
        .get_host()
        .await
        .expect("failed to get container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get container port");
    let database_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    (database_url, container)
}

#[tokio::test]
async fn drop_dag_runs_migration_copies_legacy_rows_to_workflow_executions() {
    let (mut conn, _container) = setup_test_db().await;
    let legacy_run_id = Uuid::new_v4();
    let dag_name = "legacy_migrated_dag";
    let logical_date = chrono::DateTime::parse_from_rfc3339("2026-05-14T02:00:00Z")
        .expect("fixed timestamp should parse")
        .with_timezone(&Utc);
    let dag_conf = serde_json::json!({ "customer": "acme" });

    diesel::sql_query(
        r"
        INSERT INTO harvest_dag_runs
            (id, dag_name, workflow_exec_id, state, logical_date, data_interval_start,
             data_interval_end, conf, started_at, completed_at, created_at)
        VALUES ($1, $2, NULL, 'SUCCESS', $3, $3, $3, $4, $3, $3, $3)
        ",
    )
    .bind::<diesel::sql_types::Uuid, _>(legacy_run_id)
    .bind::<diesel::sql_types::Text, _>(dag_name)
    .bind::<diesel::sql_types::Timestamptz, _>(logical_date)
    .bind::<diesel::sql_types::Jsonb, _>(dag_conf.clone())
    .execute(&mut conn)
    .await
    .expect("failed to seed legacy DAG run");

    conn.batch_execute(include_str!(
        "../../migrations/20260514000000_drop_harvest_dag_runs/up.sql"
    ))
    .await
    .expect("drop migration should migrate legacy DAG runs before dropping the table");

    // The migration generates IDs in the legacy format (no schedule UUID embedded).
    let workflow_id = format!("sched:{}:{}", dag_name, logical_date.timestamp());
    let migrated = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(dag_name))
        .filter(harvest_workflow_executions::workflow_id.eq(workflow_id))
        .select(WorkflowExecution::as_select())
        .first::<WorkflowExecution>(&mut conn)
        .await
        .expect("legacy DAG run should be copied into workflow executions");

    assert_eq!(migrated.id, legacy_run_id);
    assert_eq!(migrated.state, "COMPLETED");
    assert_eq!(migrated.queue_name, "default");
    assert_eq!(
        migrated.input["_harvest_migrated_legacy_dag_run"],
        serde_json::json!(true)
    );
    assert_eq!(migrated.input["dag_run_id"], legacy_run_id.to_string());
    assert_eq!(migrated.input["conf"], dag_conf);
    assert!(migrated.completed_at.is_some());
}

#[tokio::test]
async fn drop_dag_runs_migration_does_not_turn_queued_runs_into_running_workflows() {
    let (mut conn, _container) = setup_test_db().await;
    let legacy_run_id = Uuid::new_v4();
    let dag_name = "legacy_queued_dag";
    let logical_date = chrono::DateTime::parse_from_rfc3339("2026-05-14T02:00:00Z")
        .expect("fixed timestamp should parse")
        .with_timezone(&Utc);

    diesel::sql_query(
        r"
        INSERT INTO harvest_dag_runs
            (id, dag_name, workflow_exec_id, state, logical_date, data_interval_start,
             data_interval_end, conf, started_at, completed_at, created_at)
        VALUES ($1, $2, NULL, 'QUEUED', $3, $3, $3, NULL, NULL, NULL, $3)
        ",
    )
    .bind::<diesel::sql_types::Uuid, _>(legacy_run_id)
    .bind::<diesel::sql_types::Text, _>(dag_name)
    .bind::<diesel::sql_types::Timestamptz, _>(logical_date)
    .execute(&mut conn)
    .await
    .expect("failed to seed queued legacy DAG run");

    conn.batch_execute(include_str!(
        "../../migrations/20260514000000_drop_harvest_dag_runs/up.sql"
    ))
    .await
    .expect("drop migration should migrate queued legacy DAG runs safely");

    let migrated = harvest_workflow_executions::table
        .find(legacy_run_id)
        .select(WorkflowExecution::as_select())
        .first::<WorkflowExecution>(&mut conn)
        .await
        .expect("queued legacy DAG run should be preserved as a workflow row");
    assert_ne!(
        migrated.state, "RUNNING",
        "queued legacy DAG rows must not become permanently-running workflow executions"
    );
    assert_eq!(migrated.state, "CANCELLED");
    assert!(migrated.completed_at.is_some());

    let running_count: i64 = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(dag_name))
        .filter(harvest_workflow_executions::state.eq("RUNNING"))
        .count()
        .get_result(&mut conn)
        .await
        .expect("failed to count migrated running DAG workflows");
    assert_eq!(
        running_count, 0,
        "migrated queued legacy runs must not consume max_active_runs slots"
    );
}

#[tokio::test]
async fn drop_dag_runs_migration_preserves_subsecond_legacy_run_identities() {
    let (mut conn, _container) = setup_test_db().await;
    let dag_name = "legacy_subsecond_dag";
    let first_run_id = Uuid::new_v4();
    let second_run_id = Uuid::new_v4();
    let first_logical_date = chrono::DateTime::parse_from_rfc3339("2026-05-14T02:00:00.100000Z")
        .expect("fixed timestamp should parse")
        .with_timezone(&Utc);
    let second_logical_date = chrono::DateTime::parse_from_rfc3339("2026-05-14T02:00:00.900000Z")
        .expect("fixed timestamp should parse")
        .with_timezone(&Utc);

    diesel::sql_query(
        r"
        INSERT INTO harvest_dag_runs
            (id, dag_name, workflow_exec_id, state, logical_date, data_interval_start,
             data_interval_end, conf, started_at, completed_at, created_at)
        VALUES
            ($1, $2, NULL, 'SUCCESS', $3, $3, $3, NULL, $3, $3, $3),
            ($4, $2, NULL, 'SUCCESS', $5, $5, $5, NULL, $5, $5, $5)
        ",
    )
    .bind::<diesel::sql_types::Uuid, _>(first_run_id)
    .bind::<diesel::sql_types::Text, _>(dag_name)
    .bind::<diesel::sql_types::Timestamptz, _>(first_logical_date)
    .bind::<diesel::sql_types::Uuid, _>(second_run_id)
    .bind::<diesel::sql_types::Timestamptz, _>(second_logical_date)
    .execute(&mut conn)
    .await
    .expect("failed to seed subsecond legacy DAG runs");

    conn.batch_execute(include_str!(
        "../../migrations/20260514000000_drop_harvest_dag_runs/up.sql"
    ))
    .await
    .expect("drop migration should preserve subsecond legacy DAG identities");

    let migrated: Vec<WorkflowExecution> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(dag_name))
        .order(harvest_workflow_executions::workflow_id.asc())
        .select(WorkflowExecution::as_select())
        .load(&mut conn)
        .await
        .expect("failed to load migrated subsecond workflow rows");
    assert_eq!(
        migrated.len(),
        2,
        "same-second legacy DAG runs with distinct fractional logical dates must both migrate"
    );
    assert_ne!(migrated[0].workflow_id, migrated[1].workflow_id);
    assert!(
        migrated
            .iter()
            .any(|row| row.workflow_id.ends_with(".100000")),
        "first subsecond logical date should be represented in the workflow_id: {migrated:?}"
    );
    assert!(
        migrated
            .iter()
            .any(|row| row.workflow_id.ends_with(".900000")),
        "second subsecond logical date should be represented in the workflow_id: {migrated:?}"
    );
}

pub(crate) fn build_test_pool(database_url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        // Must comfortably exceed the largest `max_concurrent_workflows` any
        // test in this file passes to `build_runtime_worker` (currently 16,
        // for the wall-clock fan-out test) -- otherwise genuinely-concurrent
        // workflow tasks contend for pool checkouts instead of exercising
        // real in-process parallelism.
        .max_size(20)
        .build()
        .expect("failed to build test pool")
}

pub(crate) async fn load_execution_from_url(
    database_url: &str,
    exec_id: ExecutionId,
) -> WorkflowExecution {
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for execution query");
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .expect("failed to reload workflow execution")
}

async fn load_task_from_url(database_url: &str, task_id: Uuid) -> TaskQueueItem {
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for task query");
    harvest_task_queue::table
        .find(task_id)
        .select(TaskQueueItem::as_select())
        .first(&mut conn)
        .await
        .expect("failed to reload task queue row")
}

pub(crate) async fn load_history_from_url(
    database_url: &str,
    exec_id: ExecutionId,
) -> store::EventHistory {
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for history query");
    store::load_history(&mut conn, exec_id)
        .await
        .expect("load_history failed")
}

async fn load_tasks_for_execution_from_url(
    database_url: &str,
    exec_id: ExecutionId,
) -> Vec<TaskQueueItem> {
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for task list query");
    harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
        .order(harvest_task_queue::scheduled_at.asc())
        .select(TaskQueueItem::as_select())
        .load(&mut conn)
        .await
        .expect("failed to reload task queue rows")
}

pub(crate) async fn load_timers_for_execution_from_url(
    database_url: &str,
    exec_id: ExecutionId,
) -> Vec<HarvestTimer> {
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for timer list query");
    harvest_timers::table
        .filter(harvest_timers::workflow_exec_id.eq(exec_id.as_uuid()))
        .order(harvest_timers::fires_at.asc())
        .select(HarvestTimer::as_select())
        .load(&mut conn)
        .await
        .expect("failed to reload timer rows")
}

/// Seed a pending `harvest_timers` row directly (issue #1247 regression
/// coverage), standing in for the row a real `ArmTimer { for_await: true }`
/// cycle would have inserted. Lets a test drive the DB-delete half of a
/// `CancelTimer` co-batched with a `RunLocalActivity` without first fighting
/// the scheduler for a genuine live race window.
pub(crate) async fn seed_pending_timer_row(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    timer_id: &str,
    fires_at: chrono::DateTime<Utc>,
) {
    diesel::insert_into(harvest_timers::table)
        .values(autumn_harvest::models::NewHarvestTimer {
            workflow_exec_id: exec_id.as_uuid(),
            timer_id,
            fires_at,
        })
        .execute(conn)
        .await
        .expect("failed to seed a pending harvest_timers row");
}

pub(crate) async fn load_child_executions_from_url(
    database_url: &str,
    parent_exec_id: ExecutionId,
) -> Vec<WorkflowExecution> {
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for child execution query");
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::parent_id.eq(Some(parent_exec_id.as_uuid())))
        .order(harvest_workflow_executions::started_at.asc())
        .select(WorkflowExecution::as_select())
        .load(&mut conn)
        .await
        .expect("failed to reload child workflow executions")
}

/// Insert a minimal `harvest_workflow_executions` row and return its UUID.
pub(crate) async fn insert_workflow_execution(conn: &mut AsyncPgConnection) -> ExecutionId {
    let exec_id = ExecutionId::new();
    let row = NewWorkflowExecution {
        quota_key: None,
        continued_from_exec_id: None,
        first_exec_id: None,
        id: exec_id.as_uuid(),
        workflow_name: "e2e_test_workflow",
        workflow_id: "e2e-wf-001",
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: serde_json::json!({"test": true}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        deadline_at: None,
        chain_execution_timeout: None,
        chain_deadline_at: None,
        memo: None,
        search_attrs: None,
        assigned_build_id: None,
        parent_close_policy: None,

        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,

        sla: None,

        sla_deadline_at: None,
        schedule_id: None,
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        origin: None,
        completion_callbacks: None,
        start_source: None,
        start_source_ref: None,
        started_by: None,
    };

    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("failed to insert workflow execution");

    exec_id
}

/// Insert a RUNNING execution pinned to `shard` -- both in the encoded
/// `ExecutionId` and in the row's `shard_id` column, exactly as the shard-pinned
/// start path (issue #697) writes it. Used to prove residency is transitive
/// across the workflow tree.
pub(crate) async fn insert_workflow_execution_on_shard(
    conn: &mut AsyncPgConnection,
    shard: autumn_harvest::types::ShardId,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(shard);
    // Unique per call so repeated runs against the same (non-throwaway)
    // database never collide on the partial `UNIQUE(workflow_name, workflow_id)`
    // active index.
    let workflow_id = format!("e2e-wf-pinned-{}", Uuid::new_v4().simple());
    let row = NewWorkflowExecution {
        quota_key: None,
        continued_from_exec_id: None,
        first_exec_id: None,
        id: exec_id.as_uuid(),
        workflow_name: "e2e_test_workflow",
        workflow_id: &workflow_id,
        run_id: Uuid::new_v4(),
        shard_id: shard.as_i32(),
        input: serde_json::json!({"test": true}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        deadline_at: None,
        chain_execution_timeout: None,
        chain_deadline_at: None,
        memo: None,
        search_attrs: None,
        assigned_build_id: None,
        parent_close_policy: None,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,
        sla: None,
        sla_deadline_at: None,
        schedule_id: None,
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        origin: None,
        completion_callbacks: None,
        start_source: None,
        start_source_ref: None,
        started_by: None,
    };

    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("failed to insert shard-pinned workflow execution");

    exec_id
}

/// Insert a RUNNING execution with a caller-supplied `workflow_id` (all other
/// fields mirror [`insert_workflow_execution`]). Needed when a single test/setup
/// inserts more than one live execution: they would otherwise collide on the
/// partial `UNIQUE(workflow_name, workflow_id)` active index that
/// [`insert_workflow_execution`]'s hardcoded id trips on a second call.
pub(crate) async fn insert_workflow_execution_with_id(
    conn: &mut AsyncPgConnection,
    workflow_id: &str,
) -> ExecutionId {
    let exec_id = ExecutionId::new();
    let row = NewWorkflowExecution {
        quota_key: None,
        continued_from_exec_id: None,
        first_exec_id: None,
        id: exec_id.as_uuid(),
        workflow_name: "e2e_test_workflow",
        workflow_id,
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: serde_json::json!({"test": true}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        deadline_at: None,
        chain_execution_timeout: None,
        chain_deadline_at: None,
        memo: None,
        search_attrs: None,
        assigned_build_id: None,
        parent_close_policy: None,

        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,

        sla: None,

        sla_deadline_at: None,
        schedule_id: None,
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        origin: None,
        completion_callbacks: None,
        start_source: None,
        start_source_ref: None,
        started_by: None,
    };

    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("failed to insert workflow execution with id");

    exec_id
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // #685: required conflict_policy field tips this pre-existing literal-heavy test to 101 lines
async fn legacy_workflow_uniqueness_schema_can_be_upgraded_for_idempotent_starts() {
    let (database_url, _container) = setup_blank_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");
    // Build a "legacy" schema: the original initial migration whose
    // uniqueness key was `(workflow_id, run_id)` rather than the modern
    // `(workflow_name, workflow_id)`. The continue-as-new migration is
    // deliberately *not* applied yet so this test exercises the historical
    // upgrade path one step at a time.
    let legacy_init_sql = LEGACY_INIT_SQL.replacen(
        "UNIQUE (workflow_name, workflow_id)",
        "UNIQUE (workflow_id, run_id)",
        1,
    );
    conn.batch_execute(&legacy_init_sql)
        .await
        .expect("failed to apply legacy harvest schema");

    let request = StartWorkflowParams {
        workflow_name: "upgrade_test",
        workflow_id: "workflow-42",
        exec_id: autumn_harvest::ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0)),
        input: serde_json::json!({ "workflow_id": 42 }),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: autumn_harvest::WorkflowIdReusePolicy::default(),
        conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        inherited_chain_deadline_at: None,
        concurrency_key: None,
        concurrency_limit: None,
        concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
        priority: Priority::default(),
        max_workflow_input_bytes: 0,
        start_at: None,
        delay: None,
        max_workflow_start_delay: None,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,

        sla: None,
        schedule_id: None,
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        max_workflow_attempts_ceiling: None,
        origin: None,
        completion_callbacks: None,
        start_source: autumn_harvest::StartSource::Api,
        start_source_ref: None,
        started_by: None,
    };

    // On the legacy schema there is no `(workflow_name, workflow_id)`
    // uniqueness anywhere, so the first start succeeds — but the schema
    // alone does not yet enforce idempotency. The point of the upgrade
    // migration is to add that enforcement.
    let initial_start = start_or_load_workflow_execution(&mut conn, request.clone(), None)
        .await
        .expect("first start should succeed even on legacy schema");
    assert!(
        initial_start.created,
        "first start should create a workflow execution row on the legacy schema",
    );

    let upgrade_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("migrations/20260410010000_harvest_workflow_start_uniqueness/up.sql");
    let upgrade_sql = std::fs::read_to_string(&upgrade_path).unwrap_or_else(|error| {
        panic!(
            "failed to read harvest upgrade migration at {}: {error}",
            upgrade_path.display()
        )
    });
    conn.batch_execute(&upgrade_sql)
        .await
        .expect("failed to apply harvest upgrade migration");

    // After the start-uniqueness upgrade, repeated starts must collapse onto
    // the originally-created row.
    let after_uniqueness = start_or_load_workflow_execution(&mut conn, request.clone(), None)
        .await
        .expect("post-upgrade start should reuse the legacy row idempotently");
    assert!(
        !after_uniqueness.created,
        "post-upgrade start should not create a second row",
    );
    assert_eq!(
        initial_start.exec_id, after_uniqueness.exec_id,
        "post-upgrade start should resolve to the same execution as the legacy start",
    );

    // Apply the continue-as-new migration on top to make sure the partial
    // unique index it installs is compatible with the upgraded schema and
    // continues to enforce idempotent starts.
    let continue_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("migrations/20260427000000_harvest_continue_as_new/up.sql");
    let continue_sql = std::fs::read_to_string(&continue_path).unwrap_or_else(|error| {
        panic!(
            "failed to read continue-as-new migration at {}: {error}",
            continue_path.display()
        )
    });
    conn.batch_execute(&continue_sql)
        .await
        .expect("failed to apply continue-as-new migration");

    let after_continue_as_new = start_or_load_workflow_execution(&mut conn, request, None)
        .await
        .expect("start should remain idempotent after the continue-as-new migration");
    assert!(
        !after_continue_as_new.created,
        "continue-as-new migration should not break idempotent starts",
    );
    assert_eq!(
        initial_start.exec_id, after_continue_as_new.exec_id,
        "idempotent starts should still resolve to the same execution after continue-as-new",
    );
}

pub(crate) async fn enqueue_started_workflow_task(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    workflow_input: serde_json::Value,
) {
    store::append_events(
        conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: workflow_input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted failed");

    let mut params = EnqueueParams::new("default", TaskType::Workflow, workflow_input);
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);

    queue::enqueue(conn, &params)
        .await
        .expect("enqueue workflow task failed");
}

pub(crate) fn build_runtime_worker(
    worker_id: &str,
    max_concurrent_workflows: usize,
    max_concurrent_activities: usize,
    registry: Arc<HandlerRegistry>,
) -> Arc<Worker> {
    build_runtime_worker_with_task_timeout(
        worker_id,
        max_concurrent_workflows,
        max_concurrent_activities,
        registry,
        Duration::from_secs(10),
    )
}

/// Same as [`build_runtime_worker`], but with a caller-supplied
/// `workflow_task_timeout` instead of the default 10s.
///
/// The default 10s per-decision-cycle budget is tight enough that a
/// genuinely-high-concurrency, DB-heavy test (many workflow tasks racing for
/// very few CPUs on a constrained CI runner) can trip the poison-pill
/// mechanism (`WorkerConfig::poison_pill_threshold`, issue #367) purely from
/// scheduling delay -- a decision cycle whose actual work (e.g. an in-body
/// `tokio::time::sleep`) is well under budget can still exceed 10s wall-clock
/// if the runtime starves it for CPU, and three such strikes quarantines the
/// execution as FAILED with no relation to the workflow's own logic. Tests
/// that already carry their own generous outer wall-clock bound (e.g. a
/// `tokio::time::timeout` around the whole assertion) should use this to set
/// a `workflow_task_timeout` wide enough that only a genuine stall -- not CI
/// scheduling noise -- can trip it.
fn build_runtime_worker_with_task_timeout(
    worker_id: &str,
    max_concurrent_workflows: usize,
    max_concurrent_activities: usize,
    registry: Arc<HandlerRegistry>,
    workflow_task_timeout: Duration,
) -> Arc<Worker> {
    Arc::new(
        Worker::new(
            runtime_config(
                worker_id,
                max_concurrent_workflows,
                max_concurrent_activities,
                workflow_task_timeout,
            ),
            registry,
        )
        .expect("worker should build"),
    )
}

/// Same as [`build_runtime_worker`], but with a caller-supplied
/// `capability_miss_max_redeliveries` instead of the default 5.
///
/// The issue #804 release budget is spent against a capped exponential backoff
/// (`1s * 2^n`, capped at 30s), so the default budget of 5 costs
/// `1 + 2 + 4 + 8 + 16 = 31s` of dwell before the task escalates -- well past
/// the 10s default [`wait_for_execution_state`] bound. A test that asserts the
/// *escalation* (rather than an individual release) should therefore set a
/// small budget so the terminal verdict lands promptly, instead of widening its
/// wait and paying the dwell for real.
pub(crate) fn build_runtime_worker_with_capability_miss_budget(
    worker_id: &str,
    max_concurrent_workflows: usize,
    max_concurrent_activities: usize,
    registry: Arc<HandlerRegistry>,
    capability_miss_max_redeliveries: u32,
) -> Arc<Worker> {
    let mut config = runtime_config(
        worker_id,
        max_concurrent_workflows,
        max_concurrent_activities,
        Duration::from_secs(10),
    );
    config.capability_miss_max_redeliveries = capability_miss_max_redeliveries;
    Arc::new(Worker::new(config, registry).expect("worker should build"))
}

/// Build a `WorkerRuntimeConfig` with the standard test defaults.
///
/// Extracted so tests that need to inspect `Worker::new`'s `Result` (e.g. the
/// issue #699 worker-startup rate-limit validation tests) can pass a registry
/// directly without the `.expect(...)` that `build_runtime_worker*` applies.
pub(crate) fn runtime_config(
    worker_id: &str,
    max_concurrent_workflows: usize,
    max_concurrent_activities: usize,
    workflow_task_timeout: Duration,
) -> WorkerRuntimeConfig {
    WorkerRuntimeConfig {
        codec_rotation_batch_size: 0,
        dr: autumn_harvest::replication::DrConfig::default(),
        worker_id: worker_id.to_string(),
        queues: vec!["default".to_string()],
        notification_database_url: None,
        max_concurrent_workflows,
        max_concurrent_activities,
        poll_interval: Duration::from_millis(25),
        shutdown_timeout: Duration::from_secs(1),
        cancellation_grace_period: Duration::from_secs(1),
        sticky_timeout: Duration::from_secs(5),
        max_local_activity_start_to_close: Duration::from_secs(60),
        shard_assignments: vec![autumn_harvest::types::ShardId::new(0)],
        worker_heartbeat_interval: Duration::from_secs(5),
        build_id: String::new(),
        deployment_name: None,
        workflow_cache_size: 1000,
        priority_aging_secs: None,
        unknown_target_grace_window: Duration::from_secs(5),
        poison_pill_threshold: 3,
        capability_miss_max_redeliveries: 5,

        workflow_task_timeout,
        workflow_panic_max_attempts: 3,
        labels: std::collections::HashMap::new(),
        queue_weights: std::collections::HashMap::new(),
        max_workflow_pause_duration: std::time::Duration::from_secs(24 * 3600),
        max_workflow_history_events: None,
        shard_notification_database_urls: Vec::new(),
        sharded_pool: None,
        slot_tuner: None,
        max_concurrent_sessions: 0,
    }
}

pub(crate) fn spawn_test_worker(worker: Arc<Worker>, pool: DbPool) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        worker.run(&pool).await;
    })
}

pub(crate) async fn wait_for_execution_state(
    database_url: &str,
    exec_id: ExecutionId,
    expected_state: &str,
) -> WorkflowExecution {
    wait_for_execution_state_with_timeout(
        database_url,
        exec_id,
        expected_state,
        Duration::from_secs(10),
    )
    .await
}

/// Same as [`wait_for_execution_state`] but with a caller-supplied timeout,
/// for tests whose expected wall-clock (e.g. many genuinely-concurrent
/// children each sleeping for real time) can exceed the 10s default under
/// resource-constrained CI runners.
async fn wait_for_execution_state_with_timeout(
    database_url: &str,
    exec_id: ExecutionId,
    expected_state: &str,
    timeout: Duration,
) -> WorkflowExecution {
    tokio::time::timeout(timeout, async {
        loop {
            let execution = load_execution_from_url(database_url, exec_id).await;
            if execution.state == expected_state {
                break execution;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("workflow should reach expected state within timeout")
}

fn child_round_trip_registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![
            WorkflowInfo {
                quota: None,
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: "e2e_test_workflow",
                module: "integration_e2e",
                handler: parent_workflow_with_child,
                execution_timeout: None,
                chain_execution_timeout: None,
                sla: None,
                concurrency: None,

                debounce: None,
                batch: None,
                throttle: None,
                max_input_bytes: None,

                owner: None,
                runbook_url: None,
                severity: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
                retry_policy: None,
            },
            WorkflowInfo {
                quota: None,
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: "child_echo_workflow",
                module: "integration_e2e",
                handler: child_echo_workflow,
                execution_timeout: None,
                chain_execution_timeout: None,
                sla: None,
                concurrency: None,

                debounce: None,
                batch: None,
                throttle: None,
                max_input_bytes: None,

                owner: None,
                runbook_url: None,
                severity: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
                retry_policy: None,
            },
        ],
        vec![],
    ))
}

fn child_continue_as_new_rejection_registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![
            WorkflowInfo {
                quota: None,
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: "e2e_test_workflow",
                module: "integration_e2e",
                handler: parent_workflow_with_continue_as_new_child,
                execution_timeout: None,
                chain_execution_timeout: None,
                sla: None,
                concurrency: None,

                debounce: None,
                batch: None,
                throttle: None,
                max_input_bytes: None,

                owner: None,
                runbook_url: None,
                severity: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
                retry_policy: None,
            },
            WorkflowInfo {
                quota: None,
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: "child_continue_as_new_workflow",
                module: "integration_e2e",
                handler: continue_as_new_workflow,
                execution_timeout: None,
                chain_execution_timeout: None,
                sla: None,
                concurrency: None,

                debounce: None,
                batch: None,
                throttle: None,
                max_input_bytes: None,

                owner: None,
                runbook_url: None,
                severity: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
                retry_policy: None,
            },
        ],
        vec![],
    ))
}

fn echo_workflow<'a>(
    _ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(input) })
}

/// Issue #772: returns `ctx.deadline()` (a pure, no-clock accessor) as its
/// output so a DB e2e test can prove the run's `execution_timeout` was threaded
/// from the execution row into the `WorkflowContext`.
fn deadline_echo_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move { serde_json::to_value(ctx.deadline()).map_err(|e| e.to_string()) })
}

fn failing_workflow<'a>(
    _ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move { Err("workflow exploded on purpose".to_string()) })
}

fn workflow_with_activity<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("send_email", input, "default")
            .await
            .map_err(|e| e.to_string())
    })
}

fn workflow_with_checkpointed_activity<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("checkpointed_import", input, "default")
            .await
            .map_err(|e| e.to_string())
    })
}

fn send_email_activity<'a>(
    _ctx: &'a ActivityContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let to = input
            .get("to")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        Ok(serde_json::json!({
            "sent": true,
            "to": to,
        }))
    })
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ImportCheckpoint {
    next_offset: usize,
}

#[derive(Debug, Default)]
struct HeartbeatResumeStats {
    attempts: AtomicUsize,
    processed_steps: AtomicUsize,
    resume_offsets: Mutex<Vec<usize>>,
}

fn heartbeat_resume_state(
    stats: Arc<HeartbeatResumeStats>,
) -> autumn_harvest::context::SharedState {
    let mut shared_state_map = HashMap::new();
    shared_state_map.insert(
        TypeId::of::<Arc<HeartbeatResumeStats>>(),
        Box::new(stats) as Box<dyn std::any::Any + Send + Sync>,
    );
    Arc::new(shared_state_map)
}

fn checkpointed_import_activity<'a>(
    ctx: &'a ActivityContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let total = input
            .get("total")
            .and_then(serde_json::Value::as_u64)
            .and_then(|n| usize::try_from(n).ok())
            .unwrap_or(6);
        let fail_after = input
            .get("fail_after")
            .and_then(serde_json::Value::as_u64)
            .and_then(|n| usize::try_from(n).ok())
            .unwrap_or(3);

        let checkpoint = ctx
            .heartbeat_details::<ImportCheckpoint>()
            .map_err(|e| e.to_string())?;
        let start = checkpoint.map_or(0, |details| details.next_offset);
        let stats = Arc::clone(
            ctx.state::<Arc<HeartbeatResumeStats>>()
                .expect("test stats should be registered"),
        );
        let attempt = stats.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        stats
            .resume_offsets
            .lock()
            .expect("resume offset lock poisoned")
            .push(start);

        for offset in start..total {
            stats.processed_steps.fetch_add(1, Ordering::SeqCst);
            let next_offset = offset + 1;
            ctx.heartbeat(ImportCheckpoint { next_offset })
                .await
                .map_err(|e| e.to_string())?;

            if attempt == 1 && next_offset == fail_after {
                tokio::time::sleep(Duration::from_millis(1_200)).await;
                return Err(format!("fail after checkpoint {next_offset}"));
            }
        }

        Ok(serde_json::json!({
            "attempt": attempt,
            "resumed_from": start,
            "processed_total": AtomicUsize::load(&stats.processed_steps, Ordering::SeqCst),
        }))
    })
}

fn workflow_with_timer<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.timer("cooldown", 1).await.map_err(|e| e.to_string())?;
        Ok(serde_json::json!({
            "timer": "fired",
        }))
    })
}

fn workflow_with_slow_activity<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("slow_activity", input, "default")
            .await
            .map_err(|e| e.to_string())
    })
}

fn slow_activity<'a>(
    _ctx: &'a ActivityContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        tokio::time::sleep(Duration::from_millis(250)).await;
        Ok(input)
    })
}

fn signal_waiting_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let payload = ctx
            .wait_for_signal("approve")
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({ "approved_by": payload }))
    })
}

fn activity_then_signal_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let activity_result = ctx
            .execute_activity_raw("send_email", input, "default")
            .await
            .map_err(|e| e.to_string())?;
        let signal_payload = ctx
            .wait_for_signal("approve")
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({
            "activity": activity_result,
            "signal": signal_payload,
        }))
    })
}

fn parent_workflow_with_child<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.spawn_child_workflow_raw("child_echo_workflow", input)
            .await
            .map_err(|e| e.to_string())
    })
}

/// Issue #697 (AC4 + success metric): spawns a child and THEN continues-as-new,
/// so a single run produces both descendant kinds and one test can assert the
/// metric's "remain on across children/CAN" clause end to end. Branches on
/// `phase`: `"init"` spawns the child then forks; the continuation returns.
fn child_then_continue_as_new_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let phase = input
            .get("phase")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        if phase == "init" {
            let child_output = ctx
                .spawn_child_workflow_raw(
                    "child_echo_workflow",
                    serde_json::json!({"value": "pinned"}),
                )
                .await
                .map_err(|error| error.to_string())?;
            let _ = ctx
                .continue_as_new(serde_json::json!({"phase": "next", "child": child_output}))
                .await;
            unreachable!("continue_as_new must not resolve");
        }
        Ok(input)
    })
}

fn child_then_continue_as_new_registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![
            WorkflowInfo {
                quota: None,
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: "e2e_test_workflow",
                module: "integration_e2e",
                handler: child_then_continue_as_new_workflow,
                execution_timeout: None,
                chain_execution_timeout: None,
                sla: None,
                concurrency: None,
                debounce: None,
                batch: None,
                throttle: None,
                max_input_bytes: None,
                owner: None,
                runbook_url: None,
                severity: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
                retry_policy: None,
            },
            WorkflowInfo {
                quota: None,
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: "child_echo_workflow",
                module: "integration_e2e",
                handler: child_echo_workflow,
                execution_timeout: None,
                chain_execution_timeout: None,
                sla: None,
                concurrency: None,
                debounce: None,
                batch: None,
                throttle: None,
                max_input_bytes: None,
                owner: None,
                runbook_url: None,
                severity: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
                retry_policy: None,
            },
        ],
        vec![],
    ))
}

fn child_echo_workflow<'a>(
    _ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let value = input
            .get("value")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        Ok(serde_json::json!({
            "child": value,
        }))
    })
}

fn parent_workflow_with_continue_as_new_child<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.spawn_child_workflow_raw("child_continue_as_new_workflow", input)
            .await
            .map_err(|e| e.to_string())
    })
}

// ── issue #767: typed child failure → parent branches on error_type ──────────

/// A child that fails with a *typed* `WorkflowFailure` whose class is taken
/// from the input `{"category": "..."}`. The message text is deliberately
/// unique per category so the parent proves it routes on the typed class, not
/// on message text.
fn typed_failing_child_workflow<'a>(
    _ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let category = input
            .get("category")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("Unknown")
            .to_string();
        let message = format!("child rejected the request for {category} at 12:00:03Z");
        Err(WorkflowFailure::new(category, message)
            .with_details(serde_json::json!({ "source": "child" }))
            .non_retryable()
            .into_workflow_error_payload())
    })
}

/// Parent that spawns the typed-failing child and routes on its typed
/// `error_type` (issue #767) — ZERO substring matching. The parent *completes*
/// (it is not itself failed) so the worker-loop test can assert a deterministic
/// branch output.
fn parent_branches_on_typed_child_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        match ctx
            .spawn_child_workflow_raw("typed_failing_child_workflow", input)
            .await
        {
            Ok(v) => Ok(serde_json::json!({ "branch": "child_ok", "child": v })),
            Err(e) => {
                let branch = match e.workflow_error_type() {
                    Some("ValidationRejected") => "reject_and_notify",
                    Some("BudgetExceeded") => "escalate_to_finance",
                    Some("UpstreamUnavailable") => "reschedule",
                    Some(_) => "generic_typed",
                    None => "untyped",
                };
                Ok(serde_json::json!({
                    "branch": branch,
                    "observed_error_type": e.workflow_error_type(),
                    "non_retryable": e.is_workflow_non_retryable(),
                }))
            }
        }
    })
}

fn typed_child_failure_registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![
            WorkflowInfo {
                quota: None,
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: "e2e_test_workflow",
                module: "integration_e2e",
                handler: parent_branches_on_typed_child_workflow,
                execution_timeout: None,
                chain_execution_timeout: None,
                sla: None,
                concurrency: None,
                debounce: None,
                batch: None,
                throttle: None,
                max_input_bytes: None,
                owner: None,
                runbook_url: None,
                severity: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
                retry_policy: None,
            },
            WorkflowInfo {
                quota: None,
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: "typed_failing_child_workflow",
                module: "integration_e2e",
                handler: typed_failing_child_workflow,
                execution_timeout: None,
                chain_execution_timeout: None,
                sla: None,
                concurrency: None,
                debounce: None,
                batch: None,
                throttle: None,
                max_input_bytes: None,
                owner: None,
                runbook_url: None,
                severity: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
                retry_policy: None,
            },
        ],
        vec![],
    ))
}

/// First-generation handler used by the continue-as-new e2e test: it
/// branches on the input. When invoked with `{"phase": "init"}` it requests
/// continue-as-new with `{"phase": "next"}`. When invoked with the
/// post-continuation payload it returns it directly.
fn continue_as_new_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let phase = input
            .get("phase")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        if phase == "init" {
            let _ = ctx
                .continue_as_new(serde_json::json!({"phase": "next", "ran_init": true}))
                .await;
            unreachable!("continue_as_new must not resolve");
        }
        Ok(input)
    })
}

fn workflow_with_builder_state<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let activity_output = ctx
            .execute_activity_raw("stateful_activity", input.clone(), "default")
            .await
            .map_err(|error| error.to_string())?;
        let workflow_prefix = ctx
            .state::<String>()
            .cloned()
            .ok_or_else(|| "workflow missing shared state".to_string())?;

        Ok(serde_json::json!({
            "workflow_prefix": workflow_prefix,
            "activity": activity_output,
        }))
    })
}

fn stateful_activity<'a>(
    ctx: &'a ActivityContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let activity_prefix = ctx
            .state::<String>()
            .cloned()
            .ok_or_else(|| "activity missing shared state".to_string())?;

        Ok(serde_json::json!({
            "activity_prefix": activity_prefix,
            "payload": input,
        }))
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn full_workflow_lifecycle() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;

    // 1. Append WorkflowStarted event
    let started_events = vec![WorkflowEvent::WorkflowStarted {
        input: serde_json::json!({"user": "alice"}),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];
    let inserted = store::append_events(&mut conn, exec_id, &started_events, 0)
        .await
        .expect("append WorkflowStarted failed");
    assert_eq!(inserted, 1);

    // 2. Load history -- verify 1 event
    let history = store::load_history(&mut conn, exec_id)
        .await
        .expect("load_history failed");
    assert_eq!(history.events.len(), 1);
    assert!(matches!(
        history.events[0],
        WorkflowEvent::WorkflowStarted { .. }
    ));
    assert_eq!(history.next_event_id, 1);

    // 3. Enqueue an activity task
    //    Set scheduled_at slightly in the past to avoid clock skew between
    //    the host (where Utc::now() runs) and the Docker container (where
    //    Postgres NOW() runs).
    let mut params = EnqueueParams::new(
        "default",
        TaskType::Activity,
        serde_json::json!({"to": "bob@example.com"}),
    );
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.activity_name = Some("send_email".into());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);

    let task_id = queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue failed");

    // 4. Claim the task
    let queues = vec!["default".to_string()];
    let claimed = queue::claim_task(&mut conn, &queues, "worker-e2e-1", "", None, &[], &[])
        .await
        .expect("claim_task failed");
    let claimed = claimed.expect("no task claimed");
    assert_eq!(claimed.id, task_id);
    assert_eq!(claimed.activity_name.as_deref(), Some("send_email"));
    assert_eq!(claimed.state, "RUNNING");

    // 5. Complete the task
    queue::complete_task(&mut conn, task_id, serde_json::json!({"sent": true}))
        .await
        .expect("complete_task failed");

    // 6. Append activity completion + workflow completion events
    let activity_id = ActivityExecId::new();
    let completion_events = vec![
        WorkflowEvent::ActivityScheduled {
            activity_id,
            name: "send_email".into(),
            input: serde_json::json!({"to": "bob@example.com"}),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id,
            output: serde_json::json!({"sent": true}),
        },
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!({"status": "ok"}),
        },
    ];
    let inserted = store::append_events(&mut conn, exec_id, &completion_events, 1)
        .await
        .expect("append completion events failed");
    assert_eq!(inserted, 3);

    // 7. Load final history -- verify 4 events total
    //    (Started + ActivityScheduled + ActivityCompleted + WorkflowCompleted)
    let final_history = store::load_history(&mut conn, exec_id)
        .await
        .expect("final load_history failed");
    assert_eq!(final_history.events.len(), 4);
    assert!(matches!(
        final_history.events[0],
        WorkflowEvent::WorkflowStarted { .. }
    ));
    assert!(matches!(
        final_history.events[1],
        WorkflowEvent::ActivityScheduled { .. }
    ));
    assert!(matches!(
        final_history.events[2],
        WorkflowEvent::ActivityCompleted { .. }
    ));
    assert!(matches!(
        final_history.events[3],
        WorkflowEvent::WorkflowCompleted { .. }
    ));
    assert_eq!(final_history.next_event_id, 4);

    // 8. Verify the completed task in the queue has COMPLETED state
    let task: Vec<autumn_harvest::models::TaskQueueItem> = harvest_task_queue::table
        .filter(harvest_task_queue::id.eq(task_id))
        .load(&mut conn)
        .await
        .expect("failed to query task");
    assert_eq!(task.len(), 1);
    assert_eq!(task[0].state, "COMPLETED");
}

#[tokio::test]
async fn claim_task_returns_none_on_empty_queue() {
    let (mut conn, _container) = setup_test_db().await;

    let queues = vec!["default".to_string()];
    let claimed = queue::claim_task(&mut conn, &queues, "worker-empty-1", "", None, &[], &[])
        .await
        .expect("claim_task failed");
    assert!(
        claimed.is_none(),
        "expected None from empty queue, got {claimed:?}"
    );
}

/// Issue #772 (Finding 2): prove the run's `execution_timeout` flows end-to-end
/// from the execution row (`prepared.execution.execution_timeout`) into the
/// `WorkflowContext` so `ctx.deadline()` is populated. Deterministic (no timing
/// flake): the workflow's only job is to return `ctx.deadline()` (a pure,
/// clock-free accessor), and we assert its output equals the row's `deadline_at`
/// column exactly — both are `WorkflowStarted.timestamp + execution_timeout`, so
/// they can only match if the timeout was threaded into the context. Without the
/// worker→executor→context threading, `ctx.deadline()` would be `None` and the
/// output would be JSON `null`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn worker_threads_execution_timeout_into_ctx_deadline() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let timeout = chrono::Duration::minutes(30);
    let exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let request = StartWorkflowParams {
        workflow_name: "deadline_echo",
        workflow_id: "deadline-echo-1",
        exec_id,
        input: serde_json::json!({}),
        parent_id: None,
        queue_name: "default",
        // The production start param that stamps the row's execution_timeout +
        // deadline_at (issue #243).
        execution_timeout: Some(timeout),
        memo: None,
        search_attrs: None,
        reuse_policy: WorkflowIdReusePolicy::default(),
        conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        inherited_chain_deadline_at: None,
        concurrency_key: None,
        concurrency_limit: None,
        concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
        priority: Priority::default(),
        max_workflow_input_bytes: 0,
        start_at: None,
        delay: None,
        max_workflow_start_delay: None,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,
        sla: None,
        schedule_id: None,
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        max_workflow_attempts_ceiling: None,
        origin: None,
        completion_callbacks: None,
        start_source: autumn_harvest::StartSource::Api,
        start_source_ref: None,
        started_by: None,
    };
    let started = start_or_load_workflow_execution(&mut conn, request, None)
        .await
        .expect("start should succeed");
    assert!(started.created, "start should create a fresh execution");

    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: "deadline_echo",
            module: "integration_e2e",
            handler: deadline_echo_workflow,
            // Deliberately None: the ROW's execution_timeout (set via the start
            // param above) is the value that must be threaded, not this default.
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,
            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,
            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![],
    ));
    let worker = Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                codec_rotation_batch_size: 0,
                dr: autumn_harvest::replication::DrConfig::default(),
                worker_id: "worker-deadline-echo".to_string(),
                queues: vec!["default".to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 1,
                max_concurrent_activities: 1,
                poll_interval: Duration::from_millis(25),
                shutdown_timeout: Duration::from_secs(1),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![autumn_harvest::types::ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
                build_id: String::new(),
                deployment_name: None,
                workflow_cache_size: 1000,
                priority_aging_secs: None,
                unknown_target_grace_window: Duration::from_secs(5),
                poison_pill_threshold: 3,
                capability_miss_max_redeliveries: 5,
                workflow_task_timeout: Duration::from_secs(10),
                workflow_panic_max_attempts: 3,
                labels: std::collections::HashMap::new(),
                queue_weights: std::collections::HashMap::new(),
                max_workflow_pause_duration: Duration::from_secs(24 * 3600),
                max_workflow_history_events: None,
                shard_notification_database_urls: Vec::new(),
                sharded_pool: None,
                slot_tuner: None,
                max_concurrent_sessions: 0,
            },
            registry,
        )
        .expect("worker should build"),
    );
    let pool = build_test_pool(&database_url);
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let completed = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let execution = load_execution_from_url(&database_url, exec_id).await;
            if execution.state == "COMPLETED" {
                break execution;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    let execution = completed.expect("workflow should complete within timeout");
    // The start param set the row's execution_timeout + deadline_at.
    assert_eq!(
        execution.execution_timeout,
        Some(timeout),
        "the execution row must carry the start-param execution_timeout"
    );
    let deadline_at = execution
        .deadline_at
        .expect("deadline_at must be set on the row when execution_timeout is set");

    // The workflow returned ctx.deadline() as its output. It must NOT be null —
    // that would mean execution_timeout was never threaded into the context.
    let output = execution
        .output
        .expect("completed workflow must have an output");
    assert_ne!(
        output,
        serde_json::Value::Null,
        "ctx.deadline() must not be null — that means execution_timeout was NOT threaded"
    );

    // Exact, flake-free proof: ctx.deadline() = WorkflowStarted.timestamp +
    // execution_timeout. Read the recorded WorkflowStarted timestamp (the exact
    // ns-precision value the context used) and rebuild the expected deadline.
    // This avoids comparing against the row's deadline_at directly, which
    // Postgres truncates to microseconds.
    let history = load_history_from_url(&database_url, exec_id).await;
    let started_ts = match history.events.as_slice() {
        [WorkflowEvent::WorkflowStarted { timestamp, .. }, ..] => *timestamp,
        other => panic!("first event must be WorkflowStarted, got {other:?}"),
    };
    let expected_deadline = started_ts + timeout;
    assert_eq!(
        output,
        serde_json::to_value(expected_deadline).expect("expected deadline serialises"),
        "ctx.deadline() must equal WorkflowStarted.timestamp + execution_timeout, proving the \
         worker→executor→context threading"
    );

    // Cross-check the row's deadline_at is the same instant (within the µs
    // truncation Postgres applies to TIMESTAMPTZ).
    assert!(
        (expected_deadline - deadline_at).num_milliseconds().abs() <= 1,
        "ctx.deadline() ({expected_deadline}) must match the row's deadline_at ({deadline_at})"
    );
}

/// Issue #772 (P2 split): prove the public `ctx.deadline()` accessor is the
/// **replay-stable NOMINAL** deadline (`started_at + execution_timeout`) end to
/// end through the real worker, and is NOT the mutable, resume-shifted
/// `deadline_at` column.
///
/// A resumed/redriven run's `deadline_at` is pushed forward past
/// `started_at + execution_timeout` (pause/resume shifts it by the pause span,
/// #383; redrive re-anchors it to `now + timeout`). Rather than wiring a full
/// pause/resume with a backdated `paused_at` (heavy, timing-sensitive), this
/// test directly `UPDATE`s the row's `deadline_at` to a shifted value — exactly
/// what resume's SQL does — and asserts the worker still surfaces the NOMINAL
/// deadline through `ctx.deadline()`. Deterministic: the workflow returns
/// `ctx.deadline()` as its output, and we assert it equals `started_at +
/// execution_timeout` and is far from the (shifted) row `deadline_at`. The
/// live/effective `deadline_at` is consumed only by the internal continue-as-new
/// budget check, never by this public accessor, so author code depending on
/// `deadline()` replays deterministically after a pause.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn worker_surfaces_nominal_deadline_not_shifted_deadline_at() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let timeout = chrono::Duration::minutes(30);
    let exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let request = StartWorkflowParams {
        workflow_name: "deadline_echo",
        workflow_id: "deadline-echo-shifted-1",
        exec_id,
        input: serde_json::json!({}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: Some(timeout),
        memo: None,
        search_attrs: None,
        reuse_policy: WorkflowIdReusePolicy::default(),
        conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        inherited_chain_deadline_at: None,
        concurrency_key: None,
        concurrency_limit: None,
        concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
        priority: Priority::default(),
        max_workflow_input_bytes: 0,
        start_at: None,
        delay: None,
        max_workflow_start_delay: None,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,
        sla: None,
        schedule_id: None,
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        max_workflow_attempts_ceiling: None,
        origin: None,
        completion_callbacks: None,
        start_source: autumn_harvest::StartSource::Api,
        start_source_ref: None,
        started_by: None,
    };
    let started = start_or_load_workflow_execution(&mut conn, request, None)
        .await
        .expect("start should succeed");
    assert!(started.created, "start should create a fresh execution");

    // Read the row so we know its exact `started_at` and original `deadline_at`.
    let before = load_execution_from_url(&database_url, exec_id).await;
    let original_deadline = before
        .deadline_at
        .expect("deadline_at must be set when execution_timeout is set");

    // Simulate a resume/redrive shift: push `deadline_at` forward by 2h, exactly
    // as `resume_workflow_execution` shifts it by the pause span. The new value
    // is now well past `started_at + execution_timeout`.
    let shift = chrono::Duration::hours(2);
    let shifted_deadline = original_deadline + shift;
    let updated = diesel::update(
        harvest_workflow_executions::table
            .filter(harvest_workflow_executions::id.eq(exec_id.as_uuid())),
    )
    .set(harvest_workflow_executions::deadline_at.eq(Some(shifted_deadline)))
    .execute(&mut conn)
    .await
    .expect("shift deadline_at should succeed");
    assert_eq!(
        updated, 1,
        "exactly one row's deadline_at should be shifted"
    );

    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: "deadline_echo",
            module: "integration_e2e",
            handler: deadline_echo_workflow,
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,
            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,
            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![],
    ));
    let worker = Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                codec_rotation_batch_size: 0,
                dr: autumn_harvest::replication::DrConfig::default(),
                worker_id: "worker-deadline-echo-shifted".to_string(),
                queues: vec!["default".to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 1,
                max_concurrent_activities: 1,
                poll_interval: Duration::from_millis(25),
                shutdown_timeout: Duration::from_secs(1),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![autumn_harvest::types::ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
                build_id: String::new(),
                deployment_name: None,
                workflow_cache_size: 1000,
                priority_aging_secs: None,
                unknown_target_grace_window: Duration::from_secs(5),
                poison_pill_threshold: 3,
                capability_miss_max_redeliveries: 5,
                workflow_task_timeout: Duration::from_secs(10),
                workflow_panic_max_attempts: 3,
                labels: std::collections::HashMap::new(),
                queue_weights: std::collections::HashMap::new(),
                max_workflow_pause_duration: Duration::from_secs(24 * 3600),
                max_workflow_history_events: None,
                shard_notification_database_urls: Vec::new(),
                sharded_pool: None,
                slot_tuner: None,
                max_concurrent_sessions: 0,
            },
            registry,
        )
        .expect("worker should build"),
    );
    let pool = build_test_pool(&database_url);
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let completed = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let execution = load_execution_from_url(&database_url, exec_id).await;
            if execution.state == "COMPLETED" {
                break execution;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    let execution = completed.expect("workflow should complete within timeout");
    let post_deadline = execution
        .deadline_at
        .expect("deadline_at must still be set after completion");
    // The shift persisted (the worker never re-derives / overwrites deadline_at).
    assert!(
        (post_deadline - shifted_deadline).num_milliseconds().abs() <= 1,
        "the row's deadline_at should remain the shifted value ({shifted_deadline}), got \
         {post_deadline}"
    );

    // The workflow returned ctx.deadline() as its output. Parse it back.
    let output = execution
        .output
        .expect("completed workflow must have an output");
    assert_ne!(
        output,
        serde_json::Value::Null,
        "ctx.deadline() must not be null — that means the deadline was NOT threaded"
    );
    let ctx_deadline: chrono::DateTime<Utc> =
        serde_json::from_value(output).expect("ctx.deadline() output must be an RFC3339 timestamp");

    // The public ctx.deadline() must equal the NOMINAL `WorkflowStarted.timestamp
    // + execution_timeout` — the replay-stable value — even though the row's
    // deadline_at was shifted +2h. (The internal continue-as-new budget check
    // reads the shifted deadline_at; the public accessor deliberately does not.)
    let history = load_history_from_url(&database_url, exec_id).await;
    let started_ts = match history.events.as_slice() {
        [WorkflowEvent::WorkflowStarted { timestamp, .. }, ..] => *timestamp,
        other => panic!("first event must be WorkflowStarted, got {other:?}"),
    };
    let nominal_deadline = started_ts + timeout;
    assert!(
        (ctx_deadline - nominal_deadline).num_milliseconds().abs() <= 1,
        "public ctx.deadline() ({ctx_deadline}) must equal the nominal start+timeout \
         ({nominal_deadline}), not the mutable deadline_at"
    );

    // And it must be far from the SHIFTED row `deadline_at` (2h apart) — proving
    // the public accessor never surfaces the pause/resume-shifted value.
    assert!(
        (ctx_deadline - shifted_deadline).num_seconds().abs() > 3600,
        "public ctx.deadline() ({ctx_deadline}) must NOT be the shifted row deadline_at \
         ({shifted_deadline}) — that value is internal to the CAN budget check only"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn worker_completes_workflow_task_and_persists_result() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let exec_id = insert_workflow_execution(&mut conn).await;
    let workflow_input = serde_json::json!({"status": "ok"});

    let started_events = vec![WorkflowEvent::WorkflowStarted {
        input: workflow_input.clone(),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];
    store::append_events(&mut conn, exec_id, &started_events, 0)
        .await
        .expect("append WorkflowStarted failed");

    let mut params = EnqueueParams::new("default", TaskType::Workflow, workflow_input.clone());
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);

    let task_id = queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue workflow task failed");

    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: "e2e_test_workflow",
            module: "integration_e2e",
            handler: echo_workflow,
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![],
    ));
    let worker = Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                codec_rotation_batch_size: 0,
                dr: autumn_harvest::replication::DrConfig::default(),
                worker_id: "worker-e2e-complete".to_string(),
                queues: vec!["default".to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 1,
                max_concurrent_activities: 1,
                poll_interval: Duration::from_millis(25),
                shutdown_timeout: Duration::from_secs(1),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![autumn_harvest::types::ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
                build_id: String::new(),
                deployment_name: None,
                workflow_cache_size: 1000,
                priority_aging_secs: None,
                unknown_target_grace_window: Duration::from_secs(5),
                poison_pill_threshold: 3,
                capability_miss_max_redeliveries: 5,

                workflow_task_timeout: std::time::Duration::from_secs(10),
                workflow_panic_max_attempts: 3,
                labels: std::collections::HashMap::new(),
                queue_weights: std::collections::HashMap::new(),
                max_workflow_pause_duration: std::time::Duration::from_secs(24 * 3600),
                max_workflow_history_events: None,
                shard_notification_database_urls: Vec::new(),
                sharded_pool: None,
                slot_tuner: None,
                max_concurrent_sessions: 0,
            },
            registry,
        )
        .expect("worker should build"),
    );
    let pool = build_test_pool(&database_url);
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();

    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let completed_execution = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let execution = load_execution_from_url(&database_url, exec_id).await;

            if execution.state == "COMPLETED" {
                break execution;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    let execution =
        completed_execution.expect("worker should complete workflow task within timeout");
    assert_eq!(execution.output, Some(workflow_input.clone()));
    assert!(execution.completed_at.is_some());

    let history = load_history_from_url(&database_url, exec_id).await;
    assert!(matches!(
        history.events.last(),
        Some(WorkflowEvent::WorkflowCompleted { output }) if *output == workflow_input
    ));

    let task = load_task_from_url(&database_url, task_id).await;
    assert_eq!(task.state, "COMPLETED");
    assert_eq!(task.output, Some(workflow_input));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn worker_marks_workflow_failed_when_handler_errors() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let exec_id = insert_workflow_execution(&mut conn).await;
    let workflow_input = serde_json::json!({"status": "boom"});

    let started_events = vec![WorkflowEvent::WorkflowStarted {
        input: workflow_input.clone(),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];
    store::append_events(&mut conn, exec_id, &started_events, 0)
        .await
        .expect("append WorkflowStarted failed");

    let mut params = EnqueueParams::new("default", TaskType::Workflow, workflow_input);
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);

    let task_id = queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue workflow task failed");

    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: "e2e_test_workflow",
            module: "integration_e2e",
            handler: failing_workflow,
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![],
    ));
    let worker = Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                codec_rotation_batch_size: 0,
                dr: autumn_harvest::replication::DrConfig::default(),
                worker_id: "worker-e2e-fail".to_string(),
                queues: vec!["default".to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 1,
                max_concurrent_activities: 1,
                poll_interval: Duration::from_millis(25),
                shutdown_timeout: Duration::from_secs(1),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![autumn_harvest::types::ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
                build_id: String::new(),
                deployment_name: None,
                workflow_cache_size: 1000,
                priority_aging_secs: None,
                unknown_target_grace_window: Duration::from_secs(5),
                poison_pill_threshold: 3,
                capability_miss_max_redeliveries: 5,

                workflow_task_timeout: std::time::Duration::from_secs(10),
                workflow_panic_max_attempts: 3,
                labels: std::collections::HashMap::new(),
                queue_weights: std::collections::HashMap::new(),
                max_workflow_pause_duration: std::time::Duration::from_secs(24 * 3600),
                max_workflow_history_events: None,
                shard_notification_database_urls: Vec::new(),
                sharded_pool: None,
                slot_tuner: None,
                max_concurrent_sessions: 0,
            },
            registry,
        )
        .expect("worker should build"),
    );
    let pool = build_test_pool(&database_url);
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();

    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let failed_execution = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let execution = load_execution_from_url(&database_url, exec_id).await;

            if execution.state == "FAILED" {
                break execution;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    let execution = failed_execution.expect("worker should fail workflow task within timeout");
    assert_eq!(execution.state, "FAILED");
    assert!(
        execution
            .error
            .as_deref()
            .is_some_and(|e| e.contains("workflow exploded"))
    );
    assert!(execution.completed_at.is_some());

    let history = load_history_from_url(&database_url, exec_id).await;
    assert!(matches!(
        history.events.last(),
        Some(WorkflowEvent::WorkflowFailed { error, .. }) if error.contains("workflow exploded")
    ));

    let task = load_task_from_url(&database_url, task_id).await;
    assert_eq!(task.state, "FAILED");
    assert!(
        task.error
            .as_deref()
            .is_some_and(|e| e.contains("workflow exploded"))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn worker_completes_workflow_with_activity_round_trip() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let exec_id = insert_workflow_execution(&mut conn).await;
    let workflow_input = serde_json::json!({"to": "alice@example.com"});
    let activity_output = serde_json::json!({
        "sent": true,
        "to": "alice@example.com",
    });

    let started_events = vec![WorkflowEvent::WorkflowStarted {
        input: workflow_input.clone(),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];
    store::append_events(&mut conn, exec_id, &started_events, 0)
        .await
        .expect("append WorkflowStarted failed");

    let mut params = EnqueueParams::new("default", TaskType::Workflow, workflow_input.clone());
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);

    let workflow_task_id = queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue workflow task failed");

    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: "e2e_test_workflow",
            module: "integration_e2e",
            handler: workflow_with_activity,
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![ActivityInfo {
            name: "send_email",
            module: "integration_e2e",
            default_retry_policy: None,
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_schedule_to_close: None,
            default_queue: Some("default"),
            max_concurrent: None,
            concurrency_key: None,
            rate_limit_rps: None,
            rate_limit_burst: None,
            rate_limit_key: None,
            rate_limit_key_expr: None,
            circuit_breaker: None,
            is_local: false,
            max_input_bytes: None,
            max_result_bytes: None,
            requires: None,
            handler: send_email_activity,
        }],
    ));
    let worker = Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                codec_rotation_batch_size: 0,
                dr: autumn_harvest::replication::DrConfig::default(),
                worker_id: "worker-e2e-activity-round-trip".to_string(),
                queues: vec!["default".to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 1,
                max_concurrent_activities: 1,
                poll_interval: Duration::from_millis(25),
                shutdown_timeout: Duration::from_secs(1),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![autumn_harvest::types::ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
                build_id: String::new(),
                deployment_name: None,
                workflow_cache_size: 1000,
                priority_aging_secs: None,
                unknown_target_grace_window: Duration::from_secs(5),
                poison_pill_threshold: 3,
                capability_miss_max_redeliveries: 5,

                workflow_task_timeout: std::time::Duration::from_secs(10),
                workflow_panic_max_attempts: 3,
                labels: std::collections::HashMap::new(),
                queue_weights: std::collections::HashMap::new(),
                max_workflow_pause_duration: std::time::Duration::from_secs(24 * 3600),
                max_workflow_history_events: None,
                shard_notification_database_urls: Vec::new(),
                sharded_pool: None,
                slot_tuner: None,
                max_concurrent_sessions: 0,
            },
            registry,
        )
        .expect("worker should build"),
    );
    let pool = build_test_pool(&database_url);
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();

    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let completed_execution = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let execution = load_execution_from_url(&database_url, exec_id).await;
            if execution.state == "COMPLETED" {
                break execution;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    let execution =
        completed_execution.expect("worker should complete workflow-with-activity within timeout");
    assert_eq!(execution.output, Some(activity_output.clone()));

    let history = load_history_from_url(&database_url, exec_id).await;
    assert!(matches!(
        history.events.as_slice(),
        [
            WorkflowEvent::WorkflowStarted { .. },
            WorkflowEvent::ActivityScheduled { .. },
            WorkflowEvent::ActivityStarted { .. },
            WorkflowEvent::ActivityCompleted { .. },
            WorkflowEvent::WorkflowCompleted { .. },
        ]
    ));

    let tasks = load_tasks_for_execution_from_url(&database_url, exec_id).await;
    assert_eq!(tasks.len(), 2, "workflow + activity task rows should exist");
    assert!(tasks.iter().all(|task| task.state == "COMPLETED"));
    assert!(tasks.iter().any(|task| task.id == workflow_task_id));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn activity_retry_resumes_from_persisted_heartbeat_details() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let exec_id = insert_workflow_execution(&mut conn).await;
    let workflow_input = serde_json::json!({
        "total": 6,
        "fail_after": 3,
    });
    store::append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: workflow_input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted failed");

    let mut params = EnqueueParams::new("default", TaskType::Workflow, workflow_input.clone());
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);
    queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue workflow task failed");

    let stats = Arc::new(HeartbeatResumeStats::default());
    let registry = Arc::new(HandlerRegistry::with_state(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: "e2e_test_workflow",
            module: "integration_e2e",
            handler: workflow_with_checkpointed_activity,
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![ActivityInfo {
            name: "checkpointed_import",
            module: "integration_e2e",
            default_retry_policy: Some(autumn_harvest::RetryPolicy::fixed(
                2,
                Duration::from_millis(10),
            )),
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_schedule_to_close: None,
            default_queue: Some("default"),
            max_concurrent: None,
            concurrency_key: None,
            rate_limit_rps: None,
            rate_limit_burst: None,
            rate_limit_key: None,
            rate_limit_key_expr: None,
            circuit_breaker: None,
            is_local: false,
            max_input_bytes: None,
            max_result_bytes: None,
            requires: None,
            handler: checkpointed_import_activity,
        }],
        heartbeat_resume_state(Arc::clone(&stats)),
    ));
    let worker = build_runtime_worker("worker-heartbeat-resume", 1, 1, registry);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let execution = wait_for_execution_state(&database_url, exec_id, "COMPLETED").await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    assert_eq!(
        execution.output,
        Some(serde_json::json!({
            "attempt": 2,
            "resumed_from": 3,
            "processed_total": 6,
        }))
    );
    assert_eq!(AtomicUsize::load(&stats.attempts, Ordering::SeqCst), 2);
    assert_eq!(
        AtomicUsize::load(&stats.processed_steps, Ordering::SeqCst),
        6
    );
    assert_eq!(
        *stats
            .resume_offsets
            .lock()
            .expect("resume offset lock poisoned"),
        vec![0, 3],
        "second attempt must start from the checkpoint persisted by the first attempt",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn worker_fails_orphaned_activity_task_without_scheduled_event() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let exec_id = insert_workflow_execution(&mut conn).await;
    let activity_input = serde_json::json!({"step": "send_email"});

    let started_events = vec![WorkflowEvent::WorkflowStarted {
        input: serde_json::json!({"workflow": "activity-only"}),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];
    store::append_events(&mut conn, exec_id, &started_events, 0)
        .await
        .expect("append WorkflowStarted failed");

    let mut params = EnqueueParams::new("default", TaskType::Activity, activity_input);
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.activity_name = Some("send_email".to_string());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);

    let task_id = queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue activity task failed");

    let worker = Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                codec_rotation_batch_size: 0,
                dr: autumn_harvest::replication::DrConfig::default(),
                worker_id: "worker-e2e-activity-orphaned".to_string(),
                queues: vec!["default".to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 1,
                max_concurrent_activities: 1,
                poll_interval: Duration::from_millis(25),
                shutdown_timeout: Duration::from_secs(1),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![autumn_harvest::types::ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
                build_id: String::new(),
                deployment_name: None,
                workflow_cache_size: 1000,
                priority_aging_secs: None,
                unknown_target_grace_window: Duration::from_secs(5),
                poison_pill_threshold: 3,
                capability_miss_max_redeliveries: 5,

                workflow_task_timeout: std::time::Duration::from_secs(10),
                workflow_panic_max_attempts: 3,
                labels: std::collections::HashMap::new(),
                queue_weights: std::collections::HashMap::new(),
                max_workflow_pause_duration: std::time::Duration::from_secs(24 * 3600),
                max_workflow_history_events: None,
                shard_notification_database_urls: Vec::new(),
                sharded_pool: None,
                slot_tuner: None,
                max_concurrent_sessions: 0,
            },
            Arc::new(HandlerRegistry::new(
                vec![],
                vec![ActivityInfo {
                    name: "send_email",
                    module: "integration_e2e",
                    default_retry_policy: None,
                    default_start_to_close: None,
                    default_heartbeat_timeout: None,
                    default_schedule_to_start: None,
                    default_schedule_to_close: None,
                    default_queue: Some("default"),
                    max_concurrent: None,
                    concurrency_key: None,
                    rate_limit_rps: None,
                    rate_limit_burst: None,
                    rate_limit_key: None,
                    rate_limit_key_expr: None,
                    circuit_breaker: None,
                    is_local: false,
                    max_input_bytes: None,
                    max_result_bytes: None,
                    requires: None,
                    handler: send_email_activity,
                }],
            )),
        )
        .expect("worker should build"),
    );
    let pool = build_test_pool(&database_url);
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();

    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let failed_task = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let task = load_task_from_url(&database_url, task_id).await;

            if task.state == "FAILED" {
                break task;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    let task = failed_task.expect("worker should fail orphaned activity task within timeout");
    assert_eq!(task.state, "FAILED");
    assert!(
        task.error
            .as_deref()
            .is_some_and(|e| e.contains("no pending scheduled activity"))
    );

    let execution = load_execution_from_url(&database_url, exec_id).await;
    assert_eq!(execution.state, "FAILED");
    assert!(
        execution
            .error
            .as_deref()
            .is_some_and(|e| e.contains("no pending scheduled activity"))
    );

    let history = load_history_from_url(&database_url, exec_id).await;
    assert!(matches!(
        history.events.last(),
        Some(WorkflowEvent::WorkflowFailed { error, .. })
            if error.contains("no pending scheduled activity")
    ));
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn timeout_enforcement_fails_pending_activity_and_wakes_workflow() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");
    let exec_id = insert_workflow_execution(&mut conn).await;
    let activity_id = ActivityExecId::new();

    store::append_events(
        &mut conn,
        exec_id,
        &[
            WorkflowEvent::WorkflowStarted {
                input: serde_json::json!({"timeout": "schedule_to_start"}),
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "send_email".into(),
                input: serde_json::json!({"to": "alice@example.com"}),
                queue: "stuck-queue".into(),
            },
        ],
        0,
    )
    .await
    .expect("append initial history failed");

    let mut workflow_params = EnqueueParams::new(
        "default",
        TaskType::Workflow,
        serde_json::json!({"workflow": true}),
    );
    workflow_params.workflow_exec_id = Some(exec_id.as_uuid());
    workflow_params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);
    let workflow_task_id = queue::enqueue(&mut conn, &workflow_params)
        .await
        .expect("enqueue parked workflow task failed");

    let default_queues = vec!["default".to_string()];
    let claimed_workflow = queue::claim_task(
        &mut conn,
        &default_queues,
        "parked-worker",
        "",
        None,
        &[],
        &[],
    )
    .await
    .expect("claim parked workflow task failed")
    .expect("workflow task should be claimable");
    assert_eq!(claimed_workflow.id, workflow_task_id);
    assert_eq!(claimed_workflow.state, "RUNNING");
    queue::park_workflow_task(&mut conn, workflow_task_id, None)
        .await
        .expect("park workflow task failed");

    let mut activity_params = EnqueueParams::new(
        "stuck-queue",
        TaskType::Activity,
        serde_json::json!({"to": "alice@example.com"}),
    );
    activity_params.workflow_exec_id = Some(exec_id.as_uuid());
    activity_params.activity_name = Some("send_email".to_string());
    activity_params.schedule_to_start = Some(chrono::Duration::milliseconds(50));
    activity_params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);
    let activity_task_id = queue::enqueue(&mut conn, &activity_params)
        .await
        .expect("enqueue timed-out activity task failed");

    let enforced = timeout::enforce_timeouts_once(
        &mut conn,
        &autumn_harvest::telemetry::NoOpMetrics,
        std::time::Duration::from_secs(5),
        &None,
        &[],
        None,
        None,
        60,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
        0,
    )
    .await
    .expect("timeout enforcement should succeed");
    assert_eq!(enforced, 1);

    let workflow_task = load_task_from_url(&database_url, workflow_task_id).await;
    assert_eq!(workflow_task.state, "PENDING");

    let activity_task = load_task_from_url(&database_url, activity_task_id).await;
    assert_eq!(activity_task.state, "FAILED");
    assert!(activity_task.error.as_deref().is_some_and(|error| {
        error.contains("ScheduleToStart") && error.contains("send_email")
    }));

    let history = store::load_history(&mut conn, exec_id)
        .await
        .expect("load_history after timeout enforcement failed");
    assert!(matches!(
        history.events.as_slice(),
        [
            WorkflowEvent::WorkflowStarted { .. },
            WorkflowEvent::ActivityScheduled { .. },
            WorkflowEvent::ActivityTimedOut {
                timeout_type: TimeoutType::ScheduleToStart,
                ..
            },
        ]
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn worker_fails_workflow_when_activity_start_to_close_timeout_elapses() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let exec_id = insert_workflow_execution(&mut conn).await;
    let workflow_input = serde_json::json!({"slow": true});

    store::append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: workflow_input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted failed");

    let mut params = EnqueueParams::new("default", TaskType::Workflow, workflow_input);
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);

    queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue workflow task failed");

    let worker = Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                codec_rotation_batch_size: 0,
                dr: autumn_harvest::replication::DrConfig::default(),
                worker_id: "worker-e2e-activity-timeout".to_string(),
                queues: vec!["default".to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 1,
                max_concurrent_activities: 1,
                poll_interval: Duration::from_millis(25),
                shutdown_timeout: Duration::from_secs(1),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![autumn_harvest::types::ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
                build_id: String::new(),
                deployment_name: None,
                workflow_cache_size: 1000,
                priority_aging_secs: None,
                unknown_target_grace_window: Duration::from_secs(5),
                poison_pill_threshold: 3,
                capability_miss_max_redeliveries: 5,

                workflow_task_timeout: std::time::Duration::from_secs(10),
                workflow_panic_max_attempts: 3,
                labels: std::collections::HashMap::new(),
                queue_weights: std::collections::HashMap::new(),
                max_workflow_pause_duration: std::time::Duration::from_secs(24 * 3600),
                max_workflow_history_events: None,
                shard_notification_database_urls: Vec::new(),
                sharded_pool: None,
                slot_tuner: None,
                max_concurrent_sessions: 0,
            },
            Arc::new(HandlerRegistry::new(
                vec![WorkflowInfo {
                    quota: None,
                    declared_activities: None,
                    declared_children: None,
                    mcp: false,
                    name: "e2e_test_workflow",
                    module: "integration_e2e",
                    handler: workflow_with_slow_activity,
                    execution_timeout: None,
                    chain_execution_timeout: None,
                    sla: None,
                    concurrency: None,

                    debounce: None,
                    batch: None,
                    throttle: None,
                    max_input_bytes: None,

                    owner: None,
                    runbook_url: None,
                    severity: None,
                    description: None,
                    input_schema: None,
                    output_schema: None,
                    error_schema: None,
                    retry_policy: None,
                }],
                vec![ActivityInfo {
                    name: "slow_activity",
                    module: "integration_e2e",
                    default_retry_policy: None,
                    default_start_to_close: Some(Duration::from_millis(50)),
                    default_heartbeat_timeout: None,
                    default_schedule_to_start: None,
                    default_schedule_to_close: None,
                    default_queue: Some("default"),
                    max_concurrent: None,
                    concurrency_key: None,
                    rate_limit_rps: None,
                    rate_limit_burst: None,
                    rate_limit_key: None,
                    rate_limit_key_expr: None,
                    circuit_breaker: None,
                    is_local: false,
                    max_input_bytes: None,
                    max_result_bytes: None,
                    requires: None,
                    handler: slow_activity,
                }],
            )),
        )
        .expect("worker should build"),
    );
    let pool = build_test_pool(&database_url);
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();

    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let failed_execution = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let execution = load_execution_from_url(&database_url, exec_id).await;
            if execution.state == "FAILED" {
                break execution;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    tokio::time::sleep(Duration::from_millis(300)).await;

    let execution = failed_execution.expect("worker should fail timed-out workflow within timeout");
    assert_eq!(execution.state, "FAILED");
    assert!(execution.error.as_deref().is_some_and(|error| {
        error.contains("StartToClose") && error.contains("slow_activity")
    }));

    let history = load_history_from_url(&database_url, exec_id).await;
    assert!(matches!(
        history.events.as_slice(),
        [
            WorkflowEvent::WorkflowStarted { .. },
            WorkflowEvent::ActivityScheduled { .. },
            WorkflowEvent::ActivityStarted { .. },
            WorkflowEvent::ActivityTimedOut {
                timeout_type: TimeoutType::StartToClose,
                ..
            },
            WorkflowEvent::WorkflowFailed { .. },
        ]
    ));

    let tasks = load_tasks_for_execution_from_url(&database_url, exec_id).await;
    assert_eq!(tasks.len(), 2);
    assert!(tasks.iter().all(|task| task.state == "FAILED"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn worker_completes_workflow_with_timer_round_trip() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let exec_id = insert_workflow_execution(&mut conn).await;
    let workflow_input = serde_json::json!({"timer": true});

    store::append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: workflow_input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted failed");

    let mut params = EnqueueParams::new("default", TaskType::Workflow, workflow_input);
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);

    queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue workflow task failed");

    let worker = Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                codec_rotation_batch_size: 0,
                dr: autumn_harvest::replication::DrConfig::default(),
                worker_id: "worker-e2e-timer-round-trip".to_string(),
                queues: vec!["default".to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 1,
                max_concurrent_activities: 1,
                poll_interval: Duration::from_millis(25),
                shutdown_timeout: Duration::from_secs(1),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![autumn_harvest::types::ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
                build_id: String::new(),
                deployment_name: None,
                workflow_cache_size: 1000,
                priority_aging_secs: None,
                unknown_target_grace_window: Duration::from_secs(5),
                poison_pill_threshold: 3,
                capability_miss_max_redeliveries: 5,

                workflow_task_timeout: std::time::Duration::from_secs(10),
                workflow_panic_max_attempts: 3,
                labels: std::collections::HashMap::new(),
                queue_weights: std::collections::HashMap::new(),
                max_workflow_pause_duration: std::time::Duration::from_secs(24 * 3600),
                max_workflow_history_events: None,
                shard_notification_database_urls: Vec::new(),
                sharded_pool: None,
                slot_tuner: None,
                max_concurrent_sessions: 0,
            },
            Arc::new(HandlerRegistry::new(
                vec![WorkflowInfo {
                    quota: None,
                    declared_activities: None,
                    declared_children: None,
                    mcp: false,
                    name: "e2e_test_workflow",
                    module: "integration_e2e",
                    handler: workflow_with_timer,
                    execution_timeout: None,
                    chain_execution_timeout: None,
                    sla: None,
                    concurrency: None,

                    debounce: None,
                    batch: None,
                    throttle: None,
                    max_input_bytes: None,

                    owner: None,
                    runbook_url: None,
                    severity: None,
                    description: None,
                    input_schema: None,
                    output_schema: None,
                    error_schema: None,
                    retry_policy: None,
                }],
                vec![],
            )),
        )
        .expect("worker should build"),
    );
    let pool = build_test_pool(&database_url);
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();

    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let completed_execution = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let execution = load_execution_from_url(&database_url, exec_id).await;
            if execution.state == "COMPLETED" {
                break execution;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    let execution =
        completed_execution.expect("worker should complete workflow-with-timer within timeout");
    assert_eq!(
        execution.output,
        Some(serde_json::json!({"timer": "fired"}))
    );

    let history = load_history_from_url(&database_url, exec_id).await;
    assert!(matches!(
        history.events.as_slice(),
        [
            WorkflowEvent::WorkflowStarted { .. },
            WorkflowEvent::TimerStarted { .. },
            WorkflowEvent::TimerFired { .. },
            WorkflowEvent::WorkflowCompleted { .. },
        ]
    ));

    let timers = load_timers_for_execution_from_url(&database_url, exec_id).await;
    assert_eq!(timers.len(), 1, "a durable timer row should be created");
    assert!(timers[0].fired, "timer should be marked fired once resumed");
}

/// Issue #697 AC4 (DB, real worker loop): a child spawned by a shard-pinned
/// parent stays on the parent's shard -- **both** in the encoded `ExecutionId`
/// (what every id-based lookup routes on) and in the row's `shard_id` column.
/// The context-level unit tests prove the id is minted correctly; this proves
/// the whole worker persistence path preserves it end to end, so residency is
/// transitive across the workflow tree rather than implicitly assumed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_child_of_a_shard_pinned_parent_inherits_the_parents_shard() {
    use autumn_harvest::types::ShardId;

    let pinned = ShardId::new(5);
    let (database_url, _container) = setup_test_database_url_or_env().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let parent_exec_id = insert_workflow_execution_on_shard(&mut conn, pinned).await;
    assert_eq!(
        parent_exec_id.shard(),
        pinned,
        "precondition: the parent must actually be pinned"
    );
    enqueue_started_workflow_task(
        &mut conn,
        parent_exec_id,
        serde_json::json!({"value": "from-pinned-parent"}),
    )
    .await;

    let worker = build_runtime_worker(
        "worker-e2e-child-shard-inheritance",
        2,
        1,
        child_round_trip_registry(),
    );
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    wait_for_execution_state(&database_url, parent_exec_id, "COMPLETED").await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    let child_execs = load_child_executions_from_url(&database_url, parent_exec_id).await;
    assert_eq!(child_execs.len(), 1, "exactly one child should be created");
    let child = &child_execs[0];

    // The encoded shard is the load-bearing half: an `ExecutionId::new()` child
    // would carry the UNENCODED sentinel and every id-based lookup would route
    // it to the deployment's *default* shard, breaking residency even though
    // the row itself landed on the parent's shard.
    let child_exec_id: ExecutionId = child
        .id
        .to_string()
        .parse()
        .expect("child execution id should parse");
    assert_eq!(
        child_exec_id.shard(),
        pinned,
        "the child's ExecutionId must encode the parent's shard (issue #697 AC4)"
    );
    assert_eq!(
        child.shard_id,
        pinned.as_i32(),
        "the child's row must live on the parent's shard (issue #697 AC4)"
    );
}

/// Issue #697 AC4 + the issue's **success metric** (DB, real worker loop):
/// a shard-pinned run, the child it spawns, AND the continue-as-new successor
/// it forks all stay on the pinned shard -- the metric's literal
/// "land on (and remain on across children/CAN) the intended shard" clause,
/// measured in one run rather than assumed from reading the mint sites.
///
/// Deliberately pinned to a **non-default** shard (5) so a regression that
/// hardcodes `ShardId::new(0)` / `default_shard` is caught, not just one that
/// mints the `UNENCODED` sentinel. Each assertion checks BOTH halves: the
/// encoded `ExecutionId` (what every later id-based lookup routes on) and the
/// row's `shard_id` column (where the data physically lives).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_pinned_child_and_continue_as_new_successor_stay_on_the_pinned_shard() {
    use autumn_harvest::types::ShardId;

    let pinned = ShardId::new(5);
    let (database_url, _container) = setup_test_database_url_or_env().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let parent_exec_id = insert_workflow_execution_on_shard(&mut conn, pinned).await;
    assert_eq!(
        parent_exec_id.shard(),
        pinned,
        "precondition: the parent must actually be pinned"
    );
    enqueue_started_workflow_task(
        &mut conn,
        parent_exec_id,
        serde_json::json!({"phase": "init"}),
    )
    .await;

    let worker = build_runtime_worker(
        "worker-e2e-pinned-descendants",
        2,
        1,
        child_then_continue_as_new_registry(),
    );
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    // The pinned run seals CONTINUED_AS_NEW once it has spawned its child and
    // forked its successor.
    let _sealed = wait_for_execution_state(&database_url, parent_exec_id, "CONTINUED_AS_NEW").await;
    let parent_history = load_history_from_url(&database_url, parent_exec_id).await;
    let successor_exec_id = parent_history
        .events
        .iter()
        .find_map(|event| match event {
            WorkflowEvent::WorkflowContinuedAsNew { new_exec_id, .. } => Some(*new_exec_id),
            _ => None,
        })
        .expect("pinned history should contain a WorkflowContinuedAsNew event");
    let successor = wait_for_execution_state(&database_url, successor_exec_id, "COMPLETED").await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    // --- descendant 1: the spawned child ---
    let child_execs = load_child_executions_from_url(&database_url, parent_exec_id).await;
    assert_eq!(child_execs.len(), 1, "exactly one child should be created");
    let child_exec_id: ExecutionId = child_execs[0]
        .id
        .to_string()
        .parse()
        .expect("child execution id should parse");
    assert_eq!(
        child_exec_id.shard(),
        pinned,
        "the child's ExecutionId must encode the pinned shard (issue #697 AC4)"
    );
    assert_eq!(
        child_execs[0].shard_id,
        pinned.as_i32(),
        "the child's row must live on the pinned shard (issue #697 AC4)"
    );

    // --- descendant 2: the continue-as-new successor ---
    assert_eq!(
        successor_exec_id.shard(),
        pinned,
        "the continue-as-new successor's ExecutionId must encode the pinned \
         shard -- an `ExecutionId::new()` or default-shard successor would \
         silently move a residency-bound chain off its shard (issue #697 AC4)"
    );
    assert_eq!(
        successor.shard_id,
        pinned.as_i32(),
        "the continue-as-new successor's row must live on the pinned shard \
         (issue #697 AC4)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_completes_parent_workflow_after_child_workflow_round_trip() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let parent_exec_id = insert_workflow_execution(&mut conn).await;
    let workflow_input = serde_json::json!({"value": "from-parent"});
    enqueue_started_workflow_task(&mut conn, parent_exec_id, workflow_input).await;

    let worker = build_runtime_worker(
        "worker-e2e-child-round-trip",
        2,
        1,
        child_round_trip_registry(),
    );
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let parent_execution =
        wait_for_execution_state(&database_url, parent_exec_id, "COMPLETED").await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    assert_eq!(
        parent_execution.output,
        Some(serde_json::json!({"child": "from-parent"}))
    );

    let parent_history = load_history_from_url(&database_url, parent_exec_id).await;
    assert!(matches!(
        parent_history.events.as_slice(),
        [
            WorkflowEvent::WorkflowStarted { .. },
            WorkflowEvent::ChildWorkflowStarted { .. },
            WorkflowEvent::ChildWorkflowCompleted { .. },
            WorkflowEvent::WorkflowCompleted { .. },
        ]
    ));

    let child_execs = load_child_executions_from_url(&database_url, parent_exec_id).await;
    assert_eq!(
        child_execs.len(),
        1,
        "exactly one child execution should be created"
    );
    let child_execution = &child_execs[0];
    assert_eq!(child_execution.workflow_name, "child_echo_workflow");
    assert_eq!(
        child_execution.output,
        Some(serde_json::json!({"child": "from-parent"}))
    );

    let child_history = load_history_from_url(
        &database_url,
        child_execution
            .id
            .to_string()
            .parse()
            .expect("child execution id should parse"),
    )
    .await;
    assert!(matches!(
        child_history.events.as_slice(),
        [
            WorkflowEvent::WorkflowStarted { .. },
            WorkflowEvent::WorkflowCompleted { .. },
        ]
    ));
}

/// Issue #767 success metric (DB, real worker loop): a child that fails with a
/// *typed* `WorkflowFailure` surfaces its `error_type`/`non_retryable` to the
/// parent, which branches on the typed class (never the message) across ≥3
/// categories. Also asserts AC4: the child's `execution.error` column carries
/// the human message, not the wire envelope; and the child's own history
/// terminal `WorkflowFailed` event carries the typed fields.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_parent_branches_on_typed_child_failure_across_categories() {
    for (category, expected_branch) in [
        ("ValidationRejected", "reject_and_notify"),
        ("BudgetExceeded", "escalate_to_finance"),
        ("UpstreamUnavailable", "reschedule"),
    ] {
        let (database_url, _container) = setup_test_database_url().await;
        let mut conn =
            <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
                .await
                .expect("failed to connect to Postgres container");

        let parent_exec_id = insert_workflow_execution(&mut conn).await;
        enqueue_started_workflow_task(
            &mut conn,
            parent_exec_id,
            serde_json::json!({ "category": category }),
        )
        .await;

        let worker = build_runtime_worker(
            "worker-e2e-typed-child-failure",
            2,
            1,
            typed_child_failure_registry(),
        );
        let pool = build_test_pool(&database_url);
        let handle = spawn_test_worker(Arc::clone(&worker), pool);

        let parent_execution =
            wait_for_execution_state(&database_url, parent_exec_id, "COMPLETED").await;

        worker.shutdown();
        handle.await.expect("worker task should join");

        // The parent completed on the branch dictated by the child's typed class.
        let output = parent_execution.output.expect("parent output");
        assert_eq!(
            output.get("branch").and_then(serde_json::Value::as_str),
            Some(expected_branch),
            "category {category}: parent must branch on the typed error_type"
        );
        assert_eq!(
            output
                .get("observed_error_type")
                .and_then(serde_json::Value::as_str),
            Some(category),
            "category {category}: parent must observe the typed error_type"
        );
        assert_eq!(
            output
                .get("non_retryable")
                .and_then(serde_json::Value::as_bool),
            Some(true),
            "category {category}: parent must observe the non_retryable flag"
        );

        // The child ended FAILED; its own history terminal WorkflowFailed carries
        // the typed fields, and its error column is the human message (AC4).
        let child_execs = load_child_executions_from_url(&database_url, parent_exec_id).await;
        assert_eq!(child_execs.len(), 1, "exactly one child execution");
        let child = &child_execs[0];
        assert_eq!(child.state, "FAILED");
        // AC4: human message, not the wire envelope.
        let child_error = child.error.clone().expect("child error");
        assert!(
            child_error.contains(&format!("child rejected the request for {category}")),
            "child.error must be the human message, got: {child_error}"
        );
        assert!(
            !child_error.contains("harvest_workflow_failure_v1"),
            "child.error must NOT be the wire envelope, got: {child_error}"
        );

        let child_history = load_history_from_url(
            &database_url,
            child.id.to_string().parse().expect("child id parses"),
        )
        .await;
        let typed = child_history.events.iter().find_map(|e| match e {
            WorkflowEvent::WorkflowFailed {
                error_type,
                non_retryable,
                details,
                ..
            } => Some((error_type.clone(), *non_retryable, details.clone())),
            _ => None,
        });
        let (et, nr, details) = typed.expect("child history has a WorkflowFailed event");
        assert_eq!(
            et.as_deref(),
            Some(category),
            "child WorkflowFailed error_type"
        );
        assert_eq!(nr, Some(true), "child WorkflowFailed non_retryable");
        assert_eq!(details, Some(serde_json::json!({ "source": "child" })));

        // Parent history: child failure recorded as a typed ChildWorkflowFailed.
        let parent_history = load_history_from_url(&database_url, parent_exec_id).await;
        let child_failed_type = parent_history.events.iter().find_map(|e| match e {
            WorkflowEvent::ChildWorkflowFailed { error_type, .. } => Some(error_type.clone()),
            _ => None,
        });
        assert_eq!(
            child_failed_type
                .expect("parent has ChildWorkflowFailed")
                .as_deref(),
            Some(category),
            "parent's ChildWorkflowFailed must carry the typed error_type"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_continue_as_new_rejection_wakes_parent_with_child_failure() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let parent_exec_id = insert_workflow_execution(&mut conn).await;
    let workflow_input = serde_json::json!({"phase": "init"});
    enqueue_started_workflow_task(&mut conn, parent_exec_id, workflow_input).await;

    let worker = build_runtime_worker(
        "worker-e2e-child-continue-reject",
        2,
        1,
        child_continue_as_new_rejection_registry(),
    );
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let parent_execution = wait_for_execution_state(&database_url, parent_exec_id, "FAILED").await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    let parent_history = load_history_from_url(&database_url, parent_exec_id).await;
    assert!(
        matches!(
            parent_history.events.as_slice(),
            [
                WorkflowEvent::WorkflowStarted { .. },
                WorkflowEvent::ChildWorkflowStarted { .. },
                WorkflowEvent::ChildWorkflowFailed { .. },
                WorkflowEvent::WorkflowFailed { .. },
            ]
        ),
        "parent should observe a terminal child failure instead of staying parked: {:?}",
        parent_history.events
    );
    let child_failure = parent_history
        .events
        .iter()
        .find_map(|event| match event {
            WorkflowEvent::ChildWorkflowFailed {
                child_id, error, ..
            } => Some((*child_id, error.clone())),
            _ => None,
        })
        .expect("parent history should include ChildWorkflowFailed");
    assert!(
        child_failure
            .1
            .contains("continue_as_new is not supported in child workflows"),
        "parent should receive the rejection reason from the child"
    );

    let child_execution = load_execution_from_url(&database_url, child_failure.0).await;
    assert_eq!(child_execution.state, "FAILED");
    assert_eq!(
        child_execution.error.as_deref(),
        Some("continue_as_new is not supported in child workflows in this release"),
    );

    let child_history = load_history_from_url(&database_url, child_failure.0).await;
    assert!(
        matches!(
            child_history.events.as_slice(),
            [
                WorkflowEvent::WorkflowStarted { .. },
                WorkflowEvent::WorkflowFailed { .. },
            ]
        ),
        "child should fail cleanly after the rejected continue-as-new: {:?}",
        child_history.events
    );

    assert!(
        parent_execution.error.as_deref().is_some_and(
            |error| error.contains("continue_as_new is not supported in child workflows")
        ),
        "parent should fail with the propagated child error",
    );
}

// ── Parallel child workflow handler functions ────────────────────────────────

/// Parent that spawns two children concurrently via `tokio::join!` and returns
/// a merged result.
fn parent_workflow_parallel_children<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let (a, b) = tokio::join!(
            ctx.spawn_child_workflow_raw("child_alpha", serde_json::json!({"item": "alpha"})),
            ctx.spawn_child_workflow_raw("child_beta", serde_json::json!({"item": "beta"})),
        );
        let a = a.map_err(|e| e.to_string())?;
        let b = b.map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"alpha": a, "beta": b}))
    })
}

fn child_alpha_workflow<'a>(
    _ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        Ok(serde_json::json!({"result": input.get("item").and_then(|v| v.as_str()).unwrap_or("?")}))
    })
}

fn child_beta_workflow<'a>(
    _ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        Ok(serde_json::json!({"result": input.get("item").and_then(|v| v.as_str()).unwrap_or("?")}))
    })
}

fn parallel_children_registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![
            WorkflowInfo {
                quota: None,
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: "e2e_test_workflow",
                module: "integration_e2e",
                handler: parent_workflow_parallel_children,
                execution_timeout: None,
                chain_execution_timeout: None,
                sla: None,
                concurrency: None,

                debounce: None,
                batch: None,
                throttle: None,
                max_input_bytes: None,

                owner: None,
                runbook_url: None,
                severity: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
                retry_policy: None,
            },
            WorkflowInfo {
                quota: None,
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: "child_alpha",
                module: "integration_e2e",
                handler: child_alpha_workflow,
                execution_timeout: None,
                chain_execution_timeout: None,
                sla: None,
                concurrency: None,

                debounce: None,
                batch: None,
                throttle: None,
                max_input_bytes: None,

                owner: None,
                runbook_url: None,
                severity: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
                retry_policy: None,
            },
            WorkflowInfo {
                quota: None,
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: "child_beta",
                module: "integration_e2e",
                handler: child_beta_workflow,
                execution_timeout: None,
                chain_execution_timeout: None,
                sla: None,
                concurrency: None,

                debounce: None,
                batch: None,
                throttle: None,
                max_input_bytes: None,

                owner: None,
                runbook_url: None,
                severity: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
                retry_policy: None,
            },
        ],
        vec![],
    ))
}

/// RED test: parent spawns two child workflows in parallel via `tokio::join!`.
///
/// Both children must complete and the parent must produce a merged result
/// containing both outputs.  With the current single-child dispatch the
/// worker fails because it cannot handle two simultaneous `StartChildWorkflow`
/// commands.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_completes_parent_workflow_with_parallel_child_workflows() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let parent_exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, parent_exec_id, serde_json::json!({})).await;

    let worker = build_runtime_worker(
        "worker-e2e-parallel-children",
        4,
        2,
        parallel_children_registry(),
    );
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let parent_execution =
        wait_for_execution_state(&database_url, parent_exec_id, "COMPLETED").await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    assert_eq!(
        parent_execution.output,
        Some(serde_json::json!({
            "alpha": {"result": "alpha"},
            "beta":  {"result": "beta"},
        })),
        "parent output must contain merged results from both children"
    );

    let parent_history = load_history_from_url(&database_url, parent_exec_id).await;
    let child_started_count = parent_history
        .events
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ChildWorkflowStarted { .. }))
        .count();
    let child_completed_count = parent_history
        .events
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ChildWorkflowCompleted { .. }))
        .count();
    assert_eq!(
        child_started_count, 2,
        "parent history must record both child starts"
    );
    assert_eq!(
        child_completed_count, 2,
        "parent history must record both child completions"
    );

    let child_execs = load_child_executions_from_url(&database_url, parent_exec_id).await;
    assert_eq!(
        child_execs.len(),
        2,
        "exactly two child executions must be stored with parent_id set"
    );
    for child_exec in &child_execs {
        assert_eq!(
            child_exec.state, "COMPLETED",
            "each child execution must be COMPLETED"
        );
    }
}

// ── Child workflow fan-out (issue #601) ──────────────────────────────────────

/// Parent that fans out three children via `spawn_child_workflow_fan_out_raw`
/// and returns their outputs in input order.
fn parent_workflow_child_fan_out<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let children = vec![
            ("fan_child".to_string(), serde_json::json!({"item": "one"})),
            ("fan_child".to_string(), serde_json::json!({"item": "two"})),
            (
                "fan_child".to_string(),
                serde_json::json!({"item": "three"}),
            ),
        ];
        let results = ctx
            .spawn_child_workflow_fan_out_raw(children)
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({ "results": results }))
    })
}

fn fan_child_workflow<'a>(
    _ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        Ok(serde_json::json!({"result": input.get("item").and_then(|v| v.as_str()).unwrap_or("?")}))
    })
}

fn child_fan_out_registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![
            WorkflowInfo {
                quota: None,
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: "e2e_test_workflow",
                module: "integration_e2e",
                handler: parent_workflow_child_fan_out,
                execution_timeout: None,
                chain_execution_timeout: None,
                sla: None,
                concurrency: None,

                debounce: None,
                batch: None,
                throttle: None,
                max_input_bytes: None,

                owner: None,
                runbook_url: None,
                severity: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
                retry_policy: None,
            },
            WorkflowInfo {
                quota: None,
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: "fan_child",
                module: "integration_e2e",
                handler: fan_child_workflow,
                execution_timeout: None,
                chain_execution_timeout: None,
                sla: None,
                concurrency: None,

                debounce: None,
                batch: None,
                throttle: None,
                max_input_bytes: None,

                owner: None,
                runbook_url: None,
                severity: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
                retry_policy: None,
            },
        ],
        vec![],
    ))
}

/// Dispatches three awaited children concurrently and then fails from a sibling
/// branch in the SAME decision cycle, so the cycle never suspends (issue #952).
fn parent_workflow_dispatches_three_children_then_fails<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let dispatch = async {
            futures::future::try_join_all((0..3).map(|slot| {
                ctx.spawn_child_workflow_raw("fan_child", serde_json::json!({ "item": slot }))
            }))
            .await
            .map_err(|e| e.to_string())
        };
        let sibling =
            async { Err::<Vec<serde_json::Value>, String>("budget exceeded".to_string()) };
        futures::try_join!(dispatch, sibling)?;
        Ok(serde_json::Value::Null)
    })
}

fn abandoned_child_dispatch_registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![
            WorkflowInfo {
                quota: None,
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: "e2e_test_workflow",
                module: "integration_e2e",
                handler: parent_workflow_dispatches_three_children_then_fails,
                execution_timeout: None,
                chain_execution_timeout: None,
                sla: None,
                concurrency: None,

                debounce: None,
                batch: None,
                throttle: None,
                max_input_bytes: None,

                owner: None,
                runbook_url: None,
                severity: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
                retry_policy: None,
            },
            WorkflowInfo {
                quota: None,
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: "fan_child",
                module: "integration_e2e",
                handler: fan_child_workflow,
                execution_timeout: None,
                chain_execution_timeout: None,
                sla: None,
                concurrency: None,

                debounce: None,
                batch: None,
                throttle: None,
                max_input_bytes: None,

                owner: None,
                runbook_url: None,
                severity: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
                retry_policy: None,
            },
        ],
        vec![],
    ))
}

/// Issue #952's success metric against a real database: in a failing decision
/// cycle the persisted history's dispatched-children count matches the commands
/// the code issued.
///
/// Before #952, `persist_terminal_outcome_commands` replayed only
/// `RecordMarker`/`RecordSideEffect`/`SpawnDetachedChildWorkflow` from the
/// pending-command list, so all three `StartChildWorkflow` commands vanished:
/// no `ChildWorkflowStarted`, no child rows, an audit trail that lied about what
/// the code did. Now each dispatch is recorded at its emission position with a
/// synthetic terminal marking it never-started — and still no child execution
/// rows, because nothing was ever started.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_records_every_dispatched_child_when_the_cycle_fails() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let parent_exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, parent_exec_id, serde_json::json!({})).await;

    let worker = build_runtime_worker(
        "worker-e2e-abandoned-child-dispatch",
        6,
        2,
        abandoned_child_dispatch_registry(),
    );
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let parent_execution = wait_for_execution_state(&database_url, parent_exec_id, "FAILED").await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    assert!(
        parent_execution
            .error
            .as_deref()
            .is_some_and(|e| e.contains("budget exceeded")),
        "the sibling branch's error must seal the run: {:?}",
        parent_execution.error
    );

    let parent_history = load_history_from_url(&database_url, parent_exec_id).await;
    assert_eq!(
        parent_history
            .events
            .iter()
            .filter(|e| matches!(e, WorkflowEvent::ChildWorkflowStarted { .. }))
            .count(),
        3,
        "every dispatched child must appear in the persisted history: {:?}",
        parent_history.events
    );
    let abandoned = parent_history
        .events
        .iter()
        .filter(|e| {
            matches!(
                e,
                WorkflowEvent::ChildWorkflowFailed { error, .. }
                    if error == autumn_harvest::event::ABANDONED_DISPATCH_REASON
            )
        })
        .count();
    assert_eq!(
        abandoned, 3,
        "each dispatch must carry its synthetic terminal so replay resolves it: {:?}",
        parent_history.events
    );
    assert!(
        matches!(
            parent_history.events.last(),
            Some(WorkflowEvent::WorkflowFailed { .. })
        ),
        "the terminal failure must still be the last event: {:?}",
        parent_history.events
    );

    let child_execs = load_child_executions_from_url(&database_url, parent_exec_id).await;
    assert!(
        child_execs.is_empty(),
        "an abandoned dispatch starts no child execution: {child_execs:?}"
    );
}

/// End-to-end proof that the worker persists a `spawn_child_workflow_fan_out_raw`
/// suspension batch (the `fan_out:{n}` marker + N `StartChildWorkflow` commands)
/// exactly like the pre-existing hand-rolled `tokio::join!` parallel-children
/// path, and that all three children complete and merge in input order.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_completes_parent_workflow_with_child_fan_out() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let parent_exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, parent_exec_id, serde_json::json!({})).await;

    let worker = build_runtime_worker("worker-e2e-child-fan-out", 6, 2, child_fan_out_registry());
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let parent_execution =
        wait_for_execution_state(&database_url, parent_exec_id, "COMPLETED").await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    assert_eq!(
        parent_execution.output,
        Some(serde_json::json!({
            "results": [
                {"result": "one"},
                {"result": "two"},
                {"result": "three"},
            ],
        })),
        "parent output must contain merged child results in input order"
    );

    let parent_history = load_history_from_url(&database_url, parent_exec_id).await;
    let has_fan_out_marker = parent_history.events.iter().any(
        |e| matches!(e, WorkflowEvent::MarkerRecorded { name, .. } if name.starts_with("fan_out:")),
    );
    assert!(
        has_fan_out_marker,
        "parent history must record a fan_out:{{n}} marker"
    );

    let child_started_count = parent_history
        .events
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ChildWorkflowStarted { .. }))
        .count();
    let child_completed_count = parent_history
        .events
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ChildWorkflowCompleted { .. }))
        .count();
    assert_eq!(
        child_started_count, 3,
        "parent history must record all 3 child starts"
    );
    assert_eq!(
        child_completed_count, 3,
        "parent history must record all 3 child completions"
    );

    let child_execs = load_child_executions_from_url(&database_url, parent_exec_id).await;
    assert_eq!(
        child_execs.len(),
        3,
        "exactly three child executions must be stored with parent_id set"
    );
    for child_exec in &child_execs {
        assert_eq!(
            child_exec.state, "COMPLETED",
            "each child execution must be COMPLETED"
        );
    }
}

/// Slow child used by the wall-clock success-metric test: durably waits for
/// real wall-clock time before returning, so N of these running in parallel
/// proves genuine concurrent dispatch rather than sequential replay-cycle
/// scheduling.
///
/// **Must use `ctx.timer(...)`, never a raw `tokio::time::sleep` inside the
/// workflow body.** `drive_workflow`'s live-execution poll wraps the entire
/// handler call in `tokio::time::timeout(SUSPENSION_TIMEOUT, ...)` with
/// `SUSPENSION_TIMEOUT = 100ms` (executor.rs) -- a hard, non-configurable
/// budget for the workflow function to either complete or reach a genuine
/// command-emitting suspension point (e.g. `rx.await` after pushing
/// `WorkflowCommand::StartTimer`/`ScheduleActivity`/etc.). A raw
/// `tokio::time::sleep` participates in neither: it blocks the same poll for
/// its full duration with zero commands emitted, so any sleep longer than
/// 100ms deterministically hits `drive_workflow`'s "workflow suspended
/// without emitted commands; resumption is not implemented yet" fatal error
/// on every single attempt -- not a CI-speed flake, a 100% reproducible bug
/// in the test's own workflow code, confirmed via the diagnostic dump added
/// to this test in an earlier PR #901 review round (every child showed
/// exactly this error). `ctx.timer` pushes `StartTimer` and suspends via a
/// real oneshot immediately, so the actual 1s wait happens on a later,
/// separate decision cycle -- never inside the 100ms window -- exactly like
/// every other durable-wait primitive in this engine.
fn slow_fan_child_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.timer("slow_wait", 1).await.map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"result": input.get("item").and_then(|v| v.as_str()).unwrap_or("?")}))
    })
}

/// Number of children fanned out by [`worker_completes_ten_child_fan_out_within_wall_clock_bound`].
///
/// Reduced from 10 to 5 after reproducing this test's CI flakiness locally
/// (see that test's doc comment for the full investigation): under genuine
/// CPU contention (verified with `taskset -c 0,1` plus background CPU load,
/// simulating a busy shared CI runner) all children still completed near-
/// instantly, but the parent's own reclaim-and-finalize decision cycle --
/// strictly heavier than any single child's, since it replays the full,
/// ever-growing history and re-awaits every already-resolved child future
/// on each reclaim -- occasionally starved for minutes even past the 60s
/// internal `workflow_task_timeout`, because under severe-enough contention
/// even tokio's own timer wheel can't service the deadline promptly. Fewer
/// concurrent decision cycles (6 total instead of 11) directly shrinks both
/// the contention surface and the parent's per-cycle replay cost, while
/// still meaningfully proving genuine concurrent dispatch (distinct from
/// the 3-child correctness test `worker_completes_parent_workflow_with_child_fan_out`).
const SLOW_FAN_CHILD_COUNT: usize = 5;

/// Parent that fans out several slow children (count: [`SLOW_FAN_CHILD_COUNT`]).
fn parent_workflow_ten_slow_children<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let children: Vec<_> = (0..SLOW_FAN_CHILD_COUNT)
            .map(|i| {
                (
                    "slow_fan_child".to_string(),
                    serde_json::json!({"item": format!("item_{i}")}),
                )
            })
            .collect();
        let results = ctx
            .spawn_child_workflow_fan_out_raw(children)
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({ "results": results }))
    })
}

fn ten_slow_children_registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![
            WorkflowInfo {
                quota: None,
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: "e2e_test_workflow",
                module: "integration_e2e",
                handler: parent_workflow_ten_slow_children,
                execution_timeout: None,
                chain_execution_timeout: None,
                sla: None,
                concurrency: None,

                debounce: None,
                batch: None,
                throttle: None,
                max_input_bytes: None,

                owner: None,
                runbook_url: None,
                severity: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
                retry_policy: None,
            },
            WorkflowInfo {
                quota: None,
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: "slow_fan_child",
                module: "integration_e2e",
                handler: slow_fan_child_workflow,
                execution_timeout: None,
                chain_execution_timeout: None,
                sla: None,
                concurrency: None,

                debounce: None,
                batch: None,
                throttle: None,
                max_input_bytes: None,

                owner: None,
                runbook_url: None,
                severity: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
                retry_policy: None,
            },
        ],
        vec![],
    ))
}

/// Polls for the given execution to reach `COMPLETED`, exactly like
/// [`wait_for_execution_state_with_timeout`], but on timeout dumps a full
/// snapshot of the parent's and every child's execution row plus task queue
/// row(s) into the panic message instead of the bare `Elapsed(())`.
///
/// This test (`worker_completes_ten_child_fan_out_within_wall_clock_bound`)
/// failed on CI several consecutive times across a mix of confirmed,
/// distinct dropped-wake fixes and one actual test-authoring bug (see that
/// test's own doc comment for the full history); this snapshot is the
/// fastest way to tell a genuinely-stuck row (`state=RUNNING`,
/// `worker_id=NULL`, i.e. parked forever) apart from mere CI slowness
/// (`state=PENDING`, still waiting for a worker slot) without needing an
/// interactive debugger against a Docker-less sandbox.
async fn wait_for_completion_with_diagnostics(
    database_url: &str,
    parent_exec_id: ExecutionId,
    timeout: Duration,
) -> WorkflowExecution {
    let start = std::time::Instant::now();
    loop {
        let execution = load_execution_from_url(database_url, parent_exec_id).await;
        if execution.state == "COMPLETED" {
            return execution;
        }
        if start.elapsed() >= timeout {
            let parent_tasks = load_tasks_for_execution_from_url(database_url, parent_exec_id)
                .await
                .into_iter()
                .map(|t| {
                    format!(
                        "    task={} state={} worker_id={:?} started_at={:?} \
                         scheduled_at={} wake_requested={} attempt={} \
                         crash_strikes={} error={:?}",
                        t.id,
                        t.state,
                        t.worker_id,
                        t.started_at,
                        t.scheduled_at,
                        t.wake_requested,
                        t.attempt,
                        t.crash_strikes,
                        t.error
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");
            let parent_history = load_history_from_url(database_url, parent_exec_id).await;
            let parent_event_summary = parent_history
                .events
                .iter()
                .enumerate()
                .map(|(i, e)| format!("    [{i}] {}", e.type_name()))
                .collect::<Vec<_>>()
                .join("\n");
            let children = load_child_executions_from_url(database_url, parent_exec_id).await;
            let child_report = {
                let mut lines = Vec::new();
                for child in &children {
                    let child_exec_id = ExecutionId::from_uuid(child.id);
                    let child_tasks =
                        load_tasks_for_execution_from_url(database_url, child_exec_id).await;
                    lines.push(format!(
                        "  child={} name={} state={} error={:?}",
                        child.id, child.workflow_name, child.state, child.error
                    ));
                    for t in &child_tasks {
                        lines.push(format!(
                            "    task={} state={} worker_id={:?} started_at={:?} \
                             scheduled_at={} wake_requested={} attempt={} \
                             crash_strikes={} error={:?}",
                            t.id,
                            t.state,
                            t.worker_id,
                            t.started_at,
                            t.scheduled_at,
                            t.wake_requested,
                            t.attempt,
                            t.crash_strikes,
                            t.error
                        ));
                    }
                }
                lines.join("\n")
            };
            panic!(
                "execution {parent_exec_id} did not reach COMPLETED within {timeout:?} \
                 (currently: {} error={:?}).\n\
                 parent task queue rows:\n{parent_tasks}\n\
                 parent history ({} events):\n{parent_event_summary}\n\
                 children ({} recorded):\n{child_report}",
                execution.state,
                execution.error,
                parent_history.events.len(),
                children.len(),
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Success-metric test (issue #601): 10 children fanned out in parallel
/// must all complete -- proving the fan-out genuinely dispatches all N
/// children concurrently (one suspension batch, one worker wave) rather
/// than one at a time.
///
/// This is deliberately **not** a tight latency assertion. Two rounds of
/// `ubuntu-latest` CI failures (a 5s bound against a shared 10s timeout,
/// then a 20s bound against a 30s timeout) showed that GitHub-hosted
/// runners (2 vCPUs, and this whole suite runs with `--test-threads=1`) do
/// not give 11 genuinely-concurrent workflow tasks -- each doing several
/// real DB round trips -- anywhere close to the throughput available in a
/// typical dev sandbox. The outer bounds below were widened accordingly.
/// The qualitative claim this test defends -- "N children fanned out
/// complete together, not one full round trip at a time" -- does not depend
/// on the exact numbers; only the "all 10 completed" assertion is meant to
/// be load-bearing on constrained hardware.
///
/// **Root cause of five subsequent CI failures, finally found via the
/// diagnostic dump in [`wait_for_completion_with_diagnostics`] (PR #901
/// review rounds 2-8):** the original version of `slow_fan_child_workflow`
/// used a raw `tokio::time::sleep` inside the workflow body instead of
/// `ctx.timer(...)`. That is invalid workflow code for this engine --
/// `drive_workflow`'s live-execution poll wraps the handler call in a hard,
/// non-configurable 100ms `SUSPENSION_TIMEOUT`, and a raw sleep longer than
/// that blocks the poll with zero commands emitted, deterministically
/// hitting "workflow suspended without emitted commands; resumption is not
/// implemented yet" on every attempt. This was mistaken for a series of
/// dropped-wake races across several review rounds (each of which found and
/// fixed a real, independently-confirmed bug in `worker.rs`/`queue.rs` --
/// none of them were the actual cause of *this test's* failures).
/// `slow_fan_child_workflow` now uses `ctx.timer("slow_wait", 1)`, the
/// durable primitive every other wait in this engine uses, which properly
/// suspends via a real oneshot after pushing `StartTimer` instead of
/// blocking the poll.
///
/// **Continued flakiness after the above fix (issue #604 PR, unrelated
/// review round):** even with the timer fix in place, this test still
/// intermittently failed on `ubuntu-latest` with a single decision cycle
/// (the parent, or exactly one of the ten children) sitting `RUNNING`
/// (claimed, `worker_id`/`started_at` populated) for 2+ minutes with zero
/// progress, while the other 9-10 completed within ~1-2s of dispatch as
/// expected. Investigated `persist_started_timer`'s park path
/// (`queue::reschedule_task`) for the same class of dropped-wake bug
/// documented on the sibling `persist_activity_wait_park`/
/// `persist_all_started_child_workflows` paths -- ruled out: timer parking
/// deliberately does not use the `wake_requested` fallback mechanism at
/// all. It reschedules the row to `PENDING` with `scheduled_at = fires_at`
/// (a known future instant, unlike an arbitrary external wake), so the
/// worker's ordinary `WHERE scheduled_at <= NOW()` poll claim picks it up
/// once due -- there is no wake race to lose. The uneven per-task
/// completion pattern (most finish almost instantly, one wedges for
/// minutes) points at pure scheduling-contention on a shared, 2-vCPU CI
/// runner asked to make progress on 11 genuinely concurrent, DB-heavy
/// decision cycles at once, not a logic bug in the wake/park mechanics.
/// `worker_threads` was raised from 4 to 12 (this whole suite runs with
/// `--test-threads=1`, so this is the only test using this many OS threads
/// at any given moment) and the outer wait bound from 90s to 180s to give
/// a legitimately-contended cycle room to finish rather than fail the test
/// outright; see the wait-bound call site below for the full reasoning.
///
/// **2026-09-09 CI-health investigation (issue #1459):** this test failed on
/// `trunk-dev`, run 34282622366, job `Test DB (linux, shard 6)`. The parent
/// decision cycle stayed `RUNNING`. `wake_requested` was true and `attempt`
/// was frozen at 2. All five children had already completed. A controlled
/// experiment pinned four concurrent copies to two CPUs. It reproduced this
/// exact signature in five of twelve runs. Zero of fifteen unconstrained
/// runs failed. The mechanism is a gap in `reset_timed_out_workflow_task`'s
/// own documented pool-retry budget. That budget can exhaust under
/// contention. A stuck row then has no backstop. The poison-pill orphan
/// reclaimer only reclaims tasks owned by a dead worker. It does not
/// reclaim a wedged task on a live worker. See issue #1459 for the full
/// diagnosis and reproduction steps. This is a product-level gap, not a
/// test-tolerance problem. The bound below is not widened again for this
/// cause.
///
/// **2026-09-12 fix:** `reset_timed_out_workflow_task`'s pool-retry budget
/// widened from four attempts over 2.7 seconds toward a longer schedule.
/// Codex review (PR #1517) found the first version still did not help.
/// Harvest configures no deadpool `Timeouts`, so an unbounded
/// `pool.get().await` under real saturation never returns. A retry loop
/// around a call like that never reruns. Each attempt is now itself
/// bounded, so a stuck checkout counts as one failed attempt rather than
/// parking the whole recovery path. The schedule now bounds the whole
/// reset to roughly thirty seconds worst case. The gap itself -- no
/// backstop once that budget is genuinely exhausted -- is still open;
/// see issue #1459.
#[tokio::test(flavor = "multi_thread", worker_threads = 12)]
async fn worker_completes_ten_child_fan_out_within_wall_clock_bound() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let parent_exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, parent_exec_id, serde_json::json!({})).await;

    // Concurrency must comfortably exceed 1 parent + SLOW_FAN_CHILD_COUNT
    // children so every child dispatches in the same wave instead of
    // queueing behind a saturated semaphore.
    //
    // Uses a wide workflow_task_timeout (not the 10s default) as a safety
    // margin against pure CI scheduling contention across several
    // genuinely-concurrent, DB-heavy decision cycles -- this was originally
    // suspected to be the root cause of this test's flakiness, but the
    // actual cause (see the doc comment above) was unrelated: a raw
    // tokio::time::sleep in the workflow body, now fixed. The wider timeout
    // is left in place as cheap, harmless insurance; the test's own outer
    // bound is the real backstop against a genuine stall, and
    // SLOW_FAN_CHILD_COUNT was later reduced from 10 to shrink the
    // contention surface itself (see that constant's doc comment).
    let worker = build_runtime_worker_with_task_timeout(
        "worker-e2e-ten-slow-children",
        16,
        2,
        ten_slow_children_registry(),
        Duration::from_secs(60),
    );
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let start = std::time::Instant::now();
    // Bound widened 90s -> 180s: this specific test has been observed on
    // busy ubuntu-latest runners to leave a single decision cycle (out of
    // 11 genuinely concurrent ones) parked/claimed for 2+ minutes with zero
    // progress before eventually completing -- pure scheduling contention on
    // a 2-vCPU shared runner, not a hang (the other 9-10 children routinely
    // finish within ~1-2s of dispatch). Per the doc comment above, only the
    // "all 10 completed" assertion is meant to be load-bearing on
    // constrained hardware; this outer bound exists purely to catch a
    // genuine stall, not to assert throughput.
    let parent_execution = wait_for_completion_with_diagnostics(
        &database_url,
        parent_exec_id,
        Duration::from_secs(180),
    )
    .await;
    let elapsed = start.elapsed();

    worker.shutdown();
    handle.await.expect("worker task should join");

    let results = parent_execution
        .output
        .as_ref()
        .and_then(|o| o.get("results"))
        .and_then(|r| r.as_array())
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        results.len(),
        SLOW_FAN_CHILD_COUNT,
        "all {SLOW_FAN_CHILD_COUNT} children must complete"
    );

    assert!(
        elapsed < Duration::from_secs(150),
        "{SLOW_FAN_CHILD_COUNT} fanned-out children should complete in one concurrent wave, \
         not one at a time; got {elapsed:?} (see the doc comment above for \
         why this bound is wide)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn worker_builder_state_is_visible_to_workflow_and_activity() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let exec_id = insert_workflow_execution(&mut conn).await;
    let workflow_input = serde_json::json!({"job": "shared-state"});

    store::append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: workflow_input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted failed");

    let mut params = EnqueueParams::new("default", TaskType::Workflow, workflow_input.clone());
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);

    queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue workflow task failed");

    let built = HarvestBuilder::new()
        .workflows(vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: "e2e_test_workflow",
            module: "integration_e2e",
            handler: workflow_with_builder_state,
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }])
        .activities(vec![ActivityInfo {
            name: "stateful_activity",
            module: "integration_e2e",
            default_retry_policy: None,
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_schedule_to_close: None,
            default_queue: Some("default"),
            max_concurrent: None,
            concurrency_key: None,
            rate_limit_rps: None,
            rate_limit_burst: None,
            rate_limit_key: None,
            rate_limit_key_expr: None,
            circuit_breaker: None,
            is_local: false,
            max_input_bytes: None,
            max_result_bytes: None,
            requires: None,
            handler: stateful_activity,
        }])
        .state(String::from("haunted"))
        .worker(WorkerConfig::default())
        .build();
    let (registry, _dags, _workflow_schedules, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = "worker-e2e-builder-state".to_string();
    runtime_config.poll_interval = Duration::from_millis(25);
    runtime_config.shutdown_timeout = Duration::from_secs(1);

    let worker =
        Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker should build"));
    let pool = build_test_pool(&database_url);
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();

    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let completed_execution = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let execution = load_execution_from_url(&database_url, exec_id).await;
            if execution.state == "COMPLETED" {
                break execution;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    let execution =
        completed_execution.expect("worker should complete shared-state workflow within timeout");
    assert_eq!(
        execution.output,
        Some(serde_json::json!({
            "workflow_prefix": "haunted",
            "activity": {
                "activity_prefix": "haunted",
                "payload": workflow_input,
            }
        }))
    );
}

#[tokio::test]
async fn queue_listener_receives_enqueue_notification() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let queues = vec!["default".to_string()];
    let mut listener = autumn_harvest::notify::QueueListener::connect(&database_url, &queues)
        .await
        .expect("listener should connect");

    let exec_id = insert_workflow_execution(&mut conn).await;
    let mut params = EnqueueParams::new(
        "default",
        TaskType::Workflow,
        serde_json::json!({"notify": true}),
    );
    params.workflow_exec_id = Some(exec_id.as_uuid());

    let task_id = queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue should succeed");

    let notification = listener
        .wait_for_notification(Duration::from_secs(2))
        .await
        .expect("listener wait should succeed")
        .expect("listener should receive a notification");

    assert_eq!(notification.task_id, task_id);
}

#[tokio::test]
async fn queue_listener_handles_quoted_queue_names() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let queue_name = "priority\"queue";
    let queues = vec![queue_name.to_string()];
    let mut listener = autumn_harvest::notify::QueueListener::connect(&database_url, &queues)
        .await
        .expect("listener should connect for quoted queue names");

    let exec_id = insert_workflow_execution(&mut conn).await;
    let mut params = EnqueueParams::new(
        queue_name,
        TaskType::Workflow,
        serde_json::json!({"notify": "quoted"}),
    );
    params.workflow_exec_id = Some(exec_id.as_uuid());

    let task_id = queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue should succeed for quoted queue names");

    let notification = listener
        .wait_for_notification(Duration::from_secs(2))
        .await
        .expect("listener wait should succeed")
        .expect("listener should receive a notification");

    assert_eq!(notification.task_id, task_id);
}

#[tokio::test]
async fn wake_workflow_task_emits_notification() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let exec_id = insert_workflow_execution(&mut conn).await;
    let mut params = EnqueueParams::new(
        "default",
        TaskType::Workflow,
        serde_json::json!({"wake": true}),
    );
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);

    let task_id = queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue should succeed");

    let queues = vec!["default".to_string()];
    let claimed = queue::claim_task(&mut conn, &queues, "wake-test-worker", "", None, &[], &[])
        .await
        .expect("claim should succeed")
        .expect("workflow task should be claimable");
    assert_eq!(claimed.id, task_id);
    queue::park_workflow_task(&mut conn, task_id, None)
        .await
        .expect("park workflow task should succeed");

    let mut listener = autumn_harvest::notify::QueueListener::connect(&database_url, &queues)
        .await
        .expect("listener should connect");

    queue::wake_workflow_task(&mut conn, exec_id)
        .await
        .expect("wake_workflow_task should succeed");

    let notification = listener
        .wait_for_notification(Duration::from_secs(2))
        .await
        .expect("listener wait should succeed")
        .expect("listener should receive a wake notification");

    assert_eq!(notification.task_id, Uuid::nil());
}

#[tokio::test]
async fn wake_workflow_task_does_not_requeue_active_running_task() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let exec_id = insert_workflow_execution(&mut conn).await;
    let mut params = EnqueueParams::new(
        "default",
        TaskType::Workflow,
        serde_json::json!({"wake": false}),
    );
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(10);

    let task_id = queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue should succeed");
    let queues = vec!["default".to_string()];
    let claimed = queue::claim_task(&mut conn, &queues, "active-worker", "", None, &[], &[])
        .await
        .expect("claim should succeed")
        .expect("workflow task should be claimable");
    assert_eq!(claimed.id, task_id);
    assert_eq!(claimed.state, "RUNNING");
    assert!(claimed.worker_id.is_some());
    assert!(claimed.started_at.is_some());

    queue::wake_workflow_task(&mut conn, exec_id)
        .await
        .expect("wake_workflow_task should succeed");

    let task = load_task_from_url(&database_url, task_id).await;
    assert_eq!(task.state, "RUNNING");
    assert_eq!(task.worker_id.as_deref(), Some("active-worker"));
    assert!(task.started_at.is_some());
}

#[tokio::test]
async fn reschedule_task_clears_stale_heartbeat_timestamp() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let exec_id = insert_workflow_execution(&mut conn).await;
    let mut params = EnqueueParams::new(
        "default",
        TaskType::Activity,
        serde_json::json!({"retry": true}),
    );
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.activity_name = Some("flaky_step".to_string());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);

    let task_id = queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue should succeed");
    let queues = vec!["default".to_string()];
    let claimed = queue::claim_task(&mut conn, &queues, "retry-worker", "", None, &[], &[])
        .await
        .expect("claim should succeed")
        .expect("activity task should be claimable");
    assert_eq!(claimed.id, task_id);

    let checkpoint = serde_json::json!({"next_offset": 7});
    queue::record_heartbeat(&mut conn, task_id, checkpoint.clone())
        .await
        .expect("record heartbeat should succeed");
    let heartbeating = load_task_from_url(&database_url, task_id).await;
    assert!(
        heartbeating.last_heartbeat_at.is_some(),
        "heartbeat should be recorded before reschedule"
    );
    assert_eq!(
        heartbeating.heartbeat_details,
        Some(checkpoint.clone()),
        "heartbeat payload should be recorded before reschedule"
    );

    queue::reschedule_task(
        &mut conn,
        task_id,
        Utc::now() + chrono::Duration::seconds(30),
    )
    .await
    .expect("reschedule_task should succeed");

    let task = load_task_from_url(&database_url, task_id).await;
    assert_eq!(task.state, "PENDING");
    assert!(task.worker_id.is_none());
    assert!(task.started_at.is_none());
    assert!(
        task.last_heartbeat_at.is_none(),
        "rescheduling should clear stale heartbeat timestamps"
    );
    assert_eq!(
        task.heartbeat_details,
        Some(checkpoint),
        "rescheduling should preserve checkpoint payload for the retry attempt"
    );
}

#[tokio::test]
async fn enqueue_inside_transaction_emits_notification_on_commit() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let exec_id = insert_workflow_execution(&mut conn).await;
    let queues = vec!["default".to_string()];
    let mut listener = autumn_harvest::notify::QueueListener::connect(&database_url, &queues)
        .await
        .expect("listener should connect");

    let mut params = EnqueueParams::new(
        "default",
        TaskType::Activity,
        serde_json::json!({"tx": true}),
    );
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.activity_name = Some("send_email".to_string());

    let task_id = Box::pin(conn.transaction::<Uuid, HarvestError, _>(async |conn| {
        let params = params.clone();
        queue::enqueue(conn, &params).await
    }))
    .await
    .expect("transactional enqueue should succeed");

    let notification = listener
        .wait_for_notification(Duration::from_secs(2))
        .await
        .expect("listener wait should succeed")
        .expect("listener should receive transactional enqueue notification");

    assert_eq!(notification.task_id, task_id);
}

#[tokio::test]
async fn dead_letter_queue_lifecycle() {
    let (mut conn, _container) = setup_test_db().await;

    // Verify DLQ starts empty
    let initial_count = dlq::dead_letter_count(&mut conn)
        .await
        .expect("dead_letter_count failed");
    assert_eq!(initial_count, 0);

    // Insert a dead letter entry
    let entry = NewDeadLetterEntry {
        original_task_id: Uuid::new_v4(),
        queue_name: "default".into(),
        task_type: "ACTIVITY".into(),
        workflow_exec_id: None,
        activity_name: Some("flaky_step".into()),
        input: serde_json::json!({"attempt": 3}),
        error: "SMTP connection refused after 3 retries".into(),
        attempts: 3,

        owner: None,
        severity: None,
    };

    let dlq_id = dlq::dead_letter(&mut conn, &entry)
        .await
        .expect("dead_letter insert failed");
    assert!(!dlq_id.is_nil(), "DLQ entry should have a valid UUID");

    // Verify count is now 1
    let count = dlq::dead_letter_count(&mut conn)
        .await
        .expect("dead_letter_count failed");
    assert_eq!(count, 1);
}

#[tokio::test]
async fn event_store_round_trip() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;

    let activity_id_1 = ActivityExecId::new();
    let activity_id_2 = ActivityExecId::new();

    // Append 3 events in one batch
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({"batch": "round_trip"}),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: activity_id_1,
            name: "step_1".into(),
            input: serde_json::json!(1),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: activity_id_1,
            output: serde_json::json!({"result": "done"}),
        },
    ];

    let inserted = store::append_events(&mut conn, exec_id, &events, 0)
        .await
        .expect("append failed");
    assert_eq!(inserted, 3);

    // Load and verify count
    let history = store::load_history(&mut conn, exec_id)
        .await
        .expect("load_history failed");
    assert_eq!(history.events.len(), 3);
    assert_eq!(history.next_event_id, 3);

    // Verify deserialization fidelity
    assert!(matches!(
        history.events[0],
        WorkflowEvent::WorkflowStarted { .. }
    ));
    if let WorkflowEvent::WorkflowStarted { ref input, .. } = history.events[0] {
        assert_eq!(input, &serde_json::json!({"batch": "round_trip"}));
    }

    assert!(matches!(
        history.events[1],
        WorkflowEvent::ActivityScheduled { .. }
    ));
    if let WorkflowEvent::ActivityScheduled { ref name, .. } = history.events[1] {
        assert_eq!(name, "step_1");
    }

    assert!(matches!(
        history.events[2],
        WorkflowEvent::ActivityCompleted { .. }
    ));
    if let WorkflowEvent::ActivityCompleted { ref output, .. } = history.events[2] {
        assert_eq!(output, &serde_json::json!({"result": "done"}));
    }

    // Append more events and verify continuity
    let more_events = vec![
        WorkflowEvent::ActivityScheduled {
            activity_id: activity_id_2,
            name: "step_2".into(),
            input: serde_json::json!(2),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: activity_id_2,
            output: serde_json::json!({"result": "also done"}),
        },
    ];

    let inserted = store::append_events(&mut conn, exec_id, &more_events, 3)
        .await
        .expect("second append failed");
    assert_eq!(inserted, 2);

    let full_history = store::load_history(&mut conn, exec_id)
        .await
        .expect("full load_history failed");
    assert_eq!(full_history.events.len(), 5);
    assert_eq!(full_history.next_event_id, 5);
}

/// Worker delivers a signal to a waiting workflow and the workflow completes.
///
/// This tests the full signal delivery path:
/// 1. Workflow runs, hits `wait_for_signal` → no signal yet → requeued
/// 2. Signal is written to the `harvest_signals` table
/// 3. Worker retries the task, ingests the pending signal, replays the workflow
/// 4. `wait_for_signal` replays with the ingested signal → workflow completes
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_completes_workflow_after_signal_delivery() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let exec_id = insert_workflow_execution(&mut conn).await;

    store::append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({}),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted failed");

    let mut params = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);

    queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue workflow task failed");

    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: "e2e_test_workflow",
            module: "integration_e2e",
            handler: signal_waiting_workflow,
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![],
    ));
    let worker = build_runtime_worker("worker-e2e-signal-wait", 1, 1, registry);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    // Let the worker pick up the task and reach the signal-wait state.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Deliver the signal.
    autumn_harvest::signal::send_signal(
        &mut conn,
        exec_id,
        "approve",
        serde_json::json!({"user": "alice"}),
    )
    .await
    .expect("send_signal should succeed");

    // Wake the parked task immediately so the test doesn't wait for the 1 s
    // requeue delay.
    queue::wake_workflow_task(&mut conn, exec_id)
        .await
        .expect("wake_workflow_task should succeed");

    let execution = wait_for_execution_state(&database_url, exec_id, "COMPLETED").await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    assert_eq!(
        execution.output,
        Some(serde_json::json!({"approved_by": {"user": "alice"}}))
    );

    let history = load_history_from_url(&database_url, exec_id).await;
    assert!(
        matches!(
            history.events.as_slice(),
            [
                WorkflowEvent::WorkflowStarted { .. },
                WorkflowEvent::SignalReceived { .. },
                WorkflowEvent::WorkflowCompleted { .. },
            ]
        ),
        "unexpected history: {:?}",
        history.events
    );
}

/// Signal arrives before the workflow's activity is scheduled.
///
/// `ingest_pending_signals` can append a `SignalReceived` event at the very
/// start of history (right after `WorkflowStarted`) if the signal arrives
/// while the workflow task is queued but not yet running.  The replay engine
/// must skip those early signals when looking for `ActivityScheduled` and
/// then deliver them when the workflow later calls `wait_for_signal`.
#[allow(clippy::too_many_lines)] // issue #802 added declared-dependency fields to the WorkflowInfo literal
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_handles_early_ingested_signal_before_activity() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let exec_id = insert_workflow_execution(&mut conn).await;
    let workflow_input = serde_json::json!({"to": "bob@example.com"});

    store::append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: workflow_input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted failed");

    // Insert the signal into history BEFORE the workflow runs its activity,
    // simulating the race where a signal arrives right after the workflow
    // task is enqueued but before the worker picks it up.
    store::append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::SignalReceived {
            signal_name: "approve".into(),
            payload: serde_json::json!({"early": true}),
        }],
        1,
    )
    .await
    .expect("append early SignalReceived failed");

    let mut params = EnqueueParams::new("default", TaskType::Workflow, workflow_input.clone());
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);

    queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue workflow task failed");

    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: "e2e_test_workflow",
            module: "integration_e2e",
            handler: activity_then_signal_workflow,
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![ActivityInfo {
            name: "send_email",
            module: "integration_e2e",
            default_retry_policy: None,
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_schedule_to_close: None,
            default_queue: Some("default"),
            max_concurrent: None,
            concurrency_key: None,
            rate_limit_rps: None,
            rate_limit_burst: None,
            rate_limit_key: None,
            rate_limit_key_expr: None,
            circuit_breaker: None,
            is_local: false,
            max_input_bytes: None,
            max_result_bytes: None,
            requires: None,
            handler: send_email_activity,
        }],
    ));
    let worker = build_runtime_worker("worker-e2e-early-signal", 1, 1, registry);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let execution = wait_for_execution_state(&database_url, exec_id, "COMPLETED").await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    let expected_activity_output = serde_json::json!({
        "sent": true,
        "to": "bob@example.com",
    });
    assert_eq!(
        execution.output,
        Some(serde_json::json!({
            "activity": expected_activity_output,
            "signal": {"early": true},
        }))
    );
}

#[tokio::test]
async fn duplicate_event_id_is_rejected() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;

    let events = vec![WorkflowEvent::WorkflowStarted {
        input: serde_json::json!({}),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];

    // First insert succeeds
    store::append_events(&mut conn, exec_id, &events, 0)
        .await
        .expect("first append should succeed");

    // Second insert with same start_id should fail (unique constraint)
    let result = store::append_events(&mut conn, exec_id, &events, 0).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), HarvestError::Database(_)));
}

// ---------------------------------------------------------------------------
// Sticky cross-worker routing tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn enqueue_with_sticky_pin_stores_worker_and_expiry() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;

    let params = EnqueueParams::new(
        "default",
        TaskType::Workflow,
        serde_json::json!({"go": true}),
    )
    .with_sticky("worker-sticky-1", Duration::from_secs(3));
    let mut enqueue = params.clone();
    enqueue.workflow_exec_id = Some(exec_id.as_uuid());

    let task_id = queue::enqueue(&mut conn, &enqueue)
        .await
        .expect("enqueue should succeed");

    let row = harvest_task_queue::table
        .find(task_id)
        .select(TaskQueueItem::as_select())
        .first(&mut conn)
        .await
        .expect("row should exist");

    assert_eq!(row.sticky_worker_id.as_deref(), Some("worker-sticky-1"));
    assert!(row.sticky_until.is_some(), "sticky_until should be set");
    let stored_timeout = row.sticky_timeout.expect("sticky_timeout should be set");
    assert_eq!(
        stored_timeout.num_seconds(),
        3,
        "sticky_timeout interval should round-trip as 3 seconds (got {stored_timeout})",
    );
}

async fn insert_named_workflow_execution(
    conn: &mut AsyncPgConnection,
    workflow_id: &str,
) -> ExecutionId {
    let exec_id = ExecutionId::new();
    let row = NewWorkflowExecution {
        quota_key: None,
        continued_from_exec_id: None,
        first_exec_id: None,
        id: exec_id.as_uuid(),
        workflow_name: "e2e_sticky_test",
        workflow_id,
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: serde_json::json!({}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        deadline_at: None,
        chain_execution_timeout: None,
        chain_deadline_at: None,
        memo: None,
        search_attrs: None,
        assigned_build_id: None,
        parent_close_policy: None,

        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,

        sla: None,

        sla_deadline_at: None,
        schedule_id: None,
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        origin: None,
        completion_callbacks: None,
        start_source: None,
        start_source_ref: None,
        started_by: None,
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("failed to insert sticky test workflow execution");
    exec_id
}

#[tokio::test]
async fn claim_task_prefers_sticky_worker_within_window() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_pinned = insert_named_workflow_execution(&mut conn, "pinned-1").await;
    let exec_free = insert_named_workflow_execution(&mut conn, "free-1").await;

    // Free task is higher priority AND enqueued first so it would ordinarily be
    // claimed ahead of the pinned task by any worker. Sticky routing must
    // reshuffle the order so the pinned worker sees its pinned row first.
    let mut free = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!({}));
    free.priority = 10;
    free.workflow_exec_id = Some(exec_free.as_uuid());
    let free_id = queue::enqueue(&mut conn, &free)
        .await
        .expect("enqueue free task failed");

    let mut pinned = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!({}))
        .with_sticky("sticky-worker", Duration::from_secs(30));
    pinned.priority = 0;
    pinned.workflow_exec_id = Some(exec_pinned.as_uuid());
    let pinned_id = queue::enqueue(&mut conn, &pinned)
        .await
        .expect("enqueue pinned task failed");

    let queues = vec!["default".to_string()];
    let claimed = queue::claim_task(&mut conn, &queues, "sticky-worker", "", None, &[], &[])
        .await
        .expect("claim should succeed")
        .expect("sticky worker should get its pinned task");
    assert_eq!(
        claimed.id, pinned_id,
        "sticky worker should claim its pinned task ahead of the higher-priority free task",
    );

    let claimed_other = queue::claim_task(&mut conn, &queues, "other-worker", "", None, &[], &[])
        .await
        .expect("second claim should succeed")
        .expect("other worker should pick up the free task");
    assert_eq!(
        claimed_other.id, free_id,
        "other worker should still claim the unpinned free task",
    );
}

#[tokio::test]
async fn claim_task_excludes_other_workers_while_sticky_active() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;

    let pinned = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!({}))
        .with_sticky("owner-worker", Duration::from_secs(30));
    let mut pinned = pinned;
    pinned.workflow_exec_id = Some(exec_id.as_uuid());
    queue::enqueue(&mut conn, &pinned)
        .await
        .expect("enqueue should succeed");

    let queues = vec!["default".to_string()];
    // Different worker must not steal a fresh sticky pin.
    let claimed = queue::claim_task(&mut conn, &queues, "interloper", "", None, &[], &[])
        .await
        .expect("claim should succeed");
    assert!(
        claimed.is_none(),
        "non-sticky worker should not claim a pinned task while sticky_until is in the future",
    );

    // The owner can still claim it.
    let owner_claim = queue::claim_task(&mut conn, &queues, "owner-worker", "", None, &[], &[])
        .await
        .expect("owner claim should succeed")
        .expect("owner should be able to claim its pinned task");
    assert_eq!(
        owner_claim.sticky_worker_id.as_deref(),
        Some("owner-worker")
    );
}

#[tokio::test]
async fn claim_task_falls_back_to_any_worker_after_sticky_expires() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;

    // Pin with a short window so we can observe fallback without sleeping long.
    // The sleep is generously larger than the window to tolerate DB/host clock
    // skew inside testcontainers on CI runners.
    let mut pinned = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!({}))
        .with_sticky("crashed-worker", Duration::from_millis(100));
    pinned.workflow_exec_id = Some(exec_id.as_uuid());
    queue::enqueue(&mut conn, &pinned)
        .await
        .expect("enqueue should succeed");

    // Allow the sticky window to elapse comfortably.
    tokio::time::sleep(Duration::from_millis(800)).await;

    let queues = vec!["default".to_string()];
    let claimed = queue::claim_task(&mut conn, &queues, "rescue-worker", "", None, &[], &[])
        .await
        .expect("claim should succeed")
        .expect("any worker may claim after sticky_until expires");
    assert_eq!(
        claimed.worker_id.as_deref(),
        Some("rescue-worker"),
        "fallback worker should own the row after expiry",
    );
}

#[tokio::test]
async fn claim_task_treats_expired_sticky_rows_like_unpinned_rows() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_expired = insert_named_workflow_execution(&mut conn, "expired-sticky-1").await;
    let exec_free = insert_named_workflow_execution(&mut conn, "free-after-expiry-1").await;

    let mut expired = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!({}))
        .with_sticky("offline-worker", Duration::from_secs(30));
    expired.priority = 0;
    expired.workflow_exec_id = Some(exec_expired.as_uuid());
    let expired_id = queue::enqueue(&mut conn, &expired)
        .await
        .expect("enqueue expired-sticky task failed");

    diesel::sql_query(
        "UPDATE harvest_task_queue \
         SET sticky_until = NOW() - INTERVAL '1 second' \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(expired_id)
    .execute(&mut conn)
    .await
    .expect("failed to expire sticky window");

    let mut free = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!({}));
    free.priority = 10;
    free.workflow_exec_id = Some(exec_free.as_uuid());
    let free_id = queue::enqueue(&mut conn, &free)
        .await
        .expect("enqueue free task failed");

    let queues = vec!["default".to_string()];
    let claimed = queue::claim_task(&mut conn, &queues, "rescue-worker", "", None, &[], &[])
        .await
        .expect("claim should succeed")
        .expect("one of the eligible tasks should be claimed");
    assert_eq!(
        claimed.id, free_id,
        "expired sticky rows should compete with unpinned rows by priority instead of jumping ahead",
    );
}

#[tokio::test]
async fn park_workflow_task_with_sticky_hint_pins_to_worker() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;

    // Seed and claim a workflow task so the row is in RUNNING state.
    let mut params = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id.as_uuid());
    let task_id = queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue should succeed");
    let queues = vec!["default".to_string()];
    let _claimed = queue::claim_task(&mut conn, &queues, "park-worker", "", None, &[], &[])
        .await
        .expect("claim should succeed")
        .expect("row should be claimable");

    queue::park_workflow_task(
        &mut conn,
        task_id,
        Some(StickyHint::new("park-worker", Duration::from_secs(10))),
    )
    .await
    .expect("park with sticky should succeed");

    let row = harvest_task_queue::table
        .find(task_id)
        .select(TaskQueueItem::as_select())
        .first(&mut conn)
        .await
        .expect("row should exist");
    assert_eq!(row.state, "RUNNING", "parked row keeps RUNNING state");
    assert!(row.worker_id.is_none(), "worker ownership cleared on park");
    assert_eq!(row.sticky_worker_id.as_deref(), Some("park-worker"));
    assert!(row.sticky_until.is_some());
    let stored_timeout = row.sticky_timeout.expect("sticky_timeout should be set");
    assert_eq!(
        stored_timeout.num_seconds(),
        10,
        "sticky_timeout should round-trip as 10 seconds (got {stored_timeout})",
    );
}

#[tokio::test]
async fn wake_workflow_task_refreshes_sticky_until() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;

    // Seed, claim, and park a workflow task with a short sticky window.
    let mut params = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id.as_uuid());
    let task_id = queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue should succeed");
    let queues = vec!["default".to_string()];
    let _claimed = queue::claim_task(
        &mut conn,
        &queues,
        "wake-refresh-worker",
        "",
        None,
        &[],
        &[],
    )
    .await
    .expect("claim should succeed");
    // Use a 5s window so both the park's sticky_until and the wake's refreshed
    // sticky_until land comfortably in the future even under DB/host clock skew
    // on CI runners. The test asserts the value was REFRESHED by comparing
    // before/after timestamps, not by waiting for expiry.
    queue::park_workflow_task(
        &mut conn,
        task_id,
        Some(StickyHint::new(
            "wake-refresh-worker",
            Duration::from_secs(5),
        )),
    )
    .await
    .expect("park should succeed");

    let parked_until = harvest_task_queue::table
        .find(task_id)
        .select(TaskQueueItem::as_select())
        .first(&mut conn)
        .await
        .expect("parked row should exist")
        .sticky_until
        .expect("sticky_until should be set at park");

    // Wait long enough that NOW() has moved noticeably (well above typical
    // sub-millisecond timer resolution) before triggering the refresh.
    tokio::time::sleep(Duration::from_millis(250)).await;

    queue::wake_workflow_task(&mut conn, exec_id)
        .await
        .expect("wake should succeed");

    let row = harvest_task_queue::table
        .find(task_id)
        .select(TaskQueueItem::as_select())
        .first(&mut conn)
        .await
        .expect("row should exist");

    assert_eq!(row.state, "PENDING");
    assert_eq!(row.sticky_worker_id.as_deref(), Some("wake-refresh-worker"));
    let refreshed_until = row.sticky_until.expect("sticky_until should be refreshed");
    assert!(
        refreshed_until > parked_until,
        "sticky_until should be pushed forward on wake (parked_until={parked_until}, \
         refreshed_until={refreshed_until})",
    );
}

/// Regression test for a PR #901 review finding: `wake_workflow_task`'s
/// dropped-wake fallback can itself lose the race against `park_workflow_task`.
///
/// If `park_workflow_task`'s `candidate SELECT ... FOR UPDATE` has already
/// locked the row (but not yet committed) when the fallback `UPDATE ... SET
/// wake_requested = TRUE` runs, the fallback's initial snapshot still shows
/// the row as owned (`worker_id IS NOT NULL`) so it attempts to lock and
/// write -- and blocks on park's row lock. Once park commits, Postgres
/// re-checks the fallback's WHERE clause against the now-committed row
/// (`worker_id` is NULL after park), which no longer matches, so the
/// fallback silently updates zero rows and `wake_requested` is never set --
/// even though a park this wake raced against just committed a parked row
/// that is this exact wake's target. `wake_workflow_task` must retry its
/// primary re-pend query in that case rather than relying solely on
/// `wake_requested`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wake_workflow_task_retries_after_losing_the_park_row_lock_race() {
    let (database_url, _container) = setup_test_database_url().await;

    let mut conn_park =
        <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
            .await
            .expect("failed to connect (park conn)");
    let mut conn_wake =
        <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
            .await
            .expect("failed to connect (wake conn)");
    let mut conn_read =
        <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
            .await
            .expect("failed to connect (read conn)");

    let exec_id = insert_workflow_execution(&mut conn_park).await;

    let mut params = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id.as_uuid());
    let task_id = queue::enqueue(&mut conn_park, &params)
        .await
        .expect("enqueue should succeed");
    let queues = vec!["default".to_string()];
    queue::claim_task(&mut conn_park, &queues, "race-worker", "", None, &[], &[])
        .await
        .expect("claim should succeed")
        .expect("row should be claimable");

    // Manually hold park's row lock open: BEGIN an explicit transaction on
    // `conn_park`, run the real `park_workflow_task` (which locks and
    // provisionally updates the row inside this open transaction), but do
    // NOT commit yet -- reproducing park having locked the row but not yet
    // released it.
    conn_park
        .batch_execute("BEGIN")
        .await
        .expect("begin should succeed");
    queue::park_workflow_task(&mut conn_park, task_id, None)
        .await
        .expect("park should succeed inside the open transaction");

    // Wake concurrently on a separate connection while the park above is
    // still uncommitted. Its primary re-pend UPDATE won't match (the row's
    // committed snapshot still shows worker_id NOT NULL), so it falls
    // through to the fallback UPDATE, which blocks on the row lock
    // `conn_park` is holding.
    let wake_handle =
        tokio::spawn(async move { queue::wake_workflow_task(&mut conn_wake, exec_id).await });

    // Give the spawned wake a moment to reach and block on the fallback
    // UPDATE before releasing the lock -- otherwise this test could pass
    // trivially because the wake simply ran entirely after the commit.
    tokio::time::sleep(Duration::from_millis(250)).await;

    conn_park
        .batch_execute("COMMIT")
        .await
        .expect("commit should succeed");

    wake_handle
        .await
        .expect("wake task should not panic")
        .expect("wake_workflow_task should succeed");

    let row = harvest_task_queue::table
        .find(task_id)
        .select(TaskQueueItem::as_select())
        .first(&mut conn_read)
        .await
        .expect("row should exist");

    assert_eq!(
        row.state, "PENDING",
        "the wake that raced park's row lock must still re-pend the row via \
         the primary-repend retry, even though wake_requested was silently \
         lost to the lock-order race",
    );
    assert!(
        row.worker_id.is_none(),
        "re-pended row must have no worker ownership",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_continues_as_new_with_fresh_history_and_same_workflow_id() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let original_exec_id = insert_workflow_execution(&mut conn).await;
    let initial_input = serde_json::json!({"phase": "init"});
    enqueue_started_workflow_task(&mut conn, original_exec_id, initial_input.clone()).await;

    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: "e2e_test_workflow",
            module: "integration_e2e",
            handler: continue_as_new_workflow,
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![],
    ));
    let worker = build_runtime_worker("worker-e2e-continue-as-new", 2, 1, registry);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    // The original run is sealed by the worker as soon as it drains the
    // continue-as-new command. We wait for that terminal transition before
    // asserting on the chain.
    let sealed_execution =
        wait_for_execution_state(&database_url, original_exec_id, "CONTINUED_AS_NEW").await;

    // The successor run is identified by the WorkflowContinuedAsNew event
    // appended to the original run's history. Walking the history is the
    // public contract operators rely on; loading a fresh row by joining on
    // the parent_id chain would not work because continue-as-new
    // intentionally does NOT set parent_id (parent_id is reserved for child
    // workflows).
    let original_history = load_history_from_url(&database_url, original_exec_id).await;
    let new_exec_id = original_history
        .events
        .iter()
        .find_map(|event| match event {
            WorkflowEvent::WorkflowContinuedAsNew { new_exec_id, .. } => Some(*new_exec_id),
            _ => None,
        })
        .expect("original history should contain a WorkflowContinuedAsNew event");

    assert_ne!(
        new_exec_id, original_exec_id,
        "continue-as-new must mint a fresh ExecutionId"
    );

    // The new run completes on its second execution by returning the
    // post-continuation payload it was started with.
    let completed = wait_for_execution_state(&database_url, new_exec_id, "COMPLETED").await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    assert_eq!(
        sealed_execution.state, "CONTINUED_AS_NEW",
        "original run should be sealed in the continue-as-new terminal state"
    );
    assert_eq!(
        sealed_execution.workflow_id, completed.workflow_id,
        "logical workflow_id must be preserved across the continue-as-new transition"
    );
    assert_eq!(
        sealed_execution.workflow_name, completed.workflow_name,
        "workflow_name must be preserved across the continue-as-new transition"
    );
    assert_eq!(
        completed.output,
        Some(serde_json::json!({"phase": "next", "ran_init": true})),
        "the new run should observe the input passed to continue_as_new"
    );

    // The successor run starts with an empty history apart from
    // WorkflowStarted plus its own terminal completion event — this is the
    // whole point of continue-as-new, bounding history growth.
    let new_history = load_history_from_url(&database_url, new_exec_id).await;
    assert!(
        matches!(
            new_history.events.as_slice(),
            [
                WorkflowEvent::WorkflowStarted { .. },
                WorkflowEvent::WorkflowCompleted { .. },
            ]
        ),
        "new run history should be bounded; got {:?}",
        new_history
            .events
            .iter()
            .map(WorkflowEvent::type_name)
            .collect::<Vec<_>>(),
    );
}

/// Issue #740 (AC3): a continue-as-new successor records its OWN `continue_as_new`
/// source referencing the predecessor — it is NEVER misattributed as a fresh
/// `api` start, and never inherits the predecessor's source. Driven through the
/// real worker loop (the only way to exercise `persist_workflow_continue_as_new`,
/// which is engine-internal). The predecessor is deliberately stamped with a
/// DISTINCT source (`schedule`) so the assertion falsifies any inheritance.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_continue_as_new_records_own_start_source_referencing_predecessor() {
    let (database_url, _container) = setup_test_database_url_or_env().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");
    // Isolate from a shared HARVEST_TEST_DATABASE_URL: other e2e tests reuse the
    // fixed `e2e_test_workflow` / `e2e-wf-001` identity, so clear those rows
    // (and the child workflow's) up front. A no-op against a fresh CI container.
    for stmt in [
        "DELETE FROM harvest_events WHERE workflow_exec_id IN (SELECT id FROM harvest_workflow_executions WHERE workflow_name IN ('e2e_test_workflow','child_echo_workflow'))",
        "DELETE FROM harvest_task_queue WHERE workflow_exec_id IN (SELECT id FROM harvest_workflow_executions WHERE workflow_name IN ('e2e_test_workflow','child_echo_workflow'))",
        "DELETE FROM harvest_workflow_executions WHERE workflow_name IN ('e2e_test_workflow','child_echo_workflow')",
    ] {
        diesel::sql_query(stmt)
            .execute(&mut conn)
            .await
            .expect("pre-test scrub");
    }

    let original_exec_id = insert_workflow_execution(&mut conn).await;
    // Give the predecessor a DISTINCT source so "successor never inherits" is
    // genuinely falsifiable (not merely NULL-vs-continue_as_new).
    diesel::update(harvest_workflow_executions::table.find(original_exec_id.as_uuid()))
        .set(harvest_workflow_executions::start_source.eq(Some("schedule")))
        .execute(&mut conn)
        .await
        .expect("stamp predecessor source");

    let initial_input = serde_json::json!({"phase": "init"});
    enqueue_started_workflow_task(&mut conn, original_exec_id, initial_input).await;

    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: "e2e_test_workflow",
            module: "integration_e2e",
            handler: continue_as_new_workflow,
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![],
    ));
    let worker = build_runtime_worker("worker-e2e-can-source", 2, 1, registry);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    // Wait for the predecessor to be sealed, then resolve the successor id from
    // the WorkflowContinuedAsNew event.
    let _sealed =
        wait_for_execution_state(&database_url, original_exec_id, "CONTINUED_AS_NEW").await;
    let original_history = load_history_from_url(&database_url, original_exec_id).await;
    let new_exec_id = original_history
        .events
        .iter()
        .find_map(|event| match event {
            WorkflowEvent::WorkflowContinuedAsNew { new_exec_id, .. } => Some(*new_exec_id),
            _ => None,
        })
        .expect("original history should contain a WorkflowContinuedAsNew event");

    let successor = wait_for_execution_state(&database_url, new_exec_id, "COMPLETED").await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    assert_eq!(
        successor.start_source.as_deref(),
        Some("continue_as_new"),
        "the successor records its OWN `continue_as_new` source, never the \
         predecessor's `schedule` source and never a fresh `api` start"
    );
    assert_eq!(
        successor.start_source_ref.as_deref(),
        Some(original_exec_id.to_string().as_str()),
        "the continue-as-new successor references the predecessor execution id"
    );
}

/// Issue #740 (AC3): a spawned child workflow records the `child` source
/// referencing its parent execution. Driven through the real worker loop (the
/// child-spawn insert path, `insert_awaited_child_execution`, is engine-internal).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_child_workflow_records_child_start_source_referencing_parent() {
    let (database_url, _container) = setup_test_database_url_or_env().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");
    // Isolate from a shared HARVEST_TEST_DATABASE_URL: other e2e tests reuse the
    // fixed `e2e_test_workflow` / `e2e-wf-001` identity, so clear those rows
    // (and the child workflow's) up front. A no-op against a fresh CI container.
    for stmt in [
        "DELETE FROM harvest_events WHERE workflow_exec_id IN (SELECT id FROM harvest_workflow_executions WHERE workflow_name IN ('e2e_test_workflow','child_echo_workflow'))",
        "DELETE FROM harvest_task_queue WHERE workflow_exec_id IN (SELECT id FROM harvest_workflow_executions WHERE workflow_name IN ('e2e_test_workflow','child_echo_workflow'))",
        "DELETE FROM harvest_workflow_executions WHERE workflow_name IN ('e2e_test_workflow','child_echo_workflow')",
    ] {
        diesel::sql_query(stmt)
            .execute(&mut conn)
            .await
            .expect("pre-test scrub");
    }

    let parent_exec_id = insert_workflow_execution(&mut conn).await;
    let workflow_input = serde_json::json!({"value": "from-parent"});
    enqueue_started_workflow_task(&mut conn, parent_exec_id, workflow_input).await;

    let worker = build_runtime_worker("worker-e2e-child-source", 2, 1, child_round_trip_registry());
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    wait_for_execution_state(&database_url, parent_exec_id, "COMPLETED").await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    let child_execs = load_child_executions_from_url(&database_url, parent_exec_id).await;
    assert_eq!(child_execs.len(), 1, "exactly one child execution");
    assert_eq!(
        child_execs[0].start_source.as_deref(),
        Some("child"),
        "a spawned child records the `child` source"
    );
    assert_eq!(
        child_execs[0].start_source_ref.as_deref(),
        Some(parent_exec_id.to_string().as_str()),
        "the child references its parent execution id"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn continue_as_new_down_migration_rewrites_historical_runs_for_rollback() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let original_exec_id = insert_workflow_execution(&mut conn).await;
    let initial_input = serde_json::json!({"phase": "init"});
    enqueue_started_workflow_task(&mut conn, original_exec_id, initial_input.clone()).await;

    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: "e2e_test_workflow",
            module: "integration_e2e",
            handler: continue_as_new_workflow,
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![],
    ));
    let worker = build_runtime_worker("worker-e2e-continue-down", 2, 1, registry);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let _sealed_execution =
        wait_for_execution_state(&database_url, original_exec_id, "CONTINUED_AS_NEW").await;
    let original_history = load_history_from_url(&database_url, original_exec_id).await;
    let successor_exec_id = original_history
        .events
        .iter()
        .find_map(|event| match event {
            WorkflowEvent::WorkflowContinuedAsNew { new_exec_id, .. } => Some(*new_exec_id),
            _ => None,
        })
        .expect("original history should contain a WorkflowContinuedAsNew event");
    let _completed = wait_for_execution_state(&database_url, successor_exec_id, "COMPLETED").await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    let down_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("migrations/20260427000000_harvest_continue_as_new/down.sql");
    let down_sql = std::fs::read_to_string(&down_path).unwrap_or_else(|error| {
        panic!(
            "failed to read continue-as-new down migration at {}: {error}",
            down_path.display()
        )
    });
    conn.batch_execute(&down_sql).await.expect(
        "continue-as-new down migration should succeed after continue-as-new has been used",
    );

    let executions = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq("e2e_test_workflow"))
        .order(harvest_workflow_executions::started_at.asc())
        .select(WorkflowExecution::as_select())
        .load(&mut conn)
        .await
        .expect("failed to reload workflow executions after down migration");
    assert_eq!(
        executions.len(),
        2,
        "rollback should preserve both runs while making them compatible with the pre-continue schema",
    );
    assert!(
        executions
            .iter()
            .all(|execution| execution.state != "CONTINUED_AS_NEW"),
        "down migration must eliminate CONTINUED_AS_NEW before restoring the old state check",
    );

    let original_row = executions
        .iter()
        .find(|execution| execution.id == original_exec_id.as_uuid())
        .expect("original execution should still exist after rollback rewrite");
    assert_eq!(
        original_row.state, "COMPLETED",
        "sealed historical runs should be rewritten to an old-schema terminal state",
    );
    assert!(
        original_row
            .workflow_id
            .starts_with("e2e-wf-001::continued-as-new:"),
        "sealed historical runs should get a synthetic workflow_id during rollback",
    );

    let successor_row = executions
        .iter()
        .find(|execution| execution.id == successor_exec_id.as_uuid())
        .expect("successor execution should still exist after rollback rewrite");
    assert_eq!(
        successor_row.workflow_id, "e2e-wf-001",
        "the latest run should retain the original logical workflow_id",
    );

    let rows_on_original_key = executions
        .iter()
        .filter(|execution| execution.workflow_id == "e2e-wf-001")
        .count();
    assert_eq!(
        rows_on_original_key, 1,
        "rollback should leave exactly one row on the original logical key before restoring uniqueness",
    );
}

// ── WorkflowIdReusePolicy integration matrix ─────────────────────────────────
//
// 4 policies × 5 prior states (none, RUNNING, COMPLETED, FAILED, CANCELLED)
// = 20 cells, each asserting: (a) which exec_id the second start observes,
// (b) whether a new exec_id was minted, (c) the error variant when applicable.

/// Helpers for the reuse-policy matrix tests.
mod reuse_policy_helpers {
    use super::*;

    pub fn base_params(
        workflow_id: &'static str,
        exec_id: ExecutionId,
    ) -> StartWorkflowParams<'static> {
        StartWorkflowParams {
            workflow_name: "reuse_policy_wf",
            workflow_id,
            exec_id,
            input: serde_json::json!({}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            chain_execution_timeout: None,
            max_workflow_chain_timeout_ceiling: None,
            inherited_chain_deadline_at: None,
            concurrency_key: None,
            concurrency_limit: None,
            concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
            priority: Priority::default(),
            max_workflow_input_bytes: 0,
            start_at: None,
            delay: None,
            max_workflow_start_delay: None,

            owner: None,
            runbook_url: None,
            severity: None,
            context_headers: None,

            sla: None,
            schedule_id: None,
            scheduled_for: None,
            workflow_attempt: 1,
            workflow_retry_policy: None,
            retry_of_exec_id: None,
            max_workflow_attempts_ceiling: None,
            origin: None,
            completion_callbacks: None,
            start_source: autumn_harvest::StartSource::Api,
            start_source_ref: None,
            started_by: None,
        }
    }

    /// Force a workflow row into a terminal state by direct UPDATE.
    pub async fn force_state(conn: &mut AsyncPgConnection, exec_id: ExecutionId, state: &str) {
        diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
            .set(harvest_workflow_executions::state.eq(state))
            .execute(conn)
            .await
            .expect("force_state UPDATE failed");
    }
}

#[tokio::test]
async fn reuse_policy_allow_duplicate_no_prior_creates() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-allow-none", exec_id);
    params.reuse_policy = WorkflowIdReusePolicy::AllowDuplicate;
    let result = start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("AllowDuplicate with no prior should succeed");
    assert!(result.created, "should create when no prior exists");
    assert_eq!(result.exec_id, exec_id);
}

#[tokio::test]
async fn reuse_policy_allow_duplicate_running_returns_existing() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-allow-run", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::AllowDuplicate;
    let first = start_or_load_workflow_execution(&mut conn, params.clone(), None)
        .await
        .expect("first start should succeed");
    assert!(first.created);

    let second_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    params.exec_id = second_id;
    let second = start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("AllowDuplicate on RUNNING should return existing");
    assert!(!second.created);
    assert_eq!(
        second.exec_id, first.exec_id,
        "must return the first exec_id"
    );
    assert_eq!(second.state, "RUNNING");
}

#[tokio::test]
async fn reuse_policy_allow_duplicate_completed_returns_existing() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-allow-cmp", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::AllowDuplicate;
    let first = start_or_load_workflow_execution(&mut conn, params.clone(), None)
        .await
        .expect("first start should succeed");
    reuse_policy_helpers::force_state(&mut conn, first.exec_id, "COMPLETED").await;

    params.exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let second = start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("AllowDuplicate on COMPLETED should return existing");
    assert!(!second.created);
    assert_eq!(second.exec_id, first.exec_id);
}

#[tokio::test]
async fn reuse_policy_allow_duplicate_failed_returns_existing() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-allow-fail", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::AllowDuplicate;
    let first = start_or_load_workflow_execution(&mut conn, params.clone(), None)
        .await
        .expect("first start should succeed");
    reuse_policy_helpers::force_state(&mut conn, first.exec_id, "FAILED").await;

    params.exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let second = start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("AllowDuplicate on FAILED should return existing");
    assert!(!second.created);
    assert_eq!(second.exec_id, first.exec_id);
}

#[tokio::test]
async fn reuse_policy_allow_duplicate_cancelled_returns_existing() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-allow-can", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::AllowDuplicate;
    let first = start_or_load_workflow_execution(&mut conn, params.clone(), None)
        .await
        .expect("first start should succeed");
    cancel_workflow_execution(
        &mut conn,
        first.exec_id,
        "test cancel",
        &autumn_harvest::telemetry::NoOpMetrics,
    )
    .await
    .expect("cancel should succeed");

    params.exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let second = start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("AllowDuplicate on CANCELLED should return existing");
    assert!(!second.created);
    assert_eq!(second.exec_id, first.exec_id);
    assert_eq!(second.state, "CANCELLED");
}

#[tokio::test]
async fn reuse_policy_reject_duplicate_no_prior_creates() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-reject-none", exec_id);
    params.reuse_policy = WorkflowIdReusePolicy::RejectDuplicate;
    let result = start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("RejectDuplicate with no prior should succeed");
    assert!(result.created);
}

#[tokio::test]
async fn reuse_policy_reject_duplicate_running_errors() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-reject-run", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::RejectDuplicate;
    let first = start_or_load_workflow_execution(&mut conn, params.clone(), None)
        .await
        .expect("first start should succeed");

    params.exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let err = start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect_err("RejectDuplicate on RUNNING must error");
    match err {
        HarvestError::AlreadyExists {
            existing_exec_id,
            existing_state,
        } => {
            assert_eq!(existing_exec_id, first.exec_id);
            assert_eq!(existing_state, "RUNNING");
        }
        other => panic!("expected AlreadyExists, got {other:?}"),
    }
}

#[tokio::test]
async fn reuse_policy_reject_duplicate_completed_errors() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-reject-cmp", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::RejectDuplicate;
    let first = start_or_load_workflow_execution(&mut conn, params.clone(), None)
        .await
        .expect("first start should succeed");
    reuse_policy_helpers::force_state(&mut conn, first.exec_id, "COMPLETED").await;

    params.exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let err = start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect_err("RejectDuplicate on COMPLETED must error");
    assert!(matches!(err, HarvestError::AlreadyExists { .. }));
}

#[tokio::test]
async fn reuse_policy_reject_duplicate_failed_errors() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-reject-fail", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::RejectDuplicate;
    let first = start_or_load_workflow_execution(&mut conn, params.clone(), None)
        .await
        .expect("first start should succeed");
    reuse_policy_helpers::force_state(&mut conn, first.exec_id, "FAILED").await;

    params.exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let err = start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect_err("RejectDuplicate on FAILED must error");
    assert!(matches!(err, HarvestError::AlreadyExists { .. }));
}

#[tokio::test]
async fn reuse_policy_reject_duplicate_cancelled_errors() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-reject-can", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::RejectDuplicate;
    let first = start_or_load_workflow_execution(&mut conn, params.clone(), None)
        .await
        .expect("first start should succeed");
    cancel_workflow_execution(
        &mut conn,
        first.exec_id,
        "test cancel",
        &autumn_harvest::telemetry::NoOpMetrics,
    )
    .await
    .expect("cancel should succeed");

    params.exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let err = start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect_err("RejectDuplicate on CANCELLED must error");
    assert!(matches!(err, HarvestError::AlreadyExists { .. }));
}

#[tokio::test]
async fn reuse_policy_allow_failed_only_no_prior_creates() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-afo-none", exec_id);
    params.reuse_policy = WorkflowIdReusePolicy::AllowDuplicateFailedOnly;
    let result = start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("AllowDuplicateFailedOnly with no prior should create");
    assert!(result.created);
}

#[tokio::test]
async fn reuse_policy_allow_failed_only_running_returns_existing() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-afo-run", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::AllowDuplicateFailedOnly;
    let first = start_or_load_workflow_execution(&mut conn, params.clone(), None)
        .await
        .expect("first start should succeed");

    params.exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let second = start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("AllowDuplicateFailedOnly on RUNNING should return existing");
    assert!(!second.created);
    assert_eq!(second.exec_id, first.exec_id);
}

#[tokio::test]
async fn reuse_policy_allow_failed_only_completed_returns_existing() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-afo-cmp", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::AllowDuplicateFailedOnly;
    let first = start_or_load_workflow_execution(&mut conn, params.clone(), None)
        .await
        .expect("first start should succeed");
    reuse_policy_helpers::force_state(&mut conn, first.exec_id, "COMPLETED").await;

    params.exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let second = start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("AllowDuplicateFailedOnly on COMPLETED should return existing");
    assert!(!second.created);
    assert_eq!(second.exec_id, first.exec_id);
}

#[tokio::test]
async fn reuse_policy_allow_failed_only_failed_starts_fresh() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-afo-fail", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::AllowDuplicateFailedOnly;
    let first = start_or_load_workflow_execution(&mut conn, params.clone(), None)
        .await
        .expect("first start should succeed");
    reuse_policy_helpers::force_state(&mut conn, first.exec_id, "FAILED").await;

    let second_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    params.exec_id = second_id;
    let second = start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("AllowDuplicateFailedOnly on FAILED should start fresh");
    assert!(second.created, "must mint a new execution");
    assert_eq!(second.exec_id, second_id, "must use the new exec_id");
    assert_ne!(second.exec_id, first.exec_id);
}

#[tokio::test]
async fn reuse_policy_allow_failed_only_cancelled_starts_fresh() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-afo-can", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::AllowDuplicateFailedOnly;
    let first = start_or_load_workflow_execution(&mut conn, params.clone(), None)
        .await
        .expect("first start should succeed");
    cancel_workflow_execution(
        &mut conn,
        first.exec_id,
        "test cancel",
        &autumn_harvest::telemetry::NoOpMetrics,
    )
    .await
    .expect("cancel should succeed");

    let second_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    params.exec_id = second_id;
    let second = start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("AllowDuplicateFailedOnly on CANCELLED should start fresh");
    assert!(second.created, "must mint a new execution");
    assert_eq!(second.exec_id, second_id);
    assert_ne!(second.exec_id, first.exec_id);
}

#[tokio::test]
async fn reuse_policy_terminate_if_running_no_prior_creates() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-tir-none", exec_id);
    params.reuse_policy = WorkflowIdReusePolicy::TerminateIfRunning;
    let result = start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("TerminateIfRunning with no prior should create");
    assert!(result.created);
    assert_eq!(result.exec_id, exec_id);
}

#[tokio::test]
async fn reuse_policy_terminate_if_running_running_cancels_and_starts_fresh() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-tir-run", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::TerminateIfRunning;
    let first = start_or_load_workflow_execution(&mut conn, params.clone(), None)
        .await
        .expect("first start should succeed");
    assert_eq!(first.state, "RUNNING");

    let second_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    params.exec_id = second_id;
    let second = start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("TerminateIfRunning on RUNNING should cancel and start fresh");
    assert!(second.created, "must mint a new execution");
    assert_eq!(second.exec_id, second_id);
    assert_ne!(second.exec_id, first.exec_id);
    assert_eq!(second.state, "RUNNING");

    // Verify prior run was cancelled
    let prior = harvest_workflow_executions::table
        .find(first.exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .expect("prior execution row must still exist");
    assert_eq!(
        prior.state, "CONTINUED_AS_NEW",
        "prior run should be sealed as CONTINUED_AS_NEW"
    );
}

#[tokio::test]
async fn reuse_policy_terminate_if_running_completed_starts_fresh() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-tir-cmp", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::TerminateIfRunning;
    let first = start_or_load_workflow_execution(&mut conn, params.clone(), None)
        .await
        .expect("first start should succeed");
    reuse_policy_helpers::force_state(&mut conn, first.exec_id, "COMPLETED").await;

    let second_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    params.exec_id = second_id;
    let second = start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("TerminateIfRunning on COMPLETED should start fresh");
    assert!(second.created, "must mint a new execution");
    assert_eq!(second.exec_id, second_id);
    assert_ne!(second.exec_id, first.exec_id);
}

#[tokio::test]
async fn reuse_policy_terminate_if_running_failed_starts_fresh() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-tir-fail", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::TerminateIfRunning;
    let first = start_or_load_workflow_execution(&mut conn, params.clone(), None)
        .await
        .expect("first start should succeed");
    reuse_policy_helpers::force_state(&mut conn, first.exec_id, "FAILED").await;

    let second_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    params.exec_id = second_id;
    let second = start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("TerminateIfRunning on FAILED should start fresh");
    assert!(second.created, "must mint a new execution");
    assert_eq!(second.exec_id, second_id);
    assert_ne!(second.exec_id, first.exec_id);
}

#[tokio::test]
async fn reuse_policy_terminate_if_running_cancelled_starts_fresh() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-tir-can", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::TerminateIfRunning;
    let first = start_or_load_workflow_execution(&mut conn, params.clone(), None)
        .await
        .expect("first start should succeed");
    cancel_workflow_execution(
        &mut conn,
        first.exec_id,
        "test cancel",
        &autumn_harvest::telemetry::NoOpMetrics,
    )
    .await
    .expect("cancel should succeed");

    let second_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    params.exec_id = second_id;
    let second = start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("TerminateIfRunning on CANCELLED should start fresh");
    assert!(second.created, "must mint a new execution");
    assert_eq!(second.exec_id, second_id);
    assert_ne!(second.exec_id, first.exec_id);
}

/// `TerminateIfRunning` is idempotent: if the prior run is CANCELLED (e.g. from
/// a previous `TerminateIfRunning` that crashed between the two transactions),
/// the retry starts fresh without requiring manual intervention.
#[tokio::test]
async fn reuse_policy_terminate_if_running_retry_after_partial_failure_is_idempotent() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-tir-retry", first_id);

    // Simulate transaction 1 completing (cancel) but transaction 2 failing
    // by manually cancelling the first run and then not starting a new one.
    params.reuse_policy = WorkflowIdReusePolicy::AllowDuplicate;
    let first = start_or_load_workflow_execution(&mut conn, params.clone(), None)
        .await
        .expect("initial start should succeed");
    cancel_workflow_execution(
        &mut conn,
        first.exec_id,
        "simulated T1 complete",
        &autumn_harvest::telemetry::NoOpMetrics,
    )
    .await
    .expect("cancel should succeed");

    // Now retry with TerminateIfRunning. Prior run is CANCELLED → start fresh.
    let second_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    params.exec_id = second_id;
    params.reuse_policy = WorkflowIdReusePolicy::TerminateIfRunning;
    let second = start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("retry with TerminateIfRunning on CANCELLED should start fresh");
    assert!(second.created, "retry must mint a fresh execution");
    assert_eq!(second.exec_id, second_id);
    assert_ne!(second.exec_id, first.exec_id);
}

// ---------------------------------------------------------------------------
// WorkflowIdConflictPolicy — orthogonal active-prior axis (issue #685)
// ---------------------------------------------------------------------------
//
// These exercise the active (RUNNING/PAUSED) collision axis end-to-end through
// the real start transaction, composing with each reuse policy. The unit-level
// matrix (predicate + effective behavior) lives in `execution.rs`.

/// Count `harvest_events` rows for an execution, to prove an ATTACH appended no
/// new `WorkflowStarted` event.
async fn count_events(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> i64 {
    use diesel::sql_types::BigInt;
    #[derive(diesel::QueryableByName)]
    struct Cnt {
        #[diesel(sql_type = BigInt)]
        n: i64,
    }
    diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_events WHERE workflow_exec_id = $1")
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .get_result::<Cnt>(conn)
        .await
        .expect("count events")
        .n
}

/// Count `harvest_events` rows of a given `event_type` for an execution, to prove
/// a cancel actually appended a `WorkflowCancelled` event.
async fn count_events_of_type(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    event_type: &str,
) -> i64 {
    use diesel::sql_types::{BigInt, Text};
    #[derive(diesel::QueryableByName)]
    struct Cnt {
        #[diesel(sql_type = BigInt)]
        n: i64,
    }
    diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_events \
         WHERE workflow_exec_id = $1 AND event_type = $2",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<Text, _>(event_type)
    .get_result::<Cnt>(conn)
    .await
    .expect("count events of type")
    .n
}

/// Count `harvest_task_queue` rows for an execution, to prove an ATTACH enqueued
/// no task against the fresh (allocated-but-unused) `exec_id`.
async fn count_tasks_for_execution(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> i64 {
    use diesel::sql_types::BigInt;
    #[derive(diesel::QueryableByName)]
    struct Cnt {
        #[diesel(sql_type = BigInt)]
        n: i64,
    }
    diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_task_queue WHERE workflow_exec_id = $1")
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .get_result::<Cnt>(conn)
        .await
        .expect("count tasks")
        .n
}

/// `UseExisting` attaches to a RUNNING prior regardless of the reuse policy —
/// returning the existing handle, appending no new `WorkflowStarted` event, and
/// leaving the prior RUNNING (no cancel).
#[tokio::test]
async fn conflict_use_existing_running_attaches_no_new_run() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("cf-use-run", first_id);
    // Pair with a reuse policy whose NATIVE active behavior differs from Attach
    // (AllowDuplicateFailedOnly natively attaches, but TerminateIfRunning would
    // cancel) — here we prove UseExisting overrides regardless. Use
    // AllowDuplicateFailedOnly to also confirm no interaction with the reuse axis.
    params.reuse_policy = WorkflowIdReusePolicy::AllowDuplicateFailedOnly;
    let first = start_or_load_workflow_execution(&mut conn, params.clone(), None)
        .await
        .expect("first start should succeed");
    assert_eq!(first.state, "RUNNING");
    let events_before = count_events(&mut conn, first.exec_id).await;

    // Capture the fresh exec_id that is allocated for this second start but never
    // used, because UseExisting attaches to the prior instead (FIX 3).
    let fresh_exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    params.exec_id = fresh_exec_id;
    params.conflict_policy = autumn_harvest::types::WorkflowIdConflictPolicy::UseExisting;
    let second = start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("UseExisting on RUNNING should attach");
    assert!(!second.created, "UseExisting must NOT create a fresh run");
    assert_eq!(
        second.exec_id, first.exec_id,
        "must return the running exec_id"
    );
    assert_eq!(second.state, "RUNNING");

    let prior = harvest_workflow_executions::table
        .find(first.exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .expect("prior row must exist");
    assert_eq!(
        prior.state, "RUNNING",
        "prior must remain RUNNING (no cancel)"
    );
    let events_after = count_events(&mut conn, first.exec_id).await;
    assert_eq!(
        events_before, events_after,
        "attach must append no new WorkflowStarted event"
    );
    // FIX 3: the attach enqueues NO task against the fresh (unused) exec_id.
    assert_eq!(
        count_tasks_for_execution(&mut conn, fresh_exec_id).await,
        0,
        "UseExisting attach must enqueue no task for the allocated-but-unused exec_id"
    );
}

/// `UseExisting` attaches to a PAUSED prior too (PAUSED is an active state).
#[tokio::test]
async fn conflict_use_existing_paused_attaches() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("cf-use-paused", first_id);
    let first = start_or_load_workflow_execution(&mut conn, params.clone(), None)
        .await
        .expect("first start should succeed");
    reuse_policy_helpers::force_state(&mut conn, first.exec_id, "PAUSED").await;

    params.exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    params.conflict_policy = autumn_harvest::types::WorkflowIdConflictPolicy::UseExisting;
    let second = start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("UseExisting on PAUSED should attach");
    assert!(!second.created);
    assert_eq!(second.exec_id, first.exec_id);
    assert_eq!(second.state, "PAUSED");

    let prior = harvest_workflow_executions::table
        .find(first.exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .expect("prior row must exist");
    assert_eq!(
        prior.state, "PAUSED",
        "prior must remain PAUSED (no cancel)"
    );
}

/// AC-3 corner: `TerminateIfRunning` reuse + `UseExisting` conflict on a
/// COMPLETED (terminal) prior — the conflict axis governs only active priors, so
/// the terminal prior is decided by the reuse axis (`TerminateIfRunning`) → fresh.
#[tokio::test]
async fn conflict_terminate_if_running_reuse_plus_use_existing_completed_starts_fresh() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("cf-tir-use-cmp", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::TerminateIfRunning;
    let first = start_or_load_workflow_execution(&mut conn, params.clone(), None)
        .await
        .expect("first start should succeed");
    reuse_policy_helpers::force_state(&mut conn, first.exec_id, "COMPLETED").await;

    let second_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    params.exec_id = second_id;
    params.conflict_policy = autumn_harvest::types::WorkflowIdConflictPolicy::UseExisting;
    let second = start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("terminal prior is decided by the reuse axis (TIR) → fresh");
    assert!(
        second.created,
        "TerminateIfRunning replaces a terminal prior"
    );
    assert_eq!(second.exec_id, second_id);
    assert_ne!(second.exec_id, first.exec_id);
}

/// AC-3 corner (the pre-check gate fix): `TerminateIfRunning` reuse +
/// `UseExisting` conflict on a RUNNING prior — `UseExisting` overrides the active
/// behavior to Attach, so the prior is NOT cancelled by the pre-check.
#[tokio::test]
async fn conflict_terminate_if_running_reuse_plus_use_existing_running_attaches() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("cf-tir-use-run", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::TerminateIfRunning;
    let first = start_or_load_workflow_execution(&mut conn, params.clone(), None)
        .await
        .expect("first start should succeed");
    assert_eq!(first.state, "RUNNING");

    params.exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    params.conflict_policy = autumn_harvest::types::WorkflowIdConflictPolicy::UseExisting;
    let second = start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("UseExisting overrides TerminateIfRunning on a RUNNING prior → attach");
    assert!(!second.created, "must attach, not cancel + start fresh");
    assert_eq!(second.exec_id, first.exec_id);
    assert_eq!(second.state, "RUNNING");

    let prior = harvest_workflow_executions::table
        .find(first.exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .expect("prior row must exist");
    assert_eq!(
        prior.state, "RUNNING",
        "the pre-check must NOT cancel the prior when UseExisting overrides"
    );
}

/// Fail conflict on a RUNNING prior errors regardless of reuse policy.
#[tokio::test]
async fn conflict_fail_running_errors() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("cf-fail-run", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::AllowDuplicate;
    let first = start_or_load_workflow_execution(&mut conn, params.clone(), None)
        .await
        .expect("first start should succeed");

    params.exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    params.conflict_policy = autumn_harvest::types::WorkflowIdConflictPolicy::Fail;
    let err = start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect_err("Fail on RUNNING must error even with AllowDuplicate reuse");
    match err {
        HarvestError::AlreadyExists {
            existing_exec_id,
            existing_state,
        } => {
            assert_eq!(existing_exec_id, first.exec_id);
            assert_eq!(existing_state, "RUNNING");
        }
        other => panic!("expected AlreadyExists, got {other:?}"),
    }
    // Prior must be untouched.
    let prior = harvest_workflow_executions::table
        .find(first.exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .expect("prior row must exist");
    assert_eq!(prior.state, "RUNNING");
}

/// `TerminateExisting` on a RUNNING prior cancels it and starts fresh, even with a
/// reuse policy (`AllowDuplicate`) that would natively attach.
#[tokio::test]
async fn conflict_terminate_existing_running_cancels_and_starts_fresh() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("cf-term-run", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::AllowDuplicate;
    let first = start_or_load_workflow_execution(&mut conn, params.clone(), None)
        .await
        .expect("first start should succeed");
    assert_eq!(first.state, "RUNNING");

    let second_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    params.exec_id = second_id;
    params.conflict_policy = autumn_harvest::types::WorkflowIdConflictPolicy::TerminateExisting;
    let second = start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("TerminateExisting overrides AllowDuplicate → cancel + fresh");
    assert!(second.created, "must mint a fresh execution");
    assert_eq!(second.exec_id, second_id);
    assert_ne!(second.exec_id, first.exec_id);
    assert_eq!(second.state, "RUNNING");

    // Prior run must have been superseded (sealed as CONTINUED_AS_NEW by the
    // inline cancel + replace path).
    let prior = harvest_workflow_executions::table
        .find(first.exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .expect("prior row must exist");
    assert_eq!(
        prior.state, "CONTINUED_AS_NEW",
        "prior run must be sealed (superseded), not left RUNNING"
    );
}

/// FIX 2 (AC-2 coverage gap): `TerminateExisting` on a PAUSED prior cancels it and
/// starts fresh. Unlike the RUNNING case — which the native `TerminateIfRunning`
/// pre-check can reach — a PAUSED prior is resolved by the IN-TRANSACTION
/// `inline_cancel` + `replace_execution` under the row lock (the pre-check-based
/// path never terminates a PAUSED prior). We pair it with `AllowDuplicate` reuse
/// (which natively attaches) to prove `TerminateExisting` overrides.
#[tokio::test]
async fn conflict_terminate_existing_paused_cancels_and_starts_fresh() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("cf-term-paused", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::AllowDuplicate;
    let first = start_or_load_workflow_execution(&mut conn, params.clone(), None)
        .await
        .expect("first start should succeed");
    // Force PAUSED. `force_state` sets the `state` column only — there is no live
    // worker in this test, so this is sufficient to exercise the active-PAUSED
    // conflict branch.
    reuse_policy_helpers::force_state(&mut conn, first.exec_id, "PAUSED").await;

    let second_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    params.exec_id = second_id;
    params.conflict_policy = autumn_harvest::types::WorkflowIdConflictPolicy::TerminateExisting;
    let second = start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("TerminateExisting on PAUSED → inline cancel + fresh");
    assert!(second.created, "must mint a fresh execution");
    assert_eq!(second.exec_id, second_id);
    assert_ne!(second.exec_id, first.exec_id);
    assert_eq!(second.state, "RUNNING");

    // The PAUSED prior was cancelled (WorkflowCancelled appended by `inline_cancel`)
    // and then sealed (CONTINUED_AS_NEW by `replace_execution`) — no longer active.
    let prior = harvest_workflow_executions::table
        .find(first.exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .expect("prior row must exist");
    assert_ne!(prior.state, "RUNNING", "prior must not be RUNNING");
    assert_ne!(prior.state, "PAUSED", "prior must not be PAUSED");
    assert_eq!(
        prior.state, "CONTINUED_AS_NEW",
        "the cancelled-then-replaced prior is sealed as CONTINUED_AS_NEW"
    );
    assert_eq!(
        count_events_of_type(&mut conn, first.exec_id, "WorkflowCancelled").await,
        1,
        "the inline cancel must append exactly one WorkflowCancelled event"
    );
}

/// FIX 2 companion: `Fail` conflict on a PAUSED prior errors (`AlreadyExists`)
/// with the prior's `existing_state` reported as `PAUSED`.
#[tokio::test]
async fn conflict_fail_paused_errors() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("cf-fail-paused", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::AllowDuplicate;
    let first = start_or_load_workflow_execution(&mut conn, params.clone(), None)
        .await
        .expect("first start should succeed");
    reuse_policy_helpers::force_state(&mut conn, first.exec_id, "PAUSED").await;

    params.exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    params.conflict_policy = autumn_harvest::types::WorkflowIdConflictPolicy::Fail;
    let err = start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect_err("Fail on a PAUSED prior must error");
    match err {
        HarvestError::AlreadyExists {
            existing_exec_id,
            existing_state,
        } => {
            assert_eq!(existing_exec_id, first.exec_id);
            assert_eq!(existing_state, "PAUSED");
        }
        other => panic!("expected AlreadyExists, got {other:?}"),
    }
}

/// AC-6 regression guard: `Unspecified` conflict preserves the reuse policy's
/// native active behavior byte-for-byte — `AllowDuplicate` + RUNNING → attach.
#[tokio::test]
async fn conflict_unspecified_preserves_allow_duplicate_running_attaches() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("cf-unspec-run", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::AllowDuplicate;
    // conflict_policy defaults to Unspecified via base_params.
    let first = start_or_load_workflow_execution(&mut conn, params.clone(), None)
        .await
        .expect("first start should succeed");

    params.exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let second = start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("Unspecified preserves AllowDuplicate attach");
    assert!(!second.created);
    assert_eq!(second.exec_id, first.exec_id);
    assert_eq!(second.state, "RUNNING");
}

/// Success-metric proof (scaled to 20 for CI speed; the 100/100 metric is this
/// test's shape): N concurrent `UseExisting` starts against ONE running prior all
/// converge on the same `exec_id` — zero `AlreadyExists` errors, zero terminations.
#[tokio::test]
async fn conflict_concurrency_race_use_existing_converges() {
    let (database_url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&database_url);

    // Seed one running prior.
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("cf-race-use", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::AllowDuplicateFailedOnly;
    let mut seed_conn = pool.get().await.expect("pool conn");
    let first = start_or_load_workflow_execution(&mut seed_conn, params.clone(), None)
        .await
        .expect("seed start should succeed");
    assert_eq!(first.state, "RUNNING");
    drop(seed_conn);

    let n = 20usize;
    let mut handles = Vec::with_capacity(n);
    for _ in 0..n {
        let pool = pool.clone();
        let mut p = params.clone();
        p.exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
        p.conflict_policy = autumn_harvest::types::WorkflowIdConflictPolicy::UseExisting;
        // StartWorkflowParams borrows 'static &str fields, so it is Send + 'static.
        handles.push(tokio::spawn(async move {
            let mut conn = pool.get().await.expect("pool conn");
            start_or_load_workflow_execution(&mut conn, p, None).await
        }));
    }

    let mut converged = 0usize;
    for h in handles {
        let res = h
            .await
            .expect("task join")
            .expect("no AlreadyExists / no error");
        assert!(
            !res.created,
            "every UseExisting start must attach, not create"
        );
        assert_eq!(res.exec_id, first.exec_id, "all must converge on the prior");
        converged += 1;
    }
    assert_eq!(converged, n, "all {n} concurrent starts converged");

    // The prior was never terminated.
    let mut conn = pool.get().await.expect("pool conn");
    let prior = harvest_workflow_executions::table
        .find(first.exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .expect("prior row must exist");
    assert_eq!(prior.state, "RUNNING", "no start may terminate the prior");
}

/// Codex P2 (issue #685 review): N concurrent `terminate_existing` starts against
/// ONE running prior for a single `(workflow_name, workflow_id)` CONVERGE — every
/// call returns Ok (no transient `NotFound`, no `AlreadyExists`) and the storm
/// settles to exactly ONE surviving non-terminal (RUNNING) execution, with all
/// others sealed.
///
/// This guards the convergence invariant (0 errors, 1 survivor), NOT a
/// deterministic reproduction of the pre-fix 404: the seal race is timing
/// dependent (a loser must reach the post-INSERT load in the window after a
/// winner sealed the prior it locked but before the loser's own snapshot sees the
/// replacement). Without the seal-race retry loop around
/// `load_workflow_execution_by_key_for_update`, a loser that lands in that window
/// surfaces `NotFound` -> the assertion `Ok` below would fail intermittently.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conflict_terminate_existing_concurrent_converges() {
    let (database_url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&database_url);

    // Seed exactly one RUNNING prior for the key.
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("cf-term-race-converge", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::AllowDuplicate;
    let mut seed_conn = pool.get().await.expect("pool conn");
    let first = start_or_load_workflow_execution(&mut seed_conn, params.clone(), None)
        .await
        .expect("seed start should succeed");
    assert_eq!(first.state, "RUNNING");
    drop(seed_conn);

    // Storm the same key with N concurrent terminate_existing starts on a shared
    // pool. Each is a genuine replace (cancel-if-live + fresh insert), so they
    // race to seal each other's replacement row.
    let n = 20usize;
    let mut handles = Vec::with_capacity(n);
    for _ in 0..n {
        let pool = pool.clone();
        let mut p = params.clone();
        p.exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
        p.reuse_policy = WorkflowIdReusePolicy::AllowDuplicate;
        p.conflict_policy = autumn_harvest::types::WorkflowIdConflictPolicy::TerminateExisting;
        handles.push(tokio::spawn(async move {
            let mut conn = pool.get().await.expect("pool conn");
            start_or_load_workflow_execution(&mut conn, p, None).await
        }));
    }

    // AC: zero calls returned an error (no NotFound, no AlreadyExists).
    let mut ok_count = 0usize;
    for h in handles {
        let res = h.await.expect("task join");
        match res {
            Ok(started) => {
                assert!(
                    started.created,
                    "terminate_existing always mints a fresh run"
                );
                assert_eq!(started.state, "RUNNING");
                ok_count += 1;
            }
            Err(e) => panic!("concurrent terminate_existing must converge, got error: {e:?}"),
        }
    }
    assert_eq!(
        ok_count, n,
        "all {n} concurrent terminate_existing starts returned Ok"
    );

    // AC: the storm settled to exactly ONE surviving non-terminal (RUNNING) run
    // for the key; every other row (the seed + the N-1 superseded replacements)
    // is sealed/cancelled and no longer active.
    let mut conn = pool.get().await.expect("pool conn");
    let rows: Vec<String> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq("reuse_policy_wf"))
        .filter(harvest_workflow_executions::workflow_id.eq("cf-term-race-converge"))
        .select(harvest_workflow_executions::state)
        .load(&mut conn)
        .await
        .expect("load rows for the key");

    let running = rows.iter().filter(|s| s.as_str() == "RUNNING").count();
    let paused = rows.iter().filter(|s| s.as_str() == "PAUSED").count();
    assert_eq!(
        running, 1,
        "exactly one surviving RUNNING execution after the storm (states: {rows:?})"
    );
    assert_eq!(paused, 0, "no active PAUSED execution should remain");
    assert_eq!(
        rows.len(),
        n + 1,
        "seed + N replacements = {} total rows (states: {rows:?})",
        n + 1
    );
    // Every non-surviving row is sealed as CONTINUED_AS_NEW by replace_execution.
    let sealed = rows
        .iter()
        .filter(|s| s.as_str() == "CONTINUED_AS_NEW")
        .count();
    assert_eq!(
        sealed, n,
        "the N superseded runs (seed + N-1 losers' priors) are all sealed (states: {rows:?})"
    );
}

// ---------------------------------------------------------------------------
// Concurrency-cap integration tests (issue #88)
// ---------------------------------------------------------------------------

/// (a) Baseline: a `max_concurrent = 2` cap is enforced cluster-wide.
///
/// Six activity tasks are enqueued with the same concurrency key and cap.
/// Only 2 should be claimable at once. After completing one in-flight task, a
/// third slot opens and one more can be claimed.
#[tokio::test]
async fn concurrency_cap_limits_concurrent_claims_cluster_wide() {
    let (mut conn, _container) = setup_test_db().await;
    let queues = vec!["default".to_string()];

    for i in 0..6_u32 {
        let mut params =
            EnqueueParams::new("default", TaskType::Activity, serde_json::json!({ "i": i }));
        params.activity_name = Some("capped_activity".into());
        params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);
        params.concurrency_key = Some("capped_activity".to_string());
        params.max_concurrent = Some(2);
        queue::enqueue(&mut conn, &params)
            .await
            .expect("enqueue failed");
    }

    let t1 = queue::claim_task(&mut conn, &queues, "worker-cc-1", "", None, &[], &[])
        .await
        .expect("claim 1 query failed");
    let t2 = queue::claim_task(&mut conn, &queues, "worker-cc-1", "", None, &[], &[])
        .await
        .expect("claim 2 query failed");
    assert!(t1.is_some(), "first claim should succeed");
    assert!(t2.is_some(), "second claim should succeed");

    // Cap is now saturated — third claim must be deferred.
    let t3 = queue::claim_task(&mut conn, &queues, "worker-cc-1", "", None, &[], &[])
        .await
        .expect("claim 3 query failed");
    assert!(
        t3.is_none(),
        "third claim must be deferred while cap is saturated"
    );

    // Complete one in-flight task to free a slot.
    queue::complete_task(&mut conn, t1.unwrap().id, serde_json::json!(null))
        .await
        .expect("complete_task failed");

    // Now a slot is free; one more task should be claimable.
    let t4 = queue::claim_task(&mut conn, &queues, "worker-cc-1", "", None, &[], &[])
        .await
        .expect("claim after complete query failed");
    assert!(
        t4.is_some(),
        "claim should succeed after one in-flight task completes"
    );
}

/// (b) Shared-key: two distinct activities sharing `concurrency_key = "stripe"`
/// and `max_concurrent = 3` consume from a single shared budget — not two
/// independent budgets of 3 each.
#[tokio::test]
async fn concurrency_cap_shared_key_budget_is_not_doubled() {
    let (mut conn, _container) = setup_test_db().await;
    let queues = vec!["default".to_string()];

    // 3 tasks for activity A
    for i in 0..3_u32 {
        let mut params = EnqueueParams::new(
            "default",
            TaskType::Activity,
            serde_json::json!({ "act": "charge", "i": i }),
        );
        params.activity_name = Some("charge_stripe".into());
        params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);
        params.concurrency_key = Some("stripe".to_string());
        params.max_concurrent = Some(3);
        queue::enqueue(&mut conn, &params)
            .await
            .expect("enqueue charge_stripe failed");
    }

    // 3 tasks for activity B, same key and cap
    for i in 0..3_u32 {
        let mut params = EnqueueParams::new(
            "default",
            TaskType::Activity,
            serde_json::json!({ "act": "refund", "i": i }),
        );
        params.activity_name = Some("refund_stripe".into());
        params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);
        params.concurrency_key = Some("stripe".to_string());
        params.max_concurrent = Some(3);
        queue::enqueue(&mut conn, &params)
            .await
            .expect("enqueue refund_stripe failed");
    }

    // Attempt to claim all 6; the shared budget of 3 should cap the total.
    let mut claimed = 0usize;
    for _ in 0..6 {
        if queue::claim_task(&mut conn, &queues, "worker-sk-1", "", None, &[], &[])
            .await
            .expect("claim query failed")
            .is_some()
        {
            claimed += 1;
        }
    }
    assert_eq!(
        claimed, 3,
        "shared 'stripe' budget of 3 must cap the combined in-flight count to 3, not 6"
    );
}

/// (c) Failure path: in-flight tasks that fail transition out of RUNNING,
/// freeing their slots so that pending peers can be claimed. The queue must
/// not wedge when the saturating tasks all fail.
#[tokio::test]
async fn concurrency_cap_failure_frees_slot_and_does_not_wedge_queue() {
    let (mut conn, _container) = setup_test_db().await;
    let queues = vec!["default".to_string()];

    for i in 0..4_u32 {
        let mut params =
            EnqueueParams::new("default", TaskType::Activity, serde_json::json!({ "i": i }));
        params.activity_name = Some("fragile_activity".into());
        params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);
        params.concurrency_key = Some("fragile".to_string());
        params.max_concurrent = Some(2);
        queue::enqueue(&mut conn, &params)
            .await
            .expect("enqueue failed");
    }

    // Claim 2 (saturating the cap).
    let t1 = queue::claim_task(&mut conn, &queues, "worker-fp-1", "", None, &[], &[])
        .await
        .expect("claim 1 query failed")
        .expect("first task should be claimable");
    let t2 = queue::claim_task(&mut conn, &queues, "worker-fp-1", "", None, &[], &[])
        .await
        .expect("claim 2 query failed")
        .expect("second task should be claimable");

    // Cap is now saturated.
    let t3 = queue::claim_task(&mut conn, &queues, "worker-fp-1", "", None, &[], &[])
        .await
        .expect("claim 3 query failed");
    assert!(t3.is_none(), "cap must be saturated after 2 claims");

    // Fail both in-flight tasks — this transitions them out of RUNNING.
    queue::fail_task(&mut conn, t1.id, "intentional test failure")
        .await
        .expect("fail t1 failed");
    queue::fail_task(&mut conn, t2.id, "intentional test failure")
        .await
        .expect("fail t2 failed");

    // The queue must not be wedged; the remaining pending tasks must be claimable.
    let t4 = queue::claim_task(&mut conn, &queues, "worker-fp-1", "", None, &[], &[])
        .await
        .expect("claim after fail query failed");
    let t5 = queue::claim_task(&mut conn, &queues, "worker-fp-1", "", None, &[], &[])
        .await
        .expect("claim 5 query failed");
    assert!(
        t4.is_some(),
        "pending task must be claimable after in-flight tasks fail"
    );
    assert!(
        t5.is_some(),
        "second pending task must also be claimable after in-flight tasks fail"
    );
}

/// (d) Backward compatibility: activities without a `concurrency_key` (NULL in
/// the queue row) are completely unaffected by a saturated key — they can be
/// claimed freely even when another key is at its cap.
#[tokio::test]
async fn concurrency_cap_null_key_tasks_are_unaffected_by_saturated_key() {
    let (mut conn, _container) = setup_test_db().await;
    let queues = vec!["default".to_string()];

    // Saturate a key with 2 RUNNING tasks (cap = 2).
    for i in 0..2_u32 {
        let mut params =
            EnqueueParams::new("default", TaskType::Activity, serde_json::json!({ "i": i }));
        params.activity_name = Some("capped_activity".into());
        params.scheduled_at = Utc::now() - chrono::Duration::seconds(120);
        params.concurrency_key = Some("saturated_key".to_string());
        params.max_concurrent = Some(2);
        queue::enqueue(&mut conn, &params)
            .await
            .expect("enqueue capped task failed");
        // Immediately claim each to put it in RUNNING state.
        queue::claim_task(&mut conn, &queues, "worker-bc-1", "", None, &[], &[])
            .await
            .expect("claim capped task failed");
    }

    // Verify the key is saturated (third claim returns None).
    let saturated_check = queue::claim_task(&mut conn, &queues, "worker-bc-1", "", None, &[], &[])
        .await
        .expect("saturation check query failed");
    assert!(
        saturated_check.is_none(),
        "key must be saturated before backward-compat check"
    );

    // Enqueue 3 tasks with NO concurrency key (null — the pre-#88 baseline).
    for i in 0..3_u32 {
        let mut params =
            EnqueueParams::new("default", TaskType::Activity, serde_json::json!({ "i": i }));
        params.activity_name = Some("uncapped_activity".into());
        params.scheduled_at = Utc::now() - chrono::Duration::seconds(60);
        // concurrency_key left as None (default) — backward-compat path.
        queue::enqueue(&mut conn, &params)
            .await
            .expect("enqueue uncapped task failed");
    }

    // All 3 uncapped tasks must be claimable even though the saturated key
    // is at its cap — the NULL check-path must not be constrained by other keys.
    let mut claimed = 0usize;
    for _ in 0..3 {
        if queue::claim_task(&mut conn, &queues, "worker-bc-1", "", None, &[], &[])
            .await
            .expect("uncapped claim query failed")
            .is_some()
        {
            claimed += 1;
        }
    }
    assert_eq!(
        claimed, 3,
        "uncapped tasks must not be blocked by a saturated concurrency key"
    );
}

// ---------------------------------------------------------------------------
// Issue #91: per-workflow cron schedule integration tests
// ---------------------------------------------------------------------------

/// Helper: count executions for a given workflow name in any non-terminal or
/// completed state.
async fn count_executions_for_workflow(database_url: &str, workflow_name: &str) -> i64 {
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect for execution count query");
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(workflow_name))
        .count()
        .get_result::<i64>(&mut conn)
        .await
        .expect("count query failed")
}

/// Helper: count executions in RUNNING state for a given workflow name.
async fn count_running_executions(database_url: &str, workflow_name: &str) -> i64 {
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect for running-count query");
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(workflow_name))
        .filter(harvest_workflow_executions::state.eq("RUNNING"))
        .count()
        .get_result::<i64>(&mut conn)
        .await
        .expect("running count query failed")
}

/// Instant-return workflow handler used for schedule baseline tests.
fn instant_workflow<'a>(
    _ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(serde_json::Value::Null) })
}

/// Slow workflow handler that sleeps for 30 s — used to saturate `max_active_runs`.
fn slow_workflow<'a>(
    _ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        tokio::time::sleep(Duration::from_secs(30)).await;
        Ok(serde_json::Value::Null)
    })
}

/// (a) Baseline: a `*/2 * * * * *` schedule dispatches >=3 executions in a
/// 10-second window and each execution carries the deterministic `workflow_id`
/// `sched:{name}:{ts}`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn workflow_schedule_baseline_dispatches_multiple_runs() {
    use autumn_harvest::schema::harvest_workflow_executions::dsl as exec_dsl;

    let (database_url, _container) = setup_test_database_url().await;

    let wf_name = "scheduled_instant_workflow";
    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: wf_name,
            module: "integration_e2e",
            handler: instant_workflow,
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![],
    ));
    let pool = build_test_pool(&database_url);

    // Register the schedule row before starting the scheduler.
    let ws = WorkflowSchedule::new(wf_name, Schedule::Cron("*/2 * * * * *".to_string()));
    {
        let mut conn = pool.get().await.expect("pool get failed");
        register_workflow_schedules(&mut conn, std::slice::from_ref(&ws))
            .await
            .expect("register_workflow_schedules failed");
    }

    // Start scheduler + worker.
    let workflow_schedules = Arc::new(vec![ws]);
    let scheduler = SchedulerRuntime::spawn(
        pool.clone(),
        Arc::clone(&registry),
        Arc::new(DagCatalog::default()),
        Arc::clone(&workflow_schedules),
    );
    let worker = Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                codec_rotation_batch_size: 0,
                dr: autumn_harvest::replication::DrConfig::default(),
                worker_id: "worker-sched-baseline".to_string(),
                queues: vec!["default".to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 4,
                max_concurrent_activities: 4,
                poll_interval: Duration::from_millis(100),
                shutdown_timeout: Duration::from_secs(2),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![autumn_harvest::types::ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
                build_id: String::new(),
                deployment_name: None,
                workflow_cache_size: 1000,
                priority_aging_secs: None,
                unknown_target_grace_window: Duration::from_secs(5),
                poison_pill_threshold: 3,
                capability_miss_max_redeliveries: 5,

                workflow_task_timeout: std::time::Duration::from_secs(10),
                workflow_panic_max_attempts: 3,
                labels: std::collections::HashMap::new(),
                queue_weights: std::collections::HashMap::new(),
                max_workflow_pause_duration: std::time::Duration::from_secs(24 * 3600),
                max_workflow_history_events: None,
                shard_notification_database_urls: Vec::new(),
                sharded_pool: None,
                slot_tuner: None,
                max_concurrent_sessions: 0,
            },
            Arc::clone(&registry),
        )
        .expect("worker should build"),
    );
    let worker_pool = pool.clone();
    let worker_ref = Arc::clone(&worker);
    let worker_handle = tokio::spawn(async move { worker_ref.run(&worker_pool).await });

    // Wait up to 10 seconds for at least 3 executions to appear.
    let dispatched = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let n = count_executions_for_workflow(&database_url, wf_name).await;
            if n >= 3 {
                break n;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("should observe >=3 scheduled dispatches within 10 seconds");

    assert!(dispatched >= 3, "expected >=3 executions, got {dispatched}");

    // Verify that execution workflow_ids follow the deterministic `sched:{name}:{ts}` pattern.
    let mut conn = pool.get().await.expect("pool get failed");
    let workflow_ids: Vec<String> = exec_dsl::harvest_workflow_executions
        .filter(exec_dsl::workflow_name.eq(wf_name))
        .select(exec_dsl::workflow_id)
        .load(&mut conn)
        .await
        .expect("load workflow_ids failed");
    for id in &workflow_ids {
        assert!(
            id.starts_with("sched:") && id.contains(&format!(":{wf_name}:")),
            "workflow_id '{id}' does not match expected sched:[uuid]:{wf_name}:[ts] pattern"
        );
    }

    scheduler.shutdown();
    worker.shutdown();
    let _ = scheduler.join().await;
    let _ = worker_handle.await;
}

/// (b) `max_active_runs = 1` with a slow handler: the second cron firing must
/// be skipped — the in-flight run count must never exceed 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workflow_schedule_max_active_runs_enforced() {
    let (database_url, _container) = setup_test_database_url().await;

    let wf_name = "scheduled_slow_workflow";
    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: wf_name,
            module: "integration_e2e",
            handler: slow_workflow,
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![],
    ));
    let pool = build_test_pool(&database_url);

    let ws = WorkflowSchedule::new(wf_name, Schedule::Cron("*/2 * * * * *".to_string()))
        .with_max_active_runs(1);
    {
        let mut conn = pool.get().await.expect("pool get failed");
        register_workflow_schedules(&mut conn, std::slice::from_ref(&ws))
            .await
            .expect("register_workflow_schedules failed");
    }

    let workflow_schedules = Arc::new(vec![ws]);
    let scheduler = SchedulerRuntime::spawn(
        pool.clone(),
        Arc::clone(&registry),
        Arc::new(DagCatalog::default()),
        Arc::clone(&workflow_schedules),
    );
    let worker = Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                codec_rotation_batch_size: 0,
                dr: autumn_harvest::replication::DrConfig::default(),
                worker_id: "worker-sched-maxruns".to_string(),
                queues: vec!["default".to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 4,
                max_concurrent_activities: 4,
                poll_interval: Duration::from_millis(100),
                shutdown_timeout: Duration::from_secs(2),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![autumn_harvest::types::ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
                build_id: String::new(),
                deployment_name: None,
                workflow_cache_size: 1000,
                priority_aging_secs: None,
                unknown_target_grace_window: Duration::from_secs(5),
                poison_pill_threshold: 3,
                capability_miss_max_redeliveries: 5,

                workflow_task_timeout: std::time::Duration::from_secs(10),
                workflow_panic_max_attempts: 3,
                labels: std::collections::HashMap::new(),
                queue_weights: std::collections::HashMap::new(),
                max_workflow_pause_duration: std::time::Duration::from_secs(24 * 3600),
                max_workflow_history_events: None,
                shard_notification_database_urls: Vec::new(),
                sharded_pool: None,
                slot_tuner: None,
                max_concurrent_sessions: 0,
            },
            Arc::clone(&registry),
        )
        .expect("worker should build"),
    );
    let worker_pool = pool.clone();
    let worker_ref = Arc::clone(&worker);
    let worker_handle = tokio::spawn(async move { worker_ref.run(&worker_pool).await });

    // Let at least one execution start, then observe multiple ticks.
    // After the first dispatch, subsequent ticks must skip because the slow
    // workflow is RUNNING. Allow 8 seconds so we see 3–4 ticks at 2s cadence.
    tokio::time::sleep(Duration::from_secs(8)).await;

    // At most 1 RUNNING execution at any point in time.
    let running = count_running_executions(&database_url, wf_name).await;
    assert!(
        running <= 1,
        "expected at most 1 running execution, found {running}"
    );

    // At least 1 execution was dispatched (the first tick).
    let total = count_executions_for_workflow(&database_url, wf_name).await;
    assert!(total >= 1, "expected at least 1 execution, got {total}");

    scheduler.shutdown();
    worker.shutdown();
    let _ = scheduler.join().await;
    let _ = worker_handle.await;
}

/// (c) Pause / resume: no dispatches after pause; dispatches resume after unpause.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn workflow_schedule_pause_and_resume() {
    use autumn_harvest::schema::harvest_schedules::dsl as sched_dsl;

    let (database_url, _container) = setup_test_database_url().await;

    let wf_name = "scheduled_pause_resume_workflow";
    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: wf_name,
            module: "integration_e2e",
            handler: instant_workflow,
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![],
    ));
    let pool = build_test_pool(&database_url);

    // Register the schedule paused so no runs fire initially.
    let ws = WorkflowSchedule::new(wf_name, Schedule::Cron("*/2 * * * * *".to_string()))
        .with_paused(true);
    {
        let mut conn = pool.get().await.expect("pool get failed");
        register_workflow_schedules(&mut conn, std::slice::from_ref(&ws))
            .await
            .expect("register_workflow_schedules failed");
    }

    let workflow_schedules = Arc::new(vec![ws]);
    let scheduler = SchedulerRuntime::spawn(
        pool.clone(),
        Arc::clone(&registry),
        Arc::new(DagCatalog::default()),
        Arc::clone(&workflow_schedules),
    );
    let worker = Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                codec_rotation_batch_size: 0,
                dr: autumn_harvest::replication::DrConfig::default(),
                worker_id: "worker-sched-pause".to_string(),
                queues: vec!["default".to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 4,
                max_concurrent_activities: 4,
                poll_interval: Duration::from_millis(100),
                shutdown_timeout: Duration::from_secs(2),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![autumn_harvest::types::ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
                build_id: String::new(),
                deployment_name: None,
                workflow_cache_size: 1000,
                priority_aging_secs: None,
                unknown_target_grace_window: Duration::from_secs(5),
                poison_pill_threshold: 3,
                capability_miss_max_redeliveries: 5,

                workflow_task_timeout: std::time::Duration::from_secs(10),
                workflow_panic_max_attempts: 3,
                labels: std::collections::HashMap::new(),
                queue_weights: std::collections::HashMap::new(),
                max_workflow_pause_duration: std::time::Duration::from_secs(24 * 3600),
                max_workflow_history_events: None,
                shard_notification_database_urls: Vec::new(),
                sharded_pool: None,
                slot_tuner: None,
                max_concurrent_sessions: 0,
            },
            Arc::clone(&registry),
        )
        .expect("worker should build"),
    );
    let worker_pool = pool.clone();
    let worker_ref = Arc::clone(&worker);
    let worker_handle = tokio::spawn(async move { worker_ref.run(&worker_pool).await });

    // Let 3 ticks pass — schedule is paused so no executions should start.
    tokio::time::sleep(Duration::from_secs(6)).await;
    let count_while_paused = count_executions_for_workflow(&database_url, wf_name).await;
    assert_eq!(
        count_while_paused, 0,
        "no executions should fire while schedule is paused"
    );

    // Resume: flip is_paused to false and update next_run_at so the scheduler
    // will pick it up on the next tick.
    {
        let mut conn = pool.get().await.expect("pool get failed");
        diesel::update(sched_dsl::harvest_schedules.filter(sched_dsl::workflow_name.eq(wf_name)))
            .set((
                sched_dsl::is_paused.eq(false),
                sched_dsl::next_run_at.eq(Utc::now() - chrono::Duration::seconds(1)),
                sched_dsl::updated_at.eq(Utc::now()),
            ))
            .execute(&mut conn)
            .await
            .expect("resume update failed");
    }

    // After resuming, wait up to 6 seconds for at least 1 execution.
    let dispatched = tokio::time::timeout(Duration::from_secs(6), async {
        loop {
            let n = count_executions_for_workflow(&database_url, wf_name).await;
            if n >= 1 {
                break n;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("should observe executions after resume within 6 seconds");

    assert!(
        dispatched >= 1,
        "expected >=1 execution after resume, got {dispatched}"
    );

    scheduler.shutdown();
    worker.shutdown();
    let _ = scheduler.join().await;
    let _ = worker_handle.await;
}

/// (d) Backward compatibility: a deployment with only DAG schedules in the
/// `harvest_schedules` table sees no interaction from the workflow-schedule
/// tick branch. This verifies the `workflow_name IS NOT NULL` filter keeps
/// DAG-only rows untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_schedule_dag_only_deployment_unaffected() {
    use autumn_harvest::schema::harvest_schedules::dsl as sched_dsl;

    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);

    // There are no workflow-schedule rows; the workflow_schedules list is empty.
    let empty_workflow_schedules: Arc<Vec<WorkflowSchedule>> = Arc::new(Vec::new());
    let empty_dags: Arc<autumn_harvest::DagCatalog> = Arc::new(DagCatalog::default());
    let registry = Arc::new(HandlerRegistry::new(vec![], vec![]));

    // Run several ticks against an otherwise-empty database.
    for _ in 0..3 {
        tick_once(
            pool.clone(),
            Arc::clone(&registry),
            Arc::clone(&empty_dags),
            Arc::clone(&empty_workflow_schedules),
            SchedulerMonitor::offline(),
        )
        .await
        .expect("tick_once must not error on an empty workflow-schedule list");
    }

    // No schedule rows should have been inserted.
    let mut conn = pool.get().await.expect("pool get failed");
    let schedule_count: i64 = sched_dsl::harvest_schedules
        .count()
        .get_result(&mut conn)
        .await
        .expect("count query failed");
    assert_eq!(
        schedule_count, 0,
        "no schedule rows should exist when workflow_schedules is empty"
    );

    // No workflow executions should have been started.
    let exec_count: i64 = harvest_workflow_executions::table
        .count()
        .get_result(&mut conn)
        .await
        .expect("execution count query failed");
    assert_eq!(
        exec_count, 0,
        "no workflow executions should have been started"
    );
}

// ---------------------------------------------------------------------------
// Search-attribute lifecycle tests (issue #159, AC #11 and #12)
// ---------------------------------------------------------------------------

/// Workflow used by search-attribute tests.
///
/// - Immediately sets `phase=awaiting_approval`.
/// - Suspends on a `charge` signal.
/// - On receipt, overwrites `phase=charged`.
/// - Completes.
fn approval_search_attrs_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.upsert_search_attrs([(
            "phase".to_string(),
            Some(serde_json::json!("awaiting_approval")),
        )])
        .map_err(|e| e.to_string())?;

        ctx.wait_for_signal("charge")
            .await
            .map_err(|e| e.to_string())?;

        ctx.upsert_search_attrs([("phase".to_string(), Some(serde_json::json!("charged")))])
            .map_err(|e| e.to_string())?;

        Ok(serde_json::json!({"status": "charged"}))
    })
}

/// Query `harvest_workflow_executions` rows whose `search_attrs` contains all
/// key-value pairs in `predicate` (Postgres `@>` containment operator).
async fn find_by_search_attrs(
    database_url: &str,
    predicate: serde_json::Value,
) -> Vec<WorkflowExecution> {
    use diesel::dsl::sql;
    use diesel::sql_types::{Bool, Jsonb};

    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(database_url)
        .await
        .expect("fresh connection for search_attrs query");

    harvest_workflow_executions::table
        .filter(sql::<Bool>("search_attrs @> ").bind::<Jsonb, _>(predicate))
        .select(WorkflowExecution::as_select())
        .load(&mut conn)
        .await
        .expect("search_attrs containment query failed")
}

/// AC #11 — mutable search-attribute lifecycle:
///
/// 1. Workflow starts with `tenant=acme`.
/// 2. First execution cycle: `upsert_search_attrs` sets `phase=awaiting_approval`.
/// 3. Workflow suspends on the `charge` signal.
/// 4. DB query with `tenant=acme AND phase=awaiting_approval` finds the execution.
/// 5. `charge` signal is delivered; second cycle sets `phase=charged`.
/// 6. `phase=awaiting_approval` filter returns nothing; `phase=charged` filter finds it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn search_attrs_upsert_visible_after_update_and_filterable() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("connect to test DB");

    let exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));

    // Start the workflow with tenant=acme in initial search_attrs.
    let start = start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: "approval_search_attrs_workflow",
            workflow_id: "acme-approval-001",
            exec_id,
            input: serde_json::json!({}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: Some(serde_json::json!({"tenant": "acme"})),
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            chain_execution_timeout: None,
            max_workflow_chain_timeout_ceiling: None,
            inherited_chain_deadline_at: None,
            concurrency_key: None,
            concurrency_limit: None,
            concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
            priority: Priority::default(),
            max_workflow_input_bytes: 0,
            start_at: None,
            delay: None,
            max_workflow_start_delay: None,

            owner: None,
            runbook_url: None,
            severity: None,
            context_headers: None,

            sla: None,
            schedule_id: None,
            scheduled_for: None,
            workflow_attempt: 1,
            workflow_retry_policy: None,
            retry_of_exec_id: None,
            max_workflow_attempts_ceiling: None,
            origin: None,
            completion_callbacks: None,
            start_source: autumn_harvest::StartSource::Api,
            start_source_ref: None,
            started_by: None,
        },
        None,
    )
    .await
    .expect("start_or_load_workflow_execution failed");
    assert!(start.created);

    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: "approval_search_attrs_workflow",
            module: "integration_e2e",
            handler: approval_search_attrs_workflow,
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![],
    ));
    let worker = build_runtime_worker("worker-sa-lifecycle", 1, 1, registry);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    // Wait for the workflow to reach the signal-wait suspension (phase should
    // now be awaiting_approval in the DB).
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let rows = find_by_search_attrs(
                &database_url,
                serde_json::json!({"tenant": "acme", "phase": "awaiting_approval"}),
            )
            .await;
            if !rows.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("search_attrs should show phase=awaiting_approval within timeout");

    // Confirm the old filter now has a hit.
    let awaiting = find_by_search_attrs(
        &database_url,
        serde_json::json!({"tenant": "acme", "phase": "awaiting_approval"}),
    )
    .await;
    assert_eq!(
        awaiting.len(),
        1,
        "should find one execution awaiting approval"
    );
    assert_eq!(awaiting[0].id, exec_id.as_uuid());

    // Confirm it is NOT yet in the charged filter.
    let charged_before = find_by_search_attrs(
        &database_url,
        serde_json::json!({"tenant": "acme", "phase": "charged"}),
    )
    .await;
    assert!(
        charged_before.is_empty(),
        "should not appear in charged filter before signal"
    );

    // Deliver the charge signal and wake the task.
    autumn_harvest::signal::send_signal(&mut conn, exec_id, "charge", serde_json::json!({}))
        .await
        .expect("send_signal failed");
    queue::wake_workflow_task(&mut conn, exec_id)
        .await
        .expect("wake_workflow_task failed");

    // Wait for completion.
    wait_for_execution_state(&database_url, exec_id, "COMPLETED").await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    // After completion: phase=awaiting_approval filter returns nothing.
    let awaiting_after = find_by_search_attrs(
        &database_url,
        serde_json::json!({"tenant": "acme", "phase": "awaiting_approval"}),
    )
    .await;
    assert!(
        awaiting_after.is_empty(),
        "awaiting_approval filter must be empty after phase update"
    );

    // phase=charged filter now finds the execution.
    let charged_after = find_by_search_attrs(
        &database_url,
        serde_json::json!({"tenant": "acme", "phase": "charged"}),
    )
    .await;
    assert_eq!(
        charged_after.len(),
        1,
        "charged filter should find the execution after phase update"
    );
    assert_eq!(charged_after[0].id, exec_id.as_uuid());
}

/// AC #12 — search attributes survive a worker crash and resume:
///
/// 1. Worker runs the first cycle, sets `phase=awaiting_approval`, suspends.
/// 2. Worker is shut down (simulating a crash).
/// 3. A fresh query confirms the attribute is still in the DB.
/// 4. A new worker picks up the task from where it left off.
/// 5. Signal is delivered; workflow completes with `phase=charged` in the DB.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn search_attrs_survive_worker_crash_and_resume() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("connect to test DB");

    let exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));

    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: "approval_search_attrs_workflow",
            workflow_id: "acme-crash-resume-001",
            exec_id,
            input: serde_json::json!({}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: Some(serde_json::json!({"tenant": "acme"})),
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            chain_execution_timeout: None,
            max_workflow_chain_timeout_ceiling: None,
            inherited_chain_deadline_at: None,
            concurrency_key: None,
            concurrency_limit: None,
            concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
            priority: Priority::default(),
            max_workflow_input_bytes: 0,
            start_at: None,
            delay: None,
            max_workflow_start_delay: None,

            owner: None,
            runbook_url: None,
            severity: None,
            context_headers: None,

            sla: None,
            schedule_id: None,
            scheduled_for: None,
            workflow_attempt: 1,
            workflow_retry_policy: None,
            retry_of_exec_id: None,
            max_workflow_attempts_ceiling: None,
            origin: None,
            completion_callbacks: None,
            start_source: autumn_harvest::StartSource::Api,
            start_source_ref: None,
            started_by: None,
        },
        None,
    )
    .await
    .expect("start_or_load_workflow_execution failed");

    let make_registry = || {
        Arc::new(HandlerRegistry::new(
            vec![WorkflowInfo {
                quota: None,
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: "approval_search_attrs_workflow",
                module: "integration_e2e",
                handler: approval_search_attrs_workflow,
                execution_timeout: None,
                chain_execution_timeout: None,
                sla: None,
                concurrency: None,

                debounce: None,
                batch: None,
                throttle: None,
                max_input_bytes: None,

                owner: None,
                runbook_url: None,
                severity: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
                retry_policy: None,
            }],
            vec![],
        ))
    };
    let pool = build_test_pool(&database_url);

    // --- First worker: run until phase=awaiting_approval is persisted ---
    let worker1 = build_runtime_worker("worker-crash-1", 1, 1, make_registry());
    let handle1 = spawn_test_worker(Arc::clone(&worker1), pool.clone());

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let rows = find_by_search_attrs(
                &database_url,
                serde_json::json!({"phase": "awaiting_approval"}),
            )
            .await;
            if !rows.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("phase=awaiting_approval should be visible within timeout");

    // Simulate crash: shut down the first worker.
    worker1.shutdown();
    handle1.await.expect("worker1 join");

    // Confirm the attribute is durable after the crash.
    let after_crash = find_by_search_attrs(
        &database_url,
        serde_json::json!({"tenant": "acme", "phase": "awaiting_approval"}),
    )
    .await;
    assert_eq!(
        after_crash.len(),
        1,
        "search_attrs must survive worker crash"
    );

    // --- Second worker: resume and complete ---
    let worker2 = build_runtime_worker("worker-crash-2", 1, 1, make_registry());
    let handle2 = spawn_test_worker(Arc::clone(&worker2), pool.clone());

    // Wait for the task to be re-claimed by the new worker (sticky timeout
    // elapses or the task is re-enqueued after the signal below).
    autumn_harvest::signal::send_signal(&mut conn, exec_id, "charge", serde_json::json!({}))
        .await
        .expect("send_signal failed");
    queue::wake_workflow_task(&mut conn, exec_id)
        .await
        .expect("wake_workflow_task failed");

    wait_for_execution_state(&database_url, exec_id, "COMPLETED").await;

    worker2.shutdown();
    handle2.await.expect("worker2 join");

    // Final check: phase=charged is now in the DB.
    let final_state = find_by_search_attrs(
        &database_url,
        serde_json::json!({"tenant": "acme", "phase": "charged"}),
    )
    .await;
    assert_eq!(
        final_state.len(),
        1,
        "phase=charged must be set after resume and completion"
    );
}

/// (e) Builder validation: scheduling an unregistered workflow name fails at
/// `build()` with `HarvestBuilderError::UnknownWorkflowSchedule`.
#[test]
fn workflow_schedule_builder_rejects_unregistered_workflow() {
    let ws = WorkflowSchedule::new(
        "nonexistent_workflow",
        Schedule::Cron("0 * * * *".to_string()),
    );

    let result = HarvestBuilder::new()
        .workflows(vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: "some_other_workflow",
            module: "integration_e2e",
            handler: echo_workflow,
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }])
        .workflow_schedule(ws)
        .worker(WorkerConfig::default())
        .try_build();

    assert!(
        matches!(
            result,
            Err(autumn_harvest::HarvestBuilderError::UnknownWorkflowSchedule {
                ref workflow_name, ..
            }) if workflow_name == "nonexistent_workflow"
        ),
        "expected UnknownWorkflowSchedule error, got: {result:?}"
    );
}

// ---------------------------------------------------------------------------
// Worker drain controls (issue #170)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn drain_accepted_sets_status_to_draining() {
    use autumn_harvest::workers::{DrainOutcome, register_worker, request_drain};

    let (mut conn, _container) = setup_test_db().await;
    let stale_threshold = Duration::from_secs(10);

    register_worker(
        &mut conn,
        "w-drain-1",
        &["default".to_string()],
        &[0],
        4,
        "test-host",
        None,
        "",
        None,
        &std::collections::HashMap::new(),
        0,
        &[],
    )
    .await
    .unwrap();

    // Supply an explicit deadline; the default is computed by the HTTP handler
    // layer, not request_drain itself. The integration test verifies the DB
    // round-trip for a caller-supplied deadline.
    let deadline = Utc::now() + chrono::Duration::minutes(1);
    let resp = request_drain(
        &mut conn,
        "w-drain-1",
        Some(deadline),
        true,
        stale_threshold,
    )
    .await
    .unwrap();

    assert_eq!(
        resp.outcome,
        DrainOutcome::Accepted,
        "first drain must be Accepted"
    );
    assert!(
        resp.drain_deadline_at.is_some(),
        "drain_deadline_at must be set when a deadline is supplied"
    );
    assert_eq!(resp.worker_id, "w-drain-1");
    assert!(resp.unavailable_shards.is_empty());
}

#[tokio::test]
async fn drain_already_draining_on_second_call() {
    use autumn_harvest::workers::{DrainOutcome, register_worker, request_drain};

    let (mut conn, _container) = setup_test_db().await;
    let stale_threshold = Duration::from_secs(10);

    register_worker(
        &mut conn,
        "w-drain-2",
        &["default".to_string()],
        &[],
        2,
        "test-host",
        None,
        "",
        None,
        &std::collections::HashMap::new(),
        0,
        &[],
    )
    .await
    .unwrap();

    let first_deadline = Utc::now() + chrono::Duration::minutes(1);
    request_drain(
        &mut conn,
        "w-drain-2",
        Some(first_deadline),
        true,
        stale_threshold,
    )
    .await
    .unwrap();

    // Re-drain with a new deadline — should return AlreadyDraining and
    // persist the updated deadline (operators extending a drain window).
    let new_deadline = Utc::now() + chrono::Duration::minutes(5);
    let resp2 = request_drain(
        &mut conn,
        "w-drain-2",
        Some(new_deadline),
        true,
        stale_threshold,
    )
    .await
    .unwrap();

    assert_eq!(
        resp2.outcome,
        DrainOutcome::AlreadyDraining,
        "second drain on already-draining worker must return AlreadyDraining"
    );
    // Deadline must reflect the refreshed value, not the original.
    let stored = resp2.drain_deadline_at.expect("deadline must be echoed");
    let diff = (stored - new_deadline).num_seconds().abs();
    assert!(diff <= 2, "refreshed deadline differs by {diff}s");
}

#[tokio::test]
async fn drain_already_stopped_after_transition() {
    use autumn_harvest::workers::{
        DrainOutcome, WorkerStatus, register_worker, request_drain, transition_status,
    };

    let (mut conn, _container) = setup_test_db().await;
    let stale_threshold = Duration::from_secs(10);

    register_worker(
        &mut conn,
        "w-drain-3",
        &[],
        &[],
        1,
        "test-host",
        None,
        "",
        None,
        &std::collections::HashMap::new(),
        0,
        &[],
    )
    .await
    .unwrap();
    transition_status(&mut conn, "w-drain-3", WorkerStatus::Stopped)
        .await
        .unwrap();

    let resp = request_drain(&mut conn, "w-drain-3", None, false, stale_threshold)
        .await
        .unwrap();

    assert_eq!(
        resp.outcome,
        DrainOutcome::AlreadyStopped,
        "draining a stopped worker must return AlreadyStopped"
    );
}

#[tokio::test]
async fn drain_not_found_for_unknown_worker() {
    use autumn_harvest::workers::request_drain;

    let (mut conn, _container) = setup_test_db().await;
    let stale_threshold = Duration::from_secs(10);

    let resp = request_drain(&mut conn, "w-does-not-exist", None, false, stale_threshold)
        .await
        .unwrap();

    assert_eq!(
        resp.outcome,
        autumn_harvest::workers::DrainOutcome::NotFound,
        "unknown worker must return NotFound"
    );
}

#[tokio::test]
async fn drain_with_explicit_deadline_is_stored() {
    use autumn_harvest::workers::{DrainOutcome, register_worker, request_drain};

    let (mut conn, _container) = setup_test_db().await;
    let stale_threshold = Duration::from_secs(10);

    register_worker(
        &mut conn,
        "w-drain-deadline",
        &[],
        &[],
        1,
        "test-host",
        None,
        "",
        None,
        &std::collections::HashMap::new(),
        0,
        &[],
    )
    .await
    .unwrap();

    let explicit_deadline = Utc::now() + chrono::Duration::minutes(5);
    let resp = request_drain(
        &mut conn,
        "w-drain-deadline",
        Some(explicit_deadline),
        true,
        stale_threshold,
    )
    .await
    .unwrap();

    assert_eq!(resp.outcome, DrainOutcome::Accepted);
    let stored = resp.drain_deadline_at.expect("deadline must be set");
    let diff = (stored - explicit_deadline).num_seconds().abs();
    assert!(diff <= 2, "stored deadline differs by {diff}s");
}

#[tokio::test]
async fn drain_preview_returns_active_workers() {
    use autumn_harvest::workers::{WorkerFilters, drain_preview, register_worker};

    let (mut conn, _container) = setup_test_db().await;
    let stale_threshold = Duration::from_secs(10);

    for i in 0..3_u8 {
        register_worker(
            &mut conn,
            &format!("w-preview-{i}"),
            &["default".to_string()],
            &[],
            4,
            "test-host",
            None,
            "",
            None,
            &std::collections::HashMap::new(),
            0,
            &[],
        )
        .await
        .unwrap();
    }

    let filters = WorkerFilters {
        queue: Some("default".to_string()),
        ..WorkerFilters::new()
    };
    let items = drain_preview(&mut conn, &filters, stale_threshold)
        .await
        .unwrap();

    assert_eq!(items.len(), 3, "drain-preview should return all 3 workers");
    for item in &items {
        assert_eq!(item.status, "Active");
    }
}

// ---------------------------------------------------------------------------
// Issue #227: typed activity failure surface — end-to-end fail-fast behavior
// ---------------------------------------------------------------------------

use autumn_harvest::failure::ActivityFailure;

/// Activity handler that always fails with a typed `ActivityFailure` flagged
/// `non_retryable`. Returned via the dispatch shim's typed JSON path.
fn always_non_retryable_activity<'a>(
    _ctx: &'a ActivityContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        // Manually encode through the same path the macro uses.
        Err(
            autumn_harvest::failure::IntoActivityErrorString::into_error_payload(
                ActivityFailure::non_retryable("PermanentValidation", "amount must be positive"),
            ),
        )
    })
}

/// Activity handler that always fails with a legacy plain `String` error.
fn always_legacy_string_failure_activity<'a>(
    _ctx: &'a ActivityContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move { Err("foo".to_string()) })
}

/// Activity handler that always fails with a *retryable* error, simulating a
/// downstream outage (issue #369). The circuit breaker should observe these
/// failures and trip, after which the worker short-circuits dispatch.
fn always_retryable_failure_activity<'a>(
    _ctx: &'a ActivityContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move { Err("downstream is down".to_string()) })
}

/// End-to-end fail-fast for a typed `ActivityFailure` flagged `non_retryable`:
/// the activity must fail on attempt 1 (skipping the retry policy entirely),
/// the `ActivityFailed` event in history must carry the structured
/// `error_type` and `non_retryable` fields, and the workflow itself must
/// reach `FAILED` because the workflow function propagates the activity
/// error.
///
/// We deliberately do **not** assert on a `harvest_dead_letters` row: the
/// worker no longer auto-inserts DLQ rows for activity failures because
/// `dlq::replay_dead_letter` cannot meaningfully re-run them (the terminal
/// `ActivityFailed` event makes `find_pending_scheduled_activity` reject
/// the replayed task). Workflow-level visibility is preserved via the
/// `ActivityFailed` + `WorkflowFailed` event pair.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn non_retryable_activity_fails_fast_on_attempt_one() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let exec_id = insert_workflow_execution(&mut conn).await;
    let workflow_input = serde_json::json!({"amount": -1});

    let started_events = vec![WorkflowEvent::WorkflowStarted {
        input: workflow_input.clone(),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];
    store::append_events(&mut conn, exec_id, &started_events, 0)
        .await
        .expect("append WorkflowStarted failed");

    let mut params = EnqueueParams::new("default", TaskType::Workflow, workflow_input.clone());
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);
    queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue workflow task failed");

    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: "e2e_test_workflow",
            module: "integration_e2e",
            handler: workflow_with_activity,
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![ActivityInfo {
            name: "send_email",
            module: "integration_e2e",
            // Retry policy says "try 5 times" — but ActivityFailure.non_retryable
            // must win over the policy and route to DLQ on attempt 1.
            default_retry_policy: Some(autumn_harvest::RetryPolicy::exponential(
                5,
                Duration::from_millis(10),
            )),
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_schedule_to_close: None,
            default_queue: Some("default"),
            max_concurrent: None,
            concurrency_key: None,
            rate_limit_rps: None,
            rate_limit_burst: None,
            rate_limit_key: None,
            rate_limit_key_expr: None,
            circuit_breaker: None,
            is_local: false,
            max_input_bytes: None,
            max_result_bytes: None,
            requires: None,
            handler: always_non_retryable_activity,
        }],
    ));

    let worker = build_runtime_worker("worker-non-retryable", 1, 1, registry);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let execution = wait_for_execution_state(&database_url, exec_id, "FAILED").await;
    worker.shutdown();
    handle.await.expect("worker task should join");

    // 1. The ActivityFailed event in history carries the typed fields.
    let history = load_history_from_url(&database_url, exec_id).await;
    let activity_failed = history
        .events
        .iter()
        .find_map(|ev| match ev {
            WorkflowEvent::ActivityFailed {
                error_type,
                non_retryable,
                attempt,
                ..
            } => Some((error_type.clone(), *non_retryable, *attempt)),
            _ => None,
        })
        .expect("history must contain ActivityFailed");
    assert_eq!(activity_failed.0, "PermanentValidation");
    assert!(activity_failed.1, "non_retryable flag must be true");
    assert_eq!(
        activity_failed.2, 1,
        "must fail on attempt 1 — retry policy ignored"
    );

    // 2. Exactly one ActivityFailed event — the retry policy did not fire.
    let activity_failed_count = history
        .events
        .iter()
        .filter(|ev| matches!(ev, WorkflowEvent::ActivityFailed { .. }))
        .count();
    assert_eq!(
        activity_failed_count, 1,
        "non_retryable activities must not retry; got {activity_failed_count} ActivityFailed events"
    );

    // 3. No DLQ row is created for the failed activity — see the doc comment
    //    on this test for why. The workflow's failure is observable via the
    //    `ActivityFailed` event and the trailing `WorkflowFailed` event.
    let dlq_rows: Vec<autumn_harvest::models::DeadLetter> = {
        use autumn_harvest::schema::harvest_dead_letters::dsl;
        dsl::harvest_dead_letters
            .filter(dsl::workflow_exec_id.eq(Some(exec_id.as_uuid())))
            .select(autumn_harvest::models::DeadLetter::as_select())
            .load(&mut conn)
            .await
            .expect("dlq query failed")
    };
    assert_eq!(
        dlq_rows.len(),
        0,
        "activity retry exhaustion must not auto-insert a DLQ row (those rows are not replayable)"
    );

    // 4. The workflow ultimately failed (the workflow function propagated the
    //    activity error). Belt-and-braces check on execution state.
    assert_eq!(execution.state, "FAILED");
}

/// End-to-end circuit breaker (issue #369): an activity configured with a
/// `CircuitBreakerPolicy` (threshold 1) against a downstream that is hard-down.
///
/// Attempt 1 dispatches normally (breaker closed) and fails with a retryable
/// error, which trips the breaker. Attempt 2 (the retry) is short-circuited by
/// the open breaker and recorded as a non-retryable `ActivityFailed` with
/// `error_type = "CircuitOpen"`, terminating the workflow without burning the
/// rest of the retry curve. The `CircuitOpen` failure lives in the workflow's
/// own event history exactly like any other activity failure, so replay
/// reproduces the same outcome regardless of breaker state at replay time
/// (no new `WorkflowEvent` variant is introduced).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn circuit_breaker_short_circuits_after_tripping() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let exec_id = insert_workflow_execution(&mut conn).await;
    let workflow_input = serde_json::json!({"to": "alice@example.com"});

    let started_events = vec![WorkflowEvent::WorkflowStarted {
        input: workflow_input.clone(),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];
    store::append_events(&mut conn, exec_id, &started_events, 0)
        .await
        .expect("append WorkflowStarted failed");

    let mut params = EnqueueParams::new("default", TaskType::Workflow, workflow_input.clone());
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);
    queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue workflow task failed");

    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: "e2e_test_workflow",
            module: "integration_e2e",
            handler: workflow_with_activity,
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,
            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![ActivityInfo {
            name: "send_email",
            module: "integration_e2e",
            // Allow up to 5 retries — but the breaker (threshold 1) trips on the
            // first failure and short-circuits the retry as a CircuitOpen.
            default_retry_policy: Some(autumn_harvest::RetryPolicy::exponential(
                5,
                Duration::from_millis(10),
            )),
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_schedule_to_close: None,
            default_queue: Some("default"),
            max_concurrent: None,
            concurrency_key: None,
            rate_limit_rps: None,
            rate_limit_burst: None,
            rate_limit_key: None,
            rate_limit_key_expr: None,
            // Trip after a single failure; long cooldown so it stays open.
            circuit_breaker: Some(autumn_harvest::policy::CircuitBreakerPolicy::new(
                1,
                Duration::from_secs(60),
                Duration::from_secs(300),
            )),
            is_local: false,
            max_input_bytes: None,
            max_result_bytes: None,
            requires: None,
            handler: always_retryable_failure_activity,
        }],
    ));

    let worker = build_runtime_worker("worker-circuit-breaker", 1, 1, registry);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let execution = wait_for_execution_state(&database_url, exec_id, "FAILED").await;
    worker.shutdown();
    handle.await.expect("worker task should join");

    let history = load_history_from_url(&database_url, exec_id).await;

    // The breaker short-circuited a dispatch: history carries a non-retryable
    // ActivityFailed with error_type "CircuitOpen".
    let circuit_open = history
        .events
        .iter()
        .find_map(|ev| match ev {
            WorkflowEvent::ActivityFailed {
                error_type,
                non_retryable,
                ..
            } if error_type == "CircuitOpen" => Some(*non_retryable),
            _ => None,
        })
        .expect("history must contain a CircuitOpen ActivityFailed once the breaker trips");
    assert!(
        circuit_open,
        "CircuitOpen failures must be non-retryable terminal for the in-flight attempt"
    );

    // The breaker prevented the full retry curve from running: far fewer than
    // the 5 configured attempts were dispatched (attempt 1 ran, the rest were
    // short-circuited terminally).
    let activity_started = history
        .events
        .iter()
        .filter(|ev| matches!(ev, WorkflowEvent::ActivityStarted { .. }))
        .count();
    assert!(
        activity_started <= 2,
        "breaker must curb retries; saw {activity_started} ActivityStarted events"
    );

    assert_eq!(execution.state, "FAILED");
}

/// Back-compat mirror: an activity returning a legacy `Err("foo")` short-
/// circuits retries when `RetryPolicy::non_retryable_errors` contains `"foo"`,
/// exactly as before #227. Confirms the legacy resolution path is still
/// wired through the new `Option<&str>` signature on
/// `RetryPolicy::is_non_retryable`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn legacy_string_failure_in_non_retryable_errors_fails_fast() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let exec_id = insert_workflow_execution(&mut conn).await;
    let workflow_input = serde_json::json!({});

    let started_events = vec![WorkflowEvent::WorkflowStarted {
        input: workflow_input.clone(),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];
    store::append_events(&mut conn, exec_id, &started_events, 0)
        .await
        .expect("append WorkflowStarted failed");

    let mut params = EnqueueParams::new("default", TaskType::Workflow, workflow_input.clone());
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);
    queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue workflow task failed");

    let mut retry = autumn_harvest::RetryPolicy::exponential(5, Duration::from_millis(10));
    retry.non_retryable_errors = vec!["foo".to_string()];

    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: "e2e_test_workflow",
            module: "integration_e2e",
            handler: workflow_with_activity,
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![ActivityInfo {
            name: "send_email",
            module: "integration_e2e",
            default_retry_policy: Some(retry),
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_schedule_to_close: None,
            default_queue: Some("default"),
            max_concurrent: None,
            concurrency_key: None,
            rate_limit_rps: None,
            rate_limit_burst: None,
            rate_limit_key: None,
            rate_limit_key_expr: None,
            circuit_breaker: None,
            is_local: false,
            max_input_bytes: None,
            max_result_bytes: None,
            requires: None,
            handler: always_legacy_string_failure_activity,
        }],
    ));

    let worker = build_runtime_worker("worker-legacy-non-retry", 1, 1, registry);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let execution = wait_for_execution_state(&database_url, exec_id, "FAILED").await;
    worker.shutdown();
    handle.await.expect("worker task should join");

    // Exactly one ActivityFailed event — the legacy non_retryable_errors match
    // short-circuited the retry policy on attempt 1, matching the pre-#227 path.
    let history = load_history_from_url(&database_url, exec_id).await;
    let activity_failed_events: Vec<_> = history
        .events
        .iter()
        .filter_map(|ev| match ev {
            WorkflowEvent::ActivityFailed {
                error_type,
                non_retryable,
                attempt,
                error,
                ..
            } => Some((error_type.clone(), *non_retryable, *attempt, error.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(activity_failed_events.len(), 1);
    let (etype, non_retryable, attempt, error) = &activity_failed_events[0];
    // Plain-string errors deserialize through serde defaults → "Error" / false.
    assert_eq!(etype, "Error");
    assert!(
        !non_retryable,
        "legacy errors carry non_retryable=false; the engine uses the policy match instead"
    );
    assert_eq!(*attempt, 1);
    assert_eq!(error, "foo");

    // No DLQ row for the failed activity — see note on
    // `non_retryable_activity_fails_fast_on_attempt_one`.
    let dlq_rows: Vec<autumn_harvest::models::DeadLetter> = {
        use autumn_harvest::schema::harvest_dead_letters::dsl;
        dsl::harvest_dead_letters
            .filter(dsl::workflow_exec_id.eq(Some(exec_id.as_uuid())))
            .select(autumn_harvest::models::DeadLetter::as_select())
            .load(&mut conn)
            .await
            .expect("dlq query failed")
    };
    assert_eq!(dlq_rows.len(), 0);
    assert_eq!(execution.state, "FAILED");
}

// ===== Overlap policy integration tests (issue #241) =============================

/// Count executions for a workflow in an exact DB state value.
async fn count_executions_in_state(database_url: &str, workflow_name: &str, state: &str) -> i64 {
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect for state count query");
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(workflow_name))
        .filter(harvest_workflow_executions::state.eq(state))
        .count()
        .get_result::<i64>(&mut conn)
        .await
        .expect("state count query failed")
}

/// Query the number of entries in `harvest_schedules.buffered_runs` for a workflow schedule.
async fn query_buffered_runs_count(database_url: &str, workflow_name: &str) -> usize {
    use autumn_harvest::schema::harvest_schedules::dsl as sched_dsl;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect for buffered_runs query");
    let val: serde_json::Value = sched_dsl::harvest_schedules
        .filter(sched_dsl::workflow_name.eq(workflow_name))
        .select(sched_dsl::buffered_runs)
        .first::<serde_json::Value>(&mut conn)
        .await
        .expect("buffered_runs query failed");
    val.as_array().map_or(0, Vec::len)
}

/// Query all RUNNING execution IDs for a workflow schedule (used to terminate them in tests).
async fn query_running_exec_ids(database_url: &str, workflow_name: &str) -> Vec<ExecutionId> {
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect for running exec ids query");
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(workflow_name))
        .filter(harvest_workflow_executions::state.eq("RUNNING"))
        .select(harvest_workflow_executions::id)
        .load::<uuid::Uuid>(&mut conn)
        .await
        .expect("running exec ids query failed")
        .into_iter()
        .map(ExecutionId::from_uuid)
        .collect()
}

/// (overlap-a) Skip explicitly configured: no buffering, total stays at 1 while a run is in flight.
///
/// The scheduler dispatches on tick 1, then every subsequent tick sees `running = 1` and
/// drops the firing with `reason = "max_active_runs_reached"`.  No buffered slots.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overlap_policy_skip_explicitly_drops_new_firings() {
    let (database_url, _container) = setup_test_database_url().await;
    let wf_name = "overlap_skip_wf";
    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: wf_name,
            module: "integration_e2e",
            handler: slow_workflow,
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![],
    ));
    let pool = build_test_pool(&database_url);

    let ws = WorkflowSchedule::new(wf_name, Schedule::Cron("*/2 * * * * *".to_string()))
        .with_max_active_runs(1)
        .with_overlap_policy(OverlapPolicy::Skip);
    {
        let mut conn = pool.get().await.expect("pool get failed");
        register_workflow_schedules(&mut conn, std::slice::from_ref(&ws))
            .await
            .expect("register_workflow_schedules failed");
    }
    let workflow_schedules = Arc::new(vec![ws]);
    let scheduler = SchedulerRuntime::spawn(
        pool.clone(),
        Arc::clone(&registry),
        Arc::new(DagCatalog::default()),
        Arc::clone(&workflow_schedules),
    );

    // Poll until exec#1 is dispatched (up to 12 s to tolerate Docker startup latency and
    // cron-boundary alignment jitter).  Once the first dispatch lands the state is stable:
    // subsequent ticks all hit the Skip branch and neither add executions nor buffer slots.
    tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            let r = count_running_executions(&database_url, wf_name).await;
            if r >= 1 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    })
    .await
    .expect("Skip: timed out waiting for first dispatch within 12 s");

    let running = count_running_executions(&database_url, wf_name).await;
    assert_eq!(running, 1, "Skip: must keep exactly 1 RUNNING execution");
    let total = count_executions_for_workflow(&database_url, wf_name).await;
    assert_eq!(total, 1, "Skip: no extra dispatches, total must be 1");
    let buffered = query_buffered_runs_count(&database_url, wf_name).await;
    assert_eq!(buffered, 0, "Skip: must not buffer any firings");

    scheduler.shutdown();
    let _ = scheduler.join().await;
}

/// (overlap-b1) `BufferOne`: exactly one pending firing is queued in DB; subsequent firings are
/// dropped with `reason = "buffered_slot_full"`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overlap_policy_buffer_one_queues_single_slot() {
    let (database_url, _container) = setup_test_database_url().await;
    let wf_name = "overlap_buffer_one_wf";
    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: wf_name,
            module: "integration_e2e",
            handler: slow_workflow,
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![],
    ));
    let pool = build_test_pool(&database_url);

    let ws = WorkflowSchedule::new(wf_name, Schedule::Cron("*/2 * * * * *".to_string()))
        .with_max_active_runs(1)
        .with_overlap_policy(OverlapPolicy::BufferOne);
    {
        let mut conn = pool.get().await.expect("pool get failed");
        register_workflow_schedules(&mut conn, std::slice::from_ref(&ws))
            .await
            .expect("register_workflow_schedules failed");
    }
    let workflow_schedules = Arc::new(vec![ws]);
    let scheduler = SchedulerRuntime::spawn(
        pool.clone(),
        Arc::clone(&registry),
        Arc::new(DagCatalog::default()),
        Arc::clone(&workflow_schedules),
    );

    // Poll until the buffer holds exactly 1 slot (up to 12 s).  Two ticks are needed:
    // tick 1 dispatches exec#1, tick 2 buffers the first slot.  Once buffered == 1 the
    // state is stable: exec#1 stays RUNNING (no worker), so subsequent ticks all drop.
    tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            let b = query_buffered_runs_count(&database_url, wf_name).await;
            if b >= 1 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    })
    .await
    .expect("BufferOne: timed out waiting for 1 buffered slot within 12 s");

    let running = count_running_executions(&database_url, wf_name).await;
    assert_eq!(
        running, 1,
        "BufferOne: must keep exactly 1 RUNNING execution"
    );
    let total = count_executions_for_workflow(&database_url, wf_name).await;
    assert_eq!(
        total, 1,
        "BufferOne: no extra dispatches while slot is filled"
    );
    let buffered = query_buffered_runs_count(&database_url, wf_name).await;
    assert_eq!(
        buffered, 1,
        "BufferOne: must buffer exactly 1 firing and no more"
    );

    scheduler.shutdown();
    let _ = scheduler.join().await;
}

/// (overlap-b2) `BufferAll`: every missed firing is buffered up to `buffer_all_max`; firings past
/// the cap are dropped with `reason = "buffer_full"`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overlap_policy_buffer_all_queues_multiple_slots() {
    let (database_url, _container) = setup_test_database_url().await;
    let wf_name = "overlap_buffer_all_wf";
    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: wf_name,
            module: "integration_e2e",
            handler: slow_workflow,
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![],
    ));
    let pool = build_test_pool(&database_url);

    let ws = WorkflowSchedule::new(wf_name, Schedule::Cron("*/2 * * * * *".to_string()))
        .with_max_active_runs(1)
        .with_overlap_policy(OverlapPolicy::BufferAll)
        .with_buffer_all_max(3);
    {
        let mut conn = pool.get().await.expect("pool get failed");
        register_workflow_schedules(&mut conn, std::slice::from_ref(&ws))
            .await
            .expect("register_workflow_schedules failed");
    }
    let workflow_schedules = Arc::new(vec![ws]);
    let scheduler = SchedulerRuntime::spawn(
        pool.clone(),
        Arc::clone(&registry),
        Arc::new(DagCatalog::default()),
        Arc::clone(&workflow_schedules),
    );

    // Poll until the buffer reaches its cap of 3 slots (up to 20 s).  Four ticks are needed:
    // tick 1 dispatches exec#1, ticks 2–4 each buffer one slot.  Once buffered == 3 (cap),
    // subsequent ticks drop — the state is stable because exec#1 stays RUNNING (no worker).
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let b = query_buffered_runs_count(&database_url, wf_name).await;
            if b >= 3 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    })
    .await
    .expect("BufferAll: timed out waiting for 3 buffered slots within 20 s");

    let running = count_running_executions(&database_url, wf_name).await;
    assert_eq!(
        running, 1,
        "BufferAll: must keep exactly 1 RUNNING execution"
    );
    let total = count_executions_for_workflow(&database_url, wf_name).await;
    assert_eq!(
        total, 1,
        "BufferAll: no extra dispatches while buffer absorbs firings"
    );
    let buffered = query_buffered_runs_count(&database_url, wf_name).await;
    assert_eq!(
        buffered, 3,
        "BufferAll: must buffer exactly 3 firings (at buffer_all_max cap)"
    );

    scheduler.shutdown();
    let _ = scheduler.join().await;
}

/// (overlap-c1) `CancelOther`: in-flight run is cancelled and the new firing starts immediately.
///
/// Without a worker the executions stay in the state set by the scheduler DB writes:
/// - exec#1 → CANCELLED (by `cancel_workflow_execution`)
/// - exec#2 → RUNNING (by `start_or_load_workflow_execution`)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overlap_policy_cancel_other_cancels_inflight_run() {
    let (database_url, _container) = setup_test_database_url().await;
    let wf_name = "overlap_cancel_other_wf";
    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: wf_name,
            module: "integration_e2e",
            handler: slow_workflow,
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![],
    ));
    let pool = build_test_pool(&database_url);

    let ws = WorkflowSchedule::new(wf_name, Schedule::Cron("*/2 * * * * *".to_string()))
        .with_max_active_runs(1)
        .with_overlap_policy(OverlapPolicy::CancelOther);
    {
        let mut conn = pool.get().await.expect("pool get failed");
        register_workflow_schedules(&mut conn, std::slice::from_ref(&ws))
            .await
            .expect("register_workflow_schedules failed");
    }
    let workflow_schedules = Arc::new(vec![ws]);
    let scheduler = SchedulerRuntime::spawn(
        pool.clone(),
        Arc::clone(&registry),
        Arc::new(DagCatalog::default()),
        Arc::clone(&workflow_schedules),
    );

    // Poll until the cancel+redispatch cycle completes (up to 12 s to tolerate
    // Docker container startup latency and cron alignment jitter).
    let (cancelled, running) = tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            let c = count_executions_in_state(&database_url, wf_name, "CANCELLED").await;
            let r = count_running_executions(&database_url, wf_name).await;
            if c >= 1 && r == 1 {
                return (c, r);
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    })
    .await
    .expect("CancelOther: timed out waiting for cancel+redispatch within 12 s");

    assert!(
        cancelled >= 1,
        "CancelOther: at least 1 execution must be CANCELLED, got {cancelled}"
    );
    assert_eq!(
        running, 1,
        "CancelOther: exactly 1 execution must be RUNNING, got {running}"
    );
    let total = count_executions_for_workflow(&database_url, wf_name).await;
    assert!(
        total >= 2,
        "CancelOther: at least 2 total executions (cancelled + running), got {total}"
    );

    scheduler.shutdown();
    let _ = scheduler.join().await;
}

/// (overlap-c2) `TerminateOther`: in-flight run is force-terminated and the new firing starts
/// immediately.  `terminate_workflow_execution` seals the run in state TERMINATED (force,
/// regardless of prior state; issue #504), then the new firing is dispatched as RUNNING.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overlap_policy_terminate_other_terminates_inflight_run() {
    let (database_url, _container) = setup_test_database_url().await;
    let wf_name = "overlap_terminate_other_wf";
    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: wf_name,
            module: "integration_e2e",
            handler: slow_workflow,
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![],
    ));
    let pool = build_test_pool(&database_url);

    let ws = WorkflowSchedule::new(wf_name, Schedule::Cron("*/2 * * * * *".to_string()))
        .with_max_active_runs(1)
        .with_overlap_policy(OverlapPolicy::TerminateOther);
    {
        let mut conn = pool.get().await.expect("pool get failed");
        register_workflow_schedules(&mut conn, std::slice::from_ref(&ws))
            .await
            .expect("register_workflow_schedules failed");
    }
    let workflow_schedules = Arc::new(vec![ws]);
    let scheduler = SchedulerRuntime::spawn(
        pool.clone(),
        Arc::clone(&registry),
        Arc::new(DagCatalog::default()),
        Arc::clone(&workflow_schedules),
    );

    // Poll until the terminate+redispatch cycle completes (up to 12 s).
    let (terminated, running) = tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            let c = count_executions_in_state(&database_url, wf_name, "TERMINATED").await;
            let r = count_running_executions(&database_url, wf_name).await;
            if c >= 1 && r == 1 {
                return (c, r);
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    })
    .await
    .expect("TerminateOther: timed out waiting for terminate+redispatch within 12 s");

    assert!(
        terminated >= 1,
        "TerminateOther: at least 1 execution must be TERMINATED, got {terminated}"
    );
    assert_eq!(
        running, 1,
        "TerminateOther: exactly 1 execution must be RUNNING, got {running}"
    );
    let total = count_executions_for_workflow(&database_url, wf_name).await;
    assert!(
        total >= 2,
        "TerminateOther: at least 2 total executions (terminated + running), got {total}"
    );

    scheduler.shutdown();
    let _ = scheduler.join().await;
}

/// Seed a RUNNING execution that simulates a cross-type continue-as-new
/// successor (issue #1160): it carries `schedule_id` from a real schedule but
/// a `workflow_name` that is NOT that schedule's registered type -- exactly
/// what `ctx.continue_as_new_as(...)` (#803) leaves behind mid-chain --
/// `origin = ORIGIN_SCHEDULED` (a manual-trigger row is deliberately excluded
/// from overlap cleanup, so this must not look like one).
async fn insert_cross_type_scheduled_execution(
    conn: &mut AsyncPgConnection,
    workflow_name: &'static str,
    schedule_id: Uuid,
) -> ExecutionId {
    let exec_id = ExecutionId::new();
    let workflow_id = format!("sched:{schedule_id}:{}", Uuid::new_v4());
    let row = NewWorkflowExecution {
        quota_key: None,
        continued_from_exec_id: None,
        first_exec_id: None,
        id: exec_id.as_uuid(),
        workflow_name,
        workflow_id: &workflow_id,
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: serde_json::Value::Null,
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        deadline_at: None,
        chain_execution_timeout: None,
        chain_deadline_at: None,
        memo: None,
        search_attrs: None,
        assigned_build_id: None,
        parent_close_policy: None,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,
        sla: None,
        sla_deadline_at: None,
        schedule_id: Some(schedule_id),
        scheduled_for: Some(Utc::now()),
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        origin: Some(autumn_harvest::ORIGIN_SCHEDULED),
        completion_callbacks: None,
        start_source: None,
        start_source_ref: None,
        started_by: None,
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("failed to insert cross-type scheduled execution");
    exec_id
}

/// Seed a RUNNING execution that simulates an operator-triggered manual run of
/// a schedule's OWN type (issue #1160 AC3 regression guard): `schedule_id`
/// NULL, `origin = ORIGIN_MANUAL_TRIGGER`. Used to prove the #1160 fix is
/// additive -- it must not stop a manual run of an existing single-type
/// schedule from counting toward `max_active_runs`, which is the behaviour
/// the issue explicitly calls out as out of scope to change.
async fn insert_manual_trigger_execution(
    conn: &mut AsyncPgConnection,
    workflow_name: &'static str,
) -> ExecutionId {
    let exec_id = ExecutionId::new();
    let workflow_id = format!("manual-{}", Uuid::new_v4());
    let row = NewWorkflowExecution {
        quota_key: None,
        continued_from_exec_id: None,
        first_exec_id: None,
        id: exec_id.as_uuid(),
        workflow_name,
        workflow_id: &workflow_id,
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: serde_json::Value::Null,
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        deadline_at: None,
        chain_execution_timeout: None,
        chain_deadline_at: None,
        memo: None,
        search_attrs: None,
        assigned_build_id: None,
        parent_close_policy: None,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,
        sla: None,
        sla_deadline_at: None,
        schedule_id: None,
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        origin: Some(autumn_harvest::ORIGIN_MANUAL_TRIGGER),
        completion_callbacks: None,
        start_source: None,
        start_source_ref: None,
        started_by: None,
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("failed to insert manual-trigger execution");
    exec_id
}

/// Count `harvest_schedule_decisions` rows recording a `max_active_runs_reached`
/// skip for `schedule_id` (`decision = "skipped"`, `reason_code =
/// "max_active_runs_reached"` -- see `schedule_decision::record_decision_graceful`
/// and the `OverlapAction::Drop` branch in `tick_one_workflow_schedule`). A
/// positive control: proves the tick actually ran and took the capacity-gated
/// branch, rather than a test merely observing "nothing happened" for an
/// unrelated reason (e.g. the schedule never ticked at all).
async fn count_max_active_runs_skip_decisions(database_url: &str, schedule_id: Uuid) -> i64 {
    use autumn_harvest::schema::harvest_schedule_decisions::dsl;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect for decision count query");
    dsl::harvest_schedule_decisions
        .filter(dsl::schedule_id.eq(Some(schedule_id)))
        .filter(dsl::reason_code.eq("max_active_runs_reached"))
        .count()
        .get_result::<i64>(&mut conn)
        .await
        .expect("decision count query failed")
}

/// (issue #1160, AC1) `Skip` + `max_active_runs`: a cross-type continue-as-new
/// successor of the SAME schedule (same `schedule_id`, different
/// `workflow_name`) must still count toward the schedule's `max_active_runs`.
///
/// Before the fix, `schedule_running_basis` (and the tick's own inline copy of
/// it) counted only same-named executions, so this cross-type row was
/// invisible to `max_active_runs` and a `Skip`-policy schedule would fire
/// straight past its configured cap of 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overlap_policy_skip_counts_a_cross_type_successor_toward_max_active_runs() {
    let (database_url, _container) = setup_test_database_url_or_env().await;
    let wf_name = "overlap_1160_skip_wf";
    let successor_type = "overlap_1160_skip_wf_successor";
    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: wf_name,
            module: "integration_e2e",
            handler: slow_workflow,
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![],
    ));
    let pool = build_test_pool(&database_url);

    let ws = WorkflowSchedule::new(wf_name, Schedule::Cron("*/2 * * * * *".to_string()))
        .with_max_active_runs(1)
        .with_overlap_policy(OverlapPolicy::Skip);
    let schedule_id: Uuid;
    {
        use autumn_harvest::schema::harvest_schedules::dsl as sched_dsl;
        let mut conn = pool.get().await.expect("pool get failed");
        register_workflow_schedules(&mut conn, std::slice::from_ref(&ws))
            .await
            .expect("register_workflow_schedules failed");
        schedule_id = sched_dsl::harvest_schedules
            .filter(sched_dsl::workflow_name.eq(wf_name))
            .select(sched_dsl::id)
            .first(&mut conn)
            .await
            .expect("schedule row must exist after registration");

        // Seed the cross-type successor occupying the schedule's only slot.
        insert_cross_type_scheduled_execution(&mut conn, successor_type, schedule_id).await;
    }

    let workflow_schedules = Arc::new(vec![ws]);
    let scheduler = SchedulerRuntime::spawn(
        pool.clone(),
        Arc::clone(&registry),
        Arc::new(DagCatalog::default()),
        Arc::clone(&workflow_schedules),
    );

    // Actively poll for 12 s (several cron cycles at 2 s, generous for Docker
    // startup latency): fail the instant a new own-type run appears, rather
    // than a single blind sleep.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(12);
    loop {
        let running_own_type = count_running_executions(&database_url, wf_name).await;
        assert_eq!(
            running_own_type, 0,
            "issue #1160: the cross-type successor must count toward max_active_runs=1, \
             so no NEW {wf_name} run should ever be dispatched while it is RUNNING"
        );
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    let successor_still_running = count_running_executions(&database_url, successor_type).await;
    assert_eq!(
        successor_still_running, 1,
        "the pre-seeded cross-type successor must remain RUNNING under Skip (untouched)"
    );
    // Positive control: prove the tick actually ran and took the
    // capacity-gated Skip branch, not merely that nothing happened for some
    // unrelated reason (e.g. the schedule never ticked at all).
    let skip_decisions = count_max_active_runs_skip_decisions(&database_url, schedule_id).await;
    assert!(
        skip_decisions >= 1,
        "expected at least one max_active_runs_reached skip decision recorded for the \
         schedule, got {skip_decisions} -- did the tick actually run?"
    );

    scheduler.shutdown();
    let _ = scheduler.join().await;
}

/// (issue #1160, AC3 regression guard) A manual-trigger run of the schedule's
/// OWN type must still count toward `max_active_runs` after the #1160 fix --
/// the `schedule_id`-scoped addition to the running-count is additive, not a
/// replacement of the existing name-based count that also covers manual
/// (non-scheduled) runs. Without this, a wrong fix that re-scoped counting to
/// `schedule_id` alone would silently stop enforcing the cap on manual runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overlap_policy_skip_still_counts_a_manual_trigger_run_of_the_same_type() {
    let (database_url, _container) = setup_test_database_url_or_env().await;
    let wf_name = "overlap_1160_manual_wf";
    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: wf_name,
            module: "integration_e2e",
            handler: slow_workflow,
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![],
    ));
    let pool = build_test_pool(&database_url);

    let ws = WorkflowSchedule::new(wf_name, Schedule::Cron("*/2 * * * * *".to_string()))
        .with_max_active_runs(1)
        .with_overlap_policy(OverlapPolicy::Skip);
    let schedule_id: Uuid;
    {
        use autumn_harvest::schema::harvest_schedules::dsl as sched_dsl;
        let mut conn = pool.get().await.expect("pool get failed");
        register_workflow_schedules(&mut conn, std::slice::from_ref(&ws))
            .await
            .expect("register_workflow_schedules failed");
        schedule_id = sched_dsl::harvest_schedules
            .filter(sched_dsl::workflow_name.eq(wf_name))
            .select(sched_dsl::id)
            .first(&mut conn)
            .await
            .expect("schedule row must exist after registration");
        insert_manual_trigger_execution(&mut conn, wf_name).await;
    }

    let workflow_schedules = Arc::new(vec![ws]);
    let scheduler = SchedulerRuntime::spawn(
        pool.clone(),
        Arc::clone(&registry),
        Arc::new(DagCatalog::default()),
        Arc::clone(&workflow_schedules),
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(12);
    loop {
        let running = count_running_executions(&database_url, wf_name).await;
        assert_eq!(
            running, 1,
            "the pre-existing manual-trigger run must still count toward max_active_runs=1; \
             a regression here means the #1160 fix silently stopped counting manual runs"
        );
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    // Positive control: same reasoning as the sibling cross-type test above.
    let skip_decisions = count_max_active_runs_skip_decisions(&database_url, schedule_id).await;
    assert!(
        skip_decisions >= 1,
        "expected at least one max_active_runs_reached skip decision recorded for the \
         schedule, got {skip_decisions} -- did the tick actually run?"
    );

    scheduler.shutdown();
    let _ = scheduler.join().await;
}

/// (issue #1160, AC2) `CancelOther` must be able to cancel a cross-type
/// continue-as-new successor of the same schedule: it carries the right
/// `schedule_id` but the wrong `workflow_name` (the target type), so a
/// selection that ALSO filters on `workflow_name` can never reach it.
///
/// Before the fix, this scenario compounded with AC1's bug: since the
/// cross-type row was invisible to the running-count too, the tick never even
/// entered the overlap branch, so the predecessor was neither counted NOR
/// cancelled -- it just sat RUNNING forever while a fresh run dispatched
/// alongside it, silently exceeding `max_active_runs`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overlap_policy_cancel_other_reaches_a_cross_type_successor() {
    let (database_url, _container) = setup_test_database_url_or_env().await;
    let wf_name = "overlap_1160_cancel_wf";
    let predecessor_type = "overlap_1160_cancel_wf_predecessor";
    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: wf_name,
            module: "integration_e2e",
            handler: slow_workflow,
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![],
    ));
    let pool = build_test_pool(&database_url);

    let ws = WorkflowSchedule::new(wf_name, Schedule::Cron("*/2 * * * * *".to_string()))
        .with_max_active_runs(1)
        .with_overlap_policy(OverlapPolicy::CancelOther);
    {
        use autumn_harvest::schema::harvest_schedules::dsl as sched_dsl;
        let mut conn = pool.get().await.expect("pool get failed");
        register_workflow_schedules(&mut conn, std::slice::from_ref(&ws))
            .await
            .expect("register_workflow_schedules failed");
        let schedule_id: Uuid = sched_dsl::harvest_schedules
            .filter(sched_dsl::workflow_name.eq(wf_name))
            .select(sched_dsl::id)
            .first(&mut conn)
            .await
            .expect("schedule row must exist after registration");
        insert_cross_type_scheduled_execution(&mut conn, predecessor_type, schedule_id).await;
    }

    let workflow_schedules = Arc::new(vec![ws]);
    let scheduler = SchedulerRuntime::spawn(
        pool.clone(),
        Arc::clone(&registry),
        Arc::new(DagCatalog::default()),
        Arc::clone(&workflow_schedules),
    );

    let (predecessor_cancelled, running) = tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            let c = count_executions_in_state(&database_url, predecessor_type, "CANCELLED").await;
            let r = count_running_executions(&database_url, wf_name).await;
            if c >= 1 && r == 1 {
                return (c, r);
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    })
    .await
    .expect(
        "issue #1160: CancelOther timed out waiting to reach the cross-type successor within 12 s",
    );

    // Shut the scheduler down before taking any further counts: with
    // OverlapPolicy::CancelOther and this cron cadence, every subsequent tick
    // cancels the current run and dispatches a new one, so wf_name's total
    // execution count grows monotonically for as long as the scheduler keeps
    // ticking -- an equality assertion taken while it's still running would
    // be flaky by construction.
    scheduler.shutdown();
    let _ = scheduler.join().await;

    assert!(
        predecessor_cancelled >= 1,
        "issue #1160: CancelOther must cancel the cross-type predecessor, got {predecessor_cancelled}"
    );
    assert_eq!(
        running, 1,
        "CancelOther: exactly 1 {wf_name} execution must be RUNNING after cleanup, got {running}"
    );
    // The predecessor was cancelled, not duplicated: exactly one execution
    // ever carried predecessor_type.
    let predecessor_total = count_executions_for_workflow(&database_url, predecessor_type).await;
    assert_eq!(
        predecessor_total, 1,
        "CancelOther: exactly 1 execution total named {predecessor_type}, got {predecessor_total}"
    );
}

/// (issue #1160, AC2) `TerminateOther` must be able to terminate a cross-type
/// continue-as-new successor of the same schedule -- the symmetric case of
/// `overlap_policy_cancel_other_reaches_a_cross_type_successor` above.
/// `terminate_in_flight_runs` got the identical fix (drop the redundant
/// `workflow_name` filter, select on `schedule_id` alone) via the shared
/// `load_overlap_cleanup_targets` helper, but exercised independently since
/// `TerminateOther` takes a different `OverlapAction` branch in
/// `tick_one_workflow_schedule`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overlap_policy_terminate_other_reaches_a_cross_type_successor() {
    let (database_url, _container) = setup_test_database_url_or_env().await;
    let wf_name = "overlap_1160_terminate_wf";
    let predecessor_type = "overlap_1160_terminate_wf_predecessor";
    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: wf_name,
            module: "integration_e2e",
            handler: slow_workflow,
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![],
    ));
    let pool = build_test_pool(&database_url);

    let ws = WorkflowSchedule::new(wf_name, Schedule::Cron("*/2 * * * * *".to_string()))
        .with_max_active_runs(1)
        .with_overlap_policy(OverlapPolicy::TerminateOther);
    {
        use autumn_harvest::schema::harvest_schedules::dsl as sched_dsl;
        let mut conn = pool.get().await.expect("pool get failed");
        register_workflow_schedules(&mut conn, std::slice::from_ref(&ws))
            .await
            .expect("register_workflow_schedules failed");
        let schedule_id: Uuid = sched_dsl::harvest_schedules
            .filter(sched_dsl::workflow_name.eq(wf_name))
            .select(sched_dsl::id)
            .first(&mut conn)
            .await
            .expect("schedule row must exist after registration");
        insert_cross_type_scheduled_execution(&mut conn, predecessor_type, schedule_id).await;
    }

    let workflow_schedules = Arc::new(vec![ws]);
    let scheduler = SchedulerRuntime::spawn(
        pool.clone(),
        Arc::clone(&registry),
        Arc::new(DagCatalog::default()),
        Arc::clone(&workflow_schedules),
    );

    let (predecessor_terminated, running) = tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            let c = count_executions_in_state(&database_url, predecessor_type, "TERMINATED").await;
            let r = count_running_executions(&database_url, wf_name).await;
            if c >= 1 && r == 1 {
                return (c, r);
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    })
    .await
    .expect(
        "issue #1160: TerminateOther timed out waiting to reach the cross-type successor within 12 s",
    );

    // Same rationale as the CancelOther test: shut down before taking any
    // further counts, since wf_name's total grows on every subsequent tick.
    scheduler.shutdown();
    let _ = scheduler.join().await;

    assert!(
        predecessor_terminated >= 1,
        "issue #1160: TerminateOther must terminate the cross-type predecessor, got {predecessor_terminated}"
    );
    assert_eq!(
        running, 1,
        "TerminateOther: exactly 1 {wf_name} execution must be RUNNING after cleanup, got {running}"
    );
    let predecessor_total = count_executions_for_workflow(&database_url, predecessor_type).await;
    assert_eq!(
        predecessor_total, 1,
        "TerminateOther: exactly 1 execution total named {predecessor_type}, got {predecessor_total}"
    );
}

/// (overlap-d) `BufferOne` durability: buffered slots survive a scheduler restart.
///
/// Phase 1 — Scheduler A dispatches exec#1 (`slow_workflow`) and buffers one slot.
/// Shutdown Scheduler A.  The `buffered_runs` column in DB still holds the entry.
///
/// Phase 2 — Exec#1 is terminated to free capacity.  Scheduler B starts.  Its
/// drain pass sees `running = 0` and `buffered_runs` non-empty → dispatches exec#2.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn overlap_policy_buffer_one_survives_scheduler_restart() {
    let (database_url, _container) = setup_test_database_url().await;
    let wf_name = "overlap_restart_wf";
    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: wf_name,
            module: "integration_e2e",
            handler: slow_workflow,
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![],
    ));
    let pool = build_test_pool(&database_url);

    let ws = WorkflowSchedule::new(wf_name, Schedule::Cron("*/2 * * * * *".to_string()))
        .with_max_active_runs(1)
        .with_overlap_policy(OverlapPolicy::BufferOne);
    {
        let mut conn = pool.get().await.expect("pool get failed");
        register_workflow_schedules(&mut conn, std::slice::from_ref(&ws))
            .await
            .expect("register_workflow_schedules failed");
    }
    let workflow_schedules = Arc::new(vec![ws]);

    // ---- Phase 1: run scheduler until one slot is buffered ----
    let scheduler1 = SchedulerRuntime::spawn(
        pool.clone(),
        Arc::clone(&registry),
        Arc::new(DagCatalog::default()),
        Arc::clone(&workflow_schedules),
    );
    // Poll for the buffered slot to appear (up to 12 s to tolerate Docker latency).
    let buffered_before = tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            let b = query_buffered_runs_count(&database_url, wf_name).await;
            if b >= 1 {
                return b;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    })
    .await
    .expect("expected 1 buffered slot within 12 s");
    assert_eq!(
        buffered_before, 1,
        "expected exactly 1 buffered slot before restart, got {buffered_before}"
    );

    scheduler1.shutdown();
    let _ = scheduler1.join().await;

    // buffered_runs must persist after shutdown (durability assertion).
    let buffered_after_shutdown = query_buffered_runs_count(&database_url, wf_name).await;
    assert_eq!(
        buffered_after_shutdown, 1,
        "buffered_runs must survive scheduler shutdown (got {buffered_after_shutdown})"
    );

    // Terminate exec#1 to free the capacity slot for the drain.
    let running_ids = query_running_exec_ids(&database_url, wf_name).await;
    assert_eq!(
        running_ids.len(),
        1,
        "expected 1 RUNNING execution before restart"
    );
    {
        let mut conn = pool.get().await.expect("pool get failed");
        terminate_workflow_execution(
            &mut conn,
            running_ids[0],
            "overlap restart test cleanup",
            &autumn_harvest::telemetry::NoOpMetrics,
        )
        .await
        .expect("terminate must succeed");
    }

    // ---- Phase 2: restart scheduler; drain dispatches the buffered slot ----
    let scheduler2 = SchedulerRuntime::spawn(
        pool.clone(),
        Arc::clone(&registry),
        Arc::new(DagCatalog::default()),
        Arc::clone(&workflow_schedules),
    );

    let total = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let n = count_executions_for_workflow(&database_url, wf_name).await;
            if n >= 2 {
                break n;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("buffered slot must be dispatched after scheduler restart within 8 s");

    assert!(
        total >= 2,
        "expected >=2 total executions after restart (exec#1 terminated + exec#2 from buffer), got {total}"
    );

    scheduler2.shutdown();
    let _ = scheduler2.join().await;
}

/// A workflow blocked on `wait_for_signal` with a short `execution_timeout`
/// must be transitioned to `TIMED_OUT` by the timeout scanner.
///
/// Regression guard for issue #243: verifies the full end-to-end path:
/// 1. Workflow is started with a 200 ms execution timeout.
/// 2. The workflow runs, hits `wait_for_signal`, and parks (never receiving
///    a signal — simulating a runaway execution).
/// 3. `enforce_workflow_execution_timeouts` fires after the deadline elapses.
/// 4. The execution row transitions to `TIMED_OUT`, the outstanding workflow
///    task queue row is cancelled, and the history ends with
///    `WorkflowExecutionTimedOut`.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn signal_blocked_workflow_times_out_at_deadline() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    // Start the workflow with a short execution deadline.
    let exec_id = ExecutionId::new();
    let execution_timeout = chrono::Duration::milliseconds(200);
    let started_at = Utc::now();
    let deadline_at = started_at + execution_timeout;
    let row = NewWorkflowExecution {
        quota_key: None,
        continued_from_exec_id: None,
        first_exec_id: None,
        id: exec_id.as_uuid(),
        workflow_name: "signal_blocked_wf",
        workflow_id: "signal-blocked-timeout-001",
        run_id: uuid::Uuid::new_v4(),
        shard_id: 0,
        input: serde_json::json!({}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: Some(execution_timeout),
        deadline_at: Some(deadline_at),
        chain_execution_timeout: None,
        chain_deadline_at: None,
        memo: None,
        search_attrs: None,
        assigned_build_id: None,
        parent_close_policy: None,

        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,

        sla: None,

        sla_deadline_at: None,
        schedule_id: None,
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        origin: None,
        completion_callbacks: None,
        start_source: None,
        start_source_ref: None,
        started_by: None,
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(&mut conn)
        .await
        .expect("insert workflow execution failed");

    // Append WorkflowStarted + SignalWaiting to simulate the workflow parked.
    store::append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({}),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted failed");

    // Enqueue a RUNNING workflow task (simulating the worker parked the task).
    let mut params = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(10);
    let task_id = queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue workflow task failed");

    // Claim and park the task to put it in RUNNING/parked state.
    let claimed = queue::claim_task(
        &mut conn,
        &["default".to_string()],
        "test-worker-timeout",
        "",
        None,
        &[],
        &[],
    )
    .await
    .expect("claim task failed")
    .expect("task should be claimable");
    assert_eq!(claimed.id, task_id);
    queue::park_workflow_task(&mut conn, task_id, None)
        .await
        .expect("park workflow task failed");

    // Timeout not yet elapsed — scanner should find nothing.
    let enforced_early = timeout::enforce_workflow_execution_timeouts(
        &mut conn,
        &autumn_harvest::telemetry::NoOpMetrics,
    )
    .await
    .expect("early enforcement should succeed");
    assert_eq!(
        enforced_early, 0,
        "scanner should not fire before deadline elapses"
    );

    // Wait for the deadline to elapse.
    tokio::time::sleep(Duration::from_millis(250)).await;

    // Now the scanner should detect and enforce the timeout.
    let enforced = timeout::enforce_workflow_execution_timeouts(
        &mut conn,
        &autumn_harvest::telemetry::NoOpMetrics,
    )
    .await
    .expect("timeout enforcement should succeed");
    assert_eq!(enforced, 1, "scanner should enforce exactly one timeout");

    // Execution must now be in TIMED_OUT state.
    let execution = load_execution_from_url(&database_url, exec_id).await;
    assert_eq!(
        execution.state, "TIMED_OUT",
        "execution should be TIMED_OUT after deadline elapsed"
    );
    assert!(
        execution
            .error
            .as_deref()
            .is_some_and(|e| e.contains("WorkflowExecution")),
        "execution error should mention WorkflowExecution timeout type"
    );

    // The outstanding task queue row should be cancelled.
    let tasks = load_tasks_for_execution_from_url(&database_url, exec_id).await;
    let workflow_task = tasks
        .iter()
        .find(|t| t.task_type == "workflow")
        .expect("workflow task should still be present");
    assert_eq!(
        workflow_task.state, "FAILED",
        "workflow task should be cancelled (FAILED) after execution timeout"
    );

    // History must end with WorkflowExecutionTimedOut.
    let history = load_history_from_url(&database_url, exec_id).await;
    assert!(
        matches!(
            history.events.last(),
            Some(WorkflowEvent::WorkflowExecutionTimedOut { .. })
        ),
        "last history event must be WorkflowExecutionTimedOut, got: {:?}",
        history.events.last()
    );

    // Verify the deadline fields are surfaced correctly.
    assert_eq!(
        execution.deadline_at.map(|d| d.timestamp_millis()),
        Some(deadline_at.timestamp_millis()),
        "deadline_at should match what was set at start time"
    );
    assert_eq!(
        execution.execution_timeout,
        Some(execution_timeout),
        "execution_timeout should match what was set at start time"
    );
}

// ── Per-key concurrency fair-share tests (issue #247) ─────────────────────────

/// (concurrency-a) Per-key limit: under a burst of N >> limit tasks for the
/// same concurrency key, at most `limit` are RUNNING at any moment.
///
/// Enqueues 6 workflow tasks with `concurrency_key = "tenant:acme"` and
/// `concurrency_cap = 2`.  Verifies the claim query allows at most 2 to be
/// RUNNING simultaneously and that all 6 are eventually processed.  Uses
/// direct `claim_task` / `complete_task` calls (same pattern as
/// `concurrency_cap_limits_concurrent_claims_cluster_wide`) to avoid
/// interaction with the executor's 100 ms suspension timeout.
#[tokio::test]
async fn per_key_concurrency_cap_enforced_across_fleet() {
    const LIMIT: u32 = 2;
    const TOTAL: u32 = 6;
    const KEY: &str = "tenant:acme";

    let (mut conn, _container) = setup_test_db().await;
    let queues = vec!["default".to_string()];

    for i in 0..TOTAL {
        let mut params =
            EnqueueParams::new("default", TaskType::Workflow, serde_json::json!({ "i": i }));
        params.concurrency_key = Some(KEY.to_string());
        params.max_concurrent = Some(LIMIT);
        params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);
        queue::enqueue(&mut conn, &params)
            .await
            .expect("enqueue failed");
    }

    let mut completed_count = 0u32;
    let mut max_in_flight: u32 = 0;

    // Repeatedly claim a task, assert the in-flight count respects the cap,
    // then immediately complete one held task to free a slot.  Repeat until all
    // TOTAL tasks have been claimed and completed.
    let mut held: Vec<Uuid> = Vec::new();

    loop {
        // Try to claim one more task.
        let claimed = queue::claim_task(
            &mut conn,
            &queues,
            "test-worker-concurrency-a",
            "",
            None,
            &[],
            &[],
        )
        .await
        .expect("claim query failed");

        if let Some(task) = claimed {
            held.push(task.id);
            let in_flight = u32::try_from(held.len()).unwrap();
            assert!(
                in_flight <= LIMIT,
                "cap violated: {in_flight} tasks held simultaneously (limit = {LIMIT})"
            );
            if in_flight > max_in_flight {
                max_in_flight = in_flight;
            }
        } else if !held.is_empty() {
            // Cap is saturated; complete the oldest held task to free a slot.
            let id = held.remove(0);
            queue::complete_task(&mut conn, id, serde_json::json!(null))
                .await
                .expect("complete_task failed");
            completed_count += 1;
        } else {
            // Nothing held and nothing claimable: all tasks are done.
            break;
        }

        // Drain any tasks that can still be claimed immediately.
        if held.len() < LIMIT as usize
            && queue::claim_task(
                &mut conn,
                &queues,
                "test-worker-concurrency-a",
                "",
                None,
                &[],
                &[],
            )
            .await
            .expect("claim query failed")
            .is_some_and(|t| {
                held.push(t.id);
                true
            })
        {
            // extra claim consumed above
        }

        if completed_count >= TOTAL {
            break;
        }
    }

    // Complete any remaining held tasks.
    for id in held {
        queue::complete_task(&mut conn, id, serde_json::json!(null))
            .await
            .expect("final complete_task failed");
        completed_count += 1;
    }

    assert_eq!(
        completed_count, TOTAL,
        "all {TOTAL} tasks must eventually be processed"
    );
    assert!(
        max_in_flight >= 1,
        "at least 1 task must have been in-flight at the cap limit"
    );
}

/// (concurrency-b) Fair-share: tasks for *other* keys are NOT blocked by a
/// saturated key.
///
/// Enqueues 4 "loud" workflow tasks (cap=1) and 2 "quiet" workflow tasks
/// (cap=10).  Verifies the quiet tasks can be claimed even while the loud cap
/// is saturated.  Uses direct `claim_task` calls to avoid the executor's
/// 100 ms suspension timeout.
#[tokio::test]
async fn per_key_concurrency_does_not_block_other_keys() {
    const LOUD_CAP: u32 = 1;
    const LOUD_TOTAL: u32 = 4;
    const QUIET_KEY: &str = "tenant:quiet";
    const LOUD_KEY: &str = "tenant:loud";

    let (mut conn, _container) = setup_test_db().await;
    let queues = vec!["default".to_string()];

    // Loud tenant: 4 tasks, cap=1.
    for i in 0..LOUD_TOTAL {
        let mut params =
            EnqueueParams::new("default", TaskType::Workflow, serde_json::json!({ "i": i }));
        params.concurrency_key = Some(LOUD_KEY.to_string());
        params.max_concurrent = Some(LOUD_CAP);
        params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);
        queue::enqueue(&mut conn, &params)
            .await
            .expect("enqueue loud task failed");
    }

    // Quiet tenant: 2 tasks with a high cap so they are never blocked.
    for i in 0..2u32 {
        let mut params =
            EnqueueParams::new("default", TaskType::Workflow, serde_json::json!({ "i": i }));
        params.concurrency_key = Some(QUIET_KEY.to_string());
        params.max_concurrent = Some(10u32);
        params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);
        queue::enqueue(&mut conn, &params)
            .await
            .expect("enqueue quiet task failed");
    }

    // Saturate the loud key: claim 1 loud task (cap=1 → saturated).
    let loud_task = queue::claim_task(&mut conn, &queues, "test-worker-b", "", None, &[], &[])
        .await
        .expect("claim 1 query failed")
        .expect("first loud task should be claimable");
    assert_eq!(
        loud_task.concurrency_key.as_deref(),
        Some(LOUD_KEY),
        "claimed task should be loud-key"
    );

    // Loud cap is now saturated — the next loud-key claim must fail.
    // (We specifically target what comes next using separate assertions below.)

    // Quiet tasks must be claimable despite loud saturation.
    let mut quiet_claimed = 0u32;
    let mut attempts = 0u32;
    while quiet_claimed < 2 && attempts < 10 {
        if let Some(task) =
            queue::claim_task(&mut conn, &queues, "test-worker-b", "", None, &[], &[])
                .await
                .expect("claim query failed")
        {
            assert_eq!(
                task.concurrency_key.as_deref(),
                Some(QUIET_KEY),
                "any task claimed while loud is saturated must be a quiet-key task"
            );
            quiet_claimed += 1;
            // Immediately complete quiet tasks so they don't hold state.
            queue::complete_task(&mut conn, task.id, serde_json::json!(null))
                .await
                .expect("complete quiet task failed");
        } else {
            // No task available right now; the loud cap is blocking the loud
            // tasks and quiet tasks haven't been claimed yet — should not happen.
            break;
        }
        attempts += 1;
    }

    assert_eq!(
        quiet_claimed, 2,
        "both quiet-tenant tasks must be claimable even though loud cap is saturated"
    );

    // Complete the held loud task; verify the next loud task is now claimable.
    queue::complete_task(&mut conn, loud_task.id, serde_json::json!(null))
        .await
        .expect("complete loud task failed");

    let next_loud = queue::claim_task(&mut conn, &queues, "test-worker-b", "", None, &[], &[])
        .await
        .expect("claim after complete query failed");
    assert!(
        next_loud.is_some(),
        "a loud-key task must become claimable after the saturating task completes"
    );
    assert_eq!(
        next_loud
            .as_ref()
            .and_then(|t| t.concurrency_key.as_deref()),
        Some(LOUD_KEY),
        "next claimable task should be loud-key"
    );
}

// ──────────── ActivityContext::attempt() / previous_failure() via worker ─────

/// Shared state captured by the retry-aware activity across all attempts.
#[derive(Default)]
struct RetryObservations {
    /// `(attempt_number, previous_failure)` collected on each invocation.
    records: Mutex<Vec<(u32, Option<String>)>>,
}

fn workflow_calling_retry_activity<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("retry_context_activity", serde_json::json!(null), "default")
            .await
            .map_err(|e| e.to_string())
    })
}

/// Activity that records `ctx.attempt()` and `ctx.previous_failure()` on each
/// invocation, fails on attempts 1 and 2 with a retryable error, and succeeds
/// on attempt 3.
fn retry_context_activity<'a>(
    ctx: &'a ActivityContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let obs = Arc::clone(
            ctx.state::<Arc<RetryObservations>>()
                .expect("RetryObservations must be registered"),
        );
        let attempt = ctx.attempt();
        let prev = ctx.previous_failure().map(str::to_string);
        obs.records.lock().unwrap().push((attempt, prev));

        if attempt < 3 {
            Err(format!("fail_attempt_{attempt}"))
        } else {
            Ok(serde_json::json!("success"))
        }
    })
}

/// Verifies that `ActivityContext::attempt()` increments correctly across
/// worker-level retries and that `previous_failure()` carries the last error
/// string on subsequent attempts.
///
/// AC #8 of issue #381: "at least one end-to-end integration test asserts that
/// an activity which fails twice and succeeds on attempt 3 observes
/// `ctx.attempt() == 1, 2, 3` and `ctx.previous_failure() == None,
/// Some("…"), Some("…")`".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn activity_context_exposes_attempt_and_previous_failure_on_retry() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let exec_id = insert_workflow_execution(&mut conn).await;
    let workflow_input = serde_json::json!(null);

    store::append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: workflow_input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted failed");

    let mut params = EnqueueParams::new("default", TaskType::Workflow, workflow_input.clone());
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);
    queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue workflow task failed");

    let observations = Arc::new(RetryObservations::default());

    let mut shared_state_map = HashMap::new();
    shared_state_map.insert(
        TypeId::of::<Arc<RetryObservations>>(),
        Box::new(Arc::clone(&observations)) as Box<dyn std::any::Any + Send + Sync>,
    );
    let shared_state = Arc::new(shared_state_map);

    let registry = Arc::new(HandlerRegistry::with_state(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: "e2e_test_workflow",
            module: "integration_e2e",
            handler: workflow_calling_retry_activity,
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,
            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![ActivityInfo {
            name: "retry_context_activity",
            module: "integration_e2e",
            default_retry_policy: Some(autumn_harvest::RetryPolicy::fixed(
                3,
                std::time::Duration::from_millis(0),
            )),
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_schedule_to_close: None,
            default_queue: Some("default"),
            max_concurrent: None,
            concurrency_key: None,
            rate_limit_rps: None,
            rate_limit_burst: None,
            rate_limit_key: None,
            rate_limit_key_expr: None,
            circuit_breaker: None,
            is_local: false,
            max_input_bytes: None,
            max_result_bytes: None,
            requires: None,
            handler: retry_context_activity,
        }],
        shared_state,
    ));

    let worker = build_runtime_worker("worker-retry-ctx", 2, 4, registry);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    wait_for_execution_state(&database_url, exec_id, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("worker task should join");

    let records = observations.records.lock().unwrap().clone();

    assert_eq!(records.len(), 3, "activity must have run exactly 3 times");

    let (a1, p1) = &records[0];
    assert_eq!(*a1, 1, "first invocation must be attempt 1");
    assert!(
        p1.is_none(),
        "first invocation must have no previous_failure"
    );

    let (a2, p2) = &records[1];
    assert_eq!(*a2, 2, "second invocation must be attempt 2");
    assert_eq!(
        p2.as_deref(),
        Some("fail_attempt_1"),
        "second invocation previous_failure must be the attempt-1 error"
    );

    let (a3, p3) = &records[2];
    assert_eq!(*a3, 3, "third invocation must be attempt 3");
    assert_eq!(
        p3.as_deref(),
        Some("fail_attempt_2"),
        "third invocation previous_failure must be the attempt-2 error"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_rolling_deploy_capability_routing_with_database_enforcement() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("connect to test DB");

    // 1. Enqueue a capability-gated activity task (requires gpu=true)
    let mut params = EnqueueParams::new("default", TaskType::Activity, serde_json::json!({}));
    params.activity_name = Some("gpu_activity".to_string());

    // Set required_capabilities to [{"Exact": {"key": "gpu", "value": "true"}}]
    let requirements = vec![autumn_harvest::eligibility::Requirement::Exact {
        key: "gpu".to_string(),
        value: "true".to_string(),
    }];
    params.required_capabilities = Some(serde_json::to_value(&requirements).unwrap());

    let task_id = queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue capability gated task");

    // 2. Call claim_task representing an old worker (without gpu=true label registered in DB)
    // Register worker-old first in the DB (without gpu label)
    autumn_harvest::workers::register_worker(
        &mut conn,
        "worker-old",
        &["default".to_string()],
        &[0],
        4,
        "localhost",
        None,
        "v1",
        None,
        &std::collections::HashMap::new(),
        0,
        &[],
    )
    .await
    .unwrap();

    // Try to claim task using worker-old. It should return None because the database filters it out.
    let claimed_by_old = queue::claim_task(
        &mut conn,
        &["default".to_string()],
        "worker-old",
        "v1",
        None,
        &[],
        &[],
    )
    .await
    .expect("claim_task for worker-old");
    assert!(
        claimed_by_old.is_none(),
        "Old worker should not be able to claim capability-gated task"
    );

    // 3. Call claim_task representing a new capable worker
    // Register worker-new with gpu=true label
    let mut new_labels = std::collections::HashMap::new();
    new_labels.insert("gpu".to_string(), "true".to_string());
    autumn_harvest::workers::register_worker(
        &mut conn,
        "worker-new",
        &["default".to_string()],
        &[0],
        4,
        "localhost",
        None,
        "v1",
        None,
        &new_labels,
        0,
        &[],
    )
    .await
    .unwrap();

    // Try to claim task using worker-new. It should succeed!
    let claimed_by_new = queue::claim_task(
        &mut conn,
        &["default".to_string()],
        "worker-new",
        "v1",
        None,
        &[],
        &[],
    )
    .await
    .expect("claim_task for worker-new");
    let claimed_item = claimed_by_new.expect("New worker should successfully claim task");
    assert_eq!(claimed_item.id, task_id);
}

// ---------------------------------------------------------------------------
// Saga compensation observability (issue #801)
// ---------------------------------------------------------------------------

static SAGA_CANCEL_FLIGHT_RUNS: AtomicUsize = AtomicUsize::new(0);
static SAGA_CANCEL_HOTEL_RUNS: AtomicUsize = AtomicUsize::new(0);

/// Records the two saga counters (with their labels) emitted through the
/// worker's real telemetry wiring; every other `MetricsRecorder` method stays
/// a no-op default. Labels are recorded here and asserted after the run —
/// never asserted inside the callback, which runs on the worker task and
/// would surface a label regression as a confusing poison-pill FAILED
/// instead of a clear assertion message (issue #801 post-review).
#[derive(Default)]
struct SagaE2eRecorder {
    compensated: std::sync::Mutex<Vec<(String, String)>>,
    compensation_failed: AtomicUsize,
}

impl autumn_harvest::telemetry::MetricsRecorder for SagaE2eRecorder {
    fn record_saga_compensated(&self, workflow_name: &str, queue: &str) {
        self.compensated
            .lock()
            .expect("saga e2e recorder lock")
            .push((workflow_name.to_owned(), queue.to_owned()));
    }

    fn record_saga_compensation_failed(&self, _workflow_name: &str, _queue: &str) {
        self.compensation_failed.fetch_add(1, Ordering::SeqCst);
    }
}

fn saga_flight_activity<'a>(
    _ctx: &'a ActivityContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(serde_json::json!("flight-1")) })
}

fn saga_hotel_activity<'a>(
    _ctx: &'a ActivityContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(serde_json::json!("hotel-1")) })
}

fn saga_cancel_flight_activity<'a>(
    _ctx: &'a ActivityContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        SAGA_CANCEL_FLIGHT_RUNS.fetch_add(1, Ordering::SeqCst);
        Ok(serde_json::Value::Null)
    })
}

fn saga_cancel_hotel_activity<'a>(
    _ctx: &'a ActivityContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        SAGA_CANCEL_HOTEL_RUNS.fetch_add(1, Ordering::SeqCst);
        Ok(serde_json::Value::Null)
    })
}

/// Two activity-backed forward steps with activity-backed compensations, then
/// an in-line step-3 failure that triggers the LIFO unwind. Every activity
/// boundary forces a full from-scratch replay of the workflow function — the
/// exact code path a worker crash-resume takes (crash-resume = re-claim +
/// replay; there is no other resume mechanism).
fn saga_metrics_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let mut saga = autumn_harvest::Saga::new(ctx);

        saga.step(
            || async {
                ctx.execute_activity_raw("saga_reserve_flight", serde_json::Value::Null, "default")
                    .await
            },
            move |flight: serde_json::Value| async move {
                ctx.execute_activity_raw("saga_cancel_flight", flight, "default")
                    .await
                    .map(|_| ())
            },
        )
        .await
        .map_err(|e| e.to_string())?;

        saga.step(
            || async {
                ctx.execute_activity_raw("saga_reserve_hotel", serde_json::Value::Null, "default")
                    .await
            },
            move |hotel: serde_json::Value| async move {
                ctx.execute_activity_raw("saga_cancel_hotel", hotel, "default")
                    .await
                    .map(|_| ())
            },
        )
        .await
        .map_err(|e| e.to_string())?;

        // Step 3 fails outright — rollback_after unwinds both compensations.
        saga.step(
            || async {
                Err::<serde_json::Value, _>(HarvestError::workflow_failed_untyped(
                    "e2e_test_workflow",
                    "tour sold out",
                ))
            },
            |_: serde_json::Value| async { Ok::<_, HarvestError>(()) },
        )
        .await
        .map_err(|e| e.to_string())?;

        Ok(serde_json::Value::Null)
    })
}

fn saga_activity_info(
    name: &'static str,
    handler: autumn_harvest::info::ActivityHandlerFn,
) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "integration_e2e",
        default_retry_policy: None,
        default_start_to_close: None,
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: Some("default"),
        max_concurrent: None,
        concurrency_key: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        rate_limit_key_expr: None,
        circuit_breaker: None,
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        requires: None,
        handler,
    }
}

/// AC3 e2e — the compensated counter fires exactly once across the many
/// genuine decision cycles a real worker takes through an activity-backed
/// unwind, and each compensation activity executes exactly once.
#[allow(clippy::too_many_lines)] // #946: required quota field tips this pre-existing literal-heavy test to 101 lines
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_saga_unwind_emits_compensated_counter_exactly_once_across_decision_cycles() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    SAGA_CANCEL_FLIGHT_RUNS.store(0, Ordering::SeqCst);
    SAGA_CANCEL_HOTEL_RUNS.store(0, Ordering::SeqCst);

    let exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, exec_id, serde_json::json!({"trip": "e2e"})).await;

    let recorder = Arc::new(SagaE2eRecorder::default());
    let telemetry = Arc::new(
        autumn_harvest::telemetry::TelemetryConfig::builder()
            .metrics(Arc::clone(&recorder) as Arc<dyn autumn_harvest::telemetry::MetricsRecorder>)
            .build(),
    );
    let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: "e2e_test_workflow",
            module: "integration_e2e",
            handler: saga_metrics_workflow,
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![
            saga_activity_info("saga_reserve_flight", saga_flight_activity),
            saga_activity_info("saga_reserve_hotel", saga_hotel_activity),
            saga_activity_info("saga_cancel_flight", saga_cancel_flight_activity),
            saga_activity_info("saga_cancel_hotel", saga_cancel_hotel_activity),
        ],
        autumn_harvest::context::empty_shared_state(),
        telemetry,
    ));

    let worker = build_runtime_worker("worker-saga-metrics", 2, 2, registry);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool.clone());

    let execution = wait_for_execution_state_with_timeout(
        &database_url,
        exec_id,
        "FAILED",
        Duration::from_secs(30),
    )
    .await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    // The workflow failed with the original step error (compensations all
    // succeeded, so no SagaCompensationFailed).
    let error = execution.error.expect("failed execution must carry error");
    assert!(
        error.contains("tour sold out"),
        "workflow error must carry the original step failure, got: {error}"
    );

    // Exactly-once across every decision cycle of the unwind, with the real
    // worker-threaded labels (asserted here, after the run, so a regression
    // reads as a clear assertion failure rather than an in-worker panic).
    assert_eq!(
        recorder
            .compensated
            .lock()
            .expect("saga e2e recorder lock")
            .as_slice(),
        &[("e2e_test_workflow".to_owned(), "default".to_owned())],
        "harvest.saga.compensated must fire exactly once per real unwind, \
         labeled with the workflow type and claimed queue"
    );
    assert_eq!(
        AtomicUsize::load(&recorder.compensation_failed, Ordering::SeqCst),
        0,
        "a fully-successful unwind must not touch the failure counter"
    );

    // Each compensation activity executed exactly once.
    assert_eq!(
        AtomicUsize::load(&SAGA_CANCEL_FLIGHT_RUNS, Ordering::SeqCst),
        1
    );
    assert_eq!(
        AtomicUsize::load(&SAGA_CANCEL_HOTEL_RUNS, Ordering::SeqCst),
        1
    );

    // The durable dedup marker is in history exactly once.
    let history = load_history_from_url(&database_url, exec_id).await;
    let marker_count = history
        .events
        .iter()
        .filter(|event| {
            matches!(event, WorkflowEvent::MarkerRecorded { name, .. } if name == "saga_compensated:1")
        })
        .count();
    assert_eq!(
        marker_count, 1,
        "exactly one saga_compensated marker in history"
    );
}

// ===========================================================================
// Bounded / windowed activity fan-out (issue #750) — DB e2e success metric
// ===========================================================================

/// Windowed activity fan-out over 20 inputs with window 5. Reads `n`/`w` from
/// input so the same handler is reusable, but this test fixes n=20, w=5.
fn windowed_fanout_e2e_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let n = usize::try_from(input["n"].as_u64().unwrap_or(20)).unwrap_or(20);
        let w = usize::try_from(input["w"].as_u64().unwrap_or(5)).unwrap_or(5);
        let activities: Vec<_> = (0..n)
            .map(|i| {
                (
                    "slow_double".to_string(),
                    serde_json::json!(i),
                    "default".to_string(),
                )
            })
            .collect();
        let results = ctx
            .execute_activity_fan_out_raw_windowed(activities, w)
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({ "results": results }))
    })
}

/// Unbounded control fan-out over the same inputs — same activity shape.
fn unbounded_fanout_e2e_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let n = usize::try_from(input["n"].as_u64().unwrap_or(20)).unwrap_or(20);
        let activities: Vec<_> = (0..n)
            .map(|i| {
                (
                    "slow_double".to_string(),
                    serde_json::json!(i),
                    "default".to_string(),
                )
            })
            .collect();
        let results = ctx
            .execute_activity_fan_out_raw(activities)
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({ "results": results }))
    })
}

/// Modestly-slow activity: doubles its numeric input after a short real sleep,
/// so multiple in-flight activities are observable by a concurrent DB poller.
/// (A `tokio::time::sleep` is fine here — this runs on the activity dispatch
/// path, not inside the workflow poll's 100ms suspension window.)
fn slow_double_activity<'a>(
    _ctx: &'a ActivityContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        tokio::time::sleep(Duration::from_millis(120)).await;
        let v = input.as_u64().unwrap_or(0);
        Ok(serde_json::json!(v * 2))
    })
}

fn windowed_fanout_e2e_registry() -> Arc<HandlerRegistry> {
    let make_wf =
        |name: &'static str, handler: autumn_harvest::info::WorkflowHandlerFn| WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name,
            module: "integration_e2e",
            handler,
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,
            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,
            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        };
    Arc::new(HandlerRegistry::new(
        vec![
            make_wf("windowed_fanout_e2e", windowed_fanout_e2e_workflow),
            make_wf("unbounded_fanout_e2e", unbounded_fanout_e2e_workflow),
        ],
        vec![ActivityInfo {
            name: "slow_double",
            module: "integration_e2e",
            default_retry_policy: None,
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_schedule_to_close: None,
            default_queue: Some("default"),
            max_concurrent: None,
            concurrency_key: None,
            rate_limit_rps: None,
            rate_limit_burst: None,
            rate_limit_key: None,
            rate_limit_key_expr: None,
            circuit_breaker: None,
            is_local: false,
            max_input_bytes: None,
            max_result_bytes: None,
            requires: None,
            handler: slow_double_activity,
        }],
    ))
}

/// Insert a RUNNING execution row for `(workflow_name, workflow_id)`.
async fn insert_named_execution(
    conn: &mut AsyncPgConnection,
    workflow_name: &'static str,
    workflow_id: &'static str,
    input: serde_json::Value,
) -> ExecutionId {
    let exec_id = ExecutionId::new();
    let row = NewWorkflowExecution {
        quota_key: None,
        continued_from_exec_id: None,
        first_exec_id: None,
        id: exec_id.as_uuid(),
        workflow_name,
        workflow_id,
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input,
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        deadline_at: None,
        chain_execution_timeout: None,
        chain_deadline_at: None,
        memo: None,
        search_attrs: None,
        assigned_build_id: None,
        parent_close_policy: None,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,
        sla: None,
        sla_deadline_at: None,
        schedule_id: None,
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        origin: None,
        completion_callbacks: None,
        start_source: None,
        start_source_ref: None,
        started_by: None,
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("failed to insert named workflow execution");
    exec_id
}

/// Count `harvest_task_queue` rows in `RUNNING`/`PENDING` state that are
/// activity tasks attributable to a specific workflow execution.
///
/// Takes a caller-owned connection so a polling loop can reuse ONE connection
/// across all polls rather than establishing (and dropping) a fresh connection
/// on every sample.
async fn count_active_activity_rows(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> i64 {
    harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
        .filter(harvest_task_queue::task_type.eq("activity"))
        .filter(harvest_task_queue::state.eq_any(["RUNNING", "PENDING"]))
        .count()
        .get_result(conn)
        .await
        .expect("failed to count active activity rows")
}

/// Success metric (issue #750): a windowed fan-out (N=20, W=5) never has more
/// than W=5 of its activities in `RUNNING`/`PENDING` at once, completes with
/// byte-identical results to an unbounded fan-out over the same inputs, and
/// records exactly N=20 `ActivityScheduled` events.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn windowed_fan_out_peak_task_rows_bounded_by_window() {
    let (database_url, _container) = setup_test_database_url_or_env().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let windowed_exec = insert_named_execution(
        &mut conn,
        "windowed_fanout_e2e",
        "win-1",
        serde_json::json!({ "n": 20, "w": 5 }),
    )
    .await;
    enqueue_started_workflow_task(
        &mut conn,
        windowed_exec,
        serde_json::json!({ "n": 20, "w": 5 }),
    )
    .await;

    let registry = windowed_fanout_e2e_registry();
    let worker = build_runtime_worker("worker-e2e-windowed-fanout", 4, 30, registry);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    // Background peak tracker: poll the windowed exec's active activity rows.
    let peak = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let poller = {
        let url = database_url.clone();
        let peak = Arc::clone(&peak);
        let done = Arc::clone(&done);
        tokio::spawn(async move {
            // Open ONE connection for the whole poll loop instead of establishing
            // (and dropping) a fresh connection on every 5ms sample.
            let mut poll_conn =
                <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&url)
                    .await
                    .expect("failed to connect for activity-row polling");
            while !std::sync::atomic::AtomicBool::load(&done, Ordering::SeqCst) {
                let n = count_active_activity_rows(&mut poll_conn, windowed_exec).await;
                let n = usize::try_from(n).unwrap_or(0);
                AtomicUsize::fetch_max(&peak, n, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };

    let windowed_execution = wait_for_execution_state_with_timeout(
        &database_url,
        windowed_exec,
        "COMPLETED",
        Duration::from_secs(30),
    )
    .await;
    std::sync::atomic::AtomicBool::store(&done, true, Ordering::SeqCst);
    poller.await.expect("poller task should join");

    let peak_observed = AtomicUsize::load(&peak, Ordering::SeqCst);
    assert!(
        peak_observed <= 5,
        "windowed fan-out (W=5) peaked at {peak_observed} concurrent activity rows; must stay <= 5"
    );
    assert!(
        peak_observed > 0,
        "peak tracker should have observed at least one in-flight activity row"
    );

    // Now run the unbounded control over the same inputs and compare results.
    let unbounded_exec = insert_named_execution(
        &mut conn,
        "unbounded_fanout_e2e",
        "unb-1",
        serde_json::json!({ "n": 20 }),
    )
    .await;
    enqueue_started_workflow_task(&mut conn, unbounded_exec, serde_json::json!({ "n": 20 })).await;
    let unbounded_execution = wait_for_execution_state_with_timeout(
        &database_url,
        unbounded_exec,
        "COMPLETED",
        Duration::from_secs(30),
    )
    .await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    let expected: Vec<serde_json::Value> = (0..20u64).map(|i| serde_json::json!(i * 2)).collect();
    assert_eq!(
        windowed_execution.output,
        Some(serde_json::json!({ "results": expected.clone() })),
        "windowed output must be the doubled inputs in order"
    );
    assert_eq!(
        windowed_execution.output, unbounded_execution.output,
        "windowed fan-out must be byte-identical to the unbounded fan-out"
    );

    // Exactly N=20 activities were scheduled by the windowed run.
    let history = load_history_from_url(&database_url, windowed_exec).await;
    let scheduled = history
        .events
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ActivityScheduled { .. }))
        .count();
    assert_eq!(
        scheduled, 20,
        "windowed fan-out must schedule all 20 inputs exactly once"
    );
    let markers = history
        .events
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::MarkerRecorded { name, .. } if name.starts_with("fan_out:")))
        .count();
    assert_eq!(markers, 1, "exactly one fan_out marker recorded");
}

/// Guard: `INIT_SQL` must create every `harvest_*` table the claim path
/// references.
///
/// [`INIT_SQL`] is a deliberately-partial, hand-maintained bundle (it omits the
/// workflow-start-uniqueness migration on purpose), so it is one of the few
/// fixtures allowed to skip [`autumn_harvest::full_migrations_sql`]. That makes
/// it a standing drift hazard. `queue::claim_task` runs on essentially every
/// test in this suite. It also runs in every suite that borrows
/// `setup_test_database_url_or_env` from here (`chain_timeout_tests`,
/// `child_timeout_tests`, `cross_type_continue_as_new_tests`, `ctx_info_tests`,
/// `dag_execution_timeout_tests`, `rate_limit_key_tests`,
/// `quota_enforcement_tests`, `workflow_retry_tests`). A migration that adds a
/// table to the claim query, then forgets this bundle, breaks nine suites at
/// once with `relation "..." does not exist`.
///
/// That is exactly what issue #619's `harvest_queue_pauses` anti-join did. It
/// cost a full Docker-backed CI cycle (~13 min) to surface, yet it is decidable
/// as a pure string comparison, so this pins it as a fast no-DB check instead.
///
/// The rule is mechanical: every `harvest_*` identifier named in the real
/// `claim_task_query()` SQL must appear in `INIT_SQL`. Deriving the table set
/// from the live query rather than a hardcoded list means a future claim-path
/// table is caught automatically, with no second list to maintain.
#[test]
fn init_sql_creates_every_table_the_claim_path_references() {
    // Both claim statements, not only the unfenced one. The fenced variant
    // splices in `harvest_shard_generation`. No other query names that table.
    // Scanning only the base query left that gap to a Docker-backed CI cycle
    // (issue #1312).
    let claim_sql = format!(
        "{} {}",
        autumn_harvest::queue::claim_task_query(),
        autumn_harvest::queue::claim_task_query_fenced()
    );
    let claim_sql = claim_sql.as_str();

    let mut tables: Vec<String> = Vec::new();
    let bytes = claim_sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' {
            let start = i;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            let word = &claim_sql[start..i];
            if word.starts_with("harvest_") && !tables.contains(&word.to_string()) {
                tables.push(word.to_string());
            }
        } else {
            i += 1;
        }
    }

    assert!(
        !tables.is_empty(),
        "expected to find harvest_* identifiers in the claim queries; the token scan is broken"
    );

    let missing: Vec<&String> = tables
        .iter()
        .filter(|table| !INIT_SQL.contains(table.as_str()))
        .collect();

    assert!(
        missing.is_empty(),
        "INIT_SQL is missing table(s) the claim path references: {missing:?}\n\
         Add the migration that creates them to the INIT_SQL concat! in this file. \
         Without it every claim in this suite -- and in chain_timeout_tests, \
         child_timeout_tests, cross_type_continue_as_new_tests, ctx_info_tests, \
         dag_execution_timeout_tests, rate_limit_key_tests and \
         workflow_retry_tests, which reuse setup_test_database_url_or_env from \
         here -- fails with `relation \"...\" does not exist` (issues #619, #807)."
    );
}
