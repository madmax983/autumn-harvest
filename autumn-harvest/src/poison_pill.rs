//! Poison-pill task quarantine (Phase 4, issue #367).
//!
//! A *poison-pill* task is one that crashes the worker **process** (panic, OOM,
//! segfault, SIGKILL, hard exit) instead of returning a clean `Err`. Because of
//! `SKIP LOCKED` re-claim semantics, the row left behind in `RUNNING` state by
//! the dead worker is eventually reclaimed and re-dispatched — and crashes the
//! next worker too, cascading across the fleet.
//!
//! This module adds a worker-liveness-driven reclaim path that is independent
//! of per-task timeout configuration (so an un-timed orphan is recovered rather
//! than stuck in `RUNNING` forever), counts how many times a task has crashed a
//! worker (`crash_strikes`), and *quarantines* a task to the dead-letter queue
//! once it has consumed `poison_pill_threshold` workers in a row — instead of
//! re-queueing it for yet another doomed attempt.
//!
//! The pure decision logic ([`quarantine_decision`]) carries no database
//! dependency and is unit-tested without the `db` feature. The DB scanner
//! ([`reclaim_orphaned_tasks`]) is gated behind `db`.
//!
//! [`reclaim_orphaned_tasks`] also runs a second, independent backstop pass
//! (issue #1459). A `workflow` decision-cycle task can stay `RUNNING` on a
//! worker that is still alive. This happens when the in-process timeout's
//! own reset call fails to reach the database in time. The worker-liveness
//! pass above never catches that case, since the worker itself never died.
//! See [`stuck_running_tasks_query`].

/// What to do with an orphaned `RUNNING` task whose claiming worker has died.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReclaimAction {
    /// Re-queue the task for another attempt (its crash-strike count has not
    /// yet reached the quarantine threshold).
    Requeue,
    /// Quarantine the task to the dead-letter queue: it has crashed a worker
    /// `threshold` times in a row and must not be re-dispatched.
    Quarantine,
}

/// Decide whether a reclaimed poison-pill task should be re-queued or
/// quarantined, given the crash-strike count **after** this reclaim has
/// incremented it and the configured quarantine threshold.
///
/// Semantics:
/// - `threshold <= 0` disables quarantine entirely — the task is always
///   re-queued. This preserves the legacy retry-loop behaviour (issue #367
///   acceptance criterion: a threshold of 0 is the documented opt-out).
/// - Otherwise the task is quarantined once `strikes_after_increment` reaches
///   the threshold.
#[must_use]
pub const fn quarantine_decision(strikes_after_increment: i32, threshold: i32) -> ReclaimAction {
    if threshold > 0 && strikes_after_increment >= threshold {
        ReclaimAction::Quarantine
    } else {
        ReclaimAction::Requeue
    }
}

/// Summary of one orphan-reclaim sweep.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReclaimSummary {
    /// Orphaned tasks re-queued for another attempt.
    pub requeued: usize,
    /// Orphaned tasks quarantined to the dead-letter queue.
    pub quarantined: usize,
    /// Stuck `workflow` tasks re-queued via the live-worker backstop path
    /// (issue #1459). Distinct from `requeued`: that count is the
    /// dead-worker orphan path, this one requires no worker liveness signal
    /// at all. See [`stuck_running_tasks_query`].
    pub stuck_requeued: usize,
}

impl ReclaimSummary {
    /// Total tasks acted on this sweep.
    #[must_use]
    pub const fn total(&self) -> usize {
        self.requeued + self.quarantined + self.stuck_requeued
    }
}

/// Upper bound (one year, in seconds) applied to the worker-stale threshold.
///
/// The reclaim path multiplies this value into a Postgres `INTERVAL` and into a
/// [`chrono::Duration`]; clamping here keeps both well inside their valid range
/// so an absurd `worker_heartbeat_interval` can never overflow or panic. A
/// one-year staleness window is already far beyond any sane fleet config.
pub const MAX_WORKER_STALE_SECS: i64 = 31_536_000;

/// Upper bound (one year, in seconds) applied to the stuck-running threshold.
///
/// Mirrors [`MAX_WORKER_STALE_SECS`]'s overflow-safety role for
/// [`stuck_running_tasks_query`].
pub const MAX_STUCK_RUNNING_SECS: i64 = 31_536_000;

/// SQL selecting `RUNNING` tasks whose claiming worker is no longer live.
///
/// A worker is considered dead when no `harvest_workers` row carries its
/// `worker_id` with a `last_heartbeat_at` newer than `$1` seconds ago. This is
/// the authoritative liveness signal (issue #367 AC1): reclaim does **not**
/// depend on the per-task `start_to_close` / `heartbeat_timeout` columns, so an
/// orphan with neither configured is still recovered rather than stuck in
/// `RUNNING` forever.
///
/// `$1` is the worker-stale threshold in seconds (BIGINT).
#[must_use]
pub const fn orphaned_running_tasks_query() -> &'static str {
    "SELECT * FROM harvest_task_queue t \
     WHERE t.state = 'RUNNING' \
       AND t.worker_id IS NOT NULL \
       AND NOT EXISTS ( \
           SELECT 1 FROM harvest_workers w \
           WHERE w.worker_id = t.worker_id \
             AND w.last_heartbeat_at > NOW() - ($1::bigint * INTERVAL '1 second') \
       )"
}

/// SQL selecting `RUNNING` `workflow` decision-cycle tasks stuck long past any
/// legitimate single decision cycle's budget, regardless of worker liveness
/// (issue #1459).
///
/// The engine hard-cancels a workflow-task dispatch once it exceeds its
/// configured `workflow_task_timeout` (`run_under_workflow_body_budget` in
/// `worker.rs`). A `workflow` row still `RUNNING` well past that budget did
/// not merely run long: its processing already stopped. The reset call
/// (`reset_timed_out_workflow_task`) is the only thing that could still leave
/// the row `RUNNING`. It failed — most often a database-pool connection that
/// could not be acquired within its own bounded retry budget.
///
/// [`orphaned_running_tasks_query`] does not catch this case: the claiming
/// worker is still alive, busy with other tasks, so the dead-worker liveness
/// check never fires.
///
/// `activity` tasks are deliberately excluded. Their own
/// `start_to_close_timeout` is unrelated to `workflow_task_timeout` and can
/// legitimately keep a row `RUNNING` for a long time.
///
/// `$1` is the stuck-running threshold in seconds (BIGINT).
#[must_use]
pub const fn stuck_running_tasks_query() -> &'static str {
    "SELECT * FROM harvest_task_queue t \
     WHERE t.state = 'RUNNING' \
       AND t.worker_id IS NOT NULL \
       AND t.task_type = 'workflow' \
       AND t.started_at IS NOT NULL \
       AND t.started_at < NOW() - ($1::bigint * INTERVAL '1 second')"
}

// ---------------------------------------------------------------------------
// Orphan-reclaim scanner (DB)
// ---------------------------------------------------------------------------

#[cfg(feature = "db")]
mod scanner {
    use chrono::Utc;
    use diesel::BoolExpressionMethods;
    use diesel::ExpressionMethods;
    use diesel::OptionalExtension;
    use diesel::QueryDsl;
    use diesel_async::AsyncConnection;
    use diesel_async::AsyncPgConnection;
    use diesel_async::RunQueryDsl;
    use tokio_util::sync::CancellationToken;

    use super::{
        ReclaimAction, ReclaimSummary, orphaned_running_tasks_query, quarantine_decision,
        stuck_running_tasks_query,
    };
    use crate::completion_trigger::DeferredTriggerStart;
    use crate::error::{HarvestError, HarvestResult};
    use crate::event::WorkflowEvent;
    use crate::execution::apply_parent_close_cascade;
    use crate::models::TaskQueueItem;
    use crate::telemetry::MetricsRecorder;
    use crate::types::ExecutionId;

    /// Reason label emitted on the `harvest.task.quarantined` metric and stored
    /// in the dead-letter reason discriminator.
    pub const QUARANTINE_REASON: &str = "poison_pill";

    fn execution_id_from_uuid(id: uuid::Uuid) -> ExecutionId {
        id.to_string()
            .parse()
            .expect("database UUIDs must round-trip into ExecutionId")
    }

    /// Re-check, under a row lock, whether the worker that holds `worker_id` is
    /// still dead. Guards against a worker that resurrected between the broad
    /// scan and acquiring the task lock.
    async fn worker_still_dead(
        conn: &mut AsyncPgConnection,
        worker_id: &str,
        worker_stale_secs: i64,
    ) -> HarvestResult<bool> {
        use crate::schema::harvest_workers::dsl;

        let cutoff = Utc::now() - chrono::Duration::seconds(worker_stale_secs);
        let live: Option<String> = dsl::harvest_workers
            .filter(dsl::worker_id.eq(worker_id))
            .filter(dsl::last_heartbeat_at.gt(cutoff))
            .select(dsl::worker_id)
            .first(conn)
            .await
            .optional()
            .map_err(crate::error::database_error)?;
        Ok(live.is_none())
    }

    /// Re-queue an orphaned task for another attempt, recording the new
    /// crash-strike count. Clears the dead worker's claim and sticky pin so any
    /// healthy worker can pick it up immediately.
    ///
    /// Returns `true` if the row was actually transitioned (it was still a
    /// `RUNNING` orphan with the expected strike count), `false` if a
    /// concurrent actor already handled it.
    async fn requeue_orphan(
        conn: &mut AsyncPgConnection,
        task: &TaskQueueItem,
        new_strikes: i32,
        worker_stale_secs: i64,
    ) -> HarvestResult<bool> {
        use crate::schema::harvest_task_queue::dsl;

        let task_id = task.id;
        let worker = task.worker_id.clone();
        let prior_strikes = task.crash_strikes;

        Box::pin(conn.transaction::<bool, HarvestError, _>(async |conn| {
            let Some(worker_id) = worker else {
                return Ok(false);
            };
            // Lock the row and re-verify it is still the same orphan.
            let current: Option<(String, Option<String>, i32)> = dsl::harvest_task_queue
                .find(task_id)
                .for_update()
                .select((dsl::state, dsl::worker_id, dsl::crash_strikes))
                .first(conn)
                .await
                .optional()
                .map_err(crate::error::database_error)?;
            match current {
                Some((state, Some(wid), strikes))
                    if state == "RUNNING" && wid == worker_id && strikes == prior_strikes => {}
                _ => return Ok(false),
            }
            if !worker_still_dead(conn, &worker_id, worker_stale_secs).await? {
                return Ok(false);
            }

            diesel::update(dsl::harvest_task_queue.find(task_id))
                .set((
                    dsl::state.eq("PENDING"),
                    dsl::worker_id.eq(None::<String>),
                    dsl::started_at.eq(None::<chrono::DateTime<Utc>>),
                    dsl::sticky_worker_id.eq(None::<String>),
                    dsl::sticky_until.eq(None::<chrono::DateTime<Utc>>),
                    // Clear the dead attempt's heartbeat timestamp so the
                    // fresh attempt is not immediately timed out by the
                    // COALESCE(last_heartbeat_at, started_at) scanner.
                    // heartbeat_details is preserved so a retry can still
                    // read the last flushed checkpoint.
                    dsl::last_heartbeat_at.eq(None::<chrono::DateTime<Utc>>),
                    // Clear the previous error: crashes don't leave a
                    // meaningful error string, and the stale message from an
                    // earlier clean failure would otherwise appear as
                    // previous_failure() on the next attempt.
                    dsl::error.eq(None::<String>),
                    dsl::crash_strikes.eq(new_strikes),
                    dsl::scheduled_at.eq(Utc::now()),
                ))
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
            Ok(true)
        }))
        .await
    }

    /// Re-queue a `workflow` task stuck `RUNNING` past the stuck-running
    /// backstop threshold, on a worker that may still be alive (issue #1459).
    ///
    /// Never touches `crash_strikes` and never quarantines. Being stuck this
    /// way says nothing about the task itself. It means a reset attempt could
    /// not reach the database in time, not that the task is poisonous.
    ///
    /// Returns `true` if the row was actually transitioned (it was still the
    /// same stuck `RUNNING` attempt), `false` if a concurrent actor already
    /// handled it.
    async fn requeue_stuck_task(
        conn: &mut AsyncPgConnection,
        task: &TaskQueueItem,
        stuck_running_secs: i64,
    ) -> HarvestResult<bool> {
        use crate::schema::harvest_task_queue::dsl;

        // Row shape re-read fresh under the lock below: state, worker id,
        // crash strikes, task type, started-at.
        type StuckRowState = (
            String,
            Option<String>,
            i32,
            String,
            Option<chrono::DateTime<Utc>>,
        );

        let task_id = task.id;
        let Some(worker_id) = task.worker_id.clone() else {
            return Ok(false);
        };
        let prior_strikes = task.crash_strikes;

        Box::pin(conn.transaction::<bool, HarvestError, _>(async |conn| {
            // Lock the row and re-verify it is still the same stuck attempt.
            // `started_at` is re-read fresh under the lock, not taken from
            // the pre-scan snapshot. A reset or a fresh re-claim between the
            // scan and here always changes it. Re-checking its age here is
            // what stops this from undoing a legitimate new attempt.
            let current: Option<StuckRowState> = dsl::harvest_task_queue
                .find(task_id)
                .for_update()
                .select((
                    dsl::state,
                    dsl::worker_id,
                    dsl::crash_strikes,
                    dsl::task_type,
                    dsl::started_at,
                ))
                .first(conn)
                .await
                .optional()
                .map_err(crate::error::database_error)?;
            let cutoff = Utc::now() - chrono::Duration::seconds(stuck_running_secs);
            match current {
                Some((state, Some(wid), strikes, task_type, Some(started_at)))
                    if state == "RUNNING"
                        && wid == worker_id
                        && strikes == prior_strikes
                        && task_type == "workflow"
                        && started_at < cutoff => {}
                _ => return Ok(false),
            }

            diesel::update(dsl::harvest_task_queue.find(task_id))
                .set((
                    dsl::state.eq("PENDING"),
                    dsl::worker_id.eq(None::<String>),
                    dsl::started_at.eq(None::<chrono::DateTime<Utc>>),
                    dsl::sticky_worker_id.eq(None::<String>),
                    dsl::sticky_until.eq(None::<chrono::DateTime<Utc>>),
                    dsl::last_heartbeat_at.eq(None::<chrono::DateTime<Utc>>),
                    dsl::scheduled_at.eq(Utc::now()),
                ))
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
            Ok(true)
        }))
        .await
    }

    /// Fail the owning workflow execution terminally via the existing
    /// `WorkflowFailed` event path (issue #367 AC4 — no new event variant).
    ///
    /// Only transitions executions still in `RUNNING`; a workflow that already
    /// reached a terminal state is left untouched. Returns the
    /// `(workflow_id, workflow_name, schedule_id, origin)` of the execution when
    /// (and only when) it actually transitioned `RUNNING` → `FAILED`, so the
    /// caller can count the failure toward schedule auto-pause once the
    /// transaction commits (with the correct origin so backfill quarantines are
    /// not mis-attributed to the cadence counter).
    #[allow(clippy::too_many_lines)]
    async fn fail_owning_workflow(
        conn: &mut AsyncPgConnection,
        exec_id: ExecutionId,
        error: &str,
        metrics: Option<&(dyn MetricsRecorder + Send + Sync)>,
        // Issue #1243: `WorkflowFailed` carries `details`, a payload-bearing
        // field, so this write encodes through the configured registry.
        codecs: &crate::payload_codec::PayloadCodecs,
    ) -> HarvestResult<(
        Option<(String, String, Option<uuid::Uuid>, Option<String>)>,
        Vec<DeferredTriggerStart>,
        Vec<(ExecutionId, String)>,
        Vec<crate::execution::StartCancelledRun>,
    )> {
        use crate::schema::harvest_workflow_executions::dsl as exec_dsl;

        type ExecRow = (
            String,
            Option<uuid::Uuid>,
            Option<String>,
            String,
            String,
            Option<uuid::Uuid>,
            Option<String>,
        );
        let current: Option<ExecRow> = exec_dsl::harvest_workflow_executions
            .find(exec_id.as_uuid())
            .for_update()
            .select((
                exec_dsl::state,
                exec_dsl::parent_id,
                exec_dsl::parent_close_policy,
                exec_dsl::workflow_id,
                exec_dsl::workflow_name,
                exec_dsl::schedule_id,
                exec_dsl::origin,
            ))
            .first(conn)
            .await
            .optional()
            .map_err(crate::error::database_error)?;
        let Some((
            state,
            parent_id,
            parent_close_policy,
            workflow_id,
            workflow_name,
            schedule_id,
            origin,
        )) = current
        else {
            return Ok((None, Vec::new(), Vec::new(), Vec::new()));
        };
        // PAUSED is a non-terminal active state (issue #383): an in-flight
        // activity that was admitted before the pause can still be running, so a
        // poison-pill quarantine of that activity must terminally fail the
        // owning workflow rather than leave it parked in PAUSED forever with a
        // dead task. Treat PAUSED like RUNNING here.
        if state != "RUNNING" && state != "PAUSED" {
            return Ok((None, Vec::new(), Vec::new(), Vec::new()));
        }

        let history = crate::store::load_history_with_codecs(conn, exec_id, codecs).await?;
        crate::store::append_events_with_codecs(
            conn,
            exec_id,
            &[WorkflowEvent::workflow_failed(error.to_string())],
            history.next_event_id,
            codecs,
        )
        .await?;
        diesel::update(
            exec_dsl::harvest_workflow_executions
                .find(exec_id.as_uuid())
                .filter(exec_dsl::state.eq_any(["RUNNING", "PAUSED"])),
        )
        .set((
            exec_dsl::state.eq("FAILED"),
            exec_dsl::output.eq(None::<serde_json::Value>),
            exec_dsl::error.eq(Some(error.to_string())),
            exec_dsl::completed_at.eq(Some(Utc::now())),
            // Clear active-pause metadata when a paused owner is failed (#383).
            exec_dsl::paused_at.eq(None::<chrono::DateTime<Utc>>),
            exec_dsl::pause_reason.eq(None::<String>),
            exec_dsl::pause_actor.eq(None::<String>),
        ))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;

        // Drain any sibling tasks still open for this execution. `claim_task`
        // filters only on task state, so without this a PENDING/RUNNING sibling
        // activity could be claimed and run user code after the workflow has
        // already failed (the timeout/cancellation paths drain for this same
        // reason). The quarantined task itself is already FAILED.
        {
            use crate::schema::harvest_task_queue::dsl as task_dsl;
            diesel::update(
                task_dsl::harvest_task_queue
                    .filter(task_dsl::workflow_exec_id.eq(exec_id.as_uuid()))
                    .filter(
                        task_dsl::state
                            .eq("PENDING")
                            .or(task_dsl::state.eq("RUNNING")),
                    ),
            )
            .set((
                task_dsl::state.eq("FAILED"),
                task_dsl::error.eq(Some(error.to_string())),
                task_dsl::completed_at.eq(Some(Utc::now())),
            ))
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;
        }

        let (mut deferred, closed_children) =
            apply_parent_close_cascade(conn, exec_id, codecs).await?;
        let mut pending_cancel_metrics = Vec::new();
        let failed_triggers =
            crate::completion_trigger::evaluate_triggers_for_execution_collecting_with_codecs(
                conn,
                exec_id,
                crate::completion_trigger::TerminalState::Failed,
                metrics,
                &mut pending_cancel_metrics,
                codecs,
            )
            .await?;
        deferred.extend(failed_triggers);

        // Wake a parent that is blocked on this child. When a parent-close
        // policy is set the cascade above owns the parent relationship, so we
        // only nudge the parent for detached children (mirrors the timeout
        // enforcement path).
        if parent_close_policy.is_none()
            && let Some(parent_uuid) = parent_id
        {
            let parent_exec_id = execution_id_from_uuid(parent_uuid);
            // Issue #956: a cross-shard parent is not on this connection.
            // `append_single_event` requires the parent row, so appending here
            // would `NotFound` and roll back this poison-pill seal — leaving the
            // child neither sealed nor retried. The cross-shard relay delivers
            // the wake instead, from the parent's own shard, once it observes
            // this child terminal. Mirrors the identical guard in
            // `worker::wake_parent_for_child_completion`/`_failure` and
            // `timeout::wake_parent_for_child_timeout`.
            if crate::worker::parent_is_on_another_shard(conn, parent_exec_id, exec_id).await? {
                tracing::debug!(
                    parent_execution_id = %parent_exec_id,
                    child_execution_id = %exec_id,
                    "cross-shard child poison-pilled; the parent wake is the \
                     relay's to deliver"
                );
            } else {
                crate::store::append_single_event_with_codecs(
                    conn,
                    parent_exec_id,
                    WorkflowEvent::child_workflow_failed(exec_id, error.to_string()),
                    codecs,
                )
                .await?;
                crate::queue::wake_workflow_task(conn, parent_exec_id).await?;
            }
        }
        Ok((
            Some((workflow_id, workflow_name, schedule_id, origin)),
            deferred,
            closed_children,
            pending_cancel_metrics,
        ))
    }

    /// Quarantine an orphaned poison-pill task: move it to the dead-letter
    /// queue with a [`PoisonPill`](super::super::dlq::DeadLetterReason) reason,
    /// mark the queue row `FAILED`, and fail the owning workflow terminally.
    ///
    /// Returns `true` if the task was quarantined, `false` if a concurrent
    /// actor already handled the row.
    #[allow(clippy::too_many_lines)]
    async fn quarantine_orphan(
        conn: &mut AsyncPgConnection,
        task: &TaskQueueItem,
        new_strikes: i32,
        worker_stale_secs: i64,
        metrics: &dyn MetricsRecorder,
        // Issue #1243: forwarded to the owning-workflow failure write.
        codecs: &crate::payload_codec::PayloadCodecs,
    ) -> HarvestResult<bool> {
        use crate::dlq::{DeadLetterReason, NewDeadLetterEntry, dead_letter};
        use crate::schema::harvest_task_queue::dsl;

        let reason = DeadLetterReason::PoisonPill {
            crash_strikes: new_strikes,
            last_worker_id: task.worker_id.clone(),
        };
        let error = reason.to_string();

        let (owner, severity) = match task.workflow_exec_id {
            Some(exec_uuid) => {
                use crate::schema::harvest_workflow_executions::dsl as exec_dsl;
                use diesel::OptionalExtension;
                use diesel::QueryDsl;
                use diesel_async::RunQueryDsl;
                exec_dsl::harvest_workflow_executions
                    .find(exec_uuid)
                    .select((exec_dsl::owner, exec_dsl::severity))
                    .first::<(Option<String>, Option<String>)>(conn)
                    .await
                    .optional()
                    .map_err(crate::error::database_error)?
                    .unwrap_or((None, None))
            }
            None => (None, None),
        };

        let task_id = task.id;
        let worker = task.worker_id.clone();
        let prior_strikes = task.crash_strikes;

        let entry = NewDeadLetterEntry {
            original_task_id: task.id,
            queue_name: task.queue_name.clone(),
            task_type: task.task_type.clone(),
            workflow_exec_id: task.workflow_exec_id,
            activity_name: task.activity_name.clone(),
            input: task.input.clone(),
            error: error.clone(),
            attempts: task.attempt,
            owner,
            severity,
        };
        let workflow_exec_id = task.workflow_exec_id;

        // The transaction returns whether the row was acted on, plus the owning
        // workflow's (id, name, schedule_id, origin) when it was actually failed
        // RUNNING → FAILED so the schedule failure counter can be bumped (with
        // the correct origin) after commit.
        let (acted, failed_workflow, deferred_starts, closed_children, pending_cancel_metrics) =
            Box::pin(conn.transaction::<(
                bool,
                Option<(String, String, Option<uuid::Uuid>, Option<String>)>,
                Vec<DeferredTriggerStart>,
                Vec<(ExecutionId, String)>,
                Vec<crate::execution::StartCancelledRun>,
            ), HarvestError, _>(async |conn| {
                let Some(worker_id) = worker else {
                    return Ok((false, None, Vec::new(), Vec::new(), Vec::new()));
                };
                let current: Option<(String, Option<String>, i32)> = dsl::harvest_task_queue
                    .find(task_id)
                    .for_update()
                    .select((dsl::state, dsl::worker_id, dsl::crash_strikes))
                    .first(conn)
                    .await
                    .optional()
                    .map_err(crate::error::database_error)?;
                match current {
                    Some((state, Some(wid), strikes))
                        if state == "RUNNING" && wid == worker_id && strikes == prior_strikes => {}
                    _ => return Ok((false, None, Vec::new(), Vec::new(), Vec::new())),
                }
                if !worker_still_dead(conn, &worker_id, worker_stale_secs).await? {
                    return Ok((false, None, Vec::new(), Vec::new(), Vec::new()));
                }

                dead_letter(conn, &entry).await?;

                diesel::update(dsl::harvest_task_queue.find(task_id))
                    .set((
                        dsl::state.eq("FAILED"),
                        dsl::worker_id.eq(None::<String>),
                        dsl::crash_strikes.eq(new_strikes),
                        dsl::error.eq(Some(error.clone())),
                        dsl::completed_at.eq(Some(Utc::now())),
                    ))
                    .execute(conn)
                    .await
                    .map_err(crate::error::database_error)?;

                let (failed_workflow, deferred, closed_children, pending_cancel_metrics) =
                    match workflow_exec_id {
                        Some(exec_uuid) => {
                            fail_owning_workflow(
                                conn,
                                execution_id_from_uuid(exec_uuid),
                                &error,
                                Some(metrics),
                                codecs,
                            )
                            .await?
                        }
                        None => (None, Vec::new(), Vec::new(), Vec::new()),
                    };
                Ok((
                    true,
                    failed_workflow,
                    deferred,
                    closed_children,
                    pending_cancel_metrics,
                ))
            }))
            .await?;

        if acted {
            // issue #1197, item 1: emitted only now that this transaction has
            // actually committed.
            crate::execution::emit_start_cancel_metrics(metrics, &pending_cancel_metrics);
            metrics.record_task_quarantined(&task.queue_name, QUARANTINE_REASON);
            // Best-effort: count the poison-pill workflow failure toward the
            // schedule auto-pause threshold (issue #360), mirroring the normal
            // failure and timeout paths. Runs after the transaction commits so
            // a counter error can never abort the quarantine.
            if let Some((workflow_id, workflow_name, schedule_id, origin)) = failed_workflow {
                crate::telemetry::emit_workflow_terminal(
                    metrics,
                    &workflow_name,
                    &task.queue_name,
                    crate::telemetry::WorkflowStatus::Failed,
                );

                if let Some(exec_uuid) = task.workflow_exec_id {
                    let exec_id = execution_id_from_uuid(exec_uuid);
                    if let Err(e) = crate::execution::check_and_report_unfinished_handlers(
                        conn,
                        exec_id,
                        &workflow_name,
                        Some(metrics),
                    )
                    .await
                    {
                        tracing::error!(
                            exec_id = %exec_id,
                            err = %e,
                            "Failed to check and report unfinished handlers on poison pill workflow failure"
                        );
                    }
                }

                crate::scheduler::maybe_increment_schedule_failure_counter(
                    conn,
                    &workflow_id,
                    &workflow_name,
                    schedule_id,
                    origin.as_deref(),
                    metrics,
                )
                .await;
            }

            for (child_id, child_name) in closed_children {
                if let Err(e) = crate::execution::check_and_report_unfinished_handlers(
                    conn,
                    child_id,
                    &child_name,
                    Some(metrics),
                )
                .await
                {
                    tracing::error!(
                        child_id = %child_id,
                        err = %e,
                        "Failed to check and report unfinished handlers on cascaded child in poison pill"
                    );
                }
            }

            for start in deferred_starts {
                start.spawn();
            }
        }
        Ok(acted)
    }

    /// Reclaim `RUNNING` tasks orphaned by a dead worker, then (issue #1459)
    /// tasks stuck long past their budget regardless of worker liveness.
    ///
    /// Each dead-worker orphan's `crash_strikes` is incremented; the task is
    /// then re-queued (under threshold) or quarantined to the DLQ (at or over
    /// threshold) per [`quarantine_decision`]. Runs shard-local against the
    /// connection's database.
    ///
    /// `worker_stale_secs` is how long a worker may go without a heartbeat
    /// before its in-flight tasks are considered orphaned (typically
    /// `2 × worker_heartbeat_interval`).
    ///
    /// `stuck_running_secs` gates the second, independent backstop pass
    /// ([`stuck_running_tasks_query`]): `None` disables it, so behavior is
    /// unchanged from before issue #1459. `Some(secs)` re-queues a stuck
    /// `workflow` task whether or not its worker is alive. This pass never
    /// touches `crash_strikes` and never quarantines — being stuck this way
    /// says nothing about the task itself.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] on query failure.
    pub async fn reclaim_orphaned_tasks(
        conn: &mut AsyncPgConnection,
        threshold: i32,
        worker_stale_secs: i64,
        stuck_running_secs: Option<i64>,
        metrics: &dyn MetricsRecorder,
        // Issue #1243: forwarded to the quarantine path, whose
        // `WorkflowFailed` carries a payload-bearing `details` field.
        codecs: &crate::payload_codec::PayloadCodecs,
    ) -> HarvestResult<ReclaimSummary> {
        // Clamp once at the entry point so neither the SQL interval arithmetic
        // nor the chrono::Duration re-check (in `worker_still_dead`) can ever
        // overflow on an out-of-range caller value.
        let worker_stale_secs = worker_stale_secs.clamp(0, super::MAX_WORKER_STALE_SECS);
        // Chaos: inject a transient DB/connection error before the orphan scan
        // (issue #940 AC1(b)). The reclaim is idempotent and the poll loop
        // retries it on the next tick, so a transient error must not strand an
        // orphaned RUNNING task with a dead worker.
        crate::chaos_fallible!(POISON_RECLAIM_BEFORE_LOAD);
        let orphans: Vec<TaskQueueItem> = diesel::sql_query(orphaned_running_tasks_query())
            .bind::<diesel::sql_types::BigInt, _>(worker_stale_secs)
            .load(conn)
            .await
            .map_err(crate::error::database_error)?;

        let mut summary = ReclaimSummary::default();
        for task in orphans {
            let new_strikes = task.crash_strikes.saturating_add(1);
            match quarantine_decision(new_strikes, threshold) {
                ReclaimAction::Quarantine => {
                    if quarantine_orphan(
                        conn,
                        &task,
                        new_strikes,
                        worker_stale_secs,
                        metrics,
                        codecs,
                    )
                    .await?
                    {
                        summary.quarantined += 1;
                    }
                }
                ReclaimAction::Requeue => {
                    if requeue_orphan(conn, &task, new_strikes, worker_stale_secs).await? {
                        summary.requeued += 1;
                        // Dispatch hint (issue #1312). The orphan is `PENDING`
                        // again and its inner transaction has committed, so the
                        // channel gets a reference to it.
                        crate::queue::record_pending_hints(conn, &[task.id]).await;
                    }
                }
            }
        }

        if let Some(stuck_running_secs) = stuck_running_secs {
            let stuck_running_secs = stuck_running_secs.clamp(0, super::MAX_STUCK_RUNNING_SECS);
            let stuck: Vec<TaskQueueItem> = diesel::sql_query(stuck_running_tasks_query())
                .bind::<diesel::sql_types::BigInt, _>(stuck_running_secs)
                .load(conn)
                .await
                .map_err(crate::error::database_error)?;
            for task in stuck {
                if requeue_stuck_task(conn, &task, stuck_running_secs).await? {
                    summary.stuck_requeued += 1;
                    crate::queue::record_pending_hints(conn, &[task.id]).await;
                }
            }
        }
        Ok(summary)
    }

    /// Spawn a background task that periodically reclaims orphaned poison-pill
    /// tasks. Stops when `cancel` is triggered.
    ///
    /// Equivalent to [`spawn_poison_pill_reclaimer_for_shard`] with no shard
    /// attributed and the issue #1459 stuck-running backstop disabled. The
    /// shard is only used to label this loop in the `scanner_liveness` health
    /// check (issue #797); it never affects which tasks the loop reclaims --
    /// that is the connection's own database.
    ///
    /// This function's signature is frozen. See
    /// `the_public_spawn_signatures_are_unchanged_by_shard_attribution` in
    /// `tests/integration/scanner_tick_db_tests.rs`. The reason matches why
    /// the shard parameter never reached it. An embedder calling this
    /// directly should not have to opt into a feature it never asked for.
    /// Call [`spawn_poison_pill_reclaimer_for_shard`] directly to enable the
    /// backstop.
    #[must_use]
    pub fn spawn_poison_pill_reclaimer(
        pool: diesel_async::pooled_connection::deadpool::Pool<AsyncPgConnection>,
        cancel: CancellationToken,
        interval: std::time::Duration,
        threshold: i32,
        worker_stale_secs: i64,
        telemetry: std::sync::Arc<crate::telemetry::TelemetryConfig>,
        // Issue #1243: forwarded to the sharded spawner.
        payload_codecs: crate::payload_codec::PayloadCodecs,
    ) -> tokio::task::JoinHandle<()> {
        spawn_poison_pill_reclaimer_for_shard(
            pool,
            cancel,
            interval,
            threshold,
            worker_stale_secs,
            None,
            telemetry,
            None,
            payload_codecs,
        )
    }

    /// [`spawn_poison_pill_reclaimer`], attributing this loop instance to
    /// `shard` in the `scanner_liveness` health check (issue #797).
    ///
    /// A multi-shard worker spawns one reclaimer per assigned shard, all
    /// registered under the same `poison_pill` scanner label. Passing the shard
    /// here is what lets the health check say *which* shard's loop is wedged --
    /// the metric carries no shard label, so this is the only surface that can
    /// localize it.
    ///
    /// Pass `None` for a process-wide loop or a single-shard deployment.
    #[must_use]
    // Issue #1243: the codec registry pushed this spawner past the
    // pedantic argument limit; its parameters are already a flat
    // shard-runtime bundle and restructuring them is out of scope here.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn_poison_pill_reclaimer_for_shard(
        pool: diesel_async::pooled_connection::deadpool::Pool<AsyncPgConnection>,
        cancel: CancellationToken,
        interval: std::time::Duration,
        threshold: i32,
        worker_stale_secs: i64,
        // Issue #1459: gates the stuck-running backstop pass (a `workflow`
        // task stuck long past its decision-cycle budget, worker liveness
        // notwithstanding). `None` disables it, matching pre-#1459 behavior.
        stuck_running_secs: Option<i64>,
        telemetry: std::sync::Arc<crate::telemetry::TelemetryConfig>,
        shard: Option<crate::types::ShardId>,
        // Issue #1243: owned so it can move into the spawned loop. The
        // registry's rotation state is shared across clones, so a later
        // `set_active_key` still reaches this long-lived task.
        payload_codecs: crate::payload_codec::PayloadCodecs,
    ) -> tokio::task::JoinHandle<()> {
        // Issue #797: declare the loop before its first iteration so the
        // `scanner_liveness` check expects it and grants it boot grace. The
        // shard is carried so a multi-shard worker's snapshot can name WHICH
        // shard's reclaimer wedged -- the tick counter carries no shard label.
        let owner = crate::scanner_health::register_scanner_for_shard(
            &*telemetry.metrics,
            crate::scanner_health::Scanner::PoisonPill,
            interval,
            shard,
        );
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = cancel.cancelled() => break,
                    () = tokio::time::sleep(interval) => {}
                }
                match pool.get().await {
                    Ok(mut conn) => {
                        match reclaim_orphaned_tasks(
                            &mut conn,
                            threshold,
                            worker_stale_secs,
                            stuck_running_secs,
                            &*telemetry.metrics,
                            &payload_codecs,
                        )
                        .await
                        {
                            Ok(summary) if summary.total() > 0 => {
                                tracing::warn!(
                                    requeued = summary.requeued,
                                    quarantined = summary.quarantined,
                                    stuck_requeued = summary.stuck_requeued,
                                    "reclaimed orphaned poison-pill tasks"
                                );
                            }
                            Ok(_) => {}
                            Err(e) => {
                                tracing::error!(error = %e, "poison-pill reclaim sweep failed");
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "failed to acquire DB connection for poison-pill reclaim");
                    }
                }
                // Issue #797: unconditional end-of-iteration liveness tick.
                crate::scanner_health::record_scanner_tick(&*telemetry.metrics, owner);
                if cancel.is_cancelled() {
                    break;
                }
            }
            // Issue #797: a graceful stop retires this loop from the expected
            // scanner set. A panic unwinds past here, so a panicked loop stays
            // registered and correctly ages into `Wedged`.
            crate::scanner_health::deregister_scanner(owner);
        })
    }
}

#[cfg(feature = "db")]
pub use scanner::{
    QUARANTINE_REASON, reclaim_orphaned_tasks, spawn_poison_pill_reclaimer,
    spawn_poison_pill_reclaimer_for_shard,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_strike_under_threshold_requeues() {
        assert_eq!(quarantine_decision(1, 3), ReclaimAction::Requeue);
        assert_eq!(quarantine_decision(2, 3), ReclaimAction::Requeue);
    }

    #[test]
    fn reaching_threshold_quarantines() {
        assert_eq!(quarantine_decision(3, 3), ReclaimAction::Quarantine);
    }

    #[test]
    fn beyond_threshold_quarantines() {
        assert_eq!(quarantine_decision(4, 3), ReclaimAction::Quarantine);
        assert_eq!(quarantine_decision(100, 3), ReclaimAction::Quarantine);
    }

    #[test]
    fn threshold_one_quarantines_on_first_strike() {
        assert_eq!(quarantine_decision(1, 1), ReclaimAction::Quarantine);
    }

    #[test]
    fn zero_threshold_never_quarantines() {
        assert_eq!(quarantine_decision(1, 0), ReclaimAction::Requeue);
        assert_eq!(quarantine_decision(1_000, 0), ReclaimAction::Requeue);
    }

    #[test]
    fn negative_threshold_never_quarantines() {
        assert_eq!(quarantine_decision(50, -1), ReclaimAction::Requeue);
    }

    #[test]
    fn orphan_query_targets_running_dead_worker_rows() {
        let sql = orphaned_running_tasks_query();
        assert!(sql.contains("harvest_task_queue"), "scans the task queue");
        assert!(sql.contains("state = 'RUNNING'"), "only RUNNING rows");
        assert!(
            sql.contains("harvest_workers"),
            "joins worker liveness, not per-task timeout columns"
        );
        assert!(
            sql.contains("last_heartbeat_at"),
            "liveness is heartbeat-driven"
        );
        assert!(sql.contains("NOT EXISTS"), "dead = no live worker row");
    }

    #[test]
    fn reclaim_summary_total_sums_all_three_buckets() {
        let summary = ReclaimSummary {
            requeued: 2,
            quarantined: 1,
            stuck_requeued: 4,
        };
        assert_eq!(summary.total(), 7);
    }

    #[test]
    fn stuck_query_targets_running_workflow_rows_past_deadline() {
        let sql = stuck_running_tasks_query();
        assert!(sql.contains("harvest_task_queue"), "scans the task queue");
        assert!(sql.contains("state = 'RUNNING'"), "only RUNNING rows");
        assert!(
            sql.contains("task_type = 'workflow'"),
            "activity tasks are excluded -- their own start_to_close is unrelated"
        );
        assert!(
            sql.contains("started_at"),
            "stuck is judged by wall-clock age, not worker liveness"
        );
        assert!(
            !sql.contains("harvest_workers"),
            "this backstop applies regardless of worker liveness"
        );
    }
}
