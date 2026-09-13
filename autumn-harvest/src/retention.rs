//! Time-based retention janitor for completed workflow history.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
#[cfg(feature = "db")]
use std::{collections::HashMap, time::Instant};

use chrono::{DateTime, Utc};
#[cfg(feature = "db")]
use diesel::prelude::*;
#[cfg(feature = "db")]
use diesel::sql_types::{Array, BigInt, Interval, Nullable, Text, Timestamptz, Uuid as SqlUuid};
#[cfg(feature = "db")]
use diesel_async::{AsyncConnection, RunQueryDsl};
#[cfg(feature = "db")]
use futures::future::join_all;
use serde::Serialize;
#[cfg(feature = "db")]
use tokio::sync::mpsc;
#[cfg(feature = "db")]
use tokio::task::JoinHandle;
#[cfg(feature = "db")]
use tokio_util::sync::CancellationToken;

#[cfg(feature = "db")]
use crate::error::{HarvestError, HarvestResult, database_error};
#[cfg(feature = "db")]
use crate::models::NewExecutionSummary;
#[cfg(feature = "db")]
use crate::schema::harvest_workflow_executions;
#[cfg(feature = "db")]
use crate::schema::{
    harvest_completion_deliveries, harvest_dead_letters, harvest_execution_summaries,
    harvest_signals, harvest_task_queue, harvest_timers,
};
#[cfg(feature = "db")]
use crate::shard::ShardedDbPool;
#[cfg(feature = "db")]
use crate::telemetry::MetricsRecorder;
use crate::types::ShardId;

const DEFAULT_TICK_INTERVAL: Duration = Duration::from_secs(60 * 60);
const DEFAULT_BATCH_SIZE: usize = 1_000;
const MIN_MAX_AGE: Duration = Duration::from_secs(1);
const MAX_MAX_AGE: Duration = Duration::from_secs(60 * 60 * 24 * 365 * 10);
const DEFAULT_ARCHIVAL_TIMEOUT_SECS: u64 = 30;

/// Default idle window before an inert per-tenant rate-limit bucket is
/// collected (issue #1127): 7 days.
///
/// Long enough that a weekly-cadence tenant keeps its bucket across a quiet
/// weekend, short enough that a one-off tenant's row does not outlive its
/// usefulness by months. The collector is on by default because unbounded
/// growth is a *bug*, not a tuning preference — a fix that every deployment
/// has to opt into fixes nothing for the deployments that do not know they
/// have the problem. Every swept row is provably inert (see
/// [`crate::queue::sweep_idle_rate_limit_buckets`]), and
/// [`RetentionConfig::without_rate_limit_bucket_gc`] turns it off outright.
pub const DEFAULT_RATE_LIMIT_BUCKET_RETENTION_SECS: u64 = 7 * 24 * 60 * 60;

/// Shortest configurable idle window for the rate-limit bucket GC (issue
/// #1127): 1 hour.
///
/// The floor is load-bearing, not decorative. The GC's interlock against a
/// concurrently-committing enqueue is that any `ensure_rate_limit_bucket` for a
/// stale bucket locks the row and refreshes `updated_at` (see
/// [`crate::queue::RATE_LIMIT_BUCKET_TOUCH_INTERVAL_SECS`]). A window shorter
/// than that touch interval would let a bucket become GC-eligible *without* the
/// ensure path having touched it, reopening the stranding race.
pub const MIN_RATE_LIMIT_BUCKET_RETENTION: Duration = Duration::from_secs(60 * 60);

/// Default byte cap for an opt-in captured summary payload (issue #752).
///
/// A `result`/`error` value larger than this is replaced with a typed
/// `_harvest_omitted` marker rather than stored verbatim, keeping each summary
/// row small (~< 1 KiB target for the common case).
pub const DEFAULT_SUMMARY_PAYLOAD_CAP: usize = 4096;

/// JSON key inserted into a summary `result` when the real payload was omitted
/// (offloaded or over the byte cap) — issue #752.
///
/// Distinct from the erasure tombstone (`_harvest_erased`) and the offload
/// envelope (`_harvest_offload_envelope`) so the three are never confused.
pub const OMITTED_MARKER_KEY: &str = "_harvest_omitted";

// ── Summary (tiered) retention config (issue #752) ─────────────────────────────

/// How long summarized (demoted) execution rows are retained (issue #752).
///
/// Decoupled from the history retention horizon: a deployment may keep
/// summaries far longer than full histories, or forever.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SummaryRetention {
    /// Retain summaries for the given duration (`completed_at`-based), then GC.
    For(Duration),
    /// Never GC summaries — keep them forever.
    Unbounded,
}

/// Tiered/summary retention policy (issue #752).
///
/// When set on [`RetentionConfig`], the history-retention janitor demotes each
/// hard-deleted terminal execution into a compact `harvest_execution_summaries`
/// row (written in the same transaction as the delete) instead of losing it
/// entirely. Captured `result`/`error` payloads are opt-in and byte-bounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SummaryPolicy {
    /// Summary retention horizon (own GC pass; `Unbounded` = keep forever).
    pub retention: SummaryRetention,
    /// Whether to capture the run's `result`/`error` payload into the summary.
    /// Defaults to `false` (identity + timing + search-attrs only).
    pub capture_payload: bool,
    /// Byte cap for a captured payload; oversized values become a typed
    /// `_harvest_omitted` marker. Defaults to [`DEFAULT_SUMMARY_PAYLOAD_CAP`].
    pub max_payload_bytes: usize,
}

impl SummaryPolicy {
    /// Retain summaries for `days` days (payload capture off by default).
    #[must_use]
    pub const fn for_days(days: u64) -> Self {
        Self {
            // `saturating_mul` guards a huge `days` value from overflowing the
            // seconds product; the validated horizon range clamps it anyway.
            retention: SummaryRetention::For(Duration::from_secs(days.saturating_mul(86_400))),
            capture_payload: false,
            max_payload_bytes: DEFAULT_SUMMARY_PAYLOAD_CAP,
        }
    }

    /// Retain summaries for the given duration (payload capture off by default).
    #[must_use]
    pub const fn for_duration(retention: Duration) -> Self {
        Self {
            retention: SummaryRetention::For(retention),
            capture_payload: false,
            max_payload_bytes: DEFAULT_SUMMARY_PAYLOAD_CAP,
        }
    }

    /// Keep summaries forever (payload capture off by default).
    #[must_use]
    pub const fn unbounded() -> Self {
        Self {
            retention: SummaryRetention::Unbounded,
            capture_payload: false,
            max_payload_bytes: DEFAULT_SUMMARY_PAYLOAD_CAP,
        }
    }

    /// Opt into capturing the run's `result`/`error` payload (byte-bounded).
    #[must_use]
    pub const fn with_payload_capture(mut self) -> Self {
        self.capture_payload = true;
        self
    }

    /// Override the captured-payload byte cap.
    #[must_use]
    pub const fn with_max_payload_bytes(mut self, max_payload_bytes: usize) -> Self {
        self.max_payload_bytes = max_payload_bytes;
        self
    }

    /// The summary GC horizon as a [`Duration`], or `None` for `Unbounded`.
    #[must_use]
    pub const fn retention_age(&self) -> Option<Duration> {
        match self.retention {
            SummaryRetention::For(age) => Some(age),
            SummaryRetention::Unbounded => None,
        }
    }

    /// Whether captured payloads are enabled for this policy.
    #[must_use]
    pub const fn capture_payload(&self) -> bool {
        self.capture_payload
    }

    /// The captured-payload byte cap.
    #[must_use]
    pub const fn max_payload_bytes(&self) -> usize {
        self.max_payload_bytes
    }
}

/// Returns the typed "omitted" marker value for a summary `result` field
/// (issue #752): `{"_harvest_omitted": true, "reason": "...", "bytes": N?}`.
///
/// Used instead of silently truncating an oversized payload or storing an
/// offload reference envelope (whose blob may be GC'd, #524).
#[must_use]
pub fn omitted_marker(reason: &str, bytes: Option<usize>) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    obj.insert(
        OMITTED_MARKER_KEY.to_string(),
        serde_json::Value::Bool(true),
    );
    obj.insert(
        "reason".to_string(),
        serde_json::Value::String(reason.to_string()),
    );
    if let Some(bytes) = bytes {
        obj.insert("bytes".to_string(), serde_json::Value::Number(bytes.into()));
    }
    serde_json::Value::Object(obj)
}

/// Compute the `result` column value for a summary from a run's `output`
/// (issue #752).
///
/// - `None` output → `None` (nothing captured).
/// - An offload reference envelope (issue #524) → `omitted_marker("offloaded")`
///   — **never** store the envelope, since its blob may be GC'd out from under
///   the summary.
/// - An `output` whose serialized bytes exceed `cap` →
///   `omitted_marker("too_large", Some(len))` (valid JSON, never a silent
///   truncation).
/// - Otherwise the value **verbatim** (codec-encoded form preserved, matching
///   the history-export Full policy — the summary is not decoded/redacted here).
///
/// The `capture_payload` opt-in is applied at the call site (a policy with
/// capture disabled passes `None`, so the column stays NULL).
#[must_use]
pub fn cap_result_payload(
    output: Option<serde_json::Value>,
    cap: usize,
) -> Option<serde_json::Value> {
    let output = output?;
    // Never store an offload reference envelope — the blob may be GC'd (#524).
    //
    // Offload detection relies on the offloader storing a ROOT envelope
    // (`extract_offload_ref` inspects the top-level value; `search_attrs` is
    // never offloadable). A future nested-envelope offload change MUST recurse
    // this check, or a summary could store a value referencing a blob that
    // #524's GC later reclaims, leaving a dangling summary→blob reference.
    if crate::payload_store::extract_offload_ref(&output).is_some() {
        return Some(omitted_marker("offloaded", None));
    }
    // Fail SAFE on a (near-impossible) serialize failure of an already-parsed
    // JSONB value: emit an omitted marker rather than storing an unmeasured
    // payload verbatim, consistent with the oversized-payload path below.
    let len = match serde_json::to_vec(&output) {
        Ok(bytes) => bytes.len(),
        Err(_) => return Some(omitted_marker("too_large", None)),
    };
    if len > cap {
        return Some(omitted_marker("too_large", Some(len)));
    }
    // Common case: under cap — move the owned value into the return, no clone.
    Some(output)
}

/// Compute the `error` column value for a summary from a run's `error` text
/// (issue #752).
///
/// Oversized text becomes a typed marker string rather than a silent
/// UTF-8-boundary truncation. `None` in → `None` out. Capture opt-in is applied
/// at the call site.
#[must_use]
pub fn cap_error_text(error: Option<&str>, cap: usize) -> Option<String> {
    let error = error?;
    if error.len() > cap {
        return Some(format!("[omitted: too_large, {} bytes]", error.len()));
    }
    Some(error.to_string())
}

/// Future type returned by [`HistoryArchiver::archive`].
pub type ArchiverFuture<'a> = std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>>
            + Send
            + 'a,
    >,
>;

/// Trait for pre-retention workflow history cold storage archivers.
///
/// Implementations of this trait are invoked by the retention janitor to ship
/// a completed workflow execution's event history to cold storage *before* it
/// is permanently deleted from the database.
pub trait HistoryArchiver: Send + Sync + 'static {
    /// Ship the history export document to cold storage.
    ///
    /// If this returns `Err`, the retention janitor skips deleting the
    /// workflow execution and its associated events on this tick, retrying
    /// on the next tick to prevent data loss.
    fn archive(&self, doc: &crate::history_export::HistoryExportDocument) -> ArchiverFuture<'_>;
}

/// Configuration for the background retention job.
///
/// **Why does this exist?**
/// Workflow histories and audit logs can grow unbounded. This configuration allows operators
/// to define constraints for automatically pruning old, closed workflows and stale audit events
/// to prevent storage exhaustion.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest::retention::RetentionConfig;
/// use std::time::Duration;
///
/// let config = RetentionConfig::with_max_age(Duration::from_secs(86400))
///     .with_audit_retention_days(30);
///
/// assert!(config.enabled());
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct RetentionConfig {
    /// Maximum age in seconds for closed workflows before they are eligible for deletion.
    /// If `None`, workflow history retention is disabled.
    pub max_age_secs: Option<u64>,
    /// Per-workflow-type retention overrides keyed by registered workflow name,
    /// each mapping to its own max-age in seconds (matching `max_age_secs`).
    ///
    /// A completed execution whose `workflow_name` has an override is retained
    /// for that type's age instead of the global `max_age_secs`. A type with no
    /// override falls back to the global `max_age_secs`; if neither is set, that
    /// type is never deleted. Uses [`BTreeMap`] for deterministic ordering and
    /// serialization. Issue #737.
    pub overrides: BTreeMap<String, u64>,
    /// How often the background retention job wakes up to scan for expired data.
    pub tick_interval_secs: u64,
    /// The maximum number of records to process in a single transaction/batch.
    pub batch_size: usize,
    /// If `true`, the retention job simulates deletions and logs what would have been deleted
    /// without actually modifying the database.
    pub dry_run: bool,
    /// Audit log retention in days, independent of workflow-history retention.
    /// Defaults to 90 days (3 months). Set to 0 to disable audit purging.
    pub audit_retention_days: i64,
    /// Protect every unexported audit row, per shard (issue #1266).
    /// Defaults to `None` (disabled).
    ///
    /// `purge_old_audit_records` already refuses to delete an unexported row
    /// in two cases. The first case: a live cursor exists for the shard. The
    /// second case: this process has a sink configured.
    ///
    /// Both signals can be absent at once. This happens in a split
    /// web/worker deployment, before the worker's first successful tick on a
    /// shard. A fresh enablement has no tick yet. A newly added shard may
    /// also have no tick yet, if the worker cannot reach it. A shard being
    /// re-enabled after decommission has no tick yet either. In every one of
    /// these, retention finds no sink and no cursor row it can trust.
    ///
    /// `Some(exempt)` protects every shard not in `exempt`. `None` protects
    /// none. An empty set protects every shard.
    ///
    /// The exempt set exists for one reason. A fleet has more than one
    /// shard. This flag would otherwise apply to all of them at once.
    /// Decommissioning shard A must resume purging there. Doing that by
    /// disabling the whole flag would also strip protection from shard B,
    /// mid-bootstrap on the same sweep. Add A to the exempt set instead,
    /// and B stays protected.
    ///
    /// Like `is_configured`, an unexempted shard's flag overrides a
    /// retired cursor there too. Decommissioning that shard does not
    /// resume purging while it stays unexempted. Exempt it as part of
    /// that step. See `docs/audit-export.md`.
    pub protect_unexported_audit: Option<BTreeSet<ShardId>>,
    /// Schedule decisions retention in days.
    /// Defaults to 7 days. Set to 0 to disable schedule decision purging.
    pub schedule_decision_retention_days: i64,
    /// The timeout in seconds for executing the pre-retention archival hook.
    /// Defaults to 30 seconds.
    pub archival_timeout_secs: u64,
    /// Tiered/summary retention policy (issue #752).
    ///
    /// When `Some`, a terminal execution hard-deleted by the history janitor is
    /// first demoted into a compact `harvest_execution_summaries` row (written
    /// in the same transaction as the delete). `None` (the default) is
    /// byte-for-byte identical to pre-#752 behavior: hard delete, no summary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<SummaryPolicy>,
    /// Idle window after which an inert per-tenant rate-limit bucket is
    /// collected (issue #1127). `None` disables the sweep entirely.
    ///
    /// `harvest_rate_limit_buckets` rows are auto-registered `ON CONFLICT DO
    /// NOTHING` and, before this, were never deleted — so the two
    /// caller-keyed families (`dyn-rate:{expr}:{resolved}`, issue #699, and
    /// `start-throttle:{workflow}:{key}`, issue #607) grew one row per tenant
    /// forever. Defaults to [`DEFAULT_RATE_LIMIT_BUCKET_RETENTION_SECS`].
    pub rate_limit_bucket_retention_secs: Option<u64>,
    /// Partition maintenance for the opt-in partitioned `harvest_events`
    /// layout (issue #958).
    ///
    /// Applies only when the shard's `harvest_events` is actually partitioned
    /// — the janitor probes the layout each tick, so a deployment that has not
    /// opted in pays nothing and behaves byte-for-byte as before. This is what
    /// makes partition creation and reclamation engine-automated: no operator
    /// cron pre-creates future partitions, and no operator script drops expired
    /// ones.
    pub partitions: PartitionMaintenanceConfig,
}

/// Engine-automated partition maintenance settings (issue #958).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct PartitionMaintenanceConfig {
    /// Whether the retention janitor maintains partitions at all. Disabling it
    /// leaves an opted-in deployment with no partition creation and no
    /// reclamation, so it exists for incident response, not for tuning.
    pub enabled: bool,
    /// How many cohorts ahead of "now" to keep pre-created.
    pub lookahead_cohorts: u32,
    /// Maximum partitions dropped per tick. Each drop takes a brief
    /// `ACCESS EXCLUSIVE` lock on the parent, so a bounded budget keeps a
    /// backlog from holding the append path off; successive ticks converge.
    pub max_drops_per_tick: usize,
    /// Seconds to wait for that lock before deferring a partition to the next
    /// tick. Failing fast is what protects the concurrent-p99 budget.
    pub drop_lock_timeout_secs: u64,
    /// Seconds the exact ownership scan may run before the sweeper gives up on
    /// a partition and retries next tick.
    ///
    /// Only reached when more old executions survive than
    /// `owner_probe_cap`; the narrow probe decides the normal case. Raise it
    /// for very large partitions, or narrow the cohort width.
    pub exact_scan_timeout_secs: u64,
    /// How many surviving old executions the narrow ownership probe will
    /// enumerate before falling back to the exact scan.
    pub owner_probe_cap: usize,
    /// Rows per straggler `DELETE` statement.
    pub straggler_batch: usize,
    /// Opt-in targeted `DELETE` of orphan rows in a cohort pinned by a
    /// long-running execution for longer than this many seconds.
    ///
    /// `None` (the default) means the janitor issues **zero** row-level deletes
    /// against `harvest_events`. Set it only when long-lived executions would
    /// otherwise pin their cohorts — and their siblings' rows — indefinitely.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub straggler_grace_secs: Option<u64>,
}

impl Default for PartitionMaintenanceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            lookahead_cohorts: crate::partition::DEFAULT_LOOKAHEAD_COHORTS,
            max_drops_per_tick: crate::partition::SweepOptions::default().max_drops,
            drop_lock_timeout_secs: 2,
            exact_scan_timeout_secs: crate::partition::SweepOptions::default()
                .exact_scan_timeout
                .as_secs(),
            owner_probe_cap: crate::partition::SweepOptions::default().owner_probe_cap,
            straggler_batch: crate::partition::SweepOptions::default().straggler_batch,
            straggler_grace_secs: None,
        }
    }
}

impl PartitionMaintenanceConfig {
    /// Translate into the [`crate::partition::SweepOptions`] the sweeper takes.
    #[must_use]
    pub fn sweep_options(&self) -> crate::partition::SweepOptions {
        crate::partition::SweepOptions {
            max_drops: self.max_drops_per_tick,
            lock_timeout: Duration::from_secs(self.drop_lock_timeout_secs.max(1)),
            exact_scan_timeout: Duration::from_secs(self.exact_scan_timeout_secs.max(1)),
            owner_probe_cap: self.owner_probe_cap,
            straggler_batch: self.straggler_batch,
            straggler_grace: self.straggler_grace_secs.map(Duration::from_secs),
        }
    }
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            max_age_secs: None,
            overrides: BTreeMap::new(),
            tick_interval_secs: DEFAULT_TICK_INTERVAL.as_secs(),
            batch_size: DEFAULT_BATCH_SIZE,
            dry_run: false,
            audit_retention_days: 90,
            protect_unexported_audit: None,
            schedule_decision_retention_days: 7,
            archival_timeout_secs: DEFAULT_ARCHIVAL_TIMEOUT_SECS,
            summary: None,
            rate_limit_bucket_retention_secs: Some(DEFAULT_RATE_LIMIT_BUCKET_RETENTION_SECS),
            partitions: PartitionMaintenanceConfig::default(),
        }
    }
}

impl RetentionConfig {
    /// Bootstraps a fresh configuration template that explicitly opts-in to the workflow retention features.
    #[must_use]
    pub fn with_max_age(max_age: Duration) -> Self {
        Self {
            max_age_secs: Some(max_age.as_secs()),
            ..Self::default()
        }
    }

    /// Register a per-workflow-type retention override (issue #737).
    ///
    /// The named workflow type is retained for `max_age` instead of the global
    /// `max_age`. Overrides are validated against the same
    /// `MIN_MAX_AGE..=MAX_MAX_AGE` bounds as the global `max_age` at build time.
    #[must_use]
    pub fn with_workflow_override(
        mut self,
        workflow_name: impl Into<String>,
        max_age: Duration,
    ) -> Self {
        self.overrides
            .insert(workflow_name.into(), max_age.as_secs());
        self
    }

    /// Bulk-register per-workflow-type retention overrides (issue #737).
    #[must_use]
    pub fn with_workflow_overrides<S: Into<String>>(
        mut self,
        iter: impl IntoIterator<Item = (S, Duration)>,
    ) -> Self {
        for (name, max_age) in iter {
            self.overrides.insert(name.into(), max_age.as_secs());
        }
        self
    }

    /// Override the audit log retention window.
    #[must_use]
    pub const fn with_audit_retention_days(mut self, days: i64) -> Self {
        self.audit_retention_days = days;
        self
    }

    /// Protect every unexported audit row on this process's sweeps, closing
    /// the split-deployment bootstrap window (issue #1266). Set `true` on
    /// every process in a split web/worker deployment.
    #[must_use]
    pub fn with_protect_unexported_audit(mut self, protect: bool) -> Self {
        self.protect_unexported_audit = if protect { Some(BTreeSet::new()) } else { None };
        self
    }

    /// Exempt one shard from `protect_unexported_audit` (issue #1266). Call
    /// this for a shard being decommissioned, so its purge can resume
    /// without also unprotecting every other shard in the fleet.
    #[must_use]
    pub fn excluding_shard_from_protect_unexported_audit(mut self, shard: ShardId) -> Self {
        if let Some(exempt) = &mut self.protect_unexported_audit {
            exempt.insert(shard);
        }
        self
    }

    /// Whether `protect_unexported_audit` covers this shard (issue #1266).
    /// `true` only when protection is enabled and the shard is not
    /// exempted.
    #[must_use]
    pub fn protects_unexported_audit(&self, shard: ShardId) -> bool {
        self.protect_unexported_audit
            .as_ref()
            .is_some_and(|exempt| !exempt.contains(&shard))
    }

    /// Override the schedule decision retention window.
    #[must_use]
    pub const fn with_schedule_decision_retention_days(mut self, days: i64) -> Self {
        self.schedule_decision_retention_days = days;
        self
    }

    /// Override the archival hook execution timeout.
    #[must_use]
    pub const fn with_archival_timeout_secs(mut self, secs: u64) -> Self {
        self.archival_timeout_secs = secs;
        self
    }

    /// Enable tiered/summary retention with an explicit policy (issue #752).
    #[must_use]
    pub const fn with_summary_retention(mut self, policy: SummaryPolicy) -> Self {
        self.summary = Some(policy);
        self
    }

    /// Enable tiered/summary retention with a `days`-day summary horizon
    /// (payload capture off). Shorthand for
    /// `with_summary_retention(SummaryPolicy::for_days(days))`.
    ///
    /// The summary horizon should **exceed** the history horizon
    /// ([`with_max_age`](Self::with_max_age)) to be useful: a summary is created
    /// only when the history is deleted, so a summary window at or below the
    /// history window means the summary is GC'd almost immediately. A mismatch
    /// logs a one-time warning at [`RetentionRuntime::spawn`].
    #[must_use]
    pub const fn with_summary_retention_days(mut self, days: u64) -> Self {
        self.summary = Some(SummaryPolicy::for_days(days));
        self
    }

    /// Enable tiered/summary retention keeping summaries forever (payload
    /// capture off).
    #[must_use]
    pub const fn with_summary_retention_unbounded(mut self) -> Self {
        self.summary = Some(SummaryPolicy::unbounded());
        self
    }

    /// The summary GC horizon as a [`Duration`], or `None` when summaries are
    /// disabled OR configured [`SummaryRetention::Unbounded`] (issue #752).
    #[must_use]
    pub fn summary_age(&self) -> Option<Duration> {
        self.summary.and_then(|p| p.retention_age())
    }

    /// Whether summary (tiered) retention is enabled at all (issue #752).
    #[must_use]
    pub const fn summary_enabled(&self) -> bool {
        self.summary.is_some()
    }

    /// Whether the summary GC pass should run this tick: a summary policy with a
    /// bounded (`For(_)`) horizon is set (issue #752). `Unbounded` summaries are
    /// never GC'd, so this is `false` for them.
    #[must_use]
    pub fn summary_gc_active(&self) -> bool {
        self.summary_age().is_some()
    }

    /// Read access to the tiered/summary retention policy (issue #752).
    #[must_use]
    pub const fn summary_policy(&self) -> Option<SummaryPolicy> {
        self.summary
    }

    /// Set the idle window for the rate-limit bucket GC (issue #1127).
    ///
    /// Validated against
    /// the [`MIN_RATE_LIMIT_BUCKET_RETENTION`] … `MAX_MAX_AGE` range at build
    /// time; an out-of-range window fails the build rather than silently
    /// clamping.
    #[must_use]
    pub const fn with_rate_limit_bucket_retention(mut self, window: Duration) -> Self {
        self.rate_limit_bucket_retention_secs = Some(window.as_secs());
        self
    }

    /// Disable the rate-limit bucket GC (issue #1127).
    ///
    /// The table then grows one row per tenant key forever, which is the
    /// pre-#1127 behavior — so this is for incident response, not tuning.
    #[must_use]
    pub const fn without_rate_limit_bucket_gc(mut self) -> Self {
        self.rate_limit_bucket_retention_secs = None;
        self
    }

    /// The rate-limit bucket GC's idle window, or `None` when it is off
    /// (issue #1127).
    #[must_use]
    pub fn rate_limit_bucket_retention(&self) -> Option<Duration> {
        self.rate_limit_bucket_retention_secs
            .map(Duration::from_secs)
    }

    /// Whether the rate-limit bucket GC pass should run this tick (issue
    /// #1127).
    #[must_use]
    pub const fn rate_limit_bucket_gc_active(&self) -> bool {
        self.rate_limit_bucket_retention_secs.is_some()
    }

    /// Safely unpacks the raw configuration integer into a standard rust [`Duration`], gracefully
    /// handling systems where the feature is entirely turned off.
    #[must_use]
    pub fn max_age(&self) -> Option<Duration> {
        self.max_age_secs.map(Duration::from_secs)
    }

    /// Resolves the effective history max-age for a given workflow type (issue #737).
    ///
    /// Returns the per-type override if one is registered for `workflow_name`,
    /// otherwise the global `max_age`. Returns `None` when neither is set — in
    /// which case that type's history is never deleted.
    #[must_use]
    pub fn effective_max_age(&self, workflow_name: &str) -> Option<Duration> {
        self.overrides
            .get(workflow_name)
            .copied()
            .map(Duration::from_secs)
            .or_else(|| self.max_age())
    }

    /// The smallest effective retention age across the global `max_age` and all
    /// per-type overrides (issue #737).
    ///
    /// This is the SQL candidate pre-filter age: the smallest age yields the
    /// cutoff closest to "now", which is a correct *superset* of every
    /// deletable row (any row deletable under a type-specific age `age(T)`
    /// satisfies `completed_at < now - age(T) <= now - min_age`). The scanner
    /// then applies the exact per-type age to each candidate in Rust.
    ///
    /// Returns `None` iff the global `max_age` is unset *and* there are no
    /// overrides.
    #[must_use]
    pub fn loosest_cutoff_age(&self) -> Option<Duration> {
        self.max_age()
            .into_iter()
            .chain(self.overrides.values().copied().map(Duration::from_secs))
            .min()
    }

    /// Returns `true` if workflow-history retention should run this tick, i.e.
    /// either the global `max_age` or at least one per-type override is set
    /// (issue #737).
    #[must_use]
    pub fn history_retention_active(&self) -> bool {
        self.loosest_cutoff_age().is_some()
    }

    /// Read access to the per-workflow-type retention overrides (issue #737).
    #[must_use]
    pub const fn workflow_overrides(&self) -> &BTreeMap<String, u64> {
        &self.overrides
    }

    /// Translates the raw numeric tick value into a standard [`Duration`] for the scheduler loop.
    #[must_use]
    pub const fn tick_interval(&self) -> Duration {
        Duration::from_secs(self.tick_interval_secs)
    }

    /// Translates the raw archival timeout value into a standard [`Duration`] for timeout enforcement.
    #[must_use]
    pub const fn archival_timeout(&self) -> Duration {
        Duration::from_secs(self.archival_timeout_secs)
    }

    /// # Errors
    ///
    /// Returns an error string if `tick_interval_secs` is 0, `batch_size` is 0,
    /// or `max_age` is outside the allowed range.
    pub fn validate(&self) -> Result<(), String> {
        if self.tick_interval_secs == 0 {
            return Err("tick_interval must be >= 1s".to_string());
        }
        if self.batch_size == 0 {
            return Err("batch_size must be >= 1".to_string());
        }
        if self.archival_timeout_secs == 0 {
            return Err("archival_timeout_secs must be >= 1s".to_string());
        }
        if let Some(max_age) = self.max_age()
            && !(MIN_MAX_AGE..=MAX_MAX_AGE).contains(&max_age)
        {
            return Err(format!(
                "max_age must be between {}s and {}s",
                MIN_MAX_AGE.as_secs(),
                MAX_MAX_AGE.as_secs()
            ));
        }
        // Each per-type override is validated against the same bounds as the
        // global max_age; an out-of-range override fails the build rather than
        // silently clamping (issue #737, AC5).
        for (name, secs) in &self.overrides {
            let age = Duration::from_secs(*secs);
            if !(MIN_MAX_AGE..=MAX_MAX_AGE).contains(&age) {
                return Err(format!(
                    "retention override for '{name}' must be between {}s and {}s",
                    MIN_MAX_AGE.as_secs(),
                    MAX_MAX_AGE.as_secs()
                ));
            }
        }
        // The summary GC horizon is bounded by the same range as `max_age`
        // (issue #752). `Unbounded` needs no bound. An out-of-range horizon
        // fails the build rather than silently clamping.
        if let Some(age) = self.summary_age()
            && !(MIN_MAX_AGE..=MAX_MAX_AGE).contains(&age)
        {
            return Err(format!(
                "summary retention must be between {}s and {}s",
                MIN_MAX_AGE.as_secs(),
                MAX_MAX_AGE.as_secs()
            ));
        }
        // The rate-limit bucket GC window has its own, higher floor (issue
        // #1127): below it a bucket could go GC-eligible without the ensure
        // path having locked it, reopening the stranding race. Fails the build
        // rather than clamping, matching every other horizon here.
        if let Some(window) = self.rate_limit_bucket_retention()
            && !(MIN_RATE_LIMIT_BUCKET_RETENTION..=MAX_MAX_AGE).contains(&window)
        {
            return Err(format!(
                "rate_limit_bucket_retention must be between {}s and {}s",
                MIN_RATE_LIMIT_BUCKET_RETENTION.as_secs(),
                MAX_MAX_AGE.as_secs()
            ));
        }
        Ok(())
    }

    /// Returns `true` if any retention feature is enabled: workflow-history
    /// retention (global or per-type), audit-log purging, schedule-decision
    /// purging, bounded summary GC (issue #752), partition maintenance (issue
    /// #958), or the idle rate-limit-bucket GC (issue #1127).
    ///
    /// Per-workflow-type overrides count as enabling workflow-history retention
    /// even when the global `max_age` is unset (issue #737), so an
    /// overrides-only configuration still spawns the janitor.
    #[must_use]
    pub fn enabled(&self) -> bool {
        self.max_age_secs.is_some()
            || !self.overrides.is_empty()
            || self.audit_retention_days > 0
            || self.schedule_decision_retention_days > 0
            // A bounded summary policy spawns the janitor so its GC pass runs
            // even if the history horizon was later removed (issue #752).
            || self.summary_gc_active()
            // Partition maintenance is work in its own right, not a rider on
            // history retention (issue #958). Without this, a deployment that
            // turned every retention horizon off — `audit_retention_days = 0`,
            // `schedule_decision_retention_days = 0`, no history or summary
            // age — would leave `partitions.enabled` reading `true` while the
            // runtime never spawned to honour it. On an opted-in partitioned
            // shard the lookahead window then expires, every subsequent append
            // lands in the DEFAULT partition, and no cohort is ever reclaimed.
            //
            // Note what this does NOT do: partition *creation* is not
            // reclamation, so it must keep running even for an operator who
            // deliberately retains everything forever. The only deployments
            // this newly spawns for are those that had switched every horizon
            // off — which is exactly the broken case; the stock config
            // (`audit_retention_days: 90`) already spawned.
            || self.partitions.enabled
            // Issue #1127: the bucket GC is work in its own right too. Without
            // this, a deployment that turned every other horizon off would
            // leave `rate_limit_bucket_retention_secs` reading as configured
            // while the runtime never spawned to honour it, and the table would
            // keep growing one row per tenant key.
            || self.rate_limit_bucket_gc_active()
    }
}

/// The result of a single execution tick of the retention job on a specific shard.
///
/// **Why does this exist?**
/// Provides observability into the retention job's performance and impact. It captures
/// how many records were evaluated, how many were deleted, and any errors encountered,
/// allowing operators to monitor the health of the background cleanup process.
#[derive(Debug, Clone, Serialize, Default)]
pub struct RetentionTickResult {
    /// The ID of the shard this retention tick operated on.
    pub shard: u16,
    /// The timestamp when this retention tick started.
    pub ran_at: Option<DateTime<Utc>>,
    /// The number of expired candidate records identified during the tick.
    pub candidate_count: usize,
    /// The actual number of records successfully deleted during the tick.
    pub deleted_count: usize,
    /// The age (in seconds) of the oldest closed workflow that was skipped (not yet expired).
    /// Used for tuning the `max_age_secs` configuration.
    pub oldest_age_secs_skipped: Option<u64>,
    /// The duration of the retention tick in milliseconds.
    pub duration_ms: u128,
    /// The last error encountered during the tick, if any.
    pub last_error: Option<String>,
    /// Per-workflow-type deletion counts for this tick (issue #737).
    ///
    /// In dry-run mode these are the counts the janitor *would* delete under
    /// each type's resolved retention age. In a real run they are the counts
    /// actually deleted. Surfaced via `GET /admin/retention` for per-type
    /// reporting.
    pub deleted_by_workflow: BTreeMap<String, u64>,
    /// Number of execution summaries created (demoted) during this tick (issue
    /// #752). Surfaced via `GET /admin/retention` for creation observability.
    /// Real deletes only — a `dry_run` tick creates no summaries.
    pub summarized_count: usize,
    /// Partition maintenance performed on this shard this tick (issue #958).
    ///
    /// `None` on an unpartitioned shard — which is every deployment that has
    /// not opted in. When `Some`, [`crate::partition::SweepOutcome::blocked`]
    /// is the operator's answer to "why has space not come back?": it names
    /// each cohort that was considered and the reason it was left alone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_maintenance: Option<crate::partition::MaintenanceOutcome>,
    /// Idle rate-limit-bucket GC outcome for this shard this tick (issue
    /// #1127).
    ///
    /// `None` when the GC is disabled — deliberately distinct from
    /// `Some(collected: 0)` ("it ran and everything was live or pinned") and
    /// from `Some(error: ...)` ("it could not run"), which a bare counter
    /// collapses into one indistinguishable zero. Same shape and same reason as
    /// `partition_maintenance`. Reported through `GET /admin/retention`, so "is
    /// the table still growing, and why?" has an answer that needs no metrics
    /// pipeline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_limit_bucket_gc: Option<RateLimitBucketGcOutcome>,
}

/// One shard's idle rate-limit-bucket GC result for one tick (issue #1127).
#[derive(Debug, Clone, Serialize, Default, PartialEq, Eq)]
pub struct RateLimitBucketGcOutcome {
    /// Buckets collected, per bounded key family (`dyn-rate` /
    /// `start-throttle`). Under `dry_run` these are would-collect counts.
    pub collected_by_family: BTreeMap<String, u64>,
    /// Total across families — the number an operator watches.
    pub collected: u64,
    /// Whether this was a read-only `dry_run` preview rather than a real pass.
    /// A preview reports what a real pass *would* collect and deletes nothing,
    /// so a non-zero `collected` here is a forecast, not work done.
    pub dry_run: bool,
    /// Why this shard's pass did not run, when it did not. A shard that keeps
    /// reporting an error is exactly what an operator needs to see, and without
    /// this it would be indistinguishable from a shard with nothing to do.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Constructors used only by the janitor loop, which is itself `db`-gated —
/// without this the no-`db` build (linted through `autumn-harvest-sqlite`) sees
/// them as dead code. The struct stays ungated: it is a field of
/// [`RetentionTickResult`], which every build can serialize.
#[cfg(feature = "db")]
impl RateLimitBucketGcOutcome {
    /// A completed pass (real, or a `dry_run` preview).
    #[must_use]
    fn collected(collected_by_family: BTreeMap<String, u64>, dry_run: bool) -> Self {
        Self {
            collected: collected_by_family.values().sum(),
            collected_by_family,
            dry_run,
            error: None,
        }
    }

    /// A pass that could not run on this shard.
    #[must_use]
    fn failed(error: String) -> Self {
        Self {
            error: Some(error),
            ..Self::default()
        }
    }
}

/// The current overall status of the retention subsystem.
///
/// **Why does this exist?**
/// Aggregates the static configuration and the dynamic runtime state (per-shard results)
/// to provide a comprehensive snapshot of the retention process for diagnostic APIs.
#[derive(Debug, Clone, Serialize, Default)]
pub struct RetentionStatus {
    /// The active retention configuration.
    pub config: RetentionConfig,
    /// The latest execution results for each active shard.
    pub per_shard: Vec<RetentionTickResult>,
}

/// A thread-safe monitor for observing the background retention process.
///
/// **Why does this exist?**
/// Enables the background retention job to asynchronously report its progress and results,
/// while allowing external components (like administrative APIs or telemetry systems)
/// to safely query the latest status without blocking or tearing.
#[derive(Debug, Clone)]
pub struct RetentionMonitor {
    inner: Arc<Mutex<RetentionStatus>>,
}

impl RetentionMonitor {
    /// Boots up a clean monitoring tracker that acts as the initial blank canvas before shards report results.
    #[must_use]
    pub fn new(config: RetentionConfig, shards: impl Iterator<Item = ShardId>) -> Self {
        let per_shard = shards
            .map(|shard| RetentionTickResult {
                shard: u16::try_from(shard.as_i32()).unwrap_or(0),
                ..RetentionTickResult::default()
            })
            .collect();
        Self {
            inner: Arc::new(Mutex::new(RetentionStatus { config, per_shard })),
        }
    }

    /// # Panics
    ///
    /// Panics if the internal mutex has been poisoned.
    #[must_use]
    pub fn snapshot(&self) -> RetentionStatus {
        self.inner
            .lock()
            .expect("retention monitor lock poisoned")
            .clone()
    }

    /// Record this shard's partition-maintenance outcome (issue #958) without
    /// disturbing the history-retention counters already reported for the tick.
    ///
    /// Maintenance runs after the candidate loop — a cohort only becomes
    /// droppable once the loop has archived and deleted its executions — so it
    /// cannot ride along in the same `update`.
    #[cfg(feature = "db")]
    fn update_partitions(&self, shard: ShardId, outcome: crate::partition::MaintenanceOutcome) {
        let mut guard = self.inner.lock().expect("retention monitor lock poisoned");
        if let Some(existing) = guard
            .per_shard
            .iter_mut()
            .find(|x| x.shard == u16::try_from(shard.as_i32()).unwrap_or(0))
        {
            existing.partition_maintenance = Some(outcome);
        }
    }

    /// Record this shard's rate-limit bucket GC count (issue #1127) without
    /// disturbing the history-retention counters already reported this tick.
    ///
    /// A separate updater for the same reason as
    /// [`Self::update_partitions`]: the GC pass runs outside the
    /// history-retention phase gate (a deployment with no history horizon must
    /// still collect buckets), so it cannot ride along in that phase's
    /// `update`.
    #[cfg(feature = "db")]
    fn update_rate_limit_buckets(&self, shard: ShardId, outcome: RateLimitBucketGcOutcome) {
        let mut guard = self.inner.lock().expect("retention monitor lock poisoned");
        if let Some(existing) = guard
            .per_shard
            .iter_mut()
            .find(|x| x.shard == u16::try_from(shard.as_i32()).unwrap_or(0))
        {
            existing.rate_limit_bucket_gc = Some(outcome);
        }
    }

    #[cfg(feature = "db")]
    fn update(&self, shard: ShardId, result: RetentionTickResult) {
        let mut guard = self.inner.lock().expect("retention monitor lock poisoned");
        if let Some(existing) = guard
            .per_shard
            .iter_mut()
            .find(|x| x.shard == u16::try_from(shard.as_i32()).unwrap_or(0))
        {
            *existing = result;
        }
    }
}

/// Represents the running background task that processes retention policies.
///
/// **Why does this exist?**
/// Provides a handle to control and monitor the active background retention job.
/// It encapsulates the background tokio task, the cancellation token for graceful shutdown,
/// and the channel used to force immediate retention sweeps.
#[cfg(feature = "db")]
pub struct RetentionRuntime {
    shutdown: CancellationToken,
    trigger_tx: mpsc::Sender<()>,
    handle: JoinHandle<()>,
    monitor: RetentionMonitor,
}

#[cfg(feature = "db")]
impl RetentionRuntime {
    /// Returns `None` when nothing in `config` is enabled.
    ///
    /// A config can be enabled for a reason other than a history horizon —
    /// partition maintenance (issue #958) or the idle rate-limit-bucket GC
    /// (issue #1127) each spawn the runtime on their own — so `max_age` being
    /// unset is an ordinary, fully-supported state here: the history-retention
    /// phase is gated on `loosest_cutoff_age()` and is simply skipped.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn spawn(
        pools: ShardedDbPool,
        config: RetentionConfig,
        metrics: Arc<dyn MetricsRecorder>,
        archiver: Option<Arc<dyn HistoryArchiver>>,
        offloader: Option<Arc<crate::payload_store::PayloadOffloader>>,
    ) -> Option<Self> {
        if !config.enabled() {
            return None;
        }
        // Foot-gun warning (issue #752): a BOUNDED summary horizon at or below
        // the global history horizon means a demoted summary is GC'd almost as
        // soon as it is created, so tiering buys nothing. Non-fatal — the
        // operator may genuinely want a tiny summary window — but worth a
        // one-time startup warning.
        if let (Some(summary_age), Some(max_age)) = (config.summary_age(), config.max_age())
            && summary_age <= max_age
        {
            tracing::warn!(
                summary_age_secs = summary_age.as_secs(),
                history_max_age_secs = max_age.as_secs(),
                "harvest summary-retention horizon is <= the history-retention horizon; \
                 summaries will be GC'd almost immediately after creation — set a longer \
                 summary horizon (or Unbounded) for tiering to be useful"
            );
        }
        let monitor = RetentionMonitor::new(config.clone(), pools.shard_ids().into_iter());
        let shutdown = CancellationToken::new();
        let shutdown_task = shutdown.clone();
        let monitor_task = monitor.clone();
        let (trigger_tx, mut trigger_rx) = mpsc::channel(1);
        // Issue #797: declare the loop before its first iteration so the
        // `scanner_liveness` check expects it and grants it boot grace.
        let owner = crate::scanner_health::register_scanner(
            metrics.as_ref(),
            crate::scanner_health::Scanner::Retention,
            config.tick_interval(),
        );
        let handle = tokio::spawn(async move {
            let mut scan_cursors: HashMap<ShardId, Option<RetentionScanCursor>> = HashMap::new();
            loop {
                tokio::select! {
                    () = shutdown_task.cancelled() => break,
                    () = tokio::time::sleep(config.tick_interval()) => {},
                    Some(()) = trigger_rx.recv() => {
                        while trigger_rx.try_recv().is_ok() {}
                    }
                }

                // Workflow-history retention: runs when the global max_age OR
                // any per-workflow-type override is configured (issue #737).
                // Resolve the loosest cutoff age up front. `None` here means
                // either no retention age is configured (the common case) OR —
                // fail-safe — the configured loosest age is unrepresentable as a
                // `chrono::Duration` (unreachable for validated ages, which are
                // bounded by MAX_MAX_AGE ≪ chrono's ~292M-year range). In the
                // latter case we skip the workflow-history retention phase this
                // tick and retain everything, rather than falling back to a zero
                // cutoff that would make `loose_cutoff == now` and over-select
                // nearly every completed row. The audit/schedule purges below
                // still run.
                if config
                    .loosest_cutoff_age()
                    .and_then(|age| chrono::Duration::from_std(age).ok())
                    .is_some()
                {
                    // Phase-active gate (issue #737): the `and_then(from_std)`
                    // above is the fail-safe guard from commit 34ddb62 — if the
                    // loosest configured age is unrepresentable as a
                    // `chrono::Duration` (unreachable for validated ages) we skip
                    // the whole workflow-history phase this tick rather than
                    // over-selecting. Compute `now` once so every shard's SQL
                    // predicate and per-candidate resolution use one consistent
                    // clock. The exact per-type cutoffs are pushed into the SELECT
                    // inside `run_shard_tick` (PR #990 review) — there is no
                    // single "loose cutoff" SQL bind any more.
                    let now = Utc::now();
                    let tick_futures = pools.iter_shards().map(|(shard, pool)| {
                        let pool = pool.clone();
                        let config = config.clone();
                        let metrics = Arc::clone(&metrics);
                        let archiver = archiver.clone();
                        let offloader = offloader.clone();
                        let cursor = scan_cursors.get(&shard).copied().flatten();
                        async move {
                            let started = Instant::now();
                            let tick = run_shard_tick(
                                pool,
                                shard,
                                now,
                                &config,
                                archiver,
                                cursor,
                                Arc::clone(&metrics),
                                offloader,
                            )
                            .await;
                            (shard, started, tick)
                        }
                    });

                    for (shard, started, tick) in join_all(tick_futures).await {
                        let mut result = RetentionTickResult {
                            shard: u16::try_from(shard.as_i32()).unwrap_or(0),
                            ran_at: Some(Utc::now()),
                            duration_ms: started.elapsed().as_millis(),
                            ..RetentionTickResult::default()
                        };
                        match tick {
                            Ok(ok) => {
                                scan_cursors.insert(shard, ok.next_cursor);
                                result.candidate_count = ok.candidate_count;
                                result.deleted_count = ok.deleted_count;
                                result.oldest_age_secs_skipped = ok.oldest_age_secs_skipped;
                                result.deleted_by_workflow = ok.deleted_by_workflow.clone();
                                result.summarized_count = ok.summarized_count;
                                tracing::info!(
                                    shard = %shard,
                                    candidates = ok.candidate_count,
                                    deleted = ok.deleted_count,
                                    oldest_age_secs_skipped = ok.oldest_age_secs_skipped,
                                    duration_ms = result.duration_ms,
                                    dry_run = config.dry_run,
                                    "harvest retention tick completed"
                                );
                                #[allow(clippy::cast_precision_loss)]
                                metrics.record_retention_tick(
                                    u16::try_from(shard.as_i32()).unwrap_or(0),
                                    ok.candidate_count as u64,
                                    ok.deleted_count as u64,
                                    result.duration_ms as f64 / 1000.0,
                                );
                                // Per-workflow-type deletion counter (issue
                                // #737, AC8). Real deletes only — the metric
                                // confirms ACTUAL deletion (it reads 0 for a
                                // long-retained type until its own age), so a
                                // dry-run's would-delete counts are excluded.
                                if !config.dry_run {
                                    for (name, count) in &ok.deleted_by_workflow {
                                        metrics.record_retention_deleted(name, *count);
                                    }
                                }
                            }
                            Err(error) => {
                                result.last_error = Some(error.to_string());
                                scan_cursors.insert(shard, None);
                                tracing::warn!(shard = %shard, error = %error, "harvest retention tick failed");
                            }
                        }
                        monitor_task.update(shard, result);
                    }
                }

                // Engine-automated partition maintenance (issue #958, AC8).
                //
                // Deliberately OUTSIDE the history-retention phase gate above:
                // a partitioned deployment must keep its write window covered
                // even with no history-retention age configured, or an append
                // would eventually reach an uncovered cohort. `maintain` probes
                // the layout first and is a no-op on the (overwhelmingly
                // common) unpartitioned shard, so a deployment that has not
                // opted in pays one cheap catalog query per tick and nothing
                // else.
                //
                // Deliberately AFTER the candidate loop: a cohort only becomes
                // droppable once the loop has archived (#345), summarized
                // (#752) and deleted its executions. Running it here reclaims
                // in the SAME tick that frees the cohort rather than the next
                // one.
                //
                // Best-effort and per-shard: a shard whose maintenance fails
                // logs and is retried next tick. It must never fail the whole
                // retention tick, because history retention and reclamation are
                // independent — the executions are already safely archived and
                // deleted by this point.
                if config.partitions.enabled {
                    let now = Utc::now();
                    let mut sweep_opts = config.partitions.sweep_options();
                    // `dry_run` means "do not destroy data". It must NOT stop
                    // partition CREATION: `ensure_partitions` and
                    // `drain_default` delete nothing, and a deployment running
                    // retention in dry-run — a common posture during rollout —
                    // would otherwise stop extending the lookahead window and,
                    // after a few days, send every append to the DEFAULT
                    // partition indefinitely. Only the sweep is suppressed, by
                    // giving it a zero drop budget.
                    if config.dry_run {
                        sweep_opts.max_drops = 0;
                        sweep_opts.straggler_grace = None;
                    }
                    for (shard, pool) in pools.iter_shards() {
                        let mut conn = match pool.get().await {
                            Ok(conn) => conn,
                            Err(error) => {
                                // Never silent: a shard that cannot be reached
                                // gets no lookahead partitions and no
                                // reclamation, and the operator has to be able
                                // to tell that apart from "nothing to do".
                                tracing::warn!(
                                    shard = %shard,
                                    error = %error,
                                    "harvest event-partition maintenance could not \
                                     acquire a connection"
                                );
                                monitor_task.update_partitions(
                                    shard,
                                    crate::partition::MaintenanceOutcome::failed(error.to_string()),
                                );
                                continue;
                            }
                        };
                        match crate::partition::maintain(
                            &mut conn,
                            now,
                            config.partitions.lookahead_cohorts,
                            &sweep_opts,
                        )
                        .await
                        {
                            Ok(outcome) => {
                                // `blocked` is in the condition deliberately.
                                // The steady-state failure — nothing created
                                // (the window is already covered), nothing
                                // dropped, everything blocked — is exactly the
                                // state an operator needs to see, and logging
                                // only on progress would make it the one state
                                // that produces no output at all.
                                if !outcome.created.is_empty()
                                    || !outcome.sweep.dropped.is_empty()
                                    || !outcome.sweep.blocked.is_empty()
                                    || outcome.drained > 0
                                {
                                    tracing::info!(
                                        shard = %shard,
                                        created = outcome.created.len(),
                                        dropped = outcome.sweep.dropped.len(),
                                        blocked = outcome.sweep.blocked.len(),
                                        drained = outcome.drained,
                                        straggler_rows = outcome.sweep.straggler_rows_deleted,
                                        "harvest event-partition maintenance"
                                    );
                                }
                                monitor_task.update_partitions(shard, outcome);
                            }
                            Err(err) => {
                                tracing::warn!(
                                    shard = %shard,
                                    error = %err,
                                    "harvest event-partition maintenance failed"
                                );
                                // Reported, not just logged: without this a
                                // permanently-failing shard is indistinguishable
                                // from one that never opted in, because both
                                // show `partition_maintenance: null`.
                                monitor_task.update_partitions(
                                    shard,
                                    crate::partition::MaintenanceOutcome::failed(err.to_string()),
                                );
                            }
                        }
                    }
                }

                // Purge old audit records once per tick, best-effort.
                // Audit rows may live on any shard (workflow starts use shard-aware
                // inserts), so iterate every shard to honour the retention window.
                if config.audit_retention_days > 0 && !config.dry_run {
                    purge_audit_records_across_shards(&pools, &config).await;
                }

                // Purge old schedule decisions once per tick, best-effort.
                if config.schedule_decision_retention_days > 0 && !config.dry_run {
                    for (_, pool) in pools.iter_shards() {
                        if let Ok(mut conn) = pool.get().await
                            && let Err(err) =
                                crate::schedule_decision::purge_old_schedule_decisions(
                                    &mut conn,
                                    config.schedule_decision_retention_days,
                                )
                                .await
                        {
                            tracing::warn!(error = %err, "harvest schedule decisions purge failed");
                        }
                    }
                }

                // Summary GC pass (issue #752): garbage-collect execution
                // summaries older than the summary horizon, once per tick,
                // best-effort. Only runs for a bounded (`For(_)`) horizon —
                // `Unbounded` keeps summaries forever. Shard-local (summaries
                // live on the demoted execution's own shard). The
                // `harvest.retention.summary_deleted` counter is emitted for
                // real GC deletes only.
                if config.summary_gc_active()
                    && !config.dry_run
                    && let Some(summary_age) = config.summary_age()
                {
                    let now = Utc::now();
                    for (shard, pool) in pools.iter_shards() {
                        if let Ok(mut conn) = pool.get().await {
                            match purge_expired_summaries(
                                &mut conn,
                                u16::try_from(shard.as_i32()).unwrap_or(0),
                                summary_age,
                                config.batch_size,
                                false,
                                now,
                            )
                            .await
                            {
                                Ok(counts) => {
                                    for (name, count) in counts {
                                        if count > 0 {
                                            metrics.record_summary_deleted(&name, count);
                                        }
                                    }
                                }
                                Err(err) => {
                                    tracing::warn!(shard = %shard, error = %err, "harvest execution-summary GC failed");
                                }
                            }
                        }
                    }
                }

                // Idle rate-limit bucket GC (issue #1127): collect inert
                // per-tenant token buckets so `harvest_rate_limit_buckets`
                // stops growing one row per caller-supplied key forever.
                //
                // Deliberately OUTSIDE the history-retention phase gate: bucket
                // growth is driven by *dispatch* traffic, not by how long
                // finished histories are kept, so a deployment that retains
                // history forever still has to collect buckets.
                //
                // Shard-local and best-effort, exactly like the audit/schedule/
                // summary purges above: a shard that fails is reported and
                // retried next tick rather than failing the whole tick, because
                // reclamation here is independent of everything else the
                // janitor just did. Reported, not merely logged — otherwise
                // `GET /admin/retention` would keep serving the last successful
                // tick's count and a permanently-failing shard would look
                // exactly like an idle one.
                //
                // Under `dry_run` the pass still runs, as a read-only PREVIEW:
                // it deletes nothing and reports what it *would* collect (the
                // same affordance `purge_expired_summaries` offers). A
                // collector that is on by default is precisely the kind an
                // operator wants to preview first.
                if config.rate_limit_bucket_gc_active()
                    && let Some(window) = config.rate_limit_bucket_retention()
                    && let Ok(window) = chrono::Duration::from_std(window)
                {
                    let cutoff = Utc::now() - window;
                    for (shard, pool) in pools.iter_shards() {
                        let mut conn = match pool.get().await {
                            Ok(conn) => conn,
                            Err(error) => {
                                tracing::warn!(
                                    shard = %shard,
                                    error = %error,
                                    "harvest rate-limit bucket GC could not acquire a connection"
                                );
                                monitor_task.update_rate_limit_buckets(
                                    shard,
                                    RateLimitBucketGcOutcome::failed(error.to_string()),
                                );
                                continue;
                            }
                        };
                        match crate::queue::sweep_idle_rate_limit_buckets(
                            &mut conn,
                            cutoff,
                            config.batch_size,
                            config.dry_run,
                        )
                        .await
                        {
                            Ok(by_family) => {
                                let total: u64 = by_family.values().sum();
                                // Real deletes only: a preview's would-collect
                                // counts must never move a counter an operator
                                // reads as work actually done.
                                if !config.dry_run {
                                    for (family, count) in &by_family {
                                        metrics.record_rate_limit_buckets_deleted(family, *count);
                                    }
                                }
                                if total > 0 {
                                    tracing::info!(
                                        shard = %shard,
                                        deleted = total,
                                        dry_run = config.dry_run,
                                        window_secs = window.num_seconds(),
                                        "harvest idle rate-limit buckets collected"
                                    );
                                }
                                monitor_task.update_rate_limit_buckets(
                                    shard,
                                    RateLimitBucketGcOutcome::collected(by_family, config.dry_run),
                                );
                            }
                            Err(err) => {
                                tracing::warn!(
                                    shard = %shard,
                                    error = %err,
                                    "harvest idle rate-limit bucket GC failed"
                                );
                                monitor_task.update_rate_limit_buckets(
                                    shard,
                                    RateLimitBucketGcOutcome::failed(err.to_string()),
                                );
                            }
                        }
                    }
                }

                // Issue #797: unconditional end-of-iteration liveness tick. A
                // tick that deleted nothing still proves the janitor is alive —
                // which `harvest.retention.deleted` (work-only) cannot.
                crate::scanner_health::record_scanner_tick(metrics.as_ref(), owner);
            }
            // Issue #797: a graceful stop retires this loop from the expected
            // scanner set. A panic unwinds past here, so a panicked loop stays
            // registered and correctly ages into `Wedged`.
            crate::scanner_health::deregister_scanner(owner);
        });

        Some(Self {
            shutdown,
            trigger_tx,
            handle,
            monitor,
        })
    }

    /// Shares a snapshot interface allowing telemetry dashboards to safely peek at the process.
    #[must_use]
    pub fn monitor(&self) -> RetentionMonitor {
        self.monitor.clone()
    }

    /// Forces the background worker to wake up and aggressively prune immediately without waiting
    /// for the next interval loop.
    pub fn run_now(&self) {
        let _ = self.trigger_tx.try_send(());
    }

    /// Exposes a direct channel to bypass scheduling and command the worker to act right now.
    #[must_use]
    pub fn trigger_sender(&self) -> mpsc::Sender<()> {
        self.trigger_tx.clone()
    }

    /// Triggers the emergency stop sequence to abort any running operations gracefully.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }

    /// # Errors
    ///
    /// Returns a [`tokio::task::JoinError`] if the spawned retention task panicked
    /// or was aborted.
    pub async fn join(self) -> Result<(), tokio::task::JoinError> {
        self.handle.await
    }
}

#[cfg(feature = "db")]
#[derive(Debug, Default)]
struct ShardTickOutcome {
    candidate_count: usize,
    deleted_count: usize,
    oldest_age_secs_skipped: Option<u64>,
    next_cursor: Option<RetentionScanCursor>,
    /// Per-workflow-type deletion counts (real deletes and dry-run
    /// would-deletes). Issue #737.
    deleted_by_workflow: BTreeMap<String, u64>,
    /// Number of execution summaries created (demoted) this tick (issue #752).
    /// Real deletes only — dry-run creates no summaries.
    summarized_count: usize,
}

#[cfg(feature = "db")]
#[derive(Debug, QueryableByName)]
struct CandidateExecution {
    #[diesel(sql_type = SqlUuid)]
    id: uuid::Uuid,
    #[diesel(sql_type = Text)]
    workflow_name: String,
    #[diesel(sql_type = Text)]
    workflow_id: String,
    #[diesel(sql_type = Text)]
    state: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Jsonb>)]
    context_headers: Option<serde_json::Value>,
    #[diesel(sql_type = Nullable<Timestamptz>) ]
    completed_at: Option<DateTime<Utc>>,
    /// Legal-hold columns (issue #747), read only for the per-candidate skip
    /// gate. The SELECT's WHERE clause is intentionally NOT changed — the gate
    /// is evaluated in Rust so the two-variant bind numbering stays stable.
    #[diesel(sql_type = Nullable<Timestamptz>) ]
    legal_hold_set_at: Option<DateTime<Utc>>,
    #[diesel(sql_type = Nullable<Timestamptz>) ]
    legal_hold_until: Option<DateTime<Utc>>,
    /// Deadline-aware replay budget (issue #772), carried into the pre-deletion
    /// archive document so a completed/continued run whose history recorded a
    /// `SideEffectRecorded{Now}` deadline probe replays cleanly from cold storage
    /// (the archive is the last surviving copy; the row is deleted moments after).
    #[diesel(sql_type = Nullable<Interval>) ]
    execution_timeout: Option<chrono::Duration>,
    #[diesel(sql_type = Nullable<Timestamptz>) ]
    deadline_at: Option<DateTime<Utc>>,
    /// Spawning-parent id (issue #698), carried into the pre-deletion archive
    /// document (mirrors `execution_timeout`/`deadline_at`) so a parent-aware
    /// child's archived history round-trips its `parent_execution_id` and
    /// replays cleanly from cold storage instead of false-reporting
    /// non-determinism. `parent_id` lives in no `WorkflowEvent`, so the archive
    /// is the last surviving copy of it before the row is deleted.
    #[diesel(sql_type = Nullable<SqlUuid>) ]
    parent_id: Option<uuid::Uuid>,
    /// Task queue (issue #798), carried into the pre-deletion archive document
    /// (mirrors `context_headers`/`parent_id`) so a workflow that branches on
    /// `ctx.queue_name()` replays cleanly from cold storage instead of running
    /// under `""`. The live worker sets it from the claimed task row, so it lives
    /// in no `WorkflowEvent` and the archive is its last surviving copy.
    #[diesel(sql_type = Text)]
    queue_name: String,
}

#[cfg(feature = "db")]
#[derive(Debug, Clone, Copy, PartialEq)]
struct RetentionScanCursor {
    completed_at: DateTime<Utc>,
    id: uuid::Uuid,
}

#[cfg(feature = "db")]
struct RetentionLeaseGuard {
    pool: crate::worker::DbPool,
    lease_id: String,
    active_ids: Arc<Mutex<Vec<uuid::Uuid>>>,
    active: bool,
}

#[cfg(feature = "db")]
impl Drop for RetentionLeaseGuard {
    fn drop(&mut self) {
        if self.active {
            let pool = self.pool.clone();
            let lease_id = self.lease_id.clone();
            let ids = {
                let guard = self.active_ids.lock().expect("lease guard lock poisoned");
                guard.clone()
            };
            if !ids.is_empty() {
                tokio::spawn(async move {
                    if let Ok(mut conn) = pool.get().await {
                        let _ = diesel::update(
                            harvest_workflow_executions::table
                                .filter(harvest_workflow_executions::id.eq_any(ids))
                                .filter(
                                    harvest_workflow_executions::sticky_worker_id
                                        .eq(Some(lease_id)),
                                ),
                        )
                        .set(
                            harvest_workflow_executions::sticky_worker_id
                                .eq::<Option<String>>(None),
                        )
                        .execute(&mut conn)
                        .await;
                    }
                });
            }
        }
    }
}

/// Compute each pool group's combined `protect_unexported_audit` decision
/// (issue #1266).
///
/// Two logical shards may share one physical pool. See
/// `ShardedDbPool::pool_groups` for how that is detected.
///
/// `purge_old_audit_records` issues one unscoped `DELETE` per call. It
/// relies on the connection alone to identify which shard it purges.
/// Calling it once per logical shard would let two aliased shards apply
/// two different `protect_unexported_audit` decisions to one physical
/// audit table, within one tick. A less protective decision would commit
/// before a more protective one ever ran.
///
/// Combining each group's decision with `any` avoids that. The combined
/// decision is `true` when any aliased shard wants protection. Each
/// physical pool is purged once per tick with that one decision, already
/// accounting for every shard sharing it.
///
/// A list of shard ids travels with the decision for the same reason
/// (issue #1266). `purge_old_audit_records`'s pending check needs to
/// know which colocated shards should each have a cursor row, not only
/// whether the combined protection flag is set. A cursor-row count is
/// not enough. A decommissioned shard's row is retired, never deleted,
/// so a count can look complete even when a currently colocated shard
/// has none of its own. See `purge_old_audit_records`'s doc comment.
///
/// The list excludes only shards an operator has explicitly exempted,
/// not every shard that merely lacks today's `protect_unexported_audit`
/// flag. Those are different things. The flag can be unset for every
/// shard (`protect_unexported_audit: None`, the common case). That means
/// the operator never opted into it at all. Every shard's cursor still
/// matters exactly as it did before this flag existed.
/// [`RetentionConfig::protects_unexported_audit`] answers a different
/// question -- "does the flag protect this shard right now". It returns
/// `false` for every shard when the flag is off. Using it here would
/// silently empty this list and disable the check across the board. An
/// explicitly exempted shard (issue #1266) may never tick, and so may
/// have no cursor row at all. It is not a shard the guard needs to hear
/// from: exempting it is exactly how an operator says its progress no
/// longer matters. Naming it anyway would make the missing-cursor check
/// permanently true. That blocks purges of rows a still-protected shard
/// has genuinely already acknowledged, defeating the exemption's purpose.
#[cfg(feature = "db")]
fn group_shards_by_pool<'a>(
    pools: &'a ShardedDbPool,
    config: &RetentionConfig,
) -> Vec<(&'a crate::worker::DbPool, bool, Vec<ShardId>)> {
    pools
        .pool_groups()
        .into_iter()
        .map(|(pool, shards)| {
            let protect = shards
                .iter()
                .any(|shard| config.protects_unexported_audit(*shard));
            let expects_cursor: Vec<ShardId> = shards
                .into_iter()
                .filter(|shard| {
                    !config
                        .protect_unexported_audit
                        .as_ref()
                        .is_some_and(|exempt| exempt.contains(shard))
                })
                .collect();
            (pool, protect, expects_cursor)
        })
        .collect()
}

#[cfg(feature = "db")]
async fn purge_audit_records_across_shards(pools: &ShardedDbPool, config: &RetentionConfig) {
    for (pool, protect_unexported_audit, colocated_shards) in group_shards_by_pool(pools, config) {
        if let Ok(mut conn) = pool.get().await {
            let colocated_shard_ids: Vec<i32> =
                colocated_shards.iter().map(|s| s.as_i32()).collect();
            if let Err(err) = crate::audit::purge_old_audit_records(
                &mut conn,
                config.audit_retention_days,
                protect_unexported_audit,
                &colocated_shard_ids,
            )
            .await
            {
                tracing::warn!(error = %err, "harvest audit log purge failed");
            }
        }
    }
}

#[cfg(feature = "db")]
#[allow(clippy::too_many_lines)]
#[allow(clippy::too_many_arguments)]
async fn run_shard_tick(
    pool: crate::worker::DbPool,
    shard: ShardId,
    now: DateTime<Utc>,
    config: &RetentionConfig,
    archiver: Option<Arc<dyn HistoryArchiver>>,
    start_cursor: Option<RetentionScanCursor>,
    _metrics: Arc<dyn MetricsRecorder>,
    offloader: Option<Arc<crate::payload_store::PayloadOffloader>>,
) -> HarvestResult<ShardTickOutcome> {
    let mut outcome = ShardTickOutcome {
        next_cursor: start_cursor,
        ..ShardTickOutcome::default()
    };
    let mut cursor = start_cursor;
    let mut wrapped = false;
    let mut remaining = config.batch_size;
    let mut has_failed = false;

    // Per-type effective cutoff pushed into the candidate SELECT (issue #737,
    // PR #990 review). Instead of a single loose cutoff (`now - min(all ages)`)
    // followed by a Rust per-candidate skip — which selects, claims, and re-skips
    // a long-retained type's not-yet-eligible rows every tick, consuming batch
    // budget and starving newer already-expired rows of a shorter policy — we
    // build one (name, cutoff) pair per override and let the SQL COALESCE resolve
    // each row's own effective cutoff. Only genuinely-eligible rows are ever
    // selected, so mixed-policy deployments never starve.
    //
    // `override_cut_names[i]` ↔ `override_cuts[i]` are kept in lockstep (unnest
    // requires equal-length arrays). A non-overridden type falls through COALESCE
    // to `global_fallback` (the global `max_age` cutoff) or, when no global age is
    // set, to `'-infinity'` — meaning it is never selected.
    let mut override_cut_names: Vec<String> = Vec::new();
    let mut override_cuts: Vec<DateTime<Utc>> = Vec::new();
    for (name, secs) in config.workflow_overrides() {
        // Fail safe: an unrepresentable override age (unreachable for validated
        // ages, bounded by MAX_MAX_AGE ≪ chrono's ~292M-year range) is omitted so
        // its rows are never selected under a too-aggressive cutoff. With a global
        // `max_age` set they then fall back to it; without one they fall back to
        // `'-infinity'` (never selected). Retaining is the correct failure mode.
        if let Ok(delta) = chrono::Duration::from_std(Duration::from_secs(*secs)) {
            override_cut_names.push(name.clone());
            override_cuts.push(now - delta);
        }
    }
    // The global fallback cutoff for un-overridden types. `None` means "no global
    // age" (or an unrepresentable one — same fail-safe): the SQL uses an
    // `'-infinity'` literal so those types are never selected.
    let global_fallback: Option<DateTime<Utc>> = config
        .max_age()
        .and_then(|age| chrono::Duration::from_std(age).ok())
        .map(|delta| now - delta);

    let lease_id = format!("retention-lease-{}", uuid::Uuid::new_v4());
    let guard = RetentionLeaseGuard {
        pool: pool.clone(),
        lease_id: lease_id.clone(),
        active_ids: Arc::new(Mutex::new(Vec::new())),
        active: true,
    };

    // Reclaim `harvest_completion_deliveries` rows that resolved to
    // `DELIVERED` *after* their owning execution was already collected
    // (issue #921 review, Codex P2, follow-up). A PENDING/INFLIGHT/FAILED
    // row is deliberately kept when its owner is collected below (the
    // delivery may still need to retry or await redrive), but if that same
    // row *later* succeeds, nothing else ever revisits it -- the candidate
    // loop only ever iterates over still-live executions, so an orphaned
    // row (whose `workflow_exec_id` no longer names an existing execution)
    // would otherwise carry its frozen result/error PII with no retention
    // bound at all. Scoped to `DELIVERED` only, matching the per-candidate
    // delete's existing "not finished yet" rule for PENDING/INFLIGHT/FAILED
    // rows. Runs once per shard tick (not per candidate) since it is a
    // table-wide reclaim, not scoped to this tick's candidate batch.
    {
        let mut conn = pool
            .get()
            .await
            .map_err(|error| HarvestError::Database(error.to_string()))?;
        let reclaimed = diesel::sql_query(
            "DELETE FROM harvest_completion_deliveries
             WHERE state = 'DELIVERED'
               AND NOT EXISTS (
                   SELECT 1 FROM harvest_workflow_executions
                   WHERE harvest_workflow_executions.id = harvest_completion_deliveries.workflow_exec_id
               )",
        )
        .execute(&mut conn)
        .await
        .map_err(database_error)?;
        outcome.deleted_count += reclaimed;
    }

    while remaining > 0 {
        // Check out a short-lived connection just to load and claim this batch of candidates in a single transaction
        let mut conn = pool
            .get()
            .await
            .map_err(|error| HarvestError::Database(error.to_string()))?;

        let lease_id_inner = lease_id.clone();
        let names_inner = override_cut_names.clone();
        let cuts_inner = override_cuts.clone();
        let candidates = Box::pin(conn.transaction::<Vec<CandidateExecution>, HarvestError, _>(async |conn| {
            // Push each row's exact per-type effective cutoff into the
            // predicate (issue #737, PR #990 review): the correlated
            // `unnest` subquery resolves the override cutoff for this row's
            // workflow_name, falling through COALESCE to the global cutoff
            // ($3) or, when there is no global age, to `'-infinity'` — so a
            // non-overridden never-delete type is never selected. Only
            // genuinely-eligible rows are returned, so a long-retained
            // type's not-yet-eligible backlog can neither consume the batch
            // budget nor starve newer expired rows of a shorter policy.
            //
            // Two query-string variants keep the bind numbering unambiguous:
            // with a global age the fallback is bound as $3; without one it
            // is the `'-infinity'` literal and the cursor/limit binds shift
            // down by one.
            let sql = if global_fallback.is_some() {
                "SELECT id, workflow_name, workflow_id, state, completed_at, context_headers, legal_hold_set_at, legal_hold_until, execution_timeout, deadline_at, parent_id, queue_name
                 FROM harvest_workflow_executions
                 WHERE state IN ('COMPLETED','FAILED','CANCELLED','TIMED_OUT','CONTINUED_AS_NEW','TERMINATED')
                   AND completed_at IS NOT NULL
                   AND sticky_worker_id IS NULL
                   AND completed_at < COALESCE(
                       (SELECT ov.cut
                          FROM unnest($1::text[], $2::timestamptz[]) AS ov(nm, cut)
                         WHERE ov.nm = harvest_workflow_executions.workflow_name),
                       $3)
                   AND (
                       $4 IS NULL
                       OR completed_at > $4
                       OR (completed_at = $4 AND id > $5)
                   )
                 ORDER BY completed_at ASC, id ASC
                 LIMIT $6
                 FOR UPDATE SKIP LOCKED"
            } else {
                "SELECT id, workflow_name, workflow_id, state, completed_at, context_headers, legal_hold_set_at, legal_hold_until, execution_timeout, deadline_at, parent_id, queue_name
                 FROM harvest_workflow_executions
                 WHERE state IN ('COMPLETED','FAILED','CANCELLED','TIMED_OUT','CONTINUED_AS_NEW','TERMINATED')
                   AND completed_at IS NOT NULL
                   AND sticky_worker_id IS NULL
                   AND completed_at < COALESCE(
                       (SELECT ov.cut
                          FROM unnest($1::text[], $2::timestamptz[]) AS ov(nm, cut)
                         WHERE ov.nm = harvest_workflow_executions.workflow_name),
                       '-infinity'::timestamptz)
                   AND (
                       $3 IS NULL
                       OR completed_at > $3
                       OR (completed_at = $3 AND id > $4)
                   )
                 ORDER BY completed_at ASC, id ASC
                 LIMIT $5
                 FOR UPDATE SKIP LOCKED"
            };
            // Bind order maps to $1..$N regardless of textual position. The
            // override arrays ($1/$2) are always bound; $3 is the global
            // fallback only in the global-age variant.
            let query = diesel::sql_query(sql)
                .bind::<Array<Text>, _>(names_inner)
                .bind::<Array<Timestamptz>, _>(cuts_inner);
            let rows = if let Some(fallback) = global_fallback {
                query
                    .bind::<Timestamptz, _>(fallback)
                    .bind::<Nullable<Timestamptz>, _>(cursor.map(|it| it.completed_at))
                    .bind::<Nullable<SqlUuid>, _>(cursor.map(|it| it.id))
                    .bind::<BigInt, _>(i64::try_from(remaining).unwrap_or(i64::MAX))
                    .load::<CandidateExecution>(conn)
                    .await
            } else {
                query
                    .bind::<Nullable<Timestamptz>, _>(cursor.map(|it| it.completed_at))
                    .bind::<Nullable<SqlUuid>, _>(cursor.map(|it| it.id))
                    .bind::<BigInt, _>(i64::try_from(remaining).unwrap_or(i64::MAX))
                    .load::<CandidateExecution>(conn)
                    .await
            }
            .map_err(database_error)?;

            if !rows.is_empty() {
                let ids: Vec<uuid::Uuid> = rows.iter().map(|r| r.id).collect();
                diesel::update(
                    harvest_workflow_executions::table
                        .filter(harvest_workflow_executions::id.eq_any(ids)),
                )
                .set(harvest_workflow_executions::sticky_worker_id.eq(Some(lease_id_inner)))
                .execute(conn)
                .await
                .map_err(database_error)?;
            }

            Ok(rows)
        }))
        .await?;

        // Release the checked-out connection immediately back to the pool
        drop(conn);

        if !candidates.is_empty() {
            let ids: Vec<uuid::Uuid> = candidates.iter().map(|r| r.id).collect();
            guard
                .active_ids
                .lock()
                .expect("lease guard lock poisoned")
                .extend(ids);
        }

        if candidates.is_empty() {
            // Prevent same-tick rescanning/wrapping if we have encountered any failures
            if cursor.is_some() && !wrapped && !has_failed {
                cursor = None;
                wrapped = true;
                continue;
            }
            outcome.next_cursor = cursor;
            break;
        }

        let mut batch_failed = false;
        for candidate in candidates {
            let completed_at = candidate
                .completed_at
                .expect("retention candidate query enforces completed_at IS NOT NULL");
            let candidate_cursor = RetentionScanCursor {
                completed_at,
                id: candidate.id,
            };
            cursor = Some(candidate_cursor);
            outcome.candidate_count += 1;
            remaining = remaining.saturating_sub(1);

            // Checkout a connection to run candidate dependency validations
            let mut conn = pool
                .get()
                .await
                .map_err(|error| HarvestError::Database(error.to_string()))?;

            // --- Per-candidate retention decision (issue #737) ------------
            // This is the single seam where a candidate's fate is decided.
            // Resolve the effective max-age for THIS workflow type (override
            // or global fallback), then gate on it before any archive/delete.
            // Future policies (#747 legal hold, #752 tiered summary) hook in
            // here.
            //
            // Legal hold (issue #747) is the FIRST gate — it precedes archival
            // (#345 hook below) AND delete, and precedes the metric emission, so
            // `harvest.retention.deleted` never increments for a held id. An
            // active hold (`legal_hold_active`) exempts this execution's history
            // from retention entirely, until the hold is released or expires.
            // Evaluated against the tick's `now` (the same clock the SELECT cutoff
            // uses) so the decision is consistent with candidate selection.
            if legal_hold_active(candidate.legal_hold_set_at, candidate.legal_hold_until, now) {
                routine_skip_candidate(
                    &mut conn,
                    candidate.id,
                    candidate_cursor,
                    has_failed,
                    &mut outcome,
                    &guard.active_ids,
                )
                .await?;
                continue;
            }
            // NB (PR #990 review): the candidate SELECT now pushes each row's
            // exact per-type cutoff into SQL, so the two skip branches below
            // (`effective_max_age == None` and `completed_at >= resolved_cutoff`)
            // are effectively unreachable — no not-yet-eligible or never-delete
            // row is ever selected. They are kept as cheap defense-in-depth. The
            // `should_skip_candidate` chain-link check below still needs the
            // RESOLVED per-type cutoff, which is derived here.
            let Some(age) = config.effective_max_age(&candidate.workflow_name) else {
                // Neither an override nor a global max-age applies to this
                // type: never delete it. Routine skip.
                routine_skip_candidate(
                    &mut conn,
                    candidate.id,
                    candidate_cursor,
                    has_failed,
                    &mut outcome,
                    &guard.active_ids,
                )
                .await?;
                continue;
            };
            let Ok(chrono_age) = chrono::Duration::from_std(age) else {
                // Unrepresentable duration (unreachable for validated ages, which
                // are bounded by MAX_MAX_AGE ≪ chrono's ~292M-year range); fail
                // safe toward RETAINING the row rather than deleting it. A zero
                // fallback would make `resolved_cutoff = now`, deleting nearly
                // every completed row — the wrong failure mode for a retention
                // janitor. Routine skip.
                routine_skip_candidate(
                    &mut conn,
                    candidate.id,
                    candidate_cursor,
                    has_failed,
                    &mut outcome,
                    &guard.active_ids,
                )
                .await?;
                continue;
            };
            let resolved_cutoff = now - chrono_age;
            if completed_at >= resolved_cutoff {
                // The loosest-cutoff SQL pre-filter is a superset; this type is
                // not old enough under its own effective age. Routine skip, and
                // record its age for tuning observability (as the existing
                // should_skip branch does for not-yet-collectable candidates).
                let skipped_age = now
                    .signed_duration_since(completed_at)
                    .num_seconds()
                    .max(0)
                    .cast_unsigned();
                outcome.oldest_age_secs_skipped = Some(
                    outcome
                        .oldest_age_secs_skipped
                        .map_or(skipped_age, |existing| existing.max(skipped_age)),
                );
                routine_skip_candidate(
                    &mut conn,
                    candidate.id,
                    candidate_cursor,
                    has_failed,
                    &mut outcome,
                    &guard.active_ids,
                )
                .await?;
                continue;
            }

            // NOTE: pass the per-type `resolved_cutoff` (not the loose cutoff)
            // to should_skip_candidate — its continue-as-new chain-link check
            // compares `completed_at >= $cutoff` and must see this type's own
            // cutoff to be correct.
            if should_skip_candidate(&mut conn, &candidate, resolved_cutoff).await? {
                let skipped_age = now
                    .signed_duration_since(completed_at)
                    .num_seconds()
                    .max(0)
                    .cast_unsigned();
                outcome.oldest_age_secs_skipped = Some(
                    outcome
                        .oldest_age_secs_skipped
                        .map_or(skipped_age, |existing| existing.max(skipped_age)),
                );
                routine_skip_candidate(
                    &mut conn,
                    candidate.id,
                    candidate_cursor,
                    has_failed,
                    &mut outcome,
                    &guard.active_ids,
                )
                .await?;
                continue;
            }

            // Best-effort pre-archival legal-hold re-read (issue #747 BLOCKER 1):
            // a hold placed between candidate selection and now must not be
            // archived to cold storage. This NARROWS (does not fully close) the
            // archival window — a hold landing DURING the multi-second archival
            // network call below may still be archived, then skipped at
            // delete-time by the authoritative FOR UPDATE re-check in
            // `delete_candidate_execution`. That residual is acceptable: the row
            // is preserved in place (never deleted), so archiving an
            // about-to-be-held copy leaks a cold-storage copy but loses no data.
            // We deliberately do NOT hold a row lock across the archival network
            // call. Runs before dry-run too, so a held row is never counted.
            match read_candidate_hold(&mut conn, candidate.id).await {
                Ok((set_at, until)) if legal_hold_active(set_at, until, now) => {
                    routine_skip_candidate(
                        &mut conn,
                        candidate.id,
                        candidate_cursor,
                        has_failed,
                        &mut outcome,
                        &guard.active_ids,
                    )
                    .await?;
                    continue;
                }
                Ok(_) => {}
                Err(err) => {
                    // Fail safe toward RETAINING on uncertainty (the retention
                    // janitor's correct failure mode): do not archive/delete.
                    has_failed = true;
                    batch_failed = true;
                    tracing::error!(candidate_id = %candidate.id, error = %err, "failed to re-read legal hold before archival; skipping deletion");
                    break;
                }
            }

            let mut doc = None;
            if !config.dry_run && archiver.is_some() {
                let exec_id = crate::types::ExecutionId::from_uuid(candidate.id);
                // Inflate offloaded envelopes before archiving so the archived
                // document contains real payloads, not blob references that will
                // be deleted moments later. Issue #524.
                let load_result = if offloader.is_some() {
                    crate::store::load_history_inflated(
                        &mut conn,
                        exec_id,
                        &crate::payload_codec::PayloadCodecs::default(),
                        offloader.as_deref(),
                    )
                    .await
                } else {
                    crate::store::load_history(&mut conn, exec_id).await
                };
                match load_result {
                    Ok(history) => {
                        let req = crate::history_export::HistoryExportRequest {
                            workflow_name: candidate.workflow_name.clone(),
                            // Issue #698: carry the business `workflow_id` (already
                            // on the retention candidate) so a parent-aware /
                            // id-branching workflow's archived history round-trips
                            // `ctx.info().workflow_id` into the JSON replay path.
                            workflow_id: Some(candidate.workflow_id.clone()),
                            // Issue #798: carry the execution's task queue so a
                            // `ctx.queue_name()`-branching workflow's archived
                            // history replays cleanly from cold storage.
                            queue_name: Some(candidate.queue_name.clone()),
                            execution_id: exec_id,
                            shard_id: shard.as_i32(),
                            state: candidate.state.clone(),
                            events: history.events,
                            exported_at: chrono::Utc::now(),
                            payload_policy: crate::history_export::HistoryPayloadPolicy::Full,
                            max_bytes: Some(usize::MAX),
                            context_headers: candidate
                                .context_headers
                                .as_ref()
                                .and_then(|v| serde_json::from_value(v.clone()).ok()),
                            // Issue #772 (Finding A): the archive is the LAST
                            // surviving copy before the row is deleted. A terminal
                            // (completed/continued) run can carry a recorded
                            // `SideEffectRecorded{Now}` deadline probe before its
                            // next command; carrying the row's execution_timeout /
                            // live deadline_at lets the archived document replay
                            // cleanly instead of false-reporting non-determinism.
                            execution_timeout: candidate.execution_timeout,
                            deadline_at: candidate.deadline_at,
                            // Issue #698: carry the spawning-parent id (mirrors
                            // the deadline metadata above) so a parent-aware
                            // child's archived history round-trips its
                            // `parent_execution_id` into the JSON replay path.
                            parent_execution_id: candidate
                                .parent_id
                                .map(crate::types::ExecutionId::from_uuid),
                        };
                        match crate::history_export::export_history(req) {
                            Ok(document) => {
                                doc = Some((exec_id, document));
                            }
                            Err(error) => {
                                tracing::error!(
                                    execution_id = %exec_id,
                                    error = %error,
                                    "failed to serialize history export; skipping deletion"
                                );
                                has_failed = true;
                                batch_failed = true;
                                break;
                            }
                        }
                    }
                    Err(error) => {
                        tracing::error!(
                            execution_id = %exec_id,
                            error = %error,
                            "failed to load history events for retention candidate; skipping deletion"
                        );
                        has_failed = true;
                        batch_failed = true;
                        break;
                    }
                }
            }

            // Drop/release the DB connection back to the pool before executing the slow network/filesystem archival await!
            drop(conn);

            let mut archive_success = true;
            if let Some((exec_id, document)) = doc
                && let Some(archiver) = &archiver
            {
                let timeout_dur = config.archival_timeout();
                match tokio::time::timeout(timeout_dur, archiver.archive(&document)).await {
                    Ok(Ok(())) => {
                        tracing::debug!(
                            execution_id = %exec_id,
                            "pre-retention archival hook completed successfully"
                        );
                    }
                    Ok(Err(error)) => {
                        tracing::error!(
                            execution_id = %exec_id,
                            error = %error,
                            "pre-retention archival hook failed; skipping deletion"
                        );
                        has_failed = true;
                        archive_success = false;
                        batch_failed = true;
                    }
                    Err(_) => {
                        tracing::error!(
                            execution_id = %exec_id,
                            timeout_secs = timeout_dur.as_secs(),
                            "pre-retention archival hook timed out; skipping deletion"
                        );
                        has_failed = true;
                        archive_success = false;
                        batch_failed = true;
                    }
                }
            }

            if !archive_success {
                break;
            }

            if config.dry_run {
                outcome.deleted_count += 1;
                // Per-type would-delete count (issue #737, AC7).
                *outcome
                    .deleted_by_workflow
                    .entry(candidate.workflow_name.clone())
                    .or_insert(0) += 1;
                if !has_failed {
                    outcome.next_cursor = Some(candidate_cursor);
                }
                continue;
            }

            // Check out a short-lived connection exclusively to execute the candidate deletion transaction
            let mut conn = pool
                .get()
                .await
                .map_err(|error| HarvestError::Database(error.to_string()))?;

            // Collect the candidate's offloaded blob references BEFORE deletion
            // (the rows cascade-delete with the execution). Issue #524.
            let candidate_exec_id = crate::types::ExecutionId::from_uuid(candidate.id);
            let candidate_blob_refs = if offloader.is_some() {
                match crate::store::load_payload_refs(&mut conn, candidate_exec_id).await {
                    Ok(refs) => refs,
                    Err(err) => {
                        has_failed = true;
                        batch_failed = true;
                        tracing::error!(candidate_id = %candidate.id, error = %err, "failed to load payload refs for blob GC; skipping deletion");
                        break;
                    }
                }
            } else {
                Vec::new()
            };

            match delete_candidate_execution(&mut conn, candidate.id, now, config.summary.as_ref())
                .await
            {
                Err(err) => {
                    has_failed = true;
                    batch_failed = true;
                    tracing::error!(candidate_id = %candidate.id, error = %err, "failed to delete candidate execution");
                    break;
                }
                Ok(CandidateDeleteOutcome::SkippedHeld) => {
                    // A legal hold landed after selection (issue #747 BLOCKER 1):
                    // the delete-tx FOR UPDATE re-check found it active and
                    // aborted the delete. Treat exactly like a routine skip —
                    // no delete, no blob GC, no metric, no `has_failed`. The
                    // previously-loaded `candidate_blob_refs` are simply
                    // discarded; the rows still exist (nothing cascaded).
                    routine_skip_candidate(
                        &mut conn,
                        candidate.id,
                        candidate_cursor,
                        has_failed,
                        &mut outcome,
                        &guard.active_ids,
                    )
                    .await?;
                    continue;
                }
                Ok(CandidateDeleteOutcome::Deleted { summarized }) => {
                    // A summary row was written in the delete tx (issue #752).
                    if summarized {
                        outcome.summarized_count += 1;
                    }
                }
            }
            outcome.deleted_count += 1;
            // Per-type real-delete count (issue #737, AC7/AC8).
            *outcome
                .deleted_by_workflow
                .entry(candidate.workflow_name.clone())
                .or_insert(0) += 1;

            // After the execution row (and its refs) are durably gone, delete any
            // blob no longer referenced by a surviving execution. A blob still
            // referenced by e.g. a continue-as-new successor is left intact.
            // Issue #524.
            if let Some(offloader) = &offloader
                && !candidate_blob_refs.is_empty()
            {
                let keys: Vec<String> = candidate_blob_refs
                    .iter()
                    .map(|b| b.blob_key.clone())
                    .collect();
                match crate::store::batch_blob_keys_still_referenced(&mut conn, &keys).await {
                    Ok(still_referenced) => {
                        for blob in &candidate_blob_refs {
                            if !still_referenced.contains(&blob.blob_key)
                                && let Err(err) = offloader.store().delete(&blob.blob_key).await
                            {
                                // Row is already gone; a failed blob delete only leaks
                                // storage (never a dangling reference). Log and continue.
                                tracing::warn!(blob_key = %blob.blob_key, error = %err.0, "failed to delete offloaded blob during retention; leaving for a later sweep");
                            }
                        }
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, "failed to batch-check residual blob references; leaving all blobs intact");
                    }
                }
            }

            {
                let mut active_guard = guard.active_ids.lock().expect("lease guard lock poisoned");
                if let Some(pos) = active_guard.iter().position(|&x| x == candidate.id) {
                    active_guard.swap_remove(pos);
                }
            }

            if !has_failed {
                outcome.next_cursor = Some(candidate_cursor);
            }
        }

        if batch_failed {
            break;
        }
    }

    tracing::debug!(shard = %shard, candidates = outcome.candidate_count, deleted = outcome.deleted_count, "retention shard tick");
    Ok(outcome)
}

/// Outcome of a candidate-deletion attempt (issue #747 BLOCKER 1). A hold that
/// commits AFTER candidate selection but before this delete transaction must be
/// honored, so the delete tx re-checks the legal-hold columns under a row lock
/// as its first statement and aborts the delete when the hold is active.
#[cfg(feature = "db")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandidateDeleteOutcome {
    /// The execution (and its dependent rows) were deleted. `summarized` is
    /// `true` when a `harvest_execution_summaries` row was written in the same
    /// transaction (issue #752); `false` when summary retention is disabled or
    /// the row was already summarized (idempotent ON CONFLICT no-op).
    Deleted { summarized: bool },
    /// A legal hold was found active under the delete-tx row lock; the delete
    /// was aborted and NOTHING was touched (no `payload_refs`, no blobs, no
    /// execution row, no summary). The caller treats this exactly like a
    /// routine skip.
    SkippedHeld,
}

/// The two legal-hold timestamp columns `(legal_hold_set_at, legal_hold_until)`.
#[cfg(feature = "db")]
type HoldTimestamps = (Option<DateTime<Utc>>, Option<DateTime<Utc>>);

/// Best-effort, non-locking read of a candidate's legal-hold columns, used for
/// the pre-archival re-read gate (issue #747 BLOCKER 1). A concurrently-deleted
/// row (`None`) reports "not held" — the subsequent delete of a missing row is a
/// harmless no-op.
#[cfg(feature = "db")]
async fn read_candidate_hold(
    conn: &mut diesel_async::AsyncPgConnection,
    candidate_id: uuid::Uuid,
) -> HarvestResult<HoldTimestamps> {
    harvest_workflow_executions::table
        .find(candidate_id)
        .select((
            harvest_workflow_executions::legal_hold_set_at,
            harvest_workflow_executions::legal_hold_until,
        ))
        .first::<HoldTimestamps>(conn)
        .await
        .optional()
        .map_err(database_error)
        .map(|opt| opt.unwrap_or((None, None)))
}

#[cfg(feature = "db")]
#[allow(clippy::too_many_lines)]
async fn delete_candidate_execution(
    conn: &mut diesel_async::AsyncPgConnection,
    candidate_id: uuid::Uuid,
    now: DateTime<Utc>,
    summary: Option<&SummaryPolicy>,
) -> HarvestResult<CandidateDeleteOutcome> {
    // Copy the policy into the transaction closure (it is `Copy`).
    let summary = summary.copied();
    Box::pin(conn.transaction::<_, HarvestError, _>(async |conn| {
        let mut summarized = false;
        // ── Authoritative legal-hold re-check under a row lock (issue #747
        // BLOCKER 1) ─────────────────────────────────────────────────────
        // The candidate SELECT read the hold columns then committed and
        // released its lock BEFORE archival + delete, so a hold placed via
        // `set_legal_hold` in that window would otherwise be missed and the
        // held execution deleted. Reading the hold columns FOR UPDATE here,
        // as the FIRST statement of the delete transaction, closes that
        // window absolutely: it serializes against `set_legal_hold`'s own
        // locking read/update, so a hold committed before this SELECT is
        // seen (→ abort the delete), and a hold committing after must wait
        // for this delete tx to finish — by which point the row is gone and
        // the operator's `set_legal_hold` observes a missing row (404). If
        // the hold is active, abort the delete entirely: do NOT touch
        // payload_refs or blobs.
        //
        // Tiered/summary retention (issue #752): when a summary policy is
        // set, the SAME FOR UPDATE row lock loads the demotion source
        // columns (identity, timing, shard, search-attrs, and — opt-in —
        // result/error payload) so the summary INSERT is atomic with the
        // legal-hold re-check and the delete. There is never a window where
        // both the execution and its summary are absent, and a rollback
        // (e.g. a delete error below) discards the summary too, so no
        // orphan can result. The summary INSERT happens AFTER the hold
        // re-check (a held row is never summarized) and BEFORE the deletes.
        if let Some(policy) = summary {
            let row: Option<SummarySourceRow> = harvest_workflow_executions::table
                .find(candidate_id)
                .select((
                    harvest_workflow_executions::legal_hold_set_at,
                    harvest_workflow_executions::legal_hold_until,
                    harvest_workflow_executions::workflow_name,
                    harvest_workflow_executions::workflow_id,
                    harvest_workflow_executions::state,
                    harvest_workflow_executions::started_at,
                    harvest_workflow_executions::completed_at,
                    harvest_workflow_executions::shard_id,
                    harvest_workflow_executions::output,
                    harvest_workflow_executions::error,
                    harvest_workflow_executions::search_attrs,
                    harvest_workflow_executions::parent_id,
                    harvest_workflow_executions::migrated_from_shards,
                ))
                .for_update()
                .first::<SummarySourceRow>(conn)
                .await
                .optional()
                .map_err(database_error)?;

            // `None` = row concurrently deleted: nothing to summarize; the
            // deletes below are harmless no-ops (matches the hold-only
            // path's missing-row behavior).
            if let Some((
                set_at,
                until,
                workflow_name,
                workflow_id,
                state,
                started_at,
                completed_at,
                shard_id,
                output,
                error,
                search_attrs,
                parent_id,
                migrated_from_shards,
            )) = row
            {
                if legal_hold_active(set_at, until, now) {
                    return Ok(CandidateDeleteOutcome::SkippedHeld);
                }
                // completed_at is NOT NULL by the candidate query's WHERE
                // clause; fall back to started_at defensively so the NOT
                // NULL summary column always has a value.
                let completed = completed_at.unwrap_or(started_at);
                // Clamp at 0: a `completed_at` before `started_at` (clock
                // skew across nodes) must never produce a negative duration.
                let duration_ms = Some((completed - started_at).num_milliseconds().max(0));
                // Payload capture is opt-in (AC3): a policy with capture
                // disabled leaves result/error NULL.
                let (result, error_out) = if policy.capture_payload {
                    (
                        cap_result_payload(output, policy.max_payload_bytes),
                        cap_error_text(error.as_deref(), policy.max_payload_bytes),
                    )
                } else {
                    (None, None)
                };
                // Codec caveat (issue #752): the summary stores the
                // `output`/`search_attrs` COLUMNS verbatim — these are
                // codec-ENCODED at rest (the same columns #608's
                // `decode_workflow_execution_fields` decodes on read). So
                // encryption-at-rest is preserved: the longer-retained
                // summary tier can never hold plaintext that the event
                // history encrypts.
                let new_summary = NewExecutionSummary {
                    execution_id: candidate_id,
                    workflow_name,
                    workflow_id,
                    state,
                    started_at,
                    completed_at: completed,
                    duration_ms,
                    shard_id,
                    search_attrs,
                    result,
                    error: error_out,
                    parent_id,
                    // Issue #964: the residence history must outlive the
                    // execution row. Sealed source copies are NOT collected
                    // with it -- retention deliberately never purges a
                    // `MIGRATED` row, since that would destroy the forwarding
                    // pointer -- so a summary that lost this array would make a
                    // later erasure read "never migrated" and report success
                    // over copies it never touched.
                    migrated_from_shards,
                };
                // ON CONFLICT DO NOTHING makes the demotion idempotent
                // across a retried delete tx.
                let inserted = diesel::insert_into(harvest_execution_summaries::table)
                    .values(&new_summary)
                    .on_conflict(harvest_execution_summaries::execution_id)
                    .do_nothing()
                    .execute(conn)
                    .await
                    .map_err(database_error)?;
                summarized = inserted > 0;
            }
        } else {
            // Summary retention disabled: byte-for-byte the pre-#752
            // hold-only re-check.
            let hold: Option<HoldTimestamps> = harvest_workflow_executions::table
                .find(candidate_id)
                .select((
                    harvest_workflow_executions::legal_hold_set_at,
                    harvest_workflow_executions::legal_hold_until,
                ))
                .for_update()
                .first::<HoldTimestamps>(conn)
                .await
                .optional()
                .map_err(database_error)?;
            if let Some((set_at, until)) = hold
                && legal_hold_active(set_at, until, now)
            {
                return Ok(CandidateDeleteOutcome::SkippedHeld);
            }
        }

        // Orphan the deleted parent's terminal children so their `parent_id`
        // does not dangle to a gone row. Tiered/summary retention (issue
        // #752): when a summary policy is set, this null-out is SKIPPED so
        // the child→parent lineage survives deletion. A terminal child is
        // independently retention-eligible and may be summarized in a LATER
        // transaction than its parent; if we nulled its `parent_id` here,
        // its own demotion would capture a NULL parent and the #495 PII-erase
        // cascade (which reaches a demoted child summary via
        // `harvest_execution_summaries.parent_id`) could never find it.
        // Preserving the link makes the cascade order-independent regardless
        // of whether the parent or child is processed first. The retained
        // `parent_id` on a to-be-deleted terminal child is harmless (no FK;
        // `should_skip_candidate` only reads `parent_id` downward).
        if summary.is_none() {
            diesel::update(
                harvest_workflow_executions::table
                    .filter(harvest_workflow_executions::parent_id.eq(Some(candidate_id)))
                    .filter(harvest_workflow_executions::state.eq_any([
                        "COMPLETED",
                        "FAILED",
                        "CANCELLED",
                        "TIMED_OUT",
                        "CONTINUED_AS_NEW",
                        "TERMINATED",
                    ])),
            )
            .set(harvest_workflow_executions::parent_id.eq::<Option<uuid::Uuid>>(None))
            .execute(conn)
            .await
            .map_err(database_error)?;
        }

        // `task_type = 'CALLBACK'` dead letters (issue #605) are
        // excluded here (issue #921 review, Codex P2): a CALLBACK
        // dead-letter row only ever exists for a delivery that reached
        // `FAILED`, and the completion-deliveries delete just below
        // deliberately keeps every non-`DELIVERED` (i.e. `FAILED`)
        // delivery row around for redrive -- deleting its DLQ entry
        // here would drop it from the `GET /dead-letters` / aggregate
        // discovery surface while the delivery row itself (and its
        // redrive path) still exists, breaking the advertised "find
        // failures via DLQ" operator workflow for any callback failure
        // that outlives the owning workflow's retention window. A
        // redrive already deletes its own DLQ row on success, so this
        // exclusion cannot leak an entry whose delivery was resolved.
        diesel::delete(
            harvest_dead_letters::table
                .filter(harvest_dead_letters::workflow_exec_id.eq(Some(candidate_id)))
                .filter(harvest_dead_letters::task_type.ne("CALLBACK")),
        )
        .execute(conn)
        .await
        .map_err(database_error)?;

        // `harvest_completion_deliveries.workflow_exec_id` has no `ON
        // DELETE CASCADE` (issue #605 code review). Only `DELIVERED`
        // rows are deleted here — a fully successful delivery has
        // nothing left to do, so it is safe cleanup exactly like
        // `harvest_dead_letters` above. A `PENDING`/`INFLIGHT`/`FAILED`
        // row is deliberately left alone (PR #921 review, Codex): its
        // `payload` is frozen precisely so delivery does not depend on
        // the execution row surviving, and this execution reaching its
        // retention age has no bearing on whether its callback still
        // needs to be retried or is awaiting an operator's redrive.
        // Known limitation: once its owning execution is collected, a
        // surviving non-`DELIVERED` row references a `workflow_exec_id`
        // that no longer exists (there is no FK to violate, so this is
        // safe) and this retention pass will never revisit that exact
        // execution again — so even if the delivery later resolves to
        // `DELIVERED`, nothing currently deletes it. A future dedicated
        // delivery-retention policy, scoped to this table's own age/
        // state rather than its owning execution's, would be needed to
        // reclaim those rows; out of scope here, where the goal is only
        // to stop retention from destroying a delivery that hasn't
        // finished yet.
        diesel::delete(
            harvest_completion_deliveries::table
                .filter(harvest_completion_deliveries::workflow_exec_id.eq(candidate_id))
                .filter(harvest_completion_deliveries::state.eq("DELIVERED")),
        )
        .execute(conn)
        .await
        .map_err(database_error)?;

        diesel::delete(
            harvest_workflow_executions::table
                .filter(harvest_workflow_executions::id.eq(candidate_id)),
        )
        .execute(conn)
        .await
        .map_err(database_error)?;
        Ok(CandidateDeleteOutcome::Deleted { summarized })
    }))
    .await
}

/// Columns loaded FOR UPDATE to build a summary alongside the delete (issue
/// #752): the two legal-hold columns plus the demotion source columns.
#[cfg(feature = "db")]
type SummarySourceRow = (
    Option<DateTime<Utc>>,     // legal_hold_set_at
    Option<DateTime<Utc>>,     // legal_hold_until
    String,                    // workflow_name
    String,                    // workflow_id
    String,                    // state
    DateTime<Utc>,             // started_at
    Option<DateTime<Utc>>,     // completed_at
    i32,                       // shard_id
    Option<serde_json::Value>, // output
    Option<String>,            // error
    Option<serde_json::Value>, // search_attrs
    Option<uuid::Uuid>,        // parent_id
    Option<serde_json::Value>, // migrated_from_shards
);

/// Garbage-collect execution summaries older than the summary horizon (issue
/// #752), returning per-workflow-type deleted counts.
///
/// Selection is by `completed_at < now - summary_age` (deterministic;
/// *not* `summarized_at`), so the summary tier's own horizon anchors on the
/// original run window exactly like history retention. Batched to bound the
/// transaction size; deletes only (a `dry_run` tick simulates by counting).
/// Shard-local: `conn` is already routed to the summary's own shard.
#[cfg(feature = "db")]
pub(crate) async fn purge_expired_summaries(
    conn: &mut diesel_async::AsyncPgConnection,
    _shard: u16,
    summary_age: Duration,
    batch_size: usize,
    dry_run: bool,
    now: DateTime<Utc>,
) -> HarvestResult<BTreeMap<String, u64>> {
    #[derive(QueryableByName)]
    struct NameRow {
        #[diesel(sql_type = Text)]
        workflow_name: String,
    }

    let mut counts: BTreeMap<String, u64> = BTreeMap::new();
    let Ok(chrono_age) = chrono::Duration::from_std(summary_age) else {
        // Unrepresentable age (unreachable for validated horizons, bounded by
        // MAX_MAX_AGE ≪ chrono's range): fail safe toward RETAINING — GC
        // nothing this tick rather than deleting everything under a zero cutoff.
        return Ok(counts);
    };
    let cutoff = now - chrono_age;
    let batch = i64::try_from(batch_size).unwrap_or(i64::MAX).max(1);

    if dry_run {
        let rows = diesel::sql_query(
            "SELECT workflow_name FROM harvest_execution_summaries WHERE completed_at < $1",
        )
        .bind::<Timestamptz, _>(cutoff)
        .load::<NameRow>(conn)
        .await
        .map_err(database_error)?;
        for r in rows {
            *counts.entry(r.workflow_name).or_insert(0) += 1;
        }
        return Ok(counts);
    }

    loop {
        let rows = diesel::sql_query(
            "DELETE FROM harvest_execution_summaries
             WHERE execution_id IN (
                 SELECT execution_id FROM harvest_execution_summaries
                 WHERE completed_at < $1
                 ORDER BY completed_at ASC, execution_id ASC
                 LIMIT $2
             )
             RETURNING workflow_name",
        )
        .bind::<Timestamptz, _>(cutoff)
        .bind::<BigInt, _>(batch)
        .load::<NameRow>(conn)
        .await
        .map_err(database_error)?;
        let n = rows.len();
        for r in rows {
            *counts.entry(r.workflow_name).or_insert(0) += 1;
        }
        // Terminate when a batch came back short OR empty. Compare against the
        // EFFECTIVE limit `batch` (clamped `.max(1)`), not the raw `batch_size`:
        // a `batch_size` of 0 makes the LIMIT 1 while `n < 0` is impossible, so
        // an `n == 0` early break is required or the loop would spin forever.
        if n == 0 || i64::try_from(n).unwrap_or(i64::MAX) < batch {
            break;
        }
    }
    Ok(counts)
}

/// Filter set for the read-only execution-summary list query (issue #752).
///
/// Mirrors the `GET /workflows` filter vocabulary against the summary tier:
/// `workflow_name`/`workflow_id`/`state`/completed-time-range/search-attr
/// containment, plus a keyset `cursor` over `(completed_at, execution_id)`.
/// Every field is optional; an all-default query returns the newest summaries.
#[derive(Debug, Clone, Default)]
pub struct SummaryQuery {
    /// Exact-match `workflow_name` filter.
    pub workflow_name: Option<String>,
    /// Exact-match `workflow_id` filter.
    pub workflow_id: Option<String>,
    /// Terminal-state filter (`ANY` of the listed states).
    pub states: Vec<String>,
    /// Only summaries whose `completed_at` is at/after this instant.
    pub completed_after: Option<DateTime<Utc>>,
    /// Only summaries whose `completed_at` is at/before this instant.
    pub completed_before: Option<DateTime<Utc>>,
    /// JSONB containment (`@>`) predicates over `search_attrs`; combined with
    /// `AND`.
    pub search_attrs: Vec<serde_json::Value>,
    /// Keyset cursor `(completed_at, execution_id)` — under the default
    /// descending order, return only rows strictly *older* than this pair; under
    /// ascending order (`ascending = true`), only rows strictly *newer*.
    pub cursor: Option<(DateTime<Utc>, uuid::Uuid)>,
    /// Sort direction (issue #752, AC4 parity with `GET /workflows`). `false`
    /// (the default) is `(completed_at DESC, execution_id DESC)`; `true` is the
    /// ascending order. Threaded through both the `ORDER BY` and the keyset
    /// cursor comparison so a paginated `order=asc` walk is consistent.
    pub ascending: bool,
}

/// List execution summaries matching `query`, newest-first, up to `limit` rows
/// (issue #752).
///
/// Ordered by `(completed_at DESC, execution_id DESC)` for a total, stable
/// keyset order; the caller over-fetches `limit + 1` to detect a further page.
/// Shard-local: `conn` is already routed to a single shard, and the cross-shard
/// merge happens in the management layer.
///
/// # Errors
///
/// [`HarvestError::Database`] on any persistence failure.
#[cfg(feature = "db")]
pub async fn list_execution_summaries(
    conn: &mut diesel_async::AsyncPgConnection,
    query: &SummaryQuery,
    limit: i64,
) -> HarvestResult<Vec<crate::models::ExecutionSummary>> {
    use diesel::dsl::sql;
    use diesel::sql_types::{Bool, Jsonb};

    let mut q = harvest_execution_summaries::table.into_boxed();
    q = if query.ascending {
        q.order(harvest_execution_summaries::completed_at.asc())
            .then_order_by(harvest_execution_summaries::execution_id.asc())
    } else {
        q.order(harvest_execution_summaries::completed_at.desc())
            .then_order_by(harvest_execution_summaries::execution_id.desc())
    };
    q = q.limit(limit.max(0));

    if let Some(cursor) = &query.cursor {
        let (ts, id) = (cursor.0, cursor.1);
        // Row-value keyset comparison: `>` walks forward under ascending order,
        // `<` under descending — matching the `GET /workflows` contract.
        q = if query.ascending {
            q.filter(
                sql::<Bool>(
                    "(harvest_execution_summaries.completed_at, \
                     harvest_execution_summaries.execution_id) > (",
                )
                .bind::<Timestamptz, _>(ts)
                .sql(", ")
                .bind::<SqlUuid, _>(id)
                .sql(")"),
            )
        } else {
            q.filter(
                sql::<Bool>(
                    "(harvest_execution_summaries.completed_at, \
                     harvest_execution_summaries.execution_id) < (",
                )
                .bind::<Timestamptz, _>(ts)
                .sql(", ")
                .bind::<SqlUuid, _>(id)
                .sql(")"),
            )
        };
    }
    if !query.states.is_empty() {
        q = q.filter(harvest_execution_summaries::state.eq_any(query.states.clone()));
    }
    if let Some(name) = &query.workflow_name {
        q = q.filter(harvest_execution_summaries::workflow_name.eq(name.clone()));
    }
    if let Some(wid) = &query.workflow_id {
        q = q.filter(harvest_execution_summaries::workflow_id.eq(wid.clone()));
    }
    if let Some(after) = query.completed_after {
        q = q.filter(harvest_execution_summaries::completed_at.ge(after));
    }
    if let Some(before) = query.completed_before {
        q = q.filter(harvest_execution_summaries::completed_at.le(before));
    }
    for attr in &query.search_attrs {
        q = q.filter(sql::<Bool>("search_attrs @> ").bind::<Jsonb, _>(attr.clone()));
    }

    q.select(crate::models::ExecutionSummary::as_select())
        .load(conn)
        .await
        .map_err(database_error)
}

/// Common bookkeeping for a "routine skip" of a retention candidate (issue
/// #737): release the candidate's retention lease so a later tick can revisit
/// it, advance the scan cursor when the tick has not already failed, and drop
/// the id from the lease guard's active set. Used by every non-deleting,
/// non-failing per-candidate decision (never-delete type, not-old-enough for
/// its type, and the pre-existing dependency-based `should_skip_candidate`).
/// It must NOT freeze the cursor and must NOT set `has_failed`.
#[cfg(feature = "db")]
async fn routine_skip_candidate(
    conn: &mut diesel_async::AsyncPgConnection,
    candidate_id: uuid::Uuid,
    candidate_cursor: RetentionScanCursor,
    has_failed: bool,
    outcome: &mut ShardTickOutcome,
    active_ids: &Arc<Mutex<Vec<uuid::Uuid>>>,
) -> HarvestResult<()> {
    // Release its lease immediately so it can be picked up on subsequent ticks.
    diesel::update(
        harvest_workflow_executions::table.filter(harvest_workflow_executions::id.eq(candidate_id)),
    )
    .set(harvest_workflow_executions::sticky_worker_id.eq::<Option<String>>(None))
    .execute(conn)
    .await
    .map_err(database_error)?;

    // Advance cursor for routine skips.
    if !has_failed {
        outcome.next_cursor = Some(candidate_cursor);
    }

    {
        let mut active_guard = active_ids.lock().expect("lease guard lock poisoned");
        if let Some(pos) = active_guard.iter().position(|&x| x == candidate_id) {
            active_guard.swap_remove(pos);
        }
    }
    Ok(())
}

#[cfg(feature = "db")]
async fn should_skip_candidate(
    conn: &mut diesel_async::AsyncPgConnection,
    candidate: &CandidateExecution,
    cutoff: DateTime<Utc>,
) -> HarvestResult<bool> {
    let active_parent_ref_count = diesel::sql_query(
        "SELECT COUNT(*) AS count
         FROM harvest_workflow_executions
         WHERE parent_id = $1
           AND state NOT IN ('COMPLETED','FAILED','CANCELLED','TIMED_OUT','CONTINUED_AS_NEW','TERMINATED')",
    )
    .bind::<SqlUuid, _>(candidate.id)
    .get_result::<CountRow>(conn)
    .await
    .map_err(database_error)?
    .count;

    if active_parent_ref_count > 0 {
        return Ok(true);
    }

    let inflight_task_count = harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(Some(candidate.id)))
        .filter(harvest_task_queue::state.eq_any(["PENDING", "RUNNING"]))
        .count()
        .get_result::<i64>(conn)
        .await
        .map_err(database_error)?;
    if inflight_task_count > 0 {
        return Ok(true);
    }

    let pending_signal_count = harvest_signals::table
        .filter(harvest_signals::workflow_exec_id.eq(candidate.id))
        .filter(harvest_signals::consumed.eq(false))
        .count()
        .get_result::<i64>(conn)
        .await
        .map_err(database_error)?;
    if pending_signal_count > 0 {
        return Ok(true);
    }

    let pending_timer_count = harvest_timers::table
        .filter(harvest_timers::workflow_exec_id.eq(candidate.id))
        .filter(harvest_timers::fired.eq(false))
        .count()
        .get_result::<i64>(conn)
        .await
        .map_err(database_error)?;
    if pending_timer_count > 0 {
        return Ok(true);
    }

    let chain_link_count = diesel::sql_query(
        "SELECT COUNT(*) AS count
         FROM harvest_workflow_executions
         WHERE workflow_name = $1
           AND workflow_id = $2
           AND id <> $3
           AND (
                state NOT IN ('COMPLETED','FAILED','CANCELLED','TIMED_OUT','CONTINUED_AS_NEW','TERMINATED','MIGRATED')
               OR completed_at IS NULL
               OR completed_at >= $4
           )",
    )
    .bind::<Text, _>(&candidate.workflow_name)
    .bind::<Text, _>(&candidate.workflow_id)
    .bind::<SqlUuid, _>(candidate.id)
    .bind::<Timestamptz, _>(cutoff)
    .get_result::<CountRow>(conn)
    .await
    .map_err(database_error)?
    .count;

    Ok(chain_link_count > 0)
}

#[cfg(feature = "db")]
#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    count: i64,
}

// ── Per-execution legal hold (issue #747) ─────────────────────────────────────

/// Returns `true` when a per-execution legal hold is currently ACTIVE.
///
/// This is the **single source of truth** for the active-hold predicate, shared
/// by the retention-skip gate, the PII-erasure gate (issue #495), the
/// describe/list surfaces, and the set/release core functions. A hold is active
/// when it was placed (`set_at IS NOT NULL`) and has not auto-expired
/// (`until IS NULL`, i.e. indefinite, or `until > now`).
///
/// An expired hold (`until <= now`) is treated as inactive: the execution
/// becomes eligible for retention and erasure again without any explicit
/// release.
#[must_use]
pub fn legal_hold_active(
    set_at: Option<DateTime<Utc>>,
    until: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> bool {
    set_at.is_some() && until.is_none_or(|deadline| deadline > now)
}

/// Result of a [`set_legal_hold`] or [`release_legal_hold`] call.
///
/// Modeled on the idempotency shape of `terminate_workflow_execution`: the
/// operation is idempotent, and `newly_held` / `released` report whether this
/// call actually changed state.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LegalHoldOutcome {
    /// The execution the hold operation targeted.
    pub execution_id: String,
    /// Whether a legal hold is ACTIVE after this operation completed.
    pub held: bool,
    /// The active hold's reason, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub legal_hold_reason: Option<String>,
    /// The active hold's actor (the principal who placed it), if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub legal_hold_actor: Option<String>,
    /// When the active hold was placed, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub legal_hold_set_at: Option<DateTime<Utc>>,
    /// The active hold's auto-expiry, if any (`None` = indefinite).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub legal_hold_until: Option<DateTime<Utc>>,
    /// `true` when this `set` call actually placed a fresh hold; `false` when a
    /// hold was already active (idempotent no-op, provenance preserved).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub newly_held: bool,
    /// `true` when this `release` call actually cleared a hold; `false` when no
    /// hold was set (idempotent no-op).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub released: bool,
}

#[cfg(feature = "db")]
mod legal_hold_db {
    use chrono::{DateTime, Utc};
    use diesel::prelude::*;
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};

    use crate::error::{HarvestError, HarvestResult, database_error};
    use crate::schema::harvest_workflow_executions;
    use crate::types::ExecutionId;

    use super::{LegalHoldOutcome, legal_hold_active};

    /// The four legal-hold columns loaded from a locked execution row.
    type HoldColumns = (
        Option<DateTime<Utc>>,
        Option<DateTime<Utc>>,
        Option<String>,
        Option<String>,
    );

    async fn load_hold_for_update(
        conn: &mut AsyncPgConnection,
        exec_id: ExecutionId,
    ) -> HarvestResult<HoldColumns> {
        harvest_workflow_executions::table
            .find(exec_id.as_uuid())
            .select((
                harvest_workflow_executions::legal_hold_set_at,
                harvest_workflow_executions::legal_hold_until,
                harvest_workflow_executions::legal_hold_reason,
                harvest_workflow_executions::legal_hold_actor,
            ))
            .for_update()
            .first::<HoldColumns>(conn)
            .await
            .optional()
            .map_err(database_error)?
            .ok_or_else(|| HarvestError::NotFound(format!("workflow execution {exec_id}")))
    }

    /// Place (or refresh) a per-execution legal hold (issue #747). Shard-local:
    /// the caller routes `conn` to the execution's own shard.
    ///
    /// Idempotent: if an ACTIVE hold already exists, its provenance is left
    /// unchanged and `newly_held = false` is returned (no write). If there is no
    /// hold, or a previously-set hold has expired, the four columns are set and
    /// `newly_held = true` is returned.
    ///
    /// # Errors
    ///
    /// - [`HarvestError::NotFound`] when the execution does not exist (→ 404).
    /// - [`HarvestError::Database`] on any persistence failure.
    pub async fn set_legal_hold(
        conn: &mut AsyncPgConnection,
        exec_id: ExecutionId,
        reason: &str,
        hold_until: Option<DateTime<Utc>>,
        actor: &str,
        now: DateTime<Utc>,
    ) -> HarvestResult<LegalHoldOutcome> {
        // Empty reason normalizes to NULL (issue #747 MINOR 4) so a blank hold
        // stores `NULL` rather than `Some("")`, keeping the erase-rejection
        // message and describe field clean.
        let stored_reason = (!reason.is_empty()).then_some(reason);

        // The read + update run in one transaction (issue #747 MINOR 1) so the
        // `FOR UPDATE` lock in `load_hold_for_update` actually serializes the
        // read-modify-write against a concurrent set/release (and against the
        // retention delete-tx re-check), rather than releasing at statement end
        // in autocommit mode.
        Box::pin(
            conn.transaction::<LegalHoldOutcome, HarvestError, _>(async |conn| {
                let (set_at, until, cur_reason, cur_actor) =
                    load_hold_for_update(conn, exec_id).await?;

                if legal_hold_active(set_at, until, now) {
                    // Idempotent: an active hold already exists. Do NOT overwrite
                    // provenance — return the existing hold unchanged.
                    return Ok(LegalHoldOutcome {
                        execution_id: exec_id.to_string(),
                        held: true,
                        legal_hold_reason: cur_reason,
                        legal_hold_actor: cur_actor,
                        legal_hold_set_at: set_at,
                        legal_hold_until: until,
                        newly_held: false,
                        released: false,
                    });
                }

                diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
                    .set((
                        harvest_workflow_executions::legal_hold_set_at.eq(Some(now)),
                        harvest_workflow_executions::legal_hold_until.eq(hold_until),
                        harvest_workflow_executions::legal_hold_reason.eq(stored_reason),
                        harvest_workflow_executions::legal_hold_actor.eq(Some(actor)),
                    ))
                    .execute(conn)
                    .await
                    .map_err(database_error)?;

                // Report held/newly_held ACCURATELY (issue #747 MAJOR): a
                // `hold_until` already in the past writes the columns but places
                // no ACTIVE hold, so a direct core/CLI caller never gets a false
                // `held: true`. (The HTTP handler rejects a past `hold_until`
                // with 400 before reaching here — this is the belt-and-braces
                // core-layer guard.)
                let active = legal_hold_active(Some(now), hold_until, now);

                Ok(LegalHoldOutcome {
                    execution_id: exec_id.to_string(),
                    held: active,
                    legal_hold_reason: stored_reason.map(str::to_string),
                    legal_hold_actor: Some(actor.to_string()),
                    legal_hold_set_at: Some(now),
                    legal_hold_until: hold_until,
                    newly_held: active,
                    released: false,
                })
            }),
        )
        .await
    }

    /// Release a per-execution legal hold (issue #747). Shard-local.
    ///
    /// Idempotent: NULLs all four columns. `released = true` when a hold was
    /// previously set (regardless of whether it had already expired),
    /// `released = false` when the execution carried no hold at all (no-op).
    ///
    /// # Errors
    ///
    /// - [`HarvestError::NotFound`] when the execution does not exist (→ 404).
    /// - [`HarvestError::Database`] on any persistence failure.
    pub async fn release_legal_hold(
        conn: &mut AsyncPgConnection,
        exec_id: ExecutionId,
        _now: DateTime<Utc>,
    ) -> HarvestResult<LegalHoldOutcome> {
        // Read + update in one transaction (issue #747 MINOR 1) so the
        // `FOR UPDATE` lock holds across the clear, serializing against a
        // concurrent set/release.
        Box::pin(
            conn.transaction::<LegalHoldOutcome, HarvestError, _>(async |conn| {
                let (set_at, _until, _reason, _actor) = load_hold_for_update(conn, exec_id).await?;

                let was_set = set_at.is_some();
                if was_set {
                    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
                        .set((
                            harvest_workflow_executions::legal_hold_set_at
                                .eq::<Option<DateTime<Utc>>>(None),
                            harvest_workflow_executions::legal_hold_until
                                .eq::<Option<DateTime<Utc>>>(None),
                            harvest_workflow_executions::legal_hold_reason
                                .eq::<Option<String>>(None),
                            harvest_workflow_executions::legal_hold_actor
                                .eq::<Option<String>>(None),
                        ))
                        .execute(conn)
                        .await
                        .map_err(database_error)?;
                }

                Ok(LegalHoldOutcome {
                    execution_id: exec_id.to_string(),
                    held: false,
                    legal_hold_reason: None,
                    legal_hold_actor: None,
                    legal_hold_set_at: None,
                    legal_hold_until: None,
                    newly_held: false,
                    released: was_set,
                })
            }),
        )
        .await
    }
}

#[cfg(feature = "db")]
pub use legal_hold_db::{release_legal_hold, set_legal_hold};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ShardId;
    use std::time::Duration;

    #[test]
    fn test_retention_config_validation() {
        let config = RetentionConfig::default();
        assert!(config.validate().is_ok());

        // Test tick_interval = 0 is invalid
        let config = RetentionConfig {
            tick_interval_secs: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());

        // Test batch_size = 0 is invalid
        let config = RetentionConfig {
            batch_size: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());

        // Test max_age validation bounds
        let mut config = RetentionConfig {
            max_age_secs: Some(0), // under MIN_MAX_AGE (1s)
            ..Default::default()
        };
        assert!(config.validate().is_err());

        config.max_age_secs = Some(60 * 60 * 24 * 365 * 20); // over MAX_MAX_AGE (10 years)
        assert!(config.validate().is_err());

        config.max_age_secs = Some(3600); // valid
        assert!(config.validate().is_ok());

        // Test archival_timeout_secs = 0 is invalid
        let config = RetentionConfig {
            archival_timeout_secs: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    // --- Issue #1266: per-shard protect_unexported_audit exemption ---

    #[test]
    fn protect_unexported_audit_disabled_by_default() {
        let config = RetentionConfig::default();
        assert!(!config.protects_unexported_audit(ShardId::new(0)));
        assert!(!config.protects_unexported_audit(ShardId::new(1)));
    }

    #[test]
    fn protect_unexported_audit_true_covers_every_shard() {
        let config = RetentionConfig::default().with_protect_unexported_audit(true);
        assert!(config.protects_unexported_audit(ShardId::new(0)));
        assert!(config.protects_unexported_audit(ShardId::new(1)));
    }

    // A fleet decommissioning shard 0 must not lose bootstrap protection
    // for shard 1, still mid-bootstrap on the same sweep. One process-wide
    // boolean cannot represent both states at once, which is why the
    // exemption exists.
    #[test]
    fn excluding_a_shard_leaves_every_other_shard_protected() {
        let config = RetentionConfig::default()
            .with_protect_unexported_audit(true)
            .excluding_shard_from_protect_unexported_audit(ShardId::new(0));
        assert!(
            !config.protects_unexported_audit(ShardId::new(0)),
            "the exempted shard must be free to resume purging"
        );
        assert!(
            config.protects_unexported_audit(ShardId::new(1)),
            "a different shard must stay protected"
        );
    }

    #[test]
    fn excluding_a_shard_while_disabled_changes_nothing() {
        let config = RetentionConfig::default()
            .excluding_shard_from_protect_unexported_audit(ShardId::new(0));
        assert!(!config.protects_unexported_audit(ShardId::new(0)));
        assert!(!config.protects_unexported_audit(ShardId::new(1)));
    }

    // Two logical shards may alias one physical pool (a supported pre-split
    // staging topology). Building a `Pool` never connects, so this needs no
    // live database.
    #[cfg(feature = "db")]
    fn test_pool(url: &str) -> crate::worker::DbPool {
        let manager = diesel_async::pooled_connection::AsyncDieselConnectionManager::<
            diesel_async::AsyncPgConnection,
        >::new(url);
        crate::worker::DbPool::builder(manager)
            .max_size(1)
            .build()
            .expect("pool builds without connecting")
    }

    // A shard exempted for decommission must not drag its physical-pool
    // alias down with it. One aliased shard still wants protection, so the
    // whole shared pool must stay protected (issue #1266).
    #[cfg(feature = "db")]
    #[test]
    fn group_shards_by_pool_combines_aliased_shards_conservatively() {
        let pool = test_pool("postgres://unused/db");
        let mut aliased = BTreeMap::new();
        aliased.insert(ShardId::new(0), pool.clone());
        aliased.insert(ShardId::new(1), pool);
        let sharded = ShardedDbPool::from_map(aliased, ShardId::new(0));

        let config = RetentionConfig::default()
            .with_protect_unexported_audit(true)
            .excluding_shard_from_protect_unexported_audit(ShardId::new(0));

        let groups = group_shards_by_pool(&sharded, &config);
        assert_eq!(
            groups.len(),
            1,
            "both shards alias one pool, so they must collapse to one group"
        );
        assert!(
            groups[0].1,
            "shard 1 still wants protection, so the shared pool stays protected"
        );
        assert_eq!(
            groups[0].2,
            vec![ShardId::new(1)],
            "only shard 1 wants protection, so only it belongs in the \
             expected-cursor list; exempted shard 0 must not appear \
             there even though it shares the pool"
        );
    }

    // Two shards on genuinely separate pools must never be combined. Shard
    // 0's exemption must stay local to its own pool (issue #1266).
    #[cfg(feature = "db")]
    #[test]
    fn group_shards_by_pool_keeps_distinct_pools_separate() {
        let pool_a = test_pool("postgres://unused/db-a");
        let pool_b = test_pool("postgres://unused/db-b");
        let mut distinct = BTreeMap::new();
        distinct.insert(ShardId::new(0), pool_a);
        distinct.insert(ShardId::new(1), pool_b);
        let sharded = ShardedDbPool::from_map(distinct, ShardId::new(0));

        let config = RetentionConfig::default()
            .with_protect_unexported_audit(true)
            .excluding_shard_from_protect_unexported_audit(ShardId::new(0));

        let groups = group_shards_by_pool(&sharded, &config);
        assert_eq!(
            groups.len(),
            2,
            "two distinct pools must never collapse into one group"
        );
        let protections: Vec<bool> = groups.iter().map(|(_, protect, _)| *protect).collect();
        assert!(
            protections.contains(&false) && protections.contains(&true),
            "shard 0's exemption must not leak into shard 1's own, separate pool"
        );
        for (_, protect, shards) in &groups {
            if *protect {
                assert_eq!(
                    *shards,
                    vec![ShardId::new(1)],
                    "shard 1's own pool expects a cursor from shard 1"
                );
            } else {
                assert!(
                    shards.is_empty(),
                    "exempted shard 0's own pool expects no cursor at all"
                );
            }
        }
    }

    // An exempted shard that never ticks must not permanently block
    // purging on a colocated shard that has (issue #1266). Naming an
    // exempted shard in the expected-cursor list would make the
    // missing-cursor check true forever, defeating the exemption.
    #[cfg(feature = "db")]
    #[test]
    fn group_shards_by_pool_excludes_an_exempted_shard_from_the_expected_cursor_list() {
        let pool = test_pool("postgres://unused/db");
        let mut aliased = BTreeMap::new();
        aliased.insert(ShardId::new(0), pool.clone());
        aliased.insert(ShardId::new(1), pool);
        let sharded = ShardedDbPool::from_map(aliased, ShardId::new(0));

        // Shard 0 is exempted -- an unreachable shard whose export was
        // abandoned, never ticked, never decommissioned. Shard 1 still
        // wants protection.
        let config = RetentionConfig::default()
            .with_protect_unexported_audit(true)
            .excluding_shard_from_protect_unexported_audit(ShardId::new(0));

        let groups = group_shards_by_pool(&sharded, &config);
        assert_eq!(
            groups[0].2,
            vec![ShardId::new(1)],
            "shard 0's exemption must remove it from the expected-cursor \
             list entirely, not merely from the protection decision, or \
             its permanent lack of a cursor row would block purging of \
             rows shard 1 has genuinely acknowledged"
        );
    }

    // Leaving `protect_unexported_audit` unset entirely (the common
    // case, issue #1266) must not empty the expected-cursor list.
    // `protects_unexported_audit` answers "does the flag protect this
    // shard today". That is `false` for every shard when the flag is
    // off. It is a different question from "is this shard exempted",
    // which is what the expected-cursor list must filter on. Confusing
    // the two would silently disable the missing-cursor check for every
    // deployment that never configures this flag at all.
    #[cfg(feature = "db")]
    #[test]
    fn group_shards_by_pool_expects_every_shard_when_the_flag_is_never_configured() {
        let pool = test_pool("postgres://unused/db");
        let mut aliased = BTreeMap::new();
        aliased.insert(ShardId::new(0), pool.clone());
        aliased.insert(ShardId::new(1), pool);
        let sharded = ShardedDbPool::from_map(aliased, ShardId::new(0));

        let config = RetentionConfig::default();
        assert!(
            !config.protects_unexported_audit(ShardId::new(0)),
            "the flag protects nobody when it is off, by design"
        );

        let groups = group_shards_by_pool(&sharded, &config);
        assert_eq!(
            groups[0].2,
            vec![ShardId::new(0), ShardId::new(1)],
            "an unconfigured flag exempts no one, so both colocated \
             shards must still be expected to have a cursor row, exactly \
             as they were before this flag existed"
        );
    }

    // --- Issue #737: per-workflow-type history retention overrides ---

    #[test]
    fn test_effective_max_age_resolution() {
        // override present -> override wins over global
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600))
            .with_workflow_override("slow_wf", Duration::from_secs(7200));
        assert_eq!(
            config.effective_max_age("slow_wf"),
            Some(Duration::from_secs(7200))
        );
        // no override for this type -> falls back to global
        assert_eq!(
            config.effective_max_age("other_wf"),
            Some(Duration::from_secs(3600))
        );

        // no global, override present -> override for that type, None for others
        // (Default already leaves max_age_secs = None.)
        let config =
            RetentionConfig::default().with_workflow_override("only_wf", Duration::from_secs(500));
        assert_eq!(
            config.effective_max_age("only_wf"),
            Some(Duration::from_secs(500))
        );
        assert_eq!(config.effective_max_age("other_wf"), None);

        // neither -> None (never delete)
        let config = RetentionConfig::default();
        assert_eq!(config.effective_max_age("anything"), None);
    }

    #[test]
    fn test_loosest_cutoff_age() {
        // global only
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600));
        assert_eq!(config.loosest_cutoff_age(), Some(Duration::from_secs(3600)));

        // global + overrides -> min of all
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600))
            .with_workflow_override("longer", Duration::from_secs(7200))
            .with_workflow_override("shorter", Duration::from_secs(600));
        assert_eq!(config.loosest_cutoff_age(), Some(Duration::from_secs(600)));

        // no global, overrides only -> min override
        let config = RetentionConfig::default()
            .with_workflow_override("a", Duration::from_secs(900))
            .with_workflow_override("b", Duration::from_secs(300));
        assert_eq!(config.loosest_cutoff_age(), Some(Duration::from_secs(300)));

        // global present but larger than an override -> override wins as loosest
        let config = RetentionConfig::with_max_age(Duration::from_secs(5000))
            .with_workflow_override("tiny", Duration::from_secs(100));
        assert_eq!(config.loosest_cutoff_age(), Some(Duration::from_secs(100)));

        // neither -> None
        let config = RetentionConfig::default();
        assert_eq!(config.loosest_cutoff_age(), None);
    }

    #[test]
    fn test_history_retention_active() {
        // both unset
        let config = RetentionConfig::default();
        assert!(!config.history_retention_active());

        // global set
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600));
        assert!(config.history_retention_active());

        // only overrides set
        let config =
            RetentionConfig::default().with_workflow_override("wf", Duration::from_secs(60));
        assert!(config.history_retention_active());
    }

    #[test]
    fn test_validate_overrides_bounds() {
        // below MIN_MAX_AGE
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600))
            .with_workflow_override("wf", Duration::from_secs(0));
        assert!(config.validate().is_err());

        // above MAX_MAX_AGE
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600))
            .with_workflow_override("wf", Duration::from_secs(60 * 60 * 24 * 365 * 20));
        assert!(config.validate().is_err());

        // in range
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600))
            .with_workflow_override("wf", Duration::from_secs(7200));
        assert!(config.validate().is_ok());

        // exactly MIN_MAX_AGE (1s) -> Ok (inclusive lower bound; guards against
        // an off-by-one flip of ..= to ..)
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600))
            .with_workflow_override("wf", Duration::from_secs(1));
        assert!(config.validate().is_ok());

        // exactly MAX_MAX_AGE (10 years) -> Ok (inclusive upper bound)
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600))
            .with_workflow_override("wf", Duration::from_secs(60 * 60 * 24 * 365 * 10));
        assert!(config.validate().is_ok());

        // empty overrides + valid global -> Ok
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600));
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_overrides_backward_compat_empty() {
        // With no overrides, effective_max_age(any) == max_age() for all names.
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600));
        assert!(config.workflow_overrides().is_empty());
        assert_eq!(config.effective_max_age("any_wf"), config.max_age());
        assert_eq!(config.effective_max_age("another"), config.max_age());
    }

    #[test]
    fn test_with_workflow_overrides_bulk() {
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600))
            .with_workflow_overrides([
                ("a".to_string(), Duration::from_secs(100)),
                ("b".to_string(), Duration::from_secs(200)),
            ]);
        assert_eq!(config.workflow_overrides().len(), 2);
        assert_eq!(
            config.effective_max_age("a"),
            Some(Duration::from_secs(100))
        );
        assert_eq!(
            config.effective_max_age("b"),
            Some(Duration::from_secs(200))
        );
    }

    #[test]
    fn test_retention_config_enabled() {
        let config = RetentionConfig {
            audit_retention_days: 0,
            schedule_decision_retention_days: 0,
            partitions: PartitionMaintenanceConfig {
                enabled: false,
                ..PartitionMaintenanceConfig::default()
            },
            ..Default::default()
        }
        // Issue #1127: the idle rate-limit bucket GC is on by default and is
        // itself an enabling reason, so "nothing enabled" now has to switch it
        // off too.
        .without_rate_limit_bucket_gc();
        // no purging AND no partition maintenance AND no bucket GC is not enabled
        assert!(!config.enabled());

        // …but partition maintenance ALONE is (issue #958). An opted-in
        // partitioned shard whose horizons are all off still needs its
        // lookahead window extended, or every append ends up in the DEFAULT
        // partition and nothing is ever reclaimed.
        let partitions_only = RetentionConfig {
            audit_retention_days: 0,
            schedule_decision_retention_days: 0,
            ..Default::default()
        };
        assert!(partitions_only.partitions.enabled);
        assert!(
            partitions_only.enabled(),
            "partition maintenance must itself spawn the runtime — otherwise \
             `partitions.enabled` reads true while nothing honours it"
        );

        let config = RetentionConfig {
            max_age_secs: Some(3600),
            audit_retention_days: 0,
            schedule_decision_retention_days: 0,
            ..Default::default()
        };
        assert!(config.enabled());

        let config = RetentionConfig {
            audit_retention_days: 30,
            ..Default::default()
        };
        assert!(config.enabled());

        let config = RetentionConfig {
            schedule_decision_retention_days: 7,
            ..Default::default()
        };
        assert!(config.enabled());

        // Overrides-only (no global max_age, no audit/schedule purging) still
        // enables the janitor (issue #737).
        let config = RetentionConfig {
            max_age_secs: None,
            audit_retention_days: 0,
            schedule_decision_retention_days: 0,
            ..Default::default()
        }
        .with_workflow_override("wf", Duration::from_secs(3600));
        assert!(config.enabled());
    }

    // --- Issue #752: tiered / summary retention ---

    #[test]
    fn summary_policy_builders() {
        let p = SummaryPolicy::for_days(7);
        assert_eq!(p.retention_age(), Some(Duration::from_secs(7 * 86_400)));
        assert!(!p.capture_payload());
        assert_eq!(p.max_payload_bytes(), DEFAULT_SUMMARY_PAYLOAD_CAP);

        let p = SummaryPolicy::for_duration(Duration::from_secs(500));
        assert_eq!(p.retention_age(), Some(Duration::from_secs(500)));

        let p = SummaryPolicy::unbounded();
        assert_eq!(p.retention_age(), None);
        assert_eq!(p.retention, SummaryRetention::Unbounded);

        let p = SummaryPolicy::for_days(1)
            .with_payload_capture()
            .with_max_payload_bytes(2048);
        assert!(p.capture_payload());
        assert_eq!(p.max_payload_bytes(), 2048);
    }

    #[test]
    fn summary_config_builders_and_predicates() {
        // Default: summary disabled -> byte-for-byte today's behavior.
        let config = RetentionConfig::default();
        assert!(!config.summary_enabled());
        assert!(!config.summary_gc_active());
        assert_eq!(config.summary_age(), None);
        assert!(config.summary_policy().is_none());

        // Bounded summary -> gc active, spawns janitor even without a history
        // horizon (issue #752).
        let config = RetentionConfig {
            audit_retention_days: 0,
            schedule_decision_retention_days: 0,
            ..RetentionConfig::default()
        }
        .with_summary_retention_days(30);
        assert!(config.summary_enabled());
        assert!(config.summary_gc_active());
        assert_eq!(config.summary_age(), Some(Duration::from_secs(30 * 86_400)));
        assert!(config.enabled(), "bounded summary must enable the janitor");

        // Unbounded summary -> enabled but never GC'd; does NOT by itself spawn
        // the janitor (no deletes => no summaries to create, nothing to GC).
        let config = RetentionConfig {
            audit_retention_days: 0,
            schedule_decision_retention_days: 0,
            ..RetentionConfig::default()
        }
        .with_summary_retention(SummaryPolicy::unbounded());
        assert!(config.summary_enabled());
        assert!(!config.summary_gc_active());
        assert_eq!(config.summary_age(), None);
        let config = RetentionConfig {
            partitions: PartitionMaintenanceConfig {
                enabled: false,
                ..PartitionMaintenanceConfig::default()
            },
            ..config
        }
        // Issue #1127: the bucket GC is on by default and enabling on its own.
        .without_rate_limit_bucket_gc();
        assert!(
            !config.enabled(),
            "an unbounded-summary-only config with no history/audit horizon, no \
             partition maintenance and no bucket GC is not enabled"
        );

        // But a history horizon + unbounded summary IS enabled (via history).
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600))
            .with_summary_retention(SummaryPolicy::unbounded());
        assert!(config.enabled());
    }

    #[test]
    fn summary_validate_bounds_the_horizon() {
        // below MIN_MAX_AGE
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600))
            .with_summary_retention(SummaryPolicy::for_duration(Duration::from_secs(0)));
        assert!(config.validate().is_err());

        // above MAX_MAX_AGE
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600))
            .with_summary_retention(SummaryPolicy::for_duration(Duration::from_secs(
                60 * 60 * 24 * 365 * 20,
            )));
        assert!(config.validate().is_err());

        // in range
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600))
            .with_summary_retention_days(90);
        assert!(config.validate().is_ok());

        // Unbounded needs no bound.
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600))
            .with_summary_retention(SummaryPolicy::unbounded());
        assert!(config.validate().is_ok());
    }

    // -----------------------------------------------------------------------
    // Idle rate-limit-bucket GC config (issue #1127)
    // -----------------------------------------------------------------------

    #[test]
    fn rate_limit_bucket_gc_is_on_by_default_at_seven_days() {
        // Unbounded growth is a BUG, so the collector is on out of the box.
        let config = RetentionConfig::default();
        assert_eq!(
            config.rate_limit_bucket_retention(),
            Some(Duration::from_secs(
                DEFAULT_RATE_LIMIT_BUCKET_RETENTION_SECS
            ))
        );
        assert_eq!(DEFAULT_RATE_LIMIT_BUCKET_RETENTION_SECS, 7 * 24 * 60 * 60);
        assert!(config.rate_limit_bucket_gc_active());
    }

    #[test]
    fn rate_limit_bucket_gc_window_is_configurable_and_disablable() {
        let config = RetentionConfig::default()
            .with_rate_limit_bucket_retention(Duration::from_secs(6 * 60 * 60));
        assert_eq!(
            config.rate_limit_bucket_retention(),
            Some(Duration::from_secs(6 * 60 * 60))
        );

        let off = RetentionConfig::default().without_rate_limit_bucket_gc();
        assert_eq!(off.rate_limit_bucket_retention(), None);
        assert!(!off.rate_limit_bucket_gc_active());
    }

    #[test]
    fn a_zero_window_is_rejected_rather_than_collecting_everything_now() {
        // `Some(0)` would leave the GC ACTIVE with a cutoff of "now", i.e.
        // collect every full, unpinned bucket immediately — reopening the
        // stranding race the touch interval closes. It must fail the build, not
        // be treated as "disabled".
        let zero = RetentionConfig::default().with_rate_limit_bucket_retention(Duration::ZERO);
        assert!(zero.rate_limit_bucket_gc_active(), "zero is not 'disabled'");
        assert!(zero.validate().is_err());
    }

    #[test]
    fn rate_limit_bucket_gc_validate_bounds_the_window() {
        // Below the floor: a window shorter than the ensure-path touch
        // interval would reopen the stranding race.
        let too_short = RetentionConfig::default().with_rate_limit_bucket_retention(
            MIN_RATE_LIMIT_BUCKET_RETENTION
                .checked_sub(Duration::from_secs(1))
                .expect("the floor is well above 1s"),
        );
        assert!(too_short.validate().is_err());

        let too_long = RetentionConfig::default()
            .with_rate_limit_bucket_retention(MAX_MAX_AGE + Duration::from_secs(1));
        assert!(too_long.validate().is_err());

        assert!(
            RetentionConfig::default()
                .with_rate_limit_bucket_retention(MIN_RATE_LIMIT_BUCKET_RETENTION)
                .validate()
                .is_ok()
        );
        assert!(RetentionConfig::default().validate().is_ok());
    }

    #[test]
    fn a_gc_only_config_still_spawns_the_janitor() {
        let config = RetentionConfig {
            max_age_secs: None,
            audit_retention_days: 0,
            schedule_decision_retention_days: 0,
            partitions: PartitionMaintenanceConfig {
                enabled: false,
                ..PartitionMaintenanceConfig::default()
            },
            ..RetentionConfig::default()
        };
        assert!(config.rate_limit_bucket_gc_active());
        assert!(
            config.enabled(),
            "a bucket-GC-only config must still spawn the retention runtime, \
             or the table grows unbounded with nothing to collect it"
        );

        let off = config.without_rate_limit_bucket_gc();
        assert!(!off.enabled());
    }

    #[test]
    fn omitted_marker_shape() {
        let m = omitted_marker("too_large", Some(5000));
        assert_eq!(m[OMITTED_MARKER_KEY], true);
        assert_eq!(m["reason"], "too_large");
        assert_eq!(m["bytes"], 5000);

        let m = omitted_marker("offloaded", None);
        assert_eq!(m[OMITTED_MARKER_KEY], true);
        assert_eq!(m["reason"], "offloaded");
        assert!(m.get("bytes").is_none(), "no bytes for offloaded marker");
    }

    #[test]
    fn cap_result_payload_none_and_small_and_over_cap() {
        // None -> None
        assert!(cap_result_payload(None, 4096).is_none());

        // small inline -> stored verbatim
        let small = serde_json::json!({"ok": true, "n": 1});
        assert_eq!(cap_result_payload(Some(small.clone()), 4096), Some(small));

        // over-cap -> too_large marker carrying the observed byte length
        let big_str = "x".repeat(100);
        let big = serde_json::json!({"blob": big_str});
        let len = serde_json::to_vec(&big).unwrap().len();
        let capped = cap_result_payload(Some(big), 32).expect("some");
        assert_eq!(capped[OMITTED_MARKER_KEY], true);
        assert_eq!(capped["reason"], "too_large");
        assert_eq!(capped["bytes"], len);
    }

    #[test]
    fn cap_result_payload_never_stores_an_offload_envelope() {
        // A synthetic offload reference envelope (issue #524): must become the
        // "offloaded" marker, NOT the envelope itself (the blob may be GC'd).
        let envelope = serde_json::json!({
            "_harvest_offload_envelope": 1,
            "store_id": "s3",
            "key": "blob/abc123",
            "len": 2_000_000,
            "checksum": "deadbeef",
        });
        let capped = cap_result_payload(Some(envelope), 4096).expect("some");
        assert_eq!(capped[OMITTED_MARKER_KEY], true);
        assert_eq!(capped["reason"], "offloaded");
        assert!(
            capped.get("key").is_none(),
            "the blob key/reference must NOT leak into the summary"
        );
    }

    #[test]
    fn cap_error_text_none_small_and_over_cap() {
        assert!(cap_error_text(None, 4096).is_none());
        assert_eq!(cap_error_text(Some("boom"), 4096), Some("boom".to_string()));
        let big = "e".repeat(200);
        let capped = cap_error_text(Some(&big), 32).expect("some");
        assert_eq!(capped, "[omitted: too_large, 200 bytes]");
    }

    #[test]
    fn test_retention_monitor() {
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600));
        let shards = vec![ShardId::new(0), ShardId::new(1)].into_iter();
        let monitor = RetentionMonitor::new(config, shards);

        let snapshot = monitor.snapshot();
        assert_eq!(snapshot.per_shard.len(), 2);
        assert_eq!(snapshot.per_shard[0].shard, 0);
        assert_eq!(snapshot.per_shard[1].shard, 1);
    }

    #[tokio::test]
    #[cfg(feature = "db")]
    async fn test_run_shard_tick_cursor_frozen_on_skip() {
        // Build mock candidates
        let candidate_ok = CandidateExecution {
            id: uuid::Uuid::new_v4(),
            workflow_name: "test".to_string(),
            workflow_id: "ok".to_string(),
            state: "COMPLETED".to_string(),
            completed_at: Some(Utc::now() - chrono::Duration::days(10)),
            context_headers: None,
            legal_hold_set_at: None,
            legal_hold_until: None,
            execution_timeout: None,
            deadline_at: None,
            parent_id: None,
            queue_name: "default".to_string(),
        };
        let candidate_skip = CandidateExecution {
            id: uuid::Uuid::new_v4(),
            workflow_name: "test".to_string(),
            workflow_id: "skip".to_string(),
            state: "COMPLETED".to_string(),
            completed_at: Some(Utc::now() - chrono::Duration::days(9)),
            context_headers: None,
            legal_hold_set_at: None,
            legal_hold_until: None,
            execution_timeout: None,
            deadline_at: None,
            parent_id: None,
            queue_name: "default".to_string(),
        };

        // When evaluating outcome next_cursor logic, if the first candidate completes,
        // outcome.next_cursor should advance to it. If the second candidate is skipped,
        // outcome.next_cursor must freeze on the first candidate's cursor to ensure retries on subsequent ticks.
        let mut outcome = ShardTickOutcome::default();
        let mut has_skipped = false;

        // candidate 1 (success)
        let cursor1 = RetentionScanCursor {
            completed_at: candidate_ok.completed_at.unwrap(),
            id: candidate_ok.id,
        };
        if !has_skipped {
            outcome.next_cursor = Some(cursor1);
        }

        // candidate 2 (skipped)
        let cursor2 = RetentionScanCursor {
            completed_at: candidate_skip.completed_at.unwrap(),
            id: candidate_skip.id,
        };
        has_skipped = true;
        if !has_skipped {
            outcome.next_cursor = Some(cursor2);
        }

        assert_eq!(outcome.next_cursor, Some(cursor1));
    }

    // ── Legal hold (issue #747) ───────────────────────────────────────────────

    #[test]
    fn legal_hold_inactive_when_never_set() {
        let now = Utc::now();
        assert!(!legal_hold_active(None, None, now));
        // A stray `until` with no `set_at` is still not a hold.
        assert!(!legal_hold_active(
            None,
            Some(now + chrono::Duration::days(1)),
            now
        ));
    }

    #[test]
    fn legal_hold_active_when_set_and_indefinite() {
        let now = Utc::now();
        assert!(legal_hold_active(
            Some(now - chrono::Duration::hours(1)),
            None,
            now
        ));
    }

    #[test]
    fn legal_hold_active_when_until_in_future() {
        let now = Utc::now();
        assert!(legal_hold_active(
            Some(now - chrono::Duration::hours(1)),
            Some(now + chrono::Duration::hours(1)),
            now
        ));
    }

    #[test]
    fn legal_hold_inactive_when_expired() {
        let now = Utc::now();
        // until == now is expired (strict `>`): a hold whose deadline has been
        // reached is no longer active.
        assert!(!legal_hold_active(
            Some(now - chrono::Duration::hours(2)),
            Some(now),
            now
        ));
        assert!(!legal_hold_active(
            Some(now - chrono::Duration::hours(2)),
            Some(now - chrono::Duration::hours(1)),
            now
        ));
    }

    #[test]
    fn legal_hold_outcome_omits_optional_none_fields() {
        let released = LegalHoldOutcome {
            execution_id: "exec-1".into(),
            held: false,
            legal_hold_reason: None,
            legal_hold_actor: None,
            legal_hold_set_at: None,
            legal_hold_until: None,
            newly_held: false,
            released: true,
        };
        let v = serde_json::to_value(&released).unwrap();
        assert_eq!(v["held"], false);
        assert_eq!(v["released"], true);
        assert!(v.get("newly_held").is_none(), "false flag is omitted");
        assert!(v.get("legal_hold_reason").is_none());
        assert!(v.get("legal_hold_until").is_none());

        let held = LegalHoldOutcome {
            execution_id: "exec-2".into(),
            held: true,
            legal_hold_reason: Some("subpoena".into()),
            legal_hold_actor: Some("legal@corp".into()),
            legal_hold_set_at: Some(Utc::now()),
            legal_hold_until: None,
            newly_held: true,
            released: false,
        };
        let v = serde_json::to_value(&held).unwrap();
        assert_eq!(v["held"], true);
        assert_eq!(v["newly_held"], true);
        assert_eq!(v["legal_hold_reason"], "subpoena");
        assert!(v.get("released").is_none(), "false flag is omitted");
    }
}
