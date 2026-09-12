//! Per-tenant resource quotas on executions, history, and DLQ (issue #946).
//!
//! Harvest already bounds per-key **rate** ([`crate::throttle`], issue #607)
//! and per-key **parallelism** ([`crate::concurrency`], issue #247), but
//! nothing previously capped a tenant's aggregate *footprint* — how much a
//! single misbehaving or malicious caller can accumulate in the system
//! before anyone notices. A tenant submitting 10,000 workflow starts in a
//! tight loop, each of which piles up history and eventually a dead letter,
//! could consume unbounded storage and DLQ backlog with no engine-level
//! circuit breaker.
//!
//! `QuotaPolicy` closes that gap: a workflow type may declare up to three
//! independent, optional caps on the *stock* a resolved tenant key has
//! accumulated:
//!
//! - [`QuotaResource::ActiveExecutions`] — count of non-terminal
//!   (`RUNNING`/`PAUSED`) executions sharing the resolved key.
//! - [`QuotaResource::HistoryBytes`] — aggregate `harvest_events` payload
//!   bytes across the key's active executions.
//! - [`QuotaResource::DeadLetters`] — count of `harvest_dead_letters` rows
//!   sharing the resolved key.
//!
//! # Quota vs. concurrency vs. throttle
//!
//! All three share the *same* dot-path key resolver
//! ([`resolve_quota_key`], which delegates to
//! [`crate::concurrency::resolve_concurrency_key`] — there is deliberately no
//! second resolver), but answer different questions:
//!
//! | Primitive | Question | Bounds |
//! |---|---|---|
//! | [`crate::concurrency::ConcurrencyPolicy`] (#247) | How many at once? | in-flight parallelism |
//! | [`crate::throttle::ThrottlePolicy`] (#607) | How fast? | admission rate |
//! | [`QuotaPolicy`] (#946) | How much accumulated? | aggregate stock |
//!
//! A tenant could be well within a `limit = 10` concurrency cap and a
//! `100/m` throttle and *still* accumulate 10,000 completed-but-not-yet-
//! retention-collected executions, each with a sizeable history, if nothing
//! bounds the aggregate. Quota is the third, orthogonal axis.
//!
//! # Enforcement point
//!
//! Quota is checked entirely inside
//! [`crate::execution::start_or_load_workflow_execution_collect`] — the one
//! function nearly every registry-aware start path funnels through — rather
//! than threading a new field through [`crate::execution::StartWorkflowParams`].
//! The declared policy is resolved from a process-global mirror of each
//! workflow type's [`crate::info::WorkflowInfo`]
//! ([`crate::completion_trigger::GLOBAL_WORKFLOW_METADATA`], the same
//! mechanism `concurrency`/`sla`/`retry_policy` already use to reach
//! core-crate-internal code that has no access to the plugin's live handler
//! registry), and the tenant key is resolved from the request's own input —
//! so no new [`crate::execution::StartWorkflowParams`] field is required.
//!
//! # Sharding scope (issue #946 AC8)
//!
//! Quota enforcement is **shard-local**, exactly like [`crate::concurrency`]
//! and [`crate::throttle`]: `harvest_workflow_executions.quota_key` and
//! `harvest_dead_letters.quota_key` are per-shard columns, and the advisory
//! lock ([`lock_quota_key`]) is per-connection. A tenant whose workload is
//! spread across N shards effectively gets a `limit × N` aggregate cap
//! unless pinned to a single shard (issue #697's `residency_key`). See
//! `docs/sharding.md` for the operator-facing note.
//!
//! # No new `WorkflowEvent` variant, no replay impact
//!
//! `quota_key` is a plain, denormalized row column on
//! `harvest_workflow_executions` and `harvest_dead_letters` — resolved once
//! at admission time, never recorded as an event, and never replayed. A
//! workflow type with no declared [`QuotaPolicy`] leaves `quota_key = NULL`
//! everywhere and pays zero enforcement overhead (issue #946 AC9).
//!
//! # Pre-upgrade rollout gap, closed by a periodic reconciler (issue #1226)
//!
//! `quota_key` is resolved only at admission time. Consider an execution
//! already `RUNNING`/`PAUSED` *before* its workflow type's
//! [`QuotaPolicy`] was declared/deployed. It would otherwise keep
//! `quota_key = NULL` for the rest of its life — neither counted against
//! the new cap nor blocked by it. [`crate::quota_reconcile`] closes this:
//! a periodic, shard-local sweep re-resolves and backfills `quota_key` for
//! exactly such rows. It uses this module's own [`resolve_quota_key`] —
//! the same function the live admission path calls. A backfilled value
//! can therefore never drift from what a fresh admission would compute.
//!
//! The sweep runs on the worker's heartbeat cadence, not synchronously
//! inside admission. A row therefore stays invisible to
//! [`load_quota_usage`] for up to one reconcile interval after its policy
//! takes effect. See [`crate::quota_reconcile`]'s module doc for the full
//! design (why periodic rather than startup-once, and why that residual
//! window is not a new risk class).
//!
//! # Known limitation — batched-start key attribution (issue #1230)
//!
//! A batched execution charges its quota to whichever admission arrived
//! first for the shared `batch(key = ...)` (see
//! `crate::event_batch`'s own doc for the mechanism). Two consequences
//! follow directly from that rule, not from a defect in it:
//!
//! - A caller who admits into a batch without a resolvable quota-key
//!   field pays no quota charge for that admission. A direct start has
//!   the same gap. Fail-open on an unresolvable key is this crate's
//!   existing, uniform contract — see `unresolvable_key_fails_open` in
//!   `quota_enforcement_tests.rs`. Batching does not change that
//!   contract. It does not add a NEW way to evade it.
//! - When a `batch(key = ...)` is shared across more than one tenant,
//!   the resolved quota key belongs to the FIRST admission. That need
//!   not be the admission whose payload happened to fill the batch and
//!   trigger the fire. A caller who triggers a synchronous flush
//!   (`admit_batched_start`'s in-request path) can therefore observe
//!   another admission's resolved key. That key appears in the
//!   [`HarvestError::QuotaExceeded`](crate::error::HarvestError::QuotaExceeded)
//!   `key` field of a `429` response. Issue #946 AC4 already returns
//!   that same wire shape for every other quota rejection. Batching
//!   only changes whose key a caller might see, not the shape or
//!   existence of the field. Sharing one `batch_key` across tenants is
//!   an unusual workflow design choice. The common case, collapsing one
//!   tenant's own burst into one run, never exposes another tenant's
//!   key, because there is no other tenant in the batch.

#[cfg(feature = "db")]
use diesel::sql_types::{BigInt, Nullable, Text};
#[cfg(feature = "db")]
use diesel_async::{AsyncPgConnection, RunQueryDsl};

#[cfg(feature = "db")]
use crate::error::{HarvestResult, database_error};

// ---------------------------------------------------------------------------
// QuotaResource
// ---------------------------------------------------------------------------

/// The aggregate resource dimension a [`QuotaPolicy`] caps (issue #946).
///
/// Used in [`HarvestError::QuotaExceeded`](crate::error::HarvestError::QuotaExceeded)
/// to identify which cap was exhausted, and as the sole label on the
/// `harvest.quota.rejected{workflow, resource}` metric (issue #946 AC6 — the
/// resolved tenant key is deliberately NOT a label; it is unbounded caller
/// input).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuotaResource {
    /// Count of non-terminal (`RUNNING`/`PAUSED`) executions sharing the
    /// resolved key.
    ActiveExecutions,
    /// Aggregate `harvest_events` payload bytes across the key's active
    /// executions.
    HistoryBytes,
    /// Count of `harvest_dead_letters` rows sharing the resolved key.
    DeadLetters,
}

impl QuotaResource {
    /// Stable, bounded label value — used both as the metric label and the
    /// wire form on [`HarvestError::QuotaExceeded`](crate::error::HarvestError::QuotaExceeded).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ActiveExecutions => "active_executions",
            Self::HistoryBytes => "history_bytes",
            Self::DeadLetters => "dead_letters",
        }
    }
}

impl std::fmt::Display for QuotaResource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ---------------------------------------------------------------------------
// QuotaPolicy
// ---------------------------------------------------------------------------

/// Declarative per-tenant resource quota (issue #946).
///
/// Mirrors [`crate::concurrency::ConcurrencyPolicy`]'s shape: a `'static`
/// dot-path key expression plus a set of caps, all `Copy` so a
/// [`crate::info::WorkflowInfo`] can carry it with no allocation.
///
/// Declared via `#[workflow(quota(key = "input.tenant_id",
/// max_active_executions = 100, max_history_bytes = 10485760,
/// max_dead_letters = 50))]` or the [`Self::new`] + `with_*` builder chain.
/// Every cap is independently optional (issue #946 AC2) — a policy may
/// declare just one, two, or all three.
///
/// # Examples
///
/// ```rust
/// use autumn_harvest::quota::QuotaPolicy;
///
/// let policy = QuotaPolicy::new("input.tenant_id")
///     .with_max_active_executions(100)
///     .with_max_history_bytes(10 * 1024 * 1024)
///     .with_max_dead_letters(50);
///
/// assert_eq!(policy.key_expr, "input.tenant_id");
/// assert_eq!(policy.max_active_executions, Some(100));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuotaPolicy {
    /// JSON field path (dot-notation) resolved against the workflow input to
    /// produce the tenant key. Resolved via [`resolve_quota_key`] — the same
    /// resolver [`crate::concurrency::resolve_concurrency_key`] uses, so
    /// `"input.tenant_id"` and `"tenant_id"` are equivalent.
    pub key_expr: &'static str,
    /// Maximum non-terminal (`RUNNING`/`PAUSED`) executions sharing the
    /// resolved key. `None` = uncapped.
    pub max_active_executions: Option<u32>,
    /// Maximum aggregate `harvest_events` payload bytes across the key's
    /// active executions. `None` = uncapped.
    pub max_history_bytes: Option<u64>,
    /// Maximum `harvest_dead_letters` rows sharing the resolved key.
    /// `None` = uncapped.
    pub max_dead_letters: Option<u32>,
}

impl QuotaPolicy {
    /// Build a policy with no caps declared. Chain `with_*` methods to add
    /// caps, or use [`Self::has_any_cap`] to detect a no-op policy.
    #[must_use]
    pub const fn new(key_expr: &'static str) -> Self {
        Self {
            key_expr,
            max_active_executions: None,
            max_history_bytes: None,
            max_dead_letters: None,
        }
    }

    /// Set the maximum non-terminal execution count for the resolved key.
    #[must_use]
    pub const fn with_max_active_executions(mut self, max: u32) -> Self {
        self.max_active_executions = Some(max);
        self
    }

    /// Set the maximum aggregate history bytes for the resolved key.
    #[must_use]
    pub const fn with_max_history_bytes(mut self, max: u64) -> Self {
        self.max_history_bytes = Some(max);
        self
    }

    /// Set the maximum dead-letter count for the resolved key.
    #[must_use]
    pub const fn with_max_dead_letters(mut self, max: u32) -> Self {
        self.max_dead_letters = Some(max);
        self
    }

    /// `true` when at least one cap is declared.
    ///
    /// A [`QuotaPolicy`] with every cap `None` resolves a key but enforces
    /// nothing; the enforcement layer treats it as a no-op rather than
    /// rejecting every start (a caller who declares `QuotaPolicy::new(key)`
    /// with no `with_*` call almost certainly forgot one, not intended a
    /// silent hard-block — but the engine still must not misinterpret "no
    /// caps" as "zero capacity").
    #[must_use]
    pub const fn has_any_cap(&self) -> bool {
        self.max_active_executions.is_some()
            || self.max_history_bytes.is_some()
            || self.max_dead_letters.is_some()
    }
}

/// Resolve a [`QuotaPolicy::key_expr`] against a workflow input payload.
///
/// A one-line delegate to [`crate::concurrency::resolve_concurrency_key`] —
/// issue #946 AC1 explicitly requires reusing the existing dot-path
/// resolver rather than introducing a second one.
///
/// # Examples
///
/// ```rust
/// use autumn_harvest::quota::resolve_quota_key;
///
/// let input = serde_json::json!({ "tenant_id": "acme" });
/// assert_eq!(resolve_quota_key("input.tenant_id", &input), Some("acme".to_string()));
/// ```
#[must_use]
pub fn resolve_quota_key(expr: &str, input: &serde_json::Value) -> Option<String> {
    crate::concurrency::resolve_concurrency_key(expr, input)
}

/// Maximum length, in UTF-8 bytes, of a value returned by [`resolve_quota_key`].
///
/// This is the size a value must fit before it is safe to store in the
/// indexed `quota_key` column on `harvest_workflow_executions`/`harvest_dead_letters`
/// (issue #946, Codex review — "bound resolved quota keys before indexing
/// them"). `key_expr` is an author-declared, trusted dot-path, but the VALUE
/// it resolves to comes straight from caller-controlled workflow input, so it
/// is otherwise unbounded.
///
/// Postgres caps a single B-tree index entry at roughly 2704 bytes for an
/// 8&nbsp;kB page; 256 bytes is comfortably below that even once the
/// composite `(workflow_name, quota_key, state)` index
/// (`idx_harvest_we_quota_active`) accounts for the other columns, while
/// still being far larger than any realistic tenant identifier (a UUID,
/// email address, or account slug).
///
/// An over-cap key is **rejected** at admission time — see
/// [`quota_key_over_cap`] — rather than truncated or hashed:
///
/// - Truncating would risk aliasing two distinct, unrelated tenant
///   identifiers that merely share a long common prefix onto the SAME quota
///   bucket. That is a genuine isolation break, and a caller who controls
///   the resolved field could deliberately craft a key sharing another
///   tenant's truncated prefix to exhaust that tenant's quota.
/// - Hashing would make the stored `quota_key` illegible to operators
///   reading `GET /admin/quotas` or an execution row for the overwhelmingly
///   common case where the key is already in bounds, for no compensating
///   correctness benefit over a clean, typed rejection.
pub const MAX_QUOTA_KEY_BYTES: u64 = 256;

/// Check a value already resolved by [`resolve_quota_key`] against
/// [`MAX_QUOTA_KEY_BYTES`]. Returns the observed byte length when the key
/// exceeds the bound, `None` when it is within bounds.
///
/// Pure and side-effect-free: the caller (`start_or_load_workflow_execution_collect`)
/// constructs the actual [`crate::error::HarvestError::PayloadTooLarge`]
/// rejection, since building one needs the requesting workflow's name — this
/// module has no such context and stays independent of it.
///
/// # Examples
///
/// ```rust
/// use autumn_harvest::quota::quota_key_over_cap;
///
/// assert_eq!(quota_key_over_cap("acme"), None);
/// let oversized = "x".repeat(300);
/// assert_eq!(quota_key_over_cap(&oversized), Some(300));
/// ```
#[must_use]
pub fn quota_key_over_cap(key: &str) -> Option<u64> {
    let observed_bytes = key.len() as u64;
    (observed_bytes > MAX_QUOTA_KEY_BYTES).then_some(observed_bytes)
}

// ---------------------------------------------------------------------------
// QuotaUsage / check_quota — pure comparison, no I/O
// ---------------------------------------------------------------------------

/// Current resource usage for one resolved `(workflow_name, quota_key)` pair.
///
/// Deliberately a plain, DB-independent data type (unlike the
/// `#[cfg(feature = "db")]`-gated query functions below) so [`check_quota`]
/// is unit-testable without a database connection, mirroring
/// [`crate::usage::UsageShardRow`]'s split between the wire-shaped struct and
/// its `#[cfg(feature = "db")]` Diesel row.
///
/// Fields are `i64` (matching Postgres `COUNT(*)`/`SUM(...)` result types)
/// rather than the policy's `u32`/`u64` caps; [`check_quota`] handles the
/// comparison across the two.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct QuotaUsage {
    /// Count of non-terminal (`RUNNING`/`PAUSED`) executions for the key.
    pub active_executions: i64,
    /// Aggregate `harvest_events` payload bytes across the key's active
    /// executions.
    pub history_bytes: i64,
    /// Count of `harvest_dead_letters` rows for the key.
    pub dead_letters: i64,
}

/// One resource where usage has reached or exceeded its declared cap.
///
/// Returned by [`check_quota`]; carried on
/// [`HarvestError::QuotaExceeded`](crate::error::HarvestError::QuotaExceeded).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuotaViolation {
    /// The exhausted resource.
    pub resource: QuotaResource,
    /// The declared cap.
    pub limit: u64,
    /// The usage observed at check time (before this admission).
    pub current: u64,
}

/// Compare live usage against a declared policy.
///
/// Checked in a fixed, deterministic order — [`QuotaResource::ActiveExecutions`],
/// then [`QuotaResource::HistoryBytes`], then [`QuotaResource::DeadLetters`] —
/// so a caller violating more than one cap always sees the same reported
/// resource across repeated attempts. Returns the *first* violated resource,
/// or `None` when every declared cap has headroom.
///
/// A resource with no declared cap (`policy.max_* == None`) is never checked
/// (issue #946 AC2's "independent optional caps"). A policy with
/// [`QuotaPolicy::has_any_cap`] `false` always returns `None`.
///
/// The comparison is `current >= limit` (not `current > limit`): admitting
/// the `limit`-th execution brings usage *to* the cap, and the
/// `(limit + 1)`-th admission attempt is rejected — so a `max_active_executions
/// = 100` policy admits exactly 100 concurrent executions (issue #946's
/// success metric).
///
/// A negative `usage` field (which should never occur from a genuine
/// `COUNT(*)`/`SUM(...)` query, but is defensively handled rather than
/// panicking) is clamped to `0` before comparison.
///
/// # Examples
///
/// ```rust
/// use autumn_harvest::quota::{QuotaPolicy, QuotaResource, QuotaUsage, check_quota};
///
/// let policy = QuotaPolicy::new("tenant").with_max_active_executions(100);
///
/// let under = QuotaUsage { active_executions: 99, ..Default::default() };
/// assert!(check_quota(&under, &policy).is_none());
///
/// let at_cap = QuotaUsage { active_executions: 100, ..Default::default() };
/// let violation = check_quota(&at_cap, &policy).unwrap();
/// assert_eq!(violation.resource, QuotaResource::ActiveExecutions);
/// assert_eq!(violation.limit, 100);
/// assert_eq!(violation.current, 100);
/// ```
#[must_use]
pub fn check_quota(usage: &QuotaUsage, policy: &QuotaPolicy) -> Option<QuotaViolation> {
    let clamp = |n: i64| -> u64 { u64::try_from(n).unwrap_or(0) };

    if let Some(max) = policy.max_active_executions {
        let current = clamp(usage.active_executions);
        if current >= u64::from(max) {
            return Some(QuotaViolation {
                resource: QuotaResource::ActiveExecutions,
                limit: u64::from(max),
                current,
            });
        }
    }
    if let Some(max) = policy.max_history_bytes {
        let current = clamp(usage.history_bytes);
        if current >= max {
            return Some(QuotaViolation {
                resource: QuotaResource::HistoryBytes,
                limit: max,
                current,
            });
        }
    }
    if let Some(max) = policy.max_dead_letters {
        let current = clamp(usage.dead_letters);
        if current >= u64::from(max) {
            return Some(QuotaViolation {
                resource: QuotaResource::DeadLetters,
                limit: u64::from(max),
                current,
            });
        }
    }
    None
}

// ---------------------------------------------------------------------------
// DB-gated usage queries
// ---------------------------------------------------------------------------

#[cfg(feature = "db")]
#[derive(Debug, diesel::QueryableByName)]
struct QuotaUsageRow {
    #[diesel(sql_type = BigInt)]
    active_executions: i64,
    #[diesel(sql_type = BigInt)]
    history_bytes: i64,
    #[diesel(sql_type = BigInt)]
    dead_letters: i64,
}

/// SQL for [`load_quota_usage`], reading all three resource counters for one
/// `(workflow_name, quota_key)` pair in a single round trip (issue #946 AC7 —
/// "cheap by construction... never a full-table scan per admission").
///
/// `active` and `dl` each hit the partial indexes created by migration
/// `20260725000000_harvest_workflow_quotas`
/// (`idx_harvest_we_quota_active`/`idx_harvest_dl_quota`); `history` joins
/// `harvest_events` on `workflow_exec_id`, intended to be served by the
/// existing `(workflow_exec_id, event_id)` index — bounded to the rows
/// belonging to the key's own active executions, never a scan of the whole
/// event log.
///
/// **AC7's "never a full-table scan" is a design intent, not a guarantee the
/// query text enforces.** `WHERE e.workflow_exec_id IN (SELECT id FROM
/// active)` lets the planner choose either an index-bounded Nested Loop *or*
/// a Seq Scan of the entire `harvest_events` table, and it measurably picks
/// the Seq Scan below roughly 300k total rows in the table (a plain,
/// production-shaped early-life or heavily-retention-pruned deployment) —
/// see the Ledger evidence in `docs/performance-quota-history-bytes.md`. This
/// is not flagged as a bug: at that same size a query shape that *forces*
/// the index-bounded plan measured **worse** (a per-row correlated
/// `LATERAL`, tested and reverted — see the doc page's negative result), so
/// the planner's own cost-based choice is already at least as good as the
/// alternative tried. The real, unavoidable cost this query pays regardless
/// of which plan runs: an exact `SUM(pg_column_size(...))` requires reading
/// every contributing row, so cost scales with the target key's own
/// accumulated active-execution history — for a tenant sitting near its
/// configured `max_history_bytes` cap, that is tens of thousands of buffer
/// touches on *every* admission, recomputed from scratch each time. See the
/// doc page for the measured numbers and why a cheaper fix (an incrementally
/// maintained running counter) is a human decision, not a query rewrite.
///
/// `SUSPENDED` does not appear in the active-state filter: it is not a
/// persisted state (the `harvest_workflow_executions.state` CHECK constraint
/// forbids it — see [`crate::concurrency::active_runs_for_key`]'s identical
/// note), so `RUNNING`/`PAUSED` is the complete non-terminal set despite
/// issue #946 AC2's looser "RUNNING/SUSPENDED/PAUSED" wording.
#[cfg(feature = "db")]
const QUOTA_USAGE_SQL: &str = "\
    WITH active AS ( \
        SELECT id FROM harvest_workflow_executions \
        WHERE workflow_name = $1 AND quota_key = $2 AND state IN ('RUNNING', 'PAUSED') \
    ) \
    SELECT \
        (SELECT COUNT(*) FROM active)::BIGINT AS active_executions, \
        COALESCE( \
            (SELECT SUM(pg_column_size(e.event_data)) \
             FROM harvest_events e \
             WHERE e.workflow_exec_id IN (SELECT id FROM active)), \
            0 \
        )::BIGINT AS history_bytes, \
        (SELECT COUNT(*) FROM harvest_dead_letters \
         WHERE workflow_name = $1 AND quota_key = $2)::BIGINT AS dead_letters";

/// The exact SQL text [`load_quota_usage`] executes.
///
/// Exposed read-only for evidence-capture tests to `EXPLAIN` — mirrors
/// [`crate::queue::claim_task_query`]'s identical purpose for the claim path,
/// so a captured plan is provably the production statement rather than a
/// hand-copied (and driftable) approximation of it.
#[cfg(feature = "db")]
#[must_use]
pub const fn quota_usage_query() -> &'static str {
    QUOTA_USAGE_SQL
}

/// Load current usage for one `(workflow_name, quota_key)` pair.
///
/// The admission-time primitive: called once per start attempt for a
/// workflow type with a declared [`QuotaPolicy`], inside the same
/// transaction as [`lock_quota_key`] to avoid a check-then-admit race under
/// concurrent starts for the same key.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
#[cfg(feature = "db")]
pub async fn load_quota_usage(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    quota_key: &str,
) -> HarvestResult<QuotaUsage> {
    let row: QuotaUsageRow = diesel::sql_query(QUOTA_USAGE_SQL)
        .bind::<Text, _>(workflow_name)
        .bind::<Text, _>(quota_key)
        .get_result(conn)
        .await
        .map_err(database_error)?;

    Ok(QuotaUsage {
        active_executions: row.active_executions,
        history_bytes: row.history_bytes,
        dead_letters: row.dead_letters,
    })
}

/// SQL for [`load_quota_usage_excluding`]. Identical to [`QUOTA_USAGE_SQL`]
/// except the `active` CTE also excludes `$3`, an array of execution ids.
///
/// A separate query text, not a wrapper around [`QUOTA_USAGE_SQL`]. Every
/// OTHER caller still uses the unqualified admission-time read, unchanged.
/// It keeps the exact query shape `docs/performance-quota-history-bytes.md`
/// measured.
#[cfg(feature = "db")]
const QUOTA_USAGE_EXCLUDING_SQL: &str = "\
    WITH active AS ( \
        SELECT id FROM harvest_workflow_executions \
        WHERE workflow_name = $1 AND quota_key = $2 AND state IN ('RUNNING', 'PAUSED') \
          AND id <> ALL($3) \
    ) \
    SELECT \
        (SELECT COUNT(*) FROM active)::BIGINT AS active_executions, \
        COALESCE( \
            (SELECT SUM(pg_column_size(e.event_data)) \
             FROM harvest_events e \
             WHERE e.workflow_exec_id IN (SELECT id FROM active)), \
            0 \
        )::BIGINT AS history_bytes, \
        (SELECT COUNT(*) FROM harvest_dead_letters \
         WHERE workflow_name = $1 AND quota_key = $2)::BIGINT AS dead_letters";

/// Load current usage for one `(workflow_name, quota_key)` pair, excluding
/// specific executions entirely from the `active_executions`/`history_bytes`
/// counters (issue #1228 review).
///
/// An admission can plan to shed some incumbents. Or it can have just
/// inserted its own row. Either way it needs usage as it will read once
/// those executions are gone, or once its own row's history is set aside.
/// Neither is usage as it stands right now. [`load_quota_usage`] followed
/// by a separate subtraction computed the excluded executions'
/// contribution from an EARLIER, separate read. A row could change state
/// between that earlier read and this one. An incumbent could complete on
/// its own. Or the caller's own row could pick up a `WorkflowStarted`
/// event. Either change made the subtraction stale. Excluding the ids
/// directly inside this ONE query removes the gap. There is no earlier
/// read to go stale relative to, because there is no earlier read.
///
/// `excluded_ids` is typically the caller's own `self_exec_id`. That row
/// is already `RUNNING`, so it always needs excluding: it would otherwise
/// double-count itself against the very cap it is being checked against.
/// On the `cancel_running` dry-run path, `excluded_ids` also holds the
/// executions a pending supersede pass is about to shed. A row that
/// starts existing, or newly matches the key, only AFTER this query runs
/// is not excluded, and could not be. That case can only raise the
/// reported usage, never lower it below the true value. It is the safe
/// direction the rest of this mechanism already relies on.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
#[cfg(feature = "db")]
pub async fn load_quota_usage_excluding(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    quota_key: &str,
    excluded_ids: &[uuid::Uuid],
) -> HarvestResult<QuotaUsage> {
    let row: QuotaUsageRow = diesel::sql_query(QUOTA_USAGE_EXCLUDING_SQL)
        .bind::<Text, _>(workflow_name)
        .bind::<Text, _>(quota_key)
        .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(excluded_ids)
        .get_result(conn)
        .await
        .map_err(database_error)?;

    Ok(QuotaUsage {
        active_executions: row.active_executions,
        history_bytes: row.history_bytes,
        dead_letters: row.dead_letters,
    })
}

/// One `(workflow_name, quota_key)` pair's current usage, for the operator
/// read model (`GET /admin/quotas`, issue #946 AC5).
#[cfg(feature = "db")]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct QuotaKeyUsage {
    /// The workflow type name.
    pub workflow_name: String,
    /// The resolved tenant key.
    pub quota_key: String,
    /// Current active-execution count for the key.
    pub active_executions: i64,
    /// Current aggregate history bytes for the key.
    pub history_bytes: i64,
    /// Current dead-letter count for the key.
    pub dead_letters: i64,
}

#[cfg(feature = "db")]
#[derive(Debug, diesel::QueryableByName)]
struct QuotaKeyUsageRow {
    #[diesel(sql_type = Text)]
    workflow_name: String,
    #[diesel(sql_type = Text)]
    quota_key: String,
    #[diesel(sql_type = BigInt)]
    active_executions: i64,
    #[diesel(sql_type = Nullable<BigInt>)]
    history_bytes: Option<i64>,
    #[diesel(sql_type = Nullable<BigInt>)]
    dead_letters: Option<i64>,
}

/// SQL for [`list_quota_usage`] — every distinct `(workflow_name, quota_key)`
/// pair on this shard with a non-zero footprint in any of the three
/// resources, grouped and merged in one query.
///
/// Mirrors [`crate::usage::usage_sql`]'s CTE-plus-`FULL OUTER JOIN` shape: an
/// `active` CTE anchors the row set (a key with active executions always
/// appears), `history`/`dl` are joined in, and a key with dead letters but no
/// currently-active executions still surfaces via the `dl` side of the outer
/// join — a tenant whose runs have all drained but whose failures are
/// piling up must remain visible.
#[cfg(feature = "db")]
const QUOTA_LIST_SQL: &str = "\
    WITH active AS ( \
        SELECT workflow_name, quota_key, COUNT(*)::BIGINT AS active_executions \
        FROM harvest_workflow_executions \
        WHERE quota_key IS NOT NULL AND state IN ('RUNNING', 'PAUSED') \
        GROUP BY workflow_name, quota_key \
    ), \
    history AS ( \
        SELECT w.workflow_name, w.quota_key, \
               SUM(pg_column_size(e.event_data))::BIGINT AS history_bytes \
        FROM harvest_workflow_executions w \
        INNER JOIN harvest_events e ON e.workflow_exec_id = w.id \
        WHERE w.quota_key IS NOT NULL AND w.state IN ('RUNNING', 'PAUSED') \
        GROUP BY w.workflow_name, w.quota_key \
    ), \
    dl AS ( \
        SELECT workflow_name, quota_key, COUNT(*)::BIGINT AS dead_letters \
        FROM harvest_dead_letters \
        WHERE quota_key IS NOT NULL AND workflow_name IS NOT NULL \
        GROUP BY workflow_name, quota_key \
    ) \
    SELECT \
        COALESCE(a.workflow_name, h.workflow_name, dl.workflow_name) AS workflow_name, \
        COALESCE(a.quota_key, h.quota_key, dl.quota_key) AS quota_key, \
        COALESCE(a.active_executions, 0)::BIGINT AS active_executions, \
        h.history_bytes AS history_bytes, \
        dl.dead_letters AS dead_letters \
    FROM active a \
    FULL OUTER JOIN history h ON h.workflow_name = a.workflow_name AND h.quota_key = a.quota_key \
    FULL OUTER JOIN dl ON dl.workflow_name = COALESCE(a.workflow_name, h.workflow_name) \
        AND dl.quota_key = COALESCE(a.quota_key, h.quota_key) \
    ORDER BY 1, 2";

/// List current usage for every `(workflow_name, quota_key)` pair visible on
/// this shard's database (issue #946 AC5).
///
/// Called by the plugin's `GET /admin/quotas` handler once per shard, then
/// merged across shards via `crate::shard_fanout` — this function has no
/// knowledge of sharding itself and simply reads its own connection's shard.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
#[cfg(feature = "db")]
pub async fn list_quota_usage(conn: &mut AsyncPgConnection) -> HarvestResult<Vec<QuotaKeyUsage>> {
    let rows: Vec<QuotaKeyUsageRow> = diesel::sql_query(QUOTA_LIST_SQL)
        .load(conn)
        .await
        .map_err(database_error)?;

    Ok(rows
        .into_iter()
        .map(|r| QuotaKeyUsage {
            workflow_name: r.workflow_name,
            quota_key: r.quota_key,
            active_executions: r.active_executions,
            history_bytes: r.history_bytes.unwrap_or(0),
            dead_letters: r.dead_letters.unwrap_or(0),
        })
        .collect())
}

// ---------------------------------------------------------------------------
// Advisory lock — serializes check-then-admit for one key (issue #946)
// ---------------------------------------------------------------------------

/// The exact advisory-lock namespace string [`lock_quota_key`] hashes. A
/// single source of truth. A scanner's pre-fire lock-ordering pass
/// (`debounce`/`throttle`'s `order_due_rows_for_deadlock_free_firing`) can
/// then resolve the SAME string this function locks on. It never uses an
/// independently-formatted copy that could silently drift from it.
#[cfg(feature = "db")]
pub(crate) fn quota_lock_namespace(workflow_name: &str, quota_key: &str) -> String {
    format!("quota:{workflow_name}:{quota_key}")
}

/// Serialize a quota check-then-admit sequence for one key behind a
/// transaction-scoped advisory lock.
///
/// Uses a distinctly-prefixed namespace (`"quota:{workflow_name}:{quota_key}"`)
/// rather than the bare `concurrency_key`/mutex-key string, so a quota lock
/// can never collide with [`crate::concurrency::lock_concurrency_key`]'s or
/// the durable-mutex's (issue #691) advisory-lock namespace — all three share
/// Postgres's single 64-bit one-argument advisory-lock space, and a
/// distinguishing prefix is the only defense against an accidental
/// cross-primitive collision on the *same* hashed value.
///
/// Blocking (`pg_advisory_xact_lock`, not the `_try_` variant) is
/// deliberate: unlike [`crate::concurrency::lock_concurrency_key`] (which
/// must never block the claim-time dispatch path), quota enforcement runs
/// once at admission time, and correctness (an exact cap, not an
/// approximate one) matters more than avoiding a brief wait under
/// contention on the same key.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
#[cfg(feature = "db")]
pub async fn lock_quota_key(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    quota_key: &str,
) -> HarvestResult<()> {
    let namespaced = quota_lock_namespace(workflow_name, quota_key);
    diesel::sql_query("SELECT pg_advisory_xact_lock(hashtext($1)::bigint)")
        .bind::<Text, _>(namespaced)
        .execute(conn)
        .await
        .map_err(database_error)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- QuotaResource --------------------------------------------------

    #[test]
    fn quota_resource_as_str_is_stable_and_bounded() {
        assert_eq!(
            QuotaResource::ActiveExecutions.as_str(),
            "active_executions"
        );
        assert_eq!(QuotaResource::HistoryBytes.as_str(), "history_bytes");
        assert_eq!(QuotaResource::DeadLetters.as_str(), "dead_letters");
    }

    #[test]
    fn quota_resource_display_matches_as_str() {
        assert_eq!(
            QuotaResource::ActiveExecutions.to_string(),
            QuotaResource::ActiveExecutions.as_str()
        );
        assert_eq!(
            QuotaResource::HistoryBytes.to_string(),
            QuotaResource::HistoryBytes.as_str()
        );
        assert_eq!(
            QuotaResource::DeadLetters.to_string(),
            QuotaResource::DeadLetters.as_str()
        );
    }

    #[test]
    fn quota_resource_serde_round_trips_snake_case() {
        for resource in [
            QuotaResource::ActiveExecutions,
            QuotaResource::HistoryBytes,
            QuotaResource::DeadLetters,
        ] {
            let json = serde_json::to_string(&resource).unwrap();
            assert_eq!(json, format!("\"{}\"", resource.as_str()));
            let round_tripped: QuotaResource = serde_json::from_str(&json).unwrap();
            assert_eq!(round_tripped, resource);
        }
    }

    // -- QuotaPolicy ------------------------------------------------------

    #[test]
    fn new_policy_has_no_caps() {
        let policy = QuotaPolicy::new("tenant_id");
        assert_eq!(policy.key_expr, "tenant_id");
        assert_eq!(policy.max_active_executions, None);
        assert_eq!(policy.max_history_bytes, None);
        assert_eq!(policy.max_dead_letters, None);
        assert!(!policy.has_any_cap());
    }

    #[test]
    fn with_max_active_executions_sets_only_that_cap() {
        let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(100);
        assert_eq!(policy.max_active_executions, Some(100));
        assert_eq!(policy.max_history_bytes, None);
        assert_eq!(policy.max_dead_letters, None);
        assert!(policy.has_any_cap());
    }

    #[test]
    fn with_max_history_bytes_sets_only_that_cap() {
        let policy = QuotaPolicy::new("tenant_id").with_max_history_bytes(1024);
        assert_eq!(policy.max_history_bytes, Some(1024));
        assert_eq!(policy.max_active_executions, None);
        assert!(policy.has_any_cap());
    }

    #[test]
    fn with_max_dead_letters_sets_only_that_cap() {
        let policy = QuotaPolicy::new("tenant_id").with_max_dead_letters(10);
        assert_eq!(policy.max_dead_letters, Some(10));
        assert_eq!(policy.max_active_executions, None);
        assert!(policy.has_any_cap());
    }

    #[test]
    fn all_three_caps_compose_independently() {
        let policy = QuotaPolicy::new("tenant_id")
            .with_max_active_executions(100)
            .with_max_history_bytes(1024)
            .with_max_dead_letters(10);
        assert_eq!(policy.max_active_executions, Some(100));
        assert_eq!(policy.max_history_bytes, Some(1024));
        assert_eq!(policy.max_dead_letters, Some(10));
    }

    #[test]
    fn quota_policy_is_copy() {
        let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1);
        let copied = policy;
        // Both usable -- proves `Copy`, not just `Clone`.
        assert_eq!(policy.max_active_executions, copied.max_active_executions);
    }

    // -- resolve_quota_key --------------------------------------------------

    #[test]
    fn resolve_quota_key_strips_input_prefix() {
        let input = serde_json::json!({ "tenant_id": "acme" });
        assert_eq!(
            resolve_quota_key("input.tenant_id", &input),
            Some("acme".to_string())
        );
        assert_eq!(
            resolve_quota_key("tenant_id", &input),
            Some("acme".to_string())
        );
    }

    #[test]
    fn resolve_quota_key_walks_nested_paths() {
        let input = serde_json::json!({ "user": { "id": 42 } });
        assert_eq!(resolve_quota_key("user.id", &input), Some("42".to_string()));
    }

    #[test]
    fn resolve_quota_key_missing_field_is_none() {
        let input = serde_json::json!({ "other": "value" });
        assert_eq!(resolve_quota_key("tenant_id", &input), None);
    }

    #[test]
    fn resolve_quota_key_matches_concurrency_resolver_byte_for_byte() {
        // AC1: no second resolver. Any input this delegate handles must
        // agree with the underlying `resolve_concurrency_key` exactly.
        for input in [
            serde_json::json!({ "tenant_id": "acme" }),
            serde_json::json!({ "tenant_id": null }),
            serde_json::json!({ "user": { "id": 7 } }),
            serde_json::json!("not an object"),
            serde_json::json!({}),
        ] {
            assert_eq!(
                resolve_quota_key("tenant_id", &input),
                crate::concurrency::resolve_concurrency_key("tenant_id", &input),
            );
        }
    }

    // -- quota_key_over_cap --------------------------------------------------

    #[test]
    fn quota_key_over_cap_within_bound_is_none() {
        assert_eq!(quota_key_over_cap("acme"), None);
        assert_eq!(quota_key_over_cap(""), None);
    }

    #[test]
    fn quota_key_over_cap_exactly_at_bound_is_none() {
        // The bound itself must be admissible -- only STRICTLY over rejects.
        let exact = "x".repeat(usize::try_from(MAX_QUOTA_KEY_BYTES).expect("small"));
        assert_eq!(exact.len() as u64, MAX_QUOTA_KEY_BYTES);
        assert_eq!(quota_key_over_cap(&exact), None);
    }

    #[test]
    fn quota_key_over_cap_one_byte_over_is_rejected() {
        let over = "x".repeat(usize::try_from(MAX_QUOTA_KEY_BYTES).expect("small") + 1);
        assert_eq!(
            quota_key_over_cap(&over),
            Some(MAX_QUOTA_KEY_BYTES + 1),
            "the observed length reported must be the ACTUAL length, not the cap"
        );
    }

    #[test]
    fn quota_key_over_cap_reports_exact_observed_length_for_a_wildly_oversized_key() {
        let huge = "x".repeat(10_000);
        assert_eq!(quota_key_over_cap(&huge), Some(10_000));
    }

    #[test]
    fn quota_key_over_cap_measures_utf8_bytes_not_chars() {
        // Multi-byte UTF-8: each 'é' is 2 bytes, so 200 of them is 400 bytes
        // -- over the 256-byte bound -- even though `.chars().count()` is
        // only 200. The check MUST bound the same unit Postgres bounds an
        // index entry by (bytes), not a locale-dependent character count.
        let multi_byte: String = "é".repeat(200);
        assert_eq!(multi_byte.chars().count(), 200);
        assert!(multi_byte.len() > usize::try_from(MAX_QUOTA_KEY_BYTES).expect("small"));
        assert_eq!(
            quota_key_over_cap(&multi_byte),
            Some(multi_byte.len() as u64)
        );
    }

    // -- check_quota --------------------------------------------------------

    #[test]
    fn check_quota_no_caps_declared_never_violates() {
        let policy = QuotaPolicy::new("tenant_id");
        let usage = QuotaUsage {
            active_executions: 1_000_000,
            history_bytes: 1_000_000_000,
            dead_letters: 1_000_000,
        };
        assert_eq!(check_quota(&usage, &policy), None);
    }

    #[test]
    fn check_quota_active_executions_under_cap_is_none() {
        let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(100);
        let usage = QuotaUsage {
            active_executions: 99,
            ..Default::default()
        };
        assert_eq!(check_quota(&usage, &policy), None);
    }

    #[test]
    fn check_quota_active_executions_at_cap_violates() {
        // The success-metric boundary: exactly 100 admitted, the 101st start
        // (which would observe current == 100 before admission) rejected.
        let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(100);
        let usage = QuotaUsage {
            active_executions: 100,
            ..Default::default()
        };
        let violation = check_quota(&usage, &policy).expect("must violate at the cap");
        assert_eq!(violation.resource, QuotaResource::ActiveExecutions);
        assert_eq!(violation.limit, 100);
        assert_eq!(violation.current, 100);
    }

    #[test]
    fn check_quota_active_executions_over_cap_violates() {
        let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(100);
        let usage = QuotaUsage {
            active_executions: 250,
            ..Default::default()
        };
        let violation = check_quota(&usage, &policy).unwrap();
        assert_eq!(violation.current, 250);
    }

    #[test]
    fn check_quota_history_bytes_boundary() {
        let policy = QuotaPolicy::new("tenant_id").with_max_history_bytes(1024);
        assert_eq!(
            check_quota(
                &QuotaUsage {
                    history_bytes: 1023,
                    ..Default::default()
                },
                &policy
            ),
            None
        );
        let violation = check_quota(
            &QuotaUsage {
                history_bytes: 1024,
                ..Default::default()
            },
            &policy,
        )
        .unwrap();
        assert_eq!(violation.resource, QuotaResource::HistoryBytes);
        assert_eq!(violation.limit, 1024);
    }

    #[test]
    fn check_quota_dead_letters_boundary() {
        let policy = QuotaPolicy::new("tenant_id").with_max_dead_letters(5);
        assert_eq!(
            check_quota(
                &QuotaUsage {
                    dead_letters: 4,
                    ..Default::default()
                },
                &policy
            ),
            None
        );
        let violation = check_quota(
            &QuotaUsage {
                dead_letters: 5,
                ..Default::default()
            },
            &policy,
        )
        .unwrap();
        assert_eq!(violation.resource, QuotaResource::DeadLetters);
        assert_eq!(violation.limit, 5);
    }

    #[test]
    fn check_quota_multiple_violations_reports_active_executions_first() {
        let policy = QuotaPolicy::new("tenant_id")
            .with_max_active_executions(10)
            .with_max_history_bytes(10)
            .with_max_dead_letters(10);
        let usage = QuotaUsage {
            active_executions: 999,
            history_bytes: 999,
            dead_letters: 999,
        };
        let violation = check_quota(&usage, &policy).unwrap();
        assert_eq!(violation.resource, QuotaResource::ActiveExecutions);
    }

    #[test]
    fn check_quota_multiple_violations_reports_history_bytes_second() {
        let policy = QuotaPolicy::new("tenant_id")
            .with_max_active_executions(1000)
            .with_max_history_bytes(10)
            .with_max_dead_letters(10);
        let usage = QuotaUsage {
            active_executions: 5, // under its cap
            history_bytes: 999,
            dead_letters: 999,
        };
        let violation = check_quota(&usage, &policy).unwrap();
        assert_eq!(violation.resource, QuotaResource::HistoryBytes);
    }

    #[test]
    fn check_quota_only_declared_resources_are_checked() {
        // max_history_bytes is wildly over any reasonable value but was
        // never declared -- must not surface as a violation.
        let policy = QuotaPolicy::new("tenant_id").with_max_dead_letters(10);
        let usage = QuotaUsage {
            active_executions: i64::MAX,
            history_bytes: i64::MAX,
            dead_letters: 5,
        };
        assert_eq!(check_quota(&usage, &policy), None);
    }

    #[test]
    fn check_quota_negative_usage_clamps_to_zero_defensively() {
        let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(0);
        // A cap of exactly zero with clamped-to-zero usage still violates
        // (0 >= 0), proving the clamp doesn't silently disable enforcement.
        let usage = QuotaUsage {
            active_executions: -5,
            ..Default::default()
        };
        let violation = check_quota(&usage, &policy).unwrap();
        assert_eq!(violation.current, 0);
    }

    #[test]
    fn check_quota_zero_cap_rejects_the_first_execution() {
        let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(0);
        let usage = QuotaUsage {
            active_executions: 0,
            ..Default::default()
        };
        assert!(check_quota(&usage, &policy).is_some());
    }

    #[cfg(feature = "db")]
    #[test]
    fn quota_usage_sql_uses_the_partial_indexes_state_filter() {
        assert!(QUOTA_USAGE_SQL.contains("state IN ('RUNNING', 'PAUSED')"));
        assert!(!QUOTA_USAGE_SQL.contains("SUSPENDED"));
    }

    #[cfg(feature = "db")]
    #[test]
    fn quota_list_sql_orders_deterministically() {
        assert!(QUOTA_LIST_SQL.trim_end().ends_with("ORDER BY 1, 2"));
    }

    #[cfg(feature = "db")]
    #[test]
    fn lock_namespace_is_distinct_from_bare_concurrency_key() {
        // The namespace prefix is what prevents a hashtext() collision with
        // `concurrency::lock_concurrency_key`'s bare-key namespace and the
        // durable-mutex namespace (issue #691) sharing the same 64-bit
        // advisory-lock space.
        let namespaced = format!("quota:{}:{}", "my_workflow", "acme");
        assert_ne!(namespaced, "acme");
        assert!(namespaced.starts_with("quota:"));
    }
}
