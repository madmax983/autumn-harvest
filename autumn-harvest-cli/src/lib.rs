//! Command-line client for the autumn-harvest management API.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use autumn_harvest::backup_verify::{
    Finding, FindingSeverity, RestoreVerifyReport, ShardTarget, VerifyOptions, VerifyStatus,
    dsn_targets_same_database, redact_dsn, verify_restore,
};
use autumn_harvest::migrate::{
    MigrationPlan, MigrationReport, MigrationScript, UnserializedReason,
};
use autumn_harvest::testing::WorkflowReplayer;
use autumn_harvest::{
    AcknowledgedBreakingChange, DetCheckReport, DetSeverity, SchemaContractDiff, SchemaDelta,
    SchemaRole, WorkflowSchemaContract, check_paths, diff_schema_contracts,
    dropped_acknowledgements, unacknowledged_breaking,
};
use clap::{Parser, Subcommand, ValueEnum};
use diesel_async::AsyncPgConnection;
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use serde_json::{Map, Value, json};
use thiserror::Error;

const DEFAULT_BASE_URL: &str = "http://localhost:3000/api/harvest";
/// Characters percent-encoded when a caller-supplied value becomes one URL path
/// segment.
///
/// `/` and `\` are both here because the URL parser reqwest uses treats **both**
/// as path separators for special (http/https) URLs, so either one would split a
/// single value into extra segments and silently retarget the request at a
/// different route — and `\` additionally re-enables `..` traversal inside what
/// should be one opaque segment (`payments\..\admin` resolved to
/// `/admin/queues/admin/pause`). `%` is here so a caller-supplied `%2e` cannot
/// become a dot-segment; the LITERAL `.`/`..` forms cannot be encoded away at
/// all and are rejected instead — see `is_url_dot_segment`.
const PATH_SEGMENT_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'<')
    .add(b'>')
    .add(b'?')
    .add(b'`')
    .add(b'{')
    .add(b'}')
    .add(b'/')
    .add(b'\\');

/// Top-level CLI arguments for the `harvest` binary.
#[derive(Debug, Parser)]
#[command(
    name = "harvest",
    version,
    about = "Manage autumn-harvest workflows and DAGs"
)]
pub struct Cli {
    /// Base URL where the Harvest management API is mounted.
    #[arg(
        long,
        global = true,
        env = "HARVEST_URL",
        default_value = DEFAULT_BASE_URL
    )]
    base_url: String,

    /// Bearer token to send with every request.
    #[arg(long, global = true, env = "HARVEST_TOKEN", hide_env_values = true)]
    token: Option<String>,

    /// Operator identity recorded in the audit trail for mutating commands.
    /// Only sent on POST/PATCH/DELETE requests as `x-harvest-actor`.
    /// If omitted, the server defaults to `"anonymous"` (acceptable only for dev).
    #[arg(long, global = true, env = "HARVEST_ACTOR")]
    actor: Option<String>,

    /// Correlation request-id forwarded as `x-request-id` on mutating commands.
    #[arg(long, global = true, env = "HARVEST_REQUEST_ID")]
    request_id: Option<String>,

    /// Output format for successful API responses.
    #[arg(long, global = true, value_enum, default_value = "pretty-json")]
    output: OutputFormat,

    #[command(subcommand)]
    command: Commands,
}

/// Successful response output format.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum OutputFormat {
    /// Pretty-printed JSON.
    PrettyJson,
    /// Compact JSON for scripts.
    Json,
}

/// Cross-region disaster-recovery operator commands (issue #954).
///
/// All three talk to shard databases directly, never to the management API:
/// during a regional failover the management API may be exactly what is down,
/// and the whole point of the fence is to be reachable when the region is not.
#[derive(Subcommand, Debug)]
pub enum DrCommand {
    /// Report each shard's write-authority epoch and measured RPO.
    ///
    /// Read-only, safe at any time, and the first thing to run when deciding
    /// whether to fail over: it shows the RPO you would be accepting.
    Status {
        /// A shard database DSN. Repeat once per shard. Accepts a bare DSN or
        /// an explicit `<shard_id>=<dsn>` pair; an unprefixed DSN takes its
        /// POSITIONAL index as its shard id.
        #[arg(long = "shard", value_name = "DSN", required = true)]
        shards: Vec<String>,

        /// Slot-name prefix identifying this shard's DR replication.
        ///
        /// Must match the workers' `replication_slot_prefix`. Without a prefix
        /// every walsender for the database would count as a DR standby —
        /// including an unrelated logical-decoding consumer such as a CDC
        /// pipeline — so a shard whose real cross-region subscriber had
        /// disconnected would report itself protected.
        #[arg(long, value_name = "PREFIX", default_value = "harvest_dr")]
        slot_prefix: String,

        /// Output format.
        #[arg(long, short = 'o', value_enum, default_value = "text")]
        format: DrFormat,
    },

    /// **Revoke the old region's write authority**: bump each shard's epoch.
    ///
    /// This is the fence. Every worker pinned to the previous epoch — in
    /// EITHER region — stops. Run it on the promoted primary, on every shard,
    /// before starting any workers.
    Fence {
        /// A shard database DSN. Repeat once per shard.
        ///
        /// Supply EVERY shard. Fencing a subset leaves a half-failed-over
        /// cluster taking live cross-shard traffic, which converts bounded,
        /// known skew into unbounded skew (see docs/cross-region-dr.md).
        #[arg(long = "shard", value_name = "DSN", required = true)]
        shards: Vec<String>,

        /// Why this fence is being applied. Recorded in
        /// `harvest_shard_generation.fenced_reason`.
        ///
        /// Required, not optional: an unattributable epoch bump found three
        /// months later is indistinguishable from a mistake.
        #[arg(long, value_name = "TEXT", required = true)]
        reason: String,

        /// Who is applying it. Recorded in `fenced_by`.
        #[arg(
            long,
            value_name = "NAME",
            env = "HARVEST_ACTOR",
            default_value = "unknown"
        )]
        actor: String,

        /// Acknowledge that this stops every worker pinned to the old epoch.
        ///
        /// Deliberately long and unpleasant to type. A fence during a healthy
        /// week is a fleet-wide outage recovered only by restarting the fleet.
        #[arg(long = "i-understand-this-stops-the-fleet", required = true)]
        confirm: bool,

        /// Fence a shard that has no fencing row yet, creating one.
        ///
        /// Off by default, and that default is a safety guard rather than
        /// tidiness. A shard with no row has never been pinned by a worker, so
        /// there is nothing to fence — and the overwhelmingly likely cause of
        /// an absent row is a **wrong shard id**: `--shard <dsn>` with no
        /// `<id>=` prefix takes its *positional index* as the shard id, so one
        /// mis-ordered DSN silently creates a phantom row, bumps it, prints
        /// success, and fences nobody while the operator believes the region is
        /// fenced. Refusing is the loud outcome.
        #[arg(long)]
        provision: bool,

        /// Fence a database that still looks like a live primary.
        ///
        /// By default this refuses a target that is not in recovery **and**
        /// still has connected standbys — i.e. a healthy primary rather than
        /// the standby you just promoted. That combination is the signature of
        /// a mis-typed DSN, and the cost of getting it wrong is a self-inflicted
        /// fleet-wide outage on a region that was fine.
        #[arg(long)]
        force: bool,

        /// Output format.
        #[arg(long, short = 'o', value_enum, default_value = "text")]
        format: DrFormat,
    },

    /// Finish promoting a standby: advance every sequence to match the data.
    ///
    /// **Required after promoting a LOGICAL standby.** Logical replication
    /// copies rows but not sequence values, so a promoted logical standby holds
    /// every replicated `harvest_events` row while `harvest_events_id_seq`
    /// still sits where it started — and the new primary's first append dies on
    /// a duplicate key.
    ///
    /// A separate verb from `fence` on purpose: folding it in would let an
    /// operator fence a shard and never advance its sequences, and discover it
    /// only when the first workflow tried to make progress. Harmless and
    /// idempotent on a physical replica, which replicates sequences already.
    Promote {
        /// A promoted shard database DSN. Repeat once per shard.
        #[arg(long = "shard", value_name = "DSN", required = true)]
        shards: Vec<String>,

        /// Output format.
        #[arg(long, short = 'o', value_enum, default_value = "text")]
        format: DrFormat,
    },
}

/// Output format for `harvest dr`.
///
/// This is a **local** flag (`--format` / `-o`); it is deliberately distinct
/// from the global `--output`, which formats management-API responses that
/// these commands never make.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum DrFormat {
    /// Human-readable table.
    #[default]
    Text,
    /// Machine-readable JSON.
    Json,
}

/// `harvest partition` — inspect and manage the opt-in partitioned
/// `harvest_events` layout (issue #958).
///
/// Every subcommand is shard-local by construction: a shard is a database, so
/// each `--shard` DSN is acted on independently and one shard's outcome never
/// depends on another's.
#[derive(Debug, Subcommand)]
pub enum PartitionCommand {
    /// Report each shard's layout, partitions, and what the sweeper would do.
    ///
    /// Read-only and safe at any time. This is the first thing to run when
    /// asking "why has space not come back?": the `blocked` column names each
    /// cohort the sweeper considered and the reason it was left alone.
    Status {
        /// A shard database DSN. Repeat once per shard. Accepts a bare DSN or
        /// an explicit `<shard_id>=<dsn>` pair; an unprefixed DSN takes its
        /// POSITIONAL index as its shard id.
        #[arg(long = "shard", value_name = "DSN", required = true)]
        shards: Vec<String>,

        /// Output format.
        #[arg(long, short = 'o', value_enum, default_value = "text")]
        format: DrFormat,
    },

    /// Print the SQL that converts a **large live** table, without running it.
    ///
    /// Emits the operator-run plan whose expensive steps sit OUTSIDE the
    /// exclusive lock window (`CREATE INDEX CONCURRENTLY`, `NOT VALID` +
    /// `VALIDATE CONSTRAINT`), leaving a metadata-only swap. Use this instead
    /// of `enable` on any table big enough that an index build inside a
    /// transaction would hold `ACCESS EXCLUSIVE` longer than you can afford.
    ///
    /// Needs no database connection: it prints a plan for you to review.
    Plan {
        /// Cohort width in seconds. Governs both reclamation granularity and
        /// the live partition count (retention horizon / width + lookahead).
        #[arg(long, value_name = "SECONDS", default_value_t = autumn_harvest::partition::DEFAULT_COHORT_WIDTH_SECS)]
        cohort_width_secs: i64,

        /// How many cohorts ahead of "now" the engine keeps pre-created.
        #[arg(long, value_name = "N", default_value_t = autumn_harvest::partition::DEFAULT_LOOKAHEAD_COHORTS)]
        lookahead_cohorts: u32,
    },

    /// **Convert this shard to the partitioned layout.**
    ///
    /// One transaction under a bounded `lock_timeout`, so a failure leaves the
    /// deployment exactly as it was. Instant on an empty table; on a populated
    /// one the existing table is attached WHOLE as the pre-cutover partition,
    /// so no row is copied or rewritten — but the index builds and constraint
    /// validation happen inside the lock window.
    ///
    /// On a large live table use `plan` instead.
    Enable {
        /// A shard database DSN. Repeat once per shard.
        #[arg(long = "shard", value_name = "DSN", required = true)]
        shards: Vec<String>,

        /// Cohort width in seconds.
        #[arg(long, value_name = "SECONDS", default_value_t = autumn_harvest::partition::DEFAULT_COHORT_WIDTH_SECS)]
        cohort_width_secs: i64,

        /// How many cohorts ahead of "now" to pre-create.
        #[arg(long, value_name = "N", default_value_t = autumn_harvest::partition::DEFAULT_LOOKAHEAD_COHORTS)]
        lookahead_cohorts: u32,

        /// Seconds to wait for `ACCESS EXCLUSIVE` on `harvest_events` before
        /// giving up. Failing fast is correct: a conversion that queues behind
        /// a long transaction blocks every append behind it.
        #[arg(long, value_name = "SECONDS", default_value_t = 5)]
        lock_timeout_secs: u64,

        /// Acknowledge that this takes a brief exclusive lock on
        /// `harvest_events`, during which appends wait.
        ///
        /// Deliberately unpleasant to type: on a populated table the window
        /// covers two index builds and a full-table constraint validation.
        #[arg(long = "i-understand-the-lock-window")]
        confirm: bool,

        /// Convert even when a logical-replication publication covers
        /// `harvest_events` without `publish_via_partition_root`.
        ///
        /// Such a publication would send the partitioned table's rows under
        /// leaf partition names the standby has no tables for, stopping the
        /// subscription. Set this only when the subscriber runs the
        /// partitioned layout too.
        #[arg(long = "allow-incompatible-publications")]
        allow_incompatible_publications: bool,

        /// Output format.
        #[arg(long, short = 'o', value_enum, default_value = "text")]
        format: DrFormat,
    },

    /// Run one maintenance pass now instead of waiting for a retention tick.
    ///
    /// Drains the `DEFAULT` partition, extends the lookahead window, then
    /// sweeps droppable cohorts. The retention janitor does exactly this every
    /// tick; this exists for incident response and for a deployment that runs
    /// with history retention disabled (where no janitor is running to do it).
    Maintain {
        /// A shard database DSN. Repeat once per shard.
        #[arg(long = "shard", value_name = "DSN", required = true)]
        shards: Vec<String>,

        /// How many cohorts ahead of "now" to keep pre-created.
        #[arg(long, value_name = "N", default_value_t = autumn_harvest::partition::DEFAULT_LOOKAHEAD_COHORTS)]
        lookahead_cohorts: u32,

        /// Maximum partitions to drop in this pass.
        #[arg(long, value_name = "N", default_value_t = 32)]
        max_drops: usize,

        /// Output format.
        #[arg(long, short = 'o', value_enum, default_value = "text")]
        format: DrFormat,
    },

    /// **Revert this shard to the ordinary unpartitioned table.**
    ///
    /// The escape hatch. Copies every surviving row back into a plain table and
    /// restores the foreign key, so it REWRITES THE WHOLE TABLE — schedule a
    /// window for it on anything large.
    Disable {
        /// A shard database DSN. Repeat once per shard.
        #[arg(long = "shard", value_name = "DSN", required = true)]
        shards: Vec<String>,

        /// Acknowledge that this rewrites `harvest_events` in full.
        #[arg(long = "i-understand-this-rewrites-the-table")]
        confirm: bool,

        /// Output format.
        #[arg(long, short = 'o', value_enum, default_value = "text")]
        format: DrFormat,
    },
}

/// `harvest backup` subcommands (issue #943).
#[derive(Debug, Subcommand)]
pub enum BackupCommand {
    /// Verify a restored (scratch) snapshot is resumable.
    Verify {
        /// A scratch-database DSN to inspect. Repeat once per shard. Accepts a
        /// bare DSN or an explicit `<shard_id>=<dsn>` pair. An unprefixed DSN
        /// takes its POSITIONAL index as its shard id (first `--shard` is
        /// shard `0`, second is shard `1`, ...) — so prefix explicitly
        /// whenever your shard ids are not `0..n`.
        ///
        /// Supply EVERY shard: a cross-shard reference into a shard that was
        /// not supplied is reported as an advisory, never silently passed.
        #[arg(long = "shard", value_name = "DSN", required = true)]
        shards: Vec<String>,

        /// A live (production) DSN to guard against. When a `--shard` target
        /// resolves to the same `(host, port, database)`, the run is refused
        /// unless `--i-know-this-is-scratch` is passed.
        ///
        /// Repeat once per live shard: a fleet whose other shards are not
        /// named here is NOT guarded against, and the run warns saying so.
        #[arg(
            long = "live-dsn",
            value_name = "DSN",
            env = "HARVEST_DATABASE_URL",
            hide_env_values = true
        )]
        live_dsn: Vec<String>,

        /// Acknowledge that every `--shard` target is a throwaway scratch copy,
        /// overriding the live-DSN guard.
        #[arg(long, default_value_t = false)]
        i_know_this_is_scratch: bool,

        /// Output format: human-readable `text` (default) or machine-readable
        /// `json`.
        #[arg(long, value_enum, default_value_t)]
        format: BackupVerifyFormat,

        /// How many non-terminal histories to sample and replay per shard.
        #[arg(long, default_value_t = 50)]
        replay_sample: usize,

        /// Worker-heartbeat staleness threshold, in seconds, for the
        /// dead-worker and broken-session probes.
        #[arg(long, default_value_t = 60)]
        worker_stale_secs: i64,

        /// Rows read per coherence probe.
        ///
        /// For the cross-shard reference scan this is a *page* size, not a
        /// ceiling: that scan pages through complete owner groups until the
        /// shard is exhausted. Raise it only if the report says a single
        /// execution carries more reference events than one page.
        #[arg(long, default_value_t = 1000)]
        probe_limit: i64,

        /// The shard a pre-sharding (unencoded) target id resolves to.
        ///
        /// Must match the fleet's configured default shard (`ShardRouter`'s
        /// `default_shard`), not whichever `--shard` happens to observe the
        /// reference. Every runtime routing path falls back to the fleet
        /// default for an unencoded id. This check must agree with them, or
        /// it reports a false `child_execution_missing` /
        /// `external_target_missing` on a fleet migrated from pre-sharding
        /// ids. Defaults to `0`, the overwhelmingly common configuration.
        #[arg(long, default_value_t = 0)]
        default_shard: i32,
    },
}

/// Output format for the `det-check` subcommand (issue #778).
///
/// This is a **local** flag (`--format`); it is deliberately distinct from the
/// global `--output` used for API responses.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum DetCheckFormat {
    /// Human-readable one-line-per-finding text (default).
    #[default]
    Text,
    /// Machine-readable `DetCheckReport` JSON for CI consumption.
    Json,
}

/// Output format for the `schema check` subcommand (issue #794).
///
/// This is a **local** flag (`--format`); it is deliberately distinct from the
/// global `--output` used for API responses.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum SchemaCheckFormat {
    /// Human-readable `workflow.role: <field_path> — <verdict> — <reason>`
    /// lines (default).
    #[default]
    Text,
    /// Machine-readable diff JSON for CI consumption.
    Json,
}

/// Output format for the `migrate` subcommands (issue #1240).
///
/// This is a **local** flag (`--format`); it is deliberately distinct from the
/// global `--output` used for API responses.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum MigrateFormat {
    /// Human-readable per-database summary (default).
    #[default]
    Text,
    /// Machine-readable JSON for deploy pipelines.
    Json,
}

/// Project template flavour for `harvest new` (issue #692).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum ScaffoldTemplate {
    /// One `#[workflow]` calling one `#[activity]`, `HarvestPlugin` wiring, a
    /// `compose.yaml` Postgres, and a README with the exact run steps.
    #[default]
    Minimal,
}

/// Signal reapply policy for `workflow reset`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum ResetSignalReapply {
    /// Discard undelivered signals on the source execution.
    Drop,
    /// Re-enqueue undelivered source signals onto the fork.
    Buffer,
}

impl ResetSignalReapply {
    const fn as_wire(self) -> &'static str {
        match self {
            Self::Drop => "drop",
            Self::Buffer => "buffer",
        }
    }
}

/// Execution-state scope for version-gate usage reports.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum VersionUsageStateGroup {
    /// Include only executions that may still replay old branches.
    Active,
    /// Include only terminal executions.
    Terminal,
    /// Include active and terminal executions.
    All,
}

impl VersionUsageStateGroup {
    const fn as_wire(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Terminal => "terminal",
            Self::All => "all",
        }
    }
}

/// Payload policy for history exports.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum HistoryExportPayloadPolicy {
    /// Redact payload-bearing fields and emit deterministic summaries.
    Redacted,
    /// Emit raw payloads for private replay fixtures. Sensitive.
    Full,
}

impl HistoryExportPayloadPolicy {
    const fn as_wire(self) -> &'static str {
        match self {
            Self::Redacted => "redacted",
            Self::Full => "full",
        }
    }
}

/// Execution-state scope for batch history exports.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum HistoryExportStateGroup {
    /// Include only executions that can still run or replay.
    Active,
    /// Include only terminal executions.
    Terminal,
    /// Include active and terminal executions.
    All,
}

impl HistoryExportStateGroup {
    const fn as_wire(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Terminal => "terminal",
            Self::All => "all",
        }
    }
}

/// HTTP method used by a management API request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApiMethod {
    /// HTTP GET.
    Get,
    /// HTTP PATCH.
    Patch,
    /// HTTP POST.
    Post,
    /// HTTP DELETE.
    Delete,
}

/// Thin request description built from CLI arguments.
#[derive(Debug, Eq, PartialEq)]
pub struct ApiRequest {
    /// HTTP method.
    pub method: ApiMethod,
    /// Path relative to the configured Harvest API mount.
    pub path: String,
    /// Optional JSON request body.
    pub body: Option<Value>,
}

/// CLI failure modes.
#[derive(Debug, Error)]
pub enum CliError {
    /// A supplied JSON string could not be parsed.
    #[error("invalid JSON for {label}: {source}")]
    InvalidJson {
        /// User-facing source label.
        label: &'static str,
        /// JSON parser error.
        source: serde_json::Error,
    },

    /// A supplied JSON file could not be read.
    #[error("failed to read {label} from {path}: {source}")]
    ReadJson {
        /// User-facing source label.
        label: &'static str,
        /// Path displayed to the user.
        path: String,
        /// I/O failure.
        source: std::io::Error,
    },

    /// Output could not be written to the requested file.
    #[error("failed to write output to {path}: {source}")]
    WriteOutput {
        /// Path displayed to the user.
        path: String,
        /// I/O failure.
        source: std::io::Error,
    },

    /// Both inline and file JSON sources were supplied for one field.
    #[error("{label} accepts either inline JSON or a file, not both")]
    ConflictingJsonSources {
        /// User-facing source label.
        label: &'static str,
    },

    /// A required input source (file or inline JSON) was not provided.
    #[error("{label}")]
    MissingInput {
        /// User-facing message.
        label: &'static str,
    },

    /// HTTP transport failed.
    #[error("request failed: {0}")]
    Request(#[from] reqwest::Error),

    /// The Harvest API returned a non-success status.
    #[error("harvest API returned {status}: {body}")]
    Http {
        /// HTTP status code.
        status: reqwest::StatusCode,
        /// Response body text.
        body: String,
    },

    /// API response JSON could not be parsed.
    #[error("failed to parse response JSON: {0}")]
    ParseResponse(serde_json::Error),

    /// JSON output could not be serialized.
    #[error("failed to serialize response JSON: {0}")]
    SerializeResponse(serde_json::Error),

    /// A `--search-attr` flag was missing the `=` separator.
    #[error("invalid --search-attr '{value}': expected 'key=value'")]
    InvalidSearchAttr {
        /// Original CLI argument value.
        value: String,
    },

    /// Preflight completed but reported a non-passing deploy-gate status.
    #[error("preflight overall_status={status}")]
    PreflightGate {
        /// Reported preflight status.
        status: String,
    },

    /// Shard health completed but the deploy gate found a non-ready target.
    #[error("shard health readiness gate failed")]
    ShardHealthGate,

    /// Replay canary completed but reported a non-passing verdict.
    #[error("replay canary gate failed: verdict={verdict}")]
    CanaryGate {
        /// Reported canary verdict.
        verdict: String,
    },

    /// A queue pause/resume was only partially applied across the fleet.
    #[error("queue mutation was only partially applied: {detail}")]
    QueuePartialMutation {
        /// Per-shard failure summary reported by the API.
        detail: String,
    },

    /// An activity pause/resume was only partially applied across the fleet.
    #[error("activity mutation was only partially applied: {detail}")]
    ActivityPartialMutation {
        /// Per-shard failure summary reported by the API.
        detail: String,
    },

    /// A queue name would be normalized away as a URL dot-segment.
    #[error(
        "invalid queue name '{value}': '.' and '..' are removed as dot-segments \
         when the request URL is parsed, which would silently retarget the \
         request at a different route"
    )]
    QueueNameDotSegment {
        /// Original CLI argument value.
        value: String,
    },

    /// An activity name would be normalized away as a URL dot-segment.
    #[error(
        "invalid activity name '{value}': '.' and '..' are removed as \
         dot-segments when the request URL is parsed, which would silently \
         retarget the request at a different route"
    )]
    ActivityNameDotSegment {
        /// Original CLI argument value.
        value: String,
    },

    /// Version-gate guard found active usage or incomplete shard inspection.
    #[error("version usage guard failed")]
    VersionUsageGate,

    /// Retirement check found active old-version executions or an unavailable shard.
    #[error("version-gate retirement check failed")]
    RetirementCheckGate,

    /// Workflow-type reachability found an orphaned type, incomplete report, or transport error.
    ///
    /// `context` carries either the original transport error (connection failure,
    /// auth error, server 5xx) so operators can distinguish infra misconfiguration
    /// from an unsafe-handler-removal verdict.
    #[error("workflow-type reachability gate failed: {context}")]
    WorkflowReachabilityGate {
        /// Human-readable cause: transport error string or verdict summary.
        context: String,
    },

    /// Queue coverage found an uncovered task queue or an incomplete report.
    ///
    /// `context` carries either the original transport error (connection
    /// failure, auth error, server 5xx) so operators can distinguish infra
    /// misconfiguration from an unsafe-deploy verdict (issue #774).
    #[error("queue coverage gate failed: {context}")]
    QueueCoverageGate {
        /// Human-readable cause: transport error string or verdict summary.
        context: String,
    },

    /// `--wait` timed out before the worker reached `Stopped`.
    #[error("timed out waiting for worker '{worker_id}' to stop (last status: {last_status})")]
    DrainWaitTimeout {
        /// Worker ID that did not reach `Stopped`.
        worker_id: String,
        /// Last observed lifecycle status.
        last_status: String,
    },

    /// The SSE event stream closed abnormally (e.g. slow consumer).
    #[error("SSE stream closed by server: {message}")]
    SseStreamError {
        /// Server-supplied error detail.
        message: String,
    },

    /// A CLI argument value was invalid (e.g. unrecognised scope format).
    #[error("{0}")]
    InvalidInput(String),

    /// `det-check` found determinism violations that fail the gate (issue #778).
    ///
    /// The findings themselves are already printed to stdout; this only carries
    /// the counts so `main` can exit with the right code. Exit code is `1`.
    #[error("det-check: {errors} hard-blocker finding(s), {warnings} warning(s)")]
    DetCheckFindings {
        /// Number of `Error`-severity findings.
        errors: usize,
        /// Number of `Warning`-severity findings.
        warnings: usize,
    },

    /// `backup verify` found a broken invariant in the restored snapshot
    /// (issue #943). Exit code is `1`.
    ///
    /// The report itself is already printed; this only carries the count so
    /// `main` can exit with the right code.
    #[error(
        "backup verify: {findings} incoherent finding(s) — DO NOT START WORKERS on this \
         restore. See the report above and docs/runbooks/backup-restore.md."
    )]
    RestoreIncoherent {
        /// Number of `Incoherent`-severity findings.
        findings: usize,
    },

    /// `backup verify` could not complete every check, so the restore's
    /// coherence could not be determined (issue #943). Exit code is `2`,
    /// distinct from a determined failure, so CI can tell "broken" apart from
    /// "unknown".
    ///
    /// Two causes: a shard that could not be connected to, and a probe that
    /// could not run (canonically a missing table — an unmigrated database).
    #[error(
        "backup verify: UNDETERMINED — {unreachable_shards} shard(s) unreachable, \
         {failed_probes} probe(s) could not run. Do not start workers on the strength \
         of this report."
    )]
    RestoreUndetermined {
        /// Number of shards that could not be reached.
        unreachable_shards: usize,
        /// Number of `Undetermined`-severity findings (probes that never ran).
        failed_probes: usize,
    },

    /// `schema check` found a backward-incompatible payload-schema change
    /// (issue #794).
    ///
    /// The diff itself is already printed to stdout; this only carries the
    /// count so `main` can exit with the right code. Exit code is `1`, per the
    /// issue's exit-code contract.
    #[error(
        "schema check: {breaking} breaking schema change(s) would break replay of in-flight \
         executions. Acknowledge a deliberate migration with `harvest schema update \
         --acknowledge \"<why this is safe>\"`."
    )]
    SchemaContractBreaking {
        /// Number of breaking deltas.
        breaking: usize,
    },

    /// `schema check --acknowledged-in` found a breaking change the artifact
    /// does not record (issue #794).
    ///
    /// The escape hatch is auditable by construction: absorbing a break writes
    /// a justification into the artifact. This fires when the artifact moved
    /// over a break with no such record — the shape a hand-edited baseline
    /// takes, which the ordinary check cannot see because the artifact and the
    /// generated contract already agree. Exit code is `1`.
    #[error(
        "schema check: {missing} breaking schema change(s) since the previous revision of the \
         artifact carry no acknowledgement:\n{detail}\nThe artifact appears to have absorbed a \
         break without recording why. Regenerate it with `harvest schema update --acknowledge \
         \"<why this is safe>\"` instead of editing it by hand."
    )]
    SchemaContractUnacknowledged {
        /// Number of breaking deltas with no covering record.
        missing: usize,
        /// One line per unrecorded break.
        detail: String,
    },

    /// The acknowledgement log lost a record it previously carried.
    ///
    /// The log is append-only, and the coverage check above leans on that: it
    /// subtracts the records the previous revision already had, so a stale one
    /// cannot cover a fresh break. Retargeting an existing record — editing the
    /// break it names rather than appending — defeats that subtraction, and is
    /// invisible to both the ordinary diff and the coverage check. A vanished
    /// record is its one observable signature. Exit code is `1`.
    #[error(
        "schema check: {dropped} acknowledgement record(s) present in the previous revision of \
         the artifact are missing now:\n{detail}\nThe acknowledgement log is append-only. Restore \
         the record(s) and append a new one with `harvest schema update --acknowledge \
         \"<why this is safe>\"` instead of editing an existing entry."
    )]
    SchemaContractAuditLogRewritten {
        /// Number of records the previous revision carried that are now gone.
        dropped: usize,
        /// One line per vanished record.
        detail: String,
    },

    /// `debug replay` was given a breakpoint that never matched (issue #949).
    ///
    /// The trace overview is already printed to stdout; this only signals the
    /// exit code. Exit code is `1`.
    #[error("debug replay: breakpoint never hit")]
    DebugBreakpointMissed,

    /// `debug replay --step N` was given an index past the end of the trace
    /// (issue #949). Exit code is `1`.
    #[error("debug replay: step {index} is out of range (trace has {len} steps)")]
    DebugStepOutOfRange {
        /// The requested step index.
        index: usize,
        /// How many steps the trace actually has.
        len: usize,
    },

    /// `debug diff` found a divergence (issue #949).
    ///
    /// The report itself is already printed to stdout; this only signals the
    /// exit code. Mirrors `diff(1)`: exit `1` means "differences found", not
    /// "the tool failed". Exit code is `1`.
    #[error("debug diff: traces diverge at step {step_index}")]
    DebugDivergence {
        /// The step index at which the two traces first differ.
        step_index: usize,
    },

    /// `debug diff` compared only a **prefix** of the two traces because at
    /// least one was capped by `max_steps`, and found no difference in it
    /// (issue #949).
    ///
    /// This is deliberately *not* exit `0`. "No difference in the part we
    /// looked at" is not "these agree", and a CI gate that treats it as a pass
    /// is silently trusting an unexamined suffix. Exit code is `2`, joining
    /// the other "could not determine" gates (`WorkflowReachabilityGate`,
    /// `QueueCoverageGate`, `RestoreUndetermined`) so a script can tell it
    /// apart from `1` = "differences found".
    #[error("debug diff: inconclusive — {reason} (compared {examined} steps)")]
    DebugDiffInconclusive {
        /// How many steps were actually compared.
        examined: usize,
        /// Why the comparison could not reach a verdict. Distinguishes an
        /// unexamined suffix (a `--max-steps` cap) from two builds that both
        /// failed to replay, which agree only because neither answered.
        reason: &'static str,
    },

    /// A terminal operation failed while running the interactive stepper
    /// (issue #949). Exit code is `1`.
    #[error("debug replay: terminal {op} failed: {reason}")]
    DebugTerminal {
        /// The terminal operation that failed.
        op: &'static str,
        /// The underlying error, rendered.
        reason: String,
    },

    /// A `migrate` operation failed against one database (issue #1240).
    ///
    /// The DSN is redacted before it reaches this message: a migration command
    /// is run from deploy pipelines whose logs are far more widely readable
    /// than the credential in `harvest.database.url`. Exit code is `1`.
    #[error("migrate: {database}: {reason}")]
    Migrate {
        /// Redacted DSN of the database the operation was pointed at.
        database: String,
        /// The underlying failure, rendered.
        reason: String,
    },

    /// `migrate status --check` found a pending migration (issue #1240).
    ///
    /// The report itself is already printed; this only signals the exit code.
    /// Exit code is `1` — "determined: not migrated", as distinct from the
    /// exit-`2` gates that mean "could not determine".
    // The remedy names the flags on purpose: this gate reports on exactly the
    // sets it was handed, so a bare `harvest migrate run` after a `--check`
    // that carried `--include-dir` would apply the embedded set only, exit 0,
    // and leave the very migration that failed the gate unapplied.
    #[error(
        "migrate status: {pending} pending migration(s) across {databases} database(s) — \
         run `harvest migrate run` with the SAME --database-url and --include-dir \
         flags you passed here, before rolling replicas"
    )]
    MigrationsPending {
        /// Total pending migrations across every inspected database.
        pending: usize,
        /// How many databases still have at least one pending migration.
        databases: usize,
    },
}

impl CliError {
    /// Process exit code associated with this error.
    #[must_use]
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::PreflightGate { status } if status == "warn" => 2,
            // Issue #520 / #774: both the workflow-reachability gate and the
            // queue-coverage gate use exit code 2 specifically so CI can
            // distinguish "an orphaned/partial/uncovered deploy hazard" from
            // a generic transport/usage failure (exit 1).
            //
            // Issue #943 joins them for the same reason: "could not determine"
            // (a shard was unreachable) must be distinguishable from
            // "determined broken" (exit 1), so an operator drill script can
            // retry a transient shard outage rather than declare the restore
            // unusable.
            //
            // Issue #949's `debug diff` joins them on the same reasoning: a
            // capped comparison examined only a prefix, so "inconclusive" must
            // be distinguishable from `1` = "differences found" and from
            // `0` = "compared in full and agree".
            Self::WorkflowReachabilityGate { .. }
            | Self::QueueCoverageGate { .. }
            | Self::RestoreUndetermined { .. }
            | Self::DebugDiffInconclusive { .. } => 2,
            _ => 1,
        }
    }
}

/// `harvest debug` subcommands (issue #949).
#[derive(Debug, Subcommand)]
enum DebugCommand {
    /// Step through a recorded history, with optional breakpoints.
    Replay {
        /// Path to an exported `HistorySnapshot` JSON file.
        #[arg(value_name = "HISTORY")]
        history: PathBuf,
        /// Output format: human-readable `text` (default) or machine-readable
        /// `json` (the full `ReplayTrace`).
        #[arg(long, value_enum, default_value_t)]
        format: crate::debug::DebugFormat,
        /// Jump straight to this step index and inspect it.
        #[arg(long, value_name = "N", conflicts_with_all = ["break_at_event_type", "break_at_index", "break_at_activity", "break_at_signal"])]
        step: Option<usize>,
        /// Run to the first step whose event type matches.
        #[arg(long, value_name = "TYPE", conflicts_with_all = ["break_at_index", "break_at_activity", "break_at_signal"])]
        break_at_event_type: Option<String>,
        /// Run to this exact event index.
        #[arg(long, value_name = "N", conflicts_with_all = ["break_at_activity", "break_at_signal"])]
        break_at_index: Option<usize>,
        /// Run to the first step that schedules this activity.
        #[arg(long, value_name = "NAME", conflicts_with = "break_at_signal")]
        break_at_activity: Option<String>,
        /// Run to the first step that receives (or waits for) this signal.
        #[arg(long, value_name = "NAME")]
        break_at_signal: Option<String>,
        /// Cap the number of steps materialised (guards a pathological history).
        #[arg(long, value_name = "N")]
        max_steps: Option<usize>,
        /// Open the interactive stepper instead of printing.
        #[arg(long, default_value_t = false, conflicts_with = "format")]
        tui: bool,
    },
    /// Diff two recorded histories and report the first divergence.
    ///
    /// Exits `1` when a divergence is found, mirroring `diff(1)`, so this is
    /// usable directly as a CI gate.
    Diff {
        /// The baseline history (the "old build" recording).
        #[arg(value_name = "LEFT")]
        left: PathBuf,
        /// The candidate history (the "new build" recording).
        #[arg(value_name = "RIGHT")]
        right: PathBuf,
        /// Output format: human-readable `text` (default) or machine-readable
        /// `json` (the full `TraceDiff`).
        #[arg(long, value_enum, default_value_t)]
        format: crate::debug::DebugFormat,
    },
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Check management API health.
    Health,
    /// Run read-only deployment readiness checks.
    Preflight,
    /// Inspect shard rollout readiness.
    Shard {
        #[command(subcommand)]
        command: ShardCommand,
    },
    /// Manage workflow executions.
    #[command(alias = "workflows")]
    Workflow {
        #[command(subcommand)]
        command: WorkflowCommand,
    },
    /// Export workflow histories for replay fixtures and diagnostics.
    History {
        #[command(subcommand)]
        command: HistoryCommand,
    },
    /// Place or release a per-execution legal hold (issue #747).
    #[command(alias = "legal-holds")]
    LegalHold {
        #[command(subcommand)]
        command: LegalHoldCommand,
    },
    /// Inspect and resolve external activity handoffs.
    #[command(
        alias = "handoffs",
        alias = "external-handoff",
        alias = "external-handoffs"
    )]
    Handoff {
        #[command(subcommand)]
        command: HandoffCommand,
    },
    /// Manage DAG schedules and runs.
    Dag {
        #[command(subcommand)]
        command: DagCommand,
    },
    /// Manage workflow and DAG schedules (issue #91).
    #[command(alias = "schedules")]
    Schedule {
        #[command(subcommand)]
        command: ScheduleCommand,
    },
    /// Manage dead-lettered tasks.
    #[command(alias = "dead-letter", alias = "dead-letters")]
    Dlq {
        #[command(subcommand)]
        command: DeadLetterCommand,
    },
    /// Inspect and redrive durable completion-callback deliveries (issue #605).
    #[command(alias = "completion-deliveries", alias = "callbacks")]
    CompletionDelivery {
        #[command(subcommand)]
        command: CompletionDeliveryCommand,
    },
    /// Retention janitor operations.
    Retention {
        #[command(subcommand)]
        command: RetentionCommand,
    },
    /// Hold or release dispatch on a named task queue (issue #619).
    #[command(alias = "queues")]
    Queue {
        #[command(subcommand)]
        command: QueueCommand,
    },
    /// Hold or release dispatch for a single activity type (issue #807).
    #[command(alias = "activities")]
    Activity {
        #[command(subcommand)]
        command: ActivityCommand,
    },
    /// Inspect cluster-wide per-activity concurrency caps.
    Concurrency {
        #[command(subcommand)]
        command: ConcurrencyCommand,
    },
    /// Manage per-activity rate limits.
    #[command(alias = "rate-limits")]
    RateLimit {
        #[command(subcommand)]
        command: RateLimitCommand,
    },
    /// Manage workflow-start throttles (issue #607, TTL'd overrides added
    /// in issue #945).
    #[command(alias = "throttles")]
    Throttle {
        #[command(subcommand)]
        command: ThrottleCommand,
    },
    /// Manage batch operations.
    Batch {
        #[command(subcommand)]
        command: BatchCommand,
    },
    /// Browse the management API audit trail.
    Audit {
        #[command(subcommand)]
        command: AuditCommand,
    },
    /// Manage admission gates for incident-response halts (issue #377).
    #[command(alias = "gates")]
    Gate {
        #[command(subcommand)]
        command: GateCommand,
    },
    /// Manage scoped API tokens for the management API (issue #942).
    #[command(alias = "tokens")]
    Token {
        #[command(subcommand)]
        command: TokenCommand,
    },
    /// Report per-tenant/per-workflow usage for chargeback and capacity planning (issue #596).
    ///
    /// The historical companion to `harvest concurrency status`. Renders a
    /// table by default; pass --json for piping.
    Usage {
        /// Inclusive lower bound of the aggregation window: RFC 3339 or a
        /// relative duration like 24h.
        #[arg(long)]
        from: String,
        /// Inclusive upper bound of the aggregation window: RFC 3339 or a
        /// relative duration like 24h.
        #[arg(long)]
        to: String,
        /// Grouping dimension: `workflow_name` (default) or
        /// `search_attr:<key>` (e.g. `search_attr:tenant_id`).
        #[arg(long = "group-by")]
        group_by: Option<String>,
        /// Emit raw JSON instead of a table.
        #[arg(long)]
        json: bool,
    },
    /// Report recorded workflow version-gate usage.
    VersionUsage {
        /// Filter by registered workflow name.
        #[arg(long)]
        workflow_name: Option<String>,
        /// Filter by version-gate change id.
        #[arg(long)]
        change_id: Option<String>,
        /// Filter by recorded version.
        #[arg(long = "version", value_parser = clap::value_parser!(u32))]
        recorded_version: Option<u32>,
        /// Filter by execution state group.
        #[arg(long, value_enum)]
        state_group: Option<VersionUsageStateGroup>,
        /// Filter by shard id.
        #[arg(long)]
        shard_id: Option<i32>,
        /// Exit non-zero if any active execution still matches the filtered version gate.
        #[arg(long, requires = "change_id", requires = "recorded_version")]
        guard: bool,
    },
    /// Check whether a version-gate change id is safe to retire below a version threshold.
    ///
    /// Exits non-zero when `--check` is passed and any non-terminal execution still carries
    /// a recorded version below `--min-safe-version`, or when any shard is unavailable.
    #[command(alias = "version-gate-check")]
    VersionGateRetirement {
        /// Version-gate change id to inspect (required).
        #[arg(long)]
        change_id: String,
        /// Versions strictly below this value are considered old branches to retire.
        #[arg(long, value_parser = clap::value_parser!(u32))]
        min_safe_version: u32,
        /// Narrow results to this workflow name.
        #[arg(long)]
        workflow_name: Option<String>,
        /// Filter by execution state group.
        #[arg(long, value_enum)]
        state_group: Option<VersionUsageStateGroup>,
        /// Restrict inspection to one shard.
        #[arg(long)]
        shard_id: Option<i32>,
        /// Exit non-zero while any non-terminal execution still uses an old version,
        /// or while any shard is unavailable.
        #[arg(long)]
        check: bool,
    },
    /// Inspect workflow-type handler reachability for safe handler removal (issue #520).
    #[command(name = "workflow-types", alias = "workflow-type")]
    WorkflowTypes {
        #[command(subcommand)]
        command: WorkflowTypesCommand,
    },
    /// Open the TUI dashboard to monitor workflows.
    Tui,
    /// Inspect and drain worker fleet (issue #170).
    #[command(alias = "workers")]
    Worker {
        #[command(subcommand)]
        command: WorkerCommand,
    },
    /// Stream live workflow execution events.
    #[command(alias = "event")]
    Events {
        #[command(subcommand)]
        command: EventsCommand,
    },
    /// Start N workflow executions in one batched request (issue #357).
    ///
    /// Reads newline-delimited JSON (NDJSON) items from a file or inline JSON
    /// array and submits them as a single `POST /workflows/batch_start` call.
    ///
    /// Each NDJSON line must be a JSON object with at least a `workflow_name`
    /// key.  Optional keys: `workflow_id`, `input`, `search_attributes`,
    /// `idempotency_key`.
    ///
    /// Exits non-zero when `--atomic` is set and any item is rejected.
    #[command(name = "start-batch")]
    StartBatch {
        /// NDJSON file of workflow start items. Use `-` to read from stdin.
        ///
        /// Conflicts with `--items-json`.
        #[arg(long, value_name = "PATH", conflicts_with = "items_json")]
        file: Option<PathBuf>,
        /// Inline JSON array of workflow start items.
        ///
        /// Conflicts with `--file`.
        #[arg(long, conflicts_with = "file")]
        items_json: Option<String>,
        /// Require all-or-nothing semantics: if any item fails validation the
        /// entire batch is rejected with no executions inserted.
        #[arg(long, default_value_t = false)]
        atomic: bool,
    },
    /// Run a deploy-time replay canary over live executions.
    Canary {
        /// Maximum number of running workflow executions to sample.
        #[arg(long, default_value = "500")]
        sample_size: usize,
        /// Filter samples to a specific workflow type.
        #[arg(long)]
        workflow_name: Option<String>,
        /// Filter samples to a specific task queue.
        #[arg(long)]
        queue: Option<String>,
        /// Output raw JSON instead of the summary table.
        #[arg(long)]
        json: bool,
    },
    /// Manage build-routing policies and percentage ramps for safe rolling
    /// deploys (issue #171, issue #604).
    #[command(alias = "build-routing")]
    Build {
        #[command(subcommand)]
        command: BuildRoutingCommand,
    },
    /// Cross-region disaster-recovery operations: inspect the RPO, apply the
    /// failover fence, finish a promotion (issue #954).
    ///
    /// Talks to shard databases directly, never to the management API — during
    /// a regional failover the management API may be exactly what is down.
    /// See `docs/runbooks/cross-region-failover.md`.
    Dr {
        #[command(subcommand)]
        command: DrCommand,
    },

    /// Inspect and manage the opt-in partitioned `harvest_events` layout
    /// (issue #958).
    ///
    /// Talks to shard databases directly, never to the management API: the
    /// layout is a property of each shard's own schema, and `enable`/`disable`
    /// are DDL that no HTTP surface should be able to trigger.
    Partition {
        #[command(subcommand)]
        command: PartitionCommand,
    },

    /// Verify that a restored backup/PITR snapshot is resumable (issue #943).
    ///
    /// Read-only against every supplied scratch database: each connection is
    /// pinned `SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY`, so
    /// Postgres itself rejects any write. Never mutates the production
    /// database, never starts a worker, and never applies a reclaim -- the
    /// scanners' work is *reported*, not performed.
    ///
    /// Exit codes: `0` clean or resumable-with-reclaim; `1` incoherent (do not
    /// start workers); `2` undetermined (a shard was unreachable).
    Backup {
        #[command(subcommand)]
        command: BackupCommand,
    },

    /// Statically check source for non-determinism reachable from `#[workflow]`
    /// bodies, including one first-party helper hop (issue #778).
    ///
    /// Read-only source analysis: no database, no network. Exits `0` when there
    /// are no hard-blocker findings and `1` when any `Error`-severity finding is
    /// present. Warnings (DET005/DET009 and command-free DET010) never fail the
    /// build unless `--deny-warnings` is passed.
    #[command(name = "det-check")]
    DetCheck {
        /// Source paths (files or directories) to scan. Defaults to the current
        /// directory. Directories are scanned recursively; `target` and hidden
        /// directories are skipped.
        #[arg(value_name = "PATHS", default_value = ".")]
        paths: Vec<PathBuf>,
        /// Output format: human-readable `text` (default) or machine-readable
        /// `json` (a full `DetCheckReport` with findings and suppressions).
        #[arg(long, value_enum, default_value_t)]
        format: DetCheckFormat,
        /// Also fail (exit `1`) when any warning-severity finding is present.
        #[arg(long, default_value_t = false)]
        deny_warnings: bool,
        /// List every active `harvest-suppress` suppression with its reason and
        /// location, then exit `0` (audit mode).
        #[arg(long, default_value_t = false)]
        list_suppressions: bool,
    },

    /// Time-travel replay debugger for a recorded workflow history (issue #949).
    ///
    /// Read-only and entirely local: no HTTP, no database, no activity
    /// execution. Operates on the `HistorySnapshot` JSON written by
    /// `harvest history export`, so a production history is debugged offline.
    ///
    /// The shipped binary cannot link your `#[workflow]` handlers, so this
    /// command covers the handler-free surface: stepping, breakpoints,
    /// inspection, and diffing two fixture histories. For per-step pending
    /// commands and for diffing two workflow-code registrations, use
    /// `autumn_harvest::debugger::ReplayDebugger` from a test or binary in your
    /// own crate.
    Debug {
        #[command(subcommand)]
        command: DebugCommand,
    },

    /// Apply Harvest's schema migrations to a dedicated Harvest database
    /// (issue #1240).
    ///
    /// For `harvest.mode = "split"` / `"external"` deployments, where Harvest
    /// storage is a database Autumn has no handle on: `autumn migrate` reaches
    /// the application database only, and outside the `dev` profile the plugin
    /// warns about pending Harvest migrations rather than applying them.
    ///
    /// Talks to Postgres directly — no management API, no running app — and
    /// uses the same `__diesel_schema_migrations` ledger Autumn and Diesel use,
    /// so a migration is applied exactly once no matter which of them applies
    /// it. Under `embedded` mode use `autumn migrate` instead; the two Harvest
    /// sets are Autumn's there.
    ///
    /// Harvest's own migrations are embedded in this binary. Sets that are not
    /// (the plugin's connector dead-letter table, an application's own) are
    /// applied by pointing `--include-dir` at their migration directories.
    Migrate {
        #[command(subcommand)]
        command: MigrateCommand,
    },

    /// Gate backward-incompatible workflow payload-schema changes (issue #794).
    ///
    /// Read-only file comparison: no database, no network.
    Schema {
        /// `check` (gate) or `update` (regenerate the baseline).
        #[command(subcommand)]
        command: SchemaCommand,
    },

    /// Scaffold a new, runnable autumn-harvest project (issue #692).
    ///
    /// Emits a complete crate — a `Cargo.toml` with crates.io deps and the `db`
    /// feature, a `#[workflow]`/`#[activity]` pair with `HarvestPlugin` wiring, a
    /// `compose.yaml` Postgres, an `autumn.toml`, and a README whose three-command
    /// path reaches one terminal execution. Pure local file generation: no
    /// database, no network. Everything is named after `<name>` — no manual
    /// find-and-replace of example identifiers.
    New {
        /// Project name. Becomes the crate name, the workflow/activity function
        /// stems, and the activity queue. Must be a valid Cargo package name
        /// (letters/digits/`-`/`_`, starting with a letter).
        #[arg(value_name = "NAME")]
        name: String,
        /// Target directory (defaults to `./<name>`).
        #[arg(long)]
        path: Option<PathBuf>,
        /// Overwrite files in a non-empty target directory. Opt-in; never
        /// removes files it did not write (no `rm -rf`).
        #[arg(long, default_value_t = false)]
        force: bool,
        /// Template to emit. Currently only `minimal` ships.
        #[arg(long, value_enum, default_value_t)]
        template: ScaffoldTemplate,
    },
}

/// `harvest migrate` subcommands (issue #1240).
///
/// Both talk to Postgres directly and never to the management API. `status` is
/// strictly read-only — it does not even create the migration ledger — so it is
/// safe to point at a database you are only inspecting.
#[derive(Debug, Subcommand)]
enum MigrateCommand {
    /// Report applied and pending migrations without changing anything.
    ///
    /// Exits `0` normally, and `1` with `--check` when any target still has a
    /// pending migration (the deploy-gate form: run it before rolling
    /// replicas).
    Status {
        /// Harvest database to inspect: the value of `harvest.database.url`.
        ///
        /// Repeat once per shard database for a multi-shard deployment; each
        /// one needs the full set.
        #[arg(
            long = "database-url",
            env = "HARVEST_DATABASE_URL",
            hide_env_values = true,
            value_name = "URL",
            required = true
        )]
        database_url: Vec<String>,
        /// Additional migration directory to include, e.g.
        /// `autumn-harvest-plugin/migrations/harvest` for the connector
        /// dead-letter table. Repeatable; each is a directory of
        /// `<version>_<description>/up.sql` migrations.
        #[arg(long = "include-dir", value_name = "DIR")]
        include_dir: Vec<PathBuf>,
        /// Output format: human-readable `text` (default) or machine-readable
        /// `json`.
        #[arg(long, value_enum, default_value_t)]
        format: MigrateFormat,
        /// Exit non-zero while any target has a pending migration.
        #[arg(long, default_value_t = false)]
        check: bool,
    },
    /// Apply every pending migration, in version order.
    ///
    /// Each migration runs inside one transaction together with its ledger row,
    /// so a failure leaves neither the schema change nor the record of it —
    /// with one exception, which the report names when it happens: a migration
    /// whose `metadata.toml` sets `run_in_transaction = false` (what `CREATE
    /// INDEX CONCURRENTLY` requires) has no transaction to roll back, so any
    /// statement of it that already succeeded still stands. A failing target
    /// stops the run: remaining targets are left untouched rather than
    /// half-migrated behind a database that already failed.
    Run {
        /// Harvest database to migrate: the value of `harvest.database.url`.
        ///
        /// Repeat once per shard database for a multi-shard deployment; they
        /// are migrated in the order given.
        #[arg(
            long = "database-url",
            env = "HARVEST_DATABASE_URL",
            hide_env_values = true,
            value_name = "URL",
            required = true
        )]
        database_url: Vec<String>,
        /// Additional migration directory to include, e.g.
        /// `autumn-harvest-plugin/migrations/harvest` for the connector
        /// dead-letter table. Repeatable; each is a directory of
        /// `<version>_<description>/up.sql` migrations.
        #[arg(long = "include-dir", value_name = "DIR")]
        include_dir: Vec<PathBuf>,
        /// Output format: human-readable `text` (default) or machine-readable
        /// `json`.
        #[arg(long, value_enum, default_value_t)]
        format: MigrateFormat,
        /// Report what would be applied and exit without applying it.
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
}

/// Workflow payload-schema contract subcommands (issue #794).
///
/// Both are pure local file comparison — no database, no network. `--current`
/// is the schema set your app publishes *right now*; generate it with a
/// three-line binary in your own crate (the only place the registry is in
/// scope), or pipe `GET /workflows/registered` straight in.
#[derive(Debug, Subcommand)]
enum SchemaCommand {
    /// Compare the current published schemas against the checked-in baseline
    /// and fail on any backward-incompatible change.
    ///
    /// Exits `0` when every delta is compatible, `1` when any delta is
    /// breaking from the replay-read perspective.
    Check {
        /// Checked-in baseline artifact.
        #[arg(long, value_name = "PATH", default_value = autumn_harvest::DEFAULT_SCHEMA_CONTRACT_PATH)]
        baseline: PathBuf,
        /// Currently published schemas: a generated contract, or a raw
        /// `GET /workflows/registered` response body.
        #[arg(long, value_name = "PATH")]
        current: PathBuf,
        /// Output format: human-readable `text` (default) or machine-readable
        /// `json` (per-type, per-field verdict and reason).
        #[arg(long, value_enum, default_value_t)]
        format: SchemaCheckFormat,
        /// Fail on ANY unabsorbed delta, not only a breaking one.
        ///
        /// Without this the artifact is allowed to lag: a compatible change
        /// passes and nobody regenerates it, so the baseline records what was
        /// deployed *some time ago* while the gate reads it as what was deployed
        /// *last*. That gap round-trips — add an enum variant (compatible, so
        /// nothing is absorbed), deploy and record payloads carrying it, then
        /// remove it again, and the generated contract equals the stale baseline
        /// while replay of the intermediate release's data fails. Use it in CI;
        /// the fix is always `harvest schema update`.
        #[arg(long, default_value_t = false, conflicts_with = "acknowledged_in")]
        require_current: bool,
        /// Verify the ESCAPE HATCH instead of the artifact's freshness.
        ///
        /// Pass the artifact as it stands now, with `--baseline` pointing at the
        /// PREVIOUS revision of it (e.g. `git show <base>:<path>`). Every
        /// breaking delta must then be covered by an acknowledgement that is new
        /// in this artifact — which catches a baseline hand-edited to absorb a
        /// break silently, something the ordinary check cannot see because the
        /// artifact and the generated contract already agree. Also enforces that
        /// the acknowledgement log is append-only: a record the previous
        /// revision carried and this one does not is reported, since retargeting
        /// an existing record would otherwise read as fresh coverage.
        #[arg(long, value_name = "PATH")]
        acknowledged_in: Option<PathBuf>,
    },

    /// Regenerate the checked-in baseline from the current schemas.
    ///
    /// A breaking delta is **refused** unless `--acknowledge` records why it is
    /// safe; the justification is written into the artifact, so the
    /// acknowledgement is visible in the checked-in diff and never silent.
    Update {
        /// Baseline artifact to rewrite in place.
        #[arg(long, value_name = "PATH", default_value = autumn_harvest::DEFAULT_SCHEMA_CONTRACT_PATH)]
        baseline: PathBuf,
        /// Currently published schemas (see `check --current`).
        #[arg(long, value_name = "PATH")]
        current: PathBuf,
        /// Why each breaking change is safe (e.g. "in-flight runs drained;
        /// pinned by build routing #171"). Required when any delta is breaking.
        #[arg(long, value_name = "REASON")]
        acknowledge: Option<String>,
        /// Where the migration is written up — a changelog fragment, PR, or
        /// runbook. Recorded alongside the justification.
        #[arg(long, value_name = "REF")]
        recorded_in: Option<String>,
    },
}

/// Build-routing subcommands (issue #604).
#[derive(Debug, Subcommand)]
enum BuildRoutingCommand {
    /// Manage a queue's percentage build ramp.
    Ramp {
        #[command(subcommand)]
        command: RampCommand,
    },
}

/// Percentage build ramp subcommands (issue #604).
#[derive(Debug, Subcommand)]
enum RampCommand {
    /// Set (or update) a queue's percentage build ramp. Requires a base
    /// build policy to already exist for the queue.
    Set {
        /// Task queue to ramp.
        #[arg(long)]
        queue: String,
        /// Ramp target build ID.
        #[arg(long)]
        target_build_id: String,
        /// Percentage of new starts routed to the target build, 0..=100.
        #[arg(long, value_parser = clap::value_parser!(i32).range(0..=100))]
        percent: i32,
    },
    /// Show current build policies and ramp state for every queue.
    #[command(alias = "list", alias = "ls")]
    Show,
    /// Clear a queue's percentage build ramp, immediately stopping new
    /// starts from reaching the target build.
    Clear {
        /// Task queue whose ramp should be cleared.
        #[arg(long)]
        queue: String,
    },
}

/// Workflow-type reachability subcommands (issue #520).
#[derive(Debug, Subcommand)]
enum WorkflowTypesCommand {
    /// Report per-workflow-type handler reachability.
    ///
    /// Exits `2` when any workflow type is `orphaned` (a non-terminal execution
    /// still depends on a handler this deployment no longer registers) or when
    /// the report is incomplete (a shard was unreachable), so it can gate a
    /// deploy in CI.
    Reachability {
        /// Narrow the report to a single workflow type.
        #[arg(long = "type")]
        workflow_type: Option<String>,
        /// Output raw JSON instead of the summary table.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
enum HistoryCommand {
    /// Export one workflow execution history.
    Export {
        /// Workflow execution ID.
        execution_id: String,
        /// Payload policy. `full` emits sensitive replay fixtures; `redacted` is safer for sharing.
        #[arg(long, value_enum, default_value = "redacted")]
        payload_policy: HistoryExportPayloadPolicy,
        /// Maximum serialized export size in bytes.
        #[arg(long)]
        max_bytes: Option<usize>,
        /// Write the JSON export to a file instead of stdout.
        #[arg(long, value_name = "PATH")]
        output_file: Option<PathBuf>,
    },
    /// Export a bounded batch of workflow histories.
    ExportBatch {
        /// Filter by registered workflow name.
        #[arg(long)]
        workflow_name: Option<String>,
        /// Filter by execution state group.
        #[arg(long, value_enum)]
        state_group: Option<HistoryExportStateGroup>,
        /// Lower bound on latest history event time, RFC 3339.
        #[arg(long)]
        updated_after: Option<String>,
        /// Upper bound on latest history event time, RFC 3339.
        #[arg(long)]
        updated_before: Option<String>,
        /// Restrict inspection to one shard.
        #[arg(long)]
        shard_id: Option<i32>,
        /// Maximum histories to export.
        #[arg(long)]
        limit: Option<usize>,
        /// Payload policy. `full` emits sensitive replay fixtures; `redacted` is safer for sharing.
        #[arg(long, value_enum, default_value = "redacted")]
        payload_policy: HistoryExportPayloadPolicy,
        /// Maximum serialized size per exported history in bytes.
        #[arg(long)]
        max_bytes: Option<usize>,
        /// Write the JSON export to a file instead of stdout.
        #[arg(long, value_name = "PATH")]
        output_file: Option<PathBuf>,
    },
    /// Export a stratified sample of IN-FLIGHT histories as a replay bundle.
    ///
    /// Takes at most `--per-workflow` non-terminal executions per registered
    /// workflow type across every shard and writes one JSON fixture per
    /// execution into `--output-dir`, alongside a
    /// `harvest-sample-manifest.json` reporting how many were sampled versus
    /// how many are actually in flight. Feed the directory to
    /// `WorkflowReplayer::replay_bundle` in CI to block promotion of a build
    /// that would diverge on in-flight work (issue #798).
    ExportSample {
        /// Directory to write the replay bundle into. Created if absent.
        #[arg(long, value_name = "DIR")]
        output_dir: PathBuf,
        /// Maximum executions to sample per workflow type (clamped to 500).
        #[arg(long, default_value_t = autumn_harvest::replay_sample::DEFAULT_PER_WORKFLOW_SAMPLE)]
        per_workflow: usize,
        /// Non-terminal states to sample. Repeatable or comma-separated.
        /// Defaults to RUNNING,PAUSED. A terminal state is rejected.
        #[arg(long, value_name = "STATE")]
        states: Vec<String>,
        /// Restrict the sample to a single registered workflow type.
        #[arg(long)]
        workflow_name: Option<String>,
        /// Which end of each type's in-flight population to sample.
        #[arg(long, value_enum, default_value = "oldest")]
        order: HistorySampleOrder,
        /// Restrict inspection to one shard.
        #[arg(long)]
        shard_id: Option<i32>,
        /// Payload policy. `full` emits sensitive replay fixtures; `redacted` is safer for sharing.
        #[arg(long, value_enum, default_value = "redacted")]
        payload_policy: HistoryExportPayloadPolicy,
        /// Maximum serialized size per exported history in bytes.
        #[arg(long)]
        max_bytes: Option<usize>,
    },
}

/// Which end of each workflow type's in-flight population to sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum HistorySampleOrder {
    /// The longest-running in-flight executions — the ones most likely to span
    /// a code change, and therefore the highest-signal drift sample.
    Oldest,
    /// The most recently started in-flight executions.
    Newest,
}

impl HistorySampleOrder {
    const fn as_wire(self) -> &'static str {
        match self {
            Self::Oldest => "oldest",
            Self::Newest => "newest",
        }
    }
}

#[derive(Debug, Subcommand)]
enum AuditCommand {
    /// List audit records, newest first.
    List {
        /// Filter by operator identity.
        #[arg(long)]
        actor: Option<String>,
        /// Filter by operation name (e.g. `workflow.start`, `dlq.replay`).
        #[arg(long)]
        operation: Option<String>,
        /// Filter by target type (e.g. `workflow`, `schedule`, `dead_letter`).
        #[arg(long)]
        target_type: Option<String>,
        /// Filter by target ID (execution ID, schedule name, DLQ entry ID, …).
        #[arg(long)]
        target_id: Option<String>,
        /// Filter by outcome: `succeeded` or `failed`.
        #[arg(long)]
        status: Option<String>,
        /// Lower bound (inclusive), RFC 3339 (e.g. `2026-01-01T00:00:00Z`).
        #[arg(long)]
        since: Option<String>,
        /// Upper bound (exclusive), RFC 3339.
        #[arg(long)]
        before: Option<String>,
        /// Maximum number of records to return [1–500].
        #[arg(long, value_parser = clap::value_parser!(i64).range(1..=500))]
        limit: Option<i64>,
    },
}

/// Admission gate subcommands (issue #377).
#[derive(Debug, Subcommand)]
enum GateCommand {
    /// Create an admission gate to halt new workflow starts.
    #[command(alias = "add")]
    Create {
        /// Scope: `fleet`, `workflow_name=<name>`, `queue=<name>`, `shard_id=<N>`, or `owner=<id>`.
        #[arg(long)]
        scope: String,
        /// Required human-readable reason included in blocked-caller errors and the audit log.
        #[arg(long)]
        reason: String,
        /// Optional extended message shown in the Vantage UI.
        #[arg(long)]
        message: Option<String>,
        /// ISO 8601 expiry timestamp after which the gate self-clears (e.g. 2026-06-06T12:00:00Z).
        #[arg(long)]
        expires_at: Option<String>,
    },
    /// List all active (non-lifted) admission gates.
    #[command(alias = "ls")]
    List,
    /// Lift (remove) an admission gate by ID.
    #[command(alias = "delete", alias = "rm", alias = "remove")]
    Lift {
        /// Gate ID (UUID) to lift.
        id: String,
    },
}

#[derive(Debug, Subcommand)]
enum TokenCommand {
    /// Mint a scoped API token. The secret is returned exactly once.
    #[command(alias = "add")]
    Create {
        /// Human-readable label for the caller (CI job, dashboard, on-call, SDK).
        name: String,
        /// Verb-level scope: `read` (read-only routes) or `mutate` (everything).
        /// Defaults to `read` (least privilege).
        #[arg(long, default_value = "read")]
        scope: String,
        /// Optional RFC 3339 expiry after which the token is rejected 401.
        #[arg(long)]
        expires_at: Option<String>,
    },
    /// List all tokens as metadata (never the secret/hash).
    #[command(alias = "ls")]
    List,
    /// Revoke a token by ID (effective on the next request).
    #[command(alias = "delete", alias = "rm", alias = "remove")]
    Revoke {
        /// Token ID (UUID) to revoke.
        id: String,
    },
    /// Rotate a token: mint a replacement via the create route. Revoking the
    /// old token is a documented second step (`harvest token revoke <old-id>`).
    Rotate {
        /// The existing token ID being rotated out (used to name the replacement).
        old_id: String,
        /// Scope for the replacement token. Defaults to `read`.
        #[arg(long, default_value = "read")]
        scope: String,
        /// Optional RFC 3339 expiry for the replacement token.
        #[arg(long)]
        expires_at: Option<String>,
    },
    /// Seed the FIRST token offline (issue #942). Prints a fresh secret ONCE
    /// and the exact `INSERT INTO harvest_api_tokens ...` SQL for you to run
    /// against your database — no API call and no DB connection is made.
    ///
    /// Standalone (tokens-only) deployments use this to mint their first
    /// `mutate` token: with tokens as the only auth there is no admin caller
    /// yet to mint one via `POST /admin/tokens`. Run the printed SQL once (you
    /// already have DB access — the trust anchor), then mint every further
    /// token through the API. The printed SQL contains ONLY the hash; the
    /// secret is shown separately for you to store.
    Bootstrap {
        /// Human-readable label for the seed token.
        #[arg(long, default_value = "bootstrap")]
        name: String,
        /// Verb-level scope: `mutate` (can mint further tokens via the API) or
        /// `read`. Defaults to `mutate` so the seed token can bootstrap the rest.
        #[arg(long, default_value = "mutate", value_parser = ["read", "mutate"])]
        scope: String,
        /// Optional RFC 3339 expiry after which the token is rejected 401.
        #[arg(long)]
        expires_at: Option<String>,
        /// Audit provenance recorded as `created_by`.
        #[arg(long, default_value = "bootstrap")]
        created_by: String,
    },
}

#[derive(Debug, Subcommand)]
enum ShardCommand {
    /// Migrate quiescent workflow executions from one shard to another (issue #964).
    ///
    /// Connects to the shard databases DIRECTLY rather than through the
    /// management API: a rebalance is a two-database operation, and the node
    /// serving the API has no reason to hold a pool for both. Supply each shard
    /// with `--shard <ID>=<DSN>`.
    ///
    /// Always dry-run first. `--dry-run` walks the same code path up to the
    /// first write and reports exactly the population a real run would move.
    Rebalance {
        /// Shard to migrate executions OFF, as `<ID>=<DSN>`. Repeat for the
        /// target; both must be supplied.
        #[arg(long = "shard", value_name = "ID=DSN", required = true)]
        shards: Vec<String>,
        /// The shard id to migrate off.
        #[arg(long)]
        from: i32,
        /// The shard id to migrate to.
        #[arg(long)]
        to: i32,
        /// Maximum executions to move in this run.
        #[arg(long, default_value_t = 50)]
        limit: usize,
        /// Report what would move without writing anything.
        #[arg(long)]
        dry_run: bool,
        /// Print the raw JSON report instead of a human table.
        #[arg(long)]
        json: bool,
    },
    /// Drive any migration left unfinished by a crash forward one step (issue #964).
    ///
    /// Idempotent and safe to re-run. A migration killed after its cutover is
    /// completed; one killed before it is either finished or cleanly abandoned,
    /// depending on whether the source woke up in the meantime.
    RebalanceResume {
        /// Shard databases, as `<ID>=<DSN>`. Supply every shard any unfinished
        /// migration touches.
        #[arg(long = "shard", value_name = "ID=DSN", required = true)]
        shards: Vec<String>,
        /// The shard whose migration records to drive.
        #[arg(long)]
        from: i32,
        /// Maximum records to advance in this run.
        #[arg(long, default_value_t = 100)]
        limit: i64,
        /// Print the raw JSON report instead of a human table.
        #[arg(long)]
        json: bool,
    },
    /// Show per-shard readiness and rollout blockers.
    Health {
        /// Evaluate this readable shard as a promotion candidate.
        #[arg(long)]
        candidate_shard: Option<i32>,
        /// Deprecated compatibility flag; shard health gates writable shards by default.
        #[arg(long)]
        fail_on_unready: bool,
    },
}

#[derive(Debug, Subcommand)]
enum WorkflowCommand {
    /// List workflow executions.
    #[command(alias = "ls")]
    List {
        /// Maximum number of rows to return.
        #[arg(long, value_parser = clap::value_parser!(i64).range(1..=200))]
        limit: Option<i64>,
        /// Filter by workflow execution state. Repeat the flag or pass a
        /// comma-separated list to match any of several states.
        #[arg(long, value_delimiter = ',')]
        state: Vec<String>,
        /// Filter by registered workflow name (exact match).
        #[arg(long)]
        workflow_name: Option<String>,
        /// Filter by a `search_attrs` key/value pair (`key=value`). Repeat to
        /// AND multiple predicates together.
        #[arg(long = "search-attr", value_name = "KEY=VALUE")]
        search_attr: Vec<String>,
        /// Filter by a typed comparison/set predicate over a search attribute,
        /// `key:op:value` where op is one of eq, ne, gt, gte, lt, lte, in,
        /// exists (e.g. `amount:gt:10000`, `phase:in:blocked,awaiting_approval`,
        /// `phase:exists`). Repeat to AND multiple predicates together. Forwarded
        /// verbatim to the `search_attr_filter` API param (issue #506).
        #[arg(long = "search-attr-filter", value_name = "KEY:OP:VALUE")]
        search_attr_filter: Vec<String>,
        /// Filter by owner (exact match).
        #[arg(long)]
        owner: Option<String>,
        /// Only return executions that have made no event progress for at
        /// least this many minutes. Excludes workflows correctly sleeping on
        /// a future-dated durable timer unless --include-sleeping is also set.
        #[arg(long)]
        no_progress_minutes: Option<i64>,
        /// Include executions sleeping on a future-dated durable timer in the
        /// stalled-workflow results. Only meaningful with --no-progress-minutes.
        #[arg(long)]
        include_sleeping: bool,
        /// Operator early-warning discovery for workflow history bloat (issue
        /// #704): return only live (non-terminal) executions whose current
        /// recorded event count is at least this many, sorted by history size
        /// descending. Distinct from the server's general-purpose
        /// `min_history_events` filter (issue #493, not currently CLI-exposed),
        /// which composes with `--state`/pagination and does not restrict to
        /// live executions or force this sort order.
        #[arg(long = "history-bloat-min-events")]
        history_bloat_min_events: Option<u64>,
        /// Filter by workflow-start provenance (issue #740): one of api,
        /// schedule, backfill, `signal_with_start`, `update_with_start`,
        /// `completion_trigger`, webhook, child, batch, `continue_as_new`,
        /// reset, outbox, or unknown (matches pre-upgrade/NULL rows). The
        /// server rejects any other value with a 400.
        #[arg(long = "start-source")]
        start_source: Option<String>,
    },
    /// List tiered/summary-retention execution summaries (issue #752).
    ///
    /// Summaries are compact rows the retention janitor demotes a hard-deleted
    /// terminal execution into instead of losing it entirely. Admin-guarded.
    Summaries {
        /// Filter by registered workflow name (exact match).
        #[arg(long)]
        workflow_name: Option<String>,
        /// Filter by workflow ID (exact match).
        #[arg(long)]
        workflow_id: Option<String>,
        /// Filter by terminal state. Repeat the flag or pass a comma-separated
        /// list to match any of several states.
        #[arg(long, value_delimiter = ',')]
        state: Vec<String>,
        /// Only summaries whose `completed_at` is on or after this RFC 3339
        /// timestamp (e.g. 2026-01-01T00:00:00Z).
        #[arg(long)]
        completed_after: Option<String>,
        /// Only summaries whose `completed_at` is on or before this RFC 3339
        /// timestamp.
        #[arg(long)]
        completed_before: Option<String>,
        /// Filter by a `search_attrs` key/value pair (`key=value`). Repeat to
        /// AND multiple containment predicates together.
        #[arg(long = "search-attr", value_name = "KEY=VALUE")]
        search_attr: Vec<String>,
        /// Maximum number of rows to return.
        #[arg(long, value_parser = clap::value_parser!(i64).range(1..=500))]
        limit: Option<i64>,
        /// Opaque keyset pagination cursor returned by the previous response.
        #[arg(long)]
        cursor: Option<String>,
        /// Sort direction: `desc` (default, newest-first) or `asc`.
        #[arg(long, value_parser = ["asc", "desc"])]
        order: Option<String>,
        /// Print the raw JSON API payload instead of a human table.
        #[arg(long)]
        json: bool,
    },
    /// Get one workflow execution and event history.
    Get {
        /// Workflow execution ID.
        execution_id: String,
    },
    /// Show what a workflow is currently waiting on.
    Stack {
        /// Workflow execution ID.
        execution_id: String,
    },
    /// Reconstruct a workflow execution's timeline (per-step durations, wait vs
    /// exec split, slowest step) from recorded history.
    Timeline {
        /// Workflow execution ID.
        execution_id: String,
    },
    /// Read the durable per-execution author log lines a workflow emitted via
    /// `ctx.logger()` / `ctx.log_info` / `log_warn` / `log_error`.
    ///
    /// Requires the opt-in durable sink
    /// (`HarvestBuilder::workflow_log_persistence`); with it disabled every
    /// execution returns an empty list. Lines are observational only: they are
    /// not part of the event history and are never replayed.
    Logs {
        /// Workflow execution ID.
        execution_id: String,
        /// Only show lines at this level. Repeat, or comma-separate, to allow
        /// several. One of: info, warn, error.
        #[arg(long = "level")]
        level: Vec<String>,
        /// Page size (default 200, max 1000).
        #[arg(long)]
        limit: Option<i64>,
        /// Exclusive keyset cursor: the previous page's last `seq`.
        #[arg(long)]
        cursor: Option<i64>,
        /// RFC 3339 exclusive lower bound on `occurred_at`.
        #[arg(long)]
        since: Option<String>,
    },
    /// Show the open awaitables an execution is parked on (pending activities,
    /// unfired timers, awaited-but-unsent signals, pending children,
    /// `await_condition` parks, pending updates), replay-derived.
    Awaitables {
        /// Workflow execution ID.
        execution_id: String,
    },
    /// Diagnose why one execution is stuck: a one-word health verdict plus the
    /// discriminated root cause (no live worker on the queue, circuit open,
    /// retry backoff, rate-limited, concurrency-deferred, queue paused,
    /// awaiting a signal, sleeping on a timer, ...), in a single call.
    ///
    /// For a determinism verdict on a code change instead, see
    /// `workflow replay-diagnosis`.
    Diagnose {
        /// Workflow execution ID.
        execution_id: String,
        /// Print the raw JSON body instead of the rendered verdict.
        #[arg(long)]
        json: bool,
    },
    /// Render the recursive descendant lineage tree for an execution across all
    /// shards — the spawn topology of a saga or fan-out, annotated with each
    /// descendant's state.
    Tree {
        /// Root workflow execution ID (may be any node in the tree; the
        /// response is the subtree below it).
        execution_id: String,
        /// Print the per-state descendant roll-up instead of the tree.
        #[arg(long)]
        summary: bool,
        /// Recursion depth cap below the root (1-50, default 20).
        #[arg(long, value_parser = clap::value_parser!(u32).range(1..=50))]
        max_depth: Option<u32>,
        /// Total node cap including the root (1-10000, default 1000).
        #[arg(long, value_parser = clap::value_parser!(u32).range(1..=10_000))]
        max_nodes: Option<u32>,
        /// Emit raw JSON instead of the default rendering.
        #[arg(long)]
        json: bool,
    },
    /// Reconstruct the ordered continue-as-new run chain a workflow execution
    /// belongs to, resolvable from any member (origin, middle, or tail).
    RunChain {
        /// Workflow execution ID of any member of the chain.
        execution_id: String,
        /// Emit raw JSON instead of the default table.
        #[arg(long)]
        json: bool,
    },
    /// Replay a single execution's recorded history against the currently
    /// registered workflow handler and report a structured determinism verdict
    /// (clean, diverged, failed, not-registered, or not-replayable).
    ///
    /// For a stall root cause instead, see `workflow diagnose`.
    ReplayDiagnosis {
        /// Workflow execution ID to replay.
        execution_id: String,
    },
    /// List child workflow executions for a parent execution.
    Children {
        /// Parent workflow execution ID.
        execution_id: String,
        /// Filter by child workflow status. Repeat the flag or pass a
        /// comma-separated list to match any of several statuses.
        #[arg(long, value_delimiter = ',')]
        status: Vec<String>,
        /// Filter by registered child workflow name (exact match).
        #[arg(long)]
        workflow_name: Option<String>,
        /// Maximum number of rows to return.
        #[arg(long, value_parser = clap::value_parser!(i64).range(1..=500))]
        limit: Option<i64>,
        /// Opaque pagination cursor returned by the previous response.
        #[arg(long)]
        cursor: Option<String>,
        /// Recursive descent depth; 0 returns direct children only.
        #[arg(long, value_parser = clap::value_parser!(u8).range(0..=5))]
        depth: Option<u8>,
        /// Print the raw JSON API payload instead of a human table.
        #[arg(long)]
        json: bool,
    },
    /// Start a workflow execution.
    Start {
        /// Registered workflow name.
        workflow_name: String,
        /// Stable workflow ID for idempotent starts.
        #[arg(long)]
        workflow_id: Option<String>,
        /// Queue to place the initial workflow task on.
        #[arg(long)]
        queue: Option<String>,
        /// Inline JSON workflow input.
        #[arg(long, conflicts_with = "input_file")]
        input_json: Option<String>,
        /// File containing JSON workflow input. Use `-` for stdin.
        #[arg(long, value_name = "PATH", conflicts_with = "input_json")]
        input_file: Option<PathBuf>,
        /// Inline JSON memo.
        #[arg(long, conflicts_with = "memo_file")]
        memo_json: Option<String>,
        /// File containing JSON memo. Use `-` for stdin.
        #[arg(long, value_name = "PATH", conflicts_with = "memo_json")]
        memo_file: Option<PathBuf>,
        /// Inline JSON search attributes.
        #[arg(long, conflicts_with = "search_attrs_file")]
        search_attrs_json: Option<String>,
        /// File containing JSON search attributes. Use `-` for stdin.
        #[arg(long, value_name = "PATH", conflicts_with = "search_attrs_json")]
        search_attrs_file: Option<PathBuf>,
        /// Execution timeout in seconds.
        #[arg(long)]
        execution_timeout_secs: Option<i64>,
        /// How to handle a duplicate `(workflow_name, workflow_id)` start.
        /// One of: `allow_duplicate` (default), `reject_duplicate`,
        /// `allow_duplicate_failed_only`, `terminate_if_running`.
        #[arg(long, value_name = "POLICY")]
        reuse_policy: Option<String>,
        /// How to handle a collision with a currently-active (RUNNING/PAUSED)
        /// prior (issue #685). Orthogonal to `--reuse-policy` (which governs
        /// terminal priors). One of: `unspecified` (default), `fail`,
        /// `use_existing`, `terminate_existing`.
        #[arg(long, value_name = "POLICY")]
        conflict_policy: Option<String>,
        /// Target ISO 8601 / RFC 3339 timestamp to start the workflow.
        #[arg(long)]
        start_at: Option<String>,
        /// Delay duration before starting the workflow (e.g. "10s", "5m").
        #[arg(long)]
        delay: Option<String>,
        /// Pin this workflow to a concrete shard (issue #697). Mutually
        /// exclusive with `--residency-key`. An unknown or drained shard is
        /// rejected with `400`, never silently re-hashed.
        #[arg(long, value_name = "SHARD", conflicts_with = "residency_key")]
        shard_id: Option<i32>,
        /// Pin this workflow via an operator-declared residency key (issue
        /// #697), e.g. `eu`. Mutually exclusive with `--shard-id`. An
        /// undeclared key is rejected with `400`.
        #[arg(long, value_name = "KEY")]
        residency_key: Option<String>,
    },
    /// Cancel a workflow execution.
    Cancel {
        /// Workflow execution ID.
        execution_id: String,
        /// Cancellation reason.
        #[arg(long)]
        reason: Option<String>,
    },
    /// Pause a running workflow execution (operator intervention).
    ///
    /// While paused the executor dispatches no new commands for the execution;
    /// in-flight activities run to completion. PAUSED is a non-terminal active
    /// state — resume with `harvest workflow resume`. Pausing an
    /// already-paused execution is a no-op.
    Pause {
        /// Workflow execution ID.
        execution_id: String,
        /// Human-readable pause reason (max 500 chars), recorded in audit log.
        #[arg(long)]
        reason: Option<String>,
    },
    /// Resume a paused workflow execution, waking the parked task.
    Resume {
        /// Workflow execution ID.
        execution_id: String,
    },
    /// Set, update, or clear an execution's operator-mutable triage tags:
    /// owner, severity, and a free-text note (issue #759).
    ///
    /// A plain metadata update on the execution row -- appends no workflow
    /// event, is never read by the workflow function, and has zero
    /// replay-determinism impact; distinct from author-controlled
    /// `search_attrs`. `owner`/`severity` set this way are immediately
    /// reflected in `harvest workflow list --owner ... --severity ...`.
    /// Works on any lifecycle state (RUNNING, PAUSED, FAILED, COMPLETED,
    /// ...). Idempotent: re-running with the same flags is a no-op.
    Annotate {
        /// Workflow execution ID.
        execution_id: String,
        /// New owner (e.g. a team or on-call handle).
        #[arg(long, conflicts_with = "clear_owner")]
        owner: Option<String>,
        /// Clear the owner (sends an explicit JSON null).
        #[arg(long)]
        clear_owner: bool,
        /// New severity/priority label (freeform, e.g. "P1").
        #[arg(long, conflicts_with = "clear_severity")]
        severity: Option<String>,
        /// Clear the severity (sends an explicit JSON null).
        #[arg(long)]
        clear_severity: bool,
        /// New free-text triage note.
        #[arg(long, conflicts_with = "clear_note")]
        note: Option<String>,
        /// Clear the note (sends an explicit JSON null).
        #[arg(long)]
        clear_note: bool,
    },
    /// Erase PII payload fields from a completed workflow execution (GDPR Art. 17).
    ///
    /// Replaces all payload-bearing fields (`input`, `output`, `payload`, `details`,
    /// `value`, `last_completion_result`) with a tombstone marker. The execution
    /// itself and its audit trail are preserved. Only terminal executions
    /// (COMPLETED, FAILED, CANCELLED, `TIMED_OUT`, `CONTINUED_AS_NEW`, TERMINATED)
    /// can be erased. Cascades to terminal child executions on the same shard.
    /// This operation is irreversible.
    ErasePayloads {
        /// Workflow execution ID.
        execution_id: String,
        /// Erasure reason (e.g. "GDPR Art. 17 request ID: DSR-12345"), recorded in audit log.
        #[arg(long)]
        reason: Option<String>,
    },
    /// Force a backing-off activity to retry immediately, skipping its backoff.
    RetryActivity {
        /// Workflow execution ID.
        workflow_id: String,
        /// Activity execution ID (the id surfaced by `workflow stack`).
        activity_exec_id: String,
    },
    /// Force-fail a hung in-flight (RUNNING) activity, skipping all remaining
    /// retries.
    ///
    /// The owning workflow observes the distinct `OperatorForceFailed` error
    /// type and advances to its own failure/compensation path — it is NOT
    /// terminated. Re-issuing the command on an already-forced activity is an
    /// idempotent no-op success.
    FailActivity {
        /// Workflow execution ID.
        workflow_id: String,
        /// Activity execution ID (the id surfaced by `workflow stack`).
        activity_exec_id: String,
        /// Human-readable reason recorded in the forced failure (e.g. an
        /// incident id).
        #[arg(long)]
        reason: Option<String>,
    },
    /// Fork a workflow execution at an event boundary.
    Reset {
        /// Workflow execution ID.
        execution_id: String,
        /// Last event ID to carry into the fork.
        #[arg(long = "to-event")]
        reset_to_event_id: i64,
        /// Recovery reason recorded in reset marker events.
        #[arg(long)]
        reason: String,
        /// Operator identity recorded in reset marker events.
        #[arg(long, default_value = "cli")]
        operator_id: String,
        /// How to handle undelivered source signals.
        #[arg(long, value_enum, default_value = "drop")]
        signal_reapply: ResetSignalReapply,
        /// Validate and print the reset plan without committing.
        #[arg(long)]
        dry_run: bool,
    },
    /// Re-run a terminal workflow execution as a brand-new run.
    ///
    /// The complement to `reset`: reset forks an execution mid-history so the
    /// surviving prefix is replayed, whereas a re-run replays nothing — it
    /// starts the whole workflow over from the source run's recorded start
    /// parameters (input, queue, memo, search attributes, timeouts, SLA,
    /// owner/severity, context headers, retry policy, completion callbacks).
    /// The source must be terminal (`COMPLETED`, `FAILED`, `CANCELLED`,
    /// `TIMED_OUT`, `TERMINATED`); a `RUNNING`/`PAUSED` or already-sealed
    /// `CONTINUED_AS_NEW` source is rejected. Admin-only, and NOT idempotent —
    /// re-running the default way seals the source to `CONTINUED_AS_NEW` to
    /// free its business key, so a second identical call returns `409`.
    Rerun {
        /// Workflow execution ID of the terminal source run.
        execution_id: String,
        /// Inline JSON input override for the new run. Omit to clone the
        /// source's stored input verbatim; required when the source's input
        /// has been erased.
        #[arg(long, conflicts_with = "input_file")]
        input_json: Option<String>,
        /// File containing the JSON input override. Use `-` for stdin.
        #[arg(long, value_name = "PATH", conflicts_with = "input_json")]
        input_file: Option<PathBuf>,
        /// Business-key override for the new run. Omit to reuse the source's
        /// workflow ID, which seals the source to `CONTINUED_AS_NEW`; supplying
        /// one starts under a different key and leaves the source untouched.
        #[arg(long)]
        workflow_id: Option<String>,
    },
    /// Send a signal to a workflow execution.
    Signal {
        /// Workflow execution ID.
        execution_id: String,
        /// Registered signal name.
        signal_name: String,
        /// Inline JSON signal payload.
        #[arg(long, conflicts_with = "payload_file")]
        payload_json: Option<String>,
        /// File containing JSON signal payload. Use `-` for stdin.
        #[arg(long, value_name = "PATH", conflicts_with = "payload_json")]
        payload_file: Option<PathBuf>,
        /// Exactly-once delivery key (issue #753). Repeated deliveries with
        /// the same key for the same execution land exactly one
        /// `SignalReceived` event; the response reports
        /// `signal_delivered=false` for the deduped retries. Omit to keep the
        /// legacy at-least-once behavior (every call delivers a distinct
        /// signal event). Typically a stable upstream event id (e.g. a Stripe
        /// event id or SQS message id). An empty key is rejected — the server
        /// treats an empty `?idempotency_key=` as omitted, which would
        /// silently degrade an intended exactly-once delivery to
        /// at-least-once.
        #[arg(long, value_name = "KEY", value_parser = parse_idempotency_key)]
        idempotency_key: Option<String>,
    },
    /// Query workflow state.
    Query {
        /// Workflow execution ID.
        execution_id: String,
        /// Registered query name.
        query_name: String,
    },
    /// Send a synchronous update request to a running workflow.
    Update {
        /// Workflow execution ID.
        execution_id: String,
        /// Registered update handler name.
        update_name: String,
        /// Inline JSON input for the update handler.
        #[arg(long, conflicts_with = "input_file")]
        input_json: Option<String>,
        /// File containing JSON input. Use `-` for stdin.
        #[arg(long, value_name = "PATH", conflicts_with = "input_json")]
        input_file: Option<PathBuf>,
        /// How long to wait for the result.
        /// `admitted` — return immediately after durable admission (202).
        /// `completed` — block until the handler returns (default).
        #[arg(long, value_name = "MODE", default_value = "completed")]
        wait: String,
        /// Timeout in seconds when `--wait completed` (default: 30).
        #[arg(long, value_name = "SECS")]
        timeout_secs: Option<u64>,
    },
    /// Look up the durable result of a previously admitted update.
    UpdateResult {
        /// Workflow execution ID.
        execution_id: String,
        /// Update ID returned by a prior `harvest workflow update` call.
        update_id: String,
    },
    /// List declarative query and update handlers registered for a workflow type.
    Handlers {
        /// Registered workflow name.
        workflow_name: String,
    },
    /// Reset a cohort of workflow executions to a shared semantic point (issue #538).
    ///
    /// Selects candidates via the filter grammar, resolves the logical anchor
    /// per execution, and returns a per-execution outcome list. Use
    /// `--preview` to resolve without forking.
    BatchReset {
        /// Inline JSON filter (e.g. `'{"states":["FAILED"],"workflow_name":"my_flow"}'`).
        #[arg(long, conflicts_with = "filter_file")]
        filter_json: Option<String>,
        /// File containing the JSON filter. Use `-` for stdin.
        #[arg(long, value_name = "PATH", conflicts_with = "filter_json")]
        filter_file: Option<PathBuf>,
        /// Reset to a specific event ID (preserved for each execution individually).
        #[arg(long, conflicts_with_all = ["first_activity", "last_workflow_task"])]
        event_id: Option<i64>,
        /// Reset each execution just before the first scheduling of this activity name.
        #[arg(long, value_name = "ACTIVITY_NAME", conflicts_with_all = ["event_id", "last_workflow_task"])]
        first_activity: Option<String>,
        /// Reset each execution to the most-recent clean workflow-task boundary.
        #[arg(long, conflicts_with_all = ["event_id", "first_activity"])]
        last_workflow_task: bool,
        /// Recovery reason recorded in reset marker events.
        #[arg(long)]
        reason: String,
        /// Operator identity recorded in reset marker events.
        #[arg(long, default_value = "cli")]
        operator_id: String,
        /// How to handle undelivered source signals.
        #[arg(long, value_enum, default_value = "drop")]
        signal_reapply: ResetSignalReapply,
        /// Resolve the semantic point per execution but do not fork any execution.
        #[arg(long)]
        preview: bool,
    },
}

#[derive(Debug, Subcommand)]
enum HandoffCommand {
    /// List external activity handoffs.
    List {
        /// Filter by handoff state. Repeat the flag or pass a comma-separated list.
        #[arg(long, value_delimiter = ',')]
        state: Vec<String>,
        /// Filter by registered workflow name.
        #[arg(long)]
        workflow_name: Option<String>,
        /// Filter by workflow execution ID.
        #[arg(long)]
        execution_id: Option<String>,
        /// Filter by registered activity name.
        #[arg(long)]
        activity_name: Option<String>,
        /// Filter by external activity token.
        #[arg(long)]
        token: Option<String>,
        /// Restrict inspection to one shard.
        #[arg(long)]
        shard_id: Option<i32>,
        /// Return handoffs due before this RFC3339 timestamp.
        #[arg(long)]
        due_before: Option<String>,
        /// Return handoffs last updated before this RFC3339 timestamp.
        #[arg(long)]
        updated_before: Option<String>,
        /// Maximum number of rows to return.
        #[arg(long, value_parser = clap::value_parser!(i64).range(1..=500))]
        limit: Option<i64>,
        /// Print the raw JSON API payload instead of a human table.
        #[arg(long)]
        json: bool,
    },
    /// Inspect one external activity handoff by token.
    #[command(alias = "get")]
    Inspect {
        /// External activity token.
        token: String,
        /// Print the raw JSON API payload instead of a human table.
        #[arg(long)]
        json: bool,
    },
    /// Complete an external activity handoff by token.
    Complete {
        /// External activity token.
        token: String,
        /// JSON value to send as the activity output.
        #[arg(long, conflicts_with = "output_file")]
        output_json: Option<String>,
        /// File containing JSON output. Use `-` for stdin.
        #[arg(long, value_name = "PATH", conflicts_with = "output_json")]
        output_file: Option<PathBuf>,
        /// Full JSON request body. Use this to send `{ "output": ... }` directly.
        #[arg(long, conflicts_with = "request_file")]
        request_json: Option<String>,
        /// File containing the full JSON request body. Use `-` for stdin.
        #[arg(long, value_name = "PATH", conflicts_with = "request_json")]
        request_file: Option<PathBuf>,
    },
    /// Fail an external activity handoff by token.
    Fail {
        /// External activity token.
        token: String,
        /// String error to record on the external activity.
        #[arg(long)]
        error: Option<String>,
        /// JSON error details, compacted into the recorded error string.
        #[arg(long, conflicts_with = "error_file")]
        error_json: Option<String>,
        /// File containing JSON error details. Use `-` for stdin.
        #[arg(long, value_name = "PATH", conflicts_with = "error_json")]
        error_file: Option<PathBuf>,
        /// Full JSON request body. Use this to send `{ "error": "...", "retryable": false }`.
        #[arg(long, conflicts_with = "request_file")]
        request_json: Option<String>,
        /// File containing the full JSON request body. Use `-` for stdin.
        #[arg(long, value_name = "PATH", conflicts_with = "request_json")]
        request_file: Option<PathBuf>,
        /// Mark the external failure as retryable for workflow replay.
        #[arg(long)]
        retryable: bool,
    },
    /// Heartbeat an external activity handoff and optionally extend its deadline.
    #[command(alias = "extend")]
    Heartbeat {
        /// External activity token.
        token: String,
        /// Seconds to extend the deadline from now.
        #[arg(long)]
        extend_by_secs: Option<u64>,
        /// Full JSON request body. Use this to send `{ "extend_by_secs": 3600 }`.
        #[arg(long, conflicts_with = "request_file")]
        request_json: Option<String>,
        /// File containing the full JSON request body. Use `-` for stdin.
        #[arg(long, value_name = "PATH", conflicts_with = "request_json")]
        request_file: Option<PathBuf>,
    },
}

#[derive(Debug, Subcommand)]
enum DagCommand {
    /// List DAG schedules.
    List,
    /// List runs for a DAG.
    Runs {
        /// Registered DAG name.
        dag_name: String,
    },
    /// Trigger a DAG run.
    Trigger {
        /// Registered DAG name.
        dag_name: String,
        /// Inline JSON DAG run config.
        #[arg(long, conflicts_with = "conf_file")]
        conf_json: Option<String>,
        /// File containing JSON DAG run config. Use `-` for stdin.
        #[arg(long, value_name = "PATH", conflicts_with = "conf_json")]
        conf_file: Option<PathBuf>,
    },
    /// Pause a DAG schedule.
    Pause {
        /// Registered DAG name.
        dag_name: String,
    },
    /// Unpause a DAG schedule.
    Unpause {
        /// Registered DAG name.
        dag_name: String,
    },
    /// Retry a failed DAG run from one or more failed nodes (issue #366).
    ///
    /// Re-executes the named node(s) and every node declared downstream of
    /// them, carrying over the recorded results of all upstream nodes. Use
    /// `--dry-run` first to preview the resolved reset point and the exact
    /// re-execute / carry-over sets without committing.
    Retry {
        /// Registered (unified) DAG name.
        dag_name: String,
        /// The failed DAG run's execution id.
        run_exec_id: String,
        /// Node (activity) name to retry from. Repeatable.
        #[arg(long = "from-node", value_name = "NODE", required = true)]
        from_node: Vec<String>,
        /// Operator-supplied recovery reason (recorded in the audit trail).
        #[arg(long)]
        reason: String,
        /// Operator identity for audit. Defaults to the global `--actor`.
        #[arg(long)]
        operator_id: Option<String>,
        /// Preview the plan without committing any write.
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Debug, Subcommand)]
enum ScheduleCommand {
    /// List all schedules (DAG and workflow), tagged with kind.
    List,
    /// Create or update a workflow schedule.
    CreateWorkflow {
        /// Registered workflow name to schedule.
        #[arg(long)]
        name: String,
        /// Cron expression (e.g. `"0 3 * * *"`) or `"interval:<secs>"`.
        #[arg(long)]
        cron: String,
        /// Inline JSON input passed to each scheduled run.
        #[arg(long, value_name = "JSON", conflicts_with = "input_file")]
        input_json: Option<String>,
        /// File containing JSON input. Use `-` for stdin.
        #[arg(long, value_name = "PATH", conflicts_with = "input_json")]
        input_file: Option<PathBuf>,
        /// Maximum concurrent in-flight runs (default: 1).
        #[arg(long, default_value_t = 1)]
        max_active_runs: u32,
        /// Backfill missed runs when the scheduler was down.
        #[arg(long)]
        catchup: bool,
        /// Create the schedule in a paused state.
        #[arg(long)]
        paused: bool,
    },
    /// Edit an existing schedule in place — partial update, `schedule_id` preserved (issue #771).
    Update {
        /// Schedule row ID (UUID).
        id: String,
        /// New cron expression (e.g. `"0 3 * * *"`).
        #[arg(long, conflicts_with_all = ["interval_secs", "manual"])]
        cron: Option<String>,
        /// New interval in seconds.
        #[arg(long, conflicts_with_all = ["cron", "manual"])]
        interval_secs: Option<u64>,
        /// Switch the schedule to manual-only firing.
        #[arg(long, conflicts_with_all = ["cron", "interval_secs"])]
        manual: bool,
        /// New IANA timezone for the cron expression (e.g. `"America/New_York"`).
        #[arg(long)]
        tz: Option<String>,
        /// New inline JSON input passed to each scheduled run. Any non-null
        /// JSON value; a literal `null` leaves the stored input unchanged
        /// (null is the one JSON value that cannot be set as the input).
        #[arg(long, value_name = "JSON")]
        input_json: Option<String>,
        /// New task queue name for scheduled runs.
        #[arg(long)]
        queue: Option<String>,
        /// New overlap policy: skip, `buffer_one`, `buffer_all`, `cancel_other`, `terminate_other`.
        #[arg(long)]
        overlap_policy: Option<String>,
        /// New maximum buffered slots under `buffer_all`.
        #[arg(long)]
        buffer_all_max: Option<u32>,
        /// New catchup policy: `skip_all`, `most_recent`, window, unbounded.
        #[arg(long)]
        catchup_policy: Option<String>,
        /// Window length in seconds for `catchup_policy` = window.
        #[arg(long)]
        catchup_window_secs: Option<i64>,
        /// New jitter window in seconds (0 disables jitter).
        #[arg(long)]
        jitter_secs: Option<u64>,
        /// New maximum concurrent in-flight runs.
        #[arg(long)]
        max_active_runs: Option<u32>,
        /// Attach a named calendar.
        #[arg(long, conflicts_with = "clear_calendar")]
        calendar: Option<String>,
        /// Detach the calendar (sends an explicit JSON null).
        #[arg(long)]
        clear_calendar: bool,
        /// New absolute UTC cutoff, RFC 3339 (e.g. 2030-01-01T00:00:00Z).
        #[arg(long, conflicts_with = "clear_end_at")]
        end_at: Option<String>,
        /// Remove the `end_at` cutoff (sends an explicit JSON null).
        #[arg(long)]
        clear_end_at: bool,
        /// New total run budget.
        #[arg(long, conflicts_with = "clear_max_runs")]
        max_runs: Option<u32>,
        /// Remove the run budget (sends an explicit JSON null).
        #[arg(long)]
        clear_max_runs: bool,
    },
    /// Backfill missed scheduled runs over an explicit time window.
    Backfill {
        /// Schedule row ID (UUID).
        id: String,
        /// Start of the backfill window, RFC 3339 (e.g. 2026-04-01T00:00:00Z). Required.
        #[arg(long, required = true)]
        from: String,
        /// End of the backfill window, RFC 3339 (e.g. 2026-04-08T00:00:00Z). Required.
        #[arg(long, required = true)]
        to: String,
        /// Preview planned timestamps without dispatching any runs.
        #[arg(long)]
        dry_run: bool,
        /// Maximum number of timestamps to plan (default: server-side limit of 1000).
        #[arg(long)]
        max_count: Option<u64>,
        /// Backfill even if the schedule is currently paused.
        #[arg(long)]
        include_paused: bool,
    },
    /// Pause a schedule (works for both DAG and workflow schedules).
    Pause {
        /// Schedule row ID (UUID).
        id: String,
    },
    /// Resume a paused schedule.
    Resume {
        /// Schedule row ID (UUID).
        id: String,
    },
    /// Delete a schedule.
    Delete {
        /// Schedule row ID (UUID).
        id: String,
    },
    /// Trigger an immediate one-off run of a schedule.
    TriggerNow {
        /// Schedule row ID (UUID).
        id: String,
        /// Optional free-text reason recorded in the audit trail.
        #[arg(long)]
        reason: Option<String>,
        /// Force-trigger even if the schedule is currently paused.
        #[arg(long)]
        force: bool,
    },
    /// List the runs a schedule launched, newest-first, with terminal outcomes.
    Runs {
        /// Schedule row ID (UUID).
        id: String,
        /// Filter by execution state (repeatable), e.g. --state FAILED --state `TIMED_OUT`.
        #[arg(long = "state")]
        state: Vec<String>,
        /// Filter by dispatch origin (repeatable): scheduled, backfill, `manual_trigger`.
        #[arg(long = "origin")]
        origin: Vec<String>,
        /// Only runs started at/after this RFC 3339 time or relative duration (e.g. 24h).
        #[arg(long)]
        since: Option<String>,
        /// Only runs started before this RFC 3339 time or relative duration.
        #[arg(long)]
        until: Option<String>,
        /// Maximum runs to return (default 20, clamped 1-200).
        #[arg(long)]
        limit: Option<u32>,
        /// Opaque keyset cursor from a prior response's `next_cursor`.
        #[arg(long)]
        cursor: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum RetentionCommand {
    /// Show retention config and last tick results.
    Status,
    /// Trigger a retention tick immediately.
    RunNow,
}

#[derive(Debug, Subcommand)]
enum ConcurrencyCommand {
    /// Show per-key concurrency stats: cap, in-flight, and pending counts.
    Status,
}

/// Task-queue pause/resume (issue #619): hold dispatch on a whole queue while a
/// downstream dependency is down, then thaw it.
///
/// A pause never fails, retries, or dead-letters work — held tasks stay
/// `PENDING` and become claimable again the instant the queue is resumed, with
/// the time they spent held credited back so the thaw does not retroactively
/// schedule-to-start-time-out the backlog.
#[derive(Debug, Subcommand)]
enum QueueCommand {
    /// Hold dispatch on a task queue.
    Pause {
        /// Task queue name.
        queue_name: String,
        /// Why the queue is being held (recorded on the pause row, surfaced by
        /// `harvest queue list-paused` and the Vantage Workers page).
        #[arg(long)]
        reason: String,
        /// Restrict the hold to one shard. Omit for a fleet-wide pause (the
        /// default).
        #[arg(long)]
        shard_id: Option<i32>,
    },
    /// Release a held task queue; held tasks become immediately claimable.
    Resume {
        /// Task queue name.
        queue_name: String,
        /// Restrict the release to one shard. Omit for fleet-wide (the default).
        #[arg(long)]
        shard_id: Option<i32>,
    },
    /// List every currently-paused queue with its reason and held-task count.
    #[command(alias = "list", alias = "status")]
    ListPaused,
    /// Report task queues with pending work but zero live workers polling
    /// them (issue #774).
    ///
    /// Exits `2` when any queue is uncovered, or when the report is
    /// incomplete (a shard was unreachable), so it can gate a deploy or
    /// migration in CI.
    Coverage {
        /// Narrow the report to a single queue name.
        #[arg(long = "queue")]
        queue_name: Option<String>,
        /// Output raw JSON instead of the summary table.
        #[arg(long)]
        json: bool,
    },
}

/// Per-activity-type pause/resume (issue #807): the surgical sibling of the
/// queue pause above.
///
/// Hold dispatch for ONE activity type while a scoped downstream dependency is
/// down, leaving every other activity type on the same queue flowing. Held
/// tasks stay `PENDING` — never failed, retried, or dead-lettered — and an
/// already-running attempt finishes naturally. On resume the time each task
/// spent held is credited back to its `scheduled_at`, so the thaw does not
/// retroactively schedule-to-start-time-out the backlog.
///
/// Reach for `harvest queue pause` instead when the WHOLE queue must stop; for
/// many affected activity types on one queue, one queue hold is easier to
/// remember to release than N activity holds.
#[derive(Debug, Subcommand)]
enum ActivityCommand {
    /// Hold dispatch for one activity type, fleet-wide.
    Pause {
        /// Activity type name, matched exactly (no trimming).
        activity_name: String,
        /// Why the activity is being held. Recorded on the pause row, surfaced
        /// by `harvest activity list`, and carried into the audit trail — which
        /// is the reason's only permanent home, since resume deletes the row.
        #[arg(long)]
        reason: Option<String>,
        /// Operator identity to record as `paused_by`. A LABEL for attribution,
        /// not authentication: the audit row always carries the authenticated
        /// actor, so this can never rewrite the security trail.
        #[arg(long)]
        actor: Option<String>,
    },
    /// Release a held activity type; held tasks become immediately claimable.
    Resume {
        /// Activity type name, matched exactly (no trimming).
        activity_name: String,
    },
    /// List registered activity types with their pause state and held backlog.
    ///
    /// Driven by the registered catalogue, not the pause table, so a healthy
    /// activity is listed with `paused: false` — this is the surface used to
    /// answer "is `charge_card` held?" during an incident. A paused-but-
    /// unregistered activity is listed too, flagged `registered: false`, so a
    /// mistyped hold can never become invisible here.
    #[command(alias = "list-paused", alias = "status")]
    List {
        /// Print the raw JSON body instead of the human-readable table.
        #[arg(long)]
        json: bool,
    },
    /// Show one activity type's pause state.
    ///
    /// Exits non-zero when the name is neither registered nor paused anywhere
    /// the read could reach (HTTP 404).
    Get {
        /// Activity type name, matched exactly (no trimming).
        activity_name: String,
    },
}

/// Per-execution legal hold (issue #747): exempt an execution's history from
/// retention deletion and PII erasure until released or auto-expired.
#[derive(Debug, Subcommand)]
enum LegalHoldCommand {
    /// Place (or refresh) a legal hold on an execution.
    Set {
        /// Workflow execution ID.
        execution_id: String,
        /// Justification for the hold (recorded in the audit trail and
        /// `legal_hold_reason`).
        #[arg(long)]
        reason: String,
        /// Optional RFC3339 auto-expiry (e.g. `2027-01-01T00:00:00Z`). Omit for
        /// an indefinite hold.
        #[arg(long)]
        until: Option<String>,
    },
    /// Release a legal hold on an execution.
    Release {
        /// Workflow execution ID.
        execution_id: String,
    },
}

#[derive(Debug, Subcommand)]
enum RateLimitCommand {
    /// Show all active per-activity rate limit token buckets and refill rates.
    Status,
    /// Insert or dynamically override a rate limit configuration.
    Set {
        /// Opaque rate limit identifier key.
        key: String,
        /// Rate at which tokens are added to the bucket per second.
        #[arg(long)]
        refill_rate: f64,
        /// Maximum capacity of the token bucket.
        #[arg(long)]
        burst: f64,
    },
    /// Set (or replace) a TTL'd runtime pacing override on top of a
    /// declared per-activity rate limit (issue #945).
    ///
    /// Takes effect on the existing token-consumption path immediately,
    /// with no worker restart; self-expires and reverts to the declared
    /// baseline at the TTL with no further operator action. Each call
    /// fully replaces the whole override.
    Override {
        /// Name of a registered activity that declares a static
        /// (non-dynamic) rate limit.
        activity_name: String,
        /// Overridden token refill rate per second. At least one of
        /// --refill-rate/--burst must be set.
        #[arg(long)]
        refill_rate: Option<f64>,
        /// Overridden token bucket burst capacity. At least one of
        /// --refill-rate/--burst must be set.
        #[arg(long)]
        burst: Option<f64>,
        /// Seconds until the override self-expires and the declared
        /// baseline resumes. Must be greater than zero and not exceed the
        /// server-side cap (24h).
        #[arg(long)]
        ttl_secs: u64,
    },
    /// Clear a TTL'd runtime pacing override before its TTL elapses,
    /// immediately reverting to the declared baseline with no worker
    /// restart (issue #945).
    Clear {
        /// Name of a registered activity whose pacing override should be
        /// cleared.
        activity_name: String,
    },
}

#[derive(Debug, Subcommand)]
enum ThrottleCommand {
    /// Show the per-(`workflow_name`, `throttle_key`) deferred-start
    /// backlog across all shards, including any active TTL'd pacing
    /// override (issue #607, issue #945).
    Status,
    /// Set (or replace) a TTL'd runtime pacing override on top of a
    /// declared workflow-start throttle (issue #945, extends issue #607).
    ///
    /// Takes effect on the existing token-consumption path immediately,
    /// with no worker restart; self-expires and reverts to the declared
    /// baseline at the TTL with no further operator action. Each call
    /// fully replaces the whole override.
    Override {
        /// Name of a registered workflow that declares a static
        /// (non-dynamic) start throttle.
        workflow_name: String,
        /// Overridden token refill rate per second. At least one of
        /// --refill-per-sec/--burst must be set.
        #[arg(long)]
        refill_per_sec: Option<f64>,
        /// Overridden token bucket burst capacity. At least one of
        /// --refill-per-sec/--burst must be set.
        #[arg(long)]
        burst: Option<f64>,
        /// Seconds until the override self-expires and the declared
        /// baseline resumes. Must be greater than zero and not exceed the
        /// server-side cap (24h).
        #[arg(long)]
        ttl_secs: u64,
    },
    /// Clear a TTL'd runtime pacing override before its TTL elapses,
    /// immediately reverting to the declared baseline with no worker
    /// restart (issue #945).
    Clear {
        /// Name of a registered workflow whose pacing override should be
        /// cleared.
        workflow_name: String,
    },
}

#[derive(Debug, Subcommand)]
enum BatchCommand {
    /// List batch operations.
    List {
        /// Maximum number of rows to return.
        #[arg(long, value_parser = clap::value_parser!(i64).range(1..=200))]
        limit: Option<i64>,
    },
    /// Get details of a batch operation.
    Get {
        /// Batch operation ID.
        batch_job_id: String,
    },
    /// Submit a new batch operation.
    Submit {
        /// Action to perform: Cancel, Terminate, or Signal.
        #[arg(value_parser = clap::builder::PossibleValuesParser::new(["Cancel", "Terminate", "Signal"]))]
        action: String,
        /// Inline JSON filter definition.
        #[arg(long, conflicts_with = "filter_file")]
        filter_json: Option<String>,
        /// File containing JSON filter definition. Use `-` for stdin.
        #[arg(long, value_name = "PATH", conflicts_with = "filter_json")]
        filter_file: Option<PathBuf>,
        /// Name of the signal (required for action Signal unless --dry-run).
        ///
        /// Enforced manually in `batch_request` (not via `required_if_eq`) so a
        /// `Signal --dry-run` preview — which reports blast radius, not signal
        /// validity — can omit it (issue #769).
        #[arg(long)]
        signal_name: Option<String>,
        /// Inline JSON signal payload.
        #[arg(long, conflicts_with = "signal_payload_file")]
        signal_payload_json: Option<String>,
        /// File containing JSON signal payload. Use `-` for stdin.
        #[arg(long, value_name = "PATH", conflicts_with = "signal_payload_json")]
        signal_payload_file: Option<PathBuf>,
        /// Preview the blast radius (count + sample) without submitting a job.
        #[arg(long)]
        dry_run: bool,
        /// With --dry-run, print the raw JSON preview instead of a table.
        #[arg(long, requires = "dry_run")]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
enum DeadLetterCommand {
    /// List dead-lettered tasks.
    List {
        /// Maximum number of rows to return.
        #[arg(long, value_parser = clap::value_parser!(i64).range(1..=200))]
        limit: Option<i64>,
    },
    /// Replay a single dead-lettered task by ID.
    Replay {
        /// Dead-letter row ID.
        dead_letter_id: String,
    },
    /// Bulk-replay dead-lettered tasks matching a filter.
    ///
    /// At least one filter criterion must be provided. Use --dry-run to preview
    /// matching rows without performing any writes.
    BulkReplay {
        /// Exact match on activity name.
        #[arg(long)]
        activity_name: Option<String>,
        /// Exact match on workflow name.
        #[arg(long)]
        workflow_name: Option<String>,
        /// Exact match on task queue name (e.g. to reproduce a queue-scoped facet).
        #[arg(long)]
        queue_name: Option<String>,
        /// Only include entries with at least this many attempts.
        #[arg(long)]
        min_attempts: Option<i32>,
        /// Inclusive lower bound on `failed_at` (RFC 3339, e.g. `2026-04-27T12:30:00Z`).
        #[arg(long)]
        failed_after: Option<String>,
        /// Exclusive upper bound on `failed_at` (RFC 3339).
        #[arg(long)]
        failed_before: Option<String>,
        /// Filter by derived error class (exact, `PascalCase`; e.g. `CircuitOpen`,
        /// `PoisonPill`, `HandlerPanic`). Matching is exact-equality, not case-folded.
        #[arg(long)]
        error_class: Option<String>,
        /// Filter by derived DLQ reason class (exact, `snake_case`; e.g.
        /// `poison_pill`, `workflow_task_timeout`, `retry_exhaustion`). Exact-equality.
        #[arg(long)]
        dlq_reason: Option<String>,
        /// Filter by derived failure signature (exact match on the normalized
        /// first line of the error).
        #[arg(long)]
        failure_signature: Option<String>,
        /// Maximum rows to act on per call (default 100, max 1000).
        #[arg(long, value_parser = clap::value_parser!(u32).range(1..=1000))]
        limit: Option<u32>,
        /// Preview matching rows without performing any writes.
        #[arg(long)]
        dry_run: bool,
    },
    /// Aggregate dead-lettered tasks by dimension for fast root-cause triage.
    ///
    /// Groups the DLQ by one or more dimensions and reports per-group counts
    /// with representative sample IDs, merged across shards. Renders a table by
    /// default; pass --json for piping.
    #[command(alias = "summary")]
    Aggregate {
        /// Grouping dimensions (comma-separated or repeated). Supported:
        /// `workflow_name`, `activity_name`, `queue_name`, `task_type`,
        /// `time_bucket`, `failure_signature`, `dlq_reason`, `error_class`.
        /// Order builds a hierarchical key.
        #[arg(long = "group-by", value_delimiter = ',', required = true)]
        group_by: Vec<String>,
        /// Granularity for the `time_bucket` dimension: hour (default) or day.
        #[arg(long)]
        time_bucket: Option<String>,
        /// Filter by workflow name (applied before grouping).
        #[arg(long)]
        workflow_name: Option<String>,
        /// Filter by activity name.
        #[arg(long)]
        activity_name: Option<String>,
        /// Filter by queue name.
        #[arg(long)]
        queue_name: Option<String>,
        /// Inclusive lower bound on `failed_at`: RFC 3339 or relative (e.g. `24h`).
        #[arg(long)]
        since: Option<String>,
        /// Exclusive upper bound on `failed_at`: RFC 3339 or relative.
        #[arg(long)]
        until: Option<String>,
        /// Only include entries with at least this many attempts.
        #[arg(long)]
        min_attempts: Option<i32>,
        /// Cap on returned groups [1–500] (default 50). Long tail rolls into `_other`.
        #[arg(long, value_parser = clap::value_parser!(u32).range(1..=500))]
        limit_groups: Option<u32>,
        /// Representative sample IDs per group [0–10] (default 3).
        #[arg(long, value_parser = clap::value_parser!(u32).range(0..=10))]
        samples_per_group: Option<u32>,
        /// Print the raw JSON API payload instead of a table.
        #[arg(long)]
        json: bool,
    },
    /// Bulk-discard dead-lettered tasks matching a filter (delete without replay).
    ///
    /// At least one filter criterion must be provided. Use --dry-run to preview
    /// matching rows without performing any deletes.
    BulkDiscard {
        /// Exact match on activity name.
        #[arg(long)]
        activity_name: Option<String>,
        /// Exact match on workflow name.
        #[arg(long)]
        workflow_name: Option<String>,
        /// Exact match on task queue name (e.g. to reproduce a queue-scoped facet).
        #[arg(long)]
        queue_name: Option<String>,
        /// Only include entries with at least this many attempts.
        #[arg(long)]
        min_attempts: Option<i32>,
        /// Inclusive lower bound on `failed_at` (RFC 3339, e.g. `2026-04-27T12:30:00Z`).
        #[arg(long)]
        failed_after: Option<String>,
        /// Exclusive upper bound on `failed_at` (RFC 3339).
        #[arg(long)]
        failed_before: Option<String>,
        /// Filter by derived error class (exact, `PascalCase`; e.g. `CircuitOpen`,
        /// `PoisonPill`, `HandlerPanic`). Matching is exact-equality, not case-folded.
        #[arg(long)]
        error_class: Option<String>,
        /// Filter by derived DLQ reason class (exact, `snake_case`; e.g.
        /// `poison_pill`, `workflow_task_timeout`, `retry_exhaustion`). Exact-equality.
        #[arg(long)]
        dlq_reason: Option<String>,
        /// Filter by derived failure signature (exact match on the normalized
        /// first line of the error).
        #[arg(long)]
        failure_signature: Option<String>,
        /// Maximum rows to act on per call (default 100, max 1000).
        #[arg(long, value_parser = clap::value_parser!(u32).range(1..=1000))]
        limit: Option<u32>,
        /// Preview matching rows without performing any deletes.
        #[arg(long)]
        dry_run: bool,
    },
    /// Redrive (re-enqueue) dead-lettered tasks matching a filter after a fix.
    ///
    /// Re-enqueues matching entries with a fresh retry budget, reactivating any
    /// owning execution that was sealed FAILED so it resumes from existing
    /// history. Idempotent: redriving an already-redriven entry is a no-op
    /// reported as `skipped`. At least one filter criterion must be provided;
    /// use --dry-run to preview without writing.
    Redrive {
        /// Exact match on the original task queue name.
        #[arg(long)]
        queue: Option<String>,
        /// Exact match on the owning execution's workflow name.
        #[arg(long)]
        workflow_name: Option<String>,
        /// Inclusive lower bound on `failed_at` (RFC 3339, e.g. `2026-04-27T12:30:00Z`).
        #[arg(long)]
        dead_lettered_after: Option<String>,
        /// Exclusive upper bound on `failed_at` (RFC 3339).
        #[arg(long)]
        dead_lettered_before: Option<String>,
        /// Case-insensitive substring match on the dead-letter error text.
        #[arg(long)]
        error_contains: Option<String>,
        /// Explicit dead-letter IDs to redrive (comma-separated or repeated).
        #[arg(long = "dead-letter-id", value_delimiter = ',')]
        dead_letter_ids: Vec<String>,
        /// Maximum rows to redrive per call (default 100, max 1000).
        #[arg(long, value_parser = clap::value_parser!(u32).range(1..=1000))]
        max: Option<u32>,
        /// Optional operator reason recorded on the redrive event.
        #[arg(long)]
        reason: Option<String>,
        /// Preview matching rows without re-enqueuing.
        #[arg(long)]
        dry_run: bool,
    },
}

/// Subcommands for `harvest completion-delivery` (issue #605).
#[derive(Debug, Subcommand)]
enum CompletionDeliveryCommand {
    /// List completion-callback deliveries registered for a workflow execution.
    ///
    /// Includes PENDING, INFLIGHT, DELIVERED, and FAILED rows, ordered by
    /// `callback_index`. Filtering by `--state` is applied client-side.
    List {
        /// Workflow execution ID.
        execution_id: String,
        /// Filter to a single delivery state: pending | inflight | delivered | failed.
        #[arg(long)]
        state: Option<String>,
    },
    /// Manually redrive a FAILED completion-callback delivery after fixing the receiver.
    ///
    /// Idempotent-shaped: redriving a delivery that is not currently FAILED
    /// returns `ok=false` with `outcome="not_failed"` instead of erroring.
    Redrive {
        /// Workflow execution ID that owns the delivery.
        execution_id: String,
        /// Completion-delivery row ID, as returned by `list`.
        delivery_id: String,
    },
}

/// Subcommands for `harvest worker` (issue #170).
#[derive(Debug, Subcommand)]
enum WorkerCommand {
    /// Request a graceful drain for a specific worker.
    ///
    /// Sets the worker status to `Draining` so it stops accepting new tasks.
    /// The worker itself will complete in-flight tasks and then transition to
    /// `Stopped`. Use `--wait` to block until the worker reaches a terminal
    /// state before the deadline.
    Drain {
        /// Worker ID to drain.
        worker_id: String,
        /// Drain-by deadline (RFC 3339, e.g. `2026-05-09T12:00:00Z`).
        /// When omitted the server uses its configured shutdown timeout.
        #[arg(long)]
        deadline: Option<String>,
        /// Block until the worker reaches `Stopped` or the deadline elapses.
        /// Polls `GET /workers/{id}` every 2 s; exits 1 on timeout.
        #[arg(long)]
        wait: bool,
        /// Maximum seconds to wait when `--wait` is set (default: 120).
        #[arg(long, default_value = "120")]
        wait_timeout_secs: u64,
    },
    /// Preview which workers would be targeted by a drain, without draining them.
    #[command(name = "drain-preview")]
    DrainPreview {
        /// Filter by task queue name.
        #[arg(long)]
        queue: Option<String>,
        /// Filter by shard id.
        #[arg(long)]
        shard_id: Option<i32>,
        /// Filter by lifecycle status (`Active`, `Draining`, `Stopped`).
        #[arg(long)]
        status: Option<String>,
        /// Maximum number of workers to return [1–500].
        #[arg(long, value_parser = clap::value_parser!(i64).range(1..=500))]
        limit: Option<i64>,
    },
    /// List registered workers.
    List {
        /// Filter by task queue name.
        #[arg(long)]
        queue: Option<String>,
        /// Filter by shard id.
        #[arg(long)]
        shard_id: Option<i32>,
        /// Filter by lifecycle status (`Active`, `Draining`, `Stopped`).
        #[arg(long)]
        status: Option<String>,
        /// Filter by health (`healthy` or `stale`).
        #[arg(long)]
        health: Option<String>,
        /// Maximum number of workers to return [1–500].
        #[arg(long, value_parser = clap::value_parser!(i64).range(1..=500))]
        limit: Option<i64>,
    },
    /// Show details for a single worker.
    Get {
        /// Worker ID.
        worker_id: String,
    },
    /// Show aggregated fleet health statistics.
    Health,
}

/// Subcommands for `harvest events`.
#[derive(Debug, Subcommand)]
enum EventsCommand {
    /// Open the SSE stream for a workflow execution and print events to stdout.
    ///
    /// Each SSE event block is printed as `<event-type>: <json-data>`.
    /// The stream terminates when the execution reaches a terminal state
    /// (`event: stream-end`) or when the connection is closed.
    Tail {
        /// Workflow execution ID to watch.
        execution_id: String,
        /// Resume from this event row ID (Last-Event-ID header).
        /// Events with id > this value are replayed before entering live-tail mode.
        #[arg(long)]
        last_event_id: Option<i64>,
    },
}

impl Cli {
    /// Build the management API request represented by these CLI arguments.
    ///
    /// # Errors
    ///
    /// Returns an error when inline JSON cannot be parsed or JSON file/stdin
    /// input cannot be read.
    pub fn api_request(&self) -> Result<ApiRequest, CliError> {
        match &self.command {
            Commands::Health => Ok(ApiRequest::get("/health")),
            Commands::Preflight => Ok(ApiRequest::get("/admin/preflight")),
            Commands::Shard { command } => Ok(shard_request(command)),
            Commands::Workflow { command } => workflow_request(command),
            Commands::History { command } => Ok(history_request(command)),
            Commands::LegalHold { command } => Ok(legal_hold_request(command)),
            Commands::Handoff { command } => handoff_request(command),
            Commands::Dag { command } => dag_request(command, self.actor.as_deref()),
            Commands::Schedule { command } => schedule_request(command),
            Commands::Dlq { command } => Ok(dead_letter_request(command)),
            Commands::CompletionDelivery { command } => Ok(completion_delivery_request(command)),
            Commands::Retention { command } => Ok(retention_request(command)),
            Commands::Queue { command } => queue_request(command),
            Commands::Activity { command } => activity_request(command),
            Commands::Concurrency { command } => Ok(concurrency_request(command)),
            Commands::RateLimit { command } => Ok(rate_limit_request(command)),
            Commands::Throttle { command } => Ok(throttle_request(command)),
            Commands::Batch { command } => batch_request(command),
            Commands::Audit { command } => Ok(audit_request(command)),
            Commands::Gate { command } => gate_request(command),
            Commands::Token { command } => Ok(token_request(command)),
            Commands::Worker { command } => Ok(worker_request(command)),
            Commands::Usage {
                from,
                to,
                group_by,
                json: _,
            } => Ok(usage_request(from, to, group_by.as_deref())),
            Commands::VersionUsage {
                workflow_name,
                change_id,
                recorded_version,
                state_group,
                shard_id,
                guard,
            } => Ok(version_usage_request(
                workflow_name.as_deref(),
                change_id.as_deref(),
                *recorded_version,
                *state_group,
                *shard_id,
                *guard,
            )),
            Commands::VersionGateRetirement {
                change_id,
                min_safe_version,
                workflow_name,
                state_group,
                shard_id,
                check: _,
            } => Ok(retirement_check_request(
                change_id,
                *min_safe_version,
                workflow_name.as_deref(),
                *state_group,
                *shard_id,
            )),
            Commands::WorkflowTypes { command } => Ok(workflow_reachability_request(command)),
            Commands::Tui => unreachable!("Tui command handles its own requests"),
            Commands::Events { .. } => unreachable!("Events command handles its own requests"),
            Commands::StartBatch {
                file,
                items_json,
                atomic,
            } => start_batch_request(file.as_deref(), items_json.as_deref(), *atomic),
            Commands::Canary {
                sample_size,
                workflow_name,
                queue,
                json: _,
            } => Ok(canary_request(
                *sample_size,
                workflow_name.as_deref(),
                queue.as_deref(),
            )),
            Commands::Build { command } => Ok(build_routing_request(command)),
            // Locally-executed commands: each returns from `run` before the
            // API-request mapper is reached, so none of them can arrive here.
            // Grouped rather than listed one arm apiece -- a three-line arm per
            // command is what pushed this function past `too_many_lines`, and
            // it would do so again on the next local command.
            cmd @ (Commands::Backup { .. }
            | Commands::Dr { .. }
            | Commands::Partition { .. }
            | Commands::DetCheck { .. }
            | Commands::Debug { .. }
            | Commands::Schema { .. }
            | Commands::Migrate { .. }
            | Commands::New { .. }) => {
                unreachable!("{cmd:?} handles its own execution locally")
            }
        }
    }
}

fn build_routing_request(command: &BuildRoutingCommand) -> ApiRequest {
    match command {
        BuildRoutingCommand::Ramp { command } => match command {
            RampCommand::Set {
                queue,
                target_build_id,
                percent,
            } => ApiRequest::post(
                "/admin/build-routing/ramp",
                Some(json!({
                    "queue_name": queue,
                    "target_build_id": target_build_id,
                    "ramp_percent": percent,
                })),
            ),
            RampCommand::Show => ApiRequest::get("/admin/build-routing"),
            RampCommand::Clear { queue } => ApiRequest {
                method: ApiMethod::Delete,
                path: format!("/admin/build-routing/ramp/{}", path_segment(queue)),
                body: None,
            },
        },
    }
}

impl ApiRequest {
    fn get(path: impl Into<String>) -> Self {
        Self {
            method: ApiMethod::Get,
            path: path.into(),
            body: None,
        }
    }

    fn patch(path: impl Into<String>, body: Value) -> Self {
        Self {
            method: ApiMethod::Patch,
            path: path.into(),
            body: Some(body),
        }
    }

    fn post(path: impl Into<String>, body: Option<Value>) -> Self {
        Self {
            method: ApiMethod::Post,
            path: path.into(),
            body,
        }
    }
}

pub mod debug;
pub mod debug_tui;
pub mod tui;

/// Run the CLI, print successful response data to stdout, and return errors.
///
/// # Errors
///
/// Returns an error if request construction, HTTP transport, response parsing,
/// or response formatting fails.
// A dispatcher: a sequence of `if let ... return` guards for locally-handled
// commands (det-check, schema, new, tui, debug, events, worker-drain-wait,
// backup verify, token bootstrap) followed
// by the shared execute/render path. Splitting it would only scatter the guards.
#[allow(clippy::too_many_lines)]
pub async fn run_cli(cli: Cli) -> Result<(), CliError> {
    // `backup verify` talks to scratch databases directly (read-only), not to
    // the management API: no HTTP, handled in-process (mirrors DetCheck).
    if let Commands::Backup {
        command:
            BackupCommand::Verify {
                shards,
                live_dsn,
                i_know_this_is_scratch,
                format,
                replay_sample,
                worker_stale_secs,
                probe_limit,
                default_shard,
            },
    } = &cli.command
    {
        return run_backup_verify(
            shards,
            live_dsn,
            *i_know_this_is_scratch,
            *format,
            *replay_sample,
            *worker_stale_secs,
            *probe_limit,
            *default_shard,
        )
        .await;
    }

    // `dr` talks to shard databases directly, not to the management API: a
    // regional failover is precisely when the management API may be
    // unreachable (mirrors `backup verify`).
    if let Commands::Dr { command } = &cli.command {
        return run_dr(command).await;
    }

    // `shard rebalance` moves rows between two shard databases, so like `dr` and
    // `partition` it connects to them directly rather than through the
    // management API -- which would need a pool for both shards on one node.
    if let Commands::Shard { command } = &cli.command
        && matches!(
            command,
            ShardCommand::Rebalance { .. } | ShardCommand::RebalanceResume { .. }
        )
    {
        return run_shard_rebalance(command, cli.actor.as_deref()).await;
    }

    // `partition` is shard-local DDL and inspection: it connects to each shard
    // database directly, so it is handled in-process before the API execute
    // path, exactly like `dr`.
    if let Commands::Partition { command } = &cli.command {
        return run_partition(command).await;
    }

    // det-check is read-only local source analysis: no HTTP, handled entirely
    // in-process before the API execute path (mirrors the Tui early-return).
    if let Commands::DetCheck {
        paths,
        format,
        deny_warnings,
        list_suppressions,
    } = &cli.command
    {
        return run_det_check(paths, *format, *deny_warnings, *list_suppressions);
    }

    // `debug` is a read-only local history analysis: no HTTP, no DB, no
    // activity execution (mirrors DetCheck).
    if let Commands::Debug { command } = &cli.command {
        return match command {
            DebugCommand::Replay {
                history,
                format,
                step,
                break_at_event_type,
                break_at_index,
                break_at_activity,
                break_at_signal,
                max_steps,
                tui,
            } => crate::debug::run_replay(
                history,
                *format,
                *step,
                break_at_event_type.as_deref(),
                *break_at_index,
                break_at_activity.as_deref(),
                break_at_signal.as_deref(),
                *max_steps,
                *tui,
            ),
            DebugCommand::Diff {
                left,
                right,
                format,
            } => crate::debug::run_diff(left, right, *format),
        };
    }

    // `schema` is read-only local file comparison: no HTTP, no DB (mirrors
    // DetCheck).
    if let Commands::Schema { command } = &cli.command {
        return match command {
            SchemaCommand::Check {
                baseline,
                current,
                format,
                require_current,
                acknowledged_in,
            } => run_schema_check(
                baseline,
                current,
                *format,
                *require_current,
                acknowledged_in.as_deref(),
            ),
            SchemaCommand::Update {
                baseline,
                current,
                acknowledge,
                recorded_in,
            } => run_schema_update(
                baseline,
                current,
                acknowledge.as_deref(),
                recorded_in.as_deref(),
            ),
        };
    }

    // `migrate` talks to the Harvest database directly (issue #1240): no HTTP,
    // and deliberately no running app — it is the step that happens *before*
    // replicas roll (mirrors `backup verify`).
    if let Commands::Migrate { command } = &cli.command {
        return match command {
            MigrateCommand::Status {
                database_url,
                include_dir,
                format,
                check,
            } => run_migrate_status(database_url, include_dir, *format, *check).await,
            MigrateCommand::Run {
                database_url,
                include_dir,
                format,
                dry_run,
            } => run_migrate_run(database_url, include_dir, *format, *dry_run).await,
        };
    }

    // `new` is pure local file generation: no HTTP, no DB (mirrors DetCheck).
    if let Commands::New {
        name,
        path,
        force,
        template,
    } = &cli.command
    {
        return run_new(name, path.as_deref(), *force, *template);
    }

    // Token bootstrap is an OFFLINE seed: print the secret + INSERT SQL, open no
    // DB connection, issue no HTTP request (mirrors the DetCheck early-return).
    if let Commands::Token {
        command:
            TokenCommand::Bootstrap {
                name,
                scope,
                expires_at,
                created_by,
            },
    } = &cli.command
    {
        return run_token_bootstrap(name, scope, expires_at.as_deref(), created_by);
    }

    if matches!(cli.command, Commands::Tui) {
        return tui::run_tui(&cli).await;
    }

    // SSE streaming: bypasses JSON execute path.
    if let Commands::Events {
        command:
            EventsCommand::Tail {
                execution_id,
                last_event_id,
            },
    } = &cli.command
    {
        return run_events_tail(&cli, execution_id, *last_event_id).await;
    }

    // --wait mode: issue drain then poll until Stopped or timeout.
    if let Commands::Worker {
        command:
            WorkerCommand::Drain {
                worker_id,
                wait: true,
                wait_timeout_secs,
                ..
            },
    } = &cli.command
    {
        return run_worker_drain_wait(&cli, worker_id, *wait_timeout_secs).await;
    }

    let response = match execute(&cli).await {
        Ok(v) => v,
        Err(err) => {
            // Fail closed: a transport/API error on a reachability gate command must
            // exit 2 (deploy hazard) rather than exit 1 (generic error). Exit 1 is
            // labelled "transport/usage error" in the runbook and operators may
            // retry or ignore it; exit 2 unambiguously signals an unsafe answer.
            if workflow_reachability_should_gate(&cli) {
                return Err(CliError::WorkflowReachabilityGate {
                    context: err.to_string(),
                });
            }
            // Issue #774: same fail-closed convention as the reachability gate
            // above -- a transport error on a queue-coverage deploy gate must
            // also read as "deploy hazard" (exit 2), not a generic error (exit 1).
            if queue_coverage_should_gate(&cli) {
                return Err(CliError::QueueCoverageGate {
                    context: err.to_string(),
                });
            }
            return Err(err);
        }
    };
    // Issue #756: a degraded cross-shard read carries its partial-availability
    // warning on STDERR, keeping STDOUT a clean/parseable body (`-o json | jq`)
    // on both the happy and degraded paths. The operator still sees the warning.
    if let Some(notice) = fanout_partial_notice(&response) {
        eprintln!("{notice}");
    }
    // `render_response` is deliberately called inside each arm rather than once
    // up front: for a bundle export it produces a full pretty-printed copy of
    // every sampled payload, which the default (non-JSON) arm then discards in
    // favour of `summary`. `write_history_sample_bundle` is already serializing
    // every fixture, so holding a second whole-response copy alongside it is
    // what pushes a large export toward an OOM on a CI runner.
    if let Some(dir) = history_sample_output_dir(&cli) {
        // Issue #798: the sample export writes a replay BUNDLE (one fixture per
        // sampled execution plus the coverage manifest), not a single blob, so
        // the directory can be fed straight to `WorkflowReplayer::replay_bundle`.
        // `-o json` still prints the raw response for scripting.
        let summary = write_history_sample_bundle(&response, dir)?;
        if cli.output == OutputFormat::Json {
            println!("{}", render_response(&cli, &response)?);
        } else {
            print!("{summary}");
        }
    } else if let Some(path) = history_output_file(&cli) {
        let rendered = render_response(&cli, &response)?;
        fs::write(path, &rendered).map_err(|source| CliError::WriteOutput {
            path: path.display().to_string(),
            source,
        })?;
    } else {
        println!("{}", render_response(&cli, &response)?);
    }
    if matches!(cli.command, Commands::Preflight) {
        let exit_code = preflight_exit_code(&response);
        if exit_code != 0 {
            let status = response
                .get("overall_status")
                .and_then(Value::as_str)
                .unwrap_or("fail")
                .to_string();
            return Err(CliError::PreflightGate { status });
        }
    }
    if shard_health_should_gate(&cli) && shard_health_exit_code(&response) != 0 {
        return Err(CliError::ShardHealthGate);
    }
    if version_usage_should_guard(&cli) && version_usage_guard_exit_code(&response) != 0 {
        return Err(CliError::VersionUsageGate);
    }
    if retirement_check_should_check(&cli) && retirement_check_exit_code(&response) != 0 {
        return Err(CliError::RetirementCheckGate);
    }
    if workflow_reachability_should_gate(&cli) && workflow_reachability_exit_code(&response) != 0 {
        return Err(CliError::WorkflowReachabilityGate {
            context: "orphaned verdict, in_use with type filter, or incomplete shard report"
                .to_string(),
        });
    }
    if queue_coverage_should_gate(&cli) && queue_coverage_exit_code(&response) != 0 {
        return Err(CliError::QueueCoverageGate {
            context: "uncovered queue or incomplete shard report".to_string(),
        });
    }
    if queue_mutation_should_gate(&cli) && queue_mutation_exit_code(&response) != 0 {
        let detail = response
            .get("partial_failures")
            .and_then(Value::as_str)
            .unwrap_or("see response body")
            .to_string();
        return Err(CliError::QueuePartialMutation { detail });
    }
    // Same 207-is-2xx hazard as the queue mutation above, and the response
    // carries the identical `ok`/`status` contract, so the same body-level gate
    // applies: a hold that missed a shard must never look like success.
    if activity_mutation_should_gate(&cli) && queue_mutation_exit_code(&response) != 0 {
        let detail = response
            .get("partial_failures")
            .and_then(Value::as_str)
            .unwrap_or("see response body")
            .to_string();
        return Err(CliError::ActivityPartialMutation { detail });
    }
    if canary_should_gate(&cli) && canary_exit_code(&response) != 0 {
        let verdict = response
            .get("verdict")
            .and_then(Value::as_str)
            .unwrap_or("fail")
            .to_string();
        return Err(CliError::CanaryGate { verdict });
    }
    Ok(())
}

fn history_output_file(cli: &Cli) -> Option<&Path> {
    match &cli.command {
        Commands::History {
            command:
                HistoryCommand::Export { output_file, .. }
                | HistoryCommand::ExportBatch { output_file, .. },
        } => output_file.as_deref(),
        _ => None,
    }
}

// ── replay-drift sample bundle (issue #798) ─────────────────────────────────

fn history_sample_output_dir(cli: &Cli) -> Option<&Path> {
    match &cli.command {
        Commands::History {
            command: HistoryCommand::ExportSample { output_dir, .. },
        } => Some(output_dir.as_path()),
        _ => None,
    }
}

/// One file to write into the replay bundle.
#[derive(Debug, PartialEq, Eq)]
pub struct BundleFile {
    /// File name, relative to the bundle directory.
    pub name: String,
    /// Serialized JSON contents.
    pub contents: String,
}

/// Split a sample-export response into the files that make up a replay bundle.
///
/// Pure: no filesystem access, so the naming, manifest placement, and
/// collision handling are unit-testable without a temp directory.
///
/// The manifest is written under the reserved
/// [`SampleManifest::FILE_NAME`](autumn_harvest::replay_sample::SampleManifest::FILE_NAME)
/// so `replay_bundle` reads it as coverage instead of trying to replay it as a
/// fixture.
///
/// # Errors
/// Returns [`CliError::InvalidInput`] when the response is not a sample-export
/// body (missing `exports` array or `manifest` object) — better than writing a
/// silently-empty bundle that a CI gate would then pass or fail for the wrong
/// reason.
pub fn history_sample_bundle_files(response: &Value) -> Result<Vec<BundleFile>, CliError> {
    let exports = response
        .get("exports")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            CliError::InvalidInput(
                "sample export response is missing an `exports` array".to_string(),
            )
        })?;
    let manifest = response.get("manifest").filter(|value| value.is_object());
    let Some(manifest) = manifest else {
        return Err(CliError::InvalidInput(
            "sample export response is missing a `manifest` object".to_string(),
        ));
    };

    let mut files = Vec::with_capacity(exports.len() + 1);
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for (index, export) in exports.iter().enumerate() {
        let workflow_name = export
            .get("workflow_name")
            .and_then(Value::as_str)
            .unwrap_or("workflow");
        let execution_id = export
            .get("execution_id")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let mut name =
            autumn_harvest::replay_sample::fixture_file_name(workflow_name, execution_id);
        // Sanitization can, in principle, map two distinct names onto one file.
        // Overwriting would silently shrink the bundle and make the gate verify
        // fewer fixtures than the manifest claims, so disambiguate instead.
        if !seen.insert(name.clone()) {
            name = format!("{index}-{name}");
            seen.insert(name.clone());
        }
        files.push(BundleFile {
            name,
            contents: serde_json::to_string_pretty(export).map_err(|error| {
                CliError::InvalidInput(format!("failed to serialize sample fixture: {error}"))
            })?,
        });
    }

    files.push(BundleFile {
        name: autumn_harvest::replay_sample::SampleManifest::FILE_NAME.to_string(),
        contents: serde_json::to_string_pretty(manifest).map_err(|error| {
            CliError::InvalidInput(format!("failed to serialize sample manifest: {error}"))
        })?,
    });
    Ok(files)
}

/// Render the operator-facing coverage table for a sample export.
///
/// Pure and separate from the write so the "how much of the fleet did this
/// verify?" answer (AC2) is testable without touching disk.
#[must_use]
pub fn render_history_sample_summary(response: &Value, dir: &Path) -> String {
    let manifest = response.get("manifest");
    let status = manifest
        .and_then(|manifest| manifest.get("status"))
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let sampled_total = manifest
        .and_then(|manifest| manifest.get("sampled_total"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let in_flight_total = manifest
        .and_then(|manifest| manifest.get("in_flight_total"))
        .and_then(Value::as_u64)
        .unwrap_or(0);

    let mut out = String::new();
    let _ = writeln!(
        out,
        "wrote {sampled_total} fixture(s) to {} (coverage: {status})",
        dir.display()
    );
    let _ = writeln!(
        out,
        "sampled {sampled_total} of {in_flight_total} in-flight execution(s)"
    );

    if let Some(rows) = manifest
        .and_then(|manifest| manifest.get("per_workflow"))
        .and_then(Value::as_array)
        && !rows.is_empty()
    {
        out.push_str("\nWORKFLOW                                 SAMPLED  IN FLIGHT  TRUNCATED\n");
        for row in rows {
            let name = row
                .get("workflow_name")
                .and_then(Value::as_str)
                .unwrap_or("-");
            let sampled = row.get("sampled").and_then(Value::as_u64).unwrap_or(0);
            let total = row
                .get("in_flight_total")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let truncated = if sampled < total { "yes" } else { "no" };
            let _ = writeln!(out, "{name:<40} {sampled:>7}  {total:>9}  {truncated}");
        }
    }

    // A partial sample is a lower bound: an operator must not read a green gate
    // over it as "the whole fleet replays clean".
    if status != "complete" {
        out.push_str("\nWARNING: shard coverage is ");
        out.push_str(status);
        out.push_str("; this bundle is a LOWER BOUND on the in-flight fleet.\n");
        if let Some(unavailable) = manifest
            .and_then(|manifest| manifest.get("unavailable_shards"))
            .and_then(Value::as_array)
        {
            for shard in unavailable {
                if let Some(reason) = shard.as_str() {
                    let _ = writeln!(out, "  unavailable: {reason}");
                }
            }
        }
    }
    if sampled_total < in_flight_total {
        out.push_str(
            "NOTE: the sample is truncated; a clean gate verifies the SAMPLE, not the fleet.\n",
        );
    }
    // Distinct from the NOTE above, and the distinction is what an operator acts
    // on: that one reports the *intended* truncation of sampling `per_workflow`
    // out of a larger population, this one an *unplanned* resource limit. Raising
    // `--per-workflow` would make this strictly worse.
    if manifest
        .and_then(|manifest| manifest.get("truncated_by_size"))
        .and_then(Value::as_bool)
        == Some(true)
    {
        out.push_str(
            "\nWARNING: the export stopped early on the response byte budget, so this bundle \
             is SMALLER than the sample you asked for. Narrow the export (fewer states, a \
             single --shard-id, a lower --max-bytes) rather than raising --per-workflow.\n",
        );
    }

    // A candidate the sample SELECTED but the export could not fetch. Reported
    // separately from the byte-budget cut above because the fix differs: this
    // one is per-document (`--max-bytes`), that one is request-wide. Both make
    // the bundle a subset of the chosen slice, and both make the gate exit 2.
    let dropped = manifest
        .and_then(|manifest| manifest.get("export_failures"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    if dropped > 0 {
        let _ = write!(
            out,
            "\nWARNING: the export could not fetch {dropped} candidate(s) the sample \
             selected (over --max-bytes, or an unreadable shard), so this bundle is a \
             BIASED subset — biased against the largest histories, which are the \
             longest-running and the most likely to span your change. The replay gate \
             will refuse it (exit 2). Raise --max-bytes, or narrow the sample until \
             every selected candidate fits.\n",
        );
    }

    // A redacted bundle is refused by the replay gate (it rewrites the very
    // activity inputs replay compares against, so every fixture would report a
    // false divergence). The CLI default is `redacted`, so an operator who omits
    // the flag writes an unusable bundle. Say so here, at write time, rather than
    // letting them discover it as an exit-2 wall of BLOCKED lines later.
    if bundle_is_redacted(response) {
        out.push_str(
            "\nWARNING: this bundle was exported with --payload-policy redacted, which the \
             replay-drift gate REFUSES (redaction rewrites the activity inputs replay compares \
             against). Re-export with `--payload-policy full`, and treat the result as \
             production data.\n",
        );
    }
    out
}

/// Whether `response` declares a redacted payload policy.
///
/// Reads the response's own top-level `payload_policy`, which the sample-export
/// route echoes back from the request. That is authoritative and present even
/// when `exports` is empty, so it beats sniffing the individual fixtures.
///
/// Absent or unrecognized reads as *not* redacted: a legacy response, or a
/// future policy value, must never produce a spurious warning about a bundle the
/// gate would happily replay.
fn bundle_is_redacted(response: &Value) -> bool {
    response.get("payload_policy").and_then(Value::as_str) == Some("redacted")
}

/// Write a replay bundle to `dir`, returning the operator summary.
///
/// # Errors
/// Returns [`CliError::WriteOutput`] when the directory or a fixture cannot be
/// written, or [`CliError::InvalidInput`] when the response is not a
/// sample-export body.
fn write_history_sample_bundle(response: &Value, dir: &Path) -> Result<String, CliError> {
    let files = history_sample_bundle_files(response)?;
    fs::create_dir_all(dir).map_err(|source| CliError::WriteOutput {
        path: dir.display().to_string(),
        source,
    })?;
    clear_stale_bundle(dir)?;
    for file in &files {
        let path = dir.join(&file.name);
        fs::write(&path, &file.contents).map_err(|source| CliError::WriteOutput {
            path: path.display().to_string(),
            source,
        })?;
    }
    Ok(render_history_sample_summary(response, dir))
}

/// Remove a previous bundle's fixtures from `dir` so a re-export replaces it.
///
/// A fixture's file name embeds its execution id, so re-exporting does not
/// overwrite the previous run's fixtures — it accumulates on top of them. The
/// manifest has a fixed name and *is* overwritten, so the two end up describing
/// different things: the manifest counts this run's sample while the directory
/// holds every run's, and `replay_bundle` walks every `*.json` it finds. The
/// gate would then verify executions that finished hours ago and report coverage
/// numbers that never described them.
///
/// Scoped narrowly, because this deletes files in a path the caller chose:
/// * A top-level manifest **that is a regular file** must be present — that is
///   the marker identifying a directory this CLI wrote. A directory holding
///   `*.json` with no such manifest is somebody else's and is **refused**, not
///   cleaned; `--output-dir .` must never eat a user's files because they
///   mistyped a path.
/// * Every `*.json` entry the gate would replay is removed — at **any depth**,
///   and whether it is a regular file or a **symlink** (the link is removed,
///   never the file it points at). The predicate deliberately mirrors
///   `testing::collect_json_files`: if the two walks disagree about what counts
///   as a fixture, a leftover entry is replayed against the fresh manifest and
///   reported as drift the candidate never caused.
/// * Directories and every non-`.json` file are left alone.
///
/// # Errors
/// [`CliError::InvalidInput`] when `dir` holds JSON that is not a Harvest
/// bundle; [`CliError::WriteOutput`] when an entry cannot be read or removed.
fn clear_stale_bundle(dir: &Path) -> Result<(), CliError> {
    let manifest_name = autumn_harvest::replay_sample::SampleManifest::FILE_NAME;
    let mut json_entries = Vec::new();
    let mut has_manifest = false;

    // Walk exactly what the gate replays. `testing::collect_json_files`
    // DESCENDS SUBDIRECTORIES, so a top-level-only clean would leave a nested
    // fixture in place while the gate still replayed it — the new manifest
    // would describe only the fresh top-level fixtures, and the stale nested
    // one would surface as unrelated drift. That is precisely the false verdict
    // this function exists to prevent, so the two walks must agree on depth as
    // well as on the extension predicate.
    let mut dirs_to_visit = vec![dir.to_path_buf()];
    while let Some(current) = dirs_to_visit.pop() {
        let at_top_level = current == dir;
        let entries = fs::read_dir(&current).map_err(|source| CliError::WriteOutput {
            path: current.display().to_string(),
            source,
        })?;
        for entry in entries {
            let entry = entry.map_err(|source| CliError::WriteOutput {
                path: current.display().to_string(),
                source,
            })?;
            let path = entry.path();
            // `file_type` does not follow symlinks, so a symlinked directory is
            // neither descended nor mistaken for a file.
            let Ok(kind) = entry.file_type() else {
                continue;
            };
            if kind.is_dir() {
                dirs_to_visit.push(path);
                continue;
            }
            // Deliberately NOT `is_file()`. `testing::collect_json_files`
            // decides what the GATE replays with the predicate "not a directory
            // and ends in `.json`" — which a SYMLINK satisfies. Requiring a
            // regular file here would leave a linked fixture behind for the
            // replay walk to pick up, and the disagreement is a false verdict:
            // the fresh manifest describes this run's sample while the
            // directory still holds a previous run's, and the stale fixture
            // surfaces as unrelated drift on an unchanged candidate.
            //
            // Containment depends on it too. `fs::write` FOLLOWS a symlink, so
            // a leftover link whose name collides with a fixture about to be
            // written sends that write outside the directory the operator
            // named. Clearing the link first is what keeps the replace inside
            // `--output-dir`; `fs::remove_file` removes the LINK, never the file
            // it points at.
            let is_manifest = entry.file_name().to_string_lossy() == manifest_name;
            if is_manifest && kind.is_file() {
                // Only a TOP-LEVEL manifest marks this directory as our bundle;
                // a nested one is just a file we happen to skip (the replay walk
                // excludes the reserved name at any depth, so it is never
                // replayed and never needs deleting).
                //
                // The MARKER is held to a stricter rule than the fixtures it
                // guards — a regular file, not merely a `.json` entry — because
                // it is what AUTHORIZES deleting this directory's contents. A
                // *linked* manifest is not evidence we wrote the directory, so
                // it falls through to the `.json` branch below and blocks the
                // replace instead of licensing it. That also keeps the manifest
                // write itself contained, since it is a name we are about to
                // `fs::write`.
                if at_top_level {
                    has_manifest = true;
                }
                continue;
            }
            if path.extension().and_then(std::ffi::OsStr::to_str) == Some("json") {
                json_entries.push(path);
            }
        }
    }

    if json_entries.is_empty() {
        return Ok(());
    }
    if !has_manifest {
        return Err(CliError::InvalidInput(format!(
            "{} already contains {} .json file(s) but no {manifest_name}, so it is \
             not a bundle written by this command. Refusing to overwrite it — \
             point --output-dir at an empty or previously-exported directory.",
            dir.display(),
            json_entries.len(),
        )));
    }

    for path in json_entries {
        fs::remove_file(&path).map_err(|source| CliError::WriteOutput {
            path: path.display().to_string(),
            source,
        })?;
    }
    Ok(())
}

// ── det-check (issue #778) ──────────────────────────────────────────────────

/// Builds one determinism report across every requested source path.
///
/// A single shared first-party helper index (issue #778) is used, so a
/// cross-file transitive violation is caught even when the two files are passed
/// as separate arguments (the changed-files CI pattern). Directories are walked
/// recursively; files are scanned directly; symlinks are not followed;
/// overlapping arguments are de-duplicated; a non-UTF-8 file mid-walk is
/// skipped. Pure — no printing.
///
/// # Errors
/// Returns [`CliError::InvalidInput`] if a top-level path is missing or a source
/// path cannot be read.
pub fn det_check_report_for_paths(paths: &[PathBuf]) -> Result<DetCheckReport, CliError> {
    let refs: Vec<&Path> = paths.iter().map(PathBuf::as_path).collect();
    check_paths(&refs).map_err(|source| {
        CliError::InvalidInput(format!("det-check: failed to read source: {source}"))
    })
}

/// Formats one finding as `file:line:col DETxxx  (safe alternative: …)`, with a
/// trailing `[in helper `H` reached from workflow `W`]` for a transitive finding.
fn format_det_finding_line(finding: &autumn_harvest::DetFinding) -> String {
    let loc = finding.location.as_ref();
    let file = loc.map_or("<unknown>", |l| l.file.as_str());
    let line = loc.map_or(0, |l| l.line);
    let col = loc.map_or(1, |l| l.col);
    let mut out = format!(
        "{file}:{line}:{col} {}  (safe alternative: {})",
        finding.rule_id, finding.alternative
    );
    if let Some(helper) = &finding.via_helper {
        let wf = finding.workflow_name.as_deref().unwrap_or("<unknown>");
        let _ = write!(out, "  [in helper `{helper}` reached from workflow `{wf}`]");
    }
    out
}

/// Renders the findings section of a report as text, one line per finding,
/// sorted by `(file, line, col, rule_id)`.
#[must_use]
pub fn format_det_findings_text(report: &DetCheckReport) -> String {
    if report.findings.is_empty() {
        return "det-check: no findings".to_string();
    }
    let mut findings: Vec<&autumn_harvest::DetFinding> = report.findings.iter().collect();
    findings.sort_by(|a, b| det_finding_sort_key(a).cmp(&det_finding_sort_key(b)));
    let mut lines: Vec<String> = findings
        .iter()
        .map(|f| format_det_finding_line(f))
        .collect();
    let (errors, warnings) = det_check_counts(report);
    lines.push(format!(
        "det-check: {errors} hard-blocker finding(s), {warnings} warning(s)"
    ));
    lines.join("\n")
}

fn det_finding_sort_key(f: &autumn_harvest::DetFinding) -> (String, u32, u32, &'static str) {
    let loc = f.location.as_ref();
    (
        loc.map_or(String::new(), |l| l.file.clone()),
        loc.map_or(0, |l| l.line),
        loc.map_or(0, |l| l.col),
        f.rule_id,
    )
}

/// Renders the always-echoed suppression audit footer for text mode.
#[must_use]
pub fn format_det_suppressions(report: &DetCheckReport) -> String {
    if report.suppressions.is_empty() {
        return "suppressed: none".to_string();
    }
    det_sorted_suppressions(report)
        .iter()
        .map(|s| {
            format!(
                "suppressed: {}:{} {} \"{}\"",
                s.location.file, s.location.line, s.rule_id, s.reason
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Renders the `--list-suppressions` audit listing (AC6): `file:line RULEID "reason"`.
#[must_use]
pub fn format_det_suppressions_list(report: &DetCheckReport) -> String {
    if report.suppressions.is_empty() {
        return "no active suppressions".to_string();
    }
    det_sorted_suppressions(report)
        .iter()
        .map(|s| {
            format!(
                "{}:{} {} \"{}\"",
                s.location.file, s.location.line, s.rule_id, s.reason
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn det_sorted_suppressions(report: &DetCheckReport) -> Vec<&autumn_harvest::DetSuppression> {
    let mut sups: Vec<&autumn_harvest::DetSuppression> = report.suppressions.iter().collect();
    sups.sort_by(|a, b| {
        (
            a.location.file.as_str(),
            a.location.line,
            a.rule_id.as_str(),
        )
            .cmp(&(
                b.location.file.as_str(),
                b.location.line,
                b.rule_id.as_str(),
            ))
    });
    sups
}

/// Serializes the report as pretty JSON (AC2).
///
/// # Errors
/// Returns [`CliError::SerializeResponse`] if serialization fails.
pub fn det_check_json(report: &DetCheckReport) -> Result<String, CliError> {
    serde_json::to_string_pretty(report).map_err(CliError::SerializeResponse)
}

/// Serializes the report's active suppressions as pretty JSON for
/// `--list-suppressions --format json` (the audit inventory as machine-readable
/// output rather than the text listing).
///
/// # Errors
/// Returns [`CliError::SerializeResponse`] if serialization fails.
pub fn det_suppressions_json(report: &DetCheckReport) -> Result<String, CliError> {
    serde_json::to_string_pretty(&json!({ "suppressions": report.suppressions }))
        .map_err(CliError::SerializeResponse)
}

/// `(errors, warnings)` counts across a report's findings.
fn det_check_counts(report: &DetCheckReport) -> (usize, usize) {
    let errors = report
        .findings
        .iter()
        .filter(|f| matches!(f.severity, DetSeverity::Error))
        .count();
    let warnings = report.findings.len() - errors;
    (errors, warnings)
}

/// Decides whether `det-check` should gate (exit non-zero).
///
/// Gates whenever a hard-blocker finding is present, or `deny_warnings` is set
/// and any warning finding is present. Returns the error to surface, or `None`
/// to pass.
#[must_use]
pub fn det_check_gate(report: &DetCheckReport, deny_warnings: bool) -> Option<CliError> {
    let (errors, warnings) = det_check_counts(report);
    if report.has_hard_blockers() || (deny_warnings && warnings > 0) {
        Some(CliError::DetCheckFindings { errors, warnings })
    } else {
        None
    }
}

/// Runs `det-check`: merges reports for `paths`, prints findings (text or JSON)
/// or the suppression listing, and gates the exit code.
///
/// # Errors
/// Returns [`CliError::DetCheckFindings`] when the gate trips (findings are
/// already on stdout), or a read/serialize error.
pub fn run_det_check(
    paths: &[PathBuf],
    format: DetCheckFormat,
    deny_warnings: bool,
    list_suppressions: bool,
) -> Result<(), CliError> {
    let report = det_check_report_for_paths(paths)?;

    if list_suppressions {
        match format {
            DetCheckFormat::Text => println!("{}", format_det_suppressions_list(&report)),
            DetCheckFormat::Json => println!("{}", det_suppressions_json(&report)?),
        }
        return Ok(());
    }

    match format {
        DetCheckFormat::Text => {
            println!("{}", format_det_findings_text(&report));
            println!("{}", format_det_suppressions(&report));
        }
        DetCheckFormat::Json => {
            println!("{}", det_check_json(&report)?);
        }
    }

    if let Some(err) = det_check_gate(&report, deny_warnings) {
        return Err(err);
    }
    Ok(())
}

// ── harvest backup verify: post-restore resumability drill (issue #943) ─────

/// Output format for `harvest backup verify`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
pub enum BackupVerifyFormat {
    /// Human-readable operator report (default).
    #[default]
    Text,
    /// Machine-readable `RestoreVerifyReport` JSON for CI consumption.
    Json,
}

/// Parse `--shard` values into [`ShardTarget`]s.
///
/// Accepts either a bare DSN (implicitly shard `0`, the single-shard default)
/// or an explicit `<shard_id>=<dsn>` pair. Rejects an empty list and duplicate
/// shard ids, since either would make the cross-shard verdict meaningless.
///
/// # Errors
///
/// [`CliError::InvalidInput`] on an empty list, a malformed shard id, an empty
/// DSN, or a duplicated shard id.
pub fn parse_shard_targets(raw: &[String]) -> Result<Vec<ShardTarget>, CliError> {
    if raw.is_empty() {
        return Err(CliError::InvalidInput(
            "at least one --shard <DSN> (or --shard <ID>=<DSN>) is required".to_string(),
        ));
    }
    let mut seen: std::collections::BTreeSet<i32> = std::collections::BTreeSet::new();
    let mut out = Vec::with_capacity(raw.len());
    for (idx, spec) in raw.iter().enumerate() {
        let spec = spec.trim();
        // Only split on a `<digits>=` prefix: a DSN can legitimately contain
        // `=` in its query string (`?sslmode=require`), so an unconditional
        // `split_once('=')` would mangle a bare DSN.
        let (shard_id, dsn) = match spec.split_once('=') {
            Some((head, tail)) if !head.is_empty() && head.chars().all(|c| c.is_ascii_digit()) => {
                let id = head.parse::<i32>().map_err(|_| {
                    CliError::InvalidInput(format!("--shard: shard id `{head}` is out of range"))
                })?;
                (id, tail)
            }
            _ => (i32::try_from(idx).unwrap_or(0), spec),
        };
        // An `ExecutionId` carries its shard as `shard & 0xFFFF`, and `0xFFFF`
        // is the reserved `ShardId::UNENCODED` sentinel -- so a value outside
        // `0..=0xFFFE` does not survive the round trip. `65536` truncates to
        // `0`, which would make every target id read out of THIS database
        // decode as shard 0, miss the supplied map, and be written off as
        // "on an uninspected shard": an advisory, exit 0, and the shard the
        // operator supplied never actually checked. Validate with the same
        // rule the shard router uses so the two cannot drift.
        if !autumn_harvest::shard::is_encodable_shard(autumn_harvest::ShardId::new(shard_id)) {
            return Err(CliError::InvalidInput(format!(
                "--shard: shard id `{shard_id}` cannot be encoded into an execution id \
                 (valid range is 0..={})",
                autumn_harvest::shard::MAX_ENCODABLE_SHARD
            )));
        }
        if dsn.trim().is_empty() {
            return Err(CliError::InvalidInput(format!(
                "--shard: shard {shard_id} has an empty DSN"
            )));
        }
        if !seen.insert(shard_id) {
            return Err(CliError::InvalidInput(format!(
                "--shard: shard {shard_id} was supplied more than once"
            )));
        }
        out.push(ShardTarget::new(shard_id, dsn.trim()));
    }
    Ok(out)
}

/// Validate `--default-shard` with the same rule [`parse_shard_targets`] uses
/// for `--shard`.
///
/// An out-of-range value can never match a `--shard` target. Every unencoded
/// reference would then fall to the advisory `uninspected_shard_reference`
/// path, instead of the coherence check this flag exists to enable (issue
/// #1205).
///
/// # Errors
///
/// [`CliError::InvalidInput`] when `default_shard` cannot be encoded into an
/// execution id.
pub fn validate_default_shard(default_shard: i32) -> Result<(), CliError> {
    if autumn_harvest::shard::is_encodable_shard(autumn_harvest::ShardId::new(default_shard)) {
        Ok(())
    } else {
        Err(CliError::InvalidInput(format!(
            "--default-shard: shard id `{default_shard}` cannot be encoded into an \
             execution id (valid range is 0..={})",
            autumn_harvest::shard::MAX_ENCODABLE_SHARD
        )))
    }
}

/// AC4: refuse to run against a DSN that resolves to the same database as the
/// live configuration, unless the operator explicitly acknowledges otherwise.
///
/// Comparison is on `(host, port, database)` only, so a different user or
/// password against the same database is still caught. The guard **fails
/// closed**: a DSN that cannot be parsed is treated as a match.
///
/// # Errors
///
/// [`CliError::InvalidInput`] when a target matches `live` and `ack` is false.
/// The message names the override flag and never echoes a password.
pub fn scratch_guard(dsns: &[String], live: &[String], ack: bool) -> Result<(), CliError> {
    if ack || live.is_empty() {
        return Ok(());
    }
    for dsn in dsns {
        let candidate = strip_shard_prefix(dsn);
        for l in live {
            if dsn_targets_same_database(candidate, l) {
                return Err(CliError::InvalidInput(format!(
                    "refusing to verify `{}`: it resolves to the same database as the live \
                     configuration. Restore into a scratch database first, or pass \
                     --i-know-this-is-scratch if this really is a throwaway copy.",
                    redact_dsn(candidate)
                )));
            }
        }
    }
    Ok(())
}

/// Strip an optional `<shard_id>=` prefix from a `--shard` value.
fn strip_shard_prefix(dsn: &str) -> &str {
    match dsn.split_once('=') {
        Some((head, tail)) if !head.is_empty() && head.chars().all(|c| c.is_ascii_digit()) => tail,
        _ => dsn,
    }
}

/// The stderr notice to print when the live-DSN guard could not actually
/// protect anything.
///
/// The guard is silent by design in two cases, and silence is the dangerous
/// part: an operator who believes a safety net is engaged will point the
/// command at whatever DSN is to hand. Say so out loud instead.
///
/// * No `--live-dsn` was supplied at all — nothing was compared.
/// * `--i-know-this-is-scratch` was passed — the comparison was skipped.
///
/// Returns `None` when the guard genuinely ran against at least one live DSN.
#[must_use]
pub fn scratch_guard_warning(live: &[String], ack: bool) -> Option<String> {
    if ack {
        return Some(
            "WARNING: --i-know-this-is-scratch was passed, so the live-database guard did \
             NOT run. Nothing checked that these DSNs are scratch copies."
                .to_string(),
        );
    }
    if live.is_empty() {
        return Some(
            "WARNING: no --live-dsn was supplied (and HARVEST_DATABASE_URL is unset), so the \
             live-database guard did NOT run. Pass --live-dsn once per live shard to be \
             refused if a --shard target resolves to production."
                .to_string(),
        );
    }
    None
}

/// Serialise a restore-verification report as pretty JSON.
///
/// # Errors
///
/// [`CliError::InvalidInput`] if serialisation fails (not reachable for the
/// report's own types; guarded rather than unwrapped).
pub fn backup_verify_json(report: &RestoreVerifyReport) -> Result<String, CliError> {
    serde_json::to_string_pretty(report)
        .map_err(|e| CliError::InvalidInput(format!("failed to serialise report: {e}")))
}

/// Render a restore-verification report for a human operator.
#[must_use]
pub fn format_backup_verify_text(report: &RestoreVerifyReport) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "restore verification: {}", report.status);
    let _ = writeln!(out, "  generated at: {}", report.generated_at);
    if let Some(skew) = report.restore_point_skew_secs {
        let _ = writeln!(out, "  restore-point skew: {skew}s across shards");
    }

    let replay = &report.replay;
    if replay.verified() {
        let _ = writeln!(
            out,
            "  replay: {} sampled, {} clean, {} divergent, {} workflow-failed, \
             {} skipped (no handler), {} unreadable",
            replay.sampled,
            replay.clean,
            replay.divergent,
            replay.failed,
            replay.skipped_no_handler,
            replay.unreadable
        );
    } else if replay.unreadable > 0
        && (replay.clean > 0 || replay.divergent > 0 || replay.failed > 0)
    {
        // Distinct from the "nothing replayed" branch below. Some histories
        // DID replay here, but at least one selected for replay was never
        // read at all, so the coverage this run reports is incomplete. The
        // "register handlers" advice below does not apply. Handlers ARE
        // registered, since something replayed.
        let _ = writeln!(
            out,
            "  replay: PARTIALLY VERIFIED — {} sampled, {} clean, {} divergent, \
             {} workflow-failed, {} skipped (no handler), {} unreadable. \
             Coverage is incomplete: {} history/histories were never read, so a \
             clean verdict here does not cover them.",
            replay.sampled,
            replay.clean,
            replay.divergent,
            replay.failed,
            replay.skipped_no_handler,
            replay.unreadable,
            replay.unreadable
        );
    } else if replay.unreadable > 0 {
        // Every sample that reached this check was unreadable, and none
        // replayed at all. This is distinct from the branch below, where
        // nothing replayed because no handler was registered. Handlers may
        // well BE registered here. The "register handlers" advice would
        // send an operator chasing the wrong cause. The `history_unreadable`
        // finding above names the actual one (a malformed, legacy, or
        // newer-version payload; a missing row).
        let _ = writeln!(
            out,
            "  replay: NOT VERIFIED — {} sampled, {} unreadable, 0 replayed. Every sampled \
             history failed to read; see the history_unreadable finding above for the cause. \
             Registering workflow handlers will not fix this.",
            replay.sampled, replay.unreadable
        );
    } else {
        let _ = writeln!(
            out,
            "  replay: NOT VERIFIED — {} sampled, {} skipped (no handler), {} unreadable. \
             Register the workflow handlers to make this drill meaningful.",
            replay.sampled, replay.skipped_no_handler, replay.unreadable
        );
    }

    for shard in &report.shards {
        if shard.reachable {
            let _ = writeln!(
                out,
                "\nshard {} ({}) — {} non-terminal execution(s)",
                shard.shard_id,
                shard.dsn,
                shard
                    .non_terminal_executions
                    .map_or_else(|| "unknown".to_string(), |n| n.to_string())
            );
        } else {
            let _ = writeln!(
                out,
                "\nshard {} ({}) — UNREACHABLE: {}",
                shard.shard_id,
                shard.dsn,
                shard.unreachable_reason.as_deref().unwrap_or("unknown")
            );
        }
        write_findings(&mut out, &shard.findings);
    }

    if !report.cross_shard.is_empty() {
        let _ = writeln!(out, "\ncross-shard");
        write_findings(&mut out, &report.cross_shard);
    }

    if report.all_findings().next().is_none() {
        let _ = writeln!(out, "\nno findings.");
    }

    let _ = write!(out, "\nverdict: {}", verdict_advice(report.status));
    out
}

fn write_findings(out: &mut String, findings: &[Finding]) {
    for f in findings {
        let more = if f.truncated { ", truncated" } else { "" };
        let _ = writeln!(
            out,
            "  [{}] {} x{}{} — {}",
            f.severity, f.class, f.count, more, f.explanation
        );
        for sample in &f.samples {
            let _ = writeln!(out, "        {sample}");
        }
        if let Some(detail) = &f.detail {
            let _ = writeln!(out, "        note: {detail}");
        }
    }
}

const fn verdict_advice(status: VerifyStatus) -> &'static str {
    match status {
        VerifyStatus::Clean => "nothing in flight; safe to start workers.",
        VerifyStatus::ResumableWithReclaim => {
            "resumable — the reclaimable findings above heal themselves once workers start."
        }
        VerifyStatus::Incoherent => {
            "DO NOT START WORKERS — the incoherent findings above are broken invariants."
        }
        VerifyStatus::Unavailable => {
            "UNDETERMINED — a shard was unreachable or a probe could not run, so coherence \
             was not actually checked. Do not start workers on the strength of this report."
        }
    }
}

/// Map a report's verdict onto the CLI's exit-code contract.
///
/// `None` means "pass" (exit `0`) for both `Clean` and `ResumableWithReclaim` —
/// a normal restore always carries reclaimable artifacts and must not fail a
/// drill. `Incoherent` exits `1`; `Unavailable` exits `2` so CI can tell
/// "determined broken" apart from "could not determine".
#[must_use]
pub fn backup_verify_gate(report: &RestoreVerifyReport) -> Option<CliError> {
    match report.status {
        VerifyStatus::Clean | VerifyStatus::ResumableWithReclaim => None,
        VerifyStatus::Incoherent => Some(CliError::RestoreIncoherent {
            findings: report
                .all_findings()
                .filter(|f| f.severity == FindingSeverity::Incoherent)
                .count(),
        }),
        VerifyStatus::Unavailable => Some(CliError::RestoreUndetermined {
            unreachable_shards: report.shards.iter().filter(|s| !s.reachable).count(),
            // Sum each finding's `count`, not the number of findings: one
            // `ProbeFailed` finding aggregates every probe that could not run
            // on that shard, so counting findings would under-report by an
            // order of magnitude and make the message read far less alarming
            // than the truth.
            failed_probes: report
                .all_findings()
                .filter(|f| f.severity == FindingSeverity::Undetermined)
                .map(|f| usize::try_from(f.count).unwrap_or(usize::MAX))
                .sum(),
        }),
    }
}

/// Run `harvest backup verify` end to end.
///
/// # Errors
///
/// [`CliError::InvalidInput`] on bad arguments or a refused live DSN;
/// [`CliError::RestoreIncoherent`] / [`CliError::RestoreUndetermined`] when the
/// report fails the gate. The report itself is always printed first.
#[allow(clippy::too_many_arguments)]
pub async fn run_backup_verify(
    shards: &[String],
    live_dsn: &[String],
    ack: bool,
    format: BackupVerifyFormat,
    replay_sample: usize,
    worker_stale_secs: i64,
    probe_limit: i64,
    default_shard: i32,
) -> Result<(), CliError> {
    scratch_guard(shards, live_dsn, ack)?;
    // Say out loud when the guard could not protect anything -- an operator
    // who believes a safety net is engaged is the one who points this at
    // production. On stderr so `-o json` stdout stays parseable.
    if let Some(w) = scratch_guard_warning(live_dsn, ack) {
        eprintln!("{w}");
    }
    let targets = parse_shard_targets(shards)?;
    validate_default_shard(default_shard)?;

    let options = VerifyOptions::default()
        .with_replay_sample(replay_sample)
        .with_worker_stale_secs(worker_stale_secs)
        .with_probe_limit(probe_limit)
        .with_default_shard(default_shard)
        .with_scratch_ack(ack);

    // The CLI ships no application workflow handlers, so replay coverage is
    // reported as NOT VERIFIED rather than silently claimed. An embedder that
    // wants replay coverage calls `verify_restore` from its own binary with a
    // populated `WorkflowReplayer` (see docs/runbooks/backup-restore.md).
    let replayer = WorkflowReplayer::new();
    let report = verify_restore(&targets, &options, &replayer).await;

    match format {
        BackupVerifyFormat::Text => println!("{}", format_backup_verify_text(&report)),
        BackupVerifyFormat::Json => println!("{}", backup_verify_json(&report)?),
    }

    backup_verify_gate(&report).map_or(Ok(()), Err)
}

// ── harvest migrate: dedicated Harvest-database migrations (issue #1240) ────

/// Assemble the migration set to apply: Harvest's own, embedded in this binary,
/// plus every `--include-dir` set in the order given.
///
/// The combined set is validated for duplicate versions here rather than per
/// directory, because that is exactly where a collision arises — two
/// independently authored sets that picked the same version. Diesel's ledger is
/// keyed by version alone, so one of them would otherwise be recorded and never
/// run.
///
/// # Errors
///
/// [`CliError::InvalidInput`] when a directory cannot be read, when a migration
/// in it has no readable `up.sql`, or when two migrations share a version.
fn migration_set(include_dir: &[PathBuf]) -> Result<Vec<MigrationScript>, CliError> {
    let mut scripts = autumn_harvest::migrate::embedded();
    for dir in include_dir {
        let extra = autumn_harvest::migrate::from_directory(dir).map_err(|error| {
            CliError::InvalidInput(format!("--include-dir `{}`: {error}", dir.display()))
        })?;
        scripts.extend(extra);
    }
    autumn_harvest::migrate::validate_versions(&scripts)
        .map_err(|error| CliError::InvalidInput(error.to_string()))?;
    Ok(scripts)
}

/// Establish the connection `harvest migrate` works over.
///
/// Deliberately not [`autumn_harvest::migrate::connect`]: that is
/// `AsyncPgConnection::establish`, which is `NoTls` and so cannot reach a
/// database whose `sslmode` demands TLS — the common shape of a managed
/// Harvest database, and exactly the production case this command exists for
/// (issue #1240). This builds the same rustls-backed connector autumn-web's
/// own migration path uses, so the two agree about which databases are
/// reachable.
///
/// **TLS is always verified** — certificate chain *and* hostname, against the
/// platform's trust store. That is stricter than libpq, whose `require` and
/// `prefer` encrypt without authenticating: a certificate this cannot verify is
/// refused here, where libpq would connect.
///
/// What that means per mode, precisely:
///
/// * `disable` — plaintext, no trust store consulted.
/// * `prefer` (the default) — TLS is attempted; tokio-postgres falls back to
///   plaintext only when the **server declines** TLS, not when the handshake
///   fails. So an untrusted or hostname-mismatched certificate is an error
///   here rather than a silent downgrade to an unauthenticated connection.
///   Deliberate: this command carries a database password and applies schema
///   changes, and "the certificate was wrong so we sent the credential in
///   clear" is not a fallback worth having. Use `sslmode=disable` to say
///   plaintext out loud, or put the CA in the trust store.
/// * `require` / `verify-ca` / `verify-full` — TLS, verified. An empty trust
///   store is a named error rather than a downgrade.
///
/// # Errors
///
/// [`CliError::Migrate`] when the DSN cannot be parsed, when the platform has
/// no usable trust store, or when the connection cannot be established.
async fn connect_for_migration(
    database_url: &str,
    redacted: &str,
) -> Result<AsyncPgConnection, CliError> {
    let dsn = normalize_sslmode(database_url);
    let config: tokio_postgres::Config = dsn
        .parse()
        .map_err(|error: tokio_postgres::Error| migrate_error(database_url, redacted, &error))?;

    // `sslmode=disable` never touches the TLS configuration, so it must not be
    // gated on one: a minimal container with no CA bundle is a perfectly good
    // place to migrate a plaintext database, and failing there would contradict
    // what this command promises.
    if config.get_ssl_mode() == tokio_postgres::config::SslMode::Disable {
        return autumn_harvest::migrate::connect(database_url)
            .await
            .map_err(|error| migrate_error(database_url, redacted, &error));
    }

    let mut roots = rustls::RootCertStore::empty();
    let native = rustls_native_certs::load_native_certs();
    for cert in native.certs {
        // A malformed certificate in the system store is not a reason to fail:
        // rustls rejects it, the rest still anchor the chain.
        let _ = roots.add(cert);
    }
    if roots.is_empty() {
        // With no anchors, every TLS handshake would fail verification. What
        // that should mean depends on whether the operator asked for TLS:
        // under `require` it is a hard error, and under `prefer` -- which is
        // libpq's "TLS if it works, plaintext otherwise" -- plaintext is the
        // documented degradation. Never the reverse: a `require` DSN is never
        // quietly downgraded.
        if config.get_ssl_mode() == tokio_postgres::config::SslMode::Prefer {
            eprintln!(
                "warning: {redacted}: no usable certificates in the platform trust \
                 store, so no TLS connection could be verified; sslmode=prefer \
                 therefore connects in PLAINTEXT. Install your distribution's \
                 ca-certificates package, or pass sslmode=require to fail instead. \
                 (With a trust store present, a certificate that fails to verify \
                 is an error, not a downgrade.)"
            );
            return autumn_harvest::migrate::connect(database_url)
                .await
                .map_err(|error| migrate_error(database_url, redacted, &error));
        }
        return Err(CliError::Migrate {
            database: redacted.to_string(),
            reason: format!(
                "no usable certificates in the platform trust store, so a TLS \
                 connection cannot be verified (install your distribution's \
                 ca-certificates package). Loader errors: {:?}",
                native.errors
            ),
        });
    }

    // An explicit provider rather than the process-wide default: nothing else
    // in this binary installs one, and `ClientConfig::builder()` panics when
    // there is none.
    let provider = std::sync::Arc::new(rustls::crypto::ring::default_provider());
    let tls_config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|error| migrate_error(database_url, redacted, &error))?
        .with_root_certificates(roots)
        .with_no_client_auth();

    let (client, connection) = config
        .connect(tokio_postgres_rustls::MakeRustlsConnect::new(tls_config))
        .await
        .map_err(|error| migrate_error(database_url, redacted, &error))?;

    // `try_from_client_and_connection` drives the connection task itself and
    // surfaces its errors on the connection, so a dropped socket mid-migration
    // is reported rather than hanging.
    AsyncPgConnection::try_from_client_and_connection(client, connection)
        .await
        .map_err(|error| migrate_error(database_url, redacted, &error))
}

/// Rewrite `sslmode=verify-ca` / `verify-full` to `require`.
///
/// tokio-postgres 0.7 accepts only `disable`, `prefer` and `require`, and
/// **fails to parse** the DSN otherwise — so a `verify-full` DSN that libpq and
/// the `diesel` CLI accept would be rejected before a connection is attempted.
///
/// Substituting `require` is not a downgrade: verification is the rustls
/// connector's job here, and it always checks the chain and the hostname. The
/// two verify modes therefore describe what [`connect_for_migration`] already
/// does unconditionally.
///
/// Handles both DSN spellings — a URL (`postgres://…?sslmode=verify-full`) and
/// libpq keyword form (`host=… sslmode=verify-full`) — and leaves anything else
/// byte-identical.
#[must_use]
pub fn normalize_sslmode(dsn: &str) -> String {
    const VERIFY_MODES: [&str; 2] = ["verify-ca", "verify-full"];

    if let Ok(mut url) = url::Url::parse(dsn) {
        let needs_rewrite = url
            .query_pairs()
            .any(|(k, v)| k == "sslmode" && VERIFY_MODES.contains(&v.as_ref()));
        if !needs_rewrite {
            return dsn.to_string();
        }
        let rewritten: Vec<(String, String)> = url
            .query_pairs()
            .map(|(k, v)| {
                if k == "sslmode" && VERIFY_MODES.contains(&v.as_ref()) {
                    (k.into_owned(), "require".to_string())
                } else {
                    (k.into_owned(), v.into_owned())
                }
            })
            .collect();
        url.query_pairs_mut()
            .clear()
            .extend_pairs(rewritten)
            .finish();
        return url.to_string();
    }

    rewrite_keyword_dsn(dsn, &VERIFY_MODES)
}

/// Rewrite the top-level `sslmode` option of a libpq **keyword/value** DSN
/// (`host=… sslmode=verify-full`), leaving every other byte alone.
///
/// Scans the DSN the way libpq reads it — options separated by whitespace,
/// optional whitespace around `=`, values optionally single-quoted with `\`
/// escapes — rather than splitting on whitespace. A whitespace split cannot see
/// quoting, so `password='abc sslmode=verify-full def'` would have had the text
/// *inside the password* rewritten, corrupting the credential and failing
/// authentication. It also could not see `sslmode = verify-full`, which libpq
/// accepts and this now rewrites.
///
/// A DSN this cannot scan (an unterminated quote, a missing `=`) is returned
/// **unchanged**, so tokio-postgres reports its own parse error rather than
/// this mangling the input first.
fn rewrite_keyword_dsn(dsn: &str, verify_modes: &[&str]) -> String {
    scan_keyword_dsn(dsn, |key, value| {
        (key == "sslmode" && verify_modes.contains(&value)).then(|| "require".to_string())
    })
    .unwrap_or_else(|| dsn.to_string())
}

/// Redact the secrets in a libpq **keyword/value** DSN, leaving the rest legible.
///
/// [`redact_dsn`] parses URLs, and answers `<unparseable dsn>` for the keyword
/// form. That is safe but useless as a *label*: repeat `--database-url` with
/// three keyword-form shards and every line of the report reads the same, which
/// is exactly the "which databases did we already migrate?" question a partial
/// report exists to answer.
///
/// Returns `None` when the DSN cannot be scanned, so a caller can fall back
/// rather than print something it has not actually inspected.
fn redact_keyword_dsn(dsn: &str) -> Option<String> {
    // Any key whose name carries `password` — `password`, and a future
    // `sslpassword` — loses its value. Everything else (host, dbname, user,
    // port) is what makes one target tellable from another.
    scan_keyword_dsn(dsn, |key, _| {
        key.to_ascii_lowercase()
            .contains("password")
            .then(|| "***".to_string())
    })
}

/// Scan a libpq keyword/value DSN, replacing the values `replace` returns
/// `Some` for and copying every other byte verbatim.
///
/// Reads the DSN the way libpq does — options separated by whitespace, optional
/// whitespace around `=`, values optionally single-quoted, backslash escaping
/// the next character in either form. A whitespace split cannot see quoting, so
/// `password='abc sslmode=verify-full def'` would otherwise have the text
/// *inside the password* rewritten.
///
/// Returns `None` for a DSN this cannot scan — an unterminated quote, a missing
/// `=`, a value that never arrives — leaving the caller to pass the original
/// through so tokio-postgres reports its own parse error rather than this
/// mangling the input first.
fn scan_keyword_dsn(
    dsn: &str,
    mut replace: impl FnMut(&str, &str) -> Option<String>,
) -> Option<String> {
    let bytes = dsn.as_bytes();
    let mut out = String::with_capacity(dsn.len());
    let mut i = 0;

    while i < bytes.len() {
        // Whitespace between options, copied verbatim.
        let start = i;
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        out.push_str(&dsn[start..i]);
        if i >= bytes.len() {
            break;
        }

        // Keyword, then `=` with optional whitespace on either side.
        let key_start = i;
        while i < bytes.len() && bytes[i] != b'=' && !bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        let key = &dsn[key_start..i];
        // The key must be a keyword libpq actually recognizes, not merely
        // keyword-SHAPED. Checking only the character set is not enough: a
        // mistyped URL that keeps a credential but loses the `://`
        // (`postgres=//alice:hunter2@db/harvest`) scans as the "keyword"
        // `postgres`, needs no redaction because there is no `password=`, and
        // comes back whole -- password included -- as if it had been examined.
        // Rejecting here yields the caller's `<unparseable dsn>` label, which
        // is what an input tokio-postgres will also reject should produce.
        if !is_connection_keyword(key) {
            return None;
        }
        let spacing_start = i;
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] != b'=' {
            return None;
        }
        i += 1;
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        // A DSN that ends after `=` (`host=db password=`) has no value to read;
        // indexing here would panic before tokio-postgres could say so.
        if i >= bytes.len() {
            return None;
        }
        out.push_str(key);
        out.push_str(&dsn[spacing_start..i]);

        // Value: single-quoted or bare, `\` escaping the next character in both.
        let value_start = i;
        let mut value = String::new();
        if bytes[i] == b'\'' {
            i += 1;
            loop {
                if i >= bytes.len() {
                    // Unterminated quote: not ours to interpret.
                    return None;
                }
                match bytes[i] {
                    b'\\' if i + 1 < bytes.len() => {
                        // Advance past the WHOLE escaped character: `\é` is
                        // three bytes, and `i += 2` would leave `i` inside it,
                        // so the next `dsn[i..]` slice panics on a non-char
                        // boundary instead of connecting.
                        let escaped = dsn[i + 1..].chars().next().unwrap_or_default();
                        value.push(escaped);
                        i += 1 + escaped.len_utf8();
                    }
                    b'\'' => {
                        i += 1;
                        break;
                    }
                    _ => {
                        let c = dsn[i..].chars().next().unwrap_or_default();
                        value.push(c);
                        i += c.len_utf8();
                    }
                }
            }
        } else {
            while i < bytes.len() && !bytes[i].is_ascii_whitespace() {
                if bytes[i] == b'\\' && i + 1 < bytes.len() {
                    let escaped = dsn[i + 1..].chars().next().unwrap_or_default();
                    value.push(escaped);
                    i += 1 + escaped.len_utf8();
                } else {
                    let c = dsn[i..].chars().next().unwrap_or_default();
                    value.push(c);
                    i += c.len_utf8();
                }
            }
        }

        match replace(key, &value) {
            Some(replacement) => out.push_str(&replacement),
            // Verbatim, quotes and escapes included: only the options the
            // caller asked about are ever touched.
            None => out.push_str(&dsn[value_start..i]),
        }
    }

    Some(out)
}

/// The label a migration target is reported under: its DSN with the credential
/// removed, and — when it cannot be redacted at all — an ordinal, so repeated
/// targets stay tellable apart.
///
/// `ordinal` is the target's 1-based position on the command line.
#[must_use]
pub fn migrate_target_label(dsn: &str, ordinal: usize) -> String {
    const UNPARSEABLE: &str = "<unparseable dsn>";
    /// `redact_dsn`'s other whole-DSN withholding: a URL carrying its password
    /// in the query string cannot be rewritten safely, so it returns this
    /// instead. Like the unparseable case it is the same for every target.
    const WITHHELD: &str = "<redacted dsn>";

    // Decide the FORM first, then redact with the reader for that form. The
    // other order leaks: `redact_dsn` parses with a general URL parser, and
    // `alice:hunter2@db.internal/harvest` is a syntactically fine URL whose
    // scheme is `alice` and whose password is nowhere the parser looks — so it
    // came back unredacted and went into the log. libpq's own rule is the
    // scheme prefix, so use exactly that.
    let trimmed = dsn.trim_start();
    let is_url_form = ["postgres://", "postgresql://"].iter().any(|scheme| {
        trimmed
            .get(..scheme.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(scheme))
    });

    if is_url_form {
        let redacted = redact_dsn(dsn);
        // A URL is a URL, malformed or not: if the URL reader cannot read it,
        // the keyword scanner has no business trying. Whenever the reader
        // withholds the WHOLE DSN -- unparseable, or a password in the query
        // string it cannot rewrite -- the label carries no identity, so every
        // target would report the same. Keep the reason and add the ordinal.
        return if redacted == UNPARSEABLE || redacted == WITHHELD {
            format!("{redacted} #{ordinal}")
        } else {
            redacted
        };
    }

    redact_keyword_dsn(dsn).unwrap_or_else(|| format!("{UNPARSEABLE} #{ordinal}"))
}

/// Whether `key` is a libpq connection keyword.
///
/// Deliberately a superset of what tokio-postgres itself accepts. Erring wide
/// only risks handing back a redacted label for a DSN the driver will reject
/// anyway; erring narrow would downgrade a legitimate DSN's label to
/// `<unparseable dsn>` and cost an operator the identity of the failing shard.
/// What it must not admit is a token that is keyword-shaped but not a keyword,
/// which is how a mistyped URL smuggles a password past redaction.
fn is_connection_keyword(key: &str) -> bool {
    const KEYWORDS: &[&str] = &[
        "application_name",
        "channel_binding",
        "client_encoding",
        "connect_timeout",
        "dbname",
        "fallback_application_name",
        "gssdelegation",
        "gssencmode",
        "gsslib",
        "host",
        "hostaddr",
        "keepalives",
        "keepalives_count",
        "keepalives_idle",
        "keepalives_interval",
        "krbsrvname",
        "load_balance_hosts",
        "options",
        "passfile",
        "password",
        "port",
        "replication",
        "require_auth",
        "requirepeer",
        "requiressl",
        "scram_client_key",
        "scram_server_key",
        "service",
        "ssl_max_protocol_version",
        "ssl_min_protocol_version",
        "sslcert",
        "sslcertmode",
        "sslcompression",
        "sslcrl",
        "sslcrldir",
        "sslkey",
        "sslmode",
        "sslnegotiation",
        "sslpassword",
        "sslrootcert",
        "sslsni",
        "target_session_attrs",
        "tcp_user_timeout",
        "user",
    ];
    // libpq keywords are lowercase; compare case-insensitively so a DSN
    // written `Host=db` keeps its label rather than being called unparseable.
    KEYWORDS
        .iter()
        .any(|keyword| key.eq_ignore_ascii_case(keyword))
}

/// Render a migration failure without leaking the DSN.
///
/// A migration command runs from deploy pipelines whose logs are read far more
/// widely than the credential in `harvest.database.url`, and a driver is free
/// to quote the connection string it was handed. Substituting the redacted form
/// costs nothing and removes the whole class.
fn migrate_error(database_url: &str, redacted: &str, error: &impl std::fmt::Display) -> CliError {
    CliError::Migrate {
        database: redacted.to_string(),
        reason: error.to_string().replace(database_url, redacted),
    }
}

/// Human-readable `migrate status` report: one block per database.
///
/// `heading` distinguishes a plain status report from `run --dry-run`, which
/// renders the same plan under a different promise.
#[must_use]
pub fn format_migrate_plan_text(heading: &str, targets: &[(String, MigrationPlan)]) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "{heading}");
    for (database, plan) in targets {
        let _ = writeln!(out, "  {database}");
        if !plan.ledger_exists {
            let _ = writeln!(
                out,
                "    ledger:  absent — this database has never been migrated"
            );
        }
        let _ = writeln!(out, "    applied: {}", plan.already_applied.len());
        let _ = writeln!(out, "    pending: {}", plan.pending.len());
        for script in &plan.pending {
            let _ = writeln!(out, "      {}", script.name);
        }
        // Reported, never removed: the usual cause is a newer build having
        // already migrated this database, but it also catches a DSN pointed at
        // the wrong one.
        if !plan.unrecognized.is_empty() {
            let _ = writeln!(
                out,
                "    unrecognized: {} ledger row(s) this binary does not know \
                 (is it older than the deployed schema?)",
                plan.unrecognized.len()
            );
            for version in &plan.unrecognized {
                let _ = writeln!(out, "      {version}");
            }
        }
    }
    let pending: usize = targets.iter().map(|(_, plan)| plan.pending.len()).sum();
    let _ = write!(
        out,
        "{pending} pending migration(s) across {} database(s)",
        targets.len()
    );
    out
}

/// A report for a target the run could not even inspect.
const fn empty_migration_report() -> MigrationReport {
    MigrationReport {
        applied: Vec::new(),
        already_applied: Vec::new(),
        applied_concurrently: Vec::new(),
        unrecognized: Vec::new(),
        // Nothing ran, so nothing ran unserialized.
        ledger_lock_available: true,
        applied_unserialized: Vec::new(),
        failed: None,
    }
}

/// One database's outcome in a `migrate run` report.
#[derive(Debug)]
pub struct MigrateRunTarget {
    /// Redacted DSN, or an ordinal label when it could not be redacted.
    pub database: String,
    /// What the run did here.
    pub report: MigrationReport,
    /// The run never reached a migration on this target: connecting, creating
    /// the ledger, or reading it failed.
    ///
    /// Distinguished because an empty report is otherwise indistinguishable
    /// from "finished, nothing to do" — a JSON consumer would read `applied:
    /// []`, `failed: null` on a database the run could not even inspect and
    /// call it done.
    pub setup_failed: bool,
}

/// Human-readable `migrate run` report: one block per database.
#[must_use]
pub fn format_migrate_run_text(targets: &[MigrateRunTarget]) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "harvest migrate run");
    for MigrateRunTarget {
        database,
        report,
        setup_failed,
    } in targets
    {
        let _ = writeln!(out, "  {database}");
        let _ = writeln!(out, "    applied: {}", report.applied.len());
        for name in &report.applied {
            let _ = writeln!(out, "      {name}");
        }
        let _ = writeln!(out, "    already applied: {}", report.already_applied.len());
        // Another migrator committed these between the plan and the apply. Said
        // out loud because "applied: 0" on the database an operator is watching
        // otherwise looks like the command did nothing.
        if !report.applied_concurrently.is_empty() {
            let _ = writeln!(
                out,
                "    applied by a concurrent migrator: {}",
                report.applied_concurrently.len()
            );
            for name in &report.applied_concurrently {
                let _ = writeln!(out, "      {name}");
            }
        }
        if !report.unrecognized.is_empty() {
            let _ = writeln!(
                out,
                "    unrecognized: {} ledger row(s) this binary does not know \
                 (is it older than the deployed schema?)",
                report.unrecognized.len()
            );
        }
        // The `harvest` binary installs no tracing subscriber, so the engine's
        // own warning about this goes nowhere. An operator only learns that
        // concurrent runs are unsafe here if the report says so.
        if !report.applied_unserialized.is_empty() {
            let _ = writeln!(
                out,
                "    WARNING: {} migration(s) applied WITHOUT the ledger lock, so a \
                 concurrent migrator was not serialized against them:",
                report.applied_unserialized.len()
            );
            // Per migration, not per run: a run can contain both reasons, and
            // they take different remedies. Naming one cause for the whole
            // list would send an operator to change a grant that cannot help
            // the non-transactional entries.
            for entry in &report.applied_unserialized {
                let cause = match entry.reason {
                    UnserializedReason::LedgerLockUnavailable => {
                        "this role lacks UPDATE/DELETE/TRUNCATE on __diesel_schema_migrations"
                    }
                    UnserializedReason::NoTransaction => {
                        "declares run_in_transaction = false, so there is no transaction \
                         to hold the lock in -- no grant changes this"
                    }
                };
                let _ = writeln!(out, "      {} -- {cause}", entry.name);
            }
            let _ = writeln!(
                out,
                "      Run migrators one at a time against this database."
            );
        }
        if *setup_failed {
            let _ = writeln!(
                out,
                "    FAILED: before any migration ran -- this database could not \
                 be prepared (see the error below); nothing was applied here"
            );
        }
        if let Some(failed) = &report.failed {
            let _ = writeln!(out, "    FAILED: {}", failed.name);
            let _ = writeln!(
                out,
                "      {}",
                if failed.rolled_back {
                    "rolled back -- this database is as it was before that migration"
                } else {
                    "NOT rolled back (run_in_transaction = false) -- any statement \
                     of it that already succeeded still stands. Inspect what it \
                     left behind and repair it before deciding whether a re-run \
                     is safe: nothing here can know the body is idempotent"
                }
            );
        }
    }
    let applied: usize = targets.iter().map(|t| t.report.applied.len()).sum();
    let _ = write!(
        out,
        "{applied} migration(s) applied across {} database(s)",
        targets.len()
    );
    out
}

/// Machine-readable `migrate status` / `run --dry-run` report.
///
/// # Errors
///
/// [`CliError::SerializeResponse`] if the report cannot be serialized.
pub fn migrate_plan_json(
    command: &str,
    targets: &[(String, MigrationPlan)],
) -> Result<String, CliError> {
    let body = json!({
        "command": command,
        "targets": targets
            .iter()
            .map(|(database, plan)| json!({
                "database": database,
                "ledger_exists": plan.ledger_exists,
                "already_applied": plan.already_applied,
                "pending": plan.pending.iter().map(|s| &s.name).collect::<Vec<_>>(),
                "unrecognized": plan.unrecognized,
            }))
            .collect::<Vec<_>>(),
        "pending_total": targets.iter().map(|(_, plan)| plan.pending.len()).sum::<usize>(),
    });
    serde_json::to_string_pretty(&body).map_err(CliError::SerializeResponse)
}

/// Machine-readable `migrate run` report.
///
/// # Errors
///
/// [`CliError::SerializeResponse`] if the report cannot be serialized.
pub fn migrate_run_json(targets: &[MigrateRunTarget]) -> Result<String, CliError> {
    let body = json!({
        "command": "migrate run",
        "targets": targets
            .iter()
            .map(|target| json!({
                "database": target.database,
                "applied": target.report.applied,
                "already_applied": target.report.already_applied,
                "applied_concurrently": target.report.applied_concurrently,
                "unrecognized": target.report.unrecognized,
                "ledger_lock_available": target.report.ledger_lock_available,
                "applied_unserialized": target.report.applied_unserialized,
                // The run could not prepare this database at all. Without it an
                // empty report reads as "finished with nothing to do".
                "setup_failed": target.setup_failed,
                // `null` on a target that finished. On one that did not, the
                // migration it stopped on and whether that rolled back -- the
                // difference between "nothing changed" and "something changed
                // and the run cannot say what".
                "failed": target.report.failed.as_ref().map(|failed| json!({
                    "name": failed.name,
                    "rolled_back": failed.rolled_back,
                })),
            }))
            .collect::<Vec<_>>(),
        "applied_total": targets.iter().map(|t| t.report.applied.len()).sum::<usize>(),
    });
    serde_json::to_string_pretty(&body).map_err(CliError::SerializeResponse)
}

/// The `--check` deploy gate: pending migrations anywhere fail the command.
#[must_use]
pub fn migrate_pending_gate(targets: &[(String, MigrationPlan)]) -> Option<CliError> {
    let pending: usize = targets.iter().map(|(_, plan)| plan.pending.len()).sum();
    if pending == 0 {
        return None;
    }
    Some(CliError::MigrationsPending {
        pending,
        databases: targets
            .iter()
            .filter(|(_, plan)| plan.has_pending())
            .count(),
    })
}

/// Read every target's migration plan, leaving all of them untouched.
async fn migrate_plans(
    database_url: &[String],
    scripts: &[MigrationScript],
) -> Result<Vec<(String, MigrationPlan)>, CliError> {
    let mut targets = Vec::with_capacity(database_url.len());
    for (ordinal, url) in database_url.iter().enumerate() {
        let redacted = migrate_target_label(url, ordinal + 1);
        let mut conn = connect_for_migration(url, &redacted).await?;
        let plan = autumn_harvest::migrate::plan_on_connection(&mut conn, scripts)
            .await
            .map_err(|error| migrate_error(url, &redacted, &error))?;
        targets.push((redacted, plan));
    }
    Ok(targets)
}

/// Print the targets a `run` finished before it failed, then return the
/// failure.
///
/// A multi-shard run stops at the first failing target, so what came before it
/// is already migrated and what comes after is untouched. Reporting that split
/// is the difference between "re-run the command" and "work out by hand which
/// databases moved".
fn report_and_fail(
    targets: &[MigrateRunTarget],
    format: MigrateFormat,
    failure: CliError,
) -> Result<(), CliError> {
    if !targets.is_empty() {
        match format {
            MigrateFormat::Text => println!("{}", format_migrate_run_text(targets)),
            MigrateFormat::Json => println!("{}", migrate_run_json(targets)?),
        }
    }
    Err(failure)
}

/// Run `harvest migrate status` end to end.
///
/// # Errors
///
/// [`CliError::InvalidInput`] on an unreadable `--include-dir` or a duplicate
/// migration version; [`CliError::Migrate`] when a database cannot be reached;
/// [`CliError::MigrationsPending`] when `--check` is set and any target still
/// has a pending migration. The report is printed before the gate fires.
pub async fn run_migrate_status(
    database_url: &[String],
    include_dir: &[PathBuf],
    format: MigrateFormat,
    check: bool,
) -> Result<(), CliError> {
    let scripts = migration_set(include_dir)?;
    let targets = migrate_plans(database_url, &scripts).await?;

    match format {
        MigrateFormat::Text => println!(
            "{}",
            format_migrate_plan_text("harvest migrate status", &targets)
        ),
        MigrateFormat::Json => println!("{}", migrate_plan_json("migrate status", &targets)?),
    }

    if let Some(error) = check.then(|| migrate_pending_gate(&targets)).flatten() {
        return Err(error);
    }
    Ok(())
}

/// Run `harvest migrate run` end to end.
///
/// Databases are migrated in the order given and a failure stops the run: the
/// remaining targets are left untouched rather than migrated behind a database
/// that already failed. Whatever completed first is still reported, so an
/// operator can see exactly how far the deploy step got.
///
/// # Errors
///
/// [`CliError::InvalidInput`] on an unreadable `--include-dir` or a duplicate
/// migration version; [`CliError::Migrate`] when a database cannot be reached
/// or a migration fails.
pub async fn run_migrate_run(
    database_url: &[String],
    include_dir: &[PathBuf],
    format: MigrateFormat,
    dry_run: bool,
) -> Result<(), CliError> {
    let scripts = migration_set(include_dir)?;

    if dry_run {
        let targets = migrate_plans(database_url, &scripts).await?;
        match format {
            MigrateFormat::Text => println!(
                "{}",
                format_migrate_plan_text("harvest migrate run --dry-run", &targets)
            ),
            MigrateFormat::Json => {
                println!("{}", migrate_plan_json("migrate run --dry-run", &targets)?);
            }
        }
        return Ok(());
    }

    let mut targets = Vec::with_capacity(database_url.len());
    for (ordinal, url) in database_url.iter().enumerate() {
        let redacted = migrate_target_label(url, ordinal + 1);
        // Every failure past the first target -- connecting to it as much as
        // migrating it -- goes through `report_and_fail`: on a multi-shard run
        // the operator needs to know which databases are already migrated
        // before deciding what to do next, and an unreachable third shard says
        // nothing about the two behind it.
        let mut conn = match connect_for_migration(url, &redacted).await {
            Ok(conn) => conn,
            Err(failure) => {
                // Named as a target that never started, rather than omitted:
                // "absent" would be indistinguishable from the targets after it
                // that the run simply never reached.
                targets.push(MigrateRunTarget {
                    database: redacted,
                    report: empty_migration_report(),
                    setup_failed: true,
                });
                return report_and_fail(&targets, format, failure);
            }
        };
        match autumn_harvest::migrate::apply_to_connection(&mut conn, &scripts).await {
            Ok(report) => targets.push(MigrateRunTarget {
                database: redacted,
                report,
                setup_failed: false,
            }),
            Err(partial) => {
                let failure = migrate_error(url, &redacted, &partial.error);
                // The failing target always joins the report, even with nothing
                // applied: a `run_in_transaction = false` migration that failed
                // part-way leaves changes it cannot list, and "no report at
                // all" and "nothing happened" must not look the same to the
                // tooling reading this.
                //
                // `failed: None` means no migration failed, so the run never
                // got that far -- the ledger could not be created or read.
                // Flagged, or an empty report would read as a clean finish.
                let setup_failed = partial.report.failed.is_none();
                targets.push(MigrateRunTarget {
                    database: redacted,
                    report: partial.report,
                    setup_failed,
                });
                return report_and_fail(&targets, format, failure);
            }
        }
    }

    match format {
        MigrateFormat::Text => println!("{}", format_migrate_run_text(&targets)),
        MigrateFormat::Json => println!("{}", migrate_run_json(&targets)?),
    }
    Ok(())
}

// ── harvest dr: cross-region disaster recovery (issue #954) ─────────────────

/// One shard's DR state, as `harvest dr status` reports it.
///
/// Serialized by hand rather than by `serde::Serialize`: this crate depends on
/// `serde_json` but not on `serde` itself, and one small projection is a better
/// trade than a new dependency.
#[derive(Debug)]
struct DrShardStatus {
    shard_id: i32,
    /// Redacted — a DSN can embed a password.
    dsn: String,
    reachable: bool,
    unreachable_reason: Option<String>,
    /// `None` when this shard has never been provisioned, which means fencing
    /// is not yet in force there. Distinct from `generation_error`: "there is
    /// no fence here" and "we could not tell" are different answers, and the
    /// runbook's verification step ("every shard's generation must have
    /// increased") reads the second as the first if they are collapsed.
    generation: Option<String>,
    /// Why the generation could not be read, when it could not be.
    generation_error: Option<String>,
    /// The measured RPO. **`None` means UNKNOWN, not zero** — see
    /// `docs/cross-region-dr.md`. Serialized as an explicit JSON `null` so a
    /// consumer cannot mistake absence for a value of `0`.
    rpo_seconds: Option<f64>,
    /// Whether `rpo_seconds` is an exact reading or only a **lower bound**.
    ///
    /// `true` when the standby has fallen behind the whole retained watermark
    /// trail: the RPO is at least the reported value and unbounded above.
    /// "42 seconds" and "at least an hour, we cannot see how much more" are
    /// different failover decisions, so the table renders the second as `≥`.
    rpo_is_lower_bound: bool,
    lag_bytes: Option<i64>,
    /// `None` when the replication views could not be read at all.
    ///
    /// Distinct from `Some(0)`, and the distinction is the point: `0` is the
    /// definitive "this shard has no standby, its RPO is unbounded", while
    /// `None` is "the role cannot read `pg_stat_replication` — most likely a
    /// missing `GRANT pg_monitor`". Collapsing the second into the first
    /// printed a categorical no-standby warning for a permissions gap, to an
    /// operator deciding whether to fail over.
    connected_standbys: Option<usize>,
    inactive_slots: Option<usize>,
    /// Why the replication views were unreadable, when they were.
    replication_error: Option<String>,
}

impl DrShardStatus {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "shard_id": self.shard_id,
            "dsn": self.dsn,
            "reachable": self.reachable,
            "unreachable_reason": self.unreachable_reason,
            "generation": self.generation,
            "generation_error": self.generation_error,
            // Explicit null, never 0: see the field docs.
            "rpo_seconds": self.rpo_seconds,
            "rpo_is_lower_bound": self.rpo_is_lower_bound,
            "lag_bytes": self.lag_bytes,
            "connected_standbys": self.connected_standbys,
            "inactive_slots": self.inactive_slots,
            "replication_error": self.replication_error,
        })
    }
}

async fn dr_connect(
    dsn: &str,
) -> Result<autumn_harvest::diesel_async::AsyncPgConnection, CliError> {
    use autumn_harvest::diesel_async::AsyncConnection as _;
    autumn_harvest::diesel_async::AsyncPgConnection::establish(dsn)
        .await
        .map_err(|e| {
            CliError::InvalidInput(format!(
                "cannot connect to {}: {e}",
                autumn_harvest::backup_verify::redact_dsn(dsn)
            ))
        })
}

/// Connect for a read-only DR command, with the session pinned read-only.
///
/// `harvest dr status` is documented as "read-only, safe at any time" and is
/// pointed at a **production primary** during an incident. `backup verify`
/// backs the same promise with `SET SESSION CHARACTERISTICS AS TRANSACTION
/// READ ONLY` rather than with care; this does too, so Postgres itself enforces
/// it. `fence` and `promote` deliberately do not use this.
async fn dr_connect_read_only(
    dsn: &str,
) -> Result<autumn_harvest::diesel_async::AsyncPgConnection, CliError> {
    use autumn_harvest::diesel_async::SimpleAsyncConnection as _;

    let mut conn = dr_connect(dsn).await?;
    conn.batch_execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
        .await
        .map_err(|e| {
            CliError::InvalidInput(format!(
                "could not pin {} read-only: {e}",
                autumn_harvest::backup_verify::redact_dsn(dsn)
            ))
        })?;
    Ok(conn)
}

// ── `harvest partition` (issue #958) ───────────────────────────────────────

/// What `harvest partition disable` did on one shard.
///
/// A named enum rather than `Option<Option<_>>`: "already unpartitioned" is a
/// successful no-op, not an absent result, and the two must not be spelled the
/// same way in the JSON an operator's script reads.
#[derive(Debug)]
enum DisableOutcome {
    /// Reverted, discarding the rows the flat layout's constraints forbid.
    Reverted(autumn_harvest::partition::DisableReport),
    /// Already unpartitioned; nothing to do.
    AlreadyUnpartitioned,
}

impl DisableOutcome {
    fn to_json(&self) -> Value {
        match self {
            Self::Reverted(r) => serde_json::to_value(r).unwrap_or(Value::Null),
            Self::AlreadyUnpartitioned => json!({"already_unpartitioned": true}),
        }
    }
}

/// One shard's row in a `harvest partition` report.
///
/// A plain struct with a hand-written JSON projection rather than a
/// `serde::Serialize` derive: this crate depends on `serde_json` but
/// deliberately not on `serde` itself (see `DrShardStatus`), and one small
/// projection is a better trade than a new dependency. The engine types it
/// embeds do derive `Serialize`, so `serde_json::to_value` handles them.
#[derive(Debug)]
struct PartitionShardReport {
    shard_id: i32,
    dsn: String,
    reachable: bool,
    unreachable_reason: Option<String>,
    layout: Option<autumn_harvest::partition::EventLayout>,
    partitions: Option<Vec<autumn_harvest::partition::PartitionInfo>>,
    maintenance: Option<autumn_harvest::partition::MaintenanceOutcome>,
    enable: Option<autumn_harvest::partition::EnableReport>,
    /// What a sweep WOULD do right now — read-only, from `status`.
    would_sweep: Option<autumn_harvest::partition::SweepOutcome>,
    /// Outcome of `disable`, when that is the verb.
    disable: Option<DisableOutcome>,
    error: Option<String>,
}

impl PartitionShardReport {
    const fn unreachable(shard_id: i32, dsn: String, reason: String) -> Self {
        Self {
            shard_id,
            dsn,
            reachable: false,
            unreachable_reason: Some(reason),
            layout: None,
            partitions: None,
            maintenance: None,
            enable: None,
            would_sweep: None,
            disable: None,
            error: None,
        }
    }

    const fn reachable(shard_id: i32, dsn: String) -> Self {
        Self {
            shard_id,
            dsn,
            reachable: true,
            unreachable_reason: None,
            layout: None,
            partitions: None,
            maintenance: None,
            enable: None,
            would_sweep: None,
            disable: None,
            error: None,
        }
    }

    fn to_json(&self) -> Value {
        let mut obj = Map::new();
        obj.insert("shard_id".into(), json!(self.shard_id));
        obj.insert("dsn".into(), json!(self.dsn));
        obj.insert("reachable".into(), json!(self.reachable));
        let mut put = |key: &str, value: Option<Value>| {
            if let Some(v) = value {
                obj.insert(key.to_string(), v);
            }
        };
        put(
            "unreachable_reason",
            self.unreachable_reason.as_ref().map(|v| json!(v)),
        );
        put(
            "layout",
            self.layout.and_then(|v| serde_json::to_value(v).ok()),
        );
        put(
            "partitions",
            self.partitions
                .as_ref()
                .and_then(|v| serde_json::to_value(v).ok()),
        );
        put(
            "maintenance",
            self.maintenance
                .as_ref()
                .and_then(|v| serde_json::to_value(v).ok()),
        );
        put(
            "enable",
            self.enable
                .as_ref()
                .and_then(|v| serde_json::to_value(v).ok()),
        );
        put(
            "would_sweep",
            self.would_sweep
                .as_ref()
                .and_then(|v| serde_json::to_value(v).ok()),
        );
        put(
            "disable",
            self.disable.as_ref().map(DisableOutcome::to_json),
        );
        put("error", self.error.as_ref().map(|v| json!(v)));
        Value::Object(obj)
    }
}

/// Dispatch for `harvest partition`.
///
/// # Errors
///
/// [`CliError::InvalidInput`] for a malformed `--shard` spec or a missing
/// confirmation flag on a destructive subcommand.
pub async fn run_partition(command: &PartitionCommand) -> Result<(), CliError> {
    match command {
        PartitionCommand::Plan {
            cohort_width_secs,
            lookahead_cohorts,
        } => {
            let opts = autumn_harvest::partition::EnableOptions {
                cohort_width_secs: *cohort_width_secs,
                lookahead_cohorts: *lookahead_cohorts,
                ..autumn_harvest::partition::EnableOptions::default()
            };
            opts.validate()
                .map_err(|e| CliError::InvalidInput(e.to_string()))?;
            println!(
                "{}",
                autumn_harvest::partition::migration_plan(
                    &opts,
                    autumn_harvest::chrono::Utc::now()
                )
            );
            Ok(())
        }
        PartitionCommand::Status { shards, format } => run_partition_status(shards, *format).await,
        PartitionCommand::Enable {
            shards,
            cohort_width_secs,
            lookahead_cohorts,
            lock_timeout_secs,
            confirm,
            allow_incompatible_publications,
            format,
        } => {
            if !confirm {
                return Err(CliError::InvalidInput(
                    "refusing to convert without --i-understand-the-lock-window: \
                     `enable` takes a brief ACCESS EXCLUSIVE lock on harvest_events, \
                     during which every append waits. On a populated table that window \
                     also covers two index builds and a full-table constraint validation \
                     — run `harvest partition plan` instead and follow its steps."
                        .to_string(),
                ));
            }
            let opts = autumn_harvest::partition::EnableOptions {
                cohort_width_secs: *cohort_width_secs,
                lookahead_cohorts: *lookahead_cohorts,
                lock_timeout: std::time::Duration::from_secs((*lock_timeout_secs).max(1)),
                allow_incompatible_publications: *allow_incompatible_publications,
            };
            opts.validate()
                .map_err(|e| CliError::InvalidInput(e.to_string()))?;
            run_partition_enable(shards, &opts, *format).await
        }
        PartitionCommand::Maintain {
            shards,
            lookahead_cohorts,
            max_drops,
            format,
        } => run_partition_maintain(shards, *lookahead_cohorts, *max_drops, *format).await,
        PartitionCommand::Disable {
            shards,
            confirm,
            format,
        } => {
            if !confirm {
                return Err(CliError::InvalidInput(
                    "refusing to revert without --i-understand-this-rewrites-the-table: \
                     `disable` copies every surviving event row back into a plain table, \
                     which rewrites harvest_events in full."
                        .to_string(),
                ));
            }
            run_partition_disable(shards, *format).await
        }
    }
}

async fn run_partition_status(shards: &[String], format: DrFormat) -> Result<(), CliError> {
    let targets = parse_shard_targets(shards)?;
    let mut out = Vec::with_capacity(targets.len());
    for target in &targets {
        let redacted = autumn_harvest::backup_verify::redact_dsn(&target.dsn);
        // Read-only, like `dr status`, and enforced by Postgres rather than by
        // care: `status` is documented as safe against a production primary.
        let mut conn = match dr_connect_read_only(&target.dsn).await {
            Ok(conn) => conn,
            Err(error) => {
                out.push(PartitionShardReport::unreachable(
                    target.shard_id,
                    redacted,
                    error.to_string(),
                ));
                continue;
            }
        };
        let mut row = PartitionShardReport::reachable(target.shard_id, redacted);
        match autumn_harvest::partition::detect_layout(&mut conn).await {
            Ok(layout) => {
                row.layout = Some(layout);
                if layout.is_partitioned() {
                    match autumn_harvest::partition::list_partitions(&mut conn).await {
                        Ok(parts) => row.partitions = Some(parts),
                        Err(e) => row.error = Some(e.to_string()),
                    }
                    // The `blocked` reasons are produced by the sweep's gate,
                    // so a status command that only listed partitions could
                    // never answer the question this command exists for. This
                    // is a read-only evaluation of the same gate: no lock, no
                    // DDL, no straggler delete.
                    match autumn_harvest::partition::evaluate(
                        &mut conn,
                        autumn_harvest::chrono::Utc::now(),
                        &autumn_harvest::partition::SweepOptions::default(),
                    )
                    .await
                    {
                        Ok(sweep) => row.would_sweep = Some(sweep),
                        Err(e) => row.error = Some(e.to_string()),
                    }
                }
            }
            Err(e) => row.error = Some(e.to_string()),
        }
        out.push(row);
    }
    emit_partition_report(&out, format, "status")
}

async fn run_partition_enable(
    shards: &[String],
    opts: &autumn_harvest::partition::EnableOptions,
    format: DrFormat,
) -> Result<(), CliError> {
    let targets = parse_shard_targets(shards)?;
    let mut out = Vec::with_capacity(targets.len());
    for target in &targets {
        let redacted = autumn_harvest::backup_verify::redact_dsn(&target.dsn);
        let mut conn = match dr_connect(&target.dsn).await {
            Ok(conn) => conn,
            Err(error) => {
                out.push(PartitionShardReport::unreachable(
                    target.shard_id,
                    redacted,
                    error.to_string(),
                ));
                continue;
            }
        };
        let mut row = PartitionShardReport::reachable(target.shard_id, redacted);
        // Per-shard independence is deliberate: a shard is a database, and a
        // half-converted cluster is a supported state (each shard's layout is
        // detected at runtime), so one shard's lock timeout must not abort the
        // conversion of the rest.
        match autumn_harvest::partition::enable_partitioning(&mut conn, opts).await {
            Ok(report) => row.enable = Some(report),
            Err(e) => row.error = Some(e.to_string()),
        }
        out.push(row);
    }
    emit_partition_report(&out, format, "enable")
}

async fn run_partition_maintain(
    shards: &[String],
    lookahead_cohorts: u32,
    max_drops: usize,
    format: DrFormat,
) -> Result<(), CliError> {
    let targets = parse_shard_targets(shards)?;
    let sweep = autumn_harvest::partition::SweepOptions {
        max_drops,
        ..autumn_harvest::partition::SweepOptions::default()
    };
    let mut out = Vec::with_capacity(targets.len());
    for target in &targets {
        let redacted = autumn_harvest::backup_verify::redact_dsn(&target.dsn);
        let mut conn = match dr_connect(&target.dsn).await {
            Ok(conn) => conn,
            Err(error) => {
                out.push(PartitionShardReport::unreachable(
                    target.shard_id,
                    redacted,
                    error.to_string(),
                ));
                continue;
            }
        };
        let mut row = PartitionShardReport::reachable(target.shard_id, redacted);
        match autumn_harvest::partition::maintain(
            &mut conn,
            autumn_harvest::chrono::Utc::now(),
            lookahead_cohorts,
            &sweep,
        )
        .await
        {
            Ok(outcome) => {
                // A pass that ran but did not COMPLETE — a `drain_default` that
                // lost its bounded lock attempt, say — comes back as `Ok` with
                // `last_error` set, because maintenance is best-effort and must
                // never fail a retention tick. The CLI is not a retention tick.
                // Without this, `harvest partition maintain` would print an
                // ordinary zero-drain report and exit 0 while the DEFAULT
                // partition stayed undrained, and scheduled operator automation
                // would never notice.
                row.error.clone_from(&outcome.last_error);
                row.maintenance = Some(outcome);
            }
            Err(e) => row.error = Some(e.to_string()),
        }
        out.push(row);
    }
    emit_partition_report(&out, format, "maintain")
}

async fn run_partition_disable(shards: &[String], format: DrFormat) -> Result<(), CliError> {
    let targets = parse_shard_targets(shards)?;
    let mut out = Vec::with_capacity(targets.len());
    for target in &targets {
        let redacted = autumn_harvest::backup_verify::redact_dsn(&target.dsn);
        let mut conn = match dr_connect(&target.dsn).await {
            Ok(conn) => conn,
            Err(error) => {
                out.push(PartitionShardReport::unreachable(
                    target.shard_id,
                    redacted,
                    error.to_string(),
                ));
                continue;
            }
        };
        let mut row = PartitionShardReport::reachable(target.shard_id, redacted);
        match autumn_harvest::partition::disable_partitioning(&mut conn).await {
            Ok(report) => {
                row.layout = Some(autumn_harvest::partition::EventLayout::Unpartitioned);
                // `None` = already unpartitioned. That is a successful no-op,
                // not a failure: `enable` on an already-partitioned shard exits
                // 0, and a deployment script must be able to run `disable`
                // twice without the second run failing.
                row.disable = Some(report.map_or(
                    DisableOutcome::AlreadyUnpartitioned,
                    DisableOutcome::Reverted,
                ));
            }
            Err(e) => row.error = Some(e.to_string()),
        }
        out.push(row);
    }
    emit_partition_report(&out, format, "disable")
}

/// Render a report, and fail the process if any shard errored.
///
/// The nonzero exit is the point: `harvest partition enable --shard a --shard b`
/// that converted `a` and failed on `b` has left a half-converted cluster, and a
/// zero exit would let a deployment script move on as though it had not.
fn emit_partition_report(
    rows: &[PartitionShardReport],
    format: DrFormat,
    verb: &str,
) -> Result<(), CliError> {
    // `status` is read-only reporting and must not fail the process for an
    // unreachable shard — that is the established `harvest dr status`
    // behaviour, and a monitoring script should get the report for the shards
    // it could reach. Only the MUTATING verbs exit nonzero, where a partial
    // result means a half-converted cluster a deployment script must not move
    // on from.
    let fail_on_error = verb != "status";
    match format {
        DrFormat::Json => {
            let payload: Vec<Value> = rows.iter().map(PartitionShardReport::to_json).collect();
            println!(
                "{}",
                serde_json::to_string_pretty(&payload)
                    .map_err(|e| CliError::InvalidInput(e.to_string()))?
            );
        }
        DrFormat::Text => {
            for r in rows {
                println!("shard {} ({})", r.shard_id, r.dsn);
                if !r.reachable {
                    println!(
                        "  UNREACHABLE: {}",
                        r.unreachable_reason.as_deref().unwrap_or("unknown")
                    );
                    continue;
                }
                if let Some(layout) = &r.layout {
                    println!("  layout: {layout:?}");
                }
                if let Some(report) = &r.enable {
                    println!("  mode: {:?}", report.mode);
                    println!(
                        "  cohort width: {}s, partitions created: {}",
                        report.cohort_width_secs,
                        report.partitions_created.len()
                    );
                }
                if let Some(parts) = &r.partitions {
                    println!("  partitions: {}", parts.len());
                    for p in parts {
                        println!(
                            "    {:<40} {} .. {}",
                            p.name,
                            p.lower
                                .map_or_else(|| "MINVALUE".into(), |t| t.to_rfc3339()),
                            p.upper
                                .map_or_else(|| "MAXVALUE".into(), |t| t.to_rfc3339()),
                        );
                    }
                }
                if let Some(m) = &r.maintenance {
                    println!(
                        "  created: {}, drained rows: {}, dropped: {}, straggler rows: {}",
                        m.created.len(),
                        m.drained,
                        m.sweep.dropped.len(),
                        m.sweep.straggler_rows_deleted,
                    );
                    if let Some(e) = &m.last_error {
                        println!("  INCOMPLETE: {e}");
                    }
                    // The answer to "why has space not come back?". Printed
                    // even when empty is noise, so only when there is
                    // something to explain.
                    for b in &m.sweep.blocked {
                        println!("  blocked: {b}");
                    }
                }
                if let Some(sweep) = &r.would_sweep {
                    println!(
                        "  droppable now: {}, blocked: {}",
                        sweep.dropped.len(),
                        sweep.blocked.len()
                    );
                    for b in &sweep.blocked {
                        println!("  blocked: {b}");
                    }
                }
                match &r.disable {
                    Some(DisableOutcome::Reverted(d)) => println!(
                        "  reverted: {} orphan row(s) and {} duplicate row(s) discarded \
                         to rebuild the flat layout's constraints",
                        d.orphans_removed, d.duplicates_removed
                    ),
                    Some(DisableOutcome::AlreadyUnpartitioned) => {
                        println!("  already unpartitioned; nothing to do");
                    }
                    None => {}
                }
                if let Some(e) = &r.error {
                    println!("  ERROR: {e}");
                }
            }
        }
    }
    let failed: Vec<String> = rows
        .iter()
        .filter(|r| !r.reachable || r.error.is_some())
        .map(|r| r.shard_id.to_string())
        .collect();
    if failed.is_empty() || !fail_on_error {
        Ok(())
    } else {
        Err(CliError::InvalidInput(format!(
            "`partition {verb}` did not succeed on shard(s) {}: see the report above",
            failed.join(", ")
        )))
    }
}

/// Run a `harvest dr` subcommand.
///
/// # Errors
///
/// Returns [`CliError::InvalidInput`] for an unparseable or unreachable shard
/// target, and propagates database errors from the fence and promote paths.
/// `status` never fails on an unreachable shard: it reports the shard as
/// unreachable and carries on, because during an incident a partial answer
/// about the shards you *can* reach is the answer you need.
pub async fn run_dr(command: &DrCommand) -> Result<(), CliError> {
    match command {
        DrCommand::Status {
            shards,
            slot_prefix,
            format,
        } => run_dr_status(shards, slot_prefix, *format).await,
        DrCommand::Fence {
            shards,
            reason,
            actor,
            confirm,
            provision,
            force,
            format,
        } => run_dr_fence(shards, reason, actor, *confirm, *provision, *force, *format).await,
        DrCommand::Promote { shards, format } => run_dr_promote(shards, *format).await,
    }
}

async fn run_dr_status(
    shards: &[String],
    slot_prefix: &str,
    format: DrFormat,
) -> Result<(), CliError> {
    use autumn_harvest::replication::{current_generation, query_replication_status};
    use autumn_harvest::types::ShardId;

    let targets = parse_shard_targets(shards)?;
    let mut out = Vec::with_capacity(targets.len());
    for target in &targets {
        let shard = ShardId::new(target.shard_id);
        let redacted = autumn_harvest::backup_verify::redact_dsn(&target.dsn);
        let mut conn = match dr_connect_read_only(&target.dsn).await {
            Ok(conn) => conn,
            Err(error) => {
                out.push(DrShardStatus {
                    shard_id: target.shard_id,
                    dsn: redacted,
                    reachable: false,
                    unreachable_reason: Some(error.to_string()),
                    generation: None,
                    generation_error: None,
                    rpo_seconds: None,
                    rpo_is_lower_bound: false,
                    lag_bytes: None,
                    connected_standbys: None,
                    inactive_slots: None,
                    replication_error: None,
                });
                continue;
            }
        };
        let (generation, generation_error) = match current_generation(&mut conn, shard).await {
            Ok(Some(g)) => (Some(g.as_i64().to_string()), None),
            Ok(None) => (None, None),
            Err(error) => (None, Some(error.to_string())),
        };
        let status = query_replication_status(&mut conn, shard, slot_prefix)
            .await
            .ok();
        out.push(DrShardStatus {
            shard_id: target.shard_id,
            dsn: redacted,
            reachable: true,
            unreachable_reason: None,
            generation,
            generation_error,
            rpo_seconds: status
                .as_ref()
                .and_then(autumn_harvest::replication::ReplicationStatus::rpo_seconds),
            rpo_is_lower_bound: status
                .as_ref()
                .is_some_and(autumn_harvest::replication::ReplicationStatus::rpo_is_lower_bound),
            lag_bytes: status
                .as_ref()
                .and_then(autumn_harvest::replication::ReplicationStatus::max_lag_bytes),
            // `Unavailable` is carried through as `None` rather than counted as
            // zero standbys: "we cannot see" and "there is nothing there" are
            // opposite answers to a failover decision.
            connected_standbys: status.as_ref().and_then(|st| {
                matches!(
                    st,
                    autumn_harvest::replication::ReplicationStatus::Observed { .. }
                )
                .then(|| st.connected_standbys())
            }),
            inactive_slots: status.as_ref().and_then(|st| {
                matches!(
                    st,
                    autumn_harvest::replication::ReplicationStatus::Observed { .. }
                )
                .then(|| st.inactive_slots())
            }),
            // `ReplicationStatus` is `#[non_exhaustive]`, so this matches the
            // two variants it cares about and treats anything else as
            // observable — a future variant must opt in to being reported as an
            // outage rather than inherit it.
            replication_error: match status.as_ref() {
                Some(autumn_harvest::replication::ReplicationStatus::Unavailable { reason }) => {
                    Some(reason.clone())
                }
                None => Some("replication views could not be queried".to_string()),
                Some(_) => None,
            },
        });
    }

    match format {
        DrFormat::Json => {
            let payload: Vec<serde_json::Value> = out.iter().map(DrShardStatus::to_json).collect();
            println!(
                "{}",
                serde_json::to_string_pretty(&payload)
                    .map_err(|e| CliError::InvalidInput(e.to_string()))?
            );
        }
        DrFormat::Text => print!("{}", format_dr_status_text(&out)),
    }
    Ok(())
}

/// Render `harvest dr status` as a table.
///
/// Pure, so the "unknown is not zero" rendering is testable without a database.
/// An unknown RPO prints `unknown`, never `0.0s`: a dead standby that reads as
/// a perfect RPO is the most dangerous line this table could print.
fn format_dr_status_text(rows: &[DrShardStatus]) -> String {
    use std::fmt::Write as _;

    let mut s = String::new();
    let _ = writeln!(
        s,
        "{:<6} {:<11} {:>10} {:>12} {:>10} {:>10}  DSN",
        "SHARD", "GENERATION", "RPO", "LAG BYTES", "STANDBYS", "IDLE SLOTS",
    );
    for r in rows {
        if !r.reachable {
            let _ = writeln!(
                s,
                "{:<6} {:<11} {:>10} {:>12} {:>10} {:>8}  {}",
                r.shard_id, "UNREACHABLE", "-", "-", "-", "-", r.dsn
            );
            continue;
        }
        let generation = r.generation.clone().unwrap_or_else(|| {
            if r.generation_error.is_some() {
                // NOT "unfenced": an unreadable row is not an absent one, and
                // the difference decides whether an operator re-fences.
                "unknown".to_string()
            } else {
                "unfenced".to_string()
            }
        });
        let rpo = r.rpo_seconds.map_or_else(
            || "unknown".to_string(),
            |v| {
                if r.rpo_is_lower_bound {
                    // The standby is behind the whole retained trail, so this
                    // is a floor. Printing it bare would read as a measurement.
                    format!(">={v:.0}s")
                } else {
                    format!("{v:.1}s")
                }
            },
        );
        let bytes = r
            .lag_bytes
            .map_or_else(|| "unknown".to_string(), |v| v.to_string());
        // `unreadable`, never `0`: `0` is the definitive "there is no standby
        // here", and printing it for a role that simply cannot read
        // `pg_stat_replication` tells an operator mid-failover that DR is down
        // when the truth is that it is unobservable.
        let standbys = r
            .connected_standbys
            .map_or_else(|| "unreadable".to_string(), |n| n.to_string());
        let slots = r
            .inactive_slots
            .map_or_else(|| "-".to_string(), |n| n.to_string());
        let _ = writeln!(
            s,
            "{:<6} {:<11} {:>10} {:>12} {:>10} {:>8}  {}",
            r.shard_id, generation, rpo, bytes, standbys, slots, r.dsn
        );
    }
    if rows
        .iter()
        .any(|r| r.reachable && r.connected_standbys == Some(0))
    {
        let _ = writeln!(
            s,
            "\nWARNING: a shard has no connected standby. Its RPO is unbounded and growing; \
             `unknown` above means UNMEASURABLE, not zero."
        );
    }
    // Deliberately a separate, differently-worded line from the warning above.
    if rows
        .iter()
        .any(|r| r.reachable && r.replication_error.is_some())
    {
        let _ = writeln!(
            s,
            "\nNOTE: a shard's replication views could not be read (usually a missing \
             `GRANT pg_monitor`), so its standby count and RPO are UNKNOWN — not zero, and not \
             evidence that replication is down."
        );
    }
    s
}

/// Phase 1 of `harvest dr fence`: validate every shard, mutate nothing.
///
/// A fence that dies partway leaves a HALF-FENCED cluster — the state the
/// runbook names as the worst possible one, because live cross-shard traffic
/// then turns bounded skew into unbounded skew — and during a regional failover
/// an unreachable shard is the *expected* condition, not the exceptional one.
/// So every failure that can be found without writing is found here, before
/// anything is written.
async fn dr_fence_preflight(
    targets: &[autumn_harvest::backup_verify::ShardTarget],
    provision: bool,
    force: bool,
) -> Result<
    Vec<(
        i32,
        autumn_harvest::types::ShardId,
        autumn_harvest::diesel_async::AsyncPgConnection,
    )>,
    CliError,
> {
    use autumn_harvest::replication::{current_generation, ensure_generation_row};
    use autumn_harvest::types::ShardId;

    // without writing is found before anything is written.
    let mut conns = Vec::with_capacity(targets.len());
    for target in targets {
        let shard = ShardId::new(target.shard_id);
        let mut conn = dr_connect(&target.dsn).await?;
        let posture = dr_write_posture(&mut conn).await?;
        if posture.looks_like_a_live_primary() && !force {
            return Err(CliError::InvalidInput(format!(
                "refusing to fence shard {}: {} is not in recovery and still has {} connected \
                 standby(s), so it looks like a healthy PRIMARY rather than the standby you just \
                 promoted. Fencing it stops every worker on it, recoverable only by restarting \
                 the fleet. Pass --force if this really is the promoted primary.",
                target.shard_id,
                autumn_harvest::backup_verify::redact_dsn(&target.dsn),
                posture.connected_standbys,
            )));
        }
        if provision {
            ensure_generation_row(&mut conn, shard)
                .await
                .map_err(|e| dr_error(target.shard_id, &e))?;
        } else if current_generation(&mut conn, shard)
            .await
            .map_err(|e| dr_error(target.shard_id, &e))?
            .is_none()
        {
            // Preflight the row's existence here rather than letting
            // `bump_generation` return NotFound in phase 2. A missing row is a
            // fully discoverable input error — almost always a wrong shard id,
            // since an unprefixed `--shard <dsn>` takes its POSITIONAL index —
            // and discovering it after earlier shards were already bumped
            // produces precisely the half-fenced cluster this two-phase
            // structure exists to prevent.
            return Err(CliError::InvalidInput(format!(
                "shard {} has no harvest_shard_generation row at {}, so there is nothing to \
                 fence: no worker has ever pinned it. This is usually a wrong shard id — an \
                 unprefixed `--shard <dsn>` takes its positional index as the shard id, so \
                 prefix explicitly as `<id>=<dsn>`. Pass --provision to create the row and fence \
                 it anyway. No shard has been fenced.",
                target.shard_id,
                autumn_harvest::backup_verify::redact_dsn(&target.dsn),
            )));
        }
        conns.push((target.shard_id, shard, conn));
    }

    Ok(conns)
}

#[allow(clippy::too_many_arguments)]
async fn run_dr_fence(
    shards: &[String],
    reason: &str,
    actor: &str,
    confirm: bool,
    provision: bool,
    force: bool,
    format: DrFormat,
) -> Result<(), CliError> {
    use autumn_harvest::replication::bump_generation;

    // clap marks the flag `required`, so this is belt-and-braces for a
    // programmatic caller — but the cost of getting it wrong is a fleet.
    if !confirm {
        return Err(CliError::InvalidInput(
            "refusing to fence without --i-understand-this-stops-the-fleet".to_string(),
        ));
    }

    let targets = parse_shard_targets(shards)?;

    let mut conns = dr_fence_preflight(&targets, provision, force).await?;

    // ── Phase 2: bump. `bump_generation` returns NotFound for a shard with no
    // row, which is deliberately NOT papered over with a provisioning call —
    // see `--provision`.
    let mut results = Vec::with_capacity(conns.len());
    let mut failure: Option<CliError> = None;
    for (shard_id, shard, conn) in &mut conns {
        match bump_generation(conn, *shard, reason, actor).await {
            Ok(generation) => results.push(serde_json::json!({
                "shard_id": *shard_id,
                "generation": generation.as_i64(),
                "reason": reason,
                "actor": actor,
            })),
            Err(error) => {
                failure = Some(dr_error(*shard_id, &error));
                break;
            }
        }
    }

    // Report what DID happen before returning any error. An operator who is
    // told only "shard 3 failed" cannot know that shards 0-2 are already
    // fenced, and that is exactly the fact they need in order to decide what to
    // do next. On the JSON path this goes to stderr so stdout stays parseable.
    let rendered = match format {
        DrFormat::Json => serde_json::to_string_pretty(&results)
            .map_err(|e| CliError::InvalidInput(e.to_string()))?,
        DrFormat::Text => {
            let mut out = String::new();
            for r in &results {
                let _ = std::fmt::Write::write_fmt(
                    &mut out,
                    format_args!(
                        "shard {} fenced at generation {}\n",
                        r["shard_id"], r["generation"]
                    ),
                );
            }
            out
        }
    };
    if let Some(error) = failure {
        eprintln!(
            "PARTIAL FENCE — {} of {} shard(s) were fenced before the failure below. The cluster \
             is now HALF-FENCED: do not start workers until every shard is fenced.\n{rendered}",
            results.len(),
            targets.len()
        );
        return Err(error);
    }

    match format {
        DrFormat::Json => println!("{rendered}"),
        DrFormat::Text => {
            print!("{rendered}");
            println!(
                "\nEvery worker pinned to the previous generation is now unable to claim or \
                 persist and will stop. Restart the fleet against this region; never re-pin a \
                 fenced worker in place."
            );
        }
    }
    Ok(())
}

/// What a target database looks like from a write-authority standpoint.
struct DrWritePosture {
    in_recovery: bool,
    connected_standbys: i64,
}

impl DrWritePosture {
    /// A database that is writable *and* still serving standbys is a primary
    /// doing its job — not the standby an operator just promoted.
    const fn looks_like_a_live_primary(&self) -> bool {
        !self.in_recovery && self.connected_standbys > 0
    }
}

async fn dr_write_posture(
    conn: &mut autumn_harvest::diesel_async::AsyncPgConnection,
) -> Result<DrWritePosture, CliError> {
    // `QueryableByName`'s generated code refers to `diesel` by that bare path,
    // so the re-export has to be in scope under that name.
    use autumn_harvest::diesel;
    use autumn_harvest::diesel_async::RunQueryDsl as _;

    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        in_recovery: bool,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        standbys: i64,
    }
    let rows: Vec<Row> = diesel::sql_query(
        "SELECT pg_is_in_recovery() AS in_recovery, \
                (SELECT COUNT(*) FROM pg_stat_replication)::bigint AS standbys",
    )
    .load(conn)
    .await
    .map_err(|e| CliError::InvalidInput(format!("could not read write posture: {e}")))?;

    // An unreadable `pg_stat_replication` (no `pg_monitor`) must not block a
    // failover: degrade to "cannot tell", which lets the fence proceed. The
    // guard is a typo catcher, not a security boundary.
    Ok(rows.into_iter().next().map_or(
        DrWritePosture {
            in_recovery: false,
            connected_standbys: 0,
        },
        |r| DrWritePosture {
            in_recovery: r.in_recovery,
            connected_standbys: r.standbys,
        },
    ))
}

/// Wrap a database error with the shard it came from.
///
/// During a multi-shard failover "which shard" is the single missing detail in
/// an error an operator reads under time pressure.
fn dr_error(shard_id: i32, error: &autumn_harvest::error::HarvestError) -> CliError {
    CliError::InvalidInput(format!("shard {shard_id}: {error}"))
}

async fn run_dr_promote(shards: &[String], format: DrFormat) -> Result<(), CliError> {
    use autumn_harvest::replication::advance_sequences_after_promotion;

    let targets = parse_shard_targets(shards)?;
    let mut results = Vec::with_capacity(targets.len());
    for target in &targets {
        let mut conn = dr_connect(&target.dsn).await?;
        let advanced = advance_sequences_after_promotion(&mut conn)
            .await
            .map_err(|e| CliError::InvalidInput(e.to_string()))?;
        results.push(serde_json::json!({
            "shard_id": target.shard_id,
            "sequences_advanced": advanced
                .iter()
                .map(|(name, value)| serde_json::json!({ "sequence": name, "set_to": value }))
                .collect::<Vec<_>>(),
        }));
    }

    match format {
        DrFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(&results)
                .map_err(|e| CliError::InvalidInput(e.to_string()))?
        ),
        DrFormat::Text => {
            for r in &results {
                let seqs = r["sequences_advanced"].as_array().map_or(0, Vec::len);
                println!("shard {}: {seqs} sequence(s) advanced", r["shard_id"]);
            }
        }
    }
    Ok(())
}

// ── harvest schema: payload-schema contract gate (issue #794) ───────────────

/// Reads and parses a schema-contract document, naming the path on failure.
fn read_schema_contract(path: &Path) -> Result<WorkflowSchemaContract, CliError> {
    let raw = std::fs::read_to_string(path).map_err(|e| {
        CliError::InvalidInput(format!(
            "failed to read schema contract `{}`: {e}",
            path.display()
        ))
    })?;
    WorkflowSchemaContract::parse(&raw)
        .map_err(|e| CliError::InvalidInput(format!("`{}` is not usable: {e}", path.display())))
}

/// Renders a schema diff as `workflow.role: <field> — <verdict> — <reason>`
/// lines, one per delta, plus a summary line.
#[must_use]
pub fn format_schema_diff_text(diff: &SchemaContractDiff) -> String {
    use std::fmt::Write as _;

    let mut out = String::new();
    for d in &diff.deltas {
        let role = d.role.map_or("*", SchemaRole::as_str);
        let field = if d.field_path.is_empty() {
            "(root)"
        } else {
            d.field_path.as_str()
        };
        // Writing into a String is infallible.
        let _ = writeln!(
            out,
            "{}.{}: {} — {} — {}",
            d.workflow,
            role,
            field,
            d.verdict.as_str(),
            d.reason
        );
    }

    // `breaking_count` rather than a count of the stored deltas: the tally
    // keeps counting past the storage cap, so a truncated report still states
    // the true number.
    let breaking = diff.breaking_count;
    let total = diff.breaking_count + diff.compatible_count;
    if breaking == 0 {
        let _ = write!(
            out,
            "schema check: no breaking changes ({} compatible delta(s))",
            diff.compatible_count
        );
    } else {
        let _ = write!(
            out,
            "schema check: {breaking} breaking change(s) of {total} delta(s)"
        );
    }
    if diff.truncated {
        let _ = write!(
            out,
            "\nschema check: report TRUNCATED at {} delta(s) — the listing above is incomplete",
            diff.deltas.len()
        );
    }
    out
}

/// Serialises a schema diff as machine-readable JSON for CI.
///
/// # Errors
/// Returns [`CliError::SerializeResponse`] if serialisation fails. The input
/// parsed fine — nothing about it was invalid — so this is an output failure,
/// matching `det_check_json`.
pub fn schema_diff_json(diff: &SchemaContractDiff) -> Result<String, CliError> {
    serde_json::to_string_pretty(diff).map_err(CliError::SerializeResponse)
}

/// Runs `harvest schema check`: diffs `current` against `baseline`, prints the
/// result, and gates the exit code.
///
/// # Errors
/// Returns [`CliError::SchemaContractBreaking`] when any delta is breaking (the
/// diff is already on stdout), or an [`CliError::InvalidInput`] read/parse
/// error naming the offending path.
///
/// A **truncated** diff also fails: once the delta cap is hit the report no
/// longer enumerates every difference, so passing it would be a claim the tool
/// cannot support. Failing closed keeps "exit `0`" meaning "nothing breaking",
/// never "nothing breaking *that fit*".
pub fn run_schema_check(
    baseline: &Path,
    current: &Path,
    format: SchemaCheckFormat,
    require_current: bool,
    acknowledged_in: Option<&Path>,
) -> Result<(), CliError> {
    let base = read_schema_contract(baseline)?;
    let cur = read_schema_contract(current)?;
    guard_empty_current(&base, &cur, current)?;
    let diff = diff_schema_contracts(&base, &cur);

    match format {
        SchemaCheckFormat::Text => println!("{}", format_schema_diff_text(&diff)),
        SchemaCheckFormat::Json => println!("{}", schema_diff_json(&diff)?),
    }

    if diff.truncated {
        return Err(CliError::InvalidInput(format!(
            "schema diff truncated at {} delta(s): the report is incomplete, so it \
             cannot certify that nothing is breaking. Split the change into smaller \
             steps, or regenerate the baseline with `harvest schema update`.",
            diff.deltas.len()
        )));
    }

    // Escape-hatch mode: a breaking delta is allowed, but only when THIS
    // revision of the artifact records why. Without it the tool's refusal to
    // absorb a break can be sidestepped by hand-editing the artifact, which
    // leaves the ordinary check clean and the change unrecorded.
    if let Some(ack_path) = acknowledged_in {
        let head = read_schema_contract(ack_path)?;
        // An INDEPENDENT check, not a precondition of the coverage one: a
        // retargeted record covers the delta, so coverage passes and only this
        // notices the rewrite. Reported first because it is the root cause when
        // both fire, and because the remedy differs — restore the record and
        // append, rather than record this break — hence its own error type.
        // The head artifact is loaded here, so `diff_schema_contracts` — which
        // only ever saw `base` and `current` — never inspected its reasons. The
        // coverage check below already refuses to let a blank record cover
        // anything; reporting it here names the root cause instead of leaving
        // the operator with "nothing acknowledges this" beside a record that
        // plainly exists.
        let blank: Vec<&AcknowledgedBreakingChange> = head
            .acknowledged_breaking_changes
            .iter()
            .filter(|a| a.reason.trim().is_empty())
            .collect();
        if !blank.is_empty() {
            return Err(CliError::InvalidInput(format!(
                "{} acknowledgement record(s) in {} record no justification; an \
                 acknowledgement without a reason is a rubber stamp:\n{}",
                blank.len(),
                ack_path.display(),
                format_dropped(&blank)
            )));
        }
        let dropped = dropped_acknowledgements(&base, &head);
        if !dropped.is_empty() {
            return Err(CliError::SchemaContractAuditLogRewritten {
                dropped: dropped.len(),
                detail: format_dropped(&dropped),
            });
        }
        let missing = unacknowledged_breaking(&diff, &base, &head);
        if !missing.is_empty() {
            return Err(CliError::SchemaContractUnacknowledged {
                missing: missing.len(),
                detail: format_unacknowledged(&missing),
            });
        }
        return Ok(());
    }

    // `breaking_count` is the authoritative tally kept by the differ, not a
    // count of the *stored* deltas: it stays correct even where storage is
    // capped, so a breaking change can never be dropped on the floor.
    if diff.has_breaking() {
        return Err(CliError::SchemaContractBreaking {
            breaking: diff.breaking_count,
        });
    }

    // Regenerated METADATA is not compared by the differ, which only ever looks
    // at `workflows` — so a hand-edit to it survives every check above. That
    // matters because `compatibility` is the machine-readable claim about WHICH
    // rules this gate implements: an artifact advertising `pattern` as analysed
    // would promise a guarantee no code provides, and `description` is the same
    // claim in prose. `schema update` regenerates both, so requiring them to
    // match the freshly generated contract costs nothing legitimate.
    //
    // Two siblings are deliberately NOT compared here. `version` records which
    // BUILD produced the artifact rather than what the gate checks, so tying
    // currency to it would force a regeneration on every crate version bump for
    // no safety gain. `contract_version` needs no check at all — `parse` already
    // hard-refuses a value this build does not implement — and `workflows` and
    // `coverage` are rebuilt from the entries on parse.
    //
    // Reported before the delta staleness below because it is the stronger
    // claim: a stale schema means the artifact is behind, while a doctored
    // ruleset means it is lying.
    if require_current {
        let stale: Vec<&str> = [
            ("description", base.description != cur.description),
            ("compatibility", base.compatibility != cur.compatibility),
        ]
        .into_iter()
        .filter_map(|(name, differs)| differs.then_some(name))
        .collect();
        if !stale.is_empty() {
            return Err(CliError::InvalidInput(format!(
                "the checked-in baseline's regenerated metadata does not match this build: {}. \
                 That metadata is the artifact's own description of WHICH rules this gate \
                 implements, so a stale or hand-edited value advertises a guarantee the checker \
                 does not provide. Regenerate it with `harvest schema update`.",
                stale.join(", ")
            )));
        }
    }

    // Checked AFTER the breaking verdict so a break is always reported as a
    // break: staleness is the milder finding, and reporting it first would bury
    // the severe one behind "run schema update".
    if require_current && !diff.deltas.is_empty() {
        return Err(CliError::InvalidInput(format!(
            "the checked-in baseline is not current: {} unabsorbed compatible delta(s). \
             A baseline allowed to lag records what was deployed some time ago while \
             this gate reads it as what was deployed last — so a field added in one \
             release and removed in the next is invisible, even though replay of data \
             written in between fails. Regenerate it with `harvest schema update`.",
            diff.deltas.len()
        )));
    }
    Ok(())
}

/// One line per unrecorded break, in the same shape as the ordinary report.
fn format_unacknowledged(missing: &[&SchemaDelta]) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    for d in missing {
        let role = d
            .role
            .map_or_else(String::new, |r| format!(".{}", r.as_str()));
        let _ = writeln!(
            out,
            "  {}{role}{}: {} — {}",
            d.workflow,
            d.field_path,
            d.change.as_str(),
            d.reason
        );
    }
    out
}

/// One line per vanished record, naming what it used to cover.
fn format_dropped(dropped: &[&AcknowledgedBreakingChange]) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    for a in dropped {
        let role = a
            .role
            .map_or_else(String::new, |r| format!(".{}", r.as_str()));
        let _ = writeln!(
            out,
            "  {}{role}{}: {} — recorded as {:?}",
            a.workflow,
            a.field_path,
            a.change.as_str(),
            a.reason
        );
    }
    out
}

/// Refuses a `--current` that publishes nothing against a non-empty baseline.
///
/// An empty contract is almost always a broken *producer*, not a deliberate
/// mass deletion: a `dump-schema-contract` binary that forgot `.workflows(…)`,
/// a `curl` that captured an error body, or a truncated redirect. Diffed
/// literally it is technically correct — every workflow "removed", every schema
/// "unpublished" — but it buries the real cause under N breaking deltas and
/// invites the author to reach for `--acknowledge`, which would overwrite the
/// baseline with nothing and disarm the gate permanently.
///
/// Deleting every workflow at once is legitimate but vanishingly rare, so this
/// trades that case (regenerate the baseline directly) for a clear diagnosis of
/// the common one.
fn guard_empty_current(
    base: &WorkflowSchemaContract,
    cur: &WorkflowSchemaContract,
    current: &Path,
) -> Result<(), CliError> {
    if cur.workflows.is_empty() && !base.workflows.is_empty() {
        return Err(CliError::InvalidInput(format!(
            "`{}` publishes no workflows at all, but the baseline has {}. This is almost always \
             a broken schema dump (a producer that registered no workflows, or a captured error \
             body) rather than a deliberate deletion of every workflow — refusing to diff it so \
             the real cause is not buried under {} `removed` verdicts. Check the producer, or \
             regenerate the baseline directly if the emptiness is intended.",
            current.display(),
            base.workflows.len(),
            base.workflows.len()
        )));
    }
    Ok(())
}

/// Writes the regenerated baseline without ever leaving a truncated file behind.
///
/// `fs::write` truncates the target *before* writing, so an interrupted run (a
/// full disk, a killed CI step, a `^C`) can destroy a checked-in baseline and
/// leave a half-written one in the working tree — and the next `schema check`
/// would then diff against garbage. Writing a sibling temp file and `rename`ing
/// it over the target makes the replacement atomic on POSIX: the baseline is
/// either the old bytes or the new bytes, never a prefix of the new ones.
///
/// The temp file is a sibling (not `/tmp`) so the rename stays within one
/// filesystem, and is removed on any failure path.
fn write_baseline_atomically(baseline: &Path, contents: &str) -> Result<(), CliError> {
    let wrap = |e: std::io::Error| {
        CliError::InvalidInput(format!(
            "failed to write schema contract `{}`: {e}",
            baseline.display()
        ))
    };

    let dir = baseline.parent().unwrap_or_else(|| Path::new("."));
    let stem = baseline
        .file_name()
        .map_or_else(|| "schema-contract".into(), |n| n.to_string_lossy());
    let tmp = dir.join(format!(".{stem}.tmp-{}", std::process::id()));

    if let Err(e) = fs::write(&tmp, contents) {
        drop(fs::remove_file(&tmp));
        return Err(wrap(e));
    }
    if let Err(e) = fs::rename(&tmp, baseline) {
        drop(fs::remove_file(&tmp));
        return Err(wrap(e));
    }
    Ok(())
}

/// Runs `harvest schema update`: rewrites `baseline` from `current`.
///
/// A breaking delta is refused unless `acknowledge` records why it is safe; the
/// justification is written into the artifact so it is visible in the
/// checked-in diff and never silent.
///
/// # Errors
/// Returns [`CliError::InvalidInput`] when a read, parse, refusal or write
/// fails. Refusal messages name `--acknowledge` explicitly.
pub fn run_schema_update(
    baseline: &Path,
    current: &Path,
    acknowledge: Option<&str>,
    recorded_in: Option<&str>,
) -> Result<(), CliError> {
    let base = read_schema_contract(baseline)?;
    let cur = read_schema_contract(current)?;
    // Refuse here too: `update` is the path that would OVERWRITE the baseline
    // with the empty document and disarm the gate for every later run.
    guard_empty_current(&base, &cur, current)?;
    let diff = diff_schema_contracts(&base, &cur);

    // The core recomputes the diff internally rather than accepting ours: it is
    // a pure function of the same two inputs, so it cannot disagree, and the
    // API cannot be fed a diff computed from *different* contracts — which
    // would write acknowledgement records for deltas that were never inspected.
    let updated = acknowledge
        .map_or_else(
            || base.compatible_update(&cur),
            |reason| base.acknowledged_update(&cur, reason, recorded_in),
        )
        .map_err(|e| CliError::InvalidInput(e.to_string()))?;

    let json = updated
        .to_json_pretty()
        .map_err(|e| CliError::InvalidInput(e.to_string()))?;
    write_baseline_atomically(baseline, &json)?;

    // The true tallies, not `deltas.len()`: the stored listing is capped at
    // `MAX_DELTAS` but the counts keep counting, so a large update still
    // reports honestly.
    let breaking = diff.breaking_count;
    let total = diff.breaking_count + diff.compatible_count;
    println!(
        "schema baseline updated: {total} delta(s) recorded, {breaking} acknowledged as breaking"
    );
    Ok(())
}

// ── harvest new: project scaffolding (issue #692) ───────────────────────────

/// Every identifier the scaffold derives from the project `<name>`.
///
/// All fields are a pure function of `<name>`, so a generated project never
/// contains leftover example identifiers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScaffoldNames {
    /// The Cargo package name (may contain `-`), verbatim `<name>`.
    pub crate_name: String,
    /// A valid Rust identifier derived from `<name>` (`-` → `_`).
    pub ident: String,
    /// The workflow function name, `{ident}_workflow`.
    pub workflow_fn: String,
    /// The activity function name, `{ident}_activity`.
    pub activity_fn: String,
    /// The activity queue name, `{ident}`.
    pub queue: String,
}

/// Rust keywords (strict + reserved) that must not be used as an identifier.
const RUST_KEYWORDS: &[&str] = &[
    "as", "break", "const", "continue", "crate", "dyn", "else", "enum", "extern", "false", "fn",
    "for", "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut", "pub", "ref",
    "return", "self", "Self", "static", "struct", "super", "trait", "true", "type", "unsafe",
    "use", "where", "while", "async", "await", "abstract", "become", "box", "do", "final", "gen",
    "macro", "override", "priv", "typeof", "unsized", "virtual", "yield", "try", "union",
];

/// Cargo/Rust special names that produce confusing or conflicting crates.
const RESERVED_PROJECT_NAMES: &[&str] = &[
    "test",
    "deps",
    "build",
    "core",
    "std",
    "alloc",
    "proc-macro",
    "proc_macro",
    "main",
    "lib",
];

/// The maximum accepted project-name length.
const MAX_PROJECT_NAME_LEN: usize = 64;

/// The embedded `minimal` template: (relative output path, template body).
const MINIMAL_TEMPLATE: &[(&str, &str)] = &[
    (
        "Cargo.toml",
        include_str!("../templates/minimal/Cargo.toml.tmpl"),
    ),
    (
        "src/main.rs",
        include_str!("../templates/minimal/main.rs.tmpl"),
    ),
    (
        "README.md",
        include_str!("../templates/minimal/README.md.tmpl"),
    ),
    (
        "compose.yaml",
        include_str!("../templates/minimal/compose.yaml.tmpl"),
    ),
    (
        "autumn.toml",
        include_str!("../templates/minimal/autumn.toml.tmpl"),
    ),
    (
        ".gitignore",
        include_str!("../templates/minimal/gitignore.tmpl"),
    ),
];

/// Derives a clean, warning-free `snake_case` Rust identifier from a project name.
///
/// Lowercases, maps `-` → `_`, collapses runs of `_`, and trims leading/trailing
/// `_`, so any spec-valid `<name>` (`^[A-Za-z][A-Za-z0-9_-]*$`) yields a valid
/// `snake_case` ident with no `non_snake_case` warnings and no double underscore:
/// `my-app` → `my_app`, `MyApp` → `myapp`, `trail-` → `trail`, `my--app` →
/// `my_app`. A spec-valid name always starts with an ASCII letter, so the result
/// is non-empty and never begins with a digit or `_`.
#[must_use]
pub fn derive_crate_ident(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut prev_underscore = false;
    for c in name.chars() {
        if c == '-' || c == '_' {
            if !prev_underscore {
                out.push('_');
            }
            prev_underscore = true;
        } else {
            out.push(c.to_ascii_lowercase());
            prev_underscore = false;
        }
    }
    out.trim_matches('_').to_string()
}

/// Validates a project name for use as a Cargo package name (from which
/// [`derive_crate_ident`] later derives the scaffold's Rust identifiers).
///
/// # Errors
///
/// Returns [`CliError::InvalidInput`] when the name is empty, too long, not a
/// valid Cargo package name (`^[A-Za-z][A-Za-z0-9_-]*$`), a Rust keyword, or a
/// reserved project name.
pub fn validate_project_name(name: &str) -> Result<(), CliError> {
    if name.is_empty() {
        return Err(CliError::InvalidInput(
            "invalid project name: name must not be empty".to_string(),
        ));
    }
    if name.len() > MAX_PROJECT_NAME_LEN {
        return Err(CliError::InvalidInput(format!(
            "invalid project name '{name}': must be at most {MAX_PROJECT_NAME_LEN} characters"
        )));
    }
    if !name.chars().next().is_some_and(|c| c.is_ascii_alphabetic()) {
        return Err(CliError::InvalidInput(format!(
            "invalid project name '{name}': must start with an ASCII letter"
        )));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(CliError::InvalidInput(format!(
            "invalid project name '{name}': only ASCII letters, digits, '-' and '_' are allowed"
        )));
    }
    // Keyword/reserved collision is checked against the raw `-` → `_` form (not
    // the case-folded render ident) so name *acceptance* matches cargo-new: a
    // name is rejected only when it is itself a keyword/reserved word, never
    // merely because it case-folds to one (e.g. `MyApp` stays accepted).
    let raw_ident = name.replace('-', "_");
    if RUST_KEYWORDS.contains(&raw_ident.as_str()) || RUST_KEYWORDS.contains(&name) {
        return Err(CliError::InvalidInput(format!(
            "invalid project name '{name}': resolves to the Rust keyword '{raw_ident}'"
        )));
    }
    if RESERVED_PROJECT_NAMES.contains(&name)
        || RESERVED_PROJECT_NAMES.contains(&raw_ident.as_str())
    {
        return Err(CliError::InvalidInput(format!(
            "invalid project name '{name}': '{name}' is a reserved name"
        )));
    }
    Ok(())
}

/// Validates `name` and derives every scaffold identifier from it.
///
/// # Errors
///
/// Propagates [`validate_project_name`]'s error for an invalid name.
pub fn derive_names(name: &str) -> Result<ScaffoldNames, CliError> {
    validate_project_name(name)?;
    let ident = derive_crate_ident(name);
    Ok(ScaffoldNames {
        crate_name: name.to_string(),
        workflow_fn: format!("{ident}_workflow"),
        activity_fn: format!("{ident}_activity"),
        queue: ident.clone(),
        ident,
    })
}

/// Replaces every `{{key}}` placeholder in `template` with its value, applying
/// substitutions in the given order.
#[must_use]
pub fn apply_substitutions(template: &str, subs: &[(&str, &str)]) -> String {
    let mut out = template.to_string();
    for (key, value) in subs {
        out = out.replace(key, value);
    }
    out
}

/// Renders the `minimal` template for `names`, returning
/// `(relative_output_path, rendered_content)` for every file to emit.
#[must_use]
pub fn render_minimal(names: &ScaffoldNames) -> Vec<(&'static str, String)> {
    let subs = [
        ("{{crate_name}}", names.crate_name.as_str()),
        ("{{ident}}", names.ident.as_str()),
        ("{{workflow_fn}}", names.workflow_fn.as_str()),
        ("{{activity_fn}}", names.activity_fn.as_str()),
        ("{{queue}}", names.queue.as_str()),
    ];
    MINIMAL_TEMPLATE
        .iter()
        .map(|(path, body)| (*path, apply_substitutions(body, &subs)))
        .collect()
}

/// Returns `true` when `dir` exists and contains at least one entry.
fn dir_is_non_empty(dir: &Path) -> bool {
    fs::read_dir(dir).is_ok_and(|mut entries| entries.next().is_some())
}

/// Scaffolds a new project named `name` into `path` (default `./<name>`).
///
/// Validates the name and renders the template entirely before writing any
/// file, so an invalid name or a non-empty target (without `force`) leaves the
/// filesystem untouched. Never removes files it did not write.
///
/// # Errors
///
/// Returns [`CliError::InvalidInput`] for an invalid name or a non-empty target
/// directory without `force`, or [`CliError::WriteOutput`] on an I/O failure.
pub fn run_new(
    name: &str,
    path: Option<&Path>,
    force: bool,
    template: ScaffoldTemplate,
) -> Result<(), CliError> {
    let names = derive_names(name)?;
    let default_path = PathBuf::from(&names.crate_name);
    let target = path.unwrap_or(&default_path);

    // A plain file (or other non-directory) at the target can never be
    // scaffolded into; reject it up front with a clear message rather than
    // letting `create_dir_all` surface an opaque OS error. `--force` cannot
    // help — it only overwrites the scaffold's own files inside a directory.
    if target.exists() && !target.is_dir() {
        return Err(CliError::InvalidInput(format!(
            "target '{}' exists and is not a directory",
            target.display()
        )));
    }

    if !force && dir_is_non_empty(target) {
        return Err(CliError::InvalidInput(format!(
            "target directory '{}' is not empty; pass --force to overwrite",
            target.display()
        )));
    }

    let files = match template {
        ScaffoldTemplate::Minimal => render_minimal(&names),
    };

    // Render is complete and the target passed the safety check: now write.
    fs::create_dir_all(target).map_err(|source| CliError::WriteOutput {
        path: target.display().to_string(),
        source,
    })?;
    for (rel, content) in &files {
        let out = target.join(rel);
        if let Some(parent) = out.parent() {
            fs::create_dir_all(parent).map_err(|source| CliError::WriteOutput {
                path: parent.display().to_string(),
                source,
            })?;
        }
        fs::write(&out, content).map_err(|source| CliError::WriteOutput {
            path: out.display().to_string(),
            source,
        })?;
    }

    print_new_next_steps(&names, target);
    Ok(())
}

/// Prints the post-scaffold "next steps" (the three-command run path).
fn print_new_next_steps(names: &ScaffoldNames, target: &Path) {
    println!(
        "Created project '{}' in {}",
        names.crate_name,
        target.display()
    );
    println!();
    println!("Next steps:");
    println!("  cd {}", target.display());
    println!("  docker compose up -d");
    println!("  AUTUMN_PROFILE=dev cargo run");
    println!();
    println!(
        "  # then trigger a run:\n  curl -X POST http://localhost:3000/api/harvest/workflows/{}/start \\",
        names.workflow_fn
    );
    println!(
        "    -H 'Content-Type: application/json' -d '{{\"workflow_id\":\"demo-1\",\"input\":\"World\"}}'"
    );
}

/// Execute the API request represented by the CLI arguments.
///
/// # Errors
///
/// Returns an error if request construction fails, the HTTP request fails, the
/// API returns a non-success status, or the response body is not valid JSON.
pub async fn execute(cli: &Cli) -> Result<Value, CliError> {
    let request = cli.api_request()?;
    let client = reqwest::Client::new();
    let url = format!("{}{}", cli.base_url.trim_end_matches('/'), request.path);
    let builder = match request.method {
        ApiMethod::Get => client.get(url),
        ApiMethod::Patch => client.patch(url),
        ApiMethod::Post => client.post(url),
        ApiMethod::Delete => client.delete(url),
    };
    // A dev-profile Autumn app answers an unrecognized-Accept validation
    // error with its HTML debug page, not the documented JSON error body.
    // Without this header, the CLI dumps that markup verbatim.
    let builder = builder.header(reqwest::header::ACCEPT, "application/json");
    let builder = if let Some(token) = &cli.token {
        builder.bearer_auth(token)
    } else {
        builder
    };
    // Mutating requests identify the CLI as the call source and carry the
    // operator identity and correlation id when supplied.
    let builder = if request.method == ApiMethod::Get {
        builder
    } else {
        let mut b = builder.header("x-harvest-source", "cli");
        if let Some(actor) = &cli.actor {
            b = b.header("x-harvest-actor", actor);
        }
        if let Some(rid) = &cli.request_id {
            b = b.header("x-request-id", rid);
        }
        b
    };
    let builder = if let Some(body) = &request.body {
        builder.json(body)
    } else {
        builder
    };

    let response = builder.send().await?;
    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        return Err(CliError::Http { status, body });
    }
    if body.trim().is_empty() {
        return Ok(Value::Null);
    }

    serde_json::from_str(&body).map_err(CliError::ParseResponse)
}

/// Open the SSE stream for `execution_id` and print events to stdout.
///
/// Each complete SSE event block is printed as `<event-type>: <data>`.
/// The function returns when the server sends `event: stream-end` or the
/// connection closes. SSE comment lines (keepalives) are silently discarded.
async fn run_events_tail(
    cli: &Cli,
    execution_id: &str,
    last_event_id: Option<i64>,
) -> Result<(), CliError> {
    let path = format!("/executions/{}", path_segment(execution_id));
    let url = format!(
        "{}{}/events/stream",
        cli.base_url.trim_end_matches('/'),
        path
    );

    let client = reqwest::Client::new();
    let mut builder = client
        .get(&url)
        .header("Accept", "text/event-stream")
        .header("Cache-Control", "no-cache");

    if let Some(token) = &cli.token {
        builder = builder.bearer_auth(token);
    }
    if let Some(id) = last_event_id {
        builder = builder.header("Last-Event-ID", id.to_string());
    }

    let response = builder.send().await?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await?;
        return Err(CliError::Http { status, body });
    }

    let mut response = response;
    let mut buf: Vec<u8> = Vec::new();
    // SSE fields for the current event block.
    let mut ev_id = String::new();
    let mut ev_type = String::new();
    let mut ev_data = String::new();

    loop {
        let chunk = response.chunk().await?;
        let Some(bytes) = chunk else {
            // Server closed the connection.
            break;
        };
        buf.extend_from_slice(&bytes);

        // Process complete lines from buf.
        while let Some(nl) = buf.iter().position(|&b| b == b'\n') {
            let line_bytes = &buf[..nl];
            // Strip trailing CR for CRLF line endings.
            let line_bytes = line_bytes.strip_suffix(b"\r").unwrap_or(line_bytes);
            let line = String::from_utf8_lossy(line_bytes).into_owned();
            buf.drain(..=nl);

            if line.is_empty() {
                // Empty line = dispatch event block.
                if !ev_data.is_empty() || !ev_type.is_empty() {
                    let display_type = if ev_type.is_empty() {
                        "message"
                    } else {
                        &ev_type
                    };
                    println!("{display_type}: {ev_data}");
                    if ev_type == "stream-end" {
                        return Ok(());
                    }
                    if ev_type == "stream-error" {
                        return Err(CliError::SseStreamError {
                            message: ev_data.clone(),
                        });
                    }
                }
                ev_id.clear();
                ev_type.clear();
                ev_data.clear();
            } else if line.starts_with(':') {
                // SSE comment (keepalive ping) — discard silently.
            } else {
                let (key, value) = line.find(':').map_or((line.as_str(), ""), |colon_idx| {
                    let (k, mut v) = line.split_at(colon_idx);
                    v = &v[1..];
                    if v.starts_with(' ') {
                        v = &v[1..];
                    }
                    (k, v)
                });
                match key {
                    "id" => {
                        ev_id = value.to_string();
                        let _ = &ev_id; // suppress unused warning; stored for protocol correctness
                    }
                    "event" => {
                        ev_type = value.to_string();
                    }
                    "data" => {
                        if !ev_data.is_empty() {
                            ev_data.push('\n');
                        }
                        ev_data.push_str(value);
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(())
}

/// Issue drain then poll `GET /workers/{id}` until status reaches `Stopped`
/// or `wait_timeout_secs` elapses. Prints each poll result as it arrives.
async fn run_worker_drain_wait(
    cli: &Cli,
    worker_id: &str,
    wait_timeout_secs: u64,
) -> Result<(), CliError> {
    // Kick off the drain.
    let response = execute(cli).await?;
    let rendered = render_response(cli, &response)?;
    println!("{rendered}");

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(wait_timeout_secs);
    let poll_interval = std::time::Duration::from_secs(2);

    loop {
        tokio::time::sleep(poll_interval).await;

        let poll_cli = Cli {
            base_url: cli.base_url.clone(),
            token: cli.token.clone(),
            actor: cli.actor.clone(),
            request_id: cli.request_id.clone(),
            output: cli.output,
            command: Commands::Worker {
                command: WorkerCommand::Get {
                    worker_id: worker_id.to_string(),
                },
            },
        };
        let worker_value = execute(&poll_cli).await?;
        let status = worker_value
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("Unknown")
            .to_string();

        let rendered = render_response(cli, &worker_value)?;
        println!("{rendered}");

        if status == "Stopped" {
            return Ok(());
        }

        if std::time::Instant::now() >= deadline {
            return Err(CliError::DrainWaitTimeout {
                worker_id: worker_id.to_string(),
                last_status: status,
            });
        }
    }
}

/// Render a successful response.
///
/// # Errors
///
/// Returns an error if the JSON value cannot be serialized.
pub fn format_output(value: &Value, output: OutputFormat) -> Result<String, CliError> {
    match output {
        OutputFormat::PrettyJson => {
            serde_json::to_string_pretty(value).map_err(CliError::SerializeResponse)
        }
        OutputFormat::Json => serde_json::to_string(value).map_err(CliError::SerializeResponse),
    }
}

fn render_response(cli: &Cli, value: &Value) -> Result<String, CliError> {
    let state_filtered = completion_delivery_list_state_filter(cli)
        .map(|state| filter_completion_deliveries_by_state(value, state));
    let value = state_filtered.as_ref().unwrap_or(value);

    if preflight_wants_table(cli) {
        return Ok(format_preflight_table(value));
    }
    if shard_health_wants_table(cli) {
        return Ok(format_shard_health_table(value));
    }
    if canary_wants_table(cli) {
        return Ok(format_canary_table(value));
    }
    if workflow_children_wants_table(cli) {
        return Ok(format_workflow_children_table(value));
    }
    if workflow_summaries_wants_table(cli) {
        return Ok(format_workflow_summaries_table(value));
    }
    if lineage_tree_wants_render(cli) {
        return Ok(if lineage_tree_wants_summary(cli) {
            format_lineage_summary(value)
        } else {
            format_lineage_tree(value)
        });
    }
    if run_chain_wants_table(cli) {
        return Ok(format_run_chain_table(value));
    }
    if batch_preview_wants_table(cli) {
        return Ok(format_batch_preview_table(value));
    }
    if handoff_wants_table(cli) {
        return Ok(format_handoff_table(value));
    }
    if dlq_aggregate_wants_table(cli) {
        return Ok(format_dlq_aggregate_table(value));
    }
    if audit_list_wants_table(cli) {
        return Ok(format_audit_table(value));
    }
    if version_usage_wants_table(cli) {
        return Ok(format_version_usage_table(value));
    }
    if retirement_check_wants_table(cli) {
        return Ok(format_retirement_check_table(value));
    }
    if workflow_reachability_wants_table(cli) {
        return Ok(format_workflow_reachability_table(value));
    }
    if queue_coverage_wants_table(cli) {
        return Ok(format_queue_coverage_table(value));
    }
    if activity_list_wants_table(cli) {
        return Ok(format_activity_list_table(value));
    }
    if backfill_wants_table(cli) {
        return Ok(format_backfill_table(value));
    }
    if rate_limit_wants_table(cli) {
        return Ok(format_rate_limit_table(value));
    }
    if usage_wants_table(cli) {
        return Ok(format_usage_table(value));
    }
    if diagnose_wants_table(cli) {
        return Ok(format_diagnose_verdict(value));
    }

    let output = if workflow_children_wants_raw_json(cli)
        || workflow_summaries_wants_raw_json(cli)
        || run_chain_wants_raw_json(cli)
        || lineage_tree_wants_raw_json(cli)
        || batch_preview_wants_raw_json(cli)
        || handoff_wants_raw_json(cli)
        || dlq_aggregate_wants_raw_json(cli)
        || canary_wants_raw_json(cli)
        || workflow_reachability_wants_raw_json(cli)
        || queue_coverage_wants_raw_json(cli)
        || activity_list_wants_raw_json(cli)
        || usage_wants_raw_json(cli)
        || diagnose_wants_raw_json(cli)
    {
        OutputFormat::Json
    } else {
        cli.output
    };
    // Issue #756: `render_response` returns ONLY the body. When a list read
    // degraded (a shard was unreachable) the caller emits the partial-
    // availability notice separately on STDERR via `fanout_partial_notice`, so
    // STDOUT stays a clean/parseable body on both paths — `workflow list -o
    // json | jq` is not corrupted by a prepended warning line. (The special
    // table formatters above, usage/dlq_aggregate, render their own
    // unavailable-shard block inline.)
    format_output(value, output)
}

/// Build a human-readable "shard(s) unavailable" notice line from a degraded
/// cross-shard fan-out envelope (issue #756), or `None` when the body is not a
/// degraded envelope (a bare array on the happy path, or an object with an
/// empty/absent `unavailable_shards`).
fn fanout_partial_notice(value: &Value) -> Option<String> {
    let obj = value.as_object()?;
    let unavailable = obj.get("unavailable_shards")?.as_array()?;
    if unavailable.is_empty() {
        return None;
    }
    let status = obj
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("partial");
    let detail: Vec<String> = unavailable
        .iter()
        .map(|shard| {
            let id = cell_number(shard.get("shard_id"));
            let reason = cell_str(shard.get("reason"));
            if reason.is_empty() {
                id
            } else {
                format!("{id}: {reason}")
            }
        })
        .collect();
    Some(format!(
        "WARNING: cross-shard read is {status}; {} shard(s) unavailable: {}",
        unavailable.len(),
        detail.join(", ")
    ))
}

fn diagnose_wants_table(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Workflow {
            command: WorkflowCommand::Diagnose { json: false, .. }
        }
    ) && cli.output == OutputFormat::PrettyJson
}

const fn diagnose_wants_raw_json(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Workflow {
            command: WorkflowCommand::Diagnose { json: true, .. }
        }
    )
}

/// Render `GET /workflows/{id}/diagnose` as a human-readable verdict
/// (issue #809).
///
/// Leads with the two things an operator reads first — the one-word `health`
/// and the one-sentence `summary` — then the discriminated `blocked_on`
/// members, so the actionable field (the uncovered queue, the open circuit's
/// cooldown, the awaited signal's name) is on screen without a `jq` pipeline.
fn format_diagnose_verdict(value: &Value) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "execution: {}  workflow: {}  state: {}",
        cell_str(value.get("execution_id")),
        cell_str(value.get("workflow_name")),
        cell_str(value.get("state")),
    );
    let _ = writeln!(out, "health:    {}", cell_str(value.get("health")));
    let _ = writeln!(out, "summary:   {}", cell_str(value.get("summary")));

    if let Some(blocked) = value.get("blocked_on").and_then(Value::as_object) {
        let _ = writeln!(
            out,
            "blocked on: {}",
            blocked
                .get("type")
                .map_or("-", |v| v.as_str().unwrap_or("-"))
        );
        // Every member except the discriminator, in the response's own order.
        for (key, member) in blocked.iter().filter(|(k, _)| k.as_str() != "type") {
            let _ = writeln!(out, "  {key}: {}", cell_str(Some(member)));
        }
    }

    if let Some(outcome) = value.get("terminal_outcome").and_then(Value::as_object) {
        let _ = writeln!(out, "terminal outcome:");
        for (key, member) in outcome {
            let _ = writeln!(out, "  {key}: {}", cell_str(Some(member)));
        }
    }

    let reasons = value
        .get("contributing_reason_codes")
        .and_then(Value::as_array)
        .map(|codes| {
            codes
                .iter()
                .map(|c| cell_str(Some(c)))
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_default();
    if !reasons.is_empty() {
        let _ = writeln!(out, "contributing reasons: {reasons}");
    }

    let _ = writeln!(
        out,
        "last event: {} ({}s ago)",
        cell_str(value.get("last_event_at")),
        format_f64(value.get("last_event_age_seconds")),
    );
    let wait_set = cell_str(value.get("wait_set"));
    match value.get("wait_set_reason").and_then(Value::as_str) {
        Some(reason) => {
            let _ = writeln!(out, "wait set:   {wait_set} ({reason})");
        }
        None => {
            let _ = writeln!(out, "wait set:   {wait_set}");
        }
    }
    out
}

fn usage_wants_table(cli: &Cli) -> bool {
    matches!(&cli.command, Commands::Usage { json: false, .. })
        && cli.output == OutputFormat::PrettyJson
}

const fn usage_wants_raw_json(cli: &Cli) -> bool {
    matches!(&cli.command, Commands::Usage { json: true, .. })
}

/// Render the `GET /admin/usage` response as a human-readable table
/// (issue #596). One row per group, plus a header line naming the window,
/// grouping dimension, and status.
fn format_usage_table(value: &Value) -> String {
    let status = cell_str(value.get("status"));
    let from = cell_str(value.get("from"));
    let to = cell_str(value.get("to"));
    let group_by = cell_str(value.get("group_by"));
    let mut summary = format!("status: {status}  window: {from} .. {to}  group_by: {group_by}");

    if let Some(unavailable) = value.get("unavailable_shards").and_then(Value::as_array)
        && !unavailable.is_empty()
    {
        let shard_ids: Vec<String> = unavailable
            .iter()
            .map(|s| cell_number(s.get("shard_id")))
            .collect();
        let _ = write!(summary, "  (unavailable shards: {})", shard_ids.join(","));
    }

    let Some(groups) = value.get("groups").and_then(Value::as_array) else {
        return format!("{summary}\nNo usage groups found.");
    };
    if groups.is_empty() {
        return format!("{summary}\nNo usage groups found.");
    }

    let header: Vec<String> = [
        "GROUP",
        "STARTS",
        "COMPLETED",
        "FAILED",
        "CANCELLED",
        "TIMED_OUT",
        "ACT_EXEC",
        "ACT_FAILED",
        "COMPUTE_S",
    ]
    .iter()
    .map(ToString::to_string)
    .collect();

    let mut rows = vec![header];
    for group in groups {
        rows.push(vec![
            cell_str(group.get("group")),
            cell_number(group.get("workflow_starts")),
            cell_number(group.get("completed")),
            cell_number(group.get("failed")),
            cell_number(group.get("cancelled")),
            cell_number(group.get("timed_out")),
            cell_number(group.get("activity_executions")),
            cell_number(group.get("activity_executions_failed")),
            format_f64(group.get("activity_compute_seconds")),
        ]);
    }

    let table = render_table(&rows);

    format!("{summary}\n\n{table}")
}

fn dlq_aggregate_wants_table(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Dlq {
            command: DeadLetterCommand::Aggregate { json: false, .. }
        }
    ) && cli.output == OutputFormat::PrettyJson
}

const fn dlq_aggregate_wants_raw_json(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Dlq {
            command: DeadLetterCommand::Aggregate { json: true, .. }
        }
    )
}

/// Render the DLQ aggregation response as a human-readable table.
///
/// One row per group: the hierarchical key columns, the count, the time window,
/// and a comma-joined preview of sample dead-letter IDs.
fn format_dlq_aggregate_table(value: &Value) -> String {
    let total = value.get("total").and_then(Value::as_i64).unwrap_or(0);
    let filtered = value
        .get("filtered_total")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let truncated = value
        .get("truncated")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let Some(groups) = value.get("groups").and_then(Value::as_array) else {
        return format!("total: {total}  filtered: {filtered}\nNo DLQ groups found.");
    };
    if groups.is_empty() {
        return format!("total: {total}  filtered: {filtered}\nNo DLQ groups found.");
    }

    // Collect the union of key field names (in first-seen order), skipping the
    // `_other` rollup marker so it does not create a phantom column.
    let mut key_cols: Vec<String> = Vec::new();
    for group in groups {
        if let Some(obj) = group.get("key").and_then(Value::as_object) {
            for name in obj.keys() {
                if name != "_other" && !key_cols.iter().any(|c| c == name) {
                    key_cols.push(name.clone());
                }
            }
        }
    }

    let mut header: Vec<String> = key_cols.iter().map(|c| c.to_uppercase()).collect();
    header.push("COUNT".to_string());
    header.push("FIRST_SEEN".to_string());
    header.push("LAST_SEEN".to_string());
    header.push("SAMPLES".to_string());

    let mut rows = vec![header];
    for group in groups {
        let key = group.get("key");
        let is_other = key
            .and_then(|k| k.get("_other"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let mut row: Vec<String> = Vec::new();
        for (idx, col) in key_cols.iter().enumerate() {
            if is_other {
                // The `_other` rollup has no per-dimension key; label the first
                // column and leave the rest blank.
                row.push(if idx == 0 {
                    "(other)".to_string()
                } else {
                    String::new()
                });
            } else {
                row.push(cell_str(key.and_then(|k| k.get(col))));
            }
        }
        row.push(cell_number(group.get("count")));
        row.push(cell_str(group.get("first_seen")));
        row.push(cell_str(group.get("last_seen")));
        let samples = group
            .get("sample_dead_letter_ids")
            .and_then(Value::as_array)
            .map(|ids| {
                ids.iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_default();
        row.push(samples);
        rows.push(row);
    }

    let table = render_table(&rows);

    let mut summary = format!("total: {total}  filtered: {filtered}");
    if truncated {
        summary.push_str("  (long tail rolled into _other)");
    }
    format!("{summary}\n\n{table}")
}

fn backfill_wants_table(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Schedule {
            command: ScheduleCommand::Backfill { .. }
        }
    ) && cli.output == OutputFormat::PrettyJson
}

fn rate_limit_wants_table(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::RateLimit {
            command: RateLimitCommand::Status
        }
    ) && cli.output == OutputFormat::PrettyJson
}

fn format_rate_limit_table(value: &Value) -> String {
    let Some(items) = value.as_array().filter(|v| !v.is_empty()) else {
        return "No rate limit buckets found.".to_string();
    };

    let mut rows: Vec<Vec<String>> = Vec::with_capacity(items.len() + 1);
    rows.push(vec![
        "KEY".to_string(),
        "REFILL_RATE".to_string(),
        "BURST_CAPACITY".to_string(),
        "CURRENT_TOKENS".to_string(),
        "LAST_REFILLED_AT".to_string(),
    ]);

    for item in items {
        rows.push(vec![
            cell_str(item.get("key")),
            format_f64(item.get("refill_rate")),
            format_f64(item.get("burst")),
            format_f64(item.get("tokens")),
            cell_str(item.get("last_refilled_at")),
        ]);
    }

    render_table(&rows)
}

fn format_f64(value: Option<&Value>) -> String {
    value
        .and_then(Value::as_f64)
        .map_or_else(String::new, |number| format!("{number:.2}"))
}

fn format_backfill_table(value: &Value) -> String {
    let status = value.get("status").and_then(Value::as_str).unwrap_or("-");
    let name = value.get("name").and_then(Value::as_str).unwrap_or("-");
    let kind = value.get("kind").and_then(Value::as_str).unwrap_or("-");
    let from = value.get("from").and_then(Value::as_str).unwrap_or("-");
    let to = value.get("to").and_then(Value::as_str).unwrap_or("-");
    let total = value.get("total").and_then(Value::as_u64).unwrap_or(0);
    let dispatched = value.get("dispatched").and_then(Value::as_u64).unwrap_or(0);
    let skipped = value.get("skipped").and_then(Value::as_u64).unwrap_or(0);
    let failed = value.get("failed").and_then(Value::as_u64).unwrap_or(0);

    let mut out = String::new();
    let _ = writeln!(out, "status: {status}  kind: {kind}  name: {name}");
    let _ = writeln!(out, "window: {from} \u{2192} {to}");
    let _ = writeln!(
        out,
        "total: {total}  dispatched: {dispatched}  skipped: {skipped}  failed: {failed}"
    );

    // Skipped reasons
    if let Some(reasons) = value
        .get("skipped_reasons")
        .and_then(Value::as_object)
        .filter(|r| !r.is_empty())
    {
        let parts: Vec<String> = reasons
            .iter()
            .map(|(k, v)| format!("{k}={}", v.as_u64().unwrap_or(0)))
            .collect();
        let _ = writeln!(out, "skipped reasons: {}", parts.join(", "));
    }

    // Planned timestamps
    if let Some(timestamps) = value.get("planned_timestamps").and_then(Value::as_array) {
        if timestamps.is_empty() {
            out.push_str("\nNo timestamps planned.\n");
        } else {
            let _ = writeln!(out, "\nPlanned timestamps ({}):", timestamps.len());
            for ts in timestamps {
                let ts_str = ts.as_str().unwrap_or("-");
                let _ = writeln!(out, "  {ts_str}");
            }
        }
    }

    // Partial shard failures
    if let Some(failures) = value
        .get("partial_shard_failures")
        .and_then(Value::as_array)
        .filter(|f| !f.is_empty())
    {
        out.push_str("\nShard failures:\n");
        for f in failures {
            let shard_id = f.get("shard_id").and_then(Value::as_i64).unwrap_or(-1);
            let reason = f.get("reason").and_then(Value::as_str).unwrap_or("-");
            let _ = writeln!(out, "  shard {shard_id}: {reason}");
        }
    }

    // Paused schedule warning (DAG backfill with include_paused=true)
    if let Some(warning) = value.get("paused_schedule_warning").and_then(Value::as_str) {
        let _ = writeln!(out, "\nWARNING: {warning}");
    }

    out
}

fn preflight_wants_table(cli: &Cli) -> bool {
    matches!(&cli.command, Commands::Preflight) && cli.output == OutputFormat::PrettyJson
}

fn canary_wants_table(cli: &Cli) -> bool {
    matches!(&cli.command, Commands::Canary { json: false, .. })
        && cli.output == OutputFormat::PrettyJson
}

const fn canary_should_gate(cli: &Cli) -> bool {
    matches!(&cli.command, Commands::Canary { .. })
}

const fn canary_wants_raw_json(cli: &Cli) -> bool {
    matches!(&cli.command, Commands::Canary { json: true, .. })
}

fn canary_exit_code(value: &Value) -> i32 {
    match value.get("verdict").and_then(Value::as_str) {
        Some("pass") => 0,
        _ => 1,
    }
}

#[allow(clippy::too_many_lines)]
fn format_canary_table(value: &Value) -> String {
    let verdict = value
        .get("verdict")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_uppercase();
    let sampled = value.get("sampled").and_then(Value::as_u64).unwrap_or(0);
    let succeeded = value
        .get("replay_succeeded")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let failed = value
        .get("replay_failed")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let truncated = value
        .get("truncated")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let mut out = String::new();
    let _ = writeln!(
        out,
        "Canary Verdict: {verdict}\nSampled: {sampled} (succeeded: {succeeded}, failed: {failed}, truncated: {truncated})"
    );

    // Summary by type
    if let Some(summary_map) = value
        .get("summary_by_type")
        .and_then(Value::as_object)
        .filter(|m| !m.is_empty())
    {
        let mut rows = Vec::with_capacity(summary_map.len() + 1);
        rows.push(vec![
            "WORKFLOW TYPE".to_string(),
            "SAMPLED".to_string(),
            "SUCCEEDED".to_string(),
            "FAILED".to_string(),
        ]);

        // Sort keys for deterministic output
        let mut keys: Vec<&String> = summary_map.keys().collect();
        keys.sort();

        for name in keys {
            let summary = &summary_map[name];
            let s_sampled = summary.get("sampled").and_then(Value::as_u64).unwrap_or(0);
            let s_succeeded = summary
                .get("replay_succeeded")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let s_failed = summary
                .get("replay_failed")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            rows.push(vec![
                name.clone(),
                s_sampled.to_string(),
                s_succeeded.to_string(),
                s_failed.to_string(),
            ]);
        }

        let table = render_table(&rows);

        let _ = writeln!(out, "\nSummary by Workflow Type:\n{table}");
    }

    // Failure details
    if let Some(details) = value
        .get("details")
        .and_then(Value::as_array)
        .filter(|a| !a.is_empty())
    {
        let mut rows = Vec::with_capacity(details.len() + 1);
        rows.push(vec![
            "EXECUTION ID".to_string(),
            "WORKFLOW TYPE".to_string(),
            "KIND".to_string(),
            "EVENT IDX".to_string(),
            "ERROR".to_string(),
        ]);

        for failure in details {
            let execution_id = failure
                .get("execution_id")
                .and_then(Value::as_str)
                .unwrap_or("-");
            let w_name = failure
                .get("workflow_name")
                .and_then(Value::as_str)
                .unwrap_or("-");
            let kind = failure.get("kind").and_then(Value::as_str).unwrap_or("-");
            let event_idx = failure
                .get("event_index")
                .and_then(Value::as_u64)
                .map_or_else(|| "-".to_string(), |idx| idx.to_string());
            let error = failure.get("error").and_then(Value::as_str).unwrap_or("-");

            rows.push(vec![
                execution_id.to_string(),
                w_name.to_string(),
                kind.to_string(),
                event_idx,
                error.to_string(),
            ]);
        }

        let table = render_table(&rows);

        let _ = writeln!(out, "\nReplay Failures:\n{table}");

        // Additional diagnostic details (expected vs actual) if present
        for failure in details {
            let expected = failure.get("expected").and_then(Value::as_str);
            let actual = failure.get("actual").and_then(Value::as_str);
            if expected.is_some() || actual.is_some() {
                let exec_id = failure
                    .get("execution_id")
                    .and_then(Value::as_str)
                    .unwrap_or("-");
                let _ = writeln!(out, "\nDiagnostic details for execution {exec_id}:");
                if let Some(exp) = expected {
                    let _ = writeln!(out, "  Expected: {exp}");
                }
                if let Some(act) = actual {
                    let _ = writeln!(out, "  Actual:   {act}");
                }
            }
        }
    }

    out
}

fn shard_health_wants_table(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Shard {
            command: ShardCommand::Health { .. }
        }
    ) && cli.output == OutputFormat::PrettyJson
}

const fn shard_health_should_gate(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Shard {
            command: ShardCommand::Health { .. }
        }
    )
}

fn preflight_exit_code(value: &Value) -> i32 {
    match value.get("overall_status").and_then(Value::as_str) {
        Some("pass") => 0,
        Some("warn") => 2,
        _ => 1,
    }
}

fn shard_health_exit_code(value: &Value) -> i32 {
    let Some(shards) = value.get("shards").and_then(Value::as_array) else {
        return 1;
    };
    let has_non_ready_gate_target = shards.iter().any(|shard| {
        let readiness = shard.get("readiness").and_then(Value::as_str);
        if readiness == Some("ready") {
            return false;
        }
        let candidate = shard
            .get("candidate")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let writable = shard
            .get("roles")
            .and_then(Value::as_array)
            .is_some_and(|roles| roles.iter().any(|role| role.as_str() == Some("writable")));
        candidate || writable
    });
    i32::from(has_non_ready_gate_target)
}

const fn version_usage_should_guard(cli: &Cli) -> bool {
    matches!(&cli.command, Commands::VersionUsage { guard: true, .. })
}

fn version_usage_wants_table(cli: &Cli) -> bool {
    matches!(&cli.command, Commands::VersionUsage { .. }) && cli.output == OutputFormat::PrettyJson
}

fn version_usage_guard_exit_code(value: &Value) -> i32 {
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unavailable");
    if matches!(status, "partial" | "unavailable") {
        return 1;
    }
    let active = value
        .get("items")
        .and_then(Value::as_array)
        .map_or(0, |items| {
            items
                .iter()
                .filter_map(|item| item.get("active_executions").and_then(Value::as_i64))
                .sum::<i64>()
        });
    i32::from(active > 0)
}

fn format_preflight_table(value: &Value) -> String {
    let overall = value
        .get("overall_status")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let observed_at = value
        .get("observed_at")
        .and_then(Value::as_str)
        .unwrap_or("-");
    let Some(checks) = value.get("checks").and_then(Value::as_array) else {
        return format!(
            "overall_status: {overall}\nobserved_at: {observed_at}\nNo checks returned."
        );
    };

    let mut rows = Vec::with_capacity(checks.len() + 1);
    rows.push(vec![
        "STATUS".to_string(),
        "CHECK".to_string(),
        "SCOPE".to_string(),
        "SUMMARY".to_string(),
    ]);
    for check in checks {
        let shards = check
            .get("affected_shards")
            .and_then(Value::as_array)
            .filter(|values| !values.is_empty())
            .map_or_else(
                || "-".to_string(),
                |values| {
                    let ids = values
                        .iter()
                        .filter_map(Value::as_i64)
                        .map(|id| id.to_string())
                        .collect::<Vec<_>>()
                        .join(",");
                    format!("shards={ids}")
                },
            );
        rows.push(vec![
            cell_str(check.get("status")),
            cell_str(check.get("name")),
            shards,
            cell_str(check.get("summary")),
        ]);
    }

    let table = render_table(&rows);

    let findings = format_preflight_findings(checks);
    if findings.is_empty() {
        format!("overall_status: {overall}\nobserved_at: {observed_at}\n\n{table}")
    } else {
        format!("overall_status: {overall}\nobserved_at: {observed_at}\n\n{table}\n\n{findings}")
    }
}

/// Render the per-check detail block the summary table cannot show.
///
/// The table's `SUMMARY` column carries only a check's one-line verdict, so the
/// payload an operator actually needs in order to act — the specific unresolved
/// references in `details.failures`, and the check's `remediation` — is
/// invisible in table mode. Surfacing them beneath the table makes a failing
/// `harvest preflight` (and its non-zero exit in CI) actionable without
/// re-running the command through `--output json | jq`.
///
/// Only non-`pass` checks contribute, and only when they carry at least one
/// failure or a remediation, so a healthy fleet's output is unchanged.
fn format_preflight_findings(checks: &[Value]) -> String {
    let mut blocks = Vec::new();
    for check in checks {
        let status = check.get("status").and_then(Value::as_str).unwrap_or("");
        if status == "pass" {
            continue;
        }
        let name = check
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("(unnamed)");
        let mut lines = vec![format!("{name} ({status})")];
        if let Some(failures) = check
            .get("details")
            .and_then(|details| details.get("failures"))
            .and_then(Value::as_array)
        {
            for failure in failures {
                lines.push(format!("  - {}", preflight_failure_text(failure)));
            }
        }
        if let Some(remediation) = check.get("remediation").and_then(Value::as_str) {
            lines.push(format!("  remediation: {remediation}"));
        }
        if lines.len() > 1 {
            blocks.push(lines.join("\n"));
        }
    }
    blocks.join("\n\n")
}

/// A `details.failures` entry is a plain string for some checks (catalog
/// consistency, worker coverage) and a structured object for others (schedule
/// resolvability); render both without dropping information.
fn preflight_failure_text(failure: &Value) -> String {
    failure
        .as_str()
        .map_or_else(|| failure.to_string(), ToString::to_string)
}

fn format_shard_health_table(value: &Value) -> String {
    let overall = value
        .get("overall_readiness")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let observed_at = value
        .get("observed_at")
        .and_then(Value::as_str)
        .unwrap_or("-");
    let Some(shards) = value.get("shards").and_then(Value::as_array) else {
        return format!(
            "overall_readiness: {overall}\nobserved_at: {observed_at}\nNo shard rows returned."
        );
    };

    let mut rows = Vec::with_capacity(shards.len() + 1);
    rows.push(vec![
        "SHARD".to_string(),
        "ROLES".to_string(),
        "READY".to_string(),
        "REACH".to_string(),
        "SCHEMA".to_string(),
        "WORKERS".to_string(),
        "SCHED".to_string(),
        "QUEUE".to_string(),
        "DLQ".to_string(),
        "BLOCKERS".to_string(),
    ]);
    for shard in shards {
        rows.push(vec![
            cell_number(shard.get("shard_id")),
            roles_cell(shard),
            cell_str(shard.get("readiness")),
            bool_cell(shard.get("reachable")),
            bool_cell(shard.get("schema").and_then(|schema| schema.get("ready"))),
            worker_coverage_cell(shard),
            scheduler_cell(shard),
            cell_number(
                shard
                    .get("queue_depth")
                    .and_then(|summary| summary.get("total_pending")),
            ),
            cell_optional_number(shard.get("dlq").and_then(|summary| summary.get("count"))),
            blockers_cell(shard),
        ]);
    }

    let table = render_table(&rows);

    format!("overall_readiness: {overall}\nobserved_at: {observed_at}\n\n{table}")
}

fn format_version_usage_table(value: &Value) -> String {
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let observed_at = value
        .get("observed_at")
        .and_then(Value::as_str)
        .unwrap_or("-");
    let Some(items) = value.get("items").and_then(Value::as_array) else {
        return format!(
            "status: {status}\nobserved_at: {observed_at}\nNo version usage rows returned."
        );
    };
    if items.is_empty() {
        return format!(
            "status: {status}\nobserved_at: {observed_at}\nNo version usage rows found."
        );
    }

    let mut rows = Vec::with_capacity(items.len() + 1);
    rows.push(vec![
        "WORKFLOW".to_string(),
        "CHANGE ID".to_string(),
        "VERSION".to_string(),
        "ACTIVE".to_string(),
        "TERMINAL".to_string(),
        "OLDEST_AGE_S".to_string(),
        "NEWEST_AGE_S".to_string(),
        "SHARDS".to_string(),
        "UNAVAILABLE".to_string(),
    ]);
    for item in items {
        rows.push(vec![
            cell_str(item.get("workflow_name")),
            cell_str(item.get("change_id")),
            cell_number(item.get("recorded_version")),
            cell_number(item.get("active_executions")),
            cell_number(item.get("terminal_executions")),
            cell_number(item.get("oldest_matching_execution_age_secs")),
            cell_number(item.get("newest_matching_execution_age_secs")),
            shard_array_cell(item, "matched_shards"),
            shard_array_cell(item, "unavailable_shards"),
        ]);
    }

    let table = render_table(&rows);

    format!("status: {status}\nobserved_at: {observed_at}\n\n{table}")
}

// ─── Workflow-type reachability helpers (issue #520) ──────────────────────────

fn workflow_reachability_request(command: &WorkflowTypesCommand) -> ApiRequest {
    let WorkflowTypesCommand::Reachability { workflow_type, .. } = command;
    workflow_type.as_ref().map_or_else(
        || ApiRequest::get("/admin/workflow-types/reachability"),
        |value| {
            ApiRequest::get(format!(
                "/admin/workflow-types/reachability?workflow_type={}",
                query_encode(value)
            ))
        },
    )
}

const fn workflow_reachability_should_gate(cli: &Cli) -> bool {
    matches!(&cli.command, Commands::WorkflowTypes { .. })
}

/// `batch submit --dry-run` renders the preview as a table by default (#769).
///
/// Pass `--json` for the raw preview body. A real submit (no `--dry-run`) falls
/// through to the default JSON renderer, so its `{batch_job_id}` output is
/// unchanged.
fn batch_preview_wants_table(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Batch {
            command: BatchCommand::Submit {
                dry_run: true,
                json: false,
                ..
            }
        }
    ) && cli.output == OutputFormat::PrettyJson
}

const fn batch_preview_wants_raw_json(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Batch {
            command: BatchCommand::Submit {
                dry_run: true,
                json: true,
                ..
            }
        }
    )
}

/// Render a #769 dry-run batch preview as a human-readable table.
fn format_batch_preview_table(value: &Value) -> String {
    use std::fmt::Write as _;

    let action = cell_str(value.get("action"));
    let matched = cell_number(value.get("matched_count"));
    let truncated = value
        .get("sample_truncated")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let mut out = String::new();
    out.push_str("DRY RUN — no changes made\n");
    let _ = writeln!(out, "action:        {action}");
    let _ = writeln!(out, "matched_count: {matched}");

    if let Some(per_shard) = value.get("per_shard").and_then(Value::as_array)
        && !per_shard.is_empty()
    {
        out.push_str("per_shard:\n");
        for s in per_shard {
            let _ = writeln!(
                out,
                "  shard {:<4} {}",
                cell_number(s.get("shard_id")),
                cell_number(s.get("matched_count"))
            );
        }
    }

    let sample = value
        .get("sample")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let _ = writeln!(out, "sample ({} shown):", sample.len());
    let _ = writeln!(
        out,
        "  {:<38} {:<24} STATE",
        "EXECUTION_ID", "WORKFLOW_NAME"
    );
    for row in &sample {
        let _ = writeln!(
            out,
            "  {:<38} {:<24} {}",
            cell_str(row.get("execution_id")),
            cell_str(row.get("workflow_name")),
            cell_str(row.get("state")),
        );
    }
    if truncated {
        let _ = writeln!(
            out,
            "(sample truncated: {} of {matched} shown)",
            sample.len()
        );
    }
    out
}

fn workflow_reachability_wants_table(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::WorkflowTypes {
            command: WorkflowTypesCommand::Reachability { json: false, .. }
        }
    ) && cli.output == OutputFormat::PrettyJson
}

const fn workflow_reachability_wants_raw_json(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::WorkflowTypes {
            command: WorkflowTypesCommand::Reachability { json: true, .. }
        }
    )
}

/// Exit `2` when the report is unsafe to deploy against:
///
/// - `partial`/`unavailable` cross-shard status: an incomplete answer must never
///   be mistaken for "safe to remove".
/// - Any `orphaned` verdict: a handler was already removed but runs are still live.
/// - When a `--type` filter is active: also block on `in_use`. Without a filter
///   the command is a fleet-wide monitor; `in_use` is the normal state for any
///   type with running workflows and should not block. With a filter the operator
///   is asking "can I delete this specific handler?"; `in_use` means "no" — live
///   runs would become orphaned the moment the handler is removed.
///
/// Exit `0` otherwise.
fn workflow_reachability_exit_code(value: &Value) -> i32 {
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unavailable");
    if matches!(status, "partial" | "unavailable") {
        return 2;
    }
    // `filter` is the echo of the `--type` query param: present → single-type check.
    let type_filter_active = value.get("filter").and_then(Value::as_str).is_some();
    let blocking_verdict = if type_filter_active {
        // Pre-removal check: any non-safe verdict blocks.
        |v: &str| matches!(v, "orphaned" | "in_use")
    } else {
        // Fleet monitor: only already-broken (orphaned) types block.
        |v: &str| v == "orphaned"
    };
    let any_blocking = value
        .get("items")
        .and_then(Value::as_array)
        .is_some_and(|items| {
            items.iter().any(|item| {
                item.get("verdict")
                    .and_then(Value::as_str)
                    .is_some_and(blocking_verdict)
            })
        });
    if any_blocking { 2 } else { 0 }
}

fn format_workflow_reachability_table(value: &Value) -> String {
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let observed_at = value
        .get("observed_at")
        .and_then(Value::as_str)
        .unwrap_or("-");
    let Some(items) = value.get("items").and_then(Value::as_array) else {
        return format!(
            "status: {status}\nobserved_at: {observed_at}\nNo workflow types returned."
        );
    };

    let mut rows = Vec::with_capacity(items.len() + 1);
    rows.push(vec![
        "WORKFLOW_TYPE".to_string(),
        "REGISTERED".to_string(),
        "NON_TERMINAL".to_string(),
        "OLDEST_AGE_S".to_string(),
        "VERDICT".to_string(),
    ]);
    for item in items {
        rows.push(vec![
            cell_str(item.get("workflow_type")),
            bool_cell(item.get("registered")),
            cell_number(item.get("non_terminal_count")),
            cell_number(item.get("oldest_non_terminal_age_secs")),
            cell_str(item.get("verdict")),
        ]);
    }

    let table = render_table(&rows);

    let unavailable = value
        .get("shards")
        .and_then(Value::as_array)
        .map(|shards| {
            shards
                .iter()
                .filter(|shard| shard.get("status").and_then(Value::as_str) == Some("unavailable"))
                .filter_map(|shard| shard.get("shard_id").and_then(Value::as_i64))
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let footer = if unavailable.is_empty() {
        String::new()
    } else {
        format!(
            "\n\nWARNING: unavailable shards [{}] — verdicts are provisional, not safe-to-remove.",
            unavailable.join(", ")
        )
    };

    format!("status: {status}\nobserved_at: {observed_at}\n\n{table}{footer}")
}

// ─── Per-activity-type pause helpers (issue #807) ─────────────────────────────

/// `harvest activity list` renders the table by default; `--json` opts out.
///
/// Mirrors the `--json`-flag idiom the other list reads use (dlq aggregate,
/// workflow-types reachability, queue coverage) rather than the queue-pause
/// sibling, which ships no renderer at all: `queue list-paused` returns only
/// held queues, whereas this read returns the whole registered catalogue and is
/// long enough that raw JSON is unreadable mid-incident.
fn activity_list_wants_table(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Activity {
            command: ActivityCommand::List { json: false }
        }
    ) && cli.output == OutputFormat::PrettyJson
}

const fn activity_list_wants_raw_json(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Activity {
            command: ActivityCommand::List { json: true }
        }
    )
}

/// Render `GET /activities` (issue #807) as an operator-readable table.
///
/// `status` is a header line rather than a column because it qualifies every
/// row at once: on a partial read `PAUSED: no` means only "not held on the
/// shards that answered", so an operator reading a negative during an incident
/// has to see the degradation before trusting it. The per-shard detail is not
/// repeated here — `run_cli` already emits it on STDERR via
/// `fanout_partial_notice`, keeping STDOUT a single clean table.
///
/// `LOCAL` earns a column despite being niche: a local activity runs inline on
/// the workflow worker and never takes a task-queue row, so a hold on one holds
/// nothing. A `PAUSED: yes` / `LOCAL: yes` row is a no-op hold, and that is
/// invisible without the column. `REGISTERED` is the mistyped-hold signal — a
/// held name this process has no catalogue entry for is still listed, flagged
/// `no`.
///
/// `SCOPE` earns one for the same reason (issue #807 review): it is the second
/// way `PAUSED: yes` can be a lie. A fleet-wide pause that only reached some
/// shards writes rows byte-identical to a complete hold and *no* row on the
/// shards it missed, so `partial_fleet` is the only signal that part of the
/// fleet is still dispatching. Leaving it to `--json` would put the one field
/// that contradicts the headline answer behind a flag nobody reaches for
/// mid-incident. The remaining fields (`scope_shard_id`, `provenance_uniform`,
/// the per-shard `shards` array) stay `--json` territory: they matter when
/// reconciling a disagreeing hold, not when answering "is this held?".
fn format_activity_list_table(value: &Value) -> String {
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let Some(items) = value.get("activities").and_then(Value::as_array) else {
        return format!("status: {status}\nNo activities returned.");
    };
    if items.is_empty() {
        return format!("status: {status}\nNo activities found.");
    }

    let mut rows = Vec::with_capacity(items.len() + 1);
    rows.push(vec![
        "ACTIVITY".to_string(),
        "QUEUE".to_string(),
        "REGISTERED".to_string(),
        "LOCAL".to_string(),
        "PAUSED".to_string(),
        "SCOPE".to_string(),
        "HELD".to_string(),
        "REASON".to_string(),
        "PAUSED_BY".to_string(),
    ]);
    for item in items {
        rows.push(vec![
            cell_str(item.get("activity_name")),
            cell_str(item.get("queue_name")),
            bool_cell(item.get("registered")),
            bool_cell(item.get("is_local")),
            bool_cell(item.get("paused")),
            cell_str(item.get("effective_scope")),
            cell_number(item.get("held_task_count")),
            cell_str(item.get("paused_reason")),
            cell_str(item.get("paused_actor")),
        ]);
    }

    let table = render_table(&rows);

    format!("status: {status}\n\n{table}")
}

// ─── Queue coverage helpers (issue #774) ───────────────────────────────────

fn queue_coverage_wants_table(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Queue {
            command: QueueCommand::Coverage { json: false, .. }
        }
    ) && cli.output == OutputFormat::PrettyJson
}

const fn queue_coverage_wants_raw_json(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Queue {
            command: QueueCommand::Coverage { json: true, .. }
        }
    )
}

const fn queue_coverage_should_gate(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Queue {
            command: QueueCommand::Coverage { .. }
        }
    )
}

/// Exit `2` when the report is unsafe to deploy against:
///
/// - `partial`/`unavailable` cross-shard status: an incomplete answer must
///   never be mistaken for "fully covered".
/// - Any queue reported `uncovered: true`: pending work with zero live
///   pollers, the exact deploy hazard this gate exists to catch.
///
/// Exit `0` otherwise.
fn queue_coverage_exit_code(value: &Value) -> i32 {
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unavailable");
    if matches!(status, "partial" | "unavailable") {
        return 2;
    }
    let uncovered = value
        .get("uncovered")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    if uncovered { 2 } else { 0 }
}

/// "WARNING: unavailable shards [..]" footer, or empty when every shard was
/// reached. Extracted so [`format_queue_coverage_table`] stays under the
/// line-count lint.
fn queue_coverage_unavailable_footer(value: &Value) -> String {
    let unavailable = value
        .get("shards")
        .and_then(Value::as_array)
        .map(|shards| {
            shards
                .iter()
                .filter(|shard| shard.get("status").and_then(Value::as_str) == Some("unavailable"))
                .filter_map(|shard| shard.get("shard_id").and_then(Value::as_i64))
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if unavailable.is_empty() {
        String::new()
    } else {
        format!(
            "\n\nWARNING: unavailable shards [{}] — coverage is provisional, not fully verified.",
            unavailable.join(", ")
        )
    }
}

/// "NOTE: paused queues .." footer, or empty when nothing was excluded.
/// Extracted so [`format_queue_coverage_table`] stays under the line-count
/// lint.
fn queue_coverage_paused_note(value: &Value) -> String {
    let paused_uncovered = value
        .get("excluded_paused_queues")
        .and_then(Value::as_array)
        .map(|names| names.iter().filter_map(Value::as_str).collect::<Vec<_>>())
        .unwrap_or_default();
    if paused_uncovered.is_empty() {
        String::new()
    } else {
        format!(
            "\n\nNOTE: paused queues with pending work and no live poller (excluded from the count above; unpausing without adding a worker would make them uncovered immediately): {}",
            paused_uncovered.join(", ")
        )
    }
}

fn format_queue_coverage_table(value: &Value) -> String {
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let observed_at = value
        .get("observed_at")
        .and_then(Value::as_str)
        .unwrap_or("-");
    let total_uncovered = value
        .get("total_uncovered_queues")
        .and_then(Value::as_u64)
        .unwrap_or(0);

    // Computed up front so an unavailable shard is warned about on EVERY
    // return path below, even the friendly "fully covered" shortcuts — a
    // partial/unavailable report's "nothing uncovered" claim is unverified
    // for the shards it never reached, and must never read as clean.
    let footer = queue_coverage_unavailable_footer(value);

    // A paused, pollerless queue with real pending work is deliberately
    // excluded from `items`/`uncovered` (see the endpoint's docs), but must
    // still surface here — unpausing it without adding a worker would
    // immediately make it uncovered, and `--json` is the only other way to
    // see this list.
    let paused_note = queue_coverage_paused_note(value);

    let Some(items) = value.get("items").and_then(Value::as_array) else {
        return format!(
            "status: {status}\nobserved_at: {observed_at}\ntotal_uncovered_queues: {total_uncovered}\nNo uncovered queues.{footer}{paused_note}"
        );
    };
    if items.is_empty() {
        return format!(
            "status: {status}\nobserved_at: {observed_at}\ntotal_uncovered_queues: 0\nAll queues with pending work are covered.{footer}{paused_note}"
        );
    }

    let mut rows = Vec::with_capacity(items.len() + 1);
    rows.push(vec![
        "QUEUE_NAME".to_string(),
        "PENDING".to_string(),
        "SAMPLE_TASK_IDS".to_string(),
        "SAMPLE_EXECUTION_IDS".to_string(),
    ]);
    for item in items {
        let sample_tasks = item
            .get("sample_task_ids")
            .and_then(Value::as_array)
            .map(|ids| {
                ids.iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_default();
        let sample_execs = item
            .get("sample_execution_ids")
            .and_then(Value::as_array)
            .map(|ids| {
                ids.iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_default();
        rows.push(vec![
            cell_str(item.get("queue_name")),
            cell_number(item.get("pending_count")),
            sample_tasks,
            sample_execs,
        ]);
    }

    let table = render_table(&rows);

    format!(
        "status: {status}\nobserved_at: {observed_at}\ntotal_uncovered_queues: {total_uncovered}\n\n{table}{footer}{paused_note}"
    )
}

fn audit_list_wants_table(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Audit {
            command: AuditCommand::List { .. }
        }
    ) && cli.output == OutputFormat::PrettyJson
}

fn workflow_children_wants_table(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Workflow {
            command: WorkflowCommand::Children { json: false, .. }
        } if cli.output == OutputFormat::PrettyJson
    )
}

const fn workflow_children_wants_raw_json(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Workflow {
            command: WorkflowCommand::Children { json: true, .. }
        }
    )
}

fn workflow_summaries_wants_table(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Workflow {
            command: WorkflowCommand::Summaries { json: false, .. }
        } if cli.output == OutputFormat::PrettyJson
    )
}

const fn workflow_summaries_wants_raw_json(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Workflow {
            command: WorkflowCommand::Summaries { json: true, .. }
        }
    )
}

/// `workflow tree` renders an indented outline by default; `--json` opts out.
fn lineage_tree_wants_render(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Workflow {
            command: WorkflowCommand::Tree { json: false, .. }
        } if cli.output == OutputFormat::PrettyJson
    )
}

const fn lineage_tree_wants_summary(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Workflow {
            command: WorkflowCommand::Tree { summary: true, .. }
        }
    )
}

const fn lineage_tree_wants_raw_json(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Workflow {
            command: WorkflowCommand::Tree { json: true, .. }
        }
    )
}

fn run_chain_wants_table(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Workflow {
            command: WorkflowCommand::RunChain { json: false, .. }
        } if cli.output == OutputFormat::PrettyJson
    )
}

const fn run_chain_wants_raw_json(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Workflow {
            command: WorkflowCommand::RunChain { json: true, .. }
        }
    )
}

fn handoff_wants_table(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Handoff {
            command:
                HandoffCommand::List { json: false, .. }
                    | HandoffCommand::Inspect { json: false, .. }
        } if cli.output == OutputFormat::PrettyJson
    )
}

const fn handoff_wants_raw_json(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Handoff {
            command: HandoffCommand::List { json: true, .. }
                | HandoffCommand::Inspect { json: true, .. }
        }
    )
}

fn format_handoff_table(value: &Value) -> String {
    let (status, items, coverage) =
        if let Some(items) = value.get("items").and_then(Value::as_array) {
            (
                value
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown"),
                items.clone(),
                value.get("shard_coverage"),
            )
        } else if let Some(item) = value.get("item") {
            (
                value
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown"),
                vec![item.clone()],
                value.get("shard_coverage"),
            )
        } else {
            return "No external handoffs found.".to_string();
        };

    if items.is_empty() {
        return format!("status: {status}\nNo external handoffs found.");
    }

    let mut rows = Vec::with_capacity(items.len() + 1);
    rows.push(vec![
        "STATE".to_string(),
        "DEADLINE".to_string(),
        "UPDATED".to_string(),
        "WORKFLOW".to_string(),
        "EXEC ID".to_string(),
        "ACTIVITY".to_string(),
        "SHARD".to_string(),
        "TOKEN".to_string(),
    ]);
    for item in items {
        rows.push(vec![
            cell_str(item.get("state")),
            cell_str(item.get("deadline_at")),
            cell_str(item.get("updated_at")),
            cell_str(
                item.get("workflow")
                    .and_then(|workflow| workflow.get("workflow_name")),
            ),
            cell_str(
                item.get("workflow")
                    .and_then(|workflow| workflow.get("execution_id")),
            ),
            cell_str(
                item.get("activity")
                    .and_then(|activity| activity.get("activity_name")),
            ),
            cell_number(
                item.get("workflow")
                    .and_then(|workflow| workflow.get("shard_id")),
            ),
            cell_str(item.get("token")),
        ]);
    }

    let table = render_table(&rows);

    let coverage = coverage.map_or_else(String::new, handoff_coverage_summary);
    if coverage.is_empty() {
        format!("status: {status}\n\n{table}")
    } else {
        format!("status: {status}\n{coverage}\n\n{table}")
    }
}

fn handoff_coverage_summary(value: &Value) -> String {
    let unavailable = value
        .get("unavailable_shards")
        .and_then(Value::as_array)
        .map_or_else(String::new, |shards| {
            if shards.is_empty() {
                return String::new();
            }
            let cells = shards
                .iter()
                .map(|shard| {
                    let id = cell_number(shard.get("shard_id"));
                    let reason = cell_str(shard.get("reason"));
                    format!("{id}:{reason}")
                })
                .collect::<Vec<_>>()
                .join(",");
            format!("unavailable_shards: {cells}")
        });
    let inspected = shard_array_cell(value, "inspected_shards");
    if unavailable.is_empty() {
        format!("inspected_shards: {inspected}")
    } else {
        format!("inspected_shards: {inspected}\n{unavailable}")
    }
}

fn format_workflow_children_table(value: &Value) -> String {
    let Some(items) = value.get("items").and_then(Value::as_array) else {
        return "No child workflows found.".to_string();
    };
    if items.is_empty() {
        return "No child workflows found.".to_string();
    }

    let mut rows = Vec::with_capacity(items.len() + 1);
    rows.push(vec![
        "DEPTH".to_string(),
        "EXEC ID".to_string(),
        "WORKFLOW".to_string(),
        "STATUS".to_string(),
        "STARTED".to_string(),
        "COMPLETED".to_string(),
        "SHARD".to_string(),
        "ERROR".to_string(),
    ]);
    for item in items {
        rows.push(vec![
            cell_number(item.get("depth")),
            cell_str(item.get("exec_id")),
            cell_str(item.get("workflow_name")),
            cell_str(item.get("status")),
            cell_str(item.get("started_at")),
            cell_optional_str(item.get("completed_at")),
            cell_number(item.get("shard_id")),
            cell_optional_str(item.get("error_summary")),
        ]);
    }

    let mut rendered = render_table(&rows);

    if let Some(cursor) = value.get("next_cursor").and_then(Value::as_str) {
        rendered.push_str("\nnext_cursor: ");
        rendered.push_str(cursor);
    }
    rendered
}

fn format_workflow_summaries_table(value: &Value) -> String {
    let Some(items) = value.get("summaries").and_then(Value::as_array) else {
        return "No execution summaries found.".to_string();
    };
    if items.is_empty() {
        return "No execution summaries found.".to_string();
    }

    let mut rows = Vec::with_capacity(items.len() + 1);
    rows.push(vec![
        "EXEC ID".to_string(),
        "WORKFLOW".to_string(),
        "WORKFLOW ID".to_string(),
        "STATE".to_string(),
        "COMPLETED".to_string(),
        "DURATION_MS".to_string(),
        "SHARD".to_string(),
    ]);
    for item in items {
        rows.push(vec![
            cell_str(item.get("execution_id")),
            cell_str(item.get("workflow_name")),
            cell_str(item.get("workflow_id")),
            cell_str(item.get("state")),
            cell_str(item.get("completed_at")),
            cell_optional_number(item.get("duration_ms")),
            cell_number(item.get("shard_id")),
        ]);
    }

    let mut rendered = render_table(&rows);

    if let Some(cursor) = value.get("next_cursor").and_then(Value::as_str) {
        rendered.push_str("\nnext_cursor: ");
        rendered.push_str(cursor);
    }
    rendered
}

// ── Lineage tree rendering (issue #621) ──────────────────────────────────────

/// Render the recursive lineage tree as an indented outline.
///
/// Indentation *is* the topology here — a flat table would lose the parent →
/// child relationship that is the whole point of the endpoint — so each node is
/// printed one level deeper than its parent with its state and identity inline.
fn format_lineage_tree(value: &Value) -> String {
    let Some(root) = value.get("root") else {
        return "No lineage tree found.".to_string();
    };

    let mut out = String::new();
    render_lineage_node(root, 0, &mut out);

    let _ = write!(
        out,
        "\n{} node(s), max depth {}\n",
        cell_number(value.get("node_count")),
        cell_number(value.get("max_depth_reached"))
    );
    out.push_str(&format_lineage_footer(value));
    out.trim_end().to_string()
}

/// One node line plus its subtree. Depth is bounded by the server's
/// `max_depth` ceiling, so this recursion cannot blow the stack.
fn render_lineage_node(node: &Value, indent: usize, out: &mut String) {
    let pad = "  ".repeat(indent);
    let branch = if indent == 0 { "" } else { "└─ " };
    let detached = if node.get("await_mode").and_then(Value::as_str) == Some("detached") {
        let policy = node
            .get("parent_close_policy")
            .and_then(Value::as_str)
            .unwrap_or("?");
        format!(" [detached:{policy}]")
    } else {
        String::new()
    };
    let _ = writeln!(
        out,
        "{pad}{branch}{state:<12} {exec}  {name} ({wf_id}){detached}",
        state = cell_str(node.get("state")),
        exec = cell_str(node.get("execution_id")),
        name = cell_str(node.get("workflow_name")),
        wf_id = cell_str(node.get("workflow_id")),
    );

    if let Some(children) = node.get("children").and_then(Value::as_array) {
        for child in children {
            render_lineage_node(child, indent + 1, out);
        }
    }
}

/// Render the `?summary=true` per-state descendant roll-up.
fn format_lineage_summary(value: &Value) -> String {
    let mut out = String::new();
    if let Some(root) = value.get("root") {
        let _ = write!(
            out,
            "root {} {} ({}) state={}\n\n",
            cell_str(root.get("execution_id")),
            cell_str(root.get("workflow_name")),
            cell_str(root.get("workflow_id")),
            cell_str(root.get("state")),
        );
    }

    match value.get("counts").and_then(Value::as_object) {
        Some(counts) if !counts.is_empty() => {
            let width = counts.keys().map(String::len).max().unwrap_or(0);
            for (state, count) in counts {
                let _ = writeln!(
                    out,
                    "{state:<width$}  {}",
                    cell_number(Some(count)),
                    width = width
                );
            }
        }
        _ => out.push_str("(no descendant counts)\n"),
    }

    let _ = write!(
        out,
        "\n{} descendant(s), max depth {}\n",
        cell_number(value.get("total_descendants")),
        cell_number(value.get("max_depth_reached"))
    );
    out.push_str(&format_lineage_footer(value));
    out.trim_end().to_string()
}

/// Shared truncation / partial-shard footer.
///
/// A truncated or partial tree must never look complete on the terminal, so
/// both conditions are called out explicitly rather than left to the operator
/// to notice a missing subtree.
fn format_lineage_footer(value: &Value) -> String {
    let mut out = String::new();

    if value.get("truncated").and_then(Value::as_bool) == Some(true) {
        let reason = value
            .get("truncation_reason")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let _ = write!(out, "\nTRUNCATED ({reason}) — the tree is incomplete.");
        if let Some(parents) = value
            .get("truncated_parent_ids")
            .and_then(Value::as_array)
            .filter(|p| !p.is_empty())
        {
            out.push_str("\n  dropped subtrees under (re-root the call here to continue):");
            for parent in parents {
                let _ = write!(out, "\n    {}", cell_str(Some(parent)));
            }
            if value
                .get("truncated_parents_capped")
                .and_then(Value::as_bool)
                == Some(true)
            {
                out.push_str("\n    ... (list capped)");
            }
        }
        out.push('\n');
    }

    if value
        .get("status")
        .and_then(Value::as_str)
        .is_some_and(|s| s != "complete")
    {
        let _ = write!(
            out,
            "\nPARTIAL ({}) — some shards could not be read:",
            cell_str(value.get("status"))
        );
        if let Some(shards) = value.get("unavailable_shards").and_then(Value::as_array) {
            for shard in shards {
                let _ = write!(
                    out,
                    "\n  shard {}: {}",
                    cell_number(shard.get("shard_id")),
                    cell_str(shard.get("reason"))
                );
            }
        }
        out.push('\n');
    }

    // The third incompleteness signal (issue #752). Unlike the other two it
    // can fire on a tree that is otherwise entirely clean — `truncated:
    // false`, `status: complete`, `failed: 0` — because a terminal descendant
    // was collected by retention rather than dropped by a bound or an
    // unreachable shard. Printing it is what keeps the documented
    // `harvest workflow tree [--summary]` triage flow from showing an
    // apparently-healthy family that is missing a failed child.
    //
    // It is deliberately NOT phrased as a truncation: re-rooting here would
    // not surface the omitted rows, because they are no longer in
    // `harvest_workflow_executions` at all.
    if let Some(parents) = value
        .get("retained_summary_parent_ids")
        .and_then(Value::as_array)
        .filter(|p| !p.is_empty())
    {
        out.push_str(
            "\nRETENTION — descendants were collected by retention and are absent from this tree.",
        );
        out.push_str("\n  omitted under (read them with `harvest workflow summaries`):");
        for parent in parents {
            let _ = write!(out, "\n    {}", cell_str(Some(parent)));
        }
        out.push('\n');
    }

    out
}

fn format_run_chain_table(value: &Value) -> String {
    let Some(runs) = value.get("runs").and_then(Value::as_array) else {
        return "No run chain found.".to_string();
    };
    if runs.is_empty() {
        return "No run chain found.".to_string();
    }

    let mut rows = Vec::with_capacity(runs.len() + 1);
    rows.push(vec![
        "SEQ".to_string(),
        "EXEC ID".to_string(),
        "RUN ID".to_string(),
        "STATE".to_string(),
        "OUTCOME".to_string(),
        "STARTED".to_string(),
        "COMPLETED".to_string(),
        "CONTINUED TO".to_string(),
    ]);
    for run in runs {
        rows.push(vec![
            cell_number(run.get("sequence")),
            cell_str(run.get("exec_id")),
            cell_str(run.get("run_id")),
            cell_str(run.get("state")),
            cell_str(run.get("outcome")),
            cell_str(run.get("started_at")),
            cell_optional_str(run.get("completed_at")),
            cell_optional_str(run.get("continued_to_exec_id")),
        ]);
    }

    let mut rendered = render_table(&rows);

    if let Some(workflow_id) = value.get("workflow_id").and_then(Value::as_str) {
        rendered = format!("workflow_id: {workflow_id}\n{rendered}");
    }
    if value
        .get("head_unknown")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        rendered.push_str(
            "\nnote: head_unknown — the chain participates in continue-as-new but its \
             true origin could not be proven (legacy rows lacking back-links); the first \
             run shown is a best-effort head.",
        );
    }
    rendered
}

fn format_audit_table(value: &Value) -> String {
    let Some(items) = value.as_array().filter(|v| !v.is_empty()) else {
        return "No audit records found.".to_string();
    };

    let mut rows: Vec<Vec<String>> = Vec::with_capacity(items.len() + 1);
    rows.push(vec![
        "OCCURRED_AT".to_string(),
        "ACTOR".to_string(),
        "OPERATION".to_string(),
        "TARGET".to_string(),
        "STATUS".to_string(),
        "SRC".to_string(),
        "ERROR".to_string(),
    ]);
    for item in items {
        let target = match (
            item.get("target_type").and_then(Value::as_str),
            item.get("target_id").and_then(Value::as_str),
        ) {
            (Some(tt), Some(tid)) => format!("{tt}:{tid}"),
            (Some(tt), None) => tt.to_string(),
            _ => String::new(),
        };
        rows.push(vec![
            cell_str(item.get("occurred_at")),
            cell_str(item.get("actor")),
            cell_str(item.get("operation")),
            target,
            cell_str(item.get("status")),
            cell_str(item.get("source")),
            cell_optional_str(item.get("error_summary")),
        ]);
    }

    render_table(&rows)
}

/// Render rows as a column-aligned table. Each column takes the width of its
/// widest cell. Two spaces separate columns, and trailing padding on each
/// line is trimmed. Callers must pass at least one row (the header).
fn render_table(rows: &[Vec<String>]) -> String {
    let widths = (0..rows[0].len())
        .map(|col| rows.iter().map(|row| row[col].len()).max().unwrap_or(0))
        .collect::<Vec<_>>();
    rows.iter()
        .map(|row| {
            row.iter()
                .enumerate()
                .map(|(col, cell)| format!("{cell:<width$}", width = widths[col]))
                .collect::<Vec<_>>()
                .join("  ")
                .trim_end()
                .to_string()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn cell_str(value: Option<&Value>) -> String {
    value.and_then(Value::as_str).unwrap_or("").to_string()
}

fn cell_optional_str(value: Option<&Value>) -> String {
    value.and_then(Value::as_str).unwrap_or("-").to_string()
}

fn cell_number(value: Option<&Value>) -> String {
    value
        .and_then(Value::as_i64)
        .map_or_else(String::new, |number| number.to_string())
}

fn cell_optional_number(value: Option<&Value>) -> String {
    value
        .and_then(Value::as_i64)
        .map_or_else(|| "-".to_string(), |number| number.to_string())
}

fn shard_array_cell(item: &Value, field: &str) -> String {
    let Some(values) = item
        .get("shard_coverage")
        .and_then(|coverage| coverage.get(field))
        .and_then(Value::as_array)
    else {
        return "-".to_string();
    };
    if values.is_empty() {
        return "-".to_string();
    }
    values
        .iter()
        .filter_map(Value::as_i64)
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn bool_cell(value: Option<&Value>) -> String {
    match value.and_then(Value::as_bool) {
        Some(true) => "yes".to_string(),
        Some(false) => "no".to_string(),
        None => "-".to_string(),
    }
}

fn roles_cell(shard: &Value) -> String {
    let mut roles = shard
        .get("roles")
        .and_then(Value::as_array)
        .map_or_else(Vec::new, |roles| {
            roles
                .iter()
                .filter_map(Value::as_str)
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        });
    if shard
        .get("candidate")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        roles.push("candidate".to_string());
    }
    if roles.is_empty() {
        "-".to_string()
    } else {
        roles.join(",")
    }
}

fn worker_coverage_cell(shard: &Value) -> String {
    let worker_counts = shard_worker_counts_cell(shard);
    let Some(coverage) = shard.get("worker_coverage").and_then(Value::as_array) else {
        return worker_counts.unwrap_or_else(|| "-".to_string());
    };
    if coverage.is_empty() {
        return worker_counts.unwrap_or_else(|| "-".to_string());
    }
    let queue_coverage = coverage
        .iter()
        .map(|queue| {
            let name = queue.get("queue").and_then(Value::as_str).unwrap_or("?");
            let healthy = queue
                .get("healthy_active")
                .and_then(Value::as_i64)
                .unwrap_or(0);
            let ready = queue.get("ready").and_then(Value::as_bool).unwrap_or(false);
            let mark = if ready { "ok" } else { "miss" };
            format!("{name}:{healthy}/{mark}")
        })
        .collect::<Vec<_>>()
        .join(",");
    if let Some(counts) = worker_counts {
        format!("{counts} {queue_coverage}")
    } else {
        queue_coverage
    }
}

fn shard_worker_counts_cell(shard: &Value) -> Option<String> {
    let active = shard.get("active_worker_count").and_then(Value::as_i64)?;
    let stale = shard.get("stale_worker_count").and_then(Value::as_i64)?;
    Some(format!("active={active} stale={stale}"))
}

fn scheduler_cell(shard: &Value) -> String {
    let Some(scheduler) = shard.get("scheduler") else {
        return "-".to_string();
    };
    if !scheduler
        .get("enabled")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return "off".to_string();
    }
    if scheduler
        .get("ready")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        "ok".to_string()
    } else {
        "stale".to_string()
    }
}

fn blockers_cell(shard: &Value) -> String {
    let Some(reasons) = shard.get("blocking_reasons").and_then(Value::as_array) else {
        return "-".to_string();
    };
    if reasons.is_empty() {
        return "-".to_string();
    }
    reasons
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>()
        .join("; ")
}

fn shard_request(command: &ShardCommand) -> ApiRequest {
    match command {
        ShardCommand::Health {
            candidate_shard, ..
        } => candidate_shard.as_ref().map_or_else(
            || ApiRequest::get("/admin/shards/health"),
            |shard| ApiRequest::get(format!("/admin/shards/health?candidate_shard={shard}")),
        ),
        // Intercepted in `run_cli` and executed against the shard databases
        // directly (issue #964); they never reach the management API. Kept in
        // the match rather than a `_` arm so a future shard subcommand is a
        // compile error here until it declares which path it takes.
        ShardCommand::Rebalance { .. } | ShardCommand::RebalanceResume { .. } => {
            unreachable!("shard rebalance commands are dispatched in-process by run_cli")
        }
    }
}

/// Execute `harvest shard rebalance` / `shard rebalance-resume` against the
/// shard databases directly (issue #964).
#[allow(clippy::too_many_lines)] // Two sibling subcommands whose argument
// handling reads better side by side than split across helpers.
async fn run_shard_rebalance(command: &ShardCommand, actor: Option<&str>) -> Result<(), CliError> {
    use autumn_harvest::payload_codec::PayloadCodecs;
    use autumn_harvest::shard::ShardedDbPool;
    use autumn_harvest::types::ShardId;

    fn build_pool(
        targets: &[autumn_harvest::backup_verify::ShardTarget],
    ) -> Result<ShardedDbPool, CliError> {
        let default = targets.first().map_or(0, |t| t.shard_id);
        ShardedDbPool::from_dsns(
            targets
                .iter()
                .map(|t| (ShardId::new(t.shard_id), t.dsn.clone())),
            ShardId::new(default),
            4,
        )
        .map_err(|e| CliError::InvalidInput(e.to_string()))
    }

    fn require_shard(
        targets: &[autumn_harvest::backup_verify::ShardTarget],
        shard: i32,
        flag: &str,
    ) -> Result<(), CliError> {
        if targets.iter().any(|t| t.shard_id == shard) {
            return Ok(());
        }
        Err(CliError::InvalidInput(format!(
            "--{flag} names shard {shard}, but no --shard {shard}=<DSN> was supplied"
        )))
    }

    match command {
        ShardCommand::Rebalance {
            shards,
            from,
            to,
            limit,
            dry_run,
            json,
        } => {
            let targets = parse_shard_targets(shards)?;
            require_shard(&targets, *from, "from")?;
            require_shard(&targets, *to, "to")?;
            if from == to {
                return Err(CliError::InvalidInput(
                    "--from and --to must name different shards".to_string(),
                ));
            }
            let pool = build_pool(&targets)?;
            let report = autumn_harvest::shard_rebalance::migrate_quiescent_executions(
                &pool,
                ShardId::new(*from),
                ShardId::new(*to),
                *limit,
                *dry_run,
                actor.unwrap_or("anonymous"),
                &PayloadCodecs::default(),
            )
            .await
            .map_err(|e| CliError::InvalidInput(e.to_string()))?;

            if *json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&report)
                        .map_err(|e| CliError::InvalidInput(e.to_string()))?
                );
            } else {
                print!("{}", format_rebalance_report(&report));
            }
            Ok(())
        }
        ShardCommand::RebalanceResume {
            shards,
            from,
            limit,
            json,
        } => {
            let targets = parse_shard_targets(shards)?;
            require_shard(&targets, *from, "from")?;
            let pool = build_pool(&targets)?;
            let outcomes = autumn_harvest::shard_rebalance::resume_incomplete_migrations(
                &pool,
                ShardId::new(*from),
                *limit,
                actor.unwrap_or("anonymous"),
                &PayloadCodecs::default(),
            )
            .await
            .map_err(|e| CliError::InvalidInput(e.to_string()))?;

            if *json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&outcomes)
                        .map_err(|e| CliError::InvalidInput(e.to_string()))?
                );
            } else if outcomes.is_empty() {
                println!("no unfinished shard migrations on shard {from}");
            } else {
                for outcome in &outcomes {
                    println!("{}", format_rebalance_outcome(outcome));
                }
            }
            Ok(())
        }
        ShardCommand::Health { .. } => unreachable!("health goes through the management API"),
    }
}

/// One line per execution, plus a summary — the progress report AC8 asks for.
fn format_rebalance_report(
    report: &autumn_harvest::shard_rebalance::MigrationBatchReport,
) -> String {
    use std::fmt::Write as _;

    let mut out = String::new();
    let mode = if report.dry_run { " (dry run)" } else { "" };
    let _ = writeln!(
        out,
        "shard rebalance {} -> {}{mode}",
        report.source_shard.as_i32(),
        report.target_shard.as_i32()
    );
    for outcome in &report.outcomes {
        out.push_str("  ");
        out.push_str(&format_rebalance_outcome(outcome));
        out.push('\n');
    }
    let _ = writeln!(
        out,
        "\nexamined {}  migrated {}  would-migrate {}  skipped {}  aborted {}",
        report.examined,
        report.migrated(),
        report.would_migrate(),
        report.skipped(),
        report.aborted()
    );
    out
}

fn format_rebalance_outcome(outcome: &autumn_harvest::shard_rebalance::MigrationOutcome) -> String {
    use autumn_harvest::shard_rebalance::MigrationOutcome;
    match outcome {
        MigrationOutcome::Migrated {
            execution_id,
            fingerprint,
        } => format!(
            "migrated      {execution_id}  (verified {})",
            &fingerprint[..fingerprint.len().min(12)]
        ),
        MigrationOutcome::WouldMigrate { execution_id } => {
            format!("would-migrate {execution_id}")
        }
        MigrationOutcome::Skipped {
            execution_id,
            blockers,
        } => format!(
            "skipped       {execution_id}  ({})",
            blockers
                .iter()
                .map(|b| b.describe())
                .collect::<Vec<_>>()
                .join("; ")
        ),
        MigrationOutcome::Aborted {
            execution_id,
            reason,
        } => format!("aborted       {execution_id}  ({reason})"),
    }
}

fn canary_request(
    sample_size: usize,
    workflow_name: Option<&str>,
    queue: Option<&str>,
) -> ApiRequest {
    ApiRequest::post(
        "/admin/workflows/replay-canary",
        Some(json!({
            "sample_size": sample_size,
            "workflow_name": workflow_name,
            "queue_name": queue,
        })),
    )
}

/// Build `GET /workflows/{id}/logs` (issue #790).
///
/// `--level` is repeatable AND comma-separable; each value is sent as its own
/// `level=` param (the server accepts both forms). Values are passed through
/// verbatim so an invalid level is rejected by the server with a clear 400
/// rather than being silently dropped here — a typo must never look like
/// "this run logged nothing".
fn workflow_logs_request(
    execution_id: &str,
    levels: &[String],
    limit: Option<i64>,
    cursor: Option<i64>,
    since: Option<&str>,
) -> ApiRequest {
    let mut params: Vec<(&'static str, String)> = Vec::new();
    for raw in levels {
        for part in raw.split(',') {
            let part = part.trim();
            if !part.is_empty() {
                params.push(("level", part.to_string()));
            }
        }
    }
    if let Some(value) = limit {
        params.push(("limit", value.to_string()));
    }
    if let Some(value) = cursor {
        params.push(("cursor", value.to_string()));
    }
    if let Some(value) = since {
        params.push(("since", value.to_string()));
    }
    let path = format!("/workflows/{}/logs", path_segment(execution_id));
    if params.is_empty() {
        return ApiRequest::get(path);
    }
    let encoded = params
        .iter()
        .map(|(key, value)| format!("{key}={}", query_encode(value)))
        .collect::<Vec<_>>()
        .join("&");
    ApiRequest::get(format!("{path}?{encoded}"))
}

fn usage_request(from: &str, to: &str, group_by: Option<&str>) -> ApiRequest {
    let mut params: Vec<(&'static str, String)> =
        vec![("from", from.to_string()), ("to", to.to_string())];
    if let Some(value) = group_by {
        params.push(("group_by", value.to_string()));
    }
    let encoded = params
        .iter()
        .map(|(key, value)| format!("{key}={}", query_encode(value)))
        .collect::<Vec<_>>()
        .join("&");
    ApiRequest::get(format!("/admin/usage?{encoded}"))
}

fn version_usage_request(
    workflow_name: Option<&str>,
    change_id: Option<&str>,
    recorded_version: Option<u32>,
    state_group: Option<VersionUsageStateGroup>,
    shard_id: Option<i32>,
    guard: bool,
) -> ApiRequest {
    let state_group = if guard {
        VersionUsageStateGroup::Active
    } else {
        state_group.unwrap_or(VersionUsageStateGroup::All)
    };
    let mut params: Vec<(&'static str, String)> = Vec::new();
    if let Some(value) = workflow_name {
        params.push(("workflow_name", value.to_string()));
    }
    if let Some(value) = change_id {
        params.push(("change_id", value.to_string()));
    }
    if let Some(value) = recorded_version {
        params.push(("recorded_version", value.to_string()));
    }
    if state_group != VersionUsageStateGroup::All {
        params.push(("state_group", state_group.as_wire().to_string()));
    }
    if let Some(value) = shard_id {
        params.push(("shard_id", value.to_string()));
    }

    if params.is_empty() {
        return ApiRequest::get("/admin/version-gates/usage");
    }
    let encoded = params
        .iter()
        .map(|(key, value)| format!("{key}={}", query_encode(value)))
        .collect::<Vec<_>>()
        .join("&");
    ApiRequest::get(format!("/admin/version-gates/usage?{encoded}"))
}

#[allow(clippy::too_many_lines)]
fn workflow_request(command: &WorkflowCommand) -> Result<ApiRequest, CliError> {
    match command {
        WorkflowCommand::List {
            limit,
            state,
            workflow_name,
            search_attr,
            search_attr_filter,
            owner,
            no_progress_minutes,
            include_sleeping,
            history_bloat_min_events,
            start_source,
        } => Ok(ApiRequest::get(build_workflow_list_path(
            *limit,
            state,
            workflow_name.as_deref(),
            search_attr,
            search_attr_filter,
            owner.as_deref(),
            *no_progress_minutes,
            *include_sleeping,
            *history_bloat_min_events,
            start_source.as_deref(),
        )?)),
        WorkflowCommand::Summaries {
            workflow_name,
            workflow_id,
            state,
            completed_after,
            completed_before,
            search_attr,
            limit,
            cursor,
            order,
            json: _,
        } => Ok(ApiRequest::get(build_summary_list_path(
            workflow_name.as_deref(),
            workflow_id.as_deref(),
            state,
            completed_after.as_deref(),
            completed_before.as_deref(),
            search_attr,
            *limit,
            cursor.as_deref(),
            order.as_deref(),
        )?)),
        WorkflowCommand::Get { execution_id } => Ok(ApiRequest::get(format!(
            "/workflows/{}",
            path_segment(execution_id)
        ))),
        WorkflowCommand::Stack { execution_id } => Ok(ApiRequest::get(format!(
            "/workflows/{}/stack",
            path_segment(execution_id)
        ))),
        WorkflowCommand::Timeline { execution_id } => Ok(ApiRequest::get(format!(
            "/workflows/{}/timeline",
            path_segment(execution_id)
        ))),
        WorkflowCommand::Logs {
            execution_id,
            level,
            limit,
            cursor,
            since,
        } => Ok(workflow_logs_request(
            execution_id,
            level,
            *limit,
            *cursor,
            since.as_deref(),
        )),
        WorkflowCommand::Awaitables { execution_id } => Ok(ApiRequest::get(format!(
            "/workflows/{}/awaitables",
            path_segment(execution_id)
        ))),
        WorkflowCommand::Diagnose {
            execution_id,
            json: _,
        } => Ok(ApiRequest::get(format!(
            "/workflows/{}/diagnose",
            path_segment(execution_id)
        ))),
        WorkflowCommand::Tree {
            execution_id,
            summary,
            max_depth,
            max_nodes,
            json: _,
        } => {
            let mut path = format!("/workflows/{}/tree", path_segment(execution_id));
            let mut params: Vec<String> = Vec::new();
            if *summary {
                params.push("summary=true".to_string());
            }
            if let Some(depth) = max_depth {
                params.push(format!("max_depth={depth}"));
            }
            if let Some(nodes) = max_nodes {
                params.push(format!("max_nodes={nodes}"));
            }
            if !params.is_empty() {
                path.push('?');
                path.push_str(&params.join("&"));
            }
            Ok(ApiRequest::get(path))
        }
        WorkflowCommand::RunChain {
            execution_id,
            json: _,
        } => Ok(ApiRequest::get(format!(
            "/workflows/{}/run-chain",
            path_segment(execution_id)
        ))),
        WorkflowCommand::ReplayDiagnosis { execution_id } => Ok(ApiRequest::post(
            format!("/workflows/{}/replay-diagnosis", path_segment(execution_id)),
            None,
        )),
        WorkflowCommand::Children {
            execution_id,
            status,
            workflow_name,
            limit,
            cursor,
            depth,
            json: _,
        } => Ok(ApiRequest::get(build_workflow_children_path(
            execution_id,
            status,
            workflow_name.as_deref(),
            *limit,
            cursor.as_deref(),
            *depth,
        ))),
        WorkflowCommand::Start {
            workflow_name,
            workflow_id,
            queue,
            input_json,
            input_file,
            memo_json,
            memo_file,
            search_attrs_json,
            search_attrs_file,
            execution_timeout_secs,
            reuse_policy,
            conflict_policy,
            start_at,
            delay,
            shard_id,
            residency_key,
        } => {
            let mut body = Map::new();
            insert_string(&mut body, "workflow_id", workflow_id.as_deref());
            insert_string(&mut body, "queue", queue.as_deref());
            insert_json(
                &mut body,
                "input",
                parse_json_source(
                    input_json.as_deref(),
                    input_file.as_deref(),
                    "workflow input",
                )?,
            );
            insert_json(
                &mut body,
                "memo",
                parse_json_source(memo_json.as_deref(), memo_file.as_deref(), "memo")?,
            );
            insert_json(
                &mut body,
                "search_attrs",
                parse_json_source(
                    search_attrs_json.as_deref(),
                    search_attrs_file.as_deref(),
                    "search attributes",
                )?,
            );
            if let Some(timeout) = execution_timeout_secs {
                body.insert("execution_timeout_secs".to_string(), json!(timeout));
            }
            insert_string(&mut body, "reuse_policy", reuse_policy.as_deref());
            insert_string(&mut body, "conflict_policy", conflict_policy.as_deref());
            insert_string(&mut body, "start_at", start_at.as_deref());
            insert_string(&mut body, "delay", delay.as_deref());
            // issue #697: explicit shard placement. Omitting a flag omits the
            // key entirely, so an unpinned start's body is byte-identical to a
            // pre-#697 CLI. Clap enforces the mutual exclusion.
            if let Some(shard) = shard_id {
                body.insert("shard_id".to_string(), json!(shard));
            }
            insert_string(&mut body, "residency_key", residency_key.as_deref());

            Ok(ApiRequest::post(
                format!("/workflows/{}/start", path_segment(workflow_name)),
                Some(Value::Object(body)),
            ))
        }
        WorkflowCommand::Cancel {
            execution_id,
            reason,
        } => {
            let mut body = Map::new();
            insert_string(&mut body, "reason", reason.as_deref());
            Ok(ApiRequest::post(
                format!("/workflows/{}/cancel", path_segment(execution_id)),
                Some(Value::Object(body)),
            ))
        }
        WorkflowCommand::Pause {
            execution_id,
            reason,
        } => {
            let mut body = Map::new();
            insert_string(&mut body, "reason", reason.as_deref());
            Ok(ApiRequest::post(
                format!("/workflows/{}/pause", path_segment(execution_id)),
                Some(Value::Object(body)),
            ))
        }
        WorkflowCommand::Resume { execution_id } => Ok(ApiRequest::post(
            format!("/workflows/{}/resume", path_segment(execution_id)),
            None,
        )),
        WorkflowCommand::Annotate {
            execution_id,
            owner,
            clear_owner,
            severity,
            clear_severity,
            note,
            clear_note,
        } => {
            let mut body = Map::new();
            // Tri-state nullable fields: --clear-* sends an explicit JSON
            // null; clap's `conflicts_with` prevents setting and clearing
            // the same field in one call.
            if let Some(v) = owner {
                body.insert("owner".to_string(), Value::String(v.clone()));
            } else if *clear_owner {
                body.insert("owner".to_string(), Value::Null);
            }
            if let Some(v) = severity {
                body.insert("severity".to_string(), Value::String(v.clone()));
            } else if *clear_severity {
                body.insert("severity".to_string(), Value::Null);
            }
            if let Some(v) = note {
                body.insert("note".to_string(), Value::String(v.clone()));
            } else if *clear_note {
                body.insert("note".to_string(), Value::Null);
            }
            Ok(ApiRequest::patch(
                format!("/workflows/{}/triage", path_segment(execution_id)),
                Value::Object(body),
            ))
        }
        WorkflowCommand::ErasePayloads {
            execution_id,
            reason,
        } => {
            let mut body = Map::new();
            insert_string(&mut body, "reason", reason.as_deref());
            Ok(ApiRequest::post(
                format!("/workflows/{}/erase-payloads", path_segment(execution_id)),
                Some(Value::Object(body)),
            ))
        }
        WorkflowCommand::RetryActivity {
            workflow_id,
            activity_exec_id,
        } => Ok(ApiRequest::post(
            format!(
                "/workflows/{}/activities/{}/retry-now",
                path_segment(workflow_id),
                path_segment(activity_exec_id)
            ),
            None,
        )),
        WorkflowCommand::FailActivity {
            workflow_id,
            activity_exec_id,
            reason,
        } => {
            let mut body = Map::new();
            insert_string(&mut body, "reason", reason.as_deref());
            Ok(ApiRequest::post(
                format!(
                    "/workflows/{}/activities/{}/fail-now",
                    path_segment(workflow_id),
                    path_segment(activity_exec_id)
                ),
                Some(Value::Object(body)),
            ))
        }
        WorkflowCommand::Reset {
            execution_id,
            reset_to_event_id,
            reason,
            operator_id,
            signal_reapply,
            dry_run,
        } => {
            let mut body = Map::new();
            body.insert("reset_to_event_id".to_string(), json!(reset_to_event_id));
            body.insert("reason".to_string(), Value::String(reason.clone()));
            body.insert(
                "operator_id".to_string(),
                Value::String(operator_id.clone()),
            );
            body.insert(
                "signal_reapply".to_string(),
                Value::String(signal_reapply.as_wire().to_string()),
            );
            let suffix = if *dry_run { "?dry_run=true" } else { "" };
            Ok(ApiRequest::post(
                format!("/workflows/{}/reset{suffix}", path_segment(execution_id)),
                Some(Value::Object(body)),
            ))
        }
        WorkflowCommand::Rerun {
            execution_id,
            input_json,
            input_file,
            workflow_id,
        } => {
            let mut body = Map::new();
            // `input` is TRI-STATE server-side: absent = clone the source's
            // stored input verbatim, explicit JSON null = override with null.
            // So the key is inserted ONLY when the operator actually supplied
            // one — never defaulted to null to mean "unset".
            if let Some(input) =
                parse_json_source(input_json.as_deref(), input_file.as_deref(), "rerun input")?
            {
                body.insert("input".to_string(), input);
            }
            insert_string(&mut body, "workflow_id", workflow_id.as_deref());
            Ok(ApiRequest::post(
                format!("/workflows/{}/rerun", path_segment(execution_id)),
                Some(Value::Object(body)),
            ))
        }
        WorkflowCommand::Signal {
            execution_id,
            signal_name,
            payload_json,
            payload_file,
            idempotency_key,
        } => {
            let payload = parse_json_source(
                payload_json.as_deref(),
                payload_file.as_deref(),
                "signal payload",
            )?
            .unwrap_or_else(|| json!({}));
            // The exactly-once delivery key rides the ?idempotency_key= query
            // param (issue #521's out-of-band surface) — the request body must
            // stay the raw signal payload, so the key is never smuggled into it.
            let suffix = idempotency_key
                .as_deref()
                .map(|key| format!("?idempotency_key={}", query_encode(key)))
                .unwrap_or_default();
            Ok(ApiRequest::post(
                format!(
                    "/workflows/{}/signal/{}{suffix}",
                    path_segment(execution_id),
                    path_segment(signal_name)
                ),
                Some(payload),
            ))
        }
        WorkflowCommand::Query {
            execution_id,
            query_name,
        } => Ok(ApiRequest::get(format!(
            "/workflows/{}/query/{}",
            path_segment(execution_id),
            path_segment(query_name)
        ))),
        WorkflowCommand::Update {
            execution_id,
            update_name,
            input_json,
            input_file,
            wait,
            timeout_secs,
        } => {
            let input =
                parse_json_source(input_json.as_deref(), input_file.as_deref(), "update input")?
                    .unwrap_or(serde_json::Value::Null);
            let path = timeout_secs.map_or_else(
                || {
                    format!(
                        "/workflows/{}/update/{}?wait={}",
                        path_segment(execution_id),
                        path_segment(update_name),
                        wait,
                    )
                },
                |secs| {
                    format!(
                        "/workflows/{}/update/{}?wait={}&timeout_secs={secs}",
                        path_segment(execution_id),
                        path_segment(update_name),
                        wait,
                    )
                },
            );
            Ok(ApiRequest::post(path, Some(json!({ "input": input }))))
        }
        WorkflowCommand::UpdateResult {
            execution_id,
            update_id,
        } => Ok(ApiRequest::get(format!(
            "/workflows/{}/update/{}/result",
            path_segment(execution_id),
            path_segment(update_id),
        ))),
        WorkflowCommand::Handlers { workflow_name } => Ok(ApiRequest::get(format!(
            "/workflows/types/{}/handlers",
            path_segment(workflow_name),
        ))),
        WorkflowCommand::BatchReset {
            filter_json,
            filter_file,
            event_id,
            first_activity,
            last_workflow_task,
            reason,
            operator_id,
            signal_reapply,
            preview,
        } => {
            let filter = parse_json_source(
                filter_json.as_deref(),
                filter_file.as_deref(),
                "batch reset filter",
            )?
            .ok_or_else(|| {
                CliError::InvalidInput(
                    "one of --filter-json or --filter-file is required".to_string(),
                )
            })?;
            let reset_point = if let Some(id) = event_id {
                json!({"type": "event_id", "event_id": id})
            } else if let Some(name) = first_activity {
                json!({"type": "first_activity_run", "activity_name": name})
            } else if *last_workflow_task {
                json!({"type": "last_workflow_task"})
            } else {
                return Err(CliError::InvalidInput(
                    "one of --event-id, --first-activity, or --last-workflow-task is required"
                        .to_string(),
                ));
            };
            let mut body = serde_json::Map::new();
            body.insert("filter".to_string(), filter);
            body.insert("reset_point".to_string(), reset_point);
            body.insert("reason".to_string(), Value::String(reason.clone()));
            body.insert(
                "operator_id".to_string(),
                Value::String(operator_id.clone()),
            );
            body.insert(
                "signal_reapply".to_string(),
                Value::String(signal_reapply.as_wire().to_string()),
            );
            body.insert("preview".to_string(), Value::Bool(*preview));
            Ok(ApiRequest::post(
                "/workflows/batch_reset".to_string(),
                Some(Value::Object(body)),
            ))
        }
    }
}

fn history_request(command: &HistoryCommand) -> ApiRequest {
    match command {
        HistoryCommand::Export {
            execution_id,
            payload_policy,
            max_bytes,
            output_file: _,
        } => {
            let mut params = vec![("payload_policy", payload_policy.as_wire().to_string())];
            if let Some(value) = max_bytes {
                params.push(("max_bytes", value.to_string()));
            }
            ApiRequest::get(format!(
                "/workflows/{}/history/export?{}",
                path_segment(execution_id),
                encode_query_params(&params)
            ))
        }
        HistoryCommand::ExportBatch {
            workflow_name,
            state_group,
            updated_after,
            updated_before,
            shard_id,
            limit,
            payload_policy,
            max_bytes,
            output_file: _,
        } => {
            let mut params: Vec<(&'static str, String)> = Vec::new();
            if let Some(value) = workflow_name {
                params.push(("workflow_name", value.clone()));
            }
            if let Some(value) = state_group {
                params.push(("state_group", value.as_wire().to_string()));
            }
            if let Some(value) = updated_after {
                params.push(("updated_after", value.clone()));
            }
            if let Some(value) = updated_before {
                params.push(("updated_before", value.clone()));
            }
            if let Some(value) = shard_id {
                params.push(("shard_id", value.to_string()));
            }
            if let Some(value) = limit {
                params.push(("limit", value.to_string()));
            }
            params.push(("payload_policy", payload_policy.as_wire().to_string()));
            if let Some(value) = max_bytes {
                params.push(("max_bytes", value.to_string()));
            }

            ApiRequest::get(format!(
                "/admin/history/exports?{}",
                encode_query_params(&params)
            ))
        }
        HistoryCommand::ExportSample {
            output_dir: _,
            per_workflow,
            states,
            workflow_name,
            order,
            shard_id,
            payload_policy,
            max_bytes,
        } => {
            let mut params: Vec<(&'static str, String)> = Vec::new();
            if let Some(value) = workflow_name {
                params.push(("workflow_name", value.clone()));
            }
            // Repeatable AND comma-separated: `--states RUNNING --states PAUSED`
            // and `--states RUNNING,PAUSED` must mean the same thing, so a CI
            // recipe copied from either idiom samples the same population.
            let joined = states
                .iter()
                .flat_map(|value| value.split(','))
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>()
                .join(",");
            if !joined.is_empty() {
                params.push(("states", joined));
            }
            // Always sent: the effective per-type cap and payload policy are the
            // two knobs that decide what the gate actually verified, so they are
            // explicit on the wire rather than left to a server-side default the
            // operator cannot see in the request log.
            params.push(("per_workflow", per_workflow.to_string()));
            params.push(("order", order.as_wire().to_string()));
            if let Some(value) = shard_id {
                params.push(("shard_id", value.to_string()));
            }
            params.push(("payload_policy", payload_policy.as_wire().to_string()));
            if let Some(value) = max_bytes {
                params.push(("max_bytes", value.to_string()));
            }

            ApiRequest::get(format!(
                "/admin/history/export-sample?{}",
                encode_query_params(&params)
            ))
        }
    }
}

fn handoff_request(command: &HandoffCommand) -> Result<ApiRequest, CliError> {
    match command {
        HandoffCommand::List {
            state,
            workflow_name,
            execution_id,
            activity_name,
            token,
            shard_id,
            due_before,
            updated_before,
            limit,
            json: _,
        } => Ok(ApiRequest::get(build_handoff_list_path(
            state,
            workflow_name.as_deref(),
            execution_id.as_deref(),
            activity_name.as_deref(),
            token.as_deref(),
            *shard_id,
            due_before.as_deref(),
            updated_before.as_deref(),
            *limit,
        ))),
        HandoffCommand::Inspect { token, json: _ } => Ok(ApiRequest::get(format!(
            "/admin/external-handoffs/{}",
            path_segment(token)
        ))),
        HandoffCommand::Complete {
            token,
            output_json,
            output_file,
            request_json,
            request_file,
        } => complete_handoff_request(
            token,
            output_json.as_deref(),
            output_file.as_deref(),
            request_json.as_deref(),
            request_file.as_deref(),
        ),
        HandoffCommand::Fail {
            token,
            error,
            error_json,
            error_file,
            request_json,
            request_file,
            retryable,
        } => fail_handoff_request(
            token,
            error.as_deref(),
            error_json.as_deref(),
            error_file.as_deref(),
            request_json.as_deref(),
            request_file.as_deref(),
            *retryable,
        ),
        HandoffCommand::Heartbeat {
            token,
            extend_by_secs,
            request_json,
            request_file,
        } => heartbeat_handoff_request(
            token,
            *extend_by_secs,
            request_json.as_deref(),
            request_file.as_deref(),
        ),
    }
}

fn complete_handoff_request(
    token: &str,
    output_json: Option<&str>,
    output_file: Option<&Path>,
    request_json: Option<&str>,
    request_file: Option<&Path>,
) -> Result<ApiRequest, CliError> {
    let request_body = parse_json_source(request_json, request_file, "handoff complete request")?;
    let body = if let Some(request_body) = request_body {
        request_body
    } else {
        let output = parse_json_source(output_json, output_file, "handoff completion output")?
            .unwrap_or(Value::Null);
        json!({ "output": output })
    };
    Ok(ApiRequest::post(
        format!("/activities/external/{}/complete", path_segment(token)),
        Some(body),
    ))
}

fn fail_handoff_request(
    token: &str,
    error: Option<&str>,
    error_json: Option<&str>,
    error_file: Option<&Path>,
    request_json: Option<&str>,
    request_file: Option<&Path>,
    retryable: bool,
) -> Result<ApiRequest, CliError> {
    let request = parse_json_source(request_json, request_file, "handoff fail request")?;
    let body = if let Some(request) = request {
        request
    } else {
        let error_value = parse_json_source(error_json, error_file, "handoff error")?;
        let error = error_value.map_or_else(
            || error.unwrap_or("external handoff failed").to_string(),
            stringify_handoff_error,
        );
        json!({ "error": error, "retryable": retryable })
    };
    Ok(ApiRequest::post(
        format!("/activities/external/{}/fail", path_segment(token)),
        Some(body),
    ))
}

fn stringify_handoff_error(value: Value) -> String {
    match value {
        Value::String(raw) => raw,
        other => serde_json::to_string(&other).unwrap_or_else(|_| other.to_string()),
    }
}

fn heartbeat_handoff_request(
    token: &str,
    extend_by_secs: Option<u64>,
    request_json: Option<&str>,
    request_file: Option<&Path>,
) -> Result<ApiRequest, CliError> {
    let body = parse_json_source(request_json, request_file, "handoff heartbeat request")?
        .unwrap_or_else(|| {
            let mut body = Map::new();
            if let Some(secs) = extend_by_secs {
                body.insert("extend_by_secs".to_string(), json!(secs));
            }
            Value::Object(body)
        });
    Ok(ApiRequest::post(
        format!("/activities/external/{}/heartbeat", path_segment(token)),
        Some(body),
    ))
}

fn dag_request(command: &DagCommand, actor: Option<&str>) -> Result<ApiRequest, CliError> {
    match command {
        DagCommand::List => Ok(ApiRequest::get("/dags")),
        DagCommand::Runs { dag_name } => Ok(ApiRequest::get(format!(
            "/dags/{}/runs",
            path_segment(dag_name)
        ))),
        DagCommand::Trigger {
            dag_name,
            conf_json,
            conf_file,
        } => {
            let mut body = Map::new();
            insert_json(
                &mut body,
                "conf",
                parse_json_source(conf_json.as_deref(), conf_file.as_deref(), "DAG run config")?,
            );
            Ok(ApiRequest::post(
                format!("/dags/{}/trigger", path_segment(dag_name)),
                Some(Value::Object(body)),
            ))
        }
        DagCommand::Pause { dag_name } => Ok(ApiRequest::patch(
            format!("/dags/{}", path_segment(dag_name)),
            json!({ "paused": true }),
        )),
        DagCommand::Unpause { dag_name } => Ok(ApiRequest::patch(
            format!("/dags/{}", path_segment(dag_name)),
            json!({ "paused": false }),
        )),
        DagCommand::Retry {
            dag_name,
            run_exec_id,
            from_node,
            reason,
            operator_id,
            dry_run,
        } => {
            let operator = operator_id
                .as_deref()
                .or(actor)
                .unwrap_or("cli")
                .to_string();
            Ok(ApiRequest::post(
                format!(
                    "/dags/{}/runs/{}/retry",
                    path_segment(dag_name),
                    path_segment(run_exec_id)
                ),
                Some(json!({
                    "from_nodes": from_node,
                    "reason": reason,
                    "operator_id": operator,
                    "dry_run": dry_run,
                })),
            ))
        }
    }
}

#[allow(clippy::too_many_lines)]
fn schedule_request(command: &ScheduleCommand) -> Result<ApiRequest, CliError> {
    match command {
        ScheduleCommand::List => Ok(ApiRequest::get("/admin/schedules")),
        ScheduleCommand::Backfill {
            id,
            from,
            to,
            dry_run,
            max_count,
            include_paused,
        } => {
            let mut body = Map::new();
            body.insert("from".to_string(), Value::String(from.clone()));
            body.insert("to".to_string(), Value::String(to.clone()));
            body.insert("dry_run".to_string(), json!(dry_run));
            body.insert("include_paused".to_string(), json!(include_paused));
            if let Some(count) = max_count {
                body.insert("max_count".to_string(), json!(count));
            }
            Ok(ApiRequest::post(
                format!("/admin/schedules/{}/backfill", path_segment(id)),
                Some(Value::Object(body)),
            ))
        }
        ScheduleCommand::CreateWorkflow {
            name,
            cron,
            input_json,
            input_file,
            max_active_runs,
            catchup,
            paused,
        } => {
            let mut body = Map::new();
            body.insert("workflow_name".to_string(), Value::String(name.clone()));
            body.insert("schedule_expr".to_string(), Value::String(cron.clone()));
            body.insert("max_active_runs".to_string(), json!(max_active_runs));
            body.insert("catchup".to_string(), json!(catchup));
            body.insert("paused".to_string(), json!(paused));
            if let Some(input) =
                parse_json_source(input_json.as_deref(), input_file.as_deref(), "input")?
            {
                body.insert("input".to_string(), input);
            }
            Ok(ApiRequest::post(
                "/admin/schedules/workflow",
                Some(Value::Object(body)),
            ))
        }
        ScheduleCommand::Update {
            id,
            cron,
            interval_secs,
            manual,
            tz,
            input_json,
            queue,
            overlap_policy,
            buffer_all_max,
            catchup_policy,
            catchup_window_secs,
            jitter_secs,
            max_active_runs,
            calendar,
            clear_calendar,
            end_at,
            clear_end_at,
            max_runs,
            clear_max_runs,
        } => {
            let mut body = Map::new();
            // Exactly one of --cron / --interval-secs / --manual (clap enforces
            // the mutual exclusion); all optional — omitting keeps the cadence.
            if let Some(expr) = cron {
                body.insert("schedule_expr".to_string(), Value::String(expr.clone()));
            } else if let Some(secs) = interval_secs {
                body.insert(
                    "schedule_expr".to_string(),
                    Value::String(format!("interval:{secs}")),
                );
            } else if *manual {
                body.insert("schedule_expr".to_string(), Value::String("manual".into()));
            }
            if let Some(tz) = tz {
                body.insert("timezone".to_string(), Value::String(tz.clone()));
            }
            if let Some(input) = parse_json_source(input_json.as_deref(), None, "input")? {
                body.insert("input".to_string(), input);
            }
            if let Some(queue) = queue {
                body.insert("queue_name".to_string(), Value::String(queue.clone()));
            }
            if let Some(policy) = overlap_policy {
                body.insert("overlap_policy".to_string(), Value::String(policy.clone()));
            }
            if let Some(max) = buffer_all_max {
                body.insert("buffer_all_max".to_string(), json!(max));
            }
            if let Some(policy) = catchup_policy {
                body.insert("catchup_policy".to_string(), Value::String(policy.clone()));
            }
            if let Some(secs) = catchup_window_secs {
                body.insert("catchup_window_secs".to_string(), json!(secs));
            }
            if let Some(secs) = jitter_secs {
                body.insert("jitter_secs".to_string(), json!(secs));
            }
            if let Some(max) = max_active_runs {
                body.insert("max_active_runs".to_string(), json!(max));
            }
            // Tri-state nullable fields: --clear-* sends an explicit JSON null.
            if let Some(name) = calendar {
                body.insert("calendar".to_string(), Value::String(name.clone()));
            } else if *clear_calendar {
                body.insert("calendar".to_string(), Value::Null);
            }
            if let Some(ts) = end_at {
                body.insert("end_at".to_string(), Value::String(ts.clone()));
            } else if *clear_end_at {
                body.insert("end_at".to_string(), Value::Null);
            }
            if let Some(max) = max_runs {
                body.insert("max_runs".to_string(), json!(max));
            } else if *clear_max_runs {
                body.insert("max_runs".to_string(), Value::Null);
            }
            Ok(ApiRequest {
                method: ApiMethod::Patch,
                path: format!("/admin/schedules/{}", path_segment(id)),
                body: Some(Value::Object(body)),
            })
        }
        ScheduleCommand::Pause { id } => Ok(ApiRequest::post(
            format!("/admin/schedules/{}/pause", path_segment(id)),
            None,
        )),
        ScheduleCommand::Resume { id } => Ok(ApiRequest::post(
            format!("/admin/schedules/{}/resume", path_segment(id)),
            None,
        )),
        ScheduleCommand::Delete { id } => {
            // DELETE uses its own `ApiMethod::Delete` variant rather than a
            // POST to a `/delete` path, so the request carries the verb the
            // admin API actually expects.
            Ok(ApiRequest {
                method: ApiMethod::Delete,
                path: format!("/admin/schedules/{}", path_segment(id)),
                body: None,
            })
        }
        ScheduleCommand::TriggerNow { id, reason, force } => {
            let mut body = serde_json::Map::new();
            if let Some(r) = reason {
                body.insert("reason".to_string(), Value::String(r.clone()));
            }
            let mut path = format!("/admin/schedules/{}/trigger", path_segment(id));
            if *force {
                path.push_str("?force=true");
            }
            Ok(ApiRequest::post(path, Some(Value::Object(body))))
        }
        ScheduleCommand::Runs {
            id,
            state,
            origin,
            since,
            until,
            limit,
            cursor,
        } => {
            let mut params: Vec<(&str, String)> = Vec::new();
            for s in state {
                params.push(("state", s.clone()));
            }
            for o in origin {
                params.push(("origin", o.clone()));
            }
            if let Some(v) = since {
                params.push(("since", v.clone()));
            }
            if let Some(v) = until {
                params.push(("until", v.clone()));
            }
            if let Some(v) = limit {
                params.push(("limit", v.to_string()));
            }
            if let Some(v) = cursor {
                params.push(("cursor", v.clone()));
            }
            let mut path = format!("/admin/schedules/{}/runs", path_segment(id));
            if !params.is_empty() {
                path.push('?');
                path.push_str(&encode_query_params(&params));
            }
            Ok(ApiRequest::get(path))
        }
    }
}

fn retention_request(command: &RetentionCommand) -> ApiRequest {
    match command {
        RetentionCommand::Status => ApiRequest::get("/admin/retention"),
        RetentionCommand::RunNow => ApiRequest::post("/admin/retention/run-now", None),
    }
}

fn concurrency_request(command: &ConcurrencyCommand) -> ApiRequest {
    match command {
        ConcurrencyCommand::Status => ApiRequest::get("/admin/concurrency"),
    }
}

fn legal_hold_request(command: &LegalHoldCommand) -> ApiRequest {
    match command {
        LegalHoldCommand::Set {
            execution_id,
            reason,
            until,
        } => {
            let mut body = Map::new();
            insert_string(&mut body, "reason", Some(reason.as_str()));
            insert_string(&mut body, "hold_until", until.as_deref());
            ApiRequest::post(
                format!("/workflows/{}/legal-hold", path_segment(execution_id)),
                Some(Value::Object(body)),
            )
        }
        LegalHoldCommand::Release { execution_id } => ApiRequest::post(
            format!(
                "/workflows/{}/legal-hold/release",
                path_segment(execution_id)
            ),
            None,
        ),
    }
}

/// True when the command is a queue pause/resume, whose response carries a
/// partial-application contract worth gating the exit code on.
///
/// `list-paused` is a read with no such contract, so it is deliberately excluded.
const fn queue_mutation_should_gate(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Queue {
            command: QueueCommand::Pause { .. } | QueueCommand::Resume { .. }
        }
    )
}

/// Exit code for a queue pause/resume response.
///
/// A `207` partial fleet-wide hold is NOT in effect on the shards it missed --
/// those keep dispatching into exactly the outage the operator is riding out --
/// so it must never look like success to a script or a runbook step. `execute`
/// only rejects non-2xx statuses, and `207` IS 2xx, hence this body-level gate
/// (the same shape as `preflight_exit_code` and friends).
///
/// Fails closed: a body carrying neither signal is not a queue-mutation response
/// we can vouch for, so it is reported as a failure rather than as a hold that
/// may not hold.
fn queue_mutation_exit_code(value: &Value) -> i32 {
    let ok = value.get("ok").and_then(Value::as_bool);
    let complete = value.get("status").and_then(Value::as_str) == Some("complete");
    i32::from(!(ok == Some(true) && complete))
}

/// True when `raw` names a WHATWG URL dot-segment.
///
/// The URL parser reqwest uses strips single-dot (`.`, `%2e`) and double-dot
/// (`..`, `.%2e`, `%2e.`, `%2e%2e`) segments, all ASCII-case-insensitively,
/// when the request URL is parsed -- which happens *after* `ApiRequest.path` is
/// assembled, so `path_segment` cannot encode its way out of the LITERAL forms:
/// `.` on `/admin/queues/{q}/pause` silently resolves to `/admin/queues/pause`
/// and `..` to `/admin/pause`, retargeting the request at a different route.
/// They are therefore rejected up front.
///
/// The percent-encoded forms are, today, already neutralized a layer down --
/// `PATH_SEGMENT_ENCODE_SET` encodes `%`, so a queue literally named `%2e`
/// reaches the URL as `%252e` and survives intact. They are matched here anyway
/// so this guard stays correct on its own terms rather than silently depending
/// on that encode set keeping `%`.
fn is_url_dot_segment(raw: &str) -> bool {
    let lower = raw.to_ascii_lowercase();
    matches!(
        lower.as_str(),
        "." | "%2e" | ".." | ".%2e" | "%2e." | "%2e%2e"
    )
}

/// Reject a queue name that cannot survive URL path parsing intact.
fn checked_queue_segment(queue_name: &str) -> Result<String, CliError> {
    if is_url_dot_segment(queue_name) {
        return Err(CliError::QueueNameDotSegment {
            value: queue_name.to_string(),
        });
    }
    Ok(path_segment(queue_name))
}

/// Map `harvest queue …` onto the three management routes (issue #619).
///
/// `--shard-id` is omitted from the body entirely when unset, so the default is
/// a fleet-wide hold rather than a shard-scoped one.
fn queue_request(command: &QueueCommand) -> Result<ApiRequest, CliError> {
    match command {
        QueueCommand::Pause {
            queue_name,
            reason,
            shard_id,
        } => {
            let segment = checked_queue_segment(queue_name)?;
            let mut body = Map::new();
            body.insert("reason".to_string(), Value::String(reason.clone()));
            if let Some(shard) = shard_id {
                body.insert("shard_id".to_string(), Value::from(*shard));
            }
            Ok(ApiRequest::post(
                format!("/admin/queues/{segment}/pause"),
                Some(Value::Object(body)),
            ))
        }
        QueueCommand::Resume {
            queue_name,
            shard_id,
        } => {
            let segment = checked_queue_segment(queue_name)?;
            let mut body = Map::new();
            if let Some(shard) = shard_id {
                body.insert("shard_id".to_string(), Value::from(*shard));
            }
            Ok(ApiRequest::post(
                format!("/admin/queues/{segment}/resume"),
                Some(Value::Object(body)),
            ))
        }
        QueueCommand::ListPaused => Ok(ApiRequest::get("/admin/queues/paused")),
        QueueCommand::Coverage { queue_name, .. } => Ok(queue_name.as_ref().map_or_else(
            || ApiRequest::get("/admin/queue-coverage"),
            |value| {
                ApiRequest::get(format!(
                    "/admin/queue-coverage?queue_name={}",
                    query_encode(value)
                ))
            },
        )),
    }
}

/// True when the command is an activity pause/resume, whose response carries
/// the same partial-application contract as the queue mutations.
///
/// `list`/`get` are reads with no such contract, so they are excluded — the
/// same split `queue_mutation_should_gate` makes for `list-paused`.
const fn activity_mutation_should_gate(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Activity {
            command: ActivityCommand::Pause { .. } | ActivityCommand::Resume { .. }
        }
    )
}

/// Map `harvest activity …` onto the four management routes (issue #807).
///
/// The activity name is checked for URL dot-segments for exactly the reason
/// `checked_queue_segment` documents: the WHATWG URL parser strips `.` and `..`
/// segments AFTER `ApiRequest.path` is assembled, so an activity literally named
/// `.` would silently retarget `/activities/./pause` at `/activities/pause`.
///
/// `--reason`/`--actor` are omitted from the body entirely when unset, so the
/// server applies its own defaults rather than receiving an empty string — and
/// a bare `harvest activity pause charge_card` is a valid, complete containment
/// action. Containment must not wait on paperwork.
fn activity_request(command: &ActivityCommand) -> Result<ApiRequest, CliError> {
    match command {
        ActivityCommand::Pause {
            activity_name,
            reason,
            actor,
        } => {
            let segment = checked_activity_segment(activity_name)?;
            let mut body = Map::new();
            if let Some(value) = reason {
                body.insert("reason".to_string(), Value::String(value.clone()));
            }
            if let Some(value) = actor {
                body.insert("actor".to_string(), Value::String(value.clone()));
            }
            Ok(ApiRequest::post(
                format!("/activities/{segment}/pause"),
                Some(Value::Object(body)),
            ))
        }
        ActivityCommand::Resume { activity_name } => {
            let segment = checked_activity_segment(activity_name)?;
            Ok(ApiRequest::post(
                format!("/activities/{segment}/resume"),
                Some(Value::Object(Map::new())),
            ))
        }
        ActivityCommand::List { .. } => Ok(ApiRequest::get("/activities")),
        ActivityCommand::Get { activity_name } => {
            let segment = checked_activity_segment(activity_name)?;
            Ok(ApiRequest::get(format!("/activities/{segment}")))
        }
    }
}

/// Reject an activity name that cannot survive URL path parsing intact.
///
/// Shares `is_url_dot_segment` with the queue path — the hazard is a property
/// of URL parsing, not of what the segment names.
fn checked_activity_segment(activity_name: &str) -> Result<String, CliError> {
    if is_url_dot_segment(activity_name) {
        return Err(CliError::ActivityNameDotSegment {
            value: activity_name.to_string(),
        });
    }
    Ok(path_segment(activity_name))
}

fn rate_limit_request(command: &RateLimitCommand) -> ApiRequest {
    match command {
        RateLimitCommand::Status => ApiRequest::get("/admin/rate-limits"),
        RateLimitCommand::Set {
            key,
            refill_rate,
            burst,
        } => ApiRequest::post(
            format!("/admin/rate-limits/{}", path_segment(key)),
            Some(json!({
                "refill_rate": refill_rate,
                "burst": burst,
            })),
        ),
        RateLimitCommand::Override {
            activity_name,
            refill_rate,
            burst,
            ttl_secs,
        } => ApiRequest::post(
            format!(
                "/admin/rate-limits/{}/override",
                path_segment(activity_name)
            ),
            Some(json!({
                "refill_rate": refill_rate,
                "burst": burst,
                "ttl_secs": ttl_secs,
            })),
        ),
        RateLimitCommand::Clear { activity_name } => ApiRequest {
            method: ApiMethod::Delete,
            path: format!(
                "/admin/rate-limits/{}/override",
                path_segment(activity_name)
            ),
            body: None,
        },
    }
}

fn throttle_request(command: &ThrottleCommand) -> ApiRequest {
    match command {
        ThrottleCommand::Status => ApiRequest::get("/admin/start-throttle"),
        ThrottleCommand::Override {
            workflow_name,
            refill_per_sec,
            burst,
            ttl_secs,
        } => ApiRequest::post(
            format!(
                "/admin/start-throttle/{}/override",
                path_segment(workflow_name)
            ),
            Some(json!({
                "refill_per_sec": refill_per_sec,
                "burst": burst,
                "ttl_secs": ttl_secs,
            })),
        ),
        ThrottleCommand::Clear { workflow_name } => ApiRequest {
            method: ApiMethod::Delete,
            path: format!(
                "/admin/start-throttle/{}/override",
                path_segment(workflow_name)
            ),
            body: None,
        },
    }
}

fn audit_request(command: &AuditCommand) -> ApiRequest {
    match command {
        AuditCommand::List {
            actor,
            operation,
            target_type,
            target_id,
            status,
            since,
            before,
            limit,
        } => {
            let mut params: Vec<(&'static str, String)> = Vec::new();
            if let Some(v) = actor {
                params.push(("actor", v.clone()));
            }
            if let Some(v) = operation {
                params.push(("operation", v.clone()));
            }
            if let Some(v) = target_type {
                params.push(("target_type", v.clone()));
            }
            if let Some(v) = target_id {
                params.push(("target_id", v.clone()));
            }
            if let Some(v) = status {
                params.push(("status", v.clone()));
            }
            if let Some(v) = since {
                params.push(("since", v.clone()));
            }
            if let Some(v) = before {
                params.push(("before", v.clone()));
            }
            if let Some(v) = limit {
                params.push(("limit", v.to_string()));
            }
            if params.is_empty() {
                return ApiRequest::get("/admin/audit");
            }
            let qs = params
                .iter()
                .map(|(k, v)| format!("{k}={}", query_encode(v)))
                .collect::<Vec<_>>()
                .join("&");
            ApiRequest::get(format!("/admin/audit?{qs}"))
        }
    }
}

fn batch_request(command: &BatchCommand) -> Result<ApiRequest, CliError> {
    match command {
        BatchCommand::List { limit } => Ok(ApiRequest::get(path_with_limit(
            "/batch-operations",
            limit.map(|value| ("limit", value)),
        ))),
        BatchCommand::Get { batch_job_id } => Ok(ApiRequest::get(format!(
            "/batch-operations/{}",
            path_segment(batch_job_id)
        ))),
        BatchCommand::Submit {
            action,
            filter_json,
            filter_file,
            signal_name,
            signal_payload_json,
            signal_payload_file,
            dry_run,
            json: _,
        } => {
            // A non-dry-run Signal submit requires signal_name (enforced here
            // rather than via clap's `required_if_eq`, so a `Signal --dry-run`
            // preview — blast radius only, not signal validity — may omit it).
            if action == "Signal" && signal_name.is_none() && !*dry_run {
                return Err(CliError::InvalidInput(
                    "--signal-name is required for action Signal (unless --dry-run)".to_string(),
                ));
            }
            let filter = parse_json_source(
                filter_json.as_deref(),
                filter_file.as_deref(),
                "filter JSON",
            )?
            .unwrap_or_else(|| json!({}));
            let mut body = Map::new();
            body.insert("action".to_string(), json!(action));
            body.insert("filter".to_string(), filter);
            if let Some(sn) = signal_name {
                body.insert("signal_name".to_string(), json!(sn));
            }
            if let Some(payload) = parse_json_source(
                signal_payload_json.as_deref(),
                signal_payload_file.as_deref(),
                "signal payload JSON",
            )? {
                body.insert("signal_payload".to_string(), payload);
            }
            // Only add the key when set, so a real-submit body stays
            // byte-identical to a pre-#769 client (issue #769).
            if *dry_run {
                body.insert("dry_run".to_string(), json!(true));
            }
            Ok(ApiRequest::post(
                "/batch-operations",
                Some(Value::Object(body)),
            ))
        }
    }
}

/// Build the `POST /workflows/batch_start` request (issue #357).
///
/// Reads NDJSON items from `file` or parses `items_json` as a JSON array,
/// then wraps them in `{ "items": [...], "atomic": <bool> }`.
fn start_batch_request(
    file: Option<&Path>,
    items_json: Option<&str>,
    atomic: bool,
) -> Result<ApiRequest, CliError> {
    let items: Value = match (file, items_json) {
        (Some(path), None) => {
            // NDJSON: one JSON object per non-empty line.
            let raw = read_json_file(path, "NDJSON items")?;
            let mut arr = Vec::new();
            for line in raw.lines() {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let item: Value =
                    serde_json::from_str(trimmed).map_err(|source| CliError::InvalidJson {
                        label: "NDJSON items",
                        source,
                    })?;
                arr.push(item);
            }
            Value::Array(arr)
        }
        (None, Some(inline)) => {
            serde_json::from_str(inline).map_err(|source| CliError::InvalidJson {
                label: "items JSON",
                source,
            })?
        }
        (None, None) => {
            return Err(CliError::MissingInput {
                label: "start-batch requires --file or --items-json",
            });
        }
        (Some(_), Some(_)) => unreachable!("clap conflicts_with prevents both being set"),
    };

    let body = json!({
        "items": items,
        "atomic": atomic,
    });
    Ok(ApiRequest::post("/workflows/batch_start", Some(body)))
}

#[allow(clippy::too_many_lines)]
/// Builds the request for `harvest completion-delivery` subcommands (issue
/// #605). `List`'s `--state` filter is applied client-side in
/// `render_response` (the server has no query-param filter for this
/// endpoint), so it never reaches the request path/body here.
fn completion_delivery_request(command: &CompletionDeliveryCommand) -> ApiRequest {
    match command {
        CompletionDeliveryCommand::List {
            execution_id,
            state: _,
        } => ApiRequest::get(format!(
            "/workflows/{}/completion-deliveries",
            path_segment(execution_id)
        )),
        CompletionDeliveryCommand::Redrive {
            execution_id,
            delivery_id,
        } => ApiRequest::post(
            format!(
                "/workflows/{}/completion-deliveries/{}/redrive",
                path_segment(execution_id),
                path_segment(delivery_id)
            ),
            None,
        ),
    }
}

/// The `--state` filter for `harvest completion-delivery list`, if the
/// current command is that one and the flag was supplied.
const fn completion_delivery_list_state_filter(cli: &Cli) -> Option<&str> {
    match &cli.command {
        Commands::CompletionDelivery {
            command:
                CompletionDeliveryCommand::List {
                    state: Some(state), ..
                },
        } => Some(state.as_str()),
        _ => None,
    }
}

/// Filter a `GET .../completion-deliveries` JSON array response down to rows
/// whose `state` field matches `state` case-insensitively. Passes non-array
/// values through unchanged (defensive; the endpoint always returns an
/// array on success).
fn filter_completion_deliveries_by_state(value: &Value, state: &str) -> Value {
    let Some(rows) = value.as_array() else {
        return value.clone();
    };
    Value::Array(
        rows.iter()
            .filter(|row| {
                row.get("state")
                    .and_then(Value::as_str)
                    .is_some_and(|row_state| row_state.eq_ignore_ascii_case(state))
            })
            .cloned()
            .collect(),
    )
}

// Pre-existing clippy::too_many_lines debt (121 lines before issue #605
// touched this file at all; unrelated to completion-callback deliveries).
// Allowed here rather than left broken, matching the precedent already
// established for `DeferredTriggerStart::spawn` in the core crate.
#[allow(clippy::too_many_lines)]
fn dead_letter_request(command: &DeadLetterCommand) -> ApiRequest {
    match command {
        DeadLetterCommand::List { limit } => ApiRequest::get(path_with_limit(
            "/dead-letters",
            limit.map(|value| ("limit", value)),
        )),
        DeadLetterCommand::Replay { dead_letter_id } => ApiRequest::post(
            format!("/dead-letters/{}/replay", path_segment(dead_letter_id)),
            None,
        ),
        DeadLetterCommand::BulkReplay {
            activity_name,
            workflow_name,
            queue_name,
            min_attempts,
            failed_after,
            failed_before,
            error_class,
            dlq_reason,
            failure_signature,
            limit,
            dry_run,
        } => ApiRequest::post(
            "/dead-letters/replay",
            Some(build_bulk_dlq_body(
                activity_name.as_deref(),
                workflow_name.as_deref(),
                queue_name.as_deref(),
                *min_attempts,
                failed_after.as_deref(),
                failed_before.as_deref(),
                error_class.as_deref(),
                dlq_reason.as_deref(),
                failure_signature.as_deref(),
                *limit,
                *dry_run,
            )),
        ),
        DeadLetterCommand::BulkDiscard {
            activity_name,
            workflow_name,
            queue_name,
            min_attempts,
            failed_after,
            failed_before,
            error_class,
            dlq_reason,
            failure_signature,
            limit,
            dry_run,
        } => ApiRequest::post(
            "/dead-letters/discard",
            Some(build_bulk_dlq_body(
                activity_name.as_deref(),
                workflow_name.as_deref(),
                queue_name.as_deref(),
                *min_attempts,
                failed_after.as_deref(),
                failed_before.as_deref(),
                error_class.as_deref(),
                dlq_reason.as_deref(),
                failure_signature.as_deref(),
                *limit,
                *dry_run,
            )),
        ),
        DeadLetterCommand::Aggregate {
            group_by,
            time_bucket,
            workflow_name,
            activity_name,
            queue_name,
            since,
            until,
            min_attempts,
            limit_groups,
            samples_per_group,
            json: _,
        } => {
            let mut params: Vec<(&str, String)> = Vec::new();
            for dim in group_by {
                params.push(("group_by", dim.clone()));
            }
            if let Some(tb) = time_bucket {
                params.push(("time_bucket", tb.clone()));
            }
            if let Some(v) = workflow_name {
                params.push(("workflow_name", v.clone()));
            }
            if let Some(v) = activity_name {
                params.push(("activity_name", v.clone()));
            }
            if let Some(v) = queue_name {
                params.push(("queue_name", v.clone()));
            }
            if let Some(v) = since {
                params.push(("since", v.clone()));
            }
            if let Some(v) = until {
                params.push(("until", v.clone()));
            }
            if let Some(v) = min_attempts {
                params.push(("min_attempts", v.to_string()));
            }
            if let Some(v) = limit_groups {
                params.push(("limit_groups", v.to_string()));
            }
            if let Some(v) = samples_per_group {
                params.push(("samples_per_group", v.to_string()));
            }
            ApiRequest::get(format!(
                "/dead-letters/aggregate?{}",
                encode_query_params(&params)
            ))
        }
        DeadLetterCommand::Redrive {
            queue,
            workflow_name,
            dead_lettered_after,
            dead_lettered_before,
            error_contains,
            dead_letter_ids,
            max,
            reason,
            dry_run,
        } => ApiRequest::post(
            "/dlq/redrive",
            Some(build_redrive_dlq_body(
                queue.as_deref(),
                workflow_name.as_deref(),
                dead_lettered_after.as_deref(),
                dead_lettered_before.as_deref(),
                error_contains.as_deref(),
                dead_letter_ids,
                *max,
                reason.as_deref(),
                *dry_run,
            )),
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn build_redrive_dlq_body(
    queue: Option<&str>,
    workflow_name: Option<&str>,
    dead_lettered_after: Option<&str>,
    dead_lettered_before: Option<&str>,
    error_contains: Option<&str>,
    dead_letter_ids: &[String],
    max: Option<u32>,
    reason: Option<&str>,
    dry_run: bool,
) -> Value {
    let mut body = Map::new();
    insert_string(&mut body, "queue", queue);
    insert_string(&mut body, "workflow_name", workflow_name);
    insert_string(&mut body, "dead_lettered_after", dead_lettered_after);
    insert_string(&mut body, "dead_lettered_before", dead_lettered_before);
    insert_string(&mut body, "error_contains", error_contains);
    insert_string(&mut body, "reason", reason);
    if !dead_letter_ids.is_empty() {
        body.insert("dead_letter_ids".to_string(), json!(dead_letter_ids));
    }
    if let Some(m) = max {
        body.insert("max".to_string(), json!(m));
    }
    if dry_run {
        body.insert("dry_run".to_string(), json!(true));
    }
    Value::Object(body)
}

#[allow(clippy::too_many_arguments)]
fn build_bulk_dlq_body(
    activity_name: Option<&str>,
    workflow_name: Option<&str>,
    queue_name: Option<&str>,
    min_attempts: Option<i32>,
    failed_after: Option<&str>,
    failed_before: Option<&str>,
    error_class: Option<&str>,
    dlq_reason: Option<&str>,
    failure_signature: Option<&str>,
    limit: Option<u32>,
    dry_run: bool,
) -> Value {
    let mut body = Map::new();
    insert_string(&mut body, "activity_name", activity_name);
    insert_string(&mut body, "workflow_name", workflow_name);
    insert_string(&mut body, "queue_name", queue_name);
    if let Some(m) = min_attempts {
        body.insert("min_attempts".to_string(), json!(m));
    }
    insert_string(&mut body, "failed_after", failed_after);
    insert_string(&mut body, "failed_before", failed_before);
    insert_string(&mut body, "error_class", error_class);
    insert_string(&mut body, "dlq_reason", dlq_reason);
    insert_string(&mut body, "failure_signature", failure_signature);
    if let Some(l) = limit {
        body.insert("limit".to_string(), json!(l));
    }
    if dry_run {
        body.insert("dry_run".to_string(), json!(true));
    }
    Value::Object(body)
}

fn gate_request(command: &GateCommand) -> Result<ApiRequest, CliError> {
    match command {
        GateCommand::List => Ok(ApiRequest::get("/admin/gates")),
        GateCommand::Lift { id } => Ok(ApiRequest {
            method: ApiMethod::Delete,
            path: format!("/admin/gates/{}", path_segment(id)),
            body: None,
        }),
        GateCommand::Create {
            scope,
            reason,
            message,
            expires_at,
        } => {
            // Parse scope string: "fleet", "workflow_name=X", "queue=X", "shard_id=N", "owner=X"
            let (scope_kind, scope_value) = if scope == "fleet" {
                ("fleet".to_string(), None::<String>)
            } else if let Some(v) = scope.strip_prefix("workflow_name=") {
                ("workflow_name".to_string(), Some(v.to_string()))
            } else if let Some(v) = scope.strip_prefix("queue=") {
                ("queue".to_string(), Some(v.to_string()))
            } else if let Some(v) = scope.strip_prefix("shard_id=") {
                ("shard_id".to_string(), Some(v.to_string()))
            } else if let Some(v) = scope.strip_prefix("owner=") {
                ("owner".to_string(), Some(v.to_string()))
            } else {
                return Err(CliError::InvalidInput(format!(
                    "unknown scope '{scope}'; expected: fleet, workflow_name=<name>, queue=<name>, shard_id=<N>, or owner=<id>"
                )));
            };
            let mut body = serde_json::json!({
                "scope_kind": scope_kind,
                "reason": reason,
            });
            if let Some(v) = scope_value {
                body["scope_value"] = serde_json::json!(v);
            }
            if let Some(msg) = message {
                body["message"] = serde_json::json!(msg);
            }
            if let Some(exp) = expires_at {
                body["expires_at"] = serde_json::json!(exp);
            }
            Ok(ApiRequest::post("/admin/gates", Some(body)))
        }
    }
}

fn token_request(command: &TokenCommand) -> ApiRequest {
    match command {
        TokenCommand::List => ApiRequest::get("/admin/tokens"),
        TokenCommand::Revoke { id } => ApiRequest {
            method: ApiMethod::Delete,
            path: format!("/admin/tokens/{}", path_segment(id)),
            body: None,
        },
        TokenCommand::Create {
            name,
            scope,
            expires_at,
        } => {
            let mut body = serde_json::json!({
                "name": name,
                "scope": scope,
            });
            if let Some(exp) = expires_at {
                body["expires_at"] = serde_json::json!(exp);
            }
            ApiRequest::post("/admin/tokens", Some(body))
        }
        // Rotation has no dedicated server route: the CLI mints a replacement via
        // the create route. The old token is revoked as a documented second step.
        TokenCommand::Rotate {
            old_id,
            scope,
            expires_at,
        } => {
            let mut body = serde_json::json!({
                "name": format!("rotation-of-{old_id}"),
                "scope": scope,
            });
            if let Some(exp) = expires_at {
                body["expires_at"] = serde_json::json!(exp);
            }
            ApiRequest::post("/admin/tokens", Some(body))
        }
        // Bootstrap is an OFFLINE seed: it issues no HTTP request and is handled
        // entirely in-process in `run_cli` (mirrors DetCheck/Tui/Events).
        TokenCommand::Bootstrap { .. } => {
            unreachable!("token bootstrap is handled locally in run_cli")
        }
    }
}

/// The offline seed-token output produced by `harvest token bootstrap`.
///
/// Carries the one-time plaintext `secret`, its stored `hash`, and the exact
/// `INSERT INTO harvest_api_tokens ...` statement to run out-of-band. The SQL
/// contains ONLY the hash — never the secret (issue #942).
pub struct BootstrapToken {
    /// The plaintext `hvst_...` secret — shown once, never stored.
    pub secret: String,
    /// `hex(SHA256(secret))` — the value embedded in the INSERT and stored.
    pub hash: String,
    /// The token scope (`read` | `mutate`).
    pub scope: String,
    /// The token label.
    pub name: String,
    /// A ready-to-run `INSERT INTO harvest_api_tokens (...) VALUES (...);`.
    pub insert_sql: String,
}

/// Wrap `s` as a Postgres single-quoted string literal, escaping embedded
/// single quotes (`'` → `''`). With `standard_conforming_strings` (the default),
/// this fully neutralizes injection through a crafted `--name`/`--created-by`.
fn sql_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Build the offline bootstrap seed: a fresh secret, its stored hash, and the
/// exact INSERT SQL. Opens **no** database connection.
///
/// The secret and hash are produced by the SHARED core helpers
/// ([`autumn_harvest::api_token::mint_secret`] / [`hash_secret`]) — the same
/// functions the server mint route uses — so a seeded token authenticates
/// byte-for-byte identically to a route-minted one (no drift).
///
/// # Errors
///
/// Returns [`CliError::InvalidInput`] if `expires_at` is not a valid RFC 3339
/// timestamp (so a broken INSERT is never emitted).
pub fn build_bootstrap_token(
    name: &str,
    scope: &str,
    expires_at: Option<&str>,
    created_by: &str,
) -> Result<BootstrapToken, CliError> {
    // Single source of truth: the mint route hashes with these exact helpers.
    let secret = autumn_harvest::api_token::mint_secret();
    let hash = autumn_harvest::api_token::hash_secret(&secret);

    // Validate the expiry up front so we never print an INSERT that Postgres
    // rejects at run time.
    if let Some(e) = expires_at {
        autumn_harvest::chrono::DateTime::parse_from_rfc3339(e).map_err(|source| {
            CliError::InvalidInput(format!(
                "token bootstrap: --expires-at '{e}' is not a valid RFC 3339 timestamp: {source}"
            ))
        })?;
    }

    // `id`/`created_at` use the column defaults (gen_random_uuid()/NOW()); every
    // user-supplied string is single-quote escaped so a crafted flag value
    // cannot inject SQL. The statement embeds only the hash, never the secret.
    let mut columns = String::from("id, name, token_hash, scope, created_at, created_by");
    let mut values = format!(
        "gen_random_uuid(), {}, {}, {}, NOW(), {}",
        sql_quote(name),
        sql_quote(&hash),
        sql_quote(scope),
        sql_quote(created_by),
    );
    if let Some(e) = expires_at {
        use std::fmt::Write as _;
        columns.push_str(", expires_at");
        let _ = write!(values, ", {}::timestamptz", sql_quote(e));
    }
    let insert_sql = format!("INSERT INTO harvest_api_tokens ({columns})\nVALUES ({values});");

    Ok(BootstrapToken {
        secret,
        hash,
        scope: scope.to_string(),
        name: name.to_string(),
        insert_sql,
    })
}

/// Execute `harvest token bootstrap`: print the one-time secret and the INSERT
/// SQL. Opens no DB connection; the operator runs the SQL out-of-band.
///
/// # Errors
///
/// Propagates [`build_bootstrap_token`]'s validation error.
fn run_token_bootstrap(
    name: &str,
    scope: &str,
    expires_at: Option<&str>,
    created_by: &str,
) -> Result<(), CliError> {
    let token = build_bootstrap_token(name, scope, expires_at, created_by)?;

    println!("Harvest API token — offline bootstrap seed");
    println!();
    println!("  Token secret (shown once — store it now, it cannot be recovered):");
    println!();
    println!("    {}", token.secret);
    println!();
    println!("  1. Save the secret above in your secret store. Only its SHA-256 hash is");
    println!("     written to the database, so the secret cannot be recovered later.");
    println!("  2. Run this SQL against your Harvest database to create the token row");
    println!("     (it embeds only the hash — never the secret):");
    println!();
    for line in token.insert_sql.lines() {
        println!("    {line}");
    }
    println!();
    println!("  3. Send the secret above as a bearer credential:");
    println!("       Authorization: Bearer <secret>");
    println!(
        "     A `{}`-scoped token can mint every further token via POST /admin/tokens.",
        token.scope
    );
    Ok(())
}

fn worker_request(command: &WorkerCommand) -> ApiRequest {
    match command {
        WorkerCommand::Drain {
            worker_id,
            deadline,
            wait: _,
            wait_timeout_secs: _,
        } => {
            let mut body = Map::new();
            if let Some(d) = deadline {
                body.insert("deadline_at".to_string(), json!(d));
            }
            ApiRequest::post(
                format!("/workers/{}/drain", path_segment(worker_id)),
                Some(Value::Object(body)),
            )
        }
        WorkerCommand::DrainPreview {
            queue,
            shard_id,
            status,
            limit,
        } => {
            let mut params: Vec<(&'static str, String)> = Vec::new();
            if let Some(q) = queue {
                params.push(("queue", q.clone()));
            }
            if let Some(s) = shard_id {
                params.push(("shard_id", s.to_string()));
            }
            if let Some(s) = status {
                params.push(("status", s.clone()));
            }
            if let Some(l) = limit {
                params.push(("limit", l.to_string()));
            }
            if params.is_empty() {
                return ApiRequest::get("/workers/drain-preview");
            }
            let qs = params
                .iter()
                .map(|(k, v)| format!("{k}={}", query_encode(v)))
                .collect::<Vec<_>>()
                .join("&");
            ApiRequest::get(format!("/workers/drain-preview?{qs}"))
        }
        WorkerCommand::List {
            queue,
            shard_id,
            status,
            health,
            limit,
        } => {
            let mut params: Vec<(&'static str, String)> = Vec::new();
            if let Some(q) = queue {
                params.push(("queue", q.clone()));
            }
            if let Some(s) = shard_id {
                params.push(("shard_id", s.to_string()));
            }
            if let Some(s) = status {
                params.push(("status", s.clone()));
            }
            if let Some(h) = health {
                params.push(("health", h.clone()));
            }
            if let Some(l) = limit {
                params.push(("limit", l.to_string()));
            }
            if params.is_empty() {
                return ApiRequest::get("/workers");
            }
            let qs = params
                .iter()
                .map(|(k, v)| format!("{k}={}", query_encode(v)))
                .collect::<Vec<_>>()
                .join("&");
            ApiRequest::get(format!("/workers?{qs}"))
        }
        WorkerCommand::Get { worker_id } => {
            ApiRequest::get(format!("/workers/{}", path_segment(worker_id)))
        }
        WorkerCommand::Health => ApiRequest::get("/workers/health"),
    }
}

fn parse_json_source(
    inline: Option<&str>,
    file: Option<&Path>,
    label: &'static str,
) -> Result<Option<Value>, CliError> {
    match (inline, file) {
        (Some(_), Some(_)) => Err(CliError::ConflictingJsonSources { label }),
        (Some(raw), None) => serde_json::from_str(raw)
            .map(Some)
            .map_err(|source| CliError::InvalidJson { label, source }),
        (None, Some(path)) => {
            let raw = read_json_file(path, label)?;
            serde_json::from_str(&raw)
                .map(Some)
                .map_err(|source| CliError::InvalidJson { label, source })
        }
        (None, None) => Ok(None),
    }
}

fn read_json_file(path: &Path, label: &'static str) -> Result<String, CliError> {
    if path == Path::new("-") {
        let mut input = String::new();
        std::io::stdin()
            .read_to_string(&mut input)
            .map_err(|source| CliError::ReadJson {
                label,
                path: "-".to_string(),
                source,
            })?;
        return Ok(input);
    }

    fs::read_to_string(path).map_err(|source| CliError::ReadJson {
        label,
        path: path.display().to_string(),
        source,
    })
}

fn insert_string(body: &mut Map<String, Value>, key: &str, value: Option<&str>) {
    if let Some(value) = value {
        body.insert(key.to_string(), Value::String(value.to_string()));
    }
}

fn insert_json(body: &mut Map<String, Value>, key: &str, value: Option<Value>) {
    if let Some(value) = value {
        body.insert(key.to_string(), value);
    }
}

fn path_segment(raw: &str) -> String {
    utf8_percent_encode(raw, PATH_SEGMENT_ENCODE_SET).to_string()
}

fn path_with_limit(base: &str, limit: Option<(&str, i64)>) -> String {
    let Some((key, value)) = limit else {
        return base.to_string();
    };

    let mut query = BTreeMap::new();
    query.insert(key, value.to_string());
    let query = query
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&");
    format!("{base}?{query}")
}

#[allow(clippy::too_many_arguments)]
fn build_handoff_list_path(
    states: &[String],
    workflow_name: Option<&str>,
    execution_id: Option<&str>,
    activity_name: Option<&str>,
    token: Option<&str>,
    shard_id: Option<i32>,
    due_before: Option<&str>,
    updated_before: Option<&str>,
    limit: Option<i64>,
) -> String {
    let mut params: Vec<(&'static str, String)> = Vec::new();
    if !states.is_empty() {
        params.push(("state", states.join(",")));
    }
    if let Some(value) = workflow_name {
        params.push(("workflow_name", value.to_string()));
    }
    if let Some(value) = execution_id {
        params.push(("execution_id", value.to_string()));
    }
    if let Some(value) = activity_name {
        params.push(("activity_name", value.to_string()));
    }
    if let Some(value) = token {
        params.push(("token", value.to_string()));
    }
    if let Some(value) = shard_id {
        params.push(("shard_id", value.to_string()));
    }
    if let Some(value) = due_before {
        params.push(("due_before", value.to_string()));
    }
    if let Some(value) = updated_before {
        params.push(("updated_before", value.to_string()));
    }
    if let Some(value) = limit {
        params.push(("limit", value.to_string()));
    }

    if params.is_empty() {
        return "/admin/external-handoffs".to_string();
    }
    let query = params
        .iter()
        .map(|(key, value)| format!("{key}={}", query_encode(value)))
        .collect::<Vec<_>>()
        .join("&");
    format!("/admin/external-handoffs?{query}")
}

#[allow(clippy::too_many_arguments)]
fn build_workflow_list_path(
    limit: Option<i64>,
    states: &[String],
    workflow_name: Option<&str>,
    search_attrs: &[String],
    search_attr_filters: &[String],
    owner: Option<&str>,
    no_progress_minutes: Option<i64>,
    include_sleeping: bool,
    history_bloat_min_events: Option<u64>,
    start_source: Option<&str>,
) -> Result<String, CliError> {
    let mut params: Vec<(&'static str, String)> = Vec::new();
    if let Some(value) = limit {
        params.push(("limit", value.to_string()));
    }
    if !states.is_empty() {
        // Use comma-separated values to match the management API's documented
        // canonical form. The server also accepts repeated `state=` params.
        params.push(("state", states.join(",")));
    }
    if let Some(name) = workflow_name {
        params.push(("workflow_name", name.to_string()));
    }
    for raw in search_attrs {
        let (key, value) = raw
            .split_once('=')
            .ok_or_else(|| CliError::InvalidSearchAttr { value: raw.clone() })?;
        params.push(("search_attr", format!("{key}:{value}")));
    }
    // Issue #506: typed comparison/set predicates forwarded verbatim. The server
    // owns validation (op grammar, numeric coercion, top-level-key rule), so the
    // CLI is a thin passthrough and returns the API's `400` message on error.
    for raw in search_attr_filters {
        params.push(("search_attr_filter", raw.clone()));
    }
    if let Some(o) = owner {
        params.push(("owner", o.to_string()));
    }
    if let Some(minutes) = no_progress_minutes {
        params.push(("no_progress_minutes", minutes.to_string()));
    }
    if include_sleeping {
        params.push(("include_sleeping", "true".to_string()));
    }
    // Issue #704: operator early-warning discovery for workflow history bloat.
    // A query param DISTINCT from the server's general-purpose
    // `min_history_events` filter (issue #493, not exposed via this CLI
    // command) — see `WorkflowFilters::history_bloat_min_events` in the
    // plugin for why the two must never share a name. The server owns
    // validation (non-numeric/negative → 400) and the restricted-to-non-
    // terminal + sorted-by-size-descending behavior — the CLI is a thin
    // passthrough.
    if let Some(value) = history_bloat_min_events {
        params.push(("history_bloat_min_events", value.to_string()));
    }
    // Issue #740: bounded provenance filter. The server owns validation (a value
    // outside the known `StartSource` set / "unknown" returns a 400), so the CLI
    // is a thin passthrough — matching how `--state` forwards verbatim.
    if let Some(source) = start_source {
        params.push(("start_source", source.to_string()));
    }

    if params.is_empty() {
        return Ok("/workflows".to_string());
    }
    let encoded = params
        .iter()
        .map(|(key, value)| format!("{key}={}", query_encode(value)))
        .collect::<Vec<_>>()
        .join("&");
    Ok(format!("/workflows?{encoded}"))
}

#[allow(clippy::too_many_arguments)]
fn build_summary_list_path(
    workflow_name: Option<&str>,
    workflow_id: Option<&str>,
    states: &[String],
    completed_after: Option<&str>,
    completed_before: Option<&str>,
    search_attrs: &[String],
    limit: Option<i64>,
    cursor: Option<&str>,
    order: Option<&str>,
) -> Result<String, CliError> {
    let mut params: Vec<(&'static str, String)> = Vec::new();
    if let Some(name) = workflow_name {
        params.push(("workflow_name", name.to_string()));
    }
    if let Some(wid) = workflow_id {
        params.push(("workflow_id", wid.to_string()));
    }
    if !states.is_empty() {
        params.push(("state", states.join(",")));
    }
    if let Some(after) = completed_after {
        params.push(("completed_after", after.to_string()));
    }
    if let Some(before) = completed_before {
        params.push(("completed_before", before.to_string()));
    }
    for raw in search_attrs {
        let (key, value) = raw
            .split_once('=')
            .ok_or_else(|| CliError::InvalidSearchAttr { value: raw.clone() })?;
        params.push(("search_attr", format!("{key}:{value}")));
    }
    if let Some(value) = limit {
        params.push(("limit", value.to_string()));
    }
    if let Some(value) = cursor {
        params.push(("cursor", value.to_string()));
    }
    if let Some(value) = order {
        params.push(("order", value.to_string()));
    }

    if params.is_empty() {
        return Ok("/workflows/summaries".to_string());
    }
    let encoded = params
        .iter()
        .map(|(key, value)| format!("{key}={}", query_encode(value)))
        .collect::<Vec<_>>()
        .join("&");
    Ok(format!("/workflows/summaries?{encoded}"))
}

fn build_workflow_children_path(
    execution_id: &str,
    statuses: &[String],
    workflow_name: Option<&str>,
    limit: Option<i64>,
    cursor: Option<&str>,
    depth: Option<u8>,
) -> String {
    let mut params: Vec<(&'static str, String)> = Vec::new();
    for status in statuses {
        params.push(("status", status.clone()));
    }
    if let Some(name) = workflow_name {
        params.push(("workflow_name", name.to_string()));
    }
    if let Some(value) = limit {
        params.push(("limit", value.to_string()));
    }
    if let Some(value) = cursor {
        params.push(("cursor", value.to_string()));
    }
    if let Some(value) = depth {
        params.push(("depth", value.to_string()));
    }

    let base = format!("/workflows/{}/children", path_segment(execution_id));
    if params.is_empty() {
        return base;
    }
    let encoded = params
        .iter()
        .map(|(key, value)| format!("{key}={}", query_encode(value)))
        .collect::<Vec<_>>()
        .join("&");
    format!("{base}?{encoded}")
}

fn encode_query_params(params: &[(&str, String)]) -> String {
    params
        .iter()
        .map(|(key, value)| format!("{key}={}", query_encode(value)))
        .collect::<Vec<_>>()
        .join("&")
}

fn query_encode(input: &str) -> String {
    // RFC 3986 query-component encoding. We intentionally leave `:` unencoded
    // so the management API sees `search_attr=key:value` as a stable shape.
    let mut out = String::with_capacity(input.len());
    for byte in input.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~' | b':' | b',') {
            out.push(byte as char);
        } else {
            use std::fmt::Write as _;
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

/// Clap value-parser for `--idempotency-key` (issue #753).
///
/// Mirrors the server's `Idempotency-Key` header semantics: a present but
/// empty (or whitespace-only) key is rejected up front rather than silently
/// degraded — the management API treats an empty `?idempotency_key=` query
/// param as omitted, which would turn an intended exactly-once delivery into
/// at-least-once without the caller noticing.
fn parse_idempotency_key(value: &str) -> Result<String, String> {
    if value.trim().is_empty() {
        return Err(
            "--idempotency-key must not be empty; omit the flag entirely for legacy \
             at-least-once delivery"
                .to_string(),
        );
    }
    Ok(value.to_string())
}

// ─── Version-gate retirement check helpers ────────────────────────────────────

const fn retirement_check_should_check(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::VersionGateRetirement { check: true, .. }
    )
}

fn retirement_check_wants_table(cli: &Cli) -> bool {
    matches!(&cli.command, Commands::VersionGateRetirement { .. })
        && cli.output == OutputFormat::PrettyJson
}

fn retirement_check_exit_code(value: &Value) -> i32 {
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unavailable");
    // Non-zero on any non-safe outcome
    i32::from(!matches!(status, "safe"))
}

fn retirement_check_request(
    change_id: &str,
    min_safe_version: u32,
    workflow_name: Option<&str>,
    state_group: Option<VersionUsageStateGroup>,
    shard_id: Option<i32>,
) -> ApiRequest {
    let mut params: Vec<(&'static str, String)> = Vec::new();
    params.push(("change_id", change_id.to_string()));
    params.push(("min_safe_version", min_safe_version.to_string()));
    if let Some(name) = workflow_name {
        params.push(("workflow_name", name.to_string()));
    }
    if let Some(sg) = state_group {
        params.push(("state_group", sg.as_wire().to_string()));
    }
    if let Some(sid) = shard_id {
        params.push(("shard_id", sid.to_string()));
    }
    let encoded = params
        .iter()
        .map(|(key, value)| format!("{key}={}", query_encode(value)))
        .collect::<Vec<_>>()
        .join("&");
    ApiRequest::get(format!("/admin/version-gates/retirement-check?{encoded}"))
}

fn format_retirement_check_table(value: &Value) -> String {
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let safe = value
        .get("safe_to_retire")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let observed_at = value
        .get("observed_at")
        .and_then(Value::as_str)
        .unwrap_or("-");
    let change_id = value
        .get("filters")
        .and_then(|f| f.get("change_id"))
        .and_then(Value::as_str)
        .unwrap_or("-");
    let min_safe = value
        .get("filters")
        .and_then(|f| f.get("min_safe_version"))
        .and_then(Value::as_i64)
        .map_or_else(|| "-".to_string(), |v| v.to_string());
    let safe_str = if safe { "yes" } else { "no" };
    let header = format!(
        "status: {status}  safe_to_retire: {safe_str}  change_id: {change_id}  min_safe_version: {min_safe}\nobserved_at: {observed_at}"
    );

    let Some(blockers) = value.get("blockers").and_then(Value::as_array) else {
        return format!("{header}\nNo blockers returned.");
    };
    if blockers.is_empty() {
        return format!("{header}\nNo old-version executions found.");
    }

    let mut rows: Vec<Vec<String>> = Vec::with_capacity(blockers.len() + 1);
    rows.push(vec![
        "WORKFLOW".to_string(),
        "VERSION".to_string(),
        "ACTIVE".to_string(),
        "TERMINAL".to_string(),
        "OLDEST_AGE_S".to_string(),
        "NEWEST_AGE_S".to_string(),
        "SHARDS".to_string(),
        "UNAVAILABLE".to_string(),
    ]);
    for blocker in blockers {
        rows.push(vec![
            cell_str(blocker.get("workflow_name")),
            cell_number(blocker.get("recorded_version")),
            cell_number(blocker.get("active_executions")),
            cell_number(blocker.get("terminal_executions")),
            cell_number(blocker.get("oldest_blocker_age_secs")),
            cell_number(blocker.get("newest_blocker_age_secs")),
            retirement_shard_array_cell(blocker, "matched_shards"),
            retirement_shard_array_cell(blocker, "unavailable_shards"),
        ]);
    }

    let table = render_table(&rows);

    format!("{header}\n\n{table}")
}

fn retirement_shard_array_cell(item: &Value, field: &str) -> String {
    let Some(values) = item
        .get("shard_coverage")
        .and_then(|coverage| coverage.get(field))
        .and_then(Value::as_array)
    else {
        return "-".to_string();
    };
    if values.is_empty() {
        return "-".to_string();
    }
    values
        .iter()
        .filter_map(Value::as_i64)
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
mod reuse_policy_tests {
    use super::*;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("harvest").chain(args.iter().copied()))
            .expect("CLI should parse successfully")
    }

    fn start_request(args: &[&str]) -> ApiRequest {
        parse(args)
            .api_request()
            .expect("request mapping should succeed")
    }

    #[test]
    fn start_omitting_reuse_policy_sends_no_field() {
        let req = start_request(&["workflow", "start", "my_wf"]);
        let body = req.body.as_ref().expect("start should have a body");
        assert!(
            body.get("reuse_policy").is_none(),
            "omitting --reuse-policy must not send the field"
        );
    }

    #[test]
    fn start_allow_duplicate_sends_correct_value() {
        let req = start_request(&[
            "workflow",
            "start",
            "my_wf",
            "--reuse-policy",
            "allow_duplicate",
        ]);
        let body = req.body.as_ref().unwrap();
        assert_eq!(body["reuse_policy"], "allow_duplicate");
    }

    #[test]
    fn start_reject_duplicate_sends_correct_value() {
        let req = start_request(&[
            "workflow",
            "start",
            "my_wf",
            "--reuse-policy",
            "reject_duplicate",
        ]);
        let body = req.body.as_ref().unwrap();
        assert_eq!(body["reuse_policy"], "reject_duplicate");
    }

    #[test]
    fn start_allow_duplicate_failed_only_sends_correct_value() {
        let req = start_request(&[
            "workflow",
            "start",
            "my_wf",
            "--reuse-policy",
            "allow_duplicate_failed_only",
        ]);
        let body = req.body.as_ref().unwrap();
        assert_eq!(body["reuse_policy"], "allow_duplicate_failed_only");
    }

    #[test]
    fn start_terminate_if_running_sends_correct_value() {
        let req = start_request(&[
            "workflow",
            "start",
            "my_wf",
            "--reuse-policy",
            "terminate_if_running",
        ]);
        let body = req.body.as_ref().unwrap();
        assert_eq!(body["reuse_policy"], "terminate_if_running");
    }

    #[test]
    fn start_preserves_other_fields_alongside_reuse_policy() {
        let req = start_request(&[
            "workflow",
            "start",
            "my_wf",
            "--workflow-id",
            "wf-123",
            "--reuse-policy",
            "reject_duplicate",
        ]);
        let body = req.body.as_ref().unwrap();
        assert_eq!(body["workflow_id"], "wf-123");
        assert_eq!(body["reuse_policy"], "reject_duplicate");
    }

    #[test]
    fn children_default_output_renders_human_table() {
        let cli = parse(&[
            "workflow",
            "children",
            "00000000-0000-0000-0000-000000000001",
        ]);
        let payload = json!({
            "items": [{
                "exec_id": "00000000-0000-0000-0000-000000000002",
                "workflow_name": "billing_child",
                "status": "Failed",
                "started_at": "2026-05-04T12:00:00Z",
                "completed_at": null,
                "error_summary": "charge card failed",
                "shard_id": 1,
                "depth": 0
            }],
            "next_cursor": null
        });

        let rendered = render_response(&cli, &payload).expect("table output should render");

        assert!(rendered.contains("EXEC ID"));
        assert!(rendered.contains("billing_child"));
        assert!(rendered.contains("charge card failed"));
        assert!(!rendered.trim_start().starts_with('{'));
    }

    #[test]
    fn children_json_flag_renders_raw_payload() {
        let cli = parse(&[
            "workflow",
            "children",
            "00000000-0000-0000-0000-000000000001",
            "--json",
        ]);
        let payload = json!({
            "items": [],
            "next_cursor": null
        });

        let rendered = render_response(&cli, &payload).expect("json output should render");

        assert_eq!(rendered, r#"{"items":[],"next_cursor":null}"#);
    }

    // ── issue #756: partial cross-shard read notice ──────────────────────

    #[test]
    fn fanout_partial_notice_none_for_bare_array() {
        // The happy path is a bare array; no notice.
        assert!(fanout_partial_notice(&json!([{"id": "a"}])).is_none());
    }

    #[test]
    fn fanout_partial_notice_none_when_unavailable_empty() {
        assert!(
            fanout_partial_notice(&json!({
                "workers": [],
                "status": "complete",
                "unavailable_shards": []
            }))
            .is_none()
        );
    }

    #[test]
    fn fanout_partial_notice_names_shard_and_reason() {
        let notice = fanout_partial_notice(&json!({
            "workflows": [],
            "status": "partial",
            "unavailable_shards": [
                {"shard_id": 1, "reason": "connection refused"}
            ]
        }))
        .expect("degraded envelope must produce a notice");
        assert!(notice.contains("partial"));
        assert!(notice.contains("1 shard(s) unavailable"));
        assert!(notice.contains("1: connection refused"));
    }

    #[test]
    fn workflow_list_degraded_body_is_clean_and_notice_is_separate() {
        // Issue #756: on the degraded path the notice goes to STDERR (via
        // `fanout_partial_notice`, `eprintln!`'d by the caller), NOT prepended
        // to the STDOUT body — so `workflow list -o json | jq` stays parseable.
        let cli = parse(&["workflow", "list"]);
        let payload = json!({
            "workflows": [{"id": "00000000-0000-0000-0000-000000000001"}],
            "status": "partial",
            "unavailable_shards": [{"shard_id": 2, "reason": "pool missing"}]
        });
        // The STDOUT body carries no warning and remains parseable JSON.
        let rendered = render_response(&cli, &payload).expect("render should succeed");
        assert!(
            !rendered.contains("WARNING"),
            "the STDOUT body must stay clean, got: {rendered}"
        );
        let parsed: Value =
            serde_json::from_str(&rendered).expect("the STDOUT body must remain parseable JSON");
        assert!(
            parsed.get("workflows").is_some(),
            "the data still renders in the body"
        );
        // The notice is available separately for the caller to emit on STDERR.
        let notice =
            fanout_partial_notice(&payload).expect("degraded payload yields a stderr notice");
        assert!(notice.starts_with("WARNING: cross-shard read is partial"));
        assert!(notice.contains("2: pool missing"));
    }

    #[test]
    fn workflow_list_happy_path_bare_array_has_no_notice() {
        let cli = parse(&["workflow", "list"]);
        let payload = json!([{"id": "00000000-0000-0000-0000-000000000001"}]);
        let rendered = render_response(&cli, &payload).expect("render should succeed");
        assert!(!rendered.contains("WARNING"));
        // No stderr notice on the happy path either.
        assert!(fanout_partial_notice(&payload).is_none());
    }

    #[test]
    fn handoff_list_default_output_renders_human_table() {
        let cli = parse(&["handoff", "list"]);
        let payload = json!({
            "status": "ok",
            "shard_coverage": {
                "inspected_shards": [0],
                "matched_shards": [0],
                "unavailable_shards": []
            },
            "items": [{
                "token": "11111111-1111-4111-8111-111111111111",
                "workflow": {
                    "execution_id": "00000000-0000-0000-0000-000000000001",
                    "workflow_id": "invoice-42",
                    "workflow_name": "billing_checkout",
                    "shard_id": 0
                },
                "activity": {
                    "activity_id": "22222222-2222-4222-8222-222222222222",
                    "activity_name": "manager_approval"
                },
                "state": "PENDING",
                "created_at": "2026-05-08T11:00:00Z",
                "updated_at": "2026-05-08T11:05:00Z",
                "deadline_at": "2026-05-08T12:00:00Z"
            }]
        });

        let rendered = render_response(&cli, &payload).expect("table output should render");

        assert!(rendered.contains("status: ok"));
        assert!(rendered.contains("STATE"));
        assert!(rendered.contains("manager_approval"));
        assert!(rendered.contains("11111111-1111-4111-8111-111111111111"));
        assert!(!rendered.trim_start().starts_with('{'));
    }

    #[test]
    fn handoff_json_flag_renders_raw_payload() {
        let cli = parse(&["handoff", "list", "--json"]);
        let payload = json!({
            "status": "ok",
            "items": [],
            "shard_coverage": {
                "inspected_shards": [0],
                "matched_shards": [],
                "unavailable_shards": []
            }
        });

        let rendered = render_response(&cli, &payload).expect("json output should render");

        assert_eq!(
            rendered,
            r#"{"items":[],"shard_coverage":{"inspected_shards":[0],"matched_shards":[],"unavailable_shards":[]},"status":"ok"}"#
        );
    }

    #[test]
    fn preflight_default_output_renders_compact_table() {
        let cli = parse(&["preflight"]);
        let payload = json!({
            "overall_status": "warn",
            "observed_at": "2026-05-06T12:00:00Z",
            "version": {
                "package": "autumn-harvest-plugin",
                "version": "0.3.0",
                "core_version": "0.3.0"
            },
            "checks": [{
                "name": "worker_coverage",
                "status": "warn",
                "summary": "queue coverage exists but one worker is stale",
                "remediation": "Restart or replace stale workers before promotion.",
                "affected_shards": [0],
                "details": {}
            }]
        });

        let rendered = render_response(&cli, &payload).expect("table output should render");

        assert!(rendered.contains("STATUS"));
        assert!(rendered.contains("worker_coverage"));
        assert!(rendered.contains("warn"));
        assert!(!rendered.trim_start().starts_with('{'));
    }

    #[test]
    fn preflight_json_output_preserves_raw_payload_shape() {
        let cli = parse(&["--output", "json", "preflight"]);
        let payload = json!({
            "overall_status": "pass",
            "observed_at": "2026-05-06T12:00:00Z",
            "version": {
                "package": "autumn-harvest-plugin",
                "version": "0.3.0",
                "core_version": "0.3.0"
            },
            "checks": []
        });

        let rendered = render_response(&cli, &payload).expect("json output should render");

        assert_eq!(
            rendered,
            r#"{"checks":[],"observed_at":"2026-05-06T12:00:00Z","overall_status":"pass","version":{"core_version":"0.3.0","package":"autumn-harvest-plugin","version":"0.3.0"}}"#
        );
    }

    #[test]
    fn preflight_exit_codes_match_deploy_gate_status() {
        assert_eq!(preflight_exit_code(&json!({ "overall_status": "pass" })), 0);
        assert_eq!(preflight_exit_code(&json!({ "overall_status": "warn" })), 2);
        assert_eq!(preflight_exit_code(&json!({ "overall_status": "fail" })), 1);
    }

    #[test]
    fn shard_health_default_output_renders_compact_table() {
        let cli = parse(&["shard", "health"]);
        let payload = json!({
            "overall_readiness": "degraded",
            "observed_at": "2026-05-06T12:00:00Z",
            "shards": [{
                "shard_id": 1,
                "roles": ["readable"],
                "candidate": true,
                "readiness": "degraded",
                "reachable": true,
                "active_worker_count": 0,
                "stale_worker_count": 0,
                "schema": { "ready": true },
                "worker_coverage": [{
                    "queue": "default",
                    "healthy_active": 0,
                    "stale": 0,
                    "draining": 0,
                    "ready": false
                }],
                "scheduler": {
                    "enabled": false,
                    "ready": true,
                    "last_tick_at": null
                },
                "queue_depth": {
                    "total_pending": 0,
                    "by_queue": {}
                },
                "dlq": { "count": 0 },
                "blocking_reasons": [
                    "no healthy active worker covers required queue 'default'"
                ],
                "error_summary": null
            }]
        });

        let rendered = render_response(&cli, &payload).expect("table output should render");

        assert!(rendered.contains("SHARD"));
        assert!(rendered.contains("readable"));
        assert!(rendered.contains("degraded"));
        assert!(rendered.contains("active=0"));
        assert!(rendered.contains("stale=0"));
        assert!(rendered.contains("default"));
        assert!(!rendered.trim_start().starts_with('{'));
    }

    #[test]
    fn shard_health_json_output_preserves_raw_payload_shape() {
        let cli = parse(&["--output", "json", "shard", "health"]);
        let payload = json!({
            "overall_readiness": "ready",
            "observed_at": "2026-05-06T12:00:00Z",
            "shards": []
        });

        let rendered = render_response(&cli, &payload).expect("json output should render");

        assert_eq!(
            rendered,
            r#"{"observed_at":"2026-05-06T12:00:00Z","overall_readiness":"ready","shards":[]}"#
        );
    }

    #[test]
    fn shard_health_gate_fails_on_non_ready_writable_or_candidate_shards_only() {
        let payload = json!({
            "shards": [
                {
                    "shard_id": 0,
                    "roles": ["readable", "writable", "default"],
                    "candidate": false,
                    "readiness": "ready"
                },
                {
                    "shard_id": 1,
                    "roles": ["readable"],
                    "candidate": false,
                    "readiness": "degraded"
                }
            ]
        });
        assert_eq!(shard_health_exit_code(&payload), 0);

        let writable_degraded = json!({
            "shards": [{
                "shard_id": 0,
                "roles": ["readable", "writable", "default"],
                "candidate": false,
                "readiness": "degraded"
            }]
        });
        assert_eq!(shard_health_exit_code(&writable_degraded), 1);

        let candidate_degraded = json!({
            "shards": [{
                "shard_id": 2,
                "roles": ["readable"],
                "candidate": true,
                "readiness": "degraded"
            }]
        });
        assert_eq!(shard_health_exit_code(&candidate_degraded), 1);
    }

    #[test]
    fn shard_health_gate_is_enabled_by_default() {
        let cli = parse(&["shard", "health"]);

        assert!(
            shard_health_should_gate(&cli),
            "shard health is a rollout gate by default"
        );
    }

    #[test]
    fn shard_health_gate_treats_degraded_and_unavailable_as_failures() {
        let degraded = json!({
            "shards": [{
                "shard_id": 0,
                "roles": ["readable", "writable", "default"],
                "candidate": false,
                "readiness": "degraded"
            }]
        });
        let unavailable = json!({
            "shards": [{
                "shard_id": 1,
                "roles": ["readable", "writable"],
                "candidate": false,
                "readiness": "unavailable"
            }]
        });

        assert_eq!(shard_health_exit_code(&degraded), 1);
        assert_eq!(shard_health_exit_code(&unavailable), 1);
    }

    #[test]
    fn shard_health_gate_fails_three_shard_rollout_with_uncovered_writable_shard() {
        let payload = json!({
            "shards": [
                {
                    "shard_id": 0,
                    "roles": ["readable", "writable", "default"],
                    "candidate": false,
                    "readiness": "ready"
                },
                {
                    "shard_id": 1,
                    "roles": ["readable", "writable"],
                    "candidate": false,
                    "readiness": "ready"
                },
                {
                    "shard_id": 2,
                    "roles": ["readable", "writable"],
                    "candidate": false,
                    "readiness": "degraded",
                    "reason_codes": ["worker_queue_uncovered"]
                }
            ]
        });

        assert_eq!(shard_health_exit_code(&payload), 1);
    }

    #[test]
    fn version_usage_default_output_renders_compact_table() {
        let cli = parse(&["version-usage"]);
        let payload = json!({
            "status": "complete",
            "observed_at": "2026-05-07T12:00:00Z",
            "items": [{
                "workflow_name": "billing_checkout",
                "change_id": "billing_checkout_v2_tax",
                "recorded_version": 1,
                "active_executions": 1,
                "terminal_executions": 2,
                "oldest_matching_execution_age_secs": 3600,
                "newest_matching_execution_age_secs": 60,
                "shard_coverage": {
                    "inspected_shards": [0, 1],
                    "matched_shards": [0],
                    "unavailable_shards": []
                }
            }],
            "shards": []
        });

        let rendered = render_response(&cli, &payload).expect("table output should render");

        assert!(rendered.contains("WORKFLOW"));
        assert!(rendered.contains("billing_checkout"));
        assert!(rendered.contains("billing_checkout_v2_tax"));
        assert!(!rendered.trim_start().starts_with('{'));
    }

    #[test]
    fn version_usage_json_output_preserves_raw_payload_shape() {
        let cli = parse(&["--output", "json", "version-usage"]);
        let payload = json!({
            "status": "no_matches",
            "observed_at": "2026-05-07T12:00:00Z",
            "items": [],
            "shards": []
        });

        let rendered = render_response(&cli, &payload).expect("json output should render");

        assert_eq!(
            rendered,
            r#"{"items":[],"observed_at":"2026-05-07T12:00:00Z","shards":[],"status":"no_matches"}"#
        );
    }

    #[test]
    fn version_usage_guard_fails_on_active_usage_or_incomplete_shards() {
        let active = json!({
            "status": "complete",
            "items": [{ "active_executions": 1 }]
        });
        assert_eq!(version_usage_guard_exit_code(&active), 1);

        let drained = json!({
            "status": "complete",
            "items": [{ "active_executions": 0, "terminal_executions": 4 }]
        });
        assert_eq!(version_usage_guard_exit_code(&drained), 0);

        let partial = json!({
            "status": "partial",
            "items": []
        });
        assert_eq!(version_usage_guard_exit_code(&partial), 1);
    }

    // ─── Workflow-type reachability CLI tests (issue #520) ───────────────────

    #[test]
    fn workflow_reachability_builds_unfiltered_request() {
        let cli = parse(&["workflow-types", "reachability"]);
        let req = cli.api_request().expect("request should build");
        assert_eq!(req.method, ApiMethod::Get);
        assert_eq!(req.path, "/admin/workflow-types/reachability");
        assert!(req.body.is_none());
    }

    #[test]
    fn workflow_reachability_threads_type_filter() {
        let cli = parse(&["workflow-types", "reachability", "--type", "onboarding"]);
        let req = cli.api_request().expect("request should build");
        assert_eq!(
            req.path,
            "/admin/workflow-types/reachability?workflow_type=onboarding"
        );
    }

    #[test]
    fn workflow_reachability_exit_code_zero_when_all_safe_or_in_use() {
        let value = json!({
            "status": "complete",
            "items": [
                { "verdict": "safe_to_remove" },
                { "verdict": "in_use" }
            ]
        });
        assert_eq!(workflow_reachability_exit_code(&value), 0);
    }

    #[test]
    fn workflow_reachability_exit_code_two_on_orphaned() {
        let value = json!({
            "status": "complete",
            "items": [
                { "verdict": "safe_to_remove" },
                { "verdict": "orphaned" }
            ]
        });
        assert_eq!(workflow_reachability_exit_code(&value), 2);
    }

    #[test]
    fn workflow_reachability_exit_code_two_on_partial_report() {
        // A partial answer must never be mistaken for safe-to-remove: fail closed.
        let value = json!({ "status": "partial", "items": [] });
        assert_eq!(workflow_reachability_exit_code(&value), 2);
        let unavailable = json!({ "status": "unavailable", "items": [] });
        assert_eq!(workflow_reachability_exit_code(&unavailable), 2);
    }

    // ─── Queue coverage CLI tests (issue #774) ────────────────────────────────

    #[test]
    fn queue_coverage_builds_unfiltered_request() {
        let cli = parse(&["queue", "coverage"]);
        let req = cli.api_request().expect("request should build");
        assert_eq!(req.method, ApiMethod::Get);
        assert_eq!(req.path, "/admin/queue-coverage");
        assert!(req.body.is_none());
    }

    #[test]
    fn queue_coverage_threads_queue_name_filter() {
        let cli = parse(&["queue", "coverage", "--queue", "email-workers"]);
        let req = cli.api_request().expect("request should build");
        assert_eq!(req.path, "/admin/queue-coverage?queue_name=email-workers");
    }

    #[test]
    fn queue_coverage_query_encodes_the_filter() {
        let cli = parse(&["queue", "coverage", "--queue", "weird queue&name"]);
        let req = cli.api_request().expect("request should build");
        assert_eq!(
            req.path,
            "/admin/queue-coverage?queue_name=weird%20queue%26name"
        );
    }

    #[test]
    fn queue_coverage_exit_code_zero_when_fully_covered() {
        let value = json!({
            "status": "complete",
            "uncovered": false,
            "items": []
        });
        assert_eq!(queue_coverage_exit_code(&value), 0);
    }

    #[test]
    fn queue_coverage_exit_code_two_on_uncovered_queue() {
        let value = json!({
            "status": "complete",
            "uncovered": true,
            "items": [{ "queue_name": "typo_queue", "pending_count": 5 }]
        });
        assert_eq!(queue_coverage_exit_code(&value), 2);
    }

    #[test]
    fn queue_coverage_exit_code_two_on_partial_report() {
        // A partial answer must never be mistaken for fully covered: fail closed.
        let value = json!({ "status": "partial", "uncovered": false, "items": [] });
        assert_eq!(queue_coverage_exit_code(&value), 2);
        let unavailable = json!({ "status": "unavailable", "uncovered": false, "items": [] });
        assert_eq!(queue_coverage_exit_code(&unavailable), 2);
    }

    #[test]
    fn queue_coverage_should_gate_only_matches_coverage_subcommand() {
        assert!(queue_coverage_should_gate(&parse(&["queue", "coverage"])));
        assert!(!queue_coverage_should_gate(&parse(&[
            "queue",
            "list-paused"
        ])));
        assert!(!queue_coverage_should_gate(&parse(&["shard", "health"])));
    }

    #[test]
    fn queue_coverage_table_lists_uncovered_queues_with_samples() {
        let value = json!({
            "status": "complete",
            "observed_at": "2026-08-07T00:00:00Z",
            "total_uncovered_queues": 1,
            "uncovered": true,
            "items": [
                {
                    "queue_name": "typo_queue",
                    "pending_count": 42,
                    "sample_task_ids": ["11111111-1111-1111-1111-111111111111"],
                    "sample_execution_ids": ["22222222-2222-2222-2222-222222222222"],
                    "shard_breakdown": [{ "shard_id": 0, "pending_count": 42 }]
                }
            ],
            "shards": [{ "shard_id": 0, "status": "inspected" }]
        });
        let rendered = format_queue_coverage_table(&value);
        assert!(rendered.contains("typo_queue"));
        assert!(rendered.contains("42"));
        assert!(rendered.contains("11111111-1111-1111-1111-111111111111"));
        assert!(rendered.contains("22222222-2222-2222-2222-222222222222"));
        assert!(!rendered.contains("WARNING"));
    }

    #[test]
    fn queue_coverage_table_reports_fully_covered_with_no_items() {
        let value = json!({
            "status": "complete",
            "observed_at": "2026-08-07T00:00:00Z",
            "total_uncovered_queues": 0,
            "uncovered": false,
            "items": [],
            "shards": [{ "shard_id": 0, "status": "inspected" }]
        });
        let rendered = format_queue_coverage_table(&value);
        assert!(rendered.contains("All queues with pending work are covered."));
    }

    #[test]
    fn queue_coverage_table_warns_on_unavailable_shard() {
        let value = json!({
            "status": "partial",
            "observed_at": "2026-08-07T00:00:00Z",
            "total_uncovered_queues": 0,
            "uncovered": false,
            "items": [],
            "shards": [
                { "shard_id": 0, "status": "inspected" },
                { "shard_id": 1, "status": "unavailable", "error": "connection refused" }
            ]
        });
        let rendered = format_queue_coverage_table(&value);
        assert!(rendered.contains("WARNING: unavailable shards [1]"));
    }

    #[test]
    fn queue_coverage_table_surfaces_excluded_paused_queues() {
        // The paused-but-pollerless exclusion (module docs on
        // `queue_coverage.rs`) must not be invisible outside `--json` --
        // an operator using the default table view needs to see it too.
        let value = json!({
            "status": "complete",
            "observed_at": "2026-08-07T00:00:00Z",
            "total_uncovered_queues": 0,
            "uncovered": false,
            "items": [],
            "shards": [{ "shard_id": 0, "status": "inspected" }],
            "excluded_paused_queues": ["seasonal-batch"]
        });
        let rendered = format_queue_coverage_table(&value);
        assert!(rendered.contains("All queues with pending work are covered."));
        assert!(rendered.contains("NOTE: paused queues"));
        assert!(rendered.contains("seasonal-batch"));
    }

    #[test]
    fn queue_coverage_table_with_uncovered_items_also_surfaces_paused_note() {
        let value = json!({
            "status": "complete",
            "observed_at": "2026-08-07T00:00:00Z",
            "total_uncovered_queues": 1,
            "uncovered": true,
            "items": [
                {
                    "queue_name": "typo_queue",
                    "pending_count": 1,
                    "sample_task_ids": [],
                    "sample_execution_ids": [],
                    "shard_breakdown": [{ "shard_id": 0, "pending_count": 1 }]
                }
            ],
            "shards": [{ "shard_id": 0, "status": "inspected" }],
            "excluded_paused_queues": ["seasonal-batch"]
        });
        let rendered = format_queue_coverage_table(&value);
        assert!(rendered.contains("typo_queue"));
        assert!(rendered.contains("NOTE: paused queues"));
        assert!(rendered.contains("seasonal-batch"));
    }

    #[test]
    fn queue_coverage_table_omits_paused_note_when_nothing_excluded() {
        let value = json!({
            "status": "complete",
            "observed_at": "2026-08-07T00:00:00Z",
            "total_uncovered_queues": 0,
            "uncovered": false,
            "items": [],
            "shards": [{ "shard_id": 0, "status": "inspected" }],
            "excluded_paused_queues": []
        });
        let rendered = format_queue_coverage_table(&value);
        assert!(!rendered.contains("NOTE: paused queues"));
    }

    #[test]
    fn queue_coverage_wants_raw_json_only_with_the_json_flag() {
        assert!(queue_coverage_wants_raw_json(&parse(&[
            "queue", "coverage", "--json"
        ])));
        assert!(!queue_coverage_wants_raw_json(&parse(&[
            "queue", "coverage"
        ])));
        assert!(!queue_coverage_wants_raw_json(&parse(&["shard", "health"])));
    }

    #[test]
    fn queue_coverage_wants_table_only_without_the_json_flag_in_pretty_mode() {
        assert!(queue_coverage_wants_table(&parse(&["queue", "coverage"])));
        assert!(!queue_coverage_wants_table(&parse(&[
            "queue", "coverage", "--json"
        ])));
    }

    #[test]
    fn queue_coverage_gate_maps_to_exit_code_two() {
        let error = CliError::QueueCoverageGate {
            context: "uncovered queue".to_string(),
        };
        assert_eq!(error.exit_code(), 2);
    }

    #[test]
    fn path_segment_encodes_both_path_separators() {
        // The URL parser treats `\` as a path separator for http/https, so an
        // unencoded backslash splits one value into extra segments and the
        // request silently lands on a different route. Route-wide: every caller
        // of `path_segment` (queue names, workflow ids, keys, ...) depends on
        // this, not just queue pause/resume.
        assert_eq!(path_segment(r"payments\eu"), "payments%5Ceu");
        assert_eq!(path_segment(r"a\b\c"), "a%5Cb%5Cc");
        assert_eq!(path_segment("payments/eu"), "payments%2Feu");
        // Worse than a missed route: a backslash re-enables `..` traversal
        // inside what should be one opaque segment, past the whole-segment
        // dot-segment guard.
        assert_eq!(path_segment(r"payments\..\admin"), "payments%5C..%5Cadmin");
        // An ordinary name is untouched.
        assert_eq!(path_segment("orders-eu"), "orders-eu");
    }

    /// Exhaustive guard for the whole path-separator / normalization class.
    ///
    /// Every printable ASCII byte, embedded in a value that becomes one path
    /// segment, must survive `path_segment` + URL parsing as **exactly one**
    /// segment that decodes back to the original. This is the test that would
    /// have caught the missing `\` (the URL parser treats it as a path
    /// separator for http/https), and it fails if any separator-class character
    /// is ever dropped from `PATH_SEGMENT_ENCODE_SET`.
    ///
    /// Scope: *embedded* bytes. A whole-segment `.`/`..` cannot be encoded away
    /// at all and is rejected instead (`is_url_dot_segment`); control
    /// characters are covered by `CONTROLS`.
    #[test]
    fn every_printable_ascii_byte_survives_as_exactly_one_path_segment() {
        let mut offenders = Vec::new();
        for byte in 0x20u8..=0x7e {
            let raw = format!("q{}z", byte as char);
            let encoded = path_segment(&raw);
            let url = format!("http://host/admin/queues/{encoded}/pause");
            let parsed = reqwest::Url::parse(&url)
                .unwrap_or_else(|e| panic!("byte {byte:#04x} produced an unparseable URL: {e}"));
            let segments: Vec<&str> = parsed.path().trim_start_matches('/').split('/').collect();
            let intact = segments.len() == 4 && percent_decode(segments[2]) == raw;
            if !intact {
                offenders.push(format!(
                    "byte {byte:#04x} ({:?}) encoded to {encoded:?} but parsed as {:?}",
                    byte as char,
                    parsed.path()
                ));
            }
        }
        assert!(
            offenders.is_empty(),
            "these bytes did not survive as one intact path segment:\n{}",
            offenders.join("\n")
        );
    }

    #[test]
    fn non_ascii_queue_names_survive_as_exactly_one_path_segment() {
        // The exhaustive sweep above can only walk printable ASCII, because a
        // lone byte >= 0x80 is not valid UTF-8 and so is not expressible as a
        // `&str` queue name at all. The reachable non-ASCII case is a real
        // multi-byte character, and it is worth pinning rather than assuming:
        // `utf8_percent_encode` percent-encodes every non-ASCII byte regardless
        // of the `AsciiSet`, so such a name has no separator hazard -- but that
        // is a property of the encoding crate, and this asserts we actually get
        // it instead of trusting it.
        for raw in [
            "paiements-europe-é",
            "支払い",
            "очередь",
            "emoji-🚀-queue",
            // A combining sequence, so normalization differences would show up.
            "e\u{0301}-queue",
        ] {
            let encoded = path_segment(raw);
            let url = format!("http://host/admin/queues/{encoded}/pause");
            let parsed = reqwest::Url::parse(&url)
                .unwrap_or_else(|e| panic!("{raw:?} produced an unparseable URL: {e}"));
            let segments: Vec<&str> = parsed.path().trim_start_matches('/').split('/').collect();
            assert_eq!(
                segments.len(),
                4,
                "{raw:?} encoded to {encoded:?} but split into {:?}",
                parsed.path()
            );
            assert_eq!(
                percent_decode(segments[2]),
                raw,
                "{raw:?} encoded to {encoded:?} but did not decode back intact"
            );
        }
    }

    fn percent_decode(value: &str) -> String {
        percent_encoding::percent_decode_str(value)
            .decode_utf8_lossy()
            .to_string()
    }

    #[test]
    fn queue_mutation_exit_code_fails_on_a_partial_hold() {
        // Issue #619: a 207 partial fleet-wide hold is NOT in effect on the
        // shards it missed -- those keep dispatching into the outage. It must
        // never look like success to a script or a runbook step.
        assert_eq!(
            queue_mutation_exit_code(&json!({ "ok": false, "status": "partial" })),
            1
        );
        assert_eq!(
            queue_mutation_exit_code(&json!({ "ok": true, "status": "complete" })),
            0
        );
    }

    #[test]
    fn queue_mutation_exit_code_gates_on_either_signal_independently() {
        // `ok` and `status` are belt-and-braces: either one reporting a
        // non-complete application is enough to fail the command.
        assert_eq!(queue_mutation_exit_code(&json!({ "ok": false })), 1);
        assert_eq!(queue_mutation_exit_code(&json!({ "status": "partial" })), 1);
        // A body missing both signals is not a queue-mutation response we can
        // vouch for -- fail closed rather than reporting a hold that may not hold.
        assert_eq!(queue_mutation_exit_code(&json!({})), 1);
    }

    #[test]
    fn queue_mutation_gate_applies_only_to_the_mutating_subcommands() {
        assert!(queue_mutation_should_gate(&parse(&[
            "queue", "pause", "q", "--reason", "x"
        ])));
        assert!(queue_mutation_should_gate(&parse(&[
            "queue", "resume", "q"
        ])));
        assert!(
            !queue_mutation_should_gate(&parse(&["queue", "list-paused"])),
            "the read route has no partial-application contract to gate on"
        );
        assert!(!queue_mutation_should_gate(&parse(&["health"])));
    }

    #[test]
    fn queue_partial_mutation_error_uses_exit_code_one() {
        assert_eq!(
            CliError::QueuePartialMutation {
                detail: "shard 1 unreachable".to_string()
            }
            .exit_code(),
            1
        );
    }

    // ─── Per-activity-type pause/resume (issue #807) ─────────────────────────

    #[test]
    fn activity_mutation_gate_applies_only_to_the_mutating_subcommands() {
        assert!(activity_mutation_should_gate(&parse(&[
            "activity",
            "pause",
            "charge_card"
        ])));
        assert!(activity_mutation_should_gate(&parse(&[
            "activity",
            "resume",
            "charge_card"
        ])));
        assert!(
            !activity_mutation_should_gate(&parse(&["activity", "list"])),
            "the read route has no partial-application contract to gate on"
        );
        assert!(!activity_mutation_should_gate(&parse(&[
            "activity",
            "get",
            "charge_card"
        ])));
        assert!(!activity_mutation_should_gate(&parse(&["health"])));
        assert!(
            !activity_mutation_should_gate(&parse(&["queue", "pause", "q", "--reason", "x"])),
            "the queue gate and the activity gate must not fire for each other"
        );
    }

    #[test]
    fn activity_partial_mutation_error_uses_exit_code_one() {
        assert_eq!(
            CliError::ActivityPartialMutation {
                detail: "shard 1 unreachable".to_string()
            }
            .exit_code(),
            1
        );
    }

    #[test]
    fn activity_name_dot_segment_error_uses_exit_code_one() {
        assert_eq!(
            CliError::ActivityNameDotSegment {
                value: "..".to_string()
            }
            .exit_code(),
            1
        );
    }

    #[test]
    fn activity_list_renders_a_table_by_default_and_raw_json_on_demand() {
        assert!(
            activity_list_wants_table(&parse(&["activity", "list"])),
            "the table is the default rendering, not an opt-in"
        );
        assert!(!activity_list_wants_raw_json(&parse(&["activity", "list"])));

        assert!(!activity_list_wants_table(&parse(&[
            "activity", "list", "--json"
        ])));
        assert!(activity_list_wants_raw_json(&parse(&[
            "activity", "list", "--json"
        ])));

        // `--output json` is the global format flag and must keep suppressing
        // the table on its own, exactly as it does for every other list read.
        assert!(!activity_list_wants_table(&parse(&[
            "--output", "json", "activity", "list"
        ])));

        // The table gate is scoped to `list`: the mutations and the single-name
        // read still fall through to the shared JSON rendering.
        assert!(!activity_list_wants_table(&parse(&[
            "activity",
            "get",
            "charge_card"
        ])));
        assert!(!activity_list_wants_table(&parse(&[
            "activity",
            "pause",
            "charge_card"
        ])));
    }

    #[test]
    fn activity_list_table_gate_survives_the_documented_aliases() {
        // The aliases are the muscle-memory spellings an operator reaches for
        // mid-incident; they must land on the table, not raw JSON.
        for alias in ["list", "list-paused", "status"] {
            assert!(
                activity_list_wants_table(&parse(&["activity", alias])),
                "`activity {alias}` should render the table"
            );
        }
    }

    #[test]
    fn activity_list_table_surfaces_the_hold_and_its_provenance() {
        let value = json!({
            "status": "complete",
            "activities": [
                {
                    "activity_name": "charge_card",
                    "registered": true,
                    "queue_name": "payments",
                    "is_local": false,
                    "paused": true,
                    "paused_reason": "stripe outage",
                    "paused_actor": "alice",
                    "paused_at": "2026-08-17T00:00:00Z",
                    "scope_shard_id": null,
                    "held_task_count": 42,
                    "provenance_uniform": true,
                    "shards": []
                },
                {
                    "activity_name": "send_email",
                    "registered": true,
                    "queue_name": "email",
                    "is_local": false,
                    "paused": false,
                    "paused_reason": null,
                    "paused_actor": null,
                    "paused_at": null,
                    "scope_shard_id": null,
                    "held_task_count": 0,
                    "provenance_uniform": true,
                    "shards": []
                }
            ],
            "unavailable_shards": []
        });
        let rendered = format_activity_list_table(&value);

        assert!(rendered.starts_with("status: complete"));
        assert!(rendered.contains("ACTIVITY"));
        assert!(rendered.contains("charge_card"));
        assert!(rendered.contains("payments"));
        assert!(rendered.contains("42"), "held backlog must be visible");
        assert!(rendered.contains("stripe outage"), "why it is held");
        assert!(rendered.contains("alice"), "who held it");
        // The healthy row is listed too — this read answers "is it held?", so a
        // flowing activity has to appear rather than be absent-by-omission.
        assert!(rendered.contains("send_email"));
    }

    #[test]
    fn activity_list_table_flags_a_no_op_hold_on_a_local_activity() {
        // A local activity runs inline and never takes a task-queue row, so a
        // hold on one holds nothing. Without the LOCAL column that row reads as
        // a successful containment action when it did nothing at all.
        let value = json!({
            "status": "complete",
            "activities": [{
                "activity_name": "compute_checksum",
                "registered": true,
                "queue_name": null,
                "is_local": true,
                "paused": true,
                "paused_reason": "held by mistake",
                "paused_actor": "bob",
                "held_task_count": 0
            }],
            "unavailable_shards": []
        });
        let rendered = format_activity_list_table(&value);
        assert!(rendered.contains("LOCAL"));
        assert!(rendered.contains("compute_checksum"));
    }

    /// Issue #807 review: a partially-applied fleet-wide hold must not render
    /// as a clean containment action.
    ///
    /// `PAUSED: yes` on its own is exactly the misleading signal: the shards a
    /// fleet-wide pause reached hold rows byte-identical to a complete hold, and
    /// the ones it missed hold no row at all, so an operator reading the table
    /// during an incident would believe `charge_card` is stopped while part of
    /// the fleet keeps dispatching it. `SCOPE` is the column that says otherwise.
    #[test]
    fn activity_list_table_flags_a_partially_applied_fleet_hold() {
        let value = json!({
            "status": "complete",
            "activities": [{
                "activity_name": "charge_card",
                "registered": true,
                "queue_name": "payments",
                "is_local": false,
                "paused": true,
                "effective_scope": "partial_fleet",
                "paused_reason": "stripe outage",
                "paused_actor": "alice",
                "scope_shard_id": null,
                "held_task_count": 4,
                "provenance_uniform": true,
                "shards": []
            }],
            "unavailable_shards": []
        });
        let rendered = format_activity_list_table(&value);
        assert!(rendered.contains("SCOPE"), "the coverage column must exist");
        assert!(
            rendered.contains("partial_fleet"),
            "an incomplete hold must say so on the surface an operator reads \
             during the incident, not only in --json: {rendered}"
        );
    }

    #[test]
    fn activity_list_table_keeps_status_visible_on_a_partial_read() {
        // The contract warns that on a partial read `paused: false` means only
        // "not held on the shards that answered". The status line is what stops
        // an operator trusting that negative, so it must render even though the
        // per-shard detail goes to STDERR via `fanout_partial_notice`.
        let value = json!({
            "status": "partial",
            "activities": [{
                "activity_name": "charge_card",
                "registered": true,
                "queue_name": "payments",
                "is_local": false,
                "paused": false,
                "held_task_count": 0
            }],
            "unavailable_shards": [{ "shard_id": 1, "reason": "connect timeout" }]
        });
        let rendered = format_activity_list_table(&value);
        assert!(
            rendered.starts_with("status: partial"),
            "a degraded read must not present a clean negative: {rendered}"
        );
    }

    #[test]
    fn activity_list_table_handles_empty_and_malformed_bodies() {
        let empty = format_activity_list_table(&json!({
            "status": "complete",
            "activities": [],
            "unavailable_shards": []
        }));
        assert!(empty.contains("status: complete"));
        assert!(empty.contains("No activities found."));

        // Defensive: never panic on a body missing the list entirely, and still
        // report the status the operator needs.
        let malformed = format_activity_list_table(&json!({ "status": "unavailable" }));
        assert!(malformed.contains("status: unavailable"));
        assert!(malformed.contains("No activities returned."));
    }

    #[test]
    fn workflow_reachability_gate_error_uses_exit_code_two() {
        assert_eq!(
            CliError::WorkflowReachabilityGate {
                context: String::new()
            }
            .exit_code(),
            2
        );
    }

    #[test]
    fn workflow_reachability_table_renders_rows_and_unavailable_warning() {
        let cli = parse(&["workflow-types", "reachability"]);
        let value = json!({
            "status": "partial",
            "observed_at": "2026-05-31T00:00:00Z",
            "filter": null,
            "items": [
                {
                    "workflow_type": "legacy_flow",
                    "registered": false,
                    "non_terminal_count": 2,
                    "oldest_non_terminal_age_secs": 3600,
                    "verdict": "orphaned",
                    "shard_breakdown": []
                }
            ],
            "shards": [
                { "shard_id": 0, "status": "inspected", "error": null },
                { "shard_id": 1, "status": "unavailable", "error": "connection refused" }
            ]
        });
        let rendered = render_response(&cli, &value).expect("table should render");
        assert!(rendered.contains("legacy_flow"));
        assert!(rendered.contains("orphaned"));
        assert!(rendered.contains("WARNING: unavailable shards [1]"));
        assert!(!rendered.trim_start().starts_with('{'));
    }

    #[test]
    fn workflow_reachability_json_flag_emits_raw_payload() {
        let cli = parse(&["workflow-types", "reachability", "--json"]);
        let value = json!({
            "status": "complete",
            "observed_at": "2026-05-31T00:00:00Z",
            "filter": null,
            "items": [],
            "shards": []
        });
        let rendered = render_response(&cli, &value).expect("json should render");
        assert!(rendered.trim_start().starts_with('{'));
    }

    // ─── VersionGateRetirement CLI tests ─────────────────────────────────────

    #[test]
    fn version_gate_retirement_builds_correct_api_request() {
        let cli = parse(&[
            "version-gate-retirement",
            "--change-id",
            "tax_v2",
            "--min-safe-version",
            "2",
        ]);
        let req = cli.api_request().expect("request should build");
        assert_eq!(req.method, ApiMethod::Get);
        assert!(
            req.path
                .starts_with("/admin/version-gates/retirement-check"),
            "path should target retirement-check endpoint; got {}",
            req.path
        );
        assert!(req.path.contains("change_id=tax_v2"));
        assert!(req.path.contains("min_safe_version=2"));
    }

    #[test]
    fn version_gate_retirement_includes_optional_workflow_name() {
        let cli = parse(&[
            "version-gate-retirement",
            "--change-id",
            "tax_v2",
            "--min-safe-version",
            "3",
            "--workflow-name",
            "billing_checkout",
        ]);
        let req = cli.api_request().expect("request should build");
        assert!(req.path.contains("workflow_name=billing_checkout"));
    }

    #[test]
    fn retirement_check_exit_code_zero_on_safe() {
        let safe = json!({ "status": "safe", "safe_to_retire": true, "blockers": [] });
        assert_eq!(retirement_check_exit_code(&safe), 0);
    }

    #[test]
    fn retirement_check_exit_code_nonzero_on_blocked() {
        let blocked = json!({
            "status": "blocked",
            "safe_to_retire": false,
            "blockers": [{ "active_executions": 3 }]
        });
        assert_eq!(retirement_check_exit_code(&blocked), 1);
    }

    #[test]
    fn retirement_check_exit_code_nonzero_on_partial() {
        let partial = json!({ "status": "partial", "safe_to_retire": false });
        assert_eq!(retirement_check_exit_code(&partial), 1);
    }

    #[test]
    fn retirement_check_exit_code_nonzero_on_unavailable() {
        let unavailable = json!({ "status": "unavailable", "safe_to_retire": false });
        assert_eq!(retirement_check_exit_code(&unavailable), 1);
    }

    #[test]
    fn version_gate_retirement_check_flag_not_set_by_default() {
        let cli = parse(&[
            "version-gate-retirement",
            "--change-id",
            "tax_v2",
            "--min-safe-version",
            "2",
        ]);
        assert!(!retirement_check_should_check(&cli));
    }

    #[test]
    fn version_gate_retirement_check_flag_set_when_passed() {
        let cli = parse(&[
            "version-gate-retirement",
            "--change-id",
            "tax_v2",
            "--min-safe-version",
            "2",
            "--check",
        ]);
        assert!(retirement_check_should_check(&cli));
    }

    #[test]
    fn version_gate_retirement_default_output_renders_table() {
        let cli = parse(&[
            "version-gate-retirement",
            "--change-id",
            "tax_v2",
            "--min-safe-version",
            "2",
        ]);
        let payload = json!({
            "status": "blocked",
            "safe_to_retire": false,
            "observed_at": "2026-05-07T12:00:00Z",
            "filters": {
                "change_id": "tax_v2",
                "min_safe_version": 2,
                "workflow_name": null,
                "state_group": "all",
                "shard_id": null
            },
            "blockers": [{
                "workflow_name": "billing_checkout",
                "change_id": "tax_v2",
                "recorded_version": 1,
                "active_executions": 2,
                "terminal_executions": 5,
                "oldest_blocker_age_secs": 3600,
                "newest_blocker_age_secs": 60,
                "sample_active_execution_ids": [],
                "shard_coverage": {
                    "inspected_shards": [0],
                    "matched_shards": [0],
                    "unavailable_shards": []
                }
            }],
            "shards": [{ "shard_id": 0, "status": "inspected", "matched_groups": 1, "error": null }]
        });

        let rendered = render_response(&cli, &payload).expect("table should render");

        assert!(rendered.contains("WORKFLOW"));
        assert!(rendered.contains("billing_checkout"));
        assert!(rendered.contains("blocked"));
        assert!(rendered.contains("tax_v2"));
        assert!(!rendered.trim_start().starts_with('{'));
    }

    #[test]
    fn version_gate_retirement_json_output_preserves_raw_payload() {
        let cli = parse(&[
            "--output",
            "json",
            "version-gate-retirement",
            "--change-id",
            "tax_v2",
            "--min-safe-version",
            "2",
        ]);
        let payload = json!({
            "status": "safe",
            "safe_to_retire": true,
            "observed_at": "2026-05-07T12:00:00Z",
            "filters": {},
            "blockers": [],
            "shards": []
        });

        let rendered = render_response(&cli, &payload).expect("json output should render");

        assert!(rendered.trim_start().starts_with('{'));
        assert!(rendered.contains("\"safe_to_retire\":true"));
    }

    // -- Worker subcommand (issue #170) --

    #[test]
    fn worker_drain_builds_post_request() {
        let req = parse(&["worker", "drain", "w-abc"]).api_request().unwrap();
        assert_eq!(req.method, ApiMethod::Post);
        assert_eq!(req.path, "/workers/w-abc/drain");
        assert!(req.body.is_some());
    }

    #[test]
    fn worker_drain_without_deadline_sends_empty_body() {
        let req = parse(&["worker", "drain", "w-abc"]).api_request().unwrap();
        let body = req.body.as_ref().unwrap();
        assert!(body.get("deadline_at").is_none());
    }

    #[test]
    fn worker_drain_with_deadline_includes_deadline_in_body() {
        let req = parse(&[
            "worker",
            "drain",
            "w-abc",
            "--deadline",
            "2026-05-09T12:00:00Z",
        ])
        .api_request()
        .unwrap();
        let body = req.body.as_ref().unwrap();
        assert_eq!(
            body["deadline_at"].as_str().unwrap(),
            "2026-05-09T12:00:00Z"
        );
    }

    #[test]
    fn worker_drain_preview_builds_get_request() {
        let req = parse(&["worker", "drain-preview"]).api_request().unwrap();
        assert_eq!(req.method, ApiMethod::Get);
        assert_eq!(req.path, "/workers/drain-preview");
        assert!(req.body.is_none());
    }

    #[test]
    fn worker_drain_preview_with_queue_filter_sends_param() {
        let req = parse(&["worker", "drain-preview", "--queue", "email-workers"])
            .api_request()
            .unwrap();
        assert_eq!(req.method, ApiMethod::Get);
        assert!(
            req.path.contains("queue=email-workers"),
            "path: {}",
            req.path
        );
    }

    #[test]
    fn worker_drain_preview_with_shard_filter_sends_param() {
        let req = parse(&["worker", "drain-preview", "--shard-id", "2"])
            .api_request()
            .unwrap();
        assert!(req.path.contains("shard_id=2"), "path: {}", req.path);
    }

    #[test]
    fn worker_drain_preview_with_status_filter_sends_param() {
        let req = parse(&["worker", "drain-preview", "--status", "Active"])
            .api_request()
            .unwrap();
        assert!(req.path.contains("status=Active"), "path: {}", req.path);
    }

    #[test]
    fn worker_list_builds_get_request() {
        let req = parse(&["worker", "list"]).api_request().unwrap();
        assert_eq!(req.method, ApiMethod::Get);
        assert_eq!(req.path, "/workers");
    }

    #[test]
    fn worker_list_with_status_filter_sends_param() {
        let req = parse(&["worker", "list", "--status", "Draining"])
            .api_request()
            .unwrap();
        assert!(req.path.contains("status=Draining"), "path: {}", req.path);
    }

    #[test]
    fn worker_list_with_queue_filter_sends_param() {
        let req = parse(&["worker", "list", "--queue", "default"])
            .api_request()
            .unwrap();
        assert!(req.path.contains("queue=default"), "path: {}", req.path);
    }

    #[test]
    fn worker_get_builds_get_request() {
        let req = parse(&["worker", "get", "w-xyz"]).api_request().unwrap();
        assert_eq!(req.method, ApiMethod::Get);
        assert_eq!(req.path, "/workers/w-xyz");
    }

    #[test]
    fn worker_health_builds_get_request() {
        let req = parse(&["worker", "health"]).api_request().unwrap();
        assert_eq!(req.method, ApiMethod::Get);
        assert_eq!(req.path, "/workers/health");
    }

    // -- Drain wait mode (AC #6) --

    #[test]
    fn worker_drain_with_wait_flag_still_builds_drain_post_request() {
        let req = parse(&["worker", "drain", "w-abc", "--wait"])
            .api_request()
            .unwrap();
        assert_eq!(req.method, ApiMethod::Post);
        assert_eq!(req.path, "/workers/w-abc/drain");
    }

    #[test]
    fn worker_drain_wait_and_deadline_are_independent_flags() {
        let req = parse(&[
            "worker",
            "drain",
            "w-abc",
            "--wait",
            "--deadline",
            "2026-05-09T12:00:00Z",
        ])
        .api_request()
        .unwrap();
        let body = req.body.as_ref().unwrap();
        assert_eq!(
            body["deadline_at"].as_str().unwrap(),
            "2026-05-09T12:00:00Z"
        );
    }

    #[test]
    fn worker_drain_wait_timeout_secs_default_is_120() {
        let cli = parse(&["worker", "drain", "w-abc", "--wait"]);
        if let Commands::Worker {
            command:
                WorkerCommand::Drain {
                    wait,
                    wait_timeout_secs,
                    ..
                },
        } = &cli.command
        {
            assert!(*wait);
            assert_eq!(*wait_timeout_secs, 120);
        } else {
            panic!("expected Worker::Drain command");
        }
    }

    #[test]
    fn worker_drain_without_wait_flag_wait_is_false() {
        let cli = parse(&["worker", "drain", "w-abc"]);
        if let Commands::Worker {
            command: WorkerCommand::Drain { wait, .. },
        } = &cli.command
        {
            assert!(!*wait);
        } else {
            panic!("expected Worker::Drain command");
        }
    }

    #[test]
    fn version_gate_retirement_empty_blockers_shows_no_old_version_message() {
        let cli = parse(&[
            "version-gate-retirement",
            "--change-id",
            "tax_v2",
            "--min-safe-version",
            "2",
        ]);
        let payload = json!({
            "status": "safe",
            "safe_to_retire": true,
            "observed_at": "2026-05-07T12:00:00Z",
            "filters": {
                "change_id": "tax_v2",
                "min_safe_version": 2,
                "workflow_name": null,
                "state_group": "all",
                "shard_id": null
            },
            "blockers": [],
            "shards": [{ "shard_id": 0, "status": "inspected", "matched_groups": 0, "error": null }]
        });

        let rendered = render_response(&cli, &payload).expect("table should render");

        assert!(rendered.contains("No old-version executions found"));
    }

    #[test]
    fn rate_limit_status_builds_get_request() {
        let req = parse(&["rate-limit", "status"]).api_request().unwrap();
        assert_eq!(req.method, ApiMethod::Get);
        assert_eq!(req.path, "/admin/rate-limits");
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn rate_limit_set_builds_post_request() {
        let req = parse(&[
            "rate-limit",
            "set",
            "my-key",
            "--refill-rate",
            "10.5",
            "--burst",
            "20",
        ])
        .api_request()
        .unwrap();
        assert_eq!(req.method, ApiMethod::Post);
        assert_eq!(req.path, "/admin/rate-limits/my-key");
        let body = req.body.as_ref().unwrap();
        assert_eq!(body["refill_rate"].as_f64().unwrap(), 10.5);
        assert_eq!(body["burst"].as_f64().unwrap(), 20.0);
    }

    #[test]
    fn rate_limit_table_renders_headers_and_rows() {
        let cli = parse(&["rate-limit", "status"]);
        let payload = json!([
            {
                "key": "test-key-1",
                "refill_rate": 5.0,
                "burst": 10.0,
                "tokens": 8.5,
                "last_refilled_at": "2026-05-22T22:00:00Z"
            }
        ]);
        let rendered = render_response(&cli, &payload).unwrap();
        assert!(rendered.contains("KEY"));
        assert!(rendered.contains("REFILL_RATE"));
        assert!(rendered.contains("BURST_CAPACITY"));
        assert!(rendered.contains("CURRENT_TOKENS"));
        assert!(rendered.contains("LAST_REFILLED_AT"));
        assert!(rendered.contains("test-key-1"));
        assert!(rendered.contains("5.00"));
        assert!(rendered.contains("10.00"));
        assert!(rendered.contains("8.50"));
        assert!(rendered.contains("2026-05-22T22:00:00Z"));
    }

    // ── TTL'd runtime pacing overrides (issue #945) ─────────────────────

    #[test]
    #[allow(clippy::float_cmp)]
    fn rate_limit_override_builds_post_request_with_both_fields() {
        let req = parse(&[
            "rate-limit",
            "override",
            "send_email",
            "--refill-rate",
            "50",
            "--burst",
            "100",
            "--ttl-secs",
            "300",
        ])
        .api_request()
        .unwrap();
        assert_eq!(req.method, ApiMethod::Post);
        assert_eq!(req.path, "/admin/rate-limits/send_email/override");
        let body = req.body.as_ref().unwrap();
        assert_eq!(body["refill_rate"].as_f64().unwrap(), 50.0);
        assert_eq!(body["burst"].as_f64().unwrap(), 100.0);
        assert_eq!(body["ttl_secs"].as_u64().unwrap(), 300);
    }

    #[test]
    fn rate_limit_override_omits_unset_optional_fields() {
        let req = parse(&[
            "rate-limit",
            "override",
            "send_email",
            "--burst",
            "100",
            "--ttl-secs",
            "60",
        ])
        .api_request()
        .unwrap();
        let body = req.body.as_ref().unwrap();
        assert!(body["refill_rate"].is_null());
        assert!(!body["burst"].is_null());
    }

    #[test]
    fn rate_limit_override_url_escapes_activity_name() {
        let req = parse(&[
            "rate-limit",
            "override",
            "weird name",
            "--refill-rate",
            "1",
            "--ttl-secs",
            "60",
        ])
        .api_request()
        .unwrap();
        assert!(!req.path.contains(' '));
        assert!(req.path.starts_with("/admin/rate-limits/"));
        assert!(req.path.ends_with("/override"));
    }

    #[test]
    fn rate_limit_clear_builds_delete_request_with_no_body() {
        let req = parse(&["rate-limit", "clear", "send_email"])
            .api_request()
            .unwrap();
        assert_eq!(req.method, ApiMethod::Delete);
        assert_eq!(req.path, "/admin/rate-limits/send_email/override");
        assert!(req.body.is_none());
    }

    #[test]
    fn throttle_status_builds_get_request() {
        let req = parse(&["throttle", "status"]).api_request().unwrap();
        assert_eq!(req.method, ApiMethod::Get);
        assert_eq!(req.path, "/admin/start-throttle");
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn throttle_override_builds_post_request_with_both_fields() {
        let req = parse(&[
            "throttle",
            "override",
            "onboard_user",
            "--refill-per-sec",
            "2.5",
            "--burst",
            "5",
            "--ttl-secs",
            "600",
        ])
        .api_request()
        .unwrap();
        assert_eq!(req.method, ApiMethod::Post);
        assert_eq!(req.path, "/admin/start-throttle/onboard_user/override");
        let body = req.body.as_ref().unwrap();
        assert_eq!(body["refill_per_sec"].as_f64().unwrap(), 2.5);
        assert_eq!(body["burst"].as_f64().unwrap(), 5.0);
        assert_eq!(body["ttl_secs"].as_u64().unwrap(), 600);
    }

    #[test]
    fn throttle_override_omits_unset_optional_fields() {
        let req = parse(&[
            "throttle",
            "override",
            "onboard_user",
            "--refill-per-sec",
            "2.5",
            "--ttl-secs",
            "60",
        ])
        .api_request()
        .unwrap();
        let body = req.body.as_ref().unwrap();
        assert!(!body["refill_per_sec"].is_null());
        assert!(body["burst"].is_null());
    }

    #[test]
    fn throttle_clear_builds_delete_request_with_no_body() {
        let req = parse(&["throttle", "clear", "onboard_user"])
            .api_request()
            .unwrap();
        assert_eq!(req.method, ApiMethod::Delete);
        assert_eq!(req.path, "/admin/start-throttle/onboard_user/override");
        assert!(req.body.is_none());
    }

    #[test]
    fn throttle_alias_throttles_resolves_to_the_same_command() {
        let req = parse(&["throttles", "status"]).api_request().unwrap();
        assert_eq!(req.method, ApiMethod::Get);
        assert_eq!(req.path, "/admin/start-throttle");
    }
}

#[cfg(test)]
mod conflict_policy_tests {
    //! CLI mapping tests for `--conflict-policy` on `workflow start` (issue #685).
    //! Mirror `mod reuse_policy_tests`: omit → no field; each of the 4 values
    //! sends the correct `snake_case` string; preserves other fields alongside.
    use super::*;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("harvest").chain(args.iter().copied()))
            .expect("CLI should parse successfully")
    }

    fn start_request(args: &[&str]) -> ApiRequest {
        parse(args)
            .api_request()
            .expect("request mapping should succeed")
    }

    #[test]
    fn start_omitting_conflict_policy_sends_no_field() {
        let req = start_request(&["workflow", "start", "my_wf"]);
        let body = req.body.as_ref().expect("start should have a body");
        assert!(
            body.get("conflict_policy").is_none(),
            "omitting --conflict-policy must not send the field"
        );
    }

    #[test]
    fn start_unspecified_sends_correct_value() {
        let req = start_request(&[
            "workflow",
            "start",
            "my_wf",
            "--conflict-policy",
            "unspecified",
        ]);
        let body = req.body.as_ref().unwrap();
        assert_eq!(body["conflict_policy"], "unspecified");
    }

    #[test]
    fn start_fail_sends_correct_value() {
        let req = start_request(&["workflow", "start", "my_wf", "--conflict-policy", "fail"]);
        let body = req.body.as_ref().unwrap();
        assert_eq!(body["conflict_policy"], "fail");
    }

    #[test]
    fn start_use_existing_sends_correct_value() {
        let req = start_request(&[
            "workflow",
            "start",
            "my_wf",
            "--conflict-policy",
            "use_existing",
        ]);
        let body = req.body.as_ref().unwrap();
        assert_eq!(body["conflict_policy"], "use_existing");
    }

    #[test]
    fn start_terminate_existing_sends_correct_value() {
        let req = start_request(&[
            "workflow",
            "start",
            "my_wf",
            "--conflict-policy",
            "terminate_existing",
        ]);
        let body = req.body.as_ref().unwrap();
        assert_eq!(body["conflict_policy"], "terminate_existing");
    }

    #[test]
    fn start_preserves_other_fields_alongside_conflict_policy() {
        let req = start_request(&[
            "workflow",
            "start",
            "my_wf",
            "--workflow-id",
            "wf-123",
            "--reuse-policy",
            "terminate_if_running",
            "--conflict-policy",
            "use_existing",
        ]);
        let body = req.body.as_ref().unwrap();
        assert_eq!(body["workflow_id"], "wf-123");
        assert_eq!(body["reuse_policy"], "terminate_if_running");
        assert_eq!(body["conflict_policy"], "use_existing");
    }
}

#[cfg(test)]
mod dlq_aggregate_cli_tests {
    use super::*;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("harvest").chain(args.iter().copied()))
            .expect("CLI should parse successfully")
    }

    fn request(args: &[&str]) -> ApiRequest {
        parse(args)
            .api_request()
            .expect("request mapping should succeed")
    }

    #[test]
    fn aggregate_maps_to_get_with_repeated_group_by() {
        let req = request(&[
            "dlq",
            "aggregate",
            "--group-by",
            "workflow_name,failure_signature",
            "--since",
            "24h",
            "--samples-per-group",
            "3",
        ]);
        assert_eq!(req.method, ApiMethod::Get);
        assert!(req.path.starts_with("/dead-letters/aggregate?"));
        assert!(
            req.path.contains("group_by=workflow_name"),
            "path: {}",
            req.path
        );
        assert!(
            req.path.contains("group_by=failure_signature"),
            "path: {}",
            req.path
        );
        assert!(req.path.contains("since=24h"), "path: {}", req.path);
        assert!(
            req.path.contains("samples_per_group=3"),
            "path: {}",
            req.path
        );
        assert!(req.body.is_none());
    }

    #[test]
    fn aggregate_passes_all_filters() {
        let req = request(&[
            "dlq",
            "aggregate",
            "--group-by",
            "queue_name",
            "--time-bucket",
            "day",
            "--workflow-name",
            "onboarding",
            "--activity-name",
            "charge_card",
            "--queue-name",
            "billing",
            "--until",
            "2026-05-18T04:00:00Z",
            "--min-attempts",
            "3",
            "--limit-groups",
            "100",
        ]);
        assert!(req.path.contains("time_bucket=day"));
        assert!(req.path.contains("workflow_name=onboarding"));
        assert!(req.path.contains("activity_name=charge_card"));
        assert!(req.path.contains("queue_name=billing"));
        assert!(req.path.contains("min_attempts=3"));
        assert!(req.path.contains("limit_groups=100"));
    }

    #[test]
    fn aggregate_requires_group_by() {
        let parsed = Cli::try_parse_from(["harvest", "dlq", "aggregate"]);
        assert!(parsed.is_err(), "--group-by is required");
    }

    #[test]
    fn aggregate_rejects_out_of_range_limit_groups() {
        let parsed = Cli::try_parse_from([
            "harvest",
            "dlq",
            "aggregate",
            "--group-by",
            "queue_name",
            "--limit-groups",
            "9999",
        ]);
        assert!(
            parsed.is_err(),
            "limit_groups > 500 must be rejected by clap"
        );
    }

    #[test]
    fn aggregate_table_renders_groups_and_other_rollup() {
        let cli = parse(&["dlq", "aggregate", "--group-by", "workflow_name"]);
        let payload = json!({
            "total": 100,
            "filtered_total": 100,
            "truncated": true,
            "groups": [
                {
                    "key": {"workflow_name": "onboarding"},
                    "count": 60,
                    "first_seen": "2026-05-18T03:00:00Z",
                    "last_seen": "2026-05-18T04:00:00Z",
                    "sample_dead_letter_ids": ["id-a", "id-b"]
                },
                {
                    "key": {"_other": true},
                    "count": 40,
                    "sample_dead_letter_ids": []
                }
            ]
        });

        let rendered = render_response(&cli, &payload).expect("table should render");
        assert!(rendered.contains("WORKFLOW_NAME"), "{rendered}");
        assert!(rendered.contains("COUNT"), "{rendered}");
        assert!(rendered.contains("onboarding"), "{rendered}");
        assert!(rendered.contains("(other)"), "{rendered}");
        assert!(rendered.contains("id-a,id-b"), "{rendered}");
        assert!(
            rendered.contains("long tail rolled into _other"),
            "{rendered}"
        );
    }

    #[test]
    fn aggregate_json_flag_emits_raw_payload() {
        let cli = parse(&["dlq", "aggregate", "--group-by", "workflow_name", "--json"]);
        let payload = json!({"total": 1, "filtered_total": 1, "truncated": false, "groups": []});
        let rendered = render_response(&cli, &payload).expect("json should render");
        // Compact JSON (no pretty indentation) for piping.
        assert!(rendered.starts_with('{'));
        assert!(rendered.contains("\"total\":1"));
    }
}

#[cfg(test)]
mod erase_payloads_cli_tests {
    use super::*;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("harvest").chain(args.iter().copied()))
            .expect("CLI should parse successfully")
    }

    fn request(args: &[&str]) -> ApiRequest {
        parse(args)
            .api_request()
            .expect("request mapping should succeed")
    }

    #[test]
    fn erase_payloads_builds_post_request() {
        let req = request(&["workflow", "erase-payloads", "abc-123"]);
        assert_eq!(req.method, ApiMethod::Post);
        assert_eq!(req.path, "/workflows/abc-123/erase-payloads");
    }

    #[test]
    fn erase_payloads_with_reason_includes_reason_in_body() {
        let req = request(&[
            "workflow",
            "erase-payloads",
            "abc-123",
            "--reason",
            "GDPR Art. 17 request DSR-99",
        ]);
        assert_eq!(req.method, ApiMethod::Post);
        assert_eq!(req.path, "/workflows/abc-123/erase-payloads");
        let body = req.body.as_ref().expect("should have a body");
        assert_eq!(body["reason"], "GDPR Art. 17 request DSR-99");
    }

    #[test]
    fn erase_payloads_without_reason_sends_no_reason_field() {
        let req = request(&["workflow", "erase-payloads", "abc-123"]);
        let body = req.body.as_ref().expect("should have a body");
        assert!(
            body.get("reason").is_none() || body["reason"].is_null(),
            "omitting --reason must not send the field"
        );
    }
}

#[cfg(test)]
mod legal_hold_cli_tests {
    use super::*;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("harvest").chain(args.iter().copied()))
            .expect("CLI should parse successfully")
    }

    fn request(args: &[&str]) -> ApiRequest {
        parse(args)
            .api_request()
            .expect("request mapping should succeed")
    }

    #[test]
    fn legal_hold_set_builds_post_request_with_reason() {
        let req = request(&["legal-hold", "set", "abc-123", "--reason", "case 42"]);
        assert_eq!(req.method, ApiMethod::Post);
        assert_eq!(req.path, "/workflows/abc-123/legal-hold");
        let body = req.body.as_ref().expect("should have a body");
        assert_eq!(body["reason"], "case 42");
        assert!(
            body.get("hold_until").is_none() || body["hold_until"].is_null(),
            "omitting --until must not send hold_until"
        );
    }

    #[test]
    fn legal_hold_set_includes_until_when_provided() {
        let req = request(&[
            "legal-hold",
            "set",
            "abc-123",
            "--reason",
            "case 42",
            "--until",
            "2027-01-01T00:00:00Z",
        ]);
        let body = req.body.as_ref().expect("should have a body");
        assert_eq!(body["hold_until"], "2027-01-01T00:00:00Z");
    }

    #[test]
    fn legal_hold_release_builds_post_request() {
        let req = request(&["legal-hold", "release", "abc-123"]);
        assert_eq!(req.method, ApiMethod::Post);
        assert_eq!(req.path, "/workflows/abc-123/legal-hold/release");
    }

    #[test]
    fn legal_hold_set_requires_reason() {
        // clap must reject `set` without --reason.
        let res = Cli::try_parse_from(["harvest", "legal-hold", "set", "abc-123"]);
        assert!(res.is_err(), "--reason is required for legal-hold set");
    }
}

#[cfg(test)]
mod pause_resume_cli_tests {
    use super::*;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("harvest").chain(args.iter().copied()))
            .expect("CLI should parse successfully")
    }

    fn request(args: &[&str]) -> ApiRequest {
        parse(args)
            .api_request()
            .expect("request mapping should succeed")
    }

    #[test]
    fn pause_builds_post_request() {
        let req = request(&["workflow", "pause", "abc-123"]);
        assert_eq!(req.method, ApiMethod::Post);
        assert_eq!(req.path, "/workflows/abc-123/pause");
    }

    #[test]
    fn pause_with_reason_includes_reason_in_body() {
        let req = request(&[
            "workflow",
            "pause",
            "abc-123",
            "--reason",
            "investigating incident INC-42",
        ]);
        assert_eq!(req.method, ApiMethod::Post);
        assert_eq!(req.path, "/workflows/abc-123/pause");
        let body = req.body.as_ref().expect("should have a body");
        assert_eq!(body["reason"], "investigating incident INC-42");
    }

    #[test]
    fn pause_without_reason_sends_no_reason_field() {
        let req = request(&["workflow", "pause", "abc-123"]);
        let body = req.body.as_ref().expect("should have a body");
        assert!(
            body.get("reason").is_none() || body["reason"].is_null(),
            "omitting --reason must not send the field"
        );
    }

    #[test]
    fn resume_builds_post_request_with_no_body() {
        let req = request(&["workflow", "resume", "abc-123"]);
        assert_eq!(req.method, ApiMethod::Post);
        assert_eq!(req.path, "/workflows/abc-123/resume");
        assert!(req.body.is_none(), "resume must send no request body");
    }
}

#[cfg(test)]
mod retry_activity_cli_tests {
    use super::*;

    fn request(args: &[&str]) -> ApiRequest {
        Cli::try_parse_from(std::iter::once("harvest").chain(args.iter().copied()))
            .expect("CLI should parse successfully")
            .api_request()
            .expect("request mapping should succeed")
    }

    #[test]
    fn retry_activity_builds_post_request_with_correct_path() {
        let req = request(&["workflow", "retry-activity", "exec-123", "act-456"]);
        assert_eq!(req.method, ApiMethod::Post);
        assert_eq!(req.path, "/workflows/exec-123/activities/act-456/retry-now");
    }

    #[test]
    fn retry_activity_sends_no_body() {
        let req = request(&["workflow", "retry-activity", "exec-123", "act-456"]);
        assert!(
            req.body.is_none(),
            "retry-activity must send no request body"
        );
    }
}

#[cfg(test)]
mod fail_activity_cli_tests {
    use super::*;

    fn request(args: &[&str]) -> ApiRequest {
        Cli::try_parse_from(std::iter::once("harvest").chain(args.iter().copied()))
            .expect("CLI should parse successfully")
            .api_request()
            .expect("request mapping should succeed")
    }

    #[test]
    fn fail_activity_builds_post_request_with_correct_path() {
        let req = request(&["workflow", "fail-activity", "exec-123", "act-456"]);
        assert_eq!(req.method, ApiMethod::Post);
        assert_eq!(req.path, "/workflows/exec-123/activities/act-456/fail-now");
    }

    #[test]
    fn fail_activity_with_reason_sends_reason_body() {
        let req = request(&[
            "workflow",
            "fail-activity",
            "exec-123",
            "act-456",
            "--reason",
            "hung on dead downstream, INC-42",
        ]);
        let body = req.body.as_ref().expect("should have a body");
        assert_eq!(body["reason"], "hung on dead downstream, INC-42");
    }

    #[test]
    fn fail_activity_without_reason_sends_no_reason_field() {
        let req = request(&["workflow", "fail-activity", "exec-123", "act-456"]);
        let body = req.body.as_ref().expect("should have a body");
        assert!(
            body.get("reason").is_none() || body["reason"].is_null(),
            "omitting --reason must not send the field"
        );
    }
}

#[cfg(test)]
mod rerun_cli_tests {
    use super::*;

    fn request(args: &[&str]) -> ApiRequest {
        Cli::try_parse_from(std::iter::once("harvest").chain(args.iter().copied()))
            .expect("CLI should parse successfully")
            .api_request()
            .expect("request mapping should succeed")
    }

    #[test]
    fn rerun_builds_post_request_with_correct_path() {
        let req = request(&["workflow", "rerun", "exec-123"]);
        assert_eq!(req.method, ApiMethod::Post);
        assert_eq!(req.path, "/workflows/exec-123/rerun");
    }

    #[test]
    fn rerun_bare_sends_empty_object_body() {
        let req = request(&["workflow", "rerun", "exec-123"]);
        assert_eq!(
            req.body,
            Some(Value::Object(Map::new())),
            "the body is optional server-side but the CLI sends an empty object"
        );
    }

    #[test]
    fn rerun_without_input_flags_sends_no_input_key() {
        let req = request(&["workflow", "rerun", "exec-123"]);
        let body = req.body.as_ref().expect("should have a body");
        // The server's `input` field is TRI-STATE: absent = clone the source's
        // stored input verbatim, explicit null = override with null.  Injecting
        // a null for "the operator did not pass --input-*" would silently wipe
        // the cloned input, so the key must be ABSENT (not present-and-null).
        assert!(
            body.get("input").is_none(),
            "omitting --input-json/--input-file must send no `input` key at all"
        );
    }

    #[test]
    fn rerun_with_workflow_id_sends_workflow_id_body() {
        let req = request(&["workflow", "rerun", "exec-123", "--workflow-id", "order-9"]);
        let body = req.body.as_ref().expect("should have a body");
        assert_eq!(body["workflow_id"], "order-9");
        assert!(
            body.get("input").is_none(),
            "--workflow-id alone must not add an `input` key"
        );
    }

    #[test]
    fn rerun_with_inline_input_json_sends_input_body() {
        let req = request(&[
            "workflow",
            "rerun",
            "exec-123",
            "--input-json",
            r#"{"k":1}"#,
        ]);
        let body = req.body.as_ref().expect("should have a body");
        assert_eq!(body["input"], serde_json::json!({"k": 1}));
    }

    #[test]
    fn rerun_with_explicit_null_input_sends_null_input() {
        let req = request(&["workflow", "rerun", "exec-123", "--input-json", "null"]);
        let body = req.body.as_ref().expect("should have a body");
        assert!(
            body.get("input").is_some(),
            "an operator-supplied JSON null IS a legitimate override and must be sent"
        );
        assert_eq!(body["input"], Value::Null);
    }

    #[test]
    fn rerun_path_encodes_the_execution_id() {
        let req = request(&["workflow", "rerun", "exec 123/../x"]);
        assert!(
            !req.path.contains("../"),
            "execution id must be path-encoded, got {}",
            req.path
        );
        assert_eq!(
            req.path,
            format!("/workflows/{}/rerun", path_segment("exec 123/../x"))
        );
    }
}

#[cfg(test)]
mod completion_delivery_cli_tests {
    use super::*;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("harvest").chain(args.iter().copied()))
            .expect("CLI should parse successfully")
    }

    fn request(args: &[&str]) -> ApiRequest {
        parse(args)
            .api_request()
            .expect("request mapping should succeed")
    }

    #[test]
    fn list_builds_get_request_with_correct_path() {
        let req = request(&["completion-delivery", "list", "exec-123"]);
        assert_eq!(req.method, ApiMethod::Get);
        assert_eq!(req.path, "/workflows/exec-123/completion-deliveries");
        assert!(req.body.is_none());
    }

    #[test]
    fn list_alias_completion_deliveries_parses() {
        let req = request(&["completion-deliveries", "list", "exec-123"]);
        assert_eq!(req.path, "/workflows/exec-123/completion-deliveries");
    }

    #[test]
    fn list_alias_callbacks_parses() {
        let req = request(&["callbacks", "list", "exec-123"]);
        assert_eq!(req.path, "/workflows/exec-123/completion-deliveries");
    }

    #[test]
    fn list_with_state_does_not_send_state_as_a_query_param() {
        // --state is applied client-side in render_response, not sent to the
        // server (the endpoint has no query-param filter).
        let req = request(&[
            "completion-delivery",
            "list",
            "exec-123",
            "--state",
            "failed",
        ]);
        assert_eq!(req.path, "/workflows/exec-123/completion-deliveries");
    }

    #[test]
    fn redrive_builds_post_request_with_correct_path_and_no_body() {
        let req = request(&["completion-delivery", "redrive", "exec-123", "delivery-456"]);
        assert_eq!(req.method, ApiMethod::Post);
        assert_eq!(
            req.path,
            "/workflows/exec-123/completion-deliveries/delivery-456/redrive"
        );
        assert!(req.body.is_none());
    }

    #[test]
    fn execution_id_and_delivery_id_are_path_segment_encoded() {
        let req = request(&["completion-delivery", "redrive", "exec/123", "del/456"]);
        assert!(!req.path.contains("exec/123"));
        assert!(!req.path.contains("del/456"));
        assert!(req.path.contains("exec%2F123"));
        assert!(req.path.contains("del%2F456"));
    }

    #[test]
    fn filter_completion_deliveries_by_state_is_case_insensitive_and_keeps_matches() {
        let value = json!([
            { "delivery_id": "a", "state": "PENDING" },
            { "delivery_id": "b", "state": "FAILED" },
            { "delivery_id": "c", "state": "DELIVERED" },
        ]);
        let filtered = filter_completion_deliveries_by_state(&value, "failed");
        assert_eq!(filtered, json!([{ "delivery_id": "b", "state": "FAILED" }]));
    }

    #[test]
    fn filter_completion_deliveries_by_state_passes_non_array_through_unchanged() {
        let value = json!({ "ok": true });
        let filtered = filter_completion_deliveries_by_state(&value, "failed");
        assert_eq!(filtered, value);
    }

    // ── Lineage tree rendering (issue #621) ──────────────────────────────

    fn lineage_tree_fixture() -> Value {
        serde_json::json!({
            "root": {
                "execution_id": "11111111-1111-1111-1111-111111111111",
                "workflow_name": "checkout_saga",
                "workflow_id": "order-42",
                "state": "RUNNING",
                "depth": 0,
                "await_mode": "awaited",
                "parent_close_policy": null,
                "children": [{
                    "execution_id": "22222222-2222-2222-2222-222222222222",
                    "workflow_name": "charge_card",
                    "workflow_id": "charge-42",
                    "state": "FAILED",
                    "depth": 1,
                    "await_mode": "detached",
                    "parent_close_policy": "request_cancel",
                    "children": [{
                        "execution_id": "33333333-3333-3333-3333-333333333333",
                        "workflow_name": "refund_card",
                        "workflow_id": "refund-42",
                        "state": "COMPLETED",
                        "depth": 2,
                        "await_mode": "awaited",
                        "parent_close_policy": null,
                        "children": []
                    }]
                }]
            },
            "node_count": 3,
            "max_depth_reached": 2,
            "truncated": false,
            "truncated_parent_ids": [],
            "truncated_parents_capped": false,
            "status": "complete",
            "unavailable_shards": []
        })
    }

    #[test]
    fn lineage_tree_renders_children_indented_below_their_parent() {
        // Indentation IS the topology for this endpoint — a child must not
        // render at the same level as its parent.
        let rendered = format_lineage_tree(&lineage_tree_fixture());
        let lines: Vec<&str> = rendered.lines().collect();
        assert!(lines[0].starts_with("RUNNING"), "root is not indented");
        assert!(
            lines[1].starts_with("  "),
            "the child must be indented below the root, got {:?}",
            lines[1]
        );
        assert!(lines[1].contains("FAILED"));
        assert!(lines[1].contains("22222222-2222-2222-2222-222222222222"));
        assert!(
            lines[1].contains("[detached:request_cancel]"),
            "a detached child must surface its parent-close policy"
        );
        // Depth 2 is what proves the indent GROWS per level rather than being
        // a constant applied to every non-root node.
        assert!(
            lines[2].starts_with("    "),
            "the grandchild must be indented one level deeper again, got {:?}",
            lines[2]
        );
        assert!(lines[2].contains("33333333-3333-3333-3333-333333333333"));
        assert!(rendered.contains("3 node(s), max depth 2"));
    }

    #[test]
    fn lineage_tree_bounds_are_rejected_locally_when_out_of_range() {
        // The server rejects these too, but a local rejection saves a
        // pointless round-trip — matching the adjacent `workflow children`.
        for args in [
            vec!["harvest", "workflow", "tree", "abc", "--max-depth", "0"],
            vec!["harvest", "workflow", "tree", "abc", "--max-depth", "51"],
            vec!["harvest", "workflow", "tree", "abc", "--max-nodes", "0"],
            vec!["harvest", "workflow", "tree", "abc", "--max-nodes", "10001"],
        ] {
            assert!(
                Cli::try_parse_from(&args).is_err(),
                "{args:?} should be rejected at parse time"
            );
        }
        assert!(
            Cli::try_parse_from([
                "harvest",
                "workflow",
                "tree",
                "abc",
                "--max-depth",
                "50",
                "--max-nodes",
                "10000",
            ])
            .is_ok(),
            "the ceilings themselves are inclusive"
        );
    }

    #[test]
    fn lineage_tree_render_calls_out_truncation_and_names_dropped_parents() {
        // A truncated tree must never look complete on a terminal.
        let mut value = lineage_tree_fixture();
        value["truncated"] = Value::Bool(true);
        value["truncation_reason"] = Value::String("max_nodes".to_string());
        value["truncated_parent_ids"] = serde_json::json!(["22222222-2222-2222-2222-222222222222"]);
        let rendered = format_lineage_tree(&value);
        assert!(rendered.contains("TRUNCATED (max_nodes)"));
        assert!(rendered.contains("22222222-2222-2222-2222-222222222222"));
        assert!(rendered.contains("re-root the call here"));
    }

    #[test]
    fn lineage_tree_render_calls_out_an_unavailable_shard() {
        let mut value = lineage_tree_fixture();
        value["status"] = Value::String("partial".to_string());
        value["unavailable_shards"] = serde_json::json!([
            { "shard_id": 1, "reason": "connection refused" }
        ]);
        let rendered = format_lineage_tree(&value);
        assert!(rendered.contains("PARTIAL (partial)"));
        assert!(rendered.contains("shard 1"));
        assert!(rendered.contains("connection refused"));
    }

    #[test]
    fn lineage_tree_render_calls_out_retention_collected_descendants() {
        // The third incompleteness signal. Unlike the other two it can fire on
        // a tree that is otherwise clean — `truncated: false`, `status:
        // complete` — so if the terminal does not print it, the documented
        // triage flow shows an apparently-healthy family that is missing a
        // descendant.
        let mut value = lineage_tree_fixture();
        value["retained_summary_parent_ids"] =
            serde_json::json!(["22222222-2222-2222-2222-222222222222"]);
        let rendered = format_lineage_tree(&value);
        assert!(
            rendered.contains("RETENTION"),
            "an otherwise-clean tree must still be called out as incomplete:\n{rendered}"
        );
        assert!(rendered.contains("22222222-2222-2222-2222-222222222222"));
        assert!(
            rendered.contains("workflow summaries"),
            "name the command that reads the omitted rows"
        );
    }

    #[test]
    fn lineage_render_stays_quiet_when_nothing_was_retention_collected() {
        // The complement: the signal must not cry wolf on a genuinely
        // complete tree, or operators will learn to ignore it.
        let value = lineage_tree_fixture();
        let rendered = format_lineage_tree(&value);
        assert!(!rendered.contains("RETENTION"), "{rendered}");
    }

    #[test]
    fn lineage_summary_render_calls_out_retention_collected_descendants() {
        // `--summary` is the *first* command in the runbook's triage flow, and
        // the one that reports `failed: 0`. It shares the footer, so this pins
        // that the sharing is real.
        let value = serde_json::json!({
            "root": {
                "execution_id": "11111111-1111-1111-1111-111111111111",
                "workflow_name": "checkout_saga",
                "workflow_id": "order-42",
                "state": "RUNNING"
            },
            // The trap this guards: a clean-looking roll-up.
            "counts": { "running": 1, "failed": 0, "completed": 2 },
            "total_descendants": 3,
            "max_depth_reached": 1,
            "truncated": false,
            "truncated_parent_ids": [],
            "truncated_parents_capped": false,
            "status": "complete",
            "unavailable_shards": [],
            "retained_summary_parent_ids": ["33333333-3333-3333-3333-333333333333"]
        });
        let rendered = format_lineage_summary(&value);
        assert!(rendered.contains("RETENTION"), "{rendered}");
        assert!(rendered.contains("33333333-3333-3333-3333-333333333333"));
    }

    #[test]
    fn lineage_summary_renders_per_state_counts() {
        let value = serde_json::json!({
            "root": {
                "execution_id": "11111111-1111-1111-1111-111111111111",
                "workflow_name": "checkout_saga",
                "workflow_id": "order-42",
                "state": "RUNNING"
            },
            "counts": { "running": 3, "failed": 1, "completed": 12 },
            "total_descendants": 16,
            "max_depth_reached": 3,
            "truncated": false,
            "truncated_parent_ids": [],
            "truncated_parents_capped": false,
            "status": "complete",
            "unavailable_shards": []
        });
        let rendered = format_lineage_summary(&value);
        assert!(rendered.contains("failed"));
        assert!(rendered.contains("16 descendant(s)"));
        assert!(rendered.contains("order-42"));
    }

    #[test]
    fn lineage_tree_flag_dispatch_picks_the_right_renderer() {
        let tree = Cli::try_parse_from([
            "harvest",
            "workflow",
            "tree",
            "00000000-0000-0000-0000-000000000001",
        ])
        .expect("parse");
        assert!(lineage_tree_wants_render(&tree));
        assert!(!lineage_tree_wants_summary(&tree));
        assert!(!lineage_tree_wants_raw_json(&tree));

        let summary = Cli::try_parse_from([
            "harvest",
            "workflow",
            "tree",
            "00000000-0000-0000-0000-000000000001",
            "--summary",
        ])
        .expect("parse");
        assert!(lineage_tree_wants_summary(&summary));

        let raw = Cli::try_parse_from([
            "harvest",
            "workflow",
            "tree",
            "00000000-0000-0000-0000-000000000001",
            "--json",
        ])
        .expect("parse");
        assert!(lineage_tree_wants_raw_json(&raw));
        assert!(
            !lineage_tree_wants_render(&raw),
            "--json must bypass the outline renderer"
        );
    }

    #[test]
    fn render_response_renders_the_diagnose_verdict_as_a_table() {
        // Pins the WIRING, not just the formatter: deleting the dispatch arm in
        // `render_response` would leave the formatter and predicate tests green
        // while `harvest workflow diagnose` silently reverted to raw JSON.
        let cli = parse(&["workflow", "diagnose", "exec-1"]);
        let value = json!({
            "execution_id": "exec-1",
            "health": "stalled",
            "summary": "no live worker is polling queue 'email'",
            "blocked_on": { "type": "activity_no_worker", "queue": "email" },
            "contributing_reason_codes": ["no_live_worker"],
        });
        let rendered = render_response(&cli, &value).expect("should render");
        assert!(
            rendered.contains("stalled"),
            "expected the rendered verdict, got: {rendered}"
        );
        assert!(
            serde_json::from_str::<Value>(&rendered).is_err(),
            "the table renderer must not emit raw JSON, got: {rendered}"
        );
    }

    #[test]
    fn render_response_diagnose_json_flag_emits_the_raw_body() {
        let cli = parse(&["workflow", "diagnose", "exec-1", "--json"]);
        let value = json!({ "execution_id": "exec-1", "health": "stalled" });
        let rendered = render_response(&cli, &value).expect("should render");
        let parsed: Value = serde_json::from_str(&rendered).expect("--json must emit valid JSON");
        assert_eq!(parsed["health"], "stalled");
    }

    #[test]
    fn render_response_applies_state_filter_only_for_completion_delivery_list() {
        let cli = parse(&[
            "completion-delivery",
            "list",
            "exec-123",
            "--state",
            "delivered",
        ]);
        let value = json!([
            { "delivery_id": "a", "state": "PENDING" },
            { "delivery_id": "b", "state": "DELIVERED" },
        ]);
        let rendered = render_response(&cli, &value).expect("should render");
        let parsed: Value = serde_json::from_str(&rendered).expect("valid json");
        assert_eq!(
            parsed,
            json!([{ "delivery_id": "b", "state": "DELIVERED" }])
        );
    }

    #[test]
    fn render_response_without_state_flag_returns_full_list() {
        let cli = parse(&["completion-delivery", "list", "exec-123"]);
        let value = json!([
            { "delivery_id": "a", "state": "PENDING" },
            { "delivery_id": "b", "state": "DELIVERED" },
        ]);
        let rendered = render_response(&cli, &value).expect("should render");
        let parsed: Value = serde_json::from_str(&rendered).expect("valid json");
        assert_eq!(parsed, value);
    }

    #[test]
    fn redrive_command_never_applies_the_list_state_filter() {
        // Sanity check that completion_delivery_list_state_filter is scoped
        // to List and does not accidentally intercept Redrive's response.
        let cli = parse(&["completion-delivery", "redrive", "exec-123", "delivery-456"]);
        assert!(completion_delivery_list_state_filter(&cli).is_none());
    }
}

#[cfg(test)]
mod usage_cli_tests {
    use super::*;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("harvest").chain(args.iter().copied()))
            .expect("CLI should parse successfully")
    }

    fn request(args: &[&str]) -> ApiRequest {
        parse(args)
            .api_request()
            .expect("request mapping should succeed")
    }

    #[test]
    fn usage_maps_to_get_admin_usage_with_from_and_to() {
        let req = request(&[
            "usage",
            "--from",
            "2026-01-01T00:00:00Z",
            "--to",
            "2026-02-01T00:00:00Z",
        ]);
        assert_eq!(req.method, ApiMethod::Get);
        assert!(req.path.starts_with("/admin/usage?"), "path: {}", req.path);
        assert!(
            req.path.contains("from=2026-01-01T00%3A00%3A00Z")
                || req.path.contains("from=2026-01-01T00:00:00Z"),
            "path: {}",
            req.path
        );
        assert!(req.body.is_none());
    }

    #[test]
    fn usage_threads_group_by_into_query() {
        let req = request(&[
            "usage",
            "--from",
            "24h",
            "--to",
            "1h",
            "--group-by",
            "search_attr:tenant_id",
        ]);
        assert!(
            req.path.contains("group_by=search_attr%3Atenant_id")
                || req.path.contains("group_by=search_attr:tenant_id"),
            "path: {}",
            req.path
        );
        assert!(req.path.contains("from=24h"), "path: {}", req.path);
        assert!(req.path.contains("to=1h"), "path: {}", req.path);
    }

    #[test]
    fn usage_omits_group_by_when_not_supplied() {
        let req = request(&["usage", "--from", "24h", "--to", "1h"]);
        assert!(!req.path.contains("group_by"), "path: {}", req.path);
    }

    #[test]
    fn usage_requires_from_and_to() {
        assert!(Cli::try_parse_from(["harvest", "usage"]).is_err());
        assert!(Cli::try_parse_from(["harvest", "usage", "--from", "24h"]).is_err());
        assert!(Cli::try_parse_from(["harvest", "usage", "--to", "1h"]).is_err());
    }

    #[test]
    fn usage_wants_table_is_default_and_json_flag_switches_to_raw_json() {
        let table_cli = parse(&["usage", "--from", "24h", "--to", "1h"]);
        assert!(usage_wants_table(&table_cli));
        assert!(!usage_wants_raw_json(&table_cli));

        let json_cli = parse(&["usage", "--from", "24h", "--to", "1h", "--json"]);
        assert!(!usage_wants_table(&json_cli));
        assert!(usage_wants_raw_json(&json_cli));
    }

    #[test]
    fn diagnose_flag_selects_table_vs_raw_json() {
        let table_cli = parse(&["workflow", "diagnose", "exec-1"]);
        assert!(diagnose_wants_table(&table_cli));
        assert!(!diagnose_wants_raw_json(&table_cli));

        let json_cli = parse(&["workflow", "diagnose", "exec-1", "--json"]);
        assert!(!diagnose_wants_table(&json_cli));
        assert!(diagnose_wants_raw_json(&json_cli));
    }

    #[test]
    fn format_diagnose_verdict_renders_the_replay_derived_wait_kinds() {
        // The renderer walks `blocked_on`'s members generically, so a newly
        // added variant must surface with no CLI change. This pins that: a
        // mutex park has to name the contended key an operator needs to find
        // the holder.
        let value = serde_json::json!({
            "execution_id": "00000000-0000-0000-0000-000000000001",
            "health": "blocked_external",
            "summary": "parked on a durable mutex wait (ledger:42)",
            "blocked_on": {
                "type": "awaiting_replay_wait",
                "wait_kind": "mutex",
                "name": "ledger:42"
            },
            "wait_set": "replayed",
        });
        let rendered = format_diagnose_verdict(&value);
        assert!(
            rendered.contains("awaiting_replay_wait"),
            "expected the verdict type, got: {rendered}"
        );
        assert!(
            rendered.contains("mutex") && rendered.contains("ledger:42"),
            "expected the wait kind and contended key, got: {rendered}"
        );
    }

    #[test]
    fn format_diagnose_verdict_renders_the_workflow_task_verdicts() {
        let value = serde_json::json!({
            "execution_id": "00000000-0000-0000-0000-000000000001",
            "health": "stalled",
            "summary": "no live worker is polling workflow task queue 'orphan'",
            "blocked_on": { "type": "workflow_no_worker", "queue": "orphan" },
        });
        let rendered = format_diagnose_verdict(&value);
        assert!(
            rendered.contains("workflow_no_worker") && rendered.contains("orphan"),
            "expected the uncovered workflow queue, got: {rendered}"
        );
    }

    #[test]
    fn format_diagnose_verdict_surfaces_the_actionable_root_cause() {
        // The headline case (#809 AC3): the operator must read the uncovered
        // queue name off the rendered verdict without a `jq` pipeline.
        let value = serde_json::json!({
            "execution_id": "00000000-0000-0000-0000-000000000001",
            "workflow_id": "order-42",
            "workflow_name": "order_flow",
            "state": "RUNNING",
            "health": "stalled",
            "blocked_on": {
                "type": "activity_no_worker",
                "queue": "typo-queue",
                "activity_name": "charge_card"
            },
            "summary": "no live worker is polling queue 'typo-queue', so activity 'charge_card' will never be claimed",
            "last_event_age_seconds": 903.5,
            "last_event_at": "2026-01-01T00:00:00Z",
            "contributing_reason_codes": ["no_live_worker"],
            "wait_set": "not_consulted"
        });
        let rendered = format_diagnose_verdict(&value);
        assert!(rendered.contains("health:    stalled"), "{rendered}");
        assert!(
            rendered.contains("blocked on: activity_no_worker"),
            "{rendered}"
        );
        assert!(rendered.contains("queue: typo-queue"), "{rendered}");
        assert!(
            rendered.contains("activity_name: charge_card"),
            "{rendered}"
        );
        assert!(rendered.contains("no_live_worker"), "{rendered}");
        assert!(rendered.contains("order_flow"), "{rendered}");
    }

    #[test]
    fn format_diagnose_verdict_renders_a_terminal_outcome_without_blocked_on() {
        let value = serde_json::json!({
            "execution_id": "00000000-0000-0000-0000-000000000002",
            "workflow_id": "order-43",
            "workflow_name": "order_flow",
            "state": "FAILED",
            "health": "terminal",
            "summary": "execution reached terminal state FAILED — nothing to diagnose",
            "last_event_age_seconds": 12.0,
            "terminal_outcome": {"state": "FAILED", "error": "boom"},
            "contributing_reason_codes": [],
            "wait_set": "not_consulted"
        });
        let rendered = format_diagnose_verdict(&value);
        assert!(rendered.contains("health:    terminal"), "{rendered}");
        assert!(rendered.contains("terminal outcome:"), "{rendered}");
        assert!(rendered.contains("error: boom"), "{rendered}");
        assert!(
            !rendered.contains("blocked on:"),
            "a terminal verdict has no blocked_on to render: {rendered}"
        );
    }

    #[test]
    fn format_usage_table_renders_header_and_rows() {
        let value = serde_json::json!({
            "status": "complete",
            "from": "2026-01-01T00:00:00Z",
            "to": "2026-02-01T00:00:00Z",
            "group_by": "workflow_name",
            "groups": [
                {
                    "group": "onboarding",
                    "workflow_starts": 10,
                    "completed": 8,
                    "failed": 1,
                    "cancelled": 0,
                    "timed_out": 1,
                    "activity_executions": 25,
                    "activity_executions_failed": 2,
                    "activity_compute_seconds": 123.456
                }
            ],
            "unavailable_shards": []
        });
        let rendered = format_usage_table(&value);
        assert!(rendered.contains("status: complete"));
        assert!(rendered.contains("2026-01-01T00:00:00Z"));
        assert!(rendered.contains("group_by: workflow_name"));
        assert!(rendered.contains("onboarding"));
        assert!(rendered.contains("123.46"));
    }

    #[test]
    fn format_usage_table_notes_unavailable_shards() {
        let value = serde_json::json!({
            "status": "partial",
            "from": "2026-01-01T00:00:00Z",
            "to": "2026-02-01T00:00:00Z",
            "group_by": "workflow_name",
            "groups": [],
            "unavailable_shards": [{"shard_id": 1, "reason": "connection refused"}]
        });
        let rendered = format_usage_table(&value);
        assert!(rendered.contains("unavailable shards: 1"), "{rendered}");
    }

    #[test]
    fn format_usage_table_handles_empty_groups() {
        let value = serde_json::json!({
            "status": "complete",
            "from": "2026-01-01T00:00:00Z",
            "to": "2026-02-01T00:00:00Z",
            "group_by": "workflow_name",
            "groups": [],
            "unavailable_shards": []
        });
        let rendered = format_usage_table(&value);
        assert!(rendered.contains("No usage groups found."));
    }

    #[test]
    fn render_table_pads_columns_to_their_widest_cell() {
        let rows = vec![
            vec!["A".to_string(), "BB".to_string()],
            vec!["CCC".to_string(), "D".to_string()],
        ];
        assert_eq!(render_table(&rows), "A    BB\nCCC  D");
    }

    #[test]
    fn render_table_trims_trailing_padding_on_each_line() {
        let rows = vec![
            vec!["A".to_string(), "B".to_string(), "C".to_string()],
            vec![String::new(), String::new(), String::new()],
        ];
        assert_eq!(render_table(&rows), "A  B  C\n");
    }

    #[test]
    fn render_table_handles_a_single_row() {
        let rows = vec![vec!["HEADER".to_string()]];
        assert_eq!(render_table(&rows), "HEADER");
    }

    #[test]
    fn format_workflow_summaries_table_renders_rows_and_next_cursor() {
        let value = serde_json::json!({
            "summaries": [
                {
                    "execution_id": "exec-1",
                    "workflow_name": "onboarding",
                    "workflow_id": "wf-1",
                    "state": "completed",
                    "completed_at": "2026-05-18T00:00:00Z",
                    "duration_ms": 4200,
                    "shard_id": 3
                }
            ],
            "next_cursor": "abc123"
        });
        let rendered = format_workflow_summaries_table(&value);
        assert!(rendered.contains("EXEC ID"), "{rendered}");
        assert!(rendered.contains("exec-1"), "{rendered}");
        assert!(rendered.contains("onboarding"), "{rendered}");
        assert!(rendered.ends_with("\nnext_cursor: abc123"), "{rendered}");
    }

    #[test]
    fn format_workflow_summaries_table_reports_no_summaries() {
        let value = serde_json::json!({ "summaries": [] });
        assert_eq!(
            format_workflow_summaries_table(&value),
            "No execution summaries found."
        );
    }

    #[test]
    fn format_run_chain_table_renders_rows_workflow_id_and_head_unknown_note() {
        let value = serde_json::json!({
            "workflow_id": "wf-9",
            "head_unknown": true,
            "runs": [
                {
                    "sequence": 1,
                    "exec_id": "exec-1",
                    "run_id": "run-1",
                    "state": "completed",
                    "outcome": "success",
                    "started_at": "2026-05-18T00:00:00Z",
                    "completed_at": "2026-05-18T00:05:00Z",
                    "continued_to_exec_id": "exec-2"
                }
            ]
        });
        let rendered = format_run_chain_table(&value);
        assert!(rendered.starts_with("workflow_id: wf-9\n"), "{rendered}");
        assert!(rendered.contains("exec-1"), "{rendered}");
        assert!(rendered.contains("note: head_unknown"), "{rendered}");
    }

    #[test]
    fn format_run_chain_table_reports_no_runs() {
        let value = serde_json::json!({ "runs": [] });
        assert_eq!(format_run_chain_table(&value), "No run chain found.");
    }

    #[test]
    fn format_audit_table_renders_target_type_and_id_joined() {
        let value = serde_json::json!([
            {
                "occurred_at": "2026-05-18T00:00:00Z",
                "actor": "operator@example.com",
                "operation": "pause",
                "target_type": "workflow",
                "target_id": "wf-1",
                "status": "ok",
                "source": "cli",
                "error_summary": null
            }
        ]);
        let rendered = format_audit_table(&value);
        assert!(rendered.contains("workflow:wf-1"), "{rendered}");
        assert!(rendered.contains("operator@example.com"), "{rendered}");
    }

    #[test]
    fn format_audit_table_reports_no_records() {
        let value = serde_json::json!([]);
        assert_eq!(format_audit_table(&value), "No audit records found.");
    }
}

#[cfg(test)]
mod det_check_cli_tests {
    use super::*;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("harvest").chain(args.iter().copied()))
            .expect("CLI should parse successfully")
    }

    fn report(src: &str) -> DetCheckReport {
        autumn_harvest::check_source(src, "test.rs")
    }

    // NOTE: these fixtures embed `#[workflow]` source and MUST be single-line
    // string literals (with `\n` escapes), not multi-line `"\`-continuation
    // strings — a multi-line literal containing `#[workflow]` at a line start is
    // misread as a real workflow by the line-based det_check scanner (the
    // documented multi-line-string lexer caveat), producing a self-scan false
    // positive on this very file. Single-line literals are stripped correctly.
    const WF_TIME: &str = "#[workflow]\nasync fn wf(ctx: &WorkflowContext) -> Result<(), String> {\n    let _ = std::time::SystemTime::now();\n    Ok(())\n}\n";

    const WF_WARN: &str = "#[workflow]\nasync fn wf(ctx: &WorkflowContext) -> Result<(), String> {\n    let _ = std::process::id();\n    Ok(())\n}\n";

    const WF_SUPPRESSED: &str = "#[workflow]\nasync fn wf(ctx: &WorkflowContext) -> Result<(), String> {\n    // harvest-suppress: DET001 \"recorded in signal payload\"\n    let _ = std::time::SystemTime::now();\n    Ok(())\n}\n";

    // ── backup verify (issue #943) ─────────────────────────────────────────

    #[test]
    fn backup_verify_parses_repeated_shards_and_flags() {
        let cli = parse(&[
            "backup",
            "verify",
            "--shard",
            "0=postgres://scratch/a",
            "--shard",
            "1=postgres://scratch/b",
            "--format",
            "json",
            "--replay-sample",
            "7",
            "--worker-stale-secs",
            "120",
            "--probe-limit",
            "250",
            "--i-know-this-is-scratch",
        ]);
        match cli.command {
            Commands::Backup {
                command:
                    BackupCommand::Verify {
                        shards,
                        i_know_this_is_scratch,
                        format,
                        replay_sample,
                        worker_stale_secs,
                        probe_limit,
                        ..
                    },
            } => {
                assert_eq!(shards.len(), 2);
                assert_eq!(shards[1], "1=postgres://scratch/b");
                assert!(i_know_this_is_scratch);
                assert_eq!(format, BackupVerifyFormat::Json);
                assert_eq!(replay_sample, 7);
                assert_eq!(worker_stale_secs, 120);
                assert_eq!(probe_limit, 250);
            }
            other => panic!("expected Backup::Verify, got {other:?}"),
        }
    }

    #[test]
    fn backup_verify_defaults_are_text_and_guarded() {
        let cli = parse(&["backup", "verify", "--shard", "postgres://scratch/a"]);
        match cli.command {
            Commands::Backup {
                command:
                    BackupCommand::Verify {
                        i_know_this_is_scratch,
                        format,
                        replay_sample,
                        probe_limit,
                        ..
                    },
            } => {
                assert!(
                    !i_know_this_is_scratch,
                    "the scratch guard must be opt-OUT, never the default"
                );
                assert_eq!(format, BackupVerifyFormat::Text);
                assert_eq!(replay_sample, 50);
                assert_eq!(
                    probe_limit,
                    autumn_harvest::backup_verify::DEFAULT_PROBE_LIMIT,
                    "the CLI default must track the library default"
                );
            }
            other => panic!("expected Backup::Verify, got {other:?}"),
        }
    }

    #[test]
    fn backup_verify_default_shard_defaults_to_zero_and_is_overridable() {
        let cli = parse(&["backup", "verify", "--shard", "postgres://scratch/a"]);
        match cli.command {
            Commands::Backup {
                command: BackupCommand::Verify { default_shard, .. },
            } => assert_eq!(default_shard, 0, "0 is the overwhelmingly common default"),
            other => panic!("expected Backup::Verify, got {other:?}"),
        }

        let cli = parse(&[
            "backup",
            "verify",
            "--shard",
            "postgres://scratch/a",
            "--default-shard",
            "3",
        ]);
        match cli.command {
            Commands::Backup {
                command: BackupCommand::Verify { default_shard, .. },
            } => assert_eq!(default_shard, 3),
            other => panic!("expected Backup::Verify, got {other:?}"),
        }
    }

    #[test]
    fn backup_verify_requires_at_least_one_shard() {
        Cli::try_parse_from(["harvest", "backup", "verify"])
            .expect_err("--shard is required; a verify with no target is meaningless");
    }

    #[test]
    fn det_check_parses_paths_and_flags() {
        let cli = parse(&["det-check", "--format", "json", "some/path"]);
        match cli.command {
            Commands::DetCheck {
                paths,
                format,
                deny_warnings,
                list_suppressions,
            } => {
                assert_eq!(paths, vec![PathBuf::from("some/path")]);
                assert_eq!(format, DetCheckFormat::Json);
                assert!(!deny_warnings);
                assert!(!list_suppressions);
            }
            other => panic!("expected DetCheck, got {other:?}"),
        }
    }

    #[test]
    fn det_check_defaults_to_current_dir_and_text_format() {
        let cli = parse(&["det-check"]);
        match cli.command {
            Commands::DetCheck {
                paths,
                format,
                deny_warnings,
                list_suppressions,
            } => {
                assert_eq!(paths, vec![PathBuf::from(".")]);
                assert_eq!(format, DetCheckFormat::Text);
                assert!(!deny_warnings);
                assert!(!list_suppressions);
            }
            other => panic!("expected DetCheck, got {other:?}"),
        }
    }

    #[test]
    fn det_check_accepts_deny_warnings_and_list_suppressions() {
        let cli = parse(&[
            "det-check",
            "--deny-warnings",
            "--list-suppressions",
            "a",
            "b",
        ]);
        match cli.command {
            Commands::DetCheck {
                paths,
                deny_warnings,
                list_suppressions,
                ..
            } => {
                assert_eq!(paths, vec![PathBuf::from("a"), PathBuf::from("b")]);
                assert!(deny_warnings);
                assert!(list_suppressions);
            }
            other => panic!("expected DetCheck, got {other:?}"),
        }
    }

    #[test]
    fn det_check_format_default_is_text() {
        assert_eq!(DetCheckFormat::default(), DetCheckFormat::Text);
    }

    #[test]
    fn text_finding_line_has_location_rule_and_alternative() {
        let r = report(WF_TIME);
        let text = format_det_findings_text(&r);
        assert!(text.contains("DET001"), "{text}");
        assert!(text.contains("test.rs:"), "{text}");
        assert!(text.contains("safe alternative:"), "{text}");
        // A direct finding has no helper attribution.
        assert!(!text.contains("in helper"), "{text}");
    }

    #[test]
    fn transitive_text_line_names_helper_and_entry() {
        let src = "\
#[workflow]
async fn entry_wf(ctx: &WorkflowContext) -> Result<(), String> {
    let _ = bad_helper();
    Ok(())
}

fn bad_helper() -> i64 {
    autumn_harvest::chrono::Utc::now().timestamp()
}
";
        let text = format_det_findings_text(&report(src));
        assert!(
            text.contains("in helper `bad_helper` reached from workflow `entry_wf`"),
            "{text}"
        );
    }

    #[test]
    fn text_findings_empty_report_is_no_findings() {
        let empty = DetCheckReport::default();
        assert_eq!(format_det_findings_text(&empty), "det-check: no findings");
    }

    #[test]
    fn suppression_formatter_renders_reason_and_location() {
        let r = report(WF_SUPPRESSED);
        let footer = format_det_suppressions(&r);
        assert!(footer.contains("suppressed:"), "{footer}");
        assert!(footer.contains("DET001"), "{footer}");
        assert!(footer.contains("recorded in signal payload"), "{footer}");

        let list = format_det_suppressions_list(&r);
        assert!(list.contains("DET001"), "{list}");
        assert!(list.contains("\"recorded in signal payload\""), "{list}");
    }

    #[test]
    fn suppression_formatters_handle_none() {
        let empty = DetCheckReport::default();
        assert_eq!(format_det_suppressions(&empty), "suppressed: none");
        assert_eq!(
            format_det_suppressions_list(&empty),
            "no active suppressions"
        );
    }

    #[test]
    fn gate_trips_on_hard_blocker() {
        let r = report(WF_TIME);
        let gated = det_check_gate(&r, false);
        assert!(matches!(
            gated,
            Some(CliError::DetCheckFindings { errors: 1, .. })
        ));
    }

    #[test]
    fn gate_passes_on_warning_only_unless_deny_warnings() {
        let r = report(WF_WARN);
        assert!(det_check_gate(&r, false).is_none());
        let gated = det_check_gate(&r, true);
        assert!(matches!(
            gated,
            Some(CliError::DetCheckFindings {
                errors: 0,
                warnings: 1
            })
        ));
    }

    #[test]
    fn gate_passes_on_clean_report() {
        let clean = report(
            "#[workflow]\nasync fn wf(ctx: &WorkflowContext) -> Result<(), String> {\n    ctx.timer(\"t\", 1).await?;\n    Ok(())\n}\n",
        );
        assert!(det_check_gate(&clean, false).is_none());
        assert!(det_check_gate(&clean, true).is_none());
    }

    #[test]
    fn det_check_findings_error_exits_with_code_one() {
        let err = CliError::DetCheckFindings {
            errors: 3,
            warnings: 1,
        };
        assert_eq!(err.exit_code(), 1);
    }

    #[test]
    fn det_check_json_serializes_report() {
        let json = det_check_json(&report(WF_TIME)).expect("json");
        assert!(json.contains("\"rule_id\""));
        assert!(json.contains("DET001"));
        assert!(json.contains("\"severity\": \"error\""));
    }

    // FIX #5: `--list-suppressions --format json` must emit JSON (not silently
    // fall back to the text listing). The output must parse as JSON containing
    // the suppression.
    #[test]
    fn det_suppressions_json_is_valid_json_containing_the_suppression() {
        let r = report(WF_SUPPRESSED);
        let json = det_suppressions_json(&r).expect("suppressions json");
        let value: Value = serde_json::from_str(&json).expect("output must be valid JSON");
        let sups = value["suppressions"]
            .as_array()
            .expect("suppressions array");
        assert!(
            sups.iter().any(|s| s["rule_id"] == "DET001"),
            "the suppression must be present in the JSON, got: {json}"
        );
        assert!(
            sups.iter()
                .any(|s| s["reason"] == "recorded in signal payload"),
            "the reason must be present in the JSON, got: {json}"
        );
    }

    #[test]
    fn run_det_check_list_suppressions_json_exits_ok() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("sup.rs"), WF_SUPPRESSED).unwrap();
        let result = run_det_check(
            &[dir.path().to_path_buf()],
            DetCheckFormat::Json,
            false,
            true,
        );
        assert!(
            result.is_ok(),
            "--list-suppressions --format json must exit Ok, got: {result:?}"
        );
    }
}

#[cfg(test)]
mod schema_contract_cli_tests {
    use super::*;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("harvest").chain(args.iter().copied()))
            .expect("CLI should parse successfully")
    }

    //    // These live inline rather than in `tests/integration/schema_check_cli.rs`
    // because `Commands` and `SchemaCommand` are private: an external test can
    // only assert that parsing *succeeded*, which would pass just as happily
    // with a wrong default baked in. Destructuring is what actually pins the
    // defaults. Mirrors the det-check tests directly above.

    #[test]
    fn schema_check_parses_paths_and_format() {
        let cli = parse(&[
            "schema",
            "check",
            "--baseline",
            "base.json",
            "--current",
            "cur.json",
            "--format",
            "json",
        ]);
        match cli.command {
            Commands::Schema {
                command:
                    SchemaCommand::Check {
                        baseline,
                        current,
                        format,
                        require_current,
                        acknowledged_in,
                    },
            } => {
                assert_eq!(baseline, PathBuf::from("base.json"));
                assert_eq!(current, PathBuf::from("cur.json"));
                assert_eq!(format, SchemaCheckFormat::Json);
                assert!(!require_current, "the currency check is opt-in");
                assert_eq!(acknowledged_in, None, "the escape-hatch mode is opt-in");
            }
            other => panic!("expected Schema::Check, got {other:?}"),
        }
    }

    #[test]
    fn schema_check_require_current_and_acknowledged_in_are_mutually_exclusive() {
        let cli = parse(&[
            "schema",
            "check",
            "--current",
            "cur.json",
            "--require-current",
        ]);
        match cli.command {
            Commands::Schema {
                command:
                    SchemaCommand::Check {
                        require_current, ..
                    },
            } => assert!(require_current, "--require-current must set the flag"),
            other => panic!("expected Schema::Check, got {other:?}"),
        }

        // The escape-hatch mode diffs the BASE revision against the generated
        // contract, where deltas are the whole point — demanding currency there
        // would fail every legitimate acknowledged change.
        assert!(
            Cli::try_parse_from([
                "harvest",
                "schema",
                "check",
                "--current",
                "cur.json",
                "--require-current",
                "--acknowledged-in",
                "head.json",
            ])
            .is_err(),
            "combining the two modes is a contradiction and must not parse"
        );
    }

    #[test]
    fn schema_check_baseline_defaults_to_the_documented_path() {
        let cli = parse(&["schema", "check", "--current", "cur.json"]);
        match cli.command {
            Commands::Schema {
                command:
                    SchemaCommand::Check {
                        baseline, format, ..
                    },
            } => {
                // The default is what makes the documented one-liner short; if
                // it drifts from the constant the guide and CI recipe rot.
                assert_eq!(
                    baseline,
                    PathBuf::from(autumn_harvest::DEFAULT_SCHEMA_CONTRACT_PATH)
                );
                assert_eq!(format, SchemaCheckFormat::Text);
            }
            other => panic!("expected Schema::Check, got {other:?}"),
        }
    }

    #[test]
    fn schema_update_parses_acknowledge_and_recorded_in() {
        let cli = parse(&[
            "schema",
            "update",
            "--current",
            "cur.json",
            "--acknowledge",
            "drained via reset",
            "--recorded-in",
            "docs/changelog.d/pr-794-schema-contract-gate.md",
        ]);
        match cli.command {
            Commands::Schema {
                command:
                    SchemaCommand::Update {
                        baseline,
                        current,
                        acknowledge,
                        recorded_in,
                    },
            } => {
                assert_eq!(
                    baseline,
                    PathBuf::from(autumn_harvest::DEFAULT_SCHEMA_CONTRACT_PATH)
                );
                assert_eq!(current, PathBuf::from("cur.json"));
                assert_eq!(acknowledge.as_deref(), Some("drained via reset"));
                assert_eq!(
                    recorded_in.as_deref(),
                    Some("docs/changelog.d/pr-794-schema-contract-gate.md")
                );
            }
            other => panic!("expected Schema::Update, got {other:?}"),
        }
    }

    #[test]
    fn schema_update_acknowledge_is_optional_at_parse_time() {
        // The refusal is a *runtime* decision made only when a delta is
        // breaking, not a clap requirement — a purely compatible regeneration
        // must not need a justification.
        let cli = parse(&["schema", "update", "--current", "cur.json"]);
        match cli.command {
            Commands::Schema {
                command:
                    SchemaCommand::Update {
                        acknowledge,
                        recorded_in,
                        ..
                    },
            } => {
                assert!(acknowledge.is_none());
                assert!(recorded_in.is_none());
            }
            other => panic!("expected Schema::Update, got {other:?}"),
        }
    }

    #[test]
    fn schema_check_format_default_is_text() {
        assert_eq!(SchemaCheckFormat::default(), SchemaCheckFormat::Text);
    }
}

#[cfg(test)]
mod token_bootstrap_tests {
    use super::*;

    /// The builder produces a valid `hvst_` secret, an INSERT statement whose
    /// `name`/`scope`/`created_by` match the inputs, embeds ONLY the hash, and
    /// never leaks the secret into the SQL (issue #942, Codex P1 output-shape check).
    #[test]
    fn bootstrap_builder_emits_secret_and_hash_only_sql() {
        let token = build_bootstrap_token("ci-seed", "mutate", None, "op").expect("builds");

        assert!(
            token.secret.starts_with("hvst_"),
            "secret must carry the hvst_ prefix: {}",
            token.secret
        );
        // No drift: the stored hash IS the shared core helper's output.
        assert_eq!(
            token.hash,
            autumn_harvest::api_token::hash_secret(&token.secret),
            "hash must be hash_secret(secret) — the shared mint helper"
        );

        let sql = &token.insert_sql;
        assert!(
            sql.contains("INSERT INTO harvest_api_tokens"),
            "must be an INSERT: {sql}"
        );
        assert!(sql.contains("'mutate'"), "scope must appear in SQL: {sql}");
        assert!(sql.contains("'ci-seed'"), "name must appear in SQL: {sql}");
        assert!(sql.contains("'op'"), "created_by must appear in SQL: {sql}");
        assert!(sql.contains(&token.hash), "hash must be embedded: {sql}");
        assert!(
            sql.contains("gen_random_uuid()"),
            "id default expected: {sql}"
        );
        assert!(sql.contains("NOW()"), "created_at default expected: {sql}");
        // The secret must NEVER be smuggled into the SQL — only the hash is.
        assert!(
            !sql.contains(&token.secret),
            "the plaintext secret must not appear in the INSERT SQL: {sql}"
        );
    }

    /// `--scope` defaults to `mutate` (a seed must be able to mint others) and
    /// `--created-by`/`--name` default to `bootstrap`.
    #[test]
    fn bootstrap_defaults_scope_to_mutate() {
        let cli = Cli::try_parse_from(["harvest", "token", "bootstrap"])
            .expect("token bootstrap should parse with no flags");
        match cli.command {
            Commands::Token {
                command:
                    TokenCommand::Bootstrap {
                        name,
                        scope,
                        expires_at,
                        created_by,
                    },
            } => {
                assert_eq!(scope, "mutate", "default scope must be mutate");
                assert_eq!(name, "bootstrap");
                assert_eq!(created_by, "bootstrap");
                assert_eq!(expires_at, None);
            }
            other => panic!("expected token bootstrap, got {other:?}"),
        }
    }

    /// The `value_parser` rejects a scope outside {read, mutate}.
    #[test]
    fn bootstrap_rejects_invalid_scope() {
        let result = Cli::try_parse_from(["harvest", "token", "bootstrap", "--scope", "admin"]);
        assert!(result.is_err(), "an invalid --scope must fail to parse");
    }

    /// A `read`-scoped bootstrap flows the flag values through to the SQL.
    #[test]
    fn bootstrap_read_scope_flows_through_to_sql() {
        let cli = Cli::try_parse_from([
            "harvest",
            "token",
            "bootstrap",
            "--scope",
            "read",
            "--name",
            "dashboard",
            "--created-by",
            "release-eng",
        ])
        .expect("parses");
        let Commands::Token {
            command:
                TokenCommand::Bootstrap {
                    name,
                    scope,
                    expires_at,
                    created_by,
                },
        } = cli.command
        else {
            panic!("expected token bootstrap");
        };
        let token = build_bootstrap_token(&name, &scope, expires_at.as_deref(), &created_by)
            .expect("builds");
        assert!(token.insert_sql.contains("'read'"));
        assert!(token.insert_sql.contains("'dashboard'"));
        assert!(token.insert_sql.contains("'release-eng'"));
        assert!(!token.insert_sql.contains(&token.secret));
    }

    /// A valid `--expires-at` is embedded as a `timestamptz` literal.
    #[test]
    fn bootstrap_with_expiry_includes_timestamptz() {
        let token =
            build_bootstrap_token("n", "read", Some("2027-01-01T00:00:00Z"), "op").expect("builds");
        assert!(token.insert_sql.contains("expires_at"), "column present");
        assert!(
            token
                .insert_sql
                .contains("'2027-01-01T00:00:00Z'::timestamptz"),
            "expiry literal present: {}",
            token.insert_sql
        );
    }

    /// A malformed `--expires-at` is rejected before any SQL is emitted.
    #[test]
    fn bootstrap_rejects_bad_expiry() {
        let err = build_bootstrap_token("n", "read", Some("not-a-date"), "op");
        assert!(err.is_err(), "a non-RFC-3339 expiry must be rejected");
    }

    /// A crafted name with a single quote is escaped (Postgres `'` -> `''`),
    /// neutralizing SQL injection through the flag value.
    #[test]
    fn bootstrap_escapes_single_quotes_in_name() {
        let token = build_bootstrap_token("O'Brien", "read", None, "op").expect("builds");
        assert!(
            token.insert_sql.contains("'O''Brien'"),
            "single quote must be doubled: {}",
            token.insert_sql
        );
        assert!(!token.insert_sql.contains(&token.secret));
    }

    #[test]
    fn batch_preview_table_renders_count_sample_and_truncation() {
        let payload = serde_json::json!({
            "dry_run": true,
            "action": "Cancel",
            "matched_count": 150,
            "per_shard": [{ "shard_id": 0, "matched_count": 150 }],
            "sample": [
                { "execution_id": "e1", "workflow_name": "onboarding", "state": "RUNNING" },
                { "execution_id": "e2", "workflow_name": "onboarding", "state": "RUNNING" }
            ],
            "sample_cap": 100,
            "sample_truncated": true
        });
        let out = format_batch_preview_table(&payload);
        assert!(out.contains("DRY RUN"), "table: {out}");
        assert!(out.contains("matched_count: 150"), "table: {out}");
        assert!(out.contains("onboarding"), "sample rendered: {out}");
        assert!(
            out.contains("e1") && out.contains("e2"),
            "ids rendered: {out}"
        );
        // N2: the per_shard breakdown block renders.
        assert!(out.contains("shard"), "per_shard block rendered: {out}");
        assert!(
            out.contains("truncated: 2 of 150 shown"),
            "truncation note: {out}"
        );
    }

    /// M5: `batch submit --dry-run --json` renders the RAW compact preview body
    /// (no table header / `DRY RUN` banner), so `--json` output is pipeable.
    #[test]
    fn batch_preview_dry_run_json_renders_compact_body() {
        let cli = <Cli as clap::Parser>::try_parse_from([
            "harvest",
            "batch",
            "submit",
            "Cancel",
            "--filter-json",
            r#"{"workflow_name":"x"}"#,
            "--dry-run",
            "--json",
        ])
        .expect("CLI should parse");
        let value = serde_json::json!({
            "dry_run": true,
            "action": "Cancel",
            "matched_count": 42,
            "per_shard": [{ "shard_id": 0, "matched_count": 42 }],
            "sample": [],
            "sample_cap": 100,
            "sample_truncated": false,
            "status": "complete"
        });
        let out = render_response(&cli, &value).expect("render");
        assert!(out.contains("\"matched_count\""), "raw JSON body: {out}");
        assert!(!out.contains("DRY RUN"), "no table banner in --json: {out}");
        // Compact (not pretty): no multi-space indentation.
        assert!(!out.contains("\n  "), "compact, not pretty-printed: {out}");
    }

    /// N1: `--json` without `--dry-run` is rejected at clap parse time
    /// (`requires = "dry_run"`).
    #[test]
    fn batch_submit_json_without_dry_run_rejected() {
        let result = <Cli as clap::Parser>::try_parse_from([
            "harvest",
            "batch",
            "submit",
            "Cancel",
            "--filter-json",
            r#"{"workflow_name":"x"}"#,
            "--json",
        ]);
        assert!(
            result.is_err(),
            "--json without --dry-run must be a clap parse error"
        );
    }

    #[test]
    fn batch_preview_wants_table_only_with_dry_run() {
        fn parse_cli(args: &[&str]) -> Cli {
            <Cli as clap::Parser>::try_parse_from(
                std::iter::once("harvest").chain(args.iter().copied()),
            )
            .expect("CLI should parse")
        }
        let base = &[
            "batch",
            "submit",
            "Cancel",
            "--filter-json",
            r#"{"workflow_name":"x"}"#,
        ];

        let with_dry = parse_cli(&[base.as_slice(), &["--dry-run"]].concat());
        assert!(batch_preview_wants_table(&with_dry));
        assert!(!batch_preview_wants_raw_json(&with_dry));

        let dry_json = parse_cli(&[base.as_slice(), &["--dry-run", "--json"]].concat());
        assert!(!batch_preview_wants_table(&dry_json));
        assert!(batch_preview_wants_raw_json(&dry_json));

        let real = parse_cli(base);
        assert!(
            !batch_preview_wants_table(&real),
            "real submit must not table-render"
        );
    }
}

#[cfg(test)]
mod scaffold_new_tests {
    //! Unit tests for the `harvest new` scaffold (issue #692): clap parsing of
    //! the `New` variant and the name-derivation / keyword-rejection helpers.
    use super::*;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("harvest").chain(args.iter().copied()))
            .expect("CLI should parse successfully")
    }

    #[test]
    fn new_parses_name_and_flags() {
        let cli = parse(&[
            "new",
            "orders",
            "--force",
            "--template",
            "minimal",
            "--path",
            "/tmp/x",
        ]);
        match cli.command {
            Commands::New {
                name,
                path,
                force,
                template,
            } => {
                assert_eq!(name, "orders");
                assert_eq!(path, Some(PathBuf::from("/tmp/x")));
                assert!(force);
                assert_eq!(template, ScaffoldTemplate::Minimal);
            }
            other => panic!("expected New, got {other:?}"),
        }
    }

    #[test]
    fn new_defaults_no_path_no_force_minimal_template() {
        let cli = parse(&["new", "orders"]);
        match cli.command {
            Commands::New {
                name,
                path,
                force,
                template,
            } => {
                assert_eq!(name, "orders");
                assert_eq!(path, None);
                assert!(!force);
                assert_eq!(template, ScaffoldTemplate::Minimal);
            }
            other => panic!("expected New, got {other:?}"),
        }
    }

    #[test]
    fn derive_crate_ident_hyphens_to_underscores() {
        assert_eq!(derive_crate_ident("my-app"), "my_app");
        assert_eq!(derive_crate_ident("plain"), "plain");
    }

    #[test]
    fn keyword_names_are_rejected() {
        for kw in ["fn", "match", "async", "type", "move", "struct"] {
            assert!(
                validate_project_name(kw).is_err(),
                "keyword {kw:?} must be rejected"
            );
        }
    }

    #[test]
    fn valid_names_pass_validation() {
        for ok in ["orders", "my-app", "app2", "a"] {
            assert!(
                validate_project_name(ok).is_ok(),
                "{ok:?} should be a valid name"
            );
        }
    }

    #[test]
    fn uppercase_names_stay_accepted_cargo_new_parity() {
        // Acceptance must match `cargo new`: an uppercase name is legal (only a
        // cosmetic cargo warning), even when it case-folds to a keyword/reserved
        // word. Only names that ARE a keyword/reserved word are rejected.
        for ok in ["MyApp", "Fn", "Core", "Orders"] {
            assert!(
                validate_project_name(ok).is_ok(),
                "{ok:?} should be accepted (cargo-new parity)"
            );
        }
        for bad in ["fn", "core", "async"] {
            assert!(
                validate_project_name(bad).is_err(),
                "{bad:?} should be rejected (keyword/reserved)"
            );
        }
    }

    #[test]
    fn derive_crate_ident_is_clean_snake_case() {
        assert_eq!(derive_crate_ident("MyApp"), "myapp");
        assert_eq!(derive_crate_ident("trail-"), "trail");
        assert_eq!(derive_crate_ident("my--app"), "my_app");
        assert_eq!(derive_crate_ident("A_B-c"), "a_b_c");
    }
}

#[cfg(test)]
mod shard_placement_tests {
    //! CLI mapping tests for `--shard-id` / `--residency-key` on
    //! `workflow start` (issue #697). Mirror `mod conflict_policy_tests`:
    //! omitting a flag must omit the key entirely so an unpinned start's body
    //! stays byte-identical to a pre-#697 CLI.
    use super::*;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("harvest").chain(args.iter().copied()))
            .expect("CLI should parse successfully")
    }

    fn start_request(args: &[&str]) -> ApiRequest {
        parse(args)
            .api_request()
            .expect("request mapping should succeed")
    }

    #[test]
    fn start_omitting_placement_sends_neither_field() {
        let req = start_request(&["workflow", "start", "my_wf"]);
        let body = req.body.as_ref().expect("start should have a body");
        assert!(
            body.get("shard_id").is_none(),
            "omitting --shard-id must not send the field"
        );
        assert!(
            body.get("residency_key").is_none(),
            "omitting --residency-key must not send the field"
        );
    }

    #[test]
    fn start_with_shard_id_sends_a_numeric_field() {
        let req = start_request(&["workflow", "start", "my_wf", "--shard-id", "2"]);
        let body = req.body.as_ref().expect("start should have a body");
        assert_eq!(body.get("shard_id"), Some(&serde_json::json!(2)));
        assert!(body.get("residency_key").is_none());
    }

    #[test]
    fn start_with_residency_key_sends_a_string_field() {
        let req = start_request(&["workflow", "start", "my_wf", "--residency-key", "eu"]);
        let body = req.body.as_ref().expect("start should have a body");
        assert_eq!(body.get("residency_key"), Some(&serde_json::json!("eu")));
        assert!(body.get("shard_id").is_none());
    }

    #[test]
    fn start_rejects_both_placement_flags_at_parse_time() {
        // Two placement sources are ambiguous; clap must reject them before a
        // request is ever built.
        let parsed = Cli::try_parse_from([
            "harvest",
            "workflow",
            "start",
            "my_wf",
            "--shard-id",
            "1",
            "--residency-key",
            "eu",
        ]);
        assert!(
            parsed.is_err(),
            "--shard-id and --residency-key must be mutually exclusive"
        );
    }

    #[test]
    fn placement_preserves_other_start_fields() {
        let req = start_request(&[
            "workflow",
            "start",
            "my_wf",
            "--workflow-id",
            "order-42",
            "--residency-key",
            "eu",
        ]);
        let body = req.body.as_ref().expect("start should have a body");
        assert_eq!(
            body.get("workflow_id"),
            Some(&serde_json::json!("order-42"))
        );
        assert_eq!(body.get("residency_key"), Some(&serde_json::json!("eu")));
    }
}

#[cfg(test)]
mod replay_sample_bundle_tests {
    //! Bundle-writer tests for `harvest history export-sample` (issue #798).
    //!
    //! The split into `history_sample_bundle_files` (which files, named how) and
    //! `render_history_sample_summary` (the coverage table) keeps both pure, so
    //! the two things a CI operator depends on — that every sampled fixture is
    //! written under a name `replay_bundle` will read, and that truncation is
    //! stated out loud — are testable without touching a filesystem.
    use super::*;
    use serde_json::json;

    fn sample_response() -> Value {
        json!({
            "status": "complete",
            "payload_policy": "redacted",
            "manifest": {
                "generated_at": "2026-01-01T00:00:00Z",
                "status": "complete",
                "states": ["PAUSED", "RUNNING"],
                "per_workflow": [
                    { "workflow_name": "noisy", "sampled": 3, "in_flight_total": 700 },
                    { "workflow_name": "quiet", "sampled": 2, "in_flight_total": 2 }
                ],
                "sampled_total": 5,
                "in_flight_total": 702,
                "unavailable_shards": [],
                "inspected_shards": [0]
            },
            "exports": [
                { "workflow_name": "noisy", "execution_id": "11111111-1111-4111-8111-111111111111", "events": [] },
                { "workflow_name": "quiet", "execution_id": "22222222-2222-4222-8222-222222222222", "events": [] }
            ],
            "failures": []
        })
    }

    #[test]
    fn bundle_writes_one_fixture_per_export_plus_the_manifest() {
        let files = history_sample_bundle_files(&sample_response()).expect("bundle files");
        assert_eq!(files.len(), 3, "2 fixtures + 1 manifest");

        let names: Vec<&str> = files.iter().map(|file| file.name.as_str()).collect();
        assert!(names.contains(&"noisy--11111111-1111-4111-8111-111111111111.json"));
        assert!(names.contains(&"quiet--22222222-2222-4222-8222-222222222222.json"));
        assert!(
            names.contains(&autumn_harvest::replay_sample::SampleManifest::FILE_NAME),
            "the manifest must use the reserved name so replay_bundle reads it as \
             coverage rather than replaying it as a fixture: {names:?}"
        );
    }

    /// Every written fixture must be exactly what the replay harness reads, or
    /// the gate blocks on a harness error instead of verifying anything.
    #[test]
    fn each_fixture_deserializes_as_a_replay_snapshot_shape() {
        let files = history_sample_bundle_files(&sample_response()).expect("bundle files");
        for file in files
            .iter()
            .filter(|file| file.name != autumn_harvest::replay_sample::SampleManifest::FILE_NAME)
        {
            let parsed: Value = serde_json::from_str(&file.contents).expect("valid JSON");
            assert!(parsed.get("workflow_name").is_some(), "{}", file.name);
            assert!(parsed.get("events").is_some(), "{}", file.name);
        }
    }

    #[test]
    fn manifest_round_trips_into_the_core_type() {
        let files = history_sample_bundle_files(&sample_response()).expect("bundle files");
        let manifest = files
            .iter()
            .find(|file| file.name == autumn_harvest::replay_sample::SampleManifest::FILE_NAME)
            .expect("manifest present");
        let parsed: autumn_harvest::replay_sample::SampleManifest =
            serde_json::from_str(&manifest.contents)
                .expect("the written manifest must deserialize as the core SampleManifest");
        assert_eq!(parsed.sampled_total, 5);
        assert_eq!(parsed.in_flight_total, 702);
        assert!(parsed.is_truncated());
    }

    /// Two workflow names that sanitize to the same file name must not silently
    /// overwrite each other — that would make the bundle smaller than the
    /// manifest claims, so the gate would verify fewer fixtures than reported.
    #[test]
    fn colliding_fixture_names_are_disambiguated_never_overwritten() {
        let response = json!({
            "manifest": { "status": "complete", "sampled_total": 2, "in_flight_total": 2,
                          "per_workflow": [], "states": [], "unavailable_shards": [],
                          "inspected_shards": [0], "generated_at": "2026-01-01T00:00:00Z" },
            "exports": [
                { "workflow_name": "a/b", "execution_id": "same-id", "events": [] },
                { "workflow_name": "a:b", "execution_id": "same-id", "events": [] }
            ]
        });
        let files = history_sample_bundle_files(&response).expect("bundle files");
        let fixture_names: Vec<&str> = files
            .iter()
            .map(|file| file.name.as_str())
            .filter(|name| *name != autumn_harvest::replay_sample::SampleManifest::FILE_NAME)
            .collect();
        assert_eq!(fixture_names.len(), 2);
        assert_ne!(
            fixture_names[0], fixture_names[1],
            "a sanitization collision must be disambiguated: {fixture_names:?}"
        );
    }

    /// A non-sample body must fail loudly rather than write an empty bundle a
    /// gate would then evaluate for the wrong reason.
    #[test]
    fn a_non_sample_response_is_rejected_not_written_as_an_empty_bundle() {
        assert!(history_sample_bundle_files(&json!({ "error": "nope" })).is_err());
        assert!(history_sample_bundle_files(&json!({ "exports": [] })).is_err());
        assert!(
            history_sample_bundle_files(&json!({ "manifest": { "status": "complete" } })).is_err()
        );
    }

    #[test]
    fn summary_reports_per_type_coverage_and_names_truncation() {
        let summary = render_history_sample_summary(&sample_response(), Path::new("/tmp/fixtures"));
        assert!(summary.contains("/tmp/fixtures"), "{summary}");
        assert!(summary.contains("sampled 5 of 702"), "{summary}");
        assert!(summary.contains("noisy"), "{summary}");
        assert!(summary.contains("700"), "{summary}");
        assert!(
            summary.contains("NOTE: the sample is truncated"),
            "AC2: truncation must be stated, never implied: {summary}"
        );
    }

    /// A partial shard read makes the bundle a LOWER BOUND. A green gate over
    /// it must not read as "the fleet replays clean", so the warning is loud.
    #[test]
    fn summary_warns_loudly_on_partial_shard_coverage() {
        let mut response = sample_response();
        response["manifest"]["status"] = json!("partial");
        response["manifest"]["unavailable_shards"] = json!(["shard 1: connection refused"]);

        let summary = render_history_sample_summary(&response, Path::new("./fixtures"));
        assert!(summary.contains("WARNING"), "{summary}");
        assert!(summary.contains("LOWER BOUND"), "{summary}");
        assert!(summary.contains("shard 1: connection refused"), "{summary}");
    }

    #[test]
    fn summary_of_a_complete_untruncated_sample_carries_no_warning() {
        let response = json!({
            "manifest": { "status": "complete", "sampled_total": 4, "in_flight_total": 4,
                          "per_workflow": [{ "workflow_name": "wf", "sampled": 4, "in_flight_total": 4 }],
                          "states": ["RUNNING"], "unavailable_shards": [], "inspected_shards": [0],
                          "generated_at": "2026-01-01T00:00:00Z" },
            "exports": []
        });
        let summary = render_history_sample_summary(&response, Path::new("./fixtures"));
        assert!(!summary.contains("WARNING"), "{summary}");
        assert!(!summary.contains("NOTE:"), "{summary}");
    }

    /// An export stopped by the response byte budget must say so at write time.
    ///
    /// Distinct from the `NOTE: the sample is truncated` line, and the
    /// distinction is what the operator acts on: that reports the *intended*
    /// truncation of sampling `--per-workflow` out of a larger population, this
    /// reports an *unplanned* resource limit. Reading the two as the same thing
    /// leads to raising `--per-workflow`, which makes it strictly worse.
    #[test]
    fn summary_warns_that_the_export_was_cut_short_by_the_byte_budget() {
        let mut response = sample_response();
        response["manifest"]["truncated_by_size"] = json!(true);

        let summary = render_history_sample_summary(&response, Path::new("./fixtures"));
        assert!(
            summary.contains("response byte budget"),
            "the cause must be named: {summary}"
        );
        assert!(
            summary.contains("SMALLER than the sample you asked for"),
            "{summary}"
        );
        assert!(
            summary.contains("rather than raising --per-workflow"),
            "the fix must steer away from the one that makes it worse: {summary}"
        );
    }

    /// Control: an export that was NOT size-truncated must not claim it was.
    ///
    /// `sample_response()` is already truncated by population, so this also pins
    /// that the two truncation kinds are reported independently.
    #[test]
    fn summary_of_a_population_truncated_sample_does_not_claim_a_size_cut() {
        let summary = render_history_sample_summary(&sample_response(), Path::new("./fixtures"));
        assert!(
            summary.contains("NOTE: the sample is truncated"),
            "population truncation is still reported: {summary}"
        );
        assert!(
            !summary.contains("response byte budget"),
            "a population-truncated sample must not claim a size cut: {summary}"
        );
    }

    /// A candidate the sample selected but the export could not fetch must be
    /// reported at write time.
    ///
    /// The count is otherwise invisible: the failed candidate produces no
    /// document, so `sampled_total` counts only survivors and matches the file
    /// count exactly. An operator reading a summary that says "4 of 4 sampled"
    /// would reasonably believe the bundle is whole.
    #[test]
    fn summary_warns_that_the_export_dropped_selected_candidates() {
        let mut response = sample_response();
        response["manifest"]["export_failures"] = json!(2);

        let summary = render_history_sample_summary(&response, Path::new("./fixtures"));
        assert!(
            summary.contains("could not fetch 2 candidate(s)"),
            "the count must be named: {summary}"
        );
        assert!(
            summary.contains("BIASED subset"),
            "the consequence must be named, not just the cause: {summary}"
        );
        assert!(
            summary.contains("--max-bytes"),
            "the fix must be named: {summary}"
        );
        // The two shortfall causes are reported independently.
        assert!(
            !summary.contains("response byte budget"),
            "a dropped candidate is not a byte-budget cut: {summary}"
        );
    }

    /// Control: a clean export must not claim it dropped anything.
    #[test]
    fn summary_of_a_clean_export_does_not_claim_dropped_candidates() {
        let summary = render_history_sample_summary(&sample_response(), Path::new("./fixtures"));
        assert!(!summary.contains("BIASED subset"), "{summary}");
    }

    /// A manifest written before `truncated_by_size` existed must not warn.
    #[test]
    fn summary_of_a_legacy_manifest_without_the_size_flag_does_not_warn() {
        let response = json!({
            "manifest": { "status": "complete", "sampled_total": 4, "in_flight_total": 4,
                          "per_workflow": [{ "workflow_name": "wf", "sampled": 4, "in_flight_total": 4 }],
                          "states": ["RUNNING"], "unavailable_shards": [], "inspected_shards": [0],
                          "generated_at": "2026-01-01T00:00:00Z" },
            "exports": []
        });
        let summary = render_history_sample_summary(&response, Path::new("./fixtures"));
        assert!(!summary.contains("response byte budget"), "{summary}");
        assert!(
            !summary.contains("BIASED subset"),
            "a legacy manifest predates export_failures too, so it must not warn: {summary}"
        );
    }

    /// The CLI default is `redacted`, and the replay gate REFUSES a redacted
    /// bundle. An operator who omits `--payload-policy full` must find that out
    /// here — at the moment they write the bundle — not later, as an exit-2 wall
    /// of BLOCKED lines against a bundle they can no longer re-export (the fleet
    /// has moved on).
    #[test]
    fn summary_warns_that_a_redacted_bundle_is_refused_by_the_gate() {
        let summary = render_history_sample_summary(&sample_response(), Path::new("./fixtures"));
        assert!(summary.contains("WARNING"), "{summary}");
        assert!(summary.contains("redacted"), "{summary}");
        assert!(
            summary.contains("--payload-policy full"),
            "the warning must name the fix, not just the problem: {summary}"
        );
    }

    /// The complement: a `full` bundle is exactly what the gate wants, so it
    /// must carry no policy warning at all. Without this, the warning above
    /// could be unconditional and still pass.
    #[test]
    fn summary_of_a_full_bundle_carries_no_payload_policy_warning() {
        let mut response = sample_response();
        response["payload_policy"] = json!("full");
        let summary = render_history_sample_summary(&response, Path::new("./fixtures"));
        assert!(
            !summary.contains("--payload-policy full"),
            "a full bundle must not be told to re-export as full: {summary}"
        );
    }

    /// Re-exporting into a directory that already holds a bundle must replace
    /// it, not accumulate on top of it.
    ///
    /// Fixture names embed the execution id, so a second export does not
    /// overwrite the first — it *adds* to it. The manifest, however, has a fixed
    /// name and IS overwritten. The two then disagree: the manifest says
    /// "sampled 1", the directory holds two fixtures, and the gate replays both.
    /// The stale one is an execution that may have completed hours ago, so the
    /// gate silently verifies a population the coverage record never described.
    #[test]
    fn re_exporting_replaces_a_stale_bundle_rather_than_accumulating() {
        let dir = tempfile::tempdir().expect("tempdir");

        let mut first = sample_response();
        first["exports"] = json!([
            { "workflow_name": "old", "execution_id": "11111111-1111-4111-8111-111111111111", "events": [] }
        ]);
        write_history_sample_bundle(&first, dir.path()).expect("first export");

        let mut second = sample_response();
        second["exports"] = json!([
            { "workflow_name": "new", "execution_id": "22222222-2222-4222-8222-222222222222", "events": [] }
        ]);
        write_history_sample_bundle(&second, dir.path()).expect("second export");

        // Enumerated exactly the way `ReplayVerifier`'s bundle walk does, so
        // this counts what the gate would actually replay.
        let fixtures: Vec<String> = fs::read_dir(dir.path())
            .expect("read dir")
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.extension().and_then(std::ffi::OsStr::to_str) == Some("json"))
            .filter_map(|path| {
                path.file_name()
                    .map(|name| name.to_string_lossy().into_owned())
            })
            .filter(|name| name != autumn_harvest::replay_sample::SampleManifest::FILE_NAME)
            .collect();

        assert_eq!(
            fixtures.len(),
            1,
            "a stale fixture must not survive a re-export — the gate would \
             replay an execution the fresh manifest never counted: {fixtures:?}"
        );
        assert!(fixtures[0].starts_with("new--"), "{fixtures:?}");
    }

    /// A stale fixture in a SUBDIRECTORY must not survive a re-export either.
    ///
    /// `testing::collect_json_files` descends subdirectories, so a top-level-only
    /// clean leaves a nested fixture that the gate still replays — against a
    /// manifest that never counted it. The result is drift reported for an
    /// execution the operator did not sample, i.e. a false red on a healthy
    /// build, which is the exact verdict this whole feature exists to avoid.
    #[test]
    fn re_exporting_replaces_a_stale_fixture_nested_in_a_subdirectory() {
        let dir = tempfile::tempdir().expect("tempdir");

        let mut first = sample_response();
        first["exports"] = json!([
            { "workflow_name": "old", "execution_id": "11111111-1111-4111-8111-111111111111", "events": [] }
        ]);
        write_history_sample_bundle(&first, dir.path()).expect("first export");

        // Simulate a fixture that ended up one level down (a hand-copied file,
        // or a bundle laid out by an older/other tool).
        let nested = dir.path().join("shard-1");
        fs::create_dir_all(&nested).expect("nested dir");
        fs::write(nested.join("stale--33333333.json"), "{}").expect("nested fixture");

        let mut second = sample_response();
        second["exports"] = json!([
            { "workflow_name": "new", "execution_id": "22222222-2222-4222-8222-222222222222", "events": [] }
        ]);
        write_history_sample_bundle(&second, dir.path()).expect("second export");

        // Count what the gate would replay: recursive, `*.json`, manifest
        // excluded at any depth — the `collect_json_files` predicate.
        let mut found = Vec::new();
        let mut stack = vec![dir.path().to_path_buf()];
        while let Some(current) = stack.pop() {
            for entry in fs::read_dir(&current).expect("read dir").flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else if path.extension().and_then(std::ffi::OsStr::to_str) == Some("json")
                    && path.file_name().and_then(std::ffi::OsStr::to_str)
                        != Some(autumn_harvest::replay_sample::SampleManifest::FILE_NAME)
                {
                    found.push(path.display().to_string());
                }
            }
        }

        assert_eq!(
            found.len(),
            1,
            "a nested stale fixture must not survive a re-export — the gate \
             walks subdirectories and would replay it: {found:?}"
        );
        assert!(found[0].contains("new--"), "{found:?}");
    }

    /// The replace above must never reach a directory this CLI did not write.
    /// Refusing beats guessing: `--output-dir .` should not delete a user's
    /// JSON files because they happened to pick the wrong path.
    #[test]
    fn a_directory_of_foreign_json_is_refused_not_cleaned() {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::write(dir.path().join("important.json"), "{}").expect("write");

        let error = write_history_sample_bundle(&sample_response(), dir.path())
            .expect_err("a non-bundle directory must not be silently emptied");
        let rendered = error.to_string();
        assert!(
            rendered.contains("not a bundle") || rendered.contains("--output-dir"),
            "the error must tell the operator what to do: {rendered}"
        );
        assert!(
            dir.path().join("important.json").exists(),
            "a foreign file must survive"
        );
    }

    // ── JSON symlinks: the cleaner and the replay walk must agree (#798) ──
    //
    // `testing::collect_json_files` decides what the GATE replays with the
    // predicate "not a directory, extension `.json`" — a symlink satisfies it.
    // The cleaner therefore has to use the same rule, or the two walks disagree
    // about what a bundle contains and the disagreement is a false verdict.

    /// A `*.json` symlink is a fixture to the replay walk, so a re-export has to
    /// clear it like any other. Leaving it behind means the fresh manifest
    /// describes this run's sample while the directory still holds a previous
    /// one, and the gate replays the stale fixture and blocks an unchanged
    /// candidate — the exact false red this replace exists to prevent.
    ///
    /// Removing the LINK must not touch what it points at: the operator pointed
    /// `--output-dir` at the bundle, not at the target's directory.
    #[cfg(unix)]
    #[test]
    fn a_json_symlink_in_a_marked_bundle_is_cleared_like_a_regular_fixture() {
        let dir = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::tempdir().expect("outside tempdir");

        // Mark the directory as ours so the replace is authorized at all.
        write_history_sample_bundle(&sample_response(), dir.path()).expect("first export");

        let target = outside.path().join("someone-elses-history.json");
        fs::write(&target, "{\"not\":\"ours\"}").expect("write target");
        let link = dir.path().join("stale-linked-fixture.json");
        std::os::unix::fs::symlink(&target, &link).expect("symlink");

        write_history_sample_bundle(&sample_response(), dir.path()).expect("re-export");

        assert!(
            !link.exists() && link.symlink_metadata().is_err(),
            "a .json symlink is replayed by the gate, so the replace must clear \
             it; leaving it makes the next gate run replay a stale fixture"
        );
        assert!(
            target.exists(),
            "only the LINK may be removed — the file it points at is outside \
             the bundle and is not ours to delete"
        );
    }

    /// `fs::write` follows a symlink, so a stale link whose name collides with a
    /// fixture about to be written would send that write OUTSIDE the bundle.
    /// Clearing the link first is what keeps the replace contained to the
    /// directory the operator named.
    #[cfg(unix)]
    #[test]
    fn a_re_export_never_writes_through_a_colliding_json_symlink() {
        let dir = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::tempdir().expect("outside tempdir");
        write_history_sample_bundle(&sample_response(), dir.path()).expect("first export");

        // Collide with a name this export really writes, rather than a guess.
        let fixture_name = history_sample_bundle_files(&sample_response())
            .expect("bundle files")
            .into_iter()
            .map(|file| file.name)
            .find(|name| name != autumn_harvest::replay_sample::SampleManifest::FILE_NAME)
            .expect("the sample response must produce at least one fixture");

        let target = outside.path().join("untouchable.json");
        fs::write(&target, "ORIGINAL").expect("write target");
        let link = dir.path().join(&fixture_name);
        fs::remove_file(&link).ok();
        std::os::unix::fs::symlink(&target, &link).expect("symlink");

        write_history_sample_bundle(&sample_response(), dir.path()).expect("re-export");

        assert_eq!(
            fs::read_to_string(&target).expect("read target"),
            "ORIGINAL",
            "the replace must stay inside --output-dir: writing through a \
             colliding symlink clobbers a file the operator never named"
        );
    }

    /// The "is this our bundle?" evidence is the count of JSON entries, so a
    /// symlink has to count toward it too. If it does not, a directory holding
    /// only a linked JSON reads as empty, the refusal never fires, and this
    /// command writes into a directory it did not create.
    #[cfg(unix)]
    #[test]
    fn a_directory_whose_only_json_is_a_symlink_is_refused_not_written_into() {
        let dir = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::tempdir().expect("outside tempdir");
        let target = outside.path().join("important.json");
        fs::write(&target, "{}").expect("write target");
        std::os::unix::fs::symlink(&target, dir.path().join("linked.json")).expect("symlink");

        let error = write_history_sample_bundle(&sample_response(), dir.path())
            .expect_err("a directory of foreign JSON must be refused, linked or not");
        let rendered = error.to_string();
        assert!(
            rendered.contains("not a bundle") || rendered.contains("--output-dir"),
            "the error must tell the operator what to do: {rendered}"
        );
        assert!(target.exists(), "a foreign file must survive");
    }

    /// The manifest is what AUTHORIZES deleting a directory's contents, so the
    /// marker is held to a stricter rule than the fixtures it guards: only a
    /// real file we wrote counts. A linked manifest is not evidence this bundle
    /// is ours, and the safe answer to "not ours" is to refuse.
    #[cfg(unix)]
    #[test]
    fn a_symlinked_manifest_does_not_authorize_clearing_the_directory() {
        let dir = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::tempdir().expect("outside tempdir");
        let manifest_target = outside.path().join("manifest-elsewhere.json");
        fs::write(&manifest_target, "{}").expect("write target");
        std::os::unix::fs::symlink(
            &manifest_target,
            dir.path()
                .join(autumn_harvest::replay_sample::SampleManifest::FILE_NAME),
        )
        .expect("symlink");
        fs::write(dir.path().join("keep-me.json"), "{}").expect("write");

        let error = write_history_sample_bundle(&sample_response(), dir.path())
            .expect_err("a linked manifest must not authorize deleting real files");
        assert!(
            error.to_string().contains("not a bundle")
                || error.to_string().contains("--output-dir"),
            "{error}"
        );
        assert!(dir.path().join("keep-me.json").exists(), "must survive");
        assert_eq!(
            fs::read_to_string(&manifest_target).expect("read"),
            "{}",
            "the manifest write must not follow the link out of the bundle"
        );
    }

    /// A response that predates the field (or uses a policy this CLI does not
    /// know) must not be accused of being redacted — a spurious warning on a
    /// perfectly replayable bundle trains operators to ignore the real one.
    #[test]
    fn an_absent_or_unknown_payload_policy_is_not_reported_as_redacted() {
        let mut response = sample_response();
        response
            .as_object_mut()
            .expect("object")
            .remove("payload_policy");
        assert!(!bundle_is_redacted(&response));

        response["payload_policy"] = json!("some-future-policy");
        assert!(!bundle_is_redacted(&response));

        response["payload_policy"] = json!("redacted");
        assert!(bundle_is_redacted(&response));
    }
}

#[cfg(test)]
mod preflight_findings_tests {
    //! Rendering tests for the preflight detail block (issue #802 review).
    //!
    //! The summary table carries only each check's one-line verdict, so the
    //! specific unresolved references live in `details.failures` and used to be
    //! invisible in table mode — the exact payload the declared-dependency
    //! check exists to produce. These pin that they are rendered, that a
    //! healthy fleet's output is unchanged, and that both `failures` element
    //! shapes (plain string, structured object) survive.
    use super::*;

    fn check(name: &str, status: &str, failures: &Value, remediation: Option<&str>) -> Value {
        let mut value = serde_json::json!({
            "name": name,
            "status": status,
            "summary": "…",
            "details": { "failures": failures },
        });
        if let Some(text) = remediation {
            value["remediation"] = serde_json::json!(text);
        }
        value
    }

    #[test]
    fn a_failing_check_renders_each_failure_and_its_remediation() {
        let checks = vec![check(
            "catalog_consistency",
            "fail",
            &serde_json::json!([
                "workflow 'onboarding' references unregistered activity 'send_emial'",
                "workflow 'onboarding' references unregistered child workflow 'generate_reprot'",
            ]),
            Some("Register the named handler."),
        )];

        let rendered = format_preflight_findings(&checks);

        assert!(
            rendered.starts_with("catalog_consistency (fail)"),
            "{rendered}"
        );
        assert!(
            rendered.contains(
                "  - workflow 'onboarding' references unregistered activity 'send_emial'"
            ),
            "{rendered}"
        );
        assert!(
            rendered.contains(
                "  - workflow 'onboarding' references unregistered child workflow 'generate_reprot'"
            ),
            "{rendered}"
        );
        assert!(
            rendered.contains("  remediation: Register the named handler."),
            "{rendered}"
        );
    }

    #[test]
    fn a_passing_check_contributes_nothing() {
        let checks = vec![check(
            "catalog_consistency",
            "pass",
            &serde_json::json!([]),
            None,
        )];
        assert_eq!(format_preflight_findings(&checks), "");
    }

    #[test]
    fn a_healthy_fleet_leaves_the_table_output_unchanged() {
        let value = serde_json::json!({
            "overall_status": "pass",
            "observed_at": "2026-08-14T09:12:00Z",
            "checks": [check("catalog_consistency", "pass", &serde_json::json!([]), None)],
        });
        let rendered = format_preflight_table(&value);
        assert!(
            !rendered.contains("catalog_consistency (pass)"),
            "{rendered}"
        );
        assert!(
            rendered
                .ends_with("registered catalog contains unresolved workflow runtime references")
                || rendered.contains("STATUS"),
            "{rendered}"
        );
    }

    #[test]
    fn structured_failure_objects_are_rendered_not_dropped() {
        // `schedule_resolvability` pushes objects rather than strings; the
        // renderer must not silently swallow them.
        let checks = vec![check(
            "schedule_resolvability",
            "fail",
            &serde_json::json!([{ "schedule": "nightly", "reason": "unregistered workflow" }]),
            None,
        )];
        let rendered = format_preflight_findings(&checks);
        assert!(rendered.contains("nightly"), "{rendered}");
        assert!(rendered.contains("unregistered workflow"), "{rendered}");
    }

    #[test]
    fn a_warn_check_with_no_failures_and_no_remediation_is_omitted() {
        let mut value = serde_json::json!({ "name": "worker_health", "status": "warn" });
        value["details"] = serde_json::json!({});
        assert_eq!(format_preflight_findings(&[value]), "");
    }

    #[test]
    fn the_detail_block_is_appended_beneath_the_table() {
        let value = serde_json::json!({
            "overall_status": "fail",
            "observed_at": "2026-08-14T09:12:00Z",
            "checks": [check(
                "catalog_consistency",
                "fail",
                &serde_json::json!(["workflow 'onboarding' references unregistered activity 'send_emial'"]),
                Some("Register the named handler."),
            )],
        });

        let rendered = format_preflight_table(&value);
        let table_at = rendered.find("STATUS").expect("table header");
        let detail_at = rendered
            .find("catalog_consistency (fail)")
            .expect("detail block");
        assert!(
            table_at < detail_at,
            "detail block must follow the table:\n{rendered}"
        );
        assert!(rendered.contains("send_emial"), "{rendered}");
    }
}

// Issue #949: `harvest debug` clap-layer tests. The rendering / focus-resolution
// / breakpoint behaviour lives in `tests/integration/debug_cli.rs`; these pin
// only the argument surface (defaults, aliases, mutual exclusion), mirroring
// `det_check_cli_tests`.
#[cfg(test)]
mod debug_cli_tests {
    use super::*;
    use crate::debug::DebugFormat;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("harvest").chain(args.iter().copied()))
            .expect("CLI should parse successfully")
    }

    fn try_parse(args: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(std::iter::once("harvest").chain(args.iter().copied()))
    }

    #[test]
    fn debug_replay_parses_with_defaults() {
        let cli = parse(&["debug", "replay", "history.json"]);
        let Commands::Debug {
            command:
                DebugCommand::Replay {
                    history,
                    format,
                    step,
                    break_at_event_type,
                    break_at_index,
                    break_at_activity,
                    break_at_signal,
                    max_steps,
                    tui,
                },
        } = cli.command
        else {
            panic!("expected debug replay");
        };
        assert_eq!(history, PathBuf::from("history.json"));
        assert_eq!(format, DebugFormat::Text, "text is the default format");
        assert!(step.is_none());
        assert!(break_at_event_type.is_none());
        assert!(break_at_index.is_none());
        assert!(break_at_activity.is_none());
        assert!(break_at_signal.is_none());
        assert!(max_steps.is_none());
        assert!(!tui, "the stepper is opt-in");
    }

    #[test]
    fn debug_replay_parses_every_flag() {
        let cli = parse(&[
            "debug",
            "replay",
            "h.json",
            "--format",
            "json",
            "--max-steps",
            "50",
        ]);
        let Commands::Debug {
            command: DebugCommand::Replay {
                format, max_steps, ..
            },
        } = cli.command
        else {
            panic!("expected debug replay");
        };
        assert_eq!(format, DebugFormat::Json);
        assert_eq!(max_steps, Some(50));
    }

    #[test]
    fn debug_replay_accepts_each_breakpoint_flag_alone() {
        for args in [
            vec![
                "debug",
                "replay",
                "h.json",
                "--break-at-event-type",
                "TimerStarted",
            ],
            vec!["debug", "replay", "h.json", "--break-at-index", "3"],
            vec!["debug", "replay", "h.json", "--break-at-activity", "charge"],
            vec!["debug", "replay", "h.json", "--break-at-signal", "approval"],
        ] {
            assert!(
                try_parse(&args).is_ok(),
                "a single breakpoint flag must parse: {args:?}"
            );
        }
    }

    #[test]
    fn debug_replay_rejects_two_breakpoint_flags() {
        // AC2's breakpoints are alternatives, not a conjunction; clap enforces
        // it so `resolve_breakpoint` never has to pick a silent winner.
        assert!(
            try_parse(&[
                "debug",
                "replay",
                "h.json",
                "--break-at-index",
                "1",
                "--break-at-activity",
                "charge",
            ])
            .is_err(),
            "two breakpoint flags must be rejected"
        );
    }

    #[test]
    fn debug_replay_rejects_step_with_a_breakpoint() {
        assert!(
            try_parse(&[
                "debug",
                "replay",
                "h.json",
                "--step",
                "1",
                "--break-at-index",
                "2"
            ])
            .is_err(),
            "--step and a breakpoint are mutually exclusive"
        );
    }

    #[test]
    fn debug_replay_rejects_tui_with_format() {
        // `--tui` drives a terminal; a format flag alongside it would silently
        // do nothing.
        assert!(
            try_parse(&["debug", "replay", "h.json", "--tui", "--format", "json"]).is_err(),
            "--tui and --format are mutually exclusive"
        );
    }

    #[test]
    fn debug_diff_parses_both_sides() {
        let cli = parse(&["debug", "diff", "old.json", "new.json", "--format", "json"]);
        let Commands::Debug {
            command:
                DebugCommand::Diff {
                    left,
                    right,
                    format,
                },
        } = cli.command
        else {
            panic!("expected debug diff");
        };
        assert_eq!(left, PathBuf::from("old.json"));
        assert_eq!(right, PathBuf::from("new.json"));
        assert_eq!(format, DebugFormat::Json);
    }

    #[test]
    fn debug_diff_requires_two_paths() {
        assert!(try_parse(&["debug", "diff", "only-one.json"]).is_err());
    }
}

#[cfg(test)]
mod migrate_cli_tests {
    //! `harvest migrate` argument mapping, rendering, and the `--check` gate
    //! (issue #1240). No database: the DB half is covered by
    //! `autumn-harvest/tests/integration/migrate_tests.rs`.
    use super::*;
    use autumn_harvest::migrate::{
        FailedMigration, MigrationPlan, MigrationReport, MigrationScript, UnserializedMigration,
    };

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("harvest").chain(args.iter().copied()))
            .expect("CLI should parse successfully")
    }

    fn script(name: &str) -> MigrationScript {
        MigrationScript::new(name, "SELECT 1;").expect("well-formed migration name")
    }

    fn plan(pending: &[&str], already: usize, unrecognized: &[&str]) -> MigrationPlan {
        MigrationPlan {
            already_applied: (0..already)
                .map(|i| format!("2026010{i}000000_applied"))
                .collect(),
            pending: pending.iter().map(|n| script(n)).collect(),
            unrecognized: unrecognized.iter().map(|v| (*v).to_string()).collect(),
            ledger_exists: true,
        }
    }

    // ── argument mapping ────────────────────────────────────────────────────

    #[test]
    fn migrate_status_parses_repeated_targets_and_include_dirs() {
        let cli = parse(&[
            "migrate",
            "status",
            "--database-url",
            "postgres://harvest/a",
            "--database-url",
            "postgres://harvest/b",
            "--include-dir",
            "autumn-harvest-plugin/migrations/harvest",
            "--format",
            "json",
            "--check",
        ]);
        let Commands::Migrate {
            command:
                MigrateCommand::Status {
                    database_url,
                    include_dir,
                    format,
                    check,
                },
        } = cli.command
        else {
            panic!("expected migrate status");
        };
        // A multi-shard deployment migrates every shard database; one flag per
        // shard, not a delimiter-split single value.
        assert_eq!(
            database_url,
            vec!["postgres://harvest/a", "postgres://harvest/b"]
        );
        assert_eq!(
            include_dir,
            vec![PathBuf::from("autumn-harvest-plugin/migrations/harvest")]
        );
        assert_eq!(format, MigrateFormat::Json);
        assert!(check);
    }

    #[test]
    fn migrate_defaults_are_text_and_ungated() {
        let cli = parse(&[
            "migrate",
            "status",
            "--database-url",
            "postgres://harvest/a",
        ]);
        let Commands::Migrate {
            command:
                MigrateCommand::Status {
                    include_dir,
                    format,
                    check,
                    ..
                },
        } = cli.command
        else {
            panic!("expected migrate status");
        };
        assert!(include_dir.is_empty());
        assert_eq!(format, MigrateFormat::Text);
        assert!(!check, "the deploy gate must be opt-in");
    }

    #[test]
    fn migrate_run_dry_run_is_opt_in() {
        let cli = parse(&[
            "migrate",
            "run",
            "--database-url",
            "postgres://harvest/a",
            "--dry-run",
        ]);
        let Commands::Migrate {
            command: MigrateCommand::Run { dry_run, .. },
        } = cli.command
        else {
            panic!("expected migrate run");
        };
        assert!(dry_run);

        let cli = parse(&["migrate", "run", "--database-url", "postgres://harvest/a"]);
        let Commands::Migrate {
            command: MigrateCommand::Run { dry_run, .. },
        } = cli.command
        else {
            panic!("expected migrate run");
        };
        assert!(!dry_run);
    }

    #[test]
    fn migrate_requires_a_database_url() {
        // Nothing to default to: the whole point is a database Autumn cannot
        // reach, so guessing one would migrate the wrong database.
        assert!(
            Cli::try_parse_from(["harvest", "migrate", "run"]).is_err()
                || std::env::var("HARVEST_DATABASE_URL").is_ok(),
            "--database-url must be required when the env var is unset"
        );
    }

    #[test]
    fn migrate_is_not_routed_through_the_api() {
        // Guards the local-execution early return in `run_cli`: if a future
        // edit drops it, this panics instead of the command silently trying to
        // build an HTTP request.
        let cli = parse(&[
            "migrate",
            "status",
            "--database-url",
            "postgres://harvest/a",
        ]);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| cli.api_request()));
        assert!(
            result.is_err(),
            "migrate must be handled locally, never mapped to an API request"
        );
    }

    // ── include-dir loading ─────────────────────────────────────────────────

    #[test]
    fn include_dir_extends_the_embedded_set() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir(dir.path().join("29990101000000_extra")).expect("mkdir");
        std::fs::write(
            dir.path().join("29990101000000_extra").join("up.sql"),
            "SELECT 1;",
        )
        .expect("write up.sql");

        let scripts = migration_set(&[dir.path().to_path_buf()]).expect("set loads");
        let embedded = autumn_harvest::migrate::embedded();
        assert_eq!(scripts.len(), embedded.len() + 1);
        assert!(scripts.iter().any(|s| s.name == "29990101000000_extra"));
    }

    #[test]
    fn an_include_dir_reusing_an_embedded_version_is_refused() {
        // Diesel's ledger is keyed by version alone: one of the two would be
        // recorded and never run. Refused up front, before any connection.
        let embedded = autumn_harvest::migrate::embedded();
        let collision = embedded
            .first()
            .expect("Harvest has migrations")
            .version
            .clone();

        let dir = tempfile::tempdir().expect("tempdir");
        let name = format!("{collision}_collides");
        std::fs::create_dir(dir.path().join(&name)).expect("mkdir");
        std::fs::write(dir.path().join(&name).join("up.sql"), "SELECT 1;").expect("write up.sql");

        let error = migration_set(&[dir.path().to_path_buf()])
            .expect_err("a duplicate version must be refused");
        assert!(error.to_string().contains(&collision), "{error}");
    }

    #[test]
    fn a_missing_include_dir_names_the_path() {
        let error = migration_set(&[PathBuf::from("/nonexistent/harvest/migrations")])
            .expect_err("a missing directory must fail loudly");
        assert!(
            error
                .to_string()
                .contains("/nonexistent/harvest/migrations"),
            "{error}"
        );
    }

    // ── rendering ───────────────────────────────────────────────────────────

    #[test]
    fn status_text_lists_pending_migrations_per_database() {
        let targets = vec![
            (
                "postgres://harvest/a".to_string(),
                plan(&["20260801000000_x"], 2, &[]),
            ),
            ("postgres://harvest/b".to_string(), plan(&[], 3, &[])),
        ];
        let text = format_migrate_plan_text("harvest migrate status", &targets);
        assert!(text.contains("postgres://harvest/a"));
        assert!(text.contains("20260801000000_x"));
        assert!(
            text.contains("1 pending migration(s) across 2 database(s)"),
            "the summary must total across databases: {text}"
        );
    }

    #[test]
    fn status_text_says_when_a_database_has_never_been_migrated() {
        let mut empty = plan(&["20260801000000_x"], 0, &[]);
        empty.ledger_exists = false;
        let targets = vec![("postgres://harvest/a".to_string(), empty)];
        let text = format_migrate_plan_text("harvest migrate status", &targets);
        assert!(text.contains("never been migrated"), "{text}");
    }

    #[test]
    fn status_text_flags_ledger_rows_the_binary_does_not_know() {
        let targets = vec![(
            "postgres://harvest/a".to_string(),
            plan(&[], 2, &["29990101000000"]),
        )];
        let text = format_migrate_plan_text("harvest migrate status", &targets);
        assert!(text.contains("unrecognized"), "{text}");
        assert!(text.contains("29990101000000"), "{text}");
    }

    #[test]
    fn status_json_reports_names_and_a_pending_total() {
        let targets = vec![
            (
                "postgres://harvest/a".to_string(),
                plan(&["20260801000000_x"], 2, &[]),
            ),
            (
                "postgres://harvest/b".to_string(),
                plan(&["20260802000000_y"], 2, &[]),
            ),
        ];
        let rendered = migrate_plan_json("migrate status", &targets).expect("serializes");
        let value: Value = serde_json::from_str(&rendered).expect("valid JSON");
        assert_eq!(value["command"], "migrate status");
        assert_eq!(value["pending_total"], 2);
        assert_eq!(value["targets"][0]["database"], "postgres://harvest/a");
        assert_eq!(value["targets"][0]["pending"][0], "20260801000000_x");
        assert_eq!(value["targets"][1]["ledger_exists"], true);
    }

    #[test]
    fn an_unlocked_run_says_so_in_text_and_json() {
        // The engine logs this through `tracing`, and the `harvest` binary
        // installs no subscriber -- so if the report did not carry it, an
        // operator would get no signal that concurrent runs are unsafe here.
        let targets = vec![MigrateRunTarget {
            database: "postgres://harvest/a".to_string(),
            setup_failed: false,
            report: MigrationReport {
                applied: vec!["20260801000000_x".to_string()],
                already_applied: Vec::new(),
                applied_concurrently: Vec::new(),
                unrecognized: Vec::new(),
                failed: None,
                ledger_lock_available: false,
                applied_unserialized: vec![UnserializedMigration {
                    name: "20260801000000_x".to_string(),
                    reason: UnserializedReason::LedgerLockUnavailable,
                }],
            },
        }];

        let text = format_migrate_run_text(&targets);
        assert!(
            text.contains("WITHOUT the ledger lock"),
            "an unserialized apply must be visible to the operator: {text}"
        );
        assert!(
            text.contains("lacks UPDATE/DELETE/TRUNCATE"),
            "a missing grant must name the grant as the cause: {text}"
        );
        assert!(
            text.contains("one at a time"),
            "the warning must say what to do about it: {text}"
        );
        assert!(
            text.contains("20260801000000_x"),
            "the warning must name which migrations ran that way: {text}"
        );

        let value: Value =
            serde_json::from_str(&migrate_run_json(&targets).expect("serializes")).expect("JSON");
        assert_eq!(value["targets"][0]["ledger_lock_available"], false);
        assert_eq!(
            value["targets"][0]["applied_unserialized"][0]["name"],
            "20260801000000_x"
        );
        assert_eq!(
            value["targets"][0]["applied_unserialized"][0]["reason"],
            "ledger_lock_unavailable"
        );
    }

    #[test]
    fn a_nontransactional_migration_warns_without_blaming_privileges() {
        // A privileged role still cannot hold a lock across a migration that
        // declares `run_in_transaction = false` -- there is no transaction to
        // hold it in. Telling this operator to fix a grant would send them to
        // change something that was never the cause.
        let targets = vec![MigrateRunTarget {
            database: "postgres://harvest/a".to_string(),
            setup_failed: false,
            report: MigrationReport {
                applied: vec![
                    "20260801000000_x".to_string(),
                    "20260802000000_concurrent_index".to_string(),
                ],
                already_applied: Vec::new(),
                applied_concurrently: Vec::new(),
                unrecognized: Vec::new(),
                failed: None,
                ledger_lock_available: true,
                applied_unserialized: vec![UnserializedMigration {
                    name: "20260802000000_concurrent_index".to_string(),
                    reason: UnserializedReason::NoTransaction,
                }],
            },
        }];

        let text = format_migrate_run_text(&targets);
        assert!(text.contains("WITHOUT the ledger lock"), "{text}");
        assert!(
            text.contains("run_in_transaction = false"),
            "the cause must be the migration's own metadata: {text}"
        );
        assert!(
            !text.contains("lacks UPDATE/DELETE/TRUNCATE"),
            "a privileged role must not be told to fix a grant: {text}"
        );
        // Scoped to the warning block: the transactional migration belongs in
        // `applied:` above it, just not in the list of what ran unserialized.
        let warning = text.split("WARNING:").nth(1).expect("a warning block");
        assert!(
            warning.contains("20260802000000_concurrent_index"),
            "the warning names the unserialized migration: {warning}"
        );
        assert!(
            !warning.contains("20260801000000_x"),
            "the warning must not name a migration that WAS serialized: {warning}"
        );
    }

    #[test]
    fn a_locked_run_carries_no_warning() {
        let targets = vec![MigrateRunTarget {
            database: "postgres://harvest/a".to_string(),
            setup_failed: false,
            report: MigrationReport {
                applied: vec!["20260801000000_x".to_string()],
                already_applied: Vec::new(),
                applied_concurrently: Vec::new(),
                unrecognized: Vec::new(),
                failed: None,
                ledger_lock_available: true,
                applied_unserialized: Vec::new(),
            },
        }];

        let text = format_migrate_run_text(&targets);
        assert!(
            !text.contains("without the ledger lock"),
            "the normal path must not cry wolf: {text}"
        );
    }

    #[test]
    fn a_mixed_run_gives_each_migration_its_own_cause() {
        // A role without the privilege applies everything unserialized, and a
        // `run_in_transaction = false` migration in the same run is
        // unserialized for a reason no grant fixes. Attributing the whole list
        // to the missing grant would tell an operator that granting it makes
        // the run safe, which for the second entry is false.
        let targets = vec![MigrateRunTarget {
            database: "postgres://harvest/a".to_string(),
            setup_failed: false,
            report: MigrationReport {
                applied: vec![
                    "20260801000000_x".to_string(),
                    "20260802000000_concurrent_index".to_string(),
                ],
                already_applied: Vec::new(),
                applied_concurrently: Vec::new(),
                unrecognized: Vec::new(),
                failed: None,
                ledger_lock_available: false,
                applied_unserialized: vec![
                    UnserializedMigration {
                        name: "20260801000000_x".to_string(),
                        reason: UnserializedReason::LedgerLockUnavailable,
                    },
                    UnserializedMigration {
                        name: "20260802000000_concurrent_index".to_string(),
                        reason: UnserializedReason::NoTransaction,
                    },
                ],
            },
        }];

        let text = format_migrate_run_text(&targets);
        let warning = text.split("WARNING:").nth(1).expect("a warning block");
        assert!(
            warning.contains("lacks UPDATE/DELETE/TRUNCATE"),
            "the privilege cause must appear: {warning}"
        );
        assert!(
            warning.contains("run_in_transaction = false"),
            "the non-transactional cause must appear too, not be collapsed into \
             the privilege one: {warning}"
        );
        assert!(
            warning.contains("no grant changes this"),
            "the operator must be told the grant will not fix that entry: {warning}"
        );

        // Each cause sits on its own migration's line.
        let index_line = warning
            .lines()
            .find(|line| line.contains("20260802000000_concurrent_index"))
            .expect("a line for the non-transactional migration");
        assert!(
            index_line.contains("run_in_transaction = false"),
            "the cause must be attached to the migration it explains: {index_line}"
        );
        assert!(
            !index_line.contains("lacks UPDATE"),
            "and not to the other one's: {index_line}"
        );
    }

    #[test]
    fn run_text_and_json_report_what_was_applied() {
        let targets = vec![MigrateRunTarget {
            database: "postgres://harvest/a".to_string(),
            setup_failed: false,
            report: MigrationReport {
                applied: vec!["20260801000000_x".to_string()],
                already_applied: vec!["20260101000000_a".to_string()],
                applied_concurrently: vec!["20260802000000_y".to_string()],
                unrecognized: Vec::new(),
                failed: None,
                ledger_lock_available: true,
                applied_unserialized: Vec::new(),
            },
        }];

        let text = format_migrate_run_text(&targets);
        assert!(text.contains("20260801000000_x"), "{text}");
        assert!(
            text.contains("applied by a concurrent migrator"),
            "a concurrent apply must not read as 'nothing happened': {text}"
        );
        assert!(
            text.contains("1 migration(s) applied across 1 database(s)"),
            "{text}"
        );

        let value: Value =
            serde_json::from_str(&migrate_run_json(&targets).expect("serializes")).expect("JSON");
        assert_eq!(value["command"], "migrate run");
        assert_eq!(value["applied_total"], 1);
        assert_eq!(value["targets"][0]["applied"][0], "20260801000000_x");
        assert_eq!(
            value["targets"][0]["applied_concurrently"][0],
            "20260802000000_y"
        );
    }

    #[test]
    fn a_failing_target_is_reported_even_when_nothing_applied() {
        // The case that matters: a `run_in_transaction = false` migration that
        // failed part-way left changes it cannot list. An empty report for that
        // target would read as "nothing happened".
        let targets = vec![MigrateRunTarget {
            database: "postgres://harvest/a".to_string(),
            setup_failed: false,
            report: MigrationReport {
                applied: Vec::new(),
                already_applied: Vec::new(),
                applied_concurrently: Vec::new(),
                unrecognized: Vec::new(),
                failed: Some(FailedMigration {
                    name: "20260801000000_concurrent_index".to_string(),
                    rolled_back: false,
                }),
                ledger_lock_available: true,
                applied_unserialized: Vec::new(),
            },
        }];

        let text = format_migrate_run_text(&targets);
        assert!(
            text.contains("FAILED: 20260801000000_concurrent_index"),
            "{text}"
        );
        assert!(
            text.contains("NOT rolled back"),
            "an unrolled-back failure must say so: {text}"
        );

        let value: Value =
            serde_json::from_str(&migrate_run_json(&targets).expect("serializes")).expect("JSON");
        assert_eq!(
            value["targets"][0]["failed"]["name"],
            "20260801000000_concurrent_index"
        );
        assert_eq!(value["targets"][0]["failed"]["rolled_back"], false);
    }

    #[test]
    fn a_transactional_failure_says_the_database_is_unchanged() {
        let targets = vec![MigrateRunTarget {
            database: "postgres://harvest/a".to_string(),
            setup_failed: false,
            report: MigrationReport {
                applied: vec!["20260801000000_x".to_string()],
                already_applied: Vec::new(),
                applied_concurrently: Vec::new(),
                unrecognized: Vec::new(),
                failed: Some(FailedMigration {
                    name: "20260802000000_y".to_string(),
                    rolled_back: true,
                }),
                ledger_lock_available: true,
                applied_unserialized: Vec::new(),
            },
        }];
        let text = format_migrate_run_text(&targets);
        assert!(text.contains("rolled back"), "{text}");
        assert!(!text.contains("NOT rolled back"), "{text}");

        let value: Value =
            serde_json::from_str(&migrate_run_json(&targets).expect("serializes")).expect("JSON");
        assert_eq!(value["targets"][0]["failed"]["rolled_back"], true);
        // What DID apply is still listed beside the failure.
        assert_eq!(value["targets"][0]["applied"][0], "20260801000000_x");
    }

    #[test]
    fn a_target_the_run_could_not_prepare_is_not_reported_as_finished() {
        // Creating or reading the ledger failed, so no migration ever ran: the
        // report is empty and `failed` is None. Without the flag that is
        // byte-identical to "finished with nothing to do" -- a JSON consumer
        // would mark a database the run could not even inspect as done.
        let targets = vec![MigrateRunTarget {
            database: "postgres://harvest/a".to_string(),
            setup_failed: true,
            report: MigrationReport {
                applied: Vec::new(),
                already_applied: Vec::new(),
                applied_concurrently: Vec::new(),
                unrecognized: Vec::new(),
                failed: None,
                ledger_lock_available: true,
                applied_unserialized: Vec::new(),
            },
        }];

        let text = format_migrate_run_text(&targets);
        assert!(text.contains("FAILED: before any migration ran"), "{text}");

        let value: Value =
            serde_json::from_str(&migrate_run_json(&targets).expect("serializes")).expect("JSON");
        assert_eq!(value["targets"][0]["setup_failed"], true);
        assert!(value["targets"][0]["failed"].is_null());
    }

    #[test]
    fn a_finished_target_reports_no_failure() {
        let targets = vec![MigrateRunTarget {
            database: "postgres://harvest/a".to_string(),
            setup_failed: false,
            report: MigrationReport {
                applied: vec!["20260801000000_x".to_string()],
                already_applied: Vec::new(),
                applied_concurrently: Vec::new(),
                unrecognized: Vec::new(),
                failed: None,
                ledger_lock_available: true,
                applied_unserialized: Vec::new(),
            },
        }];
        assert!(!format_migrate_run_text(&targets).contains("FAILED"));
        let value: Value =
            serde_json::from_str(&migrate_run_json(&targets).expect("serializes")).expect("JSON");
        assert!(value["targets"][0]["failed"].is_null());
        assert_eq!(value["targets"][0]["setup_failed"], false);
    }

    // ── the deploy gate ─────────────────────────────────────────────────────

    #[test]
    fn the_check_gate_is_silent_when_every_database_is_migrated() {
        let targets = vec![("postgres://harvest/a".to_string(), plan(&[], 3, &[]))];
        assert!(migrate_pending_gate(&targets).is_none());
    }

    #[test]
    fn the_check_gate_counts_pending_migrations_and_databases() {
        let targets = vec![
            (
                "postgres://harvest/a".to_string(),
                plan(&["20260801000000_x"], 2, &[]),
            ),
            ("postgres://harvest/b".to_string(), plan(&[], 2, &[])),
            (
                "postgres://harvest/c".to_string(),
                plan(&["20260801000000_x", "20260802000000_y"], 2, &[]),
            ),
        ];
        let error = migrate_pending_gate(&targets).expect("pending migrations must gate");
        match error {
            CliError::MigrationsPending { pending, databases } => {
                assert_eq!(pending, 3);
                assert_eq!(databases, 2);
            }
            other => panic!("expected MigrationsPending, got {other:?}"),
        }
        // Exit 1 = "determined: not migrated", distinct from the exit-2
        // "could not determine" gates.
        let error = migrate_pending_gate(&targets).expect("pending migrations must gate");
        assert_eq!(error.exit_code(), 1);
        // The remedy must carry the flags forward: a bare `run` after a
        // `--check` with `--include-dir` applies fewer sets than the gate
        // examined, exits 0, and leaves the gating migration unapplied.
        let message = error.to_string();
        assert!(message.contains("--include-dir"), "{message}");
        assert!(message.contains("--database-url"), "{message}");
    }

    #[test]
    fn an_unrecognized_ledger_row_alone_never_gates() {
        // The database is ahead of this binary. That is worth reporting, but it
        // is not a reason to fail a deploy that has nothing to apply.
        let targets = vec![(
            "postgres://harvest/a".to_string(),
            plan(&[], 3, &["29990101000000"]),
        )];
        assert!(migrate_pending_gate(&targets).is_none());
    }

    // ── TLS / DSN normalization ─────────────────────────────────────────────

    #[test]
    fn a_verify_dsn_is_rewritten_so_tokio_postgres_can_parse_it() {
        // tokio-postgres 0.7 knows only disable/prefer/require and FAILS TO
        // PARSE anything else, so a `verify-full` DSN that libpq and the
        // `diesel` CLI accept would be rejected before we ever connect. rustls
        // verifies the chain and the hostname regardless, so `require` here
        // describes what the connector already does.
        for mode in ["verify-ca", "verify-full"] {
            let rewritten = normalize_sslmode(&format!(
                "postgres://u:p@db.internal/harvest?sslmode={mode}"
            ));
            assert!(rewritten.contains("sslmode=require"), "{rewritten}");
            assert!(!rewritten.contains(mode), "{rewritten}");
            rewritten
                .parse::<tokio_postgres::Config>()
                .expect("the rewritten DSN must parse");
        }
    }

    #[test]
    fn other_ssl_modes_and_dsns_pass_through_byte_identical() {
        for dsn in [
            "postgres://u:p@db.internal/harvest",
            "postgres://u:p@db.internal/harvest?sslmode=require",
            "postgres://u:p@db.internal/harvest?sslmode=disable",
            "postgres://u:p@db.internal/harvest?application_name=harvest%20migrate",
        ] {
            assert_eq!(normalize_sslmode(dsn), dsn, "must not be rewritten: {dsn}");
        }
    }

    #[test]
    fn the_libpq_keyword_form_is_rewritten_too() {
        let rewritten = normalize_sslmode("host=db.internal dbname=harvest sslmode=verify-full");
        assert_eq!(rewritten, "host=db.internal dbname=harvest sslmode=require");
        rewritten
            .parse::<tokio_postgres::Config>()
            .expect("the rewritten DSN must parse");
    }

    #[test]
    fn a_password_that_merely_contains_the_text_is_untouched() {
        // Only a whole `sslmode=<verify mode>` option is rewritten.
        let dsn = "host=db.internal password=sslmode=verify-full-not-really sslmode=require";
        assert_eq!(normalize_sslmode(dsn), dsn);
    }

    #[test]
    fn a_quoted_value_containing_whitespace_is_never_rewritten_inside() {
        // A whitespace split cannot see quoting, so it would rewrite the text
        // INSIDE the password -- corrupting the credential and failing
        // authentication against a database that was reachable.
        let dsn = "password='abc sslmode=verify-full def' sslmode=verify-full";
        let rewritten = normalize_sslmode(dsn);
        assert_eq!(
            rewritten, "password='abc sslmode=verify-full def' sslmode=require",
            "only the top-level option may change"
        );
        let config: tokio_postgres::Config = rewritten.parse().expect("still parses");
        assert_eq!(
            config.get_password(),
            Some(b"abc sslmode=verify-full def".as_slice())
        );
    }

    #[test]
    fn spacing_around_the_equals_sign_is_handled() {
        // libpq accepts it, so a DSN using it must not slip past unrewritten
        // and then fail to parse.
        let rewritten = normalize_sslmode("host=db.internal sslmode = verify-full");
        assert_eq!(rewritten, "host=db.internal sslmode = require");
        rewritten
            .parse::<tokio_postgres::Config>()
            .expect("the rewritten DSN must parse");
    }

    #[test]
    fn a_quoted_sslmode_value_is_rewritten_and_an_escaped_password_survives() {
        let rewritten = normalize_sslmode(r"password='a\'b c' sslmode='verify-ca'");
        assert_eq!(rewritten, r"password='a\'b c' sslmode=require");
        let config: tokio_postgres::Config = rewritten.parse().expect("still parses");
        assert_eq!(config.get_password(), Some(b"a'b c".as_slice()));
    }

    #[test]
    fn a_backslash_escape_in_a_bare_value_is_understood() {
        // tokio-postgres honours `\` escapes in unquoted values too, so
        // `password=abc\ def` is ONE value. Stopping at the escaped space
        // would take `def` for the next keyword and abandon the rewrite,
        // leaving a `verify-full` the driver then refuses.
        let rewritten = normalize_sslmode(r"password=abc\ def sslmode=verify-full");
        assert_eq!(rewritten, r"password=abc\ def sslmode=require");
        let config: tokio_postgres::Config = rewritten.parse().expect("still parses");
        assert_eq!(config.get_password(), Some(b"abc def".as_slice()));
    }

    #[test]
    fn an_escaped_multibyte_character_does_not_panic() {
        // `\é` is three bytes: advancing two would leave the scanner inside
        // the character and the next slice would panic on a non-char boundary.
        for dsn in [
            r"password='a\éb' sslmode=verify-full",
            r"password=a\éb sslmode=verify-full",
        ] {
            let rewritten = normalize_sslmode(dsn);
            assert!(rewritten.contains("sslmode=require"), "{rewritten}");
            let config: tokio_postgres::Config = rewritten.parse().expect("still parses");
            assert_eq!(config.get_password(), Some("aéb".as_bytes()));
        }
    }

    #[test]
    fn a_dsn_this_cannot_scan_is_returned_unchanged() {
        // An unterminated quote is tokio-postgres's error to report, not ours
        // to paper over by mangling the string first. `host=db password=` ends
        // after the `=`: reading a value there indexed past the end and
        // panicked, which an empty templated environment value would trip.
        for dsn in [
            "password='unterminated sslmode=verify-full",
            "host",
            "host=db password=",
            "host=db password=   ",
            "host=db sslmode=",
        ] {
            assert_eq!(normalize_sslmode(dsn), dsn);
        }
    }

    // ── target labels ───────────────────────────────────────────────────────

    #[test]
    fn a_keyword_form_target_is_labelled_by_its_own_dsn() {
        // `redact_dsn` only parses URLs, so every keyword-form shard used to
        // report as the same `<unparseable dsn>` -- and a partial report that
        // cannot tell shard A from shard B answers nothing.
        let label = migrate_target_label("host=shard-a dbname=harvest password='s e cret'", 1);
        assert!(label.contains("host=shard-a"), "{label}");
        assert!(label.contains("dbname=harvest"), "{label}");
        assert!(
            !label.contains("cret"),
            "the credential must not survive: {label}"
        );
    }

    #[test]
    fn keyword_form_targets_stay_distinguishable() {
        let a = migrate_target_label("host=shard-a dbname=harvest", 1);
        let b = migrate_target_label("host=shard-b dbname=harvest", 2);
        assert_ne!(a, b);
    }

    #[test]
    fn a_url_target_still_uses_the_url_redaction() {
        let label = migrate_target_label("postgres://u:hunter2@db.internal/harvest", 1);
        assert!(!label.contains("hunter2"), "{label}");
        assert!(label.contains("db.internal"), "{label}");
    }

    #[test]
    fn a_malformed_url_never_reaches_the_keyword_scanner() {
        // `redact_dsn` cannot parse this (`notaport`), and the keyword scanner
        // would take the whole prefix for one keyword whose value needs no
        // redaction -- handing the password straight to the deploy log.
        let label = migrate_target_label(
            "postgres://alice:hunter2@db:notaport/harvest?sslmode=require",
            1,
        );
        assert!(
            !label.contains("hunter2"),
            "credential leaked into: {label}"
        );
        assert_eq!(label, "<unparseable dsn> #1");
    }

    #[test]
    fn a_url_password_option_other_than_password_is_withheld_too() {
        // The URL branch delegates to `redact_dsn`, which matched the exact
        // key `password` while the keyword branch matched any key *containing*
        // it. So the same secret was redacted or not depending purely on how
        // the DSN was spelled, and `?sslpassword=` -- a real libpq option --
        // reached the migration report.
        for dsn in [
            "postgres://db.prod/harvest?sslpassword=hunter2",
            "postgresql://db.prod/harvest?sslmode=require&sslpassword=hunter2",
        ] {
            let label = migrate_target_label(dsn, 2);
            assert!(
                !label.contains("hunter2"),
                "credential leaked into: {label} (from {dsn})"
            );
            // Withholding the whole DSN costs the label its identity, so the
            // ordinal is what keeps two targets apart.
            assert_eq!(label, "<redacted dsn> #2", "from {dsn}");
        }
    }

    #[test]
    fn a_keyword_shaped_token_that_is_not_a_keyword_is_refused() {
        // A mistyped URL that loses the `://` but keeps the credential scans
        // as the "keyword" `postgres`: character-set-valid, and with no
        // `password=` key there is nothing for the scanner to redact, so the
        // whole DSN came back as if it had been examined. Only requiring a
        // keyword libpq actually recognizes catches it.
        for dsn in [
            "postgres=//alice:hunter2@db/harvest",
            "postgresql=//alice:hunter2@db/harvest",
            "notakeyword=alice:hunter2@db",
        ] {
            let label = migrate_target_label(dsn, 1);
            assert!(
                !label.contains("hunter2"),
                "credential leaked into: {label} (from {dsn})"
            );
            assert_eq!(label, "<unparseable dsn> #1", "from {dsn}");
        }
    }

    #[test]
    fn recognized_keywords_still_produce_a_usable_label() {
        // The refusal above must not swallow legitimate keyword DSNs: an
        // operator needs the shard's identity to know which one failed, and
        // libpq keywords are case-insensitive.
        let label = migrate_target_label("host=db.internal port=5432 dbname=harvest", 1);
        assert!(label.contains("db.internal"), "{label}");
        assert!(!label.starts_with("<unparseable"), "{label}");

        let mixed_case = migrate_target_label("Host=db.internal DBName=harvest", 1);
        assert!(!mixed_case.starts_with("<unparseable"), "{mixed_case}");

        // And a password in a recognized keyword DSN is still redacted.
        let with_password =
            migrate_target_label("host=db.internal password=hunter2 dbname=harvest", 1);
        assert!(
            !with_password.contains("hunter2"),
            "credential leaked into: {with_password}"
        );
    }

    #[test]
    fn a_credential_bearing_non_url_is_not_passed_through_either() {
        // No scheme, so not caught by the URL check -- caught instead by the
        // scanner refusing a "keyword" that is not `[A-Za-z0-9_]+`.
        for dsn in [
            "alice:hunter2@db.internal/harvest?sslmode=require",
            "postgres://alice:hunter2@db/harvest",
        ] {
            let label = migrate_target_label(dsn, 3);
            assert!(
                !label.contains("hunter2"),
                "credential leaked into: {label}"
            );
        }
    }

    #[test]
    fn wholly_withheld_url_targets_stay_distinguishable() {
        // A password in the query string cannot be rewritten, so `redact_dsn`
        // withholds the whole DSN -- identically for every shard. Without the
        // ordinal a partial multi-shard report cannot say which database moved.
        let a = migrate_target_label("postgres://db-a/harvest?password=secret", 1);
        let b = migrate_target_label("postgres://db-b/harvest?password=secret", 2);
        assert!(!a.contains("secret"), "{a}");
        assert!(!b.contains("secret"), "{b}");
        assert_eq!(a, "<redacted dsn> #1");
        assert_ne!(a, b);
    }

    #[test]
    fn an_unredactable_target_falls_back_to_an_ordinal() {
        // Neither a URL nor a scannable keyword DSN: it must still be tellable
        // apart from the next one, and must not print anything unexamined.
        let first = migrate_target_label("host=db password='unterminated", 1);
        let second = migrate_target_label("host=db password='unterminated", 2);
        assert_eq!(first, "<unparseable dsn> #1");
        assert_ne!(first, second);
    }

    // ── credential hygiene ──────────────────────────────────────────────────

    #[test]
    fn a_failure_message_carries_the_redacted_dsn_not_the_password() {
        let url = "postgres://harvest:hunter2@db.internal:5432/harvest";
        let redacted = autumn_harvest::backup_verify::redact_dsn(url);
        let error = migrate_error(
            url,
            &redacted,
            &format!("connection to `{url}` was refused"),
        );
        let rendered = error.to_string();
        assert!(
            !rendered.contains("hunter2"),
            "a deploy log must not learn the database password: {rendered}"
        );
        assert!(rendered.contains(&redacted), "{rendered}");
        assert_eq!(error.exit_code(), 1);
    }
}
