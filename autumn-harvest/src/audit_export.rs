//! Streaming the management-API audit trail to an external sink (issue #953).
//!
//! # Overview
//!
//! Every mutating management-API operation writes a row to
//! `harvest_audit_log` ([`crate::audit`], issue #158) — but those rows live
//! *per-shard, inside the same Postgres database they describe*, readable
//! only through Harvest's own API. For a security/compliance team that is
//! backwards: SIEM pipelines need privileged-action logs centralized,
//! off-box, and gap-detectable.
//!
//! This module ships that export as a deliberate replay of the durable
//! completion-callback design (issue #605): a boxed async transport trait in
//! core with **no HTTP client dependency** ([`AuditSink`]), a `reqwest`-based
//! signed-webhook implementation in `autumn-harvest-plugin`, a
//! two-transaction claim/deliver scanner that never holds a row lock across
//! network I/O, at-least-once delivery, and an operator redrive path.
//!
//! # Determinism contract
//!
//! **No new [`crate::event::WorkflowEvent`] variant. Zero replay-determinism
//! impact.** Audit rows are operational metadata, never event history;
//! `harvest_events` is never read or written here. The exporter only *reads*
//! `harvest_audit_log` and writes its own bookkeeping
//! (`harvest_audit_log.export_seq`, `harvest_audit_export_cursor`).
//!
//! # Opt-in, but one cost is not zero
//!
//! With no sink registered, [`GLOBAL_AUDIT_EXPORT_CONFIG`] is `None`. The
//! scanner returns `Ok(0)` before it issues a single query. No cursor row is
//! ever created, and `export_seq` stays `NULL` on every audit row. No new
//! query runs and no new row exists: read behavior matches the code before
//! this module existed.
//!
//! Write behavior does not. `harvest_audit_log_unexported_idx` is a partial
//! index on `export_seq IS NULL`. An unconfigured deployment leaves every row
//! `NULL` forever, so the index matches the whole audit table, and every
//! audit insert pays its maintenance cost. That cost is bounded by the audit
//! retention window only while retention stays enabled. At
//! `audit_retention_days = 0` the table, and the index, grow without bound.
//! Tracked as issue #1272.
//!
//! # Where the monotonic sequence comes from (and why not `BIGSERIAL`)
//!
//! AC4 requires a *strictly monotonic per-shard sequence* so a receiving SIEM
//! can detect gaps. The obvious implementation — a `BIGSERIAL` column on
//! `harvest_audit_log` — is wrong twice over:
//!
//! 1. **It would lose records.** A serial value is handed out *before* the
//!    transaction commits, so two concurrent audited operations can take
//!    sequence 5 and 6 and commit in the order 6, 5. A cursor of the form
//!    `WHERE seq > last_exported` that ships 6 first would then skip 5
//!    forever, which is precisely the silent loss AC2 forbids "by
//!    construction". `occurred_at` has the same defect (it is transaction
//!    *start* time, so it can even move backwards between concurrent
//!    inserts).
//! 2. **It would break under DR failover.** As
//!    `migrations/20260726000000_harvest_shard_generation/up.sql` records for
//!    issue #954, logical replication does not replicate sequence values — a
//!    promoted standby would re-issue sequence numbers it had already
//!    exported, corrupting the receiver's `(shard, seq)` accounting.
//!
//! Instead the exporter assigns the sequence itself, under the per-shard
//! cursor row lock, to rows it can actually *see* (`export_seq IS NULL`).
//! A late-committing row is still `NULL` when the next tick runs and simply
//! receives a later sequence: skipping is not representable. The counter
//! lives in `harvest_audit_export_cursor` — ordinary replicated table data,
//! not a sequence object. Sequences come out **dense**, so a receiver can
//! check contiguity rather than merely detect gaps.
//!
//! # At-least-once, never at-most-once
//!
//! Unlike a completion callback, an audit record may **never** be dropped
//! after N attempts — the export *is* the compliance artifact. There is
//! therefore no dead-letter arm in [`classify_export_outcome`]: a failing
//! sink backs off (capped exponential) and retries forever, and the cursor
//! never advances past the failure. A batch can consequently be delivered
//! more than once (a process death between the POST and the cursor write
//! re-sends it); receivers deduplicate on `(shard, seq)`, exactly as #605
//! specifies for callbacks.
//!
//! # OTLP-logs mapping
//!
//! [`AuditExportRecord`] is deliberately flat and maps 1:1 onto an
//! OpenTelemetry log record for embedders bridging to a collector:
//!
//! | `AuditExportRecord` field | OTLP log record field |
//! |---|---|
//! | `occurred_at` | `timeObservedUnixNano` / `timeUnixNano` |
//! | `operation` | `body` (or `event.name`) |
//! | `status` | `severityText` (`"succeeded"` -> `INFO`, `"failed"` -> `ERROR`). **Lowercase on the wire** — these are `audit::STATUS_SUCCEEDED` / `STATUS_FAILED` passed through verbatim, so a receiver matching `"FAILED"` silently files every failed privileged action as `INFO` |
//! | `error_summary` | `attributes["exception.message"]` |
//! | `shard` | `attributes["harvest.audit.source_shard"]` — the database the record was read from, and half the dedup key |
//! | `seq` | `attributes["harvest.audit.seq"]` |
//! | `shard_id` | `attributes["harvest.shard.id"]` — the shard the operation itself named, which can differ from `shard` |
//! | `actor`, `target_type`, `target_id`, `route_or_command`, `request_id`, `idempotency_key`, `source` | `attributes["harvest.audit.<field>"]` |
//! | `id` | `attributes["harvest.audit.id"]` |
//!
//! See `docs/audit-export.md` for the full receiver contract.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub use crate::completion_callback::{CallbackSecret, SIGNATURE_HEADER, TIMESTAMP_HEADER};

// ---------------------------------------------------------------------
// M1: wire shape
// ---------------------------------------------------------------------

/// HTTP header naming the shard a batch came from.
///
/// **Routing and triage metadata only — not covered by the signature.** The
/// signature (per #605's scheme) is over the request body alone, so a
/// receiver must read the authoritative `(shard, seq)` pair from each record
/// *in the body*. Deduplicating on these headers would mean deduplicating on
/// unauthenticated input: an attacker replaying a captured, validly-signed
/// batch with a shifted range could mark a real range as already-seen and
/// create exactly the silent gap this feature exists to prevent.
pub const SHARD_HEADER: &str = "X-Harvest-Audit-Shard";
/// HTTP header carrying the first (lowest) `seq` in the batch. Unsigned — see
/// [`SHARD_HEADER`].
pub const FIRST_SEQ_HEADER: &str = "X-Harvest-Audit-First-Seq";
/// HTTP header carrying the last (highest) `seq` in the batch. Unsigned — see
/// [`SHARD_HEADER`].
pub const LAST_SEQ_HEADER: &str = "X-Harvest-Audit-Last-Seq";

/// Default number of audit records claimed and delivered per batch.
pub const DEFAULT_EXPORT_BATCH_SIZE: i64 = 500;

/// Hard ceiling on the configurable batch size.
///
/// A batch is buffered in memory and `POSTed` as one body; an unbounded value
/// would let a misconfiguration try to serialize an entire retention window
/// into a single request.
pub const MAX_EXPORT_BATCH_SIZE: i64 = 5_000;

/// One exported audit record.
///
/// **The field set and JSON shape are a public contract** — a receiver's
/// index mappings depend on them. Do not reorder, rename, or drop a field
/// without a compatibility plan. Optional fields serialize as an explicit
/// `null` rather than being omitted, so a SIEM's schema inference sees a
/// stable object shape across every batch.
///
/// Carries no workflow payloads, activity inputs, or signal bodies: the
/// audit trail deliberately is not a second PII store ([`crate::audit`]),
/// and exporting it must not make it one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditExportRecord {
    /// The shard whose database this record was read from — half of the
    /// receiver's dedup key and of the gap-detection tuple (AC4).
    pub shard: i32,
    /// Strictly monotonic, dense, per-shard sequence assigned by the
    /// exporter. The other half of `(shard, seq)`.
    pub seq: i64,
    /// The audit row's own primary key.
    pub id: Uuid,
    /// The shard the *operation* recorded itself against, straight from
    /// `harvest_audit_log.shard_id`.
    ///
    /// Distinct from [`Self::shard`], which names the database the record was
    /// read from and is the dedup dimension. The two normally agree, but a
    /// control-plane mutation writes its audit row on the default shard while
    /// naming the shard it acted on, so exporting only one of them would make
    /// a receiver's correlation silently wrong.
    pub shard_id: Option<i32>,
    pub occurred_at: DateTime<Utc>,
    pub actor: String,
    pub operation: String,
    pub target_type: String,
    pub target_id: Option<String>,
    pub route_or_command: String,
    pub request_id: Option<String>,
    pub idempotency_key: Option<String>,
    pub status: String,
    pub error_summary: Option<String>,
    pub source: String,
}

/// Serialize a batch as JSON lines — one compact JSON object per line,
/// newline-terminated.
///
/// These are the exact bytes that get signed and delivered; a caller must
/// sign *these* bytes, never a re-serialization, so the receiver verifies
/// precisely what it received. `serde_json` emits struct fields in
/// declaration order with no interior newlines, so the output is
/// deterministic — which is what makes a redrive byte-identical (AC6).
///
/// An empty slice produces zero bytes (callers never deliver an empty
/// batch).
///
/// # Errors
/// Returns `Err` only if a record fails to serialize, which cannot happen
/// for [`AuditExportRecord`]'s field types; kept fallible so a future field
/// change cannot silently panic in the delivery path.
pub fn serialize_batch(records: &[AuditExportRecord]) -> Result<Vec<u8>, serde_json::Error> {
    let mut out = Vec::with_capacity(records.len() * 256);
    for record in records {
        serde_json::to_writer(&mut out, record)?;
        out.push(b'\n');
    }
    Ok(out)
}

/// Build the signed header set for one exported batch.
///
/// Uses the same `X-Harvest-Signature` HMAC-SHA256 scheme as issue #605
/// (delegating to [`crate::completion_callback::sign`], so the two can never
/// drift). **The signature covers the body only** — the shard/sequence/
/// timestamp headers are unauthenticated routing metadata; see
/// [`SHARD_HEADER`].
#[must_use]
pub fn export_headers(
    secret: &CallbackSecret,
    body: &[u8],
    shard: i32,
    first_seq: i64,
    last_seq: i64,
    now: DateTime<Utc>,
) -> Vec<(&'static str, String)> {
    vec![
        (
            SIGNATURE_HEADER,
            crate::completion_callback::sign(secret, body),
        ),
        (TIMESTAMP_HEADER, now.to_rfc3339()),
        (SHARD_HEADER, shard.to_string()),
        (FIRST_SEQ_HEADER, first_seq.to_string()),
        (LAST_SEQ_HEADER, last_seq.to_string()),
    ]
}

// ---------------------------------------------------------------------
// M2: the embedder transport seam
// ---------------------------------------------------------------------

/// The result of one sink delivery attempt.
///
/// Either a response status was observed (`status`, whatever it was), or the
/// request never got one (`transport_error` — connect failure, timeout, TLS
/// or DNS error). Mirrors
/// [`crate::completion_callback::DeliveryAttempt`] rather than reusing it so
/// a future change to callback delivery semantics cannot silently alter
/// audit-export semantics, where the cost of a misclassification is a
/// compliance gap rather than a missed notification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkAttempt {
    pub status: Option<u16>,
    pub transport_error: Option<String>,
}

impl SinkAttempt {
    /// A response was received with this status (success or not).
    #[must_use]
    pub const fn success(status: u16) -> Self {
        Self {
            status: Some(status),
            transport_error: None,
        }
    }

    /// No response was received at all.
    #[must_use]
    pub const fn transport_error(message: String) -> Self {
        Self {
            status: None,
            transport_error: Some(message),
        }
    }

    /// `true` only for a 2xx response status.
    ///
    /// A 3xx is deliberately *not* a success: the plugin sink never follows
    /// redirects (`redirect::Policy::none()`), so a 3xx means the batch was
    /// not accepted and must be retried, not acknowledged.
    #[must_use]
    pub fn is_success(&self) -> bool {
        matches!(self.status, Some(s) if (200..300).contains(&s))
    }
}

/// One batch handed to an [`AuditSink`].
///
/// Carries both the canonical `body` (the signed bytes an HTTP sink POSTs
/// verbatim) and the parsed `records`, so a non-HTTP sink — Kinesis, a file,
/// an OTLP-logs bridge — can map the structured form without re-parsing what
/// core just serialized.
pub struct AuditBatch<'a> {
    /// Shard this batch was read from.
    pub shard: i32,
    /// Lowest `seq` in the batch.
    pub first_seq: i64,
    /// Highest `seq` in the batch; the position the cursor advances to on
    /// acknowledgement.
    pub last_seq: i64,
    /// The records, ascending by `seq`.
    pub records: &'a [AuditExportRecord],
    /// Canonical JSON-lines body — exactly the bytes covered by the
    /// signature in `headers`.
    pub body: &'a [u8],
    /// Signature, timestamp, shard, and sequence-range headers.
    pub headers: &'a [(&'static str, String)],
}

/// Future returned by [`AuditSink::deliver`].
pub type SinkFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = SinkAttempt> + Send + 'a>>;

/// An embedder-supplied (or plugin-default) transport for audit-record
/// export.
///
/// Implementations are a thin transport: hand `batch` to the destination and
/// report what happened. Batching, sequencing, HMAC signing, cursor
/// management, retry/backoff, and lag accounting all happen in core, above
/// this trait — an implementation does not need to reason about any of it,
/// and **must not** acknowledge a batch it did not durably accept: a
/// `success` return advances the cursor past those records.
///
/// Core ships no HTTP client, so there is no default implementation here;
/// `autumn-harvest-plugin` supplies the `reqwest`-based signed-webhook sink,
/// exactly as it does for [`crate::completion_callback::CompletionCallbackDeliverer`]
/// and [`crate::payload_store::PayloadStore`].
pub trait AuditSink: Send + Sync + 'static {
    fn deliver<'a>(&'a self, batch: &'a AuditBatch<'a>) -> SinkFuture<'a>;
}

// ---------------------------------------------------------------------
// M3: pure retry/cursor decisions
// ---------------------------------------------------------------------

/// Capped exponential backoff for a failing sink.
///
/// Deliberately *not* [`crate::policy::RetryPolicy`]: that type carries a
/// `max_attempts` ceiling whose whole purpose is to eventually give up and
/// dead-letter, which must never happen to an audit record. Only the
/// backoff math is shared (via [`crate::policy::compute_retry_delay`]), so
/// the two cannot drift.
#[derive(Debug, Clone, PartialEq)]
pub struct ExportBackoff {
    /// Delay after the first failure.
    pub initial_interval: std::time::Duration,
    /// Multiplier applied per consecutive failure.
    pub backoff_coefficient: f64,
    /// Hard ceiling on the delay, so a long sink outage still retries on a
    /// predictable cadence (and recovers promptly when the sink returns).
    pub max_interval: std::time::Duration,
}

impl Default for ExportBackoff {
    /// 1s -> 2s -> 4s ... capped at 60s.
    ///
    /// The cap is what bounds recovery time after an outage: the success
    /// metric asks for zero gaps after a 10-minute sink outage, and a
    /// 60-second ceiling means at most one minute of extra lag once the sink
    /// returns.
    fn default() -> Self {
        Self {
            initial_interval: std::time::Duration::from_secs(1),
            backoff_coefficient: 2.0,
            max_interval: std::time::Duration::from_secs(60),
        }
    }
}

/// What the scanner should write after one delivery attempt.
///
/// Note the absence of a dead-letter arm — see the module docs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExportOutcome {
    /// 2xx — advance the shard's cursor to `through_seq` and clear the error
    /// state.
    Advance { through_seq: i64, status: u16 },
    /// Anything else — **hold the cursor exactly where it is** and retry at
    /// `next_attempt_at`.
    Backoff {
        next_attempt_at: DateTime<Utc>,
        last_status: Option<u16>,
        last_error: Option<String>,
        consecutive_failures: i32,
    },
}

/// Decide what to write after one delivery attempt.
///
/// Pure, so the "never advance past a failure" invariant (AC2) is pinned by
/// a unit test rather than requiring a database and a broken sink to
/// observe.
///
/// `consecutive_failures` is the count *before* this attempt; the returned
/// [`ExportOutcome::Backoff`] carries the incremented value (saturating).
#[must_use]
pub fn classify_export_outcome(
    attempt: &SinkAttempt,
    through_seq: i64,
    consecutive_failures: i32,
    backoff: &ExportBackoff,
    now: DateTime<Utc>,
) -> ExportOutcome {
    if let Some(status) = attempt.status
        && attempt.is_success()
    {
        return ExportOutcome::Advance {
            through_seq,
            status,
        };
    }

    // `compute_retry_delay` treats `attempt` as 1-based and exponentiates on
    // `attempt - 1`, so the first failure (0 prior failures) must map to 1.
    let failures = consecutive_failures.saturating_add(1);
    let delay = crate::policy::compute_retry_delay(
        backoff.initial_interval,
        backoff.backoff_coefficient,
        backoff.max_interval,
        u32::try_from(failures).unwrap_or(u32::MAX),
    );
    let next_attempt_at = now
        + chrono::Duration::from_std(delay).unwrap_or_else(|_| {
            chrono::Duration::from_std(backoff.max_interval)
                .unwrap_or_else(|_| chrono::Duration::seconds(60))
        });

    ExportOutcome::Backoff {
        next_attempt_at,
        last_status: attempt.status,
        last_error: attempt.transport_error.clone(),
        consecutive_failures: failures,
    }
}

/// Result of resolving an operator's redrive request against the live
/// cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RewindOutcome {
    /// The cursor moves backwards from `from` to `to`; every record with
    /// `seq > to` re-exports.
    Rewound { from: i64, to: i64 },
    /// Nothing to do — the request did not move the cursor backwards.
    NoOp { cursor: i64, requested: i64 },
    /// No cursor exists for this shard: audit export has never run here, so
    /// there is nothing to rewind. Returned only by the database-backed
    /// [`rewind_cursor`]; [`resolve_rewind`] is pure over an existing cursor.
    NotConfigured,
}

/// Resolve a redrive request. **A cursor may only ever move backwards.**
///
/// Moving it forward would mark records delivered that never were — the
/// exact gap this feature exists to make impossible — so a forward request
/// is refused outright rather than clamped and applied, and the caller
/// reports the refusal to the operator instead of silently doing nothing
/// useful.
///
/// A negative request is clamped to `0` (re-export everything still
/// retained) rather than rejected: `0` is its only sane reading, and writing
/// a negative cursor would violate the table's `CHECK (last_acked_seq >= 0)`.
#[must_use]
pub const fn resolve_rewind(current_acked: i64, requested: i64) -> RewindOutcome {
    let target = if requested < 0 { 0 } else { requested };
    if target >= current_acked {
        return RewindOutcome::NoOp {
            cursor: current_acked,
            requested,
        };
    }
    RewindOutcome::Rewound {
        from: current_acked,
        to: target,
    }
}

// ---------------------------------------------------------------------
// M3b: builder-time configuration
// ---------------------------------------------------------------------

/// Everything an embedder can set through `HarvestBuilder`'s
/// `audit_export_*` methods, resolved into an [`AuditExportRuntimeConfig`] at
/// startup.
///
/// With `sink` and `webhook_url` both `None` — the default — audit export is
/// never installed and the scanner is entirely inert (AC8). The partial
/// index still costs insert-time maintenance; see the module-level caveat
/// above (issue #1272).
#[derive(Clone)]
pub struct AuditExportBuilderConfig {
    /// Allowed sink hosts. Required (non-empty) for a `webhook_url` to
    /// validate, mirroring the completion-callback SSRF posture (#605).
    pub allowlist: crate::completion_callback::HostAllowlist,
    /// Permit `http://` sink URLs. Off by default: audit records name who did
    /// what to which tenant, and shipping them in cleartext is a finding in
    /// its own right.
    pub allow_http: bool,
    /// Permit IP-literal sink hosts.
    pub allow_ip_literals: bool,
    /// Signed-webhook endpoint for the plugin's default sink.
    pub webhook_url: Option<String>,
    /// HMAC key for `X-Harvest-Signature`.
    pub secret: Option<CallbackSecret>,
    /// Embedder-supplied sink. Takes precedence over `webhook_url`.
    pub sink: Option<std::sync::Arc<dyn AuditSink>>,
    /// Records per batch.
    pub batch_size: i64,
    /// Capped exponential backoff after a sink failure.
    pub backoff: ExportBackoff,
    /// How long a claim holds a shard's cursor, and the timeout applied to the
    /// sink call itself. Must exceed the sink's own per-request timeout — see
    /// [`DEFAULT_EXPORT_LEASE`].
    pub lease: std::time::Duration,
}

/// A webhook URL reduced to its origin, for diagnostics.
///
/// A SIEM ingest endpoint routinely carries its credential in the path or the
/// query string (`.../services/collector/<token>`, `?api_key=...`), so the full
/// URL is a secret in the same way the HMAC key is (issue #953, Codex review
/// round 23 P1). `AuditExportBuilderConfig`'s `Debug` is hand-written precisely
/// to keep secrets out of logs and panic messages — it already redacts the HMAC
/// key and the sink — but it printed the URL verbatim, which is the same leak
/// `audit_sink::describe_without_url` exists to prevent on the error path. The
/// origin is what makes a config dump useful ("which host is this pointed at?")
/// and is the part that is not a credential.
///
/// Falls back to a marker rather than the input when the URL cannot be parsed:
/// an unparseable string must not be echoed on the assumption it is harmless.
pub(crate) fn redact_webhook_url(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return "<unparseable webhook url redacted>".to_string();
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    // Strip any userinfo (`user:pass@host`), itself a credential.
    let host = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    if host.is_empty() {
        return "<unparseable webhook url redacted>".to_string();
    }
    format!("{scheme}://{host}/<redacted>")
}

impl std::fmt::Debug for AuditExportBuilderConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuditExportBuilderConfig")
            .field("allowlist", &self.allowlist)
            .field("allow_http", &self.allow_http)
            .field("allow_ip_literals", &self.allow_ip_literals)
            .field(
                "webhook_url",
                &self.webhook_url.as_deref().map(redact_webhook_url),
            )
            .field("secret", &self.secret)
            .field("sink", &self.sink.as_ref().map(|_| "<AuditSink>"))
            .field("batch_size", &self.batch_size)
            .field("backoff", &self.backoff)
            .field("lease", &self.lease)
            .finish()
    }
}

impl Default for AuditExportBuilderConfig {
    fn default() -> Self {
        Self {
            allowlist: crate::completion_callback::HostAllowlist::new(),
            allow_http: false,
            allow_ip_literals: false,
            webhook_url: None,
            secret: None,
            sink: None,
            batch_size: DEFAULT_EXPORT_BATCH_SIZE,
            backoff: ExportBackoff::default(),
            lease: DEFAULT_EXPORT_LEASE,
        }
    }
}

impl AuditExportBuilderConfig {
    /// `true` when audit export should be installed at all.
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.sink.is_some() || self.webhook_url.is_some()
    }

    /// The [`crate::completion_callback::SsrfPolicy`] implied by this config.
    #[must_use]
    pub fn ssrf_policy(&self) -> crate::completion_callback::SsrfPolicy {
        crate::completion_callback::SsrfPolicy {
            allowlist: self.allowlist.clone(),
            allow_http: self.allow_http,
            allow_ip_literals: self.allow_ip_literals,
        }
    }

    /// Validate the configured webhook URL against this config's own SSRF
    /// policy. Called at `HarvestBuilder::try_build()` time so a bad sink URL
    /// fails startup rather than silently never delivering.
    ///
    /// An embedder-supplied [`AuditSink`] is not validated here — it is not a
    /// URL, and where it ships is the embedder's decision.
    ///
    /// `true` when a signed webhook is configured but no HMAC key is.
    ///
    /// Fails the build rather than warning (issue #953 review): the signature
    /// *is* the tamper-evidence control this feature promises, and HMAC-SHA256
    /// accepts a zero-length key happily — so an unconfigured secret does not
    /// produce a missing header, it produces a well-formed
    /// `X-Harvest-Signature` that anyone can recompute. A receiver verifying
    /// it would see a valid signature on a batch any third party could have
    /// forged, which is worse than no signature at all. A startup
    /// `tracing::warn!` on a fleet is not a control.
    ///
    /// Only the *webhook* path is gated. An embedder-supplied [`AuditSink`]
    /// may authenticate however it likes (IAM, mTLS, a local file), so an
    /// absent HMAC key there is a legitimate configuration and only warns.
    #[must_use]
    pub fn webhook_is_missing_a_secret(&self) -> bool {
        if self.sink.is_some() || self.webhook_url.is_none() {
            return false;
        }
        // An *explicitly empty* secret is missing, not configured (issue #953,
        // Codex review round 3 P1). `audit_export_secret(env::var("...")
        // .unwrap_or_default())` with the variable unset yields
        // `Some(CallbackSecret(b""))`, which an `is_none()` check waves through
        // — and produces exactly the publicly-reproducible signature this
        // check exists to reject. Failing closed has to mean the bytes, not
        // the `Option`.
        self.secret
            .as_ref()
            .is_none_or(|secret| secret.as_bytes().is_empty())
    }

    /// Validate the webhook URL against the SSRF policy.
    ///
    /// Skipped entirely when an embedder-supplied `sink` is set, matching
    /// [`Self::webhook_is_missing_a_secret`] and the documented precedence:
    /// the sink wins, no `ReqwestAuditSink` is ever constructed, and no request
    /// can reach the URL. Failing startup on a stale or non-allowlisted URL
    /// that nothing will call is a false positive (issue #953, Codex review
    /// P2) — the two webhook checks must agree about when a webhook is live,
    /// or a config that skips one still trips the other.
    ///
    /// # Errors
    /// Returns the `(url, rejection)` pair when the webhook URL fails
    /// validation.
    pub fn validate_webhook_url(
        &self,
    ) -> Result<(), (String, crate::completion_callback::SsrfRejection)> {
        if self.sink.is_some() {
            return Ok(());
        }
        let Some(url) = &self.webhook_url else {
            return Ok(());
        };
        let policy = self.ssrf_policy();
        crate::completion_callback::validate_target_url(url, &policy)
            .map(|_| ())
            // The REDACTED origin, never the full URL (issue #953, Codex
            // review round 24 P1). This pair becomes
            // `HarvestBuilderError::AuditSinkRejected`, whose `Display` a
            // startup failure writes straight to the logs -- and a SIEM ingest
            // URL carries its credential in the path or query. Redacting here
            // rather than at the construction site means the full URL cannot
            // escape through this result at all.
            //
            // Nothing diagnostic is lost: every `SsrfRejection` variant
            // discriminates on an ORIGIN property -- scheme, host, port, IP
            // literal, userinfo -- so the origin is exactly what explains the
            // rejection, and the part that is dropped is exactly the part that
            // is a secret.
            .map_err(|rejection| (redact_webhook_url(url), rejection))
    }

    /// The claim lease, floored at one second.
    ///
    /// A zero lease would make every claim immediately reclaimable and give
    /// the sink call a zero timeout, so every batch would fail before it was
    /// sent.
    #[must_use]
    pub fn effective_lease(&self) -> std::time::Duration {
        self.lease.max(std::time::Duration::from_secs(1))
    }

    /// Batch size clamped into the supported range.
    #[must_use]
    pub const fn effective_batch_size(&self) -> i64 {
        if self.batch_size < 1 {
            1
        } else if self.batch_size > MAX_EXPORT_BATCH_SIZE {
            MAX_EXPORT_BATCH_SIZE
        } else {
            self.batch_size
        }
    }
}

// ---------------------------------------------------------------------
// M4: process-global runtime config (opt-in; `None` == fully inert)
// ---------------------------------------------------------------------

/// Bound on acquiring a shard's connection inside the scanner (issue #953,
/// Codex review P1).
///
/// `pool.get()` is an **unbounded** wait — Harvest configures no deadpool
/// `Timeouts`. The scanner is already holding a connection when it runs (the
/// timeout checker checks one out before calling `enforce_timeouts_once`), and
/// for a per-shard checker that connection comes from the very pool this
/// function then asks for a second one. On a one-connection pool that is a
/// self-deadlock: the wait never returns, audit export never runs, and every
/// scanner resident sequenced after it is wedged with it.
///
/// Bounding converts the indefinite park into "skip this shard for one tick",
/// which is visible in the log and self-heals, mirroring
/// `worker::shard_acquire_bound` (added for the same class of bug in #961).
/// Deliberately generous: the bound exists to stop an *indefinite* block, not
/// to police normal contention on a busy pool.
pub const SHARD_ACQUIRE_BOUND: std::time::Duration = std::time::Duration::from_secs(5);

/// Default lease held on a shard's cursor while a batch is in flight.
///
/// Long enough to cover a slow sink, short enough that a crashed exporter's
/// shard resumes promptly. A lease expiring early is safe — it costs a
/// duplicate delivery, which the receiver dedupes on `(shard, seq)` — while a
/// lease expiring late costs export lag.
///
/// **It also bounds the sink call itself.** The exporter wraps every
/// `deliver` in a timeout of exactly the configured lease, so a sink can
/// never outlive its own claim: without that, a sink slower than the lease
/// would livelock (each attempt superseded by the next claim, the cursor
/// never advancing) and would additionally wedge the shared background
/// scanner behind an embedder-supplied `await`.
pub const DEFAULT_EXPORT_LEASE: std::time::Duration = std::time::Duration::from_secs(60);

/// Everything the exporter needs at runtime, installed once at startup.
///
/// `None` (the default, before any builder wiring runs) means the feature is
/// fully inert: [`fire_due_audit_exports`] returns `Ok(0)` before issuing a
/// single query, so an embedder who never configures an audit sink sees zero
/// behavior change and zero scanner work (AC8).
#[derive(Clone)]
pub struct AuditExportRuntimeConfig {
    /// Embedder-supplied (or plugin-default) transport.
    pub sink: std::sync::Arc<dyn AuditSink>,
    /// HMAC key for `X-Harvest-Signature`.
    pub secret: CallbackSecret,
    /// Records claimed and delivered per batch, clamped to
    /// `[1, MAX_EXPORT_BATCH_SIZE]` at read time.
    pub batch_size: i64,
    /// Capped exponential backoff after a sink failure.
    pub backoff: ExportBackoff,
    /// How long a claim holds a shard's cursor.
    pub lease: std::time::Duration,
}

// `Arc`-wrapped for the same reason as `GLOBAL_CALLBACK_CONFIG` (issue #605
// review): every read clones the value out of the lock, and the struct carries
// owned fields that would otherwise be deep-copied on every scanner tick for a
// value that only ever changes at startup.
pub static GLOBAL_AUDIT_EXPORT_CONFIG: std::sync::RwLock<
    Option<std::sync::Arc<AuditExportRuntimeConfig>>,
> = std::sync::RwLock::new(None);

/// Read [`GLOBAL_AUDIT_EXPORT_CONFIG`], tolerating a poisoned lock.
///
/// Mirrors `completion_callback::read_global_callback_config`: a poisoned
/// `RwLock` means some other thread panicked while holding the write guard,
/// but the single `*lock = Some(..)` that write path performs is not a
/// multi-step invariant a panic could leave half-applied, so the data behind
/// the guard is still valid. Recovering it avoids the failure mode of a bare
/// `.read().ok()`, where one unrelated panic would silently stop exporting
/// audit records — a compliance gap — for the rest of the process's life.
fn read_global_audit_export_config() -> Option<std::sync::Arc<AuditExportRuntimeConfig>> {
    match GLOBAL_AUDIT_EXPORT_CONFIG.read() {
        Ok(guard) => guard.clone(),
        Err(poisoned) => {
            tracing::error!(
                "GLOBAL_AUDIT_EXPORT_CONFIG lock was poisoned by a panic elsewhere in the \
                 process; recovering the last-written config rather than treating it as \
                 unconfigured (which would silently stop audit export)"
            );
            poisoned.into_inner().clone()
        }
    }
}

/// Install [`GLOBAL_AUDIT_EXPORT_CONFIG`] for an embedder using the core
/// `HarvestBuilder::build()` -> `into_worker_parts()` path directly.
///
/// Mirrors
/// [`crate::completion_callback::install_global_callback_config_for_direct_worker`]
/// and exists for the same reason (issue #921 review): the plugin's runner is
/// the only installer this crate ships, so a direct core embedder would
/// otherwise get an `audit_export_*` builder API that silently did nothing —
/// and a silently-inert audit export is a compliance gap discovered at audit
/// time.
///
/// Only an **embedder-supplied [`AuditSink`]** can be installed here: core
/// ships no HTTP client, so a bare `audit_export_webhook(...)` has no
/// transport on this path. That case is logged rather than ignored, since the
/// embedder plainly intended export to happen.
///
/// **Clears any prior config when this runtime configures none**, for the
/// same reason as the callback installer: the config is a single process-wide
/// static, so a second runtime built without a sink must not keep shipping
/// audit records to the first runtime's destination.
pub fn install_global_audit_export_config_for_direct_worker(config: &AuditExportBuilderConfig) {
    let Some(sink) = config.sink.clone() else {
        if config.webhook_url.is_some() {
            tracing::warn!(
                "audit_export_webhook(...) was configured but this runtime was built \
                 through the direct core worker path, which ships no HTTP client -- no \
                 audit records will be exported. Supply audit_export_sink(...) with your \
                 own AuditSink, or run through autumn-harvest-plugin, which provides the \
                 default reqwest signed-webhook sink."
            );
        }
        if let Ok(mut lock) = GLOBAL_AUDIT_EXPORT_CONFIG.write() {
            *lock = None;
        }
        return;
    };
    let secret = config.secret.clone().unwrap_or_else(|| {
        tracing::warn!(
            "audit-export HMAC secret was never configured via \
             HarvestBuilder::audit_export_secret(...) -- every exported batch will be \
             signed with an empty key, which defeats the X-Harvest-Signature \
             tamper-evidence guarantee for any receiver relying on it"
        );
        CallbackSecret::new(Vec::new())
    });
    if let Ok(mut lock) = GLOBAL_AUDIT_EXPORT_CONFIG.write() {
        *lock = Some(std::sync::Arc::new(AuditExportRuntimeConfig {
            sink,
            secret,
            batch_size: config.effective_batch_size(),
            backoff: config.backoff.clone(),
            lease: config.effective_lease(),
        }));
    }
}

/// `true` when an audit sink is configured in this process.
///
/// Two callers, and both matter:
/// - [`crate::audit::purge_old_audit_records`] uses it to decide whether the
///   "never purge an unexported record" guard is live. The guard cannot be
///   inferred from the cursor table alone: an empty table means *either*
///   "export was never configured" (purge freely) *or* "export is configured
///   but has not ticked on this shard yet" (purge nothing), and a stale
///   cursor row left behind by a since-disabled exporter would otherwise
///   block retention forever.
/// - `GET /admin/audit-export` reports it as `sink_configured`, so an
///   operator can see export configured on the web app but not on the worker
///   fleet (or vice versa).
#[must_use]
pub fn is_configured() -> bool {
    read_global_audit_export_config().is_some()
}

// ---------------------------------------------------------------------
// M5: cursor + claim (transaction 1 — no network I/O, no lock held across it)
// ---------------------------------------------------------------------

/// What a successful claim hands back to the delivery step.
#[cfg(feature = "db")]
#[derive(Debug, Clone)]
pub struct ClaimedBatch {
    /// The shard this batch belongs to.
    pub shard: i32,
    /// The epoch the post-delivery write must be guarded on.
    pub claim_epoch: i64,
    /// Failure count before this attempt, threaded into
    /// [`classify_export_outcome`].
    pub consecutive_failures: i32,
    /// Records to deliver, ascending by `seq`. Never empty — a claim with
    /// nothing to send is released instead.
    pub records: Vec<AuditExportRecord>,
    /// When this claim's database lease expires — the same `lease_until`
    /// written to the cursor row inside the claim transaction.
    ///
    /// Delivery is bounded by what **remains** of this, not by the full lease
    /// (issue #953, Codex review round 19 P1). The lease starts running when
    /// the claim commits; serializing the batch and reaching the sink consumes
    /// some of it. Bounding the sink call by the whole lease therefore lets a
    /// delivery outlive the row lease, at which point another exporter can
    /// reclaim the shard and bump the epoch, so this attempt's acknowledgement
    /// is refused. With sink latency consistently near the lease that repeats
    /// indefinitely: the cursor never advances and the sink is handed the same
    /// batch over and over.
    pub lease_until: DateTime<Utc>,
}

/// Create this shard's cursor row if it does not exist, and stamp it as alive.
///
/// Deliberately not seeded by the migration: a shard's database cannot know
/// its own shard id (see the migration's comment, and
/// `replication::ensure_generation_row` for the same pattern in #954).
///
/// **`updated_at` doubles as an exporter heartbeat** (issue #953, Codex review
/// P1). Called on every scanner tick, including ticks that claim nothing, so a
/// fresh `updated_at` means "an exporter is running against this shard right
/// now" — durable, shared state that a *different process* can read. That is
/// what lets [`crate::audit::purge_old_audit_records`] protect unexported rows
/// in a split web/worker deployment, where the process running retention may
/// have no sink configured and so cannot answer the question locally.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
pub async fn ensure_cursor_row(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: i32,
) -> crate::error::HarvestResult<()> {
    use diesel_async::RunQueryDsl;

    // Two defences for the sequence high-water mark, because re-issuing a
    // `(shard, seq)` pair that names a *different* record is the one way to
    // make a receiver deduping on that pair discard genuine audit events.
    //
    // 1. The row is retired rather than deleted (`decommission_cursor`), so
    //    `last_assigned_seq` survives even when retention later purges every
    //    stamped row (issue #953, Codex review round 7 P1). The ON CONFLICT
    //    arm therefore only clears `retired_at` -- re-enabling export resumes
    //    the preserved counter, and never resets it.
    // 2. The INSERT arm still seeds from `MAX(export_seq)` rather than 0, for
    //    the paths where the row genuinely went missing anyway: a manual
    //    DELETE, a partial restore (round 4 P1). Restarting at 0 there would
    //    also violate the cursor's `last_acked_seq <= last_assigned_seq` CHECK
    //    the moment a retained batch was acknowledged.
    //
    // On a fresh INSERT `last_acked_seq` stays 0, so a cursor rebuilt from
    // stamped rows re-delivers them rather than assuming they shipped:
    // at-least-once, deduped by the receiver on a now-stable pair, erring
    // toward re-export over silent loss.
    diesel::sql_query(
        "INSERT INTO harvest_audit_export_cursor (shard_id, last_assigned_seq) \
         SELECT $1, COALESCE(MAX(export_seq), 0) FROM harvest_audit_log \
         ON CONFLICT (shard_id) DO UPDATE SET updated_at = NOW(), retired_at = NULL",
    )
    .bind::<diesel::sql_types::Integer, _>(shard_id)
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;
    Ok(())
}

/// Remove a shard's export cursor, re-enabling audit retention there.
///
/// The **explicit decommission step** for audit export (issue #953, Codex
/// review round 3 P1). While a cursor row exists,
/// [`crate::audit::purge_old_audit_records`] will not delete a record that
/// shard still owes the sink — the row's presence, not a timeout, is what says
/// "an exporter is responsible for this shard".
///
/// This deliberately replaced a 24-hour heartbeat TTL. A TTL cannot tell
/// "export was intentionally removed" from "the worker has been down since
/// Friday", and it resolves that ambiguity by *deleting audit records* — the
/// one outcome this feature exists to prevent, arriving precisely during an
/// outage. Unbounded table growth is the strictly better failure: it is loud
/// (`harvest.audit.export_lag`, the `last_error` on
/// `GET /admin/audit-export`), it is bounded by the genuine unexported
/// backlog rather than by the whole table (fully-acknowledged records are
/// purged normally), and it is *reversible* — deleted audit records are not.
///
/// So retiring an export configuration is an operator action, not an inferred
/// one. See `docs/audit-export.md`.
///
/// The row is **retired, never deleted** (issue #953, Codex review round 7 P1).
/// `last_assigned_seq` has to outlive the audit rows themselves, and retiring
/// the cursor is precisely what lets retention purge them: seeding a recreated
/// cursor from `MAX(export_seq)` preserves the counter only while at least one
/// stamped row survives, so a decommission followed by a full purge would reset
/// it to 0 and re-issue `(shard, seq)` pairs the SIEM still holds against
/// different records. Keeping the row makes the high-water mark durable
/// independently of retention.
///
/// Safe to reverse: the next [`ensure_cursor_row`] un-retires the row and
/// resumes from the preserved `last_assigned_seq`, so re-enabling export
/// continues the sequence. Records purged while retired are gone and are not
/// re-delivered — `last_acked_seq` is preserved too.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
pub async fn decommission_cursor(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: i32,
) -> crate::error::HarvestResult<bool> {
    use diesel_async::RunQueryDsl;

    // Bumping `claim_epoch` invalidates any delivery still in flight (issue
    // #953, Codex review round 13 P2). `apply_outcome` is guarded on
    // `(shard_id, claim_epoch)` alone, so without this an attempt claimed
    // before the retirement could land after it -- advancing the cursor or
    // writing backoff state onto a row the status route now reports as a
    // frozen `RETIRED` snapshot, and racing retention, which is permitted to
    // purge the shard the moment it is retired. This is exactly what the epoch
    // is for: it already exists so a slow attempt whose HTTP call outlives its
    // lease cannot apply a stale outcome over a fresher one. Clearing
    // `lease_until` in the same statement means the row does not also read as
    // mid-delivery.
    let retired = diesel::sql_query(
        "UPDATE harvest_audit_export_cursor \
         SET retired_at = NOW(), updated_at = NOW(), \
             claim_epoch = claim_epoch + 1, lease_until = NULL \
         WHERE shard_id = $1 AND retired_at IS NULL",
    )
    .bind::<diesel::sql_types::Integer, _>(shard_id)
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;
    Ok(retired > 0)
}

/// The `MAX(export_seq)` read back from the sequence-assignment statement —
/// the high-water mark of sequences actually handed out.
#[cfg(feature = "db")]
#[derive(diesel::QueryableByName)]
struct HighWater {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    high_water: i64,
}

/// Claim a shard for export: assign sequences to newly-visible audit rows and
/// load the next batch, all under the cursor row's lock.
///
/// This is transaction 1 of the two-transaction shape (#605): it takes the
/// cursor row lock, does only local work, and commits **before** any network
/// I/O happens. No row lock is ever held across a sink call.
///
/// Returns `Ok(None)` when the shard is not due (backing off), is already
/// leased by another exporter, or has nothing to deliver.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
#[allow(clippy::too_many_lines)] // claim + assign + load is one atomic unit
pub async fn claim_shard(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: i32,
    batch_size: i64,
    lease: std::time::Duration,
    now: DateTime<Utc>,
) -> crate::error::HarvestResult<Option<ClaimedBatch>> {
    use diesel::prelude::*;
    use diesel_async::AsyncConnection;
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_audit_export_cursor::dsl as cur;
    use crate::schema::harvest_audit_log::dsl as log;

    let batch_size = batch_size.clamp(1, MAX_EXPORT_BATCH_SIZE);
    let lease_until =
        now + chrono::Duration::from_std(lease).unwrap_or_else(|_| chrono::Duration::seconds(60));

    Box::pin(
        conn.transaction::<Option<ClaimedBatch>, crate::error::HarvestError, _>(async |conn| {
            let cursor: Option<crate::models::AuditExportCursor> = cur::harvest_audit_export_cursor
                .find(shard_id)
                .select(crate::models::AuditExportCursor::as_select())
                .for_update()
                .first(conn)
                .await
                .optional()
                .map_err(crate::error::database_error)?;

            let Some(cursor) = cursor else {
                return Ok(None);
            };

            // A retired cursor is inert (issue #953, Codex review round 14 P2).
            // `export_once_on_conn` calls `ensure_cursor_row` first, which
            // un-retires, so in the ordinary flow a running exporter never sees
            // this. The window is a `decommission_cursor` that commits between
            // that call and this locked read: without the check the scanner
            // takes a NEW claim and delivers a batch after the retirement.
            // Bumping the epoch on retirement only invalidates claims taken
            // *before* it, so this is the other half of that fix -- and it
            // matters because retention is permitted to purge the shard's
            // records the moment it is retired.
            if cursor.retired_at.is_some() {
                return Ok(None);
            }

            // Backing off after a sink failure, or another exporter (or this
            // one, on a previous tick whose HTTP call has not returned) holds
            // a live lease. Either way this shard is not ours right now.
            if cursor.next_attempt_at > now {
                return Ok(None);
            }
            if cursor.lease_until.is_some_and(|until| until > now) {
                return Ok(None);
            }

            // ── Assign sequences to rows that are now visible ──────────────
            //
            // Ordered by `(occurred_at, id)` purely for a stable, index-backed
            // scan; correctness does NOT depend on that order matching commit
            // order, because a row that commits later is still `NULL` here and
            // simply receives a later sequence on a later tick. That is the
            // whole reason the sequence is stamped by the exporter rather than
            // handed out by a `BIGSERIAL` before commit — see the module docs.
            // The high-water mark is read back from the rows actually
            // stamped (`MAX(export_seq)`), never inferred from the affected
            // row count (issue #953 review): the two diverge if any row in
            // the CTE's snapshot disappears before the UPDATE takes its
            // lock, and a high-water mark one short of the highest sequence
            // handed out would let the NEXT tick reissue that sequence to a
            // different record -- two audit records sharing a `(shard, seq)`,
            // which a receiver deduping on that key silently drops.
            //
            // The outer `AND a.export_seq IS NULL` repeats the CTE's
            // predicate under the row lock for the same reason: without it
            // the UPDATE would overwrite a sequence another writer assigned
            // between the CTE's evaluation and the lock.
            let assigned: HighWater = diesel::sql_query(
                "WITH claimed AS ( \
                     SELECT id, row_number() OVER (ORDER BY occurred_at, id) AS rn \
                     FROM harvest_audit_log \
                     WHERE export_seq IS NULL \
                     ORDER BY occurred_at, id \
                     LIMIT $2 \
                 ), stamped AS ( \
                     UPDATE harvest_audit_log a \
                     SET export_seq = $1 + claimed.rn \
                     FROM claimed \
                     WHERE a.id = claimed.id AND a.export_seq IS NULL \
                     RETURNING a.export_seq \
                 ) \
                 SELECT COALESCE(MAX(export_seq), $1) AS high_water FROM stamped",
            )
            .bind::<diesel::sql_types::BigInt, _>(cursor.last_assigned_seq)
            .bind::<diesel::sql_types::BigInt, _>(batch_size)
            .get_result(conn)
            .await
            .map_err(crate::error::database_error)?;

            let last_assigned_seq = assigned.high_water.max(cursor.last_assigned_seq);

            // ── Load the batch to deliver ─────────────────────────────────
            //
            // Everything above the cursor, not merely what was just assigned:
            // a retry after a failed delivery, and a redrive that rewound the
            // cursor, both re-send already-sequenced records. Reading by
            // `export_seq` (never re-stamping) is what makes a re-export
            // byte-identical (AC6).
            let rows: Vec<crate::models::AuditExportRow> = log::harvest_audit_log
                .filter(log::export_seq.gt(cursor.last_acked_seq))
                .select(crate::models::AuditExportRow::as_select())
                .order(log::export_seq.asc())
                .limit(batch_size)
                .load(conn)
                .await
                .map_err(crate::error::database_error)?;

            if last_assigned_seq != cursor.last_assigned_seq {
                diesel::update(cur::harvest_audit_export_cursor.find(shard_id))
                    .set((
                        cur::last_assigned_seq.eq(last_assigned_seq),
                        cur::updated_at.eq(now),
                    ))
                    .execute(conn)
                    .await
                    .map_err(crate::error::database_error)?;
            }

            if rows.is_empty() {
                return Ok(None);
            }

            let claim_epoch = cursor.claim_epoch + 1;
            diesel::update(cur::harvest_audit_export_cursor.find(shard_id))
                .set((
                    cur::claim_epoch.eq(claim_epoch),
                    cur::lease_until.eq(Some(lease_until)),
                    cur::updated_at.eq(now),
                ))
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;

            let records = rows
                .into_iter()
                .filter_map(|row| {
                    // `export_seq` is non-NULL by the filter above; a row
                    // without one is skipped rather than exported with a
                    // fabricated sequence, which would corrupt the receiver's
                    // gap accounting.
                    row.export_seq.map(|seq| AuditExportRecord {
                        shard: shard_id,
                        seq,
                        id: row.id,
                        shard_id: row.shard_id,
                        occurred_at: row.occurred_at,
                        actor: row.actor,
                        operation: row.operation,
                        target_type: row.target_type,
                        target_id: row.target_id,
                        route_or_command: row.route_or_command,
                        request_id: row.request_id,
                        idempotency_key: row.idempotency_key,
                        status: row.status,
                        error_summary: row.error_summary,
                        source: row.source,
                    })
                })
                .collect::<Vec<_>>();

            if records.is_empty() {
                return Ok(None);
            }

            Ok(Some(ClaimedBatch {
                shard: shard_id,
                claim_epoch,
                consecutive_failures: cursor.consecutive_failures,
                records,
                lease_until,
            }))
        }),
    )
    .await
}

// ---------------------------------------------------------------------
// M6: apply the outcome (transaction 2)
// ---------------------------------------------------------------------

/// Apply a delivery outcome to a shard's cursor.
///
/// Every write is guarded on `claim_epoch = $epoch`, so an attempt whose sink
/// call outlived its lease — and whose batch a later claim has already
/// re-delivered — can never apply a stale outcome over the fresher one, and a
/// redrive that ran mid-flight (which bumps the epoch) can never be silently
/// undone by the in-flight batch's acknowledgement.
///
/// Returns `true` when the guarded write applied.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
pub async fn apply_outcome(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: i32,
    claim_epoch: i64,
    outcome: &ExportOutcome,
    now: DateTime<Utc>,
) -> crate::error::HarvestResult<bool> {
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_audit_export_cursor::dsl as cur;

    let target = cur::harvest_audit_export_cursor
        .find(shard_id)
        .filter(cur::claim_epoch.eq(claim_epoch));

    let updated = match outcome {
        ExportOutcome::Advance {
            through_seq,
            status,
        } => {
            // Plain assignment, not `GREATEST(...)`: the epoch guard already
            // rules out a stale attempt writing here, and a `GREATEST` would
            // defeat a legitimate redrive by re-raising a cursor an operator
            // deliberately rewound.
            diesel::update(target)
                .set((
                    cur::last_acked_seq.eq(*through_seq),
                    cur::lease_until.eq(None::<DateTime<Utc>>),
                    cur::next_attempt_at.eq(now),
                    cur::consecutive_failures.eq(0),
                    cur::last_status.eq(Some(i32::from(*status))),
                    cur::last_error.eq(None::<String>),
                    cur::last_delivered_at.eq(Some(now)),
                    cur::updated_at.eq(now),
                ))
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?
        }
        ExportOutcome::Backoff {
            next_attempt_at,
            last_status,
            last_error,
            consecutive_failures,
        } => {
            // `last_acked_seq` is deliberately untouched: a failed delivery
            // never advances the cursor, so the same records are re-sent on
            // the next attempt. This is the AC2 "never advances past the
            // failure" invariant, enforced by simply not writing the column.
            diesel::update(target)
                .set((
                    cur::lease_until.eq(None::<DateTime<Utc>>),
                    cur::next_attempt_at.eq(*next_attempt_at),
                    cur::consecutive_failures.eq(*consecutive_failures),
                    cur::last_status.eq(last_status.map(i32::from)),
                    cur::last_error.eq(last_error.clone()),
                    cur::updated_at.eq(now),
                ))
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?
        }
    };

    Ok(updated > 0)
}

/// Drop a claim without changing the cursor (nothing was delivered).
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
pub async fn release_claim(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: i32,
    claim_epoch: i64,
    now: DateTime<Utc>,
) -> crate::error::HarvestResult<()> {
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_audit_export_cursor::dsl as cur;

    diesel::update(
        cur::harvest_audit_export_cursor
            .find(shard_id)
            .filter(cur::claim_epoch.eq(claim_epoch)),
    )
    .set((
        cur::lease_until.eq(None::<DateTime<Utc>>),
        cur::updated_at.eq(now),
    ))
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;
    Ok(())
}

// ---------------------------------------------------------------------
// M7: observability primitives
// ---------------------------------------------------------------------

/// Delivery state reported by `GET /admin/audit-export`.
///
/// Derived from the cursor row rather than stored, so it can never disagree
/// with the columns it summarizes.
#[must_use]
pub fn delivery_state(
    lease_until: Option<DateTime<Utc>>,
    consecutive_failures: i32,
    next_attempt_at: DateTime<Utc>,
    now: DateTime<Utc>,
    retired_at: Option<DateTime<Utc>>,
) -> &'static str {
    // A retired cursor's other columns are a frozen snapshot of whatever the
    // exporter last did (issue #953, Codex review round 9 P2). Deriving IDLE,
    // BACKOFF or RETRYING from them would tell an operator that records are
    // pending delivery when no exporter owes them and retention is free to
    // purge them. `RETIRED` is checked first because it overrides every other
    // reading of those columns.
    if retired_at.is_some() {
        return "RETIRED";
    }
    if lease_until.is_some_and(|until| until > now) {
        return "DELIVERING";
    }
    if consecutive_failures > 0 && next_attempt_at > now {
        return "BACKOFF";
    }
    if consecutive_failures > 0 {
        return "RETRYING";
    }
    "IDLE"
}

/// One shard's export status.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuditExportShardStatus {
    pub shard: i32,
    /// The cursor: every record at or below this sequence has been
    /// acknowledged by the sink at least once.
    pub cursor_seq: i64,
    /// High-water mark of sequences handed out.
    pub last_assigned_seq: i64,
    /// Records not yet acknowledged (assigned-but-unacked plus not-yet-assigned).
    pub pending_records: i64,
    /// Age in seconds of the oldest record not yet acknowledged; `0.0` when
    /// nothing is pending. This is `harvest.audit.export_lag`.
    pub lag_seconds: f64,
    /// `IDLE` | `DELIVERING` | `BACKOFF` | `RETRYING` | `RETIRED`, from
    /// [`delivery_state`]. `GET /admin/audit-export` additionally synthesizes
    /// `NOT_STARTED` for a shard with no cursor row at all, which this struct
    /// cannot represent (there is no cursor to describe).
    ///
    /// `RETIRED` means an operator ran `decommission_cursor`: the row survives
    /// only to preserve the sequence high-water mark, no exporter owes this
    /// shard records, and retention may purge them. The remaining fields are a
    /// frozen snapshot of the last export activity, not a live one.
    pub delivery_state: String,
    pub consecutive_failures: i32,
    pub last_status: Option<i32>,
    pub last_error: Option<String>,
    pub last_delivered_at: Option<DateTime<Utc>>,
    pub next_attempt_at: DateTime<Utc>,
}

/// Age in seconds of the oldest audit record the sink has not acknowledged,
/// or `0.0` when nothing is pending. This is `harvest.audit.export_lag`.
///
/// Deliberately **two index-servable point lookups** rather than the single
/// `MIN(occurred_at) WHERE export_seq IS NULL OR export_seq > $1` the admin
/// view uses (issue #953 review): that disjunction spans both partial indexes
/// and degenerates into visiting every pending heap tuple. This runs on
/// **every** scanner tick, and the pending set is largest during exactly the
/// sink outage when the database is already under stress — so the per-tick
/// query must not scale with the backlog.
///
/// - Not-yet-sequenced rows: `MIN(occurred_at) WHERE export_seq IS NULL`,
///   served as an index-min by `harvest_audit_log_unexported_idx`.
/// - Sequenced-but-unacknowledged rows: the `occurred_at` of the *next*
///   record the exporter owes the sink, found by `ORDER BY export_seq LIMIT 1`
///   on `harvest_audit_log_export_seq_idx`. Sequences are assigned in
///   `(occurred_at, id)` order within a batch, so this is the oldest such
///   record in every ordinary case, and it is the operationally meaningful
///   one — "how old is the next thing we owe the sink?" — in any case.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
#[allow(clippy::cast_precision_loss)] // millisecond lag never approaches 2^53
pub async fn export_lag_seconds(
    conn: &mut diesel_async::AsyncPgConnection,
    last_acked_seq: i64,
    now: DateTime<Utc>,
) -> crate::error::HarvestResult<f64> {
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_audit_log::dsl as log;

    let unsequenced: Option<DateTime<Utc>> = log::harvest_audit_log
        .filter(log::export_seq.is_null())
        .select(diesel::dsl::min(log::occurred_at))
        .first::<Option<DateTime<Utc>>>(conn)
        .await
        .map_err(crate::error::database_error)?;

    let next_owed: Option<DateTime<Utc>> = log::harvest_audit_log
        .filter(log::export_seq.gt(last_acked_seq))
        .select(log::occurred_at)
        .order(log::export_seq.asc())
        .first::<DateTime<Utc>>(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?;

    let oldest = match (unsequenced, next_owed) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) | (None, Some(a)) => Some(a),
        (None, None) => None,
    };

    Ok(oldest.map_or(0.0, |oldest| {
        let secs = (now - oldest).num_milliseconds() as f64 / 1000.0;
        if secs.is_finite() && secs > 0.0 {
            secs
        } else {
            0.0
        }
    }))
}

/// Pending-record count and lag for one shard.
///
/// "Pending" is `export_seq IS NULL OR export_seq > last_acked_seq` — both a
/// record the exporter has not sequenced yet and one it sequenced but has not
/// had acknowledged are equally undelivered.
///
/// The `COUNT(*)` makes this **O(backlog)**, so it is deliberately confined to
/// the operator-triggered `GET /admin/audit-export` read. The scanner's
/// per-tick gauge uses [`export_lag_seconds`], which is not.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
#[allow(clippy::cast_precision_loss)] // millisecond lag never approaches 2^53
pub async fn pending_and_lag(
    conn: &mut diesel_async::AsyncPgConnection,
    last_acked_seq: i64,
    now: DateTime<Utc>,
) -> crate::error::HarvestResult<(i64, f64)> {
    use diesel_async::RunQueryDsl;

    #[derive(diesel::QueryableByName)]
    struct PendingRow {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        pending: i64,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
        oldest: Option<DateTime<Utc>>,
    }

    let row: PendingRow = diesel::sql_query(
        "SELECT COUNT(*)::BIGINT AS pending, MIN(occurred_at) AS oldest \
         FROM harvest_audit_log \
         WHERE export_seq IS NULL OR export_seq > $1",
    )
    .bind::<diesel::sql_types::BigInt, _>(last_acked_seq)
    .get_result(conn)
    .await
    .map_err(crate::error::database_error)?;

    let lag = row.oldest.map_or(0.0, |oldest| {
        let secs = (now - oldest).num_milliseconds() as f64 / 1000.0;
        if secs.is_finite() && secs > 0.0 {
            secs
        } else {
            0.0
        }
    });
    Ok((row.pending, lag))
}

/// Read one shard's export status.
///
/// Returns `Ok(None)` when this shard has no cursor row — audit export has
/// never run here.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
pub async fn export_status(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: i32,
    now: DateTime<Utc>,
) -> crate::error::HarvestResult<Option<AuditExportShardStatus>> {
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_audit_export_cursor::dsl as cur;

    let cursor: Option<crate::models::AuditExportCursor> = cur::harvest_audit_export_cursor
        .find(shard_id)
        .select(crate::models::AuditExportCursor::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?;

    let Some(cursor) = cursor else {
        return Ok(None);
    };

    // A retired cursor owes nothing, so there is no backlog to report (issue
    // #953, Codex review round 10 P2). Recomputing it from the live audit table
    // would keep `pending_records` and `lag_seconds` climbing for a shard no
    // exporter is responsible for -- firing backlog alerts that no action can
    // clear, and contradicting the contract's promise that a retired entry's
    // fields are a frozen snapshot. Skipping the query is also the honest
    // answer to what the fields mean: how much the exporter still owes.
    let (pending_records, lag_seconds) = if cursor.retired_at.is_some() {
        (0, 0.0)
    } else {
        pending_and_lag(conn, cursor.last_acked_seq, now).await?
    };

    Ok(Some(AuditExportShardStatus {
        shard: shard_id,
        cursor_seq: cursor.last_acked_seq,
        last_assigned_seq: cursor.last_assigned_seq,
        pending_records,
        lag_seconds,
        delivery_state: delivery_state(
            cursor.lease_until,
            cursor.consecutive_failures,
            cursor.next_attempt_at,
            now,
            cursor.retired_at,
        )
        .to_string(),
        consecutive_failures: cursor.consecutive_failures,
        last_status: cursor.last_status,
        last_error: cursor.last_error,
        last_delivered_at: cursor.last_delivered_at,
        next_attempt_at: cursor.next_attempt_at,
    }))
}

// ---------------------------------------------------------------------
// M8: redrive
// ---------------------------------------------------------------------

/// What an operator asked the cursor to be rewound to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RewindRequest {
    /// Rewind to an explicit sequence: records with `seq > n` re-export.
    Seq(i64),
    /// Rewind so that every record that occurred at or after this instant
    /// re-exports.
    Before(DateTime<Utc>),
}

/// Rewind a shard's export cursor so already-delivered records re-export
/// (AC6), after sink-side data loss.
///
/// Re-exported records are **byte-identical**: `export_seq` is never
/// re-stamped, so a record carries the same `(shard, seq)` and the same JSON
/// on every delivery, and the receiver dedupes.
///
/// The cursor may only ever move **backwards** — see [`resolve_rewind`].
/// Bumps `claim_epoch` and clears the lease, so a delivery already in flight
/// when the rewind lands cannot acknowledge over it.
///
/// Opens its own transaction. A caller that must commit the rewind together
/// with something else — the management route pairs it with its own audit
/// record, so a rewind can never be applied unaudited — should open the
/// transaction itself and call [`rewind_cursor_locked`].
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
pub async fn rewind_cursor(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: i32,
    request: RewindRequest,
    now: DateTime<Utc>,
) -> crate::error::HarvestResult<RewindOutcome> {
    use diesel_async::AsyncConnection;

    Box::pin(
        conn.transaction::<RewindOutcome, crate::error::HarvestError, _>(async |conn| {
            rewind_cursor_locked(conn, shard_id, request, now).await
        }),
    )
    .await
}

/// [`rewind_cursor`] without the surrounding transaction.
///
/// Takes the cursor row's `FOR UPDATE` lock and applies the rewind but leaves
/// the commit to the caller, so a caller can bind the rewind to another write
/// in the same unit. The management route uses this to write its
/// `audit_export.redrive` record *before* committing, which makes an applied
/// but unaudited redrive unrepresentable rather than merely unlikely.
///
/// **Must be called inside a transaction.** On an autocommit connection each
/// statement commits independently and the atomicity this exists to provide is
/// gone.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
pub async fn rewind_cursor_locked(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: i32,
    request: RewindRequest,
    now: DateTime<Utc>,
) -> crate::error::HarvestResult<RewindOutcome> {
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_audit_export_cursor::dsl as cur;
    use crate::schema::harvest_audit_log::dsl as log;

    {
        {
            let cursor: Option<crate::models::AuditExportCursor> = cur::harvest_audit_export_cursor
                .find(shard_id)
                .select(crate::models::AuditExportCursor::as_select())
                .for_update()
                .first(conn)
                .await
                .optional()
                .map_err(crate::error::database_error)?;

            // A *retired* cursor is not a rewindable one (issue #953, Codex
            // review round 8 P2). The row now outlives a decommission so its
            // sequence high-water mark survives — but retention deliberately
            // ignores retired cursors and is free to purge the very records a
            // rewind would target, and no exporter is running to ship them.
            // Answering 200 "rewound" there is a promise the system cannot
            // keep, so a retired shard is reported exactly as an unconfigured
            // one was before the row began to persist.
            let Some(cursor) = cursor.filter(|c| c.retired_at.is_none()) else {
                return Ok(RewindOutcome::NotConfigured);
            };

            let requested = match request {
                RewindRequest::Seq(seq) => seq,
                RewindRequest::Before(instant) => {
                    // The LOWEST sequence among records that occurred at or
                    // after `instant`, minus one -- never `MAX(seq)` over the
                    // records *before* it (issue #953 review).
                    //
                    // The two are not equivalent, and the difference is the
                    // same hazard this whole design is built around.
                    // `occurred_at` is transaction START time, so a
                    // long-running request's audit row can carry an earlier
                    // `occurred_at` than a row that committed sooner and yet
                    // be sequenced later. Taking the max over the "before"
                    // side would then land ABOVE records the operator asked to
                    // re-export, and they would silently never be re-sent --
                    // the exact skew the forward path is immune to,
                    // reintroduced in the recovery path.
                    //
                    // Anchoring on the "at or after" side is monotone in the
                    // safe direction: any skew makes the rewind reach FURTHER
                    // back, costing duplicate deliveries the receiver dedupes
                    // on `(shard, seq)`, never a missing record. No sequenced
                    // record at or after the instant means there is nothing to
                    // re-export from there, so the cursor stays put.
                    let lowest: Option<Option<i64>> = log::harvest_audit_log
                        .filter(log::occurred_at.ge(instant))
                        .filter(log::export_seq.is_not_null())
                        .select(diesel::dsl::min(log::export_seq))
                        .first(conn)
                        .await
                        .optional()
                        .map_err(crate::error::database_error)?;
                    lowest
                        .flatten()
                        .map_or(cursor.last_acked_seq, |seq| seq.saturating_sub(1))
                }
            };

            let outcome = resolve_rewind(cursor.last_acked_seq, requested);
            if let RewindOutcome::Rewound { to, .. } = outcome {
                diesel::update(cur::harvest_audit_export_cursor.find(shard_id))
                    .set((
                        cur::last_acked_seq.eq(to),
                        // Invalidate any in-flight delivery's acknowledgement.
                        cur::claim_epoch.eq(cursor.claim_epoch + 1),
                        cur::lease_until.eq(None::<DateTime<Utc>>),
                        // Re-export immediately rather than serving out a backoff
                        // that belonged to a since-resolved sink failure.
                        cur::next_attempt_at.eq(now),
                        cur::consecutive_failures.eq(0),
                        cur::last_error.eq(None::<String>),
                        cur::updated_at.eq(now),
                    ))
                    .execute(conn)
                    .await
                    .map_err(crate::error::database_error)?;
            }
            Ok(outcome)
        }
    }
}

// ---------------------------------------------------------------------
// M9: the scanner
// ---------------------------------------------------------------------

/// Deliver `batch`, bounded by what remains of this claim's database lease.
///
/// Never the full `config.lease` (issue #953, Codex review round 19 P1). The
/// lease starts running when the claim commits, and claiming, serializing and
/// signing the batch have already consumed part of it. Bounding by the whole
/// lease would let the call outlive the row lease, after which another exporter
/// can reclaim the shard and bump the epoch — so this attempt's acknowledgement
/// is refused and the batch is redelivered. At sink latencies consistently near
/// the lease that never converges: the cursor stays put while the sink receives
/// the same batch forever.
///
/// An already-elapsed remainder is not skipped, it is an immediate timeout —
/// classified like any other transport failure, so the cursor is held and the
/// batch retried under a fresh claim. Never a loss.
#[cfg(feature = "db")]
async fn deliver_within_lease(
    config: &AuditExportRuntimeConfig,
    batch: &AuditBatch<'_>,
    lease_until: DateTime<Utc>,
) -> SinkAttempt {
    let remaining = (lease_until - Utc::now())
        .to_std()
        .unwrap_or(std::time::Duration::ZERO);
    tokio::time::timeout(remaining, config.sink.deliver(batch))
        .await
        .unwrap_or_else(|_| {
            SinkAttempt::transport_error(format!(
                "audit sink did not respond within the {}s remaining on this claim's {}s \
                 export lease",
                remaining.as_secs(),
                config.lease.as_secs()
            ))
        })
}

/// Export one batch for one shard on one connection.
///
/// Three phases, deliberately separated: claim (transaction 1), deliver (no
/// transaction, no locks), apply (transaction 2). Returns the number of
/// records delivered.
/// Discard a delivery whose sink was replaced while it was in flight.
///
/// `read_global_audit_export_config` clones the `Arc` at the top of a tick, so
/// a second runtime publishing a different sink does not disturb that clone and
/// the batch reaches the OLD destination (issue #953, Codex review round 25
/// P1). `apply_outcome` is guarded on `claim_epoch`, which a config swap does
/// not change, so a 2xx from the old sink would advance the cursor: the records
/// would be delivered, marked delivered, and absent from the SIEM the operator
/// actually configured.
///
/// Comparing the `Arc` this attempt used against the one now installed closes
/// that window. A mismatch becomes a transport failure, so the cursor is held
/// and the batch is redelivered under the new sink. The old sink keeps its
/// copy — that is the at-least-once contract, not a leak.
///
/// A `current` of `None` also counts as swapped: export was turned off
/// mid-flight, and advancing the cursor on the strength of the retired sink's
/// answer would strand those records.
///
/// Takes `current` rather than reading the global itself, so the decision is a
/// pure function of its inputs — the same shape as [`classify_export_outcome`].
/// That is deliberate: a version of this that read the process-wide static
/// internally could only be tested by mutating that static, and two tests in
/// this PR have already failed on CI for exactly that reason while passing
/// locally.
#[cfg(feature = "db")]
fn fence_against_sink_swap(
    attempt: SinkAttempt,
    delivered_with: &std::sync::Arc<AuditExportRuntimeConfig>,
    current: Option<&std::sync::Arc<AuditExportRuntimeConfig>>,
) -> SinkAttempt {
    if current.is_some_and(|current| std::sync::Arc::ptr_eq(current, delivered_with)) {
        return attempt;
    }
    SinkAttempt::transport_error(
        "the audit sink was replaced while this batch was in flight; holding the cursor so \
         the batch is redelivered to the newly configured sink"
            .to_string(),
    )
}

#[cfg(feature = "db")]
async fn export_once_on_conn(
    conn: &mut diesel_async::AsyncPgConnection,
    config_arc: &std::sync::Arc<AuditExportRuntimeConfig>,
    shard_id: i32,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
) -> crate::error::HarvestResult<usize> {
    let config = config_arc.as_ref();
    ensure_cursor_row(conn, shard_id).await?;

    let now = Utc::now();
    let Some(claim) = claim_shard(conn, shard_id, config.batch_size, config.lease, now).await?
    else {
        // Nothing claimed, but the lag gauge must still be emitted: an
        // operator's "is the export keeping up?" signal has to stay live
        // exactly when deliveries are NOT happening.
        emit_lag_and_observed(conn, shard_id, metrics).await;
        return Ok(0);
    };

    let first_seq = claim.records.first().map_or(0, |r| r.seq);
    let last_seq = claim.records.last().map_or(0, |r| r.seq);

    let body = match serialize_batch(&claim.records) {
        Ok(body) => body,
        Err(error) => {
            // Cannot serialize what we claimed. Hold the cursor AND write a
            // real backoff (issue #953 review): a bare `release_claim` leaves
            // `next_attempt_at` in the past, so the next tick re-claims the
            // same unserializable batch immediately and the shard hot-spins
            // once per poll interval with the cursor frozen and nothing but a
            // log line per iteration. A backoff paces the failure and surfaces
            // it on `GET /admin/audit-export`.
            tracing::error!(
                shard = shard_id,
                error = %error,
                "failed to serialize an audit export batch; holding the cursor"
            );
            let now = Utc::now();
            let outcome = classify_export_outcome(
                &SinkAttempt::transport_error(format!("batch serialization failed: {error}")),
                last_seq,
                claim.consecutive_failures,
                &config.backoff,
                now,
            );
            apply_outcome(conn, shard_id, claim.claim_epoch, &outcome, now).await?;
            // A serialization failure says nothing about the shard's cursor
            // and lag. Both were readable this tick, moments ago, in
            // `claim_shard`. So this tick still counts as observed
            // (issue #1268).
            emit_lag_and_observed(conn, shard_id, metrics).await;
            return Ok(0);
        }
    };

    let delivered_at = Utc::now();
    let headers = export_headers(
        &config.secret,
        &body,
        shard_id,
        first_seq,
        last_seq,
        delivered_at,
    );
    let batch = AuditBatch {
        shard: shard_id,
        first_seq,
        last_seq,
        records: &claim.records,
        body: &body,
        headers: &headers,
    };

    // Bound the embedder-supplied call by the claim's own lease (issue #953
    // review). Two failures this closes:
    //
    //   * A sink slower than the lease livelocks: its claim is superseded by
    //     the next tick's before its 2xx lands, that acknowledgement is
    //     refused by the epoch guard, and the cursor never advances while the
    //     sink receives the same batch forever.
    //   * `fire_due_audit_exports` is awaited inline in
    //     `enforce_timeouts_once`, so an un-timed `await` on embedder code
    //     would wedge the shared scanner -- and every resident sequenced after
    //     it -- for as long as the sink hangs.
    //
    // A timeout is classified exactly like any other transport failure: the
    // cursor is held and the batch is retried. Never a loss.
    let attempt = deliver_within_lease(config, &batch, claim.lease_until).await;

    let attempt = fence_against_sink_swap(
        attempt,
        config_arc,
        read_global_audit_export_config().as_ref(),
    );

    let outcome = classify_export_outcome(
        &attempt,
        last_seq,
        claim.consecutive_failures,
        &config.backoff,
        Utc::now(),
    );

    let applied = apply_outcome(conn, shard_id, claim.claim_epoch, &outcome, Utc::now()).await?;

    let delivered = match &outcome {
        ExportOutcome::Advance { .. } if applied => {
            let count = claim.records.len();
            metrics
                .record_audit_exported(u16::try_from(shard_id).unwrap_or(u16::MAX), count as u64);
            count
        }
        ExportOutcome::Advance { .. } => {
            // The guarded write did not apply: this attempt's lease expired
            // and a fresher claim owns the shard, or a redrive bumped the
            // epoch. The batch was delivered (the receiver dedupes on
            // `(shard, seq)`) but this attempt must not move the cursor.
            tracing::warn!(
                shard = shard_id,
                claim_epoch = claim.claim_epoch,
                "audit export batch was acknowledged by the sink but its claim had \
                 already been superseded; the cursor was not advanced and the batch \
                 will be re-delivered (at-least-once)"
            );
            0
        }
        ExportOutcome::Backoff {
            last_status,
            last_error,
            consecutive_failures,
            ..
        } => {
            tracing::warn!(
                shard = shard_id,
                status = ?last_status,
                error = ?last_error,
                consecutive_failures,
                "audit export delivery failed; cursor held at its current position"
            );
            0
        }
    };

    emit_lag_and_observed(conn, shard_id, metrics).await;
    Ok(delivered)
}

/// Emit `harvest.audit.export_lag` and `harvest.audit.export_observed` for
/// one shard, best-effort.
///
/// **Always records `export_observed`, success or failure** (issue #1268).
/// Before this, a failed cursor read or lag query returned silently, and
/// `export_lag` simply kept its last value — commonly `0`, the caught-up
/// reading. Prometheus then saw neither a high value nor an absent series
/// while the shard went unexported: the two alerts the feature documents
/// both stayed quiet. `export_observed` going to `0` is the signal a rule can
/// alert on instead.
///
/// `export_lag` itself is untouched on failure, deliberately. A fabricated
/// reading would conflate "behind" with "unreadable". The stale value is
/// left exactly as before, and `export_observed` is the availability signal.
#[cfg(feature = "db")]
async fn emit_lag_and_observed(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: i32,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
) {
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_audit_export_cursor::dsl as cur;

    let shard_u16 = u16::try_from(shard_id).unwrap_or(u16::MAX);

    let acked: Result<Option<i64>, _> = cur::harvest_audit_export_cursor
        .find(shard_id)
        .select(cur::last_acked_seq)
        .first(conn)
        .await
        .optional();
    let Ok(Some(acked)) = acked else {
        metrics.record_audit_export_observed(shard_u16, false);
        return;
    };

    match export_lag_seconds(conn, acked, Utc::now()).await {
        Ok(lag) => {
            metrics.record_audit_export_observed(shard_u16, true);
            metrics.record_audit_export_lag(shard_u16, lag);
        }
        Err(_) => metrics.record_audit_export_observed(shard_u16, false),
    }
}

/// Export due audit batches across every assigned shard.
///
/// Follows the established scanner pattern: called from
/// [`crate::timeout::enforce_timeouts_once`] on the existing
/// `spawn_timeout_checker` poll interval — no new background task is spawned
/// (AC3). Mirrors
/// [`crate::completion_callback::fire_due_completion_deliveries`]'s per-shard
/// fan-out, because audit rows live on the shard whose database recorded
/// them, and a single-connection scan would never see the others.
///
/// A no-op (returns `Ok(0)` **before any query**) when no audit sink has been
/// configured (AC8).
///
/// # Connection handling
///
/// `conn` is used **only** for the unsharded fallback. When a `sharded_pool` is
/// present, every assigned shard — including a lone one — is exported through
/// that shard's own pool, acquired under [`SHARD_ACQUIRE_BOUND`].
///
/// Deliberately no inference that a single `shard_assignments` entry means
/// `conn` belongs to it. This is a `pub` primitive an embedder may drive by
/// hand, nothing here can verify which database `conn` points at, and being
/// wrong stamps one shard's audit rows under another shard's key — silent
/// corruption of the `(shard, seq)` identity the whole feature rests on.
/// Acquiring the exact pool can at worst skip a shard, loudly. See the comment
/// in the match arm for the full reasoning and its cost.
///
/// # Errors
/// Returns `HarvestError` if a database query fails. A sink's transport
/// failure is never an `Err` here — it is captured as a [`SinkAttempt`] and
/// classified into a backoff write.
#[cfg(feature = "db")]
pub async fn fire_due_audit_exports(
    conn: &mut diesel_async::AsyncPgConnection,
    sharded_pool: &Option<crate::shard::ShardedDbPool>,
    shard_assignments: &[crate::types::ShardId],
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
) -> crate::error::HarvestResult<usize> {
    let Some(config) = read_global_audit_export_config() else {
        return Ok(0);
    };

    let mut total = 0usize;

    match sharded_pool {
        Some(sp) if !shard_assignments.is_empty() => {
            // EVERY shard is exported through its own pool, including a single
            // assignment. An earlier revision reused the caller's `conn` when
            // `shard_assignments.len() == 1`, inferring from that alone that
            // `conn` belonged to the named shard (issue #953, Codex review
            // rounds 3, 14 and 18 -- the same question, answered three times).
            //
            // The inference is not sound for a `pub` primitive. A caller that
            // hands over a default-pool connection with a single NON-default
            // assignment -- which `enforce_timeouts_once` cannot detect --
            // would have its rows stamped in the default database under
            // another shard's key, a cursor created there under that key, the
            // real shard left unexported, and the mislabelled records deduped
            // against the true shard's sequence stream by the receiver.
            //
            // That is silent cross-shard corruption of the very `(shard, seq)`
            // identity this feature exists to guarantee. Acquiring the exact
            // pool can at worst SKIP a shard, loudly and recoverably. For an
            // audit exporter a visible skip beats invisible corruption, so the
            // inference is gone.
            //
            // The cost is that a shard pool sized 1 cannot export while the
            // scanner holds its only connection. That is already true of every
            // other per-shard resident of this loop (#605's delivery included),
            // so such a pool is not a supported configuration; the skip is
            // logged with that cause, and
            // autumn-foundation/autumn-harvest#1269 removes the constraint
            // outright by giving export its own task.
            for shard in shard_assignments {
                // A shard the scanner cannot reach is unobserved, not merely
                // silent (issue #1268). Every `continue` below must mark it
                // so before moving on. Otherwise `export_lag` is the only
                // signal left, and it just keeps its stale reading.
                let shard_u16 = u16::try_from(shard.as_i32()).unwrap_or(u16::MAX);
                let Some(pool) = sp.exact_pool_for(*shard).cloned() else {
                    metrics.record_audit_export_observed(shard_u16, false);
                    continue;
                };
                // Bounded, never a bare `pool.get()` — see `SHARD_ACQUIRE_BOUND`.
                let mut shard_conn =
                    match tokio::time::timeout(SHARD_ACQUIRE_BOUND, pool.get()).await {
                        Ok(Ok(c)) => c,
                        Ok(Err(e)) => {
                            tracing::error!(
                                "[audit_export] failed to get connection to shard {shard:?}: {e:?}"
                            );
                            metrics.record_audit_export_observed(shard_u16, false);
                            continue;
                        }
                        Err(_elapsed) => {
                            tracing::error!(
                                shard = shard.as_i32(),
                                bound = ?SHARD_ACQUIRE_BOUND,
                                "[audit_export] timed out acquiring a connection for this shard; \
                                 skipping it for one tick. If this repeats, the shard's pool is \
                                 saturated or too small: the scanner already holds one connection \
                                 from it, so a max_size of 1 can never yield a second"
                            );
                            metrics.record_audit_export_observed(shard_u16, false);
                            continue;
                        }
                    };
                // One shard's failure must never stop the others: an
                // unreachable shard is an availability problem, but silently
                // skipping every *later* shard's export would be a compliance
                // one.
                match export_once_on_conn(&mut shard_conn, &config, shard.as_i32(), metrics).await {
                    Ok(n) => total += n,
                    Err(e) => {
                        tracing::error!(
                            shard = shard.as_i32(),
                            error = %e,
                            "[audit_export] shard export failed"
                        );
                        // `export_once_on_conn` returned before it could emit
                        // either gauge for this tick (issue #1268).
                        metrics.record_audit_export_observed(shard_u16, false);
                    }
                }
            }
        }
        _ => {
            // Unsharded deployment, or a pool with no assignments. Label the
            // records with the pool's own default shard rather than a hardcoded
            // `0` (issue #953 review): a sharded pool whose default is not 0
            // would otherwise write a cursor row keyed `0` into that shard's
            // database, alongside the correctly-keyed cursor a worker assigned
            // to it maintains -- two independent counters stamping `export_seq`
            // on the same rows from different bases, which breaks per-shard
            // density and the receiver's contiguity check.
            let shard = sharded_pool
                .as_ref()
                .map_or(0, |sp| sp.default_shard().as_i32());
            // Logged and swallowed for the same reason as the sharded arm
            // above: `fire_due_audit_exports` is one resident of
            // `enforce_timeouts_once`, and a transient audit-export database
            // error must not abort the timeout/SLA/session residents
            // sequenced after it.
            match export_once_on_conn(conn, &config, shard, metrics).await {
                Ok(n) => total += n,
                Err(e) => {
                    tracing::error!(
                        shard,
                        error = %e,
                        "[audit_export] export failed on the default shard"
                    );
                    // See the sharded arm above (issue #1268): a propagated
                    // error means neither gauge was emitted this tick.
                    metrics.record_audit_export_observed(
                        u16::try_from(shard).unwrap_or(u16::MAX),
                        false,
                    );
                }
            }
        }
    }

    Ok(total)
}

// ── Unit tests (pure, no DB) ─────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn rec(seq: i64) -> AuditExportRecord {
        AuditExportRecord {
            shard: 3,
            seq,
            id: Uuid::from_u128(u128::try_from(seq).unwrap_or(0)),
            shard_id: Some(3),
            occurred_at: DateTime::from_timestamp(1_800_000_000, 0).expect("valid ts"),
            actor: "alice".to_string(),
            operation: "workflow.cancel".to_string(),
            target_type: "workflow".to_string(),
            target_id: Some("exec-1".to_string()),
            route_or_command: "POST /workflows/{id}/cancel".to_string(),
            request_id: None,
            idempotency_key: None,
            status: "SUCCEEDED".to_string(),
            error_summary: None,
            source: "api".to_string(),
        }
    }

    // ── JSON-lines batch shape ──────────────────────────────────────────────

    #[test]
    fn serialize_batch_emits_one_compact_json_object_per_line() {
        let body = serialize_batch(&[rec(1), rec(2), rec(3)]).expect("serializes");
        let text = String::from_utf8(body).expect("utf8");
        assert!(
            text.ends_with('\n'),
            "JSON-lines bodies must end with a newline so a log-ingest tail never \
             merges the last record with the next batch; got {text:?}"
        );
        let lines: Vec<&str> = text.trim_end_matches('\n').split('\n').collect();
        assert_eq!(lines.len(), 3, "one line per record");
        for (i, line) in lines.iter().enumerate() {
            let parsed: serde_json::Value = serde_json::from_str(line).expect("each line is JSON");
            assert_eq!(
                parsed["seq"],
                serde_json::json!(i64::try_from(i).unwrap() + 1)
            );
            assert!(
                !line.contains('\n'),
                "a record must never contain a raw newline or it would split a line"
            );
        }
    }

    #[test]
    fn serialize_batch_of_no_records_is_empty() {
        let body = serialize_batch(&[]).expect("serializes");
        assert!(
            body.is_empty(),
            "an empty batch must produce no bytes at all"
        );
    }

    // Redrive byte-identity (AC6): re-exporting the same (shard, seq) records
    // must produce exactly the same bytes, or the receiver's dedup on
    // (shard, seq) would be checking a different payload than it stored.
    #[test]
    fn serialize_batch_is_byte_identical_across_calls() {
        let first = serialize_batch(&[rec(7), rec(8)]).expect("serializes");
        let second = serialize_batch(&[rec(7), rec(8)]).expect("serializes");
        assert_eq!(first, second, "re-export must be byte-identical");
    }

    // A SIEM maps this to a fixed schema, so an absent optional field must
    // still appear as an explicit `null` rather than vanishing from the object.
    #[test]
    fn absent_optional_fields_serialize_as_explicit_null() {
        let body = serialize_batch(&[rec(1)]).expect("serializes");
        let line = String::from_utf8(body).expect("utf8");
        let parsed: serde_json::Value =
            serde_json::from_str(line.trim_end_matches('\n')).expect("json");
        for field in ["request_id", "idempotency_key", "error_summary"] {
            assert!(
                parsed.get(field).is_some_and(serde_json::Value::is_null),
                "{field} must serialize as an explicit null, not be omitted"
            );
        }
        // Tamper-evidence identity fields (AC4) are always present.
        assert_eq!(parsed["shard"], serde_json::json!(3));
        assert_eq!(parsed["seq"], serde_json::json!(1));
    }

    // ── Signing (the #605 X-Harvest-Signature scheme, reused verbatim) ───────

    #[test]
    fn export_headers_sign_the_exact_body_with_the_605_scheme() {
        let secret = CallbackSecret::new(b"topsecret".to_vec());
        let body = serialize_batch(&[rec(1), rec(2)]).expect("serializes");
        let now = DateTime::from_timestamp(1_800_000_000, 0).expect("valid ts");
        let headers = export_headers(&secret, &body, 3, 1, 2, now);

        let signature = headers
            .iter()
            .find(|(name, _)| *name == SIGNATURE_HEADER)
            .map(|(_, v)| v.clone())
            .expect("signature header present");
        assert_eq!(
            signature,
            crate::completion_callback::sign(&secret, &body),
            "must be the same HMAC scheme as issue #605, over the exact POSTed bytes"
        );
        assert!(signature.starts_with("sha256="));
    }

    #[test]
    fn export_headers_carry_the_shard_and_sequence_range() {
        let secret = CallbackSecret::new(Vec::new());
        let now = DateTime::from_timestamp(1_800_000_000, 0).expect("valid ts");
        let headers = export_headers(&secret, b"body", 3, 10, 42, now);
        let get = |name: &str| {
            headers
                .iter()
                .find(|(n, _)| *n == name)
                .map_or_else(|| panic!("{name} present"), |(_, value)| value.clone())
        };
        assert_eq!(get(SHARD_HEADER), "3");
        assert_eq!(get(FIRST_SEQ_HEADER), "10");
        assert_eq!(get(LAST_SEQ_HEADER), "42");
        assert_eq!(get(TIMESTAMP_HEADER), now.to_rfc3339());
    }

    // ── Sink attempt classification ─────────────────────────────────────────

    #[test]
    fn sink_attempt_is_success_only_for_2xx() {
        assert!(SinkAttempt::success(200).is_success());
        assert!(SinkAttempt::success(204).is_success());
        assert!(SinkAttempt::success(299).is_success());
        assert!(!SinkAttempt::success(199).is_success());
        assert!(!SinkAttempt::success(302).is_success());
        assert!(!SinkAttempt::success(500).is_success());
        assert!(!SinkAttempt::transport_error("refused".to_string()).is_success());
    }

    // ── The core AC2 invariant: never advance past a failure ────────────────

    #[test]
    fn a_2xx_advances_the_cursor_through_the_delivered_batch() {
        let now = Utc::now();
        let outcome = classify_export_outcome(
            &SinkAttempt::success(202),
            42,
            3,
            &ExportBackoff::default(),
            now,
        );
        assert_eq!(
            outcome,
            ExportOutcome::Advance {
                through_seq: 42,
                status: 202
            }
        );
    }

    #[test]
    fn a_failure_never_advances_the_cursor() {
        let now = Utc::now();
        for attempt in [
            SinkAttempt::success(500),
            SinkAttempt::success(404),
            SinkAttempt::success(301),
            SinkAttempt::transport_error("timeout".to_string()),
        ] {
            let outcome = classify_export_outcome(&attempt, 42, 0, &ExportBackoff::default(), now);
            assert!(
                matches!(outcome, ExportOutcome::Backoff { .. }),
                "a failed delivery must hold the cursor, never advance it: {attempt:?}"
            );
        }
    }

    // There is deliberately no dead-letter arm: unlike a completion callback
    // (#605), an audit record may never be dropped after N attempts — the
    // export is the compliance artifact. Retry forever, capped.
    #[test]
    fn repeated_failures_keep_backing_off_and_never_give_up() {
        let now = Utc::now();
        let backoff = ExportBackoff::default();
        for failures in [1_i32, 5, 50, 5_000, i32::MAX] {
            let outcome =
                classify_export_outcome(&SinkAttempt::success(503), 7, failures, &backoff, now);
            match outcome {
                ExportOutcome::Backoff {
                    next_attempt_at,
                    consecutive_failures,
                    ..
                } => {
                    assert!(next_attempt_at >= now, "backoff is always in the future");
                    assert!(
                        next_attempt_at
                            <= now + chrono::Duration::from_std(backoff.max_interval).unwrap(),
                        "backoff is capped at max_interval even after {failures} failures"
                    );
                    assert_eq!(
                        consecutive_failures,
                        failures.saturating_add(1),
                        "the failure counter increments (saturating at i32::MAX)"
                    );
                }
                ExportOutcome::Advance { .. } => {
                    panic!("a 503 must never advance the cursor")
                }
            }
        }
    }

    #[test]
    fn backoff_grows_exponentially_before_the_cap() {
        let now = DateTime::from_timestamp(1_800_000_000, 0).expect("valid ts");
        let backoff = ExportBackoff {
            initial_interval: Duration::from_secs(1),
            backoff_coefficient: 2.0,
            max_interval: Duration::from_secs(60),
        };
        let delay_after = |failures: i32| match classify_export_outcome(
            &SinkAttempt::success(500),
            1,
            failures,
            &backoff,
            now,
        ) {
            ExportOutcome::Backoff {
                next_attempt_at, ..
            } => (next_attempt_at - now).num_seconds(),
            ExportOutcome::Advance { .. } => panic!("not a success"),
        };
        assert_eq!(delay_after(0), 1, "first failure waits initial_interval");
        assert_eq!(delay_after(1), 2);
        assert_eq!(delay_after(2), 4);
        assert_eq!(delay_after(3), 8);
        assert_eq!(delay_after(30), 60, "capped at max_interval");
    }

    #[test]
    fn a_transport_error_is_recorded_as_the_last_error() {
        let now = Utc::now();
        let outcome = classify_export_outcome(
            &SinkAttempt::transport_error("connection refused".to_string()),
            9,
            0,
            &ExportBackoff::default(),
            now,
        );
        match outcome {
            ExportOutcome::Backoff {
                last_status,
                last_error,
                ..
            } => {
                assert_eq!(last_status, None);
                assert_eq!(last_error.as_deref(), Some("connection refused"));
            }
            ExportOutcome::Advance { .. } => panic!("transport error is never a success"),
        }
    }

    // ── Redrive: a rewind may only ever move the cursor backwards ───────────

    #[test]
    fn rewind_moves_the_cursor_backwards() {
        assert_eq!(
            resolve_rewind(100, 40),
            RewindOutcome::Rewound { from: 100, to: 40 }
        );
        assert_eq!(
            resolve_rewind(100, 0),
            RewindOutcome::Rewound { from: 100, to: 0 },
            "rewinding to 0 re-exports every retained record"
        );
    }

    // The dangerous direction: moving a cursor FORWARD would skip records that
    // were never delivered, silently creating exactly the gap this feature
    // exists to make impossible. It must be refused, not clamped-and-applied.
    #[test]
    fn rewind_refuses_to_move_the_cursor_forward() {
        assert_eq!(
            resolve_rewind(100, 101),
            RewindOutcome::NoOp {
                cursor: 100,
                requested: 101
            }
        );
        assert_eq!(
            resolve_rewind(100, i64::MAX),
            RewindOutcome::NoOp {
                cursor: 100,
                requested: i64::MAX
            }
        );
    }

    #[test]
    fn rewind_to_the_current_position_is_a_noop() {
        assert_eq!(
            resolve_rewind(100, 100),
            RewindOutcome::NoOp {
                cursor: 100,
                requested: 100
            }
        );
    }

    #[test]
    fn rewind_clamps_a_negative_request_to_zero() {
        assert_eq!(
            resolve_rewind(100, -5),
            RewindOutcome::Rewound { from: 100, to: 0 },
            "a negative position is meaningless; clamp to the beginning rather \
             than writing a negative cursor the CHECK constraint would reject"
        );
    }

    // ── Delivery state (pure; four branches, all reachable) ─────────────────

    #[test]
    fn delivery_state_reports_every_branch() {
        let now = DateTime::from_timestamp(1_800_000_000, 0).expect("valid ts");
        let future = now + chrono::Duration::seconds(30);
        let past = now - chrono::Duration::seconds(30);

        assert_eq!(
            delivery_state(Some(future), 0, past, now, None),
            "DELIVERING",
            "a live lease means a batch is in flight right now"
        );
        assert_eq!(
            delivery_state(Some(future), 4, future, now, None),
            "DELIVERING",
            "a live lease outranks a pending backoff"
        );
        assert_eq!(
            delivery_state(Some(past), 3, future, now, None),
            "BACKOFF",
            "an expired lease with failures and a future retry is backing off"
        );
        assert_eq!(
            delivery_state(None, 3, past, now, None),
            "RETRYING",
            "failures with the retry deadline already passed is a due retry, \
             not a healthy idle"
        );
        assert_eq!(delivery_state(None, 0, past, now, None), "IDLE");

        // Retirement overrides every other reading of the row: those columns
        // are a frozen snapshot, and reporting BACKOFF or RETRYING from them
        // would say records are pending that nothing owes and retention may
        // purge.
        for (lease, failures, attempt) in [
            (Some(future), 0, past),
            (Some(past), 3, future),
            (None, 3, past),
            (None, 0, past),
        ] {
            assert_eq!(
                delivery_state(lease, failures, attempt, now, Some(past)),
                "RETIRED",
                "a retired cursor must never report a live delivery state"
            );
        }
        assert_eq!(
            delivery_state(Some(now), 0, past, now, None),
            "IDLE",
            "a lease expiring exactly now is expired, not live"
        );
    }

    // ── Batch-size and lease clamping ───────────────────────────────────────

    #[test]
    fn batch_size_is_clamped_into_the_supported_range() {
        let with = |size| {
            AuditExportBuilderConfig {
                batch_size: size,
                ..AuditExportBuilderConfig::default()
            }
            .effective_batch_size()
        };

        assert_eq!(with(0), 1, "a zero batch would never make progress");
        assert_eq!(with(-7), 1);
        assert_eq!(with(i64::MIN), 1);
        assert_eq!(with(1), 1);
        assert_eq!(with(250), 250);
        assert_eq!(with(MAX_EXPORT_BATCH_SIZE), MAX_EXPORT_BATCH_SIZE);
        assert_eq!(
            with(MAX_EXPORT_BATCH_SIZE + 1),
            MAX_EXPORT_BATCH_SIZE,
            "a batch is buffered in memory and POSTed as one body; the ceiling \
             is what stops a misconfiguration serializing a whole retention \
             window into one request"
        );
        assert_eq!(with(i64::MAX), MAX_EXPORT_BATCH_SIZE);
        assert_eq!(
            AuditExportBuilderConfig::default().effective_batch_size(),
            DEFAULT_EXPORT_BATCH_SIZE
        );
    }

    #[test]
    fn the_lease_is_floored_at_one_second() {
        let with = |lease| {
            AuditExportBuilderConfig {
                lease,
                ..AuditExportBuilderConfig::default()
            }
            .effective_lease()
        };

        assert_eq!(
            with(std::time::Duration::ZERO),
            std::time::Duration::from_secs(1),
            "a zero lease also becomes a zero sink timeout, so every batch \
             would fail before it was sent"
        );
        assert_eq!(
            with(std::time::Duration::from_millis(1)),
            std::time::Duration::from_secs(1)
        );
        assert_eq!(
            with(std::time::Duration::from_secs(120)),
            std::time::Duration::from_secs(120)
        );
        assert_eq!(
            AuditExportBuilderConfig::default().effective_lease(),
            DEFAULT_EXPORT_LEASE
        );
    }

    // ── The HMAC secret is a required control for the webhook path ──────────

    #[test]
    fn a_webhook_without_a_secret_is_reported_as_misconfigured() {
        let webhook_only = AuditExportBuilderConfig {
            webhook_url: Some("https://siem.example.com/audit".to_string()),
            ..AuditExportBuilderConfig::default()
        };
        assert!(
            webhook_only.webhook_is_missing_a_secret(),
            "HMAC-SHA256 accepts an empty key and emits a well-formed signature \
             anyone can recompute, so an unconfigured secret is worse than no \
             signature at all -- it must fail the build, not warn"
        );

        let with_secret = AuditExportBuilderConfig {
            secret: Some(CallbackSecret::new(b"k".to_vec())),
            ..webhook_only.clone()
        };
        assert!(!with_secret.webhook_is_missing_a_secret());

        // An embedder-supplied sink may authenticate some other way (IAM,
        // mTLS, a local file), so a missing HMAC key there is legitimate.
        let custom_sink = AuditExportBuilderConfig {
            sink: Some(std::sync::Arc::new(NoopSink)),
            ..webhook_only
        };
        assert!(!custom_sink.webhook_is_missing_a_secret());

        assert!(
            !AuditExportBuilderConfig::default().webhook_is_missing_a_secret(),
            "an unconfigured feature is not a misconfiguration"
        );
    }

    #[test]
    fn the_builder_config_debug_never_prints_the_secret() {
        let config = AuditExportBuilderConfig {
            secret: Some(CallbackSecret::new(b"SUPERSECRET".to_vec())),
            webhook_url: Some("https://siem.example.com/audit".to_string()),
            ..AuditExportBuilderConfig::default()
        };
        let rendered = format!("{config:?}");
        assert!(
            !rendered.contains("SUPERSECRET"),
            "the HMAC key must never reach a log line: {rendered}"
        );
        assert!(rendered.contains("redacted"));
    }

    // ── Trait shape ─────────────────────────────────────────────────────────

    struct NoopSink;

    impl AuditSink for NoopSink {
        fn deliver<'a>(&'a self, _batch: &'a AuditBatch<'a>) -> SinkFuture<'a> {
            Box::pin(async { SinkAttempt::success(200) })
        }
    }

    /// Delivery is bounded by what REMAINS of the claim's lease, not the full
    /// lease (issue #953, Codex review round 19 P1). A sink that answers just
    /// inside the full lease but outside the remainder must be timed out here,
    /// because the database lease has expired and another exporter may already
    /// have reclaimed the shard and bumped the epoch — an acknowledgement from
    /// this attempt would be refused, and at consistently near-lease latency
    /// the cursor would never advance.
    #[cfg(feature = "db")]
    #[tokio::test]
    async fn delivery_is_bounded_by_the_remaining_lease_not_the_whole_one() {
        struct SlowSink;
        impl AuditSink for SlowSink {
            fn deliver<'a>(&'a self, _batch: &'a AuditBatch<'a>) -> SinkFuture<'a> {
                Box::pin(async {
                    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                    SinkAttempt::success(200)
                })
            }
        }

        let config = AuditExportRuntimeConfig {
            sink: std::sync::Arc::new(SlowSink),
            secret: CallbackSecret::new(b"k".to_vec()),
            batch_size: 10,
            backoff: ExportBackoff::default(),
            // A generous lease: bounding by THIS would let the 300ms sink win.
            lease: std::time::Duration::from_secs(60),
        };
        let batch = AuditBatch {
            shard: 0,
            first_seq: 1,
            last_seq: 1,
            body: b"{}",
            headers: &[],
            records: &[],
        };

        // Most of the lease is already spent: only 50ms remain.
        let nearly_expired = Utc::now() + chrono::Duration::milliseconds(50);
        let attempt = deliver_within_lease(&config, &batch, nearly_expired).await;
        assert!(
            attempt.status.is_none(),
            "a sink answering after the lease remainder must be a transport \
             failure, not a success: the claim is gone by then"
        );

        // Ample remainder: the same sink succeeds.
        let fresh = Utc::now() + chrono::Duration::seconds(5);
        let attempt = deliver_within_lease(&config, &batch, fresh).await;
        assert_eq!(attempt.status, Some(200));

        // An already-elapsed lease is an immediate timeout, never a skip.
        let past = Utc::now() - chrono::Duration::seconds(1);
        let attempt = deliver_within_lease(&config, &batch, past).await;
        assert!(attempt.status.is_none());
    }

    /// A SIEM ingest URL routinely carries its credential in the path or query,
    /// so `Debug` must not echo it (issue #953, Codex review round 23 P1). The
    /// hand-written impl already redacted the HMAC key; the URL is a secret in
    /// the same way.
    #[test]
    fn debug_never_prints_a_webhook_urls_path_or_query() {
        let config = AuditExportBuilderConfig {
            webhook_url: Some(
                "https://http-inputs.example.splunkcloud.com/services/collector/\
                 B5A79AAD-D822-46CC-80D1-819F80D7BFB0?index=audit"
                    .to_string(),
            ),
            secret: Some(CallbackSecret::new(b"hmac".to_vec())),
            ..AuditExportBuilderConfig::default()
        };

        let rendered = format!("{config:?}");
        assert!(
            !rendered.contains("B5A79AAD"),
            "the collector token must never reach a log line: {rendered}"
        );
        assert!(
            !rendered.contains("index=audit"),
            "nor the query string, which can carry one too: {rendered}"
        );
        assert!(
            rendered.contains("https://http-inputs.example.splunkcloud.com/<redacted>"),
            "the origin is the useful, non-secret part and should survive: {rendered}"
        );
        assert!(
            !rendered.contains("hmac"),
            "and the HMAC key stays redacted: {rendered}"
        );
    }

    #[test]
    fn redact_webhook_url_strips_userinfo_and_refuses_to_echo_junk() {
        // Userinfo is itself a credential.
        assert_eq!(
            redact_webhook_url("https://user:s3cret@siem.example.com/ingest?k=v"),
            "https://siem.example.com/<redacted>"
        );
        // A bare origin still renders.
        assert_eq!(
            redact_webhook_url("https://siem.example.com"),
            "https://siem.example.com/<redacted>"
        );
        // Anything unparseable is NOT echoed back on the assumption it is safe.
        for junk in ["not-a-url", "https://", ""] {
            let rendered = redact_webhook_url(junk);
            assert_eq!(rendered, "<unparseable webhook url redacted>");
        }
    }

    /// A delivery whose sink was swapped mid-flight must not advance the
    /// cursor: the records went to the OLD destination, and marking them
    /// delivered would leave the newly configured SIEM without them (issue
    /// #953, Codex review round 25 P1).
    ///
    /// Fully deterministic — the decision is a pure function of its arguments,
    /// so this touches no process-wide state and cannot race the 33 other tests
    /// in this binary that write `GLOBAL_AUDIT_EXPORT_CONFIG`.
    #[cfg(feature = "db")]
    #[test]
    fn a_sink_swapped_mid_flight_holds_the_cursor() {
        fn config(batch_size: i64) -> std::sync::Arc<AuditExportRuntimeConfig> {
            std::sync::Arc::new(AuditExportRuntimeConfig {
                sink: std::sync::Arc::new(NoopSink),
                secret: CallbackSecret::new(b"k".to_vec()),
                batch_size,
                backoff: ExportBackoff::default(),
                lease: std::time::Duration::from_secs(30),
            })
        }

        let delivered_with = config(10);

        // Same Arc still installed: the outcome is untouched.
        let kept = fence_against_sink_swap(
            SinkAttempt::success(200),
            &delivered_with,
            Some(&delivered_with),
        );
        assert_eq!(
            kept.status,
            Some(200),
            "an unchanged sink must leave the outcome alone"
        );

        // A different runtime's sink is now installed: the 2xx must not count.
        let replacement = config(99);
        let fenced = fence_against_sink_swap(
            SinkAttempt::success(200),
            &delivered_with,
            Some(&replacement),
        );
        assert!(
            fenced.status.is_none(),
            "a 2xx from the sink that was replaced mid-flight must be held, or \
             the cursor advances past records the new SIEM never received"
        );

        // An equal-but-distinct config is still a different sink: identity,
        // not value, is what decides where the bytes actually went.
        let twin = config(10);
        let fenced =
            fence_against_sink_swap(SinkAttempt::success(200), &delivered_with, Some(&twin));
        assert!(fenced.status.is_none());

        // Export switched off mid-flight counts as swapped too.
        let fenced = fence_against_sink_swap(SinkAttempt::success(200), &delivered_with, None);
        assert!(fenced.status.is_none());
    }

    #[test]
    fn an_explicitly_empty_webhook_secret_is_missing() {
        // The realistic path: `audit_export_secret(std::env::var("SIEM_HMAC")
        // .unwrap_or_default())` with the variable unset. An `is_none()` check
        // waves this through and the build ships a signature anyone can
        // recompute -- the exact outcome the check exists to prevent.
        let mut config = AuditExportBuilderConfig {
            webhook_url: Some("https://siem.example.com/ingest".to_string()),
            secret: Some(CallbackSecret::new(Vec::new())),
            ..AuditExportBuilderConfig::default()
        };
        assert!(
            config.webhook_is_missing_a_secret(),
            "an empty secret must fail closed exactly like an absent one"
        );

        config.secret = Some(CallbackSecret::new(b"real".to_vec()));
        assert!(!config.webhook_is_missing_a_secret());

        // Still scoped to the webhook path: an embedder sink may authenticate
        // however it likes, so an empty secret there is only a warning.
        config.secret = Some(CallbackSecret::new(Vec::new()));
        config.sink = Some(std::sync::Arc::new(NoopSink));
        assert!(!config.webhook_is_missing_a_secret());
    }

    #[test]
    fn audit_sink_is_object_safe_and_send_sync() {
        fn assert_bounds<T: AuditSink>() {}
        assert_bounds::<NoopSink>();
        let boxed: Box<dyn AuditSink> = Box::new(NoopSink);
        let _arc: std::sync::Arc<dyn AuditSink> = std::sync::Arc::from(boxed);
    }
}
