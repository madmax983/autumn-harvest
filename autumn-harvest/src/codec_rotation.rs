//! Payload-codec key rotation and the lazy re-encryption sweep (issue #948).
//!
//! ## Why
//!
//! The [`crate::payload_codec`] boundary lets an embedder encrypt every
//! payload-bearing field before it touches `harvest_events`. Without rotation
//! that is compliance theater: `harvest_events` is append-only, so after a key
//! compromise every byte of stored history stays encrypted under the
//! compromised key forever. This module closes that: envelopes carry a key id,
//! the registry holds many keyed codecs with exactly one active, and a
//! shard-local background sweep lazily re-encrypts stored rows onto the active
//! key until the old key can be retired with proof that nothing depends on it.
//!
//! ## ⚠️ Sanctioned in-place mutation exception #3
//!
//! Re-encryption **mutates stored `harvest_events.event_data` bytes in place**.
//! Exactly two code paths write `event_data` after insert: PII erasure
//! ([`crate::erase`], issue #495, historically "exception #2") and this
//! ("#3"). See the "Engine Invariants" section of `CLAUDE.md`, including why
//! the heartbeat checkpoint issue #948 named as the first exception is not one
//! (it mutates `harvest_task_queue`, not the event log).
//!
//! The scope guarantee that makes it safe: **only the ciphertext bytes inside
//! payload fields change.** The decoded plaintext is byte-identical before and
//! after, and the event `type`, variant structure, event ids, ordering, and
//! timestamps are never touched — so replay determinism is unaffected *by
//! construction*, not by convention. `replay_fidelity` in
//! `tests/integration/codec_rotation_db_tests.rs` proves it end to end.
//!
//! ## What the sweep will not touch
//!
//! - **Offload reference envelopes** (`_harvest_offload_envelope`, issue #524).
//!   Offload composes *after* codec encode, so the field holds a reference, not
//!   ciphertext; re-encoding it would encrypt the reference and orphan the
//!   blob. Re-encrypting the offloaded blob itself is embedder-owned storage
//!   and explicitly out of scope.
//! - **Erasure tombstones** (`_harvest_erased`, issue #495) — no ciphertext.
//! - **Plaintext fields.** A field that is not an envelope carries no key id,
//!   so it is not "a row carrying a non-active key id". The sweep migrates keys;
//!   it never newly encrypts history that was written in the clear.
//! - **Envelopes already on the active key** — which is what makes a re-run a
//!   no-op and the sweep idempotent.

use serde_json::Value;

use crate::error::{HarvestError, HarvestResult};
use crate::payload_codec::{PayloadCodecs, codec_envelope_key_id};
use crate::payload_store::{PAYLOAD_FIELD_KEYS, is_offload_envelope};

/// What one call to [`reencrypt_event_payload_fields`] did to a single event.
///
/// Counts only — never payload content — so it is safe to thread into logs and
/// operator reports.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReencryptOutcome {
    /// Payload fields decoded with a retired key and re-encoded under the
    /// active one.
    pub fields_reencrypted: usize,
    /// Offload reference envelopes (issue #524) passed through untouched.
    pub fields_skipped_offloaded: usize,
    /// Erasure tombstones (issue #495) passed through untouched.
    pub fields_skipped_erased: usize,
    /// Fields already encoded under the active key — the idempotence path.
    pub fields_already_active: usize,
}

impl ReencryptOutcome {
    /// Whether the event's stored bytes actually changed, i.e. whether the
    /// caller has anything to write back.
    #[must_use]
    pub const fn changed(&self) -> bool {
        self.fields_reencrypted > 0
    }

    /// Saturating merge for multi-row accumulation.
    #[must_use]
    pub const fn merged(self, other: Self) -> Self {
        Self {
            fields_reencrypted: self
                .fields_reencrypted
                .saturating_add(other.fields_reencrypted),
            fields_skipped_offloaded: self
                .fields_skipped_offloaded
                .saturating_add(other.fields_skipped_offloaded),
            fields_skipped_erased: self
                .fields_skipped_erased
                .saturating_add(other.fields_skipped_erased),
            fields_already_active: self
                .fields_already_active
                .saturating_add(other.fields_already_active),
        }
    }
}

/// `true` when `codecs` is in a state where sweeping could only *remove*
/// encryption.
///
/// Registering [`crate::payload_codec::IdentityCodec`] under a key id and
/// activating it is a request to store payloads in the clear. Honouring that on
/// the *write* path is the embedder's business; honouring it on the sweep would
/// mean walking stored history and replacing ciphertext with plaintext, which
/// is a crypto-shredding-in-reverse operation nobody asked for. The sweep
/// refuses to do it and reports zero work instead.
///
/// Only the sweep consults this, and the sweep only exists under `db`.
#[cfg(feature = "db")]
fn active_key_would_decrypt(codecs: &PayloadCodecs) -> bool {
    codecs.active_codec_id().is_none_or(|id| id == "identity")
}

/// A bounded category for a per-row sweep failure, safe to put in a log line.
///
/// The failure text itself is NOT loggable. A decode error here originates in
/// an **embedder-supplied** `PayloadCodec`, so its `CodecError` message is
/// arbitrary text this crate does not control and cannot bound — it may carry
/// key material or plaintext diagnostics the codec author included for local
/// debugging and never expected in a production log.
///
/// The lossy read path already made this decision: every failure it can hit
/// becomes one of the fixed `UNDECODABLE_REASON_*` strings rather than the
/// codec's own message. The sweep runs over the same rows with the same codecs
/// and has no reason to be laxer. What makes the line actionable is the row id
/// and the key ids, both of which are bounded and stay.
/// How often a shard whose pass is already complete re-confirms that against the
/// census.
///
/// The census is a full scan, and a converged shard reaches the end of its scan
/// on every scanner tick, so confirming completion per tick would
/// sequential-scan `harvest_events` several times a second per shard. But it
/// cannot be dropped entirely: a row that commits below the cursor (its
/// `BIGSERIAL` id allocated before the scan passed it, its transaction
/// committing after) is found no other way, and leaving it stranded deadlocks
/// retirement.
///
/// Five minutes bounds the scan load to something negligible while keeping the
/// recovery window far shorter than any rotation an operator is watching.
#[cfg(feature = "db")]
const COMPLETED_CURSOR_REVALIDATION_SECS: f64 = 300.0;

#[cfg(feature = "db")]
const fn sweep_error_kind(error: &HarvestError) -> &'static str {
    match error {
        HarvestError::UnknownCodecKey { .. } => "unknown_key",
        HarvestError::UnknownPayloadCodec { .. } => "unknown_codec",
        HarvestError::Serialization(_) => "invalid_json",
        HarvestError::Database(_) => "database",
        HarvestError::Config(_) => "codec_error",
        _ => "other",
    }
}

/// Re-encode every payload-bearing field of one serialized event that carries a
/// **non-active** codec key id, leaving everything else byte-identical.
///
/// `event_value` must be the adjacently-tagged form stored in
/// `harvest_events.event_data`: `{"type": "...", "data": {...}}`. Only values
/// under [`PAYLOAD_FIELD_KEYS`] inside `data` are considered — the same
/// allowlist the codec, erasure and export paths already share — so the event
/// `type`, event ids, ordering and timestamps are structurally out of reach.
///
/// **All-or-nothing.** Every field is decoded and re-encoded into a staging
/// buffer first; `event_value` is mutated only once every field has succeeded.
/// A field that cannot be decoded (an unregistered key id, corrupt ciphertext)
/// returns `Err` with `event_value` **completely unmodified**, so the caller can
/// never persist a half-rotated row and never persists plaintext where
/// ciphertext used to be.
///
/// Returns a zero outcome — touching nothing — when no keyed codec is
/// registered, which is every deployment that has not adopted rotation.
///
/// # Errors
///
/// [`HarvestError::UnknownCodecKey`] when a field names a key id this registry
/// cannot resolve; any other [`HarvestError`] the codec's own decode/encode
/// raises.
pub fn reencrypt_event_payload_fields(
    codecs: &PayloadCodecs,
    event_value: &mut Value,
) -> HarvestResult<ReencryptOutcome> {
    reencrypt_event_payload_fields_under(codecs, &codecs.active_key_id(), event_value)
}

/// [`reencrypt_event_payload_fields`], pinned to an explicit target key id.
///
/// [`sweep_codec_reencryption_once`] resolves the active key once per batch and
/// calls THIS function (not the wrapper) for every row, so a concurrent
/// `set_active_key` cannot straddle a single row and leave half its
/// fields on the old key and half on the new one. It also cannot cause a
/// plaintext write: [`PayloadCodecs::encode_payload_under`] refuses an identity
/// codec at the point of use rather than relying on a check made earlier.
///
/// # Errors
///
/// As [`reencrypt_event_payload_fields`], plus [`HarvestError::Config`] when
/// `target_key_id` names the identity codec and
/// [`HarvestError::UnknownCodecKey`] when it is not registered at all.
pub fn reencrypt_event_payload_fields_under(
    codecs: &PayloadCodecs,
    target_key_id: &str,
    event_value: &mut Value,
) -> HarvestResult<ReencryptOutcome> {
    let mut outcome = ReencryptOutcome::default();
    if !codecs.has_keyed_codecs() {
        return Ok(outcome);
    }
    let Some(data) = event_value.get("data").and_then(Value::as_object) else {
        return Ok(outcome);
    };

    let mut staged: Vec<(&'static str, Value)> = Vec::with_capacity(PAYLOAD_FIELD_KEYS.len());
    for key in PAYLOAD_FIELD_KEYS {
        let Some(field) = data.get(key) else {
            continue;
        };
        if crate::erase::is_erasure_tombstone(field) {
            outcome.fields_skipped_erased += 1;
            continue;
        }
        // Discriminator-only, NOT the strict `extract_offload_ref` parser: a
        // field bearing the offload marker is passed through whether or not its
        // reference parses, because there is no ciphertext here to rotate and
        // rewriting it would orphan the blob either way.
        if is_offload_envelope(field) {
            outcome.fields_skipped_offloaded += 1;
            continue;
        }
        let Some(key_id) = codec_envelope_key_id(field) else {
            // Plaintext: no key id, so nothing to rotate. The sweep never
            // newly encrypts history written in the clear.
            continue;
        };
        if key_id == target_key_id {
            outcome.fields_already_active += 1;
            continue;
        }
        // `field` was just confirmed to be a codec envelope by
        // `codec_envelope_key_id` above, so `decode_payload_bytes` always
        // returns `Some` here. Re-encryption migrates ciphertext only and
        // never needs the plaintext parsed as JSON, so it stays as raw bytes
        // straight through to `encode_payload_bytes_under` -- skipping the
        // deserialize-then-reserialize round trip `decode_payload` +
        // `encode_payload_under` would otherwise pay for every field.
        let plaintext_bytes = codecs.decode_payload_bytes(field)?.ok_or_else(|| {
            HarvestError::Config(
                "field matched codec_envelope_key_id but decode_payload_bytes found no envelope"
                    .to_string(),
            )
        })?;
        staged.push((
            key,
            codecs.encode_payload_bytes_under(target_key_id, &plaintext_bytes)?,
        ));
    }

    if staged.is_empty() {
        return Ok(outcome);
    }
    // The immutable borrow above established that `data` is an object, so this
    // cannot fail; treating it as an error rather than an `if let` keeps a
    // future refactor from silently discarding `staged` and reporting no work.
    let data_mut = event_value
        .get_mut("data")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| {
            HarvestError::Config(
                "event data object vanished between read and write during re-encryption"
                    .to_string(),
            )
        })?;
    for (key, value) in staged {
        data_mut.insert(key.to_string(), value);
        outcome.fields_reencrypted += 1;
    }
    Ok(outcome)
}

/// Whether any payload-bearing field of a serialized event carries a codec key
/// id other than `active` — i.e. whether this row has anything for the sweep to
/// do at all.
///
/// A few map lookups and no allocation, so the sweep can skip the deep clone of
/// an event that is already fully converted. That is the steady state on a
/// swept shard, and cloning multi-kilobyte event JSON to discover it is pure
/// waste. Deliberately NOT built on [`event_key_ids`], which allocates a
/// `BTreeSet<String>` — right for the error-reporting path, wrong for a
/// per-row pre-check.
#[must_use]
pub fn has_non_active_key(event_value: &Value, active: &str) -> bool {
    event_value
        .get("data")
        .and_then(Value::as_object)
        .is_some_and(|data| {
            PAYLOAD_FIELD_KEYS.iter().any(|key| {
                data.get(*key)
                    .and_then(codec_envelope_key_id)
                    .is_some_and(|key_id| key_id != active)
            })
        })
}

/// Every codec key id referenced by one serialized event's payload fields
/// (issue #948).
///
/// A kid-less envelope resolves to
/// [`CODEC_LEGACY_KEY_ID`](crate::payload_codec::CODEC_LEGACY_KEY_ID). Fields
/// that carry no ciphertext at all — plaintext, offload reference envelopes,
/// erasure tombstones — contribute nothing, which is exactly what makes the
/// retirement gate's census agree with what the sweep is able to convert.
#[must_use]
pub fn event_key_ids(event_value: &Value) -> std::collections::BTreeSet<String> {
    let mut ids = std::collections::BTreeSet::new();
    let Some(data) = event_value.get("data").and_then(Value::as_object) else {
        return ids;
    };
    for key in PAYLOAD_FIELD_KEYS {
        if let Some(field) = data.get(key)
            && let Some(key_id) = codec_envelope_key_id(field)
        {
            ids.insert(key_id.to_string());
        }
    }
    ids
}

// ── DB-gated sweep, census, and retirement gate ──────────────────────────────

/// Rows examined per shard per scanner tick when the caller does not override
/// it.
///
/// The sweep's rate limiter: raising it converts history faster and holds the
/// scanner connection longer; lowering it (or passing `0`) throttles or stops
/// the sweep without a deploy. Unconditional (not `db`-gated) so a no-`db`
/// build can still name the default in a `WorkerConfig`.
pub const CODEC_ROTATION_DEFAULT_BATCH: i64 = 200;

#[cfg(feature = "db")]
pub use db::{
    CodecRotationCursor, FleetWriteFence, ShardRotationProgress, activate_codec_key,
    compare_and_swap_event, count_rows_by_key_id, load_shard_rotation_progress,
    load_shard_rotation_progress_against, refresh_active_codec_key, retire_codec_key,
    sweep_codec_reencryption, sweep_codec_reencryption_once, write_cursor,
};

#[cfg(feature = "db")]
mod db {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use chrono::{DateTime, Utc};
    use diesel::sql_types::{Array, BigInt, Double, Integer, Jsonb, Nullable, Text, Timestamptz};
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
    use serde::Serialize;
    use serde_json::Value;

    use crate::error::{
        CodecKeyActivationBlocker, CodecKeyShardRemainder, HarvestError, HarvestResult,
        database_error,
    };
    use crate::payload_codec::PayloadCodecs;
    use crate::payload_store::PAYLOAD_FIELD_KEYS;
    use crate::telemetry::MetricsRecorder;

    use super::{
        active_key_would_decrypt, has_non_active_key, reencrypt_event_payload_fields_under,
    };

    /// The exact SQL mirror of
    /// [`codec_envelope_parts`](crate::payload_codec) — an object with
    /// `_harvest_codec_envelope == 1`, string `codec_id`, string `data`, and
    /// either exactly those three keys or those three plus a string `kid`.
    ///
    /// Kept byte-for-byte in step with the Rust shape check so the census can
    /// never count a row the sweep is unable to convert (which would make the
    /// retirement gate block forever) nor miss one it can (which would let the
    /// gate open early). `a_near_envelope_is_neither_counted_nor_swept` in
    /// `tests/integration/codec_rotation_db_tests.rs` pins the two together.
    const ENVELOPE_PREDICATE: &str = "
              jsonb_typeof(f.value) = 'object'
          AND jsonb_typeof(f.value -> 'codec_id') = 'string'
          AND jsonb_typeof(f.value -> 'data') = 'string'
          AND (
                  -- Version 1: exactly three keys, no `kid`. Every pre-#948
                  -- envelope, and every envelope written while the legacy key
                  -- is active.
                  (
                      f.value -> '_harvest_codec_envelope' = '1'::jsonb
                      -- jsonb compares numbers as `numeric`, so the line above
                      -- alone also accepts 1.0 -- which serde_json's `as_i64`
                      -- rejects. Pin the text form too, so Postgres and Rust
                      -- classify byte-identically.
                  AND f.value ->> '_harvest_codec_envelope' = '1'
                  AND (SELECT COUNT(*) FROM jsonb_object_keys(f.value)) = 3
                  )
               OR
                  -- Version 2: exactly four keys, the fourth a `kid` satisfying
                  -- the same charset/length rule `validate_key_id` applies in
                  -- Rust (so crafted workflow input cannot inject census rows).
                  (
                      f.value -> '_harvest_codec_envelope' = '2'::jsonb
                  AND f.value ->> '_harvest_codec_envelope' = '2'
                  AND (SELECT COUNT(*) FROM jsonb_object_keys(f.value)) = 4
                  AND jsonb_typeof(f.value -> 'kid') = 'string'
                  AND f.value ->> 'kid' ~ '^[A-Za-z0-9._:-]{1,64}$'
                  )
              )";

    #[derive(diesel::QueryableByName)]
    struct KeyCountRow {
        #[diesel(sql_type = Text)]
        key_id: String,
        #[diesel(sql_type = BigInt)]
        row_count: i64,
    }

    #[derive(diesel::QueryableByName)]
    struct EventRow {
        #[diesel(sql_type = BigInt)]
        id: i64,
        #[diesel(sql_type = Jsonb)]
        event_data: Value,
    }

    #[derive(diesel::QueryableByName)]
    struct CursorRow {
        #[diesel(sql_type = Text)]
        active_key_id: String,
        #[diesel(sql_type = BigInt)]
        last_event_id: i64,
        #[diesel(sql_type = BigInt)]
        rows_reencrypted: i64,
        #[diesel(sql_type = BigInt)]
        unresolved_rows: i64,
        #[diesel(sql_type = Nullable<Timestamptz>)]
        completed_at: Option<DateTime<Utc>>,
        #[diesel(sql_type = Timestamptz)]
        updated_at: DateTime<Utc>,
    }

    #[derive(diesel::QueryableByName)]
    struct Present {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        present: bool,
    }

    /// The durable per-shard resume cursor for one rotation pass (issue #948).
    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    pub struct CodecRotationCursor {
        /// The key id this pass is converting ONTO. When it differs from the
        /// process's current active key the pass is stale and the next sweep
        /// restarts from the beginning of the shard.
        pub active_key_id: String,
        /// Highest `harvest_events.id` this pass has examined.
        pub last_event_id: i64,
        /// Rows this pass has actually rewritten.
        pub rows_reencrypted: i64,
        /// Rows this pass examined but could not convert — an unregistered key
        /// id, corrupt ciphertext, or a compare-and-swap lost to a concurrent
        /// erasure. Non-zero at the end of a pass forces another pass instead
        /// of a completion stamp, so a transient failure always gets another
        /// chance.
        pub unresolved_rows: i64,
        /// Set only when a pass reached the end of the shard having converted
        /// everything it saw — "this key is safe to gate on", not merely "the
        /// scan ran off the end".
        pub completed_at: Option<DateTime<Utc>>,
        /// Last time the cursor advanced.
        pub updated_at: DateTime<Utc>,
    }

    /// One shard's answer to "how far along is the rotation?" (issue #948, AC7).
    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    pub struct ShardRotationProgress {
        /// The key id this process is currently encoding new writes under.
        pub active_key_id: String,
        /// Rows carrying each observed key id, including the active one. A
        /// kid-less (pre-rotation) envelope counts under
        /// [`CODEC_LEGACY_KEY_ID`](crate::payload_codec::CODEC_LEGACY_KEY_ID).
        pub rows_by_key_id: BTreeMap<String, i64>,
        /// The resume cursor for the active key's pass, when one exists.
        pub cursor: Option<CodecRotationCursor>,
    }

    impl ShardRotationProgress {
        /// Rows still carrying a **non-active** key id — the number that must
        /// reach zero before the outgoing key can be retired.
        #[must_use]
        pub fn rows_remaining(&self) -> i64 {
            self.rows_by_key_id
                .iter()
                .filter(|(key_id, _)| *key_id != &self.active_key_id)
                .map(|(_, count)| *count)
                .sum()
        }
    }

    /// Whether `harvest_codec_rotation_cursor` exists yet.
    ///
    /// A deployment mid-migration must not fail its whole scanner tick because
    /// the sweep's bookkeeping table has not landed.
    async fn cursor_table_present(conn: &mut AsyncPgConnection) -> HarvestResult<bool> {
        let probe: Present = diesel::sql_query(
            "SELECT to_regclass('harvest_codec_rotation_cursor') IS NOT NULL AS present",
        )
        .get_result(conn)
        .await
        .map_err(database_error)?;
        Ok(probe.present)
    }

    /// Count `harvest_events` rows per codec key id on this connection's shard
    /// (issue #948).
    ///
    /// A row is counted once per distinct key id it references, so an event
    /// whose `input` and `output` sit under different keys contributes to both.
    /// Fields with no ciphertext — plaintext, offload reference envelopes
    /// (#524), erasure tombstones (#495) — contribute nothing, which is what
    /// keeps this census in agreement with what the sweep can actually convert.
    ///
    /// This is a sequential scan of the largest table in the schema. It backs an
    /// admin-gated, operator-invoked read and the retirement gate — both
    /// rotation-scoped, run a handful of times per rotation — not anything on a
    /// request or dispatch path.
    ///
    /// # Errors
    ///
    /// Propagates database failures.
    pub async fn count_rows_by_key_id(
        conn: &mut AsyncPgConnection,
    ) -> HarvestResult<BTreeMap<String, i64>> {
        // Targeted `->` lookups over the compile-time-constant field list,
        // rather than `jsonb_each` materialising a tuple for every key of
        // `data` (timestamps, ids, everything) only to discard most of them.
        let sql = format!(
            "SELECT key_id, COUNT(*)::BIGINT AS row_count \
             FROM ( \
                 SELECT e.id AS event_row_id, \
                        COALESCE(f.value ->> 'kid', $2) AS key_id \
                 FROM harvest_events e \
                 CROSS JOIN LATERAL unnest($1::TEXT[]) AS k(field) \
                 CROSS JOIN LATERAL (SELECT e.event_data -> 'data' -> k.field) AS f(value) \
                 WHERE f.value IS NOT NULL AND {ENVELOPE_PREDICATE} \
                 GROUP BY e.id, COALESCE(f.value ->> 'kid', $2) \
             ) s \
             GROUP BY key_id"
        );
        let field_keys: Vec<String> = PAYLOAD_FIELD_KEYS
            .iter()
            .map(|k| (*k).to_string())
            .collect();
        let rows: Vec<KeyCountRow> = diesel::sql_query(sql)
            .bind::<Array<Text>, _>(field_keys)
            .bind::<Text, _>(crate::payload_codec::CODEC_LEGACY_KEY_ID)
            .load(conn)
            .await
            .map_err(database_error)?;
        Ok(rows
            .into_iter()
            .map(|row| (row.key_id, row.row_count))
            .collect())
    }

    /// Read one shard's rotation progress: the per-key census plus the active
    /// key's resume cursor (issue #948, AC7).
    ///
    /// Read-only — it never mutates a row.
    ///
    /// # Errors
    ///
    /// Propagates database failures.
    pub async fn load_shard_rotation_progress(
        conn: &mut AsyncPgConnection,
        shard_id: i32,
        codecs: &PayloadCodecs,
    ) -> HarvestResult<ShardRotationProgress> {
        let active_key_id = codecs.active_key_id();
        load_shard_rotation_progress_against(conn, shard_id, codecs, &active_key_id).await
    }

    /// [`load_shard_rotation_progress`], classifying against a **caller-pinned**
    /// active key rather than whatever the registry holds right now.
    ///
    /// A multi-shard report reads each shard on its own connection, so reading
    /// the active key per shard lets a concurrent
    /// [`PayloadCodecs::set_active_key`] straddle the fan-out: shards observed
    /// before the flip classify their rows against the outgoing key and count
    /// them as converted, shards observed after classify the same key's rows as
    /// remaining, and the aggregate advertises whichever key was active when it
    /// was assembled. The dangerous reading is `rows_remaining_total: 0` beside
    /// the *new* `active_key_id` while rows under the old key are still out
    /// there -- precisely what the runbook tells an operator means "safe to
    /// retire".
    ///
    /// Pinning does not make the census transactional across shards -- rows can
    /// still be written while the fan-out runs -- but it removes the one
    /// inconsistency that turns a partial observation into a *wrong* one rather
    /// than merely a stale one. The retirement gate does its own census and
    /// does not rely on this report.
    ///
    /// # Errors
    ///
    /// Propagates [`crate::error::HarvestError::Database`] from the census or
    /// the cursor read.
    pub async fn load_shard_rotation_progress_against(
        conn: &mut AsyncPgConnection,
        shard_id: i32,
        codecs: &PayloadCodecs,
        active_key_id: &str,
    ) -> HarvestResult<ShardRotationProgress> {
        // With no keyed codec registered -- every deployment that has not
        // adopted rotation -- the answer is trivially "nothing to rotate", and
        // running a sequential scan of the largest table to say so would let an
        // unauthenticated-to-the-DB dashboard poll turn an admin GET into
        // repeated full-table reads on every shard.
        if !codecs.has_keyed_codecs() {
            return Ok(ShardRotationProgress {
                active_key_id: active_key_id.to_string(),
                rows_by_key_id: BTreeMap::new(),
                cursor: None,
            });
        }
        let rows_by_key_id = count_rows_by_key_id(conn).await?;
        // Scope the cursor to the key being reported, exactly as the sweep
        // scopes the cursor it resumes from. `ShardRotationProgress::cursor` is
        // documented as the resume cursor for the *active key's* pass, and
        // between a flip and that shard's next tick the stored row still
        // belongs to the previous key. Reporting it here would put a completed
        // cursor beside the new `active_key_id`, which reads as a finished
        // rotation that has not started -- to a dashboard, and to the operator
        // the runbook sends to this endpoint.
        let cursor = if cursor_table_present(conn).await? {
            load_cursor(conn, shard_id)
                .await?
                .filter(|cursor| cursor.active_key_id == active_key_id)
        } else {
            None
        };
        Ok(ShardRotationProgress {
            active_key_id: active_key_id.to_string(),
            rows_by_key_id,
            cursor,
        })
    }

    async fn load_cursor(
        conn: &mut AsyncPgConnection,
        shard_id: i32,
    ) -> HarvestResult<Option<CodecRotationCursor>> {
        let rows: Vec<CursorRow> = diesel::sql_query(
            "SELECT active_key_id, last_event_id, rows_reencrypted, unresolved_rows, \
                    completed_at, updated_at \
             FROM harvest_codec_rotation_cursor WHERE shard_id = $1",
        )
        .bind::<Integer, _>(shard_id)
        .load(conn)
        .await
        .map_err(database_error)?;
        Ok(rows.into_iter().next().map(|row| CodecRotationCursor {
            active_key_id: row.active_key_id,
            last_event_id: row.last_event_id,
            rows_reencrypted: row.rows_reencrypted,
            unresolved_rows: row.unresolved_rows,
            completed_at: row.completed_at,
            updated_at: row.updated_at,
        }))
    }

    /// Run ONE bounded batch of the lazy re-encryption sweep on this
    /// connection's shard (issue #948, AC4).
    ///
    /// Returns the number of rows whose stored bytes were rewritten.
    ///
    /// **Zero cost when unused.** With no keyed codec registered — every
    /// deployment that has not adopted rotation — this returns without issuing
    /// a single statement.
    ///
    /// **Resumable.** Progress is a durable `(shard_id, active_key_id)` cursor,
    /// so a restart continues where the last batch stopped and a key flip starts
    /// a fresh pass automatically.
    ///
    /// **Idempotent.** A row already on the active key is skipped, so re-running
    /// over swept ground rewrites nothing.
    ///
    /// **Rate-limitable.** `batch_limit` bounds rows examined per call; `<= 0`
    /// disables the sweep entirely without a redeploy.
    ///
    /// ## Racing writers
    ///
    /// The write is a **compare-and-swap**:
    /// `UPDATE harvest_events SET event_data = $new WHERE id = $1 AND event_data = $old`.
    /// If anything changed the row between the read and the write — PII erasure
    /// (#495) tombstoning it, or a second sweeper — the update matches zero
    /// rows and this sweep counts it unresolved and skips it. The sweep always
    /// loses such a race, which is the only safe direction: re-writing
    /// ciphertext over a tombstone would resurrect payload data an erasure had
    /// just destroyed.
    ///
    /// A row that cannot be decoded is logged (by id only, never content) and
    /// left byte-identical, so one unreadable row cannot wedge the whole pass —
    /// and it keeps the retirement gate correctly blocked.
    ///
    /// ## Racing retirement (issue #1251)
    ///
    /// The batch pins its target key
    /// ([`PayloadCodecs::pin_key_for_sweep`]) before writing a single row.
    /// Without the pin, a second rotation could move the active key away
    /// from this batch's target while it still ran. `retire_codec_key`
    /// could then drop the target on a zero census. The batch had not
    /// committed any row under it yet, so the census looked clean.
    /// The pin makes that retirement fail closed for as long as the batch
    /// runs. This batch can then never commit a row under a key that
    /// stopped being registered.
    ///
    /// # Errors
    ///
    /// Propagates database failures. A per-row codec failure is not an error.
    // The batch loop, the unresolved-vs-complete decision and the
    // cursor-churn guard are one transaction's worth of reasoning; splitting
    // them would hide the fact that the cursor written at the end depends on
    // what every branch above decided.
    #[allow(clippy::too_many_lines)]
    pub async fn sweep_codec_reencryption_once(
        conn: &mut AsyncPgConnection,
        shard_id: i32,
        codecs: &PayloadCodecs,
        batch_limit: i64,
        metrics: &(dyn MetricsRecorder + Send + Sync),
    ) -> HarvestResult<usize> {
        if batch_limit <= 0 || !codecs.has_keyed_codecs() || active_key_would_decrypt(codecs) {
            return Ok(0);
        }
        if !cursor_table_present(conn).await? {
            return Ok(0);
        }
        let shard = crate::types::ShardId::new(shard_id);
        // Resolve the target key ONCE for the whole batch. Every row is
        // re-encoded under this exact key id, so a concurrent `set_active_key`
        // cannot straddle a row, and the progress we file below is attributed to
        // the key we actually converted onto.
        let active_key_id = codecs.active_key_id();
        // Pin the target for the whole batch (issue #1251). Without this, a
        // second rotation can move `active_key_id` off this batch's target
        // while the batch is still running. Retirement can then drop that
        // target on a zero census, because its own rows have not committed
        // yet. The pin makes retirement of `active_key_id` fail closed
        // instead, for as long as this guard lives. This binds to the whole
        // function, including every `?` early return below, by RAII rather
        // than an explicit unpin.
        let _target_pin = codecs.pin_key_for_sweep(&active_key_id);
        let previous = load_cursor(conn, shard_id).await?;
        // A cursor recorded against a DIFFERENT key is stale: rotating, rotating
        // again, and ROLLING BACK all mean "rescan this shard from the start".
        // Rollback is the case that makes this load-bearing — the key being
        // rolled back to may have completed a pass of its own long ago, and
        // resuming that pass would skip every row written under the key being
        // rolled back from.
        let resumed = previous
            .as_ref()
            .filter(|cursor| cursor.active_key_id == active_key_id);
        let resume_from = resumed.map_or(0, |cursor| cursor.last_event_id);

        let rows: Vec<EventRow> = diesel::sql_query(
            "SELECT id, event_data FROM harvest_events \
             WHERE id > $1 ORDER BY id LIMIT $2",
        )
        .bind::<BigInt, _>(resume_from)
        .bind::<BigInt, _>(batch_limit)
        .load(conn)
        .await
        .map_err(database_error)?;

        let batch_len = rows.len();
        let reached_end = i64::try_from(rows.len()).unwrap_or(i64::MAX) < batch_limit;
        let highest_id = rows.last().map_or(resume_from, |row| row.id);

        let mut rewritten = 0usize;
        let mut unresolved = 0i64;
        for row in rows {
            // Cheap pre-check before the deep clone: the steady state is a
            // fully-converted shard where every fetched row is already on the
            // active key, and cloning a multi-kilobyte event JSON to discover
            // that is pure waste.
            if !has_non_active_key(&row.event_data, &active_key_id) {
                continue;
            }
            let original = row.event_data;
            let mut candidate = original.clone();
            match reencrypt_event_payload_fields_under(codecs, &active_key_id, &mut candidate) {
                Ok(outcome) if outcome.changed() => {
                    if compare_and_swap_event(conn, shard, row.id, &original, &candidate).await? {
                        rewritten += 1;
                    } else {
                        // The row changed under us — a PII erasure, or another
                        // sweeper. We lose, by design. Count it so the pass does
                        // not report itself complete over a row it never
                        // converted.
                        unresolved += 1;
                        tracing::debug!(
                            event_row_id = row.id,
                            shard_id,
                            "codec re-encryption skipped: the event row changed under the sweep"
                        );
                    }
                }
                Ok(_) => {}
                Err(error) => {
                    unresolved += 1;
                    // Bounded and content-free: the row id, the operator-chosen
                    // key ids the row references, and a fixed category for the
                    // failure. Never a payload, never ciphertext — and never
                    // the error's own text, which for a decode failure comes
                    // from an embedder-supplied codec and is unbounded (see
                    // `sweep_error_kind`). Naming the key ids is what makes the
                    // log actionable — the usual cause is a key dropped from
                    // the registry before its rows were converted, and this
                    // says which one to put back.
                    tracing::warn!(
                        event_row_id = row.id,
                        shard_id,
                        key_ids = ?super::event_key_ids(&original),
                        error_kind = super::sweep_error_kind(&error),
                        "codec re-encryption skipped: the event row could not be decoded"
                    );
                }
            }
        }

        let carried = resumed.map_or((0, 0), |cursor| {
            (cursor.rows_reencrypted, cursor.unresolved_rows)
        });
        let rows_reencrypted_total = carried
            .0
            .saturating_add(i64::try_from(rewritten).unwrap_or(i64::MAX));
        let unresolved_total = carried.1.saturating_add(unresolved);

        // A pass that ran off the end of the shard having left something
        // unconverted must NOT be marked complete and must NOT leave those rows
        // behind the cursor forever: reset to 0 so they get another attempt.
        // That is what makes a transient failure — a key registered late, a race
        // lost to an erasure — eventually consistent rather than permanent.
        //
        // `unresolved_total` only counts rows this pass SAW and could not
        // convert. It cannot speak for a row the pass never saw, and there is a
        // way for one to exist: `harvest_events.id` is a `BIGSERIAL`, so an
        // INSERT allocates its id before it commits. A scan can pass an id that
        // is not yet visible and leave that row below the cursor once it
        // commits, where `WHERE id > $1` will never look again. Nothing counted
        // it, so the pass would look clean and record `completed_at` — and the
        // census would report the outgoing key forever while `retire_codec_key`
        // refused forever. A deadlock of the whole procedure, needing a manual
        // cursor reset to escape.
        //
        // So completion is confirmed against the census rather than inferred
        // from having reached the end. The census is the same count the
        // retirement gate trusts, which is the point: "complete" now means the
        // thing the runbook tells an operator it means. It costs one aggregate
        // per *completing* pass, not per batch, and only on a shard that would
        // otherwise have declared itself done.
        // On a shard that has already converged, EVERY tick reaches the end
        // with nothing unresolved -- the batch is empty because there is
        // nothing past the cursor. Censusing on that condition alone would run
        // a full scan of `harvest_events` per shard per tick, forever, at the
        // scanner's interval (500 ms by default). So an already-complete cursor
        // re-confirms itself on a slow clock instead, using its own
        // `updated_at` as the timer.
        //
        // The clock has to be the stored one, not an in-process one: several
        // workers sweep the same shard, and a DB-side timestamp makes the
        // revalidation fleet-wide rather than once per worker.
        let already_complete = resumed.is_some_and(|cursor| cursor.completed_at.is_some());
        // Census when a pass is newly completing, and on a converged shard only
        // when this worker wins the interval's claim. Skipping it in the steady
        // state is the whole point; skipping it forever is not, because a row
        // that commits below the cursor is only ever found this way.
        let census_needed = if reached_end && unresolved_total == 0 {
            if already_complete {
                claim_completed_cursor_revalidation(conn, shard).await?
            } else {
                true
            }
        } else {
            false
        };
        let census_clean = if census_needed {
            let by_key = count_rows_by_key_id(conn).await?;
            by_key
                .iter()
                .filter(|(key_id, _)| *key_id != &active_key_id)
                .all(|(_, count)| *count == 0)
        } else {
            false
        };

        let (next_last_event_id, next_unresolved, next_completed_at) = if !reached_end {
            (highest_id, unresolved_total, None)
        } else if unresolved_total > 0 {
            // This pass failed rows outright. Start over rather than declare
            // victory over rows it could not convert.
            (0, 0, None)
        } else if !census_needed {
            // Converged, and the revalidation clock has not come round. Keep
            // the completion stamp -- this branch must not be mistaken for
            // "the census said no", which would reset a perfectly good
            // completion on every tick.
            //
            // But DO advance over what this tick examined. `highest_id` is
            // correct in both cases by construction: the max id read, falling
            // back to the resume point when the batch was empty. Carrying the
            // stored `last_event_id` instead would stand still over an
            // underfilled batch, so a converged shard receiving traffic would
            // re-read and re-deserialize the same rows every tick until the
            // batch filled or the five-minute clock moved it -- read
            // amplification bounded only by `batch_limit`.
            (
                highest_id,
                0,
                resumed.and_then(|cursor| cursor.completed_at),
            )
        } else if census_clean {
            (
                highest_id,
                0,
                resumed
                    .and_then(|cursor| cursor.completed_at)
                    .or_else(|| Some(Utc::now())),
            )
        } else {
            // Something the scan could not see is still on an outgoing key.
            (0, 0, None)
        };

        // Steady state on a converted shard: the pass is already complete and
        // this tick fetched nothing new. Re-upserting the identical row would
        // refresh `updated_at` on every scanner tick, forever, generating WAL
        // and dead tuples per shard for a deployment that simply keeps a keyed
        // codec configured -- and making the cursor read as freshly active
        // while doing no work, which is exactly backwards for an operator
        // watching it.
        //
        // A revalidation tick needs no exception here: the claim statement has
        // already bumped `updated_at`, so a clean re-census writes nothing and
        // a dirty one falls through this guard anyway, its reset changing the
        // fields below.
        let cursor_unchanged = batch_len == 0
            && resumed.is_some_and(|cursor| {
                cursor.completed_at.is_some()
                    && cursor.last_event_id == next_last_event_id
                    && cursor.unresolved_rows == next_unresolved
                    && cursor.rows_reencrypted == rows_reencrypted_total
            });
        if !cursor_unchanged {
            write_cursor(
                conn,
                shard,
                &active_key_id,
                next_last_event_id,
                rows_reencrypted_total,
                next_unresolved,
                next_completed_at,
            )
            .await?;
        }

        if rewritten > 0 {
            metrics.record_codec_reencrypted(
                &shard_id.to_string(),
                u64::try_from(rewritten).unwrap_or(u64::MAX),
            );
        }
        Ok(rewritten)
    }

    /// Write `candidate` over `original` only if the row still holds
    /// `original`. Returns whether the swap took effect.
    ///
    /// This is the single guard that keeps sanctioned exception #3 from
    /// undoing exception #2: if anything changed the row between the sweep's
    /// read and this write — a PII erasure (#495) tombstoning it, or a second
    /// sweeper — the `WHERE` clause matches nothing and the sweep loses. That
    /// is the only safe direction, and the caller counts the loss so the pass
    /// re-runs rather than reporting itself complete over that row.
    ///
    /// It is also fenced against the shard generation (#954). This is the only
    /// path that UPDATEs `harvest_events` in place, so `store.rs`'s fence on
    /// every INSERT does not cover it, and the failure an unfenced sweep allows
    /// is the worst one this feature has: a worker still pinned to the old
    /// generation, reconnected to the promoted primary, would have its appends
    /// refused but its **re-encryption accepted** — rewriting rows the new
    /// region owns under an active key that region may already have retired.
    /// Silent, permanent, and destroys payloads rather than merely forking
    /// history. The assertion and the swap share one transaction so the
    /// assertion's `FOR SHARE` stays a commit-order barrier against a
    /// concurrent promotion.
    ///
    /// The wrapper is skipped entirely when fencing is off, so the pre-#954
    /// path is unchanged: no fence read, no savepoint, one UPDATE.
    ///
    /// `#[doc(hidden)] pub` purely so that race semantics can be exercised
    /// directly by an integration test (a stale `original` must not win), which
    /// is not reachable through the batch-oriented public sweep entry point.
    /// Not part of the engine's semver-stable surface.
    ///
    /// # Errors
    ///
    /// Propagates database failures, and
    /// [`crate::error::HarvestError::ShardFenced`] when this process is pinned
    /// to a superseded generation.
    #[doc(hidden)]
    pub async fn compare_and_swap_event(
        conn: &mut AsyncPgConnection,
        shard: crate::types::ShardId,
        event_row_id: i64,
        original: &Value,
        candidate: &Value,
    ) -> HarvestResult<bool> {
        use diesel_async::AsyncConnection as _;

        if crate::replication::FenceRegistry::is_enabled() {
            let candidate = candidate.clone();
            let original = original.clone();
            return Box::pin(conn.transaction::<bool, HarvestError, _>(async |conn| {
                crate::replication::assert_fence(conn, shard).await?;
                let updated = swap_statement(conn, event_row_id, &original, &candidate).await?;
                Ok(updated)
            }))
            .await;
        }
        swap_statement(conn, event_row_id, original, candidate).await
    }

    async fn swap_statement(
        conn: &mut AsyncPgConnection,
        event_row_id: i64,
        original: &Value,
        candidate: &Value,
    ) -> HarvestResult<bool> {
        let updated = diesel::sql_query(
            "UPDATE harvest_events SET event_data = $1 WHERE id = $2 AND event_data = $3",
        )
        .bind::<Jsonb, _>(candidate)
        .bind::<BigInt, _>(event_row_id)
        .bind::<Jsonb, _>(original)
        .execute(conn)
        .await
        .map_err(database_error)?;
        Ok(updated > 0)
    }

    /// Atomically claim the right to re-census a completed shard.
    ///
    /// Returns `true` for the ONE caller that wins the interval.
    ///
    /// Two properties, both of which a read-then-decide check gets wrong:
    ///
    /// - **The comparison happens on the database clock.** `updated_at` is
    ///   written with `NOW()`, so testing it against the worker's `Utc::now()`
    ///   measures host clock skew as much as elapsed time: a worker running
    ///   five minutes fast finds the cursor due immediately after every
    ///   refresh and censuses on every tick, which is precisely the per-tick
    ///   full scan the throttle exists to stop. A worker running slow silently
    ///   stretches the recovery window instead. `NOW()` on both sides of the
    ///   predicate removes the host clock from the question entirely.
    ///
    /// - **The claim and the bump are one statement.** Several replicas run a
    ///   timeout checker over the same shard. Reading the timestamp, deciding,
    ///   then censusing, then writing, lets every replica pass the check before
    ///   any of them writes -- so a large shard takes one simultaneous
    ///   sequential scan *per replica* every interval, which is worse than the
    ///   unthrottled single-worker case this was meant to fix. `UPDATE ...
    ///   WHERE updated_at < deadline RETURNING` makes exactly one replica win,
    ///   because the row lock serialises them and the loser's predicate no
    ///   longer holds.
    ///
    /// Bumping `updated_at` before the census rather than after is deliberate:
    /// the interval is a rate limit on an expensive scan, not a lease on
    /// completing it. A crash mid-census costs one skipped revalidation, which
    /// the next interval picks up.
    ///
    /// ## Fencing (issue #954, issue #1257)
    ///
    /// Fenced the same way as [`write_cursor`]. Claiming the interval writes
    /// `updated_at`, so a worker pinned to a superseded generation must not
    /// be the one who wins the claim.
    async fn claim_completed_cursor_revalidation(
        conn: &mut AsyncPgConnection,
        shard: crate::types::ShardId,
    ) -> HarvestResult<bool> {
        use diesel_async::AsyncConnection as _;

        if crate::replication::FenceRegistry::is_enabled() {
            return Box::pin(
                conn.transaction::<bool, HarvestError, _>(async move |conn| {
                    crate::replication::assert_fence(conn, shard).await?;
                    claim_completed_cursor_revalidation_statement(conn, shard.as_i32()).await
                }),
            )
            .await;
        }
        claim_completed_cursor_revalidation_statement(conn, shard.as_i32()).await
    }

    async fn claim_completed_cursor_revalidation_statement(
        conn: &mut AsyncPgConnection,
        shard_id: i32,
    ) -> HarvestResult<bool> {
        let claimed = diesel::sql_query(
            "UPDATE harvest_codec_rotation_cursor \
                SET updated_at = NOW() \
              WHERE shard_id = $1 \
                AND completed_at IS NOT NULL \
                AND updated_at < NOW() - make_interval(secs => $2)",
        )
        .bind::<Integer, _>(shard_id)
        .bind::<diesel::sql_types::Double, _>(super::COMPLETED_CURSOR_REVALIDATION_SECS)
        .execute(conn)
        .await
        .map_err(database_error)?;
        Ok(claimed > 0)
    }

    /// Persist the pass's absolute state.
    ///
    /// The value is absolute, not incremental (`SET x = x + n`), because it
    /// comes from one read this same call already did.
    ///
    /// ## The write is a compare-and-swap (issue #1257)
    ///
    /// Two sweepers can read the same cursor row and each compute their own
    /// next state from it. The write that commits last must not silently
    /// overwrite a fresher one -- a lost update moves the cursor backward or
    /// drops rows from `rows_reencrypted`.
    ///
    /// The `WHERE` clause on `DO UPDATE` guards against that. For the same
    /// `active_key_id`, it applies the write only when the new
    /// `rows_reencrypted` is at least the stored value. A write computed
    /// from a stale read fails this check and is dropped, not applied --
    /// mirroring [`compare_and_swap_event`] for `harvest_events`.
    ///
    /// `rows_reencrypted` never gets a rewind exception. Every branch that
    /// resets `last_event_id` still carries `rows_reencrypted` forward from
    /// the same read (see `sweep_codec_reencryption_once`). A stale write
    /// can never legitimately need to lower it.
    ///
    /// `last_event_id` gets two exceptions:
    /// - A different `active_key_id` starts a fresh pass. Its
    ///   `last_event_id` is not comparable to the previous key's, so the
    ///   whole write applies regardless of `rows_reencrypted` too.
    /// - `last_event_id = 0` is the deliberate rewind a pass takes when it
    ///   leaves rows unresolved. That reset must stay possible even over a
    ///   higher stored `last_event_id`. It still needs `rows_reencrypted`
    ///   to pass its own check, or a stale rewind could drop rows from the
    ///   count it carries forward.
    ///
    /// ## Fencing (issue #954, issue #1257)
    ///
    /// Fenced the same way as [`compare_and_swap_event`]. When
    /// [`crate::replication::FenceRegistry::is_enabled`] holds, the write
    /// runs inside a transaction behind [`crate::replication::assert_fence`].
    /// A worker pinned to a superseded generation cannot advance the cursor
    /// this way. That covers a batch that converted no rows too, since such
    /// a batch never reaches the per-row CAS. The fence check is skipped
    /// when fencing is off: one statement, no extra round trip.
    ///
    /// `#[doc(hidden)] pub` so the CAS is directly testable by an
    /// integration test. Not part of the engine's stable API.
    ///
    /// # Errors
    ///
    /// Propagates database failures, and
    /// [`crate::error::HarvestError::ShardFenced`] when this process is
    /// pinned to a superseded generation.
    #[doc(hidden)]
    pub async fn write_cursor(
        conn: &mut AsyncPgConnection,
        shard: crate::types::ShardId,
        active_key_id: &str,
        last_event_id: i64,
        rows_reencrypted: i64,
        unresolved_rows: i64,
        completed_at: Option<DateTime<Utc>>,
    ) -> HarvestResult<bool> {
        use diesel_async::AsyncConnection as _;

        if crate::replication::FenceRegistry::is_enabled() {
            let active_key_id = active_key_id.to_string();
            return Box::pin(
                conn.transaction::<bool, HarvestError, _>(async move |conn| {
                    crate::replication::assert_fence(conn, shard).await?;
                    write_cursor_statement(
                        conn,
                        shard.as_i32(),
                        &active_key_id,
                        last_event_id,
                        rows_reencrypted,
                        unresolved_rows,
                        completed_at,
                    )
                    .await
                }),
            )
            .await;
        }
        write_cursor_statement(
            conn,
            shard.as_i32(),
            active_key_id,
            last_event_id,
            rows_reencrypted,
            unresolved_rows,
            completed_at,
        )
        .await
    }

    async fn write_cursor_statement(
        conn: &mut AsyncPgConnection,
        shard_id: i32,
        active_key_id: &str,
        last_event_id: i64,
        rows_reencrypted: i64,
        unresolved_rows: i64,
        completed_at: Option<DateTime<Utc>>,
    ) -> HarvestResult<bool> {
        let applied = diesel::sql_query(
            "INSERT INTO harvest_codec_rotation_cursor \
                 (shard_id, active_key_id, last_event_id, rows_reencrypted, unresolved_rows, \
                  completed_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, NOW()) \
             ON CONFLICT (shard_id) DO UPDATE SET \
                 active_key_id = EXCLUDED.active_key_id, \
                 last_event_id = EXCLUDED.last_event_id, \
                 rows_reencrypted = EXCLUDED.rows_reencrypted, \
                 unresolved_rows = EXCLUDED.unresolved_rows, \
                 completed_at = EXCLUDED.completed_at, \
                 updated_at = NOW() \
             WHERE harvest_codec_rotation_cursor.active_key_id <> EXCLUDED.active_key_id \
                OR ( \
                     (EXCLUDED.last_event_id = 0 \
                      OR harvest_codec_rotation_cursor.last_event_id <= EXCLUDED.last_event_id) \
                     AND harvest_codec_rotation_cursor.rows_reencrypted \
                         <= EXCLUDED.rows_reencrypted \
                   )",
        )
        .bind::<Integer, _>(shard_id)
        .bind::<Text, _>(active_key_id)
        .bind::<BigInt, _>(last_event_id)
        .bind::<BigInt, _>(rows_reencrypted)
        .bind::<BigInt, _>(unresolved_rows)
        .bind::<Nullable<Timestamptz>, _>(completed_at)
        .execute(conn)
        .await
        .map_err(database_error)?;
        if applied == 0 {
            tracing::debug!(
                shard_id,
                "codec rotation cursor write dropped: a newer pass already advanced this \
                 shard's cursor"
            );
        }
        Ok(applied > 0)
    }

    /// Run one sweep batch for the shard this connection already serves
    /// (issue #948).
    ///
    /// Folded into [`crate::timeout::enforce_timeouts_once`], and deliberately
    /// **shard-local** — it sweeps through the connection it is handed and never
    /// acquires another.
    ///
    /// That is not just an optimisation, it is a deadlock fix.
    /// `spawn_timeout_checker_for_shard` holds its shard pool's connection for
    /// the whole `enforce_timeouts_once` call. Harvest configures no deadpool
    /// `Timeouts`, so every `pool.get().await` is an **unbounded** wait (see
    /// `worker::shard_acquire_bound`) — a sweep that reached back into the same
    /// pool for a second connection would park forever on a single-connection
    /// pool, wedging not just rotation but every later resident of that tick:
    /// timeout enforcement, broken-session reclaim, mutex-lease reclaim, and the
    /// scanner-liveness heartbeat.
    ///
    /// Shard-local costs nothing in coverage: a multi-shard worker spawns one
    /// timeout checker per assigned shard (`monitor_shard_scope` narrows each to
    /// its own), so every shard is swept by its own checker on its own
    /// connection. Unlike the throttle / debounce / start-idempotency residents,
    /// this sweep has no cross-shard work to route — a row is rewritten on the
    /// shard that stores it.
    ///
    /// `shard_assignments` is used only to attribute progress to the right shard
    /// id; the connection decides which rows are actually swept.
    ///
    /// # Errors
    ///
    /// Propagates database failures.
    pub async fn sweep_codec_reencryption(
        conn: &mut AsyncPgConnection,
        shard_assignments: &[crate::types::ShardId],
        codecs: &PayloadCodecs,
        batch_limit: i64,
        metrics: &(dyn MetricsRecorder + Send + Sync),
    ) -> HarvestResult<usize> {
        if batch_limit <= 0 || !codecs.has_keyed_codecs() || active_key_would_decrypt(codecs) {
            return Ok(0);
        }
        let shard_id = shard_assignments.first().map_or(0, |s| s.as_i32());
        sweep_codec_reencryption_once(conn, shard_id, codecs, batch_limit, metrics).await
    }

    // ── Durable fleet-wide key state (issue #1244) ───────────────────────

    #[derive(diesel::QueryableByName)]
    struct KeyStateRow {
        #[diesel(sql_type = Text)]
        state: String,
        #[diesel(sql_type = Nullable<Timestamptz>)]
        retiring_since: Option<DateTime<Utc>>,
        /// Seconds since `retiring_since`, computed entirely on this shard's
        /// own database clock. `NULL` when `retiring_since` is `NULL`.
        ///
        /// [`staleness_gate`] uses this, never `Utc::now() -
        /// retiring_since`. Comparing a DB-stamped timestamp against the
        /// *caller's* host clock is only correct when the two clocks agree.
        ///
        /// A retirement host whose clock runs ahead of a shard's database
        /// would otherwise see an inflated `elapsed`. It could pass the gate
        /// before that shard's own live workers had actually had
        /// `staleness_window` to refresh.
        #[diesel(sql_type = Nullable<Double>)]
        elapsed_secs: Option<f64>,
    }

    #[derive(diesel::QueryableByName)]
    struct BlockingWorkerRow {
        #[diesel(sql_type = Text)]
        worker_id: String,
        #[diesel(sql_type = Text)]
        host: String,
        #[diesel(sql_type = Text)]
        reason: String,
    }

    #[derive(diesel::QueryableByName)]
    struct ActiveKeyRow {
        #[diesel(sql_type = Text)]
        key_id: String,
    }

    /// Read `key_id`'s durable lifecycle row on this connection's shard
    /// (issue #1244). `None` when the key has never been activated here.
    async fn read_key_state(
        conn: &mut AsyncPgConnection,
        key_id: &str,
    ) -> HarvestResult<Option<KeyStateRow>> {
        let rows: Vec<KeyStateRow> = diesel::sql_query(
            "SELECT state, retiring_since, \
                    EXTRACT(EPOCH FROM (NOW() - retiring_since))::float8 AS elapsed_secs \
               FROM harvest_codec_key_state WHERE key_id = $1",
        )
        .bind::<Text, _>(key_id)
        .load(conn)
        .await
        .map_err(database_error)?;
        Ok(rows.into_iter().next())
    }

    /// Durably record `key_id` becoming the active key on this connection's
    /// shard (issue #1244). Any other row still marked `active` is demoted to
    /// `retiring` with `retiring_since = NOW()`, in the same transaction. So
    /// there is never a moment with zero or two active rows.
    ///
    /// `outgoing_key_id` is `codecs.active_key_id()` as read *before* this
    /// call -- the key this process considered active a moment ago.
    ///
    /// A first activation ever, on an empty `harvest_codec_key_state`, has
    /// no durable `"active"` row for the UPDATE above to demote. The
    /// in-memory outgoing key was never durable to begin with. Left alone,
    /// that key would carry no `"retiring"` row forever, and
    /// [`retire_codec_key`]'s structural gate would refuse it permanently.
    ///
    /// So when that UPDATE demotes nothing, this seeds a `"retiring"` row
    /// for `outgoing_key_id` directly. It uses `ON CONFLICT DO NOTHING`: a
    /// row already there, in any state, is a fact this call must not
    /// overwrite.
    async fn write_key_state_activation(
        conn: &mut AsyncPgConnection,
        key_id: &str,
        outgoing_key_id: &str,
    ) -> HarvestResult<()> {
        conn.transaction::<(), HarvestError, _>(async |conn| {
            let demoted = diesel::sql_query(
                "UPDATE harvest_codec_key_state \
                    SET state = 'retiring', retiring_since = NOW(), updated_at = NOW() \
                  WHERE state = 'active' AND key_id <> $1",
            )
            .bind::<Text, _>(key_id)
            .execute(conn)
            .await
            .map_err(database_error)?;

            if demoted == 0 && outgoing_key_id != key_id {
                diesel::sql_query(
                    "INSERT INTO harvest_codec_key_state \
                         (key_id, state, retiring_since, updated_at) \
                     VALUES ($1, 'retiring', NOW(), NOW()) \
                     ON CONFLICT (key_id) DO NOTHING",
                )
                .bind::<Text, _>(outgoing_key_id)
                .execute(conn)
                .await
                .map_err(database_error)?;
            }

            diesel::sql_query(
                "INSERT INTO harvest_codec_key_state (key_id, state, activated_at, updated_at) \
                     VALUES ($1, 'active', NOW(), NOW()) \
                 ON CONFLICT (key_id) DO UPDATE SET \
                     state = 'active', activated_at = NOW(), retiring_since = NULL, \
                     retired_at = NULL, updated_at = NOW()",
            )
            .bind::<Text, _>(key_id)
            .execute(conn)
            .await
            .map_err(database_error)?;
            Ok(())
        })
        .await
    }

    /// Durably record `key_id` as retired on this connection's shard (issue
    /// #1244). Best-effort bookkeeping: the in-memory
    /// [`PayloadCodecs::retire_key_local`] drop is what actually stops this
    /// process encoding or decoding with the key. A failure here is logged by
    /// the caller rather than un-retiring the key.
    async fn write_key_state_retired(
        conn: &mut AsyncPgConnection,
        key_id: &str,
    ) -> HarvestResult<()> {
        diesel::sql_query(
            "UPDATE harvest_codec_key_state \
                SET state = 'retired', retired_at = NOW(), updated_at = NOW() \
              WHERE key_id = $1",
        )
        .bind::<Text, _>(key_id)
        .execute(conn)
        .await
        .map_err(database_error)?;
        Ok(())
    }

    /// Live workers on this connection's shard that cannot safely read or
    /// write under `key_id` (issue #1244).
    ///
    /// Blocks on either of two independent conditions:
    ///
    /// 1. The worker does not advertise support for the keyed (version-2)
    ///    codec envelope, so it cannot even parse a `kid`-bearing payload.
    /// 2. The worker's binary does support version 2, but its own
    ///    `PayloadCodecs` does not have `key_id` registered. It cannot
    ///    decode a payload some other worker encoded under it.
    ///
    /// A staggered rollout can deploy the new binary fleet-wide before the
    /// new key's material reaches every worker's config. So condition 2 is
    /// not implied by condition 1.
    ///
    /// "Live" mirrors [`crate::worker::worker_stale_secs`]'s convention: a
    /// heartbeat within `worker_stale_secs`. `labels` is an operator-facing
    /// JSONB column, so its value is never trusted to parse. Both checks
    /// treat a malformed or absent label as failing — fail closed, not a
    /// query error.
    #[allow(clippy::cast_precision_loss)] // a liveness window in seconds never approaches 2^53
    async fn blocking_workers(
        conn: &mut AsyncPgConnection,
        worker_stale_secs: i64,
        key_id: &str,
    ) -> HarvestResult<Vec<BlockingWorkerRow>> {
        diesel::sql_query(
            "SELECT worker_id, host, \
                    CASE WHEN COALESCE( \
                                 CASE WHEN labels ->> $2 ~ '^[0-9]+$' \
                                      THEN (labels ->> $2)::bigint \
                                      ELSE NULL END, \
                                 1 \
                               ) < $3 \
                         THEN 'cannot read envelope version 2' \
                         ELSE 'codec key ' || $4 || ' is not registered on this worker' \
                    END AS reason \
               FROM harvest_workers \
              WHERE status <> 'Stopped' \
                AND NOW() - last_heartbeat_at <= make_interval(secs => $1::float8) \
                AND ( \
                      COALESCE( \
                        CASE WHEN labels ->> $2 ~ '^[0-9]+$' \
                             THEN (labels ->> $2)::bigint \
                             ELSE NULL END, \
                        1 \
                      ) < $3 \
                      OR NOT COALESCE(labels -> $5 ? $4, false) \
                    )",
        )
        .bind::<Double, _>(worker_stale_secs as f64)
        .bind::<Text, _>(crate::payload_codec::CODEC_ENVELOPE_CAPABILITY_LABEL)
        .bind::<BigInt, _>(crate::payload_codec::CODEC_ENVELOPE_VERSION_KEYED)
        .bind::<Text, _>(key_id)
        .bind::<Text, _>(crate::payload_codec::CODEC_REGISTERED_KEY_IDS_LABEL)
        .load(conn)
        .await
        .map_err(database_error)
    }

    /// Wrap [`blocking_workers`]'s rows as [`CodecKeyActivationBlocker`]s for
    /// one shard (issue #1244).
    fn blockers_from_rows(
        shard_id: i32,
        rows: Vec<BlockingWorkerRow>,
    ) -> Vec<CodecKeyActivationBlocker> {
        rows.into_iter()
            .map(|r| CodecKeyActivationBlocker {
                shard_id,
                worker_id: Some(r.worker_id),
                host: Some(r.host),
                reachable: true,
                reason: Some(r.reason),
            })
            .collect()
    }

    /// This connection's shard's current fleet-wide active key, per
    /// `harvest_codec_key_state` (issue #1244). `None` when no row is marked
    /// `active` yet (a fresh deployment that has never called
    /// [`activate_codec_key`]).
    async fn active_key_row(conn: &mut AsyncPgConnection) -> HarvestResult<Option<String>> {
        let rows: Vec<ActiveKeyRow> = diesel::sql_query(
            "SELECT key_id FROM harvest_codec_key_state WHERE state = 'active' LIMIT 1",
        )
        .load(conn)
        .await
        .map_err(database_error)?;
        Ok(rows.into_iter().next().map(|r| r.key_id))
    }

    /// Resolve a connection to `shard` within `sharded_pool`, or the reason it
    /// could not be reached (issue #1244).
    ///
    /// A missing pool or a failed acquisition must fail closed, never skip the
    /// shard silently. Shared by every per-shard loop in this module that
    /// needs that guarantee: [`census_remaining`], [`staleness_gate`], and
    /// [`activate_codec_key`]'s capability check.
    async fn resolve_shard_conn(
        sharded_pool: &crate::shard::ShardedDbPool,
        shard: crate::types::ShardId,
    ) -> Result<
        deadpool::managed::Object<
            diesel_async::pooled_connection::AsyncDieselConnectionManager<AsyncPgConnection>,
        >,
        String,
    > {
        let Some(pool) = sharded_pool.exact_pool_for(shard) else {
            return Err("no connection pool for this shard in this process".to_string());
        };
        pool.get()
            .await
            .map_err(|e| format!("connection unavailable: {e}"))
    }

    /// One census pass: `key_id`'s remaining `harvest_events` rows per
    /// expected shard (issue #948, AC6). Never checks `expected_shards`
    /// completeness against `sharded_pool` — callers that need the
    /// fail-closed omission guard run it once, before any census pass.
    async fn census_remaining(
        sharded_pool: &crate::shard::ShardedDbPool,
        expected_shards: &[crate::types::ShardId],
        key_id: &str,
    ) -> Vec<CodecKeyShardRemainder> {
        let mut remaining = Vec::new();
        for shard in expected_shards {
            let shard_id = shard.as_i32();
            let mut conn = match resolve_shard_conn(sharded_pool, *shard).await {
                Ok(c) => c,
                Err(reason) => {
                    remaining.push(CodecKeyShardRemainder {
                        shard_id,
                        rows: 0,
                        reachable: false,
                        reason: Some(reason),
                    });
                    continue;
                }
            };
            match count_rows_by_key_id(&mut conn).await {
                Ok(counts) => {
                    let rows = counts.get(key_id).copied().unwrap_or(0);
                    if rows > 0 {
                        remaining.push(CodecKeyShardRemainder {
                            shard_id,
                            rows,
                            reachable: true,
                            reason: None,
                        });
                    }
                }
                Err(e) => remaining.push(CodecKeyShardRemainder {
                    shard_id,
                    rows: 0,
                    reachable: false,
                    reason: Some(format!("census failed: {e}")),
                }),
            }
        }
        remaining
    }

    /// Escape hatch for [`retire_codec_key`]'s structural write fence (issue
    /// #1244; formerly the *only* fence, issue #948).
    ///
    /// # Why the census alone cannot establish the fence
    ///
    /// [`PayloadCodecs`] is a **per-process** registry: `set_active_key` on one
    /// worker is invisible to every other worker. The census counts rows that
    /// are *committed and visible* on the shards this process can reach, at one
    /// instant. Neither of the two ways a new old-key row can still appear is
    /// observable to it:
    ///
    /// 1. **Another live writer.** A worker that has not yet observed the new
    ///    active key keeps encoding under the old one. It writes another
    ///    old-key row a millisecond after the census reads zero.
    /// 2. **An in-flight append.** A transaction that already encoded its
    ///    payload under the old key, but has not committed, is invisible to the
    ///    census and becomes visible immediately afterwards.
    ///
    /// The default path (`NotConfirmed`) narrows hazard 1 structurally. It
    /// requires every expected shard's durable `harvest_codec_key_state` row to
    /// have named this key `"retiring"` for at least `staleness_window`.
    /// [`activate_codec_key`] stamps that row the instant a newer key becomes
    /// active.
    ///
    /// Every other process refreshes its own view of the active key.
    /// Roughly every scanner-tick interval, see
    /// [`refresh_active_codec_key`], **provided that process's scanner
    /// loop is actually ticking on schedule.**
    ///
    /// That proviso is not free. [`crate::timeout::spawn_timeout_checker_for_shard`]
    /// bounds its own pool acquisition to one `interval`, so pool contention
    /// alone cannot stretch a tick unboundedly. It does **not** bound the
    /// enforcement pass itself: a single slow or wedged query inside that
    /// pass can still delay the refresh beyond `interval`.
    ///
    /// `staleness_window` is an operational margin, set generously past the
    /// deployment's nominal `interval` for exactly this reason. It is not a
    /// proof immune to a stuck scanner. A scanner loop that has stopped
    /// ticking entirely is `crate::scanner_health`'s job to surface, separately from
    /// this gate.
    ///
    /// This path narrows hazard 2 with an immediate recensus before
    /// finalizing (see `retire_codec_key`'s `recheck_delay`). It cannot close
    /// hazard 2 completely without tracking individual transaction lifetimes
    /// — a known, documented limitation, not a silent gap.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum FleetWriteFence {
        /// Skip the durable staleness-window wait and go straight to the
        /// census. For single-process embedders and tests where "another live
        /// writer" cannot exist by construction — the operator is attesting to
        /// exactly that. The census (and its recheck) still has to come back
        /// zero on top of this.
        ConfirmedByOperator,
        /// Use the structural gate: every expected shard's durable key state
        /// must show `key_id` `"retiring"` for at least `staleness_window`
        /// before the census even runs.
        NotConfirmed,
    }

    /// The four preconditions decidable *without* touching the database,
    /// split out of [`retire_codec_key`] so the gate itself reads as
    /// validate-then-fence-then-census.
    ///
    /// Each check refuses one way to report a vacuous success:
    ///
    /// - retiring the active key;
    /// - retiring a key an in-flight sweep batch is still writing onto;
    /// - proving zero over no shards at all;
    /// - proving zero for a key this process never registered.
    ///
    /// The pin check here is a fast-fail only — it saves the per-shard census
    /// when the answer is already known. It is not what makes retirement
    /// race-safe. [`PayloadCodecs::retire_key_local`] repeats the same check
    /// under its write lock, atomically with removing the key. That repeated
    /// check is what actually closes issue #1251's race.
    fn validate_retirement_request(
        expected_shards: &[crate::types::ShardId],
        codecs: &PayloadCodecs,
        key_id: &str,
    ) -> HarvestResult<()> {
        if codecs.active_key_id() == key_id {
            return Err(HarvestError::Config(format!(
                "codec key id {key_id:?} is the active key and cannot be retired; \
                 activate a different key first"
            )));
        }
        if codecs.is_pinned_by_sweep(key_id) {
            return Err(HarvestError::Config(format!(
                "codec key id {key_id:?} is pinned by an in-flight re-encryption sweep batch \
                 and cannot be retired yet; retry once the batch completes"
            )));
        }
        if expected_shards.is_empty() {
            return Err(HarvestError::Config(format!(
                "cannot retire codec key id {key_id:?}: no shards were supplied to inspect, so \
                 zero remaining rows cannot be established"
            )));
        }
        if codecs.codec_for_key(key_id).is_none() {
            return Err(HarvestError::Config(format!(
                "codec key id {key_id:?} is not registered; retiring it would report success \
                 without having proved anything about the rows that reference it"
            )));
        }
        Ok(())
    }

    /// The structural half of [`FleetWriteFence::NotConfirmed`] (issue #1244).
    /// Every expected shard must show `key_id` durably `"retiring"`. The
    /// least-aged of those transitions (the straggling shard sets the pace)
    /// must be at least `staleness_window` in the past.
    ///
    /// Elapsed time is computed on each shard's own database clock, never
    /// the caller's host clock -- see [`KeyStateRow::elapsed_secs`].
    async fn staleness_gate(
        sharded_pool: &crate::shard::ShardedDbPool,
        expected_shards: &[crate::types::ShardId],
        key_id: &str,
        staleness_window: Duration,
    ) -> HarvestResult<()> {
        let mut remaining: Vec<CodecKeyShardRemainder> = Vec::new();
        // The binding constraint is the LEAST-elapsed shard. Every shard's
        // own window must have elapsed, so the whole gate is only as stale
        // as its freshest transition. Kept alongside its own
        // `retiring_since` purely for the error message below.
        let mut least_elapsed: Option<(f64, DateTime<Utc>)> = None;
        for shard in expected_shards {
            let shard_id = shard.as_i32();
            let mut conn = match resolve_shard_conn(sharded_pool, *shard).await {
                Ok(c) => c,
                Err(reason) => {
                    remaining.push(CodecKeyShardRemainder {
                        shard_id,
                        rows: 0,
                        reachable: false,
                        reason: Some(reason),
                    });
                    continue;
                }
            };
            match read_key_state(&mut conn, key_id).await {
                Ok(Some(row)) if row.state == "retiring" => {
                    let since = row.retiring_since.unwrap_or_else(Utc::now);
                    // A `NULL` `elapsed_secs` (retiring_since somehow NULL on a
                    // "retiring" row) fails closed as zero elapsed, never as
                    // "already stale enough".
                    let elapsed_secs = row.elapsed_secs.unwrap_or(0.0);
                    let is_new_min =
                        least_elapsed.is_none_or(|(min_secs, _)| elapsed_secs < min_secs);
                    if is_new_min {
                        least_elapsed = Some((elapsed_secs, since));
                    }
                }
                Ok(Some(row)) => remaining.push(CodecKeyShardRemainder {
                    shard_id,
                    rows: 0,
                    reachable: true,
                    reason: Some(format!(
                        "durable key state on this shard is {:?}, not \"retiring\"; call \
                         activate_codec_key with a different key first",
                        row.state
                    )),
                }),
                Ok(None) => remaining.push(CodecKeyShardRemainder {
                    shard_id,
                    rows: 0,
                    reachable: true,
                    reason: Some(
                        "no durable key state on this shard yet; call activate_codec_key with a \
                         different key first so this key transitions to \"retiring\""
                            .to_string(),
                    ),
                }),
                Err(e) => remaining.push(CodecKeyShardRemainder {
                    shard_id,
                    rows: 0,
                    reachable: false,
                    reason: Some(format!("key state read failed: {e}")),
                }),
            }
        }
        if !remaining.is_empty() {
            return Err(HarvestError::CodecKeyRetirementBlocked {
                key_id: key_id.to_string(),
                remaining,
            });
        }
        // `remaining` is empty, so every expected shard matched the `Ok(Some(_))`
        // "retiring" arm above and contributed to `least_elapsed`.
        let (elapsed_secs, since) =
            least_elapsed.expect("every shard reached the retiring arm when `remaining` is empty");
        let window_secs = staleness_window.as_secs_f64();
        if elapsed_secs < window_secs {
            // A staleness window measured in seconds never remotely
            // approaches `i64::MAX`.
            #[allow(clippy::cast_possible_truncation)]
            let remaining_secs = (window_secs - elapsed_secs).round().max(0.0) as i64;
            return Err(HarvestError::CodecKeyRetirementBlocked {
                key_id: key_id.to_string(),
                remaining: expected_shards
                    .iter()
                    .map(|s| CodecKeyShardRemainder {
                        shard_id: s.as_i32(),
                        rows: 0,
                        reachable: true,
                        reason: Some(format!(
                            "retirement staleness window not yet elapsed: {remaining_secs}s \
                             remaining (least-aged shard retiring since {since})"
                        )),
                    })
                    .collect(),
            });
        }
        Ok(())
    }

    /// Retire a codec key, refusing unless **every** expected shard proves it
    /// holds zero rows referencing it (issue #948, AC6).
    ///
    /// By default, also refuses unless the durable write fence has held for
    /// `staleness_window` (issue #1244).
    ///
    /// Fail-closed by construction: a shard whose pool is missing or whose
    /// census errors is recorded as `reachable = false` and blocks the
    /// retirement on its own. An uncounted shard is never counted as a zero —
    /// that is the difference between a compliance control and a compliance
    /// incident. An empty `expected_shards` proves nothing and is refused for
    /// the same reason.
    ///
    /// The census runs **twice**: once, and — only if the first came back
    /// zero — again after `recheck_delay`, before anything is finalized. This
    /// narrows (does not eliminate; see [`FleetWriteFence`]) the in-flight-
    /// append hazard, catching a transaction that committed in the gap.
    ///
    /// See [`FleetWriteFence`] for the two ways to supply the fence.
    ///
    /// On success the key is dropped from this process's in-memory registry via
    /// [`PayloadCodecs::retire_key_local`]; disposing of the key *material* is
    /// the embedder's business (this crate never holds it). Read
    /// `docs/operations/codec-key-rotation.md` before you do: an `Ok` here is
    /// scoped to `harvest_events`, not to every place a codec envelope can sit.
    ///
    /// # Errors
    ///
    /// - [`HarvestError::Config`] when `key_id` is the active key, when
    ///   `expected_shards` is empty, or when `key_id` is not registered.
    /// - [`HarvestError::CodecKeyRetirementBlocked`] naming, per blocking
    ///   shard, the remaining row count, an unreadable shard, an incomplete
    ///   durable transition to `"retiring"`, or time left on the staleness
    ///   window.
    pub async fn retire_codec_key(
        sharded_pool: &crate::shard::ShardedDbPool,
        expected_shards: &[crate::types::ShardId],
        codecs: &PayloadCodecs,
        key_id: &str,
        fence: FleetWriteFence,
        staleness_window: Duration,
        recheck_delay: Duration,
    ) -> HarvestResult<()> {
        validate_retirement_request(expected_shards, codecs, key_id)?;

        // Fail closed on an INCOMPLETE list, not just an unreachable shard. A
        // caller that passes a stale or process-local subset would otherwise get
        // a vacuous `Ok` while whole shards were never censused -- and the
        // runbook tells the operator that `Ok` is when key material may be
        // destroyed.
        let missing: Vec<CodecKeyShardRemainder> = sharded_pool
            .iter_shards()
            .map(|(shard, _)| shard)
            .filter(|shard| !expected_shards.contains(shard))
            .map(|shard| CodecKeyShardRemainder {
                shard_id: shard.as_i32(),
                rows: 0,
                reachable: false,
                reason: Some(
                    "this process has a pool for this shard but it was omitted from the \
                     supplied shard list"
                        .to_string(),
                ),
            })
            .collect();
        if !missing.is_empty() {
            return Err(HarvestError::CodecKeyRetirementBlocked {
                key_id: key_id.to_string(),
                remaining: missing,
            });
        }

        if fence == FleetWriteFence::NotConfirmed {
            staleness_gate(sharded_pool, expected_shards, key_id, staleness_window).await?;
        }

        let first_pass = census_remaining(sharded_pool, expected_shards, key_id).await;
        if !first_pass.is_empty() {
            return Err(HarvestError::CodecKeyRetirementBlocked {
                key_id: key_id.to_string(),
                remaining: first_pass,
            });
        }
        if !recheck_delay.is_zero() {
            tokio::time::sleep(recheck_delay).await;
        }
        let second_pass = census_remaining(sharded_pool, expected_shards, key_id).await;
        if !second_pass.is_empty() {
            return Err(HarvestError::CodecKeyRetirementBlocked {
                key_id: key_id.to_string(),
                remaining: second_pass,
            });
        }

        // Bookkeeping runs regardless of `fence`. The escape hatch skips only
        // the staleness-window *wait* (see `FleetWriteFence`'s doc), never the
        // durable record that the key is now retired. Best-effort -- a failed
        // write here does not un-retire the key.
        for shard in expected_shards {
            if let Some(pool) = sharded_pool.exact_pool_for(*shard)
                && let Ok(mut conn) = pool.get().await
                && let Err(e) = write_key_state_retired(&mut conn, key_id).await
            {
                tracing::warn!(
                    error = %e,
                    key_id,
                    shard_id = shard.as_i32(),
                    "codec key state: failed to record retirement (bookkeeping only; the \
                     in-memory key was still dropped)"
                );
            }
        }

        codecs.retire_key_local(key_id)
    }

    /// Activate a codec key fleet-wide, refusing while any live worker cannot
    /// safely read or write under `key_id` (issue #1244).
    ///
    /// # Why the check exists
    ///
    /// A pre-#948 reader recognises an envelope only as exactly three keys at
    /// version 1. A version-2 envelope (four keys, a `kid`) comes back
    /// unchanged from its decoder — silent wrong data, not an error. See
    /// [`PayloadCodecs::set_active_key`]'s rustdoc.
    ///
    /// This function is the structural version of that rustdoc's manual
    /// rollout-ordering warning. Every live worker's `harvest_workers.labels`
    /// row must advertise [`crate::payload_codec::CODEC_ENVELOPE_VERSION_KEYED`]
    /// support, **and** carry `key_id` in
    /// [`crate::payload_codec::CODEC_REGISTERED_KEY_IDS_LABEL`], before
    /// activation is allowed to proceed at all.
    ///
    /// The second half matters separately from the first. A binary can
    /// support the version-2 envelope's *syntax* fleet-wide before the
    /// target key's *material* reaches every worker's config. A worker
    /// missing the key cannot decode a payload some other worker encodes
    /// under it.
    ///
    /// "Live" mirrors [`crate::worker::worker_stale_secs`]: pass that
    /// function's result (computed from the deployment's configured
    /// `worker_heartbeat_interval`) as `worker_stale_secs`.
    ///
    /// Fail-closed like [`retire_codec_key`]: an unreachable shard, or one
    /// omitted from `expected_shards` while this process holds a pool for it,
    /// blocks activation on its own.
    ///
    /// # Two passes, not one
    ///
    /// The capability check above runs as one bulk pass over every expected
    /// shard before anything is written. A second, per-shard pass then
    /// re-checks and writes each shard back to back, on the same connection.
    ///
    /// The bulk pass alone would leave a gap. A worker that registers, or
    /// sends its first heartbeat, strictly after that pass and before this
    /// call's last shard write was never scanned. The per-shard recheck
    /// catches that worker on its own shard. It runs immediately before the
    /// write that shard would otherwise make unsafe for it.
    ///
    /// This narrows the gap; it does not close it to zero. A worker can
    /// still slip in between one shard's own recheck and that shard's own
    /// write. Those are two statements back to back, on one already-open
    /// connection. Closing that residual fully would need a
    /// registration-side fence:
    /// worker registration itself consulting `harvest_codec_key_state`. This
    /// function does not implement that.
    ///
    /// On success, every expected shard's `harvest_codec_key_state` is updated
    /// durably (the previously active row, if any, becomes `"retiring"`), and
    /// this process's registry is flipped via [`PayloadCodecs::set_active_key`].
    /// Shard writes are sequential, not two-phase-committed. A failure partway
    /// through — a database error, or the per-shard recheck above finding a
    /// new blocker — leaves some shards active and others not-yet-written.
    /// The call therefore returns the first such error rather than continuing
    /// past it. Re-running once the underlying failure is fixed converges
    /// every shard via `ON CONFLICT DO UPDATE`.
    ///
    /// This function does not coordinate across concurrent calls activating
    /// **different** keys. Each shard resolves such a race independently, and
    /// whichever write commits last on a shard wins it. So two operators
    /// racing to rotate onto different keys at once can leave different
    /// shards durably active on different keys. Nothing in this crate detects
    /// that split automatically. An operator who runs concurrent rotations
    /// must reconcile it by hand: re-run one `activate_codec_key` call to
    /// converge every shard.
    ///
    /// # Errors
    ///
    /// - [`HarvestError::Config`] when `key_id` is not registered or
    ///   `expected_shards` is empty.
    /// - [`HarvestError::CodecKeyActivationBlocked`] naming every blocking live
    ///   worker or unreachable/omitted shard.
    /// - Any database error from the durable write, after the checks pass.
    ///   This includes a shard that became unreachable between the capability
    ///   check and this call's own write. A connection is re-resolved per
    ///   shard rather than held across the two passes.
    pub async fn activate_codec_key(
        sharded_pool: &crate::shard::ShardedDbPool,
        expected_shards: &[crate::types::ShardId],
        codecs: &PayloadCodecs,
        key_id: &str,
        worker_stale_secs: i64,
    ) -> HarvestResult<()> {
        if codecs.codec_for_key(key_id).is_none() {
            return Err(HarvestError::Config(format!(
                "cannot activate unregistered codec key id {key_id:?}; register it with \
                 PayloadCodecs::register_key first"
            )));
        }
        if expected_shards.is_empty() {
            return Err(HarvestError::Config(
                "cannot activate a codec key: no shards were supplied to inspect for live \
                 workers"
                    .to_string(),
            ));
        }

        let mut blockers: Vec<CodecKeyActivationBlocker> = sharded_pool
            .iter_shards()
            .map(|(shard, _)| shard)
            .filter(|shard| !expected_shards.contains(shard))
            .map(|shard| CodecKeyActivationBlocker {
                shard_id: shard.as_i32(),
                worker_id: None,
                host: None,
                reachable: false,
                reason: Some(
                    "this process has a pool for this shard but it was omitted from the \
                     supplied shard list"
                        .to_string(),
                ),
            })
            .collect();

        for shard in expected_shards {
            let shard_id = shard.as_i32();
            let mut conn = match resolve_shard_conn(sharded_pool, *shard).await {
                Ok(c) => c,
                Err(reason) => {
                    blockers.push(CodecKeyActivationBlocker {
                        shard_id,
                        worker_id: None,
                        host: None,
                        reachable: false,
                        reason: Some(reason),
                    });
                    continue;
                }
            };
            match blocking_workers(&mut conn, worker_stale_secs, key_id).await {
                Ok(rows) => blockers.extend(blockers_from_rows(shard_id, rows)),
                Err(e) => blockers.push(CodecKeyActivationBlocker {
                    shard_id,
                    worker_id: None,
                    host: None,
                    reachable: false,
                    reason: Some(format!("capability query failed: {e}")),
                }),
            }
        }

        if !blockers.is_empty() {
            return Err(HarvestError::CodecKeyActivationBlocked {
                key_id: key_id.to_string(),
                blockers,
            });
        }

        // Re-check each shard's capability immediately before writing it, on
        // the same connection the write itself uses. The bulk census above
        // ran as one pass over every shard. A worker that registered or
        // heartbeated *after* it -- one that never advertised `key_id` --
        // would otherwise go durably unblocked.
        //
        // This narrows that window. It shrinks "the whole census-plus-write
        // pass" down to "this one shard's own back-to-back
        // recheck-then-write". It catches the same case the bulk census
        // exists for, just later.
        //
        // A shard that fails this recheck stops the loop without writing
        // that shard or any shard after it. Shards already written in this
        // call are not rolled back. This is the same partial-write outcome
        // this function's own rustdoc already documents for a plain
        // database error here, converged by re-running once fixed.
        //
        // Read before any shard is written, once, not per shard. It names
        // the single in-memory key this process is rotating away from --
        // the same fact on every shard this call touches.
        let outgoing_key_id = codecs.active_key_id();
        for shard in expected_shards {
            let shard_id = shard.as_i32();
            let mut conn = resolve_shard_conn(sharded_pool, *shard)
                .await
                .map_err(|reason| {
                    HarvestError::Database(format!(
                        "codec key activation: shard {} became unreachable between the capability \
                     check and the durable write: {reason}",
                        shard.as_i32()
                    ))
                })?;
            let recheck = blocking_workers(&mut conn, worker_stale_secs, key_id)
                .await
                .map_err(|e| {
                    HarvestError::Database(format!(
                        "codec key activation: shard {shard_id} pre-write capability recheck \
                         failed: {e}"
                    ))
                })?;
            if !recheck.is_empty() {
                return Err(HarvestError::CodecKeyActivationBlocked {
                    key_id: key_id.to_string(),
                    blockers: blockers_from_rows(shard_id, recheck),
                });
            }
            write_key_state_activation(&mut conn, key_id, &outgoing_key_id).await?;
        }

        codecs.set_active_key(key_id)
    }

    /// Refresh this process's active codec key from the durable, fleet-wide
    /// `harvest_codec_key_state` table (issue #1244).
    ///
    /// Folded into [`crate::timeout::enforce_timeouts_once`] — shard-local, on
    /// the connection the caller already holds, on the same nominal cadence.
    /// This is what turns [`activate_codec_key`]'s durable write into a fact
    /// every *other* process eventually observes.
    ///
    /// The deployment's configured scanner-tick interval is the operational
    /// margin [`FleetWriteFence`]'s docs discuss. See the caveats there
    /// about what that margin does and does not actually bound.
    ///
    /// This call runs on *every* tick that reaches it. So
    /// `enforce_timeouts_once` places it before any resident that can end
    /// the tick early with `?`, not beside the re-encryption sweep.
    ///
    /// The sweep sits later, among the fallible residents, because its own
    /// safety comes from idempotent, re-run-to-converge CAS writes. It does
    /// not depend on a staleness-window timing proof the way this call does.
    ///
    /// Never surfaces an error that should break the rest of the tick. Three
    /// things are logged and swallowed by the caller instead: an unreachable
    /// table, an active key this process never registered, and a local flip
    /// failure. That is the same posture as [`sweep_codec_reencryption`]
    /// beside it. Returns `true` when this call actually changed the local
    /// active key.
    ///
    /// # Errors
    ///
    /// Propagates database failures reading the table. Never a local-flip
    /// failure. That is logged separately, because flipping to an
    /// unregistered key is expected during a rotation window. It must not
    /// look like a query failure.
    pub async fn refresh_active_codec_key(
        conn: &mut AsyncPgConnection,
        codecs: &PayloadCodecs,
    ) -> HarvestResult<bool> {
        let Some(key_id) = active_key_row(conn).await? else {
            return Ok(false);
        };
        if key_id == codecs.active_key_id() {
            return Ok(false);
        }
        if codecs.codec_for_key(&key_id).is_none() {
            tracing::warn!(
                key_id,
                "codec key state refresh: fleet-wide active key is not registered on this \
                 process; new writes here still use the previous key until it is"
            );
            return Ok(false);
        }
        match codecs.set_active_key(&key_id) {
            Ok(()) => Ok(true),
            Err(e) => {
                tracing::warn!(error = %e, key_id, "codec key state refresh: local flip failed");
                Ok(false)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::payload_codec::{
        CODEC_ENVELOPE_KID_KEY, CODEC_LEGACY_KEY_ID, CodecError, PayloadCodec,
    };
    use serde_json::json;
    use std::sync::Arc;

    #[derive(Debug)]
    struct XorCodec(u8);

    impl PayloadCodec for XorCodec {
        fn codec_id(&self) -> &'static str {
            "xor"
        }
        fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, CodecError> {
            Ok(raw.iter().map(|b| b ^ self.0).collect())
        }
        fn decode(&self, encoded: &[u8]) -> Result<Vec<u8>, CodecError> {
            Ok(encoded.iter().map(|b| b ^ self.0).collect())
        }
    }

    /// A registry holding `k1` (retired) and `k2` (active).
    fn rotated_registry() -> PayloadCodecs {
        let codecs = PayloadCodecs::default();
        codecs
            .register_key("k1", Arc::new(XorCodec(0x11)))
            .expect("register k1");
        codecs
            .register_key("k2", Arc::new(XorCodec(0x22)))
            .expect("register k2");
        codecs.set_active_key("k2").expect("activate k2");
        codecs
    }

    /// A serialized `WorkflowStarted` whose `input` is encoded under `key_id`.
    fn event_under(codecs: &PayloadCodecs, key_id: &str, input: Value) -> Value {
        let restore = codecs.active_key_id();
        codecs.set_active_key(key_id).expect("activate for fixture");
        let event = codecs
            .encode_event(&crate::event::WorkflowEvent::WorkflowStarted {
                input,
                timestamp: chrono::DateTime::parse_from_rfc3339("2026-08-29T12:00:00Z")
                    .expect("fixed timestamp")
                    .with_timezone(&chrono::Utc),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            })
            .expect("encode");
        codecs.set_active_key(&restore).expect("restore active key");
        event
    }

    #[test]
    fn reencrypts_a_field_carrying_a_non_active_key_id() {
        let codecs = rotated_registry();
        let mut event = event_under(&codecs, "k1", json!({"user": "alice"}));
        assert_eq!(event["data"]["input"][CODEC_ENVELOPE_KID_KEY], "k1");

        let outcome = reencrypt_event_payload_fields(&codecs, &mut event).expect("reencrypt");

        assert_eq!(outcome.fields_reencrypted, 1);
        assert!(outcome.changed());
        assert_eq!(event["data"]["input"][CODEC_ENVELOPE_KID_KEY], "k2");
    }

    #[test]
    fn decoded_plaintext_is_byte_identical_and_structure_is_untouched() {
        // The scope guarantee behind sanctioned exception #3, at unit level.
        let codecs = rotated_registry();
        let plaintext = json!({"user": "alice", "amounts": [1, 2, 3], "nested": {"k": null}});
        let mut event = event_under(&codecs, "k1", plaintext.clone());
        let before = event.clone();

        reencrypt_event_payload_fields(&codecs, &mut event).expect("reencrypt");

        assert_eq!(event["type"], before["type"], "event type is never touched");
        assert_eq!(
            event["data"]["timestamp"], before["data"]["timestamp"],
            "timestamps are never touched"
        );
        assert_ne!(
            event["data"]["input"]["data"], before["data"]["input"]["data"],
            "the ciphertext bytes did change"
        );
        let decoded = codecs.decode_event(event).expect("decode after sweep");
        match decoded {
            crate::event::WorkflowEvent::WorkflowStarted { input, .. } => {
                assert_eq!(input, plaintext, "decoded plaintext is byte-identical");
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn is_idempotent() {
        let codecs = rotated_registry();
        let mut event = event_under(&codecs, "k1", json!({"user": "alice"}));

        let first = reencrypt_event_payload_fields(&codecs, &mut event).expect("first");
        let after_first = event.clone();
        let second = reencrypt_event_payload_fields(&codecs, &mut event).expect("second");

        assert_eq!(first.fields_reencrypted, 1);
        assert_eq!(second.fields_reencrypted, 0);
        assert!(!second.changed(), "a re-run is a no-op");
        assert_eq!(event, after_first, "the row is byte-identical on a re-run");
    }

    #[test]
    fn skips_offload_reference_envelopes() {
        // AC8: offload composes AFTER encode, so the field holds a reference,
        // not ciphertext. Re-encoding it would orphan the blob.
        let codecs = rotated_registry();
        let mut event = event_under(&codecs, "k1", json!({"user": "alice"}));
        let offload_envelope = json!({
            "_harvest_offload_envelope": 1,
            "store_id": "mem",
            "key": "blob/abc",
            "len": 4096,
            "checksum": "deadbeef",
        });
        event["data"]["input"] = offload_envelope.clone();

        let outcome = reencrypt_event_payload_fields(&codecs, &mut event).expect("reencrypt");

        assert_eq!(outcome.fields_reencrypted, 0);
        assert_eq!(outcome.fields_skipped_offloaded, 1);
        assert_eq!(
            event["data"]["input"], offload_envelope,
            "passed through verbatim"
        );
    }

    #[test]
    fn skips_erasure_tombstones() {
        // AC8: a tombstone has no ciphertext to rotate.
        let codecs = rotated_registry();
        let mut event = event_under(&codecs, "k1", json!({"user": "alice"}));
        event["data"]["input"] = crate::erase::erasure_tombstone();

        let outcome = reencrypt_event_payload_fields(&codecs, &mut event).expect("reencrypt");

        assert_eq!(outcome.fields_reencrypted, 0);
        assert_eq!(outcome.fields_skipped_erased, 1);
        assert_eq!(event["data"]["input"], crate::erase::erasure_tombstone());
    }

    #[test]
    fn skips_plaintext_fields() {
        // The sweep migrates keys; it never newly encrypts cleartext history.
        let codecs = rotated_registry();
        let mut event = event_under(&codecs, "k1", json!({"user": "alice"}));
        event["data"]["input"] = json!({"plain": "text"});

        let outcome = reencrypt_event_payload_fields(&codecs, &mut event).expect("reencrypt");

        assert_eq!(outcome.fields_reencrypted, 0);
        assert!(!outcome.changed());
        assert_eq!(event["data"]["input"], json!({"plain": "text"}));
    }

    #[test]
    fn a_kidless_envelope_is_swept_as_the_legacy_key() {
        // AC1 + AC4: pre-upgrade rows carry no `kid` and must be swept onto the
        // active key like any other non-active key id.
        let codecs = PayloadCodecs::default();
        codecs
            .register_key(CODEC_LEGACY_KEY_ID, Arc::new(XorCodec(0x11)))
            .expect("register legacy");
        let mut event = event_under(&codecs, CODEC_LEGACY_KEY_ID, json!({"old": true}));
        assert!(
            event["data"]["input"].get(CODEC_ENVELOPE_KID_KEY).is_none(),
            "the fixture really is a pre-upgrade, kid-less envelope"
        );

        codecs
            .register_key("k2", Arc::new(XorCodec(0x22)))
            .expect("register k2");
        codecs.set_active_key("k2").expect("activate k2");

        let outcome = reencrypt_event_payload_fields(&codecs, &mut event).expect("reencrypt");

        assert_eq!(outcome.fields_reencrypted, 1);
        assert_eq!(event["data"]["input"][CODEC_ENVELOPE_KID_KEY], "k2");
    }

    #[test]
    fn a_row_that_cannot_be_decoded_is_left_completely_untouched() {
        // Never write a half-rotated row, and never write plaintext: a field we
        // cannot decode aborts the whole row with the ORIGINAL bytes intact.
        let writer = PayloadCodecs::default();
        writer
            .register_key("k0", Arc::new(XorCodec(0x00)))
            .expect("register k0");
        let mut event = event_under(&writer, "k0", json!({"user": "alice"}));
        let before = event.clone();

        // A registry that has never heard of `k0`.
        let codecs = rotated_registry();
        let err = reencrypt_event_payload_fields(&codecs, &mut event)
            .expect_err("an unresolvable key must fail the row");
        assert!(
            matches!(err, HarvestError::UnknownCodecKey { .. }),
            "{err:?}"
        );
        assert_eq!(
            event, before,
            "the row is byte-identical after a failed sweep"
        );
    }

    #[derive(Debug)]
    struct GarbageDecodeCodec;

    impl PayloadCodec for GarbageDecodeCodec {
        fn codec_id(&self) -> &'static str {
            "garbage"
        }
        fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, CodecError> {
            Ok(raw.to_vec())
        }
        fn decode(&self, _encoded: &[u8]) -> Result<Vec<u8>, CodecError> {
            // Simulates a codec that "succeeds" but returns non-JSON bytes --
            // e.g. corrupt ciphertext an unauthenticated cipher decodes
            // without detecting the tampering.
            Ok(b"not valid json".to_vec())
        }
    }

    #[test]
    fn a_field_that_decodes_to_non_json_bytes_is_left_completely_untouched() {
        // Regression test (PR #1275 review): decode_payload_bytes must still
        // validate the decoded bytes as JSON. A codec that "successfully"
        // decodes corrupt or mismatched ciphertext into non-JSON garbage must
        // fail the row, not be silently re-encrypted and written back out as
        // new ciphertext wrapping the garbage.
        let codecs = PayloadCodecs::default();
        codecs
            .register_key("k-garbage", Arc::new(GarbageDecodeCodec))
            .expect("register k-garbage");
        codecs
            .register_key("k2", Arc::new(XorCodec(0x22)))
            .expect("register k2");
        codecs.set_active_key("k2").expect("activate k2");

        let mut event = event_under(&codecs, "k-garbage", json!({"user": "alice"}));
        let before = event.clone();

        let err = reencrypt_event_payload_fields(&codecs, &mut event)
            .expect_err("non-JSON decoded bytes must fail the row");
        assert!(matches!(err, HarvestError::Serialization(_)), "{err:?}");
        assert_eq!(
            event, before,
            "the row is byte-identical after a failed sweep"
        );
    }

    #[test]
    fn every_payload_bearing_field_is_swept() {
        let codecs = rotated_registry();
        let mut event = event_under(&codecs, "k1", json!({"user": "alice"}));
        // Plant an encoded value in every payload-bearing key, not just `input`.
        let encoded = event["data"]["input"].clone();
        for key in PAYLOAD_FIELD_KEYS {
            event["data"][key] = encoded.clone();
        }

        let outcome = reencrypt_event_payload_fields(&codecs, &mut event).expect("reencrypt");

        assert_eq!(outcome.fields_reencrypted, PAYLOAD_FIELD_KEYS.len());
        for key in PAYLOAD_FIELD_KEYS {
            assert_eq!(
                event["data"][key][CODEC_ENVELOPE_KID_KEY], "k2",
                "field {key}"
            );
        }
    }

    #[test]
    fn census_counts_key_ids_referenced_by_an_event() {
        let codecs = rotated_registry();
        let mut event = event_under(&codecs, "k1", json!({"user": "alice"}));
        event["data"]["output"] =
            event_under(&codecs, "k2", json!({"done": true}))["data"]["input"].clone();
        event["data"]["details"] = crate::erase::erasure_tombstone();

        let ids = event_key_ids(&event);

        assert_eq!(
            ids,
            ["k1".to_string(), "k2".to_string()].into_iter().collect()
        );
    }

    #[test]
    fn a_registry_with_no_keyed_codecs_sweeps_nothing() {
        // AC: zero overhead on the ~all deployments that never rotate.
        let mut codecs = PayloadCodecs::default();
        codecs.set_default(Arc::new(XorCodec(0x11)));
        let mut event = codecs
            .encode_event(&crate::event::WorkflowEvent::SideEffectRecorded {
                kind: crate::event::SideEffectKind::Custom,
                name: Some("x".to_string()),
                value: json!({"a": 1}),
            })
            .expect("encode");
        let before = event.clone();

        let outcome = reencrypt_event_payload_fields(&codecs, &mut event).expect("reencrypt");

        assert!(!outcome.changed());
        assert_eq!(event, before);
    }

    /// No engine path may read history through the **identity** loader.
    ///
    /// `store::load_history` delegates to `load_history_with_codecs` with the
    /// identity registry, so it hard-errors `UnknownCodecKey` on the first
    /// keyed envelope. Once the engine's writes encode through the configured
    /// registry, any engine read still on that loader is a live defect, not a
    /// latent one: the activity-start path reloads history under the execution
    /// row lock, so an identity read there means **no activity can start** on a
    /// deployment with a keyed codec configured.
    ///
    /// Every engine read must therefore be one of two things:
    ///
    /// - `load_history_with_codecs` / `load_history_inflated`, when it looks at
    ///   payload fields; or
    /// - `load_history_undecoded`, when it only does id arithmetic
    ///   (`next_event_id`) or matches on event variants — decoding buys such a
    ///   caller nothing and costs it a registry it may not have.
    ///
    /// Source-level because the property is "which function was called", which
    /// no runtime assertion can observe without a keyed deployment of every
    /// path. The allowlist is deliberately explicit: a file leaves it only by
    /// being fixed, and a new identity read anywhere else fails here.
    ///
    /// # Why the counting is written this way
    ///
    /// An earlier version of this guard matched the literal `store::load_history(`
    /// and missed three real defects: a bare `load_history(` in a module that
    /// imports it, and the `lock_and_load_history` **wrapper**, whose one caller
    /// (`ActivityContext::run_transactional`) would have rolled back every
    /// transactional activity commit under a keyed codec. Matching a call
    /// spelling is a guess about how callers write; neutralising every *safe*
    /// spelling and counting the remainder is not.
    #[test]
    fn no_engine_path_reads_history_through_the_identity_loader() {
        /// Every loader that is safe by construction. `lock_and_load_history_undecoded`
        /// is here because its name states the contract; a wrapper that hides an
        /// identity read behind a neutral name is exactly what this guard missed once.
        const SAFE_LOADERS: &[&str] = &[
            "load_history_with_codecs(",
            "load_history_undecoded(",
            "load_history_inflated(",
            "load_history_since_inflated(",
            "load_history_since(",
            "load_history_page(",
            "load_history_with_timestamps(",
            "lock_and_load_history_undecoded(",
            "lock_workflow_execution_row_and_load_history(",
            "lock_workflow_execution_and_load_history(",
            "fn load_history(",
        ];

        // Neutralise every safe spelling, then count what is left. This catches
        // `store::load_history(`, a bare `load_history(` in a module that
        // imports it, and any future wrapper spelled some third way.
        fn identity_reads(src: &str) -> usize {
            let mut text = src.to_string();
            for safe in SAFE_LOADERS {
                text = text.replace(safe, "SAFE_LOADER_CALL");
            }
            text.matches("load_history(").count()
        }

        // Both still need a `PayloadCodecs` threaded through a public
        // signature (`RetentionScanner::spawn`, `run_canary`), which is
        // issue #1243's remaining scope rather than rotation's. Neither is on
        // the task-processing path: archival and the replay canary.
        const KNOWN_IDENTITY_READS: &[(&str, usize)] = &[("retention.rs", 1), ("testing.rs", 2)];

        // `store.rs` is excluded: it *defines* the loaders, and its own
        // delegation between them is the thing every other file must not do.
        let engine_sources: &[(&str, &str)] = &[
            ("worker.rs", include_str!("worker.rs")),
            ("timeout.rs", include_str!("timeout.rs")),
            ("execution.rs", include_str!("execution.rs")),
            ("sessions.rs", include_str!("sessions.rs")),
            ("poison_pill.rs", include_str!("poison_pill.rs")),
            ("context.rs", include_str!("context.rs")),
            ("retention.rs", include_str!("retention.rs")),
            ("testing.rs", include_str!("testing.rs")),
            ("reset.rs", include_str!("reset.rs")),
            ("handle.rs", include_str!("handle.rs")),
            ("event_batch.rs", include_str!("event_batch.rs")),
            ("debounce.rs", include_str!("debounce.rs")),
            ("throttle.rs", include_str!("throttle.rs")),
            ("batch.rs", include_str!("batch.rs")),
            ("external_task.rs", include_str!("external_task.rs")),
            (
                "completion_trigger.rs",
                include_str!("completion_trigger.rs"),
            ),
        ];

        for (name, src) in engine_sources {
            let found = identity_reads(src);
            let allowed = KNOWN_IDENTITY_READS
                .iter()
                .find(|(file, _)| file == name)
                .map_or(0, |(_, n)| *n);
            assert_eq!(
                found, allowed,
                "{name}: found {found} call(s) to the identity `load_history`, \
                 expected {allowed}. Use `load_history_with_codecs` when the \
                 caller reads payload fields, or `load_history_undecoded` when \
                 it only needs `next_event_id` or event variants. If a new \
                 identity read is genuinely unavoidable, add it to \
                 KNOWN_IDENTITY_READS with the reason -- but an identity read \
                 raises `UnknownCodecKey` on any keyed history, so it is almost \
                 certainly a live bug."
            );
        }
    }

    /// The write-side twin of
    /// [`no_engine_path_reads_history_through_the_identity_loader`].
    ///
    /// `store::append_events` encodes with the identity registry, so a
    /// payload-bearing event written through it lands in `harvest_events` as
    /// **cleartext** — and stays that way. Unlike an identity *read*, which
    /// fails loudly with `UnknownCodecKey`, an identity write succeeds
    /// silently, and the sweep never repairs it: converting plaintext to
    /// ciphertext is not something the sweep does, by design (it re-encodes
    /// rows that already carry a non-active key id, and plaintext carries
    /// none). So the leak is permanent and invisible.
    ///
    /// That asymmetry is exactly why this guard exists separately: the read
    /// guard catches its class the first time a keyed deployment runs, and this
    /// one has to catch its class before that, because nothing downstream will.
    ///
    /// The allowlist is not aspirational — every entry is a real remaining gap
    /// tracked in issue #1243, with the count pinned so the number can only go
    /// down. `store.rs` is excluded: it *defines* both helpers, and its own
    /// delegation between them is the thing every other file must not do.
    #[test]
    fn no_engine_path_appends_history_through_the_identity_encoder() {
        const SAFE_APPENDS: &[&str] = &[
            "append_events_with_codecs(",
            "append_events_offloaded_with_codecs(",
            "fn append_events(",
            "fn append_events_offloaded(",
        ];

        fn identity_appends(src: &str) -> usize {
            let mut text = src.to_string();
            for safe in SAFE_APPENDS {
                text = text.replace(safe, "SAFE_APPEND_CALL");
            }
            text.matches("append_events(").count()
                + text.matches("append_events_offloaded(").count()
        }

        // Issue #1243 closed the `WorkflowStarted.input` gap. `execution.rs`'s
        // start paths now encode through `append_events_with_codecs`.
        //
        // The remaining 5 write `WorkflowCancelled` (from
        // `cancel_workflow_execution_collect` and
        // `terminate_workflow_execution_collect`), `WorkflowExecutionPaused`,
        // `WorkflowExecutionResumed`, and `WorkflowRedriven`. None of these
        // carry a `PAYLOAD_FIELD_KEYS` field, so identity is exact here.
        // `reset.rs` keeps its fork marker and source-execution terminal, also
        // both free of payload fields.
        const KNOWN_IDENTITY_APPENDS: &[(&str, usize)] = &[("execution.rs", 5), ("reset.rs", 2)];

        let engine_sources: &[(&str, &str)] = &[
            ("worker.rs", include_str!("worker.rs")),
            ("timeout.rs", include_str!("timeout.rs")),
            ("execution.rs", include_str!("execution.rs")),
            ("sessions.rs", include_str!("sessions.rs")),
            ("poison_pill.rs", include_str!("poison_pill.rs")),
            ("context.rs", include_str!("context.rs")),
            ("reset.rs", include_str!("reset.rs")),
            ("retention.rs", include_str!("retention.rs")),
            ("testing.rs", include_str!("testing.rs")),
            ("external_task.rs", include_str!("external_task.rs")),
            (
                "completion_trigger.rs",
                include_str!("completion_trigger.rs"),
            ),
            ("batch.rs", include_str!("batch.rs")),
        ];

        for (name, src) in engine_sources {
            let found = identity_appends(src);
            let allowed = KNOWN_IDENTITY_APPENDS
                .iter()
                .find(|(file, _)| file == name)
                .map_or(0, |(_, n)| *n);
            assert_eq!(
                found, allowed,
                "{name}: found {found} call(s) to the identity `append_events`, \
                 expected {allowed}. Use `append_events_with_codecs` and pass the \
                 configured registry. Pass it even when the event carries no \
                 payload-bearing field: the codec only touches \
                 PAYLOAD_FIELD_KEYS, so it is a no-op for the rest, and a \
                 uniform rule removes a per-site judgement that is easy to get \
                 wrong. An identity append writes cleartext that the sweep will \
                 never encrypt."
            );
        }
    }
}
