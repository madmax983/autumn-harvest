//! Interfaces for transforming event payloads before they are persisted.
//!
//! **Why does this exist?**
//! By default, harvest workflow inputs, outputs, and parameters are stored in the database
//! as plain JSON. If a workflow processes sensitive data (like PII, secrets, or financial data),
//! storing it in plain text is a security risk. Payload codecs solve this by providing a hook
//! to intercept and transform these JSON values (typically encrypting or compressing them)
//! *before* they hit the database, and reversing the process when they are read back.
//!
//! ## Examples
//!
//! Implementing a simple rot13 "encryption" codec:
//!
//! ```rust
//! use autumn_harvest::payload_codec::{PayloadCodec, CodecError};
//!
//! struct Rot13Codec;
//!
//! impl PayloadCodec for Rot13Codec {
//!     fn codec_id(&self) -> &'static str { "rot13" }
//!     fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, CodecError> {
//!         Ok(raw.iter().map(|&b| if b.is_ascii_alphabetic() {
//!             let base = if b.is_ascii_lowercase() { b'a' } else { b'A' };
//!             (b - base + 13) % 26 + base
//!         } else { b }).collect())
//!     }
//!     fn decode(&self, encoded: &[u8]) -> Result<Vec<u8>, CodecError> {
//!         // rot13 is its own inverse
//!         self.encode(encoded)
//!     }
//! }
//! ```

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use base64::Engine as _;
use serde_json::Value;

use crate::error::{HarvestError, HarvestResult};

/// Discriminator key marking a codec envelope (issue #608).
///
/// Public for the same reason as its offload sibling
/// [`crate::payload_store::OFFLOAD_ENVELOPE_KEY`]: a consumer that must
/// *detect* an undecoded payload without a codec registry — e.g. the
/// replay-drift gate's fixture guard (issue #798), which refuses a bundle it
/// cannot decode — can cheaply reject on the raw JSON without hardcoding the
/// key and drifting from this definition.
pub const CODEC_ENVELOPE_KEY: &str = "_harvest_codec_envelope";

/// Marker key for a payload field the read path could not decode (issue #608).
///
/// Sibling discriminator of [`CODEC_ENVELOPE_KEY`],
/// `_harvest_offload_envelope` (issue #524), and `_harvest_erased`
/// (issue #495).
pub const UNDECODABLE_MARKER_KEY: &str = "_harvest_undecodable";

/// Envelope key carrying the **codec key id** a payload was encoded under
/// (issue #948).
///
/// Optional by design. An envelope written before key rotation existed — and
/// one written while the [`CODEC_LEGACY_KEY_ID`] key is active, which is the
/// canonical spelling of the same thing — carries no `kid` at all, so an
/// un-rotated deployment's stored bytes are byte-identical to pre-#948. An
/// absent `kid` **is** [`CODEC_LEGACY_KEY_ID`]; the two forms are one key id,
/// not two.
///
/// The key id rides *inside* the codec envelope, which is already opaque
/// payload content, so rotation adds no `WorkflowEvent` variant and does not
/// touch the adjacently-tagged event JSON contract.
pub const CODEC_ENVELOPE_KID_KEY: &str = "kid";

/// Discriminator value of a **legacy** (pre-#948) codec envelope: exactly three
/// keys, no [`CODEC_ENVELOPE_KID_KEY`].
///
/// Every envelope ever written before key rotation carries this, and so does
/// every envelope written while [`CODEC_LEGACY_KEY_ID`] is the active key — so
/// an un-rotated deployment's stored bytes stay byte-identical to pre-#948.
pub const CODEC_ENVELOPE_VERSION_LEGACY: i64 = 1;

/// Discriminator value of a **keyed** codec envelope (issue #948): exactly four
/// keys, the fourth a valid [`CODEC_ENVELOPE_KID_KEY`].
///
/// A distinct version rather than a fourth key under
/// [`CODEC_ENVELOPE_VERSION_LEGACY`], because reusing version `1` would
/// retroactively reinterpret data. Pre-#948, `{"_harvest_codec_envelope": 1,
/// "codec_id": ..., "data": ..., "kid": ...}` was **not** an envelope — the
/// parser required exactly three keys — so on an identity deployment such a
/// value could legitimately be stored *business plaintext*. Widening version
/// `1` to accept it would turn that plaintext into a decode target: strict
/// reads would fail with `UnknownCodecKey` where they used to pass through, and
/// with a matching key registered the sweep would decode and rewrite data that
/// was never ciphertext. Version `2` is a shape no prior release could write or
/// accept, so nothing is reinterpreted.
pub const CODEC_ENVELOPE_VERSION_KEYED: i64 = 2;

/// The designated key id for envelopes that carry no explicit
/// [`CODEC_ENVELOPE_KID_KEY`] (issue #948).
///
/// Every pre-rotation row in existence resolves here, so an embedder adopting
/// rotation registers its *current* codec under this id (or keeps it
/// registered by `codec_id` via [`PayloadCodecs::register`], which is the
/// kid-less fallback) and its stored history keeps decoding unchanged.
pub const CODEC_LEGACY_KEY_ID: &str = "legacy";

/// Maximum length in bytes of a codec key id (issue #948).
///
/// Key ids are operator-chosen and appear in stored envelopes, in the
/// `GET /admin/codec/rotation` census, and in the retirement gate's typed
/// error, so they are bounded and restricted to
/// `[A-Za-z0-9._:-]` — see [`PayloadCodecs::register_key`].
pub const MAX_CODEC_KEY_ID_BYTES: usize = 64;

/// `harvest_workers.labels` key a worker advertises its highest readable
/// codec envelope version under (issue #1244).
///
/// Written automatically by `workers::register_worker` /
/// `workers::heartbeat_worker`. It is never operator-configured, so an old
/// binary that has never heard of this label can never claim a version it
/// cannot read. Absence reads as [`CODEC_ENVELOPE_VERSION_LEGACY`] (`1`) — the
/// fail-closed default. `codec_rotation::activate_codec_key` refuses to
/// switch new writes to a keyed codec while any live worker's row is missing
/// this label or names a version below [`CODEC_ENVELOPE_VERSION_KEYED`].
pub const CODEC_ENVELOPE_CAPABILITY_LABEL: &str = "codec_envelope_version";

/// `harvest_workers.labels` key a worker advertises its registered codec key
/// ids under (issue #1244).
///
/// Envelope version alone proves a worker's *binary* can parse a version-2
/// envelope. It proves nothing about whether that worker's `PayloadCodecs`
/// has the *specific* key being activated registered.
///
/// An operator can deploy the new binary fleet-wide before pushing the new
/// key's material everywhere. A worker missing the key cannot decode a
/// payload written under it. So `codec_rotation::activate_codec_key` also
/// refuses while any live worker's row omits the target key id here.
///
/// Written automatically, refreshed on every heartbeat: never
/// operator-configured, mirroring [`CODEC_ENVELOPE_CAPABILITY_LABEL`].
/// Absence reads as "no keys registered" — the fail-closed default.
pub const CODEC_REGISTERED_KEY_IDS_LABEL: &str = "codec_registered_key_ids";

/// Merge this build's highest readable envelope version, and this process's
/// registered codec key ids, into a worker's `labels` JSON (issue #1244).
///
/// `labels` is otherwise entirely operator-chosen (issue #382). These are the
/// two keys the engine itself writes. Both always overwrite any prior
/// value — a worker cannot advertise a capability its own binary and
/// registry do not actually have.
///
/// Non-object `labels` is replaced with a fresh object, rather than silently
/// dropping the capability markers. [`crate::worker::WorkerRegistration`]
/// never actually produces a non-object value; its type does not rule one
/// out.
#[must_use]
pub fn advertise_codec_capability(labels: &Value, registered_key_ids: &[String]) -> Value {
    let mut merged = match labels {
        Value::Object(map) => Value::Object(map.clone()),
        _ => Value::Object(serde_json::Map::new()),
    };
    merged[CODEC_ENVELOPE_CAPABILITY_LABEL] = Value::from(CODEC_ENVELOPE_VERSION_KEYED);
    merged[CODEC_REGISTERED_KEY_IDS_LABEL] = Value::from(registered_key_ids.to_vec());
    merged
}

/// A worker's operator-set capability labels, read back for `requires` /
/// `capable_of` matching (issue #382), as string values only.
///
/// `harvest_workers.labels` now always carries the two engine-owned,
/// non-string entries [`advertise_codec_capability`] writes: an integer under
/// [`CODEC_ENVELOPE_CAPABILITY_LABEL`] and an array under
/// [`CODEC_REGISTERED_KEY_IDS_LABEL`]. A blanket
/// `serde_json::from_value::<HashMap<String, String>>` of the whole object
/// fails the instant either key is present. Every call site historically
/// swallowed that error with `.unwrap_or_default()`, silently discarding
/// every real operator label along with it, on every worker, on every call.
/// That turns `requires`-based routing fleet-wide into a no-op the moment a
/// worker advertises codec capabilities, which happens on every
/// registration and heartbeat.
///
/// This keeps only the string-valued entries instead of failing the whole
/// object. Operator labels keep matching exactly as before, and the two
/// engine-owned keys are simply invisible to capability matching -- neither
/// was ever an operator-settable capability.
#[must_use]
pub fn string_valued_labels(labels: &Value) -> std::collections::HashMap<String, String> {
    labels
        .as_object()
        .into_iter()
        .flat_map(serde_json::Map::iter)
        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
        .collect()
}

/// Undecodable reason: the envelope names a codec that is not registered.
pub const UNDECODABLE_REASON_UNKNOWN_CODEC: &str = "unknown_codec";
/// Undecodable reason: the envelope names an unregistered codec **key id**.
///
/// Issue #948. Typically a key retired too early, or a reader that has not been
/// given the outgoing key during a rotation window.
pub const UNDECODABLE_REASON_UNKNOWN_KEY: &str = "unknown_key";
/// Undecodable reason: the envelope's `data` field is not valid base64.
pub const UNDECODABLE_REASON_INVALID_BASE64: &str = "invalid_base64";
/// Undecodable reason: the codec's `decode` returned an error (bad key,
/// corrupt ciphertext). The codec's own error text is deliberately **not**
/// embedded — it could carry key material.
pub const UNDECODABLE_REASON_CODEC_ERROR: &str = "codec_error";
/// Undecodable reason: the decoded plaintext bytes are not valid JSON.
pub const UNDECODABLE_REASON_INVALID_JSON: &str = "invalid_json";

/// Builds the typed graceful-degrade marker for a payload field the read
/// path could not decode (issue #608):
/// `{"_harvest_undecodable": {"codec_id": <id>, "reason": <reason>}}`.
///
/// `reason` is one of the bounded `UNDECODABLE_REASON_*` strings — the marker
/// never echoes ciphertext or codec error text.
#[must_use]
pub fn undecodable_marker(codec_id: &str, reason: &str) -> Value {
    serde_json::json!({
        UNDECODABLE_MARKER_KEY: {
            "codec_id": codec_id,
            "reason": reason,
        }
    })
}

/// Outcome of a tolerant read-path decode walk (issue #608).
///
/// Counts only — never payload content — so it is safe to thread into audit
/// records and logs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LossyDecodeOutcome {
    /// Number of codec envelopes successfully decoded in place.
    pub decoded: usize,
    /// Number of envelopes replaced with an [`UNDECODABLE_MARKER_KEY`] marker.
    pub failed: usize,
}

impl LossyDecodeOutcome {
    /// Whether the walk decoded or marked at least one envelope.
    // No arithmetic: `merged` saturates, so `decoded + failed` could overflow
    // after a saturated merge — keep the predicate addition-free.
    #[must_use]
    pub const fn touched(&self) -> bool {
        self.decoded > 0 || self.failed > 0
    }

    /// Saturating merge for multi-field / multi-row accumulation.
    #[must_use]
    pub const fn merged(self, other: Self) -> Self {
        Self {
            decoded: self.decoded.saturating_add(other.decoded),
            failed: self.failed.saturating_add(other.failed),
        }
    }
}

/// The parsed contents of a codec envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CodecEnvelopeParts<'a> {
    /// The `codec_id` the payload was encoded with.
    codec_id: &'a str,
    /// The explicit key id, or `None` for a kid-less (legacy) envelope. Use
    /// [`CodecEnvelopeParts::key_id`] to resolve `None` to
    /// [`CODEC_LEGACY_KEY_ID`].
    kid: Option<&'a str>,
    /// The base64 `data` field.
    encoded_b64: &'a str,
}

impl<'a> CodecEnvelopeParts<'a> {
    /// The envelope's effective key id: its explicit `kid`, or
    /// [`CODEC_LEGACY_KEY_ID`] when it carries none (issue #948).
    const fn key_id(&self) -> &'a str {
        match self.kid {
            Some(kid) => kid,
            None => CODEC_LEGACY_KEY_ID,
        }
    }
}

/// The exact codec-envelope shape check shared by the strict
/// (`decode_payload`) and lossy (`decode_value_lossy`) read paths, so the two
/// can never disagree about what an envelope is. Exactly two shapes qualify:
///
/// - [`CODEC_ENVELOPE_VERSION_LEGACY`] with **exactly three** keys —
///   `_harvest_codec_envelope`, string `codec_id`, string `data`. Byte-identical
///   to every envelope written before issue #948.
/// - [`CODEC_ENVELOPE_VERSION_KEYED`] with **exactly four** — those three plus a
///   [`CODEC_ENVELOPE_KID_KEY`] that satisfies [`validate_key_id`].
///
/// Anything else is **not** an envelope: an unknown version, a legacy version
/// carrying a `kid`, a keyed version without one, a non-string or malformed
/// `kid`, five keys. That preserves the pre-#948 strictness against
/// near-envelopes (offload envelopes, erase tombstones, business data carrying
/// its own `codec_id` field), and keeps a four-key **version 1** value
/// classified as plaintext exactly as it was before.
///
/// # The one shape this does reinterpret
///
/// A pre-#948 reader rejected version 2 outright, so business data shaped
/// *exactly* like a version-2 envelope — those four keys, integer `2` under the
/// discriminator, a `kid` passing [`validate_key_id`] — was stored and read back
/// as plaintext, and is now classified as ciphertext. Replay then fails with
/// `UnknownCodecKey`, or, if a key with that id happens to be registered,
/// decodes to garbage the sweep would re-encrypt permanently.
///
/// Bumping the version moved this collision rather than removing it: the
/// envelope is *structurally* indistinguishable from user data that happens to
/// match, and no choice of version number changes that. Eliminating it needs a
/// representation user data cannot coincide with, which is an ADR-0003 envelope
/// change rather than a rotation one. Tracked separately; the probability is
/// remote (four exact keys under a deliberately obscure discriminator) but the
/// guarantee is **not** unconditional, and this comment previously claimed it
/// was.
fn codec_envelope_parts(payload: &Value) -> Option<CodecEnvelopeParts<'_>> {
    let obj = payload.as_object()?;
    let version = obj.get(CODEC_ENVELOPE_KEY).and_then(Value::as_i64)?;
    let codec_id = obj.get("codec_id").and_then(Value::as_str)?;
    let encoded_b64 = obj.get("data").and_then(Value::as_str)?;
    let kid = match (version, obj.len()) {
        (CODEC_ENVELOPE_VERSION_LEGACY, 3) => None,
        (CODEC_ENVELOPE_VERSION_KEYED, 4) => {
            let kid = obj.get(CODEC_ENVELOPE_KID_KEY).and_then(Value::as_str)?;
            // A `kid` read back out of STORAGE is untrusted. On a deployment
            // with no non-identity codec, `encode_payload` stores caller input
            // verbatim, so a workflow can be started with an input that is
            // literally envelope-shaped and carries an arbitrary `kid` — which
            // would otherwise land in the rotation census as an
            // attacker-chosen, unbounded JSON object key, keep
            // `rows_remaining` permanently non-zero (a denial of the retirement
            // procedure), and stream unbounded untrusted text into the sweep's
            // logs. `register_key` applies the same rule on the way in, so a
            // genuinely-written envelope always passes.
            if validate_key_id(kid).is_err() {
                return None;
            }
            Some(kid)
        }
        // Anything else -- version 1 with four keys, version 2 with three, an
        // unknown version -- is NOT an envelope, exactly as before.
        _ => return None,
    };
    Some(CodecEnvelopeParts {
        codec_id,
        kid,
        encoded_b64,
    })
}

/// The codec key id a payload field was encoded under (issue #948), or `None`
/// when `payload` is not a codec envelope at all (plaintext, an offload
/// reference envelope, an erase tombstone).
///
/// A kid-less envelope resolves to [`CODEC_LEGACY_KEY_ID`], so this is the one
/// place the "absent `kid` means legacy" rule is applied for external callers
/// — the re-encryption sweep and the retirement-gate census both read key ids
/// through here rather than reaching into the envelope themselves.
#[must_use]
pub fn codec_envelope_key_id(payload: &Value) -> Option<&str> {
    codec_envelope_parts(payload).map(|parts| parts.key_id())
}

/// `true` when `payload` is a codec envelope written by
/// [`PayloadCodecs::encode_event`] — i.e. an opaque stored value whose real
/// contents this process cannot read without decoding it first.
///
/// Delegates to the single authoritative shape check
/// ([`codec_envelope_parts`]) that the strict and lossy read paths already
/// share, so a caller's "is this opaque?" question can never drift from what
/// the decoder itself recognises.
///
/// Its sibling for the offload half is
/// [`crate::payload_store::extract_offload_ref`]. A reader that derives
/// anything from a payload-bearing field on a RAW (undecoded) history — for
/// example the DAG run graph's issue #780 compensation-dispatch filter — uses
/// these two to decide whether it must run an inflate/decode pass before its
/// derivation is meaningful.
#[must_use]
pub fn is_codec_envelope(payload: &Value) -> bool {
    codec_envelope_parts(payload).is_some()
}

/// A trait for intercepting and transforming raw payload bytes.
///
/// Implementations of this trait are used by the [`PayloadCodecs`] registry
/// to encode and decode fields (such as inputs and outputs) before they are
/// written to or after they are read from the database.
pub trait PayloadCodec: Send + Sync {
    /// Returns a unique identifier for this codec.
    ///
    /// This ID is stored alongside encoded data to ensure that the correct codec
    /// is used when decoding. It must be unique across all registered codecs.
    fn codec_id(&self) -> &'static str;
    /// Encode raw payload bytes for persistence.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] when encoding fails.
    fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, CodecError>;
    /// Decode persisted payload bytes back into raw bytes.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] when decoding fails.
    fn decode(&self, encoded: &[u8]) -> Result<Vec<u8>, CodecError>;
}

/// Represents an error that occurred during encoding or decoding.
///
/// This error is returned by implementations of the [`PayloadCodec`] trait when a transformation fails.
/// For example, if a decryption codec fails due to a bad key, it should return this error.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest::payload_codec::CodecError;
///
/// let error = CodecError("decryption failed: bad key".to_string());
/// assert_eq!(error.to_string(), "payload codec error: decryption failed: bad key");
/// ```
#[derive(Debug, thiserror::Error)]
#[error("payload codec error: {0}")]
pub struct CodecError(pub String);

/// A pass-through codec that performs no transformation.
///
/// **Why does this exist?**
/// By default, the system uses the `IdentityCodec`. This ensures that payloads are written
/// to the database exactly as they are passed to the workflow APIs (as raw JSON). It also
/// provides a safe fallback when no other codec is explicitly configured.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest::payload_codec::{PayloadCodec, IdentityCodec};
///
/// let codec = IdentityCodec;
/// assert_eq!(codec.codec_id(), "identity");
///
/// let raw_data = b"hello world";
/// let encoded = codec.encode(raw_data).unwrap();
/// assert_eq!(encoded, raw_data); // Identity changes nothing!
/// ```
#[derive(Debug, Default)]
pub struct IdentityCodec;

impl PayloadCodec for IdentityCodec {
    fn codec_id(&self) -> &'static str {
        "identity"
    }
    fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(raw.to_vec())
    }
    fn decode(&self, encoded: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(encoded.to_vec())
    }
}

/// A registry of available payload codecs and the configured default.
///
/// **Why does this exist?**
/// When decoding a payload, the system needs to know which codec to use. Because
/// a workflow's history might span a long period, it may contain payloads encoded
/// with different codecs (e.g., if you migrated from "encryption-v1" to "encryption-v2").
/// The `PayloadCodecs` struct holds all known codecs so it can dynamically look up
/// the right one based on the `codec_id` stored alongside the data.
///
/// The `default` codec is the one used for encoding *new* payloads.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest::payload_codec::{PayloadCodecs, IdentityCodec};
/// use std::sync::Arc;
///
/// let mut codecs = PayloadCodecs::default();
/// // The default is automatically IdentityCodec.
/// // But you can change the default for encoding new payloads:
/// codecs.set_default(Arc::new(IdentityCodec));
/// ```
#[derive(Clone)]
pub struct PayloadCodecs {
    default: Arc<dyn PayloadCodec>,
    codecs: BTreeMap<&'static str, Arc<dyn PayloadCodec>>,
    /// Key-rotation state (issue #948), deliberately **shared** across clones.
    ///
    /// `PayloadCodecs` is cloned into the worker, the store call sites, and the
    /// management API at build time. If the active key id lived in the cloned
    /// value, a clone captured before a rotation would keep encrypting under
    /// the retired key for the life of the process — the exact
    /// restart-ordering window AC2 forbids. Behind an `Arc<RwLock<_>>` every
    /// clone observes [`PayloadCodecs::set_active_key`] the instant it returns.
    keyed: Arc<RwLock<KeyRegistry>>,
    /// Lock-free mirror of "is the keyed registry non-empty?".
    ///
    /// `encode_payload` and `resolve_decoder` run per payload field per event on
    /// the hot write/read paths, and the overwhelmingly common case is a
    /// deployment that has never registered a keyed codec at all. Consulting an
    /// atomic instead of taking the `RwLock` keeps that case free. Written only
    /// under the write lock, so it can never claim keys exist when they do not.
    any_keys: Arc<AtomicBool>,
}

/// The multi-key half of the codec registry (issue #948): every registered
/// keyed codec plus the single active key id used for new writes.
struct KeyRegistry {
    keys: BTreeMap<String, Arc<dyn PayloadCodec>>,
    /// Exactly one key id is active at a time. Defaults to
    /// [`CODEC_LEGACY_KEY_ID`], which is also the resolution target for every
    /// kid-less stored envelope.
    active: String,
    /// Count of live [`SweepKeyPin`] guards per key id (issue #1251).
    ///
    /// A re-encryption batch pins the key it is about to write rows onto for
    /// the whole batch. [`PayloadCodecs::retire_key_local`] refuses a key with
    /// a non-zero count here, under the same write lock that would otherwise
    /// let it race a batch's own commit. Several shards can pin the same key
    /// at once, so this counts rather than flags.
    sweep_pins: BTreeMap<String, u64>,
}

impl Default for PayloadCodecs {
    fn default() -> Self {
        let identity: Arc<dyn PayloadCodec> = Arc::new(IdentityCodec);
        let mut codecs = BTreeMap::new();
        codecs.insert(identity.codec_id(), identity.clone());
        Self {
            default: identity,
            codecs,
            keyed: Arc::new(RwLock::new(KeyRegistry {
                keys: BTreeMap::new(),
                active: CODEC_LEGACY_KEY_ID.to_string(),
                sweep_pins: BTreeMap::new(),
            })),
            any_keys: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl std::fmt::Debug for PayloadCodecs {
    /// Never prints codec state that could carry key material — only the
    /// registered identifiers, which are operator-chosen labels.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let keyed = self.keys_read();
        f.debug_struct("PayloadCodecs")
            .field("default_codec_id", &self.default.codec_id())
            .field("codec_ids", &self.codecs.keys().collect::<Vec<_>>())
            .field("key_ids", &keyed.keys.keys().collect::<Vec<_>>())
            .field("active_key_id", &keyed.active)
            // `finish_non_exhaustive`: the `keyed` field itself is deliberately
            // not printed as a field — only the identifiers read out of it —
            // because it holds live codec handles that may close over key
            // material.
            .finish_non_exhaustive()
    }
}

/// Validate an operator-supplied codec key id (issue #948).
///
/// Key ids are persisted inside stored envelopes and echoed in the rotation
/// census and the retirement gate's error, so they are bounded and restricted
/// to a conservative ASCII alphabet.
fn validate_key_id(key_id: &str) -> HarvestResult<()> {
    if key_id.is_empty() {
        return Err(HarvestError::Config(
            "codec key id must not be empty".to_string(),
        ));
    }
    if key_id.len() > MAX_CODEC_KEY_ID_BYTES {
        return Err(HarvestError::Config(format!(
            "codec key id {key_id:?} is {} bytes; the maximum is {MAX_CODEC_KEY_ID_BYTES}",
            key_id.len()
        )));
    }
    if !key_id
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':'))
    {
        return Err(HarvestError::Config(format!(
            "codec key id {key_id:?} must contain only ASCII alphanumerics and `-_.:`"
        )));
    }
    Ok(())
}

impl PayloadCodecs {
    /// Registers a codec so it can be used for decoding.
    ///
    /// The codec will only be used if an incoming payload is marked with its `codec_id`.
    /// Registering a codec does not make it the default for new payloads (use [`PayloadCodecs::set_default`] for that).
    pub fn register(&mut self, codec: Arc<dyn PayloadCodec>) {
        self.codecs.insert(codec.codec_id(), codec);
    }

    /// Sets the default codec used for encoding new payloads.
    ///
    /// The codec will also be implicitly registered for decoding.
    /// You should typically call this when starting up your Harvest worker
    /// or server to ensure all newly written payloads use this codec.
    pub fn set_default(&mut self, codec: Arc<dyn PayloadCodec>) {
        self.register(codec.clone());
        self.default = codec;
    }

    // ── Key rotation (issue #948) ────────────────────────────────────────

    fn keys_read(&self) -> RwLockReadGuard<'_, KeyRegistry> {
        // A panic inside a codec while a guard is held must not brick payload
        // decoding for the rest of the process: recover the inner state rather
        // than propagating the poison.
        self.keyed.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn keys_write(&self) -> RwLockWriteGuard<'_, KeyRegistry> {
        self.keyed.write().unwrap_or_else(PoisonError::into_inner)
    }

    /// Register a keyed codec under `key_id` (issue #948).
    ///
    /// Unlike [`PayloadCodecs::register`], which is keyed by the codec's own
    /// `codec_id`, this is keyed by **key material identity**: rotation means
    /// two codecs that share a `codec_id` (`"aes-gcm"`) and differ only in the
    /// key they hold, so `codec_id` cannot distinguish them and this registry
    /// deliberately does not consult it.
    ///
    /// The **first** key registered on an empty registry becomes the active
    /// key, so a single-key deployment needs no separate activation call. Use
    /// [`PayloadCodecs::set_active_key`] to rotate.
    ///
    /// **Registrations are immutable.** Re-registering an existing key id is
    /// refused, not silently replaced: a config reload that bound the same id
    /// to different key material would otherwise destroy the *only* decoder for
    /// every stored envelope bearing that `kid`, and the sweep could not repair
    /// them — it classifies their unchanged key id as already active and skips
    /// them. Failing loudly at registration is the only recoverable outcome. To
    /// re-assert a configuration, build a fresh [`PayloadCodecs`].
    ///
    /// Takes `&self`: rotation state is shared across clones of the registry,
    /// so a key may be added at runtime (a config reload) and every existing
    /// clone sees it.
    ///
    /// # Errors
    ///
    /// [`HarvestError::Config`] when `key_id` is empty, longer than
    /// [`MAX_CODEC_KEY_ID_BYTES`], contains anything outside ASCII
    /// alphanumerics and `-_.:`, or is **already registered**.
    pub fn register_key(&self, key_id: &str, codec: Arc<dyn PayloadCodec>) -> HarvestResult<()> {
        validate_key_id(key_id)?;
        let mut guard = self.keys_write();
        if guard.keys.contains_key(key_id) {
            return Err(HarvestError::Config(format!(
                "codec key id {key_id:?} is already registered; key registrations are \
                 immutable because replacing one would destroy the only decoder for every \
                 stored envelope bearing that key id"
            )));
        }
        let first = guard.keys.is_empty();
        guard.keys.insert(key_id.to_string(), codec);
        if first {
            guard.active = key_id.to_string();
        }
        // Publish the mirror while STILL holding the write guard. Storing it
        // after `drop(guard)` leaves it unordered against the map mutation, so
        // a concurrent `retire_key_local` that removed the last key can compute
        // `false`, lose the race to this insert, and then overwrite this `true`
        // -- leaving a non-empty registry advertising itself as empty, which
        // makes `resolve_decoder` skip the keyed lookup and encoding fall back
        // to the default codec. Under the lock, the two stores are ordered by
        // the same mutex that orders the map.
        self.any_keys.store(true, Ordering::Release);
        drop(guard);
        Ok(())
    }

    /// Make `key_id` the active key: **all** new writes encode under it from
    /// the moment this returns (issue #948, AC2).
    ///
    /// Because the rotation state is shared across every clone of this
    /// registry, there is no restart-ordering window in which a clone taken
    /// before the flip keeps writing under the old key.
    ///
    /// # ⚠️ Rollout ordering: upgrade every reader before you activate
    ///
    /// Activating a **non-legacy** key switches new writes to envelope version
    /// 2 ([`CODEC_ENVELOPE_VERSION_KEYED`]), which carries a `kid` and so has
    /// four keys instead of three. A reader built before issue #948 recognises
    /// an envelope only as *exactly three keys with version 1*, and its decoder
    /// returns anything else **unchanged**:
    ///
    /// ```text
    /// let Some(parts) = codec_envelope_parts(payload) else { return Ok(payload.clone()) };
    /// ```
    ///
    /// So a pre-#948 worker does not reject a version-2 envelope loudly — it
    /// hands the raw `{_harvest_codec_envelope, codec_id, kid, data}` object to
    /// workflow code as if it were the payload. That is silent wrong data, not
    /// an error.
    ///
    /// Therefore: **deploy the version-2-capable binary to every reader in the
    /// fleet first, and only then activate a keyed codec.** While the legacy
    /// key is active no `kid` is written and envelopes stay version 1, so the
    /// upgrade itself is safe to roll out in any order — it is the *activation*
    /// that must come last. This crate cannot enforce the ordering: it has no
    /// fleet-wide view of which binaries are running.
    ///
    /// # Errors
    ///
    /// [`HarvestError::Config`] when `key_id` is not registered. The active key
    /// is left unchanged — a rotation onto a key this process cannot encode
    /// with must fail loudly rather than half-apply.
    pub fn set_active_key(&self, key_id: &str) -> HarvestResult<()> {
        let mut guard = self.keys_write();
        if !guard.keys.contains_key(key_id) {
            return Err(HarvestError::Config(format!(
                "cannot activate unregistered codec key id {key_id:?}; register it with \
                 `PayloadCodecs::register_key` first"
            )));
        }
        guard.active = key_id.to_string();
        drop(guard);
        Ok(())
    }

    /// The key id new writes are currently encoded under (issue #948).
    ///
    /// [`CODEC_LEGACY_KEY_ID`] when no keyed codec is registered, which is also
    /// the key id every kid-less stored envelope resolves to.
    #[must_use]
    pub fn active_key_id(&self) -> String {
        self.keys_read().active.clone()
    }

    /// Every registered key id, sorted (issue #948).
    #[must_use]
    pub fn registered_key_ids(&self) -> Vec<String> {
        self.keys_read().keys.keys().cloned().collect()
    }

    /// Whether any keyed codec is registered (issue #948).
    ///
    /// `false` on every deployment that has not adopted rotation, which is what
    /// lets the re-encryption sweep return without touching a connection.
    #[must_use]
    pub fn has_keyed_codecs(&self) -> bool {
        self.any_keys.load(Ordering::Acquire)
    }

    /// The codec registered under `key_id`, if any (issue #948).
    #[must_use]
    pub fn codec_for_key(&self, key_id: &str) -> Option<Arc<dyn PayloadCodec>> {
        self.keys_read().keys.get(key_id).map(Arc::clone)
    }

    /// Pin `key_id` so it cannot be retired while the returned guard lives
    /// (issue #1251).
    ///
    /// A re-encryption batch resolves its target key once and writes every row
    /// of the batch under that exact id (see
    /// [`crate::codec_rotation::reencrypt_event_payload_fields_under`]). Without
    /// a pin, a second rotation can move the active key away from the batch's
    /// target *while the batch is still running*.
    /// [`PayloadCodecs::retire_key_local`] can then drop that target. Its own
    /// census reads zero rows, because the batch has not committed yet. The
    /// next row the batch writes then lands under a key that no longer has a
    /// decoder anywhere: silent, permanent data loss.
    ///
    /// Call this once per batch, right after resolving the target key id, and
    /// hold the guard for the whole batch. Retirement then fails closed
    /// instead of racing: see [`PayloadCodecs::retire_key_local`].
    ///
    /// Several batches — one per shard, say — may pin the same key id at once;
    /// the pin is a count, not a flag.
    ///
    /// This is `#[doc(hidden)] pub` rather than `pub(crate)`. The
    /// batch-oriented public sweep entry point calls this internally. But
    /// the race this guards is exercised directly by an integration test
    /// outside this crate. Not part of the semver-stable surface.
    #[doc(hidden)]
    #[must_use = "dropping the guard immediately un-pins the key"]
    pub fn pin_key_for_sweep(&self, key_id: &str) -> SweepKeyPin {
        let mut guard = self.keys_write();
        *guard.sweep_pins.entry(key_id.to_string()).or_insert(0) += 1;
        drop(guard);
        SweepKeyPin {
            codecs: self.clone(),
            key_id: key_id.to_string(),
        }
    }

    /// Release one pin taken by [`PayloadCodecs::pin_key_for_sweep`].
    ///
    /// Only [`SweepKeyPin::drop`] calls this. A pin therefore always
    /// releases, even when its batch returns early on an error. An unpin
    /// lost to a failed batch would block that key's retirement forever.
    fn unpin_key_for_sweep(&self, key_id: &str) {
        let mut guard = self.keys_write();
        if let Some(count) = guard.sweep_pins.get_mut(key_id) {
            *count -= 1;
            if *count == 0 {
                guard.sweep_pins.remove(key_id);
            }
        }
        drop(guard);
    }

    /// Whether an in-flight sweep batch currently holds a pin on `key_id`
    /// (issue #1251). See [`PayloadCodecs::pin_key_for_sweep`].
    ///
    /// `#[doc(hidden)] pub` for the same reason as `pin_key_for_sweep`: an
    /// integration test outside this crate asserts on it directly.
    #[doc(hidden)]
    #[must_use]
    pub fn is_pinned_by_sweep(&self, key_id: &str) -> bool {
        self.keys_read().sweep_pins.contains_key(key_id)
    }

    /// Drop `key_id` from this process's in-memory registry (issue #948).
    ///
    /// This is the **local** half of retirement and carries no storage
    /// guarantee of its own. The gate that proves no stored row still depends
    /// on the key lives in [`crate::codec_rotation`]; call that instead unless
    /// you have independently established the same fact.
    ///
    /// # Errors
    ///
    /// [`HarvestError::Config`] when `key_id` is the active key. Retiring the
    /// key new writes are being encoded under would immediately produce
    /// undecodable history. The same error applies when a sweep batch holds
    /// a pin on it (issue #1251; see [`PayloadCodecs::pin_key_for_sweep`]).
    pub fn retire_key_local(&self, key_id: &str) -> HarvestResult<()> {
        let mut guard = self.keys_write();
        if guard.sweep_pins.contains_key(key_id) {
            return Err(HarvestError::Config(format!(
                "codec key id {key_id:?} is pinned by an in-flight re-encryption sweep batch \
                 and cannot be retired yet; retry once the batch completes"
            )));
        }
        if guard.active == key_id {
            return Err(HarvestError::Config(format!(
                "codec key id {key_id:?} is the active key and cannot be retired; \
                 activate a different key first"
            )));
        }
        guard.keys.remove(key_id);
        let any_left = !guard.keys.is_empty();
        // Under the guard, for the reason spelled out in `register_key`: the
        // mirror and the map must be mutated inside the same critical section
        // or a concurrent registration can be undone by this store.
        self.any_keys.store(any_left, Ordering::Release);
        drop(guard);
        Ok(())
    }

    /// The `(key_id, codec)` pair new writes encode under.
    ///
    /// Falls back to the [`PayloadCodecs::set_default`] codec under
    /// [`CODEC_LEGACY_KEY_ID`] when no keyed codec is registered, so a
    /// pre-rotation deployment behaves exactly as it did before issue #948.
    fn active_codec(&self) -> (Option<String>, Arc<dyn PayloadCodec>) {
        if !self.has_keyed_codecs() {
            // The un-rotated path: no lock, no allocation. `None` means the
            // legacy key id, which is exactly the envelope form that omits
            // `kid`.
            return (None, Arc::clone(&self.default));
        }
        let guard = self.keys_read();
        if let Some(codec) = guard.keys.get(&guard.active) {
            let key_id = (guard.active != CODEC_LEGACY_KEY_ID).then(|| guard.active.clone());
            return (key_id, Arc::clone(codec));
        }
        drop(guard);
        (None, Arc::clone(&self.default))
    }

    /// The `codec_id` of the codec new writes would encode under, or `None`
    /// when no keyed codec is registered.
    ///
    /// A single lock acquisition and no allocation — the sweep's
    /// "would activating this key decrypt history?" guard calls it per batch.
    #[must_use]
    pub fn active_codec_id(&self) -> Option<&'static str> {
        if !self.has_keyed_codecs() {
            return None;
        }
        let guard = self.keys_read();
        guard.keys.get(&guard.active).map(|codec| codec.codec_id())
    }

    /// Resolve the codec that can decode an envelope's `(codec_id, kid)` pair.
    ///
    /// Resolution order:
    ///
    /// - An envelope naming an explicit `kid` resolves **only** through the
    ///   keyed registry, by exact key id. It deliberately does not fall back to
    ///   the `codec_id` map: two keys share a `codec_id` during a rotation, so
    ///   that fallback would decode with the wrong key material and produce
    ///   garbage instead of a clean failure.
    /// - A **kid-less** envelope resolves to the [`CODEC_LEGACY_KEY_ID`] entry
    ///   *only when that entry's own `codec_id` matches the envelope's*,
    ///   otherwise through the pre-#948 `codec_id` map — so a deployment using
    ///   today's [`PayloadCodecs::register`] / [`PayloadCodecs::set_default`]
    ///   API keeps decoding its own history verbatim after adopting rotation.
    ///
    /// That `codec_id` match on the legacy entry is load-bearing. Kid-less
    /// history can span *several* codec ids — an embedder who migrated
    /// `"aes-v1"` to `"aes-v2"` before rotation existed has both in the
    /// `codec_id` map. Letting the legacy keyed entry win unconditionally would
    /// decode every one of those rows with whichever codec happened to be
    /// registered under `legacy`: the wrong algorithm and the wrong key. An
    /// unauthenticated codec can return plausible-but-wrong bytes rather than
    /// failing, and the sweep would then re-encrypt that garbage under the
    /// active key — silently and permanently destroying the payload.
    fn resolve_decoder(&self, parts: &CodecEnvelopeParts<'_>) -> Option<Arc<dyn PayloadCodec>> {
        // Bind and release the guard before touching `self.codecs`, so the
        // rotation lock is never held across the fallback lookup.
        let keyed = if self.has_keyed_codecs() {
            let guard = self.keys_read();
            guard.keys.get(parts.key_id()).map(Arc::clone)
        } else {
            None
        };
        let Some(codec) = keyed else {
            // No keyed entry for this key id. A kid-less envelope may still
            // resolve through the pre-#948 map.
            return if parts.kid.is_none() {
                self.codecs.get(parts.codec_id).map(Arc::clone)
            } else {
                None
            };
        };
        if codec.codec_id() == parts.codec_id {
            return Some(codec);
        }
        if parts.kid.is_some() {
            // An EXPLICIT kid resolved to a registered key, but the envelope
            // names a different algorithm than that key is registered with. It
            // is malformed -- corrupted, imported from elsewhere, or crafted --
            // and there is no safe reading of it. Decoding under the key's own
            // codec anyway is the exact hazard this function's doc names: an
            // unauthenticated codec returns plausible-but-wrong bytes instead
            // of failing, and the sweep re-encrypts that garbage under the
            // active key, destroying the payload permanently.
            //
            // Falling back to the codec_id map would be just as wrong: the
            // envelope named a KEY, and resolving it through a codec unrelated
            // to that key is the confusion being rejected. Fail cleanly, so
            // strict decode raises `UnknownCodecKey` and lossy decode leaves a
            // bounded marker.
            return None;
        }
        // Kid-less, and the legacy entry is for a DIFFERENT codec than this
        // envelope names. Defer to the codec_id map, which knows the right one.
        self.codecs.get(parts.codec_id).map(Arc::clone)
    }

    /// Encode payload-bearing fields inside an event into codec envelopes.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError`] if event serialization or codec encoding fails.
    pub fn encode_event(&self, event: &crate::event::WorkflowEvent) -> HarvestResult<Value> {
        let mut value = serde_json::to_value(event)?;
        self.transform_event_data(&mut value, true)?;
        Ok(value)
    }

    /// Decode codec envelopes in a serialized event back to raw JSON payloads.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError`] if codec decoding or event deserialization fails.
    pub fn decode_event(&self, mut value: Value) -> HarvestResult<crate::event::WorkflowEvent> {
        self.transform_event_data(&mut value, false)?;
        Ok(serde_json::from_value(value)?)
    }

    fn transform_event_data(&self, root: &mut Value, encode: bool) -> HarvestResult<()> {
        let Some(data) = root.get_mut("data") else {
            return Ok(());
        };
        // `value` is the arbitrary `ctx.side_effect(...)` closure result on a
        // SideEffectRecorded event (issue #384). Pre-#384 custom side effects
        // were stored under MarkerRecorded.details and were codec-encoded; the
        // new field must be encoded too so a configured codec still encrypts /
        // compresses any secret or PII the closure captured. `value` is unique
        // to SideEffectRecorded among event variants, so no other event is
        // affected.
        // `last_completion_result` is a copy of a prior run's output frozen into the
        // WorkflowStarted event for scheduled carryover (issue #488). It must be encoded
        // too so a configured codec encrypts/compresses any secret or PII the prior
        // output carried; it is unique to WorkflowStarted among event variants.
        for key in crate::payload_store::PAYLOAD_FIELD_KEYS {
            if let Some(payload) = data.get_mut(key) {
                if encode {
                    *payload = self.encode_payload(payload)?;
                } else {
                    *payload = self.decode_payload(payload)?;
                }
            }
        }
        Ok(())
    }

    /// Encode ONE payload-bearing field into a codec envelope under the
    /// **active** key (issue #948).
    ///
    /// A no-op returning `payload` unchanged when the active codec is the
    /// identity codec. The emitted envelope carries a
    /// [`CODEC_ENVELOPE_KID_KEY`] only when the active key id is not
    /// [`CODEC_LEGACY_KEY_ID`], so an un-rotated deployment's stored bytes are
    /// byte-identical to pre-#948.
    ///
    /// Public so the re-encryption sweep can re-encode a field it has just
    /// decoded with a retired key, without duplicating the envelope-writing
    /// rules.
    ///
    /// # Errors
    ///
    /// [`HarvestError`] when serialization or the codec's `encode` fails.
    pub fn encode_payload(&self, payload: &Value) -> HarvestResult<Value> {
        let (key_id, codec) = self.active_codec();
        if codec.codec_id() == "identity" {
            return Ok(payload.clone());
        }
        let raw = serde_json::to_vec(payload)?;
        let encoded = codec
            .encode(&raw)
            .map_err(|e| HarvestError::Config(e.to_string()))?;
        Ok(Self::envelope(
            codec.codec_id(),
            key_id.as_deref().unwrap_or(CODEC_LEGACY_KEY_ID),
            &encoded,
        ))
    }

    /// Encode ONE payload-bearing field under a **specific registered key id**
    /// (issue #948).
    ///
    /// The re-encryption sweep uses this rather than
    /// [`PayloadCodecs::encode_payload`] for two reasons, both correctness:
    ///
    /// 1. **It cannot silently decrypt.** `encode_payload` returns the payload
    ///    unchanged when the active codec is the identity codec — correct on
    ///    the write path, catastrophic on the sweep path, where the value in
    ///    hand is freshly decoded *plaintext* and returning it unchanged would
    ///    commit cleartext over ciphertext. Because the active key can be
    ///    flipped at runtime from another thread, a check-then-encode against
    ///    `encode_payload` has a real window; this refuses an identity codec at
    ///    the point of use, closing it structurally.
    /// 2. **It pins one key for a whole row.** Re-encoding each field through
    ///    "whatever is active right now" could straddle a mid-row flip and
    ///    leave a half-rotated row. The sweep resolves the key id once and
    ///    passes it here for every field.
    ///
    /// Emits a [`CODEC_ENVELOPE_KID_KEY`] only when `key_id` is not
    /// [`CODEC_LEGACY_KEY_ID`], exactly like `encode_payload`.
    ///
    /// # Errors
    ///
    /// - [`HarvestError::UnknownCodecKey`] when `key_id` is not registered.
    /// - [`HarvestError::Config`] when the codec registered under `key_id` is
    ///   the identity codec (see reason 1 above).
    /// - [`HarvestError`] when serialization or the codec's `encode` fails.
    pub fn encode_payload_under(&self, key_id: &str, payload: &Value) -> HarvestResult<Value> {
        let raw = serde_json::to_vec(payload)?;
        self.encode_payload_bytes_under(key_id, &raw)
    }

    /// [`PayloadCodecs::encode_payload_under`], taking raw plaintext bytes
    /// rather than a `Value` to serialize (issue #948).
    ///
    /// Pairs with [`PayloadCodecs::decode_payload_bytes`] so the
    /// re-encryption sweep ([`crate::codec_rotation`]) can migrate a field's
    /// ciphertext onto a new key without ever parsing or re-serializing its
    /// JSON structure — it rewrites ciphertext only and never needs the
    /// decoded value.
    ///
    /// # Errors
    ///
    /// As [`PayloadCodecs::encode_payload_under`].
    pub(crate) fn encode_payload_bytes_under(
        &self,
        key_id: &str,
        raw: &[u8],
    ) -> HarvestResult<Value> {
        let codec = self
            .codec_for_key(key_id)
            .ok_or_else(|| HarvestError::UnknownCodecKey {
                key_id: key_id.to_string(),
                codec_id: String::new(),
            })?;
        if codec.codec_id() == "identity" {
            return Err(HarvestError::Config(format!(
                "refusing to encode under codec key id {key_id:?}: it is registered to the \
                 identity codec, and writing its output would replace stored ciphertext with \
                 plaintext"
            )));
        }
        let encoded = codec
            .encode(raw)
            .map_err(|e| HarvestError::Config(e.to_string()))?;
        Ok(Self::envelope(codec.codec_id(), key_id, &encoded))
    }

    /// Build the stored envelope for `encoded`, omitting `kid` for the legacy
    /// key id so an un-rotated deployment's bytes stay byte-identical to
    /// pre-#948.
    fn envelope(codec_id: &str, key_id: &str, encoded: &[u8]) -> Value {
        let keyed = key_id != CODEC_LEGACY_KEY_ID;
        let mut envelope = serde_json::Map::with_capacity(4);
        envelope.insert(
            CODEC_ENVELOPE_KEY.to_string(),
            Value::from(if keyed {
                CODEC_ENVELOPE_VERSION_KEYED
            } else {
                CODEC_ENVELOPE_VERSION_LEGACY
            }),
        );
        envelope.insert("codec_id".to_string(), Value::from(codec_id));
        if keyed {
            envelope.insert(CODEC_ENVELOPE_KID_KEY.to_string(), Value::from(key_id));
        }
        envelope.insert(
            "data".to_string(),
            Value::from(base64::engine::general_purpose::STANDARD.encode(encoded)),
        );
        Value::Object(envelope)
    }

    /// Decode ONE payload-bearing field, resolving whichever registered key the
    /// envelope names (issue #948) — so a mixed-key history decodes
    /// transparently throughout a rotation window.
    ///
    /// Returns `payload` unchanged when it is not a codec envelope (plaintext,
    /// an offload reference envelope, an erase tombstone).
    ///
    /// Public for the same reason as [`PayloadCodecs::encode_payload`].
    ///
    /// # Errors
    ///
    /// - [`HarvestError::UnknownCodecKey`] when the envelope names an explicit
    ///   key id that is not registered (a key retired too early).
    /// - [`HarvestError::UnknownPayloadCodec`] when a kid-less envelope names a
    ///   `codec_id` that is not registered.
    /// - [`HarvestError`] on invalid base64, a codec `decode` failure, or
    ///   plaintext that is not valid JSON.
    pub fn decode_payload(&self, payload: &Value) -> HarvestResult<Value> {
        let Some(parts) = codec_envelope_parts(payload) else {
            return Ok(payload.clone());
        };
        let decoded = self.decode_envelope_bytes(&parts)?;
        Ok(serde_json::from_slice(&decoded)?)
    }

    /// [`PayloadCodecs::decode_payload`], returning the raw plaintext bytes
    /// instead of a parsed `Value` (issue #948).
    ///
    /// The re-encryption sweep ([`crate::codec_rotation`]) migrates a field's
    /// ciphertext from one registered key to another; it never inspects or
    /// modifies the decoded structure, so materializing a full `Value` here
    /// only for [`PayloadCodecs::encode_payload_bytes_under`] to immediately
    /// re-serialize it back to bytes is a `serde_json::Value` tree — with its
    /// heap-backed `Map` — built and torn down for nothing, on every field,
    /// of every row, of every sweep batch, forever, on any deployment that
    /// rotates keys.
    ///
    /// The bytes are still **validated** as well-formed JSON — via
    /// `serde_json::from_slice::<serde::de::IgnoredAny>`, which walks the
    /// document to confirm its syntax without allocating a `Value` for it —
    /// so a codec that "successfully" decodes corrupt or mismatched
    /// ciphertext into garbage bytes is caught here exactly as
    /// [`PayloadCodecs::decode_payload`] would catch it, and the sweep counts
    /// the row unresolved rather than writing the garbage back out under a
    /// new key.
    ///
    /// Returns `Ok(None)` when `payload` is not a codec envelope, exactly
    /// like [`codec_envelope_key_id`] returning `None`. The re-encryption
    /// sweep has already confirmed `payload` names a key id via that function
    /// before calling this one, so `None` is unreachable at its call site —
    /// but it is returned rather than assumed, the same defensive choice
    /// [`crate::codec_rotation::reencrypt_event_payload_fields_under`] makes
    /// for its own "cannot happen" branch.
    ///
    /// # Errors
    ///
    /// As [`PayloadCodecs::decode_payload`], including
    /// [`HarvestError::Serialization`] when the decoded bytes are not valid
    /// JSON.
    pub(crate) fn decode_payload_bytes(&self, payload: &Value) -> HarvestResult<Option<Vec<u8>>> {
        let Some(parts) = codec_envelope_parts(payload) else {
            return Ok(None);
        };
        let decoded = self.decode_envelope_bytes(&parts)?;
        serde_json::from_slice::<serde::de::IgnoredAny>(&decoded)?;
        Ok(Some(decoded))
    }

    /// Decode one already-shape-verified envelope's ciphertext to raw
    /// plaintext bytes. Shared by [`PayloadCodecs::decode_payload`] (which
    /// parses the result as JSON) and [`PayloadCodecs::decode_payload_bytes`]
    /// (which does not).
    fn decode_envelope_bytes(&self, parts: &CodecEnvelopeParts<'_>) -> HarvestResult<Vec<u8>> {
        let codec = self.resolve_decoder(parts).ok_or_else(|| {
            if parts.kid.is_some() {
                HarvestError::UnknownCodecKey {
                    key_id: parts.key_id().to_string(),
                    codec_id: parts.codec_id.to_string(),
                }
            } else {
                HarvestError::UnknownPayloadCodec {
                    id: parts.codec_id.to_string(),
                }
            }
        })?;
        let encoded = base64::engine::general_purpose::STANDARD
            .decode(parts.encoded_b64)
            .map_err(|e| HarvestError::Config(e.to_string()))?;
        codec
            .decode(&encoded)
            .map_err(|e| HarvestError::Config(e.to_string()))
    }

    /// Decode one already-shape-verified envelope, mapping every failure mode
    /// to the typed [`undecodable_marker`] instead of an error (issue #608).
    ///
    /// `Ok(plaintext)` on success, `Err(marker)` on failure — the caller
    /// substitutes whichever it gets, so a bad key / rotated-away codec can
    /// never fail the surrounding response.
    fn decode_envelope_lossy(&self, parts: &CodecEnvelopeParts<'_>) -> Result<Value, Value> {
        let codec_id = parts.codec_id;
        let Some(codec) = self.resolve_decoder(parts) else {
            // An explicit, unregistered `kid` is a *key* miss (issue #948); a
            // kid-less envelope whose `codec_id` is unknown keeps the pre-#948
            // reason so existing operator tooling reads unchanged.
            let reason = if parts.kid.is_some() {
                UNDECODABLE_REASON_UNKNOWN_KEY
            } else {
                UNDECODABLE_REASON_UNKNOWN_CODEC
            };
            return Err(undecodable_marker(codec_id, reason));
        };
        let Ok(encoded) = base64::engine::general_purpose::STANDARD.decode(parts.encoded_b64)
        else {
            return Err(undecodable_marker(
                codec_id,
                UNDECODABLE_REASON_INVALID_BASE64,
            ));
        };
        let Ok(decoded) = codec.decode(&encoded) else {
            return Err(undecodable_marker(codec_id, UNDECODABLE_REASON_CODEC_ERROR));
        };
        serde_json::from_slice(&decoded)
            .map_err(|_| undecodable_marker(codec_id, UNDECODABLE_REASON_INVALID_JSON))
    }

    /// Tolerantly decode every codec envelope found anywhere inside `value`,
    /// in place, for the operator read path (issue #608).
    ///
    /// Infallible: a per-envelope failure replaces that one field with an
    /// [`UNDECODABLE_MARKER_KEY`] marker (bounded `UNDECODABLE_REASON_*`
    /// reason, never ciphertext or codec error text) and the walk continues —
    /// one un-decryptable field never fails the surrounding response.
    ///
    /// Single-pass: a decoded result is **never** re-scanned, so
    /// envelope-shaped plaintext (business data, a frozen
    /// `last_completion_result` copy) is preserved verbatim as data. Offload
    /// envelopes (`_harvest_offload_envelope`), erase tombstones
    /// (`_harvest_erased`), and malformed near-envelopes pass through
    /// untouched, exactly as the strict `decode_event` path tolerates them.
    ///
    /// This is a read-path helper for in-memory response copies only — it
    /// must never be applied to values that are written back to storage.
    pub fn decode_value_lossy(&self, value: &mut Value) -> LossyDecodeOutcome {
        let mut outcome = LossyDecodeOutcome::default();
        self.decode_value_lossy_inner(value, &mut outcome);
        outcome
    }

    fn decode_value_lossy_inner(&self, value: &mut Value, outcome: &mut LossyDecodeOutcome) {
        let replacement =
            codec_envelope_parts(value).map(|parts| self.decode_envelope_lossy(&parts));
        if let Some(result) = replacement {
            match result {
                Ok(plaintext) => {
                    outcome.decoded = outcome.decoded.saturating_add(1);
                    *value = plaintext;
                }
                Err(marker) => {
                    outcome.failed = outcome.failed.saturating_add(1);
                    *value = marker;
                }
            }
            // Single-pass rule: never recurse into a decoded result.
            return;
        }
        match value {
            Value::Object(map) => {
                for child in map.values_mut() {
                    self.decode_value_lossy_inner(child, outcome);
                }
            }
            Value::Array(items) => {
                for child in items.iter_mut() {
                    self.decode_value_lossy_inner(child, outcome);
                }
            }
            _ => {}
        }
    }

    /// Tolerant decode for TEXT columns (execution / dead-letter `error`)
    /// that may carry a serialized codec envelope (issue #608).
    ///
    /// Returns `(Some(decoded), outcome)` only when `raw` parses as JSON and
    /// that JSON is **exactly** a codec envelope; `(None, default)` means
    /// "leave the original string untouched" (plain error text, or JSON that
    /// is not an envelope). A decoded plain-string payload is returned
    /// unwrapped (the original error was a plain string before encoding);
    /// any other decoded JSON is returned serialized. A decode failure
    /// returns the [`undecodable_marker`] serialized to a string, counted in
    /// the outcome as `failed`.
    #[must_use]
    pub fn decode_error_string_lossy(&self, raw: &str) -> (Option<String>, LossyDecodeOutcome) {
        let Ok(parsed) = serde_json::from_str::<Value>(raw) else {
            return (None, LossyDecodeOutcome::default());
        };
        let Some(parts) = codec_envelope_parts(&parsed) else {
            return (None, LossyDecodeOutcome::default());
        };
        match self.decode_envelope_lossy(&parts) {
            Ok(plaintext) => {
                let decoded = match plaintext {
                    Value::String(s) => s,
                    other => other.to_string(),
                };
                (
                    Some(decoded),
                    LossyDecodeOutcome {
                        decoded: 1,
                        failed: 0,
                    },
                )
            }
            Err(marker) => (
                Some(marker.to_string()),
                LossyDecodeOutcome {
                    decoded: 0,
                    failed: 1,
                },
            ),
        }
    }
}

/// RAII guard returned by [`PayloadCodecs::pin_key_for_sweep`] (issue #1251).
///
/// Holds one key id pinned against retirement for as long as the guard lives.
/// Dropping it always releases the pin — on the happy path, and on an early
/// return from a batch that hit an error. A failed batch can therefore
/// never leave a key permanently unretirable.
#[must_use = "the key is pinned only while this guard is alive; dropping it immediately un-pins"]
pub struct SweepKeyPin {
    codecs: PayloadCodecs,
    key_id: String,
}

impl Drop for SweepKeyPin {
    fn drop(&mut self) {
        self.codecs.unpin_key_for_sweep(&self.key_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[derive(Debug)]
    struct ReverseCodec;

    impl PayloadCodec for ReverseCodec {
        fn codec_id(&self) -> &'static str {
            "reverse"
        }
        fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, CodecError> {
            let mut v = raw.to_vec();
            v.reverse();
            Ok(v)
        }
        fn decode(&self, encoded: &[u8]) -> Result<Vec<u8>, CodecError> {
            let mut v = encoded.to_vec();
            v.reverse();
            Ok(v)
        }
    }

    // ── advertise_codec_capability (issue #1244) ─────────────────────────

    #[test]
    fn advertise_codec_capability_adds_the_label_to_an_empty_object() {
        let merged = advertise_codec_capability(&json!({}), &[]);
        assert_eq!(
            merged[CODEC_ENVELOPE_CAPABILITY_LABEL],
            CODEC_ENVELOPE_VERSION_KEYED
        );
        assert_eq!(merged[CODEC_REGISTERED_KEY_IDS_LABEL], json!([]));
    }

    #[test]
    fn advertise_codec_capability_preserves_operator_labels() {
        let merged = advertise_codec_capability(
            &json!({"gpu": "true", "region": "eu-west-1"}),
            &["k1".to_string()],
        );
        assert_eq!(merged["gpu"], "true");
        assert_eq!(merged["region"], "eu-west-1");
        assert_eq!(
            merged[CODEC_ENVELOPE_CAPABILITY_LABEL],
            CODEC_ENVELOPE_VERSION_KEYED
        );
        assert_eq!(merged[CODEC_REGISTERED_KEY_IDS_LABEL], json!(["k1"]));
    }

    #[test]
    fn advertise_codec_capability_overwrites_a_forged_lower_version() {
        // The label is engine-written, never operator-configured. A stale or
        // tampered value must never survive the merge. Issue #1244's whole
        // point is that this label can be trusted as this binary's true
        // capability.
        let merged = advertise_codec_capability(&json!({"codec_envelope_version": 1}), &[]);
        assert_eq!(
            merged[CODEC_ENVELOPE_CAPABILITY_LABEL],
            CODEC_ENVELOPE_VERSION_KEYED
        );
    }

    #[test]
    fn advertise_codec_capability_overwrites_forged_registered_keys() {
        // Same trust boundary as the version label above: this process's
        // actual registry always wins over anything already in `labels`.
        let merged = advertise_codec_capability(
            &json!({"codec_registered_key_ids": ["not-really-registered"]}),
            &["k1".to_string(), "k2".to_string()],
        );
        assert_eq!(merged[CODEC_REGISTERED_KEY_IDS_LABEL], json!(["k1", "k2"]));
    }

    #[test]
    fn advertise_codec_capability_replaces_a_non_object_labels_value() {
        let merged = advertise_codec_capability(&json!("not-an-object"), &[]);
        assert!(merged.is_object());
        assert_eq!(
            merged[CODEC_ENVELOPE_CAPABILITY_LABEL],
            CODEC_ENVELOPE_VERSION_KEYED
        );
    }

    #[test]
    fn string_valued_labels_recovers_operator_labels_past_the_merged_engine_keys() {
        // The exact shape `register_worker` / `heartbeat_worker` persist: an
        // operator string label alongside both engine-owned, non-string keys.
        let merged = advertise_codec_capability(
            &json!({"gpu": "true", "region": "eu-west-1"}),
            &["k1".to_string(), "k2".to_string()],
        );
        let labels = string_valued_labels(&merged);
        assert_eq!(
            labels.len(),
            2,
            "engine-owned non-string keys must be dropped, not fail the whole map: {labels:?}"
        );
        assert_eq!(labels.get("gpu").map(String::as_str), Some("true"));
        assert_eq!(labels.get("region").map(String::as_str), Some("eu-west-1"));
        assert!(!labels.contains_key(CODEC_ENVELOPE_CAPABILITY_LABEL));
        assert!(!labels.contains_key(CODEC_REGISTERED_KEY_IDS_LABEL));
    }

    #[test]
    fn string_valued_labels_on_a_non_object_value_is_empty() {
        assert!(string_valued_labels(&json!("not-an-object")).is_empty());
        assert!(string_valued_labels(&json!(null)).is_empty());
    }

    #[test]
    fn encode_then_decode_round_trips_workflow_event_payloads() {
        let mut codecs = PayloadCodecs::default();
        codecs.set_default(Arc::new(ReverseCodec));

        let event = crate::event::WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({"user":"alice"}),
            timestamp: chrono::Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        };

        let encoded = codecs.encode_event(&event).expect("encode");
        assert_eq!(encoded["data"]["input"]["_harvest_codec_envelope"], 1);
        assert_eq!(encoded["data"]["input"]["codec_id"], "reverse");

        let decoded = codecs.decode_event(encoded).expect("decode");
        match decoded {
            crate::event::WorkflowEvent::WorkflowStarted { input, .. } => {
                assert_eq!(input, serde_json::json!({"user":"alice"}));
            }
            _ => panic!("unexpected event"),
        }
    }

    #[test]
    fn a_payload_shaped_like_an_offload_envelope_is_still_encoded() {
        // Issue #1243 review (P1, Codex): the codec boundary must stay
        // unconditional. A workflow's own input can legally contain any
        // JSON shape, including one that happens to carry the offload
        // discriminator key. Nothing may use that shape as a signal to
        // skip encoding -- doing so would store the field in plaintext
        // under a real codec.
        let mut codecs = PayloadCodecs::default();
        codecs.set_default(Arc::new(ReverseCodec));

        let event = crate::event::WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({
                "_harvest_offload_envelope": 1,
                "secret": "still must be encoded",
            }),
            timestamp: chrono::Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        };

        let encoded = codecs.encode_event(&event).expect("encode");
        assert_eq!(
            encoded["data"]["input"]["_harvest_codec_envelope"], 1,
            "an offload-shaped user payload must still be wrapped in a codec envelope: {encoded}"
        );
    }

    #[test]
    fn encode_then_decode_round_trips_side_effect_recorded_value() {
        // issue #384: a custom side_effect closure result lands in
        // SideEffectRecorded.value and must be codec-encoded (encryption /
        // compression) just like the MarkerRecorded.details it replaced.
        let mut codecs = PayloadCodecs::default();
        codecs.set_default(Arc::new(ReverseCodec));

        let event = crate::event::WorkflowEvent::SideEffectRecorded {
            kind: crate::event::SideEffectKind::Custom,
            name: Some("api_credential".to_string()),
            value: serde_json::json!({"token": "super-secret"}),
        };

        let encoded = codecs.encode_event(&event).expect("encode");
        // The value field is wrapped in a codec envelope (not stored raw)…
        assert_eq!(encoded["data"]["value"]["_harvest_codec_envelope"], 1);
        assert_eq!(encoded["data"]["value"]["codec_id"], "reverse");
        // …while the side-effect name (metadata) is left intact.
        assert_eq!(encoded["data"]["name"], "api_credential");

        let decoded = codecs.decode_event(encoded).expect("decode");
        match decoded {
            crate::event::WorkflowEvent::SideEffectRecorded { value, name, .. } => {
                assert_eq!(value, serde_json::json!({"token": "super-secret"}));
                assert_eq!(name.as_deref(), Some("api_credential"));
            }
            _ => panic!("unexpected event"),
        }
    }

    #[test]
    fn identity_codec_preserves_raw_payload_shape() {
        let codecs = PayloadCodecs::default();
        let event = crate::event::WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!({"a": 1}),
        };
        let encoded = codecs.encode_event(&event).expect("encode");
        assert_eq!(encoded["data"]["output"], serde_json::json!({"a": 1}));
    }

    #[test]
    fn legacy_object_with_codec_id_but_not_envelope_is_not_decoded() {
        let codecs = PayloadCodecs::default();
        let event = serde_json::json!({
            "type": "WorkflowCompleted",
            "data": {
                "output": {
                    "codec_id": "business-field",
                    "value": 1
                }
            }
        });

        let decoded = codecs.decode_event(event).expect("decode");
        match decoded {
            crate::event::WorkflowEvent::WorkflowCompleted { output } => {
                assert_eq!(output["codec_id"], "business-field");
                assert_eq!(output["value"], 1);
            }
            _ => panic!("unexpected"),
        }
    }

    #[test]
    fn decode_unknown_codec_id_fails() {
        let codecs = PayloadCodecs::default();
        let bad = serde_json::json!({
            "type": "WorkflowCompleted",
            "data": {
                "output": {
                    "_harvest_codec_envelope": 1,
                    "codec_id": "missing",
                    "data": "e30="
                }
            }
        });

        let err = codecs.decode_event(bad).expect_err("must fail");
        assert!(matches!(err, HarvestError::UnknownPayloadCodec { .. }));
    }

    #[test]
    fn codec_envelope_survives_plain_event_serde_round_trip() {
        // Mechanism behind `store::load_history_undecoded` (issue #608,
        // PR #936 review): payload fields on `WorkflowEvent` are opaque
        // `Value`s, so a stored non-identity envelope deserializes — and
        // re-serializes — verbatim when no codec transform is applied. This
        // is what lets the export handlers load an encrypted history without
        // the strict identity-only decode erroring `UnknownPayloadCodec`.
        let envelope = serde_json::json!({
            "_harvest_codec_envelope": 1,
            "codec_id": "kms-prod",
            "data": "bm90LXJlYWwtY2lwaGVydGV4dA==",
        });
        let stored = serde_json::json!({
            "type": "WorkflowCompleted",
            "data": { "output": envelope },
        });

        let event: crate::event::WorkflowEvent =
            serde_json::from_value(stored.clone()).expect("envelope must deserialize as-is");
        let round_tripped = serde_json::to_value(&event).expect("serialize");
        assert_eq!(
            round_tripped, stored,
            "an undecoded load must preserve the envelope byte-identical"
        );
    }

    // ── Tolerant read-path decode (issue #608) ────────────────────────────────
    //
    // `decode_value_lossy` / `decode_error_string_lossy` are the operator
    // read-path siblings of the strict `decode_event`: infallible, recursive,
    // per-field graceful degrade via the `_harvest_undecodable` marker.
    // (`base64::Engine` is already in scope via `use super::*`.)

    /// A codec whose `decode` always fails — exercises the graceful-degrade
    /// marker path (bad key / corrupt ciphertext at read time).
    #[derive(Debug)]
    struct FailingCodec;

    impl PayloadCodec for FailingCodec {
        fn codec_id(&self) -> &'static str {
            "failing"
        }
        fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, CodecError> {
            Ok(raw.to_vec())
        }
        fn decode(&self, _encoded: &[u8]) -> Result<Vec<u8>, CodecError> {
            Err(CodecError("simulated bad key".to_string()))
        }
    }

    /// Registry with `ReverseCodec` + `FailingCodec` registered (identity is
    /// always present); default stays identity so encoding elsewhere is inert.
    fn lossy_test_codecs() -> PayloadCodecs {
        let mut codecs = PayloadCodecs::default();
        codecs.register(Arc::new(ReverseCodec));
        codecs.register(Arc::new(FailingCodec));
        codecs
    }

    /// Builds a well-formed codec envelope for `plain` under `codec_id`,
    /// using `ReverseCodec`'s byte-reversal for the ciphertext.
    fn reverse_envelope(plain: &Value) -> Value {
        let mut bytes = serde_json::to_vec(plain).expect("serialize plain");
        bytes.reverse();
        serde_json::json!({
            "_harvest_codec_envelope": 1,
            "codec_id": "reverse",
            "data": base64::engine::general_purpose::STANDARD.encode(bytes),
        })
    }

    /// An envelope naming a codec that is not registered anywhere.
    fn unknown_codec_envelope() -> Value {
        serde_json::json!({
            "_harvest_codec_envelope": 1,
            "codec_id": "kms-rotated-away",
            "data": "e30=",
        })
    }

    #[test]
    fn decode_value_lossy_decodes_envelope_in_place() {
        let codecs = lossy_test_codecs();
        let plain = serde_json::json!({"user": "alice", "ssn": "s3cret-pii"});
        let mut value = serde_json::json!({
            "input": reverse_envelope(&plain),
            "meta": "untouched",
        });

        let outcome = codecs.decode_value_lossy(&mut value);

        assert_eq!(value["input"], plain, "envelope must decode in place");
        assert_eq!(value["meta"], "untouched");
        assert_eq!(
            outcome,
            LossyDecodeOutcome {
                decoded: 1,
                failed: 0
            }
        );
        assert!(outcome.touched());
    }

    #[test]
    fn decode_value_lossy_identity_envelope_round_trips() {
        // The identity codec is always registered, so an identity envelope
        // (written by a future/foreign path) must round-trip on read.
        let codecs = PayloadCodecs::default();
        let plain = serde_json::json!({"n": 42});
        let raw = serde_json::to_vec(&plain).unwrap();
        let mut value = serde_json::json!({
            "output": {
                "_harvest_codec_envelope": 1,
                "codec_id": "identity",
                "data": base64::engine::general_purpose::STANDARD.encode(raw),
            }
        });

        let outcome = codecs.decode_value_lossy(&mut value);

        assert_eq!(value["output"], plain);
        assert_eq!(outcome.decoded, 1);
        assert_eq!(outcome.failed, 0);
    }

    #[test]
    fn decode_value_lossy_unknown_codec_id_yields_undecodable_marker_not_error() {
        let codecs = lossy_test_codecs();
        let mut value = serde_json::json!({"input": unknown_codec_envelope()});

        // Infallible by signature: no Result to unwrap.
        let outcome = codecs.decode_value_lossy(&mut value);

        assert_eq!(
            outcome,
            LossyDecodeOutcome {
                decoded: 0,
                failed: 1
            }
        );
        let marker = &value["input"][UNDECODABLE_MARKER_KEY];
        assert_eq!(marker["codec_id"], "kms-rotated-away");
        assert_eq!(marker["reason"], UNDECODABLE_REASON_UNKNOWN_CODEC);
    }

    #[test]
    fn decode_value_lossy_bad_base64_yields_marker() {
        let codecs = lossy_test_codecs();
        let mut value = serde_json::json!({
            "input": {
                "_harvest_codec_envelope": 1,
                "codec_id": "reverse",
                "data": "!!!not-base64!!!",
            }
        });

        let outcome = codecs.decode_value_lossy(&mut value);

        assert_eq!(outcome.failed, 1);
        assert_eq!(outcome.decoded, 0);
        let marker = &value["input"][UNDECODABLE_MARKER_KEY];
        assert_eq!(marker["codec_id"], "reverse");
        assert_eq!(marker["reason"], UNDECODABLE_REASON_INVALID_BASE64);
    }

    #[test]
    fn decode_value_lossy_codec_decode_failure_yields_marker() {
        let codecs = lossy_test_codecs();
        let ciphertext_b64 = base64::engine::general_purpose::STANDARD.encode(b"ciphertext-bytes");
        let mut value = serde_json::json!({
            "input": {
                "_harvest_codec_envelope": 1,
                "codec_id": "failing",
                "data": ciphertext_b64,
            }
        });

        let outcome = codecs.decode_value_lossy(&mut value);

        assert_eq!(outcome.failed, 1);
        let marker = &value["input"][UNDECODABLE_MARKER_KEY];
        assert_eq!(marker["codec_id"], "failing");
        assert_eq!(marker["reason"], UNDECODABLE_REASON_CODEC_ERROR);
        // The marker must never echo the ciphertext (or the codec's own error
        // text, which could carry key material) back to the caller.
        let serialized = serde_json::to_string(&value).unwrap();
        assert!(
            !serialized.contains(&ciphertext_b64),
            "marker must not echo ciphertext: {serialized}"
        );
        assert!(
            !serialized.contains("simulated bad key"),
            "marker must not embed codec error text: {serialized}"
        );
    }

    #[test]
    fn decode_value_lossy_invalid_decoded_json_yields_marker() {
        // ReverseCodec decodes fine, but the plaintext bytes are not JSON.
        let codecs = lossy_test_codecs();
        let mut bytes = b"this is not json".to_vec();
        bytes.reverse();
        let mut value = serde_json::json!({
            "output": {
                "_harvest_codec_envelope": 1,
                "codec_id": "reverse",
                "data": base64::engine::general_purpose::STANDARD.encode(bytes),
            }
        });

        let outcome = codecs.decode_value_lossy(&mut value);

        assert_eq!(outcome.failed, 1);
        let marker = &value["output"][UNDECODABLE_MARKER_KEY];
        assert_eq!(marker["reason"], UNDECODABLE_REASON_INVALID_JSON);
    }

    #[test]
    fn decode_value_lossy_non_envelope_values_pass_through_untouched() {
        let codecs = lossy_test_codecs();
        let mut value = serde_json::json!({
            "string": "plain",
            "number": 42,
            "bool": true,
            "null": null,
            "array": [1, {"nested": "obj"}, [2, 3]],
            "object": {"deep": {"deeper": {"leaf": "x"}}},
        });
        let pristine = value.clone();

        let outcome = codecs.decode_value_lossy(&mut value);

        assert_eq!(value, pristine, "non-envelope JSON must be byte-identical");
        assert_eq!(outcome, LossyDecodeOutcome::default());
        assert!(!outcome.touched());
    }

    #[test]
    fn decode_value_lossy_ignores_offload_envelope() {
        // The claim-check reference envelope (issue #524) has a different
        // discriminator key — it must pass through untouched.
        let codecs = lossy_test_codecs();
        let mut value = serde_json::json!({
            "output": {
                "_harvest_offload_envelope": 1,
                "store_id": "s3-main",
                "key": "blob/abc",
                "len": 2048,
                "checksum": "deadbeef",
            }
        });
        let pristine = value.clone();

        let outcome = codecs.decode_value_lossy(&mut value);

        assert_eq!(value, pristine);
        assert_eq!(outcome, LossyDecodeOutcome::default());
    }

    #[test]
    fn decode_value_lossy_ignores_erasure_tombstone() {
        // The PII-erasure tombstone (issue #495) must pass through untouched.
        let codecs = lossy_test_codecs();
        let mut value = serde_json::json!({"input": {"_harvest_erased": true}});
        let pristine = value.clone();

        let outcome = codecs.decode_value_lossy(&mut value);

        assert_eq!(value, pristine);
        assert_eq!(outcome, LossyDecodeOutcome::default());
    }

    #[test]
    fn decode_value_lossy_ignores_non_envelope_object_with_codec_id_key() {
        // Same semantics as the strict decoder: business data that happens to
        // carry a `codec_id` field is not an envelope.
        let codecs = lossy_test_codecs();
        let mut value = serde_json::json!({
            "output": {"codec_id": "business-field", "value": 1}
        });
        let pristine = value.clone();

        let outcome = codecs.decode_value_lossy(&mut value);

        assert_eq!(value, pristine);
        assert_eq!(outcome, LossyDecodeOutcome::default());
    }

    #[test]
    fn decode_value_lossy_passes_through_malformed_envelope_variants() {
        // Near-envelopes must pass through untouched (same tolerance as the
        // strict `decode_payload`), never decode and never mark.
        let codecs = lossy_test_codecs();
        let malformed = [
            // Wrong version.
            serde_json::json!({
                "_harvest_codec_envelope": 2,
                "codec_id": "reverse",
                "data": "e30=",
            }),
            // Four keys.
            serde_json::json!({
                "_harvest_codec_envelope": 1,
                "codec_id": "reverse",
                "data": "e30=",
                "extra": true,
            }),
            // Non-string data.
            serde_json::json!({
                "_harvest_codec_envelope": 1,
                "codec_id": "reverse",
                "data": 42,
            }),
            // Non-string codec_id.
            serde_json::json!({
                "_harvest_codec_envelope": 1,
                "codec_id": 7,
                "data": "e30=",
            }),
            // Missing data field entirely (only 2 keys).
            serde_json::json!({
                "_harvest_codec_envelope": 1,
                "codec_id": "reverse",
            }),
        ];

        for near_envelope in malformed {
            let mut value = serde_json::json!({"input": near_envelope});
            let pristine = value.clone();
            let outcome = codecs.decode_value_lossy(&mut value);
            assert_eq!(value, pristine, "malformed variant must pass through");
            assert_eq!(outcome, LossyDecodeOutcome::default());
        }
    }

    #[test]
    fn decode_value_lossy_does_not_rescan_decoded_plaintext_for_envelopes() {
        // Single-pass rule: decoded plaintext that is itself envelope-shaped
        // (business data, or a frozen last_completion_result copy) must be
        // preserved verbatim — never decoded a second time, never marked.
        let codecs = lossy_test_codecs();
        let envelope_shaped_plaintext = unknown_codec_envelope();
        let mut value = serde_json::json!({
            "output": reverse_envelope(&envelope_shaped_plaintext),
        });

        let outcome = codecs.decode_value_lossy(&mut value);

        assert_eq!(
            value["output"], envelope_shaped_plaintext,
            "decoded plaintext must be preserved as data, not re-scanned"
        );
        assert_eq!(
            outcome,
            LossyDecodeOutcome {
                decoded: 1,
                failed: 0
            }
        );
    }

    #[test]
    fn decode_value_lossy_recurses_into_arrays_and_nested_objects() {
        let codecs = lossy_test_codecs();
        let plain_a = serde_json::json!("alpha");
        let plain_b = serde_json::json!({"b": 2});
        let plain_c = serde_json::json!([3, 3, 3]);
        let mut value = serde_json::json!({
            "events": [
                {"data": {"input": reverse_envelope(&plain_a)}},
                {"data": {"output": reverse_envelope(&plain_b)}},
            ],
            "nested": {"deep": [reverse_envelope(&plain_c)]},
        });

        let outcome = codecs.decode_value_lossy(&mut value);

        assert_eq!(value["events"][0]["data"]["input"], plain_a);
        assert_eq!(value["events"][1]["data"]["output"], plain_b);
        assert_eq!(value["nested"]["deep"][0], plain_c);
        assert_eq!(outcome.decoded, 3);
        assert_eq!(outcome.failed, 0);
    }

    #[test]
    fn decode_value_lossy_mixed_good_and_bad_envelopes_decodes_good_marks_bad() {
        // The per-field tolerance AC: one bad envelope never poisons its
        // siblings — the good field decodes, the bad one degrades to a marker.
        let codecs = lossy_test_codecs();
        let plain = serde_json::json!({"ok": true});
        let mut value = serde_json::json!({
            "good": reverse_envelope(&plain),
            "bad": unknown_codec_envelope(),
        });

        let outcome = codecs.decode_value_lossy(&mut value);

        assert_eq!(value["good"], plain);
        assert_eq!(
            value["bad"][UNDECODABLE_MARKER_KEY]["reason"],
            UNDECODABLE_REASON_UNKNOWN_CODEC
        );
        assert_eq!(
            outcome,
            LossyDecodeOutcome {
                decoded: 1,
                failed: 1
            }
        );
    }

    #[test]
    fn decode_value_lossy_counts_decoded_and_failed_fields() {
        let codecs = lossy_test_codecs();
        let mut value = serde_json::json!({
            "a": reverse_envelope(&serde_json::json!(1)),
            "b": reverse_envelope(&serde_json::json!(2)),
            "c": reverse_envelope(&serde_json::json!(3)),
            "d": unknown_codec_envelope(),
            "e": unknown_codec_envelope(),
        });

        let outcome = codecs.decode_value_lossy(&mut value);

        assert_eq!(
            outcome,
            LossyDecodeOutcome {
                decoded: 3,
                failed: 2
            }
        );
    }

    #[test]
    fn decode_error_string_lossy_decodes_stringified_envelope() {
        // TEXT error columns can carry a serialized envelope. A decoded
        // string plain payload is returned unwrapped (the original error was
        // a plain string before encoding).
        let codecs = lossy_test_codecs();
        let envelope = reverse_envelope(&serde_json::json!("boom"));
        let raw = serde_json::to_string(&envelope).unwrap();

        let (decoded, outcome) = codecs.decode_error_string_lossy(&raw);

        assert_eq!(decoded.as_deref(), Some("boom"));
        assert_eq!(
            outcome,
            LossyDecodeOutcome {
                decoded: 1,
                failed: 0
            }
        );
    }

    #[test]
    fn decode_error_string_lossy_leaves_plain_error_text_untouched() {
        let codecs = lossy_test_codecs();

        let (decoded, outcome) =
            codecs.decode_error_string_lossy("activity failed: connection refused");

        assert_eq!(decoded, None, "plain error text must be left untouched");
        assert_eq!(outcome, LossyDecodeOutcome::default());
    }

    #[test]
    fn decode_error_string_lossy_leaves_non_envelope_json_untouched() {
        let codecs = lossy_test_codecs();

        let (decoded, outcome) =
            codecs.decode_error_string_lossy(r#"{"code": 500, "message": "downstream"}"#);

        assert_eq!(
            decoded, None,
            "JSON that is not exactly a codec envelope must be left untouched"
        );
        assert_eq!(outcome, LossyDecodeOutcome::default());
    }

    #[test]
    fn decode_error_string_lossy_failure_returns_serialized_marker() {
        // The fourth branch: the raw string IS exactly a serialized envelope,
        // but the codec's decode fails — the response copy gets the marker
        // serialized to a string, counted as `failed: 1`, and neither the
        // ciphertext nor the codec's own error text is echoed.
        let codecs = lossy_test_codecs();
        let ciphertext_b64 = base64::engine::general_purpose::STANDARD.encode(b"opaque-bytes");
        let raw = serde_json::to_string(&serde_json::json!({
            "_harvest_codec_envelope": 1,
            "codec_id": "failing",
            "data": ciphertext_b64,
        }))
        .unwrap();

        let (rewritten, outcome) = codecs.decode_error_string_lossy(&raw);

        let rewritten = rewritten.expect("a failed decode must rewrite to the marker string");
        let marker: Value = serde_json::from_str(&rewritten).expect("marker string is JSON");
        assert_eq!(
            marker,
            undecodable_marker("failing", UNDECODABLE_REASON_CODEC_ERROR)
        );
        assert!(
            !rewritten.contains(&ciphertext_b64) && !rewritten.contains("simulated bad key"),
            "neither ciphertext nor codec error text may be echoed: {rewritten}"
        );
        assert_eq!(
            outcome,
            LossyDecodeOutcome {
                decoded: 0,
                failed: 1
            }
        );
    }

    #[test]
    fn decode_error_string_lossy_serializes_non_string_decoded_json() {
        // The doc-comment contract: a decoded plain string comes back
        // unwrapped, but any other decoded JSON is returned serialized.
        let codecs = lossy_test_codecs();
        let envelope = reverse_envelope(&serde_json::json!({"code": 500}));
        let raw = serde_json::to_string(&envelope).unwrap();

        let (rewritten, outcome) = codecs.decode_error_string_lossy(&raw);

        assert_eq!(
            rewritten.as_deref(),
            Some(r#"{"code":500}"#),
            "non-string decoded JSON must be returned compact-serialized"
        );
        assert_eq!(
            outcome,
            LossyDecodeOutcome {
                decoded: 1,
                failed: 0
            }
        );
    }

    #[test]
    fn decode_value_lossy_transforms_envelope_shaped_business_plaintext() {
        // Known limit (documented in docs/operations/read-path-decode.md):
        // envelopes are purely self-describing, so business data stored as
        // plaintext (identity write path) that is byte-for-byte a codec
        // envelope — at any nesting depth — is indistinguishable from a real
        // envelope and IS transformed by the walk: decoded when its codec_id
        // is registered, replaced with a marker when not.
        let codecs = lossy_test_codecs();
        let mut value = serde_json::json!({
            "batch": [
                {"business": reverse_envelope(&serde_json::json!({"n": 1}))},
                {"business": unknown_codec_envelope()},
            ]
        });

        let outcome = codecs.decode_value_lossy(&mut value);

        assert_eq!(
            value["batch"][0]["business"],
            serde_json::json!({"n": 1}),
            "registered codec_id ⇒ decoded even though it was business data"
        );
        assert_eq!(
            value["batch"][1]["business"],
            undecodable_marker("kms-rotated-away", UNDECODABLE_REASON_UNKNOWN_CODEC),
            "unregistered codec_id ⇒ the business object degrades to a marker"
        );
        assert_eq!(
            outcome,
            LossyDecodeOutcome {
                decoded: 1,
                failed: 1
            }
        );
    }

    #[test]
    fn undecodable_marker_shape_and_reasons_are_stable() {
        // Pins the marker key, the exact marker shape, and the four bounded
        // reason strings — the operator-facing degrade contract (issue #608).
        assert_eq!(UNDECODABLE_MARKER_KEY, "_harvest_undecodable");
        assert_eq!(UNDECODABLE_REASON_UNKNOWN_CODEC, "unknown_codec");
        assert_eq!(UNDECODABLE_REASON_INVALID_BASE64, "invalid_base64");
        assert_eq!(UNDECODABLE_REASON_CODEC_ERROR, "codec_error");
        assert_eq!(UNDECODABLE_REASON_INVALID_JSON, "invalid_json");

        let marker = undecodable_marker("kms-v1", UNDECODABLE_REASON_CODEC_ERROR);
        assert_eq!(
            marker,
            serde_json::json!({
                UNDECODABLE_MARKER_KEY: {
                    "codec_id": "kms-v1",
                    "reason": "codec_error",
                }
            })
        );
    }

    #[test]
    fn lossy_decode_outcome_merged_and_touched_semantics() {
        let zero = LossyDecodeOutcome::default();
        assert!(!zero.touched());

        let decoded_only = LossyDecodeOutcome {
            decoded: 2,
            failed: 0,
        };
        assert!(decoded_only.touched());

        let failed_only = LossyDecodeOutcome {
            decoded: 0,
            failed: 1,
        };
        assert!(failed_only.touched(), "a marked field is still a touch");

        let merged = decoded_only.merged(failed_only);
        assert_eq!(
            merged,
            LossyDecodeOutcome {
                decoded: 2,
                failed: 1
            }
        );
        assert_eq!(zero.merged(zero), zero);
    }

    // ── issue #948: keyed codecs / key rotation ───────────────────────────

    /// A second "encryption key" for the same logical codec: rotation means
    /// two codecs with the SAME `codec_id` and different key material, which
    /// is precisely why a key id cannot be folded into `codec_id`.
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

    fn started(input: Value) -> crate::event::WorkflowEvent {
        crate::event::WorkflowEvent::WorkflowStarted {
            input,
            timestamp: chrono::Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }
    }

    #[test]
    fn envelope_carries_kid_once_a_non_legacy_key_is_active() {
        // AC1: the envelope carries a key id alongside the discriminator.
        let codecs = PayloadCodecs::default();
        codecs
            .register_key("k2", Arc::new(XorCodec(0x5a)))
            .expect("register");

        let encoded = codecs
            .encode_event(&started(json!({"user": "alice"})))
            .expect("encode");
        let env = &encoded["data"]["input"];
        assert_eq!(
            env[CODEC_ENVELOPE_KEY], CODEC_ENVELOPE_VERSION_KEYED,
            "a keyed envelope declares its own version, so it can never be confused with a \
             four-key version-1 value that a prior release would have stored as plaintext"
        );
        assert_eq!(env["codec_id"], "xor");
        assert_eq!(env[CODEC_ENVELOPE_KID_KEY], "k2");
    }

    #[test]
    fn envelope_omits_kid_while_the_legacy_key_is_active() {
        // AC1 (back-compat half): an un-rotated deployment's stored bytes are
        // byte-identical to pre-#948 — the three-key envelope is the canonical
        // spelling of "kid == CODEC_LEGACY_KEY_ID".
        let mut codecs = PayloadCodecs::default();
        codecs.set_default(Arc::new(ReverseCodec));

        let encoded = codecs
            .encode_event(&started(json!({"user": "alice"})))
            .expect("encode");
        let env = encoded["data"]["input"].as_object().expect("object");
        assert_eq!(env.len(), 3, "no `kid` key is written: {env:?}");
        assert!(!env.contains_key(CODEC_ENVELOPE_KID_KEY));
        assert_eq!(
            env[CODEC_ENVELOPE_KEY], CODEC_ENVELOPE_VERSION_LEGACY,
            "and the discriminator is unchanged, so the bytes match pre-#948 exactly"
        );
    }

    #[test]
    fn kidless_envelope_resolves_to_the_legacy_key_id() {
        // AC1: pre-upgrade rows (no `kid`) decode unchanged, resolving to the
        // designated legacy key id.
        let writer = PayloadCodecs::default();
        writer
            .register_key(CODEC_LEGACY_KEY_ID, Arc::new(XorCodec(0x11)))
            .expect("register");
        let encoded = writer
            .encode_event(&started(json!({"n": 1})))
            .expect("encode");
        assert!(
            encoded["data"]["input"]
                .get(CODEC_ENVELOPE_KID_KEY)
                .is_none(),
            "legacy-active writes stay kid-less"
        );

        // A reader that only knows the legacy key still decodes it.
        let reader = PayloadCodecs::default();
        reader
            .register_key(CODEC_LEGACY_KEY_ID, Arc::new(XorCodec(0x11)))
            .expect("register");
        let decoded = reader.decode_event(encoded).expect("decode");
        match decoded {
            crate::event::WorkflowEvent::WorkflowStarted { input, .. } => {
                assert_eq!(input, json!({"n": 1}));
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn decode_resolves_any_registered_key_by_id() {
        // AC3: a mixed-key history replays transparently.
        let codecs = PayloadCodecs::default();
        codecs
            .register_key("k1", Arc::new(XorCodec(0x01)))
            .expect("register k1");
        let under_k1 = codecs
            .encode_event(&started(json!({"v": "one"})))
            .expect("encode k1");

        codecs
            .register_key("k2", Arc::new(XorCodec(0x02)))
            .expect("register k2");
        codecs.set_active_key("k2").expect("activate k2");
        let under_k2 = codecs
            .encode_event(&started(json!({"v": "two"})))
            .expect("encode k2");

        assert_eq!(under_k1["data"]["input"][CODEC_ENVELOPE_KID_KEY], "k1");
        assert_eq!(under_k2["data"]["input"][CODEC_ENVELOPE_KID_KEY], "k2");

        for (encoded, expected) in [(under_k1, "one"), (under_k2, "two")] {
            match codecs.decode_event(encoded).expect("decode") {
                crate::event::WorkflowEvent::WorkflowStarted { input, .. } => {
                    assert_eq!(input, json!({ "v": expected }));
                }
                other => panic!("unexpected event: {other:?}"),
            }
        }
    }

    #[test]
    fn active_key_flip_is_observed_by_registry_clones() {
        // AC2: no restart-ordering window. A clone taken BEFORE the flip (the
        // shape every worker/store call site holds) must encrypt under the new
        // key immediately after the flip is acknowledged.
        let codecs = PayloadCodecs::default();
        codecs
            .register_key("k1", Arc::new(XorCodec(0x01)))
            .expect("register k1");
        codecs
            .register_key("k2", Arc::new(XorCodec(0x02)))
            .expect("register k2");

        let captured_at_boot = codecs.clone();
        assert_eq!(captured_at_boot.active_key_id(), "k1");

        codecs.set_active_key("k2").expect("activate k2");

        assert_eq!(captured_at_boot.active_key_id(), "k2");
        let encoded = captured_at_boot
            .encode_event(&started(json!({"after": "flip"})))
            .expect("encode");
        assert_eq!(encoded["data"]["input"][CODEC_ENVELOPE_KID_KEY], "k2");
    }

    #[test]
    fn exactly_one_key_is_active_and_activation_requires_registration() {
        let codecs = PayloadCodecs::default();
        assert_eq!(
            codecs.active_key_id(),
            CODEC_LEGACY_KEY_ID,
            "an unconfigured registry is on the legacy key"
        );
        assert!(codecs.registered_key_ids().is_empty());

        codecs
            .register_key("k1", Arc::new(XorCodec(1)))
            .expect("register k1");
        codecs
            .register_key("k2", Arc::new(XorCodec(2)))
            .expect("register k2");
        assert_eq!(
            codecs.registered_key_ids(),
            vec!["k1".to_string(), "k2".to_string()]
        );
        assert_eq!(
            codecs.active_key_id(),
            "k1",
            "first registered key is active"
        );

        let err = codecs
            .set_active_key("nope")
            .expect_err("unregistered key must be refused");
        assert!(err.to_string().contains("nope"), "{err}");
        assert_eq!(
            codecs.active_key_id(),
            "k1",
            "a refused flip must not change the active key"
        );
    }

    #[test]
    fn register_key_rejects_malformed_key_ids() {
        let codecs = PayloadCodecs::default();
        for bad in [
            "",
            " ",
            "has space",
            "emoji-🔑",
            &"x".repeat(MAX_CODEC_KEY_ID_BYTES + 1),
        ] {
            assert!(
                codecs.register_key(bad, Arc::new(XorCodec(1))).is_err(),
                "key id {bad:?} must be refused"
            );
        }
        codecs
            .register_key("2026-q3.rotation:a_b", Arc::new(XorCodec(1)))
            .expect("sane id");
    }

    #[test]
    fn strict_decode_of_an_unknown_kid_is_a_typed_error() {
        let writer = PayloadCodecs::default();
        writer
            .register_key("k9", Arc::new(XorCodec(9)))
            .expect("register");
        let encoded = writer
            .encode_event(&started(json!({"a": 1})))
            .expect("encode");

        let reader = PayloadCodecs::default();
        reader
            .register_key("k1", Arc::new(XorCodec(1)))
            .expect("register");
        let err = reader
            .decode_event(encoded)
            .expect_err("unknown kid must fail closed");
        match err {
            HarvestError::UnknownCodecKey { key_id, codec_id } => {
                assert_eq!(key_id, "k9");
                assert_eq!(codec_id, "xor");
            }
            other => panic!("expected UnknownCodecKey, got {other:?}"),
        }
    }

    #[test]
    fn lossy_decode_of_an_unknown_kid_is_a_bounded_marker() {
        let writer = PayloadCodecs::default();
        writer
            .register_key("k9", Arc::new(XorCodec(9)))
            .expect("register");
        let mut encoded = writer
            .encode_event(&started(json!({"a": 1})))
            .expect("encode");

        let reader = PayloadCodecs::default();
        let outcome = reader.decode_value_lossy(&mut encoded);
        assert_eq!(outcome.failed, 1);
        assert_eq!(
            encoded["data"]["input"][UNDECODABLE_MARKER_KEY]["reason"],
            UNDECODABLE_REASON_UNKNOWN_KEY
        );
    }

    #[test]
    fn a_four_key_version_1_payload_is_still_plaintext() {
        // The regression this envelope versioning exists to prevent. Pre-#948
        // the parser required EXACTLY three keys, so on an identity deployment
        // business data of this exact shape was stored — and read back —
        // verbatim. Widening version 1 to accept a fourth `kid` key would have
        // turned that plaintext into a decode target: a strict read would fail
        // with `UnknownCodecKey` where it used to pass through, and with a
        // matching key registered the sweep would decode and rewrite data that
        // was never ciphertext.
        let business_data = json!({
            CODEC_ENVELOPE_KEY: CODEC_ENVELOPE_VERSION_LEGACY,
            "codec_id": "xor",
            "data": "AAAA",
            CODEC_ENVELOPE_KID_KEY: "k1",
        });
        assert!(!is_codec_envelope(&business_data));

        // And it survives a real decode untouched, on a registry that HAS `k1`.
        let codecs = PayloadCodecs::default();
        codecs
            .register_key("k1", Arc::new(XorCodec(0x11)))
            .expect("register k1");
        assert_eq!(
            codecs
                .decode_payload(&business_data)
                .expect("must not error"),
            business_data,
            "previously-valid four-key plaintext must pass through verbatim"
        );

        // The mirror case: version 2 with only three keys is not an envelope
        // either.
        assert!(!is_codec_envelope(&json!({
            CODEC_ENVELOPE_KEY: CODEC_ENVELOPE_VERSION_KEYED,
            "codec_id": "xor",
            "data": "AAAA",
        })));
        // Nor is an unknown version.
        assert!(!is_codec_envelope(&json!({
            CODEC_ENVELOPE_KEY: 3,
            "codec_id": "xor",
            "data": "AAAA",
        })));
    }

    #[test]
    fn a_malformed_kid_in_stored_bytes_is_not_an_envelope() {
        // A `kid` read back out of storage is untrusted: on an identity
        // deployment a caller's workflow input is stored verbatim, so crafted
        // input must not be able to inject an unbounded key id into the
        // rotation census, the admin response, or the sweep's logs.
        for bad in ["has space", &"A".repeat(MAX_CODEC_KEY_ID_BYTES + 1), ""] {
            assert!(
                !is_codec_envelope(&json!({
                    CODEC_ENVELOPE_KEY: CODEC_ENVELOPE_VERSION_KEYED,
                    "codec_id": "xor",
                    "data": "AAAA",
                    CODEC_ENVELOPE_KID_KEY: bad,
                })),
                "kid {bad:?} must not be accepted from storage"
            );
        }
    }

    #[test]
    fn a_kidless_envelope_prefers_the_codec_it_actually_names() {
        // Kid-less history can span several codec ids -- an embedder who
        // migrated "reverse" to "xor" before rotation existed has both in the
        // codec_id map. Registering the CURRENT pre-rotation codec under
        // `legacy` must not make it win for envelopes naming the other one:
        // that decodes with the wrong algorithm and key, and an unauthenticated
        // codec returns plausible-but-wrong bytes rather than failing -- which
        // the sweep would then re-encrypt under the active key, destroying the
        // payload permanently.
        let mut writer = PayloadCodecs::default();
        writer.set_default(Arc::new(ReverseCodec));
        let old_history = writer
            .encode_event(&started(json!({"era": "reverse"})))
            .expect("encode under the older codec");
        assert_eq!(old_history["data"]["input"]["codec_id"], "reverse");

        // The reader: "reverse" still registered by codec_id, and the CURRENT
        // pre-rotation codec ("xor") registered under the legacy key id.
        let mut codecs = PayloadCodecs::default();
        codecs.register(Arc::new(ReverseCodec));
        codecs
            .register_key(CODEC_LEGACY_KEY_ID, Arc::new(XorCodec(0x11)))
            .expect("register legacy");

        match codecs.decode_event(old_history).expect("decode") {
            crate::event::WorkflowEvent::WorkflowStarted { input, .. } => {
                assert_eq!(
                    input,
                    json!({"era": "reverse"}),
                    "a kid-less envelope must be decoded by the codec it NAMES, not by \
                     whatever happens to sit under the legacy key id"
                );
            }
            other => panic!("unexpected event: {other:?}"),
        }

        // And the legacy entry still wins when it IS the named codec.
        let legacy_history = {
            let w = PayloadCodecs::default();
            w.register_key(CODEC_LEGACY_KEY_ID, Arc::new(XorCodec(0x11)))
                .expect("register legacy");
            w.encode_event(&started(json!({"era": "xor"})))
                .expect("encode")
        };
        assert_eq!(legacy_history["data"]["input"]["codec_id"], "xor");
        match codecs.decode_event(legacy_history).expect("decode") {
            crate::event::WorkflowEvent::WorkflowStarted { input, .. } => {
                assert_eq!(input, json!({"era": "xor"}));
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn key_registration_is_immutable() {
        // Silently replacing a registered key id would destroy the only decoder
        // for every stored envelope bearing that `kid`, and the sweep could not
        // repair them — it sees the key id as already active and skips.
        let codecs = PayloadCodecs::default();
        codecs
            .register_key("k1", Arc::new(XorCodec(0x11)))
            .expect("first registration");

        let err = codecs
            .register_key("k1", Arc::new(XorCodec(0x99)))
            .expect_err("re-registering an existing key id must be refused");
        assert!(err.to_string().contains("already registered"), "{err}");

        // The original codec is still the one installed.
        let encoded = codecs
            .encode_event(&started(json!({"a": 1})))
            .expect("encode");
        match codecs.decode_event(encoded).expect("decode") {
            crate::event::WorkflowEvent::WorkflowStarted { input, .. } => {
                assert_eq!(input, json!({"a": 1}));
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn a_four_key_object_without_a_kid_is_not_an_envelope() {
        // The pre-#948 strictness against near-envelopes must survive widening
        // the shape check from "exactly 3 keys" to "3 keys, or 4 with a kid".
        let not_an_envelope = json!({
            CODEC_ENVELOPE_KEY: CODEC_ENVELOPE_VERSION_KEYED,
            "codec_id": "xor",
            "data": "AAAA",
            "something_else": true,
        });
        assert!(!is_codec_envelope(&not_an_envelope));

        let numeric_kid = json!({
            CODEC_ENVELOPE_KEY: CODEC_ENVELOPE_VERSION_KEYED,
            "codec_id": "xor",
            "data": "AAAA",
            CODEC_ENVELOPE_KID_KEY: 7,
        });
        assert!(
            !is_codec_envelope(&numeric_kid),
            "a non-string kid is not an envelope"
        );
    }

    #[test]
    fn codec_id_registration_remains_the_kidless_fallback() {
        // Back-compat: a deployment using today's `register()` + `set_default()`
        // API (no key ids at all) keeps decoding its own history verbatim.
        let mut codecs = PayloadCodecs::default();
        codecs.set_default(Arc::new(ReverseCodec));
        let encoded = codecs
            .encode_event(&started(json!({"legacy": true})))
            .expect("encode");

        // Now rotate onto a keyed codec; the old kid-less rows must still decode
        // through the codec_id map.
        codecs
            .register_key("k2", Arc::new(XorCodec(2)))
            .expect("register");
        codecs.set_active_key("k2").expect("activate");
        match codecs.decode_event(encoded).expect("decode") {
            crate::event::WorkflowEvent::WorkflowStarted { input, .. } => {
                assert_eq!(input, json!({"legacy": true}));
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn a_keyed_envelope_naming_the_wrong_codec_is_refused() {
        // Codex round 8 (P2). The round-2 fix required the codec_id to match
        // for KID-LESS envelopes and left the keyed branch short-circuiting on
        // `parts.kid.is_some()`, so an explicit kid that resolved was decoded
        // without ever checking the algorithm the envelope named.
        //
        // That is the hazard `resolve_decoder`'s own doc describes: an
        // unauthenticated codec returns plausible-but-wrong bytes rather than
        // failing, and the sweep then re-encrypts that garbage under the active
        // key -- silently and permanently destroying the payload. `XorCodec`
        // is exactly such a codec: it never fails, it just returns different
        // bytes, which is why it can stand in for the real thing here.
        let codecs = PayloadCodecs::default();
        codecs
            .register_key("k1", Arc::new(XorCodec(1)))
            .expect("register k1");
        codecs.set_active_key("k1").expect("activate k1");

        let encoded = codecs
            .encode_event(&started(json!({"secret": "value"})))
            .expect("encode");
        let mut tampered = encoded;
        let field = tampered
            .get_mut("data")
            .and_then(|d| d.get_mut("input"))
            .expect("input envelope");
        // Same registered kid, a codec_id the key is not registered with.
        field["codec_id"] = json!("not-the-registered-codec");

        assert!(
            codecs.decode_event(tampered.clone()).is_err(),
            "a keyed envelope naming a different codec must fail, not decode \
             under the key's own algorithm"
        );

        // And the lossy path degrades to a bounded marker rather than handing
        // workflow code the wrong plaintext.
        let mut lossy = tampered;
        let outcome = codecs.decode_value_lossy(&mut lossy);
        assert_eq!(outcome.failed, 1, "the mismatch is counted as undecodable");
        assert_eq!(outcome.decoded, 0);
    }

    #[test]
    fn retire_key_local_refuses_the_active_key() {
        let codecs = PayloadCodecs::default();
        codecs
            .register_key("k1", Arc::new(XorCodec(1)))
            .expect("register k1");
        codecs
            .register_key("k2", Arc::new(XorCodec(2)))
            .expect("register k2");
        codecs.set_active_key("k2").expect("activate k2");

        assert!(
            codecs.retire_key_local("k2").is_err(),
            "the active key is never retirable"
        );
        codecs
            .retire_key_local("k1")
            .expect("retire the inactive key");
        assert_eq!(codecs.registered_key_ids(), vec!["k2".to_string()]);
        assert!(codecs.codec_for_key("k1").is_none());
    }

    /// Issue #1251: a sweep batch pins the key it is writing onto.
    /// Retirement must refuse that key while the pin holds. That holds even
    /// though a zero census -- no rows committed under it yet -- would
    /// otherwise say it is safe.
    #[test]
    fn retire_key_local_refuses_a_key_pinned_by_a_sweep_batch() {
        let codecs = PayloadCodecs::default();
        codecs
            .register_key("k1", Arc::new(XorCodec(1)))
            .expect("register k1");
        codecs
            .register_key("k2", Arc::new(XorCodec(2)))
            .expect("register k2");
        codecs.set_active_key("k2").expect("activate k2");

        let pin = codecs.pin_key_for_sweep("k1");
        assert!(codecs.is_pinned_by_sweep("k1"));

        let err = codecs
            .retire_key_local("k1")
            .expect_err("a pinned key must not be retirable");
        assert!(matches!(err, HarvestError::Config(_)), "{err:?}");
        assert_eq!(
            codecs.registered_key_ids(),
            vec!["k1".to_string(), "k2".to_string()],
            "the refused retirement must not have removed the key"
        );

        drop(pin);
        assert!(!codecs.is_pinned_by_sweep("k1"));
        codecs
            .retire_key_local("k1")
            .expect("retirable once the pin is released");
    }

    /// Several batches -- one per shard -- can pin the same key id at once.
    /// The key must stay protected until every pin on it releases, not just
    /// the first.
    #[test]
    fn a_key_pinned_by_two_batches_stays_protected_until_both_release() {
        let codecs = PayloadCodecs::default();
        codecs
            .register_key("k1", Arc::new(XorCodec(1)))
            .expect("register k1");
        codecs
            .register_key("k2", Arc::new(XorCodec(2)))
            .expect("register k2");
        codecs.set_active_key("k2").expect("activate k2");

        let pin_a = codecs.pin_key_for_sweep("k1");
        let pin_b = codecs.pin_key_for_sweep("k1");

        drop(pin_a);
        assert!(
            codecs.is_pinned_by_sweep("k1"),
            "one release must not clear a second, still-live pin"
        );
        assert!(codecs.retire_key_local("k1").is_err());

        drop(pin_b);
        assert!(!codecs.is_pinned_by_sweep("k1"));
        codecs
            .retire_key_local("k1")
            .expect("retirable once both pins are released");
    }

    /// The `any_keys` mirror must be published **under** the map lock.
    ///
    /// It is a lock-free fast path for `has_keyed_codecs`, so a stale `false`
    /// makes `resolve_decoder` skip the keyed lookup entirely and encoding fall
    /// back to the default codec — a non-empty keyed registry behaving as if it
    /// were empty. Storing it after `drop(guard)` leaves the store unordered
    /// with respect to the map mutation, so two writers can interleave:
    /// a retirement removes the last key and computes `false`, a registration
    /// then inserts and stores `true`, and the retirement's delayed store
    /// overwrites it with `false`.
    ///
    /// Source-level because the hazard is *statement order* across two
    /// functions, which no single-threaded assertion can observe: the race
    /// window is a few instructions wide and would make a timing test flaky in
    /// both directions.
    #[test]
    fn the_key_presence_mirror_is_published_under_the_lock() {
        let src = include_str!("payload_codec.rs");
        for func in ["pub fn register_key(", "pub fn retire_key_local("] {
            let start = src
                .find(func)
                .expect("both key-mutating functions must exist");
            let body = &src[start..];
            let end = body[1..].find("\n    /// ").map_or(body.len(), |o| o + 1);
            let body = &body[..end];

            // Match the STATEMENTS, not the prose: the comments below each of
            // these lines discuss `drop(guard)` by name, and a bare substring
            // search finds the explanation before the code it explains.
            let store = body
                .find("self.any_keys.store(")
                .expect("each key-mutating function must publish the presence mirror");
            let unlock = body
                .find("drop(guard);")
                .expect("each key-mutating function must release the write guard");
            assert!(
                store < unlock,
                "{func}: the any_keys store must happen BEFORE drop(guard), or it is \
                 unordered against the map mutation and a concurrent register/retire \
                 pair can leave the mirror disagreeing with a non-empty map"
            );
        }
    }
}
