//! Shard routing and per-shard database pools.
//!
//! Autumn-Harvest can spread workflow state across several independent
//! Postgres databases. Each workflow execution lives entirely on a single
//! shard — the event log, task queue rows, timers, signals, and dead-letter
//! entries for an execution all join back to the same database — so per-
//! workflow ACID guarantees are preserved without cross-shard transactions.
//!
//! This module provides the two primitives used to wire that design into the
//! runtime:
//!
//! * [`ShardRouter`] picks a [`ShardId`] for a *new* workflow. For existing
//!   workflows the shard is already encoded in the [`ExecutionId`] UUID and
//!   no routing decision is required.
//! * [`ShardedDbPool`] owns one [`crate::worker::DbPool`] per shard and
//!   resolves a pool from either a `ShardId` or an `ExecutionId`. Single-DB
//!   deployments use [`ShardedDbPool::single`], which places the only pool at
//!   `ShardId(0)` and behaves identically to the pre-sharding code path.
//!
//! Routing is deliberately directory-less. Because every `ExecutionId` writes
//! its shard into the UUID's first two bytes, any holder of an
//! `ExecutionId` can resolve to the correct pool in O(1). Lookups for ids that
//! were minted before sharding (or in tests) produce
//! [`ShardId::UNENCODED`]; the pool falls back to a configured default shard
//! for those cases.

use std::collections::BTreeMap;
use std::hash::Hasher;

#[cfg(feature = "db")]
use crate::types::ExternalTarget;
use crate::types::{ExecutionId, ShardId};
#[cfg(feature = "db")]
use crate::worker::DbPool;

/// Where a brand-new workflow should be placed (issue #697).
///
/// The default, [`ShardPlacement::Auto`], is today's rendezvous routing and is
/// byte-for-byte unchanged. The other two variants are *explicit placement
/// policy* for deployments with a data-residency or tenant-isolation
/// obligation, where "whichever shard the hash picked" is not an acceptable
/// answer.
///
/// Placement is decided **before** the execution id is minted, so the chosen
/// shard is baked into the `ExecutionId` bytes and every later lookup — plus
/// every child workflow, continue-as-new successor, retry, and reset fork,
/// which all inherit `exec_id.shard()` — stays on the pinned shard. No event
/// is written and no migration is involved.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest::shard::{ShardPlacement, ShardRouter};
/// use autumn_harvest::types::ShardId;
///
/// let router = ShardRouter::new(
///     vec![ShardId::new(0), ShardId::new(1)],
///     vec![ShardId::new(0), ShardId::new(1)],
///     ShardId::new(0),
/// )
/// .with_residency_map([("eu".to_string(), ShardId::new(1))]);
///
/// // Unpinned: today's hash.
/// let auto = router.resolve_placement(&ShardPlacement::Auto, "wf", "order-42")?;
/// assert_eq!(auto, router.pick_for_new_workflow("wf", "order-42"));
///
/// // Pinned by residency key: always the EU database.
/// let eu = router.resolve_placement(&ShardPlacement::residency_key("eu"), "wf", "order-42")?;
/// assert_eq!(eu, ShardId::new(1));
/// # Ok::<(), autumn_harvest::shard::ShardPlacementError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum ShardPlacement {
    /// Route by rendezvous hash over `(workflow_name, workflow_id)`.
    ///
    /// This is the pre-#697 behaviour and the default for every caller that
    /// does not opt in.
    #[default]
    Auto,
    /// Pin to a concrete shard.
    ///
    /// Rejected unless the shard is in the router's `readable_shards` *and*
    /// `writable_shards` sets.
    Shard(ShardId),
    /// Pin via a stable, operator-declared residency key (e.g. `"eu"`).
    ///
    /// Resolved through [`ShardRouter::with_residency_map`]. An unmapped key is
    /// an error, never a hash fallback.
    ResidencyKey(String),
}

impl ShardPlacement {
    /// Convenience constructor for [`ShardPlacement::ResidencyKey`].
    #[must_use]
    pub fn residency_key(key: impl Into<String>) -> Self {
        Self::ResidencyKey(key.into())
    }

    /// Is this the default (unpinned) placement?
    #[must_use]
    pub const fn is_auto(&self) -> bool {
        matches!(self, Self::Auto)
    }
}

/// Why an explicit [`ShardPlacement`] could not be honoured.
///
/// Every variant is a caller error that must surface as a `400`-class failure.
/// Placement never falls back to the default shard: a silent fallback is
/// exactly the failure mode explicit pinning exists to remove.
///
/// # Disclosure
///
/// [`Display`](std::fmt::Display) renders the **operator** view and enumerates
/// the valid shard set / declared residency keys, which is what you want in a
/// log line or a Rust-API caller's error. Do **not** return it verbatim to an
/// untrusted HTTP caller — residency keys are frequently tenant- or
/// region-identifying, so echoing the declared set lets one probe enumerate
/// your shard topology, live drain state, and other tenants' keys. Use
/// [`ShardPlacementError::client_message`] on any caller-facing boundary.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ShardPlacementError {
    /// The requested shard is not in the deployment's `readable_shards` set.
    #[error(
        "shard {requested} is not a configured shard; readable shards are {}",
        format_shards(readable)
    )]
    #[non_exhaustive]
    UnknownShard {
        /// The shard the caller asked for.
        requested: ShardId,
        /// The shards this deployment can read from.
        readable: Vec<ShardId>,
    },
    /// The requested shard exists but has been drained out of
    /// `writable_shards`, so it is deliberately not accepting new workflows.
    #[error(
        "shard {requested} is readable but not writable (drained); writable shards are {}",
        format_shards(writable)
    )]
    #[non_exhaustive]
    ShardNotWritable {
        /// The shard the caller asked for.
        requested: ShardId,
        /// The shards that currently accept new workflows.
        writable: Vec<ShardId>,
    },
    /// The residency key has no entry in the router's residency map.
    #[error(
        "residency key '{key}' is not mapped to a shard; declared keys are [{}]",
        known.join(", ")
    )]
    #[non_exhaustive]
    UnknownResidencyKey {
        /// The key the caller asked for.
        key: String,
        /// The residency keys this deployment declares.
        known: Vec<String>,
    },
    /// The residency key was empty or whitespace-only.
    #[error("residency key must not be blank")]
    EmptyResidencyKey,
}

impl ShardPlacementError {
    /// The **caller-facing** rendering, safe to return over HTTP.
    ///
    /// Names the value the caller got wrong and what to do about it, but never
    /// enumerates the deployment's shard set, live drain state, or the declared
    /// residency keys — see the type-level "Disclosure" note. A caller does not
    /// need the valid set to correct their own request; an operator reads the
    /// full [`Display`](std::fmt::Display) form from the log and the audit row.
    #[must_use]
    pub fn client_message(&self) -> String {
        match self {
            Self::UnknownShard { requested, .. } => {
                format!("shard {requested} is not a placeable shard for this deployment")
            }
            Self::ShardNotWritable { requested, .. } => {
                format!(
                    "shard {requested} is not currently accepting new workflows; \
                     it is being drained"
                )
            }
            Self::UnknownResidencyKey { key, .. } => {
                format!("residency key '{key}' is not declared for this deployment")
            }
            Self::EmptyResidencyKey => "residency key must not be blank".to_string(),
        }
    }
}

/// Largest shard number an [`ExecutionId`] can carry.
///
/// `ExecutionId::new_for_shard` encodes the shard into the UUID's first two
/// bytes as `shard & 0xFFFF`, and `0xFFFF` is reserved for
/// [`ShardId::UNENCODED`], so `0xFFFE` is the highest usable value.
pub const MAX_ENCODABLE_SHARD: i32 = 0xFFFE;

/// Can this shard number survive a round trip through an [`ExecutionId`]?
///
/// [`ShardId::new`] is an unbounded `const fn`, so an out-of-range value is
/// constructible even though its own docs call `0..=0xFFFE` the valid range.
/// Anything outside that range is silently truncated by the encoder, which would
/// route later id-based lookups to a different shard than the one the row was
/// written to — so placement rejects it up front.
#[must_use]
pub const fn is_encodable_shard(shard: ShardId) -> bool {
    let raw = shard.as_i32();
    raw >= 0 && raw <= MAX_ENCODABLE_SHARD
}

fn format_shards(shards: &[ShardId]) -> String {
    let rendered: Vec<String> = shards.iter().map(ToString::to_string).collect();
    format!("[{}]", rendered.join(", "))
}

/// Decides which shard a newly started workflow should live on.
///
/// The router carries two lists:
///
/// * `readable_shards`: the superset of shards the deployment can load from.
///   Rendezvous-hashing is performed over this set so the hash is stable
///   across deployments where the writable set is being widened or narrowed
///   (e.g. adding a new shard for new workflows only).
/// * `writable_shards`: the subset that accepts *new* workflows. When the
///   initial rendezvous pick lands outside this subset the router re-hashes
///   among the writable subset.
///
/// Hashes use `seahash` rather than `std::hash` because `std::hash::BuildHasher`
/// is seeded randomly per-process and would produce different placements on
/// every boot, breaking idempotent outbox retries.
///
/// See [`ShardRouter::parts`] for the exhaustive-destructuring accessor the
/// operator-facing config snapshot uses.
#[derive(Debug, Clone)]
pub struct ShardRouter {
    readable_shards: Vec<ShardId>,
    writable_shards: Vec<ShardId>,
    default_shard: ShardId,
    /// Operator-declared residency key → shard mapping (issue #697).
    ///
    /// Empty by default. Populated via [`ShardRouter::with_residency_map`].
    residency_map: BTreeMap<String, ShardId>,
    /// Operator-declared retired shard → successor mapping (issue #964).
    ///
    /// Empty by default. Populated via [`ShardRouter::with_shard_forwards`]
    /// once a shard's residents have all been rebalanced off it and the shard
    /// itself has been removed from `readable_shards`. Without it, an
    /// `ExecutionId` minted on the retired shard would fall back to the default
    /// shard and silently resolve to the wrong run — the one failure mode a
    /// decommission must never produce.
    shard_forwards: BTreeMap<ShardId, ShardId>,
}

/// Every placement-affecting field of a [`ShardRouter`], borrowed together.
///
/// Returned by [`ShardRouter::parts`] so a projection that must not silently
/// miss a field can destructure it exhaustively (no `..`) and get a compile
/// error when a new placement input is added.
#[derive(Debug, Clone)]
pub struct ShardRouterParts<'a> {
    /// The superset of shards the deployment can load from.
    pub readable_shards: &'a [ShardId],
    /// The subset that accepts new workflows.
    pub writable_shards: &'a [ShardId],
    /// The shard unencoded execution ids resolve to.
    pub default_shard: ShardId,
    /// The declared residency key → shard mapping (issue #697).
    pub residency_map: &'a BTreeMap<String, ShardId>,
    /// The declared retired-shard → successor mapping (issue #964).
    pub shard_forwards: &'a BTreeMap<ShardId, ShardId>,
}

impl ShardRouter {
    /// Build a router from an explicit list of readable and writable shards.
    ///
    /// `default_shard` is returned when a lookup is asked to resolve an
    /// `ExecutionId` that carries [`ShardId::UNENCODED`] in its shard bits.
    ///
    /// # Panics
    ///
    /// Panics if `readable_shards` is empty or if any entry in
    /// `writable_shards` is absent from `readable_shards`.
    #[must_use]
    pub fn new(
        readable_shards: Vec<ShardId>,
        writable_shards: Vec<ShardId>,
        default_shard: ShardId,
    ) -> Self {
        assert!(
            !readable_shards.is_empty(),
            "ShardRouter requires at least one readable shard"
        );
        for writable in &writable_shards {
            assert!(
                readable_shards.contains(writable),
                "writable shard {writable} is not in the readable set"
            );
        }
        Self {
            readable_shards,
            writable_shards,
            default_shard,
            residency_map: BTreeMap::new(),
            shard_forwards: BTreeMap::new(),
        }
    }

    /// Attach an operator-declared residency key → shard mapping (issue #697).
    ///
    /// This is the *placement policy* for data-residency and tenant-isolation
    /// deployments: the operator — who alone knows which physical database sits
    /// in which jurisdiction — declares `"eu" -> ShardId(1)`, and application
    /// code then starts workflows with the semantic key rather than a shard
    /// number. Renumbering shards is a config change, not a redeploy.
    ///
    /// The mapping is deliberately **declared, not derived**. A hash (rendezvous
    /// or otherwise) cannot know which database is physically in the EU, and
    /// moves roughly `1/N` of its keys whenever a shard is added — so a hashed
    /// residency key could silently migrate a tenant's placement mid-contract.
    /// A declared map is stable by construction across process restarts and
    /// across any widening of the readable/writable sets.
    ///
    /// Keys are trimmed on insert, matching how a request's `residency_key` is
    /// trimmed before lookup — so a declared `" eu "` is reachable as `"eu"`
    /// rather than becoming a permanently dead entry.
    ///
    /// Calling this twice replaces the previous map.
    ///
    /// A declared target that is readable but currently drained out of
    /// `writable_shards` is **not** a panic — draining a residency shard is a
    /// legitimate transient state and crash-looping the fleet over it would be
    /// worse than the outage it signals. It logs a warning instead, because
    /// every start under that key will be refused until the shard is restored
    /// or the key remapped.
    ///
    /// # Panics
    ///
    /// Panics if a key is blank, maps to a shard outside `readable_shards`, or
    /// two keys normalize to the same trimmed key with *conflicting* shards
    /// (e.g. `("eu", 1)` and `(" eu ", 2)`) — which target survived would depend
    /// on the source's iteration order, and from an unordered source could
    /// differ across restarts. A duplicate that maps to the *same* shard is
    /// harmless and accepted. This mirrors [`ShardRouter::new`]'s existing panic
    /// contract so a misconfigured residency map fails at boot rather than at
    /// the first residency-pinned start.
    #[must_use]
    pub fn with_residency_map(
        mut self,
        entries: impl IntoIterator<Item = (String, ShardId)>,
    ) -> Self {
        // Insert one at a time rather than `.collect()`ing: two RAW keys that
        // normalize to the same trimmed key (`"eu"` and `" eu "`) would silently
        // overwrite one another, and from an UNORDERED source (a `HashMap`)
        // *which* target survives can differ across process restarts — breaking
        // the stable-mapping guarantee that is the entire point of a declared
        // map, and potentially moving work between jurisdictions on a restart.
        // Fail construction instead, consistent with the blank-key and
        // shard-outside-the-readable-set panics below.
        let mut map: BTreeMap<String, ShardId> = BTreeMap::new();
        for (raw, shard) in entries {
            let key = raw.trim().to_string();
            if let Some(existing) = map.insert(key.clone(), shard) {
                assert_eq!(
                    existing, shard,
                    "residency key '{key}' is declared more than once (after trimming) with \
                     conflicting shards {existing} and {shard}; which one survives would depend \
                     on iteration order"
                );
            }
        }
        for (key, shard) in &map {
            assert!(
                !key.is_empty(),
                "residency key must not be blank (mapped to shard {shard})"
            );
            assert!(
                self.readable_shards.contains(shard),
                "residency key '{key}' maps to shard {shard}, which is not in the readable set"
            );
            // Readable but drained: the router boots, yet EVERY start under this
            // key will be refused with `ShardNotWritable` until the shard is
            // restored to `writable_shards` or the key is remapped. That is the
            // likelier operational mistake (draining a shard is routine;
            // removing one from `readable_shards` is rare), so surface it at
            // boot rather than leaving it to be discovered from production 400s.
            if !self.writable_shards.contains(shard) {
                tracing::warn!(
                    residency_key = %key,
                    shard = %shard,
                    "residency key targets a shard that is readable but not writable (drained); \
                     new workflows pinned to this key will be rejected until the shard is \
                     restored to the writable set or the key is remapped"
                );
            }
        }
        self.residency_map = map;
        self
    }

    /// The declared residency key → shard mapping.
    ///
    /// Empty unless [`ShardRouter::with_residency_map`] was used.
    #[must_use]
    pub const fn residency_map(&self) -> &BTreeMap<String, ShardId> {
        &self.residency_map
    }

    /// Declare where ids minted on a **retired** shard now resolve (issue #964).
    ///
    /// Shard rebalancing seals a migrated execution's source row with a
    /// forwarding pointer, which is enough for as long as the source shard is
    /// still readable. A fully *decommissioned* shard is not, and its
    /// `ExecutionId`s would otherwise fall through
    /// [`ShardRouter::shard_for_execution`]'s unknown-shard branch to the
    /// default shard — resolving to the wrong database, silently. This map is
    /// the decommission story: once every resident has been migrated off shard
    /// A to shard B and A has been removed from `readable_shards`, declare
    /// `A → B` and every id minted on A keeps resolving.
    ///
    /// ```rust
    /// # use autumn_harvest::shard::ShardRouter;
    /// # use autumn_harvest::types::ShardId;
    /// let router = ShardRouter::new(
    ///     vec![ShardId::new(1)],
    ///     vec![ShardId::new(1)],
    ///     ShardId::new(1),
    /// )
    /// .with_shard_forwards([(ShardId::new(0), ShardId::new(1))]);
    /// ```
    ///
    /// # Panics
    ///
    /// Like [`ShardRouter::with_residency_map`], a misconfigured map fails at
    /// boot rather than at the first misrouted lookup. Panics when a source
    /// shard forwards to itself, when a source shard is still in
    /// `readable_shards` (forwarding a *live* shard would shadow its own rows),
    /// when a target is not readable, or when one source is declared twice with
    /// conflicting targets.
    ///
    /// Chains and cycles need no separate rule: a forward's source must be
    /// outside `readable_shards` and its target inside it, so no shard can be
    /// both, and resolution is therefore a single hop by construction.
    #[must_use]
    pub fn with_shard_forwards(
        mut self,
        entries: impl IntoIterator<Item = (ShardId, ShardId)>,
    ) -> Self {
        let mut map: BTreeMap<ShardId, ShardId> = BTreeMap::new();
        for (from, to) in entries {
            if let Some(existing) = map.insert(from, to) {
                assert_eq!(
                    existing, to,
                    "shard {from} is forwarded more than once with conflicting targets \
                     {existing} and {to}; which one survives would depend on iteration order"
                );
            }
        }
        for (from, to) in &map {
            assert_ne!(from, to, "shard {from} cannot be forwarded to itself");
            assert!(
                !self.readable_shards.contains(from),
                "shard {from} is forwarded to {to} but is still in the readable set; \
                 remove it from `readable_shards` first, or drop the forward"
            );
            assert!(
                self.readable_shards.contains(to),
                "shard {from} is forwarded to {to}, which is not in the readable set"
            );
            // A chain needs no separate check: the two assertions above already
            // make one impossible. A forward's SOURCE must be outside
            // `readable_shards` and its TARGET must be inside it, so no shard
            // can be both, and `shard_for_execution`'s single hop is therefore
            // exhaustive by construction rather than by a hop counter.
        }
        self.shard_forwards = map;
        self
    }

    /// The declared retired-shard → successor mapping (issue #964).
    ///
    /// Empty unless [`ShardRouter::with_shard_forwards`] was used.
    #[must_use]
    pub const fn shard_forwards(&self) -> &BTreeMap<ShardId, ShardId> {
        &self.shard_forwards
    }

    /// Borrow every placement-affecting field at once, for exhaustive
    /// destructuring by a projection that must not silently miss one.
    ///
    /// [`crate::effective_config::ShardTopologyView::from_router`] destructures
    /// the returned [`ShardRouterParts`] with **no** `..`, so adding a field
    /// here is a compile error until the operator-facing config snapshot
    /// surfaces it. That guard exists because `residency_map` — which decides
    /// which *jurisdiction* a pinned workflow lands in — was added to this
    /// router (issue #697) and silently missing from the snapshot until a
    /// review caught it.
    ///
    /// Add a field here whenever a new `ShardRouter` field influences
    /// placement; a purely internal cache or memo does not belong.
    // Not `const`: `Vec<ShardId> -> &[ShardId]` deref coercion is not yet
    // const-stable, and the MSRV is 1.88.
    #[must_use]
    pub fn parts(&self) -> ShardRouterParts<'_> {
        // First hop of the coverage guard: destructure `Self` EXHAUSTIVELY so a
        // new `ShardRouter` field is a compile error HERE, forcing an explicit
        // surface-or-omit decision. `ShardTopologyView::from_router` then
        // destructures the result with no `..` as the second hop. Do NOT add `..`.
        let Self {
            readable_shards,
            writable_shards,
            default_shard,
            residency_map,
            shard_forwards,
        } = self;
        ShardRouterParts {
            readable_shards,
            writable_shards,
            default_shard: *default_shard,
            residency_map,
            shard_forwards,
        }
    }

    /// Resolve a [`ShardPlacement`] to a concrete shard for a *new* workflow.
    ///
    /// [`ShardPlacement::Auto`] delegates to [`Self::pick_for_new_workflow`] and
    /// is byte-for-byte identical to the pre-#697 code path. The explicit
    /// variants validate first and never fall back to the default shard.
    ///
    /// Because the resolved shard is encoded into the `ExecutionId` the caller
    /// then mints, residency is transitive: children, continue-as-new
    /// successors, workflow-level retries, and reset forks all inherit
    /// `exec_id.shard()` and therefore stay on the pinned database.
    ///
    /// # Errors
    ///
    /// Returns [`ShardPlacementError`] when an explicit shard is unknown or
    /// drained, or a residency key is blank or undeclared.
    pub fn resolve_placement(
        &self,
        placement: &ShardPlacement,
        workflow_name: &str,
        workflow_id: &str,
    ) -> Result<ShardId, ShardPlacementError> {
        self.resolve_placement_inner(placement, workflow_name, workflow_id, true)
    }

    /// Resolve a [`ShardPlacement`] to the shard an execution for it *would*
    /// live on, **without** enforcing writability.
    ///
    /// Use this when the resolved shard is only being used to *look something
    /// up* — an idempotency-key dedup probe, an operator read — rather than to
    /// place new work. Writability is a fresh-start-only concern: refusing to
    /// even *read* from a shard the operator happens to be draining would, for
    /// example, turn an at-least-once retry of an already-committed keyed start
    /// into a spurious `400` instead of the `200` no-op it must return.
    ///
    /// Every other validation still applies — an unknown shard or an undeclared
    /// residency key is still an error, never a silent fall back to the default
    /// shard.
    ///
    /// # Errors
    ///
    /// Returns [`ShardPlacementError`] when an explicit shard is unknown, or a
    /// residency key is blank or undeclared. Never returns
    /// [`ShardPlacementError::ShardNotWritable`].
    pub fn resolve_placement_for_lookup(
        &self,
        placement: &ShardPlacement,
        workflow_name: &str,
        workflow_id: &str,
    ) -> Result<ShardId, ShardPlacementError> {
        self.resolve_placement_inner(placement, workflow_name, workflow_id, false)
    }

    fn resolve_placement_inner(
        &self,
        placement: &ShardPlacement,
        workflow_name: &str,
        workflow_id: &str,
        require_writable: bool,
    ) -> Result<ShardId, ShardPlacementError> {
        match placement {
            ShardPlacement::Auto => Ok(self.pick_for_new_workflow(workflow_name, workflow_id)),
            ShardPlacement::Shard(shard) => self.validate_pinned_shard(*shard, require_writable),
            ShardPlacement::ResidencyKey(key) => {
                let trimmed = key.trim();
                if trimmed.is_empty() {
                    return Err(ShardPlacementError::EmptyResidencyKey);
                }
                let shard = self.residency_map.get(trimmed).copied().ok_or_else(|| {
                    ShardPlacementError::UnknownResidencyKey {
                        key: trimmed.to_string(),
                        known: self.residency_map.keys().cloned().collect(),
                    }
                })?;
                // Defence in depth: `with_residency_map` already rejects targets
                // outside the readable set, but a router mutated by other means
                // (or a shard drained after the map was declared) must still
                // fail loud rather than place residency-bound work incorrectly.
                self.validate_pinned_shard(shard, require_writable)
            }
        }
    }

    /// Shared validation for an explicitly pinned shard.
    ///
    /// `require_writable` distinguishes placing new work (`true`) from resolving
    /// where existing work lives (`false`) — see
    /// [`Self::resolve_placement_for_lookup`].
    fn validate_pinned_shard(
        &self,
        shard: ShardId,
        require_writable: bool,
    ) -> Result<ShardId, ShardPlacementError> {
        // A shard number outside `0..=0xFFFE` is not representable in an
        // `ExecutionId`: `ExecutionId::new_for_shard` writes `shard & 0xFFFF`
        // into the UUID's first two bytes, so `65536` would encode as `0` and
        // `-1` as the `0xFFFF` sentinel. `ShardId::new` is an unbounded
        // `const fn`, so a router CAN be configured with such a value; accepting
        // the pin would write the row to the requested pool while every later
        // id-based lookup routed to the truncated shard, leaving a pinned
        // workflow inaccessible or visible through the wrong database. Reject it
        // before the set membership checks — the value is unusable regardless of
        // how the deployment is configured.
        //
        // `is_unencoded` (`0xFFFF`) is inside this rejected range: the sentinel
        // is a routing marker ("resolve to the deployment default"), never a
        // shard number, and accepting it would silently place pinned work on the
        // default shard — the exact failure this feature removes.
        if !is_encodable_shard(shard) {
            return Err(ShardPlacementError::UnknownShard {
                requested: shard,
                readable: self.readable_shards.clone(),
            });
        }
        if !self.readable_shards.contains(&shard) {
            return Err(ShardPlacementError::UnknownShard {
                requested: shard,
                readable: self.readable_shards.clone(),
            });
        }
        if require_writable && !self.writable_shards.contains(&shard) {
            return Err(ShardPlacementError::ShardNotWritable {
                requested: shard,
                writable: self.writable_shards.clone(),
            });
        }
        Ok(shard)
    }

    /// Build a router for a single-shard deployment.
    ///
    /// Equivalent to the pre-sharding runtime: all workflows land on
    /// `ShardId(0)` and all reads resolve to the same database.
    #[must_use]
    pub fn single() -> Self {
        let shard = ShardId::new(0);
        Self::new(vec![shard], vec![shard], shard)
    }

    /// Shards this router accepts reads from.
    #[must_use]
    pub fn readable_shards(&self) -> &[ShardId] {
        &self.readable_shards
    }

    /// Shards this router accepts *new* workflows on.
    #[must_use]
    pub fn writable_shards(&self) -> &[ShardId] {
        &self.writable_shards
    }

    /// Does this router currently accept *new* workflows on `shard`?
    ///
    /// A shard that is readable but not writable is being drained: existing
    /// work there is still served, but new work must not be placed on it.
    #[must_use]
    pub fn is_writable(&self, shard: ShardId) -> bool {
        self.writable_shards.contains(&shard)
    }

    /// The shard returned when an `ExecutionId` carries the unencoded sentinel.
    #[must_use]
    pub const fn default_shard(&self) -> ShardId {
        self.default_shard
    }

    /// Rendezvous-pick a *writable* shard for a brand new workflow, keyed on
    /// two arbitrary string tokens.
    ///
    /// The initial pick is taken over the full readable set (so the hash is
    /// stable while the writable set is widened/narrowed) and, when it lands
    /// outside the writable subset, re-hashed among the writable shards.
    fn pick_writable(&self, primary: &str, secondary: &str) -> ShardId {
        let initial = rendezvous_pick(&self.readable_shards, primary, secondary);
        if self.writable_shards.contains(&initial) {
            return initial;
        }
        if self.writable_shards.is_empty() {
            return self.default_shard;
        }
        rendezvous_pick(&self.writable_shards, primary, secondary)
    }

    /// Pick a shard for a brand new workflow using rendezvous hashing.
    ///
    /// The input is `(workflow_name, workflow_id)` which uniquely identifies
    /// the logical workflow independent of its execution UUID — the same
    /// `(name, id)` therefore always hashes to the same shard, making outbox
    /// retries idempotent.
    #[must_use]
    pub fn pick_for_new_workflow(&self, workflow_name: &str, workflow_id: &str) -> ShardId {
        self.pick_writable(workflow_name, workflow_id)
    }

    /// Pick a shard for a new workflow started via a request-scoped
    /// `idempotency_key` (issue #808).
    ///
    /// Unlike [`Self::pick_for_new_workflow`], which routes by `(workflow_name,
    /// workflow_id)`, this routes by `(workflow_name, idempotency_key)` so two
    /// same-key retries — whose `workflow_id` may be independently
    /// auto-generated per request — deterministically co-locate on the *same*
    /// shard, hit the same `harvest_start_idempotency` claim row, and
    /// deduplicate. Routing by `workflow_id` would scatter same-key retries
    /// across shards in a multi-shard deployment (one claim row per shard →
    /// one execution per shard), defeating dedup.
    ///
    /// The same writable-subset redirect as [`Self::pick_for_new_workflow`]
    /// applies (a keyed start still creates a brand-new workflow, so it must
    /// land on a writable shard).
    ///
    /// The caller (`api.rs`) only routes here when the `workflow_id` was
    /// auto-generated; an explicit `workflow_id` routes by `workflow_id`
    /// instead. That split is the resolution of the P1↔P2 routing tension —
    /// routing *all* keyed starts by the key would break the reuse-policy
    /// matrix for explicit-`workflow_id` starts (see the routing comment in
    /// `api.rs` and `docs/getting-started/06-idempotency.md`).
    ///
    /// KNOWN LIMITATION (Codex #808 P2): because this reuses `pick_writable`
    /// verbatim, keyed dedup inherits the *exact* shard-drain behavior of
    /// `(workflow_name, workflow_id)` uniqueness. If a key first claims a run on
    /// shard 0 and shard 0 is later removed from the writable set (drained to
    /// read-only) while still readable, a same-key retry rehashes via the
    /// writable-subset redirect to a *different* writable shard, probes/reserves
    /// a different shard-local `harvest_start_idempotency` row, and can create a
    /// second execution within the retention window. Issue #808 scopes dedup to
    /// be shard-local, exactly as `(name, workflow_id)` uniqueness already is —
    /// so this matches (and is bounded by) that existing guarantee. See the
    /// "Known limitation" note in `docs/getting-started/06-idempotency.md`.
    #[must_use]
    pub fn pick_for_idempotency_key(&self, workflow_name: &str, idempotency_key: &str) -> ShardId {
        self.pick_writable(workflow_name, idempotency_key)
    }

    /// Resolve the shard for an arbitrary `ExecutionId`.
    ///
    /// Returns the encoded shard when present, or the configured default
    /// shard for ids carrying [`ShardId::UNENCODED`].
    #[must_use]
    pub fn shard_for_execution(&self, exec_id: ExecutionId) -> ShardId {
        let encoded = exec_id.shard();
        if encoded.is_unencoded() {
            return self.default_shard;
        }
        if self.readable_shards.contains(&encoded) {
            return encoded;
        }
        // A retired shard's ids resolve to its declared successor (issue #964).
        // Checked BEFORE the default-shard fallback, which for a decommissioned
        // shard would silently answer with the wrong database. `with_shard_forwards`
        // rejects chains at construction, so this single hop is exhaustive.
        if let Some(successor) = self.shard_forwards.get(&encoded) {
            return *successor;
        }
        self.default_shard
    }

    /// Pick a shard for a DAG at catalog-compile time.
    ///
    /// DAG schedules (`harvest_schedules`) are scoped per database, so each
    /// DAG must be pinned to a single shard that owns it.
    /// The same name always maps to the same shard because rendezvous hashing
    /// is stable.
    #[must_use]
    pub fn pick_for_dag(&self, dag_name: &str) -> ShardId {
        let primary = if self.writable_shards.is_empty() {
            &self.readable_shards
        } else {
            &self.writable_shards
        };
        rendezvous_pick(primary, dag_name, "")
    }
}

/// Resolves the shard that owns `target` (issue #751).
///
/// For [`ExternalTarget::ExecutionId`] this is O(1) and always authoritative
/// — the shard is encoded in the id itself, exactly like every other
/// `ExecutionId`-keyed lookup in the engine.
///
/// For [`ExternalTarget::WorkflowId`] there is no execution id yet to decode,
/// so the shard is derived by rendezvous-hashing `(workflow_name,
/// workflow_id)` via [`ShardRouter::pick_for_new_workflow`] — the SAME hash a
/// fresh start of that business key would land on **under the default
/// [`ShardPlacement::Auto`] placement**. Every execution in a given
/// `(workflow_name, workflow_id)` chain (including every continue-as-new
/// successor, which is minted with [`ExecutionId::new_for_shard`] pinned to
/// its predecessor's shard) then lives on this same shard, so resolving a
/// `WorkflowId` target's owning shard needs no directory lookup for the
/// overwhelming majority of workflows.
///
/// # This is a placement *prediction*, not a location (issue #1146)
///
/// For a `WorkflowId` target the returned shard answers "where would a fresh
/// start of this business key be placed?" — **not** "where does this business
/// key live?". The two differ whenever a workflow was started with an explicit
/// pin (`ShardPlacement::Shard`/`ShardPlacement::ResidencyKey`, issue #697),
/// which can place it on a shard the pure hash never produces, and whenever a
/// shard has been drained out of `writable_shards` since the workflow was
/// placed, which moves where the same key re-hashes.
///
/// Its remaining callers — `worker::reject_cross_shard_continue_as_new` and the
/// deprecated [`ShardedDbPool::exact_pool_for_target`] — use it as a proxy for a
/// *third* question: "which shard would a shard-local uniqueness check for this
/// key run on?" (`execution`'s re-run `workflow_id`-override guard asks the same
/// question, but reaches `pick_for_new_workflow` directly rather than through
/// this function.) Those guards create the new run on an **existing** run's shard (the
/// predecessor's, the re-run source's), never on the hashed one, and both refuse
/// the operation when the two differ — because the uniqueness index they rely on
/// lives on one shard and cannot see a live run of the key on another. That
/// makes the hash the right input for them, but for a narrower reason than
/// "placing new work", and it means both are stricter than they have to be: a
/// residency-pinned run whose key hashes elsewhere is refused even though the
/// new run would be residency-correct and (since this issue) perfectly
/// reachable. Loosening either into a real cross-shard occupancy check is a
/// follow-up that #1146's fan-out now makes possible.
///
/// To find where an existing business key actually **lives** — which is what a
/// `workflow_id`-addressed signal/cancel delivery needs — use
/// [`crate::external_target_location::resolve_location_by_workflow_id`],
/// which observes every expected shard instead of predicting one. Delivery
/// stopped using this function for that in issue #1146.
///
/// Returns `None` only when `target` is a `WorkflowId` and the process-global
/// shard router has not been initialized (a boot-window / non-plugin-embedder
/// edge case). Both remaining callers treat that as "no divergence is knowable,
/// so do not refuse", which is also correct by construction: a deployment
/// without a router is single-shard, and a single shard cannot diverge from
/// itself. The delivery paths no longer consult this function at all, so the
/// pre-#1146 "assume same shard as the caller, attempt inline" fallback this
/// paragraph used to describe no longer exists.
#[cfg(feature = "db")]
#[must_use]
pub fn external_target_owning_shard(target: &ExternalTarget) -> Option<ShardId> {
    match target {
        ExternalTarget::ExecutionId(id) => Some(id.shard()),
        ExternalTarget::WorkflowId {
            workflow_name,
            workflow_id,
        } => GLOBAL_SHARD_ROUTER
            .read()
            .ok()
            .and_then(|guard| guard.as_ref().cloned())
            .map(|router| router.pick_for_new_workflow(workflow_name, workflow_id)),
    }
}

impl Default for ShardRouter {
    fn default() -> Self {
        Self::single()
    }
}

fn rendezvous_pick(shards: &[ShardId], primary: &str, secondary: &str) -> ShardId {
    debug_assert!(!shards.is_empty(), "rendezvous pick requires candidates");
    let mut best = shards[0];
    let mut best_hash = rendezvous_hash(best, primary, secondary);
    for shard in &shards[1..] {
        let candidate = rendezvous_hash(*shard, primary, secondary);
        if candidate > best_hash {
            best = *shard;
            best_hash = candidate;
        }
    }
    best
}

fn rendezvous_hash(shard: ShardId, primary: &str, secondary: &str) -> u64 {
    let mut hasher = seahash::SeaHasher::new();
    hasher.write_i32(shard.as_i32());
    hasher.write(primary.as_bytes());
    hasher.write_u8(0);
    hasher.write(secondary.as_bytes());
    hasher.finish()
}

/// Collection of [`DbPool`] handles keyed by [`ShardId`].
///
/// In single-shard deployments the map has one entry and
/// [`ShardedDbPool::pool_for`] / [`ShardedDbPool::pool_for_execution`] always
/// return it. Multi-shard deployments populate one pool per shard and rely on
/// the encoded shard bits in each [`ExecutionId`] for routing.
#[cfg(feature = "db")]
#[derive(Clone)]
pub struct ShardedDbPool {
    pools: BTreeMap<ShardId, DbPool>,
    default_shard: ShardId,
    /// Which shards share one physical pool (issue #1266). Shards with the
    /// same group number are the same physical database. A group number
    /// is otherwise opaque. It has no meaning across two `ShardedDbPool`
    /// instances. See `pool_groups`.
    pool_group: BTreeMap<ShardId, u32>,
}

/// Group shards by underlying pool identity (issue #1266).
///
/// Two `Pool` values are the same physical pool exactly when they are
/// clones of one `Arc`. `Pool::manager()` returns a reference into that
/// shared allocation, so `ptr::eq` on it detects aliasing safely, with no
/// private field or unsafe code.
///
/// This is the right signal for `from_map`, whose caller may hand in
/// clones of one pool under two shard ids. It cannot see through two
/// independently built pools that merely share a connection string.
/// `from_dsns` computes its own grouping for that case instead, with
/// [`canonical_dsn_key`], before the DSNs are ever built into a pool.
#[cfg(feature = "db")]
fn group_by_pool_identity(pools: &BTreeMap<ShardId, DbPool>) -> BTreeMap<ShardId, u32> {
    let mut representatives: Vec<&DbPool> = Vec::new();
    let mut groups = BTreeMap::new();
    for (shard, pool) in pools {
        let group = representatives
            .iter()
            .position(|existing| std::ptr::eq(existing.manager(), pool.manager()))
            .unwrap_or_else(|| {
                representatives.push(pool);
                representatives.len() - 1
            });
        groups.insert(*shard, u32::try_from(group).unwrap_or(u32::MAX));
    }
    groups
}

/// Canonical grouping key for a DSN (issue #1266).
///
/// Two DSNs can reach the same physical database while written
/// differently. Credentials can differ, a port can be explicit or
/// default, or a connection-tuning parameter such as `application_name`
/// or `sslmode` can differ. Comparing the raw strings would treat these
/// as separate databases. Each apparent group would then apply its own
/// protection decision to rows the other group was meant to protect.
///
/// Parsed with **`tokio_postgres::Config`**, the exact parser
/// `diesel_async` hands the DSN to at connect time. This is the same
/// choice `backup_verify.rs`'s `parse_dsn_identity` makes, for the same
/// reason. `url::Url` disagrees with it on percent-decoding
/// (`/%68arvest` is database `harvest`). It also disagrees on
/// `?dbname=`/`?host=`/`?port=`/`?hostaddr=` overrides, and on
/// comma-separated multi-host DSNs. Every one of those parses cleanly
/// under `url`. Each resolves somewhere else entirely at connect time.
/// A key built on `url` cannot see two spellings of one database as the
/// same pool.
///
/// The key keeps host and `hostaddr` (a numeric `host` counts as an
/// address, needing no DNS to compare). It prefers `hostaddr` over
/// `host` whenever `hostaddr` is given at all, since `hostaddr` pins
/// the actual TCP destination. Two DSNs sharing one stay one pool
/// however differently each spells the hostname. The key also keeps
/// port (defaulted to 5432 when absent) and the database name. It keeps
/// only a `search_path` setting extracted from the `options` parameter
/// — see [`extract_search_path`]. `options` is libpq's escape hatch for
/// arbitrary session settings, and `search_path` is the one setting it
/// can carry that picks the schema `harvest_audit_log` resolves to. Two
/// DSNs that differ only there can still reach different data and must
/// never be grouped as one pool. Every other query parameter is
/// dropped. So is every other `options` flag, such as
/// `application_name` or `client_min_messages`. None of it changes
/// which relation a query resolves against.
///
/// A DNS hostname is lowercased, since it is case-insensitive. A
/// Unix-socket path is kept as written instead, since a filesystem path
/// is not: `/run/PG-A` and `/run/pg-a` name different sockets.
///
/// A DSN with no path names no database. That is not the same as
/// naming none: libpq defaults an omitted `dbname` to the connecting
/// username. The key uses the username in that case, and only in that
/// case. The rest of this comment's reasoning against using the
/// username still holds whenever a path is present.
///
/// Four gaps are accepted rather than chased further:
/// - A host alias — two hostnames that resolve to one address — is not
///   detected. Closing it needs a live connection, and building a pool
///   must stay a pure, local operation with no network access.
/// - A role's own `search_path`, set server-side with `ALTER ROLE ...
///   SET search_path`, is invisible in the DSN. The username is dropped
///   with the rest of the credentials, not kept as a proxy for it. Two
///   DSNs for one database under different usernames are a supported
///   topology (`from_dsns`'s own `harvest shard rebalance` use, issue
///   #964). Treating them as different pools would reopen the exact bug
///   this key exists to close.
/// - A multi-host DSN's hosts and ports are each sorted and deduplicated
///   independently, not paired positionally. `host=a,b port=5432,6432`
///   and `host=a,b port=6432,5432` can name different endpoint pairs,
///   yet compare equal. `from_dsns` is built for its one documented use
///   — one host per shard entry (`harvest shard rebalance`, issue #964)
///   — where this never arises. Unlike the other two gaps, getting this
///   wrong over-merges rather than under-merges. The failure is a
///   skipped purge on one endpoint, not a premature delete. It is left
///   for whoever first needs multi-host `from_dsns` entries to fix
///   alongside a real use case to test it against.
/// - Two different `search_path` orders can still resolve one unqualified
///   relation to the identical schema. This happens when the
///   earlier-searched schemas in one order do not contain that relation
///   at all. `tenant_a,public` and `tenant_b,public` both resolve
///   `harvest_audit_log` from `public`, whenever neither tenant schema
///   defines its own copy of that table. Unlike the other three gaps,
///   this one is not conservative. Two aliases of one physical table can
///   compare as distinct pools. That is the same under-merging risk this
///   key exists to close elsewhere. Detecting it needs to know what each
///   named schema actually contains. That is a live catalog lookup, not
///   a fact this key can read from the DSN text. It is out of reach for
///   the same reason as the host alias gap above. Building a pool must
///   stay a pure, local operation with no network access. It is left
///   undetected rather than guessed at without a connection.
///
/// A DSN that does not parse falls back to the raw string, unchanged
/// from before this key existed.
#[cfg(feature = "db")]
fn canonical_dsn_key(dsn: &str) -> String {
    use std::str::FromStr as _;

    let Ok(config) = tokio_postgres::Config::from_str(dsn.trim()) else {
        return dsn.to_string();
    };

    let mut hostaddrs: Vec<String> = config
        .get_hostaddrs()
        .iter()
        .map(ToString::to_string)
        .collect();
    let explicit_hostaddr = !hostaddrs.is_empty();
    let mut hosts: Vec<String> = Vec::new();
    for h in config.get_hosts() {
        match h {
            // A numeric `host` is skipped outright once `hostaddr` is
            // explicit (issue #1266). `hostaddr` alone pins the TCP
            // destination then. `host` text -- numeric or not -- only
            // affects authentication, never which server is reached.
            // Folding a numeric `host` into `hostaddrs` regardless made
            // `host=10.0.0.1&hostaddr=10.0.0.2` key differently from
            // `host=alias&hostaddr=10.0.0.2`, even though both pin the
            // identical destination.
            tokio_postgres::config::Host::Tcp(_) if explicit_hostaddr => {}
            tokio_postgres::config::Host::Tcp(name) => {
                if let Ok(addr) = std::net::IpAddr::from_str(name) {
                    hostaddrs.push(addr.to_string());
                } else {
                    hosts.push(name.to_ascii_lowercase());
                }
            }
            #[cfg(unix)]
            tokio_postgres::config::Host::Unix(path) => {
                hosts.push(path.to_string_lossy().into_owned());
            }
        }
    }
    hosts.sort_unstable();
    hosts.dedup();
    hostaddrs.sort_unstable();
    hostaddrs.dedup();
    // `hostaddr` pins the actual TCP destination, so it wins over `host`
    // text. `backup_verify.rs`'s `parse_dsn_identity` treats it the same
    // way. Two DSNs sharing an address are one pool however differently
    // each spells the hostname. `host` matters only when neither side
    // pins an address.
    let location = if hostaddrs.is_empty() {
        &hosts
    } else {
        &hostaddrs
    };

    let mut ports: Vec<u16> = config.get_ports().to_vec();
    if ports.is_empty() {
        ports.push(5432);
    }
    ports.sort_unstable();
    ports.dedup();

    let db = config.get_dbname().or_else(|| config.get_user());
    let search_path = extract_search_path(config.get_options().unwrap_or_default());

    format!("{location:?}{ports:?}/{db:?}?search_path={search_path:?}")
}

/// Pulls only `search_path` settings out of a libpq `options` string,
/// discarding every other `-c name=value` flag it may carry (issue
/// #1266). `options` is a general escape hatch. An operator can set
/// `application_name`, `client_min_messages`, or anything else through
/// it just as easily as `search_path`. None of those change which
/// relation a query resolves against. Keeping the whole string verbatim
/// reopened the same bug this key exists to close. Two DSNs for the
/// same pool, differing only in an unrelated `-c` flag, no longer
/// merged.
///
/// This recognizes three shapes. One is whitespace-separated tokens
/// where a `-c` token is immediately followed by a `search_path=value`
/// token. The other two are one-token spellings: the compact
/// `-csearch_path=value`, and the long-form `--search_path=value`.
/// `PostgreSQL`'s own server documentation names the long form as an
/// alternate spelling for any run-time parameter. The GUC name itself
/// is matched case-insensitively in all three shapes (issue #1266).
/// `PostgreSQL` parameter names are case-insensitive, so
/// `SEARCH_PATH=shared` sets the identical GUC as `search_path=shared`.
/// The long form also normalizes a hyphen to an underscore in the name
/// before matching. `PostgreSQL` does the same when mapping a
/// `--long-option` to its GUC, so `--search-path=shared` sets the
/// identical GUC as `--search_path=shared`. A quoted value with
/// embedded spaces is not recognized. Treating an
/// unparsed `options` string as carrying no `search_path` is the
/// conservative direction here. It only widens which DSNs compare as
/// different, never the reverse.
///
/// Splitting honors libpq's own escaping rule for `options` (issue
/// #1266). A backslash before a space embeds a literal space in the
/// current argument rather than ending it. `\\` embeds a literal
/// backslash. Splitting on bare whitespace instead can truncate a value
/// at an escaped space. A truncated value can then differ from an
/// alias's untruncated one even when both name the same effective
/// schema. That is exactly the false difference this key must not
/// create, since it stops two aliases of one physical pool from being
/// combined.
///
/// The extracted value is then normalized the way `PostgreSQL` itself
/// parses a schema list (`SplitIdentifierString`): comma-separated,
/// with insignificant whitespace around each name. An unquoted name is
/// folded to lowercase, since `PostgreSQL` folds unquoted identifiers
/// the same way. A double-quoted name keeps its case. It can also
/// contain a comma or space that is not a separator. `""` inside one is
/// a literal quote character. `tenant,public` and `tenant, public` name
/// the same search path and must compare equal. `"tenant, one"` (one
/// quoted name) must never compare equal to `tenant,one` (two unquoted
/// names). Full `PostgreSQL` locale-dependent case folding is not
/// chased here; ASCII/Unicode lowercasing is the accepted
/// approximation, alongside the other documented gaps below.
///
/// `options` can repeat `-c search_path=...` more than once. libpq
/// applies each as a `SET` in order at session start, so only the last
/// one takes effect. Returning every match found would keep an
/// overridden, inert value in the key, splitting two DSNs whose
/// sessions actually resolve to the same schema. This keeps overwriting
/// as it scans, so the last match wins, matching what the server does.
#[cfg(feature = "db")]
fn extract_search_path(options: &str) -> Option<String> {
    let mut tokens = split_options_preserving_escapes(options).into_iter();
    let mut search_path = None;
    while let Some(tok) = tokens.next() {
        let value: Option<String> = if tok == "-c" {
            tokens
                .next()
                .and_then(|kv| strip_search_path_name(&kv).map(str::to_string))
        } else if let Some(rest) = tok.strip_prefix("-c") {
            strip_search_path_name(rest).map(str::to_string)
        } else if let Some(rest) = tok.strip_prefix("--") {
            strip_search_path_name_long_form(rest).map(str::to_string)
        } else {
            None
        };
        if let Some(value) = value {
            search_path = Some(normalize_search_path(&value));
        }
    }
    search_path
}

/// Splits a `name=value` token and returns `value` only when `name`
/// case-insensitively equals `search_path` (issue #1266). `PostgreSQL`
/// parameter names are case-insensitive, so `SEARCH_PATH=shared` sets
/// the identical GUC as `search_path=shared` and must extract the same
/// way.
#[cfg(feature = "db")]
fn strip_search_path_name(token: &str) -> Option<&str> {
    let (name, value) = token.split_once('=')?;
    name.eq_ignore_ascii_case("search_path").then_some(value)
}

/// Splits a long-form `--name=value` token and returns `value` only
/// when `name` names `search_path` (issue #1266). `PostgreSQL`
/// normalizes a hyphen to an underscore in a long-form GUC name before
/// matching it, so `--search-path=shared` sets the identical GUC as
/// `--search_path=shared`. The owned, hyphen-normalized name cannot
/// reuse `strip_search_path_name`'s borrow of the original token.
#[cfg(feature = "db")]
fn strip_search_path_name_long_form(token: &str) -> Option<&str> {
    let (name, value) = token.split_once('=')?;
    name.replace('-', "_")
        .eq_ignore_ascii_case("search_path")
        .then_some(value)
}

/// Splits a libpq `options` string into arguments, honoring its
/// documented escaping (issue #1266). A backslash before any
/// whitespace character embeds that character literally in the
/// current argument instead of ending it there. `PostgreSQL`'s own
/// splitter (`pg_split_opts`) tests with `isspace()`, not specifically
/// a space, so a tab or other whitespace escapes the same way. A
/// backslash before any other character consumes the backslash too
/// (issue #1266). `pg_split_opts` removes it unconditionally, so
/// `public\,public` reaches the server the same as `public,public` --
/// the backslash never survives to `SplitIdentifierString`. Keeping it
/// here would compare two equivalent values as different.
/// A trailing backslash with nothing after it has nothing to escape,
/// so it is kept literally. Naive whitespace splitting would end an
/// argument at an escaped whitespace character, corrupting any value
/// that contains one.
#[cfg(feature = "db")]
fn split_options_preserving_escapes(options: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut has_token = false;
    let mut chars = options.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\' && chars.peek().is_some() {
            current.push(chars.next().expect("peeked Some above"));
            has_token = true;
        } else if c.is_whitespace() {
            if has_token {
                tokens.push(std::mem::take(&mut current));
                has_token = false;
            }
        } else {
            current.push(c);
            has_token = true;
        }
    }
    if has_token {
        tokens.push(current);
    }
    tokens
}

/// Normalizes a `search_path` value by parsing it as a `PostgreSQL`
/// identifier list and rejoining the result (issue #1266). Falls back
/// to the value unchanged when it does not parse. A malformed value
/// cannot be normalized, so it is kept distinguishable from every other
/// value rather than guessed at. This is the same conservative choice
/// made elsewhere in this key.
///
/// Each item is escaped before rejoining, backslash-quoting its own
/// backslashes and commas (issue #1266). Joining with a bare `,` would
/// let a quoted item's own embedded comma read back as an item
/// boundary. The one item `tenant,one` and the two items `tenant` and
/// `one` would then both join to the same string `tenant,one` -- a
/// real collision. Escaping first keeps every item's boundary in the
/// joined key, so the two cases above never compare equal.
///
/// `pg_catalog` is inserted at the front when the parsed list omits it
/// (issue #1266). `PostgreSQL` always searches `pg_catalog` first when
/// it is not named explicitly. `public` and `pg_catalog,public`
/// therefore resolve an unqualified relation the same way, and must
/// key the same. A list that already names `pg_catalog` anywhere is
/// left as-is. Its explicit position then decides the resolution
/// order, and an explicit, non-leading position is a genuinely
/// different order from the implicit one.
///
/// `pg_temp` is inserted the same way, but at the very front (issue
/// #1266). `PostgreSQL` searches the session's temporary-object schema
/// before `pg_catalog` too, unless `pg_temp` is named explicitly. The
/// two implicit insertions are independent, so `pg_temp` is applied
/// after `pg_catalog`'s, landing ahead of it exactly when both were
/// omitted.
///
/// A repeated name is then dropped, keeping only its first occurrence
/// (issue #1266). `public` and `public,public` search the identical
/// schema in the identical order. A later repeat of a name already
/// searched changes nothing about where a relation resolves, so they
/// must key the same too.
#[cfg(feature = "db")]
fn normalize_search_path(value: &str) -> String {
    parse_identifier_list(value).map_or_else(
        || value.to_string(),
        |mut items| {
            if !items.iter().any(|item| item == "pg_catalog") {
                items.insert(0, "pg_catalog".to_string());
            }
            if !items.iter().any(|item| item == "pg_temp") {
                items.insert(0, "pg_temp".to_string());
            }
            let mut seen = std::collections::HashSet::new();
            items.retain(|item| seen.insert(item.clone()));
            items
                .iter()
                .map(|item| escape_identifier_list_item(item))
                .collect::<Vec<_>>()
                .join(",")
        },
    )
}

/// Backslash-escapes a parsed identifier-list item's own backslashes
/// and commas (issue #1266). Joining escaped items with a bare `,`
/// then keeps every item boundary recoverable in the joined string.
#[cfg(feature = "db")]
fn escape_identifier_list_item(item: &str) -> String {
    let mut escaped = String::with_capacity(item.len());
    for c in item.chars() {
        if c == '\\' || c == ',' {
            escaped.push('\\');
        }
        escaped.push(c);
    }
    escaped
}

/// The longest identifier `PostgreSQL` stores without truncating it
/// (issue #1266). `NAMEDATALEN` is 64, and one byte is reserved for
/// the terminator. A name longer than this is silently truncated to
/// it. `SplitIdentifierString` truncates each `search_path` entry the
/// same way. Two names differing only after this many bytes truncate
/// to the identical stored name and must key the same.
#[cfg(feature = "db")]
const POSTGRES_MAX_IDENTIFIER_LEN: usize = 63;

/// Truncates `name` to `PostgreSQL`'s identifier length limit (issue
/// #1266). This cuts at the last full character rather than splitting
/// a multi-byte one, matching `PostgreSQL`'s own byte-based truncation.
#[cfg(feature = "db")]
fn truncate_postgres_identifier(name: &str) -> &str {
    if name.len() <= POSTGRES_MAX_IDENTIFIER_LEN {
        return name;
    }
    let mut end = POSTGRES_MAX_IDENTIFIER_LEN;
    while !name.is_char_boundary(end) {
        end -= 1;
    }
    &name[..end]
}

/// Parses a comma-separated identifier list the way `PostgreSQL`'s own
/// `SplitIdentifierString` does (issue #1266), used for `search_path`
/// and similar GUCs. Whitespace around an item is not significant. An
/// unquoted item is folded to lowercase, matching `PostgreSQL`'s own
/// folding of an unquoted identifier. A double-quoted item keeps its
/// case verbatim, including any comma or whitespace it encloses; `""`
/// inside one is a literal quote character. Either form is then
/// truncated to `PostgreSQL`'s identifier length limit, matching what
/// `SplitIdentifierString` itself does. Returns `None` on anything
/// that does not fit this grammar, rather than guessing at a malformed
/// value. An unterminated quote is one such case. Content trailing a
/// closing quote before the next comma is another.
#[cfg(feature = "db")]
fn parse_identifier_list(value: &str) -> Option<Vec<String>> {
    let mut items = Vec::new();
    let mut chars = value.chars().peekable();
    loop {
        while chars.next_if(|c| c.is_whitespace()).is_some() {}
        match chars.peek() {
            None => break,
            Some('"') => {
                chars.next();
                let mut ident = String::new();
                loop {
                    match chars.next() {
                        None => return None,
                        Some('"') if chars.peek() == Some(&'"') => {
                            ident.push('"');
                            chars.next();
                        }
                        Some('"') => break,
                        Some(c) => ident.push(c),
                    }
                }
                items.push(truncate_postgres_identifier(&ident).to_string());
            }
            Some(_) => {
                let mut ident = String::new();
                while let Some(&c) = chars.peek() {
                    if c == ',' || c.is_whitespace() {
                        break;
                    }
                    ident.push(c);
                    chars.next();
                }
                let folded = ident.to_lowercase();
                items.push(truncate_postgres_identifier(&folded).to_string());
            }
        }
        while chars.next_if(|c| c.is_whitespace()).is_some() {}
        match chars.next() {
            None => break,
            Some(',') => {}
            Some(_) => return None,
        }
    }
    Some(items)
}

#[cfg(feature = "db")]
impl std::fmt::Debug for ShardedDbPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedDbPool")
            .field("shards", &self.pools.keys())
            .field("default_shard", &self.default_shard)
            .field("pool_group", &self.pool_group)
            .finish()
    }
}

#[cfg(feature = "db")]
pub static GLOBAL_SHARDED_POOL: std::sync::RwLock<Option<ShardedDbPool>> =
    std::sync::RwLock::new(None);

#[cfg(feature = "db")]
pub static GLOBAL_SHARD_ROUTER: std::sync::RwLock<Option<ShardRouter>> =
    std::sync::RwLock::new(None);

/// Install a `ShardRouter` into the global registry.
///
/// This should only be called once during runtime initialization (e.g. by
/// the `HarvestRunner` or `HarvestApiRuntime`) to avoid race conditions and
/// overwrites from temporary constructors in tests.
#[cfg(feature = "db")]
pub fn install_global_router(router: ShardRouter) {
    if let Ok(mut lock) = GLOBAL_SHARD_ROUTER.write() {
        *lock = Some(router);
    }
}

/// Install a `ShardedDbPool` into the global registry.
///
/// `ShardedDbPool::single` and `from_map` already self-install at
/// construction, so the global normally reflects whichever pool was built
/// **last** — which is not necessarily the pool the runtime went on to select.
/// A runtime that resolves a pool by precedence (see
/// `runner::resolve_runtime_storage_pool`) must therefore re-install its
/// choice, or every consumer of the global — the by-id fan-out, the inline
/// gate, completion triggers, the timeout sweeps — reads a pool the runtime is
/// not using (issue #1146 review).
///
/// Same caveat as [`install_global_router`]: this is runtime-initialization
/// API, not something to call from a temporary constructor.
#[cfg(feature = "db")]
pub fn install_global_sharded_pool(pool: ShardedDbPool) {
    if let Ok(mut lock) = GLOBAL_SHARDED_POOL.write() {
        *lock = Some(pool);
    }
}

#[cfg(feature = "db")]
impl ShardedDbPool {
    /// Wrap an existing single pool as a one-shard sharded pool at `ShardId(0)`.
    ///
    /// This is the shape used by every pre-sharding deployment. All lookups
    /// resolve to the same pool and no behavior changes.
    #[must_use]
    pub fn single(pool: DbPool) -> Self {
        let shard = ShardId::new(0);
        let mut pools = BTreeMap::new();
        pools.insert(shard, pool);
        let this = Self {
            pools,
            default_shard: shard,
            pool_group: BTreeMap::from([(shard, 0)]),
        };
        if let Ok(mut lock) = GLOBAL_SHARDED_POOL.write() {
            *lock = Some(this.clone());
        }
        this
    }

    /// Build a sharded pool from a pre-computed map of shard → pool.
    ///
    /// # Panics
    ///
    /// Panics if `pools` is empty or does not contain `default_shard`.
    #[must_use]
    pub fn from_map(pools: BTreeMap<ShardId, DbPool>, default_shard: ShardId) -> Self {
        assert!(
            !pools.is_empty(),
            "ShardedDbPool requires at least one pool"
        );
        assert!(
            pools.contains_key(&default_shard),
            "default_shard {default_shard} has no configured pool"
        );
        let pool_group = group_by_pool_identity(&pools);
        let this = Self {
            pools,
            default_shard,
            pool_group,
        };
        if let Ok(mut lock) = GLOBAL_SHARDED_POOL.write() {
            *lock = Some(this.clone());
        }
        this
    }

    /// Every shard, grouped by underlying physical pool (issue #1266).
    ///
    /// `from_map` detects a shared pool by object identity — aliased
    /// clones, the shape a pre-split staging deployment uses. `from_dsns`
    /// builds a fresh pool per entry, even for two DSNs that reach one
    /// physical database. It detects the alias from a canonical form of
    /// each connection string instead.
    ///
    /// # Panics
    ///
    /// Never, in practice. Every constructor keeps `pool_group` naming
    /// exactly the shards present in `pools`.
    #[must_use]
    pub fn pool_groups(&self) -> Vec<(&DbPool, Vec<ShardId>)> {
        let mut by_group: BTreeMap<u32, Vec<ShardId>> = BTreeMap::new();
        for (shard, group) in &self.pool_group {
            by_group.entry(*group).or_default().push(*shard);
        }
        by_group
            .into_values()
            .map(|shards| {
                let pool = self
                    .pools
                    .get(&shards[0])
                    .expect("pool_group only names shards with a pool");
                (pool, shards)
            })
            .collect()
    }

    /// The default shard used when an `ExecutionId` carries the unencoded
    /// sentinel or references a shard that isn't configured locally.
    #[must_use]
    pub const fn default_shard(&self) -> ShardId {
        self.default_shard
    }

    /// Look up the pool for a shard. Falls back to the default shard when the
    /// requested shard is not present in this map.
    ///
    /// # Panics
    ///
    /// Panics only if the pool was constructed by bypassing the public API
    /// and the default shard entry has been removed; [`ShardedDbPool::single`]
    /// and [`ShardedDbPool::from_map`] guarantee a default entry exists.
    #[must_use]
    pub fn pool_for(&self, shard: ShardId) -> &DbPool {
        self.pools
            .get(&shard)
            .or_else(|| self.pools.get(&self.default_shard))
            .expect("default shard pool is always present")
    }

    /// Look up the pool for a shard exactly, with no default fallback.
    #[must_use]
    pub fn exact_pool_for(&self, shard: ShardId) -> Option<&DbPool> {
        self.pools.get(&shard)
    }

    /// The shard an `ExecutionId` routes to, after any router-declared
    /// retired-shard forward (issue #964).
    ///
    /// This is the **entry point** for an id, not necessarily where the run
    /// currently lives: an execution rebalanced off a still-readable shard is
    /// found by following the durable forwarding pointer on its sealed source
    /// row, which needs a database and therefore lives in
    /// [`crate::shard_rebalance::resolve_execution_shard`]. This function is the
    /// synchronous part — the one that costs nothing on the hot path and is
    /// correct for every execution that never moved.
    #[must_use]
    pub fn routed_shard_for_execution(&self, exec_id: ExecutionId) -> ShardId {
        let shard = exec_id.shard();
        if shard.is_unencoded() {
            return self.default_shard;
        }
        // Consult the router for a declared retired-shard forward ONLY. Nothing
        // else about the router is applied here: an id whose shard the router
        // does not forward must keep resolving byte-for-byte as it did before
        // issue #964, including the cases where the pool map and the router's
        // readable set legitimately disagree (mid a shard-add rollout, or in a
        // test that installs a pool without a router).
        if let Ok(guard) = GLOBAL_SHARD_ROUTER.read()
            && let Some(successor) = guard.as_ref().and_then(|r| r.shard_forwards.get(&shard))
        {
            return *successor;
        }
        shard
    }

    /// Whether `shard` has been declared **retired** — decommissioned, its pool
    /// removed from every node, and its ids forwarded to a successor.
    ///
    /// A retired shard is not "a shard I happen to have no pool for right now".
    /// The two look identical from the pool map and must not be treated alike:
    /// a missing pool mid a shard-add rollout is a transient gap where the data
    /// is very much still there, while a retired shard is one an operator has
    /// asserted is gone. `ShardRouter::with_shard_forwards` refuses to declare a
    /// forward for a shard that is still readable, which is what makes the
    /// declaration mean something.
    ///
    /// The distinction matters wherever code must reach data on a shard rather
    /// than merely route to it — cross-residence payload erasure above all,
    /// which fails closed on an unreachable residence and would otherwise be
    /// permanently unable to erase any run that ever lived on a retired shard.
    #[must_use]
    pub fn shard_is_retired(shard: ShardId) -> bool {
        GLOBAL_SHARD_ROUTER
            .read()
            .ok()
            .and_then(|guard| {
                guard
                    .as_ref()
                    .map(|r| r.shard_forwards.contains_key(&shard))
            })
            .unwrap_or(false)
    }

    /// Build a multi-shard pool from `(shard, DSN)` pairs.
    ///
    /// The paved path for operator tooling that must reach several shard
    /// databases directly rather than through the management API — `harvest
    /// shard rebalance` (issue #964) is the first such command, and a rebalance
    /// is inherently two-database. Lives here rather than in the CLI so the
    /// pool's shape (and its `max_size`) stays a property of the engine.
    ///
    /// # Errors
    ///
    /// [`HarvestError::Config`](crate::error::HarvestError::Config) when a DSN
    /// is not a usable connection string. Note that a pool is lazy: a DSN that
    /// parses but cannot connect surfaces at first checkout, as
    /// [`HarvestError::ShardUnavailable`](crate::error::HarvestError::ShardUnavailable).
    pub fn from_dsns(
        entries: impl IntoIterator<Item = (ShardId, String)>,
        default_shard: ShardId,
        max_size: usize,
    ) -> crate::error::HarvestResult<Self> {
        let mut pools = BTreeMap::new();
        // A fresh `Pool` is built per entry here, even for two DSNs that
        // reach one physical database (issue #1266).
        // `group_by_pool_identity` could never see through that, so
        // `canonical_dsn_key` is the grouping key, compared before the
        // DSN is consumed into a manager.
        let mut seen_keys: Vec<String> = Vec::new();
        let mut pool_group = BTreeMap::new();
        for (shard, dsn) in entries {
            let key = canonical_dsn_key(&dsn);
            let group = seen_keys
                .iter()
                .position(|seen| *seen == key)
                .unwrap_or_else(|| {
                    seen_keys.push(key.clone());
                    seen_keys.len() - 1
                });
            pool_group.insert(shard, u32::try_from(group).unwrap_or(u32::MAX));

            let manager = diesel_async::pooled_connection::AsyncDieselConnectionManager::<
                diesel_async::AsyncPgConnection,
            >::new(dsn);
            let pool = deadpool::managed::Pool::builder(manager)
                .max_size(max_size.max(1))
                .build()
                .map_err(|e| {
                    crate::error::HarvestError::Config(format!(
                        "shard {shard}: could not build a connection pool: {e}"
                    ))
                })?;
            pools.insert(shard, pool);
        }
        let mut this = Self::from_map(pools, default_shard);
        this.pool_group = pool_group;
        if let Ok(mut lock) = GLOBAL_SHARDED_POOL.write() {
            *lock = Some(this.clone());
        }
        Ok(this)
    }

    /// Resolve the pool that owns a given `ExecutionId`.
    #[must_use]
    pub fn pool_for_execution(&self, exec_id: ExecutionId) -> &DbPool {
        self.pool_for(self.routed_shard_for_execution(exec_id))
    }

    /// Resolve the pool that owns a given `ExecutionId` exactly, with no default fallback.
    #[must_use]
    pub fn exact_pool_for_execution(&self, exec_id: ExecutionId) -> Option<&DbPool> {
        self.pools.get(&self.routed_shard_for_execution(exec_id))
    }

    /// Resolve the pool that owns `target` exactly, with no default fallback
    /// (issue #751).
    ///
    /// For [`ExternalTarget::ExecutionId`] this delegates to
    /// [`Self::exact_pool_for_execution`] and is authoritative. For
    /// [`ExternalTarget::WorkflowId`] the owning shard is resolved via
    /// [`external_target_owning_shard`]; when that returns `None` (the
    /// process-global shard router isn't initialized), `fallback_shard` is used
    /// instead.
    ///
    /// # Deprecated (issue #1146)
    ///
    /// Asking which *pool* holds a target is always a "where does this live?"
    /// question, and for a `WorkflowId` target the rendezvous hash answers a
    /// different one — "where would a fresh start go?" (see
    /// [`external_target_owning_shard`]). A workflow pinned by an explicit
    /// [`ShardPlacement`], or one left behind by a shard drained out of
    /// `writable_shards`, resolves here to a pool it is not in, and the caller
    /// concludes the target does not exist.
    ///
    /// Use [`crate::external_target_location::resolve_location_by_workflow_id`]
    /// and then [`Self::exact_pool_for`] on the shard it reports. Retained,
    /// rather than removed, because it is public API.
    #[deprecated(
        since = "0.7.0",
        note = "hash-derived and wrong for explicitly-placed or drained-shard workflows; \
                use external_target_location::resolve_location_by_workflow_id then exact_pool_for"
    )]
    #[must_use]
    pub fn exact_pool_for_target(
        &self,
        target: &ExternalTarget,
        fallback_shard: ShardId,
    ) -> Option<&DbPool> {
        match target {
            ExternalTarget::ExecutionId(id) => self.exact_pool_for_execution(*id),
            ExternalTarget::WorkflowId { .. } => {
                let shard = external_target_owning_shard(target).unwrap_or(fallback_shard);
                self.exact_pool_for(shard)
            }
        }
    }

    /// Iterate over `(shard, pool)` pairs in ascending shard order.
    pub fn iter_shards(&self) -> impl Iterator<Item = (ShardId, &DbPool)> {
        self.pools.iter().map(|(shard, pool)| (*shard, pool))
    }

    /// Shards this pool serves, in ascending order.
    #[must_use]
    pub fn shard_ids(&self) -> Vec<ShardId> {
        self.pools.keys().copied().collect()
    }

    /// How many shards are represented.
    #[must_use]
    pub fn len(&self) -> usize {
        self.pools.len()
    }

    /// Is this an empty pool map?
    ///
    /// Always `false` for values constructed through the public API but
    /// exposed for completeness.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pools.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn router_with(shards: &[i32]) -> ShardRouter {
        let ids: Vec<ShardId> = shards.iter().copied().map(ShardId::new).collect();
        ShardRouter::new(ids.clone(), ids.clone(), ids[0])
    }

    #[test]
    fn single_router_always_returns_shard_zero() {
        let router = ShardRouter::single();
        assert_eq!(
            router.pick_for_new_workflow("onboarding", "user-1"),
            ShardId::new(0)
        );
        assert_eq!(
            router.pick_for_new_workflow("etl", "nightly"),
            ShardId::new(0)
        );
        assert_eq!(router.default_shard(), ShardId::new(0));
    }

    #[test]
    fn rendezvous_hash_is_stable_across_runs() {
        let router = router_with(&[0, 1, 2, 3]);
        let a = router.pick_for_new_workflow("onboarding", "user-42");
        let b = router.pick_for_new_workflow("onboarding", "user-42");
        assert_eq!(a, b);
    }

    #[test]
    fn rendezvous_hash_distributes_across_shards() {
        let router = router_with(&[0, 1, 2]);
        let mut counts = [0usize; 3];
        for i in 0..300 {
            let shard = router.pick_for_new_workflow("onboarding", &format!("user-{i}"));
            counts[usize::try_from(shard.as_i32()).unwrap()] += 1;
        }
        // With 300 samples across 3 shards we expect ~100 each; allow a wide
        // band to stay robust against hash skew.
        for count in counts {
            assert!(count > 50, "shard counts too imbalanced: {counts:?}");
            assert!(count < 200, "shard counts too imbalanced: {counts:?}");
        }
    }

    #[test]
    fn writable_subset_redirects_when_initial_pick_is_read_only() {
        let readable = vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)];
        let writable = vec![ShardId::new(1)];
        let router = ShardRouter::new(readable, writable, ShardId::new(0));

        for i in 0..20 {
            let picked = router.pick_for_new_workflow("wf", &format!("id-{i}"));
            assert_eq!(picked, ShardId::new(1));
        }
    }

    #[test]
    fn shard_for_execution_falls_back_on_unencoded_sentinel() {
        let router = router_with(&[0, 1, 2]);
        let unencoded = ExecutionId::new();
        assert_eq!(router.shard_for_execution(unencoded), ShardId::new(0));
    }

    #[test]
    fn shard_for_execution_honours_encoded_shard() {
        let router = router_with(&[0, 1, 2]);
        let id = ExecutionId::new_for_shard(ShardId::new(2));
        assert_eq!(router.shard_for_execution(id), ShardId::new(2));
    }

    #[test]
    fn shard_for_execution_falls_back_when_shard_is_unknown() {
        let router = router_with(&[0, 1]);
        let id = ExecutionId::new_for_shard(ShardId::new(7));
        assert_eq!(router.shard_for_execution(id), ShardId::new(0));
    }

    #[test]
    fn pick_for_dag_is_stable() {
        let router = router_with(&[0, 1, 2, 3]);
        let a = router.pick_for_dag("daily_etl");
        let b = router.pick_for_dag("daily_etl");
        assert_eq!(a, b);
    }

    // ── issue #808: key-based idempotency routing ────────────────────────────

    #[test]
    fn pick_for_idempotency_key_is_deterministic_for_same_name_and_key() {
        let router = router_with(&[0, 1, 2, 3]);
        // The same (name, key) always resolves to the same shard so two
        // same-key retries co-locate and dedup — independent of workflow_id.
        let a = router.pick_for_idempotency_key("order_flow", "delivery-42");
        let b = router.pick_for_idempotency_key("order_flow", "delivery-42");
        assert_eq!(a, b);
    }

    #[test]
    fn pick_for_idempotency_key_ignores_workflow_id_entirely() {
        // Routing must depend only on (name, key), never on workflow_id: a
        // same-key retry whose workflow_id was auto-generated per request must
        // still land on the same shard. The method takes no workflow_id, so
        // this is guaranteed by construction — assert two independent calls
        // (as two separate requests would make) agree.
        let router = router_with(&[0, 1, 2, 3, 4]);
        let first = router.pick_for_idempotency_key("wf", "same-key");
        let second = router.pick_for_idempotency_key("wf", "same-key");
        assert_eq!(
            first, second,
            "same key must co-locate regardless of per-request workflow_id"
        );
    }

    #[test]
    fn distinct_keys_can_map_to_distinct_shards() {
        let router = router_with(&[0, 1, 2, 3]);
        let mut seen = std::collections::BTreeSet::new();
        for i in 0..200 {
            seen.insert(router.pick_for_idempotency_key("wf", &format!("key-{i}")));
        }
        assert!(
            seen.len() > 1,
            "distinct keys must spread across shards: {seen:?}"
        );
    }

    #[test]
    fn keyed_pick_honours_writable_subset() {
        // A keyed start still creates a brand-new workflow, so it must land on
        // a writable shard even when the key hashes to a read-only shard.
        let readable = vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)];
        let writable = vec![ShardId::new(1)];
        let router = ShardRouter::new(readable, writable, ShardId::new(0));
        for i in 0..20 {
            assert_eq!(
                router.pick_for_idempotency_key("wf", &format!("k-{i}")),
                ShardId::new(1)
            );
        }
    }

    // ── issue #697: explicit shard pinning for data residency ────────────────

    /// Build a three-shard router with an `eu`/`us` residency map.
    fn residency_router() -> ShardRouter {
        ShardRouter::new(
            vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)],
            vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)],
            ShardId::new(0),
        )
        .with_residency_map([
            ("eu".to_string(), ShardId::new(1)),
            ("us".to_string(), ShardId::new(2)),
        ])
    }

    #[test]
    fn auto_placement_is_byte_for_byte_todays_rendezvous_routing() {
        // AC1: when no placement is supplied, behaviour must be identical to
        // `pick_for_new_workflow` for every input — zero regression.
        let router = residency_router();
        for i in 0..200 {
            let wid = format!("user-{i}");
            assert_eq!(
                router
                    .resolve_placement(&ShardPlacement::Auto, "onboarding", &wid)
                    .expect("Auto never fails"),
                router.pick_for_new_workflow("onboarding", &wid),
            );
        }
    }

    #[test]
    fn default_placement_is_auto() {
        assert_eq!(ShardPlacement::default(), ShardPlacement::Auto);
    }

    #[test]
    fn explicit_shard_placement_wins_over_the_hash() {
        let router = residency_router();
        // Find a workflow id whose hash does NOT land on shard 2 so the pin is
        // observably doing the work.
        let wid = (0..500)
            .map(|i| format!("id-{i}"))
            .find(|wid| router.pick_for_new_workflow("wf", wid) != ShardId::new(2))
            .expect("some id must hash off shard 2");
        assert_eq!(
            router
                .resolve_placement(&ShardPlacement::Shard(ShardId::new(2)), "wf", &wid)
                .expect("shard 2 is readable and writable"),
            ShardId::new(2)
        );
    }

    #[test]
    fn explicit_shard_outside_readable_set_is_rejected_not_defaulted() {
        // AC2: typed, actionable error — never a panic and never a silent
        // fallback to the default shard.
        let router = residency_router();
        let err = router
            .resolve_placement(&ShardPlacement::Shard(ShardId::new(9)), "wf", "id-1")
            .expect_err("shard 9 is not configured");
        match err {
            ShardPlacementError::UnknownShard {
                requested,
                readable,
            } => {
                assert_eq!(requested, ShardId::new(9));
                assert_eq!(
                    readable,
                    vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)]
                );
            }
            other => panic!("expected UnknownShard, got {other:?}"),
        }
        // The message must name the offending shard and the valid set.
        let msg = router
            .resolve_placement(&ShardPlacement::Shard(ShardId::new(9)), "wf", "id-1")
            .unwrap_err()
            .to_string();
        assert!(msg.contains('9'), "message must name the shard: {msg}");
        assert!(
            msg.contains("readable"),
            "message must name the valid set: {msg}"
        );
    }

    #[test]
    fn explicit_shard_that_is_readable_but_drained_is_rejected() {
        // A shard drained out of `writable_shards` is deliberately not
        // accepting new work; pinning new work onto it must fail loud rather
        // than contradict the operator's drain.
        let router = ShardRouter::new(
            vec![ShardId::new(0), ShardId::new(1)],
            vec![ShardId::new(0)],
            ShardId::new(0),
        );
        let err = router
            .resolve_placement(&ShardPlacement::Shard(ShardId::new(1)), "wf", "id-1")
            .expect_err("shard 1 is readable but drained");
        assert!(
            matches!(err, ShardPlacementError::ShardNotWritable { requested, .. } if requested == ShardId::new(1)),
            "expected ShardNotWritable, got {err:?}"
        );
    }

    #[test]
    fn unencoded_sentinel_is_never_a_valid_pin() {
        // `ShardId::UNENCODED` is a routing sentinel, not a shard number.
        let router = residency_router();
        let err = router
            .resolve_placement(&ShardPlacement::Shard(ShardId::UNENCODED), "wf", "id-1")
            .expect_err("the sentinel is not a placeable shard");
        assert!(
            matches!(err, ShardPlacementError::UnknownShard { .. }),
            "expected UnknownShard, got {err:?}"
        );
    }

    /// Codex P1 (issue #697 review): `ShardId::new` is an unbounded `const fn`,
    /// but `ExecutionId::new_for_shard` masks the value to 16 bits. A pin to a
    /// shard outside `0..=0xFFFE` would write the row to the requested pool
    /// while every id-based lookup routed to the TRUNCATED shard, leaving the
    /// workflow inaccessible or visible through the wrong database. Rejected
    /// even when the router is (mis)configured to consider it readable+writable.
    #[test]
    fn shard_outside_the_encodable_range_is_rejected_even_when_configured() {
        for raw in [0x1_0000, 0x1_0001, i32::MAX, -1, i32::MIN] {
            let out_of_range = ShardId::new(raw);
            // Deliberately configure the router to accept it, so the rejection
            // can only come from the encodability check.
            let router = ShardRouter::new(
                vec![ShardId::new(0), out_of_range],
                vec![ShardId::new(0), out_of_range],
                ShardId::new(0),
            );
            let err = router
                .resolve_placement(&ShardPlacement::Shard(out_of_range), "wf", "id-1")
                .expect_err("a non-encodable shard must never be a valid pin");
            assert!(
                matches!(err, ShardPlacementError::UnknownShard { .. }),
                "expected UnknownShard for {raw}, got {err:?}"
            );
        }
    }

    #[test]
    fn encodable_shard_predicate_matches_the_uuid_round_trip() {
        // Everything the predicate ACCEPTS must survive the `ExecutionId`
        // encoding unchanged, and must not be the reserved sentinel.
        for raw in [0, 1, 0xFFFE] {
            let shard = ShardId::new(raw);
            assert!(is_encodable_shard(shard), "{raw} should be encodable");
            assert_eq!(ExecutionId::new_for_shard(shard).shard(), shard);
            assert!(!shard.is_unencoded(), "{raw} must not be the sentinel");
        }

        // Everything it REJECTS is unsafe to pin, for one of two reasons.
        //
        // (a) The value does not fit in the two shard bytes, so
        //     `ExecutionId::new_for_shard` silently truncates it (`& 0xFFFF`)
        //     and the id reads back as a DIFFERENT shard -- the exact
        //     cross-jurisdiction hazard the predicate exists to prevent.
        for raw in [-1, 0x1_0000, 0x1_0001] {
            let shard = ShardId::new(raw);
            assert!(!is_encodable_shard(shard), "{raw} should not be encodable");
            assert_ne!(
                ExecutionId::new_for_shard(shard).shard(),
                shard,
                "{raw} silently truncates through the ExecutionId encoding"
            );
        }

        // (b) `0xFFFE + 1 == 0xFFFF` DOES round-trip byte-for-byte -- it is not
        //     truncated -- but it is `ShardId::UNENCODED`, the reserved "no
        //     shard encoded, fall back to the default shard" sentinel. Pinning
        //     to it would mint ids the router resolves to the DEFAULT shard,
        //     so it is rejected for a different reason than (a).
        let sentinel = ShardId::new(0xFFFF);
        assert!(sentinel.is_unencoded());
        assert!(
            !is_encodable_shard(sentinel),
            "the reserved sentinel is never a pinnable shard"
        );
        assert_eq!(
            ExecutionId::new_for_shard(sentinel).shard(),
            sentinel,
            "the sentinel round-trips (it is reserved, not truncated)"
        );
    }

    /// Codex P1 (issue #697 review): two RAW keys that normalize to the same
    /// trimmed key with CONFLICTING shards would silently discard one mapping,
    /// and from an unordered source which one survives could vary across
    /// restarts — breaking the stable-mapping guarantee that is the whole point
    /// of a declared map, and potentially moving work between jurisdictions.
    #[test]
    #[should_panic(expected = "declared more than once")]
    fn conflicting_residency_keys_that_collide_after_trimming_panic() {
        let _ = router_with(&[0, 1, 2]).with_residency_map([
            ("eu".to_string(), ShardId::new(1)),
            (" eu ".to_string(), ShardId::new(2)),
        ]);
    }

    #[test]
    fn duplicate_residency_keys_agreeing_on_the_shard_are_accepted() {
        // Harmless: there is no ambiguity about which target survives.
        let router = router_with(&[0, 1]).with_residency_map([
            ("eu".to_string(), ShardId::new(1)),
            (" eu ".to_string(), ShardId::new(1)),
        ]);
        assert_eq!(router.residency_map().get("eu"), Some(&ShardId::new(1)));
        assert_eq!(router.residency_map().len(), 1);
    }

    #[test]
    fn residency_key_resolves_through_the_declared_map() {
        let router = residency_router();
        assert_eq!(
            router
                .resolve_placement(&ShardPlacement::residency_key("eu"), "wf", "id-1")
                .expect("eu is mapped"),
            ShardId::new(1)
        );
        assert_eq!(
            router
                .resolve_placement(&ShardPlacement::residency_key("us"), "wf", "id-1")
                .expect("us is mapped"),
            ShardId::new(2)
        );
    }

    #[test]
    fn unmapped_residency_key_is_rejected_never_hashed() {
        // A silent hash fallback is exactly the "hope the hash cooperates"
        // failure mode this feature exists to remove.
        let router = residency_router();
        let err = router
            .resolve_placement(&ShardPlacement::residency_key("apac"), "wf", "id-1")
            .expect_err("apac is not mapped");
        match err {
            ShardPlacementError::UnknownResidencyKey { ref key, ref known } => {
                assert_eq!(key, "apac");
                assert_eq!(known, &vec!["eu".to_string(), "us".to_string()]);
            }
            other => panic!("expected UnknownResidencyKey, got {other:?}"),
        }
        assert!(
            err.to_string().contains("apac"),
            "message must name the key: {err}"
        );
    }

    #[test]
    fn residency_key_on_a_router_with_no_map_is_rejected() {
        let router = router_with(&[0, 1, 2]);
        let err = router
            .resolve_placement(&ShardPlacement::residency_key("eu"), "wf", "id-1")
            .expect_err("no residency map is configured");
        assert!(
            matches!(err, ShardPlacementError::UnknownResidencyKey { .. }),
            "expected UnknownResidencyKey, got {err:?}"
        );
    }

    #[test]
    fn blank_residency_key_is_rejected() {
        let router = residency_router();
        for blank in ["", "   ", "\t"] {
            let err = router
                .resolve_placement(&ShardPlacement::residency_key(blank), "wf", "id-1")
                .expect_err("blank keys are not resolvable");
            assert!(
                matches!(err, ShardPlacementError::EmptyResidencyKey),
                "expected EmptyResidencyKey for {blank:?}, got {err:?}"
            );
        }
    }

    #[test]
    fn residency_key_resolution_is_stable_across_a_widening_of_the_shard_set() {
        // AC3 (the money test): a key that resolves to shard N must KEEP
        // resolving to N when shards are added. Rendezvous hashing cannot
        // promise this — a declared map can, by construction.
        let map = [
            ("eu".to_string(), ShardId::new(1)),
            ("us".to_string(), ShardId::new(2)),
        ];
        let before = ShardRouter::new(
            vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)],
            vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)],
            ShardId::new(0),
        )
        .with_residency_map(map.clone());

        // Widen both the readable and writable sets by three new shards.
        let after = ShardRouter::new(
            (0..6).map(ShardId::new).collect(),
            (0..6).map(ShardId::new).collect(),
            ShardId::new(0),
        )
        .with_residency_map(map);

        for key in ["eu", "us"] {
            let placement = ShardPlacement::residency_key(key);
            assert_eq!(
                before.resolve_placement(&placement, "wf", "id-1").unwrap(),
                after.resolve_placement(&placement, "wf", "id-1").unwrap(),
                "residency key {key} moved when shards were added"
            );
        }

        // Contrast: the rendezvous hash genuinely does move keys on widening,
        // which is precisely why residency cannot be built on it.
        let moved = (0..200)
            .map(|i| format!("id-{i}"))
            .filter(|wid| {
                before.pick_for_new_workflow("wf", wid) != after.pick_for_new_workflow("wf", wid)
            })
            .count();
        assert!(
            moved > 0,
            "the hash is expected to move some keys on widening; if it does not, \
             this test no longer proves the map buys anything"
        );
    }

    #[test]
    fn residency_conformance_n_workflows_across_k_keys() {
        // Success metric: 100% of starts under a residency key land on that
        // key's shard, independent of workflow name/id.
        let router = residency_router();
        let keys = [("eu", ShardId::new(1)), ("us", ShardId::new(2))];
        for (key, expected) in keys {
            for i in 0..100 {
                let resolved = router
                    .resolve_placement(
                        &ShardPlacement::residency_key(key),
                        &format!("wf-{}", i % 7),
                        &format!("id-{i}"),
                    )
                    .expect("mapped key resolves");
                assert_eq!(resolved, expected, "key {key} escaped its shard on run {i}");
            }
        }
    }

    #[test]
    fn residency_map_accessor_exposes_the_declared_mapping() {
        let router = residency_router();
        let map = router.residency_map();
        assert_eq!(map.get("eu"), Some(&ShardId::new(1)));
        assert_eq!(map.get("us"), Some(&ShardId::new(2)));
        assert_eq!(map.len(), 2);
        assert!(router_with(&[0]).residency_map().is_empty());
    }

    #[test]
    #[should_panic(expected = "residency key")]
    fn residency_map_targeting_an_unconfigured_shard_panics_at_construction() {
        // Misconfiguration must fail at boot, not at the first EU start.
        let _ = router_with(&[0, 1]).with_residency_map([("eu".to_string(), ShardId::new(7))]);
    }

    #[test]
    #[should_panic(expected = "residency key")]
    fn residency_map_with_a_blank_key_panics_at_construction() {
        let _ = router_with(&[0, 1]).with_residency_map([(String::new(), ShardId::new(1))]);
    }

    // Building a `Pool` never connects, so these need no live database.
    #[cfg(feature = "db")]
    fn test_pool() -> DbPool {
        let manager = diesel_async::pooled_connection::AsyncDieselConnectionManager::<
            diesel_async::AsyncPgConnection,
        >::new("postgres://unused/db");
        DbPool::builder(manager)
            .max_size(1)
            .build()
            .expect("pool builds without connecting")
    }

    // `from_map` sees a shared pool by object identity: two shard ids
    // backed by clones of one `Pool` must land in one group (issue #1266).
    #[cfg(feature = "db")]
    #[test]
    fn from_map_groups_cloned_pools_together() {
        let pool = test_pool();
        let mut pools = BTreeMap::new();
        pools.insert(ShardId::new(0), pool.clone());
        pools.insert(ShardId::new(1), pool);
        let sharded = ShardedDbPool::from_map(pools, ShardId::new(0));

        let groups = sharded.pool_groups();
        assert_eq!(
            groups.len(),
            1,
            "two clones of one pool must collapse to one group"
        );
        let mut shards = groups[0].1.clone();
        shards.sort();
        assert_eq!(shards, vec![ShardId::new(0), ShardId::new(1)]);
    }

    // `from_dsns` builds a fresh `Pool` per entry, even for one DSN reused
    // across two shard ids. Object identity alone would report these as
    // unrelated. The DSN itself must still group them (issue #1266).
    #[cfg(feature = "db")]
    #[test]
    fn from_dsns_groups_shards_sharing_one_dsn() {
        let sharded = ShardedDbPool::from_dsns(
            [
                (ShardId::new(0), "postgres://unused/shared".to_string()),
                (ShardId::new(1), "postgres://unused/shared".to_string()),
            ],
            ShardId::new(0),
            1,
        )
        .expect("pool builds without connecting");

        let groups = sharded.pool_groups();
        assert_eq!(
            groups.len(),
            1,
            "two shards on the same DSN must collapse to one group, even \
             though from_dsns built them as two separate Pool objects"
        );
        let mut shards = groups[0].1.clone();
        shards.sort();
        assert_eq!(shards, vec![ShardId::new(0), ShardId::new(1)]);
    }

    // Two shards on genuinely different DSNs must never be combined
    // (issue #1266).
    #[cfg(feature = "db")]
    #[test]
    fn from_dsns_keeps_distinct_dsns_separate() {
        let sharded = ShardedDbPool::from_dsns(
            [
                (ShardId::new(0), "postgres://unused/db-a".to_string()),
                (ShardId::new(1), "postgres://unused/db-b".to_string()),
            ],
            ShardId::new(0),
            1,
        )
        .expect("pool builds without connecting");

        let groups = sharded.pool_groups();
        assert_eq!(
            groups.len(),
            2,
            "two different DSNs must never collapse into one group"
        );
    }

    // Two DSNs can reach one physical database while written differently:
    // different credentials, and an explicit default port versus none
    // (issue #1266). Comparing the raw strings would miss this.
    #[cfg(feature = "db")]
    #[test]
    fn from_dsns_groups_equivalent_dsns_with_different_credentials_and_port() {
        let sharded = ShardedDbPool::from_dsns(
            [
                (
                    ShardId::new(0),
                    "postgres://alice:secret1@db.example/shared".to_string(),
                ),
                (
                    ShardId::new(1),
                    "postgres://bob:secret2@db.example:5432/shared".to_string(),
                ),
            ],
            ShardId::new(0),
            1,
        )
        .expect("pool builds without connecting");

        let groups = sharded.pool_groups();
        assert_eq!(
            groups.len(),
            1,
            "same host and database, differing only in credentials and an \
             explicit default port, must collapse to one group"
        );
        let mut shards = groups[0].1.clone();
        shards.sort();
        assert_eq!(shards, vec![ShardId::new(0), ShardId::new(1)]);
    }

    // Same host, different database name: never the same physical
    // database (issue #1266).
    #[cfg(feature = "db")]
    #[test]
    fn from_dsns_keeps_distinct_dbnames_on_the_same_host_separate() {
        let sharded = ShardedDbPool::from_dsns(
            [
                (ShardId::new(0), "postgres://db.example/db-a".to_string()),
                (ShardId::new(1), "postgres://db.example/db-b".to_string()),
            ],
            ShardId::new(0),
            1,
        )
        .expect("pool builds without connecting");

        let groups = sharded.pool_groups();
        assert_eq!(
            groups.len(),
            2,
            "two different database names on the same host must never \
             collapse into one group"
        );
    }

    // A `search_path` set through `?options=...` selects which schema
    // `harvest_audit_log` resolves to. Two DSNs differing only there
    // must never collapse (issue #1266).
    #[cfg(feature = "db")]
    #[test]
    fn from_dsns_keeps_distinct_search_path_options_separate() {
        let sharded = ShardedDbPool::from_dsns(
            [
                (
                    ShardId::new(0),
                    "postgres://db.example/shared?options=-c%20search_path%3Dschema_a".to_string(),
                ),
                (
                    ShardId::new(1),
                    "postgres://db.example/shared?options=-c%20search_path%3Dschema_b".to_string(),
                ),
            ],
            ShardId::new(0),
            1,
        )
        .expect("pool builds without connecting");

        let groups = sharded.pool_groups();
        assert_eq!(
            groups.len(),
            2,
            "different `options` can select a different schema, so these \
             must never collapse into one group"
        );
    }

    // `application_name` and `sslmode` never change which relation a
    // query resolves against. Two shards on the same database, differing
    // only in credentials and these tuning parameters, must still
    // collapse to one group (issue #1266).
    #[cfg(feature = "db")]
    #[test]
    fn from_dsns_ignores_connection_only_parameters() {
        let sharded = ShardedDbPool::from_dsns(
            [
                (
                    ShardId::new(0),
                    "postgres://alice@db.example/shared?application_name=web".to_string(),
                ),
                (
                    ShardId::new(1),
                    "postgres://bob@db.example/shared?application_name=worker&sslmode=require"
                        .to_string(),
                ),
            ],
            ShardId::new(0),
            1,
        )
        .expect("pool builds without connecting");

        let groups = sharded.pool_groups();
        assert_eq!(
            groups.len(),
            1,
            "application_name and sslmode never affect relation \
             resolution, so these must collapse to one group"
        );
    }

    // A Unix-socket DSN carries the real endpoint in a `host` query
    // parameter, not the URI authority. Two such DSNs for different
    // sockets must never collapse (issue #1266).
    #[cfg(feature = "db")]
    #[test]
    fn from_dsns_keeps_distinct_unix_socket_hosts_separate() {
        let sharded = ShardedDbPool::from_dsns(
            [
                (
                    ShardId::new(0),
                    "postgresql:///harvest?host=%2Frun%2Fpg-a".to_string(),
                ),
                (
                    ShardId::new(1),
                    "postgresql:///harvest?host=%2Frun%2Fpg-b".to_string(),
                ),
            ],
            ShardId::new(0),
            1,
        )
        .expect("pool builds without connecting");

        let groups = sharded.pool_groups();
        assert_eq!(
            groups.len(),
            2,
            "different `host` query parameters name different sockets, \
             so these must never collapse into one group"
        );
    }

    // Two Unix-socket DSNs for the *same* socket, named through `host`,
    // must still collapse to one group (issue #1266).
    #[cfg(feature = "db")]
    #[test]
    fn from_dsns_groups_shards_sharing_one_unix_socket_host() {
        let sharded = ShardedDbPool::from_dsns(
            [
                (
                    ShardId::new(0),
                    "postgresql:///harvest?host=%2Frun%2Fpg-a".to_string(),
                ),
                (
                    ShardId::new(1),
                    "postgresql:///harvest?host=%2Frun%2Fpg-a".to_string(),
                ),
            ],
            ShardId::new(0),
            1,
        )
        .expect("pool builds without connecting");

        let groups = sharded.pool_groups();
        assert_eq!(
            groups.len(),
            1,
            "the same `host` query parameter names the same socket, so \
             these must collapse into one group"
        );
    }

    // Unix filesystem paths are case-sensitive: `/run/PG-A` and
    // `/run/pg-a` name different sockets (issue #1266).
    #[cfg(feature = "db")]
    #[test]
    fn from_dsns_keeps_distinctly_cased_socket_paths_separate() {
        let sharded = ShardedDbPool::from_dsns(
            [
                (
                    ShardId::new(0),
                    "postgresql:///harvest?host=%2Frun%2FPG-A".to_string(),
                ),
                (
                    ShardId::new(1),
                    "postgresql:///harvest?host=%2Frun%2Fpg-a".to_string(),
                ),
            ],
            ShardId::new(0),
            1,
        )
        .expect("pool builds without connecting");

        let groups = sharded.pool_groups();
        assert_eq!(
            groups.len(),
            2,
            "a socket path's case is significant, so these must never \
             collapse into one group"
        );
    }

    // A DNS hostname stays case-insensitive even when it arrives through
    // `host=`, unlike a socket path (issue #1266).
    #[cfg(feature = "db")]
    #[test]
    fn from_dsns_groups_shards_sharing_one_hostname_regardless_of_case() {
        let sharded = ShardedDbPool::from_dsns(
            [
                (
                    ShardId::new(0),
                    "postgresql:///harvest?host=DB.EXAMPLE".to_string(),
                ),
                (
                    ShardId::new(1),
                    "postgresql:///harvest?host=db.example".to_string(),
                ),
            ],
            ShardId::new(0),
            1,
        )
        .expect("pool builds without connecting");

        let groups = sharded.pool_groups();
        assert_eq!(
            groups.len(),
            1,
            "a DNS hostname is case-insensitive, so these must collapse \
             into one group"
        );
    }

    // libpq defaults an omitted dbname to the connecting username, so
    // two users with no explicit dbname reach different databases
    // (issue #1266).
    #[cfg(feature = "db")]
    #[test]
    fn from_dsns_keeps_distinct_users_with_no_explicit_dbname_separate() {
        let sharded = ShardedDbPool::from_dsns(
            [
                (ShardId::new(0), "postgres://alice@db.example".to_string()),
                (ShardId::new(1), "postgres://bob@db.example".to_string()),
            ],
            ShardId::new(0),
            1,
        )
        .expect("pool builds without connecting");

        let groups = sharded.pool_groups();
        assert_eq!(
            groups.len(),
            2,
            "an omitted dbname defaults to the username, so two \
             different users must never collapse into one group"
        );
    }

    // The username-as-dbname fallback applies only when no path is
    // given. An explicit, shared dbname still groups regardless of
    // username (issue #1266).
    #[cfg(feature = "db")]
    #[test]
    fn from_dsns_ignores_username_when_dbname_is_explicit() {
        let sharded = ShardedDbPool::from_dsns(
            [
                (
                    ShardId::new(0),
                    "postgres://alice@db.example/shared".to_string(),
                ),
                (
                    ShardId::new(1),
                    "postgres://bob@db.example/shared".to_string(),
                ),
            ],
            ShardId::new(0),
            1,
        )
        .expect("pool builds without connecting");

        let groups = sharded.pool_groups();
        assert_eq!(
            groups.len(),
            1,
            "an explicit dbname is not defaulted from the username, so \
             these must still collapse into one group"
        );
    }

    // `url::Url` and `tokio_postgres::Config` disagree on percent-decoding:
    // `url::Url::path()` returns the raw, still-encoded path, but the real
    // connector decodes it. Two spellings of one database name must
    // collapse (issue #1266).
    #[cfg(feature = "db")]
    #[test]
    fn from_dsns_groups_percent_encoded_and_plain_dbname_spellings() {
        let sharded = ShardedDbPool::from_dsns(
            [
                (ShardId::new(0), "postgres://db.example/harvest".to_string()),
                (
                    ShardId::new(1),
                    "postgres://db.example/%68arvest".to_string(),
                ),
            ],
            ShardId::new(0),
            1,
        )
        .expect("pool builds without connecting");

        let groups = sharded.pool_groups();
        assert_eq!(
            groups.len(),
            1,
            "`%68` decodes to `h`, so both DSNs name the same database \
             and must collapse into one group"
        );
    }

    // `options` can carry any `-c name=value` GUC, not only
    // `search_path`. Two DSNs differing only in an unrelated one must
    // still collapse (issue #1266).
    #[cfg(feature = "db")]
    #[test]
    fn from_dsns_ignores_non_search_path_options_flags() {
        let sharded = ShardedDbPool::from_dsns(
            [
                (
                    ShardId::new(0),
                    "postgres://db.example/shared?options=-c%20application_name%3Dweb".to_string(),
                ),
                (
                    ShardId::new(1),
                    "postgres://db.example/shared?options=-c%20application_name%3Dworker"
                        .to_string(),
                ),
            ],
            ShardId::new(0),
            1,
        )
        .expect("pool builds without connecting");

        let groups = sharded.pool_groups();
        assert_eq!(
            groups.len(),
            1,
            "`application_name` set through `options` never affects \
             relation resolution, so these must collapse into one group"
        );
    }

    // `hostaddr` pins the actual TCP destination. Two DSNs sharing one
    // must collapse regardless of how each spells the hostname (issue
    // #1266).
    #[cfg(feature = "db")]
    #[test]
    fn from_dsns_groups_shards_sharing_one_hostaddr_regardless_of_hostname() {
        let sharded = ShardedDbPool::from_dsns(
            [
                (
                    ShardId::new(0),
                    "postgres://alias-a/shared?hostaddr=10.0.0.5".to_string(),
                ),
                (
                    ShardId::new(1),
                    "postgres://alias-b/shared?hostaddr=10.0.0.5".to_string(),
                ),
            ],
            ShardId::new(0),
            1,
        )
        .expect("pool builds without connecting");

        let groups = sharded.pool_groups();
        assert_eq!(
            groups.len(),
            1,
            "a shared `hostaddr` names one physical destination, so these \
             must collapse into one group even though the hostnames differ"
        );
    }

    #[cfg(feature = "db")]
    #[test]
    fn from_dsns_groups_a_numeric_host_with_a_differing_explicit_hostaddr() {
        let sharded = ShardedDbPool::from_dsns(
            [
                (
                    ShardId::new(0),
                    "postgres://10.0.0.1/shared?hostaddr=10.0.0.2".to_string(),
                ),
                (
                    ShardId::new(1),
                    "postgres://alias/shared?hostaddr=10.0.0.2".to_string(),
                ),
            ],
            ShardId::new(0),
            1,
        )
        .expect("pool builds without connecting");

        let groups = sharded.pool_groups();
        assert_eq!(
            groups.len(),
            1,
            "an explicit hostaddr alone pins the TCP destination, so a \
             numeric host text must not also be folded into the address \
             set -- both DSNs pin the identical server and must collapse"
        );
    }

    // The compact `-csearch_path=value` spelling has no space before
    // `-c`. It is already used elsewhere in this codebase. It must be
    // recognized the same as the spaced form (issue #1266).
    #[cfg(feature = "db")]
    #[test]
    fn from_dsns_recognizes_the_compact_search_path_options_spelling() {
        let sharded = ShardedDbPool::from_dsns(
            [
                (
                    ShardId::new(0),
                    "postgres://db.example/shared?options=-csearch_path%3Dschema_a".to_string(),
                ),
                (
                    ShardId::new(1),
                    "postgres://db.example/shared?options=-csearch_path%3Dschema_b".to_string(),
                ),
            ],
            ShardId::new(0),
            1,
        )
        .expect("pool builds without connecting");

        let groups = sharded.pool_groups();
        assert_eq!(
            groups.len(),
            2,
            "the compact `-csearch_path=` spelling must select a schema \
             just as the spaced form does, so these must never collapse"
        );
    }

    #[cfg(feature = "db")]
    #[test]
    fn from_dsns_recognizes_the_long_form_search_path_options_spelling() {
        let sharded = ShardedDbPool::from_dsns(
            [
                (
                    ShardId::new(0),
                    "postgres://db.example/shared?options=--search_path%3Dschema_a".to_string(),
                ),
                (
                    ShardId::new(1),
                    "postgres://db.example/shared?options=--search_path%3Dschema_b".to_string(),
                ),
            ],
            ShardId::new(0),
            1,
        )
        .expect("pool builds without connecting");

        let groups = sharded.pool_groups();
        assert_eq!(
            groups.len(),
            2,
            "the long-form `--search_path=` spelling must select a schema \
             just as `-c search_path=` does, so these must never collapse"
        );
    }

    #[cfg(feature = "db")]
    #[test]
    fn from_dsns_recognizes_a_hyphenated_long_form_search_path_name() {
        let sharded = ShardedDbPool::from_dsns(
            [
                (
                    ShardId::new(0),
                    "postgres://db.example/shared?options=--search-path%3Dschema_a".to_string(),
                ),
                (
                    ShardId::new(1),
                    "postgres://db.example/shared?options=--search_path%3Dschema_a".to_string(),
                ),
            ],
            ShardId::new(0),
            1,
        )
        .expect("pool builds without connecting");

        let groups = sharded.pool_groups();
        assert_eq!(
            groups.len(),
            1,
            "PostgreSQL normalizes a hyphen to an underscore in a \
             long-form GUC name, so --search-path= and --search_path= \
             select the same schema and must collapse"
        );
    }

    #[cfg(feature = "db")]
    #[test]
    fn from_dsns_groups_unquoted_names_differing_only_past_the_identifier_length_limit() {
        let prefix = "a".repeat(63);
        let name_a = format!("{prefix}x");
        let name_b = format!("{prefix}y");
        let sharded = ShardedDbPool::from_dsns(
            [
                (
                    ShardId::new(0),
                    format!("postgres://db.example/shared?options=-c%20search_path%3D{name_a}"),
                ),
                (
                    ShardId::new(1),
                    format!("postgres://db.example/shared?options=-c%20search_path%3D{name_b}"),
                ),
            ],
            ShardId::new(0),
            1,
        )
        .expect("pool builds without connecting");

        let groups = sharded.pool_groups();
        assert_eq!(
            groups.len(),
            1,
            "PostgreSQL silently truncates an unquoted identifier past \
             its 63-byte limit, so two names sharing that many bytes \
             store as the identical name and must collapse"
        );
    }

    #[cfg(feature = "db")]
    #[test]
    fn from_dsns_groups_dsns_whose_search_path_differs_only_by_an_escaped_space() {
        let sharded = ShardedDbPool::from_dsns(
            [
                (
                    ShardId::new(0),
                    "postgres://db.example/shared?options=-c%20search_path%3Dtenant%2Cpublic"
                        .to_string(),
                ),
                (
                    ShardId::new(1),
                    "postgres://db.example/shared?options=-c%20search_path%3Dtenant%2C%5C%20public"
                        .to_string(),
                ),
            ],
            ShardId::new(0),
            1,
        )
        .expect("pool builds without connecting");

        let groups = sharded.pool_groups();
        assert_eq!(
            groups.len(),
            1,
            "an escaped space in one alias's search_path value must not \
             stop it from collapsing with the other: PostgreSQL treats \
             `tenant,public` and `tenant, public` as the same schema list"
        );
    }

    #[cfg(feature = "db")]
    #[test]
    fn from_dsns_groups_search_path_identifiers_that_differ_only_by_case() {
        let sharded = ShardedDbPool::from_dsns(
            [
                (
                    ShardId::new(0),
                    "postgres://db.example/shared?options=-c%20search_path%3DPUBLIC".to_string(),
                ),
                (
                    ShardId::new(1),
                    "postgres://db.example/shared?options=-c%20search_path%3Dpublic".to_string(),
                ),
            ],
            ShardId::new(0),
            1,
        )
        .expect("pool builds without connecting");

        let groups = sharded.pool_groups();
        assert_eq!(
            groups.len(),
            1,
            "PostgreSQL folds an unquoted identifier to lowercase, so \
             `PUBLIC` and `public` name the same schema and must collapse"
        );
    }

    #[cfg(feature = "db")]
    #[test]
    fn from_dsns_keeps_a_quoted_comma_containing_schema_distinct_from_two_plain_ones() {
        let sharded = ShardedDbPool::from_dsns(
            [
                (
                    ShardId::new(0),
                    "postgres://db.example/shared?options=-c%20search_path%3D%22tenant%2C%20one%22"
                        .to_string(),
                ),
                (
                    ShardId::new(1),
                    "postgres://db.example/shared?options=-c%20search_path%3Dtenant%2Cone"
                        .to_string(),
                ),
            ],
            ShardId::new(0),
            1,
        )
        .expect("pool builds without connecting");

        let groups = sharded.pool_groups();
        assert_eq!(
            groups.len(),
            2,
            "a quoted schema named literally `tenant, one` is one \
             identifier, distinct from the two unquoted identifiers \
             `tenant` and `one`, and must never collapse with them"
        );
    }

    #[cfg(feature = "db")]
    #[test]
    fn from_dsns_keeps_a_quoted_comma_containing_schema_distinct_with_no_space_to_hide_behind() {
        let sharded = ShardedDbPool::from_dsns(
            [
                (
                    ShardId::new(0),
                    "postgres://db.example/shared?options=-c%20search_path%3D%22tenant%2Cone%22"
                        .to_string(),
                ),
                (
                    ShardId::new(1),
                    "postgres://db.example/shared?options=-c%20search_path%3Dtenant%2Cone"
                        .to_string(),
                ),
            ],
            ShardId::new(0),
            1,
        )
        .expect("pool builds without connecting");

        let groups = sharded.pool_groups();
        assert_eq!(
            groups.len(),
            2,
            "the one quoted item `tenant,one` and the two unquoted items \
             `tenant` and `one` must not join to the same string just \
             because a bare comma also separates joined items -- an \
             unescaped join collapses both to `tenant,one`"
        );
    }

    #[cfg(feature = "db")]
    #[test]
    fn from_dsns_groups_an_implicit_pg_catalog_with_an_explicit_leading_one() {
        let sharded = ShardedDbPool::from_dsns(
            [
                (
                    ShardId::new(0),
                    "postgres://db.example/shared?options=-c%20search_path%3Dpublic".to_string(),
                ),
                (
                    ShardId::new(1),
                    "postgres://db.example/shared?options=-c%20search_path%3Dpg_catalog%2Cpublic"
                        .to_string(),
                ),
            ],
            ShardId::new(0),
            1,
        )
        .expect("pool builds without connecting");

        let groups = sharded.pool_groups();
        assert_eq!(
            groups.len(),
            1,
            "PostgreSQL always searches pg_catalog first when it is \
             omitted, so `public` and `pg_catalog,public` resolve an \
             unqualified relation the same way and must collapse"
        );
    }

    #[cfg(feature = "db")]
    #[test]
    fn from_dsns_keeps_an_explicit_trailing_pg_catalog_distinct_from_the_implicit_leading_one() {
        let sharded = ShardedDbPool::from_dsns(
            [
                (
                    ShardId::new(0),
                    "postgres://db.example/shared?options=-c%20search_path%3Dpublic".to_string(),
                ),
                (
                    ShardId::new(1),
                    "postgres://db.example/shared?options=-c%20search_path%3Dpublic%2Cpg_catalog"
                        .to_string(),
                ),
            ],
            ShardId::new(0),
            1,
        )
        .expect("pool builds without connecting");

        let groups = sharded.pool_groups();
        assert_eq!(
            groups.len(),
            2,
            "omitting pg_catalog always searches it first, but naming it \
             explicitly last searches it last -- a genuinely different \
             resolution order that must never collapse with the implicit one"
        );
    }

    #[cfg(feature = "db")]
    #[test]
    fn from_dsns_groups_a_search_path_with_a_repeated_name() {
        let sharded = ShardedDbPool::from_dsns(
            [
                (
                    ShardId::new(0),
                    "postgres://db.example/shared?options=-c%20search_path%3Dpublic".to_string(),
                ),
                (
                    ShardId::new(1),
                    "postgres://db.example/shared?options=-c%20search_path%3Dpublic%2Cpublic"
                        .to_string(),
                ),
            ],
            ShardId::new(0),
            1,
        )
        .expect("pool builds without connecting");

        let groups = sharded.pool_groups();
        assert_eq!(
            groups.len(),
            1,
            "public and public,public search the identical schema in \
             the identical order, so a repeated name must not stop \
             these from collapsing into one group"
        );
    }

    #[cfg(feature = "db")]
    #[test]
    fn from_dsns_groups_dsns_whose_search_path_differs_only_by_an_escaped_tab() {
        let sharded = ShardedDbPool::from_dsns(
            [
                (
                    ShardId::new(0),
                    "postgres://db.example/shared?options=-c%20search_path%3Dtenant%2Cpublic"
                        .to_string(),
                ),
                (
                    ShardId::new(1),
                    "postgres://db.example/shared?options=-c%20search_path%3Dtenant%2C%5C%09public"
                        .to_string(),
                ),
            ],
            ShardId::new(0),
            1,
        )
        .expect("pool builds without connecting");

        let groups = sharded.pool_groups();
        assert_eq!(
            groups.len(),
            1,
            "an escaped tab in one alias's search_path value must not \
             stop it from collapsing with the other, just as an escaped \
             space does not"
        );
    }

    #[cfg(feature = "db")]
    #[test]
    fn from_dsns_recognizes_an_uppercase_search_path_guc_name() {
        let sharded = ShardedDbPool::from_dsns(
            [
                (
                    ShardId::new(0),
                    "postgres://db.example/shared?options=-c%20SEARCH_PATH%3Dshared".to_string(),
                ),
                (
                    ShardId::new(1),
                    "postgres://db.example/shared?options=-c%20search_path%3Dshared".to_string(),
                ),
            ],
            ShardId::new(0),
            1,
        )
        .expect("pool builds without connecting");

        let groups = sharded.pool_groups();
        assert_eq!(
            groups.len(),
            1,
            "PostgreSQL parameter names are case-insensitive, so \
             SEARCH_PATH=shared sets the identical GUC as \
             search_path=shared and must select the same schema"
        );
    }

    #[cfg(feature = "db")]
    #[test]
    fn from_dsns_groups_an_implicit_pg_temp_with_an_explicit_leading_one() {
        let sharded = ShardedDbPool::from_dsns(
            [
                (
                    ShardId::new(0),
                    "postgres://db.example/shared?options=-c%20search_path%3Dpublic".to_string(),
                ),
                (
                    ShardId::new(1),
                    "postgres://db.example/shared?options=-c%20search_path%3Dpg_temp%2Cpg_catalog%2Cpublic"
                        .to_string(),
                ),
            ],
            ShardId::new(0),
            1,
        )
        .expect("pool builds without connecting");

        let groups = sharded.pool_groups();
        assert_eq!(
            groups.len(),
            1,
            "PostgreSQL always searches the session's temporary schema \
             before pg_catalog when it is omitted, so public and \
             pg_temp,pg_catalog,public resolve the same way and must \
             collapse"
        );
    }

    #[cfg(feature = "db")]
    #[test]
    fn from_dsns_keeps_an_explicit_trailing_pg_temp_distinct_from_the_implicit_leading_one() {
        let sharded = ShardedDbPool::from_dsns(
            [
                (
                    ShardId::new(0),
                    "postgres://db.example/shared?options=-c%20search_path%3Dpublic".to_string(),
                ),
                (
                    ShardId::new(1),
                    "postgres://db.example/shared?options=-c%20search_path%3Dpublic%2Cpg_temp"
                        .to_string(),
                ),
            ],
            ShardId::new(0),
            1,
        )
        .expect("pool builds without connecting");

        let groups = sharded.pool_groups();
        assert_eq!(
            groups.len(),
            2,
            "omitting pg_temp always searches it first, but naming it \
             explicitly last searches it last -- a genuinely different \
             resolution order that must never collapse with the implicit one"
        );
    }

    #[cfg(feature = "db")]
    #[test]
    fn from_dsns_groups_dsns_whose_search_path_differs_only_by_an_escaped_comma() {
        let sharded = ShardedDbPool::from_dsns(
            [
                (
                    ShardId::new(0),
                    "postgres://db.example/shared?options=-c%20search_path%3Dpublic".to_string(),
                ),
                (
                    ShardId::new(1),
                    "postgres://db.example/shared?options=-c%20search_path%3Dpublic%5C%2Cpublic"
                        .to_string(),
                ),
            ],
            ShardId::new(0),
            1,
        )
        .expect("pool builds without connecting");

        let groups = sharded.pool_groups();
        assert_eq!(
            groups.len(),
            1,
            "pg_split_opts removes a backslash before any character, so \
             public\\,public reaches the server the same as \
             public,public, and both must select the same schema"
        );
    }

    // libpq applies a repeated `-c search_path=...` as a `SET`, in
    // order, so only the last one has any effect. Two DSNs with the
    // same effective `search_path` must collapse even when one carries
    // an earlier, overridden value the other never mentions at all
    // (issue #1266).
    #[cfg(feature = "db")]
    #[test]
    fn from_dsns_groups_dsns_with_the_same_effective_search_path() {
        let sharded = ShardedDbPool::from_dsns(
            [
                (
                    ShardId::new(0),
                    "postgres://db.example/shared?options=-c%20search_path%3Dold%20-c%20search_path%3Dshared"
                        .to_string(),
                ),
                (
                    ShardId::new(1),
                    "postgres://db.example/shared?options=-c%20search_path%3Dshared".to_string(),
                ),
            ],
            ShardId::new(0),
            1,
        )
        .expect("pool builds without connecting");

        let groups = sharded.pool_groups();
        assert_eq!(
            groups.len(),
            1,
            "only the last `-c search_path=` takes effect, so an \
             overridden earlier value must not stop these from \
             collapsing into one group"
        );
    }
}

// ── Cross-shard child placement (issue #956) ─────────────────────────────────

/// Where a **child** workflow should be placed (issue #956).
///
/// Children are pinned to the parent's shard by default and that default is
/// permanent: [`ChildPlacement::ParentShard`] is byte-for-byte today's
/// behaviour, resolves without consulting the router at all, and is what every
/// existing `spawn_child_workflow*` call gets. The other variants are an
/// **opt-in, per-spawn** policy for the child-heavy orchestrator workloads that
/// otherwise concentrate a whole fan-out's storage and dispatch load on one
/// database.
///
/// # Choosing a variant
///
/// | Variant | Use when |
/// |---|---|
/// | [`ParentShard`](ChildPlacement::ParentShard) | Anything without a fan-out scale problem. The default. |
/// | [`Distributed`](ChildPlacement::Distributed) | A large fan-out whose write load should spread across `writable_shards`. |
/// | [`Shard`](ChildPlacement::Shard) | Ops tooling that already knows the shard number. |
/// | [`ResidencyKey`](ChildPlacement::ResidencyKey) | The child has a jurisdiction of its own, distinct from the parent's. |
///
/// # Residency interaction (issue #697)
///
/// Residency is transitive across the workflow tree *under the default*: a
/// child of a pinned parent stays on the parent's shard. Opting a child into
/// `Distributed` deliberately breaks that transitivity for that child, which is
/// exactly what a residency-bound tree must not do. Use
/// [`ChildPlacement::ResidencyKey`] when the child has its own declared
/// jurisdiction, and leave residency-bound trees on the default.
///
/// ```rust
/// use autumn_harvest::shard::ChildPlacement;
///
/// assert_eq!(ChildPlacement::default(), ChildPlacement::ParentShard);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum ChildPlacement {
    /// Pin the child to the parent's shard. **The default, permanently.**
    ///
    /// Resolves without touching [`ShardRouter`], so a deployment that never
    /// installs a router (every single-shard deployment, every existing test)
    /// is unaffected.
    #[default]
    ParentShard,
    /// Spread children across `writable_shards` by rendezvous hash.
    ///
    /// Uses the same [`ShardRouter::pick_for_new_workflow`] a top-level start
    /// uses, keyed on a deterministic per-parent placement key (see
    /// [`child_placement_key`]) so a decision cycle retried after a crash
    /// re-derives the identical shard.
    Distributed,
    /// Pin the child to a concrete shard.
    ///
    /// Rejected — never silently re-hashed — unless the shard is both readable
    /// and writable, exactly like [`ShardPlacement::Shard`].
    Shard(ShardId),
    /// Pin the child via an operator-declared residency key.
    ///
    /// Resolved through [`ShardRouter::with_residency_map`]; an undeclared key
    /// is an error, never a hash fallback.
    ResidencyKey(String),
}

impl ChildPlacement {
    /// Is this the default (parent-pinned) placement?
    ///
    /// Callers use this to take the untouched same-shard code path without
    /// pattern-matching on a `#[non_exhaustive]` enum.
    #[must_use]
    pub const fn is_parent_shard(&self) -> bool {
        matches!(self, Self::ParentShard)
    }
}

/// The deterministic rendezvous key for the `seq`-th child of `parent`.
///
/// Restart stability is the whole point: a top-level start hashes a caller-
/// supplied `workflow_id`, which is stable by construction, but a child's
/// `ExecutionId` is minted fresh on every dispatch. Hashing the *minted id*
/// would re-roll the shard whenever a decision cycle is retried after a crash.
/// Hashing `(parent, seq)` instead re-derives the identical shard, giving
/// children the same restart-stability contract top-level starts have.
///
/// ```rust
/// use autumn_harvest::shard::child_placement_key;
/// use autumn_harvest::types::{ExecutionId, ShardId};
///
/// let parent = ExecutionId::new_for_shard(ShardId::new(0));
/// assert_eq!(child_placement_key(parent, 3), child_placement_key(parent, 3));
/// assert_ne!(child_placement_key(parent, 3), child_placement_key(parent, 4));
/// ```
#[must_use]
pub fn child_placement_key(parent: ExecutionId, seq: u32) -> String {
    format!("{parent}#{seq}")
}

/// Resolve a [`ChildPlacement`] to the shard the child must be created on.
///
/// Pure: the router is passed in rather than read from
/// [`GLOBAL_SHARD_ROUTER`], so every branch is unit-testable without a process
/// global. [`ChildPlacement::ParentShard`] short-circuits before `router` is
/// even inspected, which is why `router` is an `Option` — the default path must
/// work in a deployment that never installs one.
///
/// # Where a *drained* shard is rejected — and why not here
///
/// This function rejects only **static misconfiguration**: no router installed,
/// an unknown shard, an undeclared or blank residency key. Retrying any of those
/// never helps, so surfacing them to the workflow author as a terminal error is
/// right.
///
/// A shard that is merely **drained** (readable but out of `writable_shards`)
/// is a *transient* operational state, and it is deliberately **not** rejected
/// here. This function runs inside the workflow handler, and the handler ABI
/// erases the error type — a workflow's `?` turns any `HarvestError` into a
/// `String`, which the executor maps to a terminal `WorkflowOutcome::Failed`.
/// The worker's typed `ShardUnavailable` recovery would never see it, so
/// rejecting a drain here would *permanently* fail every workflow that spawned a
/// placed child during a maintenance window — the opposite of the documented
/// bounded retry.
///
/// Writability is therefore enforced one layer down, by
/// `cross_shard_child::preflight_target_shard`, which runs inside the parent's
/// **persist** transaction where `ShardUnavailable` is recognised and requeued
/// with a bounded backoff. Nothing is recorded in the meantime: the resolved id
/// only reaches history if that persist succeeds, and the persist is exactly
/// what rejects it.
///
/// This is not a fallback. The shard this returns is the shard the caller asked
/// for (or the rendezvous pick); it is never quietly swapped for the parent's
/// shard or the default shard, which is the failure mode AC8 exists to remove.
///
/// # Errors
///
/// Returns [`HarvestError::Config`] when a non-default placement is requested
/// but no router is installed, or when the router rejects the pin as unknown or
/// undeclared.
pub fn resolve_child_placement(
    router: Option<&ShardRouter>,
    placement: &ChildPlacement,
    parent_shard: ShardId,
    workflow_name: &str,
    placement_key: &str,
) -> crate::error::HarvestResult<ShardId> {
    if placement.is_parent_shard() {
        return Ok(parent_shard);
    }

    let Some(router) = router else {
        return Err(crate::error::HarvestError::Config(format!(
            "child placement {placement:?} requires an installed ShardRouter; \
             refusing to fall back to the parent's shard"
        )));
    };

    let requested = match placement {
        ChildPlacement::ParentShard => unreachable!("short-circuited above"),
        ChildPlacement::Distributed => {
            // `pick_for_new_workflow` falls back to `default_shard` when NOTHING
            // is writable. That degenerate case is deliberately allowed to land
            // the child, and it is deliberately traced.
            //
            // Allowed, because the alternatives are worse. Failing the spawn
            // would be terminal (the handler ABI erases the error type — see the
            // note above). Requeuing it would *deadlock the drain itself*: a
            // drained shard is one that should let its in-flight work finish,
            // and a parent cannot finish while the children it is awaiting are
            // refused. And "the parent's shard" is not an arbitrary consolation
            // prize here — with zero writable shards it is where an *unplaced*
            // child would go, and where the parent already lives, so no
            // cross-shard placement contract is broken: none was made.
            //
            // MUST be `parent_shard`, never `default_shard()` (issue #1263 item
            // 15). The two coincide only when the parent happens to already live
            // on the default shard. For any other in-flight parent,
            // `default_shard()` would encode the child onto a DIFFERENT shard
            // than the parent. The persist path then classifies that child
            // remote, and `preflight_target_shard` rejects it — an empty
            // writable set makes every shard, including the default one,
            // unwritable. That is the exact drain deadlock this fallback exists
            // to prevent. Returning the parent's own shard makes the child
            // genuinely LOCAL, so it can never reach that remote-classifying
            // check at all.
            //
            // Traced, because AC8's requirement is that a fallback never happens
            // "without trace". A `warn!` naming the workflow and the shard is
            // that trace; the operator draining the fleet can see exactly which
            // placed spawns degenerated while the window was open.
            if router.writable_shards().is_empty() {
                tracing::warn!(
                    workflow_name,
                    shard = parent_shard.as_i32(),
                    "no shard is currently writable; a Distributed child placement \
                     stays on the parent's own shard for the duration of the drain"
                );
                return Ok(parent_shard);
            }
            return Ok(router.pick_for_new_workflow(workflow_name, placement_key));
        }
        ChildPlacement::Shard(shard) => ShardPlacement::Shard(*shard),
        ChildPlacement::ResidencyKey(key) => ShardPlacement::ResidencyKey(key.clone()),
    };

    match router.resolve_placement(&requested, workflow_name, placement_key) {
        Ok(shard) => Ok(shard),
        // A drained shard is a *transient* operational state — a rebalance, a
        // maintenance window. Resolve it to the shard the caller named and let
        // the persist-boundary preflight reject it retryably; see this
        // function's "Where a drained shard is rejected" note. Everything else
        // is static misconfiguration and stays a terminal `Config` error.
        Err(ShardPlacementError::ShardNotWritable { requested, .. }) => Ok(requested),
        Err(other) => Err(crate::error::HarvestError::Config(other.to_string())),
    }
}

/// Lifecycle status of one cross-shard child outbox row (issue #956).
///
/// Persisted as the row's `status` TEXT column. Deliberately a two-state
/// machine: everything after `Started` is decided by *observed* facts (the
/// child's state on the target shard, the parent's state here), never by a
/// status the relay has to remember to advance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CrossShardChildStatus {
    /// The parent committed the spawn; the child does not exist on the target
    /// shard yet.
    PendingStart,
    /// The child row exists on the target shard.
    Started,
}

impl CrossShardChildStatus {
    /// The database representation of this status.
    #[must_use]
    pub const fn as_db_str(self) -> &'static str {
        match self {
            Self::PendingStart => "PENDING_START",
            Self::Started => "STARTED",
        }
    }

    /// Parse a database `status` value, or `None` when it is not recognised.
    #[must_use]
    pub fn from_db(raw: &str) -> Option<Self> {
        match raw {
            "PENDING_START" => Some(Self::PendingStart),
            "STARTED" => Some(Self::Started),
            _ => None,
        }
    }
}

/// Everything the relay knows about one cross-shard child this tick.
///
/// Assembled from the outbox row on the parent's shard plus one batched read of
/// the child's state on the target shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrossShardChildObservation<'a> {
    /// Where the row is in its lifecycle.
    pub status: CrossShardChildStatus,
    /// A parent-side cancel that has not been delivered to the target shard yet.
    pub cancel_requested: bool,
    /// `None` for an **awaited** child; `Some(policy)` for a **detached** one.
    pub parent_close_policy: Option<crate::types::ParentClosePolicy>,
    /// Whether the parent has reached a terminal state on this shard, or `None`
    /// when this sweep could not read it.
    ///
    /// The three-state shape is load-bearing. A failed batch read must NOT be
    /// collapsed into "terminal": `Retire` deletes the row outright, with no
    /// second look at the parent, so one transient read error would permanently
    /// lose the wake of every awaited cross-shard child in the batch — and would
    /// cascade-cancel detached children whose parents are alive and well.
    /// `None` means "not known to be closed", and no destructive action is
    /// decided from it.
    ///
    /// A `Some(true)` from a *successful* read whose result simply lacks the
    /// parent's id is correct: the row is genuinely gone (retention collection,
    /// erase), and there is nobody left to wake.
    pub parent_terminal: Option<bool>,
    /// The child's `state` column on the target shard, or `None` when the child
    /// row is not visible yet (not created, or the shard was unreadable).
    pub child_state: Option<&'a str>,
}

/// What the relay should do with one cross-shard child outbox row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrossShardChildAction {
    /// Create the child execution on the target shard, then mark the row
    /// [`CrossShardChildStatus::Started`].
    StartChild,
    /// Deliver an idempotent cancel to the child on the target shard.
    CancelChild,
    /// Append the child's terminal event to the parent's history, wake the
    /// parent, and drop the row — all in one transaction on the parent's shard.
    DeliverTerminal,
    /// Apply the parent-close policy to a detached child on the target shard.
    ApplyCloseCascade,
    /// Nothing to do this tick.
    Wait,
    /// The row is owed nothing more; drop it.
    Retire,
}

/// Decide what one cross-shard child outbox row needs, from observed facts only.
///
/// Factored out of the scanner so every branch is exhaustively unit-testable
/// with no database. The ordering is load-bearing:
///
/// 1. A row that has not started yet always starts first — a cancel or a closed
///    parent still needs a child row to act on, and the parent's history already
///    records `ChildWorkflowStarted`.
/// 2. A pending cancel beats everything else, so a race-loser or over-deadline
///    child stops burning work at the first opportunity.
/// 3. A terminal child beats a closed parent: delivery re-checks the parent
///    under `FOR UPDATE` and degrades to a plain row-delete when it has sealed,
///    whereas retiring first would drop a wake the parent could still consume.
/// 4. An **unknown** parent (`parent_terminal: None` — this sweep could not read
///    it) decides nothing destructive. Only `Some(true)` retires or cascades.
#[must_use]
pub fn next_cross_shard_child_action(
    obs: &CrossShardChildObservation<'_>,
) -> CrossShardChildAction {
    use crate::types::ParentClosePolicy;

    if obs.status == CrossShardChildStatus::PendingStart {
        return CrossShardChildAction::StartChild;
    }
    if obs.cancel_requested {
        return CrossShardChildAction::CancelChild;
    }

    let child_terminal = obs.child_state.is_some_and(is_terminal_execution_state);

    obs.parent_close_policy.map_or_else(
        // Awaited: the parent is parked on this child's terminal.
        || {
            if child_terminal {
                CrossShardChildAction::DeliverTerminal
            } else if obs.parent_terminal == Some(true) {
                // Parity with the same-shard contract: an awaited child can
                // outlive a cancelled or terminated parent. Nobody is left to
                // wake, so stop tracking it rather than polling forever.
                //
                // `Some(true)` and not a bare truthiness check: an unread parent
                // must never reach this arm, because retiring here is
                // irreversible and silently drops the child's terminal wake.
                CrossShardChildAction::Retire
            } else {
                CrossShardChildAction::Wait
            }
        },
        // Detached: the parent never consumes a terminal; the only thing left
        // owed is the parent-close cascade.
        |policy| {
            // `Abandon` is owed nothing at all once the child exists: no terminal
            // to deliver, and by definition no cascade. Retiring immediately
            // matters at scale — an abandoned child may be long-lived or never
            // terminate, and keeping its row would grow the table and the poll
            // set without bound across repeated detached fan-outs. The row's job
            // (getting the child created on the target shard) is done.
            if policy == ParentClosePolicy::Abandon || child_terminal {
                CrossShardChildAction::Retire
            } else if obs.parent_terminal == Some(true) {
                CrossShardChildAction::ApplyCloseCascade
            } else {
                // Includes `None`: cascading a live parent's children because a
                // read blipped would cancel or terminate perfectly healthy work.
                CrossShardChildAction::Wait
            }
        },
    )
}

/// Is `state` one of the engine's terminal execution states?
///
/// Delegates to [`crate::erase::is_terminal_state`] rather than restating the
/// list. A local copy had already drifted — it omitted `CONTINUED_AS_NEW`, so a
/// child sealing that way would have been polled as non-terminal forever, the
/// parent parked forever and the outbox row leaked. `erase` carries no
/// `db`-feature gate, so there is no reason to keep a second list.
fn is_terminal_execution_state(state: &str) -> bool {
    crate::erase::is_terminal_state(state)
}
