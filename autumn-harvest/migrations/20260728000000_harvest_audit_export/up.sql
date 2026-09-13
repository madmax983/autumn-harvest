-- Off-box audit-record export for SIEM compliance (issue #953).
--
-- Two additive pieces, both operational metadata: a per-row export sequence on
-- the existing audit table, and a per-shard delivery cursor. No new
-- `WorkflowEvent` variant, no change to `harvest_events`, no replay impact —
-- the exporter only READS `harvest_audit_log` and writes its own bookkeeping.
--
-- Nothing here is seeded and nothing runs on the hot path: with no sink
-- configured, `export_seq` stays NULL on every row forever and the cursor
-- table stays empty.
--
-- One cost is NOT zero, and saying otherwise would be wrong: the partial index
-- below is predicated on `export_seq IS NULL`, which for an unconfigured
-- deployment matches EVERY row. Such a deployment therefore pays a full index
-- build at migration time and index maintenance on every subsequent audit
-- insert, for a feature it never turns on. Bounded by the audit retention
-- window only while retention stays enabled: `audit_retention_days = 0`
-- disables the purge and removes that bound too. Tracked as
-- autumn-foundation/autumn-harvest#1272, which weighs creating the index
-- lazily on first opt-in against leaving it here.

-- ── The per-shard monotonic sequence (AC4) ────────────────────────────────
--
-- Deliberately a plain nullable BIGINT stamped by the exporter, NOT a
-- BIGSERIAL, for two independent reasons:
--
--   1. A serial is assigned BEFORE commit. Two concurrent audited operations
--      can take 5 and 6 and commit in the order 6, 5 — a `WHERE seq > cursor`
--      exporter that shipped 6 first would skip 5 forever. That is exactly the
--      silent record loss AC2 rules out "by construction". (`occurred_at` has
--      the same defect: it is transaction START time and can move backwards
--      between concurrent inserts.) Stamping rows the exporter can actually
--      SEE (`export_seq IS NULL`) makes a skip unrepresentable: a
--      late-committing row is still NULL on the next tick and simply gets a
--      later sequence.
--
--   2. Logical replication does not replicate sequence values (see
--      20260726000000_harvest_shard_generation/up.sql, issue #954). A promoted
--      DR standby would re-issue sequence numbers it had already exported,
--      corrupting the receiver's `(shard, seq)` dedup and gap accounting. The
--      counter therefore lives in `harvest_audit_export_cursor` below —
--      ordinary replicated table data, not a sequence object.
ALTER TABLE harvest_audit_log
    ADD COLUMN IF NOT EXISTS export_seq BIGINT NULL;

COMMENT ON COLUMN harvest_audit_log.export_seq IS
    'Dense, strictly monotonic per-shard export sequence assigned by the audit '
    'exporter (issue #953). NULL = not yet assigned (the steady state when no '
    'audit sink is configured). Never reused, never rewritten: a redrive moves '
    'the cursor, not these values, which is what makes re-export byte-identical.';

-- Claim scan: `WHERE export_seq IS NULL ORDER BY occurred_at, id LIMIT n`.
-- A partial index, but it is NOT empty when no sink is configured: it then
-- matches every row. See the header caveat above (issue #1272).
CREATE INDEX IF NOT EXISTS harvest_audit_log_unexported_idx
    ON harvest_audit_log (occurred_at, id)
    WHERE export_seq IS NULL;

-- Delivery scan: `WHERE export_seq > $cursor ORDER BY export_seq LIMIT n`,
-- and the redrive lookup `MAX(export_seq) WHERE occurred_at < $ts`.
CREATE INDEX IF NOT EXISTS harvest_audit_log_export_seq_idx
    ON harvest_audit_log (export_seq)
    WHERE export_seq IS NOT NULL;

-- ── The per-shard delivery cursor ─────────────────────────────────────────
--
-- Exactly one row per shard, in that shard's own database. Like
-- `harvest_shard_generation` (#954) it is deliberately NOT seeded here: a
-- shard's database cannot know its own shard id, and seeding a speculative
-- `(0, ...)` would put a row with the wrong identity in shards 1..N. The row is
-- provisioned by the exporter, which is told which shard it is scanning.
--
-- `last_acked_seq` is the compliance-relevant value: every record at or below
-- it has been acknowledged by the sink at least once. It advances ONLY in the
-- post-delivery transaction, and only on a 2xx.
CREATE TABLE IF NOT EXISTS harvest_audit_export_cursor (
    shard_id             INTEGER     PRIMARY KEY,

    -- High-water mark of sequences handed out so far. Bumped in the claim
    -- transaction, under this row's lock, so two exporters can never hand the
    -- same sequence to two different records.
    last_assigned_seq    BIGINT      NOT NULL DEFAULT 0,

    -- The cursor proper: the highest sequence the sink has acknowledged.
    last_acked_seq       BIGINT      NOT NULL DEFAULT 0,

    -- Monotonic claim counter. The post-delivery write is guarded on the epoch
    -- it claimed with, so a slow attempt whose HTTP call outlives its lease can
    -- never apply a stale outcome over a fresher one (same hazard #605 guards
    -- with its per-delivery `attempt` counter, and the same fix: never reuse a
    -- guard value over the row's lifetime).
    claim_epoch          BIGINT      NOT NULL DEFAULT 0,

    -- Set while a delivery is in flight; a claim is only granted when the
    -- previous lease has expired, so a crashed exporter self-heals.
    lease_until          TIMESTAMPTZ NULL,

    -- Capped exponential backoff after a sink failure. NEVER a dead-letter
    -- deadline: an audit record is a compliance artifact and is retried
    -- forever.
    next_attempt_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    consecutive_failures INTEGER     NOT NULL DEFAULT 0,

    -- Last-error surface for `GET /admin/audit-export`.
    last_status          INTEGER     NULL,
    last_error           TEXT        NULL,
    last_delivered_at    TIMESTAMPTZ NULL,

    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Set when an operator retires audit export for this shard. The row is
    -- NEVER deleted, because `last_assigned_seq` above must outlive the audit
    -- rows themselves: retiring the cursor is what permits retention to purge
    -- them, and a deleted cursor recreated later would restart the sequence at
    -- 0 and re-issue `(shard, seq)` pairs a SIEM already holds against
    -- different records -- which a receiver deduping on that pair, exactly as
    -- this feature instructs, would silently discard. Retire, never delete.
    retired_at           TIMESTAMPTZ NULL,

    -- A negative cursor would sort below every real sequence and silently
    -- re-export the whole retained window on every tick.
    CONSTRAINT harvest_audit_export_cursor_non_negative
        CHECK (last_acked_seq >= 0 AND last_assigned_seq >= 0 AND consecutive_failures >= 0),

    -- Acknowledging a sequence that was never assigned would skip records.
    CONSTRAINT harvest_audit_export_cursor_ordered
        CHECK (last_acked_seq <= last_assigned_seq)
);

COMMENT ON TABLE harvest_audit_export_cursor IS
    'Per-shard audit-export delivery cursor (issue #953). One row per shard, in '
    'that shard own database, provisioned by the exporter (a database cannot '
    'know its own shard id). Advances only after the sink acknowledges a batch. '
    'See docs/audit-export.md.';
