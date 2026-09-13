-- Index both sides of the three external-outbox claim queries (issue #1486).
--
-- `timeout::enforce_timeouts_once` runs three sibling scanners on every
-- worker's periodic tick -- `enforce_external_signals_outbox`,
-- `enforce_external_cancels_outbox` and `enforce_external_awaits_outbox`.
-- Each drains its own outbox of pending cross-workflow requests with the same
-- query: claim one candidate row, act on it, loop until empty. Before this
-- migration nothing indexed either side of that claim.
--
-- ## What was unindexed, and what it cost
--
-- The outer scan selects one event type out of the largest table in the
-- engine. That is `event_type = 'ExternalSignalRequested'` and its two
-- siblings. `harvest_events` already carries partial indexes for exactly this
-- problem on other rare event types (`idx_harvest_events_activity_type_ts`,
-- `idx_harvest_events_reset_terminated`, both in
-- `20260702000000_harvest_usage_report_indexes`). It carried none for these
-- three. On a 1.02M-event fixture one cold claim read 1,020,000 rows to
-- return one, at 21,245 buffers, and every claim of every drain loop paid it.
--
-- The paired resolution check asks whether a request is already delivered or
-- failed. It was unindexed too, and that half is the larger cost. The check
-- is correlated per execution, so an unindexed probe re-reads every event of
-- the owning execution. A drain pays that once per already-resolved candidate
-- it steps over, so the cost grows with the square of the backlog.
--
-- The split is measured, not inferred. Adding the outer index ALONE moves a
-- full 50-request drain by about 7%. The resolution check is the rest.
--
-- ## Why the outer index alone was measured as a regression
--
-- Issue #1486 measured the obvious fix, which is the outer partial index by
-- itself, and reported a 271% regression on a full drain. Applying THIS
-- migration without the code is not that experiment: it indexes the
-- resolution check as well, and measures as an improvement on its own. The
-- halves still belong together, for two different reasons:
--
--   * The indexes alone win only while the planner's row estimate is
--     accurate. The estimate swings across the outbox's own draining range.
--     Under a 400x stale estimate the index-only form gives most of the win
--     back. Its cold claim returns to a 21,042-buffer sequential scan. The
--     pinned form stays at 8 buffers.
--   * The query rewrite alone pins the executions probe to its correlated
--     form. Without these indexes that probe is a scan, and the drain
--     measures 5 to 7 times the baseline.
--
-- So this migration and the `timeout.rs` rewrite ship together, and the
-- measurement that matters is of the pair. A full 50-request drain costs
-- about 300,000 buffers before and about 10,000 after. One committed instance
-- of that is 299,082 against 16,679, with the plans and the
-- `pg_stat_statements` snapshots, in
-- `docs/perf-artifacts/external-outbox-scan/`.
--
-- Read those as orders of magnitude. A drain total is not reproducible to the
-- digit in either direction. The per-claim plan is the stable part: a cold
-- claim falls from a 21,245-buffer sequential scan to 7 buffers.
--
-- One limit belongs here rather than only in the writeup. `harvest_events` is
-- append-only, so the pending index below holds every request the deployment
-- ever made. Each claim walks the resolved ones first. The drain stays
-- cheaper than before at every history size measured. The margin decays from
-- about 97% at no history to about 19% at 20,000 resolved requests. See
-- `docs/performance-external-outbox-scan.md` for the table and for what
-- closing that would take.
--
-- ## The four indexes
--
-- One index serves all three outer scans. Each scanner filters a single
-- `event_type`, which the leading column answers as a prefix, so three
-- scanners cost one index rather than three.
--
-- The remaining two columns, `(timestamp, id)`, supply the `ORDER BY
-- e.timestamp, e.id` the rewritten claim adds. That order is what pins the
-- plan. No other index on this table can produce it, so every competing plan
-- needs an explicit sort. A sort under `LIMIT 1` has to read every candidate
-- before it can return the first. The ordered index scan returns after one
-- row instead, and wins whatever the planner's row estimate says.
--
-- Ordering on `id` alone is not enough, and this was measured rather than
-- assumed. `harvest_events_pkey` supplies `id` order too. Under an inflated
-- estimate the planner walked the primary key, and filtered every unrelated
-- event out of a full ascending scan.
--
-- Prefixing the order with `event_type` does not fix it either. The planner
-- drops a column that the `WHERE` clause pins to a constant, which makes the
-- primary key eligible again. `timestamp` is neither constant nor served by
-- any other index, so it is the column that makes the choice unambiguous.
--
-- Three more index the resolution check, one per outbox family. Each is
-- keyed exactly to that check's own predicates: equality on
-- `workflow_exec_id`, then equality on the JSON-extracted correlation id.
-- Each is partial on the two terminal event types of its family, so only
-- those rows pay for it.
--
-- Expression indexes over `event_data` have precedent on this table:
-- `idx_harvest_events_activity_started_lookup`
-- (`20260905181020_harvest_usage_activity_lookback_index`) keys on
-- `event_data #>> '{data,activity_id}'`. The `->`/`->>` spelling here matches
-- the claim queries character for character, which is what makes the
-- expression index usable at all.
--
-- ## Interaction with the two sanctioned `harvest_events` writers
--
-- `erase.rs` (exception #2) and `codec_rotation.rs` (exception #3) both
-- rewrite `event_data` in place. Neither invalidates these indexes. Postgres
-- maintains an expression index across an `UPDATE` like any other index.
--
-- The reason the indexed values never move is the payload-field allowlist,
-- and not the scope guarantees quoted in `CLAUDE.md`. Both writers edit only
-- the six keys in `payload_store::PAYLOAD_FIELD_KEYS` -- `input`, `output`,
-- `payload`, `details`, `value` and `last_completion_result`
-- (`codec_rotation.rs` iterates that constant, `erase.rs` its own identical
-- `PAYLOAD_FIELDS`). A correlation id is not one of them, so neither writer
-- can change a key these indexes read.
--
-- Citing the scope guarantees instead would be wrong in a way that matters
-- to whoever adds the next exception. Codec rotation preserves the decoded
-- PLAINTEXT. These indexes key on the STORED bytes, which rotation rewrites
-- on purpose.
--
-- Suppose a correlation id were ever a payload field. The same plaintext
-- under a new key gives different stored bytes. The two sides of the
-- resolution check then stop matching, and an already-resolved request
-- returns to the outbox. The allowlist is what rules that out.
--
-- Erasure has a second, independent reason. It runs on terminal executions
-- only, and these scanners read RUNNING ones. An erased row is therefore
-- never a claim candidate.
--
-- ## Build cost and the zero-downtime recipe
--
-- `CREATE INDEX` (not `CONCURRENTLY`) takes `SHARE` on `harvest_events`,
-- which blocks every append, claim and completion touching this table. That
-- is the trade-off `20260702000000_harvest_usage_report_indexes` and
-- `20260905181020_harvest_usage_activity_lookback_index` made and documented.
--
-- Note the window is ONE lock hold, not four. All four builds run inside the
-- `DO` block below, inside Diesel's own migration transaction. `SHARE` is
-- held from the first build until the migration commits.
--
-- Size that window from the table scan, not from the index. Each build must
-- read every row to evaluate its partial predicate, so the cost tracks total
-- `harvest_events` rows. On a 1.22M-row fixture holding 200,000 resolution
-- events, all four builds together took 642 ms and produced 11 MB. Index
-- size tracks matching rows only, at roughly 55 bytes per row.
--
-- All four indexes are partial on event types that are rare in any history,
-- so the ongoing write cost falls on those rows alone. Measured on the same
-- fixture: 10,000 `ExternalSignalRequested` inserts cost 3.6% more WAL, and
-- `ActivityStarted` inserts show no measurable change. No other event type's
-- write path is touched.
--
-- For a live, already-large deployment, build them out of band first, then
-- run this migration. The guard below accepts an index that already exists
-- with the expected definition and is valid.
--
-- The out-of-band recipe is the one written out in full in
-- `20260905181020_harvest_usage_activity_lookback_index/up.sql`. It covers
-- the partitioned-`harvest_events` variant (`harvest partition enable`,
-- issue #958), the `indisvalid` cleanup pass, the per-leaf convergence loop
-- and the partition-maintenance freeze it needs. Run it four times, once per
-- index below.
--
-- That recipe is parameterised by the index it builds. Substitute all five
-- places, per index, or it silently targets the wrong one:
--
--   1. the cleanup generator's `relname LIKE 'idx_%_<suffix>'`,
--   2. the build generator's `format(...)` column list and predicate,
--   3. the per-leaf name `'idx_' || child.relname || '_<suffix>'`,
--   4. the `regexp_replace(...) = '<fingerprint>'` comparison -- the four
--      fingerprints are the `fingerprints` array in the `DO` block below,
--   5. the parent `CREATE INDEX` statement.
--
-- Step 5 is part of the out-of-band work, not of this migration. Skip it and
-- this migration performs the attach itself. That takes locks on the parent
-- and on every leaf, which is the outage the recipe exists to avoid.
--
-- The guard rejects two states rather than reporting success over them. That
-- migration sets out both reasons. A same-named index with a DIFFERENT
-- definition means this migration never installed the intended index, and it
-- would make `down.sql` drop an unrelated one. A matching but INVALID index
-- means an out-of-band `CONCURRENTLY` build never finished.
--
-- Two details in the guard carry weight. The `(ONLY )?` in the fingerprint
-- regexp lets a correctly pre-built PARTITIONED parent index pass, because
-- `pg_get_indexdef` renders those as `ON ONLY`. The
-- `indrelid = 'harvest_events'::regclass` clause scopes the lookup to this
-- table, because index names are unique per schema and not per table.
DO $$
DECLARE
    -- name, CREATE statement, expected `pg_get_indexdef` suffix.
    wanted CONSTANT text[] := ARRAY[
        'idx_harvest_events_external_outbox_pending',
        'idx_harvest_events_external_signal_resolved',
        'idx_harvest_events_external_cancel_resolved',
        'idx_harvest_events_external_await_resolved'
    ];
    creates CONSTANT text[] := ARRAY[
        'CREATE INDEX idx_harvest_events_external_outbox_pending ON harvest_events (event_type, timestamp, id) WHERE event_type IN (''ExternalSignalRequested'', ''ExternalCancelRequested'', ''ExternalAwaitRequested'')',
        'CREATE INDEX idx_harvest_events_external_signal_resolved ON harvest_events (workflow_exec_id, (event_data->''data''->>''signal_id'')) WHERE event_type IN (''ExternalSignalDelivered'', ''ExternalSignalFailed'')',
        'CREATE INDEX idx_harvest_events_external_cancel_resolved ON harvest_events (workflow_exec_id, (event_data->''data''->>''cancel_id'')) WHERE event_type IN (''ExternalCancelDelivered'', ''ExternalCancelFailed'')',
        'CREATE INDEX idx_harvest_events_external_await_resolved ON harvest_events (workflow_exec_id, (event_data->''data''->>''await_id'')) WHERE event_type IN (''ExternalAwaitResolved'', ''ExternalAwaitFailed'')'
    ];
    fingerprints CONSTANT text[] := ARRAY[
        'USING btree (event_type, "timestamp", id) WHERE (event_type = ANY (ARRAY[''ExternalSignalRequested''::text, ''ExternalCancelRequested''::text, ''ExternalAwaitRequested''::text]))',
        'USING btree (workflow_exec_id, (((event_data -> ''data''::text) ->> ''signal_id''::text))) WHERE (event_type = ANY (ARRAY[''ExternalSignalDelivered''::text, ''ExternalSignalFailed''::text]))',
        'USING btree (workflow_exec_id, (((event_data -> ''data''::text) ->> ''cancel_id''::text))) WHERE (event_type = ANY (ARRAY[''ExternalCancelDelivered''::text, ''ExternalCancelFailed''::text]))',
        'USING btree (workflow_exec_id, (((event_data -> ''data''::text) ->> ''await_id''::text))) WHERE (event_type = ANY (ARRAY[''ExternalAwaitResolved''::text, ''ExternalAwaitFailed''::text]))'
    ];
    existing_def text;
    existing_valid boolean;
BEGIN
    FOR i IN 1 .. array_length(wanted, 1) LOOP
        SELECT pg_get_indexdef(pg_class.oid), pg_index.indisvalid
          INTO existing_def, existing_valid
        FROM pg_class
        JOIN pg_index ON pg_index.indexrelid = pg_class.oid
        WHERE pg_class.relname = wanted[i]
          AND pg_index.indrelid = 'harvest_events'::regclass;

        IF existing_def IS NULL THEN
            EXECUTE creates[i];
        ELSIF regexp_replace(existing_def, '^CREATE INDEX \S+ ON (ONLY )?\S+ ', '') <> fingerprints[i] THEN
            RAISE EXCEPTION
                '% already exists with an unexpected definition -- resolve the name collision (rename or drop the existing index) before retrying this migration: %',
                wanted[i], existing_def;
        ELSIF NOT existing_valid THEN
            RAISE EXCEPTION
                '% already exists with the expected definition but is INVALID -- DROP INDEX CONCURRENTLY and retry the out-of-band build before retrying this migration',
                wanted[i];
        END IF;
    END LOOP;
END $$;
