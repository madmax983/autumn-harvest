#!/usr/bin/env bash
# Four-way comparison harness for the external-outbox claim queries (issue
# #1486), documented in `docs/performance-external-outbox-scan.md`.
#
# The Rust evidence capture
# (`external_outbox_scan_perf_repro.sh`) measures one thing well: the shipped
# query against the shipped schema, through the real drain loop. It cannot
# answer "what would the other three combinations have cost", because it runs
# the query the crate actually ships.
#
# This harness answers that. It swaps schema and query text freely over one
# fixture, which is what the page's four-way table and its stale-estimate
# table need. Every figure on that page that does not come from
# `docs/perf-artifacts/external-outbox-scan/` comes from here.
#
# Usage:
#   PGURL=postgres://postgres:postgres@localhost:5432 \
#     ./autumn-harvest/scripts/external_outbox_scan_matrix.sh
#
# Preconditions: `psql` on PATH, a Postgres 16 the URL can reach as a
# superuser, and `pg_stat_statements` in `shared_preload_libraries`. The
# harness creates and drops its own databases and never touches an existing
# one.
#
# What it prints, per scenario: total buffers over a full 50-request drain
# (`pg_stat_statements`, scoped to this run's own database), and the cold
# single-claim plan's buffer count.
#
# Every scenario builds its own 1.02M-row database from scratch, so each one
# costs about three minutes. The default run is the two tables the page leads
# with, and takes roughly 25 minutes. The request-history sweep triples that,
# so it is opt-in:
#
#   OUTBOX_MATRIX_HISTORY=1 ./autumn-harvest/scripts/external_outbox_scan_matrix.sh
set -euo pipefail

PGURL="${PGURL:-postgres://postgres:postgres@localhost:5432}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

psql_run() { psql "$PGURL/$1" -v ON_ERROR_STOP=1 -q "${@:2}"; }
# `ON_ERROR_STOP` here too, and deliberately so. `psql -f` otherwise continues
# past a failed statement, and the drain would still print a
# `pg_stat_statements` total covering only the steps that ran. A partial total
# is indistinguishable from a real one once it reaches the page, so this
# harness stops instead of publishing it.
psql_at() { psql "$PGURL/$1" -At -v ON_ERROR_STOP=1 "${@:2}"; }

# ---------------------------------------------------------------------------
# Schema and fixture
# ---------------------------------------------------------------------------

# Every migration in directory order, which is how a fresh database is built.
for d in "$ROOT"/autumn-harvest/migrations/*/; do
    printf -- '-- ==== %s\n' "$d"
    cat "$d/up.sql" 2>/dev/null || true
    printf '\n'
done > "$WORK/schema.sql"

# Issue #1486's own fixture: 5,000 RUNNING executions with 200 events each, a
# 2,000-execution terminal tail with 10 each, and 50 unresolved requests.
cat > "$WORK/fixture.sql" <<'SQL'
INSERT INTO harvest_workflow_executions (id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name, started_at, created_at)
SELECT gen_random_uuid(), 'probe_wf', 'probe_wf_'||gs, gen_random_uuid(), 0, 'RUNNING', '{}'::jsonb, 'default', NOW(), NOW()
FROM generate_series(1,5000) gs;
INSERT INTO harvest_workflow_executions (id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name, started_at, created_at)
SELECT gen_random_uuid(), 'probe_wf_done', 'probe_wf_done_'||gs, gen_random_uuid(), 0, 'COMPLETED', '{}'::jsonb, 'default', NOW(), NOW()
FROM generate_series(1,2000) gs;
INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
SELECT e.id, gs,
       (ARRAY['ActivityScheduled','ActivityStarted','ActivityCompleted','WorkflowTaskScheduled'])[1 + (gs % 4)],
       jsonb_build_object('type', 'ActivityScheduled', 'data', jsonb_build_object('activity_id', 'a'||gs)),
       NOW() - (gs || ' seconds')::interval
FROM harvest_workflow_executions e, generate_series(1,200) gs
WHERE e.state = 'RUNNING';
INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
SELECT e.id, gs,
       (ARRAY['ActivityScheduled','ActivityStarted','ActivityCompleted','WorkflowTaskScheduled'])[1 + (gs % 4)],
       jsonb_build_object('type', 'ActivityScheduled', 'data', jsonb_build_object('activity_id', 'a'||gs)),
       NOW() - (gs || ' seconds')::interval
FROM harvest_workflow_executions e, generate_series(1,10) gs
WHERE e.state <> 'RUNNING';
INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
SELECT e.id, 5000, 'ExternalSignalRequested',
       jsonb_build_object('type','ExternalSignalRequested','data',
           jsonb_build_object('signal_id', 'sig-'||e.workflow_id)),
       NOW()
FROM (SELECT * FROM harvest_workflow_executions WHERE state = 'RUNNING' ORDER BY workflow_id LIMIT 50) e;
SQL

# ---------------------------------------------------------------------------
# The two query shapes
# ---------------------------------------------------------------------------

cat > "$WORK/legacy.sql" <<'SQL'
SELECT e.* FROM harvest_events e
INNER JOIN harvest_workflow_executions execs ON e.workflow_exec_id = execs.id
WHERE e.event_type = 'ExternalSignalRequested'
  AND execs.state = 'RUNNING'
  AND execs.shard_id = ANY('{0}'::int[])
  AND (e.event_data->'data'->>'signal_id') IS NOT NULL
  AND NOT (e.id = ANY('{}'::bigint[]))
  AND NOT EXISTS (
      SELECT 1 FROM harvest_events res
      WHERE res.workflow_exec_id = e.workflow_exec_id
        AND res.event_type IN ('ExternalSignalDelivered', 'ExternalSignalFailed')
        AND res.event_data->'data'->>'signal_id' = e.event_data->'data'->>'signal_id'
  )
LIMIT 1
FOR UPDATE OF e SKIP LOCKED
SQL

cat > "$WORK/rewritten.sql" <<'SQL'
SELECT e.* FROM harvest_events e
JOIN LATERAL (
    SELECT 1 AS running_exec FROM harvest_workflow_executions x
    WHERE x.id = e.workflow_exec_id
      AND x.state = 'RUNNING'
      AND x.shard_id = ANY('{0}'::int[])
    LIMIT 1
) running ON TRUE
WHERE e.event_type = 'ExternalSignalRequested'
  AND (e.event_data->'data'->>'signal_id') IS NOT NULL
  AND NOT (e.id = ANY('{}'::bigint[]))
  AND NOT EXISTS (
      SELECT 1 FROM harvest_events res
      WHERE res.workflow_exec_id = e.workflow_exec_id
        AND res.event_type IN ('ExternalSignalDelivered', 'ExternalSignalFailed')
        AND res.event_data->'data'->>'signal_id' = e.event_data->'data'->>'signal_id'
  )
ORDER BY e.timestamp, e.id
LIMIT 1
FOR UPDATE OF e SKIP LOCKED
SQL

# The shipped query with the executions join written as a plain `INNER JOIN`
# instead of a `LATERAL`. Everything else -- the `ORDER BY` pin and the
# `NOT EXISTS` resolution check -- is identical, so a comparison against
# `rewritten.sql` isolates what pinning that one join is worth.
cat > "$WORK/rewritten_plain_join.sql" <<'SQL'
SELECT e.* FROM harvest_events e
INNER JOIN harvest_workflow_executions execs ON e.workflow_exec_id = execs.id
WHERE e.event_type = 'ExternalSignalRequested'
  AND execs.state = 'RUNNING'
  AND execs.shard_id = ANY('{0}'::int[])
  AND (e.event_data->'data'->>'signal_id') IS NOT NULL
  AND NOT (e.id = ANY('{}'::bigint[]))
  AND NOT EXISTS (
      SELECT 1 FROM harvest_events res
      WHERE res.workflow_exec_id = e.workflow_exec_id
        AND res.event_type IN ('ExternalSignalDelivered', 'ExternalSignalFailed')
        AND res.event_data->'data'->>'signal_id' = e.event_data->'data'->>'signal_id'
  )
ORDER BY e.timestamp, e.id
LIMIT 1
FOR UPDATE OF e SKIP LOCKED
SQL

# The indexes, minus the migration's guard, so a scenario can install a subset.
cat > "$WORK/idx_pending.sql" <<'SQL'
CREATE INDEX idx_harvest_events_external_outbox_pending
    ON harvest_events (event_type, timestamp, id)
    WHERE event_type IN ('ExternalSignalRequested', 'ExternalCancelRequested', 'ExternalAwaitRequested');
SQL
cat > "$WORK/idx_all.sql" <<'SQL'
CREATE INDEX idx_harvest_events_external_outbox_pending
    ON harvest_events (event_type, timestamp, id)
    WHERE event_type IN ('ExternalSignalRequested', 'ExternalCancelRequested', 'ExternalAwaitRequested');
CREATE INDEX idx_harvest_events_external_signal_resolved
    ON harvest_events (workflow_exec_id, (event_data->'data'->>'signal_id'))
    WHERE event_type IN ('ExternalSignalDelivered', 'ExternalSignalFailed');
SQL

# ---------------------------------------------------------------------------
# Drain generator: 50 claim-and-resolve rounds, then one claim that finds none
# ---------------------------------------------------------------------------

gen_drain() {
    local claim; claim="$(cat "$1")"
    echo "SELECT pg_stat_statements_reset(0, (SELECT oid FROM pg_database WHERE datname = current_database()), 0);"
    for i in $(seq 1 50); do
        echo "BEGIN;"
        # Project the two columns the resolution marker needs, so `\gset` can
        # carry them into the INSERT. The predicate is untouched.
        echo "$claim" | sed "1s|^SELECT e\\.\\*|SELECT e.workflow_exec_id AS cexec, e.event_data->'data'->>'signal_id' AS csig|"
        echo '\gset'
        echo "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp) VALUES (:'cexec', 900000 + $i, 'ExternalSignalDelivered', jsonb_build_object('type','ExternalSignalDelivered','data',jsonb_build_object('signal_id', :'csig')), NOW());"
        echo "COMMIT;"
    done
    echo "BEGIN;"; echo "$claim"; echo ";"; echo "COMMIT;"
    echo "SELECT sum(shared_blks_hit + shared_blks_read) AS total_buffers, sum(calls) AS statements"
    echo "FROM pg_stat_statements WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database());"
}

# run_scenario <label> <query file> [index file] [extra fixture file]
run_scenario() {
    local name="$1" query="$2" indexes="${3:-}" extra="${4:-}"
    local db="outbox_matrix_$$"
    psql "$PGURL/postgres" -q -c "DROP DATABASE IF EXISTS $db;" >/dev/null
    psql "$PGURL/postgres" -q -c "CREATE DATABASE $db;" >/dev/null
    psql_run "$db" -f "$WORK/schema.sql" >/dev/null 2>&1
    # The bundle above includes this change's own migration, so start every
    # scenario from no indexes and install only what it asks for. Without
    # this, the baseline silently measures the fixed state.
    psql_run "$db" -f "$ROOT/autumn-harvest/migrations/20260911213344_harvest_external_outbox_scan_indexes/down.sql" >/dev/null
    psql_run "$db" -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;" >/dev/null
    psql_run "$db" -f "$WORK/fixture.sql" >/dev/null
    [ -n "$indexes" ] && psql_run "$db" -f "$indexes" >/dev/null
    psql_run "$db" -c "ANALYZE harvest_events; ANALYZE harvest_workflow_executions;" >/dev/null
    # After the ANALYZE, so a scenario can leave the statistics deliberately
    # stale, or add history the statistics already describe.
    [ -n "$extra" ] && psql_run "$db" -f "$extra" >/dev/null

    local cold plan
    if ! plan=$(psql_at "$db" -c "BEGIN; EXPLAIN (ANALYZE, BUFFERS, TIMING OFF) $(cat "$query"); ROLLBACK;"); then
        echo "scenario '$name': the cold-claim plan failed; refusing to report" >&2
        exit 1
    fi
    cold=$(printf '%s\n' "$plan" | grep -oE 'shared hit=[0-9]+( read=[0-9]+)?' | head -1)

    gen_drain "$query" > "$WORK/drain.sql"
    local drain total
    if ! drain=$(psql_at "$db" -f "$WORK/drain.sql"); then
        echo "scenario '$name': the drain failed part way; refusing to report a partial total" >&2
        exit 1
    fi
    total=$(printf '%s\n' "$drain" | tail -1 | cut -d'|' -f1)
    printf '%-44s drain=%-12s cold claim: %s\n' "$name" "$total" "$cold"
    psql "$PGURL/postgres" -q -c "DROP DATABASE IF EXISTS $db;" >/dev/null
}

echo "== full 50-request drain, total buffers (pg_stat_statements, this database only) =="
echo "== repeat this script a few times: the sequential-scan scenarios vary  =="
echo "== by up to 3x run to run, and the shipped form barely moves           =="
run_scenario "baseline: no index, legacy query"      "$WORK/legacy.sql"
run_scenario "outer index only, legacy query"        "$WORK/legacy.sql"     "$WORK/idx_pending.sql"
run_scenario "rewrite only, no indexes"              "$WORK/rewritten.sql"
run_scenario "all indexes, legacy query"             "$WORK/legacy.sql"     "$WORK/idx_all.sql"
run_scenario "all indexes + rewrite (as shipped)"    "$WORK/rewritten.sql"  "$WORK/idx_all.sql"

# ---------------------------------------------------------------------------
# Stale row estimate
# ---------------------------------------------------------------------------
#
# The condition a drained outage backlog leaves behind: `ANALYZE` records the
# backlog, the backlog drains, and the stored estimate stays far above the
# truth. This is the case the query rewrite exists for.
cat > "$WORK/stale.sql" <<'SQL'
INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
SELECT e.id, 20000 + gs, 'ExternalSignalRequested',
       jsonb_build_object('type','ExternalSignalRequested','data',
           jsonb_build_object('signal_id','burst-'||e.workflow_id||'-'||gs)),
       NOW()
FROM harvest_workflow_executions e, generate_series(1,4) gs
WHERE e.state = 'RUNNING';
ANALYZE harvest_events;
ANALYZE harvest_workflow_executions;
DELETE FROM harvest_events
WHERE event_type = 'ExternalSignalRequested'
  AND event_data->'data'->>'signal_id' LIKE 'burst-%';
SQL
echo
echo "== the same drain under a row estimate left 400x above the truth =="
run_scenario "stale: baseline"                    "$WORK/legacy.sql"    ""                  "$WORK/stale.sql"
run_scenario "stale: all indexes, legacy query"   "$WORK/legacy.sql"    "$WORK/idx_all.sql" "$WORK/stale.sql"
run_scenario "stale: as shipped, executions join plain" "$WORK/rewritten_plain_join.sql" "$WORK/idx_all.sql" "$WORK/stale.sql"
run_scenario "stale: as shipped"                  "$WORK/rewritten.sql" "$WORK/idx_all.sql" "$WORK/stale.sql"

# ---------------------------------------------------------------------------
# Lifetime request history
# ---------------------------------------------------------------------------
#
# `harvest_events` is append-only, so the pending index holds every request the
# deployment ever made. Each claim walks the resolved ones before it reaches a
# pending one, so the claim cost tracks lifetime requests, not backlog.
#
# Opt in with OUTBOX_MATRIX_HISTORY=1. Three more fixtures is three times the
# runtime, and the two tables above are the ones most readers want.
for h in ${OUTBOX_MATRIX_HISTORY:+2000 8000 20000}; do
    cat > "$WORK/history.sql" <<SQL
INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
SELECT e.id, 800000 + gs, 'ExternalSignalRequested',
       jsonb_build_object('type','ExternalSignalRequested','data',
           jsonb_build_object('signal_id','h-'||e.workflow_id||'-'||gs)),
       NOW() - interval '30 days' + (gs || ' seconds')::interval
FROM (SELECT * FROM harvest_workflow_executions WHERE state='RUNNING' ORDER BY workflow_id LIMIT 1000) e,
     generate_series(1, $((h / 1000))) gs;
INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
SELECT e.id, 850000 + gs, 'ExternalSignalDelivered',
       jsonb_build_object('type','ExternalSignalDelivered','data',
           jsonb_build_object('signal_id','h-'||e.workflow_id||'-'||gs)),
       NOW() - interval '29 days'
FROM (SELECT * FROM harvest_workflow_executions WHERE state='RUNNING' ORDER BY workflow_id LIMIT 1000) e,
     generate_series(1, $((h / 1000))) gs;
SQL
    echo
    echo "== $h resolved requests already in the pending index =="
    run_scenario "history $h: pre-change (no indexes)" "$WORK/legacy.sql"    ""                  "$WORK/history.sql"
    run_scenario "history $h: as shipped"              "$WORK/rewritten.sql" "$WORK/idx_all.sql" "$WORK/history.sql"
done
