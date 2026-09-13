#![cfg(feature = "db")]
//! Plan-shape and equivalence gates for the three external-outbox claim
//! queries (issue #1486).
//!
//! `timeout::enforce_timeouts_once` runs three sibling scanners on every
//! worker tick: [`external_signal_outbox_claim_query`],
//! [`external_cancel_outbox_claim_query`] and
//! [`external_await_outbox_claim_query`]. Each one claims a single pending
//! request row from `harvest_events`, acts on it, and loops until its outbox
//! is empty.
//!
//! Two independent defects made that loop read the whole event table:
//!
//! 1. No index served `event_type = 'External<X>Requested'`, so the outer
//!    scan read every row of the largest table in the engine to return one.
//! 2. The paired `NOT EXISTS` resolution check was unindexed too, and it
//!    costs more. It is correlated per execution, so each probe re-read every
//!    event of the owning execution.
//!
//! The fix pairs four partial indexes
//! (`20260911213344_harvest_external_outbox_scan_indexes`) with a query
//! rewrite that pins the outer scan and the executions join. The indexes do
//! most of the work. The rewrite matters when the planner's row estimate goes
//! stale, which is what a drained outage backlog leaves behind.
//!
//! Which gate covers which half is worth stating, because they are not
//! symmetric:
//!
//! - [`outbox_claim_plans_are_index_only`] gates the MIGRATION. The legacy
//!   query also produces an all-index plan once these indexes exist, so this
//!   one passes with the rewrite reverted. Its `Anti Join` assertion is the
//!   exception, and guards one specific regression the rewrite could
//!   reintroduce.
//! - [`outbox_claim_plans_survive_a_stale_row_estimate`] gates the REWRITE.
//!   With the rewrite reverted the legacy form degrades to a `Seq Scan` here.
//! - [`outbox_claim_queries_match_the_legacy_anti_join`] guards the result
//!   set, against the exact SQL this rewrite replaced.
//! - [`outbox_claim_returns_the_oldest_pending_request_first`] guards the
//!   drain order the `ORDER BY` pin also buys.
//! - [`zz_capture_external_outbox_scan_evidence`] is `#[ignore]`d and
//!   regenerates `docs/perf-artifacts/external-outbox-scan/`.
//!
//! `timeout::tests` carries two DB-free gates on the same queries. They cover
//! the defect class a template introduces: a transposed event type, or a
//! correlation key borrowed from another family.

use std::fmt::Write as _;

use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Text};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};

use autumn_harvest::timeout::{
    external_await_outbox_claim_query, external_cancel_outbox_claim_query,
    external_signal_outbox_claim_query,
};

use crate::claim_bench_support::db as claim_bench_db;
use crate::integration_e2e::setup_test_database_url_or_env;

/// Shard every fixture row belongs to.
const SHARD_ID: i32 = 0;

/// A shard no fixture row sits on. Proves the shard filter survives the
/// rewrite.
const OTHER_SHARD_ID: i32 = 7;

/// Filler events per bulk execution.
const EVENTS_PER_EXEC: usize = 60;

/// Bulk executions seeded by the plan gates.
///
/// With [`EVENTS_PER_EXEC`] this makes `harvest_events` large enough that a
/// `Seq Scan` is visibly the wrong plan, and small enough to seed in one
/// statement.
const BULK_EXECUTIONS: usize = 1_000;

/// The one partial index that serves all three outer scans.
///
/// Its leading `event_type` column turns each scanner's single-type equality
/// into a prefix lookup. Its `(timestamp, id)` columns supply the order the
/// rewrite asks for.
const PENDING_INDEX: &str = "idx_harvest_events_external_outbox_pending";

/// One outbox family: the request type, the two events that resolve it, and
/// the payload key that correlates them.
///
/// The three claim queries are one query with these values substituted, so
/// every test here runs against all three.
struct OutboxFamily {
    requested: &'static str,
    resolved: [&'static str; 2],
    id_key: &'static str,
    query: fn() -> &'static str,
    resolved_index: &'static str,
}

const FAMILIES: [OutboxFamily; 3] = [
    OutboxFamily {
        requested: "ExternalSignalRequested",
        resolved: ["ExternalSignalDelivered", "ExternalSignalFailed"],
        id_key: "signal_id",
        query: external_signal_outbox_claim_query,
        resolved_index: "idx_harvest_events_external_signal_resolved",
    },
    OutboxFamily {
        requested: "ExternalCancelRequested",
        resolved: ["ExternalCancelDelivered", "ExternalCancelFailed"],
        id_key: "cancel_id",
        query: external_cancel_outbox_claim_query,
        resolved_index: "idx_harvest_events_external_cancel_resolved",
    },
    OutboxFamily {
        requested: "ExternalAwaitRequested",
        resolved: ["ExternalAwaitResolved", "ExternalAwaitFailed"],
        id_key: "await_id",
        query: external_await_outbox_claim_query,
        resolved_index: "idx_harvest_events_external_await_resolved",
    },
];

#[derive(QueryableByName, Debug)]
struct PlanLine {
    #[diesel(sql_type = Text, column_name = "QUERY PLAN")]
    line: String,
}

#[derive(QueryableByName, Debug)]
struct EventId {
    #[diesel(sql_type = BigInt)]
    id: i64,
}

// ---------------------------------------------------------------------------
// Fixture helpers
// ---------------------------------------------------------------------------

/// Seed RUNNING executions with ordinary history, under one workflow name.
///
/// No pending outbox row is seeded here. Each test appends the rows its own
/// case needs.
async fn seed_bulk_history(conn: &mut AsyncPgConnection, workflow_name: &str, executions: usize) {
    conn.batch_execute(&format!(
        "INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name, \
              started_at, created_at) \
         SELECT gen_random_uuid(), '{workflow_name}', '{workflow_name}_' || gs, \
                gen_random_uuid(), {SHARD_ID}, 'RUNNING', '{{}}'::jsonb, 'default', NOW(), NOW() \
         FROM generate_series(1, {executions}) gs; \
         INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp) \
         SELECT e.id, gs, \
                (ARRAY['ActivityScheduled','ActivityStarted','ActivityCompleted','WorkflowTaskScheduled'])[1 + (gs % 4)], \
                jsonb_build_object('type', 'ActivityScheduled', \
                                   'data', jsonb_build_object('activity_id', 'a' || gs)), \
                NOW() - (gs || ' seconds')::interval \
         FROM harvest_workflow_executions e, generate_series(1, {EVENTS_PER_EXEC}) gs \
         WHERE e.workflow_name = '{workflow_name}';"
    ))
    .await
    .expect("seed bulk history");
}

/// Insert one execution and return its id.
async fn seed_execution(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    state: &str,
    shard: i32,
) -> uuid::Uuid {
    let id = uuid::Uuid::new_v4();
    conn.batch_execute(&format!(
        "INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name, \
              started_at, created_at) \
         VALUES ('{id}', '{workflow_name}', '{workflow_id}', gen_random_uuid(), {shard}, \
                 '{state}', '{{}}'::jsonb, 'default', NOW(), NOW())"
    ))
    .await
    .expect("seed execution");
    id
}

/// Append one event and return its `harvest_events.id`.
///
/// `data` is the inner `data` object, written verbatim, so a caller can seed
/// a missing or JSON-null id key.
async fn seed_event(
    conn: &mut AsyncPgConnection,
    exec_id: uuid::Uuid,
    event_id: i32,
    event_type: &str,
    data: &str,
) -> i64 {
    let rows: Vec<EventId> = diesel::sql_query(format!(
        "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp) \
         VALUES ('{exec_id}', {event_id}, '{event_type}', \
                 jsonb_build_object('type', '{event_type}', 'data', '{data}'::jsonb), NOW()) \
         RETURNING id"
    ))
    .load(conn)
    .await
    .expect("seed event");
    rows.into_iter()
        .next()
        .map(|r| r.id)
        .expect("INSERT ... RETURNING yields one row")
}

/// Append one event with an explicit `timestamp` expression.
///
/// `timestamp_sql` is SQL, not a literal, so a caller can seed a row whose
/// append order and request order disagree.
async fn seed_event_at(
    conn: &mut AsyncPgConnection,
    exec_id: uuid::Uuid,
    event_id: i32,
    event_type: &str,
    data: &str,
    timestamp_sql: &str,
) -> i64 {
    let rows: Vec<EventId> = diesel::sql_query(format!(
        "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp) \
         VALUES ('{exec_id}', {event_id}, '{event_type}', \
                 jsonb_build_object('type', '{event_type}', 'data', '{data}'::jsonb), \
                 {timestamp_sql}) \
         RETURNING id"
    ))
    .load(conn)
    .await
    .expect("seed event");
    rows.into_iter()
        .next()
        .map(|r| r.id)
        .expect("INSERT ... RETURNING yields one row")
}

async fn analyze(conn: &mut AsyncPgConnection) {
    conn.batch_execute("ANALYZE harvest_events; ANALYZE harvest_workflow_executions;")
        .await
        .expect("analyze");
}

/// Provision an isolated, fully migrated database for a plan gate.
///
/// The plan gates assert which scan the planner picks, so they need a
/// database whose size and statistics they control completely. A shared
/// `HARVEST_TEST_DATABASE_URL` carries other suites' rows, and seeding
/// [`BULK_EXECUTIONS`] into it would disturb them in return.
///
/// A database that cannot be provisioned fails the gate. A plan gate that
/// skips itself reports success over the defect it exists to catch.
async fn plan_gate_db() -> claim_bench_db::BenchDb {
    claim_bench_db::setup_bench_db().await.unwrap_or_else(|r| {
        panic!(
            "the plan gates need an isolated database, from HARVEST_TEST_DATABASE_URL \
             (an admin connection string) or a reachable Docker daemon: {}",
            r.0
        )
    })
}

// ---------------------------------------------------------------------------
// Query-text helpers
// ---------------------------------------------------------------------------

/// Replace the two bind placeholders with literals.
///
/// `EXPLAIN` on a parameterised statement can report a generic plan, which is
/// not the plan the scanner runs. Literals measure the real one.
fn with_literal_binds(sql: &str, shards: &[i32], excluded: &[i64]) -> String {
    let shard_list = shards
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let excluded_list = excluded
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",");
    sql.replace("$1", &format!("'{{{shard_list}}}'::int[]"))
        .replace("$2", &format!("'{{{excluded_list}}}'::bigint[]"))
}

/// Strip the row-limiting and locking tail, so the query reports every row it
/// selects.
///
/// The equivalence gate compares sets. `LIMIT 1` compares one row, which two
/// queries can agree on while disagreeing about every other row.
///
/// The cut is the LAST `LIMIT 1`. The rewritten query also carries one inside
/// each `LATERAL`. Those belong to the predicate, not to the tail.
fn as_full_result_set(sql: &str) -> String {
    const TAIL: &str = "FOR UPDATE OF e SKIP LOCKED";
    let cut = sql
        .rfind("LIMIT 1")
        .unwrap_or_else(|| panic!("a claim query must carry a `LIMIT 1` tail: {sql}"));
    let tail = sql[cut..].split_whitespace().collect::<Vec<_>>().join(" ");
    assert_eq!(
        tail,
        format!("LIMIT 1 {TAIL}"),
        "the last `LIMIT 1` must be the outer one, or this cut removes the wrong clause"
    );
    sql[..cut].to_string()
}

/// The claim query as it stood before issue #1486.
///
/// This is the equivalence oracle. A rewrite that changes which rows the
/// scanner claims is a behaviour change, not an optimisation. So the new SQL
/// is compared against the replaced text, and not against a hand-listed
/// expectation.
fn legacy_claim_query(family: &OutboxFamily) -> String {
    let OutboxFamily {
        requested,
        resolved: [first, second],
        id_key,
        ..
    } = family;
    format!(
        "SELECT e.* FROM harvest_events e \
         INNER JOIN harvest_workflow_executions execs ON e.workflow_exec_id = execs.id \
         WHERE e.event_type = '{requested}' \
           AND execs.state = 'RUNNING' \
           AND execs.shard_id = ANY($1) \
           AND (e.event_data->'data'->>'{id_key}') IS NOT NULL \
           AND NOT (e.id = ANY($2)) \
           AND NOT EXISTS ( \
               SELECT 1 FROM harvest_events res \
               WHERE res.workflow_exec_id = e.workflow_exec_id \
                 AND res.event_type IN ('{first}', '{second}') \
                 AND res.event_data->'data'->>'{id_key}' = e.event_data->'data'->>'{id_key}' \
           ) \
         LIMIT 1 \
         FOR UPDATE OF e SKIP LOCKED"
    )
}

async fn explain(conn: &mut AsyncPgConnection, sql: &str) -> String {
    let lines: Vec<PlanLine> = diesel::sql_query(format!("EXPLAIN (COSTS OFF) {sql}"))
        .load(conn)
        .await
        .unwrap_or_else(|e| panic!("EXPLAIN failed: {e}\n{sql}"));
    lines
        .into_iter()
        .map(|l| l.line)
        .collect::<Vec<_>>()
        .join("\n")
}

/// Run one claim exactly as the scanner does, and report the row it claims.
///
/// The query keeps its `LIMIT 1`, its `ORDER BY` and its locking clause, so
/// this reads the scanner's real choice rather than a re-sorted set.
async fn claim_one(conn: &mut AsyncPgConnection, sql: &str) -> Option<i64> {
    let rows: Vec<EventId> = diesel::sql_query(format!("SELECT e.id FROM ({sql}) e"))
        .load(conn)
        .await
        .unwrap_or_else(|e| panic!("claim query failed: {e}\n{sql}"));
    // `into_iter().next()`, not `first()`: diesel's `RunQueryDsl` is in scope
    // and claims that method name on `Vec` before the slice inherent one.
    rows.into_iter().next().map(|r| r.id)
}

/// Run a claim query and return the `harvest_events.id` of every row it
/// selects.
///
/// `scope` names the workflow the caller seeded. The claim query itself runs
/// over the whole table, exactly as the scanner does. Only the reported rows
/// are narrowed, so a shared database's leftovers cannot decide the result.
async fn selected_ids(conn: &mut AsyncPgConnection, sql: &str, scope: &str) -> Vec<i64> {
    let rows: Vec<EventId> = diesel::sql_query(format!(
        "SELECT e.id FROM ({sql}) e \
         JOIN harvest_workflow_executions x ON x.id = e.workflow_exec_id \
         WHERE x.workflow_name = '{scope}' \
         ORDER BY e.id"
    ))
    .load(conn)
    .await
    .unwrap_or_else(|e| panic!("claim query failed: {e}\n{sql}"));
    rows.into_iter().map(|r| r.id).collect()
}

// ---------------------------------------------------------------------------
// Plan-shape gates
// ---------------------------------------------------------------------------

/// Every node of every claim plan reads through an index.
///
/// This is the issue's headline defect. The outer scan read 1.02M rows to
/// return one, on every claim of every drain loop, on every worker tick.
#[tokio::test]
async fn outbox_claim_plans_are_index_only() {
    let bench = plan_gate_db().await;
    let mut conn = claim_bench_db::connect(&bench.url).await;

    seed_bulk_history(&mut conn, "outbox_plan_wf", BULK_EXECUTIONS).await;
    for family in &FAMILIES {
        let exec = seed_execution(
            &mut conn,
            "outbox_plan_pending_wf",
            family.requested,
            "RUNNING",
            SHARD_ID,
        )
        .await;
        seed_event(
            &mut conn,
            exec,
            1,
            family.requested,
            &format!("{{\"{}\": \"pending-1\"}}", family.id_key),
        )
        .await;
    }
    analyze(&mut conn).await;

    for family in &FAMILIES {
        let sql = with_literal_binds((family.query)(), &[SHARD_ID], &[]);
        let plan = explain(&mut conn, &sql).await;
        let tag = family.requested;

        assert!(
            !plan.contains("Seq Scan on harvest_events"),
            "{tag}: the claim must not scan the event table\n{plan}"
        );
        assert!(
            plan.contains(PENDING_INDEX),
            "{tag}: the outer scan must read through {PENDING_INDEX}\n{plan}"
        );
        assert!(
            plan.contains(family.resolved_index),
            "{tag}: the resolution check must read through {}\n{plan}",
            family.resolved_index
        );
        assert!(
            !plan.contains("Materialize"),
            "{tag}: the resolution check must stay correlated, not materialised\n{plan}"
        );
        // The resolution check must remain an anti-join, and this is the one
        // assertion that is about cost rather than correctness.
        //
        // `harvest_events` is append-only, so a resolved request stays in the
        // pending index and every later claim walks past it. An anti-join
        // lets the planner discard such a row before the executions probe,
        // which then runs once. Written as an outer join the check cannot be
        // reordered, so both probes run for every discarded row. An earlier
        // revision of this change did exactly that, and measured twice the
        // buffers over a backlog of 8,000 resolved requests.
        assert!(
            plan.contains("Anti Join"),
            "{tag}: the resolution check must be an anti-join, so a discarded \
             candidate does not also pay the executions probe\n{plan}"
        );
        assert!(
            !plan.contains("Sort"),
            "{tag}: the index must supply the order, so no sort is needed\n{plan}"
        );
    }
}

/// The index plan holds when the planner's row estimate is far too high.
///
/// This is the failure mode the issue measured, from a cause a deployment
/// meets in practice. An outage fills the outbox, `ANALYZE` records the
/// backlog, and the backlog drains. The stored estimate is then orders of
/// magnitude above the truth. A plan that depends on it reverts to a `Seq
/// Scan`, or walks `harvest_events_pkey` and filters every unrelated event.
///
/// The `ORDER BY e.timestamp, e.id` pin removes the dependency. No other
/// index supplies that order, so every competing plan needs a sort. A sort
/// under `LIMIT 1` must read every candidate before it returns one.
#[tokio::test]
async fn outbox_claim_plans_survive_a_stale_row_estimate() {
    let bench = plan_gate_db().await;
    let mut conn = claim_bench_db::connect(&bench.url).await;

    // This gate deletes about 12,000 rows, then reads the plan the STALE
    // statistics produce. Autoanalyze would refresh them mid-test, so turn it
    // off rather than race it.
    conn.batch_execute("ALTER TABLE harvest_events SET (autovacuum_enabled = false)")
        .await
        .expect("pin the statistics this gate is about");

    seed_bulk_history(&mut conn, "outbox_stale_wf", BULK_EXECUTIONS).await;

    // The outage backlog: four pending requests per family per execution.
    // Each family takes its own `event_id` band, because the three share the
    // executions and `(workflow_exec_id, event_id)` is unique.
    for (slot, family) in FAMILIES.iter().enumerate() {
        let band = 90_000 + slot * 10;
        conn.batch_execute(&format!(
            "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp) \
             SELECT e.id, {band} + gs, '{requested}', \
                    jsonb_build_object('type', '{requested}', \
                                       'data', jsonb_build_object('{key}', 'burst-' || e.workflow_id || '-' || gs)), \
                    NOW() \
             FROM harvest_workflow_executions e, generate_series(1, 4) gs \
             WHERE e.workflow_name = 'outbox_stale_wf';",
            requested = family.requested,
            key = family.id_key
        ))
        .await
        .expect("seed the outage backlog");
    }
    analyze(&mut conn).await;

    // The backlog drains. Autovacuum has not re-analysed, so the estimate
    // stays at the burst size while the real count falls to one.
    for family in &FAMILIES {
        conn.batch_execute(&format!(
            "DELETE FROM harvest_events \
             WHERE event_type = '{}' AND event_data->'data'->>'{}' LIKE 'burst-%';",
            family.requested, family.id_key
        ))
        .await
        .expect("drain the outage backlog");
        let exec = seed_execution(
            &mut conn,
            "outbox_stale_pending_wf",
            family.requested,
            "RUNNING",
            SHARD_ID,
        )
        .await;
        seed_event(
            &mut conn,
            exec,
            1,
            family.requested,
            &format!("{{\"{}\": \"pending-1\"}}", family.id_key),
        )
        .await;
    }

    for family in &FAMILIES {
        let sql = with_literal_binds((family.query)(), &[SHARD_ID], &[]);
        let plan = explain(&mut conn, &sql).await;
        let tag = family.requested;
        assert!(
            !plan.contains("Seq Scan on harvest_events"),
            "{tag}: a stale row estimate must not cost the index plan\n{plan}"
        );
        assert!(
            plan.contains(PENDING_INDEX),
            "{tag}: the outer scan must still read through {PENDING_INDEX}\n{plan}"
        );
    }
}

// ---------------------------------------------------------------------------
// Equivalence gate
// ---------------------------------------------------------------------------

/// One family's equivalence fixture, and what a correct claim query must
/// make of it.
struct EquivalenceCases {
    /// Workflow name every case execution carries, unique per invocation.
    scope: String,
    /// The requests that must be selected, in `harvest_events.id` order.
    expected: Vec<i64>,
    /// The caller's per-sweep give-up list.
    excluded: Vec<i64>,
}

/// Seed one case execution, named after the case it covers.
async fn case_execution(
    conn: &mut AsyncPgConnection,
    scope: &str,
    case: &str,
    state: &str,
    shard: i32,
) -> uuid::Uuid {
    seed_execution(conn, scope, &format!("{scope}-{case}"), state, shard).await
}

/// Seed one event carrying `key = value` in its payload.
async fn case_event(
    conn: &mut AsyncPgConnection,
    exec: uuid::Uuid,
    event_id: i32,
    event_type: &str,
    key: &str,
    value: &str,
) -> i64 {
    seed_event(
        conn,
        exec,
        event_id,
        event_type,
        &format!("{{\"{key}\": \"{value}\"}}"),
    )
    .await
}

/// Seed every predicate the rewrite touches, for one family.
async fn seed_equivalence_cases(
    conn: &mut AsyncPgConnection,
    family: &OutboxFamily,
    suffix: &str,
) -> EquivalenceCases {
    let key = family.id_key;
    let [first, second] = family.resolved;
    let tag = family.requested;
    let scope = format!("outbox_equiv_{tag}_{suffix}");

    // Selected: a pending request on a RUNNING execution in this shard.
    let plain = case_execution(conn, &scope, "plain", "RUNNING", SHARD_ID).await;
    let want_plain = case_event(conn, plain, 1, tag, key, "s1").await;

    // Excluded: resolved by each of the two terminal event types.
    let delivered = case_execution(conn, &scope, "delivered", "RUNNING", SHARD_ID).await;
    case_event(conn, delivered, 1, tag, key, "s2").await;
    case_event(conn, delivered, 2, first, key, "s2").await;
    let failed = case_execution(conn, &scope, "failed", "RUNNING", SHARD_ID).await;
    case_event(conn, failed, 1, tag, key, "s3").await;
    case_event(conn, failed, 2, second, key, "s3").await;

    // Selected: a resolution event carries the same key but belongs to
    // another execution. The check is correlated per execution.
    let mine = case_execution(conn, &scope, "mine", "RUNNING", SHARD_ID).await;
    let theirs = case_execution(conn, &scope, "theirs", "RUNNING", SHARD_ID).await;
    let want_mine = case_event(conn, mine, 1, tag, key, "s4").await;
    case_event(conn, theirs, 1, first, key, "s4").await;

    // Selected: the execution holds a resolution for a different request.
    let other = case_execution(conn, &scope, "other", "RUNNING", SHARD_ID).await;
    let want_other = case_event(conn, other, 1, tag, key, "s5").await;
    case_event(conn, other, 2, first, key, "s6").await;

    // Excluded: the execution is terminal, or sits on another shard.
    let terminal = case_execution(conn, &scope, "terminal", "COMPLETED", SHARD_ID).await;
    case_event(conn, terminal, 1, tag, key, "s7").await;
    let elsewhere = case_execution(conn, &scope, "elsewhere", "RUNNING", OTHER_SHARD_ID).await;
    case_event(conn, elsewhere, 1, tag, key, "s8").await;

    // Excluded: the key is absent, and the key is JSON null. The second case
    // is why the resolution check stays `NOT EXISTS` and never becomes `NOT
    // IN`.
    let keyless = case_execution(conn, &scope, "keyless", "RUNNING", SHARD_ID).await;
    seed_event(conn, keyless, 1, tag, "{}").await;
    seed_event(conn, keyless, 2, tag, &format!("{{\"{key}\": null}}")).await;

    // Excluded by the caller's per-sweep give-up list.
    let skipped = case_execution(conn, &scope, "skipped", "RUNNING", SHARD_ID).await;
    let skip_id = case_event(conn, skipped, 1, tag, key, "s9").await;

    EquivalenceCases {
        scope,
        expected: vec![want_plain, want_mine, want_other],
        excluded: vec![skip_id],
    }
}

/// The rewritten query selects exactly the rows the legacy anti-join did.
///
/// The fixture covers every predicate the rewrite touches. That includes the
/// two the issue named as needing their own correctness review: the `NOT
/// EXISTS` NULL semantics, and the per-execution correlation of the
/// resolution check.
#[tokio::test]
async fn outbox_claim_queries_match_the_legacy_anti_join() {
    let (database_url, _container) = setup_test_database_url_or_env().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect to test database");

    // Unique per invocation, because `HARVEST_TEST_DATABASE_URL` may name a
    // database this suite shares with others and with its own earlier runs.
    let suffix = uuid::Uuid::new_v4().simple().to_string();

    for family in &FAMILIES {
        let tag = family.requested;
        // The oracle is a hand-copy of the replaced SQL. If the shipped query
        // ever returns to that text, comparing the two proves nothing.
        assert_ne!(
            (family.query)(),
            legacy_claim_query(family),
            "{tag}: the shipped query is identical to the pre-#1486 oracle -- \
             update `legacy_claim_query` before trusting this comparison"
        );
        let cases = seed_equivalence_cases(&mut conn, family, &suffix).await;

        let legacy = as_full_result_set(&with_literal_binds(
            &legacy_claim_query(family),
            &[SHARD_ID],
            &cases.excluded,
        ));
        let rewritten = as_full_result_set(&with_literal_binds(
            (family.query)(),
            &[SHARD_ID],
            &cases.excluded,
        ));

        let legacy_ids = selected_ids(&mut conn, &legacy, &cases.scope).await;
        let rewritten_ids = selected_ids(&mut conn, &rewritten, &cases.scope).await;

        assert_eq!(
            legacy_ids, rewritten_ids,
            "{tag}: the rewrite must select the legacy row set"
        );
        assert_eq!(
            rewritten_ids, cases.expected,
            "{tag}: unexpected row set, so both queries agree on the wrong answer"
        );

        // Remove the fixture. These are live `External*Requested` rows on
        // RUNNING executions. Left behind on a shared
        // `HARVEST_TEST_DATABASE_URL`, they give every other suite's outbox
        // sweep a permanent candidate it cannot decode. The foreign key
        // cascade takes the events with the executions.
        conn.batch_execute(&format!(
            "DELETE FROM harvest_workflow_executions WHERE workflow_name = '{}'",
            cases.scope
        ))
        .await
        .expect("clean up the equivalence fixture");
    }
}

// ---------------------------------------------------------------------------
// Drain-order gate
// ---------------------------------------------------------------------------

/// The scanner claims the oldest pending request first.
///
/// The `ORDER BY e.timestamp, e.id` that pins the outer scan also fixes the
/// drain order. `timestamp` is the request instant and `id` breaks ties, so
/// the oldest pending request goes first and a backlog cannot be starved by
/// newer arrivals.
///
/// The fixture appends one request LAST that carries the OLDEST timestamp.
/// Without that row every order in play agrees. Append order, `id` order,
/// `timestamp` order and heap order are then the same, so the test passes
/// against an unordered query and proves nothing. That row separates them,
/// and it must be claimed first.
///
/// Runs against an isolated database, because it reads what the real
/// `LIMIT 1` claim returns. On a shared database that row can belong to
/// another suite.
#[tokio::test]
async fn outbox_claim_returns_the_oldest_pending_request_first() {
    let bench = plan_gate_db().await;
    let mut conn = claim_bench_db::connect(&bench.url).await;

    for family in &FAMILIES {
        let key = family.id_key;
        let tag = family.requested;
        let scope = format!("outbox_order_{tag}");
        let exec = seed_execution(&mut conn, &scope, &scope, "RUNNING", SHARD_ID).await;

        // Appended first, and newest. Each carries the label the resolution
        // marker must repeat, so a claim can be resolved by id lookup.
        let mut seeded: Vec<(i64, String)> = Vec::new();
        for n in 1..=4 {
            let label = format!("q{n}");
            let id = seed_event_at(
                &mut conn,
                exec,
                n,
                tag,
                &format!("{{\"{key}\": \"{label}\"}}"),
                &format!("NOW() - INTERVAL '{} minutes'", 5 - n),
            )
            .await;
            seeded.push((id, label));
        }
        // Appended last, and oldest. This is the row that makes the test mean
        // something: the highest `id`, and the lowest timestamp.
        let oldest = seed_event_at(
            &mut conn,
            exec,
            5,
            tag,
            &format!("{{\"{key}\": \"q5\"}}"),
            "NOW() - INTERVAL '30 minutes'",
        )
        .await;
        seeded.insert(0, (oldest, "q5".to_string()));
        let expected: Vec<i64> = seeded.iter().map(|(id, _)| *id).collect();

        // Drain one claim at a time, marking each claim resolved exactly as
        // the scanner does.
        let mut drained = Vec::new();
        for n in 1..=5 {
            let sql = with_literal_binds((family.query)(), &[SHARD_ID], &[]);
            let claimed = claim_one(&mut conn, &sql)
                .await
                .unwrap_or_else(|| panic!("{tag}: the outbox drained early"));
            let label = seeded.iter().find(|(id, _)| *id == claimed).map_or_else(
                || panic!("{tag}: claimed an event the fixture never seeded"),
                |(_, label)| label.clone(),
            );
            drained.push(claimed);
            seed_event(
                &mut conn,
                exec,
                100 + n,
                family.resolved[0],
                &format!("{{\"{key}\": \"{label}\"}}"),
            )
            .await;
        }

        assert_eq!(
            drained, expected,
            "{tag}: the outbox must drain oldest request first"
        );
        assert!(
            claim_one(
                &mut conn,
                &with_literal_binds((family.query)(), &[SHARD_ID], &[])
            )
            .await
            .is_none(),
            "{tag}: a fully resolved outbox must claim nothing"
        );
    }
}

// ---------------------------------------------------------------------------
// Evidence capture
// ---------------------------------------------------------------------------

/// The fixture the issue's profile used: RUNNING executions with long
/// histories, plus a terminal tail.
const EVIDENCE_RUNNING_EXECUTIONS: usize = 5_000;
const EVIDENCE_EVENTS_PER_RUNNING: usize = 200;
const EVIDENCE_TERMINAL_EXECUTIONS: usize = 2_000;
const EVIDENCE_EVENTS_PER_TERMINAL: usize = 10;
const EVIDENCE_PENDING_REQUESTS: usize = 50;

/// The four indexes the migration adds, as the evidence capture rebuilds them.
///
/// Deliberately not an `include_str!` of the migration. `migration_hygiene`
/// forbids a test fixture from building schema out of a migration bundle, and
/// this capture needs four indexes rather than a schema.
///
/// Copying DDL into a test invites drift, so the capture does not rely on this
/// text being right. `setup_bench_db` applies the real migration first, so the
/// capture records what the migration built, drops it, rebuilds from here, and
/// compares. A difference fails the capture instead of publishing numbers for
/// indexes the engine does not ship.
const CANDIDATE_INDEXES_SQL: &str = "\
    CREATE INDEX idx_harvest_events_external_outbox_pending \
        ON harvest_events (event_type, timestamp, id) \
        WHERE event_type IN ('ExternalSignalRequested', 'ExternalCancelRequested', 'ExternalAwaitRequested'); \
    CREATE INDEX idx_harvest_events_external_signal_resolved \
        ON harvest_events (workflow_exec_id, (event_data->'data'->>'signal_id')) \
        WHERE event_type IN ('ExternalSignalDelivered', 'ExternalSignalFailed'); \
    CREATE INDEX idx_harvest_events_external_cancel_resolved \
        ON harvest_events (workflow_exec_id, (event_data->'data'->>'cancel_id')) \
        WHERE event_type IN ('ExternalCancelDelivered', 'ExternalCancelFailed'); \
    CREATE INDEX idx_harvest_events_external_await_resolved \
        ON harvest_events (workflow_exec_id, (event_data->'data'->>'await_id')) \
        WHERE event_type IN ('ExternalAwaitResolved', 'ExternalAwaitFailed');";

#[derive(QueryableByName, Debug, PartialEq, Eq)]
struct IndexDefinition {
    #[diesel(sql_type = Text)]
    name: String,
    #[diesel(sql_type = Text)]
    definition: String,
}

/// Report every external-outbox index on `harvest_events`, name and body.
///
/// The body comes from `pg_get_indexdef`, so two builds of the same index
/// compare equal whatever spelling produced them.
async fn outbox_index_definitions(conn: &mut AsyncPgConnection) -> Vec<IndexDefinition> {
    diesel::sql_query(
        "SELECT c.relname AS name, pg_get_indexdef(c.oid) AS definition \
         FROM pg_class c \
         JOIN pg_index i ON i.indexrelid = c.oid \
         WHERE i.indrelid = 'harvest_events'::regclass \
           AND c.relname LIKE 'idx_harvest_events_external_%' \
         ORDER BY c.relname",
    )
    .load(conn)
    .await
    .expect("read the outbox index definitions")
}

/// Regenerate `docs/perf-artifacts/external-outbox-scan/`.
///
/// Captures the before and after form of one drain: the legacy query against
/// an unindexed table, then the rewritten query against the four partial
/// indexes. Both drains run over the same fixture, and both must resolve the
/// same request set.
///
/// `#[ignore]`d, because it seeds over a million event rows.
///
/// Needs `HARVEST_TEST_DATABASE_URL` (an admin connection string) or a
/// reachable Docker daemon for `claim_bench_support::db::setup_bench_db`'s
/// testcontainer fallback.
#[tokio::test]
#[ignore = "seeds a 1.02M-event fixture; run via scripts/external_outbox_scan_perf_repro.sh"]
async fn zz_capture_external_outbox_scan_evidence() {
    let bench = match claim_bench_db::setup_bench_db().await {
        Ok(b) => b,
        Err(reason) => {
            eprintln!("no database reachable; nothing captured: {}", reason.0);
            return;
        }
    };

    let out_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("autumn-harvest/ has a workspace-root parent")
        .join("docs")
        .join("perf-artifacts")
        .join("external-outbox-scan");
    std::fs::create_dir_all(&out_dir).expect("create artifact output directory");

    let mut conn = claim_bench_db::connect(&bench.url).await;
    // `pg_stat_statements` is preloaded cluster-wide, but its view still has
    // to be created in this database before it reports anything.
    diesel::sql_query("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        .execute(&mut conn)
        .await
        .ok();

    let family = &FAMILIES[0];
    assert_ne!(
        (family.query)(),
        legacy_claim_query(family),
        "the before leg replays the pre-#1486 oracle; it is now identical to \
         the shipped query, so this capture would compare nothing"
    );
    seed_evidence_fixture(&mut conn).await;

    // `setup_bench_db` runs every migration, so the four indexes already
    // exist. Record what the migration built before dropping them, so the
    // rebuild below can be checked against it.
    let shipped_indexes = outbox_index_definitions(&mut conn).await;
    assert_eq!(
        shipped_indexes.len(),
        4,
        "the migration must have built four indexes for this capture to check its rebuild"
    );

    // Drop them for the before capture, so this test reproduces the pre-fix
    // baseline on either side of the migration.
    conn.batch_execute(
        "DROP INDEX IF EXISTS idx_harvest_events_external_outbox_pending; \
         DROP INDEX IF EXISTS idx_harvest_events_external_signal_resolved; \
         DROP INDEX IF EXISTS idx_harvest_events_external_cancel_resolved; \
         DROP INDEX IF EXISTS idx_harvest_events_external_await_resolved;",
    )
    .await
    .expect("drop the candidate indexes for a clean before capture");
    analyze(&mut conn).await;

    let before = capture_drain(
        &mut conn,
        &out_dir,
        "before",
        &legacy_claim_query(family),
        family,
    )
    .await;

    reset_evidence_outbox(&mut conn, family).await;
    conn.batch_execute(CANDIDATE_INDEXES_SQL)
        .await
        .expect("build the candidate indexes");
    assert_eq!(
        outbox_index_definitions(&mut conn).await,
        shipped_indexes,
        "the rebuilt indexes differ from the ones the migration builds, so this \
         capture would measure something the engine does not ship -- reconcile \
         CANDIDATE_INDEXES_SQL with the migration"
    );
    analyze(&mut conn).await;

    let after = capture_drain(&mut conn, &out_dir, "after", (family.query)(), family).await;

    assert_eq!(
        before, after,
        "before and after must resolve the same request set -- only the \
         schema and the query shape changed, not which rows are claimed"
    );
    eprintln!(
        "equivalence confirmed over {} drained requests. Artifacts in {}",
        before.len(),
        out_dir.display()
    );
}

async fn seed_evidence_fixture(conn: &mut AsyncPgConnection) {
    conn.batch_execute(&format!(
        "INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name, \
              started_at, created_at) \
         SELECT gen_random_uuid(), 'outbox_evidence_wf', 'outbox_evidence_wf_' || gs, \
                gen_random_uuid(), {SHARD_ID}, 'RUNNING', '{{}}'::jsonb, 'default', NOW(), NOW() \
         FROM generate_series(1, {EVIDENCE_RUNNING_EXECUTIONS}) gs; \
         INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name, \
              started_at, created_at) \
         SELECT gen_random_uuid(), 'outbox_evidence_done_wf', 'outbox_evidence_done_wf_' || gs, \
                gen_random_uuid(), {SHARD_ID}, 'COMPLETED', '{{}}'::jsonb, 'default', NOW(), NOW() \
         FROM generate_series(1, {EVIDENCE_TERMINAL_EXECUTIONS}) gs;"
    ))
    .await
    .expect("seed evidence executions");

    for (name, per_exec) in [
        ("outbox_evidence_wf", EVIDENCE_EVENTS_PER_RUNNING),
        ("outbox_evidence_done_wf", EVIDENCE_EVENTS_PER_TERMINAL),
    ] {
        conn.batch_execute(&format!(
            "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp) \
             SELECT e.id, gs, \
                    (ARRAY['ActivityScheduled','ActivityStarted','ActivityCompleted','WorkflowTaskScheduled'])[1 + (gs % 4)], \
                    jsonb_build_object('type', 'ActivityScheduled', \
                                       'data', jsonb_build_object('activity_id', 'a' || gs)), \
                    NOW() - (gs || ' seconds')::interval \
             FROM harvest_workflow_executions e, generate_series(1, {per_exec}) gs \
             WHERE e.workflow_name = '{name}';"
        ))
        .await
        .expect("seed evidence history");
    }

    reset_evidence_outbox(conn, &FAMILIES[0]).await;
}

/// Return the outbox to its pre-drain state.
///
/// The before capture drains the outbox and appends a delivery marker per
/// request. The after capture has to start from the same place.
///
/// The `DELETE` is scoped to this fixture's own workflow names. Only the
/// evidence capture calls this, and only against the throwaway database
/// `setup_bench_db` provisions. An unscoped delete of three event types
/// would be destructive if that ever changed.
async fn reset_evidence_outbox(conn: &mut AsyncPgConnection, family: &OutboxFamily) {
    let types = [family.requested, family.resolved[0], family.resolved[1]]
        .map(|t| format!("'{t}'"))
        .join(", ");
    conn.batch_execute(&format!(
        "DELETE FROM harvest_events e USING harvest_workflow_executions x \
         WHERE x.id = e.workflow_exec_id \
           AND x.workflow_name IN ('outbox_evidence_wf', 'outbox_evidence_done_wf') \
           AND e.event_type IN ({types}); \
         INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp) \
         SELECT e.id, 900000, '{requested}', \
                jsonb_build_object('type', '{requested}', \
                                   'data', jsonb_build_object('{key}', 'pending-' || e.workflow_id)), \
                NOW() \
         FROM (SELECT * FROM harvest_workflow_executions \
               WHERE workflow_name = 'outbox_evidence_wf' \
               ORDER BY workflow_id LIMIT {EVIDENCE_PENDING_REQUESTS}) e;",
        requested = family.requested,
        key = family.id_key
    ))
    .await
    .expect("reset the evidence outbox");
}

/// Drain the outbox under `sql`, and write the plan and the statement
/// statistics for that drain.
///
/// Returns the sorted correlation ids the drain resolved, so the caller can
/// prove the two forms claim the same set.
async fn capture_drain(
    conn: &mut AsyncPgConnection,
    out_dir: &std::path::Path,
    label: &str,
    sql: &str,
    family: &OutboxFamily,
) -> Vec<String> {
    let claim = with_literal_binds(sql, &[SHARD_ID], &[]);

    let plan = explain_analyzed(conn, &claim).await;
    std::fs::write(out_dir.join(format!("{label}.explain.txt")), &plan)
        .expect("write the plan artifact");

    // Scoped to this database, so a shared cluster's other tenants keep their
    // own statistics.
    conn.batch_execute(
        "SELECT pg_stat_statements_reset(0, (SELECT oid FROM pg_database WHERE datname = current_database()), 0);",
    )
    .await
    .expect("reset statement statistics for this database");

    let mut resolved = Vec::new();
    // Bounded, so a marker that stops resolving its own claim fails loudly
    // instead of spinning.
    for _ in 0..EVIDENCE_PENDING_REQUESTS * 2 {
        let rows: Vec<ClaimedRequest> = diesel::sql_query(format!(
            "SELECT e.workflow_exec_id, e.event_data->'data'->>'{}' AS correlation_id \
             FROM ({claim}) e",
            family.id_key
        ))
        .load(conn)
        .await
        .expect("claim one pending request");
        let Some(row) = rows.into_iter().next() else {
            break;
        };
        conn.batch_execute(&format!(
            "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp) \
             VALUES ('{exec}', {marker}, '{resolved_type}', \
                     jsonb_build_object('type', '{resolved_type}', \
                                        'data', jsonb_build_object('{key}', '{correlation}')), NOW())",
            exec = row.workflow_exec_id,
            marker = 900_000 + i32::try_from(resolved.len()).expect("a small drain count") + 1,
            resolved_type = family.resolved[0],
            key = family.id_key,
            correlation = row.correlation_id
        ))
        .await
        .expect("append the delivery marker");
        resolved.push(row.correlation_id);
    }
    assert_eq!(
        resolved.len(),
        EVIDENCE_PENDING_REQUESTS,
        "the drain must resolve every seeded request, and only those"
    );

    let stats = statement_statistics(conn).await;
    std::fs::write(
        out_dir.join(format!("{label}.pg_stat_statements.txt")),
        &stats,
    )
    .expect("write the statistics artifact");
    eprintln!("== {label} ==\n{stats}");

    resolved.sort();
    resolved
}

#[derive(QueryableByName, Debug)]
struct ClaimedRequest {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    workflow_exec_id: uuid::Uuid,
    #[diesel(sql_type = Text)]
    correlation_id: String,
}

async fn explain_analyzed(conn: &mut AsyncPgConnection, sql: &str) -> String {
    let lines: Vec<PlanLine> = diesel::sql_query(format!(
        "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF) {sql}"
    ))
    .load(conn)
    .await
    .unwrap_or_else(|e| panic!("EXPLAIN failed: {e}\n{sql}"));
    lines
        .into_iter()
        .map(|l| l.line)
        .collect::<Vec<_>>()
        .join("\n")
}

async fn statement_statistics(conn: &mut AsyncPgConnection) -> String {
    let rows: Vec<StatementStat> = diesel::sql_query(
        "SELECT calls, shared_blks_hit + shared_blks_read AS buffers, left(query, 200) AS query \
         FROM pg_stat_statements \
         WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database()) \
         ORDER BY shared_blks_hit + shared_blks_read DESC \
         LIMIT 10",
    )
    .load(conn)
    .await
    .expect("read statement statistics");
    let total: i64 = rows.iter().map(|r| r.buffers).sum();
    let mut out = format!("total buffers across the listed statements: {total}\n\n");
    for row in rows {
        writeln!(
            out,
            "calls={:<5} buffers={:<10} {}",
            row.calls,
            row.buffers,
            row.query.replace('\n', " ")
        )
        .expect("writing to a String cannot fail");
    }
    out
}

#[derive(QueryableByName, Debug)]
struct StatementStat {
    #[diesel(sql_type = BigInt)]
    calls: i64,
    #[diesel(sql_type = BigInt)]
    buffers: i64,
    #[diesel(sql_type = Text)]
    query: String,
}
