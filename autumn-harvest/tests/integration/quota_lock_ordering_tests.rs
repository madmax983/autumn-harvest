//! Quota-lock ABBA-deadlock mechanism — issue #1228, Finding 2.
//!
//! `create_detached_child_executions` (`worker.rs`) locked each detached
//! child's quota key one at a time, in raw command order. It did not
//! pre-acquire every distinct `(workflow_name, quota_key)` pair first, in
//! one sorted order, the way the awaited-child fan-out already does.
//!
//! Two concurrent parents can spawn detached children under the same keys
//! in OPPOSITE command order. Each could then hold one key while it waits
//! on the other. That is a classic ABBA wait-for cycle. Postgres resolves
//! it by aborting one transaction with a raw `deadlock_detected` error.
//!
//! `create_detached_child_executions` itself is private, so these tests
//! cannot drive it directly. Instead they exercise
//! [`autumn_harvest::quota::lock_quota_key`] -- the exact advisory-lock
//! primitive both fan-outs call. They drive it under a deterministic,
//! manually-sequenced two-connection interleaving. This proves the two
//! properties the fix depends on:
//!
//! - Unsorted concurrent acquisition of the SAME two keys in OPPOSITE order
//!   CAN deadlock. This is the pre-fix hazard.
//! - Sorted (same-order) concurrent acquisition of the SAME two keys NEVER
//!   deadlocks. This is the property the new `BTreeSet` pre-acquisition
//!   pass relies on.
//!
//! Both sequences are forced by explicit ordering of awaits across the two
//! connections, not by timing -- so neither test is flaky.

#![cfg(feature = "db")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::missing_panics_doc)]

use autumn_harvest::error::HarvestError;
use autumn_harvest::quota::lock_quota_key;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use uuid::Uuid;

use crate::integration_e2e::setup_test_database_url_or_env;

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url)
        .await
        .expect("connect to test database")
}

async fn begin(conn: &mut AsyncPgConnection) {
    diesel::sql_query("BEGIN")
        .execute(conn)
        .await
        .expect("begin transaction");
}

async fn end(conn: &mut AsyncPgConnection) {
    // Best-effort. A connection whose transaction already aborted (the
    // deadlock victim) rejects a plain `ROLLBACK` follow-up the same way a
    // real one would. This only needs to leave the session out of an open
    // transaction before the connection drops.
    let _ = diesel::sql_query("ROLLBACK").execute(conn).await;
}

fn is_deadlock(err: &HarvestError) -> bool {
    matches!(err, HarvestError::Database(msg) if msg.to_lowercase().contains("deadlock"))
}

/// A unique workflow name per test. `lock_quota_key`'s advisory-lock
/// namespace is `"quota:{workflow_name}:{key}"`. Distinct names keep
/// concurrently-run tests from ever colliding on the same lock.
fn leaked(prefix: &str) -> &'static str {
    Box::leak(format!("{prefix}_{}", Uuid::new_v4().simple()).into_boxed_str())
}

/// The pre-fix hazard: two transactions locking the SAME two keys in
/// OPPOSITE order deadlock. Forced deterministically:
///
/// 1. conn1 locks A. conn2 locks B (both uncontended).
/// 2. conn1 asks for B (blocks -- conn2 holds it).
/// 3. Once conn1's request is outstanding, conn2 asks for A -- now each
///    transaction holds what the other wants, a genuine ABBA cycle.
///
/// Postgres's deadlock detector aborts exactly one side with a raw
/// `deadlock_detected` error; the other proceeds normally once the abort
/// releases its locks.
#[tokio::test]
async fn opposite_order_lock_acquisition_deadlocks() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let wf = leaked("lock_order_abba");

    let mut conn1 = connect(&url).await;
    let mut conn2 = connect(&url).await;
    begin(&mut conn1).await;
    begin(&mut conn2).await;

    lock_quota_key(&mut conn1, wf, "A")
        .await
        .expect("conn1 locks A uncontended");
    lock_quota_key(&mut conn2, wf, "B")
        .await
        .expect("conn2 locks B uncontended");

    // conn1 now wants B, held by conn2 -- this blocks. Drive it on its own
    // task, and join it after the cycle is closed below.
    let conn1_wants_b =
        tokio::spawn(async move { (lock_quota_key(&mut conn1, wf, "B").await, conn1) });

    // Give conn1's request time to actually reach the server and start
    // waiting. This is a one-directional wait for our own setup step to
    // land. It is not a race against the mechanism under test. That
    // mechanism is closed by explicit ordering below, not by timing.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // conn2 now wants A, held by (blocked) conn1 -- the cycle is closed.
    // Postgres's deadlock_timeout (default 1s) bounds how long this can
    // block before the detector aborts a side.
    let conn2_wants_a = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        lock_quota_key(&mut conn2, wf, "A"),
    )
    .await
    .expect("deadlock detector must resolve the cycle well within 10s");

    let (conn1_result, mut conn1) = conn1_wants_b.await.expect("conn1 task join");

    let conn1_deadlocked = conn1_result.as_ref().is_err_and(is_deadlock);
    let conn2_deadlocked = conn2_wants_a.as_ref().is_err_and(is_deadlock);
    assert!(
        conn1_deadlocked ^ conn2_deadlocked,
        "exactly one side of a genuine ABBA cycle must be aborted with a \
         deadlock error -- conn1: {conn1_result:?}, conn2: {conn2_wants_a:?}"
    );

    end(&mut conn1).await;
    end(&mut conn2).await;
}

/// The fix's own invariant: two transactions locking the SAME two keys in
/// the SAME (sorted) order never deadlock, even under real concurrency.
/// The later one simply blocks until the earlier one releases, then
/// proceeds. This is the property `create_detached_child_executions`'s
/// `BTreeSet` pre-acquisition pass relies on to be deadlock-free.
///
/// Forced deterministically: conn1 locks A then B. conn2 asks for the
/// SAME first key (A) and blocks behind conn1, never reaching B first.
/// So no cycle can form, regardless of how long conn1 holds both.
#[tokio::test]
async fn same_order_lock_acquisition_never_deadlocks() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let wf = leaked("lock_order_sorted");

    let mut conn1 = connect(&url).await;
    let mut conn2 = connect(&url).await;
    begin(&mut conn1).await;
    begin(&mut conn2).await;

    lock_quota_key(&mut conn1, wf, "A")
        .await
        .expect("conn1 locks A uncontended");

    // conn2 wants A first (SAME order as conn1), then B -- this blocks on A
    // and only reaches the B request once conn1 releases below.
    let conn2_task = tokio::spawn(async move {
        let a = lock_quota_key(&mut conn2, wf, "A").await;
        let b = lock_quota_key(&mut conn2, wf, "B").await;
        (a, b, conn2)
    });

    // Give conn2's A-request time to actually reach the server and start
    // waiting, mirroring the sibling test's setup wait.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // conn1 takes B too -- uncontended, since conn2 is still blocked on A
    // and has not asked for B yet. No cycle is possible: conn2 never holds
    // anything conn1 is waiting on.
    lock_quota_key(&mut conn1, wf, "B")
        .await
        .expect("conn1 locks B uncontended -- conn2 has not reached it yet");

    end(&mut conn1).await; // Releases A and B, unblocking conn2's A request.

    let (a_result, b_result, mut conn2) =
        tokio::time::timeout(std::time::Duration::from_secs(10), conn2_task)
            .await
            .expect("conn2 must proceed once conn1 releases, never wait for a detector timeout")
            .expect("conn2 task join");

    a_result.expect("conn2's A request must succeed once conn1 releases it");
    b_result.expect("conn2's B request must succeed -- never contended, so never a deadlock");

    end(&mut conn2).await;
}
