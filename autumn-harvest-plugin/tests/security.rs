use std::collections::HashMap;

use autumn_harvest_plugin::api::{HarvestApiState, harvest_api_router};
use autumn_harvest_plugin::ui::harvest_ui_router;
use autumn_web::AppState;
use autumn_web::auth::RequireAuth;
use autumn_web::reexports::axum::body::Body;
use autumn_web::reexports::http::{Method, Request, StatusCode};
use autumn_web::session::Session;
use tower::ServiceExt;

/// Build an unauthenticated test app (no middleware applied).
fn unauthenticated_app() -> impl tower::Service<
    Request<Body>,
    Response = autumn_web::reexports::axum::response::Response,
    Error = std::convert::Infallible,
    Future = impl std::future::Future,
> + Clone {
    harvest_api_router(HarvestApiState::new()).with_state(AppState::for_test())
}

fn app_with_api_state(
    api_state: HarvestApiState,
) -> impl tower::Service<
    Request<Body>,
    Response = autumn_web::reexports::axum::response::Response,
    Error = std::convert::Infallible,
    Future = impl std::future::Future,
> + Clone {
    harvest_api_router(api_state).with_state(AppState::for_test())
}

fn unauthenticated_app_with_ui() -> impl tower::Service<
    Request<Body>,
    Response = autumn_web::reexports::axum::response::Response,
    Error = std::convert::Infallible,
    Future = impl std::future::Future,
> + Clone {
    let api_state = HarvestApiState::new();
    harvest_api_router(api_state.clone())
        .nest("/ui", harvest_ui_router(api_state))
        .with_state(AppState::for_test())
}

/// Build a test app protected by `RequireAuth`.
fn authenticated_app() -> impl tower::Service<
    Request<Body>,
    Response = autumn_web::reexports::axum::response::Response,
    Error = std::convert::Infallible,
    Future = impl std::future::Future,
> + Clone {
    harvest_api_router(HarvestApiState::new())
        .route_layer(RequireAuth::new("user_id"))
        .with_state(AppState::for_test())
}

fn get(uri: &str) -> Request<Body> {
    Request::builder()
        .method(Method::GET)
        .uri(uri)
        .body(Body::empty())
        .unwrap()
}

fn post_json(uri: &str, body: &str) -> Request<Body> {
    Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header("Content-Type", "application/json")
        .body(Body::from(body.to_owned()))
        .unwrap()
}

fn post_json_with_session(uri: &str, body: &str, session_key: &str) -> Request<Body> {
    let mut request = post_json(uri, body);
    let mut data = HashMap::new();
    data.insert(session_key.to_string(), "operator-1".to_string());
    request.extensions_mut().insert(Session::new_for_test(
        "harvest-test-session".to_string(),
        data,
    ));
    request
}

fn patch_json(uri: &str, body: &str) -> Request<Body> {
    Request::builder()
        .method(Method::PATCH)
        .uri(uri)
        .header("Content-Type", "application/json")
        .body(Body::from(body.to_owned()))
        .unwrap()
}

fn delete(uri: &str) -> Request<Body> {
    Request::builder()
        .method(Method::DELETE)
        .uri(uri)
        .body(Body::empty())
        .unwrap()
}

// ── Without authentication middleware ────────────────────────────────────────
//
// When `harvest_api_router` is mounted without any auth layer, ordinary routes
// remain directly reachable while high-impact management operations use
// Harvest's built-in guard.

#[tokio::test]
async fn eris_unauthenticated_health_is_accessible() {
    let app = unauthenticated_app();
    let res = app.oneshot(get("/health")).await.unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn eris_unauthenticated_list_workflows_is_accessible() {
    let app = unauthenticated_app();
    let res = app.oneshot(get("/workflows")).await.unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn eris_unauthenticated_start_workflow_is_accessible() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json("/workflows/my-workflow/start", "{}"))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn eris_unauthenticated_start_workflow_terminate_if_running_is_blocked() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/workflows/my-workflow/start",
            r#"{"reuse_policy": "terminate_if_running"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

// A `conflict_policy = terminate_existing` request (with ANY reuse policy)
// resolves to `Terminate` — it can cancel a live RUNNING/PAUSED prior — so it is
// admin-gated exactly like `terminate_if_running` with the default conflict.
#[tokio::test]
async fn eris_unauthenticated_start_workflow_terminate_existing_conflict_is_blocked() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/workflows/my-workflow/start",
            r#"{"conflict_policy": "terminate_existing"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

// Narrowing proof (issue #685): the flagship non-admin idempotent-starter shape
// `reuse = terminate_if_running` + `conflict = use_existing` resolves to
// `Attach` (return the existing handle) and provably CANNOT cancel a live run,
// so it must NOT be admin-gated — it is allowed through the auth gate (and then
// fails later for an unrelated boot-window reason, exactly like every other
// "allowed through" eris start case, so we assert only "not blocked by auth").
#[tokio::test]
async fn eris_unauthenticated_start_workflow_terminate_if_running_plus_use_existing_is_allowed() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/workflows/my-workflow/start",
            r#"{"reuse_policy": "terminate_if_running", "conflict_policy": "use_existing"}"#,
        ))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

// Narrowing proof: `reuse = terminate_if_running` + `conflict = fail` resolves to
// `Fail` (`Err(AlreadyExists)` over a live prior) — it touches no live work — so
// it is likewise NOT admin-gated.
#[tokio::test]
async fn eris_unauthenticated_start_workflow_terminate_if_running_plus_fail_is_allowed() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/workflows/my-workflow/start",
            r#"{"reuse_policy": "terminate_if_running", "conflict_policy": "fail"}"#,
        ))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

// Narrowing proof: a `conflict_policy = use_existing` (attach) start with the
// default reuse policy resolves to `Attach` — non-destructive — and is NOT
// admin-gated.
#[tokio::test]
async fn eris_unauthenticated_start_workflow_use_existing_conflict_is_allowed() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/workflows/my-workflow/start",
            r#"{"conflict_policy": "use_existing"}"#,
        ))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

// issue #685 review (Codex P2): the early cancel-capable admin gate must NOT
// pre-empt a request that carries an idempotency key. The #808 committed-replay
// claim is keyed on `(workflow_name, idempotency_key)` ALONE — independent of the
// reuse/conflict policy and the caller's auth — so an ADMIN may legitimately have
// committed a keyed `terminate_if_running` start that a later NON-admin retry
// (same key) must replay as a `200`, not a `401`. A keyed cancel-capable request
// therefore defers past the early gate to the downstream path. In this no-DB
// harness the downstream path fails-closed at the runtime-not-started check (a
// non-`401` status), which is exactly the "no longer early-`401`'d" proof: the
// UNKEYED `terminate_if_running` case above asserts `401`, this KEYED case must
// NOT. (A genuinely-fresh keyed `Terminate` by a non-admin is still `401`'d
// downstream by the authoritative gate, which runs AFTER the committed-replay
// probe — that requires a DB and is covered by the #808 integration tests.)
#[tokio::test]
async fn eris_keyed_terminate_if_running_defers_past_early_gate() {
    let app = unauthenticated_app();
    let req = Request::builder()
        .method(Method::POST)
        .uri("/workflows/my-workflow/start")
        .header("Content-Type", "application/json")
        .header("Idempotency-Key", "replay-key-1")
        .body(Body::from(
            r#"{"reuse_policy": "terminate_if_running"}"#.to_owned(),
        ))
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

// Companion narrowing proof for the other cancel-capable shape: a keyed
// `conflict_policy = terminate_existing` request likewise defers past the early
// gate (the UNKEYED variant above asserts `401`).
#[tokio::test]
async fn eris_keyed_terminate_existing_conflict_defers_past_early_gate() {
    let app = unauthenticated_app();
    let req = Request::builder()
        .method(Method::POST)
        .uri("/workflows/my-workflow/start")
        .header("Content-Type", "application/json")
        .header("Idempotency-Key", "replay-key-2")
        .body(Body::from(
            r#"{"conflict_policy": "terminate_existing"}"#.to_owned(),
        ))
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn eris_start_workflow_terminate_if_running_honors_configured_session_key() {
    let api_state = HarvestApiState::new();
    api_state.set_deployment_profile("dev");
    api_state.set_admin_auth_session_key("operator_id");
    let app = app_with_api_state(api_state);

    let res = app
        .oneshot(post_json_with_session(
            "/workflows/my-workflow/start",
            r#"{"reuse_policy": "terminate_if_running"}"#,
            "operator_id",
        ))
        .await
        .unwrap();

    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn eris_unauthenticated_list_dags_is_accessible() {
    let app = unauthenticated_app();
    let res = app.oneshot(get("/dags")).await.unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn eris_unauthenticated_get_workflow_is_accessible() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(get("/workflows/00000000-0000-0000-0000-000000000001"))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn eris_unauthenticated_signal_workflow_is_accessible() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/workflows/00000000-0000-0000-0000-000000000001/signal/approve",
            r#"{"approved": true}"#,
        ))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn eris_unauthenticated_cancel_workflow_is_blocked() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/workflows/00000000-0000-0000-0000-000000000001/cancel",
            r#"{"reason": "operator request"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_fail_activity_now_is_blocked() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/workflows/00000000-0000-0000-0000-000000000001/activities/00000000-0000-0000-0000-000000000002/fail-now",
            r#"{"reason": "operator request"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

/// Issue #619 AC1/AC6: pausing or resuming a task queue is an operator action
/// that must be admin-gated and audited — never reachable unauthenticated.
#[tokio::test]
async fn eris_unauthenticated_queue_pause_is_blocked() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/admin/queues/email-workers/pause",
            r#"{"reason": "SMTP outage"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_queue_resume_is_blocked() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json("/admin/queues/email-workers/resume", "{}"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

/// Issue #807: pausing dispatch for an activity TYPE is a fleet-wide kill
/// switch for that downstream — the most surgical, and therefore the most
/// reachable-for, of the hold controls. It must be admin-gated and audited,
/// never reachable unauthenticated.
///
/// The body is deliberately optional on this route (containment must not wait
/// on paperwork), which makes it the easiest of the mutations to fire
/// accidentally — a bare POST is a valid hold — so the gate matters more here,
/// not less. Both cases below send a body-free POST for exactly that reason.
#[tokio::test]
async fn eris_unauthenticated_activity_pause_is_blocked() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json("/activities/charge_card/pause", ""))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_activity_resume_is_blocked() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json("/activities/charge_card/resume", ""))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

/// Issue #807: unlike the queue-pause sibling, whose paused-queue list is an
/// ungated read, BOTH activity reads are admin-gated — they surface
/// operator-authored hold reasons (free text written mid-incident) and the full
/// shape of the registered activity catalogue, which is a map of the fleet's
/// downstream dependencies. Pinned so the gate cannot be dropped by someone
/// pattern-matching on the queue sibling.
#[tokio::test]
async fn eris_unauthenticated_activity_list_is_blocked() {
    let app = unauthenticated_app();
    let res = app.oneshot(get("/activities")).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_activity_get_is_blocked() {
    let app = unauthenticated_app();
    let res = app.oneshot(get("/activities/charge_card")).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_workflow_logs_is_blocked() {
    // Durable per-execution author log lines (issue #790): admin-only read —
    // a log message is free-form author text that routinely carries business
    // detail, so it takes the /awaitables posture, not the execution-row one.
    let app = unauthenticated_app();
    let res = app
        .oneshot(get("/workflows/00000000-0000-0000-0000-000000000001/logs"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_awaitables_is_blocked() {
    // Open-awaitables diagnostic (issue #615): admin-only read (the
    // eligibility endpoints' posture).
    let app = unauthenticated_app();
    let res = app
        .oneshot(get(
            "/workflows/00000000-0000-0000-0000-000000000001/awaitables",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_replay_diagnosis_is_blocked() {
    // Single-execution replay diagnosis (issue #614): admin-only read.
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/workflows/00000000-0000-0000-0000-000000000001/replay-diagnosis",
            "{}",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_rerun_is_blocked() {
    // Operator re-run of a terminal workflow (issue #777): starts a brand-new
    // execution from a source's recorded start params — admin-only mutation.
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/workflows/00000000-0000-0000-0000-000000000001/rerun",
            r"{}",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_legal_hold_set_is_blocked() {
    // Per-execution legal hold (issue #747): admin-only mutation.
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/workflows/00000000-0000-0000-0000-000000000001/legal-hold",
            r#"{"reason": "litigation hold"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_legal_hold_release_is_blocked() {
    // Per-execution legal hold release (issue #747): admin-only mutation.
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/workflows/00000000-0000-0000-0000-000000000001/legal-hold/release",
            "{}",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_query_workflow_is_accessible() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(get(
            "/workflows/00000000-0000-0000-0000-000000000001/query/status",
        ))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn eris_unauthenticated_list_dag_runs_is_accessible() {
    let app = unauthenticated_app();
    let res = app.oneshot(get("/dags/my-dag/runs")).await.unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn eris_unauthenticated_trigger_dag_is_accessible() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json("/dags/my-dag/trigger", "{}"))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn eris_unauthenticated_patch_dag_is_accessible() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(patch_json("/dags/my-dag", r#"{"paused": true}"#))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

// ── With RequireAuth middleware ───────────────────────────────────────────────
//
// When the router is wrapped with `RequireAuth`, every endpoint must reject
// unauthenticated requests with 401 before any handler logic runs.

#[tokio::test]
async fn eris_require_auth_blocks_health() {
    let app = authenticated_app();
    let res = app.oneshot(get("/health")).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_list_workflows() {
    let app = authenticated_app();
    let res = app.oneshot(get("/workflows")).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_get_workflow() {
    let app = authenticated_app();
    let res = app
        .oneshot(get("/workflows/00000000-0000-0000-0000-000000000001"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_workflow_children() {
    let app = authenticated_app();
    let res = app
        .oneshot(get(
            "/workflows/00000000-0000-0000-0000-000000000001/children",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_start_workflow() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json("/workflows/my-workflow/start", "{}"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_signal_workflow() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json(
            "/workflows/00000000-0000-0000-0000-000000000001/signal/approve",
            "{}",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_cancel_workflow() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json(
            "/workflows/00000000-0000-0000-0000-000000000001/cancel",
            "{}",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_query_workflow() {
    let app = authenticated_app();
    let res = app
        .oneshot(get(
            "/workflows/00000000-0000-0000-0000-000000000001/query/status",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_list_dags() {
    let app = authenticated_app();
    let res = app.oneshot(get("/dags")).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_list_dag_runs() {
    let app = authenticated_app();
    let res = app.oneshot(get("/dags/my-dag/runs")).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_trigger_dag() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json("/dags/my-dag/trigger", "{}"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_patch_dag() {
    let app = authenticated_app();
    let res = app
        .oneshot(patch_json("/dags/my-dag", r#"{"paused": true}"#))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_list_dead_letters_is_blocked() {
    let app = unauthenticated_app();
    let res = app.oneshot(get("/dead-letters")).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_admin_status_is_blocked() {
    // Issue #679: the rolled-up health summary is admin-gated; the built-in
    // guard rejects before the handler runs, so no DB is needed here.
    let app = unauthenticated_app();
    let res = app.oneshot(get("/admin/status")).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_replay_dead_letter_is_blocked() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/dead-letters/00000000-0000-0000-0000-000000000001/replay",
            "{}",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_bulk_replay_dead_letters_is_blocked() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json("/dead-letters/replay", r#"{"ids": []}"#))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_bulk_discard_dead_letters_is_blocked() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json("/dead-letters/discard", r#"{"ids": []}"#))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_vantage_dead_letters_page_is_blocked() {
    let app = unauthenticated_app_with_ui();
    let res = app.oneshot(get("/ui/dead-letters")).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_builtin_guard_honors_configured_session_key() {
    let api_state = HarvestApiState::new();
    api_state.set_deployment_profile("dev");
    api_state.set_admin_auth_session_key("operator_id");
    let app = app_with_api_state(api_state);

    let res = app
        .oneshot(post_json_with_session(
            "/dead-letters/replay",
            r#"{"ids": []}"#,
            "operator_id",
        ))
        .await
        .unwrap();

    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn eris_builtin_guard_does_not_accept_hard_coded_admin_id() {
    let api_state = HarvestApiState::new();
    api_state.set_deployment_profile("dev");
    api_state.set_admin_auth_session_key("operator_id");
    let app = app_with_api_state(api_state);

    let res = app
        .oneshot(post_json_with_session(
            "/dead-letters/replay",
            r#"{"ids": []}"#,
            "admin_id",
        ))
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_declared_outer_auth_boundary_skips_inner_session_check() {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    let app = app_with_api_state(api_state);

    let res = app
        .oneshot(post_json("/dead-letters/replay", r#"{"ids": []}"#))
        .await
        .unwrap();

    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn eris_require_auth_blocks_list_dead_letters() {
    let app = authenticated_app();
    let res = app.oneshot(get("/dead-letters")).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_replay_dead_letter() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json(
            "/dead-letters/00000000-0000-0000-0000-000000000001/replay",
            "{}",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

// ── Additional unauthenticated-accessible checks ──────────────────────────────
//
// Documents remaining routes that are intentionally still open without an
// outer middleware or one of Harvest's high-impact built-in guards.

#[tokio::test]
async fn eris_unauthenticated_reset_workflow_is_accessible() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/workflows/00000000-0000-0000-0000-000000000001/reset",
            "{}",
        ))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn eris_unauthenticated_retention_run_now_is_blocked() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json("/admin/retention/run-now", "{}"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_non_admin_session_is_blocked_in_non_dev_profile() {
    let api_state = HarvestApiState::new();
    let app = app_with_api_state(api_state);

    let mut request = post_json("/admin/retention/run-now", "{}");
    let mut data = HashMap::new();
    data.insert("user_id".to_string(), "some-user".to_string());
    request.extensions_mut().insert(Session::new_for_test(
        "harvest-test-session".to_string(),
        data,
    ));

    let res = app.oneshot(request).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_admin_session_is_allowed_in_non_dev_profile() {
    let api_state = HarvestApiState::new();
    let app = app_with_api_state(api_state);

    let mut request = post_json("/admin/retention/run-now", "{}");
    let mut data = HashMap::new();
    data.insert("user_id".to_string(), "some-user".to_string());
    data.insert("role".to_string(), "admin".to_string());
    request.extensions_mut().insert(Session::new_for_test(
        "harvest-test-session".to_string(),
        data,
    ));

    let res = app.oneshot(request).await.unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_create_schedule_is_accessible() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json("/admin/schedules/workflow", "{}"))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn eris_unauthenticated_submit_batch_cancel_operation_is_blocked() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/batch-operations",
            r#"{"action": "Cancel", "filter": {"workflow_name": "billing"}}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_submit_batch_terminate_operation_is_blocked() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/batch-operations",
            r#"{"action": "Terminate", "filter": {"workflow_name": "billing"}}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_worker_drain_is_accessible() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/workers/worker-abc/drain",
            r#"{"deadline_secs": 60}"#,
        ))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

// ── RequireAuth blocks all mutating routes (AC5) ──────────────────────────────
//
// Proves that every route in the "Mutating" security class returns 401 when
// the RequireAuth middleware is applied. Covers: workflow start/signal/cancel
// (existing), workflow reset, DLQ replay/discard (bulk + single, existing),
// schedule mutation, batch submission, retention run-now, external activity
// completion, and worker drain.

#[tokio::test]
async fn eris_require_auth_blocks_reset_workflow() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json(
            "/workflows/00000000-0000-0000-0000-000000000001/reset",
            "{}",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_admit_update() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json(
            "/workflows/00000000-0000-0000-0000-000000000001/update/approve",
            "{}",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_bulk_replay_dead_letters() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json("/dead-letters/replay", r#"{"ids": []}"#))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_bulk_discard_dead_letters() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json("/dead-letters/discard", r#"{"ids": []}"#))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_retention_run_now() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json("/admin/retention/run-now", "{}"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_create_schedule() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json("/admin/schedules/workflow", "{}"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_pause_schedule() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json(
            "/admin/schedules/00000000-0000-0000-0000-000000000001/pause",
            "{}",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_resume_schedule() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json(
            "/admin/schedules/00000000-0000-0000-0000-000000000001/resume",
            "{}",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_delete_schedule() {
    let app = authenticated_app();
    let res = app
        .oneshot(delete(
            "/admin/schedules/00000000-0000-0000-0000-000000000001",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_complete_external_activity() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json(
            "/activities/external/some-task-token/complete",
            r#"{"result": null}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_fail_external_activity() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json(
            "/activities/external/some-task-token/fail",
            r#"{"error": "timeout"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_heartbeat_external_activity() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json(
            "/activities/external/some-task-token/heartbeat",
            "{}",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_worker_drain() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json(
            "/workers/worker-abc/drain",
            r#"{"deadline_secs": 60}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_submit_batch_operation() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json("/batch-operations", "{}"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_trigger_schedule() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json(
            "/admin/schedules/00000000-0000-0000-0000-000000000001/trigger",
            "{}",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

// ── TTL'd runtime pacing overrides (issue #945) — admin-gated ────────────────
//
// All five pacing-override routes (2 rate-limit, 2 start-throttle, plus the
// read-only start-throttle-override lookup) carry `require_admin` at the
// route-registration site (`api.rs`'s `harvest_api_router`). Mirroring every
// other admin route in this file, an unauthenticated request never even
// reaches that per-route gate — `RequireAuth`, wrapping the whole router,
// already rejects it with 401 first. This proves the pacing-override routes
// are wired into the SAME auth chain as every other admin-gated route, not
// silently left unauthenticated.

#[tokio::test]
async fn eris_require_auth_blocks_set_rate_limit_override() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json(
            "/admin/rate-limits/send_email/override",
            r#"{"refill_rate": 100.0, "ttl_secs": 60}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_clear_rate_limit_override() {
    let app = authenticated_app();
    let res = app
        .oneshot(delete("/admin/rate-limits/send_email/override"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_set_start_throttle_override() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json(
            "/admin/start-throttle/onboard_user/override",
            r#"{"refill_per_sec": 100.0, "ttl_secs": 60}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_clear_start_throttle_override() {
    let app = authenticated_app();
    let res = app
        .oneshot(delete("/admin/start-throttle/onboard_user/override"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_get_start_throttle_override() {
    let app = authenticated_app();
    let res = app
        .oneshot(get("/admin/start-throttle/onboard_user/override"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

// ── Read-only operator role (issue #776) ──────────────────────────────────────
//
// Headline success-metric test (AC1/AC2, no DB): a synthetic router mounts a
// trivial 200-OK handler at EVERY autumn_harvest::audit::CLASSIFIED_ROUTES
// (method, path), wraps it in the read-only class-enforcement layer, and drives
// it with a read-only Session. Every Mutating route → 403; every
// ReadOnly/PublicSafe route → 200 (reaches the trivial handler). The success
// metric — "0 of N mutation routes reachable, N of N read routes reachable" —
// is asserted explicitly. Using URI-path matching (not MatchedPath) makes a
// top-level synthetic router faithful to nested production.

use autumn_harvest::audit::{CLASSIFIED_ROUTES, RouteClass};
use autumn_harvest_plugin::api::enforce_read_only_class;
use autumn_web::reexports::axum::Router;
use autumn_web::reexports::axum::middleware::from_fn;
use autumn_web::reexports::axum::routing::{MethodFilter, MethodRouter};

fn method_filter(m: &Method) -> MethodFilter {
    match *m {
        Method::GET => MethodFilter::GET,
        Method::POST => MethodFilter::POST,
        Method::PUT => MethodFilter::PUT,
        Method::PATCH => MethodFilter::PATCH,
        Method::DELETE => MethodFilter::DELETE,
        _ => panic!("unexpected method in CLASSIFIED_ROUTES: {m}"),
    }
}

/// Substitute each `{param}` segment with a non-static placeholder so the
/// concrete request path uniquely matches its own template (never a sibling
/// static route).
fn concrete_request_path(template: &str) -> String {
    let mut out = String::new();
    let mut k = 0u32;
    for seg in template.split('/') {
        if seg.is_empty() {
            continue;
        }
        out.push('/');
        if seg.starts_with('{') && seg.ends_with('}') {
            out.push_str("zzztestparam");
            out.push_str(&k.to_string());
            k += 1;
        } else {
            out.push_str(seg);
        }
    }
    if out.is_empty() {
        out.push('/');
    }
    out
}

/// Build the synthetic router: every `CLASSIFIED_ROUTES` (method, path) → 200,
/// wrapped in the read-only enforcement layer. Routes sharing a path are merged
/// into one `MethodRouter` (registering the same (method, path) twice would
/// panic; `CLASSIFIED_ROUTES` is dup-free).
fn synthetic_enforced_router() -> Router {
    let mut by_path: HashMap<&str, MethodRouter> = HashMap::new();
    for (template, _class) in CLASSIFIED_ROUTES {
        let (method_str, path) = template.split_once(' ').expect("well-formed template");
        let method: Method = method_str.parse().expect("known method");
        let filter = method_filter(&method);
        let entry = by_path.entry(path).or_default();
        let mr = std::mem::take(entry);
        *entry = mr.on(filter, || async { StatusCode::OK });
    }
    let mut router: Router = Router::new();
    for (path, mr) in by_path {
        router = router.route(path, mr);
    }
    router.layer(from_fn(enforce_read_only_class))
}

fn session_role(role: &str) -> Session {
    let mut data = HashMap::new();
    data.insert("role".to_string(), role.to_string());
    Session::new_for_test("harvest-role-test".to_string(), data)
}

async fn status_for(
    app: &Router,
    method: &Method,
    concrete_path: &str,
    session: Option<Session>,
) -> StatusCode {
    let mut req = Request::builder()
        .method(method.clone())
        .uri(concrete_path)
        .body(Body::empty())
        .unwrap();
    if let Some(session) = session {
        req.extensions_mut().insert(session);
    }
    app.clone().oneshot(req).await.unwrap().status()
}

#[tokio::test]
async fn eris_readonly_principal_reaches_all_reads_and_no_mutations() {
    let app = synthetic_enforced_router();

    let mut mutations_total = 0usize;
    let mut mutations_reachable_by_readonly = 0usize;
    let mut reads_total = 0usize;
    let mut reads_reachable_by_readonly = 0usize;

    for (template, class) in CLASSIFIED_ROUTES {
        let (method_str, path) = template.split_once(' ').unwrap();
        let method: Method = method_str.parse().unwrap();
        let concrete = concrete_request_path(path);

        // Read-only principal.
        let ro_status = status_for(
            &app,
            &method,
            &concrete,
            Some(session_role("harvest_readonly")),
        )
        .await;

        match class {
            RouteClass::Mutating => {
                mutations_total += 1;
                assert_eq!(
                    ro_status,
                    StatusCode::FORBIDDEN,
                    "read-only principal must be denied 403 on Mutating route {template}"
                );
                if ro_status != StatusCode::FORBIDDEN {
                    mutations_reachable_by_readonly += 1;
                }
            }
            RouteClass::ReadOnly | RouteClass::PublicSafe => {
                reads_total += 1;
                assert_eq!(
                    ro_status,
                    StatusCode::OK,
                    "read-only principal must reach (200) ReadOnly/PublicSafe route {template}"
                );
                if ro_status == StatusCode::OK {
                    reads_reachable_by_readonly += 1;
                }
            }
        }

        // Admin principal: never denied by the class layer, reaches everything.
        let admin_status = status_for(&app, &method, &concrete, Some(session_role("admin"))).await;
        assert_eq!(
            admin_status,
            StatusCode::OK,
            "admin principal must reach (200) every route {template}"
        );
    }

    // The 100% / 0% split — the issue's explicit success metric.
    assert_eq!(
        mutations_reachable_by_readonly, 0,
        "0 of {mutations_total} mutation routes must be reachable by a read-only principal"
    );
    assert_eq!(
        reads_reachable_by_readonly, reads_total,
        "all {reads_total} read routes must be reachable by a read-only principal"
    );
    assert!(
        mutations_total > 0 && reads_total > 0,
        "sanity: both sets non-empty"
    );
}

#[tokio::test]
async fn eris_readonly_post_for_body_read_routes_are_reachable() {
    // The two ReadOnly routes that use POST (query, retire) and the POST
    // preview must be reachable (200) by a read-only principal — enforcement is
    // keyed on the class table, never the HTTP verb.
    let app = synthetic_enforced_router();
    for concrete in [
        "/workflows/zzztestparam0/query/zzztestparam1",
        "/admin/build-routing/retire",
        "/admin/schedules/preview",
        "/workflows/by-id/zzztestparam0",
    ] {
        let status = status_for(
            &app,
            &Method::POST,
            concrete,
            Some(session_role("harvest_readonly")),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "read-only principal must reach ReadOnly POST route {concrete}"
        );
    }
}

#[tokio::test]
async fn eris_unmarked_and_admin_principals_are_not_verb_restricted() {
    // AC6-adjacent: a principal with no read-only marker (or an admin) is never
    // denied by the class layer, even on Mutating routes — the read-only
    // restriction is opt-in per-principal.
    let app = synthetic_enforced_router();
    let mutating = "/workflows/zzztestparam0/cancel";

    // No session at all → not read-only-restricted → reaches the handler.
    let anon = status_for(&app, &Method::POST, mutating, None).await;
    assert_eq!(
        anon,
        StatusCode::OK,
        "unauthenticated/unmarked reaches handler"
    );

    // Session with an unrelated marker → not read-only-restricted.
    let unmarked = status_for(
        &app,
        &Method::POST,
        mutating,
        Some({
            let mut d = HashMap::new();
            d.insert("user_id".to_string(), "u1".to_string());
            Session::new_for_test("s".to_string(), d)
        }),
    )
    .await;
    assert_eq!(
        unmarked,
        StatusCode::OK,
        "unmarked principal reaches handler"
    );

    // Admin marker → not read-only-restricted.
    let admin = status_for(&app, &Method::POST, mutating, Some(session_role("admin"))).await;
    assert_eq!(admin, StatusCode::OK, "admin reaches handler");
}

// H2 (issue #776): the headline synthetic test proves the class layer's verb
// decision, but not the *production wiring* the whole feature rests on — the
// enforce layer sitting INSIDE the `/api/harvest` nest (so it sees the
// mount-stripped path), and the `admin_auth_boundary=true` reconciliation that
// lets a read-only principal reach the ~15 `require_admin`-carrying ReadOnly
// routes (and the SSE stream's in-handler admin check). This test reproduces
// that wiring against the REAL `harvest_api_router` and drives full
// `/api/harvest/...` paths through the nest.

use autumn_web::reexports::axum::middleware::Next;

/// Reproduce the `api_with_role_auth` production mount: the real
/// `harvest_api_router` with `admin_auth_boundary=true`, the class layer
/// installed (when `with_enforce`), an embedder auth middleware that tags the
/// Session `role`, and the whole thing nested under `/api/harvest`.
fn role_auth_nested_app(role: &'static str, with_enforce: bool) -> Router {
    let api_state = HarvestApiState::new();
    // `api_with_role_auth` → `api_with_auth` sets `admin_auth_boundary=true`,
    // making per-route `require_admin` (and the SSE in-handler admin check) a
    // no-op for the read-only principal — the AC1 reconciliation.
    api_state.set_admin_auth_boundary(true);
    let mut router = harvest_api_router(api_state);
    if with_enforce {
        router = router.layer(from_fn(enforce_read_only_class));
    }
    // Embedder auth middleware: authenticate + tag the Session. Applied LAST so
    // it is the outermost layer (runs first) and populates the Session
    // extension before the enforce layer reads it — the exact production order.
    router = router.layer(from_fn(
        move |mut req: Request<Body>, next: Next| async move {
            let mut d = HashMap::new();
            d.insert("role".to_string(), role.to_string());
            req.extensions_mut()
                .insert(Session::new_for_test("nested-test".to_string(), d));
            next.run(req).await
        },
    ));
    Router::new()
        .nest("/api/harvest", router)
        .with_state(AppState::for_test())
}

async fn drive_nested(app: &Router, method: Method, uri: &str) -> (StatusCode, String) {
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = autumn_web::reexports::axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

#[tokio::test]
async fn eris_readonly_role_auth_reaches_admin_reads_and_403s_mutations_through_the_nest() {
    let ro = role_auth_nested_app("harvest_readonly", true);

    // A read-only principal REACHES an admin-gated ReadOnly route through the
    // nest (require_admin is a no-op under the boundary): NOT 401 (auth passed),
    // NOT 403 (class layer allowed the read). A 5xx/400 from the handler
    // (no runtime installed) proves the request reached it.
    for path in ["/api/harvest/admin/config", "/api/harvest/admin/status"] {
        let (status, _) = drive_nested(&ro, Method::GET, path).await;
        assert_ne!(
            status,
            StatusCode::UNAUTHORIZED,
            "{path}: read-only must pass require_admin under the boundary (got 401)"
        );
        assert_ne!(
            status,
            StatusCode::FORBIDDEN,
            "{path}: read-only must reach an admin-gated ReadOnly route (got 403)"
        );
    }

    // The SSE event stream (ReadOnly, admin-gated + an in-handler admin check)
    // is reachable by a read-only principal. A valid uuid gets past the id
    // parse to the in-handler admin gate, which passes under the boundary; the
    // stream then fails closed on the missing runtime. Timeout-guarded so a
    // (theoretical) DB connect can't hang the test — a timeout still proves the
    // request passed auth (a 401/403 returns immediately).
    let sse = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        drive_nested(
            &ro,
            Method::GET,
            "/api/harvest/executions/00000000-0000-0000-0000-000000000001/events/stream",
        ),
    )
    .await;
    if let Ok((status, _)) = sse {
        assert_ne!(
            status,
            StatusCode::UNAUTHORIZED,
            "SSE: read-only must pass the in-handler admin gate (got 401)"
        );
        assert_ne!(
            status,
            StatusCode::FORBIDDEN,
            "SSE: read-only must reach the SSE handler (got 403)"
        );
    }

    // A read-only principal is 403'd on a real Mutating route THROUGH THE NEST —
    // proving the layer sees the mount-stripped path and produces the
    // distinguishable read-only-denial body.
    let (mstatus, mbody) =
        drive_nested(&ro, Method::POST, "/api/harvest/workflows/exec-1/cancel").await;
    assert_eq!(
        mstatus,
        StatusCode::FORBIDDEN,
        "read-only must be 403'd on POST cancel through the /api/harvest nest"
    );
    assert!(
        mbody.contains("read-only principal: mutation not permitted"),
        "nest 403 must carry the read-only-denial marker, got: {mbody}"
    );

    // An admin principal reaches the same Mutating route (not 403).
    let admin = role_auth_nested_app("admin", true);
    let (astatus, _) =
        drive_nested(&admin, Method::POST, "/api/harvest/workflows/exec-1/cancel").await;
    assert_ne!(
        astatus,
        StatusCode::FORBIDDEN,
        "admin must reach the Mutating route through the nest"
    );
}

#[tokio::test]
async fn eris_role_auth_off_leaves_the_router_unchanged_through_the_nest() {
    // AC6: with NO enforce layer (the `api_with_auth`/`api()` shape), a
    // read-only principal reaches the Mutating handler — byte-for-byte the
    // pre-#776 behavior. `admin_auth_boundary=true` makes require_admin a
    // no-op, so the request reaches the handler (which fails closed on the
    // missing runtime), and is NEVER the read-only-denial 403.
    let off = role_auth_nested_app("harvest_readonly", false);
    let (status, body) =
        drive_nested(&off, Method::POST, "/api/harvest/workflows/exec-1/cancel").await;
    assert_ne!(
        status,
        StatusCode::FORBIDDEN,
        "without the enforce layer, a read-only principal is not 403'd (got {status})"
    );
    assert!(
        !body.contains("read-only principal: mutation not permitted"),
        "the read-only-denial marker must never appear when the layer is off"
    );
}

#[tokio::test]
async fn eris_readonly_denial_body_is_distinguishable() {
    // AC7 observability: a denied mutation is a 403 with a distinguishable body
    // marker, so a probing/misconfigured client is diagnosable. The layer also
    // emits `tracing::warn!(method, path, "...denied mutation (403)")` on every
    // denial (verified in `enforce_read_only_class`); that warn is deliberately
    // NOT asserted here — capturing it in this parallel test binary is
    // unreliable (`tracing` caches callsite interest globally, and sibling
    // tests hit the same callsite with no subscriber active, poisoning it), so
    // this stable body marker is the pinned distinguishability signal.
    let app = synthetic_enforced_router();
    let mut req = Request::builder()
        .method(Method::POST)
        .uri("/workflows/zzztestparam0/cancel")
        .body(Body::empty())
        .unwrap();
    req.extensions_mut()
        .insert(session_role("harvest_readonly"));
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);
    let bytes = autumn_web::reexports::axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let body = String::from_utf8_lossy(&bytes);
    assert!(
        body.contains("read-only principal: mutation not permitted"),
        "403 body must carry the distinguishable read-only-denial marker, got: {body}"
    );
}

#[tokio::test]
async fn eris_readonly_denied_on_unclassified_ui_path_through_the_layer() {
    // L4 + the documented /ui limitation: /ui is unclassified, so under
    // fail-closed enforcement a read-only principal is 403'd on it while an
    // admin reaches it. Drives the actual `enforce_read_only_class` LAYER
    // (not just the `classify_route` unit fn) over an /ui route.
    use autumn_web::reexports::axum::routing::get as axum_get;
    let router: Router = Router::new()
        .route("/ui/workflows", axum_get(|| async { StatusCode::OK }))
        .layer(from_fn(enforce_read_only_class));

    let mut req = Request::builder()
        .method(Method::GET)
        .uri("/ui/workflows")
        .body(Body::empty())
        .unwrap();
    req.extensions_mut()
        .insert(session_role("harvest_readonly"));
    let res = router.clone().oneshot(req).await.unwrap();
    assert_eq!(
        res.status(),
        StatusCode::FORBIDDEN,
        "read-only principal must be 403'd on unclassified /ui (fail closed)"
    );
    let bytes = autumn_web::reexports::axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    assert!(
        String::from_utf8_lossy(&bytes).contains("read-only principal: mutation not permitted")
    );

    let admin = status_for(
        &router,
        &Method::GET,
        "/ui/workflows",
        Some(session_role("admin")),
    )
    .await;
    assert_eq!(admin, StatusCode::OK, "admin must reach /ui unchanged");
}

#[tokio::test]
async fn eris_readonly_options_preflight_is_not_denied() {
    // F3: OPTIONS is a preflight/metadata verb, never a mutation. A read-only
    // principal's OPTIONS request (even to a Mutating route's path) must NOT be
    // 403'd by the layer. (The synthetic router has no OPTIONS handler, so it
    // routes to 405 — the point is that the layer let it THROUGH rather than
    // fail-closing it to 403.)
    let app = synthetic_enforced_router();
    let status = status_for(
        &app,
        &Method::OPTIONS,
        "/workflows/zzztestparam0/cancel",
        Some(session_role("harvest_readonly")),
    )
    .await;
    assert_ne!(
        status,
        StatusCode::FORBIDDEN,
        "OPTIONS must pass the read-only layer (got 403)"
    );
}

// ── Scoped API token layer (issue #942) ──────────────────────────────────────
//
// No-DB tests of the token verification/scope layer wired via
// `from_fn_with_state`. Pass-through paths (absent/non-`hvst_` bearer, OPTIONS)
// never touch the pool, so they hold without a database; a claimed `hvst_`
// bearer with no store available must NOT pass through (503), proving a claimed
// token is never trusted unverified.

use autumn_harvest_plugin::api_token::enforce_token_scope;
use autumn_web::reexports::axum::middleware::from_fn_with_state;

fn token_layer_app() -> impl tower::Service<
    Request<Body>,
    Response = autumn_web::reexports::axum::response::Response,
    Error = std::convert::Infallible,
    Future = impl std::future::Future,
> + Clone {
    let api_state = HarvestApiState::new();
    harvest_api_router(api_state.clone())
        .layer(from_fn_with_state(api_state, enforce_token_scope))
        .with_state(AppState::for_test())
}

fn get_bearer(uri: &str, bearer: &str) -> Request<Body> {
    Request::builder()
        .method(Method::GET)
        .uri(uri)
        .header("authorization", format!("Bearer {bearer}"))
        .body(Body::empty())
        .unwrap()
}

#[tokio::test]
async fn token_layer_passes_through_absent_bearer() {
    // AC7: no bearer → the token layer is a pass-through (embedder/session
    // governs). A read route stays reachable. `/health` is used (not
    // `/workflows`) because the no-pool test rig makes `/workflows` fail-closed
    // to 503 downstream (issue #756), which is orthogonal to — and would mask —
    // the token layer's pass-through behavior under test.
    let app = token_layer_app();
    let res = app.oneshot(get("/health")).await.unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
    assert_ne!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn token_layer_passes_through_non_hvst_bearer() {
    // AC7: a bearer that is not a harvest token is ignored — the embedder's own
    // JWT still governs. The token layer must not 401 it. `/health` is used for
    // the same rig reason as above (the no-pool `/workflows` 503 is downstream).
    let app = token_layer_app();
    let res = app
        .oneshot(get_bearer("/health", "eyJhbGciOiJIUzI1NiJ9.embedder.jwt"))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
    assert_ne!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn token_layer_passes_through_options_preflight() {
    let app = token_layer_app();
    let req = Request::builder()
        .method(Method::OPTIONS)
        .uri("/workflows/abc/cancel")
        .header("authorization", "Bearer hvst_whatever")
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    // OPTIONS is never a mutation; the layer passes it through (no 403/503).
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
    assert_ne!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn token_layer_rejects_hvst_bearer_when_store_unavailable() {
    // A claimed hvst_ token that cannot be verified (no pool installed) must NOT
    // pass through — under the admin boundary that would be an auth bypass. It
    // is rejected 503 (service unavailable), not silently trusted.
    let app = token_layer_app();
    let res = app
        .oneshot(get_bearer("/workflows", "hvst_someclaimedtoken"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn embedder_only_router_has_no_token_layer() {
    // AC7: the default router (no `enable_api_tokens`) is byte-identical to
    // trunk. A hvst_-looking bearer is meaningless to it — it is passed through
    // untouched (no token layer to interpret it), reaching the handler.
    // `/health` is used (not `/workflows`) because the no-pool test rig makes
    // `/workflows` fail-closed to 503 downstream (issue #756), independent of
    // the token layer whose absence is what this test asserts.
    let app = unauthenticated_app();
    let res = app
        .oneshot(get_bearer("/health", "hvst_ignored_here"))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
    assert_ne!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

/// Issue #944 review: the connector's derived idempotency keys must be
/// unclaimable from outside.
///
/// A `SignalsWithStart` binding derives its key into the same
/// `(workflow_exec_id, idempotency_key)` scope on `harvest_signals` that this
/// route writes, and derived keys are *predictable* — Kafka coordinates are
/// `{topic}:{partition}:{offset}`. A caller who landed one first would make
/// the broker's own delivery read as an idempotent replay and be acknowledged
/// without ever dispatching its payload: silent loss.
///
/// Driven through the real router with no storage configured, which is exactly
/// what proves the guard runs at the request boundary rather than somewhere
/// behind a database read. The body is asserted, not just the status, so this
/// cannot pass for an unrelated 400.
#[tokio::test]
async fn eris_a_reserved_connector_idempotency_key_is_refused_on_the_signal_route() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/workflows/00000000-0000-0000-0000-000000000001/signal/order_event")
                .header("Content-Type", "application/json")
                .header("Idempotency-Key", "conn:L6:orders:L0::L10:orders:0:7")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body = autumn_web::reexports::axum::body::to_bytes(res.into_body(), 64 * 1024)
        .await
        .expect("body");
    let text = String::from_utf8_lossy(&body);
    assert!(
        text.contains("reserved"),
        "the refusal must name the reservation, not some other 400: {text}"
    );
}

/// The guard must not over-reject: a key that merely resembles the reserved
/// prefix cannot alias a derived one, so it has to get past the boundary.
/// `CONN:` differs from `conn:` under the Postgres text comparison backing the
/// uniqueness scope, so refusing it would reject a legitimate caller key.
#[tokio::test]
async fn eris_a_near_miss_idempotency_key_is_not_refused_as_reserved() {
    for key in ["CONN:x", "conn", "connector-key", "xconn:y"] {
        let app = unauthenticated_app();
        let res = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/workflows/00000000-0000-0000-0000-000000000001/signal/order_event")
                    .header("Content-Type", "application/json")
                    .header("Idempotency-Key", key)
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = res.status();
        let body = autumn_web::reexports::axum::body::to_bytes(res.into_body(), 64 * 1024)
            .await
            .expect("body");
        let text = String::from_utf8_lossy(&body);
        assert!(
            !text.contains("reserved"),
            "{key} must not be refused as reserved (status {status}): {text}"
        );
    }
}

// ── Audit export to a SIEM sink (issue #953) ─────────────────────────────────
//
// Both routes must sit in the SAME auth chain as every other admin-gated
// route. `GET /admin/audit-export` reports the completeness posture of the
// privileged-action log, which is exactly what an attacker would want to read
// before acting; `POST .../redrive` mutates a compliance cursor. Neither may
// be reachable unauthenticated, and the assertion is on `401` specifically —
// a `404`/`405` here would mean the route is not registered at all and the
// test would be passing for the wrong reason.

#[tokio::test]
async fn eris_require_auth_blocks_audit_export_status() {
    let app = authenticated_app();
    let res = app.oneshot(get("/admin/audit-export")).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_audit_export_redrive() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json(
            "/admin/audit-export/redrive",
            r#"{"shard": 0, "to_seq": 0}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

// Issue #1273: decommission and reactivate mutate the same compliance
// cursor as redrive (one retires it, discarding the compliance guarantee
// over unshipped records; the other resumes it) and must sit behind the
// same auth gate.

#[tokio::test]
async fn eris_require_auth_blocks_audit_export_decommission() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json(
            "/admin/audit-export/decommission",
            r#"{"shard": 0}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_audit_export_reactivate() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json(
            "/admin/audit-export/reactivate",
            r#"{"shard": 0}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

// ── Dev-profile unauthenticated management API (issue #1284) ─────────────────
//
// The quickstart's documented Step 3 —
//
//   cargo run -p autumn-harvest-cli -- --base-url http://localhost:3000/api/harvest preflight
//
// — is a bare `reqwest` GET with no cookie, no bearer token and no session. In
// the `dev` profile it must reach `GET /admin/preflight` rather than 401, which
// is what `set_deployment_profile`'s own contract has always promised ("`dev`
// allows an unauthenticated local management API").
//
// Two request shapes both have to work, and they are NOT the same shape:
//
//   1. NO `Session` extension at all. This is what a standalone integration
//      that mounts `harvest_api_router` directly (with no autumn-web session
//      layer) produces — and what these tests produce by default.
//   2. A FRESH, NON-COOKIE-BACKED `Session`. This is what the real quickstart
//      app produces. autumn-web installs its session layer unconditionally, and
//      that layer mints an empty session for a request that carries no valid
//      cookie and inserts it into the extensions — so the quickstart's CLI call
//      arrives as `Some(session)`, not `None`. A fix that only handles shape 1
//      passes here and still 401s in the quickstart.
//
// The widening is bounded on three sides, each pinned below: `dev` only,
// no-established-session only, and an embedder-declared boundary still wins.

fn get_with_cookieless_session(uri: &str) -> Request<Body> {
    let mut request = get(uri);
    request
        .extensions_mut()
        .insert(Session::new_for_test_without_cookie(
            "harvest-cookieless-session".to_string(),
            HashMap::new(),
        ));
    request
}

fn get_with_session(uri: &str, session_key: &str) -> Request<Body> {
    let mut request = get(uri);
    let mut data = HashMap::new();
    data.insert(session_key.to_string(), "operator-1".to_string());
    request.extensions_mut().insert(Session::new_for_test(
        "harvest-established-session".to_string(),
        data,
    ));
    request
}

fn dev_profile_app() -> impl tower::Service<
    Request<Body>,
    Response = autumn_web::reexports::axum::response::Response,
    Error = std::convert::Infallible,
    Future = impl std::future::Future,
> + Clone {
    let api_state = HarvestApiState::new();
    api_state.set_deployment_profile("dev");
    app_with_api_state(api_state)
}

/// AC1, shape 1: the documented preflight command with no session extension.
#[tokio::test]
async fn hermes_dev_profile_admits_preflight_with_no_session_extension() {
    let res = dev_profile_app()
        .oneshot(get("/admin/preflight"))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

/// AC1, shape 2: the quickstart's ACTUAL shape — autumn-web's session layer has
/// already minted an empty, non-cookie-backed session for the cookieless CLI
/// request. This is the assertion the issue's own stated mechanism
/// (`session = None`) would have missed.
#[tokio::test]
async fn hermes_dev_profile_admits_preflight_with_fresh_cookieless_session() {
    let res = dev_profile_app()
        .oneshot(get_with_cookieless_session("/admin/preflight"))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

/// AC1: the fix is the shared admin gate, not a `/admin/preflight` special
/// case — a second admin-gated read route admits the same caller.
#[tokio::test]
async fn hermes_dev_profile_admits_second_admin_route_when_unauthenticated() {
    let res = dev_profile_app()
        .oneshot(get_with_cookieless_session("/admin/shards/health"))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

/// AC3: an ESTABLISHED (cookie-backed) session is still judged by the
/// configured session key in `dev`. The widening is for callers who presented
/// no credential at all — it does not delete `set_admin_auth_session_key`'s
/// meaning for an embedder running its own auth middleware.
#[tokio::test]
async fn hermes_dev_profile_still_requires_configured_key_for_established_session() {
    let api_state = HarvestApiState::new();
    api_state.set_deployment_profile("dev");
    api_state.set_admin_auth_session_key("operator_id");
    let app = app_with_api_state(api_state);

    let res = app
        .oneshot(get_with_session("/admin/preflight", "admin_id"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

/// AC3: and the established-session path still ADMITS the configured key.
#[tokio::test]
async fn hermes_dev_profile_admits_established_session_with_configured_key() {
    let api_state = HarvestApiState::new();
    api_state.set_deployment_profile("dev");
    api_state.set_admin_auth_session_key("operator_id");
    let app = app_with_api_state(api_state);

    let res = app
        .oneshot(get_with_session("/admin/preflight", "operator_id"))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

/// AC2: the bypass does not escape `dev`. A named non-dev profile stays
/// fail-closed for both unauthenticated shapes.
#[tokio::test]
async fn hermes_non_dev_profile_rejects_unauthenticated_admin_route() {
    let api_state = HarvestApiState::new();
    api_state.set_deployment_profile("prod");
    let app = app_with_api_state(api_state);

    let res = app.clone().oneshot(get("/admin/preflight")).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    let res = app
        .oneshot(get_with_cookieless_session("/admin/preflight"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

/// AC2: the default (`unknown`) profile is non-dev and stays fail-closed. This
/// is the profile a standalone integration gets when it never calls
/// `set_deployment_profile`, so it is the one that must not drift open.
#[tokio::test]
async fn hermes_unknown_profile_rejects_unauthenticated_admin_route() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(get_with_cookieless_session("/admin/preflight"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

/// AC4: an embedder-declared auth boundary still short-circuits ahead of the
/// profile branch — precedence is unchanged.
#[tokio::test]
async fn hermes_declared_auth_boundary_still_short_circuits_in_dev() {
    let api_state = HarvestApiState::new();
    api_state.set_deployment_profile("dev");
    api_state.set_admin_auth_boundary(true);
    let app = app_with_api_state(api_state);

    let res = app.oneshot(get("/admin/preflight")).await.unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}
