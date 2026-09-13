//! Integration tests for the Vantage dashboard UI.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use autumn_harvest::builder::WorkerConfig;
use autumn_harvest::context::{DEFAULT_HISTORY_CONTINUE_AS_NEW_THRESHOLD, WorkflowHistoryPolicy};
use autumn_harvest::dag::DagBuilder;
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::models::NewHarvestEvent;
use autumn_harvest::prelude::{ActivityContext, activities, activity, dag, dags, workflows};
use autumn_harvest::scheduler::{RegisteredDag, SchedulerMonitor, compile_dag_catalog};
use autumn_harvest::schema::{
    harvest_dead_letters, harvest_events, harvest_task_queue, harvest_workflow_executions,
};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::shard::ShardedDbPool;
use autumn_harvest::store;
use autumn_harvest::types::{ExecutionId, Priority, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{StartWorkflowParams, start_or_load_workflow_execution};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime};
use autumn_harvest_plugin::ui::harvest_ui_router;
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel_async::SimpleAsyncConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use std::time::Duration;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

fn init_sql() -> Vec<u8> {
    autumn_harvest::test_init_sql().as_bytes().to_vec()
}

async fn setup_test_database_url() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container
        .get_host()
        .await
        .expect("failed to get container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get container port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, container)
}

/// Prefer `HARVEST_TEST_DATABASE_URL` (a real, already-running Postgres, for
/// sandboxes with no Docker daemon) over spinning up a testcontainer.
async fn overdue_read_database_url() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let (url, container) = setup_test_database_url().await;
    (url, Some(container))
}

async fn setup_sharded_test_database_urls() -> ((String, String), ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container
        .get_host()
        .await
        .expect("failed to get container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get container port");
    let admin_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let shard0_db = format!("harvest_ui_shard_{}", uuid::Uuid::new_v4().simple());
    let shard1_db = format!("harvest_ui_shard_{}", uuid::Uuid::new_v4().simple());

    let mut admin_conn = <AsyncPgConnection as AsyncConnection>::establish(&admin_url)
        .await
        .expect("failed to connect to admin database");
    diesel::sql_query(format!("CREATE DATABASE {shard0_db}"))
        .execute(&mut admin_conn)
        .await
        .expect("failed to create shard 0 database");
    diesel::sql_query(format!("CREATE DATABASE {shard1_db}"))
        .execute(&mut admin_conn)
        .await
        .expect("failed to create shard 1 database");

    let shard0_url = format!("postgres://postgres:postgres@{host}:{port}/{shard0_db}");
    let shard1_url = format!("postgres://postgres:postgres@{host}:{port}/{shard1_db}");

    for shard_url in [&shard0_url, &shard1_url] {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(shard_url)
            .await
            .expect("failed to connect to shard database");
        conn.batch_execute(&autumn_harvest::test_init_sql())
            .await
            .expect("failed to apply harvest migrations to shard database");
    }

    ((shard0_url, shard1_url), container)
}

fn build_test_pool(database_url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("failed to build test pool")
}

fn build_two_shard_pool(shard0_url: &str, shard1_url: &str) -> HarvestDbPool {
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), build_test_pool(shard0_url));
    pools.insert(ShardId::new(1), build_test_pool(shard1_url));
    HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0)))
}

/// Provision N additional databases in the same Postgres instance and run the
/// harvest schema in each.  Returns the URLs in shard-index order.
async fn setup_n_shard_databases(container: &ContainerAsync<Postgres>, n: usize) -> Vec<String> {
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let admin_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    let mut admin_conn = <AsyncPgConnection as AsyncConnection>::establish(&admin_url)
        .await
        .expect("admin connection");

    let mut urls = Vec::with_capacity(n);
    for i in 0..n {
        let db_name = format!("harvest_perf_shard_{}_{}", i, uuid::Uuid::new_v4().simple());
        diesel::sql_query(format!("CREATE DATABASE {db_name}"))
            .execute(&mut admin_conn)
            .await
            .expect("create shard db");
        let url = format!("postgres://postgres:postgres@{host}:{port}/{db_name}");
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
            .await
            .expect("shard connection");
        conn.batch_execute(&autumn_harvest::test_init_sql())
            .await
            .expect("apply migrations");
        urls.push(url);
    }
    urls
}

fn test_app_state_without_database() -> AppState {
    AppState::for_test().with_profile("test")
}

fn echo_registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: "echo_workflow",
            module: "tests",
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![],
    ))
}

fn spawn_test_worker(
    registry: Arc<HandlerRegistry>,
    pool: DbPool,
) -> (Arc<Worker>, tokio::task::JoinHandle<()>) {
    let mut runtime_config = WorkerRuntimeConfig::from(WorkerConfig::default());
    runtime_config.worker_id = "ui-test-worker".to_string();
    runtime_config.poll_interval = Duration::from_millis(25);
    let worker =
        Arc::new(Worker::new(runtime_config, registry).expect("worker config should be valid"));
    let worker_task = {
        let worker = Arc::clone(&worker);
        tokio::spawn(async move {
            worker.run(&pool).await;
        })
    };
    (worker, worker_task)
}

async fn fetch_html(app: &axum::Router, uri: &str) -> (StatusCode, String) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("valid request"),
        )
        .await
        .expect("GET request failed");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("failed to read response body");
    (status, String::from_utf8_lossy(&bytes).to_string())
}

async fn post_form(
    app: &axum::Router,
    uri: &str,
    body: impl Into<String>,
) -> (StatusCode, axum::http::HeaderMap, String) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(body.into()))
                .expect("valid form request"),
        )
        .await
        .expect("POST form request failed");
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("failed to read response body");
    (status, headers, String::from_utf8_lossy(&bytes).to_string())
}

async fn insert_workflow_on_url(
    database_url: &str,
    shard: ShardId,
    workflow_name: &str,
    workflow_id: &str,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(shard);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for workflow insert");
    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name,
            workflow_id,
            exec_id,
            input: json!({ "workflow_id": workflow_id, "shard": shard.as_i32() }),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::default(),
            conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            chain_execution_timeout: None,
            max_workflow_chain_timeout_ceiling: None,
            inherited_chain_deadline_at: None,
            concurrency_key: None,
            concurrency_limit: None,
            concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
            priority: Priority::default(),
            max_workflow_input_bytes: 0,
            start_at: None,
            delay: None,
            max_workflow_start_delay: None,

            owner: None,
            runbook_url: None,
            severity: None,
            context_headers: None,

            sla: None,
            schedule_id: None,
            scheduled_for: None,
            workflow_attempt: 1,
            workflow_retry_policy: None,
            retry_of_exec_id: None,
            max_workflow_attempts_ceiling: None,
            origin: None,
            completion_callbacks: None,
            start_source: autumn_harvest::StartSource::Api,
            start_source_ref: None,
            started_by: None,
        },
        None,
    )
    .await
    .expect("workflow insert should succeed");
    exec_id
}

async fn append_test_events(database_url: &str, exec_id: ExecutionId, prefix: &str, count: i32) {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect for event insert");
    for event_id in 1..=count {
        diesel::insert_into(harvest_events::table)
            .values(&NewHarvestEvent {
                workflow_exec_id: exec_id.as_uuid(),
                event_id,
                event_type: &format!("{prefix}Event{event_id}"),
                event_data: json!({ "event": event_id, "prefix": prefix }),
            })
            .execute(&mut conn)
            .await
            .expect("failed to insert test event");
    }
}

#[derive(Debug, Clone)]
struct SeededDeadLetter {
    id: uuid::Uuid,
    shard_url: String,
    workflow_name: String,
    activity_name: Option<String>,
}

async fn insert_dead_letter_on_url(
    database_url: &str,
    shard: ShardId,
    workflow_name: &str,
    workflow_id: &str,
    task_type: &str,
    activity_name: Option<&str>,
    ordinal: usize,
) -> SeededDeadLetter {
    let exec_id = insert_workflow_on_url(database_url, shard, workflow_name, workflow_id).await;
    append_test_events(database_url, exec_id, workflow_name, 12).await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect for dead-letter insert");
    let id = autumn_harvest::dlq::dead_letter(
        &mut conn,
        &autumn_harvest::dlq::NewDeadLetterEntry {
            original_task_id: uuid::Uuid::new_v4(),
            queue_name: "default".to_string(),
            task_type: task_type.to_string(),
            workflow_exec_id: Some(exec_id.as_uuid()),
            activity_name: activity_name.map(str::to_string),
            input: json!({ "ordinal": ordinal, "workflow": workflow_name }),
            error: format!("{workflow_name} failed at attempt {ordinal}: downstream timeout with enough text to truncate"),
            attempts: i32::try_from(ordinal + 1).expect("ordinal fits i32"),
            owner: None,
            severity: None,
        },
    )
    .await
    .expect("dead-letter insert should succeed");

    SeededDeadLetter {
        id,
        shard_url: database_url.to_string(),
        workflow_name: workflow_name.to_string(),
        activity_name: activity_name.map(str::to_string),
    }
}

async fn count_dead_letter_by_id(database_url: &str, id: uuid::Uuid) -> i64 {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect for dead-letter count");
    harvest_dead_letters::table
        .filter(harvest_dead_letters::id.eq(id))
        .count()
        .get_result(&mut conn)
        .await
        .expect("failed to count dead-letter row")
}

async fn count_task_queue_by_activity(database_url: &str, activity_name: &str) -> i64 {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect for task queue count");
    harvest_task_queue::table
        .filter(harvest_task_queue::activity_name.eq(activity_name))
        .count()
        .get_result(&mut conn)
        .await
        .expect("failed to count task queue rows")
}

async fn start_workflow_and_wait(
    api_app: &axum::Router,
    workflow_id: &str,
    database_url: &str,
) -> String {
    let response = api_app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workflows/echo_workflow/start")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "workflow_id": workflow_id,
                        "input": { "hello": "world" },
                    })
                    .to_string(),
                ))
                .expect("valid request"),
        )
        .await
        .expect("POST /workflows/echo_workflow/start failed");
    assert_eq!(response.status(), StatusCode::CREATED);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&bytes).unwrap();
    let exec_id = json["execution_id"].as_str().unwrap().to_string();

    for _ in 0..200 {
        let mut conn = AsyncPgConnection::establish(database_url).await.unwrap();
        let state: String = harvest_workflow_executions::table
            .find(
                exec_id
                    .parse::<autumn_harvest::ExecutionId>()
                    .unwrap()
                    .as_uuid(),
            )
            .select(harvest_workflow_executions::state)
            .first(&mut conn)
            .await
            .unwrap();
        if state == "COMPLETED" {
            return exec_id;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("workflow {exec_id} never completed");
}

#[tokio::test]
async fn ui_root_redirects_to_workflows() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let registry = echo_registry();
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool));
    api_state.install(HarvestApiRuntime::new(
        Arc::clone(&registry),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        Some("ui-test-worker".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));

    let app = harvest_ui_router(api_state).with_state(test_app_state_without_database());

    let response = app
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .expect("GET / failed");
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    let location = response
        .headers()
        .get("location")
        .expect("redirect must have Location")
        .to_str()
        .unwrap();
    assert_eq!(location, "workflows");
}

#[tokio::test]
async fn ui_lists_workflows_and_renders_detail_page() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let registry = echo_registry();
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::clone(&registry),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        Some("ui-test-worker".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));

    let (worker, worker_task) = spawn_test_worker(Arc::clone(&registry), pool.clone());

    let api_app = autumn_harvest_plugin::harvest_api_router(api_state.clone())
        .with_state(test_app_state_without_database());
    let exec_id = start_workflow_and_wait(&api_app, "ui-demo-1", &database_url).await;

    let ui_app = harvest_ui_router(api_state.clone()).with_state(test_app_state_without_database());

    let (status, list_html) = fetch_html(&ui_app, "/workflows").await;
    assert_eq!(status, StatusCode::OK);
    assert!(list_html.contains("🔭 Vantage"), "layout header missing");
    assert!(
        list_html.contains("echo_workflow"),
        "workflow name missing from list: {list_html}"
    );
    assert!(
        list_html.contains("COMPLETED"),
        "completed badge missing from list"
    );
    // state filter controls are present and no external asset URLs leak in
    assert!(list_html.contains("name=\"state\""));
    assert!(!list_html.contains("http://"));
    assert!(!list_html.contains("https://"));
    assert!(!list_html.contains("<script"));

    let (status, filtered_html) = fetch_html(&ui_app, "/workflows?state=COMPLETED").await;
    assert_eq!(status, StatusCode::OK);
    assert!(filtered_html.contains("echo_workflow"));
    let (status, empty_html) = fetch_html(&ui_app, "/workflows?state=FAILED").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        empty_html.contains("No workflows match this filter"),
        "expected empty state message"
    );

    let (status, detail_html) = fetch_html(&ui_app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(detail_html.contains("Event history"));
    assert!(detail_html.contains("Metadata"));
    assert!(detail_html.contains(&exec_id));
    assert!(
        detail_html.contains("WorkflowStarted"),
        "detail history should include start event"
    );
    assert!(
        detail_html.contains("WorkflowCompleted"),
        "detail history should include completion event"
    );
    // Input payload is pretty-printed and HTML-escaped
    assert!(detail_html.contains("&quot;hello&quot;"));
    assert!(!detail_html.contains("<script"));

    let (status, _body) = fetch_html(&ui_app, "/workflows/not-a-uuid").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let random = uuid::Uuid::new_v4();
    let (status, _body) = fetch_html(&ui_app, &format!("/workflows/{random}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    worker.shutdown();
    worker_task.await.expect("worker should exit cleanly");
}

#[tokio::test]
async fn ui_lists_workflows_across_shards() {
    let ((shard0_url, shard1_url), _container) = setup_sharded_test_database_urls().await;
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(build_two_shard_pool(&shard0_url, &shard1_url));
    api_state.install(HarvestApiRuntime::new(
        echo_registry(),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        Some("ui-test-worker".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::new(
            vec![ShardId::new(0), ShardId::new(1)],
            vec![ShardId::new(0), ShardId::new(1)],
            ShardId::new(0),
        ),
    ));

    let exec_on_zero =
        insert_workflow_on_url(&shard0_url, ShardId::new(0), "workflow_on_zero", "ui-zero").await;
    let exec_on_one =
        insert_workflow_on_url(&shard1_url, ShardId::new(1), "workflow_on_one", "ui-one").await;

    let ui_app = harvest_ui_router(api_state).with_state(test_app_state_without_database());
    let (status, list_html) = fetch_html(&ui_app, "/workflows").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        list_html.contains("workflow_on_zero") && list_html.contains(&exec_on_zero.to_string()),
        "workflow from shard 0 should appear in the UI list"
    );
    assert!(
        list_html.contains("workflow_on_one") && list_html.contains(&exec_on_one.to_string()),
        "workflow from shard 1 should appear in the UI list"
    );
}

// ---------------------------------------------------------------------------
// Workers page tests
// ---------------------------------------------------------------------------

/// Insert a single row into `harvest_workers` with controllable status and
/// heartbeat time.  `heartbeat_offset_secs` < 0 → past (stale); 0 → now.
async fn insert_test_worker(
    conn: &mut AsyncPgConnection,
    worker_id: &str,
    status: &str,
    heartbeat_offset_secs: i64,
) {
    let sql = format!(
        "INSERT INTO harvest_workers \
            (worker_id, last_heartbeat_at, status, queues, shard_assignments, max_concurrency, host) \
         VALUES \
            ('{worker_id}', NOW() + interval '{heartbeat_offset_secs} seconds', \
             '{status}', '[]'::jsonb, '[0]'::jsonb, 10, 'test-host') \
         ON CONFLICT (worker_id) DO UPDATE \
            SET last_heartbeat_at = excluded.last_heartbeat_at, \
                status            = excluded.status"
    );
    conn.batch_execute(&sql)
        .await
        .expect("insert_test_worker failed");
}

fn build_single_shard_ui_app(database_url: &str) -> axum::Router {
    let pool = build_test_pool(database_url);
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool));
    api_state.install(HarvestApiRuntime::new(
        echo_registry(),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        None,
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));
    harvest_ui_router(api_state).with_state(test_app_state_without_database())
}

fn build_sharded_api_with_ui_app(shard0_url: &str, shard1_url: &str) -> axum::Router {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(build_two_shard_pool(shard0_url, shard1_url));
    api_state.install(HarvestApiRuntime::new(
        echo_registry(),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        None,
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::new(
            vec![ShardId::new(0), ShardId::new(1)],
            vec![ShardId::new(0), ShardId::new(1)],
            ShardId::new(0),
        ),
    ));

    autumn_harvest_plugin::harvest_api_router(api_state.clone())
        .nest("/ui", harvest_ui_router(api_state))
        .with_state(test_app_state_without_database())
}

async fn seed_dead_letter_ui_fixture(shard0_url: &str, shard1_url: &str) -> Vec<SeededDeadLetter> {
    let mut seeded = Vec::new();
    for i in 0..5 {
        seeded.push(
            insert_dead_letter_on_url(
                shard0_url,
                ShardId::new(0),
                if i % 2 == 0 {
                    "invoice_workflow"
                } else {
                    "settlement_workflow"
                },
                &format!("dlq-shard0-{i}"),
                "activity",
                Some("charge_card"),
                i,
            )
            .await,
        );
        seeded.push(
            insert_dead_letter_on_url(
                shard1_url,
                ShardId::new(1),
                if i % 2 == 0 {
                    "invoice_workflow"
                } else {
                    "settlement_workflow"
                },
                &format!("dlq-shard1-{i}"),
                "workflow",
                None,
                i + 5,
            )
            .await,
        );
    }
    seeded
}

fn assert_dead_letter_list_html(html: &str, seeded: &[SeededDeadLetter]) {
    assert!(html.contains("Dead Letters"), "page title missing: {html}");
    assert!(
        html.contains("href=\"dead-letters\"") && html.contains("Workers"),
        "nav should link Dead Letters alongside Workers: {html}"
    );
    assert!(
        html.contains("name=\"workflow_name\""),
        "workflow filter missing"
    );
    assert!(
        html.contains("name=\"task_kind\""),
        "task kind filter missing"
    );
    assert!(
        html.contains("name=\"failed_after\""),
        "failed_after filter missing"
    );
    assert!(
        html.contains("name=\"failed_before\""),
        "failed_before filter missing"
    );
    assert!(html.contains("name=\"shard_id\""), "shard filter missing");
    assert!(
        html.contains("Replay all matching"),
        "bulk replay control missing"
    );
    assert!(
        html.contains("Discard all matching"),
        "bulk discard control missing"
    );
    assert!(
        html.contains("action=\"../dead-letters/replay\""),
        "row replay form should post to the existing bulk replay endpoint: {html}"
    );
    assert!(
        html.contains("action=\"../dead-letters/discard\""),
        "row discard form should post to the existing bulk discard endpoint: {html}"
    );
    assert!(
        html.contains("view-toggle") && html.contains("view=summary"),
        "list view should offer a Summary toggle (issue #385): {html}"
    );

    for row in seeded {
        assert!(
            html.contains(&row.id.to_string()),
            "dead-letter id should be present for row action: {}",
            row.id
        );
        assert!(
            html.contains(&row.workflow_name),
            "workflow name should render: {}",
            row.workflow_name
        );
    }
    assert!(
        html.contains("invoice_workflowEvent12") && !html.contains("invoice_workflowEvent1</code>"),
        "row detail should include the last events leading to failure, not the whole cemetery: {html}"
    );
    assert!(
        html.contains("&quot;ordinal&quot;"),
        "original task payload should render in row detail: {html}"
    );
}

fn assert_dead_letter_filtered_html(filtered_html: &str) {
    assert!(
        filtered_html.contains("invoice_workflow"),
        "invoice rows should remain after filter: {filtered_html}"
    );
    assert!(
        !filtered_html.contains("settlement_workflow"),
        "settlement rows should be filtered out: {filtered_html}"
    );
}

fn assert_dead_letter_task_kind_filtered_html(
    html: &str,
    seeded: &[SeededDeadLetter],
    include_activities: bool,
) {
    for row in seeded {
        let should_include = row.activity_name.is_some() == include_activities;
        assert_eq!(
            html.contains(&row.id.to_string()),
            should_include,
            "task kind filter should {} row {} in HTML: {html}",
            if should_include { "include" } else { "exclude" },
            row.id
        );
    }
}

#[tokio::test]
async fn ui_dead_letters_lists_filters_and_replays_single_entry() {
    let ((shard0_url, shard1_url), _container) = setup_sharded_test_database_urls().await;
    let seeded = seed_dead_letter_ui_fixture(&shard0_url, &shard1_url).await;
    let app = build_sharded_api_with_ui_app(&shard0_url, &shard1_url);

    let (status, html) = fetch_html(&app, "/ui/dead-letters").await;
    assert_eq!(status, StatusCode::OK, "DLQ page should render: {html}");
    assert_dead_letter_list_html(&html, &seeded);
    assert!(
        html.contains("name=\"return_to\" value=\"../ui/dead-letters\""),
        "DLQ forms should return relative to the bulk endpoint path: {html}"
    );

    let (status, filtered_html) =
        fetch_html(&app, "/ui/dead-letters?workflow_name=invoice_workflow").await;
    assert_eq!(status, StatusCode::OK, "filtered DLQ page should render");
    assert_dead_letter_filtered_html(&filtered_html);

    let (status, activity_html) = fetch_html(&app, "/ui/dead-letters?task_kind=Activity").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "activity-filtered DLQ page should render"
    );
    assert_dead_letter_task_kind_filtered_html(&activity_html, &seeded, true);

    let (status, workflow_html) = fetch_html(&app, "/ui/dead-letters?task_kind=Workflow").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "workflow-filtered DLQ page should render"
    );
    assert_dead_letter_task_kind_filtered_html(&workflow_html, &seeded, false);

    let target = seeded
        .iter()
        .find(|row| row.activity_name.as_deref() == Some("charge_card"))
        .expect("activity dead-letter should exist");
    let legacy_target = seeded
        .iter()
        .find(|row| row.id != target.id)
        .expect("second dead-letter should exist");
    let (legacy_status, _legacy_headers, legacy_body) = post_form(
        &app,
        "/ui/dead-letters/replay",
        format!(
            "dead_letter_id={}&return_to=ui%2Fdead-letters",
            legacy_target.id
        ),
    )
    .await;
    assert_eq!(
        legacy_status,
        StatusCode::NOT_FOUND,
        "UI router must not keep a duplicate mutating replay route: {legacy_body}"
    );
    assert_eq!(
        count_dead_letter_by_id(&legacy_target.shard_url, legacy_target.id).await,
        1,
        "legacy UI POST path should not mutate DLQ rows"
    );

    let (status, headers, body) = post_form(
        &app,
        "/dead-letters/replay",
        format!(
            "dead_letter_id={}&return_to=..%2Fui%2Fdead-letters",
            target.id
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::SEE_OTHER,
        "single replay should redirect back to the page: {body}"
    );
    let location = headers
        .get("location")
        .expect("redirect should include Location")
        .to_str()
        .expect("Location should be valid UTF-8");
    assert!(
        location.starts_with("../ui/dead-letters?flash="),
        "redirect should return to mounted DLQ UI with flash, got {location}"
    );
    assert_eq!(
        count_dead_letter_by_id(&target.shard_url, target.id).await,
        0,
        "replayed DLQ row should be removed"
    );
    assert_eq!(
        count_task_queue_by_activity(&target.shard_url, "charge_card").await,
        1,
        "single replay should enqueue exactly one activity task"
    );
}

/// RED baseline this test replaces the assumption of:
/// `parse_dead_letter_ui_filters` used to parse `task_kind` with
/// `DeadLetterTaskKind::parse(..)?`, a bare `?` on `Result<_, AutumnError>`.
/// An unrecognized value 400-aborted the whole `/dead-letters` response
/// before the filter form ever rendered. That discarded the `workflow_name`
/// filter the operator had already typed. Same discard-the-page-on-bad-filter
/// pattern already fixed for the Workflows page's
/// `started_after`/`started_before` (#1333) and the Workers page's
/// `status`/`stale` (#1378). This DLQ page was the one sibling list page
/// still carrying it.
///
/// GREEN (this commit): the request still renders the DLQ page (`200`). It
/// preserves the other filter (`workflow_name=invoice_workflow`, still in
/// its input's `value=`). It also surfaces a `role="alert"` message naming
/// the bad value and the valid options next to the Task kind field.
#[tokio::test]
async fn ui_dead_letters_unknown_task_kind_redisplays_form_instead_of_aborting_page() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);

    let (status, html) = fetch_html(
        &app,
        "/dead-letters?task_kind=zombie&workflow_name=invoice_workflow",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "an unknown task_kind value must not abort the whole DLQ page: {html}"
    );
    assert!(
        html.contains("name=\"task_kind\""),
        "filter form must still render: {html}"
    );
    assert!(
        html.contains("value=\"invoice_workflow\""),
        "the other filter the operator already typed must not be discarded: {html}"
    );
    assert!(
        html.contains("role=\"alert\""),
        "an inline, screen-reader-announced error must sit next to the field: {html}"
    );
    assert!(
        html.contains("zombie") && html.contains("Activity"),
        "the error must name the bad value and a valid option: {html}"
    );
    assert!(
        html.contains("option value=\"zombie\" selected"),
        "the Task kind select must echo the invalid value back as its selected \
         option, not silently revert to 'All': {html}"
    );
}

/// Same fix, `failed_after` side: `parse_dead_letter_time_filter` used to
/// `?`-propagate a bare `AutumnError` on a malformed RFC 3339 timestamp.
#[tokio::test]
async fn ui_dead_letters_invalid_failed_after_redisplays_form_instead_of_aborting_page() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);

    let (status, html) = fetch_html(
        &app,
        "/dead-letters?failed_after=not-a-date&workflow_name=invoice_workflow",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "an invalid failed_after value must not abort the whole DLQ page: {html}"
    );
    assert!(
        html.contains("value=\"invoice_workflow\""),
        "the other filter the operator already typed must not be discarded: {html}"
    );
    assert!(
        html.contains("role=\"alert\""),
        "an inline, screen-reader-announced error must sit next to the field: {html}"
    );
    assert!(
        html.contains("value=\"not-a-date\""),
        "the operator's exact invalid text must be echoed back into the field: {html}"
    );
    assert!(
        html.contains("RFC 3339"),
        "the error must name the expected format: {html}"
    );
}

/// Same fix, `failed_before` side.
#[tokio::test]
async fn ui_dead_letters_invalid_failed_before_redisplays_form_instead_of_aborting_page() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);

    let (status, html) = fetch_html(
        &app,
        "/dead-letters?failed_before=not-a-date&workflow_name=invoice_workflow",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "an invalid failed_before value must not abort the whole DLQ page: {html}"
    );
    assert!(
        html.contains("value=\"invoice_workflow\""),
        "the other filter the operator already typed must not be discarded: {html}"
    );
    assert!(
        html.contains("role=\"alert\""),
        "an inline, screen-reader-announced error must sit next to the field: {html}"
    );
    assert!(
        html.contains("value=\"not-a-date\""),
        "the operator's exact invalid text must be echoed back into the field: {html}"
    );
}

/// Codex-review-class regression guard, matching #1333/#1378's own
/// follow-up: an invalid filter's raw text must survive into pagination
/// instead of being dropped on the very next click.
#[tokio::test]
async fn ui_dead_letters_invalid_task_kind_persists_across_pagination() {
    let ((shard0_url, shard1_url), _container) = setup_sharded_test_database_urls().await;
    let _seeded = seed_dead_letter_ui_fixture(&shard0_url, &shard1_url).await;
    let app = build_sharded_api_with_ui_app(&shard0_url, &shard1_url);

    let (status, html) = fetch_html(&app, "/ui/dead-letters?task_kind=zombie&limit=1").await;
    assert_eq!(status, StatusCode::OK, "unexpected status: {html}");
    assert!(
        html.contains("Next"),
        "test setup must produce a Next link to exercise pagination: {html}"
    );
    assert!(
        html.contains("task_kind=zombie"),
        "the invalid task_kind must be carried into the Next/Previous links \
         instead of being dropped on the next page view: {html}"
    );
}

/// `shard_id` used to be typed `Option<i32>` straight on the DLQ page's
/// `Query<..>` extractor struct. A non-numeric value failed axum's own
/// query deserialization, a bare framework 400 before `list_dead_letters_ui`
/// ever ran. That is one layer earlier than the `task_kind`/`failed_after`/
/// `failed_before` page-abort bug #1420 already fixed on this same page.
#[tokio::test]
async fn ui_dead_letters_invalid_shard_id_redisplays_form_instead_of_aborting_page() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);

    let (status, html) = fetch_html(
        &app,
        "/dead-letters?shard_id=north&workflow_name=invoice_workflow",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "an invalid shard_id must not abort the whole DLQ page: {html}"
    );
    assert!(
        html.contains("value=\"invoice_workflow\""),
        "the other filter the operator already typed must not be discarded: {html}"
    );
    assert!(
        html.contains("role=\"alert\""),
        "an inline, screen-reader-announced error must sit next to the field: {html}"
    );
    assert!(
        html.contains("value=\"north\""),
        "the operator's exact invalid text must be echoed back into the field: {html}"
    );
    assert!(
        html.contains("shard_id") && html.contains("north"),
        "the error must name the field and the bad value: {html}"
    );
}

/// DLQ Summary toggle (issue #385): the aggregation view groups entries,
/// reports counts merged across shards, and links back into the filtered list.
#[tokio::test]
async fn ui_dead_letters_summary_view_groups_and_links() {
    let ((shard0_url, shard1_url), _container) = setup_sharded_test_database_urls().await;
    let _seeded = seed_dead_letter_ui_fixture(&shard0_url, &shard1_url).await;
    let app = build_sharded_api_with_ui_app(&shard0_url, &shard1_url);

    let (status, html) = fetch_html(&app, "/ui/dead-letters?view=summary").await;
    assert_eq!(status, StatusCode::OK, "summary view should render: {html}");

    // Toggle is in summary mode with a link back to the list view.
    assert!(
        html.contains("view-toggle"),
        "summary view should show the toggle: {html}"
    );
    assert!(
        html.contains("<span class=\"active\">Summary</span>"),
        "Summary tab should be active: {html}"
    );

    // Grouping control + default dimension columns.
    assert!(
        html.contains("name=\"group_by\""),
        "summary view should expose a group_by selector: {html}"
    );
    assert!(
        html.contains("workflow_name") && html.contains("failure_signature"),
        "default grouping columns should render: {html}"
    );

    // Both seeded workflow names appear as groups, with cross-shard counts.
    assert!(
        html.contains("invoice_workflow") && html.contains("settlement_workflow"),
        "both workflow groups should render: {html}"
    );
    // 6 invoice + 4 settlement = 10 across both shards.
    assert!(
        html.contains("10 total in DLQ") || html.contains("10</strong> total in DLQ"),
        "summary stats should report the cross-shard total: {html}"
    );

    // Click-through into the list view with workflow_name pre-applied.
    assert!(
        html.contains("View entries") && html.contains("workflow_name=invoice_workflow"),
        "summary groups should drill into the filtered list view: {html}"
    );
}

/// Invalid `group_by` on the summary view returns 400, not 500 or a silent
/// empty match (parity with the aggregation endpoint).
#[tokio::test]
async fn ui_dead_letters_summary_invalid_group_by_returns_400() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);

    let (status, _body) = fetch_html(&app, "/dead-letters?view=summary&group_by=tenant_id").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// The summary view can re-group on a chosen dimension and the selector
/// reflects the active choice.
#[tokio::test]
async fn ui_dead_letters_summary_regroups_on_selected_dimension() {
    let ((shard0_url, shard1_url), _container) = setup_sharded_test_database_urls().await;
    let _seeded = seed_dead_letter_ui_fixture(&shard0_url, &shard1_url).await;
    let app = build_sharded_api_with_ui_app(&shard0_url, &shard1_url);

    let (status, html) = fetch_html(&app, "/ui/dead-letters?view=summary&group_by=task_type").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "regrouped summary should render: {html}"
    );
    assert!(
        html.contains("<option value=\"task_type\" selected"),
        "selector should reflect the active group_by: {html}"
    );
    // Seed inserts ACTIVITY on shard0 and WORKFLOW on shard1.
    assert!(
        html.contains("activity") && html.contains("workflow"),
        "task_type groups should render both kinds: {html}"
    );
}

/// Navigation link: the index and workflows pages must include a Workers link.
#[tokio::test]
async fn ui_workers_nav_link_on_index_page() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);

    let (status, html) = fetch_html(&app, "/workflows").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("workers") || html.contains("Workers"),
        "workflows page should have a navigation link to Workers: {html}"
    );
}

/// Empty fleet: GET /workers returns 200 with an empty-state explanation.
#[tokio::test]
async fn ui_workers_empty_fleet() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);

    let (status, html) = fetch_html(&app, "/workers").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "workers page must return 200, got {status}"
    );
    assert!(html.contains("🔭 Vantage"), "layout header missing");
    assert!(
        html.contains("No workers") || html.contains("no workers"),
        "empty fleet should show a message: {html}"
    );
    assert!(!html.contains("<script"), "no script tags allowed");
    assert!(!html.contains("http://"), "no external URLs allowed");
    assert!(!html.contains("https://"), "no external HTTPS URLs allowed");
}

/// Health banner: all fresh Active workers → Healthy.
#[tokio::test]
async fn ui_workers_banner_healthy() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    insert_test_worker(&mut conn, "w-healthy-1", "Active", 0).await;
    insert_test_worker(&mut conn, "w-healthy-2", "Active", 0).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/workers").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("Healthy"),
        "banner should say Healthy with all fresh Active workers: {html}"
    );
}

/// Health banner: some stale workers but at least one active → Degraded.
#[tokio::test]
async fn ui_workers_banner_degraded_stale() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    insert_test_worker(&mut conn, "w-active", "Active", 0).await;
    // stale: heartbeat 60 s ago, well past the default 10 s threshold
    insert_test_worker(&mut conn, "w-stale", "Active", -60).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/workers").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("Degraded"),
        "banner should say Degraded when stale workers exist: {html}"
    );
}

/// Health banner: no Active workers at all → Unhealthy.
#[tokio::test]
async fn ui_workers_banner_unhealthy_no_active() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    insert_test_worker(&mut conn, "w-stopped", "Stopped", -5).await;
    insert_test_worker(&mut conn, "w-draining", "Draining", -5).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/workers").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("Unhealthy"),
        "banner should say Unhealthy when no Active workers: {html}"
    );
}

/// Worker rows: ID, status, relative heartbeat time and in-flight count are rendered.
#[tokio::test]
async fn ui_workers_shows_worker_rows() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    insert_test_worker(&mut conn, "worker-alpha", "Active", 0).await;
    insert_test_worker(&mut conn, "worker-beta", "Draining", -3).await;
    insert_test_worker(&mut conn, "worker-gamma", "Stopped", -5).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/workers").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("worker-a"), "alpha worker missing: {html}");
    assert!(html.contains("worker-b"), "beta worker missing: {html}");
    assert!(html.contains("worker-g"), "gamma worker missing: {html}");
    assert!(html.contains("Active"), "Active status missing: {html}");
    assert!(html.contains("Draining"), "Draining status missing: {html}");
    assert!(html.contains("Stopped"), "Stopped status missing: {html}");
    // Relative time should appear (e.g. "just now", "Xs ago")
    assert!(
        html.contains("ago") || html.contains("just now"),
        "relative heartbeat time missing: {html}"
    );
}

/// Stale workers are visually flagged in the table.
#[tokio::test]
async fn ui_workers_stale_rows_flagged() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    insert_test_worker(&mut conn, "w-fresh", "Active", 0).await;
    insert_test_worker(&mut conn, "w-old", "Active", -120).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/workers").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("stale"),
        "stale workers should be flagged in the table: {html}"
    );
}

/// Filter ?status=Active shows only Active workers.
#[tokio::test]
async fn ui_workers_filter_by_status_active() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    insert_test_worker(&mut conn, "w-act", "Active", 0).await;
    insert_test_worker(&mut conn, "w-drain", "Draining", 0).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/workers?status=Active").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("w-act"),
        "Active worker should appear: {html}"
    );
    assert!(
        !html.contains("w-drain"),
        "Draining worker should NOT appear after status=Active filter: {html}"
    );
}

/// Filter ?stale=true shows only stale workers.
#[tokio::test]
async fn ui_workers_filter_stale_true() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    insert_test_worker(&mut conn, "w-fresh-2", "Active", 0).await;
    insert_test_worker(&mut conn, "w-stale-2", "Active", -120).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/workers?stale=true").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("w-stale-"),
        "stale worker should appear: {html}"
    );
    assert!(
        !html.contains("w-fresh-"),
        "fresh worker should NOT appear after ?stale=true filter: {html}"
    );
}

/// RED (was): `?status=zombie` used to `?`-abort `list_workers_ui` with a
/// bare 400 before the filter form was ever rendered, discarding the
/// `build_id` filter the operator had already typed alongside it — the same
/// discard-the-page-on-bad-filter pattern fixed for the Workflows page's
/// `started_after`/`started_before` in #1333 (that PR's commit message
/// names this exact test, `ui_workers_unknown_status_value_returns_400`, as
/// evidence the pattern was systemic, not a one-off).
///
/// GREEN (this commit): the request still renders the Workers page (`200`),
/// preserves the other filter (`build_id=abc123`, still in its input's
/// `value=`), and surfaces a `role="alert"` message naming the bad value and
/// the valid options next to the Status field — the same four error-path
/// booleans (adjacent to cause, persists until resolved, says how to
/// recover, entered data preserved) the Workflows-page fix established now
/// hold here too.
#[tokio::test]
async fn ui_workers_unknown_status_value_redisplays_form_instead_of_aborting_page() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);

    let (status, html) = fetch_html(&app, "/workers?status=zombie&build_id=abc123").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "an unknown status value must not abort the whole Workers page: {html}"
    );
    assert!(
        html.contains("name=\"status\""),
        "filter form must still render: {html}"
    );
    assert!(
        html.contains("value=\"abc123\""),
        "the other filter the operator already typed must not be discarded: {html}"
    );
    assert!(
        html.contains("role=\"alert\""),
        "an inline, screen-reader-announced error must sit next to the field: {html}"
    );
    assert!(
        html.contains("zombie") && html.contains("Active"),
        "the error must name the bad value and a valid option: {html}"
    );
    assert!(
        html.contains("option value=\"zombie\" selected"),
        "the Status select must echo the invalid value back as its selected \
         option, not silently revert to 'All' (Codex review, #1378 P2): {html}"
    );
}

/// Codex review on #1378 (P2): the first version of this fix parsed the
/// invalid value down to `None` before it ever reached
/// `render_workers_page`, so the pagination links and a plain form
/// resubmission were built without it — a Next click (or clicking Apply
/// with nothing changed) silently dropped the still-unresolved `status`
/// filter and its error, one click after the operator saw it. Fixed by
/// carrying the raw text alongside the parsed value all the way to
/// `build_worker_query_string`.
#[tokio::test]
async fn ui_workers_invalid_status_value_persists_across_pagination() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    insert_test_worker(&mut conn, "pg-worker-zombie-a", "Active", 0).await;
    insert_test_worker(&mut conn, "pg-worker-zombie-b", "Active", 0).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/workers?status=zombie&limit=1").await;
    assert_eq!(status, StatusCode::OK, "unexpected status: {html}");
    assert!(
        html.contains("Next"),
        "test setup must produce a Next link to exercise pagination: {html}"
    );
    assert!(
        html.contains("status=zombie"),
        "the invalid status must be carried into the Next/Previous links \
         instead of being dropped on the next page view: {html}"
    );
}

/// Same fix, `stale` side — a distinct code path in `list_workers_ui`, and
/// the one most likely to be hit organically rather than only by hand-edited
/// URLs: matching is case-sensitive by design (only the literal `true`
/// applies the filter), so a capitalized `True` — plausible from a
/// runbook example, a shell variable, or a JSON boolean serialized
/// upstream — used to `?`-abort the page the same way `status=zombie` did.
#[tokio::test]
async fn ui_workers_unknown_stale_value_redisplays_form_instead_of_aborting_page() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);

    let (status, html) = fetch_html(&app, "/workers?stale=True&build_id=abc123").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "an unknown stale value must not abort the whole Workers page: {html}"
    );
    assert!(
        html.contains("value=\"abc123\""),
        "the other filter the operator already typed must not be discarded: {html}"
    );
    assert!(
        html.contains("role=\"alert\""),
        "an inline, screen-reader-announced error must sit next to the field: {html}"
    );
    assert!(
        html.contains("True"),
        "the error must name the exact bad value the operator sent: {html}"
    );
}

/// `shard` used to be typed `Option<i32>` straight on the Workers page's
/// `Query<..>` extractor struct. A non-numeric value failed axum's own
/// query deserialization, a bare framework 400 before `list_workers_ui`
/// ever ran. That is one layer earlier than the status/stale page-abort
/// bug #1378 already fixed on this same page.
#[tokio::test]
async fn ui_workers_invalid_shard_value_redisplays_form_instead_of_aborting_page() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);

    let (status, html) = fetch_html(&app, "/workers?shard=north&build_id=abc123").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "an invalid shard value must not abort the whole Workers page: {html}"
    );
    assert!(
        html.contains("value=\"abc123\""),
        "the other filter the operator already typed must not be discarded: {html}"
    );
    assert!(
        html.contains("role=\"alert\""),
        "an inline, screen-reader-announced error must sit next to the field: {html}"
    );
    assert!(
        html.contains("value=\"north\""),
        "the operator's exact invalid text must be echoed back into the field: {html}"
    );
    assert!(
        html.contains("shard") && html.contains("north"),
        "the error must name the field and the bad value: {html}"
    );
}

/// Pagination: ?limit=1 caps the rendered set to one row.
#[tokio::test]
async fn ui_workers_pagination_limits_rows() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    insert_test_worker(&mut conn, "pg-worker-1", "Active", 0).await;
    insert_test_worker(&mut conn, "pg-worker-2", "Active", 0).await;
    insert_test_worker(&mut conn, "pg-worker-3", "Active", 0).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/workers?limit=1").await;
    assert_eq!(status, StatusCode::OK);
    // With limit=1, exactly one row appears and the Next pagination link is present.
    let worker_count = html.matches("pg-worke").count();
    assert_eq!(
        worker_count, 1,
        "limit=1 should render exactly 1 worker row: {html}"
    );
    assert!(
        html.contains("Next"),
        "Next pagination link should appear when there are more rows: {html}"
    );
}

/// Multi-shard: workers from two shards both appear and are grouped.
#[tokio::test]
async fn ui_workers_multi_shard_grouped() {
    let ((shard0_url, shard1_url), _container) = setup_sharded_test_database_urls().await;

    let mut conn0 = AsyncPgConnection::establish(&shard0_url).await.unwrap();
    let mut conn1 = AsyncPgConnection::establish(&shard1_url).await.unwrap();
    insert_test_worker(&mut conn0, "shard0-worker", "Active", 0).await;
    insert_test_worker(&mut conn1, "shard1-worker", "Active", 0).await;

    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(build_two_shard_pool(&shard0_url, &shard1_url));
    api_state.install(HarvestApiRuntime::new(
        echo_registry(),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        None,
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::new(
            vec![ShardId::new(0), ShardId::new(1)],
            vec![ShardId::new(0), ShardId::new(1)],
            ShardId::new(0),
        ),
    ));
    let app = harvest_ui_router(api_state).with_state(test_app_state_without_database());

    let (status, html) = fetch_html(&app, "/workers").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("shard0-w"), "shard 0 worker missing: {html}");
    assert!(html.contains("shard1-w"), "shard 1 worker missing: {html}");
    // Multi-shard deployments should group workers with shard headers.
    assert!(
        html.contains("Shard") || html.contains("shard"),
        "multi-shard page should show shard grouping: {html}"
    );
}

/// Partial shard failure: degraded banner + shard-unavailable stub.
#[tokio::test]
async fn ui_workers_partial_shard_failure_degraded() {
    let (good_url, _container) = setup_test_database_url().await;
    let mut conn = AsyncPgConnection::establish(&good_url).await.unwrap();
    insert_test_worker(&mut conn, "good-worker", "Active", 0).await;

    // Build a two-shard pool where shard 1 has an invalid URL so it will error.
    let bad_pool = build_test_pool("postgres://invalid:5432/nonexistent");
    let good_pool = build_test_pool(&good_url);
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), good_pool);
    pools.insert(ShardId::new(1), bad_pool);
    let harvest_pool = HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0)));

    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(harvest_pool);
    api_state.install(HarvestApiRuntime::new(
        echo_registry(),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        None,
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::new(
            vec![ShardId::new(0), ShardId::new(1)],
            vec![ShardId::new(0), ShardId::new(1)],
            ShardId::new(0),
        ),
    ));
    let app = harvest_ui_router(api_state).with_state(test_app_state_without_database());

    let (status, html) = fetch_html(&app, "/workers").await;
    // Must not 5xx — partial shard failure is a degraded scenario, not a crash.
    assert_ne!(
        status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "partial shard failure must not 5xx: {html}"
    );
    assert_eq!(
        status,
        StatusCode::OK,
        "partial shard failure should return 200 with degraded view: {html}"
    );
    assert!(
        html.contains("Degraded") || html.contains("unavailable"),
        "partial shard failure should show Degraded banner or unavailable stub: {html}"
    );
}

/// Issue #619 review — a shard whose pause state cannot be read must not leave
/// the Workers page silently claiming dispatch is flowing.
///
/// Nothing is paused on the reachable shard, so before this fix the paused-queue
/// banner rendered *nothing at all* on a page where one shard's pause state was
/// simply unknown — indistinguishable from a genuinely clean fleet, on exactly
/// the page an operator lands on when investigating idle workers. A hold that
/// exists only on the unread shard is likewise absent from the banner, which is
/// why "we do not know" has to be said out loud.
#[tokio::test]
async fn ui_workers_warns_when_a_shards_pause_state_is_unreadable() {
    let (good_url, _container) = setup_test_database_url().await;
    let mut conn = AsyncPgConnection::establish(&good_url).await.unwrap();
    insert_test_worker(&mut conn, "good-worker", "Active", 0).await;

    // Shard 0 is live and holds no pause; shard 1 is unreachable, so its pause
    // state is UNKNOWN rather than "not paused".
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), build_test_pool(&good_url));
    pools.insert(
        ShardId::new(1),
        build_test_pool("postgres://invalid:5432/nonexistent"),
    );
    let harvest_pool = HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0)));

    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(harvest_pool);
    api_state.install(HarvestApiRuntime::new(
        echo_registry(),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        None,
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::new(
            vec![ShardId::new(0), ShardId::new(1)],
            vec![ShardId::new(0), ShardId::new(1)],
            ShardId::new(0),
        ),
    ));
    let app = harvest_ui_router(api_state).with_state(test_app_state_without_database());

    let (status, html) = fetch_html(&app, "/workers").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "an unreadable shard must degrade the page, never fail it: {html}"
    );
    assert!(
        html.contains("Queue pause state incomplete"),
        "the page must say the pause state is unknown, not render a silent clean \
         banner: {html}"
    );
    assert!(
        html.contains("/admin/queues/paused"),
        "it must point at the authoritative per-shard read: {html}"
    );
    // Nothing is actually held on the reachable shard, so there must be no hold
    // banner claiming otherwise.
    assert!(
        !html.contains("Queue dispatch paused"),
        "no queue is held on the readable shard, so no hold may be claimed: {html}"
    );
}

/// Performance: 1 k workers across 4 shards must render in ≤ 500 ms p95.
///
/// Seeds 250 workers per shard, makes a single page request, and asserts:
/// 1. The page renders successfully (200 OK).
/// 2. The rendered HTML body is non-trivial (contains worker ids).
/// 3. The end-to-end wall-clock time is under the 500 ms budget.
#[tokio::test]
async fn ui_workers_perf_1k_workers_4_shards_under_500ms() {
    // Provision 4 shard databases in a single container.
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let shard_urls = setup_n_shard_databases(&container, 4).await;

    // Seed 250 workers per shard = 1 000 total.
    let workers_per_shard: usize = 250;
    for (shard_idx, url) in shard_urls.iter().enumerate() {
        let mut conn = AsyncPgConnection::establish(url).await.unwrap();
        for w in 0..workers_per_shard {
            let worker_id = format!("perf-s{shard_idx}-w{w}");
            insert_test_worker(&mut conn, &worker_id, "Active", 0).await;
        }
    }

    // Build sharded pool and UI app.
    let mut pools = BTreeMap::new();
    for (i, url) in shard_urls.iter().enumerate() {
        pools.insert(
            ShardId::new(i32::try_from(i).unwrap()),
            build_test_pool(url),
        );
    }
    let harvest_pool = HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0)));
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(harvest_pool);
    api_state.install(HarvestApiRuntime::new(
        echo_registry(),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        None,
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::new(
            (0..4).map(ShardId::new).collect(),
            (0..4).map(ShardId::new).collect(),
            ShardId::new(0),
        ),
    ));
    let app = harvest_ui_router(api_state).with_state(test_app_state_without_database());

    // Warm the lazy shard pools before measuring. This test covers the workers
    // page render/query budget, not first-use connection establishment against
    // Docker-backed Postgres shards.
    let (warm_status, _) = fetch_html(&app, "/workers?limit=200").await;
    assert_eq!(
        warm_status,
        StatusCode::OK,
        "perf test warm-up page must return 200",
    );

    // Measure wall-clock time for the warmed page render.
    let start = std::time::Instant::now();
    let (status, html) = fetch_html(&app, "/workers?limit=200").await;
    let elapsed = start.elapsed();

    assert_eq!(status, StatusCode::OK, "perf test page must return 200");
    assert!(
        html.len() > 10_000,
        "1k-worker page should produce substantial HTML (got {} bytes)",
        html.len()
    );
    assert!(
        elapsed.as_millis() < 500,
        "Workers page with 1k workers must render in < 500 ms (got {} ms)",
        elapsed.as_millis()
    );
}

// ---------------------------------------------------------------------------
// Schedules page tests
// ---------------------------------------------------------------------------

/// Insert a test row into `harvest_schedules`.
/// `kind` is `"Workflow"` or `"Dag"`; `name` is the `workflow_name` / `dag_name`.
async fn insert_test_schedule(
    database_url: &str,
    kind: &str,
    name: &str,
    is_paused: bool,
) -> uuid::Uuid {
    let id = uuid::Uuid::new_v4();
    let mut conn = AsyncPgConnection::establish(database_url)
        .await
        .expect("failed to connect for schedule insert");
    let (dag_col, wf_col) = if kind == "Dag" {
        (format!("'{name}'"), "NULL".to_string())
    } else {
        ("NULL".to_string(), format!("'{name}'"))
    };
    let paused_at_col = if is_paused { "NOW()" } else { "NULL" };
    let sql = format!(
        "INSERT INTO harvest_schedules \
            (id, dag_name, workflow_name, schedule_expr, timezone, catchup, \
             max_active_runs, is_paused, paused_at, created_at, updated_at) \
         VALUES \
            ('{id}', {dag_col}, {wf_col}, '0 * * * *', 'UTC', false, 1, \
             {is_paused}, {paused_at_col}, NOW(), NOW())"
    );
    conn.batch_execute(&sql)
        .await
        .expect("insert_test_schedule failed");
    id
}

async fn insert_test_schedule_with_tz(
    database_url: &str,
    kind: &str,
    name: &str,
    is_paused: bool,
    timezone: &str,
) -> uuid::Uuid {
    let id = uuid::Uuid::new_v4();
    let mut conn = AsyncPgConnection::establish(database_url)
        .await
        .expect("failed to connect for schedule insert");
    let (dag_col, wf_col) = if kind == "Dag" {
        (format!("'{name}'"), "NULL".to_string())
    } else {
        ("NULL".to_string(), format!("'{name}'"))
    };
    let paused_at_col = if is_paused { "NOW()" } else { "NULL" };
    let sql = format!(
        "INSERT INTO harvest_schedules \
            (id, dag_name, workflow_name, schedule_expr, timezone, catchup, \
             max_active_runs, is_paused, paused_at, created_at, updated_at) \
         VALUES \
            ('{id}', {dag_col}, {wf_col}, '0 * * * *', '{timezone}', false, 1, \
             {is_paused}, {paused_at_col}, NOW(), NOW())"
    );
    conn.batch_execute(&sql)
        .await
        .expect("insert_test_schedule_with_tz failed");
    id
}

/// Empty schedules table → page renders "No schedules registered."
#[tokio::test]
async fn ui_schedules_empty_state() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);

    let (status, html) = fetch_html(&app, "/schedules").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "schedules page must return 200: {html}"
    );
    assert!(html.contains("Schedules"), "page heading missing: {html}");
    assert!(
        html.contains("No schedules registered."),
        "empty state message missing: {html}"
    );
    assert!(html.contains("🔭 Vantage"), "layout header missing: {html}");
    assert!(!html.contains("<script"), "no script tags allowed: {html}");
    assert!(!html.contains("http://"), "no external http URLs: {html}");
    assert!(!html.contains("https://"), "no external https URLs: {html}");
}

/// Six schedules (3 Workflow + 3 Dag, mix of paused/active) all render.
#[tokio::test]
async fn ui_schedules_lists_all_rows() {
    let (database_url, _container) = setup_test_database_url().await;
    let id_wf1 = insert_test_schedule(&database_url, "Workflow", "payment_workflow", false).await;
    let id_wf2 = insert_test_schedule(&database_url, "Workflow", "invoice_workflow", true).await;
    let id_wf3 = insert_test_schedule(&database_url, "Workflow", "report_workflow", false).await;
    let id_dag1 = insert_test_schedule(&database_url, "Dag", "nightly_etl", true).await;
    let id_dag2 = insert_test_schedule(&database_url, "Dag", "hourly_sync", false).await;
    let id_dag3 = insert_test_schedule(&database_url, "Dag", "weekly_report", true).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/schedules").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "schedules page must return 200: {html}"
    );

    for id in [id_wf1, id_wf2, id_wf3, id_dag1, id_dag2, id_dag3] {
        assert!(
            html.contains(&id.to_string()),
            "schedule id {id} missing from page: {html}"
        );
    }
    assert!(
        html.contains("payment_workflow"),
        "payment_workflow missing: {html}"
    );
    assert!(
        html.contains("invoice_workflow"),
        "invoice_workflow missing: {html}"
    );
    assert!(html.contains("nightly_etl"), "nightly_etl missing: {html}");
    assert!(html.contains("Workflow"), "Workflow kind missing: {html}");
    assert!(html.contains("Dag"), "Dag kind missing: {html}");
    assert!(html.contains("Paused"), "paused badge missing: {html}");
    assert!(html.contains("Active"), "active badge missing: {html}");
    // Nav must link to Schedules
    assert!(
        html.contains("schedules"),
        "nav must include schedules link: {html}"
    );
}

/// Filter `kind=Workflow` shows only Workflow rows and hides Dag rows.
#[tokio::test]
async fn ui_schedules_filter_by_kind_workflow() {
    let (database_url, _container) = setup_test_database_url().await;
    insert_test_schedule(&database_url, "Workflow", "target_wf", false).await;
    insert_test_schedule(&database_url, "Dag", "target_dag", false).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/schedules?kind=Workflow").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("target_wf"),
        "workflow row missing after kind=Workflow: {html}"
    );
    assert!(
        !html.contains("target_dag"),
        "dag row should not appear after kind=Workflow: {html}"
    );
}

/// Filter `kind=Dag` shows only Dag rows.
#[tokio::test]
async fn ui_schedules_filter_by_kind_dag() {
    let (database_url, _container) = setup_test_database_url().await;
    insert_test_schedule(&database_url, "Workflow", "wf_hidden", false).await;
    insert_test_schedule(&database_url, "Dag", "dag_visible", false).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/schedules?kind=Dag").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("dag_visible"),
        "dag row missing after kind=Dag: {html}"
    );
    assert!(
        !html.contains("wf_hidden"),
        "workflow row should not appear after kind=Dag: {html}"
    );
}

/// `list_schedules_ui` used to `?`-propagate `ScheduleKindFilter::parse`'s
/// `Result` directly. A bad `kind` value aborted the whole page with a
/// bare 400, before the filter form, the table, or the operator's other
/// filters ever rendered. It is the exact page-abort defect already fixed
/// on this page's three sibling list pages: Workflows #1333, Workers
/// #1378, Dead-Letters #1420. It never reached the Schedules page itself.
#[tokio::test]
async fn ui_schedules_invalid_kind_value_redisplays_form_instead_of_aborting_page() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);

    let (status, html) = fetch_html(&app, "/schedules?kind=zombie&target=billing").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "an invalid kind value must not abort the whole Schedules page: {html}"
    );
    assert!(
        html.contains("value=\"billing\""),
        "the other filter the operator already typed must not be discarded: {html}"
    );
    assert!(
        html.contains("role=\"alert\""),
        "an inline, screen-reader-announced error must sit next to the field: {html}"
    );
    assert!(
        html.contains("option value=\"zombie\" selected"),
        "the Kind select must echo the invalid value back as its selected \
         option, not silently revert to 'All': {html}"
    );
    assert!(
        html.contains("zombie") && html.contains("Workflow"),
        "the error must name the bad value and a valid option: {html}"
    );
}

/// Same page-abort defect, `shard_id` side. It was typed `Option<i32>`
/// straight on the `Query<..>` extractor struct, so a non-numeric value
/// failed axum's own query deserialization before the handler ran at all.
/// That is one layer earlier than the `kind`/`paused`/`health` fix above,
/// and with no styled error whatsoever.
#[tokio::test]
async fn ui_schedules_invalid_shard_id_redisplays_form_instead_of_aborting_page() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);

    let (status, html) = fetch_html(&app, "/schedules?shard_id=north&target=billing").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "an invalid shard_id must not abort the whole Schedules page: {html}"
    );
    assert!(
        html.contains("value=\"billing\""),
        "the other filter the operator already typed must not be discarded: {html}"
    );
    assert!(
        html.contains("role=\"alert\""),
        "an inline, screen-reader-announced error must sit next to the field: {html}"
    );
    assert!(
        html.contains("value=\"north\""),
        "the operator's exact invalid text must be echoed back into the field: {html}"
    );
    assert!(
        html.contains("shard_id") && html.contains("north"),
        "the error must name the field and the bad value: {html}"
    );
}

/// Filter `paused=Paused` shows only paused rows.
#[tokio::test]
async fn ui_schedules_filter_by_paused() {
    let (database_url, _container) = setup_test_database_url().await;
    insert_test_schedule(&database_url, "Workflow", "active_wf", false).await;
    insert_test_schedule(&database_url, "Workflow", "paused_wf", true).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/schedules?paused=Paused").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("paused_wf"),
        "paused row missing after paused=Paused: {html}"
    );
    assert!(
        !html.contains("active_wf"),
        "active row should not appear after paused=Paused: {html}"
    );
}

/// Filter `paused=Active` shows only active rows.
#[tokio::test]
async fn ui_schedules_filter_by_active() {
    let (database_url, _container) = setup_test_database_url().await;
    insert_test_schedule(&database_url, "Workflow", "active_visible", false).await;
    insert_test_schedule(&database_url, "Workflow", "paused_hidden", true).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/schedules?paused=Active").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("active_visible"),
        "active row missing after paused=Active: {html}"
    );
    assert!(
        !html.contains("paused_hidden"),
        "paused row should not appear after paused=Active: {html}"
    );
}

/// Filter `target=my_wf` narrows by name substring.
#[tokio::test]
async fn ui_schedules_filter_by_target() {
    let (database_url, _container) = setup_test_database_url().await;
    insert_test_schedule(&database_url, "Workflow", "my_special_wf", false).await;
    insert_test_schedule(&database_url, "Workflow", "other_wf", false).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/schedules?target=my_special").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("my_special_wf"),
        "target row missing after target filter: {html}"
    );
    assert!(
        !html.contains("other_wf"),
        "other row should not appear after target filter: {html}"
    );
}

/// Filter returns empty-set message when nothing matches.
#[tokio::test]
async fn ui_schedules_filter_no_match() {
    let (database_url, _container) = setup_test_database_url().await;
    insert_test_schedule(&database_url, "Workflow", "some_workflow", false).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/schedules?target=nonexistent").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("No schedules match this filter"),
        "empty filter message missing: {html}"
    );
}

/// Pause action: POST to /schedules/{id}/pause → 303 redirect with flash.
#[tokio::test]
async fn ui_schedules_pause_action_redirects() {
    let (database_url, _container) = setup_test_database_url().await;
    let id = insert_test_schedule(&database_url, "Workflow", "pause_target", false).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, headers, _body) =
        post_form(&app, &format!("/schedules/{id}/pause"), String::new()).await;
    assert!(
        status == StatusCode::SEE_OTHER || status == StatusCode::FOUND,
        "pause action must redirect (got {status})"
    );
    let location = headers
        .get("location")
        .expect("redirect must have Location header")
        .to_str()
        .unwrap();
    assert!(
        location.contains("schedules"),
        "redirect must go back to the schedules page: {location}"
    );
    assert!(
        location.contains("flash"),
        "redirect must carry a flash message: {location}"
    );

    // Verify the DB row is now paused.
    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    let is_paused: bool = autumn_harvest::schema::harvest_schedules::table
        .filter(autumn_harvest::schema::harvest_schedules::id.eq(id))
        .select(autumn_harvest::schema::harvest_schedules::is_paused)
        .first(&mut conn)
        .await
        .expect("schedule should exist");
    assert!(is_paused, "schedule should be paused after pause action");
}

/// Resume action: POST to /schedules/{id}/resume → 303 redirect with flash.
#[tokio::test]
async fn ui_schedules_resume_action_redirects() {
    let (database_url, _container) = setup_test_database_url().await;
    let id = insert_test_schedule(&database_url, "Workflow", "resume_target", true).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, headers, _body) =
        post_form(&app, &format!("/schedules/{id}/resume"), String::new()).await;
    assert!(
        status == StatusCode::SEE_OTHER || status == StatusCode::FOUND,
        "resume action must redirect (got {status})"
    );
    let location = headers
        .get("location")
        .expect("redirect must have Location header")
        .to_str()
        .unwrap();
    assert!(
        location.contains("schedules"),
        "redirect must go back to the schedules page: {location}"
    );
    assert!(
        location.contains("flash"),
        "redirect must carry a flash message: {location}"
    );

    // Verify the DB row is now active.
    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    let is_paused: bool = autumn_harvest::schema::harvest_schedules::table
        .filter(autumn_harvest::schema::harvest_schedules::id.eq(id))
        .select(autumn_harvest::schema::harvest_schedules::is_paused)
        .first(&mut conn)
        .await
        .expect("schedule should exist");
    assert!(!is_paused, "schedule should be active after resume action");
}

/// Delete action: POST to /schedules/{id}/delete → 303 redirect with flash.
#[tokio::test]
async fn ui_schedules_delete_action_redirects() {
    let (database_url, _container) = setup_test_database_url().await;
    let id = insert_test_schedule(&database_url, "Workflow", "delete_target", false).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, headers, _body) =
        post_form(&app, &format!("/schedules/{id}/delete"), String::new()).await;
    assert!(
        status == StatusCode::SEE_OTHER || status == StatusCode::FOUND,
        "delete action must redirect (got {status})"
    );
    let location = headers
        .get("location")
        .expect("redirect must have Location header")
        .to_str()
        .unwrap();
    assert!(
        location.contains("schedules"),
        "redirect must go back to the schedules page: {location}"
    );
    assert!(
        location.contains("flash"),
        "redirect must carry a flash message: {location}"
    );

    // Verify the row is gone.
    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    let count: i64 = autumn_harvest::schema::harvest_schedules::table
        .filter(autumn_harvest::schema::harvest_schedules::id.eq(id))
        .count()
        .get_result(&mut conn)
        .await
        .expect("count query");
    assert_eq!(count, 0, "schedule should be deleted");
}

/// Auto-refresh: `?refresh=30` emits a meta http-equiv refresh tag.
///
/// The tag's `content` carries an explicit, flash-free `url=` target
/// (Codex review, #1437 P2), not a bare interval. A repeating reload
/// must land on the operator's current filtered view, instead of
/// looping on a stale flash message — see `layout_schedules`'s own doc
/// comment.
#[tokio::test]
async fn ui_schedules_auto_refresh_meta_tag() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);

    let (status, html) = fetch_html(&app, "/schedules?refresh=30").await;
    assert_eq!(status, StatusCode::OK);
    // Maud HTML-escapes attribute values. The rendered `&` between query
    // params comes back as `&amp;`, same as every other multi-param
    // assertion in this file (Codex review, #1437).
    assert!(
        html.contains(r#"content="30; url=schedules?page=0&amp;refresh=30""#)
            && html.contains("http-equiv=\"refresh\""),
        "auto-refresh meta tag with content=30 and a flash-free target missing: {html}"
    );

    let (_, html_no_refresh) = fetch_html(&app, "/schedules").await;
    assert!(
        !html_no_refresh.contains("http-equiv=\"refresh\""),
        "refresh tag must not appear when refresh not requested: {html_no_refresh}"
    );
}

/// Bulk pause/resume forms are present in the HTML.
#[tokio::test]
async fn ui_schedules_bulk_actions_present() {
    let (database_url, _container) = setup_test_database_url().await;
    insert_test_schedule(&database_url, "Workflow", "bulk_wf", false).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/schedules").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("bulk-pause") || html.contains("Pause all"),
        "bulk pause action missing: {html}"
    );
    assert!(
        html.contains("bulk-resume") || html.contains("Resume all"),
        "bulk resume action missing: {html}"
    );
}

/// Bulk pause: POST to /schedules/bulk-pause with filter → redirects and pauses matching rows.
#[tokio::test]
async fn ui_schedules_bulk_pause_pauses_matching_rows() {
    let (database_url, _container) = setup_test_database_url().await;
    let id1 = insert_test_schedule(&database_url, "Workflow", "bulk_target_a", false).await;
    let id2 = insert_test_schedule(&database_url, "Workflow", "bulk_target_b", false).await;
    let id_other = insert_test_schedule(&database_url, "Dag", "other_dag", false).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, headers, _body) =
        post_form(&app, "/schedules/bulk-pause", "kind=Workflow&paused=Active").await;
    assert!(
        status == StatusCode::SEE_OTHER || status == StatusCode::FOUND,
        "bulk-pause must redirect (got {status})"
    );
    let location = headers
        .get("location")
        .expect("redirect must have Location header")
        .to_str()
        .unwrap();
    assert!(
        location.contains("schedules"),
        "redirect must go to schedules: {location}"
    );

    // Verify workflow schedules are paused, dag is not.
    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    let paused1: bool = autumn_harvest::schema::harvest_schedules::table
        .filter(autumn_harvest::schema::harvest_schedules::id.eq(id1))
        .select(autumn_harvest::schema::harvest_schedules::is_paused)
        .first(&mut conn)
        .await
        .unwrap();
    let paused2: bool = autumn_harvest::schema::harvest_schedules::table
        .filter(autumn_harvest::schema::harvest_schedules::id.eq(id2))
        .select(autumn_harvest::schema::harvest_schedules::is_paused)
        .first(&mut conn)
        .await
        .unwrap();
    let paused_other: bool = autumn_harvest::schema::harvest_schedules::table
        .filter(autumn_harvest::schema::harvest_schedules::id.eq(id_other))
        .select(autumn_harvest::schema::harvest_schedules::is_paused)
        .first(&mut conn)
        .await
        .unwrap();
    assert!(paused1, "bulk_target_a should be paused");
    assert!(paused2, "bulk_target_b should be paused");
    assert!(
        !paused_other,
        "other_dag should NOT be paused (different kind)"
    );
}

/// Codex review on #1437 (P2): before this fix, a bulk-action redirect
/// always landed on a bare, unfiltered `schedules?flash=…`. That dropped
/// whatever filters the operator had set, including a still-unresolved
/// invalid value and its inline error, on the very next Pause/Resume
/// click. Submitting the rendered `return_to` hidden field must land
/// back on the same filtered view instead.
#[tokio::test]
async fn ui_schedules_bulk_pause_redirect_preserves_the_filtered_view() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);

    let (status, headers, _body) = post_form(
        &app,
        "/schedules/bulk-pause",
        "kind=Workflow&return_to=..%2Fschedules%3Fkind%3DWorkflow",
    )
    .await;
    assert!(
        status == StatusCode::SEE_OTHER || status == StatusCode::FOUND,
        "bulk-pause must redirect (got {status})"
    );
    let location = headers
        .get("location")
        .expect("redirect must have Location header")
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        location.starts_with("../schedules?kind=Workflow&flash="),
        "the redirect must preserve the operator's filtered view, not drop it: {location}"
    );

    // Codex review on #1437 (P2): a relative `Location` resolves against
    // the URL this test posted to (`/schedules/bulk-pause`), not the
    // list page. Follow it the way a browser does: RFC 3986 §5.3 has
    // `../` step back out of `bulk-pause`'s own directory. Confirm it
    // actually lands on the Schedules list, instead of the doubled-path
    // 404 a bare `schedules?...` would have produced.
    let resolved = format!(
        "/{}",
        location
            .strip_prefix("../")
            .expect("location must start with ../ to resolve correctly from bulk-pause")
    );
    assert!(
        resolved.starts_with("/schedules?kind=Workflow&flash="),
        "unexpected resolved redirect target: {resolved}"
    );
    let (list_status, list_body) = fetch_html(&app, &resolved).await;
    assert_eq!(
        list_status,
        StatusCode::OK,
        "the resolved redirect target must actually load the Schedules list, not 404: {list_body}"
    );
}

/// Codex review on #1437 (P2): a `return_to` with a form-decoded control
/// character (`%0A` decodes to a raw newline) used to reach
/// `axum::response::Redirect::to` unfiltered. `HeaderValue::try_from`
/// rejects that byte, so the mutation succeeded but the response became
/// an internal error instead of a redirect to the operator's filtered
/// view.
#[tokio::test]
async fn ui_schedules_bulk_pause_rejects_control_characters_in_return_to() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);

    let (status, headers, body) = post_form(
        &app,
        "/schedules/bulk-pause",
        "return_to=..%2Fschedules%3Fx%3Da%0Ab",
    )
    .await;
    assert!(
        status == StatusCode::SEE_OTHER || status == StatusCode::FOUND,
        "a return_to with a control character must fall back to the safe \
         default redirect, not fail the response after the mutation already \
         ran (got {status}): {body}"
    );
    let location = headers
        .get("location")
        .expect("redirect must have Location header")
        .to_str()
        .unwrap();
    assert!(
        location.starts_with("../schedules?flash="),
        "must fall back to the safe default, not carry the raw control character: {location}"
    );
}

/// Codex review on #1437 (P1): before this fix, an invalid `shard_id` in
/// a bulk-action POST silently dropped to "no shard restriction". It
/// paused schedules on every shard instead of rejecting the request —
/// the exact scope-broadening a mutating endpoint must never allow.
#[tokio::test]
async fn ui_schedules_bulk_pause_rejects_invalid_shard_id_instead_of_broadening_scope() {
    let (database_url, _container) = setup_test_database_url().await;
    let id = insert_test_schedule(&database_url, "Workflow", "shard_guard_target", false).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, _headers, body) = post_form(&app, "/schedules/bulk-pause", "shard_id=north").await;
    assert!(
        status.is_client_error(),
        "an invalid shard_id must reject the bulk action, not silently drop the \
         restriction and pause every shard: got {status}, body: {body}"
    );

    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    let paused: bool = autumn_harvest::schema::harvest_schedules::table
        .filter(autumn_harvest::schema::harvest_schedules::id.eq(id))
        .select(autumn_harvest::schema::harvest_schedules::is_paused)
        .first(&mut conn)
        .await
        .unwrap();
    assert!(
        !paused,
        "no schedule should be paused when the bulk action itself is rejected"
    );
}

/// Pagination: `?limit=1` caps the list to 1 row and shows Next link.
#[tokio::test]
async fn ui_schedules_pagination() {
    let (database_url, _container) = setup_test_database_url().await;
    insert_test_schedule(&database_url, "Workflow", "pag_wf_1", false).await;
    insert_test_schedule(&database_url, "Workflow", "pag_wf_2", false).await;
    insert_test_schedule(&database_url, "Workflow", "pag_wf_3", false).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/schedules?limit=1").await;
    assert_eq!(status, StatusCode::OK);
    let row_count = html.matches("pag_wf_").count();
    assert_eq!(row_count, 1, "limit=1 should render exactly 1 row: {html}");
    assert!(
        html.contains("Next"),
        "Next pagination link missing when there are more rows: {html}"
    );
}

/// Multi-shard: schedules from two shards both appear.
#[tokio::test]
async fn ui_schedules_multi_shard() {
    let ((shard0_url, shard1_url), _container) = setup_sharded_test_database_urls().await;
    insert_test_schedule(&shard0_url, "Workflow", "shard0_schedule", false).await;
    insert_test_schedule(&shard1_url, "Dag", "shard1_dag_schedule", false).await;

    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(build_two_shard_pool(&shard0_url, &shard1_url));
    api_state.install(HarvestApiRuntime::new(
        echo_registry(),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        None,
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::new(
            vec![ShardId::new(0), ShardId::new(1)],
            vec![ShardId::new(0), ShardId::new(1)],
            ShardId::new(0),
        ),
    ));
    let app = harvest_ui_router(api_state).with_state(test_app_state_without_database());

    let (status, html) = fetch_html(&app, "/schedules").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "multi-shard schedules page must return 200: {html}"
    );
    assert!(
        html.contains("shard0_schedule"),
        "shard 0 schedule missing: {html}"
    );
    assert!(
        html.contains("shard1_dag_schedule"),
        "shard 1 schedule missing: {html}"
    );
}

/// Partial shard failure: renders 200 with shard-error banner, does not 5xx.
#[tokio::test]
async fn ui_schedules_partial_shard_failure() {
    let (good_url, _container) = setup_test_database_url().await;
    insert_test_schedule(&good_url, "Workflow", "good_shard_schedule", false).await;

    let bad_pool = build_test_pool("postgres://invalid:5432/nonexistent");
    let good_pool = build_test_pool(&good_url);
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), good_pool);
    pools.insert(ShardId::new(1), bad_pool);
    let harvest_pool = HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0)));

    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(harvest_pool);
    api_state.install(HarvestApiRuntime::new(
        echo_registry(),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        None,
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::new(
            vec![ShardId::new(0), ShardId::new(1)],
            vec![ShardId::new(0), ShardId::new(1)],
            ShardId::new(0),
        ),
    ));
    let app = harvest_ui_router(api_state).with_state(test_app_state_without_database());

    let (status, html) = fetch_html(&app, "/schedules").await;
    assert_ne!(
        status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "partial shard failure must not 5xx: {html}"
    );
    assert_eq!(
        status,
        StatusCode::OK,
        "partial shard failure should return 200: {html}"
    );
    assert!(
        html.contains("unavailable") || html.contains("Shard"),
        "partial shard failure should show shard-error banner: {html}"
    );
    assert!(
        html.contains("good_shard_schedule"),
        "good shard row should still appear: {html}"
    );
}

/// Flash message from a redirect renders in the schedules list page.
#[tokio::test]
async fn ui_schedules_flash_message_renders() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);

    let (status, html) = fetch_html(&app, "/schedules?flash=Paused+my_workflow").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("Paused my_workflow") || html.contains("Paused+my_workflow"),
        "flash message should appear on the page: {html}"
    );
}

/// Existing nav layouts include a Schedules link so operators can navigate.
#[tokio::test]
async fn ui_all_pages_have_schedules_nav_link() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);

    for path in ["/workflows", "/workers", "/schedules"] {
        let (status, html) = fetch_html(&app, path).await;
        assert_eq!(status, StatusCode::OK, "page {path} must render: {html}");
        assert!(
            html.contains("href=\"schedules\"")
                || html.contains("href=\"/schedules\"")
                || html.contains(">Schedules<"),
            "page {path} must include a Schedules nav link: {html}"
        );
    }
}

// ── issue #951: schedules management page ───────────────────────────────────

/// Health / policy columns the #951 list view must surface. Every value maps to
/// an existing `harvest_schedules` column, so this seeds a row exactly as the
/// scheduler would leave it.
#[derive(Default)]
struct ScheduleFixture<'a> {
    kind: &'a str,
    name: &'a str,
    is_paused: bool,
    jitter_secs: i64,
    overlap_policy: Option<&'a str>,
    catchup_policy: Option<&'a str>,
    catchup_window_secs: Option<i64>,
    last_catchup_dropped: i32,
    max_runs: Option<i32>,
    runs_started: i32,
    exhausted_reason: Option<&'a str>,
    /// Force `exhausted_at` even with no reason recorded — a state the schema
    /// permits and the badge renderer handles separately.
    exhausted: bool,
    auto_paused: bool,
    next_run_at_sql: Option<&'a str>,
    schedule_expr: Option<&'a str>,
}

async fn insert_schedule_fixture(database_url: &str, fixture: &ScheduleFixture<'_>) -> uuid::Uuid {
    let id = uuid::Uuid::new_v4();
    let mut conn = AsyncPgConnection::establish(database_url)
        .await
        .expect("failed to connect for schedule fixture insert");
    // Quote-escape every interpolated literal so a fixture can seed a hostile
    // name (the pure tests use one containing a single quote) and get a row
    // rather than a syntax error.
    let sql_lit = |s: &str| format!("'{}'", s.replace('\'', "''"));
    let (dag_col, wf_col) = if fixture.kind == "Dag" {
        (sql_lit(fixture.name), "NULL".to_string())
    } else {
        ("NULL".to_string(), sql_lit(fixture.name))
    };
    let sql_opt_str = |v: Option<&str>| v.map_or_else(|| "NULL".to_string(), sql_lit);
    let sql_opt_i = |v: Option<i64>| v.map_or_else(|| "NULL".to_string(), |n| n.to_string());
    let sql = format!(
        "INSERT INTO harvest_schedules \
            (id, dag_name, workflow_name, schedule_expr, timezone, catchup, \
             max_active_runs, is_paused, paused_at, created_at, updated_at, \
             jitter_secs, overlap_policy, catchup_policy, catchup_window_secs, \
             last_catchup_dropped, last_catchup_at, max_runs, runs_started, \
             exhausted_at, exhausted_reason, auto_paused_at, next_run_at) \
         VALUES \
            ('{id}', {dag_col}, {wf_col}, {expr}, 'UTC', false, 1, {is_paused}, \
             {paused_at}, NOW(), NOW(), {jitter}, {overlap}, {catchup_policy}, \
             {catchup_window}, {dropped}, {last_catchup_at}, {max_runs}, {runs_started}, \
             {exhausted_at}, {exhausted_reason}, {auto_paused_at}, {next_run_at})",
        expr = sql_opt_str(Some(fixture.schedule_expr.unwrap_or("0 * * * *"))),
        is_paused = fixture.is_paused,
        paused_at = if fixture.is_paused { "NOW()" } else { "NULL" },
        jitter = fixture.jitter_secs,
        overlap = sql_opt_str(Some(fixture.overlap_policy.unwrap_or("skip"))),
        catchup_policy = sql_opt_str(fixture.catchup_policy),
        catchup_window = sql_opt_i(fixture.catchup_window_secs),
        dropped = fixture.last_catchup_dropped,
        last_catchup_at = if fixture.last_catchup_dropped > 0 {
            "NOW()"
        } else {
            "NULL"
        },
        max_runs = sql_opt_i(fixture.max_runs.map(i64::from)),
        runs_started = fixture.runs_started,
        exhausted_at = if fixture.exhausted_reason.is_some() || fixture.exhausted {
            "NOW()"
        } else {
            "NULL"
        },
        exhausted_reason = sql_opt_str(fixture.exhausted_reason),
        auto_paused_at = if fixture.auto_paused { "NOW()" } else { "NULL" },
        next_run_at = fixture.next_run_at_sql.unwrap_or("NULL"),
    );
    conn.batch_execute(&sql)
        .await
        .expect("insert_schedule_fixture failed");
    id
}

/// Seed one execution attributed to a schedule, as the scheduler/backfill/manual
/// trigger paths do (`schedule_id` + `origin` + `scheduled_for`), so the run
/// history drill-down has something to render.
async fn insert_schedule_run(
    database_url: &str,
    shard: ShardId,
    schedule_id: uuid::Uuid,
    workflow_name: &str,
    state: &str,
    origin: &str,
    scheduled_for_sql: &str,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(shard);
    let mut conn = AsyncPgConnection::establish(database_url)
        .await
        .expect("failed to connect for schedule run insert");
    let completed = if state == "RUNNING" { "NULL" } else { "NOW()" };
    let sql = format!(
        "INSERT INTO harvest_workflow_executions \
            (id, workflow_name, workflow_id, run_id, shard_id, state, input, \
             queue_name, started_at, completed_at, schedule_id, scheduled_for, origin) \
         VALUES \
            ('{exec}', '{workflow_name}', '{exec}', gen_random_uuid(), {shard}, '{state}', \
             '{{}}'::jsonb, 'default', NOW() - INTERVAL '1 hour', {completed}, \
             '{schedule_id}', {scheduled_for_sql}, '{origin}')",
        exec = exec_id.as_uuid(),
        shard = shard.as_i32(),
    );
    conn.batch_execute(&sql)
        .await
        .expect("insert_schedule_run failed");
    exec_id
}

/// AC2/AC3: a list containing a paused schedule and an exhausted schedule
/// renders every policy/health field and floats the unhealthy rows to the top.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn ui_schedules_list_renders_health_policy_and_bounded_run_state() {
    let (database_url, _container) = setup_test_database_url().await;

    let healthy = insert_schedule_fixture(
        &database_url,
        &ScheduleFixture {
            kind: "Workflow",
            name: "healthy_nightly",
            jitter_secs: 300,
            overlap_policy: Some("buffer_all"),
            next_run_at_sql: Some("NOW() + INTERVAL '1 hour'"),
            ..Default::default()
        },
    )
    .await;
    let paused = insert_schedule_fixture(
        &database_url,
        &ScheduleFixture {
            kind: "Workflow",
            name: "paused_billing",
            is_paused: true,
            next_run_at_sql: Some("NOW() + INTERVAL '10 hours'"),
            ..Default::default()
        },
    )
    .await;
    let exhausted = insert_schedule_fixture(
        &database_url,
        &ScheduleFixture {
            kind: "Dag",
            name: "exhausted_etl",
            max_runs: Some(3),
            runs_started: 3,
            exhausted_reason: Some("max_runs_exhausted"),
            next_run_at_sql: Some("NOW() + INTERVAL '20 hours'"),
            ..Default::default()
        },
    )
    .await;
    let dropped = insert_schedule_fixture(
        &database_url,
        &ScheduleFixture {
            kind: "Workflow",
            name: "catchup_dropped_sync",
            catchup_policy: Some("window"),
            catchup_window_secs: Some(3600),
            last_catchup_dropped: 5,
            next_run_at_sql: Some("NOW() + INTERVAL '30 hours'"),
            ..Default::default()
        },
    )
    .await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/schedules").await;
    assert_eq!(status, StatusCode::OK, "schedules page must render: {html}");

    // AC2: every field group is on the page.
    assert!(html.contains("Health"), "health column missing: {html}");
    assert!(html.contains("Overlap"), "overlap column missing: {html}");
    assert!(html.contains("Catchup"), "catchup column missing: {html}");
    assert!(
        html.contains("Bounded runs"),
        "bounded-run column missing: {html}"
    );
    assert!(
        html.contains("buffer_all"),
        "overlap policy value missing: {html}"
    );
    assert!(
        html.contains("effective"),
        "jitter-adjusted effective fire time missing: {html}"
    );
    assert!(
        html.contains("window (3600s)"),
        "catchup policy + window missing: {html}"
    );
    assert!(
        html.contains("dropped 5"),
        "catchup drop count missing: {html}"
    );
    assert!(
        html.contains("0 of 3 left"),
        "bounded-run budget missing: {html}"
    );

    // AC3: each unhealthy state has its own badge.
    assert!(html.contains("Paused"), "paused badge missing: {html}");
    assert!(
        html.contains("Exhausted"),
        "exhausted badge missing: {html}"
    );
    assert!(
        html.contains("max_runs_exhausted"),
        "exhaustion reason missing: {html}"
    );
    assert!(
        html.contains("Catchup dropped"),
        "catchup-dropped badge missing: {html}"
    );
    assert!(
        html.contains("Needs attention"),
        "unhealthy summary strip missing: {html}"
    );

    // AC3: unhealthy rows sort above the healthy one despite firing later.
    let pos = |id: uuid::Uuid| {
        html.find(&id.to_string())
            .unwrap_or_else(|| panic!("schedule {id} missing from page: {html}"))
    };
    let healthy_pos = pos(healthy);
    for unhealthy in [paused, exhausted, dropped] {
        assert!(
            pos(unhealthy) < healthy_pos,
            "unhealthy schedule {unhealthy} must sort above the healthy row: {html}"
        );
    }

    assert!(!html.contains("<script"), "no script tags allowed: {html}");
}

/// AC3: `health=Unhealthy` narrows the list to schedules that need attention.
#[tokio::test]
async fn ui_schedules_health_filter_narrows_to_unhealthy_rows() {
    let (database_url, _container) = setup_test_database_url().await;
    insert_schedule_fixture(
        &database_url,
        &ScheduleFixture {
            kind: "Workflow",
            name: "healthy_row",
            ..Default::default()
        },
    )
    .await;
    insert_schedule_fixture(
        &database_url,
        &ScheduleFixture {
            kind: "Workflow",
            name: "paused_row",
            is_paused: true,
            ..Default::default()
        },
    )
    .await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/schedules?health=Unhealthy").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("paused_row"), "unhealthy row missing: {html}");
    assert!(
        !html.contains("healthy_row"),
        "healthy row must be filtered out: {html}"
    );

    // #1437 (this PR's own subject): an unknown health value used to
    // `?`-abort the whole page with a bare 400. It now degrades to
    // "filter not applied" instead, like every other Vantage list-page
    // filter. A visible `role="alert"` error names the bad value. That
    // is neither the silent all-match the earlier version of this test
    // worried about, nor a page-abort.
    let (status, html) = fetch_html(&app, "/schedules?health=bogus").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "an unknown health value must redisplay the form, not abort the page: {html}"
    );
    assert!(
        html.contains("role=\"alert\"") && html.contains("bogus"),
        "the bad value must be named in a visible, screen-reader-announced error: {html}"
    );
    assert!(
        html.contains("healthy_row") && html.contains("paused_row"),
        "with the health filter not applied, both rows must show: {html}"
    );
}

/// AC4: the pause action round-trips through the audited endpoint and the row
/// comes back paused with an audit record naming the UI as the source.
#[tokio::test]
async fn ui_schedules_pause_action_round_trip_writes_audit() {
    let (database_url, _container) = setup_test_database_url().await;
    let id = insert_schedule_fixture(
        &database_url,
        &ScheduleFixture {
            kind: "Workflow",
            name: "pause_round_trip",
            ..Default::default()
        },
    )
    .await;

    let app = build_single_shard_ui_app(&database_url);

    // The list offers a confirmed pause action for an active schedule.
    let (_, html) = fetch_html(&app, "/schedules").await;
    assert!(
        html.contains(&format!("schedules/{id}/pause")),
        "pause action missing: {html}"
    );
    assert!(
        html.contains("return confirm("),
        "pause must be confirmed before it fires: {html}"
    );

    let (status, headers, body) = post_form(&app, &format!("/schedules/{id}/pause"), "").await;
    assert_eq!(
        status,
        StatusCode::SEE_OTHER,
        "pause must redirect back to the list: {body}"
    );
    let location = headers
        .get("location")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    assert!(
        location.contains("schedules"),
        "redirect target: {location}"
    );

    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect for pause assertion");
    let paused: bool = diesel::sql_query(format!(
        "SELECT is_paused AS value FROM harvest_schedules WHERE id = '{id}'"
    ))
    .get_result::<BoolValue>(&mut conn)
    .await
    .expect("schedule row must still exist")
    .value;
    assert!(paused, "the schedule must be paused after the round trip");

    let audits: i64 = diesel::sql_query(format!(
        "SELECT COUNT(*) AS value FROM harvest_audit_log \
         WHERE target_id = '{id}' AND operation = 'schedule.pause' AND source = 'ui'"
    ))
    .get_result::<CountValue>(&mut conn)
    .await
    .expect("audit count query")
    .value;
    assert_eq!(
        audits, 1,
        "the UI pause must write exactly one audit record"
    );

    // And the list now offers resume instead.
    let (_, html) = fetch_html(&app, "/schedules").await;
    assert!(
        html.contains(&format!("schedules/{id}/resume")),
        "resume action missing after pause: {html}"
    );
}

#[derive(diesel::QueryableByName)]
struct BoolValue {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    value: bool,
}

/// `auto_paused_at`-cleared + failure-counter probe for the #360 resume tests.
#[derive(diesel::QueryableByName)]
struct AutoPauseState {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    cleared: bool,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    failures: i32,
}

#[derive(diesel::QueryableByName)]
struct CountValue {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    value: i64,
}

/// AC5: the preview drill-down renders the next fire times, with the jitter
/// window and the schedule's local rendering.
#[tokio::test]
async fn ui_schedules_preview_modal_renders_fire_times() {
    let (database_url, _container) = setup_test_database_url().await;
    let id = insert_schedule_fixture(
        &database_url,
        &ScheduleFixture {
            kind: "Workflow",
            name: "preview_wf",
            jitter_secs: 120,
            schedule_expr: Some("cron:0 * * * *"),
            ..Default::default()
        },
    )
    .await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, &format!("/schedules/{id}/preview?count=3")).await;
    assert_eq!(status, StatusCode::OK, "preview must render: {html}");
    assert!(
        html.contains("Fire-time preview"),
        "heading missing: {html}"
    );
    assert!(html.contains("preview_wf"), "target missing: {html}");
    assert!(
        html.contains("cron+jitter"),
        "jitter reason missing from a jittered schedule: {html}"
    );
    assert!(
        html.contains("Jitter window"),
        "jitter bounds column missing: {html}"
    );
    assert!(!html.contains("<script"), "no script tags allowed: {html}");
}

/// AC5 (#543): a bounded schedule with a spent budget previews zero entries and
/// says why — never a blank panel (AC8).
#[tokio::test]
async fn ui_schedules_preview_explains_an_exhausted_schedule() {
    let (database_url, _container) = setup_test_database_url().await;
    let id = insert_schedule_fixture(
        &database_url,
        &ScheduleFixture {
            kind: "Workflow",
            name: "spent_wf",
            max_runs: Some(2),
            runs_started: 2,
            exhausted_reason: Some("max_runs_exhausted"),
            ..Default::default()
        },
    )
    .await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, &format!("/schedules/{id}/preview")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("exhausted") || html.contains("Exhausted"),
        "must say the schedule is exhausted: {html}"
    );
    assert!(
        html.contains("max_runs_exhausted"),
        "must name the exhaustion reason: {html}"
    );
    assert!(
        html.contains("No upcoming fire times"),
        "must render an explicit empty state: {html}"
    );
}

/// An unknown schedule id is a 404 on every drill-down, not a blank page.
#[tokio::test]
async fn ui_schedules_drilldowns_404_on_unknown_id() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);
    let missing = uuid::Uuid::new_v4();
    for leaf in ["preview", "runs", "backfill"] {
        let (status, html) = fetch_html(&app, &format!("/schedules/{missing}/{leaf}")).await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "{leaf} drill-down for an unknown id must 404: {html}"
        );
    }
}

/// AC7: run history renders newest-first rows with nominal fire time, origin,
/// terminal badges, the scheduled-only summary, and execution links.
#[tokio::test]
async fn ui_schedules_run_history_renders_rows_and_summary() {
    let (database_url, _container) = setup_test_database_url().await;
    let id = insert_schedule_fixture(
        &database_url,
        &ScheduleFixture {
            kind: "Workflow",
            name: "runs_wf",
            next_run_at_sql: Some("NOW() + INTERVAL '1 hour'"),
            ..Default::default()
        },
    )
    .await;

    let scheduled_ok = insert_schedule_run(
        &database_url,
        ShardId::new(0),
        id,
        "runs_wf",
        "COMPLETED",
        "scheduled",
        "NOW() - INTERVAL '1 hour'",
    )
    .await;
    let scheduled_failed = insert_schedule_run(
        &database_url,
        ShardId::new(0),
        id,
        "runs_wf",
        "FAILED",
        "scheduled",
        "NOW() - INTERVAL '2 hours'",
    )
    .await;
    let backfilled = insert_schedule_run(
        &database_url,
        ShardId::new(0),
        id,
        "runs_wf",
        "COMPLETED",
        "backfill",
        "NOW() - INTERVAL '3 hours'",
    )
    .await;
    let manual = insert_schedule_run(
        &database_url,
        ShardId::new(0),
        id,
        "runs_wf",
        "RUNNING",
        "manual_trigger",
        "NULL",
    )
    .await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, &format!("/schedules/{id}/runs")).await;
    assert_eq!(status, StatusCode::OK, "run history must render: {html}");

    for exec in [scheduled_ok, scheduled_failed, backfilled, manual] {
        assert!(
            html.contains(&format!("workflows/{}", exec.as_uuid())),
            "run {} must link to its execution detail view: {html}",
            exec.as_uuid()
        );
    }
    assert!(
        html.contains("scheduled"),
        "scheduled origin missing: {html}"
    );
    assert!(html.contains("backfill"), "backfill origin missing: {html}");
    assert!(
        html.contains("manual_trigger"),
        "manual_trigger origin missing: {html}"
    );
    assert!(
        html.contains("Scheduled-run summary"),
        "cadence summary missing: {html}"
    );
    assert!(
        html.contains("Nominal fire time"),
        "nominal fire time column missing: {html}"
    );
    // The cadence summary counts scheduled-origin runs only: 1 succeeded, 1
    // failed — the backfilled COMPLETED run must not inflate it.
    let summary_start = html
        .find("Scheduled-run summary")
        .expect("summary section present");
    let summary = &html[summary_start..];
    let succeeded_at = summary.find("Succeeded").expect("succeeded row present");
    assert!(
        summary[succeeded_at..succeeded_at + 80].contains(">1<"),
        "only the scheduled COMPLETED run may be counted: {summary}"
    );
    assert!(!html.contains("<script"), "no script tags allowed: {html}");
}

/// AC7/AC8: a schedule with no runs yet renders an explicit message.
#[tokio::test]
async fn ui_schedules_run_history_empty_state() {
    let (database_url, _container) = setup_test_database_url().await;
    let id = insert_schedule_fixture(
        &database_url,
        &ScheduleFixture {
            kind: "Workflow",
            name: "never_run_wf",
            ..Default::default()
        },
    )
    .await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, &format!("/schedules/{id}/runs")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("No runs yet"),
        "no-runs empty state missing: {html}"
    );
}

/// AC7: a cross-shard `partial` response renders a visible "some shards
/// unreachable" banner rather than silently truncated history.
#[tokio::test]
async fn ui_schedules_run_history_partial_shard_banner() {
    let (good_url, _container) = setup_test_database_url().await;
    let id = insert_schedule_fixture(
        &good_url,
        &ScheduleFixture {
            kind: "Workflow",
            name: "partial_runs_wf",
            ..Default::default()
        },
    )
    .await;
    insert_schedule_run(
        &good_url,
        ShardId::new(0),
        id,
        "partial_runs_wf",
        "COMPLETED",
        "scheduled",
        "NOW() - INTERVAL '1 hour'",
    )
    .await;

    let bad_pool = build_test_pool("postgres://invalid:5432/nonexistent");
    let good_pool = build_test_pool(&good_url);
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), good_pool);
    pools.insert(ShardId::new(1), bad_pool);
    let harvest_pool = HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0)));

    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(harvest_pool);
    api_state.install(HarvestApiRuntime::new(
        echo_registry(),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        None,
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::new(
            vec![ShardId::new(0), ShardId::new(1)],
            vec![ShardId::new(0), ShardId::new(1)],
            ShardId::new(0),
        ),
    ));
    let app = harvest_ui_router(api_state).with_state(test_app_state_without_database());

    let (status, html) = fetch_html(&app, &format!("/schedules/{id}/runs")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "an unreachable shard must degrade, never 5xx: {html}"
    );
    assert!(
        html.contains("Some shards unreachable"),
        "partial-shard banner missing: {html}"
    );
    assert!(
        html.contains("may be understated"),
        "summary must be flagged as possibly understated: {html}"
    );
    assert!(
        html.contains("partial_runs_wf"),
        "the reachable shard's data must still render: {html}"
    );
}

/// AC6: the backfill launcher renders a window form, previews with a dry run
/// before dispatching, and lands the operator on the run history afterwards.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn ui_schedules_backfill_form_previews_then_dispatches() {
    let (database_url, _container) = setup_test_database_url().await;
    let id = insert_schedule_fixture(
        &database_url,
        &ScheduleFixture {
            kind: "Workflow",
            name: "echo",
            schedule_expr: Some("cron:0 * * * *"),
            ..Default::default()
        },
    )
    .await;

    let app = build_single_shard_ui_app(&database_url);

    // The form itself is a pure GET.
    let (status, html) = fetch_html(&app, &format!("/schedules/{id}/backfill")).await;
    assert_eq!(status, StatusCode::OK, "backfill form must render: {html}");
    assert!(
        html.contains("name=\"from\""),
        "start field missing: {html}"
    );
    assert!(html.contains("name=\"to\""), "end field missing: {html}");
    assert!(
        html.contains("name=\"stage\" value=\"preview\""),
        "the form must submit the dry-run stage: {html}"
    );

    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect for backfill assertions");
    let runs_before: i64 = diesel::sql_query(
        "SELECT COUNT(*) AS value FROM harvest_workflow_executions WHERE origin = 'backfill'",
    )
    .get_result::<CountValue>(&mut conn)
    .await
    .expect("count query")
    .value;
    assert_eq!(runs_before, 0, "the GET form must dispatch nothing");

    // Stage 1: dry run → confirmation with the planned count.
    let window = "from=2026-08-01T00%3A00%3A00Z&to=2026-08-01T06%3A00%3A00Z&stage=preview";
    let (status, _headers, html) =
        post_form(&app, &format!("/schedules/{id}/backfill"), window).await;
    assert_eq!(status, StatusCode::OK, "dry run must render: {html}");
    assert!(
        html.contains("Confirm backfill"),
        "confirm heading missing: {html}"
    );
    assert!(
        html.contains("Planned slots"),
        "planned count missing: {html}"
    );
    assert!(
        html.contains("name=\"stage\" value=\"commit\""),
        "confirmation must carry an explicit commit stage: {html}"
    );

    let dispatched_after_dry_run: i64 = diesel::sql_query(
        "SELECT COUNT(*) AS value FROM harvest_workflow_executions WHERE origin = 'backfill'",
    )
    .get_result::<CountValue>(&mut conn)
    .await
    .expect("count query")
    .value;
    assert_eq!(
        dispatched_after_dry_run, 0,
        "the dry run must not dispatch anything"
    );

    // Stage 2: commit → dispatch, then redirect to this schedule's run history.
    let commit = "from=2026-08-01T00%3A00%3A00Z&to=2026-08-01T06%3A00%3A00Z&stage=commit";
    let (status, headers, body) =
        post_form(&app, &format!("/schedules/{id}/backfill"), commit).await;
    assert_eq!(
        status,
        StatusCode::SEE_OTHER,
        "a committed backfill must redirect: {body}"
    );
    let location = headers
        .get("location")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    assert!(
        location.contains(&format!("schedules/{id}/runs")),
        "the commit must land on this schedule's run history: {location}"
    );

    let dispatched: i64 = diesel::sql_query(
        "SELECT COUNT(*) AS value FROM harvest_workflow_executions WHERE origin = 'backfill'",
    )
    .get_result::<CountValue>(&mut conn)
    .await
    .expect("count query")
    .value;
    assert!(
        dispatched > 0,
        "the committed backfill must dispatch runs, got {dispatched}"
    );

    let audits: i64 = diesel::sql_query(format!(
        "SELECT COUNT(*) AS value FROM harvest_audit_log \
         WHERE target_id = '{id}' AND source = 'ui' \
           AND route_or_command = 'POST /ui/schedules/{{id}}/backfill'"
    ))
    .get_result::<CountValue>(&mut conn)
    .await
    .expect("audit count query")
    .value;
    assert!(
        audits >= 2,
        "both the dry run and the commit must be audited as UI-sourced, got {audits}"
    );
}

/// AC6: a malformed window is a rendered form error, never a 500.
#[tokio::test]
async fn ui_schedules_backfill_rejects_a_malformed_window() {
    let (database_url, _container) = setup_test_database_url().await;
    let id = insert_schedule_fixture(
        &database_url,
        &ScheduleFixture {
            kind: "Workflow",
            name: "echo",
            ..Default::default()
        },
    )
    .await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, _headers, html) = post_form(
        &app,
        &format!("/schedules/{id}/backfill"),
        "from=yesterday&to=today&stage=preview",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a malformed window must render an error, not 5xx: {html}"
    );
    assert!(
        html.contains("Backfill not started"),
        "the form must explain the rejection: {html}"
    );
    assert!(
        html.contains("RFC 3339"),
        "the error must say what a valid instant looks like: {html}"
    );

    // An inverted window is rejected the same way.
    let (status, _headers, html) = post_form(
        &app,
        &format!("/schedules/{id}/backfill"),
        "from=2026-08-02T00%3A00%3A00Z&to=2026-08-01T00%3A00%3A00Z&stage=preview",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "inverted window must not 5xx: {html}"
    );
    assert!(
        html.contains("at or after"),
        "the inverted-window error must explain itself: {html}"
    );
}

/// AC6 defence-in-depth: a POST that omits `stage` runs the dry run. A bare
/// form post can never dispatch work.
#[tokio::test]
async fn ui_schedules_backfill_without_a_stage_defaults_to_the_dry_run() {
    let (database_url, _container) = setup_test_database_url().await;
    let id = insert_schedule_fixture(
        &database_url,
        &ScheduleFixture {
            kind: "Workflow",
            name: "echo",
            schedule_expr: Some("cron:0 * * * *"),
            ..Default::default()
        },
    )
    .await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, _headers, html) = post_form(
        &app,
        &format!("/schedules/{id}/backfill"),
        "from=2026-08-01T00%3A00%3A00Z&to=2026-08-01T06%3A00%3A00Z",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "must render the confirmation: {html}"
    );
    assert!(
        html.contains("Confirm backfill"),
        "must be the dry run: {html}"
    );

    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect for dispatch assertion");
    let dispatched: i64 = diesel::sql_query(
        "SELECT COUNT(*) AS value FROM harvest_workflow_executions WHERE origin = 'backfill'",
    )
    .get_result::<CountValue>(&mut conn)
    .await
    .expect("count query")
    .value;
    assert_eq!(dispatched, 0, "a stage-less POST must dispatch nothing");
}

/// The schedules list links every row to its three drill-downs (AC5/AC6/AC7).
#[tokio::test]
async fn ui_schedules_rows_link_to_drilldowns() {
    let (database_url, _container) = setup_test_database_url().await;
    let id = insert_schedule_fixture(
        &database_url,
        &ScheduleFixture {
            kind: "Workflow",
            name: "linked_wf",
            ..Default::default()
        },
    )
    .await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/schedules").await;
    assert_eq!(status, StatusCode::OK);
    for leaf in ["preview", "runs", "backfill"] {
        assert!(
            html.contains(&format!("schedules/{id}/{leaf}")),
            "row must link to the {leaf} drill-down: {html}"
        );
    }
}

/// AC4: the drill-downs mirror their endpoints' admin-auth posture — the run
/// history is gated (as `GET /admin/schedules/{id}/runs` is), preview and
/// backfill are not (as their API routes are not).
#[tokio::test]
async fn ui_schedules_drilldown_auth_matches_the_api_routes() {
    let (database_url, _container) = setup_test_database_url().await;
    let id = insert_schedule_fixture(
        &database_url,
        &ScheduleFixture {
            kind: "Workflow",
            name: "gated_wf",
            ..Default::default()
        },
    )
    .await;

    // `set_admin_auth_boundary(false)` with no session = not an admin.
    let pool = build_test_pool(&database_url);
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(false);
    api_state.install_storage_pool(HarvestDbPool::from(pool));
    api_state.install(HarvestApiRuntime::new(
        echo_registry(),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        None,
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));
    let app = harvest_ui_router(api_state).with_state(test_app_state_without_database());

    let (status, html) = fetch_html(&app, &format!("/schedules/{id}/runs")).await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "run history must be admin-gated, matching its API route: {html}"
    );

    for leaf in ["preview", "backfill"] {
        let (status, html) = fetch_html(&app, &format!("/schedules/{id}/{leaf}")).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{leaf} must stay ungated, matching its API route: {html}"
        );
    }
}

/// AC6: the flash a committed backfill redirects with is actually rendered on
/// the run-history page — the dispatch counts (including failures) are only
/// reported there.
#[tokio::test]
async fn ui_schedules_backfill_flash_renders_on_the_run_history() {
    let (database_url, _container) = setup_test_database_url().await;
    let id = insert_schedule_fixture(
        &database_url,
        &ScheduleFixture {
            kind: "Workflow",
            name: "echo",
            schedule_expr: Some("cron:0 * * * *"),
            ..Default::default()
        },
    )
    .await;

    let app = build_single_shard_ui_app(&database_url);
    let commit = "from=2026-08-01T00%3A00%3A00Z&to=2026-08-01T03%3A00%3A00Z&stage=commit";
    let (status, headers, body) =
        post_form(&app, &format!("/schedules/{id}/backfill"), commit).await;
    assert_eq!(
        status,
        StatusCode::SEE_OTHER,
        "commit must redirect: {body}"
    );
    let location = headers
        .get("location")
        .and_then(|v| v.to_str().ok())
        .expect("redirect carries a location")
        .to_string();
    assert!(
        location.contains("flash="),
        "redirect must carry a flash: {location}"
    );

    // Follow the redirect the way a browser would, resolving it against the
    // request path /schedules/{id}/backfill.
    let Some(rest) = location.strip_prefix("../../") else {
        panic!("unexpected redirect shape: {location}");
    };
    let followed = format!("/{rest}");
    let (status, html) = fetch_html(&app, &followed).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the redirect target must resolve: {html}"
    );
    assert!(
        html.contains("Backfill dispatched"),
        "the dispatch summary must be rendered on the run history: {html}"
    );
}

/// AC6: `max_count` is validated and capped rather than passed through, because
/// the endpoint treats it as the planning limit that replaces its own guard.
#[tokio::test]
async fn ui_schedules_backfill_validates_and_caps_max_count() {
    let (database_url, _container) = setup_test_database_url().await;
    let id = insert_schedule_fixture(
        &database_url,
        &ScheduleFixture {
            kind: "Workflow",
            name: "echo",
            schedule_expr: Some("cron:0 * * * *"),
            ..Default::default()
        },
    )
    .await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, _headers, html) = post_form(
        &app,
        &format!("/schedules/{id}/backfill"),
        "from=2026-08-01T00%3A00%3A00Z&to=2026-08-01T03%3A00%3A00Z&max_count=lots&stage=preview",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "must not 5xx: {html}");
    assert!(
        html.contains("invalid max count"),
        "a non-numeric max count must be a rendered error: {html}"
    );
    assert!(
        html.contains("value=\"lots\""),
        "the rejected form must echo what was typed: {html}"
    );

    // An enormous window with an enormous max_count is capped, not enumerated.
    let (status, _headers, html) = post_form(
        &app,
        &format!("/schedules/{id}/backfill"),
        "from=1990-01-01T00%3A00%3A00Z&to=2030-01-01T00%3A00%3A00Z&max_count=100000000&stage=preview",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "must not 5xx: {html}");
    assert!(
        html.contains("Backfill not started") || html.contains("Confirm backfill"),
        "an over-wide window must be answered, not hung: {html}"
    );
}

/// AC3/AC4: a bulk action acts on exactly the health-filtered set the button
/// counted — not on every schedule on the shard.
#[tokio::test]
async fn ui_schedules_bulk_pause_respects_the_health_filter() {
    let (database_url, _container) = setup_test_database_url().await;
    let healthy = insert_schedule_fixture(
        &database_url,
        &ScheduleFixture {
            kind: "Workflow",
            name: "healthy_bulk",
            ..Default::default()
        },
    )
    .await;
    let unhealthy = insert_schedule_fixture(
        &database_url,
        &ScheduleFixture {
            kind: "Workflow",
            name: "dropping_bulk",
            last_catchup_dropped: 4,
            ..Default::default()
        },
    )
    .await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, _headers, body) =
        post_form(&app, "/schedules/bulk-pause", "health=Unhealthy").await;
    assert_eq!(
        status,
        StatusCode::SEE_OTHER,
        "bulk pause must redirect: {body}"
    );

    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect for bulk assertion");
    let paused_healthy: bool = diesel::sql_query(format!(
        "SELECT is_paused AS value FROM harvest_schedules WHERE id = '{healthy}'"
    ))
    .get_result::<BoolValue>(&mut conn)
    .await
    .expect("healthy row exists")
    .value;
    let paused_unhealthy: bool = diesel::sql_query(format!(
        "SELECT is_paused AS value FROM harvest_schedules WHERE id = '{unhealthy}'"
    ))
    .get_result::<BoolValue>(&mut conn)
    .await
    .expect("unhealthy row exists")
    .value;
    assert!(
        paused_unhealthy,
        "the health-filtered bulk pause must pause the matching schedule"
    );
    assert!(
        !paused_healthy,
        "a bulk pause under health=Unhealthy must not touch a healthy schedule"
    );
}

/// The audit `source` fix covers every schedule mutation, not just pause.
#[tokio::test]
async fn ui_schedules_every_mutation_is_audited_as_ui_sourced() {
    let (database_url, _container) = setup_test_database_url().await;
    let pause_id = insert_schedule_fixture(
        &database_url,
        &ScheduleFixture {
            kind: "Workflow",
            name: "audit_pause",
            ..Default::default()
        },
    )
    .await;
    let delete_id = insert_schedule_fixture(
        &database_url,
        &ScheduleFixture {
            kind: "Workflow",
            name: "audit_delete",
            ..Default::default()
        },
    )
    .await;
    insert_schedule_fixture(
        &database_url,
        &ScheduleFixture {
            kind: "Workflow",
            name: "audit_bulk",
            ..Default::default()
        },
    )
    .await;

    let app = build_single_shard_ui_app(&database_url);
    post_form(&app, &format!("/schedules/{pause_id}/pause"), "").await;
    post_form(&app, &format!("/schedules/{pause_id}/resume"), "").await;
    post_form(&app, &format!("/schedules/{delete_id}/delete"), "").await;
    post_form(&app, "/schedules/bulk-pause", "target=audit_bulk").await;
    post_form(&app, "/schedules/bulk-resume", "target=audit_bulk").await;

    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect for audit assertion");
    let api_sourced: i64 = diesel::sql_query(
        "SELECT COUNT(*) AS value FROM harvest_audit_log \
         WHERE route_or_command LIKE 'POST /ui/schedules%' AND source <> 'ui'",
    )
    .get_result::<CountValue>(&mut conn)
    .await
    .expect("audit query")
    .value;
    assert_eq!(
        api_sourced, 0,
        "every /ui/schedules audit record must be source = ui"
    );

    for op in ["schedule.pause", "schedule.resume", "schedule.delete"] {
        let n: i64 = diesel::sql_query(format!(
            "SELECT COUNT(*) AS value FROM harvest_audit_log \
             WHERE operation = '{op}' AND source = 'ui'"
        ))
        .get_result::<CountValue>(&mut conn)
        .await
        .expect("audit query")
        .value;
        assert!(n > 0, "{op} must be audited as UI-sourced, got {n}");
    }
}

/// A per-row mutation's redirect resolves back to the list, not to a path
/// nested under the schedule id.
#[tokio::test]
async fn ui_schedules_pause_redirect_resolves_to_the_list() {
    let (database_url, _container) = setup_test_database_url().await;
    insert_schedule_fixture(
        &database_url,
        &ScheduleFixture {
            kind: "Workflow",
            name: "redirect_wf",
            ..Default::default()
        },
    )
    .await;
    let id = insert_schedule_fixture(
        &database_url,
        &ScheduleFixture {
            kind: "Workflow",
            name: "redirect_target",
            ..Default::default()
        },
    )
    .await;

    let app = build_single_shard_ui_app(&database_url);
    let (_status, headers, _body) = post_form(&app, &format!("/schedules/{id}/pause"), "").await;
    let location = headers
        .get("location")
        .and_then(|v| v.to_str().ok())
        .expect("redirect carries a location")
        .to_string();

    // Resolve it the way a browser would against /schedules/{id}/pause, whose
    // base directory is /schedules/{id}/.
    let Some(rest) = location.strip_prefix("../") else {
        panic!("per-row redirect must climb a segment: {location}");
    };
    let resolved = format!("/{rest}");
    let (status, html) = fetch_html(&app, &resolved).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the pause redirect must land on the schedules list: {html}"
    );
    assert!(
        html.contains("redirect_wf"),
        "landed on the wrong page: {html}"
    );
}

/// Codex round 1 #3: an auto-paused schedule (#360 sets `auto_paused_at`
/// **without** `is_paused`) must be resumable from Vantage — per row and in
/// bulk — and the resume must clear the auto-pause state the way the API's does,
/// or the next tick immediately re-pauses it.
#[tokio::test]
async fn ui_schedules_auto_paused_row_can_be_resumed() {
    let (database_url, _container) = setup_test_database_url().await;
    let id = insert_schedule_fixture(
        &database_url,
        &ScheduleFixture {
            kind: "Workflow",
            name: "auto_paused_wf",
            auto_paused: true,
            ..Default::default()
        },
    )
    .await;

    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect for auto-pause setup");
    conn.batch_execute(&format!(
        "UPDATE harvest_schedules SET consecutive_failure_count = 5 WHERE id = '{id}'"
    ))
    .await
    .expect("seed a failure count");

    let app = build_single_shard_ui_app(&database_url);

    // The row offers Resume, not Pause, even though `is_paused` is false.
    let (status, html) = fetch_html(&app, "/schedules").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("Auto-paused"),
        "auto-paused badge missing: {html}"
    );
    assert!(
        html.contains(&format!("schedules/{id}/resume")),
        "an auto-paused row must offer Resume: {html}"
    );
    assert!(
        !html.contains(&format!("schedules/{id}/pause")),
        "an auto-paused row must not offer Pause: {html}"
    );

    let (status, _headers, body) = post_form(&app, &format!("/schedules/{id}/resume"), "").await;
    assert_eq!(
        status,
        StatusCode::SEE_OTHER,
        "resume must redirect: {body}"
    );

    let state: AutoPauseState = diesel::sql_query(format!(
        "SELECT (auto_paused_at IS NULL) AS cleared, consecutive_failure_count AS failures \
         FROM harvest_schedules WHERE id = '{id}'"
    ))
    .get_result(&mut conn)
    .await
    .expect("row still exists");
    assert!(
        state.cleared,
        "resume must clear auto_paused_at, or the schedule stays held back"
    );
    assert_eq!(
        state.failures, 0,
        "resume must reset the failure counter, or the next tick re-pauses it"
    );
}

/// The same for the bulk resume path, which selected solely on `is_paused`.
#[tokio::test]
async fn ui_schedules_bulk_resume_reaches_auto_paused_rows() {
    let (database_url, _container) = setup_test_database_url().await;
    let id = insert_schedule_fixture(
        &database_url,
        &ScheduleFixture {
            kind: "Workflow",
            name: "auto_paused_bulk",
            auto_paused: true,
            ..Default::default()
        },
    )
    .await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, _headers, body) =
        post_form(&app, "/schedules/bulk-resume", "target=auto_paused_bulk").await;
    assert_eq!(
        status,
        StatusCode::SEE_OTHER,
        "bulk resume must redirect: {body}"
    );

    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect for bulk resume assertion");
    let cleared: bool = diesel::sql_query(format!(
        "SELECT (auto_paused_at IS NULL) AS value FROM harvest_schedules WHERE id = '{id}'"
    ))
    .get_result::<BoolValue>(&mut conn)
    .await
    .expect("row exists")
    .value;
    assert!(
        cleared,
        "a bulk resume must reach an auto-paused schedule, not just is_paused rows"
    );
}

/// Codex round 1 #2: the preview drill-down must render for a schedule on a
/// healthy shard even when an earlier shard is unreachable — the resilient
/// resolver found the row, so a second fragile lookup must not undo that.
#[tokio::test]
async fn ui_schedules_preview_survives_an_unreachable_earlier_shard() {
    let (good_url, _container) = setup_test_database_url().await;
    let id = insert_schedule_fixture(
        &good_url,
        &ScheduleFixture {
            kind: "Workflow",
            name: "later_shard_wf",
            schedule_expr: Some("cron:0 * * * *"),
            ..Default::default()
        },
    )
    .await;

    // Shard 0 is unreachable; the schedule lives on shard 1.
    let bad_pool = build_test_pool("postgres://invalid:5432/nonexistent");
    let good_pool = build_test_pool(&good_url);
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), bad_pool);
    pools.insert(ShardId::new(1), good_pool);
    let harvest_pool = HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(1)));

    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(harvest_pool);
    api_state.install(HarvestApiRuntime::new(
        echo_registry(),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        None,
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::new(
            vec![ShardId::new(0), ShardId::new(1)],
            vec![ShardId::new(0), ShardId::new(1)],
            ShardId::new(1),
        ),
    ));
    let app = harvest_ui_router(api_state).with_state(test_app_state_without_database());

    let (status, html) = fetch_html(&app, &format!("/schedules/{id}/preview")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a preview for a schedule on a healthy shard must render \
         despite an unreachable earlier shard: {html}"
    );
    assert!(
        html.contains("later_shard_wf"),
        "the resolved schedule must be rendered: {html}"
    );
}

/// Codex round 2 #2: the backfill launcher must work for a schedule on a
/// healthy shard while an earlier shard is unreachable. Round 1 fixed the
/// preview instance; the shared lookup underneath is now resilient, so the
/// backfill path is covered by the same rule.
#[tokio::test]
async fn ui_schedules_backfill_survives_an_unreachable_earlier_shard() {
    let (good_url, _container) = setup_test_database_url().await;
    let id = insert_schedule_fixture(
        &good_url,
        &ScheduleFixture {
            kind: "Workflow",
            name: "echo",
            schedule_expr: Some("cron:0 * * * *"),
            ..Default::default()
        },
    )
    .await;

    // Shard 0 is unreachable; the schedule lives on shard 1.
    let bad_pool = build_test_pool("postgres://invalid:5432/nonexistent");
    let good_pool = build_test_pool(&good_url);
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), bad_pool);
    pools.insert(ShardId::new(1), good_pool);
    let harvest_pool = HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(1)));

    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(harvest_pool);
    api_state.install(HarvestApiRuntime::new(
        echo_registry(),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        None,
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::new(
            vec![ShardId::new(0), ShardId::new(1)],
            vec![ShardId::new(0), ShardId::new(1)],
            ShardId::new(1),
        ),
    ));
    let app = harvest_ui_router(api_state).with_state(test_app_state_without_database());

    let (status, html) = fetch_html(&app, &format!("/schedules/{id}/backfill")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the backfill form must render despite an unreachable earlier shard: {html}"
    );

    let (status, _headers, html) = post_form(
        &app,
        &format!("/schedules/{id}/backfill"),
        "from=2026-08-01T00%3A00%3A00Z&to=2026-08-01T03%3A00%3A00Z&stage=preview",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the dry run must reach the schedule on the healthy shard: {html}"
    );
    assert!(
        html.contains("Confirm backfill"),
        "the dry run must produce a confirmation, not a resolution failure: {html}"
    );
}

/// Codex round 2 #3: a schedule that is terminal on its live bounds but whose
/// tick never stamped `exhausted_at` must still read as unhealthy on the list.
#[tokio::test]
async fn ui_schedules_list_flags_a_row_bounded_out_before_its_tick_stamped_it() {
    let (database_url, _container) = setup_test_database_url().await;
    // `runs_started == max_runs`, `exhausted_at` still NULL.
    let bounded = insert_schedule_fixture(
        &database_url,
        &ScheduleFixture {
            kind: "Workflow",
            name: "unstamped_bounded",
            max_runs: Some(4),
            runs_started: 4,
            next_run_at_sql: Some("NOW() + INTERVAL '10 hours'"),
            ..Default::default()
        },
    )
    .await;
    let healthy = insert_schedule_fixture(
        &database_url,
        &ScheduleFixture {
            kind: "Workflow",
            name: "still_healthy",
            next_run_at_sql: Some("NOW() + INTERVAL '1 minute'"),
            ..Default::default()
        },
    )
    .await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/schedules").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("run budget spent"),
        "an unstamped exhaustion must be badged with its bound: {html}"
    );

    // It sorts above the healthy row despite firing much later.
    let pos = |id: uuid::Uuid| {
        html.find(&id.to_string())
            .unwrap_or_else(|| panic!("schedule {id} missing: {html}"))
    };
    assert!(
        pos(bounded) < pos(healthy),
        "a live-bounded-out row must sort to the top: {html}"
    );

    // And the health filter finds it.
    let (status, html) = fetch_html(&app, "/schedules?health=Unhealthy").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("unstamped_bounded"),
        "health=Unhealthy must include it: {html}"
    );
    assert!(
        !html.contains("still_healthy"),
        "the healthy row must be filtered out: {html}"
    );
}

/// Codex round 2 #1: a legacy `max_runs = 0` row is unlimited, and must not be
/// rendered as a spent budget or flagged unhealthy.
#[tokio::test]
async fn ui_schedules_zero_run_cap_renders_as_unlimited() {
    let (database_url, _container) = setup_test_database_url().await;
    insert_schedule_fixture(
        &database_url,
        &ScheduleFixture {
            kind: "Workflow",
            name: "zero_cap_wf",
            max_runs: Some(0),
            runs_started: 9,
            ..Default::default()
        },
    )
    .await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/schedules").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("zero_cap_wf"), "row missing: {html}");
    assert!(
        !html.contains("of 0 left"),
        "max_runs = 0 is unlimited and must not render a budget: {html}"
    );
    assert!(
        !html.contains("Needs attention"),
        "an unlimited schedule must not be counted unhealthy: {html}"
    );
}

/// Timezone column renders and differentiates UTC (subdued) from other timezones (prominent badge).
#[tokio::test]
async fn ui_schedules_displays_timezone() {
    let (database_url, _container) = setup_test_database_url().await;
    insert_test_schedule_with_tz(&database_url, "Workflow", "utc_wf", false, "UTC").await;
    insert_test_schedule_with_tz(
        &database_url,
        "Workflow",
        "la_wf",
        false,
        "America/Los_Angeles",
    )
    .await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/schedules").await;
    assert_eq!(status, StatusCode::OK);

    // Differentiate UTC vs America/Los_Angeles rendering:
    // UTC should be subdued, and America/Los_Angeles should be prominent.
    // Timezone column header must be present.
    assert!(
        html.contains("<th>Timezone</th>")
            || html.contains("<th>TIMEZONE</th>")
            || html.contains("<th>timezone</th>"),
        "Timezone header missing: {html}"
    );
    assert!(
        html.contains("America/Los_Angeles"),
        "America/Los_Angeles timezone missing: {html}"
    );
    assert!(html.contains("UTC"), "UTC timezone missing: {html}");
}

// ---------------------------------------------------------------------------
// Issue #253 — Workflow Execution Detail page
// ---------------------------------------------------------------------------

async fn insert_workflow_events(
    database_url: &str,
    exec_id: ExecutionId,
    events: &[autumn_harvest::WorkflowEvent],
    start_id: i32,
) {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("connect for event insert");
    autumn_harvest::store::append_events(&mut conn, exec_id, events, start_id)
        .await
        .expect("append_events should succeed");
}

async fn insert_child_workflow_on_url(
    database_url: &str,
    shard: ShardId,
    workflow_name: &str,
    workflow_id: &str,
    parent_id: ExecutionId,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(shard);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("connect for child workflow insert");
    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name,
            workflow_id,
            exec_id,
            input: serde_json::json!({}),
            parent_id: Some(parent_id.as_uuid()),
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::default(),
            conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            chain_execution_timeout: None,
            max_workflow_chain_timeout_ceiling: None,
            inherited_chain_deadline_at: None,
            concurrency_key: None,
            concurrency_limit: None,
            concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
            priority: Priority::default(),
            max_workflow_input_bytes: 0,
            start_at: None,
            delay: None,
            max_workflow_start_delay: None,

            owner: None,
            runbook_url: None,
            severity: None,
            context_headers: None,

            sla: None,
            schedule_id: None,
            scheduled_for: None,
            workflow_attempt: 1,
            workflow_retry_policy: None,
            retry_of_exec_id: None,
            max_workflow_attempts_ceiling: None,
            origin: None,
            completion_callbacks: None,
            start_source: autumn_harvest::StartSource::Api,
            start_source_ref: None,
            started_by: None,
        },
        None,
    )
    .await
    .expect("child workflow insert should succeed");
    exec_id
}

/// Detail page groups activity events into an "Activity attempts" section.
#[tokio::test]
async fn detail_page_shows_activity_attempts_panel() {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id =
        insert_workflow_on_url(&database_url, ShardId::new(0), "send_email_wf", "act-1").await;

    let activity_exec_id = autumn_harvest::ActivityExecId::new();
    let events = vec![
        autumn_harvest::WorkflowEvent::ActivityScheduled {
            activity_id: activity_exec_id,
            name: "send_email".to_string(),
            input: serde_json::json!({}),
            queue: "default".to_string(),
        },
        autumn_harvest::WorkflowEvent::ActivityCompleted {
            activity_id: activity_exec_id,
            output: serde_json::json!("sent"),
        },
    ];
    insert_workflow_events(&database_url, exec_id, &events, 1).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "detail page should render: {html}");
    assert!(
        html.contains("Activity attempts"),
        "detail page should show an 'Activity attempts' panel: {html}"
    );
    assert!(
        html.contains("send_email"),
        "activity name should appear in the attempts panel: {html}"
    );
}

/// Detail page shows a children panel on the parent and a parent link on the child.
#[tokio::test]
async fn detail_page_shows_parent_children_panel() {
    let (database_url, _container) = setup_test_database_url().await;
    let parent_exec_id =
        insert_workflow_on_url(&database_url, ShardId::new(0), "parent_wf", "parent-1").await;
    let child_exec_id = insert_child_workflow_on_url(
        &database_url,
        ShardId::new(0),
        "child_wf",
        "child-1",
        parent_exec_id,
    )
    .await;

    let app = build_single_shard_ui_app(&database_url);

    // Parent page should show children section with child exec id
    let (status, html) = fetch_html(&app, &format!("/workflows/{parent_exec_id}")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "parent detail page should render: {html}"
    );
    assert!(
        html.contains("Children") || html.contains("children"),
        "parent detail page should show a 'Children' section: {html}"
    );
    assert!(
        html.contains(&child_exec_id.to_string()[..8]),
        "child exec id prefix should appear on parent page: {html}"
    );

    // Child page should show parent as a clickable link
    let (status, html) = fetch_html(&app, &format!("/workflows/{child_exec_id}")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "child detail page should render: {html}"
    );
    let parent_str = parent_exec_id.to_string();
    assert!(
        html.contains(&format!("href=\"../../workflows/{parent_str}\""))
            || html.contains(&format!("href=\"../workflows/{parent_str}\""))
            || html.contains(&format!("href=\"{parent_str}\"")),
        "parent exec id should be a clickable link on child page: {html}"
    );
}

/// Detail page shows a Signals & Updates section when those events are present.
#[tokio::test]
async fn detail_page_shows_signals_updates_panel() {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id =
        insert_workflow_on_url(&database_url, ShardId::new(0), "signal_wf", "signal-1").await;

    let events = vec![autumn_harvest::WorkflowEvent::SignalReceived {
        signal_name: "approve_request".to_string(),
        payload: serde_json::json!({"approved": true}),
    }];
    insert_workflow_events(&database_url, exec_id, &events, 1).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "detail page should render: {html}");
    assert!(
        html.contains("Signals") || html.contains("Updates"),
        "detail page should show a 'Signals & Updates' section: {html}"
    );
    assert!(
        html.contains("approve_request"),
        "signal name should appear in the signals panel: {html}"
    );
}

/// Detail page shows operator action forms: cancel and export history.
#[tokio::test]
async fn detail_page_shows_operator_actions() {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id =
        insert_workflow_on_url(&database_url, ShardId::new(0), "action_wf", "action-1").await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "detail page should render: {html}");
    assert!(
        html.contains("Cancel") || html.contains("cancel"),
        "detail page should show a Cancel action: {html}"
    );
    assert!(
        html.contains("history/export") || html.contains("Export history"),
        "detail page should show an Export history link: {html}"
    );
}

/// `ActivityScheduled` events render with a human-readable label in the event timeline.
#[tokio::test]
async fn detail_page_event_labels_human_readable() {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id =
        insert_workflow_on_url(&database_url, ShardId::new(0), "label_wf", "label-1").await;

    let activity_exec_id = autumn_harvest::ActivityExecId::new();
    let events = vec![autumn_harvest::WorkflowEvent::ActivityScheduled {
        activity_id: activity_exec_id,
        name: "charge_card".to_string(),
        input: serde_json::json!({}),
        queue: "payments".to_string(),
    }];
    insert_workflow_events(&database_url, exec_id, &events, 1).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "detail page should render: {html}");
    assert!(
        html.contains("Activity scheduled") || html.contains("activity scheduled"),
        "ActivityScheduled event should render with a human-readable label: {html}"
    );
}

/// Detail page paginates the event timeline when there are many events.
#[tokio::test]
async fn detail_page_events_paginated_for_large_history() {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id = insert_workflow_on_url(&database_url, ShardId::new(0), "big_wf", "big-1").await;

    // Insert 150 marker events so the pagination threshold is crossed.
    let many_events: Vec<autumn_harvest::WorkflowEvent> = (0..150)
        .map(|i| autumn_harvest::WorkflowEvent::MarkerRecorded {
            name: format!("marker_{i}"),
            details: serde_json::json!({}),
        })
        .collect();
    insert_workflow_events(&database_url, exec_id, &many_events, 1).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "detail page should render: {html}");

    // The first page shows events 0–99; marker_149 is on page 1 and must not appear.
    assert!(
        !html.contains("marker_149"),
        "marker_149 is on page 1 (event 150 of 150) and should not render on page 0: {html}"
    );
    assert!(
        !html.contains("marker_100"),
        "marker_100 is on page 1 (event 101 of 150) and should not render on page 0: {html}"
    );

    // Pagination controls must be visible.
    assert!(
        html.contains("Next") || html.contains("Jump to latest"),
        "pagination or jump-to-latest control should appear for large event histories: {html}"
    );
}

/// Status badges on the detail page include aria-label for screen reader accessibility.
#[tokio::test]
async fn detail_page_status_badge_has_aria_label() {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id = insert_workflow_on_url(&database_url, ShardId::new(0), "aria_wf", "aria-1").await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "detail page should render: {html}");
    assert!(
        html.contains("aria-label"),
        "detail page status badge should have aria-label for accessibility: {html}"
    );
}

/// Workflow list filter form includes `started_after` and `started_before` inputs.
#[tokio::test]
async fn list_page_has_time_range_filters() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);

    let (status, html) = fetch_html(&app, "/workflows").await;
    assert_eq!(status, StatusCode::OK, "list page should render: {html}");
    assert!(
        html.contains("name=\"started_after\""),
        "list page filter form should have a started_after input: {html}"
    );
    assert!(
        html.contains("name=\"started_before\""),
        "list page filter form should have a started_before input: {html}"
    );
}

/// Wayfinder error-path fix: a malformed `started_after`/`started_before`
/// used to `?`-abort `list_workflows_ui` with a bare 400 before the filter
/// form was ever rendered — discarding every other filter the operator had
/// typed and giving no way back to the list short of hand-editing the URL.
///
/// RED baseline (pre-fix, reproduced by checking out the parent commit and
/// running this test): `GET /workflows?started_after=yesterday&workflow_name=billing`
/// returned `400 Bad Request` with a body that did not contain the filters
/// form, so `workflow_name=billing` (a valid, already-typed filter) was
/// silently discarded along with the page.
///
/// GREEN (this commit): the request still renders the list page (`200`),
/// preserves the other filter (`workflow_name=billing`, still in its
/// input's `value=`), preserves the operator's exact invalid text
/// (`started_after`'s `value="yesterday"`, not blanked out), and surfaces a
/// `role="alert"` message adjacent to the offending field explaining the
/// expected format — all four error-path booleans (adjacent to cause,
/// persists until resolved, says how to recover, entered data preserved)
/// now hold, where before only "says how to recover" did (in a 400 body the
/// operator would only see by opening dev tools).
#[tokio::test]
async fn invalid_started_after_redisplays_form_instead_of_aborting_page() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);

    let (status, html) = fetch_html(
        &app,
        "/workflows?started_after=yesterday&workflow_name=billing",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "an invalid started_after must not abort the whole list page: {html}"
    );
    assert!(
        html.contains("name=\"started_after\""),
        "filter form must still render: {html}"
    );
    assert!(
        html.contains("value=\"yesterday\""),
        "the operator's exact invalid input must be echoed back for correction: {html}"
    );
    assert!(
        html.contains("value=\"billing\""),
        "the other filter the operator already typed must not be discarded: {html}"
    );
    assert!(
        html.contains("role=\"alert\""),
        "an inline, screen-reader-announced error must sit next to the field: {html}"
    );
    assert!(
        html.to_lowercase().contains("rfc 3339"),
        "the error must say how to recover (expected format): {html}"
    );
}

/// Same fix, `started_before` side — a distinct code path in
/// `list_workflows_ui`, so covered independently rather than assumed
/// symmetric with `started_after`.
#[tokio::test]
async fn invalid_started_before_redisplays_form_instead_of_aborting_page() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);

    let (status, html) = fetch_html(&app, "/workflows?started_before=not-a-date").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "an invalid started_before must not abort the whole list page: {html}"
    );
    assert!(
        html.contains("value=\"not-a-date\""),
        "the operator's exact invalid input must be echoed back for correction: {html}"
    );
    assert!(
        html.contains("role=\"alert\""),
        "an inline, screen-reader-announced error must sit next to the field: {html}"
    );
}

/// Detail page event timestamps are displayed (not "—" for every row).
#[tokio::test]
async fn detail_page_event_timestamps_display() {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id = insert_workflow_on_url(&database_url, ShardId::new(0), "ts_wf", "ts-1").await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "detail page should render: {html}");
    // WorkflowStarted event is inserted by start_or_load; its timestamp should appear.
    assert!(
        html.contains("UTC") || html.contains("2026"),
        "event timestamps should display real dates, not placeholder dashes: {html}"
    );
}

// ---------------------------------------------------------------------------
// Issue #253 — NEW RED tests for remaining acceptance criteria
// ---------------------------------------------------------------------------

/// Event timeline shows collapsible payload for each event.
#[tokio::test]
async fn detail_page_event_timeline_has_collapsible_payload() {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id =
        insert_workflow_on_url(&database_url, ShardId::new(0), "payload_wf", "payload-1").await;

    let activity_exec_id = autumn_harvest::ActivityExecId::new();
    let events = vec![autumn_harvest::WorkflowEvent::ActivityScheduled {
        activity_id: activity_exec_id,
        name: "do_work".to_string(),
        input: serde_json::json!({"amount": 42}),
        queue: "default".to_string(),
    }];
    insert_workflow_events(&database_url, exec_id, &events, 1).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "detail page should render: {html}");
    assert!(
        html.contains("<details"),
        "event timeline should have a <details> element for collapsible payload: {html}"
    );
    assert!(
        html.contains("view payload") || html.contains("payload"),
        "details summary should mention payload: {html}"
    );
    // The raw JSON payload data should appear in the page
    assert!(
        html.contains("amount") || html.contains("42"),
        "event payload data should be present in the expanded section: {html}"
    );
}

/// Detail page blocked-on panel appears for a running workflow with a pending activity.
#[tokio::test]
async fn detail_page_blocked_on_panel_for_running_workflow() {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id =
        insert_workflow_on_url(&database_url, ShardId::new(0), "blocked_wf", "blocked-1").await;

    // Insert a pending task queue row for this workflow
    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    let task_id = uuid::Uuid::new_v4();
    let sql = format!(
        "INSERT INTO harvest_task_queue \
            (id, queue_name, task_type, workflow_exec_id, activity_name, input, state, priority, \
             attempt, max_attempts, scheduled_at) \
         VALUES \
            ('{task_id}', 'default', 'activity', '{exec_uuid}', 'process_payment', \
             '{{}}', 'PENDING', 0, 0, 3, NOW())",
        exec_uuid = exec_id.as_uuid()
    );
    conn.batch_execute(&sql).await.expect("insert pending task");

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "detail page should render: {html}");
    assert!(
        html.to_lowercase().contains("blocked")
            || html.contains("Blocked on")
            || html.contains("pending"),
        "detail page should show blocked-on or pending panel: {html}"
    );
    assert!(
        html.contains("process_payment"),
        "pending activity name should appear in blocked-on panel: {html}"
    );
}

/// Detail page renders the latest heartbeat checkpoint payload for a running
/// heartbeating activity (issue #503).
#[tokio::test]
async fn detail_page_renders_heartbeat_checkpoint() {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id =
        insert_workflow_on_url(&database_url, ShardId::new(0), "blocked_wf", "blocked-hb").await;

    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    let task_id = uuid::Uuid::new_v4();
    let sql = format!(
        "INSERT INTO harvest_task_queue \
            (id, queue_name, task_type, workflow_exec_id, activity_name, input, state, priority, \
             attempt, max_attempts, scheduled_at, started_at, last_heartbeat_at, heartbeat_details) \
         VALUES \
            ('{task_id}', 'default', 'activity', '{exec_uuid}', 'pipeline', \
             '{{}}', 'RUNNING', 0, 0, 3, NOW(), NOW(), NOW(), \
             '{{\"processed\": 4500, \"total\": 10000}}'::jsonb)",
        exec_uuid = exec_id.as_uuid()
    );
    conn.batch_execute(&sql)
        .await
        .expect("insert heartbeating task");

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "detail page should render: {html}");
    assert!(
        html.contains("Checkpoint"),
        "pending-activities table should have a Checkpoint column: {html}"
    );
    assert!(
        html.contains("processed") && html.contains("4500"),
        "the latest heartbeat checkpoint payload should be rendered: {html}"
    );
}

/// Cancel action in the UI redirects back to the detail page with a flash message.
#[tokio::test]
async fn detail_page_cancel_action_redirects_with_flash() {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id =
        insert_workflow_on_url(&database_url, ShardId::new(0), "cancel_wf", "cancel-2").await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, headers, body) = post_form(
        &app,
        &format!("/workflows/{exec_id}/cancel"),
        "reason=test+cancellation",
    )
    .await;
    assert!(
        status == StatusCode::SEE_OTHER || status == StatusCode::FOUND,
        "cancel action must redirect (got {status}): {body}"
    );
    let location = headers
        .get("location")
        .expect("redirect must have Location header")
        .to_str()
        .unwrap();
    assert!(
        location.contains(&exec_id.to_string()) || location.contains("workflows"),
        "redirect must go to the workflow detail page: {location}"
    );
    assert!(
        location.contains("flash"),
        "redirect must carry a flash message: {location}"
    );
    assert!(
        !location.ends_with("flash="),
        "flash param must be non-empty: {location}"
    );
}

/// Read the persisted state of a workflow execution directly from its shard DB.
async fn read_execution_state(database_url: &str, exec_id: ExecutionId) -> String {
    let mut conn = AsyncPgConnection::establish(database_url).await.unwrap();
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(harvest_workflow_executions::state)
        .first(&mut conn)
        .await
        .expect("execution row should exist")
}

/// Terminate action on a RUNNING execution seals it as TERMINATED and redirects
/// back to the detail page with a flash message (issue #788).
#[tokio::test]
async fn detail_page_terminate_action_seals_terminated() {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id = insert_workflow_on_url(
        &database_url,
        ShardId::new(0),
        "terminate_wf",
        "terminate-1",
    )
    .await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, headers, body) = post_form(
        &app,
        &format!("/workflows/{exec_id}/terminate"),
        "reason=wedged+run",
    )
    .await;
    assert!(
        status == StatusCode::SEE_OTHER || status == StatusCode::FOUND,
        "terminate action must redirect (got {status}): {body}"
    );
    let location = headers
        .get("location")
        .expect("redirect must have Location header")
        .to_str()
        .unwrap();
    assert!(
        location.contains(&exec_id.to_string()) || location.contains("workflows"),
        "redirect must go to the workflow detail page: {location}"
    );
    assert!(
        location.contains("flash") && !location.ends_with("flash="),
        "redirect must carry a non-empty flash message: {location}"
    );

    let state = read_execution_state(&database_url, exec_id).await;
    assert_eq!(
        state, "TERMINATED",
        "terminate must seal the execution as TERMINATED"
    );
}

/// The detail page renders an enabled Terminate form (not the old placeholder)
/// for a RUNNING execution (issue #788).
#[tokio::test]
async fn detail_page_terminate_button_enabled_when_running() {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id = insert_workflow_on_url(
        &database_url,
        ShardId::new(0),
        "terminate_wf",
        "terminate-2",
    )
    .await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "detail page should render: {html}");
    assert!(
        html.contains("/terminate"),
        "running execution should expose a Terminate form posting to /terminate: {html}"
    );
    assert!(
        !html.contains("Not yet available"),
        "the disabled Terminate placeholder must be gone: {html}"
    );
}

/// The Terminate button is disabled once the execution is terminal, mirroring
/// the Pause gate (issue #788).
#[tokio::test]
async fn detail_page_terminate_button_disabled_when_terminal() {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id = insert_workflow_on_url(
        &database_url,
        ShardId::new(0),
        "terminate_wf",
        "terminate-3",
    )
    .await;

    // First terminate seals it TERMINATED.
    let app = build_single_shard_ui_app(&database_url);
    let _ = post_form(
        &app,
        &format!("/workflows/{exec_id}/terminate"),
        String::new(),
    )
    .await;
    assert_eq!(
        read_execution_state(&database_url, exec_id).await,
        "TERMINATED"
    );

    // Now the detail page must render Terminate as disabled.
    let (status, html) = fetch_html(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "detail page should render: {html}");
    let term_idx = html
        .find(">Terminate<")
        .or_else(|| html.find("Terminate</button>"))
        .expect("detail page should contain a Terminate button");
    // The opening <button ... disabled ...> tag precedes the label; scan back to it.
    let tag_start = html[..term_idx].rfind("<button").expect("button tag");
    assert!(
        html[tag_start..term_idx].contains("disabled"),
        "Terminate button must be disabled for a terminal execution: {}",
        &html[tag_start..term_idx]
    );
}

/// Terminating one execution does not touch a second concurrent execution
/// (collateral-isolation parity with #504).
#[tokio::test]
async fn detail_page_terminate_only_affects_target() {
    let (database_url, _container) = setup_test_database_url().await;
    let target = insert_workflow_on_url(
        &database_url,
        ShardId::new(0),
        "terminate_wf",
        "terminate-t",
    )
    .await;
    let bystander = insert_workflow_on_url(
        &database_url,
        ShardId::new(0),
        "terminate_wf",
        "terminate-b",
    )
    .await;

    let app = build_single_shard_ui_app(&database_url);
    let _ = post_form(
        &app,
        &format!("/workflows/{target}/terminate"),
        String::new(),
    )
    .await;

    assert_eq!(
        read_execution_state(&database_url, target).await,
        "TERMINATED",
        "target must be terminated"
    );
    assert_eq!(
        read_execution_state(&database_url, bystander).await,
        "RUNNING",
        "bystander execution must be untouched"
    );
}

/// Send signal action redirects back with flash.
#[tokio::test]
async fn detail_page_signal_action_redirects_with_flash() {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id =
        insert_workflow_on_url(&database_url, ShardId::new(0), "signal_wf2", "signal-2").await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, headers, body) = post_form(
        &app,
        &format!("/workflows/{exec_id}/signal"),
        "signal_name=ping&payload=%7B%7D",
    )
    .await;
    assert!(
        status == StatusCode::SEE_OTHER || status == StatusCode::FOUND,
        "signal action must redirect (got {status}): {body}"
    );
    let location = headers
        .get("location")
        .expect("redirect must have Location header")
        .to_str()
        .unwrap();
    assert!(
        location.contains(&exec_id.to_string()) || location.contains("workflows"),
        "redirect must go back to the detail page: {location}"
    );
    assert!(
        location.contains("flash"),
        "redirect must carry a flash message: {location}"
    );
}

/// Flash message is rendered on the detail page when flash param is present.
#[tokio::test]
async fn detail_page_renders_flash_message() {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id =
        insert_workflow_on_url(&database_url, ShardId::new(0), "flash_wf", "flash-1").await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) =
        fetch_html(&app, &format!("/workflows/{exec_id}?flash=Hello%20world")).await;
    assert_eq!(status, StatusCode::OK, "detail page should render: {html}");
    assert!(
        html.contains("Hello world") || html.contains("Hello+world"),
        "flash message should appear on the page: {html}"
    );
}

/// Reset action redirects back with flash.
#[tokio::test]
async fn detail_page_reset_action_redirects_with_flash() {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id =
        insert_workflow_on_url(&database_url, ShardId::new(0), "reset_wf2", "reset-2").await;

    // Insert at least 2 events so there is a valid reset point
    let events = vec![
        autumn_harvest::WorkflowEvent::TimerStarted {
            timer_id: autumn_harvest::types::TimerId::new("t1"),
            duration_secs: 60,
        },
        autumn_harvest::WorkflowEvent::TimerFired {
            timer_id: autumn_harvest::types::TimerId::new("t1"),
        },
    ];
    insert_workflow_events(&database_url, exec_id, &events, 1).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, headers, body) = post_form(
        &app,
        &format!("/workflows/{exec_id}/reset"),
        "reset_to_event_id=0&reason=rollback",
    )
    .await;
    assert!(
        status == StatusCode::SEE_OTHER || status == StatusCode::FOUND,
        "reset action must redirect (got {status}): {body}"
    );
    let location = headers
        .get("location")
        .expect("redirect must have Location header")
        .to_str()
        .unwrap();
    assert!(
        location.contains(&exec_id.to_string()) || location.contains("workflows"),
        "redirect must go back to the detail page: {location}"
    );
    assert!(
        location.contains("flash"),
        "redirect must carry a flash message: {location}"
    );
}

/// GREEN — `reset_to_event_id` used to be typed `i64` directly on
/// `WorkflowResetForm`. A non-numeric value failed axum's own `Form`
/// deserialization before `reset_workflow_ui` ever ran. That was a bare
/// framework 400, not the styled flash-redirect every other action on this
/// page produces. Reset is the runbook's destructive recovery action, not
/// a filter, so the fix rejects the value with a clear message. It does
/// not guess an event number the operator never typed.
#[tokio::test]
async fn detail_page_reset_action_with_invalid_event_number_redirects_with_flash_instead_of_aborting()
 {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id =
        insert_workflow_on_url(&database_url, ShardId::new(0), "reset_wf3", "reset-3").await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, headers, body) = post_form(
        &app,
        &format!("/workflows/{exec_id}/reset"),
        "reset_to_event_id=not-a-number&reason=oops",
    )
    .await;
    assert!(
        status == StatusCode::SEE_OTHER || status == StatusCode::FOUND,
        "an invalid event number must redirect with a flash error, not abort the request (got {status}): {body}"
    );
    let location = headers
        .get("location")
        .expect("redirect must have Location header")
        .to_str()
        .unwrap();
    assert!(
        location.contains(&exec_id.to_string()) || location.contains("workflows"),
        "redirect must go back to the detail page: {location}"
    );
    assert!(
        location.contains("flash"),
        "redirect must carry a flash message naming the bad value: {location}"
    );
}

/// Detail page shows a "Jump to event" control in large histories.
#[tokio::test]
async fn detail_page_has_jump_to_event_n_control() {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id = insert_workflow_on_url(&database_url, ShardId::new(0), "jump_wf", "jump-1").await;

    // Insert 150 events so pagination threshold is crossed
    let many_events: Vec<autumn_harvest::WorkflowEvent> = (0..150)
        .map(|i| autumn_harvest::WorkflowEvent::SignalReceived {
            signal_name: format!("jump_signal_{i}"),
            payload: serde_json::json!({}),
        })
        .collect();
    insert_workflow_events(&database_url, exec_id, &many_events, 1).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "detail page should render: {html}");
    assert!(
        html.contains("name=\"event_page\"") || html.contains("name=\"jump_event\""),
        "detail page should have a jump-to-event or event_page input control for large histories: {html}"
    );
    assert!(
        html.contains("Jump to") || html.contains("jump") || html.contains("Go"),
        "detail page should have a jump/go button or label for the control: {html}"
    );
}

/// GREEN — the fix under test. `jump_event`/`event_page` used to be typed
/// `i64` straight on `WorkflowDetailParams`, a `Query<..>` extractor
/// struct. A non-numeric value failed axum's own query deserialization.
/// That aborted the request with a bare framework 400, before
/// `workflow_detail_ui` ran at all. No metadata, timeline, or panel
/// rendered. The fix falls back to page zero. It surfaces the bad value
/// through the page's own flash banner instead of aborting.
#[tokio::test]
async fn detail_page_invalid_jump_event_renders_page_with_flash_instead_of_aborting() {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id =
        insert_workflow_on_url(&database_url, ShardId::new(0), "jump_wf2", "jump-2").await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(
        &app,
        &format!("/workflows/{exec_id}?jump_event=not-a-number"),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "an invalid jump_event must still render the page, not abort it: {html}"
    );
    assert!(
        html.contains("invalid jump_event") && html.contains("not-a-number"),
        "the page must surface the bad value through its flash banner: {html}"
    );
    assert!(
        html.contains(&exec_id.to_string()),
        "the detail page's own metadata must still render: {html}"
    );
}

// ---------------------------------------------------------------------------
// Issue #279: history event count and continue-as-new threshold on detail page
// ---------------------------------------------------------------------------

/// The workflow detail page must show the current event count alongside the
/// configured continue-as-new threshold in the Metadata card.
#[tokio::test]
async fn detail_page_shows_history_event_count_and_threshold() {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id = insert_workflow_on_url(&database_url, ShardId::new(0), "hist_wf", "hist-1").await;
    // Insert 7 events so we have a known, non-zero count.
    append_test_events(&database_url, exec_id, "hist", 7).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "detail page must render: {html}");

    // The page must display the event count explicitly in the metadata section.
    assert!(
        html.contains("History events"),
        "metadata card should have a 'History events' label: {html}"
    );
    assert!(
        html.contains('7'),
        "metadata card should show the count 7: {html}"
    );

    // The default threshold (10 000) must be visible alongside the count.
    let threshold_str = DEFAULT_HISTORY_CONTINUE_AS_NEW_THRESHOLD.to_string();
    assert!(
        html.contains(&threshold_str),
        "metadata card should include the continue-as-new threshold ({threshold_str}): {html}"
    );
}

/// When the registry is configured with a custom threshold the detail page must
/// reflect that value rather than the default 10 000.
#[tokio::test]
async fn detail_page_shows_custom_continue_as_new_threshold() {
    const CUSTOM_THRESHOLD: u64 = 500;

    let (database_url, _container) = setup_test_database_url().await;
    let exec_id = insert_workflow_on_url(&database_url, ShardId::new(0), "cust_wf", "cust-1").await;
    append_test_events(&database_url, exec_id, "cust", 3).await;

    // Build an app whose registry uses a distinctive non-default threshold.
    let pool = build_test_pool(&database_url);
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool));
    let registry = Arc::new(
        HandlerRegistry::new(
            vec![WorkflowInfo {
                quota: None,
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: "cust_wf",
                module: "tests",
                handler: |_ctx, input| Box::pin(async move { Ok(input) }),
                execution_timeout: None,
                chain_execution_timeout: None,
                sla: None,
                concurrency: None,

                debounce: None,
                batch: None,
                throttle: None,
                max_input_bytes: None,

                owner: None,
                runbook_url: None,
                severity: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
                retry_policy: None,
            }],
            vec![],
        )
        .with_history_policy(
            WorkflowHistoryPolicy::default().with_continue_as_new_threshold(CUSTOM_THRESHOLD),
        ),
    );
    api_state.install(HarvestApiRuntime::new(
        registry,
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        None,
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));
    let app = harvest_ui_router(api_state).with_state(test_app_state_without_database());

    let (status, html) = fetch_html(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "detail page must render: {html}");

    assert!(
        html.contains("History events"),
        "metadata card should have a 'History events' label: {html}"
    );
    assert!(
        html.contains("500"),
        "metadata card should show the custom threshold 500: {html}"
    );
    // The default threshold value must NOT appear as the threshold.
    // (It could appear elsewhere, e.g. as an event count or ID, so we verify
    // the custom value is present rather than asserting the default is absent.)
    assert!(
        html.contains('3'),
        "metadata card should show the event count 3: {html}"
    );
}

#[tokio::test]
async fn ui_schedules_displays_recent_decisions() {
    let (database_url, _container) = setup_test_database_url().await;
    let id = insert_test_schedule(&database_url, "Workflow", "decision_ui_workflow", false).await;

    // Connect to database and insert a decision
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    let occurred_at = chrono::Utc::now();
    let next_fire_at = occurred_at + chrono::Duration::hours(1);

    autumn_harvest::schedule_decision::record_decision_graceful(
        &mut conn,
        None,
        Some(id),
        "decision_ui_workflow",
        "workflow",
        "fired",
        "fired_ok",
        Some(serde_json::json!({ "run_id": "run-xyz-456" })),
        occurred_at,
        next_fire_at,
        0,
    )
    .await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/schedules").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "schedules page must return 200: {html}"
    );

    // Verify recent decisions collapsible is rendered in HTML
    assert!(
        html.contains("Recent Decisions (1)"),
        "recent decisions collapsible summary missing: {html}"
    );
    assert!(
        html.contains("fired"),
        "decision type 'fired' missing from page: {html}"
    );
    assert!(
        html.contains("fired_ok"),
        "reason code 'fired_ok' missing from page: {html}"
    );
}

#[tokio::test]
async fn ui_trigger_preserves_dag_metadata() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let dag_name = "ui_metadata_dag";

    let dag_info = autumn_harvest::info::DagInfo {
        name: dag_name,
        module: "tests",
        schedule: Some(autumn_harvest::policy::Schedule::Manual),
        catchup: false,
        max_active_runs: 1,
        default_queue: Some("dag-workers"),
        builder: |_dag| {},
        workflow_handler: Some(|_ctx, input| Box::pin(async move { Ok(input) })),
        jitter: std::time::Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,
        owner: Some("ui-team"),
        runbook_url: Some("http://ui-runbook"),
        severity: Some("sev3"),
        mcp: false,
        execution_timeout: None,
        sla: None,
    };

    let dag_catalog =
        Arc::new(autumn_harvest::compile_dag_catalog(vec![dag_info]).expect("dag compiles"));

    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: dag_name,
            module: "tests",
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,
            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![],
    ));

    let schedule_id = insert_test_schedule(&database_url, "Dag", dag_name, false).await;

    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::clone(&registry),
        Arc::clone(&dag_catalog),
        Arc::new(Vec::new()),
        Some("scheduler-only".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));
    // Mount the UI router
    let app = harvest_ui_router(api_state).with_state(test_app_state_without_database());

    // POST /schedules/{id}/trigger-now
    let (status, _headers, _body) = post_form(
        &app,
        &format!("/schedules/{schedule_id}/trigger-now"),
        String::new(),
    )
    .await;
    assert!(
        status.is_redirection(),
        "UI trigger must redirect; got {status}"
    );

    let execution = load_latest_workflow_execution_by_name_from_url(&database_url, dag_name)
        .await
        .expect("triggered execution should exist");
    assert_eq!(execution.owner.as_deref(), Some("ui-team"));
    assert_eq!(execution.runbook_url.as_deref(), Some("http://ui-runbook"));
    assert_eq!(execution.severity.as_deref(), Some("sev3"));
}

/// Issue #743 review (PR #1141, Finding #6): `POST /schedules/{id}/trigger-now`
/// (the Vantage UI manual-trigger route) is a DISTINCT DAG-start entry point
/// from the scheduler tick, the buffered-drain, the direct HTTP/MCP trigger
/// (`trigger_unified_dag`), and a backfill -- and its `execute_schedule_trigger_ui`
/// handler hardcoded `execution_timeout`/`max_execution_timeout_ceiling` to
/// `None`. Declares a 10h `execution_timeout` + 5h `sla` against a 1h
/// fleet-wide ceiling, so both the ceiling clamp AND the clamp-sla-to-
/// effective-timeout rule fire in one assertion (mirrors the sibling backfill
/// test `backfill_dag_threads_declared_execution_timeout_sla_and_fleet_ceiling`
/// in `api_scheduler_integration.rs`).
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn ui_trigger_now_threads_dag_execution_timeout_sla_and_fleet_ceiling() {
    let (database_url, _container) = overdue_read_database_url().await;
    let pool = build_test_pool(&database_url);
    let dag_name = "ui_trigger_deadline_dag";

    let dag_info = autumn_harvest::info::DagInfo {
        name: dag_name,
        module: "tests",
        schedule: Some(autumn_harvest::policy::Schedule::Manual),
        catchup: false,
        max_active_runs: 1,
        default_queue: Some("dag-workers"),
        builder: |_dag| {},
        workflow_handler: Some(|_ctx, input| Box::pin(async move { Ok(input) })),
        jitter: std::time::Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,
        owner: None,
        runbook_url: None,
        severity: None,
        mcp: false,
        execution_timeout: Some(std::time::Duration::from_secs(10 * 3600)),
        sla: Some(std::time::Duration::from_secs(5 * 3600)),
    };

    let dag_catalog =
        Arc::new(autumn_harvest::compile_dag_catalog(vec![dag_info]).expect("dag compiles"));

    // The DAG's shadow WorkflowInfo, registered under `dag_name` -- exactly
    // what `DagInfo::as_workflow_info()` would produce, hand-built here
    // (matching the pre-existing sibling test's convention) since
    // `execute_schedule_trigger_ui` reads execution_timeout/sla from THIS
    // registry lookup, not from `DagInfo` directly.
    let registry = Arc::new(
        HandlerRegistry::new(
            vec![WorkflowInfo {
                quota: None,
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: dag_name,
                module: "tests",
                handler: |_ctx, input| Box::pin(async move { Ok(input) }),
                execution_timeout: Some(std::time::Duration::from_secs(10 * 3600)),
                chain_execution_timeout: None,
                sla: Some(std::time::Duration::from_secs(5 * 3600)),
                concurrency: None,
                debounce: None,
                batch: None,
                throttle: None,
                max_input_bytes: None,
                owner: None,
                runbook_url: None,
                severity: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
                retry_policy: None,
            }],
            vec![],
        )
        .with_max_workflow_execution_timeout(Some(std::time::Duration::from_secs(3600))),
    );

    let schedule_id = insert_test_schedule(&database_url, "Dag", dag_name, false).await;

    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::clone(&registry),
        Arc::clone(&dag_catalog),
        Arc::new(Vec::new()),
        Some("scheduler-only".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));
    let app = harvest_ui_router(api_state).with_state(test_app_state_without_database());

    let (status, _headers, _body) = post_form(
        &app,
        &format!("/schedules/{schedule_id}/trigger-now"),
        String::new(),
    )
    .await;
    assert!(
        status.is_redirection(),
        "UI trigger must redirect; got {status}"
    );

    let execution = load_latest_workflow_execution_by_name_from_url(&database_url, dag_name)
        .await
        .expect("triggered execution should exist");

    assert_eq!(
        execution.execution_timeout,
        Some(chrono::Duration::seconds(3600)),
        "execution_timeout must be clamped to the 1h fleet-wide ceiling, not the declared 10h"
    );
    let deadline_at = execution
        .deadline_at
        .expect("deadline_at must be set from the ceiling-clamped execution_timeout");
    let deadline_delta = (deadline_at - execution.started_at) - chrono::Duration::seconds(3600);
    assert!(
        deadline_delta.num_milliseconds().abs() < 2000,
        "deadline_at must be ~1h after started_at (ceiling-clamped); delta={deadline_delta}"
    );
    assert_eq!(
        execution.sla,
        Some(chrono::Duration::seconds(3600)),
        "sla must clamp to the ceiling-clamped effective execution_timeout, not the raw 5h"
    );
    let sla_deadline_at = execution
        .sla_deadline_at
        .expect("sla_deadline_at must be set from the clamped sla");
    let sla_deadline_delta =
        (sla_deadline_at - execution.started_at) - chrono::Duration::seconds(3600);
    assert!(
        sla_deadline_delta.num_milliseconds().abs() < 2000,
        "sla_deadline_at must be ~1h after started_at (clamped); delta={sla_deadline_delta}"
    );
}

async fn load_latest_workflow_execution_by_name_from_url(
    database_url: &str,
    workflow_name: &str,
) -> Option<autumn_harvest::models::WorkflowExecution> {
    use diesel::OptionalExtension;
    use diesel::SelectableHelper;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for workflow lookup");
    autumn_harvest::schema::harvest_workflow_executions::table
        .filter(
            autumn_harvest::schema::harvest_workflow_executions::workflow_name.eq(workflow_name),
        )
        .order(autumn_harvest::schema::harvest_workflow_executions::created_at.desc())
        .select(autumn_harvest::models::WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .optional()
        .expect("failed to load workflow rows by workflow name")
}

// ── Read-path payload decoding in the Vantage UI (issue #608) ────────────────
//
// Seeding note: engine write paths use identity codecs, so envelope-bearing
// rows are synthesized directly (an envelope is just JSON; identity
// persistence stores it verbatim). No Docker in the authoring sandbox — these
// tests are compile-checked only, per the #543/#544/#601 precedent.

#[derive(Debug)]
struct ReverseCodec608;

impl autumn_harvest::payload_codec::PayloadCodec for ReverseCodec608 {
    fn codec_id(&self) -> &'static str {
        "reverse"
    }
    fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, autumn_harvest::payload_codec::CodecError> {
        let mut v = raw.to_vec();
        v.reverse();
        Ok(v)
    }
    fn decode(&self, encoded: &[u8]) -> Result<Vec<u8>, autumn_harvest::payload_codec::CodecError> {
        let mut v = encoded.to_vec();
        v.reverse();
        Ok(v)
    }
}

fn decode_test_codecs() -> autumn_harvest::payload_codec::PayloadCodecs {
    let mut codecs = autumn_harvest::payload_codec::PayloadCodecs::default();
    codecs.set_default(Arc::new(ReverseCodec608));
    codecs
}

/// Builds a well-formed `reverse` codec envelope for `plain` via the public
/// `encode_event` round-trip (no base64 dep needed in this test crate).
fn envelope_608(plain: &Value) -> Value {
    let event = autumn_harvest::WorkflowEvent::WorkflowCompleted {
        output: plain.clone(),
    };
    let encoded = decode_test_codecs()
        .encode_event(&event)
        .expect("encode event");
    encoded["data"]["output"].clone()
}

/// Single-shard API+UI app with read-path decoding enabled (issue #608):
/// admin boundary + codec registry mirrored + the opt-in flag set.
fn build_decode_enabled_api_with_ui_app(database_url: &str) -> axum::Router {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    // Codec registry mirror + deployment opt-in (issue #608).
    api_state.set_payload_codecs(decode_test_codecs());
    api_state.set_decode_payloads_on_read(true);
    api_state.install_storage_pool(HarvestDbPool::from(build_test_pool(database_url)));
    api_state.install(HarvestApiRuntime::new(
        echo_registry(),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        None,
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));

    autumn_harvest_plugin::harvest_api_router(api_state.clone())
        .nest("/ui", harvest_ui_router(api_state))
        .with_state(test_app_state_without_database())
}

async fn count_decode_audit_rows(database_url: &str) -> i64 {
    use autumn_harvest::schema::harvest_audit_log;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect for audit count");
    harvest_audit_log::table
        .filter(harvest_audit_log::operation.eq(autumn_harvest::audit::OP_PAYLOAD_DECODE_READ))
        .count()
        .get_result(&mut conn)
        .await
        .expect("failed to count decode audit rows")
}

/// The `source` column of every `payload.decode_read` audit row — UI page
/// renders must attribute their rows to `SOURCE_UI` like every other audit
/// row ui.rs writes.
async fn decode_audit_sources(database_url: &str) -> Vec<String> {
    use autumn_harvest::schema::harvest_audit_log;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect for audit sources");
    harvest_audit_log::table
        .filter(harvest_audit_log::operation.eq(autumn_harvest::audit::OP_PAYLOAD_DECODE_READ))
        .select(harvest_audit_log::source)
        .load(&mut conn)
        .await
        .expect("failed to load decode audit sources")
}

/// Issue #608 (DLQ UI): the dead-letter page renders the decoded task input
/// and error instead of ciphertext, and the plaintext page render is audited.
#[tokio::test]
async fn dlq_ui_page_renders_decoded_payloads_and_audits() {
    let (database_url, _container) = setup_test_database_url().await;

    // Seed one dead letter whose input is a codec envelope and whose TEXT
    // error is a stringified envelope.
    let exec_id = insert_workflow_on_url(
        &database_url,
        ShardId::new(0),
        "encrypted_workflow",
        "dlq-decode-1",
    )
    .await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect for dead-letter insert");
    autumn_harvest::dlq::dead_letter(
        &mut conn,
        &autumn_harvest::dlq::NewDeadLetterEntry {
            original_task_id: uuid::Uuid::new_v4(),
            queue_name: "default".to_string(),
            task_type: "ACTIVITY".to_string(),
            workflow_exec_id: Some(exec_id.as_uuid()),
            activity_name: Some("charge_card".to_string()),
            input: envelope_608(&json!({"card": "pii-dlq-input"})),
            error: serde_json::to_string(&envelope_608(&json!("declined: pii-dlq-error")))
                .expect("serialize envelope"),
            attempts: 3,
            owner: None,
            severity: None,
        },
    )
    .await
    .expect("dead-letter insert should succeed");

    let app = build_decode_enabled_api_with_ui_app(&database_url);

    let (status, html) = fetch_html(&app, "/ui/dead-letters").await;
    assert_eq!(status, StatusCode::OK, "DLQ page should render: {html}");
    assert!(
        html.contains("pii-dlq-input"),
        "DLQ row input must render decoded plaintext: {html}"
    );
    assert!(
        html.contains("pii-dlq-error"),
        "DLQ row error must render decoded plaintext: {html}"
    );
    assert!(
        !html.contains("_harvest_codec_envelope"),
        "DLQ page must not render raw envelopes: {html}"
    );

    assert!(
        count_decode_audit_rows(&database_url).await >= 1,
        "a DLQ page render that decoded payloads must write a payload.decode_read audit row"
    );
}

/// Issue #608 (workflow detail UI): the detail page renders the decoded
/// workflow input instead of the stored envelope.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn workflow_detail_ui_renders_decoded_input() {
    let (database_url, _container) = setup_test_database_url().await;

    // A workflow whose stored input is a codec envelope (identity persistence
    // stores the envelope object verbatim).
    let input_envelope = envelope_608(&json!({"user": "pii-detail-input"}));
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect for workflow insert");
    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: "encrypted_workflow",
            workflow_id: "detail-decode-1",
            exec_id,
            input: input_envelope.clone(),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::default(),
            conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            chain_execution_timeout: None,
            max_workflow_chain_timeout_ceiling: None,
            inherited_chain_deadline_at: None,
            concurrency_key: None,
            concurrency_limit: None,
            concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
            priority: Priority::default(),
            max_workflow_input_bytes: 0,
            start_at: None,
            delay: None,
            max_workflow_start_delay: None,
            owner: None,
            runbook_url: None,
            severity: None,
            context_headers: None,
            sla: None,
            schedule_id: None,
            scheduled_for: None,
            workflow_attempt: 1,
            workflow_retry_policy: None,
            retry_of_exec_id: None,
            max_workflow_attempts_ceiling: None,
            origin: None,
            completion_callbacks: None,
            start_source: autumn_harvest::StartSource::Api,
            start_source_ref: None,
            started_by: None,
        },
        None,
    )
    .await
    .expect("workflow insert should succeed");

    // Blocked-on panel + timeline fixtures (issue #608, AC4): an
    // envelope-bearing timeline event, a pending activity whose stored
    // heartbeat checkpoint is an envelope, and an unconsumed signal whose
    // payload is an envelope. The panel renders the checkpoint and the
    // timeline renders each event's data; the blocked-on activity *input*
    // and signal *payload* are never rendered, so they are deliberately NOT
    // decoded (PR #936 review, round 5) — their plaintext must not appear
    // anywhere in the page.
    {
        // `load_history_undecoded`: the seeded `WorkflowStarted` input is a
        // "reverse"-codec envelope, and the strict `load_history` (identity-only
        // registry) hard-errors `UnknownPayloadCodec` on it — the exact
        // strict-loader behavior PR #936 round 2 documented. The raw loader is
        // the correct fixture tool for reading `next_event_id` here.
        let history = autumn_harvest::store::load_history_undecoded(&mut conn, exec_id)
            .await
            .expect("load history");
        autumn_harvest::store::append_events(
            &mut conn,
            exec_id,
            &[autumn_harvest::WorkflowEvent::ActivityScheduled {
                activity_id: autumn_harvest::types::ActivityExecId::new(),
                name: "charge_card".to_string(),
                input: envelope_608(&json!({"card": "pii-detail-event"})),
                queue: "default".to_string(),
            }],
            history.next_event_id,
        )
        .await
        .expect("append timeline event");

        let mut params = autumn_harvest::queue::EnqueueParams::new(
            "default",
            autumn_harvest::queue::TaskType::Activity,
            envelope_608(&json!({"card": "pii-detail-task-input"})),
        );
        params.workflow_exec_id = Some(exec_id.as_uuid());
        params.activity_name = Some("charge_card".to_string());
        autumn_harvest::queue::enqueue(&mut conn, &params)
            .await
            .expect("seed pending activity task");
        diesel::update(
            harvest_task_queue::table
                .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid()))),
        )
        .set((
            harvest_task_queue::heartbeat_details.eq(Some(envelope_608(
                &json!({"progress": "pii-detail-checkpoint"}),
            ))),
            harvest_task_queue::last_heartbeat_at.eq(Some(chrono::Utc::now())),
        ))
        .execute(&mut conn)
        .await
        .expect("seed heartbeat checkpoint");

        autumn_harvest::signal::send_signal(
            &mut conn,
            exec_id,
            "approval",
            envelope_608(&json!({"approver": "pii-detail-signal"})),
        )
        .await
        .expect("seed pending signal");
    }

    let app = build_decode_enabled_api_with_ui_app(&database_url);

    let (status, html) = fetch_html(&app, &format!("/ui/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "detail page should render: {html}");
    assert!(
        html.contains("pii-detail-input"),
        "workflow detail must render the decoded input: {html}"
    );
    assert!(
        html.contains("pii-detail-event"),
        "the timeline must render decoded event payloads: {html}"
    );
    assert!(
        html.contains("pii-detail-checkpoint"),
        "the blocked-on panel must render the decoded heartbeat checkpoint: {html}"
    );
    assert!(
        !html.contains("_harvest_codec_envelope"),
        "workflow detail must not render the raw envelope: {html}"
    );
    // Hidden fields are not decoded (PR #936 round 5): their plaintext never
    // reaches the page.
    assert!(
        !html.contains("pii-detail-task-input"),
        "the never-rendered pending-activity input must not be decoded: {html}"
    );
    assert!(
        !html.contains("pii-detail-signal"),
        "the never-rendered pending-signal payload must not be decoded: {html}"
    );

    // UI-originated decode rows carry `source: ui`, matching every other
    // audit row ui.rs writes.
    let sources = decode_audit_sources(&database_url).await;
    assert!(
        !sources.is_empty() && sources.iter().all(|s| s == "ui"),
        "UI decode audit rows must carry source=ui: {sources:?}"
    );

    // The stored row keeps its ciphertext (read-path only, append-only safe).
    let stored_input: Value = autumn_harvest::schema::harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(autumn_harvest::schema::harvest_workflow_executions::input)
        .first(&mut conn)
        .await
        .expect("load stored input");
    assert_eq!(
        stored_input, input_envelope,
        "stored input must remain ciphertext after the decoded render"
    );
}

/// Issue #608 / PR #936 round 5: a detail render whose *only* envelopes live
/// in fields the page never displays — the pending-activity `input`, the
/// pending-signal payload, and the attempts/signals panel event copies
/// (isolated from the timeline via `?event_page=1`) — must do zero decode
/// work and write NO `payload.decode_read` audit row.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn workflow_detail_ui_writes_no_audit_row_when_only_hidden_fields_carry_envelopes() {
    let (database_url, _container) = setup_test_database_url().await;

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect for workflow insert");
    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: "encrypted_workflow",
            workflow_id: "detail-decode-hidden-1",
            exec_id,
            // A plain, already-decoded input: the rendered fields carry no
            // envelopes at all in this fixture.
            input: json!({"user": "plain-input"}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::default(),
            conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            chain_execution_timeout: None,
            max_workflow_chain_timeout_ceiling: None,
            inherited_chain_deadline_at: None,
            concurrency_key: None,
            concurrency_limit: None,
            concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
            priority: Priority::default(),
            max_workflow_input_bytes: 0,
            start_at: None,
            delay: None,
            max_workflow_start_delay: None,
            owner: None,
            runbook_url: None,
            severity: None,
            context_headers: None,
            sla: None,
            schedule_id: None,
            scheduled_for: None,
            workflow_attempt: 1,
            workflow_retry_policy: None,
            retry_of_exec_id: None,
            max_workflow_attempts_ceiling: None,
            origin: None,
            completion_callbacks: None,
            start_source: autumn_harvest::StartSource::Api,
            start_source_ref: None,
            started_by: None,
        },
        None,
    )
    .await
    .expect("workflow insert should succeed");

    // Hidden-surface envelopes only: an attempts-panel activity event and a
    // signals-panel signal event (kept off the requested timeline page via
    // `?event_page=1`), a pending activity whose `input` is an envelope (no
    // heartbeat checkpoint), and an unconsumed signal with an envelope
    // payload.
    {
        let history = autumn_harvest::store::load_history(&mut conn, exec_id)
            .await
            .expect("load history");
        autumn_harvest::store::append_events(
            &mut conn,
            exec_id,
            &[
                autumn_harvest::WorkflowEvent::ActivityScheduled {
                    activity_id: autumn_harvest::types::ActivityExecId::new(),
                    name: "charge_card".to_string(),
                    input: envelope_608(&json!({"card": "pii-hidden-event-input"})),
                    queue: "default".to_string(),
                },
                autumn_harvest::WorkflowEvent::SignalReceived {
                    signal_name: "approval".to_string(),
                    payload: envelope_608(&json!({"approver": "pii-hidden-event-signal"})),
                },
            ],
            history.next_event_id,
        )
        .await
        .expect("append hidden-panel events");

        let mut params = autumn_harvest::queue::EnqueueParams::new(
            "default",
            autumn_harvest::queue::TaskType::Activity,
            envelope_608(&json!({"card": "pii-hidden-task-input"})),
        );
        params.workflow_exec_id = Some(exec_id.as_uuid());
        params.activity_name = Some("charge_card".to_string());
        autumn_harvest::queue::enqueue(&mut conn, &params)
            .await
            .expect("seed pending activity task");

        autumn_harvest::signal::send_signal(
            &mut conn,
            exec_id,
            "approval",
            envelope_608(&json!({"approver": "pii-hidden-signal"})),
        )
        .await
        .expect("seed pending signal");
    }

    let app = build_decode_enabled_api_with_ui_app(&database_url);

    // event_page=1 → the timeline page is empty (offset 100 past the seeded
    // events), so the only envelope-bearing copies the handler holds are the
    // hidden ones.
    let (status, html) = fetch_html(&app, &format!("/ui/workflows/{exec_id}?event_page=1")).await;
    assert_eq!(status, StatusCode::OK, "detail page should render: {html}");
    assert!(
        !html.contains("pii-hidden-"),
        "no hidden-field plaintext may appear in the page: {html}"
    );
    assert!(
        !html.contains("_harvest_codec_envelope"),
        "no raw envelope may appear in the page: {html}"
    );

    assert_eq!(
        count_decode_audit_rows(&database_url).await,
        0,
        "a render whose only envelopes are hidden fields must not write a \
         payload.decode_read audit row"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Issue #957 — Vantage DAG run graph view
//
// These exercise the enhanced `GET /dags/{dag_name}` page (server-rendered inline
// SVG built from `dag_graph::build_run_graph`, the same in-process call the #690
// API handler makes) plus the 3-click retry flow (node → dry-run confirm →
// admin-gated commit) that delegates to the shipped #366 retry endpoint via the
// extracted `retry_dag_run_inner`. Docker-backed (testcontainers); compile-checked
// in sandboxes without Docker, run on Linux CI.
// ─────────────────────────────────────────────────────────────────────────────

#[activity(queue = "default")]
async fn dag957_step_a(_ctx: &ActivityContext) -> Result<(), String> {
    Ok(())
}
#[activity(queue = "default")]
async fn dag957_step_b(_ctx: &ActivityContext) -> Result<(), String> {
    Ok(())
}
#[activity(queue = "default")]
async fn dag957_step_c(_ctx: &ActivityContext) -> Result<(), String> {
    Ok(())
}
#[activity(queue = "default")]
async fn dag957_step_d(_ctx: &ActivityContext) -> Result<(), String> {
    Ok(())
}

/// Linear: `dag957_step_a` -> `dag957_step_b` -> `dag957_step_c`
#[dag(default_queue = "default")]
fn dag957_linear(dag: &mut DagBuilder) {
    let a = dag.activity(dag957_step_a);
    let b = dag.activity(dag957_step_b).upstream(&a);
    let _c = dag.activity(dag957_step_c).upstream(&b);
}

/// Fan-out: a -> {b, c} -> d
#[dag(default_queue = "default")]
fn dag957_fanout(dag: &mut DagBuilder) {
    let a = dag.activity(dag957_step_a);
    let b = dag.activity(dag957_step_b).upstream(&a);
    let c = dag.activity(dag957_step_c).upstream(&a);
    let _d = dag.activity(dag957_step_d).upstream(&b).upstream(&c);
}

fn dag957_registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        workflows![dag957_linear, dag957_fanout],
        activities![dag957_step_a, dag957_step_b, dag957_step_c, dag957_step_d],
    ))
}

/// A hand-built classic (non-unified) `RegisteredDag`; `compile_dag_catalog`
/// rejects classic DAGs, so a hand-built definition is the only way to register
/// one for the degraded-state test.
fn dag957_classic(name: &str) -> RegisteredDag {
    let mut builder = DagBuilder::new();
    let _a = builder.activity(dag957_step_a);
    let definition = builder.build().expect("classic dag builds");
    RegisteredDag {
        name: name.to_string(),
        module: "test".to_string(),
        schedule: None,
        catchup: false,
        max_active_runs: 1,
        default_queue: Some("default".to_string()),
        is_unified: false,
        definition,
        jitter: Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::default(),
        buffer_all_max: 0,
        owner: None,
        runbook_url: None,
        severity: None,
    }
}

/// A large (>200-task) unified DAG built by hand so the ≥200-node scrollable
/// fallback can be exercised without declaring 200 activity fns.
fn dag957_large(name: &str) -> RegisteredDag {
    let mut builder = DagBuilder::new();
    for _ in 0..220 {
        let _ = builder.activity(dag957_step_a);
    }
    let definition = builder.build().expect("large dag builds");
    RegisteredDag {
        name: name.to_string(),
        module: "test".to_string(),
        schedule: None,
        catchup: false,
        max_active_runs: 1,
        default_queue: Some("default".to_string()),
        is_unified: true,
        definition,
        jitter: Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::default(),
        buffer_all_max: 0,
        owner: None,
        runbook_url: None,
        severity: None,
    }
}

fn build_dag957_ui_app(
    database_url: &str,
    admin: bool,
    extra: Vec<(String, RegisteredDag)>,
) -> axum::Router {
    let pool = build_test_pool(database_url);
    let mut catalog =
        compile_dag_catalog(dags![dag957_linear, dag957_fanout]).expect("dag catalog compiles");
    for (name, dag) in extra {
        catalog.insert(name, dag);
    }
    let api_state = HarvestApiState::new();
    if admin {
        api_state.set_admin_auth_boundary(true);
    }
    api_state.install_storage_pool(HarvestDbPool::from(pool));
    api_state.install(HarvestApiRuntime::new(
        dag957_registry(),
        Arc::new(catalog),
        Arc::new(Vec::new()),
        Some("dag957-ui-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));
    harvest_ui_router(api_state).with_state(test_app_state_without_database())
}

fn dag957_sched(name: &str, id: autumn_harvest::ActivityExecId) -> autumn_harvest::WorkflowEvent {
    autumn_harvest::WorkflowEvent::ActivityScheduled {
        activity_id: id,
        name: name.to_string(),
        input: json!({ "dag_task": name }),
        queue: "default".to_string(),
    }
}
fn dag957_started(id: autumn_harvest::ActivityExecId) -> autumn_harvest::WorkflowEvent {
    autumn_harvest::WorkflowEvent::ActivityStarted {
        activity_id: id,
        worker_id: autumn_harvest::types::WorkerId::new("worker-1"),
    }
}
const fn dag957_completed(id: autumn_harvest::ActivityExecId) -> autumn_harvest::WorkflowEvent {
    autumn_harvest::WorkflowEvent::ActivityCompleted {
        activity_id: id,
        output: Value::Null,
    }
}
fn dag957_failed(id: autumn_harvest::ActivityExecId) -> autumn_harvest::WorkflowEvent {
    autumn_harvest::WorkflowEvent::ActivityFailed {
        activity_id: id,
        error: "transient S3 500\nstack trace line".to_string(),
        attempt: 1,
        error_type: "S3Error".to_string(),
        non_retryable: false,
        details: None,
    }
}

async fn dag957_seed_run(
    database_url: &str,
    dag_name: &'static str,
    workflow_id: &str,
    events: Vec<autumn_harvest::WorkflowEvent>,
    state: &str,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("connect for dag957 seed");
    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: dag_name,
            workflow_id,
            exec_id,
            input: json!({}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::default(),
            conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            chain_execution_timeout: None,
            max_workflow_chain_timeout_ceiling: None,
            inherited_chain_deadline_at: None,
            concurrency_key: None,
            concurrency_limit: None,
            concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
            priority: Priority::default(),
            max_workflow_input_bytes: 0,
            start_at: None,
            delay: None,
            max_workflow_start_delay: None,
            owner: None,
            runbook_url: None,
            severity: None,
            context_headers: None,
            sla: None,
            schedule_id: None,
            scheduled_for: None,
            workflow_attempt: 1,
            workflow_retry_policy: None,
            retry_of_exec_id: None,
            max_workflow_attempts_ceiling: None,
            origin: None,
            completion_callbacks: None,
            start_source: autumn_harvest::StartSource::Api,
            start_source_ref: None,
            started_by: None,
        },
        None,
    )
    .await
    .expect("seed workflow");

    let history = store::load_history(&mut conn, exec_id).await.unwrap();
    store::append_events(&mut conn, exec_id, &events, history.next_event_id)
        .await
        .expect("append seed events");
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set(harvest_workflow_executions::state.eq(state))
        .execute(&mut conn)
        .await
        .expect("set state");
    exec_id
}

async fn dag957_audit_rows(database_url: &str, operation: &str, source: &str) -> i64 {
    use autumn_harvest::schema::harvest_audit_log;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("connect for audit count");
    harvest_audit_log::table
        .filter(harvest_audit_log::operation.eq(operation.to_string()))
        .filter(harvest_audit_log::source.eq(source.to_string()))
        .count()
        .get_result(&mut conn)
        .await
        .expect("count audit rows")
}

// I-A
#[tokio::test]
async fn ui_dag_run_graph_renders_mixed_status_run() {
    let (url, _c) = setup_test_database_url().await;
    let app = build_dag957_ui_app(&url, true, vec![]);

    let (ia, ib) = (
        autumn_harvest::ActivityExecId::new(),
        autumn_harvest::ActivityExecId::new(),
    );
    // a succeeded, b failed, c pending. Run FAILED.
    let events = vec![
        dag957_sched("dag957_step_a", ia),
        dag957_started(ia),
        dag957_completed(ia),
        dag957_sched("dag957_step_b", ib),
        dag957_started(ib),
        dag957_failed(ib),
        autumn_harvest::WorkflowEvent::workflow_failed("dag failed"),
    ];
    let exec_id = dag957_seed_run(&url, "dag957_linear", "graph-mixed", events, "FAILED").await;

    let (status, html) = fetch_html(&app, &format!("/dags/dag957_linear?run={exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "body: {html}");
    assert!(html.contains("<svg"), "inline svg present: {html}");
    assert!(html.contains("dag957_step_a"));
    assert!(html.contains("dag957_step_b"));
    assert!(html.contains("dag957_step_c"));
    // Status fills for the three distinct statuses present in the run.
    assert!(html.contains("#166534"), "succeeded fill present"); // Succeeded
    assert!(html.contains("#991b1b"), "failed fill present"); // Failed
}

// I-B
#[tokio::test]
async fn ui_dag_run_graph_node_panel_shows_failure_detail() {
    let (url, _c) = setup_test_database_url().await;
    let app = build_dag957_ui_app(&url, true, vec![]);

    let (ia, ib) = (
        autumn_harvest::ActivityExecId::new(),
        autumn_harvest::ActivityExecId::new(),
    );
    let events = vec![
        dag957_sched("dag957_step_a", ia),
        dag957_started(ia),
        dag957_completed(ia),
        dag957_sched("dag957_step_b", ib),
        dag957_started(ib),
        dag957_failed(ib),
        autumn_harvest::WorkflowEvent::workflow_failed("dag failed"),
    ];
    let exec_id = dag957_seed_run(&url, "dag957_linear", "graph-panel", events, "FAILED").await;

    // node index 1 == dag957_step_b (failed).
    let (status, html) =
        fetch_html(&app, &format!("/dags/dag957_linear?run={exec_id}&node=1")).await;
    assert_eq!(status, StatusCode::OK, "body: {html}");
    assert!(html.contains("S3Error"), "error_type shown: {html}");
    assert!(html.contains("transient S3 500"), "first-line error shown");
    assert!(
        html.contains("/retry"),
        "retry link offered for the failed node"
    );
    assert!(html.contains("from_node=dag957_step_b"));
}

// I-C
#[tokio::test]
async fn ui_dag_retry_confirm_lists_widened_nodes() {
    let (url, _c) = setup_test_database_url().await;
    let app = build_dag957_ui_app(&url, true, vec![]);

    let (ia, ib) = (
        autumn_harvest::ActivityExecId::new(),
        autumn_harvest::ActivityExecId::new(),
    );
    let events = vec![
        dag957_sched("dag957_step_a", ia),
        dag957_started(ia),
        dag957_completed(ia),
        dag957_sched("dag957_step_b", ib),
        dag957_started(ib),
        dag957_failed(ib),
        autumn_harvest::WorkflowEvent::workflow_failed("dag failed"),
    ];
    let exec_id = dag957_seed_run(&url, "dag957_linear", "graph-confirm", events, "FAILED").await;

    let (status, html) = fetch_html(
        &app,
        &format!("/dags/dag957_linear/runs/{exec_id}/retry?from_node=dag957_step_b"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {html}");
    // Retrying from b widens to b + its downstream closure (c).
    assert!(
        html.contains("dag957_step_b"),
        "widened list names the source node"
    );
    assert!(
        html.contains("dag957_step_c"),
        "widened list names the downstream node"
    );
    assert!(html.contains("reason"), "reason field present");
    assert!(
        html.to_lowercase().contains("confirm"),
        "a confirm control is present: {html}"
    );
}

// I-D
#[tokio::test]
async fn ui_dag_retry_commit_starts_new_run_and_audits() {
    let (url, _c) = setup_test_database_url().await;
    let app = build_dag957_ui_app(&url, true, vec![]);

    let (ia, ib) = (
        autumn_harvest::ActivityExecId::new(),
        autumn_harvest::ActivityExecId::new(),
    );
    let events = vec![
        dag957_sched("dag957_step_a", ia),
        dag957_started(ia),
        dag957_completed(ia),
        dag957_sched("dag957_step_b", ib),
        dag957_started(ib),
        dag957_failed(ib),
        autumn_harvest::WorkflowEvent::workflow_failed("dag failed"),
    ];
    let exec_id = dag957_seed_run(&url, "dag957_linear", "graph-commit", events, "FAILED").await;

    let (status, headers, _body) = post_form(
        &app,
        &format!("/dags/dag957_linear/runs/{exec_id}/retry"),
        "from_node=dag957_step_b&reason=retry+via+vantage",
    )
    .await;
    assert!(status.is_redirection(), "commit redirects; got {status}");
    let location = headers
        .get("location")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    assert!(
        location.contains("flash="),
        "redirect carries a flash: {location}"
    );

    // Audit row written under OP_DAG_RETRY with source=ui.
    let count = dag957_audit_rows(
        &url,
        autumn_harvest::audit::OP_DAG_RETRY,
        autumn_harvest::audit::SOURCE_UI,
    )
    .await;
    assert!(count >= 1, "a dag.retry audit row from the UI must exist");
}

// I-E
#[tokio::test]
async fn ui_dag_retry_error_renders_human_message() {
    let (url, _c) = setup_test_database_url().await;
    let app = build_dag957_ui_app(&url, true, vec![]);

    // A COMPLETED run cannot be retried (409, "DAG run succeeded ...").
    let ia = autumn_harvest::ActivityExecId::new();
    let events = vec![
        dag957_sched("dag957_step_a", ia),
        dag957_started(ia),
        dag957_completed(ia),
        autumn_harvest::WorkflowEvent::WorkflowCompleted {
            output: Value::Null,
        },
    ];
    let exec_id = dag957_seed_run(&url, "dag957_linear", "graph-done", events, "COMPLETED").await;

    let (status, html) = fetch_html(
        &app,
        &format!("/dags/dag957_linear/runs/{exec_id}/retry?from_node=dag957_step_a"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {html}");
    assert!(
        html.contains("succeeded"),
        "the 409 conflict renders as a human message: {html}"
    );
    assert!(
        !html.contains("\"message\""),
        "no raw JSON body is shown to the operator: {html}"
    );
}

// I-F
#[tokio::test]
async fn ui_dag_run_graph_classic_dag_degraded() {
    let (url, _c) = setup_test_database_url().await;
    let classic = dag957_classic("dag957_classic");
    let app = build_dag957_ui_app(&url, true, vec![("dag957_classic".to_string(), classic)]);

    let (status, html) = fetch_html(&app, "/dags/dag957_classic").await;
    assert_eq!(status, StatusCode::OK, "body: {html}");
    assert!(
        html.contains("No topology available for classic DAG runs"),
        "classic DAGs render the degraded message: {html}"
    );
}

// I-G
#[tokio::test]
async fn ui_dag_retry_requires_admin() {
    let (url, _c) = setup_test_database_url().await;
    // No admin boundary → the retry route layer rejects before any handler runs.
    let app = build_dag957_ui_app(&url, false, vec![]);
    let bogus = ExecutionId::new_for_shard(ShardId::new(0));

    let (status, _headers, _body) = post_form(
        &app,
        &format!("/dags/dag957_linear/runs/{bogus}/retry"),
        "from_node=dag957_step_b&reason=x",
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

// I-H
#[tokio::test]
async fn ui_dag_run_graph_large_dag_scrollable() {
    let (url, _c) = setup_test_database_url().await;
    let large = dag957_large("dag957_large");
    let app = build_dag957_ui_app(&url, true, vec![("dag957_large".to_string(), large)]);

    let exec_id = dag957_seed_run(&url, "dag957_large", "graph-large", vec![], "RUNNING").await;

    // Issue #957 success metric: the graph view renders a large run (here 220
    // nodes, well past the 100-node budget) in < 1 s server-side. Mirrors the
    // sibling #960 J-G perf test and the workers-page precedent.
    let start = std::time::Instant::now();
    let (status, html) = fetch_html(&app, &format!("/dags/dag957_large?run={exec_id}")).await;
    let elapsed = start.elapsed();
    assert_eq!(status, StatusCode::OK, "body: {html}");
    assert!(
        html.contains("dag-graph-scroll"),
        "a large DAG uses the scrollable container, not a disabled render: {html}"
    );
    assert!(html.contains("Large DAG"), "a large-DAG note is shown");
    assert!(
        elapsed < Duration::from_secs(1),
        "220-node DAG graph renders server-side in < 1s (took {elapsed:?})"
    );
}

// I-I
#[tokio::test]
async fn ui_dag_run_graph_unknown_run_returns_404_message() {
    let (url, _c) = setup_test_database_url().await;
    let app = build_dag957_ui_app(&url, true, vec![]);

    let ia = autumn_harvest::ActivityExecId::new();
    // A genuinely-valid run of this DAG exists — the unknown `?run=` must NOT be
    // silently substituted for it.
    let valid = dag957_seed_run(
        &url,
        "dag957_linear",
        "graph-fallback",
        vec![
            dag957_sched("dag957_step_a", ia),
            dag957_started(ia),
            dag957_completed(ia),
        ],
        "RUNNING",
    )
    .await;

    // Issue #957 AC7: an explicitly-provided-but-unknown run id renders the 404
    // message, never a silent fallback to a different run.
    let bogus = ExecutionId::new_for_shard(ShardId::new(0));
    let (status, html) = fetch_html(&app, &format!("/dags/dag957_linear?run={bogus}")).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "an unknown present `?run=` renders the 404 message: {html}"
    );
    // The 404 body must not silently render the valid run's graph in its place.
    assert!(
        !html.contains("<svg"),
        "no substitute graph is rendered for an unknown run: {html}"
    );
    assert!(
        !html.contains(&valid.to_string()),
        "the valid run is not silently substituted: {html}"
    );

    // Sanity: the same DAG page WITHOUT `?run=` still defaults to the latest run
    // and renders normally (omitted-run behavior is preserved).
    let (default_status, default_html) = fetch_html(&app, "/dags/dag957_linear").await;
    assert_eq!(
        default_status,
        StatusCode::OK,
        "omitted `?run=` still renders: {default_html}"
    );
    assert!(
        default_html.contains("dag957_step_a"),
        "omitted `?run=` defaults to the latest run's graph: {default_html}"
    );
}

// I-J — a real `skipped` node (via the #482 `dag_skip:` marker path, exactly as
// `build_run_graph`/#690 read it) renders distinctly from a `pending` node end-
// to-end, exercising #957 AC6 ("pending vs skipped visually distinct") through
// the whole `build_run_graph` → SVG pipeline rather than the helpers in isolation.
#[tokio::test]
async fn ui_dag_run_graph_skipped_node_distinct_from_pending() {
    let (url, _c) = setup_test_database_url().await;
    let app = build_dag957_ui_app(&url, true, vec![]);

    // Fan-out: a -> {b, c} -> d. a and b succeed; c is condition-skipped (#482);
    // d is never reached (RUNNING) → pending. So c renders `skipped`, d `pending`.
    let (ia, ib) = (
        autumn_harvest::ActivityExecId::new(),
        autumn_harvest::ActivityExecId::new(),
    );
    let events = vec![
        dag957_sched("dag957_step_a", ia),
        dag957_started(ia),
        dag957_completed(ia),
        dag957_sched("dag957_step_b", ib),
        dag957_started(ib),
        dag957_completed(ib),
        // A #482 data-dependent skip of node index 2 (dag957_step_c, upstream a=0).
        autumn_harvest::WorkflowEvent::MarkerRecorded {
            name: "dag_skip:2".to_string(),
            details: json!({ "task": "dag957_step_c", "upstreams": [0] }),
        },
    ];
    let exec_id = dag957_seed_run(&url, "dag957_fanout", "graph-skip", events, "RUNNING").await;

    let (status, html) = fetch_html(&app, &format!("/dags/dag957_fanout?run={exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "body: {html}");

    // The node rect fill attribute is node-specific (the legend swatch uses a
    // `style="background:…"` attribute, never `fill="…"`), so these prove the
    // node itself carries the Skipped vs Pending status — distinct fills.
    assert!(
        html.contains("fill=\"#475569\""),
        "the skipped node renders with the Skipped fill: {html}"
    );
    assert!(
        html.contains("fill=\"#334155\""),
        "the pending node renders with the (distinct) Pending fill: {html}"
    );
    // Icon-per-status carries the accessible, colour-independent cue on the node
    // label text: `↳ dag957_step_c` (Skipped) vs `○ dag957_step_d` (Pending).
    assert!(
        html.contains("↳ dag957_step_c"),
        "skipped node label carries the Skipped icon+name: {html}"
    );
    assert!(
        html.contains("○ dag957_step_d"),
        "pending node label carries the (distinct) Pending icon+name: {html}"
    );
}

// I-K — index-alignment invariant (security review nit-1). `build_run_graph`
// returns nodes in `def.tasks()` order, so `nodes[i]` ↔ `tasks()[i]` ↔ the index
// used by `execution_levels()` (columns) and the SVG's `?node=i` anchors. A
// future reorder in `build_run_graph` or the DAG topology would silently mis-map
// a node's status/edges onto the wrong box; this pins the alignment the whole
// render depends on. Pure — no DB (uses `DagBuilder` + `build_run_graph`, which
// this integration test file already links).
#[test]
fn dag957_build_run_graph_nodes_align_with_task_indices() {
    use autumn_harvest_plugin::dag_graph::build_run_graph;

    // Fan-out: a -> {b, c} -> d — distinct upstream sets per node so a swap
    // would be caught.
    let mut builder = DagBuilder::new();
    let a = builder.activity(dag957_step_a);
    let b = builder.activity(dag957_step_b).upstream(&a);
    let c = builder.activity(dag957_step_c).upstream(&a);
    let _d = builder.activity(dag957_step_d).upstream(&b).upstream(&c);
    let def = builder.build().expect("fanout dag builds");

    let nodes = build_run_graph(&def, &[], "RUNNING");

    assert_eq!(nodes.len(), def.tasks().len(), "exactly one node per task");
    for (i, task) in def.tasks().iter().enumerate() {
        assert_eq!(
            nodes[i].node_name, task.activity_name,
            "node[{i}] name aligns with tasks()[{i}]"
        );
        // `depends_on` names must resolve to this task's upstream indices —
        // proving the drawn edge set corresponds to the same task position.
        let mut expected: Vec<String> = task
            .upstreams
            .iter()
            .map(|&u| def.tasks()[u].activity_name.clone())
            .collect();
        let mut actual = nodes[i].depends_on.clone();
        expected.sort();
        actual.sort();
        assert_eq!(
            actual, expected,
            "node[{i}] depends_on aligns with its upstream task names"
        );
    }
    // Every execution-level task index is a valid node index, covering all
    // nodes exactly once (columns map to real nodes).
    let mut covered: Vec<usize> = def
        .execution_levels()
        .iter()
        .flat_map(|level| level.iter().copied())
        .collect();
    covered.sort_unstable();
    assert_eq!(
        covered,
        (0..nodes.len()).collect::<Vec<_>>(),
        "execution levels cover every node index exactly once"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Issue #960 — Vantage execution timeline / Gantt view
//
// These exercise the standalone `GET /workflows/{id}/timeline` page (server-
// rendered inline SVG built from `autumn_harvest::derive_timeline`, the same
// in-process call the #739 API handler makes) plus the pause/ND-block bands
// sourced from the execution row and the "Timeline" tab link on the detail
// page. Docker-backed (testcontainers); compile-checked in sandboxes without
// Docker, run on Linux CI.
// ─────────────────────────────────────────────────────────────────────────────

/// Set an event row's wall-clock timestamp so the derived timeline has
/// meaningful, spread-out durations (events default to `NOW()` on insert).
async fn tl960_set_event_ts(
    database_url: &str,
    exec_id: ExecutionId,
    event_id: i32,
    ts: chrono::DateTime<chrono::Utc>,
) {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("connect for event-ts update");
    let sql = format!(
        "UPDATE harvest_events SET timestamp = '{}' WHERE workflow_exec_id = '{}' AND event_id = {}",
        ts.to_rfc3339(),
        exec_id.as_uuid(),
        event_id,
    );
    conn.batch_execute(&sql)
        .await
        .expect("update event timestamp");
}

/// Seed a plain workflow run with the given events (appended after the
/// `WorkflowStarted` at event 1), spread across wall-clock time starting at
/// `base`, then apply the state/column overrides. Returns the exec id and the
/// number of appended events.
#[allow(clippy::too_many_arguments)]
async fn tl960_seed_run(
    database_url: &str,
    workflow_id: &str,
    events: Vec<autumn_harvest::WorkflowEvent>,
    base: chrono::DateTime<chrono::Utc>,
    step_ms: i64,
    state: &str,
    completed_at: Option<chrono::DateTime<chrono::Utc>>,
    paused_at: Option<chrono::DateTime<chrono::Utc>>,
    pause_reason: Option<&str>,
    pause_actor: Option<&str>,
    nd_blocked_at: Option<chrono::DateTime<chrono::Utc>>,
    nd_block_reason: Option<&str>,
    current_details: Option<&str>,
) -> ExecutionId {
    let exec_id =
        insert_workflow_on_url(database_url, ShardId::new(0), "echo_workflow", workflow_id).await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("connect for tl960 seed");
    let history = store::load_history(&mut conn, exec_id).await.unwrap();
    let start_event_id = history.next_event_id;
    let count = i32::try_from(events.len()).unwrap();
    store::append_events(&mut conn, exec_id, &events, start_event_id)
        .await
        .expect("append tl960 events");

    // Anchor the WorkflowStarted (event 1) to `base` and spread the rest.
    tl960_set_event_ts(database_url, exec_id, 1, base).await;
    for offset in 0..count {
        let ts = base + chrono::Duration::milliseconds((i64::from(offset) + 1) * step_ms);
        tl960_set_event_ts(database_url, exec_id, start_event_id + offset, ts).await;
    }

    // Apply the row overrides (state, completion, pause/ND-block, current_details).
    let mut sets: Vec<String> = vec![format!("state = '{state}'")];
    sets.push(format!("started_at = '{}'", base.to_rfc3339()));
    if let Some(c) = completed_at {
        sets.push(format!("completed_at = '{}'", c.to_rfc3339()));
    }
    if let Some(p) = paused_at {
        sets.push(format!("paused_at = '{}'", p.to_rfc3339()));
    }
    if let Some(r) = pause_reason {
        sets.push(format!("pause_reason = '{r}'"));
    }
    if let Some(a) = pause_actor {
        sets.push(format!("pause_actor = '{a}'"));
    }
    if let Some(nd) = nd_blocked_at {
        sets.push(format!("nd_blocked_at = '{}'", nd.to_rfc3339()));
    }
    if let Some(r) = nd_block_reason {
        sets.push(format!("nd_block_reason = '{r}'"));
    }
    if let Some(cd) = current_details {
        sets.push(format!("current_details = '{cd}'"));
    }
    let sql = format!(
        "UPDATE harvest_workflow_executions SET {} WHERE id = '{}'",
        sets.join(", "),
        exec_id.as_uuid(),
    );
    conn.batch_execute(&sql)
        .await
        .expect("apply tl960 row overrides");
    exec_id
}

fn tl960_act_sched(
    name: &str,
    id: autumn_harvest::ActivityExecId,
) -> autumn_harvest::WorkflowEvent {
    autumn_harvest::WorkflowEvent::ActivityScheduled {
        activity_id: id,
        name: name.to_string(),
        input: json!({}),
        queue: "default".to_string(),
    }
}
fn tl960_act_started(id: autumn_harvest::ActivityExecId) -> autumn_harvest::WorkflowEvent {
    autumn_harvest::WorkflowEvent::ActivityStarted {
        activity_id: id,
        worker_id: autumn_harvest::types::WorkerId::new("tl-worker"),
    }
}
const fn tl960_act_completed(id: autumn_harvest::ActivityExecId) -> autumn_harvest::WorkflowEvent {
    autumn_harvest::WorkflowEvent::ActivityCompleted {
        activity_id: id,
        output: Value::Null,
    }
}
fn tl960_act_failed(
    id: autumn_harvest::ActivityExecId,
    attempt: u32,
) -> autumn_harvest::WorkflowEvent {
    autumn_harvest::WorkflowEvent::ActivityFailed {
        activity_id: id,
        error: "transient failure".to_string(),
        attempt,
        error_type: "Transient".to_string(),
        non_retryable: false,
        details: None,
    }
}
fn tl960_timer_started(id: &str) -> autumn_harvest::WorkflowEvent {
    autumn_harvest::WorkflowEvent::TimerStarted {
        timer_id: autumn_harvest::types::TimerId::new(id),
        duration_secs: 60,
    }
}
fn tl960_timer_fired(id: &str) -> autumn_harvest::WorkflowEvent {
    autumn_harvest::WorkflowEvent::TimerFired {
        timer_id: autumn_harvest::types::TimerId::new(id),
    }
}
fn tl960_child_started(child: ExecutionId, name: &str) -> autumn_harvest::WorkflowEvent {
    autumn_harvest::WorkflowEvent::ChildWorkflowStarted {
        child_id: child,
        workflow_name: name.to_string(),
        input: json!({}),
    }
}
const fn tl960_child_completed(child: ExecutionId) -> autumn_harvest::WorkflowEvent {
    autumn_harvest::WorkflowEvent::ChildWorkflowCompleted {
        child_id: child,
        output: Value::Null,
    }
}

// J-A
#[tokio::test]
async fn ui_timeline_renders_activity_timer_pause_ndblock() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);
    let base = chrono::DateTime::parse_from_rfc3339("2026-07-15T10:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);

    // Activity retried twice (attempt 1 and 2 fail, then attempt 3 completes),
    // so `derive_timeline` reports attempt = 2 → a "×2" badge. Plus a durable timer.
    let a = autumn_harvest::ActivityExecId::new();
    let t_id = "wait_gate";
    let events = vec![
        tl960_act_sched("charge_card", a),
        tl960_act_started(a),
        tl960_act_failed(a, 1),
        tl960_act_started(a),
        tl960_act_failed(a, 2),
        tl960_act_started(a),
        tl960_act_completed(a),
        tl960_timer_started(t_id),
        tl960_timer_fired(t_id),
    ];
    let exec_id = tl960_seed_run(
        &database_url,
        "tl-a",
        events,
        base,
        1000,
        "RUNNING",
        None,
        Some(base + chrono::Duration::seconds(4)),
        Some("incident-4821"),
        Some("oncall@corp"),
        Some(base + chrono::Duration::seconds(6)),
        Some("expected ActivityScheduled got TimerStarted"),
        None,
    )
    .await;

    let (status, html) = fetch_html(&app, &format!("/workflows/{exec_id}/timeline")).await;
    assert_eq!(status, StatusCode::OK, "timeline renders: {html}");
    assert!(html.contains("<svg"), "inline svg present");
    assert!(html.contains("charge_card"), "activity span present");
    assert!(html.contains("×2"), "retry attempt badge visible: {html}");
    assert!(html.contains("wait_gate"), "timer span present");
    assert!(html.contains("Timer"), "timer lane label present");
    // Pause band + reason/actor.
    assert!(html.contains("gantt-pause-band"), "pause band present");
    assert!(html.contains("incident-4821"), "pause reason labelled");
    assert!(html.contains("oncall@corp"), "pause actor labelled");
    // ND marker + runbook path (surfaced as code/text, not a clickable link —
    // the runbook is a repo path, not a served URL).
    assert!(html.contains("gantt-nd-marker"), "ND marker present");
    assert!(
        html.contains("docs/runbooks/nondeterminism-block.md"),
        "runbook path surfaced: {html}"
    );
    assert!(
        !html.contains("href=\"docs/runbooks/nondeterminism-block.md\""),
        "runbook path is not a dead relative link: {html}"
    );
}

// J-B
#[tokio::test]
async fn ui_timeline_split_only_when_present() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);
    let base = chrono::DateTime::parse_from_rfc3339("2026-07-15T11:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);

    // An activity WITH ActivityStarted → wait/exec split. A child workflow → no split.
    let a = autumn_harvest::ActivityExecId::new();
    let child = ExecutionId::new_for_shard(ShardId::new(0));
    let events = vec![
        tl960_act_sched("split_activity", a),
        tl960_act_started(a),
        tl960_act_completed(a),
        tl960_child_started(child, "sub_flow"),
        tl960_child_completed(child),
    ];
    let exec_id = tl960_seed_run(
        &database_url,
        "tl-b",
        events,
        base,
        1000,
        "COMPLETED",
        Some(base + chrono::Duration::seconds(6)),
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .await;

    let (status, html) = fetch_html(&app, &format!("/workflows/{exec_id}/timeline")).await;
    assert_eq!(status, StatusCode::OK);
    // Started activity → wait + exec segments.
    assert!(
        html.contains("gantt-seg-wait"),
        "wait segment present: {html}"
    );
    assert!(html.contains("gantt-seg-exec"), "exec segment present");
    // Child → single undivided span.
    assert!(
        html.contains("gantt-seg-whole"),
        "child renders one whole span"
    );
    assert!(
        html.contains("sub_flow")
            || html.contains("ChildWorkflow")
            || html.contains("Child workflow")
    );
}

// J-C
#[tokio::test]
async fn ui_timeline_slowest_highlight_and_rollup() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);
    let base = chrono::DateTime::parse_from_rfc3339("2026-07-15T12:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);

    // A quick activity then a much slower one (the slowest step).
    let a1 = autumn_harvest::ActivityExecId::new();
    let a2 = autumn_harvest::ActivityExecId::new();
    let events = vec![
        tl960_act_sched("quick", a1),
        tl960_act_completed(a1),
        tl960_act_sched("slow_bottleneck", a2),
        tl960_act_completed(a2),
    ];
    // event offsets (step_ms=1000): sched(quick)@1s, complete(quick)@2s,
    // sched(slow)@3s, complete(slow)@4s → but that makes both 1s. Use explicit
    // widening below by pushing the slow completion far out via a large step.
    let exec_id = tl960_seed_run(
        &database_url,
        "tl-c",
        events,
        base,
        3000,
        "COMPLETED",
        Some(base + chrono::Duration::seconds(20)),
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .await;
    // Push the slow activity's completion far out so it dominates.
    tl960_set_event_ts(
        &database_url,
        exec_id,
        5,
        base + chrono::Duration::seconds(20),
    )
    .await;

    let (status, html) = fetch_html(&app, &format!("/workflows/{exec_id}/timeline")).await;
    assert_eq!(status, StatusCode::OK);
    // Rollup header present.
    assert!(
        html.contains("Total") || html.contains("wall-clock") || html.contains("Wall-clock"),
        "rollup header: {html}"
    );
    assert!(html.contains("slow_bottleneck"), "slowest step named");
    // Slowest span anchor + highlight.
    assert!(html.contains("id=\"slowest\""), "slowest anchor present");
    assert!(
        html.contains("gantt-span-slowest"),
        "slowest highlight present"
    );
}

// J-D
#[tokio::test]
async fn ui_timeline_inflight_open_span_and_current_details() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);
    let base = chrono::DateTime::parse_from_rfc3339("2026-07-15T13:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);

    // A running execution with a scheduled-but-not-completed activity.
    let a = autumn_harvest::ActivityExecId::new();
    let events = vec![
        tl960_act_sched("in_flight_activity", a),
        tl960_act_started(a),
    ];
    let exec_id = tl960_seed_run(
        &database_url,
        "tl-d",
        events,
        base,
        1000,
        "RUNNING",
        None,
        None,
        None,
        None,
        None,
        None,
        Some("step 2/3: awaiting downstream"),
    )
    .await;

    let (status, html) = fetch_html(&app, &format!("/workflows/{exec_id}/timeline")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("gantt-span-open"),
        "in-flight open span present: {html}"
    );
    assert!(
        html.contains("step 2/3: awaiting downstream"),
        "current_details shown in header"
    );
}

// J-E
#[tokio::test]
async fn ui_timeline_unknown_execution_404() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);
    // A random exec id has no execution row (mirrors a classic DAG run, which is
    // not on the execution path) → 404.
    let bogus = ExecutionId::new_for_shard(ShardId::new(0));
    let (status, _html) = fetch_html(&app, &format!("/workflows/{bogus}/timeline")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "unknown execution → 404");
}

// J-F
#[tokio::test]
async fn ui_detail_page_has_timeline_link() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);
    let base = chrono::DateTime::parse_from_rfc3339("2026-07-15T14:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let a = autumn_harvest::ActivityExecId::new();
    let exec_id = tl960_seed_run(
        &database_url,
        "tl-f",
        vec![tl960_act_sched("a", a), tl960_act_completed(a)],
        base,
        1000,
        "COMPLETED",
        Some(base + chrono::Duration::seconds(3)),
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .await;

    let (status, html) = fetch_html(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("/timeline") && html.to_lowercase().contains("timeline"),
        "detail page links to the timeline view: {html}"
    );
}

// J-G (perf)
#[tokio::test]
async fn ui_timeline_200_steps_under_1s() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);
    let base = chrono::DateTime::parse_from_rfc3339("2026-07-15T15:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);

    // 200 activities (400 events: sched + complete each).
    let mut events = Vec::with_capacity(400);
    for _ in 0..200 {
        let id = autumn_harvest::ActivityExecId::new();
        events.push(tl960_act_sched("step", id));
        events.push(tl960_act_completed(id));
    }
    let exec_id = tl960_seed_run(
        &database_url,
        "tl-g",
        events,
        base,
        100,
        "COMPLETED",
        Some(base + chrono::Duration::seconds(60)),
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .await;

    let start = std::time::Instant::now();
    let (status, html) = fetch_html(&app, &format!("/workflows/{exec_id}/timeline")).await;
    let elapsed = start.elapsed();
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("<svg"), "renders the gantt");
    assert!(
        elapsed < Duration::from_secs(1),
        "200-step timeline renders server-side in < 1s (took {elapsed:?})"
    );
}
