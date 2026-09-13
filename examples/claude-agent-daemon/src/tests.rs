//! Offline tests for the agent daemon.
//!
//! Every test runs the scripted stub model, so the suite needs no API key and
//! no network. The stub drives the same loop the live model does: one tool
//! call, one approval-gated write, then a final answer.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use autumn_harvest_sqlite::{ExecutionId, RunState, SqliteRuntime};
use serde_json::{Value, json};

use crate::claude;
use crate::daemon;
use crate::guard;
use crate::inspect;
use crate::protocol::{self, Request, Response};
use crate::session::{
    self, ApprovalDecision, SIGNAL_TOOL_APPROVAL, SessionReport, SessionTask, ToolCall,
    ToolOutcome, ToolRequest, TurnReply, TurnRequest, WORKFLOW_NAME,
};
use crate::tools;

/// The resolved workspace identity a session records.
fn workspace_id(workspace: &Path) -> String {
    workspace
        .canonicalize()
        .expect("the workspace resolves")
        .to_string_lossy()
        .into_owned()
}

/// The task every test submits. The stub model is what the tests register.
fn task(workspace: &Path) -> Value {
    task_on(workspace, claude::OFFLINE_MODEL)
}

/// A task recorded against a specific model identity.
fn task_on(workspace: &Path, model: &str) -> Value {
    serde_json::to_value(SessionTask {
        goal: "summarise the workspace".to_string(),
        max_turns: 6,
        approval_timeout_secs: 300,
        workspace: workspace_id(workspace),
        model: model.to_string(),
    })
    .expect("the task encodes")
}

/// One `run_tool` activity input, as the workflow would build it.
fn tool_request(workspace: &Path, tool: &str, input: Value) -> Value {
    serde_json::to_value(ToolRequest {
        workspace: workspace_id(workspace),
        call: ToolCall {
            id: "toolu_test".to_string(),
            name: tool.to_string(),
            input,
        },
    })
    .expect("the call encodes")
}

/// A stub model body that counts its calls.
///
/// It stands in for `claude::activity_body` with no API key, so it answers to
/// the same recorded identity.
fn counting_model(
    calls: Arc<AtomicUsize>,
) -> impl Fn(Value) -> Result<Value, String> + Send + Sync + 'static {
    move |input| {
        let request: TurnRequest =
            serde_json::from_value(input).map_err(|e| format!("bad request: {e}"))?;
        assert_eq!(
            request.model,
            claude::OFFLINE_MODEL,
            "a turn must carry the identity its session recorded"
        );
        calls.fetch_add(1, Ordering::SeqCst);
        serde_json::to_value(claude::offline::reply(&request))
            .map_err(|e| format!("bad reply: {e}"))
    }
}

/// Open a runtime with both activity bodies registered.
fn runtime(db: &Path, workspace: &Path, calls: &Arc<AtomicUsize>) -> SqliteRuntime {
    let mut rt = SqliteRuntime::open(db).expect("the database opens");
    rt.register_workflow(&session::agent_session_info());
    rt.register_activity(&session::claude_turn_info(), counting_model(calls.clone()));
    rt.register_activity(
        &session::run_tool_info(),
        tools::activity_body(workspace.to_path_buf()),
    );
    rt
}

/// Deliver an approval decision to the signal a session is waiting on.
fn approve(rt: &mut SqliteRuntime, exec: ExecutionId, signal: &str) {
    let payload = serde_json::to_value(ApprovalDecision {
        approved: true,
        note: None,
    })
    .expect("the decision encodes");
    rt.send_signal(exec, signal, payload)
        .expect("the signal is staged");
}

/// Wait for a daemon to answer on its socket.
///
/// The path existing is not enough. A dead daemon leaves its socket file
/// behind on purpose, so the test has to wait for an answer rather than for a
/// name.
async fn await_daemon(socket: &Path) {
    for _ in 0..200 {
        if let Ok(Response::Sessions { .. }) =
            protocol::call(socket, &Request::List { before: None }).await
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the daemon never answered on its socket");
}

/// Wait for a session to park on its approval.
async fn await_parked(socket: &Path, execution_id: &str) {
    for _ in 0..200 {
        let answer = protocol::call(
            socket,
            &Request::Status {
                execution_id: execution_id.to_string(),
                full: false,
            },
        )
        .await
        .expect("the status is answered");
        let Response::Session { session } = answer else {
            panic!("unexpected answer: {answer:?}");
        };
        if session.blocked_on.is_some() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the session never parked");
}

/// Drive to the next stop and return the approval signal the run waits on.
async fn drive_to_approval(rt: &mut SqliteRuntime, exec: ExecutionId) -> String {
    let state = rt.run_until_blocked(exec).await.expect("the run advances");
    let RunState::WaitingSignal(signal) = state else {
        panic!("expected an approval wait, got {state:?}");
    };
    assert!(
        session::approval_call_id(&signal).is_some(),
        "the wait must name the tool call it releases: {signal}"
    );
    signal
}

#[tokio::test]
async fn a_session_runs_its_tools_and_finishes_after_approval() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");
    std::fs::write(workspace.join("README.md"), "hello").expect("the fixture is written");

    let calls = Arc::new(AtomicUsize::new(0));
    let mut rt = runtime(&dir.path().join("agentd.db"), &workspace, &calls);
    let exec = rt
        .start_workflow(WORKFLOW_NAME, task(&workspace))
        .expect("the session starts");

    // Turn one lists the workspace. Turn two proposes a write, which parks the
    // run on that call's own approval signal.
    let signal = drive_to_approval(&mut rt, exec).await;
    assert!(
        !workspace.join("agent-notes.md").exists(),
        "the gated write must not run before approval"
    );

    approve(&mut rt, exec, &signal);
    let state = rt.run_until_blocked(exec).await.expect("the run finishes");
    assert!(
        matches!(state, RunState::Completed(_)),
        "expected completion, got {state:?}"
    );
    assert!(
        workspace.join("agent-notes.md").exists(),
        "the approved write must run"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 3, "three model calls");
}

/// A transcript that no longer fits ENDS the session, and does not fail it.
///
/// The transcript rides in the activity input, so one turn can reach the cap
/// on its own. A reply asking for 33 `read_file` calls, each returning the
/// 64 KiB a read may return, builds a request of about 2.1 MB. The cap is
/// 2 MiB. Measured at 33 calls: 2164819 bytes.
///
/// The engine answers `PayloadTooLarge`, which is not retryable. The session
/// FAILED at the top of the next turn, after that turn's model call was
/// billed and after its approved writes had already run. The work was done,
/// paid for, and then thrown away with an error naming none of it.
///
/// It now ends under its own stop reason, before the next model call.
#[tokio::test]
async fn a_transcript_too_large_to_send_ends_the_session() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");

    // Enough files at the read cap that ONE turn of reads passes the cap.
    let files: usize = 35;
    for index in 0..files {
        std::fs::write(
            workspace.join(format!("big-{index}.txt")),
            "x".repeat(64 * 1024),
        )
        .expect("the fixture is written");
    }

    // A model that asks for every file in its first reply, and would keep
    // asking afterwards. The session must stop before it is asked again.
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let model = move |input: Value| -> Result<Value, String> {
        let request: TurnRequest =
            serde_json::from_value(input).map_err(|e| format!("bad request: {e}"))?;
        counted.fetch_add(1, Ordering::SeqCst);
        let reads: Vec<Value> = (0..files)
            .map(|index| {
                json!({
                    "type": "tool_use",
                    "id": format!("toolu_read_{index}"),
                    "name": tools::TOOL_READ_FILE,
                    "input": { "path": format!("big-{index}.txt") },
                })
            })
            .collect();
        let mut content = vec![json!({ "type": "text", "text": "reading everything" })];
        content.extend(reads);
        let _ = &request;
        serde_json::to_value(session::TurnReply {
            content: Value::Array(content),
            stop_reason: claude::STOP_TOOL_USE.to_string(),
            text: "reading everything".to_string(),
            tool_calls: (0..files)
                .map(|index| session::ToolCall {
                    id: format!("toolu_read_{index}"),
                    name: tools::TOOL_READ_FILE.to_string(),
                    input: json!({ "path": format!("big-{index}.txt") }),
                })
                .collect(),
        })
        .map_err(|e| format!("bad reply: {e}"))
    };

    let mut rt = SqliteRuntime::open(dir.path().join("agentd.db")).expect("the database opens");
    rt.register_workflow(&session::agent_session_info());
    rt.register_activity(&session::claude_turn_info(), model);
    rt.register_activity(
        &session::run_tool_info(),
        tools::activity_body(workspace.clone()),
    );
    let exec = rt
        .start_workflow(WORKFLOW_NAME, task(&workspace))
        .expect("the session starts");

    let state = rt.run_until_blocked(exec).await.expect("the run advances");

    // COMPLETED, and not FAILED. That distinction is the whole fix: a
    // non-retryable failure here would discard a turn already paid for.
    let RunState::Completed(output) = state else {
        panic!("the session must end rather than fail: {state:?}");
    };
    let report: session::SessionReport = serde_json::from_value(output).expect("the report reads");
    assert_eq!(
        report.stop,
        session::STOP_TRANSCRIPT_FULL,
        "and it must end under its own name: {report:?}"
    );

    // The work of the turn that DID run is kept and counted.
    assert_eq!(
        u64::from(report.tool_calls),
        files as u64,
        "every read of the turn that ran is counted"
    );
    // And ONLY the turns that ran. The guard fires before the model call of
    // turn two, so one turn happened. A report of two would count a turn
    // nobody made and nobody paid for. That report is what `list` and
    // `status` show for the rest of the session's life.
    assert_eq!(
        report.turns, 1,
        "the report must count the turns that ran, not the one refused"
    );
    assert_eq!(
        report.answer, "reading everything",
        "and the text of that turn is the answer"
    );

    // The model was asked ONCE. The turn that could not be sent was never
    // billed, which is what checking before the activity buys.
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the turn that cannot be sent must cost no model call"
    );
}

#[tokio::test]
async fn a_denied_call_is_reported_to_the_model_and_the_session_continues() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");

    let calls = Arc::new(AtomicUsize::new(0));
    let mut rt = runtime(&dir.path().join("agentd.db"), &workspace, &calls);
    let exec = rt
        .start_workflow(WORKFLOW_NAME, task(&workspace))
        .expect("the session starts");
    let signal = drive_to_approval(&mut rt, exec).await;

    let payload = serde_json::to_value(ApprovalDecision {
        approved: false,
        note: Some("not this file".to_string()),
    })
    .expect("the decision encodes");
    rt.send_signal(exec, &signal, payload)
        .expect("the signal is staged");

    let state = rt.run_until_blocked(exec).await.expect("the run finishes");
    let RunState::Completed(output) = state else {
        panic!("expected completion, got {state:?}");
    };
    assert!(
        !workspace.join("agent-notes.md").exists(),
        "a denied write must never run"
    );

    // The session must say what happened. The stub proposed the write, the
    // operator denied it, and there is no file. An answer that reported the
    // note as recorded would be a false success in the one demonstration that
    // needs no key.
    let report: SessionReport = serde_json::from_value(output).expect("the report decodes");
    assert!(
        report.answer.contains("NOT recorded"),
        "a denied write must be reported as such: {}",
        report.answer
    );
}

#[tokio::test]
async fn a_restart_resumes_the_session_without_repeating_model_calls() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("agentd.db");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");

    // Session one drives to the approval block, then the process "crashes".
    let first_calls = Arc::new(AtomicUsize::new(0));
    let (exec, signal) = {
        let mut rt = runtime(&db, &workspace, &first_calls);
        let exec = rt
            .start_workflow(WORKFLOW_NAME, task(&workspace))
            .expect("the session starts");
        let signal = drive_to_approval(&mut rt, exec).await;
        (exec, signal)
    };
    assert_eq!(first_calls.load(Ordering::SeqCst), 2, "two model calls");

    // Session two reopens the same file. The recorded turns replay, so only the
    // turn after the approval reaches the model.
    let second_calls = Arc::new(AtomicUsize::new(0));
    let mut rt = runtime(&db, &workspace, &second_calls);
    approve(&mut rt, exec, &signal);
    let state = rt.run_until_blocked(exec).await.expect("the run finishes");

    assert!(
        matches!(state, RunState::Completed(_)),
        "expected completion, got {state:?}"
    );
    assert_eq!(
        second_calls.load(Ordering::SeqCst),
        1,
        "the replayed turns must not call the model again"
    );
}

#[test]
fn the_toolbox_refuses_a_path_outside_the_workspace() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let body = tools::activity_body(dir.path().to_path_buf());
    let raw = body(tool_request(
        dir.path(),
        tools::TOOL_READ_FILE,
        json!({ "path": "../../etc/passwd" }),
    ))
    .expect("a tool failure is a result, not an activity error");
    let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");
    assert!(outcome.is_error, "the escape must fail");
    assert!(
        outcome.output.contains("leaves the workspace"),
        "unexpected message: {}",
        outcome.output
    );
}

/// Poll one session over the socket until it leaves `RUNNING`, approving the
/// call it shows. Returns the terminal state it reached.
///
/// The decision names the call the status printed, and a decision naming
/// another call is asserted to be refused.
async fn settle_over_socket(socket: &Path, execution_id: &str) -> String {
    let mut approved = false;
    for _ in 0..200 {
        let answer = protocol::call(
            socket,
            &Request::Status {
                execution_id: execution_id.to_string(),
                full: false,
            },
        )
        .await
        .expect("the status is answered");
        let Response::Session { session } = answer else {
            panic!("unexpected answer: {answer:?}");
        };
        if session.state != "RUNNING" {
            assert!(approved, "the session never asked for approval");
            return session.state;
        }

        if !approved && session.blocked_on.is_some() {
            // An operator approves an action, not a session, so the exact call
            // must be visible before the decision.
            let pending = session.pending.as_ref().expect("the pending call is shown");
            assert_eq!(pending.tool, tools::TOOL_WRITE_FILE);
            assert!(
                pending.input.contains("agent-notes.md"),
                "the status must show what the write does: {}",
                pending.input
            );

            // A decision that does not name the wait it saw is refused. The
            // bare tool-use id is exactly what must NOT be enough: the model
            // can reuse one, so an older status would release a later call.
            let stale = protocol::call(
                socket,
                &Request::Approve {
                    execution_id: execution_id.to_string(),
                    token: pending.id.clone(),
                    approved: true,
                    note: None,
                },
            )
            .await
            .expect("the daemon answers");
            assert!(
                matches!(stale, Response::Stale { .. }),
                "a decision for another call must be refused: {stale:?}"
            );
            // The refusal names a follow-up command, so the CLIENT renders it
            // against the socket this command reached. A command built in the
            // daemon would send the operator to `agentd.sock`.
            let told = crate::rendered_lines(&stale, Path::new("/run/agentd/project-b.sock"));
            assert!(
                told[0].contains("--socket=/run/agentd/project-b.sock")
                    && told[0].contains(execution_id),
                "the recovery command must reach the same daemon: {}",
                told[0]
            );

            protocol::call(
                socket,
                &Request::Approve {
                    execution_id: execution_id.to_string(),
                    token: pending.token.clone(),
                    approved: true,
                    note: None,
                },
            )
            .await
            .expect("the approval is answered");
            approved = true;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("the session never reached a terminal state");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_daemon_resumes_a_session_the_first_one_left_running() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    // One database, one socket each. Both daemons run in THIS process, and the
    // accept loop of an aborted one keeps its listener, which a killed process
    // would not. The socket reclaim has its own test; this one is about the
    // session surviving in the file.
    let first_socket = dir.path().join("first.sock");
    let second_socket = dir.path().join("second.sock");
    let options = |socket: &Path| daemon::Options {
        db: dir.path().join("agentd.db"),
        socket: socket.to_path_buf(),
        workspace: dir.path().join("workspace"),
        model: claude::DEFAULT_MODEL.to_string(),
        max_tokens: claude::DEFAULT_MAX_TOKENS,
        tick: Duration::from_millis(50),
        api_key: None,
    };

    let first = tokio::spawn(daemon::serve(options(&first_socket)));
    await_daemon(&first_socket).await;
    let submitted = protocol::call(
        &first_socket,
        &Request::Submit {
            goal: "summarise the workspace".to_string(),
            max_turns: 6,
            approval_timeout_secs: 300,
        },
    )
    .await
    .expect("the submit is answered");
    let Response::Submitted { execution_id } = submitted else {
        panic!("unexpected answer: {submitted:?}");
    };
    await_parked(&first_socket, &execution_id).await;

    // The process dies with the session parked. Aborting the task drops the
    // listener and the database lock, which is what a kill does.
    first.abort();
    drop(first.await);

    // The second daemon holds no memory of the session. It has to find the
    // session in the file, or nothing advances it again: this daemon is the
    // only writer.
    let second = tokio::spawn(daemon::serve(options(&second_socket)));
    await_daemon(&second_socket).await;
    let state = settle_over_socket(&second_socket, &execution_id).await;
    assert_eq!(
        state, "COMPLETED",
        "the resumed session did not finish under the second daemon"
    );
    second.abort();
}

#[tokio::test]
async fn a_control_connection_is_identified_by_its_peer() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let socket = dir.path().join("probe.sock");
    let listener = tokio::net::UnixListener::bind(&socket).expect("the socket binds");

    let caller = tokio::spawn(async move {
        tokio::net::UnixStream::connect(&socket)
            .await
            .expect("the caller connects")
    });
    let (served, _) = listener.accept().await.expect("the daemon accepts");
    drop(caller.await.expect("the caller finishes"));

    // The kernel reports the caller's user, and no directory mode can forge
    // it. This is what the daemon checks, because a socket's own mode is
    // enforced on `connect` by Linux and not by macOS.
    let owner = rustix::process::geteuid().as_raw();
    assert!(
        daemon::peer_is_owner(&served, owner),
        "a caller running as the daemon's own user must be served"
    );
    assert!(
        !daemon::peer_is_owner(&served, owner.wrapping_add(1)),
        "a caller running as anyone else must be refused"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_caller_that_sends_nothing_does_not_hold_the_daemon() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let socket = dir.path().join("agentd.sock");
    let options = daemon::Options {
        db: dir.path().join("agentd.db"),
        socket: socket.clone(),
        workspace: dir.path().join("workspace"),
        model: claude::DEFAULT_MODEL.to_string(),
        max_tokens: claude::DEFAULT_MAX_TOKENS,
        tick: Duration::from_millis(50),
        api_key: None,
    };
    let daemon = tokio::spawn(daemon::serve(options));
    await_daemon(&socket).await;

    // Connections that open and send no newline. Each one holds a permit until
    // its deadline, and there are more of them than the daemon holds at once.
    let mut silent = Vec::new();
    for _ in 0..40 {
        silent.push(
            tokio::net::UnixStream::connect(&socket)
                .await
                .expect("the caller connects"),
        );
    }

    // An honest caller is still answered. The silent ones are not holding the
    // daemon: a bounded read gives their permits back.
    let answered = tokio::time::timeout(
        Duration::from_secs(60),
        protocol::call(&socket, &Request::List { before: None }),
    )
    .await
    .expect("the honest caller must not wait on the silent ones")
    .expect("the list is answered");
    assert!(
        matches!(answered, Response::Sessions { .. }),
        "unexpected answer: {answered:?}"
    );

    drop(silent);
    daemon.abort();
}

#[tokio::test]
async fn a_decision_after_the_deadline_is_refused() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");
    let db = dir.path().join("agentd.db");

    let calls = Arc::new(AtomicUsize::new(0));
    let mut rt = runtime(&db, &workspace, &calls);
    // A deadline short enough to pass while the operator thinks.
    let task = serde_json::to_value(SessionTask {
        goal: "summarise the workspace".to_string(),
        max_turns: 6,
        approval_timeout_secs: 1,
        workspace: workspace_id(&workspace),
        model: claude::OFFLINE_MODEL.to_string(),
    })
    .expect("the task encodes");
    let exec = rt
        .start_workflow(WORKFLOW_NAME, task)
        .expect("the session starts");
    let signal = drive_to_approval(&mut rt, exec).await;

    let reader = crate::inspect::open(&db).expect("the inspector opens");
    let parked = |signal: &str| {
        let mut blocked = std::collections::HashMap::new();
        blocked.insert(
            exec,
            daemon::ParkedState {
                reason: "waiting for a tool approval".to_string(),
                signal: Some(signal.to_string()),
            },
        );
        blocked
    };

    // Before the deadline the decision is taken.
    let mut blocked = parked(&signal);
    let answer = daemon::approve(
        &mut rt,
        &reader,
        &mut blocked,
        &exec.to_string(),
        &signal,
        true,
        None,
    );
    assert!(
        matches!(answer, Response::Ack { .. }),
        "an on-time decision must be taken: {answer:?}"
    );

    // After it, the backend fires the expired timer BEFORE a late signal, so
    // the session denies the call however this answer reads. An "approved"
    // here would be a lie the operator finds only in the history.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let mut blocked = parked(&signal);
    let refused = daemon::approve(
        &mut rt,
        &reader,
        &mut blocked,
        &exec.to_string(),
        &signal,
        true,
        None,
    );
    let Response::Error { message } = refused else {
        panic!("a late decision must not be acknowledged, got {refused:?}");
    };
    assert!(
        message.contains("deadline"),
        "the refusal must say why: {message}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_daemon_answers_more_connections_than_it_holds_at_once() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let socket = dir.path().join("agentd.sock");
    let options = daemon::Options {
        db: dir.path().join("agentd.db"),
        socket: socket.clone(),
        workspace: dir.path().join("workspace"),
        model: claude::DEFAULT_MODEL.to_string(),
        max_tokens: claude::DEFAULT_MAX_TOKENS,
        tick: Duration::from_millis(50),
        api_key: None,
    };
    let daemon = tokio::spawn(daemon::serve(options));
    await_daemon(&socket).await;

    // More callers at once than the daemon holds connections for. The bound
    // stops a polling script from spending the daemon's descriptors. This test
    // proves the bound costs no answers. The kernel queues the callers that
    // wait on the listening socket.
    let callers = 80;
    let mut answers = Vec::with_capacity(callers);
    for _ in 0..callers {
        let socket = socket.clone();
        answers.push(tokio::spawn(async move {
            protocol::call(&socket, &Request::List { before: None }).await
        }));
    }

    for answer in answers {
        let answered = answer.await.expect("the caller finishes");
        assert!(
            matches!(answered, Ok(Response::Sessions { .. })),
            "every caller must be answered: {answered:?}"
        );
    }
    daemon.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_daemon_serves_one_session_over_its_socket() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let socket = dir.path().join("agentd.sock");
    let options = daemon::Options {
        db: dir.path().join("agentd.db"),
        socket: socket.clone(),
        workspace: dir.path().join("workspace"),
        model: claude::DEFAULT_MODEL.to_string(),
        max_tokens: claude::DEFAULT_MAX_TOKENS,
        tick: Duration::from_millis(50),
        api_key: None,
    };
    let daemon = tokio::spawn(daemon::serve(options));

    // The daemon binds the socket a moment after it starts.
    let mut ready = false;
    for _ in 0..100 {
        if socket.exists() {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(ready, "the daemon never bound its socket");

    let submitted = protocol::call(
        &socket,
        &Request::Submit {
            goal: "summarise the workspace".to_string(),
            max_turns: 6,
            approval_timeout_secs: 300,
        },
    )
    .await
    .expect("the submit is answered");
    let Response::Submitted { execution_id } = submitted else {
        panic!("unexpected answer: {submitted:?}");
    };

    let final_state = settle_over_socket(&socket, &execution_id).await;

    assert_eq!(final_state, "COMPLETED", "the session did not finish");

    // The list and history answers carry sequences, which an internally tagged
    // enum only encodes from a struct variant. Assert both over the socket.
    let listed = protocol::call(&socket, &Request::List { before: None })
        .await
        .expect("the list is answered");
    let Response::Sessions { sessions, .. } = listed else {
        panic!("unexpected answer: {listed:?}");
    };
    assert_eq!(sessions.len(), 1, "one session is recorded");

    let logged = protocol::call(
        &socket,
        &Request::History {
            execution_id,
            before: None,
        },
    )
    .await
    .expect("the history is answered");
    let Response::History { events, .. } = logged else {
        panic!("unexpected answer: {logged:?}");
    };
    assert!(
        events.iter().any(|event| event.contains("WorkflowStarted")),
        "the event log is missing its start: {events:?}"
    );
    // An audit trail must say what the agent did, not only which events ran.
    assert!(
        events
            .iter()
            .any(|event| event.contains("agent-notes.md") || event.contains("write_file")),
        "the event log does not record what the tools did: {events:?}"
    );

    // A well-formed id that names no session is an error from both commands.
    // A mistyped audit target must not read as a session that did nothing.
    let unknown = protocol::call(
        &socket,
        &Request::Status {
            execution_id: "00000000-0000-4000-8000-000000000000".to_string(),
            full: false,
        },
    )
    .await
    .expect("the status is answered");
    assert!(
        matches!(unknown, Response::Error { .. }),
        "an unknown session must be refused: {unknown:?}"
    );
    let missing = protocol::call(
        &socket,
        &Request::History {
            execution_id: "00000000-0000-4000-8000-000000000000".to_string(),
            before: None,
        },
    )
    .await
    .expect("the history is answered");
    let Response::Error { message } = missing else {
        panic!("an unknown session must be refused, got {missing:?}");
    };
    assert!(
        message.contains("no session"),
        "the refusal must name the missing session: {message}"
    );

    daemon.abort();
}

#[tokio::test]
async fn a_truncated_turn_never_reports_a_clean_finish() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");

    // A reply cut short by the output cap: text, no tool calls, and the
    // `max_tokens` stop reason the API reports for a truncated turn.
    let mut rt = SqliteRuntime::open(dir.path().join("agentd.db")).expect("the database opens");
    rt.register_workflow(&session::agent_session_info());
    rt.register_activity(&session::claude_turn_info(), |_input| {
        serde_json::to_value(TurnReply {
            content: json!([{ "type": "text", "text": "half an ans" }]),
            stop_reason: "max_tokens".to_string(),
            text: "half an ans".to_string(),
            tool_calls: Vec::new(),
        })
        .map_err(|e| format!("bad reply: {e}"))
    });
    rt.register_activity(
        &session::run_tool_info(),
        tools::activity_body(workspace.clone()),
    );

    let exec = rt
        .start_workflow(WORKFLOW_NAME, task(&workspace))
        .expect("the session starts");
    let state = rt.run_until_blocked(exec).await.expect("the run finishes");
    let RunState::Completed(output) = state else {
        panic!("expected a terminal report, got {state:?}");
    };

    let report: SessionReport = serde_json::from_value(output).expect("the report decodes");
    assert_eq!(
        report.stop, "max_tokens",
        "a truncated turn must not report as `end_turn`"
    );
}

#[test]
fn the_toolbox_refuses_a_symlink_that_escapes_the_workspace() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    let outside = dir.path().join("outside");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");
    std::fs::create_dir_all(&outside).expect("the outside directory is created");
    std::fs::write(outside.join("secret.txt"), "classified").expect("the secret is written");

    // One link to a directory outside the workspace, and one straight to the
    // file. The first escapes through the middle of a path, the second through
    // its final component.
    std::os::unix::fs::symlink(&outside, workspace.join("link")).expect("the link is created");
    std::os::unix::fs::symlink(outside.join("secret.txt"), workspace.join("direct"))
        .expect("the link is created");

    let body = tools::activity_body(workspace.clone());
    for path in ["link/secret.txt", "direct"] {
        let raw = body(tool_request(
            &workspace,
            tools::TOOL_READ_FILE,
            json!({ "path": path }),
        ))
        .expect("a tool failure is a result, not an activity error");
        let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");
        assert!(outcome.is_error, "`{path}` must not resolve");
        assert!(
            !outcome.output.contains("classified"),
            "`{path}` leaked the file outside the workspace"
        );
    }

    // A write through the escaping link must not land outside either.
    let raw = body(tool_request(
        &workspace,
        tools::TOOL_WRITE_FILE,
        json!({ "path": "link/planted.txt", "content": "planted" }),
    ))
    .expect("a tool failure is a result, not an activity error");
    let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");
    assert!(outcome.is_error, "the write must not resolve");
    assert!(
        !outside.join("planted.txt").exists(),
        "the write escaped the workspace"
    );
}

#[test]
fn a_second_daemon_cannot_open_a_database_another_one_holds() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("agentd.db");

    let held = guard::acquire(&db).expect("the first daemon takes the lock");
    let refused = guard::acquire(&db);
    assert!(
        refused.is_err(),
        "a second daemon must not open a database another one holds"
    );

    // Releasing the lock is what a process exit does, so a restart succeeds.
    drop(held);
    assert!(
        guard::acquire(&db).is_ok(),
        "the lock must be free once its holder is gone"
    );
}

#[tokio::test]
async fn a_stale_approval_cannot_release_a_later_tool_call() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");

    let calls = Arc::new(AtomicUsize::new(0));
    let mut rt = runtime(&dir.path().join("agentd.db"), &workspace, &calls);
    let exec = rt
        .start_workflow(WORKFLOW_NAME, task(&workspace))
        .expect("the session starts");
    let signal = drive_to_approval(&mut rt, exec).await;

    // A decision that does not name this call must not release it. The bare
    // prefix is what an early or repeated `approve` used to stage.
    let payload = serde_json::to_value(ApprovalDecision {
        approved: true,
        note: None,
    })
    .expect("the decision encodes");
    rt.send_signal(exec, SIGNAL_TOOL_APPROVAL, payload)
        .expect("the signal is staged");

    let state = rt.run_until_blocked(exec).await.expect("the run advances");
    assert!(
        matches!(&state, RunState::WaitingSignal(name) if name == &signal),
        "an unaddressed decision must leave the call parked, got {state:?}"
    );
    assert!(
        !workspace.join("agent-notes.md").exists(),
        "an unaddressed decision must not authorise the write"
    );

    // The decision that names the call does release it.
    approve(&mut rt, exec, &signal);
    let state = rt.run_until_blocked(exec).await.expect("the run finishes");
    assert!(
        matches!(state, RunState::Completed(_)),
        "expected completion, got {state:?}"
    );
}

#[test]
fn the_daemon_lock_follows_the_database_through_a_symlink() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let real = dir.path().join("real.db");
    let symlinked = dir.path().join("current.db");
    std::fs::write(&real, b"").expect("the database file is created");
    std::os::unix::fs::symlink(&real, &symlinked).expect("the symbolic link is created");

    // Two spellings of one file must take one lock, or two daemons write it.
    // The lock is held on the file itself, so both names reach it. A hard link
    // is refused outright instead — see
    // `a_hard_linked_database_is_refused`, because `SQLite` cannot share a
    // write-ahead log across two pathnames.
    let held = guard::acquire(&real).expect("the first daemon takes the lock");
    assert!(
        guard::acquire(&symlinked).is_err(),
        "an alias of a held database must not take a second lock"
    );

    drop(held);
    assert!(
        guard::acquire(&symlinked).is_ok(),
        "the lock must be free once its holder is gone"
    );
}

#[test]
fn the_toolbox_refuses_a_file_over_the_read_cap_without_reading_it() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().to_path_buf();
    let oversized = vec![b'x'; 70 * 1024];
    std::fs::write(workspace.join("big.txt"), &oversized).expect("the fixture is written");

    let body = tools::activity_body(workspace.clone());
    let raw = body(tool_request(
        &workspace,
        tools::TOOL_READ_FILE,
        json!({ "path": "big.txt" }),
    ))
    .expect("a tool failure is a result, not an activity error");
    let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");

    assert!(outcome.is_error, "an oversized file must not be read");
    assert!(
        outcome.output.contains("the limit is"),
        "unexpected message: {}",
        outcome.output
    );
}

#[tokio::test]
async fn a_session_refuses_to_run_in_another_workspace() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let theirs = dir.path().join("their-project");
    let ours = dir.path().join("our-project");
    std::fs::create_dir_all(&theirs).expect("the workspace is created");
    std::fs::create_dir_all(&ours).expect("the workspace is created");

    // The session belongs to one workspace; this daemon serves another. A
    // restart pointed elsewhere must not run the session's writes here.
    let calls = Arc::new(AtomicUsize::new(0));
    let mut rt = runtime(&dir.path().join("agentd.db"), &ours, &calls);
    let exec = rt
        .start_workflow(WORKFLOW_NAME, task(&theirs))
        .expect("the session starts");

    let state = rt.run_until_blocked(exec).await.expect("the run advances");
    let RunState::Failed(error) = state else {
        panic!("expected a terminal failure, got {state:?}");
    };
    assert!(
        error.contains("belongs to the workspace"),
        "unexpected failure: {error}"
    );
    assert!(
        !ours.join("agent-notes.md").exists(),
        "a session from another workspace must not write here"
    );
}

#[tokio::test]
async fn the_control_socket_is_private_and_never_deletes_another_file() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().expect("a temporary directory");

    // A typo in `--socket` must not destroy the file it happens to name.
    let precious = dir.path().join("notes.txt");
    std::fs::write(&precious, "keep me").expect("the file is written");
    let refused = daemon::bind(&precious).await;
    assert!(refused.is_err(), "a regular file must not be bound over");
    assert_eq!(
        std::fs::read_to_string(&precious).expect("the file survives"),
        "keep me",
        "the file must not be deleted"
    );

    // Whoever can connect can spend money, so the socket is owner-only.
    let socket = dir.path().join("agentd.sock");
    let listener = daemon::bind(&socket).await.expect("the socket binds");
    let mode = std::fs::metadata(&socket)
        .expect("the socket exists")
        .permissions()
        .mode()
        & 0o777;
    // The property is that no other local user can reach it. The owner's
    // execute bit is meaningless on a socket. The mask keeps that bit so a
    // DIRECTORY created while the mask is held stays enterable.
    assert_eq!(
        mode & 0o077,
        0,
        "the control socket must be owner-only, and has mode {mode:o}"
    );
    assert_eq!(mode & 0o700, 0o700, "the owner must reach its own socket");
    drop(listener);
}

#[test]
fn a_write_that_landed_is_never_reported_as_absent() {
    use crate::session::Message;

    // Two assistant turns put the stub on its final turn, where it reports.
    let transcript = |result: Value| TurnRequest {
        model: claude::OFFLINE_MODEL.to_string(),
        messages: vec![
            Message::user(json!([{ "type": "text", "text": "go" }])),
            Message::assistant(json!([{ "type": "text", "text": "listing" }])),
            Message::assistant(json!([{ "type": "text", "text": "writing" }])),
            Message::user(json!([result])),
        ],
    };
    let answer = |result: Value| claude::offline::reply(&transcript(result)).text;

    // A write can fail AFTER its rename: the file holds the new bytes, and
    // only the flush failed. Reporting that as "not recorded" would be false,
    // and it would contradict the reason printed beside it.
    let durable_failure = answer(json!({
        "type": "tool_result",
        "tool_use_id": "toolu_offline_write",
        "content": format!("`agent-notes.md` {} yet: no space left", tools::LANDED_UNFLUSHED),
        "is_error": true,
    }));
    assert!(
        durable_failure.contains("IS recorded"),
        "a write that landed must not be reported as absent: {durable_failure}"
    );

    // A write that never landed is still reported as absent.
    let refused = answer(json!({
        "type": "tool_result",
        "tool_use_id": "toolu_offline_write",
        "content": "the operator denied this call",
        "is_error": true,
    }));
    assert!(
        refused.contains("NOT recorded"),
        "a denied write must be reported as absent: {refused}"
    );

    // The block this daemon builds is replayed to the Messages API on the
    // next turn. It carries the fields that API defines for a tool_result,
    // and nothing else. A field invented here would travel with it.
    let outcome = session::ToolOutcome {
        output: "wrote 12 bytes".to_string(),
        is_error: false,
    };
    let block = session::tool_result_block("toolu_a", &outcome);
    let mut keys: Vec<&str> = block
        .as_object()
        .expect("the block is an object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec!["content", "is_error", "tool_use_id", "type"],
        "the tool_result block must carry no invented property"
    );
}

#[test]
fn a_blank_model_name_is_refused() {
    // A blank name is not a model. Every request would carry it, the API
    // would refuse each one, and the refusal of an accepted request is
    // terminal. The daemon would advertise readiness and fail every session.
    for blank in ["", " ", "\t\n"] {
        let (_, signal) = crate::shutdown::channel();
        let refusal = claude::ModelConfig::new(
            Some("sk-ant-example".to_string()),
            blank,
            claude::DEFAULT_MAX_TOKENS,
            signal,
        );
        let Err(message) = refusal else {
            panic!("a blank model name must be refused: {blank:?}");
        };
        assert!(
            message.contains("blank"),
            "the refusal must say what is wrong: {message}"
        );
    }

    // A real name still opens, and a padded one is stored trimmed. A check
    // that read the trimmed value while the verbatim one was sent would pass
    // this. It would then send a name with spaces to the API.
    for given in [
        claude::DEFAULT_MODEL,
        " claude-opus-5 ",
        "\tclaude-opus-5\n",
    ] {
        let (_, signal) = crate::shutdown::channel();
        let config = claude::ModelConfig::new(
            Some("sk-ant-example".to_string()),
            given,
            claude::DEFAULT_MAX_TOKENS,
            signal,
        )
        .expect("a real model name must be accepted");
        assert_eq!(
            config.identity(),
            given.trim(),
            "the stored name must carry no padding: {given:?}"
        );
    }
}

#[test]
fn a_long_history_does_not_make_one_unbounded_listing() {
    // `list` reads the whole row of every session it names, and both the goal
    // and the report are unbounded. The runtime is serialised, so an
    // unbounded listing would also block every session drive while it ran.
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("agentd.db");
    let conn = rusqlite::Connection::open(&db).expect("the database opens");
    conn.execute_batch(
        "CREATE TABLE harvest_executions (
            rowid_alias INTEGER, exec_id TEXT, workflow_name TEXT, state TEXT,
            input_json TEXT, output_json TEXT, error TEXT
        );",
    )
    .expect("the fixture schema is created");
    let rows = inspect::MAX_LISTED_SESSIONS + 25;
    for n in 0..rows {
        conn.execute(
            "INSERT INTO harvest_executions
             (exec_id, workflow_name, state, input_json, output_json, error)
             VALUES (?1, ?2, 'COMPLETED', '{}', NULL, NULL)",
            rusqlite::params![format!("exec-{n:04}"), WORKFLOW_NAME],
        )
        .expect("the fixture row is inserted");
    }
    drop(conn);

    let reader = inspect::open(&db).expect("the read-only connection opens");
    let listed = inspect::executions(&reader, WORKFLOW_NAME, None).expect("the listing reads");
    assert_eq!(
        listed.len(),
        inspect::MAX_LISTED_SESSIONS as usize + 1,
        "the listing must stop one past the cap, so the caller can say there are more"
    );

    // The newest are the ones an operator is looking for, and they read in
    // the order they were submitted.
    let last = listed.last().expect("the listing is not empty");
    assert_eq!(
        last.exec_id,
        Some(format!("exec-{:04}", rows - 1)),
        "the newest session must be in the listing"
    );
}

#[test]
fn a_listing_reads_no_whole_payload() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("agentd.db");
    let conn = rusqlite::Connection::open(&db).expect("the database opens");
    conn.execute_batch(
        "CREATE TABLE harvest_executions (
            exec_id TEXT, workflow_name TEXT, state TEXT,
            input_json TEXT, output_json TEXT, error TEXT
        );",
    )
    .expect("the fixture schema is created");

    // A recorded task and a recorded report are each written by somebody
    // else, and neither is bounded at the source. A listing that read them
    // whole would hold every byte of every session it names.
    let huge = "x".repeat(200_000);
    conn.execute(
        "INSERT INTO harvest_executions
         (exec_id, workflow_name, state, input_json, output_json, error)
         VALUES ('exec-1', ?1, 'COMPLETED', ?2, ?3, ?4)",
        rusqlite::params![
            WORKFLOW_NAME,
            json!({ "goal": huge, "workspace": "/w", "model": "m", "max_turns": 4,
                    "approval_timeout_secs": 1 })
            .to_string(),
            json!({ "answer": huge, "turns": 2, "tool_calls": 1, "stop": "end_turn" }).to_string(),
            huge,
        ],
    )
    .expect("the fixture row is inserted");
    drop(conn);

    let reader = inspect::open(&db).expect("the reader opens");
    let listed = inspect::executions(&reader, WORKFLOW_NAME, None).expect("the listing reads");
    let row = listed.first().expect("the session is listed");
    let cap = inspect::MAX_LISTED_CHARS as usize;

    for (name, field) in [
        ("goal", &row.goal),
        ("answer", &row.answer),
        ("error", &row.error),
    ] {
        let held = field.as_deref().unwrap_or_default();
        // Both halves matter. The first says the field obeys the cap. The
        // second says the cap is doing work: an assertion against the cap
        // alone would hold however large the cap became.
        //
        // The projection reads ONE character past the printed cap. That
        // character is how the renderer knows the field was cut, and it is
        // the only reason this bound is not the cap itself.
        assert_eq!(
            held.len(),
            cap + 1,
            "the listing must read `{name}` to one character past the cap"
        );
        assert!(
            held.len() < huge.len(),
            "the listing must not read the whole `{name}`, and read {} of {} bytes",
            held.len(),
            huge.len()
        );
    }

    // The fields it does not cut are the ones that are already small, and the
    // listing still says what the session did.
    assert_eq!(
        row.stop.as_deref(),
        Some("end_turn"),
        "the stop reason reads"
    );
    assert_eq!(row.turns, Some(2), "the turn count reads");
    assert_eq!(row.tool_calls, Some(1), "the tool call count reads");
}

#[tokio::test]
async fn a_status_reads_a_bounded_slice_of_the_history() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("agentd.db");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");

    let calls = Arc::new(AtomicUsize::new(0));
    let mut rt = runtime(&db, &workspace, &calls);
    let exec = rt
        .start_workflow(WORKFLOW_NAME, task(&workspace))
        .expect("the session starts");
    let signal = drive_to_approval(&mut rt, exec).await;
    drop(rt);

    let reader = inspect::open(&db).expect("the reader opens");
    let exec_id = exec.to_string();

    // The status must find the awaited call in the newest events. Every model
    // activity carries the whole transcript, so reading the history entire is
    // unbounded twice over, and one status would block every session drive.
    let call = daemon::pending_call(&reader, &exec_id, &signal, false)
        .expect("the replies read")
        .expect("the awaited call is in the newest events");
    assert_eq!(call.token, signal, "the call must be the awaited one");

    // The cap is what bounds the read. A session this short has fewer events
    // than `MAX_SCANNED_EVENTS`, so asserting against that cap would pass
    // whether or not the query carries a limit. The assertion uses a small
    // limit instead, which only holds if the limit reaches the query.
    let whole = inspect::event_lines(&reader, &exec_id, None, u32::MAX).expect("the events read");
    assert!(
        whole.len() > 3,
        "the fixture must hold more events than the limit below, and holds {}",
        whole.len()
    );
    let capped = inspect::event_lines(&reader, &exec_id, None, 3).expect("the events read");
    assert_eq!(
        capped.len(),
        3,
        "the read must stop at the limit it is given"
    );

    // Newest first, so the scan reaches the last model reply immediately.
    assert_eq!(
        capped.first().map(|line| line.seq),
        whole.first().map(|line| line.seq),
        "the bounded read must start at the newest event"
    );

    // A page is not a window. The search must reach an event that sits further
    // back than one page. A turn with many tool calls before its gated write
    // would otherwise leave the operator with no token to approve.
    let oldest_seq = whole.last().expect("the history is not empty").seq;
    let reached = (0..)
        .scan(None, |before: &mut Option<i64>, _| {
            let page = inspect::event_lines(&reader, &exec_id, *before, 1).ok()?;
            let seq = page.first()?.seq;
            *before = Some(seq);
            Some(seq)
        })
        .take(whole.len())
        .last()
        .expect("the walk reads at least one page");
    assert_eq!(
        reached, oldest_seq,
        "a page-by-page walk must reach the oldest event"
    );
}

#[tokio::test]
async fn a_status_finds_a_call_behind_more_events_than_one_page() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("agentd.db");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");

    let calls = Arc::new(AtomicUsize::new(0));
    let mut rt = runtime(&db, &workspace, &calls);
    let exec = rt
        .start_workflow(WORKFLOW_NAME, task(&workspace))
        .expect("the session starts");
    let signal = drive_to_approval(&mut rt, exec).await;
    drop(rt);

    let reader = inspect::open(&db).expect("the reader opens");
    let exec_id = exec.to_string();

    // One page of ONE event. A fixed window of this size would step over the
    // model reply that holds the awaited call. The status would then print no
    // token at all, and the operator could not approve before the deadline. A
    // page must bound the memory in hand, and nothing else.
    let mut before = None;
    let mut walked = 0;
    let found = loop {
        let page = inspect::event_lines(&reader, &exec_id, before, 1).expect("the page reads");
        let Some(line) = page.first() else {
            break None;
        };
        before = Some(line.seq);
        walked += 1;
        if line.label == "ActivityCompleted"
            && line
                .detail
                .as_deref()
                .is_some_and(|detail| detail.contains("tool_use"))
        {
            break Some(walked);
        }
    };
    let depth = found.expect("a model reply with a call is in the log");
    assert!(
        depth > 1,
        "the fixture must hide the reply behind at least one other event"
    );

    // The daemon's own lookup finds it whatever the page size.
    let call = daemon::pending_call(&reader, &exec_id, &signal, false)
        .expect("the replies read")
        .expect("the awaited call must be found however deep it sits");
    assert_eq!(call.token, signal, "the call must be the awaited one");

    // The lookup reads the CALLS of a reply and not the reply. A reply holds
    // every earlier turn in its content, and none of that names a call. A read
    // of the whole reply would carry the transcript with it.
    let replies = inspect::reply_calls(&reader, &exec_id, None, 1).expect("the calls read");
    let (_, calls) = replies.first().expect("a reply is recorded");
    let inspect::ReplyCalls::Calls(calls) = calls else {
        panic!("the query must return the calls it read: {calls:?}");
    };
    assert!(
        calls.first().is_some_and(|call| !call.id.is_empty()),
        "the calls must carry their ids: {calls:?}"
    );
    // The discriminating assertion. The whole reply is an OBJECT carrying a
    // stop reason and the replayed content blocks. What the read returns is
    // the calls alone. A tool input may hold a `content` field of its own,
    // so the stop reason is the field that tells the two shapes apart.
    let rendered = serde_json::to_string(calls).expect("the calls serialise");
    assert!(
        !rendered.contains("stop_reason"),
        "the read must carry the calls alone: {rendered}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_history_command_reads_a_bounded_page() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("agentd.db");
    let socket = dir.path().join("agentd.sock");
    let options = daemon::Options {
        db: db.clone(),
        socket: socket.clone(),
        workspace: dir.path().join("workspace"),
        model: claude::DEFAULT_MODEL.to_string(),
        max_tokens: claude::DEFAULT_MAX_TOKENS,
        tick: Duration::from_millis(50),
        api_key: None,
    };
    let served = tokio::spawn(daemon::serve(options));
    await_daemon(&socket).await;

    let submitted = protocol::call(
        &socket,
        &Request::Submit {
            goal: "summarise the workspace".to_string(),
            max_turns: 6,
            approval_timeout_secs: 300,
        },
    )
    .await
    .expect("the submit is answered");
    let Response::Submitted { execution_id } = submitted else {
        panic!("unexpected answer: {submitted:?}");
    };
    await_parked(&socket, &execution_id).await;

    let answer = protocol::call(
        &socket,
        &Request::History {
            execution_id: execution_id.clone(),
            before: None,
        },
    )
    .await
    .expect("the history is answered");
    let Response::History { events, .. } = answer else {
        panic!("unexpected answer: {answer:?}");
    };

    // The audit trail is the point of the command, so a short session prints
    // whole. The bound is on what one command reads, not on what it may show.
    assert!(!events.is_empty(), "the history must not be empty");
    assert!(
        events.len() <= inspect::MAX_HISTORY_EVENTS as usize + 1,
        "the history must stay bounded, and printed {} lines",
        events.len()
    );
    assert!(
        !events[0].contains("the log holds more"),
        "a short session is the whole log: {}",
        events[0]
    );

    // An event's data is cut in the DATABASE. A recorded activity can approach
    // the backend's payload cap, and a page names hundreds of them. A page
    // that read them whole would hold gigabytes for one command.
    let cap = inspect::MAX_EVENT_DETAIL_CHARS as usize;
    for line in &events {
        assert!(
            line.chars().count() <= cap + 64,
            "an audit line must be cut, and printed {} characters",
            line.chars().count()
        );
    }

    // Each line carries the event's OWN sequence number, which the log counts
    // from zero. A bounded read therefore never renumbers the log it shows,
    // and a later page reads on from where this one ended.
    assert!(
        events[0].trim_start().starts_with("0  "),
        "the first line must carry the log's own first sequence number: {}",
        events[0]
    );

    // A truncated log must be reachable. The marker names the command that
    // reads the events before this page, and that command must work.
    let page = protocol::call(
        &socket,
        &Request::History {
            execution_id: execution_id.clone(),
            before: Some(2),
        },
    )
    .await
    .expect("the page is answered");
    let Response::History { events: older, .. } = page else {
        panic!("unexpected answer: {page:?}");
    };
    assert_eq!(older.len(), 2, "the page before event 2 holds two events");

    // The continuation command is rendered by the CLIENT, so it carries the
    // socket the operator asked. A command built in the daemon cannot know
    // it, and would send the operator to whatever answers the default.
    let hinted = crate::rendered_lines(
        &Response::History {
            events: vec!["0  WorkflowStarted".to_string()],
            execution_id: execution_id.clone(),
            older: Some(7),
        },
        Path::new("/run/agentd/project-b.sock"),
    );
    assert!(
        hinted[0].contains("--socket=/run/agentd/project-b.sock")
            && hinted[0].contains("--before 7")
            && hinted[0].contains(&execution_id),
        "the continuation command must reach the same daemon: {}",
        hinted[0]
    );
    assert!(
        older[0].trim_start().starts_with("0  "),
        "the page must start at the log's own first event: {}",
        older[0]
    );

    served.abort();
    drop(served.await);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_empty_goal_never_starts_a_session() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let socket = dir.path().join("agentd.sock");
    let served = tokio::spawn(daemon::serve(daemon::Options {
        db: dir.path().join("agentd.db"),
        socket: socket.clone(),
        workspace: dir.path().join("workspace"),
        model: claude::DEFAULT_MODEL.to_string(),
        max_tokens: claude::DEFAULT_MAX_TOKENS,
        tick: Duration::from_millis(50),
        api_key: None,
    }));
    await_daemon(&socket).await;

    // An empty goal is sent as an empty text block, which the API refuses.
    // The refusal of an accepted request is terminal here, so the daemon
    // would acknowledge a session that could never make its first call.
    for empty in ["", "   ", "\t\n"] {
        let answer = protocol::call(
            &socket,
            &Request::Submit {
                goal: empty.to_string(),
                max_turns: 4,
                approval_timeout_secs: 60,
            },
        )
        .await
        .expect("the submit is answered");
        let Response::Error { message } = answer else {
            panic!("an empty goal must be refused, and got: {answer:?}");
        };
        assert!(
            message.contains("empty"),
            "the refusal must say what is wrong: {message}"
        );
    }

    // The field beside it takes the same argument. A protocol client can send
    // a bound of zero even though the CLI refuses it. The loop would then run
    // `1..=0` and record a COMPLETED session that never called the model.
    // The deadline is recorded and read back as a signed integer. A larger
    // one would be accepted here and would then stop every later daemon.
    let huge = protocol::call(
        &socket,
        &Request::Submit {
            goal: "a real goal".to_string(),
            max_turns: 4,
            approval_timeout_secs: u64::MAX,
        },
    )
    .await
    .expect("the submit is answered");
    let Response::Error { message } = huge else {
        panic!("an unrecordable deadline must be refused, and got: {huge:?}");
    };
    assert!(
        message.contains("approval deadline"),
        "the refusal must name the field: {message}"
    );

    let zero = protocol::call(
        &socket,
        &Request::Submit {
            goal: "a real goal".to_string(),
            max_turns: 0,
            approval_timeout_secs: 60,
        },
    )
    .await
    .expect("the submit is answered");
    let Response::Error { message } = zero else {
        panic!("a zero turn bound must be refused, and got: {zero:?}");
    };
    assert!(
        message.contains("turn"),
        "the refusal must say what is wrong: {message}"
    );

    // Nothing was recorded, so no session is left to fail.
    let listed = protocol::call(&socket, &Request::List { before: None })
        .await
        .expect("the listing is answered");
    let Response::Sessions { sessions, .. } = listed else {
        panic!("unexpected answer: {listed:?}");
    };
    assert!(
        sessions.is_empty(),
        "a refused submit must record nothing: {sessions:?}"
    );

    served.abort();
    drop(served.await);
}

#[test]
fn a_block_that_cannot_be_replayed_is_refused() {
    use autumn_harvest::failure::parse_error_payload_full;

    let reply = |content: Value| TurnReply {
        content,
        stop_reason: "tool_use".to_string(),
        text: String::new(),
        tool_calls: vec![ToolCall {
            id: "toolu_a".to_string(),
            name: tools::TOOL_WRITE_FILE.to_string(),
            input: json!({ "path": "notes.md", "content": "x" }),
        }],
    };

    // The assistant blocks are replayed VERBATIM on the next request. A block
    // the API will not accept back fails the turn AFTER this turn's tools
    // have run. A malformed billed response would therefore leave a real
    // change on the disk, and a failed session behind it.
    for malformed in [
        json!([Value::Null, { "type": "tool_use", "id": "toolu_a" }]),
        json!(["a bare string"]),
        json!([{ "text": "a block with no type" }]),
        json!([{ "type": "" }]),
        json!("not an array at all"),
    ] {
        assert!(
            !claude::has_replayable_content(&reply(malformed.clone())),
            "a block that cannot be replayed must be refused: {malformed}"
        );
    }

    // A block of a type this example KNOWS must carry that type's fields. The
    // API refuses these on replay, and `parse_reply` would quietly default
    // them here. A text block with no text reads as an empty answer. A call
    // with no name reads as a call to nothing.
    for incomplete in [
        json!([{ "type": "text" }]),
        json!([{ "type": "text", "text": 7 }]),
        // A text block is declared with a minimum length of one character,
        // so an empty one is refused on replay.
        json!([{ "type": "text", "text": "" }]),
        json!([{ "type": "tool_use", "id": "toolu_a", "input": {} }]),
        json!([{ "type": "tool_use", "id": "toolu_a", "name": " ", "input": {} }]),
        json!([{ "type": "tool_use", "name": "write_file", "input": {} }]),
        json!([{ "type": "tool_use", "id": "toolu_a", "name": "write_file" }]),
        json!([{ "type": "tool_use", "id": "toolu_a", "name": "write_file", "input": Value::Null }]),
        json!([{ "type": "tool_use", "id": "toolu_a", "name": "write_file", "input": "text" }]),
        json!([{ "type": "tool_use", "id": "toolu_a", "name": "write_file", "input": [] }]),
        json!([{ "type": "thinking", "signature": "abc" }]),
        json!([{ "type": "thinking", "thinking": Value::Null, "signature": "abc" }]),
        json!([{ "type": "thinking", "thinking": 7, "signature": "abc" }]),
        // The signature carries the encrypted reasoning, and the API reads it
        // to prove the block came from the model. It is present whatever the
        // display setting, so a block without one cannot be replayed.
        json!([{ "type": "thinking", "thinking": "reasoned" }]),
        json!([{ "type": "thinking", "thinking": "", "signature": "" }]),
        json!([{ "type": "thinking", "thinking": "", "signature": " " }]),
        json!([{ "type": "thinking", "thinking": "", "signature": 7 }]),
        json!([{ "type": "redacted_thinking" }]),
        json!([{ "type": "redacted_thinking", "data": "" }]),
        // A padded name is a CORRUPTED block of a type this example knows,
        // and not a type from a later API. `parse_reply` matches the type
        // exactly, so it would ignore the block while the API refuses it.
        json!([{ "type": " text ", "text": "hello" }]),
        json!([{ "type": "text\n", "text": "hello" }]),
        json!([{ "type": " thinking ", "thinking": "", "signature": "abc" }]),
        json!([{ "type": " tool_use ", "id": "toolu_a", "name": "write_file", "input": {} }]),
    ] {
        assert!(
            !claude::has_replayable_content(&reply(incomplete.clone())),
            "a known block missing its fields must be refused: {incomplete}"
        );
    }

    // The text of a thinking block may be EMPTY, and the block is still
    // replayed unchanged. This request asks for adaptive thinking and asks
    // for no display, and under the default display every thinking block
    // comes back with an empty text. A check for text here would refuse the
    // model's ORDINARY replies, which is a worse fault than the one above.
    for empty in [
        json!([{ "type": "thinking", "thinking": "", "signature": "abc" }]),
        json!([{ "type": "thinking", "thinking": "reasoned", "signature": "abc" }]),
        json!([{ "type": "redacted_thinking", "data": "abc" }]),
    ] {
        assert!(
            claude::has_replayable_content(&reply(empty.clone())),
            "an empty thinking block must still be replayed: {empty}"
        );
    }

    // A block type this example does not know about still passes, because the
    // API knows types this example does not. Guessing at the fields of a type
    // from a later API would refuse replies that are perfectly good.
    for fine in [
        json!([{ "type": "text", "text": "hello" }]),
        // One space is a character, so it meets the minimum. This asks for
        // length and not for content.
        json!([{ "type": "text", "text": " " }]),
        json!([{ "type": "a_type_from_a_later_api" }]),
        json!([{ "type": "tool_use", "id": "toolu_a", "name": "write_file", "input": {} }]),
        json!([]),
    ] {
        assert!(
            claude::has_replayable_content(&reply(fine.clone())),
            "a well-formed block must pass: {fine}"
        );
    }

    // The refusal is terminal, because the response was billed.
    let refused = parse_error_payload_full(&claude::body_failure(
        reqwest::StatusCode::OK,
        "its response carried a content block that cannot be replayed",
    ));
    assert!(refused.non_retryable, "a billed malformed body is terminal");
}

#[test]
fn a_zero_turn_session_is_rejected() {
    use clap::Parser;

    // Zero turns is not a session. The loop runs no iteration, and the run is
    // recorded COMPLETED with a blank answer and `max_turns` as its reason.
    assert!(
        crate::Cli::try_parse_from(["agentd", "submit", "goal", "--max-turns", "0"]).is_err(),
        "a zero turn bound must be refused at the boundary"
    );
    assert!(
        crate::Cli::try_parse_from(["agentd", "submit", "goal", "--max-turns", "1"]).is_ok(),
        "one turn is a session"
    );
}

#[test]
fn a_zero_drive_interval_is_rejected() {
    use clap::Parser;

    // A zero period panics the timer, which would take the daemon down.
    assert!(
        crate::Cli::try_parse_from(["agentd", "serve", "--tick-ms", "0"]).is_err(),
        "a zero tick must be refused at the boundary"
    );
    assert!(
        crate::Cli::try_parse_from(["agentd", "serve", "--tick-ms", "1"]).is_ok(),
        "one millisecond is a usable period"
    );
}

#[tokio::test]
async fn the_full_view_shows_a_write_that_the_status_trims() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");

    // A write may carry up to 64 KiB, and an operator approves the WHOLE of it.
    let content = "x".repeat(5000);
    let tail = "THE-END-OF-THE-PAYLOAD";
    let payload = format!("{content}{tail}");

    let mut rt = SqliteRuntime::open(dir.path().join("agentd.db")).expect("the database opens");
    rt.register_workflow(&session::agent_session_info());
    let written = payload.clone();
    rt.register_activity(&session::claude_turn_info(), move |_input| {
        let input = json!({ "path": "notes.md", "content": written });
        serde_json::to_value(TurnReply {
            content: json!([
                { "type": "tool_use", "id": "toolu_big", "name": tools::TOOL_WRITE_FILE, "input": input },
            ]),
            stop_reason: "tool_use".to_string(),
            text: String::new(),
            tool_calls: vec![ToolCall {
                id: "toolu_big".to_string(),
                name: tools::TOOL_WRITE_FILE.to_string(),
                input: json!({ "path": "notes.md", "content": payload }),
            }],
        })
        .map_err(|e| format!("bad reply: {e}"))
    });
    rt.register_activity(
        &session::run_tool_info(),
        tools::activity_body(workspace.clone()),
    );

    let exec = rt
        .start_workflow(WORKFLOW_NAME, task(&workspace))
        .expect("the session starts");
    let signal = drive_to_approval(&mut rt, exec).await;

    // The status reads the event log through the read-only connection, and it
    // reads a bounded number of the newest events rather than the whole
    // history. See `inspect::MAX_SCANNED_EVENTS`.
    let reader = inspect::open(&dir.path().join("agentd.db")).expect("the reader opens");
    let exec_id = exec.to_string();
    let trimmed = daemon::pending_call(&reader, &exec_id, &signal, false)
        .expect("the replies read")
        .expect("the status shows a call");
    assert!(
        trimmed.input.contains("truncated"),
        "the status must say when it has trimmed the payload"
    );
    assert!(
        !trimmed.input.contains(tail),
        "the trimmed view cannot hold the whole payload"
    );
    assert!(
        trimmed.truncated,
        "the trimmed view must say it was cut, so no client offers an approval from it"
    );
    // The destination survives the cut. The arguments serialise in key order,
    // so `content` comes before `path`, and a payload this long would push
    // the path past the cut. The operator would read a cut write with no
    // destination in it.
    assert!(
        trimmed.input.contains("\"path\":\"notes.md\""),
        "the cut view must still name where the write goes: {}",
        trimmed.input
    );

    let whole = daemon::pending_call(&reader, &exec_id, &signal, true)
        .expect("the replies read")
        .expect("the full view shows a call");
    assert!(
        whole.input.contains(tail),
        "the full view must show every byte an approval authorises"
    );
    assert!(
        !whole.input.contains("truncated"),
        "the full view must not be trimmed"
    );
    assert!(
        !whole.truncated,
        "the full view carries the approval, so it must not be marked cut"
    );
    // The full view is the recorded call, verbatim. The cut view reorders the
    // arguments to save the short ones, and that reordering must never reach
    // the text an approval is given against.
    assert_eq!(
        whole.input,
        json!({ "path": "notes.md", "content": format!("{content}{tail}") }).to_string(),
        "the full view must print the recorded call exactly"
    );
}

/// A cut view offers no approval, and a whole one does.
///
/// An approval decides about the WHOLE call. The truncated flag is the only
/// thing that separates these two views, and `pending_call` sets it from the
/// call it actually cut. See
/// [`the_full_view_shows_a_write_that_the_status_trims`].
///
/// The `deny` command stays on the cut view. Refusing a call nobody has read
/// refuses a write. Making an operator read 64 KiB before they may refuse it
/// is a reason to skip the reading.
#[test]
fn a_cut_pending_call_offers_no_approval() {
    let view = |truncated: bool| protocol::SessionView {
        execution_id: "01JCEXEC".to_string(),
        goal: "tidy the notes".to_string(),
        state: "RUNNING".to_string(),
        blocked_on: Some("waiting for a tool approval".to_string()),
        pending: Some(protocol::PendingCall {
            token: "tool_approval:toolu_a".to_string(),
            id: "toolu_a".to_string(),
            tool: tools::TOOL_WRITE_FILE.to_string(),
            input: "{\"path\":\"notes.md\"}".to_string(),
            truncated,
        }),
        answer: None,
        error: None,
    };
    let rendered = |truncated: bool| {
        crate::session_lines(&view(truncated), Path::new("agentd.sock")).join("\n")
    };

    let cut = rendered(true);
    assert!(
        !cut.contains("agentd approve"),
        "a cut view must offer no approval: {cut}"
    );
    assert!(
        cut.contains("agentd status 01JCEXEC --full"),
        "a cut view must name the command that shows the call whole: {cut}"
    );
    assert!(
        cut.contains("agentd deny 01JCEXEC tool_approval:toolu_a"),
        "a cut view must still offer the refusal: {cut}"
    );

    let whole = rendered(false);
    assert!(
        whole.contains("agentd approve 01JCEXEC tool_approval:toolu_a"),
        "a whole view carries the approval: {whole}"
    );
}

#[test]
fn only_an_accepted_request_is_refused_a_retry() {
    use autumn_harvest::failure::parse_error_payload_full;
    use reqwest::StatusCode;

    // A body that fails after a 2xx means the turn was billed. Retrying buys it
    // twice, so that case is terminal.
    let accepted = parse_error_payload_full(&claude::body_failure(StatusCode::OK, "it went away"));
    assert!(
        accepted.non_retryable,
        "a lost response to an accepted request must not be retried"
    );

    // A rate limit produced no turn, so it must still back off and retry.
    for status in [StatusCode::TOO_MANY_REQUESTS, StatusCode::BAD_GATEWAY] {
        let transient = parse_error_payload_full(&claude::body_failure(status, "it went away"));
        assert!(
            !transient.non_retryable,
            "{status} must stay retryable even when its body is unreadable"
        );
    }

    // A rejected request fails the same way whether or not its body read.
    let rejected = parse_error_payload_full(&claude::body_failure(StatusCode::BAD_REQUEST, "gone"));
    assert!(
        rejected.non_retryable,
        "a rejected request must not be retried"
    );
}

#[tokio::test]
async fn a_session_refuses_to_continue_on_another_model() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");

    // The session was started on the offline stub; this daemon serves a real
    // model. Continuing would move a conversation onto another model, and an
    // offline session onto billed calls.
    let model = claude::ModelConfig::new(
        Some("sk-ant-not-a-real-key".to_string()),
        claude::DEFAULT_MODEL,
        claude::DEFAULT_MAX_TOKENS,
        crate::shutdown::channel().1,
    )
    .expect("the configuration builds");

    let mut rt = SqliteRuntime::open(dir.path().join("agentd.db")).expect("the database opens");
    rt.register_workflow(&session::agent_session_info());
    rt.register_activity(&session::claude_turn_info(), claude::activity_body(model));
    rt.register_activity(
        &session::run_tool_info(),
        tools::activity_body(workspace.clone()),
    );

    let exec = rt
        .start_workflow(WORKFLOW_NAME, task_on(&workspace, claude::OFFLINE_MODEL))
        .expect("the session starts");
    let state = rt.run_until_blocked(exec).await.expect("the run advances");

    let RunState::Failed(error) = state else {
        panic!("expected a terminal failure, got {state:?}");
    };
    assert!(
        error.contains("this session runs on"),
        "unexpected failure: {error}"
    );
}

#[test]
fn a_billed_response_that_is_not_a_message_is_refused() {
    use autumn_harvest::failure::parse_error_payload_full;

    // Valid JSON is not yet a message. Without the shape check these fall
    // through every default and record a clean, empty `end_turn`.
    for malformed in [
        json!({}),
        json!({ "content": [] }),
        json!({ "stop_reason": "end_turn" }),
        json!({ "content": "not an array", "stop_reason": "end_turn" }),
    ] {
        assert!(
            !claude::is_message(&malformed),
            "{malformed} must not pass as a message"
        );
    }

    let message = json!({
        "content": [{ "type": "text", "text": "hello" }],
        "stop_reason": "end_turn",
    });
    assert!(claude::is_message(&message), "a real message must pass");

    // A blank stop reason is not a stop reason. It also differs from
    // `end_turn`, so the usability test would accept it, and the loop would
    // record a completed session whose stop reason says nothing. Whitespace
    // is as blank as an empty string, and it takes the same path.
    for blank in [
        json!({ "content": [Value::Null], "stop_reason": "" }),
        json!({ "content": [Value::Null], "stop_reason": " " }),
        json!({ "content": [Value::Null], "stop_reason": "\t\n" }),
    ] {
        assert!(
            !claude::is_message(&blank),
            "{blank} must not pass as a message"
        );
    }

    // A malformed body from an accepted request is terminal, like the others.
    let refused = parse_error_payload_full(&claude::body_failure(
        reqwest::StatusCode::OK,
        "its response was not a message",
    ));
    assert!(refused.non_retryable, "a billed malformed body is terminal");
}

#[test]
fn a_padded_stop_reason_is_refused_rather_than_normalised() {
    // A reason with space around it has no safe normalisation. Trimming turns
    // ` tool_use ` into the reason that AUTHORISES a tool call, so a
    // malformed response would reach the approval gate and run an approved
    // write. Keeping it verbatim matches no reason this loop acts on, so
    // ` end_turn ` would record a session as complete with no answer.
    //
    // So it is refused as the malformed body it is, before either.
    for padded in [" end_turn ", " tool_use ", "tool_use\t", "\nend_turn"] {
        let payload = json!({
            "content": [{ "type": "text", "text": "done" }],
            "stop_reason": padded,
        });
        assert!(
            !claude::is_message(&payload),
            "a padded stop reason must be refused: {padded:?}"
        );
    }

    // The two reasons this loop acts on still pass, exactly as they arrive.
    for exact in [claude::STOP_END_TURN, claude::STOP_TOOL_USE, "max_tokens"] {
        let payload = json!({
            "content": [{ "type": "text", "text": "done" }],
            "stop_reason": exact,
        });
        assert!(
            claude::is_message(&payload),
            "an exact stop reason must pass: {exact}"
        );
    }

    // A blank one is still refused, which is what this check was built for.
    for blank in ["", " ", "\t"] {
        let payload = json!({
            "content": [{ "type": "text", "text": "done" }],
            "stop_reason": blank,
        });
        assert!(
            !claude::is_message(&payload),
            "a blank stop reason must be refused: {blank:?}"
        );
    }

    // The projection keeps the reason VERBATIM, so nothing downstream can
    // turn a padded one into the reason that authorises a tool call.
    let padded = claude::parse_reply(&json!({
        "content": [{ "type": "text", "text": "done" }],
        "stop_reason": " tool_use ",
    }));
    assert_eq!(
        padded.stop_reason, " tool_use ",
        "the projection must not normalise the reason"
    );
    assert_ne!(
        padded.stop_reason,
        claude::STOP_TOOL_USE,
        "a padded reason must never match the one that authorises a tool call"
    );
}

#[test]
fn a_turn_of_whitespace_is_not_an_answer() {
    // Whitespace is not an answer. A turn carrying only blank text passes an
    // emptiness test, so the session reports a clean finish with nothing in
    // it. The bytes are kept as they arrive, because indentation and line
    // breaks are part of a code answer. Only the decision reads the trim.
    let blank = claude::parse_reply(&json!({
        "content": [{ "type": "text", "text": "  \n\t " }],
        "stop_reason": "end_turn",
    }));
    assert_eq!(blank.text, "  \n\t ", "the bytes must arrive unchanged");
    assert!(
        !claude::is_usable(&blank),
        "a turn of whitespace must not pass as an answer"
    );

    let indented = claude::parse_reply(&json!({
        "content": [{ "type": "text", "text": "    let x = 1;\n" }],
        "stop_reason": "end_turn",
    }));
    assert!(
        claude::is_usable(&indented),
        "an indented answer must still pass"
    );
    assert_eq!(
        indented.text, "    let x = 1;\n",
        "an answer keeps its own layout"
    );
}

#[test]
fn a_new_database_and_its_sidecars_are_owner_only() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("agentd.db");

    // The database holds every prompt and every tool result, so it is at least
    // as sensitive as the control socket.
    let held = guard::acquire(&db).expect("the lock is taken");
    let mode = std::fs::metadata(&db)
        .expect("the database exists")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600, "a new database must be owner-only");
    drop(held);

    // The `-wal` and `-shm` sidecars carry the same data, and `SQLite` creates
    // them itself, so the mask is what makes them private.
    let runtime = guard::with_private_umask(|| SqliteRuntime::open(&db));
    drop(runtime.expect("the runtime opens"));
    for sidecar in ["agentd.db-wal", "agentd.db-shm"] {
        let path = dir.path().join(sidecar);
        if let Ok(meta) = std::fs::metadata(&path) {
            assert_eq!(
                meta.permissions().mode() & 0o777,
                0o600,
                "{sidecar} must be owner-only"
            );
        }
    }
}

#[test]
fn the_toolbox_refuses_a_named_pipe_without_blocking_on_it() {
    use std::ffi::CString;

    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().to_path_buf();
    let fifo = workspace.join("pipe");

    let path = CString::new(fifo.to_string_lossy().as_bytes()).expect("a C path");
    // SAFETY: `path` is a valid, NUL-terminated C string that lives across the
    // call, and `mkfifo` only reads it.
    let made = unsafe { libc::mkfifo(path.as_ptr(), 0o600) };
    assert_eq!(made, 0, "the test fixture needs a FIFO");

    // A FIFO with no writer blocks a plain open. One runtime serves every
    // session, so a blocked body would wedge the whole daemon.
    let body = tools::activity_body(workspace.clone());
    for tool in [tools::TOOL_READ_FILE, tools::TOOL_WRITE_FILE] {
        let raw = body(tool_request(
            &workspace,
            tool,
            json!({ "path": "pipe", "content": "x" }),
        ))
        .expect("a tool failure is a result, not an activity error");
        let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");
        assert!(outcome.is_error, "{tool} must refuse a FIFO");
        assert!(
            outcome.output.contains("not an ordinary file"),
            "unexpected message from {tool}: {}",
            outcome.output
        );
    }
}

#[test]
fn a_write_replaces_its_target_atomically() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().to_path_buf();
    std::fs::write(workspace.join("notes.md"), "the previous content")
        .expect("the fixture is written");

    let body = tools::activity_body(workspace.clone());
    let raw = body(tool_request(
        &workspace,
        tools::TOOL_WRITE_FILE,
        json!({ "path": "notes.md", "content": "the approved content" }),
    ))
    .expect("a tool failure is a result, not an activity error");
    let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");
    assert!(
        !outcome.is_error,
        "the write must succeed: {}",
        outcome.output
    );

    assert_eq!(
        std::fs::read_to_string(workspace.join("notes.md")).expect("the target exists"),
        "the approved content"
    );

    // The scratch file is renamed, never left behind.
    let leftovers: Vec<_> = std::fs::read_dir(&workspace)
        .expect("the workspace lists")
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.contains("agentd-tmp"))
        .collect();
    assert!(leftovers.is_empty(), "scratch files remain: {leftovers:?}");
}

#[test]
fn a_turn_that_says_nothing_is_not_an_answer() {
    // A malformed content block, or an empty `content`, reaches the projection
    // as a clean `end_turn` with no text. Reporting that as a finished session
    // would present a billed non-answer as work.
    let empty = TurnReply {
        content: json!([]),
        stop_reason: "end_turn".to_string(),
        text: String::new(),
        tool_calls: Vec::new(),
    };
    assert!(
        !claude::is_usable(&empty),
        "an empty end_turn is not usable"
    );

    let spoken = TurnReply {
        text: "here is the summary".to_string(),
        ..empty.clone()
    };
    assert!(claude::is_usable(&spoken), "text makes a turn usable");

    let calling = TurnReply {
        stop_reason: "tool_use".to_string(),
        tool_calls: vec![ToolCall {
            id: "toolu_x".to_string(),
            name: tools::TOOL_READ_FILE.to_string(),
            input: json!({ "path": "." }),
        }],
        ..empty.clone()
    };
    assert!(
        claude::is_usable(&calling),
        "a tool call makes a turn usable"
    );

    // A stop reason that speaks for itself needs no content.
    let refused = TurnReply {
        stop_reason: "refusal".to_string(),
        ..empty.clone()
    };
    assert!(
        claude::is_usable(&refused),
        "a refusal reports itself and must not be re-classified"
    );

    // A turn that stopped TO CALL A TOOL must carry one. Otherwise the loop
    // takes its no-tool-calls branch and reports a finished session.
    let promised = TurnReply {
        content: json!([null]),
        stop_reason: "tool_use".to_string(),
        ..empty
    };
    assert!(
        !claude::is_usable(&promised),
        "a `tool_use` turn with no call is not usable"
    );
}

#[test]
fn a_hard_linked_database_is_refused() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let real = dir.path().join("real.db");
    let alias = dir.path().join("alias.db");
    std::fs::write(&real, b"").expect("the database file is created");
    std::fs::hard_link(&real, &alias).expect("the hard link is created");

    // `SQLite` derives its write-ahead log from the PATH. After an unclean
    // exit, opening `alias.db` reads `alias.db-wal` and never sees what
    // `real.db-wal` holds, so committed sessions become invisible.
    for name in [&real, &alias] {
        let refused = guard::acquire(name);
        let message = refused.err().unwrap_or_else(|| {
            panic!(
                "{} must be refused while it is multiply linked",
                name.display()
            )
        });
        assert!(
            message.contains("hard links"),
            "unexpected message: {message}"
        );
    }

    // One name again, and it opens.
    std::fs::remove_file(&alias).expect("the link is removed");
    assert!(
        guard::acquire(&real).is_ok(),
        "a single-named database must open"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_daemon_refuses_a_session_it_cannot_read() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("agentd.db");
    let first_socket = dir.path().join("first.sock");
    let second_socket = dir.path().join("second.sock");
    let options = |socket: &Path| daemon::Options {
        db: db.clone(),
        socket: socket.to_path_buf(),
        workspace: dir.path().join("workspace"),
        model: claude::DEFAULT_MODEL.to_string(),
        max_tokens: claude::DEFAULT_MAX_TOKENS,
        tick: Duration::from_millis(50),
        api_key: None,
    };

    // Park one session, then drop the daemon holding it.
    let first = tokio::spawn(daemon::serve(options(&first_socket)));
    await_daemon(&first_socket).await;
    let submitted = protocol::call(
        &first_socket,
        &Request::Submit {
            goal: "summarise the workspace".to_string(),
            max_turns: 6,
            approval_timeout_secs: 300,
        },
    )
    .await
    .expect("the submit is answered");
    let Response::Submitted { execution_id } = submitted else {
        panic!("unexpected answer: {submitted:?}");
    };
    await_parked(&first_socket, &execution_id).await;
    first.abort();
    drop(first.await);

    // Stand in for rows a newer daemon wrote. The fixture edits the EXECUTION
    // row, which is state rather than history: the append-only event log is
    // not touched.
    //
    // Each task below is complete APART FROM the one field named against it.
    // The startup check reads the recorded task the way the runtime reads it,
    // so a fault in any field answers with nothing. A check that read fewer
    // of them would enlist the row. The first drive would then fail to
    // deserialise the task, and the runtime would seal the session FAILED
    // where no later daemon could resume it.
    let workspace = dir.path().join("workspace").to_string_lossy().to_string();
    let task = |field: &str, value: Value| {
        let mut whole = json!({
            "goal": "summarise the workspace",
            "max_turns": 6,
            "approval_timeout_secs": 300,
            "workspace": workspace,
            "model": claude::OFFLINE_MODEL,
        });
        let object = whole.as_object_mut().expect("the task is an object");
        if value.is_null() {
            object.remove(field);
        } else {
            object.insert(field.to_string(), value);
        }
        whole.to_string()
    };
    let broken = [
        ("goal", Value::Null),
        // A goal of no characters is refused for the reason `submit` refuses
        // one. An empty text block is below the minimum the API accepts, so
        // the first live turn would end the session terminally.
        ("goal", json!("")),
        ("goal", json!("   ")),
        ("goal", json!("\t\n ")),
        // A deadline past `i64::MAX` seconds reads as the `u64` the task
        // declares. It cannot be armed as a timer, and `submit` refuses one,
        // so the recorded one is refused here.
        (
            "approval_timeout_secs",
            json!(9_223_372_036_854_775_808_u64),
        ),
        ("max_turns", Value::Null),
        ("max_turns", json!(-1)),
        ("max_turns", json!(0)),
        ("max_turns", json!(true)),
        ("max_turns", json!(4_294_967_296i64)),
        ("approval_timeout_secs", Value::Null),
        ("approval_timeout_secs", json!(-1)),
    ];

    for (field, value) in broken {
        let writer = rusqlite::Connection::open(&db).expect("the database opens");
        writer
            .execute(
                "UPDATE harvest_executions SET input_json = ?2 WHERE exec_id = ?1",
                rusqlite::params![&execution_id, task(field, value.clone())],
            )
            .expect("the input is replaced");
        drop(writer);

        // The daemon must say so rather than report readiness over a session
        // it silently dropped. Without the refusal `serve` runs until Ctrl-C,
        // so the timeout keeps a regression short.
        let refusal = tokio::time::timeout(
            Duration::from_secs(10),
            daemon::serve(options(&second_socket)),
        )
        .await
        .unwrap_or_else(|_| {
            panic!("the daemon must refuse rather than start on {field} = {value}")
        });
        let message = refusal.expect_err(&format!("{field} = {value} must refuse the start"));
        assert!(
            message.contains(&execution_id),
            "the refusal must name the row: {message}"
        );
        assert!(
            message.contains("cannot read"),
            "the refusal must say what is wrong: {message}"
        );
    }
}

#[test]
fn model_text_cannot_drive_the_terminal() {
    // A file in the workspace can tell the model what to answer, so the
    // answer is untrusted. The operator reads a pending call from this
    // output and approves it.
    let clearing = crate::visible("done\u{1b}[2K\u{1b}[1A");
    assert!(
        !clearing.contains('\u{1b}'),
        "an escape must not reach the terminal: {clearing}"
    );
    assert!(
        clearing.contains("\\u{001b}"),
        "the escape must be shown instead: {clearing}"
    );

    // OSC 52 writes the operator's clipboard.
    let clipboard = crate::visible("\u{1b}]52;c;cm0K\u{7}");
    assert!(
        !clipboard.contains('\u{1b}') && !clipboard.contains('\u{7}'),
        "a clipboard sequence must not reach the terminal: {clipboard}"
    );

    // A carriage return overwrites the line the operator already read.
    let overwrite = crate::visible("safe.md\rmalicious.md");
    assert!(
        !overwrite.contains('\r'),
        "a carriage return must not reach the terminal: {overwrite}"
    );

    // A bidirectional control reorders what is displayed, so one path reads
    // as another. The whole `Bidi_Control` set counts, and not only the
    // overrides: a single mark beside right-to-left text reorders it too.
    for control in [
        '\u{061c}', '\u{200e}', '\u{200f}', '\u{202a}', '\u{202b}', '\u{202c}', '\u{202d}',
        '\u{202e}', '\u{2066}', '\u{2067}', '\u{2068}', '\u{2069}',
    ] {
        let path = format!("notes{control}gnp.md");
        let reordered = crate::visible(&path);
        assert!(
            !reordered.contains(control),
            "a bidirectional control must not reach the terminal: {:04x}",
            control as u32
        );
    }

    // An answer keeps its own layout, and ordinary text is untouched.
    let answer = "line one\nline two\n\tindented 👩‍💻 done";
    assert_eq!(
        crate::visible(answer),
        answer,
        "a newline, a tab and a joiner are part of the answer"
    );

    // The status view is the thing an operator reads before approving, and
    // every field of it carries the model's own words.
    let view = protocol::SessionView {
        execution_id: "01JCEXEC".to_string(),
        goal: "tidy the notes\u{1b}[31m".to_string(),
        state: "RUNNING".to_string(),
        blocked_on: Some("a tool approval\r".to_string()),
        pending: Some(protocol::PendingCall {
            token: "tool_approval:1:0:toolu_a".to_string(),
            id: "toolu_a".to_string(),
            tool: "write_file\u{1b}[2K".to_string(),
            input: "{\"path\":\"notes\u{202e}gnp.md\"}".to_string(),
            truncated: false,
        }),
        answer: Some("done\u{1b}]52;c;cm0K\u{7}".to_string()),
        error: None,
    };
    for rendered in crate::session_lines(&view, Path::new("agentd.sock")) {
        assert!(
            !rendered
                .chars()
                .any(|c| c.is_control() && c != '\n' && c != '\t'),
            "a printed line must carry no control character: {rendered:?}"
        );
        assert!(
            !rendered.contains('\u{202e}'),
            "a printed line must carry no override: {rendered:?}"
        );
    }
}

#[test]
fn a_created_directory_can_be_entered_by_its_owner() {
    use std::os::unix::fs::PermissionsExt;

    // The umask is one value for the whole process, so the hostile mask runs
    // in a CHILD. A sibling test creating a file at the same moment would
    // otherwise see it, and the mode it asserts would be wrong. The child is
    // this same test binary, told by the marker to do the second half.
    const MARKER: &str = "AGENTD_UMASK_WORKSPACE";
    let Ok(workspace) = std::env::var(MARKER) else {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let status =
            std::process::Command::new(std::env::current_exe().expect("the running test binary"))
                .args([
                    "--exact",
                    "tests::a_created_directory_can_be_entered_by_its_owner",
                ])
                .env(MARKER, dir.path())
                .status()
                .expect("the child runs");
        assert!(status.success(), "the child must pass: {status}");
        return;
    };

    // A umask that masks every owner bit. `create_dir_all` asks for 0777, so
    // what survives is 000, and nothing can be written inside the result.
    unsafe { libc::umask(0o777) };
    let workspace = Path::new(&workspace);
    let nested = workspace.join("deep/nested");
    tools::create_enterable(&nested).expect("the directories are created");

    for level in [workspace.join("deep"), nested.clone()] {
        let mode = std::fs::metadata(&level)
            .expect("the directory exists")
            .permissions()
            .mode()
            & 0o7777;
        assert_eq!(
            mode & 0o700,
            0o700,
            "{} must stay enterable by its owner, and has mode {mode:o}",
            level.display()
        );
    }

    // The point of the owner bits: a file can be written inside.
    std::fs::write(nested.join("notes.md"), "hello").expect("a write lands inside");

    // A directory that exists and cannot be entered is refused, not widened.
    // An operator can lock one deliberately, and a daemon killed between the
    // creation and the mode leaves the same thing. The two are identical on
    // disk, so the mode is left alone and the path is named instead.
    let locked = workspace.join("locked");
    std::fs::create_dir(&locked).expect("the directory is created");
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000))
        .expect("the directory is locked");
    let refusal =
        tools::create_enterable(&locked.join("child")).expect_err("a locked parent is refused");
    assert_eq!(
        refusal.kind(),
        std::io::ErrorKind::PermissionDenied,
        "the refusal must say what is wrong: {refusal}"
    );
    assert!(
        refusal.to_string().contains("locked"),
        "the refusal must name the path: {refusal}"
    );
    let kept = std::fs::metadata(&locked)
        .expect("the directory exists")
        .permissions()
        .mode()
        & 0o7777;
    assert_eq!(
        kept, 0o000,
        "a locked directory keeps the mode it was given"
    );

    // The private mask is held around the database open and the socket bind.
    // It is one value for the whole process, so a directory created by any
    // other thread in that window takes it. A mask that hid the owner's
    // execute bit would make such a directory unusable. The refusal above
    // would then fire on a directory the daemon itself caused.
    let under_mask = guard::with_private_umask(|| {
        let held = workspace.join("held");
        std::fs::create_dir(&held).expect("the directory is created");
        std::fs::metadata(&held)
            .expect("the directory exists")
            .permissions()
            .mode()
            & 0o7777
    });
    assert_eq!(
        under_mask & 0o700,
        0o700,
        "a directory created under the private mask must stay usable, \
         and has mode {under_mask:o}"
    );
    assert_eq!(
        under_mask & 0o077,
        0,
        "a directory created under the private mask must stay private, \
         and has mode {under_mask:o}"
    );

    // A directory that is narrow but USABLE is a mode an operator can mean.
    // It is left exactly as they set it.
    let narrow = workspace.join("narrow");
    std::fs::create_dir(&narrow).expect("the directory is created");
    std::fs::set_permissions(&narrow, std::fs::Permissions::from_mode(0o500))
        .expect("the directory is made read-only");
    tools::create_enterable(&narrow).expect("an existing usable directory is accepted");
    let kept = std::fs::metadata(&narrow)
        .expect("the directory exists")
        .permissions()
        .mode()
        & 0o7777;
    assert_eq!(
        kept, 0o500,
        "a usable narrow directory keeps the mode the operator chose"
    );
}

#[test]
fn the_decide_line_reaches_the_daemon_that_printed_it() {
    let view = |token: &str| protocol::SessionView {
        execution_id: "01JCEXEC".to_string(),
        goal: "tidy the notes".to_string(),
        state: "RUNNING".to_string(),
        blocked_on: None,
        pending: Some(protocol::PendingCall {
            token: token.to_string(),
            id: "toolu_a".to_string(),
            tool: "write_file".to_string(),
            input: "{}".to_string(),
            truncated: false,
        }),
        answer: None,
        error: None,
    };
    let decide = |socket: &str| {
        crate::session_lines(&view("tool_approval:1:0:toolu_a"), Path::new(socket))
            .into_iter()
            .find(|line| line.contains("decide:"))
            .expect("the decide line is printed")
    };

    // The documented setup gives each daemon its own socket. A command copied
    // out of one daemon's status must not go to another daemon, or to none.
    let named = decide("/run/agentd/project-b.sock");
    assert!(
        named.contains("--socket=/run/agentd/project-b.sock"),
        "the chosen socket must be carried: {named}"
    );

    // The submit command prints a follow-up too, and it is the same failure
    // one branch away.
    let watch = |socket: &str| {
        crate::rendered_lines(
            &Response::Submitted {
                execution_id: "01JCEXEC".to_string(),
            },
            Path::new(socket),
        )
        .into_iter()
        .find(|line| line.contains("Watch it with"))
        .expect("the watch line is printed")
    };
    let watch_named = watch("/run/agentd/project-b.sock");
    assert!(
        watch_named.contains("--socket=/run/agentd/project-b.sock"),
        "the watch command must carry the socket: {watch_named}"
    );
    assert!(
        !watch("agentd.sock").contains("--socket"),
        "the default socket needs no flag on the watch command"
    );

    // The common case stays short.
    let default = decide("agentd.sock");
    assert!(
        !default.contains("--socket"),
        "the default socket needs no flag: {default}"
    );

    // A directory with a space in its name is ordinary, and the line is made
    // to be copied into a shell.
    let spaced = decide("/home/a b/agentd.sock");
    assert!(
        spaced.contains("--socket='/home/a b/agentd.sock'"),
        "a socket a shell would split must be quoted: {spaced}"
    );

    // A late decision points at the history, and that command is the same
    // failure again. The daemon sends the id and the client builds the line.
    let late = |socket: &str| {
        crate::rendered_lines(
            &Response::Ack {
                detail: "approved, and the deadline passed".to_string(),
                history_of: Some("01JCEXEC".to_string()),
            },
            Path::new(socket),
        )
        .into_iter()
        .find(|line| line.contains("history"))
        .expect("the history line is printed")
    };
    let late_named = late("/run/agentd/project-b.sock");
    assert!(
        late_named.contains("--socket=/run/agentd/project-b.sock")
            && late_named.contains("01JCEXEC"),
        "the history command must carry the socket: {late_named}"
    );

    // The refusal an operator meets when no daemon answers names the socket
    // it tried, so the command that STARTS one must name it too. This line is
    // built in the protocol module rather than the renderer.
    let unreachable = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime")
        .block_on(protocol::call(
            Path::new("/run/agentd/project-b.sock"),
            &Request::List { before: None },
        ))
        .expect_err("no daemon listens there");
    assert!(
        unreachable.contains("agentd serve --socket=/run/agentd/project-b.sock"),
        "the start command must name the socket that failed: {unreachable}"
    );
}

/// A goal opening with a NUL is still a goal.
///
/// Rust keeps that byte through `trim`, so `submit` accepts such a goal. A
/// check that measured the goal in the database would disagree. `SQLite`
/// counts TEXT to the first NUL and stops, so such a goal measures zero
/// there. The startup check would refuse to start over a task it can read
/// perfectly well, and no daemon of this version could resume the session.
/// The check reads the goal in Rust, so the two ends cannot diverge. This
/// test holds that line where `submit` draws it.
#[test]
fn a_goal_opening_with_a_nul_is_measured_whole() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("nul.db");
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    writer
        .execute(
            "CREATE TABLE harvest_executions (exec_id TEXT, workflow_name TEXT, \
             state TEXT, input_json TEXT, output_json TEXT, error TEXT)",
            [],
        )
        .expect("the fixture table is created");
    let task = |goal: &str| {
        json!({
            "goal": goal,
            "max_turns": 4,
            "approval_timeout_secs": 300,
            "workspace": "/tmp/w",
            "model": claude::OFFLINE_MODEL,
        })
        .to_string()
    };
    for (exec, goal) in [
        ("nul-first", "\u{0}summarise the workspace"),
        ("nul-middle", "summarise\u{0}the workspace"),
        ("plain", "summarise the workspace"),
    ] {
        writer
            .execute(
                "INSERT INTO harvest_executions VALUES (?1, ?2, 'RUNNING', ?3, NULL, NULL)",
                rusqlite::params![exec, WORKFLOW_NAME, task(goal)],
            )
            .expect("the session is recorded");
    }
    // A goal that is only space is still refused, so the byte count did not
    // trade one fault for another.
    writer
        .execute(
            "INSERT INTO harvest_executions VALUES ('blank', ?1, 'RUNNING', ?2, NULL, NULL)",
            rusqlite::params![WORKFLOW_NAME, task("   ")],
        )
        .expect("the blank session is recorded");
    drop(writer);

    let reader = rusqlite::Connection::open(&db).expect("the database opens");
    let running = inspect::running(&reader, WORKFLOW_NAME).expect("the query runs");
    let goal_of = |exec: &str| {
        running
            .iter()
            .find(|row| row.exec_id == exec)
            .expect("the session is RUNNING")
            .task
            .as_ref()
            .expect("the recorded task is readable")
            .has_goal
    };
    assert!(
        goal_of("nul-first"),
        "a goal opening with a NUL must count as a goal"
    );
    assert!(
        goal_of("nul-middle"),
        "a goal holding a NUL must count as a goal"
    );
    assert!(goal_of("plain"), "an ordinary goal must count as a goal");
    assert!(
        !goal_of("blank"),
        "a goal of only space must still be refused"
    );
}

/// A recorded task `SQLite` calls readable, and Rust cannot read at all.
///
/// The two readers disagree. `json_valid` accepts a goal of `"\uD800"`. It
/// gives the goal the type `text` and a length of three bytes. A projection
/// built from those three terms calls the row readable. `serde_json` refuses
/// the unpaired surrogate, because a Rust string cannot hold one.
///
/// The startup check has to refuse the row. An enlisted session is driven,
/// the drive deserialises the same task, and the runtime seals the session
/// FAILED on that failure. No later daemon could resume it.
///
/// The test measures the OLD predicate on the same document, so it carries
/// the evidence that the row would have been enlisted.
#[test]
fn a_recorded_task_rust_cannot_read_is_never_enlisted() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("surrogate.db");
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    fixture_table(&writer);
    // The documents are written as TEXT, the way a writer of another version
    // would leave them. A Rust string cannot carry a lone surrogate, so the
    // escape is written and never built from a `Value`.
    let bad_goal = r#"{"goal":"\uD800","max_turns":4,
                       "approval_timeout_secs":300,
                       "workspace":"/tmp/w","model":"offline"}"#;
    let bad_workspace = r#"{"goal":"summarise it","max_turns":4,
                            "approval_timeout_secs":300,
                            "workspace":"\uD800","model":"offline"}"#;
    // A surrogate in a field no check reads still fails the whole document.
    let bad_spare = r#"{"goal":"summarise it","max_turns":4,
                        "approval_timeout_secs":300,
                        "workspace":"/tmp/w","model":"offline",
                        "note":"\uD800"}"#;
    for (exec, document) in [
        ("good", READABLE_TASK),
        ("bad-goal", bad_goal),
        ("bad-workspace", bad_workspace),
        ("bad-spare", bad_spare),
    ] {
        record_task(&writer, exec, document);
    }
    // One document of bytes that are not UTF-8 at all. Reading a field of it
    // as text fails the WHOLE query, which would name no row.
    let mut raw = br#"{"goal":"x"#.to_vec();
    raw.push(0xED);
    raw.extend_from_slice(br#"","max_turns":4,"approval_timeout_secs":300,"#);
    raw.extend_from_slice(br#""workspace":"/tmp/w","model":"offline"}"#);
    writer
        .execute(
            "INSERT INTO harvest_executions \
             VALUES ('raw-bytes', ?1, 'RUNNING', CAST(?2 AS TEXT), NULL, NULL)",
            rusqlite::params![WORKFLOW_NAME, raw],
        )
        .expect("the byte session is recorded");

    // The three terms the old projection read, on the document it admitted.
    let (valid, kind, length): (i64, Option<String>, Option<i64>) = writer
        .query_row(
            "SELECT json_valid(?1), json_type(?1, '$.goal'), \
                    length(cast(trim(json_extract(?1, '$.goal'), \
                                     char(9,10,13,32)) as blob))",
            [bad_goal],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("the projection answers");
    assert_eq!(valid, 1, "SQLite calls the document valid JSON");
    assert_eq!(
        kind.as_deref(),
        Some("text"),
        "SQLite calls the goal a string"
    );
    assert!(
        length.is_some_and(|bytes| bytes > 0),
        "SQLite measures the goal as saying something"
    );
    assert!(
        serde_json::from_str::<session::SessionTask>(bad_goal).is_err(),
        "Rust cannot read the same document"
    );
    drop(writer);

    let reader = rusqlite::Connection::open(&db).expect("the database opens");
    let running = inspect::running(&reader, WORKFLOW_NAME).expect("the query still answers");
    assert!(enlisted(&running, "good"), "a readable task is enlisted");
    for row in ["bad-goal", "bad-workspace", "bad-spare", "raw-bytes"] {
        assert!(
            !enlisted(&running, row),
            "{row} carries a task Rust cannot read and must not be enlisted"
        );
    }
}

/// A task of the right bytes in the wrong storage class is never enlisted.
///
/// The engine reads `input_json` straight into a `String`, so a BLOB value
/// fails there whatever it holds. A column of TEXT affinity keeps a stored
/// BLOB as a BLOB, so a damaged row can carry a perfect task in that class.
///
/// Casting to a blob hides the class. Such a row would be enlisted, and the
/// drive would fail on every tick over a session nothing ever seals.
#[test]
fn a_task_document_in_the_wrong_storage_class_is_never_enlisted() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("class.db");
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    fixture_table(&writer);
    record_task(&writer, "good", READABLE_TASK);
    writer
        .execute(
            "INSERT INTO harvest_executions \
             VALUES ('blob-class', ?1, 'RUNNING', CAST(?2 AS BLOB), NULL, NULL)",
            rusqlite::params![WORKFLOW_NAME, READABLE_TASK],
        )
        .expect("the blob session is recorded");

    // The same bytes in both rows, and only the class differs.
    let class = |exec: &str| -> String {
        writer
            .query_row(
                "SELECT typeof(input_json) FROM harvest_executions WHERE exec_id = ?1",
                [exec],
                |row| row.get(0),
            )
            .expect("the class answers")
    };
    assert_eq!(class("good"), "text", "the readable row is TEXT");
    assert_eq!(
        class("blob-class"),
        "blob",
        "a TEXT column keeps a stored BLOB as a BLOB"
    );
    // The engine's own read of the row, which is what the drive would do.
    let engine_read: Result<String, _> = writer.query_row(
        "SELECT input_json FROM harvest_executions WHERE exec_id = 'blob-class'",
        [],
        |row| row.get(0),
    );
    assert!(
        engine_read.is_err(),
        "the engine's own read of this row must fail"
    );
    drop(writer);

    let reader = rusqlite::Connection::open(&db).expect("the database opens");
    let running = inspect::running(&reader, WORKFLOW_NAME).expect("the query still answers");
    assert!(enlisted(&running, "good"), "a readable task is enlisted");
    assert!(
        !enlisted(&running, "blob-class"),
        "a task in the wrong storage class must not be enlisted"
    );
}

/// A recorded task both ends of the startup check can read.
const READABLE_TASK: &str = r#"{"goal":"summarise it","max_turns":4,
                                "approval_timeout_secs":300,
                                "workspace":"/tmp/w","model":"offline"}"#;

/// The one table the two enlistment tests read, with the columns they name.
fn fixture_table(writer: &rusqlite::Connection) {
    writer
        .execute(
            "CREATE TABLE harvest_executions (exec_id TEXT, workflow_name TEXT, \
             state TEXT, input_json TEXT, output_json TEXT, error TEXT)",
            [],
        )
        .expect("the fixture table is created");
}

/// Record one RUNNING session against a document written as TEXT.
fn record_task(writer: &rusqlite::Connection, exec: &str, document: &str) {
    writer
        .execute(
            "INSERT INTO harvest_executions VALUES (?1, ?2, 'RUNNING', ?3, NULL, NULL)",
            rusqlite::params![exec, WORKFLOW_NAME, document],
        )
        .expect("the session is recorded");
}

/// Did the startup check read a task for this session?
fn enlisted(running: &[inspect::RunningSession], exec: &str) -> bool {
    running
        .iter()
        .find(|row| row.exec_id == exec)
        .expect("the session is RUNNING")
        .task
        .is_some()
}

/// The startup check draws the blank-goal line where `submit` draws it.
///
/// Rust's `trim` removes the whole Unicode whitespace set. A goal of
/// non-breaking spaces says nothing, and `submit` refuses it. A check written
/// in SQL would remove only the characters it was given. A narrower set would
/// resume a session the other end of this invariant calls blank. Both ends
/// now run the same `trim`, and this test holds the line.
#[test]
fn a_goal_of_unicode_space_is_refused_as_submit_refuses_it() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("space.db");
    let conn = rusqlite::Connection::open(&db).expect("the database opens");
    conn.execute(
        "CREATE TABLE harvest_executions (exec_id TEXT, workflow_name TEXT, \
         state TEXT, input_json TEXT, output_json TEXT, error TEXT)",
        [],
    )
    .expect("the fixture table is created");
    drop(conn);
    let task = |goal: &str| {
        json!({
            "goal": goal,
            "max_turns": 4,
            "approval_timeout_secs": 300,
            "workspace": "/tmp/w",
            "model": claude::OFFLINE_MODEL,
        })
        .to_string()
    };
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    for (exec, goal) in [
        ("nbsp", "\u{a0}\u{a0}"),
        ("ideographic", "\u{3000}"),
        ("thin", "\u{2009}\u{2009}"),
        ("nel", "\u{85}"),
    ] {
        writer
            .execute(
                "INSERT INTO harvest_executions VALUES (?1, ?2, 'RUNNING', ?3, NULL, NULL)",
                rusqlite::params![exec, WORKFLOW_NAME, task(goal)],
            )
            .expect("the session is recorded");
    }
    // One wrapped in that space is still a goal, so the trim is a trim.
    writer
        .execute(
            "INSERT INTO harvest_executions VALUES ('wrapped', ?1, 'RUNNING', ?2, NULL, NULL)",
            rusqlite::params![WORKFLOW_NAME, task("\u{a0}do it\u{3000}")],
        )
        .expect("the session is recorded");
    drop(writer);

    let reader = rusqlite::Connection::open(&db).expect("the database opens");
    let running = inspect::running(&reader, WORKFLOW_NAME).expect("the query runs");
    let unicode_goal = |exec: &str| {
        running
            .iter()
            .find(|row| row.exec_id == exec)
            .expect("the session is RUNNING")
            .task
            .as_ref()
            .expect("the recorded task is readable")
            .has_goal
    };
    for blank in ["nbsp", "ideographic", "thin", "nel"] {
        assert!(
            !unicode_goal(blank),
            "a goal of only {blank} space must be refused, as `submit` refuses it"
        );
    }
    assert!(
        unicode_goal("wrapped"),
        "a real goal wrapped in that space is still a goal"
    );
}

/// A name that is not text is counted, and never rendered.
///
/// A filename is bytes on this platform and the model can only send a string,
/// so such an entry cannot be named through this tool. Rendering it with the
/// replacement character costs twice. The name addresses no file, and two
/// entries differing only in those bytes collapse into one. The second then
/// disappears from a walk that claims to reach everything.
#[test]
fn a_directory_entry_that_is_not_text_is_counted_and_not_named() {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;

    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");
    std::fs::write(workspace.join("good.txt"), "x").expect("a readable name");
    // Two names that differ ONLY in a byte that is not UTF-8. Lossy rendering
    // maps both to the same string, and a set keyed on that loses one.
    for raw in [b"bad\xff.txt".as_slice(), b"bad\xfe.txt".as_slice()] {
        std::fs::write(workspace.join(OsStr::from_bytes(raw)), "x").expect("a byte name");
    }

    let body = tools::activity_body(workspace.clone());
    let raw = body(tool_request(
        &workspace,
        tools::TOOL_LIST_FILES,
        json!({ "path": "." }),
    ))
    .expect("a tool failure is a result, not an activity error");
    let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");
    assert!(
        !outcome.is_error,
        "the listing must succeed: {}",
        outcome.output
    );

    assert!(
        !outcome.output.contains('\u{fffd}'),
        "a name that is not text must never be rendered: {}",
        outcome.output
    );
    assert!(
        outcome.output.lines().any(|line| line == "good.txt"),
        "a readable name is still listed: {}",
        outcome.output
    );
    // BOTH are accounted for. A lossy rendering would have collapsed them
    // into one line and reported nothing missing.
    assert!(
        outcome.output.contains("2 entries are not listed"),
        "both unnameable entries must be counted: {}",
        outcome.output
    );
}

/// A name holding the listing's delimiter is counted, and never rendered.
///
/// The listing is one entry per line. A name with a line break in it renders
/// as two lines, and a directory of `a` and `b` renders as those same two
/// lines. Neither line names the file.
///
/// The cursor of the next page is a line of this listing. Such a line would
/// also page the walk onto a name that does not exist. The entry is counted
/// instead.
#[test]
fn a_directory_entry_holding_a_line_break_is_counted_and_not_named() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");
    std::fs::write(workspace.join("a\nb"), "x").expect("a name with a line break");

    let body = tools::activity_body(workspace.clone());
    let listing = |path: &str| -> String {
        let raw = body(tool_request(
            &workspace,
            tools::TOOL_LIST_FILES,
            json!({ "path": path }),
        ))
        .expect("a tool failure is a result, not an activity error");
        let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");
        assert!(
            !outcome.is_error,
            "the listing must succeed: {}",
            outcome.output
        );
        outcome.output
    };

    // EVERY line that names an entry names one that is there. The two halves
    // of the broken name would each fail this.
    let output = listing(".");
    for line in output.lines().filter(|line| !line.starts_with("... ")) {
        assert!(
            workspace.join(line).symlink_metadata().is_ok(),
            "the listing named `{line}`, which is not an entry: {output}"
        );
    }
    assert!(
        output.contains("1 entries are not listed"),
        "the entry must be counted: {output}"
    );
    assert!(
        !output.lines().any(|line| line == "a" || line == "b"),
        "neither half of the name may be rendered: {output}"
    );

    // A real entry beside it is still listed, and the count still holds.
    std::fs::write(workspace.join("a"), "x").expect("a plain name");
    let output = listing(".");
    assert!(
        output.lines().any(|line| line == "a"),
        "a nameable entry is still listed: {output}"
    );
    assert!(
        output.contains("1 entries are not listed"),
        "the unnameable entry is still counted: {output}"
    );
}

/// Every paged query is planned as a SEEK, and none of them sorts.
///
/// A cap on the rows a query RETURNS is not a cap on the rows it reads. Two
/// shapes break that, and both were here:
///
/// `(?2 IS NULL OR seq < ?2)` is not an index bound. `SQLite` cannot know
/// which side of the `OR` holds while it plans. It finds rows by the other
/// terms and tests this one on each. A page deep in a long history walks
/// past every newer row to reach it.
///
/// An index on the workflow name cannot answer `ORDER BY rowid`. A lookup
/// through it therefore sorts every matching session in a temporary B-tree
/// before the LIMIT applies. The `+` takes the name out of the planner's
/// index choice, which leaves the rowid walk the listing already wants.
///
/// The test reads the PRODUCTION query strings and asks the database how it
/// would run them. The fixture carries the schema shapes and the index the
/// engine creates. A plan is what the engine decides, so no other assertion
/// here can stand in for it.
#[test]
fn every_paged_query_is_planned_as_a_seek() {
    let conn = rusqlite::Connection::open_in_memory().expect("the database opens");
    conn.execute_batch(
        "CREATE TABLE harvest_executions (exec_id TEXT, workflow_name TEXT, \
         workflow_id TEXT, state TEXT, input_json TEXT, output_json TEXT, error TEXT);
         CREATE INDEX idx_harvest_executions_key \
             ON harvest_executions (workflow_name, workflow_id);
         CREATE TABLE harvest_events (exec_id TEXT NOT NULL, seq INTEGER NOT NULL, \
         event_json TEXT NOT NULL, PRIMARY KEY (exec_id, seq));",
    )
    .expect("the fixture schema is created");

    let plan = |sql: &str, params: &[&dyn rusqlite::ToSql]| -> String {
        let mut statement = conn
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
            .expect("the plan is explained");
        statement
            .query_map(params, |row| row.get::<_, String>(3))
            .expect("the plan reads")
            .map(|row| row.expect("a plan line reads"))
            .collect::<Vec<_>>()
            .join(" | ")
    };

    let top = crate::inspect::no_cursor(None);
    let turn_ceiling = i64::from(u32::MAX);
    let cases = [
        (
            "events",
            crate::inspect::EVENTS_QUERY,
            vec![&"exec-1" as &dyn rusqlite::ToSql, &top, &500_i64, &240_i64],
            "seq<?",
        ),
        (
            "replies",
            crate::inspect::REPLIES_QUERY,
            vec![&"exec-1" as &dyn rusqlite::ToSql, &top, &1_i64],
            "seq<?",
        ),
        (
            // The two evidence reads are bounded the OTHER way: they look
            // only at rows after the reply in hand. A scan here would put
            // the whole log back into one `status`.
            "newer turns",
            crate::inspect::NEWER_TURN_QUERY,
            vec![&"exec-1" as &dyn rusqlite::ToSql, &7_i64, &"claude_turn"],
            "seq>?",
        ),
        (
            "unclassified events",
            crate::inspect::UNCLASSIFIED_AFTER_QUERY,
            vec![&"exec-1" as &dyn rusqlite::ToSql, &7_i64],
            "seq>?",
        ),
        (
            "sessions",
            crate::inspect::SESSIONS_QUERY,
            vec![
                &"agent_session" as &dyn rusqlite::ToSql,
                &201_i64,
                &2000_i64,
                &top,
                &crate::inspect::COUNTER_CEILING,
                &turn_ceiling,
                &crate::inspect::TIMEOUT_CEILING,
            ],
            "rowid<?",
        ),
    ];
    for (name, sql, params, bound) in cases {
        let shown = plan(sql, &params);
        assert!(
            shown.contains(bound),
            "the {name} query must seek on {bound}: {shown}"
        );
        assert!(
            !shown.contains("TEMP B-TREE"),
            "the {name} query must not sort what it reads: {shown}"
        );
        assert!(
            !shown.contains("SCAN harvest_events"),
            "the {name} query must not scan the event log: {shown}"
        );
    }

    // A later page is planned the same way as the first. That is the point
    // of standing a bound in for the absent cursor.
    let deep = plan(
        crate::inspect::EVENTS_QUERY,
        &[
            &"exec-1" as &dyn rusqlite::ToSql,
            &100_i64,
            &500_i64,
            &240_i64,
        ],
    );
    assert!(
        deep.contains("seq<?") && !deep.contains("TEMP B-TREE"),
        "a later page is a seek too: {deep}"
    );
}

/// A report is never shown with a count nobody recorded.
///
/// The listing projects an unreadable field as nothing, and a zero in its
/// place would read as a genuine result: `[end_turn after 0 turns, 0 tool
/// calls]`. An operator cannot tell that from a session that really did stop
/// after no turns.
///
/// A row with nothing readable says nothing, which is what the single status
/// does with a report it cannot deserialise. A row with only some of the
/// fields is named as unreadable, so a silence is never read as "no report
/// yet".
#[test]
fn a_listed_report_is_shown_only_when_it_reads() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("reports.db");
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    fixture_table(&writer);
    let report = |exec: &str, output: &str| {
        writer
            .execute(
                "INSERT INTO harvest_executions \
                 VALUES (?1, ?2, 'COMPLETED', ?3, ?4, NULL)",
                rusqlite::params![exec, WORKFLOW_NAME, READABLE_TASK, output],
            )
            .expect("the session is recorded");
    };
    report(
        "whole",
        r#"{"stop":"end_turn","turns":3,"tool_calls":2,"answer":"done"}"#,
    );
    // A readable stop reason beside a count that is not an integer.
    report(
        "part",
        r#"{"stop":"end_turn","turns":"many","tool_calls":2,"answer":"done"}"#,
    );
    // Nothing readable at all, which is the shape of a damaged report.
    report("none", "}{");
    // A session that ended with no text is a real outcome, not a fault.
    report(
        "quiet",
        r#"{"stop":"end_turn","turns":1,"tool_calls":0,"answer":""}"#,
    );
    drop(writer);

    let reader = inspect::open(&db).expect("the reader opens");
    let (views, _, _) = daemon::sessions(&reader, &daemon::Parked::new(), false, None)
        .expect("the listing renders");
    let shown = |exec: &str| -> Option<String> {
        views
            .iter()
            .find(|view| view.execution_id == exec)
            .expect("the session is listed")
            .answer
            .clone()
    };

    assert_eq!(
        shown("whole").as_deref(),
        Some("[end_turn after 3 turns, 2 tool calls] done"),
        "a readable report is shown as it stands"
    );
    assert_eq!(
        shown("part").as_deref(),
        Some("<unreadable report>"),
        "a report missing a field it asserts is named unreadable"
    );
    assert_eq!(
        shown("none"),
        None,
        "a report with nothing readable says nothing, as the status does"
    );
    assert_eq!(
        shown("quiet").as_deref(),
        Some("[end_turn after 1 turns, 0 tool calls] "),
        "an answer of no text is still a report"
    );
    // The zero that would have been invented is nowhere in the listing.
    assert!(
        !shown("part").unwrap_or_default().contains("0 turns"),
        "no count is invented for a field nobody recorded"
    );
}

/// One damaged row does not hide every other session.
///
/// `json_extract` on a document that is not JSON raises `malformed JSON`, and
/// that aborts the whole statement. A listing names many sessions, so one
/// damaged row would hide all of them. That includes a session waiting for a
/// decision, which is the one an operator most needs to find.
///
/// The damaged row is still named, with no goal, which is what the caller
/// already shows as an unreadable task.
#[test]
fn a_damaged_row_does_not_hide_the_listing() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("damaged.db");
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    fixture_table(&writer);
    record_task(&writer, "readable", READABLE_TASK);
    // Not JSON at all, in the two payload columns a listing reads.
    writer
        .execute(
            "INSERT INTO harvest_executions \
             VALUES ('damaged-task', ?1, 'FAILED', 'not json at all', NULL, 'it broke')",
            rusqlite::params![WORKFLOW_NAME],
        )
        .expect("the damaged row is recorded");
    writer
        .execute(
            "INSERT INTO harvest_executions \
             VALUES ('damaged-report', ?1, 'COMPLETED', ?2, '}{', NULL)",
            rusqlite::params![WORKFLOW_NAME, READABLE_TASK],
        )
        .expect("the damaged report is recorded");
    drop(writer);

    let reader = rusqlite::Connection::open(&db).expect("the database opens");
    let listed = inspect::executions(&reader, WORKFLOW_NAME, None).expect("the listing answers");
    assert_eq!(listed.len(), 3, "every row is still named");
    let row = |exec: &str| listed_row(&listed, exec);
    assert_eq!(
        row("readable").goal.as_deref(),
        Some("summarise it"),
        "a readable task still reads"
    );
    assert!(
        row("damaged-task").goal.is_none(),
        "a document that is not JSON reads as no goal"
    );
    // The error column is not JSON, so it is readable whatever the task holds.
    assert_eq!(
        row("damaged-task").error.as_deref(),
        Some("it broke"),
        "a plain column is unaffected by the guard"
    );
    assert!(
        row("damaged-report").stop.is_none() && row("damaged-report").answer.is_none(),
        "a damaged report reads as no report"
    );
    assert_eq!(
        row("damaged-report").goal.as_deref(),
        Some("summarise it"),
        "the task of that row is readable, and is still read"
    );
}

/// Valid JSON that this row cannot be read from is read as nothing.
///
/// Two faults hide behind valid JSON. A field can hold the WRONG TYPE: a
/// report of `{"stop":1}` is valid, and reading that integer as text aborts
/// the whole statement. A field can also hold text that Rust cannot read. A
/// JSON string of one unpaired surrogate is text to `SQLite`, and
/// `json_extract` yields bytes that are not UTF-8.
///
/// Either one would hide every valid session, which is the fault the
/// malformed-document guard was added to prevent.
#[test]
fn a_row_that_cannot_be_read_is_read_as_nothing() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("unreadable.db");
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    fixture_table(&writer);
    record_task(&writer, "readable", READABLE_TASK);
    // Valid JSON, wrong types, field by field.
    writer
        .execute(
            "INSERT INTO harvest_executions VALUES ('wrong-types', ?1, 'COMPLETED', ?2, ?3, NULL)",
            rusqlite::params![
                WORKFLOW_NAME,
                json!({
                    "goal": 7,
                    "max_turns": 4,
                    "approval_timeout_secs": 300,
                    "workspace": "/tmp/w",
                    "model": "offline",
                })
                .to_string(),
                json!({ "stop": 1, "turns": "many", "tool_calls": [], "answer": false })
                    .to_string(),
            ],
        )
        .expect("the mistyped row is recorded");
    // Text to SQLite, and not text to Rust.
    writer
        .execute(
            "INSERT INTO harvest_executions \
             VALUES ('bad-unicode', ?1, 'COMPLETED', ?2, \
                     '{\"stop\":\"\\uD800\",\"turns\":2,\"tool_calls\":1,\"answer\":\"done\"}', \
                     NULL)",
            rusqlite::params![WORKFLOW_NAME, READABLE_TASK],
        )
        .expect("the surrogate row is recorded");
    drop(writer);

    let reader = rusqlite::Connection::open(&db).expect("the database opens");
    let listed = inspect::executions(&reader, WORKFLOW_NAME, None).expect("the listing answers");
    assert_eq!(listed.len(), 3, "every row is still named");
    let row = |exec: &str| listed_row(&listed, exec);
    assert_eq!(
        row("readable").goal.as_deref(),
        Some("summarise it"),
        "the readable session is still listed"
    );

    let mistyped = row("wrong-types");
    assert!(
        mistyped.goal.is_none(),
        "a goal that is not text reads as no goal"
    );
    assert!(
        mistyped.stop.is_none() && mistyped.answer.is_none(),
        "a stop reason and an answer that are not text read as nothing"
    );
    assert!(
        mistyped.turns.is_none() && mistyped.tool_calls.is_none(),
        "counts that are not integers read as nothing"
    );

    // A stop reason Rust cannot read is no stop reason, and the readable
    // fields of that same row still read.
    let surrogate = row("bad-unicode");
    assert!(
        surrogate.stop.is_none(),
        "a stop reason that is not valid Unicode reads as nothing"
    );
    assert_eq!(
        surrogate.answer.as_deref(),
        Some("done"),
        "the readable fields of that row are still read"
    );
    assert_eq!(surrogate.turns, Some(2), "as are its counts");
}

/// A listed document in the wrong storage class reads as nothing.
///
/// `json_valid` answers 1 for JSON text stored as a BLOB, so a `json_valid`
/// guard alone admits a class the engine cannot read. The listing would then
/// print a goal from a document no drive can load. The operator reads a
/// session the runtime treats as unreadable.
///
/// This is the guard `running` already carries, applied where the listing
/// reads the same two columns.
#[test]
fn a_listed_document_in_the_wrong_storage_class_reads_as_nothing() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("listed-class.db");
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    fixture_table(&writer);
    record_task(&writer, "readable", READABLE_TASK);
    writer
        .execute(
            "INSERT INTO harvest_executions VALUES \
             ('blob-task', ?1, 'RUNNING', CAST(?2 AS BLOB), NULL, NULL)",
            rusqlite::params![WORKFLOW_NAME, READABLE_TASK],
        )
        .expect("the blob task is recorded");
    let report = r#"{"stop":"end_turn","turns":2,"tool_calls":1,"answer":"done"}"#;
    writer
        .execute(
            "INSERT INTO harvest_executions VALUES \
             ('blob-report', ?1, 'COMPLETED', ?2, CAST(?3 AS BLOB), NULL)",
            rusqlite::params![WORKFLOW_NAME, READABLE_TASK, report],
        )
        .expect("the blob report is recorded");

    // The fault these rows carry: the class is blob, and `json_valid` still
    // answers 1 over it. A `json_valid` guard on its own admits them.
    let valid = |exec: &str, column: &str| -> (String, i64) {
        writer
            .query_row(
                &format!(
                    "SELECT typeof({column}), json_valid({column}) \
                     FROM harvest_executions WHERE exec_id = ?1"
                ),
                [exec],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("the class answers")
    };
    assert_eq!(
        valid("blob-task", "input_json"),
        ("blob".to_string(), 1),
        "a stored BLOB stays a BLOB, and reads as valid JSON"
    );
    assert_eq!(
        valid("blob-report", "output_json"),
        ("blob".to_string(), 1),
        "the report column carries the same fault"
    );
    drop(writer);

    let reader = rusqlite::Connection::open(&db).expect("the database opens");
    let listed = inspect::executions(&reader, WORKFLOW_NAME, None).expect("the listing answers");
    assert_eq!(listed.len(), 3, "every row is still named");
    let row = |exec: &str| listed_row(&listed, exec);
    assert_eq!(
        row("readable").goal.as_deref(),
        Some("summarise it"),
        "a document in the right class still reads"
    );
    assert!(
        row("blob-task").goal.is_none(),
        "a task the engine cannot read is listed with no goal"
    );
    let blob_report = row("blob-report");
    assert!(
        blob_report.stop.is_none()
            && blob_report.answer.is_none()
            && blob_report.turns.is_none()
            && blob_report.tool_calls.is_none(),
        "no field of a report in the wrong class is read: {blob_report:?}"
    );
    assert_eq!(
        blob_report.goal.as_deref(),
        Some("summarise it"),
        "the task of that row is in the right class, and is still read"
    );
}

/// A counter outside the Rust range is no report.
///
/// `SessionReport` declares both counts as `u32`. A recorded `-1` or
/// `4294967296` is a report `status` refuses, and both are `integer` to
/// `json_type` AND to `typeof`. Neither guard sees the range, so the listing
/// rendered `[end_turn after -1 turns, 1 tool calls]` — a result that looks
/// genuine and that the rest of the daemon cannot read.
///
/// The bound is passed to the query FROM the Rust type, so this asserts the
/// range and not a literal.
#[test]
fn a_listed_counter_outside_the_rust_range_is_no_report() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("range.db");
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    fixture_table(&writer);
    let report = |turns: &str, calls: &str| {
        format!(r#"{{"stop":"end_turn","turns":{turns},"tool_calls":{calls},"answer":"done"}}"#)
    };
    let rows = [
        ("readable", report("2", "3")),
        ("negative", report("-1", "3")),
        ("too-wide", report("4294967296", "3")),
        ("wide-calls", report("2", "4294967296")),
        ("at-the-ceiling", report("4294967295", "0")),
    ];
    for (exec, document) in &rows {
        writer
            .execute(
                "INSERT INTO harvest_executions VALUES (?1, ?2, 'COMPLETED', ?3, ?4, NULL)",
                rusqlite::params![exec, WORKFLOW_NAME, READABLE_TASK, document],
            )
            .expect("the row is recorded");
    }

    // Why the two existing guards do not see this. Both values are
    // `integer` to each of them, and Rust still refuses the document.
    for (exec, document) in &rows {
        let (kind, class): (String, String) = writer
            .query_row(
                "SELECT json_type(output_json, '$.turns'), \
                        typeof(json_extract(output_json, '$.turns')) \
                 FROM harvest_executions WHERE exec_id = ?1",
                [exec],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("the classification answers");
        assert_eq!(
            (kind.as_str(), class.as_str()),
            ("integer", "integer"),
            "{exec} passes both existing guards"
        );
        let readable = serde_json::from_str::<session::SessionReport>(document).is_ok();
        assert_eq!(
            readable,
            exec == &"readable" || exec == &"at-the-ceiling",
            "{exec} must agree with what `status` reads"
        );
    }
    drop(writer);

    let reader = rusqlite::Connection::open(&db).expect("the database opens");
    let listed = inspect::executions(&reader, WORKFLOW_NAME, None).expect("the listing answers");
    let row = |exec: &str| listed_row(&listed, exec);
    assert_eq!(
        (row("readable").turns, row("readable").tool_calls),
        (Some(2), Some(3)),
        "a report inside the range still reads"
    );
    // The ceiling itself is inside the range, so it is not refused.
    assert_eq!(
        (
            row("at-the-ceiling").turns,
            row("at-the-ceiling").tool_calls
        ),
        (Some(u32::MAX), Some(0)),
        "the largest readable count is still read"
    );
    // Each column is guarded on its own, so the out-of-range count reads as
    // nothing and the other one still reads. The RENDERING is the layer that
    // matters to an operator, so it is asserted too.
    assert_eq!(
        (row("negative").turns, row("negative").tool_calls),
        (None, Some(3)),
        "the out-of-range count reads as nothing, and its neighbour reads"
    );
    assert_eq!(
        (row("wide-calls").turns, row("wide-calls").tool_calls),
        (Some(2), None),
        "either counter is guarded on its own"
    );

    let rendered = inspect::open(&db).expect("the reader opens");
    let (views, _, _) = daemon::sessions(&rendered, &daemon::Parked::new(), false, None)
        .expect("the listing renders");
    let shown = |exec: &str| -> Option<String> {
        views
            .iter()
            .find(|view| view.execution_id == exec)
            .expect("the session is listed")
            .answer
            .clone()
    };
    assert_eq!(
        shown("readable").as_deref(),
        Some("[end_turn after 2 turns, 3 tool calls] done"),
        "a report inside the range is shown as it stands"
    );
    for exec in ["negative", "too-wide", "wide-calls"] {
        assert_eq!(
            shown(exec).as_deref(),
            Some("<unreadable report>"),
            "{exec} must never be shown as a genuine result"
        );
    }
}

/// A field whose bytes are not text is no field, prefix or not.
///
/// A stop reason of `"end_turn\ud800"` is text to `SQLite`. `json_extract`
/// yields `end_turn` followed by `ED A0 80`, so keeping the valid prefix
/// showed a session that ended well, while `status` refused the same report.
///
/// A valid prefix is kept for ONE reason: the database cuts on bytes, and the
/// cut can land inside a character. `Utf8Error::error_len` separates the two.
/// It answers nothing only when the bytes END inside a character.
#[test]
fn a_listed_field_of_malformed_bytes_is_not_shown_as_its_prefix() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("prefix.db");
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    fixture_table(&writer);
    let bad = r#"{"stop":"end_turn\ud800","turns":2,"tool_calls":1,"answer":"done"}"#;
    writer
        .execute(
            "INSERT INTO harvest_executions VALUES \
             ('bad-suffix', ?1, 'COMPLETED', ?2, ?3, NULL)",
            rusqlite::params![WORKFLOW_NAME, READABLE_TASK, bad],
        )
        .expect("the row is recorded");

    // The bytes the guard admits, and the error that separates the two cases.
    let bytes: Vec<u8> = writer
        .query_row(
            "SELECT cast(json_extract(output_json, '$.stop') as blob) \
             FROM harvest_executions WHERE exec_id = 'bad-suffix'",
            [],
            |row| row.get(0),
        )
        .expect("the bytes answer");
    let split = std::str::from_utf8(&bytes).expect_err("the bytes are not text");
    assert_eq!(
        std::str::from_utf8(&bytes[..split.valid_up_to()]),
        Ok("end_turn"),
        "the valid prefix is a stop reason that reads as success"
    );
    assert!(
        split.error_len().is_some(),
        "a malformed sequence is not the end of the input"
    );
    assert!(
        serde_json::from_str::<session::SessionReport>(bad).is_err(),
        "`status` refuses the same report"
    );
    drop(writer);

    let reader = rusqlite::Connection::open(&db).expect("the database opens");
    let listed = inspect::executions(&reader, WORKFLOW_NAME, None).expect("the listing answers");
    let row = listed_row(&listed, "bad-suffix");
    assert!(
        row.stop.is_none(),
        "a stop reason of bytes nobody wrote must read as nothing: {row:?}"
    );
    assert_eq!(
        row.goal.as_deref(),
        Some("summarise it"),
        "the readable fields of that row are still read"
    );

    // The cut is the case a prefix is kept for. A character split by the byte
    // budget ends the input, so `error_len` answers nothing.
    let split_character = "ab\u{20ac}".as_bytes();
    let cut = &split_character[..split_character.len() - 1];
    let ending = std::str::from_utf8(cut).expect_err("the cut bytes are not text");
    assert!(
        ending.error_len().is_none(),
        "a character cut by the budget must stay readable as its prefix"
    );
}

/// A listing MARKS a field it cut, and the single status still shows it all.
///
/// A model answer routinely passes 500 characters. The listing showed the
/// first 500 as if they were the whole answer. An operator then had no reason
/// to open the single status, and no way to know there was more.
///
/// The projection reads one character past the cap, and the renderer marks
/// the cut. This is what `describe` already did for one event.
#[test]
fn a_listing_marks_a_field_it_cut() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("marked.db");
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    fixture_table(&writer);
    let cap = inspect::MAX_LISTED_CHARS as usize;
    // One character short of the cap, exactly at it, and past it.
    let long_answer = "a".repeat(cap + 700);
    let exact_goal = "g".repeat(cap);
    let short_goal = "s".repeat(cap - 1);
    let rows = [
        ("cut", exact_goal.clone(), long_answer),
        ("exact", exact_goal, "b".repeat(cap)),
        ("short", short_goal, "c".repeat(cap - 1)),
    ];
    for (exec, goal, answer) in &rows {
        writer
            .execute(
                "INSERT INTO harvest_executions VALUES (?1, ?2, 'COMPLETED', ?3, ?4, NULL)",
                rusqlite::params![
                    exec,
                    WORKFLOW_NAME,
                    json!({ "goal": goal, "workspace": "/w", "model": "m",
                            "max_turns": 4, "approval_timeout_secs": 1 })
                    .to_string(),
                    json!({ "stop": "end_turn", "turns": 2, "tool_calls": 1,
                            "answer": answer })
                    .to_string(),
                ],
            )
            .expect("the row is recorded");
    }
    drop(writer);

    let reader = inspect::open(&db).expect("the reader opens");
    let (views, _, _) = daemon::sessions(&reader, &daemon::Parked::new(), false, None)
        .expect("the listing renders");
    let view = |exec: &str| {
        views
            .iter()
            .find(|view| view.execution_id == exec)
            .expect("the session is listed")
    };

    // A field past the cap is marked. The mark is the LAST character, so an
    // operator reads it at the end of what they were given.
    let cut = view("cut");
    let answer = cut.answer.as_deref().expect("the report reads");
    assert!(
        answer.ends_with('…'),
        "an answer past the cap must be marked as cut: {}",
        &answer[answer.len().saturating_sub(40)..]
    );
    // The rendered line prefixes the report, so the answer is the tail after
    // it. Counting `a` over the whole line would also count the prefix.
    let (prefix, shown_answer) = answer
        .split_once("] ")
        .expect("the report line names its counts first");
    assert_eq!(
        shown_answer.chars().filter(|c| *c == 'a').count(),
        cap,
        "the marked answer must hold exactly the printed cap, in {prefix}]"
    );
    assert_eq!(
        shown_answer.chars().count(),
        cap + 1,
        "and the mark is the one character beyond it"
    );

    // A field exactly AT the cap ended by itself, so it carries no mark. This
    // is the boundary the extra character exists to tell apart.
    let exact = view("exact");
    assert!(
        !exact.goal.contains('…'),
        "a goal exactly at the cap is not cut, and must not be marked"
    );
    assert_eq!(
        exact.goal.chars().count(),
        cap,
        "and it is shown whole: {} characters",
        exact.goal.chars().count()
    );
    let exact_answer = exact.answer.as_deref().expect("the report reads");
    assert!(
        !exact_answer.contains('…'),
        "an answer exactly at the cap is not marked either"
    );

    // A short field is untouched.
    assert_eq!(
        view("short").goal.chars().count(),
        cap - 1,
        "a field under the cap is shown as it stands"
    );

    // The single status is what the mark points at, and the row it reads
    // holds the whole field. The cap is a LISTING bound, and not a loss.
    let row = inspect::execution(&reader, WORKFLOW_NAME, "cut")
        .expect("the row reads")
        .expect("the session is named");
    let report = serde_json::from_str::<session::SessionReport>(
        row.output_json.as_deref().expect("the report is recorded"),
    )
    .expect("the single status reads the same report");
    assert_eq!(
        report.answer.chars().count(),
        cap + 700,
        "the row a single status reads holds the whole answer"
    );
}

/// An unreadable answer is not an empty one, and the listing says which.
///
/// An empty answer is a REAL outcome: a session can end with the model
/// writing no text, and `status` reads that report. An answer that is
/// absent, not a string, or not valid Unicode is a report `status` refuses.
///
/// The projection reported all four as nothing, and the renderer turned that
/// nothing into an empty answer. So four documents the daemon reads four
/// different ways printed one identical line.
///
/// `SQLite` is why this needed a query change rather than a Rust one.
/// `substr` answers NULL over a zero-length value, so an empty field arrived
/// as the same nothing as an absent one. The cut is wrapped in
/// `coalesce(..., zeroblob(0))`, so a readable empty field is zero BYTES.
#[test]
fn an_unreadable_answer_is_not_shown_as_an_empty_one() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("answers.db");
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    fixture_table(&writer);
    let counts = r#""stop":"end_turn","turns":2,"tool_calls":1"#;
    let cases = [
        ("absent", format!("{{{counts}}}")),
        ("number", format!(r#"{{{counts},"answer":7}}"#)),
        ("surrogate", format!(r#"{{{counts},"answer":"\ud800"}}"#)),
        ("empty", format!(r#"{{{counts},"answer":""}}"#)),
        ("whole", format!(r#"{{{counts},"answer":"done"}}"#)),
    ];
    for (exec, document) in &cases {
        writer
            .execute(
                "INSERT INTO harvest_executions VALUES (?1, ?2, 'COMPLETED', ?3, ?4, NULL)",
                rusqlite::params![exec, WORKFLOW_NAME, READABLE_TASK, document],
            )
            .expect("the row is recorded");
    }
    drop(writer);

    let reader = inspect::open(&db).expect("the reader opens");
    let listed = inspect::executions(&reader, WORKFLOW_NAME, None).expect("the listing answers");
    let (views, _, _) = daemon::sessions(&reader, &daemon::Parked::new(), false, None)
        .expect("the listing renders");

    for (exec, document) in &cases {
        let readable = serde_json::from_str::<session::SessionReport>(document).is_ok();
        let projected = listed_row(&listed, exec).answer.clone();
        let shown = views
            .iter()
            .find(|view| view.execution_id == *exec)
            .expect("the session is listed")
            .answer
            .clone();

        // The property: the listing and the single status agree about whether
        // this report reads at all. Anything else is the two views
        // disagreeing about the same document.
        assert_eq!(
            projected.is_some(),
            readable,
            "{exec}: the projection must agree with what `status` reads"
        );
        assert_eq!(
            shown.as_deref() == Some("<unreadable report>"),
            !readable,
            "{exec}: the rendering must agree too, and showed {shown:?}"
        );
    }

    // An EMPTY answer keeps its own reading, and is not merely "readable".
    // This is the case the fix had to preserve rather than sweep up.
    assert_eq!(
        listed_row(&listed, "empty").answer.as_deref(),
        Some(""),
        "an answer of no text reads as an empty answer"
    );
    let quiet = views
        .iter()
        .find(|view| view.execution_id == "empty")
        .expect("the session is listed");
    assert_eq!(
        quiet.answer.as_deref(),
        Some("[end_turn after 2 turns, 1 tool calls] "),
        "and it is still shown as a genuine report"
    );
}

/// A damaged identity does not hide every other session.
///
/// `exec_id` and `state` were read straight into a `String`, so one row in
/// the wrong storage class aborted the WHOLE listing with `Invalid column
/// type Blob at index: 0`. Every readable session then disappeared, which is
/// the fault the payload projections beside them already refused.
///
/// A row with no readable id is still LISTED. An operator can see that it
/// exists, and no command can name it, because it holds no id to name.
/// Hiding it would deny them both.
#[test]
fn a_damaged_identity_does_not_hide_every_session() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("identity.db");
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    fixture_table(&writer);
    record_task(&writer, "readable", READABLE_TASK);
    writer
        .execute(
            "INSERT INTO harvest_executions \
             VALUES (CAST('blob-id' AS BLOB), ?1, 'RUNNING', ?2, NULL, NULL)",
            rusqlite::params![WORKFLOW_NAME, READABLE_TASK],
        )
        .expect("the blob id is recorded");
    writer
        .execute(
            "INSERT INTO harvest_executions VALUES ('num-state', ?1, 7, ?2, NULL, NULL)",
            rusqlite::params![WORKFLOW_NAME, READABLE_TASK],
        )
        .expect("the numeric state is recorded");

    // The class each damaged row carries, measured before it is read. A TEXT
    // column keeps a stored BLOB, and affinity turns a stored integer into
    // text, so only the id is in the wrong class here.
    let class = |column: &str, exec: &str| -> String {
        writer
            .query_row(
                &format!("SELECT typeof({column}) FROM harvest_executions WHERE rowid = ?1"),
                [exec],
                |row| row.get(0),
            )
            .expect("the class answers")
    };
    assert_eq!(
        class("exec_id", "2"),
        "blob",
        "the second row holds a BLOB id"
    );
    assert_eq!(
        class("state", "3"),
        "text",
        "affinity converts a stored integer, so that row is readable"
    );
    drop(writer);

    let reader = inspect::open(&db).expect("the reader opens");
    let listed = inspect::executions(&reader, WORKFLOW_NAME, None).expect("the listing answers");
    assert_eq!(listed.len(), 3, "every row is still named");
    assert_eq!(
        listed_row(&listed, "readable").goal.as_deref(),
        Some("summarise it"),
        "a readable session still reads"
    );

    // The damaged row is present, and its id reads as nothing.
    let damaged = listed
        .iter()
        .find(|row| row.exec_id.is_none())
        .expect("the damaged row is listed");
    assert!(
        damaged.goal.is_some(),
        "the readable fields of that row are still read: {damaged:?}"
    );

    // The rendering NAMES it rather than dropping it, and offers no call on
    // it: nothing could name that session to approve or deny.
    let (views, _, _) = daemon::sessions(&reader, &daemon::Parked::new(), false, None)
        .expect("the listing renders");
    assert_eq!(views.len(), 3, "every row is still shown");
    let shown = views
        .iter()
        .find(|view| view.execution_id == "<unreadable id>")
        .expect("the damaged row is shown");
    assert!(
        shown.pending.is_none(),
        "no decision is offered on a session nothing can name"
    );
}

/// A model name a printed command cannot carry is refused.
///
/// A session records the model it runs on, and a daemon started on another
/// model prints a `--model` command to resume it. Quoting made that command
/// one shell word, and it cannot make a character visible. This is the check
/// the workspace path already carries, applied to the twin I missed.
///
/// The TRIM does not reach this. It removes the whitespace at the ENDS, so an
/// interior tab or newline passed straight through it.
#[test]
fn a_model_no_printed_command_can_carry_is_refused() {
    for name in ["claude\ropus", "claude\topus", "claude\nopus"] {
        let (_, signal) = crate::shutdown::channel();
        let refused = claude::ModelConfig::new(
            Some("sk-ant-example".to_string()),
            name,
            claude::DEFAULT_MAX_TOKENS,
            signal,
        )
        .err()
        .expect("the model name must be refused");
        assert!(
            refused.contains("no command this daemon prints can carry"),
            "unexpected message for {name:?}: {refused}"
        );
        // The refusal names the model on ONE line, which is the property it
        // exists to protect.
        assert_eq!(
            crate::failure(&refused).lines().count(),
            1,
            "the refusal itself must stay on one line: {refused}"
        );
    }

    // A name that only needs QUOTING is still accepted, and is recorded
    // verbatim after the trim.
    for name in ["claude-opus-5", " claude-opus-5\n", "my model"] {
        let (_, signal) = crate::shutdown::channel();
        let config = claude::ModelConfig::new(
            Some("sk-ant-example".to_string()),
            name,
            claude::DEFAULT_MAX_TOKENS,
            signal,
        )
        .expect("an ordinary model name must be accepted");
        assert_eq!(
            config.identity(),
            name.trim(),
            "the recorded identity is the trimmed name"
        );
    }
}

/// A damaged reply does not hide the call an operator waits on.
///
/// `tool_calls` was extracted without a type test. A recorded
/// `"tool_calls": 1` yields an INTEGER, and reading that as text aborted the
/// whole page. `pending_call` turned the error into "no call". A session was
/// then named as WAITING FOR APPROVAL while showing no call and no command.
/// The operator is asked to decide and given nothing.
///
/// The damaged replies here are NEWER than the awaited one, which is the
/// order that matters: the search walks replies newest first.
#[test]
fn a_damaged_reply_does_not_hide_the_awaited_call() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("replies-damaged.db");
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    writer
        .execute_batch(
            "CREATE TABLE harvest_events (exec_id TEXT, seq INTEGER, event_json TEXT, \
             PRIMARY KEY (exec_id, seq));",
        )
        .expect("the fixture schema is created");
    let wanted = json!({
        "type": "ActivityCompleted",
        "data": { "output": {
            "stop_reason": "tool_use",
            "tool_calls": [{
                "id": "toolu_wanted",
                "name": "write_file",
                "input": { "path": "notes.md", "content": "x" },
            }],
        }},
    })
    .to_string();
    writer
        .execute(
            "INSERT INTO harvest_events VALUES ('e', 0, ?1)",
            rusqlite::params![wanted],
        )
        .expect("the awaited reply is recorded");

    // Each of these sits AFTER the awaited reply, so the search meets it
    // first. Valid JSON, and not an array; then a row that is not JSON.
    let damaged = [
        r#"{"type":"ActivityCompleted","data":{"output":{"stop_reason":"tool_use","tool_calls":1}}}"#,
        r#"{"type":"ActivityCompleted","data":{"output":{"stop_reason":"tool_use","tool_calls":"x"}}}"#,
        "not json at all",
    ];
    for (offset, document) in damaged.iter().enumerate() {
        writer
            .execute(
                "INSERT INTO harvest_events VALUES ('e', ?1, ?2)",
                rusqlite::params![i64::try_from(offset).unwrap() + 1, document],
            )
            .expect("the damaged reply is recorded");
    }

    // An array whose BYTES are not text. This one needs RAW bytes: an
    // escaped surrogate is re-serialised as its escape, so the array text
    // stays ASCII. Only a raw sequence reaches Rust as damage.
    let mut raw: Vec<u8> = br#"{"type":"ActivityCompleted","data":{"output":"#.to_vec();
    raw.extend_from_slice(br#"{"stop_reason":"tool_use","tool_calls":[{"id":""#);
    raw.extend_from_slice(&[0xED, 0xA0, 0x80]);
    raw.extend_from_slice(br#""}]}}}"#);
    writer
        .execute(
            "INSERT INTO harvest_events VALUES ('e', 9, ?1)",
            rusqlite::params![raw],
        )
        .expect("the raw-byte reply is recorded");

    assert_the_recorded_faults(&writer);
    drop(writer);

    let reader = rusqlite::Connection::open(&db).expect("the database opens");
    assert_every_reply_is_still_named(&reader);

    // The PAGE holds every reply. The SEARCH is a different question. It
    // refuses to walk past a reply it cannot read, because the awaited call
    // may be in that reply. A tool-use id is unique only within one reply.
    // `a_search_refuses_to_walk_past_a_reply_it_cannot_read` states why.
    let signal = session::approval_signal(0, 0, "toolu_wanted");
    let refused = daemon::pending_call(&reader, "e", &signal, false)
        .expect_err("the search must not walk past a reply it cannot read");
    assert!(
        refused.contains("cannot be read"),
        "and it must say so: {refused}"
    );
}

/// A search refuses to walk past a reply it cannot read.
///
/// A tool-use id is unique within ONE reply, which `has_addressable_calls`
/// proves. Nothing makes an id unique across a run, so an older reply can
/// hold the same id for a DIFFERENT tool.
///
/// The search walks replies newest first. When the newest reply cannot be
/// read, walking on found the older call and showed ITS tool and arguments
/// beside the current approval token. The operator reads one call, pastes the
/// command beside it, and releases the call they never saw.
///
/// This was reachable only after the reply projection stopped failing closed:
/// an unreadable array became a skipped reply rather than an aborted page.
/// A fix that makes a read degrade has to say what the degraded value means
/// to every reader of it.
#[test]
fn a_search_refuses_to_walk_past_a_reply_it_cannot_read() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("stale-id.db");
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    writer
        .execute_batch(
            "CREATE TABLE harvest_events (exec_id TEXT, seq INTEGER, event_json TEXT,              PRIMARY KEY (exec_id, seq));",
        )
        .expect("the fixture schema is created");

    // An OLDER turn that reused the awaited id, for a different tool and a
    // different target. This is the call that must never be offered.
    let older = json!({
        "type": "ActivityCompleted",
        "data": { "output": {
            "stop_reason": "tool_use",
            "tool_calls": [{ "id": "toolu_same", "name": "read_file",
                             "input": { "path": "secrets.txt" } }],
        }},
    })
    .to_string();
    writer
        .execute(
            "INSERT INTO harvest_events VALUES ('e', 0, ?1)",
            rusqlite::params![older],
        )
        .expect("the older reply is recorded");
    // The NEWEST reply, which is the one the session waits on, and whose
    // calls cannot be read.
    writer
        .execute(
            "INSERT INTO harvest_events VALUES ('e', 1, ?1)",
            rusqlite::params![
                r#"{"type":"ActivityCompleted","data":{"output":{"stop_reason":"tool_use","tool_calls":1}}}"#
            ],
        )
        .expect("the unreadable reply is recorded");
    drop(writer);

    let reader = rusqlite::Connection::open(&db).expect("the database opens");

    // The two replies are told apart, which is what lets the search stop.
    let page = inspect::reply_calls(&reader, "e", None, 8).expect("the replies read");
    let calls = |seq: i64| {
        page.iter()
            .find(|reply| reply.0 == seq)
            .map(|reply| &reply.1)
    };
    assert!(
        matches!(calls(1), Some(inspect::ReplyCalls::Unreadable)),
        "the newest reply is unreadable: {page:?}"
    );
    assert!(
        matches!(calls(0), Some(inspect::ReplyCalls::Calls(_))),
        "and the older one is readable: {page:?}"
    );

    // The awaited call belongs to the NEWER turn, and its id collides.
    let signal = session::approval_signal(1, 0, "toolu_same");
    let refused = daemon::pending_call(&reader, "e", &signal, false)
        .expect_err("no call may be offered past a reply that cannot be read");
    assert!(
        refused.contains("cannot be read"),
        "the refusal must say what stopped it: {refused}"
    );

    // The rendering offers no decision, and still says the session waits.
    // A silence would read as a session with nothing to answer.
    let parked = daemon::ParkedState {
        signal: Some(signal),
        reason: "waiting for approval of write_file".to_string(),
    };
    let (pending, blocked_on) = daemon::decidable(&reader, "e", Some(&parked), false);
    assert!(
        pending.is_none(),
        "the older call must NEVER be offered: {pending:?}"
    );
    let reason = blocked_on.expect("the session still says why it is parked");
    assert!(
        reason.contains("waiting for approval of write_file") && reason.contains("cannot be read"),
        "the reason keeps the wait and names the read that stopped: {reason}"
    );
    assert!(
        !reason.contains("secrets.txt") && !reason.contains("read_file"),
        "and it never names the older call: {reason}"
    );
}

/// The search never looks past the newest reply, whatever that reply holds.
///
/// A parked session waits on a call of its NEWEST reply: the run cannot call
/// the model again until the call it parked on resolves. So a newest reply
/// that does not hold the awaited call is a fault, and not a reason to look
/// further back.
///
/// Looking further back is unsafe, for the reason
/// `a_search_refuses_to_walk_past_a_reply_it_cannot_read` gives. A tool-use
/// id is unique inside ONE reply. An older reply can hold the same id for a
/// different tool, so the operator reads one call and releases another.
///
/// An earlier fix made the unreadable reply fail closed, and kept the walk
/// for the other two cases. That was half a fix. The id argument condemns the
/// walk itself, and not only the walk past damage.
#[test]
fn a_search_never_looks_past_the_newest_reply() {
    // The older reply reuses the awaited id for a different tool and target.
    // This is the call that must never be offered.
    let older = json!({
        "type": "ActivityCompleted",
        "data": { "output": {
            "stop_reason": "tool_use",
            "tool_calls": [{ "id": "toolu_same", "name": "read_file",
                             "input": { "path": "secrets.txt" } }],
        }},
    })
    .to_string();

    // Each newest reply is READABLE, and none of them holds `toolu_same`.
    let cases = [
        (
            "asked for nothing",
            json!({ "type": "ActivityCompleted",
                    "data": { "output": { "stop_reason": "end_turn" } } }),
            "asked for no tool call",
        ),
        (
            "holds another call",
            json!({ "type": "ActivityCompleted", "data": { "output": {
                "stop_reason": "tool_use",
                "tool_calls": [{ "id": "toolu_other", "name": "write_file",
                                 "input": { "path": "notes.md", "content": "x" } }],
            }}}),
            "does not hold the call",
        ),
        (
            "holds an empty array",
            json!({ "type": "ActivityCompleted", "data": { "output": {
                "stop_reason": "tool_use", "tool_calls": [],
            }}}),
            "asked for no tool call",
        ),
    ];

    for (case, newest, expected) in cases {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let db = dir.path().join("stale-id-walk.db");
        let writer = rusqlite::Connection::open(&db).expect("the database opens");
        writer
            .execute_batch(
                "CREATE TABLE harvest_events (exec_id TEXT, seq INTEGER, event_json TEXT,              PRIMARY KEY (exec_id, seq));",
            )
            .expect("the fixture schema is created");
        writer
            .execute(
                "INSERT INTO harvest_events VALUES ('e', 0, ?1)",
                rusqlite::params![older],
            )
            .expect("the older reply is recorded");
        writer
            .execute(
                "INSERT INTO harvest_events VALUES ('e', 1, ?1)",
                rusqlite::params![newest.to_string()],
            )
            .expect("the newest reply is recorded");
        drop(writer);

        let reader = rusqlite::Connection::open(&db).expect("the database opens");

        // The hazard is reachable only because the older call IS there and IS
        // readable. The page proves both, so a refusal below is a choice and
        // not a failure to read.
        let page = inspect::reply_calls(&reader, "e", None, 8).expect("the replies read");
        let older_calls = page.iter().find(|reply| reply.0 == 0).map(|reply| &reply.1);
        assert!(
            matches!(older_calls, Some(inspect::ReplyCalls::Calls(calls))
                     if calls.iter().any(|call| call.id == "toolu_same")),
            "[{case}] the older reply holds the awaited id: {page:?}"
        );

        // The awaited call belongs to the newer turn, and its id collides.
        let signal = session::approval_signal(1, 0, "toolu_same");
        let refused = daemon::pending_call(&reader, "e", &signal, false)
            .expect_err("no call may be offered from an older reply");
        assert!(
            refused.contains(expected),
            "[{case}] the refusal must say what stopped it: {refused}"
        );
        assert!(
            !refused.contains("secrets.txt") && !refused.contains("read_file"),
            "[{case}] and never name the older call: {refused}"
        );

        // The rendering offers no decision, and still says the session waits.
        let parked = daemon::ParkedState {
            signal: Some(signal),
            reason: "waiting for approval of write_file".to_string(),
        };
        let (pending, blocked_on) = daemon::decidable(&reader, "e", Some(&parked), false);
        assert!(
            pending.is_none(),
            "[{case}] the older call must NEVER be offered: {pending:?}"
        );
        let reason = blocked_on.expect("the session still says why it is parked");
        assert!(
            reason.contains("waiting for approval of write_file") && reason.contains(expected),
            "[{case}] the reason keeps the wait and names what stopped: {reason}"
        );
        assert!(
            !reason.contains("secrets.txt") && !reason.contains("read_file"),
            "[{case}] and it never names the older call: {reason}"
        );
    }
}

/// A parked session finds its call in the newest reply, which is where it is.
///
/// The refusals above cost nothing a real run needs. This is the case the
/// daemon actually serves. The same id sits in an older reply for a different
/// tool. A search that looked back could answer with the wrong one even
/// here.
#[test]
fn a_parked_call_in_the_newest_reply_is_still_found() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("newest-call.db");
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    writer
        .execute_batch(
            "CREATE TABLE harvest_events (exec_id TEXT, seq INTEGER, event_json TEXT,              PRIMARY KEY (exec_id, seq));",
        )
        .expect("the fixture schema is created");
    for (seq, tool, path) in [
        (0_i64, "read_file", "secrets.txt"),
        (1, "write_file", "notes.md"),
    ] {
        let reply = json!({
            "type": "ActivityCompleted",
            "data": { "output": {
                "stop_reason": "tool_use",
                "tool_calls": [{ "id": "toolu_same", "name": tool,
                                 "input": { "path": path } }],
            }},
        })
        .to_string();
        writer
            .execute(
                "INSERT INTO harvest_events VALUES ('e', ?1, ?2)",
                rusqlite::params![seq, reply],
            )
            .expect("the reply is recorded");
    }
    drop(writer);

    let reader = rusqlite::Connection::open(&db).expect("the database opens");
    let signal = session::approval_signal(1, 0, "toolu_same");
    let found = daemon::pending_call(&reader, "e", &signal, false)
        .expect("the replies read")
        .expect("the awaited call is in the newest reply");
    assert_eq!(
        found.tool, "write_file",
        "the NEWEST call answers: {found:?}"
    );
    assert!(
        found.input.contains("notes.md") && !found.input.contains("secrets.txt"),
        "with its own arguments and not the older ones: {found:?}"
    );
}

/// The fault each recorded reply carries, before any of them is read.
fn assert_the_recorded_faults(writer: &rusqlite::Connection) {
    let kind = |seq: i64| -> Option<String> {
        writer
            .query_row(
                "SELECT json_type(event_json, '$.data.output.tool_calls') \
                 FROM harvest_events WHERE seq = ?1",
                [seq],
                |row| row.get(0),
            )
            .expect("the classification answers")
    };
    assert_eq!(
        kind(0).as_deref(),
        Some("array"),
        "the awaited reply is an array"
    );
    assert_eq!(
        kind(1).as_deref(),
        Some("integer"),
        "a scalar is not an array"
    );
    assert_eq!(kind(2).as_deref(), Some("text"), "nor is a string");

    // The raw-byte row passes BOTH database tests, so only the decode in
    // Rust can refuse it. That row is what the byte read exists for.
    assert_eq!(
        kind(9).as_deref(),
        Some("array"),
        "the raw-byte reply is an array to SQLite"
    );
    let raw_bytes: Option<Vec<u8>> = writer
        .query_row(
            "SELECT cast(json_extract(event_json, '$.data.output.tool_calls') as blob) \
             FROM harvest_events WHERE seq = 9",
            [],
            |row| row.get(0),
        )
        .expect("the bytes answer");
    assert!(
        raw_bytes.is_some_and(|bytes| std::str::from_utf8(&bytes).is_err()),
        "and its array bytes are not text"
    );
}

/// Every REPLY is still named, so the search can walk past the damaged ones.
///
/// The `not json at all` row is not among them: it carries no `stop_reason`,
/// so the page does not count it as a reply. Its part here is to prove the
/// WHERE clause does not raise over it.
fn assert_every_reply_is_still_named(reader: &rusqlite::Connection) {
    let page = inspect::reply_calls(reader, "e", None, 16).expect("the replies still read");
    assert_eq!(page.len(), 4, "every reply is still named: {page:?}");
    assert!(
        page.iter().all(|(seq, _)| *seq != 3),
        "a row that is not JSON is not a reply: {page:?}"
    );
    // A reply this reader cannot decode is named UNREADABLE, and not as a
    // reply that asked for nothing. The searcher must be able to tell those
    // apart: it may walk past the second, and never past the first.
    assert!(
        page.iter()
            .filter(|(seq, _)| *seq != 0)
            .all(|(_, calls)| matches!(calls, inspect::ReplyCalls::Unreadable)),
        "a reply this reader cannot decode is unreadable: {page:?}"
    );
}

/// A status that cannot read the reply SAYS so, rather than showing nothing.
///
/// The projections degrade a damaged reply on their own, so a read that still
/// fails is the query or the table. Reporting "no call" for that would name
/// a session as waiting and give the operator nothing to answer with. That is
/// the worse of the two readings.
#[test]
fn a_status_that_cannot_read_a_reply_says_so() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("no-events.db");
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    fixture_table(&writer);
    record_task(&writer, "waiting", READABLE_TASK);
    // No `harvest_events` table at all, so the reply read fails outright.
    drop(writer);

    let reader = rusqlite::Connection::open(&db).expect("the database opens");
    let signal = session::approval_signal(0, 0, "toolu_wanted");
    let read = daemon::pending_call(&reader, "waiting", &signal, false);
    assert!(
        read.is_err(),
        "the reply read must fail on this database: {read:?}"
    );

    // The parked state the last drive left behind is what names the session
    // as waiting, and it is what the operator reads.
    let parked = daemon::ParkedState {
        signal: Some(signal),
        reason: "waiting for approval of write_file".to_string(),
    };
    let (pending, blocked_on) = daemon::decidable(&reader, "waiting", Some(&parked), false);
    assert!(
        pending.is_none(),
        "no call can be offered when the reply cannot be read"
    );
    let reason = blocked_on.expect("the session still says why it is parked");
    assert!(
        reason.contains("waiting for approval of write_file") && reason.contains("cannot be read"),
        "the reason must keep the wait AND name the read that failed: {reason}"
    );
}

/// An error in the wrong storage class is no error the listing can show.
///
/// `error` is a plain column, so the guard the JSON columns carry did not
/// look at it. A BLOB holding valid UTF-8 was cast and decoded as though it
/// were an ordinary failure reason. The listing then showed a genuine-looking
/// one for a row `status` refuses to read at all.
///
/// The single status ABORTS on that row rather than degrading. That is the
/// right boundary there. The operator named one row, and the error names
/// exactly the row they asked for. The listing has to hold every other
/// session, so it degrades instead.
#[test]
fn a_listed_error_in_the_wrong_storage_class_reads_as_nothing() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("error-class.db");
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    fixture_table(&writer);
    writer
        .execute(
            "INSERT INTO harvest_executions \
             VALUES ('text-error', ?1, 'FAILED', ?2, NULL, 'it broke')",
            rusqlite::params![WORKFLOW_NAME, READABLE_TASK],
        )
        .expect("the readable failure is recorded");
    writer
        .execute(
            "INSERT INTO harvest_executions \
             VALUES ('blob-error', ?1, 'FAILED', ?2, NULL, CAST('it broke' AS BLOB))",
            rusqlite::params![WORKFLOW_NAME, READABLE_TASK],
        )
        .expect("the blob failure is recorded");

    // The same bytes in both rows, and only the class differs.
    let class = |exec: &str| -> String {
        writer
            .query_row(
                "SELECT typeof(error) FROM harvest_executions WHERE exec_id = ?1",
                [exec],
                |row| row.get(0),
            )
            .expect("the class answers")
    };
    assert_eq!(class("text-error"), "text", "the readable row is TEXT");
    assert_eq!(
        class("blob-error"),
        "blob",
        "a TEXT column keeps a stored BLOB in that class"
    );
    drop(writer);

    let reader = inspect::open(&db).expect("the reader opens");

    // The single status refuses the blob row, and the listing must not claim
    // to have read what that path cannot.
    assert!(
        inspect::execution(&reader, WORKFLOW_NAME, "blob-error").is_err(),
        "the single status refuses this row"
    );

    let listed = inspect::executions(&reader, WORKFLOW_NAME, None).expect("the listing answers");
    assert_eq!(listed.len(), 2, "both rows are still named");
    assert_eq!(
        listed_row(&listed, "text-error").error.as_deref(),
        Some("it broke"),
        "an error in the right class still reads"
    );
    let damaged = listed_row(&listed, "blob-error");
    assert!(
        damaged.error.is_none(),
        "an error the single status cannot read is listed as none: {damaged:?}"
    );
    assert_eq!(
        damaged.goal.as_deref(),
        Some("summarise it"),
        "the readable fields of that row are still read"
    );
    assert_eq!(
        damaged.state.as_deref(),
        Some("FAILED"),
        "and the row still says it failed"
    );
}

/// A failure whose reason cannot be read says so, and is not a silence.
///
/// The `typeof(error) = 'text'` gate answers SQL NULL over a damaged class,
/// and `error` already answers NULL for a row that recorded no reason. So a
/// FAILED session with a corrupt reason looked exactly like one that failed
/// with no reason recorded, while `status` calls the same row unreadable.
///
/// This is the fault the previous gate introduced. A guard that reports
/// "unreadable" as the SAME value the renderer reads as "absent" moves the
/// lie one layer down rather than removing it.
#[test]
fn a_failure_whose_reason_cannot_be_read_says_so() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("damaged-error.db");
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    fixture_table(&writer);
    writer
        .execute(
            "INSERT INTO harvest_executions \
             VALUES ('readable', ?1, 'FAILED', ?2, NULL, 'it broke')",
            rusqlite::params![WORKFLOW_NAME, READABLE_TASK],
        )
        .expect("the readable failure is recorded");
    writer
        .execute(
            "INSERT INTO harvest_executions \
             VALUES ('damaged', ?1, 'FAILED', ?2, NULL, CAST('it broke' AS BLOB))",
            rusqlite::params![WORKFLOW_NAME, READABLE_TASK],
        )
        .expect("the damaged failure is recorded");
    writer
        .execute(
            "INSERT INTO harvest_executions VALUES ('silent', ?1, 'FAILED', ?2, NULL, NULL)",
            rusqlite::params![WORKFLOW_NAME, READABLE_TASK],
        )
        .expect("the reasonless failure is recorded");
    drop(writer);

    let reader = inspect::open(&db).expect("the reader opens");
    let listed = inspect::executions(&reader, WORKFLOW_NAME, None).expect("the listing answers");

    // The projection tells the two apart, which is the fact the renderer
    // needs. Both read as no error text, and only one is damaged.
    let row = |exec: &str| listed_row(&listed, exec);
    assert!(
        row("damaged").error.is_none() && row("damaged").error_is_damaged,
        "a damaged reason reads as no text, and is marked: {:?}",
        row("damaged")
    );
    assert!(
        row("silent").error.is_none() && !row("silent").error_is_damaged,
        "a reasonless failure reads as no text, and is NOT marked: {:?}",
        row("silent")
    );
    assert!(
        !row("readable").error_is_damaged,
        "a readable reason is not marked either"
    );

    let (views, _, _) = daemon::sessions(&reader, &daemon::Parked::new(), false, None)
        .expect("the listing renders");
    let shown = |exec: &str| -> Option<String> {
        views
            .iter()
            .find(|view| view.execution_id == exec)
            .expect("the session is listed")
            .error
            .clone()
    };
    assert_eq!(
        shown("readable").as_deref(),
        Some("it broke"),
        "a readable reason is shown as it stands"
    );
    assert_eq!(
        shown("damaged").as_deref(),
        Some("<unreadable error>"),
        "a damaged reason is NAMED"
    );
    assert_eq!(
        shown("silent"),
        None,
        "and a failure that recorded no reason stays silent"
    );
}

/// One listed session, by id.
fn listed_row<'a>(
    listed: &'a [inspect::SessionSummary],
    exec: &str,
) -> &'a inspect::SessionSummary {
    listed
        .iter()
        .find(|row| row.exec_id.as_deref() == Some(exec))
        .expect("the session is listed")
}

/// A listing and a status agree about a goal holding a NUL.
///
/// `submit` accepts a goal with an embedded NUL: Rust's `trim` keeps that
/// byte, so the goal says something. The listing cut each field with
/// `substr` on TEXT, which counts to the first NUL and stops. A goal opening
/// with one measured empty, so `agentd list` showed no goal while
/// `agentd status` showed all of it.
///
/// The two views read the same row. Neither may show a goal the other one
/// does not.
#[test]
fn a_listing_shows_a_goal_that_holds_a_nul() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("nul-listing.db");
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    writer
        .execute(
            "CREATE TABLE harvest_executions (exec_id TEXT, workflow_name TEXT, \
             state TEXT, input_json TEXT, output_json TEXT, error TEXT)",
            [],
        )
        .expect("the fixture table is created");
    let goal = "\u{0}do it";
    let task = json!({
        "goal": goal,
        "max_turns": 4,
        "approval_timeout_secs": 300,
        "workspace": "/tmp/w",
        "model": "offline",
    })
    .to_string();
    writer
        .execute(
            "INSERT INTO harvest_executions VALUES ('nul', ?1, 'RUNNING', ?2, NULL, NULL)",
            rusqlite::params![WORKFLOW_NAME, task],
        )
        .expect("the session is recorded");
    drop(writer);

    let reader = rusqlite::Connection::open(&db).expect("the database opens");
    let listed = inspect::executions(&reader, WORKFLOW_NAME, None).expect("the listing answers");
    let row = listed.first().expect("the session is listed");
    assert_eq!(
        row.goal.as_deref(),
        Some(goal),
        "the listing must show the whole goal"
    );

    // The single status reads the row whole. The two views must not disagree.
    let whole = inspect::execution(&reader, WORKFLOW_NAME, "nul")
        .expect("the single-row query answers")
        .expect("the session is readable by id");
    let status_goal = serde_json::from_str::<session::SessionTask>(&whole.input_json)
        .expect("the task reads")
        .goal;
    assert_eq!(
        row.goal.as_deref(),
        Some(status_goal.as_str()),
        "the listing and the status must agree about the goal"
    );
}

/// The listing's own answer cannot be a filename.
///
/// `(no entries)` is a legal name. An empty directory answered with exactly
/// that text, so a directory holding only that one file read as an empty
/// one. The model could not tell whether the file was there, and could never
/// reach a file it could otherwise read.
///
/// The count is appended after every name, so no entry can take its place.
#[test]
fn an_empty_listing_is_not_a_filename() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");
    std::fs::create_dir(workspace.join("empty")).expect("the directory is created");
    let trap = workspace.join("trap");
    std::fs::create_dir(&trap).expect("the directory is created");
    std::fs::write(trap.join("(no entries)"), "x").expect("the legal name is written");

    let body = tools::activity_body(workspace.clone());
    let listed = |path: &str| -> String {
        let raw = body(tool_request(
            &workspace,
            tools::TOOL_LIST_FILES,
            json!({ "path": path }),
        ))
        .expect("a tool failure is a result, not an activity error");
        let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");
        assert!(
            !outcome.is_error,
            "the listing must succeed: {}",
            outcome.output
        );
        outcome.output
    };

    let empty = listed("empty");
    let trapped = listed("trap");
    assert_ne!(
        empty, trapped,
        "an empty directory must not read like one holding that name"
    );
    assert_eq!(
        empty, "... entries named: 0",
        "an empty directory names nothing and counts none"
    );
    assert!(
        trapped.lines().any(|line| line == "(no entries)"),
        "the legal name must be listed: {trapped}"
    );
    assert!(
        trapped.ends_with("... entries named: 1"),
        "the count is the last line, after the name: {trapped}"
    );

    // The named file is reachable, which is what the ambiguity cost.
    let raw = body(tool_request(
        &workspace,
        tools::TOOL_READ_FILE,
        json!({ "path": "trap/(no entries)" }),
    ))
    .expect("a tool failure is a result, not an activity error");
    let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");
    assert!(
        !outcome.is_error,
        "the listed name must read: {}",
        outcome.output
    );
    assert_eq!(outcome.output, "x", "the file's own bytes come back");
}

/// A capped directory listing reaches every entry.
///
/// `read_dir` gives no order, so a truncated READ returns an arbitrary subset
/// and the same call returns that same subset again. Everything outside it is
/// unreachable to the model, which has only a path to ask with.
///
/// The selection is bounded instead: the smallest names after the cursor. The
/// page is therefore in order, and the cursor walks the whole directory.
#[test]
fn a_capped_listing_walks_the_whole_directory() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");
    // Enough to overflow one page, named so that sorted order is known.
    let total = tools::MAX_ENTRIES + 5;
    for index in 0..total {
        std::fs::write(workspace.join(format!("f{index:04}.txt")), "x").expect("a file");
    }

    let body = tools::activity_body(workspace.clone());
    let call = |after: Option<&str>| -> String {
        let mut input = json!({ "path": "." });
        if let Some(after) = after {
            input["after"] = json!(after);
        }
        let raw = body(tool_request(&workspace, tools::TOOL_LIST_FILES, input))
            .expect("a tool failure is a result, not an activity error");
        let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");
        assert!(
            !outcome.is_error,
            "the listing must succeed: {}",
            outcome.output
        );
        outcome.output
    };

    let first = call(None);
    let named: Vec<&str> = first.lines().filter(|line| line.starts_with('f')).collect();
    assert_eq!(
        named.len(),
        tools::MAX_ENTRIES,
        "the first page holds one page of entries"
    );
    assert_eq!(
        named[0], "f0000.txt",
        "the page is the SMALLEST names, and not an arbitrary subset"
    );
    assert!(
        first.contains("after"),
        "a truncated listing must name the cursor that continues it: {first}"
    );

    // The cursor reaches the entries the first page left out, which a capped
    // read with no cursor could never do.
    let last = named.last().expect("the page is not empty");
    let second = call(Some(last));
    let rest: Vec<&str> = second
        .lines()
        .filter(|line| line.starts_with('f'))
        .collect();
    assert_eq!(rest.len(), 5, "the rest of the directory is reachable");
    assert_eq!(
        rest[0],
        format!("f{:04}.txt", tools::MAX_ENTRIES),
        "the second page starts after the cursor"
    );
}

/// The words a shell hands to the program, with the quoting removed.
///
/// The split is on the spaces OUTSIDE the quotes, so this reads a printed
/// line the way a shell reads it. A test using it asserts nothing about how a
/// flag is spelled. A value printed as its own word arrives as its own
/// argument, which is the case that fails.
fn words(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut word = String::new();
    let mut quoted = false;
    let mut started = false;
    for character in line.chars() {
        match character {
            '\'' => {
                quoted = !quoted;
                started = true;
            }
            ' ' if !quoted => {
                if started {
                    out.push(std::mem::take(&mut word));
                    started = false;
                }
            }
            other => {
                word.push(other);
                started = true;
            }
        }
    }
    if started {
        out.push(word);
    }
    out
}

/// What the daemon prints parses back to the socket it printed.
///
/// Every follow-up command names the socket, and an operator copies the line.
/// The value is attached to the flag, because a path may begin with a dash.
/// `AGENTD_SOCKET=-team.sock` puts the daemon on one. As a separate word,
/// `clap` reads that value as more options. This argument sets no
/// `allow_hyphen_values`, and an attached value needs none.
///
/// The test is the round trip, and not the spelling. The printed flag goes
/// back through the real parser, and the path that comes out must be the one
/// that went in.
#[test]
fn a_printed_socket_flag_parses_back_to_the_same_socket() {
    use clap::Parser;

    for raw in [
        "/run/agentd/project-b.sock",
        "/home/a b/agentd.sock",
        "-team.sock",
        "--socket.sock",
        "./-team.sock",
        "-",
    ] {
        let printed = protocol::socket_flag(Path::new(raw));
        let mut argv = vec!["agentd".to_string()];
        argv.extend(words(&printed));
        argv.push("list".to_string());
        let cli = crate::Cli::try_parse_from(&argv)
            .unwrap_or_else(|e| panic!("the printed flag must parse:{printed} -> {e}"));
        assert_eq!(
            cli.socket,
            Path::new(raw),
            "the parsed socket must be the printed one:{printed}"
        );
    }

    // The default socket prints no flag at all, so the common line stays
    // short. See `protocol::DEFAULT_SOCKET`.
    assert_eq!(
        protocol::socket_flag(Path::new(protocol::DEFAULT_SOCKET)),
        "",
        "the default socket needs no flag"
    );
}

/// A status offers no approval once the deadline has passed.
///
/// The parked state is what the last drive left behind, and it says nothing
/// about the clock. A deadline can pass between that drive and the next tick,
/// and `--tick-ms` decides how wide that window is. `approve` refuses a wait
/// whose deadline has fired, so a status that still printed the approval
/// advertised a command the daemon rejects.
///
/// The runtime is not driven here, so nothing clears the wait. The deadline
/// passes by the clock alone, which is the case the tick cannot cover.
#[tokio::test]
async fn a_status_offers_no_approval_past_the_deadline() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");
    let db = dir.path().join("agentd.db");
    let calls = Arc::new(AtomicUsize::new(0));
    let mut rt = runtime(&db, &workspace, &calls);

    // One second, so the deadline passes while the run stays parked.
    let brief = json!({
        "goal": "summarise the workspace",
        "max_turns": 6,
        "approval_timeout_secs": 1,
        "workspace": workspace.to_str().expect("the workspace path is UTF-8"),
        "model": claude::OFFLINE_MODEL,
    });
    let exec = rt
        .start_workflow(WORKFLOW_NAME, brief)
        .expect("the session starts");
    let signal = drive_to_approval(&mut rt, exec).await;
    let exec_id = exec.to_string();
    let parked = daemon::ParkedState {
        reason: "waiting for a tool approval".to_string(),
        signal: Some(signal),
    };

    let reader = inspect::open(&db).expect("the reader opens");
    // While the deadline still has time, the call is shown.
    let (pending, reason) = daemon::decidable(&reader, &exec_id, Some(&parked), false);
    assert!(
        pending.is_some(),
        "a live wait must still show its pending call"
    );
    assert_eq!(
        reason.as_deref(),
        Some("waiting for a tool approval"),
        "the parked reason is shown as it stands"
    );

    // The wait is never driven again, so only the clock moves.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let (gone, expired) = daemon::decidable(&reader, &exec_id, Some(&parked), false);
    assert!(
        gone.is_none(),
        "a wait past its deadline must offer no approval"
    );
    let expired = expired.expect("the status still says why it is parked");
    assert!(
        expired.contains("deadline") && expired.contains("denies it"),
        "the status must say why there is nothing to decide: {expired}"
    );
}

/// A log field carries nothing a terminal would act on.
///
/// The daemon's log holds text the model wrote (a session's answer) and text
/// the API wrote (up to 400 characters of an error body). That log goes to a
/// terminal, and `tracing` does not escape a field.
///
/// The newline is escaped here, unlike in `visible`. A log line is ONE line,
/// and a newline inside a field would split it into a second entry that
/// nothing wrote.
#[test]
fn a_log_field_obeys_nothing() {
    let forged = "done\u{1b}]52;c;cm0K\u{7}\nINFO\tforged entry\r\u{202e}";
    let escaped = crate::one_line(forged);
    assert!(
        !escaped.chars().any(crate::is_obeyed),
        "the field must obey nothing: {escaped}"
    );
    assert!(
        !escaped.contains('\n') && !escaped.contains('\t'),
        "one field stays on one line, and no column is moved: {escaped}"
    );
    assert!(
        escaped.starts_with("done") && escaped.contains("forged entry"),
        "the text an operator needs is still readable: {escaped}"
    );
    // `visible` keeps the newline, which is why the log needs its own sink.
    assert!(
        crate::visible(forged).contains('\n') && crate::visible(forged).contains('\t'),
        "a printed message may hold several lines, and use a tab for layout"
    );

    // Every log field that carries such text goes through it. The guard reads
    // the source, because a `tracing` call writes to a subscriber this suite
    // does not install.
    let source = include_str!("daemon.rs");
    for field in [
        "output = %",
        "error = %",
        "path = %",
        "db = %",
        "workspace = %",
    ] {
        for line in source.lines().filter(|line| line.contains(field)) {
            assert!(
                line.contains("one_line("),
                "an untrusted log field must be escaped: {line}"
            );
        }
    }
    // The socket path is the one field logged as it stands. `printable`
    // refuses a socket path that carries any of this before a command runs,
    // so there is nothing left for a sink to escape.
    assert!(
        source.contains("socket = %options.socket.display()"),
        "the socket path is logged as itself, because it is validated"
    );
}

/// One socket path has one daemon, whatever database each one holds.
///
/// The database lock cannot stand in for this. Two daemons on different
/// databases contend for neither the file nor the task reclaim. Both could
/// therefore find one stale socket refused and decide to replace it. The
/// first removes it and binds; the second then unlinks the live socket the
/// first is listening on and binds its own. The first keeps running,
/// unreachable, and never learns.
///
/// The loser now exits instead, naming the socket, and the winner stays
/// reachable.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_daemon_cannot_take_a_socket_another_one_holds() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let socket = dir.path().join("shared.sock");
    let options = |db: &str| daemon::Options {
        db: dir.path().join(db),
        socket: socket.clone(),
        workspace: dir.path().join("workspace"),
        model: claude::DEFAULT_MODEL.to_string(),
        max_tokens: claude::DEFAULT_MAX_TOKENS,
        tick: Duration::from_millis(50),
        api_key: None,
    };

    let first = tokio::spawn(daemon::serve(options("first.db")));
    await_daemon(&socket).await;

    // A second daemon, its own database, the same socket.
    let refusal =
        tokio::time::timeout(Duration::from_secs(10), daemon::serve(options("second.db")))
            .await
            .expect("the second daemon must refuse rather than start");
    let message = refusal.expect_err("the second daemon must not take the socket");
    assert!(
        message.contains("another daemon holds the socket"),
        "the refusal must say what is held: {message}"
    );
    assert!(
        message.contains(socket.to_str().expect("the socket path is UTF-8")),
        "the refusal must name the socket: {message}"
    );

    // The first daemon is still the one behind the name.
    let answer = protocol::call(&socket, &Request::List { before: None })
        .await
        .expect("the first daemon still answers");
    assert!(
        matches!(answer, Response::Sessions { .. }),
        "unexpected answer: {answer:?}"
    );

    first.abort();
    drop(first.await);

    // The descriptor IS the lock, so dropping it frees the socket for a later
    // daemon. This is asserted on the guard itself. Aborting `serve` drops its
    // locks, and the accept loop it spawned keeps the listener. The daemon
    // path therefore cannot show the release without a second process.
    let held = guard::acquire_socket(&socket).expect("the lock is free again");
    let denied = guard::acquire_socket(&socket);
    assert!(
        denied.is_err(),
        "a held socket lock must refuse a second holder"
    );
    drop(held);
    assert!(
        guard::acquire_socket(&socket).is_ok(),
        "dropping the lock frees the socket"
    );
}

/// A socket path no printed command can carry is refused.
///
/// Being UTF-8 is not enough. Every printed line leaves through `visible`,
/// which rewrites a character a terminal would act on. A path holding one is
/// printed as an escape, and the copied command names another socket.
///
/// A NEWLINE and a TAB are refused too, and for the opposite reason:
/// `visible` keeps both, because a printed message uses them for layout.
/// Every printed command names the socket on ONE line, and an operator reads
/// that socket back off the screen. A newline splits the line and leaves an
/// unterminated quote. A tab is drawn as the gap to the next tab stop, and a
/// copy of that gap commonly carries spaces.
///
/// The refusal itself is a printed line. It names the path, so it must leave
/// through the same renderer: the last assertion holds the message an
/// operator actually reads.
#[test]
fn a_socket_path_no_printed_command_can_carry_is_refused() {
    use clap::Parser;

    for raw in [
        "/tmp/a\u{1b}[2K.sock",
        "/tmp/a\u{202e}b.sock",
        "/tmp/a\u{0}b.sock",
        "/tmp/a\nb.sock",
        // A tab is not its own text on screen. A terminal draws the gap to
        // the next tab stop, and a copy of that gap commonly carries spaces.
        // The line an operator reads is not the line they copy.
        "/tmp/a\tb.sock",
        // `OSC 52` writes the operator's clipboard, so the refusal of this
        // path must not emit it while saying so.
        "/tmp/a\u{1b}]52;c;cm0K\u{7}.sock",
    ] {
        let cli = crate::Cli::try_parse_from(["agentd", "--socket", raw, "list"])
            .expect("clap accepts the text; the refusal is the daemon's own");
        let refused = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime")
            .block_on(crate::run(cli));
        let message = refused.expect_err("a path no printed command can carry must be refused");
        assert!(
            message.contains("no command this daemon prints"),
            "the refusal must name the reason: {message}"
        );
        // The line an operator READS. A refusal that obeyed the characters it
        // refuses would do the damage it exists to prevent.
        let printed = crate::failure(&message);
        assert!(
            !printed.chars().any(crate::breaks_one_line),
            "the printed refusal must obey nothing, and stay on one line: {printed:?}"
        );
    }

    // An ordinary path is still served, so the check refuses only what the
    // renderer would rewrite.
    let plain = crate::Cli::try_parse_from(["agentd", "--socket", "/tmp/plain.sock", "list"])
        .expect("clap accepts the path");
    let answered = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime")
        .block_on(crate::run(plain));
    let message = answered.expect_err("no daemon listens there");
    assert!(
        message.contains("cannot reach the daemon"),
        "an ordinary path must reach the connect attempt: {message}"
    );
}

/// A socket is replaced only when nothing is proved to be listening.
///
/// The reclaim removes a name and binds over it. Doing that to a LIVE socket
/// leaves the daemon behind it running with nothing able to reach it. Only an
/// answer that proves the name has no listener may license the removal.
///
/// A refusal and a missing entry prove it. A permission error does not, and
/// that is the reported case: another user's live socket in a shared
/// directory. Exhausted file descriptors have the same shape on a socket this
/// daemon owns.
#[test]
fn only_a_proven_absence_licenses_a_reclaim() {
    use std::io::{Error, ErrorKind};

    for proof in [ErrorKind::ConnectionRefused, ErrorKind::NotFound] {
        assert!(
            daemon::proves_nothing_listens(&Error::new(proof, "probe")),
            "{proof:?} proves the name has no listener"
        );
    }
    // Every other answer leaves the question open, so the name is not known
    // to be free and the socket must stay.
    for open in [
        ErrorKind::PermissionDenied,
        ErrorKind::ConnectionAborted,
        ErrorKind::TimedOut,
        ErrorKind::WouldBlock,
        ErrorKind::Other,
    ] {
        assert!(
            !daemon::proves_nothing_listens(&Error::new(open, "probe")),
            "{open:?} does not prove the name has no listener"
        );
    }
}

/// A restored wait is the ARMED one, and not one already answered.
///
/// Two tables decide this, and the backend's own rule is that an armed but
/// unfired timer proves a wait. A fired timer is an approval that ran out of
/// time, and a timed-out wait carries no answer either. Reading every timer
/// would therefore restore the EXPIRED call of an earlier turn.
///
/// A decision is staged in `harvest_signals` when it is sent, and its event
/// is appended later. A daemon that stopped between the two holds a decision
/// that wins on the next drive, so the wait must not come back.
#[test]
fn a_restored_wait_is_armed_and_unanswered() {
    // A fixed clock, so a deadline can be placed on either side of it.
    const NOW: i64 = 1_000;

    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("waits.db");
    let conn = rusqlite::Connection::open(&db).expect("the database opens");
    conn.execute_batch(
        "CREATE TABLE harvest_timers (timer_id TEXT, exec_id TEXT, fire_at INTEGER, \
         fired INTEGER NOT NULL DEFAULT 0, arm_seq INTEGER, \
         PRIMARY KEY (exec_id, timer_id)); \
         CREATE TABLE harvest_events (exec_id TEXT, seq INTEGER, event_json TEXT, \
         PRIMARY KEY (exec_id, seq)); \
         CREATE TABLE harvest_signals (signal_seq INTEGER PRIMARY KEY AUTOINCREMENT, \
         exec_id TEXT NOT NULL, name TEXT NOT NULL, payload_json TEXT NOT NULL, \
         delivered INTEGER NOT NULL DEFAULT 0, received_at INTEGER NOT NULL DEFAULT 0)",
    )
    .expect("the fixture tables are created");

    let expired = "tool_approval:1:0:toolu_first";
    let armed = "tool_approval:2:0:toolu_second";
    let overdue = "tool_approval:3:0:toolu_third";
    // The earlier call timed out, so its timer stays behind as FIRED. The
    // current call waits on an armed timer.
    conn.execute(
        "INSERT INTO harvest_timers VALUES (?1, 'e', 10, 1, 1)",
        [format!("__signal_timeout:1:{expired}")],
    )
    .expect("the expired timer is recorded");
    conn.execute(
        "INSERT INTO harvest_timers VALUES (?1, 'e', 9999, 0, 2)",
        [format!("__signal_timeout:2:{armed}")],
    )
    .expect("the armed timer is recorded");

    let found = inspect::outstanding_signal(&conn, "e", NOW)
        .expect("the wait query runs")
        .expect("an armed wait must be reported");
    assert_eq!(
        found, armed,
        "the armed wait must be restored, and not the timed-out one"
    );

    // A daemon stopped PAST a deadline has had no drive in which to mark the
    // timer fired, so an overdue wait still reads as armed. Restoring it
    // would print a token that `approve` refuses every time.
    conn.execute("DELETE FROM harvest_timers WHERE exec_id = 'e'", [])
        .expect("the timers are cleared");
    conn.execute(
        "INSERT INTO harvest_timers VALUES (?1, 'e', 500, 0, 3)",
        [format!("__signal_timeout:3:{overdue}")],
    )
    .expect("the overdue timer is recorded");
    let gone = inspect::outstanding_signal(&conn, "e", NOW).expect("the wait query runs");
    assert!(
        gone.is_none(),
        "an overdue wait must not be restored, and got {gone:?}"
    );

    // The same timer with its deadline ahead is restored, so the filter is
    // the deadline and not the row.
    conn.execute(
        "UPDATE harvest_timers SET fire_at = 9999 WHERE exec_id = 'e'",
        [],
    )
    .expect("the deadline is moved ahead");
    let ahead = inspect::outstanding_signal(&conn, "e", NOW)
        .expect("the wait query runs")
        .expect("a wait with time left must be restored");
    assert_eq!(
        ahead, overdue,
        "the wait with time left is the one reported"
    );

    // Back to the armed pair for the answer checks below.
    conn.execute("DELETE FROM harvest_timers WHERE exec_id = 'e'", [])
        .expect("the timers are cleared");
    conn.execute(
        "INSERT INTO harvest_timers VALUES (?1, 'e', 9999, 0, 2)",
        [format!("__signal_timeout:2:{armed}")],
    )
    .expect("the armed timer is recorded");

    // A decision sent but not yet taken up lives only in the staged table.
    // The wait must not come back over it, or a second answer would be taken
    // for a call that is already decided.
    conn.execute(
        "INSERT INTO harvest_signals (exec_id, name, payload_json) VALUES ('e', ?1, '{}')",
        [armed],
    )
    .expect("the decision is staged");
    let staged = inspect::outstanding_signal(&conn, "e", NOW).expect("the wait query runs");
    assert!(
        staged.is_none(),
        "a staged decision ends the wait, and got {staged:?}"
    );

    // Once the workflow takes the decision up, the row is marked delivered
    // and the event carries it. The wait stays closed.
    conn.execute("UPDATE harvest_signals SET delivered = 1", [])
        .expect("the decision is taken up");
    let event = json!({
        "type": "SignalReceived",
        "data": { "signal_name": armed, "payload": { "approved": true } },
    });
    conn.execute(
        "INSERT INTO harvest_events VALUES ('e', 0, ?1)",
        [event.to_string()],
    )
    .expect("the delivery is appended");
    let done = inspect::outstanding_signal(&conn, "e", NOW).expect("the wait query runs");
    assert!(
        done.is_none(),
        "a delivered decision ends the wait, and got {done:?}"
    );
}

/// A restarted daemon knows what a parked session awaits before it serves.
///
/// The parked state was empty until the first drive, while the socket already
/// accepted requests and readiness was announced. A decision that arrived in
/// that window was refused as a session that is not waiting. That is the
/// sequence the restart recipe describes.
///
/// The wait is durable, so it is read rather than driven. This drives the
/// reconstruction itself. The window it closes is a race, so an end-to-end
/// test of it would pass either way on a lucky schedule.
#[tokio::test(flavor = "multi_thread")]
async fn a_parked_wait_is_read_from_the_database() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("agentd.db");
    let socket = dir.path().join("agentd.sock");
    let served = tokio::spawn(daemon::serve(daemon::Options {
        db: db.clone(),
        socket: socket.clone(),
        workspace: dir.path().join("workspace"),
        model: claude::DEFAULT_MODEL.to_string(),
        max_tokens: claude::DEFAULT_MAX_TOKENS,
        tick: Duration::from_millis(50),
        api_key: None,
    }));
    await_daemon(&socket).await;
    let submitted = protocol::call(
        &socket,
        &Request::Submit {
            goal: "summarise the workspace".to_string(),
            max_turns: 6,
            approval_timeout_secs: 300,
        },
    )
    .await
    .expect("the submit is answered");
    let Response::Submitted { execution_id } = submitted else {
        panic!("unexpected answer: {submitted:?}");
    };
    await_parked(&socket, &execution_id).await;

    // The token the operator would approve, read while the daemon still holds
    // its own parked state.
    let view = protocol::call(
        &socket,
        &Request::Status {
            execution_id: execution_id.clone(),
            full: false,
        },
    )
    .await
    .expect("the status is answered");
    let Response::Session { session } = view else {
        panic!("unexpected answer: {view:?}");
    };
    let token = session.pending.expect("the call is pending").token;

    served.abort();
    drop(served.await);

    // A fresh reader, as a restarted daemon opens. The wait must be readable
    // with no drive at all, and it must be the SAME token.
    let reader = inspect::open(&db).expect("the database opens");
    let now_ms = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the clock is after the epoch")
            .as_millis(),
    )
    .expect("the clock is in range");
    let waited = inspect::outstanding_signal(&reader, &execution_id, now_ms)
        .expect("the wait query runs")
        .expect("a parked session must report its wait");
    assert_eq!(
        waited, token,
        "the wait read from the database must be the one the operator approves"
    );

    // A signal the previous daemon delivered before it stopped ends the wait.
    // The timer can outlive that, so a timer alone would report a wait that is
    // over, and this is an approval gate.
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    let next: i64 = writer
        .query_row(
            "SELECT COALESCE(MAX(seq), -1) + 1 FROM harvest_events WHERE exec_id = ?1",
            [&execution_id],
            |row| row.get(0),
        )
        .expect("the log is read");
    let delivered = json!({
        "type": "SignalReceived",
        "data": { "signal_name": waited, "payload": { "approved": true } },
    });
    writer
        .execute(
            "INSERT INTO harvest_events (exec_id, seq, event_json) VALUES (?1, ?2, ?3)",
            rusqlite::params![&execution_id, next, delivered.to_string()],
        )
        .expect("the delivery is appended");
    drop(writer);

    let after =
        inspect::outstanding_signal(&reader, &execution_id, now_ms).expect("the wait query runs");
    assert!(
        after.is_none(),
        "a delivered signal ends the wait, and got {after:?}"
    );
}

/// A capped listing stays reachable through its cursor.
///
/// The listing is capped so an old database cannot be read whole into memory.
/// Without a cursor that cap HIDES rows. An old session still waiting for a
/// decision becomes unreachable once enough newer sessions arrive, unless the
/// operator kept its execution id.
#[test]
fn a_listing_walks_back_through_its_cursor() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("listing.db");
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    writer
        .execute(
            "CREATE TABLE harvest_executions (exec_id TEXT, workflow_name TEXT, \
             state TEXT, input_json TEXT, output_json TEXT, error TEXT)",
            [],
        )
        .expect("the fixture table is created");
    for index in 0..5 {
        let task = json!({
            "goal": format!("goal {index}"),
            "max_turns": 4,
            "approval_timeout_secs": 300,
            "workspace": "/tmp/w",
            "model": claude::OFFLINE_MODEL,
        });
        writer
            .execute(
                "INSERT INTO harvest_executions VALUES (?1, ?2, 'COMPLETED', ?3, NULL, NULL)",
                rusqlite::params![format!("exec-{index}"), WORKFLOW_NAME, task.to_string()],
            )
            .expect("the session is recorded");
    }
    drop(writer);

    let reader = rusqlite::Connection::open(&db).expect("the database opens");
    let all = inspect::executions(&reader, WORKFLOW_NAME, None).expect("the listing reads");
    assert_eq!(all.len(), 5, "the fixture holds five sessions");
    assert_eq!(
        all[0].exec_id.as_deref(),
        Some("exec-0"),
        "the listing reads oldest first: {:?}",
        all[0].exec_id
    );

    // Walk back from the third row. The page before it is the first two, and
    // nothing newer.
    let cursor = all[2].row;
    let older = inspect::executions(&reader, WORKFLOW_NAME, Some(cursor)).expect("the page reads");
    let named: Vec<&str> = older
        .iter()
        .map(|row| row.exec_id.as_deref().unwrap_or_default())
        .collect();
    assert_eq!(
        named,
        vec!["exec-0", "exec-1"],
        "the cursor must read the rows BEFORE it"
    );

    // The oldest row has nothing before it, which is how a walk ends.
    let none =
        inspect::executions(&reader, WORKFLOW_NAME, Some(all[0].row)).expect("the page reads");
    assert!(
        none.is_empty(),
        "the oldest row ends the walk, and got {} rows",
        none.len()
    );
}

/// A full page carries the cursor that reads the page before it.
///
/// The query and the renderer are covered above and below. This covers the
/// DAEMON deciding to send the cursor, which neither of those reaches: a
/// reverted cursor left both of them passing.
#[test]
fn a_full_page_carries_its_cursor() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("full.db");
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    writer
        .execute(
            "CREATE TABLE harvest_executions (exec_id TEXT, workflow_name TEXT, \
             state TEXT, input_json TEXT, output_json TEXT, error TEXT)",
            [],
        )
        .expect("the fixture table is created");

    // One session past the cap, which is what makes the page full.
    let rows = inspect::MAX_LISTED_SESSIONS + 1;
    let task = json!({
        "goal": "tidy the notes",
        "max_turns": 4,
        "approval_timeout_secs": 300,
        "workspace": "/tmp/w",
        "model": claude::OFFLINE_MODEL,
    })
    .to_string();
    for index in 0..rows {
        writer
            .execute(
                "INSERT INTO harvest_executions VALUES (?1, ?2, 'COMPLETED', ?3, NULL, NULL)",
                rusqlite::params![format!("exec-{index}"), WORKFLOW_NAME, task],
            )
            .expect("the session is recorded");
    }
    drop(writer);

    let reader = rusqlite::Connection::open(&db).expect("the database opens");
    let parked = daemon::Parked::new();
    let (shown, more, older) =
        daemon::sessions(&reader, &parked, false, None).expect("the listing reads");
    assert_eq!(
        shown.len(),
        inspect::MAX_LISTED_SESSIONS as usize,
        "a full page shows the cap and no more"
    );
    assert!(more, "the table holds more than one page");
    let cursor = older.expect("a full page must carry its cursor");

    // The cursor reads the page BEFORE this one, so it must find the session
    // the page left out and not repeat one it showed.
    let before =
        inspect::executions(&reader, WORKFLOW_NAME, Some(cursor)).expect("the earlier page reads");
    assert_eq!(
        before.len(),
        1,
        "the cursor must reach the one session this page omitted"
    );
    assert_eq!(
        before[0].exec_id.as_deref(),
        Some("exec-0"),
        "the omitted session is the oldest one: {:?}",
        before[0].exec_id
    );
}

/// The listing's own continuation command carries the socket.
#[test]
fn the_listing_cursor_reaches_the_daemon_that_printed_it() {
    let view = protocol::SessionView {
        execution_id: "01JCEXEC".to_string(),
        goal: "tidy the notes".to_string(),
        state: "RUNNING".to_string(),
        blocked_on: None,
        pending: None,
        answer: None,
        error: None,
    };
    let rendered = crate::rendered_lines(
        &Response::Sessions {
            sessions: vec![view],
            more: true,
            older: Some(41),
        },
        Path::new("/run/agentd/project-b.sock"),
    );
    let hint = rendered
        .iter()
        .find(|line| line.contains("--before"))
        .expect("the continuation command is printed");
    assert!(
        hint.contains("--socket=/run/agentd/project-b.sock") && hint.contains("--before 41"),
        "the continuation command must reach the same daemon: {hint}"
    );
}

/// The flush chain holds every directory above a write.
///
/// An entry is durable only after the directory naming it is flushed. A write
/// that creates nested directories therefore needs each one above it flushed,
/// up to the workspace root.
///
/// The workspace may be named through a symlink. Both sides of the comparison
/// must be canonical. The chain otherwise collapses to the target's own
/// directory, and a crash loses the file while the history says the write
/// finished.
#[test]
fn the_flush_chain_reaches_the_root_through_a_symlink() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let real = dir.path().join("real");
    std::fs::create_dir_all(real.join("a/b")).expect("the tree is created");
    let link = dir.path().join("link");
    std::os::unix::fs::symlink(&real, &link).expect("the symlink is made");

    // The workspace is named through the link, which is how an operator who
    // keeps a stable path to a moving directory names it.
    let chain = tools::directories_to_flush(&link.join("a/b/notes.md"), &link);
    let canonical = real.canonicalize().expect("the real path resolves");

    assert_eq!(
        chain.len(),
        3,
        "the chain must hold b, a and the root: {chain:?}"
    );
    assert!(
        chain[0].ends_with("a/b") && chain[1].ends_with("a") && chain[2] == canonical,
        "the chain must run deepest first up to the root: {chain:?}"
    );

    // The directory holding the entry that names `b` is the one a collapsed
    // chain leaves out, so it is named here on its own.
    assert!(
        chain.iter().any(|entry| entry.ends_with("a")),
        "the parent that names the deepest directory must be flushed: {chain:?}"
    );

    // The same workspace named directly gives the same chain, so the fix is
    // about the spelling and not about the walk.
    let direct = tools::directories_to_flush(&real.join("a/b/notes.md"), &real);
    assert_eq!(
        direct, chain,
        "a link and the real path must flush the same directories"
    );
}

/// The reply search stops at the newest reply.
///
/// The `stop_reason` test is not indexed. The database reads and decodes each
/// row to know whether it matches, and `LIMIT` counts only the rows that DO.
/// A page of many therefore reads backward past older replies until it has
/// that many. On a long history that is the whole log for one `status`.
///
/// This asserts the property the page size buys: a page of one returns the
/// NEWEST reply and no older one. What it cannot assert is the row count the
/// database read to get there, which needs a progress hook this build does
/// not carry.
#[test]
fn the_reply_search_reads_no_further_than_the_newest_reply() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("replies.db");
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    writer
        .execute(
            "CREATE TABLE harvest_events (exec_id TEXT, seq INTEGER, event_json TEXT, \
             PRIMARY KEY (exec_id, seq))",
            [],
        )
        .expect("the fixture table is created");

    // Four turns, each a reply and then the events of its tool calls. The
    // newest reply sits behind the tool events of its own turn, and three
    // older replies sit behind those.
    let mut seq = 0_i64;
    for turn in 0..4 {
        let reply = json!({
            "type": "ActivityCompleted",
            "data": { "output": {
                "stop_reason": "tool_use",
                "tool_calls": [{
                    "id": format!("toolu_{turn}"),
                    "name": "write_file",
                    "input": { "path": "notes.md", "content": "x" },
                }],
            }},
        });
        writer
            .execute(
                "INSERT INTO harvest_events VALUES ('e', ?1, ?2)",
                rusqlite::params![seq, reply.to_string()],
            )
            .expect("the reply is recorded");
        seq += 1;
        for _ in 0..20 {
            let result = json!({
                "type": "ActivityCompleted",
                "data": { "output": { "ok": true } },
            });
            writer
                .execute(
                    "INSERT INTO harvest_events VALUES ('e', ?1, ?2)",
                    rusqlite::params![seq, result.to_string()],
                )
                .expect("the tool event is recorded");
            seq += 1;
        }
    }
    drop(writer);

    let reader = rusqlite::Connection::open(&db).expect("the database opens");
    let page = inspect::reply_calls(&reader, "e", None, 1).expect("the page reads");
    assert_eq!(page.len(), 1, "a page of one must hold one reply: {page:?}");
    assert_eq!(
        page[0].0, 63,
        "the page must hold the NEWEST reply and stop there"
    );

    // The whole log holds four replies, so a larger page walks back over the
    // older three. That is the reading this page size avoids.
    let wide = inspect::reply_calls(&reader, "e", None, 64).expect("the page reads");
    assert_eq!(
        wide.len(),
        4,
        "a wide page reads back to the end of the log: {wide:?}"
    );
}

/// One damaged event does not hide a session's whole history.
///
/// `json_extract` over a value that is not JSON raises `malformed JSON`, and
/// that error aborts the STATEMENT, not the row. One unreadable event would
/// therefore answer `status --history` with an error and name no event at
/// all. An operator reads the audit trail of a run to learn what the model
/// did. Hiding all of it over one row is the worst reading.
///
/// A second fault hides behind valid JSON: an event of `{"type":7}` extracts
/// an INTEGER. The byte cast renders it as its digits, so the audit line
/// would name an event type of `7` that no event carries. `json_type` reads
/// the type INSIDE the document, and `typeof` on the column cannot.
///
/// The guards keep the "one row cannot hide the others" property the listing
/// already holds. This test asserts it for the history and the replies.
#[test]
fn a_damaged_event_does_not_hide_a_history() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("damaged-events.db");
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    writer
        .execute(
            "CREATE TABLE harvest_events (exec_id TEXT, seq INTEGER, event_json TEXT, \
             PRIMARY KEY (exec_id, seq))",
            [],
        )
        .expect("the fixture table is created");

    let readable = json!({
        "type": "ActivityCompleted",
        "data": { "output": { "stop_reason": "end_turn", "tool_calls": [] } },
    })
    .to_string();
    // A reply whose row is readable, so the reply page has something to hold.
    let rows: [(i64, &str); 5] = [
        (0, readable.as_str()),
        // Not JSON at all. `json_extract` raises over this row.
        (1, "not json at all"),
        // Valid JSON, wrong type. Reading the integer as text raises.
        (2, r#"{"type":7,"data":{"output":{"stop_reason":"x"}}}"#),
        // Valid JSON, and text SQLite accepts that Rust cannot read.
        (3, r#"{"type":"\ud800","data":"d"}"#),
        // A type that is valid JSON but not a scalar.
        (4, r#"{"type":{"nested":true},"data":null}"#),
    ];
    for (seq, document) in rows {
        writer
            .execute(
                "INSERT INTO harvest_events VALUES ('e', ?1, ?2)",
                rusqlite::params![seq, document],
            )
            .expect("the event is recorded");
    }

    // The fault each damaged row carries, measured before it is read.
    let extract = |seq: i64| -> Result<Option<String>, rusqlite::Error> {
        writer.query_row(
            "SELECT json_extract(event_json, '$.type') FROM harvest_events WHERE seq = ?1",
            [seq],
            |row| row.get(0),
        )
    };
    assert!(
        extract(1).is_err(),
        "an unguarded extract over row 1 must fail"
    );
    assert!(
        extract(2).is_err(),
        "an unguarded extract over row 2 must fail"
    );
    assert!(
        extract(3).is_err(),
        "an unguarded extract over row 3 must fail"
    );
    drop(writer);

    let reader = rusqlite::Connection::open(&db).expect("the database opens");
    let page = inspect::event_lines(&reader, "e", None, 16).expect("the history still answers");
    assert_eq!(page.len(), 5, "every event is still named: {page:?}");
    let label = |seq: i64| -> &str {
        &page
            .iter()
            .find(|line| line.seq == seq)
            .expect("the event is named")
            .label
    };
    assert_eq!(
        label(0),
        "ActivityCompleted",
        "a readable event still reads"
    );
    for seq in [1, 2, 3, 4] {
        assert_eq!(
            label(seq),
            "unknown",
            "event {seq} carries a type this reader cannot take"
        );
    }

    // The reply page is the other statement over the same rows, and the
    // damaged rows sit NEWER than the reply it must find. Row 2 also holds a
    // `stop_reason`, so the page counts it as a reply and reads no calls from
    // it. A damaged reply is still not allowed to hide a readable one.
    let replies = inspect::reply_calls(&reader, "e", None, 8).expect("the replies still answer");
    let calls = |seq: i64| {
        replies
            .iter()
            .find(|reply| reply.0 == seq)
            .map(|reply| &reply.1)
    };
    assert!(
        matches!(calls(0), Some(inspect::ReplyCalls::NoCalls)),
        "the readable reply asked for no call: {replies:?}"
    );
    // Row 2 is `{"type":7,...}` with a `stop_reason` and NO `tool_calls`, so
    // it asked for nothing. That is a fact, and not damage: the type fault
    // this test is about is in a field the reply search never reads.
    assert!(
        matches!(calls(2), Some(inspect::ReplyCalls::NoCalls)),
        "a reply with no tool_calls field asked for nothing: {replies:?}"
    );
}

#[test]
fn a_response_body_is_read_under_a_byte_cap() {
    // `text()` buffers whatever arrives. The request timeout bounds the TIME
    // a body may take, and not the BYTES it carries. One answer could
    // therefore spend the daemon's memory and stall every session.
    let cap = claude::MAX_BODY_BYTES;

    // A body that ends under the cap is read whole.
    let mut body = Vec::new();
    assert!(
        !claude::push_capped(&mut body, b"{\"ok\":true}"),
        "a small chunk must not end the read"
    );
    assert_eq!(body.len(), 11, "a small chunk is taken whole");

    // A chunk that PASSES the cap ends the read, and the buffer holds one
    // byte more than the cap. That one byte is how the caller knows the body
    // did not end there, rather than reading the prefix as the whole answer.
    // See `a_response_body_longer_than_the_cap_is_refused`.
    let mut body = vec![b'a'; cap - 10];
    assert!(
        claude::push_capped(&mut body, &vec![b'b'; 4096]),
        "a chunk past the cap must end the read"
    );
    assert_eq!(
        body.len(),
        cap + 1,
        "the buffer must hold one byte past the cap, and not the overshoot"
    );

    // A body already at the cap takes ONE more byte and ends. It must not
    // grow by the whole chunk.
    let mut body = vec![b'a'; cap];
    assert!(
        claude::push_capped(&mut body, b"more"),
        "a full body must end the read"
    );
    assert_eq!(body.len(), cap + 1, "a full body grows by one byte only");

    // A chunk that reaches the cap EXACTLY does not pass it, so a body of
    // exactly the cap is still accepted.
    let mut body = Vec::new();
    assert!(
        !claude::push_capped(&mut body, &vec![b'a'; cap]),
        "a body of exactly the cap must not end the read"
    );
    assert_eq!(body.len(), cap, "and it is taken whole");

    // The property this cap must hold. It is a MEMORY bound, so it must sit
    // ABOVE the recorded payload cap. A reply the backend could record must
    // never be cut, whatever proportion of it duplicates.
    //
    // The durable limit is NOT enforced here. A cap of half the recorded one
    // would enforce both at once only if every reply duplicated, and a
    // thinking-heavy reply duplicates nothing. See
    // `a_recordable_thinking_reply_is_never_cut`.
    assert!(
        cap as u64 >= claude::DURABLE_CAP_BYTES,
        "the read cap must not cut a recordable reply: {cap} against {}",
        claude::DURABLE_CAP_BYTES
    );
}

/// A reply the backend cannot record is refused, and the two caps agree.
///
/// `TurnReply` keeps the assistant blocks verbatim in `content` and copies
/// their text and tool calls into `text` and `tool_calls`. Every payload byte
/// is therefore stored TWICE, so a body well under the recorded cap became a
/// durable reply above it. The backend answers `PayloadTooLarge`, which is
/// not retryable, and the turn was billed and then discarded.
#[test]
fn a_reply_the_backend_cannot_record_is_refused() {
    let reply_of = |chars: usize| {
        claude::parse_reply(&json!({
            "content": [
                { "type": "text", "text": "t".repeat(chars) },
                { "type": "tool_use", "id": "toolu_1", "name": "write_file",
                  "input": { "path": "notes.md", "content": "c".repeat(chars) } },
            ],
            "stop_reason": "tool_use",
        }))
    };
    let sizes = |chars: usize| -> (u64, u64) {
        let payload = json!({
            "content": [
                { "type": "text", "text": "t".repeat(chars) },
                { "type": "tool_use", "id": "toolu_1", "name": "write_file",
                  "input": { "path": "notes.md", "content": "c".repeat(chars) } },
            ],
            "stop_reason": "tool_use",
        });
        let body = serde_json::to_vec(&payload).expect("the body serialises");
        let durable = serde_json::to_vec(&reply_of(chars)).expect("the reply serialises");
        (body.len() as u64, durable.len() as u64)
    };

    // The duplication, measured rather than assumed. The factor must BOUND
    // the cost of a reply of text and tool calls. It must also be no looser
    // than it needs to be for that shape.
    let (body, durable) = sizes(400_000);
    // The worst case, which is a property of the representation rather than
    // a constant any code path derives from. Nothing but this test reads it.
    let factor = 2_u64;
    assert!(
        durable <= body * factor,
        "the factor must bound the cost: {durable} recorded for a body of {body}"
    );
    assert!(
        durable > body * (factor - 1),
        "and a smaller factor must not: {durable} recorded for a body of {body}"
    );

    // A body UNDER the recorded cap whose reply is OVER it. This is the case
    // that was accepted, billed, and then discarded by the backend.
    let (body, durable) = sizes(900_000);
    assert!(
        body < claude::DURABLE_CAP_BYTES && durable > claude::DURABLE_CAP_BYTES,
        "the fixture must straddle the cap: body {body}, recorded {durable}"
    );

    // The read cap does NOT refuse that body, and must not. The same size of
    // body carrying thinking blocks records half as much, and is recordable.
    // Only the reply itself can tell the two apart.
    assert!(
        body <= claude::MAX_BODY_BYTES as u64,
        "the read cap is a memory bound and must admit this body"
    );

    // So the reply is measured as it will be STORED, which no arithmetic over
    // the body can promise. That measurement is the durable limit.
    let refusal = claude::activity_refusal_for_oversized_reply(&reply_of(900_000));
    assert!(
        refusal.is_some(),
        "a reply over the recorded cap must be refused"
    );
    let message = refusal.expect("the refusal says why");
    assert!(
        message.contains("once recorded") && message.contains("--max-tokens"),
        "the refusal must name the cap and what to lower: {message}"
    );

    // A reply that FITS is not refused, so the guard is not simply a wall.
    assert!(
        claude::activity_refusal_for_oversized_reply(&reply_of(1_000)).is_none(),
        "an ordinary reply must still be accepted"
    );
}

/// A recordable thinking-heavy reply is never cut by the read cap.
///
/// `parse_reply` copies text and tool calls out of `content` into `text` and
/// `tool_calls`, and copies NOTHING out of a `thinking` or
/// `redacted_thinking` block. So the duplication factor is a worst case and
/// not a rate: a thinking-heavy reply is stored once.
///
/// A read cap derived by DIVIDING the recorded cap by that worst case cuts
/// such a reply in half. A cut body
/// does not parse, so the turn failed as a malformed response. The spend was
/// the same, and the response was perfectly recordable. The cap is a memory
/// bound, and the durable limit belongs on the reply.
#[test]
fn a_recordable_thinking_reply_is_never_cut() {
    let thinking = |chars: usize| {
        json!({
            "id": "msg_1", "type": "message", "role": "assistant", "model": "m",
            "content": [
                { "type": "thinking", "thinking": "r".repeat(chars), "signature": "sig" },
                { "type": "text", "text": "done" },
            ],
            "stop_reason": "end_turn",
            "usage": { "input_tokens": 10, "output_tokens": 20 },
        })
    };

    // A thinking block is NOT duplicated, so the reply is about the size of
    // the body rather than twice it.
    let payload = thinking(1_800_000);
    let body = serde_json::to_vec(&payload).expect("the body serialises");
    let reply = claude::parse_reply(&payload);
    let durable = serde_json::to_vec(&reply).expect("the reply serialises");
    assert!(
        durable.len() <= body.len(),
        "a thinking-heavy reply must not grow: {} recorded for a body of {}",
        durable.len(),
        body.len()
    );
    assert!(
        reply.text == "done" && reply.tool_calls.is_empty(),
        "nothing is copied out of a thinking block: {:?}",
        reply.text
    );

    // The backend would record it, so this daemon must not refuse it.
    assert!(
        durable.len() as u64 <= claude::DURABLE_CAP_BYTES,
        "the fixture must be recordable: {} against {}",
        durable.len(),
        claude::DURABLE_CAP_BYTES
    );
    assert!(
        claude::activity_refusal_for_oversized_reply(&reply).is_none(),
        "a recordable reply must not be refused"
    );

    // And the read cap must not cut it. This is the assertion that fails
    // against a cap derived by dividing the recorded cap.
    assert!(
        body.len() <= claude::MAX_BODY_BYTES,
        "the read cap must admit a recordable body: {} against {}",
        body.len(),
        claude::MAX_BODY_BYTES
    );

    // Why a cut is terminal rather than merely lossy: the bytes stop mid
    // document, so nothing can read them.
    let cut = &body[..body.len() / 2];
    assert!(
        serde_json::from_slice::<serde_json::Value>(cut).is_err(),
        "a cut body does not parse, so a cut turn is a malformed one"
    );
}

/// A response body that is not text is refused, and never repaired.
///
/// `from_utf8_lossy` replaces an invalid byte with U+FFFD. That can turn a
/// malformed body into VALID JSON carrying a value the model never sent, and
/// the daemon then records and runs it. A `write_file` path is the sharp
/// case: the operator approves the path they are shown, and the model asked
/// for another one.
///
/// Strict decoding costs nothing. A body cut at the read cap is refused
/// either way. The decode refuses it when the cut splits a character, and
/// the JSON parse refuses it when the document ends unclosed.
#[test]
fn a_response_body_that_is_not_text_is_refused() {
    // A raw invalid byte inside the `path` of a gated write.
    let mut body: Vec<u8> =
        br#"{"content":[{"type":"tool_use","id":"toolu_1","name":"write_file","#.to_vec();
    body.extend_from_slice(br#""input":{"path":"note"#);
    body.push(0xFF);
    body.extend_from_slice(br#".md","content":"x"}}],"stop_reason":"tool_use"}"#);

    // What the lossy read would have done, which is why this matters. The
    // repaired body PARSES, and the call it carries names a path the model
    // never sent.
    let repaired = String::from_utf8_lossy(&body).into_owned();
    let parsed: Value =
        serde_json::from_str(&repaired).expect("the repaired body parses, which is the hazard");
    let reply = claude::parse_reply(&parsed);
    let path = reply
        .tool_calls
        .first()
        .and_then(|call| call.input.get("path"))
        .and_then(Value::as_str)
        .expect("the repaired call carries a path");
    assert!(
        path.contains(char::REPLACEMENT_CHARACTER),
        "the repaired path is not the one sent: {path}"
    );

    // The strict read refuses those bytes, and says where.
    let refused = claude::decode_body(body).expect_err("a body that is not text must be refused");
    assert!(
        refused.contains("not UTF-8 text") && refused.contains("begins no character"),
        "the refusal must say what is wrong: {refused}"
    );

    // An ordinary body still reads, so the check is not simply a wall.
    let whole = serde_json::to_vec(&json!({
        "content": [{ "type": "text", "text": "caf\u{e9} \u{20ac}" }],
        "stop_reason": "end_turn",
    }))
    .expect("the body serialises");
    assert!(
        claude::decode_body(whole.clone()).is_ok(),
        "a body of ordinary text must still read"
    );

    // And the case the lossy read was written for: a body CUT at the cap. It
    // is refused either way, so nothing that worked is lost.
    let cut = whole[..whole.len() - 2].to_vec();
    let by_json = serde_json::from_str::<Value>(&String::from_utf8_lossy(&cut));
    assert!(
        claude::decode_body(cut).is_err() || by_json.is_err(),
        "a cut body must be refused by one check or the other"
    );
}

/// A body longer than the cap is refused, not read as its prefix.
///
/// The read once stopped AT the cap and handed back what it had. JSON allows
/// trailing whitespace, so a complete message padded to the boundary parses,
/// and the daemon ran a tool call from it. The bytes after the cap were
/// never seen. The WHOLE body does not parse, which is the answer the turn
/// should have given.
///
/// The read now takes one byte past the cap. That byte tells a body which
/// ENDED at the cap from one that merely reached it.
#[test]
fn a_response_body_longer_than_the_cap_is_refused() {
    let cap = claude::MAX_BODY_BYTES;

    // A complete, valid `tool_use` message, padded with whitespace to
    // exactly the cap, and then more data.
    let message = serde_json::to_vec(&json!({
        "content": [{ "type": "tool_use", "id": "toolu_1", "name": "write_file",
                      "input": { "path": "note.md", "content": "x" } }],
        "stop_reason": "tool_use",
    }))
    .expect("the message serialises");
    let mut whole = message;
    whole.resize(cap, b' ');
    whole.extend_from_slice(br#"{"content":[],"stop_reason":"end_turn"}"#);

    // The hazard, stated first. The PREFIX is a valid message carrying a
    // call, and the whole body is not valid JSON at all.
    let prefix = whole[..cap].to_vec();
    let parsed: Value = serde_json::from_slice(&prefix).expect("the prefix parses on its own");
    assert_eq!(
        claude::parse_reply(&parsed).tool_calls.len(),
        1,
        "the prefix carries a tool call, which is what made this dangerous"
    );
    assert!(
        serde_json::from_slice::<Value>(&whole).is_err(),
        "the whole body must not parse, so the turn should be refused"
    );

    // The read, driven the way `read_capped` drives it.
    let mut body: Vec<u8> = Vec::new();
    let mut stopped = false;
    for chunk in whole.chunks(64 * 1024) {
        if claude::push_capped(&mut body, chunk) {
            stopped = true;
            break;
        }
    }
    assert!(stopped, "the read must end on a body past the cap");
    assert_eq!(
        body.len(),
        cap + 1,
        "and it must hold the one byte that proves the body continued"
    );

    // So the body is refused, and its prefix is never parsed. The read is
    // mapped to its LENGTH before asserting, because a failure here would
    // otherwise print four megabytes of padding.
    let refused = claude::decode_body(body)
        .map(|text| text.len())
        .expect_err("a body past the cap must be refused");
    assert!(
        refused.contains("longer than") && refused.contains("--max-tokens"),
        "the refusal must say what is wrong and what to lower: {refused}"
    );

    // A body of EXACTLY the cap is still accepted. This is the boundary the
    // extra byte exists to draw, and refusing here would refuse a response
    // that genuinely ended.
    let mut body: Vec<u8> = Vec::new();
    for chunk in prefix.chunks(64 * 1024) {
        assert!(
            !claude::push_capped(&mut body, chunk),
            "a body of exactly the cap must not end the read early"
        );
    }
    assert_eq!(body.len(), cap, "the whole body is buffered");
    assert!(
        claude::decode_body(body).is_ok(),
        "a body that ends at the cap must still be read"
    );
}

#[test]
fn a_key_that_cannot_be_a_header_is_refused() {
    // A key read out of a file can carry an interior newline. The trim on the
    // way in removes the ends and not the middle. The value therefore counts
    // as present, and the daemon would run live against it.
    let interior = "sk-ant-aa\nbb";
    assert_eq!(
        crate::usable_key(interior),
        Some(interior.to_string()),
        "the trim leaves an interior newline, which is why this check exists"
    );

    // The key is tested as the thing it becomes. Reqwest is asked, rather
    // than this example guessing which bytes a header value accepts.
    for bad in ["sk-ant-aa\nbb", "sk-ant-aa\rbb", "sk-ant-aa\u{0}bb"] {
        let (_, signal) = crate::shutdown::channel();
        let refused = claude::ModelConfig::new(
            Some(bad.to_string()),
            claude::DEFAULT_MODEL,
            claude::DEFAULT_MAX_TOKENS,
            signal,
        );
        let Err(message) = refused else {
            panic!("{bad:?} cannot be a header value and must be refused");
        };
        assert!(
            message.contains("header"),
            "the refusal must name the reason: {message}"
        );
    }

    // A key of ordinary characters is accepted, so the check refuses only
    // what the header refuses.
    let (_, signal) = crate::shutdown::channel();
    assert!(
        claude::ModelConfig::new(
            Some("sk-ant-api03-aAbB09_-".to_string()),
            claude::DEFAULT_MODEL,
            claude::DEFAULT_MAX_TOKENS,
            signal,
        )
        .is_ok(),
        "an ordinary key must be accepted"
    );
}

#[test]
fn a_socket_path_that_cannot_be_printed_is_refused() {
    use clap::Parser;
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    // A path is bytes on this platform. Every command prints follow-up
    // commands naming the socket, and a byte that is not UTF-8 cannot be
    // written into one of those lines unchanged. The printed line would then
    // name another socket, or none.
    let raw = OsString::from_vec(vec![b'/', b't', b'm', b'p', b'/', 0xff, b'.', b's']);
    let cli = crate::Cli::try_parse_from([
        OsString::from("agentd"),
        OsString::from("--socket"),
        raw,
        OsString::from("list"),
    ])
    .expect("clap accepts the bytes; the refusal is the daemon's own");

    let refused = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime")
        .block_on(crate::run(cli));
    let message = refused.expect_err("a path that cannot be printed must be refused");
    assert!(
        message.contains("not UTF-8"),
        "the refusal must name the reason: {message}"
    );
}

/// The parked state is built before the socket accepts anything.
///
/// This is an ORDERING, and the window it closes is a race. No end-to-end
/// test fails reliably without it, because a lucky schedule lets the first
/// drive win and the answer comes out right anyway. The reconstruction query
/// has its own test. This pins the daemon using it, and using it first.
///
/// Reverting the startup rebuild left the query's own test passing, which is
/// why this guard exists rather than a comment promising the order.
#[test]
fn the_parked_state_is_rebuilt_before_the_socket_is_bound() {
    let daemon = include_str!("daemon.rs");
    let built = daemon
        .find("Parked::new()")
        .expect("the daemon builds its parked state");
    let bound = daemon
        .find("bind(&options.socket)")
        .expect("the daemon binds its socket");
    assert!(
        built < bound,
        "the parked state must be built before the socket accepts anything"
    );
    // Built early and left empty would pass the order and fix nothing.
    let read = daemon
        .find("outstanding_signal")
        .expect("the daemon reads each durable wait");
    assert!(
        read < bound,
        "each durable wait must be read before the socket accepts anything"
    );
}

/// The daemon builds no command an operator can copy.
///
/// Five separate findings were one defect: a command formatted in the daemon,
/// which cannot know which socket the client asked. Fixing them one at a time
/// left the next one to be found. This reads the source as data, so a command
/// added to the daemon fails here rather than in review.
#[test]
fn no_operator_command_is_built_in_the_daemon() {
    let daemon = include_str!("daemon.rs");
    assert!(
        !daemon.contains("`agentd "),
        "the daemon must send data and let the client render the command"
    );
    // The guard is only worth having if the pattern it looks for is the one
    // the client actually uses.
    assert!(
        include_str!("main.rs").contains("`agentd "),
        "the client is where these commands belong"
    );
}

#[test]
fn a_workspace_that_is_a_file_is_refused() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().expect("a temporary directory");
    let file = dir.path().join("not-a-directory");
    std::fs::write(&file, "I am a file").expect("the fixture is written");

    // A file with the owner's execute bit reads as enterable by mode alone.
    // Nothing is missing above it, so nothing would be created, and the
    // daemon would start over a workspace no tool can use.
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o700))
        .expect("the fixture is made executable");
    let refusal = tools::create_enterable(&file).expect_err("a file must be refused");
    assert_eq!(
        refusal.kind(),
        std::io::ErrorKind::NotADirectory,
        "the refusal must say what is wrong: {refusal}"
    );

    // Without the owner bits the repair path would have changed the mode of
    // a file nobody asked this daemon to touch.
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600))
        .expect("the fixture is made unexecutable");
    tools::create_enterable(&file).expect_err("a file must still be refused");
    let mode = std::fs::metadata(&file)
        .expect("the file exists")
        .permissions()
        .mode()
        & 0o7777;
    assert_eq!(mode, 0o600, "the file's mode must be untouched");

    // A file BELOW the target is refused too, rather than created through.
    let under = file.join("child");
    tools::create_enterable(&under).expect_err("a path through a file must be refused");
}

#[test]
fn a_key_of_whitespace_is_not_a_key() {
    // A key of whitespace would count as present, and the daemon would run
    // live against it. Every turn would fail at the API. An absent key runs
    // the offline stub instead, which is the quieter and correct outcome.
    for blank in ["", " ", "\t\n", "   "] {
        assert_eq!(
            crate::usable_key(blank),
            None,
            "a key of whitespace must read as no key: {blank:?}"
        );
    }

    // An operator commonly reads a key out of a file and keeps the newline.
    // A header carries that byte to the API, which rejects it.
    assert_eq!(
        crate::usable_key("sk-ant-example\n").as_deref(),
        Some("sk-ant-example"),
        "a key keeps none of the whitespace around it"
    );
}

#[test]
fn a_turn_whose_tool_calls_share_an_id_is_refused() {
    let call = |id: &str| ToolCall {
        id: id.to_string(),
        name: tools::TOOL_WRITE_FILE.to_string(),
        input: json!({ "path": "notes.md", "content": "x" }),
    };
    let reply = |calls: Vec<ToolCall>| TurnReply {
        content: json!([]),
        stop_reason: "tool_use".to_string(),
        text: String::new(),
        tool_calls: calls,
    };

    // An approval is addressed by id, so a repeated one would let a single
    // decision release a call the operator never read.
    assert!(
        !claude::has_addressable_calls(&reply(vec![call("toolu_a"), call("toolu_a")])),
        "a repeated id must be refused"
    );
    assert!(
        !claude::has_addressable_calls(&reply(vec![call("")])),
        "a blank id must be refused"
    );
    assert!(
        claude::has_addressable_calls(&reply(vec![call("toolu_a"), call("toolu_b")])),
        "distinct ids are addressable"
    );
    assert!(
        claude::has_addressable_calls(&reply(Vec::new())),
        "a turn with no tool calls has nothing to address"
    );

    // The id becomes part of the approval token, and the status view prints
    // that token unquoted in an `agentd approve` command line. An operator
    // copies that line. A shell drops a trailing space and splits an inner
    // one, so the copied token no longer matches the staged name. The write
    // then stays blocked until its deadline, with no sign of why.
    // A shell reads what the operator copies. An id of `x;reboot` is not a
    // token at all under that reading: it is a command, and the model chose
    // it. Whitespace, a quote, a backtick, a pipe and a glob mangle or obey
    // the line in the same way. A leading dash reads as a flag.
    for mangled in [
        " ", "\t", "toolu_a ", " toolu_a", "toolu a", "x;reboot", "$(id)", "`id`", "a|b", "a&b",
        "a>b", "a*", "a'b", "a\"b", "-toolu_a",
    ] {
        assert!(
            !claude::has_addressable_calls(&reply(vec![call(mangled)])),
            "an id a shell would read differently must be refused: {mangled:?}"
        );
    }

    // The shape the API actually mints stays acceptable.
    for minted in ["toolu_01A09q90qw90lq917835lq9", "call-1.2_3"] {
        assert!(
            claude::has_addressable_calls(&reply(vec![call(minted)])),
            "a minted id must be addressable: {minted:?}"
        );
    }

    // An accepted id therefore always yields a token an operator can copy.
    let token = session::approval_signal(1, 0, "toolu_01A09q90qw90lq917835lq9");
    assert!(
        token
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | ':')),
        "an accepted id must give a token a shell leaves alone: {token:?}"
    );
}

#[test]
fn a_write_flushes_the_whole_chain_below_the_workspace() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");
    let root = workspace.canonicalize().expect("the workspace resolves");
    let target = root.join("notes/day/1/log.md");
    let chain = vec![
        root.join("notes/day/1"),
        root.join("notes/day"),
        root.join("notes"),
        root.clone(),
    ];

    // Each directory between the target and the root can hold an entry this
    // write created. An entry is durable only once its own directory is
    // flushed.
    assert_eq!(
        tools::directories_to_flush(&target, &workspace),
        chain,
        "the flush must run from the target's directory up to the root"
    );

    // The same list after the directories already exist. Activity execution is
    // at-least-once, so a retry can find the directories a crashed attempt
    // created and never flushed. A list built from this attempt's own
    // creations would name nothing.
    std::fs::create_dir_all(target.parent().expect("a parent")).expect("the chain exists");
    assert_eq!(
        tools::directories_to_flush(&target, &workspace),
        chain,
        "a retry must flush the chain it did not create"
    );

    // A target in the root itself flushes the root, and nothing above it.
    assert_eq!(
        tools::directories_to_flush(&root.join("notes.md"), &workspace),
        vec![root.clone()],
        "the walk must stop at the workspace"
    );

    // The real path still writes the nested file and lands the content.
    let body = tools::activity_body(workspace.clone());
    let raw = body(tool_request(
        &workspace,
        tools::TOOL_WRITE_FILE,
        json!({ "path": "deep/er/still/notes.md", "content": "hello" }),
    ))
    .expect("a tool failure is a result, not an activity error");
    let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");
    assert!(
        !outcome.is_error,
        "the nested write must succeed: {}",
        outcome.output
    );
    assert_eq!(
        std::fs::read_to_string(root.join("deep/er/still/notes.md")).expect("the file exists"),
        "hello"
    );
}

#[test]
fn a_turn_that_ends_and_still_asks_for_a_tool_is_refused() {
    let reply = |stop: &str, calls: Vec<ToolCall>| TurnReply {
        content: json!([]),
        stop_reason: stop.to_string(),
        text: String::new(),
        tool_calls: calls,
    };
    let call = vec![ToolCall {
        id: "toolu_a".to_string(),
        name: tools::TOOL_WRITE_FILE.to_string(),
        input: json!({ "path": "notes.md", "content": "x" }),
    }];

    // A turn cannot both end and ask for a tool. Running the call and then
    // reporting a clean finish, or dropping it and reporting one, both present
    // a malformed billed response as a finished session.
    assert!(
        !claude::agrees_with_its_content(&reply(claude::STOP_END_TURN, call.clone())),
        "`end_turn` with a tool call must be refused"
    );
    assert!(
        claude::agrees_with_its_content(&reply(claude::STOP_TOOL_USE, call.clone())),
        "a tool call under `tool_use` is the ordinary case"
    );
    assert!(
        claude::agrees_with_its_content(&reply(claude::STOP_END_TURN, Vec::new())),
        "a finished turn with no tool call is consistent"
    );

    // A truncated turn can carry a partial block. The loop drops it unrun and
    // reports `max_tokens`, so this must stay a report and not become a
    // refusal.
    assert!(
        claude::agrees_with_its_content(&reply("max_tokens", call)),
        "a truncated turn must still report rather than fail"
    );
}

#[tokio::test]
async fn a_tool_call_under_an_unknown_stop_reason_is_not_run() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");

    // A stop reason this example does not know about, carrying a write. The
    // pair is not a contradiction, so the turn is not refused. The call is
    // still not what the stop reason asked for, so it must not run.
    let mut rt = SqliteRuntime::open(dir.path().join("agentd.db")).expect("the database opens");
    rt.register_workflow(&session::agent_session_info());
    rt.register_activity(&session::claude_turn_info(), |_input| {
        serde_json::to_value(TurnReply {
            content: json!([]),
            stop_reason: "pause_turn".to_string(),
            text: "thinking".to_string(),
            tool_calls: vec![ToolCall {
                id: "toolu_paused".to_string(),
                name: tools::TOOL_WRITE_FILE.to_string(),
                input: json!({ "path": "unasked.md", "content": "never" }),
            }],
        })
        .map_err(|e| format!("bad reply: {e}"))
    });
    rt.register_activity(
        &session::run_tool_info(),
        tools::activity_body(workspace.clone()),
    );

    let exec = rt
        .start_workflow(WORKFLOW_NAME, task(&workspace))
        .expect("the session starts");
    let state = rt.run_until_blocked(exec).await.expect("the run finishes");
    let RunState::Completed(output) = state else {
        panic!("expected a terminal report, got {state:?}");
    };

    let report: SessionReport = serde_json::from_value(output).expect("the report decodes");
    assert_eq!(
        report.stop, "pause_turn",
        "the session must end under the stop reason it was given"
    );
    assert_eq!(report.tool_calls, 0, "the unasked call must not run");
    assert!(
        !workspace.join("unasked.md").exists(),
        "the unasked write must not land"
    );
}

#[test]
fn the_toolbox_stops_listing_a_directory_at_the_cap() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");

    // One entry over the cap is enough to prove the read stops. The tool bodies
    // run on the one runtime, so naming a huge directory in full would block
    // every session and every control command.
    for index in 0..=tools::MAX_ENTRIES {
        std::fs::write(workspace.join(format!("file-{index:05}.txt")), "x")
            .expect("the fixture is written");
    }

    let body = tools::activity_body(workspace.clone());
    let listed = |input: Value| -> String {
        let raw = body(input).expect("a tool failure is a result, not an activity error");
        let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");
        assert!(
            !outcome.is_error,
            "the listing must succeed: {}",
            outcome.output
        );
        outcome.output
    };

    let output = listed(tool_request(
        &workspace,
        tools::TOOL_LIST_FILES,
        json!({ "path": "." }),
    ));
    let lines: Vec<&str> = output.lines().collect();
    assert_eq!(
        lines.len(),
        tools::MAX_ENTRIES + 2,
        "the listing must carry the cap, one marker and the count"
    );
    assert_eq!(
        lines.last().copied(),
        Some(format!("... entries named: {}", tools::MAX_ENTRIES).as_str()),
        "the count is the last line"
    );
    let marker = lines[lines.len() - 2];
    assert!(
        marker.starts_with("... more entries"),
        "the truncation must be reported: {marker}"
    );

    // A directory inside the cap is listed whole, sorted, with no marker.
    let small = workspace.join("small");
    std::fs::create_dir(&small).expect("the directory is created");
    std::fs::write(small.join("b.txt"), "x").expect("the fixture is written");
    std::fs::write(small.join("a.txt"), "x").expect("the fixture is written");
    assert_eq!(
        listed(tool_request(
            &workspace,
            tools::TOOL_LIST_FILES,
            json!({ "path": "small" }),
        )),
        "a.txt\nb.txt\n... entries named: 2",
        "a small directory must be listed whole and sorted, and counted"
    );
}

#[tokio::test]
async fn the_startup_and_status_queries_read_only_what_they_need() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");
    let db = dir.path().join("agentd.db");

    let calls = Arc::new(AtomicUsize::new(0));
    let mut rt = runtime(&db, &workspace, &calls);

    // One session driven to completion, and one parked on its approval.
    let done = rt
        .start_workflow(WORKFLOW_NAME, task(&workspace))
        .expect("the session starts");
    let signal = drive_to_approval(&mut rt, done).await;
    approve(&mut rt, done, &signal);
    let state = rt.run_until_blocked(done).await.expect("the run finishes");
    assert!(
        matches!(state, RunState::Completed(_)),
        "expected completion, got {state:?}"
    );

    let parked = rt
        .start_workflow(WORKFLOW_NAME, task(&workspace))
        .expect("the second session starts");
    drive_to_approval(&mut rt, parked).await;

    // The tick polls several times a second, so it must not read the rows it
    // cannot act on.
    let reader = crate::inspect::open(&db).expect("the inspector opens");
    let running = crate::inspect::running(&reader, WORKFLOW_NAME).expect("the drive query answers");
    assert_eq!(
        running
            .iter()
            .map(|session| session.exec_id.clone())
            .collect::<Vec<_>>(),
        vec![parked.to_string()],
        "only the parked session is drivable"
    );
    // The startup check compares the workspace and the model, so the query
    // carries those two FIELDS and not the whole recorded task. A goal can
    // approach the control-request cap, and a restart reads every parked
    // session. Reading the tasks whole could spend the daemon's memory before
    // it is ready.
    let first = running
        .first()
        .expect("the parked session is listed")
        .task
        .as_ref()
        .expect("the parked task is readable");
    assert_eq!(
        first.workspace,
        workspace.to_str().expect("the workspace path is UTF-8"),
        "the running row must carry the workspace it was recorded against"
    );
    assert_eq!(
        first.model,
        claude::OFFLINE_MODEL,
        "the running row must carry the model it was recorded against"
    );
    assert!(
        first.has_goal,
        "the parked session was recorded with a goal"
    );
    assert!(
        !format!("{} {}", first.workspace, first.model).contains("summarise the workspace"),
        "the running row must not carry the goal"
    );
    assert_eq!(
        crate::inspect::executions(&reader, WORKFLOW_NAME, None)
            .expect("the listing answers")
            .len(),
        2,
        "both sessions are still listed for the operator"
    );

    // `status` names one session, so it reads one row rather than building a
    // view of every session that ever ran.
    let one = |exec: ExecutionId| {
        crate::inspect::execution(&reader, WORKFLOW_NAME, &exec.to_string())
            .expect("the single-row query answers")
    };
    assert_eq!(
        one(done).map(|row| row.state),
        Some("COMPLETED".to_string()),
        "the finished session is readable by id"
    );
    assert_eq!(
        one(parked).map(|row| row.state),
        Some("RUNNING".to_string()),
        "the parked session is readable by id"
    );
    assert!(
        crate::inspect::execution(
            &reader,
            WORKFLOW_NAME,
            "00000000-0000-4000-8000-000000000000",
        )
        .expect("the single-row query answers")
        .is_none(),
        "an id that names no session reads as nothing"
    );
}

#[test]
fn a_write_lands_on_a_name_at_the_component_limit() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");

    // A 255-byte name is legal on the common filesystems. A scratch name that
    // copied it whole would be 20 bytes longer. Every attempt then failed with
    // `ENAMETOOLONG`, and the approved write could not land at all.
    let long = "n".repeat(255);
    // A name of multi-byte characters, to prove the cut lands on a boundary.
    let wide = "é".repeat(120);

    let body = tools::activity_body(workspace.clone());
    for name in [long.as_str(), wide.as_str()] {
        let raw = body(tool_request(
            &workspace,
            tools::TOOL_WRITE_FILE,
            json!({ "path": name, "content": "landed" }),
        ))
        .expect("a tool failure is a result, not an activity error");
        let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");
        assert!(
            !outcome.is_error,
            "the write must land on a {}-byte name: {}",
            name.len(),
            outcome.output
        );
        assert_eq!(
            std::fs::read_to_string(workspace.join(name)).expect("the file exists"),
            "landed"
        );
    }

    // The scratch name fits whatever the target does, and a short target keeps
    // its whole stem so a leftover file stays identifiable.
    for name in [long.as_str(), wide.as_str(), "notes.md"] {
        let scratch = tools::scratch_name(name, 4_294_967_295, u64::MAX, 15);
        assert!(
            scratch.len() <= name.len().max(96),
            "`{scratch}` is longer than the name it replaces"
        );
        assert!(scratch.len() <= 255, "`{scratch}` is over the limit");
    }
    assert!(
        tools::scratch_name("notes.md", 123, 1, 0).contains("notes.md"),
        "a short target must keep its stem"
    );

    // The name carries a value that does not repeat across restarts. A daemon
    // that always starts as pid 1 would otherwise retry the same sixteen names
    // after a crash between the create and the rename.
    assert_ne!(
        tools::scratch_nonce(),
        tools::scratch_nonce(),
        "the scratch nonce must not repeat"
    );
    assert_ne!(
        tools::scratch_name("notes.md", 1, 1, 0),
        tools::scratch_name("notes.md", 1, 2, 0),
        "a different nonce must give a different name"
    );
}

#[test]
fn a_live_daemon_refuses_the_offline_identity() {
    // A key plus the stub's own name would make `identity` match a session
    // recorded offline. The restart would send that transcript to the API.
    // `expect_err` is not available here on purpose: `ModelConfig` holds the
    // API key, so it does not implement `Debug`.
    let Err(refusal) = claude::ModelConfig::new(
        Some("sk-not-a-real-key".to_string()),
        claude::OFFLINE_MODEL,
        claude::DEFAULT_MAX_TOKENS,
        crate::shutdown::channel().1,
    ) else {
        panic!("the stub's name must not be accepted as a model");
    };
    assert!(
        refusal.contains("would leave this machine"),
        "the refusal must say what is at stake: {refusal}"
    );

    // Without a key the name is what the daemon records anyway, so it is no
    // error. A real model with a key is the ordinary case.
    let offline = claude::ModelConfig::new(
        None,
        claude::OFFLINE_MODEL,
        claude::DEFAULT_MAX_TOKENS,
        crate::shutdown::channel().1,
    )
    .expect("the stub needs no key");
    assert_eq!(offline.identity(), claude::OFFLINE_MODEL);

    let live = claude::ModelConfig::new(
        Some("sk-not-a-real-key".to_string()),
        claude::DEFAULT_MODEL,
        claude::DEFAULT_MAX_TOKENS,
        crate::shutdown::channel().1,
    )
    .expect("a real model with a key is ordinary");
    assert_eq!(live.identity(), claude::DEFAULT_MODEL);
}

#[tokio::test]
async fn the_shutdown_flag_is_never_missed() {
    // A waiter that starts AFTER the signal must not wait forever. The model
    // request waits on this flag from inside a blocking call, so it starts
    // late by construction.
    let (trigger, signal) = crate::shutdown::channel();
    trigger.send_replace(true);
    let mut late = signal.clone();
    tokio::time::timeout(Duration::from_secs(5), late.raised())
        .await
        .expect("a raised flag must not make a late waiter wait");

    // A waiter that starts first is released when the flag goes up.
    let (trigger, signal) = crate::shutdown::channel();
    let mut early = signal.clone();
    let waiting = tokio::spawn(async move { early.raised().await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    trigger.send_replace(true);
    tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .expect("the waiter must be released")
        .expect("the waiter finishes");

    // A trigger that is dropped reads as a stop. Its only holder is the task
    // that waits for the signal, so losing it means the daemon is going away.
    let (trigger, signal) = crate::shutdown::channel();
    let mut orphaned = signal.clone();
    drop(trigger);
    tokio::time::timeout(Duration::from_secs(5), orphaned.raised())
        .await
        .expect("a lost trigger must not make a waiter wait");
}

#[test]
fn a_written_file_never_keeps_set_id_bits() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().to_path_buf();
    let target = workspace.join("helper.sh");
    std::fs::write(&target, "old").expect("the fixture is written");
    // A `setuid` script the agent is then asked to rewrite.
    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o4755))
        .expect("the fixture is made set-user-id");

    let body = tools::activity_body(workspace.clone());
    let raw = body(tool_request(
        &workspace,
        tools::TOOL_WRITE_FILE,
        json!({ "path": "helper.sh", "content": "#!/bin/sh\necho mine\n" }),
    ))
    .expect("a tool failure is a result, not an activity error");
    let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");
    assert!(
        !outcome.is_error,
        "the write must succeed: {}",
        outcome.output
    );

    // The new inode belongs to the daemon and the model chose its bytes. A
    // `setuid` file here would run as the daemon's user for anyone who could
    // execute it, and the approval never showed the mode.
    let mode = std::fs::metadata(&target)
        .expect("the target exists")
        .permissions()
        .mode()
        & 0o7777;
    assert_eq!(mode, 0o0755, "the set-user-id bit must not survive a write");
}

#[test]
fn a_write_keeps_the_mode_of_the_file_it_replaces() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().to_path_buf();
    let secret = workspace.join("secret.txt");
    std::fs::write(&secret, "old").expect("the fixture is written");
    std::fs::set_permissions(&secret, std::fs::Permissions::from_mode(0o600))
        .expect("the fixture is made private");

    let body = tools::activity_body(workspace.clone());
    let raw = body(tool_request(
        &workspace,
        tools::TOOL_WRITE_FILE,
        json!({ "path": "secret.txt", "content": "new" }),
    ))
    .expect("a tool failure is a result, not an activity error");
    let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");
    assert!(
        !outcome.is_error,
        "the write must succeed: {}",
        outcome.output
    );

    // A content change is not a permission change.
    let mode = std::fs::metadata(&secret)
        .expect("the target exists")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(
        mode, 0o600,
        "the target's mode must survive the replacement"
    );

    // A file the agent brings into being starts private.
    let raw = body(tool_request(
        &workspace,
        tools::TOOL_WRITE_FILE,
        json!({ "path": "fresh.txt", "content": "new" }),
    ))
    .expect("a tool failure is a result, not an activity error");
    let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");
    assert!(
        !outcome.is_error,
        "the write must succeed: {}",
        outcome.output
    );
    let mode = std::fs::metadata(workspace.join("fresh.txt"))
        .expect("the new file exists")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600, "a new file must be owner-only");
}

#[test]
fn a_write_keeps_a_group_readable_mode_the_umask_would_strip() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().to_path_buf();
    let shared = workspace.join("shared.txt");
    std::fs::write(&shared, "old").expect("the fixture is written");
    std::fs::set_permissions(&shared, std::fs::Permissions::from_mode(0o660))
        .expect("the fixture is made group-writable");

    // A mode passed to `open` is filtered through the umask, so `0660` would
    // come back `0640` under the common one. The mode is applied to the
    // descriptor instead, where nothing filters it.
    let body = tools::activity_body(workspace.clone());
    let raw = body(tool_request(
        &workspace,
        tools::TOOL_WRITE_FILE,
        json!({ "path": "shared.txt", "content": "new" }),
    ))
    .expect("a tool failure is a result, not an activity error");
    let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");
    assert!(
        !outcome.is_error,
        "the write must succeed: {}",
        outcome.output
    );

    let mode = std::fs::metadata(&shared)
        .expect("the target exists")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(
        mode, 0o660,
        "the target's mode must survive the replacement"
    );
}

#[test]
fn a_write_never_removes_a_file_that_occupies_a_scratch_name() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().to_path_buf();

    // Whatever sits on a scratch name may be residue someone wants to read.
    // The write picks another name rather than deleting it.
    let squatter = workspace.join(format!(".notes.md.agentd-{}-0.tmp", std::process::id()));
    std::fs::write(&squatter, "do not delete me").expect("the fixture is written");

    let body = tools::activity_body(workspace.clone());
    let raw = body(tool_request(
        &workspace,
        tools::TOOL_WRITE_FILE,
        json!({ "path": "notes.md", "content": "the approved content" }),
    ))
    .expect("a tool failure is a result, not an activity error");
    let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");
    assert!(
        !outcome.is_error,
        "the write must succeed: {}",
        outcome.output
    );

    assert_eq!(
        std::fs::read_to_string(&squatter).expect("the occupant survives"),
        "do not delete me"
    );
    assert_eq!(
        std::fs::read_to_string(workspace.join("notes.md")).expect("the target exists"),
        "the approved content"
    );
}

#[tokio::test]
async fn a_decision_can_only_be_delivered_once() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let socket = dir.path().join("agentd.sock");
    let options = daemon::Options {
        db: dir.path().join("agentd.db"),
        socket: socket.clone(),
        workspace: dir.path().join("workspace"),
        model: claude::DEFAULT_MODEL.to_string(),
        max_tokens: claude::DEFAULT_MAX_TOKENS,
        tick: Duration::from_millis(50),
        api_key: None,
    };
    let daemon = tokio::spawn(daemon::serve(options));

    let mut ready = false;
    for _ in 0..100 {
        if socket.exists() {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(ready, "the daemon never bound its socket");

    let submitted = protocol::call(
        &socket,
        &Request::Submit {
            goal: "summarise the workspace".to_string(),
            max_turns: 6,
            approval_timeout_secs: 300,
        },
    )
    .await
    .expect("the submit is answered");
    let Response::Submitted { execution_id } = submitted else {
        panic!("unexpected answer: {submitted:?}");
    };

    // Wait for the gate, then send the SAME decision twice in a row. The
    // second must be refused: two staged signals would leave one queued for a
    // later call to consume without being shown.
    let mut token = None;
    for _ in 0..200 {
        let answer = protocol::call(
            &socket,
            &Request::Status {
                execution_id: execution_id.clone(),
                full: false,
            },
        )
        .await
        .expect("the status is answered");
        let Response::Session { session } = answer else {
            panic!("unexpected answer: {answer:?}");
        };
        if let Some(pending) = session.pending {
            token = Some(pending.token);
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let token = token.expect("the session never asked for approval");

    let decision = |token: String| Request::Approve {
        execution_id: execution_id.clone(),
        token,
        approved: true,
        note: None,
    };
    let first = protocol::call(&socket, &decision(token.clone()))
        .await
        .expect("the first decision is answered");
    assert!(
        matches!(first, Response::Ack { .. }),
        "the first decision must be accepted: {first:?}"
    );
    let second = protocol::call(&socket, &decision(token))
        .await
        .expect("the second decision is answered");
    assert!(
        matches!(second, Response::Error { .. }),
        "a repeated decision must be refused: {second:?}"
    );

    daemon.abort();
}

#[tokio::test]
async fn a_daemon_flushes_the_path_above_its_workspace() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let root = dir.path().canonicalize().expect("the directory resolves");
    let workspace = root.join("projects/agent/workspace");

    // The write path flushes from a target up to the workspace root. The entry
    // that NAMES the root lives above it, so the chain above is the daemon's
    // to flush at startup.
    let chain = daemon::path_above(&workspace);
    assert_eq!(
        chain.get(..3),
        Some(
            &[
                root.join("projects/agent"),
                root.join("projects"),
                root.clone()
            ][..]
        ),
        "the chain must start in the directory just above the workspace"
    );
    assert_eq!(
        chain.last().map(PathBuf::as_path),
        Some(Path::new("/")),
        "the chain must end at the filesystem root"
    );
    assert!(
        !chain.contains(&workspace),
        "the workspace itself is flushed by every write, not here"
    );

    // A workspace several levels deep is created and served. The startup flush
    // must not stop that.
    let options = daemon::Options {
        db: root.join("agentd.db"),
        socket: root.join("agentd.sock"),
        workspace: workspace.clone(),
        model: claude::DEFAULT_MODEL.to_string(),
        max_tokens: claude::DEFAULT_MAX_TOKENS,
        tick: Duration::from_millis(50),
        api_key: None,
    };
    let daemon = tokio::spawn(daemon::serve(options));
    await_daemon(&root.join("agentd.sock")).await;
    assert!(workspace.is_dir(), "the workspace must exist");
    daemon.abort();
}

#[tokio::test]
async fn a_daemon_refuses_a_database_the_agent_could_write() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");

    // `--workspace .` with the default database name is the natural way into
    // this. The model can write any path in the workspace, and a write
    // replaces its target. The daemon would then run from a file that one
    // approved tool call could destroy.
    let inside = daemon::Options {
        db: workspace.join("agentd.db"),
        socket: dir.path().join("agentd.sock"),
        workspace: workspace.clone(),
        model: claude::DEFAULT_MODEL.to_string(),
        max_tokens: claude::DEFAULT_MAX_TOKENS,
        tick: Duration::from_millis(50),
        api_key: None,
    };
    // A timeout, because a daemon that does NOT refuse runs until `Ctrl-C`.
    // Without it a regression would hang the suite instead of failing it.
    let refusal = tokio::time::timeout(Duration::from_secs(10), daemon::serve(inside))
        .await
        .expect("the daemon must refuse rather than start")
        .expect_err("a database inside the workspace must be refused");
    assert!(
        refusal.contains("inside the workspace"),
        "the refusal must name the problem: {refusal}"
    );
    assert!(
        !dir.path().join("agentd.sock").exists(),
        "a refused daemon must not bind its socket"
    );

    // A directory below the workspace is no better: the model reaches that too.
    std::fs::create_dir_all(workspace.join("state")).expect("the directory is created");
    let nested = daemon::Options {
        db: workspace.join("state/agentd.db"),
        socket: dir.path().join("agentd.sock"),
        workspace: workspace.clone(),
        model: claude::DEFAULT_MODEL.to_string(),
        max_tokens: claude::DEFAULT_MAX_TOKENS,
        tick: Duration::from_millis(50),
        api_key: None,
    };
    assert!(
        tokio::time::timeout(Duration::from_secs(10), daemon::serve(nested))
            .await
            .expect("the daemon must refuse rather than start")
            .expect_err("a nested database must be refused")
            .contains("inside the workspace"),
        "a database below the workspace must be refused too"
    );

    // A link OUTSIDE the workspace can name a target inside it. The lock and
    // `SQLite` both follow the link. A test on the link's own path would
    // report the safe side of a rule the daemon then breaks.
    std::fs::write(workspace.join("real.db"), "").expect("the target exists");
    let link = dir.path().join("linked.db");
    std::os::unix::fs::symlink(workspace.join("real.db"), &link).expect("the link is made");
    let linked = daemon::Options {
        db: link,
        socket: dir.path().join("agentd.sock"),
        workspace: workspace.clone(),
        model: claude::DEFAULT_MODEL.to_string(),
        max_tokens: claude::DEFAULT_MAX_TOKENS,
        tick: Duration::from_millis(50),
        api_key: None,
    };
    assert!(
        tokio::time::timeout(Duration::from_secs(10), daemon::serve(linked))
            .await
            .expect("the daemon must refuse rather than start")
            .expect_err("a linked database must be refused")
            .contains("inside the workspace"),
        "a link into the workspace must be refused"
    );

    // A link that resolves to nothing would create its file wherever it
    // points, so it is refused rather than guessed at.
    let dangling = dir.path().join("dangling.db");
    std::os::unix::fs::symlink(workspace.join("absent.db"), &dangling).expect("the link is made");
    let broken = daemon::Options {
        db: dangling,
        socket: dir.path().join("agentd.sock"),
        workspace,
        model: claude::DEFAULT_MODEL.to_string(),
        max_tokens: claude::DEFAULT_MAX_TOKENS,
        tick: Duration::from_millis(50),
        api_key: None,
    };
    assert!(
        tokio::time::timeout(Duration::from_secs(10), daemon::serve(broken))
            .await
            .expect("the daemon must refuse rather than start")
            .expect_err("a dangling link must be refused")
            .contains("resolves to nothing"),
        "a link to nothing must be refused"
    );
}

/// A workspace a printed command cannot carry is refused before it is used.
///
/// Quoting made the restart hint one shell word, and it cannot make a
/// character visible. `visible` shows a carriage return as `\u{000d}`, so the
/// printed command named a path holding those twelve characters. Copying it
/// would start a daemon on a NEW directory and resume nothing.
///
/// A tab is the quieter case. It survives the quoting into `argv`, and it
/// renders as spaces, so the line an operator READS is not the line they
/// copy. Neither is a fault the hint can fix, so the path is refused where
/// the socket path already is.
#[tokio::test]
async fn a_workspace_no_printed_command_can_carry_is_refused() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    // One database and one socket PER CASE. A daemon that accepted the path
    // would still hold both when the next case ran. That case would then fail
    // on the lock rather than on the path.
    for (index, name) in ["my\rproject", "my\tproject", "my\nproject"]
        .into_iter()
        .enumerate()
    {
        let workspace = dir.path().join(name);
        std::fs::create_dir_all(&workspace).expect("the workspace is created");
        // The refusal is bounded in TIME as well. A daemon that accepted the
        // path would serve forever, and a hang says less than a failure.
        let refused = tokio::time::timeout(
            Duration::from_secs(5),
            daemon::serve(daemon::Options {
                db: dir.path().join(format!("agentd-{index}.db")),
                socket: dir.path().join(format!("agentd-{index}.sock")),
                workspace: workspace.clone(),
                model: claude::DEFAULT_MODEL.to_string(),
                max_tokens: claude::DEFAULT_MAX_TOKENS,
                tick: Duration::from_millis(50),
                api_key: None,
            }),
        )
        .await
        .unwrap_or_else(|_| panic!("the daemon must refuse {name:?} rather than serve it"))
        .expect_err("the daemon must refuse to start");
        assert!(
            refused.contains("no command this daemon prints can carry"),
            "unexpected message for {name:?}: {refused}"
        );
        // The refusal names the path on ONE line, which is the property it
        // exists to protect. A message that split itself would do the damage.
        assert_eq!(
            crate::failure(&refused).lines().count(),
            1,
            "the refusal itself must stay on one line: {refused}"
        );
    }

    // An ordinary name is still accepted, including one that needs quoting.
    let ordinary = dir.path().join("my project; reboot");
    std::fs::create_dir_all(&ordinary).expect("the workspace is created");
    let started = tokio::time::timeout(
        Duration::from_millis(600),
        daemon::serve(daemon::Options {
            db: dir.path().join("ordinary.db"),
            socket: dir.path().join("ordinary.sock"),
            workspace: ordinary,
            model: claude::DEFAULT_MODEL.to_string(),
            max_tokens: claude::DEFAULT_MAX_TOKENS,
            tick: Duration::from_millis(50),
            api_key: None,
        }),
    )
    .await;
    assert!(
        started.is_err(),
        "a path that only needs quoting must be accepted, and the daemon \
         must then keep serving: {started:?}"
    );
}

/// A restart hint parses back to the workspace it names.
///
/// The refusal tells the operator which `--workspace` resumes the session,
/// and the line is made to be copied. A path holding a space split into two
/// arguments, so the copied line named neither workspace and failed before it
/// started. A path holding `;` was worse: a shell ran the rest of the line as
/// a command.
///
/// This asserts the property, and not the spelling. The printed hint is split
/// the way a shell splits it, and the words go to the real parser. See
/// [`words`].
#[tokio::test]
async fn a_restart_hint_parses_back_to_the_workspace_it_names() {
    use clap::Parser;

    /// The backticked span that names the flag, which is the copied command.
    ///
    /// The message also names the workspace in PROSE. That value is read and
    /// not copied, so only the span holding the flag is under test.
    fn hint(message: &str) -> String {
        message
            .split('`')
            .find(|span| span.starts_with("--workspace"))
            .expect("the message holds a workspace hint")
            .to_string()
    }

    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("agentd.db");
    // A name a shell reads as more than one word, and as a command.
    let theirs = dir.path().join("my project; reboot");
    let ours = dir.path().join("ours");
    std::fs::create_dir_all(&theirs).expect("the workspace is created");
    std::fs::create_dir_all(&ours).expect("the workspace is created");

    let calls = Arc::new(AtomicUsize::new(0));
    {
        let mut rt = runtime(&db, &theirs, &calls);
        let exec = rt
            .start_workflow(WORKFLOW_NAME, task(&theirs))
            .expect("the session starts");
        drive_to_approval(&mut rt, exec).await;
    }

    let message = daemon::serve(daemon::Options {
        db: db.clone(),
        socket: dir.path().join("agentd.sock"),
        workspace: ours,
        model: claude::DEFAULT_MODEL.to_string(),
        max_tokens: claude::DEFAULT_MAX_TOKENS,
        tick: Duration::from_millis(50),
        api_key: None,
    })
    .await
    .expect_err("the daemon must refuse to start");
    assert!(
        message.contains("belongs to the workspace"),
        "unexpected message: {message}"
    );

    // The hint is one word to a shell, so it cannot carry a command.
    let printed = hint(&message);
    let split = words(&printed);
    assert_eq!(
        split.len(),
        1,
        "the hint must be ONE shell word: {printed} -> {split:?}"
    );
    // One word proves the SPACE is quoted. The `;` needs its own reading,
    // because a shell ends a command on it and this split does not.
    let mut open = false;
    for character in printed.chars() {
        if character == '\'' {
            open = !open;
        }
        assert!(
            character != ';' || open,
            "a `;` outside the quotes ends the copied command: {printed}"
        );
    }
    assert!(!open, "the quoting must be closed: {printed}");

    // And it parses back to the workspace the session belongs to.
    let argv = vec!["agentd".to_string(), "serve".to_string(), split[0].clone()];
    let cli = crate::Cli::try_parse_from(&argv)
        .unwrap_or_else(|e| panic!("the printed hint must parse: {printed} -> {e}"));
    let crate::Command::Serve { workspace, .. } = cli.command else {
        panic!("the hint must name the serve command: {printed}");
    };
    assert_eq!(
        workspace, theirs,
        "the parsed workspace must be the recorded one: {printed}"
    );

    // Why the value is ATTACHED. A workspace may begin with a dash, and
    // `AGENTD_WORKSPACE` or `--workspace=-x` can put a daemon on one. As a
    // separate word, `clap` reads that value as more options. This is the
    // parser reading both spellings, because a recorded path under a
    // temporary directory is absolute and cannot carry the case.
    let dashed = "-x/project";
    assert!(
        crate::Cli::try_parse_from(["agentd", "serve", "--workspace", dashed]).is_err(),
        "a separate word must fail, which is why the value is attached"
    );
    let attached =
        crate::Cli::try_parse_from(["agentd", "serve", &format!("--workspace={dashed}")])
            .expect("an attached value parses");
    let crate::Command::Serve { workspace, .. } = attached.command else {
        panic!("the attached value must name the serve command");
    };
    assert_eq!(
        workspace,
        Path::new(dashed),
        "an attached value reaches the daemon whole"
    );
}

#[tokio::test]
async fn a_daemon_refuses_to_start_where_a_session_does_not_belong() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("agentd.db");
    let theirs = dir.path().join("their-project");
    let ours = dir.path().join("our-project");
    std::fs::create_dir_all(&theirs).expect("the workspace is created");
    std::fs::create_dir_all(&ours).expect("the workspace is created");

    // A session parked mid-run, recorded against one workspace.
    let calls = Arc::new(AtomicUsize::new(0));
    let exec = {
        let mut rt = runtime(&db, &theirs, &calls);
        let exec = rt
            .start_workflow(WORKFLOW_NAME, task(&theirs))
            .expect("the session starts");
        drive_to_approval(&mut rt, exec).await;
        exec
    };

    // Starting on another workspace must be refused BEFORE anything drives the
    // session. A mismatched tool call fails the run non-retryably, and only
    // `RUNNING` rows are ever driven, so a failure here could never be undone.
    let refused = daemon::serve(daemon::Options {
        db: db.clone(),
        socket: dir.path().join("agentd.sock"),
        workspace: ours,
        model: claude::DEFAULT_MODEL.to_string(),
        max_tokens: claude::DEFAULT_MAX_TOKENS,
        tick: Duration::from_millis(50),
        api_key: None,
    })
    .await;
    let message = refused.expect_err("the daemon must refuse to start");
    assert!(
        message.contains("belongs to the workspace"),
        "unexpected message: {message}"
    );

    // The session is untouched, so the operator can fix the flag and resume.
    let mut rt = runtime(&db, &theirs, &calls);
    assert!(
        matches!(
            rt.outcome(exec),
            Ok(autumn_harvest_sqlite::ExecutionOutcome::Running)
        ),
        "the session must stay resumable"
    );
    let signal = drive_to_approval(&mut rt, exec).await;
    approve(&mut rt, exec, &signal);
    let state = rt.run_until_blocked(exec).await.expect("the run finishes");
    assert!(
        matches!(state, RunState::Completed(_)),
        "the session must still complete, got {state:?}"
    );
}

#[tokio::test]
async fn a_workspace_path_that_cannot_be_written_down_is_refused() {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    let dir = tempfile::tempdir().expect("a temporary directory");

    // A path that is not valid UTF-8 cannot be recorded exactly. A lossy name
    // would never match the real path again, so every tool call in the session
    // would fail.
    let mut raw = OsString::from_vec(b"workspace-\xff".to_vec());
    let workspace = dir.path().join(&mut raw);
    std::fs::create_dir_all(&workspace).expect("the workspace is created");

    let refused = daemon::serve(daemon::Options {
        db: dir.path().join("agentd.db"),
        socket: dir.path().join("agentd.sock"),
        workspace,
        model: claude::DEFAULT_MODEL.to_string(),
        max_tokens: claude::DEFAULT_MAX_TOKENS,
        tick: Duration::from_millis(50),
        api_key: None,
    })
    .await;
    let message = refused.expect_err("the daemon must refuse the path");
    assert!(
        message.contains("not valid UTF-8"),
        "unexpected message: {message}"
    );
}

#[test]
fn an_approval_signal_names_one_wait_and_only_that_wait() {
    // A late decision is recorded in history behind its expired deadline, where
    // it stays unconsumed. Under a name shared with a later wait it would
    // release a call nobody reviewed. So the name carries the turn and the
    // position as well as the tool-use id.
    let first = session::approval_signal(1, 0, "toolu_a");
    let same_call_later_turn = session::approval_signal(2, 0, "toolu_a");
    let same_turn_later_call = session::approval_signal(1, 1, "toolu_a");

    assert_ne!(
        first, same_call_later_turn,
        "a later turn must wait on its own name"
    );
    assert_ne!(
        first, same_turn_later_call,
        "a second call in one turn must wait on its own name"
    );

    // The operator still decides by tool-use id, so the name must give it back.
    for name in [&first, &same_call_later_turn, &same_turn_later_call] {
        assert_eq!(
            session::approval_call_id(name),
            Some("toolu_a"),
            "the call id must survive the round trip: {name}"
        );
    }

    // An id containing the separator still round-trips, and a name that is not
    // an approval signal is not mistaken for one.
    let odd = session::approval_signal(3, 4, "toolu:with:colons");
    assert_eq!(
        session::approval_call_id(&odd),
        Some("toolu:with:colons"),
        "the id is the remainder of the name"
    );
    assert_eq!(session::approval_call_id("something_else:1:0:x"), None);
    assert_eq!(session::approval_call_id("tool_approval"), None);
}

/// Options for a daemon that answers over `socket` and runs the stub model.
fn socket_options(dir: &Path, socket: &Path) -> daemon::Options {
    daemon::Options {
        db: dir.join("agentd.db"),
        socket: socket.to_path_buf(),
        workspace: dir.join("workspace"),
        model: claude::OFFLINE_MODEL.to_string(),
        max_tokens: claude::DEFAULT_MAX_TOKENS,
        tick: Duration::from_millis(50),
        api_key: None,
    }
}

/// Send raw bytes over the control socket and read the answer.
///
/// The answer is absent when the read fails. A caller that sent bytes the
/// daemon never read loses the answer to a connection reset.
async fn send_raw(socket: &Path, wire: &[u8]) -> Option<String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::UnixStream::connect(socket)
        .await
        .expect("the caller connects");
    stream.write_all(wire).await.expect("the caller writes");
    stream.flush().await.expect("the caller flushes");
    let mut answer = String::new();
    let read = tokio::time::timeout(Duration::from_secs(30), stream.read_to_string(&mut answer))
        .await
        .expect("the caller is answered or closed inside the deadline");
    read.ok().map(|_| answer)
}

/// A request padded with whitespace to `len` bytes, which still parses.
fn padded_request(request: &Request, len: usize) -> String {
    let mut padded = serde_json::to_string(request).expect("the request encodes");
    assert!(
        padded.len() <= len,
        "the request is longer than the padding"
    );
    while padded.len() < len {
        padded.push(' ');
    }
    assert!(
        serde_json::from_str::<Request>(padded.trim()).is_ok(),
        "the test proves nothing unless the padded request parses on its own"
    );
    padded
}

#[tokio::test(flavor = "multi_thread")]
async fn a_request_longer_than_the_cap_is_refused() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let socket = dir.path().join("agentd.sock");
    std::fs::create_dir_all(dir.path().join("workspace")).expect("the workspace is created");
    let daemon = tokio::spawn(daemon::serve(socket_options(dir.path(), &socket)));
    await_daemon(&socket).await;

    // The hazard: a request padded with whitespace to exactly the cap parses
    // on its own, because `trim` removes the padding. The caller then sends
    // more bytes, so the padded prefix is not a whole request.
    //
    // The bytes that follow are whitespace. Any prefix the daemon reads
    // therefore still parses, so only the length can refuse this request.
    let cap = daemon::MAX_REQUEST_BYTES;
    let submit = Request::Submit {
        goal: "past the cap".to_string(),
        max_turns: 1,
        approval_timeout_secs: 300,
    };
    let mut wire = padded_request(&submit, cap).into_bytes();
    wire.extend_from_slice(b"        \n");
    send_raw(&socket, &wire).await;

    // A started session is the damage, and the prefix asked for one. The
    // answer is not asserted here: the bytes past the cap stay unread, so the
    // close resets the connection and the caller loses the answer.
    let listed = protocol::call(&socket, &Request::List { before: None })
        .await
        .expect("the list is answered");
    let Response::Sessions { sessions, .. } = listed else {
        panic!("unexpected answer: {listed:?}");
    };
    assert!(
        sessions.is_empty(),
        "a request that continued past the cap started a session: {sessions:?}"
    );

    daemon.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_request_whose_newline_arrives_past_the_cap_is_refused() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let socket = dir.path().join("agentd.sock");
    std::fs::create_dir_all(dir.path().join("workspace")).expect("the workspace is created");
    let daemon = tokio::spawn(daemon::serve(socket_options(dir.path(), &socket)));
    await_daemon(&socket).await;

    // A whole line, one byte past the cap. The newline does arrive, so a
    // daemon that only required a newline would accept this. The length is
    // what refuses it.
    let cap = daemon::MAX_REQUEST_BYTES;
    let mut line = padded_request(&Request::List { before: None }, cap);
    line.push('\n');
    assert_eq!(line.len(), cap + 1);

    // The daemon reads every byte this caller sent, so nothing is left to
    // reset the connection. This caller does read its answer.
    let answer = send_raw(&socket, line.as_bytes())
        .await
        .expect("a caller whose bytes are all read is answered");
    let refusal: Response = serde_json::from_str(answer.trim()).expect("the answer parses");
    let Response::Error { message } = refusal else {
        panic!("a request past the cap must be refused: {refusal:?}");
    };
    assert!(
        message.contains(&cap.to_string()) && message.contains("Shorten the goal or the note"),
        "the refusal must name the cap and what to do: {message}"
    );

    daemon.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_request_that_ends_at_the_cap_is_answered() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let socket = dir.path().join("agentd.sock");
    std::fs::create_dir_all(dir.path().join("workspace")).expect("the workspace is created");
    let daemon = tokio::spawn(daemon::serve(socket_options(dir.path(), &socket)));
    await_daemon(&socket).await;

    // The boundary the extra byte draws. This request ends inside the cap, so
    // refusing it would refuse a caller that sent a whole line.
    let cap = daemon::MAX_REQUEST_BYTES;
    let mut line = padded_request(&Request::List { before: None }, cap - 1);
    line.push('\n');
    assert_eq!(line.len(), cap);
    let answer = send_raw(&socket, line.as_bytes())
        .await
        .expect("a request that ends at the cap is answered");
    let answered: Response = serde_json::from_str(answer.trim()).expect("the answer parses");
    assert!(
        matches!(answered, Response::Sessions { .. }),
        "a request that ends at the cap must be served: {answered:?}"
    );

    daemon.abort();
}

/// A parent swapped after the path resolved is refused, and nothing is
/// written.
///
/// `resolve` proves the chain contained WHEN IT RUNS, and the missing levels
/// are created after that. Another process can put a symbolic link at one of
/// those names in between.
///
/// Measured before the fix: the creation walked THROUGH the link, made a
/// directory outside the workspace, reported success, and the write landed
/// there. `exists` follows a link, so the swapped parent read as an ordinary
/// existing directory and `create_dir` never saw `AlreadyExists` at all.
///
/// The review named the `AlreadyExists` arm. That arm is real but is not the
/// reachable path, which is why both are covered here.
#[test]
fn a_parent_swapped_after_the_path_resolved_is_refused() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("ws");
    let outside = dir.path().join("outside");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");
    std::fs::create_dir_all(&outside).expect("the outside directory is created");

    // The state the window leaves behind, reached directly: `resolve` has
    // already run and passed, and the link appears after it.
    std::os::unix::fs::symlink(&outside, workspace.join("a")).expect("the link is made");
    let parent = workspace.join("a").join("b");

    // The hazard, stated before the refusal. The creation still succeeds and
    // still lands outside, because it cannot know the root. That is exactly
    // why the caller re-proves containment.
    tools::create_enterable(&parent).expect("the creation walks through the link");
    assert!(
        outside.join("b").exists(),
        "the window must be real, or this test proves nothing"
    );

    // The guard the write runs before it creates any scratch file.
    let refused = tools::contained(&parent, &workspace)
        .expect_err("a parent that leaves the workspace must be refused");
    assert!(
        refused.contains("leaves the workspace"),
        "the refusal must say what is wrong: {refused}"
    );
    assert!(
        !outside.join("b").join("note.md").exists(),
        "and no content may be written outside: {refused}"
    );

    // The other arm. A DANGLING link occupies the name, so `exists` reads
    // false, the level is treated as missing, and `create_dir` reports
    // `AlreadyExists`. That was accepted in silence.
    let dangling = workspace.join("c");
    std::os::unix::fs::symlink(outside.join("gone"), &dangling).expect("the link is made");
    assert!(!dangling.exists(), "the link dangles");
    let owned = tools::create_enterable(&dangling)
        .expect_err("a name another process owns is not a directory this call vouches for");
    assert_eq!(
        owned.kind(),
        std::io::ErrorKind::AlreadyExists,
        "and it says what it found: {owned}"
    );

    // The not-the-fault case: an ordinary nested parent inside the workspace
    // is created and accepted, so the guard costs no legitimate write.
    let honest = workspace.join("d").join("e");
    tools::create_enterable(&honest).expect("an ordinary parent is created");
    tools::contained(&honest, &workspace).expect("and it is inside the workspace");

    // The write path must RUN that guard. No test can open the window it
    // closes. The link has to appear between the resolve and the creation,
    // which a test cannot schedule. The source is read instead, as the log
    // guards in this suite do.
    let source = include_str!("tools.rs");
    let body = source
        .split("fn write_file(")
        .nth(1)
        .expect("write_file is in the source");
    let body = &body[..body.find("\nfn ").unwrap_or(body.len())];
    let created = body
        .find("create_enterable(")
        .expect("the parents are created");
    let guard = body
        .find("contained(parent, workspace)")
        .expect("the write path re-proves containment");
    let scratch = body
        .find("create_scratch(")
        .expect("the scratch file is created");
    assert!(
        created < guard && guard < scratch,
        "the guard must run after the creation and BEFORE any scratch file"
    );

    // End to end, the toolbox refuses a link that is there FIRST. That is
    // `resolve`, and it is a different half from the guard above.
    let fresh = dir.path().join("ws2");
    std::fs::create_dir_all(&fresh).expect("the second workspace is created");
    std::os::unix::fs::symlink(&outside, fresh.join("a")).expect("the link is made");
    let body = tools::activity_body(fresh.clone());
    let answered = body(
        serde_json::to_value(session::ToolRequest {
            workspace: fresh.to_string_lossy().into_owned(),
            call: session::ToolCall {
                id: "toolu_probe".to_string(),
                name: "write_file".to_string(),
                input: json!({ "path": "a/b/note.md", "content": "secret" }),
            },
        })
        .expect("the request encodes"),
    )
    .expect("the tool answers");
    assert_eq!(
        answered["output"], "`a/b/note.md` leaves the workspace",
        "a link that is there first is refused by the resolve: {answered}"
    );
    assert!(
        !outside.join("b").join("note.md").exists(),
        "and nothing is written outside by either half"
    );
}

/// One hidden-reply fixture, and everything the daemon must do with it.
fn assert_no_older_call_answers(case: &str, older: &str, schedule: String, newest: String) {
    let scheduled_old = json!({"type":"ActivityScheduled",
           "data":{"activity_id":"act_old","name":"claude_turn","queue":"default"}})
    .to_string();
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("hidden-reply.db");
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    writer
        .execute_batch(
            "CREATE TABLE harvest_events (exec_id TEXT, seq INTEGER, event_json TEXT,              PRIMARY KEY (exec_id, seq));",
        )
        .expect("the fixture schema is created");
    for (seq, row) in [
        (0_i64, scheduled_old),
        (1, older.to_string()),
        (2, schedule),
        (3, newest),
    ] {
        writer
            .execute(
                "INSERT INTO harvest_events VALUES ('e', ?1, ?2)",
                rusqlite::params![seq, row],
            )
            .expect("the row is recorded");
    }
    drop(writer);

    let reader = rusqlite::Connection::open(&db).expect("the database opens");

    // The hazard, stated first. The page still answers with the OLDER reply,
    // because the newest one carries no `stop_reason` it can read. A refusal
    // below is therefore a choice, and not a failure to read.
    let page = inspect::reply_calls(&reader, "e", None, 8).expect("the replies read");
    assert_eq!(
        page.first().map(|reply| reply.0),
        Some(1),
        "[{case}] the page must still name the older reply as its newest: {page:?}"
    );

    // The rows the damage cannot reach say so.
    let evidence = inspect::newer_turn_evidence(&reader, "e", 1).expect("the evidence reads");
    assert_ne!(
        evidence,
        (0, 0),
        "[{case}] a turn after that reply must be visible in another row"
    );

    let signal = session::approval_signal(2, 0, "toolu_same");
    let refused = daemon::pending_call(&reader, "e", &signal, false)
        .expect_err("no call may be offered from an older reply");
    assert!(
        refused.contains("cannot be named"),
        "[{case}] the refusal must say what stopped it: {refused}"
    );
    assert!(
        !refused.contains("secrets.txt") && !refused.contains("read_file"),
        "[{case}] and never name the older call: {refused}"
    );

    // The rendering offers no decision, and still says the session waits.
    let parked = daemon::ParkedState {
        signal: Some(signal),
        reason: "waiting for approval of write_file".to_string(),
    };
    let (pending, blocked_on) = daemon::decidable(&reader, "e", Some(&parked), false);
    assert!(
        pending.is_none(),
        "[{case}] the older call must NEVER be offered: {pending:?}"
    );
    let reason = blocked_on.expect("the session still says why it is parked");
    assert!(
        reason.contains("waiting for approval of write_file") && reason.contains("cannot be named"),
        "[{case}] the reason keeps the wait and names what stopped: {reason}"
    );
    assert!(
        !reason.contains("secrets.txt") && !reason.contains("read_file"),
        "[{case}] and it never names the older call: {reason}"
    );
}

/// A reply the page cannot see does not let an OLDER call answer.
///
/// [`inspect::REPLIES_QUERY`] finds a reply by a `stop_reason` in its own
/// payload. A reply whose payload is damaged is therefore dropped from the
/// page, and the search answered with the newest reply it COULD read.
///
/// Measured before the fix, with the older reply reusing the awaited id: the
/// stale `read_file secrets.txt` was offered beside the current token. The
/// newest-reply rule of the previous fix was defeated by the query that
/// decides which reply is newest.
///
/// The evidence that the damage cannot reach is in other rows. A turn is
/// scheduled in a row of its own, before its reply exists.
#[test]
fn a_reply_the_page_cannot_see_does_not_let_an_older_call_answer() {
    let scheduled = |id: &str| {
        json!({"type":"ActivityScheduled",
               "data":{"activity_id":id,"name":"claude_turn","queue":"default"}})
        .to_string()
    };
    let older = json!({"type":"ActivityCompleted","data":{"activity_id":"act_old","output":{
        "stop_reason":"tool_use",
        "tool_calls":[{"id":"toolu_same","name":"read_file",
                       "input":{"path":"secrets.txt"}}]}}})
    .to_string();

    // Each newest reply holds the awaited call, and each is invisible to the
    // page for a different reason. The last case damages the SCHEDULE too, so
    // no row left can say a turn happened. Only a count of rows with no
    // readable kind catches that one.
    let hidden = json!({"type":"ActivityCompleted","data":{"activity_id":"act_new","output":{
        "tool_calls":[{"id":"toolu_same","name":"write_file",
                       "input":{"path":"notes.md"}}]}}})
    .to_string();
    let cases = [
        ("no stop_reason", scheduled("act_new"), hidden.clone()),
        (
            "a null stop_reason",
            scheduled("act_new"),
            json!({"type":"ActivityCompleted","data":{"activity_id":"act_new","output":{
                "stop_reason":null,
                "tool_calls":[{"id":"toolu_same","name":"write_file",
                               "input":{"path":"notes.md"}}]}}})
            .to_string(),
        ),
        (
            "no valid JSON at all",
            scheduled("act_new"),
            r#"{"type":"ActivityCompleted","data":{"activity_id":"act_new","output":{"#.to_string(),
        ),
        (
            "a schedule with no readable kind either",
            r#"{"type":"ActivityScheduled","data":{"activity_id":"act_new","name":"#.to_string(),
            hidden.clone(),
        ),
        // Below, the schedule stays valid JSON, and no query can settle what
        // it is. A count of invalid JSON reports nothing for every one.
        (
            "a name that is not a string",
            r#"{"type":"ActivityScheduled","data":{"activity_id":"act_new","name":5}}"#.to_string(),
            hidden.clone(),
        ),
        (
            "a schedule with no name",
            r#"{"type":"ActivityScheduled","data":{"activity_id":"act_new"}}"#.to_string(),
            hidden.clone(),
        ),
        (
            "a null name",
            r#"{"type":"ActivityScheduled","data":{"activity_id":"act_new","name":null}}"#
                .to_string(),
            hidden.clone(),
        ),
        (
            "a kind that is not a string",
            r#"{"type":5,"data":{"activity_id":"act_new","name":"claude_turn"}}"#.to_string(),
            hidden.clone(),
        ),
        (
            "a schedule with no kind",
            r#"{"data":{"activity_id":"act_new","name":"claude_turn"}}"#.to_string(),
            hidden.clone(),
        ),
        (
            "data that is not an object",
            r#"{"type":"ActivityScheduled","data":5}"#.to_string(),
            hidden.clone(),
        ),
        (
            "a row that is not an object",
            r#"["ActivityScheduled"]"#.to_string(),
            hidden.clone(),
        ),
        // Below, every query CAN read the row, and two readers disagree on
        // what it says. The database answers with the first value of a
        // repeated key, and the engine reader answers with the last.
        (
            "a repeated kind",
            r#"{"type":"ActivityCompleted","type":"ActivityScheduled",
                "data":{"activity_id":"act_new","name":"claude_turn"}}"#
                .to_string(),
            hidden.clone(),
        ),
        (
            "a repeated name",
            r#"{"type":"ActivityScheduled",
                "data":{"activity_id":"act_new","name":"run_tool","name":"claude_turn"}}"#
                .to_string(),
            hidden.clone(),
        ),
        (
            "repeated data",
            r#"{"type":"ActivityScheduled","data":{"name":"run_tool"},
                "data":{"activity_id":"act_new","name":"claude_turn"}}"#
                .to_string(),
            hidden,
        ),
    ];

    for (case, schedule, newest) in cases {
        assert_no_older_call_answers(case, &older, schedule, newest);
    }
}

/// The evidence reads a newer turn whose ROW is in the wrong storage class.
///
/// A `TEXT` column keeps a stored `BLOB` as a `BLOB`, and an `INTEGER` column
/// keeps text that holds no number. Both were assumed to fail closed here,
/// and an assumption is not a guard. This measures them.
///
/// A `BLOB` payload reads as JSON, so the turn test names it. Text in `seq`
/// sorts above every integer, so the bound admits the row. Neither shape can
/// hide a newer turn.
///
/// The `exec_id` of the row is the one class this cannot reach. See
/// [`inspect::UNCLASSIFIED_AFTER_QUERY`].
#[test]
fn the_evidence_reads_a_newer_turn_in_the_wrong_storage_class() {
    let turn = r#"{"type":"ActivityScheduled","data":{"activity_id":"a","name":"claude_turn"}}"#;
    let older = json!({"type":"ActivityCompleted","data":{"activity_id":"act_old","output":{
        "stop_reason":"tool_use",
        "tool_calls":[{"id":"toolu_same","name":"read_file",
                       "input":{"path":"secrets.txt"}}]}}})
    .to_string();
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("classes.db");
    let conn = rusqlite::Connection::open(&db).expect("the database opens");
    conn.execute_batch(
        "CREATE TABLE harvest_events (exec_id TEXT, seq INTEGER, event_json TEXT, \
         PRIMARY KEY (exec_id, seq));",
    )
    .expect("the fixture schema is created");

    for (case, sql, class) in [
        (
            "a payload stored as bytes",
            "INSERT INTO harvest_events VALUES (?1, 1, cast(?2 as blob))",
            "SELECT typeof(event_json) FROM harvest_events WHERE exec_id = ?1 AND seq = 1",
        ),
        (
            "a sequence number stored as text",
            "INSERT INTO harvest_events VALUES (?1, 'later', ?2)",
            "SELECT typeof(seq) FROM harvest_events WHERE exec_id = ?1 AND seq = 'later'",
        ),
    ] {
        conn.execute(
            "INSERT INTO harvest_events VALUES (?1, 0, ?2)",
            rusqlite::params![case, older],
        )
        .expect("the older reply is recorded");
        conn.execute(sql, rusqlite::params![case, turn])
            .expect("the newer turn is recorded");
        let stored: String = conn
            .query_row(class, rusqlite::params![case], |row| row.get(0))
            .expect("the storage class reads");
        assert_ne!(
            stored, "",
            "[{case}] the fixture must store the value it means to"
        );

        let evidence = inspect::newer_turn_evidence(&conn, case, 0).expect("the evidence reads");
        assert_ne!(
            evidence,
            (0, 0),
            "[{case}] a turn in class {stored} must still be counted"
        );
        let signal = session::approval_signal(2, 0, "toolu_same");
        let refused = daemon::pending_call(&conn, case, &signal, false)
            .expect_err("no call may be offered from an older reply");
        assert!(
            !refused.contains("secrets.txt"),
            "[{case}] and the older call is never named: {refused}"
        );
    }
}

/// An ordinary row after the newest reply is still evidence of NOTHING.
///
/// The count above must answer for damage only. A refusal on a healthy
/// history would park every session an operator cannot then release.
///
/// Each row here carries a kind the daemon reads, and none of them is a model
/// turn. A tool schedule names another activity. A completion and a signal
/// carry no name to read, and the count must not ask them for one.
#[test]
fn an_ordinary_row_after_the_newest_reply_counts_as_nothing() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("healthy.db");
    let conn = rusqlite::Connection::open(&db).expect("the database opens");
    conn.execute_batch(
        "CREATE TABLE harvest_events (exec_id TEXT, seq INTEGER, event_json TEXT, \
         PRIMARY KEY (exec_id, seq));",
    )
    .expect("the fixture schema is created");
    let rows = [
        r#"{"type":"ActivityScheduled","data":{"activity_id":"t1","name":"run_tool"}}"#,
        r#"{"type":"ActivityCompleted","data":{"activity_id":"t1","output":{"ok":true}}}"#,
        r#"{"type":"SignalReceived","data":{"name":"tool_approval:2:0:toolu_same"}}"#,
        r#"{"type":"ExecutionStarted"}"#,
        r#"{"type":"TimerFired","data":null}"#,
    ];
    for (seq, row) in rows.iter().enumerate() {
        conn.execute(
            "INSERT INTO harvest_events VALUES ('e', ?1, ?2)",
            rusqlite::params![i64::try_from(seq).expect("the fixture is small"), row],
        )
        .expect("the row is recorded");
    }
    assert_eq!(
        inspect::newer_turn_evidence(&conn, "e", -1).expect("the evidence reads"),
        (0, 0),
        "no ordinary row is evidence of a turn this daemon cannot name"
    );
}

/// A REAL parked session still shows its call, with the evidence read live.
///
/// The refusals above must cost nothing a healthy history needs. This runs
/// the stub model to a parked approval, so the rows are the engine's own.
/// A turn is scheduled before each reply, and tool events follow the newest
/// one.
#[tokio::test]
async fn a_live_parked_session_still_shows_its_awaited_call() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");
    std::fs::write(workspace.join("README.md"), "hello").expect("the fixture is written");
    let db = dir.path().join("agentd.db");
    let calls = Arc::new(AtomicUsize::new(0));
    let mut rt = runtime(&db, &workspace, &calls);
    let exec = rt
        .start_workflow(WORKFLOW_NAME, task(&workspace))
        .expect("the session starts");
    let signal = drive_to_approval(&mut rt, exec).await;
    drop(rt);

    let reader = rusqlite::Connection::open(&db).expect("the database opens");
    let exec_id = exec.to_string();

    // The turn test matches the engine's OWN schedule rows. Every fixture
    // above writes the field names this query reads, so a fixture cannot
    // prove the names are the ones the engine records. Read from the start of
    // the history, every turn of this run is counted.
    let (turns, unclassified) =
        inspect::newer_turn_evidence(&reader, &exec_id, i64::MIN).expect("the evidence reads");
    assert!(
        turns > 0,
        "the engine records a turn this query can name: {turns} turns, \
         {unclassified} unclassified"
    );
    assert_eq!(
        unclassified, 0,
        "and no row of a healthy history is unclassified"
    );

    // Tool events sit after the newest reply, and none of them is a turn.
    // This is what the guard must not mistake for a newer reply.
    let page = inspect::reply_calls(&reader, &exec_id, None, 1).expect("the replies read");
    let seq = page.first().expect("a reply is recorded").0;
    assert_eq!(
        inspect::newer_turn_evidence(&reader, &exec_id, seq).expect("the evidence reads"),
        (0, 0),
        "a healthy history records no turn after its newest reply"
    );

    let found = daemon::pending_call(&reader, &exec_id, &signal, false)
        .expect("the replies read")
        .expect("a parked session shows the call it waits on");
    assert_eq!(
        found.tool, "write_file",
        "the awaited call answers: {found:?}"
    );
    assert_eq!(found.token, signal, "beside its own token");
}

/// One reply carrying a tool call with this id.
fn reply_with_call_id(id: &str) -> TurnReply {
    TurnReply {
        content: json!([{ "type": "tool_use", "id": id, "name": "write_file",
                          "input": { "path": "notes.md", "content": "x" } }]),
        stop_reason: "tool_use".to_string(),
        text: String::new(),
        tool_calls: vec![ToolCall {
            id: id.to_string(),
            name: "write_file".to_string(),
            input: json!({ "path": "notes.md", "content": "x" }),
        }],
    }
}

/// The decision an operator sends for one approval token, as a wire line.
fn decision_frame(token: &str) -> String {
    let mut line = serde_json::to_string(&Request::Approve {
        execution_id: "ffffffff-1111-2222-3333-444444444444".to_string(),
        token: token.to_string(),
        approved: true,
        note: None,
    })
    .expect("the request encodes");
    line.push('\n');
    line
}

/// A tool-use id too long to decide is refused when the reply arrives.
///
/// The id becomes part of the approval token, and a decision crosses the
/// control socket as one line under the request cap. The reply itself is
/// RECORDABLE, so the durable cap never sees this. A reply costs about twice
/// its ids, and the request cap is about half the recorded cap.
///
/// Measured before the fix, at an id of 1048456 bytes. The reply records in
/// 2097137 bytes, under the 2097152-byte cap. Its decision needs 1048585
/// bytes against a 1048576-byte cap. The call could then be neither approved
/// nor denied, and the session parked until its deadline.
#[test]
fn an_id_too_long_to_decide_is_refused() {
    let cap = daemon::MAX_REQUEST_BYTES;

    // The hazard, stated first. This reply is recordable, so nothing else
    // refuses it, and its decision does not fit.
    let undecidable = "a".repeat(1_048_456);
    let reply = reply_with_call_id(&undecidable);
    assert!(
        claude::activity_refusal_for_oversized_reply(&reply).is_none(),
        "the durable cap must NOT catch this, or the test proves nothing"
    );
    let frame = decision_frame(&session::approval_signal(2, 0, &undecidable));
    assert!(
        frame.len() > cap,
        "the decision must not fit the control frame: {} against {cap}",
        frame.len()
    );

    // So the reply is refused where a call is checked for being addressable.
    assert!(
        !claude::has_addressable_calls(&reply),
        "a call nobody can approve or deny is not addressable"
    );

    // The boundary. An id AT the cap is accepted, and its decision fits with
    // room to spare, which is what proves the allowance is not a guess.
    let longest = "a".repeat(claude::MAX_CALL_ID_BYTES);
    assert!(
        claude::has_addressable_calls(&reply_with_call_id(&longest)),
        "the longest allowed id must still be addressable"
    );
    let widest = decision_frame(&session::approval_signal(u32::MAX, usize::MAX, &longest));
    assert!(
        widest.len() <= cap,
        "the widest decision for the longest id must fit: {} against {cap}",
        widest.len()
    );
    assert!(
        cap - widest.len() >= 64,
        "and leave room for a short note: {} spare",
        cap - widest.len()
    );

    // An ordinary id is untouched by any of this.
    assert!(
        claude::has_addressable_calls(&reply_with_call_id("toolu_01A2b3C4d5E6f7")),
        "a real tool-use id must still be addressable"
    );
}

/// The identity a daemon started with these two settings records.
///
/// This is the value `check_resumable` compares a recorded session against.
/// Following a restart hint can therefore be checked against the same
/// function the hint exists to satisfy.
fn identity_for(key: Option<&str>, model: &str) -> String {
    claude::ModelConfig::new(
        key.map(str::to_string),
        model,
        claude::DEFAULT_MAX_TOKENS,
        crate::shutdown::channel().1,
    )
    .expect("the configuration builds")
    .identity()
}

/// A model restart hint names a command that actually resumes the session.
///
/// The recorded identity is not the `--model` flag. A daemon holding a key
/// records the model it was given, and one without a key records the stub.
///
/// Measured before the fix, in both directions. An offline session under a
/// daemon with a key was told `--model=offline-stub`, which that daemon
/// refuses outright. A live session under a daemon with no key was told
/// `--model=claude-opus-5`, which builds and still records the stub, so the
/// same refusal arrives again.
#[tokio::test]
async fn a_model_restart_hint_names_a_command_that_works() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");
    let calls = Arc::new(AtomicUsize::new(0));
    let live_model = "claude-opus-5";

    for (case, recorded, key) in [
        (
            "an offline session under a daemon with a key",
            claude::OFFLINE_MODEL,
            Some("sk-ant-example".to_string()),
        ),
        (
            "a live session under a daemon with no key",
            live_model,
            None,
        ),
    ] {
        let db = dir
            .path()
            .join(format!("{}.db", recorded.replace('-', "_")));
        {
            // The runtime creates the schema. A live row is written directly,
            // because the offline stub refuses to serve a live identity.
            let mut rt = runtime(&db, &workspace, &calls);
            let exec = rt
                .start_workflow(WORKFLOW_NAME, task(&workspace))
                .expect("the session starts");
            drive_to_approval(&mut rt, exec).await;
        }
        if recorded != claude::OFFLINE_MODEL {
            let writer = rusqlite::Connection::open(&db).expect("the database opens");
            writer
                .execute(
                    "INSERT INTO harvest_executions \
                     (exec_id, workflow_name, workflow_id, input_json, state) \
                     VALUES (?1, ?2, '', ?3, 'RUNNING')",
                    rusqlite::params![
                        "ffffffff-1111-2222-3333-444444444444",
                        WORKFLOW_NAME,
                        task_on(&workspace, recorded).to_string()
                    ],
                )
                .expect("the live session is recorded");
        }

        let message = daemon::serve(daemon::Options {
            db,
            socket: dir.path().join(format!("{recorded}.sock")),
            workspace: workspace.clone(),
            model: claude::DEFAULT_MODEL.to_string(),
            max_tokens: claude::DEFAULT_MAX_TOKENS,
            tick: Duration::from_millis(50),
            api_key: key,
        })
        .await
        .expect_err("the daemon must refuse to start");

        if recorded == claude::OFFLINE_MODEL {
            // The key is what records a real identity, so the key is what has
            // to go. The old advice named a model this daemon refuses.
            assert!(
                message.contains("Unset `ANTHROPIC_API_KEY`"),
                "[{case}] the hint must name the key: {message}"
            );
            assert!(
                !message.contains("--model=offline-stub"),
                "[{case}] and must not advertise a model this daemon refuses: {message}"
            );
            // Following it resumes the session: the identity then matches.
            assert_eq!(
                identity_for(None, claude::DEFAULT_MODEL),
                recorded,
                "[{case}] unsetting the key must record the identity the session has"
            );
        } else {
            // A flag alone cannot make this daemon live, so the hint names
            // both halves. The old advice named only the flag.
            assert!(
                message.contains("Set `ANTHROPIC_API_KEY`")
                    && message.contains(&format!("--model={recorded}")),
                "[{case}] the hint must name the key AND the model: {message}"
            );
            assert_eq!(
                identity_for(Some("sk-ant-example"), recorded),
                recorded,
                "[{case}] both together must record the identity the session has"
            );
            // The flag on its own is what the old hint advertised.
            assert_ne!(
                identity_for(None, recorded),
                recorded,
                "[{case}] the flag alone must be known NOT to resume it"
            );
        }
    }

    // Two live daemons on different models need only the flag, and that hint
    // is unchanged. The key is already set in that case.
    assert_eq!(
        identity_for(Some("sk-ant-example"), live_model),
        live_model,
        "a live daemon records the model it was given"
    );
}

/// A read follows no directory swapped in after the path resolved.
///
/// `O_NOFOLLOW` refuses a link at the FINAL component only, so an
/// intermediate component replaced between `resolve` and the open sent the
/// open THROUGH it.
///
/// Measured before the fix: the open succeeded and `read_file` returned the
/// contents of a file outside the workspace.
///
/// A descriptor pins the file it opened, so this window CLOSES rather than
/// narrowing. A swap after the proof cannot change what the descriptor reads.
#[test]
fn a_read_follows_no_directory_swapped_in_after_the_path_resolved() {
    use std::os::unix::fs::OpenOptionsExt;

    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("ws");
    let outside = dir.path().join("outside");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");
    std::fs::create_dir_all(&outside).expect("the outside directory is created");
    std::fs::write(outside.join("passwd"), b"root:x:0:0").expect("the secret is written");

    let open = |path: &Path| {
        std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits().cast_signed())
            .open(path)
    };

    // The state the window leaves, reached directly: `resolve` has already
    // run and passed, and the link appears after it.
    std::os::unix::fs::symlink(&outside, workspace.join("a")).expect("the link is made");
    let escaped = workspace.join("a").join("passwd");

    // The hazard, stated before the refusal. The open still succeeds and
    // still reads the outside file, because the link is not the final
    // component. That is why the descriptor has to be proved.
    let file = open(&escaped).expect("the open walks through the link");
    let mut text = String::new();
    {
        use std::io::Read;
        let mut handle = &file;
        handle
            .read_to_string(&mut text)
            .expect("the outside file reads");
    }
    assert_eq!(
        text, "root:x:0:0",
        "the window must be real, or this test proves nothing"
    );

    // The proof the read now runs before it returns any bytes.
    let refused = tools::opened_inside(&file, &escaped, &workspace)
        .expect_err("a path that leaves the workspace must be refused");
    assert!(
        refused.contains("leaves the workspace"),
        "the refusal must say what is wrong: {refused}"
    );

    // The identity half. Containment holds here, and the descriptor is still
    // a DIFFERENT file, because the name was replaced after it was opened.
    let target = workspace.join("real.txt");
    std::fs::write(&target, b"mine").expect("the file is written");
    let pinned = open(&target).expect("the honest file opens");
    tools::opened_inside(&pinned, &target, &workspace).expect("it is inside the workspace");
    std::fs::remove_file(&target).expect("the file is removed");
    std::fs::write(&target, b"theirs").expect("another file takes the name");
    let swapped = tools::opened_inside(&pinned, &target, &workspace)
        .expect_err("a descriptor holding another file must be refused");
    assert!(
        swapped.contains("no longer the file that path names"),
        "the refusal must name what changed: {swapped}"
    );

    // The read path must RUN the proof, and no test can open the window it
    // closes. The link has to appear between the resolve and the open, which
    // a test cannot schedule. The source is read instead, as the other
    // guards in this suite are.
    let source = include_str!("tools.rs");
    let body = source
        .split("fn read_file(")
        .nth(1)
        .expect("read_file is in the source");
    let body = &body[..body.find("\nfn ").unwrap_or(body.len())];
    let opened = body.find("open_regular(").expect("the file is opened");
    let proof = body
        .find("opened_inside(&file")
        .expect("the read path proves what it opened");
    let read = body.find("read_to_end(").expect("the file is read");
    assert!(
        opened < proof && proof < read,
        "the proof must run after the open and BEFORE any bytes are read"
    );
}

/// A note too large to deliver is refused where the operator types it.
///
/// The decision crosses the socket as one request and is delivered as one
/// SIGNAL. The signal cap is a quarter of the request cap, so a note can pass
/// the socket and still be undeliverable.
///
/// Measured before the fix: a note of 262144 bytes encodes to 262171, over
/// the 262144-byte signal cap, inside a 262290-byte request the socket
/// accepts. The backend then refused the delivery, so the approve command the
/// status advertises could not answer the call.
#[tokio::test(flavor = "multi_thread")]
async fn a_note_too_large_to_deliver_is_refused() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");
    let socket = dir.path().join("agentd.sock");
    let daemon = tokio::spawn(daemon::serve(socket_options(dir.path(), &socket)));
    await_daemon(&socket).await;

    let submitted = protocol::call(
        &socket,
        &Request::Submit {
            goal: "summarise the workspace".to_string(),
            max_turns: 6,
            approval_timeout_secs: 300,
        },
    )
    .await
    .expect("the submit is answered");
    let Response::Submitted { execution_id } = submitted else {
        panic!("unexpected answer: {submitted:?}");
    };
    await_parked(&socket, &execution_id).await;

    // The token the operator reads, from the status they read it in.
    let answer = protocol::call(
        &socket,
        &Request::Status {
            execution_id: execution_id.clone(),
            full: false,
        },
    )
    .await
    .expect("the status is answered");
    let Response::Session { session } = answer else {
        panic!("unexpected answer: {answer:?}");
    };
    let token = session
        .pending
        .expect("the session offers a call to decide")
        .token;

    // The hazard: this note passes the request cap and not the signal cap.
    let note = "n".repeat(usize::try_from(daemon::SIGNAL_CAP_BYTES).expect("the cap fits"));
    let decision = Request::Approve {
        execution_id: execution_id.clone(),
        token: token.clone(),
        approved: true,
        note: Some(note.clone()),
    };
    let mut line = serde_json::to_string(&decision).expect("the request encodes");
    line.push('\n');
    assert!(
        line.len() <= daemon::MAX_REQUEST_BYTES,
        "the request must still fit the socket, or this tests the wrong cap: {}",
        line.len()
    );

    let refused = protocol::call(&socket, &decision)
        .await
        .expect("the decision is answered");
    let Response::Error { message } = refused else {
        panic!("a note past the signal cap must be refused: {refused:?}");
    };
    assert!(
        message.contains(&daemon::SIGNAL_CAP_BYTES.to_string())
            && message.contains("Shorten the note"),
        "the refusal must name the cap and what to do: {message}"
    );

    // The call is still decidable, which is the point of refusing early.
    let accepted = protocol::call(
        &socket,
        &Request::Approve {
            execution_id,
            token,
            approved: true,
            note: Some("short".to_string()),
        },
    )
    .await
    .expect("the decision is answered");
    assert!(
        matches!(accepted, Response::Ack { .. }),
        "a note inside the cap must still be delivered: {accepted:?}"
    );

    daemon.abort();
}

/// A listing names entries of the directory it OPENED.
///
/// `read_dir` takes a path, so it resolves the name a second time. A
/// directory swapped in between was listed instead, and the model reads that
/// listing, so names from outside the workspace reached it.
///
/// The listing is now read from the descriptor. This proves the property
/// directly. The path is replaced with a link to another directory AFTER the
/// open, and the entries are still the ones that were opened.
#[test]
fn a_listing_names_entries_of_the_directory_it_opened() {
    use std::os::unix::fs::OpenOptionsExt;

    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("ws");
    let outside = dir.path().join("outside");
    let inside = workspace.join("sub");
    std::fs::create_dir_all(&inside).expect("the workspace is created");
    std::fs::create_dir_all(&outside).expect("the outside directory is created");
    std::fs::write(inside.join("mine.txt"), b"x").expect("the inside file is written");
    std::fs::write(outside.join("secret.txt"), b"x").expect("the outside file is written");

    let handle = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(
            rustix::fs::OFlags::NOFOLLOW.bits().cast_signed()
                | rustix::fs::OFlags::DIRECTORY.bits().cast_signed(),
        )
        .open(&inside)
        .expect("the directory opens");
    tools::opened_inside(&handle, &inside, &workspace).expect("it is inside the workspace");

    // The swap lands after the open, which is the window a path read loses.
    // The directory is MOVED rather than removed, so it still holds its
    // entries and the descriptor still names them.
    std::fs::rename(&inside, workspace.join("moved")).expect("the directory is moved");
    std::os::unix::fs::symlink(&outside, &inside).expect("the link takes its place");

    // A path read would now name the outside entry. This is the hazard.
    let by_path: Vec<String> = std::fs::read_dir(&inside)
        .expect("the path reads")
        .map(|entry| {
            entry
                .expect("the entry reads")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    assert!(
        by_path.contains(&"secret.txt".to_string()),
        "a path read must be shown to follow the swap: {by_path:?}"
    );

    // The descriptor read does not.
    let names: Vec<String> = rustix::fs::Dir::read_from(&handle)
        .expect("the descriptor reads")
        .map(|entry| {
            String::from_utf8_lossy(entry.expect("the entry reads").file_name().to_bytes())
                .into_owned()
        })
        .collect();
    assert!(
        names.contains(&"mine.txt".to_string()),
        "the opened directory's own entry must be named: {names:?}"
    );
    assert!(
        !names.contains(&"secret.txt".to_string()),
        "and nothing from outside the workspace: {names:?}"
    );

    // The proof the listing runs before it names anything, and that the
    // entries come from the handle rather than the path.
    let source = include_str!("tools.rs");
    let body = source
        .split("fn list_files(")
        .nth(1)
        .expect("list_files is in the source");
    let body = &body[..body.find("\nfn ").unwrap_or(body.len())];
    assert!(
        !body.contains("read_dir("),
        "the listing must not resolve the path a second time"
    );
    let opened = body
        .find("open_directory(")
        .expect("the directory is opened");
    let proof = body
        .find("opened_inside(&handle")
        .expect("the listing proves what it opened");
    let read = body
        .find("Dir::read_from(&handle)")
        .expect("the entries come from the descriptor");
    assert!(
        opened < proof && proof < read,
        "the proof must run after the open and BEFORE any entry is named"
    );
}

/// A listing never shows a report the single status refuses.
///
/// Each field is projected on its own, so a document that repeats one of the
/// four declared keys answers every projection. `json_type` reports the FIRST
/// value of a repeated key, so a repeat of another type passes the type
/// guards too.
///
/// Measured before the fix: `list` rendered `[end_turn after 1 turns, 1 tool
/// calls] ok` for a document `serde` refuses, while `status` showed nothing.
///
/// The cases are chosen against what `serde` actually does, not against what
/// looks damaged. A repeated key the report does not DECLARE deserialises.
/// So does an unpaired surrogate in such a key. Both must still show their
/// report.
#[test]
fn a_listing_never_shows_a_report_the_status_refuses() {
    let cases: [(&str, &str); 6] = [
        (
            "a repeated declared key",
            r#"{"answer":"ok","turns":1,"tool_calls":1,"stop":"end_turn","stop":"refusal"}"#,
        ),
        (
            "a repeated declared key of another type",
            r#"{"answer":"ok","turns":1,"turns":"two","tool_calls":1,"stop":"end_turn"}"#,
        ),
        (
            "an unpaired surrogate in the answer",
            r#"{"answer":"a\ud800b","turns":1,"tool_calls":1,"stop":"end_turn"}"#,
        ),
        (
            "a repeated key the report does not declare",
            r#"{"answer":"ok","turns":1,"tool_calls":1,"stop":"end_turn","x":1,"x":2}"#,
        ),
        (
            "an unpaired surrogate in a key it does not declare",
            r#"{"answer":"ok","turns":1,"tool_calls":1,"stop":"end_turn","x":"a\ud800b"}"#,
        ),
        (
            "a readable report",
            r#"{"answer":"ok","turns":1,"tool_calls":1,"stop":"end_turn"}"#,
        ),
    ];

    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("report-divergence.db");
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    fixture_table(&writer);
    for (index, (_, document)) in cases.iter().enumerate() {
        writer
            .execute(
                "INSERT INTO harvest_executions VALUES (?1, ?2, 'COMPLETED', ?3, ?4, NULL)",
                rusqlite::params![
                    format!("exec-{index}"),
                    WORKFLOW_NAME,
                    READABLE_TASK,
                    document
                ],
            )
            .expect("the row is recorded");
    }
    drop(writer);

    let reader = inspect::open(&db).expect("the reader opens");
    let (views, _, _) = daemon::sessions(&reader, &daemon::Parked::new(), false, None)
        .expect("the listing renders");

    for (index, (case, document)) in cases.iter().enumerate() {
        let exec = format!("exec-{index}");
        let shown = views
            .iter()
            .find(|view| view.execution_id == exec)
            .unwrap_or_else(|| panic!("[{case}] the session is listed"))
            .answer
            .clone();
        // The single status reads the WHOLE document, and the listing must
        // not claim more than that read allows.
        let whole = serde_json::from_str::<SessionReport>(document).is_ok();
        if whole {
            assert_eq!(
                shown.as_deref(),
                Some("[end_turn after 1 turns, 1 tool calls] ok"),
                "[{case}] a document the status reads must keep its report"
            );
        } else {
            assert_eq!(
                shown.as_deref(),
                Some("<unreadable report>"),
                "[{case}] a document the status refuses must not be shown as a result"
            );
        }
    }
}

/// A database replaced while the daemon starts is refused.
///
/// The lock is held on an INODE, and the runtime is handed a PATH, because
/// that is how `SQLite` names its write-ahead log. A file replaced between
/// the two leaves the lock on the file that was there and the runtime on the
/// one that is there now. A second daemon can then lock the replacement and
/// open it too. Two runtimes would write one database, and each would reclaim
/// work the other is running.
///
/// The swap is applied directly, which is the state that window leaves.
#[test]
fn a_database_replaced_while_the_daemon_starts_is_refused() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("agentd.db");
    std::fs::write(&db, b"").expect("the database file is created");

    let lock = guard::acquire(&db).expect("the lock is taken");
    // The not-the-fault case: nothing moved, so the check costs a start
    // nothing.
    lock.still_names(&db)
        .expect("the locked file is the named file");

    // The hazard: another process replaces the file atomically. The lock is
    // still held, and it is held on a file this path no longer names.
    let replacement = dir.path().join("other.db");
    std::fs::write(&replacement, b"").expect("the replacement is created");
    std::fs::rename(&replacement, &db).expect("the database is replaced");

    // A second lock on the NEW file succeeds, which is the damage: two
    // daemons would each believe they are the only writer.
    let second = guard::acquire(&db).expect("the replacement locks freely");
    drop(second);

    let refused = lock
        .still_names(&db)
        .expect_err("a replaced database must be refused");
    assert!(
        refused.contains("was replaced while this daemon started"),
        "the refusal must say what happened: {refused}"
    );

    // The open is NOT inert. It flips every RUNNING task of the database it
    // opens back to PENDING. Opening a file swapped in here re-queues the
    // live work of the daemon that owns it.
    let other = dir.path().join("other-daemon.db");
    {
        let runtime = SqliteRuntime::open(&other).expect("the other database opens");
        drop(runtime);
    }
    let writer = rusqlite::Connection::open(&other).expect("the database opens");
    writer
        .execute(
            "INSERT INTO harvest_tasks (task_id, exec_id, activity_id, name, input_json, \
             queue, state, attempt, run_at, seq, scheduled_at) \
             VALUES ('t1', 'e1', 'a1', 'claude_turn', '{}', 'default', 'RUNNING', 0, 0, 1, 0)",
            [],
        )
        .expect("a live task is recorded");
    let running = |conn: &rusqlite::Connection| -> i64 {
        conn.query_row(
            "SELECT count(*) FROM harvest_tasks WHERE state = 'RUNNING'",
            [],
            |row| row.get(0),
        )
        .expect("the count answers")
    };
    assert_eq!(running(&writer), 1, "the other daemon has live work");
    drop(writer);

    // Opening it is what does the damage, which is why the check has to run
    // BEFORE the open and not only after it.
    drop(SqliteRuntime::open(&other).expect("the swapped database opens"));
    let reader = rusqlite::Connection::open(&other).expect("the database opens");
    assert_eq!(
        running(&reader),
        0,
        "opening another daemon's database re-queues its live work"
    );

    // So the start proves the identity on BOTH sides of the open. No test can
    // schedule a swap between two system calls. The order is read from the
    // source instead, as the other guards in this suite are.
    let source = include_str!("daemon.rs");
    let body = source
        .split("pub async fn serve(")
        .nth(1)
        .expect("serve is in the source");
    let opened = body
        .find("SqliteRuntime::open(&options.db)")
        .expect("the runtime is opened");
    let before = body
        .find("lock.still_names(&options.db)")
        .expect("the identity is proved before the open");
    let after = body[opened..]
        .find("lock.still_names(&options.db)")
        .map(|at| at + opened)
        .expect("the identity is proved after the open");
    assert!(
        before < opened && opened < after,
        "the identity must be proved on both sides of the open"
    );
}

/// One reply body with this content and stop reason.
fn reply_body(content: &Value, stop: &str) -> TurnReply {
    claude::parse_reply(&json!({
        "id": "msg_1",
        "type": "message",
        "role": "assistant",
        "model": "claude-opus-5",
        "content": content,
        "stop_reason": stop,
    }))
}

/// A turn cut short at the output cap is a finished session, not a failure.
///
/// The reply validation exists for what comes AFTER a turn: the blocks are
/// replayed into the next request, and the calls reach the approval gate. A
/// turn stopped at the cap has neither. The loop drops its calls and ends the
/// session under `max_tokens`.
///
/// Measured before the fix: a `max_tokens` reply cut off inside a block was
/// refused. An already-billed answer ended the session FAILED, where the loop
/// would have ended it with a report.
///
/// The narrowing is to that ONE stop reason. `end_turn` also ends the
/// session, and its report carries the answer. A block accepted unchecked
/// there could show an earlier turn's text as a clean finish.
#[test]
fn a_turn_cut_short_at_the_cap_is_not_refused() {
    // Every shape a cut leaves partway through a block.
    let cut = [
        (
            "a tool_use with no input",
            json!([{"type":"text","text":"I will write"},
                   {"type":"tool_use","id":"toolu_a","name":"write_file"}]),
        ),
        (
            "a tool_use with no name",
            json!([{"type":"text","text":"I will write"},
                   {"type":"tool_use","id":"toolu_a"}]),
        ),
        (
            "a tool_use with nothing but its type",
            json!([{"type":"text","text":"I will write"}, {"type":"tool_use"}]),
        ),
        (
            "a text block cut to nothing",
            json!([{"type":"text","text":""}]),
        ),
    ];

    for (case, content) in &cut {
        // The hazard: the block really is one the API would refuse back.
        // This is a choice about when that matters, and not a failure to see
        // it.
        let truncated = reply_body(content, "max_tokens");
        assert!(
            !claude::has_replayable_content(&truncated),
            "[{case}] the block must be unreplayable, or this proves nothing"
        );
        assert_eq!(
            claude::malformed_reply(&truncated),
            None,
            "[{case}] a turn stopped at the cap must not be refused"
        );

        // The SAME content under a stop reason that continues the session is
        // still refused, because that reply is replayed.
        let continuing = reply_body(content, claude::STOP_TOOL_USE);
        assert!(
            claude::malformed_reply(&continuing).is_some(),
            "[{case}] a turn that asks for a tool must still be refused"
        );

        // And under the reason whose report carries the answer.
        let ending = reply_body(content, "end_turn");
        assert!(
            claude::malformed_reply(&ending).is_some(),
            "[{case}] a turn that reports an answer must still be refused"
        );
    }

    // The ordinary case: a whole reply that merely stopped at the cap was
    // never refused, and still is not.
    let whole = reply_body(
        &json!([{"type":"text","text":"as far as I got"}]),
        "max_tokens",
    );
    assert!(claude::has_replayable_content(&whole));
    assert_eq!(claude::malformed_reply(&whole), None);

    // The loop is what makes this safe. A reply stopped at the cap ends the
    // session, so its content is never replayed and its calls never run.
    let source = include_str!("session.rs");
    let body = source
        .split("pub async fn agent_session(")
        .nth(1)
        .expect("the workflow is in the source");
    let capped = body
        .find("if reply.stop_reason == STOP_MAX_TOKENS {")
        .expect("the loop ends the session at the cap");
    let runs_tools = body
        .find("if reply.stop_reason != claude::STOP_TOOL_USE")
        .expect("the loop runs tools under one reason");
    assert!(
        capped < runs_tools,
        "the cap must end the session before any tool call is considered"
    );
}

/// A session that says RUNNING in the wrong storage class is not stranded.
///
/// `harvest_executions.state` has TEXT affinity, and a TEXT-affinity column
/// keeps a stored BLOB as a BLOB. A damaged row can therefore hold the right
/// bytes in the wrong class, and `state = 'RUNNING'` is false for one of
/// those.
///
/// Measured before the fix: the row was left out of the running set. The
/// startup seeds the DRIVEN set from that query, so the daemon reported ready
/// and the session was never driven and never refused.
#[test]
fn a_running_session_in_the_wrong_storage_class_is_not_skipped() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("state-class.db");
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    fixture_table(&writer);
    for (exec, state) in [
        ("text-run", "'RUNNING'"),
        ("blob-run", "CAST('RUNNING' AS BLOB)"),
        // Terminal, and damaged in the same way. It strands no work, so it
        // must NOT make the daemon refuse to start.
        ("blob-done", "CAST('COMPLETED' AS BLOB)"),
    ] {
        writer
            .execute(
                &format!(
                    "INSERT INTO harvest_executions \
                     VALUES (?1, ?2, {state}, ?3, NULL, NULL)"
                ),
                rusqlite::params![exec, WORKFLOW_NAME, READABLE_TASK],
            )
            .expect("the row is recorded");
    }

    // The same bytes in both live rows, and only the class differs.
    let class = |exec: &str| -> String {
        writer
            .query_row(
                "SELECT typeof(state) FROM harvest_executions WHERE exec_id = ?1",
                [exec],
                |row| row.get(0),
            )
            .expect("the class answers")
    };
    assert_eq!(class("text-run"), "text", "the readable row is TEXT");
    assert_eq!(class("blob-run"), "blob", "and the damaged one is a BLOB");
    drop(writer);

    let reader = inspect::open(&db).expect("the reader opens");
    let running = inspect::running(&reader, WORKFLOW_NAME).expect("the running rows read");
    let named: Vec<&str> = running.iter().map(|row| row.exec_id.as_str()).collect();
    assert!(
        named.contains(&"blob-run"),
        "a row whose RUNNING is in the wrong class must still be seen: {named:?}"
    );
    assert!(
        !named.contains(&"blob-done"),
        "and a terminal row must not be: {named:?}"
    );

    let damaged = |exec: &str| -> bool {
        running
            .iter()
            .find(|row| row.exec_id == exec)
            .expect("the session is named")
            .state_is_damaged
    };
    assert!(!damaged("text-run"), "the readable row is not damaged");
    assert!(damaged("blob-run"), "and the other one is");
}

/// A session whose WORKFLOW NAME is in the wrong class is not skipped either.
///
/// The same fault as the state, one column over, in the same query. The name
/// was compared against a TEXT parameter. A row holding the right bytes in
/// the wrong class was excluded before the startup check could see it.
///
/// A session of ANOTHER workflow is still excluded, which is what the name is
/// there to decide.
#[test]
fn a_running_session_naming_its_workflow_in_the_wrong_class_is_not_skipped() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("name-class.db");
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    fixture_table(&writer);
    for (exec, name) in [
        ("text-name", format!("'{WORKFLOW_NAME}'")),
        ("blob-name", format!("CAST('{WORKFLOW_NAME}' AS BLOB)")),
        // Another workflow's session, which this daemon must never drive.
        ("other-text", "'other_workflow'".to_string()),
        ("other-blob", "CAST('other_workflow' AS BLOB)".to_string()),
    ] {
        writer
            .execute(
                &format!(
                    "INSERT INTO harvest_executions                      VALUES (?1, {name}, 'RUNNING', ?2, NULL, NULL)"
                ),
                rusqlite::params![exec, READABLE_TASK],
            )
            .expect("the row is recorded");
    }
    drop(writer);

    let reader = inspect::open(&db).expect("the reader opens");
    let running = inspect::running(&reader, WORKFLOW_NAME).expect("the running rows read");
    let named: Vec<&str> = running.iter().map(|row| row.exec_id.as_str()).collect();
    assert!(
        named.contains(&"blob-name"),
        "a row naming this workflow in the wrong class must still be seen: {named:?}"
    );
    assert!(
        !named.contains(&"other-text") && !named.contains(&"other-blob"),
        "and another workflow's sessions must not be: {named:?}"
    );
    let damaged = |exec: &str| -> bool {
        running
            .iter()
            .find(|row| row.exec_id == exec)
            .expect("the session is named")
            .name_is_damaged
    };
    assert!(!damaged("text-name"), "the readable row is not damaged");
    assert!(damaged("blob-name"), "and the other one is");
}

/// An id in the wrong class fails the read, rather than naming another row.
///
/// This completes the sweep of the columns that query touches. The id is read
/// into a `String`, and `rusqlite` refuses a BLOB there, so the whole query
/// fails and the startup refuses. A row that cannot be named is never
/// silently skipped.
#[test]
fn a_running_row_whose_id_is_in_the_wrong_class_fails_the_read() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("id-class.db");
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    fixture_table(&writer);
    writer
        .execute(
            "INSERT INTO harvest_executions              VALUES (CAST('blob-id' AS BLOB), ?1, 'RUNNING', ?2, NULL, NULL)",
            rusqlite::params![WORKFLOW_NAME, READABLE_TASK],
        )
        .expect("the row is recorded");
    drop(writer);

    let reader = inspect::open(&db).expect("the reader opens");
    let Err(refused) = inspect::running(&reader, WORKFLOW_NAME) else {
        panic!("an id this daemon cannot read must fail the read");
    };
    assert!(
        refused.contains("cannot read the running sessions"),
        "the failure must say what it was doing: {refused}"
    );
}

/// The daemon refuses to start over a session it cannot drive.
///
/// The startup seeds the driven set from the running query. A row it cannot
/// read is not a row it can skip. The session would stay RUNNING for the life
/// of the file, and nothing would ever say so.
#[tokio::test]
async fn a_daemon_refuses_to_start_over_a_running_row_it_cannot_read() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");
    let db = dir.path().join("agentd.db");
    let calls = Arc::new(AtomicUsize::new(0));
    // The runtime creates the schema the daemon expects.
    drop(runtime(&db, &workspace, &calls));

    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    writer
        .execute(
            "INSERT INTO harvest_executions              (exec_id, workflow_name, workflow_id, input_json, state)              VALUES (?1, ?2, '', ?3, CAST('RUNNING' AS BLOB))",
            rusqlite::params![
                "ffffffff-1111-2222-3333-444444444444",
                WORKFLOW_NAME,
                task_on(&workspace, claude::OFFLINE_MODEL).to_string()
            ],
        )
        .expect("the damaged row is recorded");
    drop(writer);

    // The refusal is BOUNDED. A daemon that does not refuse starts serving and
    // never returns, so an unbounded await would hang here rather than fail.
    let message = tokio::time::timeout(
        Duration::from_secs(30),
        daemon::serve(daemon::Options {
            db,
            socket: dir.path().join("agentd.sock"),
            workspace,
            model: claude::DEFAULT_MODEL.to_string(),
            max_tokens: claude::DEFAULT_MAX_TOKENS,
            tick: Duration::from_millis(50),
            api_key: None,
        }),
    )
    .await
    .expect("a daemon that serves this row would strand the session")
    .expect_err("the daemon must refuse to start");
    assert!(
        message.contains("storage class this daemon cannot read"),
        "the refusal must say what it found: {message}"
    );
    assert!(
        message.contains("ffffffff-1111-2222-3333-444444444444"),
        "and which row it found it in: {message}"
    );
}

/// A goal is listed only when the WHOLE document reads as a task.
///
/// The listing projects `$.goal` on its own, so a document that answers that
/// projection shows a goal. The single status deserialises the whole
/// document, and refuses it entire. One row then had a task in `list` and
/// `<unreadable task>` in `status`.
///
/// Measured before the fix, for each document below: the listing showed the
/// goal and the status refused the document. The two now answer alike.
///
/// The last two rows are the other end of the rule. An undeclared key is
/// ignored by the reader, which never decodes its value. Neither an extra
/// field nor a broken escape inside one stops the document from reading. A
/// listing that refused those would hide a session the status shows.
#[test]
fn a_listed_goal_is_shown_only_when_the_whole_task_reads() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("tasks.db");
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    fixture_table(&writer);
    let cases: [(&str, &str, bool); 9] = [
        (
            "repeated",
            r#"{"goal":"first","goal":"second","max_turns":4,
                "approval_timeout_secs":300,"workspace":"/tmp/w","model":"offline"}"#,
            false,
        ),
        (
            "missing",
            r#"{"goal":"do it","approval_timeout_secs":300,
                "workspace":"/tmp/w","model":"offline"}"#,
            false,
        ),
        (
            "wrong-type",
            r#"{"goal":"do it","max_turns":"eight","approval_timeout_secs":300,
                "workspace":"/tmp/w","model":"offline"}"#,
            false,
        ),
        (
            "negative",
            r#"{"goal":"do it","max_turns":-1,"approval_timeout_secs":300,
                "workspace":"/tmp/w","model":"offline"}"#,
            false,
        ),
        (
            "over-u32",
            r#"{"goal":"do it","max_turns":4294967296,"approval_timeout_secs":300,
                "workspace":"/tmp/w","model":"offline"}"#,
            false,
        ),
        (
            "no-character",
            r#"{"goal":"do it","max_turns":4,"approval_timeout_secs":300,
                "workspace":"\ud800","model":"offline"}"#,
            false,
        ),
        (
            "extra-key",
            r#"{"goal":"do it","note":"fine","max_turns":4,
                "approval_timeout_secs":300,"workspace":"/tmp/w","model":"offline"}"#,
            true,
        ),
        (
            "extra-key-broken",
            r#"{"goal":"do it","note":"\ud800","max_turns":4,
                "approval_timeout_secs":300,"workspace":"/tmp/w","model":"offline"}"#,
            true,
        ),
        ("whole", READABLE_TASK, true),
    ];
    for (exec, document, _) in cases {
        record_task(&writer, exec, document);
    }
    drop(writer);

    let reader = inspect::open(&db).expect("the reader opens");
    let listed = inspect::executions(&reader, WORKFLOW_NAME, None).expect("the listing answers");
    let (views, _, _) = daemon::sessions(&reader, &daemon::Parked::new(), false, None)
        .expect("the listing renders");

    for (exec, document, reads) in cases {
        let row = listed_row(&listed, exec);
        let shown = views
            .iter()
            .find(|view| view.execution_id == exec)
            .expect("the session is listed")
            .goal
            .clone();
        // The single status reads the whole document. This is the reader it
        // uses, so the two cannot drift apart in the test either.
        let status = daemon::task_goal(document);

        assert_eq!(
            status.is_some(),
            reads,
            "[{exec}] the fixture must be the document this case means"
        );
        assert_eq!(
            row.task_is_damaged, !reads,
            "[{exec}] the listing must reach the same verdict as the status"
        );
        if reads {
            assert_eq!(
                Some(shown.clone()),
                status,
                "[{exec}] a whole task shows the same goal in both"
            );
        } else {
            // The projection still answers. The refusal is the whole-document
            // test, and not a goal the query failed to read.
            assert!(
                row.goal.is_some(),
                "[{exec}] the goal still projects on its own: {row:?}"
            );
            assert_eq!(
                shown, "<unreadable task>",
                "[{exec}] and the listing refuses it as the status does"
            );
        }
    }
}
