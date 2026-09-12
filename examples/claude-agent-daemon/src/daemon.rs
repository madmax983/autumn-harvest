//! The daemon: one process, one database file, one writer.
//!
//! The process owns three things:
//!
//! 1. The [`SqliteRuntime`] — the only write handle to the database.
//! 2. A Unix socket — the control surface the CLI talks to.
//! 3. A drive tick — the poll the backend needs in place of `LISTEN`/`NOTIFY`.
//!
//! The main loop selects between a control command and the tick. Both take the
//! runtime by mutable reference, so exactly one of them runs at a time. That is
//! the single-writer contract made visible: while a model call is in flight,
//! the next command waits. A fleet that needs concurrent writers wants the
//! Postgres core instead.

use std::collections::HashMap;
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest_sqlite::{ExecutionId, RunState, SqliteRuntime};
use rusqlite::Connection;
use std::os::unix::fs::MetadataExt;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Semaphore, mpsc, oneshot};

use crate::claude::{self, ModelConfig};
use crate::guard;
use crate::inspect::{self, ExecutionRow};
use crate::protocol::{PendingCall, Request, Response, SessionView};
use crate::session::{self, ApprovalDecision, SessionReport, SessionTask, WORKFLOW_NAME};
use crate::shutdown;
use crate::tools;

/// How many control commands may queue while the runtime is busy.
const COMMAND_BACKLOG: usize = 32;

/// How many control connections the daemon holds at once.
///
/// A command waits while the runtime is busy, and a model call holds it for as
/// long as the API takes. Without this bound, every connection accepted during
/// that time is a task and a descriptor parked in the channel send. A polling
/// script would spend the daemon's descriptors, and the answer to every
/// command would arrive no sooner.
///
/// The permit is taken BEFORE the accept. A connection that has nowhere to go
/// is not accepted at all, so the kernel queues it on the listening socket
/// instead. The operator sees one command wait, rather than a daemon that ran
/// out of descriptors.
const MAX_CONNECTIONS: usize = COMMAND_BACKLOG;

/// The longest control request the daemon reads.
///
/// A request is one line of JSON. A goal can be long, and a megabyte is far
/// past anything an operator types. The cap stops one caller from growing the
/// daemon's memory without end.
///
/// A request that passes this cap is REFUSED, not truncated. The daemon reads
/// one byte more than the cap. A request that ended inside the cap is then
/// distinguishable from one that continued past it. See
/// [`REQUEST_READ_BYTES`].
pub const MAX_REQUEST_BYTES: usize = 1024 * 1024;

/// How many bytes the daemon reads before it stops.
///
/// One past [`MAX_REQUEST_BYTES`], so reaching the cap is not the same as
/// passing it. A reader stopped AT the cap reports the same end of input as a
/// caller that closed the connection. The daemon cannot then tell a complete
/// request from the start of a longer one.
///
/// A prefix that parses is the hazard. Valid JSON followed by whitespace
/// parses on its own. A truncated request can then start a session, or
/// release an approval nobody sent in full.
const REQUEST_READ_BYTES: u64 = MAX_REQUEST_BYTES as u64 + 1;

/// How long one caller may take to send its request.
///
/// The deadline covers the REQUEST only. The answer can take as long as the
/// runtime needs, because a command waits for the drive loop.
///
/// A real client writes its line as soon as it connects, so this is generous
/// by a wide margin. It is deliberately SHORT, because a connection that
/// sends nothing still holds one of the daemon's connections until it expires.
const REQUEST_DEADLINE: Duration = Duration::from_secs(5);

/// How long one caller may take to read its answer.
///
/// A client that asks for an answer and then stops reading would otherwise
/// hold a connection until it disconnected. The socket is local, so it moves
/// at memory speed: a client that has not taken its answer in this long is not
/// reading it.
const RESPONSE_DEADLINE: Duration = Duration::from_secs(5);

/// How long to wait after `accept` fails.
///
/// A descriptor limit makes `accept` fail at once, every time. Without a pause
/// the loop spins on it and fills the log.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

/// Everything the daemon needs to start.
pub struct Options {
    pub db: PathBuf,
    pub socket: PathBuf,
    pub workspace: PathBuf,
    pub model: String,
    pub max_tokens: u32,
    pub tick: Duration,
    /// `None` runs the offline stub model.
    pub api_key: Option<String>,
}

/// One control command plus the channel its answer goes back on.
type Job = (Request, oneshot::Sender<Response>);

/// The largest tool input the status prints. An operator decides from it, so
/// it is generous; the marker says when there is more.
const MAX_PENDING_INPUT_CHARS: usize = 2000;

/// How many model replies the pending-call search reads.
///
/// One. A parked session waits on a call of its newest reply, so one row
/// answers and the database stops reading there. The `stop_reason` test is
/// not indexed. A larger read would therefore go backward past older
/// replies until it had that many, and decode every tool event on the way.
///
/// Reading a whole history is unbounded twice over: the event count grows
/// with every turn, and each model activity carries the whole transcript. The
/// runtime is serialised, so one such read blocks every session drive.
///
/// One row is also every row the search MAY use. A tool-use id is unique
/// inside one reply. Nothing makes an id unique across a run, so an older
/// reply can hold the same id for a different tool. A read further back can
/// therefore only offer the wrong call. See [`pending_call`].
const REPLY_PAGE: u32 = 1;

/// Why one session is parked, and what would release it.
#[derive(Clone)]
pub struct ParkedState {
    /// The operator-facing reason.
    pub reason: String,
    /// The signal name a decision must carry, when the session waits for one.
    /// It names the exact tool call, so an approval cannot release another.
    pub signal: Option<String>,
}

/// The parked sessions this daemon knows about.
pub type Parked = HashMap<ExecutionId, ParkedState>;

/// The sessions the drive tick advances, in the order they arrived.
///
/// The daemon is the only writer of this database, so it knows every live
/// session. It starts them, and it sees each one reach a terminal state. The
/// set is seeded once at startup and maintained in memory after that.
///
/// The alternative is a query on every tick. That query cannot use an index.
/// `harvest_executions` is indexed on `(workflow_name, workflow_id)`, so a
/// filter on `state` visits every session that ever ran under this workflow
/// name. The cost of an idle daemon would grow with its whole history, several
/// times a second. The engine owns that schema, and an example does not add an
/// index to it.
///
/// A `Vec` rather than a set, for two reasons. The order stays the submit
/// order. The length is the number of LIVE sessions, and not of the recorded
/// history.
type Live = Vec<ExecutionId>;

/// Run the daemon until `Ctrl-C`.
///
/// # Errors
///
/// Returns an error if the database, the workspace, or the socket cannot be
/// opened, or if another daemon already holds the socket.
pub async fn serve(options: Options) -> Result<(), String> {
    let workspace = prepare_workspace(&options.workspace, &options.db)?;

    // Take the per-database lock FIRST. The open below reclaims every task left
    // `RUNNING` by a dead process. A second daemon opening the same file would
    // reclaim a LIVE task, and its activity would run twice. The lock lives as
    // long as this call, and the kernel releases it if the process dies.
    let lock = guard::acquire(&options.db)?;

    // And the per-SOCKET lock, which the database lock cannot stand in for.
    // Two daemons on different databases contend for neither the file nor the
    // reclaim. Both could therefore find one stale socket, and the second
    // would unlink the first's live one. See [`guard::acquire_socket`].
    //
    // It is taken here, before the runtime opens, so a daemon that has lost
    // the socket exits without reclaiming anything.
    let _socket_lock = guard::acquire_socket(&options.socket)?;

    // One task waits for `Ctrl-C` and raises the flag. The drive loop cannot
    // wait for the signal itself. A model call blocks its thread, so nothing
    // else on that task is polled until the call returns. See `shutdown`.
    let (trigger, signal) = shutdown::channel();
    tokio::spawn(async move {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::error!(error = %crate::one_line(&e.to_string()), "cannot listen for Ctrl-C");
        }
        trigger.send_replace(true);
    });

    let model = ModelConfig::new(
        options.api_key,
        &options.model,
        options.max_tokens,
        signal.clone(),
    )?;
    let live = model.is_live();
    // Every session records this, and a turn is refused by a daemon serving a
    // different model. See `ModelConfig::identity`.
    let identity = model.identity();

    // Opening the file applies the schema and reclaims any task a previous
    // process left RUNNING. In-flight sessions resume by replay from here.
    // The `-wal` and `-shm` sidecars are created by `SQLite`, and they carry
    // the same data as the database. The mask makes them private too.
    // The identity is proved on BOTH sides of the open, because the open is
    // not inert. It flips every RUNNING task of whatever database it opens
    // back to PENDING. Opening a file swapped in here would re-queue the live
    // work of the daemon that owns it. That daemon would then run those tasks
    // a second time, and each one can spend money.
    //
    // A check that ran only afterwards would refuse this start AFTER that
    // write had landed on another file. See [`guard::DaemonLock::still_names`].
    lock.still_names(&options.db)?;
    let mut runtime = guard::with_private_umask(|| SqliteRuntime::open(&options.db))
        .map_err(|e| format!("cannot open {}: {e}", options.db.display()))?;
    // Again, because the check above cannot cover the open itself. This one
    // catches a swap that landed inside it. The one above keeps the reclaim
    // off another daemon's file in every slower case.
    lock.still_names(&options.db)?;
    runtime.register_workflow(&session::agent_session_info());
    runtime.register_activity(&session::claude_turn_info(), claude::activity_body(model));
    runtime.register_activity(
        &session::run_tool_info(),
        tools::activity_body(options.workspace.clone()),
    );

    let reader = inspect::open(&options.db)?;
    // Check the sessions already in the file BEFORE anything drives them. A
    // mismatched tool call fails non-retryably, and a FAILED run is terminal.
    // Only `RUNNING` rows are ever driven again, so restarting with the right
    // flags could not bring it back. An operator who mistypes `--workspace`
    // gets an error here, and every session stays resumable.
    let resumed = check_resumable(&reader, &workspace, &identity)?;
    if !resumed.is_empty() {
        tracing::info!(count = resumed.len(), "resuming the sessions left running");
    }
    // The parked state is rebuilt BEFORE the socket accepts anything. It was
    // otherwise empty until the first drive, and a decision that arrived in
    // that window was refused as a session that is not waiting. The restart
    // recipe describes exactly that sequence.
    let mut blocked: Parked = Parked::new();
    // One clock reading for the whole rebuild, so two sessions cannot be
    // judged against two different instants.
    let now_ms = epoch_millis()?;
    for exec in &resumed {
        match inspect::outstanding_signal(&reader, &exec.to_string(), now_ms) {
            Ok(Some(name)) => note(&mut blocked, *exec, waiting_reason(&name), Some(name)),
            Ok(None) => {}
            Err(message) => return Err(message),
        }
    }
    let listener = bind(&options.socket).await?;
    let (tx, mut rx) = mpsc::channel::<Job>(COMMAND_BACKLOG);
    // The socket's mode is not a control surface on its own. See
    // [`peer_is_owner`].
    let owner = rustix::process::geteuid().as_raw();
    tokio::spawn(accept_loop(listener, tx, owner));

    // The socket path is logged as itself: `printable` refuses one that holds
    // anything a terminal acts on before any command runs. The database and
    // the workspace carry no such rule, because neither appears in a printed
    // command. The log sink keeps them from reaching a terminal.
    tracing::info!(
        db = %crate::one_line(&options.db.display().to_string()),
        socket = %options.socket.display(),
        workspace = %crate::one_line(&options.workspace.display().to_string()),
        model = if live { "claude api" } else { "offline stub" },
        "agentd is ready",
    );
    if !live {
        tracing::warn!(
            "ANTHROPIC_API_KEY is not set, so the offline stub model is in use. \
             Set the key and restart to run against Claude."
        );
    }

    // The startup check already read the RUNNING rows, so the live set is what
    // it validated. One query, not two.
    let mut live: Live = resumed;
    let mut ticker = tokio::time::interval(options.tick);
    // A model call can hold a drive for the whole HTTP timeout, which is
    // thousands of tick periods. The default behaviour fires every missed tick
    // at once when the drive returns, and each one drives every live session.
    // The poll wants the NEXT tick, not the ones it slept through.
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Every branch below waits on the same flag. It stays raised once it is
    // raised, so a fresh wait returns at once rather than missing the signal.
    let mut stop = signal.clone();
    let mut stop_during_drive = signal;
    'serve: loop {
        tokio::select! {
            job = rx.recv() => {
                let Some((request, answer)) = job else { break };
                let response = handle(
                    &mut runtime,
                    &reader,
                    &mut blocked,
                    &mut live,
                    &workspace,
                    &identity,
                    request,
                );
                // A closed receiver means the client hung up. Nothing to do.
                drop(answer.send(response));
            }
            _ = ticker.tick() => {
                // A copy, because each drive can remove its own session from
                // the set. The set holds the live sessions only, so this is
                // short whatever the recorded history holds.
                let ready = live.clone();
                for exec in ready {
                    tokio::select! {
                        () = drive_one(&mut runtime, exec, &mut blocked, &mut live) => {}
                        () = stop_during_drive.raised() => {
                            // The drive is dropped where it stands. Its task
                            // stays RUNNING in the file, and the next start
                            // reclaims it and replays the recorded history.
                            // That is the same path a crash takes.
                            break 'serve;
                        }
                    }
                }
            }
            () = stop.raised() => break,
        }
    }

    // The socket path is deliberately NOT removed here.
    //
    // Whatever occupies it at this moment may not be this daemon's socket. The
    // path can be replaced while the daemon runs. An inode number is also
    // reused as soon as it is freed. Neither the type nor the identity can
    // therefore prove ownership of a public pathname. Deleting another
    // daemon's socket is worse than leaving a stale one, and a stale one costs
    // nothing. `bind` reclaims it at the next start, once it has proved that
    // it is a socket and that nobody answers on it.
    tracing::info!(
        socket = %options.socket.display(),
        "agentd is stopping; in-flight sessions resume on the next start",
    );
    Ok(())
}

/// The largest signal payload the backend accepts.
///
/// A decision is delivered as one signal, so this bounds the note an operator
/// can attach. It is a QUARTER of the control request cap, so a note can pass
/// [`MAX_REQUEST_BYTES`] and still be undeliverable.
pub const SIGNAL_CAP_BYTES: u64 = autumn_harvest::builder::DEFAULT_MAX_SIGNAL_PAYLOAD_BYTES;

/// The encoded size of this decision, when the backend would refuse it.
///
/// The note is the only field an operator can grow. A note between the two
/// caps was accepted at the socket and then refused on delivery. The
/// advertised approve command could not deliver it, and the call stayed
/// parked.
fn oversized_decision(payload: &serde_json::Value) -> Option<u64> {
    let bytes = serde_json::to_vec(payload)
        .map(|json| json.len() as u64)
        .ok()?;
    (bytes > SIGNAL_CAP_BYTES).then_some(bytes)
}

/// The refusal for a recorded task this daemon cannot read.
fn unreadable(exec_id: &str) -> String {
    format!(
        "session {exec_id} carries an input this daemon cannot read. The session \
         stays RUNNING and no daemon of this version can resume it. A newer \
         daemon wrote it, or the row is damaged."
    )
}

/// The refusal for a session recorded against another model identity.
///
/// The advice has to be a command that WORKS. An operator copies it.
///
/// The recorded identity is NOT the `--model` flag. A daemon holding a key
/// records the model it was given. A daemon without one records the stub. So
/// `--model` alone cannot cross that line, in either direction.
///
/// Both errors were reachable. `--model=offline-stub` is refused outright
/// while a key is set. `--model=<a real model>` builds without a key, and
/// then records the stub, so the same refusal arrives again.
fn model_mismatch(exec_id: &str, recorded: &str, served: &str) -> String {
    if recorded == claude::OFFLINE_MODEL {
        return format!(
            "session {exec_id} ran on this daemon's own stub, and this daemon \
             serves the model `{served}`. Unset `ANTHROPIC_API_KEY` so the \
             session can resume. `--model` cannot do it: a daemon holding a key \
             records every session against a real model."
        );
    }
    if served == claude::OFFLINE_MODEL {
        return format!(
            "session {exec_id} runs on the model `{recorded}`, and this daemon \
             serves its own stub because no key is set. Set `ANTHROPIC_API_KEY` \
             and start it with `--model={}` so the session can resume.",
            crate::protocol::quoted(recorded)
        );
    }
    format!(
        "session {exec_id} runs on the model `{recorded}`, and this daemon serves \
         `{served}`. Start it with `--model={}` so the session can resume.",
        crate::protocol::quoted(recorded)
    )
}

/// Refuse to start when a session in this file belongs to another daemon.
///
/// The activity-level checks stay as a backstop, but they can only fail a run.
/// This is the check that protects the work.
fn check_resumable(
    reader: &Connection,
    workspace: &str,
    model: &str,
) -> Result<Vec<ExecutionId>, String> {
    let mut live = Vec::new();
    for row in inspect::running(reader, WORKFLOW_NAME)? {
        // A row this daemon cannot read is not a row it can skip. The
        // returned ids are the only set the tick drives. An omitted session
        // therefore stays RUNNING for as long as the file lasts, and nothing
        // ever says so. The daemon refuses to start instead, under the same
        // policy as the two checks below.
        // The read above is the runtime's own read of the whole task, so a
        // row that answers here deserialises on the first drive. One that did
        // not would be sealed FAILED by the runtime the moment it ran, and no
        // later daemon could resume it.
        // The RANGE comes with the read. The task's fields are unsigned, so
        // a recorded `-1` answers with nothing here. So does a value wider
        // than the field. A zero turn bound reads perfectly well. It is
        // refused for the reason `submit` refuses one: the loop would run no
        // turn and report the session COMPLETE.
        let Some(task) = row.task else {
            return Err(unreadable(&row.exec_id));
        };
        // The deadline is bounded where `submit` bounds it. A recorded one
        // past `i64::MAX` seconds cannot be armed as a timer, so a session
        // carrying one would be driven and could never wait.
        let armable = i64::try_from(task.approval_timeout_secs).is_ok();
        if !task.has_goal || task.max_turns == 0 || !armable {
            return Err(unreadable(&row.exec_id));
        }
        // A row that says RUNNING in a class this daemon cannot read is a row
        // it cannot drive. Leaving it out of the driven set stranded the
        // session in silence. See [`inspect::RunningSession::state_is_damaged`].
        if row.state_is_damaged {
            return Err(format!(
                "session {} says it is running, in a storage class this daemon cannot \
                 read. The session stays RUNNING and nothing resumes it. Repair the \
                 row, or remove it.",
                row.exec_id
            ));
        }
        let (recorded_workspace, recorded_model) = (&task.workspace, &task.model);
        // Both restart hints are made to be COPIED, so each value is one
        // shell word and is attached to its flag. A workspace holding a space
        // would otherwise split into two arguments. One holding `;` would run
        // the rest of the line as a command. A value that begins with a dash
        // reads as more options as a separate word. This is the argument
        // [`crate::protocol::socket_flag`] carries, applied to the two flags
        // that name a recorded value.
        if recorded_workspace != workspace {
            return Err(format!(
                "session {} belongs to the workspace `{recorded_workspace}`, and \
                 this daemon serves `{workspace}`. Start it with \
                 `--workspace={}` so the session can resume.",
                row.exec_id,
                crate::protocol::quoted(recorded_workspace)
            ));
        }
        if recorded_model != model {
            return Err(model_mismatch(&row.exec_id, recorded_model, model));
        }
        // An id that does not parse is the same failure one step on. The
        // session would pass every check above and then never be driven.
        let exec = row.exec_id.parse::<ExecutionId>().map_err(|_| {
            format!(
                "session {} has an id this daemon cannot parse. The session                  stays RUNNING and nothing resumes it.",
                row.exec_id
            )
        })?;
        live.push(exec);
    }
    Ok(live)
}

/// Take the control socket, refusing to displace a live daemon.
///
/// Anything already at the path is removed ONLY when it is a socket. A typo in
/// `--socket` must not delete a file, so any other kind of entry is an error.
///
/// The socket is created owner-only. Whoever can connect to it can spend money
/// and approve writes with this daemon's privileges, so a permissive umask must
/// not decide that. The mask is narrowed across the bind, which makes the
/// socket private AT CREATION: there is no window in which another local user
/// can connect.
pub async fn bind(socket: &Path) -> Result<UnixListener, String> {
    if let Ok(existing) = std::fs::symlink_metadata(socket) {
        if !existing.file_type().is_socket() {
            return Err(format!(
                "{} exists and is not a socket. Refusing to remove it.",
                socket.display()
            ));
        }
        // A socket another user owns is never this daemon's to replace. Whoever
        // owns it owns the daemon behind it, and removing the name would leave
        // that daemon running and unreachable.
        let owner = rustix::process::geteuid().as_raw();
        if existing.uid() != owner {
            return Err(format!(
                "{} belongs to uid {} and this daemon runs as uid {owner}. \
                 Refusing to replace another user's socket.",
                socket.display(),
                existing.uid()
            ));
        }
        match UnixStream::connect(socket).await {
            Ok(_) => return Err(format!("a daemon already listens on {}", socket.display())),
            // Only these two answers prove that nothing listens. Every other
            // failure means the question was not answered, and the name is
            // NOT known to be free. See [`proves_nothing_listens`].
            Err(e) if !proves_nothing_listens(&e) => {
                return Err(format!(
                    "cannot tell whether a daemon listens on {}: {e}. Refusing to \
                     remove a socket that may be live.",
                    socket.display()
                ));
            }
            Err(_) => {}
        }
        // The socket outlived its process, so it is safe to replace.
        //
        // A name already gone is the outcome this wanted, not a failure. The
        // entry can vanish between the two calls above. A missing entry is
        // one of the two answers that prove nothing listens, so this path is
        // reached with the name already free. The bind below still refuses if
        // something has taken the name again.
        match std::fs::remove_file(socket) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(format!(
                    "cannot remove the stale socket {}: {e}",
                    socket.display()
                ));
            }
        }
    }

    guard::with_private_umask(|| UnixListener::bind(socket))
        .map_err(|e| format!("cannot listen on {}: {e}", socket.display()))
}

/// Does this connection failure prove that nothing listens?
///
/// Only two answers do. A refusal is the kernel saying the name has no
/// listener, and a missing entry is the name being gone between the two
/// calls. Every other failure leaves the question open.
///
/// The difference decides whether a socket is REMOVED. A permission error is
/// the reported case: another user's live socket in a shared directory this
/// process can write. Exhausted file descriptors are the same shape, on a
/// socket this daemon owns. Treating either as proof would unlink the name a
/// live daemon is listening on. That daemon would keep running, with nothing
/// able to reach it.
pub fn proves_nothing_listens(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
    )
}

/// Accept connections and forward each request to the main loop.
async fn accept_loop(listener: UnixListener, tx: mpsc::Sender<Job>, owner: u32) {
    let permits = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    loop {
        // Take the permit first. See [`MAX_CONNECTIONS`].
        let Ok(permit) = Arc::clone(&permits).acquire_owned().await else {
            return;
        };
        match listener.accept().await {
            Ok((stream, _)) => {
                // Every caller is identified before it is served.
                if !peer_is_owner(&stream, owner) {
                    drop(permit);
                    continue;
                }
                let tx = tx.clone();
                tokio::spawn(async move {
                    // The permit is released when this connection is done.
                    let _permit = permit;
                    serve_connection(stream, tx).await;
                });
            }
            Err(e) => {
                drop(permit);
                tracing::warn!(error = %crate::one_line(&e.to_string()), "cannot accept a control connection");
                tokio::time::sleep(ACCEPT_BACKOFF).await;
            }
        }
    }
}

/// Is the caller the user this daemon runs as?
///
/// The socket is created `0600`, and that is not enough on its own. Linux
/// enforces a socket's mode on `connect`. macOS does not, and this example
/// supports both. A socket in a directory that other users can search would
/// therefore accept them there.
///
/// The kernel reports the peer's credentials, and no directory mode can forge
/// them. The check fails CLOSED: a peer that cannot be identified is refused,
/// because this connection spends money and approves writes.
pub fn peer_is_owner(stream: &UnixStream, owner: u32) -> bool {
    match stream.peer_cred() {
        Ok(peer) if peer.uid() == owner => true,
        Ok(peer) => {
            tracing::warn!(
                uid = peer.uid(),
                "refused a control connection from another user"
            );
            false
        }
        Err(e) => {
            tracing::warn!(error = %crate::one_line(&e.to_string()), "refused a control connection of unknown origin");
            false
        }
    }
}

/// Read one request, wait for the answer, write it back.
async fn serve_connection(stream: UnixStream, tx: mpsc::Sender<Job>) {
    let (read_half, mut write_half) = stream.into_split();
    let mut line = String::new();
    // The read is bounded in BOTH size and time. A caller that sends no
    // newline would otherwise grow this buffer without end, and hold one of
    // the daemon's connections while it did. Enough such callers would leave
    // no connection for the operator.
    let read = tokio::time::timeout(
        REQUEST_DEADLINE,
        BufReader::new(read_half.take(REQUEST_READ_BYTES)).read_line(&mut line),
    )
    .await;
    let request = match read {
        // A request longer than the cap did not end inside it. The extra byte
        // is the proof, so the prefix is refused instead of parsed. Trailing
        // whitespace makes a truncated line parse. See [`REQUEST_READ_BYTES`].
        Ok(Ok(_)) if line.len() > MAX_REQUEST_BYTES => {
            answer(
                &mut write_half,
                &Response::Error {
                    message: format!(
                        "the request is longer than the {MAX_REQUEST_BYTES} bytes this \
                         daemon reads. Shorten the goal or the note."
                    ),
                },
            )
            .await;
            return;
        }
        Ok(Ok(_)) => line,
        // A caller that stopped mid-request gets a reason, not a closed
        // connection. A line that is not JSON is reported as malformed below.
        // A line that passed the cap is refused above.
        Ok(Err(_)) => return,
        Err(_) => {
            answer(
                &mut write_half,
                &Response::Error {
                    message: format!(
                        "the request did not arrive within {} seconds",
                        REQUEST_DEADLINE.as_secs()
                    ),
                },
            )
            .await;
            return;
        }
    };

    let response = match serde_json::from_str::<Request>(request.trim()) {
        Ok(request) => {
            let (answer_tx, answer_rx) = oneshot::channel();
            if tx.send((request, answer_tx)).await.is_err() {
                Response::Error {
                    message: "the daemon is shutting down".to_string(),
                }
            } else {
                answer_rx.await.unwrap_or_else(|_| Response::Error {
                    message: "the daemon dropped the request".to_string(),
                })
            }
        }
        Err(e) => Response::Error {
            message: format!("malformed request: {e}"),
        },
    };

    answer(&mut write_half, &response).await;
}

/// Write one answer back, and end the line.
///
/// An answer that cannot be encoded still gets a line, so the client reads a
/// reason instead of a closed connection.
async fn answer(write_half: &mut tokio::net::unix::OwnedWriteHalf, response: &Response) {
    let mut encoded = serde_json::to_string(response).unwrap_or_else(|e| {
        tracing::error!(error = %crate::one_line(&e.to_string()), "cannot encode an answer");
        r#"{"status":"error","message":"the daemon cannot encode its answer"}"#.to_string()
    });
    encoded.push('\n');
    // Bounded, like the read. A client that stops reading cannot hold this
    // connection for longer than the deadline. See [`RESPONSE_DEADLINE`].
    let written = tokio::time::timeout(RESPONSE_DEADLINE, async {
        write_half.write_all(encoded.as_bytes()).await?;
        write_half.flush().await
    })
    .await;
    match written {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            tracing::debug!(error = %crate::one_line(&e.to_string()), "a caller did not take its answer");
        }
        Err(_) => tracing::warn!(
            seconds = RESPONSE_DEADLINE.as_secs(),
            "a caller did not read its answer in time"
        ),
    }
}

/// Apply one control command.
fn handle(
    runtime: &mut SqliteRuntime,
    reader: &Connection,
    blocked: &mut Parked,
    live: &mut Live,
    workspace: &str,
    model: &str,
    request: Request,
) -> Response {
    match request {
        Request::Submit {
            goal,
            max_turns,
            approval_timeout_secs,
        } => submit(
            runtime,
            live,
            workspace,
            model,
            &goal,
            max_turns,
            approval_timeout_secs,
        ),
        // One session is named, so one row is read. The listing would select
        // the input and the output of every session that ever ran.
        Request::Status { execution_id, full } => {
            match inspect::execution(reader, WORKFLOW_NAME, &execution_id) {
                Ok(Some(row)) => Response::Session {
                    session: Box::new(view(reader, &row, blocked, full)),
                },
                Ok(None) => Response::Error {
                    message: format!("no session {execution_id}"),
                },
                Err(message) => Response::Error { message },
            }
        }
        Request::List { before } => match sessions(reader, blocked, false, before) {
            Ok((sessions, more, older)) => Response::Sessions {
                sessions,
                more,
                older,
            },
            Err(message) => Response::Error { message },
        },
        Request::History {
            execution_id,
            before,
        } => history(reader, &execution_id, before),
        Request::Approve {
            execution_id,
            token,
            approved,
            note,
        } => approve(
            runtime,
            reader,
            blocked,
            &execution_id,
            &token,
            approved,
            note,
        ),
    }
}

/// Start one session.
fn submit(
    runtime: &mut SqliteRuntime,
    live: &mut Live,
    workspace: &str,
    model: &str,
    goal: &str,
    max_turns: u32,
    approval_timeout_secs: u64,
) -> Response {
    // A session needs something to do. An empty goal is sent as an empty text
    // block, which the API refuses, and the refusal of an accepted request is
    // terminal here. The daemon would acknowledge a session that could never
    // make its first model call. The check is HERE and not only in the CLI,
    // because this socket is the boundary every client crosses.
    let goal = goal.trim().to_string();
    if goal.is_empty() {
        return Response::Error {
            message: "the goal is empty. Say what the session is to do.".to_string(),
        };
    }
    // The same argument as the goal above, for the field beside it. A bound of
    // zero runs `1..=0`, which is no iteration at all. The session would be
    // recorded COMPLETED with a blank answer, and would never call the model.
    // The CLI refuses it too, and this is where every client crosses.
    if max_turns == 0 {
        return Response::Error {
            message: "the turn bound is zero. A session needs at least one turn.".to_string(),
        };
    }
    // The deadline becomes a timer, and the engine records a fire time as a
    // SIGNED 64-bit epoch millisecond. A deadline past `i64::MAX` seconds
    // cannot be armed as one. It is refused here so a session is never
    // recorded with a deadline it can never wait on.
    if i64::try_from(approval_timeout_secs).is_err() {
        return Response::Error {
            message: format!(
                "the approval deadline of {approval_timeout_secs} seconds is past                  {} and cannot be recorded. Choose a smaller one.",
                i64::MAX
            ),
        };
    }
    let task = SessionTask {
        goal,
        max_turns,
        approval_timeout_secs,
        workspace: workspace.to_string(),
        model: model.to_string(),
    };
    let input = match serde_json::to_value(task) {
        Ok(value) => value,
        Err(e) => {
            return Response::Error {
                message: format!("cannot encode the task: {e}"),
            };
        }
    };
    // The call records the start and returns at once. The drive tick runs the
    // first turn, so a slow model call never holds up the answer here.
    match runtime.start_workflow(WORKFLOW_NAME, input) {
        Ok(exec) => {
            // The tick drives it from here. A session the set does not hold
            // would sit at its first turn forever.
            enlist(live, exec);
            Response::Submitted {
                execution_id: exec.to_string(),
            }
        }
        Err(e) => Response::Error {
            message: format!("cannot start the session: {e}"),
        },
    }
}

/// Deliver one approval decision.
///
/// The decision is addressed to the signal the session is waiting on, and that
/// name carries the tool-use id. A decision can therefore only release the call
/// the operator was shown. An early, repeated, or stale `approve` has no live
/// wait to land in, so it is refused here rather than staged for a later call.
pub fn approve(
    runtime: &mut SqliteRuntime,
    reader: &Connection,
    blocked: &mut Parked,
    execution_id: &str,
    token: &str,
    approved: bool,
    note: Option<String>,
) -> Response {
    let exec = match execution_id.parse::<ExecutionId>() {
        Ok(exec) => exec,
        Err(e) => {
            return Response::Error {
                message: format!("`{execution_id}` is not an execution id: {e}"),
            };
        }
    };
    let Some(signal) = blocked.get(&exec).and_then(|state| state.signal.clone()) else {
        return Response::Error {
            message: format!("session {execution_id} is not waiting for a decision"),
        };
    };
    // The wait can move on between the status and the decision: a deadline can
    // expire, and the session then parks on the NEXT call. The token names one
    // wait of one run, and it is compared EXACTLY. A tool-use id would not be
    // enough. The model can reuse one across turns, so a decision read from an
    // older status would then release a call nobody reviewed.
    if signal != token {
        // The names travel as data. The client renders the command that reads
        // the status again, because only it knows which socket it asked.
        return Response::Stale {
            execution_id: execution_id.to_string(),
            waiting_on: signal,
            sent: token.to_string(),
        };
    }
    // A decision that arrives after the deadline cannot win. The backend fires
    // the expired race timer BEFORE a late signal, on purpose, so the session
    // reports the call as denied however this answer reads. Saying "approved"
    // here would be a lie the operator only discovers in the history.
    match expired(reader, execution_id, &signal) {
        Ok(Some(passed)) => {
            return Response::Error {
                message: format!(
                    "the deadline for this call passed {passed} seconds ago, so the \
                     session denies it on its next drive. Nothing was delivered."
                ),
            };
        }
        Ok(None) => {}
        Err(message) => return Response::Error { message },
    }

    let decision = ApprovalDecision { approved, note };
    let payload = match serde_json::to_value(decision) {
        Ok(value) => value,
        Err(e) => {
            return Response::Error {
                message: format!("cannot encode the decision: {e}"),
            };
        }
    };
    // The backend refuses a signal payload past its own cap, and that refusal
    // arrives after the operator typed the note. The cap is checked HERE, so
    // the answer names the note rather than a delivery failure.
    if let Some(bytes) = oversized_decision(&payload) {
        return Response::Error {
            message: format!(
                "the decision is {bytes} bytes once encoded, over the backend's \
                 {SIGNAL_CAP_BYTES}-byte signal cap. Shorten the note."
            ),
        };
    }
    match runtime.send_signal(exec, &signal, payload) {
        Ok(()) => {
            // The deadline can pass between the check above and the moment the
            // backend stamps this signal. It records its own arrival time, so
            // the daemon cannot stage a decision AT a chosen instant. When the
            // deadline has gone by now, the answer says what is true: the
            // decision is delivered, and the session may still deny the call.
            let crossed = matches!(expired(reader, execution_id, &signal), Ok(Some(_)));
            // The wait is spent the moment a decision is staged. Without this,
            // a second `approve` before the next drive tick would stage a
            // SECOND signal. The first releases this call and the other stays
            // queued, where a later call reusing the id could consume it and
            // run without being shown. The next tick re-reads the run's real
            // state, so clearing it here loses nothing.
            if let Some(state) = blocked.get_mut(&exec) {
                state.signal = None;
                state.reason = "a decision is delivered; awaiting the next drive".to_string();
            }
            let decision = if approved { "approved" } else { "denied" };
            // The session id travels as DATA when the answer points at the
            // history. The client renders that command, because only it
            // knows which socket it asked.
            Response::Ack {
                detail: if crossed {
                    format!(
                        "{decision}, and the deadline passed while it was delivered. \
                         The session may report this call as denied."
                    )
                } else {
                    decision.to_string()
                },
                history_of: crossed.then(|| execution_id.to_string()),
            }
        }
        Err(e) => Response::Error {
            message: format!("cannot deliver the decision: {e}"),
        },
    }
}

/// Now, as the absolute epoch-millisecond the timer table stores.
///
/// # Errors
///
/// Returns an error if the clock is before the epoch or out of range.
fn epoch_millis() -> Result<i64, String> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| format!("cannot read the clock: {e}"))?;
    i64::try_from(now.as_millis()).map_err(|e| format!("the clock is out of range: {e}"))
}

/// How long ago the deadline of this wait passed, in seconds.
///
/// `None` means the wait still has time, or has no deadline at all.
fn expired(reader: &Connection, execution_id: &str, signal: &str) -> Result<Option<i64>, String> {
    let Some(fire_at) = inspect::signal_deadline(reader, execution_id, signal)? else {
        return Ok(None);
    };
    let now = epoch_millis()?;
    if now < fire_at {
        return Ok(None);
    }
    Ok(Some((now - fire_at) / 1000))
}

/// Report the recorded event log of one session.
///
/// The session must exist. An event read of an unknown id returns no rows, so
/// without this check a mistyped audit target prints an empty history and
/// exits clean. That reads as a session that did nothing.
fn history(reader: &Connection, execution_id: &str, before: Option<i64>) -> Response {
    // Parsed for the message it gives, and not for a value. A mistyped id is
    // told apart from a real one this database does not hold.
    if let Err(e) = execution_id.parse::<ExecutionId>() {
        return Response::Error {
            message: format!("`{execution_id}` is not an execution id: {e}"),
        };
    }
    match inspect::is_session(reader, WORKFLOW_NAME, execution_id) {
        Ok(true) => {}
        Ok(false) => {
            return Response::Error {
                message: format!("no session {execution_id}"),
            };
        }
        Err(message) => return Response::Error { message },
    }
    // Read the newest events and no more. Each model activity records the
    // whole transcript, and the count grows with every turn. Loading the log
    // entire would spend the daemon's memory on one command. It would also
    // block every session drive while it ran.
    match inspect::event_lines(
        reader,
        execution_id,
        before,
        inspect::MAX_HISTORY_EVENTS + 1,
    ) {
        Ok(mut page) => {
            let more = page.len() > inspect::MAX_HISTORY_EVENTS as usize;
            if more {
                page.pop();
            }
            // The log reads forward, and each line carries the event's own
            // sequence number rather than a position in this page.
            page.reverse();
            // The omitted events must be reachable, or a bounded audit trail
            // is a lost one. The CURSOR is returned and not a command. Only
            // the client knows which socket it asked, and a command built
            // here would send the operator to another daemon.
            let older = more.then(|| page.first().map(|line| line.seq)).flatten();
            Response::History {
                events: page
                    .iter()
                    .map(|line| format!("{:>3}  {}", line.seq, describe(line)))
                    .collect(),
                execution_id: execution_id.to_string(),
                older,
            }
        }
        Err(message) => Response::Error { message },
    }
}

/// Describe one recorded event for the audit trail.
///
/// A type label alone cannot answer what the agent did, which is the whole
/// point of the command. So each event carries its own data too, rendered
/// compactly and trimmed to one readable line. The rendering is generic and
/// prints whatever the event holds. A new event variant therefore needs no
/// change here, and is never reduced to a bare name.
fn describe(line: &inspect::EventLine) -> String {
    let cap = inspect::MAX_EVENT_DETAIL_CHARS as usize;
    match line.detail.as_deref() {
        Some(detail) if !detail.is_empty() && detail != "null" => {
            // The database read one character past the cap, so a longer
            // detail is known to be cut without reading the rest of it.
            let mut rendered: String = detail.chars().take(cap).collect();
            if detail.chars().count() > cap {
                rendered.push('…');
            }
            format!("{}  {rendered}", line.label)
        }
        _ => line.label.clone(),
    }
}

/// Project every execution row into an operator view.
pub fn sessions(
    reader: &Connection,
    blocked: &Parked,
    full: bool,
    before: Option<i64>,
) -> Result<(Vec<SessionView>, bool, Option<i64>), String> {
    let mut rows = inspect::executions(reader, WORKFLOW_NAME, before)?;
    // One row past the cap was read, so the caller can say there are more
    // without a second query. The extra one is not shown.
    let more = rows.len() > inspect::MAX_LISTED_SESSIONS as usize;
    if more {
        rows.remove(0);
    }
    // The cursor is the OLDEST row this page shows, and the rows read oldest
    // first. A page of sessions is therefore reachable from the one after it,
    // however many newer sessions arrive. A capped listing with no cursor
    // would hide an old session still waiting for a decision.
    let older = more.then(|| rows.first().map(|row| row.row)).flatten();
    Ok((
        rows.iter()
            .map(|row| summary_view(reader, row, blocked, full))
            .collect(),
        more,
        older,
    ))
}

/// One listed field, cut to the printed cap and MARKED when it was cut.
///
/// The projection reads one character past the cap, so a longer field is
/// known to be cut without reading the rest of it. An unmarked prefix
/// presented a partial answer as the whole one, and the operator had no
/// reason to open the single status. This is what [`describe`] does for one
/// event, applied to the four fields a listing prints.
fn shortened(text: &str) -> String {
    let cap = inspect::MAX_LISTED_CHARS as usize;
    let mut rendered: String = text.chars().take(cap).collect();
    if text.chars().count() > cap {
        rendered.push('…');
    }
    rendered
}

/// Build one operator view from a bounded listing row.
///
/// The listing reads each field already cut, so this never holds a whole
/// recorded payload. See [`inspect::SessionSummary`].
fn summary_view(
    reader: &Connection,
    row: &inspect::SessionSummary,
    blocked: &Parked,
    full: bool,
) -> SessionView {
    // A report is shown only when ALL FOUR fields `SessionReport` declares
    // are readable. The projection reports an unreadable count as nothing,
    // and a zero in its place would present a damaged report as a genuine
    // result: `[end_turn after 0 turns, 0 tool calls]`.
    //
    // A row with none of them readable says nothing, which is what the single
    // status does with a report it cannot deserialise. A row with some of
    // them is named as unreadable. The line cannot be built, and a silence
    // would read as "no report yet".
    //
    // The ANSWER is one of the four. An empty answer is a real outcome: a
    // session can end with the model writing no text. The projection reports
    // that as an empty string, and not as nothing. So a report with NO
    // readable answer is named unreadable, rather than shown as that
    // outcome. `status` refuses the same document, and the two must agree.
    //
    // A document that REPEATS a key answers every projection and still fails
    // to deserialise. Nothing in the four fields shows it, so the row carries
    // that fault on its own. See [`inspect::SessionSummary::report_is_damaged`].
    let answer = match (
        row.stop.as_deref(),
        row.turns,
        row.tool_calls,
        row.answer.as_deref(),
    ) {
        _ if row.report_is_damaged => Some("<unreadable report>".to_string()),
        (Some(stop), Some(turns), Some(calls), Some(text)) => Some(format!(
            "[{} after {turns} turns, {calls} tool calls] {}",
            shortened(stop),
            shortened(text)
        )),
        (None, None, None, None) => None,
        _ => Some("<unreadable report>".to_string()),
    };
    // A row whose id cannot be read is still LISTED. An operator can see
    // that the row exists, which a silent omission would deny them. No
    // follow-up command can name it, because it holds no id to name. The
    // reads below are given an id that parses as nothing, so the row shows
    // no pending call rather than another session's.
    let exec_id = row.exec_id.as_deref();
    let state = exec_id
        .and_then(|id| id.parse::<ExecutionId>().ok())
        .and_then(|exec| blocked.get(&exec));
    let (pending, blocked_on) = decidable(reader, exec_id.unwrap_or_default(), state, full);

    SessionView {
        execution_id: exec_id.map_or_else(|| "<unreadable id>".to_string(), shortened),
        goal: row
            .goal
            .as_deref()
            .map_or_else(|| "<unreadable task>".to_string(), shortened),
        state: row
            .state
            .as_deref()
            .map_or_else(|| "<unreadable state>".to_string(), shortened),
        blocked_on,
        pending,
        answer,
        // A damaged reason is NAMED. A silence there would read as a failure
        // that recorded no reason, and `status` calls the same row
        // unreadable. See [`inspect::SessionSummary::error_is_damaged`].
        error: row.error.as_deref().map(shortened).or_else(|| {
            row.error_is_damaged
                .then(|| "<unreadable error>".to_string())
        }),
    }
}

/// The pending call a status shows, and the reason beside it.
///
/// A wait whose deadline has ALREADY fired is not decidable. `approve`
/// refuses it, and the session denies that call on its next drive, so a
/// status must not print a decision the daemon will reject.
///
/// The parked state is what the last drive left behind, and it says nothing
/// about the clock. The tick clears the wait when it next runs, and
/// `--tick-ms` decides how long that takes. This reads the deadline itself,
/// rather than waiting for the tick to catch up.
///
/// A deadline read that FAILS is not an expiry. Hiding a call the operator
/// can still decide is worse than showing one they cannot.
///
/// Both views are built from this, so a listing and a single status cannot
/// disagree about what is decidable.
pub fn decidable(
    reader: &Connection,
    exec_id: &str,
    state: Option<&ParkedState>,
    full: bool,
) -> (Option<PendingCall>, Option<String>) {
    let Some(state) = state else {
        return (None, None);
    };
    let Some(signal) = state.signal.as_deref() else {
        return (None, Some(state.reason.clone()));
    };
    if let Ok(Some(seconds)) = expired(reader, exec_id, signal) {
        return (
            None,
            Some(format!(
                "the deadline for this call passed {seconds} seconds ago; the \
                 session denies it on its next drive"
            )),
        );
    }
    // A read that FAILS is not a call that is not there. An error here is the
    // query, the table, or a newest reply that does not hold the awaited
    // call. Saying nothing would leave a session named as waiting with no
    // call to read and no command to answer it.
    //
    // The message says which of those happened. No call is offered from any
    // of them, because the call the operator would read may not be the call
    // the token releases.
    match pending_call(reader, exec_id, signal, full) {
        Ok(pending) => (pending, Some(state.reason.clone())),
        Err(message) => (
            None,
            Some(format!("{}; no call is offered: {message}", state.reason)),
        ),
    }
}

/// Build one operator view.
fn view(reader: &Connection, row: &ExecutionRow, blocked: &Parked, full: bool) -> SessionView {
    let goal = serde_json::from_str::<SessionTask>(&row.input_json)
        .map_or_else(|_| "<unreadable task>".to_string(), |task| task.goal);
    let answer = row
        .output_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<SessionReport>(raw).ok())
        .map(|report| {
            format!(
                "[{} after {} turns, {} tool calls] {}",
                report.stop, report.turns, report.tool_calls, report.answer
            )
        });
    let exec = row.exec_id.parse::<ExecutionId>().ok();
    let state = exec.and_then(|exec| blocked.get(&exec));
    let (pending, blocked_on) = decidable(reader, &row.exec_id, state, full);

    SessionView {
        execution_id: row.exec_id.clone(),
        goal,
        state: row.state.clone(),
        blocked_on,
        pending,
        answer,
        error: row.error.clone(),
    }
}

/// What `status` says when the newest model reply cannot be named.
///
/// A tool-use id is unique inside ONE reply. Offering the newest reply this
/// daemon CAN read would offer a call from an older turn beside a live
/// approval token. See [`inspect::newer_turn_evidence`].
const UNNAMEABLE_REPLY: &str = "a model reply newer than the newest readable one is \
     recorded, so the reply holding the awaited call cannot be named";

/// Read the awaited tool call back out of the event log.
///
/// The daemon holds no copy of it. The call was recorded as the result of the
/// model activity, so the history is the source of truth here. That is true of
/// the run itself as well. The most recent model reply holds the awaited call,
/// so the scan runs backwards.
///
/// The read is PAGED, and not windowed. Every status of a parked session comes
/// here. The history grows with every turn, and each model activity carries
/// the whole transcript, so reading it entire would block every session drive.
///
/// A fixed window would be worse than slow. One turn may ask for many tool
/// calls, and each one records events of its own. The model reply that holds
/// the awaited call can therefore sit any distance back.
///
/// The search reads ONE model reply, the newest. A parked session waits on a
/// call of that reply: the run cannot call the model again until the call it
/// parked on resolves. The tool events after that reply are the only ones the
/// read passes over.
///
/// The page size is one for that reason, and it is not a memory bound. The
/// `stop_reason` test is not indexed, so the database must read and decode
/// each row to know whether it matches. A larger page reads BACKWARD past
/// older replies until it has that many, which on a long history means the
/// whole log for one `status`.
///
/// The search does NOT go further back, and that is a safety property rather
/// than a saving. A tool-use id is unique inside one reply only. An older
/// reply can hold the same id for a different tool, so a read further back
/// could offer THAT call beside this token. The operator would approve what
/// they read and release the call they never saw.
///
/// A newest reply that does not hold the awaited call is therefore a fault,
/// not a reason to look elsewhere. No parked session can produce one. See
/// [`inspect::reply_calls`].
///
/// # Errors
///
/// Returns an error if the replies cannot be read, or if the newest reply
/// does not hold the awaited call. Neither is the same as a call the history
/// does not hold. The caller says so, rather than showing a waiting session
/// with nothing to decide.
pub fn pending_call(
    reader: &Connection,
    exec_id: &str,
    signal: &str,
    full: bool,
) -> Result<Option<PendingCall>, String> {
    let Some(call_id) = session::approval_call_id(signal) else {
        return Ok(None);
    };

    // The query returns the calls of one model reply, and nothing else of it.
    // The transcript stays in the database.
    let page = inspect::reply_calls(reader, exec_id, None, REPLY_PAGE)
        .map_err(|e| format!("the model replies cannot be read: {e}"))?;
    let Some((seq, newest)) = page.into_iter().next() else {
        // No reply this daemon can read. A turn may still be recorded, and
        // a row it cannot read at all may be one. An absence is therefore
        // reported only when there is no evidence of either.
        return match inspect::newer_turn_evidence(reader, exec_id, i64::MIN)? {
            (0, 0) => Ok(None),
            _ => Err(UNNAMEABLE_REPLY.to_string()),
        };
    };

    // The reply above was found by a `stop_reason` in its OWN payload. A
    // newer reply with a damaged payload is therefore not in the page at all.
    // Two rows that the damage cannot reach say when that happened: a turn
    // scheduled after this reply, and a row of unknown kind after it. Either
    // means the awaited call may sit in a reply this daemon cannot name.
    if inspect::newer_turn_evidence(reader, exec_id, seq)? != (0, 0) {
        return Err(UNNAMEABLE_REPLY.to_string());
    }

    // Every answer below comes from the NEWEST reply. A reply further back
    // can hold the awaited id for a different tool, so it is never read.
    let calls = match newest {
        inspect::ReplyCalls::Calls(calls) => calls,
        inspect::ReplyCalls::NoCalls => {
            return Err(
                "the newest model reply asked for no tool call, so the call this session \
                 waits on cannot be shown"
                    .to_string(),
            );
        }
        inspect::ReplyCalls::Unreadable => {
            return Err(
                "the newest model reply cannot be read, so the call it waits on cannot \
                 be shown"
                    .to_string(),
            );
        }
    };
    let Some(call) = calls.into_iter().find(|call| call.id == call_id) else {
        return Err(
            "the newest model reply does not hold the call this session waits on, so \
             that call cannot be shown"
                .to_string(),
        );
    };

    let mut input = call.input.to_string();
    // A decision needs the WHOLE payload, and a write carries up to 64 KiB.
    // The status trims it to stay readable, and `--full` prints every byte.
    //
    // A trimmed view says so, and the client offers no approval from it. The
    // operator would otherwise read a cut call and paste the command that
    // authorises all of it.
    let truncated = !full && input.chars().count() > MAX_PENDING_INPUT_CHARS;
    if truncated {
        input = shortest_first(&call.input)
            .chars()
            .take(MAX_PENDING_INPUT_CHARS)
            .collect();
        input.push_str(
            " … (truncated; no approval is offered from this view; \
             read it all with `status --full`)",
        );
    }
    Ok(Some(PendingCall {
        token: signal.to_string(),
        id: call.id,
        tool: call.name,
        input,
        truncated,
    }))
}

/// One call's arguments with the SHORTEST field first.
///
/// A cut view keeps what comes first, and the fields serialise in key order.
/// A `write_file` call carries `content` before `path`, so a long content
/// pushes the destination past the cut. The operator would read a cut call
/// that does not say where it writes.
///
/// The order is by the LENGTH of each value, so this is not the recorded
/// call. It is built only for a view that is already cut, and the full view
/// prints the call verbatim.
fn shortest_first(input: &serde_json::Value) -> String {
    let Some(fields) = input.as_object() else {
        return input.to_string();
    };
    let mut pairs: Vec<(&String, String)> = fields
        .iter()
        .map(|(key, value)| (key, value.to_string()))
        .collect();
    pairs.sort_by_key(|(_, text)| text.chars().count());
    let body: Vec<String> = pairs
        .iter()
        .map(|(key, text)| format!("{}:{text}", serde_json::Value::String((*key).clone())))
        .collect();
    format!("{{{}}}", body.join(","))
}

/// Create the workspace, make it durable, and record its resolved name.
///
/// # Errors
///
/// Returns an error if the workspace cannot be created or resolved, if its
/// name is not valid UTF-8, or if the database is inside it.
fn prepare_workspace(workspace: &Path, db: &Path) -> Result<String, String> {
    // Created enterable by its owner. A umask that masks the owner bits would
    // otherwise give the new workspace mode `000`. Nothing could then be
    // written inside it. See [`tools::create_enterable`].
    tools::create_enterable(workspace)
        .map_err(|e| format!("cannot create the workspace {}: {e}", workspace.display()))?;
    // Resolve it once. Every session records this value, and the tool activity
    // refuses a call whose session belongs to a different workspace.
    let resolved = workspace
        .canonicalize()
        .map_err(|e| format!("cannot resolve the workspace {}: {e}", workspace.display()))?;
    // `create_dir_all` above wrote no directory entry to disk. The write path
    // flushes from a target up to the workspace root, and the entry that NAMES
    // the root lives above it. See [`flush_workspace_path`].
    flush_workspace_path(&resolved);
    // The model can write to any path inside the workspace, and `write_file`
    // replaces its target. The database, and its `-wal` sidecar beside it,
    // must therefore not be reachable from there.
    refuse_state_in_workspace(db, &resolved)?;

    // A lossy conversion would mangle a path that is not valid UTF-8, and the
    // recorded identity would then never match the real one again. Refuse the
    // path instead of recording a name that cannot be compared.
    let recorded = resolved.to_str().ok_or_else(|| {
        format!(
            "the workspace path {} is not valid UTF-8. Each session records \
                 this path, so a name that cannot be written down exactly would \
                 never match again.",
            resolved.display()
        )
    })?;

    // A character no printed command can carry is refused HERE, before any
    // session records the path. A workspace mismatch prints a `--workspace`
    // command to resume the session, and `visible` shows such a character as
    // an escape. A copied command would then name a path nobody recorded, and
    // it would start a daemon on a NEW directory rather than resume anything.
    // Quoting cannot fix that: the fault is the rendering, not the argument.
    // See [`crate::unprintable`], which the socket path goes through too.
    if let Some(refused) = crate::unprintable(recorded) {
        return Err(format!(
            "the workspace path {} holds {}, which no command this daemon \
             prints can carry. A session that belongs to another workspace is \
             refused with the command that resumes it, and that command names \
             the path on ONE line. Choose a workspace of ordinary text.",
            crate::one_line(recorded),
            refused.escape_unicode()
        ));
    }
    Ok(recorded.to_string())
}

/// The directories above the workspace, closest first.
///
/// The workspace itself is not in the list. Every write flushes it already,
/// because it is the top of the chain the write path walks.
pub fn path_above(workspace: &Path) -> Vec<PathBuf> {
    workspace
        .ancestors()
        .skip(1)
        .map(Path::to_path_buf)
        .collect()
}

/// Make the workspace's own directory entry durable.
///
/// `create_dir_all` writes no entry to disk. A power loss after a committed
/// write could therefore remove the workspace that the daemon created, and the
/// recorded file with it. The write path cannot cover this: it flushes from a
/// target up to the root, and the entry that names the root is above it.
///
/// The whole chain is flushed, and not only what this process created.
/// A daemon that created those directories and then died would leave the next
/// start with nothing to flush and the same unwritten entries.
///
/// A directory that cannot be opened or flushed is logged and skipped. This is
/// durability work on an operator's own tree. It is not a reason to refuse to
/// serve.
fn flush_workspace_path(workspace: &Path) {
    for directory in path_above(workspace) {
        let flushed = std::fs::File::open(&directory).and_then(|handle| handle.sync_all());
        if let Err(e) = flushed {
            tracing::warn!(
                path = %crate::one_line(&directory.display().to_string()),
                error = %crate::one_line(&e.to_string()),
                "cannot flush a directory above the workspace"
            );
        }
    }
}

/// Refuse a database that the agent can reach.
///
/// `write_file` replaces its target through a rename. A database inside the
/// workspace is therefore one approved tool call away from replacement.
/// `SQLite` and the daemon lock still hold the old inode after that. The next
/// commits fail, and a restart opens the replacement instead of the recorded
/// history. The `-wal` and `-shm` sidecars sit beside the database and carry
/// the same data, so one containment test covers all three.
///
/// The refusal is at startup, where it costs an operator one flag. The
/// alternative is a list of reserved names in the toolbox, which has to stay
/// in step with whatever the engine writes beside its database.
fn refuse_state_in_workspace(db: &Path, workspace: &Path) -> Result<(), String> {
    let real = resolve_database(db)?;
    if !real.starts_with(workspace) {
        return Ok(());
    }

    Err(format!(
        "the database {} is inside the workspace {}. A tool call can write to \
         any path in the workspace. Replacing the database, or its `-wal` \
         sidecar, would destroy the history this daemon runs from. Keep the \
         database outside the workspace with `--db` or `--workspace`.",
        db.display(),
        workspace.display()
    ))
}

/// Where the database really is.
///
/// The final component is resolved too, and not only the directory that holds
/// it. A symbolic link outside the workspace can name a target inside it, and
/// the lock and `SQLite` both follow the link. A test on the link's own path
/// would report the safe side of a rule the daemon then breaks.
///
/// A database that does not exist yet has no target to resolve, so the
/// directory that will hold it is resolved instead. A link that resolves to
/// nothing is refused rather than guessed at. The file it creates would land
/// wherever the link points, and that is the question being asked here.
fn resolve_database(db: &Path) -> Result<PathBuf, String> {
    if let Ok(real) = db.canonicalize() {
        return Ok(real);
    }
    if db.symlink_metadata().is_ok() {
        return Err(format!(
            "the database {} is a symbolic link that resolves to nothing. \
             The daemon cannot say where it would write. Name the database \
             itself with `--db`.",
            db.display()
        ));
    }

    let directory = match db.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    let resolved = directory
        .canonicalize()
        .map_err(|e| format!("cannot resolve the directory of {}: {e}", db.display()))?;
    let name = db
        .file_name()
        .ok_or_else(|| format!("{} does not name a database file", db.display()))?;
    Ok(resolved.join(name))
}

/// Add a session to the set the tick drives.
fn enlist(live: &mut Live, exec: ExecutionId) {
    if !live.contains(&exec) {
        live.push(exec);
    }
}

/// Drop a session that reached a terminal state.
fn retire(blocked: &mut Parked, live: &mut Live, exec: ExecutionId) {
    blocked.remove(&exec);
    live.retain(|id| *id != exec);
}

/// What a session waiting on this signal is waiting FOR.
///
/// Written once, because two places record it. The drive learns the wait from
/// the engine, and the startup reads the wait out of the database. A second
/// spelling would let the two disagree about the same session.
fn waiting_reason(name: &str) -> String {
    if session::approval_call_id(name).is_some() {
        "waiting for a tool approval".to_string()
    } else {
        format!("waiting for the `{name}` signal")
    }
}

/// Drive one session to its next stopping point.
///
/// A session leaves the live set only on a state that cannot be driven again.
/// An unrecognised outcome, or a drive error, keeps it. One wasted drive costs
/// a tick. A session dropped in error would never be driven again, because
/// this daemon is the only writer.
async fn drive_one(
    runtime: &mut SqliteRuntime,
    exec: ExecutionId,
    blocked: &mut Parked,
    live: &mut Live,
) {
    match runtime.run_until_blocked(exec).await {
        Ok(RunState::WaitingSignal(name)) => {
            note(blocked, exec, waiting_reason(&name), Some(name));
        }
        Ok(RunState::WaitingTimer) => {
            note(
                blocked,
                exec,
                "waiting for a durable timer".to_string(),
                None,
            );
        }
        Ok(RunState::Completed(output)) => {
            retire(blocked, live, exec);
            // The output holds the model's own answer, and this log reaches a
            // terminal. See [`crate::one_line`].
            tracing::info!(%exec, output = %crate::one_line(&output.to_string()), "session completed");
        }
        Ok(RunState::Failed(error)) => {
            retire(blocked, live, exec);
            // The reason carries the API's error body, up to 400 characters
            // of it, which is text this daemon did not write.
            tracing::error!(%exec, error = %crate::one_line(&error), "session failed");
        }
        Ok(RunState::InProgress) => {
            blocked.remove(&exec);
        }
        // A drive failure carries the activity's own message, which holds
        // the same untrusted text one step further in.
        Err(e) => {
            tracing::error!(%exec, error = %crate::one_line(&e.to_string()), "cannot drive the session");
        }
    }
}

/// Record why a session is parked, logging only the changes.
fn note(blocked: &mut Parked, exec: ExecutionId, reason: String, signal: Option<String>) {
    if blocked.get(&exec).map(|state| state.reason.as_str()) != Some(reason.as_str()) {
        tracing::info!(%exec, reason = %reason, "session parked");
    }
    blocked.insert(exec, ParkedState { reason, signal });
}
