//! `agentd` — a durable agent harness for Claude, running as a local daemon on
//! embedded `SQLite`.
//!
//! The daemon runs an agent loop as an `autumn-harvest` workflow. Every model
//! call and every tool call is a durable activity. A session therefore
//! survives a restart: recorded turns replay from history instead of being
//! paid for a second time. The whole engine is embedded, so there is no
//! database server and no Docker.
//!
//! ```text
//! agentd serve &                         # the daemon: the only writer
//! agentd submit "summarise the README"   # returns an execution id
//! agentd status <id>                     # the session, including why it parked
//! agentd approve <id> <token>            # release a gated write
//! agentd history <id>                    # the recorded event log
//! ```
//!
//! See `README.md` for the full walkthrough, including the restart proof.

// This backend runs the registered closures, not the async fn bodies. The
// `#[activity]` macro expands each await-free placeholder body into code that
// consumes the `_`-prefixed parameters. Both lints are artifacts of that
// pattern.
#![allow(clippy::unused_async, clippy::used_underscore_binding)]

mod claude;
mod daemon;
mod guard;
mod inspect;
mod protocol;
mod session;
mod shutdown;
mod tools;

#[cfg(test)]
mod tests;

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use crate::protocol::{Request, Response, SessionView};

/// The durable Claude agent daemon.
#[derive(Parser)]
#[command(
    name = "agentd",
    version,
    about = "A durable Claude agent daemon on SQLite"
)]
struct Cli {
    /// The workflow database file.
    #[arg(long, global = true, env = "AGENTD_DB", default_value = "agentd.db")]
    db: PathBuf,
    /// The control socket the daemon listens on.
    #[arg(
        long,
        global = true,
        env = "AGENTD_SOCKET",
        default_value = protocol::DEFAULT_SOCKET
    )]
    socket: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the daemon. This process is the single writer.
    Serve {
        /// The directory the tools may read and write.
        #[arg(long, env = "AGENTD_WORKSPACE", default_value = "agent-workspace")]
        workspace: PathBuf,
        /// The model every session calls.
        #[arg(long, env = "AGENTD_MODEL", default_value = claude::DEFAULT_MODEL)]
        model: String,
        /// The output cap of one turn.
        ///
        /// The Messages API requires at least one token, and a zero cap would
        /// be refused there. That refusal is not retryable, so every session
        /// submitted to such a daemon would fail. The floor is here instead.
        #[arg(
            long,
            env = "AGENTD_MAX_TOKENS",
            default_value_t = claude::DEFAULT_MAX_TOKENS,
            value_parser = clap::value_parser!(u32).range(1..)
        )]
        max_tokens: u32,
        /// How often the daemon drives its sessions, in milliseconds.
        ///
        /// A zero period has no meaning and panics the timer, so one is the
        /// floor.
        #[arg(
            long,
            env = "AGENTD_TICK_MS",
            default_value_t = 500,
            value_parser = clap::value_parser!(u64).range(1..)
        )]
        tick_ms: u64,
    },
    /// Start one session.
    Submit {
        /// The task, in your own words.
        goal: String,
        /// The hard bound on model calls.
        ///
        /// Zero turns is not a session. The loop would run no iteration and
        /// record a completed run with a blank answer.
        #[arg(long, default_value_t = 8, value_parser = clap::value_parser!(u32).range(1..))]
        max_turns: u32,
        /// How long a gated tool call waits for approval.
        #[arg(long, default_value_t = 900)]
        approval_timeout_secs: u64,
    },
    /// Report one session.
    Status {
        execution_id: String,
        /// Print the pending call's arguments in full.
        #[arg(long)]
        full: bool,
    },
    /// Report every session.
    List {
        /// Read the page of sessions before this row, from a listing this
        /// daemon printed.
        #[arg(long)]
        before: Option<i64>,
    },
    /// Print the recorded event log of one session.
    History {
        execution_id: String,
        /// Read the events BEFORE this sequence number.
        ///
        /// A long log prints its newest events and names the number to pass
        /// here for the ones before them.
        #[arg(long)]
        before: Option<i64>,
    },
    /// Release one gated tool call.
    ///
    /// `token` is the approval token `status` printed. It names one wait of one
    /// run, which is what keeps the decision tied to the call you read.
    Approve {
        execution_id: String,
        token: String,
        #[arg(long)]
        note: Option<String>,
    },
    /// Refuse one gated tool call.
    Deny {
        execution_id: String,
        token: String,
        #[arg(long)]
        note: Option<String>,
    },
}

/// `#[tokio::main]` builds a multi-thread runtime. The model activity needs
/// one: its body is synchronous and bridges to async through
/// `tokio::task::block_in_place`, which a current-thread runtime rejects.
#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{}", failure(&message));
            ExitCode::FAILURE
        }
    }
}

/// The line an operator reads when a command fails.
///
/// The message leaves through [`visible`], as every printed line does. A
/// failure names what it refused, and the refusal of a socket path holds that
/// path. Writing it raw would obey the very characters the refusal exists to
/// refuse.
///
/// A message of several lines stays several lines. `visible` keeps the
/// newline, because a refusal that names a follow-up command puts it on its
/// own line.
fn failure(message: &str) -> String {
    format!("agentd: {}", visible(message))
}

/// The API key, from the environment only.
///
/// There is deliberately no `--api-key` flag. A process's arguments are
/// readable by every user of the host, through `ps` or `/proc/<pid>/cmdline`.
/// This daemon runs for as long as its sessions do. A key on the command line
/// would therefore be readable by the users the owner-only socket exists to
/// keep out.
///
/// An absent key selects the offline stub model.
fn api_key() -> Option<String> {
    usable_key(&std::env::var("ANTHROPIC_API_KEY").unwrap_or_default())
}

/// Read one environment value as a key, or as no key at all.
///
/// A key of whitespace is not a key. It would otherwise count as present, and
/// the daemon would run live against it. Every turn would then fail at the
/// API, where an absent key runs the offline stub instead.
///
/// The value is trimmed, because an operator commonly reads a key out of a
/// file and keeps the newline. A header carries that byte to the API, which
/// rejects it, and the error names neither the newline nor the file.
fn usable_key(raw: &str) -> Option<String> {
    let key = raw.trim();
    (!key.is_empty()).then(|| key.to_string())
}

/// The first character of this text that no printed command can carry.
///
/// A refusal and the command it suggests are printed on ONE line, and that
/// line is made to be read and to be copied. A newline splits it. A tab
/// renders as spaces, so the line an operator READS is not the line they
/// copy. Every other character a terminal acts on is shown as an escape by
/// [`visible`], so a copied command would name a path nobody recorded.
///
/// Quoting does not answer this. `--workspace='a\u{000d}b'` carries the
/// character into `argv` faithfully, and the refusal still SHOWS the escape.
/// See [`breaks_one_line`].
fn unprintable(text: &str) -> Option<char> {
    text.chars().find(|c| breaks_one_line(*c))
}

/// Refuse a socket path this daemon cannot print.
///
/// Every command prints follow-up commands that name the socket, and a path
/// is bytes rather than text on this platform. A path that is not UTF-8
/// cannot be written into one of those lines unchanged. A copied line would
/// then reach another socket, or none at all.
///
/// The refusal comes before any command runs. The daemon does not print a
/// path it cannot print.
///
/// Being UTF-8 is not enough on its own. Every printed line leaves through
/// [`visible`], which REWRITES a character a terminal would act on. A path
/// holding one is printed as the escape rather than as itself, so the copied
/// command names a different socket. That is the same fault as a path that is
/// not text, one step further on.
///
/// A newline is refused here as well, and `visible` keeps that character.
/// See [`breaks_one_line`].
///
/// # Errors
///
/// Returns an error if the path is not UTF-8, or if it holds a character no
/// printed command can carry.
fn printable(socket: &Path) -> Result<(), String> {
    // The refusal NAMES the path, and a failure leaves through [`visible`],
    // which keeps the newline and the tab. Those are two of the characters
    // refused here, so the path is rendered by the ONE-LINE sink instead. A
    // refusal that split itself into forged lines would do the damage it
    // exists to refuse.
    let shown = one_line(&socket.display().to_string()).into_owned();
    let Some(text) = socket.to_str() else {
        return Err(format!(
            "the socket path {shown} is not UTF-8. This daemon prints commands \
             that name the socket, and it cannot print this one. Choose a path \
             of text."
        ));
    };
    if let Some(refused) = unprintable(text) {
        return Err(format!(
            "the socket path {shown} holds {}, which no command this daemon \
             prints can carry. Every one of them names the socket on ONE line, \
             and that character would split the line or be shown as an escape. \
             A copied command would then name another socket. Choose a path of \
             ordinary text.",
            refused.escape_unicode()
        ));
    }
    Ok(())
}

/// Dispatch one command.
async fn run(cli: Cli) -> Result<(), String> {
    printable(&cli.socket)?;
    match cli.command {
        Command::Serve {
            workspace,
            model,
            max_tokens,
            tick_ms,
        } => {
            tracing_subscriber::fmt()
                .with_env_filter(
                    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
                )
                .init();
            daemon::serve(daemon::Options {
                db: cli.db,
                socket: cli.socket,
                workspace,
                model,
                max_tokens,
                tick: Duration::from_millis(tick_ms),
                api_key: api_key(),
            })
            .await
        }
        Command::Submit {
            goal,
            max_turns,
            approval_timeout_secs,
        } => report(
            protocol::call(
                &cli.socket,
                &Request::Submit {
                    goal,
                    max_turns,
                    approval_timeout_secs,
                },
            )
            .await?,
            &cli.socket,
        ),
        Command::Status { execution_id, full } => report(
            protocol::call(&cli.socket, &Request::Status { execution_id, full }).await?,
            &cli.socket,
        ),
        Command::List { before } => report(
            protocol::call(&cli.socket, &Request::List { before }).await?,
            &cli.socket,
        ),
        Command::History {
            execution_id,
            before,
        } => report(
            protocol::call(
                &cli.socket,
                &Request::History {
                    execution_id,
                    before,
                },
            )
            .await?,
            &cli.socket,
        ),
        Command::Approve {
            execution_id,
            token,
            note,
        } => report(
            protocol::call(
                &cli.socket,
                &Request::Approve {
                    execution_id,
                    token,
                    approved: true,
                    note,
                },
            )
            .await?,
            &cli.socket,
        ),
        Command::Deny {
            execution_id,
            token,
            note,
        } => report(
            protocol::call(
                &cli.socket,
                &Request::Approve {
                    execution_id,
                    token,
                    approved: false,
                    note,
                },
            )
            .await?,
            &cli.socket,
        ),
    }
}

/// Print one line, and end quietly when the reader has gone.
///
/// `println!` panics once stdout is closed, so `agentd history … | head` would
/// end in a backtrace instead of a clean exit.
///
/// Every line this command prints goes through here, so [`visible`] is applied
/// once, at the sink, rather than at each place that formats model text.
fn line(text: &str) {
    use std::io::Write;
    drop(writeln!(std::io::stdout(), "{}", visible(text)));
}

/// Show a character a terminal would obey, instead of obeying it.
///
/// The model writes into this output: the answer, a tool name, a tool
/// argument. A file in the workspace can tell the model what to write, so all
/// of it is untrusted. An escape sequence would rewrite the display an
/// operator reads a pending call from, and `OSC 52` would write their
/// clipboard. The operator approves a call from what this prints.
///
/// A newline and a tab are kept. An answer uses them for layout, and neither
/// one moves the cursor back over text that is already written. A carriage
/// return is NOT kept: it returns to the start of the line, and what follows
/// overwrites what the operator already read.
///
/// The bidirectional controls are escaped as well. They obey nothing, but
/// they reorder what is displayed, so a path can be shown as a different path.
/// The whole `Bidi_Control` set is covered, and not only the overrides: a
/// single mark beside right-to-left text reorders it too.
///
/// The other format characters are left alone, because a joiner is part of
/// ordinary text.
///
/// The characters are shown, not removed. An operator can then see what
/// arrived, rather than a tidied version of it.
fn visible(text: &str) -> std::borrow::Cow<'_, str> {
    shown(text, is_obeyed)
}

/// One LOG field, with everything a terminal would act on shown instead.
///
/// The daemon logs values the model wrote: a session's answer, and the reason
/// a run failed, which carries the API's own error body. Its log goes to a
/// terminal, so it needs what the CLI's output already gets.
///
/// The newline is escaped here, and [`visible`] keeps it. A log line is ONE
/// line. A newline inside a field splits it, and the half that follows reads
/// as another log entry that nothing wrote.
fn one_line(text: &str) -> std::borrow::Cow<'_, str> {
    shown(text, breaks_one_line)
}

/// Escape every character the given test names, and keep the rest.
///
/// The characters are shown, not removed. A reader can then see what arrived,
/// rather than a tidied version of it.
fn shown(text: &str, refused: fn(char) -> bool) -> std::borrow::Cow<'_, str> {
    if !text.chars().any(refused) {
        return std::borrow::Cow::Borrowed(text);
    }
    let mut safe = String::with_capacity(text.len());
    for character in text.chars() {
        if refused(character) {
            let _ = write!(safe, "\\u{{{:04x}}}", character as u32);
        } else {
            safe.push(character);
        }
    }
    std::borrow::Cow::Owned(safe)
}

/// Would this character break the ONE line it is written into?
///
/// [`is_obeyed`] covers what a terminal ACTS on, and it exempts the newline
/// and the tab. A printed message carries both legitimately: an answer uses
/// them for layout, and a refusal puts a follow-up command on its own line.
///
/// A socket path and a log field are different. Both are read back from ONE
/// rendered line, so a character whose RENDERING is not its own text cannot
/// appear in either.
///
/// A newline in a socket path splits the printed command. The first half ends
/// inside an unterminated quote, and what an operator copies reaches another
/// socket, or none.
///
/// A tab is not its own text on screen. A terminal renders it as the gap to
/// the next tab stop. A copy of that region commonly carries the spaces it
/// drew, and not the tab. The quoting around the path preserves the byte, so
/// the fault is not the shell. The line an operator reads is not the line
/// they copy.
///
/// A log field has the same shape of fault. See [`one_line`].
fn breaks_one_line(character: char) -> bool {
    is_obeyed(character) || character == '\n' || character == '\t'
}

/// Would a terminal act on this character rather than print it?
fn is_obeyed(character: char) -> bool {
    // The Unicode `Bidi_Control` property, in full: the marks, the embeddings
    // and overrides, and the isolates.
    let bidi = matches!(character,
        '\u{061c}' | '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}');
    bidi || (character.is_control() && character != '\n' && character != '\t')
}

/// Print one answer from the daemon.
fn report(response: Response, socket: &Path) -> Result<(), String> {
    if let Response::Error { message } = response {
        return Err(message);
    }
    // A stale decision is a failure as well, and its message names a
    // follow-up command. The CLIENT renders it, so the command carries the
    // socket this command reached. See [`protocol::socket_flag`].
    if matches!(response, Response::Stale { .. }) {
        return Err(rendered_lines(&response, socket).join("\n"));
    }
    for text in rendered_lines(&response, socket) {
        line(&text);
    }
    Ok(())
}

/// Render one answer as the lines an operator reads.
///
/// Built apart from the printing, so a test can read what the operator would
/// see. Every follow-up command printed here must reach the daemon this
/// command reached, which is what `socket` carries.
fn rendered_lines(response: &Response, socket: &Path) -> Vec<String> {
    match response {
        Response::Submitted { execution_id } => vec![
            execution_id.clone(),
            format!(
                "Watch it with: agentd status{} {execution_id}",
                protocol::socket_flag(socket)
            ),
        ],
        Response::Session { session } => session_lines(session, socket),
        Response::Sessions {
            sessions,
            more,
            older,
        } => {
            if sessions.is_empty() {
                return vec!["no sessions yet".to_string()];
            }
            let mut lines: Vec<String> = sessions
                .iter()
                .flat_map(|session| {
                    let mut lines = session_lines(session, socket);
                    lines.push(String::new());
                    lines
                })
                .collect();
            // The daemon returns the cursor and the CLIENT builds the
            // command, so it reaches the daemon this command reached. A
            // listing that only said MORE would leave an old session waiting
            // for a decision with no way to reach it. See
            // [`protocol::socket_flag`].
            if let Some(older) = older {
                lines.push(format!(
                    "{} sessions shown; read the ones before them with `agentd list{} \
                     --before {older}`",
                    sessions.len(),
                    protocol::socket_flag(socket)
                ));
            } else if *more {
                lines.push(format!(
                    "the newest {} sessions are shown; the database holds more",
                    sessions.len()
                ));
            }
            lines
        }
        Response::History {
            events,
            execution_id,
            older,
        } => {
            let mut lines = events.clone();
            // The daemon returns the cursor, and the CLIENT builds the
            // command. Only the client knows which socket it asked. A command
            // that dropped the socket would send the operator to another
            // daemon. See [`protocol::socket_flag`].
            if let Some(older) = older {
                lines.insert(
                    0,
                    format!(
                        "… {} events shown; read the ones before them with \
                         `agentd history{} {execution_id} --before {older}`",
                        events.len(),
                        protocol::socket_flag(socket)
                    ),
                );
            }
            lines
        }
        Response::Stale {
            execution_id,
            waiting_on,
            sent,
        } => vec![format!(
            "session {execution_id} is now waiting on `{waiting_on}`, not `{sent}`. \
             Read it again with `agentd status{} {execution_id}` before deciding.",
            protocol::socket_flag(socket)
        )],
        Response::Ack { detail, history_of } => {
            let mut lines = vec![detail.clone()];
            // The daemon sends the id, and the CLIENT builds the command, so
            // it reaches the daemon this command reached. See
            // [`protocol::socket_flag`].
            if let Some(execution_id) = history_of {
                lines.push(format!(
                    "  read which won with: agentd history{} {execution_id}",
                    protocol::socket_flag(socket)
                ));
            }
            lines
        }
        // Returned as an error by `report`, which never reaches here.
        Response::Error { message } => vec![message.clone()],
    }
}

/// Render one session as the lines an operator reads.
///
/// The lines are built here, apart from the printing, so a test can read what
/// the operator would see. This view carries the model's own words, and the
/// operator approves a pending call from it, so every line leaves through
/// [`visible`].
fn session_lines(view: &SessionView, socket: &Path) -> Vec<String> {
    let mut lines = vec![
        format!("{}  {}", view.execution_id, view.state),
        format!("  goal:    {}", view.goal),
    ];
    if let Some(blocked) = &view.blocked_on {
        lines.push(format!("  blocked: {blocked}"));
    }
    if let Some(pending) = &view.pending {
        lines.push(format!("  pending: {} ({})", pending.tool, pending.id));
        lines.push(format!("           {}", pending.input));
        if pending.truncated {
            // An approval decides about the WHOLE call, and this view is cut.
            // The command that approves it is not offered here.
            //
            // The command that DENIES it is. A denial of a call nobody has
            // read refuses a write, which is the safe answer. Making the
            // operator read 64 KiB before they may refuse it is a reason to
            // skip the reading.
            lines.push(format!(
                "  decide:  read it all first: agentd status{} {} --full",
                protocol::socket_flag(socket),
                view.execution_id,
            ));
            lines.push(format!(
                "           agentd deny{} {} {}",
                protocol::socket_flag(socket),
                view.execution_id,
                pending.token
            ));
        } else {
            lines.push(format!(
                "  decide:  agentd approve{} {} {}   (or `deny`)",
                protocol::socket_flag(socket),
                view.execution_id,
                pending.token
            ));
        }
    }
    if let Some(answer) = &view.answer {
        lines.push(format!("  answer:  {answer}"));
    }
    if let Some(error) = &view.error {
        lines.push(format!("  error:   {error}"));
    }
    lines
        .into_iter()
        .map(|text| visible(&text).into_owned())
        .collect()
}
