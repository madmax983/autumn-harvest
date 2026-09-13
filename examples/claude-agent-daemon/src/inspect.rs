//! Read-only inspection of the workflow database.
//!
//! The runtime exposes per-execution reads, not a listing, so the daemon opens
//! a second connection in READ-ONLY mode to enumerate sessions. The backend
//! opens the file in WAL mode with a busy timeout for exactly this case: one
//! writer, plus the occasional reader. Never open a second WRITE handle.

use std::path::Path;
use std::time::Duration;

use rusqlite::{Connection, OpenFlags};

use crate::session::{self, SessionTask};

/// How many events one `history` command prints.
///
/// The audit trail is the reason the command exists, so the cap is high. The
/// newest events are kept, because they are what an operator reads first, and
/// the command says when it is not the whole log.
pub const MAX_HISTORY_EVENTS: u32 = 500;

/// How many sessions one listing carries.
///
/// `list` reads the whole row of every session it names. A session's goal and
/// its report are both unbounded, and the count grows for the life of the
/// file. An ordinary `agentd list` on an old database would read all of it
/// into memory, build a view of every row, and serialise the lot. The runtime
/// is serialised, so that also blocks every session drive until it ends.
///
/// The newest sessions are the ones an operator looks for. The cap takes
/// those, and the listing says when it is not the whole history.
pub const MAX_LISTED_SESSIONS: u32 = 200;

/// How long a read waits for the writer's transaction to commit.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// One RUNNING session: its id, and the task it started from.
pub struct RunningSession {
    pub exec_id: String,
    /// The recorded task. `None` when this daemon cannot read the row.
    pub task: Option<RecordedTask>,
    /// Does this row say RUNNING in a class this daemon cannot read?
    ///
    /// A TEXT-affinity column keeps a stored BLOB as a BLOB, so a damaged row
    /// can hold the right bytes in the wrong class. `state = 'RUNNING'` is
    /// false for one of those, and the row was left out of this set. The
    /// startup seeds the DRIVEN set from here. The session was never driven
    /// and never refused, so the daemon reported ready over work nothing
    /// would ever seal.
    ///
    /// The row is matched by its BYTES now, and this says which class they
    /// were in. A state in another class entirely is not matched at all,
    /// because it claims no live session to strand.
    pub state_is_damaged: bool,
    /// Does this row name its workflow in a class this daemon cannot read?
    ///
    /// The same fault as [`Self::state_is_damaged`], one column over. The
    /// name is matched by its BYTES for that reason, so a session of this
    /// workflow is found whatever class holds the name.
    ///
    /// Every column this query tests carries the same hazard, and each is
    /// answered. The name and the state are matched by their bytes. The task
    /// is guarded by its class, which leaves it unread. The id fails the
    /// whole query rather than naming the wrong row.
    pub name_is_damaged: bool,
}

/// The recorded task of a RUNNING session, cut to what a startup check reads.
///
/// The fields carry the types the task itself declares, so a value outside
/// one of them is not represented here. A recorded `-1` turn bound, or a
/// bound wider than the field, leaves no `RecordedTask` at all.
pub struct RecordedTask {
    /// The workspace this session was recorded against.
    pub workspace: String,
    /// The model this session was recorded against.
    pub model: String,
    /// The recorded turn bound. The caller refuses a zero, as `submit` does.
    pub max_turns: u32,
    /// The recorded approval deadline. The caller refuses one too large to
    /// arm, as `submit` refuses one.
    pub approval_timeout_secs: u64,
    /// Does the recorded task carry a goal that says something?
    ///
    /// The goal itself is dropped, and never returned. A restart reads every
    /// RUNNING row, a goal reaches the size of a control request, and the
    /// returned set must not grow with them.
    ///
    /// The test is Rust's own `trim`, which is what `submit` refuses a goal
    /// by. The two ends of that invariant therefore run the same code.
    pub has_goal: bool,
}

/// One row of `harvest_executions`.
pub struct ExecutionRow {
    pub exec_id: String,
    pub state: String,
    pub input_json: String,
    pub output_json: Option<String>,
    pub error: Option<String>,
}

/// One session as a LISTING shows it, with every field already bounded.
///
/// A listing names many sessions, so it carries no whole payload. The single
/// status of one session still reads its row entire, because that is the one
/// session the operator asked about.
#[derive(Debug)]
pub struct SessionSummary {
    /// The session's id, or `None` when the row does not hold one this
    /// daemon can read. An id is what every follow-up command names, so a
    /// row without one can be SEEN and cannot be acted on.
    pub exec_id: Option<String>,
    pub state: Option<String>,
    /// `None` when the recorded task cannot be read.
    pub goal: Option<String>,
    pub stop: Option<String>,
    /// The recorded turn count, or `None` when no report this daemon can read
    /// is stored. The type is the range: see [`COUNTER_CEILING`].
    pub turns: Option<u32>,
    pub tool_calls: Option<u32>,
    pub answer: Option<String>,
    /// The recorded failure reason, or `None` when the row holds none this
    /// daemon can read. See [`SessionSummary::error_is_damaged`], which says
    /// WHICH of those two a `None` is.
    pub error: Option<String>,
    /// Does the row hold an error that cannot be read?
    ///
    /// `error` answers `None` for a row that recorded no failure reason AND
    /// for one whose reason is in a class the engine cannot read. A `FAILED`
    /// session with a damaged reason would otherwise look like one that
    /// recorded no reason, and `status` calls the same row unreadable.
    pub error_is_damaged: bool,
    /// Does the recorded report repeat one of the four fields it declares?
    ///
    /// Each field is projected on its own, so a document that repeats a key
    /// answers every projection and still fails to deserialise as a whole.
    /// `json_type` reports the FIRST value of a repeated key, so even a
    /// repeat of another type passes the type guards.
    ///
    /// The listing then showed a genuine report for a document the single
    /// status refuses. The two must agree, so the repeat is reported and the
    /// caller names the row unreadable.
    ///
    /// Only the DECLARED keys are counted. A repeated key the report does not
    /// declare is measured to deserialise, because the whole document reads
    /// and the field is ignored. Counting every key would refuse a report
    /// that `status` shows.
    ///
    /// The count is filtered rather than made distinct. A `count(DISTINCT)`
    /// sorts in a temporary B-tree, which the plan guard refuses for this
    /// query.
    pub report_is_damaged: bool,
    /// Is the recorded task a document this daemon cannot read as a whole?
    ///
    /// The goal beside it is projected on its own, so a document the single
    /// status refuses can still answer that projection with a plausible goal.
    /// The listing then attributed a task to a session whose task nothing can
    /// read, and `status` called the same row unreadable.
    ///
    /// Five shapes reach that state, and each one is measured. A declared key
    /// is repeated. A declared key is absent. A declared key holds the wrong
    /// type. A count sits outside the range of the Rust field. A text field
    /// holds no character.
    ///
    /// The test is therefore the whole declared shape, and not the goal
    /// alone. Every field `SessionTask` declares must be present once, and of
    /// the type and range that field reads as.
    ///
    /// Only the DECLARED keys are counted, as with
    /// [`SessionSummary::report_is_damaged`]. An undeclared key is measured to
    /// deserialise, and its value is never decoded. A broken escape inside one
    /// therefore does not stop the whole document from reading.
    ///
    /// Two limits stand. `approval_timeout_secs` is a `u64`, and `SQLite`
    /// holds integers as `i64`. The top of that range is compared as a float,
    /// which does not separate the last few values exactly. The `workspace`
    /// and `model` fields are read only as far as the listing cuts them. Text
    /// that holds no character BEYOND that cut is therefore not seen.
    pub task_is_damaged: bool,
    /// Where this row sits in the table, which is the cursor that reads the
    /// rows BEFORE it. The listing is capped, so an old session waiting for a
    /// decision would otherwise become unreachable once enough newer ones
    /// arrive.
    ///
    /// This is the `rowid`, because `harvest_executions` carries no time and
    /// no sequence of its own. The rows are ordered by it, and a rowid is
    /// assigned in insert order, so the order is the order sessions started.
    ///
    /// A `VACUUM` may renumber a rowid, unlike the event `seq` the history
    /// cursor uses. A cursor copied before one and used after it would name
    /// another page. The window is the seconds between reading a listing and
    /// typing the next command. No column in this table is stable across a
    /// `VACUUM`, so this is the bound of what the schema allows.
    pub row: i64,
}

/// The top of the range `SessionTask::approval_timeout_secs` reads as.
///
/// The field is a `u64`, and `SQLite` holds an integer as an `i64`. A recorded
/// value above that range extracts as a float. The bound is therefore a float
/// too, so that the two compare.
///
/// The value is two to the power of 64, which a float holds exactly. It is
/// the first float ABOVE `u64::MAX`, because no float separates the two. A
/// recorded count in that gap therefore passes this bound and fails to
/// deserialise. The gap is the last few values of the range, and a float
/// cannot be made to divide it.
pub const TIMEOUT_CEILING: f64 = 18_446_744_073_709_551_616.0;

/// How many characters of one listed field are read.
///
/// A goal, an answer and an error are all written by somebody else: the
/// operator, the model, or the engine. None of them is bounded at the source.
pub const MAX_LISTED_CHARS: u32 = 500;

/// The BYTES of one listed field the database is asked for.
///
/// The cut is on bytes, because `substr` on TEXT counts to the first NUL and
/// stops. A goal of `"\u0000do it"` is a goal `submit` accepts, and the
/// listing showed nothing for it while the single status showed all of it.
///
/// Four bytes is the longest UTF-8 character, so this budget always carries
/// at least [`LISTED_READ_CHARS`] characters. The caller cuts the characters.
const MAX_LISTED_BYTES: u32 = LISTED_READ_CHARS * 4;

/// How many characters of one listed field are READ.
///
/// One character past the printed cap, so a reader can tell a field that was
/// CUT from one that ended by itself. The renderer marks the cut. This is the
/// budget [`event_lines`] reads its detail under, applied to the listing.
///
/// Without the extra character the listing showed a 4000-character answer as
/// a complete one. An operator then had no reason to open the single status.
const LISTED_READ_CHARS: u32 = MAX_LISTED_CHARS + 1;

/// The largest counter a recorded report can carry.
///
/// `SessionReport` declares both counts as `u32`, so a recorded `-1` or
/// `4294967296` is a report `status` refuses to read. Both are still
/// `integer` to `json_type` AND to `typeof`, so neither of those guards sees
/// the range, and the listing showed `[end_turn after -1 turns, ...]`.
///
/// The bound is the Rust type's own maximum, and it is PASSED to the query.
/// The SQL and the field it protects therefore cannot drift apart.
pub const COUNTER_CEILING: i64 = u32::MAX as i64;

/// Open the inspector connection.
///
/// # Errors
///
/// Returns an error if the database cannot be opened for reading.
pub fn open(db: &Path) -> Result<Connection, String> {
    let conn = Connection::open_with_flags(db, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|e| format!("cannot open {} for reading: {e}", db.display()))?;
    conn.busy_timeout(BUSY_TIMEOUT)
        .map_err(|e| format!("cannot set the busy timeout: {e}"))?;
    Ok(conn)
}

/// Is this execution id a session of the agent workflow?
///
/// A history read needs the answer. The event query of a session that does not
/// exist returns no rows, which looks the same as a session that has recorded
/// nothing yet.
///
/// # Errors
///
/// Returns an error if the query fails.
pub fn is_session(conn: &Connection, workflow_name: &str, exec_id: &str) -> Result<bool, String> {
    conn.query_row(
        "SELECT 1 FROM harvest_executions WHERE workflow_name = ?1 AND exec_id = ?2",
        [workflow_name, exec_id],
        |_| Ok(true),
    )
    .or_else(|e| match e {
        rusqlite::Error::QueryReturnedNoRows => Ok(false),
        other => Err(format!("cannot look up the session: {other}")),
    })
}

/// Every RUNNING session, oldest first, with the task it started from.
///
/// The startup check reads this, and the drive tick is seeded from it. Both
/// want the sessions that can still run, and neither wants the output of every
/// session that ever finished. A daemon must not pay for its whole history to
/// start.
///
/// # Errors
///
/// Returns an error if the query fails.
pub fn running(conn: &Connection, workflow_name: &str) -> Result<Vec<RunningSession>, String> {
    // The task is read the way the RUNTIME reads it, and not projected field
    // by field in SQL. `SQLite` and `serde_json` do not agree about what a
    // document says, so no projection can prove a row is readable.
    // `json_valid` accepts a goal of `"\uD800"` and gives it a type and a
    // length, while `serde_json` refuses the unpaired surrogate. A row the
    // projection called readable would be sealed FAILED by the runtime on
    // its first drive, where no later daemon could resume it.
    //
    // `recorded` runs the runtime's own three steps, so a row that answers
    // here deserialises on the first drive by construction.
    //
    // The document is read as BLOB bytes. A field can hold bytes that are
    // not valid UTF-8. Reading one of those as text fails the WHOLE query,
    // which would name no row at all. The bytes name their own row.
    //
    // The STORAGE CLASS is checked first, because the cast hides it. The
    // engine reads this column straight into a `String`, and `rusqlite`
    // refuses a BLOB value there. A column of TEXT affinity still keeps a
    // stored BLOB as a BLOB. A damaged row can therefore hold the right
    // bytes in the wrong class. The cast alone would accept it, and the
    // drive would fail on every tick over a session nothing ever seals.
    //
    // The cost is ONE document at a time. The rows are read as a stream, and
    // the task is cut down before the next row, so the returned set holds no
    // goal. A single control request already costs the daemon that memory.
    let mut statement = conn
        .prepare(
            "SELECT exec_id, \
                    CASE WHEN typeof(input_json) = 'text' \
                         THEN cast(input_json as blob) END, \
                    typeof(state) <> 'text', \
                    typeof(workflow_name) <> 'text' \
             FROM harvest_executions \
             WHERE cast(workflow_name as blob) = cast(?1 as blob) \
             AND cast(state as blob) = cast('RUNNING' as blob) ORDER BY rowid",
        )
        .map_err(|e| format!("cannot prepare the running-session query: {e}"))?;
    let rows = statement
        .query_map([workflow_name], |row| {
            Ok(RunningSession {
                exec_id: row.get(0)?,
                task: row
                    .get::<_, Option<Vec<u8>>>(1)?
                    .as_deref()
                    .and_then(recorded),
                state_is_damaged: row.get(2)?,
                name_is_damaged: row.get(3)?,
            })
        })
        .map_err(|e| format!("cannot read the running sessions: {e}"))?;

    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("cannot read the running sessions: {e}"))
}

/// Read one recorded task, keeping only what a startup check needs.
///
/// `None` when the document is not a task this daemon can read. The goal is
/// measured here and dropped, so it never leaves this function.
///
/// The three steps are the runtime's own, in its order. The backend reads
/// `input_json` as TEXT, parses the WHOLE document into a `Value`, and the
/// workflow takes its task from that value. The caller has already refused a
/// value of another storage class. That is the first half of the TEXT read,
/// and the UTF-8 test here is the second. Each step refuses something the
/// next one never sees. The middle step is why a fault in a field no check
/// reads still answers `None`. Parsing a document unescapes every string in
/// it, including one this daemon ignores.
fn recorded(document: &[u8]) -> Option<RecordedTask> {
    let text = std::str::from_utf8(document).ok()?;
    let whole = serde_json::from_str::<serde_json::Value>(text).ok()?;
    let task = serde_json::from_value::<SessionTask>(whole).ok()?;
    Some(RecordedTask {
        has_goal: !task.goal.trim().is_empty(),
        workspace: task.workspace,
        model: task.model,
        max_turns: task.max_turns,
        approval_timeout_secs: task.approval_timeout_secs,
    })
}

/// The cursor that reads from the newest row, as a BOUND and not a NULL.
///
/// `(?2 IS NULL OR seq < ?2)` is not an index bound. `SQLite` cannot know
/// which side of the `OR` holds while it plans. It searches by the other
/// terms and tests this one row by row. A page deep in a long history then
/// walks past every newer row to reach it, and each further page walks
/// further. Measured on 50000 events, a page 100 rows from the start took
/// 7.5ms that way and 0.07ms as a bound.
///
/// The largest possible value stands in for "no cursor". One statement then
/// serves the first page and every page after it, and both are a seek.
///
/// The bound excludes a row AT that value, which no counter this engine
/// assigns can reach. `seq` counts the events of one run, and a `rowid` is
/// an insert counter.
pub fn no_cursor(before: Option<i64>) -> i64 {
    before.unwrap_or(i64::MAX)
}

/// One page of a session's events, newest first. See [`no_cursor`].
pub const EVENTS_QUERY: &str = "SELECT seq, \
            CASE WHEN json_valid(event_json) \
                  AND json_type(event_json, '$.type') = 'text' \
                 THEN coalesce(substr(cast(json_extract(event_json, '$.type') as blob), \
                                       1, ?4), zeroblob(0)) END, \
            CASE WHEN json_valid(event_json) \
                 THEN coalesce(substr(cast(json_extract(event_json, '$.data') as blob), \
                                       1, ?4), zeroblob(0)) END \
     FROM harvest_events \
     WHERE exec_id = ?1 AND seq < ?2 \
     ORDER BY seq DESC LIMIT ?3";

/// The tool calls of one page of model replies, newest first.
///
/// The `stop_reason` test is not indexed, so it is applied to the rows the
/// bound admits, and not used to find them. See [`no_cursor`].
///
/// The calls are read as an ARRAY, and as bytes. A recorded `"tool_calls": 1`
/// extracts an integer, and reading that as text aborted the whole page. One
/// such reply then hid the call an operator was waiting to decide, while the
/// status still said the session was waiting. A call `input` can also hold
/// text Rust cannot read, and bytes let the caller drop that ONE reply.
pub const REPLIES_QUERY: &str = "SELECT seq, \
            CASE WHEN json_valid(event_json) \
                  AND json_type(event_json, '$.data.output.tool_calls') = 'array' \
                 THEN cast(json_extract(event_json, '$.data.output.tool_calls') \
                           as blob) END, \
            json_type(event_json, '$.data.output.tool_calls') \
     FROM harvest_events \
     WHERE exec_id = ?1 AND seq < ?2 \
     AND json_valid(event_json) \
     AND json_extract(event_json, '$.data.output.stop_reason') IS NOT NULL \
     ORDER BY seq DESC LIMIT ?3";

/// Is a model turn scheduled AFTER this event?
///
/// [`REPLIES_QUERY`] finds a reply by a `stop_reason` in its own payload, so a
/// reply whose payload is damaged is dropped from the page. The search then
/// answered with an OLDER reply. A tool-use id is unique inside one reply
/// only, so that offered the wrong call beside the current approval token.
///
/// This asks a question the damaged row cannot corrupt. The engine records
/// `ActivityScheduled` for a turn BEFORE the reply arrives, in a row of its
/// own. A schedule newer than the reply in hand therefore proves that reply
/// is not the newest one, whatever its payload says.
///
/// The read is bounded by `seq >`, so it looks only at rows after the reply
/// the page returned. For a parked session that is the tool events of the
/// current turn. A correlated read of activity identities would instead parse
/// every row of the log, which is the unbounded read this module avoids. See
/// [`REPLY_PAGE`] in `daemon`.
///
/// The activity name is a PARAMETER, so it comes from the registration
/// itself. A hardcoded name could drift from the one the engine records.
pub const NEWER_TURN_QUERY: &str = "SELECT count(*) FROM harvest_events \
     WHERE exec_id = ?1 AND seq > ?2 AND json_valid(event_json) \
     AND json_extract(event_json, '$.type') = 'ActivityScheduled' \
     AND json_extract(event_json, '$.data.name') = ?3";

/// How many events after `seq` this daemon cannot classify.
///
/// [`NEWER_TURN_QUERY`] must READ a row to say the row is a model turn. A row
/// it cannot classify can be a turn it cannot see, so such a row is COUNTED
/// here rather than passed over in silence.
///
/// Four shapes of damage reach that state, and a count of invalid JSON alone
/// catches one of them:
///
/// A row that is not JSON carries no readable kind at all.
///
/// A row whose `type` is absent, or is not a string, carries no kind either.
///
/// A row of kind `ActivityScheduled` with no readable `name` is a schedule
/// this daemon cannot name. The turn test above needs that name, so the row
/// evades it while remaining valid JSON.
///
/// A row that repeats `type`, `data` or `name` is read two ways. `SQLite`
/// takes the FIRST value of a repeated key, and the Rust reader takes the
/// LAST. One such row therefore reads as an ordinary tool schedule here and
/// as a model turn in the engine.
///
/// Every test sits inside a `CASE`, which fixes the order of evaluation. The
/// validity test is first, because `json_type` over a row that is not JSON
/// aborts the whole statement.
///
/// The `json_each` counts are correlated, and the `seq >` seek bounds them.
/// They read the rows after the reply in hand, which for a parked session are
/// the tool events of the current turn.
///
/// One row stays outside this count: a row whose `exec_id` is in the wrong
/// storage class. A `BLOB` never compares equal to the `TEXT` parameter, so
/// the row belongs to no execution this daemon can read. Matching by bytes
/// instead takes the read out of the `(exec_id, seq)` primary key. Both
/// evidence counts then scan the whole event log, and every `status` of every
/// parked session pays that. The bound is kept.
pub const UNCLASSIFIED_AFTER_QUERY: &str = "SELECT count(*) FROM harvest_events \
     WHERE exec_id = ?1 AND seq > ?2 \
     AND CASE \
           WHEN NOT json_valid(event_json) THEN 1 \
           WHEN json_type(event_json, '$.type') IS NOT 'text' THEN 1 \
           WHEN (SELECT count(*) FROM json_each(event_json, '$') \
                 WHERE key = 'type') <> 1 THEN 1 \
           WHEN json_extract(event_json, '$.type') <> 'ActivityScheduled' THEN 0 \
           WHEN json_type(event_json, '$.data.name') IS NOT 'text' THEN 1 \
           WHEN (SELECT count(*) FROM json_each(event_json, '$') \
                 WHERE key = 'data') <> 1 THEN 1 \
           WHEN (SELECT count(*) FROM json_each(event_json, '$.data') \
                 WHERE key = 'name') <> 1 THEN 1 \
           ELSE 0 END";

/// Count the events after `seq` that a query cannot classify or name.
///
/// # Errors
///
/// Returns an error if either count cannot be read.
pub fn newer_turn_evidence(
    conn: &Connection,
    exec_id: &str,
    seq: i64,
) -> Result<(i64, i64), String> {
    let turns = conn
        .query_row(
            NEWER_TURN_QUERY,
            rusqlite::params![exec_id, seq, session::claude_turn_info().name],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| format!("cannot count the newer turns: {e}"))?;
    let unclassified = conn
        .query_row(
            UNCLASSIFIED_AFTER_QUERY,
            rusqlite::params![exec_id, seq],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| format!("cannot count the unclassified events: {e}"))?;
    Ok((turns, unclassified))
}

/// One recorded event, already cut to what an audit line prints.
///
/// The whole event is never read. A recorded activity can approach the
/// backend's payload cap, and a page names hundreds of them. A page that read
/// them whole would hold gigabytes for one command.
#[derive(Debug)]
pub struct EventLine {
    /// The event's own position in the log, and the cursor of the next page.
    pub seq: i64,
    /// The event's type. `unknown` when the row does not hold a readable one.
    pub label: String,
    /// The event's data, cut in the database. `None` when it carries none, or
    /// when no part of it decodes.
    pub detail: Option<String>,
}

/// Read one page of a session's events, newest first, cut for printing.
///
/// `before` is the sequence number the previous page ended on, so a caller
/// walks backwards page by page. `None` starts at the newest event.
///
/// The engine owns this table. The rows are read here, never written: the
/// event log is append-only, and a reader of it must stay a reader.
///
/// # Errors
///
/// Returns an error if the query cannot run, or if a row is not readable.
pub fn event_lines(
    conn: &Connection,
    exec_id: &str,
    before: Option<i64>,
    limit: u32,
) -> Result<Vec<EventLine>, String> {
    // One character past the printed cap, so the caller can tell a cut line
    // from one that ended by itself.
    let detail_cap = MAX_EVENT_DETAIL_CHARS + 1;
    let mut statement = conn
        .prepare(EVENTS_QUERY)
        .map_err(|e| format!("cannot prepare the event query: {e}"))?;
    let rows = statement
        .query_map(
            rusqlite::params![exec_id, no_cursor(before), limit, MAX_EVENT_DETAIL_BYTES],
            |row| {
                Ok(EventLine {
                    seq: row.get(0)?,
                    // A type is a NAME, and an empty one names nothing. It
                    // is reported like a type that cannot be read.
                    label: cut_text(row.get(1)?, detail_cap, MAX_EVENT_DETAIL_BYTES)
                        .filter(|label| !label.is_empty())
                        .unwrap_or_else(|| "unknown".to_string()),
                    detail: cut_text(row.get(2)?, detail_cap, MAX_EVENT_DETAIL_BYTES),
                })
            },
        )
        .map_err(|e| format!("cannot read the events: {e}"))?;

    rows.map(|row| row.map_err(|e| format!("cannot read the events: {e}")))
        .collect()
}

/// How many characters of one event's data an audit line prints.
pub const MAX_EVENT_DETAIL_CHARS: u32 = 240;

/// The BYTES of one event field the database is asked for.
///
/// The cut is on bytes for the reason [`MAX_LISTED_BYTES`] gives: `substr` on
/// TEXT counts to the first NUL and stops. The budget carries one character
/// past the printed cap, so a caller can still tell a cut line from a whole
/// one. The caller cuts the characters.
const MAX_EVENT_DETAIL_BYTES: u32 = (MAX_EVENT_DETAIL_CHARS + 1) * 4;

/// Read one page of the TOOL CALLS a session's model replies asked for.
///
/// Newest first, and the calls alone. The database drops the tool results,
/// which is what bounds the WORK. The walk visits one row per model turn and
/// not one row per event, and `--max-turns` bounds the turns.
///
/// It also drops the transcript. A reply carries every earlier turn in its
/// content, and none of that names an awaited call. Only the calls are read,
/// which is what bounds the BYTES.
///
/// A reply has a stop reason and a tool result does not, which is how the two
/// are told apart. The engine records both as `ActivityCompleted`, and the
/// event carries no activity name.
///
/// # Errors
///
/// Returns an error if the query cannot run, or if a row is not readable.
pub fn reply_calls(
    conn: &Connection,
    exec_id: &str,
    before: Option<i64>,
    limit: u32,
) -> Result<Vec<(i64, ReplyCalls)>, String> {
    let mut statement = conn
        .prepare(REPLIES_QUERY)
        .map_err(|e| format!("cannot prepare the reply query: {e}"))?;
    let rows = statement
        .query_map(
            rusqlite::params![exec_id, no_cursor(before), limit],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<Vec<u8>>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )
        .map_err(|e| format!("cannot read the replies: {e}"))?;

    rows.map(|row| {
        row.map_err(|e| format!("cannot read the replies: {e}"))
            .map(|(seq, calls, kind)| (seq, ReplyCalls::read(calls, kind.as_deref())))
    })
    .collect()
}

/// What one model reply says about the tool calls it asked for.
///
/// The three answers are kept APART. A reply that asked for nothing and a
/// reply holding calls nobody can read are different facts. A caller that
/// treats them alike walks past the second as though it were the first. See
/// [`ReplyCalls::Unreadable`].
#[derive(Debug)]
pub enum ReplyCalls {
    /// The reply asked for no tool call. An `end_turn` reply looks like this,
    /// and so does any reply with no `tool_calls` field.
    NoCalls,
    /// The calls the reply asked for, in the order it asked for them.
    Calls(Vec<session::ToolCall>),
    /// The reply holds calls this daemon cannot read.
    ///
    /// A caller searching for ONE call cannot walk past this, because the
    /// call it wants may be here. A tool-use id is unique within one reply.
    /// Nothing makes it unique across a run, so an older reply can hold the
    /// same id for a DIFFERENT tool. Walking on would show that one.
    Unreadable,
}

impl ReplyCalls {
    /// Read one reply's calls from the projection.
    ///
    /// `kind` is the JSON type of the `tool_calls` field, which says whether
    /// the field is there at all. `bytes` carries the array when it is one.
    ///
    /// An array is read WHOLE, and never cut: a cut array is not JSON.
    fn read(bytes: Option<Vec<u8>>, kind: Option<&str>) -> Self {
        // No field at all. The reply asked for nothing.
        if kind.is_none() {
            return Self::NoCalls;
        }
        // A field that is not an array, or bytes that are not text, or an
        // array this daemon cannot read as calls. Each is the same answer.
        let Some(calls) = bytes
            .and_then(|bytes| String::from_utf8(bytes).ok())
            .and_then(|json| serde_json::from_str::<Vec<session::ToolCall>>(&json).ok())
        else {
            return Self::Unreadable;
        };
        if calls.is_empty() {
            return Self::NoCalls;
        }
        Self::Calls(calls)
    }
}

/// One session, by id.
///
/// `status` names one session, so it reads one row. The listing would select
/// and allocate the input and the output of every session that ever ran. The
/// daemon serves its commands one at a time, so that cost blocks every one.
///
/// # Errors
///
/// Returns an error if the query fails.
pub fn execution(
    conn: &Connection,
    workflow_name: &str,
    exec_id: &str,
) -> Result<Option<ExecutionRow>, String> {
    conn.query_row(
        "SELECT exec_id, state, input_json, output_json, error FROM harvest_executions \
         WHERE workflow_name = ?1 AND exec_id = ?2",
        [workflow_name, exec_id],
        |row| {
            Ok(ExecutionRow {
                exec_id: row.get(0)?,
                state: row.get(1)?,
                input_json: row.get(2)?,
                output_json: row.get(3)?,
                error: row.get(4)?,
            })
        },
    )
    .map(Some)
    .or_else(|e| match e {
        rusqlite::Error::QueryReturnedNoRows => Ok(None),
        other => Err(format!("cannot read session {exec_id}: {other}")),
    })
}

/// When the deadline of one awaited signal expires, in epoch milliseconds.
///
/// `wait_for_signal_timeout` arms a race timer named
/// `__signal_timeout:{seq}:{signal}`. The backend fires an EXPIRED timer of
/// this kind before a signal that arrives after it, so a late decision can
/// never win the race. The daemon reads the deadline for the same reason. An
/// acknowledgement after it would tell an operator that a call was approved.
/// The session is going to report that call as denied.
///
/// The name is matched the way the backend matches it, rather than with a
/// `LIKE` pattern. A signal name can hold `%` or `_`.
///
/// # Errors
///
/// Returns an error if the query fails.
pub fn signal_deadline(
    conn: &Connection,
    exec_id: &str,
    signal: &str,
) -> Result<Option<i64>, String> {
    let mut statement = conn
        .prepare("SELECT timer_id, fire_at FROM harvest_timers WHERE exec_id = ?1")
        .map_err(|e| format!("cannot prepare the deadline query: {e}"))?;
    let rows = statement
        .query_map([exec_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })
        .map_err(|e| format!("cannot read the deadlines: {e}"))?;

    for row in rows {
        let (timer_id, fire_at) = row.map_err(|e| format!("cannot read a deadline: {e}"))?;
        if races_signal(&timer_id, signal) {
            return Ok(Some(fire_at));
        }
    }
    Ok(None)
}

/// The signal one RUNNING session is still waiting for, if it waits.
///
/// A restarted daemon knows which sessions are RUNNING, and not what each one
/// awaits. That is learned from a drive, so the parked state is empty until
/// the first drive runs. An operator who approves a call in that window is
/// told the session is not waiting, on the very path the restart recipe
/// describes.
///
/// The wait is durable, so it can be read instead. A signal wait with a
/// deadline records a `__signal_timeout:` timer, which names the signal.
///
/// A decision already in hand is checked as well. A previous daemon may have
/// taken one and stopped before the drive that consumed it. The timer can
/// outlive that, so a timer ALONE would report a wait that is already over.
/// This is an approval gate, and a decision must not be accepted twice. See
/// [`answered`], which reads the staged decision and the event both.
///
/// # Errors
///
/// Returns an error if either query fails.
pub fn outstanding_signal(
    conn: &Connection,
    exec_id: &str,
    now_ms: i64,
) -> Result<Option<String>, String> {
    // `fired = 0` is the backend's own proof of a wait that is still armed.
    // An approval that timed out leaves its timer behind with `fired = 1`,
    // and a timed-out wait has no answer either. Reading every timer would
    // therefore return the EXPIRED signal of an earlier call. The status
    // would show a token nobody can approve, and hide the one that works.
    // The DEADLINE must still be ahead as well. A daemon stopped past one has
    // had no drive in which to mark the timer fired, so an overdue wait still
    // reads as armed. Restoring it would print a token that `approve` refuses
    // every time, because the session denies that call on its next drive.
    //
    // `fire_at` is an absolute epoch-millisecond, so the caller's clock is
    // read in the same unit. The clock is a parameter, so a test can place a
    // deadline on either side of it.
    let mut timers = conn
        .prepare(
            "SELECT timer_id FROM harvest_timers WHERE exec_id = ?1 \
             AND fired = 0 AND fire_at > ?2",
        )
        .map_err(|e| format!("cannot prepare the wait query: {e}"))?;
    let named = timers
        .query_map(rusqlite::params![exec_id, now_ms], |row| {
            row.get::<_, String>(0)
        })
        .map_err(|e| format!("cannot read the waits: {e}"))?;

    for timer in named {
        let timer = timer.map_err(|e| format!("cannot read a wait: {e}"))?;
        let Some(name) = signal_of(&timer) else {
            continue;
        };
        if answered(conn, exec_id, &name)? {
            continue;
        }
        return Ok(Some(name));
    }
    Ok(None)
}

/// The signal a deadline timer belongs to, if it is one.
fn signal_of(timer_id: &str) -> Option<String> {
    timer_id
        .strip_prefix("__signal_timeout:")
        .and_then(|rest| rest.split_once(':'))
        .map(|(_seq, name)| name.to_string())
}

/// Is a decision for this signal already in hand?
///
/// TWO tables answer that, and one of them alone is not enough. A decision is
/// STAGED in `harvest_signals` when it is sent, and the event is appended
/// later, when the workflow takes it up. A daemon that stopped between the
/// two holds a decision that will win on the next drive.
///
/// Reading the event alone would restore the wait over such a decision, and a
/// second answer would be taken for a call already decided.
fn answered(conn: &Connection, exec_id: &str, signal: &str) -> Result<bool, String> {
    let staged = one_row(
        conn,
        "SELECT 1 FROM harvest_signals WHERE exec_id = ?1 AND name = ?2 \
         AND delivered = 0 LIMIT 1",
        exec_id,
        signal,
    )?;
    if staged {
        return Ok(true);
    }
    one_row(
        conn,
        "SELECT 1 FROM harvest_events WHERE exec_id = ?1 \
         AND json_valid(event_json) \
         AND json_extract(event_json, '$.type') = 'SignalReceived' \
         AND json_extract(event_json, '$.data.signal_name') = ?2 LIMIT 1",
        exec_id,
        signal,
    )
}

/// Does this query find a row?
fn one_row(conn: &Connection, sql: &str, exec_id: &str, signal: &str) -> Result<bool, String> {
    let mut statement = conn
        .prepare(sql)
        .map_err(|e| format!("cannot prepare the decision query: {e}"))?;
    let mut rows = statement
        .query([exec_id, signal])
        .map_err(|e| format!("cannot read the decisions: {e}"))?;
    rows.next()
        .map(|row| row.is_some())
        .map_err(|e| format!("cannot read a decision: {e}"))
}

/// Is this timer the deadline of that signal's wait?
fn races_signal(timer_id: &str, signal: &str) -> bool {
    timer_id
        .strip_prefix("__signal_timeout:")
        .and_then(|rest| rest.split_once(':'))
        .is_some_and(|(_seq, name)| name == signal)
}

/// Decode one field the database cut to BYTES.
///
/// The cut can land inside a character, so only the valid prefix is kept. A
/// replacement character would name bytes the field does not hold, and the
/// operator would read a character nobody wrote.
///
/// The characters are cut here, because the database was asked for a budget
/// of bytes. See [`MAX_LISTED_BYTES`] and [`MAX_EVENT_DETAIL_BYTES`].
///
/// `None` means the field CANNOT BE READ, and it never means empty. An empty
/// answer is a real outcome: a session can end with the model writing no
/// text. A read that answered `None` for both would make a report with no
/// answer look like that outcome. `status` refuses such a report.
///
/// A readable field that is EMPTY arrives as zero bytes rather than as
/// nothing. `substr` answers NULL over a zero-length value, so every query
/// here wraps the cut in `coalesce(..., zeroblob(0))`. Without it `SQLite`
/// collapses an empty field into the same answer as an absent one, and no
/// test in Rust could tell them apart.
///
/// `budget` is the byte budget the query was given. It separates the two
/// reasons the bytes can end inside a character: the database CUT them there,
/// or nobody ever wrote a whole one.
fn cut_text(bytes: Option<Vec<u8>>, chars: u32, budget: u32) -> Option<String> {
    let bytes = bytes?;
    let whole = match std::str::from_utf8(&bytes) {
        Ok(text) => text,
        // `error_len` answers NOTHING only when the bytes END inside a
        // character. That is the cut the database was asked to make, so the
        // valid prefix is the field. Any other error is a sequence nobody
        // wrote, and a prefix of it would name a value the field never held.
        // A stop reason of `"end_turn\ud800"` decodes to a valid `end_turn`,
        // and the listing would show a session that ended well.
        //
        // The budget is what says a cut happened. Bytes SHORTER than it were
        // returned whole, so an unfinished character in them is damage.
        Err(split) if split.error_len().is_none() && bytes.len() >= budget as usize => {
            std::str::from_utf8(&bytes[..split.valid_up_to()]).unwrap_or_default()
        }
        Err(_) => return None,
    };
    Some(whole.chars().take(chars as usize).collect())
}

/// One page of the sessions a listing names, newest first. See [`no_cursor`].
pub const SESSIONS_QUERY: &str = "SELECT \
                    CASE WHEN typeof(exec_id) = 'text' \
                         THEN coalesce(substr(cast(exec_id as blob), 1, ?3), \
                                       zeroblob(0)) END, \
                    CASE WHEN typeof(state) = 'text' \
                         THEN coalesce(substr(cast(state as blob), 1, ?3), \
                                       zeroblob(0)) END, \
                    CASE WHEN typeof(input_json) = 'text' AND json_valid(input_json) \
                          AND json_type(input_json, '$.goal') = 'text' \
                         THEN coalesce(substr(cast(json_extract(input_json, '$.goal') \
                                                   as blob), 1, ?3), zeroblob(0)) END, \
                    CASE WHEN typeof(output_json) = 'text' AND json_valid(output_json) \
                          AND json_type(output_json, '$.stop') = 'text' \
                         THEN coalesce(substr(cast(json_extract(output_json, '$.stop') \
                                                   as blob), 1, ?3), zeroblob(0)) END, \
                    CASE WHEN typeof(output_json) = 'text' AND json_valid(output_json) \
                          AND json_type(output_json, '$.turns') = 'integer' \
                          AND typeof(json_extract(output_json, '$.turns')) = 'integer' \
                          AND json_extract(output_json, '$.turns') BETWEEN 0 AND ?5 \
                         THEN json_extract(output_json, '$.turns') END, \
                    CASE WHEN typeof(output_json) = 'text' AND json_valid(output_json) \
                          AND json_type(output_json, '$.tool_calls') = 'integer' \
                          AND typeof(json_extract(output_json, '$.tool_calls')) \
                              = 'integer' \
                          AND json_extract(output_json, '$.tool_calls') \
                              BETWEEN 0 AND ?5 \
                         THEN json_extract(output_json, '$.tool_calls') END, \
                    CASE WHEN typeof(output_json) = 'text' AND json_valid(output_json) \
                          AND json_type(output_json, '$.answer') = 'text' \
                         THEN coalesce(substr(cast(json_extract(output_json, '$.answer') \
                                                   as blob), 1, ?3), zeroblob(0)) END, \
                    CASE WHEN typeof(error) = 'text' \
                         THEN coalesce(substr(cast(error as blob), 1, ?3), \
                                       zeroblob(0)) END, \
                    error IS NOT NULL AND typeof(error) <> 'text', \
                    CASE WHEN typeof(output_json) = 'text' AND json_valid(output_json) \
                         THEN (SELECT count(*) FROM json_each(output_json) \
                               WHERE key IN ('answer', 'turns', 'tool_calls', 'stop')) \
                              > 4 \
                         ELSE 0 END, \
                    CASE WHEN typeof(input_json) = 'text' \
                         THEN CASE WHEN json_valid(input_json) \
                                   THEN NOT coalesce(( \
                                        json_type(input_json, '$.goal') = 'text' \
                                    AND json_type(input_json, '$.workspace') = 'text' \
                                    AND json_type(input_json, '$.model') = 'text' \
                                    AND json_type(input_json, '$.max_turns') = 'integer' \
                                    AND json_extract(input_json, '$.max_turns') \
                                        BETWEEN 0 AND ?6 \
                                    AND json_type(input_json, \
                                                  '$.approval_timeout_secs') = 'integer' \
                                    AND json_extract(input_json, \
                                                     '$.approval_timeout_secs') \
                                        BETWEEN 0 AND ?7 \
                                    AND (SELECT count(*) FROM json_each(input_json) \
                                         WHERE key IN ('goal', 'max_turns', \
                                                       'approval_timeout_secs', \
                                                       'workspace', 'model')) = 5), 0) \
                                   ELSE 1 END \
                         ELSE 1 END, \
                    CASE WHEN typeof(input_json) = 'text' AND json_valid(input_json) \
                          AND json_type(input_json, '$.workspace') = 'text' \
                         THEN coalesce(substr(cast(json_extract(input_json, '$.workspace') \
                                                   as blob), 1, ?3), zeroblob(0)) END, \
                    CASE WHEN typeof(input_json) = 'text' AND json_valid(input_json) \
                          AND json_type(input_json, '$.model') = 'text' \
                         THEN coalesce(substr(cast(json_extract(input_json, '$.model') \
                                                   as blob), 1, ?3), zeroblob(0)) END, \
                    rowid \
             FROM harvest_executions WHERE +workflow_name = ?1 \
             AND rowid < ?4 \
             ORDER BY rowid DESC LIMIT ?2";

/// One page of the agent workflow's executions, oldest first.
///
/// `before` reads the page before a row this listing named. The cap is on one
/// page and not on the table, so every session stays reachable.
///
/// # Errors
///
/// Returns an error if the query fails.
pub fn executions(
    conn: &Connection,
    workflow_name: &str,
    before: Option<i64>,
) -> Result<Vec<SessionSummary>, String> {
    // The rows are walked by ROWID, and the workflow name is tested against
    // each one. The `+` is what asks for that: it takes the name out of the
    // planner's index choice.
    //
    // The index on the name cannot answer `ORDER BY rowid`, so a lookup
    // through it sorts every matching row in a temporary B-tree before the
    // LIMIT applies. The cap then bounds what comes BACK and not what is
    // read, which is the opposite of what this query is for. Walking the
    // rowid index backwards is already the order the listing wants, so it
    // stops at the cap. Measured on 20000 sessions, the first page took
    // 11.7ms through the index and 0.1ms by this walk.
    //
    // The trade is real, and the other way in one case. A file where this
    // workflow is a small minority costs a walk past the rest: 2.9ms against
    // 0.09ms at 50 rows in 20000. This daemon owns its file and starts one
    // workflow in it, so the majority case is the only one it has.
    //
    // The FIELDS are selected, and not the rows. A recorded task and a
    // recorded report can each approach the backend's payload cap. A listing
    // that read them whole would hold hundreds of megabytes for a capped
    // number of sessions. It would then copy that to build the views, and
    // once more to serialise the answer. Each field is cut in the database,
    // where the bytes already are.
    //
    // `json_valid` guards every extraction. `json_extract` on a document
    // that is not JSON raises `malformed JSON`, and that aborts the WHOLE
    // statement. One damaged row would hide every session in the file,
    // including one waiting for a decision. The guard leaves that row's
    // fields NULL, which the caller already shows as an unreadable task.
    //
    // The TYPE is guarded as well, because valid JSON can still say the
    // wrong thing. A report of `{"stop":1}` is valid, and `json_extract`
    // returns the integer 1, which fails to read as the text this row
    // expects. That failure aborts the whole statement exactly as a damaged
    // document does.
    //
    // `typeof` guards the two integers beside `json_type`, and the two catch
    // different faults. A number too large for a signed 64-bit integer is
    // still an `integer` to `json_type`, while `json_extract` returns a real.
    //
    // Every TEXT field is read as bytes, and none of them as a string. A JSON
    // string can hold an unpaired surrogate, which `SQLite` accepts and calls
    // text. `json_extract` then yields bytes that are not UTF-8, and reading
    // those into a `String` fails the whole statement. The bytes are decoded
    // by the caller, which keeps what is readable.
    let mut statement = conn
        .prepare(SESSIONS_QUERY)
        .map_err(|e| format!("cannot prepare the session query: {e}"))?;
    let rows = statement
        .query_map(
            rusqlite::params![
                workflow_name,
                MAX_LISTED_SESSIONS + 1,
                MAX_LISTED_BYTES,
                no_cursor(before),
                COUNTER_CEILING,
                i64::from(u32::MAX),
                TIMEOUT_CEILING
            ],
            |row| {
                Ok(SessionSummary {
                    exec_id: cut_text(row.get(0)?, LISTED_READ_CHARS, MAX_LISTED_BYTES),
                    state: cut_text(row.get(1)?, LISTED_READ_CHARS, MAX_LISTED_BYTES),
                    goal: cut_text(row.get(2)?, LISTED_READ_CHARS, MAX_LISTED_BYTES),
                    stop: cut_text(row.get(3)?, LISTED_READ_CHARS, MAX_LISTED_BYTES),
                    turns: row.get(4)?,
                    tool_calls: row.get(5)?,
                    answer: cut_text(row.get(6)?, LISTED_READ_CHARS, MAX_LISTED_BYTES),
                    error: cut_text(row.get(7)?, LISTED_READ_CHARS, MAX_LISTED_BYTES),
                    error_is_damaged: row.get(8)?,
                    report_is_damaged: row.get(9)?,
                    // The query decides the shape of the document. The two
                    // short text fields beside it are decoded HERE. Only Rust
                    // reads the bytes as characters, and a field that holds
                    // none is a field `status` refuses.
                    task_is_damaged: row.get::<_, bool>(10)?
                        || cut_text(row.get(11)?, LISTED_READ_CHARS, MAX_LISTED_BYTES).is_none()
                        || cut_text(row.get(12)?, LISTED_READ_CHARS, MAX_LISTED_BYTES).is_none(),
                    row: row.get(13)?,
                })
            },
        )
        .map_err(|e| format!("cannot read the sessions: {e}"))?;

    let mut listed = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("cannot read the sessions: {e}"))?;
    // The newest are read first, so the listing reads oldest first again.
    listed.reverse();
    Ok(listed)
}
