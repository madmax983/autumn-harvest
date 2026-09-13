//! The local toolbox one agent session can act with.
//!
//! Three tools, all confined to one workspace directory: list a directory,
//! read a file, and write a file. A write changes the machine, so it is the
//! one tool the workflow gates on human approval.
//!
//! Every path is relative to the workspace root. [`resolve`] rejects an
//! absolute path and any path that leaves the root, so a model cannot reach
//! the rest of the disk.

use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};

use serde_json::{Value, json};

use autumn_harvest::failure::{ActivityFailure, IntoActivityErrorString};

use crate::session::{ToolCall, ToolOutcome, ToolRequest};

/// List the entries of one directory.
pub const TOOL_LIST_FILES: &str = "list_files";
/// Read one text file.
pub const TOOL_READ_FILE: &str = "read_file";
/// Write one text file. This tool needs approval.
pub const TOOL_WRITE_FILE: &str = "write_file";

/// The largest file this toolbox reads or writes.
const MAX_FILE_BYTES: usize = 64 * 1024;
/// The largest directory listing this toolbox returns.
pub const MAX_ENTRIES: usize = 200;

/// How many scratch names one write tries before it gives up.
const SCRATCH_ATTEMPTS: u32 = 16;

/// The longest scratch basename this module builds.
///
/// A file name component is limited by the filesystem, and 255 bytes is the
/// common limit. A 255-byte target is therefore legal, and a scratch name that
/// copies it whole is not. All 16 attempts then fail with `ENAMETOOLONG`, and
/// an approved write becomes impossible.
const SCRATCH_CAP: usize = 255;

/// The scratch length a SHORT target still allows.
///
/// The cap above assumes the common limit. This floor removes the assumption
/// for a long target. The scratch name never exceeds the target's own name
/// once that name is longer than this floor. The target name is itself proof
/// that the length is allowed. An ordinary short name keeps its whole stem in
/// the scratch name, which is what makes a leftover file identifiable.
const SCRATCH_FLOOR: usize = 96;

/// The `setuid` and `setgid` bits, which a written file never keeps.
const SET_ID_BITS: u32 = 0o6000;

/// The mode a file this toolbox CREATES is given.
///
/// A file the agent brings into being starts private. An existing file keeps
/// its own mode instead, because a content change is not a permission change.
const NEW_FILE_MODE: u32 = 0o600;

/// Does a call to this tool wait for human approval?
pub fn needs_approval(tool_name: &str) -> bool {
    tool_name == TOOL_WRITE_FILE
}

/// The `tools` array of the Messages API request.
pub fn definitions() -> Value {
    json!([
        {
            "name": TOOL_LIST_FILES,
            "description": "List the files and directories under a path in the workspace. \
                            Use \".\" for the workspace root. A long listing names the \
                            `after` value that reads the next page.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Directory path, relative to the workspace root." },
                    "after": {
                        "type": ["string", "null"],
                        "description": "Continue after this entry, copied from a previous listing. \
                                        Null for the first page."
                    }
                },
                "required": ["path", "after"],
                "additionalProperties": false
            },
            "strict": true
        },
        {
            "name": TOOL_READ_FILE,
            "description": "Read one UTF-8 text file from the workspace.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "File path, relative to the workspace root." }
                },
                "required": ["path"],
                "additionalProperties": false
            },
            "strict": true
        },
        {
            "name": TOOL_WRITE_FILE,
            "description": "Write one UTF-8 text file in the workspace, replacing any previous \
                            content. A human approves each call before it runs.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "File path, relative to the workspace root." },
                    "content": { "type": "string", "description": "The complete new file content." }
                },
                "required": ["path", "content"],
                "additionalProperties": false
            },
            "strict": true
        }
    ])
}

/// Build the synchronous activity body the runtime registers for `run_tool`.
///
/// The daemon's workspace root is captured here. The call carries the workspace
/// its session was started in, and the two must be the same one. A daemon
/// restarted on another directory therefore refuses the call instead of running
/// an already-approved write against the wrong project. The refusal is
/// non-retryable, so the session fails loudly and the operator can restart the
/// daemon where the session belongs.
pub fn activity_body(
    workspace: PathBuf,
) -> impl Fn(Value) -> Result<Value, String> + Send + Sync + 'static {
    move |input| {
        let request: ToolRequest =
            serde_json::from_value(input).map_err(|e| format!("malformed tool call: {e}"))?;
        if !serves(&workspace, &request.workspace) {
            return Err(ActivityFailure::non_retryable(
                "WorkspaceMismatch",
                format!(
                    "this session belongs to the workspace `{}`, and this daemon serves `{}`",
                    request.workspace,
                    workspace.display()
                ),
            )
            .into_error_payload());
        }
        let outcome = dispatch(&workspace, &request.call);
        serde_json::to_value(outcome).map_err(|e| format!("tool result is not JSON: {e}"))
    }
}

/// Does this daemon serve the workspace the session was started in?
///
/// The comparison is between resolved paths, so a different spelling of one
/// directory still matches.
fn serves(root: &Path, recorded: &str) -> bool {
    root.canonicalize()
        .is_ok_and(|real| real == Path::new(recorded))
}

/// Run one tool call.
///
/// A tool failure is a `ToolOutcome` with `is_error`, never an activity error.
/// The model reads the message and picks its next step, which is how a real
/// harness recovers from a bad path or a missing file.
fn dispatch(workspace: &Path, call: &ToolCall) -> ToolOutcome {
    let reads = |result: Result<String, String>| result.map_err(Failure::from);
    let result = match call.name.as_str() {
        TOOL_LIST_FILES => reads(string_arg(&call.input, "path").and_then(|p| {
            // Absent, null or blank all mean the first page. A strict schema
            // makes the field required, so the model sends null for it.
            let after = call
                .input
                .get("after")
                .and_then(serde_json::Value::as_str)
                .filter(|cursor| !cursor.is_empty());
            list_files(workspace, &p, after)
        })),
        TOOL_READ_FILE => {
            reads(string_arg(&call.input, "path").and_then(|p| read_file(workspace, &p)))
        }
        TOOL_WRITE_FILE => (|| {
            let path = string_arg(&call.input, "path")?;
            let content = string_arg(&call.input, "content")?;
            write_file(workspace, &path, &content)
        })(),
        other => Err(Failure::from(format!("unknown tool `{other}`"))),
    };

    match result {
        Ok(output) => ToolOutcome {
            output,
            is_error: false,
        },
        Err(failure) => ToolOutcome::error(failure.message),
    }
}

/// The words a post-rename failure says about the target.
///
/// The write landed and only the flush failed, so the file DOES hold the new
/// bytes. A reader must tell that apart from a write that never happened. The
/// only channel to a model is this text.
///
/// The producer and every reader share this one constant, so the two cannot
/// drift apart. It is deliberately not a field on the result. Such a field
/// would travel into the API `tool_result` block, and the Messages API
/// defines no such property there.
pub const LANDED_UNFLUSHED: &str =
    "now holds the new bytes, and the change is not flushed to the disk";

/// One failed tool call.
pub struct Failure {
    message: String,
}

impl From<String> for Failure {
    fn from(message: String) -> Self {
        Self { message }
    }
}

/// Read one required string argument out of a tool input.
fn string_arg(input: &Value, key: &str) -> Result<String, String> {
    input
        .get(key)
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| format!("the `{key}` argument is missing or is not a string"))
}

/// Resolve a caller path against the workspace root, and refuse a target that
/// leaves it.
///
/// The check has three parts, because a lexical rule alone is not enough. A
/// symbolic link inside the workspace redirects a read or a write after the
/// path has already passed a lexical test.
///
/// 1. **Lexical.** An absolute path, a parent traversal, and a root prefix are
///    rejected.
/// 2. **The final component.** A symbolic link there is refused outright, even
///    one that points inside the workspace. A dangling link reports
///    `exists() == false`, so the link itself is tested, not its target.
/// 3. **The path above it.** The deepest EXISTING ancestor is resolved through
///    every symbolic link and must sit under the real workspace root. That
///    catches a link in the middle of the path, and it works for a write target
///    that does not exist yet.
fn resolve(workspace: &Path, relative: &str) -> Result<PathBuf, String> {
    // The root itself can sit behind a link, so compare against its real path.
    let root = workspace
        .canonicalize()
        .map_err(|e| format!("cannot resolve the workspace: {e}"))?;

    let candidate = Path::new(relative);
    if candidate.is_absolute() {
        return Err(format!(
            "`{relative}` is absolute; use a workspace-relative path"
        ));
    }
    for component in candidate.components() {
        match component {
            Component::Normal(_) | Component::CurDir => {}
            _ => return Err(format!("`{relative}` leaves the workspace")),
        }
    }
    let path = root.join(candidate);

    if std::fs::symlink_metadata(&path).is_ok_and(|meta| meta.file_type().is_symlink()) {
        return Err(format!(
            "`{relative}` is a symbolic link, and the toolbox refuses one"
        ));
    }

    let mut probe = path.as_path();
    let resolved = loop {
        if let Ok(real) = probe.canonicalize() {
            break real;
        }
        probe = probe
            .parent()
            .ok_or_else(|| format!("cannot resolve `{relative}`"))?;
    };
    if !resolved.starts_with(&root) {
        return Err(format!("`{relative}` leaves the workspace"));
    }

    Ok(path)
}

/// List one directory, in order, with a trailing slash on each subdirectory.
///
/// `after` continues a listing from the last name it showed. Without one, a
/// cap hides entries FOREVER. `read_dir` gives no order, so a truncated read
/// returns some arbitrary subset, and the same call returns that same subset
/// again. Everything outside it is then unreachable to the model.
///
/// The selection is bounded rather than the read. Every name is looked at,
/// and only the smallest `MAX_ENTRIES` after the cursor are held, so memory
/// is the page and not the directory. A million entries are walked without a
/// million names in hand, and each one can be reached by asking again.
///
/// The key is the name AS PRINTED, so the cursor a caller passes back is
/// exactly a line it read.
///
/// A name that is not text is COUNTED and not listed. A filename is bytes on
/// this platform, and the model can only send a string. Such an entry cannot
/// be named through this tool at all.
///
/// Rendering it with the replacement character would cost twice over. The
/// name would address no file. Two entries differing only in those bytes
/// would also collapse into one, and the second would vanish from the walk.
/// The count says they are there.
///
/// The last line is always the COUNT of the names above it. An entry can
/// hold any text, so no line of names can be reserved for an answer about
/// the listing. The count is appended after the names, where none can be.
///
/// A name holding the LINE BREAK this listing is joined with is counted the
/// same way. One entry named `a\nb` would render as the two lines `a` and
/// `b`, which is what a directory of `a` and `b` renders as. Neither line
/// names a file. The cursor of the next page is a line of this listing. One
/// of those two lines would page the walk onto a name that is not there.
fn list_files(workspace: &Path, relative: &str, after: Option<&str>) -> Result<String, String> {
    let dir = resolve(workspace, relative)?;
    // The directory is OPENED, proved, and then read from that DESCRIPTOR.
    // `read_dir` takes a path, so it resolves the name a second time, and a
    // directory swapped in between would be listed instead. The model reads
    // this listing, so those names would leave the workspace. See
    // [`opened_inside`].
    let handle = open_directory(&dir, relative)?;
    opened_inside(&handle, &dir, workspace)
        .map_err(|e| format!("cannot list `{relative}`: {e}"))?;
    let listing = rustix::fs::Dir::read_from(&handle)
        .map_err(|e| format!("cannot list `{relative}`: {e}"))?;

    let mut page: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut more = false;
    let mut unnamed = 0_usize;
    let mut split = 0_usize;
    for entry in listing {
        let entry = entry.map_err(|e| format!("cannot list `{relative}`: {e}"))?;
        let raw = entry.file_name();
        // A directory read from a descriptor carries its own two links. They
        // are not entries of it, and `read_dir` never showed them.
        if raw.to_bytes() == b"." || raw.to_bytes() == b".." {
            continue;
        }
        let Ok(name) = std::str::from_utf8(raw.to_bytes()) else {
            unnamed += 1;
            continue;
        };
        // The delimiter of the listing cannot appear inside an entry of it.
        // Such a name is counted, exactly as one that is not text is.
        if name.contains('\n') {
            split += 1;
            continue;
        }
        let is_dir = entry.file_type().is_dir();
        let shown = if is_dir {
            format!("{name}/")
        } else {
            name.to_string()
        };
        if after.is_some_and(|cursor| shown.as_str() <= cursor) {
            continue;
        }
        page.insert(shown);
        if page.len() > MAX_ENTRIES {
            // The largest falls out, so what stays is the smallest page.
            page.pop_last();
            more = true;
        }
    }

    let named = page.len();
    let mut entries: Vec<String> = page.into_iter().collect();
    if more {
        let last = entries.last().cloned().unwrap_or_default();
        entries.push(format!(
            "... more entries; read the next {MAX_ENTRIES} with `after` set to \"{last}\""
        ));
    }
    if unnamed > 0 {
        entries.push(format!(
            "... {unnamed} entries are not listed: their names are not text, so this \
             tool cannot name them"
        ));
    }
    if split > 0 {
        entries.push(format!(
            "... {split} entries are not listed: their names hold a line break, so a \
             listing of one name per line cannot name them"
        ));
    }
    // The LAST line is always this tool's own count, and it is appended after
    // every name. No entry can take its place.
    //
    // An empty directory used to answer `(no entries)`, which is a legal
    // filename. A directory holding only that one file read exactly like an
    // empty one. The model could not tell whether the file was there, and
    // could never reach it. The count tells the two apart: one names the file
    // and counts one, the other names nothing and counts none.
    entries.push(format!("... entries named: {named}"));
    Ok(entries.join("\n"))
}

/// Read one text file, up to the size cap.
///
/// The cap is applied BEFORE the file is allocated. A plain read of a
/// multi-gigabyte file would exhaust the daemon and stop every session. So the
/// size is checked first, and the read itself is bounded. The second bound
/// matters because the file can grow between the two steps.
fn read_file(workspace: &Path, relative: &str) -> Result<String, String> {
    let path = resolve(workspace, relative)?;
    let file = open_regular(&path, relative)?;
    // The open above proved the final component is no link. The levels ABOVE
    // it are proved here, against the descriptor. See [`opened_inside`].
    opened_inside(&file, &path, workspace).map_err(|e| format!("cannot read `{relative}`: {e}"))?;

    // The size comes from the OPEN descriptor, so it describes the file that
    // was opened rather than whatever the path named a moment earlier.
    let size = file
        .metadata()
        .map_err(|e| format!("cannot read `{relative}`: {e}"))?
        .len();
    if size > MAX_FILE_BYTES as u64 {
        return Err(format!(
            "`{relative}` is {size} bytes; the limit is {MAX_FILE_BYTES}"
        ));
    }

    let mut bytes = Vec::new();
    file.take(MAX_FILE_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| format!("cannot read `{relative}`: {e}"))?;
    if bytes.len() > MAX_FILE_BYTES {
        return Err(format!(
            "`{relative}` grew past the {MAX_FILE_BYTES} byte limit while it was read"
        ));
    }

    String::from_utf8(bytes).map_err(|_| format!("`{relative}` is not UTF-8 text"))
}

/// Open a directory for listing, refusing a link at the final component.
///
/// `O_DIRECTORY` fails the open when the name is not a directory, so the kind
/// is decided by the OPEN and cannot be swapped after it. `O_NOFOLLOW` does
/// the same job here that it does for a file.
fn open_directory(path: &Path, relative: &str) -> Result<std::fs::File, String> {
    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc_o_nofollow() | libc_o_directory())
        .open(path)
        .map_err(|e| format!("cannot list `{relative}`: {e}"))
}

/// `O_DIRECTORY`, from the platform's own headers.
const fn libc_o_directory() -> i32 {
    rustix::fs::OFlags::DIRECTORY.bits().cast_signed()
}

/// Open a path for reading, and prove it is an ordinary file.
///
/// Two flags carry the safety here. `O_NOFOLLOW` refuses a symbolic link at the
/// final component, even one that appears between the check and this open.
/// `O_NONBLOCK` stops a FIFO from blocking the open itself. A named pipe with
/// no writer would otherwise hang this body, and with it the whole daemon. One
/// runtime serves every session and every command.
///
/// The file type is then read from the descriptor, so the answer describes what
/// was opened and cannot be swapped afterwards.
fn open_regular(path: &Path, relative: &str) -> Result<std::fs::File, String> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc_o_nofollow() | libc_o_nonblock())
        .open(path)
        .map_err(|e| format!("cannot read `{relative}`: {e}"))?;

    let kind = file
        .metadata()
        .map_err(|e| format!("cannot read `{relative}`: {e}"))?
        .file_type();
    if !kind.is_file() {
        return Err(format!("`{relative}` is not an ordinary file"));
    }
    Ok(file)
}

/// Prove an OPEN descriptor holds the file at a contained path.
///
/// `O_NOFOLLOW` refuses a link at the FINAL component only. An intermediate
/// component replaced between [`resolve`] and the open sends the open THROUGH
/// it, and the descriptor then holds a file outside the workspace. Measured:
/// a `read_file` of `a/passwd` returned the contents of an outside file.
///
/// A descriptor pins the file it opened, so the proof can come after the
/// open. The path must still resolve under the real root, and the file there
/// must be the SAME file: one device and one inode.
///
/// What the descriptor gives is that the bytes read AFTERWARDS belong to the
/// file that was proved. A swap after the proof cannot redirect the read.
///
/// This NARROWS the window. It does not close it. A single swap is refused,
/// because the path no longer resolves inside the workspace, or the file
/// there is another file. A caller that can time THREE swaps still wins: the
/// containment and the identity are two separate resolutions of the same
/// name. It can point a parent outside for the open. It can restore that
/// parent while the path is canonicalised, and point it outside again before
/// the identity is read. Both reads then describe the same outside file.
///
/// Closing that needs every component opened relative to a held descriptor,
/// which is the `openat` design this example does not carry. The write path
/// stands further back again: a rename acts on a name, and not on a
/// descriptor. See [`contained`].
///
/// A file hard-linked into the workspace passes, because it IS in the
/// workspace. The model could name it directly.
///
/// # Errors
///
/// Returns an error if either path cannot be resolved, if the path leaves the
/// workspace, or if the descriptor holds another file.
pub fn opened_inside(file: &std::fs::File, path: &Path, workspace: &Path) -> Result<(), String> {
    contained(path, workspace)?;
    let opened = file
        .metadata()
        .map_err(|e| format!("cannot stat the open file: {e}"))?;
    let named = std::fs::metadata(path).map_err(|e| format!("cannot stat the path: {e}"))?;
    if opened.dev() != named.dev() || opened.ino() != named.ino() {
        return Err(
            "the file it opened is no longer the file that path names: another process \
             replaced a directory above it"
                .to_string(),
        );
    }
    Ok(())
}

/// `O_NOFOLLOW`, from the platform's own headers.
const fn libc_o_nofollow() -> i32 {
    rustix::fs::OFlags::NOFOLLOW.bits().cast_signed()
}

/// `O_NONBLOCK`, from the platform's own headers.
const fn libc_o_nonblock() -> i32 {
    rustix::fs::OFlags::NONBLOCK.bits().cast_signed()
}

/// Write one text file, creating the parent directories.
///
/// The body is idempotent: the same call writes the same bytes. That matters
/// because activity execution is at-least-once. A crash between the write and
/// its commit re-runs this body on resume.
fn write_file(workspace: &Path, relative: &str, content: &str) -> Result<String, Failure> {
    if content.len() > MAX_FILE_BYTES {
        return Err(Failure::from(format!(
            "the content is {} bytes; the limit is {MAX_FILE_BYTES}",
            content.len()
        )));
    }
    let path = resolve(workspace, relative)?;

    // An existing target must be an ordinary file. A FIFO would block this
    // body, and with it the whole daemon, and a device is not something a tool
    // call should write through.
    if std::fs::symlink_metadata(&path).is_ok_and(|existing| !existing.file_type().is_file()) {
        return Err(Failure::from(format!(
            "`{relative}` is not an ordinary file"
        )));
    }

    if let Some(parent) = path.parent() {
        create_enterable(parent)
            .map_err(|e| format!("cannot create the parent of `{relative}`: {e}"))?;
        // The levels above were created just now, so the containment
        // `resolve` proved is re-proved here. See [`contained`].
        contained(parent, workspace).map_err(|e| format!("cannot write `{relative}`: {e}"))?;
    }
    // Every directory between the target and the workspace root can hold an
    // entry this write created, so the whole chain is flushed after the
    // rename. See [`directories_to_flush`].
    let flush = directories_to_flush(&path, workspace);

    // Write through a temporary file beside the target, then rename over it. A
    // write that fails part way, on a full disk or a quota, would otherwise
    // leave the approved file truncated. The tool reports that as a result
    // rather than an activity error, so nothing would retry it. The rename is
    // atomic inside one directory, so the target holds the whole content or it
    // is untouched.
    // The rename replaces the target's inode, so the scratch file carries the
    // mode the result must have. An existing target keeps its own mode. The
    // operator approved a change of content. Making a private file
    // world-readable, or dropping a script's execute bits, is not that.
    //
    // The set-ID bits are the exception, and they are dropped. The new inode
    // belongs to the daemon, and the model chose every byte in it. A `setuid`
    // file here would run as the daemon's user for anyone who could execute
    // it. The approval shows the path and the content, and never the mode, so
    // an operator cannot see that they approved such a thing.
    let mode = std::fs::metadata(&path).map_or(NEW_FILE_MODE, |existing| {
        existing.permissions().mode() & 0o7777 & !SET_ID_BITS
    });

    let (temporary, file) = create_scratch(&path)?;
    match write_through(file, &temporary, &path, content, mode, &flush) {
        Ok(()) => Ok(format!("wrote {} bytes to `{relative}`", content.len())),
        Err(WriteFailure::BeforeRename(e)) => {
            // Nothing replaced the target, so the scratch file is litter.
            drop(std::fs::remove_file(&temporary));
            Err(Failure::from(format!("cannot write `{relative}`: {e}")))
        }
        // The target IS replaced. Reporting that nothing was written would be
        // false, and the model could undo work that landed. What failed is the
        // durability of the change, not the change. The flag says so to every
        // reader of the result, and not only to one that reads the message.
        Err(WriteFailure::AfterRename(e)) => Err(Failure::from(format!(
            "`{relative}` {LANDED_UNFLUSHED} yet: {e}. A host crash could \
             still lose it.",
        ))),
    }
}

/// The directories a finished write must flush, deepest first.
///
/// The chain runs from the target's own directory up to the workspace root.
/// Each one can hold an entry this write created, and an entry is durable only
/// after the directory that names it is flushed.
///
/// The chain is walked, rather than collected while the directories are
/// created. Activity execution is at-least-once. A crash after a directory is
/// created, but before its entry is flushed, leaves that directory in place.
/// The retry then creates nothing. A list of its own creations would name
/// nothing to flush, while the entry naming the directory is still unwritten.
pub fn directories_to_flush(target: &Path, workspace: &Path) -> Vec<PathBuf> {
    // BOTH sides are canonical, or neither comparison means anything. The
    // workspace may be named through a symlink. The root would then be the
    // real path while the target kept the link spelling, and the first
    // ancestor would already fail the test below. The chain would be empty,
    // and the fallback would flush the target's own directory alone. Every
    // directory above it holds an entry this write created, so a crash could
    // lose the file while the history says the write finished.
    //
    // The directories exist by the time this runs, because the write has
    // landed. Flushing the canonical directory flushes the same inode as the
    // link spelling names.
    let real = |path: &Path| path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    let root = real(workspace);
    let chain: Vec<PathBuf> = target
        .parent()
        .map(real)
        .into_iter()
        .flat_map(|leaf| {
            leaf.ancestors()
                .take_while(|directory| directory.starts_with(&root))
                .map(Path::to_path_buf)
                .collect::<Vec<_>>()
        })
        .collect();

    // The target sits under the root, so the chain holds the root at least. A
    // spelling that did not compare would otherwise flush nothing at all. The
    // directory entry of the target is the one that must not be lost.
    if chain.is_empty() {
        return target.parent().map(Path::to_path_buf).into_iter().collect();
    }
    chain
}

/// Create a directory and its missing parents, each one the owner can enter.
///
/// `create_dir_all` asks for mode `0777`, and the umask decides what survives.
/// A umask that masks the owner bits therefore gives a new directory mode
/// `000`. The daemon cannot enter its own directory after that. The next level
/// down fails, and so does the scratch file of the write. An approved write
/// would need the operator to repair the permissions by hand.
///
/// Only a directory this call creates is adjusted, and only by adding the
/// owner bits. A directory the operator already made narrow keeps the mode
/// they chose.
///
/// The mode is widened after creation, rather than through the umask. The
/// umask is one value for the whole process, and this daemon creates its
/// private files on other threads.
pub fn create_enterable(directory: &Path) -> std::io::Result<()> {
    // The deepest existing level decides. Every level above it has a child.
    // Every level above it is therefore a directory this daemon can enter,
    // which the stat that found the deepest one proves.
    if let Some(deepest) = directory.ancestors().find(|level| level.exists()) {
        // A regular file named as the workspace has nothing missing above it.
        // Nothing would be created, and the daemon would start over a
        // workspace no tool can use.
        if !deepest.is_dir() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotADirectory,
                format!("{} exists and is not a directory", deepest.display()),
            ));
        }
        // A directory that exists and cannot be entered is REFUSED, and not
        // repaired. This call cannot prove it created that directory. An
        // operator can lock one deliberately, and a daemon killed between the
        // creation and the mode leaves the same thing. The two are identical
        // on disk, so widening it would undo a choice that may have been
        // meant. The refusal names the path, which a silent stall did not.
        if !owner_can_enter(deepest) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!(
                    "{} exists and its owner cannot enter it. Repair it or \
                     remove it: this daemon does not change the mode of a \
                     directory it cannot prove it created.",
                    deepest.display()
                ),
            ));
        }
    }

    let missing: Vec<&Path> = directory
        .ancestors()
        .take_while(|level| !level.exists())
        .collect();
    for level in missing.into_iter().rev() {
        match std::fs::create_dir(level) {
            Ok(()) => grant_owner_entry(level)?,
            // Another process reached the same name first. It owns the mode,
            // and a directory it made is accepted.
            //
            // A symbolic link is NOT. This call proved the chain contained
            // before it started, and a link put here since points wherever
            // its maker chose. Everything below would be created through it.
            // The stat does not follow the link, so it sees the name itself.
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                let entry = std::fs::symlink_metadata(level)?;
                if !entry.file_type().is_dir() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::AlreadyExists,
                        format!(
                            "{} was created by another process, and it is not a \
                             directory",
                            level.display()
                        ),
                    ));
                }
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// Is this directory still inside the workspace?
///
/// [`resolve`] proves containment for the chain that exists WHEN IT RUNS. The
/// missing levels are created after that. Another process can put a symbolic
/// link at one of those names in between. `exists` follows a link, so the
/// creation walks through it and reports success.
///
/// A write would then put its scratch file, and its rename, outside the
/// workspace that approved it. This check is what refuses that, and it runs
/// before any scratch file exists.
///
/// The comparison is against the REAL root, so a workspace behind a link is
/// still served. [`resolve`] compares the same way.
///
/// # A window remains
///
/// This proves the chain contained at the moment it is read. Another process
/// can still swap a level between this check and the scratch file. Closing
/// that needs the directory to be held OPEN. The file must be created through
/// that handle. A later rename of a name cannot then redirect it. `openat`
/// and `renameat` do that, and `rustix` is already a dependency here.
///
/// That change belongs to the write path as a whole, and not to one review
/// round. This check narrows the window to the swap that happens inside one
/// pair of system calls, and refuses every slower one.
///
/// # Errors
///
/// Returns an error if either path cannot be resolved, or if the directory no
/// longer sits under the workspace root.
pub fn contained(directory: &Path, workspace: &Path) -> Result<(), String> {
    let root = workspace
        .canonicalize()
        .map_err(|e| format!("cannot resolve the workspace: {e}"))?;
    let real = directory
        .canonicalize()
        .map_err(|e| format!("cannot resolve the parent directory: {e}"))?;
    if !real.starts_with(&root) {
        return Err(
            "a directory above it now leaves the workspace: another process replaced one \
             after this path was resolved"
                .to_string(),
        );
    }
    Ok(())
}

/// Can the owner enter this directory?
///
/// A directory without the owner's `x` bit cannot be entered or listed by its
/// owner. Nothing below it can be created, so the caller is refused rather
/// than left to fail one level down.
///
/// Every existing mode is left as it is. A directory that is narrow but
/// usable, such as `0500`, is a mode an operator can mean. A write under it
/// fails with a plain permission error that names the path, which is honest.
fn owner_can_enter(directory: &Path) -> bool {
    std::fs::metadata(directory).is_ok_and(|entry| entry.permissions().mode() & 0o100 != 0)
}

/// Give the owner `rwx` on a directory, keeping the rest of the mode.
fn grant_owner_entry(directory: &Path) -> std::io::Result<()> {
    let mut mode = std::fs::metadata(directory)?.permissions();
    mode.set_mode(mode.mode() | 0o700);
    std::fs::set_permissions(directory, mode)
}

/// Create a scratch file beside the target, and return it with its path.
///
/// The same directory matters: a rename is only atomic within one filesystem.
/// The name is unique per attempt, and the file is created with `create_new`.
/// Nothing that already occupies a name is removed or written through. A fixed
/// name would have to be deleted first, and that name can belong to something
/// a person wants to keep.
fn create_scratch(path: &Path) -> Result<(PathBuf, std::fs::File), String> {
    let parent = path
        .parent()
        .ok_or_else(|| "the target has no directory".to_string())?;
    let name = path
        .file_name()
        .ok_or_else(|| "the target has no file name".to_string())?
        .to_string_lossy()
        .into_owned();

    let pid = std::process::id();
    let nonce = scratch_nonce();
    let mut last = None;
    for attempt in 0..SCRATCH_ATTEMPTS {
        let candidate = parent.join(scratch_name(&name, pid, nonce, attempt));
        // `O_NOFOLLOW` refuses a link, as everywhere else in this module. The
        // mode is deliberately conservative here; the target's mode is applied
        // to the descriptor below, where no umask can filter it.
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(NEW_FILE_MODE)
            .custom_flags(libc_o_nofollow())
            .open(&candidate)
        {
            Ok(file) => return Ok((candidate, file)),
            Err(e) => last = Some(e),
        }
    }

    Err(last.map_or_else(
        || format!("cannot create a scratch file beside `{name}`"),
        |e| format!("cannot create a scratch file beside `{name}`: {e}"),
    ))
}

/// A value that does not repeat across restarts.
///
/// The process id is not enough on its own. A container can start the daemon
/// as pid 1 every time. A crash between the create and the rename leaves that
/// scratch name behind, and nothing removes a file this module did not make.
/// The next start would try the same names, and sixteen such crashes would
/// leave an approved write with no name to use.
///
/// The hasher is seeded by the operating system, once per process, so two
/// daemons that start in the same nanosecond still differ.
pub fn scratch_nonce() -> u64 {
    use std::hash::{BuildHasher, Hash, Hasher};

    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_nanos())
        .unwrap_or_default()
        .hash(&mut hasher);
    std::process::id().hash(&mut hasher);
    hasher.finish()
}

/// The scratch basename for one attempt.
///
/// The name carries the target's stem, so a leftover file says what it was
/// for. The stem is cut when the whole name would not fit (see [`SCRATCH_CAP`]
/// and [`SCRATCH_FLOOR`]). The cut lands on a character boundary, so a name
/// of multi-byte characters is never split through one.
pub fn scratch_name(name: &str, pid: u32, nonce: u64, attempt: u32) -> String {
    let suffix = format!(".agentd-{pid}-{nonce:x}-{attempt}.tmp");
    let cap = name.len().clamp(SCRATCH_FLOOR, SCRATCH_CAP);
    // One byte for the leading dot.
    let room = cap.saturating_sub(suffix.len() + 1);
    format!(".{}{suffix}", &name[..floor_boundary(name, room)])
}

/// The largest character boundary of `text` at or below `limit`.
const fn floor_boundary(text: &str, limit: usize) -> usize {
    if limit >= text.len() {
        return text.len();
    }
    let mut index = limit;
    while index > 0 && !text.is_char_boundary(index) {
        index -= 1;
    }
    index
}

/// Fill the scratch file, give it the target's mode, and rename it over it.
fn write_through(
    mut file: std::fs::File,
    temporary: &Path,
    target: &Path,
    content: &str,
    mode: u32,
    flush: &[PathBuf],
) -> Result<(), WriteFailure> {
    use std::io::Write;

    file.write_all(content.as_bytes())
        .map_err(WriteFailure::BeforeRename)?;

    // `chmod` on the open descriptor, NOT a creation mode. A mode passed to
    // `open` is filtered through the umask, so a `0660` target would come back
    // `0640` under the common one. This sets exactly what was captured.
    file.set_permissions(std::fs::Permissions::from_mode(mode))
        .map_err(WriteFailure::BeforeRename)?;

    // Flush before the rename, so a crash cannot leave the target naming a file
    // whose content never reached the disk.
    file.sync_all().map_err(WriteFailure::BeforeRename)?;
    drop(file);

    std::fs::rename(temporary, target).map_err(WriteFailure::BeforeRename)?;

    // The rename itself is durable only once the DIRECTORY entry is. Without
    // this, a host crash can restore the old target, or lose a new one. The
    // history meanwhile records the write as done and never re-runs it.
    // Syncing the file alone does not cover the entry that names it.
    // A new directory needs the same treatment. A write to `a/b/notes.md` in an
    // empty workspace creates two directories and the file. Flushing only the
    // file's own parent leaves `b` missing from `a` after a crash, and the
    // flushed file goes with it.
    for directory in flush {
        std::fs::File::open(directory)
            .and_then(|handle| handle.sync_all())
            .map_err(WriteFailure::AfterRename)?;
    }

    Ok(())
}

/// Where a write stopped.
///
/// The two sides of the rename are not the same outcome. Before it, the target
/// is untouched and the tool reports that nothing was written. After it, the
/// target IS replaced, and a report of "nothing was written" would be false.
/// The model could then undo work that had in fact landed.
enum WriteFailure {
    /// The target was not touched.
    BeforeRename(std::io::Error),
    /// The target was replaced, and the durability work did not finish.
    AfterRename(std::io::Error),
}
