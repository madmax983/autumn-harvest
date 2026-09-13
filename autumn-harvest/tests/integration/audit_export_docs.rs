//! Guards the "opt-in" claim for audit export (issue #953) against
//! overclaiming zero cost.
//!
//! Issue #1272: `harvest_audit_log_unexported_idx` is a partial index on
//! `export_seq IS NULL`. When export is unconfigured, `export_seq` stays
//! `NULL` on every row forever, so the index matches the whole audit table.
//! An unconfigured deployment still pays index maintenance on every audit
//! insert. Four prose sources asserted "zero-cost", "byte-identical", or
//! "entirely inert" without that caveat. These guards keep the caveat
//! attached to the claim.
//!
//! These guards run in the `lint` job, unconditionally (see
//! `guards_run_on_docs_only_changes` below). A docs-only PR skips the `test`
//! matrix entirely, and it is exactly the change class most likely to
//! quietly drop the caveat. `docs/performance.md`'s guards hit this same gap
//! three rounds running. The fix there is copied here, not re-derived.

use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory must have a parent")
        .to_path_buf()
}

/// Read a file with line endings normalised to `\n`, so a `\n`-anchored
/// needle does not silently miss on a Windows checkout.
fn read_normalized(path: &Path) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
        .replace("\r\n", "\n")
}

/// Collapse whitespace runs to one space, so a needle survives Markdown or
/// a doc comment being re-wrapped at a different column.
fn collapse_ws(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn contains_collapsed(haystack: &str, needle: &str) -> bool {
    collapse_ws(haystack).contains(&collapse_ws(needle))
}

/// The known-false claim this issue retracts. None of the guarded sources
/// may state it without also naming the index cost nearby.
const FALSE_CLAIM: &str = "byte-identical to before this module existed";

/// Both markers must appear, and close enough together that an edit cannot
/// separate one from the other.
fn markers_near(text: &str, a: &str, b: &str) -> bool {
    const WINDOW: usize = 500;
    let flat = collapse_ws(text);
    let Some(a_at) = flat.find(a) else {
        return false;
    };
    let start = a_at.saturating_sub(WINDOW);
    let end = (a_at + a.len() + WINDOW).min(flat.len());
    flat[start..end].contains(b)
}

/// Both markers must appear, and close enough together that an edit cannot
/// separate the index name from the issue that explains its cost.
fn names_index_cost_near(text: &str, marker: &str) -> bool {
    markers_near(text, marker, "1272")
}

#[test]
fn module_doc_names_the_index_cost() {
    let path = repo_root().join("autumn-harvest/src/audit_export.rs");
    let text = read_normalized(&path);
    assert!(
        !contains_collapsed(&text, FALSE_CLAIM),
        "{}: the unqualified byte-identical claim must not return; \
         issue #1272 retracted it",
        path.display()
    );
    assert!(
        names_index_cost_near(&text, "harvest_audit_log_unexported_idx"),
        "{}: the opt-in section must name the partial index within reach of \
         issue #1272, next to its zero-cost claim",
        path.display()
    );
}

/// A second claim in the same file said the unconfigured feature is
/// "entirely inert" with no caveat, on `AuditExportBuilderConfig`. It is the
/// same overclaim, in a fourth place, that this issue retracts.
#[test]
fn builder_config_doc_scopes_inert_to_the_scanner() {
    let path = repo_root().join("autumn-harvest/src/audit_export.rs");
    let text = read_normalized(&path);
    assert!(
        !contains_collapsed(&text, "the feature is entirely inert"),
        "{}: 'entirely inert' must be scoped (e.g. to the scanner), not \
         asserted of the whole feature — the partial index still costs \
         insert-time maintenance when unconfigured (issue #1272)",
        path.display()
    );
}

#[test]
fn published_doc_names_the_index_cost() {
    let path = repo_root().join("docs/audit-export.md");
    let text = read_normalized(&path);
    assert!(
        names_index_cost_near(&text, "harvest_audit_log_unexported_idx"),
        "{}: the opt-in bullet must name the partial index within reach of \
         issue #1272",
        path.display()
    );
}

#[test]
fn changelog_fragment_names_the_index_cost() {
    let path = repo_root().join("docs/changelog.d/pr-953-audit-export.md");
    let text = read_normalized(&path);
    assert!(
        !contains_collapsed(&text, "Opt-in and zero-cost when unconfigured"),
        "{}: the opt-in bullet must not claim zero cost without a caveat",
        path.display()
    );
    assert!(
        names_index_cost_near(&text, "1272"),
        "{}: the opt-in bullet must reference issue #1272",
        path.display()
    );
}

/// Characterizes the fix already present in the migration header (merged
/// ahead of this issue's code fix) so a future edit cannot silently drop it.
///
/// Not a proximity check like the ones above. The header names the cost in
/// its opening comment block, well before the `CREATE INDEX` statement it
/// describes. Marker and issue number sit pages apart by construction, so
/// presence of both is the property worth pinning here.
#[test]
fn migration_header_already_names_the_index_cost() {
    let path =
        repo_root().join("autumn-harvest/migrations/20260728000000_harvest_audit_export/up.sql");
    let text = read_normalized(&path);
    assert!(
        text.contains("harvest_audit_log_unexported_idx") && text.contains("1272"),
        "{}: the migration header must keep naming the partial index and \
         issue #1272",
        path.display()
    );
}

/// The comment next to `CREATE INDEX` repeated a false claim. The header
/// above it already retracts that claim: the index "stays empty (and free)"
/// with no sink configured. A reader at the index definition may never
/// scroll back up to see the header contradict it.
#[test]
fn migration_index_comment_does_not_restate_the_false_claim() {
    let path =
        repo_root().join("autumn-harvest/migrations/20260728000000_harvest_audit_export/up.sql");
    let text = read_normalized(&path);
    assert!(
        !contains_collapsed(
            &text,
            "it stays empty (and free) when no sink is configured"
        ),
        "{}: the CREATE INDEX comment must not claim the index stays empty; \
         it matches every row when unconfigured (issue #1272)",
        path.display()
    );
}

/// `docs/upgrading/0.5.0.md`'s migration-table row for this migration made
/// the same false claim about both partial indexes, and called the whole
/// migration "inert" with no caveat.
#[test]
fn upgrade_guide_does_not_restate_the_false_claim() {
    let path = repo_root().join("docs/upgrading/0.5.0.md");
    let text = read_normalized(&path);
    assert!(
        !contains_collapsed(&text, "both stay empty when no sink is configured"),
        "{}: the audit-export row must not claim both indexes stay empty; \
         the unexported one matches every row when unconfigured (issue \
         #1272)",
        path.display()
    );
    assert!(
        markers_near(&text, "harvest_audit_export", "1272"),
        "{}: the audit-export row must reference issue #1272",
        path.display()
    );
    assert!(
        !contains_collapsed(&text, "not zero-cost until an audit sink is configured"),
        "{}: 'until configured' implies a sink removes the cost, but index \
         maintenance applies regardless of configuration — say 'when no \
         audit sink is configured' instead",
        path.display()
    );
}

/// Issue #1272's own fix added a "bounded by the retention window" claim.
/// That is itself conditional: `audit_retention_days = 0` disables the purge
/// (`retention.rs`'s `audit_retention_days > 0` gate), so the table and the
/// index grow without bound. Both sources that make the claim must qualify
/// it, not repeat the same class of overclaim this issue exists to retract.
#[test]
fn boundedness_claim_is_qualified_by_retention_setting() {
    for rel in ["autumn-harvest/src/audit_export.rs", "docs/audit-export.md"] {
        let path = repo_root().join(rel);
        let text = read_normalized(&path);
        assert!(
            markers_near(&text, "bounded by the", "audit_retention_days"),
            "{}: a 'bounded by the retention window' claim must name \
             `audit_retention_days = 0` as the case where it does not hold",
            path.display()
        );
    }
}

/// The AC8 section header in the DB-gated integration suite must not
/// restate the bare "zero-cost when unconfigured" claim either. It compiles
/// behind `#[cfg(feature = "db")]`, so an edit there can skip the ungated
/// tests above.
#[test]
fn db_gated_test_header_does_not_restate_the_false_claim() {
    let path = repo_root().join("autumn-harvest/tests/integration/audit_export_tests.rs");
    let text = read_normalized(&path);
    assert!(
        !contains_collapsed(&text, "AC8: opt-in and zero-cost when unconfigured"),
        "{}: the AC8 section header must not claim zero cost without a \
         caveat (issue #1272)",
        path.display()
    );
}

/// The whole step stanza containing `needle` in a workflow job block. Spans
/// from its `- name:` line to the line before the next step's `- name:`, or
/// the end of the block. Bounding at the *next* step, not at `needle`, is
/// load-bearing. See `performance_docs::workflow_step_stanza`. A step
/// re-gated by moving `if:` below `run:` would otherwise sit outside the
/// slice this test inspects.
fn workflow_step_stanza<'a>(block: &'a str, needle: &str) -> Option<&'a str> {
    const STEP: &str = "\n      - name:";
    let at = block.find(needle)?;
    let start = block[..at].rfind(STEP).unwrap_or(0);
    let after_marker = start + STEP.len();
    let end = block[after_marker..]
        .find(STEP)
        .map_or(block.len(), |rel| after_marker + rel);
    Some(&block[start..end])
}

/// Issue #1272's guards must run on a docs-only PR. That is the change
/// class most likely to break them. `.github/workflows/ci.yml`'s `test`
/// matrix skips it entirely: every step there is gated on
/// `changes.outputs.code == 'true'`. So these guards must also run from the
/// ungated `lint` job, unconditionally. Mirrors
/// `performance_docs::performance_guards_run_on_docs_only_changes`.
#[test]
fn guards_run_on_docs_only_changes() {
    const FILTER: &str = "--test integration audit_export_docs::";
    let workflow = read_normalized(&repo_root().join(".github/workflows/ci.yml"));

    let step = workflow
        .lines()
        .find(|line| line.contains(FILTER))
        .unwrap_or_else(|| {
            panic!(
                "ci.yml must run the audit_export_docs guards from a step that \
                 is not gated on `changes.outputs.code`, or a docs-only PR — the \
                 change class these guards exist for — skips them entirely"
            )
        });
    assert!(
        step.trim_start().starts_with("run:"),
        "expected the guard invocation to be a step `run:` line, found: {step}"
    );

    let lint_start = workflow
        .find("\n  lint:")
        .expect("ci.yml must define a `lint` job");
    let test_start = workflow
        .find("\n  test:")
        .expect("ci.yml must define a `test` job");
    let step_at = workflow.find(FILTER).expect("located above");
    assert!(
        step_at > lint_start && step_at < test_start,
        "the audit_export_docs guard step must live in the ungated `lint` \
         job; a step in the `test` matrix is gated on `changes.outputs.code` \
         and so does not run on a docs-only PR"
    );

    let block = &workflow[lint_start..test_start];
    let stanza = workflow_step_stanza(block, FILTER)
        .expect("the guard step is inside the lint block, located above");
    assert!(
        !stanza.contains("\n        if:"),
        "the audit_export_docs guard step has acquired an `if:` condition. \
         It must run unconditionally: a condition is how these guards would \
         stop running on docs-only PRs. Stanza:\n{stanza}"
    );
}
