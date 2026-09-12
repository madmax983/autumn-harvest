//! Guards the "opt-in" claim for audit export (issue #953) against
//! overclaiming zero cost.
//!
//! Issue #1272: `harvest_audit_log_unexported_idx` is a partial index on
//! `export_seq IS NULL`. When export is unconfigured, `export_seq` stays
//! `NULL` on every row forever, so the index matches the whole audit table.
//! An unconfigured deployment still pays index maintenance on every audit
//! insert. Three prose sources asserted "zero-cost" or "byte-identical"
//! without that caveat. These guards keep the caveat attached to the claim.

use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory must have a parent")
        .to_path_buf()
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// The known-false claim this issue retracts. None of the three sources may
/// state it without also naming the index cost nearby.
const FALSE_CLAIM: &str = "byte-identical to before this module existed";

#[test]
fn module_doc_names_the_index_cost() {
    let path = repo_root().join("autumn-harvest/src/audit_export.rs");
    let text = read(&path);
    assert!(
        !text.contains(FALSE_CLAIM),
        "{}: the unqualified byte-identical claim must not return; \
         issue #1272 retracted it",
        path.display()
    );
    assert!(
        text.contains("harvest_audit_log_unexported_idx") && text.contains("1272"),
        "{}: the opt-in section must name the partial index and issue #1272 \
         next to its zero-cost claim",
        path.display()
    );
}

#[test]
fn published_doc_names_the_index_cost() {
    let path = repo_root().join("docs/audit-export.md");
    let text = read(&path);
    assert!(
        text.contains("harvest_audit_log_unexported_idx") && text.contains("1272"),
        "{}: the opt-in bullet must name the partial index and issue #1272",
        path.display()
    );
}

#[test]
fn changelog_fragment_names_the_index_cost() {
    let path = repo_root().join("docs/changelog.d/pr-953-audit-export.md");
    let text = read(&path);
    assert!(
        !text.contains("Opt-in and zero-cost when unconfigured"),
        "{}: the opt-in bullet must not claim zero cost without a caveat",
        path.display()
    );
    assert!(
        text.contains("1272"),
        "{}: the opt-in bullet must reference issue #1272",
        path.display()
    );
}

/// Characterizes the fix already present in the migration header (merged
/// ahead of this issue's code fix) so a future edit cannot silently drop it.
#[test]
fn migration_header_already_names_the_index_cost() {
    let path = repo_root()
        .join("autumn-harvest/migrations/20260728000000_harvest_audit_export/up.sql");
    let text = read(&path);
    assert!(
        text.contains("harvest_audit_log_unexported_idx") && text.contains("1272"),
        "{}: the migration header must keep naming the partial index and \
         issue #1272",
        path.display()
    );
}
