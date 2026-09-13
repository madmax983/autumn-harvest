use serde_json::Value;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

const REQUIRED_ALERTS: &[&str] = &[
    "harvest_preflight_failed",
    "harvest_no_active_workers",
    "harvest_queue_uncovered",
    "harvest_worker_saturation",
    "harvest_queue_schedule_to_start_high",
    "harvest_queue_backlog_growth",
    "harvest_activity_failure_surge",
    "harvest_dlq_growth",
    "harvest_schedule_missed_runs",
    "harvest_retention_lag",
    "harvest_shard_unready",
    "harvest_no_compatible_worker",
    "harvest_workflow_non_determinism",
    "harvest_shard_undrained",
    "harvest_saga_compensation_spike",
    "harvest_saga_compensation_failed",
    "harvest_update_rejected_rate",
    "harvest_signal_unhandled_rate",
    "harvest_workflow_population_leak",
    "harvest_queue_paused_too_long",
    "harvest_workflow_history_bloat",
    "harvest_scanner_stalled",
    "harvest_no_capable_worker",
    "harvest_capability_miss_never_offered",
    "harvest_capability_miss_release_sustained",
    // Issue #954 — cross-region DR.
    "harvest_replication_down",
    "harvest_replication_lag_high",
    "harvest_shard_fenced",
    "harvest_replication_unobservable",
    // Issue #953 — audit export to a SIEM sink.
    "harvest_audit_export_lag_high",
    // Issue #1268 — the availability companion to the lag gauge.
    "harvest_audit_export_unobservable",
];

const REQUIRED_DRILLS: &[&str] = &[
    "queue-backlog",
    "dlq-spike",
    "stale-worker-fleet",
    "missed-schedule",
    "shard-unready",
    // Issue #954 — the replication half of the cross-region failover drill.
    // The full failover procedure lives in
    // docs/runbooks/cross-region-failover.md; this is the alerting rehearsal.
    "replication-stalled",
];

const REQUIRED_RUNBOOK_SUBSECTIONS: &[&str] = &[
    "### Triage steps",
    "### Likely causes",
    "### False positives",
    "### Safe actions",
    "### Escalation criteria",
];

const STABLE_PROMETHEUS_METRICS: &[&str] = &[
    "harvest_workflow_started_total",
    "harvest_workflow_duration_count",
    "harvest_workflow_duration_sum",
    "harvest_workflow_duration_bucket",
    "harvest_activity_duration_count",
    "harvest_activity_duration_sum",
    "harvest_activity_duration_bucket",
    "harvest_timer_started_total",
    "harvest_queue_depth",
    "harvest_shard_stranded_pending",
    "harvest_shard_dispatched_total",
    "harvest_queue_schedule_to_start_count",
    "harvest_queue_schedule_to_start_sum",
    "harvest_queue_schedule_to_start_bucket",
    "harvest_queue_oldest_pending_age",
    "harvest_dlq_entries",
    "harvest_queue_paused",
    "harvest_schedule_runs_total",
    "harvest_schedule_skipped_total",
    "harvest_schedule_overdue",
    "harvest_retention_deleted_total",
    "harvest_schedule_fire_attempts_total",
    "harvest_workflow_terminal_total",
    "harvest_workflow_non_determinism_total",
    "harvest_workflow_nondeterministic_block_total",
    "harvest_activity_attempts_total",
    "harvest_activity_retries_total",
    "harvest_worker_slots_in_use",
    "harvest_worker_slots_available",
    "harvest_saga_compensated_total",
    "harvest_saga_compensation_failed_total",
    "harvest_update_rejected_total",
    "harvest_signal_unhandled_total",
    "harvest_workflow_active",
    "harvest_workflow_history_bloat_total",
    "harvest_scanner_tick_total",
    "harvest_task_capability_miss_total",
    // Issue #954 — cross-region DR.
    "harvest_replication_lag_seconds",
    // Issue #953 — audit export to a SIEM sink.
    "harvest_audit_export_lag",
    "harvest_audit_exported_total",
    // Issue #1268 — the availability companion to the lag gauge.
    "harvest_audit_export_observed",
    "harvest_replication_lag_bytes",
    "harvest_replication_standbys",
    "harvest_replication_observable",
    // Issue #1249 — readable-but-unmeasurable RPO signal.
    "harvest_replication_rpo_known",
    "harvest_shard_generation",
    "harvest_shard_fenced_total",
];

#[test]
fn starter_alert_pack_is_versioned_and_complete() {
    let pack = read_pack();
    assert_eq!(
        pack["pack_version"].as_str(),
        Some("0.1.0"),
        "starter alert pack must carry the documented version"
    );
    assert!(
        pack["threshold_policy"]
            .as_str()
            .is_some_and(|policy| policy.contains("starter defaults")),
        "threshold policy must tell embedders to tune these defaults"
    );

    let rules = pack["rules"]
        .as_array()
        .expect("rules must be a JSON array");
    let found: BTreeSet<&str> = rules
        .iter()
        .map(|rule| rule["id"].as_str().expect("rule id must be a string"))
        .collect();

    for required in REQUIRED_ALERTS {
        assert!(
            found.contains(required),
            "starter alert pack is missing required alert {required}"
        );
    }

    for rule in rules {
        for field in [
            "id",
            "signal_source",
            "default_threshold",
            "severity",
            "owner_persona",
            "description",
            "first_action",
            "runbook",
        ] {
            assert!(
                rule[field].as_str().is_some_and(|value| !value.is_empty()),
                "rule {} must include non-empty {field}",
                rule["id"]
            );
        }

        assert!(
            rule["dependencies"]
                .as_array()
                .is_some_and(|deps| !deps.is_empty()),
            "rule {} must be dependency-tagged",
            rule["id"]
        );
        assert!(
            rule["management_checks"]
                .as_array()
                .is_some_and(|checks| !checks.is_empty()),
            "rule {} must include CLI/API checks for non-Prometheus operators",
            rule["id"]
        );
        assert!(
            rule["runbook"]
                .as_str()
                .is_some_and(|path| path.starts_with("docs/runbooks/harvest-alerts.md#")),
            "rule {} must link into the Harvest alert runbook",
            rule["id"]
        );
    }
}

#[test]
fn prometheus_examples_use_stable_bounded_harvest_metrics() {
    let pack = read_pack();
    let stable: BTreeSet<&str> = STABLE_PROMETHEUS_METRICS.iter().copied().collect();
    let forbidden = ["execution.id", "harvest.execution.id", "execution_id"];
    let rules = pack["rules"].as_array().expect("rules must be an array");

    for rule in rules {
        let Some(expressions) = rule["prometheus"]["expressions"].as_array() else {
            continue;
        };
        for expression in expressions {
            let expr = expression["expr"]
                .as_str()
                .expect("Prometheus expression must be a string");
            for forbidden_label in forbidden {
                assert!(
                    !expr.contains(forbidden_label),
                    "rule {} Prometheus expression uses forbidden label {forbidden_label}: {expr}",
                    rule["id"]
                );
            }
            for token in harvest_metric_tokens(expr) {
                assert!(
                    stable.contains(token.as_str()),
                    "rule {} Prometheus expression uses non-catalog metric {token}: {expr}",
                    rule["id"]
                );
            }
        }
    }
}

#[test]
fn saga_spike_alert_absolute_floor_fires_without_a_baseline() {
    // Round-4 hardening (issue #801, Codex review): a NEW or long-dormant
    // workflow has no samples in the `offset 1h` baseline window, so a bare
    // `> 4 * rate(... offset 1h)` comparison yields no series and the
    // absolute 1/min floor never fires for the FIRST rollback wave — the
    // exact case the alert exists for. The `or <current> * 0` arm defaults
    // the baseline to a zero-valued series with matching labels; pin that
    // fallback textually so it cannot be silently simplified away.
    let pack = read_pack();
    let rules = pack["rules"].as_array().expect("rules must be an array");
    let spike = rules
        .iter()
        .find(|rule| rule["id"].as_str() == Some("harvest_saga_compensation_spike"))
        .expect("saga compensation spike alert must exist");
    let expr = spike["prometheus"]["expressions"][0]["expr"]
        .as_str()
        .expect("spike alert must carry a PromQL expression");
    assert!(
        expr.contains("or sum by (workflow) (rate(harvest_saga_compensated_total[5m])) * 0"),
        "spike alert baseline must default to a zero-valued, label-matched series so the \
         absolute floor fires with no baseline samples: {expr}"
    );
}

/// Round-10 hardening (issue #619, Codex review): the queue-pause alert's
/// description originally said a pause is safe because "nothing fails, retries,
/// or dead-letters". That is true of the *relative* `schedule_to_start` timer,
/// which a pause suspends and credits back on resume, but **not** of the
/// *absolute* `schedule_to_close` deadline (issue #378), which keeps running for
/// the whole hold. This is the one alert an operator consults after the 1h/4h
/// threshold, so an unqualified "nothing fails" there invites leaving a hold in
/// place under the false assumption that all held work is protected. Pin the
/// exception textually — the API contract and the runbook both carry it, and a
/// future "tighten the wording" pass must not quietly drop it.
#[test]
fn queue_pause_alert_states_the_schedule_to_close_exception() {
    let pack = read_pack();
    let rules = pack["rules"].as_array().expect("rules must be an array");
    let rule = rules
        .iter()
        .find(|rule| rule["id"].as_str() == Some("harvest_queue_paused_too_long"))
        .expect("queue-pause alert must exist");
    let description = rule["description"]
        .as_str()
        .expect("description must be a string");
    assert!(
        description.contains("schedule_to_close"),
        "the pause-safety claim must name the absolute-deadline exception: {description}"
    );
    // The runbook says the same thing; the two must not drift apart.
    let runbook = read_doc("docs/runbooks/harvest-alerts.md");
    let section = markdown_section(&runbook, "harvest_queue_paused_too_long")
        .expect("queue-pause runbook section must exist");
    assert!(
        section.contains("schedule_to_close"),
        "the runbook section must carry the same exception as the alert description"
    );
}

#[test]
fn every_alert_links_to_a_complete_runbook_section() {
    let runbook = read_doc("docs/runbooks/harvest-alerts.md");
    for alert in REQUIRED_ALERTS {
        let section = markdown_section(&runbook, alert)
            .unwrap_or_else(|| panic!("missing runbook section ## {alert}"));
        for required in REQUIRED_RUNBOOK_SUBSECTIONS {
            assert!(
                section.contains(required),
                "runbook section {alert} is missing {required}"
            );
        }
    }
}

#[test]
fn shard_undrained_alert_does_not_infer_poller_absence_from_dispatches() {
    let pack = read_pack();
    let rules = pack["rules"].as_array().expect("rules must be an array");
    let rule = rules
        .iter()
        .find(|rule| rule["id"].as_str() == Some("harvest_shard_undrained"))
        .expect("shard-undrained alert must exist");
    let expressions = rule["prometheus"]["expressions"]
        .as_array()
        .expect("expressions must be an array");
    let narrow = expressions.get(1).expect("narrow expression must exist");
    let expr = narrow["expr"]
        .as_str()
        .expect("expression must be a string");
    let note = narrow["note"].as_str().expect("note must be a string");
    let description = rule["description"]
        .as_str()
        .expect("description must be a string");

    assert!(
        expr.starts_with("max by (shard) (harvest_shard_stranded_pending) > 0 unless ")
            && expr.contains("rate(harvest_shard_dispatched_total[5m])")
            && !expr.contains("== 0"),
        "the narrow expression must retain absent dispatch series: {expr}"
    );
    assert!(
        note.contains("unable to claim") && !note.contains("genuinely no covering poller"),
        "zero dispatch activity must not prove poller absence: {note}"
    );
    assert!(
        description.contains("no live worker can claim")
            && !description.contains("no live worker is polling"),
        "the alert description must state the claimability condition: {description}"
    );

    let runbook = read_doc("docs/runbooks/harvest-alerts.md");
    let section = markdown_section(&runbook, "harvest_shard_undrained")
        .expect("shard-undrained runbook section must exist");
    assert!(
        section.contains("`polls queue(s)` means no shard-assigned worker polls")
            && section.contains("`capability/build/sticky requirements` means a covering poller"),
        "the runbook must branch on shard-health blocking reasons"
    );
}

/// The replication-down alert must key on the standby count, not on lag.
///
/// A dead standby produces **no lag reading at all** — the lag series is
/// deliberately absent rather than `0`, because a dead standby reported as a
/// perfect RPO is the most dangerous number this feature could publish. An
/// alert written as `harvest_replication_lag_seconds > N` therefore stays
/// silent through exactly the outage it was written for. Pin the shape.
#[test]
fn the_replication_down_alert_keys_on_standby_count_not_on_lag() {
    let pack = read_pack();
    let rules = pack["rules"].as_array().expect("rules must be an array");
    let rule = rules
        .iter()
        .find(|rule| rule["id"].as_str() == Some("harvest_replication_down"))
        .expect("replication-down alert must exist");
    let expr = rule["prometheus"]["expressions"][0]["expr"]
        .as_str()
        .expect("must carry a PromQL expression");
    assert!(
        expr.contains("harvest_replication_standbys"),
        "the replication-down alert must key on the standby gauge: {expr}"
    );
    assert!(
        !expr.contains("harvest_replication_lag_seconds"),
        "the replication-down alert must NOT depend on a lag series that is absent \
         precisely when replication is down: {expr}"
    );
    let description = rule["description"].as_str().expect("description");
    assert!(
        description.contains("unknown") || description.contains("absent"),
        "the description must explain that the lag series goes ABSENT, not to zero: \
         {description}"
    );
}

/// A fenced worker never recovers on its own.
#[test]
fn the_fenced_alert_says_the_condition_is_not_self_healing() {
    let pack = read_pack();
    let rules = pack["rules"].as_array().expect("rules must be an array");
    let rule = rules
        .iter()
        .find(|rule| rule["id"].as_str() == Some("harvest_shard_fenced"))
        .expect("shard-fenced alert must exist");
    let description = rule["description"].as_str().expect("description");
    assert!(
        description.contains("restart"),
        "the fenced alert must say the fix is restarting the fleet: {description}"
    );
    let runbook = read_doc("docs/runbooks/harvest-alerts.md");
    let section = markdown_section(&runbook, "harvest_shard_fenced")
        .expect("shard-fenced runbook section must exist");
    let lower = section.to_lowercase();
    assert!(
        lower.contains("never") && (lower.contains("re-pin") || lower.contains("adopt")),
        "the runbook must forbid re-pinning a fenced worker in place"
    );
}

#[test]
fn synthetic_incident_drills_cover_required_failure_modes() {
    let drills = read_doc("docs/runbooks/synthetic-incident-drills.md");
    for drill in REQUIRED_DRILLS {
        let section = markdown_section(&drills, drill)
            .unwrap_or_else(|| panic!("missing synthetic drill section ## {drill}"));
        assert!(
            section.contains("Expected alert:"),
            "drill {drill} must name the expected alert"
        );
        assert!(
            section.contains("Runbook step:"),
            "drill {drill} must name the resolving or escalating runbook step"
        );
    }
}

#[test]
fn non_prometheus_path_documents_cli_and_api_checks() {
    let guide = read_doc("docs/alerts/README.md");
    for required in [
        "harvest preflight",
        "GET /api/harvest/admin/preflight",
        "harvest worker health",
        "GET /api/harvest/workers/health",
        "harvest shard health",
        "GET /api/harvest/admin/shards/health",
        "harvest dlq list",
        "GET /api/harvest/dead-letters",
        "harvest schedule list",
        "GET /api/harvest/admin/schedules",
        "harvest concurrency status",
        "GET /api/harvest/admin/concurrency",
        "harvest workflow stack",
        "GET /api/harvest/workflows/{execution_id}/stack",
        "GET /api/harvest/admin/retention",
        "POST /api/harvest/admin/retention/run-now",
    ] {
        assert!(
            guide.contains(required),
            "non-Prometheus alert guide must document {required}"
        );
    }
    assert!(
        !guide.contains("GET /api/harvest/admin/retention/status"),
        "non-Prometheus alert guide must not document the nonexistent retention /status endpoint"
    );
}

#[test]
fn retention_alert_uses_mounted_management_api_paths() {
    let pack = read_pack();
    let rules = pack["rules"].as_array().expect("rules must be an array");
    let retention = rules
        .iter()
        .find(|rule| rule["id"].as_str() == Some("harvest_retention_lag"))
        .expect("retention alert must exist");
    let checks: Vec<&str> = retention["management_checks"]
        .as_array()
        .expect("management_checks must be an array")
        .iter()
        .map(|check| check.as_str().expect("management check must be a string"))
        .collect();

    assert!(
        checks.contains(&"GET /api/harvest/admin/retention"),
        "retention status check must use mounted GET /admin/retention endpoint"
    );
    assert!(
        checks.contains(&"POST /api/harvest/admin/retention/run-now"),
        "retention run-now check must use mounted POST /admin/retention/run-now endpoint"
    );
    assert!(
        !checks.contains(&"GET /api/harvest/admin/retention/status"),
        "retention alert must not point at nonexistent /admin/retention/status endpoint"
    );
}

/// Issue #797, Codex review: the scanner tick series is initialized at **zero**
/// when each loop registers, at spawn time. That is what lets a loop wedging on
/// its first iteration still page — but it also means a fresh
/// retention-enabled process exports `harvest_scanner_tick_total{scanner="retention"} = 0`
/// for the whole first hour, while its hourly janitor sleeps toward its first
/// pass. Prometheus does not wait for a full `[3h]` history before evaluating
/// `increase(...)`; two scrapes are enough for it to return `0`, so a bare
/// `increase(...[3h]) == 0` pages through every healthy startup.
///
/// The pack's schema carries no `for:` field (only `mode`/`notes`/`expressions`),
/// so the hold has to live in the expression. Gating on the counter's **own
/// value** is the portable form: it needs no `process_start_time_seconds` (which
/// the metrics exporter does not emit) and no assumption about scrape interval.
///
/// Pin it textually — a future "simplify the expression" pass must not drop the
/// gate and reintroduce a rule that pages on every deploy.
#[test]
fn scanner_stalled_retention_expression_cannot_fire_during_the_startup_hour() {
    let pack = read_pack();
    let rules = pack["rules"].as_array().expect("rules must be an array");
    let stalled = rules
        .iter()
        .find(|rule| rule["id"].as_str() == Some("harvest_scanner_stalled"))
        .expect("scanner stalled alert must exist");
    let exprs: Vec<&str> = stalled["prometheus"]["expressions"]
        .as_array()
        .expect("scanner stalled alert must carry PromQL expressions")
        .iter()
        .filter_map(|expr| expr["expr"].as_str())
        .collect();
    let retention = exprs
        .iter()
        .find(|expr| expr.contains("scanner=\"retention\""))
        .expect("scanner stalled alert must carry a retention-specific expression");

    assert!(
        retention.contains("and max_over_time(harvest_scanner_tick_total{scanner=\"retention\"}"),
        "the retention expression must gate on the counter having ticked at least once, so the \
         registration-time zero cannot page through the healthy first hour: {retention}"
    );

    // The gate must be a FUNCTION result, not a bare selector. PromQL set
    // operators match on the full label set INCLUDING `__name__`; `increase()`
    // drops `__name__` while a bare selector keeps it, so
    // `increase(...) == 0 and harvest_scanner_tick_total{...} > 0` matches
    // nothing and silently disables the alert. Both sides being functions over
    // the same selector keeps their label sets identical.
    let (_, gate) = retention
        .split_once(" and ")
        .expect("the retention expression must carry an `and` gate");
    assert!(
        !gate.trim_start().starts_with("harvest_scanner_tick_total"),
        "the gate must not be a bare selector -- it would carry __name__ while the increase() \
         side does not, so the `and` would match no series and the alert would never fire: {gate}"
    );

    // The sub-minute loops must NOT carry the gate: they tick within seconds of
    // registration, so there is no startup window to ride out -- and the gate
    // would blind the alert to a loop that wedges on iteration one, which is
    // exactly what initializing the series at zero exists to catch.
    let sub_minute = exprs
        .iter()
        .find(|expr| expr.contains("scanner!=\"retention\""))
        .expect("scanner stalled alert must carry a sub-minute expression");
    assert!(
        !sub_minute.contains("> 0"),
        "the sub-minute expression must NOT gate on a prior tick -- that would hide a loop that \
         wedges on its first iteration: {sub_minute}"
    );

    // A follow-up review asked for a reset-/uptime-scoped gate, on the grounds
    // that `max_over_time` is not scoped to the current counter lifetime. It is
    // not: `increase()` IS reset-aware (`last - first + correction`), so a
    // healthy loop restarted mid-window yields a NONZERO increase and the rule
    // cannot fire at all. Adding `unless resets(...) > 0` would instead blind
    // the alert for a full window after every deploy. Pin both halves: no
    // `resets()` term in the expression, and the reasoning recorded in `notes`
    // so a future pass does not "fix" this back.
    assert!(
        !retention.contains("resets("),
        "the retention expression must not carry a resets()-scoped gate -- it would blind the \
         alert for a full window after every deploy: {retention}"
    );
    let notes = stalled["prometheus"]["notes"]
        .as_str()
        .expect("scanner stalled alert must carry prometheus notes");
    assert!(
        notes.contains("RESTART SEMANTICS"),
        "the notes must record why the startup gate needs no resets()/uptime term, so the \
         reset-aware increase() argument is not re-derived on every review"
    );
}

/// Shared accessor for the capability-miss alert-shape pins below.
///
/// Hoisted out of the test body when the pin was split in two (issue #804,
/// review round 41) so both halves read the same rule set through the same
/// severity check, rather than duplicating the closure.
fn capability_miss_rule_exprs(id: &str) -> Vec<String> {
    let pack = read_pack();
    let rules = pack["rules"].as_array().expect("rules must be an array");
    let rule = rules
        .iter()
        .find(|rule| rule["id"].as_str() == Some(id))
        .unwrap_or_else(|| panic!("{id} alert must exist"));
    assert_eq!(
        rule["severity"].as_str(),
        Some(if id == "harvest_no_capable_worker" {
            "page"
        } else {
            "ticket"
        }),
        "{id} severity is load-bearing for this pin"
    );
    rule["prometheus"]["expressions"]
        .as_array()
        .unwrap_or_else(|| panic!("{id} must carry PromQL expressions"))
        .iter()
        .filter_map(|expr| expr["expr"].as_str())
        .map(ToOwned::to_owned)
        .collect()
}

/// Issue #804, Codex review: the capability-miss counter is deliberately
/// two-outcome. `outcome="escalated"` means a task exhausted its redelivery
/// budget and an execution was FAILED — that is the page. `outcome="released"`
/// means a worker handed a claim back for a capable peer and NOTHING failed;
/// it is the expected, self-healing signature of the transient window in a
/// rolling deploy that introduces a new workflow or activity type.
///
/// Every rule in this pack carries a single `severity`, so every expression on
/// a rule fires at that severity. Putting a `released` expression on the
/// `page`-severity rule therefore pages on exactly the benign outcome the
/// rule's own notes call "not a page" — one incapable pod releasing one task
/// during a routine deploy is enough. Worse, an `increase(...[15m]) > 0` form
/// tests whether ANY release happened in the window, not whether releases
/// stayed sustained, so it cannot express "the skew is not resolving" either.
///
/// The sustained-release signal lives on its own ticket-severity rule with the
/// hold in the expression (the pack schema has no `for:` field). Pin both
/// halves textually — a future "fold these two back together" pass must not
/// reintroduce a rule that pages on every deploy.
#[test]
fn capability_miss_released_outcome_never_pages() {
    // Half 1: the PAGING rule must carry no `released` expression at all.
    let paging = capability_miss_rule_exprs("harvest_no_capable_worker");
    assert!(
        paging.iter().all(|expr| !expr.contains("released")),
        "the page-severity capability-miss rule must not select outcome=\"released\" -- a single \
         release during a rolling deploy would page on the benign, self-healing outcome: {paging:?}"
    );
    assert!(
        paging
            .iter()
            .any(|expr| expr.contains("outcome=\"escalated\"")),
        "the page-severity capability-miss rule must still page on escalation: {paging:?}"
    );

    // Half 1b: ...and must page ONLY on true budget exhaustion. Two escalation
    // causes fail the execution on its FIRST claim after ZERO releases -- a
    // `capability_miss_max_redeliveries = 0` config, and a task pinned to a
    // worker session (#606) whose host lacks the handler. Neither supports this
    // rule's whole narrative ("no live worker on this queue registers the
    // handler"): a capable peer may be live and idle on the queue the entire
    // time, and this rule's `first_action` sends on-call to
    // `GET /admin/workflow-types/reachability`, which then reports `in_use` and
    // contradicts the page they are holding.
    //
    // They record `outcome="escalated_never_offered"` for that reason. PromQL
    // `=` is exact string equality, so the selector above already excludes it --
    // pin that a future regex/prefix matcher (`outcome=~"escalated.*"`) cannot
    // silently re-conflate them.
    assert!(
        paging
            .iter()
            .all(|expr| !expr.contains("escalated_never_offered")),
        "the page-severity rule must not select the never-offered outcome: those \
         tasks were failed without ever being offered to a peer, so they are not \
         evidence the fleet lacks the handler: {paging:?}"
    );
    assert!(
        paging.iter().all(|expr| !expr.contains("=~")),
        "the escalation selector must stay an exact match: a regex matcher on \
         `outcome` would re-admit escalated_never_offered into the page: {paging:?}"
    );

    // Half 1c: ...but the never-offered escalations must not be SILENT either --
    // they fail executions. They get their own ticket-severity rule.
    let never_offered = capability_miss_rule_exprs("harvest_capability_miss_never_offered");
    assert!(
        never_offered
            .iter()
            .any(|expr| expr.contains("outcome=\"escalated_never_offered\"")),
        "the never-offered rule must select its own outcome: {never_offered:?}"
    );
    assert!(
        never_offered
            .iter()
            .all(|expr| !expr.contains("outcome=\"escalated\"")),
        "...and must not double-count the budget-exhausted escalations the \
         paging rule already owns: {never_offered:?}"
    );
}

/// The paging rule must not assert a fleet conclusion the outcome label does
/// not support (issue #804, Codex round-43).
///
/// `EscalationCause::outcome_label` maps BOTH the budget bounds and the ungated
/// absolute release ceiling to `outcome="escalated"`, deliberately: executions
/// are being failed either way and under-paging is the worse error. But the two
/// do not license the same conclusion. The gated bounds fire only once the
/// registry confirms the recorded missers cover the live fleet; the ceiling
/// fires precisely where that coverage could NOT be established, so a live,
/// never-tried peer may still be capable.
///
/// The rule's prose is what on-call reads first, so it must not send them to a
/// fleet-exhaustion investigation for an escalation that does not support one.
/// This pins the cause-neutral phrasing rather than the wording: the reason
/// string must be named as the discriminator, the ceiling's weaker conclusion
/// must be stated, and the round-15-stale "a fleet smaller than the budget"
/// claim must not reappear — a *registered* small fleet escalates at `N` via
/// the configured-total bound, not via the ceiling.
#[test]
fn capability_miss_paging_rule_prose_is_cause_neutral() {
    let pack = read_pack();
    let rules = pack["rules"].as_array().expect("rules must be an array");
    let rule = rules
        .iter()
        .find(|rule| rule["id"].as_str() == Some("harvest_no_capable_worker"))
        .expect("the paging capability-miss rule must exist");

    let field = |name: &str| -> String {
        rule[name]
            .as_str()
            .unwrap_or_else(|| panic!("{name} must be a string"))
            .to_string()
    };
    let description = field("description");
    let first_action = field("first_action");
    let threshold = field("default_threshold");

    // The discriminator must be named, and named FIRST in the triage step --
    // every other check in `first_action` is only correct for one of the two
    // conclusions.
    assert!(
        first_action.contains("reason") && first_action.contains("FIRST"),
        "first_action must send on-call to the execution's reason string before \
         any fleet API, since that string is what says which conclusion applies: {first_action}"
    );
    assert!(
        first_action.contains("ceiling"),
        "first_action must branch on the ceiling case explicitly -- the reachability/workers \
         path is only correct for a coverage-confirmed escalation: {first_action}"
    );

    // The ceiling's weaker conclusion must be stated, not left implied.
    assert!(
        description.contains("does NOT mean the queue was swept"),
        "the description must state plainly that a ceiling escalation does not support the \
         fleet-exhaustion reading: {description}"
    );
    assert!(
        !description.contains("still found no capable worker"),
        "the description must not assert fleet exhaustion for the whole outcome -- that reading \
         is false for every ceiling trip: {description}"
    );
    assert!(
        !threshold.contains("no live worker on that queue registers the handler"),
        "the threshold must justify paging by executions being failed, not by a fleet \
         conclusion that only some escalations support: {threshold}"
    );

    // Round 15 gave the small-fleet case to the configured-total bound; the
    // ceiling now covers unprovable coverage. The stale claim must not return.
    for (name, text) in [
        ("description", &description),
        ("first_action", &first_action),
        ("default_threshold", &threshold),
    ] {
        assert!(
            !text.contains("fleet smaller than the budget"),
            "{name} still credits the ceiling with the small-fleet case that the \
             configured-total bound has owned since review round 15: {text}"
        );
    }
}

/// The `escalated` metric-label docs must carry the same cause-neutral reading
/// the paging rule does (issue #804, Codex round-44).
///
/// Sibling of [`capability_miss_paging_rule_prose_is_cause_neutral`]: round 43
/// fixed the alert pack and the runbook, but the label constant in
/// `src/telemetry.rs` is the surface a consumer writing a CUSTOM alert reads,
/// and it carried the same false conclusion. Both must state it, so a future
/// edit that repairs one and leaves the other still fails.
///
/// The claim under test: `EscalationCause::outcome_label` maps BOTH
/// `BudgetExhausted` and `ReleaseCeilingExhausted` to `escalated`, and since
/// round 15 the ceiling is reachable ONLY on the two evidence states that mean
/// coverage was never established (`CapablePeerMayExist`, `Unavailable`). So a
/// sample carrying this label does not on its own prove the fleet was swept.
#[test]
fn capability_miss_escalated_label_docs_are_cause_neutral() {
    let doc = rustdoc_above_const("CAPABILITY_MISS_OUTCOME_ESCALATED");

    // The two sentences that assert fleet exhaustion for the WHOLE label.
    assert!(
        !doc.contains("the only escalation cause"),
        "the label is recorded by two causes with different evidential strength, so it cannot \
         be the only one supporting the fleet conclusion: {doc}"
    );
    assert!(
        !doc.contains("no capable worker ever claimed it"),
        "a ceiling escalation fires precisely when a live worker may be capable \
         (`CapablePeerMayExist`) or the registry was unreadable (`Unavailable`), so this is \
         false for that half of the label: {doc}"
    );

    // The weaker cause must be named, and the discriminator handed over.
    assert!(
        doc.contains("ceiling"),
        "the docs must name the ceiling cause explicitly -- a consumer alerting on this label \
         cannot reason about the sample they get otherwise: {doc}"
    );
    assert!(
        doc.contains("reason"),
        "the docs must point consumers at the execution's reason string, which is the only \
         place the actual cause is recorded: {doc}"
    );

    // Round 15 gave the small-fleet case to the configured-total bound.
    assert!(
        !doc.contains("fleet smaller than the budget"),
        "the ceiling has not owned the small-fleet case since review round 15: {doc}"
    );
}

/// The **operator-facing prose** must be cause-neutral too (issue #804, Codex
/// round-47 P2).
///
/// Round 43 corrected the alert rule, round 44 the metric-label constant, and
/// round 47 the runbook overview and the dashboard panel — three rounds of the
/// same claim surviving in a surface the previous round did not touch. So this
/// pins ALL of the operator-reachable prose at once: an on-call engineer reads
/// whichever one their tooling put in front of them, and repairing one while
/// leaving another still sends them toward a missing-deploy investigation when
/// the real cause is an untried worker.
///
/// The claim is only false for the `ReleaseCeilingExhausted` bound, which fires
/// under `CapablePeerMayExist` / `Unavailable` — where a live, untried worker
/// may well be capable. `BudgetExhausted` with confirmed coverage does support
/// it, which is why the fix is to *condition* the claim rather than delete it.
#[test]
fn capability_miss_escalation_prose_is_cause_neutral() {
    let runbook = fs::read_to_string(workspace_path("docs/runbooks/harvest-alerts.md"))
        .expect("failed to read the alerts runbook");
    let dashboard = fs::read_to_string(workspace_path("docs/dashboards/starter-pack-v0.1.0.json"))
        .expect("failed to read the dashboard pack");

    for (surface, text) in [("runbook", &runbook), ("dashboard", &dashboard)] {
        // The exact unconditional forms rounds 43/44/47 each had to remove.
        for banned in [
            "Escalation means **no live worker on that queue
registers the handler at all**",
            "the redelivery budget was exhausted: NO live worker on that queue registers the handler",
        ] {
            assert!(
                !text.contains(banned),
                "{surface}: `outcome=\"escalated\"` is recorded by two bounds with different \
                 evidential strength, so it must not assert fleet exhaustion unconditionally. \
                 Condition it on the bound (the reason string names which) rather than \
                 restoring: {banned}"
            );
        }
        // And it must actively point the reader at the discriminator, or a
        // reader who never reaches the cause table draws the old conclusion by
        // default.
        assert!(
            text.contains("reason") && (text.contains("bound") || text.contains("cause table")),
            "{surface}: the escalation prose must direct the operator to the \
             `no_capable_worker:` reason string before they conclude anything about the fleet"
        );
    }
}

/// The escalation writes **no** dead-letter row, and no doc may say otherwise
/// (issue #804, Codex round-47 P2).
///
/// `escalate_capability_miss` routes through `fail_task_and_execution_with_history`,
/// which fails the task and the execution without inserting into
/// `harvest_dead_letters` — asserted end to end by the DB test
/// `capability_miss_escalates_after_the_budget_with_no_capable_worker`. Naming a
/// DLQ destination anywhere sends an operator to an empty recovery surface
/// during exactly the incident this feature exists to make legible.
///
/// The knob's rustdoc is **mirrored across three files**, and round 47 corrected
/// only `effective_config.rs` — scoping this guard to that one file is what let
/// the stale wording survive in the other two until Codex round 52. Every
/// surface an operator can read the knob from is checked here, so the class
/// cannot recur on whichever copy the next edit misses.
#[test]
fn capability_miss_escalation_is_never_documented_as_dead_lettering() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let surfaces = [
        // The operator-facing effective-config projection (`GET /admin/config`).
        ("src/effective_config.rs", true),
        // The public builder knob an embedder sets.
        ("src/builder.rs", true),
        // The internal runtime mirror. Terse by convention — it carries no
        // positive rule-out, so only the destination constructions are banned.
        ("src/worker.rs", false),
    ];

    for (path, requires_positive_rule_out) in surfaces {
        let source = fs::read_to_string(manifest.join(path))
            .unwrap_or_else(|e| panic!("failed to read {path}: {e}"));
        let doc = rustdoc_above_field(&source, "capability_miss_max_redeliveries");
        // Ban the DESTINATION constructions, not the token: these docs
        // legitimately mention the DLQ in order to rule it out, and a bare
        // `contains("dlq")` cannot tell a negation from a claim.
        for banned in [
            "/ DLQ path",
            "DLQ path",
            "to the DLQ with",
            "dead-letter path",
        ] {
            assert!(
                !doc.contains(banned),
                "{path}: the capability-miss escalation writes no `harvest_dead_letters` \
                 row (`fail_task_and_execution_with_history` inserts none), so this field \
                 must not name a DLQ destination -- it would send an operator to an empty \
                 recovery surface mid-incident. Found {banned:?} in: {doc}"
            );
        }
        // And the operator-facing surfaces must rule the DLQ out POSITIVELY.
        // Silence is not enough: an operator who has just read the poison-pill
        // knob (which really does quarantine, #367) will otherwise assume this
        // one behaves the same way.
        if requires_positive_rule_out {
            assert!(
                doc.contains("No dead-letter row is written"),
                "{path}: the field must state outright that no dead-letter row is written, \
                 so an operator diagnosing an exhausted budget queries failed workflows: {doc}"
            );
        }
    }
}

/// The capability-miss fleet lookup uses a **floored** liveness window, and no
/// doc may claim it shares the poison-pill one (issue #804, Codex round-48 P2).
///
/// `capability_miss_fleet_stale_secs` is
/// `max(2 × worker_heartbeat_interval, CAPABILITY_MISS_MIN_FLEET_STALE_SECS)`,
/// and the floor is 120 s — so at the default 5 s cadence the capability window
/// is 120 s where the poison-pill reclaimer's is 10 s, a 12× divergence that
/// round 19 introduced deliberately (judging *peers* whose cadence you cannot
/// read needs a fleet-wide bound, not your own). An operator who reads
/// "the same window" mispredicts when the configured-total bound becomes
/// available: a pod silent for 30 s is long gone from poison-pill reclamation
/// but still holds capability evidence at `CapablePeerMayExist`.
#[test]
fn capability_miss_fleet_window_is_never_documented_as_the_poison_pill_window() {
    let builder = fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/builder.rs"))
        .expect("failed to read src/builder.rs");
    let surfaces = [
        (
            "builder.rs",
            rustdoc_above_field(&builder, "capability_miss_max_redeliveries"),
        ),
        (
            "safe-deploy runbook",
            fs::read_to_string(workspace_path("docs/runbooks/safe-deploy.md"))
                .expect("failed to read the safe-deploy runbook"),
        ),
        (
            "alerts runbook",
            fs::read_to_string(workspace_path("docs/runbooks/harvest-alerts.md"))
                .expect("failed to read the alerts runbook"),
        ),
        // The alert pack itself. Round 48 corrected the two runbooks and the
        // builder knob but not the machine-readable pack an operator reads
        // *first* — straight off the firing alert — which is how the stale
        // claim survived to Codex round 53.
        (
            "alert pack",
            fs::read_to_string(workspace_path("docs/alerts/starter-pack-v0.1.0.json"))
                .expect("failed to read the alert pack"),
        ),
    ];

    for (surface, text) in &surfaces {
        // Normalize the two ways this claim can be spelled before matching.
        // The prose surfaces write a backticked `2 × worker_heartbeat_interval`
        // (Unicode multiplication sign); the JSON pack writes a bare ASCII
        // `2x worker_heartbeat_interval`. A banned-substring list written for
        // one spelling silently passes the other, which is the second half of
        // why the pack was missed.
        let flat = squeeze_whitespace(text)
            .replace('×', "x")
            .replace('`', "")
            .replace("2 x ", "2x ");
        // Ban the SAMENESS construction, not the window itself: a correct doc
        // may well name 2x worker_heartbeat_interval in order to say the
        // capability lookup is *not* that value.
        for banned in [
            "same 2x worker_heartbeat_interval liveness window as",
            "same 2x worker_heartbeat_interval window as",
            "same 2x worker_heartbeat_interval freshness window",
            "same freshness window the poison-pill reclaimer uses",
            "same liveness window the poison-pill reclaimer uses",
        ] {
            assert!(
                !flat.contains(banned),
                "{surface}: the capability-miss fleet lookup floors its liveness window at \
                 `CAPABILITY_MISS_MIN_FLEET_STALE_SECS` (120 s), so it is NOT the poison-pill \
                 window -- at the default 5 s cadence they are 120 s and 10 s. Saying they \
                 match makes escalation timing unpredictable. Found: {banned:?}"
            );
        }
        // And it must state the floor, or a reader still derives the window
        // from their configured cadence and gets the wrong answer.
        assert!(
            flat.contains("floored at 120 s"),
            "{surface}: must state that the capability-miss fleet-liveness window is \
             floored at 120 s, so an operator can predict when the configured-total bound \
             becomes available after a pod dies"
        );
    }
}

/// `CapablePeerMayExist` withholds **both** evidence-derived bounds, and no doc
/// may claim the distinct-worker one survives (issue #804, Codex round-48).
///
/// `capability_miss_decision` gates the distinct bound on
/// `!CapablePeerMayExist` and the configured-total bound on
/// `AllLiveWorkersMissed` specifically, so a live untried peer withholds them
/// both and only the **ungated** bounds remain — chiefly the absolute `10 ×`
/// release ceiling. Two rustdocs in `worker.rs` once disagreed about this and
/// the wrong one reached three operator runbooks: an operator who believes the
/// distinct bound still fires expects escalation after a handful of distinct
/// incapable workers, when in fact nothing terminates the task until the
/// ceiling — roughly an order of magnitude more redeliveries.
#[test]
fn capable_peer_may_exist_is_never_documented_as_withholding_only_one_bound() {
    let worker = fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/worker.rs"))
        .expect("failed to read src/worker.rs");
    let builder = fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/builder.rs"))
        .expect("failed to read src/builder.rs");
    let surfaces = [
        ("worker.rs", worker),
        ("builder.rs", builder),
        (
            "safe-deploy runbook",
            fs::read_to_string(workspace_path("docs/runbooks/safe-deploy.md"))
                .expect("failed to read the safe-deploy runbook"),
        ),
        (
            "alerts runbook",
            fs::read_to_string(workspace_path("docs/runbooks/harvest-alerts.md"))
                .expect("failed to read the alerts runbook"),
        ),
    ];

    for (surface, text) in &surfaces {
        let flat = squeeze_whitespace(text);
        for banned in [
            "the distinct-worker bound and the absolute ceiling still fire",
            "the distinct-worker bound and the absolute release ceiling still fire",
            "only the fleet-covering bound is withheld",
            "only this fleet-covering bound is withheld",
            "withholds only the configured-total bound",
        ] {
            assert!(
                !flat.contains(banned),
                "{surface}: `CapablePeerMayExist` withholds BOTH evidence-derived bounds -- \
                 `capability_miss_decision` gates the distinct bound on `!CapablePeerMayExist` \
                 and the total bound on `AllLiveWorkersMissed`. Saying the distinct bound \
                 survives understates the wait by roughly the ceiling multiplier. Found: \
                 {banned:?}"
            );
        }
    }
}

/// Collapses every whitespace run to a single space so a prose assertion
/// survives a reflow. Line breaks are a formatting artifact; the claim is not.
fn squeeze_whitespace(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Reads the contiguous `///` block immediately above a named `pub` struct
/// field. Walks BACKWARD like [`rustdoc_above_const`], for the same reason.
fn rustdoc_above_field(source: &str, field: &str) -> String {
    let lines: Vec<&str> = source.lines().collect();
    let decl = lines
        .iter()
        .position(|line| line.trim_start().starts_with(&format!("pub {field}:")))
        .unwrap_or_else(|| panic!("`pub {field}` must exist in the scanned source"));
    let mut doc: Vec<&str> = Vec::new();
    for line in lines[..decl].iter().rev() {
        let trimmed = line.trim_start();
        if !trimmed.starts_with("///") {
            break;
        }
        doc.push(trimmed.trim_start_matches("///").trim());
    }
    assert!(!doc.is_empty(), "`{field}` must carry a rustdoc block");
    doc.reverse();
    doc.join(" ")
}

/// Reads the contiguous `///` block immediately above a `pub const` in
/// `src/telemetry.rs`.
///
/// Walking BACKWARD from the declaration is what makes this reformat-tolerant:
/// it needs no fixed line numbers and no assumption about how the doc wraps.
fn rustdoc_above_const(name: &str) -> String {
    let source = fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/telemetry.rs"))
        .expect("failed to read src/telemetry.rs");
    let lines: Vec<&str> = source.lines().collect();
    let decl = lines
        .iter()
        .position(|line| line.starts_with(&format!("pub const {name}:")))
        .unwrap_or_else(|| panic!("`pub const {name}` must exist in src/telemetry.rs"));

    let mut doc: Vec<&str> = Vec::new();
    for line in lines[..decl].iter().rev() {
        let trimmed = line.trim_start();
        if !trimmed.starts_with("///") {
            break;
        }
        doc.push(trimmed.trim_start_matches("///").trim());
    }
    assert!(!doc.is_empty(), "`{name}` must carry a rustdoc block");
    doc.reverse();
    doc.join(" ")
}

/// Half 2 of the capability-miss alert-shape pin (issue #804).
///
/// Split from [`capability_miss_released_outcome_never_pages`] along the seam
/// its doc already describes: half 1 pins WHICH outcomes may page, this pins
/// that the released-outcome rule HOLDS rather than firing on a single sample.
/// They are independent properties over different rules, so a failure in one
/// should not mask the other.
#[test]
fn capability_miss_release_sustained_rule_holds_for_a_full_window() {
    // Half 2: the sustained-release rule must hold, not fire on a single sample.
    let sustained = capability_miss_rule_exprs("harvest_capability_miss_release_sustained");
    let expr = sustained
        .first()
        .expect("the sustained-release rule must carry an expression");
    assert!(
        expr.contains("outcome=\"released\""),
        "the sustained-release rule must select the released outcome: {expr}"
    );
    assert!(
        expr.contains("min_over_time(") && expr.contains(":1m]"),
        "the hold must live in the expression (the pack schema has no `for:` field): a \
         `min_over_time` over a SUBQUERY -- asserting the rate was non-zero at EVERY step -- \
         is what distinguishes `still skewed` from `one release happened`. The `:1m]` step is \
         what makes it a subquery rather than a plain range vector (`min_over_time(x[15m])` \
         would take the min over raw samples, not over per-step rates); the window LENGTH is \
         pinned separately in Half 2c: {expr}"
    );
    assert!(
        !expr.contains("increase("),
        "an increase(...[15m]) > 0 form fires on a single release anywhere in the window -- i.e. \
         on every routine deploy -- which is exactly what this rule exists not to do: {expr}"
    );

    // Half 2b: `min_over_time` alone does NOT deliver the 15m hold it looks
    // like it does. Prometheus range functions skip subquery steps that have no
    // sample rather than treating them as zero, so on a series that was CREATED
    // by this very deploy -- the exact scenario, since a brand-new
    // (queue, task_type) capability-miss series appears the first time a new
    // handler is rolled out -- the earlier steps are simply absent and the min
    // is taken over only the few samples that exist. A routine deploy with one
    // release burst then satisfies `== 1` within ~3 minutes (a 5m `rate` stays
    // positive for 5m after the last release), firing the very ticket this rule
    // was split out to suppress.
    //
    // The window must therefore also be asserted PRESENT, not just positive.
    assert!(
        expr.contains("count_over_time("),
        "min_over_time over a subquery silently ignores steps with no sample, so a \
         newly-created series can satisfy it in minutes; the expression must also \
         require the window to be present (count_over_time(...)): {expr}"
    );
    // Half 2c: the presence guard must actually deliver 15 minutes, and must
    // stay satisfiable at every evaluation offset (issue #804, Codex round-41).
    //
    // Prometheus aligns a subquery's steps to absolute multiples of the
    // resolution, so `expr[Rm:1m]` yields `R` or `R + 1` points depending on
    // whether the evaluation instant happens to land on a minute boundary. Two
    // consequences, and they pull in opposite directions:
    //
    //   * `[15m:1m] >= 15` is satisfiable by 15 points, which span only 14
    //     minutes — so it opens the ticket a minute before the hold it promises.
    //   * `[15m:1m] >= 16` would demand the aligned case, so on a rule group
    //     evaluating off a minute boundary the count is 15 and the alert could
    //     never fire at all. Tightening the count on a 15m window is therefore
    //     the wrong repair — strictly worse than the bug.
    //
    // Widening the window instead fixes both: `[16m:1m]` yields at least 16
    // points at every alignment, and 16 points one minute apart span a full 15
    // minutes. So `>= 16` over a 16m window is both honest and always reachable.
    assert!(
        expr.contains("[16m:1m]"),
        "the hold must be measured over a 16m subquery: subquery steps are aligned \
         to absolute minute boundaries, so a 15m window yields 15 OR 16 points and \
         cannot both guarantee 15 minutes and stay satisfiable at every evaluation \
         offset: {expr}"
    );
    assert!(
        expr.contains(">= 16"),
        "the presence guard must require 16 one-minute steps -- the minimum a 16m \
         subquery yields at any alignment, and exactly the count that spans a full \
         15 minutes; anything less is not the advertised hold: {expr}"
    );
    assert!(
        !expr.contains("[15m:1m]"),
        "the 15m subquery is the off-by-one this guard exists to prevent -- its \
         `>= 15` is satisfied by 15 points spanning only 14 minutes: {expr}"
    );
    // Both halves are load-bearing and must be ANDed, not alternatives.
    assert!(
        expr.contains(" and "),
        "the positivity and presence guards must both hold: {expr}"
    );
}

/// `increase(...) > 0` cannot see the FIRST sample of a brand-new series, so
/// both escalation rules must also carry set-difference detection (issue #804).
///
/// Split out of `capability_miss_released_outcome_never_pages` because it pins a
/// different property: that one is about which *outcome* may page, this one is
/// about whether the expression can observe the outcome at all.
#[test]
fn capability_miss_escalation_rules_detect_a_brand_new_series() {
    let pack = read_pack();
    let rules = pack["rules"].as_array().expect("rules must be an array");

    let rule_exprs = |id: &str| -> Vec<String> {
        rules
            .iter()
            .find(|rule| rule["id"].as_str() == Some(id))
            .unwrap_or_else(|| panic!("{id} alert must exist"))["prometheus"]["expressions"]
            .as_array()
            .unwrap_or_else(|| panic!("{id} must carry PromQL expressions"))
            .iter()
            .filter_map(|expr| expr["expr"].as_str())
            .map(ToOwned::to_owned)
            .collect()
    };

    // `increase(...) > 0` cannot see the FIRST sample of a new series.
    //
    // The adapter creates a counter by incrementing it, so a
    // (queue, task_type, outcome) series that has never fired appears in the
    // scrape already at 1 -- there is no preceding zero sample. `increase`
    // needs two points and reports `last - first`, so every point in the
    // window reads 1 and the delta is 0. The first escalation on a series is
    // therefore invisible, and if it is also the ONLY one, the rule never
    // fires at all.
    //
    // That is exactly the scenario these two rules exist for: a new handler
    // rolls out, no worker registers it, and the very first escalated task
    // creates the series. A low-volume queue can escalate exactly once.
    //
    // Zero-initialising the label sets at worker startup is the usual remedy
    // and is NOT sufficient here: the zero must be *scraped* before the
    // increment, and both never-offered causes (a
    // `capability_miss_max_redeliveries = 0` rollback switch, and a
    // session-pinned task) escalate on the task's FIRST claim, which can land
    // inside the same scrape interval as worker startup. The detection has to
    // live in the expression.
    for (id, outcome) in [
        ("harvest_no_capable_worker", "escalated"),
        (
            "harvest_capability_miss_never_offered",
            "escalated_never_offered",
        ),
    ] {
        let exprs = rule_exprs(id);
        let joined = exprs.join(" ");
        assert!(
            joined.contains("offset 5m"),
            "{id} must also detect a series that did not exist one window ago, or the              first escalation on a brand-new (queue, task_type) series is invisible to              `increase`: {exprs:?}"
        );
        assert!(
            joined.contains("unless"),
            "{id}'s new-series detection must be an `unless ... offset` set difference              (present now, absent a window ago), not a value comparison: {exprs:?}"
        );
        assert!(
            joined.contains(&format!("outcome=\"{outcome}\"")),
            "{id}'s new-series arm must select its own outcome: {exprs:?}"
        );
        // The set difference must compare against a RANGE, not a bare instant
        // `offset 5m` (Codex round-14 P2). A bare offset is empty whenever the
        // target was unscrapeable for the whole lookback, so a monitoring gap
        // re-selects an unchanged counter as "new" and pages spuriously. Asking
        // whether ANY sample exists in the preceding hour tells a genuinely
        // created series apart from a scrape or remote-write outage.
        assert!(
            joined.contains("[1h] offset 5m"),
            "{id}'s new-series arm must look back over a RANGE (`[1h] offset 5m`), not a               bare instant `offset 5m`: a bare offset makes a scrape gap indistinguishable               from a brand-new series and pages on an unchanged counter: {exprs:?}"
        );
        // Both sides of the `unless` must be wrapped identically. A bare
        // instant selector on the left and `max_over_time` on the right do not
        // agree on whether `__name__` survives; a mismatch makes `unless` match
        // nothing, which silently turns the arm into an unconditional `M > 0`.
        assert_eq!(
            joined
                .matches("max_over_time(harvest_task_capability_miss_total")
                .count(),
            2,
            "{id}'s new-series arm must wrap BOTH sides of the `unless` in `max_over_time`               so the label sets stay symmetric -- an asymmetric wrapper makes `unless` match               nothing and degrades the arm to an unconditional `> 0`: {exprs:?}"
        );
    }
}

fn read_pack() -> Value {
    let contents = read_doc("docs/alerts/starter-pack-v0.1.0.json");
    serde_json::from_str(&contents).expect("starter alert pack must be valid JSON")
}

fn read_doc(relative: &str) -> String {
    fs::read_to_string(workspace_path(relative))
        .unwrap_or_else(|error| panic!("failed to read {relative}: {error}"))
}

fn workspace_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join(relative)
}

fn harvest_metric_tokens(expr: &str) -> Vec<String> {
    expr.split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_')
        .filter(|token| token.starts_with("harvest_"))
        .map(ToOwned::to_owned)
        .collect()
}

fn markdown_section<'a>(document: &'a str, heading: &str) -> Option<&'a str> {
    let marker = format!("## {heading}");
    let start = document.find(&marker)?;
    let after_start = start + marker.len();
    let end = document[after_start..]
        .find("\n## ")
        .map_or(document.len(), |relative| after_start + relative);
    Some(&document[start..end])
}

/// A capability-miss alert must never send a responder to `/dead-letters`
/// (issue #804, Codex round-54 P2).
///
/// Escalation routes through `fail_task_and_execution_with_history`, which
/// inserts no `harvest_dead_letters` row, so that endpoint physically cannot
/// surface the affected execution or its `no_capable_worker:` reason. A
/// `management_checks` entry naming it hands the on-call an empty result set
/// mid-incident — worse than saying nothing, because an empty DLQ reads as
/// evidence the alert was spurious.
///
/// This is the *endpoint* half of the round-47/52 prose guard: those ban a
/// dead-letter DESTINATION in rustdoc, and a check-list entry saying
/// "GET /dead-letters" trips neither of them. The failed-workflows lookup the
/// sibling `harvest_capability_miss_never_offered` rule already uses is the
/// correct target, so this also keeps the two rules consistent.
///
/// Bans the DIRECTIVE (`GET .../dead-letters`), not the token: a check may
/// legitimately *mention* `/dead-letters` in order to rule it out, and a bare
/// `contains("/dead-letters")` cannot tell a negation from a referral. That is
/// the same distinction the round-47 prose guard draws, and writing this one
/// naively tripped on its own corrected text during development.
#[test]
fn capability_miss_alerts_never_send_responders_to_the_dead_letter_endpoint() {
    let pack: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(workspace_path("docs/alerts/starter-pack-v0.1.0.json"))
            .expect("failed to read the alert pack"),
    )
    .expect("alert pack must be valid JSON");

    let rules = pack["rules"]
        .as_array()
        .expect("alert pack must carry a rules array");
    let capability_rules: Vec<&serde_json::Value> = rules
        .iter()
        .filter(|r| {
            r["id"].as_str().is_some_and(|id| {
                id.starts_with("harvest_capability_miss") || id == "harvest_no_capable_worker"
            })
        })
        .collect();
    assert!(
        !capability_rules.is_empty(),
        "expected at least one capability-miss rule; the filter must have drifted \
         from the rule ids, which would make this guard vacuous"
    );

    for rule in capability_rules {
        let id = rule["id"].as_str().unwrap_or("<unknown>");
        for check in rule["management_checks"]
            .as_array()
            .map(Vec::as_slice)
            .unwrap_or_default()
        {
            let text = check.as_str().unwrap_or_default();
            assert!(
                !["GET /api/harvest/dead-letters", "GET /dead-letters"]
                    .iter()
                    .any(|directive| text.contains(directive)),
                "{id}: capability-miss escalation writes no dead-letter row, so this \
                 management check sends the on-call to an endpoint that cannot surface \
                 the affected execution. Use the failed-workflows lookup \
                 (GET /api/harvest/workflows?state=FAILED) instead. Found: {text:?}"
            );
        }
    }
}
