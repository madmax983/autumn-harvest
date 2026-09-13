#!/usr/bin/env bash
# Reproduction harness for the external-outbox scan indexes and the paired
# claim-query rewrite (issue #1486), documented in
# `docs/performance-external-outbox-scan.md`.
#
# The evidence-capture test
# (`external_outbox_scan_tests::zz_capture_external_outbox_scan_evidence`)
# seeds the issue's fixture once, then captures two full drains of the same
# outbox against it:
#
#   before  the pre-fix claim query, with the four candidate indexes dropped.
#           The test drops them unconditionally, so it reproduces the baseline
#           whether or not
#           `20260911213344_harvest_external_outbox_scan_indexes` has already
#           run against the target database.
#   after   the rewritten claim query, with the four indexes built from that
#           migration's own SQL.
#
# Both drains resolve the same request set. The test asserts that, so a
# capture that changed which rows the scanner claims fails rather than
# publishing a number.
#
# Usage (works with or without HARVEST_TEST_DATABASE_URL -- see below):
#   HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
#     ./autumn-harvest/scripts/external_outbox_scan_perf_repro.sh
#
#   # or, with only a reachable Docker daemon and no external Postgres:
#   ./autumn-harvest/scripts/external_outbox_scan_perf_repro.sh
#
# `HARVEST_TEST_DATABASE_URL`, when set, is treated as an ADMIN URL, exactly
# as `claim_bench_support.rs` treats it elsewhere in this crate: the harness
# creates, migrates, seeds, measures and drops a fresh uniquely-named database
# per run. When unset, `claim_bench_support::db::setup_bench_db` falls back to
# a testcontainer automatically. If neither an external database nor a Docker
# daemon is reachable, the capture test SKIPs loudly instead of producing
# artifacts.
#
# Writes into `docs/perf-artifacts/external-outbox-scan/`:
#   {before,after}.explain.txt
#     `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)` for one cold
#     claim of each form.
#   {before,after}.pg_stat_statements.txt
#     A `pg_stat_statements` snapshot covering the whole drain of each form,
#     scoped to this run's own database.
#
# Preconditions: a Rust toolchain that can build this crate, and either Docker
# or a reachable Postgres named by `HARVEST_TEST_DATABASE_URL` with
# `pg_stat_statements` in `shared_preload_libraries`. The harness creates the
# extension itself, but the C hooks only exist once the library is preloaded
# at postmaster start, so a server without it reports zero rows.
set -euo pipefail

cd "$(dirname "$0")/../.."

exec cargo test -p autumn-harvest --features db --test integration \
    zz_capture_external_outbox_scan_evidence -- --ignored --nocapture
