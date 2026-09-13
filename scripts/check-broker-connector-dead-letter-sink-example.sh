#!/usr/bin/env bash
# Fails if docs/getting-started/13-broker-connectors.md's "record into
# harvest" example installs `RecordingDeadLetterSink` instead of
# `PostgresDeadLetterSink`.
#
# Mechanism this guards against: the chapter's "Two ways to satisfy it"
# section gives two production-facing options for a directly-constructed
# `ConnectorRuntime` (the "Testing without a broker" section above it,
# correctly, uses `RecordingDeadLetterSink` for its no-Docker unit test).
# Option (a), labeled "record into harvest — the default mode", used to read:
#
#   let runtime = ConnectorRuntime::new(/* ... */)
#       .with_dead_letter_sink(Arc::new(RecordingDeadLetterSink::new()));
#
# `RecordingDeadLetterSink`'s own doc comment
# (autumn-harvest-plugin/src/connector/dead_letter.rs) says it "records into
# memory, exported for tests and local development" — its `write()` only
# pushes into an in-process `Mutex<Vec<...>>`; nothing reaches Postgres. The
# sink that actually writes `harvest_connector_dead_letters` is
# `PostgresDeadLetterSink`, which is what `HarvestPlugin` itself installs
# (autumn-harvest-plugin/src/plugin.rs) as the very "default mode" this
# example claims to reproduce.
#
# A newcomer embedding a runtime directly (the exact audience this section
# addresses) who copies option (a) verbatim gets code that compiles and runs
# with no error: poison messages are acknowledged, so the binding makes
# progress. But they are never durably recorded — an in-process restart
# loses them all, and every triage query the same chapter documents
# (`SELECT ... FROM harvest_connector_dead_letters ...`) returns nothing.
# Silent data loss in the one code path whose entire purpose is "a poison
# message must never be silently lost."
#
# Usage: ./scripts/check-broker-connector-dead-letter-sink-example.sh

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

doc="docs/getting-started/13-broker-connectors.md"

if [ ! -f "$doc" ]; then
  echo "$doc: not found" >&2
  exit 1
fi

# Look at the few lines right after the "(a) record into harvest" label —
# that is the example this guard is about, not the "Testing without a
# broker" example above it, which correctly uses RecordingDeadLetterSink.
window="$(grep -A 3 -F '// (a) record into harvest' "$doc")"

if [ -z "$window" ]; then
  echo "$doc: could not find the '// (a) record into harvest' example;" \
    "has the chapter been restructured? Update this guard to match." >&2
  exit 1
fi

if grep -q "RecordingDeadLetterSink" <<<"$window"; then
  echo "$doc: the '(a) record into harvest — the default mode' example" \
    "installs RecordingDeadLetterSink, an in-memory, test-only sink" \
    "(see its doc comment in" \
    "autumn-harvest-plugin/src/connector/dead_letter.rs). It never writes" \
    "harvest_connector_dead_letters, so a runtime built from this example" \
    "silently loses every poison message on restart." >&2
  echo >&2
  echo "Fix: use PostgresDeadLetterSink::new(pool) — the sink HarvestPlugin" \
    "itself installs as the real default (autumn-harvest-plugin/src/plugin.rs)." >&2
  exit 1
fi

if ! grep -q "PostgresDeadLetterSink" <<<"$window"; then
  echo "$doc: the '(a) record into harvest — the default mode' example" \
    "no longer names PostgresDeadLetterSink. Update this guard to match" \
    "the current example." >&2
  exit 1
fi

# The name must actually resolve. This example is its own fenced block with
# no `use` statement of its own (unlike the "Testing without a broker"
# example above it), so a bare `PostgresDeadLetterSink::new(` here is an
# unresolved name for anyone assembling the snippet as shown — caught live
# in PR #1499 review, where the first fix used the bare name and the shared
# import block above only imports RecordingDeadLetterSink.
if grep -qF "PostgresDeadLetterSink::new(" <<<"$window" \
  && ! grep -qF "autumn_harvest_plugin::connector::PostgresDeadLetterSink" <<<"$window"; then
  echo "$doc: the example calls PostgresDeadLetterSink::new(...) by its bare" \
    "name, but this fenced block has no 'use' statement importing it (the" \
    "shared import block in the 'Testing without a broker' example above" \
    "only imports RecordingDeadLetterSink), so the name does not resolve." >&2
  echo >&2
  echo "Fix: qualify it as" \
    "autumn_harvest_plugin::connector::PostgresDeadLetterSink::new(...)." >&2
  exit 1
fi

echo "OK: the 'record into harvest' example installs a resolvable PostgresDeadLetterSink."
