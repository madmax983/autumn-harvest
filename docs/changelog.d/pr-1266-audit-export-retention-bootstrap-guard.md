## Phase — audit-export retention bootstrap-window guard (issue #1266)

`purge_old_audit_records` already refused to delete an unexported audit row
when a live cursor existed for the shard, or when the sweeping process had a
sink configured. Issue #1266 (a Codex review round 6 follow-up on PR #1261)
found both signals could be absent at once in a split web/worker deployment:
before the worker's first successful tick on a shard, the process running
retention has no sink and no cursor row to read. A retention sweep landing in
that window deleted every retention-aged audit row, including rows the
exporter had not shipped yet.

Adds a third, explicit signal: `RetentionConfig::protect_unexported_audit`
(default `None`, disabled). Set the same way on every process in a split
deployment, it closes the window from the moment export is configured rather
than from the moment the exporter's first tick succeeds — the two cheaper
fixes the issue considered and rejected (seeding the cursor row in a
migration; letting the retention process create it) both fail for the same
reason: a shard's own database cannot know whether an exporter is coming,
and the retention process does not know either.

`purge_old_audit_records` gained a third parameter, `protect_unexported_audit:
bool`, OR'd flatly with the existing `is_configured()` signal — no schema
change. A first draft instead scoped the flag to `NOT EXISTS(any cursor row)`,
so a retired cursor always overrode it; a Codex review (round 1 P1) caught
that this reopened the exact bootstrap window for a shard being **re-enabled**
after decommission, since its cursor stays retired until the worker's first
new tick, and is indistinguishable from a shard meant to stay decommissioned.
The flag now shares `is_configured`'s existing "both steps required" trade
instead: decommissioning a shard does not resume purging there while either
signal stays `true` on the sweeping process, an operational cost documented
next to the pre-existing one for `is_configured`.

The same review round found a second, independent defect in the per-row
pending check: a row already stamped with an `export_seq` was treated as
already acknowledged whenever no cursor row existed at all — exactly backward
from `ensure_cursor_row`'s own handling of a cursor lost to a manual `DELETE`
or a partial restore, which rebuilds it at `last_acked_seq = 0` so every
stamped row is redelivered. The pending check now also treats "no cursor row
for the shard" as pending, matching that rebuild.

A further review round (P1) found that a single process-wide boolean cannot
represent a fleet with more than one shard: decommissioning shard A requires
disabling the flag, which simultaneously strips bootstrap protection from
shard B if B is mid-bootstrap on the same sweep. `RetentionConfig` changed
`protect_unexported_audit` from `bool` to `Option<BTreeSet<ShardId>>` —
`None` disables it, `Some(exempt)` protects every shard not in `exempt`. The
retention sweep now computes the per-shard bool itself
(`RetentionConfig::protects_unexported_audit`) instead of passing one flag
to every shard; `purge_old_audit_records`'s own signature is unaffected,
since it already took a plain `bool` per call. `with_protect_unexported_audit`
keeps its `bool` shape for the common case; a new
`excluding_shard_from_protect_unexported_audit` adds the exemption.

A fourth review round (P1) found that the per-shard fix above was still
unsafe under a supported topology: two logical shards can alias one
physical pool (`ShardedDbPool::from_map`, a pre-split staging shape).
`purge_old_audit_records` issues one unscoped `DELETE` per call, so calling
it once per logical shard let two aliased shards apply two different
decisions to the same physical audit table within one tick — a less
protective decision could commit before a more protective one ever ran.
The sweep now groups shards by pool identity first (`Pool::manager()`
returns a reference into the pool's shared `Arc` allocation, so `ptr::eq`
on it detects aliasing safely, with no private field or unsafe code), and
combines each group's decision with `any`: protect the shared pool
whenever any aliased shard wants protection. This also purges each
physical pool exactly once per tick instead of once per logical shard.

A fifth review round (P1) found the fourth round's fix incomplete:
`ShardedDbPool::from_dsns` builds a separate `Pool` object per shard entry
even when two entries carry the same DSN, so `ptr::eq` on `Pool::manager()`
never sees the alias. `ShardedDbPool` now records each shard's pool-group
number at construction time and exposes it via `pool_groups()`.
`from_map` still groups by `Pool::manager()` identity. `from_dsns` groups
by the DSN string instead, compared before each string is consumed into a
manager. The retention sweep's own grouping logic moved into
`ShardedDbPool::pool_groups()`, so both callers share one grouping.

A sixth review round (P1) found the DSN-string comparison itself too
narrow: two DSNs reaching one physical database can differ in
credentials, an explicit default port, or extra connection parameters,
none of which change which database a connection reaches. `from_dsns`
now compares a canonical key instead — host (lowercased), port (defaulted
to 5432), path, and query string — dropping only credentials, which
never change which database or schema a connection reaches. A DSN that
does not parse as a URL falls back to the raw string, the prior behavior.
A host alias (two hostnames resolving to one address) stays undetected by
design: resolving it needs a DNS lookup, and building a pool must stay a
pure, local operation with no network access.

A seventh review round (P2) caught a mistake in the sixth round's first
version of this key: it dropped the query string along with credentials.
A `?options=-c search_path=...` parameter selects which schema
`harvest_audit_log` resolves to, so two DSNs differing only there could
reach different data yet still collapse into one group. `pool_groups()`
returns one representative pool per group, so the collapsed-away shard's
audit table would silently stop being purged at all — the opposite
failure from the rest of this PR, which is about purging too early, not
too rarely. The key now keeps the query string verbatim.

An eighth review round produced two findings pulling in opposite
directions on the same line. A P1 finding noted that keeping the whole
query string reintroduced the original problem for parameters that carry
no schema meaning: two DSNs differing only in `application_name` or
`sslmode` would no longer collapse, so an exempt shard's unprotected pass
could again run before a colocated protected shard's pass. A P2 finding
argued the opposite for username: dropping it can combine two roles whose
own `search_path` (or PostgreSQL's default, which includes the
connecting user's own schema) differ, again risking the silent
never-purged case above.

`canonical_dsn_key` now keeps only the `options` query parameter — the
one libpq mechanism that can carry `-c search_path=...` — and drops every
other query parameter, including credentials. The username finding is
documented as an accepted, unfixed gap rather than chased further: a
role's own server-side `search_path` is invisible in the DSN regardless
of username, so keeping the username would not fully close the gap; it
would only reopen the sixth round's original bug, since a documented
`from_dsns` use (`harvest shard rebalance`, issue #964) targets one
database under different usernames. Between an accepted, narrow,
documented gap and reopening a P1-severity bug this PR exists to close,
the gap stays.

A ninth review round (P2) found a case the `options`-only key still
missed: a Unix-socket DSN carries no host in its URI authority at all.
libpq instead reads the real endpoint from a `host` or `hostaddr` query
parameter (`postgresql:///harvest?host=%2Frun%2Fpg`), which the key had
never inspected, so two DSNs naming different sockets could still
collapse into one group. `canonical_dsn_key` now falls back to a `host`
or `hostaddr` query parameter when the authority host is empty, or to
`hostaddr` whenever it is given at all, matching libpq's own precedence
between the two. A `port` query parameter is honored the same way.

A tenth review round found two more gaps in the same host-resolution
fix. First (P2): a resolved host was always lowercased, which is correct
for a DNS name but wrong for a Unix-socket path — `/run/PG-A` and
`/run/pg-a` name different sockets on a case-sensitive filesystem, so
lowercasing them could falsely merge two different databases.
`canonical_dsn_key` now lowercases a resolved host only when it does not
start with `/`. Second (P2): a DSN with no path was treated as naming no
database, but libpq defaults an omitted `dbname` to the connecting
username, so two DSNs with no path but different usernames can already
name two different databases today, silently. The key now uses the
username only when the path is empty; an explicit path still ignores the
username, so this does not reopen the sixth round's credentials fix.

An eleventh review round found two further defects, in different files.

The first (P1) found that `url::Url` and `tokio_postgres::Config`
disagree on percent-decoding, on `?dbname=`/`?host=`/`?port=`/
`?hostaddr=` overrides, and on comma-separated multi-host DSNs -- fresh
evidence pointed at `backup_verify.rs`'s own `parse_dsn_identity`
comment, which documents this exact divergence for the guard that keeps
a backup off a live production database. Rather than continue chasing
individual `url`-versus-connector mismatches one at a time, as rounds
six through ten each did, `canonical_dsn_key` now parses with
`tokio_postgres::Config` directly, the same parser `parse_dsn_identity`
uses and the one `diesel_async` actually hands the DSN to at connect
time. This closes the percent-decoding gap by construction, along with
any other divergence class between the two parsers, rather than adding
another special case. Unlike `parse_dsn_identity`, which serves a
different guard with different needs, this key still keeps `options`
(schema-relevant) and drops every other query parameter, and keeps a
resolved Unix-socket path's case instead of folding it, matching the
ninth and tenth rounds' fixes.

The second (P1) found a defect in `audit.rs`, not `shard.rs`. Verifying
it meant tracing the actual SQL: with two colocated shards sharing one
pool, shard A ticked and acknowledged some rows while shard B has no
cursor row yet, a row already acknowledged by A read as fully
acknowledged even though B has acknowledged nothing. The pending check's
own doc comment had assumed "a shard's database holds at most one cursor
row" -- an assumption the fourth through tenth rounds' own colocated-pool
work had already made false. `purge_old_audit_records` gains a fourth
parameter (`colocated_shard_count` in this round, revised below), and
the pending check now compares against it instead of testing for zero
rows. The real caller, `retention.rs`'s `group_shards_by_pool`, now
carries this alongside its combined protection decision, sourced from
`ShardedDbPool::pool_groups()`.

A twelfth review round found three more defects, two of them P1.

The first (P1) found that comparing a *count* of cursor rows, as the
eleventh round's fix did, is not the same as confirming the *right*
shards each have one. `decommission_cursor` retires a row rather than
deleting it, so a shard removed from the fleet entirely can leave a
row behind that makes the count look complete while a currently
colocated shard still has none. `colocated_shard_count: i64` is
replaced with `colocated_shard_ids: &[i32]`; the pending check now
walks that exact list with `unnest` and is satisfied only when every
named id has a matching cursor row, regardless of how many other rows
exist. A single-shard caller passes that one shard's id, unchanged in
effect from the very first "no cursor row at all" test.

The second (P1) found that `options` is a general escape hatch, not a
`search_path`-only channel: an operator can set `application_name` or
any other GUC through it just as easily, and keeping the whole string
verbatim (the seventh round's fix) meant two DSNs for one pool,
differing only in such an unrelated flag, no longer merged --
reopening the same premature-deletion bug this key exists to close.
`canonical_dsn_key` now extracts only `search_path` settings from
`options` with a small, deliberately narrow tokenizer (`-c` immediately
followed by `search_path=value`, no embedded whitespace), and drops
everything else it finds there.

The third (P2) found that a multi-host DSN's hosts and ports are
sorted and deduplicated independently, not paired positionally, so
`host=a,b port=5432,6432` and `host=a,b port=6432,5432` can name
different endpoint pairs yet compare equal. This one is deliberately
not fixed: `from_dsns` is built for its one documented use, one host
per shard entry (`harvest shard rebalance`, issue #964), where this
never arises, and getting it wrong over-merges rather than
under-merges -- a skipped purge on one endpoint, not a premature
delete. Documented as an accepted gap alongside host-alias detection
and role-level `search_path`, left for whoever first needs multi-host
`from_dsns` entries to fix alongside a real case to test it against.

A thirteenth review round found three more defects: two P1, one P2.

The first (P1) found that `canonical_dsn_key` kept `host` and
`hostaddr` as two independent identity components, so `host=alias-a
&hostaddr=10.0.0.5` and `host=alias-b&hostaddr=10.0.0.5` no longer
merged despite sharing the same actual TCP destination -- exactly the
disagreement `backup_verify.rs`'s own `parse_dsn_identity` was already
built to avoid, and cited as this round's own precedent. The key now
prefers `hostaddr` outright whenever it is given at all, matching
`parse_dsn_identity`'s stated rule that a shared address is the same
database "however differently each DSN spells the hostname."

The second (P2) found that the twelfth round's `search_path` tokenizer
missed the compact `-csearch_path=value` spelling (no space before
`-c`) -- fresh evidence being that this exact spelling already appears
in this codebase's own `claim_bench_support.rs`. `extract_search_path`
now recognizes both the spaced and compact forms.

The third (P1) found a defect in `retention.rs`'s own new code, not a
carryover: an exempted shard that never ticks (an unreachable shard
whose export is abandoned, never formally decommissioned) was still
named in `colocated_shard_ids`, so `purge_old_audit_records`'s
missing-cursor check stayed permanently true and blocked purging of
rows a colocated, still-protected shard had genuinely acknowledged --
defeating the whole point of exempting it. `group_shards_by_pool` now
filters the expected-cursor list to shards that currently want
protection, not every shard sharing the pool; an exempted shard's own
permanent lack of a cursor no longer blocks anyone else's purge.

New tests:
- `retention_protects_unexported_audit_when_configured_with_no_cursor_and_no_local_sink`
  reproduces the exact bootstrap window (no cursor row anywhere, no sink in
  the sweeping process) and asserts the flag alone keeps every row.
- `retention_decommission_alone_does_not_resume_purging_while_the_flag_is_true`
  and `retention_protects_a_shard_being_re_enabled_after_decommission` pin the
  round 1 P1 fix: the flag survives a retired cursor, and a shard coming back
  from decommission stays protected until the worker ticks it again.
- `retention_protects_a_stamped_row_when_its_cursor_row_is_gone` pins the
  second round 1 P1 fix: a stamped row with no cursor row to check against
  is never treated as acknowledged.
- `retention_still_purges_acknowledged_rows_when_protect_unexported_audit_is_true`
  confirms the flag never blocks purging of rows the exporter already
  shipped, even while a live cursor exists.
- `excluding_a_shard_leaves_every_other_shard_protected` and
  `excluding_a_shard_while_disabled_changes_nothing` pin the per-shard
  exemption: excluding shard 0 never touches shard 1's protection, and
  excluding a shard while the flag is off entirely is a no-op.
- `from_map_groups_cloned_pools_together` pins the fourth round's fix at
  its new home in `shard.rs`. `from_dsns_groups_shards_sharing_one_dsn`
  pins the fifth round's fix: two shards built from one DSN string
  through `from_dsns` still collapse to one group, even though `from_dsns`
  built them as two distinct `Pool` objects.
  `from_dsns_keeps_distinct_dsns_separate` confirms two different DSNs
  never collapse.
- `from_dsns_groups_equivalent_dsns_with_different_credentials_and_port`
  pins the sixth round's fix: two DSNs for one database, differing only
  in credentials and an explicit default port, still collapse to one
  group. `from_dsns_keeps_distinct_dbnames_on_the_same_host_separate`
  confirms two database names on the same host never collapse.
- `from_dsns_keeps_distinct_search_path_options_separate` pins the
  seventh round's fix: two DSNs whose `options` set a different
  `search_path` never collapse into one group, even with the same host,
  port, and database name.
- `from_dsns_ignores_connection_only_parameters` pins the eighth round's
  P1 fix: two DSNs differing only in `application_name` and `sslmode`,
  and in credentials, still collapse into one group.
- `from_dsns_keeps_distinct_unix_socket_hosts_separate` and
  `from_dsns_groups_shards_sharing_one_unix_socket_host` pin the ninth
  round's fix: two Unix-socket DSNs naming different sockets through
  `host` never collapse, and two naming the same socket still do.
- `from_dsns_keeps_distinctly_cased_socket_paths_separate` and
  `from_dsns_groups_shards_sharing_one_hostname_regardless_of_case` pin
  the tenth round's first fix: a socket path's case is significant and
  is never folded, while a DNS hostname's case still is.
- `from_dsns_keeps_distinct_users_with_no_explicit_dbname_separate` and
  `from_dsns_ignores_username_when_dbname_is_explicit` pin the tenth
  round's second fix: two users with no explicit dbname reach different
  databases and must never collapse, while an explicit, shared dbname
  still collapses regardless of username.
- `from_dsns_groups_percent_encoded_and_plain_dbname_spellings` pins the
  eleventh round's first fix: `postgres://db.example/harvest` and
  `postgres://db.example/%68arvest` collapse into one group now that the
  key is parsed with the real connector's parser.
- `retention_protects_rows_until_every_colocated_shard_has_a_cursor` pins
  the eleventh round's second fix: told to expect two colocated shards
  but only one has a cursor row, no stamped row purges even if already
  acknowledged by the shard that has ticked; told to expect only that one
  shard, the same rows purge normally.
- `retention_ignores_an_unrelated_shards_lingering_cursor_row` pins the
  twelfth round's first fix: a decommissioned, unrelated shard's retired
  cursor row must never stand in for an actually colocated shard's
  missing one, even when the row count happens to match.
- `from_dsns_ignores_non_search_path_options_flags` pins the twelfth
  round's second fix: two DSNs differing only in `application_name` set
  through `options` still collapse into one group.
- `from_dsns_groups_shards_sharing_one_hostaddr_regardless_of_hostname`
  pins the thirteenth round's first fix: a shared `hostaddr` collapses
  two DSNs into one group even when their `host` text differs.
- `from_dsns_recognizes_the_compact_search_path_options_spelling` pins
  the thirteenth round's second fix: `-csearch_path=` (no space) selects
  a schema exactly as the spaced form does.
- `group_shards_by_pool_excludes_an_exempted_shard_from_the_expected_cursor_list`
  pins the thirteenth round's third fix: an exempted shard that never
  ticks is left out of the expected-cursor list, so its permanent lack
  of a cursor never blocks purging rows a colocated, protected shard has
  genuinely acknowledged.

A fourteenth review round found three defects, plus a regression caught
before it ever reached a review comment.

Verifying the round's third finding meant re-reading `group_shards_by_pool`
against its own new test from the thirteenth round, and that reading
surfaced a defect the review had not flagged: the thirteenth round's fix
computed the expected-cursor list with
`shards.filter(|s| config.protects_unexported_audit(*s))`.
`protects_unexported_audit` answers "does the flag protect this shard
**today**", which is `false` for every shard whenever
`protect_unexported_audit` is left at its default, `None` -- the common
case, and the case every test up to that point happened to leave
untested, since each configured the flag explicitly. Filtering on it
silently emptied the expected-cursor list, and so disabled this PR's own
bootstrap-window guard, for any deployment that never sets the flag at
all. The filter now tests exemption directly --
`!exempt.contains(shard)` when the flag is `Some(exempt)`, true
unconditionally when it is `None` -- so an unconfigured flag once again
leaves every shard's cursor mattering, exactly as before the flag
existed. `group_shards_by_pool_expects_every_shard_when_the_flag_is_never_configured`
pins this directly.

The first finding (P2) proposed falling back to another handle sharing a
pool group when one shard's connection fails, so a purge is not skipped
purely because the shard the sweep happened to pick is unreachable. This
is deferred: it is a connection-pool resilience question, not a defect in
the guard logic this PR closes, and risks masking a real per-shard outage
as a routine skip. Left for a follow-up with its own test for a partial
pool failure.

The second finding (P1) found that `extract_search_path` kept every
`-c search_path=...` occurrence it found in `options`, joined, rather
than the one Postgres actually applies. Postgres processes repeated `-c`
flags in the order given, so a later `search_path` setting overrides an
earlier one; keeping both meant two DSNs whose *effective* search path
agreed, because a later flag on one matched the only flag on the other,
could still compare unequal and be split into separate groups --
over-merging is not at risk here, but the two DSNs disagreeing on
apparent history could also under-merge two aliases of the same shard
into different groups, reopening the premature-deletion failure this key
exists to prevent. `extract_search_path` now returns `Option<String>`
instead of `Vec<String>` and keeps only the last occurrence.
`from_dsns_groups_dsns_with_the_same_effective_search_path` pins this: a
DSN with two `-c search_path=...` flags groups with one whose single flag
matches only the second.

The third finding (P2) found the acknowledgment check itself unscoped:
`purge_old_audit_records` treated any colocated shard's cursor row with
`last_acked_seq` past a row's `export_seq` as sufficient, even one for a
shard no longer in `colocated_shard_ids`. `decommission_cursor` retires a
row rather than deleting it, so an excluded shard's cursor persists,
frozen at whatever it last acknowledged, and could keep shielding rows a
still-relevant, colocated shard had already fully acknowledged. The
acknowledgment disjunct now scopes to `c.shard_id = ANY($3::int4[])`, so
only a shard still named in `colocated_shard_ids` can satisfy it.
`retention_ignores_an_excluded_shards_stale_acknowledgment` pins this.

New tests:
- `group_shards_by_pool_expects_every_shard_when_the_flag_is_never_configured`
  pins the self-discovered regression fix: leaving
  `protect_unexported_audit` unset must not empty the expected-cursor
  list.
- `from_dsns_groups_dsns_with_the_same_effective_search_path` pins the
  second finding: only the last `-c search_path=...` in `options`
  decides the group, matching Postgres's own sequential-`SET` semantics.
- `retention_ignores_an_excluded_shards_stale_acknowledgment` pins the
  third finding: an excluded shard's stale, frozen acknowledgment must
  never protect rows a still-relevant, colocated shard has already
  acknowledged.

A fifteenth review round found two more defects, both P2.

The first found that `extract_search_path` recognized `-c search_path=...`
and `-csearch_path=...` but not PostgreSQL's long-form `--search_path=...`,
which its own server documentation names as an equally valid spelling for
any run-time parameter. Two DSNs setting different schemas through
`--search_path=...` both extracted `None`, so they could collapse into
one canonical key despite resolving to different schemas -- risking the
premature-deletion failure this key exists to prevent.
`extract_search_path` now recognizes all three spellings; the last one
found still wins, unchanged from the fourteenth round's fix.
`from_dsns_recognizes_the_long_form_search_path_options_spelling` pins
this.

The second found that a per-shard exemption can be defeated by
`is_configured()`, the pre-existing, process-wide "does this process have
a local sink installed" signal from #953: a process hosting a live sink
for some other shard on the same pool group keeps `is_configured()` true,
which still blocks a purge on a shard an operator just exempted. This is
documented as an accepted gap rather than fixed in this round.
`is_configured()` cannot be narrowed to "is this specific shard's export
live" without knowing which shard a given sink instance actually serves --
information this guard does not have -- and guessing wrong risks the
dangerous direction, treating a shard's own still-live export as finished.
See `docs/audit-export.md` for the operator-facing note.

A sixteenth review round (P1) found that `extract_search_path` split
`options` on bare whitespace, ignoring libpq's own escaping rule: a
backslash before a space embeds a literal space in the current argument
rather than ending it. A `search_path` value containing an escaped
space was truncated at that space, so two aliases of one physical pool
whose sessions resolve to the identical schema list (`tenant,public`
and `tenant, public`, the latter written with an escaped space) could
extract different, truncated values and be split into separate pool
groups -- exactly the under-merging this key exists to prevent, and the
dangerous direction: an unprotected purge on one alias could then delete
rows a still-protected alias has not exported. `extract_search_path` now
splits `options` with an escape-aware tokenizer
(`split_options_preserving_escapes`), and normalizes the extracted value
the way Postgres's own schema-list parser does -- comma-separated, with
insignificant whitespace around each name -- so the two values above
compare equal. A double-quoted schema name is not specially handled;
normalization only trims surrounding whitespace, so it never touches
whitespace a quoted name encloses.
`from_dsns_groups_dsns_whose_search_path_differs_only_by_an_escaped_space`
pins this.

A seventeenth review round found two more defects in the normalized
`search_path` comparison, one P1 and one P2, both in the sixteenth
round's own comma-split-and-trim normalization.

The P1 found that two aliases setting `search_path=PUBLIC` and
`search_path=public` resolve to the identical schema server-side --
Postgres folds an unquoted identifier to lowercase -- but the prior
normalization compared case verbatim, splitting them into separate pool
groups. That is the dangerous direction: an unprotected purge on one
alias could delete rows a still-protected alias has not exported.

The P2 found the opposite failure on quoted identifiers: a value like
`"tenant, one"` names one schema literally containing a comma, but
splitting on every comma before trimming treated it the same as the
two distinct unquoted schemas `tenant` and `one`, merging pools that
resolve to different schemas -- the same silent-never-purged failure
the seventh round's fix was written to prevent.

Both trace to the same root cause: comma-splitting and trimming is not
how Postgres actually parses an identifier list. `extract_search_path`
now calls `parse_identifier_list`, a small parser matching Postgres's
own `SplitIdentifierString` grammar -- quoting, `""` as an escaped
literal quote, and case-folding applied only to unquoted names. A value
that does not fit this grammar (an unterminated quote, trailing content
after a closing quote) falls back to unparsed comparison, the same
conservative choice made elsewhere in this key. Full Postgres
locale-dependent case folding is not chased; Unicode lowercasing is the
accepted approximation.

New tests: `from_dsns_groups_search_path_identifiers_that_differ_only_by_case`
pins the P1 fix, and
`from_dsns_keeps_a_quoted_comma_containing_schema_distinct_from_two_plain_ones`
pins the P2 fix.

An eighteenth review round (P2) found a defect in the seventeenth
round's own fix: `normalize_search_path` rejoined the parsed names with
a bare comma, the same character that separates them. A quoted name
containing a literal comma and two separate unquoted names could then
join to the identical string -- `"tenant,one"` parses as the single
name `tenant,one`, while `tenant,one` parses as the two names `tenant`
and `one`, and both rejoin to the same key `tenant,one`. Two DSNs
resolving `harvest_audit_log` through different schema lists would then
merge into one pool group, and `pool_groups()` keeps only one
representative -- the collapsed-away shard's audit table silently stops
being purged, the same failure the seventh round's fix was written to
prevent.

Each name is now escaped before rejoining
(`escape_identifier_list_item`), backslash-quoting its own backslashes
and commas, so a name boundary can never be confused with the join
delimiter.
`from_dsns_keeps_a_quoted_comma_containing_schema_distinct_with_no_space_to_hide_behind`
pins this: an earlier test for the same distinction happened to pass
for the wrong reason, since its quoted value also differed in
whitespace from the two-name case: this test uses values that agree on
everything except the escaping this round adds.

A nineteenth review round raised two findings, one rebutted and one fixed.

The first (P1) proposed normalizing an omitted `host` to `localhost`
before keying `from_dsns` aliases, on the premise that `tokio_postgres`
defaults an omitted host to a `localhost` TCP connection. Checking that
premise against the actual crate this codebase connects through found
it false: `tokio-postgres` 0.7.18's own `connect()` rejects a config
with both `host` and `hostaddr` empty outright (`"both host and
hostaddr are missing"`), and its DSN parser adds no default host at
all when the authority host is empty (`parse_host` simply returns).
`diesel-async` 0.9.2 calls `tokio_postgres::connect` directly, with no
wrapper that fills in a default. A DSN naming neither `host` nor
`hostaddr` therefore cannot successfully connect through this
codebase's actual connector at all, so it can never execute a purge in
the first place; the premature-deletion scenario the finding describes
cannot occur for that pairing. This differs from libpq's C client,
which does default an unspecified host to a local socket or
`localhost` -- the finding's premise held for that connector, not this
one. Not fixed; replied with this evidence instead.

The second (P2) found that a decommissioned colocated shard's retired
cursor can block a still-active colocated shard's rows forever under
the default policy (`protect_unexported_audit` never configured).
`decommission_cursor`'s own doc comment says retiring a cursor "is
precisely what lets retention purge" that shard's rows, but the
acknowledgment disjunct counted a retired cursor's frozen
`last_acked_seq` the same as a live one, so a permanently decommissioned
shard's stale ack shielded rows a still-relevant, active shard had
already acknowledged, with no way to ever stop.
`purge_old_audit_records` gains a fourth parameter, the raw
`protect_unexported_audit` bool (previously only folded into the
combined `export_may_be_live` signal), and the acknowledgment
disjunct now reads `AND ($4::BOOLEAN OR c.retired_at IS NULL)`: a
retired cursor's stale ack is ignored only while the flag is not
itself protecting this pool group. When the flag *is* protecting the
group, a retired cursor still blocks, preserving the first review
round's re-enablement guarantee: an operator keeping the flag on
through a decommission-then-resume transition still needs the
re-enabling shard's retired, not-yet-un-retired cursor treated as
pending.

New tests:
`retention_resumes_purging_once_a_colocated_shard_is_decommissioned_under_the_default_policy`
pins the fix, and
`retention_still_waits_on_a_decommissioned_shards_ack_while_the_flag_protects_the_group`
pins that the first round's re-enablement guarantee still holds.

A twentieth review round (P1) found that `search_path=public` and
`search_path=pg_catalog,public` resolve an unqualified relation
identically: Postgres always searches `pg_catalog` first when it is
not named explicitly, so omitting it is equivalent to naming it first.
The parsed-list normalization treated the two lists as different --
`["public"]` versus `["pg_catalog", "public"]` -- the same dangerous
under-merging direction as the earlier case-folding and quoted-comma
findings, since it stops two aliases of one physical pool from being
combined and can reopen the premature-deletion race this key exists to
prevent. `normalize_search_path` now inserts `pg_catalog` at the front
of the parsed list whenever it is not already present, matching
Postgres's implicit-first search order. A list that already names
`pg_catalog` anywhere is left alone, since its explicit position then
decides the resolution order -- an explicit, non-leading `pg_catalog`
names a genuinely different order from the implicit one and must not
collapse with it.

New tests:
`from_dsns_groups_an_implicit_pg_catalog_with_an_explicit_leading_one`
pins the fix, and
`from_dsns_keeps_an_explicit_trailing_pg_catalog_distinct_from_the_implicit_leading_one`
pins that an explicit, non-leading `pg_catalog` still stays distinct.

A twenty-first review round raised two more findings in the same area,
both P1, both the dangerous under-merging direction.

The first found that `split_options_preserving_escapes` only
recognized an escaped space or backslash, but `PostgreSQL`'s own
splitter (`pg_split_opts`) tests with `isspace()`, not specifically a
space -- a tab or other whitespace escapes the same way. An alias using
an escaped tab within a `search_path` value truncated at that tab, the
same failure mode the sixteenth round's escaped-space fix closed for
spaces specifically. The escape condition now recognizes a backslash
before any whitespace character, not only a literal space.

The second found that `search_path=public` and `search_path=public,
public` resolve the identical schema in the identical order --
Postgres does not treat a repeated name specially, so a later repeat
changes nothing about where a relation resolves -- but the
normalization kept both copies, splitting the two aliases into
separate pool groups. `normalize_search_path` now drops a repeated name
after the `pg_catalog` insertion, keeping only its first occurrence.

New tests:
`from_dsns_groups_dsns_whose_search_path_differs_only_by_an_escaped_tab`
pins the first fix, and
`from_dsns_groups_a_search_path_with_a_repeated_name` pins the second.

A twenty-second review round raised two more findings, both P1, both
the dangerous under-merging direction, continuing the pattern from the
rounds above.

The first found that GUC parameter names are case-insensitive in
Postgres, so `SEARCH_PATH=shared` sets the identical GUC as
`search_path=shared`, but the prefix check was case-sensitive and
extracted `None` for the uppercase spelling, splitting the two
aliases. `extract_search_path` now matches the GUC name
case-insensitively across all three recognized spellings via a shared
`strip_search_path_name` helper.

The second found that `search_path=public` and
`search_path=pg_temp,pg_catalog,public` resolve the identical order:
the session's temporary-object schema, `pg_temp`, is implicitly
searched first when omitted, just as `pg_catalog` is implicitly
searched first (ahead of the listed path) when it is omitted.
`normalize_search_path` now inserts `pg_temp` at the very front when
absent, applied after the existing `pg_catalog` insertion so it lands
ahead of `pg_catalog` exactly when both were omitted. A list that
already names `pg_temp` anywhere keeps its explicit position, matching
`pg_catalog`'s own rule.

New tests: `from_dsns_recognizes_an_uppercase_search_path_guc_name`
pins the first fix.
`from_dsns_groups_an_implicit_pg_temp_with_an_explicit_leading_one` and
`from_dsns_keeps_an_explicit_trailing_pg_temp_distinct_from_the_implicit_leading_one`
pin the second, mirroring the twentieth round's `pg_catalog` test pair.

A twenty-third review round raised two more findings in the same area,
both P2: one fixed, one declined.

The first found that `split_options_preserving_escapes` only consumed
a backslash before whitespace or another backslash, but
`pg_split_opts` removes a backslash before *any* character
unconditionally, so `public\,public` reaches the server the same as
`public,public` -- the backslash never survives to
`SplitIdentifierString`. This finding sits in tension with this same
PR's own twenty-first-round reasoning, which assumed the opposite
(that `pg_split_opts` keeps an escaped character's backslash in
place). Neither could be checked against the actual `PostgreSQL` C
source in this environment. The fix was applied anyway, because the
risk here is asymmetric: implementing it and being wrong lands in the
safe, over-merging direction (two genuinely different values compare
equal), while leaving it unfixed and being wrong stays in the
dangerous, under-merging direction this whole key exists to close. The
escape condition now consumes a backslash before any character, with a
trailing, nothing-following backslash kept literally.
`from_dsns_groups_dsns_whose_search_path_differs_only_by_an_escaped_comma`
pins the fix.

The second restates a gap the eighth review round already accepted,
not a new one: `search_path="$user"` resolves to a schema named after
the connecting role, so two DSNs differing only in username but
sharing this setting can resolve `harvest_audit_log` through different
schemas. Substituting the real username into that comparison would
require keeping the username in the key at all -- exactly what the
eighth round rejected, since `from_dsns` has a documented use
(`harvest shard rebalance`, issue #964) with one database reached under
different usernames, which dropping the username exists to keep
merged. The finding's own description confirms the failure direction
is the safe one -- a schema silently never purged, not a row deleted
early -- consistent with why the eighth round accepted this gap rather
than fixing it. Not fixed; replied with this reasoning instead.

A twenty-fourth review round (P1) found a defect different in kind from
every prior round in this stretch: not a parsing bug, but a limit on
what any DSN-text-only key can determine. Two different `search_path`
orders can resolve one unqualified relation to the identical schema
when the earlier-searched schemas in one order simply do not define
that relation -- `tenant_a,public` and `tenant_b,public` both resolve
`harvest_audit_log` from `public` whenever neither tenant schema
defines its own copy. Unlike every fix in rounds fourteen through
twenty-three, closing this needs to know what each named schema
actually contains: a live catalog lookup, not a fact derivable from the
DSN text no matter how precisely `search_path` is parsed. `canonical_dsn_key`
is built to stay a pure, local operation with no network access, the
same constraint that already leaves the host-alias gap (two hostnames
resolving to one address) undetected. This is documented as a fourth
accepted gap alongside it, rather than attempted: unlike the other
three, it is not conservative, since two aliases of one physical table
can compare as distinct pools. Not fixed; replied explaining why a
fix would require the connection this function is built not to make.

A twenty-fifth review round (P1) found a gap in the thirteenth round's
own `hostaddr`-wins-outright fix: a numeric `host` value was still
folded into `hostaddrs` alongside an explicit `hostaddr`, rather than
being discarded the way a non-numeric `host` already was.
`host=10.0.0.1&hostaddr=10.0.0.2` therefore keyed as both addresses,
while `host=alias&hostaddr=10.0.0.2` keyed only the one address, even
though an explicit `hostaddr` pins the same destination regardless of
what `host` says -- the same dangerous under-merging direction as
every other fix in this stretch. A numeric TCP `host` entry is now
skipped outright once `hostaddr` is explicit, matching how a
non-numeric `host` was already handled.
`from_dsns_groups_a_numeric_host_with_a_differing_explicit_hostaddr`
pins the fix.

A twenty-sixth review round raised two more findings, both P2: one
fixed, one declined.

The first found that `--search-path=schema_a` and `--search_path=schema_a`
set the identical GUC, since Postgres normalizes a hyphen to an
underscore when mapping a long-form `--long-option` to its GUC name --
but the long-form comparison only recognized the underscore spelling,
so the two would extract as `None` and `Some("schema_a")`
respectively, splitting two aliases that resolve identically.
`strip_search_path_name_long_form` now normalizes a hyphen to an
underscore in the long-form name before matching.
`from_dsns_recognizes_a_hyphenated_long_form_search_path_name` pins
the fix.

The second restates a narrower variant of the eighth review round's
already-accepted `$user`/username gap, not a cleanly separable new
bug. It proposes substituting an explicit `-c role=...` value, when
present in `options`, for the `$user` token in `search_path` -- but
that only helps the narrow sub-case where `role` is *also* explicitly
set via `options`. The far more common case, a DSN connecting directly
as the target role through its own credentials with no `role=`
override, would still leave `$user` unresolved, since resolving it
needs the username this key deliberately drops (per the eighth
round's reasoning, to keep the `harvest shard rebalance` topology,
issue #964, merged). `extract_search_path`'s own parser gives `$user`
no special treatment at all today -- it is read as a literal, opaque
identifier, matching identically regardless of the actual user or
role. A `role=`-only fix would patch one narrow trigger of that same
gap while leaving its more common trigger untouched, an inconsistent
partial fix. Not fixed; replied explaining why.

**Zero migration, zero engine impact beyond the new parameter.** No new
`WorkflowEvent` variant, no schema change, no change to any existing call
site's behavior when the new flag is left at its default (disabled).
