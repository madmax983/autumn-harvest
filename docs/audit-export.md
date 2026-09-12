# Audit export to an external SIEM sink

Issue #953. Ships every management-API audit record off-box, to a sink you
run, with at-least-once delivery, visible lag, and a redrive path.

Harvest writes an audit record for every mutating management-API operation
(`docs/runbooks/audit-trail.md`). Those rows live **per shard, inside the same
Postgres databases they describe** — which is backwards for a compliance team:
an attacker (or a fat-fingered operator) with database access is the same
principal who can rewrite the record of what they did, and a SOC 2 / ISO 27001
audit asks where privileged-action logs ship and how you know none were lost.

This feature answers both. The design is a deliberate replay of the durable
completion-callback architecture (#605): a boxed async trait in core with no
HTTP client, a `reqwest` signed-webhook implementation in the plugin, a
two-transaction scanner that never holds a row lock across network I/O, and a
per-shard cursor that advances only on acknowledgement.

- **It is opt-in.** With no sink configured, nothing changes: no sequence is
  assigned, no cursor row is created, the scanner returns before issuing a
  single query.
- **It never touches workflow history.** No new `WorkflowEvent` variant, no
  replay-determinism impact. Audit rows are operational metadata; the exporter
  only reads them.

---

## Configuring it

The batteries-included path — a signed webhook your SIEM (or a collector in
front of it) receives:

```rust
use autumn_harvest::completion_callback::HostAllowlist;

let harvest = HarvestBuilder::new()
    .audit_export_allowlist(HostAllowlist::new().with_pattern("siem.example.com"))
    .audit_export_webhook("https://siem.example.com/harvest/audit")
    .audit_export_secret(std::env::var("HARVEST_AUDIT_HMAC")?)
    .build();
```

The allowlist is required, and HTTPS is required, for the same reason as
completion callbacks: the URL is validated at `try_build()` time and a
rejection **fails the build** rather than warning. An audit export that
silently never delivers is a compliance gap you would discover at audit time.
`audit_export_allow_http(true)` and `audit_export_allow_ip_literals(true)`
exist for local development; audit records name who acted on which tenant, so
shipping them in cleartext is itself a finding.

`audit_export_secret(...)` is **required** alongside a webhook, and its absence
fails the build too. HMAC-SHA256 accepts a zero-length key and produces a
well-formed signature, so an unconfigured secret does not yield a *missing*
`X-Harvest-Signature` — it yields one any third party can reproduce, which is
worse than none for a receiver that verifies it. A custom `AuditSink` may
authenticate however it likes (IAM, mTLS, a local file), so there the secret is
optional and its absence only warns.

Other knobs:

| Builder method | Default | Notes |
|---|---|---|
| `audit_export_batch_size(n)` | 500 | Records per batch, clamped to `[1, 5000]`. |
| `audit_export_backoff(b)` | 1s → 2s → 4s … capped at 60s | Capped exponential. **No attempt ceiling** — see below. |
| `audit_export_lease(d)` | 60s | How long one exporter holds a shard's cursor, **and** the timeout on the sink call. Set it above your sink's own request timeout. |
| `audit_export_secret(k)` | *(required for a webhook)* | HMAC key for `X-Harvest-Signature`. |
| `audit_export_sink(s)` | *(none)* | Your own `AuditSink`; takes precedence over the webhook. |

### Bringing your own sink

`AuditSink` lives in core and has no HTTP dependency, so a Kinesis writer, a
file appender, or an OTLP-logs bridge is a first-class implementation rather
than a fork:

```rust
use autumn_harvest::audit_export::{AuditBatch, AuditSink, SinkAttempt, SinkFuture};

struct KinesisSink { /* ... */ }

impl AuditSink for KinesisSink {
    fn deliver<'a>(&'a self, batch: &'a AuditBatch<'a>) -> SinkFuture<'a> {
        Box::pin(async move {
            // `batch.records` is the structured form; `batch.body` is the
            // canonical JSON-lines bytes core signed.
            match self.put_records(batch.records).await {
                Ok(()) => SinkAttempt::success(200),
                Err(e) => SinkAttempt::transport_error(e.to_string()),
            }
        })
    }
}
```

**A `success` return advances the cursor past those records.** Only return one
once the batch is durably accepted downstream.

> A direct core embedder (`HarvestBuilder::build()` → `into_worker_parts()`,
> without `autumn-harvest-plugin`) must use `audit_export_sink(...)`: core
> ships no HTTP client, so `audit_export_webhook(...)` alone has no transport
> on that path and logs a warning rather than silently doing nothing.

---

## The wire format

One POST per batch, `Content-Type: application/x-ndjson`, body is JSON lines —
one audit record per line, newline-terminated.

```
POST /harvest/audit
Content-Type: application/x-ndjson
X-Harvest-Signature: sha256=<hex HMAC-SHA256 of the exact body>
X-Harvest-Timestamp: 2026-08-31T04:11:07.881Z
X-Harvest-Audit-Shard: 0
X-Harvest-Audit-First-Seq: 4181
X-Harvest-Audit-Last-Seq: 4680

{"shard":0,"seq":4181,"id":"...","shard_id":0,"occurred_at":"2026-08-31T04:11:02.117Z","actor":"alice@example.com","operation":"workflow.cancel","target_type":"workflow","target_id":"exec-9f2…","route_or_command":"POST /workflows/{id}/cancel","request_id":null,"idempotency_key":null,"status":"succeeded","error_summary":null,"source":"api"}
{"shard":0,"seq":4182, …}
```

The signature is HMAC-SHA256 over the exact bytes of the body, in the same
`X-Harvest-Signature: sha256=<hex>` scheme as completion callbacks (#605) — a
receiver already verifying those can reuse the verification code unchanged.
Compare with a constant-time comparison.

> **The signature covers the body only.** `X-Harvest-Audit-Shard`,
> `-First-Seq`, `-Last-Seq`, and `X-Harvest-Timestamp` are unauthenticated
> routing and triage metadata. Read the authoritative `(shard, seq)` pair from
> each record **in the body**: deduplicating on the headers would mean
> deduplicating on attacker-controlled input, and a replay of a captured,
> validly-signed batch with a shifted range could mark a real range as
> already-seen — creating exactly the silent gap this feature exists to
> prevent.

Optional fields serialize as an explicit `null` rather than being omitted, so
a SIEM's schema inference sees a stable object shape across every batch.

### Verifying completeness: `(shard, seq)`

`seq` is a **dense, strictly monotonic, per-shard** sequence. Per shard it
starts at 1 and increases by exactly 1 per record, so a receiver can do better
than gap *detection* — it can check contiguity:

- Deduplicate on `(shard, seq)`. Delivery is at-least-once, so the same pair
  can arrive more than once (a retry, a process death between the POST and the
  cursor write, or an operator redrive). Re-deliveries are **byte-identical**.
- Alert on a hole. A missing `seq` between two received ones means records did
  not reach you — either still in flight, or lost on your side.
- Do not compare sequences across shards. They are independent counters.

#### Why the sequence is assigned by the exporter

The obvious implementation — a `BIGSERIAL` on the audit table — is wrong twice
over, and both reasons are worth knowing if you are auditing this design:

1. **It would lose records.** A serial value is handed out *before* the
   transaction commits, so two concurrent audited operations can take 5 and 6
   and commit in the order 6, 5. An exporter with a `WHERE seq > cursor` cursor
   that shipped 6 first would skip 5 forever. (`occurred_at` has the same
   defect — it is transaction *start* time and can move backwards between
   concurrent inserts.)
2. **It would break under DR failover.** Logical replication does not replicate
   sequence values, so a promoted standby (`docs/cross-region-dr.md`) would
   re-issue sequence numbers it had already exported, corrupting your
   `(shard, seq)` accounting.

Instead the exporter stamps `harvest_audit_log.export_seq` on rows it can
actually *see* (`export_seq IS NULL`), under the per-shard cursor row lock. A
row that commits late is still `NULL` on the next tick and simply receives a
later sequence — skipping is not representable. The counter lives in
`harvest_audit_export_cursor`, ordinary replicated table data.

### Mapping to OTLP logs

The record is deliberately flat, for embedders bridging to an OpenTelemetry
collector:

| Field | OTLP log record |
|---|---|
| `occurred_at` | `timeUnixNano` / `timeObservedUnixNano` |
| `operation` | `body` (or `event.name`) |
| `status` | `severityText` — `"succeeded"` → `INFO`, `"failed"` → `ERROR`. **Lowercase on the wire**: these are the audit table's own values (`audit::STATUS_SUCCEEDED` / `STATUS_FAILED`), passed through verbatim. A receiver matching `"FAILED"` will silently classify every failed privileged action as `INFO`. |
| `error_summary` | `attributes["exception.message"]` |
| `shard_id` | `attributes["harvest.shard.id"]` — the shard the **operation acted on**, and the one a correlation should key off |
| `shard` | `attributes["harvest.audit.source_shard"]` — the shard whose **database this record was read from**. Together with `seq` it is the dedup and gap-detection key, *not* an operation attribute |
| `seq` | `attributes["harvest.audit.seq"]` |
| `id` | `attributes["harvest.audit.id"]` |
| `actor`, `target_type`, `target_id`, `route_or_command`, `request_id`, `idempotency_key`, `source` | `attributes["harvest.audit.<field>"]` |

**`shard` and `shard_id` are different things and both are exported.** They
normally agree, but a control-plane mutation writes its audit row on the
default shard while naming the shard it acted on, so `shard` is `0` and
`shard_id` is the target. A bridge that maps `shard` to `harvest.shard.id` will
attribute those actions to the wrong shard — quietly, since every
single-shard-per-operation record still looks right. `shard_id` is `null` for
an operation that names no shard.

Vendor-specific integrations (Splunk HEC, Datadog intake) are embedder glue on
top of this surface, not engine features.

---

## Delivery semantics

**At-least-once, and it never gives up.** Unlike a completion callback, an
audit record has no dead-letter path: the export *is* the compliance artifact,
so a failing sink backs off (capped exponential) and retries forever, and the
cursor is held exactly where it was. A non-2xx response, a transport error, or
a 3xx (redirects are never followed — an allowlisted host answering with a
pointer at an internal address must not be chased) all mean "not delivered".

The delivery loop is three phases, and the separation is the point:

1. **Claim** (one transaction): take the shard's cursor row lock, stamp
   sequences on newly-visible audit rows, load the batch above the cursor, bump
   the claim epoch, take a lease. Commits before any network call.
2. **Deliver** (no transaction, no locks): hand the batch to the sink.
3. **Acknowledge** (one transaction): on a 2xx, advance the cursor to the
   batch's highest sequence. On anything else, write the backoff and **leave
   the cursor alone**.

Every acknowledgement is guarded on the claim epoch, so an attempt whose sink
call outlived its lease — and whose batch a later claim already re-delivered —
cannot apply a stale outcome over a fresher one, and a redrive that lands
mid-flight cannot be silently undone.

The exporter rides the existing background-scanner cadence
(`enforce_timeouts_once`); it spawns no task of its own.

### Retention interaction

`purge_old_audit_records` will **never** delete a record the exporter has not
shipped, even one past the retention window. A sweep that removed an unexported
row would be a silent compliance gap — gone from the database *and* absent from
the SIEM, with nothing anywhere to show it was lost.

The guard applies when **any** signal says an exporter still owes this
shard records:

- **A cursor row exists for the shard.** Durable, shared state, so it works when
  retention and export run in **different processes** — a split web/worker
  deployment where only the worker configures the sink would otherwise have the
  web app's retention sweep delete rows the worker still owes.
- **A sink is configured in the sweeping process.** Covers the window before the
  exporter's first tick on a shard has created the cursor row at all (freshly
  enabled, newly added to the fleet, or a shard whose pool has been failing).
- **`RetentionConfig::protect_unexported_audit` is `true`.** Covers a gap the
  first two signals share (issue #1266). In a split web/worker deployment, the
  process running retention may have no sink and no cursor row at the same
  time. This happens before the worker's first successful tick on a shard —
  a fresh enablement, a newly added shard, or a shard being re-enabled after
  decommission all have this same gap. Neither of the first two signals can
  close that window alone. Both need the worker to have reached the shard at
  least once. Set this flag the same way on every process in the deployment.
  This closes the window from the moment export is configured, not from the
  moment it first succeeds.

  ```rust
  autumn_harvest::retention::RetentionConfig::default()
      .with_audit_retention_days(90)
      .with_protect_unexported_audit(true);
  ```

  Two cheaper fixes were considered and rejected. Seeding the cursor row in a
  migration cannot work: a shard's own database does not know its own shard
  id, which is the same reason `ensure_cursor_row` provisions it lazily
  instead (see `harvest_shard_generation`'s migration, issue #954, for the
  identical argument). Having the retention process create the row itself
  cannot work either: that process does not know whether an exporter is
  coming, which is exactly the information this flag supplies instead.

  Like `is_configured`, this flag overrides a retired cursor too — an
  earlier draft scoped it to "no cursor row at all" instead, which reopened
  the exact re-enablement window it exists to close (issue #1266). The two
  signals therefore share one cost: see "Retiring audit export on a shard"
  below.

  A fleet has more than one shard, and this flag would otherwise apply to
  all of them at once. Decommissioning shard A must resume purging there.
  Turning the flag off fleet-wide to do that would also strip protection
  from shard B, still mid-bootstrap on the same sweep. Exempt shard A
  instead:

  ```rust
  autumn_harvest::retention::RetentionConfig::default()
      .with_audit_retention_days(90)
      .with_protect_unexported_audit(true)
      .excluding_shard_from_protect_unexported_audit(shard_a);
  ```

  Shard A resumes purging. Every other shard, including a genuinely
  bootstrapping shard B, stays protected.

  A pre-split staging deployment can back two logical shards with one
  physical pool. The sweep detects this on its own and combines their
  decisions conservatively — protecting the shared pool whenever any
  aliased shard wants protection — so exempting shard A never
  accidentally strips shard B's protection just because they share a
  database. No operator action is needed for this case.

  The per-row pending check also needs to know which shard ids share the
  pool, not only the combined decision. A row already acknowledged by
  one colocated shard is not acknowledged by another that has not
  ticked yet and so has no cursor row there at all.
  `purge_old_audit_records` takes this as `colocated_shard_ids`, matched
  by identity rather than counted — a decommissioned shard's cursor row
  is retired, never deleted, so a shard removed from the fleet entirely
  can leave a row behind that would make a mere count look complete. The
  list names every shard sharing the pool except one an operator has
  explicitly exempted. That is not the same list as "shards that
  currently want protection": when the flag is unset fleet-wide (the
  common case), no shard wants protection today, yet every shard's
  cursor still matters exactly as it did before the flag existed, so all
  of them belong in the list. An exempted shard that never ticks has no
  cursor row of its own by design, and naming it anyway would block
  purging rows a still-protected, colocated shard has genuinely
  acknowledged. Only its explicit exemption removes it. The sweep
  sources the list from the same `ShardedDbPool::pool_groups()` call
  that computes the combined decision, so this needs no separate
  operator action either.

  The acknowledgment check itself is scoped the same way: a colocated
  shard's `last_acked_seq` only protects rows once that shard's id
  appears in `colocated_shard_ids`. A shard excluded from the list keeps
  its last cursor row, frozen at whatever it last acknowledged before
  decommissioning, and that stale row must not go on shielding rows a
  still-relevant, colocated shard has already fully acknowledged.

  A shard still named in `colocated_shard_ids` — the default policy
  never excludes anyone — can also be decommissioned, and its retired
  cursor's stale ack is ignored the same way, but only while
  `protect_unexported_audit` is not itself protecting this pool group.
  `decommission_cursor`'s own guarantee is that retiring a cursor "is
  precisely what lets retention purge" that shard's rows; without this,
  a decommissioned shard's frozen ack would block a still-active
  colocated shard's rows forever, since a retired cursor row is never
  deleted. Gating this on `protect_unexported_audit` preserves the
  flag's guarantee for a shard mid-re-enablement: an operator who keeps
  the flag protecting this group through a decommission-then-resume
  transition still sees the re-enabling shard's retired cursor treated
  as pending, exactly as before this change, until its next tick
  un-retires it.

  Detection covers both ways a fleet builds a `ShardedDbPool`.
  `ShardedDbPool::from_map` can receive one cloned `Pool` under two shard
  IDs; the sweep groups these by pool identity. `ShardedDbPool::from_dsns`
  builds a separate `Pool` per entry even for two connection strings that
  reach one database, so identity alone cannot see the alias; the sweep
  groups these by a canonical form of the DSN instead (host, port, path,
  and the `options` query parameter, ignoring everything else including
  credentials), compared before the DSN is consumed into a pool. The
  canonical form is parsed with `tokio_postgres::Config`, the exact
  parser `diesel_async` hands the DSN to at connect time — the same
  choice `backup_verify.rs`'s `parse_dsn_identity` makes, since `url::Url`
  disagrees with it on percent-decoding, on `?dbname=`/`?host=`/`?port=`/
  `?hostaddr=` overrides, and on comma-separated multi-host DSNs.
  Only a `search_path` setting is extracted from `options` (`options`
  itself can carry `-c search_path=...`, `-csearch_path=...`, or
  PostgreSQL's long-form `--search_path=...`, all three recognized, but
  also any other GUC an operator sets, so the whole string is not kept).
  The GUC name is matched case-insensitively in all three spellings,
  since PostgreSQL parameter names are: `SEARCH_PATH=shared` sets the
  identical GUC as `search_path=shared`.
  Postgres applies repeated `-c` flags in order, so
  a later `-c search_path=...` overrides an earlier one; the extraction
  keeps only the last occurrence, matching that sequential-`SET`
  semantic rather than the first or a concatenation. `search_path` picks
  which schema a query resolves against — two DSNs differing only there
  must stay in separate groups, and two DSNs whose last `search_path`
  setting agrees must stay in one even if an earlier, overridden setting
  differs. Splitting `options` into arguments honors libpq's own
  escaping: a backslash before any other character — not only
  whitespace — is consumed by `pg_split_opts`, which removes it
  unconditionally before the value ever reaches `SplitIdentifierString`,
  so `public\,public` reaches the server the same as `public,public`.
  Keeping the value from being truncated at an escaped space or tab
  falls out of this same general rule. The extracted value is then
  parsed as a
  Postgres identifier list, the same grammar `SplitIdentifierString`
  uses for `search_path` server-side: comma-separated, with
  insignificant whitespace around each name, an unquoted name folded to
  lowercase, and a double-quoted name kept verbatim — case, embedded
  commas, embedded spaces, and all, with `""` inside one read as a
  literal quote. `tenant,public` and `tenant, public` compare equal, and
  `PUBLIC` collapses with `public`, but a quoted `"tenant, one"` (one
  schema) never collapses with the two unquoted schemas `tenant` and
  `one`. A value that does not fit this grammar is compared unparsed,
  the conservative fallback. Each parsed name is escaped before the
  names are rejoined into the key, quoting its own backslashes and
  commas: joining with a bare comma would let a quoted name's own
  embedded comma read back as a name boundary, so the one name
  `tenant,one` and the two names `tenant` and `one` would otherwise join
  to the identical string. `pg_catalog` is inserted at the front of the
  parsed list when it is not already named: Postgres always searches
  `pg_catalog` first when it is omitted, so `public` and
  `pg_catalog,public` resolve an unqualified relation identically and
  must key the same, while `public,pg_catalog` (an explicit, trailing
  `pg_catalog`) names a genuinely different order and stays distinct.
  `pg_temp`, the session's temporary-object schema, is inserted the
  same way but ahead of `pg_catalog`, matching Postgres's own
  precedence when both are implicit. A repeated name is then dropped,
  keeping only its first occurrence:
  `public` and `public,public` search the identical schema in the
  identical order, so a later repeat changes nothing about where a
  relation resolves.
  Every other query parameter (`application_name`, `sslmode`, and so on)
  is dropped, since none of them changes which relation a query resolves
  against — except `host`, `hostaddr`, and `port`: a Unix-socket DSN
  carries its real endpoint there rather than in the URI authority
  (`postgresql:///harvest?host=%2Frun%2Fpg`), so the key falls back to
  them when the authority host is empty. `hostaddr` wins over `host`
  outright whenever it is given at all, matching `backup_verify.rs`'s
  `parse_dsn_identity`: it pins the actual TCP destination, so two DSNs
  sharing one stay one pool however differently each spells the
  hostname. This holds even when `host` is itself a numeric address: an
  explicit `hostaddr` alone decides the destination, so a differing
  numeric `host` is discarded rather than folded in alongside it. A
  resolved host is lowercased only when it does not start
  with `/`: a DNS name is case-insensitive, but a Unix-socket path is a
  case-sensitive filesystem path (`/run/PG-A` and `/run/pg-a` name
  different sockets). A DSN with no path is not treated as naming no
  database, since libpq defaults an omitted `dbname` to the connecting
  username — the key uses the username only in that case, never when a
  path is present.

  Four gaps are accepted rather than chased further. A host alias (two
  hostnames resolving to one address) needs a live connection to detect
  and is left undetected. A role's own `search_path` set server-side with
  `ALTER ROLE ... SET search_path` is invisible in the DSN, and not fully
  covered by keeping the username, since the same role name can be
  granted identical or different search paths across environments — two
  DSNs for one database under different usernames are also a documented
  topology (`harvest shard rebalance`, issue #964), so treating different
  usernames as different pools was rejected as reopening a worse bug. A
  multi-host DSN's hosts and ports are sorted and deduplicated
  independently rather than paired positionally, so two DSNs that pair
  the same hosts and ports differently can compare equal even though they
  name different endpoints; `from_dsns` is built for one host per shard
  entry, where this never arises, and getting it wrong skips a purge
  rather than causing a premature one, so it is left for whoever first
  needs multi-host entries to fix. Two different `search_path` orders can
  also resolve one unqualified relation to the identical schema when the
  earlier-searched schemas in one order simply do not define that
  relation — `tenant_a,public` and `tenant_b,public` both resolve
  `harvest_audit_log` from `public` whenever neither tenant schema
  defines its own copy. Unlike the other three gaps, this one is not
  conservative: two aliases of one physical table can compare as
  distinct pools, the same under-merging risk this key exists to close
  elsewhere. Detecting it needs to know what each named schema actually
  contains, a live catalog lookup rather than a fact the DSN text
  carries, so it is out of reach for the same reason as the host alias
  gap: building a pool must stay a pure, local operation with no network
  access.

  The remaining cost is operational, not architectural: an operator must
  remember to set the flag on every process, including ones added later.
  Forgetting it only reopens the original bootstrap window; it never causes
  data loss beyond that.

  One further gap is accepted rather than fixed here: exempting shard A
  restores purging only if the *sweeping process itself* has no local
  sink installed (`is_configured()` is process-wide, not per-shard, and
  predates this guard). A process that hosts a live sink for some other
  shard on the same pool group leaves `is_configured()` true, which
  still blocks shard A's purge even after its exemption. Narrowing
  `is_configured()` to "does this specific shard have a live sink" needs
  to know which shard a given sink instance actually serves — information
  this guard does not have today, and getting it wrong risks the opposite,
  dangerous direction: treating a shard's own still-live export as
  finished. Until that scoping exists, an operator retiring shard A on a
  process that also actively exports another colocated shard must stop
  that process's sink too, exactly as the pre-existing `is_configured()`
  trade already required before per-shard exemption existed.

The guard is deliberately **not** time-based. An earlier revision expired it 24
hours after the exporter's last heartbeat, so a long worker outage lifted it. A
timeout cannot distinguish "export was intentionally removed" from "the worker
has been down since Friday", and it resolves that ambiguity by deleting audit
records during exactly the outage where they matter most.

### Retiring audit export on a shard

Because the guard never expires on its own, turning export off is an explicit
operator action:

```rust
autumn_harvest::audit_export::decommission_cursor(&mut conn, shard_id).await?;
```

This marks the cursor **retired**; the row itself is never deleted, because its
`last_assigned_seq` has to outlive the audit rows. A retired cursor is inert:
retention ignores it, a redrive against that shard is refused with `404` rather
than reporting a rewind whose records nothing will ship, the status route
reports `delivery_state: "RETIRED"` with a zero backlog, and any delivery still
in flight is invalidated — retiring bumps the cursor's `claim_epoch`, so an
attempt claimed beforehand can no longer apply its outcome afterwards. Retiring is what tells
retention that nothing owes this shard records any more, so the next sweep
purges its aged audit rows normally. Do this
only once you accept that any records the shard had not yet shipped will never
reach the SIEM.

Stopping the exporter alone does **not** restore purging — the guard keys on
the cursor row, not on the sweeping process's sink configuration, which is what
makes it safe across a split web/worker deployment. Both steps are required.
Where `RetentionConfig::protect_unexported_audit` also covers this shard on
the sweeping process, it is a third thing to clear: purging does not resume
for a decommissioned shard while any of the three signals still holds. Add
the shard to the exempt set rather than disabling the flag fleet-wide, or
every other shard loses its bootstrap protection too.

Re-enabling export afterwards is safe: the next exporter tick un-retires the
cursor and resumes from the preserved `last_assigned_seq`, so new records
continue the sequence instead of re-issuing numbers that already name different
records — which a receiver deduping on `(shard, seq)`, exactly as this document
instructs it to, would silently discard. This holds even when retention purged
every stamped row in the meantime, which is why the cursor is retired rather
than deleted. Records purged while retired are gone and are not re-delivered.

Until then, a sink left down indefinitely lets the audit table grow past its
retention window. That is the deliberate trade — unbounded growth is loud
(`harvest.audit.export_lag`, the `last_error` on `GET /admin/audit-export`),
bounded by the genuine unexported backlog rather than the whole table
(fully-acknowledged records are purged on the normal schedule), and reversible.
Deleted audit records are not.

With export inactive by both signals the guard is skipped entirely and the
purge is byte-identical to its pre-#953 behaviour.

The trade is that a sink that is down indefinitely lets the audit table grow
past its retention window. That is deliberate: dropping a privileged-action log
to reclaim disk is not a decision the engine gets to make for you. Alert on
`harvest.audit.export_lag` (below) and it will not surprise you — see
`docs/runbooks/harvest-alerts.md#harvest_audit_export_lag_high`.

---

## Observability

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `harvest.audit.export_lag` | Gauge (seconds) | `shard` | Age of the **oldest** audit record the sink has not acknowledged. `0` means fully caught up. |
| `harvest.audit.exported` | Counter | `shard` | Records acknowledged, counted only after the cursor advanced. |

Both are labelled `{shard}` only. The audit `actor`, `operation`, and
`target_id` are deliberately never labels — they are unbounded, user-supplied,
and tenant-identifying (ADR-0001 §7).

> **Why oldest, not newest.** Under sustained mutating load a stuck exporter
> always has a brand-new unexported record, so a lag defined against the
> *newest* unexported record would read ≈0 during exactly the outage you need
> to see. The oldest-record age is the one an SLO like "export lag < 30s p99"
> can actually be measured against.

The gauge is emitted on **every** exporter tick, including ticks that deliver
nothing — the signal must not go stale precisely when delivery has stopped.

A suggested alert: `harvest_audit_export_lag > 300` for 10 minutes. Sustained
lag means privileged-action logs are not reaching the SIEM. Nothing is lost —
the cursor is held rather than advanced — but the window during which a
compromise would be invisible is growing.

### `GET /admin/audit-export`

Read-only, admin-gated, cross-shard.

```json
{
  "sink_configured": true,
  "shards": [
    {
      "shard": 0,
      "cursor_seq": 4680,
      "last_assigned_seq": 4712,
      "pending_records": 32,
      "lag_seconds": 1.8,
      "delivery_state": "IDLE",
      "consecutive_failures": 0,
      "last_status": 200,
      "last_error": null,
      "last_delivered_at": "2026-08-31T04:11:07.902Z",
      "next_attempt_at": "2026-08-31T04:11:07.902Z"
    }
  ],
  "status": "complete",
  "unavailable_shards": []
}
```

`delivery_state` is `IDLE`, `DELIVERING`, `BACKOFF`, `RETRYING`, `RETIRED`, or
`NOT_STARTED`. `RETIRED` means an operator ran `decommission_cursor`: no
exporter owes this shard records and retention may purge them, so the row's
other fields are a frozen snapshot rather than live state.

`sink_configured` reports whether **the process serving this request** has a
sink installed, and nothing more. Read it carefully in a split deployment: an
API process that does not run the exporter reports `false` while export is
perfectly healthy on the worker fleet, so `false` on its own is not a fault.
Conversely `true` only tells you *this* process could export, not that the
process which actually ticks the scanner is configured.

The load-bearing signals for "nothing is exporting this shard" are
`pending_records` growing across two reads and `lag_seconds` rising, or a
`delivery_state` of `NOT_STARTED` that persists — all of which are properties
of the shared database rather than of whichever process answered.

An unreachable shard degrades the response to `"status": "partial"` rather than
failing the read.

---

## Redrive: recovering from sink-side data loss

Your SIEM lost a day. Rewind the cursor and Harvest re-exports:

```bash
curl -X POST https://app.example.com/api/harvest/admin/audit-export/redrive \
  -H 'Content-Type: application/json' \
  -d '{"shard": 0, "before": "2026-08-30T00:00:00Z"}'
```

`{"shard": 0, "to_seq": 4100}` rewinds to an exact sequence instead; supply
exactly one of `to_seq` or `before`. Records with `seq > to` re-export on the
next scanner tick, **byte-identical** to their first delivery — the export
sequence is never re-stamped — so your receiver's `(shard, seq)` dedup sees
exactly what it stored.

Three properties worth knowing:

- **A cursor can only ever move backwards.** A request that would advance it —
  or leave it exactly where it is, which includes replaying the same redrive
  twice — is refused with `400`, not applied. Advancing it would mark records
  delivered that never were, the exact gap this feature exists to make
  impossible.
- **`before` resolves conservatively.** It rewinds to one below the *lowest*
  sequence assigned to a record at or after that instant, never to the highest
  sequence before it. `occurred_at` is transaction start time, so commit order
  and timestamp order can disagree; anchoring this way means any skew makes the
  rewind reach *further back* (costing duplicate deliveries your receiver
  dedupes) rather than skipping records the operator asked for.
- **The redrive is itself audited** (`audit_export.redrive`), so re-exporting is
  as auditable as the operations being exported. The rewind and its audit
  record are **one transaction on one connection** — the audit row is written
  through the very connection holding the cursor lock — so an
  applied-but-unaudited redrive is not representable. That row lands in the
  *target shard's* audit log rather than the default shard's, unlike every
  other audited route: it describes a shard-scoped mutation, and a second
  connection's insert would commit independently (breaking the atomicity) and
  could self-deadlock when the target *is* the default shard. It is exported by
  that shard's own exporter like any other audit record. The `target_id`
  records the shard *and* the requested position (`shard=0;to_seq=42`), since a
  redrive is the one operation here that can trigger a mass re-export. A
  refused redrive (`no-op`, unknown shard) is audited as `FAILED`: nothing
  moved, and the trail must not say otherwise.
- **It invalidates in-flight deliveries.** The rewind bumps the shard's claim
  epoch, so a batch already in flight cannot acknowledge over it.

Only records still present in the audit table can be re-exported; a redrive
past the retention window returns whatever survives.

---

## Out of scope

- **Exactly-once delivery.** At-least-once is the contract; receivers dedupe on
  `(shard, seq)`, matching #605.
- **Hash-chained / Merkle tamper-proofing of the at-rest rows.** A worthy but
  separate cryptographic-audit-log effort. What ships here is gap-detectable
  off-box export, which removes most of the incentive to tamper at rest:
  rewriting a row in the database does not rewrite the copy the SIEM already
  holds, and deleting an **exported** row leaves a sequence hole the receiver
  can see.

  Be precise about the limit: a row deleted **before the exporter has sequenced
  it** — inside the window between the audited action and the next scanner
  tick, or anywhere in the backlog during a sink outage — never receives a
  sequence, so the surviving rows are stamped densely and there is no hole to
  detect. Tamper evidence begins at the moment a record is sequenced, not at
  the moment it is written. Shorten that window by keeping the export healthy;
  close it properly only with at-rest hash chaining, which is out of scope
  here.
- **Exporting workflow event history.** That is `HistoryArchiver` (#345). This
  is the audit trail only.

---

## See also

- `docs/runbooks/audit-trail.md` — what is audited and why.
- `docs/completion-callbacks.md` — the #605 pattern this replays.
- `docs/security-posture.md` — mounting the management API safely.
- `docs/cross-region-dr.md` — why the sequence is not a Postgres sequence.
