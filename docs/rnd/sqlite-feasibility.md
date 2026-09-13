# SQLite edge/local-first harvest — storage-trait feasibility report (issue #966)

> **Status: retrospective.** This report was written *after* the spike ran and
> after its recommendation was executed. That is unusual and worth stating in
> the first paragraph rather than burying: the spike (PR #1066) and its
> productization (`autumn-harvest-sqlite`, issue #1068, Phase 5.0) both landed
> before the written deliverable this issue asked for. The report is therefore
> a **decision record backed by shipped evidence** rather than an estimate
> made in advance of one. Where a forward-looking spike report would have said
> "we estimate X", this one says "X, measured" — which makes it more useful,
> not less, but the reader should know the arrow of time.
>
> Nothing here re-opens the decision. It exists so that (i) the coupling
> inventory is written down and **kept honest by CI**, (ii) the road not taken
> — a `StorageBackend` trait inside core — is costed so nobody re-litigates it
> from scratch, and (iii) the hub-sync question is scoped before someone builds
> it by accident.

**Audit date:** 2026-08-12. **Audited revision:** `195a663`.

The inventory in this document is not hand-maintained trivia. It is
re-derived from `autumn-harvest/src/*.rs` on every CI run by
`autumn-harvest/tests/integration/sqlite_feasibility_docs.rs`, which fails the
build if a module becomes Postgres-coupled without being added here, if a row
here names a module that no longer exists, if a row is left unclassified, or
if the counts quoted below drift from a live grep. Treat the numbers as
current for the audited revision, not as prose.

---

## Decision summary

**Go — as a separate companion crate, not as a `StorageBackend` trait in core.**

| Question | Answer |
|---|---|
| Is harvest's determinism core backend-portable? | **Yes, already.** It consumes plain values (`ExecutionId`, `Vec<WorkflowEvent>`, a handler `fn`, JSON) — no connection, no trait object. |
| Is harvest's *coordination* layer backend-portable? | **No.** Multi-worker claim, push notification, and cross-connection locking are the three load-bearing Postgres features, and SQLite substitutes them only by dropping capability. |
| Did the prototype work? | **Yes — 4/4 durability scenarios**, plus cross-backend replay. Now productized. |
| Should core grow a `StorageBackend` trait? | **No.** Buildable, but costed below at a scale the benefit does not justify — 25 of 58 coupled modules are portable only by dropping a capability or reimplementing wholesale, for a use case that does not share the Postgres concurrency model. |
| What shipped instead? | `autumn-harvest-sqlite` — reuses the determinism core wholesale, reimplements persistence only. |

The one-sentence version: **the valuable half of harvest is already portable
and the coupled half should not be abstracted, so the correct seam is a
separate crate that reuses the core and rewrites persistence — which is what
was built.**

---

## Coupling mechanisms

Ten distinct Postgres-coupled mechanisms appear in core. The counts are
module counts at the audited revision, recomputed by CI:

| Mechanism | Reach | Portable? |
|---|---|---|
| `diesel` query layer | 58 modules | Query construction is mechanical; the *type* layer is not. |
| `skip-locked` claim (`FOR UPDATE SKIP LOCKED`) | 15 modules | Only by dropping multi-worker concurrency. |
| `row-lock` blocking row lock (Diesel `.for_update()`) | 17 modules | Subsumed by the single write lock. |
| `interval-sql` (`INTERVAL '…'`, `make_interval()`) | 13 modules | Yes — integer epoch milliseconds. |
| `raw-sql` — reaches for Diesel's raw-SQL escape hatch (`sql::<…>`, `sql_query`) | 40 modules | Case by case — the SQL must be read, not inferred from the ORM. |
| `raw-pg-sql` — *identified* Postgres-only syntax within that SQL (JSONB `#>>`/`@>`, `::TYPE` casts in either case, `EXTRACT(EPOCH …)`, `JOIN LATERAL`, `~` regex) | 28 modules | Mostly — but each is a hand rewrite, and `~` has no SQLite equivalent at all. |
| `advisory-lock` (`pg_advisory_*` / `pg_try_advisory_*`) | 12 modules | Subsumed by the single write lock. |
| `to_regclass` table-existence probes | 9 modules | Yes — `sqlite_master` lookup. |
| `listen/notify` push wakeups | 4 modules | No — polling is a degradation, not a translation. |
| `gen_random_uuid` server-side ids | 1 module | Yes — mint application-side. |

Plus **104 migrations** written in Postgres DDL (`JSONB`, `TIMESTAMPTZ`,
`INTERVAL`, `UUID`, partial indexes, `gen_random_uuid()` defaults), none of
which apply to SQLite. The SQLite crate does not translate them; it declares
its own schema.

**58 of the 113 core modules** exhibit at least one mechanism — a shade under
half. That ratio is the headline finding, and it cuts *both* ways: the
determinism core really is clean, and the persistence layer really is
saturated.

### Two of these rows are different in kind — read them differently

`raw-sql` is a **closed** rule and `raw-pg-sql` is an **open** one, and the
distinction matters more than either count.

`raw-pg-sql` enumerates Postgres-only syntax. An enumeration cannot be
complete: four review rounds of this document each found another construct the
list had missed — first the blocking row lock, then JSONB `#>>`/`LATERAL`/`~`,
then JSONB `@>`/`||` and `NOW()`, then every *lowercase* spelling of the casts
the list already named. Each miss silently understated the port, always in the
same direction. **Treat that count as a lower bound.**

`raw-sql` instead detects the *escape hatch* — any module reaching for
`sql::<…>` or `sql_query`. It cannot miss a construct, because it does not
enumerate constructs. It is a weaker claim (raw SQL may be portable ANSI), so
it is not evidence of coupling by itself. What it does establish is that such a
module's portability **cannot be read off its Diesel usage** — which is exactly
what class (a) asserts. CI enforces the consequence: a module using raw SQL may
never be classified (a).

The fourth round is the sharpest evidence that this split is the right one. It
more than doubled `raw-pg-sql`, 8 modules → 18. **Not one classification
changed**, because the closed rule had already caught all eighteen: every one
was already (b) or (c) on the strength of hand-written SQL alone. The open rule
kept being wrong about *how much*; the closed rule was never wrong about *which
class* — which is the only thing a reader makes a decision on.

That has held every time since. The table above quotes the live counts, which
have grown past that round's 18 as new Postgres-only syntax lands; each module
the open rule newly catches has so far already been (b) or (c). The counts move;
the classifications do not.

That 40 of the 58 coupled modules hand-write SQL is therefore the more
decision-relevant number than any dialect tally. It is the volume of query text
a second backend must re-author by hand, and it is knowable exactly.

Two of the four misses are worth separating out, because they were **not**
new constructs and were not open-ended. `pg_advisory` does not substring-match
`pg_try_advisory_xact_lock` (`try_` is infixed), and the cast list named only
UPPERCASE type spellings. Both were the *same* mechanism written with too
narrow a token — a bounded dimension, closable exactly, and now closed: the
advisory pair `pg_advisory`/`pg_try_advisory` covers all ten of Postgres's
advisory-lock functions exhaustively, and casts are matched on cast *position*
(`'::`, `)::`) rather than by enumerating type names. When a rule can be
closed, close it; the open list is the residue that genuinely cannot be.

### The finding a SQL-only grep would have missed

Two modules — `store` and `concurrency` — reference `SKIP LOCKED` **only in
comments**. They issue no such SQL; they *rely on the invariant that someone
else does*. `concurrency`'s per-key limit is enforced "across the whole fleet"
precisely because the claim query serialises it, and `store` skips a TOCTOU
guard because "tasks are serialised per-execution by SKIP LOCKED".

(A third comment-only consumer, `error`'s `SuspendedClaimAmbiguous`, joined
this list after the audited revision above — issue #1182 — for the identical
reason: the variant exists only to represent an ambiguity a `SKIP LOCKED`
probe can produce, and would have no reason to exist under a single-writer
engine that produces no such ambiguity. See the inventory row below; the
count in *Coupling mechanisms* above is kept current by CI, not this prose.)

This is the single most important structural result in the audit. A
`StorageBackend` trait derived mechanically from SQL call sites would have
abstracted the *writers* of that invariant and silently orphaned its
*consumers*, which would compile, pass unit tests, and be wrong under
concurrency. Coupling in harvest is not only syntactic, and that is why the
classification below is a human judgement rather than a generated table.

---

## Postgres-coupling inventory

Classification rule:

- **(a) trivially trait-able** — plain query/CRUD through Diesel with no
  Postgres-specific semantics. A trait method signature falls straight out.
- **(b) needs a semantic substitute** — uses a Postgres-specific mechanism for
  which SQLite has a *semantically equivalent* replacement.
- **(c) fundamentally Postgres-shaped** — "porting" it means either **dropping
  a capability** or **reimplementing the module wholesale**. Not a translation.

| Module | Coupling | Class | Substitute on SQLite / note |
|---|---|---|---|
| `activity_pause` | diesel, raw-sql | (b) | Claim-time gate on one activity type. The snapshot-window re-check and the two-pass resume credit exist only for READ COMMITTED; a single writer subsumes both. |
| `admission_gate` | diesel, advisory-lock, raw-sql | (b) | Advisory lock subsumed by the single write lock. |
| `audit` | diesel, raw-sql | (b) | Append-only row writes, plus one hand-written statement: the retention purge carries a `NOT EXISTS` guard against `harvest_audit_export_cursor` so a sweep can never delete an audit record the exporter has not yet shipped (#953). Written as one statement deliberately -- a read-then-delete pair would widen the window in which a concurrent redrive lowers the cursor between the two. Plain DML with nothing Postgres-specific in it, so the substitute is a direct transcription; it is (b) rather than (a) only because the SQL is hand-written and must be read rather than inferred from the ORM. |
| `audit_export` | diesel, raw-pg-sql, raw-sql, row-lock | (b) | Off-box audit export (#953). `SELECT ... FOR UPDATE` on the per-shard cursor row serializes exporters (`row-lock`); a single writer subsumes it. The sequence-assignment statement uses a `row_number() OVER (ORDER BY ...)` window in an `UPDATE ... FROM` CTE -- SQLite has window functions (3.25+) but not `UPDATE ... FROM` before 3.33, so it needs a correlated-subquery rewrite. |
| `backup_verify` | diesel, interval-sql, raw-pg-sql, raw-sql | (b) | Read-only post-restore probes (issue #943). Every statement is a `SELECT`, but the reused scanner predicates carry `NOW() - ($1 * INTERVAL '1 second')` interval arithmetic — rewrite against integer epoch ms, as `build_routing` does. `COUNT(*) OVER ()` already works (SQLite window functions, 3.25+). |
| `batch` | diesel, raw-pg-sql, raw-sql | (b) | JSONB `\|\|` concatenation and `search_attrs @> $jsonb` containment. SQLite JSON1 has neither — rewrite with `json_patch`/`json_extract`. |
| `build_routing` | diesel, interval-sql, raw-sql | (b) | Integer epoch ms for the interval arithmetic. |
| `calendar` | diesel | (a) | Plain CRUD. |
| `codec_rotation` | diesel, interval-sql, raw-pg-sql, raw-sql, to_regclass | (b) | Lazy re-encryption sweep (issue #948). Every Postgres-ism is a rewrite, not a capability: `::jsonb`/`::TEXT` casts, a `JOIN LATERAL` over the payload-field allowlist, a `~` regex validating the stored key id, and a `to_regclass` probe for the cursor table. The `~` check is the only awkward one — validate the key id in Rust, as the decoder already does. No claim, no lock: the sweep is a batched scan whose writes are compare-and-swaps. |
| `completion_callback` | diesel, skip-locked, row-lock, to_regclass, raw-sql | (c) | Two-transaction claim scanner; multi-worker delivery dropped. |
| `completion_trigger` | diesel, skip-locked, advisory-lock, raw-sql | (c) | Terminal-commit fan-out; claim semantics dropped. |
| `concurrency` | diesel, skip-locked, advisory-lock, raw-pg-sql, raw-sql | (c) | Was a pure consumer of the claim invariant; the latest-wins supersede path (#811) added a `pg_advisory_xact_lock(hashtext(key)::bigint)` critical section and a raw candidate scan of its own. Per-key fleet limits are meaningless single-writer, and the advisory lock is subsumed by the single write lock. |
| `context` | diesel, listen/notify | (c) | Wakeup path; no push primitive exists. |
| `cross_shard_child` | diesel, interval-sql, raw-pg-sql, raw-sql, row-lock, to_regclass | (c) | The cross-shard child relay (#956). Not a translation problem — the module exists **because** there is more than one database. A single-file SQLite deployment has exactly one shard, so the whole capability (placement, the relay, the outbox row) has nothing to do and would be dropped wholesale, exactly like the per-key fleet limits in `concurrency`. Its own mechanisms are mild (Diesel, one `FOR UPDATE` on the parent row before the terminal append — subsumed by the single write lock — a raw `INTERVAL` retry-backoff predicate that would become integer epoch ms exactly as `build_routing` does, and a `to_regclass('…')::text` probe that lets the relay skip quietly on a database whose migrations have not run — `sqlite_master` answers the same question); the coupling is architectural, not syntactic. |
| `debounce` | diesel, skip-locked, to_regclass, raw-pg-sql, raw-sql | (c) | Scanner claim; `sqlite_master` probe for the table check. |
| `dlq` | diesel, row-lock, raw-sql | (b) | Row lock on replay/redrive; subsumed by the single write lock. |
| `erase` | diesel, row-lock | (b) | Scrub holds a row lock; subsumed by the single write lock. |
| `error` | diesel, skip-locked | (c) | **Comment-only consumer, like `concurrency` above and `store` below.** `SuspendedClaimAmbiguous` (issue #1182) represents the ambiguity a `SKIP LOCKED` claim probe can produce; a single-writer engine has no such ambiguity to represent, so the variant itself would not exist there. |
| `event_batch` | diesel, skip-locked, to_regclass, raw-pg-sql, raw-sql | (c) | Scanner claim. |
| `execution` | diesel, skip-locked, row-lock, interval-sql, raw-pg-sql, raw-sql | (c) | Start/reuse matrix under `FOR UPDATE`; row-lock ordering is load-bearing. |
| `external_target_location` | diesel | (c) | Placement-aware resolution for `workflow_id`-addressed signal/cancel (#1146). Like `cross_shard_child` above, the coupling is **architectural, not syntactic**: the module exists *because* there is more than one database. It answers "which shard holds this business key?" by fanning a read across every expected shard and merging the per-shard answers. A single-file SQLite deployment has exactly one shard, so the fan-out has nothing to do — the engine's own single-shard short-circuit already skips it — and the whole module would be dropped, leaving the shard-local resolver it delegates to (`execution::resolve_execution_id_by_workflow_id`) as the entire answer. Its lone mechanism is the `AsyncPgConnection` in its signatures. |
| `external_task` | diesel, row-lock | (b) | `find_by_token_locked` serialises completion/failure; subsumed. |
| `handle` | diesel | (a) | Read paths. |
| `heartbeat` | diesel | (a) | Batched last-write-wins update. |
| `hot_swap` | diesel | (a) | **Nothing to port: the match is a doc comment.** The runtime workflow-module registry (issue #967, behind the `hot-code-swap` feature) is a purely in-process table of compiled WASM modules and never touches a connection. `diesel_async` appears once, in prose explaining why the method is named `load_module` rather than `load` — `RunQueryDsl`'s blanket impl would otherwise capture the shorter name. Kept as a row rather than an exclusion because the audit is grep-level by design and will keep finding it. |
| `hot_swap_store` | diesel, raw-sql | (b) | The `harvest_workflow_modules` table (issue #967, `hot-code-swap` feature): publish, fetch, list, retire. The raw SQL is upserts and one `UPDATE`; the only constructs needing a rewrite are `octet_length` → `length`, `now()` → `datetime('now')` and `ON CONFLICT DO NOTHING`, which SQLite spells identically. No Postgres-only syntax was identified, hence (b) rather than (c) — but it hand-writes SQL, so not (a). |
| `lib` | diesel | (a) | `embed_migrations!()` only. |
| `migrate` | diesel, raw-sql, to_regclass | (c) | Applies the Postgres migration set to a dedicated Harvest database (issue #1240). The ledger CRUD and the `to_regclass` probe are mechanical (`sqlite_master`), but the payload is 96 files of Postgres DDL: `autumn-harvest-sqlite` declares its own schema rather than translating them, so there is nothing here to port. |
| `models` | diesel | (c) | Postgres type layer (`Jsonb`/`Timestamptz`/`Interval`/`Uuid`); reimplemented wholesale. |
| `mutex` | diesel, advisory-lock, to_regclass, interval-sql, raw-pg-sql, raw-sql | (b) | Advisory lock subsumed by the write lock; lease TTL as epoch ms. |
| `notify` | diesel, listen/notify, raw-sql | (c) | **The one mechanism with no SQLite equivalent at all.** Polling replaces it. |
| `partition` | diesel, raw-pg-sql, raw-sql | (c) | Native Postgres declarative partitioning of `harvest_events` (issue #958). Nothing to translate: SQLite has no partitioned tables, no `ATTACH PARTITION`, and no metadata-only `DROP TABLE`-as-reclamation — and it does not need them. The pain this module removes is dead-tuple bloat and autovacuum pressure from row-level retention deletes, neither of which a single-writer SQLite file exhibits in the same way (`DELETE` there is followed by an incremental vacuum the single writer already owns). The whole module is a no-op under the SQLite backend, which is why it is (c) rather than a port: it is Postgres-specific *relief for a Postgres-specific problem*. |
| `poison_pill` | diesel, skip-locked, row-lock, interval-sql, raw-pg-sql, raw-sql | (c) | Crash reclaim keyed on *peer* worker liveness — no peers single-writer. |
| `queue` | diesel, skip-locked, row-lock, advisory-lock, listen/notify, interval-sql, raw-pg-sql, raw-sql | (c) | The claim path itself. Reimplemented on `BEGIN IMMEDIATE`. |
| `queue_pause` | diesel, skip-locked, advisory-lock, raw-pg-sql, raw-sql | (c) | Claim-time gate. |
| `quota` | advisory-lock, diesel, raw-pg-sql, raw-sql | (b) | Admission-time check inside the start transaction, no scanner. `pg_advisory_xact_lock(hashtext(key)::bigint)` serialises concurrent admissions for the same key — subsumed by the single write lock. The `history_bytes` aggregate sums `pg_column_size(event_data)`; SQLite's `length(event_data)` is a direct substitute. |
| `quota_reconcile` | diesel, raw-sql | (b) | Periodic backfill sweep for pre-upgrade `quota_key` rows (issue #1226). Its own SQL text is a `SELECT ... LIMIT $3` and one `UPDATE ... WHERE id = $2 AND quota_key IS NULL`, with no token the audit's `raw-pg-sql` list matches (the candidate scan's `ANY($1)` array bind is genuinely Postgres-specific, but is a dialect rewrite -- an `IN (?, ?, ...)` placeholder list -- not a capability gap, and outside what that list enumerates). It is (b) rather than (a) only because the SQL is hand-written and must be read rather than inferred from the ORM. Two mechanisms live one call away rather than in this module's own source, so they are attributed to the modules that actually contain them, matching how `execution`'s row treats the identical pattern: the write takes `pg_advisory_xact_lock` through `quota::lock_quota_key` (counted on `quota`'s row) to serialize with a concurrent admission for the same key -- `WHERE quota_key IS NULL` on both the scan and the write is what makes two concurrent SWEEPS safe on its own, the lock exists for the admission race specifically -- and every write asserts the cross-region DR fence through `replication::assert_fence` (counted on `replication`'s row), the ordinary-CRUD half that row already notes "would survive" a port. |
| `replication` | advisory-lock, diesel, interval-sql, raw-pg-sql, raw-sql | (c) | Cross-region DR fencing and RPO measurement (issue #954). The **most** Postgres-bound module in the inventory: it reads `pg_stat_replication`, `pg_replication_slots`, `pg_current_wal_lsn()` and the `pg_lsn` type, none of which SQLite has in any form — there is no replication to observe, so there is nothing to translate. The fencing epoch itself is ordinary CRUD and would survive; the measurement half does not. Advisory lock subsumed by the single write lock; `make_interval(secs => …)` as epoch ms. |
| `reset` | diesel, row-lock | (b) | Fork takes a row lock before appending; subsumed. |
| `retention` | diesel, skip-locked, row-lock, raw-pg-sql, raw-sql | (c) | Batched delete scanner with claim. |
| `schedule_decision` | diesel | (a) | Append-only decision log. |
| `scheduler` | diesel, row-lock, advisory-lock, interval-sql, raw-pg-sql, raw-sql | (b) | Cron/interval arithmetic as epoch ms. |
| `schema` | diesel | (c) | Diesel `table!` definitions; reimplemented wholesale. |
| `sessions` | diesel, row-lock, interval-sql, raw-pg-sql, raw-sql | (b) | Lease expiry as epoch ms. |
| `signal` | diesel, row-lock | (b) | Insert under a row lock; subsumed by the single write lock. |
| `shard` | diesel | (a) | Routing and pool construction. The router itself is pure arithmetic over shard ids; the only Diesel contact is `ShardedDbPool::from_dsns` building a connection pool per shard (issue #964). A single-file SQLite deployment has exactly one shard, so this is a one-entry map rather than a port. |
| `shard_rebalance` | diesel, raw-pg-sql, raw-sql | (c) | Migrating quiescent executions between shards (issue #964). Coupled the same way `cross_shard_child` is: the module exists **because** there is more than one database, so under a single-file backend the whole capability has nothing to do and would be dropped wholesale rather than translated. Its syntax is Postgres-bound throughout — the copy round-trips whole rows through `to_jsonb` / `jsonb_populate_record` / `jsonb_to_recordset` (SQLite's JSON1 has no record-shaped equivalent, and the point of using them is schema-drift safety, which a hand-written column list would lose), and the cutover is one statement built from data-modifying CTEs whose `sealed` output feeds two further `UPDATE`s — SQLite's CTEs cannot contain DML at all, so the atomic seal-and-cancel would have to become several statements inside the single write lock. |
| `start_idempotency` | diesel, to_regclass, interval-sql, raw-sql | (b) | `ON CONFLICT` upsert has a direct SQLite form. |
| `store` | diesel, skip-locked, row-lock | (c) | **Consumer of the claim invariant — issues no `SKIP LOCKED` SQL of its own.** Event append itself is (a); its TOCTOU assumption is not. |
| `testing` | diesel | (a) | Test-only helpers. |
| `throttle` | diesel, skip-locked, to_regclass, gen_random_uuid, raw-pg-sql, raw-sql | (c) | Token-bucket scanner claim; the accrual formula itself (issue #945) lives behind `queue::effective_available_tokens_expr`. Issue #1230 Finding 2 added its own `sql_query`: a pre-fire pass resolves every distinct quota key's real advisory-lock id with one `hashtext(n)` round trip over `unnest($1::text[])`, so rows sort by lock id instead of by string, closing an ABBA deadlock the earlier string-sort left open. |
| `timeout` | diesel, skip-locked, row-lock, advisory-lock, raw-pg-sql, raw-sql | (c) | The scanner family; lock ordering vs the claim path is load-bearing. |
| `usage` | diesel, raw-pg-sql, raw-sql | (b) | Aggregate reads, but through `JOIN LATERAL`, `EXTRACT(EPOCH …)` and `::` casts. Rewrite as a correlated subquery + `strftime`/`CAST`. |
| `version_gate_retirement` | diesel, raw-pg-sql, raw-sql | (b) | Marker scan over JSONB `#>>` with a `~ '^[0-9]{1,19}$'` guard. SQLite has no regex — substitute `GLOB`/`CAST`. |
| `version_usage` | diesel, raw-pg-sql, raw-sql | (b) | Same JSONB-path + POSIX-regex shape as `version_gate_retirement`. |
| `wasm_store` | diesel, advisory-lock, raw-pg-sql, raw-sql | (b) | Content-hash upsert; advisory lock subsumed. |
| `worker` | diesel, skip-locked, row-lock, advisory-lock, listen/notify, raw-pg-sql, raw-sql | (c) | The dispatch loop; wakeups and persistence are interleaved. |
| `workers` | diesel, interval-sql, raw-pg-sql, raw-sql | (b) | Fleet registry rows, but the sticky-lease filter embeds `NOW()` and the capability-miss fleet lookup adds an `INTERVAL` liveness window plus a `queues @> to_jsonb($2::text)` containment test. SQLite: `CURRENT_TIMESTAMP`/epoch ms; JSON1 `EXISTS (SELECT 1 FROM json_each(queues) …)` for the containment. |

**Totals: (a) 8 · (b) 25 · (c) 25.**

The shape matters more than the totals. The (a) column is genuinely
mechanical CRUD. The (b) column is dominated by **pessimistic row locking**:
15 modules take a blocking `.for_update()` lock, and every one of them is
portable only because the single-writer model subsumes the lock — the same
argument that carries the advisory locks. The (c) column is narrow but
load-bearing: it contains the claim path, the wakeup path, the scanner family,
the start/reuse matrix, and the type layer. **Those are precisely the modules a
`StorageBackend` trait would have to abstract, and precisely the ones whose
semantics do not survive abstraction.**

Row locking deserves its own line because it is the mechanism most likely to
be mistaken for plain CRUD. A first cut of this inventory classified `dlq`,
`erase`, `external_task`, `reset` and `signal` as (a) — they *look* like
straightforward inserts and updates, and their Diesel coupling is
unremarkable. All five in fact hold a `SELECT … FOR UPDATE` row lock to
serialise concurrent writers (`external_task::find_by_token_locked` is the
clearest case: it exists solely to serialise completion against failure).
Under a single writer that lock is free; under any multi-writer substitute it
is load-bearing. Misreading it as CRUD is exactly the error that would make a
port look cheaper than it is.

---

## Sizing a hypothetical `StorageBackend` seam

This seam was **not built**. This section costs it so the option is closed
with a reason rather than left as folklore.

### What it would have to cover

A trait that genuinely allowed a second backend would need, at minimum:

1. **Event store** — append (with the sequential-id contract), load, delta-load.
   *Trait-able cleanly.* ~6 methods.
2. **Task queue** — enqueue, claim, complete, fail, requeue, park, wake, plus
   the timer and signal tables. *This is the most demanding method.* The claim
   method's contract is not "give me a task"; it is "give me a task **such that
   no concurrent claimer can also get it**, without blocking on rows other
   claimers hold". That postcondition **is** statable as a trait contract
   independent of SKIP LOCKED — but the two backends satisfy its two clauses
   very differently, and one of them fails outright. See *Why the seam is the
   wrong shape*.
3. **The scanner family** — `timeout`, `retention`, `poison_pill`, `debounce`,
   `throttle`, `completion_callback`, `event_batch`: seven claim-batch-mutate
   loops, each relying on `FOR UPDATE SKIP LOCKED` for its concurrency safety.
   (Those seven are part of the `skip-locked` modules in the table above; the
   rest are `queue`/`queue_pause`/`execution`/`completion_trigger` plus the
   comment-only consumers described above. That count is kept current by CI,
   not by this prose.)
4. **Coordination primitives** — advisory locks with a *defined lock ordering*
   (documented in `timeout.rs` and the mutex work) and the notification
   channel.
5. **The type layer** — `models.rs`/`schema.rs`, ~2 500 combined lines (~100 KB)
   of Diesel definitions bound to Postgres column types.
6. **Migrations** — 83, in Postgres DDL.

### Why the seam is the wrong shape

The trait's hard part is not the method list. It is that **the two backends do
not share a concurrency model.** Postgres harvest is a multi-worker, multi-
replica fleet coordinating through the database. SQLite harvest is one writer
with a global write lock.

An earlier draft turned that into a dichotomy: a trait must either encode the
Postgres *mechanism* (SKIP LOCKED, advisory-lock ordering, push notification),
leaving every guarantee unverifiable on the SQLite side, or encode a weaker
intersection that strips the Postgres path of the coordination that makes it
correct. **That was a false dichotomy**, and — like the performance claim
corrected in the next section — it is fixed here rather than quietly dropped.

A trait *can* state the behavioural postcondition instead of the mechanism —
"a claimed task is claimed by exactly one caller, and claiming does not block
behind claims on unrelated rows" — and such a contract is verifiable against
both implementations. A guarantee that one backend satisfies trivially is not
thereby unverifiable: a single-writer test can still assert exclusivity, and
will pass. What the backends do not share is the mechanism, and a well-drawn
trait is not obliged to expose one.

Two narrower observations survive, and neither is a dichotomy:

- **The honest postcondition is not trivially satisfied — half of it is
  violated.** Exclusivity is trivial under a single writer. The *non-blocking*
  clause is not met at all: SQLite serialises on a global write lock, so a claim
  waits behind any concurrent write, whether or not it touches a row the claimer
  wants. So a trait stating the honest contract would have one implementation
  that fails it, and a trait weakened to accommodate that would no longer state
  the property the Postgres scanners are built on. That is a real cost of the
  seam — not a proof that the seam cannot be drawn.
- **A trait derived from SQL call sites would have been drawn in the wrong
  place.** `store` and `concurrency` depend on the claim invariant *without
  issuing the SQL* (see the finding above). Mechanically extracting a trait from
  call sites would have abstracted the invariant's writers and orphaned its
  consumers. That constrains how the seam must be *derived* — by human
  judgement, not by generation — not whether it can exist.

**So the trait is buildable.** The recommendation does not rest on impossibility;
it rests on the measured cost of building it, sized next, against the measured
absence of demand for what it would buy.

### Estimated build cost of the seam

| Component | Scope |
|---|---|
| Trait definition + Postgres impl | ~58 modules touched |
| Rewriting scanners against the trait | ~13 modules, each with a concurrency contract to re-specify |
| Type-layer abstraction | `models.rs` + `schema.rs` wholesale |
| Test matrix | Every DB-gated suite runs twice, with per-backend expectations where semantics diverge |
| Ongoing | Every future feature pays the trait tax — and this codebase ships features weekly |

Against a companion crate, which touched **zero** core modules.

---

## Cost to the Postgres path

The seam's cost to the shipped product is the largest single input to the
recommendation — though, per the note below, not a load-bearing one on its own.

**Performance — and what it does *not* depend on.** An earlier draft of this
report asserted that a `StorageBackend` trait would impose dynamic dispatch on
the claim path and force the fused claim query to decompose into multiple round
trips. Both claims were **design-specific, not inherent**, and stating them as
inherent overstated the case. They are corrected here rather than quietly
dropped, because the conclusion should not rest on the weakest argument
available for it.

Two trait designs are possible, and they trade off differently:

| | Fine-grained + `dyn` | Coarse-grained + generic (`<B: StorageBackend>`) |
|---|---|---|
| Dispatch | Virtual call per operation on the hot path | **None** — monomorphised |
| Fused claim CTE | Foreclosed; decomposes into round trips | **Retained** — the whole claim is one trait method, and the Postgres impl keeps its CTE inside it |
| Cost paid instead | Runtime | **Viral type parameters**: `Worker<B>`, the registry, and every function touching storage become generic, plus monomorphisation and compile-time cost |

So the honest position is that a coarse-grained generic seam **avoids both
performance objections**. If performance were the deciding factor, the trait
would be buildable.

**No single argument here is load-bearing — deliberately.** Twice now, an
argument nominated as *the* decisive one has not survived review: first the
dispatch and query-decomposition claims corrected immediately above, then the
"no statable shared contract" dichotomy corrected in *Why the seam is the wrong
shape*. The structural fix is to stop resting a recommendation on one limb. It
rests instead on the audit's measurements, none of which any review round has
disputed:

- **25 modules are class (c)** — portable only by dropping a capability or
  reimplementing wholesale — against 8 that are trivially trait-able.
- **40 modules reach for raw SQL**, so their portability cannot be read off
  their Diesel usage at all.
- The **capability losses are documented and unavoidable** on the single-writer
  side (*Known capability losses*, below), so the second backend is not the same
  product with a different file on disk.
- The companion crate delivered that capability at **structurally zero cost** to
  the Postgres path, against a seam sized above at ~58 modules touched.

Each subsection below is a cost, weighed against that. None is offered as a
proof that the trait is impossible; the previous section establishes that it is
not.

**Code complexity.** Harvest's correctness work is overwhelmingly about
concurrency edges — lock ordering, claim-vs-scanner races, the wake-request
mechanism, ABBA-deadlock avoidance. Every one of those fixes reasons about
concrete Postgres semantics. An abstraction layer between that reasoning and
the SQL would make the hardest class of bug in this codebase harder to see.

**Test matrix.** Doubling every DB-gated suite is not the real cost. The real
cost is that most of the interesting ones (multi-worker contention, lock
ordering, HA claim exclusivity) are **meaningless** on the single-writer side,
so the matrix would be mostly skips — carrying the maintenance weight of a
second axis while testing nothing new.

**Blast radius.** The companion crate's cost to the Postgres path is
**zero, structurally**: the dependency edge points one way
(`autumn-harvest-sqlite` → `autumn-harvest`, `default-features = false`), core
has no `rusqlite` dependency, and `cargo build -p autumn-harvest` output is
byte-unaffected. This is asserted by
`zero_regression_to_the_postgres_path_is_structurally_true`, not by prose.

---

## Test-suite portability

**No-DB suite: 100% passes, unchanged.** At the audited revision the core no-DB
set is 1849 lib tests plus 1188 integration tests, green with
`--no-default-features`. The SQLite work required no change to any of them —
which is the strongest single piece of evidence that the determinism core was
already backend-neutral.

**SQLite crate: 164 tests green** (43 unit + 120 integration + 1 doc), no
Docker required (`rusqlite` `bundled`).

### The four durability scenarios, and the tests that prove them

Deliverable 2 named four scenarios. Each maps to a named test in
`autumn-harvest-sqlite/tests/integration/durability.rs`, so the "4/4" claim in
the decision summary is checkable rather than asserted:

| Scenario (as worded in the issue) | Test |
|---|---|
| One workflow with two activities and a retry | `two_activities_with_retry` (plus `activity_retry_then_success` for the single-activity retry) |
| One durable timer firing across a process restart | `timer_fires_across_restart` |
| One signal delivery | `signal_delivery_unblocks_workflow` |
| Deterministic replay after a simulated crash | `deterministic_replay_after_crash_does_not_reexecute_activity` |

`orphaned_running_task_is_reclaimed_and_body_reruns` covers the fifth case the
prototype surfaced but the issue did not ask for: a crash *mid-activity*, where
the at-least-once boundary is visible.

Of the Postgres-gated suites, portability splits three ways:

| Category | Portable? | Examples |
|---|---|---|
| Replay determinism, event-schema fidelity | **Yes — and ported.** | Cross-backend replay, golden encoding, typed-failure metadata |
| Single-execution lifecycle | **Yes — ported with substitutes.** | Retry policy/backoff, timers across restart, signals, payload caps, reuse-policy matrix |
| Multi-worker contention | **No — semantically void.** | SKIP LOCKED claim exclusivity, HA scheduler claim (#350), sticky routing (#235), poison-pill peer-liveness reclaim (#367) |
| Fleet/operational surfaces | **No — out of model.** | Sharding, management API, Vantage UI, build routing, worker fleet registry |

The third row is the honest one: those tests are not "hard to port", they are
**not meaningful**. A claim-exclusivity test on a single-writer backend asserts
a tautology. Porting them would manufacture false confidence.

### Cross-backend portability evidence

The invariant that the event log is backend-portable is proven, not asserted,
by `sqlite_history_replays_on_core_replayer` in
`autumn-harvest-sqlite/tests/integration/cross_backend.rs`: a history written
by the SQLite backend replays clean on the core `WorkflowReplayer`. Alongside
it, `stored_event_matches_golden_encoding` pins the adjacently-tagged JSON
byte-for-byte and `pg_shaped_history_with_activity_started_is_replay_equivalent`
proves a Postgres-shaped history (which carries an `ActivityStarted` the SQLite
backend never writes) is replay-equivalent.

**No new `WorkflowEvent` variant was introduced.** That was the point: the
event log is portable *because* the schema did not change.

---

## Recommendation

**Go — as a separate companion crate (option ii). Do not add a
`StorageBackend` trait to core.**

Executed as `autumn-harvest-sqlite` (issue #1068, Phase 5.0; hardened in
Phase 5.1). Rationale, in priority order:

1. **The expensive half is already free.** The determinism core needed no
   abstraction — it was reused verbatim. A trait would have been built to
   share code that was already shareable.
2. **The coupled half should not be shared.** The (c) modules encode a
   concurrency model the two backends do not have in common.
3. **Zero cost to the shipped product.** A separate crate cannot regress the
   Postgres path; a core trait provably would touch it.
4. **Saying "no" stays free.** If the edge use case dies, the crate is deleted
   and core is untouched.

**Load-bearing constraint, restated:** single writer, single process, one
SQLite file. This is not an implementation gap to close later; it is the
assumption that makes `BEGIN IMMEDIATE` a valid substitute for SKIP LOCKED. If
multi-writer edge deployment ever becomes a requirement, this decision must be
re-opened, not extended.

### Known capability losses

Multi-worker/multi-process; push wakeups (polling latency floor); per-key fleet
concurrency (#247); sticky routing (#235); poison-pill peer reclaim (#367);
sharding; management API and Vantage UI; child workflows, external
signal/cancel, updates, local and external activities, cancellable timers,
worker sessions, and continue-as-new — each **rejected loudly by name** at the
command layer rather than silently ignored. Activity execution is
**at-least-once** (a crash mid-body re-runs it on resume), so bodies must be
idempotent.

---

## Scoping the hub handoff

**Not built. Not designed. Scoped only**, so it is not built by accident.

Handing an edge-executed workflow to a Postgres hub raises three questions the
current design does not answer:

1. **`ExecutionId` shard semantics on an edge node.** `ExecutionId` encodes a
   shard in its first two bytes; the SQLite backend has no shard concept and
   mints ids the router resolves via the `UNENCODED` sentinel. An ingested
   history would need a shard assigned at ingest — which changes the id, which
   is the primary key the whole event log hangs off. Either ingest rewrites the
   id (and every reference to it) or the hub accepts foreign-shard ids. Neither
   is free.
2. **Conflict rules.** The uniqueness contract is `(workflow_name,
   workflow_id)` per shard. Two disconnected edge nodes can both start
   `order-42`. There is no merge semantics for two divergent event logs of the
   same logical workflow, and there should not be one invented casually —
   append-only histories do not merge.
3. **Replay fidelity on ingest.** A history is only safe to resume on the hub
   if the hub has a registered handler for that workflow type at a compatible
   version, and if every side effect the edge recorded is honoured verbatim.
   The cross-backend replay test proves the *format* is portable; it does not
   prove an arbitrary edge history is *resumable* on a given hub build. The
   ingest path would need the replay-canary treatment (#512) as an admission
   gate, plus a rule for the derived state (task rows, timers) that the event
   log does not carry.

A credible first cut would be **export-only**: ship terminal histories to the
hub for archival and analytics, with no resumption. That sidesteps all three
questions above and is a genuinely useful product on its own. Live handoff of
in-flight executions should be treated as a separate, larger design.

---

## Other embedded backends

**libSQL / Turso** is the strongest adjacent option: a SQLite fork that keeps
the file format and adds server-side replication, which is exactly the axis
this backend is weakest on. Because it is wire- and file-compatible, the
persistence layer would largely carry over; its embedded-replica model could
plausibly relax the single-writer constraint at the edge, and it is the first
thing to evaluate if multi-writer edge becomes a requirement. **DuckDB** is a
poor fit and should be declined: it is an OLAP engine optimised for columnar
scans, and harvest's workload is high-frequency small point writes and
row-level claims — the opposite. It would be interesting as an *analytics*
sink over exported histories, not as an execution store. **`sled`** and other
embedded KV stores would require reimplementing indexing and transactional
multi-table updates that SQLite provides for free, for no clear gain.

---

## Timebox and method

The spike was scoped as evidence-gathering, not productization: the
prototype was to be abandoned rather than polished once the four durability
scenarios passed. In practice it passed that bar and the recommendation was
acted on immediately, so the prototype was carried into
`autumn-harvest-sqlite` instead of being discarded — the outcome the timebox
was protecting against (indefinite polishing of throwaway code) did not
occur, but neither did the discipline of stopping at the evidence.

**The process lesson, recorded honestly:** the report should have preceded the
productization. The decision was sound and the evidence supports it, but
issue #966's own success metric — *"leadership can green-light or kill the
direction from the report alone"* — was not available to leadership at the
moment the direction was green-lit. For the next R&D spike, the written
deliverable should gate the productization issue, not trail it.

**Method.** The inventory was produced by grepping `autumn-harvest/src/*.rs`
for eight precise mechanism tokens, then classified by hand. The detector is
deliberately narrow, because an inventory padded with false positives would
discredit the report as badly as one with holes:

- A bare `INTERVAL` matches Rust constants such as
  `DEFAULT_WORKER_POLL_INTERVAL`; a bare `diesel` matches the guardrail lint's
  own prose listing forbidden crates. Four modules (`effective_config`,
  `guardrail`, `history_export`, `wasm_activities`) match the loose detector
  and are correctly excluded by the precise one.
- Row locking is detected through the Diesel DSL (`.for_update()`) rather than
  the raw `FOR UPDATE` text. Matching the text would flag `mutex` twice — once
  for a doc comment reading "No `FOR UPDATE`" and once for a unit test
  asserting `!expr.contains("FOR UPDATE")` — i.e. it would report a module as
  coupled *because it documents that it is not*. The DSL set is also a
  superset of the modules issuing genuine raw-SQL row locks (`queue` and
  `timeout`, both of which also use the DSL), and it sidesteps
  `FOR UPDATE OF t SKIP LOCKED`, which a naive "`FOR UPDATE` not followed by
  `SKIP LOCKED`" rule misreads as a blocking lock.

The exact tokens live in `MECHANISMS` in the guard test and are the executable
definition of "Postgres-coupled" for this audit. The row-lock mechanism was
added after review; it is the reason five modules moved from (a) to (b), and
a worked example of why the guard re-derives the inventory from source instead
of trusting the prose.
