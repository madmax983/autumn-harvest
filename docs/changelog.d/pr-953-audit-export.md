## Phase 5.x — stream audit records to an external SIEM sink (issue #953)

Every mutating management-API operation already writes an audit record, but
those rows lived per-shard inside the same Postgres databases they describe —
readable only through Harvest's own API, by whoever holds Harvest credentials.
Harvest now streams them off-box.

- **`AuditSink` trait in core**, boxed and async, with no HTTP client
  dependency — the same embedder-supplied-transport seam as
  `CompletionCallbackDeliverer` (#605) and `PayloadStore` (#524). The plugin
  ships `ReqwestAuditSink`, a signed-webhook implementation posting JSON-lines
  batches with the `X-Harvest-Signature` HMAC scheme from #605 and
  `redirect::Policy::none()`. The record shape is documented as
  OTLP-logs-mappable for embedders bridging to a collector.
- **Cursor-based, at-least-once, per shard.** A durable per-shard cursor
  (`harvest_audit_export_cursor`) advances only after the sink acknowledges a
  batch; a failed delivery retries with capped exponential backoff and **never
  advances past the failure**. There is deliberately no dead-letter arm — an
  audit record is a compliance artifact, so the exporter retries forever.
- **Gap-detectable by construction.** Each record carries its shard id and a
  dense, strictly monotonic per-shard `seq`, so a receiver can check contiguity
  rather than merely detect gaps, and dedupe on `(shard, seq)`. The sequence is
  stamped by the exporter rather than a `BIGSERIAL` for two independent
  reasons: a serial is assigned pre-commit, so a late-committing transaction
  would be skipped forever by a `seq > cursor` cursor; and logical replication
  does not replicate sequence values, so a promoted DR standby would re-issue
  already-exported numbers.
- **Follows the established scanner pattern** — folded into the existing
  `enforce_timeouts_once` cadence, no new background task, with the #605
  two-transaction claim/deliver shape so no row lock is held across network
  I/O. Acknowledgements are guarded on a monotonic claim epoch.
- **`harvest.audit.export_lag{shard}`** (gauge, age of the oldest
  unacknowledged record — not the newest, which would read ≈0 during exactly
  the outage that matters) and **`harvest.audit.exported{shard}`** (counter),
  both labelled by shard only per ADR-0001 §7. Two dashboard panels added.
- **`GET /admin/audit-export`** reports per-shard cursor position, lag, last
  error, and delivery state. **`POST /admin/audit-export/redrive`** rewinds a
  shard's cursor by sequence or timestamp for re-export after sink-side data
  loss; re-exported records are byte-identical, the cursor can only ever move
  **backwards** (a forward request is refused, not applied), and the redrive
  itself writes an audit record.
- **Retention will never purge an unexported record.** A sweep that removed one
  would be a silent compliance gap — gone from the database *and* absent from
  the SIEM. When no sink is configured the guard finds nothing and the purge is
  unchanged.
- **Opt-in, but one cost is not zero**: no sink configured means no sequence
  assigned, no cursor row, and the scanner returns before issuing a query. The
  partial index still matches every row and costs insert-time maintenance
  regardless of configuration. Tracked as issue #1272. **No new
  `WorkflowEvent` variant, zero replay-determinism impact.**

New migration: `20260728000000_harvest_audit_export`. See
`docs/audit-export.md`.
