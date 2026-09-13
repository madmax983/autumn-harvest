# Payload codec key rotation (issues #948, #1244)

Rotating the key that protects stored workflow payloads, and retiring the old
one with proof that nothing still depends on it.

## The problem this solves

The [`PayloadCodec`](../adr/0003-payload-codec-event-boundary.md) boundary lets
you encrypt every payload-bearing field before it touches `harvest_events`.
Because event rows are append-only and nothing ever rewrites them, that
encryption used to be permanent: after a key compromise — or a routine
compliance-mandated rotation — every byte of stored history remained encrypted
under the old key forever. Encryption you cannot rotate is compliance theater.

Harvest owns its storage, so it can do what an external event store cannot: run
an automated, progress-reporting, retirement-gated sweep that converts stored
history onto the new key.

## The shape

- Each codec envelope carries an optional **key id** (`kid`) alongside the
  existing `_harvest_codec_envelope` discriminator. It rides inside the
  envelope, which is already opaque payload content — no new `WorkflowEvent`
  variant, no change to the event JSON contract.
- The registry holds **many keyed codecs**, exactly **one active**. New writes
  use the active key; decode resolves *any* registered key, so a mixed-key
  history replays transparently for the whole migration window.
- An envelope with **no `kid`** — every row written before this feature — is
  defined to be under the key id `legacy`. Pre-upgrade rows decode unchanged,
  and while `legacy` is the active key nothing new writes a `kid` at all, so an
  un-rotated deployment's stored bytes are byte-identical to before.
- A background **lazy re-encryption sweep** walks event rows carrying a
  non-active key id, decodes with the old key, and re-encodes with the active
  one — batched, rate-limitable, idempotent, resumable.
- Retiring a key is **gated**: it is refused while any reachable shard still
  holds a row referencing it, and an *unreachable* shard blocks retirement too.
- Both preconditions are **structural, not operator discipline** (issue
  #1244): a durable `harvest_codec_key_state` table records each key's
  lifecycle fleet-wide, and activation checks every live worker's advertised
  envelope-reading capability before it flips new writes to a keyed codec.

## Wiring it up

> **Read this first — which writes are codec-aware.** Every payload-bearing
> write does (issue #1243). The worker replays with the *same* registry, so a
> mixed-key history round-trips end to end.
>
> This includes:
>
> - the worker's batched task-processing writes: activity results and
>   failures, workflow completions and failures, timers, signals, DLQ and
>   quarantine writes;
> - `ActivityCompleted.output` committed inline by `ctx.run_transactional`;
> - `ActivityFailed.details` from a broken session;
> - `WorkflowFailed.details` from the poison-pill reclaimer;
> - `execution.rs`'s start paths — `WorkflowStarted.input` and
>   `last_completion_result`, the first event of every execution;
> - every payload-bearing `store::append_single_event` call:
>   `ChildWorkflowStarted.input`, `ChildWorkflowCompleted.output`, a typed
>   `ChildWorkflowFailed.details` (both `worker.rs` and the cross-shard child
>   relay), and `ActivityCompletedExternally.output` (`external_task.rs`);
> - `UpdateAdmitted.input` (`store::admit_update_event`) and a workflow rerun's
>   own start input (`execution::rerun_workflow_execution`);
> - `scheduler.rs`'s three dispatch paths: a schedule tick, a buffered-run
>   drain, and a unified-DAG trigger;
> - `completion_trigger.rs`'s relay-gate-checked start, both
>   `evaluate_triggers_for_execution` variants, the outbox sweep, and a
>   cross-shard `DeferredTriggerStart` (the registry rides on the struct since
>   `spawn` runs detached from the evaluating call's scope);
> - the plugin layer: `admit_batched_start` (event-batch admission), the
>   outbox relay's workflow-start dispatch, a UI-triggered manual schedule
>   fire, and the outbound webhook-delivery start.
>
> Two groups stay on the identity registry, for different reasons:
>
> - Events with no payload-bearing field at all: parent-close cascade
>   bookkeeping, operator cancel/terminate reasons, pause/resume, and
>   `WorkflowRedriven`. Encoding these is a byte-for-byte no-op (see
>   `PayloadCodecs::encode_event`'s field list) — nothing in this group is
>   reachable from a workflow's real input, output, or error detail.
> - A disclosed residual gap: `cancel_workflow_execution_collect`,
>   `terminate_workflow_execution_collect`, and
>   `commit_workflow_execution_timeout` each fire a completion-trigger
>   evaluation with the identity registry. These three public functions have
>   many external callers with no configured registry threaded through, so
>   closing this gap needs a signature change beyond issue #1243's scope. The
>   events these three write directly carry no payload field either, so only
>   a *downstream* trigger-fired start could be affected — track closing this
>   under a follow-up issue.

```rust
use autumn_harvest::payload_codec::CODEC_LEGACY_KEY_ID;

let harvest = HarvestBuilder::new()
    // Your existing codec, under the legacy key id, so already-stored
    // (kid-less) history keeps decoding.
    .payload_codec_key(CODEC_LEGACY_KEY_ID, AesGcmCodec::new(old_key))
    // The incoming key.
    .payload_codec_key("2026-q3", AesGcmCodec::new(new_key))
    // Flip: from here, every new write is encrypted under 2026-q3.
    .active_payload_codec_key("2026-q3")
    .build()?;
```

The registry's rotation state is **shared across clones**, so
`PayloadCodecs::set_active_key` at runtime (a config reload) takes effect for
every writer immediately, in *this process*. There is no restart-ordering
window in which a clone taken before the flip keeps writing under the retired
key.

`.active_payload_codec_key(...)` at build time is a same-process bootstrap
convenience — the process is not serving traffic yet, so there is no fleet to
coordinate. For every activation after that, including your very first
runtime rotation, use `codec_rotation::activate_codec_key` (below) instead of
calling `set_active_key` directly: it is what gives `retire_codec_key`'s
default gate the durable record it needs.

## Running the sweep

The sweep is a resident of the existing timeout-scanner cadence, one bounded
batch per shard per tick. It costs nothing — not one statement — on a deployment
with no keyed codec registered.

| Knob | Meaning |
| --- | --- |
| `WorkerConfig::with_codec_rotation_batch_size(n)` | Rows examined per shard per tick. Default 200. |
| `…(0)` | Sweep off, no redeploy needed. |

**Sizing the batch.** The unit is *rows examined*, not rows converted — the
sweep walks `harvest_events` in `id` order and skips rows that need no work, so
a pass costs one visit per row on the shard. The scanner interval is
`WorkerRuntimeConfig::poll_interval`, which the builder fixes at
`DEFAULT_WORKER_POLL_INTERVAL` = **500 ms**, so the default 200 rows per tick is
roughly 400 rows/second and a 1M-row shard converges in about 40 minutes. Raise
the batch to convert faster (a first pass over a large corpus is the case that
wants a big number) or lower it to reduce the load a rotation imposes; it is
safe to change at any time, and once the first pass reaches the end of the shard
the cursor keeps it cheap forever after — later ticks only look at rows appended
since.

Watch progress:

```
GET /admin/codec/rotation      # admin-gated, read-only
```

```json
{
  "active_key_id": "2026-q3",
  "registered_key_ids": ["2026-q3", "legacy"],
  "shards": [
    {
      "shard_id": 0,
      "rows_by_key_id": { "legacy": 412, "2026-q3": 999588 },
      "rows_remaining": 412,
      "cursor": {
        "last_event_id": 998112,
        "rows_reencrypted": 999588,
        "completed_at": null,
        "updated_at": "2026-08-29T11:03:22Z"
      }
    }
  ],
  "rows_remaining_total": 412,
  "status": "complete",
  "unavailable_shards": []
}
```

`rows_remaining_total` is only a count when `status` is `"complete"`. Under
`"partial"` it is a **lower bound** — an unread shard's rows are unknown, never
zero.

Metric: `harvest.codec.reencrypted{shard}` counts swept rows. A rotation that
has stalled shows as
`rate(harvest_codec_reencrypted_total[5m]) == 0` while
`GET /admin/codec/rotation` still reports rows remaining.

## Activating a key

```rust
autumn_harvest::codec_rotation::activate_codec_key(
    &sharded_pool,
    &expected_shards,
    &codecs,
    "2026-q3",
    worker_stale_secs, // crate::worker::worker_stale_secs(your worker_heartbeat_interval)
).await?;
```

This is the fleet-safe path (issue #1244). Prefer it over the local-only
`PayloadCodecs::set_active_key` for every activation, including your very
first key — `retire_codec_key`'s default gate (below) needs a durable record
of when a key became active, and `set_active_key` alone never writes one.

It refuses with `HarvestError::CodecKeyActivationBlocked` while any live
worker's `harvest_workers` row does not advertise support for envelope
version 2 (see "Upgrade every reader" below), and is fail-closed the same way
retirement is: an unreachable shard, or a shard this process can see but
omitted from `expected_shards`, blocks activation on its own.

On success it durably marks `key_id` `active`, marks the previously active key
(if any) `retiring`, and flips this process's registry immediately — the same
zero-restart-window guarantee `set_active_key` always gave.

**Do not run two rotations at once.** `activate_codec_key` does not coordinate
across concurrent calls activating *different* keys: each shard resolves such
a race independently, so two operators rotating onto different keys at the
same time can leave different shards durably active on different keys.
Nothing detects that split automatically — run one rotation to completion
before starting another, and if you suspect a race happened, re-run
`activate_codec_key` with your intended key to converge every shard.

## Retiring the old key

```rust
use autumn_harvest::codec_rotation::FleetWriteFence;

autumn_harvest::codec_rotation::retire_codec_key(
    &sharded_pool,
    &expected_shards,
    &codecs,
    CODEC_LEGACY_KEY_ID,
    FleetWriteFence::NotConfirmed, // the structural gate; see below
    staleness_window,
    recheck_delay,
).await?;
```

It refuses with `HarvestError::CodecKeyRetirementBlocked` naming the remaining
count **per shard** while any row is left, and succeeds only at exactly zero
everywhere, confirmed **twice** (`recheck_delay` apart) before it finalizes
anything. It is fail-closed in every one of these ways, all deliberate:

- a shard with no connection pool in this process blocks retirement;
- a shard whose census errors blocks retirement;
- an empty shard list is refused outright — proving nothing is not proving zero;
- a shard list that omits a shard this process can see is refused;
- with `FleetWriteFence::NotConfirmed`, a shard whose durable key state is not
  `"retiring"` yet, or whose staleness window has not elapsed, blocks
  retirement.

### ⚠️ Retirement needs a fleet write fence — issue #1244 makes it structural

`PayloadCodecs` is a **per-process** registry: a purely local
`set_active_key` call is invisible to every other worker. The census only
sees rows that are committed and visible, at one instant, on the shards this
process can reach. Two things it therefore cannot see on its own:

1. **Another live writer.** A worker that has not yet rolled onto the new
   active key is still encoding under the old one, and will write another
   old-key row a millisecond after your census read zero.
2. **An in-flight append.** A transaction that already encoded its payload
   under the old key but has not committed is invisible to the census, and
   becomes visible immediately after it.

`FleetWriteFence::NotConfirmed` — the default, and the path this section is
about — closes hazard 1 structurally instead of by operator attestation:

- `activate_codec_key` durably stamps the superseded key `"retiring"` with a
  timestamp, on every expected shard.
- Every other process refreshes its view of the active key from that same
  durable table roughly once per scanner-tick interval (folded into
  `enforce_timeouts_once`, before any resident that can end the tick early —
  see `codec_rotation::refresh_active_codec_key`), **provided that process's
  scanner loop is actually keeping up.** Pool acquisition for the tick is
  bounded to one `interval`, but the enforcement pass itself is not: a slow
  or wedged query inside it can still delay a refresh past `interval`.
- So `staleness_window` is an operational margin, not a hard guarantee — set
  it well past **twice** your deployment's nominal tick interval to absorb
  ordinary jitter, and treat `crate::scanner_health`'s liveness signal, not
  this gate, as the thing that tells you a scanner has actually stopped
  ticking.

Hazard 2 is narrowed, not eliminated, by the built-in double census:
`recheck_delay` is how long to wait between the first (zero) census and the
recheck before finalizing. A wider delay catches a slower straggling commit at
the cost of a slower retirement call; it cannot close the hazard completely
without tracking individual transaction lifetimes, which this crate does not
do.

**The escape hatch.** `FleetWriteFence::ConfirmedByOperator` skips the durable
staleness-window check (the census still runs, twice) — for a single-process
embedder where "another live writer" cannot exist by construction, or a test.
Passing it elsewhere is asserting hazard 1 does not apply; Harvest cannot
verify that for you.

**A third source of a stale zero census is closed automatically (issue
#1251), not by attestation.** The re-encryption sweep pins the key it is
writing a batch onto, for the life of that batch. `retire_codec_key` refuses
a pinned key even when the per-shard census reads zero — the exact state a
batch is in right after it resolves its target and before it commits a
single row. This is process-local and needs no operator action, unlike the
two fleet-wide gaps above: it protects against this process's own sweep, not
against another worker or another process's in-flight append, which are
still invisible to it and still require the fence.

### ⚠️ Upgrade every reader before activating a keyed codec

Activating a non-legacy key switches new writes to **envelope version 2** (four
keys, carrying `kid`). A reader built before issue #948 recognises an envelope
only as exactly three keys with version 1, and its decoder returns anything
else *unchanged* rather than rejecting it. A pre-#948 worker therefore hands the
raw envelope object to workflow code as if it were the payload — silent wrong
data, not a loud failure.

`activate_codec_key` (issue #1244) enforces the deployment order
structurally: it refuses while any worker that has heartbeated within
`worker_stale_secs` does not advertise `codec_envelope_version >= 2` in its
`harvest_workers.labels` row. Every #1244-capable binary advertises this
automatically, on registration and every heartbeat — there is nothing to
configure. A binary built before #1244 never writes the label at all, so it
reads as version 1 and blocks activation by default (fail closed).

So the deployment order remains the same, now enforced rather than merely
documented:

1. Deploy the #1244-capable binary to **every** reader in the fleet.
2. Confirm the rollout completed (or just call `activate_codec_key` — it
   tells you who is still missing).
3. *Then* activate a keyed codec.

Registering keys is safe at any point: while the legacy key is active no `kid`
is written and envelopes stay version 1, byte-identical to what a pre-#948
deployment stores. It is the **activation** that must come last.

### ⚠️ What "zero" does and does not authorise

The gate proves one specific thing: **no `harvest_events` row on any expected
shard still references the key.** That is what the sweep converts, so that is
what the census counts. It is *not* a licence to destroy the key material yet,
because a codec envelope can also be sitting in places this feature does not
sweep:

- **Offloaded blobs** (issue #524). Offload composes *after* codec encode, so
  the ciphertext — and its key id — lives in your `PayloadStore`, while the DB
  row holds only a reference envelope. Those are the *large* payloads, and they
  are explicitly out of scope here (embedder-owned storage). Re-encrypt or
  re-key them yourself before retiring.
- **Codec-encoded columns outside the event log**:
  `harvest_workflow_executions.{input,output,memo,search_attrs,error}`,
  `harvest_execution_summaries.{result,search_attrs}`,
  `harvest_dead_letters.{input,error}`, `harvest_signals.payload`, and
  `harvest_completion_deliveries.payload` are all decoded on the read path
  (`decode_workflow_execution_fields` and friends) and are **not** swept or
  censused.
- **Nested envelopes.** The census and the sweep classify a payload field by its
  *top-level* envelope. A field whose decoded plaintext itself contains an
  envelope — e.g. an `ExternalAwaitResolved.output` frozen from another
  execution's raw column — is counted only by its outer key id.

So: treat a green gate as "the event log no longer needs this key", keep the key
registered (not destroyed) until you have independently accounted for the three
cases above, and prefer retiring the key from the registry well before
destroying the material. Closing these gaps is tracked as follow-up work.

Only after that, and after a successful retirement, should you dispose of the
key material itself. Harvest never holds it.

**Supplying `expected_shards`.** Pass every shard the deployment has, not just
the ones this process serves. The gate refuses outright when the list omits a
shard this process has a pool for, but it cannot see shards no process here
knows about — an omitted shard is not censused, and the gate's `Ok` would be
vacuous for it.

## What the sweep will not touch

- **Offloaded payloads** (issue #524). Offload composes *after* codec encode, so
  the stored field is a reference envelope, not ciphertext; re-encoding it would
  encrypt the reference and orphan the blob. Re-encrypting the blob in your own
  `PayloadStore` is embedder-owned and out of scope.
- **Erasure tombstones** (issue #495) — no ciphertext to rotate.
- **Plaintext fields.** The sweep migrates keys; it never newly encrypts history
  that was written in the clear.
- **Rows already on the active key**, which is what makes a re-run a no-op.

Because none of these carry a rotatable key id, none of them counts toward
`rows_remaining` — so none of them can block retirement forever.

## The append-only exception

Re-encryption mutates stored `harvest_events.event_data` bytes in place. That is
**sanctioned in-place mutation exception #3**, named alongside the other two in
the "Engine Invariants" section of the repository `CLAUDE.md`.

The scope guarantee that makes it safe: only the ciphertext bytes inside payload
fields change. The decoded plaintext is byte-identical before and after, and the
event `type`, variant structure, event ids, ordering and timestamps are never
touched — so replay determinism is unaffected **by construction**. It is proven
by `replay_fidelity_is_byte_identical_across_a_sweep`, which replays a fixture
history, runs the sweep, and replays again, asserting identical decoded
histories and `ReplaySucceeded` both times.

The sweep writes with a **compare-and-swap** on the row's previous bytes, so it
always loses a race against PII erasure — the only other code path that
mutates `harvest_events.event_data` after insert (see CLAUDE.md's Engine
Invariants; a heartbeat checkpoint mutates `harvest_task_queue`, not the event
log, so it is not a party to this race). Losing is the only safe direction:
writing re-encrypted ciphertext over an erasure tombstone would resurrect
payload data the erasure had just destroyed.

## Troubleshooting

**`rows_remaining` is stuck above zero.** A row the sweep cannot decode is
logged (by row id and the key ids it references, never content), skipped, and
counted as *unresolved*. A pass that reaches the end of the shard with a
non-zero unresolved count resets its cursor to 0 and runs again instead of
being marked complete, so re-registering a key that was removed too early is
enough — the next pass picks those rows up with no manual intervention. A
cursor whose `completed_at` is set is therefore a real signal: that pass
converted everything it saw.

**`status` is `"partial"`.** A shard is unreachable. Retirement will refuse until
it is readable again; that is intended.

**Nothing is being swept.** Check that a keyed codec is registered
(`registered_key_ids` is non-empty), that `codec_rotation_batch_size` is not `0`,
and that the active key's codec is not the identity codec — the sweep refuses to
replace ciphertext with plaintext.
