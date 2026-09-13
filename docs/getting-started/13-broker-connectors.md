# Chapter 13: Broker connectors (Kafka, SQS)

A message on a Kafka topic or an SQS queue is the other most common production
trigger for a durable workflow, alongside an inbound webhook
([chapter 12](12-webhooks.md)). This chapter binds one to a workflow — with
idempotent redelivery, correct ack ordering, poison isolation and backpressure
— in under 20 lines of your own code.

## The core engine never sees your broker

The connector lives in `autumn-harvest-plugin`, behind Cargo features. The
`autumn-harvest` engine crate stays Postgres-only:

```bash
cargo tree -p autumn-harvest --all-features | grep -E 'rdkafka|aws-sdk-sqs'   # empty
```

That is not a convention — `autumn-harvest-plugin/tests/connector_dependency_graph.rs`
runs exactly that query in CI and fails the build if a broker client ever
reaches the engine's graph. Everything the engine contributes is
dependency-free: four `harvest.connector.*` metric constants with no-op
`MetricsRecorder` defaults, and the additive `StartSource::Broker` provenance
value.

Features:

| Feature | Brings |
|---|---|
| `connectors` | The broker-**agnostic** layer: bindings, idempotency, ack ordering, poison isolation, backpressure, and `MockSource`. No broker client at all — enough to unit-test your mapping function and the whole dispatch path with no Docker. |
| `kafka` | `connectors` + `rdkafka`. |
| `sqs` | `connectors` + `aws-config` / `aws-sdk-sqs`. |

Building with `kafka` compiles vendored librdkafka, which needs libcurl headers
(`libcurl4-openssl-dev` on Debian/Ubuntu).

## 1. Kafka → a workflow start

```rust
use std::sync::Arc;
use autumn_harvest::prelude::*;
use autumn_harvest_plugin::HarvestPlugin;
use autumn_harvest_plugin::connector::{
    KafkaSource, KafkaSourceConfig, MappedMessage, SourceBinding,
};

#[derive(serde::Deserialize, serde::Serialize)]
struct OrderPlaced { order_id: String, total_cents: i64 }

#[workflow]
async fn fulfil_order(_ctx: &WorkflowContext, order: OrderPlaced) -> Result<String, String> {
    Ok(format!("fulfilled {}", order.order_id))
}

#[autumn_web::main]
async fn main() {
    let source = Arc::new(
        KafkaSource::connect(&KafkaSourceConfig::new(
            "localhost:9092", "harvest-orders", "orders.placed",
        ))
        .expect("kafka consumer should connect"),
    );

    let binding = SourceBinding::starts("orders", "orders.placed", "fulfil_order")
        .map_json(|_ctx, order: OrderPlaced| {
            let payload = serde_json::to_value(&order).map_err(|e| e.to_string())?;
            Ok::<_, String>(MappedMessage::new(format!("order-{}", order.order_id), payload))
        })
        .max_in_flight(32);

    autumn_web::app()
        .plugin(
            HarvestPlugin::new()
                .workflows(workflows![fulfil_order])
                .connector(binding, source)
                .worker(WorkerConfig::default())
                .api("/api/harvest"),
        )
        .run()
        .await;
}
```

Runnable: [`examples/kafka_connector_quickstart.rs`](../../autumn-harvest-plugin/examples/kafka_connector_quickstart.rs).

`SourceBinding::starts(binding_name, stream, workflow)` says *what* a message
maps to; `KafkaSource` is the adapter that feeds it. The mapping function is
**synchronous** and returns a `MappedMessage`: the workflow id, and the JSON
payload to hand the workflow as its start input.

`HarvestPlugin::build` **panics** on any of these, rather than letting them
surface as a silently idle consumer or a message that retries forever:

| Rejected | Why |
|---|---|
| Empty binding name, or empty stream | The name is the metrics `source` label and the idempotency-key namespace |
| No mapping function | Nothing to map with |
| Two bindings share a name | They would silently deduplicate each other's messages |
| Two bindings consume the same **physical subscription** (same brokers + group + topic; same SQS queue URL) | Both receive loops compete, so each binding sees an arbitrary *subset* rather than the whole stream |
| Target is a registered DAG | Trigger DAGs via `POST /dags/{name}/trigger` |
| Target workflow is not registered | Call `.workflows(...)` before `.connector(...)` |
| Adapter's `stream()` does not match the binding's | The consumer would never deliver anything |
| `IdempotencyMode::BrokerCoordinates` on a throttled / debounced / batched target | The start route rejects a keyed start for a deferred admission (`400`) |
| `signals_with_start` onto a throttled / debounced / batched target | signal-with-start refuses a fresh start on a debounced/batched workflow (so the first message per entity would be dead-lettered) and bypasses a throttle entirely |
| `.broker_native_dead_letter()` on an adapter with no dead-letter destination (Kafka always; SQS on a queue with no redrive policy) | Nothing quarantines the message, so a poison message would be re-read forever |

One case **warns rather than panics**: two bindings consuming the same
`(stream, target)` pair from *different* physical subscriptions. That is
legitimate fan-in — two independent brokers exposing the same stream name into
one workflow, as in an active/active deployment or a cluster migration — and
only you know whether you meant it. But against a single broker it
double-dispatches every message, because each binding namespaces its own
idempotency key by binding name. The warning names both bindings; if the
fan-in is deliberate, it is safe to ignore.

Note the distinction the panic draws. Two consumers on one topic under
**distinct** Kafka group ids are two subscriptions and each receives the whole
stream — that is the supported way to fan one topic out to two workflows. Two
consumers in the **same** group split the partitions between them, which is
what the panic catches. For Kafka the subscription identity is
`bootstrap.servers` + `group.id` + topic, read from the *effective* client
config (so a `.property("group.id", …)` override is what counts) and with the
broker seed list canonicalized, since it is a seed list rather than an address.

## Before you start: apply the plugin migration

The default dead-letter destination is `harvest_connector_dead_letters`, a
**plugin**-owned table shipped in
`autumn-harvest-plugin/migrations/harvest/`. It is applied to the **harvest**
database (the same one the core `harvest_*` tables live in), so under
`harvest.mode = split` or `external` it lands where the connector actually
writes, not in your application database.

Under `AUTUMN_PROFILE=dev` it is applied automatically at startup — by Autumn
in the default `embedded` mode (the plugin registers its migrations with the
framework), or by the plugin itself under `split` / `external`, where the
harvest database is one Autumn has no handle on. Outside dev, a pending
migration is only *warned* about, and it must be applied before you enable a
connector: otherwise the first poison message will fail its dead-letter write,
be downgraded to a retry, and redeliver forever.

Which command applies it depends on the mode, because `autumn migrate` only
ever reaches the **application** database:

```bash
# embedded (default): the application database IS the harvest database.
autumn migrate

# split / external: the harvest database is a separate one. `autumn migrate`
# does NOT apply this table there -- name the plugin's set explicitly.
export HARVEST_DATABASE_URL=postgres://…   # == harvest.database.url
harvest migrate run --include-dir autumn-harvest-plugin/migrations/harvest
```

The `--include-dir` set rides along with Harvest's own embedded migrations, so
that one command leaves the harvest database fully migrated. See
[Migrations](10-operations.md#migrations) for the whole procedure.
(Use `.broker_native_dead_letter()` on SQS if you would rather not have the
table at all — that needs a redrive policy on the queue; see
[Poison messages](#poison-messages).)

### Production configuration

The 17-line wiring above is honest for a local broker. Two things a managed one
needs, neither of which changes the shape:

```rust
// Kafka: any librdkafka property — SASL, TLS, fetch tuning.
KafkaSourceConfig::new(brokers, "harvest-orders", "orders.placed")
    .property("security.protocol", "SASL_SSL")
    .property("sasl.mechanisms", "PLAIN")
    .property("sasl.username", &user)
    .property("sasl.password", &pass);

// SQS: build the client yourself for explicit credentials, an assumed role,
// or a LocalStack/ElasticMQ endpoint. `SqsSource::connect` uses the ambient
// AWS config chain instead.
SqsSource::new(my_sqs_client, SqsSourceConfig::new(queue_url));

// `new` is sync, so it cannot probe the queue. If the queue has a redrive
// policy and you want `.broker_native_dead_letter()`, say so:
SqsSource::new(
    my_sqs_client,
    SqsSourceConfig::new(queue_url).has_redrive_policy(true),
);
```

Note the small asymmetry: `KafkaSource::connect(&config)` is **sync** and takes
a reference (librdkafka connects lazily); `SqsSource::connect(config)` is
**async** and takes the config by value — and uses that async-ness to probe the
queue's `RedrivePolicy`, so broker-native dead-lettering is validated against
the queue's real configuration rather than assumed.

Polling knobs live on `ConnectorRuntimeConfig`, passed via
`.connector_with_config(binding, source, Some(config))`. The defaults are tuned
for a cheap idle binding — a 1s poll (so SQS **long**-polls rather than
short-polls, which would bill an API call per round trip), a 200ms idle
backoff, and a consumer-lag sample at most every 15s. Lower `poll_timeout` for
faster pickup at the cost of more idle API calls; raise `max_batch` for a
high-throughput topic. A `max_batch` of `0` is floored to `1` at the runtime's
own `receive` call — a source asked for zero messages returns an empty batch,
which the runtime would read as an idle poll, so the binding would consume
nothing forever with no error to alert on.

## 2. SQS → an entity workflow (start-or-signal)

Whenever messages carry an entity key, prefer a `signals_with_start` binding.
The first message for a key starts the run; every later message is delivered to
the *same* run as a signal ([issue #244's atomic start-or-attach](06-idempotency.md)):

```rust
let binding = SourceBinding::signals_with_start(
        "telemetry", "device-telemetry", "device_session", "reading",
    )
    .map_json(|_ctx, r: Reading| {
        let payload = serde_json::to_value(&r).map_err(|e| e.to_string())?;
        Ok::<_, String>(MappedMessage::new(format!("device-{}", r.device_id), payload))
    })
    .broker_native_dead_letter()   // let SQS redrive own poison messages
    .max_in_flight(16);

let source = Arc::new(
    SqsSource::connect(
        SqsSourceConfig::new(QUEUE_URL)
            .stream("device-telemetry")
            .visibility_timeout_secs(60),
    )
    .await
    .expect("sqs client should build"),
);
```

Runnable: [`examples/sqs_connector_quickstart.rs`](../../autumn-harvest-plugin/examples/sqs_connector_quickstart.rs).

Size `visibility_timeout_secs` above your worst-case dispatch latency, or SQS
will redeliver a message that is still in flight. (Redelivery is *safe* — see
idempotency below — but it wastes work.)

## The ack-ordering contract

**A message is acknowledged only after harvest durably owns it.** Concretely:

1. The mapping function runs.
2. The dispatch commits — a `WorkflowStarted` event and execution row, or a
   staged signal — or harvest recognises the message as an already-committed
   replay, or the start throttle defers it (`202`).
3. *Only then* is the message acked: a Kafka offset commit, an SQS
   `DeleteMessage`.

Anything else leaves the message unacked, so the broker redelivers it. Kill the
process between step 2 and step 3 and you get a redelivery, not a lost message —
and because the dedupe key is derived from the message's own broker coordinates,
that redelivery resolves to the *same* execution rather than a second one.
That is the at-least-once half of the contract; step 2's idempotency is what
makes it safe.

For Kafka there is one extra rule. A Kafka offset commit is a **high-water
mark**: committing offset `N` asserts everything below `N` is done. Because the
runtime dispatches concurrently, offsets can finish out of order. The connector
therefore commits only the **contiguous completed prefix** — if offsets 11 and
12 finish while 10 is still in flight, nothing is committed until 10 lands, at
which point all three commit at once. A crash in that window redelivers 10, 11
and 12; all three dedupe.

A redelivery of an offset *at or below* the current mark is already durably
settled (a rebalance replays it), so it is re-acked — a commit is idempotent —
rather than silently withheld.

A partition can also be handed back **behind** the mark: an operator resets the
group offset, or a rebalance returns a partition another consumer moved. A
*fresh* delivery below the mark is treated as exactly that — the previous
generation's mark is discarded and the prefix rebuilt from the new position —
so one low offset's completion can never commit a stale higher mark and skip
everything in between.

### When the prefix cannot advance

The contiguous-prefix rule has a corollary worth stating plainly: a message
that is **retried rather than settled** blocks its partition's commit while
every later message settles behind it.

On SQS that resolves itself — the visibility timeout lapses and the message
comes back. On Kafka it does **not**: `abandon` is a no-op there because not
committing is not a nack, so nothing hands the message back until the consumer
is recreated and re-reads from the last commit. The stall is silent, because
the connector otherwise looks healthy: messages keep flowing, they all dispatch,
only the commit stops moving.

The connector detects this **and fixes it in process**, on either of two
signals:

- **A retried head**, reported immediately. When the runtime retries a message
  on a source that declares `abandon_redelivers() == false` (Kafka), that
  offset is a wedge *by construction* — nothing will hand it back — so there is
  nothing to wait for. This is the only signal that catches a retry at the tail
  of a **quiet** partition, where no later message ever settles behind it.
- **A backlog** of at least `ConnectorRuntimeConfig::stall_threshold` completed
  offsets behind an unsettled head, as a backstop for a head blocked without
  going through the retry path (a dispatch task lost to a panic).

Either way the pass fails with a distinct `ConnectorError::Stalled`
carrying the partition, depth and bound. `run` treats that as its own case
rather than a transient error — re-polling a wedged consumer accomplishes
nothing — and instead calls `EventSource::recover()`, which rebuilds the
consumer. A fresh consumer rejoins the group from the **last committed offset**,
so the blocked message is genuinely redelivered; the runtime then clears that
partition's tracker state, without which the redelivered offsets would arrive
below the stale in-memory mark and the prefix would stay blocked. Poison
strikes are deliberately *not* cleared, so a repeatedly-rejected message still
reaches its threshold and dead-letters rather than restarting its count on
every recovery. The check runs both before each receive — so a wedged binding
stops pulling batches it would only drop — and again after settling the batch,
so a stall that forms *during* a pass is acted on in that same pass.

`recover()` defaults to "I cannot rebuild myself" (`Ok(false)`) — correct for
SQS, whose visibility timeout already redelivers an abandoned message, so its
prefix cannot wedge this way. Kafka implements it. If a source that *cannot*
rebuild itself does stall, there is no in-process recovery available, so the
binding logs an error and **stops** rather than spinning forever pretending to
retry; restart the process, or supply a source that implements `recover`.

The check is **on by default**, since a stall nobody configured a detector for
is precisely the one that goes unnoticed. The default bound is derived from the
binding's `max_in_flight` (×4, floored at 32) because that is what bounds
*healthy* out-of-order settlement: only `max_in_flight` messages are ever
outstanding, so a held depth well past it means the head is not settling at all
rather than settling late. Set it explicitly to tune it, or to `Some(0)` to opt
out of the backlog heuristic. `Some(0)` does **not** disable the retried-head
signal: that one is a correctness guarantee, not a tunable, and suppressing it
on a positional broker would silently drop every retried message.

## Idempotency

The dedupe key is derived from stable broker coordinates, namespaced by the
binding, and passed to harvest's own `idempotency_key` machinery
([chapter 6](06-idempotency.md)):

| Broker | Coordinate |
|---|---|
| Kafka | `topic:partition:offset` |
| SQS (FIFO and standard) | `MessageId` — stable across every redelivery of the same message, and distinct for every distinct message |

Namespacing by binding means two bindings consuming the same topic never alias
each other. The key is bounded and injectively encoded, so a pathological
coordinate cannot collide with another message's key or blow the column limit.

Derived keys carry a **reserved `conn:` prefix**, and that reservation is
enforced rather than conventional: every caller-facing route that writes into a
scope a derived key reaches — the plain start route (header and body), the
in-process transactional-start client, signal-with-start, and the standalone
signal route — rejects a caller-supplied key beginning with `conn:` with a
`400`. That matters because derived keys are *predictable*: anyone who knows a
topic name can enumerate `topic:partition:offset`. Without the reservation a
caller could claim the key first, and the broker's own delivery of that message
would then read as an idempotent replay and be acknowledged **without ever
dispatching its payload**. The check is case-sensitive, matching the Postgres
text comparison behind the uniqueness scope — `CONN:` is a different key that
cannot alias a derived one, so it is still accepted.

A coordinate identifies a **message**, not a logical event. A genuine
*re-publish* of the same event is a different message, so it dispatches again.
On a FIFO queue it is tempting to reach for the producer-controlled
`MessageDeduplicationId` instead — harvest deliberately does not, because it is
wrong in both directions. Inside SQS's five-minute deduplication interval SQS
already collapses the re-publish itself, so the second message never reaches a
consumer and the dedup id buys nothing. Outside that interval a legitimately
reused dedup id (a nightly job keyed on a business id, say) is a genuinely new
message; keying on the dedup id would ack it as a replay and **silently drop a
valid event**. When you need event-level rather than message-level identity, put
the business key in the mapping function's `workflow_id` and use workflow-id
dedupe.

There is one interaction worth knowing. Harvest's start route rejects an
`idempotency_key` combined with a throttle / debounce / batch admission policy
(they defer the start, so there is no execution id to return). When your target
workflow has one of those policies, the connector automatically falls back to
**workflow-id** dedupe: the mapping function's `workflow_id` becomes the dedupe
unit. Override with `.idempotency_mode(...)` if you want to force one or the
other — forcing `BrokerCoordinates` onto a deferred-admission workflow is
rejected at build time rather than at the first message.

That fallback is worth being precise about, because workflow-id reuse only
arbitrates when an execution is **created**, and a deferred admission mutates a
pending record long before that:

| Target policy | `starts` binding | What a redelivery costs |
|---|---|---|
| **throttle** (#607) | allowed | Nothing. Each pending row fires through `start_or_load_workflow_execution`, where id reuse collapses the duplicate onto the original run and refunds its token. Exactly one run. |
| **debounce** (#499) | allowed | A bounded delay. The upsert collapses on `(workflow_name, debounce_key)`, so the redelivery lands on the *same* pending row — still exactly one run — but it resets the trailing-edge deadline (capped by `max_wait`) and increments `pending_count`, which therefore counts admissions rather than distinct messages. |
| **batch** (#518) | **rejected at build time** | A visible duplicate, which is why it is refused. Batch admission appends to `buffered_payloads`, so a redelivery would put the same message in the collapsed run's input twice — and it counts toward `max_size`, so it can flush the batch early. |

To consume a broker into a batched workflow, bind to an unbatched workflow and
have *it* start the batched one: the connector then dedupes the broker message
on coordinates as usual, and the batch only ever sees one admission per message.

`signals_with_start` bindings always use broker coordinates: the signal path's
key is a body field with no such mutual exclusion.

### The dedupe guarantee has a lifetime — and the knob differs by target

Coordinate dedupe is *"one execution per message for as long as the claim
survives"*, not unconditionally forever. The two targets persist that claim in
**different tables with different lifetimes**, so there is no single knob:

| Binding | Where the claim lives | What purges it | Default bound |
|---|---|---|---|
| `starts` | `harvest_start_idempotency` (#808) | `start_idempotency_window` | **24 hours** |
| `signals_with_start` | `harvest_signals.idempotency_key` | execution retention — the row is `ON DELETE CASCADE` on its execution | **unbounded** (retention is off by default) |

That matters because brokers can replay far older than a day:

* **Kafka** — a consumer-group offset reset or a deliberate topic replay can
  re-deliver an offset from anywhere inside the topic's retention.
* **SQS** — message retention runs up to 14 days, and a message that keeps
  failing can be redelivered across many receives, well past 24 hours.

A redelivery that lands *after* the claim is gone reserves the key as fresh, so
if the first run has already reached a terminal state you get a **second
execution** (and, for `signals_with_start`, the signal delivered again).

**For a `starts` binding**, set the window to at least how far back your broker
can replay:

```rust
HarvestPlugin::new()
    // Cover the topic's full retention, so a replay can never look fresh.
    .start_idempotency_window(std::time::Duration::from_secs(7 * 24 * 60 * 60))
```

Harvest logs a warning at startup for any coordinate-dedupe `starts` binding
when the window is left at its default, naming the binding. Setting the window
explicitly — to any value — silences it, on the assumption that an explicit
value is a considered one.

The cost of a longer window is retained rows in `harvest_start_idempotency`
(one small row per keyed start until it is purged), so size it to the replay
lifetime you actually need rather than the largest number you can think of.

**For a `signals_with_start` binding**, `start_idempotency_window` does nothing
— that path never reserves a start-idempotency claim. Its dedupe lasts exactly
as long as the target execution row does. With retention off (the default) that
is forever, which is *stronger* than the `starts` case. Once you turn retention
on, the claim dies with the run:

```rust
HarvestPlugin::new()
    // Deleting a cart's history at 3 days also deletes the dedupe claims for
    // every broker message that fed it -- a replay older than that re-delivers.
    .retention(RetentionConfig::default().with_max_age(Duration::from_secs(3 * 24 * 60 * 60)))
```

Harvest warns at startup for every coordinate-dedupe `signals_with_start`
binding whose target workflow has active retention, reporting the effective
age. That warning is **not** silenceable by tuning the window, precisely
because the window is the wrong remedy here — size retention (globally or via a
per-workflow-type override) to at least your broker's replay horizon, or accept
the bound knowingly.

### The dedupe guarantee is shard-local

Coordinate dedupe holds **unconditionally on a single-shard deployment** — the
default, and what most deployments are. On a [multi-shard](../sharding.md)
deployment it narrows to *"holds while the mapping function's `workflow_id` is
deterministic"*, for a mechanical reason worth understanding:

* The connector always dispatches with the `workflow_id` your mapper returned.
* [Issue #808](06-idempotency.md) routes a keyed start by the **idempotency
  key** only when `workflow_id` was *omitted*; with an explicit id it routes by
  the **id**, so the id picks the shard.
* `harvest_start_idempotency` claims are **per-shard**.

So if a redelivery of the same message maps to a *different* `workflow_id` —
because the mapping function is non-deterministic, or because its id derivation
changed between deployments while the broker still holds the old message — the
redelivery routes to a different shard, cannot see the claim the first delivery
wrote, and starts a **second execution**. On a single shard there is nowhere
else to route, so the claim is always found and the promise holds regardless of
what the mapper did.

This is the same shard-local scope every sibling dedupe primitive in the engine
carries — start idempotency (#808), signal idempotency (#521), per-key
concurrency (#247), the start throttle (#607), the durable mutex (#691). Cross-
shard coordination is out of scope engine-wide, so the connector does not
attempt it either: probing every shard on the dispatch hot path would fan a
query out per message, and unilaterally pinning the start to a key-derived
shard would trip the ["be consistent — always pin or never pin"](../sharding.md)
hazard against every *other* producer that starts the same `workflow_id`.

The remedy is the one the `WorkflowId` mode already documents, and it is cheap:
**make the mapping function's `workflow_id` a deterministic function of the
message**. Derive it from a business key in the payload or from the broker
coordinate itself — never from a clock, a counter, or a random value:

```rust
// Deterministic: the same message always maps to the same id, on any replica,
// in any deployment, across a redeploy.
fn map_order(ctx: &MessageCtx, body: Order) -> Result<MappedMessage, MappingError> {
    Ok(MappedMessage {
        workflow_id: WorkflowId::new(format!("order-{}", body.order_id)),
        payload: serde_json::to_value(body).map_err(|e| MappingError::Deserialize(e.to_string()))?,
    })
}
```

Harvest logs a warning at startup for every coordinate-dedupe binding when it
detects more than one readable shard, naming the binding and the shard count,
so you cannot land in this combination without being told. It is a warning
rather than a refusal because a deterministic mapper is both the common case
and perfectly safe here.

### Cutting a binding over to a new cluster or a recreated topic

Broker coordinates are unique only within one **incarnation** of the stream
they address. Kafka's `{topic}:{partition}:{offset}` restarts at zero when a
topic is deleted and recreated, and means nothing at all across a cutover to a
different cluster. Point an existing binding at either and the old coordinates
come back around while their claims are still live, so genuinely new records
are classified as replays and **acknowledged without being dispatched** —
silent loss, for as long as the claim lifetime above.

Nothing detects this for you, and nothing can: Kafka exposes no topic
incarnation to a consumer, and a bootstrap broker list is not a stable cluster
identity. Rotate the namespace yourself as part of the cutover:

```rust
SourceBinding::starts("orders", "orders", "order_flow")
    .map_json(|order: OrderPlaced| Ok(WorkflowId::new(order.order_id)))
    // Bump on any cutover: new cluster, or a deleted-and-recreated topic.
    .key_incarnation("2026-08-cutover")
```

Any value that changes when the stream underneath does will do — a date, a
cluster name, a ticket id. Renaming the binding works too (the name is already
the key namespace), but the name is also the `source` metric label, so that
remedy breaks every dashboard and alert built on the binding at the same time.

Rotating deliberately invalidates the binding's live claims, so anything the
broker still holds unacknowledged is dispatched again. On a genuine cutover
that is exactly right — those records are new — but it makes this a cutover
knob rather than something to churn. **A cutover is not the only trigger**:
recreating a topic in staging to reset it has the same effect, within the
claim lifetime.

## Poison messages

A message that can never succeed must not wedge its partition. Two classes:

* **Deterministic** — the body does not decode, or the start route rejects it
  (`4xx`: schema validation, an unregistered workflow). Dead-lettered
  immediately; retrying is pointless.
* **Strike-counted** — the mapping function *rejects* the message (it returned
  `Err`). Retried until `poison_threshold` consecutive rejections (default `3`,
  mirroring `poison_pill_threshold` from issue #367), then dead-lettered. A
  transient rejection — a lookup table that has not loaded yet — gets a few
  chances; a permanently unmappable message does not spin forever.

Setting `poison_threshold(0)` disables the strike counter (retry a rejection
forever) but **not** deterministic dead-lettering, because retrying an
undecodable body forever is exactly the wedge this exists to prevent.

### The strike counter is in-process and bounded

Strikes have to survive *between* deliveries to be counted at all, and they are
held in memory, capped at 10 000 entries per binding. Two consequences worth
knowing before you rely on a threshold above 1:

* **A restart resets them.** At worst a message in flight gets `threshold` more
  redeliveries before it is dead-lettered.
* **An active poison working set larger than the cap never accumulates.** With
  more than 10 000 distinct messages being rejected in round-robin order, each
  one is evicted before its next delivery, so every attempt is strike 1 and
  nothing ever reaches a threshold above 1 — the harvest sink never fires. This
  is inherent to a bounded in-process counter, so it is *reported* rather than
  hidden: the first eviction that discards a live strike count logs a warning.

If you expect a poison working set anywhere near that size, use one of:

* `poison_threshold(1)` — dead-letter on the first rejection. Nothing needs to
  survive between deliveries, so the cap stops mattering entirely. This is the
  right setting whenever your mapping rejections are deterministic (a schema
  mismatch does not become mappable on the third try).
* **An SQS redrive policy** — `ApproximateReceiveCount` is per-message, lives in
  the broker, and survives both eviction and restarts. It is the durable
  backstop at that scale.

Kafka cannot reach this shape: a retried message blocks its partition prefix, so
the stall detector fires long before a working set that large builds up.

Where it goes depends on the binding:

* Default (`DeadLetterMode::HarvestSink`) — a row in
  `harvest_connector_dead_letters` (a **plugin**-owned table) carrying the
  binding, stream, rendered coordinates, dedupe key, reason, detail, attempt
  count and the **raw payload**, so an operator can replay it by hand after a
  fix. The message is then acked. The `idempotency_key` column is `UNIQUE`, so
  dead-lettering is itself idempotent.
* `.broker_native_dead_letter()` — hand the message back to the broker's own
  machinery instead. For SQS the visibility timeout is reset to `0`, so SQS
  re-delivers, counts the receive, and moves the message to the queue's
  configured DLQ once `maxReceiveCount` is hit. This mode is **rejected at
  build time** when the adapter reports no dead-letter destination:
  * **Kafka**, always — its "abandon" is simply not committing, which never
    advances any counter.
  * **SQS**, when the queue has no redrive policy — there is nowhere to move
    the message to, so it would be redelivered forever. `SqsSource::connect`
    probes `RedrivePolicy` to find out; `SqsSource::new` cannot (it is sync),
    so it reports *no* destination unless you declare one with
    `SqsSourceConfig::has_redrive_policy(true)`. A probe that fails — usually
    an IAM policy without `sqs:GetQueueAttributes` — is logged and also fails
    closed, so the same declaration is the escape hatch.

  Fail-closed is deliberate: the alternative is a build that succeeds and then
  loops a poison message forever at runtime.

A transient harvest failure (a `5xx`, a pool exhaustion) is **never** written to
`harvest_connector_dead_letters` no matter how many times it recurs — it is not
the message's fault. It stays unacked and is redelivered, with whatever backoff
the broker provides: an SQS visibility timeout lapsing (size it with
`SqsSourceConfig::visibility_timeout_secs`), or a Kafka offset simply not being
committed. The connector deliberately does **not** rush a transient retry back;
that is reserved for the poison path, where fast redelivery is the point.

> **SQS + redrive caveat.** Every redelivery increments
> `ApproximateReceiveCount`, including one caused by a transient harvest
> failure. So on a queue with a redrive policy, a failure that persists past
> `maxReceiveCount` redeliveries *will* eventually reach the queue's DLQ — SQS
> cannot distinguish "harvest was down" from "this message is bad". Size
> `maxReceiveCount` above your expected outage length, or use the default
> harvest-sink mode, where a transient failure is genuinely never
> dead-lettered.

## Backpressure

`max_in_flight` (default `16`) bounds concurrently-dispatched messages per
binding. It composes with the start throttle (issue #607 — see
`autumn-harvest/examples/throttle_fanout.rs`): a throttled start returns `202` with no
execution id, and the connector treats that as a **successful** dispatch and
acks. Busy-retrying a deferred start would defeat the throttle and stampede the
admission path; the throttle already owns the pacing and will fire the start
when a token frees up.

## Ordering caveat

**The connector does not preserve broker partition ordering.** It dispatches up
to `max_in_flight` messages concurrently, so two messages from the same
partition can commit out of order. This is deliberate — serialising every
message would cap throughput at one dispatch per round trip.

If you need per-key **affinity**, use the entity pattern. A `signals_with_start`
binding whose mapping function derives a stable `workflow_id` from the
partition key routes every message for that key into the **same execution**,
while distinct entities still run concurrently.

Affinity is not ordering, and the distinction matters: the entity pattern gives
you *"all of one key's messages land in one run"*, **not** *"in broker order"*.
See the caveat below for why, and for the one knob that does buy you order.

The key is on the mapping context:

```rust
.map_json(|ctx, event: Reading| {
    // Kafka's record key — the thing the broker partitioned by, so all
    // messages for one key are already in one partition, in order.
    let entity = ctx.key_str().unwrap_or("unkeyed");
    Ok::<_, String>(MappedMessage::new(
        format!("device-{entity}"),
        serde_json::to_value(&event).map_err(|e| e.to_string())?,
    ))
})
```

`ctx.key_str()` is `Some` only where the broker has a partition-key concept and
the key is UTF-8 (`ctx.key` is the raw bytes). **SQS has no record key**, so
derive the entity id from a body field or a message attribute
(`ctx.header("...")`) there instead.

**Why affinity is not ordering, stated plainly.** The entity workflow
serializes the signals it receives, but the *order it receives them in* is the
order the connector dispatched them — and the connector spawns up to
`max_in_flight` dispatches concurrently, **including two messages for the same
key in the same batch**. Two same-key records can therefore race for a
connection, and the later offset can persist its signal first. The workflow
then replays them in database-recorded order, which is not broker order. The
run is still exactly one, and every message still lands in it; only the
sequence is unguaranteed.

The one knob that does buy you order is `.max_in_flight(1)` on that binding: a
permit is acquired *before* the next message is dispatched, so a single permit
makes dispatch strictly sequential in batch order — which, within one
partition, is broker order. You trade throughput for it, and it applies to the
whole binding rather than per key. (Locked in by
`max_in_flight_one_dispatches_in_broker_order`, which fails if the bound is
raised.)

Global total ordering across a whole topic is out of scope.

## Failure modes at a glance

| What happened | Acked? | Outcome |
|---|---|---|
| Start committed | yes | Execution created. `dispatched` |
| Redelivery of an already-committed message | yes | Same execution returned; no second run. `idempotent_replay` |
| Consumer-group rebalance replays uncommitted offsets | yes, on re-settle | Same as redelivery: dedupe collapses them |
| Crash between commit and ack | no → redelivered | Dedupe collapses the replay onto the original run |
| Start deferred by a throttle (`202`) | yes | The throttle fires it later. `deferred` |
| Body does not decode | yes | Dead-lettered `malformed` |
| Mapping function *panics* | yes | Contained and dead-lettered `malformed` (a panic is deterministic in the payload) |
| Start route rejects it deterministically (schema validation, a mutually-exclusive option) | yes | Dead-lettered `target_rejected` |
| Start route returns a *transient* `4xx` (boot window, target paused, `429`) | no → redelivered | Never dead-lettered; it clears on its own |
| Mapping function rejects it, under threshold | no → redelivered | `retried` |
| Mapping function rejects it, at threshold | yes | Dead-lettered `mapping_rejected` |
| Transient harvest failure (`5xx`, pool exhausted) | no → redelivered | Never dead-lettered |
| Dead-letter write itself fails | no → redelivered | Downgraded to a retry; the strike count is preserved so the next attempt still dead-letters |
| Broker `receive` errors | n/a | Back off `error_backoff`, poll again |

## Observability

Four metrics, all with bounded labels. Per [ADR-0001 §7](../adr/0001-otel-trace-contract.md)
the message key, offset and execution id are **never** labels:

| Metric | Type | Labels |
|---|---|---|
| `harvest.connector.received` | counter | `source` (binding name) |
| `harvest.connector.dispatched` | counter | `source`, `outcome` ∈ `dispatched` / `idempotent_replay` / `deferred` / `dead_lettered` / `retried` |
| `harvest.connector.poisoned` | counter | `source`, `reason` ∈ `malformed` / `mapping_rejected` / `target_rejected` |
| `harvest.connector.lag` | gauge | `source` — where the broker client exposes it. Sampled at most once per `lag_sample_interval` (default 15s), since it is a billed broker round-trip. See below for exactly what each adapter reports. |

Both adapters report **work this connector still owes**, which is the quantity
a restart would have to redo — not the cheaper number each broker offers first:

| Adapter | Reports | Why not the obvious one |
|---|---|---|
| Kafka | high-watermark minus the durable **committed group offset**, summed across **every partition of the topic** | The consumer's *read position* advances the moment a record is fetched, regardless of whether it was dispatched, is being retried, or is stuck behind a blocked commit prefix. Reading against it makes a wedged consumer report its lag falling to **zero** while a restart would replay the whole uncommitted span — inverting the gauge in exactly the case it exists to expose. A partition with nothing committed yet is baselined at the first offset **this replica actually read** from it, which is where the group's `auto.offset.reset` resolved. If it has never read from the partition (one owned by another replica), the policy itself is the baseline: the `earliest` default makes it the low watermark, and a caller's `.property("auto.offset.reset", "latest")` makes it the high watermark, so a brand-new latest-starting group owes nothing rather than reporting the whole retained log as a backlog that never drains on a quiet topic. Reading the *live* high watermark for a `latest` group that has already started would make every sample `high - high = 0`: a group stuck at offset 100 while producers push the topic to 1000 would report an all-clear for the 900 records it owes, hiding exactly the pre-first-commit outage the gauge exists to expose. (Unlike `enable.auto.commit`, the reset policy is *not* forced — see "A `latest` group is anchored, not forbidden" below.) A committed offset that retention has since advanced *past* is clamped up to the low watermark for the same reason: those records are gone from the log, so subtracting from the stale commit would report a backlog the consumer can never read. |
| SQS | `ApproximateNumberOfMessages` **plus** `ApproximateNumberOfMessagesNotVisible` | A message abandoned for visibility-timeout retry becomes *not visible*, so the visible count alone drains toward zero during a downstream outage while the outstanding population is unchanged. Both attributes ride the single `GetQueueAttributes` call, so this costs no extra round-trip. `ApproximateNumberOfMessagesDelayed` is excluded: a `DelaySeconds` message has not been delivered to anyone and is not work this connector currently owes. |

> Because the gauge counts uncommitted and in-flight work, it stays **high**
> while a partition is wedged or a backlog is being retried. That is the point:
> pair it with an alert on a lag that is not falling, rather than one that only
> fires on a large visible queue.

Both adapters report a **group-wide** number, so every replica of a binding
emits the same value and the starter dashboard's `max by (source)` deduplicates
replicas without under-reporting. That is free for SQS — `GetQueueAttributes`
returns the whole queue's depth to whoever asks — but for Kafka it is a
deliberate choice: the sample enumerates the topic's partitions from metadata
rather than reading `assignment()`. An assignment-scoped sum would be a property
of one *replica*, so four replicas would each report about a quarter of the
backlog, `max` would surface the largest quarter, and a partition stalled on any
other replica would be invisible. The cost is that each replica asks the broker
about every partition, so calls per sample scale with replica count — bounded
work on the `lag_sample_interval`, never on the message path.

### The lag sample is budgeted so it cannot starve consumption

The sample runs **before** `receive`, so no message is ever in custody across a
billed broker round-trip. That placement is what makes an *unbounded* sample
lethal: it blocks the pass outright, and the connector stops consuming entirely
while it waits. Worse, the failure is self-reinforcing and strikes exactly when
it matters most — a degraded broker makes the round-trip slow *and* makes the
backlog grow, so the gauge you are paying for reports a number you cannot act on
because the connector is too busy measuring it to consume.

Two bounds close that, and both are needed:

* **The sample is budgeted at `lag_sample_interval`.** A sample that cannot
  finish within the cadence it is taken at is by definition too expensive for
  that cadence, and the operator's remedy is the knob they already have. An
  over-budget sample is abandoned and logged; the gauge simply does not update,
  the same honest failure mode the adapters already use when a partial answer is
  unavailable.
* **The interval is measured from when a sample *finishes*, not when it starts.**
  Measuring from the start is what turns an expensive sample into a starvation
  loop: one that outlasts the interval is due again the instant it returns, so
  the very next pass samples again. Measuring from completion guarantees a full
  interval of message polling between samples however expensive one turns out to
  be. In the worst case the connector spends half its wall clock sampling, which
  is bounded; in the normal case a healthy sample is milliseconds and neither
  bound ever engages.

The Kafka adapter carries its own overall deadline for the walk as well, clamping
each `fetch_watermarks` to whatever budget is left. That is not redundant with
the runtime's timeout: abandoning the outer future only drops the join handle,
and the blocking walk would otherwise run to completion regardless — so
abandoned walks would pile up one per interval and *increase* load on the broker
that is already struggling. If a sample is repeatedly abandoned you will see a
`connector lag sample exceeded its budget` warning; widen
`lag_sample_interval`, or reduce what the sample costs (its cost scales with
partition count).

### A `latest` group is anchored, not forbidden

`enable.auto.commit` is forced off no matter what a caller sets, because
auto-commit breaks the ack-after-commit contract unconditionally.
`auto.offset.reset` is **not** forced: starting a new binding at the tail of a
huge existing topic is a legitimate thing to want.

It does need care, though, because `latest` resolves against a **moving**
target. It applies only while a partition has no committed offset — and for a
group whose very first message keeps failing, nothing ever commits, so that
window has no bound. Two things would follow the tail rather than the group:

* **Recovery.** A stalled commit prefix is retried by rebuilding the consumer,
  which normally resumes from the last commit. With no commit, the rebuild
  re-resolves `latest` against the *current* tail — which the blocked record has
  already advanced past — silently skipping it while the runtime reports a
  successful retry.
* **The lag gauge.** Baselining at the live high watermark makes every sample
  `high - high = 0`, hiding the outage described in the table above.

Harvest closes both with one fact: **the first offset the consumer reads from a
partition is where `latest` resolved**, and that is fixed. It is recorded on
receive, raised by every commit, committed synchronously before any rebuild, and
used as the lag baseline. Committing it is not a loss — "everything below where
I started is not mine" is precisely what `latest` *means*; this only makes that
decision durable instead of re-deriving it against a tail that has since moved.
As a side effect, it also makes an in-flight async ack-commit durable rather
than racing the rebuild.

If the anchor commit fails, recovery fails with it and the runtime backs off and
retries rather than rebuilding into a possible skip.

**An anchor is a local fact, and a commit is a group-wide statement.** Nothing
evicts an anchor when a rebalance moves that partition to another replica, so a
long-lived consumer accumulates floors for partitions it no longer owns. Before
committing, Harvest therefore keeps only the anchors it may speak for: a
partition this consumer **still owns**, and a floor that **strictly raises** what
the group has already committed. Both are needed. Ownership alone would still let
a stale floor through, because the first-read-wins rule is right within one
continuous ownership span but stale across a revoke-and-return — a partition read
at 100, lost, advanced to 5000 elsewhere and then handed back still carries a
floor of 100. Writing that back rewinds the group and replays records whose
idempotency claims expired long ago, which lands as **duplicate executions**
rather than deduplicated retries. Mid-rebalance the assignment is briefly empty
and nothing is committed at all, which is the conservative answer: the rebuild
rejoins and resumes from the group.

The lag baseline deliberately does **not** apply that assignment filter. It is
read-only and per-replica, so a stale anchor can only skew one sample — whereas
dropping anchors during a rebalance would re-open the `latest` under-report and
show a false all-clear, which is the failure direction that actually hurts.

**A rebuild is in-process, so a crash never runs it.** Re-asserting the anchor
before a rebuild covers a wedged prefix, but a crash or `SIGKILL` skips that path
entirely: a replacement process has neither a committed offset nor the anchor
map, and re-resolves `latest` against a tail the in-flight record has already
advanced past. That is the same permanent skip, for a record the connector had
already *accepted* — so under `latest` the floor is committed **eagerly**, before
the batch is dispatched, through the same ownership filter. It costs one
synchronous commit per partition per process: once it lands, the partition is
retired and never revisited. `earliest` pays nothing for this, because its
baseline is the low watermark, which does not move backwards — a restart with no
committed offset re-reads the record rather than skipping it.

An eager commit that fails is logged and left queued for the next receive to
retry; the batch is still returned, because discarding it would lose records the
consumer's local position has already advanced past.

Two residuals, both deliberate. A partition this replica has **never read** —
one owned by another replica — has no anchor and falls back to the reset policy
for its lag baseline; no local fact establishes otherwise, and closing it would
need cross-replica state. For `latest` that is also the correct answer: records
that arrived before this source existed are genuinely not its work. The narrower
one is a partition read since the last successful eager commit, whose commit
failed and is awaiting retry — a crash inside that window still skips its
in-flight record. Neither applies to the default (`earliest`), which is anchored
at the low watermark by definition.

A broker-triggered execution also records `start_source = 'broker'` with
`start_source_ref` set to the rendered coordinates, so a run traces back to the
exact message that produced it. That holds for **both** binding kinds — a
`signals_with_start` binding threads the same coordinates through, so the query
below does not need to special-case it. (Only the *fresh start* records
provenance; a later signal attaching to an existing run leaves the original
run's `start_source_ref` alone, which is the honest answer for "what created
this execution".)

```sql
SELECT id, workflow_name, start_source_ref
FROM harvest_workflow_executions
WHERE start_source = 'broker';
```

Triage a dead-letter backlog with:

```sql
SELECT binding, reason, count(*), max(failed_at)
FROM harvest_connector_dead_letters
GROUP BY binding, reason ORDER BY 3 DESC;
```

### Replaying a dead letter

There is no management API route or CLI command for this table yet (unlike the
engine's own DLQ, which has `/dead-letters` and `harvest dlq …`) — replay is a
manual, three-step recipe. Read the raw payload out:

```sql
SELECT coordinates, reason, detail, convert_from(payload, 'UTF8') AS body
FROM harvest_connector_dead_letters
WHERE binding = 'orders' AND reason = 'mapping_rejected'
ORDER BY failed_at DESC LIMIT 20;
```

Fix the mapping function or the workflow, deploy, then re-submit the body
through the ordinary start route — the same thing the connector would have
done:

```bash
curl -X POST "$HARVEST/api/harvest/workflows/fulfil_order/start" \
  -H 'content-type: application/json' \
  -d '{"workflow_id":"order-A-1001","input":{...the body...}}'
```

Finally delete the row, so it does not show up in the next triage:

```sql
DELETE FROM harvest_connector_dead_letters WHERE id = '...';
```

Re-publishing to the topic instead also works, and is often easier — the
connector will dedupe it against the original coordinates only if the *same*
offset comes back, so a genuine re-publish starts a fresh run.

## Testing without a broker

The `connectors` feature alone ships `MockSource`, which implements
`EventSource` over an in-memory queue and records what was acked and abandoned.
Drive the real `ConnectorRuntime` against it and every guarantee above — ack
ordering, dedupe, poison isolation, backpressure — is under test with no Docker:

```rust
use autumn_harvest_plugin::connector::{
    ConnectorRuntime, IdempotencyMode, MockSource, RecordingDeadLetterSink, SourceBinding,
};

let source = Arc::new(MockSource::new("orders.placed"));
let sink = Arc::new(RecordingDeadLetterSink::new());
let runtime = ConnectorRuntime::new(
    Arc::new(binding),           // the SourceBinding you are testing
    Arc::clone(&source) as Arc<dyn autumn_harvest_plugin::connector::EventSource>,
    api_state,                   // a HarvestApiState with storage + runtime installed
    Arc::new(autumn_harvest::telemetry::NoOpMetrics),
    IdempotencyMode::BrokerCoordinates,
)
.with_dead_letter_sink(Arc::clone(&sink));

source.push_kafka(0, 41, br#"{"order_id":"A-1"}"#);
source.push_kafka(0, 41, br#"{"order_id":"A-1"}"#); // deliberate redelivery

let summary = runtime.run_once().await.expect("pass");
assert_eq!(summary.received, 2);
assert_eq!(summary.acked, 2, "both deliveries settle");
assert_eq!(summary.retried, 0);
// ...and the database has exactly one execution for order-A-1.
```

`PassSummary` reports `received` / `acked` / `retried` / `dead_lettered`;
the finer-grained split (`dispatched` vs `idempotent_replay` vs `deferred`)
is on the `harvest.connector.dispatched` counter's `outcome` label, so assert
it with a recording `MetricsRecorder`.

The `.with_dead_letter_sink(...)` call is **not** optional when you build a
runtime yourself. A binding dead-letters into harvest by default, and a
directly-constructed runtime starts with a sink that deliberately *fails*
rather than silently succeeding — otherwise a poison message would be
acknowledged with no record in any table or broker, which is the one outcome a
dead-letter path must never produce. Leave it out and the first poison message
logs a configuration error naming both remedies and stays on the broker for
redelivery; nothing is lost, but the binding will not make progress past it.

`HarvestPlugin` wires the Postgres sink for you, so this only affects runtimes
you construct directly (tests, embedding harness, a custom supervisor). Two
ways to satisfy it:

```rust
// (a) record into harvest — the default mode:
let runtime = ConnectorRuntime::new(/* ... */)
    .with_dead_letter_sink(Arc::new(
        autumn_harvest_plugin::connector::PostgresDeadLetterSink::new(pool.clone()),
    ));

// (b) or let the broker's own redrive policy own dead-lettering, in which
//     case no harvest sink is consulted at all:
let binding = SourceBinding::starts("orders", "orders.placed", "order_flow")
    .broker_native_dead_letter();
```

The `HarvestApiState` is the fiddly part; copy the `api_state(...)` helper from
[`connector_integration.rs`](../../autumn-harvest-plugin/tests/connector_integration.rs),
which builds one over a test Postgres in about a dozen lines. That file is also
the acceptance-criteria suite, including the soak test that pushes 10,000
messages with 5% forced redeliveries and 10 poison messages and asserts exactly
9,990 executions, zero duplicates and 10 dead letters.

## Out of scope

Deliberately not shipped: NATS / RabbitMQ / Pub-Sub / Kinesis adapters (only
`EventSource` is broker-specific, so they are follow-ups rather than rewrites);
outbound event *publishing* (the plugin's transactional outbox covers the
"write a row in my business transaction, start a workflow reliably" direction); Kafka exactly-once / transactional semantics;
ordering guarantees beyond the entity pattern above; and schema-registry
(Avro / Protobuf) decoding — the mapping function receives raw bytes, so a
registry client is yours to call.

---

[← Prev](12-webhooks.md) · [Index](README.md)
