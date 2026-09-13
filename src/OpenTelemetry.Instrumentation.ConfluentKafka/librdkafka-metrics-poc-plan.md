# PoC plan: librdkafka client statistics as OpenTelemetry metrics

Tracking issue: [open-telemetry/opentelemetry-dotnet-contrib#5203](https://github.com/open-telemetry/opentelemetry-dotnet-contrib/issues/5203)
Package: `OpenTelemetry.Instrumentation.ConfluentKafka`

## Decisions

Agreed with maintainer feedback on the issue. All marked as open to change in the PR description.

| Decision | Choice | Rationale |
|---|---|---|
| Metric naming | Follow the Java `kafka-clients` instrumentation (`kafka.consumer.*`, `kafka.producer.*`, attributes `client-id`, `topic`, `partition`, `node-id`) | Messaging semconv has been stalled since 2024; Java is the de facto reference. Makes librdkafka a private implementation detail, so schema drift never causes a user-facing rename. |
| Scope | A core subset (~20 instruments), not Java's full 174 | Maintainer explicitly asked for a reasonable subset. |
| Opt-in | `EnableClientMetrics` defaults to `false` | Generating and parsing the stats JSON has a runtime cost. |
| Interval | `StatisticsInterval` (`TimeSpan`, default 10s). `Build()` sets `statistics.interval.ms` only when the user has not already configured it | Matches the API shape in the issue. One flag makes it work; user's explicit config always wins. Trivially reversible if review objects. Setter validates `> TimeSpan.Zero`. |
| Granularity | `EnablePartitionMetrics` defaults to `false`. Java's most-granular-wins rule: when partition metrics are on, topic-level series come from partition sums and the client-level series is not also emitted | Avoids double counting and keeps cardinality opt-in. |
| Units | Java units on the wire: ms for latencies, s for ages. Convert µs→ms for `rtt`, `int_latency`, `throttle`; ms→s for `rebalance_age` | Java names must carry Java units. |
| Rates | No `*_rate` gauges | librdkafka gives cumulative totals; backends derive rates. |
| Drift strategy | Name-based tolerant parsing, per-version JSON fixtures with a conformance test, librdkafka version logged once | See "Handling librdkafka schema drift" below. |

## Metric set

### Consumer

| Instrument | Type | Attributes | librdkafka source | Notes |
|---|---|---|---|---|
| `kafka.consumer.assigned_partitions` | gauge | client-id | `cgrp.assignment_size` | |
| `kafka.consumer.rebalance_total` | counter | client-id | `cgrp.rebalance_cnt` | **Deviation:** counts all rebalances; Java counts successful only |
| `kafka.consumer.last_rebalance_seconds_ago` | gauge | client-id | `cgrp.rebalance_age` | ms → s |
| `kafka.consumer.records_consumed_total` | counter | client-id, topic | partition `rxmsgs` summed per topic | |
| `kafka.consumer.bytes_consumed_total` | counter | client-id, topic | partition `rxbytes` summed per topic | |
| `kafka.consumer.records_lag` | gauge | client-id, topic, partition | partition `consumer_lag_stored` | Partition-gated. Matches Java's position-based lag. Drop `-1` |

### Producer

| Instrument | Type | Attributes | librdkafka source | Notes |
|---|---|---|---|---|
| `kafka.producer.record_send_total` | counter | client-id, topic | partition `txmsgs` summed per topic | |
| `kafka.producer.byte_total` | counter | client-id, topic | partition `txbytes` summed per topic | |
| `kafka.producer.record_retry_total` | counter | client-id | broker `txretries` summed | **Deviation:** Java reports per topic; librdkafka only has per broker |
| `kafka.producer.requests_in_flight` | gauge | client-id | broker `waitresp_cnt` summed | |
| `kafka.producer.buffer_total_bytes` | gauge | client-id | `msg_size_max` | Closest analogue to Java's buffer pool |
| `kafka.producer.buffer_available_bytes` | gauge | client-id | `msg_size_max - msg_size` | |
| `kafka.producer.record_queue_time_avg` | gauge | client-id | broker `int_latency.avg` | µs → ms |
| `kafka.producer.produce_throttle_time_avg` / `_max` | gauge | client-id | broker `throttle.avg` / `.max` | |

### Node level (both client types, prefix by `type`)

| Instrument | Type | Attributes | librdkafka source | Notes |
|---|---|---|---|---|
| `kafka.{consumer,producer}.request_latency_avg` / `_max` | gauge | client-id, node-id | broker `rtt.avg` / `.max` | µs → ms |
| `kafka.{consumer,producer}.request_total` | counter | client-id, node-id | broker `tx` | |
| `kafka.{consumer,producer}.response_total` | counter | client-id, node-id | broker `rx` | |
| `kafka.{consumer,producer}.incoming_byte_total` | counter | client-id, node-id | broker `rxbytes` | |
| `kafka.{consumer,producer}.outgoing_byte_total` | counter | client-id, node-id | broker `txbytes` | |
| `kafka.{consumer,producer}.connection_count` | gauge | client-id | count of brokers with `state == "UP"` | |

`node-id` comes from broker `nodeid`; bootstrap brokers report `-1` and are skipped.

### Intentionally excluded

- All `*_rate` gauges (no clean windowed equivalent; derive from totals).
- Coordinator latencies: `join_time_*`, `sync_time_*`, `commit_latency_*` (librdkafka has no per-request-type latency, only the `req` count map).
- I/O-thread metrics (`io_*`, `select_*`).
- Authentication metrics.
- Producer `record_error_total`, `batch_*`, `compression_*` (defer until there's an agreed mapping).

## Repository context

Observed in `src/OpenTelemetry.Instrumentation.ConfluentKafka` on `main`:

- `InstrumentedConsumerBuilder<K,V>` / `InstrumentedProducerBuilder<K,V>` own `Config` at `Build()` and already read the user's `StatisticsHandler` via reflection (`GetInternalStatisticsHandler` / `SetInternalStatisticsHandler`).
- Public options types `ConfluentKafkaInstrumentedConsumerBuilderOptions` / `...ProducerBuilderOptions` are unshipped (in `PublicAPI.Unshipped.txt`) and currently hold only `EnableMetrics` / `EnableTraces`. Extend these.
- Internal mirrors `ConfluentKafkaConsumerInstrumentationOptions<K,V>` / `...ProducerInstrumentationOptions<K,V>`.
- Shared `ConfluentKafkaCommon.Meter` (semconv version 1.44.0).
- `ConfluentKafkaInstrumentationEventSource` (`OpenTelemetry-Instrumentation-ConfluentKafka`).
- `InstrumentedConsumer.Dispose()` / `InstrumentedProducer.Dispose()` just forward to the wrapped client.
- Tests: `KafkaFixture` (Testcontainers, KRaft), `MeteringTests`, `HostedMeteringTests`, in-memory exporter helpers.
- Confluent.Kafka floor `[2.4.0,)`. TFMs include `net462`.

## Phases

### Phase 0 — spike (~half a day)

1. Run a producer and consumer against `KafkaFixture` with `statistics.interval.ms=1000`.
2. Capture raw stats JSON in three states: idle, under load, after a rebalance.
3. Save to `test/OpenTelemetry.Instrumentation.ConfluentKafka.Tests/Statistics/Fixtures/librdkafka-<version>.json`.
4. Confirm presence of every field in the metric set at the Confluent.Kafka floor version. Note `client_id`, `type`, `cgrp`, broker `nodeid`.

### Phase 1 — plumbing

1. **Options.** Add to both public builder-options types and their internal mirrors:
   - `bool EnableClientMetrics` (default `false`)
   - `TimeSpan StatisticsInterval` (default 10s, setter validates `> TimeSpan.Zero`)
   - `bool EnablePartitionMetrics` (default `false`)

   Update `PublicAPI.Unshipped.txt`.
2. **Config injection.** In `Build()`, when `EnableClientMetrics` is on and `statistics.interval.ms` is absent from `Config`, set it from `StatisticsInterval`. Never overwrite a user-supplied value.
3. **Composite handler.** In `Build()`, read the user's handler via `GetInternalStatisticsHandler`, install a handler that invokes the bridge then the user's callback, each in its own `try/catch`. Nothing may throw back into librdkafka.
4. **Registry.** `Statistics/KafkaClientMetrics.cs`: static `ConcurrentDictionary<string, KafkaStatisticsSnapshot>` keyed by `client_id`; observable instruments created once on `ConfluentKafkaCommon.Meter`; callbacks enumerate live snapshots. `Dispose()` on the instrumented consumer/producer removes the entry, which stops observation.
5. **EventSource.** Add `StatisticsParseFailed(exception)` (warning) and a one-off `StatisticsBridgeStarted(librdkafkaVersion)` (informational) using `Library.VersionString`.

### Phase 2 — parser and snapshot

1. `Statistics/KafkaStatisticsSnapshot.cs`: plain record with client-level totals, `cgrp`, `List<BrokerStats>`, `Dictionary<string, List<PartitionStats>>`. Shared by producer and consumer; the JSON `type` field decides which instruments observe it. All mapped values are nullable.
2. `Statistics/KafkaStatisticsParser.cs`: `JsonDocument` + `TryGetProperty` for mapped fields only.
   - Missing field → `null`, never zero.
   - Unknown fields ignored.
   - Filter sentinels: `-1` lag, `-1001` offsets.
   - Skip partitions with `desired == false` and brokers with `nodeid < 0`.
   - Unit conversions applied here so the snapshot is already in Java units.
3. Check whether `System.Text.Json` needs an explicit package reference for `net462` / `netstandard2.0` in this csproj.

### Phase 3 — instruments

Implement in this order, each as its own commit:

1. Producer client-level: `record_send_total`, `byte_total`, `record_retry_total`, `requests_in_flight`, `buffer_total_bytes`, `buffer_available_bytes`, `record_queue_time_avg`, `produce_throttle_time_avg/max`.
2. Consumer client-level: `assigned_partitions`, `rebalance_total`, `last_rebalance_seconds_ago`, `records_consumed_total`, `bytes_consumed_total`.
3. Partition-gated: `records_lag`. Apply the most-granular-wins rule for topic sums.
4. Node-level: `request_latency_avg/max`, `request_total`, `response_total`, `incoming/outgoing_byte_total`, `connection_count`.

Totals are `ObservableCounter<long>`; everything else is `ObservableGauge<double>`.

### Phase 4 — tests

Unit (no Kafka):

- Parser produces the expected snapshot for each fixture.
- Conformance test: every field the mapper reads exists in every fixture. This is the drift alarm when Confluent.Kafka is bumped.
- Malformed / truncated JSON does not throw and increments the parse-failure event.
- User's statistics handler is still invoked when the bridge throws.
- Sentinel and negative-nodeid filtering.
- `Dispose()` removes the client from observation.
- `Build()` sets `statistics.interval.ms` only when absent.

Integration (extend `MeteringTests` on `KafkaFixture`):

- `StatisticsInterval = 1s`, produce N, consume N, wait ~2 intervals, `Collect()`.
- Assert `kafka.producer.record_send_total == N` on `[client-id, topic]`.
- Assert `kafka.consumer.records_lag` present with partition metrics on and absent with them off.
- Assert existing `messaging.client.*` metrics are unchanged.

### Phase 5 — docs and PR

- README: metrics table with Java name / librdkafka source / notes, the intentional exclusions, and the interval precedence rule.
- CHANGELOG entry under Unreleased.
- PR description leads with: opt-in, Java naming, drift strategy, the two deviations (rebalance counts all attempts; retries at client level), and the decisions marked open to change (10s default interval, precedence rule, partition gating).

## Handling librdkafka schema drift

The stats JSON is a documented public interface that Confluent's own tooling depends on; historical changes have been additive. The package pins Confluent.Kafka, which pins `librdkafka.redist`, so a version bump is a deliberate PR where the tests run. Mitigations:

1. Parse by name with `TryGetProperty`; never deserialize the whole document into a typed model.
2. Missing field means "no observation this cycle."
3. Any parser exception is swallowed, logged via EventSource, and the previous snapshot retained.
4. Fixtures per librdkafka version plus the conformance test.
5. Log `Library.VersionString` once at bridge start.

## Sequencing

Phase 0 → Phase 1 → Phase 2 → Phase 3 step 1 → producer integration test → **open as draft PR**.

The draft carries only producer client-level metrics: small enough to review, large enough to settle architecture and naming. Phase 3 steps 2–4 land as follow-up commits on the same branch once a maintainer has confirmed the shape.

## Gotchas

- The consumer's statistics callback runs on the thread calling `Consume()`; the producer's on the background poll thread. Parsing must be cheap and must never block.
- JSON size scales with partitions × brokers; for the PoC `JsonDocument` is fine, but note `Utf8JsonReader` as the upgrade path if allocation shows up in benchmarks.
- Counter resets on client restart: observable counters report the raw cumulative value; a new client instance gets a new `client_id` entry, so the SDK sees it as a fresh series. Document rather than special-case.
- Do not export librdkafka's generated handle `name` as an attribute; use `client_id`.
