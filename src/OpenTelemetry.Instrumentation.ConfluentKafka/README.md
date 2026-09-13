# Confluent.Kafka client instrumentation for OpenTelemetry

| Status | |
| ------ | --- |
| Stability | [Development](../../README.md#development) |
| Code Owners | [@g7ed6e](https://github.com/g7ed6e) |

[![NuGet version badge](https://img.shields.io/nuget/v/OpenTelemetry.Instrumentation.ConfluentKafka)](https://www.nuget.org/packages/OpenTelemetry.Instrumentation.ConfluentKafka)
[![NuGet download count badge](https://img.shields.io/nuget/dt/OpenTelemetry.Instrumentation.ConfluentKafka)](https://www.nuget.org/packages/OpenTelemetry.Instrumentation.ConfluentKafka)
[![codecov.io](https://codecov.io/gh/open-telemetry/opentelemetry-dotnet-contrib/branch/main/graphs/badge.svg?flag=unittests-Instrumentation.ConfluentKafka)](https://app.codecov.io/gh/open-telemetry/opentelemetry-dotnet-contrib?flags[0]=unittests-Instrumentation.ConfluentKafka)

## Usage

To use the `OpenTelemetry.Instrumentation.ConfluentKafka` package, follow these
steps:

1. **Install the package**:

    ```shell
    dotnet add package OpenTelemetry.Instrumentation.ConfluentKafka --prerelease
    ```

2. **Configure OpenTelemetry in your application**:

    ```csharp
    using Confluent.Kafka;
    using OpenTelemetry.Metrics;
    using OpenTelemetry.Trace;

    var builder = Host.CreateApplicationBuilder(args);

    const string bootstrapServers = "localhost:9092";

    builder.Services.AddSingleton(_ =>
    {
        ProducerConfig producerConfig = new() { BootstrapServers = bootstrapServers };
        return new InstrumentedProducerBuilder<string, string>(producerConfig);
    });
    builder.Services.AddSingleton(_ =>
    {
        ConsumerConfig consumerConfigA = new()
        {
            BootstrapServers = bootstrapServers,
            GroupId = "group-a",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnablePartitionEof = true,
        };
        return new InstrumentedConsumerBuilder<string, string>(consumerConfigA);
    });

    builder.Services.AddOpenTelemetry()
        .WithTracing(tracing =>
        {
            tracing.AddConsoleExporter()
                .AddOtlpExporter()
                // AddKafkaProducerInstrumentation and AddKafkaConsumerInstrumentation
                // are what enable Kafka traces.
                .AddKafkaProducerInstrumentation<string, string>()
                .AddKafkaConsumerInstrumentation<string, string>();
        })
        .WithMetrics(metering =>
        {
            metering.AddConsoleExporter()
                .AddOtlpExporter()
                // AddKafkaProducerInstrumentation and AddKafkaConsumerInstrumentation
                // are what enable Kafka metrics.
                .AddKafkaProducerInstrumentation<string, string>()
                .AddKafkaConsumerInstrumentation<string, string>();
        });

    builder.Services.AddHostedService<ProduceConsumeHostedService>();

    var app = builder.Build();
    await app.RunAsync();
    ```

This will set up OpenTelemetry instrumentation for Confluent.Kafka producers
and consumers, allowing you to collect and export telemetry data.

## Metrics

The instrumentation is implemented based on the [messaging metrics semantic
conventions](https://github.com/open-telemetry/semantic-conventions/blob/v1.44.0/docs/messaging/messaging-metrics.md).
The following metrics are produced:

| Name | Instrument Type | Unit | Description | Attributes |
| --- | --- | --- | --- | --- |
| `messaging.client.operation.duration` | Histogram | `s` | Duration of messaging operation initiated by a producer or consumer client. | `messaging.operation.name`, `messaging.operation.type`, `messaging.system`, `messaging.destination.name`[^1], `messaging.destination.partition.id`[^2], `messaging.consumer.group.name`[^3], `error.type`[^4] |
| `messaging.client.sent.messages` | Counter | `{message}` | Number of messages producer attempted to send to the broker. | `messaging.operation.name`, `messaging.operation.type`, `messaging.system`, `messaging.destination.name`, `messaging.destination.partition.id`[^2], `error.type`[^4] |
| `messaging.client.consumed.messages` | Counter | `{message}` | Number of messages that were delivered to the application. | `messaging.operation.name`, `messaging.operation.type`, `messaging.system`, `messaging.destination.name`[^1], `messaging.destination.partition.id`[^2], `messaging.consumer.group.name`[^3], `error.type`[^4] |

[^1]: `messaging.destination.name` is only included for consumer operations
  when the topic partition is known (for example, it is omitted after a
  `PartitionEOF` event that carries no topic partition).
[^2]: `messaging.destination.partition.id` is only included when the topic
  partition is known.
[^3]: `messaging.consumer.group.name` is only included for consumer
  operations, when a consumer group ID is configured.
[^4]: `error.type` is only included when an error occurs.

### Kafka client metrics

Metrics derived from librdkafka's statistics callback are disabled by default
because generating and parsing the statistics JSON has a runtime cost. Enable
them when converting a producer or consumer builder:

```csharp
var options = new ConfluentKafkaInstrumentedProducerBuilderOptions
{
    EnableClientMetrics = true,
    StatisticsInterval = TimeSpan.FromSeconds(10),
};

var instrumentedBuilder = producerBuilder.AsInstrumentedProducerBuilder(options);
```

`StatisticsInterval` defaults to 10 seconds. The instrumentation sets
`statistics.interval.ms` from this value only when that librdkafka property is
not already present in the client configuration. An explicitly configured
`statistics.interval.ms`, including `0`, always takes precedence.

The metric names and attributes follow the OpenTelemetry Java `kafka-clients`
instrumentation. Cumulative librdkafka values are exposed as observable
counters; rates should be calculated by the metrics backend.

| Name | Type | Unit | Attributes | librdkafka source |
| --- | --- | --- | --- | --- |
| `kafka.consumer.assigned_partitions` | Gauge | `{partition}` | `client-id` | `cgrp.assignment_size` |
| `kafka.consumer.rebalance_total` | Counter | `{rebalance}` | `client-id` | `cgrp.rebalance_cnt` |
| `kafka.consumer.last_rebalance_seconds_ago` | Gauge | `s` | `client-id` | `cgrp.rebalance_age` |
| `kafka.consumer.records_consumed_total` | Counter | `{record}` | `client-id`, `topic` | Sum of partition `rxmsgs` |
| `kafka.consumer.bytes_consumed_total` | Counter | `By` | `client-id`, `topic` | Sum of partition `rxbytes` |
| `kafka.consumer.records_lag` | Gauge | `{record}` | `client-id`, `topic`, `partition` | Partition `consumer_lag_stored` |
| `kafka.producer.record_send_total` | Counter | `{record}` | `client-id`, `topic` | Sum of partition `txmsgs` |
| `kafka.producer.byte_total` | Counter | `By` | `client-id`, `topic` | Sum of partition `txbytes` |
| `kafka.producer.record_retry_total` | Counter | `{record}` | `client-id` | Sum of broker `txretries` |
| `kafka.producer.requests_in_flight` | Gauge | `{request}` | `client-id` | Sum of broker `waitresp_cnt` |
| `kafka.producer.buffer_total_bytes` | Gauge | `By` | `client-id` | `msg_size_max` |
| `kafka.producer.buffer_available_bytes` | Gauge | `By` | `client-id` | `msg_size_max - msg_size` |
| `kafka.producer.record_queue_time_avg` | Gauge | `ms` | `client-id` | Broker `int_latency.avg` |
| `kafka.producer.produce_throttle_time_avg` | Gauge | `ms` | `client-id` | Broker `throttle.avg` |
| `kafka.producer.produce_throttle_time_max` | Gauge | `ms` | `client-id` | Maximum broker `throttle.max` |
| `kafka.consumer.request_latency_avg` | Gauge | `ms` | `client-id`, `node-id` | Broker `rtt.avg` |
| `kafka.consumer.request_latency_max` | Gauge | `ms` | `client-id`, `node-id` | Broker `rtt.max` |
| `kafka.consumer.request_total` | Counter | `{request}` | `client-id`, `node-id` | Broker `tx` |
| `kafka.consumer.response_total` | Counter | `{response}` | `client-id`, `node-id` | Broker `rx` |
| `kafka.consumer.incoming_byte_total` | Counter | `By` | `client-id`, `node-id` | Broker `rxbytes` |
| `kafka.consumer.outgoing_byte_total` | Counter | `By` | `client-id`, `node-id` | Broker `txbytes` |
| `kafka.consumer.connection_count` | Gauge | `{connection}` | `client-id` | Brokers in the `UP` state |
| `kafka.producer.request_latency_avg` | Gauge | `ms` | `client-id`, `node-id` | Broker `rtt.avg` |
| `kafka.producer.request_latency_max` | Gauge | `ms` | `client-id`, `node-id` | Broker `rtt.max` |
| `kafka.producer.request_total` | Counter | `{request}` | `client-id`, `node-id` | Broker `tx` |
| `kafka.producer.response_total` | Counter | `{response}` | `client-id`, `node-id` | Broker `rx` |
| `kafka.producer.incoming_byte_total` | Counter | `By` | `client-id`, `node-id` | Broker `rxbytes` |
| `kafka.producer.outgoing_byte_total` | Counter | `By` | `client-id`, `node-id` | Broker `txbytes` |
| `kafka.producer.connection_count` | Gauge | `{connection}` | `client-id` | Brokers in the `UP` state |

Set `EnablePartitionMetrics = true` to emit
`kafka.consumer.records_lag`; it is omitted otherwise to avoid partition-level
cardinality. Bootstrap brokers and lag sentinel values are not observed;
partitions not currently desired by a consumer are excluded from the lag gauge
but retained in cumulative consumed-record and byte totals. Latency values are
converted from microseconds to milliseconds where required.

There are two intentional differences from the Java client metrics:

- `kafka.consumer.rebalance_total` counts every partition assignment or
  revocation because librdkafka does not expose successful rebalances
  separately. `kafka.consumer.last_rebalance_seconds_ago` likewise measures
  from the latest assignment or revocation.
- `kafka.producer.record_retry_total` is client-level because librdkafka
  reports retries per broker rather than per topic.

Rate gauges, coordinator and I/O-thread latencies, authentication metrics, and
producer error, batch, and compression metrics are intentionally excluded
until a reliable librdkafka mapping is available.

## Runnable example

A complete end-to-end sample that produces and consumes messages with
instrumentation enabled is available in
[`examples/kafka`](../../examples/kafka). Follow that example's README to
start a local Kafka broker and see traces and metrics flowing to the
configured exporters.

## Extending `ConsumerBuilder` or `ProducerBuilder` instances

To extend an already built `ConsumerBuilder<TKey, TValue>`
or `ProducerBuilder<TKey, TValue>`
instance with OpenTelemetry instrumentation, you can use the
`AsInstrumentedConsumerBuilder`
and `AsInstrumentedProducerBuilder` extension methods.

> [!IMPORTANT]
> When you create dynamic producers or consumers outside a DI container,
> OpenTelemetry instrumentation (metrics and traces) is disabled by default.
> You must explicitly pass configuration options to enable it.
> If you do not use the standard DI registration methods (such as
`.AddKafkaProducerInstrumentation()`
> or `.AddKafkaConsumerInstrumentation()`), you must also manually call
`.AddSource("OpenTelemetry.Instrumentation.ConfluentKafka")` on your
> TracerProviderBuilder
> and `.AddMeter("OpenTelemetry.Instrumentation.ConfluentKafka")` on your
> MeterProviderBuilder so that the providers can listen to the emitted signals.

### Example for `ConsumerBuilder<TKey, TValue>`

```csharp
using Confluent.Kafka;
using OpenTelemetry.Instrumentation.ConfluentKafka;

var consumerConfig = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "my-group",
    AutoOffsetReset = AutoOffsetReset.Earliest
};

var consumerBuilder = new ConsumerBuilder<string, string>(consumerConfig);

// Set various handlers and properties
consumerBuilder.SetErrorHandler((consumer, error) => Console.WriteLine($"Error: {error.Reason}"));
consumerBuilder.SetLogHandler((consumer, logMessage) => Console.WriteLine($"Log: {logMessage.Message}"));
consumerBuilder.SetStatisticsHandler((consumer, statistics) => Console.WriteLine($"Statistics: {statistics}"));

// Explicitly enable OpenTelemetry features for standalone usage
var telemetryOptions = new ConfluentKafkaInstrumentedConsumerBuilderOptions
{
    EnableClientMetrics = true,
    EnableTraces = true,
    EnableMetrics = true,
};

// Convert to InstrumentedConsumerBuilder with options
var instrumentedConsumerBuilder = consumerBuilder.AsInstrumentedConsumerBuilder(telemetryOptions);

// Build the consumer
var consumer = instrumentedConsumerBuilder.Build();
```

### Example for `ProducerBuilder<TKey, TValue>`

```csharp
using Confluent.Kafka;
using OpenTelemetry.Instrumentation.ConfluentKafka;

var producerConfig = new ProducerConfig
{
    BootstrapServers = "localhost:9092"
};

var producerBuilder = new ProducerBuilder<string, string>(producerConfig);

// Set various handlers and properties
producerBuilder.SetErrorHandler((producer, error) => Console.WriteLine($"Error: {error.Reason}"));
producerBuilder.SetLogHandler((producer, logMessage) => Console.WriteLine($"Log: {logMessage.Message}"));
producerBuilder.SetStatisticsHandler((producer, statistics) => Console.WriteLine($"Statistics: {statistics}"));

// Explicitly enable OpenTelemetry features for standalone usage
var telemetryOptions = new ConfluentKafkaInstrumentedProducerBuilderOptions
{
    EnableClientMetrics = true,
    EnableTraces = true,
    EnableMetrics = true,
};

// Convert to InstrumentedProducerBuilder with options
var instrumentedProducerBuilder = producerBuilder.AsInstrumentedProducerBuilder(telemetryOptions);

// Build the producer
var producer = instrumentedProducerBuilder.Build();
```
