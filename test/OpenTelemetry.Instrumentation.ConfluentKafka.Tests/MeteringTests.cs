// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

using System.Diagnostics;
using Confluent.Kafka;
using OpenTelemetry.Metrics;
using OpenTelemetry.Tests;
using OpenTelemetry.Trace;

namespace OpenTelemetry.Instrumentation.ConfluentKafka.Tests;

[Collection(KafkaCollection.Name)]
[Trait("CategoryName", "KafkaIntegrationTests")]
public class MeteringTests(KafkaFixture fixture)
{
    private readonly string connectionString = fixture.TypedContainer.GetConnectionString();

    [EnabledOnDockerPlatformFact(DockerPlatform.Linux)]
    public async Task BasicProduceToTopicTest()
    {
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = this.connectionString,
        };
        InstrumentedProducerBuilder<string, string> producerBuilder = new(producerConfig);
        var metrics = new List<Metric>();
        var topic = $"otel-topic-{Guid.NewGuid()}";
        using (var meterProvider = Sdk.CreateMeterProviderBuilder()
                   .AddInMemoryExporter(metrics)
                   .AddKafkaProducerInstrumentation(producerBuilder)
                   .Build())
        {
            var producer = producerBuilder.Build();
            producer.Produce(topic, new Message<string, string>
            {
                Value = "any_value",
            });

            await producer.FlushAsync();

            meterProvider.EnsureMetricsAreFlushed();
        }

        var groups = metrics.GroupBy(m => m.Name).ToArray();

        Assert.Equal(2, groups.Length);
        Assert.Contains(SemanticConventions.MetricMessagingClientSentMessages, groups.Select(g => g.Key));
        Assert.Contains(SemanticConventions.MetricMessagingClientOperationDuration, groups.Select(g => g.Key));
        AssertOperation(metrics, SemanticConventions.MetricMessagingClientOperationDuration, expectedOperationName: "send", expectedOperationType: "send");
    }

    [EnabledOnDockerPlatformFact(DockerPlatform.Linux)]
    public async Task BasicProduceAsyncToTopicTest()
    {
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = this.connectionString,
        };
        InstrumentedProducerBuilder<string, string> producerBuilder = new(producerConfig);
        var metrics = new List<Metric>();
        var topic = $"otel-topic-{Guid.NewGuid()}";
        using (var meterProvider = Sdk.CreateMeterProviderBuilder()
                   .AddInMemoryExporter(metrics)
                   .AddKafkaProducerInstrumentation(producerBuilder)
                   .Build())
        {
            var producer = producerBuilder.Build();
            await producer.ProduceAsync(topic, new Message<string, string>
            {
                Value = "any_value",
            });

            await producer.FlushAsync();

            meterProvider.EnsureMetricsAreFlushed();
        }

        var groups = metrics.GroupBy(m => m.Name).ToArray();

        Assert.Equal(2, groups.Length);
        Assert.Contains(SemanticConventions.MetricMessagingClientSentMessages, groups.Select(g => g.Key));
        Assert.Contains(SemanticConventions.MetricMessagingClientOperationDuration, groups.Select(g => g.Key));
        AssertOperation(metrics, SemanticConventions.MetricMessagingClientOperationDuration, expectedOperationName: "send", expectedOperationType: "send");
    }

    [EnabledOnDockerPlatformFact(DockerPlatform.Linux)]
    public async Task BasicConsumeWithTimeoutTimespanTest()
    {
        var topic = await KafkaHelpers.ProduceTestMessageAsync(this.connectionString);

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = this.connectionString,
            GroupId = "test-consumer-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnablePartitionEof = true,
        };
        InstrumentedConsumerBuilder<string, string> consumerBuilder = new(consumerConfig);

        var metrics = new List<Metric>();
        using (var meterProvider = Sdk.CreateMeterProviderBuilder()
                .AddInMemoryExporter(metrics)
                .AddKafkaConsumerInstrumentation(consumerBuilder)
                .Build())
        {
            using (var consumer = consumerBuilder.Build())
            {
                consumer.Subscribe(topic);
                while (true)
                {
                    var consumeResult = consumer.Consume();

                    if (consumeResult == null)
                    {
                        continue;
                    }

                    if (consumeResult.IsPartitionEOF)
                    {
                        break;
                    }
                }

                consumer.Close();
            }

            meterProvider.EnsureMetricsAreFlushed();
        }

        var groups = metrics.GroupBy(m => m.Name).ToArray();

        Assert.Equal(2, groups.Length);
        Assert.Contains(SemanticConventions.MetricMessagingClientConsumedMessages, groups.Select(g => g.Key));
        Assert.Contains(SemanticConventions.MetricMessagingClientOperationDuration, groups.Select(g => g.Key));
        AssertOperation(metrics, SemanticConventions.MetricMessagingClientOperationDuration, expectedOperationName: "poll", expectedOperationType: "receive");
    }

    [EnabledOnDockerPlatformFact(DockerPlatform.Linux)]
    public async Task ClientStatisticsMetricsTest()
    {
        const int MessageCount = 3;
        var topic = $"otel-client-metrics-{Guid.NewGuid()}";
        var producerClientId = $"otel-producer-{Guid.NewGuid()}";
        var consumerClientId = $"otel-consumer-{Guid.NewGuid()}";
        var producerBuilder = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = this.connectionString,
            ClientId = producerClientId,
        }).AsInstrumentedProducerBuilder(new ConfluentKafkaInstrumentedProducerBuilderOptions
        {
            EnableClientMetrics = true,
            EnableMetrics = true,
            StatisticsInterval = TimeSpan.FromSeconds(1),
        });
        var consumerBuilder = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            AutoOffsetReset = AutoOffsetReset.Earliest,
            BootstrapServers = this.connectionString,
            ClientId = consumerClientId,
            EnablePartitionEof = true,
            GroupId = $"otel-consumer-group-{Guid.NewGuid()}",
        }).AsInstrumentedConsumerBuilder(new ConfluentKafkaInstrumentedConsumerBuilderOptions
        {
            EnableClientMetrics = true,
            EnableMetrics = true,
            EnablePartitionMetrics = true,
            StatisticsInterval = TimeSpan.FromSeconds(1),
        });
        var metrics = new List<Metric>();

        using var meterProvider = Sdk.CreateMeterProviderBuilder()
            .AddInMemoryExporter(metrics)
            .AddKafkaProducerInstrumentation(producerBuilder)
            .AddKafkaConsumerInstrumentation(consumerBuilder)
            .Build();
        using var producer = producerBuilder.Build();
        using var consumer = consumerBuilder.Build();

        for (var i = 0; i < MessageCount; i++)
        {
            await producer.ProduceAsync(topic, new Message<string, string> { Value = $"message-{i}" });
        }

        producer.Flush(TimeSpan.FromSeconds(10));
        consumer.Subscribe(topic);
        var consumed = 0;
        var consumeStopwatch = Stopwatch.StartNew();
        while (consumed < MessageCount && consumeStopwatch.Elapsed < TimeSpan.FromSeconds(10))
        {
            var result = consumer.Consume(TimeSpan.FromMilliseconds(250));
            if (result is { IsPartitionEOF: false })
            {
                consumed++;
            }
        }

        Assert.Equal(MessageCount, consumed);

        // Consumer statistics callbacks are served by Consume(), so poll until the metric arrives.
        var stopwatch = Stopwatch.StartNew();
        while (stopwatch.Elapsed < TimeSpan.FromSeconds(10) && !HasMetricPoint(metrics, "kafka.consumer.records_lag", consumerClientId))
        {
            consumer.Consume(TimeSpan.FromMilliseconds(100));
            meterProvider.ForceFlush();
        }

        Assert.True(meterProvider.ForceFlush());

        Assert.Equal(MessageCount, GetLongMetricValue(metrics, "kafka.producer.record_send_total", producerClientId, "topic", topic));
        Assert.True(HasMetricPoint(metrics, "kafka.consumer.records_lag", consumerClientId));
        Assert.Contains(metrics, static metric => metric.Name == SemanticConventions.MetricMessagingClientSentMessages);
        Assert.Contains(metrics, static metric => metric.Name == SemanticConventions.MetricMessagingClientConsumedMessages);
    }

    private static void AssertOperation(
        IEnumerable<Metric> metrics,
        string metricName,
        string expectedOperationName,
        string expectedOperationType)
    {
        // The in-memory exporter may emit the same instrument more than once (e.g. on flush and on
        // dispose), so inspect every matching metric rather than expecting a single instance.
        var matching = metrics.Where(m => m.Name == metricName).ToList();
        Assert.NotEmpty(matching);
        Assert.All(matching, m => Assert.StartsWith("https://opentelemetry.io/schemas/", m.MeterSchemaUrl, StringComparison.Ordinal));

        string? operationName = null;
        string? operationType = null;
        string? system = null;

        foreach (var metric in matching)
        {
            foreach (ref readonly var metricPoint in metric.GetMetricPoints())
            {
                foreach (var tag in metricPoint.Tags)
                {
                    switch (tag.Key)
                    {
                        case SemanticConventions.AttributeMessagingOperationName:
                            operationName = tag.Value?.ToString();
                            break;

                        case SemanticConventions.AttributeMessagingOperationType:
                            operationType = tag.Value?.ToString();
                            break;

                        case SemanticConventions.AttributeMessagingSystem:
                            system = tag.Value?.ToString();
                            break;

                        default:
                            break;
                    }
                }
            }
        }

        Assert.Equal(expectedOperationName, operationName);
        Assert.Equal(expectedOperationType, operationType);
        Assert.Equal(ConfluentKafkaCommon.KafkaMessagingSystem, system);
    }

    private static long GetLongMetricValue(IEnumerable<Metric> metrics, string metricName, string clientId, string tagName, string tagValue)
    {
        foreach (var metric in metrics.Where(metric => metric.Name == metricName))
        {
            foreach (ref readonly var point in metric.GetMetricPoints())
            {
                if (HasTags(point, clientId, tagName, tagValue))
                {
                    return point.GetSumLong();
                }
            }
        }

        throw new Xunit.Sdk.XunitException($"Metric point '{metricName}' was not found.");
    }

    private static bool HasMetricPoint(IEnumerable<Metric> metrics, string metricName, string clientId)
    {
        foreach (var metric in metrics.Where(metric => metric.Name == metricName))
        {
            foreach (ref readonly var point in metric.GetMetricPoints())
            {
                if (HasTags(point, clientId, tagName: null, tagValue: null))
                {
                    return true;
                }
            }
        }

        return false;
    }

    private static bool HasTags(in MetricPoint point, string clientId, string? tagName, string? tagValue)
    {
        var hasClientId = false;
        var hasTag = tagName == null;
        foreach (var tag in point.Tags)
        {
            hasClientId |= string.Equals(tag.Key, "client-id", StringComparison.Ordinal) && string.Equals(tag.Value?.ToString(), clientId, StringComparison.Ordinal);
            hasTag |= string.Equals(tag.Key, tagName, StringComparison.Ordinal) && string.Equals(tag.Value?.ToString(), tagValue, StringComparison.Ordinal);
        }

        return hasClientId && hasTag;
    }
}
