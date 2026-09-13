// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

using System.Diagnostics.Tracing;
using System.Reflection;
using System.Text.Json;
using Confluent.Kafka;
using OpenTelemetry.Metrics;

namespace OpenTelemetry.Instrumentation.ConfluentKafka.Tests;

public class KafkaStatisticsTests
{
    [Fact]
    public void ParsesProducerStatistics()
    {
        var snapshot = KafkaStatisticsParser.Parse(ReadFixture("librdkafka-2.4.0-producer.json"));

        Assert.Equal("fixture-producer", snapshot.ClientId);
        Assert.Equal("producer", snapshot.ClientType);
        Assert.Equal(100, snapshot.MessageSize);
        Assert.Equal(1000, snapshot.MessageSizeMaximum);
        Assert.Equal(2, snapshot.Brokers.Count);
        Assert.Equal(3, snapshot.Brokers[0].RequestLatencyMilliseconds?.Average);
        Assert.Equal(5, snapshot.Brokers[0].RequestLatencyMilliseconds?.Maximum);
        Assert.Equal(2, snapshot.Brokers[0].InternalLatencyMilliseconds?.Average);
        Assert.Equal(7, snapshot.Brokers[0].ThrottleMilliseconds?.Average);
        Assert.Equal(2, snapshot.Topics["orders"].Count);
        Assert.Equal(10, snapshot.Topics["orders"].Sum(static partition => partition.TransmittedMessages));
    }

    [Fact]
    public void ParsesConsumerStatisticsAndFiltersSentinels()
    {
        var snapshot = KafkaStatisticsParser.Parse(ReadFixture("librdkafka-2.4.0-consumer.json"));

        Assert.Equal("fixture-consumer", snapshot.ClientId);
        Assert.Equal(1, snapshot.ConsumerGroup?.AssignmentSize);
        Assert.Equal(2, snapshot.ConsumerGroup?.RebalanceCount);
        Assert.Equal(3.5, snapshot.ConsumerGroup?.RebalanceAgeSeconds);
        Assert.Single(snapshot.Brokers);
        Assert.Equal(3, snapshot.Topics["orders"].Count);
        Assert.Equal(1011, snapshot.Topics["orders"].Sum(static partition => partition.ReceivedMessages));
        Assert.Equal(4, snapshot.Topics["orders"][0].ConsumerLag);
        Assert.False(snapshot.Topics["orders"][1].Desired);
        Assert.Null(snapshot.Topics["orders"][2].ConsumerLag);
    }

    [Theory]
    [InlineData("librdkafka-2.4.0-producer.json")]
    [InlineData("librdkafka-2.4.0-consumer.json")]
    public void FixtureContainsMappedSchema(string fixtureName)
    {
        using var document = JsonDocument.Parse(ReadFixture(fixtureName));
        var root = document.RootElement;

        AssertProperties(root, "client_id", "type", "msg_size", "msg_size_max", "brokers", "topics");
        foreach (var broker in root.GetProperty("brokers").EnumerateObject())
        {
            AssertProperties(broker.Value, "nodeid", "state", "waitresp_cnt", "tx", "txbytes", "txretries", "rx", "rxbytes", "int_latency", "rtt", "throttle");
            AssertWindow(broker.Value.GetProperty("int_latency"));
            AssertWindow(broker.Value.GetProperty("rtt"));
            AssertWindow(broker.Value.GetProperty("throttle"));
        }

        foreach (var topic in root.GetProperty("topics").EnumerateObject())
        {
            foreach (var partition in topic.Value.GetProperty("partitions").EnumerateObject())
            {
                AssertProperties(partition.Value, "partition", "desired", "txmsgs", "txbytes", "rxmsgs", "rxbytes", "consumer_lag_stored");
            }
        }

        if (string.Equals(root.GetProperty("type").GetString(), "consumer", StringComparison.Ordinal))
        {
            AssertProperties(root.GetProperty("cgrp"), "assignment_size", "rebalance_cnt", "rebalance_age");
        }
    }

    [Fact]
    public void MalformedStatisticsAreLoggedAndUserHandlerIsInvoked()
    {
        var events = new List<EventWrittenEventArgs>();
        using var listener = new StatisticsEventListener(events);
        var registration = KafkaClientMetrics.Register(enablePartitionMetrics: false);
        var userHandlerInvoked = false;
        Action<IProducer<string, string>, string> userHandler = (_, _) => userHandlerInvoked = true;
        var handler = KafkaStatisticsHandler.Create(registration, userHandler);

        try
        {
            handler(null!, "{truncated");
        }
        finally
        {
            registration.Unregister();
        }

        Assert.True(userHandlerInvoked);
        var loggedEvent = Assert.Single(events, static item => item.EventId == 2);
        Assert.Contains("Json", (string?)loggedEvent.Payload?[0]);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void EmitsExpectedMetrics(bool enablePartitionMetrics)
    {
        var metrics = new List<Metric>();
        using var provider = Sdk.CreateMeterProviderBuilder()
            .AddMeter(ConfluentKafkaCommon.Meter.Name)
            .AddInMemoryExporter(metrics)
            .Build();
        var producerRegistration = KafkaClientMetrics.Register(enablePartitionMetrics);
        var consumerRegistration = KafkaClientMetrics.Register(enablePartitionMetrics);

        try
        {
            producerRegistration.Update(ReadFixture("librdkafka-2.4.0-producer.json"));
            consumerRegistration.Update(ReadFixture("librdkafka-2.4.0-consumer.json"));
            Assert.True(provider.ForceFlush());

            Assert.Equal(10, GetLongValue(metrics, "kafka.producer.record_send_total", "fixture-producer", "topic", "orders"));
            Assert.Equal(4, GetLongValue(metrics, "kafka.producer.record_retry_total", "fixture-producer"));
            Assert.Equal(3, GetDoubleValue(metrics, "kafka.producer.requests_in_flight", "fixture-producer"));
            Assert.Equal(900, GetDoubleValue(metrics, "kafka.producer.buffer_available_bytes", "fixture-producer"));
            Assert.Equal(3, GetDoubleValue(metrics, "kafka.producer.record_queue_time_avg", "fixture-producer"));
            Assert.Equal(8, GetDoubleValue(metrics, "kafka.producer.produce_throttle_time_avg", "fixture-producer"));
            Assert.Equal(12, GetDoubleValue(metrics, "kafka.producer.produce_throttle_time_max", "fixture-producer"));
            Assert.Equal(3, GetDoubleValue(metrics, "kafka.producer.request_latency_avg", "fixture-producer", "node-id", "1"));
            Assert.Equal(1, GetDoubleValue(metrics, "kafka.producer.connection_count", "fixture-producer"));

            Assert.Equal(1011, GetLongValue(metrics, "kafka.consumer.records_consumed_total", "fixture-consumer", "topic", "orders"));
            Assert.Equal(1, GetDoubleValue(metrics, "kafka.consumer.assigned_partitions", "fixture-consumer"));
            Assert.Equal(3.5, GetDoubleValue(metrics, "kafka.consumer.last_rebalance_seconds_ago", "fixture-consumer"));
            Assert.Equal(enablePartitionMetrics, HasPoint(metrics, "kafka.consumer.records_lag", "fixture-consumer", "partition", "0"));
        }
        finally
        {
            producerRegistration.Unregister();
            consumerRegistration.Unregister();
        }
    }

    [Fact]
    public void StatisticsIntervalHonorsExplicitConfiguration()
    {
        var implicitConfig = new ProducerConfig();
        KafkaStatisticsConfiguration.ConfigureInterval(implicitConfig, TimeSpan.FromMilliseconds(1234));
        Assert.Equal(1234, implicitConfig.StatisticsIntervalMs);

        var explicitConfig = new ProducerConfig { StatisticsIntervalMs = 4321 };
        KafkaStatisticsConfiguration.ConfigureInterval(explicitConfig, TimeSpan.FromMilliseconds(1234));
        Assert.Equal(4321, explicitConfig.StatisticsIntervalMs);
    }

    [Fact]
    public void AggregatesClientsWithTheSameClientIdIntoOneSeries()
    {
        var metrics = new List<Metric>();
        using var provider = Sdk.CreateMeterProviderBuilder()
            .AddMeter(ConfluentKafkaCommon.Meter.Name)
            .AddInMemoryExporter(metrics)
            .Build();
        var firstRegistration = KafkaClientMetrics.Register(enablePartitionMetrics: false);
        var secondRegistration = KafkaClientMetrics.Register(enablePartitionMetrics: false);

        try
        {
            var statistics = ReadFixture("librdkafka-2.4.0-producer.json");
            firstRegistration.Update(statistics);
            secondRegistration.Update(statistics);
            Assert.True(provider.ForceFlush());

            Assert.Equal(20, GetLongValue(metrics, "kafka.producer.record_send_total", "fixture-producer", "topic", "orders"));
            Assert.Equal(1, CountPoints(metrics, "kafka.producer.record_send_total", "fixture-producer", "topic", "orders"));

            firstRegistration.Unregister();
            Assert.True(provider.ForceFlush());
            Assert.Equal(20, GetLongValue(metrics, "kafka.producer.record_send_total", "fixture-producer", "topic", "orders"));
        }
        finally
        {
            firstRegistration.Unregister();
            secondRegistration.Unregister();
        }
    }

    [Fact]
    public void PartialPartitionStatisticsDoNotDoubleCount()
    {
        var metrics = new List<Metric>();
        using var provider = Sdk.CreateMeterProviderBuilder()
            .AddMeter(ConfluentKafkaCommon.Meter.Name)
            .AddInMemoryExporter(metrics)
            .Build();
        var registration = KafkaClientMetrics.Register(enablePartitionMetrics: false);

        try
        {
            registration.Update(ReadFixture("librdkafka-2.4.0-producer.json"));
            registration.Update("{\"client_id\":\"fixture-producer\",\"type\":\"producer\",\"topics\":{\"orders\":{\"partitions\":{\"0\":{\"partition\":0,\"txmsgs\":5}}}}}");
            Assert.True(provider.ForceFlush());
            Assert.Equal(11, GetLongValue(metrics, "kafka.producer.record_send_total", "fixture-producer", "topic", "orders"));

            registration.Update("{\"client_id\":\"fixture-producer\",\"type\":\"producer\",\"topics\":{\"orders\":{\"partitions\":{\"0\":{\"partition\":0,\"txmsgs\":5},\"1\":{\"partition\":1,\"txmsgs\":7}}}}}");
            Assert.True(provider.ForceFlush());
            Assert.Equal(12, GetLongValue(metrics, "kafka.producer.record_send_total", "fixture-producer", "topic", "orders"));

            for (var i = 0; i < 4; i++)
            {
                registration.Update("{\"client_id\":\"fixture-producer\",\"type\":\"producer\",\"topics\":{\"orders\":{\"partitions\":{\"0\":{\"partition\":0,\"txmsgs\":5}}}}}");
            }

            registration.Update("{\"client_id\":\"fixture-producer\",\"type\":\"producer\",\"topics\":{\"orders\":{\"partitions\":{\"0\":{\"partition\":0,\"txmsgs\":5},\"1\":{\"partition\":1,\"txmsgs\":8}}}}}");
            Assert.True(provider.ForceFlush());
            Assert.Equal(13, GetLongValue(metrics, "kafka.producer.record_send_total", "fixture-producer", "topic", "orders"));
        }
        finally
        {
            registration.Unregister();
        }
    }

    private static string ReadFixture(string name)
    {
        var assembly = Assembly.GetExecutingAssembly();
        var resourceName = Assert.Single(assembly.GetManifestResourceNames(), value => value.EndsWith(name, StringComparison.Ordinal));
        using var stream = assembly.GetManifestResourceStream(resourceName);
        Assert.NotNull(stream);
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }

    private static void AssertProperties(JsonElement element, params string[] names)
    {
        foreach (var name in names)
        {
            Assert.True(element.TryGetProperty(name, out _), $"Expected property '{name}'.");
        }
    }

    private static void AssertWindow(JsonElement element)
        => AssertProperties(element, "avg", "max", "cnt");

    private static long GetLongValue(IEnumerable<Metric> metrics, string metricName, string clientId, string? tagName = null, string? tagValue = null)
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

    private static double GetDoubleValue(IEnumerable<Metric> metrics, string metricName, string clientId, string? tagName = null, string? tagValue = null)
    {
        foreach (var metric in metrics.Where(metric => metric.Name == metricName))
        {
            foreach (ref readonly var point in metric.GetMetricPoints())
            {
                if (HasTags(point, clientId, tagName, tagValue))
                {
                    return point.GetGaugeLastValueDouble();
                }
            }
        }

        throw new Xunit.Sdk.XunitException($"Metric point '{metricName}' was not found.");
    }

    private static bool HasPoint(IEnumerable<Metric> metrics, string metricName, string clientId, string tagName, string tagValue)
    {
        foreach (var metric in metrics.Where(metric => metric.Name == metricName))
        {
            foreach (ref readonly var point in metric.GetMetricPoints())
            {
                if (HasTags(point, clientId, tagName, tagValue))
                {
                    return true;
                }
            }
        }

        return false;
    }

    private static int CountPoints(IEnumerable<Metric> metrics, string metricName, string clientId, string tagName, string tagValue)
    {
        var count = 0;
        foreach (var metric in metrics.Where(metric => metric.Name == metricName))
        {
            foreach (ref readonly var point in metric.GetMetricPoints())
            {
                if (HasTags(point, clientId, tagName, tagValue))
                {
                    count++;
                }
            }
        }

        return count;
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

    private sealed class StatisticsEventListener : EventListener
    {
        private readonly List<EventWrittenEventArgs> events;
        private readonly int threadId = Environment.CurrentManagedThreadId;

        public StatisticsEventListener(List<EventWrittenEventArgs> events)
        {
            this.events = events;
        }

        protected override void OnEventSourceCreated(EventSource eventSource)
        {
            if (eventSource.Name == "OpenTelemetry-Instrumentation-ConfluentKafka")
            {
                this.EnableEvents(eventSource, EventLevel.Warning, EventKeywords.All);
            }
        }

        protected override void OnEventWritten(EventWrittenEventArgs eventData)
        {
            if (Environment.CurrentManagedThreadId == this.threadId)
            {
                this.events.Add(eventData);
            }
        }
    }
}
