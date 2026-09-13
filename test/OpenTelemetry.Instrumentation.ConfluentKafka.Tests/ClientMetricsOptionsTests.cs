// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

using System.Globalization;
using Confluent.Kafka;

namespace OpenTelemetry.Instrumentation.ConfluentKafka.Tests;

public class ClientMetricsOptionsTests
{
    [Fact]
    public void ProducerOptionsAreCopied()
    {
        var options = new ConfluentKafkaInstrumentedProducerBuilderOptions
        {
            EnableClientMetrics = true,
            EnablePartitionMetrics = true,
            StatisticsInterval = TimeSpan.FromSeconds(2),
        };

        var builder = new ProducerBuilder<string, string>(new ProducerConfig()).AsInstrumentedProducerBuilder(options);

        Assert.True(builder.EnableClientMetrics);
        Assert.True(builder.EnablePartitionMetrics);
        Assert.Equal(TimeSpan.FromSeconds(2), builder.StatisticsInterval);
    }

    [Fact]
    public void ConsumerOptionsAreCopied()
    {
        var options = new ConfluentKafkaInstrumentedConsumerBuilderOptions
        {
            EnableClientMetrics = true,
            EnablePartitionMetrics = true,
            StatisticsInterval = TimeSpan.FromSeconds(2),
        };

        var builder = new ConsumerBuilder<string, string>(new ConsumerConfig()).AsInstrumentedConsumerBuilder(options);

        Assert.True(builder.EnableClientMetrics);
        Assert.True(builder.EnablePartitionMetrics);
        Assert.Equal(TimeSpan.FromSeconds(2), builder.StatisticsInterval);
    }

    [Fact]
    public void ClientMetricsDefaultsAreOptIn()
    {
        var producerOptions = new ConfluentKafkaInstrumentedProducerBuilderOptions();
        var consumerOptions = new ConfluentKafkaInstrumentedConsumerBuilderOptions();

        Assert.False(producerOptions.EnableClientMetrics);
        Assert.False(producerOptions.EnablePartitionMetrics);
        Assert.Equal(TimeSpan.FromSeconds(10), producerOptions.StatisticsInterval);
        Assert.False(consumerOptions.EnableClientMetrics);
        Assert.False(consumerOptions.EnablePartitionMetrics);
        Assert.Equal(TimeSpan.FromSeconds(10), consumerOptions.StatisticsInterval);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void StatisticsIntervalMustBePositive(int milliseconds)
    {
        var producerOptions = new ConfluentKafkaInstrumentedProducerBuilderOptions();
        var consumerOptions = new ConfluentKafkaInstrumentedConsumerBuilderOptions();

        Assert.Throws<ArgumentOutOfRangeException>(() => producerOptions.StatisticsInterval = TimeSpan.FromMilliseconds(milliseconds));
        Assert.Throws<ArgumentOutOfRangeException>(() => consumerOptions.StatisticsInterval = TimeSpan.FromMilliseconds(milliseconds));
    }

    [Theory]
    [InlineData(null, 1234)]
    [InlineData(4321, 4321)]
    public void ProducerBuildConfiguresStatisticsIntervalOnlyWhenMissing(int? configuredInterval, int expectedInterval)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = "localhost:9092",
            StatisticsIntervalMs = configuredInterval,
        };
        var builder = new ProducerBuilder<string, string>(config).AsInstrumentedProducerBuilder(
            new ConfluentKafkaInstrumentedProducerBuilderOptions
            {
                EnableClientMetrics = true,
                StatisticsInterval = TimeSpan.FromMilliseconds(1234),
            });

        using var producer = builder.Build();
        var builtInterval = Assert.Single(
            builder.GetInternalConfig()!,
            static item => string.Equals(item.Key, "statistics.interval.ms", StringComparison.Ordinal)).Value;

        Assert.Equal(expectedInterval.ToString(CultureInfo.InvariantCulture), builtInterval);
    }
}
