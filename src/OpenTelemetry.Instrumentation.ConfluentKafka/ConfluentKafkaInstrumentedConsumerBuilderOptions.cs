// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

namespace Confluent.Kafka;

/// <summary>
/// Options for configuring telemetry on a <see cref="InstrumentedConsumerBuilder{TKey, TValue}"/>
/// when creating an instrumented producer in code.
/// </summary>
public sealed class ConfluentKafkaInstrumentedConsumerBuilderOptions
{
    private TimeSpan statisticsInterval = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Gets or sets a value indicating whether librdkafka client statistics should be emitted as metrics.
    /// </summary>
    public bool EnableClientMetrics { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether metrics should be enabled for the consumer.
    /// </summary>
    public bool EnableMetrics { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether metrics with partition-level attributes should be emitted.
    /// </summary>
    public bool EnablePartitionMetrics { get; set; }

    /// <summary>
    /// Gets or sets the interval at which librdkafka emits client statistics.
    /// </summary>
    public TimeSpan StatisticsInterval
    {
        get => this.statisticsInterval;
        set
        {
            if (value <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(value), value, "The statistics interval must be greater than zero.");
            }

            this.statisticsInterval = value;
        }
    }

    /// <summary>
    /// Gets or sets a value indicating whether tracing should be enabled for the consumer.
    /// </summary>
    public bool EnableTraces { get; set; }
}
