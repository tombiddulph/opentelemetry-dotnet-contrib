// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

using Confluent.Kafka;

namespace OpenTelemetry.Instrumentation.ConfluentKafka;

internal static class KafkaStatisticsConfiguration
{
    public static void ConfigureInterval(ClientConfig config, TimeSpan statisticsInterval)
    {
        if (config.Any(static item => string.Equals(item.Key, "statistics.interval.ms", StringComparison.Ordinal)))
        {
            return;
        }

        var totalMilliseconds = Math.Ceiling(statisticsInterval.TotalMilliseconds);
        if (totalMilliseconds > int.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(statisticsInterval), statisticsInterval, "The statistics interval is too large.");
        }

        config.StatisticsIntervalMs = (int)totalMilliseconds;
    }
}
