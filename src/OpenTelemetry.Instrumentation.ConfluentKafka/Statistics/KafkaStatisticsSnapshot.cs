// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

namespace OpenTelemetry.Instrumentation.ConfluentKafka;

internal sealed class KafkaStatisticsSnapshot
{
    public string? ClientId { get; set; }

    public string? ClientType { get; set; }

    public long? MessageSize { get; set; }

    public long? MessageSizeMaximum { get; set; }

    public ConsumerGroupStatistics? ConsumerGroup { get; set; }

    public IReadOnlyList<BrokerStatistics> Brokers { get; set; } = [];

    public IReadOnlyDictionary<string, IReadOnlyList<PartitionStatistics>> Topics { get; set; }
        = new Dictionary<string, IReadOnlyList<PartitionStatistics>>(StringComparer.Ordinal);
}
