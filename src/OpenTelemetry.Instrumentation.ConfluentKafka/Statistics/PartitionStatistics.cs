// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

namespace OpenTelemetry.Instrumentation.ConfluentKafka;

internal sealed class PartitionStatistics
{
    public int Partition { get; set; }

    public long? ConsumerLag { get; set; }

    public bool? Desired { get; set; }

    public long? ReceivedBytes { get; set; }

    public long? ReceivedMessages { get; set; }

    public long? TransmittedBytes { get; set; }

    public long? TransmittedMessages { get; set; }
}
