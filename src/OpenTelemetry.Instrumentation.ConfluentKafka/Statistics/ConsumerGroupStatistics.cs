// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

namespace OpenTelemetry.Instrumentation.ConfluentKafka;

internal sealed class ConsumerGroupStatistics
{
    public long? AssignmentSize { get; set; }

    public long? RebalanceCount { get; set; }

    public double? RebalanceAgeSeconds { get; set; }
}
