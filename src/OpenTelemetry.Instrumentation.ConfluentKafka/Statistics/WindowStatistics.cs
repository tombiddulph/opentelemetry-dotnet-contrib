// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

namespace OpenTelemetry.Instrumentation.ConfluentKafka;

internal sealed class WindowStatistics
{
    public double? Average { get; set; }

    public double? Maximum { get; set; }

    public long? Count { get; set; }
}
