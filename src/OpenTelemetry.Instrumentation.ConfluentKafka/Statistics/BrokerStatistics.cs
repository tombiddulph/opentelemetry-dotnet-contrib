// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

namespace OpenTelemetry.Instrumentation.ConfluentKafka;

internal sealed class BrokerStatistics
{
    public int NodeId { get; set; }

    public string? State { get; set; }

    public long? RequestsInFlight { get; set; }

    public long? RequestsSent { get; set; }

    public long? ResponsesReceived { get; set; }

    public long? Retries { get; set; }

    public long? IncomingBytes { get; set; }

    public long? OutgoingBytes { get; set; }

    public WindowStatistics? InternalLatencyMilliseconds { get; set; }

    public WindowStatistics? RequestLatencyMilliseconds { get; set; }

    public WindowStatistics? ThrottleMilliseconds { get; set; }
}
