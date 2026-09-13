// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

using System.Diagnostics.Tracing;

namespace OpenTelemetry.Instrumentation.ConfluentKafka;

[EventSource(Name = "OpenTelemetry-Instrumentation-ConfluentKafka")]
internal sealed class ConfluentKafkaInstrumentationEventSource : EventSource
{
    public static ConfluentKafkaInstrumentationEventSource Log = new();

    private const int EventIdFailedToFetchClusterId = 1;
    private const int EventIdStatisticsParseFailed = 2;
    private const int EventIdStatisticsBridgeStarted = 3;

    [NonEvent]
    public void FailedToFetchClusterId(Exception ex)
    {
        if (this.IsEnabled(EventLevel.Warning, EventKeywords.All))
        {
            this.FailedToFetchClusterId(ex.ToString());
        }
    }

    [Event(EventIdFailedToFetchClusterId, Message = "Failed to fetch Kafka cluster id. Exception: '{0}'", Level = EventLevel.Warning)]
    public void FailedToFetchClusterId(string exception)
    {
        this.WriteEvent(EventIdFailedToFetchClusterId, exception);
    }

    [NonEvent]
    public void StatisticsParseFailed(Exception ex)
    {
        if (this.IsEnabled(EventLevel.Warning, EventKeywords.All))
        {
            this.StatisticsParseFailed(ex.ToString());
        }
    }

    [Event(EventIdStatisticsParseFailed, Message = "Failed to parse librdkafka statistics. Exception: '{0}'", Level = EventLevel.Warning)]
    public void StatisticsParseFailed(string exception)
    {
        this.WriteEvent(EventIdStatisticsParseFailed, exception);
    }

    [Event(EventIdStatisticsBridgeStarted, Message = "Started the librdkafka statistics metrics bridge. librdkafka version: '{0}'", Level = EventLevel.Informational)]
    public void StatisticsBridgeStarted(string librdkafkaVersion)
    {
        this.WriteEvent(EventIdStatisticsBridgeStarted, librdkafkaVersion);
    }
}
