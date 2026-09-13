// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

namespace OpenTelemetry.Instrumentation.ConfluentKafka;

internal sealed class KafkaClientMetricsRegistration
{
    private readonly long id;
    private int disposed;

    internal KafkaClientMetricsRegistration(long id)
    {
        this.id = id;
    }

    internal bool IsRegistered => KafkaClientMetrics.IsRegistered(this.id);

    public void Unregister()
    {
        if (Interlocked.Exchange(ref this.disposed, 1) == 0)
        {
            KafkaClientMetrics.Unregister(this.id);
        }
    }

    internal void Update(string json)
    {
        try
        {
            var snapshot = KafkaStatisticsParser.Parse(json);
            if (Volatile.Read(ref this.disposed) == 0)
            {
                KafkaClientMetrics.Update(this.id, snapshot);
            }
        }
        catch (Exception ex)
        {
            ConfluentKafkaInstrumentationEventSource.Log.StatisticsParseFailed(ex);
        }
    }
}
