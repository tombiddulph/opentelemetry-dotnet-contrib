// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

using Confluent.Kafka;

namespace OpenTelemetry.Instrumentation.ConfluentKafka;

internal static class KafkaStatisticsHandler
{
    public static Action<IConsumer<TKey, TValue>, string> Create<TKey, TValue>(
        KafkaClientMetricsRegistration registration,
        Action<IConsumer<TKey, TValue>, string>? userHandler)
        => (consumer, statistics) =>
        {
            try
            {
                registration.Update(statistics);
            }
            catch (Exception ex)
            {
                ConfluentKafkaInstrumentationEventSource.Log.StatisticsParseFailed(ex);
            }

            try
            {
                userHandler?.Invoke(consumer, statistics);
            }
            catch
            {
                // User callbacks must not throw back into librdkafka.
            }
        };

    public static Action<IProducer<TKey, TValue>, string> Create<TKey, TValue>(
        KafkaClientMetricsRegistration registration,
        Action<IProducer<TKey, TValue>, string>? userHandler)
        => (producer, statistics) =>
        {
            try
            {
                registration.Update(statistics);
            }
            catch (Exception ex)
            {
                ConfluentKafkaInstrumentationEventSource.Log.StatisticsParseFailed(ex);
            }

            try
            {
                userHandler?.Invoke(producer, statistics);
            }
            catch
            {
                // User callbacks must not throw back into librdkafka.
            }
        };
}
