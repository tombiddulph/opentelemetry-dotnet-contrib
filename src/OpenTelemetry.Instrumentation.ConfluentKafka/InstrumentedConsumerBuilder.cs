// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

using OpenTelemetry.Instrumentation.ConfluentKafka;

namespace Confluent.Kafka;

/// <summary>
/// A builder of <see cref="IConsumer{TKey, TValue}"/> with support for instrumentation.
/// </summary>
/// <typeparam name="TKey">Type of the key.</typeparam>
/// <typeparam name="TValue">Type of value.</typeparam>
public sealed class InstrumentedConsumerBuilder<TKey, TValue> : ConsumerBuilder<TKey, TValue>
{
    private readonly ConfluentKafkaConsumerInstrumentationOptions<TKey, TValue> options = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="InstrumentedConsumerBuilder{TKey, TValue}"/> class.
    /// </summary>
    /// <param name="config"> A collection of librdkafka configuration parameters (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md) and parameters specific to this client (refer to: <see cref="ConfigPropertyNames" />). At a minimum, 'bootstrap.servers' must be specified.</param>
    public InstrumentedConsumerBuilder(IEnumerable<KeyValuePair<string, string>> config)
        : base(config)
    {
    }

    internal bool EnableMetrics
    {
        get => this.options.Metrics;
        set => this.options.Metrics = value;
    }

    internal bool EnableClientMetrics
    {
        get => this.options.ClientMetrics;
        set => this.options.ClientMetrics = value;
    }

    internal bool EnablePartitionMetrics
    {
        get => this.options.PartitionMetrics;
        set => this.options.PartitionMetrics = value;
    }

    internal TimeSpan StatisticsInterval
    {
        get => this.options.StatisticsInterval;
        set => this.options.StatisticsInterval = value;
    }

    internal bool EnableTraces
    {
        get => this.options.Traces;
        set => this.options.Traces = value;
    }

    /// <summary>
    /// Build a new IConsumer instance.
    /// </summary>
    /// <returns>an <see cref="IProducer{TKey,TValue}"/>.</returns>
    public override IConsumer<TKey, TValue> Build()
    {
        var config = (ConsumerConfig)this.Config;
        KafkaClientMetricsRegistration? metricsRegistration = null;
        Action<IConsumer<TKey, TValue>, string>? userStatisticsHandler = null;

        if (this.options.ClientMetrics)
        {
            KafkaStatisticsConfiguration.ConfigureInterval(config, this.options.StatisticsInterval);
            userStatisticsHandler = this.GetInternalStatisticsHandler();
            var registration = KafkaClientMetrics.Register(this.options.PartitionMetrics);
            metricsRegistration = registration;
            try
            {
                this.SetInternalStatisticsHandler(KafkaStatisticsHandler.Create(registration, userStatisticsHandler));
            }
            catch
            {
                registration.Unregister();
                throw;
            }
        }

        IConsumer<TKey, TValue> builtConsumer;
        try
        {
            builtConsumer = base.Build();
        }
        catch
        {
            metricsRegistration?.Unregister();
            throw;
        }
        finally
        {
            if (metricsRegistration != null)
            {
                this.SetInternalStatisticsHandler(userStatisticsHandler);
            }
        }

        var consumer = new InstrumentedConsumer<TKey, TValue>(builtConsumer, this.options, config.BootstrapServers, metricsRegistration)
        {
            GroupId = config.GroupId,
        };

        return consumer;
    }
}
