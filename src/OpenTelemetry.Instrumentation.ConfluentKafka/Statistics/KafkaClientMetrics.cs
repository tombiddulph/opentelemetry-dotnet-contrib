// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using Confluent.Kafka;

namespace OpenTelemetry.Instrumentation.ConfluentKafka;

internal static class KafkaClientMetrics
{
    private const string Consumer = "consumer";
    private const string Producer = "producer";
    private const string ClientIdTag = "client-id";
    private const string NodeIdTag = "node-id";
    private const string PartitionTag = "partition";
    private const string TopicTag = "topic";
    private const int MissingSourceRetentionIntervals = 3;
    private const int MaximumExpiredCounterSources = 4096;

    private static readonly ConcurrentDictionary<long, Entry> Entries = new();
    private static readonly Dictionary<CounterSeriesKey, long> CounterValues = new();
    private static readonly object CounterLock = new();

    private static long nextId;
    private static int bridgeStarted;

    static KafkaClientMetrics()
    {
        CreateProducerMetrics();
        CreateConsumerMetrics();
        CreateNodeMetrics(Producer);
        CreateNodeMetrics(Consumer);
    }

    private enum CounterMetric
    {
        ProducerRecordsSent,
        ProducerBytesSent,
        ProducerRetries,
        ProducerRequests,
        ProducerResponses,
        ProducerIncomingBytes,
        ProducerOutgoingBytes,
        ConsumerRebalances,
        ConsumerRecordsConsumed,
        ConsumerBytesConsumed,
        ConsumerRequests,
        ConsumerResponses,
        ConsumerIncomingBytes,
        ConsumerOutgoingBytes,
    }

    internal static KafkaClientMetricsRegistration Register(bool enablePartitionMetrics)
    {
        if (Interlocked.Exchange(ref bridgeStarted, 1) == 0)
        {
            ConfluentKafkaInstrumentationEventSource.Log.StatisticsBridgeStarted(Library.VersionString);
        }

        var id = Interlocked.Increment(ref nextId);
        Entries[id] = new Entry(enablePartitionMetrics);
        return new KafkaClientMetricsRegistration(id);
    }

    internal static void Unregister(long id)
    {
        lock (CounterLock)
        {
            if (!Entries.TryRemove(id, out _))
            {
                return;
            }

            PruneInactiveCounterSeries();
        }
    }

    internal static bool IsRegistered(long id)
        => Entries.ContainsKey(id);

    internal static void Update(long id, KafkaStatisticsSnapshot snapshot)
    {
        lock (CounterLock)
        {
            if (!Entries.TryGetValue(id, out var entry))
            {
                return;
            }

            var extractedValues = ExtractCounterValues(snapshot);
            entry.ActiveCounters.Clear();
            var deltas = new Dictionary<CounterSeriesKey, long>();
            foreach (var item in extractedValues)
            {
                if (!entry.CounterValues.TryGetValue(item.Key, out var previousValue)
                    && entry.ExpiredCounterValues.TryGetValue(item.Key, out var expiredValue))
                {
                    previousValue = expiredValue;
                    entry.ExpiredCounterValues.Remove(item.Key);
                }

                var delta = item.Value >= previousValue ? item.Value - previousValue : item.Value;
                deltas.TryGetValue(item.Key.Series, out var aggregateDelta);
                deltas[item.Key.Series] = aggregateDelta + delta;
                entry.CounterValues[item.Key] = item.Value;
                entry.ActiveCounters.Add(item.Key);
                entry.MissingCounterIntervals.Remove(item.Key);
            }

            PruneMissingCounterSources(entry);

            foreach (var item in deltas)
            {
                if (CounterValues.TryGetValue(item.Key, out var aggregateValue))
                {
                    CounterValues[item.Key] = aggregateValue + item.Value;
                }
                else
                {
                    CounterValues[item.Key] = Entries.Values.Sum(candidate => candidate.ActiveCounters
                        .Where(source => source.Series == item.Key)
                        .Sum(source => candidate.CounterValues[source]));
                }
            }

            PruneInactiveCounterSeries();
            entry.Snapshot = snapshot;
        }
    }

    private static void CreateProducerMetrics()
    {
        ConfluentKafkaCommon.Meter.CreateObservableCounter(
            "kafka.producer.record_send_total",
            () => ObserveCounters(CounterMetric.ProducerRecordsSent),
            unit: "{record}",
            description: "The total number of records sent.");
        ConfluentKafkaCommon.Meter.CreateObservableCounter(
            "kafka.producer.byte_total",
            () => ObserveCounters(CounterMetric.ProducerBytesSent),
            unit: "By",
            description: "The total number of bytes sent.");
        ConfluentKafkaCommon.Meter.CreateObservableCounter(
            "kafka.producer.record_retry_total",
            () => ObserveCounters(CounterMetric.ProducerRetries),
            unit: "{record}",
            description: "The total number of retried requests.");
        ConfluentKafkaCommon.Meter.CreateObservableGauge(
            "kafka.producer.requests_in_flight",
            () => ObserveBrokerGauge(Producer, static broker => broker.RequestsInFlight),
            unit: "{request}",
            description: "The number of requests awaiting a response.");
        ConfluentKafkaCommon.Meter.CreateObservableGauge(
            "kafka.producer.buffer_total_bytes",
            () => ObserveClientGauge(Producer, static snapshot => snapshot.MessageSizeMaximum),
            unit: "By",
            description: "The total producer queue capacity in bytes.");
        ConfluentKafkaCommon.Meter.CreateObservableGauge(
            "kafka.producer.buffer_available_bytes",
            () => ObserveClientGauge(Producer, static snapshot => AvailableBufferBytes(snapshot)),
            unit: "By",
            description: "The available producer queue capacity in bytes.");
        ConfluentKafkaCommon.Meter.CreateObservableGauge(
            "kafka.producer.record_queue_time_avg",
            () => ObserveWindowAverage(Producer, static broker => broker.InternalLatencyMilliseconds),
            unit: "ms",
            description: "The average time records spend in the producer queue.");
        ConfluentKafkaCommon.Meter.CreateObservableGauge(
            "kafka.producer.produce_throttle_time_avg",
            () => ObserveWindowAverage(Producer, static broker => broker.ThrottleMilliseconds),
            unit: "ms",
            description: "The average broker throttle time.");
        ConfluentKafkaCommon.Meter.CreateObservableGauge(
            "kafka.producer.produce_throttle_time_max",
            () => ObserveWindowMaximum(Producer, static broker => broker.ThrottleMilliseconds),
            unit: "ms",
            description: "The maximum broker throttle time.");
    }

    private static void CreateConsumerMetrics()
    {
        ConfluentKafkaCommon.Meter.CreateObservableGauge(
            "kafka.consumer.assigned_partitions",
            () => ObserveClientGauge(Consumer, static snapshot => snapshot.ConsumerGroup?.AssignmentSize),
            unit: "{partition}",
            description: "The number of partitions currently assigned to the consumer.");
        ConfluentKafkaCommon.Meter.CreateObservableCounter(
            "kafka.consumer.rebalance_total",
            () => ObserveCounters(CounterMetric.ConsumerRebalances),
            unit: "{rebalance}",
            description: "The total number of consumer group rebalances.");
        ConfluentKafkaCommon.Meter.CreateObservableGauge(
            "kafka.consumer.last_rebalance_seconds_ago",
            () => ObserveClientDoubleGauge(Consumer, static snapshot => snapshot.ConsumerGroup?.RebalanceAgeSeconds),
            unit: "s",
            description: "The time since the last consumer group rebalance.");
        ConfluentKafkaCommon.Meter.CreateObservableCounter(
            "kafka.consumer.records_consumed_total",
            () => ObserveCounters(CounterMetric.ConsumerRecordsConsumed),
            unit: "{record}",
            description: "The total number of records consumed.");
        ConfluentKafkaCommon.Meter.CreateObservableCounter(
            "kafka.consumer.bytes_consumed_total",
            () => ObserveCounters(CounterMetric.ConsumerBytesConsumed),
            unit: "By",
            description: "The total number of bytes consumed.");
        ConfluentKafkaCommon.Meter.CreateObservableGauge(
            "kafka.consumer.records_lag",
            ObservePartitionLag,
            unit: "{record}",
            description: "The difference between the end offset and the consumer's current position.");
    }

    private static void CreateNodeMetrics(string clientType)
    {
        var prefix = string.Concat("kafka.", clientType, ".");
        ConfluentKafkaCommon.Meter.CreateObservableGauge(
            string.Concat(prefix, "request_latency_avg"),
            () => ObserveNodeWindowAverage(clientType, static broker => broker.RequestLatencyMilliseconds),
            unit: "ms",
            description: "The average request latency for a broker.");
        ConfluentKafkaCommon.Meter.CreateObservableGauge(
            string.Concat(prefix, "request_latency_max"),
            () => ObserveNodeWindowMaximum(clientType, static broker => broker.RequestLatencyMilliseconds),
            unit: "ms",
            description: "The maximum request latency for a broker.");
        ConfluentKafkaCommon.Meter.CreateObservableCounter(
            string.Concat(prefix, "request_total"),
            () => ObserveCounters(string.Equals(clientType, Producer, StringComparison.Ordinal) ? CounterMetric.ProducerRequests : CounterMetric.ConsumerRequests),
            unit: "{request}",
            description: "The total number of requests sent to a broker.");
        ConfluentKafkaCommon.Meter.CreateObservableCounter(
            string.Concat(prefix, "response_total"),
            () => ObserveCounters(string.Equals(clientType, Producer, StringComparison.Ordinal) ? CounterMetric.ProducerResponses : CounterMetric.ConsumerResponses),
            unit: "{response}",
            description: "The total number of responses received from a broker.");
        ConfluentKafkaCommon.Meter.CreateObservableCounter(
            string.Concat(prefix, "incoming_byte_total"),
            () => ObserveCounters(string.Equals(clientType, Producer, StringComparison.Ordinal) ? CounterMetric.ProducerIncomingBytes : CounterMetric.ConsumerIncomingBytes),
            unit: "By",
            description: "The total number of bytes received from a broker.");
        ConfluentKafkaCommon.Meter.CreateObservableCounter(
            string.Concat(prefix, "outgoing_byte_total"),
            () => ObserveCounters(string.Equals(clientType, Producer, StringComparison.Ordinal) ? CounterMetric.ProducerOutgoingBytes : CounterMetric.ConsumerOutgoingBytes),
            unit: "By",
            description: "The total number of bytes sent to a broker.");
        ConfluentKafkaCommon.Meter.CreateObservableGauge(
            string.Concat(prefix, "connection_count"),
            () => ObserveConnectionCount(clientType),
            unit: "{connection}",
            description: "The number of brokers with an active connection.");
    }

    private static IEnumerable<Measurement<long>> ObserveCounters(CounterMetric metric)
    {
        KeyValuePair<CounterSeriesKey, long>[] values;
        lock (CounterLock)
        {
            values = CounterValues.Where(item => item.Key.Metric == metric).ToArray();
        }

        foreach (var item in values)
        {
            if (item.Key.DimensionName == null)
            {
                yield return new Measurement<long>(item.Value, ClientTags(item.Key.ClientId));
            }
            else
            {
                yield return new Measurement<long>(item.Value, new TagList
                {
                    { ClientIdTag, item.Key.ClientId },
                    { item.Key.DimensionName, item.Key.DimensionValue },
                });
            }
        }
    }

    private static Dictionary<SourceCounterKey, long> ExtractCounterValues(KafkaStatisticsSnapshot snapshot)
    {
        var values = new Dictionary<SourceCounterKey, long>();
        if (snapshot.ClientId == null)
        {
            return values;
        }

        if (string.Equals(snapshot.ClientType, Producer, StringComparison.Ordinal))
        {
            AddBrokerCounters(values, snapshot, CounterMetric.ProducerRetries, dimensionName: null, static broker => broker.Retries);
            AddTopicCounters(values, snapshot, CounterMetric.ProducerRecordsSent, static partition => partition.TransmittedMessages);
            AddTopicCounters(values, snapshot, CounterMetric.ProducerBytesSent, static partition => partition.TransmittedBytes);
            AddBrokerCounters(values, snapshot, CounterMetric.ProducerRequests, NodeIdTag, static broker => broker.RequestsSent);
            AddBrokerCounters(values, snapshot, CounterMetric.ProducerResponses, NodeIdTag, static broker => broker.ResponsesReceived);
            AddBrokerCounters(values, snapshot, CounterMetric.ProducerIncomingBytes, NodeIdTag, static broker => broker.IncomingBytes);
            AddBrokerCounters(values, snapshot, CounterMetric.ProducerOutgoingBytes, NodeIdTag, static broker => broker.OutgoingBytes);
        }
        else if (string.Equals(snapshot.ClientType, Consumer, StringComparison.Ordinal))
        {
            AddCounter(values, new(new(CounterMetric.ConsumerRebalances, snapshot.ClientId), source: string.Empty), snapshot.ConsumerGroup?.RebalanceCount);
            AddTopicCounters(values, snapshot, CounterMetric.ConsumerRecordsConsumed, static partition => partition.ReceivedMessages);
            AddTopicCounters(values, snapshot, CounterMetric.ConsumerBytesConsumed, static partition => partition.ReceivedBytes);
            AddBrokerCounters(values, snapshot, CounterMetric.ConsumerRequests, NodeIdTag, static broker => broker.RequestsSent);
            AddBrokerCounters(values, snapshot, CounterMetric.ConsumerResponses, NodeIdTag, static broker => broker.ResponsesReceived);
            AddBrokerCounters(values, snapshot, CounterMetric.ConsumerIncomingBytes, NodeIdTag, static broker => broker.IncomingBytes);
            AddBrokerCounters(values, snapshot, CounterMetric.ConsumerOutgoingBytes, NodeIdTag, static broker => broker.OutgoingBytes);
        }

        return values;
    }

    private static void AddTopicCounters(
        Dictionary<SourceCounterKey, long> values,
        KafkaStatisticsSnapshot snapshot,
        CounterMetric metric,
        Func<PartitionStatistics, long?> selector)
    {
        foreach (var topic in snapshot.Topics)
        {
            var series = new CounterSeriesKey(metric, snapshot.ClientId!, TopicTag, topic.Key);
            foreach (var partition in topic.Value)
            {
                AddCounter(values, new(series, partition.Partition.ToString(CultureInfo.InvariantCulture)), selector(partition));
            }
        }
    }

    private static void AddBrokerCounters(
        Dictionary<SourceCounterKey, long> values,
        KafkaStatisticsSnapshot snapshot,
        CounterMetric metric,
        string? dimensionName,
        Func<BrokerStatistics, long?> selector)
    {
        foreach (var broker in snapshot.Brokers)
        {
            var nodeId = broker.NodeId.ToString(CultureInfo.InvariantCulture);
            var series = new CounterSeriesKey(metric, snapshot.ClientId!, dimensionName, dimensionName == null ? null : nodeId);
            AddCounter(values, new(series, nodeId), selector(broker));
        }
    }

    private static void AddCounter(Dictionary<SourceCounterKey, long> values, SourceCounterKey key, long? value)
    {
        if (value is not { } result)
        {
            return;
        }

        values.TryGetValue(key, out var existingValue);
        values[key] = existingValue + result;
    }

    private static void PruneInactiveCounterSeries()
    {
        var activeSeries = new HashSet<CounterSeriesKey>(Entries.Values
            .SelectMany(static entry => entry.ActiveCounters)
            .Select(static source => source.Series));

        foreach (var series in CounterValues.Keys.Where(series => !activeSeries.Contains(series)).ToList())
        {
            CounterValues.Remove(series);
        }

        foreach (var entry in Entries.Values)
        {
            foreach (var source in entry.CounterValues.Keys.Where(source => !activeSeries.Contains(source.Series)).ToList())
            {
                entry.CounterValues.Remove(source);
                entry.MissingCounterIntervals.Remove(source);
                entry.ExpiredCounterValues.Remove(source);
            }

            foreach (var source in entry.ExpiredCounterValues.Keys.Where(source => !activeSeries.Contains(source.Series)).ToList())
            {
                entry.ExpiredCounterValues.Remove(source);
            }
        }
    }

    private static void PruneMissingCounterSources(Entry entry)
    {
        foreach (var source in entry.CounterValues.Keys.Where(source => !entry.ActiveCounters.Contains(source)).ToList())
        {
            entry.MissingCounterIntervals.TryGetValue(source, out var missingIntervals);
            missingIntervals++;
            if (missingIntervals > MissingSourceRetentionIntervals)
            {
                RememberExpiredCounterSource(entry, source, entry.CounterValues[source]);
                entry.CounterValues.Remove(source);
                entry.MissingCounterIntervals.Remove(source);
            }
            else
            {
                entry.MissingCounterIntervals[source] = missingIntervals;
            }
        }
    }

    private static void RememberExpiredCounterSource(Entry entry, SourceCounterKey source, long value)
    {
        if (entry.ExpiredCounterValues.ContainsKey(source))
        {
            entry.ExpiredCounterValues[source] = value;
            return;
        }

        entry.ExpiredCounterValues[source] = value;
        if (entry.ExpiredCounterValues.Count > MaximumExpiredCounterSources)
        {
            var sourceToRemove = entry.ExpiredCounterValues.Keys.First(candidate => candidate != source);
            entry.ExpiredCounterValues.Remove(sourceToRemove);
        }
    }

    private static IEnumerable<Measurement<double>> ObserveClientGauge(string clientType, Func<KafkaStatisticsSnapshot, long?> selector)
    {
        foreach (var group in GetSnapshotGroups(clientType))
        {
            long total = 0;
            var hasValue = false;
            foreach (var snapshot in group)
            {
                if (selector(snapshot) is { } value)
                {
                    total += value;
                    hasValue = true;
                }
            }

            if (hasValue)
            {
                yield return new Measurement<double>(total, ClientTags(group.Key));
            }
        }
    }

    private static IEnumerable<Measurement<double>> ObserveClientDoubleGauge(string clientType, Func<KafkaStatisticsSnapshot, double?> selector)
    {
        foreach (var group in GetSnapshotGroups(clientType))
        {
            double? minimum = null;
            foreach (var snapshot in group)
            {
                if (selector(snapshot) is { } value && (minimum is null || value < minimum))
                {
                    minimum = value;
                }
            }

            if (minimum is { } result)
            {
                yield return new Measurement<double>(result, ClientTags(group.Key));
            }
        }
    }

    private static IEnumerable<Measurement<double>> ObserveBrokerGauge(string clientType, Func<BrokerStatistics, long?> selector)
    {
        foreach (var group in GetSnapshotGroups(clientType))
        {
            long total = 0;
            var hasValue = false;
            foreach (var broker in group.SelectMany(static snapshot => snapshot.Brokers))
            {
                if (selector(broker) is { } value)
                {
                    total += value;
                    hasValue = true;
                }
            }

            if (hasValue)
            {
                yield return new Measurement<double>(total, ClientTags(group.Key));
            }
        }
    }

    private static IEnumerable<Measurement<double>> ObserveWindowAverage(string clientType, Func<BrokerStatistics, WindowStatistics?> selector)
    {
        foreach (var group in GetSnapshotGroups(clientType))
        {
            double weightedTotal = 0;
            long totalCount = 0;
            double unweightedTotal = 0;
            var valueCount = 0;
            foreach (var broker in group.SelectMany(static snapshot => snapshot.Brokers))
            {
                var window = selector(broker);
                if (window?.Average is not { } average)
                {
                    continue;
                }

                unweightedTotal += average;
                valueCount++;
                if (window.Count is > 0)
                {
                    weightedTotal += average * window.Count.Value;
                    totalCount += window.Count.Value;
                }
            }

            if (valueCount > 0)
            {
                var value = totalCount > 0 ? weightedTotal / totalCount : unweightedTotal / valueCount;
                yield return new Measurement<double>(value, ClientTags(group.Key));
            }
        }
    }

    private static IEnumerable<Measurement<double>> ObserveWindowMaximum(string clientType, Func<BrokerStatistics, WindowStatistics?> selector)
    {
        foreach (var group in GetSnapshotGroups(clientType))
        {
            double? maximum = null;
            foreach (var broker in group.SelectMany(static snapshot => snapshot.Brokers))
            {
                if (selector(broker)?.Maximum is { } value && (maximum is null || value > maximum))
                {
                    maximum = value;
                }
            }

            if (maximum is { } result)
            {
                yield return new Measurement<double>(result, ClientTags(group.Key));
            }
        }
    }

    private static IEnumerable<Measurement<double>> ObserveNodeWindowAverage(string clientType, Func<BrokerStatistics, WindowStatistics?> selector)
    {
        foreach (var group in GetSnapshotGroups(clientType))
        {
            foreach (var nodeId in group.SelectMany(static snapshot => snapshot.Brokers).Select(static broker => broker.NodeId).Distinct())
            {
                double weightedTotal = 0;
                long totalCount = 0;
                double unweightedTotal = 0;
                var valueCount = 0;
                foreach (var broker in group.SelectMany(static snapshot => snapshot.Brokers).Where(broker => broker.NodeId == nodeId))
                {
                    var window = selector(broker);
                    if (window?.Average is not { } average)
                    {
                        continue;
                    }

                    unweightedTotal += average;
                    valueCount++;
                    if (window.Count is > 0)
                    {
                        weightedTotal += average * window.Count.Value;
                        totalCount += window.Count.Value;
                    }
                }

                if (valueCount > 0)
                {
                    var value = totalCount > 0 ? weightedTotal / totalCount : unweightedTotal / valueCount;
                    yield return new Measurement<double>(value, NodeTags(group.Key, nodeId));
                }
            }
        }
    }

    private static IEnumerable<Measurement<double>> ObserveNodeWindowMaximum(string clientType, Func<BrokerStatistics, WindowStatistics?> selector)
    {
        foreach (var group in GetSnapshotGroups(clientType))
        {
            foreach (var nodeId in group.SelectMany(static snapshot => snapshot.Brokers).Select(static broker => broker.NodeId).Distinct())
            {
                var maximum = group
                    .SelectMany(static snapshot => snapshot.Brokers)
                    .Where(broker => broker.NodeId == nodeId)
                    .Select(broker => selector(broker)?.Maximum)
                    .Where(static value => value.HasValue)
                    .Max();
                if (maximum is { } value)
                {
                    yield return new Measurement<double>(value, NodeTags(group.Key, nodeId));
                }
            }
        }
    }

    private static IEnumerable<Measurement<double>> ObserveConnectionCount(string clientType)
    {
        foreach (var group in GetSnapshotGroups(clientType))
        {
            var brokers = group.SelectMany(static snapshot => snapshot.Brokers).ToList();
            if (brokers.Any(static broker => broker.State != null))
            {
                var count = brokers.Count(static broker => string.Equals(broker.State, "UP", StringComparison.Ordinal));
                yield return new Measurement<double>(count, ClientTags(group.Key));
            }
        }
    }

    private static IEnumerable<Measurement<double>> ObservePartitionLag()
    {
        var snapshots = Entries.Values
            .Where(static entry => entry.EnablePartitionMetrics)
            .Select(static entry => entry.Snapshot)
            .Where(static snapshot => snapshot?.ClientId != null && string.Equals(snapshot.ClientType, Consumer, StringComparison.Ordinal))
            .Cast<KafkaStatisticsSnapshot>();
        foreach (var group in snapshots.GroupBy(static snapshot => snapshot.ClientId!, StringComparer.Ordinal))
        {
            foreach (var topicName in group.SelectMany(static snapshot => snapshot.Topics.Keys).Distinct(StringComparer.Ordinal))
            {
                var partitions = group
                    .Where(snapshot => snapshot.Topics.ContainsKey(topicName))
                    .SelectMany(snapshot => snapshot.Topics[topicName])
                    .Where(static partition => partition.Desired != false && partition.ConsumerLag.HasValue);
                foreach (var partitionGroup in partitions.GroupBy(static partition => partition.Partition))
                {
                    var value = partitionGroup.Max(static partition => partition.ConsumerLag!.Value);
                    yield return new Measurement<double>(value, new TagList
                    {
                        { ClientIdTag, group.Key },
                        { TopicTag, topicName },
                        { PartitionTag, partitionGroup.Key.ToString(CultureInfo.InvariantCulture) },
                    });
                }
            }
        }
    }

    private static IEnumerable<KafkaStatisticsSnapshot> GetSnapshots(string clientType)
    {
        foreach (var entry in Entries.Values)
        {
            var snapshot = entry.Snapshot;
            if (snapshot?.ClientId != null
                && string.Equals(snapshot.ClientType, clientType, StringComparison.Ordinal))
            {
                yield return snapshot;
            }
        }
    }

    private static IEnumerable<IGrouping<string, KafkaStatisticsSnapshot>> GetSnapshotGroups(string clientType)
        => GetSnapshots(clientType).GroupBy(static snapshot => snapshot.ClientId!, StringComparer.Ordinal);

    private static long? AvailableBufferBytes(KafkaStatisticsSnapshot snapshot)
        => snapshot.MessageSizeMaximum is { } maximum && snapshot.MessageSize is { } size
            ? Math.Max(0, maximum - size)
            : null;

    private static TagList NodeTags(string? clientId, int nodeId)
        => new()
        {
            { ClientIdTag, clientId },
            { NodeIdTag, nodeId.ToString(CultureInfo.InvariantCulture) },
        };

    private static TagList ClientTags(string? clientId)
        => new()
        {
            { ClientIdTag, clientId },
        };

    private readonly struct CounterSeriesKey : IEquatable<CounterSeriesKey>
    {
        public CounterSeriesKey(CounterMetric metric, string clientId, string? dimensionName = null, string? dimensionValue = null)
        {
            this.Metric = metric;
            this.ClientId = clientId;
            this.DimensionName = dimensionName;
            this.DimensionValue = dimensionValue;
        }

        public CounterMetric Metric { get; }

        public string ClientId { get; }

        public string? DimensionName { get; }

        public string? DimensionValue { get; }

        public static bool operator ==(CounterSeriesKey left, CounterSeriesKey right)
            => left.Equals(right);

        public static bool operator !=(CounterSeriesKey left, CounterSeriesKey right)
            => !left.Equals(right);

        public bool Equals(CounterSeriesKey other)
            => this.Metric == other.Metric
                && string.Equals(this.ClientId, other.ClientId, StringComparison.Ordinal)
                && string.Equals(this.DimensionName, other.DimensionName, StringComparison.Ordinal)
                && string.Equals(this.DimensionValue, other.DimensionValue, StringComparison.Ordinal);

        public override bool Equals(object? obj)
            => obj is CounterSeriesKey other && this.Equals(other);

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (int)this.Metric;
                hashCode = (hashCode * 397) ^ StringComparer.Ordinal.GetHashCode(this.ClientId);
                hashCode = (hashCode * 397) ^ (this.DimensionName == null ? 0 : StringComparer.Ordinal.GetHashCode(this.DimensionName));
                hashCode = (hashCode * 397) ^ (this.DimensionValue == null ? 0 : StringComparer.Ordinal.GetHashCode(this.DimensionValue));
                return hashCode;
            }
        }
    }

    private readonly struct SourceCounterKey : IEquatable<SourceCounterKey>
    {
        public SourceCounterKey(CounterSeriesKey series, string source)
        {
            this.Series = series;
            this.Source = source;
        }

        public CounterSeriesKey Series { get; }

        public string Source { get; }

        public static bool operator ==(SourceCounterKey left, SourceCounterKey right)
            => left.Equals(right);

        public static bool operator !=(SourceCounterKey left, SourceCounterKey right)
            => !left.Equals(right);

        public bool Equals(SourceCounterKey other)
            => this.Series == other.Series && string.Equals(this.Source, other.Source, StringComparison.Ordinal);

        public override bool Equals(object? obj)
            => obj is SourceCounterKey other && this.Equals(other);

        public override int GetHashCode()
        {
            unchecked
            {
                return (this.Series.GetHashCode() * 397) ^ StringComparer.Ordinal.GetHashCode(this.Source);
            }
        }
    }

    private sealed class Entry
    {
        private KafkaStatisticsSnapshot? snapshot;

        public Entry(bool enablePartitionMetrics)
        {
            this.EnablePartitionMetrics = enablePartitionMetrics;
        }

        public bool EnablePartitionMetrics { get; }

        public Dictionary<SourceCounterKey, long> CounterValues { get; } = new();

        public HashSet<SourceCounterKey> ActiveCounters { get; } = new();

        public Dictionary<SourceCounterKey, int> MissingCounterIntervals { get; } = new();

        public Dictionary<SourceCounterKey, long> ExpiredCounterValues { get; } = new();

        public KafkaStatisticsSnapshot? Snapshot
        {
            get => Volatile.Read(ref this.snapshot);
            set => Volatile.Write(ref this.snapshot, value);
        }
    }
}
