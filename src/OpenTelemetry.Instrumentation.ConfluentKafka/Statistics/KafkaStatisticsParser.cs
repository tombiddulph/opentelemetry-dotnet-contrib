// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

using System.Globalization;
using System.Text.Json;

namespace OpenTelemetry.Instrumentation.ConfluentKafka;

internal static class KafkaStatisticsParser
{
    private const double MicrosecondsPerMillisecond = 1000;
    private const double MillisecondsPerSecond = 1000;

    public static KafkaStatisticsSnapshot Parse(string json)
    {
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;
        var clientType = GetString(root, "type");

        return new KafkaStatisticsSnapshot
        {
            ClientId = GetString(root, "client_id"),
            ClientType = clientType,
            MessageSize = GetInt64(root, "msg_size"),
            MessageSizeMaximum = GetInt64(root, "msg_size_max"),
            ConsumerGroup = ParseConsumerGroup(root),
            Brokers = ParseBrokers(root),
            Topics = ParseTopics(root),
        };
    }

    private static ConsumerGroupStatistics? ParseConsumerGroup(JsonElement root)
    {
        if (!TryGetObject(root, "cgrp", out var consumerGroup))
        {
            return null;
        }

        return new ConsumerGroupStatistics
        {
            AssignmentSize = GetInt64(consumerGroup, "assignment_size"),
            RebalanceCount = GetInt64(consumerGroup, "rebalance_cnt"),
            RebalanceAgeSeconds = Convert(GetDouble(consumerGroup, "rebalance_age"), MillisecondsPerSecond),
        };
    }

    private static List<BrokerStatistics> ParseBrokers(JsonElement root)
    {
        if (!TryGetObject(root, "brokers", out var brokersElement))
        {
            return [];
        }

        var brokers = new List<BrokerStatistics>();
        foreach (var property in brokersElement.EnumerateObject())
        {
            var broker = property.Value;
            var nodeId = GetInt32(broker, "nodeid");
            if (nodeId is null or < 0)
            {
                continue;
            }

            brokers.Add(new BrokerStatistics
            {
                NodeId = nodeId.Value,
                State = GetString(broker, "state"),
                RequestsInFlight = GetInt64(broker, "waitresp_cnt"),
                RequestsSent = GetInt64(broker, "tx"),
                ResponsesReceived = GetInt64(broker, "rx"),
                Retries = GetInt64(broker, "txretries"),
                IncomingBytes = GetInt64(broker, "rxbytes"),
                OutgoingBytes = GetInt64(broker, "txbytes"),
                InternalLatencyMilliseconds = ParseWindow(broker, "int_latency", MicrosecondsPerMillisecond),
                RequestLatencyMilliseconds = ParseWindow(broker, "rtt", MicrosecondsPerMillisecond),

                // Unlike the other librdkafka window statistics, throttle is already in milliseconds.
                ThrottleMilliseconds = ParseWindow(broker, "throttle", divisor: 1),
            });
        }

        return brokers;
    }

    private static Dictionary<string, IReadOnlyList<PartitionStatistics>> ParseTopics(JsonElement root)
    {
        var topics = new Dictionary<string, IReadOnlyList<PartitionStatistics>>(StringComparer.Ordinal);
        if (!TryGetObject(root, "topics", out var topicsElement))
        {
            return topics;
        }

        foreach (var topicProperty in topicsElement.EnumerateObject())
        {
            if (!TryGetObject(topicProperty.Value, "partitions", out var partitionsElement))
            {
                continue;
            }

            var partitions = new List<PartitionStatistics>();
            foreach (var partitionProperty in partitionsElement.EnumerateObject())
            {
                var partitionElement = partitionProperty.Value;
                var partition = GetInt32(partitionElement, "partition") ?? ParseInt32(partitionProperty.Name);
                if (partition is null or < 0)
                {
                    continue;
                }

                var consumerLag = GetInt64(partitionElement, "consumer_lag_stored");
                partitions.Add(new PartitionStatistics
                {
                    Partition = partition.Value,
                    ConsumerLag = consumerLag == -1 ? null : consumerLag,
                    Desired = GetBoolean(partitionElement, "desired"),
                    ReceivedBytes = GetInt64(partitionElement, "rxbytes"),
                    ReceivedMessages = GetInt64(partitionElement, "rxmsgs"),
                    TransmittedBytes = GetInt64(partitionElement, "txbytes"),
                    TransmittedMessages = GetInt64(partitionElement, "txmsgs"),
                });
            }

            if (partitions.Count > 0)
            {
                topics[topicProperty.Name] = partitions;
            }
        }

        return topics;
    }

    private static WindowStatistics? ParseWindow(JsonElement parent, string propertyName, double divisor)
    {
        if (!TryGetObject(parent, propertyName, out var window))
        {
            return null;
        }

        return new WindowStatistics
        {
            Average = Convert(GetDouble(window, "avg"), divisor),
            Maximum = Convert(GetDouble(window, "max"), divisor),
            Count = GetInt64(window, "cnt"),
        };
    }

    private static bool TryGetObject(JsonElement parent, string propertyName, out JsonElement value)
    {
        value = default;
        return parent.ValueKind == JsonValueKind.Object
            && parent.TryGetProperty(propertyName, out value)
            && value.ValueKind == JsonValueKind.Object;
    }

    private static string? GetString(JsonElement parent, string propertyName)
        => parent.ValueKind == JsonValueKind.Object
            && parent.TryGetProperty(propertyName, out var value)
            && value.ValueKind == JsonValueKind.String
                ? value.GetString()
                : null;

    private static long? GetInt64(JsonElement parent, string propertyName)
        => parent.ValueKind == JsonValueKind.Object
            && parent.TryGetProperty(propertyName, out var value)
            && value.ValueKind == JsonValueKind.Number
            && value.TryGetInt64(out var result)
                ? result
                : null;

    private static double? GetDouble(JsonElement parent, string propertyName)
        => parent.ValueKind == JsonValueKind.Object
            && parent.TryGetProperty(propertyName, out var value)
            && value.ValueKind == JsonValueKind.Number
            && value.TryGetDouble(out var result)
                ? result
                : null;

    private static int? GetInt32(JsonElement parent, string propertyName)
    {
        var value = GetInt64(parent, propertyName);
        return value is >= int.MinValue and <= int.MaxValue ? (int)value.Value : null;
    }

    private static int? ParseInt32(string value)
        => int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var result) ? result : null;

    private static bool? GetBoolean(JsonElement parent, string propertyName)
    {
        if (parent.ValueKind != JsonValueKind.Object || !parent.TryGetProperty(propertyName, out var value))
        {
            return null;
        }

        return value.ValueKind switch
        {
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            _ => null,
        };
    }

    private static double? Convert(double? value, double divisor)
        => value / divisor;
}
