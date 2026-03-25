using System.Globalization;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SqsWorkerKafka.Configuration;
using SqsWorkerKafka.Contracts;
using SqsWorkerKafka.Models;

namespace SqsWorkerKafka.Infrastructure;

public sealed class DynamoConversationRepository : IDynamoConversationRepository
{
    private const string PartitionKeyAttribute = "PartitionKey";
    private const string SortKeyAttribute = "SortKey";
    private const string IdAttribute = "Id";
    private const string ContentAttribute = "Content";
    private const string RoleAttribute = "Role";
    private const string TimestampAttribute = "Timestamp";
    private const string SortKeyPrefix = "MENSAGEM#";

    private readonly IAmazonDynamoDB _dynamoDb;
    private readonly AppOptions _options;
    private readonly ILogger<DynamoConversationRepository> _logger;

    public DynamoConversationRepository(
        IAmazonDynamoDB dynamoDb,
        IOptions<AppOptions> options,
        ILogger<DynamoConversationRepository> logger)
    {
        _dynamoDb = dynamoDb;
        _options = options.Value;
        _logger = logger;
    }

    public async Task<IReadOnlyList<ConversationMessage>> GetMessagesAsync(
        string partitionKey,
        string id,
        long fromTimestamp,
        long toTimestamp,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(partitionKey))
            throw new ArgumentException("partitionKey is required.", nameof(partitionKey));

        if (string.IsNullOrWhiteSpace(id))
            throw new ArgumentException("id is required.", nameof(id));

        if (fromTimestamp < 0)
            throw new ArgumentOutOfRangeException(nameof(fromTimestamp));

        if (toTimestamp < 0)
            throw new ArgumentOutOfRangeException(nameof(toTimestamp));

        if (fromTimestamp > toTimestamp)
            throw new ArgumentException("fromTimestamp must be less than or equal to toTimestamp.");

        var fromSortKey = BuildSortKey(fromTimestamp);
        var toSortKey = BuildSortKey(toTimestamp);

        var result = new List<ConversationMessage>();
        Dictionary<string, AttributeValue>? lastEvaluatedKey = null;

        do
        {
            var response = await _dynamoDb.QueryAsync(new QueryRequest
            {
                TableName = _options.DynamoTableName,
                KeyConditionExpression = $"{PartitionKeyAttribute} = :pk AND {SortKeyAttribute} BETWEEN :from AND :to",
                FilterExpression = $"{IdAttribute} = :id",
                ProjectionExpression = $"{ContentAttribute}, #{RoleAttribute}, #{TimestampAttribute}",
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    [$"#{RoleAttribute}"] = RoleAttribute,
                    [$"#{TimestampAttribute}"] = TimestampAttribute
                },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":pk"] = new AttributeValue { S = partitionKey },
                    [":from"] = new AttributeValue { S = fromSortKey },
                    [":to"] = new AttributeValue { S = toSortKey },
                    [":id"] = new AttributeValue { S = id }
                },
                ExclusiveStartKey = lastEvaluatedKey,
                ConsistentRead = true,
                ScanIndexForward = true
            }, cancellationToken);

            foreach (var item in response.Items)
                result.Add(Map(item));

            lastEvaluatedKey = response.LastEvaluatedKey;
        }
        while (lastEvaluatedKey is not null && lastEvaluatedKey.Count > 0);

        return result;
    }

    private static ConversationMessage Map(Dictionary<string, AttributeValue> item)
    {
        var content = GetString(item, ContentAttribute);
        var role = GetString(item, RoleAttribute);
        var timestamp = GetLong(item, TimestampAttribute);

        return new ConversationMessage(content, role, timestamp);
    }

    private static string GetString(Dictionary<string, AttributeValue> item, string attributeName)
    {
        if (!item.TryGetValue(attributeName, out var value) || string.IsNullOrWhiteSpace(value.S))
            throw new InvalidOperationException($"Missing or invalid DynamoDB attribute: {attributeName}");

        return value.S;
    }

    private static long GetLong(Dictionary<string, AttributeValue> item, string attributeName)
    {
        if (!item.TryGetValue(attributeName, out var value))
            throw new InvalidOperationException($"Missing DynamoDB attribute: {attributeName}");

        if (!string.IsNullOrWhiteSpace(value.N) &&
            long.TryParse(value.N, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            return parsed;
        }

        if (!string.IsNullOrWhiteSpace(value.S) &&
            long.TryParse(value.S, NumberStyles.Integer, CultureInfo.InvariantCulture, out parsed))
        {
            return parsed;
        }

        throw new InvalidOperationException($"Invalid numeric DynamoDB attribute: {attributeName}");
    }

    private static string BuildSortKey(long timestamp)
    {
        return $"{SortKeyPrefix}{timestamp.ToString("D13", CultureInfo.InvariantCulture)}";
    }
}
