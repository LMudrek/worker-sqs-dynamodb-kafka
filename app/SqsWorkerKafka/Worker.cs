using System.Globalization;
using System.Text;
using System.Text.Json;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<AppOptions>(builder.Configuration.GetSection(AppOptions.SectionName));

builder.Services.AddSingleton<IAmazonSQS>(_ => new AmazonSQSClient());
builder.Services.AddSingleton<IAmazonDynamoDB>(_ => new AmazonDynamoDBClient());
builder.Services.AddSingleton<IKafkaProducer, KafkaProducer>();
builder.Services.AddSingleton<IDynamoConversationRepository, DynamoConversationRepository>();
builder.Services.AddSingleton<ISqsMessageHandler, SqsMessageHandler>();
builder.Services.AddHostedService<SqsWorker>();

await builder.Build().RunAsync();

public sealed class AppOptions
{
    public const string SectionName = "App";

    public string SqsQueueUrl { get; set; } = string.Empty;
    public string DynamoTableName { get; set; } = string.Empty;
    public string KafkaBootstrapServers { get; set; } = string.Empty;
    public string KafkaTopic { get; set; } = string.Empty;

    public int MaxMessagesPerPoll { get; set; } = 10;
    public int WaitTimeSeconds { get; set; } = 20;
    public int VisibilityTimeoutSeconds { get; set; } = 60;

    public TimeSpan DynamoLookbackWindow { get; set; } = TimeSpan.FromHours(12);
}

public sealed record SqsEnvelope(
    string PartitionKey,
    string Id,
    long Timestamp);

public sealed record ConversationMessage(
    string Content,
    string Role,
    long Timestamp);

public sealed record KafkaConversationPayload(
    string Id,
    IReadOnlyList<KafkaConversationMessage> Messages);

public sealed record KafkaConversationMessage(
    string Content,
    string Role);

public interface ISqsMessageHandler
{
    Task HandleAsync(string sqsBody, CancellationToken cancellationToken);
}

public interface IDynamoConversationRepository
{
    Task<IReadOnlyList<ConversationMessage>> GetMessagesAsync(
        string partitionKey,
        string id,
        long referenceTimestamp,
        CancellationToken cancellationToken);
}

public interface IKafkaProducer
{
    Task PublishAsync(KafkaConversationPayload payload, CancellationToken cancellationToken);
}

public sealed class SqsWorker : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private readonly IAmazonSQS _sqs;
    private readonly ILogger<SqsWorker> _logger;
    private readonly AppOptions _options;
    private readonly ISqsMessageHandler _handler;

    public SqsWorker(
        IAmazonSQS sqs,
        IOptions<AppOptions> options,
        ISqsMessageHandler handler,
        ILogger<SqsWorker> logger)
    {
        _sqs = sqs;
        _handler = handler;
        _logger = logger;
        _options = options.Value;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        ValidateOptions();

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var response = await _sqs.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    QueueUrl = _options.SqsQueueUrl,
                    MaxNumberOfMessages = Math.Clamp(_options.MaxMessagesPerPoll, 1, 10),
                    WaitTimeSeconds = Math.Clamp(_options.WaitTimeSeconds, 0, 20),
                    VisibilityTimeout = Math.Max(0, _options.VisibilityTimeoutSeconds),
                    MessageAttributeNames = new List<string> { "All" },
                    AttributeNames = new List<string> { "All" }
                }, stoppingToken);

                if (response.Messages.Count == 0)
                {
                    continue;
                }

                foreach (var sqsMessage in response.Messages)
                {
                    if (stoppingToken.IsCancellationRequested)
                    {
                        break;
                    }

                    try
                    {
                        await _handler.HandleAsync(sqsMessage.Body, stoppingToken);

                        await _sqs.DeleteMessageAsync(new DeleteMessageRequest
                        {
                            QueueUrl = _options.SqsQueueUrl,
                            ReceiptHandle = sqsMessage.ReceiptHandle
                        }, stoppingToken);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed processing SQS message. MessageId={MessageId}", sqsMessage.MessageId);
                    }
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error in worker loop");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }
    }

    private void ValidateOptions()
    {
        if (string.IsNullOrWhiteSpace(_options.SqsQueueUrl))
            throw new InvalidOperationException("App:SqsQueueUrl is required.");

        if (string.IsNullOrWhiteSpace(_options.DynamoTableName))
            throw new InvalidOperationException("App:DynamoTableName is required.");

        if (string.IsNullOrWhiteSpace(_options.KafkaBootstrapServers))
            throw new InvalidOperationException("App:KafkaBootstrapServers is required.");

        if (string.IsNullOrWhiteSpace(_options.KafkaTopic))
            throw new InvalidOperationException("App:KafkaTopic is required.");
    }
}

public sealed class SqsMessageHandler : ISqsMessageHandler
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private readonly IDynamoConversationRepository _repository;
    private readonly IKafkaProducer _producer;
    private readonly AppOptions _options;
    private readonly ILogger<SqsMessageHandler> _logger;

    public SqsMessageHandler(
        IDynamoConversationRepository repository,
        IKafkaProducer producer,
        IOptions<AppOptions> options,
        ILogger<SqsMessageHandler> logger)
    {
        _repository = repository;
        _producer = producer;
        _logger = logger;
        _options = options.Value;
    }

    public async Task HandleAsync(string sqsBody, CancellationToken cancellationToken)
    {
        var envelope = JsonSerializer.Deserialize<SqsEnvelope>(sqsBody, JsonOptions)
                       ?? throw new InvalidOperationException("Invalid SQS body.");

        ValidateEnvelope(envelope);

        var fromTimestamp = envelope.Timestamp - (long)_options.DynamoLookbackWindow.TotalMilliseconds;
        if (fromTimestamp < 0)
        {
            fromTimestamp = 0;
        }

        var items = await _repository.GetMessagesAsync(
            envelope.PartitionKey,
            envelope.Id,
            envelope.Timestamp,
            cancellationToken);


        _logger.LogInformation(items.ToString());

        var payload = new KafkaConversationPayload(
            envelope.Id,
            items
                .OrderBy(x => x.Timestamp)
                .Select(x => new KafkaConversationMessage(x.Content, x.Role))
                .ToList());

        _logger.LogInformation(payload.ToString());

        await _producer.PublishAsync(payload, cancellationToken);

        _logger.LogInformation(
            "Processed SQS message. PartitionKey={PartitionKey}, Id={Id}, Messages={Count}",
            envelope.PartitionKey,
            envelope.Id,
            payload.Messages.Count);
    }

    private static void ValidateEnvelope(SqsEnvelope envelope)
    {
        if (string.IsNullOrWhiteSpace(envelope.PartitionKey))
            throw new InvalidOperationException("partitionKey is required.");

        if (string.IsNullOrWhiteSpace(envelope.Id))
            throw new InvalidOperationException("id is required.");

        if (envelope.Timestamp <= 0)
            throw new InvalidOperationException("timestamp must be greater than zero.");
    }
}

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

    public DynamoConversationRepository(IAmazonDynamoDB dynamoDb, IOptions<AppOptions> options, ILogger<DynamoConversationRepository> logger)
    {
        _dynamoDb = dynamoDb;
        _options = options.Value;
        _logger = logger;
    }

    public async Task<IReadOnlyList<ConversationMessage>> GetMessagesAsync(
        string partitionKey,
        string id,
        long referenceTimestamp,
        CancellationToken cancellationToken)
    {
        var fromTimestamp = referenceTimestamp - (long)_options.DynamoLookbackWindow.TotalMilliseconds;
        if (fromTimestamp < 0)
        {
            fromTimestamp = 0;
        }

        var fromSortKey = BuildSortKey(fromTimestamp);
        var toSortKey = BuildSortKey(referenceTimestamp);

        _logger.LogInformation("fromSortKey={fromSortKey}, toSortKey={toSortKey}", fromSortKey, toSortKey);

        var result = new List<ConversationMessage>();
        Dictionary<string, AttributeValue>? lastEvaluatedKey = null;

        do
        {
            var request = new QueryRequest
            {
                TableName = _options.DynamoTableName,
                KeyConditionExpression = $"{PartitionKeyAttribute} = :pk AND {SortKeyAttribute} BETWEEN :from AND :to",
                FilterExpression = $"{IdAttribute} = :id",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":pk"] = new AttributeValue { S = partitionKey },
                    [":from"] = new AttributeValue { S = fromSortKey },
                    [":to"] = new AttributeValue { S = toSortKey },
                    [":id"] = new AttributeValue { S = id }
                },
                ExclusiveStartKey = lastEvaluatedKey,
                ConsistentRead = false,
                ScanIndexForward = true
            };


            var response = await _dynamoDb.QueryAsync(request, cancellationToken);

            foreach (var item in response.Items)
            {
                result.Add(Map(item));
            }

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

        if (!string.IsNullOrWhiteSpace(value.N) && long.TryParse(value.N, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
            return parsed;

        if (!string.IsNullOrWhiteSpace(value.S) && long.TryParse(value.S, NumberStyles.Integer, CultureInfo.InvariantCulture, out parsed))
            return parsed;

        throw new InvalidOperationException($"Invalid numeric DynamoDB attribute: {attributeName}");
    }

    private static string BuildSortKey(long timestamp)
    {
        return $"{SortKeyPrefix}{timestamp.ToString("D13", CultureInfo.InvariantCulture)}";
    }
}

public sealed class KafkaProducer : IKafkaProducer, IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private readonly IProducer<string, byte[]> _producer;
    private readonly AppOptions _options;

    private readonly ILogger<KafkaProducer> _logger;

    public KafkaProducer(IOptions<AppOptions> options, ILogger<KafkaProducer> logger)
    {
        _logger = logger;
        _options = options.Value;

        var config = new ProducerConfig
        {
            BootstrapServers = _options.KafkaBootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true,
            MessageSendMaxRetries = 5,
            RetryBackoffMs = 250,
            LingerMs = 5,
            CompressionType = CompressionType.Snappy
        };

        //_producer = new ProducerBuilder<string, byte[]>(config).Build();
    }

    public async Task PublishAsync(KafkaConversationPayload payload, CancellationToken cancellationToken)
    {
        var key = payload.Id;
        var value = JsonSerializer.SerializeToUtf8Bytes(payload, JsonOptions);

        

        var options = new JsonSerializerOptions { WriteIndented = true };
        string indentedJsonString = JsonSerializer.Serialize(payload, options);

        _logger.LogInformation("payload={value}", indentedJsonString);
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
    }
}