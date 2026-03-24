using System.Text.Json;
using Amazon.Runtime;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SqsWorkerKafka.Configuration;
using SqsWorkerKafka.Contracts;
using SqsWorkerKafka.Infrastructure;
using SqsWorkerKafka.Models;

namespace SqsWorkerKafka.Application;

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
        _options = options.Value;
        _logger = logger;
    }

    public async Task HandleAsync(string sqsBody, CancellationToken cancellationToken)
    {
        SqsEnvelope envelope;

        try
        {
            envelope = JsonSerializer.Deserialize<SqsEnvelope>(sqsBody, JsonOptions)
                ?? throw new InvalidOperationException("Invalid SQS body.");
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException("Invalid SQS body.", ex);
        }

        if (string.IsNullOrWhiteSpace(envelope.PartitionKey))
            throw new InvalidOperationException("partitionKey is required.");

        if (string.IsNullOrWhiteSpace(envelope.Id))
            throw new InvalidOperationException("id is required.");

        if (envelope.Timestamp <= 0)
            throw new InvalidOperationException("timestamp must be greater than zero.");

        var fromTimestamp = Math.Max(0, envelope.Timestamp - (long)_options.DynamoLookbackWindow.TotalMilliseconds);

        var messages = await RetryPolicyInfrastructure.ExecuteAsync(
            token => _repository.GetMessagesAsync(
                envelope.PartitionKey,
                envelope.Id,
                fromTimestamp,
                envelope.Timestamp,
                token),
            _options.MaxRetryAttempts,
            _options.BaseRetryDelayMs,
            cancellationToken,
            static ex => ex is AmazonServiceException or TimeoutException);

        var payload = new KafkaConversationPayload(
            envelope.Id,
            messages.Select(m => new KafkaConversationMessage(m.Content, m.Role)).ToArray());

        _logger.LogInformation(
            "Publishing conversation {ConversationId} with {MessageCount} messages",
            envelope.Id,
            messages.Count);

        await RetryPolicyInfrastructure.ExecuteAsync(
            token => _producer.PublishAsync(payload, token),
            _options.MaxRetryAttempts,
            _options.BaseRetryDelayMs,
            cancellationToken,
            static ex => ex is KafkaException or TimeoutException);
    }
}
