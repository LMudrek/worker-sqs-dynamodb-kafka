// =======================
// Infrastructure/KafkaProducer.cs
// =======================
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SqsWorkerKafka.Configuration;
using SqsWorkerKafka.Contracts;
using SqsWorkerKafka.Models;

namespace SqsWorkerKafka.Infrastructure;

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
            MessageSendMaxRetries = 10,
            RetryBackoffMs = 250,
            LingerMs = 5,
            CompressionType = CompressionType.Snappy,
            ClientId = "sqs-worker-kafka"
        };

        _producer = new ProducerBuilder<string, byte[]>(config).Build();
    }

    internal KafkaProducer(
        IProducer<string, byte[]> producer,
        IOptions<AppOptions> options,
        ILogger<KafkaProducer> logger)
    {
        _producer = producer;
        _options = options.Value;
        _logger = logger;
    }

    public async Task PublishAsync(KafkaConversationPayload payload, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(payload);

        if (string.IsNullOrWhiteSpace(payload.Id))
            throw new InvalidOperationException("payload.Id is required.");

        if (payload.Messages is null)
            throw new InvalidOperationException("payload.Messages is required.");

        var value = JsonSerializer.SerializeToUtf8Bytes(payload, JsonOptions);

        var delivery = await _producer.ProduceAsync(
            _options.KafkaTopic,
            new Message<string, byte[]>
            {
                Key = payload.Id,
                Value = value
            },
            cancellationToken);

        if (delivery.Status != PersistenceStatus.Persisted)
            throw new InvalidOperationException("Kafka delivery not persisted");

        _logger.LogInformation(
            "Kafka OK Topic={Topic} Partition={Partition} Offset={Offset}",
            delivery.Topic,
            delivery.Partition.Value,
            delivery.Offset.Value);
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
    }
}
