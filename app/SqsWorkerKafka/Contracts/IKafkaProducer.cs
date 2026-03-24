using SqsWorkerKafka.Models;

namespace SqsWorkerKafka.Contracts;

public interface IKafkaProducer
{
    Task PublishAsync(KafkaConversationPayload payload, CancellationToken cancellationToken);
}