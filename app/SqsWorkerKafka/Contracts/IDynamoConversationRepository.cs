using SqsWorkerKafka.Models;

namespace SqsWorkerKafka.Contracts;

public interface IDynamoConversationRepository
{
    Task<IReadOnlyList<ConversationMessage>> GetMessagesAsync(
        string partitionKey,
        string id,
        long fromTimestamp,
        long toTimestamp,
        CancellationToken cancellationToken);
}