namespace SqsWorkerKafka.Models;

public sealed record KafkaConversationMessage(
    string Content,
    string Role);

public sealed record KafkaConversationPayload(string Id, IReadOnlyList<KafkaConversationMessage> Messages);
