namespace SqsWorkerKafka.Models;

public sealed record ConversationMessage(string Content, string Role, long Timestamp);
