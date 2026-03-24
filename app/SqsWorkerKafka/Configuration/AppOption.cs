// =======================
// Configuration/AppOptions.cs
// =======================
namespace SqsWorkerKafka.Configuration;

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

    public int ChannelCapacity { get; set; } = 200;
    public int ConsumerCount { get; set; } = Math.Max(2, Environment.ProcessorCount);

    public int MaxConcurrentHandlers { get; set; } = 32;

    public int MaxRetryAttempts { get; set; } = 3;
    public int BaseRetryDelayMs { get; set; } = 200;

    public TimeSpan DynamoLookbackWindow { get; set; } = TimeSpan.FromHours(12);
}