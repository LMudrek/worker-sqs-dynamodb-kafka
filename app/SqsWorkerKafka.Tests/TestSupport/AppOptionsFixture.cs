using Microsoft.Extensions.Options;
using SqsWorkerKafka.Configuration;

namespace SqsWorkerKafka.Tests.TestSupport;

internal static class AppOptionsFixture
{
    public static AppOptions Create(
        int maxRetryAttempts = 0,
        int baseRetryDelayMs = 1,
        int consumerCount = 2,
        int maxConcurrentHandlers = 2,
        int visibilityTimeoutSeconds = 10,
        int channelCapacity = 10)
    {
        return new AppOptions
        {
            SqsQueueUrl = "http://localhost:4566/000000000000/my-queue",
            DynamoTableName = "conversation-table",
            KafkaBootstrapServers = "localhost:9092",
            KafkaTopic = "conversations",
            MaxMessagesPerPoll = 10,
            WaitTimeSeconds = 0,
            VisibilityTimeoutSeconds = visibilityTimeoutSeconds,
            ChannelCapacity = channelCapacity,
            ConsumerCount = consumerCount,
            MaxConcurrentHandlers = maxConcurrentHandlers,
            MaxRetryAttempts = maxRetryAttempts,
            BaseRetryDelayMs = baseRetryDelayMs,
            DynamoLookbackWindow = TimeSpan.FromHours(12)
        };
    }

    public static IOptions<AppOptions> CreateOptions(
        int maxRetryAttempts = 0,
        int baseRetryDelayMs = 1,
        int consumerCount = 2,
        int maxConcurrentHandlers = 2,
        int visibilityTimeoutSeconds = 10,
        int channelCapacity = 10)
    {
        return Options.Create(Create(
            maxRetryAttempts,
            baseRetryDelayMs,
            consumerCount,
            maxConcurrentHandlers,
            visibilityTimeoutSeconds,
            channelCapacity));
    }
}
