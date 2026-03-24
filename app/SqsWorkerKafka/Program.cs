using Amazon;
using Amazon.DynamoDBv2;
using Amazon.Runtime;
using Amazon.SQS;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SqsWorkerKafka.Application;
using SqsWorkerKafka.Configuration;
using SqsWorkerKafka.Contracts;
using SqsWorkerKafka.Infrastructure;
using SqsWorkerKafka.Workers;

var builder = Host.CreateApplicationBuilder(args);

builder.Services
    .AddOptions<AppOptions>()
    .BindConfiguration(AppOptions.SectionName)
    .Validate(o => !string.IsNullOrWhiteSpace(o.SqsQueueUrl), "App:SqsQueueUrl is required.")
    .Validate(o => !string.IsNullOrWhiteSpace(o.DynamoTableName), "App:DynamoTableName is required.")
    .Validate(o => !string.IsNullOrWhiteSpace(o.KafkaBootstrapServers), "App:KafkaBootstrapServers is required.")
    .Validate(o => !string.IsNullOrWhiteSpace(o.KafkaTopic), "App:KafkaTopic is required.")
    .Validate(o => o.MaxMessagesPerPoll is >= 1 and <= 10, "App:MaxMessagesPerPoll invalid.")
    .Validate(o => o.WaitTimeSeconds is >= 0 and <= 20, "App:WaitTimeSeconds invalid.")
    .Validate(o => o.VisibilityTimeoutSeconds > 0, "App:VisibilityTimeoutSeconds invalid.")
    .Validate(o => o.ChannelCapacity > 0, "App:ChannelCapacity invalid.")
    .Validate(o => o.ConsumerCount > 0, "App:ConsumerCount invalid.")
    .Validate(o => o.MaxConcurrentHandlers > 0, "App:MaxConcurrentHandlers invalid.")
    .Validate(o => o.MaxRetryAttempts >= 0, "App:MaxRetryAttempts invalid.")
    .Validate(o => o.BaseRetryDelayMs > 0, "App:BaseRetryDelayMs invalid.")
    .Validate(o => o.DynamoLookbackWindow > TimeSpan.Zero, "App:DynamoLookbackWindow invalid.")
    .ValidateOnStart();

builder.Services.AddSingleton<IAmazonSQS>(sp =>
{
    var configuration = sp.GetRequiredService<IConfiguration>();
    var regionName = string.IsNullOrWhiteSpace(configuration["AWS_REGION"])
        ? "sa-east-1"
        : configuration["AWS_REGION"]!;
    var endpoint = configuration["AWS_ENDPOINT_URL"];

    var config = new AmazonSQSConfig
    {
        RegionEndpoint = RegionEndpoint.GetBySystemName(regionName)
    };

    if (!string.IsNullOrWhiteSpace(endpoint))
    {
        config.ServiceURL = endpoint;
        return new AmazonSQSClient(new BasicAWSCredentials("test", "test"), config);
    }

    return new AmazonSQSClient(config);
});

builder.Services.AddSingleton<IAmazonDynamoDB>(sp =>
{
    var configuration = sp.GetRequiredService<IConfiguration>();
    var regionName = string.IsNullOrWhiteSpace(configuration["AWS_REGION"])
        ? "us-east-1"
        : configuration["AWS_REGION"]!;
    var endpoint = configuration["AWS_ENDPOINT_URL"];

    var config = new AmazonDynamoDBConfig
    {
        RegionEndpoint = RegionEndpoint.GetBySystemName(regionName)
    };

    if (!string.IsNullOrWhiteSpace(endpoint))
    {
        config.ServiceURL = endpoint;
        return new AmazonDynamoDBClient(new BasicAWSCredentials("test", "test"), config);
    }

    return new AmazonDynamoDBClient(config);
});

builder.Services.AddSingleton<IKafkaProducer, KafkaProducer>();
builder.Services.AddSingleton<IDynamoConversationRepository, DynamoConversationRepository>();
builder.Services.AddSingleton<ISqsMessageHandler, SqsMessageHandler>();

builder.Services.AddHostedService<SqsWorker>();

await builder.Build().RunAsync();
