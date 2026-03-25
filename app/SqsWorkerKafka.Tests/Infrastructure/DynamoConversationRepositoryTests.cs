using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using SqsWorkerKafka.Infrastructure;
using SqsWorkerKafka.Tests.TestSupport;

namespace SqsWorkerKafka.Tests.Infrastructure;

public sealed class DynamoConversationRepositoryTests
{
    [Fact]
    public async Task GetMessagesAsync_ShouldQueryDynamoWithExpectedParametersAndPaginate()
    {
        var dynamoDb = new Mock<IAmazonDynamoDB>(MockBehavior.Strict);
        var requests = new List<QueryRequest>();
        var lastEvaluatedKey = new Dictionary<string, AttributeValue>
        {
            ["PartitionKey"] = new() { S = "user#42" },
            ["SortKey"] = new() { S = "MENSAGEM#1710000000000" }
        };

        var callIndex = 0;

        dynamoDb
            .Setup(x => x.QueryAsync(It.IsAny<QueryRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((QueryRequest request, CancellationToken _) =>
            {
                requests.Add(request);

                if (callIndex++ == 0)
                {
                    return new QueryResponse
                    {
                        Items =
                        {
                            new Dictionary<string, AttributeValue>
                            {
                                ["Content"] = new() { S = "Primeira mensagem do cliente" },
                                ["Role"] = new() { S = "customer" },
                                ["Timestamp"] = new() { N = "1710000000000" }
                            }
                        },
                        LastEvaluatedKey = lastEvaluatedKey
                    };
                }

                return new QueryResponse
                {
                    Items =
                    {
                        new Dictionary<string, AttributeValue>
                        {
                            ["Content"] = new() { S = "Resposta do atendente" },
                            ["Role"] = new() { S = "assistant" },
                            ["Timestamp"] = new() { N = "1710000001000" }
                        }
                    }
                };
            });

        var sut = new DynamoConversationRepository(
            dynamoDb.Object,
            AppOptionsFixture.CreateOptions(),
            NullLogger<DynamoConversationRepository>.Instance);

        var result = await sut.GetMessagesAsync("user#42", "conversation-abc123", 1710000000000, 1710000002000, CancellationToken.None);

        result.Select(x => x.Content).Should().ContainInOrder("Primeira mensagem do cliente", "Resposta do atendente");
        requests.Should().HaveCount(2);
        requests[0].TableName.Should().Be("conversation-table");
        requests[0].KeyConditionExpression.Should().Be("PartitionKey = :pk AND SortKey BETWEEN :from AND :to");
        requests[0].FilterExpression.Should().Be("Id = :id");
        requests[0].ExpressionAttributeValues[":pk"].S.Should().Be("user#42");
        requests[0].ExpressionAttributeValues[":id"].S.Should().Be("conversation-abc123");
        requests[0].ExpressionAttributeValues[":from"].S.Should().Be("MENSAGEM#1710000000000");
        requests[0].ExpressionAttributeValues[":to"].S.Should().Be("MENSAGEM#1710000002000");
        requests[0].ConsistentRead.Should().BeTrue();
        requests[1].ExclusiveStartKey.Should().BeSameAs(lastEvaluatedKey);
    }

    [Theory]
    [InlineData(null, "conversation-abc123", 0, 10, "partitionKey")]
    [InlineData("", "conversation-abc123", 0, 10, "partitionKey")]
    [InlineData("user#42", null, 0, 10, "id")]
    [InlineData("user#42", "", 0, 10, "id")]
    public async Task GetMessagesAsync_InvalidTextArguments_ShouldThrow(string? partitionKey, string? id, long fromTimestamp, long toTimestamp, string expectedParam)
    {
        var sut = new DynamoConversationRepository(
            Mock.Of<IAmazonDynamoDB>(),
            AppOptionsFixture.CreateOptions(),
            NullLogger<DynamoConversationRepository>.Instance);

        var act = () => sut.GetMessagesAsync(partitionKey!, id!, fromTimestamp, toTimestamp, CancellationToken.None);

        await act.Should().ThrowAsync<ArgumentException>()
            .Where(x => x.ParamName == expectedParam);
    }

    [Theory]
    [InlineData(-1, 10, "fromTimestamp")]
    [InlineData(0, -10, "toTimestamp")]
    public async Task GetMessagesAsync_NegativeTimestamps_ShouldThrow(long fromTimestamp, long toTimestamp, string expectedParam)
    {
        var sut = new DynamoConversationRepository(
            Mock.Of<IAmazonDynamoDB>(),
            AppOptionsFixture.CreateOptions(),
            NullLogger<DynamoConversationRepository>.Instance);

        var act = () => sut.GetMessagesAsync("user#42", "conversation-abc123", fromTimestamp, toTimestamp, CancellationToken.None);

        await act.Should().ThrowAsync<ArgumentOutOfRangeException>()
            .Where(x => x.ParamName == expectedParam);
    }

    [Fact]
    public async Task GetMessagesAsync_FromGreaterThanTo_ShouldThrow()
    {
        var sut = new DynamoConversationRepository(
            Mock.Of<IAmazonDynamoDB>(),
            AppOptionsFixture.CreateOptions(),
            NullLogger<DynamoConversationRepository>.Instance);

        var act = () => sut.GetMessagesAsync("user#42", "conversation-abc123", 20, 10, CancellationToken.None);

        await act.Should().ThrowAsync<ArgumentException>()
            .WithMessage("fromTimestamp must be less than or equal to toTimestamp.");
    }

    [Fact]
    public async Task GetMessagesAsync_MissingContent_ShouldThrow()
    {
        var dynamoDb = new Mock<IAmazonDynamoDB>(MockBehavior.Strict);
        dynamoDb
            .Setup(x => x.QueryAsync(It.IsAny<QueryRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueryResponse
            {
                Items =
                {
                    new Dictionary<string, AttributeValue>
                    {
                        ["Role"] = new() { S = "assistant" },
                        ["Timestamp"] = new() { N = "1710000001000" }
                    }
                }
            });

        var sut = new DynamoConversationRepository(
            dynamoDb.Object,
            AppOptionsFixture.CreateOptions(),
            NullLogger<DynamoConversationRepository>.Instance);

        var act = () => sut.GetMessagesAsync("user#42", "conversation-abc123", 1710000000000, 1710000002000, CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("Missing or invalid DynamoDB attribute: Content");
    }

    [Fact]
    public async Task GetMessagesAsync_InvalidTimestampAttribute_ShouldThrow()
    {
        var dynamoDb = new Mock<IAmazonDynamoDB>(MockBehavior.Strict);
        dynamoDb
            .Setup(x => x.QueryAsync(It.IsAny<QueryRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueryResponse
            {
                Items =
                {
                    new Dictionary<string, AttributeValue>
                    {
                        ["Content"] = new() { S = "Mensagem" },
                        ["Role"] = new() { S = "assistant" },
                        ["Timestamp"] = new() { S = "not-a-number" }
                    }
                }
            });

        var sut = new DynamoConversationRepository(
            dynamoDb.Object,
            AppOptionsFixture.CreateOptions(),
            NullLogger<DynamoConversationRepository>.Instance);

        var act = () => sut.GetMessagesAsync("user#42", "conversation-abc123", 1710000000000, 1710000002000, CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("Invalid numeric DynamoDB attribute: Timestamp");
    }
}
