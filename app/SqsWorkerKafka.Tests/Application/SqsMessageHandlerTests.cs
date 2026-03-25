using Amazon.Runtime;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using SqsWorkerKafka.Application;
using SqsWorkerKafka.Contracts;
using SqsWorkerKafka.Models;
using SqsWorkerKafka.Tests.TestSupport;

namespace SqsWorkerKafka.Tests.Application;

public sealed class SqsMessageHandlerTests
{
    [Fact]
    public async Task HandleAsync_ValidPayload_ShouldQueryRepositoryAndPublishOrderedPayload()
    {
        var repository = new Mock<IDynamoConversationRepository>(MockBehavior.Strict);
        var producer = new Mock<IKafkaProducer>(MockBehavior.Strict);
        var options = AppOptionsFixture.CreateOptions();
        var sqsBody = """
                      {
                        "partitionKey": "user#42",
                        "id": "conversation-abc123",
                        "timestamp": 1710000002000
                      }
                      """;

        KafkaConversationPayload? publishedPayload = null;
        CancellationToken publishedToken = default;

        repository
            .Setup(x => x.GetMessagesAsync(
                "user#42",
                "conversation-abc123",
                1709956802000,
                1710000002000,
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new[]
            {
                new ConversationMessage("Olá, preciso de ajuda com minha cobrança.", "customer", 1710000000000),
                new ConversationMessage("Claro, vou verificar seu histórico agora.", "assistant", 1710000001000)
            });

        producer
            .Setup(x => x.PublishAsync(It.IsAny<KafkaConversationPayload>(), It.IsAny<CancellationToken>()))
            .Callback<KafkaConversationPayload, CancellationToken>((payload, token) =>
            {
                publishedPayload = payload;
                publishedToken = token;
            })
            .Returns(Task.CompletedTask);

        var sut = new SqsMessageHandler(repository.Object, producer.Object, options, NullLogger<SqsMessageHandler>.Instance);

        await sut.HandleAsync(sqsBody, CancellationToken.None);

        repository.VerifyAll();
        producer.Verify(x => x.PublishAsync(It.IsAny<KafkaConversationPayload>(), It.IsAny<CancellationToken>()), Times.Once);

        publishedPayload.Should().NotBeNull();
        publishedPayload!.Id.Should().Be("conversation-abc123");
        publishedPayload.Messages.Select(x => x.Content).Should().ContainInOrder(
            "Olá, preciso de ajuda com minha cobrança.",
            "Claro, vou verificar seu histórico agora.");
        publishedPayload.Messages.Select(x => x.Role).Should().ContainInOrder("customer", "assistant");
        publishedToken.Should().Be(CancellationToken.None);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task HandleAsync_EmptyBody_ShouldThrow(string? sqsBody)
    {
        var sut = new SqsMessageHandler(
            Mock.Of<IDynamoConversationRepository>(),
            Mock.Of<IKafkaProducer>(),
            AppOptionsFixture.CreateOptions(),
            NullLogger<SqsMessageHandler>.Instance);

        var act = () => sut.HandleAsync(sqsBody!, CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("SQS body is required.");
    }

    [Fact]
    public async Task HandleAsync_InvalidJson_ShouldThrow()
    {
        var sut = new SqsMessageHandler(
            Mock.Of<IDynamoConversationRepository>(),
            Mock.Of<IKafkaProducer>(),
            AppOptionsFixture.CreateOptions(),
            NullLogger<SqsMessageHandler>.Instance);

        var act = () => sut.HandleAsync("{invalid-json", CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("Invalid SQS body.");
    }

    [Fact]
    public async Task HandleAsync_MissingPartitionKey_ShouldThrow()
    {
        var sut = new SqsMessageHandler(
            Mock.Of<IDynamoConversationRepository>(),
            Mock.Of<IKafkaProducer>(),
            AppOptionsFixture.CreateOptions(),
            NullLogger<SqsMessageHandler>.Instance);

        var act = () => sut.HandleAsync("""{"partitionKey":"","id":"conversation-1","timestamp":1710000002000}""", CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("partitionKey is required.");
    }

    [Fact]
    public async Task HandleAsync_MissingId_ShouldThrow()
    {
        var sut = new SqsMessageHandler(
            Mock.Of<IDynamoConversationRepository>(),
            Mock.Of<IKafkaProducer>(),
            AppOptionsFixture.CreateOptions(),
            NullLogger<SqsMessageHandler>.Instance);

        var act = () => sut.HandleAsync("""{"partitionKey":"user#42","id":"","timestamp":1710000002000}""", CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("id is required.");
    }

    [Fact]
    public async Task HandleAsync_InvalidTimestamp_ShouldThrow()
    {
        var sut = new SqsMessageHandler(
            Mock.Of<IDynamoConversationRepository>(),
            Mock.Of<IKafkaProducer>(),
            AppOptionsFixture.CreateOptions(),
            NullLogger<SqsMessageHandler>.Instance);

        var act = () => sut.HandleAsync("""{"partitionKey":"user#42","id":"conversation-1","timestamp":0}""", CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("timestamp must be greater than zero.");
    }

    [Fact]
    public async Task HandleAsync_DynamoThrows_ShouldPropagateAndNotPublish()
    {
        var repository = new Mock<IDynamoConversationRepository>(MockBehavior.Strict);
        var producer = new Mock<IKafkaProducer>(MockBehavior.Strict);

        repository
            .Setup(x => x.GetMessagesAsync("user#42", "conversation-abc123", 1709956802000, 1710000002000, It.IsAny<CancellationToken>()))
            .ThrowsAsync(new TimeoutException("Dynamo timeout"));

        var sut = new SqsMessageHandler(repository.Object, producer.Object, AppOptionsFixture.CreateOptions(maxRetryAttempts: 0), NullLogger<SqsMessageHandler>.Instance);

        var act = () => sut.HandleAsync("""{"partitionKey":"user#42","id":"conversation-abc123","timestamp":1710000002000}""", CancellationToken.None);

        await act.Should().ThrowAsync<TimeoutException>()
            .WithMessage("Dynamo timeout");

        producer.Verify(x => x.PublishAsync(It.IsAny<KafkaConversationPayload>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task HandleAsync_KafkaThrows_ShouldPropagate()
    {
        var repository = new Mock<IDynamoConversationRepository>(MockBehavior.Strict);
        var producer = new Mock<IKafkaProducer>(MockBehavior.Strict);

        repository
            .Setup(x => x.GetMessagesAsync("user#42", "conversation-abc123", 1709956802000, 1710000002000, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new[]
            {
                new ConversationMessage("Cobrança duplicada", "customer", 1710000000000)
            });

        producer
            .Setup(x => x.PublishAsync(It.IsAny<KafkaConversationPayload>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Kafka delivery failed"));

        var sut = new SqsMessageHandler(repository.Object, producer.Object, AppOptionsFixture.CreateOptions(maxRetryAttempts: 0), NullLogger<SqsMessageHandler>.Instance);

        var act = () => sut.HandleAsync("""{"partitionKey":"user#42","id":"conversation-abc123","timestamp":1710000002000}""", CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("Kafka delivery failed");
    }

    [Fact]
    public async Task HandleAsync_EmptyMessageList_ShouldNotPublishToKafka()
    {
        var repository = new Mock<IDynamoConversationRepository>(MockBehavior.Strict);
        var producer = new Mock<IKafkaProducer>(MockBehavior.Strict);

        repository
            .Setup(x => x.GetMessagesAsync("user#42", "conversation-abc123", 1709956802000, 1710000002000, It.IsAny<CancellationToken>()))
            .ReturnsAsync(Array.Empty<ConversationMessage>());

        var sut = new SqsMessageHandler(repository.Object, producer.Object, AppOptionsFixture.CreateOptions(), NullLogger<SqsMessageHandler>.Instance);

        await sut.HandleAsync("""{"partitionKey":"user#42","id":"conversation-abc123","timestamp":1710000002000}""", CancellationToken.None);

        producer.Verify(x => x.PublishAsync(It.IsAny<KafkaConversationPayload>(), It.IsAny<CancellationToken>()), Times.Never);
    }
}
