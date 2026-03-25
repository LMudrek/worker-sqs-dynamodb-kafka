using System.Text.Json;
using Confluent.Kafka;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using SqsWorkerKafka.Infrastructure;
using SqsWorkerKafka.Models;
using SqsWorkerKafka.Tests.TestSupport;

namespace SqsWorkerKafka.Tests.Infrastructure;

public sealed class KafkaProducerTests
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    [Fact]
    public async Task PublishAsync_ValidPayload_ShouldSendExpectedTopicKeyAndSerializedPayload()
    {
        var producer = new Mock<IProducer<string, byte[]>>(MockBehavior.Strict);
        string? producedTopic = null;
        Message<string, byte[]>? producedMessage = null;
        CancellationToken producedToken = default;

        producer
            .Setup(x => x.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, byte[]>>(), It.IsAny<CancellationToken>()))
            .Callback<string, Message<string, byte[]>, CancellationToken>((topic, message, token) =>
            {
                producedTopic = topic;
                producedMessage = message;
                producedToken = token;
            })
            .ReturnsAsync(new DeliveryResult<string, byte[]>
            {
                Status = PersistenceStatus.Persisted,
                TopicPartitionOffset = new TopicPartitionOffset("conversations", new Partition(2), new Offset(42))
            });

        producer.Setup(x => x.Flush(It.IsAny<TimeSpan>())).Returns(0);
        producer.Setup(x => x.Dispose());

        var sut = new KafkaProducer(producer.Object, AppOptionsFixture.CreateOptions(), NullLogger<KafkaProducer>.Instance);
        var payload = new KafkaConversationPayload(
            "conversation-abc123",
            new[]
            {
                new KafkaConversationMessage("Primeira mensagem", "customer"),
                new KafkaConversationMessage("Resposta do atendente", "assistant")
            });

        await sut.PublishAsync(payload, CancellationToken.None);

        producedTopic.Should().Be("conversations");
        producedMessage.Should().NotBeNull();
        producedMessage!.Key.Should().Be("conversation-abc123");
        producedToken.Should().Be(CancellationToken.None);

        var deserialized = JsonSerializer.Deserialize<KafkaConversationPayload>(producedMessage.Value!, JsonOptions);
        deserialized.Should().NotBeNull();
        deserialized!.Id.Should().Be("conversation-abc123");
        deserialized.Messages.Select(x => x.Content).Should().ContainInOrder("Primeira mensagem", "Resposta do atendente");
    }

    [Fact]
    public async Task PublishAsync_NullPayload_ShouldThrow()
    {
        var sut = new KafkaProducer(
            new Mock<IProducer<string, byte[]>>(MockBehavior.Strict).Object,
            AppOptionsFixture.CreateOptions(),
            NullLogger<KafkaProducer>.Instance);

        var act = () => sut.PublishAsync(null!, CancellationToken.None);

        await act.Should().ThrowAsync<ArgumentNullException>();
    }

    [Fact]
    public async Task PublishAsync_BlankId_ShouldThrow()
    {
        var sut = new KafkaProducer(
            new Mock<IProducer<string, byte[]>>(MockBehavior.Strict).Object,
            AppOptionsFixture.CreateOptions(),
            NullLogger<KafkaProducer>.Instance);

        var act = () => sut.PublishAsync(
            new KafkaConversationPayload("", new[] { new KafkaConversationMessage("Mensagem", "customer") }),
            CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("payload.Id is required.");
    }

    [Fact]
    public async Task PublishAsync_NotPersisted_ShouldThrow()
    {
        var producer = new Mock<IProducer<string, byte[]>>(MockBehavior.Strict);
        producer
            .Setup(x => x.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, byte[]>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeliveryResult<string, byte[]>
            {
                Status = PersistenceStatus.NotPersisted
            });
        producer.Setup(x => x.Flush(It.IsAny<TimeSpan>())).Returns(0);
        producer.Setup(x => x.Dispose());

        var sut = new KafkaProducer(producer.Object, AppOptionsFixture.CreateOptions(), NullLogger<KafkaProducer>.Instance);

        var act = () => sut.PublishAsync(
            new KafkaConversationPayload("conversation-abc123", new[] { new KafkaConversationMessage("Mensagem", "customer") }),
            CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("Kafka delivery not persisted");
    }

    [Fact]
    public async Task PublishAsync_ProducerThrows_ShouldPropagate()
    {
        var producer = new Mock<IProducer<string, byte[]>>(MockBehavior.Strict);
        producer
            .Setup(x => x.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, byte[]>>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new ProduceException<string, byte[]>(new Error(ErrorCode.Local_Transport, "broker unreachable"), null));
        producer.Setup(x => x.Flush(It.IsAny<TimeSpan>())).Returns(0);
        producer.Setup(x => x.Dispose());

        var sut = new KafkaProducer(producer.Object, AppOptionsFixture.CreateOptions(), NullLogger<KafkaProducer>.Instance);

        var act = () => sut.PublishAsync(
            new KafkaConversationPayload("conversation-abc123", new[] { new KafkaConversationMessage("Mensagem", "customer") }),
            CancellationToken.None);

        await act.Should().ThrowAsync<ProduceException<string, byte[]>>();
    }

    [Fact]
    public void Dispose_ShouldFlushAndDisposeUnderlyingProducer()
    {
        var producer = new Mock<IProducer<string, byte[]>>(MockBehavior.Strict);
        producer.Setup(x => x.Flush(TimeSpan.FromSeconds(10))).Returns(0);
        producer.Setup(x => x.Dispose());

        var sut = new KafkaProducer(producer.Object, AppOptionsFixture.CreateOptions(), NullLogger<KafkaProducer>.Instance);

        sut.Dispose();

        producer.Verify(x => x.Flush(TimeSpan.FromSeconds(10)), Times.Once);
        producer.Verify(x => x.Dispose(), Times.Once);
    }
}
