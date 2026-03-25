using Amazon.SQS;
using Amazon.SQS.Model;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using SqsWorkerKafka.Contracts;
using SqsWorkerKafka.Tests.TestSupport;
using SqsWorkerKafka.Workers;

namespace SqsWorkerKafka.Tests.Workers;

public sealed class SqsWorkerTests
{
    [Fact]
    public async Task StartAsync_MessageProcessedSuccessfully_ShouldDeleteMessage()
    {
        var sqs = new Mock<IAmazonSQS>(MockBehavior.Strict);
        var handler = new Mock<ISqsMessageHandler>(MockBehavior.Strict);
        var deleteCalled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var receiveCount = 0;

        sqs.Setup(x => x.ReceiveMessageAsync(It.IsAny<ReceiveMessageRequest>(), It.IsAny<CancellationToken>()))
            .Returns<ReceiveMessageRequest, CancellationToken>(async (_, token) =>
            {
                if (Interlocked.Increment(ref receiveCount) == 1)
                {
                    return new ReceiveMessageResponse
                    {
                        Messages =
                        {
                            new Message
                            {
                                MessageId = "msg-001",
                                ReceiptHandle = "rh-001",
                                Body = """{"partitionKey":"user#42","id":"conversation-abc123","timestamp":1710000002000}"""
                            }
                        }
                    };
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                return new ReceiveMessageResponse();
            });

        handler
            .Setup(x => x.HandleAsync("""{"partitionKey":"user#42","id":"conversation-abc123","timestamp":1710000002000}""", It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        sqs.Setup(x => x.DeleteMessageAsync(
                It.Is<DeleteMessageRequest>(r => r.QueueUrl == "http://localhost:4566/000000000000/my-queue" && r.ReceiptHandle == "rh-001"),
                It.IsAny<CancellationToken>()))
            .Callback(() => deleteCalled.TrySetResult())
            .ReturnsAsync(new DeleteMessageResponse());

        sqs.Setup(x => x.ChangeMessageVisibilityAsync(It.IsAny<ChangeMessageVisibilityRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ChangeMessageVisibilityResponse());

        var sut = new SqsWorker(sqs.Object, AppOptionsFixture.CreateOptions(), handler.Object, NullLogger<SqsWorker>.Instance);

        await sut.StartAsync(CancellationToken.None);
        await deleteCalled.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await sut.StopAsync(CancellationToken.None);

        handler.VerifyAll();
        sqs.Verify(x => x.DeleteMessageAsync(
            It.Is<DeleteMessageRequest>(r => r.QueueUrl == "http://localhost:4566/000000000000/my-queue" && r.ReceiptHandle == "rh-001"),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task StartAsync_HandlerThrows_ShouldNotDeleteMessage()
    {
        var sqs = new Mock<IAmazonSQS>(MockBehavior.Strict);
        var handler = new Mock<ISqsMessageHandler>(MockBehavior.Strict);
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var receiveCount = 0;

        sqs.Setup(x => x.ReceiveMessageAsync(It.IsAny<ReceiveMessageRequest>(), It.IsAny<CancellationToken>()))
            .Returns<ReceiveMessageRequest, CancellationToken>(async (_, token) =>
            {
                if (Interlocked.Increment(ref receiveCount) == 1)
                {
                    return new ReceiveMessageResponse
                    {
                        Messages =
                        {
                            new Message
                            {
                                MessageId = "msg-002",
                                ReceiptHandle = "rh-002",
                                Body = """{"partitionKey":"user#77","id":"conversation-failed","timestamp":1710000100000}"""
                            }
                        }
                    };
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                return new ReceiveMessageResponse();
            });

        handler
            .Setup(x => x.HandleAsync("""{"partitionKey":"user#77","id":"conversation-failed","timestamp":1710000100000}""", It.IsAny<CancellationToken>()))
            .Callback(() => handled.TrySetResult())
            .ThrowsAsync(new InvalidOperationException("handler failure"));

        sqs.Setup(x => x.ChangeMessageVisibilityAsync(It.IsAny<ChangeMessageVisibilityRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ChangeMessageVisibilityResponse());

        var sut = new SqsWorker(sqs.Object, AppOptionsFixture.CreateOptions(), handler.Object, NullLogger<SqsWorker>.Instance);

        await sut.StartAsync(CancellationToken.None);
        await handled.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await sut.StopAsync(CancellationToken.None);

        sqs.Verify(x => x.DeleteMessageAsync(It.IsAny<DeleteMessageRequest>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task StartAsync_ShouldRespectConfiguredConcurrency()
    {
        var sqs = new Mock<IAmazonSQS>(MockBehavior.Strict);
        var handler = new Mock<ISqsMessageHandler>(MockBehavior.Strict);
        var receiveCount = 0;
        var releaseHandlers = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var reachedConcurrency = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var processedCount = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var activeHandlers = 0;
        var completedHandlers = 0;
        var peakConcurrency = 0;

        sqs.Setup(x => x.ReceiveMessageAsync(It.IsAny<ReceiveMessageRequest>(), It.IsAny<CancellationToken>()))
            .Returns<ReceiveMessageRequest, CancellationToken>(async (_, token) =>
            {
                if (Interlocked.Increment(ref receiveCount) == 1)
                {
                    return new ReceiveMessageResponse
                    {
                        Messages =
                        {
                            new Message { MessageId = "msg-101", ReceiptHandle = "rh-101", Body = "body-101" },
                            new Message { MessageId = "msg-102", ReceiptHandle = "rh-102", Body = "body-102" },
                            new Message { MessageId = "msg-103", ReceiptHandle = "rh-103", Body = "body-103" }
                        }
                    };
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                return new ReceiveMessageResponse();
            });

        handler
            .Setup(x => x.HandleAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns<string, CancellationToken>(async (_, token) =>
            {
                var current = Interlocked.Increment(ref activeHandlers);
                peakConcurrency = Math.Max(peakConcurrency, current);

                if (peakConcurrency == 2)
                    reachedConcurrency.TrySetResult();

                await releaseHandlers.Task.WaitAsync(token);

                Interlocked.Decrement(ref activeHandlers);
                if (Interlocked.Increment(ref completedHandlers) == 3)
                    processedCount.TrySetResult();
            });

        sqs.Setup(x => x.DeleteMessageAsync(It.IsAny<DeleteMessageRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeleteMessageResponse());
        sqs.Setup(x => x.ChangeMessageVisibilityAsync(It.IsAny<ChangeMessageVisibilityRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ChangeMessageVisibilityResponse());

        var sut = new SqsWorker(
            sqs.Object,
            AppOptionsFixture.CreateOptions(consumerCount: 4, maxConcurrentHandlers: 2, visibilityTimeoutSeconds: 30),
            handler.Object,
            NullLogger<SqsWorker>.Instance);

        await sut.StartAsync(CancellationToken.None);
        await reachedConcurrency.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await Task.Delay(150);

        peakConcurrency.Should().Be(2);

        releaseHandlers.TrySetResult();
        await processedCount.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await sut.StopAsync(CancellationToken.None);

        handler.Verify(x => x.HandleAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Exactly(3));
        sqs.Verify(x => x.DeleteMessageAsync(It.IsAny<DeleteMessageRequest>(), It.IsAny<CancellationToken>()), Times.Exactly(3));
    }

    [Fact]
    public async Task StopAsync_ShouldCancelLongPollingGracefully()
    {
        var sqs = new Mock<IAmazonSQS>(MockBehavior.Strict);
        var handler = new Mock<ISqsMessageHandler>(MockBehavior.Strict);
        var receiveStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        sqs.Setup(x => x.ReceiveMessageAsync(It.IsAny<ReceiveMessageRequest>(), It.IsAny<CancellationToken>()))
            .Returns<ReceiveMessageRequest, CancellationToken>(async (_, token) =>
            {
                receiveStarted.TrySetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                return new ReceiveMessageResponse();
            });

        var sut = new SqsWorker(sqs.Object, AppOptionsFixture.CreateOptions(), handler.Object, NullLogger<SqsWorker>.Instance);

        await sut.StartAsync(CancellationToken.None);
        await receiveStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));

        var act = () => sut.StopAsync(CancellationToken.None);

        await act.Should().CompleteWithinAsync(TimeSpan.FromSeconds(2));
        handler.Verify(x => x.HandleAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
        sqs.Verify(x => x.DeleteMessageAsync(It.IsAny<DeleteMessageRequest>(), It.IsAny<CancellationToken>()), Times.Never);
    }
}
