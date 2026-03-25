using FluentAssertions;
using SqsWorkerKafka.Infrastructure;

namespace SqsWorkerKafka.Tests.Infrastructure;

public sealed class RetryPolicyTests
{
    [Fact]
    public async Task ExecuteAsync_RetryableFailure_ShouldRetryUntilSuccess()
    {
        var attempts = 0;

        await RetryPolicyInfrastructure.ExecuteAsync(
            _ =>
            {
                attempts++;
                if (attempts < 3)
                    throw new TimeoutException("transient");

                return Task.CompletedTask;
            },
            maxRetries: 3,
            baseDelayMs: 1,
            ct: CancellationToken.None,
            shouldRetry: static ex => ex is TimeoutException);

        attempts.Should().Be(3);
    }

    [Fact]
    public async Task ExecuteAsync_NonRetryableFailure_ShouldStopImmediately()
    {
        var attempts = 0;

        var act = () => RetryPolicyInfrastructure.ExecuteAsync(
            _ =>
            {
                attempts++;
                throw new InvalidOperationException("fatal");
            },
            maxRetries: 3,
            baseDelayMs: 1,
            ct: CancellationToken.None,
            shouldRetry: static ex => ex is TimeoutException);

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("fatal");
        attempts.Should().Be(1);
    }

    [Fact]
    public async Task ExecuteAsync_InvalidRetryArguments_ShouldThrow()
    {
        var act = () => RetryPolicyInfrastructure.ExecuteAsync(
            _ => Task.CompletedTask,
            maxRetries: -1,
            baseDelayMs: 1,
            ct: CancellationToken.None);

        await act.Should().ThrowAsync<ArgumentOutOfRangeException>();
    }

    [Fact]
    public async Task ExecuteAsync_GenericOverload_ShouldReturnValue()
    {
        var result = await RetryPolicyInfrastructure.ExecuteAsync(
            _ => Task.FromResult(42),
            maxRetries: 0,
            baseDelayMs: 1,
            ct: CancellationToken.None);

        result.Should().Be(42);
    }
}
