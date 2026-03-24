namespace SqsWorkerKafka.Infrastructure;

public static class RetryPolicyInfrastructure
{
    public static async Task ExecuteAsync(
        Func<CancellationToken, Task> action,
        int maxRetries,
        int baseDelayMs,
        CancellationToken ct,
        Func<Exception, bool>? shouldRetry = null)
    {
        await ExecuteAsync<object?>(
            async token =>
            {
                await action(token);
                return null;
            },
            maxRetries,
            baseDelayMs,
            ct,
            shouldRetry);
    }

    public static async Task<T> ExecuteAsync<T>(
        Func<CancellationToken, Task<T>> action,
        int maxRetries,
        int baseDelayMs,
        CancellationToken ct,
        Func<Exception, bool>? shouldRetry = null)
    {
        if (maxRetries < 0)
            throw new ArgumentOutOfRangeException(nameof(maxRetries));

        if (baseDelayMs <= 0)
            throw new ArgumentOutOfRangeException(nameof(baseDelayMs));

        shouldRetry ??= static ex => ex is not OperationCanceledException;

        for (var attempt = 0; ; attempt++)
        {
            ct.ThrowIfCancellationRequested();

            try
            {
                return await action(ct);
            }
            catch (Exception ex) when (attempt < maxRetries && shouldRetry(ex))
            {
                var delay = TimeSpan.FromMilliseconds(baseDelayMs * Math.Pow(2, attempt));
                await Task.Delay(delay, ct);
            }
        }
    }
}
