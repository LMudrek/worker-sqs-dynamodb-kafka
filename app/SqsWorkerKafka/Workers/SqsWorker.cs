using System.Threading.Channels;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SqsWorkerKafka.Configuration;
using SqsWorkerKafka.Contracts;

namespace SqsWorkerKafka.Workers;

public sealed class SqsWorker : BackgroundService
{
    private readonly IAmazonSQS _sqs;
    private readonly ILogger<SqsWorker> _logger;
    private readonly AppOptions _options;
    private readonly ISqsMessageHandler _handler;
    private readonly Channel<SqsReceivedMessage> _channel;
    private readonly int _processorCount;

    public SqsWorker(
        IAmazonSQS sqs,
        IOptions<AppOptions> options,
        ISqsMessageHandler handler,
        ILogger<SqsWorker> logger)
    {
        _sqs = sqs;
        _handler = handler;
        _logger = logger;
        _options = options.Value;
        _processorCount = Math.Min(_options.ConsumerCount, _options.MaxConcurrentHandlers);

        _channel = Channel.CreateBounded<SqsReceivedMessage>(new BoundedChannelOptions(_options.ChannelCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleWriter = true,
            SingleReader = false
        });
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumers = Enumerable.Range(0, _processorCount)
            .Select(_ => ConsumeAsync(stoppingToken))
            .ToArray();

        try
        {
            await PollAsync(stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
        finally
        {
            _channel.Writer.TryComplete();
        }

        await Task.WhenAll(consumers);
    }

    private async Task PollAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            ReceiveMessageResponse response;

            try
            {
                response = await _sqs.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    QueueUrl = _options.SqsQueueUrl,
                    MaxNumberOfMessages = _options.MaxMessagesPerPoll,
                    WaitTimeSeconds = _options.WaitTimeSeconds,
                    VisibilityTimeout = _options.VisibilityTimeoutSeconds
                }, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Polling error");

                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }

                continue;
            }

            foreach (var msg in response.Messages)
            {
                if (string.IsNullOrWhiteSpace(msg.ReceiptHandle) || msg.Body is null)
                {
                    _logger.LogWarning("Skipping malformed SQS message {MessageId}", msg.MessageId);
                    continue;
                }

                await _channel.Writer.WriteAsync(
                    new SqsReceivedMessage(msg.MessageId ?? string.Empty, msg.ReceiptHandle, msg.Body),
                    stoppingToken);
            }
        }
    }

    private async Task ConsumeAsync(CancellationToken stoppingToken)
    {
        await foreach (var message in _channel.Reader.ReadAllAsync(stoppingToken))
            await ProcessAsync(message, stoppingToken);
    }

    private async Task ProcessAsync(SqsReceivedMessage message, CancellationToken ct)
    {
        using var visibilityCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var visibilityTask = ExtendVisibilityAsync(message, visibilityCts.Token);

        try
        {
            await _handler.HandleAsync(message.Body, ct);

            await _sqs.DeleteMessageAsync(new DeleteMessageRequest
            {
                QueueUrl = _options.SqsQueueUrl,
                ReceiptHandle = message.ReceiptHandle
            }, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Processing failed {MessageId}", message.MessageId);
        }
        finally
        {
            visibilityCts.Cancel();

            try
            {
                await visibilityTask;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested || visibilityCts.IsCancellationRequested)
            {
            }
        }
    }

    private async Task ExtendVisibilityAsync(SqsReceivedMessage message, CancellationToken ct)
    {
        var delay = TimeSpan.FromSeconds(Math.Max(1, _options.VisibilityTimeoutSeconds / 2.0));

        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(delay, ct);

                await _sqs.ChangeMessageVisibilityAsync(new ChangeMessageVisibilityRequest
                {
                    QueueUrl = _options.SqsQueueUrl,
                    ReceiptHandle = message.ReceiptHandle,
                    VisibilityTimeout = _options.VisibilityTimeoutSeconds
                }, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Visibility extension failed for {MessageId}", message.MessageId);
            }
        }
    }

    private sealed record SqsReceivedMessage(string MessageId, string ReceiptHandle, string Body);
}
