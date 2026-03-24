namespace SqsWorkerKafka.Contracts;

public interface ISqsMessageHandler
{
    Task HandleAsync(string sqsBody, CancellationToken cancellationToken);
}