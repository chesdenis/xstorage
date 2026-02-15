namespace XStorage.Common;

public interface IMessagePublisher : IAsyncDisposable
{
    Task PublishAsync(
        string topic,
        string routingKey,
        ReadOnlyMemory<byte> body,
        CancellationToken ct);
}