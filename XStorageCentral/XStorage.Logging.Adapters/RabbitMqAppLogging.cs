using System.Text;
using Polly;
using Polly.Retry;
using RabbitMQ.Client;

namespace XStorage.Logging.Adapters;

public class RabbitMqAppLogging(IConnectionFactory connectionFactory, string exchangeName) : AppLogging
{
    private readonly string _exchangeName = exchangeName;
    private IConnection? _connection;
    private IChannel? _channel;
    private readonly SemaphoreSlim _semaphore = new(1, 1);

    private readonly ResiliencePipeline _resiliencePipeline = new ResiliencePipelineBuilder()
        .AddRetry(new RetryStrategyOptions
        {
            ShouldHandle = new PredicateBuilder().Handle<Exception>(),
            BackoffType = DelayBackoffType.Exponential,
            MaxRetryAttempts = 3,
            UseJitter = true
        })
        .Build();
    
    protected override void Flush(List<string> logs)
    {
        if (logs.Count == 0) return;

        _resiliencePipeline.ExecuteAsync(async (state, ct) =>
        {
            var channel = await state.This.GetChannelAsync();
            
            foreach (var log in state.Logs)
            {
                var body = Encoding.UTF8.GetBytes(log);
                await channel.BasicPublishAsync(
                    exchange: state.This._exchangeName,
                    routingKey: string.Empty,
                    mandatory: false,
                    body: body,
                    cancellationToken: ct);
            }
        }, (This: this, Logs: logs)).ConfigureAwait(false).GetAwaiter().GetResult();
    }

    private async Task<IChannel> GetChannelAsync()
    {
        if (_channel is { IsOpen: true }) return _channel;

        // Since we are called from within ProcessQueue (background task)
        // and GetChannelAsync is called within the resilience pipeline,
        // we should handle concurrency if multiple tasks could call FlushAsync.
        // However, LogBuffer.ProcessQueue is a single background task processing the queue.
        // So a simple lock (or SemaphoreSlim for async) is good.
        // Using a lock with async inside is problematic, so let's use SemaphoreSlim.
        
        await _semaphore.WaitAsync();
        try
        {
            if (_channel is { IsOpen: true }) return _channel;

            if (_channel != null) await _channel.DisposeAsync();
            if (_connection != null) await _connection.DisposeAsync();

            _connection = await connectionFactory.CreateConnectionAsync();
            _channel = await _connection.CreateChannelAsync();
            return _channel;
        }
        finally
        {
            _semaphore.Release();
        }
    }

    public override void Dispose()
    {
        base.Dispose();
        _semaphore.Wait();
        try
        {
            _channel?.Dispose();
            _connection?.Dispose();
        }
        finally
        {
            _semaphore.Release();
        }
        _semaphore.Dispose();
    }
}