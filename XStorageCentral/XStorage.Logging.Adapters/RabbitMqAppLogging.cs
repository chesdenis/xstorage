using System.Net.Sockets;
using System.Text;
using Polly;
using Polly.Retry;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using XStorage.Common;

namespace XStorage.Logging.Adapters;



public class RabbitMqAppLogging : AppLogging
{
    private readonly IMessagePublisher _publisher;
    private readonly string _exchangeName;
    private readonly IConnectionFactory _connectionFactory;
    private IConnection? _connection;
    private IChannel? _channel;
    private readonly SemaphoreSlim _semaphore = new(1, 1);

    public RabbitMqAppLogging(IMessagePublisher publisher)
    {
        _publisher = publisher;
        _exchangeName = "RABBITMQ_EXCHANGE".ResolveFromEnv();
        var hostName = "RABBITMQ_HOST".ResolveFromEnv();
        var userName = "RABBITMQ_USER".ResolveFromEnv();
        var password = "RABBITMQ_PASS".ResolveFromEnv();
        var portStr = "RABBITMQ_PORT".ResolveFromEnv();
        var virtualHost = "RABBITMQ_VHOST".ResolveFromEnv();

        var factory = new ConnectionFactory
        {
            HostName = hostName,
            UserName = userName,
            Password = password,
            VirtualHost = virtualHost
        };

        if (int.TryParse(portStr, out var port))
        {
            factory.Port = port;
        }

        _connectionFactory = factory;
    }

    private readonly ResiliencePipeline _resiliencePipeline = new ResiliencePipelineBuilder()
        .AddRetry(new RetryStrategyOptions
        {
            ShouldHandle = new PredicateBuilder().Handle<Exception>(),
            BackoffType = DelayBackoffType.Exponential,
            MaxRetryAttempts = 3,
            UseJitter = true
        })
        .Build();
    
    protected override void Flush(List<LogMessage> logs)
    {
        if (logs.Count == 0) return;

        var processedIndex = 0;
        var logsSnapshot = logs.ToArray();
        
        // we use sync because these are just logs ingestion.
        _resiliencePipeline.ExecuteAsync(async (state, ct) =>
        {
            while (processedIndex < logsSnapshot.Length)
            {
                var message = logsSnapshot[processedIndex].Message;
                var type = logsSnapshot[processedIndex].Type;
                
                var body = Encoding.UTF8.GetBytes(message);
                
                await _publisher.PublishAsync(
                    topic: state.This._exchangeName,
                    routingKey: type,
                    body: body,
                    CancellationToken.None);
                
                processedIndex++;
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

            _connection = await _connectionFactory.CreateConnectionAsync();
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