using System.Net.Sockets;
using Polly;
using Polly.Retry;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using XStorage.Common;

namespace XStorage.RabbitMq;

public sealed class RabbitMqMessagePublisher : IMessagePublisher
{
    private readonly ConnectionFactory _connectionFactory;
    private IConnection? _connection;
    private IChannel? _channel;
    private readonly ResiliencePipeline _publishPipeline;
    
    public RabbitMqMessagePublisher()
    {
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
            VirtualHost = virtualHost,

            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true
        };

        if (int.TryParse(portStr, out var port))
        {
            factory.Port = port;
        }

        _connectionFactory = factory;
        
        _publishPipeline = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = 5,
                Delay = TimeSpan.FromMilliseconds(100),
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = true,
                ShouldHandle = new PredicateBuilder().Handle<Exception>(IsTransient),
                OnRetry = _ =>
                {
                    // Force reconnect on next attempt
                    Invalidate();
                    return default;
                }
            })
            .Build();
    }

    public Task PublishAsync(string topic, string routingKey, ReadOnlyMemory<byte> body, CancellationToken ct)
    {
        return _publishPipeline.ExecuteAsync(async pipelineCt =>
        {
            await EnsureConnectedAsync().ConfigureAwait(false);

            var props = new BasicProperties
            {
                Persistent = true,
                ContentType = "application/octet-stream"
            };

            // Confirms are enabled on the channel, so this await completes only when confirmed.
            await _channel!.BasicPublishAsync(
                exchange: topic,
                routingKey: routingKey,
                mandatory: true,
                basicProperties: props,
                body: body, cancellationToken: pipelineCt).ConfigureAwait(false);

        }, ct).AsTask();
    }
    
    private async Task EnsureConnectedAsync()
    {
        if (_connection is { IsOpen: true } && _channel is { IsOpen: true })
            return;

        Invalidate();

        _connection = await _connectionFactory.CreateConnectionAsync().ConfigureAwait(false);

        var channelOpts = new CreateChannelOptions(
            publisherConfirmationsEnabled: true,
            publisherConfirmationTrackingEnabled: true
        );

        _channel = await _connection.CreateChannelAsync(channelOpts).ConfigureAwait(false);
    }
    
    private void Invalidate()
    {
        try
        {
            _channel?.CloseAsync().ConfigureAwait(false).GetAwaiter().GetResult();
        }
        catch
        {
            // ignored
        }

        try
        {
            _channel?.DisposeAsync().AsTask().ConfigureAwait(false).GetAwaiter().GetResult();
        }
        catch
        {
            // ignored
        }

        _channel = null;

        try
        {
            _connection?.CloseAsync().ConfigureAwait(false).GetAwaiter().GetResult();
        }
        catch
        {
            // ignored
        }

        try
        {
            _connection?.DisposeAsync().AsTask().ConfigureAwait(false).GetAwaiter().GetResult();
        }
        catch
        {
            // ignored
        }

        _connection = null;
    }
    
    private static bool IsTransient(Exception ex) =>
        ex is AlreadyClosedException
        || ex is OperationInterruptedException
        || ex is BrokerUnreachableException
        || ex is SocketException
        || ex is TimeoutException
        || ex is System.IO.IOException;

    public ValueTask DisposeAsync()
    {
        Invalidate();
        return ValueTask.CompletedTask;
    }
}