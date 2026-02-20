using System.Net.Sockets;
using Polly;
using Polly.Retry;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using XStorage.Common;

namespace XStorage.RabbitMq;

/// <summary>
/// This component can publish with automatic recreation of the connection on failure.
/// Must be used as long running per process singleton
/// </summary>
public sealed class RabbitMqMessagePublisher : IMessagePublisher
{
    private readonly ConnectionFactory _connectionFactory;
    private IConnection? _connection;
    private IChannel? _channel;
    private readonly ResiliencePipeline _publishPipeline;
    
    public RabbitMqMessagePublisher()
    {
        var hostName = EnvVars.RabbitHost.Key.FromEnvAsString();
        var userName = EnvVars.RabbitUser.Key.FromEnvAsString();
        var password = EnvVars.RabbitPwd.Key.FromEnvAsString();
        var portAsInt = EnvVars.RabbitPort.Key.FromEnvAsInt();
        var virtualHost = EnvVars.RabbitVHost.Key.FromEnvAsString();

        var factory = new ConnectionFactory
        {
            HostName = hostName,
            UserName = userName,
            Password = password,
            VirtualHost = virtualHost, 
            Port = portAsInt,
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true
        };
        
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
            try
            {
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
            }
            catch (Exception)
            {
                await EnsureConnectedAsync().ConfigureAwait(false);
                throw;
            }

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