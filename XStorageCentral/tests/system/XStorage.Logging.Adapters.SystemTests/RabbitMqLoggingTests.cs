using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using XStorage.RabbitMq;

namespace XStorage.Logging.Adapters.SystemTests;

public sealed class RabbitMqFixture : IAsyncLifetime
{
    public IContainer Container { get; }

    public string Host => Container.Hostname;
    public int AmqpPort => Container.GetMappedPublicPort(5672);
    public string User => "guest";
    public string Pass => "guest";
    public string VHost => "/";
    
    public RabbitMqFixture()
    {
        Container = new ContainerBuilder("rabbitmq:3-management")
            .WithPortBinding(5672, true)     // AMQP
            .WithPortBinding(15672, true)    // management (optional)
            // Wait until AMQP port is available (good enough for most tests)
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilInternalTcpPortIsAvailable(5672))
            .Build();
    }
    public async Task InitializeAsync() => await Container.StartAsync();
    public async Task DisposeAsync() => await Container.StopAsync();
    
    public ConnectionFactory CreateFactory() => new()
    {
        HostName = Host,
        Port = AmqpPort,
        UserName = User,
        Password = Pass,
        VirtualHost = VHost,
        // recommended in tests so you see failures quickly
        AutomaticRecoveryEnabled = false
    };
}

[CollectionDefinition(nameof(RabbitMqCollection))]
public class RabbitMqCollection : ICollectionFixture<RabbitMqFixture>;

[Collection(nameof(RabbitMqCollection))]
public class RabbitMqLoggingTests(RabbitMqFixture fixture)
{
    [Fact]
    public async Task Flush_Publishes_Few_Messages()
    {
        var (queue, ch, cnn) = await SetupTestAssertionEnvironment();

        var publisher = new RabbitMqMessagePublisher();
        
        var sut = new RabbitMqAppLogging(publisher);
        sut.WriteInfo("one", "some object");
        sut.WriteInfo("two", "some other object");
        sut.WriteInfo("three", "yet another object");
        
        var received = Receive(ch, queue, sut);

        Assert.Equal(3, received.Count);
        
        Assert.NotNull(received.FirstOrDefault(f=>f.Contains("one", StringComparison.OrdinalIgnoreCase)));
        Assert.NotNull(received.FirstOrDefault(f=>f.Contains("two", StringComparison.OrdinalIgnoreCase)));
        Assert.NotNull(received.FirstOrDefault(f=>f.Contains("three", StringComparison.OrdinalIgnoreCase)));

        await cnn.DisposeAsync().AsTask();
        await ch.DisposeAsync().AsTask();
    } 
    
    [Fact]
    public async Task Flush_Publishes_Tons_Messages()
    {
        var (queue, ch, cnn) = await SetupTestAssertionEnvironment();
        await using var channel = ch;

        var sut = new RabbitMqAppLogging(new RabbitMqMessagePublisher());
        
        Enumerable.Range(0, 50000).ToList().ForEach(i => sut.WriteInfo($"msg{i}", i));
        
        var received = Receive(ch, queue, sut);

        Assert.Equal(50000, received.Count);

        for (var i = 0; i < 50000; i++)
        {
            Assert.NotNull(received.FirstOrDefault(f=>f.Contains($"msg{i}", StringComparison.OrdinalIgnoreCase)));
        }
        
        await cnn.DisposeAsync().AsTask();
        await ch.DisposeAsync().AsTask();
    }

    private List<string> Receive(IChannel ch, string queue, RabbitMqAppLogging sut)
    {
        var received = new List<string>();
        var consumer = new AsyncEventingBasicConsumer(ch);
        consumer.ReceivedAsync += async (_, ea) =>
        {
            received.Add(Encoding.UTF8.GetString(ea.Body.ToArray()));
            await ch.BasicAckAsync(ea.DeliveryTag, multiple: false);
        };

        ch.BasicConsumeAsync(queue: queue, autoAck: false, consumer: consumer);
        
        // dispose must deliver all messages even if they are in buffer.
        sut.Dispose();

        Task.Delay(TimeSpan.FromSeconds(30)).GetAwaiter().GetResult();
        return received;
    }

    private async Task<(string queue, IChannel ch, IConnection cnn)> SetupTestAssertionEnvironment()
    {
        IChannel? ch = null;
        try
        {
            var exchange = "logs-exch";
            var queue = "logs-q";
            var routingKey = "inf";
        
            var factory = fixture.CreateFactory();
        
            var cnn = await factory.CreateConnectionAsync();
            ch = await cnn.CreateChannelAsync();
        
            await ch.ExchangeDeclareAsync(exchange, type: "topic", durable: false, autoDelete: true);
            await ch.QueueDeclareAsync(queue, durable: false, exclusive: false, autoDelete: true);
            await ch.QueueBindAsync(queue, exchange, routingKey);
        
            Environment.SetEnvironmentVariable("RABBITMQ_EXCHANGE", exchange);
            Environment.SetEnvironmentVariable("RABBITMQ_USER", factory.UserName);
            Environment.SetEnvironmentVariable("RABBITMQ_PASS", factory.Password);
            Environment.SetEnvironmentVariable("RABBITMQ_PORT", fixture.AmqpPort.ToString());
            Environment.SetEnvironmentVariable("RABBITMQ_HOST", fixture.Host);
            Environment.SetEnvironmentVariable("RABBITMQ_VHOST", fixture.VHost);
            return (queue, ch, cnn);
        }
        catch
        {
            await ch.DisposeAsync();
            throw;
        }
    }
}