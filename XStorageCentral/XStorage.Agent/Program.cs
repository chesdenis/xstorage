using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Polly;
using Polly.Retry;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using XStorage.Common;
using XStorage.Logging.Adapters;
using XStorage.RabbitMq;

var agentId = "AGENT_ID".FromEnvAsString();;
var rabbitHost = "RABBIT_HOST".FromEnvAsString();
var rabbitUser = "RABBIT_USER".FromEnvAsString();
var rabbitPass = "RABBIT_PASS".FromEnvAsString();
var rabbitVhost = "RABBIT_VHOST".FromEnvAsString();
var rabbitPort = "RABBIT_PORT".FromEnvAsString();

var cmdExchange = "CMD_EXCHANGE".FromEnvAsString();
var queuePrefix = "QUEUE_PREFIX".FromEnvAsString();

var queueName = $"{queuePrefix}{agentId}";

var jsonOpts = new JsonSerializerOptions
{
    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    WriteIndented = false
};

var rabbitMqMessagePublisher = new RabbitMqMessagePublisher();
var appLogging = new RabbitMqAppLogging(rabbitMqMessagePublisher);

using var http = new HttpClient();
http.Timeout = TimeSpan.FromSeconds(30);
http.DefaultRequestHeaders.UserAgent.Add(new ProductInfoHeaderValue("Agent", "1.0"));

AsyncRetryPolicy<HttpResponseMessage> callbackPolicy = Policy<HttpResponseMessage>
    .Handle<HttpRequestException>()
    .OrResult(resp =>
    {
        if ((int)resp.StatusCode is >= 200 and <= 299) return false;
        if (resp.StatusCode == HttpStatusCode.Conflict) return false;

        return resp.StatusCode == HttpStatusCode.TooManyRequests
            || resp.StatusCode == HttpStatusCode.RequestTimeout
            || resp.StatusCode == HttpStatusCode.BadGateway
            || resp.StatusCode == HttpStatusCode.ServiceUnavailable
            || resp.StatusCode == HttpStatusCode.GatewayTimeout
            || (int)resp.StatusCode >= 500;
    })
    .WaitAndRetryForeverAsync(_ => TimeSpan.FromSeconds(3));

appLogging.WriteFact("AgentId: {agentId}", agentId);
Console.WriteLine($"Agent started. AgentId: {agentId}");
Console.WriteLine("Press Ctrl+C to stop.");

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true; 
    cts.Cancel();
    
    // to not lose logs which are inside buffer
    appLogging.Dispose();
};

var factory = new ConnectionFactory
{
    HostName = rabbitHost,
    Port = rabbitPort.FromEnvAsInt(),
    UserName = rabbitUser,
    Password = rabbitPass,
    VirtualHost = rabbitVhost,

    AutomaticRecoveryEnabled = true,
    NetworkRecoveryInterval = TimeSpan.FromSeconds(5),
    TopologyRecoveryEnabled = true
};

await using var conn = await factory.CreateConnectionAsync(clientProvidedName: $"agent-{agentId}");
await using var ch = await conn.CreateChannelAsync();

// Ensure topology (idempotent)
await ch.ExchangeDeclareAsync(exchange: cmdExchange, type: ExchangeType.Direct, durable: true, autoDelete: false, arguments: null);
await ch.QueueDeclareAsync(queue: queueName, durable: true, exclusive: false, autoDelete: false, arguments: null);
await ch.QueueBindAsync(queue: queueName, exchange: cmdExchange, routingKey: agentId);

await ch.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

var consumer = new AsyncEventingBasicConsumer(ch);
consumer.ReceivedAsync += async (_, ea) =>
{
    var deliveryTag = ea.DeliveryTag;
    
    try
    {
        var json = Encoding.UTF8.GetString(ea.Body.ToArray());

        CommandEnvelope? cmd;
        try
        {
            cmd = JsonSerializer.Deserialize<CommandEnvelope>(json, jsonOpts);
        }
        catch (Exception ex)
        {
            appLogging.WriteFault("[BAD JSON] {agentId} {message}", new {agentId, ex.Message});
            await ch.BasicRejectAsync(deliveryTag, requeue: false);
            return;
        }

        if (cmd is null || string.IsNullOrWhiteSpace(cmd.CommandId) || string.IsNullOrWhiteSpace(cmd.CallbackUrl))
        {
            appLogging.WriteFault("[INVALID CMD] Missing commandId or callbackUrl. {agentId} {message}", new {agentId});
            await ch.BasicRejectAsync(deliveryTag, requeue: false);
            return;
        }

        if (!string.Equals(cmd.ClientId, agentId, StringComparison.Ordinal))
        {
            appLogging.WriteFault("[WRONG TARGET] cmd.ClientId={clientId} agentId={agentId}", new {cmd.ClientId, agentId});
            await ch.BasicRejectAsync(deliveryTag, requeue: true);
            return;
        }

        appLogging.WriteInfo("[RECV] commandId={commandId} kind={kind}", new {cmd.CommandId, cmd.Kind});

        var started = DateTimeOffset.UtcNow;
        var (ok, payload, error) = await ExecutePlaceholderAsync(cmd, cts.Token);
        var finished = DateTimeOffset.UtcNow;

        var result = new CommandResult
        {
            CommandId = cmd.CommandId,
            ClientId = agentId,
            Ok = ok,
            JsonPayload = payload,
            Error = error,
            StartedAtUtc = started,
            FinishedAtUtc = finished
        };

        await PostResults(http, callbackPolicy, cmd.CallbackUrl!, cmd.ResultToken, result, cts.Token);

        await ch.BasicAckAsync(deliveryTag, multiple: false);
        appLogging.WriteTraffic("[ACK] agentId={agentId} commandId={commandId} ok={ok}",  new {agentId, cmd.CommandId});
    }
    catch (OperationCanceledException)
    {
        try { await ch.BasicNackAsync(deliveryTag, multiple: false, requeue: true); } catch { }
    }
    catch (Exception ex)
    {
        appLogging.WriteFault("[FAIL] {name}: {message}", new {name = ex.GetType().Name, message = ex.Message});
        try { await ch.BasicNackAsync(deliveryTag, multiple: false, requeue: true); } catch { }
    }
};

var consumerTag = await ch.BasicConsumeAsync(queue: queueName, autoAck: false, consumer: consumer);

try
{
    await Task.Delay(Timeout.Infinite, cts.Token);
}
finally
{
    try { await ch.BasicCancelAsync(consumerTag); }
    catch
    {
        // ignored
    }
}

static async Task<(bool Ok, string? Payload, string? Error)> ExecutePlaceholderAsync(CommandEnvelope cmd, CancellationToken ct)
{
    var delayMs = cmd.Kind?.Equals("Ping", StringComparison.OrdinalIgnoreCase) == true ? 50 : 300;
    await Task.Delay(delayMs, ct);

    return (true, JsonSerializer.Serialize(new { kind = cmd.Kind, echo = cmd.JsonPayload }, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase }), null);
}

static async Task PostResults(
    HttpClient http,
    AsyncRetryPolicy<HttpResponseMessage> policy,
    string callbackUrl,
    string? token,
    CommandResult result,
    CancellationToken ct)
{
    var json = JsonSerializer.Serialize(result, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

    // IMPORTANT: HttpContent can't be reused across retries. Create it inside the execution.
    HttpResponseMessage resp = await policy.ExecuteAsync(async (context, tokenCt) =>
    {
        using var req = new HttpRequestMessage(HttpMethod.Post, callbackUrl);
        req.Content = new StringContent(json, Encoding.UTF8, "application/json");

        if (!string.IsNullOrWhiteSpace(token))
            req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);

        return await http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, tokenCt);
    }, new Context(), ct);

    // Final result handling (non-retriable statuses land here)
    if ((int)resp.StatusCode is >= 200 and <= 299 || resp.StatusCode == HttpStatusCode.Conflict)
    {
        resp.Dispose();
        return;
    }

    var body = await resp.Content.ReadAsStringAsync(ct);
    resp.Dispose();

    // Fail hard on common auth/client errors
    if ((int)resp.StatusCode is 400 or 401 or 403 or 404)
        throw new InvalidOperationException($"Callback rejected: {(int)resp.StatusCode} {resp.ReasonPhrase}. Body={body}");

    throw new TimeoutException($"Callback failed: {(int)resp.StatusCode} {resp.ReasonPhrase}. Body={body}");
}

public sealed class CommandEnvelope
{
    public string? CommandId { get; set; }
    public string? ClientId { get; set; }
    public string? Kind { get; set; }
    public string? JsonPayload { get; set; }

    // Callback endpoint in central API
    public string? CallbackUrl { get; set; }

    // Optional auth token to send as Bearer for callback
    public string? ResultToken { get; set; }

    public DateTimeOffset? ExpiresAtUtc { get; set; }
}

public sealed class CommandResult
{
    public string CommandId { get; set; } = "";
    public string ClientId { get; set; } = "";
    public bool Ok { get; set; }
    public string? JsonPayload { get; set; }
    public string? Error { get; set; }
    public DateTimeOffset StartedAtUtc { get; set; }
    public DateTimeOffset FinishedAtUtc { get; set; }
}