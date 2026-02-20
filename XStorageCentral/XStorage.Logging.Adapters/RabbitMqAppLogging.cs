using System.Text;
using Polly;
using Polly.Retry;
using XStorage.Common;

namespace XStorage.Logging.Adapters;

public class RabbitMqAppLogging(IMessagePublisher publisher) : AppLogging
{
    private readonly string _exchangeName = "RABBITMQ_EXCHANGE".FromEnvAsString();

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
                
                await publisher.PublishAsync(
                    topic: state.This._exchangeName,
                    routingKey: type,
                    body: body,
                    CancellationToken.None);
                
                processedIndex++;
            }

        }, (This: this, Logs: logs)).ConfigureAwait(false).GetAwaiter().GetResult();
    }
}