using XStorage.Logging;
using System.Collections.Concurrent;
using System.Text.Json;

namespace XStorage.Logging.UnitTests;

public class AppLoggingUnitTests
{
    private class TestAppLogging : AppLogging
    {
        public ConcurrentBag<LogMessage> FlushedLogs { get; } = new();
        public ManualResetEventSlim FlushEvent { get; } = new(false);
        public int ExpectedLogs { get; set; }
        private int _totalLogsFlushed;

        protected override void Flush(List<LogMessage> logs)
        {
            foreach (var log in logs)
            {
                FlushedLogs.Add(log);
            }
            
            var current = Interlocked.Add(ref _totalLogsFlushed, logs.Count);
            if (ExpectedLogs > 0 && current >= ExpectedLogs)
            {
                FlushEvent.Set();
            }
        }
    }

    [Fact]
    public void WriteMethods_ShouldUseSameCorrelationId_ForSingleInstance()
    {
        // Arrange
        using var logger = new TestAppLogging();
        logger.ExpectedLogs = 3;

        // Act
        logger.WriteTraffic("traffic msg", new { Id = 1 });
        logger.WriteInfo("event msg", new { Value = "test" });
        logger.WriteFact("fact msg", new { Status = "OK" });
        logger.Dispose();

        // Assert
        var envelopes = logger.FlushedLogs
            .Select(log => JsonSerializer.Deserialize<LogEnvelope<LogMessage>>(log.Message))
            .ToList();

        Assert.Equal(3, envelopes.Count);
        var correlationId = envelopes[0].Cid;
        Assert.False(string.IsNullOrEmpty(correlationId));
        Assert.All(envelopes, e => Assert.Equal(correlationId, e.Cid));
    }

    [Fact]
    public void DifferentInstances_ShouldHaveDifferentCorrelationIds()
    {
        // Arrange
        using var logger1 = new TestAppLogging();
        using var logger2 = new TestAppLogging();

        // Act
        logger1.WriteInfo("msg1", 1);
        logger2.WriteInfo("msg2", 2);
        logger1.Dispose();
        logger2.Dispose();

        // Assert
        var env1 = JsonSerializer.Deserialize<LogEnvelope<int>>(logger1.FlushedLogs.First().Message);
        var env2 = JsonSerializer.Deserialize<LogEnvelope<int>>(logger2.FlushedLogs.First().Message);

        Assert.NotEqual(env1.Cid, env2.Cid);
    }

    [Fact]
    public void WriteTraffic_ShouldProduceTrfType()
    {
        // Arrange
        using var logger = new TestAppLogging();

        // Act
        logger.WriteTraffic<object>("msg");
        logger.Dispose();

        // Assert
        var envelope = JsonSerializer.Deserialize<LogEnvelope<object>>(logger.FlushedLogs.First().Message);
        Assert.Equal("trf", envelope.Type);
    }

    [Fact]
    public void WriteInfo_ShouldProduceInfType()
    {
        // Arrange
        using var logger = new TestAppLogging();

        // Act
        logger.WriteInfo("msg", "data");
        logger.Dispose();

        // Assert
        var envelope = JsonSerializer.Deserialize<LogEnvelope<string>>(logger.FlushedLogs.First().Message);
        Assert.Equal("inf", envelope.Type);
    }

    [Fact]
    public void WriteFact_ShouldProduceFctType()
    {
        // Arrange
        using var logger = new TestAppLogging();

        // Act
        logger.WriteFact("msg", "data");
        logger.Dispose();

        // Assert
        var envelope = JsonSerializer.Deserialize<LogEnvelope<string>>(logger.FlushedLogs.First().Message);
        Assert.Equal("fct", envelope.Type);
    }

    [Fact]
    public void WriteDebug_ShouldProduceDbgType()
    {
        // Arrange
        using var logger = new TestAppLogging();

        // Act
        logger.WriteDebug("msg", "data");
        logger.Dispose();

        // Assert
        var envelope = JsonSerializer.Deserialize<LogEnvelope<string>>(logger.FlushedLogs.First().Message);
        Assert.Equal("dbg", envelope.Type);
    }
}
