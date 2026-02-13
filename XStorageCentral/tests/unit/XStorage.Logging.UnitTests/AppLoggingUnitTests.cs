using XStorage.Logging;
using System.Collections.Concurrent;
using System.Text.Json;

namespace XStorage.Logging.UnitTests;

public class AppLoggingUnitTests
{
    private class TestAppLogging : AppLogging
    {
        public ConcurrentBag<string> FlushedLogs { get; } = new();
        public ManualResetEventSlim FlushEvent { get; } = new(false);
        public int ExpectedLogs { get; set; }
        private int _totalLogsFlushed;

        protected override void Flush(List<string> logs)
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
            .Select(log => JsonSerializer.Deserialize<LogEnvelope<object>>(log))
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
        var env1 = JsonSerializer.Deserialize<LogEnvelope<int>>(logger1.FlushedLogs.First());
        var env2 = JsonSerializer.Deserialize<LogEnvelope<int>>(logger2.FlushedLogs.First());

        Assert.NotEqual(env1.Cid, env2.Cid);
    }

    [Fact]
    public void LogEnvelope_ShouldContainCorrectMetadata()
    {
        // Arrange
        using var logger = new TestAppLogging();
        var data = new { Info = "test" };
        var startTime = DateTime.UtcNow;

        // Act
        logger.WriteTraffic("msg", data);
        logger.Dispose();

        // Assert
        var log = logger.FlushedLogs.First();
        var envelope = JsonSerializer.Deserialize<LogEnvelope<JsonElement>>(log);

        Assert.Equal("trf", envelope.Type);
        Assert.Equal("msg", envelope.Msg);
        Assert.Equal("test", envelope.Data.GetProperty("Info").GetString());
        Assert.True(envelope.TimeStamp >= startTime);
        Assert.True(envelope.TimeStamp <= DateTime.UtcNow);
        Assert.False(string.IsNullOrEmpty(envelope.Cid));
    }
}
