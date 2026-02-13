using XStorage.Logging;
using System.Collections.Concurrent;

namespace XStorage.Logging.UnitTests;

public class LogBufferUnitTests
{
    private class TestLogBuffer : LogBuffer
    {
        public ConcurrentBag<List<string>> Batches { get; } = new();
        public List<string> AllLogs => Batches.SelectMany(x => x).ToList();
        public ManualResetEventSlim FlushEvent { get; } = new(false);
        public int ExpectedLogs { get; set; }
        private int _totalLogsFlushed;

        public void CallAdd(string message) => Add(message);

        protected override void Flush(List<string> logs)
        {
            Batches.Add([..logs]);
            var current = Interlocked.Add(ref _totalLogsFlushed, logs.Count);
            if (ExpectedLogs > 0 && current >= ExpectedLogs)
            {
                FlushEvent.Set();
            }
        }
    }

    [Fact]
    public void Add_ShouldFlush_WhenBatchSizeReached()
    {
        // Arrange
        using var buffer = new TestLogBuffer();
        const int batchSize = 10000; // From LogBuffer.BatchSize
        buffer.ExpectedLogs = batchSize;

        // Act
        for (int i = 0; i < batchSize; i++)
        {
            buffer.CallAdd($"msg {i}");
        }

        // Assert
        Assert.True(buffer.FlushEvent.Wait(5000), "Flush should occur when batch size is reached");
        Assert.Single(buffer.Batches);
        Assert.Equal(batchSize, buffer.AllLogs.Count);
    }

    [Fact]
    public void Dispose_ShouldFlushRemainingLogs()
    {
        // Arrange
        const int count = 500;
        TestLogBuffer buffer;
        
        // Act
        using (buffer = new TestLogBuffer())
        {
            for (int i = 0; i < count; i++)
            {
                buffer.CallAdd($"msg {i}");
            }
        } // Dispose called here

        // Assert
        Assert.Equal(count, buffer.AllLogs.Count);
    }

    [Fact]
    public void Add_AfterDispose_ShouldThrowObjectDisposedException()
    {
        // Arrange
        var buffer = new TestLogBuffer();
        buffer.Dispose();

        // Act & Assert
        Assert.Throws<ObjectDisposedException>(() => buffer.CallAdd("msg"));
    }

    [Fact]
    public async Task Add_MultiThreaded_ShouldNotLoseLogs()
    {
        // Arrange
        using var buffer = new TestLogBuffer();
        const int threadCount = 10;
        const int logsPerThread = 2000;
        const int totalLogs = threadCount * logsPerThread;
        buffer.ExpectedLogs = totalLogs;

        // Act
        var tasks = Enumerable.Range(0, threadCount).Select(t => Task.Run(() =>
        {
            for (int i = 0; i < logsPerThread; i++)
            {
                buffer.CallAdd($"t{t} msg {i}");
            }
        }));

        await Task.WhenAll(tasks);
        buffer.Dispose();

        // Assert
        Assert.Equal(totalLogs, buffer.AllLogs.Count);
        Assert.Equal(totalLogs, buffer.AllLogs.Distinct().Count());
    }

    [Fact]
    public void Add_WhenQueueFull_ShouldWaitAndSucceed()
    {
        // Arrange
        using var buffer = new TestLogBuffer();
        const int totalLogs = 70000; // More than internal queue capacity (50000)
        buffer.ExpectedLogs = totalLogs;

        // Act
        var producer = Task.Run(() =>
        {
            for (int i = 0; i < totalLogs; i++)
            {
                buffer.CallAdd($"msg {i}");
            }
        });

        // Assert
        Assert.True(producer.Wait(10000), "Producer should complete (eventually items should be consumed and new ones added)");
        buffer.Dispose();
        Assert.Equal(totalLogs, buffer.AllLogs.Count);
    }
}