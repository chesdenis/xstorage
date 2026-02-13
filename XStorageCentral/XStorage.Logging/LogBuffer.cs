using System.Collections.Concurrent;

namespace XStorage.Logging;


public abstract class LogBuffer : IDisposable
{
    private readonly BlockingCollection<string> _queue = new(50000);
    private const int BatchSize = 10000;
    private readonly Task _flushingTask;
    private volatile bool _disposed;
    
    protected LogBuffer()
    {
        _flushingTask = Task.Run(ProcessQueue);
    }


    protected void Add(string message)
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(LogBuffer));
        }

        try
        {
            _queue.Add(message);
        }
        catch (InvalidOperationException)
        {
            throw new ObjectDisposedException(nameof(LogBuffer));
        }
    }

    private void ProcessQueue()
    {
        var batch = new List<string>(BatchSize);
        foreach (var log in _queue.GetConsumingEnumerable())
        {
            batch.Add(log);
            
            if (batch.Count >= BatchSize)
            {
                Flush(batch);
                batch.Clear();
            }
        }

        if (batch.Count > 0) 
            Flush(batch);
    }

    protected abstract void Flush(List<string> logs);

    public void Dispose()
    {
        if(Interlocked.Exchange(ref _disposed, true))
            return;
        
        _disposed = true;

        _queue.CompleteAdding();
        _flushingTask.Wait();
        _queue.Dispose();
    }
}