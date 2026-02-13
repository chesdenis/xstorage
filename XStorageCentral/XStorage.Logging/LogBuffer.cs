using System.Collections.Concurrent;
using System.Text.Json;

namespace XStorage.Logging;

public interface IAppLogging
{
    void WriteTraffic<T>(string msg, T? data = default);
    void WriteEvent<T>(string msg, T data);
    void WriteFact<T>(string msg, T data);
}

public abstract class AppLoging : LogBuffer, IAppLogging
{
    private readonly string _correlationId = Guid.NewGuid().ToString("N");
    
    public void WriteTraffic<T>(string msg, T? data = default) => Enqueue("trf", msg, data);

    public void WriteEvent<T>(string msg, T data) => Enqueue("evt", msg, data);
    
    public void WriteFact<T>(string msg, T data) => Enqueue("fct", msg, data);

    private void Enqueue<T>(string type, string msg, T? data)
    {
        var entry = new LogEnvelope<T>
        {
            TimeStamp = DateTime.UtcNow,
            Type = type,
            Msg = msg,
            Data = data,
            Cid = _correlationId
        };

        Add(JsonSerializer.Serialize(entry));
    }
}

public struct LogEnvelope<T>
{
    public DateTime TimeStamp { get; set; }
    public string Type { get; set; }
    public string Msg { get; set; }
    public T? Data { get; set; }
    public string Cid { get; set; }
}

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