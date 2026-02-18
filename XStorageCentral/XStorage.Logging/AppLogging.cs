using System.Text.Json;

namespace XStorage.Logging;

public interface IAppLogging
{
    void WriteTraffic<T>(string msg, T? data = default);
    void WriteInfo<T>(string msg, T data);
    void WriteFact<T>(string msg, T data);
    void WriteDebug<T>(string msg, T data);
}


public struct LogEnvelope<T>
{
    public DateTime TimeStamp { get; set; }
    public string Type { get; set; }
    public string Msg { get; set; }
    public T? Data { get; set; }
    public string Cid { get; set; }
}

public abstract class AppLogging : LogBuffer, IAppLogging
{
    private readonly string _correlationId = Guid.NewGuid().ToString("N");
    
    public void WriteTraffic<T>(string msg, T? data = default) => Enqueue("trf", msg, data);

    public void WriteInfo<T>(string msg, T data) => Enqueue("inf", msg, data);
    public void WriteFault<T>(string msg, T data) => Enqueue("flt", msg, data);
    public void WriteDebug<T>(string msg, T data) => Enqueue("dbg", msg, data);
    
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

        Add(JsonSerializer.Serialize(entry), type);
    }
}