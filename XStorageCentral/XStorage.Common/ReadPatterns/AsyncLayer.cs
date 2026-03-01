using XStorage.Common.Caching;

namespace XStorage.Common.ReadPatterns;

public sealed class AsyncLayer<T> : IAsyncReadLayer<T>
{
    private readonly Func<CancellationToken, ValueTask<CacheResult<T>>> _tryReadAsync;
    private readonly Func<T, CancellationToken, ValueTask> _setAsync;

    public AsyncLayer(
        string name,
        Func<CancellationToken, ValueTask<CacheResult<T>>> tryReadAsync,
        Func<T, CancellationToken, ValueTask> setAsync)
    {
        Name = name;
        _tryReadAsync = tryReadAsync;
        _setAsync = setAsync;
    }

    public string Name { get; }

    // Matches your interface: Task SetAsync(T value)
    // We ignore ct here because your interface doesn't take it.
    public ValueTask SetAsync(T value, CancellationToken ct) => _setAsync(value, CancellationToken.None);
        
    public ValueTask<CacheResult<T>> TryReadAsync(CancellationToken ct) => _tryReadAsync(ct);
}