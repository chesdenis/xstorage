using XStorage.Common.Caching;

namespace XStorage.Common.ReadPatterns;

public sealed class SyncLayer<T> : ISyncReadLayer<T>
{
    private readonly Func<CacheResult<T>> _tryRead;
    private readonly Action<T> _set;

    public SyncLayer(string name, Func<CacheResult<T>> tryRead, Action<T> set)
    {
        Name = name;
        _tryRead = tryRead;
        _set = set;
    }

    public string Name { get; }
    public CacheResult<T> TryRead() => _tryRead();
    public void Set(T value) => _set(value);
}