using XStorage.Common.Caching;

namespace XStorage.Common.ReadPatterns;

public interface ISyncReadLayer<T> : IReadLayer<T>
{
    void Set(T value);
    CacheResult<T> TryRead();
}