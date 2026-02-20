using XStorage.Common.Caching;

namespace XStorage.Common.ReadPatterns;

public interface IAsyncReadLayer<T> : IReadLayer<T>
{
    ValueTask SetAsync(T value, CancellationToken ct);
    ValueTask<CacheResult<T>> TryReadAsync(CancellationToken ct);
}