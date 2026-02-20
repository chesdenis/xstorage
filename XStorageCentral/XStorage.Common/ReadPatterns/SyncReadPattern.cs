namespace XStorage.Common.ReadPatterns;

public sealed class SyncReadPattern<T>
{
    private readonly List<ISyncReadLayer<T>> _layers = new();

    public SyncReadPattern(ISyncReadLayer<T> first) => _layers.Add(first);

    public SyncReadPattern<T> Then(ISyncReadLayer<T> next)
    {
        _layers.Add(next);
        return this;
    }

    public T Execute(Func<T> source)
    {
        // 1) try layers in order, first layer means is fastest
        for (int i = 0; i < _layers.Count; i++)
        {
            var res = _layers[i].TryRead();

            // Decide your error policy: ignore layer failures or fail fast
            if (res.Error is not null)
                continue; // "best effort" chain

            if (!res.Hit) continue;

            var value = res.Value!;

            // promote to earlier (faster) layers, for example we here at the disk drive operation
            // - so we have to promote that value to fastest upper level to 0, so this is why it's back flow
            PromoteBack(i - 1, value);
            return value;
        }

        // 2) fallback to source, just in case, but ideally must be on last layer above ;( 
        var srcValue = source();

        // 3) promote to all layers, ie rebuild all layers
        PromoteBack(_layers.Count - 1, srcValue);

        return srcValue;
    }

    private void PromoteBack(int lastIndex, T value)
    {
        for (var i = lastIndex; i >= 0; i--)
        {
            try
            {
                _layers[i].Set(value);
            }
            catch
            {
                // ignored
            }
        }
    }
}