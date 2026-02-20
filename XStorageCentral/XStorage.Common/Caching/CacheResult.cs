namespace XStorage.Common.Caching;

public readonly record struct CacheResult<T>(
    bool Hit,
    T? Value,
    Exception? Error = null)
{
    public static CacheResult<T> MissResult() => new(
        Hit: false, Value: default, Error: null);

    public static CacheResult<T> HitResult(T value) =>
        new(Hit: true, Value: value, Error: null);

    public static CacheResult<T> FaultResult(Exception ex) =>
        new(Hit: false, Value: default, Error: ex);
}