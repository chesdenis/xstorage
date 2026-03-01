namespace XStorage.ContentReadApi;

sealed class PartitionMap
{
    private readonly string[]? _partitions = new string[256];

    private PartitionMap()
    {
        for (var i = 0; i < 256; i++)
        {
            _partitions[i] = i.ToString("x2");
        }
    }
    
    public static PartitionMap Build() => new();

    public IEnumerable<string> Enumerate()
    {
        if (_partitions == null)
        {
            throw new ArgumentNullException(nameof(_partitions));
        }
        
        foreach (var partition in _partitions)
        {
            yield return partition;
        }
    }
}