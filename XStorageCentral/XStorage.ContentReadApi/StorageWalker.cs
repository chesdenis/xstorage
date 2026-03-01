namespace XStorage.ContentReadApi;

sealed class StorageWalker
{
    public static HashSet<string> BuildMd5HashMap(string partition)
    {
        var ht = new HashSet<string>();

        foreach (var path in Directory.EnumerateFiles(partition, "*.json", SearchOption.AllDirectories))
        {
            var md5 = Path.GetFileNameWithoutExtension(path);
            if (md5.Length != 32) continue;

            ht.Add(md5);
        }

        return ht;
    }
}