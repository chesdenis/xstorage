using System.Globalization;

namespace XStorage.Common;

public static class ReadPartitionsExtensions
{
    public static string GetPartition(this int rangeId) => rangeId.ToString("x2");

    public static string SelectHddRoot(this string[] hddRoots, string md5)
    {
        // Assumes md5[0..8] are valid hex; user requested no validation/normalization.
        var v = uint.Parse(md5.AsSpan(0, 8), NumberStyles.HexNumber, CultureInfo.InvariantCulture);
        return hddRoots[(int)(v % (uint)hddRoots.Length)];
    }

    public static string SelectHddRootByPartition(this string[] hddRoots, string partition)
    {
        int p = Convert.ToInt32(partition, 16);
        return hddRoots[p % hddRoots.Length];
    }

    public static string GetBlobPath(this string root, string md5)
        => Path.Combine(root, md5[..2], md5.Substring(2, 2), md5);

    public static string GetMetaPath(this string root, string md5)
        => Path.Combine(root, md5[..2], md5.Substring(2, 2), md5 + ".json");
}