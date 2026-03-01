using System.Globalization;

namespace XStorage.ContentReadApi;

sealed class StorageSelector
{
    public required string[] HddRoots { get; init; }
    public required string SsdMetaRoot { get; init; }
    
    public static StorageSelector Build()
    {
        var h0 = Environment.GetEnvironmentVariable("XSTORAGE_HDD0");
        var h1 = Environment.GetEnvironmentVariable("XSTORAGE_HDD1");
        var h2 = Environment.GetEnvironmentVariable("XSTORAGE_HDD2");

        if (string.IsNullOrWhiteSpace(h0) ||
            string.IsNullOrWhiteSpace(h1) ||
            string.IsNullOrWhiteSpace(h2))
            throw new InvalidOperationException("Set XSTORAGE_HDD0, XSTORAGE_HDD1, XSTORAGE_HDD2 (absolute paths). Optional: XSTORAGE_SSD_META.");

        var ssd = Environment.GetEnvironmentVariable("XSTORAGE_SSD_META");
        if (string.IsNullOrWhiteSpace(ssd))
        {
            throw new InvalidOperationException(
                "Set XSTORAGE_SSD_META to avoid damaging of your HDD disks due to frequent operations.");
        }

        return new StorageSelector
        {
            HddRoots = [h0, h1, h2],
            SsdMetaRoot = ssd
        };
    }
}