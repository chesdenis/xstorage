using System.Net;
using System.Text.Json;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Images;
using XStorage.Common;

namespace XStorage.ContentReadApi.SystemTests;

public class ContentReadApiContainerTests : IAsyncLifetime
{
    private readonly IFutureDockerImage _image;
    private IContainer? _container;
    
    private readonly string _tempHdd0;
    private readonly string _tempHdd1;
    private readonly string _tempHdd2;
    private readonly string _tempSsd;

    public ContentReadApiContainerTests()
    {
        var repoRoot = Path.GetFullPath(
            Path.Combine(AppContext.BaseDirectory, 
                "..", "..", "..", "..", "..", ".."));

        _image = new ImageFromDockerfileBuilder()
            .WithDockerfileDirectory(repoRoot)
            .WithDockerfile("infra/XStorage.ContentReadApi/Dockerfile")
            .WithName($"xstorage-contentreadapi-test:{Guid.NewGuid():N}")
            .Build();

        _tempHdd0 = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
        _tempHdd1 = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
        _tempHdd2 = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
        _tempSsd = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
        
        Directory.CreateDirectory(_tempHdd0);
        Directory.CreateDirectory(_tempHdd1);
        Directory.CreateDirectory(_tempHdd2);
        Directory.CreateDirectory(_tempSsd);
    }
    
    public async Task InitializeAsync()
    {
        await _image.CreateAsync();
        
        _container = new ContainerBuilder()
            .WithImage(_image.FullName)
            .WithCleanUp(true)
            .WithName($"xstorage-contentreadapi-{Guid.NewGuid():N}")
            .WithEnvironment("XSTORAGE_HDD0", "/data/hdd0")
            .WithEnvironment("XSTORAGE_HDD1", "/data/hdd1")
            .WithEnvironment("XSTORAGE_HDD2", "/data/hdd2")
            .WithEnvironment("XSTORAGE_SSD_META", "/data/ssd")
            .WithBindMount(_tempHdd0, "/data/hdd0")
            .WithBindMount(_tempHdd1, "/data/hdd1")
            .WithBindMount(_tempHdd2, "/data/hdd2")
            .WithBindMount(_tempSsd, "/data/ssd")
            .WithPortBinding(8080, assignRandomHostPort: true)
            .WithWaitStrategy(
                Wait.ForUnixContainer()
                    .UntilHttpRequestIsSucceeded(
                        r => r.ForPort(8080)
                            .ForPath("/")
                            .ForStatusCode(HttpStatusCode.OK)))
            .Build();

        await _container.StartAsync();
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
        {
            await _container.DisposeAsync();
        }

        await _image.DisposeAsync();
        
        try { Directory.Delete(_tempHdd0, true); } catch {}
        try { Directory.Delete(_tempHdd1, true); } catch {}
        try { Directory.Delete(_tempHdd2, true); } catch {}
        try { Directory.Delete(_tempSsd, true); } catch {}
    }
    
    [Fact]
    public async Task PingMustWork()
    {
        var hostPort = _container!.GetMappedPublicPort(8080);
        var baseUrl = $"http://localhost:{hostPort}";

        using var http = BuildClient();
        var text = await http.GetStringAsync($"{baseUrl}/");

        Assert.Equal("ok", text);
    }

    [Fact]
    public async Task GetFileShouldReturnContent()
    {
        var md5 = "0123456789abcdef0123456789abcdef";
        var hddRoots = new[] { _tempHdd0, _tempHdd1, _tempHdd2 };
        var targetHdd = hddRoots.SelectHddRoot(md5);
        var blobPath = targetHdd.GetBlobPath(md5);
        
        Directory.CreateDirectory(Path.GetDirectoryName(blobPath)!);
        await File.WriteAllBytesAsync(blobPath, Array.Empty<byte>());

        var hostPort = _container!.GetMappedPublicPort(8080);
        var baseUrl = $"http://localhost:{hostPort}";

        using var http = BuildClient();
        var response = await http.GetAsync($"{baseUrl}/files/{md5}");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var content = await response.Content.ReadAsByteArrayAsync();
        Assert.Empty(content);
    }

    [Fact]
    public async Task GetMetaShouldReturnContentAndPromoteToSsd()
    {
        var md5 = "abcdef0123456789abcdef0123456789";
        var hddRoots = new[] { _tempHdd0, _tempHdd1, _tempHdd2 };
        var targetHdd = hddRoots.SelectHddRoot(md5);
        var metaPathHdd = targetHdd.GetMetaPath(md5);
        var metaPathSsd = _tempSsd.GetMetaPath(md5);
        
        Directory.CreateDirectory(Path.GetDirectoryName(metaPathHdd)!);
        var metaContent = "{\"test\":123}";
        await File.WriteAllTextAsync(metaPathHdd, metaContent);

        Assert.False(File.Exists(metaPathSsd), "SSD should not have the file initially");

        var hostPort = _container!.GetMappedPublicPort(8080);
        var baseUrl = $"http://localhost:{hostPort}";

        using var http = BuildClient();
        var response = await http.GetAsync($"{baseUrl}/meta/{md5}");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var responseText = await response.Content.ReadAsStringAsync();
        Assert.Equal(metaContent, responseText);

        // Verify promotion to SSD
        // Promotion might be async in some cases, but in BuildMetaAccessPattern it seems to be awaited 
        // OR the response is sent AFTER pattern.ExecuteAsync.
        // In Program.cs: var metaPath = await pattern.ExecuteAsync(...)
        // AsyncReadPattern.ExecuteAsync calls PromoteBack and awaits it.
        // So it should be there.
        
        Assert.True(File.Exists(metaPathSsd), "File should be promoted to SSD");
        var ssdContent = await File.ReadAllTextAsync(metaPathSsd);
        Assert.Equal(metaContent, ssdContent);
    }

    [Fact]
    public async Task GetMetaRangeShouldReturnFilteredObjects()
    {
        var rangeId = 10; // "0a"
        var partition = rangeId.GetPartition();
        var md5 = "0a0123456789abcdef0123456789abcd"; // starts with 0a
        
        var hddRoots = new[] { _tempHdd0, _tempHdd1, _tempHdd2 };
        var targetHdd = hddRoots.SelectHddRoot(md5);
        
        var blobPath = targetHdd.GetBlobPath(md5);
        Directory.CreateDirectory(Path.GetDirectoryName(blobPath)!);
        await File.WriteAllBytesAsync(blobPath, Array.Empty<byte>());
        
        var metaPath = targetHdd.GetMetaPath(md5);
        Directory.CreateDirectory(Path.GetDirectoryName(metaPath)!);
        await File.WriteAllTextAsync(metaPath, "{\"a\":1, \"b\":2, \"c\":3}");

        var hostPort = _container!.GetMappedPublicPort(8080);
        var baseUrl = $"http://localhost:{hostPort}";

        using var http = BuildClient();
        var response = await http.GetAsync($"{baseUrl}/meta-range/{rangeId}?fields=a&fields=b");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var responseText = await response.Content.ReadAsStringAsync();
        
        var json = JsonDocument.Parse(responseText);
        Assert.Equal(JsonValueKind.Array, json.RootElement.ValueKind);
        Assert.Equal(1, json.RootElement.GetArrayLength());
        
        var obj = json.RootElement[0];
        Assert.Equal(md5, obj.GetProperty("md5").GetString());
        Assert.Equal(1, obj.GetProperty("a").GetInt32());
        Assert.Equal(2, obj.GetProperty("b").GetInt32());
        Assert.Throws<KeyNotFoundException>(() => obj.GetProperty("c"));
    }

    private static HttpClient BuildClient()
    {
        var httpClient = new HttpClient();
        httpClient.Timeout = TimeSpan.FromSeconds(30);

        return httpClient;
    }
}
