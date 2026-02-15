using System.Net;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Images;

namespace XStorage.RestApi.SystemTests;

public class ApiCallsContainerTests : IAsyncLifetime
{
    private readonly IFutureDockerImage _image;
    private IContainer? _container;
    
    public ApiCallsContainerTests()
    {
        var repoRoot = Path.GetFullPath(
            Path.Combine(AppContext.BaseDirectory, 
                "..", "..", "..", "..", "..", ".."));

        _image = new ImageFromDockerfileBuilder()
            .WithDockerfileDirectory(repoRoot)
            .WithDockerfile("infra/XStorage.RestApi/Dockerfile")
            .WithName($"xstorage-restapi-test:{Guid.NewGuid():N}")
            .Build();
    }
    
    public async Task InitializeAsync()
    {
        await _image.CreateAsync();
        
        _container = new ContainerBuilder(_image.FullName)
            .WithImage(_image)
            .WithCleanUp(true)
            .WithName($"xstorage-restapi-{Guid.NewGuid():N}")
            .WithPortBinding(8080, assignRandomHostPort:true)
            .WithWaitStrategy(
                Wait.ForUnixContainer()
                    .UntilHttpRequestIsSucceeded(
                        r=>r.ForPort(8080)
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
    }
    
    [Fact]
    public async Task PingMustWork()
    {
        var hostPort = _container!.GetMappedPublicPort(8080);
        var baseUrl = $"http://localhost:{hostPort}";

        using var http = new HttpClient();
        var text = await http.GetStringAsync($"{baseUrl}/");

        Assert.Equal("ok", text);
    } 
    
    [Theory]
    [InlineData("/filter")]
    [InlineData("/search")]
    [InlineData("/partitions")]
    [InlineData("/sections")]
    [InlineData("/ids")]
    [InlineData("/selections")]
    public async Task GeneralRoutesMustBeAccessible(string route)
    {
        var hostPort = _container!.GetMappedPublicPort(8080);
        var baseUrl = $"http://localhost:{hostPort}";

        using var http = new HttpClient();
        var responseMessage = await http.GetAsync($"{baseUrl}{route}");

        Assert.NotEqual(HttpStatusCode.NotFound, responseMessage.StatusCode);
    }
}