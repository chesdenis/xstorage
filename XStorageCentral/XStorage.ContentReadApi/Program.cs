using Microsoft.AspNetCore.Server.Kestrel.Core;
using XStorage.Common;
using XStorage.ContentReadApi;

var builder = WebApplication.CreateBuilder(args);

builder.Logging.ClearProviders();
builder.Logging.SetMinimumLevel(LogLevel.None);

builder.WebHost.ConfigureKestrel((o) =>
{
    o.ConfigureEndpointDefaults(lo =>
    {
        lo.Protocols = HttpProtocols.Http1;
    });

    o.AddServerHeader = false;

    o.Limits.MaxConcurrentConnections = 50_000;
    o.Limits.MaxConcurrentUpgradedConnections = 10_000;
    o.Limits.KeepAliveTimeout = TimeSpan.FromMinutes(10);
    o.Limits.RequestHeadersTimeout = TimeSpan.FromSeconds(5);

    o.Limits.MaxRequestBodySize = 0; // reject bodies, because we only get
});

builder.Services.AddRouting(o =>
{
    o.LowercaseUrls = true;
    o.AppendTrailingSlash = false;
});

var app = builder.Build();

app.Use((ctx, next) =>
{
    // Small micro-optimizations + explicit cache hints for content-addressed data
    ctx.Response.Headers["Cache-Control"] = 
        "no-store, no-cache, must-revalidate, max-age=0";
    ctx.Response.Headers["Pragma"] = "no-cache";
    ctx.Response.Headers["Expires"] = "0";
    return next(ctx);
});

var cfg = StorageSelector.Build();
var partitionMap = PartitionMap.Build();

foreach (var r in cfg.HddRoots) Directory.CreateDirectory(r);
Directory.CreateDirectory(cfg.SsdMetaRoot);

app.MapGet("/", () => Results.Text("ok"));

// ----------------------------------------------------
// GET BLOB: /objects/{md5}
// ----------------------------------------------------
app.MapGet("/objects/{md5}", async (HttpContext ctx, string md5) =>
{
    var root = cfg.HddRoots.SelectHddRoot(md5);
    var blobPath = root.GetBlobPath(md5);

    if (!File.Exists(blobPath))
    {
        ctx.Response.StatusCode = StatusCodes.Status404NotFound;
        return;
    }

    ctx.Response.Headers.ETag = $"\"{md5}\"";
    ctx.Response.ContentType = "application/octet-stream";

    // Fast path: let the server send the file efficiently
    await ctx.Response.SendFileAsync(blobPath, ctx.RequestAborted);
});

// ----------------------------------------------------
// GET META (SSD preferred): /objects/{md5}/meta
// ----------------------------------------------------
app.MapGet("/objects/{md5}/meta", async (HttpContext ctx, string md5) =>
{
    string? metaPath = null;

    {
        var p = cfg.SsdMetaRoot.GetMetaPath(md5);
        if (File.Exists(p)) metaPath = p;
    }

    if (metaPath is null)
    {
        var root = cfg.HddRoots.SelectHddRoot(md5);
        var p = root.GetMetaPath(md5);
        if (File.Exists(p)) metaPath = p;
    }

    if (metaPath is null)
    {
        ctx.Response.StatusCode = StatusCodes.Status404NotFound;
        return;
    }

    ctx.Response.Headers.ETag = $"\"{md5}.json\"";
    ctx.Response.ContentType = "application/json; charset=utf-8";
    await ctx.Response.SendFileAsync(metaPath, ctx.RequestAborted);
});


app.Urls.Clear();

app.Run();
