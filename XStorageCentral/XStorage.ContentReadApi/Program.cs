using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Mvc;
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

async Task SendFilteredJsonAsync(HttpContext ctx, string filePath, string[] requestedFields)
{
    await using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, true);
    var node = await JsonNode.ParseAsync(fs, cancellationToken: ctx.RequestAborted);
    
    if (node is not JsonObject obj)
    {
        ctx.Response.StatusCode = StatusCodes.Status500InternalServerError;
        return;
    }

    var filtered = new JsonObject();
    foreach (var field in requestedFields)
    {
        if (obj.TryGetPropertyValue(field, out var value))
        {
            filtered[field] = value?.DeepClone();
        }
    }

    ctx.Response.ContentType = "application/json; charset=utf-8";
    await JsonSerializer.SerializeAsync(ctx.Response.Body, filtered, cancellationToken: ctx.RequestAborted);
}

app.MapGet("/", () => Results.Text("ok"));

// ----------------------------------------------------
// GET BLOB: /files/{md5}
// ----------------------------------------------------
app.MapGet("/files/{md5}", async (HttpContext ctx, string md5) =>
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
app.MapGet("/meta/{md5}", async (HttpContext ctx, string md5) =>
{
    var fields = ctx.Request.Query["fields"].ToString().Split(',', StringSplitOptions.RemoveEmptyEntries);
    if (fields.Length == 0)
    {
        ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
        return;
    }

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
    await SendFilteredJsonAsync(ctx, metaPath, fields);
});

// ----------------------------------------------------
// GET META (SSD preferred): /meta-range/{rangeId}?fields=a,b,c,d,e
// ----------------------------------------------------
app.MapGet("/meta-range/{rangeId}", async (HttpContext ctx, [FromRoute]int rangeId, [FromQuery]string[] fields) =>
{
    if (rangeId < 0 || rangeId > 255)
    {
        ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
        return;
    }

    var partition = rangeId.GetPartition();

    if (fields.Length == 0)
    {
        ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
        return;
    }

    ctx.Response.ContentType = "application/json; charset=utf-8";
    await using var writer = new Utf8JsonWriter(ctx.Response.Body);
    writer.WriteStartArray();

    var seenFiles = new HashSet<string>();
    
    async Task ProcessDirectory(string root)
    {
        var partitionDir = Path.Combine(root, partition);
        if (!Directory.Exists(partitionDir)) return;

        foreach (var subDir in Directory.EnumerateDirectories(partitionDir))
        {
            foreach (var metaFile in Directory.EnumerateFiles(subDir, "*.json"))
            {
                var fileName = Path.GetFileName(metaFile);
                if (seenFiles.Add(fileName))
                {
                    await using var fs = new FileStream(metaFile, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, true);
                    var node = await JsonNode.ParseAsync(fs, cancellationToken: ctx.RequestAborted);
                    if (node is JsonObject obj)
                    {
                        var filtered = new JsonObject();
                        foreach (var field in fields)
                        {
                            if (obj.TryGetPropertyValue(field, out var value))
                            {
                                filtered[field] = value?.DeepClone();
                            }
                        }
                        JsonSerializer.Serialize(writer, filtered);
                    }
                }
            }
        }
    }

    await ProcessDirectory(cfg.SsdMetaRoot);
    
    var hddRoot = cfg.HddRoots.SelectHddRootByPartition(partition);
    await ProcessDirectory(hddRoot);

    writer.WriteEndArray();
    await writer.FlushAsync(ctx.RequestAborted);
});


app.Urls.Clear();

app.Run();
