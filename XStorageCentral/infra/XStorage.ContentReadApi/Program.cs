using System.Buffers;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using XStorage.Common;
using XStorage.Common.Caching;
using XStorage.Common.ReadPatterns;
using XStorage.ContentReadApi;

var builder = WebApplication.CreateBuilder(args);

// builder.Logging.ClearProviders();
// builder.Logging.SetMinimumLevel(LogLevel.None);

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

foreach (var r in cfg.HddRoots) Directory.CreateDirectory(r);
Directory.CreateDirectory(cfg.SsdMetaRoot);

app.MapGet("/", () => Results.Text("ok"));

// ----------------------------------------------------
// GET BLOB: /files/{md5}
// ----------------------------------------------------
app.MapGet("/files/{md5}", async (HttpContext ctx, [FromRoute]string md5) =>
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
// GET META: /meta/{md5}?fields=a&fields=b
// ----------------------------------------------------
app.MapGet("/meta/{md5}", async (HttpContext ctx, [FromRoute]string md5, [FromQuery] string[] fields) =>
{
    // we do not support when fields collection is empty, ie to avoid full meta loading
    if (fields.Length == 0)
    {
        ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
        return;
    }
    
    // Resolve actual read path using layers: SSD preferred, then mapped HDD
    var pattern = BuildMetaAccessPattern(md5);
    
    // Sentinel source: "" means not found anywhere
    var metaPath = await pattern.ExecuteAsync(
        source: () => Task.FromResult(string.Empty),
        ct: ctx.RequestAborted);
    
    if (string.IsNullOrEmpty(metaPath))
    {
        ctx.Response.StatusCode = StatusCodes.Status404NotFound;
        return;
    }
    
    var fieldSet = new HashSet<string>(fields, StringComparer.Ordinal);
    await using var body = ctx.Response.BodyWriter.AsStream();
    await using var writer = new Utf8JsonWriter(body, new JsonWriterOptions { Indented = false, SkipValidation = true });
    
    writer.WriteStartArray();
    
    // Write filtered object (best effort)
    await WriteFilteredMetaObjectAsync(writer, metaPath, md5, fieldSet, ctx.RequestAborted);
    
    writer.WriteEndArray();
    await writer.FlushAsync(ctx.RequestAborted);
});

// ----------------------------------------------------
// GET PREVIEW: /previews/{md5}
// ----------------------------------------------------
app.MapGet("/previews/{md5}", async (HttpContext ctx, [FromRoute]string md5) =>
{
    // Resolve actual read path using layers: SSD preferred, then mapped HDD
    var pattern = BuildMetaAccessPattern(md5);
    
    // Sentinel source: "" means not found anywhere
    var metaPath = await pattern.ExecuteAsync(
        source: () => Task.FromResult(string.Empty),
        ct: ctx.RequestAborted);

    var previewPath = metaPath + ".previews.json";
    
    if (string.IsNullOrEmpty(previewPath))
    {
        ctx.Response.StatusCode = StatusCodes.Status404NotFound;
        return;
    }
    await using var body = ctx.Response.BodyWriter.AsStream();
    await using var writer = new Utf8JsonWriter(body, new JsonWriterOptions { Indented = false, SkipValidation = true });
    
    writer.WriteStartArray();
    
    // Write filtered object (best effort)
    await WriteFilteredMetaObjectAsync(writer, previewPath, md5, ["previews"], ctx.RequestAborted);
    
    writer.WriteEndArray();
    await writer.FlushAsync(ctx.RequestAborted);
});

// ----------------------------------------------------
// GET RANGE OF META with selected fields: /meta-range/{rangeId}?fields=a,b,c,d,e
// ----------------------------------------------------
app.MapGet("/meta-range/{rangeId}", async (HttpContext ctx, [FromRoute]int rangeId, [FromQuery]string[] fields) =>
{
    if (rangeId < 0 || rangeId > 255)
    {
        ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
        return;
    }

    var partition = rangeId.GetPartition();
    var hddSource = cfg.HddRoots.SelectHddRootByPartition(partition);
    var partitionPath = Path.Combine(hddSource, partition);

    // we do not support when fields collection is empty, ie to avoid full meta loading
    if (fields.Length == 0)
    {
        ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
        return;
    }
    
    var fieldSet = new HashSet<string>(fields, StringComparer.Ordinal);
    
    // building partition index, from HDDs
    // this can help to understand full map before we read from everything else
    var md5HashSet = StorageWalker.BuildMd5HashMap(partitionPath);
    
    ctx.Response.ContentType = "application/json; charset=utf-8";
    await using var body = ctx.Response.BodyWriter.AsStream();
    await using var writer = new Utf8JsonWriter(body, new JsonWriterOptions { Indented = false, SkipValidation = true });

    writer.WriteStartArray();
    
    foreach (var md5 in md5HashSet)
    {
        if (ctx.RequestAborted.IsCancellationRequested) break;

        // Resolve actual read path using layers: SSD preferred, then mapped HDD
        var pattern = BuildMetaAccessPattern(md5);

        // Sentinel source: "" means not found anywhere
        var metaPath = await pattern.ExecuteAsync(
            source: () => Task.FromResult(string.Empty),
            ct: ctx.RequestAborted);
        
        // Write filtered object (best effort)
        await WriteFilteredMetaObjectAsync(writer, metaPath, md5, fieldSet, ctx.RequestAborted);
    }
    
    writer.WriteEndArray();
    await writer.FlushAsync(ctx.RequestAborted);

});


app.Urls.Clear();

app.Run();

AsyncReadPattern<string> BuildMetaAccessPattern(string md5)
{
    var ssdPath = cfg.SsdMetaRoot.GetMetaPath(md5);
    var hddRoot = cfg.HddRoots.SelectHddRoot(md5);
    var hddPath = hddRoot.GetMetaPath(md5);
    
    IAsyncReadLayer<string> LO() => new AsyncLayer<string>("SSD",
        tryReadAsync: ct =>
            new ValueTask<CacheResult<string>>(
                File.Exists(ssdPath)
                    ? CacheResult<string>.HitResult(ssdPath)
                    : CacheResult<string>.MissResult()),
        setAsync: async (fromPath, ct) =>
        {
            // promote from HDD path -> SSD path
            try
            {
                // first step is to just copy metafile
                if (File.Exists(ssdPath)) return;

                Directory.CreateDirectory(Path.GetDirectoryName((string?)ssdPath)!);
                var tmp = ssdPath + ".tmp";
                File.Copy(fromPath, tmp, overwrite: true);
                File.Move(tmp, ssdPath, overwrite: true);
                
                // second step is to split previews and general metadata.
                // this is because previews content are 99% of size of metafile, but they
                // must be accessible via separate isolated endpoint
                
                var bytes = await File.ReadAllBytesAsync(ssdPath, ct);

                using var doc = JsonDocument.Parse(bytes);

                // we dont work with not supported jsons
                if (doc.RootElement.ValueKind != JsonValueKind.Object)
                    return;

                var previews = await BuildJsonWithSelectedFields(doc, md5, ["previews"]);
                var meta = await BuildJsonAndExcludeFields(doc, md5, ["previews"]);

                await File.WriteAllTextAsync(ssdPath, meta);
                await File.WriteAllTextAsync(ssdPath + ".previews.json", previews);
            }
            catch
            {
                // best effort
            }
        });
    
    IAsyncReadLayer<string> L1() => new AsyncLayer<string>(
        name: "HDD",
        tryReadAsync: ct =>
            new ValueTask<CacheResult<string>>(
                File.Exists(hddPath)
                    ? CacheResult<string>.HitResult(hddPath)
                    : CacheResult<string>.MissResult()),
        setAsync: (v, ct) => ValueTask.CompletedTask // no-op
    );
    
    var asyncReadPattern = new AsyncReadPattern<string>(LO()).Then(L1());
    return asyncReadPattern;
}

static async Task<string> BuildJsonAndExcludeFields(JsonDocument doc, string md5, HashSet<string> excludedFields)
{
    await using var ms = new MemoryStream();
    await using var jsonWriter = new Utf8JsonWriter(ms, new JsonWriterOptions { Indented = false, SkipValidation = true });

    jsonWriter.WriteStartObject();
    jsonWriter.WriteString("md5", md5);

    foreach (var prop in doc.RootElement.EnumerateObject())
    {
        if (excludedFields.Contains(prop.Name))
            continue;

        jsonWriter.WritePropertyName(prop.Name);
        // This copies the value (object/array/string/number/etc.) correctly with zero custom code.
        prop.Value.WriteTo(jsonWriter);
    }

    jsonWriter.WriteEndObject();
    await jsonWriter.FlushAsync();
    ms.Seek(0, SeekOrigin.Begin);

    return Encoding.UTF8.GetString(ms.ToArray());
}

static async Task<string> BuildJsonWithSelectedFields(JsonDocument doc, string md5, HashSet<string> selectedFields)
{
    await using var ms = new MemoryStream();
    await using var jsonWriter = new Utf8JsonWriter(ms, new JsonWriterOptions { Indented = false, SkipValidation = true });

    jsonWriter.WriteStartObject();
    jsonWriter.WriteString("md5", md5);

    foreach (var prop in doc.RootElement.EnumerateObject())
    {
        if (!selectedFields.Contains(prop.Name))
            continue;

        jsonWriter.WritePropertyName(prop.Name);
        // This copies the value (object/array/string/number/etc.) correctly with zero custom code.
        prop.Value.WriteTo(jsonWriter);
    }

    jsonWriter.WriteEndObject();
    await jsonWriter.FlushAsync();
    ms.Seek(0, SeekOrigin.Begin);

    return Encoding.UTF8.GetString(ms.ToArray());
}

static async ValueTask<bool> WriteFilteredMetaObjectAsync(
    Utf8JsonWriter writer,
    string metaPath,
    string md5,
    HashSet<string> fieldSet,
    CancellationToken ct)
{
    try
    {
        var bytes = await File.ReadAllBytesAsync(metaPath, ct);

        using var doc = JsonDocument.Parse(bytes);

        if (doc.RootElement.ValueKind != JsonValueKind.Object)
            return false;

        writer.WriteStartObject();
        writer.WriteString("md5", md5);

        foreach (var prop in doc.RootElement.EnumerateObject())
        {
            if (!fieldSet.Contains(prop.Name))
                continue;

            writer.WritePropertyName(prop.Name);
            // This copies the value (object/array/string/number/etc.) correctly with zero custom code.
            prop.Value.WriteTo(writer);
        }

        writer.WriteEndObject();
        return true;
    }
    catch
    {
        return false;
    }
}