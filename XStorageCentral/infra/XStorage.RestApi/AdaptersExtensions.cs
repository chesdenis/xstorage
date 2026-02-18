using Microsoft.AspNetCore.Mvc;

namespace XStorage.RestApi;

public static class AdaptersExtensions
{
    public static IEndpointRouteBuilder MapUpsertEndpoints(this IEndpointRouteBuilder app)
    {
        var adapter = app.ServiceProvider.GetRequiredService<IUpsertAdapter>();
        MapUpsert(app, "/description/upsert",  adapter.UpsertDescriptionAsync);
        MapUpsert(app, "/engshort/upsert",  adapter.UpsertEngShortAsync);
        MapUpsert(app, "/30tags/upsert",  adapter.UpsertEng30TagsAsync);
        MapUpsert(app, "/metadata/upsert",  adapter.UpsertMetadataAsync);
        MapUpsert(app, "/preview/upsert",  adapter.UpsertPreviewAsync);
        MapUpsert(app, "/emb/upsert",  adapter.UpsertEmbAsync);
        return app;
    }
    
    private static void MapUpsert(IEndpointRouteBuilder app, string route, Func<object, CancellationToken, Task> upsertOperationAsync)
    {
        app.MapPost(route, async (
            [AsParameters] object metadata,
            [FromServices] IUpsertAdapter adapter,
            CancellationToken ct) =>
        {
            try
            {
                await upsertOperationAsync(metadata, ct);
                return Results.Ok();
            }
            catch (NotImplementedException ex)
            {
                return Results.Problem(title: "Not implemented", detail: ex.Message, statusCode: 501);
            }
            catch (Exception ex)
            {
                return Results.Problem(title: "Adapter error", detail: ex.Message, statusCode: 500);
            }
        });
    }
    
    public static IEndpointRouteBuilder MapReadEndpoints(this IEndpointRouteBuilder app)
    {
        // One shared query model for ALL endpoints
        // (superset is fine; adapters can ignore what they don't need)

        MapGetWithAdapter<IFilterAdapter>(app, "/filter");
        MapGetWithAdapter<ISearchAdapter>(app, "/search");
        MapGetWithAdapter<IPartitionsAdapter>(app, "/partitions");
        MapGetWithAdapter<ISectionsAdapter>(app, "/sections");
        MapGetWithAdapter<IIdsAdapter>(app, "/ids");
        MapGetWithAdapter<ISelectionsAdapter>(app, "/selections");

        return app;
    }
    
    private static void MapGetWithAdapter<TAdapter>(IEndpointRouteBuilder app, string route)
        where TAdapter : class, IQueryAdapter
    {
        app.MapGet(route, async ([AsParameters] StorageQueryArgs queryArgs, [FromServices]TAdapter adapter, CancellationToken ct) =>
        {
            try
            {
                var result = await adapter.HandleAsync(queryArgs, ct);
                return Results.Ok(result);
            }
            catch (NotImplementedException ex)
            {
                return Results.Problem(title: "Not implemented", detail: ex.Message, statusCode: 501);
            }
            catch (Exception ex)
            {
                return Results.Problem(title: "Adapter error", detail: ex.Message, statusCode: 500);
            }
        });
    }
}

