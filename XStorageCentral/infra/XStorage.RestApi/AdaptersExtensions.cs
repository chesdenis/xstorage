using Microsoft.AspNetCore.Mvc;
using XStorage.Common;

namespace XStorage.RestApi;

public static class AdaptersExtensions
{
    public static void RegisterImplementations(this WebApplicationBuilder builder)
    {
        var appType = "APP_TYPE".FromEnvAsString();

        switch (appType)
        {
            case "SYSTEM_TESTS":
            {
                throw new NotImplementedException();
            }
                break;
            case "NOT_IMPL":
            {
                builder.Services.AddSingleton<IFilterAdapter, NotImplementedFilterAdapter>();
                builder.Services.AddSingleton<ISearchAdapter, NotImplementedSearchAdapter>();
                builder.Services.AddSingleton<IPartitionsAdapter, NotImplementedPartitionsAdapter>();
                builder.Services.AddSingleton<ISectionsAdapter, NotImplementedSectionsAdapter>();
                builder.Services.AddSingleton<IIdsAdapter, NotImplementedIdsAdapter>();
                builder.Services.AddSingleton<ISelectionsAdapter, NotImplementedSelectionsAdapter>();
                builder.Services.AddSingleton<IUpsertAdapter, NotImplementedUpsertAdapter>();
            }
                break;
            case "PROD":
            {
                throw new NotImplementedException();
            }
                break;
            default:
            {
                throw new NotImplementedException();
            }
                break;
        }
    }

    /// <summary>
    /// These set of endpoints for collect data from agents into central storage. 
    /// </summary>
    /// <returns></returns>
    public static IEndpointRouteBuilder MapUpsertEndpoints(this IEndpointRouteBuilder app)
    {
        var adapter = app.ServiceProvider.GetRequiredService<IUpsertAdapter>();
        MapCommonPostEndpoint(app, "/description/upsert", adapter.UpsertDescriptionAsync);
        MapCommonPostEndpoint(app, "/engshort/upsert", adapter.UpsertEngShortAsync);
        MapCommonPostEndpoint(app, "/30tags/upsert", adapter.UpsertEng30TagsAsync);
        MapCommonPostEndpoint(app, "/metadata/upsert", adapter.UpsertMetadataAsync);
        MapCommonPostEndpoint(app, "/commerceMark/upsert", adapter.UpsertCommerceMarkAsync);
        MapCommonPostEndpoint(app, "/preview/upsert", adapter.UpsertPreviewAsync);
        MapCommonPostEndpoint(app, "/emb/upsert", adapter.UpsertEmbAsync);
        
        return app;
    }
    
    /// <summary>
    /// These endpoints to read and get data collected for other endpoints
    /// </summary>
    /// <param name="app"></param>
    /// <returns></returns>
    public static IEndpointRouteBuilder MapReadEndpoints(this IEndpointRouteBuilder app)
    {
        MapCommonReadEndpoint<IFilterAdapter>(app, "/filter");
        MapCommonReadEndpoint<ISearchAdapter>(app, "/search");
        MapCommonReadEndpoint<IPartitionsAdapter>(app, "/partitions");
        MapCommonReadEndpoint<ISectionsAdapter>(app, "/sections");
        MapCommonReadEndpoint<IIdsAdapter>(app, "/ids");
        MapCommonReadEndpoint<ISelectionsAdapter>(app, "/selections");

        return app;
    }

    private static void MapCommonReadEndpoint<TAdapter>(IEndpointRouteBuilder app, string route)
        where TAdapter : class, IQueryAdapter
    {
        app.MapGet(route,
            async ([AsParameters] StorageQueryArgs queryArgs, [FromServices] TAdapter adapter, CancellationToken ct) =>
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
    
    private static IEndpointRouteBuilder MapCommonPostEndpoint(IEndpointRouteBuilder app, string route,
        Func<object, CancellationToken, Task> upsertOperationAsync)
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

        return app;
    }

}