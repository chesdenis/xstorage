using Microsoft.AspNetCore.Mvc;

namespace XStorage.RestApi;

public static class AdaptersExtensions
{
    public static IEndpointRouteBuilder MapBackboneEndpoints(this IEndpointRouteBuilder app)
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

