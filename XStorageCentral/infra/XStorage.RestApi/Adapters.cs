using Microsoft.AspNetCore.Mvc;

namespace XStorage.RestApi;

public sealed record StorageQueryArgs
{
    [FromQuery(Name = "q")] public string? Q { get; init; }
    [FromQuery(Name = "partition")] public string? Partition { get; init; }
    [FromQuery(Name = "section")] public string? Section { get; init; }
    
    // ?tag=..&tag=..
    [FromQuery(Name = "tag")] public string[]? Tag { get; init; }

    // ?id=..&id=..
    [FromQuery(Name = "id")] public string[]? Id { get; init; }

    // ?sel=..&sel=..
    [FromQuery(Name = "sel")] public string[]? Sel { get; init; }

    [FromQuery(Name = "skip")] public int? Skip { get; init; }
    [FromQuery(Name = "take")] public int? Take { get; init; }
}

public record StorageResult(string Result);

public interface IQueryAdapter
{
    Task<StorageResult> HandleAsync(StorageQueryArgs queryArgs, CancellationToken ct);
}

public interface IUpsertAdapter : IQueryAdapter
{
    Task UpsertMetadataAsync(object metadata, CancellationToken ct);
    Task UpsertDescriptionAsync(object metadata, CancellationToken ct);
    Task UpsertEngShortAsync(object metadata, CancellationToken ct);
    Task UpsertEng30TagsAsync(object metadata, CancellationToken ct);
    Task UpsertCommerceMarkAsync(object metadata, CancellationToken ct);
    Task UpsertEmbAsync(object metadata, CancellationToken ct);
    Task UpsertPreviewAsync(object metadata, CancellationToken ct);
}

public interface IFilterAdapter : IQueryAdapter
{
}

public interface IIdsAdapter : IQueryAdapter
{
}

public interface IPartitionsAdapter : IQueryAdapter
{
}

public interface ISearchAdapter : IQueryAdapter
{
}

public interface ISectionsAdapter : IQueryAdapter
{
}

public interface ISelectionsAdapter : IQueryAdapter
{
}

internal abstract class NotImplementedAdapterBase : IQueryAdapter
{
    public Task<StorageResult> HandleAsync(StorageQueryArgs args, CancellationToken ct) =>
        throw new NotImplementedException("Adapter not implemented yet.");
}

internal sealed class NotImplementedFilterAdapter : NotImplementedAdapterBase, IFilterAdapter { }
internal sealed class NotImplementedSearchAdapter : NotImplementedAdapterBase, ISearchAdapter { }
internal sealed class NotImplementedPartitionsAdapter : NotImplementedAdapterBase, IPartitionsAdapter { }
internal sealed class NotImplementedSectionsAdapter : NotImplementedAdapterBase, ISectionsAdapter { }
internal sealed class NotImplementedIdsAdapter : NotImplementedAdapterBase, IIdsAdapter { }
internal sealed class NotImplementedSelectionsAdapter : NotImplementedAdapterBase, ISelectionsAdapter { }

internal sealed class NotImplementedUpsertAdapter : IUpsertAdapter
{
    public Task<StorageResult> HandleAsync(StorageQueryArgs queryArgs, CancellationToken ct)
    {
        throw new NotImplementedException();
    }

    public Task UpsertMetadataAsync(object metadata, CancellationToken ct)
    {
        throw new NotImplementedException();
    }

    public Task UpsertDescriptionAsync(object metadata, CancellationToken ct)
    {
        throw new NotImplementedException();
    }

    public Task UpsertEngShortAsync(object metadata, CancellationToken ct)
    {
        throw new NotImplementedException();
    }

    public Task UpsertEng30TagsAsync(object metadata, CancellationToken ct)
    {
        throw new NotImplementedException();
    }

    public Task UpsertCommerceMarkAsync(object metadata, CancellationToken ct)
    {
        throw new NotImplementedException();
    }

    public Task UpsertEmbAsync(object metadata, CancellationToken ct)
    {
        throw new NotImplementedException();
    }

    public Task UpsertPreviewAsync(object metadata, CancellationToken ct)
    {
        throw new NotImplementedException();
    }
}