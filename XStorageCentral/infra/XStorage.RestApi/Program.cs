using Microsoft.AspNetCore.Mvc;
using XStorage.RestApi;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<IFilterAdapter, NotImplementedFilterAdapter>();
builder.Services.AddSingleton<ISearchAdapter, NotImplementedSearchAdapter>();
builder.Services.AddSingleton<IPartitionsAdapter, NotImplementedPartitionsAdapter>();
builder.Services.AddSingleton<ISectionsAdapter, NotImplementedSectionsAdapter>();
builder.Services.AddSingleton<IIdsAdapter, NotImplementedIdsAdapter>();
builder.Services.AddSingleton<ISelectionsAdapter, NotImplementedSelectionsAdapter>();

builder.Services.AddSingleton<IUpsertAdapter, NotImplementedUpsertAdapter>();

var app = builder.Build();

app.MapGet("/", () => "ok");

app.MapReadEndpoints();
app.MapUpsertEndpoints();

app.Run();