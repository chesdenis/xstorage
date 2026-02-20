using Microsoft.AspNetCore.Mvc;
using XStorage.RestApi;

var builder = WebApplication.CreateBuilder(args);

builder.RegisterImplementations();

var app = builder.Build();

app.MapGet("/", () => "ok");

app.MapReadEndpoints();
app.MapUpsertEndpoints();

app.Run();