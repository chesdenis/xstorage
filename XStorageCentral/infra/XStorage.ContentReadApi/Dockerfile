FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS base
USER $APP_UID
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["infra/XStorage.ContentReadApi/XStorage.ContentReadApi.csproj", "infra/XStorage.ContentReadApi/"]
RUN dotnet restore "infra/XStorage.ContentReadApi/XStorage.ContentReadApi.csproj"
COPY . .
WORKDIR "/src/infra/XStorage.ContentReadApi"
RUN dotnet build "./XStorage.ContentReadApi.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./XStorage.ContentReadApi.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "XStorage.ContentReadApi.dll"]

ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080

ENTRYPOINT ["dotnet", "XStorage.ContentReadApi.dll"]
