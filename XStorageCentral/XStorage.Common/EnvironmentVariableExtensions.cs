namespace XStorage.Common;

public static class EnvironmentVariableExtensions
{
    public static string ResolveFromEnv(this string variableName) =>
        Environment.GetEnvironmentVariable(variableName) 
        ?? throw new InvalidOperationException($"Environment variable {variableName} is not set");
    
    public static int AsInt(this string variableName) =>
        int.Parse(variableName.ResolveFromEnv());
}