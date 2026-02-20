namespace XStorage.Common;

public static class EnvironmentVariableExtensions
{
    extension(string variableName)
    {
        public string FromEnvAsString() =>
            Environment.GetEnvironmentVariable(variableName) 
            ?? throw new InvalidOperationException($"Environment variable {variableName} is not set");

        public int FromEnvAsInt() =>
            int.Parse(variableName.FromEnvAsString());
    }
}