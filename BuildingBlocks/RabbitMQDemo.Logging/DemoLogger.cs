using System.Text;

namespace RabbitMQDemo.Logging;

/// <summary>
/// Lightweight shared logger for the RabbitMQ POC.
/// All applications write to one project-level text log so the complete
/// message lifecycle can be reviewed in chronological order.
/// </summary>
public static class DemoLogger
{
    private static readonly object Sync = new();
    private static readonly string LogFile = ResolveLogFile();

    public static void Info(string service, string message) => Write(service, "INFO", message);
    public static void Success(string service, string message) => Write(service, "SUCCESS", message);
    public static void Failure(string service, string message) => Write(service, "FAILURE", message);
    public static void Retry(string service, string message) => Write(service, "RETRY", message);

    private static void Write(string service, string level, string message)
    {
        var line = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} | {service,-16} | {level,-7} | {message}";
        var directory = Path.GetDirectoryName(LogFile);

        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);

        lock (Sync)
        {
            // FileShare.ReadWrite allows all POC processes to append to the same file.
            using var stream = new FileStream(LogFile, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
            using var writer = new StreamWriter(stream, new UTF8Encoding(false));
            writer.WriteLine(line);
        }

        Console.WriteLine(line);
    }

    private static string ResolveLogFile()
    {
        // Optional override for Docker/CI/other environments.
        var configured = Environment.GetEnvironmentVariable("RABBITMQ_DEMO_LOG_FILE");
        if (!string.IsNullOrWhiteSpace(configured))
            return Path.GetFullPath(configured);

        // dotnet run starts each service from its own project directory. Walk upward
        // from the compiled application directory until the solution file is found.
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory != null)
        {
            var solution = Path.Combine(directory.FullName, "ECommerceAppPoc2.slnx");
            if (File.Exists(solution))
                return Path.Combine(directory.FullName, "logs", "RabbitMQ-Demo.txt");

            directory = directory.Parent;
        }

        // Fallback for unusual hosting environments where the solution file is absent.
        return Path.Combine(Directory.GetCurrentDirectory(), "logs", "RabbitMQ-Demo.txt");
    }
}
