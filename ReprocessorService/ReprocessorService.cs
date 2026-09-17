using RabbitMQDemo.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

class ReprocessorService
{
    const string ServiceName = "ReprocessorService";
    const string ExchangeName = "poc2.order.events";
    const int MaxRetries = 3;

    static async Task Main()
    {
        var envPath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "../../../../.env"));
        if (!File.Exists(envPath))
            throw new FileNotFoundException($"RabbitMQ configuration file was not found: {envPath}");

        DotNetEnv.Env.Load(envPath);
        var factory = new ConnectionFactory
        {
            HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost",
            Port = int.TryParse(Environment.GetEnvironmentVariable("RABBITMQ_PORT"), out var port) ? port : 5675,
            UserName = Environment.GetEnvironmentVariable("RABBITMQ_USERNAME")
                ?? throw new InvalidOperationException("RABBITMQ_USERNAME environment variable is required."),
            Password = Environment.GetEnvironmentVariable("RABBITMQ_PASSWORD")
                ?? throw new InvalidOperationException("RABBITMQ_PASSWORD environment variable is required."),
            ClientProvidedName = "POC2-ReprocessorService"
        };

        using var connection = await factory.CreateConnectionAsync();
        using var channel = await connection.CreateChannelAsync();

        // POC02 uses a topic exchange so retries can be routed to the original
        // service instead of being broadcast to every service queue again.
        await channel.ExchangeDeclareAsync(ExchangeName, ExchangeType.Topic, durable: true);
        await channel.QueueDeclareAsync("poc2.common.failed.orders.queue", durable: true, exclusive: false, autoDelete: false);

        await channel.BasicQosAsync(0, prefetchCount: 1, global: false);

        var dlqs = new[] { "poc2.payment.dead", "poc2.inventory.dead", "poc2.warehouse.dead" };
        foreach (var queue in dlqs)
        {
            await channel.QueueDeclareAsync(queue, durable: true, exclusive: false, autoDelete: false);
            var sourceQueue = queue;
            var retryRoutingKey = GetRetryRoutingKey(sourceQueue);

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += async (_, ea) =>
            {
                var message = Encoding.UTF8.GetString(ea.Body.ToArray());
                var retryCount = GetRetryCount(ea.BasicProperties);
                var orderId = ExtractOrderId(message);

                DemoLogger.Info(ServiceName, $"DLQ_RECEIVED | source={sourceQueue} | order={orderId} | retry={retryCount}");

                #region DEMO UI VISIBILITY - ENABLED RETRY DELAY
                // Intentionally enabled so RabbitMQ Management UI can visibly show
                // the DLQ message while ReprocessorService is processing it.
                await Task.Delay(TimeSpan.FromSeconds(3));
                #endregion

                if (retryCount >= MaxRetries)
                {
                    var headers = CopyHeaders(ea.BasicProperties);
                    headers["x-final-status"] = "failed";
                    headers["x-failed-source-queue"] = sourceQueue;

                    var finalProps = new BasicProperties { Headers = headers };
                    await channel.BasicPublishAsync("", "poc2.common.failed.orders.queue", mandatory: false, body: ea.Body, basicProperties: finalProps);
                    DemoLogger.Failure(ServiceName, $"FINAL_FAILURE | order={orderId} | retries={retryCount} | {sourceQueue} -> poc2.common.failed.orders.queue | ACK source DLQ");
                    await channel.BasicAckAsync(ea.DeliveryTag, false);
                    return;
                }

                var propsRetry = new BasicProperties
                {
                    Headers = new Dictionary<string, object?>
                    {
                        ["x-retry-count"] = retryCount + 1,
                        ["x-retry-source"] = sourceQueue
                    }
                };

                await channel.BasicPublishAsync(ExchangeName, retryRoutingKey, mandatory: false, body: ea.Body, basicProperties: propsRetry);
                DemoLogger.Retry(ServiceName, $"REPUBLISHED | order={orderId} | retry={retryCount + 1} | routingKey={retryRoutingKey} | {sourceQueue} -> {ExchangeName} | ACK source DLQ");
                await channel.BasicAckAsync(ea.DeliveryTag, false);
            };

            await channel.BasicConsumeAsync(queue, autoAck: false, consumer: consumer);
            DemoLogger.Info(ServiceName, $"BOUND | source={queue} | retryRoutingKey={retryRoutingKey}");
        }

        DemoLogger.Info(ServiceName, "STARTED | watching poc2.payment.dead, poc2.inventory.dead, poc2.warehouse.dead");
        Console.WriteLine("ReprocessorService started. Press Enter to exit.");
        Console.ReadLine();
    }

    static string GetRetryRoutingKey(string sourceQueue) => sourceQueue switch
    {
        "poc2.payment.dead" => "order.retry.payment",
        "poc2.inventory.dead" => "order.retry.inventory",
        "poc2.warehouse.dead" => "order.retry.warehouse",
        _ => throw new InvalidOperationException($"No retry routing key configured for {sourceQueue}.")
    };

    static Dictionary<string, object?> CopyHeaders(IReadOnlyBasicProperties? properties)
    {
        var headers = new Dictionary<string, object?>();
        if (properties?.Headers != null)
            foreach (var h in properties.Headers)
                headers[h.Key] = h.Value;
        return headers;
    }

    static int GetRetryCount(IReadOnlyBasicProperties? properties)
    {
        if (properties?.Headers is null || !properties.Headers.TryGetValue("x-retry-count", out var value))
            return 0;
        try { return Convert.ToInt32(value); } catch { return 0; }
    }

    static string ExtractOrderId(string message)
    {
        const string marker = "OrderId";
        var index = message.IndexOf(marker, StringComparison.OrdinalIgnoreCase);
        if (index < 0) return "unknown";
        var colon = message.IndexOf(':', index);
        if (colon < 0) return "unknown";
        var start = colon + 1;
        while (start < message.Length && (message[start] == ' ' || message[start] == '"')) start++;
        var end = start;
        while (end < message.Length && message[end] != '"' && message[end] != ',' && !char.IsWhiteSpace(message[end])) end++;
        return message[start..end];
    }
}
