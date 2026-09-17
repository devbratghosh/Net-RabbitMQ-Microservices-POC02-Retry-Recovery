using RabbitMQDemo.Logging;
using RabbitMQ.Client;
using System.Text;
using System.Text.Json;

namespace OrderProducer;

class Program
{
    const string ExchangeName = "poc2.order.events";
    const string PublishRoutingKey = "order.submitted";

    static async Task Main(string[] args)
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
            ClientProvidedName = "POC2-OrderProducer" 
        };

        using var connection = await factory.CreateConnectionAsync();
        using var channel = await connection.CreateChannelAsync();

        // Topic exchange is used so the same OrderSubmitted event can be broadcast
        // to all interested services while retries can be routed to one service.
        await channel.ExchangeDeclareAsync(ExchangeName, ExchangeType.Topic, durable: true);

        Console.WriteLine("============================================================");
        Console.WriteLine(" RabbitMQ E-Commerce Demo Producer");
        Console.WriteLine("============================================================");
        Console.WriteLine("Press ENTER to publish the deterministic 10-order demo batch.");
        Console.WriteLine("Press Q + ENTER to exit.");
        DemoLogger.Info("OrderProducer", $"STARTED | publishing to {ExchangeName} | routingKey={PublishRoutingKey}");

        while (true)
        {
            var input = Console.ReadLine();
            if (string.Equals(input, "q", StringComparison.OrdinalIgnoreCase))
                break;

            var orders = BuildDemoBatch();
            Console.WriteLine($"\nPublishing {orders.Count} orders...\n");

            foreach (var order in orders)
            {
                var json = JsonSerializer.Serialize(order);
                var body = Encoding.UTF8.GetBytes(json);
                await channel.BasicPublishAsync(ExchangeName, PublishRoutingKey, body: body);

                var description = $"{order.OrderId} Customer={order.Customer} Amount={order.Amount:F2} FailureMode={order.FailureMode}";
                DemoLogger.Info("OrderProducer", $"PUBLISHED | routingKey={PublishRoutingKey} | {description}");
            }

            DemoLogger.Success("OrderProducer", $"BATCH_COMPLETE | published={orders.Count} orders");
            Console.WriteLine("\nBatch published. Watch the service consoles and RabbitMQ UI.");
            Console.WriteLine("Press ENTER for another batch or Q + ENTER to exit.\n");
        }
    }

    static List<OrderMessage> BuildDemoBatch() => new()
    {
        new("ORD-1001", "Alice",   120.00m, "None"),
        new("ORD-1002", "Bob",     245.50m, "PaymentTransient"),
        new("ORD-1003", "Charlie",  89.99m, "None"),
        new("ORD-1004", "David",   175.25m, "InventoryTransient"),
        new("ORD-1005", "Eva",     310.00m, "WarehousePermanent"),
        new("ORD-1006", "Frank",    59.95m, "None"),
        new("ORD-1007", "Grace",   450.00m, "PaymentPermanent"),
        new("ORD-1008", "Henry",   135.75m, "None"),
        new("ORD-1009", "Isha",    220.40m, "WarehouseTransient"),
        new("ORD-1010", "Jack",     75.00m, "None")
    };
}

record OrderMessage(string OrderId, string Customer, decimal Amount, string FailureMode);
