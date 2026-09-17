using RabbitMQDemo.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;

class PaymentService
{
    const string ServiceName = "PaymentService";
    const string ExchangeName = "poc2.order.events";
    const string QueueName = "poc2.payment.orders";
    const string DeadQueue = "poc2.payment.dead";
    const string Dlx = "poc2.payment.dlx";
    const string DeadRoutingKey = "payment.dead";
    const string InitialRoutingKey = "order.submitted";
    const string RetryRoutingKey = "order.retry.payment";
    const string TransientMode = "PaymentTransient";
    const string PermanentMode = "PaymentPermanent";

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
            ClientProvidedName = "POC2-PaymentService"
        };
        using var connection = await factory.CreateConnectionAsync();
        using var channel = await connection.CreateChannelAsync();

        await channel.ExchangeDeclareAsync(ExchangeName, ExchangeType.Topic, durable: true);
        await channel.ExchangeDeclareAsync(Dlx, ExchangeType.Direct, durable: true);

        await channel.QueueDeclareAsync(QueueName, durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?>
            {
                { "x-dead-letter-exchange", Dlx },
                { "x-dead-letter-routing-key", DeadRoutingKey }
            });
        await channel.QueueBindAsync(QueueName, ExchangeName, InitialRoutingKey);
        await channel.QueueBindAsync(QueueName, ExchangeName, RetryRoutingKey);

        await channel.QueueDeclareAsync(DeadQueue, durable: true, exclusive: false, autoDelete: false);
        await channel.QueueBindAsync(DeadQueue, Dlx, DeadRoutingKey);

        await channel.BasicQosAsync(0, prefetchCount: 1, global: false);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, ea) =>
        {
            var message = Encoding.UTF8.GetString(ea.Body.ToArray());
            var retryCount = GetRetryCount(ea.BasicProperties);
            var order = JsonSerializer.Deserialize<OrderMessage>(message);

            if (order is null)
            {
                DemoLogger.Failure(ServiceName, $"INVALID MESSAGE | retry={retryCount} | NACK -> {DeadQueue}");
                await channel.BasicNackAsync(ea.DeliveryTag, false, requeue: false);
                return;
            }

            DemoLogger.Info(ServiceName, $"RECEIVED | {order.OrderId} | retry={retryCount} | FailureMode={order.FailureMode}");

            #region DEMO UI VISIBILITY - ENABLED DELAY
            // Intentionally enabled so RabbitMQ Management UI can visibly show
            // the message moving from Ready -> Unacked -> ACK.
            await Task.Delay(TimeSpan.FromSeconds(2));
            #endregion

            try
            {
                if (ShouldFail(order.FailureMode, retryCount))
                    throw new Exception(GetFailureReason(order.FailureMode));

                Console.WriteLine($"[{ServiceName}] {order.OrderId} processed successfully.");
                DemoLogger.Success(ServiceName, $"SUCCESS | {order.OrderId} | attempt={retryCount + 1} | ACK");
                await channel.BasicAckAsync(ea.DeliveryTag, false);
            }
            catch (Exception ex)
            {
                DemoLogger.Failure(ServiceName, $"FAILED | {order.OrderId} | attempt={retryCount + 1} | {ex.Message} | NACK -> {DeadQueue}");
                await channel.BasicNackAsync(ea.DeliveryTag, false, requeue: false);
            }
        };

        await channel.BasicConsumeAsync(QueueName, autoAck: false, consumer: consumer);
        DemoLogger.Info(ServiceName, $"STARTED | consuming {QueueName} | bindings={InitialRoutingKey},{RetryRoutingKey}");
        Console.WriteLine($"{ServiceName} started. Press Enter to exit.");
        Console.ReadLine();
    }

    static int GetRetryCount(IReadOnlyBasicProperties? properties)
    {
        if (properties?.Headers is null || !properties.Headers.TryGetValue("x-retry-count", out var value))
            return 0;
        try { return Convert.ToInt32(value); } catch { return 0; }
    }

    static bool ShouldFail(string failureMode, int retryCount)
    {
        if (failureMode == PermanentMode) return true;
        if (failureMode == TransientMode) return retryCount < 2;
        return false;
    }

    static string GetFailureReason(string failureMode) => failureMode switch
    {
        PermanentMode => "Simulated permanent payment failure",
        TransientMode => "Simulated transient payment failure",
        _ => "Unknown failure"
    };
}

record OrderMessage(string OrderId, string Customer, decimal Amount, string FailureMode);
