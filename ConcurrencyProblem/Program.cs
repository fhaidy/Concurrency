using Azure.Messaging.ServiceBus;
using System.Text.Json;
using Confluent.Kafka;

public class InventoryMessage
{
    public string ProductId { get; set; } = default!;
    public int Quantity { get; set; }
}

// InventoryService simates database
public class InventoryService
{
    private readonly Dictionary<string, int> _stock = new()
    {
        ["product-1"] = 10
    };

    public Task<bool> DecreaseStockAsync(string productId, int quantity)
    {
        Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [Product {productId}] Checking stock...");
        var currentStock = _stock.GetValueOrDefault(productId);

        if (currentStock >= quantity)
        {
            Thread.Sleep(1000); //force delay to cause concurrency
            _stock[productId] -= quantity;
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [Product {productId}] Stock decreased. Remaining: {_stock[productId]}");
            return Task.FromResult(true);
        }

        Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [Product {productId}] Insufficient stock.");
        return Task.FromResult(false);
    }
}

class Program
{
    #region AzureServiceBus
    // private const string ConnectionString = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";
    // private const string QueueName = "queue.1";
    // private static readonly InventoryService InventoryService = new();
    //
    // static async Task Main()
    // {
    //     await SendTestMessagesAsync();
    //     await StartSessionProcessorAsync();
    // }
    //
    // static async Task SendTestMessagesAsync()
    // {
    //     var client = new ServiceBusClient(ConnectionString);
    //     var sender = client.CreateSender(QueueName);
    //
    //     var messages = new[]
    //     {
    //         new InventoryMessage { ProductId = "product-1", Quantity = 10 },
    //         new InventoryMessage { ProductId = "product-1", Quantity = 10 }
    //     };
    //     
    //     var sbMessages = messages.Select(message => new ServiceBusMessage(JsonSerializer.Serialize(message))
    //     {
    //         SessionId = message.ProductId
    //     });
    //
    //     foreach (var msg in sbMessages)
    //     {
    //         await sender.SendMessageAsync(msg);
    //         Console.WriteLine($"Sent message for {msg.SessionId} to reduce 10 units.");
    //     }
    // }
    //
    // static async Task StartSessionProcessorAsync()
    // {
    //     var client = new ServiceBusClient(ConnectionString);
    //
    //     var options = new ServiceBusSessionProcessorOptions
    //     {
    //         MaxConcurrentSessions = 5,
    //         AutoCompleteMessages = false
    //     };
    //
    //     var processor = client.CreateSessionProcessor(QueueName, options);
    //
    //     processor.ProcessMessageAsync += async args =>
    //     {
    //         var msgBody = args.Message.Body.ToString();
    //         var message = JsonSerializer.Deserialize<InventoryMessage>(msgBody)!;
    //
    //         await InventoryService.DecreaseStockAsync(message.ProductId, message.Quantity);
    //
    //         await args.CompleteMessageAsync(args.Message);
    //     };
    //
    //     processor.ProcessErrorAsync += args =>
    //     {
    //         Console.WriteLine($"Error: {args.Exception.Message}");
    //         return Task.CompletedTask;
    //     };
    //
    //     await processor.StartProcessingAsync();
    //
    //     Console.WriteLine("Processing messages. Press any key to exit.");
    //     Console.ReadKey();
    //     await processor.StopProcessingAsync();
    // }
    #endregion

    #region Kafka

    const string bootstrapServers = "localhost:9092";
    const string topic = "inventory-events";

    static readonly InventoryService inventoryService = new();

    static async Task Main()
    {
        await ProduceMessagesAsync();
        
        ConsumeMessages();
    }

    static async Task ProduceMessagesAsync()
    {
        var config = new ProducerConfig { BootstrapServers = bootstrapServers };

        using var producer = new ProducerBuilder<string, string>(config).Build();

        var messages = new[]
        {
            new InventoryMessage { ProductId = "product-1", Quantity = 10 },
            new InventoryMessage { ProductId = "product-1", Quantity = 10 }
        };

        foreach (var msg in messages)
        {
            var json = JsonSerializer.Serialize(msg);
            await producer.ProduceAsync(topic, new Message<string, string>
            {
                Key = msg.ProductId, // partition by product
                Value = json
            });

            Console.WriteLine($"Sent Kafka message for {msg.ProductId} - {msg.Quantity} units.");
        }

        producer.Flush(TimeSpan.FromSeconds(5));
    }

    static void ConsumeMessages()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = "inventory-consumer-group",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(topic);

        Console.WriteLine("Consuming messages... Press Ctrl+C to stop.");

        try
        {
            while (true)
            {
                var cr = consumer.Consume();
                var message = JsonSerializer.Deserialize<InventoryMessage>(cr.Message.Value)!;
                inventoryService.DecreaseStockAsync(message.ProductId, message.Quantity).Wait();
            }
        }
        catch (OperationCanceledException)
        {
            consumer.Close();
        }
    }

    #endregion
}
