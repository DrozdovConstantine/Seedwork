using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using SeedWork.Messaging;
using SeedWork.Messaging.Kafka;
using SeedWork.Messaging.RabbitMQ;

var builder = Host.CreateApplicationBuilder(args);
// Один тип обработчика используется для обоих брокеров; каждый endpoint имеет собственное имя и подписку.
builder.Services.AddSeedWorkMessaging(b =>
{
    // RabbitMQ создаёт входную очередь, binding и error queue; этот сервис запускается до издателя.
    b.AddRabbitMq("rabbit", r => r.Configure(o =>
    {
        o.Connection.Uri = new Uri(builder.Configuration["RabbitMQ"] ?? "amqp://guest:guest@localhost:5673/");
        o.Topology = TopologyMode.CreateMissing;
    }).AddConsumersFromAssembly(typeof(OrderConsumer).Assembly, builder.Configuration));
    // Экземпляры сервиса с одной группой billing-v1 делят партиции между собой.
    b.AddKafka("kafka", k => k.Configure(o =>
    {
        o.Client.BootstrapServers = builder.Configuration["Kafka"] ?? "127.0.0.1:19092";
        o.Topology = TopologyMode.CreateMissing;
    }).AddConsumersFromAssembly(typeof(OrderConsumer).Assembly, builder.Configuration));
});
// Подключаем общий источник и источники адаптеров; связь с издателем восстанавливается из headers.
builder.Services.AddOpenTelemetry().ConfigureResource(r => r.AddService("seedwork-consumer"))
    .WithTracing(t => t.AddSource(MessagingTelemetry.InstrumentationName,
        RabbitMqTransport.InstrumentationName, KafkaTransport.InstrumentationName).AddOtlpExporter())
    .WithMetrics(m => m.AddMeter(MessagingTelemetry.InstrumentationName,
        RabbitMqTransport.InstrumentationName, KafkaTransport.InstrumentationName).AddOtlpExporter());
builder.Logging.AddOpenTelemetry(o => o.AddOtlpExporter());
using var host = builder.Build();
var runSeconds = builder.Configuration.GetValue("RunSeconds", 0);
using var stop = runSeconds > 0 ? new CancellationTokenSource(TimeSpan.FromSeconds(runSeconds)) : new CancellationTokenSource();
await host.RunAsync(stop.Token);

// Независимая модель получателя: необязательное поле совместимо с JSON первой версии издателя.
public sealed record OrderSubmitted(Guid Id, decimal Amount, string Currency = "RUB");
// Простая подписка RabbitMQ задаётся атрибутом; отдельная конфигурация ниже уточняет её политику повторов.
[RabbitMqConsumer(typeof(OrderSubmitted), Endpoint = "orders-rabbit", Queue = "billing.orders.v1", Exchange = "orders.v1", ExchangeType = RabbitMqExchangeType.Topic, MaxRetries = 3, Exponential = true)]
public sealed class OrderConsumer(ILogger<OrderConsumer> logger) : IConsumer<OrderSubmitted>
{
    public Task ConsumeAsync(MessageContext<OrderSubmitted> context, CancellationToken cancellationToken)
    {
        // Ack/commit выполняет адаптер после завершения метода; прикладные действия должны учитывать повторную доставку.
        logger.LogInformation("Received order {OrderId} through {Endpoint}, attempt {Attempt}, currency {Currency}",
            context.Message.Id, context.Endpoint, context.Attempt, context.Message.Currency);
        return Task.CompletedTask;
    }
}

// Значения атрибута уже заполнены: меняем только повторы, сохраняя привязку очереди.
public sealed class OrderRabbitConfiguration : RabbitMqConsumerConfiguration<OrderSubmitted, OrderConsumer>
{
    public override void Configure(RabbitMqConsumerOptions options, IConfiguration configuration)
    {
        options.Retry.MaxRetries = configuration.GetValue("Messaging:RabbitRetries", 3);
        options.Retry.Exponential = true;
    }
}

// Kafka использует только класс конфигурации; группа может задаваться настройками приложения.
public sealed class OrderKafkaConfiguration : KafkaConsumerConfiguration<OrderSubmitted, OrderConsumer>
{
    public override void Configure(KafkaConsumerOptions options, IConfiguration configuration)
    {
        options.Endpoint = "orders-kafka";
        options.Topic = "orders.v1";
        options.Partitions = 2;
        options.ReplicationFactor = 1;
        options.Group = configuration["Messaging:KafkaGroup"] ?? "billing-v1";
        // Без повторов первая ошибка сразу приостанавливает партицию.
        options.Retry = RetryOptions.NoRetry();
    }
}
