using System.Diagnostics;
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
// Оба транспорта регистрируются в одной шине; выбор брокера задаётся именем маршрута при публикации.
builder.Services.AddSeedWorkMessaging(b =>
{
    b.AddRabbitMq("rabbit", r => r.Configure(o =>
    {
        o.Connection.Uri = new Uri(builder.Configuration["RabbitMQ"] ?? "amqp://guest:guest@localhost:5673/");
        o.Topology = TopologyMode.CreateMissing;
    }).Exchange("orders.v1").Publish<OrderSubmitted>("orders-rabbit", "orders.v1", "submitted"));
    b.AddKafka("kafka", k => k.Configure(o =>
    {
        o.Client.BootstrapServers = builder.Configuration["Kafka"] ?? "127.0.0.1:19092";
        o.Topology = TopologyMode.CreateMissing;
    }).Topic("orders.v1", 2, 1).Publish<OrderSubmitted>("orders-kafka", "orders.v1"));
});
// Пример сам настраивает экспорт в Aspire Dashboard; при Service Defaults экспортёр повторно не добавляют.
builder.Services.AddOpenTelemetry().ConfigureResource(r => r.AddService("seedwork-publisher"))
    .WithTracing(t => t.AddSource("Sample.Orders", MessagingTelemetry.InstrumentationName,
        RabbitMqTransport.InstrumentationName, KafkaTransport.InstrumentationName).AddOtlpExporter())
    .WithMetrics(m => m.AddMeter(MessagingTelemetry.InstrumentationName,
        RabbitMqTransport.InstrumentationName, KafkaTransport.InstrumentationName).AddOtlpExporter());
builder.Logging.AddOpenTelemetry(o => o.AddOtlpExporter());
using var host = builder.Build();
// До первой публикации host должен открыть подключения и подготовить топологию.
await host.StartAsync();
try
{
    using var source = new ActivitySource("Sample.Orders");
    var publisher = host.Services.GetRequiredService<IMessagePublisher>();
    var lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();
    var count = builder.Configuration.GetValue("Count", 5);
    for (var i = 0; i < count; i++)
    {
        // Обе публикации и обработчики в другом сервисе будут связаны одной трассой.
        using var activity = source.StartActivity("submit-order");
        var message = new OrderSubmitted(Guid.NewGuid(), 100 + i);
        // В Kafka ключ выбирает партицию, а в RabbitMQ переопределяет routing key; подписка примера принимает все ключи.
        var options = new PublishOptions { CorrelationId = message.Id.ToString(), Key = message.Id.ToString() };
        // Это две независимые отправки: сбой второй не отменяет уже подтверждённую первую.
        await publisher.PublishAsync("orders-rabbit", message, options, lifetime.ApplicationStopping);
        await publisher.PublishAsync("orders-kafka", message, options, lifetime.ApplicationStopping);
        Console.WriteLine($"Published order {message.Id} to both transports.");
    }
}
finally { await host.StopAsync(); }

// Контракт принадлежит сервису-издателю; общей сборки бизнес-моделей у сервисов нет.
public sealed record OrderSubmitted(Guid Id, decimal Amount);
