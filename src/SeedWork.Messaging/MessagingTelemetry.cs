using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace SeedWork.Messaging;

/// <summary>Общие источники трасс и метрик; регистрацию в OpenTelemetry и экспорт настраивает приложение.</summary>
public static class MessagingTelemetry
{
    public const string InstrumentationName = "SeedWork.Messaging";
    public static readonly ActivitySource Activities = new(InstrumentationName);
    public static readonly Meter Meter = new(InstrumentationName);
    public static readonly Counter<long> Published = Meter.CreateCounter<long>("messaging.published");
    public static readonly Counter<long> PublishErrors = Meter.CreateCounter<long>("messaging.publish.errors");
    public static readonly Histogram<double> PublishDuration = Meter.CreateHistogram<double>("messaging.publish.duration", "s");
    public static readonly Counter<long> Consumed = Meter.CreateCounter<long>("messaging.consumed");
    public static readonly Counter<long> Retries = Meter.CreateCounter<long>("messaging.retries");
    public static readonly Counter<long> Errors = Meter.CreateCounter<long>("messaging.errors");
    public static readonly Histogram<double> ConsumeDuration = Meter.CreateHistogram<double>("messaging.consume.duration", "s");

    /// <summary>Стабильные измерения метрик без идентификаторов и содержимого отдельных сообщений.</summary>
    public static TagList Tags(string connection, string endpoint) => new()
    {
        { "messaging.connection", connection }, { "messaging.destination.name", endpoint }
    };

    /// <summary>Создаёт span обработки с удалённым родительским контекстом из заголовков сообщения.</summary>
    public static Activity? StartConsumer(string endpoint, TransportMessage message)
    {
        message.Headers.TryGetValue(MessageHeaders.TraceParent, out var parent);
        message.Headers.TryGetValue(MessageHeaders.TraceState, out var state);
        ActivityContext.TryParse(parent, state, isRemote: true, out var context);
        return Activities.StartActivity(endpoint + " process", ActivityKind.Consumer, context);
    }
}
