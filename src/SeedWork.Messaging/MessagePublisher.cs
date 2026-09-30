using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace SeedWork.Messaging;

internal sealed class MessagePublisher(MessagingBuilder builder, IEnumerable<IMessageTransport> transports,
    ILogger<MessagePublisher> logger) : IMessagePublisher
{
    private readonly IReadOnlyDictionary<string, IMessageTransport> _transports = transports.ToDictionary(t => t.Name);

    public async Task PublishAsync<T>(string route, T message, PublishOptions? options = null,
        CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(message);
        cancellationToken.ThrowIfCancellationRequested();
        // Тип определяется конфигурацией маршрута, а не данными входящего сообщения или именем CLR-типа.
        if (!builder.Routes.TryGetValue(route, out var target) || target.MessageType != typeof(T))
            throw new ArgumentException($"Unknown route or incompatible message type: {route}", nameof(route));
        options ??= new();
        var tags = MessagingTelemetry.Tags(target.Connection, route);
        using var activity = MessagingTelemetry.Activities.StartActivity(route + " publish", ActivityKind.Producer);
        activity?.SetTag("messaging.connection", target.Connection);
        activity?.SetTag("messaging.destination.name", target.Destination);
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        // Прикладные заголовки не могут подменять служебные идентификаторы и контекст трассировки.
        foreach (var (key, value) in options.Headers)
        {
            if (string.IsNullOrWhiteSpace(key) || value is null || key.StartsWith("sw-", StringComparison.OrdinalIgnoreCase) ||
                key.Equals("traceparent", StringComparison.OrdinalIgnoreCase) || key.Equals("tracestate", StringComparison.OrdinalIgnoreCase))
                throw new ArgumentException("Reserved or invalid message header.", nameof(options));
            headers.Add(key, value);
        }
        var id = options.MessageId ?? Guid.NewGuid().ToString("N");
        headers[MessageHeaders.MessageId] = id;
        if (options.CorrelationId is not null) headers[MessageHeaders.CorrelationId] = options.CorrelationId;
        // Передаём текущий W3C-контекст, чтобы обработка в другом сервисе продолжила ту же трассу.
        if (Activity.Current is { IdFormat: ActivityIdFormat.W3C } current)
        {
            headers[MessageHeaders.TraceParent] = current.Id!;
            if (current.TraceStateString is not null) headers[MessageHeaders.TraceState] = current.TraceStateString;
        }
        var started = Stopwatch.GetTimestamp();
        try
        {
            var body = JsonSerializer.SerializeToUtf8Bytes(message, builder.Json);
            // Успех учитываем после подтверждения транспорта; это ещё не подтверждение бизнес-обработки.
            await _transports[target.Connection].PublishAsync(target.Destination,
                new(body, options.Key, new ReadOnlyDictionary<string, string>(headers)), cancellationToken);
            MessagingTelemetry.Published.Add(1, tags);
            logger.LogDebug("Published {MessageId} via {Route} on {Connection}", id, route, target.Connection);
        }
        catch (Exception exception)
        {
            activity?.SetStatus(ActivityStatusCode.Error);
            activity?.SetTag("error.type", exception.GetType().FullName);
            MessagingTelemetry.PublishErrors.Add(1, tags);
            throw;
        }
        finally { MessagingTelemetry.PublishDuration.Record(Stopwatch.GetElapsedTime(started).TotalSeconds, tags); }
    }
}
