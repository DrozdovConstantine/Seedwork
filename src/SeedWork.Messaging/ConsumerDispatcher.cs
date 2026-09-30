using System.Diagnostics;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace SeedWork.Messaging;

/// <summary>Окончательная ошибка текущей доставки; решение об ack или паузе принимает адаптер.</summary>
public sealed record ConsumerFailure(Exception Exception, int Attempts);

/// <summary>Выполняет десериализацию, создание DI scope, вызов консумера и повторы для обоих транспортов.</summary>
public sealed class ConsumerDispatcher(IServiceScopeFactory scopes, ILogger<ConsumerDispatcher> logger)
{
    /// <summary>Возвращает null при успехе или окончательную ошибку; отмену передаёт вызывающему адаптеру.</summary>
    public async Task<ConsumerFailure?> DispatchAsync<T, TConsumer>(TransportMessage message,
        string connection, string endpoint, RetryOptions retry, JsonSerializerOptions json,
        CancellationToken cancellationToken) where T : class where TConsumer : class, IConsumer<T>
    {
        var tags = MessagingTelemetry.Tags(connection, endpoint);
        using var activity = MessagingTelemetry.StartConsumer(endpoint, message);
        activity?.SetTag("messaging.connection", connection);
        activity?.SetTag("messaging.destination.name", endpoint);
        var started = Stopwatch.GetTimestamp();
        var attempt = 0;
        try
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                attempt++;
                var deserialized = false;
                try
                {
                    // Изменения объекта в неудачной попытке не должны попадать в следующую попытку.
                    var value = JsonSerializer.Deserialize<T>(message.Body, json)
                        ?? throw new JsonException("A message cannot be JSON null.");
                    deserialized = true;
                    // Каждая попытка получает новый scope, чтобы не использовать повреждённое состояние зависимостей.
                    await using var scope = scopes.CreateAsyncScope();
                    var consumer = scope.ServiceProvider.GetRequiredService<TConsumer>();
                    await consumer.ConsumeAsync(new MessageContext<T>(value,
                        message.Headers.GetValueOrDefault(MessageHeaders.MessageId),
                        message.Headers.GetValueOrDefault(MessageHeaders.CorrelationId),
                        message.Key, message.Headers, endpoint, attempt), cancellationToken);
                    cancellationToken.ThrowIfCancellationRequested();
                    MessagingTelemetry.Consumed.Add(1, tags);
                    return null;
                }
                // Остановка или потеря назначения не являются основанием для error queue либо commit.
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
                catch (Exception exception)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    // Текст исключения может содержать тело сообщения или секреты; записываем только тип.
                    logger.LogWarning("Consumer {Endpoint} on {Connection} failed for message {MessageId}, attempt {Attempt}: {ErrorType}",
                        endpoint, connection, message.Headers.GetValueOrDefault(MessageHeaders.MessageId), attempt, exception.GetType().FullName);
                    // Повтор не исправит некорректный JSON; остальные ошибки подчиняются политике endpoint.
                    if (!deserialized || attempt > retry.MaxRetries || !retry.ShouldRetry(exception))
                    {
                        activity?.SetStatus(ActivityStatusCode.Error);
                        activity?.SetTag("error.type", exception.GetType().FullName);
                        MessagingTelemetry.Errors.Add(1, tags);
                        return new ConsumerFailure(exception, attempt);
                    }
                    MessagingTelemetry.Retries.Add(1, tags);
                    await Task.Delay(retry.GetDelay(attempt), cancellationToken);
                }
            }
        }
        finally { MessagingTelemetry.ConsumeDuration.Record(Stopwatch.GetElapsedTime(started).TotalSeconds, tags); }
    }
}
