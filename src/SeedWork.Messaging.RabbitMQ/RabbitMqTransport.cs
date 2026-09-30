using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace SeedWork.Messaging.RabbitMQ;

/// <summary>Адаптер RabbitMQ с подтверждением публикаций, последовательной обработкой очередей и отправкой ошибок.</summary>
public sealed class RabbitMqTransport : IMessageTransport
{
    public const string InstrumentationName = "SeedWork.Messaging.RabbitMQ";
    private static readonly ActivitySource Activities = new(InstrumentationName);
    private static readonly Meter Meter = new(InstrumentationName);
    private static readonly Counter<long> ErrorMessages = Meter.CreateCounter<long>("messaging.rabbitmq.error_queue.messages");
    private readonly RabbitMqBuilder _config;
    private readonly ConsumerDispatcher _dispatcher;
    private readonly ILogger _logger;
    private IConnection? _connection;
    private TaskCompletionSource _disconnected = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public string Name { get; }

    internal RabbitMqTransport(string name, RabbitMqBuilder config, ConsumerDispatcher dispatcher, ILogger logger)
        => (Name, _config, _dispatcher, _logger) = (name, config, dispatcher, logger);

    /// <summary>Открывает соединение и подготавливает топологию до запуска подписок.</summary>
    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        // Соединение и консумеры восстанавливаются единым циклом адаптера, без конкурирующего восстановления клиента.
        var factory = _config.ConnectionFactory!;
        var connection = await factory.CreateConnectionAsync(cancellationToken);
        var disconnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        connection.ConnectionShutdownAsync += (_, _) => { disconnected.TrySetResult(); return Task.CompletedTask; };
        try { await RabbitTopology.EnsureAsync(connection, _config, factory, cancellationToken); }
        catch { await connection.DisposeAsync(); throw; }
        _disconnected = disconnected;
        _connection = connection;
    }

    /// <summary>Публикует сообщение по маршруту RabbitMQ и ожидает подтверждения либо возврата от брокера.</summary>
    public async Task PublishAsync(string destination, TransportMessage message, CancellationToken cancellationToken)
    {
        var route = _config.Routes[destination];
        var connection = _connection ?? throw new InvalidOperationException("Messaging has not started.");
        // Отдельный канал изолирует параллельные публикации; оба флага включают publisher confirms и их отслеживание.
        await using var channel = await connection.CreateChannelAsync(new CreateChannelOptions(true, true), cancellationToken);
        await SendAsync(channel, route.Exchange, message.Key ?? route.RoutingKey, message, cancellationToken);
    }

    // mandatory превращает отсутствие подходящей очереди в ошибку публикации, а не в незаметную потерю сообщения.
    private static ValueTask SendAsync(IChannel channel, string exchange, string routingKey, TransportMessage message, CancellationToken ct)
        => channel.BasicPublishAsync(exchange, routingKey, mandatory: true, new BasicProperties
        {
            Persistent = true, ContentType = "application/json",
            MessageId = message.Headers.GetValueOrDefault(MessageHeaders.MessageId),
            CorrelationId = message.Headers.GetValueOrDefault(MessageHeaders.CorrelationId),
            Headers = message.Headers.ToDictionary(k => k.Key, v => (object?)v.Value)
        }, message.Body, ct);

    /// <summary>Обслуживает консумеров и переподключается при потере соединения или невозможности подтвердить обработку.</summary>
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            using var cycle = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var channels = new List<IChannel>();
            try
            {
                if (_connection is null) await InitializeAsync(cancellationToken);
                var signal = _disconnected;
                foreach (var endpoint in _config.Endpoints)
                {
                    var channel = await _connection!.CreateChannelAsync(new CreateChannelOptions(true, true), cancellationToken);
                    channels.Add(channel);
                    channel.ChannelShutdownAsync += (_, _) => { signal.TrySetResult(); return Task.CompletedTask; };
                    // Вместе с single active consumer ограничивает очередь одним обрабатываемым сообщением, включая повторы.
                    await channel.BasicQosAsync(0, 1, false, cancellationToken);
                    var consumer = new AsyncEventingBasicConsumer(channel);
                    consumer.ReceivedAsync += async (_, delivery) =>
                    {
                        try
                        {
                            var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                            if (delivery.BasicProperties.Headers is { } incoming)
                                foreach (var (key, value) in incoming)
                                    headers[key] = value is byte[] bytes ? Encoding.UTF8.GetString(bytes) : value?.ToString() ?? "";
                            if (delivery.BasicProperties.MessageId is { } id) headers.TryAdd(MessageHeaders.MessageId, id);
                            if (delivery.BasicProperties.CorrelationId is { } correlation) headers.TryAdd(MessageHeaders.CorrelationId, correlation);
                            // Сохраняем собственную копию тела для обработки и повторов, независимо от буфера клиента.
                            var message = new TransportMessage(delivery.Body.ToArray(), delivery.RoutingKey, new ReadOnlyDictionary<string, string>(headers));
                            var failure = await endpoint.Dispatch(message, _dispatcher, cycle.Token);
                            if (failure is not null)
                            {
                                using var activity = Activities.StartActivity(endpoint.Name + " error", ActivityKind.Producer);
                                headers[MessageHeaders.ErrorType] = failure.Exception.GetType().FullName!;
                                headers[MessageHeaders.ErrorAttempts] = failure.Attempts.ToString(System.Globalization.CultureInfo.InvariantCulture);
                                headers[MessageHeaders.ErrorEndpoint] = endpoint.Name;
                                headers[MessageHeaders.OriginalExchange] = delivery.Exchange;
                                headers[MessageHeaders.OriginalRoutingKey] = delivery.RoutingKey;
                                // Через стандартный exchange адресуем error queue напрямую и ждём подтверждения записи.
                                await SendAsync(channel, "", endpoint.ErrorQueue, message, cycle.Token);
                                ErrorMessages.Add(1, MessagingTelemetry.Tags(Name, endpoint.Name));
                            }
                            // Ack разрешён после успешной обработки либо сохранения ошибки. Сбой между отправкой и ack допускает дубликат.
                            await channel.BasicAckAsync(delivery.DeliveryTag, false, cycle.Token);
                        }
                        catch (OperationCanceledException) when (cycle.IsCancellationRequested) { }
                        catch (Exception exception)
                        {
                            // Исходное сообщение не подтверждаем; закрытие соединения вернёт его брокеру для новой доставки.
                            _logger.LogError("RabbitMQ endpoint {Endpoint} on {Connection} stopped: {ErrorType}", endpoint.Name, Name, exception.GetType().FullName);
                            signal.TrySetResult();
                        }
                    };
                    await channel.BasicConsumeAsync(endpoint.Queue, false, consumer, cancellationToken);
                }
                await signal.Task.WaitAsync(cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { break; }
            catch (Exception exception)
            {
                _logger.LogError("RabbitMQ connection {Connection} recovering: {ErrorType}", Name, exception.GetType().FullName);
            }
            finally
            {
                // Сначала отменяем обработчики, затем закрываем каналы и соединение текущего цикла.
                await cycle.CancelAsync();
                foreach (var channel in channels) await channel.DisposeAsync();
                var connection = Interlocked.Exchange(ref _connection, null);
                if (connection is not null) await connection.DisposeAsync();
            }
            await Task.Delay(_config.Options.ReconnectInterval, cancellationToken);
        }
    }

    public async ValueTask DisposeAsync()
    {
        var connection = Interlocked.Exchange(ref _connection, null);
        if (connection is not null) await connection.DisposeAsync();
    }
}
