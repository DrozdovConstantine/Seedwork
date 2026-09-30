namespace SeedWork.Messaging;

/// <summary>Публикует сообщения через именованные маршруты независимо от выбранного брокера.</summary>
public interface IMessagePublisher
{
    /// <summary>Отправляет сообщение в один маршрут и ожидает подтверждения брокера.</summary>
    /// <typeparam name="T">Тип контракта, указанный при регистрации маршрута.</typeparam>
    /// <param name="route">Логическое имя маршрута, связывающее тип сообщения с подключением и назначением.</param>
    /// <param name="message">Сообщение для сериализации в JSON.</param>
    /// <param name="options">Идентификаторы, ключ маршрутизации и дополнительные заголовки.</param>
    /// <param name="cancellationToken">Отмена ожидания; она не отменяет уже принятую брокером публикацию.</param>
    /// <remarks>
    /// Вызывать после запуска host. Подтверждение брокера не означает завершения обработки консумером.
    /// При потере подтверждения возможны дубликаты; атомарность с транзакцией БД не обеспечивается.
    /// </remarks>
    Task PublishAsync<T>(string route, T message, PublishOptions? options = null,
        CancellationToken cancellationToken = default) where T : class;
}

/// <summary>Обрабатывает сообщение прикладного контракта без зависимости от типов брокера.</summary>
/// <typeparam name="T">Тип входящего сообщения.</typeparam>
public interface IConsumer<T> where T : class
{
    /// <summary>Выполняет одну попытку обработки; исключение передаётся политике повторов.</summary>
    /// <param name="context">Сообщение, метаданные доставки и номер текущей попытки.</param>
    /// <param name="cancellationToken">Сигнал остановки обработки, в том числе при потере Kafka-партиции.</param>
    /// <remarks>Обработчик должен учитывать отмену и повторную доставку уже выполненных бизнес-действий.</remarks>
    Task ConsumeAsync(MessageContext<T> context, CancellationToken cancellationToken);
}

/// <summary>Метаданные одной публикации, общие для обоих транспортов.</summary>
public sealed record PublishOptions
{
    /// <summary>Идентификатор сообщения; при null издатель создаёт новый GUID.</summary>
    public string? MessageId { get; init; }
    /// <summary>Идентификатор прикладной операции для связи нескольких сообщений.</summary>
    public string? CorrelationId { get; init; }
    /// <summary>Ключ распределения Kafka; в RabbitMQ переопределяет routing key маршрута. Null оставляет выбор транспорту.</summary>
    public string? Key { get; init; }
    /// <summary>Строковые заголовки. Имена с префиксом sw-, traceparent и tracestate зарезервированы библиотекой.</summary>
    public IReadOnlyDictionary<string, string> Headers { get; init; } = new Dictionary<string, string>();
}

/// <summary>Данные одной попытки обработки сообщения.</summary>
/// <typeparam name="T">Тип прикладного контракта.</typeparam>
/// <param name="Message">Объект, заново десериализованный для текущей попытки.</param>
/// <param name="MessageId">Исходный идентификатор сообщения; у внешнего издателя может отсутствовать.</param>
/// <param name="CorrelationId">Идентификатор связанной прикладной операции, если передан издателем.</param>
/// <param name="Key">Ключ Kafka или routing key входящей доставки RabbitMQ.</param>
/// <param name="Headers">Заголовки сообщения, включая служебные метаданные библиотеки.</param>
/// <param name="Endpoint">Имя зарегистрированного обработчика доставки.</param>
/// <param name="Attempt">Номер попытки с единицы; новая доставка начинает отсчёт заново.</param>
public sealed record MessageContext<T>(T Message, string? MessageId, string? CorrelationId,
    string? Key, IReadOnlyDictionary<string, string> Headers, string Endpoint, int Attempt) where T : class;

/// <summary>ValidateOnly проверяет существующие ресурсы; CreateMissing создаёт отсутствующие и проверяет совместимость.</summary>
public enum TopologyMode { ValidateOnly, CreateMissing }

/// <summary>Сериализованное сообщение для адаптеров; отделяет отправку от прикладного API и будущего outbox.</summary>
/// <param name="Body">Исходные байты JSON, сохраняемые между попытками обработки.</param>
/// <param name="Key">Ключ распределения или маршрутизации сообщения.</param>
/// <param name="Headers">Пользовательские и служебные заголовки.</param>
public sealed record TransportMessage(byte[] Body, string? Key, IReadOnlyDictionary<string, string> Headers);

/// <summary>Имена служебных заголовков для идентификации, трассировки и диагностики ошибок.</summary>
public static class MessageHeaders
{
    public const string MessageId = "sw-message-id";
    public const string CorrelationId = "sw-correlation-id";
    public const string TraceParent = "traceparent";
    public const string TraceState = "tracestate";
    public const string ErrorType = "sw-error-type";
    public const string ErrorAttempts = "sw-error-attempts";
    public const string ErrorEndpoint = "sw-error-endpoint";
    public const string OriginalExchange = "sw-original-exchange";
    public const string OriginalRoutingKey = "sw-original-routing-key";
}
