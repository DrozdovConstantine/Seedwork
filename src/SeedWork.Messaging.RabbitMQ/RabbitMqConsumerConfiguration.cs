namespace SeedWork.Messaging.RabbitMQ;

/// <summary>Простая привязка сообщения консумера к RabbitMQ. Класс конфигурации применяется после атрибута.</summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = true, Inherited = false)]
public sealed class RabbitMqConsumerAttribute(Type messageType) : Attribute
{
    /// <summary>Тип сообщения из реализованного интерфейса IConsumer.</summary>
    public Type MessageType { get; } = messageType;
    /// <summary>Имя endpoint; при отсутствии формируется из подключения и полных имён типов.</summary>
    public string? Endpoint { get; set; }
    /// <summary>Имя входной очереди; обязательно после применения конфигурации.</summary>
    public string? Queue { get; set; }
    /// <summary>Имя exchange подписки.</summary>
    public string? Exchange { get; set; }
    /// <summary>Тип exchange; по умолчанию Topic. Общие объявления должны совпадать.</summary>
    public RabbitMqExchangeType ExchangeType { get; set; } = RabbitMqExchangeType.Topic;
    /// <summary>Ключ binding; по умолчанию #.</summary>
    public string BindingKey { get; set; } = "#";
    /// <summary>Очередь ошибок; по умолчанию имя входной очереди с суффиксом _error.</summary>
    public string? ErrorQueue { get; set; }
    /// <summary>Число повторов сверх первой попытки; по умолчанию ноль.</summary>
    public int MaxRetries { get; set; }
    /// <summary>Начальная задержка между повторами в миллисекундах.</summary>
    public long IntervalMilliseconds { get; set; } = 1000;
    /// <summary>Удваивать задержку между повторами.</summary>
    public bool Exponential { get; set; }
    /// <summary>Максимальная задержка в миллисекундах.</summary>
    public long MaxIntervalMilliseconds { get; set; } = 30000;
    /// <summary>Повторяемые исключения; пустой массив разрешает все типы.</summary>
    public Type[] Handle { get; set; } = [];
    /// <summary>Исключения без повторов; имеют приоритет над Handle.</summary>
    public Type[] Ignore { get; set; } = [];

}

/// <summary>Настройки найденной подписки; сначала заполняются из атрибута, затем передаются классу конфигурации.</summary>
public sealed class RabbitMqConsumerOptions
{
    /// <summary>Уникальное имя endpoint; может быть переопределено конфигурацией.</summary>
    public string? Endpoint { get; set; }
    /// <summary>Обязательное имя входной очереди.</summary>
    public string? Queue { get; set; }
    /// <summary>Обязательное имя exchange подписки.</summary>
    public string? Exchange { get; set; }
    /// <summary>Тип exchange; по умолчанию Topic. Общие объявления должны совпадать.</summary>
    public RabbitMqExchangeType ExchangeType { get; set; } = RabbitMqExchangeType.Topic;
    /// <summary>Ключ binding; # соответствует всем ключам topic exchange.</summary>
    public string BindingKey { get; set; } = "#";
    /// <summary>Имя очереди ошибок; null выбирает стандартное имя.</summary>
    public string? ErrorQueue { get; set; }
    /// <summary>Политика повторов этой подписки.</summary>
    public RetryOptions Retry { get; set; } = RetryOptions.NoRetry();
}

/// <summary>Настройка одной пары «сообщение — консумер» RabbitMQ; создаётся через публичный конструктор без параметров.</summary>
public abstract class RabbitMqConsumerConfiguration<TMessage, TConsumer>
    where TMessage : class where TConsumer : class, IConsumer<TMessage>
{
    /// <summary>Переопределяет значения атрибута константными настройками подписки.</summary>
    public abstract void Configure(RabbitMqConsumerOptions options);
}
