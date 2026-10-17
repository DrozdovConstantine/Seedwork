namespace SeedWork.Messaging.RabbitMQ;

/// <summary>Настройки подписки RabbitMQ, передаваемые найденному классу конфигурации.</summary>
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
    /// <summary>Задаёт настройки подписки.</summary>
    public abstract void Configure(RabbitMqConsumerOptions options);
}
