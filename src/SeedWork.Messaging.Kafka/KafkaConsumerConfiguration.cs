namespace SeedWork.Messaging.Kafka;

/// <summary>Настройки подписки Kafka, передаваемые найденному классу конфигурации.</summary>
public sealed class KafkaConsumerOptions
{
    /// <summary>Уникальное имя endpoint; может быть переопределено конфигурацией.</summary>
    public string? Endpoint { get; set; }
    /// <summary>Обязательное имя topic подписки.</summary>
    public string? Topic { get; set; }
    /// <summary>Число партиций; ноль использует общее объявление topic.</summary>
    public int Partitions { get; set; }
    /// <summary>Фактор репликации; ноль использует общее объявление topic.</summary>
    public short ReplicationFactor { get; set; }
    /// <summary>Обязательное имя consumer group.</summary>
    public string? Group { get; set; }
    /// <summary>Политика повторов этой подписки.</summary>
    public RetryOptions Retry { get; set; } = RetryOptions.NoRetry();
}

/// <summary>Настройка одной пары «сообщение — консумер» Kafka; создаётся через публичный конструктор без параметров.</summary>
public abstract class KafkaConsumerConfiguration<TMessage, TConsumer>
    where TMessage : class where TConsumer : class, IConsumer<TMessage>
{
    /// <summary>Задаёт настройки подписки.</summary>
    public abstract void Configure(KafkaConsumerOptions options);
}
