using Microsoft.Extensions.Configuration;

namespace SeedWork.Messaging.Kafka;

/// <summary>Простая привязка сообщения консумера к Kafka. Класс конфигурации применяется после атрибута.</summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = true, Inherited = false)]
public sealed class KafkaConsumerAttribute(Type messageType) : Attribute
{
    /// <summary>Тип сообщения из реализованного интерфейса IConsumer.</summary>
    public Type MessageType { get; } = messageType;
    /// <summary>Имя endpoint; при отсутствии формируется из подключения и полных имён типов.</summary>
    public string? Endpoint { get; set; }
    /// <summary>Имя topic подписки.</summary>
    public string? Topic { get; set; }
    /// <summary>Число партиций; ноль использует общее объявление topic.</summary>
    public int Partitions { get; set; }
    /// <summary>Фактор репликации; ноль использует общее объявление topic.</summary>
    public short ReplicationFactor { get; set; }
    /// <summary>Имя consumer group; обязательно после применения конфигурации.</summary>
    public string? Group { get; set; }
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

/// <summary>Настройки найденной подписки Kafka после чтения атрибута и до применения класса конфигурации.</summary>
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
    /// <summary>Переопределяет значения атрибута; configuration берётся из аргумента сканирования и по умолчанию пуст.</summary>
    public abstract void Configure(KafkaConsumerOptions options, IConfiguration configuration);
}
