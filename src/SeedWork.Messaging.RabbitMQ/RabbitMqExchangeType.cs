namespace SeedWork.Messaging.RabbitMQ;

/// <summary>Способ маршрутизации сообщений из exchange в очереди RabbitMQ.</summary>
public enum RabbitMqExchangeType
{
    /// <summary>Сопоставление routing key с шаблоном binding; вариант по умолчанию.</summary>
    Topic = 0,
    /// <summary>Точное совпадение routing key и binding key.</summary>
    Direct,
    /// <summary>Доставка во все привязанные очереди независимо от ключа.</summary>
    Fanout
}
