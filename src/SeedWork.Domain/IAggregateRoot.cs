namespace SeedWork.Domain;

/// <summary>Корень агрегата, предоставляющий накопленные доменные события для передачи внешнему диспетчеру.</summary>
public interface IAggregateRoot
{
    /// <summary>Возвращает накопленные события в порядке добавления, включая повторы.</summary>
    IReadOnlyCollection<IDomainEvent> DomainEvents { get; }

    /// <summary>Удаляет первое вхождение указанного экземпляра события.</summary>
    /// <param name="domainEvent">Экземпляр события.</param>
    /// <returns>True, если событие удалено.</returns>
    bool RemoveDomainEvent(IDomainEvent domainEvent);

    /// <summary>Очищает накопленные события; вызов для пустой коллекции допустим.</summary>
    /// <remarks>Вызывающий код определяет, когда события успешно переданы его диспетчеру.</remarks>
    void ClearDomainEvents();
}
