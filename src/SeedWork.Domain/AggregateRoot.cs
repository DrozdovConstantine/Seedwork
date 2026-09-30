using System.Collections.ObjectModel;

namespace SeedWork.Domain;

/// <summary>Сущность, накапливающая доменные события для передачи внешнему диспетчеру.</summary>
/// <typeparam name="TId">Тип неизменяемого идентификатора.</typeparam>
/// <remarks>
/// События не публикуются и не очищаются автоматически. Тип не обеспечивает потокобезопасность.
/// Очищайте события только после того, как вызывающее приложение сочтёт их передачу успешной.
/// </remarks>
public abstract class AggregateRoot<TId> : Entity<TId>, IAggregateRoot
{
    private readonly List<IDomainEvent> _domainEvents = [];
    private readonly ReadOnlyCollection<IDomainEvent> _domainEventsView;

    /// <summary>Создаёт агрегат с указанным идентификатором и пустой коллекцией событий.</summary>
    /// <param name="id">Идентификатор агрегата.</param>
    protected AggregateRoot(TId id) : base(id)
    {
        _domainEventsView = _domainEvents.AsReadOnly();
    }

    /// <summary>Возвращает представление накопленных событий только для чтения в порядке добавления, отражающее изменения коллекции.</summary>
    public IReadOnlyCollection<IDomainEvent> DomainEvents => _domainEventsView;

    /// <summary>Добавляет событие, сохраняя порядок добавления и повторы.</summary>
    /// <param name="domainEvent">Событие для добавления.</param>
    /// <exception cref="ArgumentNullException">Событие равно <see langword="null" />.</exception>
    protected void AddDomainEvent(IDomainEvent domainEvent)
    {
        ArgumentNullException.ThrowIfNull(domainEvent);
        _domainEvents.Add(domainEvent);
    }

    /// <inheritdoc />
    public void ClearDomainEvents() => _domainEvents.Clear();
}
