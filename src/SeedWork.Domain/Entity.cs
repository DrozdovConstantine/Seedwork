using System.Runtime.CompilerServices;

namespace SeedWork.Domain;

/// <summary>Доменная сущность, определяемая конкретным типом и идентификатором, отличным от значения по умолчанию.</summary>
/// <typeparam name="TId">Тип идентификатора со стабильными правилами равенства и вычисления хеш-кода.</typeparam>
/// <remarks>
/// Идентификатор, равный <c>default(TId)</c>, считается неназначенным. Сущности с неназначенными
/// идентификаторами равны только по ссылке. Пустая строка считается назначенным идентификатором.
/// Значение идентификатора должно оставаться неизменным на протяжении всей жизни сущности.
/// </remarks>
public abstract class Entity<TId> : IEquatable<Entity<TId>>
{
    /// <summary>Создаёт сущность с указанным идентификатором, который может быть неназначенным.</summary>
    /// <param name="id">Идентификатор, который нельзя заменить после создания сущности.</param>
    protected Entity(TId id) => Id = id;

    /// <summary>Возвращает идентификатор, переданный при создании сущности.</summary>
    public TId Id { get; }

    /// <summary>Сравнивает сущности по правилам равенства с учётом отсутствующих значений.</summary>
    /// <param name="left">Левая сущность для сравнения.</param>
    /// <param name="right">Правая сущность для сравнения.</param>
    /// <returns>Значение <see langword="true" />, если сущности равны или обе равны <see langword="null" />; иначе <see langword="false" />.</returns>
    public static bool operator ==(Entity<TId>? left, Entity<TId>? right) =>
        ReferenceEquals(left, right) || (left is not null && left.Equals(right));

    /// <summary>Проверяет неравенство сущностей с учётом отсутствующих значений.</summary>
    /// <param name="left">Левая сущность для сравнения.</param>
    /// <param name="right">Правая сущность для сравнения.</param>
    /// <returns>Значение <see langword="true" />, если сущности не равны; иначе <see langword="false" />.</returns>
    public static bool operator !=(Entity<TId>? left, Entity<TId>? right) => !(left == right);

    /// <summary>Сравнивает ссылки либо конкретные типы и назначенные идентификаторы.</summary>
    /// <param name="other">Сущность для сравнения.</param>
    /// <returns>Значение <see langword="true" />, если объекты представляют одну и ту же сущность; иначе <see langword="false" />.</returns>
    public bool Equals(Entity<TId>? other)
    {
        if (ReferenceEquals(this, other))
            return true;

        return other is not null
            && GetType() == other.GetType()
            && !EqualityComparer<TId>.Default.Equals(Id, default)
            && EqualityComparer<TId>.Default.Equals(Id, other.Id);
    }

    /// <inheritdoc />
    public sealed override bool Equals(object? obj) => obj is Entity<TId> other && Equals(other);

    /// <summary>Возвращает хеш-код экземпляра для неназначенного идентификатора либо хеш-код конкретного типа и идентификатора.</summary>
    /// <returns>Хеш-код, согласованный с правилами равенства сущностей.</returns>
    public sealed override int GetHashCode() => EqualityComparer<TId>.Default.Equals(Id, default)
        ? RuntimeHelpers.GetHashCode(this)
        : HashCode.Combine(GetType(), Id);
}
