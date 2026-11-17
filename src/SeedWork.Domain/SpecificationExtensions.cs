namespace SeedWork.Domain;

/// <summary>Композиция спецификаций для проверки в памяти.</summary>
public static class SpecificationExtensions
{
    /// <summary>Создаёт спецификацию, выполняющую оба условия по порядку.</summary>
    public static ISpecification<T> And<T>(this ISpecification<T> first, ISpecification<T> second)
    {
        ArgumentNullException.ThrowIfNull(first);
        ArgumentNullException.ThrowIfNull(second);
        return new CombinedSpecification<T>(item => first.IsSatisfiedBy(item) && second.IsSatisfiedBy(item));
    }

    /// <summary>Создаёт спецификацию, выполняющую хотя бы одно условие.</summary>
    public static ISpecification<T> Or<T>(this ISpecification<T> first, ISpecification<T> second)
    {
        ArgumentNullException.ThrowIfNull(first);
        ArgumentNullException.ThrowIfNull(second);
        return new CombinedSpecification<T>(item => first.IsSatisfiedBy(item) || second.IsSatisfiedBy(item));
    }

    /// <summary>Создаёт спецификацию с противоположным результатом.</summary>
    public static ISpecification<T> Not<T>(this ISpecification<T> specification)
    {
        ArgumentNullException.ThrowIfNull(specification);
        return new CombinedSpecification<T>(item => !specification.IsSatisfiedBy(item));
    }

    private sealed class CombinedSpecification<T>(Func<T, bool> predicate) : ISpecification<T>
    {
        public bool IsSatisfiedBy(T candidate) => predicate(candidate);
    }
}
