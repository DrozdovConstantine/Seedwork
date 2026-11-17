namespace SeedWork.Domain;

/// <summary>Условие, проверяемое для объекта в памяти.</summary>
/// <typeparam name="T">Тип проверяемого объекта.</typeparam>
public interface ISpecification<in T>
{
    /// <summary>Проверяет, удовлетворяет ли объект условию.</summary>
    bool IsSatisfiedBy(T candidate);
}
