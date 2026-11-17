namespace SeedWork.Domain;

/// <summary>Ошибка, возникающая при нарушении доменного инварианта.</summary>
public class DomainException : Exception
{
    /// <summary>Создаёт исключение с описанием нарушения.</summary>
    public DomainException(string message) : base(message) { }

    /// <summary>Создаёт исключение с описанием и причиной нарушения.</summary>
    public DomainException(string message, Exception innerException) : base(message, innerException) { }
}
