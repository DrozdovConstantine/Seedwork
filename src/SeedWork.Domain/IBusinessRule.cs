namespace SeedWork.Domain;

/// <summary>Бизнес-правило, определённое доменной моделью сервиса.</summary>
public interface IBusinessRule
{
    /// <summary>Сообщение, описывающее нарушение правила.</summary>
    string Message { get; }

    /// <summary>Возвращает true, если правило нарушено.</summary>
    bool IsBroken();
}
