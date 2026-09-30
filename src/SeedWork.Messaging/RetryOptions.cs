namespace SeedWork.Messaging;

/// <summary>Политика повторов одного endpoint; применяется к текущей доставке сообщения.</summary>
public sealed class RetryOptions
{
    /// <summary>Число повторов сверх первой попытки; по умолчанию ноль — повторы отключены.</summary>
    public int MaxRetries { get; set; }
    /// <summary>Создаёт отдельную политику с одной попыткой обработки без повторов.</summary>
    public static RetryOptions NoRetry() => new();

    /// <summary>Фиксированный интервал либо начальный интервал экспоненциальной задержки.</summary>
    public TimeSpan Interval { get; set; } = TimeSpan.FromSeconds(1);
    /// <summary>Удваивать задержку после каждого повтора до достижения MaxInterval.</summary>
    public bool Exponential { get; set; }
    /// <summary>Верхний предел задержки между попытками.</summary>
    public TimeSpan MaxInterval { get; set; } = TimeSpan.FromSeconds(30);
    /// <summary>Типы повторяемых исключений, включая производные; пустой набор разрешает все типы.</summary>
    public ISet<Type> Handle { get; } = new HashSet<Type>();
    /// <summary>Типы исключений без повторов; имеет приоритет над Handle.</summary>
    public ISet<Type> Ignore { get; } = new HashSet<Type>();

    /// <summary>Проверяет фильтры исключений; лимит попыток и отмену проверяет диспетчер.</summary>
    public bool ShouldRetry(Exception exception) =>
        !Ignore.Any(t => t.IsInstanceOfType(exception)) &&
        (Handle.Count == 0 || Handle.Any(t => t.IsInstanceOfType(exception)));

    /// <summary>Возвращает задержку для номера повтора, начиная с единицы.</summary>
    public TimeSpan GetDelay(int retry) => TimeSpan.FromMilliseconds(Math.Min(
        MaxInterval.TotalMilliseconds,
        Interval.TotalMilliseconds * (Exponential ? Math.Pow(2, Math.Min(retry - 1, 60)) : 1)));

    /// <summary>Проверяет допустимость задержек, числа повторов и типов исключений до запуска шины.</summary>
    public void Validate()
    {
        if (MaxRetries < 0 || Interval < TimeSpan.Zero || MaxInterval < Interval ||
            MaxInterval.TotalMilliseconds > uint.MaxValue - 1 ||
            Handle.Concat(Ignore).Any(t => !typeof(Exception).IsAssignableFrom(t)))
            throw new ArgumentException("Invalid retry count, interval, or exception type.");
    }
}
