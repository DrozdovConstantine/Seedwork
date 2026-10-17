namespace SeedWork.Application;

/// <summary>Определяет порядок шагов при регистрации приложения.</summary>
public interface IApplicationPipelineConfiguration
{
    /// <summary>Настраивает общие и частные шаги.</summary>
    void Configure(IApplicationPipelineBuilder builder);
}

/// <summary>Настройка всех сценариев и их отдельных цепочек.</summary>
public interface IApplicationPipelineBuilder
{
    /// <summary>Добавляет внешний шаг для всех сценариев, включая сценарии с <see cref="Unit"/>.</summary>
    IApplicationPipelineBuilder AddGlobal(Type stepType);
    /// <summary>Выбирает все сценарии, запрос которых реализует интерфейс-маркер.</summary>
    IUseCasePipelineBuilder ForRequests<TMarker>();
    /// <summary>Выбирает сценарий.</summary>
    IUseCasePipelineBuilder For<TRequest, TResponse>();
}

/// <summary>Изменения цепочки конкретного сценария. Тип шага может быть открытым generic-типом.</summary>
public interface IUseCasePipelineBuilder
{
    /// <summary>Добавляет шаг в конец цепочки.</summary>
    IUseCasePipelineBuilder Add(Type stepType);
    /// <summary>Вставляет шаг перед указанным шагом.</summary>
    IUseCasePipelineBuilder InsertBefore(Type anchorType, Type stepType);
    /// <summary>Вставляет шаг после указанного шага.</summary>
    IUseCasePipelineBuilder InsertAfter(Type anchorType, Type stepType);
    /// <summary>Исключает общий или ранее добавленный шаг.</summary>
    IUseCasePipelineBuilder Remove(Type stepType);
}
