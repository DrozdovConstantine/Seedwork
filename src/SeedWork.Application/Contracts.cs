namespace SeedWork.Application;

/// <summary>Прикладной сценарий с результатом; для операции без полезного ответа используйте <see cref="Unit"/>.</summary>
public interface IUseCase<TRequest, TResponse>
{
    /// <summary>Выполняет сценарий.</summary>
    Task<TResponse> ExecuteAsync(TRequest request, CancellationToken cancellationToken = default);
}

/// <summary>Результат операции без полезного ответа.</summary>
public readonly record struct Unit
{
    /// <summary>Единственное значимое значение результата.</summary>
    public static Unit Value => default;

    /// <summary>Завершённая операция с результатом <see cref="Value"/>.</summary>
    public static Task<Unit> CompletedTask { get; } = Task.FromResult(Value);
}

/// <summary>Продолжает сценарий. Допускается не более одного вызова.</summary>
public delegate Task<TResponse> UseCaseNext<TResponse>();

/// <summary>Шаг сценария.</summary>
public interface IUseCaseStep<TRequest, TResponse>
{
    /// <summary>Выполняет шаг; может вызвать next один раз или завершить сценарий самостоятельно.</summary>
    Task<TResponse> InvokeAsync(TRequest request, CancellationToken cancellationToken, UseCaseNext<TResponse> next);
}
