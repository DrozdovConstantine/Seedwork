# SeedWork.Application

Один NuGet-пакет для прикладных сценариев и настраиваемого pipeline на .NET 10.
Он содержит runtime-контракты и source generator. Сервис подключает только
`SeedWork.Application`; генератор во время компиляции создаёт публичный метод
`AddSeedWorkApplication(params IApplicationPipelineConfiguration[])` в проекте
сервиса. Вызов сценария остаётся обычным
вызовом `IUseCase.ExecuteAsync`.

## Быстрый старт

В сервисе установите пакет `SeedWork.Application`, определите запрос и обработчик:

```csharp
using SeedWork.Application;

public sealed record Greet(string Name);

public sealed class GreetHandler : IUseCase<Greet, string>
{
    public Task<string> ExecuteAsync(Greet request, CancellationToken cancellationToken = default)
        => Task.FromResult($"Hello, {request.Name}!");
}
```

Для операции без полезного ответа используйте `Unit`. Это тот же контракт и тот
же pipeline; обработчик возвращает готовый `Unit.CompletedTask`:

```csharp
public sealed record PrintGreeting(string Text);

public sealed class PrintHandler : IUseCase<PrintGreeting, Unit>
{
    public Task<Unit> ExecuteAsync(PrintGreeting request, CancellationToken cancellationToken = default)
    {
        Console.WriteLine(request.Text);
        return Unit.CompletedTask;
    }
}
```

Если шаги пока не нужны, вызовите регистрацию без конфигураций. Затем
вызовите сценарий через DI:

```csharp
using Microsoft.Extensions.DependencyInjection;
using SeedWork.Application;

var services = new ServiceCollection();
services.AddSeedWorkApplication();
using var provider = services.BuildServiceProvider();
using var scope = provider.CreateScope();
using var cancellation = new CancellationTokenSource();

var result = await scope.ServiceProvider.GetRequiredService<IUseCase<Greet, string>>()
    .ExecuteAsync(new Greet("SeedWork"), cancellation.Token);
```

`cancellation.Token` здесь — токен вызывающего кода. Обработчики и шаги
регистрируются как `scoped`; создавайте scope для каждой операции вне веб-хоста.
Генератор создаёт метод после анализа исходников проекта. Если IDE ещё не
показывает `AddSeedWorkApplication()`, выполните сборку проекта.

## Один запускаемый пример

Из корня репозитория:

```shell
dotnet run --project samples/Application/Application.csproj -c Release
```

Ожидаемый вывод:

```text
validate: Greet
enter: Greet
audit: before
port token: True
audit: after
exit: Greet
Result: Hello, SeedWork!
enter: PrintGreeting
authorize: PrintGreeting
transaction: before
Hello, SeedWork!
transaction: after
exit: PrintGreeting
cache hit
Short circuit: Hello from cache!
enter: FailGreeting
authorize: FailGreeting
Caught: sample failure
```

Полный код расположен в [samples/Application](../../samples/Application/Program.cs).
Его классы запросов, обработчиков, шагов и конфигурации находятся в одном проекте,
который вызывает сгенерированный метод регистрации.

## Настройка порядка шагов

Маркеры объявляет сам сервис. Запрос может реализовать несколько маркеров:

```csharp
public interface ICommand;
public interface IQuery;

public sealed record Greet(string Name) : IQuery;
public sealed record PrintGreeting(string Text) : ICommand;
```

Запускаемый пример передаёт три конфигурации в нужном порядке:

```csharp
services.AddSeedWorkApplication(
    new CommonPipeline(),
    new CommandPipeline(),
    new ScenarioPipeline());
```

Первая задаёт общий шаг, вторая — два шага для команд, третья меняет
цепочки отдельных сценариев:

```csharp
public sealed class CommonPipeline : IApplicationPipelineConfiguration
{
    public void Configure(IApplicationPipelineBuilder builder)
        => builder.AddGlobal(typeof(TraceStep<,>));
}

public sealed class CommandPipeline : IApplicationPipelineConfiguration
{
    public void Configure(IApplicationPipelineBuilder builder)
    {
        builder.ForRequests<ICommand>()
            .InsertAfter(typeof(TraceStep<,>), typeof(AuthorizeCommandStep<,>))
            .InsertAfter(typeof(AuthorizeCommandStep<,>), typeof(TransactionStep<,>));
    }
}

public sealed class ScenarioPipeline : IApplicationPipelineConfiguration
{
    public void Configure(IApplicationPipelineBuilder builder)
    {
        builder.For<Greet, string>()
            .InsertBefore(typeof(TraceStep<,>), typeof(ValidateNameStep))
            .InsertAfter(typeof(TraceStep<,>), typeof(AuditGreetingStep));
        builder.For<SkipGreeting, string>()
            .Remove(typeof(TraceStep<,>))
            .Add(typeof(CachedGreetingStep));
        builder.For<FailGreeting, string>()
            .Remove(typeof(TransactionStep<,>));
    }
}
```

Общий шаг из того же примера одинаково выполняется для `Greet` с ответом
`string` и для `PrintGreeting` с ответом `Unit`:

```csharp
public sealed class TraceStep<TRequest, TResponse> : IUseCaseStep<TRequest, TResponse>
{
    public async Task<TResponse> InvokeAsync(TRequest request, CancellationToken cancellationToken,
        UseCaseNext<TResponse> next)
    {
        Console.WriteLine($"enter: {typeof(TRequest).Name}");
        var response = await next();
        Console.WriteLine($"exit: {typeof(TRequest).Name}");
        return response;
    }
}
```

`AddGlobal` применяет `TraceStep<TRequest, TResponse>` ко всем сценариям,
включая `IUseCase<PrintGreeting, Unit>`. `ForRequests<ICommand>()` выбирает все
сценарии, запросы которых реализуют маркер. Например, командный шаг может
объявить ограничение `where TRequest : ICommand`:

```csharp
public sealed class AuthorizeCommandStep<TRequest, TResponse> : IUseCaseStep<TRequest, TResponse>
    where TRequest : ICommand
{
    public Task<TResponse> InvokeAsync(TRequest request, CancellationToken cancellationToken,
        UseCaseNext<TResponse> next)
    {
        Console.WriteLine($"authorize: {typeof(TRequest).Name}");
        return next();
    }
}
```

Для конкретного сценария
`For<TRequest, TResponse>()`, в том числе `For<PrintGreeting, Unit>()`,
позволяет добавить шаг в конец (`Add`), до или после
существующего шага (`InsertBefore`, `InsertAfter`) и убрать шаг (`Remove`).
Тип шага встречается в одной цепочке только один раз.

| Сценарий | Порядок входа в шаги и обработчик |
| --- | --- |
| `Greet` | `ValidateNameStep` → `TraceStep` → `AuditGreetingStep` → `GreetHandler` |
| `PrintGreeting` | `TraceStep` → `AuthorizeCommandStep` → `TransactionStep` → `PrintHandler` |
| `SkipGreeting` | `CachedGreetingStep`; обработчик не вызывается |
| `FailGreeting` | `TraceStep` → `AuthorizeCommandStep` → `FailHandler`; `TransactionStep` удалён для этого сценария |

Первый шаг обрамляет следующие. При возврате из `GreetHandler` сначала завершается
`AuditGreetingStep`, затем `TraceStep`; поэтому `audit: after` выводится
перед `exit: Greet`.

Конфигурации вызываются в порядке передачи над одним builder. При сборке цепочки
сначала применяются общие шаги, затем групповые операции в порядке их записи,
затем настройки конкретного сценария. Если запрос реализует
несколько маркеров, применяются все подходящие группы. `InsertBefore` и
`InsertAfter` могут ссылаться на общий или уже добавленный групповой шаг.

## Досрочное завершение, отмена и ошибки

Шаг может вернуть ответ, не вызывая `next`. В примере это имитирует попадание
в кэш:

```csharp
public sealed class CachedGreetingStep : IUseCaseStep<SkipGreeting, string>
{
    public Task<string> InvokeAsync(SkipGreeting request, CancellationToken cancellationToken,
        UseCaseNext<string> next)
        => Task.FromResult("Hello from cache!");
}
```

Для сценария с `Unit` шаг досрочно завершает работу, возвращая
`Unit.CompletedTask` без вызова `next`.
Если шаг вызывает `next`, он может сделать это **один раз**; повторный вызов
приводит к `InvalidOperationException`. При исключении из обработчика исходное
исключение проходит через pipeline без преобразования в `Result`. Строка
`exit: FailGreeting` не появляется, потому что `TraceStep` не получил
успешный ответ от `next`.

Передавайте полученный `CancellationToken` во все внешние асинхронные операции.
В примере `GreetHandler` вызывает порт `IGreetingPort.BuildAsync(..., cancellationToken)`;
строка `port token: True` подтверждает получение отменяемого токена. Сам пакет
не отменяет операции и не подменяет исключения отмены.

## ASP.NET Core Minimal API

Endpoint получает scoped `IUseCase` через DI. `CancellationToken` параметра
обработчика связан с отменой HTTP-запроса:

```csharp
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using SeedWork.Application;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddScoped<IGreetingPort, GreetingPort>();
builder.Services.AddSeedWorkApplication(
    new CommonPipeline(), new CommandPipeline(), new ScenarioPipeline());

var app = builder.Build();
app.MapPost("/greetings", async (Greet request, IUseCase<Greet, string> useCase,
    CancellationToken cancellationToken) =>
    TypedResults.Ok(await useCase.ExecuteAsync(request, cancellationToken)));
app.MapPost("/print", async (PrintGreeting request, IUseCase<PrintGreeting, Unit> useCase,
    CancellationToken cancellationToken) =>
{
    await useCase.ExecuteAsync(request, cancellationToken);
    return TypedResults.NoContent();
});
app.Run();
```

Фрагмент регистрации и endpoint компилируется в
[MinimalApiSnippet.cs](../../samples/Application/MinimalApiSnippet.cs) запускаемого
примера; консольный запуск не поднимает HTTP-сервер. Типы `Greet`, `GreetHandler`,
`IGreetingPort` и `GreetingPort` определены в том же проекте примера. Правила
HTTP-ответов и обработки исключений остаются в веб-сервисе.

## Обнаружение и ошибки конфигурации

Генератор ищет сценарии и шаги **только в текущем проекте**.
Классы должны быть конкретными, верхнего уровня, `public` или `internal`.
Обработчики не должны быть generic; открытые generic-шаги поддерживаются для
`IUseCaseStep<TRequest, TResponse>`; поддерживаются ограничения параметров на
интерфейсы-маркеры. Конфигурации передаются готовыми объектами и могут
находиться в других проектах или иметь конструктор с параметрами. Сканирования
сборок во время запуска нет.

При компиляции `SWA001` сообщает о двух обработчиках одного контракта,
`SWA002` — о неподдерживаемой форме класса. При вызове
`AddSeedWorkApplication()` пакет проверяет массив конфигураций и его элементы
на `null`, затем — неизвестные или несовместимые шаги, отсутствующие точки вставки,
дубли шагов, маркеры, не являющиеся интерфейсами, и ссылки на неизвестные сценарии.

## Сборка из исходников

`SeedWork.Application.csproj` собирает runtime для `net10.0` и генератор для
`netstandard2.0`. При прямой ссылке на проект в этом репозитории пример и тесты
подключают его дважды: как runtime и как analyzer. Пользователю NuGet нужна
одна ссылка на пакет.

```shell
dotnet test SeedWork.slnx -c Release
dotnet pack src/SeedWork.Application/SeedWork.Application.csproj -c Release -o artifacts/packages
```
