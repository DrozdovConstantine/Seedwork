using Microsoft.Extensions.DependencyInjection;
using SeedWork.Application;

var services = new ServiceCollection();
services.AddScoped<IGreetingPort, GreetingPort>();
services.AddSeedWorkApplication(new CommonPipeline(), new CommandPipeline(), new ScenarioPipeline());

using var provider = services.BuildServiceProvider();
using var scope = provider.CreateScope();
using var cancellation = new CancellationTokenSource();

var greeting = await scope.ServiceProvider.GetRequiredService<IUseCase<Greet, string>>()
    .ExecuteAsync(new Greet("SeedWork"), cancellation.Token);
Console.WriteLine($"Result: {greeting}");

await scope.ServiceProvider.GetRequiredService<IUseCase<PrintGreeting, Unit>>()
    .ExecuteAsync(new PrintGreeting(greeting), cancellation.Token);

var cached = await scope.ServiceProvider.GetRequiredService<IUseCase<SkipGreeting, string>>()
    .ExecuteAsync(new SkipGreeting(), cancellation.Token);
Console.WriteLine($"Short circuit: {cached}");

try
{
    await scope.ServiceProvider.GetRequiredService<IUseCase<FailGreeting, string>>()
        .ExecuteAsync(new FailGreeting(), cancellation.Token);
}
catch (InvalidOperationException exception)
{
    Console.WriteLine($"Caught: {exception.Message}");
}

public interface ICommand;
public interface IQuery;
public sealed record Greet(string Name) : IQuery;
public sealed record PrintGreeting(string Text) : ICommand;
public sealed record SkipGreeting : IQuery;
public sealed record FailGreeting : ICommand;

public interface IGreetingPort
{
    Task<string> BuildAsync(string name, CancellationToken cancellationToken);
}

public sealed class GreetingPort : IGreetingPort
{
    public Task<string> BuildAsync(string name, CancellationToken cancellationToken)
    {
        Console.WriteLine($"port token: {cancellationToken.CanBeCanceled}");
        return Task.FromResult($"Hello, {name}!");
    }
}

public sealed class GreetHandler(IGreetingPort port) : IUseCase<Greet, string>
{
    public Task<string> ExecuteAsync(Greet request, CancellationToken cancellationToken = default)
        => port.BuildAsync(request.Name, cancellationToken);
}

public sealed class PrintHandler : IUseCase<PrintGreeting, Unit>
{
    public Task<Unit> ExecuteAsync(PrintGreeting request, CancellationToken cancellationToken = default)
    {
        Console.WriteLine(request.Text);
        return Unit.CompletedTask;
    }
}

public sealed class SkipHandler : IUseCase<SkipGreeting, string>
{
    public Task<string> ExecuteAsync(SkipGreeting request, CancellationToken cancellationToken = default)
        => throw new InvalidOperationException("A cache hit must bypass this handler.");
}

public sealed class FailHandler : IUseCase<FailGreeting, string>
{
    public Task<string> ExecuteAsync(FailGreeting request, CancellationToken cancellationToken = default)
        => throw new InvalidOperationException("sample failure");
}

public sealed class TraceStep<TRequest, TResponse> : IUseCaseStep<TRequest, TResponse>
{
    public async Task<TResponse> InvokeAsync(TRequest request, CancellationToken cancellationToken, UseCaseNext<TResponse> next)
    {
        Console.WriteLine($"enter: {typeof(TRequest).Name}");
        var response = await next();
        Console.WriteLine($"exit: {typeof(TRequest).Name}");
        return response;
    }
}

public sealed class AuthorizeCommandStep<TRequest, TResponse> : IUseCaseStep<TRequest, TResponse>
    where TRequest : ICommand
{
    public Task<TResponse> InvokeAsync(TRequest request, CancellationToken cancellationToken, UseCaseNext<TResponse> next)
    {
        Console.WriteLine($"authorize: {typeof(TRequest).Name}");
        return next();
    }
}

public sealed class TransactionStep<TRequest, TResponse> : IUseCaseStep<TRequest, TResponse>
    where TRequest : ICommand
{
    public async Task<TResponse> InvokeAsync(TRequest request, CancellationToken cancellationToken, UseCaseNext<TResponse> next)
    {
        Console.WriteLine("transaction: before");
        var response = await next();
        Console.WriteLine("transaction: after");
        return response;
    }
}

public sealed class ValidateNameStep : IUseCaseStep<Greet, string>
{
    public Task<string> InvokeAsync(Greet request, CancellationToken cancellationToken, UseCaseNext<string> next)
    {
        Console.WriteLine("validate: Greet");
        if (string.IsNullOrWhiteSpace(request.Name))
            throw new ArgumentException("Name is required.", nameof(request));
        return next();
    }
}

public sealed class AuditGreetingStep : IUseCaseStep<Greet, string>
{
    public async Task<string> InvokeAsync(Greet request, CancellationToken cancellationToken, UseCaseNext<string> next)
    {
        Console.WriteLine("audit: before");
        var response = await next();
        Console.WriteLine("audit: after");
        return response;
    }
}

public sealed class CachedGreetingStep : IUseCaseStep<SkipGreeting, string>
{
    public Task<string> InvokeAsync(SkipGreeting request, CancellationToken cancellationToken, UseCaseNext<string> next)
    {
        Console.WriteLine("cache hit");
        return Task.FromResult("Hello from cache!");
    }
}

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
