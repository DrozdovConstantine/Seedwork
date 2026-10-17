using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using SeedWork.Application;

/// <summary>Компилируемый фрагмент Minimal API из README. Консольный пример его не запускает.</summary>
public static class MinimalApiSnippet
{
    public static WebApplication Build(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);
        builder.Services.AddScoped<IGreetingPort, GreetingPort>();
        builder.Services.AddSeedWorkApplication(new CommonPipeline(), new CommandPipeline(), new ScenarioPipeline());

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
        return app;
    }
}
