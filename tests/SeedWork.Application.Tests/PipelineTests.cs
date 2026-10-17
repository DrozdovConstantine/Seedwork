using Microsoft.Extensions.DependencyInjection;
using SeedWork.Application;

namespace SeedWork.Application.Tests;

public sealed record Create(string Value);
public sealed record Skip;
public sealed record VoidRequest;
public sealed record SkipUnit;
public sealed record FaultUnit;
public sealed record Fault;
public sealed record DoubleNext;
public sealed record Cancel;
public interface ICommand;
public interface IAudited;
public interface IQuery;
public sealed record GroupedCommand : ICommand, IAudited;
public sealed record IndividualCommand : ICommand;
public sealed record GroupedQuery : IQuery;

public sealed class Recorder
{
    public List<string> Events { get; } = new();
    public CancellationToken Token { get; set; }
    public int HandlerCalls { get; set; }
}

public sealed class AppConfiguration : IApplicationPipelineConfiguration
{
    public void Configure(IApplicationPipelineBuilder builder)
    {
        builder.AddGlobal(typeof(GlobalResultStep<,>));
        builder.For<IndividualCommand, Unit>()
            .Remove(typeof(CommandStep<,>))
            .Add(typeof(IndividualStep));
        builder.ForRequests<ICommand>().InsertAfter(typeof(GlobalResultStep<,>), typeof(CommandStep<,>));
        builder.ForRequests<IAudited>().InsertAfter(typeof(CommandStep<,>), typeof(AuditedStep<,>));
        builder.For<Create, string>().InsertAfter(typeof(GlobalResultStep<,>), typeof(ExtraStep));
        builder.For<Skip, string>().Remove(typeof(GlobalResultStep<,>)).Add(typeof(ShortCircuitStep));
        builder.For<DoubleNext, string>().Add(typeof(DoubleNextStep));
        builder.For<SkipUnit, Unit>().Add(typeof(ShortCircuitUnitStep));
    }
}

public sealed class ConfigAction(Action<IApplicationPipelineBuilder> apply) : IApplicationPipelineConfiguration
{
    public void Configure(IApplicationPipelineBuilder builder) => apply(builder);
}

public sealed class CreateHandler(Recorder recorder) : IUseCase<Create, string>
{
    public Task<string> ExecuteAsync(Create request, CancellationToken cancellationToken = default)
    {
        recorder.Events.Add("handler");
        recorder.HandlerCalls++;
        recorder.Token = cancellationToken;
        return Task.FromResult(request.Value);
    }
}

public sealed class SkipHandler(Recorder recorder) : IUseCase<Skip, string>
{
    public Task<string> ExecuteAsync(Skip request, CancellationToken cancellationToken = default)
    {
        recorder.HandlerCalls++;
        return Task.FromResult("handler");
    }
}

public sealed class VoidHandler(Recorder recorder) : IUseCase<VoidRequest, Unit>
{
    public Task<Unit> ExecuteAsync(VoidRequest request, CancellationToken cancellationToken = default)
    {
        recorder.Events.Add("void-handler");
        recorder.Token = cancellationToken;
        return Unit.CompletedTask;
    }
}

public sealed class SkipUnitHandler(Recorder recorder) : IUseCase<SkipUnit, Unit>
{
    public Task<Unit> ExecuteAsync(SkipUnit request, CancellationToken cancellationToken = default)
    {
        recorder.HandlerCalls++;
        return Unit.CompletedTask;
    }
}

public sealed class FaultUnitHandler : IUseCase<FaultUnit, Unit>
{
    public static readonly Exception Failure = new InvalidOperationException("unit failure");
    public Task<Unit> ExecuteAsync(FaultUnit request, CancellationToken cancellationToken = default)
        => Task.FromException<Unit>(Failure);
}

public sealed class FaultHandler : IUseCase<Fault, string>
{
    public static readonly Exception Failure = new InvalidOperationException("original");
    public Task<string> ExecuteAsync(Fault request, CancellationToken cancellationToken = default) => Task.FromException<string>(Failure);
}

public sealed class DoubleNextHandler(Recorder recorder) : IUseCase<DoubleNext, string>
{
    public Task<string> ExecuteAsync(DoubleNext request, CancellationToken cancellationToken = default)
    {
        recorder.HandlerCalls++;
        return Task.FromResult("done");
    }
}

public interface IExternalPort { Task<string> ReadAsync(CancellationToken cancellationToken); }
public sealed class Port(Recorder recorder) : IExternalPort
{
    public Task<string> ReadAsync(CancellationToken cancellationToken)
    {
        recorder.Token = cancellationToken;
        return Task.FromResult("port");
    }
}

public sealed class CancelHandler(IExternalPort port) : IUseCase<Cancel, string>
{
    public Task<string> ExecuteAsync(Cancel request, CancellationToken cancellationToken = default) => port.ReadAsync(cancellationToken);
}

public sealed class GroupedCommandHandler(Recorder recorder) : IUseCase<GroupedCommand, string>
{
    public Task<string> ExecuteAsync(GroupedCommand request, CancellationToken cancellationToken = default)
    {
        recorder.Events.Add("command-handler");
        return Task.FromResult("command");
    }
}

public sealed class IndividualCommandHandler(Recorder recorder) : IUseCase<IndividualCommand, Unit>
{
    public Task<Unit> ExecuteAsync(IndividualCommand request, CancellationToken cancellationToken = default)
    {
        recorder.Events.Add("individual-handler");
        return Unit.CompletedTask;
    }
}

public sealed class GroupedQueryHandler(Recorder recorder) : IUseCase<GroupedQuery, string>
{
    public Task<string> ExecuteAsync(GroupedQuery request, CancellationToken cancellationToken = default)
    {
        recorder.Events.Add("query-handler");
        return Task.FromResult("query");
    }
}

public sealed class GlobalResultStep<TRequest, TResponse>(Recorder recorder) : IUseCaseStep<TRequest, TResponse>
{
    public async Task<TResponse> InvokeAsync(TRequest request, CancellationToken cancellationToken, UseCaseNext<TResponse> next)
    {
        recorder.Events.Add("global-before");
        var response = await next();
        recorder.Events.Add("global-after");
        return response;
    }
}

public sealed class CommandStep<TRequest, TResponse>(Recorder recorder) : IUseCaseStep<TRequest, TResponse>
    where TRequest : ICommand
{
    public async Task<TResponse> InvokeAsync(TRequest request, CancellationToken cancellationToken, UseCaseNext<TResponse> next)
    {
        recorder.Events.Add("command-before");
        var response = await next();
        recorder.Events.Add("command-after");
        return response;
    }
}

public sealed class AuditedStep<TRequest, TResponse>(Recorder recorder) : IUseCaseStep<TRequest, TResponse>
    where TRequest : IAudited
{
    public async Task<TResponse> InvokeAsync(TRequest request, CancellationToken cancellationToken, UseCaseNext<TResponse> next)
    {
        recorder.Events.Add("audited-before");
        var response = await next();
        recorder.Events.Add("audited-after");
        return response;
    }
}

public sealed class FinalGroupStep<TRequest, TResponse> : IUseCaseStep<TRequest, TResponse>
{
    public Task<TResponse> InvokeAsync(TRequest request, CancellationToken cancellationToken, UseCaseNext<TResponse> next)
        => next();
}

public sealed class IndividualStep(Recorder recorder) : IUseCaseStep<IndividualCommand, Unit>
{
    public async Task<Unit> InvokeAsync(IndividualCommand request, CancellationToken cancellationToken, UseCaseNext<Unit> next)
    {
        recorder.Events.Add("individual-before");
        var response = await next();
        recorder.Events.Add("individual-after");
        return response;
    }
}

public sealed class ExtraStep(Recorder recorder) : IUseCaseStep<Create, string>
{
    public async Task<string> InvokeAsync(Create request, CancellationToken cancellationToken, UseCaseNext<string> next)
    {
        recorder.Events.Add("extra-before");
        var response = await next();
        recorder.Events.Add("extra-after");
        return response;
    }
}

public sealed class ShortCircuitStep : IUseCaseStep<Skip, string>
{
    public Task<string> InvokeAsync(Skip request, CancellationToken cancellationToken, UseCaseNext<string> next) => Task.FromResult("short");
}

public sealed class ShortCircuitUnitStep : IUseCaseStep<SkipUnit, Unit>
{
    public Task<Unit> InvokeAsync(SkipUnit request, CancellationToken cancellationToken, UseCaseNext<Unit> next)
        => Unit.CompletedTask;
}

public sealed class DoubleNextStep : IUseCaseStep<DoubleNext, string>
{
    public async Task<string> InvokeAsync(DoubleNext request, CancellationToken cancellationToken, UseCaseNext<string> next)
    {
        await next();
        return await next();
    }
}

public sealed class PipelineTests
{
    private static ServiceProvider Provider()
    {
        var services = new ServiceCollection();
        services.AddScoped<Recorder>();
        services.AddScoped<IExternalPort, Port>();
        services.AddSeedWorkApplication(new AppConfiguration());
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task ResultPipeline_OrdersStepsAndPassesCancellation()
    {
        using var provider = Provider();
        using var scope = provider.CreateScope();
        using var source = new CancellationTokenSource();
        var result = await scope.ServiceProvider.GetRequiredService<IUseCase<Create, string>>().ExecuteAsync(new Create("ok"), source.Token);
        var recorder = scope.ServiceProvider.GetRequiredService<Recorder>();
        Assert.Equal("ok", result);
        Assert.Equal(["global-before", "extra-before", "handler", "extra-after", "global-after"], recorder.Events);
        Assert.Equal(source.Token, recorder.Token);
    }

    [Fact]
    public async Task MarkerGroups_ApplyInConfigurationOrder()
    {
        using var provider = Provider();
        using var scope = provider.CreateScope();
        var result = await scope.ServiceProvider.GetRequiredService<IUseCase<GroupedCommand, string>>().ExecuteAsync(new GroupedCommand());
        Assert.Equal("command", result);
        Assert.Equal(["global-before", "command-before", "audited-before", "command-handler", "audited-after", "command-after", "global-after"],
            scope.ServiceProvider.GetRequiredService<Recorder>().Events);
    }

    [Fact]
    public async Task IndividualConfiguration_CanRemoveGroupStep()
    {
        using var provider = Provider();
        using var scope = provider.CreateScope();
        Assert.Equal(Unit.Value, await scope.ServiceProvider.GetRequiredService<IUseCase<IndividualCommand, Unit>>().ExecuteAsync(new IndividualCommand()));
        Assert.Equal(["global-before", "individual-before", "individual-handler", "individual-after", "global-after"],
            scope.ServiceProvider.GetRequiredService<Recorder>().Events);
    }

    [Fact]
    public async Task QueryDoesNotReceiveCommandSteps()
    {
        using var provider = Provider();
        using var scope = provider.CreateScope();
        Assert.Equal("query", await scope.ServiceProvider.GetRequiredService<IUseCase<GroupedQuery, string>>().ExecuteAsync(new GroupedQuery()));
        Assert.Equal(["global-before", "query-handler", "global-after"], scope.ServiceProvider.GetRequiredService<Recorder>().Events);
    }

    [Fact]
    public async Task UnitPipeline_ExecutesGlobalStep()
    {
        using var provider = Provider();
        using var scope = provider.CreateScope();
        using var source = new CancellationTokenSource();
        var result = await scope.ServiceProvider.GetRequiredService<IUseCase<VoidRequest, Unit>>().ExecuteAsync(new VoidRequest(), source.Token);
        Assert.Equal(Unit.Value, result);
        var recorder = scope.ServiceProvider.GetRequiredService<Recorder>();
        Assert.Equal(["global-before", "void-handler", "global-after"], recorder.Events);
        Assert.Equal(source.Token, recorder.Token);
    }

    [Fact]
    public async Task UnitPipeline_CanShortCircuit()
    {
        using var provider = Provider();
        using var scope = provider.CreateScope();
        var result = await scope.ServiceProvider.GetRequiredService<IUseCase<SkipUnit, Unit>>().ExecuteAsync(new SkipUnit());
        Assert.Equal(Unit.Value, result);
        Assert.Equal(0, scope.ServiceProvider.GetRequiredService<Recorder>().HandlerCalls);
        Assert.Equal(["global-before", "global-after"], scope.ServiceProvider.GetRequiredService<Recorder>().Events);
    }

    [Fact]
    public async Task ScenarioCanRemoveGlobalAndShortCircuit()
    {
        using var provider = Provider();
        using var scope = provider.CreateScope();
        var result = await scope.ServiceProvider.GetRequiredService<IUseCase<Skip, string>>().ExecuteAsync(new Skip());
        Assert.Equal("short", result);
        Assert.Equal(0, scope.ServiceProvider.GetRequiredService<Recorder>().HandlerCalls);
        Assert.Empty(scope.ServiceProvider.GetRequiredService<Recorder>().Events);
    }

    [Fact]
    public async Task ErrorRetainsItsIdentity()
    {
        using var provider = Provider();
        using var scope = provider.CreateScope();
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => scope.ServiceProvider.GetRequiredService<IUseCase<Fault, string>>().ExecuteAsync(new Fault()));
        Assert.Same(FaultHandler.Failure, error);
    }

    [Fact]
    public async Task UnitErrorRetainsItsIdentity()
    {
        using var provider = Provider();
        using var scope = provider.CreateScope();
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            scope.ServiceProvider.GetRequiredService<IUseCase<FaultUnit, Unit>>().ExecuteAsync(new FaultUnit()));
        Assert.Same(FaultUnitHandler.Failure, error);
    }

    [Fact]
    public async Task NextCannotBeCalledTwice()
    {
        using var provider = Provider();
        using var scope = provider.CreateScope();
        await Assert.ThrowsAsync<InvalidOperationException>(() => scope.ServiceProvider.GetRequiredService<IUseCase<DoubleNext, string>>().ExecuteAsync(new DoubleNext()));
        Assert.Equal(1, scope.ServiceProvider.GetRequiredService<Recorder>().HandlerCalls);
    }

    [Fact]
    public async Task CancellationReachesExternalPort()
    {
        using var provider = Provider();
        using var scope = provider.CreateScope();
        using var source = new CancellationTokenSource();
        Assert.Equal("port", await scope.ServiceProvider.GetRequiredService<IUseCase<Cancel, string>>().ExecuteAsync(new Cancel(), source.Token));
        Assert.Equal(source.Token, scope.ServiceProvider.GetRequiredService<Recorder>().Token);
    }

    [Fact]
    public void RegistrationsAreScoped()
    {
        using var provider = Provider();
        using var first = provider.CreateScope();
        using var second = provider.CreateScope();
        Assert.Same(first.ServiceProvider.GetRequiredService<IUseCase<Create, string>>(), first.ServiceProvider.GetRequiredService<IUseCase<Create, string>>());
        Assert.NotSame(first.ServiceProvider.GetRequiredService<IUseCase<Create, string>>(), second.ServiceProvider.GetRequiredService<IUseCase<Create, string>>());
        Assert.NotSame(first.ServiceProvider.GetRequiredService<Recorder>(), second.ServiceProvider.GetRequiredService<Recorder>());
    }

    [Fact]
    public void GeneratedRegistration_AcceptsZeroOneAndMultipleConfigurations()
    {
        var empty = new ServiceCollection();
        empty.AddSeedWorkApplication();
        Assert.Empty(empty.BuildServiceProvider().GetRequiredService<PipelineRegistry>().GetSteps<Create, string>());

        var single = new ServiceCollection();
        single.AddSeedWorkApplication(new ConfigAction(builder => builder.AddGlobal(typeof(GlobalResultStep<,>))));
        Assert.Equal([typeof(GlobalResultStep<,>)],
            single.BuildServiceProvider().GetRequiredService<PipelineRegistry>().GetSteps<Create, string>());

        var order = new List<string>();
        var multiple = new ServiceCollection();
        multiple.AddSeedWorkApplication(
            new ConfigAction(builder => { order.Add("first"); builder.AddGlobal(typeof(GlobalResultStep<,>)); }),
            new ConfigAction(builder =>
            {
                order.Add("second");
                builder.For<Create, string>().InsertAfter(typeof(GlobalResultStep<,>), typeof(ExtraStep));
            }));
        Assert.Equal(["first", "second"], order);
        Assert.Equal([typeof(GlobalResultStep<,>), typeof(ExtraStep)],
            multiple.BuildServiceProvider().GetRequiredService<PipelineRegistry>().GetSteps<Create, string>());
    }

    [Fact]
    public void GeneratedRegistration_RejectsNullsBeforeAddingServices()
    {
        var services = new ServiceCollection();
        Assert.Throws<ArgumentNullException>(() => services.AddSeedWorkApplication((IApplicationPipelineConfiguration[])null!));
        Assert.Throws<ArgumentException>(() => services.AddSeedWorkApplication(new IApplicationPipelineConfiguration[] { null! }));
        Assert.Empty(services);
        Assert.Throws<ArgumentNullException>(() => GeneratedApplicationRegistration.AddSeedWorkApplication(null!));
    }

    [Fact]
    public void GeneratedRegistration_RejectsInvalidCombinedPipeline()
    {
        var services = new ServiceCollection();
        var first = new ConfigAction(builder => builder.AddGlobal(typeof(GlobalResultStep<,>)));
        Assert.Throws<InvalidOperationException>(() => services.AddSeedWorkApplication(first, first));
        Assert.Empty(services);
        Assert.Throws<InvalidOperationException>(() => services.AddSeedWorkApplication(first,
            new ConfigAction(builder => builder.For<Create, string>()
                .InsertAfter(typeof(ShortCircuitStep), typeof(ExtraStep)))));
        Assert.Empty(services);
    }

    [Fact]
    public void InvalidConfigurationFailsAtRegistration()
    {
        var descriptors = new[] { new PipelineDescriptor(typeof(Create), typeof(string), [typeof(ExtraStep)]) };
        Assert.Throws<InvalidOperationException>(() => new PipelineRegistry(descriptors,
            builder => builder.For<Create, string>().InsertBefore(typeof(ShortCircuitStep), typeof(ExtraStep))));
        Assert.Throws<InvalidOperationException>(() => new PipelineRegistry(descriptors,
            builder => builder.AddGlobal(typeof(ShortCircuitStep))));
        Assert.Throws<InvalidOperationException>(() => new PipelineRegistry(descriptors,
            builder => builder.For<Create, string>().Add(typeof(ShortCircuitStep))));
        Assert.Throws<InvalidOperationException>(() => new PipelineRegistry(descriptors,
            builder => builder.For<Create, string>().Remove(typeof(ExtraStep))));
    }

    [Fact]
    public void ScenarioCanInsertBeforeGlobalStep()
    {
        var descriptors = new[] { new PipelineDescriptor(typeof(Create), typeof(string), [typeof(ExtraStep), typeof(GlobalResultStep<,>)]) };
        var registry = new PipelineRegistry(descriptors, builder =>
        {
            builder.AddGlobal(typeof(GlobalResultStep<,>));
            builder.For<Create, string>().InsertBefore(typeof(GlobalResultStep<,>), typeof(ExtraStep));
        });
        Assert.Equal([typeof(ExtraStep), typeof(GlobalResultStep<,>)], registry.GetSteps<Create, string>());
    }

    [Fact]
    public void MarkerOperations_KeepCallOrderAcrossOverlappingGroups()
    {
        var descriptors = new[]
        {
            new PipelineDescriptor(typeof(GroupedCommand), typeof(string),
                [typeof(GlobalResultStep<,>), typeof(CommandStep<,>), typeof(AuditedStep<,>), typeof(FinalGroupStep<,>)])
        };
        var registry = new PipelineRegistry(descriptors, builder =>
        {
            builder.AddGlobal(typeof(GlobalResultStep<,>));
            builder.ForRequests<ICommand>().Add(typeof(CommandStep<,>));
            builder.ForRequests<IAudited>().Add(typeof(AuditedStep<,>));
            builder.ForRequests<ICommand>().Add(typeof(FinalGroupStep<,>));
        });
        Assert.Equal([typeof(GlobalResultStep<,>), typeof(CommandStep<,>), typeof(AuditedStep<,>), typeof(FinalGroupStep<,>)],
            registry.GetSteps<GroupedCommand, string>());
    }

    [Fact]
    public void MarkerGroup_CanInsertBeforeAndRemoveSharedStep()
    {
        var descriptors = new[]
        {
            new PipelineDescriptor(typeof(GroupedCommand), typeof(string),
                [typeof(GlobalResultStep<,>), typeof(CommandStep<,>), typeof(AuditedStep<,>), typeof(FinalGroupStep<,>)])
        };
        var registry = new PipelineRegistry(descriptors, builder =>
        {
            builder.AddGlobal(typeof(GlobalResultStep<,>));
            builder.AddGlobal(typeof(FinalGroupStep<,>));
            builder.ForRequests<ICommand>()
                .InsertBefore(typeof(FinalGroupStep<,>), typeof(CommandStep<,>))
                .Remove(typeof(FinalGroupStep<,>))
                .Add(typeof(AuditedStep<,>));
        });
        Assert.Equal([typeof(GlobalResultStep<,>), typeof(CommandStep<,>), typeof(AuditedStep<,>)],
            registry.GetSteps<GroupedCommand, string>());
    }

    [Fact]
    public void InvalidMarkerConfigurationFailsAtRegistration()
    {
        var descriptors = new[]
        {
            new PipelineDescriptor(typeof(GroupedQuery), typeof(string), [typeof(GlobalResultStep<,>)])
        };
        Assert.Throws<InvalidOperationException>(() => new PipelineRegistry(descriptors,
            builder => builder.ForRequests<string>().Add(typeof(GlobalResultStep<,>))));
        Assert.Throws<InvalidOperationException>(() => new PipelineRegistry(descriptors,
            builder => builder.ForRequests<IQuery>().Add(typeof(CommandStep<,>))));
        Assert.Throws<InvalidOperationException>(() => new PipelineRegistry(descriptors,
            builder => builder.ForRequests<IQuery>().InsertAfter(typeof(CommandStep<,>), typeof(GlobalResultStep<,>))));
        Assert.Throws<InvalidOperationException>(() => new PipelineRegistry(descriptors, builder =>
        {
            builder.AddGlobal(typeof(GlobalResultStep<,>));
            builder.ForRequests<IQuery>().Add(typeof(GlobalResultStep<,>));
        }));
    }
}
