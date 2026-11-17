using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Microsoft.Extensions.DependencyInjection;
using SeedWork.Application;
// BenchmarkDotNet 0.15.8 does not recognize .NET 11 in its out-of-process SDK validator.
if (!args.Contains("--inProcess", StringComparer.OrdinalIgnoreCase))
    args = [.. args, "--inProcess"];

try
{
    BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
}
catch (Exception exception)
{
    Console.Error.WriteLine(exception);
    Environment.ExitCode = 1;
}
[MemoryDiagnoser]
public class Bench
{
    private readonly Request request = new();
    private readonly Handler direct = new();
    private ServiceProvider provider = null!;
    private IServiceScope scope = null!;
    private IUseCase<Request, int> useCase = null!;
    [Params(0,1,3,8)] public int Count { get; set; }
    [GlobalSetup] public void Setup() { provider = Make().BuildServiceProvider(); scope = provider.CreateScope(); useCase = scope.ServiceProvider.GetRequiredService<IUseCase<Request,int>>(); }
    private ServiceCollection Make() { var s = new ServiceCollection(); s.AddSeedWorkApplication(new Config(Count, false)); return s; }
    [GlobalCleanup] public void Cleanup() { scope.Dispose(); provider.Dispose(); }
    [Benchmark(Baseline=true)] public Task<int> Direct() => direct.ExecuteAsync(request);
    [Benchmark] public Task<int> Pipeline() => useCase.ExecuteAsync(request);
    [Benchmark] public IServiceCollection Registration() => Make();
}
[MemoryDiagnoser]
public class ShortCircuitBench
{
    private readonly Request request = new(); private ServiceProvider provider = null!; private IServiceScope scope = null!; private IUseCase<Request,int> useCase = null!;
    [GlobalSetup] public void Setup() { var s = new ServiceCollection(); s.AddSeedWorkApplication(new Config(8, true)); provider=s.BuildServiceProvider(); scope=provider.CreateScope(); useCase=scope.ServiceProvider.GetRequiredService<IUseCase<Request,int>>(); }
    [GlobalCleanup] public void Cleanup() { scope.Dispose(); provider.Dispose(); }
    [Benchmark] public Task<int> ShortCircuit() => useCase.ExecuteAsync(request);
}
public sealed class Request { }
public sealed class Handler : IUseCase<Request,int> { public Task<int> ExecuteAsync(Request r, CancellationToken ct=default) => Task.FromResult(42); }
public sealed class Step1 : IUseCaseStep<Request,int> { public Task<int> InvokeAsync(Request r, CancellationToken ct, UseCaseNext<int> next) => next(); }
public sealed class Step2 : IUseCaseStep<Request,int> { public Task<int> InvokeAsync(Request r, CancellationToken ct, UseCaseNext<int> next) => next(); }
public sealed class Step3 : IUseCaseStep<Request,int> { public Task<int> InvokeAsync(Request r, CancellationToken ct, UseCaseNext<int> next) => next(); }
public sealed class Step4 : IUseCaseStep<Request,int> { public Task<int> InvokeAsync(Request r, CancellationToken ct, UseCaseNext<int> next) => next(); }
public sealed class Step5 : IUseCaseStep<Request,int> { public Task<int> InvokeAsync(Request r, CancellationToken ct, UseCaseNext<int> next) => next(); }
public sealed class Step6 : IUseCaseStep<Request,int> { public Task<int> InvokeAsync(Request r, CancellationToken ct, UseCaseNext<int> next) => next(); }
public sealed class Step7 : IUseCaseStep<Request,int> { public Task<int> InvokeAsync(Request r, CancellationToken ct, UseCaseNext<int> next) => next(); }
public sealed class Step8 : IUseCaseStep<Request,int> { public Task<int> InvokeAsync(Request r, CancellationToken ct, UseCaseNext<int> next) => next(); }
public sealed class Stop : IUseCaseStep<Request,int> { public Task<int> InvokeAsync(Request r, CancellationToken ct, UseCaseNext<int> next) => Task.FromResult(42); }
public sealed class A {} public sealed class B {} public sealed class C {} public sealed class D {} public sealed class E {} public sealed class F {} public sealed class G {} public sealed class H {}
public sealed class Config(int count, bool stop) : IApplicationPipelineConfiguration
{
 public void Configure(IApplicationPipelineBuilder builder) { var b=builder.For<Request,int>(); if(stop) b.Add(typeof(Stop)); Type[] types=[typeof(Step1),typeof(Step2),typeof(Step3),typeof(Step4),typeof(Step5),typeof(Step6),typeof(Step7),typeof(Step8)]; foreach(var t in types.Take(count)) b.Add(t); }
}

