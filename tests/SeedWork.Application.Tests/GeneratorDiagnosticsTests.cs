using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using SeedWork.Application.Generators;

namespace SeedWork.Application.Tests;

public sealed class GeneratorDiagnosticsTests
{
    [Fact]
    public void DuplicateUseCasesProduceDiagnostic()
    {
        var result = Generate("""
            using SeedWork.Application;
            public sealed class Request { }
            public sealed class First : IUseCase<Request, string>
            {
                public System.Threading.Tasks.Task<string> ExecuteAsync(Request r, System.Threading.CancellationToken ct = default) => System.Threading.Tasks.Task.FromResult("");
            }
            public sealed class Second : IUseCase<Request, string>
            {
                public System.Threading.Tasks.Task<string> ExecuteAsync(Request r, System.Threading.CancellationToken ct = default) => System.Threading.Tasks.Task.FromResult("");
            }
            public sealed class Config : IApplicationPipelineConfiguration
            {
                public void Configure(IApplicationPipelineBuilder builder) { }
            }
            """);
        Assert.Contains(result.Diagnostics, d => d.Id == "SWA001");
    }

    [Fact]
    public void InvalidUseCaseProducesDiagnostic()
    {
        var result = Generate("""
            using SeedWork.Application;
            public sealed class Request { }
            public abstract class Handler : IUseCase<Request, string>
            {
                public abstract System.Threading.Tasks.Task<string> ExecuteAsync(Request r, System.Threading.CancellationToken ct = default);
            }
            public sealed class Config : IApplicationPipelineConfiguration
            {
                public void Configure(IApplicationPipelineBuilder builder) { }
            }
            """);
        Assert.Contains(result.Diagnostics, d => d.Id == "SWA002");
    }

    [Fact]
    public void ConfigurationIsNotRequiredForGeneration()
    {
        var result = Generate("""
            using SeedWork.Application;
            public sealed class Request { }
            public sealed class Handler : IUseCase<Request, string>
            {
                public System.Threading.Tasks.Task<string> ExecuteAsync(Request r, System.Threading.CancellationToken ct = default) => System.Threading.Tasks.Task.FromResult("");
            }
            """);
        Assert.DoesNotContain(result.Diagnostics, d => d.Severity == DiagnosticSeverity.Error);
        Assert.Contains("AddSeedWorkApplication", result.GeneratedTrees.Single().ToString());
    }

    [Fact]
    public void MultipleConfigurationsWithConstructorArgumentsAreAccepted()
    {
        var result = Generate("""
            using SeedWork.Application;
            public sealed class Request { }
            public sealed class Handler : IUseCase<Request, string>
            {
                public System.Threading.Tasks.Task<string> ExecuteAsync(Request r, System.Threading.CancellationToken ct = default) => System.Threading.Tasks.Task.FromResult("");
            }
            public sealed class First : IApplicationPipelineConfiguration
            {
                public First(string name) { }
                public void Configure(IApplicationPipelineBuilder builder) { }
            }
            public sealed class Second : IApplicationPipelineConfiguration
            {
                public void Configure(IApplicationPipelineBuilder builder) { }
            }
            """);
        Assert.DoesNotContain(result.Diagnostics, d => d.Severity == DiagnosticSeverity.Error);
        Assert.DoesNotContain("new global::First", result.GeneratedTrees.Single().ToString());
    }

    private static GeneratorDriverRunResult Generate(string source)
    {
        var references = ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!).Split(Path.PathSeparator)
            .Select(path => MetadataReference.CreateFromFile(path)).ToList();
        references.Add(MetadataReference.CreateFromFile(typeof(SeedWork.Application.IUseCase<,>).Assembly.Location));
        var compilation = CSharpCompilation.Create("GeneratorProbe", [CSharpSyntaxTree.ParseText(source)], references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));
        GeneratorDriver driver = CSharpGeneratorDriver.Create(new ApplicationGenerator());
        driver = driver.RunGenerators(compilation);
        return driver.GetRunResult();
    }
}
