using System.Text;
using BenchmarkDotNet.Attributes;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using SeedWork.Application.Generators;

[MemoryDiagnoser]
public class GeneratorBench
{
    [Params("10/0", "10/5", "100/0", "100/5", "500/10")]
    public string Size { get; set; } = "";
    private CSharpCompilation compilation = null!;
    [GlobalSetup]
    public void Setup()
    {
        var parts = Size.Split('/'); int cases = int.Parse(parts[0]), steps = int.Parse(parts[1]);
        var source = new StringBuilder("using SeedWork.Application;\n");
        for (int i = 0; i < cases; i++)
            source.AppendLine($"public sealed class Request{i} {{ }} public sealed class Handler{i} : IUseCase<Request{i}, int> {{ public System.Threading.Tasks.Task<int> ExecuteAsync(Request{i} r, System.Threading.CancellationToken ct = default) => System.Threading.Tasks.Task.FromResult(42); }}");
        for (int i = 0; i < steps; i++)
            source.AppendLine($"public sealed class Step{i}<TRequest,TResponse> : IUseCaseStep<TRequest,TResponse> {{ public System.Threading.Tasks.Task<TResponse> InvokeAsync(TRequest r, System.Threading.CancellationToken ct, UseCaseNext<TResponse> next) => next(); }}");
        var references = ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!).Split(Path.PathSeparator).Select(path => MetadataReference.CreateFromFile(path)).ToList();
        references.Add(MetadataReference.CreateFromFile(typeof(SeedWork.Application.IUseCase<,>).Assembly.Location));
        references.Add(MetadataReference.CreateFromFile(typeof(Microsoft.Extensions.DependencyInjection.ServiceCollection).Assembly.Location));
        references.Add(MetadataReference.CreateFromFile(typeof(Microsoft.Extensions.DependencyInjection.IServiceCollection).Assembly.Location));
        compilation = CSharpCompilation.Create("Probe", [CSharpSyntaxTree.ParseText(source.ToString())], references, new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));
        var result = Run().GetRunResult();
        var generated = result.GeneratedTrees.Single().GetText().ToString();
        var errors = compilation.AddSyntaxTrees(result.GeneratedTrees).GetDiagnostics().Where(d => d.Severity == DiagnosticSeverity.Error).ToArray();
        if (result.Diagnostics.Any(d => d.Severity == DiagnosticSeverity.Error) || errors.Length != 0) throw new InvalidOperationException(string.Join("\n", errors.Select(e => e.ToString())));
        Console.WriteLine($"GENERATED_SIZE {Size} {Encoding.UTF8.GetByteCount(generated)} bytes {generated.Length} chars");
    }
    private GeneratorDriver Run() => CSharpGeneratorDriver.Create(new ApplicationGenerator()).RunGenerators(compilation);
    [Benchmark] public GeneratorDriver Generate() => Run();
}
