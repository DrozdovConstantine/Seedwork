```

BenchmarkDotNet v0.15.8, Windows 11 (10.0.26300.9974)
Unknown processor
.NET SDK 11.0.100
  [Host] : .NET 11.0.0 (11.0.0, 11.0.26.53018), X64 RyuJIT x86-64-v3

Job=Dry  Toolchain=InProcessEmitToolchain  IterationCount=1  
LaunchCount=1  RunStrategy=ColdStart  UnrollFactor=1  
WarmupCount=1  

```
| Method       | Mean     | Error | Allocated |
|------------- |---------:|------:|----------:|
| ShortCircuit | 467.5 μs |    NA |         - |
