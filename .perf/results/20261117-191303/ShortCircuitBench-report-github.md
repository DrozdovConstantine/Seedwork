```

BenchmarkDotNet v0.15.8, Windows 11 (10.0.26300.9974)
Unknown processor
.NET SDK 11.0.100
  [Host] : .NET 11.0.0 (11.0.0, 11.0.26.53018), X64 RyuJIT x86-64-v3

Job=ShortRun  Toolchain=InProcessEmitToolchain  IterationCount=3  
LaunchCount=1  WarmupCount=3  

```
| Method       | Mean     | Error    | StdDev   | Gen0   | Allocated |
|------------- |---------:|---------:|---------:|-------:|----------:|
| ShortCircuit | 90.65 ns | 30.57 ns | 1.676 ns | 0.0196 |     328 B |
