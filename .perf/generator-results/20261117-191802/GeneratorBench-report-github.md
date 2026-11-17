```

BenchmarkDotNet v0.15.8, Windows 11 (10.0.26300.9974)
Unknown processor
.NET SDK 11.0.100
  [Host] : .NET 11.0.0 (11.0.0, 11.0.26.53018), X64 RyuJIT x86-64-v3

Job=Dry  Toolchain=InProcessEmitToolchain  IterationCount=1  
LaunchCount=1  RunStrategy=ColdStart  UnrollFactor=1  
WarmupCount=1  

```
| Method   | Size   | Mean       | Error | Gen0      | Allocated   |
|--------- |------- |-----------:|------:|----------:|------------:|
| **Generate** | **10/0**   |   **1.075 ms** |    **NA** |         **-** |   **120.98 KB** |
| **Generate** | **10/5**   |   **1.710 ms** |    **NA** |         **-** |   **339.35 KB** |
| **Generate** | **100/0**  |   **4.357 ms** |    **NA** |         **-** |   **966.73 KB** |
| **Generate** | **100/5**  |  **13.420 ms** |    **NA** |         **-** |  **3027.22 KB** |
| **Generate** | **500/10** | **136.057 ms** |    **NA** | **1000.0000** | **25260.86 KB** |
