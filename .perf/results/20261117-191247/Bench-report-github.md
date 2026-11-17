```

BenchmarkDotNet v0.15.8, Windows 11 (10.0.26300.9974)
Unknown processor
.NET SDK 11.0.100
  [Host] : .NET 11.0.0 (11.0.0, 11.0.26.53018), X64 RyuJIT x86-64-v3

Job=Dry  Toolchain=InProcessEmitToolchain  IterationCount=1  
LaunchCount=1  RunStrategy=ColdStart  UnrollFactor=1  
WarmupCount=1  

```
| Method       | Count | Mean       | Error | Ratio | Allocated | Alloc Ratio |
|------------- |------ |-----------:|------:|------:|----------:|------------:|
| **Direct**       | **0**     |   **662.5 μs** |    **NA** |  **1.00** |         **-** |          **NA** |
| Pipeline     | 0     | 1,011.4 μs |    NA |  1.53 |         - |          NA |
| Registration | 0     |   189.2 μs |    NA |  0.29 |         - |          NA |
|              |       |            |       |       |           |             |
| **Direct**       | **1**     |   **391.8 μs** |    **NA** |  **1.00** |         **-** |          **NA** |
| Pipeline     | 1     |   647.0 μs |    NA |  1.65 |         - |          NA |
| Registration | 1     |   174.7 μs |    NA |  0.45 |         - |          NA |
|              |       |            |       |       |           |             |
| **Direct**       | **3**     |   **432.8 μs** |    **NA** |  **1.00** |         **-** |          **NA** |
| Pipeline     | 3     |   834.2 μs |    NA |  1.93 |         - |          NA |
| Registration | 3     |   163.4 μs |    NA |  0.38 |         - |          NA |
|              |       |            |       |       |           |             |
| **Direct**       | **8**     |   **468.1 μs** |    **NA** |  **1.00** |         **-** |          **NA** |
| Pipeline     | 8     |   765.2 μs |    NA |  1.63 |    8344 B |          NA |
| Registration | 8     |   162.5 μs |    NA |  0.35 |         - |          NA |
