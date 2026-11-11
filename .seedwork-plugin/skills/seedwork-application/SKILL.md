---
name: seedwork-application
description: Use when implementing use cases or configuring pipelines with SeedWork.Application, including its generated dependency injection registration. Do not assume the package is installed.
---

# SeedWork.Application

Inspect the consuming project's references, target framework, and SeedWork.Application version before changing code. The guidance below describes the repository's 0.1.0 API. Verify the installed version's contracts and follow the project's conventions; do not install or upgrade a package unless requested.

- Implement `IUseCase<TRequest, TResponse>` with `Task<TResponse> ExecuteAsync(TRequest request, CancellationToken cancellationToken = default)`. For operations without a payload, use `Unit` and `Unit.CompletedTask`.
- Register with the source-generated `services.AddSeedWorkApplication(...)` and resolve use cases from DI. Handlers and steps are scoped. Build the project if generated registration is not yet visible to the IDE.
- The generator discovers handlers and steps in the project being compiled: use concrete, top-level public or internal classes; handlers are non-generic. Do not assume discovery crosses project boundaries.
- Configure optional pipelines using `IApplicationPipelineConfiguration` and `IApplicationPipelineBuilder`: `AddGlobal`, `ForRequests<TMarker>()`, and `For<TRequest, TResponse>()`. Operations include `Add`, `InsertBefore`, `InsertAfter`, and `Remove`; verify the existing chain and ordering before modifying it.
- Steps implement `IUseCaseStep<TRequest, TResponse>` and may short-circuit by returning without calling `next`; if called, `next` may be called only once. Propagate cancellation to external asynchronous operations. Exceptions are not automatically converted into result objects.

For supported patterns and generator diagnostics, consult `src/SeedWork.Application/README.md` and `samples/Application` in the matching SeedWork source version when available.
