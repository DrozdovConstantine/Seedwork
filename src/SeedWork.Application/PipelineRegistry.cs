namespace SeedWork.Application;

/// <summary>Описание известного генератору сценария и совместимых шагов.</summary>
public sealed record PipelineDescriptor(Type RequestType, Type ResponseType, IReadOnlyList<Type> StepTypes);

/// <summary>Проверенные цепочки сценариев; создаётся сгенерированной регистрацией.</summary>
public sealed class PipelineRegistry
{
    private readonly Dictionary<(Type Request, Type Response), IReadOnlyList<Type>> _pipelines = new();

    /// <summary>Применяет конфигурации по порядку и проверяет каждую цепочку при регистрации приложения.</summary>
    public PipelineRegistry(IReadOnlyList<PipelineDescriptor> descriptors, IReadOnlyList<IApplicationPipelineConfiguration> configurations)
    {
        ArgumentNullException.ThrowIfNull(descriptors);
        ArgumentNullException.ThrowIfNull(configurations);
        var builder = new Builder(descriptors);
        for (var index = 0; index < configurations.Count; index++)
        {
            var configuration = configurations[index]
                ?? throw new ArgumentException($"Configuration at index {index} is null.", nameof(configurations));
            configuration.Configure(builder);
        }
        foreach (var descriptor in descriptors)
        {
            var key = (descriptor.RequestType, descriptor.ResponseType);
            if (_pipelines.ContainsKey(key))
                throw new InvalidOperationException($"Duplicate use case: {key}.");
            _pipelines.Add(key, builder.Build(descriptor));
        }
        builder.ValidateSelections();
    }

    /// <summary>Применяет одну конфигурацию.</summary>
    public PipelineRegistry(IReadOnlyList<PipelineDescriptor> descriptors, IApplicationPipelineConfiguration configuration)
        : this(descriptors, new[] { configuration ?? throw new ArgumentNullException(nameof(configuration)) }) { }

    /// <summary>Применяет конфигурацию, заданную делегатом.</summary>
    public PipelineRegistry(IReadOnlyList<PipelineDescriptor> descriptors, Action<IApplicationPipelineBuilder> configure)
        : this(descriptors, new DelegateConfiguration(configure)) { }

    /// <summary>Возвращает порядок шагов для сценария.</summary>
    public IReadOnlyList<Type> GetSteps<TRequest, TResponse>() => _pipelines[(typeof(TRequest), typeof(TResponse))];

    private sealed class DelegateConfiguration(Action<IApplicationPipelineBuilder> configure) : IApplicationPipelineConfiguration
    {
        public void Configure(IApplicationPipelineBuilder builder) => configure(builder);
    }

    private sealed class Builder(IReadOnlyList<PipelineDescriptor> descriptors) : IApplicationPipelineBuilder
    {
        private readonly List<Type> _global = new();
        private readonly List<(Type Marker, Action<List<Type>> Apply)> _groupOperations = new();
        private readonly Dictionary<(Type, Type), Selection> _selections = new();

        public IApplicationPipelineBuilder AddGlobal(Type stepType) { _global.Add(Required(stepType)); return this; }
        public IUseCasePipelineBuilder ForRequests<TMarker>()
        {
            var marker = typeof(TMarker);
            if (!marker.IsInterface)
                throw new InvalidOperationException($"Request marker '{marker}' must be an interface.");
            return new Selection(operation => _groupOperations.Add((marker, operation)));
        }
        public IUseCasePipelineBuilder For<TRequest, TResponse>() => Select(typeof(TRequest), typeof(TResponse));

        private Selection Select(Type request, Type response)
        {
            var key = (request, response);
            if (!_selections.TryGetValue(key, out var selection))
                _selections.Add(key, selection = new Selection());
            return selection;
        }

        public void ValidateSelections()
        {
            foreach (var key in _selections.Keys)
                if (!descriptors.Any(d => d.RequestType == key.Item1 && d.ResponseType == key.Item2))
                    throw new InvalidOperationException($"Unknown use case: {key}.");
        }

        public IReadOnlyList<Type> Build(PipelineDescriptor descriptor)
        {
            var steps = new List<Type>(_global);
            foreach (var (marker, apply) in _groupOperations)
                if (marker.IsAssignableFrom(descriptor.RequestType)) apply(steps);
            if (_selections.TryGetValue((descriptor.RequestType, descriptor.ResponseType), out var selection))
                selection.Apply(steps);
            foreach (var step in steps)
                if (!descriptor.StepTypes.Contains(step))
                    throw new InvalidOperationException($"Unknown or incompatible pipeline step '{step}' for '{descriptor.RequestType}'.");
            if (steps.Count != steps.Distinct().Count())
                throw new InvalidOperationException($"Duplicate pipeline step for '{descriptor.RequestType}'.");
            return steps.ToArray();
        }

        private static Type Required(Type? type) => type ?? throw new ArgumentNullException(nameof(type));

        private sealed class Selection : IUseCasePipelineBuilder
        {
            private readonly List<Action<List<Type>>> _operations = new();
            private readonly Action<Action<List<Type>>> _record;
            public Selection() => _record = _operations.Add;
            public Selection(Action<Action<List<Type>>> record) => _record = record;
            public IUseCasePipelineBuilder Add(Type stepType) { Required(stepType); _record(s => s.Add(stepType)); return this; }
            public IUseCasePipelineBuilder InsertBefore(Type anchorType, Type stepType) => Insert(anchorType, stepType, false);
            public IUseCasePipelineBuilder InsertAfter(Type anchorType, Type stepType) => Insert(anchorType, stepType, true);
            public IUseCasePipelineBuilder Remove(Type stepType)
            {
                Required(stepType);
                _record(s => { if (!s.Remove(stepType)) throw new InvalidOperationException($"Pipeline step '{stepType}' is absent."); });
                return this;
            }
            private IUseCasePipelineBuilder Insert(Type anchor, Type step, bool after)
            {
                Required(anchor); Required(step);
                _record(s =>
                {
                    var index = s.IndexOf(anchor);
                    if (index < 0) throw new InvalidOperationException($"Pipeline anchor '{anchor}' is absent.");
                    s.Insert(index + (after ? 1 : 0), step);
                });
                return this;
            }
            public void Apply(List<Type> steps) { foreach (var operation in _operations) operation(steps); }
        }
    }
}

/// <summary>Выполняет сгенерированную цепочку с результатом.</summary>
public static class PipelineInvoker
{
    /// <summary>Выполняет шаги по порядку и затем обработчик сценария.</summary>
    public static Task<TResponse> InvokeAsync<TRequest, TResponse>(TRequest request, CancellationToken cancellationToken,
        IReadOnlyList<Type> steps, Func<Type, IUseCaseStep<TRequest, TResponse>> resolve,
        Func<TRequest, CancellationToken, Task<TResponse>> execute)
    {
        Task<TResponse> Run(int index)
        {
            if (index == steps.Count) return execute(request, cancellationToken);
            var step = resolve(steps[index]);
            var called = 0;
            return step.InvokeAsync(request, cancellationToken, () =>
            {
                if (Interlocked.Exchange(ref called, 1) != 0)
                    throw new InvalidOperationException("Pipeline next can only be called once.");
                return Run(index + 1);
            });
        }
        return Run(0);
    }

}
