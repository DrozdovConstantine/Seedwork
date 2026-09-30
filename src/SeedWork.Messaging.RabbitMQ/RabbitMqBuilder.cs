using System.Reflection;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;

namespace SeedWork.Messaging.RabbitMQ;

/// <summary>Настройки одного подключения RabbitMQ; адрес, секреты и TLS предоставляет приложение.</summary>
public sealed class RabbitMqOptions
{
    /// <summary>Параметры AMQP-соединения; восстановлением управляет адаптер.</summary>
    public ConnectionFactory Connection { get; } = new();
    /// <summary>Проверка существующей топологии либо создание недостающих ресурсов при запуске.</summary>
    public TopologyMode Topology { get; set; } = TopologyMode.ValidateOnly;
    /// <summary>Базовый URI Management API с завершающим слешем; необходим для проверки топологии без изменений.</summary>
    public Uri? ManagementUri { get; set; }
    /// <summary>Задержка перед восстановлением соединения и всех его консумеров.</summary>
    public TimeSpan ReconnectInterval { get; set; } = TimeSpan.FromSeconds(5);
}

internal sealed record RabbitRoute(string Exchange, string RoutingKey);
internal sealed record RabbitEndpoint(string Name, string Queue, string Exchange, string BindingKey, string ErrorQueue,
    Func<TransportMessage, ConsumerDispatcher, CancellationToken, Task<ConsumerFailure?>> Dispatch);

/// <summary>Описывает exchanges, маршруты публикации и очереди обработки одного подключения RabbitMQ.</summary>
public sealed class RabbitMqBuilder
{
    internal RabbitMqOptions Options { get; } = new();
    internal Dictionary<string, string> Exchanges { get; } = new(StringComparer.Ordinal);
    internal Dictionary<string, RabbitRoute> Routes { get; } = new(StringComparer.Ordinal);
    internal List<RabbitEndpoint> Endpoints { get; } = [];
    private readonly List<RabbitMqConsumerOptions> _consumerTopology = [];
    private readonly MessagingBuilder _messaging;
    private readonly string _name;
    private readonly HashSet<(Type Consumer, Type Message)> _registeredConsumers = [];
    private readonly HashSet<(Type Consumer, Type Message)> _scannedConsumers = [];
    internal RabbitMqBuilder(MessagingBuilder messaging, string name) => (_messaging, _name) = (messaging, name);

    /// <summary>Задаёт параметры соединения, проверки топологии и восстановления.</summary>
    public RabbitMqBuilder Configure(Action<RabbitMqOptions> configure) { configure(Options); return this; }
    /// <summary>Описывает durable exchange типа topic, direct или fanout; существующий должен иметь совместимые параметры.</summary>
    public RabbitMqBuilder Exchange(string name, RabbitMqExchangeType type = RabbitMqExchangeType.Topic)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        var brokerType = type switch
        {
            RabbitMqExchangeType.Topic => ExchangeType.Topic,
            RabbitMqExchangeType.Direct => ExchangeType.Direct,
            RabbitMqExchangeType.Fanout => ExchangeType.Fanout,
            _ => throw new ArgumentOutOfRangeException(nameof(type), type, "Unknown RabbitMQ exchange type.")
        };
        if (!Exchanges.TryAdd(name, brokerType) && Exchanges[name] != brokerType) throw new ArgumentException($"Conflicting types for RabbitMQ exchange '{name}'.");
        return this;
    }

    /// <summary>Связывает маршрут с exchange и routing key; ключ отдельной публикации может переопределить routing key.</summary>
    public RabbitMqBuilder Publish<T>(string route, string exchange, string routingKey = "") where T : class
    {
        _messaging.AddRoute<T>(route, _name, route);
        Routes.Add(route, new(exchange, routingKey));
        return this;
    }

    /// <summary>Описывает последовательную обработку очереди, binding, повторы и отдельную очередь ошибок.</summary>
    /// <remarks>
    /// Binding key по умолчанию равен #, что соответствует всем ключам только для topic exchange.
    /// По умолчанию error queue называется как входная очередь с суффиксом _error.
    /// </remarks>
    public RabbitMqBuilder Consume<T, TConsumer>(string endpoint, string queue, string exchange,
        string bindingKey = "#", Action<RetryOptions>? configureRetry = null, string? errorQueue = null)
        where T : class where TConsumer : class, IConsumer<T>
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);
        if (_scannedConsumers.Contains((typeof(TConsumer), typeof(T))))
            throw new InvalidOperationException($"Consumer '{typeof(TConsumer).FullName}' for message '{typeof(T).FullName}' is already registered by scanning on '{_name}'.");
        _messaging.AddConsumer<T, TConsumer>(endpoint);
        var retry = new RetryOptions();
        configureRetry?.Invoke(retry);
        retry.Validate();
        Endpoints.Add(new(endpoint, queue, exchange, bindingKey, errorQueue ?? queue + "_error",
            (message, dispatcher, ct) => dispatcher.DispatchAsync<T, TConsumer>(message, _name, endpoint, retry, _messaging.Json, ct)));
        _registeredConsumers.Add((typeof(TConsumer), typeof(T)));
        return this;
    }

    /// <summary>Находит в переданной сборке консумеры с атрибутом или конфигурацией RabbitMQ и регистрирует их подписки.</summary>
    /// <remarks>Конфигурация переопределяет атрибут. Ненастроенные консумеры пропускаются; топология берётся из итоговых настроек.</remarks>
    [RequiresUnreferencedCode("Consumer scanning requires untrimmed consumer and configuration types. Use explicit Consume registration when trimming.")]
    [RequiresDynamicCode("Consumer scanning closes generic registration methods at runtime. Use explicit Consume registration for NativeAOT.")]
    public RabbitMqBuilder AddConsumersFromAssembly(Assembly assembly, IConfiguration? configuration = null)
    {
        ArgumentNullException.ThrowIfNull(assembly);
        configuration ??= new ConfigurationBuilder().Build();
        var registrations = ConsumerDiscovery.Find<RabbitMqConsumerAttribute>(assembly,
            typeof(RabbitMqConsumerConfiguration<,>), a => a.MessageType);
        foreach (var registration in registrations)
        {
            var pair = (registration.Consumer, registration.Message);
            if (_registeredConsumers.Contains(pair))
                throw new InvalidOperationException($"Consumer '{pair.Consumer.FullName}' for message '{pair.Message.FullName}' is already registered on '{_name}'.");
            var attribute = (RabbitMqConsumerAttribute?)registration.Attribute;
            var options = new RabbitMqConsumerOptions
            {
                Endpoint = attribute?.Endpoint ?? ConsumerDiscovery.EndpointName(_name, registration),
                Queue = attribute?.Queue, Exchange = attribute?.Exchange, ExchangeType = attribute?.ExchangeType ?? RabbitMqExchangeType.Topic,
                BindingKey = attribute?.BindingKey ?? "#", ErrorQueue = attribute?.ErrorQueue
            };
            if (attribute is not null)
            {
                options.Retry.MaxRetries = attribute.MaxRetries;
                options.Retry.Interval = TimeSpan.FromMilliseconds(attribute.IntervalMilliseconds);
                options.Retry.MaxInterval = TimeSpan.FromMilliseconds(attribute.MaxIntervalMilliseconds);
                options.Retry.Exponential = attribute.Exponential;
                options.Retry.Handle.UnionWith(attribute.Handle ?? throw new ArgumentException("Handle cannot be null."));
                options.Retry.Ignore.UnionWith(attribute.Ignore ?? throw new ArgumentException("Ignore cannot be null."));
            }
            ConsumerDiscovery.ApplyConfiguration(registration.Configuration, options, configuration);
            if (string.IsNullOrWhiteSpace(options.Endpoint) || string.IsNullOrWhiteSpace(options.Queue) ||
                string.IsNullOrWhiteSpace(options.Exchange) || options.BindingKey is null ||
                options.ErrorQueue is not null && string.IsNullOrWhiteSpace(options.ErrorQueue))
                throw new InvalidOperationException($"Incomplete RabbitMQ configuration for consumer '{pair.Consumer.FullName}', message '{pair.Message.FullName}'.");
            if (options.Retry is null)
                throw new InvalidOperationException($"Retry policy for consumer '{pair.Consumer.FullName}' cannot be null. Use RetryOptions.NoRetry().");
            options.Retry.Validate();
            ConsumerDiscovery.Register(this, registration, options.Endpoint, options.Queue, options.Exchange,
                options.BindingKey, (Action<RetryOptions>)(r => ConsumerDiscovery.CopyRetry(options.Retry, r)), options.ErrorQueue);
            _consumerTopology.Add(options);
            _scannedConsumers.Add(pair);
        }
        return this;
    }

    internal void Validate()
    {
        // Все подписки задают тип, включая Topic по умолчанию; конфликт не зависит от порядка объявлений.
        foreach (var options in _consumerTopology)
            Exchange(options.Exchange!, options.ExchangeType);

        if (Options.ReconnectInterval <= TimeSpan.Zero) throw new ArgumentException("Reconnect interval must be positive.");
        if (Options.Topology == TopologyMode.ValidateOnly && Options.ManagementUri is null)
            throw new ArgumentException("ValidateOnly requires ManagementUri to inspect queue arguments, exchanges and bindings without changing them.");
        if (Routes.Values.Any(r => !Exchanges.ContainsKey(r.Exchange)) || Endpoints.Any(e => !Exchanges.ContainsKey(e.Exchange)))
            throw new ArgumentException("Declare every exchange with Exchange(...).");
        var queues = Endpoints.SelectMany(e => new[] { e.Queue, e.ErrorQueue }).ToArray();
        if (queues.Any(string.IsNullOrWhiteSpace) || queues.Distinct(StringComparer.Ordinal).Count() != queues.Length)
            throw new ArgumentException("Consumer and error queues must be distinct within a connection.");
    }
}

public static class RabbitMqRegistrationExtensions
{
    /// <summary>Добавляет именованный адаптер RabbitMQ; маршруты могут сосуществовать с другими подключениями шины.</summary>
    public static MessagingBuilder AddRabbitMq(this MessagingBuilder messaging, string name, Action<RabbitMqBuilder> configure)
    {
        var builder = new RabbitMqBuilder(messaging, name);
        configure(builder);
        builder.Validate();
        messaging.AddConnection(name, sp => new RabbitMqTransport(name, builder,
            sp.GetRequiredService<ConsumerDispatcher>(), sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<RabbitMqTransport>>()));
        return messaging;
    }
}
