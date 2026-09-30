using System.Reflection;
using System.Diagnostics.CodeAnalysis;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace SeedWork.Messaging.Kafka;

/// <summary>Настройки одного подключения Kafka; секреты и параметры SSL/SASL предоставляет приложение.</summary>
public sealed class KafkaOptions
{
    /// <summary>Общие параметры клиентов; адаптер создаёт независимые копии для admin, producer и consumer.</summary>
    public ClientConfig Client { get; } = new();
    /// <summary>Проверка существующих topics либо создание недостающих при запуске.</summary>
    public TopologyMode Topology { get; set; } = TopologyMode.ValidateOnly;
}

internal sealed record KafkaTopic(string Name, int Partitions, short ReplicationFactor);
internal sealed record KafkaEndpoint(string Name, string Topic, string Group,
    Func<TransportMessage, ConsumerDispatcher, CancellationToken, Task<ConsumerFailure?>> Dispatch);

/// <summary>Описывает topics, маршруты публикации и consumer groups одного подключения Kafka.</summary>
public sealed class KafkaBuilder
{
    internal KafkaOptions Options { get; } = new();
    internal Dictionary<string, KafkaTopic> Topics { get; } = new(StringComparer.Ordinal);
    internal List<KafkaEndpoint> Endpoints { get; } = [];
    private readonly List<KafkaConsumerOptions> _consumerTopology = [];
    private readonly MessagingBuilder _messaging;
    private readonly string _name;
    private readonly HashSet<(Type Consumer, Type Message)> _registeredConsumers = [];
    private readonly HashSet<(Type Consumer, Type Message)> _scannedConsumers = [];
    private readonly HashSet<string> _routeTopics = new(StringComparer.Ordinal);
    internal KafkaBuilder(MessagingBuilder messaging, string name) => (_messaging, _name) = (messaging, name);

    /// <summary>Задаёт параметры клиентов Kafka и режим управления топологией.</summary>
    public KafkaBuilder Configure(Action<KafkaOptions> configure) { configure(Options); return this; }
    /// <summary>Описывает topic с ожидаемым числом партиций и реплик; существующие параметры автоматически не меняются.</summary>
    public KafkaBuilder Topic(string name, int partitions, short replicationFactor)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        if (partitions <= 0 || replicationFactor <= 0) throw new ArgumentOutOfRangeException(nameof(partitions));
        var topic = new KafkaTopic(name, partitions, replicationFactor);
        if (!Topics.TryAdd(name, topic) && Topics[name] != topic)
            throw new ArgumentException($"Conflicting topology for Kafka topic '{name}'.");
        return this;
    }
    /// <summary>Связывает маршрут одного типа сообщения с явно описанным topic.</summary>
    public KafkaBuilder Publish<T>(string route, string topic) where T : class
    {
        _routeTopics.Add(topic);
        _messaging.AddRoute<T>(route, _name, topic);
        return this;
    }
    /// <summary>Регистрирует обработку topic в consumer group с собственной политикой повторов.</summary>
    /// <remarks>Экземпляры сервиса используют одну группу для распределения партиций; отдельная группа получает независимый поток.</remarks>
    public KafkaBuilder Consume<T, TConsumer>(string endpoint, string topic, string group,
        Action<RetryOptions>? configureRetry = null) where T : class where TConsumer : class, IConsumer<T>
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(group);
        if (_scannedConsumers.Contains((typeof(TConsumer), typeof(T))))
            throw new InvalidOperationException($"Consumer '{typeof(TConsumer).FullName}' for message '{typeof(T).FullName}' is already registered by scanning on '{_name}'.");
        _messaging.AddConsumer<T, TConsumer>(endpoint);
        var retry = new RetryOptions();
        configureRetry?.Invoke(retry);
        retry.Validate();
        Endpoints.Add(new(endpoint, topic, group,
            (message, dispatcher, ct) => dispatcher.DispatchAsync<T, TConsumer>(message, _name, endpoint, retry, _messaging.Json, ct)));
        _registeredConsumers.Add((typeof(TConsumer), typeof(T)));
        return this;
    }

    /// <summary>Находит в переданной сборке консумеры с атрибутом или конфигурацией Kafka и регистрирует их подписки.</summary>
    /// <remarks>Конфигурация переопределяет атрибут. Ненастроенные консумеры пропускаются; топология берётся из итоговых настроек.</remarks>
    [RequiresUnreferencedCode("Consumer scanning requires untrimmed consumer and configuration types. Use explicit Consume registration when trimming.")]
    [RequiresDynamicCode("Consumer scanning closes generic registration methods at runtime. Use explicit Consume registration for NativeAOT.")]
    public KafkaBuilder AddConsumersFromAssembly(Assembly assembly, IConfiguration? configuration = null)
    {
        ArgumentNullException.ThrowIfNull(assembly);
        configuration ??= new ConfigurationBuilder().Build();
        var registrations = ConsumerDiscovery.Find<KafkaConsumerAttribute>(assembly,
            typeof(KafkaConsumerConfiguration<,>), a => a.MessageType);
        foreach (var registration in registrations)
        {
            var pair = (registration.Consumer, registration.Message);
            if (_registeredConsumers.Contains(pair))
                throw new InvalidOperationException($"Consumer '{pair.Consumer.FullName}' for message '{pair.Message.FullName}' is already registered on '{_name}'.");
            var attribute = (KafkaConsumerAttribute?)registration.Attribute;
            var options = new KafkaConsumerOptions
            {
                Endpoint = attribute?.Endpoint ?? ConsumerDiscovery.EndpointName(_name, registration),
                Topic = attribute?.Topic, Group = attribute?.Group,
                Partitions = attribute?.Partitions ?? 0, ReplicationFactor = attribute?.ReplicationFactor ?? 0
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
            if (string.IsNullOrWhiteSpace(options.Endpoint) || string.IsNullOrWhiteSpace(options.Topic) || string.IsNullOrWhiteSpace(options.Group))
                throw new InvalidOperationException($"Incomplete Kafka configuration for consumer '{pair.Consumer.FullName}', message '{pair.Message.FullName}'.");
            if (options.Retry is null)
                throw new InvalidOperationException($"Retry policy for consumer '{pair.Consumer.FullName}' cannot be null. Use RetryOptions.NoRetry().");
            options.Retry.Validate();
            ConsumerDiscovery.Register(this, registration, options.Endpoint, options.Topic, options.Group,
                (Action<RetryOptions>)(r => ConsumerDiscovery.CopyRetry(options.Retry, r)));
            _consumerTopology.Add(options);
            _scannedConsumers.Add(pair);
        }
        return this;
    }
    internal void Validate()
    {
        // Частичные объявления объединяются после завершения настройки подключения.
        foreach (var group in _consumerTopology.GroupBy(o => o.Topic!))
        {
            Topics.TryGetValue(group.Key, out var existing);
            if (group.Any(o => o.Partitions < 0 || o.ReplicationFactor < 0))
                throw new ArgumentException($"Invalid topology for Kafka topic '{group.Key}'.");
            var partitions = group.Select(o => o.Partitions).Append(existing?.Partitions ?? 0).Where(v => v != 0).Distinct().ToArray();
            var replicas = group.Select(o => o.ReplicationFactor).Append(existing?.ReplicationFactor ?? (short)0).Where(v => v != 0).Distinct().ToArray();
            if (partitions.Length != 1 || replicas.Length != 1)
                throw new ArgumentException($"Incomplete or conflicting topology for Kafka topic '{group.Key}': specify partitions and replication factor.");
            Topic(group.Key, partitions[0], replicas[0]);
        }

        if (string.IsNullOrWhiteSpace(Options.Client.BootstrapServers)) throw new ArgumentException("Kafka BootstrapServers is required.");
        if (_routeTopics.Concat(Endpoints.Select(e => e.Topic)).Any(t => !Topics.ContainsKey(t)))
            throw new ArgumentException("Declare every topic with Topic(...).");
        if (Endpoints.GroupBy(e => (e.Topic, e.Group)).Any(g => g.Count() > 1))
            throw new ArgumentException("A topic/group pair must have one endpoint within a connection.");
    }
}

public static class KafkaRegistrationExtensions
{
    /// <summary>Добавляет именованный адаптер Kafka с общим producer и отдельным consumer для каждого endpoint.</summary>
    public static MessagingBuilder AddKafka(this MessagingBuilder messaging, string name, Action<KafkaBuilder> configure)
    {
        var builder = new KafkaBuilder(messaging, name);
        configure(builder);
        builder.Validate();
        messaging.AddConnection(name, sp => new KafkaTransport(name, builder,
            sp.GetRequiredService<ConsumerDispatcher>(), sp.GetRequiredService<ILogger<KafkaTransport>>()));
        return messaging;
    }
}
