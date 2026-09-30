using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace SeedWork.Messaging;

/// <summary>Контракт адаптера брокера: подготовка, приём и отправка сериализованных сообщений.</summary>
public interface IMessageTransport : IAsyncDisposable
{
    /// <summary>Уникальное имя подключения, на которое ссылаются маршруты.</summary>
    string Name { get; }
    /// <summary>Подготавливает соединение и проверяет либо создаёт топологию до запуска консумеров.</summary>
    Task InitializeAsync(CancellationToken cancellationToken);
    /// <summary>Обслуживает приём сообщений и завершает обработчики при отмене.</summary>
    Task RunAsync(CancellationToken cancellationToken);
    /// <summary>Отправляет готовое сообщение в назначение адаптера с ожиданием подтверждения брокера.</summary>
    Task PublishAsync(string destination, TransportMessage message, CancellationToken cancellationToken);
}

/// <summary>Связывает подключение, назначение адаптера и точный тип публикуемого контракта.</summary>
public sealed record MessageRoute(string Connection, string Destination, Type MessageType);

/// <summary>Собирает конфигурацию шины до запуска host; изменение настроек после регистрации не поддерживается.</summary>
public sealed class MessagingBuilder
{
    /// <summary>Коллекция DI для регистрации зависимостей адаптеров и консумеров.</summary>
    public IServiceCollection Services { get; }
    /// <summary>Общие настройки JSON; становятся доступными только для чтения после регистрации шины.</summary>
    public JsonSerializerOptions Json { get; } = new(JsonSerializerDefaults.Web);
    internal Dictionary<string, MessageRoute> Routes { get; } = new(StringComparer.Ordinal);
    private readonly HashSet<string> _connections = new(StringComparer.Ordinal);
    private readonly HashSet<string> _endpoints = new(StringComparer.Ordinal);

    internal MessagingBuilder(IServiceCollection services) => Services = services;

    /// <summary>Регистрирует один экземпляр адаптера под уникальным именем подключения.</summary>
    public void AddConnection(string name, Func<IServiceProvider, IMessageTransport> factory)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        if (!_connections.Add(name)) throw new ArgumentException($"Duplicate connection: {name}");
        Services.AddSingleton(factory);
    }

    /// <summary>Добавляет маршрут для одного типа сообщения; назначение интерпретируется выбранным адаптером.</summary>
    public void AddRoute<T>(string name, string connection, string destination) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentException.ThrowIfNullOrWhiteSpace(destination);
        if (!Routes.TryAdd(name, new(connection, destination, typeof(T))))
            throw new ArgumentException($"Duplicate route: {name}");
    }

    /// <summary>Регистрирует scoped-консумер и уникальное имя endpoint; подписку на брокер создаёт адаптер.</summary>
    public void AddConsumer<T, TConsumer>(string endpoint) where T : class where TConsumer : class, IConsumer<T>
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(endpoint);
        if (!_endpoints.Add(endpoint)) throw new ArgumentException($"Duplicate endpoint: {endpoint}");
        Services.TryAddScoped<TConsumer>();
    }

    internal void Validate()
    {
        foreach (var route in Routes.Values)
            if (!_connections.Contains(route.Connection)) throw new ArgumentException($"Unknown connection: {route.Connection}");
        Json.MakeReadOnly(populateMissingResolver: true);
    }
}

public static class MessagingServiceCollectionExtensions
{
    /// <summary>Однократно регистрирует шину, издателя и службу управления жизненным циклом всех подключений.</summary>
    public static IServiceCollection AddSeedWorkMessaging(this IServiceCollection services, Action<MessagingBuilder> configure)
    {
        if (services.Any(x => x.ServiceType == typeof(MessagingBuilder)))
            throw new InvalidOperationException("Configure SeedWork messaging once per service collection.");
        var builder = new MessagingBuilder(services);
        configure(builder);
        builder.Validate();
        services.AddSingleton(builder);
        services.AddLogging();
        services.AddSingleton<ConsumerDispatcher>();
        services.AddSingleton<IMessagePublisher, MessagePublisher>();
        services.AddHostedService<MessagingHostedService>();
        return services;
    }
}

internal sealed class MessagingHostedService(IEnumerable<IMessageTransport> transports) : BackgroundService
{
    private readonly IMessageTransport[] _transports = transports.ToArray();
    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        // Ошибка топологии любого подключения должна обнаружиться до начала обработки сообщений.
        foreach (var transport in _transports) await transport.InitializeAsync(cancellationToken);
        await base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var tasks = _transports.Select(t => t.RunAsync(linked.Token)).ToList();
        try
        {
            while (tasks.Count > 0)
            {
                var completed = await Task.WhenAny(tasks);
                await completed;
                tasks.Remove(completed);
            }
        }
        finally
        {
            // При остановке или сбое одного транспорта отменяем остальные и ждём их завершения.
            await linked.CancelAsync();
            try { await Task.WhenAll(tasks); } catch when (linked.IsCancellationRequested) { }
        }
    }
}
