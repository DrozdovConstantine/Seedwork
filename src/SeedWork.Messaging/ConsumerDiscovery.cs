using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;

[assembly: InternalsVisibleTo("SeedWork.Messaging.RabbitMQ")]
[assembly: InternalsVisibleTo("SeedWork.Messaging.Kafka")]

namespace SeedWork.Messaging;

/// <summary>Поиск деклараций при настройке приложения; в обработке сообщений рефлексия не используется.</summary>
internal static class ConsumerDiscovery
{
    internal sealed record Registration(Type Consumer, Type Message, Attribute? Attribute, Type? Configuration);

    internal static IReadOnlyList<Registration> Find<TAttribute>(Assembly assembly, Type configurationBase,
        Func<TAttribute, Type> messageType) where TAttribute : Attribute
    {
        Type[] types;
        try { types = assembly.GetTypes(); }
        catch (ReflectionTypeLoadException exception)
        {
            // Частичное сканирование может незаметно оставить сервис без обязательных подписок.
            throw new InvalidOperationException($"Cannot load consumer types from assembly '{assembly.FullName}'.", exception);
        }
        var concrete = types.Where(t => t.IsClass && !t.IsAbstract && !t.ContainsGenericParameters)
            .OrderBy(t => t.FullName, StringComparer.Ordinal).ToArray();
        var registrations = new Dictionary<(Type Consumer, Type Message), Registration>();

        foreach (var consumer in concrete)
        {
            foreach (var attribute in consumer.GetCustomAttributes<TAttribute>(inherit: false))
            {
                var message = messageType(attribute);
                ValidatePair(consumer, message);
                var pair = (consumer, message);
                if (!registrations.TryAdd(pair, new(consumer, message, attribute, null)))
                    throw new InvalidOperationException($"Duplicate {typeof(TAttribute).Name} for consumer '{consumer.FullName}', message '{message.FullName}'.");
            }
        }

        foreach (var configuration in concrete)
        {
            var closedBase = FindBase(configuration, configurationBase);
            if (closedBase is null) continue;
            var arguments = closedBase.GetGenericArguments();
            var message = arguments[0];
            var consumer = arguments[1];
            ValidatePair(consumer, message);
            if (!concrete.Contains(consumer))
                throw new InvalidOperationException($"Configuration '{configuration.FullName}' targets consumer '{consumer.FullName}' outside the scanned concrete types.");
            if (configuration.GetConstructor(Type.EmptyTypes) is null)
                throw new InvalidOperationException($"Configuration '{configuration.FullName}' must have a public parameterless constructor.");
            var pair = (consumer, message);
            registrations.TryGetValue(pair, out var existing);
            if (existing?.Configuration is not null)
                throw new InvalidOperationException($"Duplicate configurations '{existing.Configuration.FullName}' and '{configuration.FullName}' for consumer '{consumer.FullName}', message '{message.FullName}'.");
            registrations[pair] = new(consumer, message, existing?.Attribute, configuration);
        }

        return registrations.Values.OrderBy(r => r.Consumer.FullName, StringComparer.Ordinal)
            .ThenBy(r => r.Message.FullName, StringComparer.Ordinal).ToArray();
    }

    private static Type? FindBase(Type type, Type definition)
    {
        for (var current = type.BaseType; current is not null; current = current.BaseType)
            if (current.IsGenericType && current.GetGenericTypeDefinition() == definition) return current;
        return null;
    }

    private static void ValidatePair(Type consumer, Type message)
    {
        if (message is null || message.IsValueType || message.ContainsGenericParameters ||
            !consumer.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IConsumer<>) &&
                i.GetGenericArguments()[0] == message))
            throw new InvalidOperationException($"Consumer '{consumer.FullName}' does not implement IConsumer for message '{message?.FullName}'.");
    }

    internal static void ApplyConfiguration(Type? type, object options, object configuration)
    {
        if (type is null) return;
        try
        {
            var instance = Activator.CreateInstance(type)!;
            type.GetMethod("Configure", [options.GetType(), typeof(Microsoft.Extensions.Configuration.IConfiguration)])!
                .Invoke(instance, [options, configuration]);
        }
        catch (TargetInvocationException exception) when (exception.InnerException is not null)
        {
            ExceptionDispatchInfo.Capture(exception.InnerException).Throw();
        }
    }

    internal static void Register(object builder, Registration registration, params object?[] arguments)
    {
        try
        {
            builder.GetType().GetMethod("Consume")!.MakeGenericMethod(registration.Message, registration.Consumer)
                .Invoke(builder, arguments);
        }
        catch (TargetInvocationException exception) when (exception.InnerException is not null)
        {
            ExceptionDispatchInfo.Capture(exception.InnerException).Throw();
        }
    }

    internal static string EndpointName(string connection, Registration registration)
        => $"{connection}/{registration.Consumer.FullName}/{registration.Message.FullName}";

    internal static void CopyRetry(RetryOptions source, RetryOptions target)
    {
        target.MaxRetries = source.MaxRetries;
        target.Interval = source.Interval;
        target.Exponential = source.Exponential;
        target.MaxInterval = source.MaxInterval;
        target.Handle.Clear();
        target.Handle.UnionWith(source.Handle);
        target.Ignore.Clear();
        target.Ignore.UnionWith(source.Ignore);
    }
}
