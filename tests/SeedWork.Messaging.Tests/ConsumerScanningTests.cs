using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.ExceptionServices;
using Microsoft.Extensions.DependencyInjection;
using SeedWork.Messaging.Kafka;
using SeedWork.Messaging.RabbitMQ;

namespace SeedWork.Messaging.Tests;

[Collection("Messaging")]
public sealed class ConsumerScanningTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AttributeOnly_RegistersScopedConsumer(bool kafka)
    {
        var fixture = new Fixture();
        var consumer = fixture.Consumer(typeof(NoopConsumer), Attribute(kafka));
        var services = Register(kafka, fixture, declareTopology: false);
        var registration = Assert.Single(services, s => s.ServiceType == consumer);
        Assert.Equal(ServiceLifetime.Scoped, registration.Lifetime);
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        await using var anotherScope = provider.CreateAsyncScope();
        var resolved = Assert.IsAssignableFrom<IConsumer<Message>>(scope.ServiceProvider.GetRequiredService(consumer));
        Assert.Same(resolved, scope.ServiceProvider.GetRequiredService(consumer));
        Assert.NotSame(resolved, anotherScope.ServiceProvider.GetRequiredService(consumer));
        await resolved.ConsumeAsync(new(new Message(), null, null, null, new Dictionary<string, string>(), "test", 1), default);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ConfigurationOnly_UsesConstantValues(bool kafka)
    {
        var fixture = new Fixture();
        var consumer = fixture.Consumer(typeof(NoopConsumer));
        fixture.Configuration(kafka, consumer, typeof(Message), options =>
        {
            if (options is RabbitMqConsumerOptions rabbit) { rabbit.Queue = "orders"; rabbit.Exchange = "events"; }
            if (options is KafkaConsumerOptions k) { k.Topic = "events"; k.Group = "orders"; k.Partitions = 2; k.ReplicationFactor = 1; }
        }, inherit: true);
        Assert.Contains(Register(kafka, fixture, declareTopology: false), s => s.ServiceType == consumer);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Configuration_OverridesAttribute_AndPreservesUntouchedValues(bool kafka)
    {
        var fixture = new Fixture();
        var consumer = fixture.Consumer(typeof(NoopConsumer), Attribute(kafka));
        fixture.Configuration(kafka, consumer, typeof(Message), options =>
        {
            if (options is RabbitMqConsumerOptions r)
            {
                Assert.Equal("events", r.Exchange);
                Assert.Equal("orders", r.Queue);
                Assert.Equal("#", r.BindingKey);
                Assert.Equal("rabbit/" + consumer.FullName + "/" + typeof(Message).FullName, r.Endpoint);
                r.Exchange = "override";
                r.Retry.MaxRetries = 7;
            }
            else if (options is KafkaConsumerOptions k)
            {
                Assert.Equal("events", k.Topic);
                Assert.Equal("orders", k.Group);
                Assert.Equal("kafka/" + consumer.FullName + "/" + typeof(Message).FullName, k.Endpoint);
                k.Topic = "override";
                k.Retry.MaxRetries = 7;
            }
        });
        // Совместное использование класса конфигурации и ручного объявления топологии.
        Assert.Contains(Register(kafka, fixture, destination: "override"), s => s.ServiceType == consumer);
    }

    [Fact]
    public void DifferentTransports_ConfigureSameConsumerIndependently()
    {
        var fixture = new Fixture();
        var consumer = fixture.Consumer(typeof(NoopConsumer), Attribute(false), Attribute(true));
        var services = new ServiceCollection();
        services.AddSeedWorkMessaging(b =>
        {
            b.AddRabbitMq("rabbit", r => { Prepare(r); Scan(r, fixture.Marker); });
            b.AddKafka("kafka", k => { Prepare(k); Scan(k, fixture.Marker); });
        });
        Assert.Single(services, s => s.ServiceType == consumer);
        Assert.Equal(2, services.Count(s => s.ServiceType == typeof(IMessageTransport)));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void MultipleContracts_RegisterEachPair(bool kafka)
    {
        var fixture = new Fixture();
        var consumer = fixture.Consumer(typeof(MultiConsumer), Attribute(kafka), Attribute(kafka, typeof(OtherMessage), "other"));
        var services = Register(kafka, fixture);
        Assert.Single(services, s => s.ServiceType == consumer);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void UnconfiguredAbstractOpenGenericAndInheritedAttributes_AreSkipped(bool kafka)
    {
        var fixture = new Fixture();
        var unconfigured = fixture.Consumer(typeof(NoopConsumer));
        var otherTransport = fixture.Consumer(typeof(NoopConsumer), Attribute(!kafka));
        var inheritedAttribute = fixture.Consumer(typeof(AttributedBase));
        var abstractType = fixture.Consumer(typeof(NoopConsumer), [Attribute(kafka)], abstractType: true);
        var openType = fixture.Consumer(typeof(NoopConsumer), [Attribute(kafka)], openGeneric: true);
        var services = Register(kafka, fixture);
        foreach (var skipped in new[] { unconfigured, otherTransport, inheritedAttribute, abstractType, openType })
            Assert.DoesNotContain(services, s => s.ServiceType == skipped);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void DuplicateAttributes_AreRejected(bool kafka)
    {
        var fixture = new Fixture();
        fixture.Consumer(typeof(NoopConsumer), Attribute(kafka), Attribute(kafka));
        Assert.Contains("Duplicate", Assert.Throws<InvalidOperationException>(() => Register(kafka, fixture)).Message);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void DuplicateConfigurations_AreRejected(bool kafka)
    {
        var fixture = new Fixture();
        var consumer = fixture.Consumer(typeof(NoopConsumer), Attribute(kafka));
        fixture.Configuration(kafka, consumer, typeof(Message), _ => { });
        fixture.Configuration(kafka, consumer, typeof(Message), _ => { });
        Assert.Contains("Duplicate configurations", Assert.Throws<InvalidOperationException>(() => Register(kafka, fixture)).Message);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void AttributeForUnimplementedContract_IsRejected(bool kafka)
    {
        var fixture = new Fixture();
        fixture.Consumer(typeof(NoopConsumer), Attribute(kafka, typeof(OtherMessage)));
        Assert.Contains("does not implement", Assert.Throws<InvalidOperationException>(() => Register(kafka, fixture)).Message);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void IncompleteSettingsAndInvalidRetry_AreRejected(bool kafka)
    {
        var incomplete = new Fixture();
        incomplete.Consumer(typeof(NoopConsumer), kafka
            ? new KafkaConsumerAttribute(typeof(Message))
            : new RabbitMqConsumerAttribute(typeof(Message)));
        Assert.Contains("Incomplete", Assert.Throws<InvalidOperationException>(() => Register(kafka, incomplete)).Message);
        var invalidRetry = new Fixture();
        var consumer = invalidRetry.Consumer(typeof(NoopConsumer), Attribute(kafka));
        invalidRetry.Configuration(kafka, consumer, typeof(Message), o =>
        {
            if (o is KafkaConsumerOptions k) k.Retry.MaxRetries = -1;
            if (o is RabbitMqConsumerOptions r) r.Retry.MaxRetries = -1;
        });
        Assert.Throws<ArgumentException>(() => Register(kafka, invalidRetry));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ConfigurationWithoutPublicParameterlessConstructor_IsRejected(bool kafka)
    {
        var fixture = new Fixture();
        var consumer = fixture.Consumer(typeof(NoopConsumer), Attribute(kafka));
        fixture.Configuration(kafka, consumer, typeof(Message), _ => { }, publicConstructor: false);
        Assert.Contains("public parameterless constructor", Assert.Throws<InvalidOperationException>(() => Register(kafka, fixture)).Message);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ConfigurationExceptions_AreNotWrapped(bool kafka)
    {
        var fixture = new Fixture();
        var consumer = fixture.Consumer(typeof(NoopConsumer), Attribute(kafka));
        var original = new FormatException("configuration failure");
        fixture.Configuration(kafka, consumer, typeof(Message), _ => throw original);
        Assert.Same(original, Assert.Throws<FormatException>(() => Register(kafka, fixture)));
    }

    [Theory]
    [InlineData(false, "scan-scan")]
    [InlineData(true, "scan-scan")]
    [InlineData(false, "manual-scan")]
    [InlineData(true, "manual-scan")]
    [InlineData(false, "scan-manual")]
    [InlineData(true, "scan-manual")]
    public void DuplicateScannedAndManualRegistrations_AreRejected(bool kafka, string order)
    {
        var fixture = new Fixture();
        var consumer = fixture.Consumer(typeof(NoopConsumer), Attribute(kafka));
        var services = new ServiceCollection();
        Assert.Throws<InvalidOperationException>(() => services.AddSeedWorkMessaging(b =>
        {
            void RegisterPair(object builder)
            {
                foreach (var step in order.Split('-'))
                    if (step == "scan") Scan(builder, fixture.Marker);
                    else Manual(builder, consumer);
            }
            if (kafka) b.AddKafka("kafka", k => { Prepare(k); RegisterPair(k); });
            else b.AddRabbitMq("rabbit", r => { Prepare(r); RegisterPair(r); });
        }));
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task AttributeRetry_IsApplied_AndConfigurationCanReplaceFilters(bool kafka, bool configure)
    {
        var fixture = new Fixture();
        Attribute attribute = kafka
            ? new KafkaConsumerAttribute(typeof(Message)) { Topic = "events", Group = "orders", Partitions = 2, ReplicationFactor = 1,
                MaxRetries = 2, IntervalMilliseconds = 0, MaxIntervalMilliseconds = 15, Exponential = true,
                Handle = [typeof(ArgumentException)], Ignore = [typeof(InvalidOperationException)] }
            : new RabbitMqConsumerAttribute(typeof(Message)) { Queue = "orders", Exchange = "events",
                MaxRetries = 2, IntervalMilliseconds = 0, MaxIntervalMilliseconds = 15, Exponential = true,
                Handle = [typeof(ArgumentException)], Ignore = [typeof(InvalidOperationException)] };
        var consumer = fixture.Consumer(typeof(FailingConsumer), attribute);
        if (configure) fixture.Configuration(kafka, consumer, typeof(Message), o =>
        {
            var retry = o is KafkaConsumerOptions k ? k.Retry : ((RabbitMqConsumerOptions)o).Retry;
            Assert.Equal(2, retry.MaxRetries);
            Assert.Equal(TimeSpan.Zero, retry.Interval);
            Assert.Equal(TimeSpan.FromMilliseconds(15), retry.MaxInterval);
            Assert.True(retry.Exponential);
            Assert.Contains(typeof(ArgumentException), retry.Handle);
            Assert.Contains(typeof(InvalidOperationException), retry.Ignore);
            retry.Handle.Clear();
            retry.Ignore.Clear();
        });
        object? captured = null;
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSeedWorkMessaging(b =>
        {
            if (kafka) b.AddKafka("k", k => { k.Configure(o => o.Connection = KafkaConnection()); Scan(k, fixture.Marker); captured = k; });
            else b.AddRabbitMq("r", r => { r.Configure(o => { o.Connection = RabbitConnection(); o.Topology = TopologyMode.CreateMissing; }); Scan(r, fixture.Marker); captured = r; });
        });
        var endpoints = (System.Collections.IEnumerable)captured!.GetType().GetProperty("Endpoints", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(captured)!;
        var endpoint = Assert.Single(endpoints.Cast<object>());
        var dispatch = (Func<TransportMessage, ConsumerDispatcher, CancellationToken, Task<ConsumerFailure?>>)endpoint.GetType().GetProperty("Dispatch")!.GetValue(endpoint)!;
        await using var provider = services.BuildServiceProvider();
        var failure = await dispatch(new("{}"u8.ToArray(), null, new Dictionary<string, string>()), provider.GetRequiredService<ConsumerDispatcher>(), default);
        Assert.Equal(configure ? 3 : 1, failure!.Attempts);
    }

    [Theory]
    [InlineData(false, false, false)]
    [InlineData(false, true, false)]
    [InlineData(false, false, true)]
    [InlineData(false, true, true)]
    [InlineData(true, false, false)]
    [InlineData(true, true, false)]
    [InlineData(true, false, true)]
    [InlineData(true, true, true)]
    public void SharedTopology_IsOrderIndependent(bool kafka, bool manualFirst, bool conflict)
    {
        var fixture = new Fixture();
        fixture.Consumer(typeof(NoopConsumer), kafka
            ? new KafkaConsumerAttribute(typeof(Message)) { Topic = "events", Group = "orders", Partitions = 2, ReplicationFactor = 1 }
            : new RabbitMqConsumerAttribute(typeof(Message)) { Exchange = "events", ExchangeType = RabbitMqExchangeType.Direct, Queue = "orders" });
        fixture.Consumer(typeof(NoopConsumer), kafka
            ? new KafkaConsumerAttribute(typeof(Message)) { Topic = "events", Group = "other" }
            : new RabbitMqConsumerAttribute(typeof(Message)) { Exchange = "events", ExchangeType = RabbitMqExchangeType.Direct, Queue = "other" });
        void RegisterTopology() => new ServiceCollection().AddSeedWorkMessaging(b =>
        {
            if (kafka) b.AddKafka("k", k =>
            {
                k.Configure(o => o.Connection = KafkaConnection());
                if (manualFirst) k.Topic("events", conflict ? 3 : 2, 1);
                Scan(k, fixture.Marker);
                if (!manualFirst) k.Topic("events", conflict ? 3 : 2, 1);
            });
            else b.AddRabbitMq("r", r =>
            {
                r.Configure(o => { o.Connection = RabbitConnection(); o.Topology = TopologyMode.CreateMissing; });
                if (manualFirst) r.Exchange("events", conflict ? RabbitMqExchangeType.Fanout : RabbitMqExchangeType.Direct);
                Scan(r, fixture.Marker);
                if (!manualFirst) r.Exchange("events", conflict ? RabbitMqExchangeType.Fanout : RabbitMqExchangeType.Direct);
            });
        });
        if (conflict) Assert.Throws<ArgumentException>(RegisterTopology);
        else RegisterTopology();
    }

    [Theory]
    [InlineData(0, 0)]
    [InlineData(2, 0)]
    [InlineData(-1, 1)]
    public void Kafka_MissingOrInvalidTopology_IsRejected(int partitions, short replicas)
    {
        var fixture = new Fixture();
        fixture.Consumer(typeof(NoopConsumer), new KafkaConsumerAttribute(typeof(Message))
            { Topic = "events", Group = "orders", Partitions = partitions, ReplicationFactor = replicas });
        Assert.Throws<ArgumentException>(() => Register(true, fixture, declareTopology: false));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void NullAssembly_IsRejected(bool kafka)
    {
        Assert.Throws<ArgumentNullException>(() => new ServiceCollection().AddSeedWorkMessaging(b =>
        {
            if (kafka) b.AddKafka("k", k => k.AddConsumersFromAssembly(null!));
            else b.AddRabbitMq("r", r => r.AddConsumersFromAssembly(null!));
        }));
    }

    public abstract class FailingConsumer : IConsumer<Message>
    {
        public Task ConsumeAsync(MessageContext<Message> context, CancellationToken cancellationToken)
            => throw new InvalidOperationException("retry probe");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Configuration_CanReplaceEverySubscriptionAndTopologyField(bool kafka)
    {
        var fixture = new Fixture();
        var consumer = fixture.Consumer(typeof(NoopConsumer), kafka
            ? new KafkaConsumerAttribute(typeof(Message)) { Endpoint = "old", Topic = "old", Group = "old", Partitions = 1, ReplicationFactor = 1 }
            : new RabbitMqConsumerAttribute(typeof(Message)) { Endpoint = "old", Exchange = "old", ExchangeType = RabbitMqExchangeType.Fanout, Queue = "old", BindingKey = "old", ErrorQueue = "old_error" });
        fixture.Configuration(kafka, consumer, typeof(Message), o =>
        {
            RetryOptions retry;
            if (o is KafkaConsumerOptions k)
            {
                k.Endpoint = "new"; k.Topic = "new"; k.Group = "new"; k.Partitions = 4; k.ReplicationFactor = 2;
                retry = k.Retry;
            }
            else
            {
                var r = (RabbitMqConsumerOptions)o;
                r.Endpoint = "new"; r.Queue = "new"; r.Exchange = "new"; r.ExchangeType = RabbitMqExchangeType.Direct;
                r.BindingKey = "new.key"; r.ErrorQueue = "new.error";
                retry = r.Retry;
            }
            retry.MaxRetries = 0; retry.Interval = TimeSpan.Zero; retry.MaxInterval = TimeSpan.Zero;
            retry.Exponential = true; retry.Handle.Add(typeof(Exception)); retry.Ignore.Add(typeof(FormatException));
        });
        object? captured = null;
        new ServiceCollection().AddSeedWorkMessaging(b =>
        {
            if (kafka) b.AddKafka("k", k => { k.Configure(o => o.Connection = KafkaConnection()); Scan(k, fixture.Marker); captured = k; });
            else b.AddRabbitMq("r", r => { r.Configure(o => { o.Connection = RabbitConnection(); o.Topology = TopologyMode.CreateMissing; }); Scan(r, fixture.Marker); captured = r; });
        });
        object Property(object target, string name) => target.GetType().GetProperty(name, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(target)!;
        var endpoint = Assert.Single(((System.Collections.IEnumerable)Property(captured!, "Endpoints")).Cast<object>());
        Assert.Equal("new", Property(endpoint, "Name"));
        var topology = (System.Collections.IDictionary)Property(captured!, kafka ? "Topics" : "Exchanges");
        Assert.Single(topology.Keys.Cast<object>());
        if (kafka)
        {
            Assert.Equal("new", Property(endpoint, "Topic")); Assert.Equal("new", Property(endpoint, "Group"));
            Assert.Equal(4, Property(topology["new"]!, "Partitions"));
            Assert.Equal((short)2, Property(topology["new"]!, "ReplicationFactor"));
        }
        else
        {
            Assert.Equal("new", Property(endpoint, "Exchange")); Assert.Equal("new", Property(endpoint, "Queue"));
            Assert.Equal("new.key", Property(endpoint, "BindingKey")); Assert.Equal("new.error", Property(endpoint, "ErrorQueue"));
            Assert.Equal("direct", topology["new"]);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void InvalidAttributeRetry_IsRejected(bool kafka)
    {
        var fixture = new Fixture();
        fixture.Consumer(typeof(NoopConsumer), kafka
            ? new KafkaConsumerAttribute(typeof(Message)) { Topic = "events", Group = "orders", MaxRetries = -1 }
            : new RabbitMqConsumerAttribute(typeof(Message)) { Queue = "orders", Exchange = "events", MaxRetries = -1 });
        Assert.Throws<ArgumentException>(() => Register(kafka, fixture));
    }

    [Theory]
    [InlineData(false, "manual")]
    [InlineData(true, "manual")]
    [InlineData(false, "attribute")]
    [InlineData(true, "attribute")]
    [InlineData(false, "configuration")]
    [InlineData(true, "configuration")]
    [InlineData(false, "replace")]
    [InlineData(true, "replace")]
    public async Task NoRetry_AllRegistrationPaths_InvokeConsumerOnce(bool kafka, string mode)
    {
        var fixture = new Fixture();
        var attribute = Attribute(kafka);
        if (mode == "replace")
        {
            if (attribute is KafkaConsumerAttribute k) { k.MaxRetries = 5; k.Handle = [typeof(Exception)]; }
            if (attribute is RabbitMqConsumerAttribute r) { r.MaxRetries = 5; r.Ignore = [typeof(FormatException)]; }
        }
        var consumer = fixture.Consumer(typeof(FailingConsumer), mode is "manual" or "configuration" ? [] : [attribute]);
        if (mode is "configuration" or "replace") fixture.Configuration(kafka, consumer, typeof(Message), o =>
        {
            if (o is KafkaConsumerOptions k)
            {
                k.Topic = "events"; k.Group = "orders"; k.Partitions = 2; k.ReplicationFactor = 1;
                if (mode == "replace") k.Retry = RetryOptions.NoRetry();
                Assert.Empty(k.Retry.Handle); Assert.Empty(k.Retry.Ignore);
            }
            else
            {
                var r = (RabbitMqConsumerOptions)o;
                r.Exchange = "events"; r.Queue = "orders";
                if (mode == "replace") r.Retry = RetryOptions.NoRetry();
                Assert.Empty(r.Retry.Handle); Assert.Empty(r.Retry.Ignore);
            }
        });
        object? captured = null;
        var services = new ServiceCollection();
        services.AddSeedWorkMessaging(b =>
        {
            void RegisterConsumer(object builder)
            {
                captured = builder;
                if (mode == "manual") Manual(builder, consumer);
                else Scan(builder, fixture.Marker);
            }
            if (kafka) b.AddKafka("k", k => { Prepare(k); RegisterConsumer(k); });
            else b.AddRabbitMq("r", r => { Prepare(r); RegisterConsumer(r); });
        });
        var endpoints = (System.Collections.IEnumerable)captured!.GetType().GetProperty("Endpoints", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(captured)!;
        var endpoint = Assert.Single(endpoints.Cast<object>());
        var dispatch = (Func<TransportMessage, ConsumerDispatcher, CancellationToken, Task<ConsumerFailure?>>)endpoint.GetType().GetProperty("Dispatch")!.GetValue(endpoint)!;
        await using var provider = services.BuildServiceProvider();
        var failure = await dispatch(new("{}"u8.ToArray(), null, new Dictionary<string, string>()), provider.GetRequiredService<ConsumerDispatcher>(), default);
        Assert.IsType<InvalidOperationException>(failure!.Exception);
        Assert.Equal(1, failure.Attempts);
    }

    [Fact]
    public void NoRetry_ReturnsIndependentPolicies()
    {
        var first = RetryOptions.NoRetry();
        first.MaxRetries = 5;
        first.Handle.Add(typeof(Exception));
        first.Ignore.Add(typeof(FormatException));
        var second = RetryOptions.NoRetry();
        Assert.NotSame(first, second);
        Assert.Equal(0, second.MaxRetries);
        Assert.Empty(second.Handle); Assert.Empty(second.Ignore);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void NullRetry_ReportsConsumerConfigurationError(bool kafka)
    {
        var fixture = new Fixture();
        var consumer = fixture.Consumer(typeof(NoopConsumer), Attribute(kafka));
        fixture.Configuration(kafka, consumer, typeof(Message), o =>
        {
            if (o is KafkaConsumerOptions k) k.Retry = null!;
            else ((RabbitMqConsumerOptions)o).Retry = null!;
        });
        var error = Assert.Throws<InvalidOperationException>(() => Register(kafka, fixture));
        Assert.Contains("RetryOptions.NoRetry()", error.Message);
        Assert.Contains(consumer.FullName!, error.Message);
    }

    [Theory]
    [InlineData(RabbitMqExchangeType.Topic, "topic")]
    [InlineData(RabbitMqExchangeType.Direct, "direct")]
    [InlineData(RabbitMqExchangeType.Fanout, "fanout")]
    public void ExchangeEnum_MapsToBrokerType(RabbitMqExchangeType type, string expected)
    {
        var fixture = new Fixture();
        fixture.Consumer(typeof(NoopConsumer), new RabbitMqConsumerAttribute(typeof(Message))
            { Queue = "orders", Exchange = "events", ExchangeType = type });
        RabbitMqBuilder? captured = null;
        new ServiceCollection().AddSeedWorkMessaging(b => b.AddRabbitMq("r", r =>
        {
            r.Configure(o => { o.Connection = RabbitConnection(); o.Topology = TopologyMode.CreateMissing; });
            Scan(r, fixture.Marker); captured = r;
        }));
        var exchanges = (Dictionary<string, string>)typeof(RabbitMqBuilder).GetProperty("Exchanges", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(captured)!;
        Assert.Equal(expected, exchanges["events"]);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void DefaultTopic_ConflictsWithExplicitDirect(bool manualFirst)
    {
        var fixture = new Fixture();
        fixture.Consumer(typeof(NoopConsumer), Attribute(false));
        Assert.Throws<ArgumentException>(() => new ServiceCollection().AddSeedWorkMessaging(b => b.AddRabbitMq("r", r =>
        {
            r.Configure(o => { o.Connection = RabbitConnection(); o.Topology = TopologyMode.CreateMissing; });
            if (manualFirst) r.Exchange("events", RabbitMqExchangeType.Direct);
            Scan(r, fixture.Marker);
            if (!manualFirst) r.Exchange("events", RabbitMqExchangeType.Direct);
        })));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void UnknownExchangeEnum_IsRejected(bool scan)
    {
        var fixture = new Fixture();
        fixture.Consumer(typeof(NoopConsumer), new RabbitMqConsumerAttribute(typeof(Message))
            { Queue = "orders", Exchange = "events", ExchangeType = (RabbitMqExchangeType)999 });
        Assert.Throws<ArgumentOutOfRangeException>(() => new ServiceCollection().AddSeedWorkMessaging(b => b.AddRabbitMq("r", r =>
        {
            r.Configure(o => { o.Connection = RabbitConnection(); o.Topology = TopologyMode.CreateMissing; });
            if (scan) Scan(r, fixture.Marker);
            else r.Exchange("events", (RabbitMqExchangeType)999);
        })));
    }

    private static ServiceCollection Register(bool kafka, Fixture fixture, string destination = "events", bool declareTopology = true)
    {
        var services = new ServiceCollection();
        services.AddSeedWorkMessaging(b =>
        {
            if (kafka) b.AddKafka("kafka", k => { k.Configure(o => o.Connection = KafkaConnection()); if (declareTopology) k.Topic(destination, 2, 1); Scan(k, fixture.Marker); });
            else b.AddRabbitMq("rabbit", r => { r.Configure(o => { o.Connection = RabbitConnection(); o.Topology = TopologyMode.CreateMissing; }); if (declareTopology) r.Exchange(destination); Scan(r, fixture.Marker); });
        });
        return services;
    }
    private static RabbitMqUriConnectionSettings RabbitConnection() => new(new Uri("amqp://guest:guest@localhost:5672/"));
    private static KafkaConnectionSettings KafkaConnection() => new() { BootstrapServers = "unused:9092" };
    private static void Prepare(RabbitMqBuilder builder, string destination = "events")
        => builder.Configure(o => { o.Connection = RabbitConnection(); o.Topology = TopologyMode.CreateMissing; }).Exchange(destination);
    private static void Prepare(KafkaBuilder builder, string destination = "events")
        => builder.Configure(o => o.Connection = KafkaConnection()).Topic(destination, 2, 1);
    private static Attribute Attribute(bool kafka, Type? message = null, string subscription = "orders") => kafka
        ? new KafkaConsumerAttribute(message ?? typeof(Message)) { Topic = "events", Group = subscription, Partitions = 2, ReplicationFactor = 1 }
        : new RabbitMqConsumerAttribute(message ?? typeof(Message)) { Queue = subscription, Exchange = "events" };
    private static void Scan(object builder, Type marker)
        {
        if (builder is KafkaBuilder kafka) kafka.AddConsumersFromAssembly(marker.Assembly);
        else ((RabbitMqBuilder)builder).AddConsumersFromAssembly(marker.Assembly);
    }
    private static void Manual(object builder, Type consumer)
        => Invoke(builder.GetType().GetMethod("Consume")!.MakeGenericMethod(typeof(Message), consumer), builder,
            builder is KafkaBuilder ? ["manual", "events", "manual", null] : ["manual", "manual", "events", "#", null, null]);
    private static void Invoke(MethodInfo method, object target, object?[] args)
    {
        try { method.Invoke(target, args); }
        catch (TargetInvocationException e) when (e.InnerException is not null) { ExceptionDispatchInfo.Capture(e.InnerException).Throw(); }
    }

    public sealed record Message;
    public sealed record OtherMessage;
    public abstract class NoopConsumer : IConsumer<Message>
    {
        public Task ConsumeAsync(MessageContext<Message> context, CancellationToken cancellationToken) => Task.CompletedTask;
    }
    public abstract class MultiConsumer : NoopConsumer, IConsumer<OtherMessage>
    {
        public Task ConsumeAsync(MessageContext<OtherMessage> context, CancellationToken cancellationToken) => Task.CompletedTask;
    }
    [RabbitMqConsumer(typeof(Message), Queue = "orders", Exchange = "events")]
    [KafkaConsumer(typeof(Message), Topic = "events", Group = "orders")]
    public abstract class AttributedBase : NoopConsumer;

    // Изолированные сборки позволяют проверять ошибочные декларации без загрязнения других сценариев сканирования.
    private sealed class Fixture
    {
        private readonly ModuleBuilder _module;
        private int _index;
        public Type Marker { get; }
        public Fixture()
        {
            var assembly = AssemblyBuilder.DefineDynamicAssembly(new AssemblyName("ConsumerFixture" + Guid.NewGuid().ToString("N")), AssemblyBuilderAccess.RunAndCollect);
            _module = assembly.DefineDynamicModule("Main");
            Marker = _module.DefineType("Marker", TypeAttributes.Public).CreateType()!;
        }
        public Type Consumer(Type parent, params Attribute[] attributes) => Consumer(parent, attributes, false, false);
        public Type Consumer(Type parent, Attribute[] attributes, bool abstractType = false, bool openGeneric = false)
        {
            var type = _module.DefineType("Consumer" + _index++, TypeAttributes.Public | (abstractType ? TypeAttributes.Abstract : 0), parent);
            if (openGeneric) type.DefineGenericParameters("T");
            type.DefineDefaultConstructor(MethodAttributes.Public);
            foreach (var attribute in attributes)
            {
                var attrType = attribute.GetType();
                var properties = attrType.GetProperties().Where(p => p.CanWrite).ToArray();
                type.SetCustomAttribute(new CustomAttributeBuilder(attrType.GetConstructor([typeof(Type)])!,
                    [attrType.GetProperty("MessageType")!.GetValue(attribute)], properties, properties.Select(p => p.GetValue(attribute)).ToArray()!));
            }
            return type.CreateType()!;
        }
        public void Configuration(bool kafka, Type consumer, Type message, Action<object> configure,
            bool inherit = false, bool publicConstructor = true)
        {
            var baseType = (kafka ? typeof(KafkaConsumerConfiguration<,>) : typeof(RabbitMqConsumerConfiguration<,>)).MakeGenericType(message, consumer);
            var optionsType = kafka ? typeof(KafkaConsumerOptions) : typeof(RabbitMqConsumerOptions);
            var type = _module.DefineType("Configuration" + _index++, TypeAttributes.Public | (inherit ? TypeAttributes.Abstract : 0), baseType);
            type.DefineDefaultConstructor(publicConstructor ? MethodAttributes.Public : MethodAttributes.Private);
            var callback = type.DefineField("Callback", typeof(Action<object>), FieldAttributes.Public | FieldAttributes.Static);
            var method = type.DefineMethod("Configure", MethodAttributes.Public | MethodAttributes.Virtual, typeof(void), [optionsType]);
            var il = method.GetILGenerator();
            il.Emit(OpCodes.Ldsfld, callback);
            il.Emit(OpCodes.Ldarg_1);
            il.Emit(OpCodes.Callvirt, typeof(Action<object>).GetMethod("Invoke")!);
            il.Emit(OpCodes.Ret);
            type.DefineMethodOverride(method, baseType.GetMethod("Configure")!);
            var configuredType = type.CreateType()!;
            configuredType.GetField("Callback")!.SetValue(null, configure);
            if (inherit)
            {
                var concrete = _module.DefineType("DerivedConfiguration" + _index++, TypeAttributes.Public, configuredType);
                concrete.DefineDefaultConstructor(MethodAttributes.Public);
                concrete.CreateType();
            }
        }
    }
}
