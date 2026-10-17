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
    public async Task Configuration_RegistersScopedConsumer(bool kafka)
    {
        var fixture = new Fixture();
        var consumer = fixture.Consumer(typeof(NoopConsumer));
        Configure(fixture, kafka, consumer);
        var services = Register(kafka, fixture, declareTopology: false);
        var registration = Assert.Single(services, s => s.ServiceType == consumer);
        Assert.Equal(ServiceLifetime.Scoped, registration.Lifetime);
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        await using var anotherScope = provider.CreateAsyncScope();
        var resolved = Assert.IsAssignableFrom<IConsumer<Message>>(scope.ServiceProvider.GetRequiredService(consumer));
        Assert.Same(resolved, scope.ServiceProvider.GetRequiredService(consumer));
        Assert.NotSame(resolved, anotherScope.ServiceProvider.GetRequiredService(consumer));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Configuration_UsesDefaultsAndDerivedConfiguration(bool kafka)
    {
        var fixture = new Fixture();
        var consumer = fixture.Consumer(typeof(NoopConsumer));
        fixture.Configuration(kafka, consumer, typeof(Message), options =>
        {
            Assert.Equal((kafka ? "kafka" : "rabbit") + "/" + consumer.FullName + "/" + typeof(Message).FullName,
                options is KafkaConsumerOptions k ? k.Endpoint : ((RabbitMqConsumerOptions)options).Endpoint);
            var retry = options is KafkaConsumerOptions kafkaOptions ? kafkaOptions.Retry : ((RabbitMqConsumerOptions)options).Retry;
            Assert.Equal(0, retry.MaxRetries);
            Assert.Empty(retry.Handle);
            if (options is KafkaConsumerOptions ko) { ko.Topic = "events"; ko.Group = "orders"; ko.Partitions = 2; ko.ReplicationFactor = 1; }
            else { var ro = (RabbitMqConsumerOptions)options; Assert.Equal("#", ro.BindingKey); Assert.Equal(RabbitMqExchangeType.Topic, ro.ExchangeType); ro.Queue = "orders"; ro.Exchange = "events"; }
        }, inherit: true);
        Assert.Contains(Register(kafka, fixture, declareTopology: false), s => s.ServiceType == consumer);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void TransportDefaults_AreCopiedBeforeConsumerConfiguration(bool kafka)
    {
        var fixture = new Fixture();
        var first = fixture.Consumer(typeof(NoopConsumer));
        var second = fixture.Consumer(typeof(NoopConsumer));
        RetryOptions? firstRetry = null;
        RetryOptions? secondRetry = null;
        fixture.Configuration(kafka, first, typeof(Message), o =>
        {
            if (o is KafkaConsumerOptions k)
            {
                Assert.Equal("shared", k.Group);
                k.Topic = "events";
                k.Group = "override";
                firstRetry = k.Retry;
            }
            else
            {
                var r = (RabbitMqConsumerOptions)o;
                Assert.Equal(RabbitMqExchangeType.Direct, r.ExchangeType);
                r.Queue = "first";
                r.Exchange = "other";
                r.ExchangeType = RabbitMqExchangeType.Topic;
                firstRetry = r.Retry;
            }
            Assert.Equal(2, firstRetry.MaxRetries);
            Assert.Contains(typeof(InvalidOperationException), firstRetry.Handle);
            firstRetry.MaxRetries = 3;
            firstRetry.Handle.Clear();
        });
        fixture.Configuration(kafka, second, typeof(Message), o =>
        {
            if (o is KafkaConsumerOptions k)
            {
                Assert.Equal("shared", k.Group);
                k.Topic = "events";
                secondRetry = k.Retry;
                k.Retry = RetryOptions.NoRetry();
            }
            else
            {
                var r = (RabbitMqConsumerOptions)o;
                Assert.Equal(RabbitMqExchangeType.Direct, r.ExchangeType);
                r.Queue = "second";
                r.Exchange = "events";
                secondRetry = r.Retry;
                r.Retry = RetryOptions.NoRetry();
            }
            Assert.Equal(2, secondRetry.MaxRetries);
            Assert.Contains(typeof(InvalidOperationException), secondRetry.Handle);
        });

        object? captured = null;
        var defaults = RetryOptions.NoRetry();
        defaults.MaxRetries = 2;
        defaults.Handle.Add(typeof(InvalidOperationException));
        new ServiceCollection().AddSeedWorkMessaging(b =>
        {
            if (kafka) b.AddKafka("k", k =>
            {
                k.Configure(o =>
                {
                    o.Connection = KafkaConnection();
                    o.ConsumerDefaults.Group = "shared";
                    o.ConsumerDefaults.Retry = defaults;
                }).Topic("events", 2, 1);
                Scan(k, fixture.Marker);
                captured = k;
            });
            else b.AddRabbitMq("r", r =>
            {
                r.Configure(o =>
                {
                    o.Connection = RabbitConnection();
                    o.Topology = TopologyMode.CreateMissing;
                    o.ConsumerDefaults.ExchangeType = RabbitMqExchangeType.Direct;
                    o.ConsumerDefaults.Retry = defaults;
                }).Exchange("events");
                Scan(r, fixture.Marker);
                captured = r;
            });
        });

        Assert.NotSame(defaults, firstRetry);
        Assert.NotSame(defaults, secondRetry);
        Assert.NotSame(firstRetry, secondRetry);
        Assert.Equal(2, defaults.MaxRetries);
        Assert.Contains(typeof(InvalidOperationException), defaults.Handle);
        var topology = (System.Collections.IDictionary)captured!.GetType()
            .GetProperty(kafka ? "Topics" : "Exchanges", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(captured)!;
        if (!kafka)
        {
            Assert.Equal("direct", topology["events"]);
            Assert.Equal("topic", topology["other"]);
        }
        else
        {
            var endpoints = ((System.Collections.IEnumerable)captured.GetType()
                .GetProperty("Endpoints", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(captured)!).Cast<object>();
            Assert.Equal(new[] { "override", "shared" }, endpoints.Select(e => (string)e.GetType().GetProperty("Group")!.GetValue(e)!).Order());
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ManualConsumers_InheritAndOverrideTransportRetry(bool kafka)
    {
        var fixture = new Fixture();
        var inherited = fixture.Consumer(typeof(FailingConsumer));
        var overridden = fixture.Consumer(typeof(FailingConsumer));
        var services = new ServiceCollection();
        object? captured = null;
        services.AddSeedWorkMessaging(b =>
        {
            if (kafka) b.AddKafka("k", k =>
            {
                k.Configure(o =>
                {
                    o.Connection = KafkaConnection();
                    o.ConsumerDefaults.Group = "shared";
                    o.ConsumerDefaults.Retry.MaxRetries = 1;
                    o.ConsumerDefaults.Retry.Interval = TimeSpan.Zero;
                }).Topic("events", 2, 1);
                var method = k.GetType().GetMethod("Consume")!;
                Invoke(method.MakeGenericMethod(typeof(Message), inherited), k, ["first", "events", null, null]);
                Invoke(method.MakeGenericMethod(typeof(Message), overridden), k,
                    ["second", "events", "other", (Action<RetryOptions>)(retry => retry.MaxRetries = 0)]);
                captured = k;
            });
            else b.AddRabbitMq("r", r =>
            {
                r.Configure(o =>
                {
                    o.Connection = RabbitConnection();
                    o.Topology = TopologyMode.CreateMissing;
                    o.ConsumerDefaults.ExchangeType = RabbitMqExchangeType.Direct;
                    o.ConsumerDefaults.Retry.MaxRetries = 1;
                    o.ConsumerDefaults.Retry.Interval = TimeSpan.Zero;
                }).Exchange("events");
                var method = r.GetType().GetMethod("Consume")!;
                Invoke(method.MakeGenericMethod(typeof(Message), inherited), r, ["first", "first", "events", "key", null, null]);
                Invoke(method.MakeGenericMethod(typeof(Message), overridden), r,
                    ["second", "second", "events", "key", (Action<RetryOptions>)(retry => retry.MaxRetries = 0), null]);
                captured = r;
            });
        });

        var endpoints = ((System.Collections.IEnumerable)captured!.GetType()
            .GetProperty("Endpoints", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(captured)!).Cast<object>().ToArray();
        await using var provider = services.BuildServiceProvider();
        Assert.Equal(2, (await Dispatch(endpoints[0], provider)).Attempts);
        Assert.Equal(1, (await Dispatch(endpoints[1], provider)).Attempts);
        if (kafka)
        {
            Assert.Equal("shared", endpoints[0].GetType().GetProperty("Group")!.GetValue(endpoints[0]));
            Assert.Equal("other", endpoints[1].GetType().GetProperty("Group")!.GetValue(endpoints[1]));
        }
        else
        {
            var exchanges = (System.Collections.IDictionary)captured.GetType()
                .GetProperty("Exchanges", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(captured)!;
            Assert.Equal("direct", exchanges["events"]);
        }
    }

    [Fact]
    public void DifferentTransports_ConfigureSameConsumerIndependently()
    {
        var fixture = new Fixture();
        var consumer = fixture.Consumer(typeof(NoopConsumer));
        Configure(fixture, false, consumer);
        Configure(fixture, true, consumer);
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
        var consumer = fixture.Consumer(typeof(MultiConsumer));
        Configure(fixture, kafka, consumer);
        Configure(fixture, kafka, consumer, typeof(OtherMessage), "other");
        var services = Register(kafka, fixture);
        Assert.Single(services, s => s.ServiceType == consumer);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void UnconfiguredAndOtherTransportConsumers_AreSkipped(bool kafka)
    {
        var fixture = new Fixture();
        var unconfigured = fixture.Consumer(typeof(NoopConsumer));
        var otherTransport = fixture.Consumer(typeof(NoopConsumer));
        Configure(fixture, !kafka, otherTransport);
        var configured = fixture.Consumer(typeof(NoopConsumer));
        Configure(fixture, kafka, configured);
        var services = Register(kafka, fixture);
        Assert.DoesNotContain(services, s => s.ServiceType == unconfigured || s.ServiceType == otherTransport);
        Assert.Contains(services, s => s.ServiceType == configured);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void DuplicateConfigurations_AreRejected(bool kafka)
    {
        var fixture = new Fixture();
        var consumer = fixture.Consumer(typeof(NoopConsumer));
        Configure(fixture, kafka, consumer);
        Configure(fixture, kafka, consumer);
        Assert.Contains("Duplicate configurations", Assert.Throws<InvalidOperationException>(() => Register(kafka, fixture)).Message);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ConfigurationForUnimplementedContract_IsRejected(bool kafka)
    {
        var fixture = new Fixture();
        var consumer = fixture.Consumer(typeof(NoopConsumer));
        Assert.Throws<ArgumentException>(() => fixture.Configuration(kafka, consumer, typeof(OtherMessage), _ => { }));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void IncompleteSettingsAndInvalidRetry_AreRejected(bool kafka)
    {
        var incomplete = new Fixture();
        var consumer = incomplete.Consumer(typeof(NoopConsumer));
        incomplete.Configuration(kafka, consumer, typeof(Message), _ => { });
        Assert.Contains("Incomplete", Assert.Throws<InvalidOperationException>(() => Register(kafka, incomplete)).Message);
        var invalid = new Fixture();
        consumer = invalid.Consumer(typeof(NoopConsumer));
        invalid.Configuration(kafka, consumer, typeof(Message), o =>
        {
            if (o is KafkaConsumerOptions k) { k.Topic = "events"; k.Group = "orders"; k.Retry.MaxRetries = -1; }
            else { var r = (RabbitMqConsumerOptions)o; r.Queue = "orders"; r.Exchange = "events"; r.Retry.MaxRetries = -1; }
        });
        Assert.Throws<ArgumentException>(() => Register(kafka, invalid));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ConfigurationWithoutPublicParameterlessConstructor_IsRejected(bool kafka)
    {
        var fixture = new Fixture();
        var consumer = fixture.Consumer(typeof(NoopConsumer));
        fixture.Configuration(kafka, consumer, typeof(Message), _ => { }, publicConstructor: false);
        Assert.Contains("public parameterless constructor", Assert.Throws<InvalidOperationException>(() => Register(kafka, fixture)).Message);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ConfigurationExceptions_AreNotWrapped(bool kafka)
    {
        var fixture = new Fixture();
        var consumer = fixture.Consumer(typeof(NoopConsumer));
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
        var consumer = fixture.Consumer(typeof(NoopConsumer));
        Configure(fixture, kafka, consumer);
        Assert.Throws<InvalidOperationException>(() => new ServiceCollection().AddSeedWorkMessaging(b =>
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
    [InlineData(false)]
    [InlineData(true)]
    public async Task ConfigurationRetry_IsApplied(bool kafka)
    {
        var fixture = new Fixture();
        var consumer = fixture.Consumer(typeof(FailingConsumer));
        fixture.Configuration(kafka, consumer, typeof(Message), o =>
        {
            RetryOptions retry;
            if (o is KafkaConsumerOptions k) { k.Topic = "events"; k.Group = "orders"; retry = k.Retry; }
            else { var r = (RabbitMqConsumerOptions)o; r.Queue = "orders"; r.Exchange = "events"; retry = r.Retry; }
            retry.MaxRetries = 2;
            retry.Interval = TimeSpan.Zero;
            retry.MaxInterval = TimeSpan.FromMilliseconds(15);
            retry.Exponential = true;
        });
        var (services, endpoint) = BuildEndpoint(kafka, fixture);
        await using var provider = services.BuildServiceProvider();
        var failure = await Dispatch(endpoint, provider);
        Assert.Equal(3, failure.Attempts);
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
        foreach (var group in new[] { "orders", "other" })
        {
            var consumer = fixture.Consumer(typeof(NoopConsumer));
            fixture.Configuration(kafka, consumer, typeof(Message), o =>
            {
                if (o is KafkaConsumerOptions k) { k.Topic = "events"; k.Group = group; if (group == "orders") { k.Partitions = 2; k.ReplicationFactor = 1; } }
                else { var r = (RabbitMqConsumerOptions)o; r.Exchange = "events"; r.Queue = group; r.ExchangeType = RabbitMqExchangeType.Direct; }
            });
        }
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
        var consumer = fixture.Consumer(typeof(NoopConsumer));
        fixture.Configuration(true, consumer, typeof(Message), o =>
        {
            var k = (KafkaConsumerOptions)o;
            k.Topic = "events"; k.Group = "orders"; k.Partitions = partitions; k.ReplicationFactor = replicas;
        });
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

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Configuration_CanSetEverySubscriptionAndTopologyField(bool kafka)
    {
        var fixture = new Fixture();
        var consumer = fixture.Consumer(typeof(NoopConsumer));
        fixture.Configuration(kafka, consumer, typeof(Message), o =>
        {
            if (o is KafkaConsumerOptions k) { k.Endpoint = "new"; k.Topic = "new"; k.Group = "new"; k.Partitions = 4; k.ReplicationFactor = 2; }
            else { var r = (RabbitMqConsumerOptions)o; r.Endpoint = "new"; r.Queue = "new"; r.Exchange = "new"; r.ExchangeType = RabbitMqExchangeType.Direct; r.BindingKey = "new.key"; r.ErrorQueue = "new.error"; }
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
    [InlineData(false, "manual")]
    [InlineData(true, "manual")]
    [InlineData(false, "configuration")]
    [InlineData(true, "configuration")]
    [InlineData(false, "replace")]
    [InlineData(true, "replace")]
    public async Task NoRetry_AllRegistrationPaths_InvokeConsumerOnce(bool kafka, string mode)
    {
        var fixture = new Fixture();
        var consumer = fixture.Consumer(typeof(FailingConsumer));
        if (mode != "manual") fixture.Configuration(kafka, consumer, typeof(Message), o =>
        {
            RetryOptions retry;
            if (o is KafkaConsumerOptions k) { k.Topic = "events"; k.Group = "orders"; retry = k.Retry; }
            else { var r = (RabbitMqConsumerOptions)o; r.Exchange = "events"; r.Queue = "orders"; retry = r.Retry; }
            if (mode == "replace")
            {
                retry.MaxRetries = 5;
                retry.Handle.Add(typeof(Exception));
                if (o is KafkaConsumerOptions ko) ko.Retry = RetryOptions.NoRetry();
                else ((RabbitMqConsumerOptions)o).Retry = RetryOptions.NoRetry();
            }
        });
        var (services, endpoint) = mode == "manual" ? BuildManualEndpoint(kafka, consumer) : BuildEndpoint(kafka, fixture);
        await using var provider = services.BuildServiceProvider();
        var failure = await Dispatch(endpoint, provider);
        Assert.IsType<InvalidOperationException>(failure.Exception);
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
        var consumer = fixture.Consumer(typeof(NoopConsumer));
        fixture.Configuration(kafka, consumer, typeof(Message), o =>
        {
            if (o is KafkaConsumerOptions k) { k.Topic = "events"; k.Group = "orders"; k.Retry = null!; }
            else { var r = (RabbitMqConsumerOptions)o; r.Exchange = "events"; r.Queue = "orders"; r.Retry = null!; }
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
        var consumer = fixture.Consumer(typeof(NoopConsumer));
        fixture.Configuration(false, consumer, typeof(Message), o => { var r = (RabbitMqConsumerOptions)o; r.Queue = "orders"; r.Exchange = "events"; r.ExchangeType = type; });
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
        var consumer = fixture.Consumer(typeof(NoopConsumer));
        Configure(fixture, false, consumer);
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
        var consumer = fixture.Consumer(typeof(NoopConsumer));
        fixture.Configuration(false, consumer, typeof(Message), o => { var r = (RabbitMqConsumerOptions)o; r.Queue = "orders"; r.Exchange = "events"; r.ExchangeType = (RabbitMqExchangeType)999; });
        Assert.Throws<ArgumentOutOfRangeException>(() => new ServiceCollection().AddSeedWorkMessaging(b => b.AddRabbitMq("r", r =>
        {
            r.Configure(o => { o.Connection = RabbitConnection(); o.Topology = TopologyMode.CreateMissing; });
            if (scan) Scan(r, fixture.Marker);
            else r.Exchange("events", (RabbitMqExchangeType)999);
        })));
    }

    public abstract class FailingConsumer : IConsumer<Message>
    {
        public Task ConsumeAsync(MessageContext<Message> context, CancellationToken cancellationToken)
            => throw new InvalidOperationException("retry probe");
    }

    private static (ServiceCollection Services, object Endpoint) BuildEndpoint(bool kafka, Fixture fixture)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        object? captured = null;
        services.AddSeedWorkMessaging(b =>
        {
            if (kafka) b.AddKafka("k", k => { k.Configure(o => o.Connection = KafkaConnection()).Topic("events", 2, 1); Scan(k, fixture.Marker); captured = k; });
            else b.AddRabbitMq("r", r => { r.Configure(o => { o.Connection = RabbitConnection(); o.Topology = TopologyMode.CreateMissing; }).Exchange("events"); Scan(r, fixture.Marker); captured = r; });
        });
        var endpoints = (System.Collections.IEnumerable)captured!.GetType().GetProperty("Endpoints", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(captured)!;
        return (services, Assert.Single(endpoints.Cast<object>()));
    }

    private static (ServiceCollection Services, object Endpoint) BuildManualEndpoint(bool kafka, Type consumer)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        object? captured = null;
        services.AddSeedWorkMessaging(b =>
        {
            if (kafka) b.AddKafka("k", k => { k.Configure(o => o.Connection = KafkaConnection()).Topic("events", 2, 1); Manual(k, consumer); captured = k; });
            else b.AddRabbitMq("r", r => { r.Configure(o => { o.Connection = RabbitConnection(); o.Topology = TopologyMode.CreateMissing; }).Exchange("events"); Manual(r, consumer); captured = r; });
        });
        var endpoints = (System.Collections.IEnumerable)captured!.GetType().GetProperty("Endpoints", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(captured)!;
        return (services, Assert.Single(endpoints.Cast<object>()));
    }

    private static async Task<ConsumerFailure> Dispatch(object endpoint, IServiceProvider provider)
    {
        var dispatch = (Func<TransportMessage, ConsumerDispatcher, CancellationToken, Task<ConsumerFailure?>>)endpoint.GetType().GetProperty("Dispatch")!.GetValue(endpoint)!;
        return (await dispatch(new("{}"u8.ToArray(), null, new Dictionary<string, string>()), provider.GetRequiredService<ConsumerDispatcher>(), default))!;
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
    private static void Configure(Fixture fixture, bool kafka, Type consumer, Type? message = null, string subscription = "orders")
        => fixture.Configuration(kafka, consumer, message ?? typeof(Message), options =>
        {
            if (options is KafkaConsumerOptions k) { k.Topic = "events"; k.Group = subscription; k.Partitions = 2; k.ReplicationFactor = 1; }
            else if (options is RabbitMqConsumerOptions r) { r.Queue = subscription; r.Exchange = "events"; }
        });
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
        public Type Consumer(Type parent)
        {
            var type = _module.DefineType("Consumer" + _index++, TypeAttributes.Public, parent);
            type.DefineDefaultConstructor(MethodAttributes.Public);
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
