using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using SeedWork.Messaging.Kafka;
using SeedWork.Messaging.RabbitMQ;

namespace SeedWork.Messaging.Tests;

public sealed class BrokerFactAttribute : FactAttribute
{
    public BrokerFactAttribute()
    {
        if (Environment.GetEnvironmentVariable("SEEDWORK_BROKER_TESTS") != "1")
            Skip = "Set SEEDWORK_BROKER_TESTS=1 and start samples/Messaging/compose.yaml.";
    }
}

[Trait("Category", "Integration")]
[Collection("Messaging")]
public sealed class BrokerTests
{
    private readonly string _name = "sw-" + Guid.NewGuid().ToString("N");
    private static string KafkaAddress => Environment.GetEnvironmentVariable("SEEDWORK_KAFKA") ?? "127.0.0.1:19092";
    private static Uri RabbitAddress => new(Environment.GetEnvironmentVariable("SEEDWORK_RABBITMQ") ?? "amqp://guest:guest@localhost:5673/");
    private static Uri ManagementAddress => new(Environment.GetEnvironmentVariable("SEEDWORK_RABBITMQ_MANAGEMENT") ?? "http://localhost:15673/");

    [BrokerFact]
    public async Task BothTransports_DeliverThroughNamedRoutes_WithMetadata()
    {
        await using var host = Build();
        await host.StartAsync();
        foreach (var route in new[] { "rabbit", "kafka" })
            await host.Publisher.PublishAsync(route, new Event("ok"), new()
            {
                MessageId = route + "-id", CorrelationId = "correlation", Key = "key",
                Headers = new Dictionary<string, string> { ["custom"] = "value" }
            });
        await Until(() => host.State.Received.Count == 2);
        Assert.Equal(new[] { "kafka-id", "rabbit-id" }, host.State.Received.Select(r => r.MessageId).Order());
        Assert.All(host.State.Received, r =>
        {
            Assert.Equal("correlation", r.CorrelationId);
            Assert.Equal("key", r.Key);
            Assert.Equal("value", r.Headers["custom"]);
        });
    }

    [BrokerFact]
    public async Task BothTransports_ScannedConsumers_ApplyAttributesAndConfigurations()
    {
        await using var host = Build(scan: true);
        await host.StartAsync();
        foreach (var route in new[] { "rabbit", "kafka" })
            await host.Publisher.PublishAsync(route, new Event("retry"), new() { MessageId = route + "-scanned" });
        await Until(() => host.State.Received.Count == 6);
        Assert.Equal(3, host.State.Received.Count(c => c.Endpoint == _name + "-r"));
        Assert.Equal(3, host.State.Received.Count(c => c.Endpoint == _name + "-k"));
        Assert.Equal(new[] { 1, 2, 3 }, host.State.Received.Where(c => c.MessageId == "rabbit-scanned").Select(c => c.Attempt));
        Assert.Equal(new[] { 1, 2, 3 }, host.State.Received.Where(c => c.MessageId == "kafka-scanned").Select(c => c.Attempt));
    }

    [BrokerFact]
    public async Task Rabbit_RetriesThenErrors_AndContinuesInOrder()
    {
        await using var host = Build();
        await host.StartAsync();
        await host.Publisher.PublishAsync("rabbit", new Event("retry"));
        await host.Publisher.PublishAsync("rabbit", new Event("poison"));
        await host.Publisher.PublishAsync("rabbit", new Event("after"));
        await Until(() => host.State.Received.Count == 7);
        Assert.Equal(new[] { "retry", "retry", "retry", "poison", "poison", "poison", "after" }, host.State.Received.Select(r => r.Message.Kind));
        await using var connection = await new ConnectionFactory { Uri = RabbitAddress }.CreateConnectionAsync();
        await using var channel = await connection.CreateChannelAsync();
        var failed = await Get(channel, _name + "_error");
        Assert.Equal("poison", JsonSerializer.Deserialize<Event>(failed.Body.Span, new JsonSerializerOptions(JsonSerializerDefaults.Web))!.Kind);
        Assert.Equal("3", Encoding.UTF8.GetString((byte[])failed.BasicProperties.Headers![MessageHeaders.ErrorAttempts]!));
        Assert.Equal(_name, Encoding.UTF8.GetString((byte[])failed.BasicProperties.Headers![MessageHeaders.OriginalExchange]!));
        Assert.Equal((uint)0, await channel.MessageCountAsync(_name));
    }

    [BrokerFact]
    public async Task Rabbit_MalformedJson_GoesDirectlyToErrorQueue_AndUnroutablePublishFails()
    {
        await using var host = Build();
        await host.StartAsync();
        await using var connection = await new ConnectionFactory { Uri = RabbitAddress }.CreateConnectionAsync();
        await using var channel = await connection.CreateChannelAsync(new CreateChannelOptions(true, true));
        await channel.BasicPublishAsync(_name, "key", true, new BasicProperties { Persistent = true }, "bad-json"u8.ToArray());
        var failed = await Get(channel, _name + "_error");
        Assert.Equal("bad-json", Encoding.UTF8.GetString(failed.Body.Span));
        Assert.Empty(host.State.Received);
        await channel.QueueUnbindAsync(_name, _name, "#");
        await Assert.ThrowsAnyAsync<Exception>(() => host.Publisher.PublishAsync("rabbit", new Event("unroutable")));
    }

    [BrokerFact]
    public async Task Rabbit_ErrorQueueUnavailable_DoesNotAcknowledgeOriginal()
    {
        await using var host = Build(reconnect: TimeSpan.FromSeconds(10));
        await host.StartAsync();
        await using var connection = await new ConnectionFactory { Uri = RabbitAddress }.CreateConnectionAsync();
        await using var channel = await connection.CreateChannelAsync();
        await channel.QueueDeleteAsync(_name + "_error", false, false);
        await host.Publisher.PublishAsync("rabbit", new Event("poison"));
        await Until(() => host.State.Received.Count == 3);
        await UntilAsync(async () => await channel.MessageCountAsync(_name) == 1);
        Assert.Equal((uint)1, await channel.MessageCountAsync(_name));
    }

    [BrokerFact]
    public async Task Rabbit_ConnectionLoss_ReconnectsAndConsumes()
    {
        await using var host = Build();
        await host.StartAsync();
        await host.Publisher.PublishAsync("rabbit", new Event("before"));
        await Until(() => host.State.Received.Count == 1);
        using var http = ManagementClient();
        string? connectionName = null;
        // Management statistics are sampled; a live connection may not be listed immediately.
        await UntilAsync(async () =>
        {
            using var json = JsonDocument.Parse(await http.GetStringAsync("api/connections"));
            foreach (var connection in json.RootElement.EnumerateArray())
                if (connection.GetProperty("client_properties").TryGetProperty("connection_name", out var name) && name.GetString() == _name)
                    connectionName = connection.GetProperty("name").GetString();
            return connectionName is not null;
        });
        using var response = await http.DeleteAsync("api/connections/" + Uri.EscapeDataString(connectionName!));
        response.EnsureSuccessStatusCode();
        await UntilAsync(async () =>
        {
            try { await host.Publisher.PublishAsync("rabbit", new Event("after")); return true; }
            catch { return false; }
        });
        await Until(() => host.State.Received.Any(r => r.Message.Kind == "after"));
    }

    [BrokerFact]
    public async Task Kafka_RetriesInOrder_ThenCommitsSuccessfulMessages()
    {
        await using var host = Build();
        await host.StartAsync();
        using var producer = NativeProducer();
        await NativePublish(producer, 0, "retry");
        await NativePublish(producer, 0, "after");
        await Until(() => host.State.Received.Count == 4);
        Assert.Equal(new[] { "retry", "retry", "retry", "after" }, host.State.Received.Select(r => r.Message.Kind));
        await Until(() => Committed(0) == 2);
    }

    [BrokerFact]
    public async Task Kafka_Exhaustion_PausesOnlyFailedPartition_WithoutCommittingOrCreatingErrorTopic()
    {
        long paused = 0;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, meter) =>
        {
            if (instrument.Name == "messaging.kafka.paused_partitions") meter.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            foreach (var tag in tags)
                if (tag.Key == "messaging.destination.name" && Equals(tag.Value, _name + "-k")) Interlocked.Add(ref paused, value);
        });
        listener.Start();
        await using var host = Build();
        await host.StartAsync();
        using var producer = NativeProducer();
        await NativePublish(producer, 0, "poison");
        await NativePublish(producer, 0, "must-wait");
        await NativePublish(producer, 1, "healthy");
        await Until(() => Interlocked.Read(ref paused) == 1 && host.State.Received.Any(r => r.Message.Kind == "healthy"));
        await NativePublish(producer, 1, "still-healthy");
        await Until(() => Committed(1) == 2);
        Assert.Equal(3, host.State.Received.Count(r => r.Message.Kind == "poison"));
        Assert.DoesNotContain(host.State.Received, r => r.Message.Kind == "must-wait");
        Assert.True(Committed(0) <= 0);
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = KafkaAddress }).Build();
        Assert.DoesNotContain(admin.GetMetadata(TimeSpan.FromSeconds(10)).Topics, t => t.Topic.StartsWith(_name) && t.Topic != _name);
        await host.Host.StopAsync();
        Assert.Equal(0, Interlocked.Read(ref paused));
    }

    [BrokerFact]
    public async Task Kafka_StopCancelsHandler_DoesNotCommit_AndRestartRedelivers()
    {
        await using (var first = Build())
        {
            first.State.Block = true;
            await first.StartAsync();
            using var producer = NativeProducer();
            await NativePublish(producer, 0, "block");
            await first.State.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await first.Host.StopAsync(timeout.Token);
            await first.State.Cancelled.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.True(Committed(0) <= 0);
        }
        await using var restarted = Build(mode: TopologyMode.ValidateOnly);
        await restarted.StartAsync();
        await Until(() => restarted.State.Received.Any(r => r.Message.Kind == "block"));
        await Until(() => Committed(0) == 1);
    }

    [BrokerFact]
    public async Task Topology_ValidateExisting_AndRejectIncompatibleResources()
    {
        await using (var created = Build()) { await created.StartAsync(); }
        await using (var validated = Build(mode: TopologyMode.ValidateOnly)) { await validated.StartAsync(); }
        await using var wrongKafka = Build(mode: TopologyMode.ValidateOnly, partitions: 3);
        await Assert.ThrowsAsync<InvalidOperationException>(() => wrongKafka.StartAsync());
        await using var wrongRabbit = Build(mode: TopologyMode.ValidateOnly, exchangeType: RabbitMqExchangeType.Direct);
        await Assert.ThrowsAsync<InvalidOperationException>(() => wrongRabbit.StartAsync());
    }

    [BrokerFact]
    public async Task Topology_ValidationDoesNotCreateMissingResources()
    {
        await using var host = Build(mode: TopologyMode.ValidateOnly);
        await Assert.ThrowsAsync<InvalidOperationException>(() => host.StartAsync());
        using var http = ManagementClient();
        using var response = await http.GetAsync("api/exchanges/%2F/" + _name);
        Assert.Equal(System.Net.HttpStatusCode.NotFound, response.StatusCode);
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        builder.Services.AddSeedWorkMessaging(b => b.AddKafka("k", k => k
            .Configure(o => o.Client.BootstrapServers = KafkaAddress).Topic(_name, 2, 1)));
        await using var kafkaHost = new Harness(builder.Build(), new Probe());
        await Assert.ThrowsAsync<InvalidOperationException>(() => kafkaHost.StartAsync());
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = KafkaAddress }).Build();
        Assert.DoesNotContain(admin.GetMetadata(TimeSpan.FromSeconds(10)).Topics, t => t.Topic == _name);
    }

    [BrokerFact]
    public async Task Kafka_Rebalance_CancelsOldOwner_WithoutCommittingItsInFlightMessage()
    {
        await using var first = Build();
        first.State.Block = true;
        await first.StartAsync();
        using var producer = NativeProducer();
        await NativePublish(producer, 0, "block");
        await first.State.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
        // Joining the same eager/range group revokes the first assignment before reassignment.
        await using var second = Build(mode: TopologyMode.ValidateOnly);
        second.State.Block = true;
        await second.StartAsync();
        await first.State.Cancelled.Task.WaitAsync(TimeSpan.FromSeconds(40));
        Assert.True(Committed(0) <= 0);
        await Until(() => first.State.Received.Count + second.State.Received.Count >= 2);
        Assert.True(Committed(0) <= 0);
    }

    [BrokerFact]
    public async Task Kafka_MalformedPayload_PausesWithoutCallingConsumer()
    {
        long paused = 0;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, meter) =>
        {
            if (instrument.Name == "messaging.kafka.paused_partitions") meter.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            foreach (var tag in tags)
                if (tag.Key == "messaging.destination.name" && Equals(tag.Value, _name + "-k")) Interlocked.Add(ref paused, value);
        });
        listener.Start();
        await using var host = Build();
        await host.StartAsync();
        using var producer = NativeProducer();
        await producer.ProduceAsync(new TopicPartition(_name, 0), new Message<string, byte[]> { Key = "key", Value = "invalid"u8.ToArray() });
        await Until(() => Interlocked.Read(ref paused) == 1);
        Assert.Empty(host.State.Received);
        Assert.True(Committed(0) <= 0);
    }

    [BrokerFact]
    public Task NoRetry_DefaultManualRegistration_FinalizesFirstFailure() => VerifyNoRetry(false);

    [BrokerFact]
    public Task NoRetry_ConfigurationReplacesAttribute_FinalizesFirstFailure() => VerifyNoRetry(true);

    private async Task VerifyNoRetry(bool scan)
    {
        long paused = 0;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, meter) =>
        {
            if (instrument.Name == "messaging.kafka.paused_partitions") meter.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            foreach (var tag in tags)
                if (tag.Key == "messaging.destination.name" && Equals(tag.Value, _name + "-k")) Interlocked.Add(ref paused, value);
        });
        listener.Start();
        await using var host = Build(scan: scan, noRetry: true);
        await host.StartAsync();
        await host.Publisher.PublishAsync("rabbit", new Event("poison"));
        await host.Publisher.PublishAsync("rabbit", new Event("after"));
        await using var connection = await new ConnectionFactory { Uri = RabbitAddress }.CreateConnectionAsync();
        await using var channel = await connection.CreateChannelAsync();
        var failed = await Get(channel, _name + "_error");
        Assert.Equal("1", Encoding.UTF8.GetString((byte[])failed.BasicProperties.Headers![MessageHeaders.ErrorAttempts]!));
        await Until(() => host.State.Received.Any(r => r.Endpoint == _name + "-r" && r.Message.Kind == "after"));
        Assert.Single(host.State.Received, r => r.Endpoint == _name + "-r" && r.Message.Kind == "poison");
        using var producer = NativeProducer();
        await NativePublish(producer, 0, "poison");
        await NativePublish(producer, 0, "must-wait");
        await NativePublish(producer, 1, "healthy");
        await Until(() => Interlocked.Read(ref paused) == 1 && Committed(1) == 1);
        Assert.Single(host.State.Received, r => r.Endpoint == _name + "-k" && r.Message.Kind == "poison");
        Assert.DoesNotContain(host.State.Received, r => r.Message.Kind == "must-wait");
        Assert.True(Committed(0) <= 0);
    }

    private Harness Build(TopologyMode mode = TopologyMode.CreateMissing, int partitions = 2,
        RabbitMqExchangeType exchangeType = RabbitMqExchangeType.Topic, TimeSpan? reconnect = null, bool scan = false, bool noRetry = false)
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        var state = new Probe();
        builder.Services.AddSingleton(state);
        builder.Configuration["Scanning:Name"] = _name;
        builder.Configuration["Scanning:NoRetry"] = noRetry.ToString();
        builder.Services.AddSeedWorkMessaging(b =>
        {
            b.AddRabbitMq("rabbit", r =>
            {
                r.Configure(o =>
                {
                    o.Connection.Uri = RabbitAddress;
                    o.Connection.ClientProvidedName = _name;
                    o.ManagementUri = ManagementAddress;
                    o.Topology = mode;
                    o.ReconnectInterval = reconnect ?? TimeSpan.FromMilliseconds(200);
                }).Publish<Event>("rabbit", _name);
                if (!scan) r.Exchange(_name, exchangeType);
                if (scan) r.AddConsumersFromAssembly(typeof(ProbeConsumer).Assembly, builder.Configuration);
                else r.Consume<Event, ProbeConsumer>(_name + "-r", _name, _name, configureRetry: noRetry ? null : Retry);
            });
            b.AddKafka("kafka", k =>
            {
                k.Configure(o => { o.Client.BootstrapServers = KafkaAddress; o.Topology = mode; })
                    .Publish<Event>("kafka", _name);
                if (!scan) k.Topic(_name, partitions, 1);
                if (scan) k.AddConsumersFromAssembly(typeof(ProbeConsumer).Assembly, builder.Configuration);
                else k.Consume<Event, ProbeConsumer>(_name + "-k", _name, _name, noRetry ? null : Retry);
            });
        });
        return new Harness(builder.Build(), state);
    }

    private static void Retry(RetryOptions retry) { retry.MaxRetries = 2; retry.Interval = TimeSpan.FromMilliseconds(20); }
    private static async Task Until(Func<bool> condition) => await UntilAsync(() => Task.FromResult(condition()));
    private static async Task UntilAsync(Func<Task<bool>> condition)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(40));
        while (!await condition()) await Task.Delay(50, timeout.Token);
    }
    private static async Task<BasicGetResult> Get(IChannel channel, string queue)
    {
        BasicGetResult? result = null;
        await UntilAsync(async () => (result = await channel.BasicGetAsync(queue, true)) is not null);
        return result!;
    }
    private static HttpClient ManagementClient()
    {
        var client = new HttpClient { BaseAddress = ManagementAddress };
        var factory = new ConnectionFactory { Uri = RabbitAddress };
        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes(factory.UserName + ":" + factory.Password)));
        return client;
    }
    private static IProducer<string, byte[]> NativeProducer() => new ProducerBuilder<string, byte[]>(new ProducerConfig { BootstrapServers = KafkaAddress, Acks = Acks.All }).Build();
    private Task NativePublish(IProducer<string, byte[]> producer, int partition, string kind) => producer.ProduceAsync(
        new TopicPartition(_name, partition), new Message<string, byte[]> { Key = "key", Value = JsonSerializer.SerializeToUtf8Bytes(new Event(kind), new JsonSerializerOptions(JsonSerializerDefaults.Web)) });
    private long Committed(int partition)
    {
        using var consumer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig { BootstrapServers = KafkaAddress, GroupId = _name, EnableAutoCommit = false }).Build();
        return Assert.Single(consumer.Committed([new TopicPartition(_name, partition)], TimeSpan.FromSeconds(10))).Offset.Value;
    }

    public sealed record Event(string Kind);
    public sealed class Probe
    {
        public ConcurrentQueue<MessageContext<Event>> Received { get; } = new();
        public bool Block;
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Cancelled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
    [RabbitMqConsumer(typeof(Event), Queue = "attribute-queue", Exchange = "attribute-exchange", MaxRetries = 2)]
    [KafkaConsumer(typeof(Event), Topic = "attribute-topic", Group = "attribute-group", MaxRetries = 2)]
    public sealed class ProbeConsumer(Probe state) : IConsumer<Event>
    {
        public async Task ConsumeAsync(MessageContext<Event> context, CancellationToken cancellationToken)
        {
            state.Received.Enqueue(context);
            if (context.Message.Kind == "block" && state.Block)
            {
                state.Entered.TrySetResult();
                try { await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken); }
                finally { state.Cancelled.TrySetResult(); }
            }
            if (context.Message.Kind == "poison" || context.Message.Kind == "retry" && context.Attempt < 3)
                throw new InvalidOperationException("simulated failure");
        }
    }
    public sealed class ProbeRabbitConfiguration : RabbitMqConsumerConfiguration<Event, ProbeConsumer>
    {
        public override void Configure(RabbitMqConsumerOptions options, IConfiguration configuration)
        {
            var name = configuration["Scanning:Name"]!;
            options.Endpoint = name + "-r";
            options.Queue = name;
            options.Exchange = name;
            if (configuration.GetValue<bool>("Scanning:NoRetry")) options.Retry = RetryOptions.NoRetry();
            else Retry(options.Retry);
        }
    }
    public sealed class ProbeKafkaConfiguration : KafkaConsumerConfiguration<Event, ProbeConsumer>
    {
        public override void Configure(KafkaConsumerOptions options, IConfiguration configuration)
        {
            var name = configuration["Scanning:Name"]!;
            options.Endpoint = name + "-k";
            options.Topic = name;
            options.Partitions = 2;
            options.ReplicationFactor = 1;
            options.Group = name;
            if (configuration.GetValue<bool>("Scanning:NoRetry")) options.Retry = RetryOptions.NoRetry();
            else Retry(options.Retry);
        }
    }
    private sealed class Harness(IHost host, Probe state) : IAsyncDisposable
    {
        public IHost Host { get; } = host;
        public Probe State { get; } = state;
        public IMessagePublisher Publisher => Host.Services.GetRequiredService<IMessagePublisher>();
        public async Task StartAsync()
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            await Host.StartAsync(timeout.Token);
        }
        public async ValueTask DisposeAsync()
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            try { await Host.StopAsync(timeout.Token); }
            finally { await ((IAsyncDisposable)Host).DisposeAsync(); }
        }
    }
}
