using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

namespace SeedWork.Messaging.Tests;

[Collection("Messaging")]
public sealed class PipelineTests
{
    [Fact]
    public async Task Retry_UsesFreshScopeAndMessage_AndPreservesMetadata()
    {
        var state = new State { Failures = 2 };
        await using var services = Services(state);
        var result = await Dispatch(services, new RetryOptions { MaxRetries = 3, Interval = TimeSpan.Zero });
        Assert.Null(result);
        Assert.Equal(new[] { 1, 2, 3 }, state.Contexts.Select(c => c.Attempt));
        Assert.Equal(3, state.Instances.Distinct().Count());
        Assert.All(state.Contexts, c => Assert.Equal("original-id", c.MessageId));
        Assert.All(state.ReceivedValues, v => Assert.Equal(1, v));
        Assert.Equal(3, state.Disposed);
    }

    [Fact]
    public async Task Retry_ExhaustionAndIgnoredException_ReturnFailure()
    {
        var state = new State { Failures = 100 };
        await using var services = Services(state);
        var result = await Dispatch(services, new RetryOptions { MaxRetries = 2, Interval = TimeSpan.Zero });
        Assert.Equal(3, result!.Attempts);
        var ignore = new RetryOptions { MaxRetries = 3 };
        ignore.Ignore.Add(typeof(InvalidOperationException));
        result = await Dispatch(services, ignore);
        Assert.Equal(1, result!.Attempts);
    }

    [Fact]
    public async Task MalformedJson_DoesNotInvokeConsumerOrRetry()
    {
        var state = new State();
        await using var services = Services(state);
        var result = await services.GetRequiredService<ConsumerDispatcher>().DispatchAsync<Payload, TestConsumer>(
            new("invalid"u8.ToArray(), null, new Dictionary<string, string>()), "test", "test", new(), new(), default);
        Assert.IsAssignableFrom<JsonException>(result!.Exception);
        Assert.Equal(1, result.Attempts);
        Assert.Empty(state.Contexts);
    }

    [Fact]
    public async Task Cancellation_DuringRetry_DoesNotBecomeFailure()
    {
        var state = new State { Failures = 100 };
        await using var services = Services(state);
        using var cancellation = new CancellationTokenSource();
        var dispatch = Dispatch(services, new RetryOptions { MaxRetries = 3, Interval = TimeSpan.FromSeconds(10) }, cancellation.Token);
        await state.FirstAttempt.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await cancellation.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => dispatch);
        Assert.Single(state.Contexts);
    }

    [Fact]
    public async Task PublishAndConsume_ExportLinkedSpansAndMetrics_WithoutPayload()
    {
        var spans = new List<Activity>();
        var metrics = new List<Metric>();
        using var traceProvider = Sdk.CreateTracerProviderBuilder().SetSampler(new AlwaysOnSampler()).AddSource(MessagingTelemetry.InstrumentationName).AddInMemoryExporter(spans).Build();
        using var meterProvider = Sdk.CreateMeterProviderBuilder().AddMeter(MessagingTelemetry.InstrumentationName).AddInMemoryExporter(metrics).Build();
        var state = new State();
        var transport = new RecordingTransport();
        var collection = new ServiceCollection();
        collection.AddSingleton(state);
        collection.AddSeedWorkMessaging(b =>
        {
            b.AddConnection("recording", _ => transport);
            b.AddRoute<Payload>("route", "recording", "destination");
            b.AddConsumer<Payload, TestConsumer>("endpoint");
        });
        await using var services = collection.BuildServiceProvider();
        var publisher = services.GetRequiredService<IMessagePublisher>();
        using var parent = new Activity("request").SetIdFormat(ActivityIdFormat.W3C).Start();
        await publisher.PublishAsync("route", new Payload { Value = 1 }, new() { MessageId = "stable", CorrelationId = "correlation", Key = "key" });
        var message = Assert.Single(transport.Messages);
        Assert.Equal("key", message.Key);
        Assert.Equal("stable", message.Headers[MessageHeaders.MessageId]);
        Assert.Null(await services.GetRequiredService<ConsumerDispatcher>().DispatchAsync<Payload, TestConsumer>(message, "recording", "endpoint", new(), new(JsonSerializerDefaults.Web), default));
        traceProvider.ForceFlush();
        meterProvider.ForceFlush();
        var publish = Assert.Single(spans, a => a.Kind == ActivityKind.Producer);
        var consume = Assert.Single(spans, a => a.Kind == ActivityKind.Consumer);
        Assert.Equal(publish.TraceId, consume.TraceId);
        Assert.Equal(publish.SpanId, consume.ParentSpanId);
        Assert.DoesNotContain(spans.SelectMany(a => a.TagObjects), tag => tag.Key.Contains("body") || tag.Key.Contains("payload"));
        Assert.Contains(metrics, m => m.Name == "messaging.published");
        Assert.Contains(metrics, m => m.Name == "messaging.consumed");
        await Assert.ThrowsAsync<ArgumentException>(() => publisher.PublishAsync("unknown", new Payload()));
        await Assert.ThrowsAsync<ArgumentException>(() => publisher.PublishAsync("route", new Payload(), new()
        {
            Headers = new Dictionary<string, string> { ["traceparent"] = "forged" }
        }));
    }

    [Fact]
    public void Configuration_RejectsUnknownConnectionsAndInvalidRetries()
    {
        Assert.Throws<ArgumentException>(() => new ServiceCollection().AddSeedWorkMessaging(b => b.AddRoute<Payload>("r", "missing", "t")));
        Assert.Throws<ArgumentException>(() => new RetryOptions { MaxRetries = -1 }.Validate());
        var retry = new RetryOptions { Interval = TimeSpan.FromSeconds(2), Exponential = true, MaxInterval = TimeSpan.FromSeconds(5) };
        Assert.Equal(TimeSpan.FromSeconds(2), retry.GetDelay(1));
        Assert.Equal(TimeSpan.FromSeconds(4), retry.GetDelay(2));
        Assert.Equal(TimeSpan.FromSeconds(5), retry.GetDelay(3));
    }

    private static ServiceProvider Services(State state)
    {
        var services = new ServiceCollection();
        services.AddSingleton(state);
        services.AddSeedWorkMessaging(b => b.AddConsumer<Payload, TestConsumer>("endpoint"));
        return services.BuildServiceProvider();
    }
    private static Task<ConsumerFailure?> Dispatch(ServiceProvider services, RetryOptions retry, CancellationToken ct = default)
        => services.GetRequiredService<ConsumerDispatcher>().DispatchAsync<Payload, TestConsumer>(
            new(Encoding.UTF8.GetBytes("{\"Value\":1}"), "key", new Dictionary<string, string> { [MessageHeaders.MessageId] = "original-id" }),
            "test", "endpoint", retry, new(), ct);

    public sealed class Payload { public int Value { get; set; } }
    public sealed class State
    {
        public int Failures;
        public int Disposed;
        public List<MessageContext<Payload>> Contexts { get; } = [];
        public List<Guid> Instances { get; } = [];
        public List<int> ReceivedValues { get; } = [];
        public TaskCompletionSource FirstAttempt { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
    public sealed class TestConsumer(State state) : IConsumer<Payload>, IDisposable
    {
        private readonly Guid _id = Guid.NewGuid();
        public Task ConsumeAsync(MessageContext<Payload> context, CancellationToken cancellationToken)
        {
            state.Contexts.Add(context);
            state.Instances.Add(_id);
            state.ReceivedValues.Add(context.Message.Value);
            context.Message.Value = 999;
            state.FirstAttempt.TrySetResult();
            if (state.Failures-- > 0) throw new InvalidOperationException("failure");
            return Task.CompletedTask;
        }
        public void Dispose() => state.Disposed++;
    }
    private sealed class RecordingTransport : IMessageTransport
    {
        public string Name => "recording";
        public List<TransportMessage> Messages { get; } = [];
        public Task InitializeAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task RunAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task PublishAsync(string destination, TransportMessage message, CancellationToken cancellationToken) { Messages.Add(message); return Task.CompletedTask; }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
