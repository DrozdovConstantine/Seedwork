using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;

namespace SeedWork.Messaging.Kafka;

/// <summary>Адаптер Kafka с ручным commit и независимой последовательной обработкой каждой партиции.</summary>
public sealed class KafkaTransport : IMessageTransport
{
    public const string InstrumentationName = "SeedWork.Messaging.Kafka";
    private static readonly ActivitySource Activities = new(InstrumentationName);
    private static readonly Meter Meter = new(InstrumentationName);
    private static readonly UpDownCounter<long> Paused = Meter.CreateUpDownCounter<long>("messaging.kafka.paused_partitions");
    private readonly KafkaBuilder _config;
    private readonly ConsumerDispatcher _dispatcher;
    private readonly ILogger _logger;
    private IProducer<string, byte[]>? _producer;
    public string Name { get; }

    internal KafkaTransport(string name, KafkaBuilder config, ConsumerDispatcher dispatcher, ILogger logger)
        => (Name, _config, _dispatcher, _logger) = (name, config, dispatcher, logger);

    /// <summary>Проверяет или создаёт topics и подготавливает producer до запуска обработки.</summary>
    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig(CopyClientConfig())).Build();
        foreach (var topic in _config.Topics.Values)
        {
            if (_config.Options.Topology == TopologyMode.CreateMissing)
            {
                try
                {
                    await admin.CreateTopicsAsync([new TopicSpecification
                    {
                        Name = topic.Name, NumPartitions = topic.Partitions, ReplicationFactor = topic.ReplicationFactor
                    }], new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(15) }).WaitAsync(cancellationToken);
                }
                catch (CreateTopicsException e) when (e.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists)) { }
            }
            // Запрос неизвестного topic по имени может создать его автоматически; для проверки читаем общие метаданные.
            var metadata = await Task.Run(() => admin.GetMetadata(TimeSpan.FromSeconds(15)), cancellationToken);
            var actual = metadata.Topics.SingleOrDefault(t => t.Topic == topic.Name);
            if (actual is null || actual.Error.IsError || actual.Partitions.Count != topic.Partitions ||
                actual.Partitions.Any(p => p.Replicas.Length != topic.ReplicationFactor))
                throw new InvalidOperationException($"Missing or incompatible Kafka topic: {topic.Name}");
        }
        // Идемпотентный producer защищает от части повторных записей при отправке, но не обеспечивает exactly-once бизнес-действия.
        _producer = new ProducerBuilder<string, byte[]>(new ProducerConfig(CopyClientConfig())
        {
            EnableIdempotence = true, Acks = Acks.All, AllowAutoCreateTopics = false,
            MessageTimeoutMs = 30000
        }).Build();
    }

    /// <summary>Отправляет JSON и заголовки в topic; завершение означает получение подтверждения producer.</summary>
    public async Task PublishAsync(string destination, TransportMessage message, CancellationToken cancellationToken)
    {
        var producer = _producer ?? throw new InvalidOperationException("Messaging has not started.");
        var headers = new Headers();
        foreach (var (key, value) in message.Headers) headers.Add(key, Encoding.UTF8.GetBytes(value));
        await producer.ProduceAsync(destination, new Message<string, byte[]>
        {
            Key = message.Key!, Value = message.Body, Headers = headers
        }, cancellationToken);
    }

    /// <summary>Запускает цикл каждого endpoint и отменяет остальные циклы при остановке или необработанном сбое.</summary>
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var tasks = _config.Endpoints.Select(e => Task.Run(() => RunEndpointAsync(e, linked.Token), linked.Token)).ToList();
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
            await linked.CancelAsync();
            try { await Task.WhenAll(tasks); } catch when (linked.IsCancellationRequested) { }
        }
    }

    private async Task RunEndpointAsync(KafkaEndpoint endpoint, CancellationToken ct)
    {
        // На партицию допускается одна задача обработки. Состояние и вызовы consumer обслуживаются этим циклом.
        var work = new Dictionary<TopicPartition, PartitionWork>();
        var assigned = new List<TopicPartition>();
        // Задачи отозванных партиций завершаются отдельно; их результаты больше не могут продвинуть offset.
        var retired = new List<PartitionWork>();
        var tags = MessagingTelemetry.Tags(Name, endpoint.Name);

        void Revoke(IEnumerable<TopicPartition> partitions)
        {
            foreach (var partition in partitions)
            {
                assigned.Remove(partition);
                if (!work.Remove(partition, out var pending)) continue;
                pending.Cancellation.Cancel();
                if (pending.Faulted) Paused.Add(-1, tags);
                retired.Add(pending);
            }
        }

        using var consumer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig(CopyClientConfig())
        {
            GroupId = endpoint.Group, EnableAutoCommit = false, EnableAutoOffsetStore = false,
            AutoOffsetReset = AutoOffsetReset.Earliest, AllowAutoCreateTopics = false,
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.Range
        })
            .SetPartitionsAssignedHandler((_, partitions) =>
            {
                assigned.AddRange(partitions);
                // Новое назначение начинает чтение с сохранённого брокером offset, а не с позиции предыдущей выборки.
                return partitions.Select(p => new TopicPartitionOffset(p, Offset.Stored));
            })
            .SetPartitionsRevokedHandler((_, partitions) => Revoke(partitions.Select(p => p.TopicPartition)))
            .SetPartitionsLostHandler((_, partitions) => Revoke(partitions.Select(p => p.TopicPartition)))
            .Build();
        consumer.Subscribe(endpoint.Topic);
        try
        {
            while (!ct.IsCancellationRequested)
            {
                foreach (var pending in retired.Where(p => p.Task.IsCompleted).ToArray())
                {
                    await ObserveRetiredAsync(pending);
                    retired.Remove(pending);
                }
                foreach (var (partition, pending) in work.ToArray())
                {
                    if (!pending.Task.IsCompleted || pending.Faulted) continue;
                    var failure = await pending.Task;
                    ct.ThrowIfCancellationRequested();
                    if (failure is not null)
                    {
                        pending.Faulted = true;
                        Paused.Add(1, tags);
                        using var activity = Activities.StartActivity(endpoint.Name + " pause", ActivityKind.Internal);
                        activity?.SetStatus(ActivityStatusCode.Error);
                        activity?.SetTag("messaging.destination.partition.id", partition.Partition.Value);
                        _logger.LogError("Kafka partition paused: {Connection}/{Endpoint}, topic {Topic}, partition {Partition}, offset {Offset}, error {ErrorType}",
                            Name, endpoint.Name, endpoint.Topic, partition.Partition.Value, pending.Record.Offset.Value, failure.Exception.GetType().FullName);
                        // Партиция остаётся на паузе; ошибочное сообщение не пропускается и не отправляется в отдельный topic.
                        // TODO: явное возобновление и сохранение паузы между назначениями требуют отдельного решения.
                        continue;
                    }
                    try
                    {
                        // Commit сохраняет позицию после успешно обработанной записи; до этого партицию не возобновляем.
                        consumer.Commit(pending.Record);
                    }
                    catch (KafkaException exception) when (!exception.Error.IsFatal)
                    {
                        // При ошибке commit удерживаем паузу до его успеха или отзыва назначения, не повторяя выполненный handler.
                        if (!pending.CommitErrorLogged)
                        {
                            _logger.LogWarning("Kafka commit pending for {Endpoint}, partition {Partition}: {Code}", endpoint.Name, partition.Partition.Value, exception.Error.Code);
                            pending.CommitErrorLogged = true;
                        }
                        continue;
                    }
                    consumer.Resume([partition]);
                    work.Remove(partition);
                    pending.Cancellation.Dispose();
                }

                ConsumeResult<string, byte[]>? record;
                // Polling продолжается во время обработки и ошибок партиций, чтобы обслуживать назначения consumer group.
                try { record = consumer.Consume(TimeSpan.FromMilliseconds(50)); }
                catch (ConsumeException exception) when (!exception.Error.IsFatal)
                {
                    _logger.LogWarning("Kafka polling error on {Endpoint}: {Code}", endpoint.Name, exception.Error.Code);
                    await Task.Delay(TimeSpan.FromMilliseconds(100), ct);
                    continue;
                }
                if (assigned.Count > 0)
                {
                    // При возврате партиции тому же экземпляру librdkafka может сохранить паузу прошлого назначения.
                    consumer.Resume(assigned.Where(p => !work.ContainsKey(p)));
                    assigned.Clear();
                }
                if (record is null || record.IsPartitionEOF) continue;
                if (work.ContainsKey(record.TopicPartition))
                    throw new InvalidOperationException("Kafka delivered a second message for a paused partition.");
                // Следующая запись этой партиции ждёт завершения обработки и commit; другие партиции продолжают читаться.
                consumer.Pause([record.TopicPartition]);
                var cancellation = CancellationTokenSource.CreateLinkedTokenSource(ct);
                var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                if (record.Message.Headers is not null)
                    foreach (var header in record.Message.Headers)
                        headers[header.Key] = Encoding.UTF8.GetString(header.GetValueBytes() ?? []);
                var message = new TransportMessage(record.Message.Value ?? "null"u8.ToArray(), record.Message.Key, new ReadOnlyDictionary<string, string>(headers));
                // Пользовательский handler выполняется вне цикла polling и не обращается к самому Kafka consumer.
                var task = Task.Run(() => endpoint.Dispatch(message, _dispatcher, cancellation.Token), cancellation.Token);
                work.Add(record.TopicPartition, new(record, cancellation, task));
            }
        }
        finally
        {
            Revoke(work.Keys.ToArray());
            // Закрываем consumer в управляющем цикле с отключённым auto-commit, затем ждём отменённых обработчиков.
            consumer.Close();
            foreach (var pending in retired) await ObserveRetiredAsync(pending);
        }
    }

    private static async Task ObserveRetiredAsync(PartitionWork pending)
    {
        try { await pending.Task; }
        catch (OperationCanceledException) when (pending.Cancellation.IsCancellationRequested) { }
        finally { pending.Cancellation.Dispose(); }
    }

    // Конструкторы копирования Confluent могут разделять словарь настроек; роли клиентов должны иметь независимые копии.
    private Dictionary<string, string> CopyClientConfig() => _config.Options.Client.ToDictionary(x => x.Key, x => x.Value);

    private sealed class PartitionWork(ConsumeResult<string, byte[]> record, CancellationTokenSource cancellation, Task<ConsumerFailure?> task)
    {
        public ConsumeResult<string, byte[]> Record { get; } = record;
        public CancellationTokenSource Cancellation { get; } = cancellation;
        public Task<ConsumerFailure?> Task { get; } = task;
        public bool Faulted { get; set; }
        public bool CommitErrorLogged { get; set; }
    }

    public ValueTask DisposeAsync()
    {
        Interlocked.Exchange(ref _producer, null)?.Dispose();
        return ValueTask.CompletedTask;
    }
}
