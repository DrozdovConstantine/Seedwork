# SeedWork.Messaging.Kafka

Руководство по адаптеру Kafka для .NET 10. Пакет использует Confluent.Kafka 2.15.1;
общие контракты SeedWork.Messaging подключаются транзитивно.

Повторы по умолчанию отключены. Первая ошибка обработчика приостанавливает партицию,
не фиксируя offset ошибочного сообщения. Другие партиции продолжают работать.
Ниже — 16 примеров: первые два представляют самостоятельные приложения, остальные
заменяют или дополняют указанные части первого. Альтернативные регистрации не складываются вместе.

## Содержание

- [Термины и настройки](#reference)
- [Установка и запуск](#setup)
- [1. Минимальный получатель](#consumer)
- [2. Отдельный издатель](#publisher)
- [3. Публикация через DI и ключ сообщения](#metadata)
- [4. Scoped-зависимости и отмена](#scoped)
- [5. Только атрибут](#attribute)
- [6. Только класс конфигурации](#configuration)
- [7. Класс поверх атрибута](#override)
- [8. Сканирование Assembly](#assemblies)
- [Один класс для нескольких типов сообщений](#multi-contract)
- [9. Несколько экземпляров одной группы](#same-group)
- [10. Две независимые группы](#different-groups)
- [11. Общее объявление topic и ValidateOnly](#topology)
- [12. Фиксированные повторы](#retry)
- [13. Экспоненциальные повторы и фильтры](#retry-filters)
- [14. Без повторов и пауза партиции](#no-retry)
- [15. Несколько подключений и RabbitMQ](#connections)
- [16. Телеметрия](#telemetry)
- [Остановка, гарантии и типичные ошибки](#operations)

<a id="reference"></a>
## Термины и настройки

| Имя | Назначение | Пример |
|---|---|---|
| connection | Именованное подключение адаптера | `kafka` |
| route | Локальный маршрут для PublishAsync | `orders-created` |
| endpoint | Имя обработчика и метка телеметрии | `billing-orders` |
| topic | Журнал сообщений Kafka | `orders.v1` |
| partition | Часть topic, внутри которой сохраняется порядок записей | 0 или 1 |
| key | Ключ распределения публикуемого сообщения | идентификатор клиента |
| group | Группа совместного чтения topic | `billing-v1` |
| offset | Позиция записи внутри одной партиции | управляется адаптером |

Route, connection и endpoint имеют уникальные имена в рамках регистрации шины.
Они не подменяют topic или group. `Publish<T>` связывает маршрут с конкретным CLR-типом T.
Один endpoint создаёт отдельный Kafka consumer; producer общий для именованного подключения.

| Настройка подключения | Значение / поведение |
|---|---|
| `Client.BootstrapServers` | Обязательный адрес брокеров |
| `Client` | Общие параметры Confluent ClientConfig, включая SSL/SASL |
| `Topology` | По умолчанию `ValidateOnly` |

| Настройка консумера | Значение / требование |
|---|---|
| `Topic`, `Group` | Обязательны после применения класса конфигурации |
| `Partitions` | Положительное число для нового объявления; 0 использует общее объявление topic |
| `ReplicationFactor` | Положительное число для нового объявления; 0 использует общее объявление |
| `Endpoint` при сканировании | `{connection}/{consumer.FullName}/{message.FullName}` |
| `Retry` | Отдельный RetryOptions без повторов; null недопустим |

Адаптер фиксирует EnableAutoCommit=false, EnableAutoOffsetStore=false,
AutoOffsetReset=Earliest и запрет auto-create topic. Новая группа без сохранённого offset
начинает с ранних доступных записей; существующая продолжает с сохранённой позиции.
Число партиций и реплик проверяется по фактической топологии, автоматически не меняется.

<a id="setup"></a>
## Установка и локальный запуск

Команды PowerShell выполняются из корня репозитория. Примеры подключают SeedWork
напрямую через ссылки на проекты; внешние зависимости восстанавливаются обычным способом.

```powershell
docker compose -p seedwork-messaging -f samples/Messaging/compose.yaml up -d --wait kafka

dotnet new console -n KafkaConsumer -o artifacts/tutorials/KafkaConsumer --framework net10.0
dotnet add artifacts/tutorials/KafkaConsumer reference src/SeedWork.Messaging.Kafka/SeedWork.Messaging.Kafka.csproj
dotnet add artifacts/tutorials/KafkaConsumer package Microsoft.Extensions.Hosting --version 10.0.12 --no-restore
dotnet restore artifacts/tutorials/KafkaConsumer
```

Локальный broker доступен по `127.0.0.1:19092`. Примеры выполняются на хосте:
advertised listeners в Compose настроены именно на этот адрес. Для приложения в другом
контейнере потребуется другая сетевая конфигурация. ReplicationFactor=1 соответствует
одному локальному брокеру; значение 3 требует минимум трёх доступных брокеров.
После работы: `docker compose -p seedwork-messaging -f samples/Messaging/compose.yaml stop kafka`.

<a id="consumer"></a>
## 1. Минимальный получатель

Замените Program.cs созданного KafkaConsumer. Запустите
`dotnet run --project artifacts/tutorials/KafkaConsumer` и оставьте его работать.

<!-- example:kafka-consumer -->
```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SeedWork.Messaging;
using SeedWork.Messaging.Kafka;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddSeedWorkMessaging(bus => bus.AddKafka("kafka", kafka => kafka
    .Configure(options =>
    {
        options.Client.BootstrapServers = "127.0.0.1:19092";
        options.Topology = TopologyMode.CreateMissing;
    })
    .Topic("orders.v1", partitions: 2, replicationFactor: 1)
    .Consume<OrderCreated, OrderConsumer>("billing-orders", "orders.v1", "billing-v1")));

using var host = builder.Build();
await host.RunAsync();

public sealed record OrderCreated(Guid Id, decimal Amount);

public sealed class OrderConsumer(ILogger<OrderConsumer> logger) : IConsumer<OrderCreated>
{
    public Task ConsumeAsync(MessageContext<OrderCreated> context, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        logger.LogInformation("Заказ {OrderId}, сумма {Amount}, попытка {Attempt}",
            context.Message.Id, context.Message.Amount, context.Attempt);
        return Task.CompletedTask;
    }
}
```

Host готовит topic до начала обработки. После успешного ConsumeAsync адаптер фиксирует
следующий offset и возобновляет выдачу партиции. При исключении в этом примере партиция
останавливается сразу, поскольку MaxRetries по умолчанию равен нулю.

<a id="publisher"></a>
## 2. Отдельный издатель

Создайте второй проект из корня репозитория и добавьте ссылку на проект адаптера:

```powershell
dotnet new console -n KafkaPublisher -o artifacts/tutorials/KafkaPublisher --framework net10.0
dotnet add artifacts/tutorials/KafkaPublisher reference src/SeedWork.Messaging.Kafka/SeedWork.Messaging.Kafka.csproj
dotnet add artifacts/tutorials/KafkaPublisher package Microsoft.Extensions.Hosting --version 10.0.12 --no-restore
dotnet restore artifacts/tutorials/KafkaPublisher
```

Замените Program.cs и запустите `dotnet run --project artifacts/tutorials/KafkaPublisher`:


<!-- example:kafka-publisher -->
```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SeedWork.Messaging;
using SeedWork.Messaging.Kafka;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddSeedWorkMessaging(bus => bus.AddKafka("kafka", kafka => kafka
    .Configure(options =>
    {
        options.Client.BootstrapServers = "127.0.0.1:19092";
        options.Topology = TopologyMode.CreateMissing;
    })
    .Topic("orders.v1", partitions: 2, replicationFactor: 1)
    .Publish<OrderCreated>("orders-created", "orders.v1")));

using var host = builder.Build();
await host.StartAsync();
try
{
    var publisher = host.Services.GetRequiredService<IMessagePublisher>();
    await publisher.PublishAsync("orders-created", new OrderCreated(Guid.NewGuid(), 1250m),
        new PublishOptions { Key = "customer-42" });
}
finally
{
    await host.StopAsync();
}

public sealed record OrderCreated(Guid Id, decimal Amount);
```

Успешная публикация означает подтверждение producer, а не обработку консумером.
Адаптер включает идемпотентный producer, acks=all и timeout сообщения 30 секунд.
Publisher может работать до запуска получателя, если topic существует или создаётся
через CreateMissing. История доступна в пределах retention брокера.

<a id="metadata"></a>
## 3. Публикация через DI и выбор ключа

В проект издателя добавьте класс ниже и регистрацию
`builder.Services.AddScoped<OrderEvents>()` до Build. Вызывайте из DI scope после StartAsync.

```csharp
public sealed class OrderEvents(IMessagePublisher publisher)
{
    public Task CreatedAsync(Guid orderId, string customerId, decimal amount, CancellationToken ct)
        => publisher.PublishAsync("orders-created", new OrderCreated(orderId, amount),
            new PublishOptions
            {
                Key = customerId,
                MessageId = Guid.NewGuid().ToString("N"),
                CorrelationId = orderId.ToString(),
                Headers = new Dictionary<string, string> { ["source"] = "checkout" }
            }, ct);
}
```

Key используется producer при выборе партиции. При неизменной схеме партиционирования
сообщения с одним ключом направляются в одну партицию; изменение числа партиций может
изменить распределение. Порядок гарантируется в пределах партиции, общего порядка topic нет.
Последовательные await PublishAsync задают порядок отправки в примере; конкурентные вызовы
не задают прикладную последовательность сами по себе. `Key = null` оставляет распределение клиенту.

MessageId при отсутствии генерируется. У внешних сообщений MessageId/CorrelationId могут
отсутствовать. Headers строковые; префикс `sw-`, traceparent и tracestate зарезервированы.
MessageContext содержит Key, Headers, Endpoint и Attempt; partition/offset в общем контракте
не предоставляются. Прямой выбор партиции через IMessagePublisher также не предусмотрен.

<a id="scoped"></a>
## 4. Scoped-зависимости и отмена

В получателе замените OrderConsumer и добавьте OrderHandler. До регистрации шины добавьте
`builder.Services.AddScoped<OrderHandler>()`:

```csharp
public sealed class OrderHandler(ILogger<OrderHandler> logger)
{
    public Task HandleAsync(OrderCreated order, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        logger.LogInformation("Прикладная обработка {OrderId}", order.Id);
        return Task.CompletedTask;
    }
}

public sealed class OrderConsumer(OrderHandler handler, ILogger<OrderConsumer> logger)
    : IConsumer<OrderCreated>
{
    public async Task ConsumeAsync(MessageContext<OrderCreated> context, CancellationToken ct)
    {
        logger.LogInformation("Endpoint {Endpoint}, сообщение {MessageId}, корреляция {CorrelationId}, ключ {Key}",
            context.Endpoint, context.MessageId, context.CorrelationId, context.Key);
        await handler.HandleAsync(context.Message, ct);
    }
}
```

Новый scope и новый объект сообщения создаются для каждой попытки. Обработки разных
партиций могут выполняться одновременно; singleton-зависимости должны учитывать это.
Передавайте ct во внешние операции: он отменяется при остановке и отзыве партиции.
Не скрывайте ошибку обработчика, если offset не должен быть зафиксирован.

<a id="attribute"></a>
## 5. Подписка только через атрибут

В примере 1 замените `.Topic(...).Consume<...>(...)` на
`.AddConsumersFromAssembly(typeof(OrderConsumer).Assembly, builder.Configuration)`.
Непосредственно перед объявлением существующего OrderConsumer добавьте:

```csharp
[KafkaConsumer(typeof(OrderCreated), Endpoint = "billing-orders", Topic = "orders.v1",
    Group = "billing-v1", Partitions = 2, ReplicationFactor = 1)]
```

Отдельное объявление Topic больше не требуется. Атрибут описывает одну пару
«консумер — сообщение»; тип сообщения указывается явно. Повторов в этом примере нет.

<a id="configuration"></a>
## 6. Подписка только через класс конфигурации

Используйте тот же вызов сканирования, удалите атрибут и добавьте класс в сборку консумера.
Нужен `using Microsoft.Extensions.Configuration;`.

```csharp
public sealed class OrderConsumerConfiguration
    : KafkaConsumerConfiguration<OrderCreated, OrderConsumer>
{
    public override void Configure(KafkaConsumerOptions options, IConfiguration configuration)
    {
        options.Endpoint = "billing-orders";
        options.Topic = "orders.v1";
        options.Group = configuration["Messaging:OrdersGroup"] ?? "billing-v1";
        options.Partitions = configuration.GetValue<int>("Messaging:Partitions", 2);
        options.ReplicationFactor = 1;
        options.Retry = RetryOptions.NoRetry();
    }
}
```

Конфигурация имеет публичный конструктор без параметров; зависимости через DI в неё
не передаются. Консумер и конфигурация должны находиться в одной сканируемой сборке.
Без аргумента configuration метод получает пустой IConfiguration. Привязка ConsumerOptions
к JSON не выполняется автоматически: чтение значений явно показано в Configure.

appsettings.json в рабочем каталоге запуска:

```json
{
  "Kafka": "127.0.0.1:19092",
  "Messaging": {
    "OrdersGroup": "billing-v1",
    "Partitions": 2,
    "RetryCount": 3
  }
}
```

В настройке подключения замените BootstrapServers на
`builder.Configuration["Kafka"] ?? "127.0.0.1:19092"`. Host.CreateApplicationBuilder читает
appsettings.json из content root. При запуске из другой папки настройте content root
или передавайте конфигурацию переменными окружения:

```powershell
$env:Messaging__OrdersGroup = 'billing-preview-v1'
dotnet run --project artifacts/tutorials/KafkaConsumer
Remove-Item Env:Messaging__OrdersGroup
```

Это новая независимая группа, а не переименование старой: без сохранённых offsets она
прочитает доступную историю. Изменение Messaging:Partitions не выполняет миграцию topic.

<a id="override"></a>
## 7. Класс поверх атрибута

Оставьте атрибут из примера 5 и замените тело Configure класса из примера 6:

```csharp
options.Group = configuration["Messaging:OrdersGroup"] ?? options.Group;
options.Retry = new RetryOptions
{
    MaxRetries = configuration.GetValue<int>("Messaging:RetryCount", 3),
    Interval = TimeSpan.FromSeconds(2)
};
```

Defaults заполняются из атрибута, затем класс меняет необходимые поля. Topic, Partitions
и ReplicationFactor сохраняются. Присваивание Retry заменяет политику целиком;
`options.Retry.Ignore.Clear()` очищает только Ignore. В одном транспорте допускается
один атрибут и один класс конфигурации для пары «консумер — сообщение».

<a id="assemblies"></a>
## 8. Передача Assembly и нескольких сборок

До регистрации получите `var consumerAssembly = typeof(OrderConsumer).Assembly;`.
Внутри AddKafka после Configure:

```csharp
kafka.AddConsumersFromAssembly(consumerAssembly, builder.Configuration);
```

Для переданной приложением коллекции `IEnumerable<Assembly> consumerAssemblies`
добавьте `using System.Reflection;` и используйте:

```csharp
foreach (var assembly in consumerAssemblies.Distinct())
    kafka.AddConsumersFromAssembly(assembly, builder.Configuration);
```

Сканируются конкретные закрытые классы с IConsumer, включая унаследованные интерфейсы.
Атрибуты базового класса не наследуются. Абстрактные, открытые generic-классы и консумеры
без настроек Kafka пропускаются; RabbitMQ-декларации игнорируются.
Дубли сканирования и смешивание с ручной регистрацией той же пары в одном подключении
запрещены. Для нескольких типов сообщений требуются отдельные декларации и уникальные
endpoint; пара topic/group также должна быть уникальной в подключении.
Ошибка загрузки типов отменяет сканирование. Для trimming/NativeAOT нужна явная регистрация
Consume и отдельная проверка совместимости JSON и клиента.

<a id="same-group"></a>
## 9. Несколько экземпляров одной группы

После сборки получателя запустите команду в двух терминалах:

```powershell
dotnet run --project artifacts/tutorials/KafkaConsumer -- --Messaging:OrdersGroup=billing-v1
```

Этот аргумент используется вариантом с классом из примера 6; в минимальном примере 1
группа уже зафиксирована как billing-v1. При двух партициях Kafka распределяет их между
участниками группы. У одной партиции в пределах группы один текущий владелец. Дополнительные
экземпляры сверх числа партиций могут остаться без назначений. Один процесс способен
обрабатывать несколько назначенных партиций параллельно, сохраняя последовательность в каждой.

<a id="different-groups"></a>
## 10. Две независимые группы

В примере 1 замените цепочку Topic/Consume:

```csharp
.Topic("orders.v1", partitions: 2, replicationFactor: 1)
.Consume<OrderCreated, OrderConsumer>("billing-orders", "orders.v1", "billing-v1")
.Consume<OrderCreated, OrderConsumer>("audit-orders", "orders.v1", "audit-v1")
```

Обе группы читают topic независимо и хранят собственные offsets. Это ручная регистрация
одного класса для двух endpoint; обработчики могут быть разными. Два endpoint с одинаковой
парой topic/group в одном подключении библиотека отклонит. Новая группа начинает с ранних
доступных записей, поэтому сообщение может быть обработано каждым из двух получателей.

<a id="topology"></a>
## 11. Общее объявление topic и ValidateOnly

Вместо полного объявления топологии на каждом консумере можно указать topic один раз.
В примере 1 используйте цепочку:

```csharp
.Topic("orders.v1", partitions: 2, replicationFactor: 1)
.AddConsumersFromAssembly(typeof(OrderConsumer).Assembly, builder.Configuration)
```

При этом атрибут OrderConsumer замените на вариант без размеров topic:

```csharp
[KafkaConsumer(typeof(OrderCreated), Topic = "orders.v1", Group = "billing-v1")]
```

Partitions/ReplicationFactor со значением 0 используют общее объявление. Если необходимых
значений нет нигде, регистрация завершится ошибкой. Совпадающие объявления допустимы,
разные значения отклоняются независимо от порядка ручных вызовов и сканирования.

После создания ресурсов замените тело Configure подключения:

```csharp
options.Client.BootstrapServers = "127.0.0.1:19092";
options.Topology = TopologyMode.ValidateOnly;
```

ValidateOnly проверяет существование topic, число партиций и реплик без создания.
CreateMissing создаёт отсутствующий topic и проверяет существующий. Изменений количества
партиций/реплик адаптер не делает. Несоответствие останавливает запуск host.
Общие настройки SSL/SASL задаются через ClientConfig в Configure; адреса и секреты
берутся из окружения приложения. После регистрации менять options нельзя.

<a id="multi-contract"></a>
## Один класс обрабатывает несколько типов сообщений

Класс реализует два IConsumer-контракта, поэтому у каждого типа свой метод
`ConsumeAsync`. Добавьте следующие объявления в проект получателя вместо
`OrderConsumer` из первого примера:

```csharp
public sealed record OrderCreated(Guid Id, decimal Amount);
public sealed record OrderCancelled(Guid Id, string Reason);

public sealed class OrderEventsConsumer(ILogger<OrderEventsConsumer> logger)
    : IConsumer<OrderCreated>, IConsumer<OrderCancelled>
{
    public Task ConsumeAsync(MessageContext<OrderCreated> context, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        logger.LogInformation("Создан заказ {OrderId}, сумма {Amount}",
            context.Message.Id, context.Message.Amount);
        return Task.CompletedTask;
    }

    public Task ConsumeAsync(MessageContext<OrderCancelled> context, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        logger.LogInformation("Отменён заказ {OrderId}: {Reason}",
            context.Message.Id, context.Message.Reason);
        return Task.CompletedTask;
    }
}
```

Для каждого типа ниже используется отдельный topic и endpoint. Общая group
`billing-v1` допустима, поскольку пары topic/group различаются. Повторы одного
endpoint не изменяют политику другого.

### Вариант 1: ручная регистрация

Внутри `AddKafka` замените цепочку из первого примера. Для созданного заказа
разрешены два повтора; отменённый сразу приостанавливает свою партицию при ошибке.

```csharp
kafka.Topic("orders.created.v1", partitions: 2, replicationFactor: 1)
    .Topic("orders.cancelled.v1", partitions: 2, replicationFactor: 1)
    .Consume<OrderCreated, OrderEventsConsumer>(
        "orders-created-consumer", "orders.created.v1", "billing-v1",
        configureRetry: retry => retry.MaxRetries = 2)
    .Consume<OrderCancelled, OrderEventsConsumer>(
        "orders-cancelled-consumer", "orders.cancelled.v1", "billing-v1");
```

### Вариант 2: два атрибута и одно сканирование

Удалите ручные `Consume` и `Topic`, добавьте два атрибута непосредственно над
`OrderEventsConsumer`:

```csharp
[KafkaConsumer(typeof(OrderCreated), Endpoint = "orders-created-consumer",
    Topic = "orders.created.v1", Group = "billing-v1",
    Partitions = 2, ReplicationFactor = 1, MaxRetries = 2)]
[KafkaConsumer(typeof(OrderCancelled), Endpoint = "orders-cancelled-consumer",
    Topic = "orders.cancelled.v1", Group = "billing-v1",
    Partitions = 2, ReplicationFactor = 1, MaxRetries = 0)]
```

Внутри `AddKafka` после `Configure` вызовите
`kafka.AddConsumersFromAssembly(typeof(OrderEventsConsumer).Assembly, builder.Configuration)`.
Сканирование создаст две подписки. Для одной пары «тип — консумер» повторный атрибут
запрещён; для двух разных типов атрибуты работают независимо.

### Вариант 3: два класса конфигурации

Уберите атрибуты, оставьте одно сканирование и добавьте классы в ту же сборку.
Параметр `Messaging:CancelledGroup` управляет только второй подпиской.

```csharp
public sealed class CreatedConfiguration
    : KafkaConsumerConfiguration<OrderCreated, OrderEventsConsumer>
{
    public override void Configure(KafkaConsumerOptions options, IConfiguration configuration)
    {
        options.Endpoint = "orders-created-consumer";
        options.Topic = "orders.created.v1";
        options.Group = "billing-v1";
        options.Partitions = 2;
        options.ReplicationFactor = 1;
        options.Retry.MaxRetries = 2;
    }
}

public sealed class CancelledConfiguration
    : KafkaConsumerConfiguration<OrderCancelled, OrderEventsConsumer>
{
    public override void Configure(KafkaConsumerOptions options, IConfiguration configuration)
    {
        options.Endpoint = "orders-cancelled-consumer";
        options.Topic = "orders.cancelled.v1";
        options.Group = configuration["Messaging:CancelledGroup"] ?? "billing-v1";
        options.Partitions = 2;
        options.ReplicationFactor = 1;
        options.Retry = RetryOptions.NoRetry();
    }
}
```

Добавьте `using Microsoft.Extensions.Configuration;`. Публичный конструктор без
параметров нужен каждому классу конфигурации. При одинаковой group два разных topic
остаются независимыми подписками. Для одной пары «тип — консумер» допускается один
класс конфигурации; для разных типов классы различаются generic-аргументом.

### Маршруты издателя для двух типов

В отдельном издателе замените цепочку регистрации внутри `AddKafka` на следующую.
Тип `OrderCancelled` должен быть доступен издателю или объявлен им в совместимой
с JSON форме:

```csharp
kafka.Topic("orders.created.v1", 2, 1)
    .Topic("orders.cancelled.v1", 2, 1)
    .Publish<OrderCreated>("orders-created", "orders.created.v1")
    .Publish<OrderCancelled>("orders-cancelled", "orders.cancelled.v1");
```

После `host.StartAsync()` используйте `IMessagePublisher` так же, как во втором примере:

```csharp
await publisher.PublishAsync("orders-created", new OrderCreated(Guid.NewGuid(), 1250m),
    new PublishOptions { Key = "customer-42" });
await publisher.PublishAsync("orders-cancelled", new OrderCancelled(Guid.NewGuid(), "customer request"),
    new PublishOptions { Key = "customer-42" });
```

Это две независимые публикации. Один concrete-класс консумера регистрируется в DI
как scoped; для каждой попытки каждого типа создаётся новый scope. Изоляция обработки
и окончательной ошибки определяется topic/partition каждого endpoint, а не общим классом.

<a id="retry"></a>
## 12. Фиксированные повторы

В ручной регистрации замените Consume:

```csharp
.Consume<OrderCreated, OrderConsumer>("billing-orders", "orders.v1", "billing-v1",
    configureRetry: retry =>
    {
        retry.MaxRetries = 3;
        retry.Interval = TimeSpan.FromSeconds(2);
    })
```

Это до четырёх попыток одной записи. Последующие записи этой партиции ждут, другие
партиции работают независимо. Polling продолжается во время обработки и задержек.
Attempt начинается с 1, новая доставка после rebalance начинает отсчёт заново.

<a id="retry-filters"></a>
## 13. Экспоненциальные задержки и фильтры

В Configure класса консумера:

```csharp
options.Retry = new RetryOptions
{
    MaxRetries = 4,
    Interval = TimeSpan.FromSeconds(1),
    Exponential = true,
    MaxInterval = TimeSpan.FromSeconds(5)
};
options.Retry.Handle.Add(typeof(TimeoutException));
options.Retry.Ignore.Add(typeof(ArgumentException));
```

Задержки составляют 1, 2, 4, 5 секунд. Пустой Handle разрешает повтор всех типов;
непустой разрешает указанные типы и наследников. Ignore имеет приоритет.
Та же политика в атрибуте вместо атрибута примера 5:

```csharp
[KafkaConsumer(typeof(OrderCreated), Topic = "orders.v1", Group = "billing-v1",
    Partitions = 2, ReplicationFactor = 1, MaxRetries = 4,
    IntervalMilliseconds = 1000, Exponential = true, MaxIntervalMilliseconds = 5000,
    Handle = new[] { typeof(TimeoutException) }, Ignore = new[] { typeof(ArgumentException) })]
```

| Атрибут | RetryOptions | По умолчанию |
|---|---|---|
| MaxRetries | MaxRetries | 0 |
| IntervalMilliseconds | Interval | 1000 мс |
| Exponential | Exponential | false |
| MaxIntervalMilliseconds | MaxInterval | 30000 мс |
| Handle / Ignore | Handle / Ignore | пустые массивы / наборы |

Количество и интервалы неотрицательные, MaxInterval не меньше Interval и не больше
4 294 967 294 мс. Фильтры содержат типы исключений. Интервал сам по себе не включает повторы.
Ошибки JSON и tombstone/null не повторяются и приводят к паузе партиции.

<a id="no-retry"></a>
## 14. Без повторов и пауза партиции

В Configure класса, в том числе поверх атрибута с MaxRetries больше нуля:

```csharp
options.Retry = RetryOptions.NoRetry();
```

Фабрика создаёт отдельный объект, не общий mutable singleton. Для атрибута достаточно
MaxRetries=0; в ручном Consume можно не передавать configureRetry. Null в Retry запрещён.
Для сохранения прежнего поведения с тремя повторами необходимо явно указать MaxRetries=3.

Сценарий: запись A в партиции 0 завершается ошибкой, за ней стоит запись B. После ошибки
без повторов (или исчерпания настроенных повторов) партиция 0 остаётся paused. Offset A
не фиксируется, B не обрабатывается. Партиция 1 продолжает работу. Ошибка отражается в
логе, span и метрике `messaging.kafka.paused_partitions`.

Error topic, пропуск ошибочной записи и публичный Resume API не реализованы. Пауза хранится
только в текущем назначении партиции. После перезапуска или rebalance новый владелец
возобновляет чтение с сохранённого offset и может снова встретить A. Перезапуск не устраняет
причину ошибки. Следует определить причину по телеметрии и исправить обработку; автоматического
механизма восстановления проблемной записи в текущем пакете нет. Retention Kafka действует
даже во время паузы, поэтому остановка чтения не гарантирует бессрочного хранения записи.

<a id="connections"></a>
## 15. Несколько подключений и RabbitMQ в одной шине

Замените единственный вызов AddSeedWorkMessaging в приложении издателя. Для RabbitMQ
добавьте ссылку на `src/SeedWork.Messaging.RabbitMQ/SeedWork.Messaging.RabbitMQ.csproj`
через `dotnet add <проект> reference <путь>` и `using SeedWork.Messaging.RabbitMQ;`.
Задайте SalesKafka, AuditKafka и RabbitMQ в конфигурации приложения.

```csharp
builder.Services.AddSeedWorkMessaging(bus =>
{
    bus.AddKafka("sales-kafka", kafka => kafka
        .Configure(o => { o.Client.BootstrapServers = builder.Configuration["SalesKafka"]!; o.Topology = TopologyMode.CreateMissing; })
        .Topic("orders.v1", 2, 1)
        .Publish<OrderCreated>("sales-orders", "orders.v1"));
    bus.AddKafka("audit-kafka", kafka => kafka
        .Configure(o => { o.Client.BootstrapServers = builder.Configuration["AuditKafka"]!; o.Topology = TopologyMode.CreateMissing; })
        .Topic("orders.v1", 2, 1)
        .Publish<OrderCreated>("audit-orders", "orders.v1"));
    bus.AddRabbitMq("billing-rabbit", rabbit => rabbit
        .Configure(o => { o.Connection.Uri = new Uri(builder.Configuration["RabbitMQ"]!); o.Topology = TopologyMode.CreateMissing; })
        .Exchange("orders.v1")
        .Publish<OrderCreated>("billing-orders", "orders.v1", "orders.created"));
});
```

Все имена маршрутов уникальны. Один PublishAsync выбирает один маршрут; для нескольких
назначений нужны отдельные вызовы без общей транзакции. Получатель RabbitMQ должен заранее
подготовить очередь и binding. AddSeedWorkMessaging вызывается один раз на IServiceCollection.

<a id="telemetry"></a>
## 16. Логи, трассы и метрики

Добавьте OpenTelemetry.Extensions.Hosting и OpenTelemetry.Exporter.OpenTelemetryProtocol
версии 1.19.1; добавьте `using OpenTelemetry.Logs;`, `using OpenTelemetry.Metrics;`,
`using OpenTelemetry.Resources;`, `using OpenTelemetry.Trace;`. До Build:

```csharp
builder.Services.AddOpenTelemetry()
    .ConfigureResource(r => r.AddService("billing-kafka"))
    .WithTracing(t => t.AddSource(MessagingTelemetry.InstrumentationName,
        KafkaTransport.InstrumentationName).AddOtlpExporter())
    .WithMetrics(m => m.AddMeter(MessagingTelemetry.InstrumentationName,
        KafkaTransport.InstrumentationName).AddOtlpExporter());
builder.Logging.AddOpenTelemetry(o => o.AddOtlpExporter());
```

Для локального просмотра из корня репозитория:

```powershell
docker compose -p seedwork-messaging -f samples/Messaging/compose.yaml --profile telemetry up -d dashboard
$env:OTEL_EXPORTER_OTLP_ENDPOINT = 'http://localhost:4317'
$env:OTEL_EXPORTER_OTLP_PROTOCOL = 'grpc'
dotnet run --project artifacts/tutorials/KafkaConsumer
```

Dashboard: `http://localhost:18888`. Подключите экспорт на стороне издателя, чтобы видеть
связанные producer/consumer spans. Общие метрики: messaging.published, messaging.publish.errors,
messaging.consumed, messaging.retries, messaging.errors, длительности publish/consume.
Kafka добавляет `messaging.kafka.paused_partitions`: число партиций, оставленных paused
из-за окончательной ошибки в текущем назначении. Краткая пауза во время нормальной обработки
не является таким сбоем. При отзыве назначения этот показатель может уменьшиться без
исправления исходной причины. Тело и текст исключения библиотека в телеметрию не пишет.

<a id="operations"></a>
## Остановка, гарантии и типичные ошибки

При revoke/lost отменяется обработка отозванных партиций, её поздний результат не приводит
к commit. При остановке host обработчики также отменяются; незавершённая запись может
поступить снова. Публикация с отменённым ожиданием могла уже попасть в Kafka.
Бизнес-действия должны учитывать дубли: producer idempotence не обеспечивает exactly-once
бизнес-обработку или атомарность с БД. Готовых outbox и хранилища дедупликации нет.

JSON использует web defaults. Сервисы могут иметь независимые совместимые модели сообщений;
необязательное новое поле — допустимый способ расширения. Для несовместимого изменения
контракта заведите новый topic/маршрут версии v2. Несколько типов в одном topic не
диспетчеризуются автоматически по CLR-имени: тип задаётся регистрацией endpoint.

| Симптом | Что проверить / исправить |
|---|---|
| Unknown route / неподходящий тип сообщения | Согласовать имя и тип с Publish<T> |
| Messaging has not started | Публиковать после успешного StartAsync |
| BootstrapServers is required | Передать адрес в Configure |
| Incomplete or conflicting topology | Указать размеры topic или согласовать их с общим объявлением |
| Missing or incompatible Kafka topic | Проверить наличие и фактическое число партиций/реплик; миграция не выполняется автоматически |
| Topic/group pair must have one endpoint | Оставить один endpoint этой пары в подключении; для независимого чтения использовать другую группу |
| Consumer already registered | Убрать повторную регистрацию пары, не смешивать scan и manual для неё |
| Новый экземпляр не получает сообщения | Проверить общую группу, число партиций, назначения и сохранённые offsets |
| Новая группа читает старые записи | Это Earliest при отсутствии сохранённой позиции |
| Партиция перестала обрабатываться | Проверить окончательную ошибку и paused_partitions; error topic и Resume API отсутствуют |
| Первая ошибка сразу ставит паузу | По умолчанию повторов нет; включить MaxRetries, если они нужны |
| Ошибка повторяется после rebalance | Пауза не сохраняется между назначениями; устранить причину ошибки |
| Не работает подключение из контейнера | Проверить advertised listeners; локальный Compose рассчитан на клиента на хосте |

См. [общие контракты](../SeedWork.Messaging/README.md), [руководство RabbitMQ](../SeedWork.Messaging.RabbitMQ/README.md)
и [готовый пример двух сервисов](../../samples/Messaging/README.md).
