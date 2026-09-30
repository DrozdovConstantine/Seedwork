# SeedWork.Messaging.RabbitMQ

Руководство по адаптеру RabbitMQ для .NET 10. Пакет использует RabbitMQ.Client 7.2.2;
общие контракты находятся в SeedWork.Messaging и подключаются транзитивно.

По умолчанию exchange имеет тип `RabbitMqExchangeType.Topic`, повторы отключены.
Ошибка первой попытки направляет сообщение в очередь ошибок. Ниже — 17 примеров:
первые два являются самостоятельными приложениями, остальные заменяют или дополняют
указанные части первого примера. Альтернативные регистрации не нужно складывать вместе.

## Содержание

- [Термины и настройки](#reference)
- [Установка и запуск](#setup)
- [1. Минимальный получатель](#consumer)
- [2. Отдельный издатель](#publisher)
- [3. Публикация через DI и метаданные](#metadata)
- [4. Scoped-зависимости и отмена](#scoped)
- [5. Только атрибут](#attribute)
- [6. Только класс конфигурации](#configuration)
- [7. Класс поверх атрибута](#override)
- [8. Сканирование Assembly](#assemblies)
- [Один класс для нескольких типов сообщений](#multi-contract)
- [9. Фиксированные повторы](#retry)
- [10. Экспоненциальные повторы и фильтры](#retry-filters)
- [11. Без повторов и собственная error queue](#errors)
- [12–14. Topic, Direct, Fanout](#routing)
- [15. Проверка существующей топологии](#topology)
- [16. Несколько подключений и Kafka](#connections)
- [17. Телеметрия](#telemetry)
- [Гарантии и типичные ошибки](#operations)

<a id="reference"></a>
## Термины и настройки

| Имя | Назначение | Пример |
|---|---|---|
| connection | Именованное подключение в приложении | `rabbit` |
| route | Локальное имя маршрута для PublishAsync | `orders-created` |
| endpoint | Имя обработчика доставки и метка телеметрии | `billing-orders` |
| exchange | Ресурс RabbitMQ, направляющий сообщения в очереди | `orders.v1` |
| queue | Очередь хранения и обработки | `billing.orders.v1` |
| routing key | Ключ публикуемого сообщения | `orders.created` |
| binding key | Ключ или шаблон привязки очереди к exchange | `orders.*` |

Connection, route и endpoint должны иметь уникальные имена в рамках одной регистрации
шины. Названия ресурсов брокера задаются отдельно: route не создаёт очередь, endpoint
не является её именем. `Publish<T>` связывает маршрут с конкретным CLR-типом `T`.

| Настройка подключения | Значение / поведение по умолчанию |
|---|---|
| `Connection` | ConnectionFactory клиента RabbitMQ; адрес и учётные данные задаёт приложение |
| `Topology` | `ValidateOnly`; существующие ресурсы должны быть подготовлены |
| `ManagementUri` | Не задан; обязателен для ValidateOnly, должен заканчиваться `/` |
| `ReconnectInterval` | 5 секунд; должен быть положительным |

| Настройка консумера | Значение / требование |
|---|---|
| `Queue`, `Exchange` | Обязательны после применения класса конфигурации |
| `ExchangeType` | `Topic`; также доступны `Direct`, `Fanout` |
| `BindingKey` | `#`; для Direct этот ключ не является шаблоном |
| `ErrorQueue` | `<Queue>_error`, если имя не указано |
| `Endpoint` при сканировании | `{connection}/{consumer.FullName}/{message.FullName}` |
| `Retry` | Отдельный объект RetryOptions без повторов; null недопустим |

Очереди текущей версии фиксированы: durable quorum; входная очередь использует single
active consumer и `x-delivery-limit=-1`, prefetch равен 1. Exchange durable.
Эти параметры не являются пользовательскими настройками ConsumerOptions.

<a id="setup"></a>
## Установка и локальный запуск

Команды PowerShell выполняются из корня репозитория. Примеры подключают SeedWork
напрямую через ссылки на проекты; внешние зависимости восстанавливаются обычным способом.

```powershell
docker compose -p seedwork-messaging -f samples/Messaging/compose.yaml up -d --wait rabbitmq

dotnet new console -n RabbitConsumer -o artifacts/tutorials/RabbitConsumer --framework net10.0
dotnet add artifacts/tutorials/RabbitConsumer reference src/SeedWork.Messaging.RabbitMQ/SeedWork.Messaging.RabbitMQ.csproj
dotnet add artifacts/tutorials/RabbitConsumer package Microsoft.Extensions.Hosting --version 10.0.12 --no-restore
dotnet restore artifacts/tutorials/RabbitConsumer
```

RabbitMQ доступен по `amqp://guest:guest@localhost:5673/`, Management UI —
`http://localhost:15673/`. Это адреса локального Compose, а не production-настройки.
После работы можно выполнить `docker compose -p seedwork-messaging -f samples/Messaging/compose.yaml stop rabbitmq`.

<a id="consumer"></a>
## 1. Минимальный получатель

Замените Program.cs созданного RabbitConsumer целиком. Запустите
`dotnet run --project artifacts/tutorials/RabbitConsumer` и оставьте процесс работающим.

<!-- example:rabbit-consumer -->
```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SeedWork.Messaging;
using SeedWork.Messaging.RabbitMQ;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddSeedWorkMessaging(bus => bus.AddRabbitMq("rabbit", rabbit => rabbit
    .Configure(options =>
    {
        options.Connection.Uri = new Uri("amqp://guest:guest@localhost:5673/");
        options.Topology = TopologyMode.CreateMissing;
    })
    .Exchange("orders.v1")
    .Consume<OrderCreated, OrderConsumer>(
        endpoint: "billing-orders", queue: "billing.orders.v1", exchange: "orders.v1",
        bindingKey: "orders.created")));

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

Host сначала готовит топологию и только затем начинает обработку. Ack после успешного
завершения обработчика выполняет адаптер. При исключении повторы здесь не выполняются:
сообщение отправляется в `billing.orders.v1_error`.

<a id="publisher"></a>
## 2. Отдельный издатель

Создайте второй проект из корня репозитория и добавьте ссылку на проект адаптера:

```powershell
dotnet new console -n RabbitPublisher -o artifacts/tutorials/RabbitPublisher --framework net10.0
dotnet add artifacts/tutorials/RabbitPublisher reference src/SeedWork.Messaging.RabbitMQ/SeedWork.Messaging.RabbitMQ.csproj
dotnet add artifacts/tutorials/RabbitPublisher package Microsoft.Extensions.Hosting --version 10.0.12 --no-restore
dotnet restore artifacts/tutorials/RabbitPublisher
```

Замените Program.cs и запустите `dotnet run --project artifacts/tutorials/RabbitPublisher`:


<!-- example:rabbit-publisher -->
```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SeedWork.Messaging;
using SeedWork.Messaging.RabbitMQ;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddSeedWorkMessaging(bus => bus.AddRabbitMq("rabbit", rabbit => rabbit
    .Configure(options =>
    {
        options.Connection.Uri = new Uri("amqp://guest:guest@localhost:5673/");
        options.Topology = TopologyMode.CreateMissing;
    })
    .Exchange("orders.v1")
    .Publish<OrderCreated>("orders-created", "orders.v1", routingKey: "orders.created")));

using var host = builder.Build();
await host.StartAsync();
try
{
    var publisher = host.Services.GetRequiredService<IMessagePublisher>();
    await publisher.PublishAsync("orders-created", new OrderCreated(Guid.NewGuid(), 1250m));
}
finally
{
    await host.StopAsync();
}

public sealed record OrderCreated(Guid Id, decimal Amount);
```

Запускайте после получателя: publisher объявляет exchange, но не очередь и binding.
Публикация использует mandatory routing и завершится ошибкой, если сообщение не попало
ни в одну очередь. Успех PublishAsync означает подтверждение брокером, а не завершение
бизнес-обработки. Контракт в двух сервисах может объявляться независимо при совместимом JSON.

<a id="metadata"></a>
## 3. Публикация через DI и метаданные

Добавьте класс ниже в проект издателя. Зарегистрируйте его через
`builder.Services.AddScoped<OrderEvents>()` до Build; разрешайте и вызывайте его из scope
после StartAsync. Имя маршрута должно совпадать с `.Publish<OrderCreated>(...)`.

```csharp
public sealed class OrderEvents(IMessagePublisher publisher)
{
    public Task CreatedAsync(Guid orderId, decimal amount, CancellationToken ct)
        => publisher.PublishAsync("orders-created", new OrderCreated(orderId, amount),
            new PublishOptions
            {
                MessageId = Guid.NewGuid().ToString("N"),
                CorrelationId = orderId.ToString(),
                Key = "orders.created", // Переопределяет routing key маршрута.
                Headers = new Dictionary<string, string> { ["source"] = "checkout" }
            }, ct);
}
```

`Key = null` сохраняет routing key маршрута. Не передавайте собственные `sw-*`,
`traceparent` и `tracestate`: они зарезервированы. MessageId при отсутствии генерируется.
Отмена ожидания публикации не отзывает сообщение, уже принятое брокером.

<a id="scoped"></a>
## 4. Scoped-зависимости и отмена

В получателе замените OrderConsumer и добавьте OrderHandler. До регистрации шины добавьте
`builder.Services.AddScoped<OrderHandler>()`. Сам консумер регистрируется библиотекой.

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

Каждая попытка получает новый scope и заново десериализованный объект сообщения.
Передавайте ct во внешние операции. Исключение должно дойти до адаптера: если перехватить
его и вернуть успешный Task, сообщение будет подтверждено. Внешний издатель может не
передать MessageId/CorrelationId, поэтому в MessageContext они допускают null.

<a id="attribute"></a>
## 5. Подписка только через атрибут

В примере 1 замените `.Exchange(...).Consume<...>(...)` на
`.AddConsumersFromAssembly(typeof(OrderConsumer).Assembly, builder.Configuration)`.
К существующему классу OrderConsumer добавьте атрибут:

```csharp
[RabbitMqConsumer(typeof(OrderCreated), Endpoint = "billing-orders",
    Queue = "billing.orders.v1", Exchange = "orders.v1",
    ExchangeType = RabbitMqExchangeType.Topic, BindingKey = "orders.created")]
```

Это атрибут объявления класса, а не самостоятельное выражение. Отдельный класс
конфигурации не нужен. Queue, exchange, binding и error queue будут описаны из атрибута.
Тип сообщения обязателен даже для консумера с единственным IConsumer-интерфейсом.

<a id="configuration"></a>
## 6. Подписка только через класс конфигурации

Используйте тот же вызов сканирования, удалите атрибут и добавьте класс ниже в сборку
консумера. Добавьте `using Microsoft.Extensions.Configuration;`.

```csharp
public sealed class OrderConsumerConfiguration
    : RabbitMqConsumerConfiguration<OrderCreated, OrderConsumer>
{
    public override void Configure(RabbitMqConsumerOptions options, IConfiguration configuration)
    {
        options.Endpoint = "billing-orders";
        options.Queue = configuration["Messaging:OrdersQueue"] ?? "billing.orders.v1";
        options.Exchange = "orders.v1";
        options.ExchangeType = RabbitMqExchangeType.Topic;
        options.BindingKey = "orders.created";
        options.ErrorQueue = "billing.orders.failed";
        options.Retry = RetryOptions.NoRetry();
    }
}
```

Класс создаётся через публичный конструктор без параметров, а не через DI. Консумер и
его конфигурация должны находиться в одной сканируемой сборке. IConfiguration передаётся
аргументом сканирования; без аргумента он пустой. Автоматического binding ConsumerOptions
из JSON нет: нужные ключи читает Configure.

Создайте appsettings.json в рабочем каталоге запуска приложения:

```json
{
  "RabbitMQ": "amqp://guest:guest@localhost:5673/",
  "Messaging": {
    "OrdersQueue": "billing.orders.v1",
    "RetryCount": 3
  }
}
```

В Configure подключения замените фиксированный адрес на
`new Uri(builder.Configuration["RabbitMQ"] ?? "amqp://guest:guest@localhost:5673/")`.
Host.CreateApplicationBuilder читает appsettings.json из content root; для запуска из
другой папки явно настройте content root или передайте значения окружением.

```powershell
$env:Messaging__OrdersQueue = 'billing.orders.v2'
dotnet run --project artifacts/tutorials/RabbitConsumer
Remove-Item Env:Messaging__OrdersQueue
```

Это создаст другую очередь: имеющиеся сообщения автоматически туда не переносятся.

<a id="override"></a>
## 7. Класс поверх атрибута

Оставьте атрибут из примера 5. В классе из примера 6 замените тело Configure:

```csharp
options.Queue = configuration["Messaging:OrdersQueue"] ?? options.Queue;
options.Retry = new RetryOptions
{
    MaxRetries = configuration.GetValue<int>("Messaging:RetryCount", 3),
    Interval = TimeSpan.FromSeconds(2)
};
```

Порядок: defaults → атрибут → класс. Незатронутые настройки атрибута сохраняются.
Присваивание Retry заменяет политику целиком; `options.Retry.Handle.Clear()` очищает
только фильтр. Если ErrorQueue не задан, стандартное имя вычисляется из итогового Queue.
В одном транспорте для пары «консумер — сообщение» допускаются один атрибут и один класс.

<a id="assemblies"></a>
## 8. Передача Assembly и нескольких сборок

Метод принимает объект Assembly. Например, до AddSeedWorkMessaging получите
`var consumerAssembly = typeof(OrderConsumer).Assembly;`, затем внутри AddRabbitMq
после Configure вызовите:

```csharp
rabbit.AddConsumersFromAssembly(consumerAssembly, builder.Configuration);
```

Если приложение уже располагает коллекцией сборок `IEnumerable<Assembly> consumerAssemblies`,
используйте `using System.Reflection;` и внутри настройки подключения:

```csharp
foreach (var assembly in consumerAssemblies.Distinct())
    rabbit.AddConsumersFromAssembly(assembly, builder.Configuration);
```

Сканируются конкретные закрытые классы; унаследованный IConsumer учитывается, атрибуты
базового класса не наследуются. Абстрактные, открытые generic-классы и ненастроенные
консумеры пропускаются. Kafka-атрибуты не влияют на регистрацию RabbitMQ.
Повторное сканирование одной пары и смешивание её сканирования с ручной регистрацией
в одном подключении запрещены. Для нескольких IConsumer-интерфейсов задавайте отдельные
декларации с разными очередями и endpoint. Ошибка загрузки типов отменяет сканирование.
Для trimming/NativeAOT используйте ручной Consume и отдельно проверяйте совместимость
JSON и клиента: автоматическое сканирование требует сохранённых типов и динамического кода.

<a id="multi-contract"></a>
## Один класс обрабатывает несколько типов сообщений

В этом варианте один класс реализует два контракта. У каждого типа свой метод
`ConsumeAsync`, endpoint, очередь и политика повторов. Сначала добавьте в проект получателя
типы и класс ниже вместо `OrderConsumer` из первого примера; нужны
`using Microsoft.Extensions.Logging;` и `using SeedWork.Messaging;`.

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

### Вариант 1: ручная регистрация

Внутри `AddRabbitMq` замените цепочку из первого примера на следующую. Для созданных
заказов разрешены два повтора, для отменённых повторов нет. Отдельные очереди сохраняют
независимую обработку и создают отдельные очереди ошибок с суффиксом `_error`.

```csharp
rabbit.Exchange("orders.v1", RabbitMqExchangeType.Topic)
    .Consume<OrderCreated, OrderEventsConsumer>(
        "orders-created-consumer", "billing.orders.created", "orders.v1",
        bindingKey: "orders.created", configureRetry: retry => retry.MaxRetries = 2)
    .Consume<OrderCancelled, OrderEventsConsumer>(
        "orders-cancelled-consumer", "billing.orders.cancelled", "orders.v1",
        bindingKey: "orders.cancelled");
```

### Вариант 2: два атрибута и один вызов сканирования

Уберите ручные `Consume` и добавьте два атрибута непосредственно над объявлением
`OrderEventsConsumer`. Политика повторов каждого типа задаётся отдельно:

```csharp
[RabbitMqConsumer(typeof(OrderCreated), Endpoint = "orders-created-consumer",
    Queue = "billing.orders.created", Exchange = "orders.v1",
    BindingKey = "orders.created", MaxRetries = 2)]
[RabbitMqConsumer(typeof(OrderCancelled), Endpoint = "orders-cancelled-consumer",
    Queue = "billing.orders.cancelled", Exchange = "orders.v1",
    BindingKey = "orders.cancelled", MaxRetries = 0)]
```

Внутри `AddRabbitMq` после `Configure` используйте
`rabbit.AddConsumersFromAssembly(typeof(OrderEventsConsumer).Assembly, builder.Configuration)`.
Один вызов обнаружит обе подписки и объявит общий exchange типа Topic.
Атрибуты указывают разные типы сообщений: два атрибута для одного и того же типа
считаются дубликатом и вызывают ошибку регистрации.

### Вариант 3: отдельный класс конфигурации для каждого типа

Уберите атрибуты и ручные `Consume`, оставьте вызов сканирования из варианта 2.
Добавьте в ту же сборку оба класса. Для второго типа показана замена имени очереди
через конфигурацию приложения; она не влияет на первый тип.

```csharp
public sealed class CreatedConfiguration
    : RabbitMqConsumerConfiguration<OrderCreated, OrderEventsConsumer>
{
    public override void Configure(RabbitMqConsumerOptions options, IConfiguration configuration)
    {
        options.Endpoint = "orders-created-consumer";
        options.Queue = "billing.orders.created";
        options.Exchange = "orders.v1";
        options.ExchangeType = RabbitMqExchangeType.Topic;
        options.BindingKey = "orders.created";
        options.Retry.MaxRetries = 2;
    }
}

public sealed class CancelledConfiguration
    : RabbitMqConsumerConfiguration<OrderCancelled, OrderEventsConsumer>
{
    public override void Configure(RabbitMqConsumerOptions options, IConfiguration configuration)
    {
        options.Endpoint = "orders-cancelled-consumer";
        options.Queue = configuration["Messaging:CancelledQueue"] ?? "billing.orders.cancelled";
        options.Exchange = "orders.v1";
        options.ExchangeType = RabbitMqExchangeType.Topic;
        options.BindingKey = "orders.cancelled";
        options.Retry = RetryOptions.NoRetry();
    }
}
```

Добавьте `using Microsoft.Extensions.Configuration;`. При указании другого имени
`Messaging:CancelledQueue` изменится только вторая очередь. Оба класса конфигурации
должны иметь публичный конструктор без параметров. Одна пара «тип — консумер» может
иметь не более одного такого класса; для разных типов пары независимы.

### Маршруты издателя для двух типов

В отдельном издателе замените цепочку регистрации внутри `AddRabbitMq` на следующую.
Контракт `OrderCancelled` должен быть доступен проекту издателя или объявлен там
в совместимой с JSON форме:

```csharp
rabbit.Exchange("orders.v1")
    .Publish<OrderCreated>("orders-created", "orders.v1", "orders.created")
    .Publish<OrderCancelled>("orders-cancelled", "orders.v1", "orders.cancelled");
```

После `host.StartAsync()` используйте `IMessagePublisher` так же, как во втором примере:

```csharp
await publisher.PublishAsync("orders-created", new OrderCreated(Guid.NewGuid(), 1250m));
await publisher.PublishAsync("orders-cancelled", new OrderCancelled(Guid.NewGuid(), "customer request"));
```

Это две независимые публикации. В каждом варианте библиотека регистрирует один concrete
класс в DI как scoped; для каждой попытки обработки создаётся новый scope. Совпадение
класса не объединяет очереди или политики ошибок.

<a id="retry"></a>
## 9. Фиксированные повторы

В ручной регистрации примера 1 замените Consume:

```csharp
.Consume<OrderCreated, OrderConsumer>("billing-orders", "billing.orders.v1", "orders.v1",
    bindingKey: "orders.created",
    configureRetry: retry =>
    {
        retry.MaxRetries = 3;
        retry.Interval = TimeSpan.FromSeconds(2);
    })
```

Это максимум четыре попытки: первая и три повтора. Следующее сообщение очереди ждёт
завершения текущего. Номер Attempt начинается с 1 и сбрасывается при новой доставке брокером.

<a id="retry-filters"></a>
## 10. Экспоненциальная задержка и фильтры

В Configure класса консумера можно полностью задать политику:

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

Задержки: 1, 2, 4, 5 секунд. Пустой Handle допускает все типы; иначе повторяются только
указанные типы и их наследники. Ignore имеет приоритет. Ошибки десериализации не повторяются.
Эквивалентная политика в атрибуте — замените атрибут примера 5:

```csharp
[RabbitMqConsumer(typeof(OrderCreated), Queue = "billing.orders.v1", Exchange = "orders.v1",
    BindingKey = "orders.created", MaxRetries = 4, IntervalMilliseconds = 1000,
    Exponential = true, MaxIntervalMilliseconds = 5000,
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
4 294 967 294 мс. Фильтры содержат типы исключений. Установка только интервала не включает повторы.

<a id="errors"></a>
## 11. Отключение повторов и очередь ошибок

В Configure класса, в том числе поверх атрибута с MaxRetries больше нуля:

```csharp
options.Retry = RetryOptions.NoRetry();
options.ErrorQueue = "billing.orders.failed";
```

NoRetry() возвращает новый независимый объект. В атрибуте используйте `MaxRetries = 0`;
при ручной регистрации можно опустить configureRetry или задать `retry.MaxRetries = 0`.
Null вместо Retry недопустим. Раньше отсутствие настройки означало три повтора; теперь
для сохранения этого поведения требуется явно указать MaxRetries = 3.

После окончательной ошибки адаптер публикует исходное тело и headers в error queue,
ждёт publisher confirm и только затем подтверждает исходную доставку. Добавляются:

| Header | Значение |
|---|---|
| `sw-error-type` | CLR-тип исключения, без текста исключения |
| `sw-error-attempts` | Число выполненных попыток |
| `sw-error-endpoint` | Имя endpoint |
| `sw-original-exchange` | Исходный exchange |
| `sw-original-routing-key` | Исходный routing key |

Если отправка в error queue не подтверждена, соединение закрывается и неподтверждённое
сообщение возвращается брокеру. Возможны повторная обработка и дубли в error queue.
Отключение прикладных повторов не отключает такую повторную доставку.
Готового API повторной отправки из error queue нет; разбор и возврат сообщений организует приложение.

<a id="routing"></a>
## 12. Topic: подписка по шаблону

В примере 1 замените цепочку Exchange/Consume; в примере 2 используйте тот же exchange
и routingKey `orders.created`:

```csharp
.Exchange("orders.v1", RabbitMqExchangeType.Topic)
.Consume<OrderCreated, OrderConsumer>("billing-orders", "billing.orders.v1", "orders.v1",
    bindingKey: "orders.*")
```

`*` соответствует одному сегменту ключа, `#` — нулю или нескольким. `orders.*` принимает
`orders.created`, но не `orders.eu.created`; `orders.#` принимает оба. Тип Topic выбирается
по умолчанию и не наследуется из другого объявления exchange.

## 13. Direct: точное совпадение

В получателе замените цепочку Exchange/Consume. Используется новое имя ресурса, поскольку
тип существующего exchange автоматически не меняется:

```csharp
.Exchange("orders.direct.v1", RabbitMqExchangeType.Direct)
.Consume<OrderCreated, OrderConsumer>("billing-orders", "billing.orders.direct.v1", "orders.direct.v1",
    bindingKey: "created")
```

В издателе замените Exchange/Publish:

```csharp
.Exchange("orders.direct.v1", RabbitMqExchangeType.Direct)
.Publish<OrderCreated>("orders-created", "orders.direct.v1", routingKey: "created")
```

Для Direct `#` означает буквальный ключ, а не «все сообщения».

## 14. Fanout: копия в каждую очередь

В получателе вместо цепочки Exchange/Consume:

```csharp
.Exchange("orders.broadcast.v1", RabbitMqExchangeType.Fanout)
.Consume<OrderCreated, OrderConsumer>("billing", "billing.broadcast.v1", "orders.broadcast.v1", bindingKey: "")
.Consume<OrderCreated, OrderConsumer>("audit", "audit.broadcast.v1", "orders.broadcast.v1", bindingKey: "")
```

В издателе вместо цепочки Exchange/Publish:

```csharp
.Exchange("orders.broadcast.v1", RabbitMqExchangeType.Fanout)
.Publish<OrderCreated>("orders-created", "orders.broadcast.v1")
```

Обе очереди получат сообщение. Здесь один класс вручную зарегистрирован для двух endpoint;
в реальном приложении можно использовать отдельные обработчики billing и audit.
При нескольких экземплярах одного сервиса с одной очередью single active consumer
оставляет активным один экземпляр; это не рассылка каждому процессу.

<a id="topology"></a>
## 15. Проверка существующей топологии

После подготовки ресурсов замените тело Configure подключения в примере 1:

```csharp
options.Connection.Uri = new Uri("amqp://guest:guest@localhost:5673/");
options.ManagementUri = new Uri("http://localhost:15673/");
options.Topology = TopologyMode.ValidateOnly;
options.ReconnectInterval = TimeSpan.FromSeconds(5);
```

ValidateOnly использует HTTP GET Management API и учётные данные из Connection; проверяет
exchange, очереди, их параметры и bindings. Ресурсы не создаются. CreateMissing использует
AMQP declarations и также отклоняет несовместимые ресурсы. Ошибка топологии останавливает
запуск host. Совпадающие объявления общего exchange допускаются; разные типы — ошибка.
Настройки TLS, учётные данные и virtual host задаются приложением через ConnectionFactory;
секреты передавайте через конфигурацию окружения. После регистрации менять options нельзя.

<a id="connections"></a>
## 16. Несколько подключений и Kafka в одной шине

Замените целиком единственный вызов AddSeedWorkMessaging. Для Kafka добавьте ссылку
на `src/SeedWork.Messaging.Kafka/SeedWork.Messaging.Kafka.csproj` через `dotnet add <проект> reference <путь>`
и `using SeedWork.Messaging.Kafka;`. Пример — регистрация
маршрутов издателя; очереди RabbitMQ предварительно создают получатели.

```csharp
builder.Services.AddSeedWorkMessaging(bus =>
{
    bus.AddRabbitMq("sales-rabbit", rabbit => rabbit
        .Configure(o => { o.Connection.Uri = new Uri(builder.Configuration["SalesRabbit"]!); o.Topology = TopologyMode.CreateMissing; })
        .Exchange("orders.v1")
        .Publish<OrderCreated>("sales-orders", "orders.v1", "orders.created"));
    bus.AddRabbitMq("audit-rabbit", rabbit => rabbit
        .Configure(o => { o.Connection.Uri = new Uri(builder.Configuration["AuditRabbit"]!); o.Topology = TopologyMode.CreateMissing; })
        .Exchange("orders.v1")
        .Publish<OrderCreated>("audit-orders", "orders.v1", "orders.created"));
    bus.AddKafka("analytics-kafka", kafka => kafka
        .Configure(o => { o.Client.BootstrapServers = builder.Configuration["Kafka"]!; o.Topology = TopologyMode.CreateMissing; })
        .Topic("orders.v1", 2, 1)
        .Publish<OrderCreated>("analytics-orders", "orders.v1"));
});
```

Задайте SalesRabbit, AuditRabbit и Kafka в конфигурации. Один PublishAsync отправляет
в один маршрут. Для двух назначений нужны два вызова; общей транзакции между ними нет.
AddSeedWorkMessaging вызывается один раз на IServiceCollection.

<a id="telemetry"></a>
## 17. Логи, трассы и метрики

Добавьте пакеты OpenTelemetry.Extensions.Hosting и OpenTelemetry.Exporter.OpenTelemetryProtocol
версии 1.19.1, а также `using OpenTelemetry.Logs;`, `using OpenTelemetry.Metrics;`,
`using OpenTelemetry.Resources;`, `using OpenTelemetry.Trace;`. До Build вставьте:

```csharp
builder.Services.AddOpenTelemetry()
    .ConfigureResource(r => r.AddService("billing-rabbit"))
    .WithTracing(t => t.AddSource(MessagingTelemetry.InstrumentationName,
        RabbitMqTransport.InstrumentationName).AddOtlpExporter())
    .WithMetrics(m => m.AddMeter(MessagingTelemetry.InstrumentationName,
        RabbitMqTransport.InstrumentationName).AddOtlpExporter());
builder.Logging.AddOpenTelemetry(o => o.AddOtlpExporter());
```

В отдельном терминале из корня репозитория:

```powershell
docker compose -p seedwork-messaging -f samples/Messaging/compose.yaml --profile telemetry up -d dashboard
$env:OTEL_EXPORTER_OTLP_ENDPOINT = 'http://localhost:4317'
$env:OTEL_EXPORTER_OTLP_PROTOCOL = 'grpc'
dotnet run --project artifacts/tutorials/RabbitConsumer
```

Dashboard: `http://localhost:18888`. Для связанных трассировок настройте экспорт и у издателя.
Общие метрики: messaging.published, messaging.publish.errors, messaging.consumed,
messaging.retries, messaging.errors и длительности publish/consume. Дополнительно:
`messaging.rabbitmq.error_queue.messages`. Тело сообщения и текст исключения библиотека
в телеметрию не пишет; прикладные логи определяет ваш код.

<a id="operations"></a>
## Гарантии и типичные ошибки

Обработка последовательна внутри очереди; разные очереди независимы. Доставка может
повториться после потери соединения или подтверждения, поэтому бизнес-действия должны
учитывать дубли. MessageId помогает корреляции, но библиотека не хранит таблицу дедупликации.
Готового outbox и атомарности с транзакцией БД нет. JSON использует web defaults;
совместимое добавление необязательных полей возможно, несовместимый контракт требует новой версии ресурса.

Публикация использует persistent messages, confirms и отдельный channel на вызов.
Массовая публикация не оптимизировалась бенчмарками. ReconnectInterval управляет восстановлением
всего именованного подключения, а Retry — повтором прикладного обработчика.

| Симптом | Что проверить / исправить |
|---|---|
| Unknown route | Зарегистрировать Publish с тем же именем и типом сообщения |
| Messaging has not started | Вызывать издателя после успешного StartAsync |
| Duplicate endpoint / route / connection | Дать уникальные имена внутри шины |
| Consumer already registered | Убрать повторное сканирование или ручную регистрацию той же пары |
| Incomplete RabbitMQ configuration | Заполнить Queue и Exchange после применения класса |
| Conflicting types | Указать один и тот же enum-тип во всех объявлениях exchange |
| Ошибка обязательной маршрутизации | Сначала создать очередь и binding; согласовать routing key с типом exchange |
| ValidateOnly требует ManagementUri | Задать адрес Management API с завершающим `/` и доступными учётными данными |
| Несовместимая топология при запуске | Согласовать существующие durable/quorum/SAC/bindings с декларациями приложения |
| Первая ошибка сразу попадает в error queue | Это default; включить повторы через MaxRetries при необходимости |
| Сообщение приходит снова при MaxRetries = 0 | Проверить подтверждение error queue и соединение; повторная доставка отличается от прикладного retry |

См. [общие контракты](../SeedWork.Messaging/README.md), [руководство Kafka](../SeedWork.Messaging.Kafka/README.md)
и [готовый пример двух сервисов](../../samples/Messaging/README.md).
