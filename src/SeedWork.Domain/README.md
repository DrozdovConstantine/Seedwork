# SeedWork.Domain

Небольшой самостоятельный пакет для доменной модели на .NET 11. Не имеет зависимостей
от других NuGet-пакетов. Бизнес-правила, идентификаторы и данные событий определяет сервис.

## Пример

После подключения `SeedWork.Domain` следующий код можно использовать как `Program.cs`
консольного приложения на .NET 11:

```csharp
using SeedWork.Domain;

var order = new Order(new OrderId(Guid.NewGuid()));
order.Confirm();

IAggregateRoot aggregate = order;
foreach (var domainEvent in aggregate.DomainEvents)
    Console.WriteLine(domainEvent); // В сервисе здесь вызывается выбранный диспетчер.

// Очистка только после успешной передачи всех событий.
aggregate.ClearDomainEvents();

public readonly record struct OrderId(Guid Value);

public sealed class Order(OrderId id) : AggregateRoot<OrderId>(id)
{
    public bool IsConfirmed { get; private set; }

    public void Confirm()
    {
        if (IsConfirmed)
            return;

        IsConfirmed = true;
        AddDomainEvent(new OrderConfirmed(Id));
    }
}

public sealed record OrderConfirmed(OrderId OrderId) : IDomainEvent;
```

## Равенство сущностей

`Entity<TId>` принимает ID в защищённом конструкторе и предоставляет свойство `Id`
только для чтения. Используйте неизменяемые значения ID со стабильными `Equals` и
`GetHashCode`: пакет не может предотвратить изменение содержимого ссылочного ID.

Разные экземпляры равны, когда совпадают их конкретный тип и назначенный ID.
Сравнение ID использует `EqualityComparer<TId>.Default`. Пустым считается
`default(TId)`: например, `0` для `int`, `Guid.Empty` для `Guid`, `null` для
ссылочного или nullable-типа. Пустая строка считается назначенным ID.

Сущности с пустым ID равны только самим себе; их хеш определяется экземпляром.
Для назначенного ID хеш учитывает тип сущности и ID. Эти правила действуют для
`Equals`, `IEquatable<Entity<TId>>`, коллекций вроде `HashSet` и операторов `==` и `!=`.
Два `null` равны; сущность и `null` не равны независимо от порядка операндов.
Операторы доступны через `Entity<TId>` и его наследников. При сравнении переменных
типа `object` операторы сохраняют стандартное сравнение ссылок.

Для объектов-значений используйте неизменяемые `record` или `record struct`.

## Сильнотипизированные идентификаторы

Сервис может определить собственный ID как `readonly record struct`, как `OrderId`
в примере выше. Такой тип сравнивается по значению вложенного `Guid` и отделяет
идентификатор заказа от идентификаторов других сущностей на уровне типов.
Общий интерфейс или базовый класс идентификатора для использования `Entity<TId>`
не требуется; конкретные типы ID остаются в сервисе.

Создавайте назначенный ID через `new OrderId(Guid.NewGuid())`.
`default(OrderId)` и `new OrderId(Guid.Empty)` равны и считаются неназначенными ID.
Сами эти значения ID равны между собой, но разные экземпляры сущностей с такими
ID не равны. Сущности одного конкретного типа с одинаковым непустым `OrderId`
равны и имеют одинаковый хеш-код.

## События

`IDomainEvent` — маркер без обязательных полей. `AggregateRoot<TId>` накапливает
события в порядке добавления, сохраняет дубликаты и запрещает `null`.
`DomainEvents` — живая коллекция только для чтения: ранее полученное представление
отражает последующие добавления и очистку. Для снимка используйте `ToArray()`.

`AddDomainEvent<TEvent>` принимает ссылочные типы событий. Прямая передача
`struct` или `record struct` не компилируется. Ограничение не предотвращает
предварительную упаковку структуры в `IDomainEvent`, поэтому используйте для
событий только классы и `record class`.

Пакет не публикует события и не очищает их автоматически. Вызывающий код определяет
момент успешной передачи диспетчеру и вызывает `ClearDomainEvents()`; повторная
очистка допустима. Гарантии доставки и обработка частичной отправки остаются
ответственностью приложения. Не изменяйте коллекцию во время её перечисления.
Агрегаты не рассчитаны на одновременный доступ из нескольких потоков.

После успешной передачи отдельного события `aggregate.RemoveDomainEvent(domainEvent)`
удаляет первое вхождение этого экземпляра по ссылке и возвращает `true`; для
отсутствующего экземпляра возвращается `false`. Равное по значению, но другое
событие не удаляется. Гарантии доставки остаются ответственностью приложения.

## Бизнес-правила и спецификации

Правило реализует `IBusinessRule`: `IsBroken()` сообщает о нарушении, а `Message`
описывает его. Наследник `Entity<TId>` может вызвать `CheckRule(rule)`; нарушение
вызывает `DomainException`. Исключение также принимает внутреннюю причину.

Например, заказ разрешает подтверждение только при положительной сумме и
использует спецификацию, чтобы определить бесплатную доставку. Следующий пример
можно запустить как отдельный `Program.cs`:

```csharp
using SeedWork.Domain;

var order = new PurchaseOrder(Guid.NewGuid(), 150m);
order.Confirm();
Console.WriteLine(order.HasFreeDelivery); // True

public sealed class PurchaseOrder(Guid id, decimal total) : AggregateRoot<Guid>(id)
{
    public decimal Total { get; } = total;
    public bool IsConfirmed { get; private set; }
    public bool HasFreeDelivery { get; private set; }

    public void Confirm()
    {
        if (IsConfirmed)
            return;

        // Проверяем инвариант до изменения состояния и добавления события.
        CheckRule(new OrderTotalMustBePositive(Total));
        ISpecification<PurchaseOrder> freeDelivery = new FreeDeliverySpecification();
        HasFreeDelivery = freeDelivery.IsSatisfiedBy(this);
        IsConfirmed = true;
        AddDomainEvent(new PurchaseOrderConfirmed(Id, HasFreeDelivery));
    }
}

public sealed record OrderTotalMustBePositive(decimal Total) : IBusinessRule
{
    public bool IsBroken() => Total <= 0;
    public string Message => "Order total must be positive.";
}

public sealed class FreeDeliverySpecification : ISpecification<PurchaseOrder>
{
    public bool IsSatisfiedBy(PurchaseOrder order) => order.Total >= 100m;
}

public sealed record PurchaseOrderConfirmed(Guid OrderId, bool HasFreeDelivery) : IDomainEvent;
```

При сумме `0` или меньше `Confirm()` выбросит `DomainException`, не изменив
состояние заказа и не добавив событие. Порог доставки — бизнес-правило примера,
которое сервис определяет самостоятельно.

`SpecificationExtensions` предоставляет методы расширения `And`, `Or` и `Not`.
Например, с `FreeDeliverySpecification` из примера выше можно проверить заказ,
который ещё не подтверждён и уже подходит для бесплатной доставки:

```csharp
ISpecification<PurchaseOrder> confirmed = new ConfirmedOrderSpecification();
ISpecification<PurchaseOrder> freeDelivery = new FreeDeliverySpecification();

ISpecification<PurchaseOrder> eligible = confirmed.Not().And(freeDelivery);
bool matches = eligible.IsSatisfiedBy(order);

public sealed class ConfirmedOrderSpecification : ISpecification<PurchaseOrder>
{
    public bool IsSatisfiedBy(PurchaseOrder order) => order.IsConfirmed;
}
```

`And` требует выполнения обоих условий, `Or` — хотя бы одного, а `Not`
инвертирует результат. `And` и `Or` не проверяют второе условие, если результат
уже определён первым. Спецификации проверяются в памяти; преобразование в запросы
к базе данных не поддерживается.

