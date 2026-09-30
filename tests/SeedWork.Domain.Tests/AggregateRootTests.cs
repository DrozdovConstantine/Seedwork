namespace SeedWork.Domain.Tests;

public sealed class AggregateRootTests
{
    [Fact]
    public void EventsPreserveOrderAndDuplicatesInLiveView()
    {
        var aggregate = new Order(Guid.NewGuid());
        IAggregateRoot root = aggregate;
        var view = root.DomainEvents;
        Assert.Empty(view);
        var first = new OrderChanged(1);
        var second = new OrderChanged(2);

        aggregate.Record(first);
        aggregate.Record(second);
        aggregate.Record(first);

        Assert.Collection(view,
            item => Assert.Same(first, item),
            item => Assert.Same(second, item),
            item => Assert.Same(first, item));
    }

    [Fact]
    public void ClearIsRepeatableAndAllowsFurtherEvents()
    {
        var aggregate = new Order(Guid.NewGuid());
        IAggregateRoot root = aggregate;
        var view = root.DomainEvents;
        aggregate.Record(new OrderChanged(1));

        root.ClearDomainEvents();
        Assert.Empty(view);
        root.ClearDomainEvents();
        Assert.Empty(view);

        var next = new OrderChanged(2);
        aggregate.Record(next);
        Assert.Same(next, Assert.Single(view));
    }

    [Fact]
    public void NullEventIsRejectedWithoutChangingPendingEvents()
    {
        var aggregate = new Order(Guid.NewGuid());
        var pending = new OrderChanged(1);
        aggregate.Record(pending);

        Assert.Throws<ArgumentNullException>(() => aggregate.Record(null!));
        Assert.Same(pending, Assert.Single(aggregate.DomainEvents));
    }

    [Fact]
    public void CallersCannotMutatePendingEventsThroughCollectionInterfaces()
    {
        var aggregate = new Order(Guid.NewGuid());
        var pending = new OrderChanged(1);
        aggregate.Record(pending);
        var collection = Assert.IsAssignableFrom<ICollection<IDomainEvent>>(aggregate.DomainEvents);
        var list = Assert.IsAssignableFrom<IList<IDomainEvent>>(aggregate.DomainEvents);

        Assert.True(collection.IsReadOnly);
        Assert.Throws<NotSupportedException>(() => collection.Add(new OrderChanged(2)));
        Assert.Throws<NotSupportedException>(() => collection.Clear());
        Assert.Throws<NotSupportedException>(() => collection.Remove(pending));
        Assert.Throws<NotSupportedException>(() => list[0] = new OrderChanged(2));
        Assert.Same(pending, Assert.Single(aggregate.DomainEvents));
    }

    private sealed class Order(Guid id) : AggregateRoot<Guid>(id)
    {
        public void Record(IDomainEvent domainEvent) => AddDomainEvent(domainEvent);
    }

    private sealed record OrderChanged(int Version) : IDomainEvent;
}
