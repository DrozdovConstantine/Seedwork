namespace SeedWork.Domain.Tests;

public sealed class StronglyTypedIdTests
{
    [Fact]
    public void EqualAssignedIdentifiersGiveEqualEntitiesAndHashCodes()
    {
        var value = Guid.Parse("11111111-1111-1111-1111-111111111111");
        var first = new Order(new OrderId(value));
        var second = new Order(new OrderId(value));

        Assert.True(first.Equals(second));
        Assert.True(second.Equals(first));
        Assert.True(first.Equals((object)second));
        Assert.True(first == second);
        Assert.False(first != second);
        Assert.Equal(first.GetHashCode(), second.GetHashCode());
        Assert.Single(new HashSet<Order> { first, second });
    }

    [Fact]
    public void DifferentAssignedIdentifiersGiveUnequalEntities()
    {
        var first = new Order(new OrderId(Guid.Parse("11111111-1111-1111-1111-111111111111")));
        var second = new Order(new OrderId(Guid.Parse("22222222-2222-2222-2222-222222222222")));

        Assert.False(first.Equals(second));
        Assert.False(second.Equals(first));
        Assert.False(first == second);
        Assert.True(first != second);
        Assert.Equal(2, new HashSet<Order> { first, second }.Count);
    }

    [Fact]
    public void DefaultAndEmptyIdentifiersGiveReferenceIdentity()
    {
        var first = new Order(default);
        var second = new Order(new OrderId(Guid.Empty));
        var same = first;

        Assert.Equal(default(OrderId), new OrderId(Guid.Empty));
        Assert.True(first.Equals(same));
        Assert.True(first == same);
        Assert.False(first != same);
        Assert.False(first.Equals(second));
        Assert.False(second.Equals(first));
        Assert.False(first == second);
        Assert.True(first != second);
        Assert.Equal(2, new HashSet<Order> { first, same, second }.Count);
    }

    private readonly record struct OrderId(Guid Value);
    private sealed class Order(OrderId id) : AggregateRoot<OrderId>(id);
}
