using System.Runtime.CompilerServices;

namespace SeedWork.Domain.Tests;

public sealed class EntityTests
{
    [Fact]
    public void OperatorsCompareDerivedAndBaseVariablesByIdentity()
    {
        var first = new Customer<int>(42);
        var equal = new Customer<int>(42);
        var different = new Customer<int>(43);

        Assert.True(first == equal);
        Assert.False(first != equal);
        Assert.False(first == different);
        Assert.True(first != different);
        AssertOperators(first, first, true);
        AssertOperators(first, equal, true);
        AssertOperators(first, different, false);
        AssertOperators(first, new Order(42), false);
        AssertOperators(first, new SpecialCustomer(42), false);
        AssertOperators(first, new Customer<int>(0), false);
    }

    [Fact]
    public void OperatorsHandleNullOnEitherSide()
    {
        var customer = new Customer<int>(42);
        Customer<int>? missing = null;
        Customer<int>? alsoMissing = null;

        Assert.False(customer == missing);
        Assert.True(customer != missing);
        Assert.False(missing == customer);
        Assert.True(missing != customer);
        Assert.True(missing == alsoMissing);
        Assert.False(missing != alsoMissing);
        AssertOperators(customer, null, false);
        AssertOperators<int>(null, null, true);
    }

    [Fact]
    public void SameTypeAndAssignedIdHaveEqualIdentity()
    {
        var first = new Customer<int>(42);
        var second = new Customer<int>(42);
        var third = new Customer<int>(42);

        Assert.True(first.Equals(second));
        Assert.True(second.Equals(first));
        Assert.True(second.Equals(third));
        Assert.True(first.Equals(third));
        Assert.True(first.Equals((object)second));
        Assert.True(((IEquatable<Entity<int>>)first).Equals(second));
        Assert.Equal(first.GetHashCode(), second.GetHashCode());
        Assert.Equal(42, first.Id);
        Assert.Single(new HashSet<Entity<int>> { first, second, third });
    }

    [Fact]
    public void DifferentIdsOrConcreteTypesAreNotEqual()
    {
        var customer = new Customer<int>(42);
        var order = new Order(42);
        var special = new SpecialCustomer(42);

        Assert.False(customer.Equals(new Customer<int>(43)));
        Assert.False(customer.Equals(order));
        Assert.False(order.Equals(customer));
        Assert.False(customer.Equals(special));
        Assert.False(special.Equals(customer));
        Assert.False(customer.Equals((Entity<int>?)null));
        Assert.False(customer.Equals((object?)null));
        Assert.False(customer.Equals(new object()));
        Assert.False(customer.Equals((object)new Customer<long>(42)));
        Assert.Equal(3, new HashSet<Entity<int>> { customer, order, special }.Count);
    }

    [Fact]
    public void DefaultIdentifiersUseReferenceIdentity()
    {
        AssertReferenceIdentity(0);
        AssertReferenceIdentity(Guid.Empty);
        AssertReferenceIdentity<string?>(null);
        AssertReferenceIdentity<int?>(null);
    }

    [Fact]
    public void AssignedAndDefaultIdentifiersAreNotEqual()
    {
        var assigned = new Customer<int>(1);
        var unassigned = new Customer<int>(0);

        Assert.False(assigned.Equals(unassigned));
        Assert.False(unassigned.Equals(assigned));
    }

    [Fact]
    public void AssignedIdentifiersUseDefaultComparer()
    {
        AssertAssignedIdentity(Guid.NewGuid());
        AssertAssignedIdentity(string.Empty);
        AssertAssignedIdentity("customer-1");
        AssertAssignedIdentity<int?>(0);
        AssertAssignedIdentity(new CustomerId(7));
        Assert.False(new Customer<string>("ABC").Equals(new Customer<string>("abc")));
    }

    private static void AssertReferenceIdentity<TId>(TId id)
    {
        var first = new Customer<TId>(id);
        var second = new Customer<TId>(id);

        Assert.True(first.Equals(first));
        Assert.True(first.Equals((object)first));
        Assert.False(first.Equals(second));
        Assert.False(second.Equals(first));
        Assert.Equal(RuntimeHelpers.GetHashCode(first), first.GetHashCode());
        AssertOperators(first, first, true);
        AssertOperators(first, second, false);
        AssertOperators(first, null, false);
        var set = new HashSet<Entity<TId>> { first, first, second };
        Assert.Equal(2, set.Count);
        Assert.Contains(first, set);
        Assert.Contains(second, set);
    }

    private static void AssertAssignedIdentity<TId>(TId id)
    {
        var first = new Customer<TId>(id);
        var second = new Customer<TId>(id);
        Assert.True(first.Equals(first));
        Assert.True(first.Equals(second));
        Assert.Equal(first.GetHashCode(), second.GetHashCode());
        AssertOperators(first, first, true);
        AssertOperators(first, second, true);
    }

    private static void AssertOperators<TId>(Entity<TId>? left, Entity<TId>? right, bool expected)
    {
        Assert.Equal(expected, left == right);
        Assert.Equal(expected, right == left);
        Assert.Equal(!expected, left != right);
        Assert.Equal(!expected, right != left);
        Assert.Equal(expected, object.Equals(left, right));
    }

    private class Customer<TId>(TId id) : Entity<TId>(id);
    private sealed class SpecialCustomer(int id) : Customer<int>(id);
    private sealed class Order(int id) : Entity<int>(id);
    private sealed record CustomerId(int Value);
}
