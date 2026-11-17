namespace SeedWork.Domain.Tests;

public sealed class DomainExtensionsTests
{
    [Fact]
    public void RuleAllowsValidStateAndReportsViolation()
    {
        var entity = new TestEntity();
        entity.Validate(new Rule(false, "invalid"));
        var error = Assert.Throws<DomainException>(() => entity.Validate(new Rule(true, "invalid")));
        Assert.Equal("invalid", error.Message);
        Assert.Throws<ArgumentNullException>(() => entity.Validate(null!));
    }

    [Fact]
    public void DomainExceptionPreservesCause()
    {
        var cause = new InvalidOperationException("cause");
        var error = new DomainException("invalid", cause);
        Assert.Equal("invalid", error.Message);
        Assert.Same(cause, error.InnerException);
    }

    [Theory]
    [InlineData(-2, false, true, true)]
    [InlineData(1, false, true, false)]
    [InlineData(2, true, true, false)]
    [InlineData(-1, false, false, true)]
    public void SpecificationsComposeInMemory(int value, bool and, bool or, bool not)
    {
        ISpecification<int> positive = new PredicateSpec(x => x > 0);
        ISpecification<int> even = new PredicateSpec(x => x % 2 == 0);
        Assert.Equal(and, positive.And(even).IsSatisfiedBy(value));
        Assert.Equal(or, positive.Or(even).IsSatisfiedBy(value));
        Assert.Equal(not, positive.Not().IsSatisfiedBy(value));
    }

    [Fact]
    public void CompositionShortCircuitsAndRejectsNull()
    {
        ISpecification<int> yes = new PredicateSpec(_ => true);
        ISpecification<int> no = new PredicateSpec(_ => false);
        ISpecification<int> throws = new PredicateSpec(_ => throw new InvalidOperationException());
        Assert.False(no.And(throws).IsSatisfiedBy(1));
        Assert.True(yes.Or(throws).IsSatisfiedBy(1));
        Assert.Throws<ArgumentNullException>(() => yes.And(null!));
        Assert.Throws<ArgumentNullException>(() => yes.Or(null!));
        Assert.Throws<ArgumentNullException>(() => SpecificationExtensions.Not<int>(null!));
    }

    private sealed class TestEntity() : Entity<int>(1)
    {
        public void Validate(IBusinessRule rule) => CheckRule(rule);
    }

    private sealed record Rule(bool Broken, string Message) : IBusinessRule
    {
        public bool IsBroken() => Broken;
    }

    private sealed record PredicateSpec(Func<int, bool> Predicate) : ISpecification<int>
    {
        public bool IsSatisfiedBy(int candidate) => Predicate(candidate);
    }
}
