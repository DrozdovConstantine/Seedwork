---
name: seedwork-domain
description: Use when implementing or changing a domain model that uses SeedWork.Domain entities, aggregate roots, identifiers, or domain events. Do not assume the library is installed.
---

# SeedWork.Domain

First inspect the consuming project's package references, target framework, and installed SeedWork.Domain version. Match its actual API and project conventions; these notes describe the repository's 0.1.0 API, not a promise about other versions. Consult package documentation or source for the matching version when available. Do not install or upgrade packages unless the task calls for it.

- Derive domain entities from `Entity<TId>` and aggregate roots from `AggregateRoot<TId>`. Define business rules and immutable identifier types in the application domain. An ID does not need a SeedWork interface; `readonly record struct` is suitable.
- Entity equality uses concrete runtime type and assigned ID. `default(TId)` is unassigned, so distinct entities with default IDs are unequal. Use stable, immutable IDs, especially when entities are dictionary or set keys.
- Define events by implementing the marker `IDomainEvent`. An aggregate records them with its protected `AddDomainEvent` method; `IAggregateRoot.DomainEvents` exposes a live read-only view. Use a snapshot when required.
- The library neither dispatches nor automatically clears events. The application chooses dispatch and reliability semantics and calls `ClearDomainEvents()` after successful handoff. Do not claim atomic delivery or thread safety.

For authoritative examples, consult `src/SeedWork.Domain/README.md` in the matching SeedWork source version if available.
