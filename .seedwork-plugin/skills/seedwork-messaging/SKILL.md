---
name: seedwork-messaging
description: Use when integrating SeedWork.Messaging publishers or consumers, or configuring its Kafka and RabbitMQ transports. Do not assume any package or broker is installed.
---

# SeedWork.Messaging

Inspect package references, target framework, installed package versions, existing configuration, and the target broker before editing. This guidance describes the repository's 0.1.0 API; verify the actual version and its documentation. Add or upgrade packages only when the task calls for it.

- Register messaging once with `services.AddSeedWorkMessaging(...)`. Choose `SeedWork.Messaging.Kafka` and/or `SeedWork.Messaging.RabbitMQ` for the broker actually used. Each transport has its own named connection and topology configuration; don't invent a shared broker abstraction beyond the package API.
- Register a typed route with the transport builder's `Publish<T>` and publish through `IMessagePublisher.PublishAsync<T>(route, message, options, cancellationToken)` after the host has started. The route and exact contract type must match registration.
- Implement `IConsumer<T>.ConsumeAsync(MessageContext<T> context, CancellationToken cancellationToken)`. Register consumers using the chosen transport's `Consume` API or its supported configuration-class assembly scanning; avoid duplicate registrations and use unique connection, endpoint, and route names.
- For Kafka, configure connection, topic and consumer group; instances in the same group share partitions. For RabbitMQ, configure the connection, exchange, queue and bindings appropriate to the chosen topology. Check `TopologyMode` and broker permissions before selecting creation rather than validation.
- Broker confirmation is not consumer completion, exactly-once delivery, or a transaction with the database. Consumers must account for redelivery and cancellation. A message ID helps correlation but is not automatic deduplication; the library does not provide a ready-made outbox.
- Preserve existing secret configuration and avoid embedding credentials. Respect reserved `sw-`, `traceparent`, and `tracestate` headers. Kafka keys and RabbitMQ routing keys have transport-specific meanings.

For exact configuration signatures and operational behavior, consult the matching version of `src/SeedWork.Messaging/README.md`, the Kafka and RabbitMQ package READMEs, and `samples/Messaging` when available.
