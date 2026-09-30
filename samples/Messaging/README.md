# Два сервиса, два транспорта

Пошаговые руководства по отдельным пакетам:

- [RabbitMQ: 17 примеров](../../src/SeedWork.Messaging.RabbitMQ/README.md).
- [Kafka: 16 примеров](../../src/SeedWork.Messaging.Kafka/README.md).

В руководствах есть самостоятельные программы и варианты настройки через атрибуты,
классы конфигурации и Assembly, а также разбор топологии, повторов и ошибок.

Требуются .NET 10 и Docker. Из корня репозитория:

```powershell
docker compose -p seedwork-messaging -f samples/Messaging/compose.yaml --profile telemetry up -d --wait
dotnet run --project samples/Messaging/Consumer
# В другом терминале, после запуска Consumer:
dotnet run --project samples/Messaging/Publisher -- --Count=5
```

Consumer сначала создаёт RabbitMQ queue/binding: обязательная маршрутизация
издателя отклоняет публикацию без подписчика. Оба приложения одновременно
используют Kafka и RabbitMQ, но объявляют `OrderSubmitted` независимо.
У consumer есть добавленное необязательное поле Currency. Общей сборки контрактов нет.

Подписки регистрируются одним `AddConsumersFromAssembly(typeof(OrderConsumer).Assembly)` на каждый
транспорт. RabbitMQ демонстрирует атрибут и отдельный класс конфигурации, который
явно включает повторы; Kafka — только класс конфигурации с RetryOptions.NoRetry().
Настройки подписок заданы в коде; конфигурация приложения используется для адресов
и учётных данных брокеров.
Топология описана в настройках консумера; отдельные Exchange/Topic перед сканированием не нужны.

Адреса задаются ключами `RabbitMQ:HostName`, `RabbitMQ:Port` и `Kafka:BootstrapServers`.
Учётные данные RabbitMQ задаются отдельно через `RabbitMQ:UserName` и `RabbitMQ:Password`;
для Kafka при необходимости используются `Kafka:SecurityProtocol`, `Kafka:SaslMechanism`,
`Kafka:SaslUsername` и `Kafka:SaslPassword` (аргументы либо environment с `__` вместо `:`).
Значения по умолчанию находятся только в примере. Production-конфигурация пакетов
не содержит адресов. Для production увеличьте replication factor Kafka согласно
кластеру; в примере один broker и factor=1.

Aspire Dashboard: http://localhost:18888; OTLP gRPC: http://localhost:4317.
Пример регистрирует sources/meters и экспорт логов напрямую через OpenTelemetry.
При использовании Aspire Service Defaults экспорт настраивается там, повторно
добавлять exporter не следует. Dashboard в Compose анонимный и доступен только
через loopback. RabbitMQ Management: http://localhost:15673 (guest/guest).

Проверьте связанные spans `submit-order` → `publish` → `process`, метрики
`messaging.published`, `messaging.consumed`, длительности и retries.
Consumer поддерживает `--RunSeconds=30` для ограниченного запуска.

## Тесты

```powershell
docker compose -p seedwork-messaging -f samples/Messaging/compose.yaml up -d --wait rabbitmq kafka
$env:SEEDWORK_BROKER_TESTS = '1'
dotnet test SeedWork.slnx
docker compose -p seedwork-messaging -f samples/Messaging/compose.yaml --profile telemetry down
```

Без `SEEDWORK_BROKER_TESTS=1` интеграционные тесты явно помечены skipped.
Переопределение адресов тестов: `SEEDWORK_KAFKA`, `SEEDWORK_RABBITMQ`,
`SEEDWORK_RABBITMQ_MANAGEMENT`. Используйте отдельные тестовые брокеры:
тесты создают уникальные topics/queues, удаляют тестовую error queue и закрывают
только своё именованное RabbitMQ-соединение. Для очистки ресурсов завершите
выделенный Compose-проект. Compose не использует постоянные volumes Kafka.
