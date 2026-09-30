# SeedWork.Observability — план совместимости с Aspire

## Назначение и границы

Необязательный NuGet-пакет для регистрации телеметрии библиотек SeedWork в приложении на .NET 10. Библиотеки создают сигналы стандартными API .NET: `ILogger`, `ActivitySource` и `Meter`. Aspire Service Defaults на стороне сервиса настраивает OpenTelemetry и экспорт в Aspire Dashboard или иной OTLP-приёмник. Домен и контракты без выполняемых операций не получают искусственную телеметрию.

## Публичный API и схема подключения

- Каждый пакет с выполняемыми операциями, начиная с будущих инфраструктурных адаптеров, публикует константу `InstrumentationName` и использует её для собственного `ActivitySource` и `Meter`. Имя стабильно внутри основной версии пакета.
- `SeedWork.Observability` предоставляет `AddSeedWorkInstrumentation(this IServiceCollection services, params string[] instrumentationNames)`. Метод регистрирует имена как источники трасс и метрик через OpenTelemetry; он не создаёт экспортёр и не меняет настройки Aspire Service Defaults.
- Сервис вызывает `builder.AddServiceDefaults()`, затем `builder.Services.AddSeedWorkInstrumentation(ИмяАдаптера, ...)`. Без Aspire сервис может настроить OpenTelemetry самостоятельно и вызвать тот же метод.
- `ILogger` получается через DI непосредственно в операционных библиотеках. Trace context берётся из `Activity.Current`; новый корневой trace для каждой внутренней операции не создаётся.

Пакет не зависит от конкретного брокера, EF-провайдера, Sentry, Serilog или Jaeger. Экспорт, sampling, имя сервиса и конфиденциальность атрибутов настраивает приложение. Не помещать в атрибуты трасс содержимое сообщений, файлы, секреты и персональные данные.

## Этапы реализации

1. Создать пакет для `net10.0` с зависимостью только от необходимых OpenTelemetry API для регистрации источников и метрик.
2. Реализовать метод регистрации так, чтобы он работал с Aspire Service Defaults и не заменял существующую конфигурацию OpenTelemetry.
3. Добавить пример сервиса с Aspire AppHost: одна операция создаёт вложенный span, счётчик и структурированный лог из тестового инструментированного компонента.
4. Описать подключение нового пакета SeedWork: определить стабильное имя, создать источники в библиотеке и передать имя в `AddSeedWorkInstrumentation` на стороне хоста.

## Проверки и критерий готовности

- Автоматический тест с in-memory OpenTelemetry exporter подтверждает появление span и метрики после регистрации имени; без регистрации собственные сигналы не собираются.
- Интеграционный пример показывает логи, трассы и метрики в Aspire Dashboard и сохраняет связь дочернего span с входящим запросом.
- Повторный вызов регистрации не создаёт второй экспортёр. `dotnet test` и `dotnet pack` проходят.

## Порядок внедрения

### Реализованные источники Messaging

- `MessagingTelemetry.InstrumentationName` = `SeedWork.Messaging`: публикация и обработка,
  число сообщений, длительности, повторы и ошибки.
- `RabbitMqTransport.InstrumentationName` = `SeedWork.Messaging.RabbitMQ`: отправка
  сообщений в error queue.
- `KafkaTransport.InstrumentationName` = `SeedWork.Messaging.Kafka`: приостановка
  партиции и `messaging.kafka.paused_partitions`.

Регистрировать общий источник и источники используемых адаптеров. До реализации
`AddSeedWorkInstrumentation` пример `samples/Messaging` использует стандартные
`AddSource` / `AddMeter` и OTLP exporter напрямую. При подключении Aspire Service
Defaults exporter повторно не регистрировать. В тестах Messaging in-memory exporter
проверяет связь producer/consumer spans и появление метрик.

Этот пакет можно реализовать до конкретных адаптеров на тестовом инструментированном компоненте. По мере появления адаптеров сервис передаёт их `InstrumentationName` в регистрацию без изменения доменного пакета.

## Основание

Aspire использует OpenTelemetry и стандартные API .NET для логов, трасс и метрик; Service Defaults отвечает за конфигурацию экспорта: [телеметрия Aspire](https://learn.microsoft.com/dotnet/aspire/fundamentals/telemetry), [Service Defaults](https://learn.microsoft.com/dotnet/aspire/fundamentals/service-defaults).
