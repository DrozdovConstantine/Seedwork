# SeedWork

SeedWork — набор библиотек .NET для построения сервисов: доменной модели, прикладных сценариев и обмена сообщениями через брокеры.

## Библиотеки

| Пакет | Назначение |
| --- | --- |
| [SeedWork.Domain](src/SeedWork.Domain/README.md) | Сущности, корни агрегатов и доменные события. |
| [SeedWork.Application](src/SeedWork.Application/README.md) | Прикладные сценарии и настраиваемый pipeline с генерацией регистрации в DI. |
| [SeedWork.Messaging](src/SeedWork.Messaging/README.md) | Контракты публикации и обработки сообщений и общий pipeline обработки. |
| [SeedWork.Messaging.RabbitMQ](src/SeedWork.Messaging.RabbitMQ/README.md) | Адаптер обмена сообщениями через RabbitMQ. |
| [SeedWork.Messaging.Kafka](src/SeedWork.Messaging.Kafka/README.md) | Адаптер обмена сообщениями через Kafka. |

## Требования и разработка

Для сборки и запуска требуется .NET 11 SDK. Для примера обмена сообщениями и интеграционных тестов с брокерами также нужен Docker с Compose.

Из корня репозитория:

```shell
dotnet build SeedWork.slnx -c Release
dotnet test SeedWork.slnx -c Release
```

По умолчанию интеграционные тесты с брокерами пропускаются. Инструкции по запуску брокеров и этих тестов находятся в [документации примера Messaging](samples/Messaging/README.md#тесты).

## Примеры

- [Application](src/SeedWork.Application/README.md#один-запускаемый-пример) — прикладные сценарии и настройка pipeline.
- [Messaging](samples/Messaging/README.md) — два сервиса, использующие RabbitMQ и Kafka.

## Плагин для ИИ-агентов

Плагин SeedWork для Codex и Claude Code предоставляет навыки работы с Domain, Application и Messaging, включая адаптеры RabbitMQ и Kafka. Инструкции навыков написаны на английском языке и описывают API библиотек версии 0.1.0; агенту следует проверять версию, используемую проектом. Плагин не устанавливает NuGet-пакеты.

Подключение, обновление и проверка описаны в [документации плагина](.seedwork-plugin/README.md).

## Структура репозитория

- `src/` — исходный код библиотек.
- `tests/` — тесты библиотек.
- `samples/` — примеры использования.
- `Plans/` — планы развития компонентов; наличие плана не означает, что компонент реализован.
