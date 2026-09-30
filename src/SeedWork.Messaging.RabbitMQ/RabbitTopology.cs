using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using RabbitMQ.Client;

namespace SeedWork.Messaging.RabbitMQ;

internal static class RabbitTopology
{
    // Отключаем лимит возвратов входной очереди, чтобы недоступная error queue не приводила к удалению сообщения брокером.
    // Single active consumer сохраняет последовательную обработку при нескольких экземплярах сервиса.
    private static Dictionary<string, object?> QueueArguments(bool consumer) => consumer
        ? new() { ["x-queue-type"] = "quorum", ["x-single-active-consumer"] = true, ["x-delivery-limit"] = -1 }
        : new() { ["x-queue-type"] = "quorum" };

    public static async Task EnsureAsync(IConnection connection, RabbitMqBuilder config, ConnectionFactory factory, CancellationToken ct)
    {
        if (config.Options.Topology == TopologyMode.ValidateOnly)
        {
            await ValidateAsync(config, factory, ct);
            return;
        }
        // AMQP declarations создают отсутствующие ресурсы и отклоняют несовместимые параметры существующих.
        await using var channel = await connection.CreateChannelAsync(cancellationToken: ct);
        foreach (var (name, type) in config.Exchanges)
            await channel.ExchangeDeclareAsync(name, type, durable: true, autoDelete: false, cancellationToken: ct);
        foreach (var e in config.Endpoints)
        {
            await channel.QueueDeclareAsync(e.Queue, true, false, false, QueueArguments(true), cancellationToken: ct);
            await channel.QueueDeclareAsync(e.ErrorQueue, true, false, false, QueueArguments(false), cancellationToken: ct);
            await channel.QueueBindAsync(e.Queue, e.Exchange, e.BindingKey, cancellationToken: ct);
        }
    }

    private static async Task ValidateAsync(RabbitMqBuilder config, ConnectionFactory factory, CancellationToken ct)
    {
        // Management API позволяет проверить аргументы и bindings чтением, без риска создать недостающий ресурс.
        using var client = new HttpClient { BaseAddress = config.Options.ManagementUri, Timeout = TimeSpan.FromSeconds(30) };
        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic",
            Convert.ToBase64String(Encoding.UTF8.GetBytes(factory.UserName + ":" + factory.Password)));
        var vhost = Uri.EscapeDataString(factory.VirtualHost);
        async Task<JsonDocument> Get(string resource)
        {
            using var response = await client.GetAsync("api/" + resource, ct);
            if (!response.IsSuccessStatusCode) throw new InvalidOperationException($"RabbitMQ topology inspection failed ({(int)response.StatusCode}).");
            return JsonDocument.Parse(await response.Content.ReadAsByteArrayAsync(ct));
        }
        foreach (var (name, type) in config.Exchanges)
        {
            using var doc = await Get($"exchanges/{vhost}/{Uri.EscapeDataString(name)}");
            var e = doc.RootElement;
            if (e.GetProperty("type").GetString() != type || !e.GetProperty("durable").GetBoolean() || e.GetProperty("auto_delete").GetBoolean())
                throw new InvalidOperationException($"Incompatible exchange: {name}");
        }
        foreach (var endpoint in config.Endpoints)
        {
            foreach (var (name, consumer) in new[] { (endpoint.Queue, true), (endpoint.ErrorQueue, false) })
            {
                using var doc = await Get($"queues/{vhost}/{Uri.EscapeDataString(name)}");
                var q = doc.RootElement;
                var arguments = q.GetProperty("arguments");
                if (!q.GetProperty("durable").GetBoolean() || q.GetProperty("auto_delete").GetBoolean() || q.GetProperty("exclusive").GetBoolean() ||
                    !arguments.TryGetProperty("x-queue-type", out var type) || type.GetString() != "quorum" ||
                    (consumer && (!arguments.TryGetProperty("x-single-active-consumer", out var sac) || !sac.GetBoolean() ||
                        !arguments.TryGetProperty("x-delivery-limit", out var limit) || limit.GetInt32() != -1)))
                    throw new InvalidOperationException($"Incompatible queue: {name}");
            }
            using var bindings = await Get($"bindings/{vhost}/e/{Uri.EscapeDataString(endpoint.Exchange)}/q/{Uri.EscapeDataString(endpoint.Queue)}");
            if (!bindings.RootElement.EnumerateArray().Any(b => b.GetProperty("routing_key").GetString() == endpoint.BindingKey))
                throw new InvalidOperationException($"Missing binding for endpoint: {endpoint.Name}");
        }
    }
}
