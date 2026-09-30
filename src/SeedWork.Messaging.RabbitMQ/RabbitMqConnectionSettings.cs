using System.Net.Security;
using System.Security.Authentication;
using RabbitMQ.Client;

namespace SeedWork.Messaging.RabbitMQ;

/// <summary>Единственный способ задания параметров одного подключения RabbitMQ.</summary>
public interface IRabbitMqConnectionSettings
{
    /// <summary>Проверяет настройки и создаёт фабрику клиента.</summary>
    ConnectionFactory CreateFactory();
}

/// <summary>Настройки подключения RabbitMQ через AMQP URI.</summary>
public sealed class RabbitMqUriConnectionSettings(Uri uri) : IRabbitMqConnectionSettings
{
    public Uri Uri { get; } = uri;

    public ConnectionFactory CreateFactory()
    {
        ArgumentNullException.ThrowIfNull(Uri);
        if (!Uri.IsAbsoluteUri || Uri.Scheme is not ("amqp" or "amqps") ||
            string.IsNullOrWhiteSpace(Uri.Host) || Uri.UserInfo.IndexOf(':') <= 0 ||
            Uri.UserInfo.EndsWith(':'))
            throw new ArgumentException("RabbitMQ URI must contain an AMQP scheme, host, username and password.");
        var factory = new ConnectionFactory { Uri = Uri };
        if (factory.Ssl.Enabled)
        {
            factory.Ssl.AcceptablePolicyErrors = SslPolicyErrors.None;
            factory.Ssl.Version = SslProtocols.None;
        }
        return factory;
    }
}

/// <summary>Настройки подключения RabbitMQ через отдельные поля.</summary>
public sealed class RabbitMqFieldsConnectionSettings : IRabbitMqConnectionSettings
{
    public required string HostName { get; init; }
    public int? Port { get; init; }
    public required string UserName { get; init; }
    public required string Password { get; init; }
    public string VirtualHost { get; init; } = "/";
    public bool UseTls { get; init; }

    public ConnectionFactory CreateFactory()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(HostName);
        ArgumentException.ThrowIfNullOrWhiteSpace(UserName);
        ArgumentException.ThrowIfNullOrWhiteSpace(Password);
        ArgumentException.ThrowIfNullOrWhiteSpace(VirtualHost);
        if (Port is <= 0 or > 65535) throw new ArgumentOutOfRangeException(nameof(Port));
        var factory = new ConnectionFactory
        {
            HostName = HostName,
            Port = Port ?? (UseTls ? 5671 : 5672),
            UserName = UserName,
            Password = Password,
            VirtualHost = VirtualHost
        };
        factory.Ssl.Enabled = UseTls;
        if (UseTls)
        {
            factory.Ssl.ServerName = HostName;
            factory.Ssl.AcceptablePolicyErrors = SslPolicyErrors.None;
        }
        return factory;
    }
}
