using Confluent.Kafka;

namespace SeedWork.Messaging.Kafka;

/// <summary>Адрес брокеров и параметры защиты одного подключения Kafka.</summary>
public sealed class KafkaConnectionSettings
{
    public required string BootstrapServers { get; init; }
    public SecurityProtocol SecurityProtocol { get; init; } = Confluent.Kafka.SecurityProtocol.Plaintext;
    public SaslMechanism? SaslMechanism { get; init; }
    public string? SaslUsername { get; init; }
    public string? SaslPassword { get; init; }

    public ClientConfig CreateClientConfig()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(BootstrapServers);
        if (!Enum.IsDefined(SecurityProtocol)) throw new ArgumentOutOfRangeException(nameof(SecurityProtocol));
        var sasl = SecurityProtocol is Confluent.Kafka.SecurityProtocol.SaslPlaintext or Confluent.Kafka.SecurityProtocol.SaslSsl;
        if (sasl && (SaslMechanism is null || !Enum.IsDefined(SaslMechanism.Value) ||
            string.IsNullOrWhiteSpace(SaslUsername) || string.IsNullOrWhiteSpace(SaslPassword)))
            throw new ArgumentException("SASL requires a mechanism, username and password.");
        if (!sasl && (SaslMechanism is not null || SaslUsername is not null || SaslPassword is not null))
            throw new ArgumentException("SASL settings require a SASL security protocol.");
        var config = new ClientConfig { BootstrapServers = BootstrapServers, SecurityProtocol = SecurityProtocol };
        if (sasl)
        {
            config.SaslMechanism = SaslMechanism;
            config.SaslUsername = SaslUsername;
            config.SaslPassword = SaslPassword;
        }
        return config;
    }
}
