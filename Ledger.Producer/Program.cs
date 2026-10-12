using Confluent.Kafka;
using Ledger.Common;
using System.Diagnostics;
using System.Text.Json;

Console.Clear();
Console.WriteLine("=================================================");
Console.WriteLine(" 🚀 HIGH-SPEED STREAM PRODUCER");
Console.WriteLine("=================================================");

string bootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "127.0.0.1:9094";
string topicName = Environment.GetEnvironmentVariable("KAFKA_TOPIC") ?? "ledger-transactions";
string businessDate = Environment.GetEnvironmentVariable("BUSINESS_DATE") ?? "2026-09-18";

string? saslUsername = Environment.GetEnvironmentVariable("KAFKA_SASL_USERNAME");
string? saslPassword = Environment.GetEnvironmentVariable("KAFKA_SASL_PASSWORD");
string? securityProtocolStr = Environment.GetEnvironmentVariable("KAFKA_SECURITY_PROTOCOL");
string? saslMechanismStr = Environment.GetEnvironmentVariable("KAFKA_SASL_MECHANISM");

var producerConfig = new ProducerConfig
{
    BootstrapServers = bootstrapServers,
    LingerMs = 5,
    BatchSize = 512 * 1024,
    CompressionType = CompressionType.Lz4,
    Acks = Acks.Leader,
    QueueBufferingMaxMessages = 2_000_000
};

// SASL / Bulut Kafka kimlik doğrulaması tanımlanmışsa ayarları uygula
if (!string.IsNullOrWhiteSpace(saslUsername) && !string.IsNullOrWhiteSpace(saslPassword))
{
    producerConfig.SaslUsername = saslUsername;
    producerConfig.SaslPassword = saslPassword;
    producerConfig.SecurityProtocol = Enum.TryParse<SecurityProtocol>(securityProtocolStr, true, out var sp) ? sp : SecurityProtocol.SaslSsl;
    producerConfig.SaslMechanism = Enum.TryParse<SaslMechanism>(saslMechanismStr, true, out var sm) ? sm : SaslMechanism.Plain;
}

var debitDescs = new[] { "Market Harcamasi", "Fatura Odeme", "Restoran", "E-Ticaret", "Benzin", "Giyim" };
var creditDescs = new[] { "Maas Yatirma", "EFT / Havale Geliri", "Nakit Yatirma", "Iade Tutari" };
var random = new Random(42);

using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

Console.WriteLine("\n[Hazır] Akışı başlatmak için ENTER'a basın...");
Console.ReadLine();

long totalSent = 0;
var sw = Stopwatch.StartNew();

while (true)
{
    for (int k = 0; k < 250; k++)
    {
        totalSent++;
        string randomCustId = $"CUST_{random.Next(1, 3001):D5}";
        bool isCredit = (random.Next(10) < 4); 
        decimal amount = random.Next(100, 2500);

        var txEvent = new TransactionEvent(
            TxId: Guid.NewGuid().ToString("N"),
            CustomerId: randomCustId,
            CustomerName: "Dinamik Musteri",
            Amount: amount,
            TransactionType: isCredit ? "CREDIT" : "DEBIT",
            BusinessDate: businessDate,
            InitialBalance: 150000.00m,
            Description: isCredit ? creditDescs[random.Next(creditDescs.Length)] : debitDescs[random.Next(debitDescs.Length)],
            CreatedAt: DateTime.Now
        );

        producer.Produce(topicName, new Message<string, string>
        {
            Key = txEvent.CustomerId, 
            Value = JsonSerializer.Serialize(txEvent)
        });
    }

    if (totalSent % 10_000 == 0)
    {
        double speed = totalSent / sw.Elapsed.TotalSeconds;
        Console.WriteLine($"[Producer] Gönderilen: {totalSent,7:N0} | Hız: {speed,5:N0} msg/sn");
    }

    Thread.Sleep(1); 
}