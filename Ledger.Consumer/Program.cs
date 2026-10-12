using Confluent.Kafka;
using Ledger.Common;
using Oracle.ManagedDataAccess.Client;
using System.Data;
using System.Diagnostics;
using System.Text.Json;

string consumerInstanceId = args.Length > 0 ? args[0] : $"Consumer-{Guid.NewGuid().ToString()[..4]}";

Console.Clear();
Console.WriteLine("=================================================");
Console.WriteLine($" ⚡ ARRAY BINDING CONSUMER: {consumerInstanceId}");
Console.WriteLine("=================================================");

// Konfigürasyonları ortam değişkenlerinden alalım (Böylece şifreler kodda sabit kalmaz)
string oracleConnString = Environment.GetEnvironmentVariable("ORACLE_CONN_STRING") 
    ?? "User Id=LEDGER_USER;Password=LedgerPass2024;Data Source=127.0.0.1:1522/XEPDB1;Min Pool Size=4;Max Pool Size=100;Connection Timeout=60;Pooling=true;";

string bootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") 
    ?? "127.0.0.1:9094";
string groupId = Environment.GetEnvironmentVariable("KAFKA_GROUP_ID") 
    ?? "ledger-arraybinding-group";
string topicName = Environment.GetEnvironmentVariable("KAFKA_TOPIC") 
    ?? "ledger-transactions";

string? saslUsername = Environment.GetEnvironmentVariable("KAFKA_SASL_USERNAME");
string? saslPassword = Environment.GetEnvironmentVariable("KAFKA_SASL_PASSWORD");
string? securityProtocolStr = Environment.GetEnvironmentVariable("KAFKA_SECURITY_PROTOCOL");
string? saslMechanismStr = Environment.GetEnvironmentVariable("KAFKA_SASL_MECHANISM");

var conf = new ConsumerConfig
{
    GroupId = groupId,
    BootstrapServers = bootstrapServers,
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false,
    FetchMinBytes = 1024 * 1024,
    FetchWaitMaxMs = 20,
    SessionTimeoutMs = 15000,
    MaxPollIntervalMs = 300000
};

// SASL / Bulut Kafka kimlik doğrulaması tanımlanmışsa ayarları uygula
if (!string.IsNullOrWhiteSpace(saslUsername) && !string.IsNullOrWhiteSpace(saslPassword))
{
    conf.SaslUsername = saslUsername;
    conf.SaslPassword = saslPassword;
    conf.SecurityProtocol = Enum.TryParse<SecurityProtocol>(securityProtocolStr, true, out var sp) ? sp : SecurityProtocol.SaslSsl;
    conf.SaslMechanism = Enum.TryParse<SaslMechanism>(saslMechanismStr, true, out var sm) ? sm : SaslMechanism.Plain;
}

var jsonOptions = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
string assignedPartitionsStr = "Bekleniyor...";

using var conn = new OracleConnection(oracleConnString);
await conn.OpenAsync();

// Disk log beklemesini bypass et (commit_write)
using (var cmdInit = conn.CreateCommand())
{
    cmdInit.CommandText = "ALTER SESSION SET COMMIT_WRITE = 'BATCH,NOWAIT'";
    await cmdInit.ExecuteNonQueryAsync();
}

var sw = Stopwatch.StartNew();
long instanceProcessedCount = 0;

using var consumer = new ConsumerBuilder<string, string>(conf)
    .SetPartitionsAssignedHandler((c, partitions) =>
    {
        var partList = partitions.Select(p => p.Partition.Value).OrderBy(p => p).ToList();
        assignedPartitionsStr = string.Join(", ", partList);
        Console.WriteLine($"\n✓ [Rebalance] Atanan Partition'lar: {assignedPartitionsStr}\n");
        UpdateConsumerMetric(conn, consumerInstanceId, assignedPartitionsStr, instanceProcessedCount);
    })
    .Build();

consumer.Subscribe(topicName);

const int BATCH_SIZE = 15_000; 
var rawEvents = new List<TransactionEvent>(BATCH_SIZE);

Console.WriteLine("🚀 [Array Binding Engine] Dinleme başlatıldı...\n");

while (true)
{
    try
    {
        var cr = consumer.Consume(TimeSpan.FromMilliseconds(5));

        if (cr != null)
        {
            var tx = JsonSerializer.Deserialize<TransactionEvent>(cr.Message.Value, jsonOptions);
            if (tx != null) rawEvents.Add(tx);
        }

        if (rawEvents.Count >= BATCH_SIZE || (rawEvents.Count > 0 && cr == null))
        {
            var swDb = Stopwatch.StartNew();
            await ProcessArrayBindingBatchAsync(conn, rawEvents);
            swDb.Stop();

            consumer.Commit();
            instanceProcessedCount += rawEvents.Count;

            try
            {
                UpdateConsumerMetric(conn, consumerInstanceId, assignedPartitionsStr, instanceProcessedCount);
            }
            catch { }

            double speed = instanceProcessedCount / sw.Elapsed.TotalSeconds;
            Console.WriteLine($"[{consumerInstanceId}] İşlenen: {instanceProcessedCount,7:N0} | Paket: {rawEvents.Count,5} | DB Süre: {swDb.ElapsedMilliseconds,4}ms | Hız: {speed,6:N0} tx/sn");

            rawEvents.Clear();
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[Hata]: {ex.Message}");
        await Task.Delay(20);
    }
}

static async Task ProcessArrayBindingBatchAsync(OracleConnection conn, List<TransactionEvent> events)
{
    // 1. RAM üzerinde müşteri bazlı net deltaları hesapla (Grouping)
    var custAgg = new Dictionary<string, (decimal NetDelta, int TxCount)>();
    foreach (var ev in events)
    {
        decimal delta = (ev.TransactionType == "CREDIT") ? ev.Amount : -ev.Amount;
        if (!custAgg.TryGetValue(ev.CustomerId, out var cur))
            custAgg[ev.CustomerId] = (delta, 1);
        else
            custAgg[ev.CustomerId] = (cur.NetDelta + delta, cur.TxCount + 1);
    }

    // 2. Deadlock önlemek için müşteri ID'lerine göre sırala (ORA-00060 engelleme)
    var aggList = custAgg.OrderBy(x => x.Key).ToList();
    int aggCount = aggList.Count;
    
    var uCids = new string[aggCount];
    var uDeltas = new decimal[aggCount];
    var uCounts = new int[aggCount];

    for (int i = 0; i < aggCount; i++)
    {
        uCids[i] = aggList[i].Key;
        uDeltas[i] = aggList[i].Value.NetDelta;
        uCounts[i] = aggList[i].Value.TxCount;
    }

    using var dbTx = conn.BeginTransaction();
    try
    {
        // 3. ARRAY BINDING İLE ACCOUNTS TABLOSUNU ANLIK GÜNCELLE
        using (var cmdUpd = conn.CreateCommand())
        {
            cmdUpd.Transaction = dbTx;
            cmdUpd.BindByName = true;
            cmdUpd.ArrayBindCount = aggCount;
            cmdUpd.CommandText = @"
                UPDATE ACCOUNTS 
                SET CURRENT_BALANCE = CURRENT_BALANCE + :netDelta, 
                    TOTAL_TX_COUNT = TOTAL_TX_COUNT + :txInc 
                WHERE CUSTOMER_ID = :cid";

            cmdUpd.Parameters.Add("netDelta", OracleDbType.Decimal, uDeltas, ParameterDirection.Input);
            cmdUpd.Parameters.Add("txInc", OracleDbType.Int32, uCounts, ParameterDirection.Input);
            cmdUpd.Parameters.Add("cid", OracleDbType.Varchar2, uCids, ParameterDirection.Input);
            
            await cmdUpd.ExecuteNonQueryAsync();
        }

        // 4. ACCOUNT_TRANSACTIONS TABLOSUNA TOPLU INSERT (ARRAY BINDING)
        int total = events.Count;
        var txIds = new string[total];
        var cids = new string[total];
        var amts = new decimal[total];
        var types = new string[total];
        var dates = new DateTime[total];
        var descs = new string[total];

        for (int i = 0; i < total; i++)
        {
            txIds[i] = events[i].TxId;
            cids[i] = events[i].CustomerId;
            amts[i] = events[i].Amount;
            types[i] = events[i].TransactionType;
            dates[i] = DateTime.Parse(events[i].BusinessDate);
            descs[i] = events[i].Description ?? "-";
        }

        using (var cmdIns = conn.CreateCommand())
        {
            cmdIns.Transaction = dbTx;
            cmdIns.BindByName = true;
            cmdIns.ArrayBindCount = total;
            cmdIns.CommandText = @"
                INSERT INTO ACCOUNT_TRANSACTIONS 
                (TX_ID, CUSTOMER_ID, AMOUNT, TRANSACTION_TYPE, STATUS, BUSINESS_DATE, DESCRIPTION, CREATED_AT)
                VALUES (:txId, :cid, :amt, :tType, 'PROCESSED', :bDate, :dsc, SYSTIMESTAMP)";

            cmdIns.Parameters.Add("txId", OracleDbType.Varchar2, txIds, ParameterDirection.Input);
            cmdIns.Parameters.Add("cid", OracleDbType.Varchar2, cids, ParameterDirection.Input);
            cmdIns.Parameters.Add("amt", OracleDbType.Decimal, amts, ParameterDirection.Input);
            cmdIns.Parameters.Add("tType", OracleDbType.Varchar2, types, ParameterDirection.Input);
            cmdIns.Parameters.Add("bDate", OracleDbType.Date, dates, ParameterDirection.Input);
            cmdIns.Parameters.Add("dsc", OracleDbType.Varchar2, descs, ParameterDirection.Input);

            await cmdIns.ExecuteNonQueryAsync();
        }

        await dbTx.CommitAsync();
    }
    catch
    {
        await dbTx.RollbackAsync();
        throw;
    }
}

static void UpdateConsumerMetric(OracleConnection conn, string consumerId, string partitions, long count)
{
    using var cmd = conn.CreateCommand();
    cmd.CommandText = @"
        MERGE INTO CONSUMER_METRICS m
        USING (SELECT :id AS CONSUMER_ID, :parts AS ASSIGNED_PARTITIONS, :cnt AS PROCESSED_COUNT FROM DUAL) src
        ON (m.CONSUMER_ID = src.CONSUMER_ID)
        WHEN MATCHED THEN
            UPDATE SET m.ASSIGNED_PARTITIONS = src.ASSIGNED_PARTITIONS, 
                       m.PROCESSED_COUNT = src.PROCESSED_COUNT, 
                       m.LAST_UPDATED = SYSTIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (CONSUMER_ID, ASSIGNED_PARTITIONS, PROCESSED_COUNT, LAST_UPDATED)
            VALUES (src.CONSUMER_ID, src.ASSIGNED_PARTITIONS, src.PROCESSED_COUNT, SYSTIMESTAMP)";

    cmd.Parameters.Add(":id", OracleDbType.Varchar2, consumerId, ParameterDirection.Input);
    cmd.Parameters.Add(":parts", OracleDbType.Varchar2, partitions, ParameterDirection.Input);
    cmd.Parameters.Add(":cnt", OracleDbType.Int64, count, ParameterDirection.Input);
    cmd.ExecuteNonQuery();
}