using Oracle.ManagedDataAccess.Client;
using System.Data;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles();

// Oracle bağlantı dizesini appsettings, ortam değişkeni veya yerel varsayılandan alalım
string oracleConnString = builder.Configuration.GetConnectionString("OracleDb")
    ?? Environment.GetEnvironmentVariable("ORACLE_CONN_STRING")
    ?? "User Id=LEDGER_USER;Password=LedgerPass2024;Data Source=127.0.0.1:1522/XEPDB1;Pooling=true;";

// 1. Canlı Sürekli Mutabakat & Kasa Sayaçları
app.MapGet("/api/live-reconciliation", async () =>
{
    using var conn = new OracleConnection(oracleConnString);
    await conn.OpenAsync();

    long totalAccounts = 0;
    decimal openingBalance = 0;
    decimal actualBalance = 0;
    long totalTx = 0;
    long totalRejected = 0;
    decimal totalCredit = 0;
    decimal totalDebit = 0;

    using (var cmd = conn.CreateCommand())
    {
        cmd.CommandText = @"
            SELECT 
                (SELECT COUNT(*) FROM ACCOUNTS) AS TOTAL_ACCOUNTS,
                (SELECT NVL(SUM(INITIAL_BALANCE), 0) FROM ACCOUNTS) AS TOTAL_INITIAL_BAL,
                (SELECT NVL(SUM(CURRENT_BALANCE), 0) FROM ACCOUNTS) AS TOTAL_CURRENT_BAL,
                (SELECT COUNT(*) FROM ACCOUNT_TRANSACTIONS) AS TOTAL_TX,
                (SELECT COUNT(*) FROM ACCOUNT_TRANSACTIONS WHERE STATUS = 'REJECTED') AS TOTAL_REJECTED,
                (SELECT NVL(SUM(AMOUNT), 0) FROM ACCOUNT_TRANSACTIONS WHERE TRANSACTION_TYPE = 'CREDIT' AND STATUS = 'PROCESSED') AS TOTAL_CREDIT,
                (SELECT NVL(SUM(AMOUNT), 0) FROM ACCOUNT_TRANSACTIONS WHERE TRANSACTION_TYPE = 'DEBIT' AND STATUS = 'PROCESSED') AS TOTAL_DEBIT
            FROM DUAL";

        using var reader = await cmd.ExecuteReaderAsync();
        if (await reader.ReadAsync())
        {
            totalAccounts = reader.GetInt64(0);
            openingBalance = reader.GetDecimal(1);
            actualBalance = reader.GetDecimal(2);
            totalTx = reader.GetInt64(3);
            totalRejected = reader.GetInt64(4);
            totalCredit = reader.GetDecimal(5);
            totalDebit = reader.GetDecimal(6);
        }
    }

    decimal theoreticalBalance = openingBalance + totalCredit - totalDebit;
    decimal discrepancy = actualBalance - theoreticalBalance;

    var consumers = new List<object>();
    using (var cmdMetric = conn.CreateCommand())
    {
        cmdMetric.CommandText = "SELECT CONSUMER_ID, ASSIGNED_PARTITIONS, PROCESSED_COUNT FROM CONSUMER_METRICS ORDER BY CONSUMER_ID";
        using var reader = await cmdMetric.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            consumers.Add(new
            {
                consumerId = reader.GetString(0),
                partitions = reader.GetString(1),
                processed = reader.GetInt64(2)
            });
        }
    }

    return Results.Ok(new
    {
        totalAccounts,
        totalTx,
        totalRejected,
        openingBalance,
        totalCredit,
        totalDebit,
        theoreticalBalance,
        actualBalance,
        discrepancy,
        isReconciled = Math.Abs(discrepancy) < 0.01m,
        consumers
    });
});

// 2. Rastgele Müşteri Getir
app.MapGet("/api/random-customer", async () =>
{
    using var conn = new OracleConnection(oracleConnString);
    await conn.OpenAsync();

    using var cmd = conn.CreateCommand();
    cmd.CommandText = @"
        SELECT CUSTOMER_NAME 
        FROM (SELECT CUSTOMER_NAME FROM ACCOUNTS WHERE TOTAL_TX_COUNT > 0 ORDER BY DBMS_RANDOM.VALUE) 
        FETCH FIRST 1 ROWS ONLY";

    using var reader = await cmd.ExecuteReaderAsync();
    if (await reader.ReadAsync())
    {
        return Results.Ok(new { customerName = reader.GetString(0) });
    }

    return Results.NotFound();
});

// 3. Müşteri Bazlı Detay & Canlı Ekstre
app.MapGet("/api/customer-statement", async (string customerQuery) =>
{
    if (string.IsNullOrWhiteSpace(customerQuery)) return Results.BadRequest();

    using var conn = new OracleConnection(oracleConnString);
    await conn.OpenAsync();

    string? custId = null;
    string? custName = null;
    decimal initBal = 0;
    decimal curBal = 0;
    int txCount = 0;

    using (var cmdAcc = conn.CreateCommand())
    {
        cmdAcc.CommandText = @"
            SELECT CUSTOMER_ID, CUSTOMER_NAME, INITIAL_BALANCE, CURRENT_BALANCE, TOTAL_TX_COUNT 
            FROM ACCOUNTS 
            WHERE CUSTOMER_ID = UPPER(:q) OR UPPER(CUSTOMER_NAME) LIKE '%' || UPPER(:q) || '%'
            FETCH FIRST 1 ROWS ONLY";

        cmdAcc.Parameters.Add(":q", OracleDbType.Varchar2, customerQuery.Trim(), ParameterDirection.Input);

        using var reader = await cmdAcc.ExecuteReaderAsync();
        if (await reader.ReadAsync())
        {
            custId = reader.GetString(0);
            custName = reader.GetString(1);
            initBal = reader.GetDecimal(2);
            curBal = reader.GetDecimal(3);
            txCount = reader.GetInt32(4);
        }
        else return Results.NotFound();
    }

    decimal totalCredit = 0, totalDebit = 0;
    using (var cmdAgg = conn.CreateCommand())
    {
        cmdAgg.CommandText = @"
            SELECT 
                NVL(SUM(CASE WHEN TRANSACTION_TYPE = 'CREDIT' THEN AMOUNT ELSE 0 END), 0) AS TOTAL_CREDIT,
                NVL(SUM(CASE WHEN TRANSACTION_TYPE = 'DEBIT' THEN AMOUNT ELSE 0 END), 0) AS TOTAL_DEBIT
            FROM ACCOUNT_TRANSACTIONS
            WHERE CUSTOMER_ID = :cid AND STATUS = 'PROCESSED'";

        cmdAgg.Parameters.Add(":cid", OracleDbType.Varchar2, custId, ParameterDirection.Input);

        using var readerAgg = await cmdAgg.ExecuteReaderAsync();
        if (await readerAgg.ReadAsync())
        {
            totalCredit = readerAgg.GetDecimal(0);
            totalDebit = readerAgg.GetDecimal(1);
        }
    }

    decimal expectedBalance = initBal + totalCredit - totalDebit;
    bool isReconciled = Math.Abs(curBal - expectedBalance) < 0.01m;

    var transactions = new List<object>();
    using (var cmdTx = conn.CreateCommand())
    {
        cmdTx.CommandText = @"
            SELECT 
                TX_ID, AMOUNT, TRANSACTION_TYPE, STATUS, DESCRIPTION,
                TO_CHAR(CREATED_AT, 'YYYY-MM-DD HH24:MI:SS') AS CREATED_AT_STR
            FROM ACCOUNT_TRANSACTIONS 
            WHERE CUSTOMER_ID = :cid 
            ORDER BY CREATED_AT DESC
            FETCH FIRST 30 ROWS ONLY";

        cmdTx.Parameters.Add(":cid", OracleDbType.Varchar2, custId, ParameterDirection.Input);

        using var readerTx = await cmdTx.ExecuteReaderAsync();
        while (await readerTx.ReadAsync())
        {
            transactions.Add(new
            {
                txId = readerTx.GetString(0),
                amount = readerTx.GetDecimal(1),
                transactionType = readerTx.GetString(2),
                status = readerTx.GetString(3),
                description = readerTx.IsDBNull(4) ? "-" : readerTx.GetString(4),
                createdAt = readerTx.GetString(5)
            });
        }
    }

    return Results.Ok(new
    {
        customerId = custId,
        customerName = custName,
        initialBalance = initBal,
        currentBalance = curBal,
        totalCredit,
        totalDebit,
        expectedBalance,
        isReconciled,
        totalTxCount = txCount,
        transactions
    });
});

string dashboardUrl = Environment.GetEnvironmentVariable("DASHBOARD_URL") ?? "http://localhost:5000";

Console.WriteLine("=================================================");
Console.WriteLine($" 🌐 DASHBOARD: {dashboardUrl}");
Console.WriteLine("=================================================");

app.Run(dashboardUrl);