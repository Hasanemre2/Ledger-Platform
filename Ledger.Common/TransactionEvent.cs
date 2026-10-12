using System.Text.Json.Serialization;

namespace Ledger.Common;

public record TransactionEvent(
    [property: JsonPropertyName("TxId")] string TxId,
    [property: JsonPropertyName("CustomerId")] string CustomerId,
    [property: JsonPropertyName("CustomerName")] string CustomerName,
    [property: JsonPropertyName("Amount")] decimal Amount,
    [property: JsonPropertyName("TransactionType")] string TransactionType, // "CREDIT" veya "DEBIT"
    [property: JsonPropertyName("BusinessDate")] string BusinessDate,       // "YYYY-MM-DD"
    [property: JsonPropertyName("InitialBalance")] decimal InitialBalance,
    [property: JsonPropertyName("Description")] string Description,
    [property: JsonPropertyName("CreatedAt")] DateTime CreatedAt
);

public record TransactionRecord(
    string TxId,
    string CustomerId,
    decimal Amount,
    string TransactionType,
    string Status, // "PROCESSED" veya "REJECTED"
    string BusinessDate,
    string Description,
    DateTime CreatedAt
);

public class CustomerState
{
    public string Name { get; set; }
    public decimal CurrentBalance { get; set; }
    public int TxCount { get; set; }

    public CustomerState(string name, decimal currentBalance, int txCount)
    {
        Name = name;
        CurrentBalance = currentBalance;
        TxCount = txCount;
    }
}