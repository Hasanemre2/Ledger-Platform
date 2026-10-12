# 🏦 LedgerPlatform

Gerçek zamanlı, yüksek hacimli finansal işlem akışlarını (High-Throughput Stream) işleyen, **Oracle ODP.NET Array Binding** ile toplu veritabanı kaydı yapan ve **anlık gün sonu mutabakatı (Continuous EOD Reconciliation)** sağlayan çekirdek bankacılık (Core-Banking) defter platformudur.

---

## 🏗️ Mimari ve Çalışma Mantığı

```text
 ┌──────────────────────┐
 │   Ledger.Producer    │  (Saniyede ~15.000 - 25.000 Finansal İşlem)
 └──────────┬───────────┘
            │  Kafka Producer (Key = CustomerId)
            ▼
 ┌──────────────────────────────────────────────────────────────┐
 │               Apache Kafka (KRaft Modu)                      │
 │           Topic: ledger-transactions (6 Partition)           │
 └──────────┬───────────────────────────────────────┬───────────┘
            │                                       │
            ▼ (Consumer Group)                      ▼ (Consumer Group)
 ┌──────────────────────┐                ┌──────────────────────┐
 │ Ledger.Consumer (C1) │                │ Ledger.Consumer (C2) │
 └──────────┬───────────┘                └──────────┬───────────┘
            │  15.000'lik Paketler                  │
            │  RAM İçi Gruplama & Sıralama          │
            │  Oracle Array Binding Batch           │
            ▼                                       ▼
 ┌──────────────────────────────────────────────────────────────┐
 │                    Oracle Database XE 21c                    │
 │    ACCOUNTS  |  ACCOUNT_TRANSACTIONS  |  CONSUMER_METRICS    │
 └──────────────────────────────┬───────────────────────────────┘
                                │
                                ▼
 ┌──────────────────────────────────────────────────────────────┐
 │              Ledger.Dashboard (Web UI & API)                 │
 │            http://localhost:5000 (Canlı Mutabakat)           │
 └──────────────────────────────────────────────────────────────┘
```

---

## 📦 Proje Modülleri

| Modül | Açıklama |
| :--- | :--- |
| **`Ledger.Common`** | Ortak veri modelleri (`TransactionEvent`, `TransactionRecord`, `CustomerState`). |
| **`Ledger.Producer`** | Rastgele müşteriler için Kafka'ya yüksek hızda `CREDIT` ve `DEBIT` finansal işlem akışı gönderir. |
| **`Ledger.Consumer`** | Kafka'dan paketler halinde veri çeker, RAM'de netleştirir ve Oracle veritabanına Array Binding ile toplu yazar. |
| **`Ledger.Dashboard`** | Gerçek zamanlı kasa mutabakatını, aktif consumer'ları ve canlı müşteri ekstrelerini gösteren Web paneli. |

---

## ⚡ Temel Teknik Özellikler

- **Oracle ODP.NET Array Binding (`ArrayBindCount`):** 15.000 işlem tek bir veritabanı turunda (single round-trip) C-seviyesi bellek bloklarıyla işlenir.
- **Deadlock (ORA-00060) Önleme:** Batch içindeki işlemler müşteri ID'lerine göre sıralanarak veritabanı kilit sırası garantiye alınır.
- **Kafka Partition Affinity:** Mesaj anahtarı (`Key = CustomerId`) sayesinde aynı müşterinin tüm işlemleri aynı partition'a gider ve sıra garantili (FIFO) işlenir.
- **Hızlı Commit:** `COMMIT_WRITE = 'BATCH,NOWAIT'` ile disk redo log beklemesi bypass edilir.

---

## 🚀 Kurulum ve Çalıştırma

### 1. Gereksinimler
- .NET 9.0 SDK
- Docker & Docker Compose

### 2. Ortam Değişkenleri (.env)
Şablon dosyayı kopyalayarak yerel ortam dosyanızı oluşturun:
```powershell
Copy-Item .env.example .env
```

### 3. Altyapıyı Başlatma
Kafka, Kafka UI ve Oracle XE veritabanını Docker ile ayağa kaldırın:
```bash
docker compose up -d
```
> Oracle konteyneri ilk açılışta `init-scripts/01_init_schema.sql` scriptiyle gerekli tabloları ve 3.000 müşteri kaydını otomatik oluşturur.

### 4. Uygulamaları Başlatma

**1. Dashboard (Web Arayüzü):**
```powershell
dotnet run --project Ledger.Dashboard
```
Tarayıcıdan **http://localhost:5000** adresine gidin.

**2. Consumer (Veritabanı Tüketici):**
```powershell
dotnet run --project Ledger.Consumer -- Consumer-1
```
*(Yatay büyüme ve yük paylaşımı testi için ayrı bir terminalde `dotnet run --project Ledger.Consumer -- Consumer-2` çalıştırabilirsiniz.)*

**3. Producer (İşlem Üretici):**
```powershell
dotnet run --project Ledger.Producer
```
Konsolda **ENTER** tuşuna basarak akışı başlatın.

---

## 🌐 API Uç Noktaları

| Metot | Uç Nokta | Açıklama |
| :--- | :--- | :--- |
| `GET` | `/api/live-reconciliation` | Genel kasa mutabakatı, fark tutarı ve aktif consumer metrikleri |
| `GET` | `/api/random-customer` | İşlem görmüş rastgele bir müşterinin adı |
| `GET` | `/api/customer-statement?customerQuery={id}` | Müşteri bakiyesi ve son 30 işlem hareket dökümü |
