-- ===================================================================
-- Core-Banking Ledger Platform - Veritabanı Şeması ve Başlangıç Verisi
-- Oracle XE 21c Container Init Script (XEPDB1 / LEDGER_USER)
-- ===================================================================

-- 1. Müşteri Hesapları Tablosu (ACCOUNTS)
BEGIN
    EXECUTE IMMEDIATE '
        CREATE TABLE ACCOUNTS (
            CUSTOMER_ID VARCHAR2(50) PRIMARY KEY,
            CUSTOMER_NAME VARCHAR2(100) NOT NULL,
            INITIAL_BALANCE NUMBER(18,2) DEFAULT 0 NOT NULL,
            CURRENT_BALANCE NUMBER(18,2) DEFAULT 0 NOT NULL,
            TOTAL_TX_COUNT NUMBER(10) DEFAULT 0 NOT NULL
        )
    ';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -955 THEN RAISE; END IF; -- ORA-00955: tablo zaten varsa yut
END;
/

-- 2. Hesap İşlemleri Tablosu (ACCOUNT_TRANSACTIONS)
BEGIN
    EXECUTE IMMEDIATE '
        CREATE TABLE ACCOUNT_TRANSACTIONS (
            TX_ID VARCHAR2(64) PRIMARY KEY,
            CUSTOMER_ID VARCHAR2(50) NOT NULL,
            AMOUNT NUMBER(18,2) NOT NULL,
            TRANSACTION_TYPE VARCHAR2(10) NOT NULL,
            STATUS VARCHAR2(20) NOT NULL,
            BUSINESS_DATE DATE NOT NULL,
            DESCRIPTION VARCHAR2(255),
            CREATED_AT TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL
        )
    ';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -955 THEN RAISE; END IF;
END;
/

-- Hızlı sorgular ve canlı ekstre için indeksler
BEGIN
    EXECUTE IMMEDIATE 'CREATE INDEX IDX_TX_CUST_DATE ON ACCOUNT_TRANSACTIONS(CUSTOMER_ID, CREATED_AT DESC)';
EXCEPTION
    WHEN OTHERS THEN IF SQLCODE != -955 AND SQLCODE != -1408 THEN RAISE; END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'CREATE INDEX IDX_TX_STATUS_TYPE ON ACCOUNT_TRANSACTIONS(STATUS, TRANSACTION_TYPE)';
EXCEPTION
    WHEN OTHERS THEN IF SQLCODE != -955 AND SQLCODE != -1408 THEN RAISE; END IF;
END;
/

-- 3. Consumer Metrikleri Tablosu (CONSUMER_METRICS)
BEGIN
    EXECUTE IMMEDIATE '
        CREATE TABLE CONSUMER_METRICS (
            CONSUMER_ID VARCHAR2(50) PRIMARY KEY,
            ASSIGNED_PARTITIONS VARCHAR2(100),
            PROCESSED_COUNT NUMBER(18) DEFAULT 0 NOT NULL,
            LAST_UPDATED TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL
        )
    ';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -955 THEN RAISE; END IF;
END;
/

-- 4. Başlangıç Müşteri Hesapları (Seed Data: 3000 Müşteri)
-- Producer uygulaması CUST_00001 - CUST_03000 aralığında işlem üretir
DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM ACCOUNTS;
    IF v_count = 0 THEN
        FOR i IN 1..3000 LOOP
            INSERT INTO ACCOUNTS (CUSTOMER_ID, CUSTOMER_NAME, INITIAL_BALANCE, CURRENT_BALANCE, TOTAL_TX_COUNT)
            VALUES (
                'CUST_' || LPAD(i, 5, '0'),
                'Müşteri ' || LPAD(i, 5, '0'),
                150000.00,
                150000.00,
                0
            );
        END LOOP;
        COMMIT;
    END IF;
END;
/
