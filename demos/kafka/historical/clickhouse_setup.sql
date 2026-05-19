-- =============================================================================
-- ClickHouse Schema for Fraud Detection Demo (Kafka Pipeline)
-- =============================================================================
-- This schema demonstrates the additional infrastructure required when using
-- Kafka for streaming fraud detection:
--
--   1. A Kafka Engine table to ingest raw transactions from Kafka
--   2. A MergeTree table for persistent storage
--   3. A Materialized View to bridge the two (the ETL glue)
--   4. Separate tables for historical data, customer profiles, fraud rings
--
-- In the VAST architecture, ALL of this lives on one platform — no separate
-- OLAP database, no ETL pipeline, no schema duplication.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Transaction table ingesting from Kafka (Kafka Engine)
-- This is the "ETL bridge" — ClickHouse polls Kafka for new messages.
-- If this consumer falls behind or crashes, historical queries return stale data.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fraud_transactions_queue (
    transaction_id String,
    timestamp DateTime64(3),
    card_id String,
    customer_id String,
    merchant_id String,
    merchant_category String,
    amount Float64,
    currency String,
    location_lat Float64,
    location_lon Float64,
    location_city String,
    device_fingerprint String,
    channel String,
    is_fraud UInt8
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'fraud.transactions.raw',
         kafka_group_name = 'clickhouse-consumer',
         kafka_format = 'JSONEachRow';

-- ---------------------------------------------------------------------------
-- Materialized view for persistent storage (MergeTree)
-- This is the "real" table that analytical queries run against.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fraud_transactions (
    transaction_id String,
    timestamp DateTime64(3),
    card_id String,
    customer_id String,
    merchant_id String,
    merchant_category String,
    amount Float64,
    currency String,
    location_lat Float64,
    location_lon Float64,
    location_city String,
    device_fingerprint String,
    channel String,
    is_fraud UInt8
) ENGINE = MergeTree()
ORDER BY (card_id, timestamp);

CREATE MATERIALIZED VIEW IF NOT EXISTS fraud_transactions_mv TO fraud_transactions AS
SELECT * FROM fraud_transactions_queue;

-- ---------------------------------------------------------------------------
-- Historical data table (pre-loaded with seed data)
-- In production this would be backfilled from a data lake or batch ETL job —
-- yet another pipeline to build and maintain.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fraud_transactions_history (
    transaction_id String,
    timestamp DateTime64(3),
    card_id String,
    customer_id String,
    merchant_id String,
    merchant_category String,
    amount Float64,
    currency String,
    location_lat Float64,
    location_lon Float64,
    location_city String,
    device_fingerprint String,
    channel String,
    is_fraud UInt8
) ENGINE = MergeTree()
ORDER BY (card_id, timestamp);

-- ---------------------------------------------------------------------------
-- Customer profiles — typically synced from an OLTP database via CDC
-- (yet another pipeline).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS customer_profiles (
    customer_id String,
    card_id String,
    avg_spend Float64,
    home_city String,
    home_lat Float64,
    home_lon Float64
) ENGINE = MergeTree()
ORDER BY customer_id;

-- ---------------------------------------------------------------------------
-- Fraud ring merchants — maintained by the fraud operations team,
-- loaded via batch job or manual INSERT.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fraud_ring_merchants (
    merchant_id String,
    risk_level String,
    first_flagged DateTime64(3)
) ENGINE = MergeTree()
ORDER BY merchant_id;
