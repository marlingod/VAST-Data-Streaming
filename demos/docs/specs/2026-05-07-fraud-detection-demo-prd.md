# PRD: Financial Fraud Detection — VAST Event Broker vs Kafka Demo

**Date**: 2026-05-07
**Author**: VAST Solutions Engineering
**Status**: Draft
**Version**: 1.0

---

## 1. Purpose

Build a live, side-by-side financial fraud detection demo that showcases the VAST Event Broker's advantages over Apache Kafka with Tiered Storage. The demo uses **identical Kafka producer code** to feed transactions into both systems, then compares real-time scoring latency, historical query performance, architectural complexity, and AI-native investigation capabilities.

The demo serves two audiences:
- **Prospects/customers** in financial services evaluating VAST for streaming infrastructure
- **VAST Solutions Engineers** who need a repeatable, well-documented demo they can run in the field

---

## 2. Background

Financial fraud detection is a high-value streaming use case where every millisecond matters. Traditional Kafka-based pipelines require 5-7 separate systems (Kafka, ZooKeeper, Schema Registry, Flink, analytics DB, ML serving, audit store) and suffer from cold-read latency when correlating live transactions with historical patterns.

The VAST Event Broker eliminates this complexity with a unified platform: Kafka API-compatible ingestion, topics-as-tables for instant SQL, DataEngine for serverless scoring, and AgentEngine for AI-powered investigation — all in one system.

See `demos/docs/research.md` for full technical research and source references.

---

## 3. Goals

| Goal | Success Metric |
|------|----------------|
| Prove Kafka API compatibility | Same `confluent-kafka` producer code works against both backends with zero changes |
| Show latency advantage | VAST sub-ms detection vs Kafka low-ms detection, visible on live dashboard |
| Show historical query advantage | VAST instant SQL on topics-as-tables vs Kafka cold-read delay from tiered storage (seconds) |
| Show architectural simplicity | 1 VAST platform vs 6 Kafka ecosystem components, visualized in dashboard |
| Show AI-native capabilities | VAST Deep Dive Agent investigates flagged transactions; Kafka side has no equivalent |
| Repeatable by any SE | Setup in under 30 minutes, demo runs in 15 minutes, documented runbook |

---

## 4. Non-Goals

- Production-grade fraud detection accuracy (this is a demo, not a production system)
- Benchmarking at 136M msgs/sec scale (demo runs at 1K-10K TPS)
- Building a full Kafka Connect pipeline or production Flink cluster
- Mobile or web-facing fraud alerting UI
- PCI-DSS compliant data handling (synthetic data only)

---

## 5. Architecture

### 5.1 Overview

```
                    +----------------------------------+
                    |     Transaction Generator         |
                    |   (confluent-kafka Python)        |
                    |   --target vast | kafka            |
                    +--------+----------------+--------+
                             |                |
               +-------------+                +---------------+
               v                                              v
  +------------------------+                  +-----------------------------+
  |  VAST Event Broker     |                  |  Kafka (Docker)             |
  |  (live cluster)        |                  |  + ZooKeeper                |
  |  Kafka API-compatible  |                  |  + Schema Registry          |
  |                        |                  |  + Tiered Storage (MinIO)   |
  +----------+-------------+                  +-------------+---------------+
             |                                              |
             v                                              v
  +------------------------+                  +------------------------+
  | Blob Expansion         |                  | Faust Stream Processor |
  | (JSON -> structured    |                  | (Docker container)     |
  |  columns, automatic)   |                  +-------------+----------+
  +----------+-------------+                                |
             |                                              v
             v                                +------------------------+
  +---------------------------+               | ClickHouse (Docker)    |
  | VAST DataBase             |               | + ETL from Kafka       |
  | - SQL on expanded tables  |               | + Historical queries   |
  | - Topics-as-tables        |               +-----------+------------+
  | - Historical JOINs        |                           |
  +----------+----------------+                           |
             |                                            |
             v                                            |
  +--------------------+                                  |
  | VAST DataEngine    |                                  |
  | (serverless fraud  |                                  |
  |  scoring)          |                                  |
  +----------+---------+                                  |
             |                                            |
             v                                            |
  +---------------------------+                           |
  | VAST AgentEngine          |                           |
  | - Deep Dive Agent (RAG)   |                           |
  | - Record Keeper (audit)   |                           |
  +----------+----------------+                           |
             |                                            |
             v                                            v
  +------------------------------------------------------------+
  |            Comparison Dashboard (Streamlit)                  |
  |  - Latency: VAST sub-ms vs Kafka ms+                        |
  |  - Throughput: msgs/sec side-by-side                         |
  |  - Architecture: 1 system vs 6 systems                      |
  |  - Query demo: SQL on topics-as-tables                       |
  |  - Detection rate + alert timeline                           |
  +------------------------------------------------------------+
```

**Blob Expansion** is the key mechanism that enables topics-as-tables. When JSON messages land in the Event Broker, Blob Expansion automatically extracts JSON fields into structured columnar tables — making them instantly queryable with SQL. This is configured once via the `vastdb` SDK or VAST Admin UI, and runs continuously on all new messages. No ETL pipeline, no Flink, no ClickHouse — just structured data.

### 5.2 Design Decisions

| Decision | Rationale |
|----------|-----------|
| Single transaction generator with `--bootstrap-servers` flag | Proves Kafka API compatibility — same code, zero changes |
| `confluent-kafka` Python library (not `kafka-python`) | Production-grade, maintained by Confluent, supports Avro serialization |
| Faust for Kafka stream processing (not Flink) | Python-native, lighter Docker footprint, easier for SEs to understand and modify |
| ClickHouse for Kafka analytics (not Druid/DuckDB) | Fast analytical queries, simple Docker setup, realistic production choice |
| MinIO for Kafka tiered storage (not real S3) | Self-contained demo, no cloud account needed, S3-compatible API |
| Streamlit for dashboard (not Grafana) | Custom comparison layout, Python-native, fast to build, easy to modify |
| Avro schemas (not JSON/Protobuf) | Industry standard for financial streaming, works with Schema Registry |

---

## 6. Data Model

### 6.1 Transaction Schema (Avro)

```json
{
  "namespace": "com.vast.demo.fraud",
  "type": "record",
  "name": "Transaction",
  "fields": [
    {"name": "transaction_id", "type": "string"},
    {"name": "timestamp", "type": "string"},
    {"name": "card_id", "type": "string"},
    {"name": "customer_id", "type": "string"},
    {"name": "merchant_id", "type": "string"},
    {"name": "merchant_category", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "currency", "type": "string"},
    {"name": "location_lat", "type": "double"},
    {"name": "location_lon", "type": "double"},
    {"name": "location_city", "type": "string"},
    {"name": "device_fingerprint", "type": "string"},
    {"name": "channel", "type": {"type": "enum", "name": "Channel", "symbols": ["online", "pos", "atm", "mobile"]}},
    {"name": "is_fraud", "type": "boolean", "default": false}
  ]
}
```

### 6.2 Topics

| Topic | Purpose | Producers | Consumers |
|-------|---------|-----------|-----------|
| `fraud.transactions.raw` | Incoming transactions | Generator | Fraud scorer (both backends) |
| `fraud.transactions.scored` | Transactions + risk score | Fraud scorer | Dashboard, alert evaluator |
| `fraud.alerts` | High-risk flagged transactions | Alert evaluator | Dashboard, AI agents (VAST) |
| `fraud.metrics` | Pipeline latency and throughput | Both pipelines | Dashboard |

### 6.3 Fraud Patterns

| Pattern | Injection Rate | Parameters | Detection Method |
|---------|---------------|------------|-----------------|
| **Velocity attack** | 5% of traffic | 10+ txns from same card in 60s | Windowed count aggregation |
| **Geographic impossibility** | 3% of traffic | Two cities > 500km apart within 5 min | Distance/time calculation |
| **Amount anomaly** | 4% of traffic | Transaction > 10x customer's 90-day avg | Historical JOIN + std deviation |
| **Card testing** | 3% of traffic | 5+ transactions of $1-2 in 30s | Pattern match on amount + velocity |
| **Known fraud ring** | 2% of traffic | Merchant ID in fraud ring lookup table | Historical pattern lookup |
| **Legitimate** | 83% of traffic | Normal distribution of amounts/locations | Should NOT trigger alerts |

---

## 7. Components

### 7.1 Transaction Generator

**Purpose**: Produce synthetic financial transactions via Kafka protocol to both backends.

**Behavior**:
- Uses `confluent-kafka` Producer with configurable `bootstrap.servers`
- Generates realistic transactions using `faker` for names, locations, amounts
- Injects fraud patterns at configurable rates (see 6.3)
- Maintains per-customer state for consistent behavioral profiles (avg spend, home location, known devices)
- Publishes to `fraud.transactions.raw` topic
- Emits throughput metrics to `fraud.metrics` topic

**Configuration**:
- `--bootstrap-servers`: VAST Event Broker or Kafka broker address
- `--tps`: Transactions per second (default: 1000)
- `--duration`: Run duration in seconds (default: 300)
- `--fraud-rate`: Overall fraud injection rate (default: 0.17)
- `--customers`: Number of synthetic customers (default: 10000)
- `--seed`: Random seed for reproducibility

### 7.2 VAST Blob Expansion (Topics-as-Tables)

**Purpose**: Automatically extract JSON fields from raw Kafka topic messages into structured, SQL-queryable columnar tables. This is the mechanism that enables "topics-as-tables" — the core VAST differentiator.

**How it works**:
1. Messages arrive in the Event Broker as JSON blobs (single `value` column)
2. Blob Expansion is configured once per topic — mapping JSON fields to typed columns
3. VAST continuously extracts new messages into a target table in a separate schema
4. The target table is instantly queryable with SQL via `vastdb` SDK, Trino, or Spark

**Configuration** (via `vastdb` SDK or VAST Admin UI):
- **Source**: Topic table in the Kafka schema (e.g., `yg-bucket.kafka_topics.fraud.transactions.raw`)
- **Source column**: `value` (the raw JSON blob)
- **Target schema**: A separate schema in the same bucket (e.g., `fraud_detection`)
- **Target table**: Structured table with typed columns (e.g., `transactions`)
- **Format**: JSON (Protobuf support coming)

**Expansion schema for `fraud.transactions.raw`**:
```python
expansion_schema = pa.schema([
    pa.field("transaction_id", pa.string()),
    pa.field("timestamp", pa.string()),
    pa.field("card_id", pa.string()),
    pa.field("customer_id", pa.string()),
    pa.field("merchant_id", pa.string()),
    pa.field("merchant_category", pa.string()),
    pa.field("amount", pa.float64()),
    pa.field("currency", pa.string()),
    pa.field("location_lat", pa.float64()),
    pa.field("location_lon", pa.float64()),
    pa.field("location_city", pa.string()),
    pa.field("device_fingerprint", pa.string()),
    pa.field("channel", pa.string()),
    pa.field("is_fraud", pa.bool_()),
])
```

**Setup script**: `demos/vast/blob_expansion_setup.py`

**Demo talking point**: "Kafka requires a separate ETL pipeline — Kafka Connect or Flink — to get data into a queryable format. VAST does it automatically with Blob Expansion. One-time config, zero ongoing maintenance."

### 7.3 VAST Fraud Scorer (DataEngine Function)

**Purpose**: Serverless function deployed on VAST DataEngine, triggered by new messages on `fraud.transactions.raw`.

**Behavior**:
- Consumes transactions from Event Broker topic
- Applies fraud rules (velocity, geo, amount, card testing, fraud ring)
- Queries VAST DataBase for historical customer data via `vastdb` SDK
- Computes composite risk score (0.0 - 1.0)
- Publishes scored transactions to `fraud.transactions.scored`
- Publishes alerts (score > 0.8) to `fraud.alerts`
- Records processing latency to `fraud.metrics`

**Deployment**: Container image deployed via `dataengine-cli`

### 7.3 Kafka Fraud Scorer (Faust)

**Purpose**: Python stream processor consuming from Kafka, equivalent logic to VAST scorer.

**Behavior**:
- Faust agent consuming from `fraud.transactions.raw`
- Applies same fraud rules as VAST scorer
- Uses Faust tables (RocksDB) for windowed state
- Queries ClickHouse for historical data (separate network call)
- Publishes scored transactions and alerts to same topics
- Records processing latency to `fraud.metrics`

**Deployment**: Docker container in docker-compose stack

### 7.4 VAST Historical Queries

**Purpose**: Demonstrate topics-as-tables — SQL on live streaming data + historical data in one query.

**Behavior**:
- Uses `vastdb` SDK to query `fraud.transactions.raw` topic as a SQL table
- JOINs live transactions with 6-month historical table (pre-loaded)
- Runs windowed aggregations, pattern matching, and std deviation calculations
- Returns results with sub-ms latency

**Key queries**:
```sql
-- Customer spending anomaly
SELECT card_id, AVG(amount) as avg_amount, STDDEV(amount) as std_amount
FROM fraud.transactions.raw
WHERE timestamp > NOW() - INTERVAL '6 months'
GROUP BY card_id
HAVING STDDEV(amount) > 3 * AVG(amount);

-- Geographic impossibility
SELECT a.card_id, a.location_city, b.location_city,
       a.timestamp, b.timestamp
FROM fraud.transactions.raw a
JOIN fraud.transactions.raw b ON a.card_id = b.card_id
WHERE a.timestamp - b.timestamp < INTERVAL '5 minutes'
  AND distance(a.location_lat, a.location_lon, b.location_lat, b.location_lon) > 500;
```

### 7.5 Kafka Historical Queries (ClickHouse)

**Purpose**: Show the Kafka side's requirement for a separate analytics database.

**Behavior**:
- ClickHouse table ingests from Kafka via Kafka Engine table
- Same SQL queries as VAST side (adapted for ClickHouse dialect)
- Historical data requires reading from MinIO tiered storage (cold read latency)
- Demonstrates ETL lag — data not queryable until ClickHouse ingests it

### 7.6 VAST AI Investigator

**Purpose**: When a transaction is flagged (score > 0.8), trigger an AI agent to investigate.

**Agents**:

| Agent | Role | VAST Features Used |
|-------|------|--------------------|
| **Deep Dive Agent** | Pulls transaction context, merchant history, customer profile; uses RAG to check regulatory watchlists; generates investigation summary | InsightEngine (RAG), DataBase (vector search), DataEngine |
| **Record Keeper Agent** | Logs the investigation, evidence, and recommended action as an immutable audit record | DataBase (append-only audit table) |

**Output**: JSON investigation report with:
- Risk assessment (high/medium/low)
- Evidence summary (which rules triggered, historical patterns found)
- Recommended action (block, flag for review, allow)
- Regulatory context (if watchlist match found)

### 7.7 Comparison Dashboard (Streamlit)

**Purpose**: Real-time side-by-side comparison of both pipelines.

**Panels**:

| Panel | Content |
|-------|---------|
| **Latency** | Rolling p50/p95/p99 latency for both pipelines (Plotly line chart) |
| **Throughput** | Msgs/sec processed by each pipeline (Plotly bar chart) |
| **Detection Feed** | Live stream of detected fraud alerts with timestamps, showing which pipeline detected first |
| **Architecture** | Visual showing VAST (1 box) vs Kafka (6 boxes) with component health status |
| **Query Demo** | Side-by-side SQL panel: run same query, show response time difference |
| **AI Investigation** | VAST-only panel showing Deep Dive Agent reports on flagged transactions |

**Data source**: Consumes from `fraud.metrics` topic on both backends + direct DB queries

---

## 8. Demo Flow (15 Minutes)

### Act 1: "Same Code, Two Backends" (2 min)

| Step | Action | Talking Point |
|------|--------|---------------|
| 1 | Show `transaction_generator.py` source code | "One producer, standard confluent-kafka library" |
| 2 | Start generator against VAST: `python generator --bootstrap-servers vast:9092` | "Producing to VAST Event Broker" |
| 3 | Start generator against Kafka: `python generator --bootstrap-servers kafka:9092` | "Same code, same flag, zero changes — Kafka API compatible" |
| 4 | Dashboard shows both pipelines receiving data | "Both ingesting identical transactions" |

### Act 2: "Real-Time Fraud Scoring" (4 min)

| Step | Action | Talking Point |
|------|--------|---------------|
| 1 | Inject velocity attack (10 txns/sec from one card) | "Simulating a stolen card being drained" |
| 2 | Dashboard latency panel lights up | "VAST detects in sub-ms, Kafka in low ms — both catch it, VAST is faster" |
| 3 | Inject geographic impossibility (NYC then London in 5 min) | "Physically impossible — classic fraud signal" |
| 4 | Show VAST DataEngine function logs vs Faust processor logs | "VAST: one serverless function. Kafka: a Faust worker in Docker with RocksDB state. Same result, different operational weight" |

### Act 3: "Query Live + Historical Data" (4 min)

| Step | Action | Talking Point |
|------|--------|---------------|
| 1 | Show Blob Expansion config in VAST Admin UI | "We configured this once — VAST automatically extracts JSON into structured columns. No Flink, no Kafka Connect, no ETL pipeline." |
| 2 | Run spending anomaly SQL on VAST (dashboard query panel) | "Topics-as-tables — querying live streaming data with standard SQL, directly on the expanded table" |
| 3 | Run same SQL on ClickHouse | "Same query, but Kafka needed a separate ClickHouse instance, a Kafka Engine table, a Materialized View for ETL, and cold reads from MinIO for 6-month history" |
| 4 | Show response times: VAST sub-ms vs ClickHouse seconds | "No cold tier, no ETL lag, no blind spots" |
| 5 | Show VAST time-travel query on 6 months of data | "Every transaction, instantly queryable — compliance auditors love this" |

### Act 4: "AI Investigates the Alert" (3 min)

| Step | Action | Talking Point |
|------|--------|---------------|
| 1 | Flagged transaction triggers Deep Dive Agent | "VAST AgentEngine kicks in automatically" |
| 2 | Show agent pulling merchant history via vector search | "Fuzzy matching against known fraud rings — not just exact lookups" |
| 3 | Show RAG-generated investigation report | "Regulatory context pulled automatically, evidence compiled, action recommended" |
| 4 | Show audit trail from Record Keeper | "Immutable audit log — compliance-ready" |
| 5 | Point to Kafka side | "On Kafka, you'd build this yourself: LangChain + Pinecone + separate model serving + custom audit pipeline" |

### Act 5: "The Architecture Slide" (2 min)

| Step | Action | Talking Point |
|------|--------|---------------|
| 1 | Dashboard architecture panel: 1 vs 6 systems | "One platform replaces Kafka, ZooKeeper, Schema Registry, Flink, ClickHouse, and MinIO" |
| 2 | Show final metrics comparison | "Sub-ms latency, instant historical queries, AI-native investigation" |
| 3 | Reference 136M msgs/sec benchmark | "And at scale, 604% more throughput — 88 VAST nodes vs 526 Kafka nodes" |
| 4 | End with CTA | "Want to run this on your data?" |

---

## 9. Project Structure

```
demos/
├── docs/
│   ├── research.md
│   └── specs/
│       └── 2026-05-07-fraud-detection-demo-prd.md
├── generator/
│   ├── __init__.py
│   ├── transaction_generator.py
│   ├── fraud_patterns.py
│   ├── schemas/
│   │   └── transaction.avsc
│   └── config.py
├── vast/
│   ├── __init__.py
│   ├── blob_expansion_setup.py        # Blob Expansion config (topics-as-tables)
│   ├── fraud_scorer/
│   │   ├── __init__.py
│   │   ├── scorer.py
│   │   ├── rules.py
│   │   └── Dockerfile
│   ├── historical/
│   │   ├── __init__.py
│   │   └── queries.py
│   ├── agents/
│   │   ├── __init__.py
│   │   ├── deep_dive_agent.py
│   │   └── record_keeper.py
│   └── setup.py
├── kafka/
│   ├── docker-compose.yml
│   ├── fraud_scorer/
│   │   ├── __init__.py
│   │   ├── scorer.py
│   │   └── rules.py
│   ├── historical/
│   │   ├── __init__.py
│   │   ├── clickhouse_setup.sql
│   │   └── queries.py
│   └── tiered_storage/
│       └── config.properties
├── dashboard/
│   ├── app.py
│   ├── metrics_collector.py
│   ├── components/
│   │   ├── latency_chart.py
│   │   ├── throughput_chart.py
│   │   ├── detection_feed.py
│   │   ├── architecture_diagram.py
│   │   └── query_demo.py
│   └── assets/
│       └── styles.css
├── scripts/
│   ├── run_demo.sh
│   ├── setup_vast.sh
│   ├── setup_kafka.sh
│   └── teardown.sh
├── requirements.txt
├── Makefile
└── README.md
```

---

## 10. Prerequisites

### VAST Cluster Requirements

| Requirement | Detail |
|-------------|--------|
| VAST Platform version | 5.0.0-sp10 or later (for vastdb SDK compatibility) |
| Event Broker | Enabled on the cluster |
| DataEngine | Enabled with compute cluster provisioned |
| AgentEngine | Enabled for AI investigation agents |
| InsightEngine | Enabled for RAG capabilities |
| S3 credentials | Access key + secret key with tabular identity policy |
| Network | SE laptop must reach VAST cluster on port 9092 (Event Broker) and management port |

### Local Machine (SE Laptop)

| Requirement | Detail |
|-------------|--------|
| Python | 3.10 - 3.13 |
| Docker Desktop | For Kafka comparison stack |
| Docker Compose | v2+ |
| RAM | 8GB+ free (Kafka + ZK + SR + Flink + ClickHouse + MinIO) |
| Disk | 10GB+ free for Docker images and data |
| `confluent-kafka` | Python package |
| `vastdb` | Python package (`pip install vastdb`) |
| `dataengine-cli` | VAST DataEngine CLI tool |
| `streamlit` | Python package for dashboard |

### Pre-loaded Data

| Data | Purpose | Size |
|------|---------|------|
| 6 months of synthetic transaction history | Historical correlation queries | ~5M rows |
| Known fraud ring merchant list | Pattern matching | ~1K entries |
| Regulatory watchlist (synthetic) | RAG-based agent lookup | ~500 entries |

---

## 11. Setup & Runbook

### Quick Start

```bash
# Clone and install
cd demos
pip install -r requirements.txt

# Step 1: Setup VAST Event Broker topics (via VAST Admin UI or dataengine-cli)
# Create topics: fraud.transactions.raw, fraud.alerts, fraud.metrics, fraud.transactions.scored

# Step 2: Configure Blob Expansion (turns JSON blobs into structured tables)
python vast/blob_expansion_setup.py \
    --endpoint https://<VAST_VMS_ENDPOINT> \
    --access-key <KEY> --secret-key <SECRET> \
    --kafka-bucket yg-bucket \
    --kafka-schema kafka_topics \
    --target-schema fraud_detection

# Step 3: Generate transactions into VAST Event Broker
PYTHONPATH=. python -m generator.transaction_generator --target vast --tps 1000 --duration 300

# Step 4: Setup Kafka comparison (local Docker)
make setup-kafka

# Step 5: Run the full demo with dashboard
make demo
```

### Makefile Targets

| Target | Action |
|--------|--------|
| `make setup-vast` | Create topics on Event Broker, deploy DataEngine functions, configure agents |
| `make setup-blob-expansion` | Configure Blob Expansion for all topics (requires VAST S3 credentials) |
| `make setup-kafka` | `docker-compose up -d`, create topics, initialize ClickHouse schema |
| `make load-history` | Load 6 months of synthetic history into VAST DataBase and ClickHouse |
| `make demo` | Start generators, scorers, and dashboard |
| `make demo-act1` | Run only Act 1 (same code, two backends) |
| `make demo-act2` | Run only Act 2 (real-time scoring) |
| `make demo-act3` | Run only Act 3 (historical queries) |
| `make demo-act4` | Run only Act 4 (AI investigation) |
| `make clean` | Tear down Kafka Docker stack, remove VAST topics/functions |

### Troubleshooting

| Issue | Fix |
|-------|-----|
| Can't reach VAST Event Broker on 9092 | Check firewall rules, VPN connection, cluster endpoint |
| Docker containers OOM | Increase Docker Desktop memory to 10GB+ |
| ClickHouse cold reads timeout | Increase MinIO timeout in `config.properties` |
| DataEngine function not deploying | Verify compute cluster is running: `vastde compute-clusters list` |
| Dashboard not updating | Check both `fraud.metrics` topics have data: consumer lag check |

---

## 12. Key Demo Talking Points

| When Prospect Asks | Answer |
|---------------------|--------|
| "Is this really the same code?" | Show `transaction_generator.py` — one `confluent-kafka.Producer()`, one `--bootstrap-servers` flag |
| "What about our existing Kafka apps?" | Zero code changes. VAST Event Broker is Kafka API-compatible. Point at your existing producers, change the bootstrap server, done. |
| "How does topics-as-tables work?" | Blob Expansion. You configure it once — map JSON fields to typed columns. VAST continuously extracts every new message into a structured table. No ETL pipeline, no Kafka Connect, no Flink, no ClickHouse. One-time config, zero maintenance. |
| "What about historical data latency?" | Kafka tiered storage reads from S3 — seconds. VAST is all-flash NVMe — sub-ms for all data, hot or cold. |
| "Can we keep our Flink jobs?" | Yes. VAST has an Apache Flink connector. But you might not need Flink — DataEngine serverless functions handle many streaming use cases with less operational overhead. |
| "What about compliance?" | VAST time-travel queries let you query any point in time across years of data. Record Keeper agent maintains immutable audit logs. |
| "What does this cost?" | Fewer systems to operate (1 vs 6), less hardware (88 nodes vs 526 for equivalent throughput), 3% write amplification vs 200%. Contact your VAST account team for sizing. |

---

## 13. Success Criteria

| Criteria | Measurement |
|----------|-------------|
| Demo runs end-to-end without manual intervention | `make demo` completes all 5 acts |
| VAST detection latency < 1ms | Dashboard p99 latency panel |
| Historical query response time: VAST < 10ms, Kafka > 1s | Dashboard query demo panel |
| All 5 fraud patterns detected by both pipelines | Detection feed shows alerts for each pattern |
| AI investigation report generated for flagged transactions | Dashboard AI panel shows agent output |
| Any SE can set up and run in < 30 minutes | Validated by 2+ SEs running from README |
| Dashboard clearly shows VAST advantages | Visual review by SE team |

---

## 14. Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| VAST cluster unavailable during demo | Cannot show VAST pipeline | Pre-record a video backup of Acts 2-4; generator + Kafka side still runs live |
| Docker stack too heavy for SE laptop | Kafka comparison fails to start | Provide a lightweight `docker-compose.light.yml` with just Kafka + ZK (no Flink/ClickHouse) and use pre-recorded results for Acts 3-5 |
| DataEngine function deployment issues | VAST scoring doesn't work | Fallback to direct `vastdb` SDK consumer script (same logic, different deployment) |
| Network latency to VAST cluster skews comparison | VAST appears slower than it is | Run from a machine on the same network as the cluster; note network round-trip in dashboard |
| Prospect asks about unsupported Kafka features | Credibility risk | Know the compatibility boundaries; redirect to "what you gain" vs "what you lose" |

---

## 15. Future Enhancements

These are out of scope for v1 but tracked for future iterations:

- **Live throughput scaling demo**: Ramp from 1K to 100K TPS and show VAST linear scaling vs Kafka degradation
- **Multi-cluster federation**: Show VAST DataSpace for cross-region fraud detection
- **Custom ML model integration**: Bring-your-own fraud model via VAST InsightEngine
- **Regulatory reporting agent**: Auto-generate SAR (Suspicious Activity Report) filings
- **Interactive mode**: Let prospect inject their own fraud patterns during the demo
