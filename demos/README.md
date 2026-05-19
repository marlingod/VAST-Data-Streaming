# VAST vs Kafka: Financial Fraud Detection Demo

A side-by-side demonstration comparing VAST Event Broker against Apache Kafka with Tiered Storage for real-time financial fraud detection.

**Key message**: Same Kafka producer code, dramatically simpler architecture, faster fraud detection, unified analytics.

---

## Quick Start

### Prerequisites

| Requirement | Detail |
|-------------|--------|
| Python | 3.10 - 3.13 |
| Docker Desktop | 8GB+ RAM allocated |
| VAST cluster | Event Broker, DataEngine, AgentEngine enabled |
| `vastdb` | `pip install vastdb` |
| `dataengine-cli` | VAST DataEngine CLI tool installed |

### 30-Second Demo (No backends needed)

Preview the dashboard with simulated data -- no VAST cluster or Kafka stack required:

```bash
cd demos
pip install -r requirements.txt
make demo-mode
```

Open http://localhost:8501

### Full Demo Setup

```bash
# 1. Install Python dependencies
cd demos
pip install -r requirements.txt

# 2. Setup VAST cluster (requires cluster access)
export VAST_ENDPOINT=<your-cluster-vip>
export VAST_ACCESS_KEY=<your-access-key>
export VAST_SECRET_KEY=<your-secret-key>
make setup-vast

# 3. Setup Kafka Docker stack (local)
make setup-kafka

# 4. Run the demo
make demo

# 5. Open the comparison dashboard
open http://localhost:8501
```

---

## Architecture

### VAST Pipeline (1 system)

```
Transaction Generator (confluent-kafka)
        |
        v
VAST Event Broker (Kafka API-compatible)
        |
        v
VAST DataEngine (serverless fraud scoring)
        |
        v
VAST DataBase (topics-as-tables, historical JOINs, vector search)
        |
        v
VAST AgentEngine (Deep Dive Agent + Record Keeper)
        |
        v
Comparison Dashboard (Streamlit)
```

### Kafka Pipeline (6 systems)

```
Transaction Generator (confluent-kafka)
        |
        v
Kafka + ZooKeeper + Schema Registry
        |
        v
Faust Stream Processor (Docker)
        |
        v
ClickHouse (ETL from Kafka) + MinIO (Tiered Storage)
        |
        v
Comparison Dashboard (Streamlit)
```

### Full Architecture Diagram

```
                    +----------------------------------+
                    |     Transaction Generator         |
                    |   (confluent-kafka Python)        |
                    |   --bootstrap-servers <VAST|KAFKA> |
                    +--------+----------------+--------+
                             |                |
               +-------------+                +---------------+
               v                                              v
  +------------------------+                  +-----------------------------+
  |  VAST Event Broker     |                  |  Kafka (Docker)             |
  |  (live cluster)        |                  |  + ZooKeeper                |
  |                        |                  |  + Schema Registry          |
  |  topics-as-tables --+  |                  |  + Tiered Storage (MinIO)   |
  +----------+-----------+ |                  +-------------+---------------+
             |             |                                |
             v             |                                v
  +--------------------+   |                  +------------------------+
  | VAST DataEngine    |   |                  | Faust Stream Processor |
  | (serverless fraud  |   |                  | (Docker container)     |
  |  scoring)          |   |                  +-------------+----------+
  +----------+---------+   |                                |
             |             |                                v
             v             v                  +------------------------+
  +---------------------------+               | ClickHouse (Docker)    |
  | VAST DataBase             |               | + ETL from Kafka       |
  | - SQL on live topics      |               | + Historical queries   |
  | - Historical JOINs        |               +-----------+------------+
  | - Vector search           |                           |
  +----------+----------------+                           |
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

---

## Demo Script (15 Minutes)

### Act 1: Same Code, Two Backends (2 min)

1. Show `generator/transaction_generator.py` -- one `confluent-kafka` producer, standard API
2. Start generator against VAST:
   ```bash
   python -m generator.transaction_generator --bootstrap-servers vast:9092
   ```
3. Start generator against Kafka:
   ```bash
   python -m generator.transaction_generator --bootstrap-servers localhost:29092
   ```
4. Dashboard shows both pipelines receiving data

**Talking point**: "Same code, same flag, zero changes -- Kafka API compatible."

### Act 2: Real-Time Fraud Scoring (4 min)

1. Inject velocity attack (10 txns/sec from one card) -- "Simulating a stolen card being drained"
2. Dashboard latency panel lights up -- VAST detects in sub-ms, Kafka in low ms
3. Inject geographic impossibility (NYC then London in 5 min)
4. Show VAST DataEngine function logs vs Faust processor logs

**Talking point**: "VAST: one serverless function. Kafka: a Faust worker in Docker with RocksDB state. Same result, different operational weight."

### Act 3: Query Live + Historical Data (4 min)

1. Run spending anomaly SQL on VAST (dashboard query panel):
   ```sql
   SELECT card_id, AVG(amount) as avg_amount, STDDEV(amount) as std_amount
   FROM fraud.transactions.raw
   WHERE timestamp > NOW() - INTERVAL '6 months'
   GROUP BY card_id
   HAVING STDDEV(amount) > 3 * AVG(amount);
   ```
2. Run same SQL on ClickHouse -- show ETL lag and cold-read latency
3. Show response times: VAST sub-ms vs ClickHouse seconds
4. Show VAST time-travel query on 6 months of data

**Talking point**: "Topics-as-tables. No ETL, no cold tier, no blind spots."

### Act 4: AI Investigates the Alert (3 min)

1. Flagged transaction triggers Deep Dive Agent automatically
2. Show agent pulling merchant history via vector search
3. Show RAG-generated investigation report (risk assessment, evidence, recommended action)
4. Show audit trail from Record Keeper -- immutable, compliance-ready
5. Point to Kafka side -- "You'd build this yourself: LangChain + Pinecone + model serving + custom audit pipeline"

### Act 5: The Architecture Slide (2 min)

1. Dashboard architecture panel: 1 system vs 6 systems
2. Show final metrics comparison
3. Reference benchmark: 136M msgs/sec on 88 VAST nodes vs 22.5M msgs/sec on 526 Kafka nodes (604% more throughput)
4. CTA: "Want to run this on your data?"

---

## Topics and Data Model

| Topic | Purpose | Partitions (VAST / Kafka) |
|-------|---------|---------------------------|
| `fraud.transactions.raw` | Incoming transactions | 8 / 3 |
| `fraud.transactions.scored` | Transactions + risk score | 8 / 3 |
| `fraud.alerts` | High-risk flagged transactions (score > 0.8) | 4 / 3 |
| `fraud.metrics` | Pipeline latency and throughput | 4 / 3 |

### Fraud Patterns Injected

| Pattern | Rate | Detection Method |
|---------|------|-----------------|
| Velocity attack | 5% | 10+ txns from same card in 60s |
| Geographic impossibility | 3% | Two cities > 500km apart within 5 min |
| Amount anomaly | 4% | Transaction > 10x customer's 90-day avg |
| Card testing | 3% | 5+ transactions of $1-2 in 30s |
| Known fraud ring | 2% | Merchant ID in fraud ring lookup table |
| Legitimate | 83% | Normal distribution (should NOT trigger alerts) |

---

## Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `VAST_ENDPOINT` | For VAST setup | -- | VAST cluster VIP or hostname |
| `VAST_ACCESS_KEY` | For VAST setup | -- | S3 access key with tabular identity policy |
| `VAST_SECRET_KEY` | For VAST setup | -- | S3 secret key |
| `VAST_BOOTSTRAP_SERVERS` | No | `vast:9092` | Event Broker bootstrap address |
| `DEMO_TPS` | No | `1000` | Transactions per second |
| `DEMO_DURATION` | No | `300` | Demo duration in seconds |
| `DEMO_MODE` | No | `false` | Run dashboard with simulated data |

### Make Targets

| Target | Description |
|--------|-------------|
| `make help` | Show all available targets |
| `make setup-vast` | Create Event Broker topics, deploy DataEngine functions, load data |
| `make setup-kafka` | Start Kafka Docker stack, create topics, initialize ClickHouse |
| `make setup-all` | Setup both VAST and Kafka |
| `make load-history` | Load 6 months of synthetic transaction history into VAST |
| `make demo` | Run full demo (both generators + dashboard) |
| `make demo-mode` | Run dashboard with simulated data (no backends needed) |
| `make demo-vast-only` | Run generator against VAST only |
| `make demo-kafka-only` | Run generator against Kafka only |
| `make clean` | Tear down Kafka Docker stack, remove VAST topics/functions |
| `make status` | Show status of all services |

---

## Services (Kafka Stack)

When `make setup-kafka` is running:

| Service | Address | Notes |
|---------|---------|-------|
| Kafka broker | `localhost:29092` | External listener for host access |
| Schema Registry | `localhost:8081` | Avro schema management |
| ClickHouse HTTP | `localhost:8123` | Analytics queries |
| MinIO Console | `localhost:9001` | Tiered storage UI (`minioadmin`/`minioadmin`) |

---

## Key Talking Points (Q&A)

| When Prospect Asks | Answer |
|--------------------|--------|
| "Is this really the same code?" | Show `transaction_generator.py` -- one `confluent-kafka.Producer()`, one `--bootstrap-servers` flag. |
| "What about our existing Kafka apps?" | Zero code changes. VAST Event Broker is Kafka API-compatible. Point at your existing producers, change the bootstrap server, done. |
| "How does topics-as-tables work?" | Every Kafka topic is automatically a SQL table in VAST DataBase. No ETL, no Kafka Connect, no ClickHouse -- just query it. |
| "What about historical data latency?" | Kafka tiered storage reads from S3 -- seconds. VAST is all-flash NVMe -- sub-ms for all data, hot or cold. |
| "Can we keep our Flink jobs?" | Yes. VAST has an Apache Flink connector. But you might not need Flink -- DataEngine serverless functions handle many streaming use cases with less operational overhead. |
| "What about compliance?" | VAST time-travel queries let you query any point in time across years of data. Record Keeper agent maintains immutable audit logs. |
| "What does this cost?" | Fewer systems (1 vs 6), less hardware (88 nodes vs 526 for equivalent throughput), 3% write amplification vs 200%. Contact your VAST account team for sizing. |

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| Cannot reach VAST Event Broker on 9092 | Check firewall rules, VPN connection, and cluster endpoint. Verify with: `nc -zv <VAST_ENDPOINT> 9092` |
| Docker containers OOM killed | Increase Docker Desktop memory to 10GB+ in Settings > Resources |
| ClickHouse cold reads timeout | Increase MinIO timeout in `kafka/tiered_storage/config.properties` |
| DataEngine function not deploying | Verify compute cluster is running: `vastde compute-clusters list` |
| Dashboard not updating | Check both `fraud.metrics` topics have data. Run a consumer lag check on each backend. |
| `make setup-kafka` hangs at "Waiting for Kafka" | Ensure Docker Desktop is running. Check logs: `docker compose -f kafka/docker-compose.yml logs kafka` |
| Python import errors | Ensure you installed dependencies: `pip install -r requirements.txt` and are running from the `demos/` directory |
| VAST cluster unavailable during live demo | Use `make demo-mode` to run with simulated data. Pre-record a video backup of Acts 2-4. |

---

## Project Structure

```
demos/
├── dashboard/              # Streamlit comparison dashboard
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
├── generator/              # Transaction generator (shared by both backends)
│   ├── transaction_generator.py
│   ├── fraud_patterns.py
│   ├── schemas/
│   │   └── transaction.avsc
│   └── config.py
├── vast/                   # VAST-specific components
│   ├── fraud_scorer/       # DataEngine serverless function
│   ├── historical/         # Topics-as-tables queries
│   ├── agents/             # Deep Dive Agent + Record Keeper
│   └── setup.py            # Schema and data loading
├── kafka/                  # Kafka-specific components
│   ├── docker-compose.yml  # Full Kafka ecosystem
│   ├── fraud_scorer/       # Faust stream processor
│   ├── historical/         # ClickHouse queries
│   └── tiered_storage/     # MinIO config
├── scripts/                # Setup and run scripts
│   ├── setup_vast.sh
│   ├── setup_kafka.sh
│   ├── run_demo.sh
│   └── teardown.sh
├── docs/                   # Research and specs
├── requirements.txt
├── Makefile
└── README.md
```
