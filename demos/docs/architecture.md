# VAST Fraud Detection — End-to-End Architecture

> **Version**: Current (VAST 5.5). DataEngine topic triggers are coming in the next VAST release. This architecture uses two complementary scoring paths: DataEngine for S3/file-based events, standalone scorer for Kafka-produced events. Both share the same VAST DataBase and Event Broker.

---

## VAST Pipeline (1 Platform, 2 Scoring Paths)

```
═══════════════════════════════════════════════════════════════════════════════════
                    VAST FRAUD DETECTION — END-TO-END ARCHITECTURE
                    (Current Version — VAST 5.5)
═══════════════════════════════════════════════════════════════════════════════════


  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                        DATA GENERATION LAYER                               │
  │                                                                             │
  │   Transaction Generator (confluent-kafka Python)                           │
  │   ├── 10,000 synthetic customers with behavioral profiles                  │
  │   ├── 63 realistic merchants (Walmart, Starbucks, Amazon, ...)             │
  │   ├── 5 fraud patterns: velocity, geo-impossible, amount, card-test, ring  │
  │   └── Configurable: --target vast --tps 1000 --duration 300                │
  │                                                                             │
  └──────────────────────────────┬──────────────────────────────────────────────┘
                                 │ Kafka protocol (JSON)
                                 ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                        VAST EVENT BROKER                                    │
  │                        (Kafka API-compatible)                               │
  │                                                                             │
  │   ┌─────────────────────────────────────────────────────────────────────┐   │
  │   │                       Kafka Topics                                  │   │
  │   │                                                                     │   │
  │   │   fraudtransactionsraw (8 part)    fraud.transactions.scored (6 part)│   │
  │   │   fraud.alerts (6 part)            fraud.metrics (6 part)           │   │
  │   │                                                                     │   │
  │   └─────────────────────────────────────────────────────────────────────┘   │
  │                                                                             │
  └──────────┬──────────────────────────┬───────────────────────────────────────┘
             │                          │
             │                          │
     ┌───────┴────────┐        ┌───────┴────────┐
     │  PATH A:        │        │  PATH B:        │
     │  Real-Time      │        │  File/Batch     │
     │  Kafka Flow     │        │  S3 Flow        │
     └───────┬────────┘        └───────┬────────┘
             │                          │
             ▼                          ▼
  ┌──────────────────────┐   ┌──────────────────────────────────────┐
  │ STANDALONE SCORER    │   │ VAST DATAENGINE                      │
  │ (Python consumer)    │   │ (Serverless Compute)                 │
  │                      │   │                                      │
  │ Consumes from:       │   │ ┌──────────────────────────────┐    │
  │ fraudtransactionsraw │   │ │ S3 Element Trigger            │    │
  │                      │   │ │ (fraudrawtrig)                │    │
  │ Same scoring logic:  │   │ │                               │    │
  │ ├── Velocity (0.25)  │   │ │ Fires on: ObjectCreated:*    │    │
  │ ├── Geo (0.30)       │   │ │ in yg-de-source-bucket       │    │
  │ ├── Amount (0.20)    │   │ │                               │    │
  │ ├── Card test (0.15) │   │ │ Use case: document ingestion,│    │
  │ └── Fraud ring(0.10) │   │ │ KYC onboarding, batch files  │    │
  │                      │   │ └──────────────┬───────────────┘    │
  │ Publishes to:        │   │                │                     │
  │ ├── fraud.trans.     │   │                ▼                     │
  │ │   scored           │   │ ┌──────────────────────────────┐    │
  │ └── fraud.alerts     │   │ │ FRAUD SCORER FUNCTION        │    │
  │   (score >= 0.8)     │   │ │ (same 5 rules, containerized)│    │
  │                      │   │ │                               │    │
  │ Runs on:             │   │ │ Publishes to:                │    │
  │ Any server with      │   │ │ ├── fraud.transactions.scored│    │
  │ Python + confluent-  │   │ │ └── fraud.alerts             │    │
  │ kafka installed      │   │ │                               │    │
  │                      │   │ │ Runs on: VAST K8s cluster    │    │
  │ Status: TESTED       │   │ │ Status: DEPLOYED             │    │
  │ 50K+ msgs scored     │   │ └──────────────────────────────┘    │
  └──────────┬───────────┘   └──────────────────┬──────────────────┘
             │                                   │
             │    Both paths produce to the       │
             │    same output topics               │
             └──────────────┬────────────────────┘
                            │
                            ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                        BLOB EXPANSION                                       │
  │                        (Topics-as-Tables)                                   │
  │                                                                             │
  │   Automatic JSON → structured columns. Configured once, runs continuously. │
  │                                                                             │
  │   fraudtransactionsraw     → fraud_detection.transactions  (14 columns)    │
  │   fraud.transactions.scored → fraud_detection.scored        (18 columns)    │
  │   fraud.alerts             → fraud_detection.alerts         (10 columns)    │
  │   fraud.metrics            → fraud_detection.metrics        ( 4 columns)    │
  │                                                                             │
  │   Queryable via Trino / vastdb SDK / Spark                                 │
  │   Latency: < 1s from Kafka produce to SQL-queryable row (demo scale)       │
  │                                                                             │
  └──────────────────────────────┬──────────────────────────────────────────────┘
                                 │
                                 ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                        VAST DATABASE                                        │
  │                        (Unified Query Layer)                                │
  │                                                                             │
  │   7 Fraud Detection SQL Queries (all tested on 11M+ records):              │
  │   ├── Velocity attack detection (windowed COUNT by card_id)                │
  │   ├── Card testing detection (amount < $3 + velocity)                      │
  │   ├── Geographic impossibility (distance + time JOIN)                      │
  │   ├── Spending anomaly (AVG + STDDEV by merchant)                          │
  │   ├── Fraud hotspot by merchant (fraud % ranking)                          │
  │   ├── Fraud concentration by city (geographic risk)                        │
  │   └── Real-time dashboard summary (total txns, fraud rate, volume)         │
  │                                                                             │
  │   ┌─────────────────┐  ┌──────────────────┐  ┌──────────────────────┐      │
  │   │ fraud_detection  │  │ fraud_detection   │  │ fraud_detection      │      │
  │   │ .transactions    │  │ .scored           │  │ .alerts              │      │
  │   │ (14 cols,        │  │ (18 cols)         │  │ (10 cols)            │      │
  │   │  11M+ rows)      │  │                   │  │                      │      │
  │   └─────────────────┘  └──────────────────┘  └──────────────────────┘      │
  │                                                                             │
  │   ┌─────────────────┐  ┌──────────────────┐  ┌──────────────────────┐      │
  │   │ fraud_detection  │  │ audit_trail       │  │ watchlists           │      │
  │   │ .metrics         │  │ (Record Keeper)   │  │ (Investigation       │      │
  │   │                  │  │                    │  │  Agent seed data)    │      │
  │   └─────────────────┘  └──────────────────┘  └──────────────────────┘      │
  │                                                                             │
  └──────────────────────────────┬──────────────────────────────────────────────┘
                                 │
                                 │  score >= 0.8 (high-risk alerts)
                                 ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                        AI AGENTS                                            │
  │                        (VAST AgentEngine / Python)                          │
  │                                                                             │
  │   Note: AgentEngine is a new VAST capability (GA 2025). Agents can run     │
  │   as AgentEngine functions or as standalone Python scripts. Both access     │
  │   the same VAST DataBase session — no data movement.                       │
  │                                                                             │
  │   ┌─────────────────────────────────────────────────────────────────────┐   │
  │   │              ROUTING FUNCTION                                        │   │
  │   │                                                                     │   │
  │   │  Consumes alerts from fraud.alerts topic                           │   │
  │   │  Routes to agents based on fraud type and confidence               │   │
  │   │  Consolidates results                                              │   │
  │   │  Status: PLANNED                                                   │   │
  │   └──────────────────────────┬──────────────────────────────────────────┘   │
  │                              │                                              │
  │                              ▼                                              │
  │   ┌──────────────────────────────────────────────────────────┐             │
  │   │              INVESTIGATION AGENT                          │             │
  │   │                                                           │             │
  │   │  Tool 1: Vector Watchlist Search                         │             │
  │   │  ├── PEP / sanctions matching                            │             │
  │   │  ├── Embedding similarity to known fraud patterns        │             │
  │   │  └── Fuzzy merchant watchlist matching                   │             │
  │   │  (Production: InsightEngine. Demo: SQL lookups)          │             │
  │   │                                                           │             │
  │   │  Tool 2: Historical Analysis                             │             │
  │   │  ├── 12-month card transaction history                   │             │
  │   │  ├── Merchant frequency and risk analysis                │             │
  │   │  ├── Fraud ring cross-reference                          │             │
  │   │  └── Evidence compilation                                │             │
  │   │                                                           │             │
  │   │  Output: Investigation report with risk level,           │             │
  │   │  evidence, and recommended action                        │             │
  │   │  Status: EXISTS (partial — SQL lookups, template reports)│             │
  │   └──────────────────────┬───────────────────────────────────┘             │
  │                          │                                                  │
  │                          ▼                                                  │
  │   ┌──────────────────────────────────────────┐                             │
  │   │           ACTION AGENT                    │                             │
  │   │                                           │                             │
  │   │  ├── BLOCK  → publish block command       │                             │
  │   │  ├── FLAG   → escalate to human review    │                             │
  │   │  ├── ALLOW  → release transaction         │                             │
  │   │  └── SAR    → trigger regulatory report   │                             │
  │   │  Status: PLANNED                          │                             │
  │   └──────────────────┬────────────────────────┘                             │
  │                      │                                                      │
  │                      ▼                                                      │
  │   ┌──────────────────────────────────────────┐                             │
  │   │        RECORD KEEPER AGENT                │                             │
  │   │                                           │                             │
  │   │  Logs every step immutably:               │                             │
  │   │  ├── Alert received                       │                             │
  │   │  ├── Agents invoked + data accessed       │                             │
  │   │  ├── Investigation findings               │                             │
  │   │  ├── Action taken + rationale             │                             │
  │   │  └── Timestamp + agent ID                 │                             │
  │   │  Status: EXISTS                           │                             │
  │   └──────────────────────────────────────────┘                             │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                        COMPARISON DASHBOARD (Streamlit)                      │
  │                                                                             │
  │   ┌─────────────────┐  ┌──────────────────┐  ┌──────────────────────┐      │
  │   │ Latency         │  │ Throughput        │  │ Detection Feed       │      │
  │   │ p50/p95/p99     │  │ msgs/sec          │  │ Live alerts          │      │
  │   └─────────────────┘  └──────────────────┘  └──────────────────────┘      │
  │   ┌─────────────────┐  ┌──────────────────┐  ┌──────────────────────┐      │
  │   │ Architecture    │  │ SQL Query Demo    │  │ AI Investigation     │      │
  │   │ 1 vs 6-8        │  │ VAST vs Kafka     │  │ Agent reports        │      │
  │   │ systems         │  │                    │  │                      │      │
  │   └─────────────────┘  └──────────────────┘  └──────────────────────┘      │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘
```

---

## Two Scoring Paths — Why?

VAST DataEngine currently supports **S3 element triggers** and **schedule triggers**. **Kafka topic triggers** (fire when a message arrives on a topic) are coming in the next VAST release.

Until topic triggers ship, the demo uses two complementary scoring paths:

| Path | Trigger | Use Case | Status |
|------|---------|----------|--------|
| **Path A: Standalone Scorer** | Kafka consumer (Python) | Real-time transaction scoring from Kafka producers | **TESTED** — 50K+ msgs scored |
| **Path B: DataEngine Function** | S3 element trigger (ObjectCreated:*) | Document processing, KYC onboarding, batch file ingestion | **DEPLOYED** — running on VAST K8s |

Both paths:
- Use the **same scoring logic** (5 weighted fraud rules)
- Publish to the **same output topics** (`fraud.transactions.scored`, `fraud.alerts`)
- Access the **same VAST DataBase** for historical lookups
- Produce data queryable through the **same Blob Expansion** tables

### Demo Talking Point

> "DataEngine runs serverless functions triggered by S3 events — ideal for document processing and batch ingestion. For real-time Kafka streams, the scorer runs as a lightweight consumer on the same platform. Kafka topic triggers are coming in the next release, collapsing both paths into one. Either way, the data never leaves VAST."

### Future (Next VAST Release)

```
Generator → Kafka produce → fraudtransactionsraw
                                    │
                            Topic Trigger (NEW)
                                    │
                                    ▼
                          DataEngine Function
                          (no standalone scorer needed)
```

---

## Component Status Summary

| Component | Status | Tested |
|-----------|--------|--------|
| Transaction Generator | **COMPLETE** | 11M+ msgs, 14K TPS on 2 CNodes |
| VAST Event Broker (topics) | **COMPLETE** | 5 topics, 2M+ in fraudtransactionsraw |
| Blob Expansion (topics-as-tables) | **COMPLETE** | 4 topic expansions, Trino queries working |
| Standalone Fraud Scorer | **COMPLETE** | 50K+ msgs scored end-to-end |
| DataEngine Pipeline | **DEPLOYED** | Running, S3 trigger ready, logs confirm "Fraud Scorer ready" |
| SQL Fraud Detection Queries | **COMPLETE** | 7 queries tested on 11M+ records |
| Investigation Agent (Deep Dive) | **EXISTS** | SQL lookups + template reports |
| Record Keeper Agent | **EXISTS** | Append-only audit trail |
| Routing Function | **PLANNED** | Alert routing by fraud type |
| Action Agent | **PLANNED** | Block/flag/allow decisions |
| Dashboard | **COMPLETE** | Demo mode working, live mode untested |
| Kafka Comparison Stack | **NOT STARTED** | Docker Compose defined but not tested |

---

## Kafka Equivalent Architecture (6-8 Systems)

> **Note on fairness**: This comparison assumes a modern Kafka deployment. Kafka 4.0+ uses KRaft (no ZooKeeper). Confluent Cloud reduces operational overhead but does not eliminate data movement between systems. ksqlDB provides SQL-on-streams but lacks historical ad-hoc queries, native vector search, and agent orchestration.

```
═══════════════════════════════════════════════════════════════════════════════════
                    KAFKA FRAUD DETECTION — EQUIVALENT ARCHITECTURE
═══════════════════════════════════════════════════════════════════════════════════


  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                        DATA GENERATION LAYER                               │
  │   Transaction Generator (confluent-kafka Python)                           │
  │   (Same code — Kafka API compatible)                                       │
  └──────────────────────────────┬──────────────────────────────────────────────┘
                                 │ Kafka protocol (JSON)
                                 ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │  SYSTEM 1: APACHE KAFKA (KRaft mode — no ZooKeeper since Kafka 4.0)       │
  │                                                                             │
  │  ┌───────────────┐  ┌─────────────────────┐                                │
  │  │ Kafka Brokers  │  │ Note: ksqlDB can    │                                │
  │  │ (KRaft, 3+     │  │ provide SQL-on-     │                                │
  │  │  nodes)         │  │ streams but lacks   │                                │
  │  │                │  │ historical ad-hoc   │                                │
  │  │ Topics:        │  │ queries, native     │                                │
  │  │ fraud.raw      │  │ vector search, and  │                                │
  │  │ fraud.scored   │  │ agent orchestration │                                │
  │  │ fraud.alerts   │  │                     │                                │
  │  │                │  │ Schema Registry     │                                │
  │  │ Tiered storage │  │ optional if using   │                                │
  │  │ → S3 (cold)    │  │ JSON (demo does)    │                                │
  │  └───────┬────────┘  └─────────────────────┘                                │
  └──────────┼──────────────────────────────────────────────────────────────────┘
             │
             │  No topics-as-tables — need ETL pipeline
             │
             ├──────────────────────────────────┐
             │                                  │
             ▼                                  ▼
  ┌────────────────────────┐     ┌────────────────────────────────────┐
  │ SYSTEM 2: STREAM       │     │ SYSTEM 3: KAFKA CONNECT            │
  │ PROCESSING             │     │ (ETL to analytics DB)              │
  │ (Flink or Faust)       │     │                                    │
  │                        │     │ Kafka → ClickHouse sink            │
  │ Fraud scoring:         │     │ Kafka → S3 sink (archival)         │
  │ ├── Same 5 rules       │     │                                    │
  │ ├── Windowed state     │     │ (Not needed in VAST — Blob         │
  │ │   (RocksDB)          │     │  Expansion does this automatically)│
  │ └── Checkpointing      │     │                                    │
  └───────────┬────────────┘     └──────────────┬─────────────────────┘
              │                                  │
              ▼                                  ▼
  ┌────────────────────────┐     ┌────────────────────────────────────┐
  │ SYSTEM 4: CLICKHOUSE   │     │ SYSTEM 5: S3 / CLOUD STORAGE      │
  │ (Analytics Database)   │     │ (Tiered Storage)                   │
  │                        │     │                                    │
  │ Historical queries     │     │ Cold data archival                 │
  │ Requires:              │     │ Cold reads: seconds to minutes     │
  │ ├── Kafka Engine table │     │ (VAST: sub-ms for all data)        │
  │ ├── Materialized View  │     │                                    │
  │ └── Separate schema    │     │ Note: S3 is a cloud service —     │
  │                        │     │ but it is a data boundary your     │
  │ Query latency: seconds │     │ pipeline must cross                │
  └────────────────────────┘     └────────────────────────────────────┘

             │  score >= 0.8 (alert)
             ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                     AI INVESTIGATION LAYER                                  │
  │                     (Build-Your-Own — 2-3 additional systems)               │
  │                                                                             │
  │   ┌──────────────────┐     ┌──────────────────────────────────┐            │
  │   │ SYSTEM 6:        │     │  Custom Investigation + Action    │            │
  │   │ LangChain /      │     │  ├── Query ClickHouse for history │            │
  │   │ LangGraph        │     │  ├── Query vector DB              │            │
  │   │ (agent framework)│     │  ├── Call LLM for reports         │            │
  │   └──────────────────┘     │  └── Write audit to separate DB  │            │
  │                            └──────────────────────────────────┘            │
  │   ┌──────────────────┐                                                     │
  │   │ SYSTEM 7:        │     Note: Both VAST and Kafka need an LLM for      │
  │   │ Pinecone /       │     the "generation" step of RAG. VAST's            │
  │   │ Milvus           │     InsightEngine handles retrieval; the LLM        │
  │   │ (vector DB)      │     runs on co-located GPU nodes or external API.  │
  │   └──────────────────┘                                                     │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                        DASHBOARD                                            │
  │   Must query 3+ different backends:                                        │
  │   ├── Kafka (metrics)    ├── ClickHouse (history)                          │
  │   ├── Vector DB (search) └── Agent framework (reports)                     │
  │                                                                             │
  │   (VAST dashboard: single vastdb session for everything)                   │
  └─────────────────────────────────────────────────────────────────────────────┘
```

---

## Side-by-Side Comparison

| Layer | VAST (1 Platform) | Kafka (6-8 Systems) |
|-------|-------------------|---------------------|
| **Streaming** | Event Broker (Kafka API-compatible) | Kafka (KRaft) |
| **ETL** | Blob Expansion (automatic, one-time config) | Kafka Connect (separate cluster) |
| **Stream Processing** | DataEngine function + standalone scorer | Flink / Faust (separate cluster) |
| **Analytics** | DataBase (topics-as-tables via Trino) | ClickHouse (requires ETL) |
| **Cold Storage** | All-flash NVMe (no cold tier) | S3 tiered storage (seconds latency) |
| **Agent Framework** | AgentEngine / Python on same platform | LangChain (self-hosted, separate) |
| **Vector Search** | InsightEngine (GPU-accelerated) | Pinecone / Milvus (separate cluster) |
| **Audit Trail** | DataBase (append-only table) | Separate DB (Postgres/MongoDB) |
| **Dashboard** | Single vastdb session | 3+ backend connections |
| | | |
| **Total Systems** | **1** | **6-8** |
| **Data Boundaries** | Zero — all data in one namespace | 3-5 boundaries |

### Why "6-8" Not More?

- **6 minimum**: Kafka + stream processor + analytics DB + vector DB + agent framework + audit store
- **8 with full stack**: Add Schema Registry + S3 tiered storage
- **Confluent Cloud**: Reduces ops overhead but data still crosses 3-5 system boundaries
- **ksqlDB**: Provides SQL-on-streams but no historical ad-hoc queries, no vector search, no agents

**The VAST advantage is zero data movement.** Even on Confluent Cloud, every query crosses system boundaries. On VAST, streaming, historical, vector, and audit data are in the same DataBase, same session.

---

## DataEngine Trigger Roadmap

| Trigger Type | Available Now | Use Case |
|---|---|---|
| **S3 Element** | Yes | File uploads, document processing, KYC onboarding, batch ingestion |
| **Schedule (Cron)** | Yes | Periodic processing, report generation, model retraining |
| **HTTP** | Yes | API-driven invocation, webhook handling |
| **Kafka Topic** | **Next release** | Real-time stream processing — fire on every Kafka message |

When topic triggers ship, the architecture simplifies to a single scoring path:
```
Generator → Kafka produce → Topic Trigger → DataEngine Function → scored/alerts
```
No standalone scorer needed.

---

## Limitations and Honest Disclaimers

### What the Demo Shows vs Production

| Capability | Demo | Production |
|---|---|---|
| **Fraud scoring** | 5 weighted rules, template-based | ML models, real-time feature engineering |
| **Vector search** | SQL lookups (simulation) | InsightEngine CUDA-accelerated (requires NVIDIA GPUs) |
| **RAG reports** | Template-based text | LLM-generated via InsightEngine + co-located GPU or API |
| **Agent orchestration** | Python scripts | AgentEngine (GA 2025, early adopter) |
| **Topic triggers** | Standalone consumer | DataEngine topic trigger (next release) |

### What Both VAST and Kafka Need

- LLM for RAG generation step (neither eliminates this)
- Monitoring and alerting (Prometheus/Grafana or equivalent)
- Schema evolution strategy

### VAST-Specific Prerequisites

- InsightEngine vector search requires NVIDIA GPU-equipped nodes
- AgentEngine is a new capability (GA 2025)
- Event Broker requires CNodes with Kafka VIP pool configured
- DataEngine topic triggers not yet available (S3 + cron + HTTP today)
