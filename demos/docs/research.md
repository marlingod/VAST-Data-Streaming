# Financial Fraud Detection: Streaming Pipeline Research

## Kafka + Tiered Storage vs. VAST Platform Event Broker

---

## Executive Summary

This document compares two approaches for building a real-time financial fraud detection streaming pipeline:

1. **Apache Kafka with Tiered Storage (KIP-405)** — the industry-standard event streaming platform with a two-tier hot/cold storage architecture
2. **VAST Platform Event Broker** — a Kafka API-compatible streaming engine embedded in VAST's unified AI Operating System

Both platforms can ingest and process financial transactions in real time. However, they differ fundamentally in architecture, operational complexity, and how they handle the intersection of streaming and analytics — which is critical for fraud detection workloads that require correlating live transactions against historical patterns.

**Key finding**: The VAST Event Broker eliminates the multi-system complexity of Kafka-based fraud pipelines by unifying event streaming, SQL analytics, and AI processing into a single platform. Its "topics-as-tables" capability means streaming transactions are instantly queryable alongside historical data — removing the ETL lag that creates blind spots in fraud detection.

---

## 1. Apache Kafka with Tiered Storage

### 1.1 Architecture Overview

Kafka's tiered storage (KIP-405) introduces a two-tier architecture:

- **Local Tier (Hot)**: Recent data on broker SSDs/EBS. Serves tail reads with millisecond latency via OS page cache. Governed by `local.retention.ms`.
- **Remote Tier (Cold)**: Older segments offloaded to S3/GCS/Azure Blob. Shared across the cluster. Governed by `retention.ms`.

**Key components**:
- RemoteStorageManager (RSM) — manages segment upload/fetch/delete (no production implementation ships with Apache Kafka; requires third-party plugin from Confluent, Aiven, or custom)
- RemoteLogMetadataManager (RLMM) — tracks remote segment metadata via internal topic
- ZooKeeper or KRaft — cluster coordination
- Schema Registry — Avro/Protobuf schema management

### 1.2 Fraud Detection Pipeline Workflow

```
[Transaction Sources]
        │
        ▼
[Kafka Producers] ──► [Kafka Brokers (Local Tier)]
                              │
                    ┌─────────┴─────────┐
                    ▼                   ▼
           [Tail Consumers]    [Async Upload to S3]
           (Real-time fraud     (Cold storage for
            scoring)             compliance/ML training)
                    │
                    ▼
        [Stream Processor (Kafka Streams / Flink)]
           ├── Device Recognition (stream-table join)
           ├── Location Analysis (windowed aggregation)
           ├── Velocity Checks (stateful time windows)
           ├── Amount Anomaly Detection (statistical models)
           └── ML Model Inference (external model serving)
                    │
                    ▼
           [Alert Topic] ──► [Case Management System]
                    │
                    ▼
           [Analytics DB] ──► [Dashboard / Reporting]
           (Separate system:
            ClickHouse, Druid,
            or data warehouse)
```

**Processing patterns** (from Confluent's published architecture):
- **Stateless operations**: Filter, map, route transactions by type/amount
- **Stateful operations**: Windowed aggregation for velocity checks, co-partitioned stream-table joins for device/location matching
- **Composite keys**: userId + deviceHash to distribute state evenly and avoid data skew
- **Kafka Connect**: Integration with external blacklists, geolocation DBs, and ML model registries

### 1.3 Tiered Storage Benefits for Fraud Detection

| Benefit | Impact |
|---------|--------|
| **Cost reduction** | 3-9x storage savings; one financial firm reduced compliance storage from $600K/month to $58K/month |
| **Infinite retention** | Regulatory compliance (PCI-DSS, SOX) — retain years of transaction history |
| **Faster recovery** | Disk failure: 2 min (vs 230 min); cluster scale-up: 1 hr (vs 13 hrs) |
| **ML training data** | Historical transaction replay for model retraining without separate data lake |

### 1.4 Limitations and Challenges

| Limitation | Detail |
|------------|--------|
| **Cold read latency** | Seconds to minutes for first batch from S3 (vs milliseconds for local) |
| **Operational complexity** | Adds failure modes without eliminating existing ones; requires managing ZooKeeper/KRaft, brokers, Schema Registry, tiered storage plugins |
| **No production RSM** | Apache Kafka ships no production RemoteStorageManager — must use Confluent, Aiven, or build custom |
| **Data loss risk** | KAFKA-17062: retention can delete local segments before remote upload completes |
| **Cross-AZ networking** | ISR replication across AZs costs ~$0.053/GiB — tiered storage doesn't reduce this |
| **No compacted topics** | Not supported with tiered storage |
| **Multi-system architecture** | Fraud detection requires Kafka + stream processor + analytics DB + ML serving + dashboard — 5+ systems to operate |

### 1.5 Production Readiness

- **Production-ready** since Kafka 3.9.0 (2024)
- Key fixes in Kafka 4.2.0: sequential remote fetch (KAFKA-14915), fetch size limits (KAFKA-19462)
- Critical open bug: KAFKA-17062 (data loss scenario)
- Managed offerings: Confluent Cloud (most mature), AWS MSK, Aiven

### 1.6 Published Performance

- **Producer throughput**: Unaffected by tiered storage
- **Consumer tail reads**: Unaffected (OS page cache)
- **Consumer cold reads**: ~6x slower than producer rate
- **Broker CPU overhead**: ~10% increase from segment upload
- **Fraud detection latency**: Sub-100ms achievable for tail reads with Kafka Streams/Flink
- **Published research**: 94.2% fraud sensitivity rate with sub-second response times using Kafka + Flink

---

## 2. VAST Platform Event Broker

### 2.1 Architecture Overview

The VAST Event Broker is a **Kafka API-compatible** streaming engine embedded directly in VAST's CNodes (compute nodes). It uses VAST's DASE (Disaggregated and Shared Everything) architecture.

**Key architectural differences from Kafka**:

| Aspect | Kafka | VAST Event Broker |
|--------|-------|-------------------|
| Broker design | Stateful (manages persistent state) | Stateless (offloads to shared storage) |
| Storage | Log segments on local disks + ISR replication | Disaggregated all-flash NVMe with erasure coding |
| Scaling | Coupled compute-storage | Independent compute and storage scaling |
| Data retention | Limited by disk/tiered storage policies | Unlimited with full fidelity |
| Write amplification | +200% (3x replication) | +3% (erasure coding) |
| Coordination | ZooKeeper/KRaft + Raft consensus | No external coordination needed |
| Analytics | Separate system required | Built-in SQL via "topics-as-tables" |

**"Topics-as-tables" via Blob Expansion**: When JSON messages land in the Event Broker, they are stored as raw blobs in a topic table (partition, key, value columns). **Blob Expansion** is a one-time configuration that automatically extracts JSON fields from the `value` column into structured, typed columns in a target table. This runs continuously on all new messages — no ETL pipeline, no Flink, no Kafka Connect. The expanded table is instantly queryable with SQL via the `vastdb` SDK, Trino, or Spark.

**How Blob Expansion works**:
1. **Source**: Kafka topic table (e.g., `yg-bucket.kafka_topics.fraud.transactions.raw`)
2. **Source column**: `value` — the raw JSON blob
3. **Target**: Structured table in a separate schema (e.g., `yg-bucket.fraud_detection.transactions`)
4. **Configuration**: Define a PyArrow schema mapping JSON fields to typed columns (string, float64, bool, etc.)
5. **Result**: Every new message is automatically extracted — the target table grows in real time

This eliminates the entire ETL stack that Kafka requires: no Schema Registry for deserialization, no Kafka Connect for data movement, no Flink/Spark for transformation, no separate analytics database for querying.

### 2.2 Fraud Detection Pipeline Workflow

```
[Transaction Sources]
        │
        ▼
[Kafka Producers] ──► [VAST Event Broker (CNodes)]
  (same Kafka                    │
   protocol)            ┌───────┴────────┐
                        ▼                ▼
              [Blob Expansion]     [Kafka Consumers]
              (JSON → structured   (Real-time fraud
               columns, one-time    scoring via
               config, automatic)   stream processing)
                        │                │
                        ▼                ▼
              [VAST DataBase]    [VAST DataEngine]
              (Expanded tables)  ├── Serverless functions
              ├── SQL on live    │   triggered by events
              │   + historical   ├── ML model inference
              ├── Windowed       └── Alert generation
              │   aggregation
              └── Pattern
                  detection
                        │
                        ▼
              [AI Agent Pipeline (VAST AgentEngine)]
              ├── Intake Agent: OCR + embedding generation
              ├── Risk Sensor Agent: fuzzy matching + anomaly detection
              ├── Deep Dive Agent: RAG-based investigation
              ├── Action Agent: evidence compilation + recommendations
              └── Record Keeper Agent: immutable audit logs
                        │
                        ▼
              [Dashboard / Case Management]
```

**Key workflow advantages**:
- Same Kafka producer code — no application changes
- Transactions land as SQL-queryable tables instantly
- Historical correlation happens in the same system (no data movement)
- AI agents run on the same platform as the data
- Single system replaces: Kafka + Flink + analytics DB + ML serving + audit store

### 2.3 VAST Real-Time Risk Neutralization

VAST has published a specific architecture for financial compliance and fraud detection:

**Five specialized AI agents** on the VAST platform:

1. **Intake Agent** — orchestrates OCR and embedding generation on incoming documents (KYC forms, transaction records)
2. **Risk Sensor Agent** — performs fuzzy watchlist matching via vector search; detects anomalies beyond exact keyword matching
3. **Deep Dive Agent** — validates alerts using RAG (Retrieval-Augmented Generation) to pull regulatory context; traces ownership structures
4. **Action Agent** — compiles evidence and recommends actions (flag, block, escalate)
5. **Record Keeper Agent** — maintains immutable audit logs for compliance

**Learning flywheel**: Human feedback refines OCR models, embedding generation, vector search relevance, and anomaly detection thresholds over time.

### 2.4 Performance Characteristics

| Metric | Value | Source |
|--------|-------|--------|
| **Throughput per CNode** | 2M events/sec | vastdata.com |
| **Cluster throughput (88 nodes)** | 136M msgs/sec at 99% scaling efficiency | OpenMessaging benchmark |
| **Max cluster throughput** | 500M+ msgs/sec (largest deployments) | vastdata.com |
| **vs Kafka** | 604% more throughput on equivalent hardware | OpenMessaging benchmark |
| **vs Redpanda** | 156% faster | Published comparison |
| **Latency** | Sub-millisecond at scale | vastdata.com |
| **Nodes to match (136M msgs/sec)** | VAST: 88, Redpanda: 136, Kafka: 526 | Calculated from benchmark |

**Benchmark details** (OpenMessaging, 1KB messages, 128 partitions):
- 88 AMD-based CNodes (98 threads, 382GB RAM each)
- 62 JBOFs with 1,364 NVMe devices + 496 SCM devices
- 12 client hosts, 120 benchmark workers
- 200GbE networking

### 2.5 Advantages for Fraud Detection

| Advantage | Detail |
|-----------|--------|
| **Unified architecture** | Single system for streaming + analytics + AI — no ETL lag creating fraud blind spots |
| **Topics-as-tables** | Live transactions queryable via SQL alongside years of historical data |
| **Sub-ms latency** | Every transaction analyzed, not just sampled |
| **Kafka compatibility** | Drop-in replacement — existing producers/consumers work unchanged |
| **No operational overhead** | No ZooKeeper, no partition management, no tiered storage plugins |
| **AI-native** | Built-in vector search, RAG, and agent orchestration for fraud investigation |
| **Compliance** | Time-travel queries across years of event data; immutable audit logs |
| **Write efficiency** | 3% write amplification (erasure coding) vs 200%+ (Kafka replication) |

### 2.6 Limitations

| Limitation | Detail |
|------------|--------|
| **Requires VAST infrastructure** | Cannot run on commodity hardware or public cloud VMs |
| **Newer technology** | Event Broker announced Feb 2025; less battle-tested than Kafka's 10+ year ecosystem |
| **Ecosystem** | No equivalent to Kafka Connect's 200+ connectors |
| **Stream processing** | Flink connector available, but no equivalent to Kafka Streams library |
| **Community** | Smaller community and fewer third-party integrations |

---

## 3. Side-by-Side Comparison

| Dimension | Kafka + Tiered Storage | VAST Event Broker |
|-----------|----------------------|-------------------|
| **Protocol** | Native Kafka | Kafka API-compatible |
| **Throughput** | ~22M msgs/sec (526 nodes) | 136M msgs/sec (88 nodes) |
| **Latency** | ms (hot), seconds (cold) | Sub-ms (all data) |
| **Storage cost** | Reduced 3-9x vs non-tiered | Unified — no separate tiers |
| **Retention** | Infinite (with tiered) | Infinite (native) |
| **Analytics** | Requires separate system | Built-in SQL (topics-as-tables) |
| **AI/ML** | External model serving | Native InsightEngine + AgentEngine |
| **Operational systems** | 5-7 (Kafka, ZK, SR, Flink, DB, ML, dashboard) | 1 (VAST platform) |
| **Write amplification** | 200%+ (3x replication) | 3% (erasure coding) |
| **Cold read latency** | Seconds to minutes | Sub-ms (all-flash, no cold tier) |
| **Compliance** | Manual audit pipeline | Built-in time-travel + audit agents |
| **Ecosystem** | 200+ connectors, massive community | Flink connector, vastdb_sdk, DataEngine |
| **Maturity** | 10+ years, battle-tested | Event Broker since Feb 2025 |
| **Infrastructure** | Any cloud/on-prem | Requires VAST hardware |

---

## 4. Available VAST Tools & SDKs (github.com/vast-data)

| Tool | Purpose | Relevance to Demo |
|------|---------|-------------------|
| **[vastdb_sdk](https://github.com/vast-data/vastdb_sdk)** | Python SDK for VAST DataBase — schema management, data ingestion, SQL queries via PyArrow | Query topics-as-tables, insert/read transaction data, predicate pushdown filtering |
| **[dataengine-cli](https://github.com/vast-data/dataengine-cli)** | CLI for managing DataEngine functions, pipelines, triggers, topics | Deploy serverless fraud scoring functions, configure event triggers |
| **[dataengine-pipelines](https://github.com/vast-data/dataengine-pipelines)** | Curated pipeline examples (cron triggers, S3 triggers, LLM processing) | Reference patterns for building event-driven fraud detection pipelines |
| **[cosmos-labs](https://github.com/vast-data/cosmos-labs)** | Hands-on lab guides for VAST platform (storage, metadata, analytics) | Training material; weather data pipeline lab demonstrates analytics patterns |
| **[vast-csi](https://github.com/vast-data/vast-csi)** | Container Storage Interface driver | Kubernetes deployment of demo components |
| **[vastpy](https://github.com/vast-data/vastpy)** | Python SDK for VAST Management System | Cluster management and monitoring |
| **[terraform-provider-vastdata](https://github.com/vast-data/terraform-provider-vastdata)** | Terraform provider | Infrastructure-as-code for demo deployment |

---

## 5. Demo Recommendation

### Approach: VAST Event Broker-First with Kafka Comparison Baseline

Build a **financial fraud detection demo** that uses the **same Kafka producer code** to feed transactions into both systems, then highlights the architectural and performance differences.

### Demo Components

#### 5.1 Transaction Generator (Shared)
- Python-based synthetic transaction generator using `kafka-python` or `confluent-kafka`
- Produces realistic financial events: card transactions, wire transfers, ACH payments
- Injects known fraud patterns: velocity attacks, geographic impossibilities, amount anomalies, card testing
- Configurable throughput: 1K-100K transactions/sec
- Output: Kafka protocol messages to configurable bootstrap servers

#### 5.2 Pipeline A — VAST Event Broker

```
Transaction Generator
        │ (Kafka protocol)
        ▼
VAST Event Broker
        │
        ├──► Topics-as-Tables (instant SQL)
        │     └── JOIN with historical patterns
        │     └── Windowed velocity checks
        │     └── Amount deviation analysis
        │
        ├──► DataEngine Functions (serverless)
        │     └── ML model inference
        │     └── Risk score calculation
        │     └── Alert generation
        │
        └──► AI Agent Investigation
              └── Deep Dive Agent (RAG-based)
              └── Audit trail (Record Keeper)
```

**Tools used**: `vastdb_sdk`, `dataengine-cli`, Kafka producer library

#### 5.3 Pipeline B — Kafka + Tiered Storage

```
Transaction Generator
        │ (Kafka protocol)
        ▼
Kafka Broker (Docker)
        │
        ├──► Kafka Streams / Flink
        │     └── Stateful windowed aggregation
        │     └── Stream-table joins
        │     └── Risk scoring
        │
        ├──► Tiered Storage (MinIO as S3)
        │     └── Historical segment archival
        │     └── Compliance retention
        │
        └──► Analytics DB (ClickHouse/DuckDB)
              └── Historical pattern queries
              └── Dashboard data
```

**Tools used**: Docker Compose (Kafka, ZooKeeper, Schema Registry, MinIO, ClickHouse), Kafka Streams/Flink

#### 5.4 Comparison Dashboard
- Side-by-side metrics: latency, throughput, detection rate
- Architecture complexity visualization (1 system vs 5-7 systems)
- Query demonstration: same SQL query on topics-as-tables (VAST) vs separate analytics DB (Kafka)
- Cost/resource comparison

### 5.5 Key Demo Scenarios

1. **Real-time velocity attack**: Burst of transactions from same card — show detection latency difference
2. **Geographic impossibility**: Transaction in NYC followed by London 5 minutes later — show cross-data-source correlation
3. **Historical pattern match**: New transaction matches a fraud ring pattern from 6 months ago — show cold read latency (VAST: sub-ms vs Kafka tiered: seconds)
4. **AI investigation**: Flagged transaction triggers deep-dive agent with RAG-based research — show VAST-native AI capability vs external ML serving
5. **Compliance audit**: Time-travel query across 1 year of transactions — show VAST's native capability vs Kafka's tiered storage cold reads

### 5.6 What to Highlight in the Demo

| Demo Moment | VAST Advantage |
|-------------|----------------|
| "Same producer code" | Kafka API compatibility — zero application changes |
| "Query live transactions with SQL" | Topics-as-tables eliminates ETL |
| "Correlate with 6-month history" | No cold read penalty — sub-ms for all data |
| "One system, not seven" | Unified platform vs Kafka + ZK + SR + Flink + DB + ML + Dashboard |
| "AI investigates the alert" | Native AgentEngine + InsightEngine |
| "136M msgs/sec" | 604% faster than Kafka on equivalent hardware |

---

## Sources

- [VAST Event Broker Announcement](https://www.vastdata.com/blog/announcing-the-vast-event-broker)
- [VAST 136M Messages/Sec Benchmark](https://www.vastdata.com/blog/streaming-136m-messages-per-second-on-vast)
- [VAST Real-Time Risk Neutralization](https://www.vastdata.com/blog/vast-unveils-first-ever-platform-for-real-time-risk-neutralization)
- [VAST Real-Time Event Analytics Feature](https://www.vastdata.com/features/real-time-event-analytics)
- [Beyond Kafka — VAST Event](https://www.vastdata.com/events/beyond-kafka-real-time-event-architectures-for-ai-with-the-vast-event-broker-4-4)
- [VAST GitHub — vastdb_sdk](https://github.com/vast-data/vastdb_sdk)
- [VAST GitHub — dataengine-cli](https://github.com/vast-data/dataengine-cli)
- [VAST GitHub — dataengine-pipelines](https://github.com/vast-data/dataengine-pipelines)
- [Kafka Tiered Storage Docs (4.1)](https://kafka.apache.org/41/operations/tiered-storage/)
- [KIP-405: Kafka Tiered Storage](https://cwiki.apache.org/confluence/display/KAFKA/KIP-405:+Kafka+Tiered+Storage)
- [Confluent — Fraud Prevention with Kafka Streams](https://www.confluent.io/blog/fraud-prevention-and-threat-detection-with-kafka-streams/)
- [IJERT — Production-Ready Fraud Pipeline with Flink and Kafka](https://www.ijert.org/from-streams-to-security-architecting-a-production-ready-fraud-pipeline-with-flink-and-kafka-ijertv15is030725)
- [Confluent — Infinite Kafka Storage](https://www.confluent.io/blog/infinite-kafka-storage-in-confluent-platform/)
- [Kafka Tiered Storage Pitfalls — Red Hat Developer](https://developers.redhat.com/articles/2025/08/21/hidden-pitfalls-kafka-tiered-storage)
- [KAFKA-17062 — Data Loss Bug](https://issues.apache.org/jira/browse/KAFKA-17062)
