# VAST Fraud Detection — End-to-End Architecture

> **Demo Scope Note**: This architecture shows the target design. Components marked "EXISTS" are implemented and tested. Components marked "PLANNED" are designed but not yet built. The demo can run today with the existing components; planned components are shown for the complete vision.

## VAST Pipeline (1 Platform)

```
═══════════════════════════════════════════════════════════════════════════════════
                    VAST FRAUD DETECTION — END-TO-END ARCHITECTURE
═══════════════════════════════════════════════════════════════════════════════════


  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                        DATA GENERATION LAYER                               │
  │                                                                             │
  │   Transaction Generator (confluent-kafka Python)                           │
  │   ├── 10,000 synthetic customers with behavioral profiles                  │
  │   ├── 63 realistic merchants (Walmart, Starbucks, Amazon, ...)             │
  │   ├── 5 fraud patterns: velocity, geo-impossible, amount, card-test, ring  │
  │   └── Configurable: --tps 1000 --duration 300 --fraud-rate 0.17            │
  │                                                                             │
  └──────────────────────────────┬──────────────────────────────────────────────┘
                                 │ Kafka protocol (JSON)
                                 ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                        VAST EVENT BROKER                                    │
  │                        (Kafka API-compatible)                               │
  │                                                                             │
  │   Topics:                                                                   │
  │   ┌───────────────────────┐  ┌──────────────────────┐                      │
  │   │ fraudtransactionsraw  │  │ fraud.transactions.   │                      │
  │   │ (8 partitions)        │  │ scored (6 partitions) │                      │
  │   └───────────┬───────────┘  └──────────▲────────────┘                      │
  │               │                         │                                   │
  │   ┌───────────┼─────────┐  ┌────────────┼────────────┐                     │
  │   │ fraud.alerts        │  │ fraud.metrics           │                      │
  │   │ (6 partitions)      │  │ (6 partitions)          │                      │
  │   └───────────▲─────────┘  └─────────────────────────┘                      │
  │               │                                                             │
  └───────────────┼──────────────────┬──────────────────────────────────────────┘
                  │                  │
                  │                  ▼
                  │   ┌──────────────────────────────────┐
                  │   │      BLOB EXPANSION               │
                  │   │      (Topics-as-Tables)            │
                  │   │                                    │
                  │   │  fraudtransactionsraw     → fraud_detection.transactions (14 cols)
                  │   │  fraud.transactions.scored → fraud_detection.scored      (18 cols)
                  │   │  fraud.alerts             → fraud_detection.alerts       (10 cols)
                  │   │  fraud.metrics            → fraud_detection.metrics      ( 4 cols)
                  │   │                                    │
                  │   │  JSON → structured columns         │
                  │   │  Queryable via Trino / vastdb SDK  │
                  │   │                                    │
                  │   │  Latency: tested < 1s at demo      │
                  │   │  scale (1K-10K TPS)                │
                  │   └──────────────────────────────────┘
                  │
                  ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                        VAST DATAENGINE                                      │
  │                        (Serverless Compute)                                 │
  │                                                                             │
  │   ┌─────────────────────────────────────────┐                              │
  │   │  S3 Element Trigger (fraudrawtrig)       │                              │
  │   │  ObjectCreated:* → fraudtransactionsraw  │                              │
  │   └────────────────────┬────────────────────┘                              │
  │                        │                                                    │
  │                        ▼                                                    │
  │   ┌─────────────────────────────────────────┐                              │
  │   │  FRAUD SCORER FUNCTION                   │                              │
  │   │                                          │                              │
  │   │  5 weighted rules:                       │                              │
  │   │  ├── Velocity (0.25)                     │                              │
  │   │  ├── Geographic impossibility (0.30)     │                              │
  │   │  ├── Amount anomaly (0.20)               │                              │
  │   │  ├── Card testing (0.15)                 │                              │
  │   │  └── Fraud ring merchant (0.10)          │                              │
  │   │                                          │                              │
  │   │  Composite risk score: 0.0 — 1.0         │                              │
  │   │  Boost: single rule >= 0.8 overrides     │                              │
  │   │                                          │                              │
  │   │  Output:                                 │                              │
  │   │  ├── ALL txns → fraud.transactions.scored│                              │
  │   │  └── score >= 0.8 → fraud.alerts         │                              │
  │   └─────────────────────┬───────────────────┘                              │
  │                         │                                                   │
  └─────────────────────────┼───────────────────────────────────────────────────┘
                            │
                            │  score >= 0.8 (high-risk alert)
                            ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                        VAST AGENTENGINE                                     │
  │                        (AI Agent Orchestration)                             │
  │                                                                             │
  │   Note: AgentEngine is a new VAST capability (GA 2025). This demo uses     │
  │   it as an early-adopter reference architecture. Agents can alternatively   │
  │   run as DataEngine functions for maximum portability.                      │
  │                                                                             │
  │   ┌─────────────────────────────────────────────────────────────────────┐   │
  │   │              ROUTING FUNCTION (DataEngine)                           │   │
  │   │                                                                     │   │
  │   │  Consumes alerts from fraud.alerts topic                           │   │
  │   │  Routes to agents based on fraud type and confidence               │   │
  │   │  Consolidates results from all agents                             │   │
  │   │                                                                     │   │
  │   └──────┬──────────────────────────────────────────────────────────────┘   │
  │          │                                                                  │
  │          ▼                                                                  │
  │   ┌──────────────────────────────────────────────────────────┐             │
  │   │              INVESTIGATION AGENT                          │             │
  │   │              (merged Risk Sensor + Deep Dive)             │             │
  │   │                                                           │             │
  │   │  Two tool calls on the same VAST DataBase session:       │             │
  │   │                                                           │             │
  │   │  Tool 1: Vector Watchlist Search (InsightEngine)         │             │
  │   │  ├── PEP / sanctions matching                            │             │
  │   │  ├── Embedding similarity to known fraud patterns        │             │
  │   │  └── Fuzzy merchant watchlist matching                   │             │
  │   │                                                           │             │
  │   │  Tool 2: Historical Analysis (DataBase)                  │             │
  │   │  ├── 12-month card transaction history                   │             │
  │   │  ├── Merchant frequency and risk analysis                │             │
  │   │  ├── Fraud ring cross-reference                          │             │
  │   │  └── Evidence compilation                                │             │
  │   │                                                           │             │
  │   │  Note on vector search: InsightEngine requires NVIDIA    │             │
  │   │  GPU-equipped VAST nodes for CUDA-accelerated search.    │             │
  │   │  Demo uses simulated vector search as fallback.          │             │
  │   │                                                           │             │
  │   │  Note on RAG: InsightEngine handles retrieval.           │             │
  │   │  LLM inference runs on co-located GPU nodes or via       │             │
  │   │  external API (OpenAI/Anthropic). Demo uses template-    │             │
  │   │  based generation as fallback.                           │             │
  │   │                                                           │             │
  │   └──────────────────────┬───────────────────────────────────┘             │
  │                          │  investigation results                           │
  │                          ▼                                                  │
  │   ┌──────────────────────────────────────────┐                             │
  │   │           ACTION AGENT                    │                             │
  │   │                                           │                             │
  │   │  Receives: investigation results          │                             │
  │   │                                           │                             │
  │   │  Decides and executes:                    │                             │
  │   │  ├── BLOCK  → publish to fraud.actions    │                             │
  │   │  ├── FLAG   → escalate to human review    │                             │
  │   │  ├── ALLOW  → release transaction         │                             │
  │   │  └── SAR    → trigger regulatory report   │                             │
  │   │                                           │                             │
  │   │  VAST: AgentEngine, Event Broker          │                             │
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
  │   │  ├── Failure events (if any agent fails)  │                             │
  │   │  └── Timestamp + agent ID                 │                             │
  │   │                                           │                             │
  │   │  VAST: DataBase (append-only audit table) │                             │
  │   └──────────────────────────────────────────┘                             │
  │                                                                             │
  │   Error Handling:                                                           │
  │   ├── Agent timeout → retry with backoff (3 attempts)                      │
  │   ├── Agent failure → Record Keeper logs failure event                     │
  │   ├── Unprocessable message → logged, skipped (no DLQ yet)                │
  │   └── All failures visible in audit_trail table                            │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                        VAST DATABASE                                        │
  │                        (Unified Query Layer)                                │
  │                                                                             │
  │   Trino / vastdb SDK / Spark                                               │
  │                                                                             │
  │   ┌─────────────────┐  ┌──────────────────┐  ┌──────────────────────┐      │
  │   │ fraud_detection  │  │ fraud_detection   │  │ fraud_detection      │      │
  │   │ .transactions    │  │ .scored           │  │ .alerts              │      │
  │   │ (14 cols)        │  │ (18 cols)         │  │ (10 cols)            │      │
  │   └─────────────────┘  └──────────────────┘  └──────────────────────┘      │
  │                                                                             │
  │   ┌─────────────────┐  ┌──────────────────┐  ┌──────────────────────┐      │
  │   │ fraud_detection  │  │ audit_trail       │  │ watchlists           │      │
  │   │ .metrics         │  │ (Record Keeper)   │  │ (seed data for       │      │
  │   │                  │  │                    │  │  Investigation Agent)│      │
  │   └─────────────────┘  └──────────────────┘  └──────────────────────┘      │
  │                                                                             │
  └──────────────────────────────┬──────────────────────────────────────────────┘
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

## Agent Summary

| # | Agent | Status | VAST Feature | Function |
|---|-------|--------|-------------|----------|
| 0 | Routing Function | PLANNED | DataEngine | Consume alerts, route to agents, consolidate results |
| 1 | Investigation Agent | EXISTS (partial) | DataBase + InsightEngine | Vector watchlist search + historical analysis + evidence compilation |
| 2 | Action Agent | PLANNED | AgentEngine + Event Broker | Execute decisions: block/flag/allow, trigger SAR |
| 3 | Record Keeper | EXISTS | DataBase (append-only) | Immutable audit trail for compliance |

Data Flow: `Alert → Router → Investigation Agent → Action Agent → Record Keeper`

All agents share a single VAST DataBase session — no data movement between systems.

### Design Decisions

- **3 agents (not 5)**: Merged Risk Sensor + Deep Dive into one Investigation Agent with two tool calls. Cleaner boundaries, less workflow hops, and all buildable for the demo.
- **Router as DataEngine function (not "Orchestrator Agent")**: Routing is deterministic dispatch, not agentic reasoning. Honest naming.
- **Template-based generation (not LLM)**: Demo runs without external LLM dependency. Production would use InsightEngine retrieval + co-located GPU inference.
- **Simulated vector search**: Demo uses direct SQL lookups. Production would use InsightEngine CUDA-accelerated vector search (requires NVIDIA GPU nodes).

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
  │  + OPTIONAL: SCHEMA REGISTRY (only if using Avro; demo uses JSON)          │
  │                                                                             │
  │  ┌───────────────┐  ┌─────────────────────┐                                │
  │  │ Kafka Brokers  │  │ Schema Registry     │                                │
  │  │ (KRaft, 3+     │  │ (optional — only    │                                │
  │  │  nodes)         │  │  needed for Avro)   │                                │
  │  │                │  │                     │                                │
  │  │ Topics:        │  │ Note: ksqlDB can    │                                │
  │  │ fraud.raw      │  │ provide SQL-on-     │                                │
  │  │ fraud.scored   │  │ streams but lacks   │                                │
  │  │ fraud.alerts   │  │ historical ad-hoc   │                                │
  │  │ fraud.metrics  │  │ queries over full   │                                │
  │  │                │  │ dataset, native     │                                │
  │  │ Tiered storage │  │ vector search, and  │                                │
  │  │ → S3 (cold)    │  │ agent orchestration │                                │
  │  └───────┬────────┘  └─────────────────────┘                                │
  │          │                                                                  │
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
  │ ├── Velocity rules     │     │                                    │
  │ ├── Geo rules          │     │ Separate cluster to manage,        │
  │ ├── Windowed state     │     │ monitor, and scale                 │
  │ │   (RocksDB)          │     │                                    │
  │ └── Checkpointing      │     │ (Not needed in VAST — Blob         │
  │                        │     │  Expansion does this automatically)│
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
  │ └── Separate schema    │     │ Note: S3 is a cloud service, not   │
  │                        │     │ a system you deploy — but it is a  │
  │ Query latency: seconds │     │ data boundary your pipeline must   │
  │ (vs VAST: sub-ms)      │     │ cross, adding latency              │
  └────────────────────────┘     └────────────────────────────────────┘

             │  score >= 0.8 (alert)
             ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                     AI INVESTIGATION LAYER                                  │
  │                     (Build-Your-Own — 2-3 additional systems)               │
  │                                                                             │
  │   ┌──────────────────┐     ┌──────────────────────────────────┐            │
  │   │ SYSTEM 6:        │     │  Custom Agent Orchestration       │            │
  │   │ LangChain /      │     │  ├── Route alerts                │            │
  │   │ LangGraph /      │     │  ├── Manage state (Redis/Postgres)│            │
  │   │ CrewAI           │     │  ├── Retry/escalation logic       │            │
  │   │                  │     │  └── Error handling              │            │
  │   │ Agent framework  │     └──────────────────────────────────┘            │
  │   │ (self-hosted)    │                                                     │
  │   └──────────────────┘     ┌──────────────────────────────────┐            │
  │                            │  Custom Investigation + Action    │            │
  │   ┌──────────────────┐     │  ├── Query ClickHouse for history │            │
  │   │ SYSTEM 7:        │     │  ├── Query vector DB              │            │
  │   │ Pinecone /       │     │  ├── Call LLM for RAG             │            │
  │   │ Milvus /         │     │  ├── Execute block/flag/allow     │            │
  │   │ Weaviate         │     │  └── Write audit to Postgres     │            │
  │   │                  │     └──────────────────────────────────┘            │
  │   │ Vector database  │                                                     │
  │   │ (for watchlist   │     Note: Both VAST and Kafka need an LLM for      │
  │   │  matching)       │     the "generation" step of RAG. VAST's            │
  │   │                  │     InsightEngine handles retrieval; the LLM        │
  │   │ (Not needed in   │     runs on co-located GPU nodes or external API.  │
  │   │  VAST —          │     Neither side eliminates this dependency.        │
  │   │  InsightEngine   │                                                     │
  │   │  has native      │                                                     │
  │   │  vector search)  │                                                     │
  │   └──────────────────┘                                                     │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                        DASHBOARD                                            │
  │   (Same Streamlit app — but must query 3+ different backends)              │
  │                                                                             │
  │   Data sources:                                                            │
  │   ├── Kafka (metrics topic) — for throughput/latency                       │
  │   ├── ClickHouse — for historical queries                                  │
  │   ├── Vector DB — for watchlist search results                             │
  │   └── Agent framework — for investigation reports                          │
  │                                                                             │
  │   (VAST dashboard: single vastdb session for everything)                   │
  └─────────────────────────────────────────────────────────────────────────────┘
```

---

## Side-by-Side Comparison

| Layer | VAST (1 Platform) | Kafka (6-8 Systems) |
|-------|-------------------|---------------------|
| **Streaming** | Event Broker | Kafka (KRaft, no ZooKeeper) |
| **ETL** | Blob Expansion (automatic, one-time config) | Kafka Connect (separate cluster) |
| **Stream Processing** | DataEngine function | Flink / Faust (separate cluster) |
| **Analytics** | DataBase (topics-as-tables via Trino) | ClickHouse (requires ETL pipeline) |
| **Cold Storage** | All-flash NVMe (no cold tier) | S3 tiered storage (seconds latency for cold reads) |
| **Agent Orchestration** | AgentEngine / DataEngine | LangChain / LangGraph (self-hosted) |
| **Vector Search** | InsightEngine (GPU-accelerated) | Pinecone / Milvus (separate cluster) |
| **LLM / RAG** | InsightEngine (retrieval) + co-located GPU or external LLM | External LLM (same dependency) |
| **Audit Trail** | DataBase (append-only table) | Part of agent framework (varies) |
| **Dashboard Data** | Single vastdb session | 3+ separate backend connections |
| | | |
| **Total Systems** | **1** | **6-8** (varies by deployment) |
| **Data Boundaries** | Zero — all data in one namespace | 3-5 boundaries (Kafka → Flink → ClickHouse → Vector DB → Audit DB) |
| **Operational Overhead** | Single platform to manage | 6-8 clusters to deploy, monitor, scale, patch |
| **Security** | Single identity policy | Multiple auth/authz configs |

### Why "6-8" and not "11+"?

The system count depends on deployment choices:
- **Managed Kafka (Confluent Cloud)**: Kafka + Schema Registry + Kafka Connect + ksqlDB = 1 SaaS subscription (but data still crosses system boundaries)
- **Self-hosted Kafka**: Each component is a separate cluster to operate
- **Minimum realistic**: Kafka + stream processor + analytics DB + vector DB + agent framework = **6 systems**
- **Full production**: Add Schema Registry, S3, audit DB, LLM serving = **8 systems**

**The VAST advantage is not just fewer systems — it is zero data movement.** Even on Confluent Cloud, data crosses 3-5 system boundaries with associated latency, security, and consistency challenges. On VAST, every query — streaming, historical, vector, audit — hits the same DataBase through the same session.

### What About ksqlDB?

ksqlDB provides SQL-on-streams within the Kafka ecosystem, which partially addresses VAST's topics-as-tables advantage. However:
- ksqlDB queries are limited to data within Kafka's retention window (no full historical ad-hoc queries)
- ksqlDB uses RocksDB state stores (additional failure mode, rebalancing overhead)
- No native vector search (still need Pinecone/Milvus)
- No agent orchestration (still need LangChain)
- No unified audit trail (still need separate audit DB)
- Schema management still separate (Schema Registry)

VAST's Blob Expansion + DataBase provides full ad-hoc SQL over the complete dataset (not just the stream window), with no state store management.

---

## Limitations and Honest Disclaimers

### What the Demo Simplifies
- **Vector search**: Demo uses direct SQL lookups instead of CUDA-accelerated InsightEngine vector search (requires NVIDIA GPU nodes)
- **RAG generation**: Demo uses template-based reports instead of LLM-generated narratives (would need co-located GPU or external API)
- **Agent orchestration**: Demo uses standalone Python consumers; production would use AgentEngine (new VAST capability, GA 2025)
- **Error handling**: Basic retry + logging; production would need dead-letter queues and circuit breakers

### What Both Sides Need (Not a VAST Advantage)
- LLM for the "generation" step of RAG (both VAST and Kafka need this)
- Monitoring and alerting (Prometheus/Grafana or equivalent)
- Schema evolution strategy (Blob Expansion handles it differently than Schema Registry, but both need a plan)

### VAST-Specific Prerequisites
- InsightEngine vector search requires NVIDIA GPU-equipped nodes
- AgentEngine is a new capability (GA 2025) — early adopter territory
- Event Broker requires CNodes with Kafka VIP pool configured
