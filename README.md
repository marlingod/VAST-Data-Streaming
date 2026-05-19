# VAST Data Streaming — Fraud Detection Demo

A side-by-side demonstration comparing **VAST Event Broker** against **Apache Kafka with Tiered Storage** for real-time financial fraud detection.

**Key message**: Same Kafka producer code, dramatically simpler architecture, faster fraud detection, unified analytics.

## Quick Start

```bash
cd demos
pip install -r requirements.txt

# Preview with simulated data (no cluster needed)
make demo-mode
# Open http://localhost:8501

# Full demo (requires VAST cluster)
./setup.sh    # First-time: guided wizard
make demo     # Starts everything
```

See [demos/README.md](demos/README.md) for full documentation, architecture diagrams, demo script, and SE runbook.

## Architecture

| | VAST Pipeline | Kafka Pipeline |
|---|---|---|
| **Systems** | 1 (VAST Platform) | 6 (Kafka + ZK + SR + Flink + ClickHouse + MinIO) |
| **Streaming** | Event Broker (Kafka API-compatible) | Apache Kafka |
| **Analytics** | Topics-as-tables via Blob Expansion | ETL to ClickHouse |
| **Scoring** | DataEngine serverless function | Faust stream processor |
| **AI Investigation** | AgentEngine + InsightEngine | Build-your-own (LangChain + vector DB) |

## What's In the Demo

- **Transaction Generator**: 5 fraud patterns (velocity, geo-impossibility, amount anomaly, card testing, fraud ring) across 63 realistic merchants
- **11M+ transactions** tested on a single VAST CNode at 14K TPS
- **Fraud Scorer**: Real-time scoring with risk scores published to `fraud.transactions.scored` and `fraud.alerts`
- **SQL Queries**: 7 fraud detection queries running directly on Kafka topics via Trino (topics-as-tables)
- **Comparison Dashboard**: Streamlit app with latency, throughput, architecture, and query comparison panels

## Project Status

See the [workload completion checklist](demos/README.md#workload-completion-status) for current status across all 6 phases.

## Documentation

- [Full README & SE Runbook](demos/README.md)
- [Research: Kafka vs VAST Event Broker](demos/docs/research.md)
- [PRD & Demo Script](demos/docs/specs/2026-05-07-fraud-detection-demo-prd.md)
- [Automation Design Spec](demos/docs/specs/2026-05-19-reproducible-demo-automation-design.md)
