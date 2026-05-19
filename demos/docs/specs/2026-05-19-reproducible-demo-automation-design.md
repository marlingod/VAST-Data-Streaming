# Design: Reproducible Demo Automation

**Date**: 2026-05-19
**Author**: VAST Solutions Engineering
**Status**: Approved

---

## Context

The fraud detection demo currently requires ~10 manual steps to set up and run. This makes it fragile and hard for other SEs to reproduce. The goal is to make the entire demo scriptable so any VAST SE on the shared lab cluster can go from git clone to running demo in under 10 minutes.

## Requirements

- **Audience**: VAST SEs using a shared lab cluster with pre-provisioned credentials
- **Scorer**: Standalone scorer by default, DataEngine deployment optional
- **SE workflow**: Guided first-time setup wizard, then `make demo` for repeat runs
- **Idempotent**: Running setup twice doesn't break anything
- **No secrets in git**: All credentials in `.env` (gitignored)

---

## 1. Setup Wizard (`setup.sh`)

Interactive first-time setup that creates `.env` and validates the VAST cluster.

### Flow

```
[1/6] VAST Event Broker Configuration
  - Prompt: Event Broker VIP or hostname
  - Prompt: Kafka port (default: 9092)
  - Test: nc -zv <host> <port>

[2/6] VAST VMS Credentials
  - Prompt: VMS endpoint (https://...)
  - Prompt: Access key
  - Prompt: Secret key
  - Test: vastdb.connect() or vastpy-cli API call

[3/6] VAST Bucket & Schema
  - Prompt: Kafka bucket name (default: yg-bucket)
  - Prompt: Kafka schema name (default: kafka_topics)
  - Prompt: Target schema name (default: fraud_detection)
  - Test: Verify bucket exists via vastdb SDK

[4/6] Creating Kafka Topics
  - Create: fraud.transactions.raw (if not exists)
  - Create: fraud.transactions.scored (if not exists)
  - Create: fraud.alerts (if not exists)
  - Create: fraud.metrics (if not exists)
  - Method: vastpy-cli or vastdb SDK

[5/6] Configuring Blob Expansion
  - fraud.transactions.raw → fraud_detection.transactions (14 columns)
  - fraud.transactions.scored → fraud_detection.scored (18 columns)
  - fraud.alerts → fraud_detection.alerts (10 columns)
  - Method: vastdb SDK (blob_expansion_setup.py)

[6/6] Saving Configuration
  - Write .env file with all settings
  - Verify .env is in .gitignore
```

### .env File Format

```bash
# VAST Event Broker
VAST_BOOTSTRAP_SERVERS=172.200.204.135:9092

# VAST VMS / S3 Credentials
VAST_ENDPOINT=https://vms.selab-var204.selab.vastdata.com
VAST_ACCESS_KEY=<key>
VAST_SECRET_KEY=<secret>

# VAST Bucket & Schema
VAST_KAFKA_BUCKET=yg-bucket
VAST_KAFKA_SCHEMA=kafka_topics
VAST_TARGET_SCHEMA=fraud_detection

# Demo Settings
DEMO_TPS=1000
DEMO_DURATION=300
```

### Idempotency

- If `.env` already exists, wizard asks: "Configuration found. Re-run setup? [y/N]"
- Each step checks if work is already done (topic exists, blob expansion exists) and skips
- Safe to run multiple times

---

## 2. `make demo` Orchestration

After `.env` exists, `make demo` runs the full end-to-end flow.

### Pre-flight Checks

1. `.env` exists → if not, tell SE to run `./setup.sh` or `make setup`
2. VAST Event Broker reachable → `nc -zv <host> <port>`
3. Docker Desktop running → `docker info`
4. Python dependencies installed → import check

### Startup Sequence

1. Source `.env`
2. Start Kafka Docker stack (`docker compose up -d`)
3. Wait for Kafka + ClickHouse health
4. Start standalone fraud scorer (background)
5. Start VAST generator (background)
6. Start Kafka generator (background)
7. Start Streamlit dashboard (background, DEMO_MODE=false)
8. Print dashboard URL
9. Wait for Ctrl+C

### Shutdown Sequence (Ctrl+C)

1. Stop generators
2. Stop scorer
3. Stop dashboard
4. Print summary (messages generated, alerts triggered)
5. Leave Docker running (use `make clean` to stop)

### All Make Targets

| Target | Description | Requires .env |
|--------|-------------|---------------|
| `make setup` | Run the setup wizard | No |
| `make demo` | Full side-by-side demo | Yes |
| `make demo-vast` | VAST pipeline only (generator + scorer + dashboard) | Yes |
| `make demo-kafka` | Kafka pipeline only (Docker + generator + dashboard) | No |
| `make demo-mode` | Dashboard with simulated data | No |
| `make generate` | Just the generator (`make generate TPS=5000 DURATION=60`) | Yes |
| `make generate-vast` | Generator against VAST only | Yes |
| `make generate-kafka` | Generator against Kafka only | No |
| `make score` | Just the standalone scorer | Yes |
| `make status` | Health check all components | Partial |
| `make clean` | Tear down Docker + optionally VAST topics | Partial |
| `make deploy-dataengine` | Optional: deploy scorer via vastde CLI | Yes |

---

## 3. Pre-flight Check Script (`scripts/preflight.sh`)

Called by `make demo` before starting anything:

```bash
#!/usr/bin/env bash
# Exit codes: 0 = all good, 1 = missing .env, 2 = VAST unreachable,
#             3 = Docker not running, 4 = deps missing

[[ -f .env ]] || { echo "ERROR: .env not found. Run: make setup"; exit 1; }
source .env

nc -zw3 "$VAST_HOST" "$VAST_PORT" 2>/dev/null || { echo "ERROR: VAST unreachable"; exit 2; }
docker info >/dev/null 2>&1 || { echo "ERROR: Docker not running"; exit 3; }
python3 -c "import confluent_kafka, faker, streamlit" 2>/dev/null || { echo "ERROR: pip install -r requirements.txt"; exit 4; }

echo "All checks passed"
exit 0
```

---

## 4. Status Check (`make status`)

Shows health of all components in one view:

```
$ make status

VAST Event Broker:    ✓ 172.200.204.135:9092 (1 broker)
VAST Topics:
  fraud.transactions.raw:     11,336,724 messages
  fraud.transactions.scored:      14,866 messages
  fraud.alerts:                    5,455 messages
  fraud.metrics:                   2,238 messages

Kafka Docker Stack:   ✓ Running (6 containers)
  Kafka broker:       ✓ localhost:29092
  ClickHouse:         ✓ localhost:8123
  MinIO:              ✓ localhost:9001

Fraud Scorer:         ✓ PID 12345 (running)
Dashboard:            ✓ http://localhost:8501
```

---

## 5. Files to Create/Modify

### New Files

| File | Purpose |
|------|---------|
| `setup.sh` | Interactive first-time setup wizard |
| `scripts/preflight.sh` | Pre-flight connectivity and dependency checks |
| `scripts/status.sh` | Health check all components |
| `.env.example` | Template for .env with placeholder values and comments |

### Modified Files

| File | Changes |
|------|---------|
| `Makefile` | Add new targets (setup, demo-vast, demo-kafka, generate, score, status, deploy-dataengine). All targets source .env. |
| `scripts/run_demo.sh` | Add pre-flight checks, source .env, start scorer, proper PID management |
| `scripts/setup_vast.sh` | Replace placeholder topic creation with actual vastdb SDK calls, call blob_expansion_setup.py |
| `vast/blob_expansion_setup.py` | Add scored + alerts expansion schemas (currently only raw) |
| `vast/setup.py` | Fix main() to actually execute setup functions, not just print commands |
| `dashboard/app.py` | Default to DEMO_MODE=false when .env exists with real credentials |
| `README.md` | Update Quick Start to reference setup.sh, simplify instructions |
| `.gitignore` | Ensure .env is listed |

### Blob Expansion Schemas to Add

**fraud.transactions.scored** (18 columns):
```
transaction_id (STRING), timestamp (STRING), card_id (STRING),
customer_id (STRING), merchant_id (STRING), merchant_category (STRING),
amount (DOUBLE), currency (STRING), location_lat (DOUBLE),
location_lon (DOUBLE), location_city (STRING), device_fingerprint (STRING),
channel (STRING), is_fraud (BOOLEAN), risk_score (DOUBLE),
triggered_rules (STRING), scored_at (STRING), scoring_latency_ms (DOUBLE)
```

**fraud.alerts** (10 columns):
```
transaction_id (STRING), card_id (STRING), amount (DOUBLE),
risk_score (DOUBLE), triggered_rules (STRING), fraud_type (STRING),
merchant_id (STRING), location_city (STRING), timestamp (STRING),
alerted_at (STRING)
```

---

## 6. SE Runbook (Quick Reference)

### First Time (10 minutes)

```bash
git clone https://github.com/marlingod/VAST-Data-Streaming.git
cd VAST-Data-Streaming/demos
pip install -r requirements.txt
./setup.sh              # Guided wizard — creates .env, topics, blob expansion
make demo               # Starts everything
# Open http://localhost:8501
```

### Repeat Runs (30 seconds)

```bash
cd VAST-Data-Streaming/demos
make demo               # Sources .env, starts everything
# Open http://localhost:8501
# Ctrl+C to stop
```

### Quick Preview (No Cluster)

```bash
cd VAST-Data-Streaming/demos
pip install -r requirements.txt
make demo-mode          # Simulated data, no VAST or Kafka needed
# Open http://localhost:8501
```

---

## 7. Verification

- [ ] `./setup.sh` completes on fresh clone with valid VAST credentials
- [ ] `make demo` starts all components and dashboard shows live data
- [ ] `make demo-mode` works without any credentials
- [ ] Running `./setup.sh` twice doesn't break anything
- [ ] `.env` is not committed to git
- [ ] `make status` shows health of all components
- [ ] `make clean` tears down everything cleanly
- [ ] A second SE can clone and run from scratch in under 10 minutes
