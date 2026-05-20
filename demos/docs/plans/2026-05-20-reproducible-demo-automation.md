# Reproducible Demo Automation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the fraud detection demo fully reproducible so any VAST SE can go from `git clone` to running demo in under 10 minutes.

**Architecture:** Interactive setup wizard (`setup.sh`) creates `.env` on first run, then `make demo` orchestrates all components. Pre-flight checks validate connectivity before starting. Standalone scorer runs by default; DataEngine is optional.

**Tech Stack:** Bash (setup wizard, scripts), Python (blob expansion, scorer), Make (orchestration), Streamlit (dashboard)

---

### Task 1: Create `.env.example` Template

**Files:**
- Create: `demos/.env.example`

- [ ] **Step 1: Create the template file**

```bash
# demos/.env.example
# Copy to .env and fill in your values:
#   cp .env.example .env
#
# Then run: make demo

# ── VAST Event Broker ─────────────────────────────────
# The Kafka-compatible endpoint for your VAST cluster
VAST_BOOTSTRAP_SERVERS=<VAST_EVENT_BROKER_VIP>:9092

# ── VAST VMS / S3 Credentials ────────────────────────
# Required for blob expansion setup and vastdb queries
VAST_ENDPOINT=https://<VAST_VMS_HOSTNAME>
VAST_ACCESS_KEY=<your-access-key>
VAST_SECRET_KEY=<your-secret-key>

# ── VAST Bucket & Schema ─────────────────────────────
VAST_KAFKA_BUCKET=yg-bucket
VAST_KAFKA_SCHEMA=kafka_topics
VAST_TARGET_SCHEMA=fraud_detection

# ── Demo Settings ────────────────────────────────────
DEMO_TPS=1000
DEMO_DURATION=300
```

- [ ] **Step 2: Verify `.env` is in `.gitignore`**

Check that `demos/.gitignore` or root `.gitignore` contains `.env`. If not, add it.

- [ ] **Step 3: Commit**

```bash
git add demos/.env.example
git commit -m "feat: add .env.example template for SE demo configuration"
```

---

### Task 2: Create Pre-flight Check Script (`scripts/preflight.sh`)

**Files:**
- Create: `demos/scripts/preflight.sh`

- [ ] **Step 1: Write the script**

```bash
#!/usr/bin/env bash
set -euo pipefail

# Pre-flight checks for the fraud detection demo.
# Exit codes: 0=pass, 1=missing .env, 2=VAST unreachable, 3=Docker down, 4=deps missing

PASS="\033[32m✓\033[0m"
FAIL="\033[31m✗\033[0m"
WARN="\033[33m!\033[0m"
errors=0

echo ""
echo "Pre-flight checks"
echo "═════════════════"

# 1. .env exists
if [[ -f .env ]]; then
    source .env
    printf "  ${PASS} .env loaded\n"
else
    printf "  ${FAIL} .env not found — run: ./setup.sh\n"
    exit 1
fi

# 2. Required env vars set
for var in VAST_BOOTSTRAP_SERVERS VAST_ENDPOINT VAST_ACCESS_KEY VAST_SECRET_KEY; do
    val="${!var:-}"
    if [[ -z "$val" || "$val" == *"<"* ]]; then
        printf "  ${FAIL} ${var} not configured in .env\n"
        errors=$((errors + 1))
    fi
done
if [[ $errors -gt 0 ]]; then
    echo "  Fix .env and re-run."
    exit 1
fi
printf "  ${PASS} Environment variables set\n"

# 3. VAST Event Broker reachable
VAST_HOST="${VAST_BOOTSTRAP_SERVERS%%:*}"
VAST_PORT="${VAST_BOOTSTRAP_SERVERS##*:}"
if nc -zw3 "$VAST_HOST" "$VAST_PORT" 2>/dev/null; then
    printf "  ${PASS} VAST Event Broker reachable (${VAST_HOST}:${VAST_PORT})\n"
else
    printf "  ${FAIL} VAST Event Broker unreachable (${VAST_HOST}:${VAST_PORT})\n"
    errors=$((errors + 1))
fi

# 4. Docker running (optional — only needed for Kafka comparison)
if docker info >/dev/null 2>&1; then
    printf "  ${PASS} Docker running\n"
else
    printf "  ${WARN} Docker not running (Kafka comparison won't work)\n"
fi

# 5. Python dependencies
if python3 -c "import confluent_kafka, faker, streamlit, plotly" 2>/dev/null; then
    printf "  ${PASS} Python dependencies installed\n"
else
    printf "  ${FAIL} Missing Python packages — run: pip install -r requirements.txt\n"
    errors=$((errors + 1))
fi

echo ""
if [[ $errors -gt 0 ]]; then
    echo "  ${errors} check(s) failed. Fix the above and re-run."
    exit 2
fi
printf "  \033[32mAll checks passed\033[0m\n"
echo ""
exit 0
```

- [ ] **Step 2: Make executable**

```bash
chmod +x demos/scripts/preflight.sh
```

- [ ] **Step 3: Test it without .env (should fail with exit 1)**

```bash
cd demos && bash scripts/preflight.sh
# Expected: "✗ .env not found — run: ./setup.sh"
```

- [ ] **Step 4: Commit**

```bash
git add demos/scripts/preflight.sh
git commit -m "feat: add pre-flight check script for demo validation"
```

---

### Task 3: Create Status Check Script (`scripts/status.sh`)

**Files:**
- Create: `demos/scripts/status.sh`

- [ ] **Step 1: Write the script**

```bash
#!/usr/bin/env bash
# Health check for all demo components.

PASS="\033[32m✓\033[0m"
FAIL="\033[31m✗\033[0m"
WARN="\033[33m!\033[0m"

echo ""
echo "Demo Status"
echo "═══════════"

# Source .env if available
if [[ -f .env ]]; then
    source .env
fi

# ── VAST Event Broker ──
echo ""
echo "VAST Event Broker"
echo "─────────────────"
VAST_HOST="${VAST_BOOTSTRAP_SERVERS%%:*}"
VAST_PORT="${VAST_BOOTSTRAP_SERVERS##*:}"
if [[ -n "${VAST_BOOTSTRAP_SERVERS:-}" ]] && nc -zw3 "$VAST_HOST" "$VAST_PORT" 2>/dev/null; then
    printf "  ${PASS} ${VAST_HOST}:${VAST_PORT}\n"

    # Topic message counts
    python3 -c "
from confluent_kafka import Consumer, TopicPartition
topics = ['fraud.transactions.raw', 'fraud.transactions.scored', 'fraud.alerts', 'fraud.metrics']
for topic in topics:
    try:
        c = Consumer({'bootstrap.servers': '${VAST_BOOTSTRAP_SERVERS}', 'group.id': 'status-check'})
        md = c.list_topics(topic, timeout=5)
        total = 0
        for pid in md.topics[topic].partitions:
            lo, hi = c.get_watermark_offsets(TopicPartition(topic, pid))
            total += hi
        c.close()
        print(f'  {topic}: {total:,} messages')
    except Exception as e:
        print(f'  {topic}: error ({e})')
" 2>/dev/null || printf "  ${WARN} Could not query topics (confluent-kafka not installed?)\n"
else
    printf "  ${FAIL} Not reachable\n"
fi

# ── Kafka Docker Stack ──
echo ""
echo "Kafka Docker Stack"
echo "──────────────────"
if docker info >/dev/null 2>&1; then
    RUNNING=$(cd kafka && docker compose ps --status running -q 2>/dev/null | wc -l | tr -d ' ')
    if [[ "$RUNNING" -gt 0 ]]; then
        printf "  ${PASS} ${RUNNING} containers running\n"
        for svc in kafka clickhouse minio schema-registry; do
            if cd kafka && docker compose ps --status running -q $svc >/dev/null 2>&1; then
                printf "    ${PASS} ${svc}\n"
            else
                printf "    ${FAIL} ${svc}\n"
            fi
            cd ..
        done
    else
        printf "  ${WARN} Not running (start with: make setup-kafka)\n"
    fi
else
    printf "  ${WARN} Docker not running\n"
fi

# ── Fraud Scorer ──
echo ""
echo "Fraud Scorer"
echo "────────────"
if pgrep -f "run_standalone.py" >/dev/null 2>&1; then
    PID=$(pgrep -f "run_standalone.py" | head -1)
    printf "  ${PASS} Running (PID ${PID})\n"
else
    printf "  ${WARN} Not running (start with: make score)\n"
fi

# ── Dashboard ──
echo ""
echo "Dashboard"
echo "─────────"
if pgrep -f "streamlit run" >/dev/null 2>&1; then
    printf "  ${PASS} http://localhost:8501\n"
else
    printf "  ${WARN} Not running\n"
fi

echo ""
```

- [ ] **Step 2: Make executable and commit**

```bash
chmod +x demos/scripts/status.sh
git add demos/scripts/status.sh
git commit -m "feat: add status check script for all demo components"
```

---

### Task 4: Fix Blob Expansion Schemas (Add Scored + Alerts)

**Files:**
- Modify: `demos/vast/blob_expansion_setup.py`

- [ ] **Step 1: Update ALERT_SCHEMA to include all 10 fields**

Replace the current `ALERT_SCHEMA` (6 fields) with the full 10-field schema:

```python
ALERT_SCHEMA = pa.schema([
    pa.field("transaction_id", pa.string()),
    pa.field("card_id", pa.string()),
    pa.field("amount", pa.float64()),
    pa.field("risk_score", pa.float64()),
    pa.field("triggered_rules", pa.string()),
    pa.field("fraud_type", pa.string()),
    pa.field("merchant_id", pa.string()),
    pa.field("location_city", pa.string()),
    pa.field("timestamp", pa.string()),
    pa.field("alerted_at", pa.string()),
])
```

- [ ] **Step 2: Add SCORED_SCHEMA (18 fields)**

Add after `TRANSACTION_SCHEMA`:

```python
SCORED_SCHEMA = pa.schema([
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
    pa.field("risk_score", pa.float64()),
    pa.field("triggered_rules", pa.string()),
    pa.field("scored_at", pa.string()),
    pa.field("scoring_latency_ms", pa.float64()),
])
```

- [ ] **Step 3: Add `fraud.transactions.scored` to TOPIC_CONFIG**

```python
TOPIC_CONFIG = {
    "fraud.transactions.raw": ("transactions", TRANSACTION_SCHEMA),
    "fraud.transactions.scored": ("scored", SCORED_SCHEMA),
    "fraud.alerts": ("alerts", ALERT_SCHEMA),
    "fraud.metrics": ("metrics", METRICS_SCHEMA),
}
```

- [ ] **Step 4: Commit**

```bash
git add demos/vast/blob_expansion_setup.py
git commit -m "fix: add scored + alerts schemas to blob expansion setup"
```

---

### Task 5: Create Setup Wizard (`setup.sh`)

**Files:**
- Create: `demos/setup.sh`

- [ ] **Step 1: Write the wizard script**

The wizard prompts for credentials, tests connectivity, creates topics (note: topic creation via VAST Admin UI since vastde CLI has dot-in-name issues), runs blob expansion, and writes `.env`.

```bash
#!/usr/bin/env bash
set -euo pipefail

PASS="\033[32m✓\033[0m"
FAIL="\033[31m✗\033[0m"
BOLD="\033[1m"
RESET="\033[0m"

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║  VAST Fraud Detection Demo — First-Time Setup       ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""

# Check if .env already exists
if [[ -f .env ]]; then
    read -rp "Configuration (.env) already exists. Re-run setup? [y/N] " confirm
    if [[ "${confirm,,}" != "y" ]]; then
        echo "Setup skipped. Run: make demo"
        exit 0
    fi
fi

# ── [1/6] Event Broker ──
echo ""
printf "${BOLD}[1/6] VAST Event Broker Configuration${RESET}\n"
read -rp "  Event Broker VIP or hostname: " VAST_HOST
read -rp "  Kafka port [9092]: " VAST_PORT
VAST_PORT="${VAST_PORT:-9092}"
VAST_BOOTSTRAP_SERVERS="${VAST_HOST}:${VAST_PORT}"

printf "  Testing connectivity..."
if nc -zw5 "$VAST_HOST" "$VAST_PORT" 2>/dev/null; then
    printf " ${PASS} Connected\n"
else
    printf " ${FAIL} Cannot reach ${VAST_HOST}:${VAST_PORT}\n"
    echo "  Check network/VPN and try again."
    exit 1
fi

# ── [2/6] VMS Credentials ──
echo ""
printf "${BOLD}[2/6] VAST VMS Credentials${RESET}\n"
read -rp "  VMS endpoint (https://...): " VAST_ENDPOINT
read -rp "  Access key: " VAST_ACCESS_KEY
read -rsp "  Secret key: " VAST_SECRET_KEY
echo ""

printf "  Testing API access..."
if python3 -c "
import vastdb
s = vastdb.connect(endpoint='${VAST_ENDPOINT}', access_key='${VAST_ACCESS_KEY}', secret_key='${VAST_SECRET_KEY}')
with s.transaction() as tx:
    tx.bucket('yg-bucket')
print('ok')
" 2>/dev/null | grep -q ok; then
    printf " ${PASS} Authenticated\n"
else
    printf " ${FAIL} Authentication failed\n"
    echo "  Check endpoint and credentials."
    exit 1
fi

# ── [3/6] Bucket & Schema ──
echo ""
printf "${BOLD}[3/6] VAST Bucket & Schema${RESET}\n"
read -rp "  Kafka bucket name [yg-bucket]: " VAST_KAFKA_BUCKET
VAST_KAFKA_BUCKET="${VAST_KAFKA_BUCKET:-yg-bucket}"
read -rp "  Kafka schema name [kafka_topics]: " VAST_KAFKA_SCHEMA
VAST_KAFKA_SCHEMA="${VAST_KAFKA_SCHEMA:-kafka_topics}"
read -rp "  Target schema name [fraud_detection]: " VAST_TARGET_SCHEMA
VAST_TARGET_SCHEMA="${VAST_TARGET_SCHEMA:-fraud_detection}"
printf "  ${PASS} Configuration noted\n"

# ── [4/6] Kafka Topics ──
echo ""
printf "${BOLD}[4/6] Kafka Topics${RESET}\n"
echo "  Topics must be created via VAST Admin UI (vastde CLI has dot-in-name issues)."
echo "  Go to: VAST Database → ${VAST_KAFKA_BUCKET} → Kafka-Compatible Broker Topics"
echo ""
for topic in fraud.transactions.raw fraud.transactions.scored fraud.alerts fraud.metrics; do
    # Check if topic exists by querying metadata
    if python3 -c "
from confluent_kafka.admin import AdminClient
a = AdminClient({'bootstrap.servers': '${VAST_BOOTSTRAP_SERVERS}'})
md = a.list_topics('${topic}', timeout=5)
if '${topic}' in md.topics:
    print('exists')
" 2>/dev/null | grep -q exists; then
        printf "  ${PASS} ${topic} exists\n"
    else
        printf "  ${FAIL} ${topic} — create it in VAST Admin UI\n"
    fi
done
echo ""
read -rp "  Press Enter once all topics are created... "

# ── [5/6] Blob Expansion ──
echo ""
printf "${BOLD}[5/6] Configuring Blob Expansion${RESET}\n"
PYTHONPATH=. python3 vast/blob_expansion_setup.py \
    --endpoint "$VAST_ENDPOINT" \
    --access-key "$VAST_ACCESS_KEY" \
    --secret-key "$VAST_SECRET_KEY" \
    --kafka-bucket "$VAST_KAFKA_BUCKET" \
    --kafka-schema "$VAST_KAFKA_SCHEMA" \
    --target-schema "$VAST_TARGET_SCHEMA" 2>&1 | while IFS= read -r line; do
    echo "  $line"
done

# ── [6/6] Save Configuration ──
echo ""
printf "${BOLD}[6/6] Saving Configuration${RESET}\n"

cat > .env << ENVEOF
# VAST Fraud Detection Demo Configuration
# Generated by setup.sh on $(date -u +%Y-%m-%dT%H:%M:%SZ)

# VAST Event Broker
VAST_BOOTSTRAP_SERVERS=${VAST_BOOTSTRAP_SERVERS}

# VAST VMS / S3 Credentials
VAST_ENDPOINT=${VAST_ENDPOINT}
VAST_ACCESS_KEY=${VAST_ACCESS_KEY}
VAST_SECRET_KEY=${VAST_SECRET_KEY}

# VAST Bucket & Schema
VAST_KAFKA_BUCKET=${VAST_KAFKA_BUCKET}
VAST_KAFKA_SCHEMA=${VAST_KAFKA_SCHEMA}
VAST_TARGET_SCHEMA=${VAST_TARGET_SCHEMA}

# Demo Settings
DEMO_TPS=1000
DEMO_DURATION=300
ENVEOF

printf "  ${PASS} .env written\n"

# Verify .gitignore
if ! grep -q "^\.env$" .gitignore 2>/dev/null; then
    echo ".env" >> .gitignore
    printf "  ${PASS} .env added to .gitignore\n"
else
    printf "  ${PASS} .env already in .gitignore\n"
fi

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║  Setup complete!                                    ║"
echo "╠══════════════════════════════════════════════════════╣"
echo "║  Run the demo:                                      ║"
echo "║    make demo          Full side-by-side demo        ║"
echo "║    make demo-vast     VAST pipeline only            ║"
echo "║    make demo-mode     Simulated data (no cluster)   ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
```

- [ ] **Step 2: Make executable**

```bash
chmod +x demos/setup.sh
```

- [ ] **Step 3: Commit**

```bash
git add demos/setup.sh
git commit -m "feat: add interactive setup wizard for first-time SE configuration"
```

---

### Task 6: Rewrite Makefile with All Targets

**Files:**
- Modify: `demos/Makefile`

- [ ] **Step 1: Replace the entire Makefile**

```makefile
.PHONY: setup demo demo-vast demo-kafka demo-mode generate generate-vast generate-kafka score setup-kafka status clean help

DEMO_TPS ?= 1000
DEMO_DURATION ?= 300

# Source .env if it exists
ifneq (,$(wildcard .env))
    include .env
    export
endif

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ── Setup ────────────────────────────────────────────

setup: ## First-time setup wizard (creates .env, validates cluster, configures blob expansion)
	@bash setup.sh

setup-kafka: ## Start Kafka Docker comparison stack
	@bash scripts/setup_kafka.sh

# ── Demo ─────────────────────────────────────────────

demo: ## Full side-by-side demo (VAST + Kafka + scorer + dashboard)
	@bash scripts/preflight.sh
	@bash scripts/run_demo.sh

demo-vast: ## VAST pipeline only (generator + scorer + dashboard)
	@bash scripts/preflight.sh
	@DEMO_KAFKA=false bash scripts/run_demo.sh

demo-kafka: ## Kafka pipeline only (Docker + generator + dashboard)
	@bash scripts/setup_kafka.sh
	@DEMO_VAST=false bash scripts/run_demo.sh

demo-mode: ## Dashboard with simulated data (no backends needed)
	@DEMO_MODE=true PYTHONPATH=. streamlit run dashboard/app.py --server.port 8501

# ── Individual Components ────────────────────────────

generate: ## Run transaction generator (TPS=5000 DURATION=60)
	@PYTHONPATH=. python3 -m generator.transaction_generator --target vast --tps $(DEMO_TPS) --duration $(DEMO_DURATION)

generate-vast: ## Generator against VAST Event Broker only
	@PYTHONPATH=. python3 -m generator.transaction_generator --target vast --tps $(DEMO_TPS) --duration $(DEMO_DURATION)

generate-kafka: ## Generator against local Kafka Docker only
	@PYTHONPATH=. python3 -m generator.transaction_generator --target kafka --tps $(DEMO_TPS) --duration $(DEMO_DURATION)

score: ## Start the standalone fraud scorer
	@PYTHONPATH=. python3 -u vast/fraud_scorer_pipeline/run_standalone.py

status: ## Show health of all demo components
	@bash scripts/status.sh

clean: ## Tear down Docker stack and stop all processes
	@bash scripts/teardown.sh

deploy-dataengine: ## Optional: deploy fraud scorer via VAST DataEngine CLI
	@echo "See demos/vast/fraud_scorer_pipeline/DEPLOYMENT.md for step-by-step guide"
```

- [ ] **Step 2: Commit**

```bash
git add demos/Makefile
git commit -m "feat: rewrite Makefile with full demo orchestration targets"
```

---

### Task 7: Rewrite `scripts/run_demo.sh` with Full Orchestration

**Files:**
- Modify: `demos/scripts/run_demo.sh`

- [ ] **Step 1: Replace the entire script**

```bash
#!/usr/bin/env bash
set -euo pipefail

# Source .env
if [[ -f .env ]]; then
    source .env
fi

VAST_BOOTSTRAP="${VAST_BOOTSTRAP_SERVERS:-}"
KAFKA_BOOTSTRAP="localhost:29092"
TPS="${DEMO_TPS:-1000}"
DURATION="${DEMO_DURATION:-300}"
DEMO_VAST="${DEMO_VAST:-true}"
DEMO_KAFKA="${DEMO_KAFKA:-true}"

PIDS=()

cleanup() {
    echo ""
    echo "Stopping demo..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait "${PIDS[@]}" 2>/dev/null || true
    echo "Demo stopped."
}
trap cleanup EXIT INT TERM

echo ""
echo "════════════════════════════════════════════"
echo "  VAST vs Kafka: Fraud Detection Demo"
echo "════════════════════════════════════════════"
echo ""

# ── Kafka Docker Stack ──
if [[ "$DEMO_KAFKA" == "true" ]]; then
    echo "[Kafka] Starting Docker Compose stack..."
    cd kafka && docker compose up -d 2>/dev/null && cd ..
    echo "[Kafka] Waiting for broker..."
    timeout 60 bash -c "until nc -zw2 localhost 29092 2>/dev/null; do sleep 2; done" || { echo "ERROR: Kafka failed to start"; exit 1; }
    echo "[Kafka] Ready"
fi

# ── Fraud Scorer ──
if [[ "$DEMO_VAST" == "true" && -n "$VAST_BOOTSTRAP" ]]; then
    echo "[Scorer] Starting standalone fraud scorer..."
    PYTHONPATH=. python3 -u vast/fraud_scorer_pipeline/run_standalone.py \
        --bootstrap-servers "$VAST_BOOTSTRAP" > /tmp/fraud-scorer.log 2>&1 &
    PIDS+=($!)
    echo "[Scorer] PID $! — consuming from fraud.transactions.raw"
fi

# ── Generators ──
if [[ "$DEMO_VAST" == "true" && -n "$VAST_BOOTSTRAP" ]]; then
    echo "[Generator] Starting VAST generator (${TPS} TPS, ${DURATION}s)..."
    PYTHONPATH=. python3 -u -m generator.transaction_generator \
        --target vast --tps "$TPS" --duration "$DURATION" > /tmp/gen-vast.log 2>&1 &
    PIDS+=($!)
fi

if [[ "$DEMO_KAFKA" == "true" ]]; then
    echo "[Generator] Starting Kafka generator (${TPS} TPS, ${DURATION}s)..."
    PYTHONPATH=. python3 -u -m generator.transaction_generator \
        --target kafka --tps "$TPS" --duration "$DURATION" > /tmp/gen-kafka.log 2>&1 &
    PIDS+=($!)
fi

# ── Dashboard ──
echo "[Dashboard] Starting Streamlit..."
DEMO_MODE=false PYTHONPATH=. streamlit run dashboard/app.py \
    --server.port 8501 --server.headless true > /tmp/dashboard.log 2>&1 &
PIDS+=($!)

echo ""
echo "════════════════════════════════════════════"
echo "  Demo running!"
echo ""
echo "  Dashboard:  http://localhost:8501"
echo "  Scorer log: tail -f /tmp/fraud-scorer.log"
echo "  VAST log:   tail -f /tmp/gen-vast.log"
echo "  Kafka log:  tail -f /tmp/gen-kafka.log"
echo ""
echo "  Press Ctrl+C to stop all."
echo "════════════════════════════════════════════"
echo ""

# Wait for any process to exit
wait "${PIDS[@]}" 2>/dev/null || true
```

- [ ] **Step 2: Make executable and commit**

```bash
chmod +x demos/scripts/run_demo.sh
git add demos/scripts/run_demo.sh
git commit -m "feat: rewrite run_demo.sh with full orchestration and PID management"
```

---

### Task 8: Update `dashboard/app.py` Default Mode

**Files:**
- Modify: `demos/dashboard/app.py`

- [ ] **Step 1: Change default DEMO_MODE to auto-detect .env**

Find the line:
```python
DEMO_MODE = os.getenv("DEMO_MODE", "true").lower() in ("true", "1", "yes")
```

Replace with:
```python
# Auto-detect: if .env exists with real credentials, default to live mode
_env_file_exists = os.path.exists(os.path.join(os.path.dirname(__file__), "..", ".env"))
_default_demo_mode = "false" if _env_file_exists else "true"
DEMO_MODE = os.getenv("DEMO_MODE", _default_demo_mode).lower() in ("true", "1", "yes")
```

- [ ] **Step 2: Commit**

```bash
git add demos/dashboard/app.py
git commit -m "feat: auto-detect live mode when .env exists"
```

---

### Task 9: Update README with New Quick Start

**Files:**
- Modify: `demos/README.md`

- [ ] **Step 1: Replace the Quick Start section**

Replace the current "Full Demo Setup" block with:

```markdown
### Full Demo Setup

```bash
# 1. Clone and install
git clone https://github.com/marlingod/VAST-Data-Streaming.git
cd VAST-Data-Streaming/demos
pip install -r requirements.txt

# 2. First-time setup (guided wizard)
./setup.sh

# 3. Run the demo
make demo
# Open http://localhost:8501
# Ctrl+C to stop
```

### Repeat Runs

```bash
cd VAST-Data-Streaming/demos
make demo        # Everything starts automatically
```
```

- [ ] **Step 2: Commit**

```bash
git add demos/README.md
git commit -m "docs: update README with setup.sh quick start flow"
```

---

### Task 10: Update Root README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update Quick Start to match new flow**

Replace the Quick Start section with:

```markdown
## Quick Start

```bash
cd demos
pip install -r requirements.txt

# Preview with simulated data (no cluster needed)
make demo-mode
# Open http://localhost:8501

# Full demo (requires VAST cluster)
./setup.sh        # First-time: guided wizard creates .env
make demo          # Starts everything — Ctrl+C to stop
```
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: update root README quick start"
```

---

### Task 11: Final Integration Test

- [ ] **Step 1: Verify `make demo-mode` works without .env**

```bash
cd demos
rm -f .env
make demo-mode
# Expected: Dashboard opens at localhost:8501 with simulated data
# Ctrl+C to stop
```

- [ ] **Step 2: Verify `make help` shows all targets**

```bash
make help
# Expected: setup, demo, demo-vast, demo-kafka, demo-mode, generate, generate-vast,
#           generate-kafka, score, setup-kafka, status, clean, deploy-dataengine, help
```

- [ ] **Step 3: Verify `make status` works**

```bash
make status
# Expected: Shows VAST broker status, Kafka Docker status, scorer status, dashboard status
```

- [ ] **Step 4: Verify preflight catches missing .env**

```bash
rm -f .env
make demo
# Expected: "✗ .env not found — run: ./setup.sh"
```

- [ ] **Step 5: Final commit and push**

```bash
git push origin main
```
