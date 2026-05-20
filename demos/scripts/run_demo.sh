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
