#!/usr/bin/env bash
set -euo pipefail

VAST_BOOTSTRAP="${VAST_BOOTSTRAP_SERVERS:-vast:9092}"
KAFKA_BOOTSTRAP="localhost:29092"
TPS="${DEMO_TPS:-1000}"
DURATION="${DEMO_DURATION:-300}"

echo "============================================"
echo "  VAST vs Kafka: Fraud Detection Demo"
echo "============================================"
echo ""
echo "VAST Event Broker:  $VAST_BOOTSTRAP"
echo "Kafka Broker:       $KAFKA_BOOTSTRAP"
echo "TPS:                $TPS"
echo "Duration:           ${DURATION}s"
echo ""

# Start dashboard in background
echo "[1/3] Starting comparison dashboard..."
DEMO_MODE="${DEMO_MODE:-false}" streamlit run dashboard/app.py --server.port 8501 --server.headless true &
DASHBOARD_PID=$!
echo "  Dashboard: http://localhost:8501"

# Start generators
echo "[2/3] Starting transaction generators..."
echo "  -> VAST Event Broker"
python -m generator.transaction_generator --bootstrap-servers "$VAST_BOOTSTRAP" --tps "$TPS" --duration "$DURATION" --seed 42 &
VAST_GEN_PID=$!

echo "  -> Kafka"
python -m generator.transaction_generator --bootstrap-servers "$KAFKA_BOOTSTRAP" --tps "$TPS" --duration "$DURATION" --seed 42 &
KAFKA_GEN_PID=$!

echo ""
echo "[3/3] Demo running! Open http://localhost:8501"
echo ""
echo "Press Ctrl+C to stop..."

# Trap cleanup
cleanup() {
    echo ""
    echo "Stopping demo..."
    kill $DASHBOARD_PID $VAST_GEN_PID $KAFKA_GEN_PID 2>/dev/null || true
    echo "Demo stopped."
}
trap cleanup EXIT INT TERM

# Wait for generators to finish
wait $VAST_GEN_PID $KAFKA_GEN_PID 2>/dev/null || true
echo "Generators finished. Dashboard still running at http://localhost:8501"
echo "Press Ctrl+C to stop dashboard."
wait $DASHBOARD_PID 2>/dev/null || true
