#!/usr/bin/env bash
set -euo pipefail

echo "=== Tearing down Fraud Detection Demo ==="

echo "[1/2] Stopping Kafka ecosystem..."
cd kafka
docker compose down -v 2>/dev/null || echo "  No Kafka containers running"
cd ..

echo "[2/2] Cleaning up VAST resources..."
if [[ -n "${VAST_ENDPOINT:-}" ]]; then
    echo "  Removing Event Broker topics..."
    vastde topics delete fraud.transactions.raw 2>/dev/null || true
    vastde topics delete fraud.transactions.scored 2>/dev/null || true
    vastde topics delete fraud.alerts 2>/dev/null || true
    vastde topics delete fraud.metrics 2>/dev/null || true
    echo "  Removing DataEngine function..."
    vastde functions delete fraud-scorer 2>/dev/null || true
else
    echo "  VAST_ENDPOINT not set — skipping VAST cleanup"
fi

echo "=== Teardown complete ==="
