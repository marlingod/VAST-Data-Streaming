#!/usr/bin/env bash
set -euo pipefail

# VAST Event Broker + DataEngine Setup
# Requires: VAST_ENDPOINT, VAST_ACCESS_KEY, VAST_SECRET_KEY environment variables

echo "=== VAST Fraud Detection Demo Setup ==="

# Check environment
if [[ -z "${VAST_ENDPOINT:-}" ]] || [[ -z "${VAST_ACCESS_KEY:-}" ]] || [[ -z "${VAST_SECRET_KEY:-}" ]]; then
    echo "ERROR: Set VAST_ENDPOINT, VAST_ACCESS_KEY, VAST_SECRET_KEY"
    exit 1
fi

echo "[1/4] Creating Event Broker topics..."
# Use dataengine-cli to create topics
vastde topics create fraud.transactions.raw --partitions 8 --replication-factor 1 2>/dev/null || echo "Topic fraud.transactions.raw already exists"
vastde topics create fraud.transactions.scored --partitions 8 --replication-factor 1 2>/dev/null || echo "Topic fraud.transactions.scored already exists"
vastde topics create fraud.alerts --partitions 4 --replication-factor 1 2>/dev/null || echo "Topic fraud.alerts already exists"
vastde topics create fraud.metrics --partitions 4 --replication-factor 1 2>/dev/null || echo "Topic fraud.metrics already exists"

echo "[2/4] Creating DataBase schema..."
python -c "from vast.setup import create_database_schema; create_database_schema()"

echo "[3/4] Loading historical data..."
python -c "from vast.setup import load_historical_data, load_fraud_ring_data, load_customer_profiles; load_historical_data(); load_fraud_ring_data(); load_customer_profiles()"

echo "[4/4] Deploying DataEngine fraud scorer function..."
cd vast/fraud_scorer
vastde functions create fraud-scorer --image fraud-scorer:latest --trigger fraud.transactions.raw 2>/dev/null || echo "Function fraud-scorer already exists"
cd ../..

echo "=== VAST setup complete ==="
