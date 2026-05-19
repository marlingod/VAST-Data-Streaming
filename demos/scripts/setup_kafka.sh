#!/usr/bin/env bash
set -euo pipefail

echo "=== Kafka Fraud Detection Demo Setup ==="

echo "[1/3] Starting Kafka ecosystem (Docker Compose)..."
cd kafka
docker compose up -d
cd ..

echo "[2/3] Waiting for Kafka to be ready..."
echo "  Waiting for Kafka broker..."
timeout 60 bash -c 'until docker compose -f kafka/docker-compose.yml exec -T kafka kafka-broker-api-versions --bootstrap-server kafka:9092 >/dev/null 2>&1; do sleep 2; done' || { echo "ERROR: Kafka failed to start"; exit 1; }

echo "  Creating topics..."
docker compose -f kafka/docker-compose.yml exec -T kafka kafka-topics --bootstrap-server kafka:9092 --create --topic fraud.transactions.raw --partitions 3 --replication-factor 1 --if-not-exists
docker compose -f kafka/docker-compose.yml exec -T kafka kafka-topics --bootstrap-server kafka:9092 --create --topic fraud.transactions.scored --partitions 3 --replication-factor 1 --if-not-exists
docker compose -f kafka/docker-compose.yml exec -T kafka kafka-topics --bootstrap-server kafka:9092 --create --topic fraud.alerts --partitions 3 --replication-factor 1 --if-not-exists
docker compose -f kafka/docker-compose.yml exec -T kafka kafka-topics --bootstrap-server kafka:9092 --create --topic fraud.metrics --partitions 3 --replication-factor 1 --if-not-exists

echo "[3/3] Waiting for ClickHouse..."
timeout 30 bash -c 'until curl -s http://localhost:8123/ping >/dev/null 2>&1; do sleep 2; done' || { echo "ERROR: ClickHouse failed to start"; exit 1; }
echo "  ClickHouse ready (schema auto-loaded from init script)"

echo "=== Kafka setup complete ==="
echo ""
echo "Services running:"
echo "  Kafka broker:     localhost:29092"
echo "  Schema Registry:  localhost:8081"
echo "  ClickHouse HTTP:  localhost:8123"
echo "  MinIO Console:    localhost:9001 (minioadmin/minioadmin)"
