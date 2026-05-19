"""
Configuration constants for the VAST Data fraud-detection demo.

Adjust these defaults when running the transaction generator in different
environments (local Docker Compose, VAST cluster, cloud, etc.).
"""

import os

# ---------------------------------------------------------------------------
# Kafka broker connection
# ---------------------------------------------------------------------------
# VAST Event Broker (Kafka-compatible endpoint)
VAST_BOOTSTRAP_SERVERS = os.getenv("VAST_BOOTSTRAP_SERVERS", "172.200.204.135:9092")

# Local Kafka (Docker Compose)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")

# Default target — set via CLI --bootstrap-servers flag
DEFAULT_BOOTSTRAP_SERVERS = VAST_BOOTSTRAP_SERVERS

# ---------------------------------------------------------------------------
# Generator tuning knobs
# ---------------------------------------------------------------------------
DEFAULT_TPS = 1000          # target transactions per second
DEFAULT_DURATION = 300      # total run time in seconds
DEFAULT_FRAUD_RATE = 0.17   # 17 % of transactions are fraudulent
DEFAULT_CUSTOMERS = 10000   # size of the synthetic customer pool
DEFAULT_SEED = 42           # reproducible randomness

# ---------------------------------------------------------------------------
# Kafka topic names
# ---------------------------------------------------------------------------
TOPIC_RAW = "fraud.transactions.raw"
TOPIC_SCORED = "fraud.transactions.scored"
TOPIC_ALERTS = "fraud.alerts"
TOPIC_METRICS = "fraud.metrics"
