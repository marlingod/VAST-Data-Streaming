"""
Metrics collection for VAST vs Kafka fraud detection comparison dashboard.

Provides both a real MetricsCollector (backed by Kafka consumers) and a
DemoMetricsCollector that generates plausible simulated data.
"""

from __future__ import annotations

import math
import random
import time
from abc import ABC, abstractmethod
from collections import deque
from datetime import datetime, timezone
from typing import Any

# ---------------------------------------------------------------------------
# Fraud types used across the demo
# ---------------------------------------------------------------------------
FRAUD_TYPES = [
    "Card-Not-Present",
    "Account Takeover",
    "Synthetic Identity",
    "Geographic Impossibility",
    "Velocity Abuse",
    "BIN Attack",
    "Credential Stuffing",
    "Refund Fraud",
]

# ---------------------------------------------------------------------------
# Named queries for the SQL comparison panel
# ---------------------------------------------------------------------------
NAMED_QUERIES: dict[str, str] = {
    "Spending Anomaly": (
        "SELECT card_id, amount, merchant, ts\n"
        "  FROM transactions\n"
        " WHERE amount > 3 * avg_spend_30d\n"
        "   AND ts >= NOW() - INTERVAL '5 minutes'\n"
        " ORDER BY amount DESC\n"
        " LIMIT 50;"
    ),
    "Geographic Impossibility": (
        "SELECT a.card_id, a.city AS city_a, b.city AS city_b,\n"
        "       a.ts AS ts_a, b.ts AS ts_b,\n"
        "       geo_distance_km(a.lat, a.lon, b.lat, b.lon) AS dist_km\n"
        "  FROM transactions a\n"
        "  JOIN transactions b\n"
        "    ON a.card_id = b.card_id\n"
        "   AND b.ts BETWEEN a.ts AND a.ts + INTERVAL '30 minutes'\n"
        " WHERE geo_distance_km(a.lat, a.lon, b.lat, b.lon) > 500;"
    ),
    "Fraud Ring Activity": (
        "SELECT merchant_id, COUNT(DISTINCT card_id) AS cards,\n"
        "       SUM(amount) AS total, AVG(risk_score) AS avg_risk\n"
        "  FROM transactions\n"
        " WHERE ts >= NOW() - INTERVAL '1 hour'\n"
        " GROUP BY merchant_id\n"
        "HAVING COUNT(DISTINCT card_id) > 20\n"
        "   AND AVG(risk_score) > 0.75\n"
        " ORDER BY total DESC;"
    ),
    "Time Travel (6 months)": (
        "SELECT card_id, COUNT(*) AS txn_count,\n"
        "       SUM(amount) AS total_amount,\n"
        "       MIN(ts) AS first_txn, MAX(ts) AS last_txn\n"
        "  FROM transactions\n"
        " WHERE ts >= NOW() - INTERVAL '6 months'\n"
        "   AND fraud_label = TRUE\n"
        " GROUP BY card_id\n"
        " ORDER BY total_amount DESC\n"
        " LIMIT 100;"
    ),
}


class MetricsCollectorBase(ABC):
    """Abstract interface for metrics collection."""

    @abstractmethod
    def collect_latency(self) -> dict[str, dict[str, float]]:
        """Return p50/p95/p99 latencies for VAST and Kafka (ms)."""

    @abstractmethod
    def collect_throughput(self) -> dict[str, float]:
        """Return msgs/sec for VAST and Kafka."""

    @abstractmethod
    def collect_alerts(self) -> list[dict[str, Any]]:
        """Return a list of recent fraud alerts."""

    @abstractmethod
    def collect_query_results(self, query_name: str) -> dict[str, Any]:
        """Run a named query on both backends and return timing comparison."""


# ---------------------------------------------------------------------------
# Real collector (Kafka-consumer backed)
# ---------------------------------------------------------------------------

class MetricsCollector(MetricsCollectorBase):
    """Collect metrics from live VAST Event Broker and Kafka clusters."""

    def __init__(self, vast_bootstrap: str, kafka_bootstrap: str):
        self.vast_bootstrap = vast_bootstrap
        self.kafka_bootstrap = kafka_bootstrap
        # In a real implementation these would be confluent_kafka.Consumer
        # instances subscribed to `fraud.metrics` on each cluster.
        self._vast_consumer = None
        self._kafka_consumer = None

    # -- helpers (stubs for the real implementation) -------------------------

    def _init_consumers(self):
        """Lazily initialise Kafka consumers."""
        try:
            from confluent_kafka import Consumer  # noqa: F401

            if self._vast_consumer is None:
                self._vast_consumer = Consumer({
                    "bootstrap.servers": self.vast_bootstrap,
                    "group.id": "dashboard-vast",
                    "auto.offset.reset": "latest",
                })
                self._vast_consumer.subscribe(["fraud.metrics"])

            if self._kafka_consumer is None:
                self._kafka_consumer = Consumer({
                    "bootstrap.servers": self.kafka_bootstrap,
                    "group.id": "dashboard-kafka",
                    "auto.offset.reset": "latest",
                })
                self._kafka_consumer.subscribe(["fraud.metrics"])
        except ImportError:
            pass

    # -- public API ----------------------------------------------------------

    def collect_latency(self) -> dict[str, dict[str, float]]:
        self._init_consumers()
        # Placeholder: would parse consumed metrics messages
        return {
            "vast":  {"p50": 0.0, "p95": 0.0, "p99": 0.0},
            "kafka": {"p50": 0.0, "p95": 0.0, "p99": 0.0},
        }

    def collect_throughput(self) -> dict[str, float]:
        self._init_consumers()
        return {"vast": 0.0, "kafka": 0.0}

    def collect_alerts(self) -> list[dict[str, Any]]:
        self._init_consumers()
        return []

    def collect_query_results(self, query_name: str) -> dict[str, Any]:
        query_sql = NAMED_QUERIES.get(query_name, "SELECT 1;")
        return {
            "query": query_sql,
            "vast":  {"rows": 0, "latency_ms": 0.0},
            "kafka": {"rows": 0, "latency_ms": 0.0},
        }


# ---------------------------------------------------------------------------
# Demo (simulated) collector
# ---------------------------------------------------------------------------

class DemoMetricsCollector(MetricsCollectorBase):
    """Generate simulated metrics that highlight VAST advantages."""

    def __init__(self):
        self._alert_history: deque[dict[str, Any]] = deque(maxlen=200)
        self._latency_history_vast: deque[dict[str, float]] = deque(maxlen=120)
        self._latency_history_kafka: deque[dict[str, float]] = deque(maxlen=120)
        self._start = time.time()
        self._alert_counter = 0

    # -- latency -------------------------------------------------------------

    def collect_latency(self) -> dict[str, dict[str, float]]:
        vast = {
            "p50": round(random.uniform(0.10, 0.35), 3),
            "p95": round(random.uniform(0.35, 0.60), 3),
            "p99": round(random.uniform(0.55, 0.80), 3),
        }
        kafka = {
            "p50": round(random.uniform(2.0, 5.0), 2),
            "p95": round(random.uniform(5.0, 10.0), 2),
            "p99": round(random.uniform(8.0, 15.0), 2),
        }
        self._latency_history_vast.append(vast)
        self._latency_history_kafka.append(kafka)
        return {"vast": vast, "kafka": kafka}

    def get_latency_history(self):
        return list(self._latency_history_vast), list(self._latency_history_kafka)

    # -- throughput ----------------------------------------------------------

    def collect_throughput(self) -> dict[str, float]:
        vast_tps = round(random.gauss(50_000, 3_000), 0)
        kafka_tps = round(random.gauss(8_000, 800), 0)
        return {"vast": max(vast_tps, 30_000), "kafka": max(kafka_tps, 4_000)}

    # -- alerts --------------------------------------------------------------

    def collect_alerts(self) -> list[dict[str, Any]]:
        # Generate 0-3 new alerts each call
        now = datetime.now(timezone.utc)
        num_new = random.choices([0, 1, 2, 3], weights=[30, 40, 20, 10])[0]

        for _ in range(num_new):
            self._alert_counter += 1
            card_suffix = random.randint(1000, 9999)
            amount = round(random.uniform(150, 25_000), 2)
            fraud_type = random.choice(FRAUD_TYPES)
            risk = round(random.uniform(0.72, 0.99), 2)
            vast_latency = round(random.uniform(0.1, 0.6), 2)
            kafka_latency = round(random.uniform(3.0, 18.0), 2)

            detected_by = "VAST" if random.random() < 0.92 else "Kafka"

            alert = {
                "id": self._alert_counter,
                "timestamp": now.isoformat(),
                "card_id": f"****-****-****-{card_suffix}",
                "amount": amount,
                "fraud_type": fraud_type,
                "risk_score": risk,
                "detected_by": detected_by,
                "vast_latency_ms": vast_latency,
                "kafka_latency_ms": kafka_latency,
            }
            self._alert_history.appendleft(alert)

        return list(self._alert_history)

    # -- query comparison ----------------------------------------------------

    def collect_query_results(self, query_name: str) -> dict[str, Any]:
        query_sql = NAMED_QUERIES.get(query_name, "SELECT 1;")

        # Simulate row counts and latencies
        base_rows = random.randint(12, 480)

        is_historical = "6 months" in query_name
        vast_latency = round(random.uniform(0.2, 0.8), 2)
        if is_historical:
            vast_latency = round(random.uniform(0.8, 2.5), 2)

        # Kafka/ClickHouse pipeline adds significant overhead
        kafka_latency = round(random.uniform(180, 900), 1)
        if is_historical:
            kafka_latency = round(random.uniform(1_800, 4_500), 1)

        return {
            "query_name": query_name,
            "query": query_sql,
            "vast": {"rows": base_rows, "latency_ms": vast_latency},
            "kafka": {"rows": base_rows, "latency_ms": kafka_latency},
        }
