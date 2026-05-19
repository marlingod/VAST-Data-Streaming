"""
Faust-based Fraud Scoring Stream Processor
============================================
Consumes raw transactions from Kafka, applies fraud detection rules, and
publishes scored results back to Kafka topics.

Architecture complexity note:
  This single file must coordinate with THREE separate backend systems:
    1. Kafka          — for message transport (input + output topics)
    2. RocksDB        — for local windowed state (velocity / location tables)
    3. ClickHouse     — for historical queries (amount anomaly, fraud ring refresh)

  In the VAST architecture, all three roles are served by one platform.
"""

import os
import time
import json
import logging
import threading
from datetime import timedelta

import faust
import requests

from rules import (
    ALERT_THRESHOLD,
    check_velocity,
    check_geographic_impossibility,
    check_amount_anomaly,
    check_card_testing,
    check_fraud_ring,
    compute_risk_score,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration — pulled from environment so docker-compose can override
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))

# ---------------------------------------------------------------------------
# Faust App
# ---------------------------------------------------------------------------
app = faust.App(
    "fraud-scorer",
    broker=f"kafka://{KAFKA_BOOTSTRAP}",
    store="rocksdb://",       # RocksDB for local windowed state
    topic_partitions=3,
    # Processing guarantee — note: exactly-once adds significant overhead
    processing_guarantee="at_least_once",
)

# ---------------------------------------------------------------------------
# Faust Records (schemas)
# ---------------------------------------------------------------------------

class RawTransaction(faust.Record, serializer="json"):
    """Incoming transaction from the raw topic."""
    transaction_id: str = ""
    timestamp: float = 0.0
    card_id: str = ""
    customer_id: str = ""
    merchant_id: str = ""
    merchant_category: str = ""
    amount: float = 0.0
    currency: str = "USD"
    location_lat: float = 0.0
    location_lon: float = 0.0
    location_city: str = ""
    device_fingerprint: str = ""
    channel: str = ""
    is_fraud: int = 0


class ScoredTransaction(faust.Record, serializer="json"):
    """Output: transaction + risk score + rule breakdown."""
    transaction_id: str = ""
    timestamp: float = 0.0
    card_id: str = ""
    customer_id: str = ""
    merchant_id: str = ""
    merchant_category: str = ""
    amount: float = 0.0
    currency: str = "USD"
    location_lat: float = 0.0
    location_lon: float = 0.0
    location_city: str = ""
    risk_score: float = 0.0
    rule_scores: dict = None
    is_alert: bool = False
    processing_latency_ms: float = 0.0


class FraudAlert(faust.Record, serializer="json"):
    """High-risk alert published to the alerts topic."""
    transaction_id: str = ""
    card_id: str = ""
    risk_score: float = 0.0
    rule_scores: dict = None
    amount: float = 0.0
    merchant_id: str = ""
    timestamp: float = 0.0


class ProcessingMetric(faust.Record, serializer="json"):
    """Latency / throughput metric published to the metrics topic."""
    transaction_id: str = ""
    processing_latency_ms: float = 0.0
    timestamp: float = 0.0
    rules_applied: int = 0

# ---------------------------------------------------------------------------
# Topics
# ---------------------------------------------------------------------------
raw_topic = app.topic("fraud.transactions.raw", value_type=RawTransaction)
scored_topic = app.topic("fraud.transactions.scored", value_type=ScoredTransaction)
alerts_topic = app.topic("fraud.alerts", value_type=FraudAlert)
metrics_topic = app.topic("fraud.metrics", value_type=ProcessingMetric)

# ---------------------------------------------------------------------------
# Faust Tables — windowed state backed by RocksDB
# ---------------------------------------------------------------------------

# Tumbling window (60 s) counting transactions per card
card_velocity_table = app.Table(
    "card_velocity",
    default=int,
    partitions=3,
).tumbling(timedelta(seconds=60), expires=timedelta(minutes=10))

# Hopping window tracking the last known location per card
# Stores {"lat": float, "lon": float, "timestamp": float}
card_location_table = app.Table(
    "card_location",
    default=dict,
    partitions=3,
).hopping(
    size=timedelta(minutes=5),
    step=timedelta(seconds=30),
    expires=timedelta(minutes=30),
)

# ---------------------------------------------------------------------------
# Fraud ring set — refreshed periodically from ClickHouse
# This is yet another cross-system dependency that must be kept alive.
# ---------------------------------------------------------------------------
fraud_ring_merchants: set = set()
_fraud_ring_lock = threading.Lock()


def _refresh_fraud_ring_set():
    """
    Fetch the current fraud ring merchant list from ClickHouse.

    Note: if ClickHouse is down, we operate with a stale (or empty) set.
    In the VAST architecture this query runs against the same platform,
    eliminating this failure mode entirely.
    """
    global fraud_ring_merchants
    query = "SELECT merchant_id FROM fraud_ring_merchants FORMAT JSONEachRow"
    try:
        resp = requests.get(
            f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/",
            params={"query": query},
            timeout=5.0,
        )
        resp.raise_for_status()
        new_set = set()
        for line in resp.text.strip().split("\n"):
            if line:
                row = json.loads(line)
                new_set.add(row.get("merchant_id", ""))
        with _fraud_ring_lock:
            fraud_ring_merchants = new_set
        logger.info("Refreshed fraud ring set: %d merchants", len(new_set))
    except Exception as exc:
        logger.warning("Failed to refresh fraud ring set from ClickHouse: %s", exc)


@app.timer(interval=60.0)
async def refresh_fraud_rings(app):
    """Periodic timer to refresh the fraud ring merchant set from ClickHouse."""
    _refresh_fraud_ring_set()


# ---------------------------------------------------------------------------
# Main Processing Agent
# ---------------------------------------------------------------------------

@app.agent(raw_topic)
async def score_transactions(stream):
    """
    Core fraud scoring agent — consumes raw transactions and applies all rules.

    Processing flow per transaction:
      1. Update velocity table (local RocksDB)
      2. Run velocity check (local state)
      3. Run geographic impossibility check (local state)
      4. Run amount anomaly check (** HTTP call to ClickHouse **)
      5. Run card testing check (local state)
      6. Run fraud ring check (in-memory set refreshed from ClickHouse)
      7. Compute weighted risk score
      8. Publish scored transaction to output topic
      9. If score > threshold, publish alert
     10. Publish latency metric

    Steps 4 and 6 demonstrate the cross-system complexity: each transaction
    requires network calls to a separate analytical database.
    """
    async for event in stream:
        start_time = time.perf_counter()

        # --- Update velocity state ---
        card_velocity_table[event.card_id] += 1

        # --- Update location state ---
        card_location_table[event.card_id] = {
            "lat": event.location_lat,
            "lon": event.location_lon,
            "timestamp": event.timestamp,
        }

        # --- Build transaction dict for rule functions ---
        txn_dict = {
            "card_id": event.card_id,
            "merchant_id": event.merchant_id,
            "amount": event.amount,
            "location_lat": event.location_lat,
            "location_lon": event.location_lon,
            "timestamp": event.timestamp,
        }

        # --- Apply fraud detection rules ---
        rule_scores = {}

        # Rule 1: Velocity (local state only)
        rule_scores["velocity"] = check_velocity(
            event.card_id, card_velocity_table
        )

        # Rule 2: Geographic impossibility (local state only)
        rule_scores["geographic"] = check_geographic_impossibility(
            txn_dict, card_location_table
        )

        # Rule 3: Amount anomaly (** REQUIRES CLICKHOUSE HTTP CALL **)
        # This is where latency spikes — every transaction triggers a query
        # to a separate database over the network.
        rule_scores["amount_anomaly"] = check_amount_anomaly(
            txn_dict,
            clickhouse_host=CLICKHOUSE_HOST,
            clickhouse_port=CLICKHOUSE_PORT,
        )

        # Rule 4: Card testing (local state)
        rule_scores["card_testing"] = check_card_testing(
            event.card_id, card_velocity_table
        )

        # Rule 5: Fraud ring (in-memory set, periodically refreshed from CH)
        with _fraud_ring_lock:
            ring_set = fraud_ring_merchants.copy()
        rule_scores["fraud_ring"] = check_fraud_ring(
            event.merchant_id, ring_set
        )

        # --- Compute composite risk score ---
        risk_score = compute_risk_score(rule_scores)
        is_alert = risk_score > ALERT_THRESHOLD

        elapsed_ms = (time.perf_counter() - start_time) * 1000.0

        # --- Publish scored transaction ---
        scored = ScoredTransaction(
            transaction_id=event.transaction_id,
            timestamp=event.timestamp,
            card_id=event.card_id,
            customer_id=event.customer_id,
            merchant_id=event.merchant_id,
            merchant_category=event.merchant_category,
            amount=event.amount,
            currency=event.currency,
            location_lat=event.location_lat,
            location_lon=event.location_lon,
            location_city=event.location_city,
            risk_score=risk_score,
            rule_scores=rule_scores,
            is_alert=is_alert,
            processing_latency_ms=elapsed_ms,
        )
        await scored_topic.send(value=scored)

        # --- Publish alert if score exceeds threshold ---
        if is_alert:
            alert = FraudAlert(
                transaction_id=event.transaction_id,
                card_id=event.card_id,
                risk_score=risk_score,
                rule_scores=rule_scores,
                amount=event.amount,
                merchant_id=event.merchant_id,
                timestamp=event.timestamp,
            )
            await alerts_topic.send(value=alert)
            logger.warning(
                "FRAUD ALERT: txn=%s card=%s score=%.3f amount=%.2f",
                event.transaction_id,
                event.card_id,
                risk_score,
                event.amount,
            )

        # --- Publish processing metric ---
        metric = ProcessingMetric(
            transaction_id=event.transaction_id,
            processing_latency_ms=elapsed_ms,
            timestamp=time.time(),
            rules_applied=len(rule_scores),
        )
        await metrics_topic.send(value=metric)

        logger.info(
            "Scored txn=%s score=%.3f latency=%.1fms alert=%s",
            event.transaction_id,
            risk_score,
            elapsed_ms,
            is_alert,
        )
