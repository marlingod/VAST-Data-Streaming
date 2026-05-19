"""
VAST DataEngine serverless function for real-time fraud scoring.

Deployed as a container function via ``dataengine-cli``. The function
subscribes to the ``fraud.transactions.raw`` Event Broker topic,
scores each transaction against the fraud rule-set, and publishes
results to downstream topics.

Output topics
-------------
* ``fraud.transactions.scored`` -- every transaction with risk metadata
* ``fraud.alerts``              -- high-risk transactions (score > ALERT_THRESHOLD)
* ``fraud.metrics``             -- per-transaction processing latency
"""

import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta

import vastdb
from confluent_kafka import Producer

from rules import (
    ALERT_THRESHOLD,
    check_amount_anomaly,
    check_card_testing,
    check_fraud_ring,
    check_geographic_impossibility,
    check_velocity,
    compute_risk_score,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("fraud_scorer")

# ---------------------------------------------------------------------------
# VAST DataBase connection
# ---------------------------------------------------------------------------
session = vastdb.connect(
    endpoint=os.environ["VAST_ENDPOINT"],
    access_key=os.environ["VAST_ACCESS_KEY"],
    secret_key=os.environ["VAST_SECRET_KEY"],
)

BUCKET = os.environ.get("VAST_BUCKET", "fraud-detection")
SCHEMA = os.environ.get("VAST_SCHEMA", "fraud")

# ---------------------------------------------------------------------------
# Kafka producer for downstream topics
# ---------------------------------------------------------------------------
_kafka_conf = {
    "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "client.id": "fraud-scorer",
}
producer = Producer(_kafka_conf)

TOPIC_SCORED = "fraud.transactions.scored"
TOPIC_ALERTS = "fraud.alerts"
TOPIC_METRICS = "fraud.metrics"

# ---------------------------------------------------------------------------
# VAST query helpers
# ---------------------------------------------------------------------------

def _fetch_recent_transactions(card_id: str, window_minutes: int = 5) -> list[dict]:
    """Query VAST DataBase for recent transactions on the given card."""
    try:
        with session.transaction() as tx:
            table = tx.bucket(BUCKET).schema(SCHEMA).table("transactions")
            cutoff = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
            reader = table.select(
                columns=["card_id", "timestamp", "amount", "latitude", "longitude", "merchant_id"],
                predicate=f"card_id = '{card_id}' AND timestamp >= '{cutoff.isoformat()}'",
            )
            pa_table = reader.read_all()
            return pa_table.to_pylist()
    except Exception:
        logger.exception("Failed to fetch recent transactions for card %s", card_id)
        return []


def _fetch_customer_history(card_id: str) -> dict:
    """Query VAST DataBase for the customer's historical spending profile."""
    try:
        with session.transaction() as tx:
            table = tx.bucket(BUCKET).schema(SCHEMA).table("customer_profiles")
            reader = table.select(
                columns=["card_id", "avg_amount", "home_latitude", "home_longitude"],
                predicate=f"card_id = '{card_id}'",
            )
            pa_table = reader.read_all()
            rows = pa_table.to_pylist()
            if rows:
                return rows[0]
            return {}
    except Exception:
        logger.exception("Failed to fetch customer history for card %s", card_id)
        return {}


def _fetch_fraud_ring_merchants() -> set:
    """Load the set of known fraud ring merchant IDs from VAST DataBase."""
    try:
        with session.transaction() as tx:
            table = tx.bucket(BUCKET).schema(SCHEMA).table("fraud_ring_merchants")
            reader = table.select(columns=["merchant_id"])
            pa_table = reader.read_all()
            return set(pa_table.column("merchant_id").to_pylist())
    except Exception:
        logger.exception("Failed to fetch fraud ring merchants")
        return set()


# Pre-load fraud ring data (refreshed periodically in production)
_fraud_ring_merchants: set = _fetch_fraud_ring_merchants()

# ---------------------------------------------------------------------------
# Kafka delivery callback
# ---------------------------------------------------------------------------

def _delivery_callback(err, msg):
    if err is not None:
        logger.error("Kafka delivery failed for topic %s: %s", msg.topic(), err)
    else:
        logger.debug("Delivered to %s [%d] @ %d", msg.topic(), msg.partition(), msg.offset())

# ---------------------------------------------------------------------------
# Main entry point -- invoked by VAST DataEngine
# ---------------------------------------------------------------------------

def handle(event: dict) -> dict:
    """
    Score a single transaction and publish results.

    Parameters
    ----------
    event : dict
        Raw transaction payload from the ``fraud.transactions.raw`` topic.
        Expected keys: transaction_id, card_id, amount, timestamp,
        latitude, longitude, merchant_id, merchant_category, ...

    Returns
    -------
    dict
        The scored transaction with risk metadata appended.
    """
    t_start = time.perf_counter()

    transaction = event if isinstance(event, dict) else json.loads(event)
    card_id = transaction.get("card_id", "unknown")
    logger.info(
        "Scoring transaction %s for card %s",
        transaction.get("transaction_id"),
        card_id,
    )

    # ---- Gather context from VAST DataBase --------------------------------
    recent_transactions = _fetch_recent_transactions(card_id)
    customer_history = _fetch_customer_history(card_id)

    # ---- Apply fraud rules ------------------------------------------------
    scores = {
        "velocity": check_velocity(transaction, recent_transactions),
        "geo": check_geographic_impossibility(transaction, recent_transactions),
        "amount": check_amount_anomaly(transaction, customer_history),
        "card_testing": check_card_testing(transaction, recent_transactions),
        "fraud_ring": check_fraud_ring(transaction, _fraud_ring_merchants),
    }
    risk_score = compute_risk_score(scores)

    # Build the list of rules that contributed to the score
    triggered_rules = [name for name, score in scores.items() if score > 0.0]

    # ---- Enrich original transaction --------------------------------------
    scored_transaction = {
        **transaction,
        "risk_score": risk_score,
        "rule_scores": scores,
        "triggered_rules": triggered_rules,
        "scored_at": datetime.now(timezone.utc).isoformat(),
    }

    # ---- Publish to scored topic ------------------------------------------
    producer.produce(
        TOPIC_SCORED,
        key=card_id,
        value=json.dumps(scored_transaction),
        callback=_delivery_callback,
    )

    # ---- Publish alert if threshold exceeded ------------------------------
    if risk_score > ALERT_THRESHOLD:
        alert_payload = {
            "transaction_id": transaction.get("transaction_id"),
            "card_id": card_id,
            "risk_score": risk_score,
            "triggered_rules": triggered_rules,
            "rule_scores": scores,
            "amount": transaction.get("amount"),
            "merchant_id": transaction.get("merchant_id"),
            "timestamp": transaction.get("timestamp"),
            "alerted_at": datetime.now(timezone.utc).isoformat(),
        }
        producer.produce(
            TOPIC_ALERTS,
            key=card_id,
            value=json.dumps(alert_payload),
            callback=_delivery_callback,
        )
        logger.warning(
            "ALERT: transaction %s scored %.4f (rules: %s)",
            transaction.get("transaction_id"),
            risk_score,
            ", ".join(triggered_rules),
        )

    # ---- Record processing latency ----------------------------------------
    latency_ms = (time.perf_counter() - t_start) * 1000
    metrics_payload = {
        "transaction_id": transaction.get("transaction_id"),
        "card_id": card_id,
        "risk_score": risk_score,
        "latency_ms": round(latency_ms, 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    producer.produce(
        TOPIC_METRICS,
        key=card_id,
        value=json.dumps(metrics_payload),
        callback=_delivery_callback,
    )

    # Flush to ensure all messages are delivered
    producer.flush(timeout=5)

    logger.info(
        "Scored transaction %s: risk=%.4f, latency=%.2fms",
        transaction.get("transaction_id"),
        risk_score,
        latency_ms,
    )
    return scored_transaction


# ---------------------------------------------------------------------------
# Standalone execution (for local testing / container CMD)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    from confluent_kafka import Consumer

    consumer_conf = {
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "group.id": "fraud-scorer-group",
        "auto.offset.reset": "latest",
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe(["fraud.transactions.raw"])
    logger.info("Fraud scorer started -- listening on fraud.transactions.raw")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue
            try:
                event = json.loads(msg.value().decode("utf-8"))
                handle(event)
            except Exception:
                logger.exception("Failed to process message at offset %s", msg.offset())
    except KeyboardInterrupt:
        logger.info("Shutting down fraud scorer")
    finally:
        consumer.close()
        producer.flush(timeout=10)
