#!/usr/bin/env python3
"""
Standalone Fraud Scorer — runs as a Kafka consumer without DataEngine.

Consumes from fraud.transactions.raw, scores each transaction,
publishes to fraud.transactions.scored and fraud.alerts.

Usage:
    python run_standalone.py --bootstrap-servers 172.200.204.135:9092

This is a fallback for when DataEngine is not available, and useful
for testing the scoring logic independently.
"""

import argparse
import json
import os
import signal
import sys
import time

from confluent_kafka import Consumer, Producer

# Import scoring logic from the DataEngine function
sys.path.insert(0, os.path.dirname(__file__))
from main import score_transaction, ALERT_THRESHOLD, FRAUD_RING_MERCHANTS

_shutdown = False


def _handle_sigint(_sig, _frame):
    global _shutdown
    print("\n[INFO] Shutting down...")
    _shutdown = True


def run(bootstrap_servers, group_id="fraud-scorer-standalone"):
    """Consume from raw topic, score, publish to scored + alerts."""

    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe(["fraud.transactions.raw"])

    producer = Producer({
        "bootstrap.servers": bootstrap_servers,
        "message.timeout.ms": 30000,
        "linger.ms": 50,
        "batch.num.messages": 500,
    })

    print(f"[INFO] Fraud Scorer consuming from fraud.transactions.raw")
    print(f"[INFO] Bootstrap: {bootstrap_servers}")
    print(f"[INFO] Publishing to: fraud.transactions.scored, fraud.alerts")
    print(f"[INFO] Alert threshold: {ALERT_THRESHOLD}")
    print("-" * 60)

    recent_txns = []
    total_scored = 0
    total_alerts = 0
    start_time = time.monotonic()
    last_report = start_time

    while not _shutdown:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"[WARN] Consumer error: {msg.error()}")
            continue

        try:
            txn = json.loads(msg.value().decode())
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue

        if "transaction_id" not in txn:
            continue

        # Score
        t0 = time.perf_counter()
        risk_score, triggered_rules = score_transaction(
            txn, recent_txns=recent_txns, customer_avg_spend=txn.get("amount", 100)
        )
        latency_ms = round((time.perf_counter() - t0) * 1000, 2)

        # Build scored transaction
        scored_txn = {
            **txn,
            "risk_score": risk_score,
            "triggered_rules": triggered_rules,
            "scored_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
            "scoring_latency_ms": latency_ms,
        }

        # Publish scored transaction
        producer.produce(
            "fraud.transactions.scored",
            key=txn.get("card_id", "").encode("utf-8"),
            value=json.dumps(scored_txn).encode("utf-8"),
        )
        total_scored += 1

        # Publish alert if high risk
        if risk_score >= ALERT_THRESHOLD:
            alert = {
                "transaction_id": txn["transaction_id"],
                "card_id": txn.get("card_id"),
                "amount": txn.get("amount"),
                "risk_score": risk_score,
                "triggered_rules": triggered_rules,
                "fraud_type": triggered_rules[0] if triggered_rules else "unknown",
                "merchant_id": txn.get("merchant_id"),
                "location_city": txn.get("location_city"),
                "timestamp": txn.get("timestamp"),
                "alerted_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
            }
            producer.produce(
                "fraud.alerts",
                key=txn.get("card_id", "").encode("utf-8"),
                value=json.dumps(alert).encode("utf-8"),
            )
            total_alerts += 1

        producer.poll(0)

        # Update sliding window
        recent_txns.append(txn)
        if len(recent_txns) > 1000:
            recent_txns = recent_txns[-1000:]

        # Progress report every 5 seconds
        now = time.monotonic()
        if now - last_report >= 5.0:
            elapsed = now - start_time
            tps = total_scored / max(elapsed, 0.001)
            print(
                f"  Scored: {total_scored:>8,} | "
                f"Alerts: {total_alerts:>6,} | "
                f"TPS: {tps:>8,.1f} | "
                f"Alert rate: {total_alerts / max(total_scored, 1):.1%}"
            )
            last_report = now

    # Cleanup
    producer.flush(timeout=10)
    consumer.close()
    elapsed = time.monotonic() - start_time
    print("-" * 60)
    print(f"[INFO] Done. Scored {total_scored:,} transactions in {elapsed:.1f}s")
    print(f"[INFO] Alerts: {total_alerts:,} ({total_alerts / max(total_scored, 1):.1%})")


if __name__ == "__main__":
    signal.signal(signal.SIGINT, _handle_sigint)
    parser = argparse.ArgumentParser(description="Standalone fraud scorer")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "172.200.204.135:9092"),
    )
    parser.add_argument("--group-id", default="fraud-scorer-standalone")
    args = parser.parse_args()
    run(args.bootstrap_servers, args.group_id)
