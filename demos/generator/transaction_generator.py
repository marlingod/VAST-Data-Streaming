#!/usr/bin/env python3
"""
Transaction Generator for the VAST Data fraud-detection demo.

Produces a realistic stream of credit-card transactions -- a configurable
mix of legitimate and fraudulent -- and publishes them as JSON to Kafka.

Usage examples
--------------
  # Default settings (1 000 TPS, 17 % fraud, 5 min run)
  python -m demos.generator.transaction_generator

  # Lower throughput, higher fraud rate, 60-second burst
  python -m demos.generator.transaction_generator \\
      --tps 200 --fraud-rate 0.30 --duration 60

  # Force only velocity-attack fraud
  python -m demos.generator.transaction_generator --inject velocity
"""

from __future__ import annotations

import argparse
import json
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

try:
    from demos.generator.config import (
        DEFAULT_BOOTSTRAP_SERVERS, DEFAULT_CUSTOMERS, DEFAULT_DURATION,
        DEFAULT_FRAUD_RATE, DEFAULT_SEED, DEFAULT_TPS,
        KAFKA_BOOTSTRAP_SERVERS, TOPIC_METRICS, TOPIC_RAW,
        VAST_BOOTSTRAP_SERVERS,
    )
    from demos.generator.fraud_patterns import (
        CHANNELS, MERCHANT_CATEGORIES, MERCHANT_CATEGORY_MAP,
        PATTERN_REGISTRY, CustomerPool, random_fraud_pattern,
    )
except ImportError:
    from generator.config import (
        DEFAULT_BOOTSTRAP_SERVERS, DEFAULT_CUSTOMERS, DEFAULT_DURATION,
        DEFAULT_FRAUD_RATE, DEFAULT_SEED, DEFAULT_TPS,
        KAFKA_BOOTSTRAP_SERVERS, TOPIC_METRICS, TOPIC_RAW,
        VAST_BOOTSTRAP_SERVERS,
    )
    from generator.fraud_patterns import (
        CHANNELS, MERCHANT_CATEGORIES, MERCHANT_CATEGORY_MAP,
        PATTERN_REGISTRY, CustomerPool, random_fraud_pattern,
    )

# ---------------------------------------------------------------------------
# Globals for graceful shutdown
# ---------------------------------------------------------------------------
_shutdown = False


def _handle_sigint(_sig, _frame):
    """Set the shutdown flag so the main loop exits cleanly."""
    global _shutdown
    print("\n[INFO] Ctrl+C received -- flushing remaining messages...")
    _shutdown = True


# ---------------------------------------------------------------------------
# Legitimate transaction builder
# ---------------------------------------------------------------------------

def _generate_legit_txn(customer) -> dict:
    """Build one normal-looking transaction for *customer*.

    The transaction stays within the customer's known behavioural
    profile: home city, usual spending range, registered devices, and
    merchants they have shopped at before.
    """
    # Amount: normal distribution centred on avg_spend, clipped to [1, 3x avg]
    amount = max(1.0, random.gauss(customer.avg_spend, customer.avg_spend * 0.3))
    amount = min(amount, customer.avg_spend * 3)

    # Slight jitter on lat/lon to simulate different locations within a city
    lat = customer.home_lat + random.uniform(-0.05, 0.05)
    lon = customer.home_lon + random.uniform(-0.05, 0.05)

    merchant_id = random.choice(customer.known_merchants)
    merchant_category = MERCHANT_CATEGORY_MAP.get(merchant_id, random.choice(MERCHANT_CATEGORIES))

    return {
        "transaction_id": f"TXN-{uuid.uuid4().hex[:12].upper()}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "card_id": customer.card_id,
        "customer_id": customer.customer_id,
        "merchant_id": merchant_id,
        "merchant_category": merchant_category,
        "amount": round(amount, 2),
        "currency": "USD",
        "location_lat": round(lat, 4),
        "location_lon": round(lon, 4),
        "location_city": customer.home_city,
        "device_fingerprint": random.choice(customer.known_devices),
        "channel": random.choice(CHANNELS),
        "is_fraud": False,
    }


# ---------------------------------------------------------------------------
# Kafka helpers
# ---------------------------------------------------------------------------

def _delivery_callback(err, msg):
    """Log delivery failures (called once per message by librdkafka)."""
    if err is not None:
        print(f"[WARN] Message delivery failed: {err}")


def _publish(producer: Producer, topic: str, value: dict, key: str | None = None):
    """Serialize *value* as JSON and send it to *topic* with metadata headers."""
    headers = {
        "source": "vast-fraud-demo",
        "content-type": "application/json",
        "event-type": value.get("fraud_type", "transaction"),
        "customer-id": value.get("customer_id", ""),
        "is-fraud": str(value.get("is_fraud", False)).lower(),
        "merchant-category": value.get("merchant_category", ""),
        "location-city": value.get("location_city", ""),
    }
    producer.produce(
        topic,
        key=key.encode("utf-8") if key else None,
        value=json.dumps(value).encode("utf-8"),
        headers=headers,
        callback=_delivery_callback,
    )
    # Trigger delivery-report callbacks without blocking
    producer.poll(0)


# ---------------------------------------------------------------------------
# Metrics publisher
# ---------------------------------------------------------------------------

def _publish_metrics(producer: Producer, msgs_per_sec: float, total_sent: int):
    """Send a throughput / latency snapshot to the metrics topic."""
    metric = {
        "source": "generator",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "msgs_per_sec": round(msgs_per_sec, 1),
        "total_sent": total_sent,
    }
    _publish(producer, TOPIC_METRICS, metric)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run(args: argparse.Namespace) -> None:
    """Core generation loop."""

    # Seed for reproducibility
    random.seed(args.seed)

    # Build the customer pool
    print(f"[INFO] Building pool of {args.customers:,} synthetic customers...")
    pool = CustomerPool(args.customers, seed=args.seed)

    # Pick the fraud pattern generator (or random each time)
    forced_pattern = None
    if args.inject:
        if args.inject not in PATTERN_REGISTRY:
            print(f"[ERROR] Unknown pattern '{args.inject}'. "
                  f"Choose from: {', '.join(PATTERN_REGISTRY)}")
            sys.exit(1)
        forced_pattern = PATTERN_REGISTRY[args.inject]()
        print(f"[INFO] Forcing fraud pattern: {args.inject}")

    # Configure the Kafka producer
    producer_conf = {
        "bootstrap.servers": args.bootstrap_servers,
        "linger.ms": 5,             # micro-batch for throughput
        "batch.num.messages": 1000,
        "queue.buffering.max.messages": 500000,
        "message.timeout.ms": 10000,
    }
    print(f"[INFO] Connecting to Kafka at {args.bootstrap_servers}...")
    producer = Producer(producer_conf)

    # Pre-compute timing
    batch_size = max(1, args.tps)   # messages per second
    sleep_per_msg = 1.0 / batch_size if batch_size else 1.0

    total_sent = 0
    total_fraud = 0
    start_time = time.monotonic()
    last_report = start_time

    print(f"[INFO] Generating {args.tps:,} TPS for {args.duration}s "
          f"(fraud rate {args.fraud_rate:.0%})...")
    print("-" * 60)

    try:
        while not _shutdown:
            elapsed = time.monotonic() - start_time
            if elapsed >= args.duration:
                print(f"\n[INFO] Duration of {args.duration}s reached.")
                break

            # Decide: legit or fraud?
            is_fraud_txn = random.random() < args.fraud_rate

            if is_fraud_txn:
                customer = pool.random_profile()
                pattern = forced_pattern or random_fraud_pattern()
                txns = pattern.generate(customer)
                for txn in txns:
                    _publish(producer, TOPIC_RAW, txn, key=txn["card_id"])
                    total_sent += 1
                    total_fraud += 1
            else:
                customer = pool.random_profile()
                txn = _generate_legit_txn(customer)
                _publish(producer, TOPIC_RAW, txn, key=txn["card_id"])
                total_sent += 1

            # ---- Progress report every 5 seconds ----
            now = time.monotonic()
            if now - last_report >= 5.0:
                window_tps = total_sent / max(elapsed, 0.001)
                _publish_metrics(producer, window_tps, total_sent)
                print(
                    f"  TPS: {window_tps:>8,.1f} | "
                    f"Total: {total_sent:>10,} | "
                    f"Fraud: {total_fraud:>8,} "
                    f"({total_fraud / max(total_sent, 1):.1%})"
                )
                last_report = now

            # ---- Throttle to target TPS ----
            # Calculate how far ahead/behind schedule we are and
            # sleep only if we are ahead.
            expected_sent = args.tps * (time.monotonic() - start_time)
            if total_sent > expected_sent:
                time.sleep(sleep_per_msg)

    except KeyboardInterrupt:
        pass  # _handle_sigint already set _shutdown

    # ---- Final flush ----
    remaining = producer.flush(timeout=10)
    elapsed = time.monotonic() - start_time

    print("-" * 60)
    print(f"[INFO] Done. {total_sent:,} messages sent in {elapsed:.1f}s "
          f"(avg {total_sent / max(elapsed, 0.001):,.1f} msg/s)")
    print(f"[INFO] Fraud injected: {total_fraud:,} "
          f"({total_fraud / max(total_sent, 1):.1%})")
    if remaining:
        print(f"[WARN] {remaining} message(s) still in queue after flush.")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="VAST Data Fraud-Detection Demo -- Transaction Generator",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--target",
        choices=["vast", "kafka"],
        default=None,
        help="Shortcut: 'vast' uses VAST Event Broker, 'kafka' uses local Docker Kafka",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=None,
        help="Kafka bootstrap servers (overrides --target)",
    )
    parser.add_argument(
        "--tps",
        type=int,
        default=DEFAULT_TPS,
        help="Target transactions per second",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=DEFAULT_DURATION,
        help="How many seconds to run",
    )
    parser.add_argument(
        "--fraud-rate",
        type=float,
        default=DEFAULT_FRAUD_RATE,
        help="Fraction of transactions that are fraudulent (0.0-1.0)",
    )
    parser.add_argument(
        "--customers",
        type=int,
        default=DEFAULT_CUSTOMERS,
        help="Number of synthetic customers to generate",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help="Random seed for reproducibility",
    )
    parser.add_argument(
        "--inject",
        choices=list(PATTERN_REGISTRY.keys()),
        default=None,
        help="Force a specific fraud pattern instead of random selection",
    )
    return parser.parse_args(argv)


def main():
    signal.signal(signal.SIGINT, _handle_sigint)
    args = parse_args()

    # Resolve bootstrap servers: --bootstrap-servers > --target > default
    if args.bootstrap_servers is None:
        if args.target == "vast":
            args.bootstrap_servers = VAST_BOOTSTRAP_SERVERS
        elif args.target == "kafka":
            args.bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS
        else:
            args.bootstrap_servers = DEFAULT_BOOTSTRAP_SERVERS

    run(args)


if __name__ == "__main__":
    main()
