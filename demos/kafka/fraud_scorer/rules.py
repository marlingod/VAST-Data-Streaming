"""
Fraud Detection Rules — Kafka/Faust Implementation
====================================================
Same fraud detection logic as the VAST side, but adapted for Faust/Kafka context.

Key difference: the VAST version runs all rules against a single unified data
platform. Here, each rule may require a different backend:
  - Velocity/card-testing checks use Faust Tables (RocksDB-backed state)
  - Amount anomaly checks require an HTTP call to ClickHouse (separate system)
  - Fraud ring checks use a periodically-refreshed in-memory set

This multi-system approach adds latency, operational burden, and failure modes
that don't exist in the unified VAST architecture.
"""

import math
import time
import logging
from typing import Dict, Optional

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
ALERT_THRESHOLD = 0.8

# Rule weights — identical to the VAST side for fair comparison
RULE_WEIGHTS = {
    "velocity": 0.25,
    "geographic": 0.30,
    "amount_anomaly": 0.20,
    "card_testing": 0.15,
    "fraud_ring": 0.10,
}

# ---------------------------------------------------------------------------
# Haversine — great-circle distance between two lat/lon points
# ---------------------------------------------------------------------------

def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return distance in kilometres between two geographic coordinates."""
    R = 6371.0  # Earth radius in km
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# ---------------------------------------------------------------------------
# Rule Functions
# ---------------------------------------------------------------------------

def check_velocity(card_id: str, velocity_table) -> float:
    """
    Velocity check — how many transactions has this card seen recently?

    Uses a Faust Table with a tumbling window (60 s). The table is backed by
    RocksDB on the local worker node. Note: in a multi-worker deployment the
    state is partitioned, so counts are only accurate if the card's partition
    lands on this worker (Faust handles this via key-based routing).

    Returns:
        Risk score 0.0 – 1.0
    """
    try:
        count = velocity_table[card_id].current() if velocity_table else 0
    except Exception:
        count = 0

    if count >= 10:
        return 1.0
    elif count >= 5:
        return 0.7
    elif count >= 3:
        return 0.4
    return 0.0


def check_geographic_impossibility(
    transaction: dict,
    location_table,
) -> float:
    """
    Geographic impossibility — can the cardholder physically be at this location
    given their last known position and the elapsed time?

    Uses a Faust hopping-window Table to track last-known location per card.
    The calculation mirrors the VAST side: if speed > 900 km/h it's suspicious.

    Returns:
        Risk score 0.0 – 1.0
    """
    card_id = transaction.get("card_id", "")
    lat = transaction.get("location_lat", 0.0)
    lon = transaction.get("location_lon", 0.0)

    try:
        last = location_table[card_id].current() if location_table else None
    except Exception:
        last = None

    if not last:
        return 0.0

    last_lat = last.get("lat", 0.0)
    last_lon = last.get("lon", 0.0)
    last_ts = last.get("timestamp", 0.0)
    current_ts = transaction.get("timestamp", time.time())

    distance_km = haversine(last_lat, last_lon, lat, lon)
    elapsed_hours = max((current_ts - last_ts) / 3600.0, 0.001)
    speed_kmh = distance_km / elapsed_hours

    if speed_kmh > 900:
        return 1.0  # Faster than a commercial airliner
    elif speed_kmh > 500:
        return 0.6
    return 0.0


def check_amount_anomaly(
    transaction: dict,
    clickhouse_host: str = "clickhouse",
    clickhouse_port: int = 8123,
) -> float:
    """
    Amount anomaly — is this transaction amount unusual for the cardholder?

    ** THIS IS THE KEY COMPLEXITY SHOWCASE **
    Unlike the VAST version (which queries the same platform holding the stream),
    this rule must make an HTTP request to a completely separate ClickHouse
    instance. That means:
      - Extra network hop and serialization overhead
      - A separate ClickHouse cluster to provision, monitor, and patch
      - A Kafka-to-ClickHouse ETL pipeline that must be kept running
      - Potential data-freshness lag (ClickHouse may not have the latest rows)

    Returns:
        Risk score 0.0 – 1.0
    """
    card_id = transaction.get("card_id", "")
    amount = transaction.get("amount", 0.0)

    query = f"""
        SELECT avg(amount) AS avg_amt, stddevPop(amount) AS std_amt
        FROM fraud_transactions
        WHERE card_id = '{card_id}'
    """

    try:
        # Note: every scored transaction triggers a ClickHouse HTTP round-trip
        resp = requests.get(
            f"http://{clickhouse_host}:{clickhouse_port}/",
            params={"query": query, "default_format": "JSONEachRow"},
            timeout=2.0,
        )
        resp.raise_for_status()
        rows = resp.json() if resp.text.strip() else []
        if not rows:
            return 0.0

        row = rows[0] if isinstance(rows, list) else rows
        avg_amt = float(row.get("avg_amt", 0) or 0)
        std_amt = float(row.get("std_amt", 0) or 0)

        if std_amt == 0:
            return 0.0

        z_score = abs(amount - avg_amt) / std_amt
        if z_score > 3:
            return 1.0
        elif z_score > 2:
            return 0.6
        elif z_score > 1.5:
            return 0.3
        return 0.0

    except requests.RequestException as exc:
        # If ClickHouse is down, we can't run this rule at all.
        # In the VAST architecture this failure mode doesn't exist because
        # historical data lives on the same platform as the stream.
        logger.warning("ClickHouse query failed for amount anomaly: %s", exc)
        return 0.0


def check_card_testing(
    card_id: str,
    velocity_table,
    recent_amounts: Optional[list] = None,
) -> float:
    """
    Card testing detection — fraudsters often validate stolen cards with many
    small transactions in rapid succession.

    Combines velocity data (from Faust Table) with amount patterns. If there
    are many transactions AND they're all small, it looks like card testing.

    Returns:
        Risk score 0.0 – 1.0
    """
    try:
        count = velocity_table[card_id].current() if velocity_table else 0
    except Exception:
        count = 0

    if count < 3:
        return 0.0

    # Check if recent amounts are suspiciously small and uniform
    if recent_amounts and len(recent_amounts) >= 3:
        avg = sum(recent_amounts) / len(recent_amounts)
        all_small = all(a < 5.0 for a in recent_amounts)
        low_variance = max(recent_amounts) - min(recent_amounts) < 2.0

        if all_small and low_variance and count >= 5:
            return 1.0
        elif all_small and count >= 3:
            return 0.6
        elif avg < 10.0 and count >= 4:
            return 0.4

    # Fall back to velocity-only heuristic
    if count >= 5:
        return 0.5
    return 0.2


def check_fraud_ring(merchant_id: str, fraud_ring_set: set) -> float:
    """
    Fraud ring detection — is this merchant flagged as part of a known ring?

    The fraud_ring_set is periodically refreshed from ClickHouse (yet another
    cross-system dependency). If the refresh fails, the set may be stale.

    Returns:
        Risk score 0.0 – 1.0
    """
    if not merchant_id or not fraud_ring_set:
        return 0.0

    if merchant_id in fraud_ring_set:
        return 1.0
    return 0.0


def compute_risk_score(scores: Dict[str, float]) -> float:
    """
    Combine individual rule scores into a single weighted risk score.

    Args:
        scores: dict mapping rule name to its raw score (0.0 – 1.0)

    Returns:
        Weighted composite score clamped to [0.0, 1.0]
    """
    total = 0.0
    for rule_name, weight in RULE_WEIGHTS.items():
        total += scores.get(rule_name, 0.0) * weight
    return min(max(total, 0.0), 1.0)
