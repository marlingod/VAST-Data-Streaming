"""
Fraud detection rules for real-time transaction scoring.

Each rule function evaluates a specific fraud signal and returns a score
between 0.0 (no risk) and 1.0 (maximum risk). The composite score is
computed as a weighted average of all individual rule scores.

These rules are applied by the VAST DataEngine scorer function to every
incoming transaction on the fraud.transactions.raw Event Broker topic.
"""

import math
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Composite score threshold above which an alert is raised
ALERT_THRESHOLD = 0.8

# ---------------------------------------------------------------------------
# Utility: Haversine distance
# ---------------------------------------------------------------------------

def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return the great-circle distance in kilometres between two points."""
    R = 6371.0  # Earth radius in km
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# ---------------------------------------------------------------------------
# Individual rule functions
# ---------------------------------------------------------------------------

def check_velocity(transaction: dict, recent_transactions: list[dict]) -> float:
    """
    Count transactions from the same card_id in the last 60 seconds.

    Returns a score between 0.0 and 1.0, where 1.0 means 10 or more
    transactions were observed -- a strong indicator of automated fraud
    or credential stuffing.
    """
    card_id = transaction.get("card_id")
    txn_time = transaction.get("timestamp")
    if not card_id or not txn_time:
        return 0.0

    if isinstance(txn_time, str):
        txn_time = datetime.fromisoformat(txn_time)

    count = 0
    for rt in recent_transactions:
        if rt.get("card_id") != card_id:
            continue
        rt_time = rt.get("timestamp")
        if isinstance(rt_time, str):
            rt_time = datetime.fromisoformat(rt_time)
        delta = abs((txn_time - rt_time).total_seconds())
        if delta <= 60:
            count += 1

    # Linear ramp: 0 at 0 txns, 1.0 at 10+ txns
    return min(count / 10.0, 1.0)


def check_geographic_impossibility(
    transaction: dict, recent_transactions: list[dict]
) -> float:
    """
    Check whether the cardholder could physically have travelled between
    the current transaction location and the previous one.

    A distance of >500 km within 5 minutes is considered impossible and
    scores 1.0. Shorter distances or longer gaps score proportionally lower.
    """
    card_id = transaction.get("card_id")
    txn_time = transaction.get("timestamp")
    lat = transaction.get("latitude")
    lon = transaction.get("longitude")
    if card_id is None or txn_time is None or lat is None or lon is None:
        return 0.0

    if isinstance(txn_time, str):
        txn_time = datetime.fromisoformat(txn_time)

    # Find the most recent prior transaction for this card
    prev = None
    prev_delta = None
    for rt in recent_transactions:
        if rt.get("card_id") != card_id:
            continue
        rt_time = rt.get("timestamp")
        if isinstance(rt_time, str):
            rt_time = datetime.fromisoformat(rt_time)
        delta = (txn_time - rt_time).total_seconds()
        if delta <= 0:
            continue  # skip future or same-time transactions
        if prev_delta is None or delta < prev_delta:
            prev = rt
            prev_delta = delta

    if prev is None or prev_delta is None:
        return 0.0

    prev_lat = prev.get("latitude")
    prev_lon = prev.get("longitude")
    if prev_lat is None or prev_lon is None:
        return 0.0

    distance_km = _haversine_km(lat, lon, prev_lat, prev_lon)

    # Flag if >500 km within 5 minutes (300 seconds)
    if prev_delta <= 300 and distance_km > 500:
        return 1.0
    elif prev_delta <= 300 and distance_km > 200:
        # Proportional score for moderate distances
        return min((distance_km - 200) / 300.0, 1.0)

    return 0.0


def check_amount_anomaly(transaction: dict, customer_history: dict) -> float:
    """
    Compare the transaction amount to the customer's historical average.

    Returns 1.0 when the amount exceeds 10x the historical average,
    scaling linearly from 0.0 (at or below average) to 1.0 (10x+).
    """
    amount = transaction.get("amount")
    avg_amount = customer_history.get("avg_amount")
    if amount is None or avg_amount is None or avg_amount <= 0:
        return 0.0

    ratio = amount / avg_amount
    if ratio <= 1.0:
        return 0.0
    # Linear ramp from 1x to 10x
    return min((ratio - 1.0) / 9.0, 1.0)


def check_card_testing(transaction: dict, recent_transactions: list[dict]) -> float:
    """
    Detect card-testing patterns: 5+ micro-transactions ($1-$2) from
    the same card within 30 seconds.

    Fraudsters commonly test stolen card numbers with tiny purchases
    before making large ones.
    """
    card_id = transaction.get("card_id")
    txn_time = transaction.get("timestamp")
    if not card_id or not txn_time:
        return 0.0

    if isinstance(txn_time, str):
        txn_time = datetime.fromisoformat(txn_time)

    micro_count = 0
    for rt in recent_transactions:
        if rt.get("card_id") != card_id:
            continue
        rt_time = rt.get("timestamp")
        if isinstance(rt_time, str):
            rt_time = datetime.fromisoformat(rt_time)
        delta = abs((txn_time - rt_time).total_seconds())
        rt_amount = rt.get("amount", 0)
        if delta <= 30 and 1.0 <= rt_amount <= 2.0:
            micro_count += 1

    # Also count the current transaction if it is a micro-transaction
    current_amount = transaction.get("amount", 0)
    if 1.0 <= current_amount <= 2.0:
        micro_count += 1

    # Threshold: 5+ micro-transactions => 1.0
    if micro_count >= 5:
        return 1.0
    elif micro_count >= 3:
        return (micro_count - 2) / 3.0
    return 0.0


def check_fraud_ring(transaction: dict, fraud_ring_merchants: set) -> float:
    """
    Check whether the merchant is in a known fraud ring.

    Returns 1.0 if the merchant_id is in the fraud ring set, else 0.0.
    """
    merchant_id = transaction.get("merchant_id")
    if merchant_id is None:
        return 0.0
    return 1.0 if merchant_id in fraud_ring_merchants else 0.0


# ---------------------------------------------------------------------------
# Composite score
# ---------------------------------------------------------------------------

# Rule weights -- must sum to 1.0
_WEIGHTS: dict[str, float] = {
    "velocity": 0.25,
    "geo": 0.30,
    "amount": 0.20,
    "card_testing": 0.15,
    "fraud_ring": 0.10,
}


def compute_risk_score(scores: dict[str, float]) -> float:
    """
    Compute a weighted composite risk score from individual rule scores.

    Parameters
    ----------
    scores : dict
        Mapping of rule name to its 0.0-1.0 score.  Expected keys:
        velocity, geo, amount, card_testing, fraud_ring.

    Returns
    -------
    float
        Weighted composite score in [0.0, 1.0].
    """
    total = 0.0
    for rule, weight in _WEIGHTS.items():
        total += scores.get(rule, 0.0) * weight
    return round(min(total, 1.0), 4)
