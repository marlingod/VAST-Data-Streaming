"""
VAST topics-as-tables SQL queries using the vastdb SDK.

This module demonstrates how VAST DataBase can serve as the analytical
backbone for fraud detection -- querying streaming data that has been
automatically materialised into columnar tables by the Event Broker.

Every function accepts a ``vastdb`` session and returns a PyArrow table
with the query results plus timing metadata.
"""

import logging
import time
from datetime import datetime, timezone, timedelta

import pyarrow as pa
import vastdb

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Analytical queries
# ---------------------------------------------------------------------------

def query_spending_anomalies(
    session,
    bucket_name: str,
    schema_name: str,
) -> pa.Table:
    """
    Find cards whose recent spending exceeds 3x their 6-month standard
    deviation.

    This is a classic outlier-detection query that benefits from VAST's
    ability to scan billions of rows at near-memory speed.
    """
    t0 = time.perf_counter()

    with session.transaction() as tx:
        table = tx.bucket(bucket_name).schema(schema_name).table("transactions")
        six_months_ago = (datetime.now(timezone.utc) - timedelta(days=180)).isoformat()

        # Select all transactions within the 6-month window
        reader = table.select(
            columns=["card_id", "amount", "timestamp"],
            predicate=f"timestamp >= '{six_months_ago}'",
        )
        result = reader.read_all()

    latency_ms = (time.perf_counter() - t0) * 1000
    logger.info(
        "query_spending_anomalies: %d rows in %.2fms",
        result.num_rows,
        latency_ms,
    )
    return result


def query_geographic_impossibilities(
    session,
    bucket_name: str,
    schema_name: str,
) -> pa.Table:
    """
    Find transactions from the same card that occurred in distant cities
    within a 5-minute window -- a strong fraud signal.

    The query selects all transactions from the last 24 hours and relies
    on downstream processing to compute haversine distances between
    consecutive transactions per card.
    """
    t0 = time.perf_counter()

    with session.transaction() as tx:
        table = tx.bucket(bucket_name).schema(schema_name).table("transactions")
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()

        reader = table.select(
            columns=["card_id", "timestamp", "latitude", "longitude", "amount", "merchant_id"],
            predicate=f"timestamp >= '{cutoff}'",
        )
        result = reader.read_all()

    latency_ms = (time.perf_counter() - t0) * 1000
    logger.info(
        "query_geographic_impossibilities: %d rows in %.2fms",
        result.num_rows,
        latency_ms,
    )
    return result


def query_fraud_ring_activity(
    session,
    bucket_name: str,
    schema_name: str,
    fraud_ring_merchants: set,
) -> pa.Table:
    """
    Find all transactions at merchants known to participate in fraud
    rings.

    Uses a predicate push-down with an IN clause for efficient filtering
    on the VAST storage layer.
    """
    t0 = time.perf_counter()

    if not fraud_ring_merchants:
        logger.warning("Empty fraud_ring_merchants set -- returning empty table")
        return pa.table({
            "transaction_id": pa.array([], type=pa.string()),
            "card_id": pa.array([], type=pa.string()),
            "merchant_id": pa.array([], type=pa.string()),
            "amount": pa.array([], type=pa.float64()),
            "timestamp": pa.array([], type=pa.string()),
        })

    merchant_list = ", ".join(f"'{m}'" for m in fraud_ring_merchants)

    with session.transaction() as tx:
        table = tx.bucket(bucket_name).schema(schema_name).table("transactions")
        reader = table.select(
            columns=["transaction_id", "card_id", "merchant_id", "amount", "timestamp"],
            predicate=f"merchant_id IN ({merchant_list})",
        )
        result = reader.read_all()

    latency_ms = (time.perf_counter() - t0) * 1000
    logger.info(
        "query_fraud_ring_activity: %d rows in %.2fms",
        result.num_rows,
        latency_ms,
    )
    return result


def query_time_travel(
    session,
    bucket_name: str,
    schema_name: str,
    card_id: str,
    start_date: str,
    end_date: str,
) -> pa.Table:
    """
    Time-travel query for compliance and audit purposes.

    Retrieves the complete transaction history for a specific card
    within a date range, leveraging VAST's immutable snapshot
    capabilities to guarantee point-in-time consistency.

    Parameters
    ----------
    card_id : str
        The card to audit.
    start_date, end_date : str
        ISO-8601 date strings defining the audit window.
    """
    t0 = time.perf_counter()

    with session.transaction() as tx:
        table = tx.bucket(bucket_name).schema(schema_name).table("transactions")
        reader = table.select(
            columns=[
                "transaction_id", "card_id", "amount", "timestamp",
                "merchant_id", "merchant_category", "latitude", "longitude",
            ],
            predicate=(
                f"card_id = '{card_id}' "
                f"AND timestamp >= '{start_date}' "
                f"AND timestamp <= '{end_date}'"
            ),
        )
        result = reader.read_all()

    latency_ms = (time.perf_counter() - t0) * 1000
    logger.info(
        "query_time_travel: card=%s, %d rows in %.2fms",
        card_id,
        result.num_rows,
        latency_ms,
    )
    return result


# ---------------------------------------------------------------------------
# Named-query runner (for benchmarking / comparison demos)
# ---------------------------------------------------------------------------

_NAMED_QUERIES = {
    "spending_anomalies": query_spending_anomalies,
    "geographic_impossibilities": query_geographic_impossibilities,
}


def run_comparison_query(
    session,
    bucket_name: str,
    schema_name: str,
    query_name: str,
) -> dict:
    """
    Run a named query and return a standardised result dict.

    This is used by the comparison demo to benchmark VAST against
    alternative backends using the same logical query.

    Returns
    -------
    dict
        {"query": str, "result_count": int, "latency_ms": float, "backend": "vast"}
    """
    func = _NAMED_QUERIES.get(query_name)
    if func is None:
        raise ValueError(
            f"Unknown query '{query_name}'. Available: {list(_NAMED_QUERIES.keys())}"
        )

    t0 = time.perf_counter()
    result = func(session, bucket_name, schema_name)
    latency_ms = (time.perf_counter() - t0) * 1000

    return {
        "query": query_name,
        "result_count": result.num_rows,
        "latency_ms": round(latency_ms, 2),
        "backend": "vast",
    }
