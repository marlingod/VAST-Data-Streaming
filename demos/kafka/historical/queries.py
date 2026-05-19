"""
ClickHouse Query Module — Kafka Pipeline Historical Analytics
===============================================================
Same analytical queries as the VAST side, but executed via ClickHouse's
HTTP interface (port 8123).

Architecture complexity demonstrated here:
  - Every query requires an HTTP round-trip to a SEPARATE ClickHouse cluster
  - The data in ClickHouse arrived via a Kafka-to-ClickHouse ETL pipeline
    (the Kafka Engine table + Materialized View in clickhouse_setup.sql)
  - If that ETL pipeline lags or breaks, these queries return stale results
  - ClickHouse must be independently provisioned, scaled, monitored, and patched

In the VAST architecture, these same queries run directly against the platform
that already holds the streaming data — no ETL, no separate cluster, no staleness.
"""

import time
import logging
from typing import Optional

import requests

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper — execute a query against ClickHouse HTTP interface
# ---------------------------------------------------------------------------

def _execute_query(
    ch_host: str,
    ch_port: int,
    sql: str,
    default_format: str = "JSONEachRow",
) -> dict:
    """
    Execute a SQL query against ClickHouse via its HTTP API.

    Note: this requires a separate ClickHouse instance and Kafka-to-ClickHouse
    ETL pipeline to be running. If either is down, queries fail entirely.

    Returns:
        dict with keys: query, rows, result_count, latency_ms, backend, error
    """
    url = f"http://{ch_host}:{ch_port}/"
    start = time.perf_counter()
    result = {
        "query": sql.strip(),
        "rows": [],
        "result_count": 0,
        "latency_ms": 0.0,
        "backend": "kafka+clickhouse",
        "error": None,
    }

    try:
        resp = requests.get(
            url,
            params={"query": sql, "default_format": default_format},
            timeout=30.0,
        )
        resp.raise_for_status()

        elapsed_ms = (time.perf_counter() - start) * 1000.0
        result["latency_ms"] = round(elapsed_ms, 2)

        rows = []
        for line in resp.text.strip().split("\n"):
            if line:
                import json
                rows.append(json.loads(line))
        result["rows"] = rows
        result["result_count"] = len(rows)

    except requests.RequestException as exc:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        result["latency_ms"] = round(elapsed_ms, 2)
        result["error"] = str(exc)
        logger.error("ClickHouse query failed: %s", exc)

    return result


# ---------------------------------------------------------------------------
# Analytical Queries — mirror the VAST side for fair comparison
# ---------------------------------------------------------------------------

def query_spending_anomalies(ch_host: str, ch_port: int) -> dict:
    """
    Detect spending anomalies by finding transactions whose amount deviates
    significantly from the cardholder's historical average.

    Note: this query only returns accurate results if the Kafka-to-ClickHouse
    ETL pipeline is current. Lag in that pipeline means recent transactions
    are invisible to this analysis.
    """
    sql = """
        SELECT
            t.transaction_id,
            t.card_id,
            t.amount,
            t.timestamp,
            t.merchant_category,
            stats.avg_amount,
            stats.std_amount,
            (t.amount - stats.avg_amount) / greatest(stats.std_amount, 0.01) AS z_score
        FROM fraud_transactions AS t
        INNER JOIN (
            SELECT
                card_id,
                avg(amount) AS avg_amount,
                stddevPop(amount) AS std_amount
            FROM fraud_transactions
            GROUP BY card_id
            HAVING count(*) >= 5
        ) AS stats ON t.card_id = stats.card_id
        WHERE abs((t.amount - stats.avg_amount) / greatest(stats.std_amount, 0.01)) > 2.0
        ORDER BY z_score DESC
        LIMIT 100
    """
    return _execute_query(ch_host, ch_port, sql)


def query_geographic_impossibilities(ch_host: str, ch_port: int) -> dict:
    """
    Find pairs of transactions from the same card that are geographically
    impossible — i.e., the cardholder would need to travel faster than
    900 km/h to be at both locations.

    Note: requires the full transaction history to be in ClickHouse.
    Any ETL lag means recent impossible-travel pairs may be missed.
    """
    sql = """
        SELECT
            t1.transaction_id AS txn1_id,
            t2.transaction_id AS txn2_id,
            t1.card_id,
            t1.location_city AS city1,
            t2.location_city AS city2,
            t1.timestamp AS time1,
            t2.timestamp AS time2,
            -- Haversine distance in km
            6371.0 * 2 * asin(sqrt(
                pow(sin(radians(t2.location_lat - t1.location_lat) / 2), 2)
                + cos(radians(t1.location_lat)) * cos(radians(t2.location_lat))
                  * pow(sin(radians(t2.location_lon - t1.location_lon) / 2), 2)
            )) AS distance_km,
            -- Time difference in hours
            dateDiff('second', t1.timestamp, t2.timestamp) / 3600.0 AS hours_diff
        FROM fraud_transactions AS t1
        INNER JOIN fraud_transactions AS t2
            ON t1.card_id = t2.card_id
            AND t2.timestamp > t1.timestamp
            AND dateDiff('second', t1.timestamp, t2.timestamp) < 86400
        WHERE
            -- Speed > 900 km/h
            (6371.0 * 2 * asin(sqrt(
                pow(sin(radians(t2.location_lat - t1.location_lat) / 2), 2)
                + cos(radians(t1.location_lat)) * cos(radians(t2.location_lat))
                  * pow(sin(radians(t2.location_lon - t1.location_lon) / 2), 2)
            ))) / greatest(dateDiff('second', t1.timestamp, t2.timestamp) / 3600.0, 0.001) > 900
        ORDER BY distance_km DESC
        LIMIT 50
    """
    return _execute_query(ch_host, ch_port, sql)


def query_fraud_ring_activity(ch_host: str, ch_port: int) -> dict:
    """
    Identify transaction activity at merchants flagged as part of fraud rings.

    Note: the fraud_ring_merchants table must be kept up-to-date by a separate
    process. In the VAST architecture, this lookup happens against the same
    data platform — no separate table sync needed.
    """
    sql = """
        SELECT
            t.merchant_id,
            fr.risk_level,
            count(*) AS txn_count,
            sum(t.amount) AS total_amount,
            uniqExact(t.card_id) AS unique_cards,
            min(t.timestamp) AS first_txn,
            max(t.timestamp) AS last_txn
        FROM fraud_transactions AS t
        INNER JOIN fraud_ring_merchants AS fr
            ON t.merchant_id = fr.merchant_id
        GROUP BY t.merchant_id, fr.risk_level
        ORDER BY txn_count DESC
        LIMIT 50
    """
    return _execute_query(ch_host, ch_port, sql)


def query_customer_history(
    ch_host: str,
    ch_port: int,
    card_id: str,
    months: int = 6,
) -> dict:
    """
    Retrieve the full transaction history for a specific card over N months.

    This is the query the fraud scoring engine calls (via HTTP) for every
    transaction that needs a historical context check. Each call adds network
    latency that doesn't exist in the unified VAST architecture.

    Args:
        ch_host: ClickHouse hostname
        ch_port: ClickHouse HTTP port
        card_id: The card ID to look up
        months: Number of months of history to retrieve (default 6)
    """
    sql = f"""
        SELECT
            transaction_id,
            timestamp,
            amount,
            merchant_id,
            merchant_category,
            location_city,
            location_lat,
            location_lon,
            is_fraud
        FROM fraud_transactions
        WHERE card_id = '{card_id}'
          AND timestamp >= now() - INTERVAL {months} MONTH
        ORDER BY timestamp DESC
        LIMIT 1000
    """
    return _execute_query(ch_host, ch_port, sql)


def run_comparison_query(
    ch_host: str,
    ch_port: int,
    query_name: str,
) -> dict:
    """
    Named query runner with timing — designed for the comparison dashboard.

    The dashboard calls this function for each query type and records the
    latency. It then compares Kafka+ClickHouse latency against VAST latency
    to demonstrate the performance difference.

    Args:
        ch_host: ClickHouse hostname
        ch_port: ClickHouse HTTP port
        query_name: One of 'spending_anomalies', 'geographic_impossibilities',
                     'fraud_ring_activity', 'customer_history'

    Returns:
        dict with query, result_count, latency_ms, backend, and error fields.
        The latency_ms is measured with time.perf_counter() for high precision.
    """
    query_map = {
        "spending_anomalies": query_spending_anomalies,
        "geographic_impossibilities": query_geographic_impossibilities,
        "fraud_ring_activity": query_fraud_ring_activity,
        "customer_history": lambda h, p: query_customer_history(h, p, "CARD-0001"),
    }

    if query_name not in query_map:
        return {
            "query": query_name,
            "rows": [],
            "result_count": 0,
            "latency_ms": 0.0,
            "backend": "kafka+clickhouse",
            "error": f"Unknown query name: {query_name}. "
                     f"Available: {list(query_map.keys())}",
        }

    # Time the full query execution including HTTP overhead
    start = time.perf_counter()
    result = query_map[query_name](ch_host, ch_port)
    total_ms = (time.perf_counter() - start) * 1000.0

    # Use the outer timing which includes any overhead beyond the raw query
    result["latency_ms"] = round(total_ms, 2)

    logger.info(
        "Comparison query '%s': %d results in %.2f ms (backend=%s)",
        query_name,
        result["result_count"],
        result["latency_ms"],
        result["backend"],
    )

    return result
