"""
VAST cluster setup script for the fraud detection demo.

Creates the required bucket, schema, tables, and synthetic data so the
demo is ready to run end-to-end. Uses the ``vastdb`` SDK and expects
connection details in environment variables.

Usage
-----
    export VAST_ENDPOINT=https://vast-cluster.example.com
    export VAST_ACCESS_KEY=...
    export VAST_SECRET_KEY=...
    python -m demos.vast.setup

Or invoke individual functions from a notebook / REPL.
"""

import logging
import os
import random
import string
import sys
import time
import uuid
from datetime import datetime, timezone, timedelta

import pyarrow as pa
import vastdb

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("vast_setup")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BUCKET = os.environ.get("VAST_BUCKET", "fraud-detection")
SCHEMA = os.environ.get("VAST_SCHEMA", "fraud")

# Event Broker topics used by the pipeline
TOPICS = [
    "fraud.transactions.raw",
    "fraud.transactions.scored",
    "fraud.alerts",
    "fraud.metrics",
]

# Merchant categories for synthetic data
MERCHANT_CATEGORIES = [
    "grocery", "gas_station", "restaurant", "online_retail",
    "electronics", "travel", "entertainment", "healthcare",
    "clothing", "utilities", "cash_advance", "jewelry",
]

# Major US cities for synthetic geo data (lat, lon)
CITIES = [
    (40.7128, -74.0060),   # New York
    (34.0522, -118.2437),  # Los Angeles
    (41.8781, -87.6298),   # Chicago
    (29.7604, -95.3698),   # Houston
    (33.4484, -112.0740),  # Phoenix
    (29.4241, -98.4936),   # San Antonio
    (32.7157, -117.1611),  # San Diego
    (32.7767, -96.7970),   # Dallas
    (37.7749, -122.4194),  # San Francisco
    (47.6062, -122.3321),  # Seattle
    (39.7392, -104.9903),  # Denver
    (25.7617, -80.1918),   # Miami
]


# ---------------------------------------------------------------------------
# Topic creation (instructions only -- requires dataengine-cli)
# ---------------------------------------------------------------------------

def create_topics():
    """
    Print ``dataengine-cli`` commands to create Event Broker topics.

    The VAST Event Broker topics cannot be created via the vastdb SDK;
    they require the ``dataengine-cli`` tool running on or against the
    VAST cluster.
    """
    logger.info("Event Broker topic creation commands:")
    print("\n# ---- Event Broker Topics ----")
    print("# Run these commands on the VAST cluster using dataengine-cli:\n")
    for topic in TOPICS:
        print(f"dataengine-cli topic create --name {topic} --partitions 6 --replication-factor 3")
    print("\n# Verify topics:")
    print("dataengine-cli topic list\n")


# ---------------------------------------------------------------------------
# Database schema creation
# ---------------------------------------------------------------------------

def create_database_schema(session):
    """
    Create the bucket, schema, and tables required by the fraud pipeline.

    Tables
    ------
    * ``transactions``         -- raw + scored transaction data
    * ``audit_trail``          -- investigation audit records
    * ``fraud_ring_merchants`` -- known fraudulent merchant IDs
    * ``customer_profiles``    -- customer spending profiles
    """
    logger.info("Creating database schema in bucket '%s', schema '%s'", BUCKET, SCHEMA)

    with session.transaction() as tx:
        # Create bucket (idempotent -- SDK will raise if it already exists)
        try:
            tx.create_bucket(BUCKET)
            logger.info("Created bucket '%s'", BUCKET)
        except Exception as exc:
            if "already exists" in str(exc).lower():
                logger.info("Bucket '%s' already exists", BUCKET)
            else:
                raise

        bucket = tx.bucket(BUCKET)

        # Create schema
        try:
            bucket.create_schema(SCHEMA)
            logger.info("Created schema '%s'", SCHEMA)
        except Exception as exc:
            if "already exists" in str(exc).lower():
                logger.info("Schema '%s' already exists", SCHEMA)
            else:
                raise

        schema = bucket.schema(SCHEMA)

        # -- transactions table ------------------------------------------------
        txn_arrow_schema = pa.schema([
            pa.field("transaction_id", pa.string()),
            pa.field("card_id", pa.string()),
            pa.field("amount", pa.float64()),
            pa.field("timestamp", pa.string()),
            pa.field("merchant_id", pa.string()),
            pa.field("merchant_category", pa.string()),
            pa.field("latitude", pa.float64()),
            pa.field("longitude", pa.float64()),
            pa.field("risk_score", pa.float64()),
            pa.field("triggered_rules", pa.string()),  # JSON array stored as string
        ])
        _create_table_safe(schema, "transactions", txn_arrow_schema)

        # -- audit_trail table -------------------------------------------------
        audit_arrow_schema = pa.schema([
            pa.field("audit_id", pa.string()),
            pa.field("timestamp", pa.string()),
            pa.field("card_id", pa.string()),
            pa.field("transaction_id", pa.string()),
            pa.field("risk_level", pa.string()),
            pa.field("recommended_action", pa.string()),
            pa.field("evidence_summary", pa.string()),
            pa.field("investigator", pa.string()),
            pa.field("investigation_duration_ms", pa.float64()),
        ])
        _create_table_safe(schema, "audit_trail", audit_arrow_schema)

        # -- fraud_ring_merchants table ----------------------------------------
        fraud_ring_schema = pa.schema([
            pa.field("merchant_id", pa.string()),
            pa.field("ring_name", pa.string()),
            pa.field("risk_category", pa.string()),
            pa.field("added_date", pa.string()),
        ])
        _create_table_safe(schema, "fraud_ring_merchants", fraud_ring_schema)

        # -- customer_profiles table -------------------------------------------
        profile_schema = pa.schema([
            pa.field("card_id", pa.string()),
            pa.field("customer_name", pa.string()),
            pa.field("avg_amount", pa.float64()),
            pa.field("std_amount", pa.float64()),
            pa.field("total_transactions", pa.int64()),
            pa.field("home_latitude", pa.float64()),
            pa.field("home_longitude", pa.float64()),
            pa.field("account_open_date", pa.string()),
        ])
        _create_table_safe(schema, "customer_profiles", profile_schema)

    logger.info("Database schema creation complete")


def _create_table_safe(schema, table_name: str, arrow_schema: pa.Schema):
    """Create a table, logging gracefully if it already exists."""
    try:
        schema.create_table(table_name, arrow_schema)
        logger.info("Created table '%s'", table_name)
    except Exception as exc:
        if "already exists" in str(exc).lower():
            logger.info("Table '%s' already exists", table_name)
        else:
            raise


# ---------------------------------------------------------------------------
# Synthetic data generation
# ---------------------------------------------------------------------------

def _random_card_id(customer_index: int) -> str:
    """Generate a deterministic card ID for a customer index."""
    return f"card_{customer_index:06d}"


def _random_merchant_id() -> str:
    """Generate a random merchant ID."""
    return f"merch_{''.join(random.choices(string.ascii_lowercase + string.digits, k=8))}"


def load_historical_data(session, num_records: int = 5_000_000):
    """
    Generate and insert synthetic 6-month transaction history.

    Data is inserted in batches to avoid memory pressure. Each batch
    contains 50,000 records.
    """
    logger.info("Generating %d synthetic transactions...", num_records)
    batch_size = 50_000
    now = datetime.now(timezone.utc)
    six_months_ago = now - timedelta(days=180)
    total_seconds = int((now - six_months_ago).total_seconds())
    num_customers = 10_000
    num_merchants = 5_000

    # Pre-generate merchant IDs
    merchant_ids = [_random_merchant_id() for _ in range(num_merchants)]

    inserted = 0
    while inserted < num_records:
        current_batch = min(batch_size, num_records - inserted)

        transaction_ids = []
        card_ids = []
        amounts = []
        timestamps = []
        m_ids = []
        categories = []
        latitudes = []
        longitudes = []

        for _ in range(current_batch):
            customer_idx = random.randint(0, num_customers - 1)
            city = random.choice(CITIES)

            transaction_ids.append(str(uuid.uuid4()))
            card_ids.append(_random_card_id(customer_idx))
            amounts.append(round(random.lognormvariate(3.5, 1.2), 2))
            ts = six_months_ago + timedelta(seconds=random.randint(0, total_seconds))
            timestamps.append(ts.isoformat())
            m_ids.append(random.choice(merchant_ids))
            categories.append(random.choice(MERCHANT_CATEGORIES))
            # Add small jitter around city centre
            latitudes.append(city[0] + random.gauss(0, 0.05))
            longitudes.append(city[1] + random.gauss(0, 0.05))

        batch_table = pa.table({
            "transaction_id": transaction_ids,
            "card_id": card_ids,
            "amount": amounts,
            "timestamp": timestamps,
            "merchant_id": m_ids,
            "merchant_category": categories,
            "latitude": latitudes,
            "longitude": longitudes,
            "risk_score": [0.0] * current_batch,
            "triggered_rules": ["[]"] * current_batch,
        })

        try:
            with session.transaction() as tx:
                table = tx.bucket(BUCKET).schema(SCHEMA).table("transactions")
                table.insert(batch_table)
            inserted += current_batch
            logger.info(
                "Inserted %d / %d transactions (%.1f%%)",
                inserted, num_records, 100 * inserted / num_records,
            )
        except Exception:
            logger.exception("Failed to insert batch at offset %d", inserted)
            raise

    logger.info("Historical data load complete: %d records", num_records)


def load_fraud_ring_data(session):
    """
    Insert ~1,000 known fraud ring merchant IDs.

    These are synthetic entries grouped into named fraud rings for
    demo purposes.
    """
    logger.info("Loading fraud ring merchant data...")

    ring_names = [
        "Eastern Syndicate", "Pacific Carders", "Midwest Skimmers",
        "Digital Ghost Ring", "Cross-Border Network", "Shell Corp Alliance",
        "Phantom POS Ring", "Refund Fraud Collective", "Identity Mill",
        "Crypto Laundry Ring",
    ]

    risk_categories = ["organized_crime", "card_skimming", "identity_fraud",
                       "money_laundering", "shell_company"]

    merchant_ids = []
    rings = []
    categories = []
    dates = []

    now = datetime.now(timezone.utc)
    for i in range(1000):
        merchant_ids.append(f"fraud_merch_{i:04d}")
        rings.append(random.choice(ring_names))
        categories.append(random.choice(risk_categories))
        added = now - timedelta(days=random.randint(1, 365))
        dates.append(added.isoformat())

    fraud_table = pa.table({
        "merchant_id": merchant_ids,
        "ring_name": rings,
        "risk_category": categories,
        "added_date": dates,
    })

    try:
        with session.transaction() as tx:
            table = tx.bucket(BUCKET).schema(SCHEMA).table("fraud_ring_merchants")
            table.insert(fraud_table)
        logger.info("Loaded %d fraud ring merchant records", len(merchant_ids))
    except Exception:
        logger.exception("Failed to load fraud ring data")
        raise


def load_customer_profiles(session, num_customers: int = 10_000):
    """
    Insert customer profile records with spending averages and home
    locations.
    """
    logger.info("Loading %d customer profiles...", num_customers)

    card_ids = []
    names = []
    avg_amounts = []
    std_amounts = []
    total_txns = []
    home_lats = []
    home_lons = []
    open_dates = []

    first_names = [
        "James", "Mary", "Robert", "Patricia", "John", "Jennifer",
        "Michael", "Linda", "David", "Elizabeth", "William", "Barbara",
        "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah",
        "Charles", "Karen",
    ]
    last_names = [
        "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
        "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez",
        "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore",
        "Jackson", "Martin",
    ]

    now = datetime.now(timezone.utc)
    for i in range(num_customers):
        city = random.choice(CITIES)
        avg = round(random.lognormvariate(3.5, 0.8), 2)

        card_ids.append(_random_card_id(i))
        names.append(f"{random.choice(first_names)} {random.choice(last_names)}")
        avg_amounts.append(avg)
        std_amounts.append(round(avg * random.uniform(0.3, 0.8), 2))
        total_txns.append(random.randint(50, 2000))
        home_lats.append(city[0] + random.gauss(0, 0.02))
        home_lons.append(city[1] + random.gauss(0, 0.02))
        open_date = now - timedelta(days=random.randint(180, 3650))
        open_dates.append(open_date.isoformat())

    profile_table = pa.table({
        "card_id": card_ids,
        "customer_name": names,
        "avg_amount": avg_amounts,
        "std_amount": std_amounts,
        "total_transactions": total_txns,
        "home_latitude": home_lats,
        "home_longitude": home_lons,
        "account_open_date": open_dates,
    })

    try:
        with session.transaction() as tx:
            table = tx.bucket(BUCKET).schema(SCHEMA).table("customer_profiles")
            table.insert(profile_table)
        logger.info("Loaded %d customer profiles", num_customers)
    except Exception:
        logger.exception("Failed to load customer profiles")
        raise


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    """Run all setup steps in sequence."""
    logger.info("=" * 60)
    logger.info("VAST Fraud Detection Demo -- Cluster Setup")
    logger.info("=" * 60)

    # Validate environment
    for var in ("VAST_ENDPOINT", "VAST_ACCESS_KEY", "VAST_SECRET_KEY"):
        if var not in os.environ:
            logger.error("Missing required environment variable: %s", var)
            sys.exit(1)

    session = vastdb.connect(
        endpoint=os.environ["VAST_ENDPOINT"],
        access_key=os.environ["VAST_ACCESS_KEY"],
        secret_key=os.environ["VAST_SECRET_KEY"],
    )

    t_start = time.perf_counter()

    # Step 1: Print topic creation commands
    create_topics()

    # Step 2: Create database schema
    create_database_schema(session)

    # Step 3: Load synthetic data
    load_fraud_ring_data(session)
    load_customer_profiles(session)
    load_historical_data(session)

    elapsed = time.perf_counter() - t_start
    logger.info("=" * 60)
    logger.info("Setup complete in %.1f seconds", elapsed)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
