#!/usr/bin/env python3
"""
VAST Blob Expansion Setup for Fraud Detection Demo
====================================================

This script creates a blob expansion on the fraud.transactions.raw Kafka topic,
turning raw JSON blobs into structured, SQL-queryable columns.

This is the "topics-as-tables" magic — the core VAST demo differentiator.

Usage:
    python blob_expansion_setup.py \
        --endpoint https://<VAST_VMS_ENDPOINT> \
        --access-key <ACCESS_KEY> \
        --secret-key <SECRET_KEY>

Prerequisites:
    - VAST cluster with Event Broker enabled
    - Kafka topics already created (fraud.transactions.raw, etc.)
    - pip install vastdb
"""

import argparse
import os
import sys

import pyarrow as pa
import vastdb
from vastdb.table import BlobExpansionConfig, ExpansionFormat

# ---------------------------------------------------------------------------
# Configuration — adjust to match your VAST cluster
# ---------------------------------------------------------------------------
KAFKA_BUCKET = os.getenv("VAST_KAFKA_BUCKET", "yg-bucket")
KAFKA_SCHEMA = os.getenv("VAST_KAFKA_SCHEMA", "kafka_topics")
TARGET_SCHEMA = os.getenv("VAST_TARGET_SCHEMA", "fraud_detection")

# Source topics (Kafka topic names = source table names)
TOPICS = [
    "fraud.transactions.raw",
    "fraud.alerts",
    "fraud.metrics",
    "fraud.transactions.scored",
]

# ---------------------------------------------------------------------------
# Expansion schemas — defines the JSON fields to extract per topic
# ---------------------------------------------------------------------------
TRANSACTION_SCHEMA = pa.schema([
    pa.field("transaction_id", pa.string()),
    pa.field("timestamp", pa.string()),
    pa.field("card_id", pa.string()),
    pa.field("customer_id", pa.string()),
    pa.field("merchant_id", pa.string()),
    pa.field("merchant_category", pa.string()),
    pa.field("amount", pa.float64()),
    pa.field("currency", pa.string()),
    pa.field("location_lat", pa.float64()),
    pa.field("location_lon", pa.float64()),
    pa.field("location_city", pa.string()),
    pa.field("device_fingerprint", pa.string()),
    pa.field("channel", pa.string()),
    pa.field("is_fraud", pa.bool_()),
])

ALERT_SCHEMA = pa.schema([
    pa.field("transaction_id", pa.string()),
    pa.field("timestamp", pa.string()),
    pa.field("card_id", pa.string()),
    pa.field("amount", pa.float64()),
    pa.field("risk_score", pa.float64()),
    pa.field("fraud_type", pa.string()),
])

METRICS_SCHEMA = pa.schema([
    pa.field("source", pa.string()),
    pa.field("timestamp", pa.string()),
    pa.field("msgs_per_sec", pa.float64()),
    pa.field("total_sent", pa.int64()),
])

# Map topic -> (target table name, expansion schema)
TOPIC_CONFIG = {
    "fraud.transactions.raw": ("transactions", TRANSACTION_SCHEMA),
    "fraud.alerts": ("alerts", ALERT_SCHEMA),
    "fraud.metrics": ("metrics", METRICS_SCHEMA),
}


def setup_blob_expansion(endpoint: str, access_key: str, secret_key: str):
    """Create target schema, tables, and blob expansions for all topics."""

    print(f"Connecting to VAST at {endpoint}...")
    session = vastdb.connect(
        access_key=access_key,
        secret_key=secret_key,
        endpoint=endpoint,
    )

    # Step 1: Create the target schema
    print(f"\n[1/3] Creating target schema '{TARGET_SCHEMA}' in bucket '{KAFKA_BUCKET}'...")
    try:
        with session.transaction() as tx:
            tx.bucket(KAFKA_BUCKET).create_schema(TARGET_SCHEMA)
        print(f"  Created schema: {TARGET_SCHEMA}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"  Schema '{TARGET_SCHEMA}' already exists — OK")
        else:
            print(f"  Warning: {e}")

    # Step 2: Create target tables
    print(f"\n[2/3] Creating target tables...")
    for topic_name, (table_name, schema) in TOPIC_CONFIG.items():
        try:
            with session.transaction() as tx:
                tx.bucket(KAFKA_BUCKET).schema(TARGET_SCHEMA).create_table(
                    table_name, schema, sorting_key=[]
                )
            print(f"  Created table: {TARGET_SCHEMA}.{table_name} ({len(schema)} columns)")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"  Table '{table_name}' already exists — OK")
            else:
                print(f"  Warning: {e}")

    # Step 3: Create blob expansions
    print(f"\n[3/3] Creating blob expansions (topics-as-tables)...")
    for topic_name, (table_name, schema) in TOPIC_CONFIG.items():
        try:
            with session.transaction() as tx:
                source_table = tx.bucket(KAFKA_BUCKET).schema(KAFKA_SCHEMA).table(topic_name)
                source_table.create_blob_expansion(
                    expansion_schema=schema,
                    target_table_name=table_name,
                    target_table_schema=TARGET_SCHEMA,
                    source_column_name="value",
                    config=BlobExpansionConfig(
                        expansion_format=ExpansionFormat("json"),
                        copy_source_column=False,
                        flatten_path=False,
                        flatten_delimiter="__",
                    ),
                )
            print(f"  Linked: {topic_name} -> {TARGET_SCHEMA}.{table_name}")
            print(f"    Columns: {', '.join(f.name for f in schema)}")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"  Expansion for '{topic_name}' already exists — OK")
            else:
                print(f"  Error: {e}")

    print("\n" + "=" * 60)
    print("Blob expansion setup complete!")
    print(f"Your 10M+ transactions are now queryable as structured SQL tables.")
    print(f"\nTarget tables:")
    for topic_name, (table_name, schema) in TOPIC_CONFIG.items():
        print(f"  {KAFKA_BUCKET}.{TARGET_SCHEMA}.{table_name} ({len(schema)} columns)")
    print("=" * 60)


def parse_args():
    parser = argparse.ArgumentParser(
        description="VAST Blob Expansion Setup — turns Kafka topics into SQL tables",
    )
    parser.add_argument(
        "--endpoint",
        default=os.getenv("VAST_ENDPOINT", ""),
        help="VAST VMS/S3 endpoint (e.g., https://vms.example.com)",
    )
    parser.add_argument(
        "--access-key",
        default=os.getenv("VAST_ACCESS_KEY", ""),
        help="VAST S3 access key",
    )
    parser.add_argument(
        "--secret-key",
        default=os.getenv("VAST_SECRET_KEY", ""),
        help="VAST S3 secret key",
    )
    parser.add_argument(
        "--kafka-bucket",
        default=KAFKA_BUCKET,
        help=f"Bucket containing Kafka topics (default: {KAFKA_BUCKET})",
    )
    parser.add_argument(
        "--kafka-schema",
        default=KAFKA_SCHEMA,
        help=f"Schema containing Kafka topics (default: {KAFKA_SCHEMA})",
    )
    parser.add_argument(
        "--target-schema",
        default=TARGET_SCHEMA,
        help=f"Target schema for expanded tables (default: {TARGET_SCHEMA})",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if not args.endpoint or not args.access_key or not args.secret_key:
        print("ERROR: Provide --endpoint, --access-key, --secret-key")
        print("       Or set VAST_ENDPOINT, VAST_ACCESS_KEY, VAST_SECRET_KEY env vars")
        sys.exit(1)

    # Override globals from CLI args
    KAFKA_BUCKET = args.kafka_bucket
    KAFKA_SCHEMA = args.kafka_schema
    TARGET_SCHEMA = args.target_schema

    setup_blob_expansion(args.endpoint, args.access_key, args.secret_key)
