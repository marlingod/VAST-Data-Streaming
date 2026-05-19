#!/usr/bin/env python3
"""
Create Kafka Views for Fraud Detection Demo via VAST VMS API.

Creates Kafka-compatible broker views for the scored and alerts topics,
since the raw and metrics topics were already created via the VAST Admin UI.

Usage:
    # Via Python script
    python create_kafka_views.py \
        --vms-address <VMS_HOSTNAME> \
        --username <USER> \
        --password <PASS>

    # Or via vastpy-cli directly:
    vastpy-cli post views \
        path=/yg-bucket/kafka_topics/fraud.transactions.scored \
        protocols=KAFKA \
        policy_id=<POLICY_ID> \
        create_dir=true

    # List existing views to find Kafka views
    vastpy-cli get views protocols=KAFKA
"""

import argparse
import json
import os
import sys

try:
    from vastpy import VASTClient
except ImportError:
    print("ERROR: pip install vastpy")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BUCKET = os.getenv("VAST_KAFKA_BUCKET", "yg-bucket")

# Topics that need Kafka views created
# (fraud.transactions.raw, fraud.alerts, fraud.metrics already exist from UI)
TOPICS_TO_CREATE = [
    {
        "name": "fraud.transactions.scored",
        "partitions": 6,
        "retention_days": 7,
        "description": "Fraud-scored transactions with risk scores",
    },
]


def discover_api(client):
    """Discover available Kafka-related API endpoints."""
    print("\n[Discovery] Checking available API endpoints...")

    # Try common VAST API endpoints for Kafka/views
    endpoints = [
        "views",
        "kafkabrokers",
        "kafka_brokers",
        "kafkatopics",
        "kafka_topics",
        "kafkaviews",
        "kafka_views",
        "messagebrokers",
        "message_brokers",
        "eventbrokers",
        "event_brokers",
    ]

    for ep in endpoints:
        try:
            result = getattr(client, ep).get()
            print(f"  {ep}: OK ({len(result) if isinstance(result, list) else 'found'})")
        except Exception as e:
            err_str = str(e)
            if "404" in err_str:
                pass  # endpoint doesn't exist
            elif "401" in err_str or "403" in err_str:
                print(f"  {ep}: EXISTS (auth required)")
            else:
                print(f"  {ep}: {err_str[:80]}")


def list_views(client):
    """List existing views to find Kafka-related ones."""
    print("\n[Views] Listing existing views...")
    try:
        views = client.views.get()
        kafka_views = []
        for v in views:
            protocols = v.get("protocols", [])
            path = v.get("path", "")
            if "KAFKA" in protocols or "kafka" in str(protocols).lower() or "fraud" in path.lower():
                kafka_views.append(v)
                print(f"  ID={v.get('id')} | path={path} | protocols={protocols}")

        if not kafka_views:
            print("  No Kafka views found. Showing all views:")
            for v in views[:10]:
                print(f"  ID={v.get('id')} | path={v.get('path')} | protocols={v.get('protocols')}")
            if len(views) > 10:
                print(f"  ... and {len(views) - 10} more")

        return views
    except Exception as e:
        print(f"  Error: {e}")
        return []


def create_kafka_view(client, topic_config, policy_id):
    """Create a Kafka view for a topic."""
    name = topic_config["name"]
    path = f"/{BUCKET}/kafka_topics/{name}"

    print(f"\n[Create] Creating Kafka view for '{name}'...")
    print(f"  Path: {path}")
    print(f"  Partitions: {topic_config['partitions']}")

    try:
        result = client.views.post(
            path=path,
            protocols=["KAFKA"],
            policy_id=policy_id,
            create_dir=True,
            # Kafka-specific settings
            kafka_partitions=topic_config["partitions"],
            kafka_retention_days=topic_config.get("retention_days", 7),
        )
        print(f"  Created: ID={result.get('id', 'unknown')}")
        return result
    except Exception as e:
        err_str = str(e)
        if "already exists" in err_str.lower():
            print(f"  Already exists — OK")
        else:
            print(f"  Error: {e}")
            print(f"  Try manually via vastpy-cli:")
            print(f"    vastpy-cli post views path={path} protocols=KAFKA policy_id={policy_id} create_dir=true")
        return None


def main():
    parser = argparse.ArgumentParser(description="Create VAST Kafka views for fraud detection demo")
    parser.add_argument("--vms-address", default=os.getenv("VAST_VMS_ADDRESS", ""), help="VAST VMS hostname")
    parser.add_argument("--username", default=os.getenv("VAST_USERNAME", ""), help="VMS username")
    parser.add_argument("--password", default=os.getenv("VAST_PASSWORD", ""), help="VMS password")
    parser.add_argument("--token", default=os.getenv("VAST_TOKEN", ""), help="VMS API token (alternative to user/pass)")
    parser.add_argument("--policy-id", type=int, default=1, help="View policy ID (default: 1)")
    parser.add_argument("--discover", action="store_true", help="Discover available API endpoints")
    parser.add_argument("--list-only", action="store_true", help="Only list existing views")
    args = parser.parse_args()

    if not args.vms_address:
        print("ERROR: Provide --vms-address or set VAST_VMS_ADDRESS")
        sys.exit(1)

    # Connect
    if args.token:
        client = VASTClient(address=args.vms_address, token=args.token)
    elif args.username and args.password:
        client = VASTClient(address=args.vms_address, user=args.username, password=args.password)
    else:
        print("ERROR: Provide --username/--password or --token")
        sys.exit(1)

    print(f"Connected to VAST VMS at {args.vms_address}")

    if args.discover:
        discover_api(client)
        return

    if args.list_only:
        list_views(client)
        return

    # List existing views first
    list_views(client)

    # Create Kafka views
    for topic in TOPICS_TO_CREATE:
        create_kafka_view(client, topic, args.policy_id)

    print("\n" + "=" * 60)
    print("Done. Verify in VAST Admin UI under yg-bucket > Kafka-Compatible Broker Topics")
    print("=" * 60)


if __name__ == "__main__":
    main()
