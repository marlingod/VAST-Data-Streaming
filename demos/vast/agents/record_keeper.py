"""
Audit trail agent for fraud investigations.

The ``RecordKeeper`` writes immutable audit records to VAST DataBase
after every investigation, ensuring full traceability for compliance,
regulatory reporting, and internal review.

All records are append-only -- VAST's immutable storage guarantees
that audit entries cannot be altered after creation.
"""

import json
import logging
import time
import uuid
from datetime import datetime, timezone, timedelta

import pyarrow as pa
import vastdb

logger = logging.getLogger(__name__)


class RecordKeeper:
    """Immutable audit trail backed by VAST DataBase."""

    def __init__(self, session, bucket_name: str, schema_name: str):
        self.session = session
        self.bucket_name = bucket_name
        self.schema_name = schema_name

    def log_investigation(self, alert: dict, investigation: dict) -> str:
        """
        Write an immutable audit record for a completed investigation.

        Parameters
        ----------
        alert : dict
            The original fraud alert payload.
        investigation : dict
            The investigation report produced by ``DeepDiveAgent``.

        Returns
        -------
        str
            The generated ``audit_id`` for the new record.
        """
        audit_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()

        # Summarise evidence into a single string for the audit column
        evidence_items = investigation.get("evidence", [])
        evidence_summary = "; ".join(evidence_items) if evidence_items else "No evidence collected"

        record = pa.table({
            "audit_id": [audit_id],
            "timestamp": [now],
            "card_id": [alert.get("card_id", "unknown")],
            "transaction_id": [alert.get("transaction_id", "unknown")],
            "risk_level": [investigation.get("risk_level", "unknown")],
            "recommended_action": [investigation.get("recommended_action", "unknown")],
            "evidence_summary": [evidence_summary],
            "investigator": ["DeepDiveAgent"],
            "investigation_duration_ms": [
                investigation.get("investigation_duration_ms", 0.0)
            ],
        })

        try:
            with self.session.transaction() as tx:
                table = (
                    tx.bucket(self.bucket_name)
                    .schema(self.schema_name)
                    .table("audit_trail")
                )
                table.insert(record)
            logger.info(
                "Audit record %s written for transaction %s (card %s)",
                audit_id,
                alert.get("transaction_id"),
                alert.get("card_id"),
            )
        except Exception:
            logger.exception("Failed to write audit record %s", audit_id)
            raise

        return audit_id

    def query_audit_trail(self, card_id: str, days: int = 90) -> list[dict]:
        """
        Retrieve audit records for a specific card.

        Parameters
        ----------
        card_id : str
            The card to query.
        days : int
            How far back to look (default 90 days).

        Returns
        -------
        list[dict]
            Audit records ordered by timestamp.
        """
        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=days)
        ).isoformat()

        try:
            with self.session.transaction() as tx:
                table = (
                    tx.bucket(self.bucket_name)
                    .schema(self.schema_name)
                    .table("audit_trail")
                )
                reader = table.select(
                    columns=[
                        "audit_id", "timestamp", "card_id", "transaction_id",
                        "risk_level", "recommended_action", "evidence_summary",
                        "investigator", "investigation_duration_ms",
                    ],
                    predicate=(
                        f"card_id = '{card_id}' AND timestamp >= '{cutoff}'"
                    ),
                )
                result = reader.read_all()
                records = result.to_pylist()

            logger.info(
                "Retrieved %d audit records for card %s (last %d days)",
                len(records), card_id, days,
            )
            return records

        except Exception:
            logger.exception(
                "Failed to query audit trail for card %s", card_id
            )
            return []
