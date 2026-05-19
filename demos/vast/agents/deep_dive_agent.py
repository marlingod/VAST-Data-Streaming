"""
AI investigation agent using VAST AgentEngine / InsightEngine concepts.

The ``DeepDiveAgent`` receives a fraud alert and performs an automated,
multi-step investigation:

1. Pull full 12-month transaction history for the card.
2. Analyse merchant patterns and categories.
3. Check against the fraud ring database (vector-search simulation).
4. Produce a structured investigation report with evidence, risk level,
   recommended action, and a human-readable summary.

In production this would be backed by a real LLM via VAST InsightEngine;
for demo purposes the summary is generated from template strings so the
demo can run without external API keys.
"""

import logging
import time
from collections import Counter
from datetime import datetime, timezone, timedelta

import vastdb

logger = logging.getLogger(__name__)


class DeepDiveAgent:
    """Automated fraud investigation agent backed by VAST DataBase."""

    def __init__(self, session, bucket_name: str, schema_name: str):
        self.session = session
        self.bucket_name = bucket_name
        self.schema_name = schema_name

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def investigate(self, alert: dict) -> dict:
        """
        Run a full investigation for a fraud alert.

        Parameters
        ----------
        alert : dict
            Alert payload with at least ``card_id``, ``transaction_id``,
            ``risk_score``, ``triggered_rules``, ``merchant_id``, and
            ``amount``.

        Returns
        -------
        dict
            Structured investigation report.
        """
        t_start = time.perf_counter()
        card_id = alert.get("card_id", "unknown")
        logger.info("Starting investigation for card %s", card_id)

        # Step 1 -- full 12-month transaction history
        history = self._pull_card_history(card_id, months=12)

        # Step 2 -- merchant analysis
        merchant_analysis = self._analyse_merchants(
            history, alert.get("merchant_id")
        )

        # Step 3 -- fraud ring check
        fraud_ring_matches = self._check_fraud_ring(alert.get("merchant_id"))

        # Step 4 -- build evidence list
        evidence = self._compile_evidence(alert, history, merchant_analysis, fraud_ring_matches)

        # Step 5 -- determine risk level and recommended action
        risk_level = self._assess_risk_level(alert, evidence)
        recommended_action = self._recommend_action(risk_level, evidence)

        # Step 6 -- regulatory watchlist flags
        regulatory_flags = self._check_regulatory_flags(alert, history)

        # Step 7 -- generate human-readable summary
        investigation_duration_ms = (time.perf_counter() - t_start) * 1000
        summary = self._generate_summary(
            alert, risk_level, evidence, merchant_analysis,
            recommended_action, investigation_duration_ms,
        )

        report = {
            "card_id": card_id,
            "transaction_id": alert.get("transaction_id"),
            "risk_level": risk_level,
            "evidence": evidence,
            "merchant_analysis": merchant_analysis,
            "recommended_action": recommended_action,
            "regulatory_flags": regulatory_flags,
            "investigation_summary": summary,
            "investigation_duration_ms": round(investigation_duration_ms, 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        logger.info(
            "Investigation complete for card %s: risk=%s, action=%s (%.2fms)",
            card_id, risk_level, recommended_action, investigation_duration_ms,
        )
        return report

    # ------------------------------------------------------------------
    # Internal: data retrieval
    # ------------------------------------------------------------------

    def _pull_card_history(self, card_id: str, months: int = 12) -> list[dict]:
        """Retrieve full transaction history from VAST DataBase."""
        try:
            with self.session.transaction() as tx:
                table = (
                    tx.bucket(self.bucket_name)
                    .schema(self.schema_name)
                    .table("transactions")
                )
                cutoff = (
                    datetime.now(timezone.utc) - timedelta(days=months * 30)
                ).isoformat()
                reader = table.select(
                    columns=[
                        "transaction_id", "card_id", "amount", "timestamp",
                        "merchant_id", "merchant_category", "latitude", "longitude",
                    ],
                    predicate=f"card_id = '{card_id}' AND timestamp >= '{cutoff}'",
                )
                return reader.read_all().to_pylist()
        except Exception:
            logger.exception("Failed to pull history for card %s", card_id)
            return []

    def _check_fraud_ring(self, merchant_id: str | None) -> list[str]:
        """Check if the merchant appears in the fraud ring database."""
        if not merchant_id:
            return []
        try:
            with self.session.transaction() as tx:
                table = (
                    tx.bucket(self.bucket_name)
                    .schema(self.schema_name)
                    .table("fraud_ring_merchants")
                )
                reader = table.select(
                    columns=["merchant_id", "ring_name", "risk_category"],
                    predicate=f"merchant_id = '{merchant_id}'",
                )
                rows = reader.read_all().to_pylist()
                return [
                    f"Merchant {merchant_id} linked to fraud ring "
                    f"'{r.get('ring_name', 'unknown')}' "
                    f"(category: {r.get('risk_category', 'unclassified')})"
                    for r in rows
                ]
        except Exception:
            logger.exception("Fraud ring lookup failed for merchant %s", merchant_id)
            return []

    # ------------------------------------------------------------------
    # Internal: analysis
    # ------------------------------------------------------------------

    def _analyse_merchants(
        self, history: list[dict], current_merchant_id: str | None
    ) -> dict:
        """Build a merchant risk profile from transaction history."""
        if not history:
            return {
                "total_merchants": 0,
                "top_categories": [],
                "current_merchant_frequency": 0,
                "risk_profile": "insufficient_data",
            }

        category_counter: Counter = Counter()
        merchant_counter: Counter = Counter()
        amounts_by_merchant: dict[str, list[float]] = {}

        for txn in history:
            cat = txn.get("merchant_category", "unknown")
            mid = txn.get("merchant_id", "unknown")
            category_counter[cat] += 1
            merchant_counter[mid] += 1
            amounts_by_merchant.setdefault(mid, []).append(txn.get("amount", 0))

        current_freq = merchant_counter.get(current_merchant_id, 0) if current_merchant_id else 0
        total_merchants = len(merchant_counter)

        # Determine risk profile
        if current_freq == 0 and current_merchant_id:
            risk_profile = "new_merchant"
        elif current_freq <= 2:
            risk_profile = "rarely_used"
        else:
            risk_profile = "established"

        return {
            "total_merchants": total_merchants,
            "top_categories": category_counter.most_common(5),
            "current_merchant_frequency": current_freq,
            "risk_profile": risk_profile,
        }

    def _compile_evidence(
        self,
        alert: dict,
        history: list[dict],
        merchant_analysis: dict,
        fraud_ring_matches: list[str],
    ) -> list[str]:
        """Build a list of human-readable evidence items."""
        evidence: list[str] = []

        # Triggered rules
        for rule in alert.get("triggered_rules", []):
            score = alert.get("rule_scores", {}).get(rule, 0.0)
            evidence.append(f"Rule '{rule}' triggered with score {score:.2f}")

        # Composite score
        evidence.append(
            f"Composite risk score: {alert.get('risk_score', 0):.4f} "
            f"(threshold: 0.8)"
        )

        # Historical context
        if history:
            total_amount = sum(t.get("amount", 0) for t in history)
            avg_amount = total_amount / len(history) if history else 0
            evidence.append(
                f"12-month history: {len(history)} transactions, "
                f"avg amount ${avg_amount:.2f}"
            )
            current_amount = alert.get("amount", 0)
            if avg_amount > 0 and current_amount > avg_amount * 3:
                evidence.append(
                    f"Current amount ${current_amount:.2f} is "
                    f"{current_amount / avg_amount:.1f}x the historical average"
                )

        # Merchant analysis
        if merchant_analysis.get("risk_profile") == "new_merchant":
            evidence.append("Transaction at a merchant never used by this cardholder")
        elif merchant_analysis.get("risk_profile") == "rarely_used":
            evidence.append(
                f"Merchant used only {merchant_analysis['current_merchant_frequency']} "
                f"time(s) in 12 months"
            )

        # Fraud ring
        evidence.extend(fraud_ring_matches)

        return evidence

    def _assess_risk_level(self, alert: dict, evidence: list[str]) -> str:
        """Classify the overall risk as high / medium / low."""
        score = alert.get("risk_score", 0)
        triggered = alert.get("triggered_rules", [])

        if score >= 0.9 or "fraud_ring" in triggered:
            return "high"
        elif score >= 0.6 or len(triggered) >= 3:
            return "medium"
        return "low"

    def _recommend_action(self, risk_level: str, evidence: list[str]) -> str:
        """Determine the recommended action based on risk level."""
        fraud_ring_involved = any("fraud ring" in e.lower() for e in evidence)

        if risk_level == "high" or fraud_ring_involved:
            return "block"
        elif risk_level == "medium":
            return "flag_for_review"
        return "allow"

    def _check_regulatory_flags(
        self, alert: dict, history: list[dict]
    ) -> list[str]:
        """Check for regulatory or watchlist matches."""
        flags: list[str] = []
        amount = alert.get("amount", 0)

        # Structuring detection: multiple transactions just under $10,000
        if history:
            recent_large = [
                t for t in history
                if 9000 <= t.get("amount", 0) < 10000
            ]
            if len(recent_large) >= 3:
                flags.append(
                    "Potential structuring detected: "
                    f"{len(recent_large)} transactions between $9,000-$10,000"
                )

        # Large single transaction
        if amount >= 10000:
            flags.append(
                f"Transaction of ${amount:,.2f} requires CTR filing "
                f"(Currency Transaction Report)"
            )

        return flags

    # ------------------------------------------------------------------
    # Internal: summary generation (template-based for demo)
    # ------------------------------------------------------------------

    def _generate_summary(
        self,
        alert: dict,
        risk_level: str,
        evidence: list[str],
        merchant_analysis: dict,
        recommended_action: str,
        duration_ms: float,
    ) -> str:
        """Generate a human-readable investigation summary."""
        card_id = alert.get("card_id", "unknown")
        txn_id = alert.get("transaction_id", "unknown")
        amount = alert.get("amount", 0)
        score = alert.get("risk_score", 0)
        rules = alert.get("triggered_rules", [])

        action_text = {
            "block": "Immediate card block and customer notification recommended.",
            "flag_for_review": (
                "Transaction flagged for manual review by the fraud operations team."
            ),
            "allow": "No immediate action required; continue monitoring.",
        }

        summary_parts = [
            f"INVESTIGATION REPORT -- Card {card_id}",
            f"Transaction {txn_id} for ${amount:,.2f} "
            f"scored {score:.4f} (risk level: {risk_level.upper()}).",
            "",
            f"Triggered rules: {', '.join(rules) if rules else 'none'}.",
            f"Merchant risk profile: {merchant_analysis.get('risk_profile', 'unknown')}.",
            "",
            "Evidence:",
        ]
        for i, item in enumerate(evidence, 1):
            summary_parts.append(f"  {i}. {item}")

        summary_parts.extend([
            "",
            f"Recommended action: {recommended_action.upper()}",
            action_text.get(recommended_action, ""),
            "",
            f"Investigation completed in {duration_ms:.0f}ms by DeepDiveAgent.",
        ])

        return "\n".join(summary_parts)
