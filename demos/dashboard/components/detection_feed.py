"""Live fraud detection alert feed component."""

from __future__ import annotations

from typing import Any

import streamlit as st

from components.html_helper import styled_html


def render_detection_feed(alerts: list[dict[str, Any]]) -> None:
    """Render a scrollable list of recent fraud alerts."""

    styled_html('<p class="section-title">Live Fraud Detection Feed</p>')

    if not alerts:
        st.info("Waiting for fraud alerts...")
        return

    # Limit to most recent 30 for display
    display_alerts = alerts[:30]

    feed_html_parts: list[str] = []
    feed_html_parts.append('<div class="feed-scroll">')

    for alert in display_alerts:
        is_vast = alert.get("detected_by") == "VAST"
        css_class = "alert-item" if is_vast else "alert-item kafka-alert"
        badge_color = "#00B4D8" if is_vast else "#FF6B35"
        badge_label = alert.get("detected_by", "VAST")

        ts_raw = alert.get("timestamp", "")
        ts_display = ts_raw[11:19] if len(ts_raw) > 19 else ts_raw

        amount = alert.get("amount", 0)
        risk = alert.get("risk_score", 0)
        fraud_type = alert.get("fraud_type", "Unknown")
        card_id = alert.get("card_id", "****-****-****-0000")

        vast_lat = alert.get("vast_latency_ms", 0)
        kafka_lat = alert.get("kafka_latency_ms", 0)

        faster_label = ""
        if vast_lat < kafka_lat:
            speedup = kafka_lat / max(vast_lat, 0.01)
            faster_label = (
                f'<span style="color:#00B4D8; font-size:0.78rem;">'
                f'VAST first ({speedup:.0f}x faster)</span>'
            )
        else:
            faster_label = (
                '<span style="color:#FF6B35; font-size:0.78rem;">'
                'Kafka first</span>'
            )

        feed_html_parts.append(f"""
        <div class="{css_class}">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:4px;">
                <span style="color:#888; font-size:0.78rem; font-family:monospace;">{ts_display}</span>
                <span style="background:{badge_color}22; color:{badge_color}; padding:1px 8px;
                             border-radius:4px; font-size:0.75rem; font-weight:600;">
                    {badge_label}
                </span>
            </div>
            <div style="display:flex; justify-content:space-between; align-items:baseline;">
                <span style="font-weight:600; color:#E0E0E0;">{fraud_type}</span>
                <span style="font-family:monospace; font-weight:700; color:#E74C3C;">
                    ${amount:,.2f}
                </span>
            </div>
            <div style="display:flex; justify-content:space-between; margin-top:3px; font-size:0.8rem; color:#A0A0B8;">
                <span>{card_id}</span>
                <span>Risk: <b style="color:{'#E74C3C' if risk > 0.9 else '#F39C12'}">{risk:.0%}</b></span>
            </div>
            <div style="display:flex; justify-content:space-between; margin-top:4px; font-size:0.78rem;">
                <span style="color:#A0A0B8;">
                    VAST: <b style="color:#00B4D8;">{vast_lat:.2f}ms</b>
                    &nbsp;|&nbsp;
                    Kafka: <b style="color:#FF6B35;">{kafka_lat:.1f}ms</b>
                </span>
                {faster_label}
            </div>
        </div>
        """)

    feed_html_parts.append("</div>")
    styled_html("".join(feed_html_parts))
