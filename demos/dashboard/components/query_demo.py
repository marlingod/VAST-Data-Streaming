"""SQL query comparison panel: VAST topics-as-tables vs Kafka ETL pipeline."""

from __future__ import annotations

from typing import Any

import streamlit as st

from components.html_helper import styled_html


def render_query_demo(vast_result: dict[str, Any], kafka_result: dict[str, Any]) -> None:
    """Render the SQL query comparison with timing results.

    Both result dicts should contain:
        query_name, query, vast: {rows, latency_ms}, kafka: {rows, latency_ms}
    """
    styled_html(
        '<p class="section-title">'
        'SQL Query Comparison &mdash; Topics-as-Tables vs ETL Pipeline'
        '</p>',
    )

    query_name = vast_result.get("query_name", "")
    query_sql = vast_result.get("query", "")
    v = vast_result.get("vast", {})
    k = vast_result.get("kafka", kafka_result.get("kafka", {}))

    v_lat = v.get("latency_ms", 0)
    k_lat = k.get("latency_ms", 0)
    v_rows = v.get("rows", 0)
    k_rows = k.get("rows", 0)

    # Big comparison banner
    speedup = k_lat / max(v_lat, 0.001)
    styled_html(
        f"""
        <div class="big-compare">
            <span class="vast-num">VAST: {v_lat:.1f} ms</span>
            &nbsp;&nbsp;vs&nbsp;&nbsp;
            <span class="kafka-num">Kafka: {k_lat:,.0f} ms</span>
        </div>
        <div style="text-align:center; color:#888; font-size:0.85rem; margin-bottom:12px;">
            VAST is <b style="color:#00B4D8;">{speedup:,.0f}x faster</b> for this query
        </div>
        """,
    )

    # Two-column result cards
    col_v, col_k = st.columns(2)

    with col_v:
        styled_html(
            f"""
            <div class="metric-card vast">
                <div style="font-weight:700; color:#00B4D8; margin-bottom:6px;">
                    VAST DataEngine
                </div>
                <div style="color:#888; font-size:0.78rem; margin-bottom:6px;">
                    Direct SQL on event topics &mdash; zero ETL
                </div>
                <div class="mono-block">{_escape(query_sql)}</div>
                <div style="display:flex; justify-content:space-between; margin-top:10px;">
                    <span style="color:#888;">Rows: <b style="color:#E0E0E0;">{v_rows:,}</b></span>
                    <span class="latency-badge fast">{v_lat:.2f} ms</span>
                </div>
            </div>
            """,
        )

    with col_k:
        styled_html(
            f"""
            <div class="metric-card kafka">
                <div style="font-weight:700; color:#FF6B35; margin-bottom:6px;">
                    Kafka + ClickHouse
                </div>
                <div style="color:#888; font-size:0.78rem; margin-bottom:6px;">
                    ETL pipeline: Kafka &#8594; Flink &#8594; ClickHouse &#8594; query
                </div>
                <div class="mono-block">{_escape(query_sql)}</div>
                <div style="display:flex; justify-content:space-between; margin-top:10px;">
                    <span style="color:#888;">Rows: <b style="color:#E0E0E0;">{k_rows:,}</b></span>
                    <span class="latency-badge slow">{k_lat:,.0f} ms</span>
                </div>
            </div>
            """,
        )


def _escape(text: str) -> str:
    """Basic HTML-escape for display in code blocks."""
    return (
        text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace("\n", "<br>")
            .replace(" ", "&nbsp;")
    )
