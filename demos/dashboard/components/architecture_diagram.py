"""Architecture comparison visualization: VAST (1 system) vs Kafka (6 systems)."""

from __future__ import annotations

import streamlit as st

from components.html_helper import styled_html


def _vast_side() -> str:
    """Return HTML for the VAST unified architecture."""
    components = [
        ("Event Broker", "Streaming ingestion & pub/sub"),
        ("DataBase", "Exabyte-scale storage"),
        ("DataEngine", "SQL engine on topics-as-tables"),
        ("AgentEngine", "AI-driven investigation"),
        ("InsightEngine", "Real-time analytics"),
    ]

    items_html = ""
    for name, desc in components:
        items_html += f"""
        <div style="display:flex; align-items:center; padding:6px 10px;
                    background:rgba(0,180,216,0.07); border-radius:6px; margin:4px 0;">
            <span class="status-dot green"></span>
            <div>
                <span style="font-weight:600; color:#00B4D8; font-size:0.88rem;">{name}</span>
                <span style="color:#888; font-size:0.75rem; margin-left:6px;">{desc}</span>
            </div>
        </div>
        """

    return f"""
    <div class="metric-card vast" style="text-align:center;">
        <div style="font-size:1.1rem; font-weight:700; color:#00B4D8; margin-bottom:10px;">
            VAST Data Platform
        </div>
        <div style="background:rgba(0,180,216,0.05); border:2px solid #00B4D8;
                    border-radius:12px; padding:14px 10px;">
            {items_html}
        </div>
        <div style="margin-top:12px; font-size:1.3rem; font-weight:800; color:#00B4D8;">
            1 System
        </div>
        <div style="color:#888; font-size:0.82rem;">
            Single platform, zero ETL, instant queries
        </div>
    </div>
    """


def _kafka_side() -> str:
    """Return HTML for the Kafka multi-component architecture."""
    components = [
        ("Kafka Brokers", "Message streaming"),
        ("ZooKeeper", "Coordination & metadata"),
        ("Schema Registry", "Schema management"),
        ("Apache Flink", "Stream processing"),
        ("ClickHouse", "Analytics database"),
        ("MinIO / S3", "Object storage"),
    ]

    items_html = ""
    for i, (name, desc) in enumerate(components):
        # Draw connector arrows between components
        arrow = ""
        if i < len(components) - 1:
            arrow = (
                '<div style="text-align:center; color:#FF6B35; font-size:0.7rem; '
                'line-height:1; margin:1px 0;">&#9660;</div>'
            )

        items_html += f"""
        <div style="display:flex; align-items:center; padding:6px 10px;
                    background:rgba(255,107,53,0.07); border:1px solid rgba(255,107,53,0.3);
                    border-radius:6px; margin:2px 0;">
            <span class="status-dot orange"></span>
            <div>
                <span style="font-weight:600; color:#FF6B35; font-size:0.88rem;">{name}</span>
                <span style="color:#888; font-size:0.75rem; margin-left:6px;">{desc}</span>
            </div>
        </div>
        {arrow}
        """

    return f"""
    <div class="metric-card kafka" style="text-align:center;">
        <div style="font-size:1.1rem; font-weight:700; color:#FF6B35; margin-bottom:10px;">
            Apache Kafka Stack
        </div>
        <div style="padding:8px 10px;">
            {items_html}
        </div>
        <div style="margin-top:12px; font-size:1.3rem; font-weight:800; color:#FF6B35;">
            6 Systems
        </div>
        <div style="color:#888; font-size:0.82rem;">
            Complex ETL pipeline, multiple failure points
        </div>
    </div>
    """


def render_architecture_comparison() -> None:
    """Render side-by-side architecture comparison."""
    styled_html('<p class="section-title">Architecture Comparison</p>')

    col_v, col_k = st.columns(2)

    with col_v:
        styled_html(_vast_side())

    with col_k:
        styled_html(_kafka_side())
