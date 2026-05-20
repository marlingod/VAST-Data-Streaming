"""
VAST vs Kafka: Financial Fraud Detection Comparison Dashboard
=============================================================

A Streamlit application that renders side-by-side performance metrics
for VAST Data Platform (Event Broker) and Apache Kafka, highlighting
VAST's advantages in latency, throughput, and operational simplicity.

Run:
    DEMO_MODE=true streamlit run demos/dashboard/app.py

Environment variables:
    DEMO_MODE          - "true" to use simulated data (default: true)
    VAST_BOOTSTRAP     - VAST Event Broker bootstrap servers
    KAFKA_BOOTSTRAP    - Apache Kafka bootstrap servers
    REFRESH_INTERVAL   - Dashboard refresh interval in seconds (default: 1)
"""

from __future__ import annotations

import os
import time

import streamlit as st

# ---------------------------------------------------------------------------
# Page configuration (must be first Streamlit call)
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="VAST vs Kafka: Fraud Detection Demo",
    page_icon=":shield:",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# Import components (styled_html injects CSS automatically per call)
# ---------------------------------------------------------------------------
from components.html_helper import styled_html  # noqa: E402
from components.latency_chart import render_latency_chart  # noqa: E402
from components.throughput_chart import render_throughput_chart  # noqa: E402
from components.detection_feed import render_detection_feed  # noqa: E402
from components.architecture_diagram import render_architecture_comparison  # noqa: E402
from components.query_demo import render_query_demo  # noqa: E402
from metrics_collector import (  # noqa: E402
    DemoMetricsCollector,
    MetricsCollector,
    NAMED_QUERIES,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# Auto-detect: if .env exists with real credentials, default to live mode
_env_file_exists = os.path.exists(os.path.join(os.path.dirname(__file__), "..", ".env"))
_default_demo_mode = "false" if _env_file_exists else "true"
DEMO_MODE = os.getenv("DEMO_MODE", _default_demo_mode).lower() in ("true", "1", "yes")
VAST_BOOTSTRAP = os.getenv("VAST_BOOTSTRAP", "localhost:9092")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
REFRESH_INTERVAL = int(os.getenv("REFRESH_INTERVAL", "1"))

# ---------------------------------------------------------------------------
# Initialise metrics collector (persisted across reruns via session state)
# ---------------------------------------------------------------------------
if "collector" not in st.session_state:
    if DEMO_MODE:
        st.session_state.collector = DemoMetricsCollector()
    else:
        st.session_state.collector = MetricsCollector(VAST_BOOTSTRAP, KAFKA_BOOTSTRAP)

collector = st.session_state.collector

# ---------------------------------------------------------------------------
# Auto-refresh via streamlit-autorefresh (fallback: manual rerun button)
# ---------------------------------------------------------------------------
try:
    from streamlit_autorefresh import st_autorefresh
    st_autorefresh(interval=REFRESH_INTERVAL * 1000, limit=None, key="auto_refresh")
except ImportError:
    # Graceful fallback when the package is not installed
    pass

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
styled_html(
    """
    <div style="text-align:center; padding:0.6rem 0 1.2rem 0;">
        <h1 style="font-size:1.8rem; font-weight:800; margin-bottom:0.15rem; color:#E0E0E0;">
            Financial Fraud Detection
        </h1>
        <p style="font-size:1.05rem; color:#888; margin:0;">
            VAST Event Broker &nbsp;vs&nbsp; Apache Kafka &mdash; Real-time Performance Comparison
        </p>
    </div>
    """,
)

# Mode badge
if DEMO_MODE:
    styled_html(
        '<div style="text-align:center; margin-bottom:1rem;">'
        '<span style="background:#333348; color:#A0A0B8; padding:3px 12px; '
        'border-radius:5px; font-size:0.78rem;">DEMO MODE &mdash; simulated data</span>'
        '</div>',
    )

# ---------------------------------------------------------------------------
# Collect current metrics
# ---------------------------------------------------------------------------
latency = collector.collect_latency()
throughput = collector.collect_throughput()
alerts = collector.collect_alerts()

# Latency history (only DemoMetricsCollector keeps rolling history)
if hasattr(collector, "get_latency_history"):
    vast_lat_hist, kafka_lat_hist = collector.get_latency_history()
else:
    vast_lat_hist = [latency["vast"]]
    kafka_lat_hist = [latency["kafka"]]

# ---------------------------------------------------------------------------
# KPI summary strip
# ---------------------------------------------------------------------------
kpi1, kpi2, kpi3, kpi4 = st.columns(4)

with kpi1:
    st.metric(
        label="VAST p50 Latency",
        value=f"{latency['vast']['p50']:.2f} ms",
        delta=f"-{latency['kafka']['p50'] - latency['vast']['p50']:.1f} ms vs Kafka",
        delta_color="inverse",
    )
with kpi2:
    st.metric(
        label="VAST Throughput",
        value=f"{throughput['vast']:,.0f} msgs/s",
        delta=f"+{throughput['vast'] - throughput['kafka']:,.0f} vs Kafka",
    )
with kpi3:
    fraud_count = len([a for a in alerts if a.get("risk_score", 0) > 0.9])
    st.metric(label="High-Risk Alerts", value=str(fraud_count))
with kpi4:
    vast_first = len([a for a in alerts if a.get("detected_by") == "VAST"])
    total = max(len(alerts), 1)
    st.metric(label="VAST Detected First", value=f"{vast_first / total:.0%}")

st.markdown("---")

# ===========================================================================
# Row 1: Latency + Throughput
# ===========================================================================
row1_left, row1_right = st.columns(2)

with row1_left:
    render_latency_chart(vast_lat_hist, kafka_lat_hist)

with row1_right:
    render_throughput_chart(throughput["vast"], throughput["kafka"])

# ===========================================================================
# Row 2: Detection Feed + Architecture
# ===========================================================================
row2_left, row2_right = st.columns(2)

with row2_left:
    render_detection_feed(alerts)

with row2_right:
    render_architecture_comparison()

# ===========================================================================
# Row 3: Query Demo + AI Investigation
# ===========================================================================
st.markdown("---")
row3_left, row3_right = st.columns(2)

with row3_left:
    query_name = st.selectbox(
        "Select Query",
        list(NAMED_QUERIES.keys()),
        key="query_selector",
    )
    query_result = collector.collect_query_results(query_name)
    render_query_demo(query_result, query_result)

with row3_right:
    styled_html('<p class="section-title">AI-Powered Investigation (VAST Only)</p>')

    styled_html(
        """
        <div class="metric-card vast">
            <div style="font-weight:700; color:#00B4D8; margin-bottom:8px;">
                AgentEngine &mdash; Fraud Investigation Report
            </div>
            <div style="color:#888; font-size:0.82rem; margin-bottom:10px;">
                VAST AgentEngine uses LLM-powered agents that query event topics
                directly. No ETL, no data movement, no additional infrastructure.
            </div>
        """,
    )

    # Simulated agent report
    if alerts:
        latest = alerts[0]
        card = latest.get("card_id", "****-0000")
        fraud_type = latest.get("fraud_type", "Unknown")
        amount = latest.get("amount", 0)
        risk = latest.get("risk_score", 0)

        styled_html(
            f"""
            <div class="mono-block" style="font-size:0.8rem; line-height:1.6;">
<b style="color:#00B4D8;">Agent Report #{latest.get('id', 0)}</b>
<b>Subject:</b> {fraud_type} detected on {card}
<b>Amount:</b> ${amount:,.2f} &nbsp;|&nbsp; <b>Risk:</b> {risk:.0%}

<b style="color:#00B4D8;">Investigation Steps:</b>
1. Queried 6-month transaction history &mdash; <b style="color:#2ECC71;">0.4ms</b>
2. Ran geo-velocity analysis &mdash; <b style="color:#2ECC71;">0.2ms</b>
3. Cross-referenced merchant fraud DB &mdash; <b style="color:#2ECC71;">0.3ms</b>
4. Computed behavioral embeddings &mdash; <b style="color:#2ECC71;">1.1ms</b>

<b style="color:#00B4D8;">Verdict:</b> <span style="color:#E74C3C;">BLOCK TRANSACTION</span>
Confidence: {risk:.0%} | Total investigation time: <b style="color:#2ECC71;">2.0ms</b>

<b style="color:#888;">Kafka equivalent:</b> <span style="color:#FF6B35;">Not available</span>
<span style="color:#888;">Requires separate Flink + ML pipeline + ClickHouse queries.
Estimated pipeline latency: 8-45 seconds.</span>
            </div>
            """,
        )
    else:
        styled_html(
            '<div style="color:#888; text-align:center; padding:2rem 0;">'
            "Waiting for alerts to investigate...</div>",
        )

    styled_html("</div>")

    # Kafka side note
    styled_html(
        """
        <div class="metric-card kafka" style="opacity:0.65;">
            <div style="font-weight:700; color:#FF6B35; margin-bottom:6px;">
                Kafka Stack &mdash; No Agent Capability
            </div>
            <div style="color:#888; font-size:0.82rem;">
                Traditional Kafka deployments require assembling separate
                ML inference services, feature stores, and orchestration
                layers to approximate automated investigation. Typical
                integration effort: <b>3-6 months</b>.
            </div>
        </div>
        """,
    )

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.markdown("---")
styled_html(
    '<div style="text-align:center; color:#555; font-size:0.75rem; padding:0.5rem 0;">'
    "VAST Data Solutions Engineering &mdash; Fraud Detection Demo Dashboard"
    "</div>",
)
