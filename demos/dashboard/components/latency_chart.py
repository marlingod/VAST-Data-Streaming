"""Plotly latency comparison chart for VAST vs Kafka."""

from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st

VAST_COLORS = {"p50": "#00B4D8", "p95": "#0096B7", "p99": "#007A94"}
KAFKA_COLORS = {"p50": "#FF6B35", "p95": "#D45A2B", "p99": "#B04A22"}


def render_latency_chart(
    vast_latencies: list[dict[str, float]],
    kafka_latencies: list[dict[str, float]],
) -> None:
    """Render a rolling latency line chart.

    Each list entry is a dict with keys p50, p95, p99 (values in ms).
    """
    if not vast_latencies or not kafka_latencies:
        st.info("Collecting latency data...")
        return

    x = list(range(len(vast_latencies)))

    fig = go.Figure()

    # -- VAST lines --
    for pct, color in VAST_COLORS.items():
        fig.add_trace(go.Scatter(
            x=x,
            y=[d[pct] for d in vast_latencies],
            mode="lines",
            name=f"VAST {pct}",
            line=dict(color=color, width=2.5 if pct == "p50" else 1.5,
                      dash="solid" if pct == "p50" else "dot"),
            legendgroup="vast",
        ))

    # -- Kafka lines --
    for pct, color in KAFKA_COLORS.items():
        fig.add_trace(go.Scatter(
            x=x,
            y=[d[pct] for d in kafka_latencies],
            mode="lines",
            name=f"Kafka {pct}",
            line=dict(color=color, width=2.5 if pct == "p50" else 1.5,
                      dash="solid" if pct == "p50" else "dot"),
            legendgroup="kafka",
        ))

    # -- Annotations --
    fig.add_annotation(
        x=0.02, y=0.95, xref="paper", yref="paper",
        text="<b>VAST: sub-ms</b>",
        showarrow=False,
        font=dict(color="#00B4D8", size=13),
        bgcolor="rgba(0,180,216,0.12)",
        bordercolor="#00B4D8",
        borderwidth=1,
        borderpad=6,
    )
    fig.add_annotation(
        x=0.02, y=0.78, xref="paper", yref="paper",
        text="<b>Kafka: 5-15 ms</b>",
        showarrow=False,
        font=dict(color="#FF6B35", size=13),
        bgcolor="rgba(255,107,53,0.12)",
        bordercolor="#FF6B35",
        borderwidth=1,
        borderpad=6,
    )

    fig.update_layout(
        title=dict(text="End-to-End Detection Latency", font=dict(size=16)),
        xaxis=dict(title="Sample", showgrid=False, color="#888"),
        yaxis=dict(title="Latency (ms)", showgrid=True,
                   gridcolor="rgba(255,255,255,0.06)", color="#888"),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#ccc"),
        legend=dict(orientation="h", y=-0.18, x=0.5, xanchor="center",
                    font=dict(size=11)),
        margin=dict(l=50, r=20, t=45, b=60),
        height=370,
    )

    st.plotly_chart(fig, use_container_width=True)
