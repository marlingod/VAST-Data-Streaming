"""Plotly throughput comparison chart for VAST vs Kafka."""

from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st


def render_throughput_chart(vast_tps: float, kafka_tps: float) -> None:
    """Render a bar chart comparing messages/sec throughput."""

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=["VAST Event Broker"],
        y=[vast_tps],
        marker_color="#00B4D8",
        text=[f"{vast_tps:,.0f} msgs/s"],
        textposition="outside",
        textfont=dict(size=14, color="#00B4D8"),
        name="VAST",
        width=0.45,
    ))

    fig.add_trace(go.Bar(
        x=["Apache Kafka"],
        y=[kafka_tps],
        marker_color="#FF6B35",
        text=[f"{kafka_tps:,.0f} msgs/s"],
        textposition="outside",
        textfont=dict(size=14, color="#FF6B35"),
        name="Kafka",
        width=0.45,
    ))

    # Reference line for VAST benchmark
    fig.add_annotation(
        x=0.5, y=1.02, xref="paper", yref="paper",
        text="VAST benchmark: <b>136 M msgs/sec</b>",
        showarrow=False,
        font=dict(color="#00B4D8", size=11),
        bgcolor="rgba(0,180,216,0.10)",
        bordercolor="#00B4D8",
        borderwidth=1,
        borderpad=5,
    )

    y_max = max(vast_tps, kafka_tps) * 1.35

    fig.update_layout(
        title=dict(text="Processing Throughput", font=dict(size=16)),
        yaxis=dict(title="Messages / sec", showgrid=True,
                   gridcolor="rgba(255,255,255,0.06)", color="#888",
                   range=[0, y_max]),
        xaxis=dict(showgrid=False, color="#888"),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#ccc"),
        showlegend=False,
        margin=dict(l=60, r=20, t=55, b=40),
        height=370,
        bargap=0.35,
    )

    st.plotly_chart(fig, use_container_width=True)
