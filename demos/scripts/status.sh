#!/usr/bin/env bash
# Health check for all demo components.

PASS="\033[32m✓\033[0m"
FAIL="\033[31m✗\033[0m"
WARN="\033[33m!\033[0m"

echo ""
echo "Demo Status"
echo "═══════════"

# Source .env if available
if [[ -f .env ]]; then
    source .env
fi

# ── VAST Event Broker ──
echo ""
echo "VAST Event Broker"
echo "─────────────────"
if [[ -n "${VAST_BOOTSTRAP_SERVERS:-}" ]]; then
    VAST_HOST="${VAST_BOOTSTRAP_SERVERS%%:*}"
    VAST_PORT="${VAST_BOOTSTRAP_SERVERS##*:}"
    if nc -zw3 "$VAST_HOST" "$VAST_PORT" 2>/dev/null; then
        printf "  ${PASS} ${VAST_HOST}:${VAST_PORT}\n"

        # Topic message counts
        python3 -c "
from confluent_kafka import Consumer, TopicPartition
import os
bs = os.environ.get('VAST_BOOTSTRAP_SERVERS', '')
topics = ['fraud.transactions.raw', 'fraud.transactions.scored', 'fraud.alerts', 'fraud.metrics']
for topic in topics:
    try:
        c = Consumer({'bootstrap.servers': bs, 'group.id': 'status-check'})
        md = c.list_topics(topic, timeout=5)
        total = 0
        for pid in md.topics[topic].partitions:
            lo, hi = c.get_watermark_offsets(TopicPartition(topic, pid))
            total += hi
        c.close()
        print(f'  {topic}: {total:,} messages')
    except Exception as e:
        print(f'  {topic}: error ({e})')
" 2>/dev/null || printf "  ${WARN} Could not query topics\n"
    else
        printf "  ${FAIL} Unreachable (${VAST_HOST}:${VAST_PORT})\n"
    fi
else
    printf "  ${WARN} VAST_BOOTSTRAP_SERVERS not set (.env missing?)\n"
fi

# ── Kafka Docker Stack ──
echo ""
echo "Kafka Docker Stack"
echo "──────────────────"
if docker info >/dev/null 2>&1; then
    RUNNING=$(cd kafka && docker compose ps --status running -q 2>/dev/null | wc -l | tr -d ' ')
    if [[ "$RUNNING" -gt 0 ]]; then
        printf "  ${PASS} ${RUNNING} containers running\n"
    else
        printf "  ${WARN} Not running (start with: make setup-kafka)\n"
    fi
else
    printf "  ${WARN} Docker not running\n"
fi

# ── Fraud Scorer ──
echo ""
echo "Fraud Scorer"
echo "────────────"
if pgrep -f "run_standalone.py" >/dev/null 2>&1; then
    PID=$(pgrep -f "run_standalone.py" | head -1)
    printf "  ${PASS} Running (PID ${PID})\n"
else
    printf "  ${WARN} Not running (start with: make score)\n"
fi

# ── Dashboard ──
echo ""
echo "Dashboard"
echo "─────────"
if pgrep -f "streamlit run" >/dev/null 2>&1; then
    printf "  ${PASS} http://localhost:8501\n"
else
    printf "  ${WARN} Not running\n"
fi

echo ""
