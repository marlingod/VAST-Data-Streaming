#!/usr/bin/env bash
set -euo pipefail

# Pre-flight checks for the fraud detection demo.
# Exit codes: 0=pass, 1=missing .env, 2=VAST unreachable, 3=Docker down, 4=deps missing

PASS="\033[32m✓\033[0m"
FAIL="\033[31m✗\033[0m"
WARN="\033[33m!\033[0m"
errors=0

echo ""
echo "Pre-flight checks"
echo "═════════════════"

# 1. .env exists
if [[ -f .env ]]; then
    source .env
    printf "  ${PASS} .env loaded\n"
else
    printf "  ${FAIL} .env not found — run: ./setup.sh\n"
    exit 1
fi

# 2. Required env vars set
for var in VAST_BOOTSTRAP_SERVERS VAST_ENDPOINT VAST_ACCESS_KEY VAST_SECRET_KEY; do
    val="${!var:-}"
    if [[ -z "$val" || "$val" == *"<"* ]]; then
        printf "  ${FAIL} ${var} not configured in .env\n"
        errors=$((errors + 1))
    fi
done
if [[ $errors -gt 0 ]]; then
    echo "  Fix .env and re-run."
    exit 1
fi
printf "  ${PASS} Environment variables set\n"

# 3. VAST Event Broker reachable
VAST_HOST="${VAST_BOOTSTRAP_SERVERS%%:*}"
VAST_PORT="${VAST_BOOTSTRAP_SERVERS##*:}"
if nc -zw3 "$VAST_HOST" "$VAST_PORT" 2>/dev/null; then
    printf "  ${PASS} VAST Event Broker reachable (${VAST_HOST}:${VAST_PORT})\n"
else
    printf "  ${FAIL} VAST Event Broker unreachable (${VAST_HOST}:${VAST_PORT})\n"
    errors=$((errors + 1))
fi

# 4. Docker running (optional — only needed for Kafka comparison)
if docker info >/dev/null 2>&1; then
    printf "  ${PASS} Docker running\n"
else
    printf "  ${WARN} Docker not running (Kafka comparison won't work)\n"
fi

# 5. Python dependencies
if python3 -c "import confluent_kafka, faker, streamlit, plotly" 2>/dev/null; then
    printf "  ${PASS} Python dependencies installed\n"
else
    printf "  ${FAIL} Missing Python packages — run: pip install -r requirements.txt\n"
    errors=$((errors + 1))
fi

echo ""
if [[ $errors -gt 0 ]]; then
    echo "  ${errors} check(s) failed. Fix the above and re-run."
    exit 2
fi
printf "  \033[32mAll checks passed\033[0m\n"
echo ""
exit 0
