.PHONY: setup-vast setup-blob-expansion setup-kafka load-history demo demo-mode clean help

VAST_ENDPOINT ?=
VAST_ACCESS_KEY ?=
VAST_SECRET_KEY ?=
DEMO_TPS ?= 1000
DEMO_DURATION ?= 300

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup-vast: ## Setup VAST Event Broker, DataEngine, and load data
	@bash scripts/setup_vast.sh

setup-kafka: ## Start Kafka Docker stack and create topics
	@bash scripts/setup_kafka.sh

setup-blob-expansion: ## Configure Blob Expansion (topics-as-tables) on VAST
	@PYTHONPATH=. python vast/blob_expansion_setup.py \
		--endpoint $${VAST_ENDPOINT} \
		--access-key $${VAST_ACCESS_KEY} \
		--secret-key $${VAST_SECRET_KEY}

setup-all: setup-vast setup-blob-expansion setup-kafka ## Setup both VAST and Kafka

load-history: ## Load 6 months of synthetic transaction history
	@PYTHONPATH=. python -c "from vast.setup import load_historical_data; load_historical_data()"

demo: ## Run the full demo (both generators + dashboard)
	@PYTHONPATH=. bash scripts/run_demo.sh

demo-mode: ## Run dashboard in demo mode (simulated data, no backends needed)
	@DEMO_MODE=true PYTHONPATH=. streamlit run dashboard/app.py --server.port 8501

demo-vast-only: ## Run generator against VAST Event Broker
	@PYTHONPATH=. python -m generator.transaction_generator --target vast --tps $(DEMO_TPS) --duration $(DEMO_DURATION)

demo-kafka-only: ## Run generator against local Kafka Docker
	@PYTHONPATH=. python -m generator.transaction_generator --target kafka --tps $(DEMO_TPS) --duration $(DEMO_DURATION)

clean: ## Tear down everything
	@bash scripts/teardown.sh

status: ## Show status of all services
	@echo "=== Kafka Stack ==="
	@cd kafka && docker compose ps 2>/dev/null || echo "Not running"
	@echo ""
	@echo "=== VAST ==="
	@if [ -n "$${VAST_ENDPOINT:-}" ]; then vastde topics list 2>/dev/null || echo "Cannot reach VAST"; else echo "VAST_ENDPOINT not set"; fi
