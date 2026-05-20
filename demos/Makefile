.PHONY: setup demo demo-vast demo-kafka demo-mode generate generate-vast generate-kafka score setup-kafka status clean help

DEMO_TPS ?= 1000
DEMO_DURATION ?= 300

# Source .env if it exists
ifneq (,$(wildcard .env))
    include .env
    export
endif

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ── Setup ────────────────────────────────────────────

setup: ## First-time setup wizard (creates .env, validates cluster, configures blob expansion)
	@bash setup.sh

setup-kafka: ## Start Kafka Docker comparison stack
	@bash scripts/setup_kafka.sh

# ── Demo ─────────────────────────────────────────────

demo: ## Full side-by-side demo (VAST + Kafka + scorer + dashboard)
	@bash scripts/preflight.sh
	@bash scripts/run_demo.sh

demo-vast: ## VAST pipeline only (generator + scorer + dashboard)
	@bash scripts/preflight.sh
	@DEMO_KAFKA=false bash scripts/run_demo.sh

demo-kafka: ## Kafka pipeline only (Docker + generator + dashboard)
	@bash scripts/setup_kafka.sh
	@DEMO_VAST=false bash scripts/run_demo.sh

demo-mode: ## Dashboard with simulated data (no backends needed)
	@DEMO_MODE=true PYTHONPATH=. streamlit run dashboard/app.py --server.port 8501

# ── Individual Components ────────────────────────────

generate: ## Run transaction generator (TPS=5000 DURATION=60)
	@PYTHONPATH=. python3 -m generator.transaction_generator --target vast --tps $(DEMO_TPS) --duration $(DEMO_DURATION)

generate-vast: ## Generator against VAST Event Broker only
	@PYTHONPATH=. python3 -m generator.transaction_generator --target vast --tps $(DEMO_TPS) --duration $(DEMO_DURATION)

generate-kafka: ## Generator against local Kafka Docker only
	@PYTHONPATH=. python3 -m generator.transaction_generator --target kafka --tps $(DEMO_TPS) --duration $(DEMO_DURATION)

score: ## Start the standalone fraud scorer
	@PYTHONPATH=. python3 -u vast/fraud_scorer_pipeline/run_standalone.py

status: ## Show health of all demo components
	@bash scripts/status.sh

clean: ## Tear down Docker stack and stop all processes
	@bash scripts/teardown.sh

deploy-dataengine: ## Optional: deploy fraud scorer via VAST DataEngine CLI
	@echo "See demos/vast/fraud_scorer_pipeline/DEPLOYMENT.md for step-by-step guide"
