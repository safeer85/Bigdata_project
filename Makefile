# =============================================================================
# One command to run everything: `make up`.
#
# Windows note: GNU make is not installed by default. `make.ps1` in this directory
# mirrors every target below (`.\make.ps1 up`), so nobody has to install make to
# mark the project. Keep the two in sync.
# =============================================================================
SHELL := /bin/bash
COMPOSE := docker compose
.DEFAULT_GOAL := help

# Services that must be healthy before the stack counts as "up".
CORE := postgres kafka spark-master spark-worker api prometheus grafana

.PHONY: help up up-tools down reset logs ps test smoke demo-idle demo-outage \
        demo-resubmit demo-late-file build wait clean-pyc lint urls

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

.env: .env.example
	@echo "creating .env from .env.example"
	@cp .env.example .env

build: .env ## Build the three custom images (app, spark, airflow)
	$(COMPOSE) build

up: .env ## Build and start the whole stack, then wait for it to be healthy
	$(COMPOSE) up -d --build
	@$(MAKE) --no-print-directory wait
	@$(MAKE) --no-print-directory urls

up-tools: .env ## Same as `up`, plus the optional Kafka UI
	$(COMPOSE) --profile tools up -d --build
	@$(MAKE) --no-print-directory wait

wait: ## Block until the core services report healthy
	@python scripts/wait_for_stack.py

urls: ## Print every UI the demo uses
	@echo ""
	@echo "  API docs        http://localhost:8000/docs"
	@echo "  Grafana         http://localhost:3000       (admin/admin)"
	@echo "  Airflow         http://localhost:8088       (admin/admin)"
	@echo "  Prometheus      http://localhost:9090"
	@echo "  Alertmanager    http://localhost:9093"
	@echo "  Spark master    http://localhost:8080"
	@echo "  Kafka UI        http://localhost:8090       (make up-tools)"
	@echo ""

down: ## Stop everything, keep the data
	$(COMPOSE) --profile tools down

reset: ## Stop and wipe volumes, lake, checkpoints and the simulated clock
	$(COMPOSE) --profile tools down -v --remove-orphans
	@echo "volumes removed: pgdata, lake, landing, shared (simclock.json), reports, kafkadata"
	@rm -rf reports/*.html reports/*.csv 2>/dev/null || true
	@echo "reset complete - the next `make up` starts simulated day 1 again"

logs: ## Tail one service: make logs s=speed
	$(COMPOSE) logs -f --tail=200 $(s)

ps: ## Show container status and health
	$(COMPOSE) ps

test: ## Unit + PySpark + API tests (runs inside the airflow image, which has pyspark)
	$(COMPOSE) run --rm --no-deps \
	  -v "$(CURDIR)/tests:/opt/fleet/tests:ro" \
	  -v "$(CURDIR)/api:/opt/fleet/api:ro" \
	  -v "$(CURDIR)/simulators:/opt/fleet/simulators:ro" \
	  -e PYTHONPATH=/opt/fleet \
	  --entrypoint bash airflow -lc \
	  "pip install --quiet pytest==8.3.3 httpx==0.27.2 fastapi==0.115.4 confluent-kafka==2.6.0 jsonschema==4.23.0 && cd /opt/fleet && python -m pytest tests -q"

smoke: ## End-to-end smoke test against the running stack
	python scripts/smoke_test.py

demo-idle: ## Force one vehicle idle -> idle alert
	python scripts/demo.py idle

demo-outage: ## Pause the telemetry producer -> TelemetryNotProduced alert
	python scripts/demo.py outage

demo-resubmit: ## Drop a corrected v2 expense file for a past day -> batch recompute
	python scripts/demo.py resubmit

demo-late-file: ## Delay the next expense file past its SLA -> ExpenseFileLate alert
	python scripts/demo.py late-file

clean-pyc:
	@find . -name '__pycache__' -type d -prune -exec rm -rf {} + 2>/dev/null || true
