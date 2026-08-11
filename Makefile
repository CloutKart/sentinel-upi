SHELL := /bin/bash
.DEFAULT_GOAL := help

# Spark 3.5 supports JDK 8/11/17 only, and Fedora ships 25/26 as system Java. Pin a
# project-local Temurin 17; `make jdk` installs it if it is not already there.
JDK_HOME ?= $(HOME)/.local/jdks/jdk-17
export JAVA_HOME := $(JDK_HOME)

ENV    ?= local
SCALE  ?= 1.0
SEED   ?= 42
UV     := uv

export SENTINEL_ENV := $(ENV)

.PHONY: help
help:  ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------- environment

.PHONY: jdk
jdk:  ## Install a project-local Temurin JDK 17 (no sudo, system JDK untouched)
	@if [ -x "$(JDK_HOME)/bin/java" ]; then \
		echo "JDK 17 present: $$($(JDK_HOME)/bin/java -version 2>&1 | head -1)"; \
	else \
		echo "Installing Temurin JDK 17 into $(JDK_HOME)..."; \
		mkdir -p $(JDK_HOME); \
		curl -sSL "https://api.adoptium.net/v3/binary/latest/17/ga/linux/x64/jdk/hotspot/normal/eclipse?project=jdk" \
			| tar xz -C $(JDK_HOME) --strip-components=1; \
		$(JDK_HOME)/bin/java -version; \
	fi

.PHONY: setup
setup: jdk  ## Create the venv and install every dependency group
	$(UV) venv --python 3.11
	$(UV) pip install -e '.[local,generate,dev]'
	@echo "Setup complete. JAVA_HOME=$(JAVA_HOME)"

.PHONY: doctor
doctor:  ## Verify the toolchain can actually start Spark and write Delta
	@$(UV) run python -c "import sys; print('python', sys.version.split()[0])"
	@$(JDK_HOME)/bin/java -version 2>&1 | head -1
	@$(UV) run python -m sentinel.doctor

# ---------------------------------------------------------------- data + runs

.PHONY: gen
gen:  ## Generate raw UPI telemetry into the drop zone (SCALE=1.0 full)
	$(UV) run sentinel-gen --scale $(SCALE) --seed $(SEED)

.PHONY: run-landing run-bronze run-silver run-gold run-local
run-landing:  ## Step 1: raw drop zone -> Landing
	$(UV) run sentinel-run landing

run-bronze:  ## Step 2: Landing -> Bronze
	$(UV) run sentinel-run bronze

run-silver:  ## Step 3: Bronze -> Silver
	$(UV) run sentinel-run silver

run-gold:  ## Step 4: Silver -> Gold
	$(UV) run sentinel-run gold

run-local:  ## Full pipeline: landing -> bronze -> silver -> gold
	$(UV) run sentinel-run all

.PHONY: report
report:  ## Print KPIs, top fraud alerts, quarantine breakdown and detection recall
	$(UV) run sentinel-run report

.PHONY: run
run:  ## Everything: pipeline (if needed), export, and the dashboard on :5173
	./run.sh

.PHONY: demo
demo:  ## Clean slate: generate, run every layer, then report
	@# clean-all, not clean: `clean` deliberately keeps data/raw, so a demo built on it
	@# would generate a *second* batch alongside the first. With a fixed seed both
	@# batches carry the same transaction ids at slightly different times, the dedupe
	@# keeps an arbitrary one of each, and the bursts the fraud rules look for end up
	@# smeared across two time origins. The pipeline is fine; the demo is a lie.
	@$(MAKE) clean-all
	@$(MAKE) gen
	@$(MAKE) run-local
	@$(MAKE) report

# ---------------------------------------------------------------- dashboard
WEB := dashboards/web

# web-build deliberately does NOT depend on web-data. Making it a prerequisite means
# `make web-build` silently re-exports over whatever data you had deliberately put in
# place. Choosing the data source is the point of these targets, so it is never
# implied — you ask for the export, or you don't.
.PHONY: web-data web-install web-dev web-build web-preview web
web-data:  ## Export the Gold layer to JSON for the dashboard (overwrites public/data)
	$(UV) run sentinel-web-export

web-install:  ## Install frontend dependencies
	cd $(WEB) && npm install

web-dev:  ## Dev server with hot reload, on whatever data is present (localhost:5173)
	cd $(WEB) && npm run dev

web-build:  ## Production build into dist, from whatever data is present
	cd $(WEB) && npm run build

web-preview:  ## Serve the production build (localhost:4173)
	cd $(WEB) && npm run preview

# Sub-makes rather than prerequisites: prerequisite order is not guaranteed under
# `make -j`, and building before the data lands is exactly the bug being avoided.
web:  ## Export Gold -> build -> serve
	@$(MAKE) web-data
	@$(MAKE) web-build
	@$(MAKE) web-preview

# ---------------------------------------------------------------- quality

.PHONY: test test-unit test-spark lint fmt typecheck check
test-unit:  ## Fast pure-Python tests (no Spark)
	$(UV) run pytest tests/unit -q

test-spark:  ## End-to-end tests against a local SparkSession
	$(UV) run pytest tests/spark -q

test:  ## Unit + Spark tests
	$(UV) run pytest -q

lint:  ## Ruff lint
	$(UV) run ruff check src tests

fmt:  ## Ruff format + autofix
	$(UV) run ruff format src tests
	$(UV) run ruff check --fix src tests

typecheck:  ## mypy, and the dashboard's TypeScript if it is installed
	$(UV) run mypy src
	@if [ -d "$(WEB)/node_modules" ]; then cd $(WEB) && npm run typecheck --silent; \
	else echo "skipping web typecheck (run \`make web-install\` first)"; fi

check: lint typecheck test  ## Everything CI runs

# ---------------------------------------------------------------- databricks

.PHONY: deploy deploy-dry deploy-run
deploy-dry:  ## Render and validate the deployment without touching a workspace
	./databricks/deploy.sh --dry-run

deploy:  ## Deploy wheel + notebooks + job to Databricks (needs `databricks auth login`)
	./databricks/deploy.sh

deploy-run:  ## Deploy, then trigger the job and wait for it
	./databricks/deploy.sh --run

# ---------------------------------------------------------------- misc

.PHONY: clean clean-all
clean:  ## Remove pipeline output, keeping generated raw data
	rm -rf data/landing data/bronze data/silver data/gold data/quarantine \
	       data/checkpoints data/_warehouse
	find . -name __pycache__ -type d -prune -exec rm -rf {} + 2>/dev/null || true

clean-all: clean  ## Also remove generated raw telemetry and truth labels
	rm -rf data build dist *.egg-info .pytest_cache .ruff_cache .mypy_cache
