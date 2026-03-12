#
# https://github.com/data-prov/spark-data-provenance/blob/main/pyspark-data-provenance/Makefile
#

PYTHON_VERSION ?= 3.12
UV ?= uv
PYTHON_PACKAGE_NAME ?= pyspark-data-provenance
PYTHON_PACKAGE_VERSION ?= $(shell cat VERSION)
SCALA_MINOR_VERSION ?= $(shell cat ../scala-spark-data-provenance/SCALA_MINOR_VERSION)
SCALA_ORG ?= org.dataprov.dp
SCALA_PACKAGE_NAME ?= dp-spark
SCALA_PACKAGE_VERSION ?= $(shell cat ../scala-spark-data-provenance/VERSION)
SCALA_PACKAGE_JAR ?= $(SCALA_ORG):$(SCALA_PACKAGE_NAME)_$(SCALA_MINOR_VERSION):$(SCALA_PACKAGE_VERSION)

PYPI_PROJECT_URL ?= https://pypi.org/project/$(PYTHON_PACKAGE_NAME)/
UV_PUBLISH_DOC_URL ?= https://docs.astral.sh/uv/guides/package/\#publishing-your-package

.PHONY: help init-uv-python init update build install-local run-local test lint fix format type-check check clean require-uv-publish-token publish publish-testpypi

help: ## Show this help message
	@echo "Usage: make [target]"
	@echo ""
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'

init-uv-python: ## Install Python $(PYTHON_VERSION) for uv
	@$(UV) python install $(PYTHON_VERSION)

init: ## Lock and sync dependencies
	@$(UV) lock
	@$(UV) sync --all-groups

update: ## Upgrade and sync dependencies
	@$(UV) lock --upgrade
	@$(UV) sync --all-groups

bump-package: ## Bump the package version of the project.
	@sed -i.bak 's/^version = .*/version = "$(PYTHON_PACKAGE_VERSION)"/' pyproject.toml && rm pyproject.toml.bak

build: ## Build project wheel
	@rm -rf dist
	@$(UV) build --wheel

test: ## Run tests
	@$(UV) run --group dev pytest

lint: ## Run Ruff linter
	@$(UV) run --group dev ruff check .

format: ## Format with Ruff
	@$(UV) run --group dev ruff format .

type-check: ## Run mypy on source and tests
	@$(UV) run --group dev mypy src tests

check: lint type-check ## Run lint and type checks

fix-lint: ## Fix linter issues
	$(UV) run --group dev ruff check --fix .

fix: fix-lint ## Fix linter and format issues

clean: ## Remove caches and build artifacts
	@rm -rf .venv dist build .pytest_cache .mypy_cache .ruff_cache
	@find . -type d -name __pycache__ -exec rm -rf {} +
	@find . -type f -name "*.pyc" -delete

require-uv-publish-token: ## Check UV_PUBLISH_TOKEN for production PyPI publish
	@if [ -n "$(UV_PUBLISH_TOKEN)" ]; then \
		echo "UV_PUBLISH_TOKEN is set."; \
		echo "It is expected to be the token for production PyPI publishing."; \
		echo "See: $(UV_PUBLISH_DOC_URL)"; \
	else \
		echo "UV_PUBLISH_TOKEN is not set."; \
		echo "Set UV_PUBLISH_TOKEN to your production PyPI token before publishing."; \
		echo "See: $(UV_PUBLISH_DOC_URL)"; \
		exit 1; \
	fi

publish: require-uv-publish-token ## Publish built artifacts to PyPI
	@$(UV) publish
	@echo "The Python package has been published on $(PYPI_PROJECT_URL)"

publish-testpypi: ## Publish built artifacts to TestPyPI
	@$(UV) publish --publish-url https://test.pypi.org/legacy/

install-local: ## Install the Python wheel locally
	@python -mpip install -U pyspark
	@python -mpip install --force-reinstall \
		$(PYTHON_PACKAGE_NAME)==$(PYTHON_PACKAGE_VERSION)

run-local: ## Run package demo
	@mkdir -p logs
	@spark-submit --packages $(SCALA_PACKAGE_JAR) \
		src/pyspark_data_provenance/jobs/getting_started_job.py | \
		tee logs/getting_started_job.log

