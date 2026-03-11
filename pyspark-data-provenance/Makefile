PYTHON_VERSION ?= 3.12
UV ?= uv
PACKAGE_NAME ?= pyspark-data-provenance
PYPI_PROJECT_URL ?= https://pypi.org/project/$(PACKAGE_NAME)/
UV_PUBLISH_DOC_URL ?= https://docs.astral.sh/uv/guides/package/\#publishing-your-package

.PHONY: help init-uv-python init update build run test lint format type-check check clean require-uv-publish-token publish publish-testpypi

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

build: ## Build project wheel
	@$(UV) build --wheel

run: ## Run package demo
	@$(UV) run main.py

test: ## Run tests
	@$(UV) run --group dev pytest

lint: ## Run Ruff linter
	@$(UV) run --group dev ruff check .

format: ## Format with Ruff
	@$(UV) run --group dev ruff format .

type-check: ## Run mypy on source and tests
	@$(UV) run --group dev mypy src tests

check: lint type-check ## Run lint and type checks

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