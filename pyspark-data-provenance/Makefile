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

check-version-format-branch:
   $(eval VERSION=$(shell cat VERSION))
   $(eval BRANCH=$(shell git rev-parse --abbrev-ref HEAD))
   @VERSION_REGEX="^[0-9]+\.[0-9]+\.[0-9]+(\.dev[0-9]+)?(\+[a-zA-Z0-9\.]+)?$$"; \
   NORMALIZED_BRANCH=$$(echo $(BRANCH) | sed 's/[^a-zA-Z0-9]/./g' | tr '[:upper:]' '[:lower:]'); \
   echo "Checking version format on branch: $(BRANCH), version: $(VERSION)"; \
   if [ "$(BRANCH)" != "main" ]; then \
      if ! echo "$(VERSION)" | grep -Eq "$$VERSION_REGEX"; then \
         echo "WARNING: Version does not match the required format (X.Y.Z or X.Y.Z.devN or X.Y.Z(.devN)+branch.name)"; \
         echo "See: https://peps.python.org/pep-0440/ for more information."; \
      fi; \
      if ! echo "$(VERSION)" | grep -Eq "\+$$NORMALIZED_BRANCH$$"; then \
         echo "WARNING: Version does not end with the normalized branch name ($$NORMALIZED_BRANCH)"; \
         echo "Recommendation: run 'make add-branch-name-to-version' to correct the version."; \
      else \
         echo "Version format is correct for branch: $(BRANCH)."; \
      fi; \
   else \
      echo "Skipping branch name check on main branch."; \
      if ! echo "$(VERSION)" | grep -Eq "$$VERSION_REGEX"; then \
         echo "WARNING: Version does not match the required format (X.Y.Z or X.Y.Z.devN)"; \
         echo "See: https://peps.python.org/pep-0440/ for more information."; \
      else \
         echo "Version format is correct for main branch."; \
      fi; \
   fi

add-branch-name-to-version:
	$(eval VERSION=$(shell cat VERSION))
	@echo # Get the current git branch name
	@echo # In GHA git rev-parse --abbrev-ref HEAD send back HEAD
	@echo # $$GITHUB_HEAD_REF is checked to determine if it's set.
	@echo # If set, it's echoed; otherwise, $${GITHUB_REF#refs/heads/} is echoed.
	@echo # sed 's|refs/pull/.*||' removes refs/pull/... patterns.
	@echo # Finally, tr '[:upper:]' '[:lower:]' converts the branch name to lowercase.
	$(eval BRANCH_NAME=$(shell [ -n "$$GITHUB_HEAD_REF" ] && echo "$$GITHUB_HEAD_REF" || echo "$${GITHUB_REF#refs/heads/}" | sed 's|refs/pull/.*||' | tr '[:upper:]' '[:lower:]'))
	@echo # Make the name PEP compliant by replacing special characters (/, _, etc.) with dot (.)
	$(eval CLEAN_BRANCH_NAME=$(shell echo "$(BRANCH_NAME)" | sed 's/[^a-zA-Z0-9]/./g' | tr '[:upper:]' '[:lower:]'))
	$(eval NEW_VERSION=$(shell echo "$(VERSION)" | sed 's/\+.*//'))
	$(eval NEW_VERSION=$(NEW_VERSION)+$(CLEAN_BRANCH_NAME))
	@echo "Updating version from: $(VERSION) to: $(NEW_VERSION)"
	@echo "$(NEW_VERSION)" > VERSION
	echo # Bumping version across files
	$(MAKE)	bump-package

increment-dev-version:
	$(eval VERSION=$(shell cat VERSION))
	@VERSION_REGEX="^[0-9]+\.[0-9]+\.[0-9]+$$"; \
	DEV_VERSION_REGEX="^[0-9]+\.[0-9]+\.[0-9]+\.dev[0-9]+$$"; \
	echo "Current version: $(VERSION)"; \
	if echo "$(VERSION)" | grep -Eq "$$VERSION_REGEX"; then \
		NEW_VERSION=$$(echo "$(VERSION)" | awk -F. '{print $$1"."$$2"."$$3+1".dev0"}'); \
		echo "Updating release version to the next minor dev version: $$NEW_VERSION"; \
	elif echo "$(VERSION)" | grep -Eq "$$DEV_VERSION_REGEX"; then \
		NEW_VERSION=$$(echo "$(VERSION)" | awk -F'.dev' '{print $$1".dev"($$2+1)}'); \
		echo "Updating dev version to: $$NEW_VERSION"; \
	else \
		echo "ERROR: Version format is invalid. Should be X.Y.Z or X.Y.Z.devN"; \
		exit 1; \
	fi; \
	echo "$$NEW_VERSION" > VERSION; \
	git add VERSION

bump-to-minor-version:
	$(eval VERSION=$(shell cat VERSION))
	@VERSION_REGEX="^[0-9]+\.[0-9]+\.[0-9]+\.dev[0-9]+$$"; \
	echo "Current version: $(VERSION)"; \
	if echo "$(VERSION)" | grep -Eq "$$VERSION_REGEX"; then \
		NEW_VERSION=$$(echo "$(VERSION)" | sed 's/\.dev[0-9]*//'); \
		echo "Bumping to next minor version: $$NEW_VERSION"; \
	else \
		echo "ERROR: Version format is invalid. Should be X.Y.Z.devN"; \
		exit 1; \
	fi; \
	echo "$$NEW_VERSION" > VERSION; \
	git add VERSION


bump-to-major-version:
	$(eval VERSION=$(shell cat VERSION))
	@VERSION_REGEX="^[0-9]+\.[0-9]+\.[0-9]+(\.dev[0-9]+)?(\+[a-zA-Z0-9._-]+)?$$"; \
	echo "Current version: $(VERSION)"; \
	if echo "$(VERSION)" | grep -Eq "$$VERSION_REGEX"; then \
		NEW_VERSION=$$(echo "$(VERSION)" | sed 's/\(\.dev[0-9]*\)\?(\+[a-zA-Z0-9._-]*)\?//g'); \
		NEW_VERSION=$$(echo "$$NEW_VERSION" | awk -F'[.]' '{print $$1+1".0.0"}'); \
		echo "Bumping version to next major: $$NEW_VERSION"; \
	else \
		echo "ERROR: Version format is invalid. Should be X.Y.Z, X.Y.Z.devN, X.Y.Z+branch.name, or X.Y.Z.devN+branch.name"; \
		exit 1; \
	fi; \
	echo "$$NEW_VERSION" > VERSION; \
	git add VERSION

bump-to-patch-version:
	$(eval VERSION=$(shell cat VERSION))
	@VERSION_REGEX="^[0-9]+\.[0-9]+\.[0-9]+(\.dev[0-9]+)?(\+[a-zA-Z0-9._-]+)?$$"; \
	echo "Current version: $(VERSION)"; \
	if echo "$(VERSION)" | grep -Eq "$$VERSION_REGEX"; then \
		NEW_VERSION=$$(echo "$(VERSION)" | sed 's/\(\.dev[0-9]*\)\?(\+[a-zA-Z0-9._-]*)\?//g'); \
		NEW_VERSION=$$(echo "$$NEW_VERSION" | awk -F'[.]' '{print $$1"."$$2"."$$3+1}'); \
		echo "Bumping version to next patch: $$NEW_VERSION"; \
	else \
		echo "ERROR: Version format is invalid. Should be X.Y.Z, X.Y.Z.devN, X.Y.Z+branch.name, or X.Y.Z.devN+branch.name"; \
		exit 1; \
	fi; \
	echo "$$NEW_VERSION" > VERSION; \
	git add VERSION

bump-package: ## Bump the package version of the project.
	@sed -i.bak 's/^version = .*/version = "$(PYTHON_PACKAGE_VERSION)"/' pyproject.toml && rm pyproject.toml.bak
	@git add pyproject.toml
	@$(UV) lock
	@$(UV) lock --upgrade
	@$(UV) sync --all-groups
	@git add uv.lock

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
	@$(UV) run --group dev mypy src

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

publish-from-ci: ## Publish built artifacts to PyPI from GitHub Actions
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

