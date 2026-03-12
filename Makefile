#
# https://github.com/data-prov/spark-data-provenance/blob/main/Makefile
#
SC_DIR ?= scala-spark-data-provenance
PY_DIR ?= pyspark-data-provenance
SCALA_PACKAGE_NAME ?= dp-spark
SCALA_PACKAGE_VERSION ?= $(shell cat $(SC_DIR)/VERSION)
SCALA_MINOR_VERSION ?= $(shell cat $(SC_DIR)/SCALA_MINOR_VERSION)
SCALA_PACKAGE_JAR ?= $(SCALA_PACKAGE_NAME)_$(SCALA_MINOR_VERSION)-$(SCALA_PACKAGE_VERSION).jar

.PHONY: build python-init python-check python-test python-format python-clean python-publish python-publish-testpypi

info:
	@echo "Package version: $(SCALA_PACKAGE_VERSION) - Scala version: $(SCALA_MINOR_VERSION)"

# Build project
scala-build:
	@echo "Testing and packaging the Scala project..."
	$(MAKE) -C $(SC_DIR) compile test build publish-local
	@echo "Scala build complete."
	@echo "Copying $(SCALA_PACKAGE_JAR) JAR to PySpark directory..."
	cp -f $(SC_DIR)/target/scala-$(SCALA_MINOR_VERSION)/$(SCALA_PACKAGE_JAR) $(PY_DIR)/src/pyspark_data_provenance/jars/
	@echo "$(SCALA_PACKAGE_JAR) JAR copy complete."

python-clean:
	@echo "Cleaning Python project..."
	$(MAKE) -C $(PY_DIR) clean
	@echo "Python project cleaned."

python-init-uv-python:
	@echo "Initializing Python installation with uv..."
	$(MAKE) -C $(PY_DIR) init-uv-python

python-bump-package:
	@echo "Bumping the Python package version..."
	$(MAKE) -C $(PY_DIR) bump-package

python-bump-to-major-version:
	@echo "[Python] Bumping to the major version and then into related files..."
	$(MAKE) -C $(PY_DIR) bump-to-major-version
	$(MAKE) -C $(PY_DIR) bump-package

python-bump-to-minor-version:
	@echo "[Python] Bumping to the minor version and then into related files..."
	$(MAKE) -C $(PY_DIR) bump-to-minor-version
	$(MAKE) -C $(PY_DIR) bump-package

python-bump-to-patch-version:
	@echo "[Python] Bumping to the patch version and then into related files..."
	$(MAKE) -C $(PY_DIR) bump-to-patch-version
	$(MAKE) -C $(PY_DIR) bump-package

python-increment-dev-version:
	@echo "[Python] Incrementing the package version and bumping it into related files..."
	$(MAKE) -C $(PY_DIR) increment-dev-version
	$(MAKE) -C $(PY_DIR) bump-package

python-init:
	@echo "Initializing Python project..."
	$(MAKE) -C $(PY_DIR) init update

python-build: python-init
	@echo "Building Python project..."
	$(MAKE) -C $(PY_DIR) build
	@echo "Python build complete."

python-check:
	@echo "Running Python lint and type checks..."
	$(MAKE) -C $(PY_DIR) check

python-format:
	@echo "Formatting Python code..."
	$(MAKE) -C $(PY_DIR) format

python-fix:
	@echo "Running Python linter fixes..."
	$(MAKE) -C $(PY_DIR) fix

python-test:
	@echo "Running Python tests..."
	$(MAKE) -C $(PY_DIR) test

python-publish:
	@echo "Publishing Python package to PyPI..."
	$(MAKE) -C $(PY_DIR) publish

python-publish-testpypi:
	@echo "Publishing Python package to TestPyPI..."
	$(MAKE) -C $(PY_DIR) publish-testpypi

python-install-local:
	@echo "Installing Python package locally..."
	$(MAKE) -C $(PY_DIR) install-local

python-run-local:
	@echo "Run Python job locally..."
	$(MAKE) -C $(PY_DIR) run-local
