.PHONY: build python-init python-check python-test python-format python-clean python-publish python-publish-testpypi

# Build project
build:
	@echo "Building Scala project..."
	$(MAKE) -C scala-spark-data-provenance build
	@echo "Scala build complete."
	@echo "Copying JAR to PySpark directory..."
	cp -f scala-spark-data-provenance/target/scala-2.13/scala-spark-data-provenance_2.13-0.0.1.jar pyspark-data-provenance/src/pyspark_data_provenance/jars/
	@echo "JAR copy complete."
	@echo "Building Python project..."
	$(MAKE) -C pyspark-data-provenance build
	@echo "Python build complete."

python-init:
	@echo "Initializing Python project..."
	$(MAKE) -C pyspark-data-provenance init

python-check:
	@echo "Running Python lint and type checks..."
	$(MAKE) -C pyspark-data-provenance check

python-test:
	@echo "Running Python tests..."
	$(MAKE) -C pyspark-data-provenance test

python-format:
	@echo "Formatting Python code..."
	$(MAKE) -C pyspark-data-provenance format

python-clean:
	@echo "Cleaning Python artifacts..."
	$(MAKE) -C pyspark-data-provenance clean

python-publish:
	@echo "Publishing Python package to PyPI..."
	$(MAKE) -C pyspark-data-provenance publish

python-publish-testpypi:
	@echo "Publishing Python package to TestPyPI..."
	$(MAKE) -C pyspark-data-provenance publish-testpypi