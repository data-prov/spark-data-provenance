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