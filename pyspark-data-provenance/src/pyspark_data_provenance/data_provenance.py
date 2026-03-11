from pyspark.sql import SparkSession
from contextlib import contextmanager


@contextmanager
def data_provenance_enabled(spark: SparkSession):
    # Remember the previous state in case these are nested
    is_data_provenance_enabled = spark.conf.get("spark.provenance.enabled", "false")
    try:
        # Turn data provenance on for this block
        spark.conf.set("spark.provenance.enabled", "true")
        yield
    finally:
        # Revert to whatever it was before
        spark.conf.set("spark.provenance.enabled", is_data_provenance_enabled)
