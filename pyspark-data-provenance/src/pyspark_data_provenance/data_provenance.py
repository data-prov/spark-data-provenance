import os
from contextlib import contextmanager
from typing import Iterator

from pyspark.sql import SparkSession


@contextmanager
def data_provenance_enabled(spark: SparkSession) -> Iterator[None]:
    # Remember the previous state in case these are nested
    is_data_provenance_enabled = str(spark.conf.get("spark.provenance.enabled", "false"))
    try:
        # Turn data provenance on for this block
        spark.conf.set("spark.provenance.enabled", "true")
        yield
    finally:
        # Revert to whatever it was before
        spark.conf.set("spark.provenance.enabled", is_data_provenance_enabled)


def build_data_provenance_session() -> SparkSession.Builder:
    """
    Helper function to automatically find the bundled JAR
    and initialize a SparkSession with the plugin enabled.
    """
    # 1. Find the path to the 'jars' folder dynamically
    current_dir = os.path.dirname(os.path.abspath(__file__))
    jar_path = os.path.join(current_dir, "jars", "dp-spark_2.13-0.0.1.jar")

    # 2. Build and return the SparkSession
    return (
        SparkSession
        .builder
        .config("spark.jars", jar_path)
        .config("spark.sql.extensions", "org.dataprov.dp.ProvenanceExtension")
    )
