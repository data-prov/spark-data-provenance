import os
from contextlib import contextmanager
from typing import Iterator

from pyspark.sql import DataFrame, SparkSession


def provenance_column_name(spark: SparkSession) -> str:
    """
    Helper function to get the name of the provenance column from the Spark configuration.
    This is useful to avoid hardcoding the column name in the code and to allow users to customize it.
    """
    return spark._jvm.org.dataprov.dp.sparkdataprovenance.DataFrameProvenanceTransformations.provenanceColumnName(
        spark._jsparkSession
    )


def add_provenance_column(df_or_name: str | DataFrame, spark: SparkSession):
    """
    Adds a provenance column to the given DataFrame.
    This is a helper function that can be used in tests to verify that the plugin is working.
    """
    jfunction = spark._jvm.org.dataprov.dp.sparkdataprovenance.DataFrameProvenanceTransformations.addProvenanceColumn
    match df_or_name:
        case str():
            jfunction(spark._jsparkSession, df_or_name)
            return df_or_name
        case DataFrame():
            return DataFrame(jfunction(df_or_name._jdf), spark)


def remove_provenance_column(df_or_name: str | DataFrame, spark: SparkSession):
    """
    Removes the provenance column from the given DataFrame.
    This is a helper function that can be used in tests to verify that the plugin is working.
    """
    jfunction = spark._jvm.org.dataprov.dp.sparkdataprovenance.DataFrameProvenanceTransformations.removeProvenanceColumn
    match df_or_name:
        case str():
            jfunction(spark._jsparkSession, df_or_name)
            return df_or_name
        case DataFrame():
            return DataFrame(jfunction(df_or_name._jdf), spark)


@contextmanager
def data_provenance_enabled(spark: SparkSession, *args: str | DataFrame) -> Iterator[tuple[DataFrame | str]]:
    """
    Context manager to enable data provenance for the duration of a block of code.
    It also adds a provenance column to the given DataFrames or temporary view names
    and removes it after the block is executed.
    """
    # Remember the previous state in case these are nested
    is_data_provenance_enabled = str(spark.conf.get("spark.provenance.enabled", "false"))
    try:
        # Turn data provenance on for this block
        spark.conf.set("spark.provenance.enabled", "true")
        yield tuple(add_provenance_column(df, spark) for df in args)
    finally:
        # Revert to whatever it was before
        spark.conf.set("spark.provenance.enabled", is_data_provenance_enabled)
        # Remove the provenance column from the DataFrames
        for df in args:
            remove_provenance_column(df, spark)


def build_data_provenance_session() -> SparkSession.Builder:
    """
    Helper function to automatically find the bundled JAR
    and initialize a SparkSession with the plugin enabled.
    """
    # 1. Find the path to the 'jars' folder dynamically
    current_dir = os.path.dirname(os.path.abspath(__file__))
    jar_path = os.path.join(current_dir, "jars", "dp-spark_2.13-0.0.1.jar")

    # 2. Build and return the SparkSession
    return SparkSession.builder.config("spark.jars", jar_path).config(
        "spark.sql.extensions", "org.dataprov.dp.ProvenanceExtension"
    )
