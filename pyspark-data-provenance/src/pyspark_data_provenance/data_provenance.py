import os
import typing as T
from contextlib import contextmanager

from pyspark.sql import DataFrame, SparkSession

from pyspark_data_provenance.py4j_utils import _get_provenance_jvm_function

DataFrameOrView = str | DataFrame


def provenance_column_name(spark: SparkSession) -> str:
    """
    Returns the name of the provenance column from the Spark configuration.
    """
    return _get_provenance_jvm_function("provenanceColumnName", spark)(spark._jsparkSession)


@T.overload
def add_provenance_column(df_or_view: str, spark: SparkSession) -> str: ...


@T.overload
def add_provenance_column(df_or_view: DataFrame, spark: SparkSession) -> DataFrame: ...


def add_provenance_column(df_or_view: DataFrameOrView, spark: SparkSession) -> DataFrameOrView:
    """
    Adds a provenance column to the given DataFrame.
    """
    jfunction = _get_provenance_jvm_function("addProvenance", spark)
    match df_or_view:
        case str():
            jfunction(spark._jsparkSession, df_or_view)
            return df_or_view
        case DataFrame():
            return DataFrame(jfunction(df_or_view._jdf), spark)
        case _:
            raise TypeError("df_or_view must be a str or a DataFrame")


@T.overload
def remove_provenance_column(df_or_view: str, spark: SparkSession) -> str: ...


@T.overload
def remove_provenance_column(df_or_view: DataFrame, spark: SparkSession) -> DataFrame: ...


def remove_provenance_column(df_or_view: DataFrameOrView, spark: SparkSession) -> DataFrameOrView:
    """
    Removes the provenance column from the given DataFrame.
    """
    jfunction = _get_provenance_jvm_function("removeProvenance", spark)
    match df_or_view:
        case str():
            jfunction(spark._jsparkSession, df_or_view)
            return df_or_view
        case DataFrame():
            return DataFrame(jfunction(df_or_view._jdf), spark)
        case _:
            raise TypeError("df_or_view must be a str or a DataFrame")


@contextmanager
def data_provenance_enabled(
    spark: SparkSession, *args: DataFrameOrView
) -> T.Generator[T.Tuple[DataFrameOrView, ...] | DataFrameOrView | None, None, None]:
    """
    Context manager to enable data provenance for the duration of a block of code.
    When DataFrames or view names are provided, it adds a provenance column to them
    and removes it after the block is executed.
    """
    # Remember the previous state in case these are nested
    is_data_provenance_enabled = str(spark.conf.get("spark.provenance.enabled", "false"))
    try:
        # Turn data provenance on for this block
        spark.conf.set("spark.provenance.enabled", "true")
        dataframe_or_views_with_provenance = tuple(add_provenance_column(df, spark) for df in args)
        if not args:
            yield None
        else:
            yield dataframe_or_views_with_provenance if len(args) > 1 else dataframe_or_views_with_provenance[0]
    finally:
        # Revert to whatever it was before
        spark.conf.set("spark.provenance.enabled", is_data_provenance_enabled)
        # Remove the provenance column from the DataFrames/views
        for df in args:
            remove_provenance_column(df, spark)


def data_provenance_session_builder(
    provenance_builder: str = "display",
) -> SparkSession.Builder:
    """
    Helper function to automatically find the bundled JAR
    and initialize a SparkSession Builder with the plugin enabled.

    Args:
        provenance_builder: Builder strategy used by the Scala extension.
            Supported values are: display, boolean, semi-why, full-why.
    """
    # 1. Find the path to the 'jars' folder dynamically
    current_dir = os.path.dirname(os.path.abspath(__file__))
    jar_path = os.path.join(current_dir, "jars", "dp-spark_2.13-0.0.1.jar")

    # 2. Build and return the SparkSession
    return (
        SparkSession.builder.config("spark.jars", jar_path)
        .config(
            "spark.sql.extensions",
            "org.dataprov.dp.sparkdataprovenance.SparkProvenanceExtension",
        )
        .config("spark.provenance.builder", provenance_builder)
    )

def get_minimal_sources(
    df: DataFrame,
    source_dfs: T.Sequence[DataFrame],
    spark: SparkSession,
    provenance_col: str | None = None
) -> T.List[DataFrame]:
    """
    Extracts the minimal rows from source DataFrames that contributed to the given final DataFrame.
    """
    if provenance_col is None:
        provenance_col = provenance_column_name(spark)

    gw = spark._sc._gateway
    
    java_source_dfs = gw.jvm.java.util.ArrayList()
    for source in source_dfs:
        java_source_dfs.add(source._jdf)
        
    j_extractor = gw.jvm.org.dataprov.dp.sparkdataprovenance.ProvenanceExtractor
    java_result_list = j_extractor.getMinimalSources(df._jdf, java_source_dfs, provenance_col)
    
    python_result_dfs = []
    for i in range(java_result_list.size()):
        python_result_dfs.append(DataFrame(java_result_list.get(i), spark))
        
    return python_result_dfs


def _py_dataframe_extension_get_minimal_sources(self, source_dfs: T.Sequence[DataFrame], provenance_col: str | None = None) -> T.List[DataFrame]:
    """Helper attached to PySpark DataFrame class for fluent API usage."""
    return get_minimal_sources(self, source_dfs, self.sparkSession, provenance_col)


DataFrame.get_minimal_sources = _py_dataframe_extension_get_minimal_sources