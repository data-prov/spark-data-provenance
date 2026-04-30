import typing as T

from py4j.java_gateway import JVMView
from pyspark.sql import SparkSession


def _get_jvm_from_spark(spark: SparkSession) -> JVMView:
    """
    Helper function to get the JVM view from a SparkSession.
    """
    assert spark._jvm is not None
    return spark._jvm


def _get_provenance_jvm_function(name: str, spark: SparkSession) -> T.Callable:
    """
    Retrieves JVM function identified by name from
    Java gateway associated with Spark session.
    """
    jvm = _get_jvm_from_spark(spark)
    return getattr(getattr(jvm, "org.dataprov.dp.sparkdataprovenance.DataFrameProvenanceTransformations"), name)
