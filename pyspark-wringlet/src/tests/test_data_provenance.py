from __future__ import annotations

from typing import cast

from pyspark.sql import SparkSession

from pyspark_data_provenance.data_provenance import data_provenance_enabled


class FakeConf:
    def __init__(self) -> None:
        self._values: dict[str, str] = {}

    def get(self, key: str, default: str) -> str:
        return self._values.get(key, default)

    def set(self, key: str, value: str) -> None:
        self._values[key] = value


class FakeSparkSession:
    def __init__(self) -> None:
        self.conf = FakeConf()


def test_data_provenance_enabled_restores_previous_conf() -> None:
    spark = FakeSparkSession()
    spark.conf.set("spark.provenance.enabled", "false")

    with data_provenance_enabled(cast(SparkSession, spark)):
        assert spark.conf.get("spark.provenance.enabled", "false") == "true"

    assert spark.conf.get("spark.provenance.enabled", "false") == "false"


def test_data_provenance_enabled_restores_on_nested_context() -> None:
    spark = FakeSparkSession()
    spark.conf.set("spark.provenance.enabled", "true")

    with data_provenance_enabled(cast(SparkSession, spark)):
        assert spark.conf.get("spark.provenance.enabled", "false") == "true"

    assert spark.conf.get("spark.provenance.enabled", "false") == "true"
