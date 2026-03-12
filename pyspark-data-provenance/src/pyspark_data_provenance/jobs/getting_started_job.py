#
# File: https://github.com/data-prov/spark-data-provenance/blob/main/pyspark-data-provenance/src/pyspark_data_provenance/jobs/getting_started_job.py
#
# See also https://github.com/data-prov/spark-data-provenance/blob/main/pyspark-data-provenance/notebooks/demo.ipynb
#
"""
Pyspark script to test the data provenance concept
"""

import datetime

from pyspark.sql import SparkSession

import pyspark_data_provenance as dp

JOB_NAME = "getting_started_job"
today_date: str = datetime.date.today().strftime("%Y-%m-%d")
now_datetime: datetime.datetime = datetime.datetime.now(datetime.UTC)

def main():
    """
    The dp-spark JAR is expected to have been installed locally, for instance,
    thanks to `sbt publishLocal publishM2` in the Scala-related folder
    """
    spark = (
        SparkSession
        .builder
        .appName(JOB_NAME)
        .config("spark.sql.extensions", "org.dataprov.dp.ProvenanceExtension")
        .enableHiveSupport()
        .getOrCreate()
    )

    # Showcase that the data_provenance_enabled() function works as expected
    print(spark.conf.get("spark.provenance.enabled", "false"))
    with dp.data_provenance_enabled(spark):
        print(spark.conf.get("spark.provenance.enabled", "false"))
    print(spark.conf.get("spark.provenance.enabled", "false"))

    # Toy sample
    df = spark.createDataFrame(
        [
            ("A", datetime.date(2026, 1, 15), 10.0, 90),
            ("A", datetime.date(2026, 1, 16), 10.0, 120),
            ("A", datetime.date(2026, 1, 17), 5.0, 300),
            ("B", datetime.date(2026, 1, 15), 100.0, 20),
            ("B", datetime.date(2026, 1, 16), 100.0, 30),
            ("B", datetime.date(2026, 1, 17), 80.0, 60),
        ], ["product", "date", "price", "sales"]
    )
    df.printSchema()
    df.show()

    # Test with PySpark syntax

    ## Without provenance
    df2 = df.select("product")
    print("Without provenance")
    df2.show()

    ## With provenance
    df3 = None
    with dp.data_provenance_enabled(spark):
        df3 = df.select("product")
    print("With provenance")
    df3.show()

    # Test with SQL syntax
    result_df = None
    df.createOrReplaceTempView("sales")
    with dp.data_provenance_enabled(spark):
        result_df = spark.sql("select * from sales")

    result_df.show()

if __name__ == "__main__":
    main()
