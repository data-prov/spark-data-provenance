package org.dataprov.dp.sparkdataprovenance

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, max, min, sum, avg}

import org.dataprov.dp.sparkdataprovenance.ProvenanceApi._

class ProvenanceAggregateTest extends AnyFunSpec with Matchers with SparkSessionTestWrapper with DataFrameComparer with ProvenanceModeTestUtils {
  import spark.implicits._

  private def toyDf: DataFrame = Seq(
    ("a", 1, 2.3),
    ("a", 1, 2.3),
    ("b", 2, 3.4),
    ("d", 2, 4.5),
    ("d", 3, 3.4),
    ("f", 3, 4.5)
  ).toDF("A", "B", "C")

  private val viewName: String = "toy_aggregate_view"

  private def assertProvenanceColumnAndDataPreserved(dfExpected: DataFrame, provColName: String, dfWithProv: DataFrame): Unit = {
    assert(dfWithProv.columns.contains(provColName))
    assertSmallDataFrameEquality(dfWithProv, dfExpected, ignoreNullable = true)
  }

  describe("Aggregate operations with provenance") {

    itWithProvenanceEnabled("should preserve the provenance column and its values when performing a sum aggregation") {
      val df = toyDf
      val dfWithProv = addProvenance(df, col("A"))

      val dfActual = dfWithProv
        .groupBy("A")
        .agg(sum("B").as("total_B"))
        .orderBy("A")

      val expected = df
        .groupBy("A")
        .agg(sum("B").as("total_B"))
        .withColumn(defaultProvenanceColName, col("A").cast("string"))
        .orderBy("A")

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfActual)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when performing a sum aggregation with views") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)
      addProvenance(spark, viewName, col("A"))

      val dfActual = spark.sql(s"SELECT A, SUM(B) AS sum_B, MIN(C) AS min_C FROM $viewName GROUP BY A ORDER BY A")

      val expected = df
        .groupBy("A")
        .agg(sum("B").as("sum_B"), min("C").as("min_C"))
        .withColumn(defaultProvenanceColName, col("A").cast("string"))
        .orderBy("A")

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfActual)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when performing a max aggregation") {
      val df = toyDf
      val dfWithProv = addProvenance(df, col("A"))

      val dfActual = dfWithProv
        .groupBy("A")
        .agg(max("B").as("max_B"))
        .orderBy("A")

      val expected = df
        .groupBy("A")
        .agg(max("B").as("max_B"))
        .withColumn(defaultProvenanceColName, col("A").cast("string"))
        .orderBy("A")

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfActual)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when performing a min aggregation with views") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)
      addProvenance(spark, viewName, col("A"))

      val dfActual = spark.sql(s"SELECT A, MIN(B) AS min_B FROM $viewName GROUP BY A ORDER BY A")

      val expected = df
        .groupBy("A")
        .agg(min("B").as("min_B"))
        .withColumn(defaultProvenanceColName, col("A").cast("string"))
        .orderBy("A")

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfActual)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when performing multiple aggregations") {
      val df = toyDf
      val dfWithProv = addProvenance(df, col("A"))

      val dfActual = dfWithProv
        .groupBy("A")
        .agg(max("B").as("max_B"), min("C").as("min_C"), sum("B").as("total_B"))
        .orderBy("A")

      val expected = df
        .groupBy("A")
        .agg(max("B").as("max_B"), min("C").as("min_C"), sum("B").as("total_B"))
        .withColumn(defaultProvenanceColName, col("A").cast("string"))
        .orderBy("A")

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfActual)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when performing multiple aggregations with views") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)
      addProvenance(spark, viewName, col("A"))

      val dfActual = spark.sql(s"SELECT A, MAX(B) AS max_B, SUM(C) AS total_C FROM $viewName GROUP BY A ORDER BY A")

      val expected = df
        .groupBy("A")
        .agg(max("B").as("max_B"), sum("C").as("total_C"))
        .withColumn(defaultProvenanceColName, col("A").cast("string"))
        .orderBy("A")

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfActual)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when performing a mean aggregation") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)
      addProvenance(spark, viewName, col("A"))

      val dfActual = spark.table(viewName)
        .filter(col("A") =!= "d")
        .groupBy("A")
        .agg(avg("C").as("avg_C"))
        .orderBy("A")

      val expected = df
        .filter(col("A") =!= "d")
        .groupBy("A")
        .agg(avg("C").as("avg_C"))
        .withColumn(defaultProvenanceColName, col("A").cast("string"))
        .orderBy("A")

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfActual)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when performing a mean aggregation with views") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)
      addProvenance(spark, viewName, col("A"))

      val dfActual = spark.sql(s"SELECT A, AVG(C) AS avg_C FROM $viewName WHERE A != 'd' GROUP BY A ORDER BY A")

      val expected = df
        .filter(col("A") =!= "d")
        .groupBy("A")
        .agg(avg("C").as("avg_C"))
        .withColumn(defaultProvenanceColName, col("A").cast("string"))
        .orderBy("A")

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfActual)
    }
    
    itWithProvenanceEnabled("should preserve the provenance column and its values when performing a min and a max aggregation") {
      val df = toyDf
      val dfWithProv = addProvenance(df, col("A"))

      val dfActual = dfWithProv
        .groupBy("A")
        .agg(min("B").as("min_B"), max("B").as("max_B"))
        .orderBy("A")

      val expected = df
        .groupBy("A")
        .agg(min("B").as("min_B"), max("B").as("max_B"))
        .withColumn(defaultProvenanceColName, col("A").cast("string"))
        .orderBy("A")

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfActual)
    }
    
    itWithProvenanceEnabled("should preserve the provenance column and its values when performing a min and a max aggregation with views") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)
      addProvenance(spark, viewName, col("A"))

      val dfActual = spark.sql(s"SELECT A, MIN(B) AS min_B, MAX(B) AS max_B FROM $viewName GROUP BY A ORDER BY A")

      val expected = df
        .groupBy("A")
        .agg(min("B").as("min_B"), max("B").as("max_B"))
        .withColumn(defaultProvenanceColName, col("A").cast("string"))
        .orderBy("A")

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfActual)
    }

  }
}