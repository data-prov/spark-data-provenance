package org.dataprov.dp.sparkdataprovenance

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
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
        .groupBy("B")
        .agg(sum("C").as("total_B"))
        .orderBy("B")

      val expected = df
        .groupBy("B")
        .agg(
          sum("C").as("total_B"),
          collect_set("A").as("raw_set") 
        )
        .withColumn(defaultProvenanceColName, 
          when(org.apache.spark.sql.functions.size(col("raw_set")).gt(lit(1)), 
            concat(lit("{"), array_join(col("raw_set"), " ⊕ "), lit("}"))
          ).otherwise(
            col("raw_set").getItem(0)
          )
        )
        .drop("raw_set") 
        .orderBy("B")

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfActual)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when performing a sum aggregation with views") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)
      addProvenance(spark, viewName, col("A"))

      val dfActual = spark.sql(s"SELECT B, SUM(C) AS total_C FROM $viewName GROUP BY B ORDER BY B")

      val expected = df
        .groupBy("B")
        .agg(
          sum("C").as("total_C"),
          collect_set("A").as("raw_set") 
        )
        .withColumn(defaultProvenanceColName, 
          when(org.apache.spark.sql.functions.size(col("raw_set")).gt(lit(1)), 
            concat(lit("{"), array_join(col("raw_set"), " ⊕ "), lit("}"))
          ).otherwise(
            col("raw_set").getItem(0)
          )
        )
        .drop("raw_set")
        .orderBy("B")

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfActual)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when performing a max aggregation") {
      val df = toyDf
      val dfWithProv = addProvenance(df, col("A"))

      val dfActual = dfWithProv
        .groupBy("B")
        .agg(max("C").as("max_C"))
        .orderBy("B")

      val expected = df
        .groupBy("B")
        .agg(
          max("C").as("max_C"),
          expr("max_by(A, C)").cast("string").alias(defaultProvenanceColName)
        )
        .orderBy("B")

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfActual)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when performing a min aggregation with views") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)
      addProvenance(spark, viewName, col("A"))

      val dfActual = spark.sql(s"SELECT B, MIN(C) AS min_C FROM $viewName GROUP BY B ORDER BY B")

      val expected = df
        .groupBy("B")
        .agg(
          min("C").as("min_C"),
          expr("min_by(A, C)").cast("string").alias(defaultProvenanceColName)
        )
        .orderBy("B")

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfActual)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when performing multiple aggregations") {
      val df = toyDf
      val dfWithProv = addProvenance(df, col("A"))

      val dfActual = dfWithProv
        .groupBy("B")
        .agg(max("C").as("max_C"), min("C").as("min_C"), sum("C").as("total_C"))
        .orderBy("B")

      val expected = df
        .groupBy("B")
        .agg(
          max("C").as("max_C"),
          min("C").as("min_C"),
          sum("C").as("total_C"),
          collect_set("A").as("raw_set") 
        )
        .withColumn(defaultProvenanceColName, 
          when(org.apache.spark.sql.functions.size(col("raw_set")).gt(lit(1)), 
            concat(lit("{"), array_join(col("raw_set"), " ⊕ "), lit("}"))
          ).otherwise(
            col("raw_set").getItem(0)
          )
        )
        .drop("raw_set") 
        .orderBy("B")

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfActual)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when performing multiple aggregations with views") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)
      addProvenance(spark, viewName, col("A"))

      val dfActual = spark.sql(s"SELECT B, MAX(C) AS max_C, MIN(C) AS min_C, SUM(C) AS total_C FROM $viewName GROUP BY B ORDER BY B")

      val expected = df
        .groupBy("B")
        .agg(
          max("C").as("max_C"),
          min("C").as("min_C"),
          sum("C").as("total_C"),
          collect_set("A").as("raw_set") 
        )
        .withColumn(defaultProvenanceColName, 
          when(org.apache.spark.sql.functions.size(col("raw_set")).gt(lit(1)), 
            concat(lit("{"), array_join(col("raw_set"), " ⊕ "), lit("}"))
          ).otherwise(
            col("raw_set").getItem(0)
          )
        )
        .drop("raw_set") 
        .orderBy("B")

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfActual)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when performing a mean aggregation") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)
      addProvenance(spark, viewName, col("A"))

      val dfActual = spark.table(viewName)
        .groupBy("B")
        .agg(avg("C").as("avg_C"))
        .orderBy("B")

      val expected = df
        .groupBy("B")
        .agg(
          avg("C").as("avg_C"),
          collect_set("A").as("raw_set") 
        )
        .withColumn(defaultProvenanceColName, 
          when(org.apache.spark.sql.functions.size(col("raw_set")).gt(lit(1)), 
            concat(lit("{"), array_join(col("raw_set"), " ⊕ "), lit("}"))
          ).otherwise(
            col("raw_set").getItem(0)
          )
        )
        .drop("raw_set")
        .orderBy("B")

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfActual)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when performing a mean aggregation with views") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)
      addProvenance(spark, viewName, col("A"))

      val dfActual = spark.sql(s"SELECT B, AVG(C) AS avg_C FROM $viewName GROUP BY B ORDER BY B")

      val expected = df
        .groupBy("B")
        .agg(
          avg("C").as("avg_C"),
          collect_set("A").as("raw_set") 
        )
        .withColumn(defaultProvenanceColName, 
          when(org.apache.spark.sql.functions.size(col("raw_set")).gt(lit(1)), 
            concat(lit("{"), array_join(col("raw_set"), " ⊕ "), lit("}"))
          ).otherwise(
            col("raw_set").getItem(0)
          )
        )
        .drop("raw_set")
        .orderBy("B")

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfActual)
    }
    
    itWithProvenanceEnabled("should preserve the provenance column and its values when performing a min and a max aggregation") {
      val df = toyDf
      val dfWithProv = addProvenance(df, col("A"))

      val dfActual = dfWithProv
        .groupBy("B")
        .agg(min("C").as("min_C"), max("C").as("max_C"))
        .orderBy("B")

      val expected = df
        .groupBy("B")
        .agg(
          min("C").as("min_C"), 
          max("C").as("max_C"),
          array_distinct(array(
            expr("max_by(A, C)"), 
            expr("min_by(A, C)")
          )).as("raw_array")
        )
        .withColumn(defaultProvenanceColName, coalesce(
          when(org.apache.spark.sql.functions.size(col("raw_array")) > lit(1), 
            concat(lit("{"), array_join(col("raw_array"), " ⊕ "), lit("}"))
          ).otherwise(
            col("raw_array").getItem(0)
          ).cast("string"),
          lit("")
        ))
        .drop("raw_array")
        .orderBy("B")

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfActual)
    }
    
    itWithProvenanceEnabled("should preserve the provenance column and its values when performing a min and a max aggregation with views") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)
      addProvenance(spark, viewName, col("A"))

      val dfActual = spark.sql(s"SELECT B, MIN(C) AS min_C, MAX(C) AS max_C FROM $viewName GROUP BY B ORDER BY B")

      val expected = df
        .groupBy("B")
        .agg(
          min("C").as("min_C"), 
          max("C").as("max_C"),
          array_distinct(array(
            expr("max_by(A, C)"), 
            expr("min_by(A, C)")
          )).as("raw_array")
        )
        .withColumn(defaultProvenanceColName, coalesce(
          when(org.apache.spark.sql.functions.size(col("raw_array")) > lit(1), 
            concat(lit("{"), array_join(col("raw_array"), " ⊕ "), lit("}"))
          ).otherwise(
            col("raw_array").getItem(0)
          ).cast("string"),
          lit("")
        ))
        .drop("raw_array")
        .orderBy("B")

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfActual)
    }

  }
}