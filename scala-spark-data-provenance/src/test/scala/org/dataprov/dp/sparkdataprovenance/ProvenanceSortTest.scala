package org.dataprov.dp.sparkdataprovenance

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, when}

import org.dataprov.dp.sparkdataprovenance.ProvenanceApi._

class ProvenanceSortTest extends AnyFunSpec with Matchers with SparkSessionTestWrapper with DataFrameComparer with ProvenanceModeTestUtils {
  import spark.implicits._

  private def toyDf: DataFrame = Seq(
    ("a", 1, 2.3),
    ("a", 1, 2.3),
    ("d", 2, 3.4),
    ("f", 3, 4.5)
  ).toDF("A", "B", "C")

 
  private val viewName: String = "toy_view"

  private def assertProvenanceColumnAndDataPreserved(dfExpected: DataFrame, provColName: String, dfWithProv: DataFrame): Unit = {
    // 1. The provenance column should be added
    assert(dfWithProv.columns.contains(provColName))
    // 2. The expected dataframe (including the provenance column) should be equal to the actual dataframe with provenance
    assertSmallDataFrameEquality(dfWithProv, dfExpected)

  }

  describe("Sorting rows from a DataFrame/view with provenance") {
    itWithProvenanceEnabled("should preserve the provenance column and its values when sorting") {
      val df = toyDf

      val dfWithProvSorted = addProvenance(df, col("B")).orderBy(col("B")).select("A", "B")
      val expected = df.orderBy(col("B")).select("A", "B").withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvSorted)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when sorting with views") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)
      addProvenance(spark, viewName, col("B"))

      val dfWithProvSorted = spark.sql(s"SELECT A, B FROM $viewName ORDER BY B")
      val expected = df.orderBy(col("B")).select("A", "B").withColumn(defaultProvenanceColName, col("B"))  

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvSorted)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when sorting with complex expressions") {
      val df = toyDf

      val dfWithProvSorted = addProvenance(df, col("B")).orderBy(col("B") + col("C")).select("A", "B")
      val expected = df.orderBy(col("B") + col("C")).select("A", "B").withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvSorted)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when sorting with complex expressions with views") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)
      addProvenance(spark, viewName, col("B"))

      val dfWithProvSorted = spark.sql(s"SELECT A, B FROM $viewName ORDER BY B + C")
      val expected = df.orderBy(col("B") + col("C")).select("A", "B").withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvSorted)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when sorting with ascending/descending order") {
      val df = toyDf

      val dfWithProvSorted = addProvenance(df, col("B")).orderBy(col("B").desc).select("A", "B")
      val expected = df.orderBy(col("B").desc).select("A", "B").withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvSorted)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when sorting with ascending/descending order with views") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)
      addProvenance(spark, viewName, col("B"))

      val dfWithProvSorted = spark.sql(s"SELECT A, B FROM $viewName ORDER BY B DESC")
      val expected = df.orderBy(col("B").desc).select("A", "B").withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvSorted)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when sorting with null values") {
      val df = toyDf.withColumn("D", col("B").cast("string"))
        .withColumn("D", when(col("B") === 1, null).otherwise(col("D")))

      val dfWithProvSorted = addProvenance(df, col("B")).orderBy(col("B")).select("A", "B", "D")
      val expected = df.orderBy(col("B")).select("A", "B", "D").withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvSorted)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when sorting with null values with views") {
      val df = toyDf.withColumn("D", col("B").cast("string"))
        .withColumn("D", when(col("B") === 1, null).otherwise(col("D")))

      df.createOrReplaceTempView(viewName)
      addProvenance(spark, viewName, col("B"))

      val dfWithProvSorted = spark.sql(s"SELECT A, B, D FROM $viewName ORDER BY B")
      val expected = df.orderBy(col("B")).select("A", "B", "D").withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvSorted)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when sorting with multiple columns") {
      val df = toyDf

      val dfWithProvSorted = addProvenance(df, col("B")).orderBy(col("B"), col("C").desc).select("A", "B", "C")
      val expected = df.orderBy(col("B"), col("C").desc).select("A", "B", "C").withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvSorted)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when sorting with multiple columns with views") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)
      addProvenance(spark, viewName, col("B"))

      val dfWithProvSorted = spark.sql(s"SELECT A, B, C FROM $viewName ORDER BY B, C DESC")
      val expected = df.orderBy(col("B"), col("C").desc).select("A", "B", "C").withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvSorted)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when sorting with multiple columns with null values") {
      val df = toyDf.withColumn("D", col("B").cast("string"))
        .withColumn("D", when(col("B") === 1, null).otherwise(col("D")))

      val dfWithProvSorted = addProvenance(df, col("B")).orderBy(col("B"), col("C").desc).select("A", "B", "C", "D")
      val expected = df.orderBy(col("B"), col("C").desc).select("A", "B", "C", "D").withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvSorted)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when sorting with multiple columns with null values with views") {
      val df = toyDf.withColumn("D", col("B").cast("string"))
        .withColumn("D", when(col("B") === 1, null).otherwise(col("D")))

      df.createOrReplaceTempView(viewName)
      addProvenance(spark, viewName, col("B"))

      val dfWithProvSorted = spark.sql(s"SELECT A, B, C, D FROM $viewName ORDER BY B, C DESC")
      val expected = df.orderBy(col("B"), col("C").desc).select("A", "B", "C", "D").withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvSorted)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when sorting with expressions on multiple columns") {
      val df = toyDf

      val dfWithProvSorted = addProvenance(df, col("B")).orderBy(col("B") + col("C"), col("C").desc).select("A", "B", "C")
      val expected = df.orderBy(col("B") + col("C"), col("C").desc).select("A", "B", "C").withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvSorted)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when sorting with expressions on multiple columns with views") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)
      addProvenance(spark, viewName, col("B"))

      val dfWithProvSorted = spark.sql(s"SELECT A, B, C FROM $viewName ORDER BY B + C, C DESC")
      val expected = df.orderBy(col("B") + col("C"), col("C").desc).select("A", "B", "C").withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvSorted)
    }
  }
}


