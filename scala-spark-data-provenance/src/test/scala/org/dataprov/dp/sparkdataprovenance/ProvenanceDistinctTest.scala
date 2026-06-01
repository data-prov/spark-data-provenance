package org.dataprov.dp

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, when}

import org.dataprov.dp.sparkdataprovenance.DataFrameProvenanceTransformations._

class ProvenanceDistinctTest extends AnyFunSpec with Matchers with SparkSessionTestWrapper with DataFrameComparer with ProvenanceModeTestUtils {
  import spark.implicits._

  private def toyDf: DataFrame = Seq(
    ("a", 1, 2.3),
    ("a", 2, 2.3),
    ("d", 2, 3.4),
    ("f", 3, 4.5)
  ).toDF("A", "B", "C")

  //private val viewName: String = "toy_view"

  private def assertProvenanceColumnAndDataPreserved(dfExpected: DataFrame, provColName: String, dfWithProv: DataFrame): Unit = {
    // 1. The provenance column should be added
    assert(dfWithProv.columns.contains(provColName))

    // Normalize provenance nullability for schema comparison while preserving values.
    val normalizedActual = dfWithProv.withColumn(
      provColName,
      when(col(provColName).isNotNull, col(provColName)).otherwise(lit(null).cast("string"))
    )

    val sortCols = dfExpected.columns.map(col).toIndexedSeq
    // 2. The expected dataframe (including the provenance column) should be equal to the actual dataframe with provenance
    assertSmallDataFrameEquality(normalizedActual.orderBy(sortCols: _*), dfExpected.orderBy(sortCols: _*))
  }

  describe("Deduplicating rows from a DataFrame/view with provenance") {
    itWithProvenanceEnabled("should preserve the provenance column and its values when performing a distinct") {    
      val df = toyDf
      val dfWithProv = addProvenance(df, col("B"))
      val dfWithProvDistinct = dfWithProv.select("A", "C").distinct()

      val expected: DataFrame = Seq(
        ("a", 2.3, "{1 ⊕ 2}"),
        ("d", 3.4, "2"),
        ("f", 4.5, "3")
      ).toDF("A", "C", defaultProvenanceColName)
      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvDistinct)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when performing a distinct with views") {
      val df = toyDf
      df.createOrReplaceTempView("toy_view")
      addProvenance(spark, "toy_view", col("B"))

      val dfWithProvDistinct = spark.sql(s"SELECT DISTINCT A, C FROM toy_view")

      val expected: DataFrame = Seq(
        ("a", 2.3, "{1 ⊕ 2}"),
        ("d", 3.4, "2"),
        ("f", 4.5, "3")
      ).toDF("A", "C", defaultProvenanceColName)
      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvDistinct)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when performing a distinct with explicit provenance selection") {
      val df = toyDf
      val dfWithProv = addProvenance(df, col("B"))
      val dfWithProvDistinct = dfWithProv.select("A", "C", defaultProvenanceColName).distinct()

      val expected: DataFrame = Seq(
        ("a", 2.3, "{1 ⊕ 2}"),
        ("d", 3.4, "2"),
        ("f", 4.5, "3")
      ).toDF("A", "C", defaultProvenanceColName)
      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvDistinct)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when performing a distinct with explicit provenance selection with views") {
      val df = toyDf
      df.createOrReplaceTempView("toy_view")
      addProvenance(spark, "toy_view", col("B"))

      val dfWithProvDistinct = spark.sql(s"SELECT DISTINCT A, C, $defaultProvenanceColName FROM toy_view")

      val expected: DataFrame = Seq(
        ("a", 2.3, "{1 ⊕ 2}"),
        ("d", 3.4, "2"),
        ("f", 4.5, "3")
      ).toDF("A", "C", defaultProvenanceColName)
      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvDistinct)
    }

    itWithProvenanceEnabled("should preserve provenance when aliasing columns and performing a distinct") {
      val df = toyDf
      val dfWithProv = addProvenance(df, col("B"))
      val dfWithProvDistinct = dfWithProv.select(col("A").as("A_alias"), col("C").as("C_alias"), col(defaultProvenanceColName)).distinct()

      val expected: DataFrame = Seq(
        ("a", 2.3, "{1 ⊕ 2}"),
        ("d", 3.4, "2"),
        ("f", 4.5, "3")
      ).toDF("A_alias", "C_alias", defaultProvenanceColName)
      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvDistinct)
    }

    itWithProvenanceEnabled("should preserve provenance when aliasing columns and performing a distinct with views") {
      val df = toyDf
      df.createOrReplaceTempView("toy_view")
      addProvenance(spark, "toy_view", col("B"))

      val dfWithProvDistinct = spark.sql(s"SELECT DISTINCT A AS A_alias, C AS C_alias, $defaultProvenanceColName FROM toy_view")

      val expected: DataFrame = Seq(
        ("a", 2.3, "{1 ⊕ 2}"),
        ("d", 3.4, "2"),
        ("f", 4.5, "3")
      ).toDF("A_alias", "C_alias", defaultProvenanceColName)
      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvDistinct)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when performing a dropDuplicates") {    
      val df = toyDf
      val dfWithProv = addProvenance(df, col("B"))
      val dfWithProvDistinct = dfWithProv.select("A", "C").dropDuplicates()

      val expected: DataFrame = Seq(
        ("a", 2.3, "{1 ⊕ 2}"),
        ("d", 3.4, "2"),
        ("f", 4.5, "3")
      ).toDF("A", "C", defaultProvenanceColName)
      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvDistinct)
    }
  }
}


