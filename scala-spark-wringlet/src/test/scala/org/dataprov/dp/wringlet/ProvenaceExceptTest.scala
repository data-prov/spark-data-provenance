package org.dataprov.dp.wringlet

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col}

import org.dataprov.dp.wringlet.ProvenanceApi._

class ProvenanceExceptTest extends AnyFunSpec with Matchers with SparkSessionTestWrapper with DataFrameComparer with ProvenanceModeTestUtils {
  import spark.implicits._

  private def toyDfLeft: DataFrame = Seq(
    ("a", Some(1), Some(2.3)),
    ("b", Some(2), Some(3.4)), 
    ("c", Some(3), Some(4.5)),
    ("b", None,    Some(5.6)),
    ("b", Some(7), None),
    (null, Some(1), Some(8.9))
  ).toDF("A", "B", "C")

  private def toyDfRight: DataFrame = Seq(
    ("a", Some(1), Some(2.3)), 
    ("d", Some(4), Some(5.6)),
    ("a", None,    Some(6.7)),
    ("a", Some(5), None),
    (null, Some(7), Some(8.9))
  ).toDF("A", "B", "C")

  private val viewLeft = "except_view_left"
  private val viewRight = "except_view_right"

  private def assertProvenanceColumnAndDataPreserved(dfExpected: DataFrame, provColName: String, dfWithProv: DataFrame): Unit = {
    // 1. The provenance column should be added
    assert(dfWithProv.columns.contains(provColName))
    // 2. The expected dataframe (including the provenance column) should be equal to the actual dataframe with provenance
    assertSmallDataFrameEquality(dfWithProv, dfExpected, ignoreNullable = true)
  }

  describe("Except operations with provenance") {

    itWithProvenanceEnabled("should preserve the left provenance column and its values when performing an except") {
      val dfLeft = toyDfLeft
      val dfRight = toyDfRight

      val dfWithProvLeft = addProvenance(dfLeft, col("A"))
      val dfWithProvRight = addProvenance(dfRight, col("A"))

      val dfActual = dfWithProvLeft.except(dfWithProvRight).orderBy("A")

      val expected = dfLeft.except(dfRight)
        .withColumn(defaultProvenanceColName, col("A").cast("string"))
        .orderBy("A")

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfActual)
    }

    itWithProvenanceEnabled("should preserve the left provenance column and its values when performing an except with views") {
      val dfLeft = toyDfLeft
      val dfRight = toyDfRight

      dfLeft.createOrReplaceTempView(viewLeft)
      dfRight.createOrReplaceTempView(viewRight)

      addProvenance(spark, viewLeft, col("A"))
      addProvenance(spark, viewRight, col("A"))

      val dfActual = spark.sql(
        s"""
           |SELECT A, B, C FROM $viewLeft
           |EXCEPT
           |SELECT A, B, C FROM $viewRight
         """.stripMargin).orderBy("A")

      val expected = dfLeft.except(dfRight)
        .withColumn(defaultProvenanceColName, col("A").cast("string"))
        .orderBy("A")

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfActual)
    }
  }
}