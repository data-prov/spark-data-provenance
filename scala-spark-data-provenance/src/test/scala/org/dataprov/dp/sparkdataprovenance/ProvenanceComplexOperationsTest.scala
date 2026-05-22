package org.dataprov.dp

import org.dataprov.dp.sparkdataprovenance.DataFrameProvenanceTransformations._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.dataprov.dp.SparkConfTestUtils
import org.dataprov.dp.SparkSessionTestWrapper
import org.dataprov.dp.sparkdataprovenance.DataFrameProvenanceTransformations.defaultProvenanceColName
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import com.github.mrpowers.spark.fast.tests.DataFrameComparer

class ProvenanceComplexOperationsTest extends AnyFunSpec with Matchers with SparkSessionTestWrapper with DataFrameComparer with SparkConfTestUtils {
  import spark.implicits._
  spark.conf.set("spark.provenance.enabled", "true")

  private def toyDfLeft: DataFrame = Seq(
    ("a", 1, 2.3),
    ("a", 1, 2.3),
    ("d", 2, 3.4),
    ("f", 3, 4.5)
  ).toDF("A", "B", "C")

  private def toyDfRight: DataFrame = Seq(
    ("a", "x"),
    ("a", "x"),
    ("d", "y"),
    ("e", "z")
  ).toDF("A", "D")

  private def assertProvenanceColumnAndDataPreserved(dfExpected: DataFrame, provColName: String, dfWithProv: DataFrame): Unit = {
    // 1. The provenance column should be added
    assert(dfWithProv.columns.contains(provColName))
    // 2. The expected dataframe (including the provenance column) should be equal to the actual dataframe with provenance
    assertSmallDataFrameEquality(dfWithProv, dfExpected)

  }

  describe("Complex operations on DataFrames/views with provenance") {
    it("should preserve the provenance column and its values when performing a cross join followed by a distinct") {
      val dfLeft = toyDfLeft
      val dfRight = toyDfRight

      val dfWithProvLeft = addProvenance(dfLeft, col("B"))
      val dfWithProvRight = addProvenance(dfRight, col("D"))

      val dfLeftSide = dfLeft.alias("l").select("l.A", "l.B", "l.C").withColumn("leftProv", col("l.B").cast("string"))
      val dfRightSide = dfRight.alias("r").select("r.A", "r.D").withColumn("rightProv", col("r.D").cast("string"))
      val dfWithProvJoin: DataFrame = dfWithProvLeft.alias("l").crossJoin(dfWithProvRight.alias("r")).select("l.A", "l.B", "l.C", "r.D").distinct().orderBy("l.A", "l.B", "l.C", "r.D")
      val expected: DataFrame = dfLeftSide.alias("l").crossJoin(dfRightSide.alias("r"))
        .withColumn(defaultProvenanceColName, concat(lit("("), col("leftProv"), lit(" ⊗ "), col("rightProv"), lit(")")).cast("string"))
        .drop("leftProv", "rightProv")
        .select("l.A", "l.B", "l.C", "r.D", defaultProvenanceColName)
        .distinct()
        .orderBy("l.A", "l.B", "l.C", "r.D")
       
      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvJoin)
    }

    it("should preserve the provenance column and its values when performing a cross join followed by a distinct with views") {
      val dfLeft = toyDfLeft
      val dfRight = toyDfRight

      dfLeft.createOrReplaceTempView("left_view")
      dfRight.createOrReplaceTempView("right_view")
      addProvenance(spark, "left_view", col("B"))
      addProvenance(spark, "right_view", col("D"))

      val dfWithProvJoin: DataFrame = spark.sql(
        s"""
           SELECT DISTINCT l.A, l.B, l.C, r.D
           FROM left_view l
           CROSS JOIN right_view r
           ORDER BY l.A, l.B, l.C, r.D
         """
      )

      val dfLeftSide = dfLeft.alias("l").select("l.A", "l.B", "l.C").withColumn("leftProv", col("l.B").cast("string"))
      val dfRightSide = dfRight.alias("r").select("r.A", "r.D").withColumn("rightProv", col("r.D").cast("string"))
      val expected: DataFrame = dfLeftSide.alias("l").crossJoin(dfRightSide.alias("r"))
         .withColumn(defaultProvenanceColName, concat(lit("("), col("leftProv"), lit(" ⊗ "), col("rightProv"), lit(")")).cast("string"))
         .drop("leftProv", "rightProv")
         .select("l.A", "l.B", "l.C", "r.D", defaultProvenanceColName)
         .distinct()
         .orderBy("l.A", "l.B", "l.C", "r.D")
      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvJoin)
    }
  }
}


