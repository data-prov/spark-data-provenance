package org.dataprov.dp.wringlet

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{coalesce, col, concat, lit}

import org.dataprov.dp.wringlet.ProvenanceApi._

class ProvenanceIntersectTest extends AnyFunSpec with Matchers with SparkSessionTestWrapper with DataFrameComparer with ProvenanceModeTestUtils {
  import spark.implicits._

  // Les DataFrames stricts pour l'Intersect (A, B, C uniquement)
  private def toyDfLeft: DataFrame = Seq(
    ("a", 1, 2.3),
    ("a", 1, 2.3), 
    ("b", 2, 3.4),
    ("c", 3, 4.5)
  ).toDF("A", "B", "C")

  private def toyDfRight: DataFrame = Seq(
    ("a", 1, 2.3),
    ("c", 3, 4.5),
    ("c", 3, 4.5), 
    ("d", 4, 5.6)
  ).toDF("A", "B", "C")

  private def assertProvenanceColumnAndDataPreserved(dfExpected: DataFrame, provColName: String, dfWithProv: DataFrame): Unit = {
    assert(dfWithProv.columns.contains(provColName))
    assertSmallDataFrameEquality(dfWithProv, dfExpected, ignoreNullable = true)
  }

  describe("Intersect operations with provenance") {

    itWithProvenanceEnabled("should preserve the provenance column and its values when performing an intersect") {
      val dfLeft = toyDfLeft
      val dfRight = toyDfRight

      val dfWithProvLeft = addProvenance(dfLeft, col("A"))
      val dfWithProvRight = addProvenance(dfRight, col("A"))

      val dfWithProvInterssect = dfWithProvLeft.intersect(dfWithProvRight).orderBy("A")
      
      val expected = dfLeft.intersect(dfRight)
        .withColumn(defaultProvenanceColName, coalesce(
          concat(lit("("), col("A").cast("string"), lit(" ⊗ "), col("A").cast("string"), lit(")")),
          lit("")
        ))
        .orderBy("A")

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvInterssect)
    }
    
    itWithProvenanceEnabled("should preserve the provenance column and its values when performing an intersect with views") {
      val dfLeft = toyDfLeft
      val dfRight = toyDfRight

      dfLeft.createOrReplaceTempView("left_view")
      dfRight.createOrReplaceTempView("right_view")
      addProvenance(spark, "left_view", col("A"))
      addProvenance(spark, "right_view", col("A"))

      val dfActual = spark.sql(
        s"""
           |SELECT A, B, C FROM left_view
           |INTERSECT
           |SELECT A, B, C FROM right_view
            """.stripMargin).orderBy("A")

      val expected = dfLeft.intersect(dfRight)
        .withColumn(defaultProvenanceColName, coalesce(
          concat(lit("("), col("A").cast("string"), lit(" ⊗ "), col("A").cast("string"), lit(")")),
          lit("")
        ))
        .orderBy("A")

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfActual)
    }
  
    itWithProvenanceEnabled("should preserve the provenance column and its values when performing an intersect with only left provenance") {
      val dfLeft = toyDfLeft
      val dfRight = toyDfRight

      val dfWithProvLeft = addProvenance(dfLeft, col("A"))

      val dfActual = dfWithProvLeft.intersect(dfRight).orderBy("A")

      val expected = dfLeft.intersect(dfRight)
        .withColumn(defaultProvenanceColName, coalesce(col("A").cast("string"), lit("")))
        .orderBy("A")

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfActual)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when performing an intersect with only left provenance with views") {
      val dfLeft = toyDfLeft
      val dfRight = toyDfRight

      dfLeft.createOrReplaceTempView("left_view")
      dfRight.createOrReplaceTempView("right_view")
      addProvenance(spark, "left_view", col("A"))

      val dfActual = spark.sql(
        s"""
           |SELECT A, B, C, $defaultProvenanceColName FROM left_view
           |INTERSECT
           |SELECT A, B, C FROM right_view
         """.stripMargin).orderBy("A")

      val expected = dfLeft.intersect(dfRight)
        .withColumn(defaultProvenanceColName, coalesce(col("A").cast("string"), lit("")))
        .orderBy("A")

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfActual)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when performing an intersect with only right provenance") {
      val dfLeft = toyDfLeft
      val dfRight = toyDfRight 

      val dfWithProvRight = addProvenance(dfRight, col("A"))

      val dfActual = dfLeft.intersect(dfWithProvRight).orderBy("A")

      val expected = dfLeft.intersect(dfRight)
        .withColumn(defaultProvenanceColName, coalesce(col("A").cast("string"), lit("")))
        .orderBy("A")

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfActual)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when performing an intersect with only right provenance with views") {
      val dfLeft = toyDfLeft
      val dfRight = toyDfRight

      dfLeft.createOrReplaceTempView("left_view")
      dfRight.createOrReplaceTempView("right_view")
      addProvenance(spark, "right_view", col("A"))

      val dfActual = spark.sql(
        s"""
           |SELECT A, B, C FROM left_view
           |INTERSECT
           |SELECT A, B, C, $defaultProvenanceColName FROM right_view
         """.stripMargin).orderBy("A")

      val expected = dfLeft.intersect(dfRight)
        .withColumn(defaultProvenanceColName, coalesce(col("A").cast("string"), lit("")))
        .orderBy("A")

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfActual)
    }
  }
}