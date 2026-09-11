package org.dataprov.dp.wringlet

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import org.dataprov.dp.wringlet.ProvenanceApi._

class ProvenanceUnionTest extends AnyFunSpec with Matchers with SparkSessionTestWrapper with DataFrameComparer with ProvenanceModeTestUtils {
  import spark.implicits._

  private def toyDfLeft: DataFrame = Seq(
    ("a", 1, 2.3),
    ("a", 1, 2.3),
    ("d", 2, 3.4),
    ("f", 3, 4.5)
  ).toDF("A", "B", "C")

  private def toyDfRight: DataFrame = Seq(
    ("a", 1, 2.3),
    ("b", 2, 3.4),
    ("d", 3, 4.5),
    ("d", 3, 4.5)
  ).toDF("A", "D", "E")

  private def assertProvenanceColumnAndDataPreserved(dfExpected: DataFrame, provColName: String, dfWithProv: DataFrame): Unit = {
    // 1. The provenance column should be added
    assert(dfWithProv.columns.contains(provColName))
    // 2. The expected dataframe (including the provenance column) should be equal to the actual dataframe with provenance
    assertSmallDataFrameEquality(dfWithProv, dfExpected)
  }

  describe("Union of DataFrames/views with provenance") {
    
    itWithProvenanceEnabled("should preserve the provenance column and its values when performing a unionByName") {
      val dfLeft = toyDfLeft
      val dfRight = toyDfRight

      val dfWithProvLeft = addProvenance(dfLeft, col("A"))
      val dfWithProvRight = addProvenance(dfRight, col("A"))
      
      val dfWithProvUnion = dfWithProvLeft.unionByName(dfWithProvRight, allowMissingColumns = true)
        .orderBy("A", "B", "C", "D", "E")

      val expected = dfLeft.select("A", "B", "C").withColumn(defaultProvenanceColName, col("A"))
        .unionByName(dfRight.select("A", "D", "E").withColumn(defaultProvenanceColName, col("A")), allowMissingColumns = true)
        .orderBy("A", "B", "C", "D", "E")

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvUnion)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when performing a positional union all with views") {
      val dfLeft = toyDfLeft
      val dfRight = toyDfRight
      dfLeft.createOrReplaceTempView("left_view")
      dfRight.createOrReplaceTempView("right_view")
      addProvenance(spark, "left_view", col("A"))
      addProvenance(spark, "right_view", col("A"))

      val dfWithProvUnion = spark.sql(
        s"""
           |SELECT A, B, C FROM left_view
           |UNION ALL
           |SELECT A, D, E FROM right_view
        """.stripMargin).orderBy("A", "B", "C")

      val expected = dfLeft.select("A", "B", "C").withColumn(defaultProvenanceColName, col("A"))
        .union(dfRight.select("A", "D", "E").withColumn(defaultProvenanceColName, col("A")))
        .orderBy("A", "B", "C")

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvUnion)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when performing a union distinct with views") {
      val dfLeft = toyDfLeft
      val dfRight = toyDfRight
      dfLeft.createOrReplaceTempView("left_view")
      dfRight.createOrReplaceTempView("right_view")
      addProvenance(spark, "left_view", col("A"))
      addProvenance(spark, "right_view", col("A"))

      val dfWithProvUnion = spark.sql(
        s"""
           |SELECT A, B, C FROM left_view
           |UNION
           |SELECT A, D, E FROM right_view
        """.stripMargin).orderBy("A", "B", "C")

      val expected = dfLeft.select("A", "B", "C").withColumn(defaultProvenanceColName, col("A"))
        .union(dfRight.select("A", "D", "E").withColumn(defaultProvenanceColName, col("A")))
        .distinct() 
        .orderBy("A", "B", "C")

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvUnion)
    }
    
    itWithProvenanceEnabled("should preserve the provenance column and its values when performing a union distinct with views and filtering") {
      val dfLeft = toyDfLeft
      val dfRight = toyDfRight
      dfLeft.createOrReplaceTempView("left_view")
      dfRight.createOrReplaceTempView("right_view")
      addProvenance(spark, "left_view", col("A"))
      addProvenance(spark, "right_view", col("A"))

      val dfWithProvUnion = spark.sql(
        s"""
           |SELECT A, B, C FROM left_view
           |UNION
           |SELECT A, D, E FROM right_view
        """.stripMargin)
        .filter(col("A") === "a")
        .orderBy("A", "B", "C")

      val expected = dfLeft.select("A", "B", "C").withColumn(defaultProvenanceColName, col("A"))
        .union(dfRight.select("A", "D", "E").withColumn(defaultProvenanceColName, col("A")))
        .distinct() 
        .filter(col("A") === "a")
        .orderBy("A", "B", "C")

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvUnion)
    }
  }
}