package org.dataprov.dp

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import org.dataprov.dp.sparkdataprovenance.DataFrameProvenanceTransformations._

class ProvenanceProjectTest extends AnyFunSpec with Matchers with SparkSessionTestWrapper with DataFrameComparer with SparkConfTestUtils {
  import spark.implicits._
  spark.conf.set("spark.provenance.enabled", "true")

  private def toyDf: DataFrame = Seq(
    ("a", 1, 2.3),
    ("a", 1, 2.3),
    ("d", 2, 3.4)
  ).toDF("A", "B", "C")
  private val viewName: String = "toy_view"

  private def assertProvenanceColumnAndDataPreserved(dfExpected: DataFrame, provColName: String, dfWithProv: DataFrame): Unit = {
    // 1. The provenance column should be added
    assert(dfWithProv.columns.contains(provColName))

    // 2. The original data (except provenance) should be preserved
    assertSmallDataFrameEquality(dfWithProv, dfExpected)

  }

  

  describe("Projecting columns from a DataFrame/view with provenance") {
    it("should preserve the provenance column and its values when projecting") {
      val df = toyDf
      val dfWithProvProjected = addProvenance(df, col("B")).select("A", "B")
      val expected = df.select("A", "B").withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvProjected)
    }

    it("should preserve the provenance column and its values when projecting with views") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)
      addProvenance(spark, viewName, col("B"))

      val dfWithProvProjected = spark.sql(s"SELECT A, B FROM $viewName")
      val expected = df.select("A", "B").withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvProjected)
    }

    it ("should not duplicate provenance when explicitly selected") {
      val df = toyDf
      val dfWithProvProjected = addProvenance(df, col("B")).select("A", "B", defaultProvenanceColName)
      val expected = df.select("A", "B").withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvProjected)
    }

    it ("should not duplicate provenance when explicitly selected with views") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)
      addProvenance(spark, viewName, col("B"))

      val dfWithProvProjected = spark.sql(s"SELECT A, B, $defaultProvenanceColName FROM $viewName")
      val expected = df.select("A", "B").withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvProjected)
    }

    it("should preserve provenance when aliasing columns") {
      val df = toyDf

      val dfWithProvProjected = spark.sql(s"SELECT A, B, B AS B_alias FROM $viewName")
      val expected = df.select("A", "B").withColumn("B_alias", col("B")).withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvProjected)
      assertSmallDataFrameEquality(dfWithProvProjected, expected)
    }

    it ("should preserve provenance when aliasing columns with views") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)
      addProvenance(spark, viewName, col("B"))

      val dfWithProvProjected = spark.sql(s"SELECT A, B, B AS B_alias FROM $viewName")
      val expected = df.select("A", "B").withColumn("B_alias", col("B")).withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvProjected)
    }


  }
}
  

