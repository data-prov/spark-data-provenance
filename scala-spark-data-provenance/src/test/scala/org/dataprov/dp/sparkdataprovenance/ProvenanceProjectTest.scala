package org.dataprov.dp

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}

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
    // 2. The expected dataframe (including the provenance column) should be equal to the actual dataframe with provenance
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

    it("should preserve provenance even when selecting only literals") {
      val df = toyDf

      val dfWithProvProjected = addProvenance(df, col("B")).select(col("A"), col("B"), col("C"), lit(1).as("literal_col"))
      val expected = df.select("A", "B", "C").withColumn("literal_col", lit(1)).withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvProjected)
    }

    it("should preserve provenance even when selecting only literals with views") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)
      addProvenance(spark, viewName, col("B"))

      val dfWithProvProjected = spark.sql(s"SELECT A, B, C, 1 AS literal_col FROM $viewName")
      val expected = df.select("A", "B", "C").withColumn("literal_col", lit(1)).withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvProjected)
    }

    it("should preserve provenance when projecting and renaming columns") {
      val df = toyDf

      val dfWithProvProjected = addProvenance(df, col("B")).withColumn("withC", col("C")).withColumnRenamed("B", "B_renamed").select("A", "B_renamed", "withC")
      val expected = df.withColumn("withC", col("C")).withColumnRenamed("B", "B_renamed").select("A", "B_renamed", "withC").withColumn(defaultProvenanceColName, col("B_renamed"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvProjected)
    }

    it("should preserve provenance when projecting and renaming columns with views") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)
      addProvenance(spark, viewName, col("B"))

      val dfWithProvProjected = spark.sql(s"SELECT A, B AS B_renamed, C FROM $viewName").withColumn("withC", col("C")).select("A", "B_renamed", "withC")
      val expected = df.withColumn("withC", col("C")).withColumnRenamed("B", "B_renamed").select("A", "B_renamed", "withC").withColumn(defaultProvenanceColName, col("B_renamed"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvProjected)
    }

    it("should preserve provenance when projecting and filtering columns") {
      val df = toyDf

      val dfWithProvProjected = addProvenance(df, col("B")).filter(col("B") > 1).select("A", "B")
      val expected = df.filter(col("B") > 1).select("A", "B").withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvProjected)
    }

    it("should preserve provenance when projecting and filtering columns with views") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)
      addProvenance(spark, viewName, col("B"))

      val dfWithProvProjected = spark.sql(s"SELECT A, B FROM $viewName WHERE B > 1")
      val expected = df.filter(col("B") > 1).select("A", "B").withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvProjected)

    }
  }
}


