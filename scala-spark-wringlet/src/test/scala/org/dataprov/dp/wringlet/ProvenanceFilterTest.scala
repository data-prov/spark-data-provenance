package org.dataprov.dp.wringlet

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import org.dataprov.dp.wringlet.ProvenanceApi._

class ProvenanceFilterTest extends AnyFunSpec with Matchers with SparkSessionTestWrapper with DataFrameComparer with ProvenanceModeTestUtils {
  import spark.implicits._

  private def toyDf: DataFrame = Seq[(String, Int, java.lang.Double)](
    ("a", 1, 2.3),
    ("a", 1, 2.3),
    ("d", 2, 3.4),
    ("f", 3, null) 
  ).toDF("A", "B", "C")

  private def assertProvenanceColumnAndDataPreserved(dfExpected: DataFrame, provColName: String, dfWithProv: DataFrame): Unit = {
    assert(dfWithProv.columns.contains(provColName))
    assertSmallDataFrameEquality(dfWithProv, dfExpected)
  }

  describe("Filtering rows from a DataFrame/view with provenance") {
    
    itWithProvenanceEnabled("should preserve the provenance column and its values when filtering") {
      val df = toyDf
      val dfWithProvFiltered = addProvenance(df, col("B")).filter(col("B") > 1).select("A", "B")
      val expected = df.filter(col("B") > 1).select("A", "B").withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvFiltered)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when filtering with views") {
      val vName = "view_filter_nominal" 
      val df = toyDf
      df.createOrReplaceTempView(vName)
      addProvenance(spark, vName, col("B"))

      val dfWithProvFiltered = spark.sql(s"SELECT A, B FROM $vName WHERE B > 1")
      val expected = df.filter(col("B") > 1).select("A", "B").withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvFiltered)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when filtering with complex conditions") {
      val df = toyDf
      val dfWithProvFiltered = addProvenance(df, col("B")).filter(col("B") > 1 && col("C") < 4).select("A", "B")
      val expected = df.filter(col("B") > 1 && col("C") < 4).select("A", "B").withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvFiltered)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when filtering with complex conditions with views") {
      val vName = "view_filter_complex_and"
      val df = toyDf
      df.createOrReplaceTempView(vName)
      addProvenance(spark, vName, col("B"))

      val dfWithProvFiltered = spark.sql(s"SELECT A, B FROM $vName WHERE B > 1 AND C < 4")
      val expected = df.filter(col("B") > 1 && col("C") < 4).select("A", "B").withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvFiltered)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when filtering with OR conditions") {
      val df = toyDf
      val dfWithProvFiltered = addProvenance(df, col("B")).filter(col("B") > 1 || col("C") < 3).select("A", "B")
      val expected = df.filter(col("B") > 1 || col("C") < 3).select("A", "B").withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvFiltered)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when filtering with OR conditions with views") {
      val vName = "view_filter_complex_or" 
      val df = toyDf
      df.createOrReplaceTempView(vName)
      addProvenance(spark, vName, col("B"))

      val dfWithProvFiltered = spark.sql(s"SELECT A, B FROM $vName WHERE B > 1 OR C < 3")
      val expected = df.filter(col("B") > 1 || col("C") < 3).select("A", "B").withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvFiltered)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when filtering with a mix of AND/OR conditions") {
      val df = toyDf
      val dfWithProvFiltered = addProvenance(df, col("B")).filter((col("B") > 1 && col("C") < 4) || col("A") === "a").select("A", "B")
      val expected = df.filter((col("B") > 1 && col("C") < 4) || col("A") === "a").select("A", "B").withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvFiltered)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when filtering with a mix of AND/OR conditions with views") {
      val vName = "view_filter_mixed" 
      val df = toyDf
      df.createOrReplaceTempView(vName)
      addProvenance(spark, vName, col("B"))

      val dfWithProvFiltered = spark.sql(s"SELECT A, B FROM $vName WHERE (B > 1 AND C < 4) OR A = 'a'")
      val expected = df.filter((col("B") > 1 && col("C") < 4) || col("A") === "a").select("A", "B").withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvFiltered)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when filtering with NOT conditions") {
      val df = toyDf
      val dfWithProvFiltered = addProvenance(df, col("B")).filter(!(col("B") > 1)).select("A", "B")
      val expected = df.filter(!(col("B") > 1)).select("A", "B").withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvFiltered)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when filtering with NOT conditions with views") {
      val vName = "view_filter_not"
      val df = toyDf
      df.createOrReplaceTempView(vName)
      addProvenance(spark, vName, col("B"))

      val dfWithProvFiltered = spark.sql(s"SELECT A, B FROM $vName WHERE NOT (B > 1)")
      val expected = df.filter(!(col("B") > 1)).select("A", "B").withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvFiltered)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when filtering with IS NULL conditions") {
      val df = toyDf
      val dfWithProvFiltered = addProvenance(df, col("B")).filter(col("C").isNull).select("A", "B", "C")
      val expected = df.filter(col("C").isNull).select("A", "B", "C").withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvFiltered)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when filtering with IS NULL conditions with views") {
      val vName = "view_filter_isnull" 
      val df = toyDf
      df.createOrReplaceTempView(vName)
      addProvenance(spark, vName, col("B"))

      val dfWithProvFiltered = spark.sql(s"SELECT A, B, C FROM $vName WHERE C IS NULL")
      val expected = df.filter(col("C").isNull).select("A", "B", "C").withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvFiltered)
    }
  }
}