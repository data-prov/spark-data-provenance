package org.dataprov.dp

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array_union, col, concat, lit}

import org.dataprov.dp.sparkdataprovenance.DataFrameProvenanceTransformations._

class ProvenanceFilterTest extends AnyFunSpec with Matchers with SparkSessionTestWrapper with DataFrameComparer with SparkConfTestUtils {
  import spark.implicits._
  spark.conf.set("spark.provenance.enabled", "true")

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

  describe("Filtering rows from a DataFrame/view with provenance") {
    it("should preserve the provenance column and its values when filtering") {
      val df = toyDf

      val dfWithProvFiltered = addProvenance(df, col("B")).filter(col("B") > 1).select("A", "B")
      val expected = df.filter(col("B") > 1).select("A", "B").withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvFiltered)
    }

    it("should preserve the provenance column and its values when filtering with views") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)
      addProvenance(spark, viewName, col("B"))

      val dfWithProvFiltered = spark.sql(s"SELECT A, B FROM $viewName WHERE B > 1")
      val expected = df.filter(col("B") > 1).select("A", "B").withColumn(defaultProvenanceColName, col("B"))

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvFiltered)
    }
  }

  


    

}


