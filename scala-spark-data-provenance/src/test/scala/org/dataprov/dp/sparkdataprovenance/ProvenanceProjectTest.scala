package org.dataprov.dp

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import org.dataprov.dp.sparkdataprovenance.DataFrameProvenanceTransformations._

class ProvenanceProjectTest extends AnyFunSpec with Matchers with SparkSessionTestWrapper with DataFrameComparer with SparkConfTestUtils {
  import spark.implicits._
  spark.conf.set(provenanceEnabledConf, "true")

  private def toyDf: DataFrame = Seq(
    ("a", "b", "c"),
    ("d", "b", "e"),
    ("f", "g", "e")
  ).toDF("A", "B", "C")
    
  private val viewName: String = "toy_view"

  describe("Projecting columns from a DataFrame/view with provenance") {
    it("should preserve the provenance column and its values when projecting") {
      val df = toyDf
      val dfWithProv = addProvenance(df, col("B"))
      val dfWithProvProjected = dfWithProv.select("A", "B")
      val expected = df.select("A", "B").withColumn(defaultProvenanceColName, col("B"))

      assert(dfWithProvProjected.columns.contains(defaultProvenanceColName))
      assertSmallDataFrameEquality(
        dfWithProvProjected,
        expected
      )
    }

    it("should preserve the provenance column and its values when projecting with views") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)
      addProvenance(spark, viewName, col("B"))
      val dfWithProvProjected = spark.sql(s"SELECT A, B FROM $viewName")
      val expected = df.select("A", "B").withColumn(defaultProvenanceColName, col("B"))

      assert(dfWithProvProjected.columns.contains(defaultProvenanceColName))
      assertSmallDataFrameEquality(
        dfWithProvProjected,
        expected
      )
    }

    it ("should not modify the provenance column if it is already present in the projection") {
      val df = toyDf
      val dfWithProv = addProvenance(df, col("B"))
      val dfWithProvProjected = dfWithProv.select("A", "B", defaultProvenanceColName)
      val expected = df.select("A", "B").withColumn(defaultProvenanceColName, col("B"))

      assertSmallDataFrameEquality(
        dfWithProvProjected,
        expected
      )
    }

    it ("should not modify the provenance column if it is already present in the projection with views") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)
      addProvenance(spark, viewName, col("B"))
      val dfWithProvProjected = spark.sql(s"SELECT A, B, $defaultProvenanceColName FROM $viewName")
      val expected = df.select("A", "B").withColumn(defaultProvenanceColName, col("B"))

      assertSmallDataFrameEquality(
        dfWithProvProjected,
        expected
      )
    }
  }
}
  



