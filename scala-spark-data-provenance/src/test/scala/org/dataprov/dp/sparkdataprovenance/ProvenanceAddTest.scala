package org.dataprov.dp

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.dataprov.dp.sparkdataprovenance.DataFrameProvenanceTransformations._
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.DataFrame


class ProvenanceAddTest extends AnyFunSpec with Matchers with SparkSessionTestWrapper with DataFrameComparer with SparkConfTestUtils {
  
  import spark.implicits._

  private def toyDf: DataFrame = Seq(
    ("a", 1, 2.3),
    ("a", 1, 2.3),
    ("d", 2, 3.4)
  ).toDF("A", "B", "C")
  private val viewName = "toy_view"

  private val customProvColName = "custom_prov_col"

  private def assertProvenanceColumnAndDataPreserved(df: DataFrame, provColName: String, dfWithProv: DataFrame): Unit = {
    // 1. The provenance column should be added
    assert(dfWithProv.columns.contains(provColName))

    // 2. The original data (except provenance) should be preserved
    assertSmallDataFrameEquality(dfWithProv.drop(provColName), df)
  }

  private def assertDataFrameProvenanceIdempotent(dfWithProv: DataFrame): Unit = {
    // If the dataframe already has a provenance column, it should stay unchanged and no new column should be added
    assertSmallDataFrameEquality(addProvenanceColumn(dfWithProv), dfWithProv)
  }

  private def assertViewProvenanceIdempotent(viewName: String): Unit = {
    val snapshotViewName = s"${viewName}_snapshot"

    try {
      spark.table(viewName).createOrReplaceTempView(snapshotViewName)
      addProvenanceColumn(spark, viewName)
      assertSmallDataFrameEquality(
        spark.table(viewName),
        spark.table(snapshotViewName),
      )
    } finally {
      spark.catalog.dropTempView(snapshotViewName)
    }
  }

  describe("Adding provenance tag (column) to a DataFrame/view") {
    it("should add a provenance column to a dataframe iff not already present") {
      val df = toyDf

      // Add provenance to dataframe
      val result = addProvenanceColumn(df)

      // Perform checks
      assertProvenanceColumnAndDataPreserved(df, provenanceColumnName(spark), result)
      assertDataFrameProvenanceIdempotent(result)
    }

    it("should add a provenance column to a dataframe iff not already present with custom column name") {
      val df = toyDf

      withSparkConf(spark, provenanceColConfKey, customProvColName) {
        // Add provenance to dataframe with custom column name
        val dfWithProv = addProvenanceColumn(df)

        // Perform checks
        assertProvenanceColumnAndDataPreserved(df, customProvColName, dfWithProv)
        assertDataFrameProvenanceIdempotent(dfWithProv)
      }
    }


    it("should add a provenance column to a view iff not already present") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)

      // Add provenance to view
      addProvenanceColumn(spark, viewName)

      // Perform checks
      assertProvenanceColumnAndDataPreserved(df, provenanceColumnName(spark), spark.table(viewName))
      assertViewProvenanceIdempotent(viewName)
    }

    it("should add a provenance column to a view iff not already present with custom column name") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)

      withSparkConf(spark, provenanceColConfKey, customProvColName) {
        // Add provenance to dataframe with custom column name
        addProvenanceColumn(spark, viewName)

        // Perform checks
        assertProvenanceColumnAndDataPreserved(df, customProvColName, spark.table(viewName))
        assertViewProvenanceIdempotent(viewName)
      }
    }

  }
}