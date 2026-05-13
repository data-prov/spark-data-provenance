package org.dataprov.dp

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.dataprov.dp.sparkdataprovenance.DataFrameProvenanceTransformations._
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.DataFrame


class ProvenanceRemoveTest extends AnyFunSpec with Matchers with SparkSessionTestWrapper with DataFrameComparer with SparkConfTestUtils {

  import spark.implicits._

  private def toyDf: DataFrame = Seq(
    ("a", 1, 2.3),
    ("a", 1, 2.3),
    ("d", 2, 3.4)
  ).toDF("A", "B", "C")
  private val viewName: String = "toy_view"

  private val customProvColName: String = "custom_prov_col"

  private def assertNoProvenanceColumnAndDataPreserved(dfWithoutProvenance: DataFrame, provColName: String, df: DataFrame): Unit = {
    // 1. The provenance column should not be present
    assert(!dfWithoutProvenance.columns.contains(provColName))

    // 2. The original data should be preserved
    assertSmallDataFrameEquality(dfWithoutProvenance, df)
  }

  private def assertDataFrameProvenanceIdempotent(dfWithoutProvenance: DataFrame): Unit = {
    // If the dataframe already has no provenance column, it should stay unchanged and no column should be removed
    assertSmallDataFrameEquality(removeProvenance(dfWithoutProvenance), dfWithoutProvenance)
  }

  private def assertViewProvenanceIdempotent(viewName: String): Unit = {
    val snapshotViewName = s"${viewName}_snapshot"

    try {
      spark.table(viewName).createOrReplaceTempView(snapshotViewName)
      removeProvenance(spark, viewName)
      assertSmallDataFrameEquality(
        spark.table(viewName),
        spark.table(snapshotViewName),
      )
    } finally {
      spark.catalog.dropTempView(snapshotViewName)
    }
  }

  describe("Removing provenance tag (column) from a DataFrame/view") {
    it("should remove the provenance column from a dataframe iff already present") {
      val df = toyDf

      // Add provenance to dataframe
      val dfWithProv = addProvenance(toyDf)

      // Remove provenance from dataframe
      val dfWithoutProvenance = removeProvenance(dfWithProv)

      // Perform checks
      assertNoProvenanceColumnAndDataPreserved(dfWithoutProvenance, defaultProvenanceColName, df)
      assertDataFrameProvenanceIdempotent(dfWithoutProvenance)
    }

    it("should remove the provenance column from a dataframe iff already present with custom column name") {
      val df = toyDf

      withSparkConf(spark, provenanceColConfKey, customProvColName) {
        // Add provenance to dataframe
        val dfWithProv = addProvenance(toyDf)

        // Remove provenance from dataframe
        val dfWithoutProvenance = removeProvenance(dfWithProv)

        // Perform checks
        assertNoProvenanceColumnAndDataPreserved(dfWithoutProvenance, customProvColName, df)
        assertDataFrameProvenanceIdempotent(dfWithoutProvenance)
      }
    }

    it("should remove the provenance column from a view iff already present") {
      val df = toyDf

      // Create view and add provenance to it
      df.createOrReplaceTempView(viewName)
      addProvenance(spark, viewName)

      // Remove provenance from view
      removeProvenance(spark, viewName)

      // Perform checks
      assertNoProvenanceColumnAndDataPreserved(spark.table(viewName), defaultProvenanceColName, df)
      assertViewProvenanceIdempotent(viewName)
    }

    it("should remove the provenance column from a view iff already present with custom column name") {
      val df = toyDf

      withSparkConf(spark, provenanceColConfKey, customProvColName) {
        // Create view and add provenance to it with custom column name
        df.createOrReplaceTempView(viewName)
        addProvenance(spark, viewName)

        // Remove provenance from view
        removeProvenance(spark, viewName)

        // Perform checks
        assertNoProvenanceColumnAndDataPreserved(spark.table(viewName), customProvColName, df)
        assertViewProvenanceIdempotent(viewName)
      }
    }
  }
}