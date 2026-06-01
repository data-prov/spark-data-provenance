package org.dataprov.dp

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.dataprov.dp.sparkdataprovenance.DataFrameProvenanceTransformations._
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col


class ProvenanceAddTest extends AnyFunSpec with Matchers with SparkSessionTestWrapper with DataFrameComparer with ProvenanceModeTestUtils {
  
  import spark.implicits._

  private def toyDf: DataFrame = Seq(
    ("a", 1, 2.3),
    ("a", 1, 2.3),
    ("d", 2, 3.4)
  ).toDF("A", "B", "C")
  private val viewName: String = "toy_view"

  private val customProvColName: String = "custom_prov_col"

  private def assertProvenanceColumnAndDataPreserved(df: DataFrame, provColName: String, dfWithProv: DataFrame): Unit = {
    // 1. The provenance column should be added
    assert(dfWithProv.columns.contains(provColName))

    // 2. The original data (except provenance) should be preserved
    assert(!isProvenanceEnabled(spark))
    assertSmallDataFrameEquality(dfWithProv.drop(provColName), df)
  }

  private def assertDataFrameProvenanceIdempotence(dfWithProv: DataFrame): Unit = {
    // If the dataframe already has a provenance column, it should stay unchanged and no new column should be added
    assertSmallDataFrameEquality(addProvenance(dfWithProv), dfWithProv)
  }

  private def assertViewProvenanceIdempotence(viewName: String): Unit = {
    val snapshotViewName = s"${viewName}_snapshot"

    try {
      spark.table(viewName).createOrReplaceTempView(snapshotViewName)
      addProvenance(spark, viewName)
      assertSmallDataFrameEquality(
        spark.table(viewName),
        spark.table(snapshotViewName),
      )
    } finally {
      spark.catalog.dropTempView(snapshotViewName)
    }
  }

  describe("Adding provenance tag (column) to a DataFrame/view") {
    // FIXME: the tests below should be runnable even with provenance enabled, but for now we disable it
    // The drop operation will create a Project node in the plan, which will be visible to our rule. 
    itWithProvenanceDisabled("should add a provenance column to a dataframe if not already present") {
      val df = toyDf

      // Add provenance to dataframe
      val dfWithProv = addProvenance(df)

      // Perform checks
      assertProvenanceColumnAndDataPreserved(df, defaultProvenanceColName, dfWithProv)
      assertDataFrameProvenanceIdempotence(dfWithProv)
    }

    itWithProvenanceDisabled("should add a provenance column to a dataframe if not already present with custom column name", customProvColName) {
      val df = toyDf

      // Add provenance to dataframe with custom column name
      val dfWithProv = addProvenance(df)

      // Perform checks
      assertProvenanceColumnAndDataPreserved(df, customProvColName, dfWithProv)
      assertDataFrameProvenanceIdempotence(dfWithProv)
    }

    itWithProvenanceDisabled("should add a provenance column to a dataframe with a provided expression") {
      val df = toyDf
      val providedProvenance = col("B")

      val dfWithProv = addProvenance(df, providedProvenance)
      val expected = df.withColumn(defaultProvenanceColName, col("B"))

      assertSmallDataFrameEquality(dfWithProv, expected)
    }

    itWithProvenanceDisabled("should replace an existing provenance column on a dataframe when a provided expression is used") {
      val df = toyDf
      val withInitialProv = addProvenance(df, col("B"))

      val updated = addProvenance(withInitialProv, col("C"))
      val expected = df.withColumn(defaultProvenanceColName, col("C"))

      assertSmallDataFrameEquality(updated, expected)
    }


    itWithProvenanceDisabled("should add a provenance column to a view if not already present") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)

      // Add provenance to view
      addProvenance(spark, viewName)

      // Perform checks
      assertProvenanceColumnAndDataPreserved(df, defaultProvenanceColName, spark.table(viewName))
      assertViewProvenanceIdempotence(viewName)
    }

    itWithProvenanceDisabled("should add a provenance column to a view if not already present with custom column name", customProvColName) {
      val df = toyDf
      df.createOrReplaceTempView(viewName)

      // Add provenance to dataframe with custom column name
      addProvenance(spark, viewName)

      // Perform checks
      assertProvenanceColumnAndDataPreserved(df, customProvColName, spark.table(viewName))
      assertViewProvenanceIdempotence(viewName)
    }

    itWithProvenanceDisabled("should add a provenance column to a view with a provided expression") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)

      addProvenance(spark, viewName, col("B"))

      val expected = df.withColumn(defaultProvenanceColName, col("B"))
      assertSmallDataFrameEquality(spark.table(viewName), expected)
    }

    itWithProvenanceDisabled("should replace an existing provenance column on a view when a provided expression is used") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)

      addProvenance(spark, viewName, col("B"))
      addProvenance(spark, viewName, col("C"))

      val expected = df.withColumn(defaultProvenanceColName, col("C"))
      assertSmallDataFrameEquality(spark.table(viewName), expected)
    }

  }
}