package org.dataprov.dp

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.dataprov.dp.sparkdataprovenance.DataFrameProvenanceTransformations._
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col


class ProvenanceAddTest extends AnyFunSpec with Matchers with SparkSessionTestWrapper with DataFrameComparer with SparkConfTestUtils {
  
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

    // FIX ME: spark-provenance: we may want to provide a utility method that ignores the provenance
    // column when comparing dataframes, to avoid having to disable provenance for this check

    // The drop operation will create a Project node in the plan, which will be visible to our rule. 
    // To avoid this, we need to disable provenance for this check
    // The session is created with the extension in SparkSessionTestWrapper
    spark.conf.set("spark.provenance.enabled", "false") 
    try {
      assertSmallDataFrameEquality(dfWithProv.drop(provColName), df)
    } finally {
      spark.conf.set("spark.provenance.enabled", "true")
    }
  }

  private def assertDataFrameProvenanceIdempotence(dfWithProv: DataFrame): Unit = {
    // If the dataframe already has a provenance column, it should stay unchanged and no new column should be added
    spark.conf.set("spark.provenance.enabled", "false")
    try {
      assertSmallDataFrameEquality(addProvenance(dfWithProv), dfWithProv)
    } finally {
      spark.conf.set("spark.provenance.enabled", "true")
    }
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
    it("should add a provenance column to a dataframe if not already present") {
      val df = toyDf

      // Add provenance to dataframe
      val dfWithProv = addProvenance(df)

      // Perform checks
      assertProvenanceColumnAndDataPreserved(df, defaultProvenanceColName, dfWithProv)
      assertDataFrameProvenanceIdempotence(dfWithProv)
    }

    it("should add a provenance column to a dataframe if not already present with custom column name") {
      val df = toyDf

      withSparkConf(spark, provenanceColConfKey, customProvColName) {
        // Add provenance to dataframe with custom column name
        val dfWithProv = addProvenance(df)

        // Perform checks
        assertProvenanceColumnAndDataPreserved(df, customProvColName, dfWithProv)
        assertDataFrameProvenanceIdempotence(dfWithProv)
      }
    }

    it("should add a provenance column to a dataframe with a provided expression") {
      val df = toyDf
      val providedProvenance = col("B")

      val dfWithProv = addProvenance(df, providedProvenance)
      val expected = df.withColumn(defaultProvenanceColName, col("B"))

      assertSmallDataFrameEquality(dfWithProv, expected)
    }

    it("should replace an existing provenance column on a dataframe when a provided expression is used") {
      val df = toyDf
      val withInitialProv = addProvenance(df, col("B"))

      val updated = addProvenance(withInitialProv, col("C"))
      val expected = df.withColumn(defaultProvenanceColName, col("C"))

      assertSmallDataFrameEquality(updated, expected)
    }


    it("should add a provenance column to a view if not already present") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)

      // Add provenance to view
      addProvenance(spark, viewName)

      // Perform checks
      assertProvenanceColumnAndDataPreserved(df, defaultProvenanceColName, spark.table(viewName))
      assertViewProvenanceIdempotence(viewName)
    }

    it("should add a provenance column to a view if not already present with custom column name") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)

      withSparkConf(spark, provenanceColConfKey, customProvColName) {
        // Add provenance to dataframe with custom column name
        addProvenance(spark, viewName)

        // Perform checks
        assertProvenanceColumnAndDataPreserved(df, customProvColName, spark.table(viewName))
        assertViewProvenanceIdempotence(viewName)
      }
    }

    it("should add a provenance column to a view with a provided expression") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)

      addProvenance(spark, viewName, col("B"))

      val expected = df.withColumn(defaultProvenanceColName, col("B"))
      assertSmallDataFrameEquality(spark.table(viewName), expected)
    }

    it("should replace an existing provenance column on a view when a provided expression is used") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)

      addProvenance(spark, viewName, col("B"))
      addProvenance(spark, viewName, col("C"))

      val expected = df.withColumn(defaultProvenanceColName, col("C"))
      assertSmallDataFrameEquality(spark.table(viewName), expected)
    }

  }
}