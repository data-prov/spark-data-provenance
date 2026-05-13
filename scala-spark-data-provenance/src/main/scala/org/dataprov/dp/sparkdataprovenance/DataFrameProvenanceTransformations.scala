package org.dataprov.dp.sparkdataprovenance

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.monotonically_increasing_id

object DataFrameProvenanceTransformations {
  val provenanceEnabledConf = "spark.provenance.enabled"
  val provenanceColConfKey = "spark.provenance.columnName"
  val defaultProvenanceColName = "_provenance_tag"

  def provenanceColumnName(spark: SparkSession): String = {
    spark.conf.get(provenanceColConfKey, defaultProvenanceColName)
  }
  def provenanceColumn: Column = monotonically_increasing_id()

  /** Adds the configured provenance column to a DataFrame only when it is not
    * already present.
    *
    * If the column already exists, the original DataFrame is returned
    * unchanged.
    */
  def addProvenance(df: DataFrame): DataFrame = {
    val colName = provenanceColumnName(df.sparkSession)
    if (df.columns.contains(colName)) {
      df
    } else {
      df.withColumn(colName, provenanceColumn)
    }
  }

  /** Adds the configured provenance column to a temp view only when it is not
    * already present.
    *
    * If the column already exists, the temp view is left unchanged. Returns the
    * provided view name to support call chaining.
    */
  def addProvenance(spark: SparkSession, view: String): String = {
    val colName = provenanceColumnName(spark)
    val df = spark.table(view)
    if (!df.columns.contains(colName)) {
      df.withColumn(colName, provenanceColumn).createOrReplaceTempView(view)
    }
    view
  }

  /** Removes the configured provenance column from a DataFrame.
    *
    * If the column does not exist, Spark leaves the DataFrame unchanged.
    */
  def removeProvenance(df: DataFrame): DataFrame = {
    val colName = provenanceColumnName(df.sparkSession)
    df.drop(colName)
  }

  /** Removes the configured provenance column from a temp view and replaces the
    * view.
    *
    * If the column does not exist, the resulting view schema is unchanged.
    * Returns the provided view name to support call chaining.
    */
  def removeProvenance(spark: SparkSession, view: String): String = {
    val colName = provenanceColumnName(spark)
    spark.table(view).drop(colName).createOrReplaceTempView(view)
    view
  }

  implicit class DataFrameWithProvenance(df: DataFrame) {
    def addProvenanceColumn: DataFrame =
      DataFrameProvenanceTransformations.addProvenance(df)
    def removeProvenanceColumn: DataFrame =
      DataFrameProvenanceTransformations.removeProvenance(df)
  }
}
