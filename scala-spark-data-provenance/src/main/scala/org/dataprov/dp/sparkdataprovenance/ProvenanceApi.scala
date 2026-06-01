package org.dataprov.dp.sparkdataprovenance

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.uuid
import org.dataprov.dp.sparkdataprovenance.SparkConfUtils._

object ProvenanceApi {
  val provenanceEnabledSparkConf = "spark.provenance.enabled"
  val provenanceColNameSparkConf = "spark.provenance.columnName"
  val defaultProvenanceColName = "_provenance_tag"

  def isProvenanceEnabled(spark: SparkSession): Boolean = {
    spark.conf.get(provenanceEnabledSparkConf, "true").toBoolean
  }
  def provenanceColumnName(spark: SparkSession): String = {
    spark.conf.get(provenanceColNameSparkConf, defaultProvenanceColName)
  }
  def defaultProvenanceColumn: Column = uuid()

  /** Adds the configured provenance column to a DataFrame when not already
    * present.
    *
    * If the column already exists, the original DataFrame is returned
    * unchanged.
    */
  def addProvenance(df: DataFrame): DataFrame = {
    addProvenance(df, None)
  }

  /** Adds (or replaces) the configured provenance column on a DataFrame with
    * the provided column expression.
    */
  def addProvenance(df: DataFrame, col: Column): DataFrame = {
    addProvenance(df, Some(col))
  }

  private def addProvenance(df: DataFrame, col: Option[Column]): DataFrame = {
    val colName = provenanceColumnName(df.sparkSession)
    if (df.columns.contains(colName) && col.isEmpty) {
      df
    } else {
      df.withColumn(colName, col.getOrElse(defaultProvenanceColumn))
    }
    // FIXME: if col references a column that is absent from df, the provenance column won't be added
  }

  /** Adds the configured provenance column to a temp view when not already
    * present.
    *
    * If the column already exists, the temp view is left unchanged. Returns the
    * provided view name to support call chaining.
    *
    * Returns the provided view name to support call chaining.
    */
  def addProvenance(spark: SparkSession, view: String): String = {
    addProvenance(spark, view, None)
  }

  /** Adds (or replaces) the configured provenance column on a temp view with
    * the provided column expression.
    *
    * Returns the provided view name to support call chaining.
    */
  def addProvenance(spark: SparkSession, view: String, col: Column): String = {
    addProvenance(spark, view, Some(col))
  }

  private def addProvenance(
      spark: SparkSession,
      view: String,
      col: Option[Column]
  ): String = {
    val colName = provenanceColumnName(spark)
    val df = spark.table(view)
    if (!df.columns.contains(colName) || col.isDefined) {
      df.withColumn(colName, col.getOrElse(defaultProvenanceColumn))
        .createOrReplaceTempView(view)
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
    * view.https://provsql.org/docs/user/tutorial.html#step-5-display-provenance-formulas
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
      ProvenanceApi.addProvenance(df)
    def addProvenanceColumn(col: Column): DataFrame =
      ProvenanceApi.addProvenance(df, col)
    def removeProvenanceColumn: DataFrame =
      ProvenanceApi.removeProvenance(df)
  }

  def withProvenanceEnabled[T](spark: SparkSession)(testBody: => T): T =
    withSparkConf(spark, provenanceEnabledSparkConf, "true")(testBody)

  def withProvenanceEnabled[T](spark: SparkSession, provenanceColName: String)(
      testBody: => T
  ): T =
    withProvenanceEnabled(spark)(
      withSparkConf(spark, provenanceColNameSparkConf, provenanceColName)(
        testBody
      )
    )

  def withProvenanceDisabled[T](spark: SparkSession)(testBody: => T): T =
    withSparkConf(spark, provenanceEnabledSparkConf, "false")(testBody)

  def withProvenanceDisabled[T](spark: SparkSession, provenanceColName: String)(
      testBody: => T
  ): T =
    withProvenanceDisabled(spark)(
      withSparkConf(spark, provenanceColNameSparkConf, provenanceColName)(
        testBody
      )
    )
}
