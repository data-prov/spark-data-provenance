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

  def addProvenanceColumn(df: DataFrame): DataFrame = {
    val colName = provenanceColumnName(df.sparkSession)
    df.withColumn(colName, provenanceColumn)
  }

  def addProvenanceColumn(spark: SparkSession, dfName: String): String = {
    val colName = provenanceColumnName(spark)
    spark
      .table(dfName)
      .withColumn(colName, provenanceColumn)
      .createOrReplaceTempView(dfName)
    dfName
  }

  def removeProvenanceColumn(df: DataFrame): DataFrame = {
    val colName = provenanceColumnName(df.sparkSession)
    df.drop(colName)
  }

  def removeProvenanceColumn(spark: SparkSession, dfName: String): String = {
    val colName = provenanceColumnName(spark)
    spark.table(dfName).drop(colName).createOrReplaceTempView(dfName)
    dfName
  }

  implicit class DataFrameWithProvenance(df: DataFrame) {
    def addProvenanceColumn: DataFrame =
      DataFrameProvenanceTransformations.addProvenanceColumn(df)
    def removeProvenanceColumn: DataFrame =
      DataFrameProvenanceTransformations.removeProvenanceColumn(df)
  }
}
