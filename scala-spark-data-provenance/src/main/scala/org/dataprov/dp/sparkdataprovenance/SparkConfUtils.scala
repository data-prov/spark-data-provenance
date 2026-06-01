package org.dataprov.dp

import org.apache.spark.sql.SparkSession
import org.dataprov.dp.sparkdataprovenance.DataFrameProvenanceTransformations._

object SparkConfUtils {
  def withSparkConf[T](spark: SparkSession, key: String, value: String)(
      testBody: => T
  ): T = {
    val previousValue = spark.conf.getOption(key)
    spark.conf.set(key, value)

    try {
      testBody
    } finally {
      previousValue match {
        case Some(previous) => spark.conf.set(key, previous)
        case None           => spark.conf.unset(key)
      }
    }
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
