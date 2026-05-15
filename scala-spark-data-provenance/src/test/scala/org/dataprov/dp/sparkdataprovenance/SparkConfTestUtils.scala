package org.dataprov.dp

import org.apache.spark.sql.SparkSession

trait SparkConfTestUtils {

  protected def withSparkConf[T](spark: SparkSession, key: String, value: String)(testBody: => T): T = {
    val previousValue = spark.conf.getOption(key)
    spark.conf.set(key, value)

    try {
      testBody
    } finally {
      previousValue match {
        case Some(previous) => spark.conf.set(key, previous)
        case None => spark.conf.unset(key)
      }
    }
  }
}
