package org.dataprov.dp

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}
import org.dataprov.dp.ProvenanceExtension

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    Logger.getLogger("org").setLevel(Level.OFF)
    SparkSession.builder().master("local").appName("spark session").withExtensions(new ProvenanceExtension()(_)).getOrCreate()
  }

}

// TODO: to be removed
