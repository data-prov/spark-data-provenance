package org.dataprov.dp

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}
import org.dataprov.dp.ProvenanceExtension

trait SparkSessionTestWrapper {
  // Helper method to create a SparkSession with the ProvenanceExtension registered, 
  //and optionally with custom operators and builders.
  protected def sparkWithProvenance(
      provenanceBuilder: ProvenanceBuilder = DisplayStringProvenanceBuilder,
      appName: String = "spark session"
  ): SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName(appName)
      .withExtensions(
        ProvenanceExtension.register(provenanceBuilder)
      )
      .getOrCreate()
  }
  // Create a SparkSession with the ProvenanceExtension registered, and set log level to ERROR to reduce noise in test output
  lazy val spark: SparkSession = {
    Logger.getLogger("org").setLevel(Level.OFF)
    sparkWithProvenance()
  }

}

// TODO: to be removed
