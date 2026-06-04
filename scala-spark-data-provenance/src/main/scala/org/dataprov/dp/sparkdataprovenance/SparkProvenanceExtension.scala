package org.dataprov.dp.sparkdataprovenance

import org.apache.spark.sql.SparkSessionExtensions

object SparkProvenanceExtension {
  def register(
      // Allow users to optionally provide custom provenance operators and builders when registering the extension
      provenanceBuilder: ProvenanceBuilder = DisplayStringProvenanceBuilder
  ): SparkSessionExtensions => Unit = { extensions =>
    new SparkProvenanceExtension(provenanceBuilder).apply(extensions)
  }
}

// This class acts as the registration hook
class SparkProvenanceExtension(
    val provenanceBuilder: ProvenanceBuilder
) extends (SparkSessionExtensions => Unit) {

  // Keep a zero-argument constructor so Spark can instantiate the extension via reflection.
  def this() = this(DisplayStringProvenanceBuilder)

  override def apply(extensions: SparkSessionExtensions): Unit = {
    // Inject provenance rule after analysis so projections created by
    // withColumn (e.g. uuid) are fully resolved.
    extensions.injectPostHocResolutionRule { session =>
      LogicalPlanWithProvenance(
        session,
        provenanceBuilder
      )
    }
  }
}
