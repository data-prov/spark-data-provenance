package org.dataprov.dp

import org.apache.spark.sql.SparkSessionExtensions

// This extension injects the LogicalPlanWithProvenance rule into the SparkSession's analysis phase,
// which adds provenance attributes to the logical plan and rewrites operators to propagate provenance information.
object ProvenanceExtension {
  def register(
      // Allow users to optionally provide custom provenance operators and builders when registering the extension
      provenanceBuilder: ProvenanceBuilder = DisplayStringProvenanceBuilder
  ): SparkSessionExtensions => Unit = { extensions =>
    new ProvenanceExtension(provenanceBuilder).apply(extensions)
  }
}

// This class acts as the registration hook
class ProvenanceExtension(
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
