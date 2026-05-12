package org.dataprov.dp

import org.apache.spark.sql.SparkSessionExtensions

// This class acts as the registration hook
class ProvenanceExtension extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    // Inject provenance rule after analysis so projections created by
    // withColumn (e.g. monotonically_increasing_id) are fully resolved.
    extensions.injectPostHocResolutionRule { session =>
      LogicalPlanWithProvenance(session)
    }
  }
}