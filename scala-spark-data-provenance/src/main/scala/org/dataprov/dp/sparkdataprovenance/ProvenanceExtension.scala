package org.dataprov.dp

import org.apache.spark.sql.SparkSessionExtensions


// This class acts as the registration hook
class ProvenanceExtension extends (SparkSessionExtensions => Unit) {
  
  override def apply(extensions: SparkSessionExtensions): Unit = {
    // Inject our custom rule into the Resolution (Analyzer) phase
    extensions.injectResolutionRule { session =>
      LogicalPlanWithProvenance(session)
    }
  }
}
