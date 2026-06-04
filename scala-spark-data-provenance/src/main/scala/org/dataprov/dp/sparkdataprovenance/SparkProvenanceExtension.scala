package org.dataprov.dp.sparkdataprovenance

import org.apache.spark.sql.SparkSessionExtensions

// This extension injects the LogicalPlanWithProvenance rule into the SparkSession's analysis phase,
// which adds provenance attributes to the logical plan and rewrites operators to propagate provenance information.
object SparkProvenanceExtension {
  val provenanceBuilderConf = "spark.provenance.builder"

  private def fromName(name: String): Option[ProvenanceBuilder] =
    name.trim.toLowerCase match {
      case "display" | "displaystring" | "displaystringprovenancebuilder" =>
        Some(DisplayStringProvenanceBuilder)
      case "boolean" | "booleanprovenancebuilder" =>
        Some(BooleanProvenanceBuilder)
      case "semiwhy" | "semi_why" | "semi-why" | "semiwhyprovenancebuilder" =>
        Some(SemiWhyProvenanceBuilder)
      case "fullwhy" | "full_why" | "full-why" | "fullwhyprovenancebuilder" =>
        Some(FullWhyProvenanceBuilder)
      case "lightwhy" | "light_why" | "light-why" | "lightwhyprovenancebuilder" =>
        Some(LightWhyProvenanceBuilder)
      case _ => None
    }

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
      val configuredBuilderName =
        session.sessionState.conf.getConfString(
          SparkProvenanceExtension.provenanceBuilderConf,
          ""
        )
      val effectiveBuilder =
        SparkProvenanceExtension
          .fromName(configuredBuilderName)
          .getOrElse(provenanceBuilder)

      LogicalPlanWithProvenance(
        session,
        effectiveBuilder
      )
    }
  }
}
