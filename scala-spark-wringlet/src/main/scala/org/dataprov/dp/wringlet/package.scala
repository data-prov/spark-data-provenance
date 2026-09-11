package org.dataprov.dp

import org.apache.spark.sql.DataFrame

import scala.jdk.CollectionConverters._

package object wringlet {

  // Implicit class to extend DataFrame with provenance-related methods
  // This allows us to call getMinimalSources directly on a DataFrame instance
  implicit class DataFrameProvenanceExtensions(df: DataFrame) {

    def getMinimalSources(
        sourceDFs: Seq[DataFrame],
        provenanceColName: String = "_provenance_tag"
    ): Seq[DataFrame] = {
      val javaSources = sourceDFs.asJava
      ProvenanceExtractor
        .getMinimalSources(df, javaSources, provenanceColName)
        .asScala
        .toSeq
    }
  }
}
