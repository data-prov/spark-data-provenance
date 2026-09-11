package org.dataprov.dp.sparkdataprovenance

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types.ArrayType

import scala.jdk.CollectionConverters._

object ProvenanceExtractor {

  /** Extract the minimal datasets from the source DataFrames that are necessary
    * to reproduce the final DataFrame. This is useful for debugging and
    * understanding the lineage of specific rows.
    */
  def getMinimalSources(
      finalDF: DataFrame,
      sourceDFs: java.util.List[DataFrame],
      provenanceColName: String
  ): java.util.List[DataFrame] = {

    val sparkSession = finalDF.sparkSession
    val wasEnabled =
      sparkSession.conf.get("spark.provenance.enabled", "false") == "true"

    try {
      sparkSession.conf.set("spark.provenance.enabled", "false")

      // 1. Determine the schema of the provenance column to understand its structure (Array, String, etc.)
      val provField = finalDF.schema(provenanceColName)

      // 2. Extract and flatten all unique tags present in the final result
      val targetTagsDF = finalDF.select(col(provenanceColName))

      val finalTags = provField.dataType match {
        // Semi-Why case: Array[Tag] (e.g., Array[String] or Array[Long])
        case _: ArrayType =>
          targetTagsDF
            .select(explode(col(provenanceColName)).as(provenanceColName))
            .distinct()

        // Boolean / Display / Simple Id case: Direct scalar value
        case _ =>
          targetTagsDF.distinct()
      }

      // 3. Filter each source DataFrame to retain only the rows that match the active tags using Left Semi Join
      val filteredSources = sourceDFs.asScala.map { sourceDF =>
        if (sourceDF.columns.contains(provenanceColName)) {
          sourceDF.join(finalTags, Seq(provenanceColName), "left_semi")
        } else {
          sourceDF
        }
      }

      filteredSources.asJava

    } finally {
      if (wasEnabled) {
        sparkSession.conf.set("spark.provenance.enabled", "true")
      }
    }
  }
}
