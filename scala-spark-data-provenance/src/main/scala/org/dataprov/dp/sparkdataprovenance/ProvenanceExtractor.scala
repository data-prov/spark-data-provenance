package org.dataprov.dp.sparkdataprovenance

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.ArrayType

object ProvenanceExtractor {

  /**
   * Extract the minimal datasets from the source DataFrames that are necessary to reproduce the final DataFrame.
   * This is useful for debugging and understanding the lineage of specific rows.
   *
   * @param finalDF The final tagged DataFrame from the pipeline
   * @param sourceDFs The list of original source DataFrames (must have their ID/UUID column)
   * @param provenanceColName The name of the provenance column (e.g., "_provenance_tag")
   * @return A list of DataFrames containing a set of rows necessary to reproduce the final DataFrame
   */
  def extractMinimalDatasets(
      finalDF: DataFrame,
      sourceDFs: Seq[DataFrame],
      provenanceColName: String = "_provenance_tag"
  ): Seq[DataFrame] = {
    
    val spark = finalDF.sparkSession
    import spark.implicits._

    // 1.Determine the schema of the provenance column to understand its structure (Array, String, etc.)
    val provField = finalDF.schema(provenanceColName)
    
    // 2. Extract and flatten all unique tags present in the final result
    val targetTagsDF = finalDF.select(col(provenanceColName))

    val flattenedTagsDF = provField.dataType match {
      // Semi-Why case: Array[Tag] (e.g., Array[String] or Array[Long])
      case _: ArrayType =>
        targetTagsDF.select(explode(col(provenanceColName)).as("source_tag"))
      
      // Boolean / Display / Simple Id case: Direct scalar value
      case _ =>
        targetTagsDF.withColumnRenamed(provenanceColName, "source_tag")
    }

    // We collect the unique tags into a Set for efficient filtering. 
    // This is especially useful when dealing with large datasets.
    // Ideal for debugging a few target rows
    val activeSourceTags = flattenedTagsDF
      .distinct()
      .as[String]
      .collect()
      .toSet

    // 3. Filter each source DataFrame to retain only the rows that match the active tags
    sourceDFs.map { sourceDF =>
      // We look for the column that serves as the identifier in the source.
      // In your framework, this is often the generated UUID or the original primary key.
      val idColumnName = if (sourceDF.columns.contains(provenanceColName)) provenanceColName else "id"
      
      // Safety in case the source has neither
      if (sourceDF.columns.contains(idColumnName)) {
        sourceDF.filter(col(idColumnName).isin(activeSourceTags.toSeq: _*))
      } else {
        // If no tracking column is found, return the original DF intact
        sourceDF
      }
    }
  }
}