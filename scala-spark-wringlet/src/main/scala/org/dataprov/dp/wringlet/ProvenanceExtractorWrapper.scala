package org.dataprov.dp.wringlet

import org.apache.spark.sql.DataFrame

class ProvenanceExtractorWrapper {
  // This method is a wrapper that allows us to call the getMinimalSources method from the ProvenanceExtractor object
  def getMinimalSources(
      finalDF: DataFrame,
      sourceDFs: java.util.List[DataFrame],
      provenanceColName: String
  ): java.util.List[DataFrame] = {
    ProvenanceExtractor.getMinimalSources(finalDF, sourceDFs, provenanceColName)
  }
}
