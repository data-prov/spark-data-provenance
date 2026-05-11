package org.dataprov.dp

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.dataprov.dp.sparkdataprovenance.DataFrameProvenanceTransformations._

class ProvenanceRemoveTest extends AnyFunSpec with Matchers with SparkSessionTestWrapper {

  import spark.implicits._

  describe("Provenance Removal") {
    it("should remove the provenance column from each row") {
      // GIVEN: A dataframe with a provenance column
      val data = Seq(
        ("a", "b", "c", "uuid1"),
        ("a", "b", "c", "uuid2"),
        ("d", "b", "e", "uuid3")
      )

      val df = data.toDF("A", "B", "C", "_provenance_tag")

      // WHEN: We call the removeProvenanceColumn function to remove provenance from the DataFrame
      val result = removeProvenanceColumn(df)

      // THEN:
      // 1. The provenance column should be removed
      assert(result.columns.contains("_provenance_tag") == false)

      // 2. The number of rows should be the same
      val collected = result.collect()
      assert(collected.length == 3)

      // 3. The original data (except provenance) should be preserved
      val resultData = collected.map(r => (r.getString(0), r.getString(1), r.getString(2)))
      val expectedData = data.map { case (a, b, c, _) => (a, b, c) }
      assert(resultData.toSet == expectedData.toSet)

    }

    it ("should not change the DataFrame if the provenance column does not exist") {
      // GIVEN: A dataframe without a provenance column
      val data = Seq(
        ("a", "b", "c"),
        ("a", "b", "c"),
        ("d", "b", "e")
      )

      val df = data.toDF("A", "B", "C")

      // WHEN: We call the removeProvenanceColumn function to remove provenance from the DataFrame
      val result = removeProvenanceColumn(df)

      // THEN:
      // 1. The schema should be unchanged
      assert(result.schema == df.schema)

      // 2. The number of rows should be the same
      val collected = result.collect()
      assert(collected.length == 3)

      // 3. The original data should be preserved
      val resultData = collected.map(r => (r.getString(0), r.getString(1), r.getString(2)))
      assert(resultData.toSet == data.toSet)
    }

    it ("should remove the provenance column from a view") {
      // GIVEN: A dataframe with a provenance column and a temporary view
      val data = Seq(
        ("a", "b", "c", "uuid1"),
        ("a", "b", "c", "uuid2"),
        ("d", "b", "e", "uuid3")
      )

      val df = data.toDF("A", "B", "C", "_provenance_tag")
      df.createOrReplaceTempView("test_view")

      // WHEN: We call the removeProvenanceColumn function to remove provenance from the view
      removeProvenanceColumn(spark, "test_view")

      // THEN:
      // 1. The provenance column should be removed from the view
      val viewDF = spark.table("test_view")
      assert(viewDF.columns.contains("_provenance_tag") == false)

      // 2. The number of rows should be the same
      val collected = viewDF.collect()
      assert(collected.length == 3)

      // 3. The original data (except provenance) should be preserved in the view
      val resultData = collected.map(r => (r.getString(0), r.getString(1), r.getString(2)))
      val expectedData = data.map { case (a, b, c, _) => (a, b, c) }
      assert(resultData.toSet == expectedData.toSet)
    }

    it ("should not change the view if the provenance column does not exist") {
      // GIVEN: A dataframe without a provenance column and a temporary view
      val data = Seq(
        ("a", "b", "c"),
        ("a", "b", "c"),
        ("d", "b", "e")
      )

      val df = data.toDF("A", "B", "C")
      df.createOrReplaceTempView("test_view_no_prov")

      // WHEN: We call the removeProvenanceColumn function to remove provenance from the view
      removeProvenanceColumn(spark, "test_view_no_prov")

      // THEN:
      // 1. The schema of the view should be unchanged
      val viewDF = spark.table("test_view_no_prov")
      assert(viewDF.schema == df.schema)

      // 2. The number of rows should be the same
      val collected = viewDF.collect()
      assert(collected.length == 3)

      // 3. The original data should be preserved in the view
      val resultData = collected.map(r => (r.getString(0), r.getString(1), r.getString(2)))
      assert(resultData.toSet == data.toSet)
    }
  }
}