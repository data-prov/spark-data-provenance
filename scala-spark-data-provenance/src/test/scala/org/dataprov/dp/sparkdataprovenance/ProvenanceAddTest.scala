package org.dataprov.dp

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.dataprov.dp.sparkdataprovenance.DataFrameProvenanceTransformations._

class ProvenanceAddTest extends AnyFunSpec with Matchers with SparkSessionTestWrapper {
  
  import spark.implicits._

  describe("Provenance Addition") {
    it("should add a unique provenance column to each row") {
      // GIVEN: A dataframe with some duplicate rows to test that provenance is added even when data is identical
      val data = Seq(
      ("a", "b", "c"),
      ("a", "b", "c"),
      ("d", "b", "e")
      )
      
      val df = data.toDF("A", "B", "C")

      // WHEN: We call the addProvenanceColumn function to add provenance to the DataFrame
      val result = addProvenanceColumn(df)

      // THEN:
      // 1. The provenance column should be added
      assert(result.columns.contains(defaultProvenanceColName))

      // 2. The number of rows should be the same
      val collected = result.collect()
      assert(collected.length == 3)

      // 3. The UUIDs should be unique (a fundamental property of provenance)
      val uniqueUUIDs = result.select(defaultProvenanceColName).distinct().count()
      assert(uniqueUUIDs == 3)

      // 4. The original data should be preserved
      val resultData = collected.map(r => (r.getString(0), r.getString(1), r.getString(2)))
      assert(resultData.toSet == data.toSet)
    }

    it("should not add a provenance column if it already exists") {
      // GIVEN: A dataframe that already has a provenance column
      val data = Seq(
        ("a", "b", "c", "uuid1"),
        ("a", "b", "c", "uuid2"),
        ("d", "b", "e", "uuid3")
      )

      val df = data.toDF("A", "B", "C", defaultProvenanceColName)

      // WHEN: We call the addProvenanceColumn function to add provenance to the DataFrame
      val result = addProvenanceColumn(df)

      // THEN:
      // 1. The schema should be unchanged (no new column added)
      assert(result.schema == df.schema)

      // 2. The number of rows should be the same
      val collected = result.collect()
      assert(collected.length == 3)

      // 3. The original data (including provenance) should be preserved
      val resultData = collected.map(r => (r.getString(0), r.getString(1), r.getString(2), r.getString(3)))
      assert(resultData.toSet == data.toSet)
    }

    it("should add a provenance column to a view if it does not already exist") {
      // GIVEN: A dataframe without a provenance column and a temporary view
      val data = Seq(
        ("a", "b", "c"),
        ("a", "b", "c"),
        ("d", "b", "e")
      )

      val df = data.toDF("A", "B", "C")
      df.createOrReplaceTempView("test_view")

      // WHEN: We call the addProvenanceColumn function to add provenance to the view
      addProvenanceColumn(spark, "test_view")

      // THEN:
      // 1. The view should now have the provenance column
      val resultSchema = spark.table("test_view").schema
      assert(resultSchema.fieldNames.contains(defaultProvenanceColName))

      // 2. The number of rows should be the same
      val collected = spark.table("test_view").collect()
      assert(collected.length == 3)

      // 3. The original data (except provenance) should be preserved
      val resultData = collected.map(r => (r.getString(0), r.getString(1), r.getString(2)))
      val expectedData = data.map { case (a, b, c) => (a, b, c) }
      assert(resultData.toSet == expectedData.toSet)
    }
    
    it("should not change the view if the provenance column already exists") {
      // GIVEN: A dataframe with a provenance column and a temporary view
      val data = Seq(
        ("a", "b", "c", "uuid1"),
        ("a", "b", "c", "uuid2"),
        ("d", "b", "e", "uuid3")
      )

      val df = data.toDF("A", "B", "C", defaultProvenanceColName)
      df.createOrReplaceTempView("test_view")

      // WHEN: We call the addProvenanceColumn function to add provenance to the view
      addProvenanceColumn(spark, "test_view")

      // THEN:
      // 1. The schema should be unchanged (no new column added)
      val resultSchema = spark.table("test_view").schema
      assert(resultSchema.fieldNames.count(_ == defaultProvenanceColName) == 1)

      // 2. The number of rows should be the same
      val collected = spark.table("test_view").collect()
      assert(collected.length == 3)

      // 3. The original data (including provenance) should be preserved
      val resultData = collected.map(r => (r.getString(0), r.getString(1), r.getString(2), r.getString(3)))
      assert(resultData.toSet == data.toSet)
    }
  }
}