package org.dataprov.dp

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat, lit, when}

import org.dataprov.dp.sparkdataprovenance.DataFrameProvenanceTransformations._

class ProvenanceJoinTest extends AnyFunSpec with Matchers with SparkSessionTestWrapper with DataFrameComparer with SparkConfTestUtils {
  import spark.implicits._
  spark.conf.set("spark.provenance.enabled", "true")

  private def toyDfLeft: DataFrame = Seq(
    ("a", 1, 2.3),
    ("a", 1, 2.3),
    ("d", 2, 3.4),
    ("f", 3, 4.5)
  ).toDF("A", "B", "C")

  private def toyDfRight: DataFrame = Seq(
    ("a", "x"),
    ("a", "x"),
    ("d", "y"),
    ("e", "z")
  ).toDF("A", "D")

  private def assertProvenanceColumnAndDataPreserved(dfExpected: DataFrame, provColName: String, dfWithProv: DataFrame): Unit = {
    // 1. The provenance column should be added
    assert(dfWithProv.columns.contains(provColName))
    // 2. The expected dataframe (including the provenance column) should be equal to the actual dataframe with provenance
    assertSmallDataFrameEquality(dfWithProv, dfExpected)

  }

  describe("Joining columns from DataFrames/views with provenance") {
    it("should preserve the provenance column and its values when performing an inner join") {      
      val dfLeft = toyDfLeft
      val dfRight = toyDfRight

      val dfWithProvLeft = addProvenance(dfLeft, col("B"))
      val dfWithProvRight = addProvenance(dfRight, col("D"))
      val dfWithProvJoin = dfWithProvLeft.join(dfWithProvRight, Seq("A"), "inner").select("A", "B", "C", "D")

      val dfLeftSide = dfLeft.select("A", "B", "C").withColumn("leftProv", col("B").cast("string"))
      val dfRightSide = dfRight.select("A", "D").withColumn("rightProv", col("D").cast("string"))
      
      val expected: DataFrame = dfLeftSide.join(dfRightSide, Seq("A"), "inner")
        .withColumn(defaultProvenanceColName, concat(lit("("), col("leftProv"), lit(" ⊗ "), col("rightProv"), lit(")")))
        .drop("leftProv", "rightProv")
        .select("A", "B", "C", "D", defaultProvenanceColName)
        .orderBy("A", "B", "C", "D") 

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvJoin)
    }

    it("should preserve the provenance column and its values when performing an inner join with views") {
      val dfLeft = toyDfLeft
      val dfRight = toyDfRight

      dfLeft.createOrReplaceTempView("left_view")
      dfRight.createOrReplaceTempView("right_view")
      addProvenance(spark, "left_view", col("B"))
      addProvenance(spark, "right_view", col("D"))

      val dfWithProvJoin = spark.sql(
        s"""
           SELECT l.A, l.B, l.C, r.D
           FROM left_view l
           INNER JOIN right_view r ON l.A = r.A
        """)

      val dfLeftSide = dfLeft.select("A", "B", "C").withColumn("leftProv", col("B").cast("string"))
      val dfRightSide = dfRight.select("A", "D").withColumn("rightProv", col("D").cast("string"))
      
      val expected: DataFrame = dfLeftSide.join(dfRightSide, Seq("A"), "inner")
        .withColumn(defaultProvenanceColName, concat(lit("("), col("leftProv"), lit(" ⊗ "), col("rightProv"), lit(")")))
        .drop("leftProv", "rightProv")
        .select("A", "B", "C", "D", defaultProvenanceColName)
        .orderBy("A", "B", "C", "D") 

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvJoin)
    }

    it("should preserve the provenance column and its values when performing a left join") {
      val dfLeft = toyDfLeft
      val dfRight = toyDfRight

      val dfWithProvLeft = addProvenance(dfLeft, col("B"))
      val dfWithProvRight = addProvenance(dfRight, col("D"))
      val dfWithProvJoin = dfWithProvLeft.join(dfWithProvRight, Seq("A"), "left").select("A", "B", "C", "D")

      val dfLeftSide = dfLeft.select("A", "B", "C").withColumn("leftProv", col("B").cast("string"))
      val dfRightSide = dfRight.select("A", "D").withColumn("rightProv", col("D").cast("string"))
      
      val expected: DataFrame = dfLeftSide.join(dfRightSide, Seq("A"), "left")
        .withColumn(defaultProvenanceColName, when(col("rightProv").isNotNull, concat(lit("("), col("leftProv"), lit(" ⊗ "), col("rightProv"), lit(")"))).otherwise(col("leftProv")))
        .drop("leftProv", "rightProv")
        .select("A", "B", "C", "D", defaultProvenanceColName)
        .orderBy("A", "B", "C", "D") 

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvJoin)
    }

    it("should preserve the provenance column and its values when performing a left join with views") {
      val dfLeft = toyDfLeft
      val dfRight = toyDfRight

      dfLeft.createOrReplaceTempView("left_view")
      dfRight.createOrReplaceTempView("right_view")
      addProvenance(spark, "left_view", col("B"))
      addProvenance(spark, "right_view", col("D"))

      val dfWithProvJoin = spark.sql(
        s"""
           SELECT l.A, l.B, l.C, r.D
           FROM left_view l
           LEFT JOIN right_view r ON l.A = r.A
        """)

      val dfLeftSide = dfLeft.select("A", "B", "C").withColumn("leftProv", col("B").cast("string"))
      val dfRightSide = dfRight.select("A", "D").withColumn("rightProv", col("D").cast("string"))
      
      val expected: DataFrame = dfLeftSide.join(dfRightSide, Seq("A"), "left")
        .withColumn(defaultProvenanceColName, when(col("rightProv").isNotNull, concat(lit("("), col("leftProv"), lit(" ⊗ "), col("rightProv"), lit(")"))).otherwise(col("leftProv")))
        .drop("leftProv", "rightProv")
        .select("A", "B", "C", "D", defaultProvenanceColName)
        .orderBy("A", "B", "C", "D") 

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvJoin)
    }

    it("should preserve the provenance column and its values when performing a right join") {
      val dfLeft = toyDfLeft
      val dfRight = toyDfRight

      val dfWithProvLeft = addProvenance(dfLeft, col("B"))
      val dfWithProvRight = addProvenance(dfRight, col("D"))
      val dfWithProvJoin = dfWithProvLeft.join(dfWithProvRight, Seq("A"), "right").select("A", "B", "C", "D")

      val dfLeftSide = dfLeft.select("A", "B", "C").withColumn("leftProv", col("B").cast("string"))
      val dfRightSide = dfRight.select("A", "D").withColumn("rightProv", col("D").cast("string"))
      
      val expected: DataFrame = dfLeftSide.join(dfRightSide, Seq("A"), "right")
        .withColumn(defaultProvenanceColName, when(col("leftProv").isNotNull, concat(lit("("), col("leftProv"), lit(" ⊗ "), col("rightProv"), lit(")"))).otherwise(col("rightProv")))
        .drop("leftProv", "rightProv")
        .select("A", "B", "C", "D", defaultProvenanceColName)
        .orderBy("A", "B", "C", "D") 

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvJoin)
    }

    it("should preserve the provenance column and its values when performing a right join with views") {
      val dfLeft = toyDfLeft
      val dfRight = toyDfRight

      dfLeft.createOrReplaceTempView("left_view")
      dfRight.createOrReplaceTempView("right_view")
      addProvenance(spark, "left_view", col("B"))
      addProvenance(spark, "right_view", col("D"))

      val dfWithProvJoin = spark.sql(
        s"""
           SELECT r.A, l.B, l.C, r.D
           FROM left_view l
           RIGHT JOIN right_view r ON l.A = r.A
        """)

      val dfLeftSide = dfLeft.select("A", "B", "C").withColumn("leftProv", col("B").cast("string"))
      val dfRightSide = dfRight.select("A", "D").withColumn("rightProv", col("D").cast("string"))
      
      val expected: DataFrame = dfLeftSide.join(dfRightSide, Seq("A"), "right")
        .withColumn(defaultProvenanceColName, when(col("leftProv").isNotNull, concat(lit("("), col("leftProv"), lit(" ⊗ "), col("rightProv"), lit(")"))).otherwise(col("rightProv")))
        .drop("leftProv", "rightProv")
        .select("A", "B", "C", "D", defaultProvenanceColName)
        .orderBy("A", "B", "C", "D") 

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvJoin)
    }

    it("should preserve the provenance column and its values when performing a full outer join") {
      val dfLeft = toyDfLeft
      val dfRight = toyDfRight

      val dfWithProvLeft = addProvenance(dfLeft, col("B"))
      val dfWithProvRight = addProvenance(dfRight, col("D"))
      val dfWithProvJoin = dfWithProvLeft.join(dfWithProvRight, Seq("A"), "full_outer").select("A", "B", "C", "D")

      val dfLeftSide = dfLeft.select("A", "B", "C").withColumn("leftProv", col("B").cast("string"))
      val dfRightSide = dfRight.select("A", "D").withColumn("rightProv", col("D").cast("string"))
      
      val expected: DataFrame = dfLeftSide.join(dfRightSide, Seq("A"), "full_outer")
        .withColumn(defaultProvenanceColName, when(col("leftProv").isNotNull && col("rightProv").isNotNull, concat(lit("("), col("leftProv"), lit(" ⊗ "), col("rightProv"), lit(")")))
          .when(col("leftProv").isNotNull, col("leftProv"))
          .when(col("rightProv").isNotNull, col("rightProv"))
          .otherwise(lit(null)))
        .drop("leftProv", "rightProv")
        .select("A", "B", "C", "D", defaultProvenanceColName)

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvJoin)
    }

    it("should preserve the provenance column and its values when performing a full outer join with views") {
      val dfLeft = toyDfLeft
      val dfRight = toyDfRight

      dfLeft.createOrReplaceTempView("left_view")
      dfRight.createOrReplaceTempView("right_view")
      addProvenance(spark, "left_view", col("B"))
      addProvenance(spark, "right_view", col("D"))

      val dfWithProvJoin = spark.sql(
        s"""
           SELECT COALESCE(l.A, r.A) AS A, l.B, l.C, r.D
           FROM left_view l
           FULL OUTER JOIN right_view r ON l.A = r.A
        """)

      val dfLeftSide = dfLeft.select("A", "B", "C").withColumn("leftProv", col("B").cast("string"))
      val dfRightSide = dfRight.select("A", "D").withColumn("rightProv", col("D").cast("string"))

      val expected: DataFrame = dfLeftSide.join(dfRightSide, Seq("A"), "full_outer")
        .withColumn(defaultProvenanceColName, when(col("leftProv").isNotNull && col("rightProv").isNotNull, concat(lit("("), col("leftProv"), lit(" ⊗ "), col("rightProv"), lit(")")))
          .when(col("leftProv").isNotNull, col("leftProv"))
          .when(col("rightProv").isNotNull, col("rightProv"))
          .otherwise(lit(null)))
        .drop("leftProv", "rightProv")
        .select("A", "B", "C", "D", defaultProvenanceColName)

        assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvJoin)
    }
  }
}


