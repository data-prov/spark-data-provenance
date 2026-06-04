package org.dataprov.dp.sparkdataprovenance

import org.dataprov.dp.sparkdataprovenance.ProvenanceApi._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.dataprov.dp.sparkdataprovenance.SparkSessionTestWrapper
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType

class ProvenanceComplexOperationsTest extends AnyFunSpec with Matchers with SparkSessionTestWrapper with DataFrameComparer with ProvenanceModeTestUtils {
  import spark.implicits._

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

  describe("Complex operations on DataFrames/views with provenance") {
    itWithProvenanceEnabled("should preserve the provenance column and its values when performing a cross join followed by a distinct") {
      val dfLeft = toyDfLeft
      val dfRight = toyDfRight

      val dfWithProvLeft = addProvenance(dfLeft, col("B"))
      val dfWithProvRight = addProvenance(dfRight, col("D"))

      val dfLeftSide = dfLeft.alias("l").select("l.A", "l.B", "l.C").withColumn("leftProv", col("l.B").cast("string"))
      val dfRightSide = dfRight.alias("r").select("r.A", "r.D").withColumn("rightProv", col("r.D").cast("string"))
      val dfWithProvJoin: DataFrame = dfWithProvLeft.alias("l").crossJoin(dfWithProvRight.alias("r")).select("l.A", "l.B", "l.C", "r.D").distinct().orderBy("l.A", "l.B", "l.C", "r.D")
      val expected: DataFrame = dfLeftSide.alias("l").crossJoin(dfRightSide.alias("r"))
        .withColumn(defaultProvenanceColName, concat(lit("("), col("leftProv"), lit(" ⊗ "), col("rightProv"), lit(")")).cast("string"))
        .drop("leftProv", "rightProv")
        .select("l.A", "l.B", "l.C", "r.D", defaultProvenanceColName)
        .distinct()
        .orderBy("l.A", "l.B", "l.C", "r.D")
       
      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvJoin)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when performing a cross join followed by a distinct with views") {
      val dfLeft = toyDfLeft
      val dfRight = toyDfRight

      dfLeft.createOrReplaceTempView("left_view")
      dfRight.createOrReplaceTempView("right_view")
      addProvenance(spark, "left_view", col("B"))
      addProvenance(spark, "right_view", col("D"))

      val dfWithProvJoin: DataFrame = spark.sql(
        s"""
           SELECT DISTINCT l.A, l.B, l.C, r.D
           FROM left_view l
           CROSS JOIN right_view r
           ORDER BY l.A, l.B, l.C, r.D
         """
      )

      val dfLeftSide = dfLeft.alias("l").select("l.A", "l.B", "l.C").withColumn("leftProv", col("l.B").cast("string"))
      val dfRightSide = dfRight.alias("r").select("r.A", "r.D").withColumn("rightProv", col("r.D").cast("string"))
      val expected: DataFrame = dfLeftSide.alias("l").crossJoin(dfRightSide.alias("r"))
        .withColumn(defaultProvenanceColName, concat(lit("("), col("leftProv"), lit(" ⊗ "), col("rightProv"), lit(")")).cast("string"))
        .drop("leftProv", "rightProv")
        .select("l.A", "l.B", "l.C", "r.D", defaultProvenanceColName)
        .distinct()
        .orderBy("l.A", "l.B", "l.C", "r.D")
      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvJoin)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when performing multiple joins"){
      //TODO: Add more complex join scenarios (e.g., multiple joins...) and ensure provenance is preserved correctly.
      val dfLeft = toyDfLeft
      val dfRight = toyDfRight
      val dfThird: DataFrame = Seq(
        ("a", "m"),
        ("d", "n"),
        ("f", "o")
      ).toDF("A", "E")

      val dfWithProvLeft = addProvenance(dfLeft, col("B"))
      val dfWithProvRight = addProvenance(dfRight, col("D"))
      val dfWithProvThird = addProvenance(dfThird, col("E"))

      dfWithProvLeft.createOrReplaceTempView("left_view_multi")
      dfWithProvRight.createOrReplaceTempView("right_view_multi")
      dfWithProvThird.createOrReplaceTempView("third_view_multi")

      val dfWithProvJoin = spark.sql(
        s"""
           SELECT l.A, l.B, l.C, r.D, t.E
           FROM left_view_multi l
           INNER JOIN right_view_multi r ON l.A = r.A
           INNER JOIN third_view_multi t ON l.A = t.A
         """
      ).orderBy("A", "B", "C", "D", "E")

        val expected: DataFrame = dfLeft.alias("l")
          .join(dfRight.alias("r"), "A")
          .join(dfThird.alias("t"), "A")
          .withColumn("leftProv", col("l.B").cast("string"))
          .withColumn("rightProv", col("r.D").cast("string"))
          .withColumn("thirdProv", col("t.E").cast("string"))
          .withColumn(defaultProvenanceColName, concat(lit("(("), col("leftProv"), lit(" ⊗ "), col("rightProv"), lit(") ⊗ "), col("thirdProv"), lit(")")))
          .drop("leftProv", "rightProv", "thirdProv")
          .select("A", "B", "C", "D", "E", defaultProvenanceColName)
          .orderBy("A", "B", "C", "D", "E")
        assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvJoin)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when performing multiple joins with views"){
      val dfLeft = toyDfLeft
      val dfRight = toyDfRight
      val dfThird: DataFrame = Seq(
        ("a", "m"),
        ("d", "n"),
        ("f", "o")
      ).toDF("A", "E")

      dfLeft.createOrReplaceTempView("left_view_multi_2")
      dfRight.createOrReplaceTempView("right_view_multi_2")
      dfThird.createOrReplaceTempView("third_view_multi_2")

      addProvenance(spark, "left_view_multi_2", col("B"))
      addProvenance(spark, "right_view_multi_2", col("D"))
      addProvenance(spark, "third_view_multi_2", col("E"))

      val dfWithProvJoin = spark.sql(
        s"""
           SELECT l.A, l.B, l.C, r.D, t.E
           FROM left_view_multi_2 l
           INNER JOIN right_view_multi_2 r ON l.A = r.A
           INNER JOIN third_view_multi_2 t ON l.A = t.A
           ORDER BY l.A, l.B, l.C, r.D, t.E
         """
      )

      val expected: DataFrame = dfLeft.alias("l")
        .join(dfRight.alias("r"), "A")
        .join(dfThird.alias("t"), "A")
        .withColumn("leftProv", col("l.B").cast("string"))
        .withColumn("rightProv", col("r.D").cast("string"))
        .withColumn("thirdProv", col("t.E").cast("string"))
        .withColumn(defaultProvenanceColName, concat(lit("(("), col("leftProv"), lit(" ⊗ "), col("rightProv"), lit(") ⊗ "), col("thirdProv"), lit(")")))
        .drop("leftProv", "rightProv", "thirdProv")
        .select("A", "B", "C", "D", "E", defaultProvenanceColName)
        .orderBy("A", "B", "C", "D", "E")

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvJoin)
    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when performing joins followed by a union and select distinct") {
      val df: DataFrame = spark.createDataFrame(
        Seq(
            ("a", "b", "c"),
            ("d", "b", "e"),
            ("f", "g", "e")
        )
      ).toDF("A", "B", "C")
      val dfWithProv = addProvenance(df, col("B"))

      val df2WithProv : DataFrame = dfWithProv
        .select("A", "B")
        .join(dfWithProv.select("B", "C"), "B")
        .select("A", "B", "C")
        .orderBy("A", "B", "C")

      val df3WithProv : DataFrame = dfWithProv
        .select("A", "C")
        .join(dfWithProv.select("B", "C"), "C")
        .select("A", "B", "C")
        .orderBy("A", "B", "C")

      val df4WithProv : DataFrame = df2WithProv
        .union(df3WithProv)
        .distinct()
        .orderBy("A", "B", "C")
        .select("A", "C")
        .distinct()
        .orderBy("A", "C")

      // The expected dataframe is constructed manually here to ensure the provenance column is 
      // correctly represented as a string with the expected format,
      // assuming the provenance column is of string type and cannot be null 
      val schema = StructType(Seq(
        StructField("A", StringType, true),
        StructField("C", StringType, true),
        StructField(defaultProvenanceColName, StringType, false) 
      ))

      val expectedRows = Seq(
        Row("a", "c", "(b ⊗ b)"),
        Row("a", "e", "(b ⊗ b)"),
        Row("d", "c", "(b ⊗ b)"),
        Row("d", "e", "{(b ⊗ g) ⊕ (b ⊗ b)}"),
        Row("f", "e", "{(g ⊗ g) ⊕ (g ⊗ b)}")
      )

      val expected: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(expectedRows), schema)

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, df4WithProv)

    }

    itWithProvenanceEnabled("should preserve the provenance column and its values when performing joins followed by a union and select distinct with views") {
      val df: DataFrame = spark.createDataFrame(
        Seq(
          ("a", "b", "c"),
          ("d", "b", "e"),
          ("f", "g", "e")
        )
      ).toDF("A", "B", "C")   
      df.createOrReplaceTempView("df_with_prov")
      addProvenance(spark, "df_with_prov", col("B"))

      val dfGlobalWithProv: DataFrame = spark.sql("""
        SELECT DISTINCT A, C
        FROM (
            SELECT t1.A, t1.B, t2.C
            FROM (
                SELECT A, B
                FROM df_with_prov
            ) AS t1
            JOIN (
                SELECT B, C
                FROM df_with_prov
            ) AS t2
            ON t1.B = t2.B

            UNION

            SELECT t1.A, t2.B, t1.C
            FROM (
                SELECT A, C
                FROM df_with_prov
            ) AS t1
            JOIN (
                SELECT B, C
                FROM df_with_prov
            ) AS t2
            ON t1.C = t2.C
        ) AS combined
        ORDER BY A, C
      """)
      


      // The expected dataframe is constructed manually here to ensure the provenance column is 
      // correctly represented as a string with the expected format,
      // assuming the provenance column is of string type and cannot be null 
      val schema = StructType(Seq(
        StructField("A", StringType, true),
        StructField("C", StringType, true),
        StructField(defaultProvenanceColName, StringType, false) 
      ))

      val expectedRows = Seq(
        Row("a", "c", "(b ⊗ b)"),
        Row("a", "e", "(b ⊗ b)"),
        Row("d", "c", "(b ⊗ b)"),
        Row("d", "e", "{(b ⊗ g) ⊕ (b ⊗ b)}"),
        Row("f", "e", "{(g ⊗ g) ⊕ (g ⊗ b)}")
      )

      val expected: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(expectedRows), schema)
      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfGlobalWithProv)
    }
  }
}


