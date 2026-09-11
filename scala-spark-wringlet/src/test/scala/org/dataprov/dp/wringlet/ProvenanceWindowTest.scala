package org.dataprov.dp.wringlet

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.dataprov.dp.wringlet.ProvenanceApi._

class ProvenanceWindowTest extends AnyFunSpec with Matchers with SparkSessionTestWrapper with DataFrameComparer with ProvenanceModeTestUtils {
      
  import spark.implicits._

  private def toyDf: DataFrame = Seq(
    ("A", "2024-01-01", 3),
    ("A", "2024-01-02", 5),
    ("B", "2024-01-01", 2)
  ).toDF("product", "date", "sales")
  
  private val viewName: String = "toy_window_view"

  private def assertProvenanceColumnAndDataPreserved(dfExpected: DataFrame, provColName: String, dfWithProv: DataFrame): Unit = {
    // 1. The provenance column should be added
    assert(dfWithProv.columns.contains(provColName))
    // 2. The expected dataframe (including the provenance column) should be equal to the actual dataframe with provenance
    assertSmallDataFrameEquality(dfWithProv, dfExpected)
  }

  describe("Window operations with provenance") {
    
    itWithProvenanceEnabled("should propagate provenance for window aggregations like group aggregate") {
      val df = toyDf
      val dfWithProv = addProvenance(df, col("sales"))

      val byProduct = Window.partitionBy("product")

      val dfWithProvWindow = dfWithProv
        .select(
          col("product"),
          col("date"),
          col("sales"),
          sum(col("sales")).over(byProduct).as("total_sales")
        )
        .filter(col("product") === "A")
        .orderBy("date")

      val expectedSchema = StructType(Seq(
        StructField("product", StringType, nullable = true),
        StructField("date", StringType, nullable = true),
        StructField("sales", IntegerType, nullable = false),
        StructField("total_sales", LongType, nullable = true), 
        StructField(defaultProvenanceColName, StringType, nullable = false)
      ))

      val expectedRows = Seq(
        Row("A", "2024-01-01", 3, 8L, "{5 ⊕ 3}"),
        Row("A", "2024-01-02", 5, 8L, "{5 ⊕ 3}")
      )

      val expected = spark.createDataFrame(spark.sparkContext.parallelize(expectedRows), expectedSchema)

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvWindow)
    }

    itWithProvenanceEnabled("should propagate provenance for window aggregations with views") {
      val df = toyDf
      df.createOrReplaceTempView(viewName)
      addProvenance(spark, viewName, col("sales"))

      val dfWithProvWindow = spark.sql(
        s"""
           |SELECT product, date, sales, sum(sales) OVER (PARTITION BY product) as total_sales 
           |FROM $viewName 
           |WHERE product = 'A'
         """.stripMargin
      ).orderBy("date")

      val expectedSchema = StructType(Seq(
        StructField("product", StringType, nullable = true),
        StructField("date", StringType, nullable = true),
        StructField("sales", IntegerType, nullable = false),
        StructField("total_sales", LongType, nullable = true), 
        StructField(defaultProvenanceColName, StringType, nullable = false)
      ))

      val expectedRows = Seq(
        Row("A", "2024-01-01", 3, 8L, "{5 ⊕ 3}"),
        Row("A", "2024-01-02", 5, 8L, "{5 ⊕ 3}")
      )

      val expected = spark.createDataFrame(spark.sparkContext.parallelize(expectedRows), expectedSchema)

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvWindow)
    }

    itWithProvenanceEnabled("should preserve provenance with nested window partition expressions") {
      val df = toyDf
      val dfWithProv = addProvenance(df, col("sales"))

      val dfWithProvNested = dfWithProv
        .select(
          col("product"),
          col("date"),
          col("sales"),
          sum(col("sales")).over(
            Window.partitionBy(max(col("date")).over(Window.partitionBy("product")))
          ).as("total_sales")
        )
        .filter(col("product") === "A")
        .orderBy("date")

      val expectedSchema = StructType(Seq(
        StructField("product", StringType, nullable = true),
        StructField("date", StringType, nullable = true),
        StructField("sales", IntegerType, nullable = false),
        StructField("total_sales", LongType, nullable = true), 
        StructField(defaultProvenanceColName, StringType, nullable = false)
      ))

      val expectedRows = Seq(
        Row("A", "2024-01-01", 3, 8L, "{5 ⊕ 3}"),
        Row("A", "2024-01-02", 5, 8L, "{5 ⊕ 3}")
      )

      val expected = spark.createDataFrame(spark.sparkContext.parallelize(expectedRows), expectedSchema)

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvNested)
    }

    itWithProvenanceEnabled("should widen provenance to the explicit sliding window bounds") {
      val df = Seq(
        ("A", "2024-01-01", 1),
        ("A", "2024-01-02", 2),
        ("A", "2024-01-03", 3),
        ("A", "2024-01-04", 4)
      ).toDF("product", "date", "sales")

      val dfWithProv = addProvenance(df, col("sales"))
      val byProductDate = Window.partitionBy("product").orderBy("date")

      val dfWithProvSliding = dfWithProv
        .select(
          col("date"),
          col("sales"),
          sum(col("sales")).over(byProductDate.rowsBetween(-1, 0)).as("prev_sum"),
          sum(col("sales")).over(byProductDate.rowsBetween(0, 1)).as("next_sum")
        )
        .orderBy("date")

      val expectedSchema = StructType(Seq(
        StructField("date", StringType, nullable = true),
        StructField("sales", IntegerType, nullable = false),
        StructField("prev_sum", LongType, nullable = true),
        StructField("next_sum", LongType, nullable = true),
        StructField(defaultProvenanceColName, StringType, nullable = false)
      ))
 
      val expectedRows = Seq(
        Row("2024-01-01", 1, 1L, 3L, "{1 ⊕ 2}"),
        Row("2024-01-02", 2, 3L, 5L, "{1 ⊕ 3 ⊕ 2}"),
        Row("2024-01-03", 3, 5L, 7L, "{3 ⊕ 4 ⊕ 2}"),
        Row("2024-01-04", 4, 7L, 4L, "{3 ⊕ 4}")
      )

      val expected = spark.createDataFrame(spark.sparkContext.parallelize(expectedRows), expectedSchema)

      assertProvenanceColumnAndDataPreserved(expected, defaultProvenanceColName, dfWithProvSliding)
    }
  }
}