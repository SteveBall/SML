package com.ons.sml.api.java.methods

import org.apache.spark.sql.DataFrame
import uk.gov.ons.SparkTesting.{SparkSessionProvider, TestSparkContext}
import java.util

class JavaLagTest extends TestSparkContext {

  test("Java_API") {

    // Create DataFrame with input data
    val input_data = "./src/test/resources/sml/inputs/lag_data.json"
    val input_df = SparkSessionProvider.sparkSession.read.json(input_data).select("id", "t", "v")
    println("Input Data Frame:")
    input_df.show()

    // Create DataFrame with expected data
    val expected_data = "./src/test/resources/sml/outputs/expected_data.json"
    val expected_df = SparkSessionProvider.sparkSession.read.json(expected_data).select("id", "t", "v", "lagged1", "lagged2")
    println("Expected Data Frame:")
    expected_df.show()

    val df_api = JavaLag.lag(input_df)

    var part_cols = new util.ArrayList[String]
    val order_cols = new util.ArrayList[String]

    part_cols.add("id")
    order_cols.add("t")

    val lagged: DataFrame = df_api.lagFunc(input_df, part_cols, order_cols, "v", 2)
    println("Result Data Frame")
    lagged.show()

    assert(lagged.collect() === expected_df.collect())

  }

}
