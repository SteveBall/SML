package com.ons.sml.businessMethods.methods

import org.apache.spark.sql.DataFrame
import uk.gov.ons.SparkTesting.{SparkSessionProvider, TestSparkContext}

class MeltTest extends TestSparkContext {

  /**
    * Test for melt method
    */
  test("Melt Test") {
    // Input data
    val inputJSON: String = "./src/test/resources/sml/inputs/Melt.json"
    val inputData: DataFrame = SparkSessionProvider.sparkSession.read.json(inputJSON)
    println("Input DataFrame")
    inputData.show()

    // Expected output data
    val expectedJSON: String = "./src/test/resources/sml/outputs/Melt.json"
    val expectedData: DataFrame = SparkSessionProvider.sparkSession.read.json(expectedJSON)
                                     .select("identifier", "date", "variable", "turnover")
                                     .orderBy("date")
    println("Expected output DataFrame")
    expectedData.show()

    // Create instance of Melt
    val transform = new Melt(inputData)

    // Input DataFrame going through the melt method
    val melted : DataFrame = transform.melt1(id_vars=List("identifier", "date"),
                                             value_vars=List("two","one","three","four"),
                                             var_name = "variable",
                                             value_name = "turnover")
                                      .select("identifier", "date", "variable", "turnover")
                                      .orderBy("date")

    // Checking that the output matches expected output
    assert(melted.collect() === expectedData.collect())
  }

}
