package com.ons.sml.api.java.methods

import org.apache.spark.sql.DataFrame
import uk.gov.ons.SparkTesting.TestSparkContext
import java.util

/**
  * Class containing the Scala tests for the Java API code
  */
class JavaMeltTest extends TestSparkContext {

  /**
    * The test for melt method 1
    */
  test("testMelt1") {
    // Input data
    val inputJSON: String = "./src/test/resources/sml/inputs/Melt.json"
    val inputData: DataFrame = _hc.read.json(inputJSON)
    println("Input DataFrame")
    inputData.show()

    // Expected output data
    val expectedJSON: String = "./src/test/resources/sml/outputs/Melt.json"
    val expectedData: DataFrame = _hc.read.json(expectedJSON)
      .select("identifier", "date", "variable", "turnover")
      .orderBy("date")
    println("Expected output DataFrame")
    expectedData.show()

    // Create instance of Melt
    val Transform = JavaMelt.melt(inputData)

    // Create the Java ArrayLists
    var id_vars = new util.ArrayList[String]
    var value_vars = new util.ArrayList[String]

    // Add in elements
    id_vars.add("identifier")
    id_vars.add("date")
    value_vars.add("two")
    value_vars.add("one")
    value_vars.add("three")
    value_vars.add("four")

    // Input DataFrame going through the melt method
    val melted : DataFrame = Transform.melt1(inputData,
                                             id_vars,
                                             value_vars,
                                             var_name = "variable",
                                             value_name = "turnover")
      .select("identifier", "date", "variable", "turnover")
      .orderBy("date")

    // Checking that the output matches expected output
    assert(melted.collect() === expectedData.collect())
  }

}
