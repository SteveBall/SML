package com.ons.sml.businessMethods.impl

import com.ons.sml.businessMethods.impl.MeltImpl._
import org.apache.spark.sql.DataFrame
import uk.gov.ons.SparkTesting.TestSparkContext

class MeltImplTest extends TestSparkContext {

  /**
    * The test for the melt implicits class
    */
  test("MeltImplicits Test") {
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

    // Input DataFrame going through the melt method
    val melted : DataFrame = inputData.melt1(id_vars=Seq("identifier", "date"),
                                             value_vars=Seq("two","one","three","four"),
                                             var_name = "variable",
                                             value_name = "turnover")
                                      .select("identifier", "date", "variable", "turnover")
                                      .orderBy("date")

    // Checking that the output matches expected output
    assert(melted.collect() === expectedData.collect())

  }

}
