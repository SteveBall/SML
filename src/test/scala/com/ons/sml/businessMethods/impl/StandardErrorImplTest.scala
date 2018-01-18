package com.ons.sml.businessMethods.impl


import org.apache.spark.sql.DataFrame
import uk.gov.ons.SparkTesting.TestSparkContext

class StandardErrorImplTest extends TestSparkContext {

  test("testStandardErrorMethodsImp") {

    // Create input DataFrame
    import com.ons.sml.businessMethods.impl.StandardErrorImpl._
    val inData: String = "./src/test/resources/sml/inputs/StandardErrorDataIn.json"
    val inputData: DataFrame = _hc.read.json(inData).select("ref", "xColumn", "yColumn","zColumn")
    //println("Input dataframe")
    //inputData.show()
    // Create expected DataFrame
    val expectedData: String = "./src/test/resources/sml/outputs/StandardErrorExpected.json"
    val expOutData: DataFrame = _hc.read.json(expectedData).select("ref", "xColumn", "yColumn","zColumn","stdError")
    //println("Expected dataframe")
    //expOutData.show()
    // Calling the method
    val realOutData: DataFrame = inputData.findStandardError("stdError", "xColumn", "yColumn", "zColumn")
    //Asserting the dataframes match
    assert(expOutData.select("ref", "xColumn", "yColumn", "zColumn", "stdError").collect() === realOutData.select("ref", "xColumn", "yColumn", "zColumn", "stdError")
      .collect())
  }

}
