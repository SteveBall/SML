package com.ons.sml.businessMethods.methods

import org.apache.spark.sql.DataFrame
import uk.gov.ons.SparkTesting.TestSparkContext

class StandardErrorTest extends TestSparkContext {
  /**
    * This test, will test whether if not input dataframe is given in the method, then the dataframe
    * given to the class will be used instead.
    */
  def dataIn(): DataFrame ={
    val inData: String = "./src/test/resources/sml/inputs/StandardErrorDataIn.json"
    val inputData: DataFrame = _hc.read.json(inData).select("ref", "xColumn", "yColumn","zColumn")
    //println("Input dataframe")
    //inputData.show()
    inputData
  }
  def dataExpected(): DataFrame ={
    val expectedData: String = "./src/test/resources/sml/outputs/StandardErrorExpected.json"
    val expOutData: DataFrame = _hc.read.json(expectedData).select("ref", "xColumn", "yColumn","zColumn","stdError")
    //println("Expected dataframe")
    //expOutData.show()
    expOutData
  }
  test("testDfIn") {
    // Input data
    val inputData: DataFrame = dataIn()
    // Expected Data
    val expOutData: DataFrame =dataExpected()

    // Calling the method
    val stdErrObj = new StandardError(inputData)
    val realOutData: DataFrame = stdErrObj.stdErr1(null, "xColumn", "yColumn", "zColumn","stdError")

    //Asserting the dataframes match
    assert(expOutData.select("ref", "xColumn", "yColumn", "zColumn", "stdError").collect() === realOutData.select("ref", "xColumn", "yColumn", "zColumn", "stdError")
      .collect())
  }

  test("testDefaultCol") {
    // Input data
    val inputData: DataFrame = dataIn()
    // Expected Data
    val expOutData: DataFrame =dataExpected()

    // Calling the method
    val stdErrObj = new StandardError(inputData)
    val realOutData: DataFrame = stdErrObj.stdErr1(inputData, "xColumn", "yColumn", "zColumn")

    //Asserting the dataframes match
    assert(expOutData.withColumnRenamed("stdError","StandardError").select("ref", "xColumn", "yColumn", "zColumn", "StandardError").collect() === realOutData.select("ref", "xColumn", "yColumn", "zColumn", "StandardError")
      .collect())
  }

  test("testStdErr1") {
    // Input data
    val inputData: DataFrame = dataIn()
    // Expected Data
    val expOutData: DataFrame =dataExpected()

    // Calling Method
    val stdErrObj = new StandardError(expOutData.select("ref"))
    val realOutData: DataFrame = stdErrObj.stdErr1(inputData, "xColumn", "yColumn", "zColumn","stdError")

    //Asserting the dataframes match
    assert(expOutData.select("ref", "xColumn", "yColumn", "zColumn", "stdError").collect() === realOutData.select("ref", "xColumn", "yColumn", "zColumn", "stdError")
      .collect())
  }

  test("testParameterException") {
    val inputData: DataFrame = dataIn()
    // Calling Method
    val stdErrObj = new StandardError(inputData.select("ref"))
    val exceptMSg=intercept[Exception]{stdErrObj.stdErr1(inputData, "xColumn", "yColumn", null,"stdError")}

    //Asserting the expected exception message
    assert(exceptMSg.getMessage === "Missing mandatory argument")
  }

  test("testStandardError") {
    val inputData: DataFrame = dataIn()
    assertCompiles("new StandardError(inputData)")
    assertDoesNotCompile("new StandardError()")
    val exceptMSg=intercept[Exception]{new StandardError(null)}
    //Asserting the expected exception message
    assert(exceptMSg.getMessage === "DataFrame cannot be null")

  }

}
