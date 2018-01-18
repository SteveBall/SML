package com.ons.sml.businessMethods.methods

import org.apache.spark.sql.DataFrame
import uk.gov.ons.SparkTesting.TestSparkContext

class StandardErrorTest extends TestSparkContext {

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
  /**
    * This test, will test whether if not input dataframe is given in the method, then the dataframe
    * given to the class will be used instead.
    */
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
  /**
    * This test that if no output column name is set, then the default from the class is chosen instead.
    */
  test("testDefaultCol") {
    // Input data
    val inputData: DataFrame = dataIn()
    // Expected Data
    val expOutData: DataFrame =dataExpected().withColumnRenamed("stdError","StandardError")

    // Calling the method
    val stdErrObj = new StandardError(inputData)
    val realOutData: DataFrame = stdErrObj.stdErr1(inputData, "xColumn", "yColumn", "zColumn")

    //Asserting the dataframes match
    assert(expOutData.select("ref", "xColumn", "yColumn", "zColumn", "StandardError").collect() === realOutData.select("ref", "xColumn", "yColumn", "zColumn", "StandardError")
      .collect())
  }
  /**
    * This test that if a dataFrame is given to the method, then this will go write the dataFrame given to the class
    */
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
  /**
    * This test checks that when a parameter is given as null and error message is returned.
    */

  test("testMissingArgException") {
    // Input Data
    val inputData: DataFrame = dataIn()
    // Calling Method
    val stdErrObj = new StandardError(inputData.select("ref"))
    val exceptMSg=intercept[Exception]{stdErrObj.stdErr1(inputData, "xColumn", "yColumn", null,"stdError")}

    //Asserting the expected exception message
    assert(exceptMSg.getMessage === "Missing mandatory argument")
  }
  /**
    * This test checks that when not all the columns selected are there, an error message is returned
    */

  test("testCHeckColumnsException") {
    // Input Data
    val inputData: DataFrame = dataIn()
    // Calling Method
    val stdErrObj = new StandardError(inputData.select("ref", "xColumn"))
    val exceptMSg=intercept[Exception]{stdErrObj.stdErr1(null, "xColumn", "yColumn", "zColumn","stdError")}

    //Asserting the expected exception message
    assert(exceptMSg.getMessage === "Missing Columns Detected")
  }

  test("testStandardError") {
    // Input Data
    val inputData: DataFrame = dataIn()

    // Asserting that the class compiles
    assertCompiles("new StandardError(inputData)")

    // Asserting that the class won't compile if nothing is given to it
    assertDoesNotCompile("new StandardError()")

    // Getting the error message form the class where the dataFrame is null
    val exceptMSg=intercept[Exception]{new StandardError(null)}

    //Asserting the expected exception message
    assert(exceptMSg.getMessage === "DataFrame cannot be null")

  }

}
