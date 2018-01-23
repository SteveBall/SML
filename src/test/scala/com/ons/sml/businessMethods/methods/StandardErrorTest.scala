package com.ons.sml.businessMethods.methods

import org.apache.spark.sql.DataFrame
import uk.gov.ons.SparkTesting.TestSparkContext

/** This class holds all the test to test the standard error scala API.
  * In total it holds 2 data setup functions and 6 tests.
  *
  */
class StandardErrorTest extends TestSparkContext {

  /** This function reads in the input data that will go through the standard error object.
    *
    * @return dataframe
    */
  def dataIn(): DataFrame ={
    // The String location of where the input data is stored
    val inData: String = "./src/test/resources/sml/inputs/StandardErrorDataIn.json"
    // Reading in the input data
    val inputData: DataFrame = _hc.read.json(inData).select("ref", "xColumn", "yColumn","zColumn")
    // Printing it the data
    println("Input dataframe")
    inputData.show()
    // Returning the data to the test
    inputData
  }

  /** This function reads in the expected data that will be compared with the output of the object.
    *
    * @return
    */
  def dataExpected(): DataFrame ={
    // The String location of where the Expected data is stored
    val expectedData: String = "./src/test/resources/sml/outputs/StandardErrorExpected.json"
    // Reading in the expected data
    val expOutData: DataFrame = _hc.read.json(expectedData).select("ref", "xColumn", "yColumn","zColumn","stdError")
    // Printing it the data
    println("Expected dataframe")
    expOutData.show()
    // Returning the data to the test
    expOutData
  }

  /** This test, will test whether if not input dataframe is given in the method, then the dataframe
    * given to the class will be used instead.
    *
    * It calls the input data and the expected data. Then it creates the object with the input data, from this the
    * method is called where the dataframe is set to null.
    *
    * The output of this is asserted against the expected data.
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
    assert(expOutData.collect() === realOutData.select("ref", "xColumn", "yColumn", "zColumn", "stdError")
      .collect())
  }

  /**
    * This test that if no output column name is set, then the default from the class is chosen instead.
    *
    * It calls the input data and the expected data, but also renames the stfError column to be StandardError like the
    * defaultCol is on the class. Then it creates the object and calls the method.
    * The output of
    * this is then asserted against the expected data.
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
    assert(expOutData.collect() === realOutData.select("ref", "xColumn", "yColumn", "zColumn",
                                                       "StandardError").collect())
  }

  /**
    * This test that if a dataFrame is given to the method, then this will go write the dataFrame given to the class.
    *
    * It calls the input data and the expected data. Then it creates the object with a dataframe with only the
    * ref column. The method is then called from this object giving it the input data.
    *
    * The output is asserted against the expected data.
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
    assert(expOutData.collect() === realOutData.select("ref", "xColumn", "yColumn", "zColumn", "stdError").collect())
  }

  /**
    * This test checks that when a parameter is given as null and error message is returned.
    *
    * It calls the input data and the expected data. Then it creates the object with a dataframe with only the
    * ref column. The method is then called from this object giving it the input data and where the zColumn is
    * set to null, the error from this is intercepted and asserted against the message that should be returned.
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
    * This test checks that when not all the columns selected are there, an error message is returned.
    *
    * It calls the input data and the expected data. Then it creates the object with a dataframe with only two columns,
    * ref and xColumn. The method is then called from this object giving it the input data and where the zColumn is
    * set to null, the error from this is intercepted and asserted against the message that should be returned.
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

  /** This test asserts three things, that the object compiles, that it won't compile if no data is given and if the
    * dataframe given to object is null.
    *
    * It does this by bring in the input data, then doing the first two asserts.
    * Then the object is created where the data is null and the error is intercepted, the message from this is
    * then asserted against the message that should appear.
    *
    */
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
