package com.ons.sml.businessMethods.methods

import org.apache.spark.sql.DataFrame
import uk.gov.ons.SparkTesting.TestSparkContext

import uk.gov.ons.SparkTesting.SparkSessionProvider

class DuplicateTest extends TestSparkContext {

  test("testDm1") {
    // Input data


    val inData: String = "./src/test/resources/sml/inputs/DuplicateMarker.json"
    val inDf: DataFrame = SparkSessionProvider.sparkSession.read.json(inData).select("id", "num", "order")
    println("Input dataframe")
    inDf.show()

    // Expected data
    val expData: String = "./src/test/resources/sml/outputs/DuplicateMarker.json"
    val expDf: DataFrame = SparkSessionProvider.sparkSession.read.json(expData).select("id", "num", "order", "marker")
    println("Expected dataframe")
    expDf.show()

    // Create object
    val dup = new Duplicate(expDf.select("id"))

    // Create actual output
    val outDf: DataFrame = dup.dm1(inDf, List("id", "num"), List("order"), "marker")
    println("Output dataframe")
    outDf.show()

    // Assert
    assertResult(expDf.collect()) {
      outDf.collect()
    }
  }

  test("testDfIn") {
    // Input data
    val inData: String = "./src/test/resources/sml/inputs/DuplicateMarker.json"
    val inDf: DataFrame = SparkSessionProvider.sparkSession.read.json(inData).select("id", "num", "order")
    println("Input dataframe")
    inDf.show()

    // Expected data
    val expData: String = "./src/test/resources/sml/outputs/DuplicateMarker.json"
    val expDf: DataFrame = SparkSessionProvider.sparkSession.read.json(expData).select("id", "num", "order", "marker")
    println("Expected dataframe")
    expDf.show()

    // Create object
    val dup = new Duplicate(inDf)

    // Create actual output
    val outDf: DataFrame = dup.dm1(null, List("id", "num"), List("order"), "marker")
    println("Output dataframe")
    outDf.show()

    // Assert
    assertResult(expDf.collect()) {
      outDf.collect()
    }
  }

  test("testDefaultCol") {
    // Input data
    val inData: String = "./src/test/resources/sml/inputs/DuplicateMarker.json"
    val inDf: DataFrame = SparkSessionProvider.sparkSession.read.json(inData).select("id", "num", "order")
    println("Input dataframe")
    inDf.show()

    // Expected data
    val expData: String = "./src/test/resources/sml/outputs/DuplicateMarker.json"
    val expDf: DataFrame = SparkSessionProvider.sparkSession.read.json(expData).select("id", "num", "order", "marker")
      .withColumnRenamed("marker", "DuplicateMarking")
    println("Expected dataframe")
    expDf.show()

    // Create object
    val dup = new Duplicate(inDf)

    // Create actual output
    val outDf: DataFrame = dup.dm1(null, List("id", "num"), List("order"))
    println("Output dataframe")
    outDf.show()

    // Assert
    assertResult(expDf.collect()) {
      outDf.collect()
    }
  }

  test("testDuplicate") {
    // Input data
    val inData: String = "./src/test/resources/sml/inputs/DuplicateMarker.json"
    val inDf: DataFrame = SparkSessionProvider.sparkSession.read.json(inData).select("id", "num", "order")
    println("Input dataframe")
    inDf.show()

    //create a invalid object
    val invalDf = intercept[Exception] {new Duplicate(null)}

    assertCompiles("new Duplicate(inDf)")
    assertDoesNotCompile("new Duplicate()")
    assert(invalDf.getMessage === "DataFrame cannot be null")
  }

}
