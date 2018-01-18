package com.ons.sml.businessMethods.impl

import com.ons.sml.businessMethods.impl.DuplicateImpl._
import org.apache.spark.sql.DataFrame
import uk.gov.ons.SparkTesting.TestSparkContext


class DuplicateImplTest extends TestSparkContext {

  test("test duplicateMarking implicit") {
    // Input data
    val inData: String = "./src/test/resources/sml/inputs/DuplicateMarker.json"
    val inDf: DataFrame = _hc.read.json(inData).select("id", "num", "order")
    println("Input dataframe")
    inDf.show()

    // Expected data
    val expData: String = "./src/test/resources/sml/outputs/DuplicateMarker.json"
    val expDf: DataFrame = _hc.read.json(expData).select("id", "num", "order", "marker")
    println("Expected dataframe")
    expDf.show()

    // Create actual output
    val outDf: DataFrame = inDf.duplicateMarking(List("id", "num"), List("order"), "marker")
    println("Output dataframe")
    outDf.show()

    // Assert
    assertResult(expDf.collect()) {
      outDf.collect()
    }
  }
}
