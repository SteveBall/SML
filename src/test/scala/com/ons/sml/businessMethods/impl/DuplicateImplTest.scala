package com.ons.sml.businessMethods.impl

import com.ons.sml.businessMethods.impl.DuplicateImpl._
import org.apache.spark.sql.DataFrame
import uk.gov.ons.SparkTesting.TestSparkContext


class DuplicateImplTest extends TestSparkContext {

  test("test duplicate_marker with test data") {
    // Input data
    val inputData: String = "./src/test/resources/sml/inputs/DuplicateMarker.json"
    val inputdf: DataFrame = _hc.read.json(inputData).select("id", "num", "order")
    println("Input dataframe")
    inputdf.show()

    // Golden data
    val goldenData: String = "./src/test/resources/sml/outputs/DuplicateMarker.json"
    val goldendf: DataFrame = _hc.read.json(goldenData).select("id", "num", "order", "marker")
    println("Golden dataframe")
    goldendf.show()

    // Create actual output
    val outDf: DataFrame = inputdf.duplicateMarking(List("id", "num"), List("order"), "marker")
    println("Output DataFrame")
    outDf.show()

    // Assert
    assertResult(goldendf.collect()) {
      outDf.collect()

    }
  }
}
