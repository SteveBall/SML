package com.ons.sml.businessMethods.impl

import com.ons.sml.businessMethods.impl.SumColImpl._
import org.apache.spark.sql.DataFrame

import uk.gov.ons.SparkTesting.TestSparkContext

class SumColImplTest extends TestSparkContext {

  /**
    * The test for the melt implicits class
    */

  test("SumColImplicits Test") {

    // Input data
    val inputJSON: String = "./src/test/resources/sml/inputs/SumCol_RAW.json"
    val inputData: DataFrame = _hc.read.json(inputJSON)
    println("Input DataFrame")
    inputData.show()


    // Expected data Test A
    val expectedJSON_A: String = "./src/test/resources/sml/outputs/SumCol_TestA.json"
    val expectedData_A: DataFrame = _hc.read.json(expectedJSON_A)
     // .select()
     // .orderBy()
    println("Expected DataFrame")
    expectedData_A.show()

    // Test A
    val df_test_a : DataFrame = inputData.sumCol(Seq("Region", "Period"), "Sales_Rounded_GBP")

    // Test A
    assert(df_test_a.collect() === expectedData_A.collect())



    // Expected data Test A
    val expectedJSON_B: String = "./src/test/resources/sml/outputs/SumCol_TestA.json"
    val expectedData_B: DataFrame = _hc.read.json(expectedJSON_B)
    // .select()
    // .orderBy()
    println("Expected DataFrame")
    expectedData_B.show()

    // Test A
    val df_test_b : DataFrame = inputData.sumCol(Seq("Department"), "Sales_Rounded_GBP")

    // Test A
    assert(df_test_b.collect() === expectedData_B.collect())


  }

}