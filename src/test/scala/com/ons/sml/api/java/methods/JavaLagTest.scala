<<<<<<< Updated upstream

package com.ons.sml.api.java.methods

import uk.gov.ons.SparkTesting.TestSparkContext

class JavaLagTest extends TestSparkContext {

  test("Java_API") {

    // Create DataFrame with input data
    val input_data = "./src/test/resources/sml/inputs/lag_data.json"
    val input_df = _hc.read.json(input_data).select("id", "t", "v")
    println("Input data frame:")
    input_df.show()

    // Create DataFrame with expected data
    val expected_data = "./src/test/resources/sml/outputs/expected_data.json"
    val expected_df = _hc.read.json(expected_data).select("id", "t", "v", "lagged1", "lagged2")
    println("Expected data frame:")
    expected_df.show()

    val df_api = JavaLag.lag(input_df)

    val res_api = df_api.lagFunc(input_df, List("id"), List("t"), "v", 2)
    println("Output Dataframe")
    res_api.show()

    // Assert result
    assertResult(expected_df.collect()){

      res_api.collect()
    }
  }
}
=======
//
//package com.ons.sml.api.java.methods
//
//import java.util
//
//import com.ons.sml.api.java.methods.JavaLag._
//import com.ons.sml.businessMethods.methods.Lag
//import uk.gov.ons.SparkTesting.TestSparkContext
//import org.apache.spark.sql.DataFrame
//
//class JavaLagTest extends TestSparkContext {
//
//
//  test("Java_API") {
//
//    // Create DataFrame with input data
//    val input_data = "./src/test/resources/sml/inputs/lag_data.json"
//    val input_df = _hc.read.json(input_data).select("id", "t", "v")
//    println("Input data frame:")
//    input_df.show()
//
//    // Create DataFrame with expected data
//    val expected_data = "./src/test/resources/sml/outputs/expected_data.json"
//    val expected_df = _hc.read.json(expected_data).select("id", "t", "v", "lagged1", "lagged2")
//    println("Expected data frame:")
//    expected_df.show()
//
//    val df_api = JavaLag.lag(input_df)
//
//    ArrayList<String> partColum = new ArrayList<String>();
//    Array
//
//    val res_api = df_api.lagFunc(input_df,  , ArrayList("t"), "v", 2)
//    println("Output Dataframe")
//    res_api.show()
//
//    // Assert result
//    assertResult(expected_df.collect()){
//
//      res_api.collect()
//    }
//  }
//}
>>>>>>> Stashed changes
