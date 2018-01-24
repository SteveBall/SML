//
//package com.ons.sml.api.java.methods;
//
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//import org.junit.Test;
//import static org.junit.Assert.assertEquals;
//import java.util.*;
//
//public class JavaLagTest {
//
//    @Test
//    public void LagTest() {
//        SparkSession spark = SparkSession
//                .builder()
//                .master("local[*]")
////                .config("spark.driver.cores", 1)
//                .appName("JavaTest")
//                .getOrCreate();
//        spark.sparkContext().setLogLevel("ERROR");
//
//    String input_data = "./src/test/resources/sml/inputs/lag_data.json";
//    Data
//
//
//    }
//    test("Java_API") {
//
//        // Create DataFrame with input data
//
//        val input_df = _hc.read.json(input_data).select("id", "t", "v")
//        println("Input data frame:")
//        input_df.show()
//
//        // Create DataFrame with expected data
//        val expected_data = "./src/test/resources/sml/outputs/expected_data.json"
//        val expected_df = _hc.read.json(expected_data).select("id", "t", "v", "lagged1", "lagged2")
//        println("Expected data frame:")
//        expected_df.show()
//
//        val df_api = JavaLag.lag(input_df)
//
//        ArrayList<String> partColum = new ArrayList<String>();
//        Array
//
//        val res_api = df_api.lagFunc(input_df,  , ArrayList("t"), "v", 2)
//        println("Output Dataframe")
//        res_api.show()
//
//        // Assert result
//        assertResult(expected_df.collect()){
//
//            res_api.collect()
//        }
//    }
//}
