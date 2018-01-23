package com.cloudera.sparkts.api.java.methods;

import com.ons.sml.api.java.methods.JavaLag;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import scala.collection.JavaConversions;
import java.util.ArrayList;
import static org.junit.Assert.assertEquals;

public class JavaLagFactoryTest {

    @Test
    public void lagTest() {
        // Create Spark/Hive context
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .config("spark.driver.cores", 1)
                .appName("JavaTest")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        // Input data
        String inputJSON = "./src/test/resources/sml/inputs/lag_data.json";
        Dataset<Row> inputData = spark.read().json(inputJSON);
        System.out.println("Input data frame:");
        inputData.show();

        // Expected output data
        String expectedJSON = "./src/test/resources/sml/outputs/expected_data.json";
        Dataset<Row> expectedData = spark.read().json(expectedJSON).select("id", "t", "v", "lagged1", "lagged2");
        System.out.println("Expected data frame:");
        expectedData.show();

        // Create Lag Class instance
        JavaLag Transform = JavaLag.lag(inputData);

        // Create empty lists
        ArrayList<String> id = new ArrayList<String>();
        ArrayList<String> t = new ArrayList<String>();

        // Add elements to list
        id.add("id");
        t.add("t");

        // Input DataFrame going through the lag method
        Dataset<Row> outputData = Transform.lagFunc(inputData, JavaConversions.asScalaBuffer(id).toList(), JavaConversions.asScalaBuffer(t).toList(), "v", 2)
                .select("id", "t", "v", "lagged1", "lagged2");

        System.out.println("Output data frame:");
        outputData.show();

        // Checking that the output matches expected output
        assertEquals(expectedData.collectAsList(), outputData.collectAsList());
    }
}
