package com.ons.sml.api.java.methods;

import com.ons.sml.businessMethods.methods.Melt;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import java.util.ArrayList;
public class JavaMeltFactoryTest {

  @Test
  public void duplicateTest() {
    // Create Spark/Hive context
    SparkSession spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.driver.cores", 1)
      .appName("JavaTest")
      .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");

    // Input data
    String inputJSON = "./src/test/resources/sml/inputs/Melt.json";
    Dataset<Row> inputData = spark.read().json(inputJSON);
    System.out.println("Input DataFrame");
    inputData.show();

    // Expected output data
    String expectedJSON = "./src/test/resources/sml/outputs/Melt.json";
    Dataset<Row> expectedData = spark.read().json(expectedJSON);
    System.out.println("Expected DataFrame");
    expectedData.show();


    // Create Melt Class instance
    Melt Transform = Melt.melt(inputData);

    // Create empty lists
    ArrayList<String> id_vars = new ArrayList<String>();
    ArrayList<String> value_vars = new ArrayList<String>();

    // Add elements to list
    id_vars.add("identifier");
    id_vars.add("date");
    value_vars.add("two");
    value_vars.add("one");
    value_vars.add("three");
    value_vars.add("four");

    // Input DataFrame going through the melt method
    Dataset<Row> melted = Transform.melt1(inputData, id_vars, value_vars, "variable", "value_name");
    System.out.println("Output DataFrame");
    melted.show();

    // Checking that the output matches expected output
    assertEquals(expectedData.collectAsList(), melted.collectAsList());
  }
}