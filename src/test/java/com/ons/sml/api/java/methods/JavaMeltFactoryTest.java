package com.ons.sml.api.java.methods;

import com.SharedSparkCtxtJava;
import com.SparkCtxtProvider;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.ClassRule;
import org.junit.Test;
import uk.gov.ons.SparkTesting.SparkSessionProvider;
import uk.gov.ons.SparkTesting.TestSparkContext;
import uk.gov.ons.SparkTesting.TestSparkContextLike;


import static org.junit.Assert.assertEquals;

import java.util.*;

/**
 * Class containing all of the tests the the melt method.
 */
//public class JavaMeltFactoryTest extends SharedSparkCtxtJava{
public class JavaMeltFactoryTest {
    @ClassRule
    public static SparkCtxtProvider spark = SparkCtxtProvider.getTestResources();

    /**
     * Test for the melt method.
     */
    @Test
    public void meltTest() {
        // Input data
        String inputJSON = "./src/test/resources/sml/inputs/Melt.json";
        //Dataset<Row> inputData = sparkS().read().json(inputJSON);
        //Dataset<Row> inputData = SparkSessionProvider.sparkSession().read().json(inputJSON);
        Dataset<Row> inputData = spark.sparkS().read().json(inputJSON);
        System.out.println("Input DataFrame");
        inputData.show();

        // Expected output data
        String expectedJSON = "./src/test/resources/sml/outputs/Melt.json";
        //Dataset<Row> expectedData = sparkS().read().json(expectedJSON);
        //Dataset<Row> expectedData = SparkSessionProvider.sparkSession().read().json(expectedJSON);
        Dataset<Row> expectedData = spark.sparkS().read().json(expectedJSON);

        System.out.println("Expected DataFrame");
        expectedData.show();


        // Create Melt Class instance
        JavaMelt Transform = JavaMeltFactory.melt(inputData);

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
        Dataset<Row> melted = Transform.melt1(inputData, id_vars, value_vars, "variable", "value_name")
                .select("date", "identifier", "value_name", "variable")
                .orderBy("date");
        System.out.println("Output DataFrame");
        melted.show();

        // Checking that the output matches expected output
        assertEquals(expectedData.collectAsList(), melted.collectAsList());
    }
}