package com.ons.sml.api.java.methods;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.junit.Assert.*;

public class JavaStandardErrorFactoryTest {
    private static SparkSession spark;

    private static Dataset<Row> inDf;
    private static Dataset<Row> expectedDf;

    @BeforeClass
    /**
     * This method populates the spark session and data- input and expected dataframe from Json files.
     */
    public static void before() throws Exception {
        // Create Spark/Hive context
        String inData = "./src/test/resources/sml/inputs/StandardErrorDataIn.json";
        String expectedData = "./src/test/resources/sml/outputs/StandardErrorExpected.json";
        spark = SparkSession
                .builder()
                .master("local[*]")
                .config("spark.driver.cores", 1)
                .appName("JavaTest")
                .getOrCreate();
        System.out.println("Setting it up!");
        spark.sparkContext().setLogLevel("ERROR");
        // Create Input data
        inDf = spark.read().json(inData);
        // Create Expected out data
        expectedDf = spark.read().json(expectedData);
        //System.out.println("Input Dataframe :: ");
        //inDf.show();
        //System.out.println("Expected Dataframe :: ");
        //expectedDf.show();
    }


    @Test
    /**
     *This is to test the findStandardError calculation is working as expected.Actual outcome is asserted against
     * the pre-calculated expected outcome.
     * the formula used to calculate stderror:
     * {{{ stdError = sqrt{ (x/y)*(x-y) / (y-1) } * z  }}}
     *
     */
    public void standardError() {

        Dataset<Row> outDf = JavaStandardErrorFactory.standardError(inDf).stdErr1(null, "xColumn", "yColumn", "zColumn", "stdError");
        System.out.println("ActualOut DataFrame");
        outDf.show();
        assertEquals(outDf.select("ref", "xColumn", "yColumn", "zColumn", "stdError").collectAsList()
                , expectedDf.select("ref", "xColumn", "yColumn", "zColumn", "stdError").collectAsList());
    }

    @Test
    /**
     * This is to test if optional input parameter 'newColName' is passed as null , default column name 'StandardError'
     * be populated on output dataframe with expected value.
     */
    public void standardError_DefaultCol() {

        Dataset<Row> outDf = JavaStandardErrorFactory.standardError(inDf).stdErr1(null, "xColumn", "yColumn", "zColumn", null);
        System.out.println("ActualOut DataFrame");
        outDf.show();
        expectedDf = expectedDf.withColumnRenamed("stdError", "StandardError");
        assertEquals(outDf.select("ref", "xColumn", "yColumn", "zColumn", "StandardError").collectAsList()
                , expectedDf.select("ref", "xColumn", "yColumn", "zColumn", "StandardError").collectAsList());

    }

    @AfterClass
    /**
     * This is to tear down the spark session.
     */
    public static void after() throws Exception {
        System.out.println("Running: tearDown");
        spark.stop();
    }
}