package com.ons.sml.api.java.methods;

import com.SharedSparkCtxtJava;
import com.SparkCtxtProvider;
import org.junit.ClassRule;
import org.junit.Test;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.junit.Assert.*;

public class JavaStandardErrorFactoryTest extends SharedSparkCtxtJava {
/*public class JavaStandardErrorFactoryTest {
    @ClassRule
    public static SparkCtxtProvider spark = SparkCtxtProvider.getTestResources();*/



    private String inData = "./src/test/resources/sml/inputs/StandardErrorDataIn.json";
    private String expectedData = "./src/test/resources/sml/outputs/StandardErrorExpected.json";



    @Test
    /**
     *This is to test the findStandardError calculation is working as expected.Actual outcome is asserted against
     * the pre-calculated expected outcome.
     * the formula used to calculate stderror:
     * {{{ stdError = sqrt{ (x/y)*(x-y) / (y-1) } * z  }}}
     *
     */
    public void standardError() {

        // Create Input data
        Dataset<Row> inDf = sparkS().read().json(inData);
        //Dataset<Row> inDf = spark.sparkS().read().json(inData);
        // Create Expected out data
        Dataset<Row> expectedDf = sparkS().read().json(expectedData);
        //Dataset<Row> expectedDf = spark.sparkS().read().json(expectedData);
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


        // Create Input data
        Dataset<Row> inDf = sparkS().read().json(inData);
        //Dataset<Row> inDf = spark.sparkS().read().json(inData);

        // Create Expected out data
        Dataset<Row> expectedDf = sparkS().read().json(expectedData);
        //Dataset<Row> expectedDf = spark.sparkS().read().json(expectedData);
        Dataset<Row> outDf = JavaStandardErrorFactory.standardError(inDf).stdErr1(null, "xColumn", "yColumn", "zColumn", null);
        System.out.println("ActualOut DataFrame");
        outDf.show();
        expectedDf = expectedDf.withColumnRenamed("stdError", "StandardError");
        assertEquals(outDf.select("ref", "xColumn", "yColumn", "zColumn", "StandardError").collectAsList()
                , expectedDf.select("ref", "xColumn", "yColumn", "zColumn", "StandardError").collectAsList());

    }


}