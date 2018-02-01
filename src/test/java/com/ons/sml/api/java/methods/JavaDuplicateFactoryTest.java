package com.ons.sml.api.java.methods;

import com.SharedSparkCtxtJava;
import com.SparkCtxtProvider;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class JavaDuplicateFactoryTest extends SharedSparkCtxtJava {
/*
public class JavaDuplicateFactoryTest {

    @ClassRule
    public static SparkCtxtProvider spark = SparkCtxtProvider.getTestResources();
*/

    @Test
    public void duplicateTest() {

        // Input data
        String inData = "./src/test/resources/sml/inputs/DuplicateMarker.json";
        Dataset<Row> inDf = sparkS().read().json(inData).select("id", "num", "order");
        //Dataset<Row> inDf = spark.sparkS().read().json(inData).select("id", "num", "order");
        System.out.println("Input DataFrame");
        inDf.show();

        // Expected data
        String expData = "./src/test/resources/sml/outputs/DuplicateMarker.json";
        Dataset<Row> expDf = sparkS().read().json(expData).select("id", "num", "order", "marker");
        //Dataset<Row> expDf = spark.sparkS().read().json(expData).select("id", "num", "order", "marker");
        System.out.println("Expected DataFrame");
        expDf.show();


        // Create Class
        JavaDuplicate Dup = JavaDuplicate.duplicate(inDf);

        // Output data
        ArrayList<String> partCol = new ArrayList<String>();
        ArrayList<String> ordCol = new ArrayList<String>();

        partCol.add("id");
        partCol.add("num");
        ordCol.add("order");

        Dataset<Row> outDf = Dup.dm1(inDf, partCol, ordCol, "marker");
        System.out.println("Output DataFrame");
        outDf.show();

        // Assert
        assertEquals(expDf.collectAsList(), outDf.collectAsList());
    }
}