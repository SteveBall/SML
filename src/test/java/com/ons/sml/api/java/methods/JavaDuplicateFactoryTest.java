package com.ons.sml.api.java.methods;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class JavaDuplicateFactoryTest {

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
        String inData = "./src/test/resources/sml/inputs/DuplicateMarker.json";
        Dataset<Row> inDf = spark.read().json(inData).select("id", "num", "order");
        System.out.println("Input DataFrame");
        inDf.show();

        // Expected data
        String expData = "./src/test/resources/sml/outputs/DuplicateMarker.json";
        Dataset<Row> expDf = spark.read().json(expData).select("id", "num", "order", "marker");
        System.out.println("Expected DataFrame");
        expDf.show();


        // Create Class
        JavaDuplicate Dup = JavaDuplicate.duplicate(inDf);

        // Output data
        String[] partCol = new String[]{"id", "num"};
        String[] ordCol = new String[]{"order"};

        Dataset<Row> outDf = Dup.dm1(inDf, partCol, ordCol, "marker");
        System.out.println("Output DataFrame");
        outDf.show();

        // Assert
        assertEquals(expDf.collectAsList(), outDf.collectAsList());
    }
}