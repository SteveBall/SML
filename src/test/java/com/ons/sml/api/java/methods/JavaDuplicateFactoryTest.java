package com.ons.sml.api.java.methods;

import com.ons.sml.api.java.methods.JavaDuplicateFactory.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

public class JavaDuplicateFactoryTest {

    @Test
    public void duplicate() {
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
        Dataset<Row> inDf = spark.read().json(inData);
        System.out.println("Input DataFrame");
        inDf.show();

        // Expected data


        // Output data
        //JavaDuplicate outDf = duplicate(inDf);

        // Assert
        //assertEquals();
    }
}