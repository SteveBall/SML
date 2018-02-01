package com;


import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;
import org.junit.rules.ExternalResource;

import java.io.File;
import java.io.IOException;

public class SparkCtxtProvider extends ExternalResource {
    private static int refCount = 0;

    private static SparkCtxtProvider currentInstance;
    private static transient SparkContext _sc;
    private static transient JavaSparkContext _jsc;
    private static transient HiveContext _hc;
    private static transient SparkSession _sparkS;

    //protected static boolean initialized = false;
    public SparkContext sc() {
        return _sc;
    }

    public JavaSparkContext jsc() {
        return _jsc;
    }

    public HiveContext hc() {
        return _hc;
    }
    public SparkSession sparkS() {
        return _sparkS;
    }

    public static SparkCtxtProvider getTestResources() {
        //System.out.println("Inside getTestResources :: refCount ::" + refCount);
        if (refCount == 0) {
            // currentInstance either hasn't been created yet, or after was called on it - create a new one
            currentInstance = new SparkCtxtProvider();
        }
        return currentInstance;
    }

    private SparkCtxtProvider() {
        //System.out.println("TestResources construction");
        // setup any instance vars
    }

    protected void before() {
        System.out.println("TestResources before");
        try {
            if (refCount == 0) {
                //System.out.println("Do actual TestResources init");
                //System.out.println("Setting up Three Contexts- sparkcontest,JavaSparkContext,HiveContext !!!!");
                SparkConf conf = new SparkConf()
                        .setMaster("local[*]")
                        .setAppName("test")
                        .set("spark.ui.enabled", "false")
                        //.set("spark.app.id", appID)
                        .set("spark.driver.host", "localhost");
                _sc = new SparkContext(conf);
                _sc.setLogLevel("ERROR");
                _jsc = new JavaSparkContext(_sc);
                _hc = new HiveContext(_sc);
                _sparkS = new SparkSession(_sc);
            }
        } finally {
            refCount++;
        }
    }

    protected void after() {
        //System.out.println("TestResources after");
        //System.out.println("Inside getTestResources :: refCount ::" + refCount);
        refCount--;
        if (refCount == 0) {
            //System.out.println("Do actual TestResources destroy");
            _sc.stop();
            _jsc.stop();
            _sparkS.stop();
            _sc = null;
            _jsc = null;
            _hc = null;
            _sparkS =null;

            File hiveLocalMetaStorePath = new File("metastore_db");
            try {
                //System.out.println("Deleting hive metastore_db !!!!");

                FileUtils.deleteDirectory(hiveLocalMetaStorePath);

                //System.out.println("Deleted hive metastore_db successfully!!!!");

            } catch (IOException e) {
                e.printStackTrace();
            }

            //System.out.println("Tearing down Three Contexts- Sparkcontext,JavaSparkContext,HiveContext !!!!");
        }
    }
}