
package com;
import org.apache.commons.io.FileUtils;
import org.apache.spark.*;
import org.apache.spark.api.java.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;
import org.junit.*;
import uk.gov.ons.SparkTesting.*;

import java.io.File;
import java.io.IOException;

//import uk.gov.ons.SparkTesting.SparkContextProvider;


/** Shares a local `SparkContext` between all tests in a suite and closes it at the end */
public class SharedSparkCtxtJava implements SparkContextProvider {
    private static transient SparkContext _sc;
    private static transient JavaSparkContext _jsc;
    private static transient HiveContext _hc;
    private static transient SparkSession _sparkS;

    protected boolean initialized = false;

    // Since we want to support java 7 for now we can't use the default method from the trait directly.
    @Override public void setup(SparkContext sc) {
        SparkContextProvider$class.setup(this, sc);
    }

    @Override public String appID() {
        return SparkContextProvider$class.appID(this);
    }

    @Override public SparkConf conf() {
        return SparkContextProvider$class.conf(this);
    }

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

    /**
     * Hooks for setup code that needs to be executed/torn down in order with SparkContexts
     */
    protected void beforeAllTestCasesHook() {
    }

    protected static void afterAllTestCasesHook() {
    }

    @Before
     public void runBefore() {
        initialized = (_sc != null);
        //System.out.println("Already initialized ::: " + initialized);
        if (!initialized) {
            //System.out.println("Setting up Three Contexts- sparkcontest,JavaSparkContext,HiveContext !!!!");
            _sc = new SparkContext(conf());
            _sc.setLogLevel("ERROR");
            _jsc = new JavaSparkContext(_sc);
            _hc = new HiveContext(_sc);
            _sparkS = new SparkSession(_sc);

            beforeAllTestCasesHook();
        }
    }

    @AfterClass
    static public void runAfterClass() {
        LocalSparkContext$.MODULE$.stop(_sc);
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

        afterAllTestCasesHook();

        //System.out.println("Tearing down Three Contexts- sparkcontest,JavaSparkContext,HiveContext !!!!");
    }
}