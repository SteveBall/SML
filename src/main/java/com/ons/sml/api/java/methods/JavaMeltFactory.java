package com.ons.sml.api.java.methods;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Java class used as an access point for the PySpark API.
 */
public class JavaMeltFactory {

    private static final JavaMelt$ JAVA_MELT = JavaMelt$.MODULE$;

    private JavaMeltFactory() {}

    public static JavaMelt melt(Dataset<Row> df){return JAVA_MELT.melt(df);}

}
