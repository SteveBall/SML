
package com.ons.sml.api.java.methods;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class JavaLagFactory {

    private static final JavaLag$ JAVA_LAG = JavaLag$.MODULE$;

    private JavaLagFactory () {}

    public static JavaLag lag(Dataset<Row> df) {return JAVA_LAG.lag(df);}
}
