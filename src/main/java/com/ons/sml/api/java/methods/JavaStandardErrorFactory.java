package com.ons.sml.api.java.methods;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class JavaStandardErrorFactory {

    private static final JavaStandardError$ JAVA_STANDARD_ERROR= JavaStandardError$.MODULE$;

    private JavaStandardErrorFactory() {
    }

    public static JavaStandardError standardError(Dataset<Row> df){return JAVA_STANDARD_ERROR.standardError(df);}
}
