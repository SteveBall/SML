package com.ons.sml.api.java.processes;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class JavaRSIFactory {

    private static final JavaRSI$ JAVA_RSI = JavaRSI$.MODULE$;

    private JavaRSIFactory() {
    }

    public static JavaRSI rsi(Dataset<Row> df){return JAVA_RSI.rsi(df);}
}
