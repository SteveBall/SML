package com.ons.sml.api.java.methods;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class JavaDuplicateFactory {

    private static final JavaDuplicate$ JAVA_DUPLICATE = JavaDuplicate$.MODULE$;

    private JavaDuplicateFactory() {}

    public static JavaDuplicate duplicate(Dataset<Row> df){
        return JAVA_DUPLICATE.duplicate(df);}
}

