package com.ons.sml.api.java.methods;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class JavaApportionmentFactory {

    private static final JavaApportionment$ JAVA_APPORTIONMENT = JavaApportionment$.MODULE$;

    private JavaApportionmentFactory() {
    }

    public static JavaApportionment apportionment(Dataset<Row> df){return JAVA_APPORTIONMENT.apportionment(df);}
}
