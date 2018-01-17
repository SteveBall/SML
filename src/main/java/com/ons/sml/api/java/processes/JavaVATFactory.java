package com.ons.sml.api.java.processes;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class JavaVATFactory {

    private static final JavaVAT$ JAVA_VAT = JavaVAT$.MODULE$;

    private JavaVATFactory() {
    }

    public static JavaVAT vat(Dataset<Row> df){return JAVA_VAT.vat(df);}
}
