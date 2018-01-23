package com.ons.sml.api.java.methods;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 *This class is a factory to create JavaStandardError instance.
 */
public class JavaStandardErrorFactory {

    private static final JavaStandardError$ JAVA_STANDARD_ERROR = JavaStandardError$.MODULE$;

    private JavaStandardErrorFactory() {
    }

    /**
     * Instantiates a JavaStandardError from a DataSet.Please refer "JavaStandardError.scala" document string
     * for more details.
     *
     * @param df -The input DataSet
     * @return JavaStandardError - It gives the access point to scala implementation of StandardError
     */
    public static JavaStandardError standardError(Dataset<Row> df) {
        return JAVA_STANDARD_ERROR.standardError(df);
    }
}
