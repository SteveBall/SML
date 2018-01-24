package com.ons.sml.api.java.methods;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class JavaEstimationFactory {

    private static final JavaEstimation$ JAVA_ESTIMATION = JavaEstimation$.MODULE$;

    private JavaEstimationFactory() {
    }

    public static JavaEstimation estimation(Dataset<Row> df){return JAVA_ESTIMATION.estimation(df);}
}
