package com.ons.sml.api.java.methods;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/** This is a Java Factory for the Duplicate Marking.*/
public final class JavaDuplicateFactory {
    /**This declares JAVA_DUPLICATE as a variable.*/
    private static final JavaDuplicate$ JAVA_DUPLICATE = JavaDuplicate$.MODULE$;

    /**This is a blank JavaDuplicateFactory.*/
    private JavaDuplicateFactory() {}

    /**This is a JavaDuplicate function which takes in a DataSet.
     *
     * @param df
     * @return DataFrame
     */
    public static JavaDuplicate duplicate(Dataset<Row> df){
        return JAVA_DUPLICATE.duplicate(df);}
}

