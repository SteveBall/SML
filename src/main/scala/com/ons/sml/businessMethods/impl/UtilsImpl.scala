package com.ons.sml.businessMethods.impl

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

object UtilsImpl {

  implicit class UtilMethodsImpl(df: DataFrame) {

    def addConstColumn(cName: String = "RunID", cVal: String): DataFrame = {

      df.withColumn(cName, lit(cVal))
    }
  }
}
