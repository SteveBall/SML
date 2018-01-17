package com.ons.sml.businessMethods.impl

import com.ons.sml.businessProcesses.ONSRuntimeException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

import scala.util.Try

object BaseImpl {

  implicit class BaseMethodsImpl(df: DataFrame) {

    def checkColNames(columns: Seq[String]): DataFrame = {

      val colsFound = columns.flatMap(col => Try(df(col)).toOption)

      val okToContinue = columns.size == colsFound.size

      if (!okToContinue) throw ONSRuntimeException("Missing Columns Detected") else df
    }

    def addConstantColumn(cName: String = "RunID", cVal: String): DataFrame = {

      df.withColumn(cName, lit(cVal))
    }

  }
}
