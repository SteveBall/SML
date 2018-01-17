package com.ons.sml.businessProcesses

import com.ons.sml.persistence.DebugTrait


object DataFrameImplicits {

  import org.apache.spark.sql.DataFrame
  import org.apache.spark.sql.functions._

  import scala.util.Try


  implicit class ONSDataFrame(df: DataFrame) extends DebugTrait{


    def checkColNames(columns : Seq[String]) : DataFrame  = {

      val colsFound = columns.flatMap(col => Try(df(col)).toOption)

      val okToContinue = columns.size == colsFound.size

      if(!okToContinue) throw ONSRuntimeException("Missing Columns Detected") else df
    }

    def addConstantColumn(cName : String, cVal: String) : DataFrame = {

        df.withColumn(cName, lit(cVal))
    }

    def writeDFToConsole(): DataFrame = {
      writeToConsole(df)
    }
  }





}
