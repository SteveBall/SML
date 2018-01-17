package com.ons.sml.businessProcesses.rsi

import org.apache.spark.sql.DataFrame
import com.ons.sml.businessMethods.impl.BaseImpl.BaseMethodsImpl


object RSIImplicits {

  val rsiText = "RSI specific text"

  implicit class RSIDataFrame(df: DataFrame) extends BaseMethodsImpl(df: DataFrame) {


    def runRSISpecificMethod() : DataFrame = {

      val rsiCol = "rsi_column"

      addConstantColumn(rsiCol, rsiText)

    }
  }
}
