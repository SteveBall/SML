package com.ons.sml.api.java.processes

import com.ons.sml.businessProcesses.rsi.RSI
import org.apache.spark.sql.{DataFrame, Dataset, Row}


class JavaRSI[K](rsi: RSI){

  def runProcess(runId : String) : DataFrame = {

    rsi.runProcess(runId)
  }

}

object JavaRSI {

  def rsi(df: Dataset[Row]) : JavaRSI[RSI] = {new JavaRSI(RSI.rsi(df))}

}