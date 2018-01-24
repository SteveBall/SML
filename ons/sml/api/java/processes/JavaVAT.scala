package com.ons.sml.api.java.processes

import com.ons.sml.businessProcesses.vat.VAT
import org.apache.spark.sql.{DataFrame, Dataset, Row}


class JavaVAT[K](vat: VAT){

  def runProcess(runId : String) : DataFrame = {

    vat.runProcess(runId)
  }

}

object JavaVAT {

  def vat(df: Dataset[Row]) : JavaVAT[VAT] = {new JavaVAT(VAT.vat(df))}

}