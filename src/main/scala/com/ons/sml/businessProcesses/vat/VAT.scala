package com.ons.sml.businessProcesses.vat

import com.ons.sml.businessProcesses.{DataFrameColumns, ExecutableProcess, ONSRuntimeException}
import org.apache.spark.sql.DataFrame
import com.ons.sml.businessMethods.impl.{ApportionmentImpl, EstimationImpl, UtilsImpl}

class VAT (val dfIn: DataFrame) extends ExecutableProcess with DataFrameColumns {

  import VATImplicits._

  import EstimationImpl._
  import ApportionmentImpl._

  import UtilsImpl._

  val appColumn = "Apportioned"
  val estColumn = "Estimated"

  @throws(classOf[ONSRuntimeException])
  override def runProcess(runId : String): DataFrame = {

    dfIn.app4(sector, turnover, appColumn)
        .est1(appColumn, 10, estColumn)
        .runVATSpecificMethod()
        .addConstColumn(cVal = runId)
  }

}

object VAT {

  def vat(df : DataFrame): VAT = new VAT(df)
}
