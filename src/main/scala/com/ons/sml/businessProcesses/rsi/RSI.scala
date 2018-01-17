package com.ons.sml.businessProcesses.rsi

import com.ons.sml.businessProcesses.{DataFrameColumns, ExecutableProcess, ONSRuntimeException}
import org.apache.spark.sql.DataFrame
import com.ons.sml.businessMethods.impl.{EstimationImpl, ApportionmentImpl, UtilsImpl}

class RSI (val dfIn: DataFrame) extends ExecutableProcess with DataFrameColumns {

  import RSIImplicits._

  import EstimationImpl._
  import ApportionmentImpl._

  import UtilsImpl._

  val appColumn = "Apportioned"
  val estColumn = "Estimated"

  @throws(classOf[ONSRuntimeException])
  override def runProcess(runId : String): DataFrame = {

    dfIn.app1(sector, turnover, appColumn)
        .est2(appColumn, 10, estColumn)
        .runRSISpecificMethod()
        .addConstColumn(cVal = runId)
  }

}

object RSI {

  def rsi(df : DataFrame): RSI = new RSI(df)
}
