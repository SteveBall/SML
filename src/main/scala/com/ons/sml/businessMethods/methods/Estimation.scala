
package com.ons.sml.businessMethods.methods

import com.ons.sml.businessMethods.impl.EstimationImpl
import org.apache.spark.sql.DataFrame


class Estimation (val dfIn: DataFrame){

  if (dfIn == null ) throw new Exception("DataFrame cannot be null")

  val defaultCol = "EstResult"

  import EstimationImpl._

  private def mandatoryArgCheck(arg1 : String) : Unit = {

    if (arg1 == null) throw new Exception("Missing mandatory argument")
  }

  def est1(df: DataFrame, inputCol : String, factor : Integer, outputCol : String = defaultCol)  : DataFrame = {

    mandatoryArgCheck(inputCol)

    val dF = if (df == null) dfIn else df

    dF.checkColNames(Seq(inputCol))
      .est1(inputCol, factor, outputCol)
  }

  def est2(df: DataFrame, inputCol : String, factor : Integer, outputCol : String = defaultCol)  : DataFrame = {

    mandatoryArgCheck(inputCol)

    val dF = if (df == null) dfIn else df

    dF.checkColNames(Seq(inputCol))
      .est2(inputCol, factor, outputCol)
  }

}

object Estimation {

  def estimation(df: DataFrame) : Estimation = {new Estimation(df)}

}
