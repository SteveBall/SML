
package com.ons.sml.businessMethods.methods

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, lag}
import com.ons.sml.businessMethods.impl.LagImpl


class Lag(val dfIn: DataFrame){

  if (dfIn == null ) throw new Exception("DataFrame cannot be null")

  import LagImpl._

  def lagFunc(dataFrame: DataFrame, partitionCols: List[String],
              orderCols: List[String], targetCol: String, lagNum: Int) : DataFrame = {

    val dF = if (dataFrame == null ) dfIn else dataFrame
    dF.lagFunc(partitionCols, orderCols, targetCol, lagNum)
  }
}

object Lag {

  def lag(df: DataFrame) : Lag = {new Lag(df)}

}
