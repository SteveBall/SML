
package com.ons.sml.businessMethods.methods

import org.apache.spark.sql.DataFrame
import com.ons.sml.businessMethods.impl.LagImpl


class Lag(val dfIn: DataFrame){

  if (dfIn == null ) throw new Exception("DataFrame cannot be null")

  import LagImpl._

  def lagFunc(df: DataFrame, partitionCols: List[String],
              orderCols: List[String], targetCol: String, lagNum: Int) : DataFrame = {

    val dF: DataFrame = if (df == null ) dfIn else df

    dF.lagFunc(partitionCols, orderCols, targetCol, lagNum)
  }
}

object Lag {

  def lag(df: DataFrame) : Lag = new Lag(df)

}
