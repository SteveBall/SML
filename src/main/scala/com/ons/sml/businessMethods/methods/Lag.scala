
package com.ons.sml.businessMethods.methods

import org.apache.spark.sql.DataFrame
import com.ons.sml.businessMethods.impl.LagImpl

/** author: Ian Edward
  * version: 1.0
  *
  * Class containing lag methods
  * @param dfIn - Input DataFrame
  */
class Lag(val dfIn: DataFrame){

  if (dfIn == null ) throw new Exception("DataFrame cannot be null")

  import LagImpl._

  /**
    * @param df DataFrame                               - Input DataFrame
    * @param partitionCols List[String]                 - List of column(s) to partition on
    * @param orderCols List[String]                     - List of column(s) to order on
    * @param targetCol String                           - Column name to create a window over
    * @param lagNum Integer                             - The number of rows back from current row from which to obtain a value
    * @return sql.DataFrame                             - DataFrame
    */
  def lagFunc(df: DataFrame, partitionCols: List[String],
              orderCols: List[String], targetCol: String, lagNum: Int) : DataFrame = {

    val dF: DataFrame = if (df == null ) dfIn else df

    dF.checkColNames(Seq(partitionCols, orderCols).flatten)
      .lagFunc(partitionCols, orderCols, targetCol, lagNum)
  }
}

object Lag {

  def lag(df: DataFrame) : Lag = new Lag(df)

}
