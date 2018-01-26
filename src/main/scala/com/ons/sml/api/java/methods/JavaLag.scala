
package com.ons.sml.api.java.methods
import java.util

import scala.collection.JavaConversions._
import com.ons.sml.businessMethods.methods.Lag
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class JavaLag[K](la: Lag){

  /**
    * @param df sql.DataFrame                           - Input DataFrame
    * @param partitionCols List[String]                 - List of column(s) to partition on
    * @param orderCols List[String]                     - List of column(s) to order on
    * @param targetCol String                           - Column name to create a window over
    * @param lagNum Integer                             - The number of rows back from the current row from which to obtain a value
    * @return
    */
  def lagFunc(df: Dataset[Row], partitionCols: util.ArrayList[String],
         orderCols: util.ArrayList[String], targetCol: String, lagNum: Int) : DataFrame = {

    la.lagFunc(df, partitionCols.toList, orderCols.toList, targetCol, lagNum)
  }
}

object JavaLag{

  def lag(df: Dataset[Row]) : JavaLag[Lag] = {new JavaLag(Lag.lag(df))}

}
