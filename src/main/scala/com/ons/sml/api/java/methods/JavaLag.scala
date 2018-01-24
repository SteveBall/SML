
package com.ons.sml.api.java.methods
import java.util

import scala.collection.JavaConversions._
import com.ons.sml.businessMethods.methods.Lag
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class JavaLag[K](la: Lag){

  def lagFunc(df: Dataset[Row], partitionCols: util.ArrayList[String],
         orderCols: util.ArrayList[String], targetCol: String, lagNum: Int) : DataFrame = {

    la.lagFunc(df, partitionCols.toList, orderCols.toList, targetCol, lagNum)
  }
}

object JavaLag{

  def lag(df: Dataset[Row]) : JavaLag[Lag] = {new JavaLag(Lag.lag(df))}

}
