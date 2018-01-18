
package com.ons.sml.api.java.methods

import com.ons.sml.businessMethods.methods.{Lag}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class JavaLag[K](la: Lag){

  def lagFunc(df: DataFrame, partitionCols: List[String],
         orderCols: List[String], targetCol: String, lagNum: Int) : DataFrame = {

    la.lagFunc(df, partitionCols, orderCols, targetCol, lagNum)

  }
}

object JavaLag {

  def lag(df: Dataset[Row]) : JavaLag[Lag] = {new JavaLag(Lag.lag(df))}

}
