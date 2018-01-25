
package com.ons.sml.businessMethods.impl

import com.ons.sml.businessMethods.impl.BaseImpl.BaseMethodsImpl
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag}

object LagImpl {

  implicit class LagMethodsImp(df: DataFrame) extends BaseMethodsImpl(df: DataFrame) {

    /**
      * @param partitionCols List[String]                 - List of column(s) to partition on
      * @param orderCols List[String]                     - List of column(s) to order on
      * @param targetCol String                           - Column name to create a window over
      * @param lagNum Integer                             - The number of rows back from the current row from which to obtain a value
      * @return sql.DataFrame                             - The DataFrame returned
      */
    def lagFunc(partitionCols: List[String],
                orderCols: List[String], targetCol: String, lagNum: Int): DataFrame = {

      val pCols: List[Column] = partitionCols.map(s => col(s))
      val oCols: List[Column] = orderCols.map(s => col(s))
      // Create window
      val w = Window.partitionBy(pCols: _*).orderBy(oCols: _*)
      // Lag target
      var df2: DataFrame = df
      for (i <- 1 to lagNum) {
        df2 = df2.withColumn(s"lagged$i", lag(targetCol, i, null).over(w))
      }
      df2
    }
  }
}
