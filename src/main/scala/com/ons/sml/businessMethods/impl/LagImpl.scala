
package com.ons.sml.businessMethods.impl

import com.ons.sml.businessMethods.impl.BaseImpl.BaseMethodsImpl
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag}

object LagImpl {

  implicit class LagMethodsImp(df: DataFrame) extends BaseMethodsImpl(df: DataFrame) {

    def lagFunc(partitionCols: List[String],
                orderCols: List[String], targetCol: String, lagNum: Int): DataFrame = {

      val pCols: List[Column] = partitionCols.map(s => col(s))
      val oCols: List[Column] = orderCols.map(s => col(s))
      // Create window
      val w = Window.partitionBy(pCols: _*).orderBy(oCols: _*)
      // Lag target
//      var df2: DataFrame = df
      for (i <- 1 to lagNum) {
        df.withColumn(s"lagged$i", lag(targetCol, i, null).over(w))
      }
      df
    }
  }
}
