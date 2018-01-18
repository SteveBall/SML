
package com.ons.sml.businessMethods.impl

import BaseImpl.BaseMethodsImpl
import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.functions.{col}

object EstimationImpl {

  implicit class EstimationMethodsImp(df: DataFrame) extends BaseMethodsImpl(df : DataFrame) {

    def est1(inputCol : String, factor : Integer, outputCol : String) : DataFrame = {

      df.withColumn(outputCol, col(inputCol) * factor)
    }

    def est2(inputCol : String, factor : Integer, outputCol : String) : DataFrame = {

      df.withColumn(outputCol, col(inputCol) / factor)
    }

  }

}
