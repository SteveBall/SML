package com.ons.sml.api.java.methods

import com.ons.sml.businessMethods.methods.{Apportionment, Estimation}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class JavaEstimation[K](est: Estimation){


  def est1(df: DataFrame, inputCol : String, factor : Integer, outputCol : String) : DataFrame = {

    est.est1(df, inputCol, factor, outputCol)
  }

  def est2(df: DataFrame, inputCol : String, factor : Integer, outputCol : String) : DataFrame = {

    est.est2(df, inputCol, factor, outputCol)
  }


}

object JavaEstimation {

  def estimation(df: Dataset[Row]) : JavaEstimation[Estimation] = {new JavaEstimation(Estimation.estimation(df))}

}