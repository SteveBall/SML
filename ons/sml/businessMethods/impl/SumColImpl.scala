package com.ons.sml.businessMethods.impl

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.{functions => F}
//import org.apache.spark.sql.expressions.Window
//import org.apache.spark.sql.functions.{col, when, row_number}
import BaseImpl.BaseMethodsImpl

/** A instance of the implicit class: SumCol */
object SumColImpl {

  /** The Implicit Class:
    *
    * @param df The Spark DataFrame in Play
    */

  implicit class SumColMethodsImpl(df: DataFrame) extends BaseMethodsImpl(df : DataFrame){

    /** This method will perform a grouping and sum procedure on the DataFrame in play.
      *
      * This method adds a column to a dataframe containing duplicate markers.
      *
      * @author Original Author Unknown - SML Build by Martyn Spooner.
      * @version 0.1
      * @param groupByCols A list of the column(s) to perform grouping on based around sum column.
      * @param sumCol The column to perform the sum upon within the DataFrame passed.
      * @return DataFrame.
      */

    def sumCol(groupByCols: Seq[String], sumCol: String): DataFrame = {
      /// grouped by:
      val grouped_df = df.groupBy(groupByCols.map(F.col(_)): _*).agg(F.sum(sumCol).alias("sum_of_" + sumCol))
      // join this back onto the original dataframe
      val summed_df = df.join(grouped_df, groupByCols, "inner")
      summed_df
    }

  }

}