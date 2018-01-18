package com.ons.sml.businessMethods.impl

import com.ons.sml.businessMethods.impl.BaseImpl.BaseMethodsImpl
import org.apache.spark.sql.{DataFrame, functions => F}

object StandardErrorImpl {

  implicit class StandardErrorMethodsImp(df: DataFrame) extends BaseMethodsImpl(df : DataFrame) {
    /** This function will add an extra column on to a DataFrame containing the standard error.
      *
      * This is calculated using the formula:
      *
      *                             {{{ stdError = sqrt{ (x/y)*(x-y) / (y-1) } * z  }}}
      *
      * @author Stuart Russell
      * @version 1.0
      * @param newColName - The name of the standard error column
      * @param xCol - The column to be used as X in the calculation
      * @param yCol - The column to be used as Y in the calculation
      * @param zCol - The column to be used as Z in the calculation
      * @return - DataFrame
      */
    def findStandardError(newColName: String, xCol: String, yCol: String, zCol: String): DataFrame = {
      val standard_df = df.withColumn(newColName, F.sqrt((F.col(xCol) / F.col(yCol)) * ((F.col(xCol) - F.col(yCol)) /
        (F.col(yCol) - 1))) * F.col(zCol))
      standard_df
    }
  }

}
