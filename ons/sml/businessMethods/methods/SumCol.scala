package com.ons.sml.businessMethods.methods

import com.ons.sml.businessMethods.impl.SumColImpl
import org.apache.spark.sql.DataFrame

/** This class executes the SumCol grouping and sum function.
  *
  * @param dfIn The DataFrame that the function will act on.
  */

class SumCol (val dfIn: DataFrame) {

  if (dfIn == null) throw new Exception("DataFrame cannot be null")

  import SumColImpl._

  //val defaultCol = "

  // TODO Replace the below checks with Options
  private def mandatoryArgCheck(arg1: Seq[String], arg2: String): Unit = {
    if ((arg1 == null) || (arg2 == null)) throw new Exception("Missing mandatory argument")
  }

  /** Generic sum and group-by operation.
    *
    * @param df The DataFrame that will be used in-place of the DataFrame given to the class.
    * @param groupByCols A list of the column(s) to perform grouping on based around sum column.
    * @param sumCol The column to perform the sum upon within the DataFrame passed.
    * @return DataFrame.
    */

  def sc1(df: DataFrame, groupByCols: Seq[String], sumCol: String): DataFrame = {

    // mandatory argument check for NULL exclusion
    mandatoryArgCheck(groupByCols, sumCol)
    val dF = if (df == null) dfIn else df
    dF.sumCol(groupByCols, sumCol)


  }


}

/** A Instance of the SumCol class */
object SumCol {

  def sumCol(df: DataFrame) : SumCol = new SumCol(df)
}







