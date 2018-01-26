package com.ons.sml.businessMethods.methods

import com.ons.sml.businessMethods.impl.MeltImpl
import org.apache.spark.sql.DataFrame

/** Class contaning functions that call each version of the melt method(s).
  *
  * @param dfIn - DataFrame
  */
class Melt(val dfIn: DataFrame) {

  if (dfIn == null) throw new Exception("DataFrame cannot be null")

  import MeltImpl._

  /** Scala function that calls version 1 of the melt method.
    *
    * @param input DataFrame        - The input DataFrame
    * @param id_vars Seq[String]    - Column(s) which are used as unique identifiers
    * @param value_vars Seq[String] - Column(s) which are being unpivoted
    * @param var_name String        - The name of a new column, which holds all the value_vars names, defaulted to variable.
    * @param value_name String      - The name of a new column, which holds all the values of value_vars column(s),
    *                                 defaulted to value.@param input
    * @return DataFrame
    */
  def melt1(input: DataFrame = dfIn, id_vars: Seq[String], value_vars: Seq[String], var_name: String = "variable",
            value_name: String = "value"): DataFrame = {

    val checkCols: Seq[String] = id_vars++value_vars

    input.checkColNames(checkCols)
             .melt1(id_vars, value_vars, var_name, value_name)
  }

}

object Melt {

  def melt(df: DataFrame): Melt = new Melt(df)

}
