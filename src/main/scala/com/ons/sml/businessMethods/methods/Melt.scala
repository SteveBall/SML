package com.ons.sml.businessMethods.methods

import com.ons.sml.businessMethods.impl.MeltImpl
import org.apache.spark.sql.DataFrame


class Melt(val dfIn: DataFrame) {

  if (dfIn == null) throw new Exception("DataFrame cannot be null")

  import MeltImpl._

  def melt1(input: DataFrame, id_vars: List[String], value_vars: List[String], var_name: String = "variable",
            value_name: String = "value"): DataFrame = {

    val inputData: DataFrame = if (input == null) dfIn else input

    val checkCols: Seq[String] = id_vars++value_vars

    inputData.checkColNames(checkCols)
             .melt1(id_vars, value_vars, var_name, value_name)
  }

}

object Melt {

  def melt(df: DataFrame): Melt = new Melt(df)

}
