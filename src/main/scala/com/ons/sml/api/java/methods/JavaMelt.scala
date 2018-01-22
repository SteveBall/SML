package com.ons.sml.api.java.methods

import com.ons.sml.businessMethods.methods.Melt
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class JavaMelt[K](trans: Melt){


  def melt1(input: DataFrame, id_vars: Seq[String], value_vars: Seq[String], var_name: String ="variable",
            value_name: String ="value") : DataFrame = {

    trans.melt1(input, id_vars, value_vars, var_name, value_name)
  }

}

object JavaMelt {

  def melt(df: Dataset[Row]) : JavaMelt[Melt] = {new JavaMelt(Melt.melt(df))}

}