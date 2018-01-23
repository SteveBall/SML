package com.ons.sml.api.java.methods

import com.ons.sml.businessMethods.methods.Melt
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import java.util
import scala.collection.JavaConversions._

class JavaMelt[K](trans: Melt){


  def melt1(input: DataFrame, id_vars: util.ArrayList[String], value_vars: util.ArrayList[String],
            var_name: String ="variable", value_name: String ="value") : DataFrame = {

    trans.melt1(input, id_vars.toSeq, value_vars.toSeq, var_name, value_name)
  }

}

object JavaMelt {

  def melt(df: Dataset[Row]) : JavaMelt[Melt] = {new JavaMelt(Melt.melt(df))}

}