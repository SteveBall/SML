package com.ons.sml.api.java.methods
import java.util

import com.ons.sml.businessMethods.methods.SumCol
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import scala.collection.JavaConversions._

/** A class which converts Java type to Scala types where required.
  */

class JavaSumCol[K](app: SumCol){


  def sumcol1(input: DataFrame, groupbycols: util.ArrayList[String], sumcol: String): DataFrame = {

    app.sc1(input,groupbycols.toSeq, sumcol)

  }


}

object JavaSumCol {

  def sumcol(df: Dataset[Row]): JavaSumCol[SumCol] = {
    new JavaSumCol(SumCol.sumCol(df))
  }

}