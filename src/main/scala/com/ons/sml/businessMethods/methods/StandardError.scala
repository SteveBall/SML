package com.ons.sml.businessMethods.methods

import com.ons.sml.businessMethods.impl.StandardErrorImpl
import org.apache.spark.sql.DataFrame

class StandardError (val dfIn: DataFrame){

  if (dfIn == null ) throw new Exception("DataFrame cannot be null")

  import StandardErrorImpl._

  val defaultCol ="StandardError"

  private def mandatoryArgCheck(arg1 : String, arg2:String, arg3:String, arg4:String) : Unit = {

    if ((arg1 == null) || (arg2 == null) ||(arg3 == null) ||(arg4 == null)) throw new Exception("Missing mandatory argument")
  }

  def stdErr1(df: DataFrame, newColName: String = defaultCol, xCol: String, yCol: String, zCol: String): DataFrame={
    mandatoryArgCheck(newColName, xCol, yCol, zCol)

    val dF = if (df == null) dfIn else df

    dF.checkColNames(Seq(xCol, yCol, zCol)).findStandardError(df, newColName, xCol, yCol, zCol)
  }
}

object StandardError {

  def standardError(df: DataFrame) : StandardError = {new StandardError(df)}

}