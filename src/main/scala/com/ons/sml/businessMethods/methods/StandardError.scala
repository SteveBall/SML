package com.ons.sml.businessMethods.methods

import com.ons.sml.businessMethods.impl.StandardErrorImpl
import org.apache.spark.sql.DataFrame

class StandardError (val dfIn: DataFrame){

  if (dfIn == null ) throw new Exception("DataFrame cannot be null")

  import StandardErrorImpl._

  val defaultCol ="StandardError"

  private def mandatoryArgCheck(arg1:String, arg2:String, arg3:String) : Unit = {

    if ((arg1 == null) ||(arg2 == null) ||(arg3 == null)) throw new Exception("Missing mandatory argument")
  }

  def stdErr1(df: DataFrame, xCol: String, yCol: String, zCol: String,newColName: String = defaultCol): DataFrame={
    mandatoryArgCheck(xCol, yCol, zCol)

    val dF = if (df == null) dfIn else df
    val newColName1 =  if (newColName==null) defaultCol else newColName
    dF.checkColNames(Seq(xCol, yCol, zCol)).findStandardError(newColName1, xCol, yCol, zCol)
  }
}

object StandardError {

  def standardError(df: DataFrame) : StandardError = {new StandardError(df)}

}


