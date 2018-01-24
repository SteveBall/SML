package com.ons.sml.businessMethods.methods

import com.ons.sml.businessMethods.impl.StandardErrorImpl
import org.apache.spark.sql.DataFrame

/** This is the class that is called by the object at the bottom.
  *
  * @param dfIn DataFrame - The dataframe that the standard error method will be applied to.
  */
class StandardError (val dfIn: DataFrame){

  if (dfIn == null ) throw new Exception("DataFrame cannot be null")

  import StandardErrorImpl._

  val defaultCol ="StandardError"

  /** THis function checks that all the arguments provided have a values in them instead of null, if there are
    * any nulls then this throws an error.
    *
    * @param arg1 String - This is the first argument that's passed into the standard error function.
    * @param arg2 String - This is the second argument that's passed into the standard error function.
    * @param arg3 String - This is the third argument that's passed into the standard error function.
    */
  private def mandatoryArgCheck(arg1:String, arg2:String, arg3:String) : Unit = {

    if ((arg1 == null) ||(arg2 == null) ||(arg3 == null)) throw new Exception("Missing mandatory argument")
  }

  /** This function first checks that the arguments given are all there, if any of them are null/missing then it
    * throws an exception. Then if decides which dataframe to use, the class one or the function one, if the function
    * one is there then it will take that one, otherwise it takes the class one.
    *
    * Next the newColName is checked to see if it is null, if it is then the defaultCol is used instead. You can also
    * leave this variable blank and it will choose the defaultCol.
    *
    * The dataframe is then checked to see if all the columns names provided are in the dataframe, if not then this
    * throws an error. If they are in the dataframe then the findStandardError is called to add the standard error
    * as a column in the dataframe.
    *
    * The standard error is calculated using the formula:
    *
    *                             {{{ stdError = sqrt{ (x/y)*(x-y) / (y-1) } * z  }}}
    *
    * @param df  DataFrame - The dataframe that the standard error method will be applied to.
    * @param xCol String - The column to be used as X in the calculation
    * @param yCol String - The column to be used as Y in the calculation
    * @param zCol String - The column to be used as Z in the calculation
    * @param newColName String - This is what the standard error column is called, it can be defaulted to StandardError
    * @return DataFrame
    */
  def stdErr1(df: DataFrame, xCol: String, yCol: String, zCol: String,newColName: String = defaultCol): DataFrame={
    mandatoryArgCheck(xCol, yCol, zCol)

    val dF = if (df == null) dfIn else df
    val newColName1 =  if (newColName==null) defaultCol else newColName
    dF.checkColNames(Seq(xCol, yCol, zCol)).findStandardError(newColName1, xCol, yCol, zCol)
  }
}

/** This object calls the class above, this is what is called in order to use the class.
  *
  */
object StandardError {

  def standardError(df: DataFrame) : StandardError = {new StandardError(df)}

}
