package com.ons.sml.businessMethods.methods

import com.ons.sml.businessMethods.impl.DuplicateImpl
import org.apache.spark.sql.DataFrame

/** This class contains the Duplicate Marking functions
  *
  * @param dfIn The DataFrame that the Duplicate Marking functions will act on
  */
class Duplicate (val dfIn: DataFrame) {

  if (dfIn == null) throw new Exception("DataFrame cannot be null")

  import DuplicateImpl._

  val defaultCol = "DuplicateMarking"

  // TODO Replace the below checks with Options
  private def mandatoryArgCheck(arg1 : List[String], arg2 : List[String], arg3 : String) : Unit = {

    if ((arg1 == null) || (arg2 == null) || (arg3 == null)) throw new Exception("Missing mandatory argument")
  }

  /** A standard duplicate marking function
    *
    * @param df A optional DataFrame that will be used inplace of the DataFrame given to the class
    * @param partCol A list of the column(s) to check for duplicates within
    * @param ordCol A list of the column(s) to order by. Later records are marked as duplicates
    * @param new_col The name of the new column that will contain the markers
    * @return DataFrame
    */
  def dm1(df: DataFrame, partCol: List[String], ordCol: List[String], new_col: String = defaultCol) : DataFrame = {

    mandatoryArgCheck(partCol, ordCol, new_col)

    val dF = if (df == null) dfIn else df

    dF.checkColNames(Seq(partCol, ordCol).flatten)
      .duplicateMarking(partCol, ordCol, new_col)
  }

}

/** A Instance of the Duplicate class */
object Duplicate {

  def duplicate(df: DataFrame) : Duplicate = new Duplicate(df)
}
