package com.ons.sml.api.java.methods
import java.util

import scala.collection.JavaConversions._
import com.ons.sml.businessMethods.methods.Duplicate
import org.apache.spark.sql.{DataFrame, Dataset, Row}

/** A class which converts Java type to Scala types.
  *
  * @param dm
  * @tparam K
  */

class JavaDuplicate[K](dm: Duplicate) {
  // TODO find out how to implement default values
  /** This converts Java to Scala for dm1.
    *
    * @param df
    * @param partCol A Java ArrayList of the column(s) to check for duplicates within
    * @param ordCol A Java ArrayList of the column(s) to order by. Later records are marked as duplicates
    * @param new_col The name of the new column that will contain the markers
    * @return Scala DataFrame
    */
  def dm1(df: Dataset[Row], partCol: util.ArrayList[String], ordCol: util.ArrayList[String],
          new_col: String): DataFrame = {
            dm.dm1(df, partCol.toList, ordCol.toList, new_col)
  }
}

/**This is a Java duplicate object.*/
object JavaDuplicate{
  /**This create a new duplicate object.
    *
    * @param df
    * @return Java duplicate object.
    */
  def duplicate(df: Dataset[Row]) : JavaDuplicate[Duplicate] = {
    new JavaDuplicate(Duplicate.duplicate(df))}
}