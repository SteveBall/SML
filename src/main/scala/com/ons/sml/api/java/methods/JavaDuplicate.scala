
package com.ons.sml.api.java.methods
import java.util

import scala.collection.JavaConversions._
import com.ons.sml.businessMethods.methods.Duplicate
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class JavaDuplicate[K](dm: Duplicate) {
  // TODO find out how to implement default values
  def dm1(df: Dataset[Row], partCol: util.ArrayList[String], ordCol: util.ArrayList[String],
          new_col: String): DataFrame = {
    dm.dm1(df, partCol.toList, ordCol.toList, new_col)
  }
}
object JavaDuplicate{

  def duplicate(df: Dataset[Row]) : JavaDuplicate[Duplicate] = {
    new JavaDuplicate(Duplicate.duplicate(df))}
}