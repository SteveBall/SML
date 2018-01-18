package com.ons.sml.api.java.methods

import com.ons.sml.businessMethods.methods.Duplicate
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class JavaDuplicate[K](dm: Duplicate) {
  // TODO find out how to implement default values
  def dm1(df: DataFrame, partCol: List[String], ordCol: List[String], new_col: String): DataFrame = {
    dm.dm1(df, partCol, ordCol, new_col)
  }
}
object JavaDuplicate{

  def duplicate(df: Dataset[Row]) : JavaDuplicate[Duplicate] = {
    new JavaDuplicate(Duplicate.duplicate(df))}
}