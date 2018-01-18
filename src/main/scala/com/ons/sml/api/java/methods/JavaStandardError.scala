package com.ons.sml.api.java.methods

import com.ons.sml.businessMethods.methods.StandardError
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class JavaStandardError[K](stderr: StandardError) {

  def stdError1(df:DataFrame, xCol:String, yCol:String, zCol:String, newColName:String): DataFrame = {
    stderr.stdErr1(df, xCol, yCol, zCol, newColName)
  }

}

object JavaStandardError{
  def standardError(df: Dataset[Row]): JavaStandardError[StandardError]={
    new JavaStandardError(StandardError.standardError(df))
  }
}
