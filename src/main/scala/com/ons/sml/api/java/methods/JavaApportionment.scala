
package com.ons.sml.api.java.methods

import com.ons.sml.businessMethods.methods.Apportionment
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class JavaApportionment[K](app: Apportionment){


  def app1(df: DataFrame, groupByCol : String, aggCol : String, appCol : String) : DataFrame = {

    app.app1(df, groupByCol, aggCol, appCol)
  }

  def app2(df: DataFrame, groupByCol : String, aggCol : String, appCol : String) : DataFrame = {

    app.app2(df, groupByCol, aggCol, appCol)
  }

  def app3(df: DataFrame, groupByCol : String, aggCol : String, appCol : String) : DataFrame = {

    app.app3(df, groupByCol, aggCol, appCol)
  }

  def app4(df: DataFrame, groupByCol : String, aggCol : String, appCol : String) : DataFrame = {

    app.app4(df, groupByCol, aggCol, appCol)
  }


}

object JavaApportionment {

  def apportionment(df: Dataset[Row]) : JavaApportionment[Apportionment] = {new JavaApportionment(Apportionment.apportionment(df))}

}