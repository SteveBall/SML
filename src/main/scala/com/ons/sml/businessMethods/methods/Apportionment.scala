package com.ons.sml.businessMethods.methods

import com.ons.sml.businessMethods.impl.ApportionmentImpl
import org.apache.spark.sql.{DataFrame, Dataset, Row}


class Apportionment (val dfIn: DataFrame){

  if (dfIn == null ) throw new Exception("DataFrame cannot be null")

  import ApportionmentImpl._

  val defaultCol = "AppResult"

  private def mandatoryArgCheck(arg1 : String, arg2 : String) : Unit = {

    if ((arg1 == null) || (arg2 == null)) throw new Exception("Missing mandatory argument")
  }

  def app1(df: DataFrame, groupByCol : String, aggCol : String, appCol : String = defaultCol) : DataFrame = {

    mandatoryArgCheck(groupByCol, aggCol)

    val dF = if (df == null) dfIn else df

    dF.checkColNames(Seq(groupByCol, aggCol))
      .app1(groupByCol, aggCol, appCol)
  }


  def app2(df: DataFrame, groupByCol : String, aggCol : String, appCol : String = defaultCol) : DataFrame = {

    mandatoryArgCheck(groupByCol, aggCol)

    val dF = if (df == null) dfIn else df

    dF.checkColNames(Seq(groupByCol, aggCol))
      .app2(groupByCol, aggCol, appCol)
  }

  def app3(df: DataFrame, groupByCol : String, aggCol : String, appCol : String = defaultCol) : DataFrame = {

    mandatoryArgCheck(groupByCol, aggCol)

    val dF = if (df == null) dfIn else df

    dF.checkColNames(Seq(groupByCol, aggCol))
      .app3(groupByCol, aggCol, appCol)
  }

  def app4(df: DataFrame, groupByCol : String, aggCol : String, appCol : String = defaultCol) : DataFrame = {

    mandatoryArgCheck(groupByCol, aggCol)

    val dF = if (df == null) dfIn else df

    dF.checkColNames(Seq(groupByCol, aggCol))
      .app4(groupByCol, aggCol, appCol)
  }
}

object Apportionment {

  def apportionment(df: DataFrame) : Apportionment = new Apportionment(df)
}
