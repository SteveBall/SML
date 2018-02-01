package com.ons.sml.businessMethods.impl

import BaseImpl.BaseMethodsImpl
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{max, min, sum, mean}

object ApportionmentImpl {

  implicit class ApportionmentMethodsImp(df: DataFrame) extends BaseMethodsImpl(df : DataFrame) {

    def app1(groupByCol : String, aggCol : String, appColName : String) : DataFrame = {

      df.groupBy(groupByCol).agg(max(aggCol).alias(appColName))
    }

    def app2(groupByCol : String, aggCol : String, appColName : String) : DataFrame = {

      df.groupBy(groupByCol).agg(min(aggCol).alias(appColName))
    }

    def app3(groupByCol : String, aggCol : String, appColName : String) : DataFrame = {

      df.groupBy(groupByCol).agg(sum(aggCol).alias(appColName))
    }

    def app4(groupByCol : String, aggCol : String, appColName : String) : DataFrame = {

      df.groupBy(groupByCol).agg(mean(aggCol).alias(appColName))
    }
  }

}
