package com.ons.sml.persistence

import org.apache.spark.sql.DataFrame

trait DebugTrait {

  def writeToConsole(df: DataFrame): DataFrame = {

    df.printSchema()

    df.collect.foreach(println)

    df
  }
}
