package com.ons.sml.persistence

import org.apache.spark.sql.DataFrame

trait CSVTrait extends BasePersistenceTrait {

  def saveAsCSV(df: DataFrame, fLoc : String, fName: String): DataFrame = {
    df.write.format("com.databricks.spark.csv").option("header", "true")
      .save(OUTPUTS + fLoc + "/" + fName + ".csv")

    df
  }
}
