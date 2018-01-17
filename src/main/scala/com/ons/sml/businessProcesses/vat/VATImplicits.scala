package com.ons.sml.businessProcesses.vat

import org.apache.spark.sql.DataFrame
import com.ons.sml.businessMethods.impl.BaseImpl.BaseMethodsImpl


object VATImplicits {

  val vatText = "VAT specific text"

  implicit class VATDataFrame(df: DataFrame) extends BaseMethodsImpl(df: DataFrame) {


    def runVATSpecificMethod() : DataFrame = {

      val vatCol = "vat_column"

      addConstantColumn(vatCol, vatText)

    }
  }
}
