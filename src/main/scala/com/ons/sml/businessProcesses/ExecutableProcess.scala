package com.ons.sml.businessProcesses

import org.apache.spark.sql.DataFrame

import scala.reflect.ClassTag

trait ExecutableProcess extends BusinessProcess{

  @throws(classOf[ONSRuntimeException])
  def runProcess(runId : String) : DataFrame

}
