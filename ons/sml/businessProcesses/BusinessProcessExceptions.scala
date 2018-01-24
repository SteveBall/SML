package com.ons.sml.businessProcesses

case class ONSRuntimeException(cause: String) extends RuntimeException(cause)
