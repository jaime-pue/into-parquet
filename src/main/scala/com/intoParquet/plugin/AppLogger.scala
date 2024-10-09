package com.intoParquet.plugin

import org.apache.log4j.LogManager
import org.apache.log4j.Level

trait AppLogger {
  private val log = LogManager.getLogger(getClass.getName)
  log.setLevel(Level.INFO)

  protected def logInfo(msg: Any) = {
    log.info(msg)
  }
  
}
