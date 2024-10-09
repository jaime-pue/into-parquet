package com.intoParquet.utils

import org.apache.log4j.{Level, LogManager}

trait AppLogger {
  private val log = LogManager.getLogger(getClass.getName)
  log.setLevel(Level.INFO)

  protected def logInfo(msg: Any) = {
    log.info(msg)
  }
  
}
