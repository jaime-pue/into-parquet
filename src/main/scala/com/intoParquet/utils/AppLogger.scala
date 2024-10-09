package com.intoParquet.utils

import org.apache.log4j.{Level, LogManager}

trait AppLogger {
    private val log = LogManager.getLogger(getClass.getName)

    protected def logInfo(msg: Any): Unit = {
        log.info(msg)
    }
    protected def logDebug(msg: Any): Unit = {
        log.debug(msg)
    }

    protected def logError(msg: Any): Unit = {
        log.error(msg)
    }

    protected def logError(msg: Any, cause: Throwable): Unit = {
        log.error(msg, cause)
    }
}
