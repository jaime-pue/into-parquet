/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.behaviour

import org.apache.logging.log4j.LogManager

trait AppLogger {
    private val log = LogManager.getLogger(getClass.getName)

    protected def logInfo(msg: Any): Unit = {
        log.info(msg)
    }
    protected def logDebug(msg: Any): Unit = {
        log.debug(msg)
    }

    protected def logWarning(msg: Any): Unit = {
        log.warn(msg)
    }

    protected def logError(msg: Any): Unit = {
        log.error(msg)
    }
}
