/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.behaviour

import com.github.jaime.intoParquet.behaviour.AppLogger.DebugMode
import org.apache.logging.log4j.LogManager

trait AppLogger {
    private val log = LogManager.getLogger(getClass.getName)

    protected def logInfo(msg: Any): Unit = {
        log.info(msg)
    }
    protected def logDebug(msg: Any): Unit = {
        if (DebugMode) {
            log.debug(msg)
        }
    }

    protected def logWarning(msg: Any): Unit = {
        log.warn(msg)
    }

    protected def logError(msg: Any): Unit = {
        log.error(msg)
    }
}

object AppLogger {
    /** Controls if debug level should be displayed in a programmatic way,
     * so user can activate some verbosity with just a flag and not compiling
     * again or editing log4j.xml file.
     *
     * Even if this is a var, it should be treated as a constant value.
     * Once is set, leave it as is.
     *
     * By default is set to `false`*/
    var DebugMode: Boolean = _
}
