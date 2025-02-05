/*
 * IntoParquet Copyright (c) 2025 Jaime Alvarez
 */

package com.github.jaime.intoParquet.controller.ignore

import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.controller.HandleExecution
import com.github.jaime.intoParquet.model.Files
import com.github.jaime.intoParquet.model.enumeration.CastMode
import com.github.jaime.intoParquet.model.execution.Executor

import scala.util.Failure
import scala.util.Success

class IgnoreExecution(csvFiles: Files, basePaths: BasePaths, castMode: CastMode)
    extends HandleExecution(csvFiles, basePaths, castMode) {

    override protected def processEachFile(e: Executor): Unit = {
        e.cast match {
            case Failure(exception) => logError(exception.getMessage)
            case Success(_)         =>
        }
    }
}
