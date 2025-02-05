/*
 * IntoParquet Copyright (c) 2025 Jaime Alvarez
 */

package com.github.jaime.intoParquet.controller.fail

import com.github.jaime.intoParquet.app.SparkBuilder
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.controller.HandleExecution
import com.github.jaime.intoParquet.model.Files
import com.github.jaime.intoParquet.model.enumeration.CastMode
import com.github.jaime.intoParquet.model.execution.Executor

import scala.util.Failure
import scala.util.Success

class FailFastExecution(csvFiles: Files, basePaths: BasePaths, castMode: CastMode)
    extends HandleExecution(csvFiles, basePaths, castMode) {

    /** Gracefully stop current Spark Session even if a failure throws its exception */
    override protected def processEachFile(e: Executor): Unit = {
        e.cast match {
            case Failure(exception) =>
                logError(s"""Something went wrong:
                            |${exception.getMessage}
                            |""".stripMargin)
                SparkBuilder.afterAll()
                throw exception
            case Success(_) =>
        }
    }
}
