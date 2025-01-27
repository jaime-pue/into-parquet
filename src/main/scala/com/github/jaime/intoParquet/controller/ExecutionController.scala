/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.controller

import com.github.jaime.intoParquet.behaviour.AppLogger
import com.github.jaime.intoParquet.behaviour.Executor
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.model.enumeration.CastMode
import com.github.jaime.intoParquet.model.enumeration.InferSchema
import com.github.jaime.intoParquet.model.enumeration.ParseSchema
import com.github.jaime.intoParquet.model.enumeration.RawSchema
import com.github.jaime.intoParquet.model.execution.Infer
import com.github.jaime.intoParquet.model.execution.Parse
import com.github.jaime.intoParquet.model.execution.Raw

import scala.util.Failure
import scala.util.Success
import scala.util.Try

class ExecutionController(
    csvFiles: Array[String],
    basePaths: BasePaths,
    castMode: CastMode,
    failFast: Boolean
) extends AppLogger {

    protected[controller] def execution: Try[Unit] = {
        logInfo(s"Apply cast mode ${castMode.toString}")
        Success(this.csvFiles.foreach(files => {
            castElement(files).cast match {
                case Failure(exception) =>
                    if (failFast) {
                        return Failure(exception)
                    } else {
                        logError(exception.getMessage)
                    }
                case Success(_) =>
            }
        }))
    }

    private def castElement(element: String): Executor = {
        logInfo(s"Start job for: ${element}")
        this.castMode match {
            case RawSchema      => new Raw(element, basePaths)
            case InferSchema    => new Infer(element, basePaths)
            case e: ParseSchema => new Parse(element, basePaths, e.fallBack)
        }
    }
}
