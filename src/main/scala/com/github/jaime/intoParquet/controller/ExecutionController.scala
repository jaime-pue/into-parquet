/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.controller

import com.github.jaime.intoParquet.app.SparkBuilder
import com.github.jaime.intoParquet.behaviour.AppLogger
import com.github.jaime.intoParquet.behaviour.Executor
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.configuration.SparkConfiguration.configuration
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

    final def buildSparkAndRun(): Unit = {
        SparkBuilder.beforeAll(configuration)
        execution match {
            case Success(_) =>
                logInfo("Job ended Ok!")
                SparkBuilder.afterAll()
            case Failure(exception) =>
                logError(s"""Something went wrong
                            |${exception.getMessage}
                            |""".stripMargin)
                throw exception
        }

    }

    protected[controller] def execution: Try[Unit] = {
        logDebug(s"Apply cast mode ${castMode.toString}")
        if (failFast) {
            failFastMode
        } else { ignoreErrorMode }
    }

    private def ignoreErrorMode: Try[Unit] = {
        logDebug("Skip errors if any")
        Success(this.csvFiles.foreach(e => {
            castElement(e).cast match {
                case Failure(exception) => logError(exception.getMessage)
                case Success(_)         =>
            }
        }))
    }

    private def failFastMode: Try[Unit] = {
        this.csvFiles.foreach(e => {
            castElement(e).cast match {
                case Failure(exception) => return Failure(exception)
                case Success(_)         =>
            }

        })
        Success()
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
