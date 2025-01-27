/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.controller

import com.github.jaime.intoParquet.app.SparkBuilder
import com.github.jaime.intoParquet.service.AppLogger
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.configuration.ReaderConfiguration
import com.github.jaime.intoParquet.configuration.SparkConfiguration.configuration
import com.github.jaime.intoParquet.mapping.IntoBasePaths
import com.github.jaime.intoParquet.mapping.IntoCastMode
import com.github.jaime.intoParquet.mapping.transformer.AsController
import com.github.jaime.intoParquet.model.enumeration.CastMode

import scala.util.Failure
import scala.util.Success

class Controller(
    basePaths: BasePaths,
    recursiveRead: Boolean,
    failFast: Boolean,
    castMode: CastMode,
    csvFiles: Option[String]
) extends AppLogger {

    final def route(): Unit = {
        intoFileController.files match {
            case Some(csvFiles) => resolveExecution(csvFiles)
            case None           => logInfo(s"No file found in ${basePaths.inputBasePath}. Skipping")
        }
    }

    /** Gracefully stop current Spark Session even if a failure throws its exception */
    private def resolveExecution(csvFiles: Array[String]): Unit = {
        logInfo("Start batch")
        SparkBuilder.beforeAll(configuration)
        intoExecutionController(csvFiles).execution match {
            case Failure(exception) =>
                logError(s"""Something went wrong:
                            |${exception.getMessage}
                            |""".stripMargin)
                SparkBuilder.afterAll()
                throw exception
            case Success(_) =>
                logInfo("Job ended Ok!")
                SparkBuilder.afterAll()
        }
    }

    private def intoFileController: FileController = {
        logDebug("Create new File Controller class")
        new FileController(
          basePaths = basePaths,
          recursiveRead = recursiveRead,
          csvFiles = csvFiles
        )
    }

    private def intoExecutionController(files: Array[String]): ExecutionController = {
        logDebug("Create new Execution Controller class")
        new ExecutionController(
          csvFiles = files,
          basePaths = basePaths,
          castMode = castMode,
          failFast = failFast
        )
    }
}

object Controller {

    def into(obj: AsController): Controller = {
        val inputArgs = obj.into
        AppLogger.DebugMode = inputArgs.debugMode
        ReaderConfiguration.Separator = inputArgs.separator.getOrElse(",")
        new Controller(
          new BasePaths(new IntoBasePaths(inputArgs.inputDir, inputArgs.outputDir)),
          inputArgs.recursive,
          inputArgs.failFast,
          new IntoCastMode(inputArgs.fallBack, inputArgs.castMethod).mode,
          inputArgs.csvFile
        )
    }
}
