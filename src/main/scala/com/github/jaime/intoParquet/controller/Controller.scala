/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.controller

import com.github.jaime.intoParquet.app.SparkBuilder
import com.github.jaime.intoParquet.service.AppLogger
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.configuration.ReaderConfiguration
import com.github.jaime.intoParquet.configuration.SparkConfiguration.configuration
import com.github.jaime.intoParquet.controller.execution.FailFastExecution
import com.github.jaime.intoParquet.controller.execution.IgnoreExecution
import com.github.jaime.intoParquet.controller.file.FailFastFile
import com.github.jaime.intoParquet.controller.file.IgnoreFile
import com.github.jaime.intoParquet.mapping.IntoBasePaths
import com.github.jaime.intoParquet.mapping.IntoCastMode
import com.github.jaime.intoParquet.mapping.transformer.AsController
import com.github.jaime.intoParquet.model.Files
import com.github.jaime.intoParquet.model.enumeration.CastMode

import scala.util.Failure
import scala.util.Success

class Controller(
    basePaths: BasePaths,
    failFast: Boolean,
    castMode: CastMode,
    csvFiles: Option[String],
    excludeFiles: Option[String] = None
) extends AppLogger {

    final def route(): Unit = {
        intoFileController.getFiles match {
            case Some(csvFiles) =>
                logDebug(s"Files for processing: [${csvFiles.toString}]")
                resolveExecution(csvFiles)
            case None => logInfo(s"No file found in ${basePaths.inputBasePath}. Skipping")
        }
    }

    /** Gracefully stop current Spark Session even if a failure throws its exception */
    private def resolveExecution(csvFiles: Files): Unit = {
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

    protected[controller] def intoFileController: HandleFile = {
        logDebug("Create new File Controller class")
        val fileController = if (failFast) {
            FailFastFile
        } else {
            IgnoreFile
        }
        fileController.buildFrom(basePaths, csvFiles, excludeFiles)
    }

    private def intoExecutionController(files: Files): HandleExecution = {
        logDebug("Create new Execution Controller class")
        val executionController = if (failFast) {
            FailFastExecution
        } else {
            IgnoreExecution
        }
        executionController.buildFrom(files, basePaths, castMode)
    }
}

object Controller {

    def into(obj: AsController): Controller = {
        val inputArgs = obj.into
        AppLogger.DebugMode = inputArgs.debugMode
        ReaderConfiguration.Separator = inputArgs.separator.getOrElse(",")
        new Controller(
          new BasePaths(new IntoBasePaths(inputArgs.inputDir, inputArgs.outputDir)),
          inputArgs.failFast,
          new IntoCastMode(inputArgs.fallBack, inputArgs.castMethod).mode,
          inputArgs.csvFile
        )
    }
}
