/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.controller

import com.github.jaime.intoParquet.behaviour.AppLogger
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.configuration.ReaderConfiguration
import com.github.jaime.intoParquet.mapping.IntoBasePaths
import com.github.jaime.intoParquet.mapping.IntoCastMode
import com.github.jaime.intoParquet.mapping.transformer.AsController
import com.github.jaime.intoParquet.model.enumeration.CastMode

class Controller(
    basePaths: BasePaths,
    recursiveRead: Boolean,
    failFast: Boolean,
    castMode: CastMode,
    csvFiles: Option[String]
) extends AppLogger {

    final def route(): Unit = {
        intoFileController.files match {
            case Some(csvFiles) =>
                intoExecutionController(csvFiles).buildSparkAndRun()
            case None => logInfo(s"No file found in ${basePaths.inputBasePath}. Skipping")
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
