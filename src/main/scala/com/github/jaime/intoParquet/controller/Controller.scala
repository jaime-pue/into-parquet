/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.controller

import com.github.jaime.intoParquet.behaviour.AppLogger
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.mapping.IntoBasePaths
import com.github.jaime.intoParquet.mapping.IntoCastMode
import com.github.jaime.intoParquet.mapping.transformer.AsController
import com.github.jaime.intoParquet.model.enumeration.CastMode
import com.github.jaime.intoParquet.service.Parser.InputArgs

class Controller(inputArgs: InputArgs) extends AppLogger {
    private val recursiveRead: Boolean = inputArgs.recursive
    private val basePaths: BasePaths   = intoBasePaths
    private val failFast: Boolean      = inputArgs.failFast

    def this(fromArgs: AsController) = {
        this(fromArgs.into)
    }

    final def route(): Unit = {
        intoFileController.files match {
            case Some(csvFiles) => intoExecutionController(csvFiles).buildSparkAndRun()
            case None           => logInfo(s"No file found in ${basePaths.inputBasePath}. Skip")
        }
    }

    private def intoBasePaths: BasePaths = {
        new BasePaths(new IntoBasePaths(inputArgs.inputDir, inputArgs.outputDir))
    }

    private def intoCastMethod: CastMode = {
        new IntoCastMode(inputArgs.fallBack, inputArgs.castMethod).mode
    }

    private def intoFileController: FileController = {
        logDebug("Create new File Controller class")
        new FileController(
          basePaths = basePaths,
          recursiveRead = recursiveRead,
          csvFiles = inputArgs.csvFile
        )
    }

    private def intoExecutionController(files: Array[String]): ExecutionController = {
        logDebug("Create new Execution Controller class")
        new ExecutionController(
          csvFiles = files,
          basePaths = basePaths,
          castMode = intoCastMethod,
          failFast = failFast
        )
    }
}
