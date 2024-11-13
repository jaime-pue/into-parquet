/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.router

import com.github.jaime.intoParquet.behaviour.AppLogger
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.controller.ExecutionController
import com.github.jaime.intoParquet.controller.FileController
import com.github.jaime.intoParquet.model.enumeration.CastMode
import com.github.jaime.intoParquet.model.enumeration.ParseSchema
import com.github.jaime.intoParquet.utils.Parser.InputArgs

class Router(inputArgs: InputArgs) extends AppLogger {
    final def route(): Unit = {
        intoFileController.files match {
            case Some(csvFiles) => intoExecutionController(csvFiles).buildSparkAndRun()
            case None        => logInfo(s"No file found in ${basePaths.inputBasePath}. Skip")
        }
    }

    private val recursiveRead: Boolean = inputArgs.recursive
    private val basePaths: BasePaths   = intoBasePaths
    private val failFast: Boolean      = inputArgs.failFast

    private def intoBasePaths: BasePaths = {
        new BasePaths(inputArgs.inputDir, inputArgs.outputDir)
    }

    private def intoCastMethod: CastMode = {
        inputArgs.fallBack match {
            case Some(value) => new ParseSchema(value)
            case None        => inputArgs.castMethod
        }
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
