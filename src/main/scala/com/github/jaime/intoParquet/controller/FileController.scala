/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.controller

import com.github.jaime.intoParquet.behaviour.AppLogger
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.controller.FileController.splitFiles
import com.github.jaime.intoParquet.service.FileLoader
import com.github.jaime.intoParquet.service.FileLoader.readAllFilesFromRaw

import scala.util.Failure
import scala.util.Success
import scala.util.Try

class FileController(basePaths: BasePaths, recursiveRead: Boolean, csvFiles: Option[String])
    extends AppLogger {

    private val inputBasePath: String     = basePaths.inputBasePath
    private val emptyArray: Array[String] = Array()

    def files: Option[Array[String]] = {
        val csvFiles = Try(loadFiles) match {
            case Success(value) => value
            case Failure(exception) =>
                logError(exception.getMessage)
                return None
        }
        csvFiles.length match {
            case 0 => None
            case _ => Some(csvFiles)
        }
    }

    private def loadFiles: Array[String] = {
        if (recursiveRead) {
            getAllFilenamesFromFolder
        } else {
            getFilenamesFromInputLine
        }
    }

    protected[controller] def getAllFilenamesFromFolder: Array[String] = {
        readAllFilesFromRaw(inputBasePath)
    }

    protected[controller] def getFilenamesFromInputLine: Array[String] = {
        csvFiles match {
            case Some(value) =>
                FileLoader.filesExists(inputBasePath, splitFiles(value))
            case None => emptyArray
        }
    }
}

object FileController {
    protected[controller] def splitFiles(inputLine: String): Array[String] = {
        if (inputLine.isBlank) {
            Array()
        } else {
            inputLine.split(",").map(_.trim).distinct
        }
    }
}
