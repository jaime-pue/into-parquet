/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.controller

import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.controller.FileController.splitFiles
import com.github.jaime.intoParquet.model.Files
import com.github.jaime.intoParquet.service.AppLogger
import com.github.jaime.intoParquet.service.FileLoader.readAllFilesFromRaw

import scala.util.Failure
import scala.util.Success
import scala.util.Try

class FileController(
    basePaths: BasePaths,
    csvFiles: Option[String],
    excludedFiles: Option[String] = None
) extends AppLogger {

    private val inputBasePath: String               = basePaths.inputBasePath
    private val includeFiles: Option[Array[String]] = setFiles(csvFiles)
    private val excludeFiles: Option[Array[String]] = setFiles(excludedFiles)

    def files: Option[Files] = {
        getFiles match {
            case Some(endFiles) =>
                val files = new Files(endFiles)
                logDebug(s"Files for processing: [${files.toString}]")
                Some(files)
            case None => None
        }
    }

    private def setFiles(listOfFiles: Option[String]): Option[Array[String]] = {
        listOfFiles match {
            case Some(value) => Some(splitFiles(value))
            case None        => None
        }
    }

    protected[controller] def getAllFilenamesFromFolder: Try[List[String]] = {
        Try(readAllFilesFromRaw(inputBasePath))
    }

    def getFiles: Option[Seq[String]] = {
        val files = getAllFilenamesFromFolder match {
            case Failure(exception) =>
                logError(exception.getMessage)
                return None
            case Success(value) => filterFiles(value)
        }
        if (files.isEmpty) {
            None
        } else { Some(files) }
    }

    protected[controller] def filterFiles(files: Seq[String]): Seq[String] = {
        val inclusiveFiles = includeFiles match {
            case Some(value) => files.filter(f => value.contains(f))
            case None        => files
        }
        val exclusiveFiles = excludeFiles match {
            case Some(value) => inclusiveFiles.filterNot(f => value.contains(f))
            case None        => inclusiveFiles
        }
        exclusiveFiles
    }

}

object FileController {
    protected[controller] def splitFiles(inputLine: String): Array[String] = {
        inputLine.split(",").map(_.trim).distinct
    }

    // java String isBlank method is java 11
    protected[controller] def isBlank(line: Option[String]): Boolean = {
        line.isEmpty || line.get.trim.isEmpty
    }
}
