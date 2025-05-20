/*
 * IntoParquet Copyright (c) 2025 Jaime Alvarez
 */

package com.github.jaime.intoParquet.controller

import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.controller.HandleFile.setFiles
import com.github.jaime.intoParquet.model.Files
import com.github.jaime.intoParquet.service.AppLogger
import com.github.jaime.intoParquet.service.FileLoader.readAllFilesFromRaw

import scala.util.Try

abstract class HandleFile(
    basePaths: BasePaths,
    csvFiles: Option[String],
    excludedFiles: Option[String]
) extends AppLogger {

    private val inputBasePath: String               = basePaths.inputBasePath
    private val includeFiles: Option[Array[String]] = setFiles(csvFiles)
    private val excludeFiles: Option[Array[String]] = setFiles(excludedFiles)


    protected[controller] def getAllFilenamesFromFolder: Try[List[String]] = {
        Try(readAllFilesFromRaw(inputBasePath))
    }

    def getFiles: Option[Files] = {
        val files = filterFiles(getRawFileNames)
        if (files.isEmpty) {
            None
        } else {
            Some(new Files(files))
        }
    }

    def getRawFileNames: Seq[String]


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

object HandleFile {
    protected[controller] def splitFiles(inputLine: String): Array[String] = {
        inputLine.split(",").map(_.trim).distinct
    }

    // java String isBlank method is java 11
    protected[controller] def isBlank(line: Option[String]): Boolean = {
        line.isEmpty || line.get.trim.isEmpty
    }

    protected[controller] def setFiles(listOfFiles: Option[String]): Option[Array[String]] = {
        listOfFiles match {
            case Some(value) => Some(splitFiles(value))
            case None        => None
        }
    }
}
