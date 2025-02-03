/*
 * IntoParquet Copyright (c) 2025 Jaime Alvarez
 */

package com.github.jaime.intoParquet.controller.file

import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.controller.HandleFile
import com.github.jaime.intoParquet.exception.NoFileFoundException
import com.github.jaime.intoParquet.model.Files

import scala.util.Failure
import scala.util.Success

class FailFastFile(
    basePaths: BasePaths,
    csvFiles: Option[String],
    excludedFiles: Option[String]
) extends HandleFile(basePaths, csvFiles, excludedFiles) {
    override def getFiles: Option[Files] = {
        val files = getAllFilenamesFromFolder match {
            case Failure(exception) =>
                logError(exception.getMessage)
                throw exception
            case Success(value) => filterFiles(value)
        }
        if (files.isEmpty) {
            throw new NoFileFoundException(basePaths.inputBasePath)
        } else { Some(new Files(files)) }
    }
}

object FailFastFile extends Builder[FailFastFile] {
    override def buildFrom(
        basePaths: BasePaths,
        csvFiles: Option[String],
        excludedFiles: Option[String]
    ): FailFastFile = {
        new FailFastFile(basePaths, csvFiles, excludedFiles)
    }
}
