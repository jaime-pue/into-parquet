/*
 * IntoParquet Copyright (c) 2025 Jaime Alvarez
 */

package com.github.jaime.intoParquet.controller.file

import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.controller.HandleFile
import com.github.jaime.intoParquet.model.Files

import scala.util.Failure
import scala.util.Success

class IgnoreFile(
    basePaths: BasePaths,
    csvFiles: Option[String],
    excludedFiles: Option[String]
) extends HandleFile(basePaths, csvFiles, excludedFiles) {

    override def getFiles: Option[Files] = {
        val files = getAllFilenamesFromFolder match {
            case Failure(exception) =>
                logError(exception.getMessage)
                return None
            case Success(value) => filterFiles(value)
        }
        if (files.isEmpty) {
            None
        } else {
            Some(new Files(files))
        }
    }
}
object IgnoreFile extends Builder[IgnoreFile] {
    override def buildFrom(
        basePaths: BasePaths,
        csvFiles: Option[String],
        excludedFiles: Option[String]
    ): IgnoreFile = {
        new IgnoreFile(basePaths, csvFiles, excludedFiles)
    }
}
