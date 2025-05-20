/*
 * IntoParquet Copyright (c) 2025 Jaime Alvarez
 */

package com.github.jaime.intoParquet.controller.fail

import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.controller.HandleFile

import scala.util.Failure
import scala.util.Success

class FailFastFile(
    basePaths: BasePaths,
    csvFiles: Option[String],
    excludedFiles: Option[String]
) extends HandleFile(basePaths, csvFiles, excludedFiles) {

    override def getRawFileNames: Seq[String] = {
        getAllFilenamesFromFolder match {
            case Failure(exception) =>
                logError(exception.getMessage)
                throw exception
            case Success(value) => value
        }
    }
}