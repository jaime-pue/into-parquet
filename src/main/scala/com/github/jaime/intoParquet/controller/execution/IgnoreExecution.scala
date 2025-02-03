/*
 * IntoParquet Copyright (c) 2025 Jaime Alvarez
 */

package com.github.jaime.intoParquet.controller.execution

import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.controller.HandleExecution
import com.github.jaime.intoParquet.model.Files
import com.github.jaime.intoParquet.model.enumeration.CastMode
import com.github.jaime.intoParquet.service.Chronometer

import scala.util.Failure
import scala.util.Success
import scala.util.Try

class IgnoreExecution(csvFiles: Files, basePaths: BasePaths, castMode: CastMode)
    extends HandleExecution(csvFiles, basePaths, castMode) {

    override def execution: Try[Unit] = {
        mapper.foreach(e => {
            val timer = new Chronometer()
            e.cast match {
                case Failure(exception) => logError(exception.getMessage)
                case Success(_)         => logInfo(s"${e} took: ${timer.toString} seconds")
            }
        })
        Success()
    }
}

object IgnoreExecution extends Builder[IgnoreExecution] {
    override def buildFrom(
        csvFiles: Files,
        basePaths: BasePaths,
        castMode: CastMode
    ): IgnoreExecution = {
        new IgnoreExecution(csvFiles, basePaths, castMode)
    }
}
