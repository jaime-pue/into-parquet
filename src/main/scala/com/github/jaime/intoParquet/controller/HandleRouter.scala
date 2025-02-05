/*
 * IntoParquet Copyright (c) 2025 Jaime Alvarez
 */

package com.github.jaime.intoParquet.controller

import com.github.jaime.intoParquet.app.SparkBuilder
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.configuration.ReaderConfiguration
import com.github.jaime.intoParquet.controller.fail.FailFastRouter
import com.github.jaime.intoParquet.controller.ignore.IgnoreRouter
import com.github.jaime.intoParquet.mapping.transformer.AsController
import com.github.jaime.intoParquet.mapping.transformer.Mapper
import com.github.jaime.intoParquet.model.Files
import com.github.jaime.intoParquet.service.AppLogger

abstract class HandleRouter(basePaths: BasePaths) extends AppLogger {
    protected def intoFileController: HandleFile

    protected def intoFileExecution(files: Files): HandleExecution

    final def route(): Unit = {
        intoFileController.getFiles match {
            case Some(csvFiles) =>
                logDebug(s"Files for processing: [${csvFiles.toString}]")
                SparkBuilder.beforeAll()
                intoFileExecution(csvFiles).execution()
                logInfo("Job ended Ok!")
                SparkBuilder.afterAll()
            case None => logInfo(s"No file found in ${basePaths.inputBasePath}. Skipping")
        }
    }
}

object HandleRouter extends Mapper[HandleRouter] {

    override def router(obj: AsController): HandleRouter = {
        AppLogger.DebugMode = obj.debugMode
        ReaderConfiguration.Separator = obj.separator
        if (obj.failFast) {
            FailFastRouter.router(obj)
        } else {
            IgnoreRouter.router(obj)
        }
    }
}
