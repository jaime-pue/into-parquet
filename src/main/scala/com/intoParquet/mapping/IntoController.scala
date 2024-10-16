package com.intoParquet.mapping

import com.intoParquet.configuration.BasePaths
import com.intoParquet.controller.Controller
import com.intoParquet.exception.NoCSVException
import com.intoParquet.model.ParsedObjectWrapper
import com.intoParquet.service.FileLoader
import com.intoParquet.utils.AppLogger
import com.intoParquet.utils.Parser.InputArgs

import scala.util.{Failure, Success, Try}

object IntoController extends AppLogger {

    def castTo(args: InputArgs): Try[Controller] = {
        val basePaths  = new BasePaths(args.inputDir, args.outputDir)
        val fileLoader = new FileLoader(basePaths)
        val csv = if (args.recursive) {
            logInfo(s"Read all csv files from ${basePaths.inputBasePath}")
            new FileLoader(basePaths).readAllFilesFromRaw match {
                case Failure(exception) => return Failure(exception)
                case Success(value)     => value
            }
        } else {
            args.csvFile match {
                case Some(value) => value.split(",").map(_.trim)
                case None        => return Failure(new NoCSVException)
            }
        }
        val files: ParsedObjectWrapper = IntoParsedObjectWrapper.castTo(csv, fileLoader)
        Success(new Controller(basePaths, args.castMethod, files, args.failFast))
    }
}
