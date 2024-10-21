package com.intoParquet.mapping

import com.intoParquet.configuration.BasePaths
import com.intoParquet.controller.Controller
import com.intoParquet.exception.NoCSVException
import com.intoParquet.model.ParsedObjectWrapper
import com.intoParquet.model.enumeration.{CastMode, ParseSchema}
import com.intoParquet.service.FileLoader
import com.intoParquet.utils.AppLogger
import com.intoParquet.utils.Parser.InputArgs

import scala.util.{Failure, Success, Try}

object IntoController extends AppLogger {

    def castTo(args: InputArgs): Try[Controller] = {
        val basePaths  = intoBasePaths(args)
        val castMethod = intoCastMethod(args)
        val files: ParsedObjectWrapper = intoFiles(args) match {
            case Failure(exception) =>
                return Failure(exception)
            case Success(value) => value
        }
        Success(new Controller(basePaths, castMethod, files, args.failFast))
    }

    private def intoCastMethod(args: InputArgs): CastMode = {
        args.fallBack match {
            case Some(value) => new ParseSchema(value)
            case None => args.castMethod
        }
    }

    private def intoFiles(args: InputArgs): Try[ParsedObjectWrapper] = {
        val basePaths  = intoBasePaths(args)
        val fileLoader = new FileLoader(basePaths)
        val csv = if (args.recursive) {
            logInfo(s"Read all csv files from ${basePaths.inputBasePath}")
            fileLoader.readAllFilesFromRaw match {
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
        Success(files)
    }

    private def intoBasePaths(args: InputArgs): BasePaths = {
        new BasePaths(args.inputDir, args.outputDir)
    }

}
