package com.github.jaime.intoParquet.mapping

import com.github.jaime.intoParquet.behaviour.AppLogger
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.model.ParsedObjectWrapper
import com.github.jaime.intoParquet.model.enumeration.{CastMode, ParseSchema}
import com.github.jaime.intoParquet.utils.Parser.InputArgs
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.controller.Controller
import com.github.jaime.intoParquet.exception.NoCSVException
import com.github.jaime.intoParquet.model.ParsedObjectWrapper
import com.github.jaime.intoParquet.model.enumeration.{CastMode, ParseSchema}
import com.github.jaime.intoParquet.service.FileLoader
import com.github.jaime.intoParquet.utils.Parser.InputArgs

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
