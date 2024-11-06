/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.mapping

import com.github.jaime.intoParquet.behaviour.AppLogger
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.controller.Controller
import com.github.jaime.intoParquet.exception.NoCSVException
import com.github.jaime.intoParquet.model.ParsedObjectWrapper
import com.github.jaime.intoParquet.model.enumeration.CastMode
import com.github.jaime.intoParquet.model.enumeration.ParseSchema
import com.github.jaime.intoParquet.service.FileLoader
import com.github.jaime.intoParquet.utils.Parser.InputArgs

import scala.util.Failure
import scala.util.Success

class IntoController(args: InputArgs) extends AppLogger {

    private val inputArgs              = args
    private val recursiveRead: Boolean = inputArgs.recursive
    private val basePaths: BasePaths   = intoBasePaths
    private val fileLoader: FileLoader = new FileLoader(basePaths)
    private val failFast: Boolean      = inputArgs.failFast

    private def intoBasePaths: BasePaths = {
        new BasePaths(inputArgs.inputDir, inputArgs.outputDir)
    }

    private def intoCastMethod: CastMode = {
        inputArgs.fallBack match {
            case Some(value) => new ParseSchema(value)
            case None        => inputArgs.castMethod
        }
    }

    def castTo: Controller = {
        new Controller(basePaths, intoCastMethod, intoParsedObjectWrapper, failFast)
    }

    private def intoParsedObjectWrapper: ParsedObjectWrapper = {
        val filenames = obtainAllFileNames
        IntoParsedObjectWrapper.castTo(filenames, fileLoader)
    }

    private def obtainAllFileNames: Array[String] = {
        if (recursiveRead) {
            getAllFilenamesFromFolder
        } else {
            getFilenamesFromInputLine
        }
    }

    protected[mapping] def getAllFilenamesFromFolder: Array[String] = {
        fileLoader.readAllFilesFromRaw match {
            case Failure(exception) => throw exception
            case Success(filenames) => filenames
        }
    }

    protected[mapping] def getFilenamesFromInputLine: Array[String] = {
        inputArgs.csvFile.getOrElse(throw new NoCSVException).split(",").map(_.trim).distinct
    }

}
