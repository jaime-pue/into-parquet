package com.intoParquet

import com.intoParquet.configuration.BasePaths
import com.intoParquet.exception.NoFileFoundException
import com.intoParquet.mapping.IntoParsedObjectWrapper
import com.intoParquet.model.{ParsedObject, ParsedObjectWrapper}
import com.intoParquet.model.enumeration.{InferSchema, Raw, ReadSchema, WriteMode}
import com.intoParquet.service.Converter.{executeInferSchema, executeRaw, executeWithTableDescription}
import com.intoParquet.service.FileLoader
import com.intoParquet.utils.AppLogger
import com.intoParquet.utils.Parser.InputArgs

import scala.util.{Failure, Success, Try}

object Controller extends AppLogger {

    def routeOnWriteMode(
        value: WriteMode,
        wrapper: ParsedObjectWrapper
    ): Unit = {
        wrapper.elements.foreach(e => {
            logInfo(s"Start job for: ${e.id}")
            value match {
                case Raw => executeRaw(e.id)
                case InferSchema => executeInferSchema(e.id)
                case ReadSchema => readFromSchema(e)
            }
        }
        )
    }

    private def readFromSchema(element: ParsedObject): Unit = {
        element.schema match {
            case Some(value) => executeWithTableDescription(element.id, value)
            case None        => executeRaw(element.id)
        }
    }

    def inputArgController(value: InputArgs): Try[Unit] = {
        val basePaths = BasePaths(value.directory)
        val fileLoader = new FileLoader(basePaths)
        val csv = if (value.recursive) {
            logInfo(s"Read all csv files from ${basePaths.InputRawPath}")
            new FileLoader(basePaths).readAllFilesFromRaw match {
                case Failure(exception) => return Failure(exception)
                case Success(value) => value
            }
        } else {
            value.csvFile.getOrElse(throw new NoFileFoundException("v")).split(";").map(_.trim)
        }
        val files = IntoParsedObjectWrapper.castTo(csv, fileLoader)
        Try(routeOnWriteMode(value.writeMethod, files))
    }
}
