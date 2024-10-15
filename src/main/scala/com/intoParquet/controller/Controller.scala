package com.intoParquet.controller

import com.intoParquet.configuration.BasePaths
import com.intoParquet.model.enumeration.{CastMode, InferSchema, ParseSchema, Raw}
import com.intoParquet.model.{ParsedObject, ParsedObjectWrapper}
import com.intoParquet.service.Converter
import com.intoParquet.utils.AppLogger

import scala.util.{Failure, Success, Try}

class Controller(
    _basePaths: BasePaths,
    _castMode: CastMode,
    _wrapper: ParsedObjectWrapper,
    failFast: Boolean
) extends AppLogger {

    private val castMode: CastMode           = _castMode
    private val wrapper: ParsedObjectWrapper = _wrapper
    private val converter: Converter         = new Converter(_basePaths)

    private def castElement(e: ParsedObject): Unit = {
        logInfo(s"Start job for: ${e.id}")
        this.castMode match {
            case Raw         => converter.executeRaw(e.id)
            case InferSchema => converter.executeInferSchema(e.id)
            case ParseSchema => readFromSchema(e)
        }
    }

    private def readFromSchema(element: ParsedObject): Unit = {
        element.schema match {
            case Some(value) => converter.executeWithTableDescription(element.id, value)
            case None        => converter.executeRaw(element.id)
        }
    }

    def execution: Try[Unit] = {
        if (failFast) {
            failFastMode
        } else { ignoreErrorMode }
    }

    private def ignoreErrorMode: Try[Unit] = {
        Success(this.wrapper.elements.foreach(e => {
            try { castElement(e) }
            catch {
                case ex: Exception => logError(ex.getMessage)
            }
        }))
    }

    private def failFastMode: Try[Unit] = {
        this.wrapper.elements.foreach(e => {
            try { castElement(e) }
            catch {
                case ex: Exception => return Failure(ex)
            }
        })
        Success()
    }
}
