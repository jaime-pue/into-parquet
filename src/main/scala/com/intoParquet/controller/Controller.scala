package com.intoParquet.controller

import com.intoParquet.configuration.BasePaths
import com.intoParquet.model.enumeration.{InferSchema, Raw, ReadSchema, CastMode}
import com.intoParquet.model.{ParsedObject, ParsedObjectWrapper}
import com.intoParquet.service.Converter
import com.intoParquet.utils.AppLogger

import scala.util.{Success, Try}

class Controller(_basePaths: BasePaths, _castMode: CastMode, _wrapper: ParsedObjectWrapper)
    extends AppLogger {

    private val castMode: CastMode         = _castMode
    private val wrapper: ParsedObjectWrapper = _wrapper
    private val converter: Converter         = new Converter(_basePaths)

    private def castElement(e: ParsedObject): Unit = {
        logInfo(s"Start job for: ${e.id}")
        this.castMode match {
            case Raw         => converter.executeRaw(e.id)
            case InferSchema => converter.executeInferSchema(e.id)
            case ReadSchema  => readFromSchema(e)
        }
    }

    private def readFromSchema(element: ParsedObject): Unit = {
        element.schema match {
            case Some(value) => converter.executeWithTableDescription(element.id, value)
            case None        => converter.executeRaw(element.id)
        }
    }

    def execution: Try[Unit] = {
        Success(this.wrapper.elements.foreach(e => {
            try { castElement(e) }
            catch {
                case ex: Exception => logError(ex.getMessage)
            }
        }))
    }
}
