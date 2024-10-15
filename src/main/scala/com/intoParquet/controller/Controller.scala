package com.intoParquet.controller

import com.intoParquet.configuration.BasePaths
import com.intoParquet.model.enumeration.{InferSchema, Raw, ReadSchema, WriteMode}
import com.intoParquet.model.{ParsedObject, ParsedObjectWrapper}
import com.intoParquet.service.Converter
import com.intoParquet.utils.AppLogger

import scala.util.Try

class Controller(_basePaths: BasePaths, _writeMode: WriteMode, _wrapper: ParsedObjectWrapper)
    extends AppLogger {

    private val writeMode: WriteMode         = _writeMode
    private val wrapper: ParsedObjectWrapper = _wrapper
    private val converter: Converter         = new Converter(_basePaths)

    def routeOnWriteMode(
        value: WriteMode,
        wrapper: ParsedObjectWrapper
    ): Unit = {
        wrapper.elements.foreach(e => {
            logInfo(s"Start job for: ${e.id}")
            value match {
                case Raw         => converter.executeRaw(e.id)
                case InferSchema => converter.executeInferSchema(e.id)
                case ReadSchema  => readFromSchema(e)
            }
        })
    }

    private def readFromSchema(element: ParsedObject): Unit = {
        element.schema match {
            case Some(value) => converter.executeWithTableDescription(element.id, value)
            case None        => converter.executeRaw(element.id)
        }
    }

    def execution: Try[Unit] = {
        Try(
          routeOnWriteMode(
            this.writeMode,
            this.wrapper
          )
        )
    }
}