package com.intoParquet.controller

import com.intoParquet.configuration.BasePaths
import com.intoParquet.model.enumeration.{
    CastMode,
    FallBack,
    FallBackFail,
    FallBackInfer,
    FallBackNone,
    FallBackRaw,
    InferSchema,
    ParseSchema,
    Raw
}
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

    private def castElement(element: ParsedObject): Try[Unit] = {
        logInfo(s"Start job for: ${element.id}")
        this.castMode match {
            case Raw         => converter.executeRaw(element.id)
            case InferSchema => converter.executeInferSchema(element.id)
            case castMode: ParseSchema =>
                if (element.schema.isEmpty) {
                    applyFallbackMethodTo(element, castMode.fallBack.get)
                } else { converter.executeWithTableDescription(element.id, element.schema.get) }
        }
    }

    private def applyFallbackMethodTo(element: ParsedObject, fallBack: FallBack): Try[Unit] = {
        logInfo(s"Apply ${fallBack.toString} method as fallback")
        fallBack match {
            case FallBackRaw   => converter.executeRaw(element.id)
            case FallBackInfer => converter.executeInferSchema(element.id)
            case FallBackFail  => Failure(new Exception())
            case FallBackNone  => Success()
        }
    }

    def execution: Try[Unit] = {
        if (failFast) {
            failFastMode
        } else { ignoreErrorMode }
    }

    private def ignoreErrorMode: Try[Unit] = {
        Success(this.wrapper.elements.foreach(e => {
            castElement(e) match {
                case Failure(exception) => logError(exception.getMessage)
                case Success(_)         =>
            }
        }))
    }

    private def failFastMode: Try[Unit] = {
        this.wrapper.elements.foreach(e => {
            castElement(e) match {
                case Failure(exception) => return Failure(exception)
                case Success(_)         =>
            }

        })
        Success()
    }
}
