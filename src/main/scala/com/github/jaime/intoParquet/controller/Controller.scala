package com.github.jaime.intoParquet.controller

import com.github.jaime.intoParquet.behaviour.{AppLogger, Executor}
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.model.enumeration._
import com.github.jaime.intoParquet.model.execution.{Raw, _}
import com.github.jaime.intoParquet.model.{ParsedObject, ParsedObjectWrapper}

import scala.util.{Failure, Success, Try}

class Controller(
    _basePaths: BasePaths,
    _castMode: CastMode,
    _wrapper: ParsedObjectWrapper,
    failFast: Boolean
) extends AppLogger {

    private val castMode: CastMode           = _castMode
    private val wrapper: ParsedObjectWrapper = _wrapper

    private def castElement(element: ParsedObject): Executor = {
        logInfo(s"Start job for: ${element.id}")
        this.castMode match {
            case Raw         => new Raw(element, _basePaths)
            case InferSchema => new Infer(element, _basePaths)
            case castMode: ParseSchema =>
                if (element.schema.isEmpty) {
                    applyFallbackMethodTo(element, castMode.fallBack.get)
                } else { new Parse(element, _basePaths) }
        }
    }

    private def applyFallbackMethodTo(element: ParsedObject, fallBack: FallBack): Executor = {
        fallBack match {
            case FallBackRaw   => new Raw(element, _basePaths)
            case FallBackInfer => new Infer(element, _basePaths)
            case FallBackFail  => new Fail(element)
            case FallBackNone  => new Pass(element)
        }
    }

    def execution: Try[Unit] = {
        if (failFast) {
            failFastMode
        } else { ignoreErrorMode }
    }

    private def ignoreErrorMode: Try[Unit] = {
        Success(this.wrapper.elements.foreach(e => {
            castElement(e).cast match {
                case Failure(exception) => logError(exception.getMessage)
                case Success(_)         =>
            }
        }))
    }

    private def failFastMode: Try[Unit] = {
        this.wrapper.elements.foreach(e => {
            castElement(e).cast match {
                case Failure(exception) => return Failure(exception)
                case Success(_)         =>
            }

        })
        Success()
    }
}
