/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.controller

import com.github.jaime.intoParquet.app.SparkBuilder
import com.github.jaime.intoParquet.behaviour.AppLogger
import com.github.jaime.intoParquet.behaviour.Executor
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.configuration.SparkConfiguration.configuration
import com.github.jaime.intoParquet.mapping.IntoController
import com.github.jaime.intoParquet.model.enumeration.CastMode
import com.github.jaime.intoParquet.model.enumeration.FallBack
import com.github.jaime.intoParquet.model.enumeration.FallBackFail
import com.github.jaime.intoParquet.model.enumeration.FallBackInfer
import com.github.jaime.intoParquet.model.enumeration.FallBackNone
import com.github.jaime.intoParquet.model.enumeration.FallBackRaw
import com.github.jaime.intoParquet.model.enumeration.InferSchema
import com.github.jaime.intoParquet.model.enumeration.ParseSchema
import com.github.jaime.intoParquet.model.enumeration.RawSchema
import com.github.jaime.intoParquet.model.execution.Fail
import com.github.jaime.intoParquet.model.execution.Infer
import com.github.jaime.intoParquet.model.execution.Parse
import com.github.jaime.intoParquet.model.execution.Pass
import com.github.jaime.intoParquet.model.execution.Raw
import com.github.jaime.intoParquet.model.PairCSVAndTableDescription
import com.github.jaime.intoParquet.model.ParsedObjectWrapper

import scala.util.Failure
import scala.util.Success
import scala.util.Try

class Controller(
    _basePaths: BasePaths,
    _castMode: CastMode,
    _wrapper: ParsedObjectWrapper,
    failFast: Boolean
) extends AppLogger {

    private val castMode: CastMode           = _castMode
    private val wrapper: ParsedObjectWrapper = _wrapper

    private def castElement(element: PairCSVAndTableDescription): Executor = {
        logInfo(s"Start job for: ${element.id}")
        this.castMode match {
            case RawSchema   => new Raw(element, _basePaths)
            case InferSchema => new Infer(element, _basePaths)
            case castMode: ParseSchema =>
                if (element.schema.isEmpty) {
                    applyFallbackMethodTo(element, castMode.fallBack.get)
                } else { new Parse(element, _basePaths) }
        }
    }

    private def applyFallbackMethodTo(element: PairCSVAndTableDescription, fallBack: FallBack): Executor = {
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

    final def buildSparkAndRun(): Unit = {
        SparkBuilder.beforeAll(configuration)
        execution match {
            case Success(_) =>
                logInfo("Job ended Ok!")
                SparkBuilder.afterAll()
            case Failure(exception) =>
                logError(s"""Something went wrong
                            |${exception.getMessage}
                            |""".stripMargin)
                throw exception
        }

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

object Controller {
    def apply(intoController: IntoController): Controller = {
        intoController.castTo
    }
}
