package com.github.jaime.intoParquet

import com.github.jaime.intoParquet.app.SparkBuilder
import com.github.jaime.intoParquet.behaviour.AppLogger
import com.github.jaime.intoParquet.controller.Controller
import com.github.jaime.intoParquet.exception.WrongInputArgsException
import com.github.jaime.intoParquet.mapping.IntoController
import com.github.jaime.intoParquet.utils.Parser.{InputArgs, parseSystemArgs}
import org.apache.log4j.{Level, Logger}

import scala.util.{Failure, Success}

object Main extends AppLogger {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)

    def main(args: Array[String]): Unit = {
        val inputArgs: InputArgs = {
            parseSystemArgs(args).getOrElse(throw new WrongInputArgsException)
        }

        SparkBuilder.beforeAll()

        IntoController.castTo(inputArgs) match {
            case Failure(exception) =>
                logError(exception)
                throw exception
            case Success(value) => launchConversion(value)
        }

        SparkBuilder.afterAll()

    }

    private def launchConversion(controller: Controller): Unit = {
        controller.execution match {
            case Failure(exception) => {
                logError(s"""Something went wrong
                            |${exception.getMessage}
                            |""".stripMargin)
                throw exception
            }
            case Success(_) => logInfo("Job ended Ok!")
        }
    }
}
