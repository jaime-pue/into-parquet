package com.intoParquet

import com.intoParquet.controller.Controller
import com.intoParquet.exception.WrongInputArgsException
import com.intoParquet.mapping.IntoController
import com.intoParquet.service.SparkBuilder
import com.intoParquet.utils.AppLogger
import com.intoParquet.utils.Parser.{InputArgs, parseSystemArgs}
import org.apache.log4j.{Level, Logger}

import scala.util.{Failure, Success}

object Main extends AppLogger {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)

    def main(args: Array[String]): Unit = {

        SparkBuilder.beforeAll()

        val inputArgs: InputArgs = {
            parseSystemArgs(args).getOrElse(throw new WrongInputArgsException)
        }
        logInfo(inputArgs.toString)
        val controller: Controller = IntoController.castTo(inputArgs) match {
            case Failure(exception) => throw exception
            case Success(value)     => value
        }
        controller.execution match {
            case Failure(exception) => {
                logError(s"""Something went wrong
                      |${exception.getMessage}
                      |""".stripMargin)
                throw exception
            }
            case Success(_) => logInfo("Job ended Ok!")
        }
        SparkBuilder.afterAll()

    }
}
