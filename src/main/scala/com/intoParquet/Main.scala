package com.intoParquet

import com.intoParquet.exception.WrongInputArgsException
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

        val inputArgs: InputArgs =
            parseSystemArgs(args).getOrElse(throw new WrongInputArgsException)
        Controller.inputArgController(inputArgs) match {
            case Failure(exception) =>
                logError(
                    s"""Job fail with exception
                      |${exception.getMessage}
                      |""".stripMargin)
                throw exception
            case Success(_) => logInfo("Everything ok")
        }
        SparkBuilder.afterAll()

    }
}
