/*
 * IntoParquet
 * This command-line tool is designed to convert CSV files into
 * the efficient Parquet format.
 * Copyright (c) 2024 Jaime Alvarez
 *
 * Contact info: jaime.af.git@gmail.com
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.github.jaime.intoParquet

import com.github.jaime.intoParquet.app.SparkBuilder
import com.github.jaime.intoParquet.behaviour.AppLogger
import com.github.jaime.intoParquet.controller.Controller
import com.github.jaime.intoParquet.exception.WrongInputArgsException
import com.github.jaime.intoParquet.mapping.IntoController
import com.github.jaime.intoParquet.utils.Parser.InputArgs
import com.github.jaime.intoParquet.utils.Parser.parseSystemArgs
import org.apache.log4j.Level
import org.apache.log4j.Logger

import scala.util.Failure
import scala.util.Success

object Main extends AppLogger {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)

    def main(args: Array[String]): Unit = {
        val inputArgs: InputArgs = {
            parseSystemArgs(args).getOrElse(throw new WrongInputArgsException)
        }

        SparkBuilder.beforeAll()

        val intoController: IntoController = new IntoController(inputArgs)
        val controller: Controller = Controller(intoController)

        launchConversion(controller)

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
