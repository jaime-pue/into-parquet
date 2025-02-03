/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model.execution

import com.github.jaime.intoParquet.service.AppLogger

import scala.util.Success
import scala.util.Try

class Pass(_file: String) extends Executor with AppLogger {

    override protected val file: String = _file

    override def cast: Try[Unit] = Success(logInfo(s"Skip ${file}"))
}

