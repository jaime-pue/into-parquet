/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model.execution

import com.github.jaime.intoParquet.exception.NoSchemaFoundException

import scala.util.Failure
import scala.util.Try

class Fail(_file: String) extends Executor {

    override protected val file: String = _file

    override def cast: Try[Unit] = Failure(new NoSchemaFoundException(file))
}