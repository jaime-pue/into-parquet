/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model.execution

import com.github.jaime.intoParquet.behaviour.AppLogger
import com.github.jaime.intoParquet.behaviour.Executor

class Pass(_file: String) extends Executor with AppLogger {

    override protected val file: String = _file

    override def execution(): Unit = logWarning(s"No table description found for ${file}")
}
