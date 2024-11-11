/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model.execution

import com.github.jaime.intoParquet.behaviour.{AppLogger, Executor}
import com.github.jaime.intoParquet.model.PairCSVAndTableDescription

class Pass(_element: PairCSVAndTableDescription) extends Executor with AppLogger {

    override protected val element: PairCSVAndTableDescription = _element

    override protected def execution(): Unit = logWarning(s"No schema found for ${element.id}")
}
