package com.github.jaime.intoParquet.model.execution

import com.github.jaime.intoParquet.behaviour.{AppLogger, Executor}
import com.github.jaime.intoParquet.model.ParsedObject

class Pass(_element: ParsedObject) extends Executor with AppLogger {

    override protected val element: ParsedObject = _element

    override protected def execution(): Unit = logWarning(s"No schema found for ${element.id}")
}
