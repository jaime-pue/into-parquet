package com.github.jaime.intoParquet.model.execution

import com.github.jaime.intoParquet.behaviour.{AppLogger, Executor}
import com.github.jaime.intoParquet.model.ParsedObject

import scala.util.{Success, Try}

class Pass(_element: ParsedObject) extends Executor with AppLogger{

    override protected val element: ParsedObject = _element

    override def cast: Try[Unit] = {
        logWarning(s"No schema found for ${element.id}")
        Success()
    }
}
