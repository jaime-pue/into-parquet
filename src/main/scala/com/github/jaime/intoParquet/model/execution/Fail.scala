package com.github.jaime.intoParquet.model.execution

import com.github.jaime.intoParquet.behaviour.Executor
import com.github.jaime.intoParquet.exception.NoSchemaFoundException
import com.github.jaime.intoParquet.model.ParsedObject

import scala.util.{Failure, Try}

class Fail(_element: ParsedObject) extends Executor {

    override protected val element: ParsedObject = _element

    override def cast: Try[Unit] = Failure(new NoSchemaFoundException(element))
}
