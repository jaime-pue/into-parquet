package com.github.jaime.intoParquet.model.execution

import com.github.jaime.intoParquet.behaviour.Executor
import com.github.jaime.intoParquet.exception.NoSchemaFoundException
import com.github.jaime.intoParquet.model.ParsedObject

class Fail(_element: ParsedObject) extends Executor {

    override protected val element: ParsedObject = _element

    override protected def execution(): Unit = throw new NoSchemaFoundException(element)
}
