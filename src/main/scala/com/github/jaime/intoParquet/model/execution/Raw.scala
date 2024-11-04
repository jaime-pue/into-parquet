package com.github.jaime.intoParquet.model.execution

import com.github.jaime.intoParquet.app.SparkAction.{readRawCSV, writeTo}
import com.github.jaime.intoParquet.behaviour.{Executor, IOOperation}
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.model.ParsedObject

class Raw(_element: ParsedObject, _paths: BasePaths) extends Executor with IOOperation {

    override protected val element: ParsedObject = _element
    override val paths: BasePaths                = _paths
    override val id: String                      = element.id

    override protected def execution(): Unit = {
        val raw = readRawCSV(absoluteInputPath)
        writeTo(raw, absoluteOutputPath)
    }
}
