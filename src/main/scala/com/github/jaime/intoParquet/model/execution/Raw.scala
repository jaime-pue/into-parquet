package com.github.jaime.intoParquet.model.execution

import com.github.jaime.intoParquet.app.SparkAction.{readRawCSV, writeTo}
import com.github.jaime.intoParquet.behaviour.{Executor, IOOperation}
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.model.ParsedObject

import scala.util.{Failure, Try}

class Raw(_element: ParsedObject, _paths: BasePaths) extends Executor with IOOperation {

    override protected val element: ParsedObject = _element
    override val paths: BasePaths                = _paths
    override val id: String                      = element.id

    override def cast: Try[Unit] = {
        try {
            val raw = readRawCSV(absoluteInputPath)
            writeTo(raw, absoluteOutputPath)
        } catch {
            case e: Exception => Failure(e)
        }
    }

}
