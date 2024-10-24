package com.github.jaime.intoParquet.model.execution

import com.github.jaime.intoParquet.app.SparkAction.{readInferSchema, writeTo}
import com.github.jaime.intoParquet.behaviour.{Executor, IOOperation}
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.model.ParsedObject

import scala.util.{Failure, Try}

class Infer(_element: ParsedObject, _paths: BasePaths) extends Executor with IOOperation {

    override protected val element: ParsedObject = _element
    override val paths: BasePaths                = _paths
    override val id: String                      = element.id

    override def cast: Try[Unit] = {
        try {
            val inferredSchema = readInferSchema(absoluteInputPath)
            writeTo(inferredSchema, absoluteOutputPath)
        } catch {
            case e: Exception => Failure(e)
        }
    }

}
