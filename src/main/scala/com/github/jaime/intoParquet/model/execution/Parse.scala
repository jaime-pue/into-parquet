/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model.execution

import com.github.jaime.intoParquet.app.SparkAction.{applySchema, readRawCSV, writeTo}
import com.github.jaime.intoParquet.behaviour.{Executor, IOOperation}
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.mapping.IntoFieldDescriptors
import com.github.jaime.intoParquet.model.ParsedObject

class Parse(_element: ParsedObject, _paths: BasePaths) extends Executor with IOOperation {

    override protected val element: ParsedObject = _element
    override val paths: BasePaths                = _paths
    override val id: String                      = element.id

    override protected def execution(): Unit = {
        val raw    = readRawCSV(absoluteInputPath)
        val fields = IntoFieldDescriptors.fromDescription(element.schema.get)
        val schema = applySchema(raw, fields)
        writeTo(schema, absoluteOutputPath)
    }
}
