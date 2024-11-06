/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model.execution

import com.github.jaime.intoParquet.app.SparkReader.readRawCSV
import com.github.jaime.intoParquet.behaviour.ReadAndWrite
import com.github.jaime.intoParquet.behaviour.Executor
import com.github.jaime.intoParquet.behaviour.IOOperation
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.model.ParsedObject
import org.apache.spark.sql.DataFrame

class Raw(_element: ParsedObject, _paths: BasePaths)
    extends Executor
    with IOOperation
    with ReadAndWrite {

    override protected val element: ParsedObject = _element
    override val paths: BasePaths                = _paths
    override val id: String                      = element.id

    override def readFrom: DataFrame = readRawCSV(absoluteInputPath)
}
