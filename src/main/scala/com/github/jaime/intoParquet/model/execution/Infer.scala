/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model.execution

import com.github.jaime.intoParquet.app.SparkReader.readInferSchema
import com.github.jaime.intoParquet.behaviour.ReadAndWrite
import com.github.jaime.intoParquet.behaviour.{Executor, IOOperation}
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.model.PairCSVAndTableDescription
import org.apache.spark.sql.DataFrame

class Infer(_element: PairCSVAndTableDescription, _paths: BasePaths)
    extends Executor
    with IOOperation
    with ReadAndWrite {

    override protected val element: PairCSVAndTableDescription = _element
    override val paths: BasePaths                = _paths
    override val id: String                      = element.id

    override def readFrom: DataFrame = readInferSchema(absoluteInputPath)
}
