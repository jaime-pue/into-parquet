/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model.execution

import com.github.jaime.intoParquet.app.SparkReader.readApplySchema
import com.github.jaime.intoParquet.behaviour.Executor
import com.github.jaime.intoParquet.behaviour.IOOperation
import com.github.jaime.intoParquet.behaviour.ReadAndWrite
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.model.PairCSVAndTableDescription
import org.apache.spark.sql.DataFrame

class Parse(_element: PairCSVAndTableDescription, _paths: BasePaths)
    extends Executor
    with IOOperation
    with ReadAndWrite {

    override protected val element: PairCSVAndTableDescription = _element
    override val paths: BasePaths                = _paths
    override val id: String                      = element.id

    override def readFrom: DataFrame = {
        readApplySchema(absoluteInputPath, element.schema.get)
    }
}
