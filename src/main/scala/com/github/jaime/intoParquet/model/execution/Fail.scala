/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model.execution

import com.github.jaime.intoParquet.behaviour.Executor
import com.github.jaime.intoParquet.exception.NoSchemaFoundException
import com.github.jaime.intoParquet.model.PairCSVAndTableDescription

class Fail(_element: PairCSVAndTableDescription) extends Executor {

    override protected val element: PairCSVAndTableDescription = _element

    override protected def execution(): Unit = throw new NoSchemaFoundException(element)
}
