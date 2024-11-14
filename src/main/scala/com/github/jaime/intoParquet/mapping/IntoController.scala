/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.mapping

import com.github.jaime.intoParquet.exception.WrongInputArgsException
import com.github.jaime.intoParquet.mapping.transformer.AsController
import com.github.jaime.intoParquet.service.Parser.InputArgs
import com.github.jaime.intoParquet.service.Parser.parseSystemArgs

class IntoController(inputArgs: Array[String]) extends AsController {

    override def into: InputArgs = {
        parseSystemArgs(inputArgs).getOrElse(throw new WrongInputArgsException)
    }
}
