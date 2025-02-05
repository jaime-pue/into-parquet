/*
 * IntoParquet Copyright (c) 2025 Jaime Alvarez
 */

package com.github.jaime.intoParquet.mapping

import com.github.jaime.intoParquet.exception.WrongInputArgsException
import com.github.jaime.intoParquet.mapping.transformer.AsController
import com.github.jaime.intoParquet.service.Parser.InputArgs
import com.github.jaime.intoParquet.service.Parser.parseSystemArgs

class IntoRouter(_inputArgs: Array[String]) extends AsController {

    override val inputArgs: InputArgs = parseSystemArgs(_inputArgs).getOrElse(throw new WrongInputArgsException)
}
