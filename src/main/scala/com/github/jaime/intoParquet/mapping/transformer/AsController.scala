/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.mapping.transformer

import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.mapping.IntoBasePaths
import com.github.jaime.intoParquet.mapping.IntoCastMode
import com.github.jaime.intoParquet.model.enumeration.CastMode
import com.github.jaime.intoParquet.service.Parser.InputArgs

trait AsController {
    protected val inputArgs: InputArgs

    lazy val basePaths: BasePaths = new BasePaths(new IntoBasePaths(inputArgs.inputDir, inputArgs.outputDir))

    lazy val castMode: CastMode = new IntoCastMode(inputArgs.fallBack, inputArgs.castMethod).mode

    lazy val csvFiles: Option[String] = inputArgs.csvFile

    lazy val excludedFiles: Option[String] = inputArgs.excludeCSV

    def debugMode: Boolean = inputArgs.debugMode

    def separator: String = inputArgs.separator.getOrElse(",")

    def failFast: Boolean = inputArgs.failFast
}
