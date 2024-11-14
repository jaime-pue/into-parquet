/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.configuration

import com.github.jaime.intoParquet.mapping.transformer.AsBasePath

import java.nio.file.Paths

class BasePaths(
    val inputBasePath: String,
    val outputBasePath: String
) {
    def this(mapper: AsBasePath) = {
        this(mapper.inputBasePath, mapper.outputBasePath)
    }

    def this(base: String) = {
        this(s"${base}input/", s"${base}output/")
    }

    def absoluteInputCSVPath(file: String): String = {
        asAbsolute(s"${inputBasePath}${file}.csv")
    }

    def absoluteInputTableDescriptionPath(file: String): String = {
        asAbsolute(s"${inputBasePath}${file}")
    }

    def absoluteOutputPath(file: String): String = {
        asAbsolute(s"${outputBasePath}${file}")
    }

    private def asAbsolute(relativePath: String): String = {
        Paths.get(relativePath).toAbsolutePath.toString
    }
}
