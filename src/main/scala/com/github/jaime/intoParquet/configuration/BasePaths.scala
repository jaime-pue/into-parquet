/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.configuration

import java.nio.file.Paths

class BasePaths(
    inputDir: Option[String] = None,
    outputDir: Option[String] = None
) {
    private val DefaultInput: String  = "./data/input/"
    private val DefaultOutput: String = "./data/output/"

    def this(base: String) = {
        this(Some(s"${base}input/"), Some(s"${base}output/"))
    }

    def inputBasePath: String = {
        inputDir.getOrElse(DefaultInput)
    }

    def outputBasePath: String = {
        if (isNewDirectory) {
            s"${inputBasePath}output/"
        } else {
            outputDir.getOrElse(DefaultOutput)
        }
    }

    private def isNewDirectory: Boolean = {
        inputDir.isDefined && outputDir.isEmpty
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
        Paths.get(relativePath).toAbsolutePath().toString
    }
}
