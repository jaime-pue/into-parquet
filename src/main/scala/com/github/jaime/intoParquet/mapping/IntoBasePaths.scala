/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.mapping

import com.github.jaime.intoParquet.mapping.transformer.AsBasePath

class IntoBasePaths(inPath: Option[String] = None, outPath: Option[String] = None)
    extends AsBasePath {

    private val DefaultInput: String  = "./data/input/"
    private val DefaultOutput: String = "./data/output/"

    override def inputBasePath: String = inPath.getOrElse(DefaultInput)

    override def outputBasePath: String = {
        if (isNewDirectory) {
            s"${inputBasePath}output/"
        } else {
            outPath.getOrElse(DefaultOutput)
        }
    }

    private def isNewDirectory: Boolean = {
        inPath.isDefined && outPath.isEmpty
    }
}
