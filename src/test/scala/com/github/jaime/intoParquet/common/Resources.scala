/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.common

import com.github.jaime.intoParquet.configuration.BasePaths

import java.io.File
import scala.reflect.io.Directory

object Resources {

    val ResourceFolder: String   = "./src/test/resources/"
    val InputTestFolder: String  = "./src/test/resources/input/"
    val OutputTestFolder: String = "./src/test/resources/output/"
    lazy val path: BasePaths     = new BasePaths(ResourceFolder)

    def cleanDirectory: Boolean = {
        val directory = new Directory(new File(path.outputBasePath))
        directory.deleteRecursively()
    }

    val goodFile: String      = "exampleTable"
    val onlyCSV: String       = "timestampConversion"
    val wrongTypeFile: String = "wrongType"

    val AllFiles: Array[String] = Array(wrongTypeFile, goodFile, onlyCSV)
}
