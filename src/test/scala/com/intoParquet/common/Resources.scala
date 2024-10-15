package com.intoParquet.common

import com.intoParquet.configuration.BasePaths

import java.io.File
import scala.reflect.io.Directory

object Resources {

    val ResourceFolder: String = "./src/test/resources"
    lazy val path: BasePaths   = BasePaths(ResourceFolder)

    def cleanDirectory: Boolean = {
        val directory = new Directory(new File(path.OutputBasePath))
        directory.deleteRecursively()
    }
}
