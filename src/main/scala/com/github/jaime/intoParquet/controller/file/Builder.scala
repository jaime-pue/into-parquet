/*
 * IntoParquet Copyright (c) 2025 Jaime Alvarez
 */

package com.github.jaime.intoParquet.controller.file

import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.controller.HandleFile

protected[file] trait Builder[A <: HandleFile]{
    def buildFrom(basePaths: BasePaths, csvFiles: Option[String], excludedFiles: Option[String]): A
}
