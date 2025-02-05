/*
 * IntoParquet Copyright (c) 2025 Jaime Alvarez
 */

package com.github.jaime.intoParquet.controller.ignore

import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.controller.HandleExecution
import com.github.jaime.intoParquet.controller.HandleFile
import com.github.jaime.intoParquet.controller.HandleRouter
import com.github.jaime.intoParquet.mapping.transformer.AsController
import com.github.jaime.intoParquet.mapping.transformer.Mapper
import com.github.jaime.intoParquet.model.Files
import com.github.jaime.intoParquet.model.enumeration.CastMode

class IgnoreRouter(
    basePaths: BasePaths,
    castMode: CastMode,
    csvFiles: Option[String],
    excludeFiles: Option[String]
) extends HandleRouter(basePaths) {

    override def intoFileExecution(files: Files): HandleExecution =
        new IgnoreExecution (files, basePaths, castMode)

    override def intoFileController: HandleFile =
        new IgnoreFile(basePaths = basePaths, csvFiles = csvFiles, excludedFiles = excludeFiles)

}

object IgnoreRouter extends Mapper[IgnoreRouter] {

    override def router(obj: AsController): IgnoreRouter =
        new IgnoreRouter(obj.basePaths, obj.castMode, obj.csvFiles, obj.excludedFiles)
}
