/*
 * IntoParquet Copyright (c) 2025 Jaime Alvarez
 */

package com.github.jaime.intoParquet.controller.fail

import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.controller.HandleExecution
import com.github.jaime.intoParquet.controller.HandleFile
import com.github.jaime.intoParquet.controller.HandleRouter
import com.github.jaime.intoParquet.mapping.transformer.AsController
import com.github.jaime.intoParquet.mapping.transformer.Mapper
import com.github.jaime.intoParquet.model.Files
import com.github.jaime.intoParquet.model.enumeration.CastMode

class FailFastRouter(
    basePaths: BasePaths,
    castMode: CastMode,
    csvFiles: Option[String],
    excludeFiles: Option[String]
) extends HandleRouter(basePaths) {

    override def intoFileExecution(files: Files): HandleExecution =
        new FailFastExecution(files, basePaths, castMode)

    override def intoFileController: HandleFile =
        new FailFastFile(basePaths = basePaths, csvFiles = csvFiles, excludedFiles = excludeFiles)

}

object FailFastRouter extends Mapper[FailFastRouter] {

    override def router(obj: AsController): FailFastRouter = {
        new FailFastRouter(obj.basePaths, obj.castMode, obj.csvFiles, obj.excludedFiles)
    }
}
