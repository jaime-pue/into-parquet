/*
 * IntoParquet Copyright (c) 2025 Jaime Alvarez
 */

package com.github.jaime.intoParquet.controller.execution

import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.controller.HandleExecution
import com.github.jaime.intoParquet.model.Files
import com.github.jaime.intoParquet.model.enumeration.CastMode

protected[execution] trait Builder[A <: HandleExecution] {
    def buildFrom(csvFiles: Files, basePaths: BasePaths, castMode: CastMode): A
}
