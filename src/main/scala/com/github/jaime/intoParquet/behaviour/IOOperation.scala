/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.behaviour

import com.github.jaime.intoParquet.configuration.BasePaths

trait IOOperation {
    self: Executor =>

    val paths: BasePaths

    def absoluteInputPath: String = {
        s"${paths.inputBasePath}${self.file}.csv"
    }

    def absoluteOutputPath: String = {
        s"${paths.outputBasePath}${self.file}"
    }

}
