package com.github.jaime.intoParquet.behaviour

import com.github.jaime.intoParquet.configuration.BasePaths

trait IOOperation {

    val paths: BasePaths
    val id: String

    def absoluteInputPath: String = {
        s"${paths.inputBasePath}${id}.csv"
    }

    def absoluteOutputPath: String = {
        s"${paths.outputBasePath}${id}"
    }

}
