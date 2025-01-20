/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.behaviour

import com.github.jaime.intoParquet.app.SparkWriter
import com.github.jaime.intoParquet.configuration.BasePaths
import org.apache.spark.sql.DataFrame

trait ReadAndWrite {
    self: Executor =>

    val paths: BasePaths

    private def absoluteOutputPath: String = {
        paths.absoluteOutputPath(self.file)
    }

    def absoluteInputCSVPath: String = {
        paths.absoluteInputCSVPath(self.file)
    }

    def readFrom: DataFrame

    protected def writeResult(): Unit = {
        val writer = new SparkWriter(readFrom, absoluteOutputPath)
        writer.writeTo()
    }

    override def execution(): Unit = {
        writeResult()
    }
}
