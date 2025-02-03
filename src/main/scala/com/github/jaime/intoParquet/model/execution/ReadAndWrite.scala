/*
 * IntoParquet Copyright (c) 2024-2025 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model.execution

import com.github.jaime.intoParquet.app.SparkWriter
import com.github.jaime.intoParquet.configuration.BasePaths
import org.apache.spark.sql.DataFrame

import scala.util.Try

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

    private def writeResult(): Unit = {
        val writer = new SparkWriter(readFrom, absoluteOutputPath)
        writer.writeTo()
    }

    override def cast: Try[Unit] = Try(writeResult())
}
