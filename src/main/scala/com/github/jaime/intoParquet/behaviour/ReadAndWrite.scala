/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.behaviour

import com.github.jaime.intoParquet.app.SparkAction.writeTo
import org.apache.spark.sql.DataFrame

trait ReadAndWrite {
    self: Executor with IOOperation =>

    def readFrom: DataFrame

    private def writeResult(): Unit = {
        writeTo(readFrom, self.absoluteOutputPath)
    }

    final override def execution(): Unit = {
        writeResult()
    }
}
