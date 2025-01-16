/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.app

import com.github.jaime.intoParquet.app.SparkBuilder.spark
import com.github.jaime.intoParquet.behaviour.AppLogger
import com.github.jaime.intoParquet.configuration.ReaderConfiguration
import com.github.jaime.intoParquet.service.Common.renderPath
import org.apache.spark.sql.DataFrame

object SparkReader extends AppLogger {

    def readInferSchema(filename: String): DataFrame = {
        renderReadMessage(filename)
        spark.read
            .option("mode", "DROPMALFORMED")
            .option("columnNameOfCorruptRecord", "ERROR")
            .option("inferSchema", true)
            .option("header", true)
            .option("nullValue", ReaderConfiguration.NullValue)
            .option("TimeStampFormat", ReaderConfiguration.TimestampFormat)
            .option("sep", ReaderConfiguration.Separator)
            .csv(filename)
    }

    def readRawCSV(filename: String): DataFrame = {
        renderReadMessage(filename)
        spark.read
            .option("header", true)
            .option("inferSchema", false)
            .option("nullValue", ReaderConfiguration.NullValue)
            .option("sep", ReaderConfiguration.Separator)
            .csv(filename)
    }

    private def renderReadMessage(path: String): Unit = {
        logDebug(s"Read data from ${renderPath(path)}")
    }
}
