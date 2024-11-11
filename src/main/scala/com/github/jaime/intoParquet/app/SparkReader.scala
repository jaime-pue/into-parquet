/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.app

import com.github.jaime.intoParquet.app.SparkBuilder.spark
import com.github.jaime.intoParquet.behaviour.AppLogger
import com.github.jaime.intoParquet.model.TableDescription
import org.apache.spark.sql.DataFrame

object SparkReader extends AppLogger {

    def readApplySchema(filename: String, description: TableDescription): DataFrame = {
        val raw = readRawCSV(filename)
        applySchema(raw, description)
    }

    protected[app] def applySchema(df: DataFrame, description: TableDescription): DataFrame = {
        logInfo(s"Apply schema to current data")
        description.fields.foldLeft(df) { (temp, field) =>
            temp.withColumn(field.fieldName, field.colExpression)
        }
    }

    def readInferSchema(filename: String): DataFrame = {
        logInfo(s"Read data & infer schema from $filename")
        spark.read
            .option("mode", "DROPMALFORMED")
            .option("columnNameOfCorruptRecord", "ERROR")
            .option("inferSchema", true)
            .option("nullValue", "NULL")
            .option("TimeStampFormat", "yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]")
            .option("header", true)
            .format("csv")
            .csv(filename)
    }

    def readRawCSV(filename: String): DataFrame = {
        logInfo(s"Read raw data from $filename")
        spark.read
            .option("header", true)
            .option("inferSchema", false)
            .option("nullValue", "NULL")
            .csv(filename)
    }
}
