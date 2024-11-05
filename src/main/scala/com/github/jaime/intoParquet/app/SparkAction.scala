/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.app

import com.github.jaime.intoParquet.app.SparkBuilder.spark
import com.github.jaime.intoParquet.behaviour.AppLogger
import com.github.jaime.intoParquet.model.FieldWrapper
import org.apache.spark.sql.{DataFrame, SaveMode}

object SparkAction extends AppLogger {

    def readApplySchema(filename: String, description: FieldWrapper): DataFrame = {
        val raw = readRawCSV(filename)
        applySchema(raw, description)
    }

    protected[app] def applySchema(df: DataFrame, description: FieldWrapper): DataFrame = {
        logInfo(s"Apply schema to current data")
        description.fields.foldLeft(df) { (temp, field) =>
            temp.withColumn(field.fieldName, field.colExpression)
        }
    }

    def writeTo(df: DataFrame, path: String): Unit = {
        logInfo(s"Writing dataframe to ${path}")

        logDebug(s"Row count: ${df.cache().count()}")
        df.repartition(1).write.mode(SaveMode.Overwrite).parquet(path)
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
