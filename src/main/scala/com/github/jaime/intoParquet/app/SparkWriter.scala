/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.app

import com.github.jaime.intoParquet.behaviour.AppLogger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode

object SparkWriter extends AppLogger {

    private val LinesForSplitting: Int = 50000

    def writeTo(df: DataFrame, path: String): Unit = {
        logInfo(s"Writing dataframe to ${path}")
        setNumberOfPartitions(df).write
            .mode(SaveMode.Overwrite)
            .parquet(path)
    }

    private def setNumberOfPartitions(df: DataFrame): DataFrame = {
        val rows = df.cache().count()
        logDebug(s"Row count: ${rows}")
        val partitions = calculateNumberOfPartitions(rows)
        partitions match {
            case 0 => df.repartition(1)
            case 1 => df.repartition(1)
            case _ => df.coalesce(partitions)
        }
    }

    protected[app] def calculateNumberOfPartitions(dataFrameRows: Long): Int = {
        (dataFrameRows.toDouble / LinesForSplitting.toDouble).ceil.toInt
    }

}
