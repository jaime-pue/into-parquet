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
        val rows = df.cache().count()
        logDebug(s"Row count: ${rows}")
        val partitions = calculateNumberOfPartitions(rows)
        setNumberOfPartitions(df, partitions).write
            .mode(SaveMode.Overwrite)
            .parquet(path)
    }

    private def setNumberOfPartitions(df: DataFrame, partitions: Int): DataFrame = {
        logDebug(s"Split in #$partitions files")
        if (isRepartitionBetter(partitions)) {
            df.repartition(1)
        } else {
            df.coalesce(partitions)
        }
    }

    private def isRepartitionBetter(parts: Int): Boolean = {
        parts == 0 || parts == 1
    }

    protected[app] def calculateNumberOfPartitions(dataFrameRows: Long): Int = {
        (dataFrameRows.toDouble / LinesForSplitting.toDouble).ceil.toInt
    }

}
