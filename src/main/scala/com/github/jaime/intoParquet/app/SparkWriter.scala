/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.app

import com.github.jaime.intoParquet.app.SparkBuilder.spark
import com.github.jaime.intoParquet.app.SparkWriter.calculateNumberOfPartitions
import com.github.jaime.intoParquet.app.SparkWriter.isRepartitionBetter
import com.github.jaime.intoParquet.service.AppLogger
import com.github.jaime.intoParquet.service.Common.renderPath
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode

import scala.math.pow

class SparkWriter(df: DataFrame, path: String) extends AppLogger {

    def writeTo(): Unit = {
        logInfo("Write parquet file")
        logDebug(s"Write data to: ${renderPath(path)}")
        val partitions = calculateNumberOfPartitions(sizeInBytes.toLong)
        setNumberOfPartitions(partitions).write
            .mode(SaveMode.Overwrite)
            .parquet(path)
    }

    private def sizeInBytes: BigInt = {
        val rows = df.cache().count()
        logDebug(s"Row count: ${rows}")
        val catalystPlan = df.queryExecution.logical
        spark.sessionState.executePlan(catalystPlan).optimizedPlan.stats.sizeInBytes
    }

    protected[app] def setNumberOfPartitions(partitions: Int): DataFrame = {
        logDebug(s"Split in #$partitions files")
        if (isRepartitionBetter(partitions)) {
            df.repartition(1)
        } else {
            df.coalesce(partitions)
        }
    }
}

object SparkWriter extends AppLogger {
    private val MaxMBForSplits: Int = 100

    protected[app] def calculateNumberOfPartitions(sizeInBytes: Long): Int = {
        (ceilBytesToInt(sizeInBytes).toDouble / MaxMBForSplits.toDouble).ceil.toInt
    }

    protected[app] def ceilBytesToInt(bytes: BigInt): Int = {
        (bytes.toDouble * pow(10, -6)).ceil.toInt
    }

    private def isRepartitionBetter(parts: Int): Boolean = {
        parts <= 1
    }
}
