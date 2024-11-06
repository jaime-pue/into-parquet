/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.app

import com.github.jaime.intoParquet.behaviour.AppLogger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode

object SparkWriter extends AppLogger {

    def writeTo(df: DataFrame, path: String): Unit = {
        logInfo(s"Writing dataframe to ${path}")

        logDebug(s"Row count: ${df.cache().count()}")
        df.repartition(1).write.mode(SaveMode.Overwrite).parquet(path)
    }

}
