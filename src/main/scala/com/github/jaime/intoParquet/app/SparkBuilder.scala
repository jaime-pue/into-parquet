/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.app

import com.github.jaime.intoParquet.behaviour.AppLogger
import org.apache.spark.sql.SparkSession

object SparkBuilder extends AppLogger {

    @transient def spark: SparkSession = {
        val localSpark = SparkSession
            .builder()
            .master("local[*]")
            .appName(this.getClass.getName)
            .getOrCreate()
        localSpark.sparkContext.setLogLevel("ERROR")
        localSpark
    }

    final def afterAll(): Unit = {
        spark.stop()
        logInfo("Stop spark session")
    }

    final def beforeAll(): Unit = {
        logInfo("Start spark session")
        spark
    }

}
