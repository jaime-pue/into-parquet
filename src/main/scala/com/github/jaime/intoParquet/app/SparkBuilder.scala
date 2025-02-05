/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.app

import com.github.jaime.intoParquet.configuration.SparkConfiguration.configuration
import com.github.jaime.intoParquet.service.AppLogger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkBuilder extends AppLogger {

    @transient def spark: SparkSession = {
        SparkSession
            .builder()
            .getOrCreate()
    }

    final def afterAll(): Unit = {
        spark.stop()
        logDebug("Stop spark session")
    }

    final def beforeAll(configuration: SparkConf = configuration): Unit = {
        logDebug("Start spark session")
        SparkSession
            .builder()
            .config(configuration)
            .getOrCreate()

    }

}
