/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.app

import com.github.jaime.intoParquet.behaviour.AppLogger
import com.github.jaime.intoParquet.text.AppInfo
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkBuilder extends AppLogger {

    private val AppName: String = AppInfo.AppName

    private def conf: SparkConf = {
        new SparkConf()
            .setMaster("local[*]")
            .setAppName(AppName)
            .set("spark.ui.enabled", "false")
            .set("spark.driver.host", "localhost")
            .set("spark.eventLog.enabled", "false")
            .set("spark.log.level", "ERROR")
    }

    @transient def spark: SparkSession = {
        SparkSession
            .builder()
            .getOrCreate()
    }

    final def afterAll(): Unit = {
        spark.stop()
        logInfo("Stop spark session")
    }

    final def beforeAll(): Unit = {
        logInfo("Start spark session")
        SparkSession
            .builder()
            .config(conf)
            .getOrCreate()

    }

}
