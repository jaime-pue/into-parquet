/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.configuration

import com.github.jaime.intoParquet.configuration.text.AppInfo
import org.apache.spark.SparkConf

object SparkConfiguration {
    private val AppName: String = AppInfo.AppName

    def configuration: SparkConf = {
        new SparkConf()
            .setMaster("local[*]")
            .setAppName(AppName)
            .set("spark.ui.enabled", "false")
            .set("spark.driver.host", "localhost")
            .set("spark.eventLog.enabled", "false")
            .set("spark.log.level", "ERROR")
    }
}
