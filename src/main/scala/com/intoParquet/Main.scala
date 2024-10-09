package com.intoParquet

import org.apache.log4j.Logger
import org.apache.log4j.Level
import com.intoParquet.plugin.SparkBuilder
import com.intoParquet.utils.AppLogger

object Main extends AppLogger {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)

    def main(args: Array[String]): Unit = {

        logInfo("Start session")

        SparkBuilder.spark

        SparkBuilder.afterAll()

    }
}
