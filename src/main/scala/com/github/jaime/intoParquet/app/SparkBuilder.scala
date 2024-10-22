package com.github.jaime.intoParquet.app

import com.github.jaime.intoParquet.behaviour.AppLogger
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SparkBuilder extends AppLogger {

    lazy val sparkContext: SparkContext = spark.sparkContext

    @transient def spark: SparkSession = {
        val localSpark = SparkSession
            .builder()
            .master("local[*]")
            .appName(this.getClass.getName)
            .getOrCreate()
        val sc = localSpark.sparkContext
        sc.setLogLevel("ERROR")
        localSpark
    }

    def afterAll(): Unit = {
        spark.stop()
        logInfo("Stop spark session")
    }

    def beforeAll(): Unit = {
        logInfo("Start spark session")
        spark
    }

}
