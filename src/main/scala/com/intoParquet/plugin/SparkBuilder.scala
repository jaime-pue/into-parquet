package com.intoParquet.plugin

import com.intoParquet.utils.AppLogger
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext


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
        logInfo("STOP SPARK SESSION")
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
        spark.stop()
    }


}
