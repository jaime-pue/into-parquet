package com.intoParquet.plugin

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode

object SparkBuilder extends AppLogger {

    private val InputBasePath: String  = "./data/input/raw/"
    private val OutputBasePath: String = "./data/output/"

    lazy val sparkContext: SparkContext = spark.sparkContext

    @transient def spark: SparkSession = {
        val localSpark = SparkSession
            .builder()
            .master("local[*]")
            .appName(this.getClass().getName())
            .getOrCreate()
        val sc = localSpark.sparkContext
        sc.setLogLevel("ERROR")
        localSpark
    }

    def afterAll: Unit = {
        logInfo("STOP SPARK SESSION")
        spark.stop()
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
    }

    private def filepath(filename: String) = {
        s"$InputBasePath${filename}.csv"
    }

    protected[plugin] def readCSV(filename: String, schema: StructType): DataFrame = {
        spark.read
            .schema(schema)
            .option("mode", "DROPMALFORMED")
            .option("columnNameOfCorruptRecord", "ERROR")
            .option("inferSchema", false)
            .option("emptyValue", "###")
            .option("nullValue", "NULL")
            .option("TimeStampFormat", "yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]")
            .option("header", true)
            .format("csv")
            .csv(filepath(filename))
    }

    protected[plugin] def rawCSV(filename: String): DataFrame = {
        spark.read
            .option("header", true)
            .option("inferSchema", false)
            .option("nullValue", "NULL")
            .csv(filepath(filename))
    }

    protected[plugin] def writeTo(df: DataFrame, path: String): Unit = {
        logInfo(s"Writing dataframe to $path")

        logInfo(s"Dataframe: $path, rows: ${df.cache().count()}")
        df.repartition(1).write.mode(SaveMode.Overwrite).parquet({ s"${OutputBasePath}$path" })
    }
}
