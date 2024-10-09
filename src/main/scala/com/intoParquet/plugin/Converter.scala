package com.intoParquet.plugin

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import com.intoParquet.plugin.SparkBuilder.spark
import com.intoParquet.mapping.IntoFieldMapper
import com.intoParquet.model.TableDescription
import com.intoParquet.mapping.IntoColumnMapper

object Converter extends AppLogger {

    private val InputBasePath: String  = "./data/input/raw/"
    private val OutputBasePath: String = "./data/output/"

    private def filepath(filename: String) = {
        s"$InputBasePath${filename}.csv"
    }

    def readCSV(filename: String, schema: StructType): DataFrame = {
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

    def readRawCSV(filename: String): DataFrame = {
        logInfo(s"Read raw data from $filename")
        spark.read
            .option("header", true)
            .option("inferSchema", false)
            .option("nullValue", "NULL")
            .csv(filepath(filename))
    }

    def writeTo(df: DataFrame, path: String): Unit = {
        logInfo(s"Writing dataframe to $path")

        logInfo(s"Dataframe: $path, rows: ${df.cache().count()}")
        df.repartition(1).write.mode(SaveMode.Overwrite).parquet({ s"${OutputBasePath}$path" })
    }

    def applyTableDescription(
        df: DataFrame,
        tableDescription: String
    ): DataFrame = {
        val description = new TableDescription(tableDescription)
        val fields      = IntoFieldMapper.fromDescription(description)
        IntoColumnMapper.applySchema(df, fields)
    }

    def executeCastGoalWithTableDescription(input: String, tableDescription: String): Unit = {
        val raw = readRawCSV(input)
        val df  = applyTableDescription(raw, tableDescription)
        writeTo(df, input)
    }
}
