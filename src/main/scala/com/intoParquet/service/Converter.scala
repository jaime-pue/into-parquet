package com.intoParquet.service

import com.intoParquet.configuration.BasePaths
import com.intoParquet.mapping.{IntoFieldDescriptors, IntoTableDescription}
import com.intoParquet.model.{FieldWrapper, TableDescription}
import com.intoParquet.service.SparkBuilder.spark
import com.intoParquet.utils.AppLogger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.{Failure, Success, Try}

class Converter(basePaths: BasePaths) extends AppLogger {

    private val inputBasePath: String  = basePaths.inputBasePath
    private val outputBasePath: String = basePaths.outputBasePath

    private def filepath(filename: String) = {
        s"$inputBasePath${filename}.csv"
    }

    private def readCSV(filename: String, schema: StructType): DataFrame = {
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

    private def readRawCSV(filename: String): DataFrame = {
        logInfo(s"Read raw data from $filename")
        spark.read
            .option("header", true)
            .option("inferSchema", false)
            .option("nullValue", "NULL")
            .csv(filepath(filename))
    }

    private def readInferSchema(filename: String): DataFrame = {
        logInfo(s"Read data & infer schema from $filename")
        spark.read
            .option("mode", "DROPMALFORMED")
            .option("columnNameOfCorruptRecord", "ERROR")
            .option("inferSchema", true)
            .option("nullValue", "NULL")
            .option("TimeStampFormat", "yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]")
            .option("header", true)
            .format("csv")
            .csv(filepath(filename))
    }

    private def writeTo(df: DataFrame, path: String): Try[Unit] = {
        logInfo(s"Writing dataframe to $outputBasePath$path")

        logInfo(s"Dataframe: $path, rows: ${df.cache().count()}")
        Try(df.repartition(1).write.mode(SaveMode.Overwrite).parquet({ s"${outputBasePath}$path" }))
    }

    def applyTableDescription(
        df: DataFrame,
        tableDescription: String
    ): DataFrame = {
        val description = IntoTableDescription.castTo(tableDescription)
        val fields      = IntoFieldDescriptors.fromDescription(description)
        applySchema(df, fields)
    }

    def applySchema(df: DataFrame, description: FieldWrapper): DataFrame = {
        logInfo(s"Apply schema to dataframe")
        description.fields.foldLeft(df) { (temp, field) =>
            temp.withColumn(field.fieldName, field.colExpression)
        }
    }

    def executeCastGoalWithTableDescription(input: String, tableDescription: String): Unit = {
        val raw = readRawCSV(input)
        val df  = applyTableDescription(raw, tableDescription)
        writeTo(df, input)
    }

    def executeWithTableDescription(id: String, tableDescription: TableDescription): Try[Unit] = {
        try {
            val raw    = readRawCSV(id)
            val fields = IntoFieldDescriptors.fromDescription(tableDescription)
            val schema = applySchema(raw, fields)
            writeTo(schema, id)
        } catch {
            case e: Exception => Failure(e)
        }
    }

    def executeRaw(id: String): Try[Unit] = {
        try {
            val raw = readRawCSV(id)
            writeTo(raw, id)
        } catch {
            case e: Exception => Failure(e)
        }
    }

    def executeInferSchema(id: String): Try[Unit] = {
        try {
            val inferredSchema = readInferSchema(id)
            writeTo(inferredSchema, id)
        } catch {
            case e: Exception => Failure(e)
        }
    }
}
