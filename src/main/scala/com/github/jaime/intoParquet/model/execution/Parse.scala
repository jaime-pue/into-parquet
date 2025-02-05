/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model.execution

import com.github.jaime.intoParquet.app.SparkReader.readRawCSV
import com.github.jaime.intoParquet.service.AppLogger
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.exception.EnrichException
import com.github.jaime.intoParquet.model.TableDescription
import com.github.jaime.intoParquet.model.enumeration.FallBack
import com.github.jaime.intoParquet.model.enumeration.FallBackFail
import com.github.jaime.intoParquet.model.enumeration.FallBackInfer
import com.github.jaime.intoParquet.model.enumeration.FallBackNone
import com.github.jaime.intoParquet.model.enumeration.FallBackRaw
import com.github.jaime.intoParquet.model.execution.Parse.applySchema
import com.github.jaime.intoParquet.service.FileLoader.readFile
import org.apache.spark.sql.DataFrame

import scala.util.Failure
import scala.util.Success
import scala.util.Try

class Parse(_file: String, _paths: BasePaths, fallBack: FallBack)
    extends Executor
    with ReadAndWrite
    with AppLogger {

    override protected val file: String = _file
    override val paths: BasePaths       = _paths
    private var tb: TableDescription    = _

    override def readFrom: DataFrame = {
        logDebug("Load table description")
        val raw = readRawCSV(absoluteInputCSVPath)
        logInfo(s"Apply schema to current data")
        applySchema(raw, tb)
    }

    override def cast: Try[Unit] = {
        val listOfFields = loadTableDescription match {
            case Some(value) => value
            case None =>
                logInfo(s"No table description file for $file")
                return applyFallbackMethodToCurrentFile.cast
        }
        val table = intoTable(listOfFields) match {
            case Failure(exception) => return Failure(exception)
            case Success(description)     => description
        }
        this.tb = table
        super.cast
    }

    private def loadTableDescription: Option[List[String]] = {
        readFile(paths.absoluteInputTableDescriptionPath(file))
    }

    private def intoTable(tableLines: List[String]): Try[TableDescription] = {
        TableDescription.fromLines(tableLines) match {
            case Failure(exception) =>
                Failure(new EnrichException(file, exception))
            case Success(value) => Success(value)
        }
    }

    private def applyFallbackMethodToCurrentFile: Executor = {
        logDebug(s"Apply fallback method ${fallBack.toString}")
        this.fallBack match {
            case FallBackRaw   => new Raw(file, paths)
            case FallBackInfer => new Infer(file, paths)
            case FallBackFail  => new Fail(file)
            case FallBackNone  => new Pass(file)
        }
    }

}

object Parse {

    /** Converts an input raw dataframe to a new dataframe with a specified user schema. If the data
      * is not in a proper format, it will return `null`
      *
      * @param df
      *   input raw dataframe with all columns as string
      * @param description
      *   new fields types to convert
      * @return
      *   a new dataframe with the new schema
      */
    protected[execution] def applySchema(
        df: DataFrame,
        description: TableDescription
    ): DataFrame = {
        description.fields.foldLeft(df) { (temp, field) =>
            temp.withColumn(field.fieldName, field.colExpression)
        }
    }
}
