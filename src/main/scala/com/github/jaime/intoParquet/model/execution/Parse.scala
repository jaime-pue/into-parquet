/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model.execution

import com.github.jaime.intoParquet.app.SparkReader.readApplySchema
import com.github.jaime.intoParquet.behaviour.AppLogger
import com.github.jaime.intoParquet.behaviour.Executor
import com.github.jaime.intoParquet.behaviour.ReadAndWrite
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.exception.EnrichNotImplementedTypeException
import com.github.jaime.intoParquet.exception.NotImplementedTypeException
import com.github.jaime.intoParquet.model.TableDescription
import com.github.jaime.intoParquet.model.enumeration.FallBack
import com.github.jaime.intoParquet.model.enumeration.FallBackFail
import com.github.jaime.intoParquet.model.enumeration.FallBackInfer
import com.github.jaime.intoParquet.model.enumeration.FallBackNone
import com.github.jaime.intoParquet.model.enumeration.FallBackRaw
import com.github.jaime.intoParquet.service.FileLoader.readFile
import org.apache.spark.sql.DataFrame

class Parse(_file: String, _paths: BasePaths, fallBack: FallBack)
    extends Executor
    with ReadAndWrite
    with AppLogger {

    override protected val file: String                         = _file
    override val paths: BasePaths                               = _paths
    private lazy val tableDescription: Option[TableDescription] = loadTableDescription

    override def readFrom: DataFrame = {
        logDebug("Load table description")
        readApplySchema(absoluteInputCSVPath, tableDescription.get)
    }

    override def execution(): Unit = {
        if (tableDescription.isEmpty) {
            applyFallbackMethodToCurrentFile.execution()
        } else {
            writeResult()
        }
    }

    private def loadTableDescription: Option[TableDescription] = {
        readFile(paths.absoluteInputTableDescriptionPath(file)) match {
            case Some(value) => Some(intoTableDescription(value))
            case None =>
                logDebug(s"No configuration file for $file")
                None
        }
    }

    private def intoTableDescription(tableLines: List[String]): TableDescription = {
        try {
            new TableDescription(tableLines)
        } catch {
            case e: Exception => throw e
            case sqlError: NotImplementedTypeException =>
                throw new EnrichNotImplementedTypeException(
                  file = _file,
                  invalidType = sqlError.invalidType
                )
        }
    }

    private def applyFallbackMethodToCurrentFile: Executor = {
        logDebug(s"Apply fallback method ${fallBack.toString}")
        fallBack match {
            case FallBackRaw   => new Raw(file, paths)
            case FallBackInfer => new Infer(file, paths)
            case FallBackFail  => new Fail(file)
            case FallBackNone  => new Pass(file)
        }
    }

}
