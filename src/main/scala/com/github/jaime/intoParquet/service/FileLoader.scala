/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.service

import com.github.jaime.intoParquet.behaviour.AppLogger
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.exception.NoFileFoundException

import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.Paths
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.io.Source
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class FileLoader(paths: BasePaths) extends AppLogger {

    private val InputSchemaPath: String = paths.inputBasePath
    private val InputRawPath: String    = paths.inputBasePath

    override def toString: String = {
        s"""> Input path: $InputRawPath
           |""".stripMargin
    }

    def readFile(fileName: String): Option[List[String]] = {
        readFromDataFolder(fileName) match {
            case Failure(_) =>
                logWarning(s"No configuration file for $fileName")
                None
            case Success(value) => Some(value)
        }
    }

    private def readFromDataFolder(fileName: String): Try[List[String]] = {
        val filePath = resolveFilePath(fileName)
        try {
            val file = Source.fromFile(filePath)
            file.bufferedReader().lines()
            val lines = file.getLines().toList
            file.close()
            Success(lines)
        } catch {
            case _: Exception => Failure(new Exception())
        }
    }

    private def resolveFilePath(fileName: String): String = {
        Paths.get(s"$InputSchemaPath$fileName").toAbsolutePath.toString
    }

    def readAllFilesFromRaw: Try[Array[String]] = {
        val filePath = Paths.get(s"$InputRawPath").toAbsolutePath
        logDebug(s"read from $InputRawPath")
        try {
            val csvFiles = Files
                .list(filePath)
                .iterator()
                .asScala
                .filter(file => isCSV(file))
                .map(_.getFileName.toString.replace(".csv", ""))
                .toArray
            csvFiles.length match {
                case 0 => Failure(new NoFileFoundException(filePath.toString))
                case _ => Success(csvFiles)
            }
        } catch {
            case _: NoSuchFileException => Failure(new NoFileFoundException(filePath.toString))
        }
    }

    private def isCSV(f: Path): Boolean = {
        Files.isRegularFile(f) && f.getFileName.toString.endsWith(".csv")
    }

}
