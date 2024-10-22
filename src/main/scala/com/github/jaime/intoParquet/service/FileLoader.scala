package com.github.jaime.intoParquet.service

import com.github.jaime.intoParquet.behaviour.AppLogger
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.exception.NoFileFoundException

import java.nio.file.{Files, NoSuchFileException, Paths}
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.io.Source
import scala.util.{Failure, Success, Try}

class FileLoader(paths: BasePaths) extends AppLogger {

    private val InputSchemaPath: String = paths.inputBasePath
    private val InputRawPath: String    = paths.inputBasePath

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
        try {
            val csv = Files
                .list(filePath)
                .iterator()
                .asScala
                .filter(Files.isRegularFile(_))
                .filter(_.getFileName.toString.endsWith(".csv"))
                .map(_.getFileName.toString.replace(".csv", ""))
                .toArray
            Success(csv)
        } catch {
            case _: NoSuchFileException => Failure(new NoFileFoundException(filePath.toString))
        }
    }

}
