package com.intoParquet.service

import com.intoParquet.configuration.BasePaths
import com.intoParquet.exception.NoFileFoundException

import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.io.Source
import scala.util.{Failure, Success, Try}

object FileLoader {

    private val InputSchemaPath: String = BasePaths().InputSchemaPath
    private val InputRawPath: String    = BasePaths().InputRawPath

    def readFile(fileName: String): Option[List[String]] = {
        readFromDataFolder(fileName) match {
            case Failure(_)     => None
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
            case _: Exception => Failure(new NoFileFoundException(filePath))
        }
    }

    private def resolveFilePath(fileName: String): String = {
        Paths.get(s"$InputSchemaPath$fileName").toAbsolutePath.toString
    }

    def readAllFilesFromRaw: Array[String] = {
        val filePath = Paths.get(s"$InputRawPath").toAbsolutePath
        Files
            .list(filePath)
            .iterator()
            .asScala
            .filter(Files.isRegularFile(_))
            .filter(_.getFileName.toString.endsWith(".csv"))
            .map(_.getFileName.toString.replace(".csv", ""))
            .toArray
    }

}
